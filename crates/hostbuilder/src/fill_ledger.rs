//! Idempotent per-order fill ledger (live-safety PR1b, first slice).
//!
//! WHY THIS EXISTS. Live and paper fills reach `trade_history` /
//! `deployment_positions` through `paper_trade_writer`, which applies every
//! `PaperFillEvent` it receives with no de-duplication (`record_paper_fill`
//! has no unique key on the fill id). Two independent code paths can report
//! the SAME real fill:
//!   1. the signal loop records the immediate result of `place_order`
//!      (`fill_id = "fill_{signal_count}"`, `lib.rs` signal loop), and
//!   2. the venue's order-update stream records it again when it arrives
//!      (`fill_id = "ws_{order_id}_{local_nanos}"`, deployment `subscribe_exchange_updates`
//!      callback).
//!
//! The stream's id embeds the LOCAL receive time, so it can never match the
//! first path's id, and a reconnect that re-delivers an event is recorded
//! again too. Result: positions, realised P&L and the trade counters can be
//! inflated, and every limit or drawdown check that reads them is wrong.
//! (Found by reading the code on 2026-09-21; not yet observed on a live
//! account. An earlier note said live fills were not recorded at all; that
//! was wrong for the current main -- the recording exists, the de-duplication
//! does not.)
//!
//! WHAT IT DOES. It turns any mix of reports about an order into the exact
//! NEW quantity to record, using the order's CUMULATIVE filled quantity as
//! the source of truth: `delta = reported_cumulative - already_recorded`.
//! Replays, duplicates and out-of-order reports produce no delta by
//! construction, whichever path delivers them. Where a venue only reports
//! individual trades, `apply_trade` de-duplicates by trade id and folds the
//! trades into the same cumulative state.
//!
//! This module is PURE (no I/O, no database, no async) and is not called by
//! anything yet, so it changes no behaviour. Wiring it in, adding a durable
//! copy of its state (so a restart cannot re-record history; see `seed`) and a
//! unique index on the stored fill id are the following slices.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

/// Absolute floor for "same quantity" comparisons.
const QTY_EPS: f64 = 1e-12;
/// Relative tolerance for "same quantity" comparisons.
const QTY_REL_EPS: f64 = 1e-9;
/// Relative tolerance when checking a cumulative quantity against the order size.
const OVERFILL_REL_TOL: f64 = 1e-6;

/// Identifies one order of one deployment on one venue. The deployment is part
/// of the key so two deployments can never share or corrupt each other's state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderKey {
    pub deployment_id: Uuid,
    pub exchange: String,
    /// A stable id for the order (the venue's order id, or the client order id
    /// -- but the SAME kind of id from every reporting path).
    pub order_id: String,
}

impl OrderKey {
    pub fn new(deployment_id: Uuid, exchange: impl Into<String>, order_id: impl Into<String>) -> Self {
        Self { deployment_id, exchange: exchange.into(), order_id: order_id.into() }
    }
}

/// The NEW fill to record: quantity, its implied price, and the new fee.
#[derive(Debug, Clone, PartialEq)]
pub struct FillDelta {
    pub qty: f64,
    pub price: f64,
    pub fee: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// NaN, infinite, negative, or a positive quantity with no positive price.
    InvalidNumber,
    /// Cumulative quantity exceeds the order size registered for this order.
    OverFill,
    /// The new quantity would need a non-positive price to reconcile with the
    /// reported cumulative average (the venue's numbers contradict each other).
    InconsistentPrice,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Applied {
    /// New quantity: record exactly this and nothing else.
    Fill(FillDelta),
    /// Same as what is already recorded (a replay or a second path).
    Duplicate,
    /// Less than what is already recorded (out-of-order delivery); ignored.
    Stale,
    /// Not recordable; state is unchanged and the caller should raise an alert.
    Rejected(RejectReason),
}

#[derive(Debug, Clone, Default)]
struct OrderState {
    /// Recorded so far: quantity, notional (sum of qty*price) and fees.
    qty: f64,
    notional: f64,
    fee: f64,
    /// Order size, when known, for the over-fill check.
    expected_qty: Option<f64>,
    /// Running totals built from individual trades (`apply_trade`).
    seen_trades: HashSet<String>,
    stream_qty: f64,
    stream_notional: f64,
    stream_fee: f64,
}

#[derive(Debug, Default)]
pub struct FillLedger {
    orders: HashMap<OrderKey, OrderState>,
}

fn same_qty(a: f64, b: f64) -> bool {
    (a - b).abs() <= QTY_EPS.max(QTY_REL_EPS * a.abs().max(b.abs()))
}

fn valid_qty(q: f64) -> bool {
    q.is_finite() && q >= 0.0
}

impl FillLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of orders tracked (state is kept for the life of the process; the
    /// durable ledger that replaces it will own retention).
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// What has been recorded for an order: (quantity, notional, fees).
    pub fn recorded(&self, key: &OrderKey) -> Option<(f64, f64, f64)> {
        self.orders.get(key).map(|s| (s.qty, s.notional, s.fee))
    }

    /// Declare the order size so an impossible cumulative quantity is rejected
    /// instead of recorded.
    pub fn register_order(&mut self, key: &OrderKey, expected_qty: f64) {
        if expected_qty.is_finite() && expected_qty > 0.0 {
            self.orders.entry(key.clone()).or_default().expected_qty = Some(expected_qty);
        }
    }

    /// Restore what is ALREADY durably recorded for an order (from stored trade
    /// rows) at startup, so a restart followed by a replay does not record
    /// history a second time.
    pub fn seed(&mut self, key: &OrderKey, qty: f64, notional: f64, fee: f64) {
        if valid_qty(qty) && valid_qty(notional) && valid_qty(fee) {
            let st = self.orders.entry(key.clone()).or_default();
            st.qty = qty;
            st.notional = notional;
            st.fee = fee;
        }
    }

    /// The venue reports the order's CUMULATIVE filled quantity and the
    /// volume-weighted average price of that cumulative quantity (and, when it
    /// knows, the cumulative fee). This is the preferred entry point.
    pub fn apply_cumulative(
        &mut self,
        key: &OrderKey,
        cum_qty: f64,
        cum_avg_price: f64,
        cum_fee: Option<f64>,
    ) -> Applied {
        if !valid_qty(cum_qty) || !cum_avg_price.is_finite() || cum_avg_price < 0.0 {
            return Applied::Rejected(RejectReason::InvalidNumber);
        }
        if cum_qty > 0.0 && cum_avg_price <= 0.0 {
            return Applied::Rejected(RejectReason::InvalidNumber);
        }
        if let Some(f) = cum_fee {
            if !valid_qty(f) {
                return Applied::Rejected(RejectReason::InvalidNumber);
            }
        }
        let st = self.orders.entry(key.clone()).or_default();
        Self::advance(st, cum_qty, cum_qty * cum_avg_price, cum_fee)
    }

    /// The venue reports an individual trade (execution). De-duplicated by
    /// `trade_id`, then folded into the order's cumulative state so it cannot
    /// double-count a quantity a cumulative report already covered.
    ///
    /// Caveat: this assumes the stream covers the order from its first
    /// execution. If an early trade was missed (a disconnect), later trades
    /// are treated as already-covered until the cumulative total catches up;
    /// that errs toward under-counting, which a reconciliation against the
    /// venue's own positions must catch. Prefer `apply_cumulative` whenever the
    /// venue supplies a cumulative quantity.
    pub fn apply_trade(&mut self, key: &OrderKey, trade_id: &str, qty: f64, price: f64, fee: f64) -> Applied {
        if trade_id.is_empty() || !valid_qty(qty) || qty <= 0.0 || !price.is_finite() || price <= 0.0 || !valid_qty(fee) {
            return Applied::Rejected(RejectReason::InvalidNumber);
        }
        let st = self.orders.entry(key.clone()).or_default();
        if !st.seen_trades.insert(trade_id.to_string()) {
            return Applied::Duplicate;
        }
        st.stream_qty += qty;
        st.stream_notional += qty * price;
        st.stream_fee += fee;
        let (q, n, f) = (st.stream_qty, st.stream_notional, st.stream_fee);
        Self::advance(st, q, n, Some(f))
    }

    fn advance(st: &mut OrderState, cum_qty: f64, cum_notional: f64, cum_fee: Option<f64>) -> Applied {
        if let Some(expected) = st.expected_qty {
            if cum_qty > expected * (1.0 + OVERFILL_REL_TOL) + QTY_EPS {
                return Applied::Rejected(RejectReason::OverFill);
            }
        }
        if same_qty(cum_qty, st.qty) {
            return Applied::Duplicate;
        }
        if cum_qty < st.qty {
            return Applied::Stale;
        }
        let dq = cum_qty - st.qty;
        let dn = cum_notional - st.notional;
        let price = dn / dq;
        if !price.is_finite() || price <= 0.0 {
            return Applied::Rejected(RejectReason::InconsistentPrice);
        }
        let fee = cum_fee.map(|f| (f - st.fee).max(0.0)).unwrap_or(0.0);
        st.qty = cum_qty;
        st.notional = cum_notional;
        st.fee += fee;
        Applied::Fill(FillDelta { qty: dq, price, fee })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> OrderKey {
        OrderKey::new(Uuid::from_u128(1), "alpaca", "ord-1")
    }

    fn fill(a: Applied) -> FillDelta {
        match a {
            Applied::Fill(f) => f,
            other => panic!("expected Fill, got {other:?}"),
        }
    }

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn a_single_complete_fill_is_recorded_once() {
        let mut l = FillLedger::new();
        let f = fill(l.apply_cumulative(&key(), 10.0, 100.0, Some(0.5)));
        assert!(approx(f.qty, 10.0) && approx(f.price, 100.0) && approx(f.fee, 0.5));
        assert_eq!(l.apply_cumulative(&key(), 10.0, 100.0, Some(0.5)), Applied::Duplicate);
        assert_eq!(l.recorded(&key()), Some((10.0, 1000.0, 0.5)));
    }

    #[test]
    fn partial_then_complete_records_only_the_new_part_at_its_implied_price() {
        let mut l = FillLedger::new();
        let a = fill(l.apply_cumulative(&key(), 1.0, 100.0, None));
        assert!(approx(a.qty, 1.0) && approx(a.price, 100.0));
        // cumulative 3 @ average 110 => notional 330; 330 - 100 = 230 over 2 units = 115
        let b = fill(l.apply_cumulative(&key(), 3.0, 110.0, None));
        assert!(approx(b.qty, 2.0) && approx(b.price, 115.0), "{b:?}");
        let (q, n, _) = l.recorded(&key()).unwrap();
        assert!(approx(q, 3.0) && approx(n, 330.0));
    }

    #[test]
    fn out_of_order_report_is_ignored_and_changes_nothing() {
        let mut l = FillLedger::new();
        fill(l.apply_cumulative(&key(), 3.0, 100.0, None));
        assert_eq!(l.apply_cumulative(&key(), 1.0, 100.0, None), Applied::Stale);
        assert!(approx(l.recorded(&key()).unwrap().0, 3.0));
    }

    // The bug this module exists for: the same real fill reported by the
    // immediate order response AND by the update stream.
    #[test]
    fn immediate_response_and_stream_do_not_double_count() {
        let mut l = FillLedger::new();
        let first = fill(l.apply_cumulative(&key(), 10.0, 100.0, None));
        assert!(approx(first.qty, 10.0));
        // stream then reports the same execution as an individual trade
        assert_eq!(l.apply_trade(&key(), "t1", 10.0, 100.0, 0.0), Applied::Duplicate);
        assert!(approx(l.recorded(&key()).unwrap().0, 10.0), "position must stay 10, not 20");
    }

    #[test]
    fn stream_trades_after_a_partial_response_only_add_the_remainder() {
        let mut l = FillLedger::new();
        fill(l.apply_cumulative(&key(), 6.0, 100.0, None)); // response saw 6
        assert_eq!(l.apply_trade(&key(), "t1", 6.0, 100.0, 0.0), Applied::Duplicate);
        let rest = fill(l.apply_trade(&key(), "t2", 4.0, 101.0, 0.0));
        assert!(approx(rest.qty, 4.0), "{rest:?}");
        assert!(approx(l.recorded(&key()).unwrap().0, 10.0));
    }

    #[test]
    fn replayed_trade_id_is_a_duplicate() {
        let mut l = FillLedger::new();
        fill(l.apply_trade(&key(), "t1", 2.0, 50.0, 0.1));
        assert_eq!(l.apply_trade(&key(), "t1", 2.0, 50.0, 0.1), Applied::Duplicate);
        assert!(approx(l.recorded(&key()).unwrap().0, 2.0));
    }

    #[test]
    fn fees_are_recorded_as_increments_and_never_go_backwards() {
        let mut l = FillLedger::new();
        let a = fill(l.apply_cumulative(&key(), 1.0, 10.0, Some(0.5)));
        assert!(approx(a.fee, 0.5));
        let b = fill(l.apply_cumulative(&key(), 2.0, 10.0, Some(0.8)));
        assert!(approx(b.fee, 0.3), "{b:?}");
        // a report carrying a smaller cumulative fee must not reduce what is recorded
        let c = fill(l.apply_cumulative(&key(), 3.0, 10.0, Some(0.2)));
        assert!(approx(c.fee, 0.0), "{c:?}");
        assert!(approx(l.recorded(&key()).unwrap().2, 0.8));
    }

    #[test]
    fn invalid_numbers_are_rejected_without_touching_state() {
        let mut l = FillLedger::new();
        fill(l.apply_cumulative(&key(), 1.0, 10.0, None));
        for (q, p) in [(f64::NAN, 10.0), (-1.0, 10.0), (2.0, f64::NAN), (2.0, 0.0), (2.0, -5.0), (f64::INFINITY, 10.0)] {
            assert_eq!(
                l.apply_cumulative(&key(), q, p, None),
                Applied::Rejected(RejectReason::InvalidNumber),
                "({q},{p})"
            );
        }
        assert_eq!(l.apply_trade(&key(), "", 1.0, 10.0, 0.0), Applied::Rejected(RejectReason::InvalidNumber));
        assert_eq!(l.apply_trade(&key(), "x", 0.0, 10.0, 0.0), Applied::Rejected(RejectReason::InvalidNumber));
        assert_eq!(l.recorded(&key()), Some((1.0, 10.0, 0.0)));
    }

    #[test]
    fn a_cumulative_quantity_above_the_order_size_is_rejected() {
        let mut l = FillLedger::new();
        l.register_order(&key(), 10.0);
        fill(l.apply_cumulative(&key(), 10.0, 100.0, None));
        assert_eq!(l.apply_cumulative(&key(), 20.0, 100.0, None), Applied::Rejected(RejectReason::OverFill));
        assert!(approx(l.recorded(&key()).unwrap().0, 10.0));
    }

    #[test]
    fn contradictory_average_price_is_rejected() {
        let mut l = FillLedger::new();
        fill(l.apply_cumulative(&key(), 1.0, 100.0, None));
        // cumulative 2 @ 40 => notional 80 < the 100 already recorded: implied price negative
        assert_eq!(l.apply_cumulative(&key(), 2.0, 40.0, None), Applied::Rejected(RejectReason::InconsistentPrice));
        assert!(approx(l.recorded(&key()).unwrap().0, 1.0));
    }

    #[test]
    fn seeding_from_durable_state_makes_a_restart_replay_harmless() {
        let mut l = FillLedger::new();
        l.seed(&key(), 10.0, 1000.0, 0.5);
        // after a restart the venue re-delivers the whole order history
        assert_eq!(l.apply_cumulative(&key(), 10.0, 100.0, Some(0.5)), Applied::Duplicate);
        assert_eq!(l.apply_cumulative(&key(), 4.0, 100.0, None), Applied::Stale);
        // and genuinely new quantity is still recorded
        let more = fill(l.apply_cumulative(&key(), 12.0, 100.0, None));
        assert!(approx(more.qty, 2.0));
    }

    #[test]
    fn deployments_and_venues_never_share_state() {
        let mut l = FillLedger::new();
        let other_dep = OrderKey::new(Uuid::from_u128(2), "alpaca", "ord-1");
        let other_venue = OrderKey::new(Uuid::from_u128(1), "kraken", "ord-1");
        fill(l.apply_cumulative(&key(), 5.0, 10.0, None));
        fill(l.apply_cumulative(&other_dep, 5.0, 10.0, None));
        fill(l.apply_cumulative(&other_venue, 5.0, 10.0, None));
        assert_eq!(l.len(), 3);
    }

    /// Any delivery order, with any number of duplicates, must record the
    /// same total and never a negative or over-counted quantity.
    #[test]
    fn total_recorded_is_independent_of_delivery_order_and_duplicates() {
        // cumulative reports of one order: 2, 5, 9 units (average 100 throughout)
        let reports = [2.0_f64, 5.0, 5.0, 9.0, 2.0, 9.0, 5.0];
        let mut seed = 12345_u64;
        let mut next = move || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (seed >> 33) as usize
        };
        for _ in 0..500 {
            let mut order: Vec<f64> = reports.to_vec();
            for i in (1..order.len()).rev() {
                order.swap(i, next() % (i + 1));
            }
            let mut l = FillLedger::new();
            let mut total = 0.0;
            for cum in &order {
                if let Applied::Fill(d) = l.apply_cumulative(&key(), *cum, 100.0, None) {
                    assert!(d.qty > 0.0, "a delta must be positive");
                    total += d.qty;
                }
            }
            assert!(approx(total, l.recorded(&key()).unwrap().0), "sum of deltas must equal recorded: {order:?}");
            assert!(total <= 9.0 + 1e-9, "never more than the largest cumulative: {order:?}");
            // the largest report always ends up recorded (it is always applied unless
            // a larger one already was, and 9 is the largest)
            assert!(approx(total, 9.0), "final total must be 9 in every order: {order:?}");
        }
    }
}
