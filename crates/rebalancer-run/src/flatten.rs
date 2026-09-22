//! Flatten: cancel our open orders, sell every long instrument in the mandate's universe down to dust, verify flat.
//! Used for the `halt_flatten` rung and the daily-loss halt. It is the ONLY code allowed to send orders while the
//! account is halting, and it only ever sends SELL orders of quantities the broker says are currently held.
//!
//! # Guarantees
//! * **Sized exactly to what is held.** Each sell is `round_quantity(held)` from a FRESH read taken after our open
//!   orders were cancelled and settled. It is never larger than the holding, so flatten can never open a short
//!   (spot has no shorts; a pre-existing short is reported as a failure and left alone). Sub-minimum dust that the
//!   venue's rules refuse is REPORTED, never sent.
//! * **Deterministic tags per flatten attempt.** `flatten_tag(account, attempt_key, symbol, k)` is
//!   `rb1:fl:<attempt>:<SYMBOL>:<k>:<hash>`. The caller derives `attempt_key` from the halt (its timestamp), so a
//!   second call after a crash uses the same tags. The index `k` is the first one the broker has NO order for
//!   (looked up by tag before every placement): an order that exists is never re-sent under its own tag.
//! * **Idempotent across a crash at any point.** Because every sell is sized from a fresh read, a call that finds
//!   the job half done sells only what is still held; an asset already sold has nothing left to sell, so nothing is
//!   sold twice (crash-at-every-call-boundary is drilled).
//! * **Unknown outcomes are looked up, not retried blindly.** After `UnknownOutcome` the order is looked up by tag;
//!   found means adopted and settled, not found allows exactly one further attempt with the same tag.
//! * **Cancels are settled and their reports read**: `cancel_and_settle` returns the order's final state; an order
//!   that filled first is accepted as filled (the fill shows up in the fresh holdings read), a partial fill is
//!   accepted as such, and an order still live after polling is a blocking failure.
//! * **Failures escalate.** Every failure is a coded entry in the report; the verdict is `HaltAndAlert` unless the
//!   account was verified flat (dust aside) AND no blocking failure occurred. Nothing is silently skipped.
//! * **Only our orders are cancelled.** Foreign open orders are listed and left alone.

use std::collections::BTreeSet;

use broker_adapters::{BrokerError, Dec, OrderReport, OrderRequest, OrderStatus, PlaceOutcome, Side};
use chrono::{DateTime, Utc};
use rebalancer_core::dec_math::div_floor;
use rebalancer_core::venue::{SizeRefusal, VenueRuleBook};
use sha2::{Digest, Sha256};

use crate::broker::{is_own_order, Broker, OWN_TAG_PREFIX};
use crate::clock::Clock;
use crate::view::{BrokerSnapshot, Holding};

/// Every flatten tag starts with this (which itself starts with [`OWN_TAG_PREFIX`]).
pub const FLATTEN_TAG_PREFIX: &str = "rb1:fl:";

/// `rb1:fl:<attempt>:<SYMBOL>:<k>:<16 hex of SHA-256 over the full identity>`.
pub fn flatten_tag(account: &str, attempt_key: &str, symbol: &str, k: u32) -> String {
    let identity = format!("flatten:{account}:{attempt_key}:{symbol}:sell:{k}");
    let hash = hex::encode(Sha256::digest(identity.as_bytes()));
    let attempt: String = attempt_key.chars().filter(|c| c.is_ascii_alphanumeric()).take(20).collect();
    let readable: String = symbol.chars().filter(char::is_ascii_alphanumeric).take(10).collect::<String>().to_uppercase();
    format!("{FLATTEN_TAG_PREFIX}{attempt}:{readable}:{k}:{}", &hash[..16])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FlattenCode {
    ReadFailed,
    CancelFailed,
    CancelTargetNotFound,
    CancelUnsettled,
    ShortHeld,
    CannotSize,
    LookupFailed,
    OrderNotSent,
    OrderRejected,
    OrderUnsettled,
    DuplicateFillAnomaly,
    OpenOrdersRemain,
    NotFlat,
}

impl FlattenCode {
    pub const ALL: [FlattenCode; 13] = [
        FlattenCode::ReadFailed,
        FlattenCode::CancelFailed,
        FlattenCode::CancelTargetNotFound,
        FlattenCode::CancelUnsettled,
        FlattenCode::ShortHeld,
        FlattenCode::CannotSize,
        FlattenCode::LookupFailed,
        FlattenCode::OrderNotSent,
        FlattenCode::OrderRejected,
        FlattenCode::OrderUnsettled,
        FlattenCode::DuplicateFillAnomaly,
        FlattenCode::OpenOrdersRemain,
        FlattenCode::NotFlat,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            FlattenCode::ReadFailed => "FLATTEN_READ_FAILED",
            FlattenCode::CancelFailed => "FLATTEN_CANCEL_FAILED",
            FlattenCode::CancelTargetNotFound => "FLATTEN_CANCEL_TARGET_NOT_FOUND",
            FlattenCode::CancelUnsettled => "FLATTEN_CANCEL_UNSETTLED",
            FlattenCode::ShortHeld => "FLATTEN_SHORT_HELD",
            FlattenCode::CannotSize => "FLATTEN_CANNOT_SIZE",
            FlattenCode::LookupFailed => "FLATTEN_LOOKUP_FAILED",
            FlattenCode::OrderNotSent => "FLATTEN_ORDER_NOT_SENT",
            FlattenCode::OrderRejected => "FLATTEN_ORDER_REJECTED",
            FlattenCode::OrderUnsettled => "FLATTEN_ORDER_UNSETTLED",
            FlattenCode::DuplicateFillAnomaly => "FLATTEN_DUPLICATE_FILL_ANOMALY",
            FlattenCode::OpenOrdersRemain => "FLATTEN_OPEN_ORDERS_REMAIN",
            FlattenCode::NotFlat => "FLATTEN_NOT_FLAT",
        }
    }

    /// A blocking failure needs a person even if the account ends up flat: an order may still be live, or the
    /// broker's own reports contradict each other.
    pub fn is_blocking(self) -> bool {
        matches!(
            self,
            FlattenCode::CancelUnsettled | FlattenCode::OrderUnsettled | FlattenCode::DuplicateFillAnomaly | FlattenCode::OpenOrdersRemain | FlattenCode::NotFlat
        )
    }
}

impl std::fmt::Display for FlattenCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlattenFailure {
    pub code: FlattenCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CancelRecord {
    pub broker_order_id: String,
    pub tag: Option<String>,
    /// Orders the cancel actually cancelled (0 when the order had already ended: filled, or gone).
    pub canceled_count: u32,
    /// The order's state AFTER the cancel: check this, not `canceled_count`.
    pub final_status: OrderStatus,
    pub executed_quantity: Dec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlattenOutcome {
    /// The order ended fully executed.
    Filled,
    /// The order ended with only part executed.
    PartiallyFilled,
    /// Ended with nothing executed (canceled/expired/rejected by the exchange after acceptance).
    NothingExecuted,
    /// The broker refused it.
    Rejected,
    /// Never sent (local refusal or connection failure before sending).
    NotSent,
    /// The outcome was unknown and a by-tag lookup found nothing.
    UnknownNotFound,
    /// Still live after polling and cancelling.
    Unsettled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlattenOrder {
    pub tag: String,
    pub symbol: String,
    /// The quantity sent (rounded down from the holding).
    pub quantity: Dec,
    pub broker_order_id: Option<String>,
    pub outcome: FlattenOutcome,
    pub status: Option<OrderStatus>,
    pub executed_quantity: Dec,
    pub avg_price: Option<Dec>,
    pub cost: Option<Dec>,
    pub fee: Option<Dec>,
    /// True when the placement outcome was unknown and the order was found by tag.
    pub adopted_by_tag: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DustRecord {
    pub symbol: String,
    pub quantity: Dec,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlattenVerdict {
    /// Flat, nothing left at all.
    Flat,
    /// Flat except dust the venue would not accept an order for.
    FlatWithDust,
    /// Not verified flat, or a blocking failure: stop and alert a person.
    HaltAndAlert,
}

impl FlattenVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            FlattenVerdict::Flat => "FLAT",
            FlattenVerdict::FlatWithDust => "FLAT_WITH_DUST",
            FlattenVerdict::HaltAndAlert => "HALT_AND_ALERT",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlattenReport {
    pub attempt_key: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub rounds: u32,
    pub cancels: Vec<CancelRecord>,
    pub orders: Vec<FlattenOrder>,
    /// Holdings left because the venue's rules refuse an order that small. Reported, never sent.
    pub dust: Vec<DustRecord>,
    /// Holdings outside the mandate's universe: not ours to sell.
    pub unmanaged: Vec<DustRecord>,
    /// Ids of open orders that are not ours (left alone).
    pub foreign_open_orders: Vec<String>,
    pub failures: Vec<FlattenFailure>,
    /// Holdings still there after the final verification, dust excluded (empty when flat).
    pub residual: Vec<DustRecord>,
    pub verified_flat: bool,
    pub verdict: FlattenVerdict,
}

impl FlattenReport {
    pub fn has(&self, code: FlattenCode) -> bool {
        self.failures.iter().any(|f| f.code == code)
    }
    pub fn failure_codes(&self) -> Vec<&'static str> {
        self.failures.iter().map(|f| f.code.as_str()).collect()
    }
    pub fn summary(&self) -> String {
        format!(
            "flatten {}: {} order(s), {} cancel(s), {} dust, {} failure(s): {}",
            self.verdict.as_str(),
            self.orders.len(),
            self.cancels.len(),
            self.dust.len(),
            self.failures.len(),
            self.failures.iter().map(|f| format!("{}: {}", f.code, f.message)).collect::<Vec<_>>().join(" | ")
        )
    }
}

pub struct FlattenSpec<'a> {
    pub account_id: &'a str,
    /// Identifies THIS flatten (the halt): the same key on a second call gives the same tags.
    pub attempt_key: &'a str,
    /// Upper-case symbols the mandate allows; only these are sold.
    pub universe: &'a BTreeSet<String>,
    pub venue_rules: &'a VenueRuleBook<'a>,
    /// Broker ids of our earlier orders (an order with one of these ids is ours whatever its tag).
    pub known_order_ids: &'a BTreeSet<String>,
    /// Sell / verify rounds (a partial fill leaves something for the next round).
    pub max_rounds: u32,
    /// `get_order` polls per order before it is cancelled.
    pub max_polls: u32,
    pub poll_secs: u64,
    /// Highest tag index tried per symbol.
    pub max_tag_index: u32,
}

impl<'a> FlattenSpec<'a> {
    pub fn new(
        account_id: &'a str,
        attempt_key: &'a str,
        universe: &'a BTreeSet<String>,
        venue_rules: &'a VenueRuleBook<'a>,
        known_order_ids: &'a BTreeSet<String>,
    ) -> Self {
        Self { account_id, attempt_key, universe, venue_rules, known_order_ids, max_rounds: 4, max_polls: 3, poll_secs: 1, max_tag_index: 25 }
    }
}

struct Flattener<'a> {
    broker: &'a dyn Broker,
    clock: &'a dyn Clock,
    spec: &'a FlattenSpec<'a>,
    cancels: Vec<CancelRecord>,
    orders: Vec<FlattenOrder>,
    failures: Vec<FlattenFailure>,
    foreign: BTreeSet<String>,
}

impl Flattener<'_> {
    fn fail(&mut self, code: FlattenCode, message: impl Into<String>) {
        self.failures.push(FlattenFailure { code, message: message.into() });
    }

    fn is_anomaly(e: &BrokerError) -> bool {
        matches!(e, BrokerError::Malformed(m) if m.contains("overfill anomaly"))
    }

    /// Poll an order until it ends; cancel it if it will not end. `Err` = the reads themselves failed.
    fn settle(&mut self, id: &str) -> Result<OrderReport, BrokerError> {
        for i in 0..self.spec.max_polls {
            let r = self.broker.get_order(id)?;
            if r.status.is_terminal() {
                return Ok(r);
            }
            if i + 1 < self.spec.max_polls {
                self.clock.sleep_secs(self.spec.poll_secs);
            }
        }
        // Still live: a market order must not be left resting.
        let (_, report) = self.broker.cancel_and_settle(id)?;
        if !report.status.is_terminal() {
            self.fail(FlattenCode::OrderUnsettled, format!("order {id} is still {:?} after polling and a cancel", report.status));
        }
        Ok(report)
    }

    /// Cancel every open order of ours and settle each. Returns false when the open-order read failed.
    fn cancel_own_open_orders(&mut self) -> bool {
        let open = match self.broker.open_orders() {
            Ok(o) => o,
            Err(e) => {
                self.fail(FlattenCode::ReadFailed, format!("could not list open orders: {e}"));
                return false;
            }
        };
        for o in open {
            if !is_own_order(&o, self.spec.known_order_ids) {
                self.foreign.insert(o.broker_order_id.clone());
                continue;
            }
            match self.broker.cancel_and_settle(&o.broker_order_id) {
                Ok((outcome, report)) => {
                    self.cancels.push(CancelRecord {
                        broker_order_id: o.broker_order_id.clone(),
                        tag: o.tag.clone(),
                        canceled_count: outcome.canceled_count,
                        final_status: report.status,
                        executed_quantity: report.executed_quantity,
                    });
                    if !report.status.is_terminal() {
                        // The cancel is pending: give it time, then look again.
                        match self.settle(&o.broker_order_id) {
                            Ok(r) if !r.status.is_terminal() => self.fail(
                                FlattenCode::CancelUnsettled,
                                format!("order {} is still {:?} after the cancel", o.broker_order_id, r.status),
                            ),
                            Ok(r) => {
                                if let Some(c) = self.cancels.last_mut() {
                                    c.final_status = r.status;
                                    c.executed_quantity = r.executed_quantity;
                                }
                            }
                            Err(e) => self.fail(FlattenCode::CancelUnsettled, format!("order {}: could not confirm the cancel: {e}", o.broker_order_id)),
                        }
                    }
                }
                Err(BrokerError::CancelTargetNotFound(id)) => {
                    self.fail(FlattenCode::CancelTargetNotFound, format!("cancel target {id} was refused as unknown and not found: the next read decides"))
                }
                Err(e) if Self::is_anomaly(&e) => self.fail(FlattenCode::DuplicateFillAnomaly, format!("order {}: {e}", o.broker_order_id)),
                Err(e) => self.fail(FlattenCode::CancelFailed, format!("cancelling {}: {e}", o.broker_order_id)),
            }
        }
        true
    }

    /// The first tag index with no order at the broker; `None` when the lookup failed or every index is taken.
    fn free_tag(&mut self, symbol: &str) -> Option<String> {
        for k in 1..=self.spec.max_tag_index {
            let tag = flatten_tag(self.spec.account_id, self.spec.attempt_key, symbol, k);
            match self.broker.find_by_tag(&tag) {
                Ok(found) if found.is_empty() => return Some(tag),
                Ok(_) => continue,
                Err(e) => {
                    self.fail(FlattenCode::LookupFailed, format!("cannot verify that tag {tag} is unused ({e}); not sending"));
                    return None;
                }
            }
        }
        self.fail(FlattenCode::LookupFailed, format!("every flatten tag index up to {} is used for {symbol}", self.spec.max_tag_index));
        None
    }

    fn record_report(&mut self, tag: &str, symbol: &str, qty: Dec, report: &OrderReport, adopted: bool) {
        let outcome = if report.status == OrderStatus::Filled {
            FlattenOutcome::Filled
        } else if report.executed_quantity.is_positive() {
            FlattenOutcome::PartiallyFilled
        } else if report.status.is_terminal() {
            FlattenOutcome::NothingExecuted
        } else {
            FlattenOutcome::Unsettled
        };
        self.orders.push(FlattenOrder {
            tag: tag.to_string(),
            symbol: symbol.to_string(),
            quantity: qty,
            broker_order_id: Some(report.broker_order_id.clone()),
            outcome,
            status: Some(report.status),
            executed_quantity: report.executed_quantity,
            avg_price: report.avg_price,
            cost: report.cost,
            fee: report.fee,
            adopted_by_tag: adopted,
        });
    }

    fn record_unsent(&mut self, tag: &str, symbol: &str, qty: Dec, outcome: FlattenOutcome) {
        self.orders.push(FlattenOrder {
            tag: tag.to_string(),
            symbol: symbol.to_string(),
            quantity: qty,
            broker_order_id: None,
            outcome,
            status: None,
            executed_quantity: Dec::ZERO,
            avg_price: None,
            cost: None,
            fee: None,
            adopted_by_tag: false,
        });
    }

    fn settle_and_record(&mut self, tag: &str, symbol: &str, qty: Dec, id: &str, adopted: bool) {
        match self.settle(id) {
            Ok(report) => self.record_report(tag, symbol, qty, &report, adopted),
            Err(e) if Self::is_anomaly(&e) => {
                self.fail(FlattenCode::DuplicateFillAnomaly, format!("order {id}: {e}"));
                self.record_unsent(tag, symbol, qty, FlattenOutcome::Unsettled);
            }
            Err(e) => {
                self.fail(FlattenCode::OrderUnsettled, format!("order {id}: could not read its final state: {e}"));
                self.record_unsent(tag, symbol, qty, FlattenOutcome::Unsettled);
            }
        }
    }

    fn sell(&mut self, symbol: &str, qty: Dec, price: Dec) {
        let Some(tag) = self.free_tag(symbol) else { return };
        let mut req = OrderRequest::market(&tag, symbol, Side::Sell, qty);
        req.reference_price = Some(price);
        for attempt in 1..=2 {
            match self.broker.place(&req) {
                Ok(PlaceOutcome::Accepted { broker_order_id, .. }) => {
                    self.settle_and_record(&tag, symbol, qty, &broker_order_id, false);
                    return;
                }
                Ok(PlaceOutcome::ValidatedOnly { .. }) => {
                    self.fail(FlattenCode::OrderNotSent, format!("{symbol}: the broker only validated the order"));
                    self.record_unsent(&tag, symbol, qty, FlattenOutcome::NotSent);
                    return;
                }
                Ok(PlaceOutcome::Rejected { errors, .. }) => {
                    let codes = errors.iter().map(|e| e.code.clone()).collect::<Vec<_>>().join(", ");
                    self.fail(FlattenCode::OrderRejected, format!("{symbol}: the broker rejected the sell: {codes}"));
                    self.record_unsent(&tag, symbol, qty, FlattenOutcome::Rejected);
                    return;
                }
                Ok(PlaceOutcome::UnknownOutcome { reason, .. }) => match self.broker.find_by_tag(&tag) {
                    Ok(found) if !found.is_empty() => {
                        let id = found[0].broker_order_id.clone();
                        self.settle_and_record(&tag, symbol, qty, &id, true);
                        return;
                    }
                    Ok(_) if attempt == 1 => continue, // verified nothing exists: one more try with the same tag
                    Ok(_) => {
                        self.fail(FlattenCode::OrderNotSent, format!("{symbol}: outcome unknown ({reason}) and no order exists after {attempt} attempts"));
                        self.record_unsent(&tag, symbol, qty, FlattenOutcome::UnknownNotFound);
                        return;
                    }
                    Err(e) => {
                        self.fail(FlattenCode::LookupFailed, format!("{symbol}: outcome unknown ({reason}) and the lookup failed ({e}); not retrying"));
                        self.record_unsent(&tag, symbol, qty, FlattenOutcome::UnknownNotFound);
                        return;
                    }
                },
                Err(e) => {
                    self.fail(FlattenCode::OrderNotSent, format!("{symbol}: the sell was not sent: {e}"));
                    self.record_unsent(&tag, symbol, qty, FlattenOutcome::NotSent);
                    return;
                }
            }
        }
    }
}

/// What a fresh snapshot says to sell: (symbol, quantity, price), plus dust, holdings outside the universe, and the
/// problems that stop a holding from being sold at all.
struct SellPlan {
    sells: Vec<(String, Dec, Dec)>,
    dust: Vec<DustRecord>,
    unmanaged: Vec<DustRecord>,
    blockers: Vec<FlattenFailure>,
}

fn plan_sells(spec: &FlattenSpec<'_>, venue: &str, snap: &BrokerSnapshot) -> SellPlan {
    let rules = spec.venue_rules.get(venue);
    let mut holdings: Vec<&Holding> = snap.holdings.iter().collect();
    holdings.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    let mut sells = Vec::new();
    let mut dust = Vec::new();
    let mut unmanaged = Vec::new();
    let mut blockers = Vec::new();
    let mut block = |code: FlattenCode, message: String| blockers.push(FlattenFailure { code, message });
    for h in holdings {
        let sym = h.symbol.to_uppercase();
        if h.quantity.is_zero() {
            continue;
        }
        if !spec.universe.contains(&sym) {
            unmanaged.push(DustRecord { symbol: sym, quantity: h.quantity, reason: "not in the mandate's universe: left alone".to_string() });
            continue;
        }
        if h.quantity.is_negative() {
            block(FlattenCode::ShortHeld, format!("{sym}: a short position of {} exists; flatten never trades it", h.quantity));
            continue;
        }
        let (Some(rules), Some(price)) = (rules, price_of(h)) else {
            block(FlattenCode::CannotSize, format!("{sym}: no venue rules or no price to size a sell of {}", h.quantity));
            continue;
        };
        match rules.round_quantity(&h.symbol, Side::Sell, h.quantity, price) {
            Ok(q) => {
                if q > h.quantity {
                    block(FlattenCode::CannotSize, format!("{sym}: venue rounding produced {q}, more than the {} held", h.quantity));
                    continue;
                }
                if q < h.quantity {
                    let rest = rebalancer_core::dec_math::sub(h.quantity, q).unwrap_or(h.quantity);
                    dust.push(DustRecord { symbol: sym.clone(), quantity: rest, reason: "below the venue's lot precision after rounding down".to_string() });
                }
                sells.push((h.symbol.clone(), q, price));
            }
            Err(SizeRefusal::RoundsToZero | SizeRefusal::BelowMinQuantity { .. } | SizeRefusal::BelowMinCost { .. }) => {
                dust.push(DustRecord { symbol: sym, quantity: h.quantity, reason: "below the venue's minimum order: reported, not sent".to_string() });
            }
            Err(other) => block(FlattenCode::CannotSize, format!("{sym}: {other}")),
        }
    }
    SellPlan { sells, dust, unmanaged, blockers }
}

fn price_of(h: &Holding) -> Option<Dec> {
    match h.mark {
        Some(m) if m.is_positive() => Some(m),
        _ if h.quantity.is_positive() && h.market_value.is_positive() => div_floor(h.market_value, h.quantity, 8).ok().filter(|p| p.is_positive()),
        _ => None,
    }
}

/// Cancel our open orders only (the pipeline's pre-run clean-up uses this as well as [`flatten`]). Returns the
/// cancel records, any failures and the ids of foreign open orders (left alone).
pub fn cancel_own_open_orders(
    broker: &dyn Broker,
    clock: &dyn Clock,
    known_order_ids: &BTreeSet<String>,
    max_polls: u32,
    poll_secs: u64,
) -> (Vec<CancelRecord>, Vec<FlattenFailure>, Vec<String>) {
    let universe = BTreeSet::new();
    let rules = VenueRuleBook::new();
    let mut spec = FlattenSpec::new("cleanup", "cleanup", &universe, &rules, known_order_ids);
    spec.max_polls = max_polls;
    spec.poll_secs = poll_secs;
    let mut f = Flattener { broker, clock, spec: &spec, cancels: Vec::new(), orders: Vec::new(), failures: Vec::new(), foreign: BTreeSet::new() };
    f.cancel_own_open_orders();
    (f.cancels, f.failures, f.foreign.into_iter().collect())
}

/// Flatten the account. See the module docs for the guarantees. Never panics on broker errors; every problem is a
/// coded failure in the report.
pub fn flatten(broker: &dyn Broker, clock: &dyn Clock, spec: &FlattenSpec<'_>) -> FlattenReport {
    let started_at = clock.now();
    let mut f = Flattener { broker, clock, spec, cancels: Vec::new(), orders: Vec::new(), failures: Vec::new(), foreign: BTreeSet::new() };
    let mut dust: Vec<DustRecord> = Vec::new();
    let mut unmanaged: Vec<DustRecord> = Vec::new();
    let mut rounds = 0;

    for round in 1..=spec.max_rounds {
        rounds = round;
        if !f.cancel_own_open_orders() {
            break;
        }
        let snap = match broker.snapshot(clock.now()) {
            Ok(s) => s,
            Err(e) => {
                f.fail(FlattenCode::ReadFailed, format!("could not read the account: {e}"));
                break;
            }
        };
        let plan = plan_sells(spec, broker.venue(), &snap);
        dust = plan.dust;
        unmanaged = plan.unmanaged;
        if plan.sells.is_empty() {
            break;
        }
        for (symbol, qty, price) in plan.sells {
            f.sell(&symbol, qty, price);
        }
    }

    // Final verification from a fresh read (never from what we think we sold).
    let mut residual = Vec::new();
    let mut verified_flat = false;
    match broker.snapshot(clock.now()) {
        Err(e) => f.fail(FlattenCode::ReadFailed, format!("could not verify flat: {e}")),
        Ok(snap) => {
            let plan = plan_sells(spec, broker.venue(), &snap);
            dust = plan.dust;
            unmanaged = plan.unmanaged;
            for (symbol, qty, _) in plan.sells {
                residual.push(DustRecord { symbol: symbol.to_uppercase(), quantity: qty, reason: "still held after flatten".to_string() });
            }
            for b in plan.blockers {
                if !f.failures.contains(&b) {
                    f.failures.push(b);
                }
            }
            for o in &snap.open_orders {
                if !is_own_order(o, spec.known_order_ids) {
                    f.foreign.insert(o.broker_order_id.clone());
                }
            }
            if !residual.is_empty() {
                let list = residual.iter().map(|r| format!("{} {}", r.quantity, r.symbol)).collect::<Vec<_>>().join(", ");
                f.fail(FlattenCode::NotFlat, format!("still holding: {list}"));
            }
            if snap.open_orders.iter().any(|o| is_own_order(o, spec.known_order_ids)) {
                f.fail(FlattenCode::OpenOrdersRemain, "one of our orders is still open after the flatten".to_string());
            }
            verified_flat = residual.is_empty() && !f.failures.iter().any(|x| matches!(x.code, FlattenCode::ShortHeld | FlattenCode::CannotSize));
        }
    }

    let blocking = f.failures.iter().any(|x| x.code.is_blocking());
    let verdict = if !verified_flat || blocking {
        FlattenVerdict::HaltAndAlert
    } else if dust.is_empty() {
        FlattenVerdict::Flat
    } else {
        FlattenVerdict::FlatWithDust
    };
    FlattenReport {
        attempt_key: spec.attempt_key.to_string(),
        started_at,
        finished_at: clock.now(),
        rounds,
        cancels: f.cancels,
        orders: f.orders,
        dust,
        unmanaged,
        foreign_open_orders: f.foreign.into_iter().collect(),
        failures: f.failures,
        residual,
        verified_flat,
        verdict,
    }
}

/// The prefix that marks an order as ours (re-exported for convenience).
pub const OWN_PREFIX: &str = OWN_TAG_PREFIX;
