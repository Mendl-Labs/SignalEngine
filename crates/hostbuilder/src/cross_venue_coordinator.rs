//! Cross-venue execution coordinator for correlated 2-leg trades.
//!
//! Conservative by design: on a leg-2 failure after a leg-1 fill, the default
//! (and only) behavior implemented here is submit-and-alert, NOT automatic
//! unwind. An automated market-order unwind of leg 1 under adverse
//! conditions can compound losses in exactly the scenario where something
//! has already gone wrong -- leaving unwind as a manual, human-in-the-loop
//! action is a deliberate choice, not an oversight. Automatic unwind, if it's
//! ever built, should be an explicit opt-in once the halt-and-alert path has
//! real operational experience behind it.
//!
//! Scoped to exactly 2 venues per deployment (see
//! `PaperDeploymentMeta::venues` doc) -- real-money coordination complexity
//! grows sharply past a two-legged trade.

use std::sync::Arc;

use dashmap::DashMap;
use lazy_static::lazy_static;
use uuid::Uuid;

use ultra_signal::{ExchangeId, Signal, SignalAction};

use crate::{OrderOrigin, OrderPriority, OrderType, PaperDeploymentMeta, SignalEngineUltraOrderManager};

lazy_static! {
    /// Deployment id -> halt reason. A halted deployment's bridge loop stops
    /// invoking `generate_signals()` for it entirely (checked before every
    /// tick, not just before order submission) until a human clears it.
    /// In-memory only, matching this file's existing `LAST_SIGNAL_EMITTED` /
    /// `LAST_BARS_ACCUMULATED` pattern -- a pod restart clears a halt, which
    /// is acceptable for a first pass since the underlying partial-fill
    /// position still requires human attention regardless of process state.
    pub static ref HALTED_DEPLOYMENTS: DashMap<Uuid, String> = DashMap::new();
}

/// Detect whether a strategy's signal batch looks like a correlated 2-leg
/// trade: exactly two signals, the same symbol, targeting two DIFFERENT
/// venues. Conservative on purpose -- any batch shape that doesn't
/// unambiguously look like a paired trade (more than 2 signals, mismatched
/// symbols, both signals for the same venue, either leg a Hold/Cancel)
/// falls through to the existing independent per-signal path instead of
/// being misinterpreted as a pair.
///
/// This is the cross-VENUE case: the SAME symbol, arbitraged across two
/// exchanges. See `is_pairs_trade_batch` for the cross-SYMBOL case (classic
/// pairs trading / statistical arbitrage) -- the two are mutually exclusive
/// by construction (this function requires equal `symbol_hash`, that one
/// requires different `symbol_hash`), so a caller can safely try both.
pub fn is_correlated_pair(sigs: &[Signal]) -> Option<(Signal, Signal)> {
    if sigs.len() != 2 {
        return None;
    }
    let (a, b) = (sigs[0], sigs[1]);
    if a.symbol_hash != b.symbol_hash {
        return None;
    }
    if a.exchange_id == b.exchange_id {
        return None;
    }
    let is_actionable = |s: &Signal| !matches!(s.action, SignalAction::Hold | SignalAction::Cancel);
    if !is_actionable(&a) || !is_actionable(&b) {
        return None;
    }
    Some((a, b))
}

/// Detect whether a strategy's signal batch looks like a classic pairs
/// trade: exactly two actionable signals for two DIFFERENT symbols (the
/// hedge's two legs), emitted together by a single `PairPythonBridgeStrategy`
/// instance (see `strategyhandler::pair_strategy`) -- as opposed to
/// `is_correlated_pair`'s cross-venue-arbitrage case (same symbol, different
/// exchange). The two legs may share an exchange or use different ones --
/// unlike cross-venue arb, that distinction doesn't matter here, since the
/// hedge relationship is between the two SYMBOLS, not between venues.
pub fn is_pairs_trade_batch(sigs: &[Signal]) -> Option<(Signal, Signal)> {
    if sigs.len() != 2 {
        return None;
    }
    let (a, b) = (sigs[0], sigs[1]);
    if a.symbol_hash == b.symbol_hash {
        return None;
    }
    let is_actionable = |s: &Signal| !matches!(s.action, SignalAction::Hold | SignalAction::Cancel);
    if !is_actionable(&a) || !is_actionable(&b) {
        return None;
    }
    Some((a, b))
}

/// Resolve which of a deployment's configured venues a signal's
/// `exchange_id` refers to, using the same name<->id mapping strategies
/// already use to stamp `exchange_id` in the first place
/// (`ExchangeId::from_venue_name`), so this can never disagree with how the
/// signal was tagged.
pub fn resolve_venue_for_exchange_id(venues: &[String], exchange_id: u8) -> Option<String> {
    venues
        .iter()
        .find(|v| ExchangeId::from_venue_name(v) as u8 == exchange_id)
        .cloned()
}

/// Resolve the connector key to submit a leg's order against.
///
/// Live mode: each venue's own name is already its connector key inside
/// `ExecutionHandler.connectors` (registered via `credential.exchange` in
/// `add_exchange_from_credential`), so no per-leg bookkeeping is needed.
///
/// Paper mode: leg 0 uses `paper_exchange`; leg 1 uses `paper_exchange_2` if
/// one was provisioned for this deployment, falling back to `paper_exchange`
/// (a single shared synthetic book) for any deployment that didn't get a
/// dedicated second connector.
pub fn connector_key_for_leg(meta: &PaperDeploymentMeta, venue: &str, leg_index: usize) -> String {
    if meta.mode == "live" {
        venue.to_string()
    } else if leg_index == 0 {
        meta.paper_exchange.clone()
    } else {
        meta.paper_exchange_2.clone().unwrap_or_else(|| meta.paper_exchange.clone())
    }
}

/// Outcome of attempting a correlated 2-leg trade.
#[derive(Debug, Clone, PartialEq)]
pub enum DualVenueOutcome {
    /// Both legs filled.
    BothFilled,
    /// Leg 1 never filled -- no partial state was taken on, nothing beyond
    /// the log to alert on.
    Leg1NotFilled,
    /// Leg 1 filled but leg 2 failed -- deployment halted, human
    /// intervention required to close leg 1's now-unhedged position.
    PartialFillHalted,
    /// The pair's signals couldn't be resolved to the deployment's
    /// configured venues at all -- neither leg was submitted.
    UnresolvedVenues,
}

/// Submit a correlated 2-leg trade: leg 1 first, then leg 2 only after leg 1
/// confirms filled. On leg-2 failure, halts the deployment (recorded in
/// `HALTED_DEPLOYMENTS`) and logs loudly -- it deliberately does NOT attempt
/// to automatically unwind leg 1's fill (see module doc).
///
/// `symbol1`/`symbol2` are each leg's own trading symbol -- equal for
/// `is_correlated_pair`'s cross-venue-arbitrage case (same instrument, two
/// exchanges), different for `is_pairs_trade_batch`'s classic-pairs case
/// (two different instruments). Passing the same string twice preserves the
/// original cross-venue-arb behavior exactly.
pub async fn execute_dual_venue_pair(
    deployment_id: Uuid,
    symbol1: &str,
    symbol2: &str,
    meta: &PaperDeploymentMeta,
    leg1: Signal,
    leg2: Signal,
    ultra_order_manager: &Arc<SignalEngineUltraOrderManager>,
) -> DualVenueOutcome {
    let (Some(venue1), Some(venue2)) = (
        resolve_venue_for_exchange_id(&meta.venues, leg1.exchange_id),
        resolve_venue_for_exchange_id(&meta.venues, leg2.exchange_id),
    ) else {
        ultra_logger::ultra_error!(format!(
            "🛑 Dual-venue pair for deployment {} could not be resolved to configured venues \
             ({:?}) -- dropping both legs without submitting either.",
            deployment_id, meta.venues
        ));
        return DualVenueOutcome::UnresolvedVenues;
    };
    let connector1 = connector_key_for_leg(meta, &venue1, 0);
    let connector2 = connector_key_for_leg(meta, &venue2, 1);

    let leg1_result = ultra_order_manager
        .process_signal_order(
            symbol1,
            &connector1,
            leg1.side,
            if leg1.is_market_order() { OrderType::Market } else { OrderType::Limit },
            leg1.quantity,
            leg1.price,
            leg1.strategy_id,
            OrderPriority::Critical,
            Some(OrderOrigin { deployment_id: meta.deployment_id, tenant_id: meta.tenant_id }),
        )
        .await;

    let leg1_filled = matches!(&leg1_result, Ok(r) if r.success && r.filled_quantity > 0.0 && r.avg_price > 0.0);
    if !leg1_filled {
        ultra_logger::ultra_warn!(format!(
            "Dual-venue pair for deployment {}: leg 1 ({}) did not fill -- leg 2 ({}) not submitted. result={:?}",
            deployment_id, venue1, venue2, leg1_result
        ));
        return DualVenueOutcome::Leg1NotFilled;
    }

    let leg2_result = ultra_order_manager
        .process_signal_order(
            symbol2,
            &connector2,
            leg2.side,
            if leg2.is_market_order() { OrderType::Market } else { OrderType::Limit },
            leg2.quantity,
            leg2.price,
            leg2.strategy_id,
            OrderPriority::Critical,
            Some(OrderOrigin { deployment_id: meta.deployment_id, tenant_id: meta.tenant_id }),
        )
        .await;

    let leg2_filled = matches!(&leg2_result, Ok(r) if r.success && r.filled_quantity > 0.0 && r.avg_price > 0.0);
    if leg2_filled {
        DualVenueOutcome::BothFilled
    } else {
        let reason = format!(
            "leg 1 filled on {} but leg 2 on {} failed: {:?}. Manual intervention required to close leg 1's position.",
            venue1, venue2, leg2_result
        );
        ultra_logger::ultra_error!(format!("🛑 HALTING deployment {}: {}", deployment_id, reason));
        HALTED_DEPLOYMENTS.insert(deployment_id, reason);
        DualVenueOutcome::PartialFillHalted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(strategy_id: u16, symbol_hash: u64, exchange_id: ExchangeId, action: SignalAction, qty: f64) -> Signal {
        Signal::new(strategy_id, symbol_hash, exchange_id, action, qty, f64::NAN)
    }

    fn test_meta(mode: &str, venues: Vec<String>, paper_exchange_2: Option<String>) -> PaperDeploymentMeta {
        PaperDeploymentMeta {
            tenant_id: Uuid::nil(),
            deployment_id: Uuid::nil(),
            strategy_id_hash: 1,
            paper_exchange: "paper_test".to_string(),
            paper_exchange_2,
            venues,
            symbols: vec!["BTC/USD".to_string()],
            mode: mode.to_string(),
            is_market_making: false,
        }
    }

    // --- is_correlated_pair ---

    #[test]
    fn is_correlated_pair_matches_two_signals_same_symbol_different_venues() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Buy, 1.0);
        let b = sig(1, 42, ExchangeId::Coinbase, SignalAction::Sell, 1.0);
        assert!(is_correlated_pair(&[a, b]).is_some());
    }

    #[test]
    fn is_correlated_pair_rejects_a_single_signal() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Buy, 1.0);
        assert!(is_correlated_pair(&[a]).is_none());
    }

    #[test]
    fn is_correlated_pair_rejects_three_signals() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Buy, 1.0);
        let b = sig(1, 42, ExchangeId::Coinbase, SignalAction::Sell, 1.0);
        let c = sig(1, 42, ExchangeId::Binance, SignalAction::Sell, 1.0);
        assert!(is_correlated_pair(&[a, b, c]).is_none());
    }

    #[test]
    fn is_correlated_pair_rejects_mismatched_symbols() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Buy, 1.0);
        let b = sig(1, 99, ExchangeId::Coinbase, SignalAction::Sell, 1.0);
        assert!(is_correlated_pair(&[a, b]).is_none());
    }

    #[test]
    fn is_correlated_pair_rejects_two_signals_for_the_same_venue() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Buy, 1.0);
        let b = sig(1, 42, ExchangeId::Kraken, SignalAction::Sell, 1.0);
        assert!(is_correlated_pair(&[a, b]).is_none());
    }

    #[test]
    fn is_correlated_pair_rejects_a_hold_leg() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Hold, 1.0);
        let b = sig(1, 42, ExchangeId::Coinbase, SignalAction::Sell, 1.0);
        assert!(is_correlated_pair(&[a, b]).is_none());
    }

    // --- is_pairs_trade_batch ---

    #[test]
    fn is_pairs_trade_batch_matches_two_different_symbols_same_exchange() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Buy, 1.0);
        let b = sig(1, 99, ExchangeId::Kraken, SignalAction::Sell, 2.0);
        assert!(is_pairs_trade_batch(&[a, b]).is_some());
    }

    #[test]
    fn is_pairs_trade_batch_matches_two_different_symbols_different_exchanges() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Buy, 1.0);
        let b = sig(1, 99, ExchangeId::Coinbase, SignalAction::Sell, 2.0);
        assert!(is_pairs_trade_batch(&[a, b]).is_some());
    }

    #[test]
    fn is_pairs_trade_batch_rejects_matching_symbols() {
        // Same symbol => this is is_correlated_pair's cross-venue-arb case,
        // not a pairs trade -- the two predicates must be mutually exclusive.
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Buy, 1.0);
        let b = sig(1, 42, ExchangeId::Coinbase, SignalAction::Sell, 1.0);
        assert!(is_pairs_trade_batch(&[a, b]).is_none());
    }

    #[test]
    fn is_pairs_trade_batch_rejects_wrong_batch_size() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Buy, 1.0);
        assert!(is_pairs_trade_batch(&[a]).is_none());
    }

    #[test]
    fn is_pairs_trade_batch_rejects_a_hold_leg() {
        let a = sig(1, 42, ExchangeId::Kraken, SignalAction::Hold, 1.0);
        let b = sig(1, 99, ExchangeId::Kraken, SignalAction::Sell, 1.0);
        assert!(is_pairs_trade_batch(&[a, b]).is_none());
    }

    // --- resolve_venue_for_exchange_id ---

    #[test]
    fn resolve_venue_for_exchange_id_finds_the_matching_configured_venue() {
        let venues = vec!["kraken".to_string(), "coinbase".to_string()];
        assert_eq!(
            resolve_venue_for_exchange_id(&venues, ExchangeId::Kraken as u8),
            Some("kraken".to_string())
        );
        assert_eq!(
            resolve_venue_for_exchange_id(&venues, ExchangeId::Coinbase as u8),
            Some("coinbase".to_string())
        );
    }

    #[test]
    fn resolve_venue_for_exchange_id_returns_none_when_no_configured_venue_matches() {
        let venues = vec!["kraken".to_string()];
        assert_eq!(resolve_venue_for_exchange_id(&venues, ExchangeId::Coinbase as u8), None);
    }

    // --- connector_key_for_leg ---

    #[test]
    fn connector_key_for_leg_uses_the_venue_name_directly_in_live_mode() {
        let meta = test_meta("live", vec!["kraken".to_string(), "coinbase".to_string()], None);
        assert_eq!(connector_key_for_leg(&meta, "kraken", 0), "kraken");
        assert_eq!(connector_key_for_leg(&meta, "coinbase", 1), "coinbase");
    }

    #[test]
    fn connector_key_for_leg_uses_the_dedicated_second_paper_connector_when_present() {
        let meta = test_meta(
            "paper",
            vec!["kraken".to_string(), "coinbase".to_string()],
            Some("paper_x_leg1".to_string()),
        );
        assert_eq!(connector_key_for_leg(&meta, "kraken", 0), "paper_test");
        assert_eq!(connector_key_for_leg(&meta, "coinbase", 1), "paper_x_leg1");
    }

    #[test]
    fn connector_key_for_leg_falls_back_to_the_shared_paper_connector_without_a_second_one() {
        let meta = test_meta("paper", vec!["kraken".to_string()], None);
        assert_eq!(connector_key_for_leg(&meta, "kraken", 0), "paper_test");
        // leg_index 1 with no dedicated second connector falls back to the same one.
        assert_eq!(connector_key_for_leg(&meta, "kraken", 1), "paper_test");
    }
}
