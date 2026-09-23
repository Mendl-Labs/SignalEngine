//! Kill / flatten drills against the fake OANDA exchange (real `OandaAdapter`, real `OandaBroker`, the EXISTING
//! `rebalancer_run::flatten`): sizing, cancels, unknown outcomes, and idempotence across a crash at EVERY call boundary.
//!
//! Scope, stated plainly: this exercises the flatten (kill) path. The rest of the existing drills (`run_once`, the
//! planner, reconciliation) are spot-shaped and are NOT run against OANDA here: see `oanda_view.rs` and the notes in
//! `rebalancer_run::broker::OandaBroker`. FX shorts cannot be flattened by this code (it refuses to trade a short);
//! that is asserted below as the honest current behaviour, not hidden.

mod common;

use std::panic::{catch_unwind, AssertUnwindSafe};

use broker_adapters::oanda::{InstrumentTable, PrepareOptions};
use broker_adapters::transport::HttpMethod;
use broker_adapters::{BrokerAdapter, Dec, OrderRequest, Side};
use common::*;
use fake_broker::oanda_rig::OandaRig;
use fake_broker::Fault;
use rebalancer_core::venue::{OandaRules, VenueRuleBook};
use rebalancer_run::broker::{Broker, OandaBroker};
use rebalancer_run::clock::ManualClock;
use rebalancer_run::flatten::{flatten, FlattenCode, FlattenOutcome, FlattenReport, FlattenSpec, FlattenVerdict};

const KEY: &str = "20260921T150000Z";

struct OEnv {
    rig: OandaRig,
    clock: ManualClock,
    instruments: InstrumentTable,
    opts: PrepareOptions,
}

impl OEnv {
    fn new() -> OEnv {
        let rig = OandaRig::with_config(|c| c.with_own_tag_prefix("rb1:").unwrap());
        let instruments = rig.adapter.instruments();
        let opts = PrepareOptions { own_tag_prefix: Some("rb1:".to_string()), ..PrepareOptions::default() };
        OEnv { rig, clock: ManualClock::new(t0()), instruments, opts }
    }

    fn broker(&self) -> OandaBroker<'_> {
        OandaBroker::fx(&self.rig.adapter)
    }

    /// Put a position on the exchange's book without any order of ours (an external trade).
    fn hold(&self, inst: &str, units: &str) {
        let avg = match inst {
            "USD_JPY" => "148.500",
            "GBP_USD" => "1.27000",
            _ => "1.10000",
        };
        self.rig.handle.set_position(inst, units, avg);
    }

    fn units(&self, inst: &str) -> Dec {
        self.rig.handle.position_units(inst)
    }

    fn sells_applied(&self) -> usize {
        self.rig.handle.applied(HttpMethod::Post, "/orders").len()
    }
}

fn run_flatten(env: &OEnv, broker: &dyn Broker, key: &str) -> FlattenReport {
    let rules = OandaRules { instruments: &env.instruments, options: &env.opts };
    let book = VenueRuleBook::new().with("oanda", &rules);
    let uni = universe(&["EUR/USD", "GBP/USD", "USD/JPY"]);
    let ids = no_ids();
    let spec = FlattenSpec::new("acct-fx", key, &uni, &book, &ids);
    flatten(broker, &env.clock, &spec)
}

fn flat(env: &OEnv) -> FlattenReport {
    run_flatten(env, &env.broker(), KEY)
}

// ---------------------------------------------------------------------------------------------------------------
// The straightforward cases
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn flatten_sells_exactly_the_held_long_units_with_market_orders_and_verifies_flat() {
    let env = OEnv::new();
    env.hold("EUR_USD", "10000");
    env.hold("USD_JPY", "2500");
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert!(r.verified_flat && r.failures.is_empty() && r.residual.is_empty() && r.dust.is_empty());
    assert_eq!((env.units("EUR_USD"), env.units("USD_JPY")), (Dec::ZERO, Dec::ZERO));
    assert_eq!(r.orders.len(), 2);
    let eur = r.orders.iter().find(|o| o.symbol == "EUR/USD").unwrap();
    assert_eq!((eur.quantity, eur.executed_quantity, eur.outcome), (d("10000"), d("10000"), FlattenOutcome::Filled));
    assert_eq!(eur.avg_price, Some(d("1.10048")), "a sell fills at the bid");
    assert!(eur.tag.starts_with("rb1:fl:20260921T150000Z:EURUSD:1:"), "{}", eur.tag);
    // exactly one sell per instrument, and every one of them was a MARKET order at the exchange even though flatten
    // supplies a reference price (the legacy connector would have turned that into a limit order)
    let orders = env.rig.handle.orders();
    assert_eq!(orders.len(), 2);
    assert!(orders.iter().all(|o| o.limit_price.is_none() && o.units.is_negative() && o.time_in_force == "FOK"), "{orders:?}");
    assert_eq!(env.sells_applied(), 2);
    // realised P&L came from the (external) 1.10000 entry to the bid: (1.10048 - 1.10000) * 10000 = 4.8
    assert!(env.rig.handle.balance() > d("100000"));
    env.rig.handle.assert_invariants();
}

#[test]
fn flatten_of_a_flat_account_sends_nothing() {
    let env = OEnv::new();
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat);
    assert!(r.orders.is_empty() && r.cancels.is_empty() && r.failures.is_empty());
    assert_eq!(env.rig.handle.order_affecting_requests(), 0);
    assert_eq!(r.rounds, 1);
}

#[test]
fn flatten_cancels_our_resting_orders_and_leaves_foreign_ones_alone() {
    let env = OEnv::new();
    env.hold("EUR_USD", "3000");
    env.rig.adapter.place_order(&OrderRequest::limit("rb1:rest", "EUR/USD", Side::Buy, d("500"), d("1.05000"))).unwrap();
    let foreign = env.rig.handle.add_foreign_limit("GBP_USD", "-800", "1.40000");
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert_eq!(r.cancels.len(), 1);
    assert_eq!(r.cancels[0].tag.as_deref(), Some("rb1:rest"));
    assert_eq!(r.foreign_open_orders, [foreign.clone()]);
    let pending = env.rig.handle.pending_orders();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, foreign, "only the foreign order is still resting");
    assert_eq!(env.units("EUR_USD"), Dec::ZERO);
}

#[test]
fn a_short_is_reported_and_left_alone_never_traded() {
    // Honest current behaviour: flatten's rule is "never open a short", so it cannot close one. The long is flattened;
    // the short is a coded, blocking-for-verification failure and the verdict is HALT_AND_ALERT.
    let env = OEnv::new();
    env.hold("EUR_USD", "5000");
    env.hold("GBP_USD", "-3000");
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::HaltAndAlert, "{}", r.summary());
    assert!(r.has(FlattenCode::ShortHeld));
    assert!(!r.verified_flat);
    assert_eq!(env.units("EUR_USD"), Dec::ZERO);
    assert_eq!(env.units("GBP_USD"), d("-3000"), "the short is untouched");
    assert_eq!(env.sells_applied(), 1);
}

#[test]
fn a_closed_market_is_an_alert_not_a_hang_and_a_later_flatten_with_the_same_key_finishes_the_job() {
    let env = OEnv::new();
    env.hold("EUR_USD", "4000");
    env.rig.handle.set_market_open(false);
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::HaltAndAlert, "{}", r.summary());
    assert!(r.has(FlattenCode::OrderRejected) || r.has(FlattenCode::NotFlat), "{}", r.summary());
    assert_eq!(env.units("EUR_USD"), d("4000"), "nothing was sold");
    // the market reopens; the second attempt (same key) skips the burnt tag index and sells once
    env.rig.handle.set_market_open(true);
    let r2 = flat(&env);
    assert_eq!(r2.verdict, FlattenVerdict::Flat, "{}", r2.summary());
    assert_eq!(env.units("EUR_USD"), Dec::ZERO);
    let tags: Vec<String> = env.rig.handle.orders().into_iter().filter_map(|o| o.client_id).collect();
    let unique: std::collections::BTreeSet<&String> = tags.iter().collect();
    assert!(tags.len() >= 2 && unique.len() == tags.len(), "a dead order's tag is never reused: {tags:?}");
    assert_eq!(env.rig.handle.fills().len(), 1, "only one sell ever executed");
    env.rig.handle.assert_invariants();
}

// ---------------------------------------------------------------------------------------------------------------
// Unknown outcomes
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_sell_whose_answer_is_lost_is_found_in_the_transaction_stream_and_sold_exactly_once() {
    // The adapter itself resolves the lost answer (it scans the stream from its checkpoint and finds the fill), so flatten
    // sees an ordinary accepted order rather than an unknown outcome to look up.
    let env = OEnv::new();
    env.hold("EUR_USD", "6000");
    env.rig.handle.inject_fault(Fault::timeout().after_apply().on_path(&env.rig.handle.path("/orders")));
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert_eq!(r.orders.len(), 1);
    assert_eq!(r.orders[0].outcome, FlattenOutcome::Filled);
    assert_eq!(r.orders[0].executed_quantity, d("6000"));
    assert_eq!(env.sells_applied(), 1, "exactly one sell reached the exchange");
    assert_eq!(env.units("EUR_USD"), Dec::ZERO, "and the account did not go short");
    env.rig.handle.assert_invariants();
}

#[test]
fn a_sell_that_never_arrived_is_looked_up_found_absent_and_sent_once_more() {
    let env = OEnv::new();
    env.hold("EUR_USD", "6000");
    env.rig.handle.inject_fault(Fault::io_error().on_path(&env.rig.handle.path("/orders")));
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert_eq!(env.sells_applied(), 1);
    assert_eq!(env.units("EUR_USD"), Dec::ZERO);
}

#[test]
fn an_exchange_that_stays_down_ends_in_an_alert_with_nothing_sold_twice() {
    let env = OEnv::new();
    env.hold("EUR_USD", "6000");
    env.rig.handle.inject_fault(Fault::http(503).after_apply().on_path(&env.rig.handle.path("/orders")).forever());
    let r = flat(&env);
    // every POST was applied at the exchange but answered 503: the by-tag lookup finds the first one, so it is adopted
    assert_eq!(env.units("EUR_USD"), Dec::ZERO);
    assert_eq!(env.sells_applied(), 1, "a 503 after apply is found by tag: never a second sell");
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    // and a genuinely unreachable exchange is an alert
    let env = OEnv::new();
    env.hold("EUR_USD", "6000");
    env.rig.handle.inject_fault(Fault::connect_failed().on_path(&env.rig.handle.path("/summary")).forever());
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::HaltAndAlert);
    assert_eq!(env.units("EUR_USD"), d("6000"));
    assert_eq!(env.sells_applied(), 0);
}

// ---------------------------------------------------------------------------------------------------------------
// A process killed at every call boundary
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn flatten_is_idempotent_across_a_crash_at_every_call_boundary() {
    // Establish how many broker calls a clean flatten of this book makes.
    let total_calls = {
        let env = OEnv::new();
        env.hold("EUR_USD", "10000");
        env.hold("USD_JPY", "2500");
        let b = env.broker();
        let cb = CrashingBroker::new(&b, None);
        let r = run_flatten(&env, &cb, KEY);
        assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
        cb.call_count()
    };
    assert!(total_calls >= 10, "{total_calls}");

    let mut crashed = 0;
    for n in 1..=total_calls {
        for crash in [CrashAt::Before(n), CrashAt::After(n)] {
            let env = OEnv::new();
            env.hold("EUR_USD", "10000");
            env.hold("USD_JPY", "2500");
            let first = {
                let b = env.broker();
                let cb = CrashingBroker::new(&b, Some(crash));
                catch_unwind(AssertUnwindSafe(|| run_flatten(&env, &cb, KEY)))
            };
            if first.is_err() {
                crashed += 1;
            }
            // the restarted process (same attempt key) finishes the job with a fresh adapter
            let mut env = env;
            env.rig.restart_adapter();
            let r = flat(&env);
            assert_eq!(r.verdict, FlattenVerdict::Flat, "{crash:?}: {}", r.summary());
            assert_eq!((env.units("EUR_USD"), env.units("USD_JPY")), (Dec::ZERO, Dec::ZERO), "{crash:?}: flat, and never short");
            // total units sold across both attempts is exactly what was held: nothing sold twice
            let fills = env.rig.handle.fills();
            let sold = |inst: &str| fills.iter().filter(|f| f.instrument == inst).fold(Dec::ZERO, |a, f| a.checked_add(f.units).unwrap());
            assert_eq!(sold("EUR_USD"), d("-10000"), "{crash:?}");
            assert_eq!(sold("USD_JPY"), d("-2500"), "{crash:?}");
            assert_eq!(env.rig.handle.orders().iter().filter(|o| o.client_id.is_some()).count(), 2, "{crash:?}: one order per instrument");
            env.rig.handle.assert_invariants();
        }
    }
    assert!(crashed >= total_calls as usize, "the drill really crashed the process ({crashed} crashes over {total_calls} boundaries)");
}
