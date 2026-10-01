//! Flatten drills against the stateful fake Kraken exchange (real adapter): sizing, dust, partial fills, raced
//! cancels, unknown outcomes, escalation, and idempotence across a crash at EVERY call boundary.

mod common;

use std::panic::{catch_unwind, AssertUnwindSafe};

use broker_adapters::{BrokerAdapter, OrderRequest, OrderStatus, PlaceOutcome, Side};
use common::*;
use fake_broker::scenarios as sc;
use fake_broker::{FillPolicy, OrderRule};
use rebalancer_run::broker::Broker;
use rebalancer_run::flatten::{
    cancel_own_open_orders, flatten, flatten_tag, FlattenCode, FlattenOutcome, FlattenReport, FlattenSpec, FlattenVerdict,
    FLATTEN_TAG_PREFIX,
};

const KEY: &str = "20260921T150000Z";

fn run_flatten(env: &Env, broker: &dyn Broker, key: &str) -> FlattenReport {
    let rules = kraken_rules(&env.pairs);
    let book = book(&rules);
    let uni = universe(&["BTC/USD", "ETH/USD"]);
    let ids = no_ids();
    let spec = FlattenSpec::new(ACCOUNT, key, &uni, &book, &ids);
    flatten(broker, &env.clock, &spec)
}

fn flat(env: &Env) -> FlattenReport {
    run_flatten(env, &env.broker(), KEY)
}

// ---------------------------------------------------------------------------------------------------------------
// Tags and codes
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn flatten_tags_are_deterministic_prefixed_short_and_unique_per_index() {
    let a = flatten_tag("acct-1", KEY, "BTC/USD", 1);
    assert_eq!(a, flatten_tag("acct-1", KEY, "BTC/USD", 1), "deterministic");
    assert!(a.starts_with(FLATTEN_TAG_PREFIX) && a.starts_with("rb1:"), "{a}");
    assert!(a.starts_with("rb1:fl:20260921T150000Z:BTCUSD:1:"), "{a}");
    assert!(a.len() < 128);
    let tags: std::collections::BTreeSet<String> =
        (1..=25).flat_map(|k| ["BTC/USD", "ETH/USD"].map(|s| flatten_tag("acct-1", KEY, s, k))).collect();
    assert_eq!(tags.len(), 50);
    assert_ne!(a, flatten_tag("acct-2", KEY, "BTC/USD", 1), "the account is part of the identity");
    assert_ne!(a, flatten_tag("acct-1", "20260922T150000Z", "BTC/USD", 1), "the attempt is part of the identity");
    // Pinned so a refactor cannot silently change the identity (which would break idempotence across a deploy).
    assert_eq!(a, "rb1:fl:20260921T150000Z:BTCUSD:1:f3b76e95eea01562");
}

#[test]
fn flatten_code_strings_are_pinned_and_unique() {
    let expected = [
        "FLATTEN_READ_FAILED",
        "FLATTEN_CANCEL_FAILED",
        "FLATTEN_CANCEL_TARGET_NOT_FOUND",
        "FLATTEN_CANCEL_UNSETTLED",
        "FLATTEN_SHORT_HELD",
        "FLATTEN_CANNOT_SIZE",
        "FLATTEN_LOOKUP_FAILED",
        "FLATTEN_ORDER_NOT_SENT",
        "FLATTEN_ORDER_REJECTED",
        "FLATTEN_ORDER_UNSETTLED",
        "FLATTEN_DUPLICATE_FILL_ANOMALY",
        "FLATTEN_OPEN_ORDERS_REMAIN",
        "FLATTEN_NOT_FLAT",
    ];
    let actual: Vec<&str> = FlattenCode::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(actual, expected);
    assert_eq!(actual.iter().collect::<std::collections::BTreeSet<_>>().len(), actual.len());
    assert_eq!((FlattenVerdict::Flat.as_str(), FlattenVerdict::FlatWithDust.as_str(), FlattenVerdict::HaltAndAlert.as_str()), ("FLAT", "FLAT_WITH_DUST", "HALT_AND_ALERT"));
}

// ---------------------------------------------------------------------------------------------------------------
// The straightforward cases
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn flatten_sells_exactly_what_is_held_and_verifies_flat() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    env.hold("ETH", "1.5");
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert!(r.verified_flat && r.failures.is_empty() && r.residual.is_empty() && r.dust.is_empty());
    assert_eq!(env.bal("BTC"), d("0"));
    assert_eq!(env.bal("ETH"), d("0"));
    assert_eq!(r.orders.len(), 2);
    let btc = r.orders.iter().find(|o| o.symbol == "BTC/USD").unwrap();
    assert_eq!((btc.quantity, btc.executed_quantity, btc.outcome), (d("0.05"), d("0.05"), FlattenOutcome::Filled));
    assert_eq!(env.sell_fills("BTC/USD"), [d("0.05")], "exactly one sell, of exactly the holding");
    assert_eq!(env.sell_fills("ETH/USD"), [d("1.5")]);
    // Proceeds arrived (less the 0.26% taker fee).
    assert!(env.bal("USD") > d("12400"), "{}", env.bal("USD"));
    env.rig.handle.assert_invariants();
    // Only sells were ever sent.
    assert!(env.rig.handle.orders(ACCOUNT).iter().all(|o| o.side == Side::Sell));
}

#[test]
fn flatten_of_a_flat_account_is_a_no_op_success() {
    let env = Env::new();
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat);
    assert!(r.orders.is_empty() && r.cancels.is_empty() && r.failures.is_empty());
    assert_eq!(env.order_requests(), 0, "nothing order-affecting reached the exchange");
    assert_eq!(r.rounds, 1);
}

#[test]
fn flatten_finishes_well_inside_the_thirty_second_bound() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    env.hold("ETH", "1");
    let r = flat(&env);
    let took = r.finished_at.signed_duration_since(r.started_at).num_seconds();
    assert!(took <= 30, "took {took} s of pipeline time");
    assert_eq!(r.started_at, t0());
}

#[test]
fn dust_below_the_venue_minimum_is_reported_and_never_sent() {
    let env = Env::new();
    env.hold("ETH", "0.001"); // ordermin is 0.002
    env.hold("BTC", "0.00005"); // ordermin is 0.0001
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::FlatWithDust, "{}", r.summary());
    assert!(r.verified_flat);
    assert!(r.orders.is_empty());
    let dust: Vec<(String, String)> = r.dust.iter().map(|x| (x.symbol.clone(), x.quantity.to_string())).collect();
    assert_eq!(dust, [("BTC/USD".to_string(), "0.00005".to_string()), ("ETH/USD".to_string(), "0.001".to_string())]);
    assert_eq!(env.order_requests(), 0, "dust is never sent");
    assert_eq!(env.bal("ETH"), d("0.001"));
}

#[test]
fn a_holding_finer_than_the_lot_precision_sells_the_rounded_amount_and_reports_the_remainder_as_dust() {
    let env = Env::new();
    env.hold("BTC", "0.123456789"); // lot precision is 8 decimals
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::FlatWithDust, "{}", r.summary());
    assert_eq!(r.orders[0].quantity, d("0.12345678"), "rounded DOWN, never above the holding");
    assert_eq!(env.sell_fills("BTC/USD"), [d("0.12345678")]);
    assert_eq!(env.bal("BTC"), d("0.000000009"));
    assert_eq!(r.dust.len(), 1);
    assert_eq!(r.dust[0].quantity, d("0.000000009"));
}

#[test]
fn only_the_mandates_universe_is_sold() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    env.hold("ETH", "1");
    let rules = kraken_rules(&env.pairs);
    let bk = book(&rules);
    let uni = universe(&["BTC/USD"]); // ETH is not in the mandate
    let ids = no_ids();
    let r = flatten(&env.broker(), &env.clock, &FlattenSpec::new(ACCOUNT, KEY, &uni, &bk, &ids));
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert_eq!(env.bal("BTC"), d("0"));
    assert_eq!(env.bal("ETH"), d("1"), "outside the universe: not ours to sell");
    assert_eq!(r.unmanaged.len(), 1);
    assert_eq!(r.unmanaged[0].symbol, "ETH/USD");
}

#[test]
fn a_short_position_is_never_touched_and_escalates() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    let real = env.broker();
    let lying = DistortingBroker {
        inner: &real,
        distort: Box::new(|s| {
            s.holdings.push(rebalancer_run::view::Holding {
                symbol: "ETH/USD".into(),
                asset_class: "crypto_spot".into(),
                quantity: d("-2"),
                mark: Some(d("3000")),
                market_value: d("-6000"),
            });
        }),
    };
    let r = run_flatten(&env, &lying, KEY);
    assert_eq!(r.verdict, FlattenVerdict::HaltAndAlert);
    assert!(r.has(FlattenCode::ShortHeld), "{}", r.summary());
    assert!(!r.verified_flat);
    assert!(env.rig.handle.orders(ACCOUNT).iter().all(|o| o.pair != "ETH/USD"), "no order on the short instrument");
    assert_eq!(env.bal("BTC"), d("0"), "the long was still flattened");
}

// ---------------------------------------------------------------------------------------------------------------
// Open orders: ours are cancelled, foreign ones are left alone
// ---------------------------------------------------------------------------------------------------------------

fn own_resting_sell(env: &Env, tag: &str, qty: &str, price: &str) -> String {
    match env.rig.adapter.place_order(&OrderRequest::limit(tag, "BTC/USD", Side::Sell, d(qty), d(price))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    }
}

#[test]
fn our_resting_orders_are_cancelled_first_so_the_held_quantity_is_free_to_sell() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    let id = own_resting_sell(&env, "rb1:20260921T140000Z:BTCUSD:sell:aaaa", "0.05", "70000");
    // The resting sell reserves the BTC: a market sell would be refused for insufficient funds.
    assert_eq!(env.rig.handle.available(ACCOUNT, "BTC"), d("0"));
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert_eq!(r.cancels.len(), 1);
    assert_eq!((r.cancels[0].broker_order_id.as_str(), r.cancels[0].canceled_count, r.cancels[0].final_status), (id.as_str(), 1, OrderStatus::Canceled));
    assert_eq!(env.bal("BTC"), d("0"));
    assert!(env.rig.handle.live_orders(ACCOUNT).is_empty());
}

#[test]
fn foreign_open_orders_are_left_alone_and_listed() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    let foreign = sc::foreign_order_appears(&env.rig.handle, ACCOUNT, "ETH/USD", Side::Buy, "0.1", "2000");
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert_eq!(r.foreign_open_orders, std::slice::from_ref(&foreign));
    assert!(r.cancels.is_empty());
    assert!(env.rig.handle.order(&foreign).unwrap().status.is_live(), "someone else's order was not cancelled");
}

#[test]
fn a_cancel_that_races_a_fill_is_settled_by_reading_the_report_and_the_fill_is_sold() {
    // Our resting BUY is being cancelled (the cancel is deferred: answered "pending"); while flatten waits for it
    // the market falls through the limit price and the order FILLS before the cancel is processed.
    let env = Env::new();
    let id = match env.rig.adapter.place_order(&OrderRequest::limit("rb1:20260921T140000Z:BTCUSD:buy:bbbb", "BTC/USD", Side::Buy, d("0.01"), d("55000"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    env.rig.handle.defer_next_cancels(1);
    let handle = env.rig.handle.clone();
    env.clock.set_sleep_hook(move || {
        handle.set_price("BTC/USD", "54000"); // the resting buy is now marketable and fills
        handle.settle_pending_cancels(); // ...and only then does the cancel get processed (too late)
    });
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert_eq!(r.cancels.len(), 1);
    let c = &r.cancels[0];
    assert_eq!((c.broker_order_id.as_str(), c.final_status, c.executed_quantity), (id.as_str(), OrderStatus::Filled, d("0.01")));
    assert_eq!(env.bal("BTC"), d("0"), "the BTC that arrived through the race was sold too");
    assert_eq!(env.sell_fills("BTC/USD"), [d("0.01")]);
}

#[test]
fn a_cancel_that_never_settles_is_a_blocking_failure_not_a_silent_skip() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    own_resting_sell(&env, "rb1:20260921T140000Z:BTCUSD:sell:cccc", "0.05", "70000");
    env.rig.handle.defer_next_cancels(100); // the cancel is acknowledged but never processed
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::HaltAndAlert);
    assert!(r.has(FlattenCode::CancelUnsettled), "{}", r.summary());
    assert!(r.has(FlattenCode::OpenOrdersRemain) || r.has(FlattenCode::NotFlat), "{}", r.summary());
}

#[test]
fn cleanup_cancels_only_our_orders_and_reports_the_foreign_ones() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    let own = own_resting_sell(&env, "rb1:20260921T140000Z:BTCUSD:sell:dddd", "0.02", "70000");
    let foreign = sc::foreign_order_appears(&env.rig.handle, ACCOUNT, "ETH/USD", Side::Buy, "0.1", "2000");
    let ids = no_ids();
    let (cancels, failures, foreigns) = cancel_own_open_orders(&env.broker(), &env.clock, &ids, 3, 1);
    assert!(failures.is_empty());
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0].broker_order_id, own);
    assert_eq!(foreigns, std::slice::from_ref(&foreign));
    assert!(env.rig.handle.order(&foreign).unwrap().status.is_live());
    assert!(!env.rig.handle.order(&own).unwrap().status.is_live());
}

// ---------------------------------------------------------------------------------------------------------------
// Partial fills, rejections, unknown outcomes, unfilled market orders
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_partial_fill_is_cancelled_and_the_rest_is_sold_in_the_next_round_without_double_selling() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    sc::partial_fills_next_order(&env.rig.handle, &["0.4"]); // the first sell executes 40% and rests
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert!(r.rounds >= 2);
    let btc: Vec<_> = r.orders.iter().filter(|o| o.symbol == "BTC/USD").collect();
    assert_eq!(btc.len(), 2, "{btc:?}");
    assert_eq!(btc[0].outcome, FlattenOutcome::PartiallyFilled);
    assert_eq!(btc[0].executed_quantity, d("0.02"));
    assert_eq!(btc[1].outcome, FlattenOutcome::Filled);
    assert_eq!(btc[1].quantity, d("0.03"), "the second sell is sized to what is STILL held");
    assert_ne!(btc[0].tag, btc[1].tag, "a new tag index for the second sell");
    assert_eq!(env.bal("BTC"), d("0"));
    let total = env.sell_fills("BTC/USD").into_iter().fold(d("0"), |a, b| a.checked_add(b).unwrap());
    assert_eq!(total, d("0.05"), "sold exactly the holding, no more");
}

#[test]
fn a_rejected_sell_is_recorded_and_the_next_round_finishes_the_job() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    sc::insufficient_funds_next_order(&env.rig.handle);
    let r = flat(&env);
    assert!(r.has(FlattenCode::OrderRejected), "{}", r.summary());
    assert!(r.verified_flat, "the retry sold it");
    assert_eq!(r.verdict, FlattenVerdict::Flat, "a transient rejection that was recovered is reported, not blocking");
    assert_eq!(r.orders.iter().filter(|o| o.outcome == FlattenOutcome::Rejected).count(), 1);
    assert_eq!(env.bal("BTC"), d("0"));
}

#[test]
fn a_persistent_rejection_escalates() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    env.rig.handle.script_orders(OrderRule::next(FillPolicy::reject("EOrder:Insufficient funds")).times(100));
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::HaltAndAlert);
    assert!(!r.verified_flat && r.has(FlattenCode::NotFlat) && r.has(FlattenCode::OrderRejected), "{}", r.summary());
    assert_eq!(r.residual.len(), 1);
    assert_eq!(r.residual[0].symbol, "BTC/USD");
    assert_eq!(env.bal("BTC"), d("0.05"));
}

#[test]
fn an_order_placed_but_response_lost_is_found_by_tag_and_never_sent_twice() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    sc::order_placed_response_lost(&env.rig.handle);
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    let o = &r.orders[0];
    assert!(o.adopted_by_tag, "the order that existed was adopted");
    assert_eq!(o.outcome, FlattenOutcome::Filled);
    assert_eq!(env.sell_fills("BTC/USD"), [d("0.05")], "exactly one sell ever reached the exchange");
    assert_eq!(env.rig.handle.applied_requests_to(fake_broker::kraken::wire::paths::ADD_ORDER).len(), 1);
}

#[test]
fn an_order_whose_request_was_lost_is_looked_up_found_absent_and_sent_once_more_with_the_same_tag() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    sc::order_request_lost(&env.rig.handle);
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    assert_eq!(env.sell_fills("BTC/USD"), [d("0.05")]);
    assert_eq!(r.orders.len(), 1);
    assert!(!r.orders[0].adopted_by_tag);
}

#[test]
fn a_market_order_that_never_fills_is_cancelled_and_retried() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    sc::no_fill_next_order(&env.rig.handle);
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    let btc: Vec<_> = r.orders.iter().filter(|o| o.symbol == "BTC/USD").collect();
    assert_eq!(btc.len(), 2);
    assert_eq!(btc[0].outcome, FlattenOutcome::NothingExecuted);
    assert_eq!(btc[1].outcome, FlattenOutcome::Filled);
    assert!(env.clock.total_slept_secs() >= 2, "it polled before giving up on the first order");
    assert_eq!(env.bal("BTC"), d("0"));
}

#[test]
fn a_broker_outage_escalates_without_trading_and_the_next_call_finishes_the_job() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    sc::broker_unreachable_for(&env.rig.handle, 1000);
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::HaltAndAlert);
    assert!(r.has(FlattenCode::ReadFailed), "{}", r.summary());
    assert!(r.orders.is_empty());
    env.rig.handle.clear_faults();
    let r2 = flat(&env);
    assert_eq!(r2.verdict, FlattenVerdict::Flat, "{}", r2.summary());
    assert_eq!(env.bal("BTC"), d("0"));
}

#[test]
fn a_duplicated_fill_report_is_a_blocking_anomaly_even_if_the_balances_are_flat() {
    let env = Env::new();
    env.hold("BTC", "0.05");
    // The next fill created anywhere is reported twice (balances move once).
    env.rig.handle.arm_fill_report_glitch(fake_broker::ReportGlitch::Duplicated);
    let r = flat(&env);
    assert_eq!(env.bal("BTC"), d("0"), "the exchange really did sell it once");
    assert!(r.has(FlattenCode::DuplicateFillAnomaly), "{}", r.summary());
    assert_eq!(r.verdict, FlattenVerdict::HaltAndAlert, "the broker's own reports contradict each other: a person must look");
}

// ---------------------------------------------------------------------------------------------------------------
// Idempotence across a crash at every call boundary
// ---------------------------------------------------------------------------------------------------------------

/// Run a full flatten against a fresh exchange, crashing at `crash` (if any), then run it again with a healthy
/// broker and the SAME attempt key. Returns the exchange for assertions.
fn crash_then_recover(setup: impl Fn(&Env), crash: Option<CrashAt>) -> (Env, bool) {
    let env = Env::new();
    setup(&env);
    let crashed = {
        let real = env.broker();
        let crashing = CrashingBroker::new(&real, crash);
        catch_unwind(AssertUnwindSafe(|| run_flatten(&env, &crashing, KEY))).is_err()
    };
    // A restarted process: same exchange, same attempt key, a new adapter would carry the persisted userref table;
    // the Kraken wrapper reserves the deterministic userref again, so the same adapter is equivalent here.
    let r = run_flatten(&env, &env.broker(), KEY);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "after {crash:?}: {}", r.summary());
    (env, crashed)
}

fn two_assets_and_a_resting_order(env: &Env) {
    env.hold("BTC", "0.05");
    env.hold("ETH", "1.5");
    own_resting_sell(env, "rb1:20260921T140000Z:BTCUSD:sell:eeee", "0.01", "70000");
}

fn healthy_call_count(setup: impl Fn(&Env)) -> u32 {
    let env = Env::new();
    setup(&env);
    let real = env.broker();
    let counting = CrashingBroker::new(&real, None);
    let r = run_flatten(&env, &counting, KEY);
    assert_eq!(r.verdict, FlattenVerdict::Flat, "{}", r.summary());
    counting.call_count()
}

#[test]
fn a_crash_at_any_call_boundary_never_double_sells_and_the_second_call_finishes_the_job() {
    let total = healthy_call_count(two_assets_and_a_resting_order);
    assert!(total >= 10, "the drill must have many boundaries to crash at, saw {total}");
    let mut crashes = 0;
    for n in 1..=total {
        for crash in [CrashAt::Before(n), CrashAt::After(n)] {
            let (env, crashed) = crash_then_recover(two_assets_and_a_resting_order, Some(crash));
            crashes += u32::from(crashed);
            assert_eq!(env.bal("BTC"), d("0"), "{crash:?}");
            assert_eq!(env.bal("ETH"), d("0"), "{crash:?}");
            // Every unit that was held was sold exactly once: total filled sells == holdings.
            let btc: rebalancer_run::Dec = env.sell_fills("BTC/USD").into_iter().fold(d("0"), |a, b| a.checked_add(b).unwrap());
            let eth: rebalancer_run::Dec = env.sell_fills("ETH/USD").into_iter().fold(d("0"), |a, b| a.checked_add(b).unwrap());
            // The own resting sell of 0.01 BTC was cancelled (or, if it had somehow filled, is inside the same total).
            assert_eq!(btc, d("0.05"), "{crash:?}: BTC sold {btc}");
            assert_eq!(eth, d("1.5"), "{crash:?}: ETH sold {eth}");
            assert!(env.rig.handle.live_orders(ACCOUNT).is_empty(), "{crash:?}: nothing left resting");
            env.rig.handle.assert_invariants();
        }
    }
    assert!(crashes >= total, "the drill really crashed: {crashes} crashes over {total} boundaries");
}

#[test]
fn a_third_call_after_recovery_is_a_no_op() {
    let (env, _) = crash_then_recover(two_assets_and_a_resting_order, Some(CrashAt::After(6)));
    let before = env.order_requests();
    let r = flat(&env);
    assert_eq!(r.verdict, FlattenVerdict::Flat);
    assert!(r.orders.is_empty() && r.cancels.is_empty());
    assert_eq!(env.order_requests(), before, "no further order-affecting request");
}

#[test]
fn flatten_is_deterministic_for_the_same_exchange_state() {
    let a = Env::new();
    two_assets_and_a_resting_order(&a);
    let b = Env::new();
    two_assets_and_a_resting_order(&b);
    let ra = flat(&a);
    let rb = flat(&b);
    let tags = |r: &FlattenReport| r.orders.iter().map(|o| (o.tag.clone(), o.symbol.clone(), o.quantity)).collect::<Vec<_>>();
    assert_eq!(tags(&ra), tags(&rb));
    assert_eq!(ra.dust, rb.dust);
}
