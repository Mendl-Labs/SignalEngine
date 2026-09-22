//! One-call setups for the broker-only drills of SPEC section 5 (B1-B10) and the WP4.4 restart
//! and duplicate-run drills. Each function only scripts the fake; the test then drives the real
//! adapter (or, later, the rebalancer) against it and asserts on behaviour and on the event log.
//!
//! Naming: a scenario says what HAPPENS AT THE EXCHANGE, not what the caller should do.

use crate::broker::{FakeBrokerHandle, IntoDec};
use crate::exchange::policy::{FillPolicy, FillStep, OrderRule};
use crate::exchange::ForeignOrder;
use crate::fault::Fault;
use crate::kraken::wire::paths;
use crate::money::{mul, sub};
use broker_adapters::{Dec, Side};

// ---------------------------------------------------------------- unknown outcomes and outages

/// The next `AddOrder` is APPLIED (the order exists) but the response is lost (timeout). The
/// dangerous "unknown outcome": the caller must look the order up by userref, never re-send.
pub fn order_placed_response_lost(h: &FakeBrokerHandle) {
    h.inject_fault(Fault::timeout().after_apply().on_path(paths::ADD_ORDER));
}

/// The next `AddOrder` never reaches the exchange (timeout before it is applied). The caller
/// cannot tell this from [`order_placed_response_lost`] and must look up by userref, find
/// nothing, and only then re-send.
pub fn order_request_lost(h: &FakeBrokerHandle) {
    h.inject_fault(Fault::timeout().on_path(paths::ADD_ORDER));
}

/// The exchange PLACES the next order but answers with an error string (for example
/// `EService:Unavailable` or `EGeneral:Internal error`).
pub fn exchange_answers_error_after_placing(h: &FakeBrokerHandle, code: &str) {
    h.inject_fault(Fault::exchange_error(code).after_apply().on_path(paths::ADD_ORDER));
}

/// Every request fails with `EService:Unavailable`, not applied, for `n` requests; then the
/// exchange recovers.
pub fn exchange_unavailable_for(h: &FakeBrokerHandle, n: u32) {
    h.inject_fault(Fault::exchange_error("EService:Unavailable").times(n));
}

/// The exchange cannot be reached at all (connection refused) for `n` requests.
pub fn broker_unreachable_for(h: &FakeBrokerHandle, n: u32) {
    h.inject_fault(Fault::connect_failed().times(n));
}

/// `n` requests are answered with `EAPI:Rate limit exceeded` (not applied).
pub fn rate_limited_for(h: &FakeBrokerHandle, n: u32) {
    h.inject_fault(Fault::rate_limit().times(n));
}

// ---------------------------------------------------------------- key sharing and interference

/// Another process using the same API key succeeds with `other_nonce`, raising the key's highest
/// nonce to at least that value. Returns the response the other process got.
pub fn nonce_collision(h: &FakeBrokerHandle, api_key: &str, other_nonce: u64) -> broker_adapters::transport::HttpResponse {
    h.other_process_call(api_key, paths::BALANCE, &[], other_nonce)
}

/// Someone else's resting limit order appears on the account (no userref of ours). Returns its txid.
pub fn foreign_order_appears(h: &FakeBrokerHandle, account: &str, pair: &str, side: Side, qty: &str, price: &str) -> String {
    h.add_foreign_order(account, ForeignOrder::limit(pair, side, qty, price))
}

/// Balance changes with no trade behind it (deposit, withdrawal, an accounting fault at the
/// broker). `delta` is signed.
pub fn balance_drift(h: &FakeBrokerHandle, account: &str, asset: &str, delta: impl IntoDec) {
    h.adjust_balance(account, asset, delta);
}

// ---------------------------------------------------------------- order behaviour

/// The next order is refused with `EOrder:Insufficient funds` (regardless of the real balance).
pub fn insufficient_funds_next_order(h: &FakeBrokerHandle) {
    h.script_orders(OrderRule::next(FillPolicy::reject("EOrder:Insufficient funds")));
}

/// The next order is refused with `EGeneral:Invalid arguments`.
pub fn invalid_arguments_next_order(h: &FakeBrokerHandle) {
    h.script_orders(OrderRule::next(FillPolicy::reject("EGeneral:Invalid arguments")));
}

/// The next order fills in pieces: the first fraction executes at placement, the rest as the
/// test calls `apply_next_fill`. `fractions` are of the order volume, for example `["0.4", "0.3"]`
/// (leaving 30 percent open).
pub fn partial_fills_next_order(h: &FakeBrokerHandle, fractions: &[&str]) {
    let steps = fractions.iter().map(|f| FillStep::fraction(f)).collect();
    h.script_orders(OrderRule::next(FillPolicy::partial(steps)));
}

/// The next order is accepted and never fills on its own.
pub fn no_fill_next_order(h: &FakeBrokerHandle) {
    h.script_orders(OrderRule::next(FillPolicy::NoFill));
}

// ---------------------------------------------------------------- market and equity moves

/// Move a pair's price by `ratio` (negative = down). Returns the new price.
pub fn market_move(h: &FakeBrokerHandle, pair: &str, ratio: impl IntoDec) -> Dec {
    h.move_price_pct(pair, ratio)
}

/// Put the account's USD equity exactly `ratio` below `reference` (for a daily loss: the
/// day-start equity; for a drawdown: the high-water mark). Uses a USD cash movement so the
/// result is exact at any boundary. Returns the new equity.
pub fn equity_below(h: &FakeBrokerHandle, account: &str, reference: impl IntoDec, ratio: impl IntoDec) -> Dec {
    let reference = reference.into_dec();
    let target = sub(reference, mul(reference, ratio.into_dec()));
    h.set_equity(account, target);
    target
}

// ---------------------------------------------------------------- assertions on the log

/// Requests that reached the exchange and could have created or cancelled an order (validate-only
/// AddOrders do not count).
pub fn order_affecting_requests(h: &FakeBrokerHandle) -> usize {
    h.requests().iter().filter(|r| r.is_order_affecting()).count()
}

/// Panic if any order-affecting request reached the exchange (shadow mode, halted accounts,
/// no-trade-on-doubt).
pub fn assert_no_order_activity(h: &FakeBrokerHandle) {
    let n = order_affecting_requests(h);
    assert!(n == 0, "expected no order-affecting requests, saw {n}:\n{}", h.dump_log());
}
