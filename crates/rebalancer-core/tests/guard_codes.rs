//! One accepting and one rejecting case for every guard denial code, plus boundary values exactly at each limit.
//!
//! Baseline mandate (mandate-core fixture): equity 5000 -> max_position 0.25 = 1250, crypto_spot cap 0.6 = 3000,
//! max_gross/max_net 1.0 = 5000, max_order_notional 1500, 20 orders/day, turnover 0.5 = 2500, cash reserve 5% = 250.

mod common;

use chrono::Duration;
use common::*;
use rebalancer_core::guard::{AccountView, DayCounters, DenialCode, PreTradeGuard, PricePoint, Verdict};
use rebalancer_core::policy::{MandateEnvelope, MandateStatus, Policy};
use rebalancer_core::Dec;
use serde_json::json;

fn check(policy: &Policy, account: &AccountView, order: &rebalancer_core::guard::ProposedOrder) -> Verdict {
    PreTradeGuard::check(policy, account, order, &day0())
}

fn codes(v: &Verdict) -> Vec<&'static str> {
    v.reasons.iter().map(|r| r.code.as_str()).collect()
}

fn assert_allowed(v: &Verdict) {
    assert!(v.allow, "expected allow, got {:?}", v.reasons);
    assert!(v.reasons.is_empty());
}

fn assert_only(v: &Verdict, code: DenialCode) {
    assert!(!v.allow, "expected a denial");
    assert_eq!(codes(v), vec![code.as_str()], "{:?}", v.reasons);
}

// ---------------------------------------------------------------------------------------------------------------
// Codes are stable strings.
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn denial_code_strings_are_pinned_and_unique() {
    let expected = [
        "MANDATE_INVALID",
        "MANDATE_NOT_ACTIVE",
        "MANDATE_EXPIRED",
        "ACCOUNT_HALTED",
        "ORDER_INVALID",
        "EQUITY_INVALID",
        "CURRENCY_MISMATCH",
        "PRICE_MISSING",
        "PRICE_STALE",
        "INSTRUMENT_DENIED",
        "INSTRUMENT_NOT_ALLOWED",
        "VENUE_NOT_ALLOWED",
        "ASSET_CLASS_NOT_ALLOWED",
        "SHORTING_FORBIDDEN",
        "DERIVATIVES_FORBIDDEN",
        "LEVERAGE_FORBIDDEN",
        "MAX_ORDER_NOTIONAL",
        "MAX_POSITION",
        "MAX_ASSET_CLASS",
        "MAX_GROSS",
        "MAX_NET",
        "MAX_ORDERS_PER_DAY",
        "MAX_TURNOVER_PER_DAY",
        "CASH_RESERVE",
        "ARITHMETIC_OVERFLOW",
    ];
    let actual: Vec<&str> = DenialCode::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(actual, expected);
    let unique: std::collections::BTreeSet<_> = actual.iter().collect();
    assert_eq!(unique.len(), actual.len());
}

#[test]
fn a_compliant_order_is_allowed_and_every_denial_has_a_message() {
    assert_allowed(&check(&policy(), &flat(), &buy("SPY", "10", "100")));
    let v = check(&policy(), &flat(), &buy("QQQ", "10", "100"));
    assert!(v.reasons.iter().all(|r| !r.message.is_empty()));
}

#[test]
fn several_denials_are_reported_together_in_a_fixed_order() {
    // Not allowed instrument, wrong venue, wrong class, and over the notional cap.
    let mut o = buy("QQQ", "20", "100");
    o.venue = "oanda".into();
    o.asset_class = "fx".into();
    let v = check(&policy(), &flat(), &o);
    assert_eq!(
        codes(&v),
        vec!["INSTRUMENT_NOT_ALLOWED", "VENUE_NOT_ALLOWED", "ASSET_CLASS_NOT_ALLOWED", "MAX_ORDER_NOTIONAL", "MAX_POSITION"]
    );
    assert_eq!(check(&policy(), &flat(), &o), v, "deterministic");
}

// ---------------------------------------------------------------------------------------------------------------
// Mandate standing
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn mandate_invalid() {
    let p = policy_with(|m| m["exposure"]["max_position"] = json!(25));
    let v = check(&p, &flat(), &buy("SPY", "1", "100"));
    assert_only(&v, DenialCode::MandateInvalid);
    assert!(v.reasons[0].message.contains("exposure.max_position"), "{}", v.reasons[0].message);
    // A reducing order is denied too: no trustworthy limits at all.
    let held = account("5000", "4000", vec![pos("SPY", "10", "1000")]);
    assert_only(&check(&p, &held, &sell("SPY", "1", "100")), DenialCode::MandateInvalid);
    assert_allowed(&check(&policy(), &flat(), &buy("SPY", "1", "100")));
}

#[test]
fn mandate_not_active() {
    let order = buy("SPY", "1", "100");
    // No envelope at all.
    let bare = Policy::compile(&body_with(|_| {}));
    assert_only(&check(&bare, &flat(), &order), DenialCode::MandateNotActive);
    for status in [MandateStatus::Draft, MandateStatus::Superseded, MandateStatus::Revoked, MandateStatus::Expired] {
        let p = Policy::compile(&body_with(|_| {})).with_envelope(MandateEnvelope { status, ..active_envelope() });
        assert_only(&check(&p, &flat(), &order), DenialCode::MandateNotActive);
    }
    // Not yet effective; exactly at the effective instant it is active.
    let env = MandateEnvelope { effective_from: now() + Duration::seconds(1), ..active_envelope() };
    let p = Policy::compile(&body_with(|_| {})).with_envelope(env);
    assert_only(&check(&p, &flat(), &order), DenialCode::MandateNotActive);
    let env = MandateEnvelope { effective_from: now(), ..active_envelope() };
    assert_allowed(&check(&Policy::compile(&body_with(|_| {})).with_envelope(env), &flat(), &order));
    // Reductions are denied too when there is no active mandate.
    let held = account("5000", "4000", vec![pos("SPY", "10", "1000")]);
    assert_only(&check(&bare, &held, &sell("SPY", "1", "100")), DenialCode::MandateNotActive);
}

#[test]
fn mandate_expired_allows_only_reducing_orders() {
    let held = account("5000", "4000", vec![pos("SPY", "10", "1000")]);
    let expired_at_now = MandateEnvelope { review_by: now(), ..active_envelope() };
    let p = Policy::compile(&body_with(|_| {})).with_envelope(expired_at_now);
    assert_only(&check(&p, &held, &buy("SPY", "1", "100")), DenialCode::MandateExpired);
    assert_allowed(&check(&p, &held, &sell("SPY", "10", "100")));
    // One second before review_by it is still active.
    let live = MandateEnvelope { review_by: now() + Duration::seconds(1), ..active_envelope() };
    let p = Policy::compile(&body_with(|_| {})).with_envelope(live);
    assert_allowed(&check(&p, &held, &buy("SPY", "1", "100")));
}

#[test]
fn account_halted_allows_only_reducing_orders() {
    let mut held = account("5000", "4000", vec![pos("SPY", "10", "1000")]);
    held.halted = true;
    assert_only(&check(&policy(), &held, &buy("SPY", "1", "100")), DenialCode::AccountHalted);
    assert_allowed(&check(&policy(), &held, &sell("SPY", "10", "100")));
    // Selling more than held is not a reduction (and would be a short).
    let v = check(&policy(), &held, &sell("SPY", "10.0001", "100"));
    assert!(v.has(DenialCode::AccountHalted) && v.has(DenialCode::ShortingForbidden), "{:?}", v.reasons);
    // Not halted: a plain buy is fine.
    held.halted = false;
    assert_allowed(&check(&policy(), &held, &buy("SPY", "1", "100")));
}

// ---------------------------------------------------------------------------------------------------------------
// Order and account sanity
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn order_invalid() {
    let p = policy();
    for bad in [buy("SPY", "0", "100"), buy("SPY", "-1", "100")] {
        assert_only(&check(&p, &flat(), &bad), DenialCode::OrderInvalid);
    }
    let mut o = buy("SPY", "1", "100");
    o.est_fee = d("-0.01");
    assert_only(&check(&p, &flat(), &o), DenialCode::OrderInvalid);
    let mut o = buy("SPY", "1", "100");
    o.symbol = "  ".into();
    assert_only(&check(&p, &flat(), &o), DenialCode::OrderInvalid);
    assert_allowed(&check(&p, &flat(), &buy("SPY", "0.000001", "100")));
}

#[test]
fn equity_invalid() {
    for eq in ["0", "-5"] {
        let acct = account(eq, "5000", vec![]);
        assert_only(&check(&policy(), &acct, &buy("SPY", "1", "100")), DenialCode::EquityInvalid);
    }
    assert_allowed(&check(&policy(), &account("100", "100", vec![]), &buy("SPY", "0.1", "100")));
}

#[test]
fn currency_mismatch() {
    let mut acct = flat();
    acct.ccy = "EUR".into();
    assert_only(&check(&policy(), &acct, &buy("SPY", "1", "100")), DenialCode::CurrencyMismatch);
    acct.ccy = "usd".into();
    assert_allowed(&check(&policy(), &acct, &buy("SPY", "1", "100")));
}

// ---------------------------------------------------------------------------------------------------------------
// Prices
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn price_missing() {
    let mut o = buy("SPY", "1", "100");
    o.price = None;
    assert_only(&check(&policy(), &flat(), &o), DenialCode::PriceMissing);
    let mut o = buy("SPY", "1", "100");
    o.price = Some(PricePoint { price: Dec::ZERO, as_of: now() });
    assert_only(&check(&policy(), &flat(), &o), DenialCode::PriceMissing);
    let mut o = buy("SPY", "1", "100");
    o.price = Some(PricePoint { price: d("-1"), as_of: now() });
    assert_only(&check(&policy(), &flat(), &o), DenialCode::PriceMissing);
    assert_allowed(&check(&policy(), &flat(), &buy("SPY", "1", "0.0001")));
}

#[test]
fn price_stale_boundaries() {
    let p = policy(); // max age 300 s
    let with_age = |secs: i64| {
        let mut o = buy("SPY", "1", "100");
        o.price = Some(PricePoint { price: d("100"), as_of: now() - Duration::seconds(secs) });
        o
    };
    assert_allowed(&check(&p, &flat(), &with_age(300)));
    assert_only(&check(&p, &flat(), &with_age(301)), DenialCode::PriceStale);
    // A tighter deployment setting.
    let strict = policy().with_max_price_age_secs(10);
    assert_allowed(&check(&strict, &flat(), &with_age(10)));
    assert_only(&check(&strict, &flat(), &with_age(11)), DenialCode::PriceStale);
    // Future-dated prices: 5 s of clock skew is tolerated, more is treated as stale.
    assert_allowed(&check(&p, &flat(), &with_age(-5)));
    assert_only(&check(&p, &flat(), &with_age(-6)), DenialCode::PriceStale);
}

#[test]
fn stale_or_missing_price_blocks_the_notional_checks_but_not_the_universe_checks() {
    let mut o = buy("QQQ", "1000000", "100");
    o.price = None;
    assert_eq!(codes(&check(&policy(), &flat(), &o)), vec!["PRICE_MISSING", "INSTRUMENT_NOT_ALLOWED"]);
}

// ---------------------------------------------------------------------------------------------------------------
// Universe
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn instrument_denied() {
    let p = policy_with(|m| {
        m["universe"]["instrument_allow"] = json!(["EFA", "BTC/USD", "ETH/USD"]);
        m["universe"]["instrument_deny"] = json!(["spy"]);
    });
    assert_only(&check(&p, &flat(), &buy("SPY", "1", "100")), DenialCode::InstrumentDenied);
    assert_allowed(&check(&p, &flat(), &buy("EFA", "1", "100")));
}

#[test]
fn instrument_not_allowed() {
    assert_only(&check(&policy(), &flat(), &buy("QQQ", "1", "100")), DenialCode::InstrumentNotAllowed);
    // Case and whitespace do not matter.
    let mut o = buy("SPY", "1", "100");
    o.symbol = " spy ".into();
    assert_allowed(&check(&policy(), &flat(), &o));
}

#[test]
fn venue_not_allowed() {
    let mut o = buy("SPY", "1", "100");
    o.venue = "oanda".into();
    assert_only(&check(&policy(), &flat(), &o), DenialCode::VenueNotAllowed);
    o.venue = "ALPACA".into();
    assert_allowed(&check(&policy(), &flat(), &o));
}

#[test]
fn asset_class_not_allowed() {
    let mut o = buy("SPY", "1", "100");
    o.asset_class = "fx".into();
    assert_only(&check(&policy(), &flat(), &o), DenialCode::AssetClassNotAllowed);
    o.asset_class = "US_ETF".into();
    assert_allowed(&check(&policy(), &flat(), &o));
}

// ---------------------------------------------------------------------------------------------------------------
// Shorting, derivatives, leverage
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn shorting_forbidden_boundaries() {
    let held = account("5000", "4000", vec![pos("SPY", "10", "1000")]);
    assert_allowed(&check(&policy(), &held, &sell("SPY", "10", "100")));
    assert_only(&check(&policy(), &held, &sell("SPY", "10.000001", "100")), DenialCode::ShortingForbidden);
    // Selling something not held at all.
    assert_only(&check(&policy(), &flat(), &sell("SPY", "1", "100")), DenialCode::ShortingForbidden);
    // Selling while already short.
    let short = account("5000", "5100", vec![pos("SPY", "-1", "-100")]);
    assert_only(&check(&policy(), &short, &sell("SPY", "1", "100")), DenialCode::ShortingForbidden);
    // Allowed once the mandate permits shorting (the caps still apply).
    let p = policy_with(|m| m["universe"]["shorting"] = json!(true));
    assert_allowed(&check(&p, &flat(), &sell("SPY", "1", "100")));
}

#[test]
fn derivatives_forbidden() {
    let mut o = buy("SPY", "1", "100");
    o.is_derivative = true;
    assert_only(&check(&policy(), &flat(), &o), DenialCode::DerivativesForbidden);
    let p = policy_with(|m| m["universe"]["derivatives"] = json!(true));
    assert_allowed(&check(&p, &flat(), &o));
}

#[test]
fn leverage_forbidden() {
    let mut o = buy("SPY", "1", "100");
    o.uses_margin = true;
    assert_only(&check(&policy(), &flat(), &o), DenialCode::LeverageForbidden);
    let p = policy_with(|m| {
        m["universe"]["leverage_max_gross"] = json!(2.0);
        m["exposure"]["max_gross"] = json!(1.5);
    });
    assert_allowed(&check(&p, &flat(), &o));
}

// ---------------------------------------------------------------------------------------------------------------
// Limits (each boundary: exactly at the limit passes, one step over is denied)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn max_order_notional_boundary() {
    // Raise the position cap so only the order cap binds: 1.0 of 5000.
    let p = policy_with(|m| m["exposure"]["max_position"] = json!(1.0));
    assert_allowed(&check(&p, &flat(), &buy("SPY", "15", "100"))); // 1500.00 exactly
    assert_only(&check(&p, &flat(), &buy("SPY", "15.0001", "100")), DenialCode::MaxOrderNotional); // 1500.01
    // A reducing order is exempt from the per-order cap.
    let held = account("5000", "2000", vec![pos("SPY", "30", "3000")]);
    assert_allowed(&check(&p, &held, &sell("SPY", "20", "100"))); // 2000 > 1500
}

#[test]
fn max_position_boundary() {
    let p = policy(); // 0.25 * 5000 = 1250
    assert_allowed(&check(&p, &flat(), &buy("SPY", "12.5", "100"))); // 1250.00
    assert_only(&check(&p, &flat(), &buy("SPY", "12.51", "100")), DenialCode::MaxPosition); // 1251.00
    // Existing position counts.
    let held = account("5000", "4000", vec![pos("SPY", "10", "1000")]);
    assert_allowed(&check(&p, &held, &buy("SPY", "2.5", "100"))); // 1250.00
    assert_only(&check(&p, &held, &buy("SPY", "2.5001", "100")), DenialCode::MaxPosition); // 1250.01
    // Reducing orders are exempt: a position already above the cap can still be trimmed.
    let over = account("5000", "3600", vec![pos("SPY", "14", "1400")]);
    assert_allowed(&check(&p, &over, &sell("SPY", "1", "100")));
    // ... but not increased.
    assert_only(&check(&p, &over, &buy("SPY", "0.01", "100")), DenialCode::MaxPosition);
}

#[test]
fn max_asset_class_boundary() {
    // crypto_spot cap 0.6 = 3000; per-instrument cap raised to 0.5 = 2500 so it does not bind first.
    let p = policy_with(|m| {
        m["exposure"]["max_position"] = json!(0.5);
        m["exposure"]["max_order_notional"]["amount"] = json!("2500.00");
    });
    let held = account("5000", "4000", vec![pos("ETH/USD", "1", "1000")]);
    assert_allowed(&check(&p, &held, &buy("BTC/USD", "0.02", "100000"))); // +2000 -> class 3000.00
    assert_only(&check(&p, &held, &buy("BTC/USD", "0.0200001", "100000")), DenialCode::MaxAssetClass); // 3000.01
    // The other class has no cap in the mandate.
    assert_allowed(&check(&p, &held, &buy("SPY", "20", "100")));
}

#[test]
fn max_gross_boundary() {
    // Shorting on so gross and net differ. gross cap 0.5 * 5000 = 2500; net cap 0.5 * 5000 = 2500.
    let p = policy_with(|m| {
        m["universe"]["shorting"] = json!(true);
        m["exposure"]["max_gross"] = json!(0.5);
        m["exposure"]["max_net"] = json!(0.5);
        m["exposure"]["max_asset_class"] = json!({});
    });
    // long SPY 1000, short EFA -1000: gross 2000, net 0.
    let acct = account("5000", "5000", vec![pos("SPY", "10", "1000"), pos("EFA", "-10", "-1000")]);
    assert_allowed(&check(&p, &acct, &buy("BTC/USD", "0.005", "100000"))); // +500 -> gross 2500.00
    assert_only(&check(&p, &acct, &buy("BTC/USD", "0.0050001", "100000")), DenialCode::MaxGross); // 2500.01
}

#[test]
fn max_net_boundary() {
    // net cap 0.1 * 5000 = 500; gross cap stays 5000.
    let p = policy_with(|m| {
        m["exposure"]["max_net"] = json!(0.1);
        m["exposure"]["max_asset_class"] = json!({});
    });
    let acct = account("5000", "4700", vec![pos("SPY", "3", "300")]);
    assert_allowed(&check(&p, &acct, &buy("EFA", "2", "100"))); // net 500.00
    assert_only(&check(&p, &acct, &buy("EFA", "2.0001", "100")), DenialCode::MaxNet); // 500.01
    // Net is signed: a short position offsets a long one.
    let ps = policy_with(|m| {
        m["universe"]["shorting"] = json!(true);
        m["exposure"]["max_net"] = json!(0.1);
        m["exposure"]["max_asset_class"] = json!({});
    });
    let acct = account("5000", "5500", vec![pos("SPY", "3", "300"), pos("EFA", "-5", "-500")]);
    assert_allowed(&check(&ps, &acct, &buy("BTC/USD", "0.007", "100000"))); // net -200 + 700 = 500
    assert_only(&check(&ps, &acct, &buy("BTC/USD", "0.0070001", "100000")), DenialCode::MaxNet);
    // Net below -cap is denied too (a large short).
    assert_only(&check(&ps, &acct, &sell("BTC/USD", "0.0080001", "100000")), DenialCode::MaxNet); // net -200 - 800.01
}

#[test]
fn max_orders_per_day_boundary() {
    let p = policy(); // 20 per day
    let o = buy("SPY", "1", "100");
    let with = |n: u32| DayCounters { orders_today: n, turnover_today: Dec::ZERO };
    assert_allowed(&PreTradeGuard::check(&p, &flat(), &o, &with(19)));
    let v = PreTradeGuard::check(&p, &flat(), &o, &with(20));
    assert_only(&v, DenialCode::MaxOrdersPerDay);
    // A normal (not halted, not expired) reducing order counts against the limit too.
    let held = account("5000", "4000", vec![pos("SPY", "10", "1000")]);
    assert_only(&PreTradeGuard::check(&p, &held, &sell("SPY", "1", "100"), &with(20)), DenialCode::MaxOrdersPerDay);
    // ... but when the account is halted, reductions skip the churn limits.
    let mut halted = held.clone();
    halted.halted = true;
    assert_allowed(&PreTradeGuard::check(&p, &halted, &sell("SPY", "1", "100"), &with(20)));
    // u32::MAX must not wrap.
    assert_only(&PreTradeGuard::check(&p, &flat(), &o, &with(u32::MAX)), DenialCode::MaxOrdersPerDay);
}

#[test]
fn max_turnover_boundary() {
    let p = policy(); // 0.5 * 5000 = 2500
    let o = buy("SPY", "10", "100"); // 1000
    let with = |t: &str| DayCounters { orders_today: 1, turnover_today: d(t) };
    assert_allowed(&PreTradeGuard::check(&p, &flat(), &o, &with("1500"))); // 2500.00
    assert_only(&PreTradeGuard::check(&p, &flat(), &o, &with("1500.01")), DenialCode::MaxTurnoverPerDay);
    // Sells count as turnover too.
    let held = account("5000", "4000", vec![pos("SPY", "10", "1000")]);
    assert_only(&PreTradeGuard::check(&p, &held, &sell("SPY", "10", "100"), &with("1500.01")), DenialCode::MaxTurnoverPerDay);
    // Halted reductions are exempt.
    let mut halted = held.clone();
    halted.halted = true;
    assert_allowed(&PreTradeGuard::check(&p, &halted, &sell("SPY", "10", "100"), &with("1500.01")));
}

#[test]
fn cash_reserve_boundary_includes_fees() {
    let p = policy(); // reserve 5% of 5000 = 250
    // Account holds 3750 of stock (not modelled: irrelevant here) and 1250 cash.
    let acct = account("5000", "1250", vec![pos("EFA", "37.5", "3750")]);
    assert_allowed(&check(&p, &acct, &buy("SPY", "10", "100"))); // cash 250.00
    assert_only(&check(&p, &acct, &buy("SPY", "10.0001", "100")), DenialCode::CashReserve); // 249.99
    // The fee comes out of cash too.
    let mut o = buy("SPY", "9.99", "100"); // 999.00 + fee
    o.est_fee = d("1.00");
    assert_allowed(&check(&p, &acct, &o)); // cash 250.00
    o.est_fee = d("1.01");
    assert_only(&check(&p, &acct, &o), DenialCode::CashReserve);
    // Reducing orders are exempt (they add cash anyway).
    let low = account("5000", "100", vec![pos("SPY", "10", "1000")]);
    assert_allowed(&check(&p, &low, &sell("SPY", "1", "100")));
}

#[test]
fn arithmetic_overflow_denies() {
    let mut o = buy("SPY", "1", "100");
    o.quantity = Dec::new(i128::MAX / 4, 0).unwrap();
    o.price = Some(PricePoint { price: d("1000000"), as_of: now() });
    let v = check(&policy(), &flat(), &o);
    assert!(!v.allow);
    assert!(v.has(DenialCode::ArithmeticOverflow), "{:?}", v.reasons);
    assert_allowed(&check(&policy(), &flat(), &buy("SPY", "1", "100")));
}

#[test]
fn f64_ratios_are_read_as_the_decimal_that_was_typed() {
    // 0.07 is not exactly representable in binary; the limit must still be exactly 7% (350 of 5000), not 350.0000000000000333.
    let p = policy_with(|m| {
        m["exposure"]["max_position"] = json!(0.07);
        m["exposure"]["max_asset_class"] = json!({});
    });
    assert_allowed(&check(&p, &flat(), &buy("SPY", "3.5", "100"))); // 350.00
    assert_only(&check(&p, &flat(), &buy("SPY", "3.50001", "100")), DenialCode::MaxPosition);
}

// ---------------------------------------------------------------------------------------------------------------
// The capital base: min(broker equity, mandate allocation) is the denominator of every percentage limit.
// Baseline allocation is 5000. Each test uses an account whose broker equity is 20000, so a guard that reverted to
// the raw equity would allow four times as much and FAIL these tests (mutation-checked, see the commit message).
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn capital_base_is_the_smaller_of_equity_and_allocation() {
    let p = policy();
    assert_eq!(p.capital_base(d("20000"), "USD"), d("5000"), "equity above the allocation: the allocation");
    assert_eq!(p.capital_base(d("2500"), "USD"), d("2500"), "equity below the allocation: the equity");
    assert_eq!(p.capital_base(d("5000"), "USD"), d("5000"));
    assert_eq!(p.capital_base(d("20000"), "usd"), d("5000"), "currency compares case-insensitively");
    assert_eq!(p.capital_base(d("20000"), "EUR"), d("20000"), "a currency mismatch cannot be compared: raw equity");
    assert_eq!(policy_with(|m| m["exposure"]["max_position"] = json!(25)).capital_base(d("20000"), "USD"), d("20000"), "invalid policy: raw equity");
}

#[test]
fn max_position_is_a_fraction_of_the_capital_base_not_of_broker_equity() {
    let rich = account("20000", "20000", vec![]);
    // 0.25 * min(20000, 5000) = 1250, not 5000.
    assert_allowed(&check(&policy(), &rich, &buy("SPY", "12.5", "100"))); // 1250.00
    assert_only(&check(&policy(), &rich, &buy("SPY", "12.51", "100")), DenialCode::MaxPosition); // 1251.00
    // Below the allocation the (smaller) equity is the base: 0.25 * 2500 = 625.
    let poor = account("2500", "2500", vec![]);
    assert_allowed(&check(&policy(), &poor, &buy("SPY", "6.25", "100")));
    assert_only(&check(&policy(), &poor, &buy("SPY", "6.26", "100")), DenialCode::MaxPosition);
}

#[test]
fn max_asset_class_is_a_fraction_of_the_capital_base() {
    let p = policy_with(|m| {
        m["exposure"]["max_position"] = json!(0.5);
        m["exposure"]["max_order_notional"]["amount"] = json!("2500.00");
    });
    let held = account("20000", "19000", vec![pos("ETH/USD", "1", "1000")]);
    // crypto_spot cap 0.6 * 5000 = 3000 (raw equity would allow 12000).
    assert_allowed(&check(&p, &held, &buy("BTC/USD", "0.02", "100000"))); // class 3000.00
    assert_only(&check(&p, &held, &buy("BTC/USD", "0.0200001", "100000")), DenialCode::MaxAssetClass);
}

#[test]
fn max_gross_and_max_net_are_fractions_of_the_capital_base() {
    let p = policy_with(|m| {
        m["exposure"]["max_gross"] = json!(0.5);
        m["exposure"]["max_net"] = json!(0.5);
        m["exposure"]["max_asset_class"] = json!({});
        m["exposure"]["max_position"] = json!(0.5);
    });
    // gross cap 0.5 * 5000 = 2500. Existing SPY 2000; +500 reaches the cap, one cent more is denied.
    let acct = account("20000", "18000", vec![pos("SPY", "20", "2000")]);
    assert_allowed(&check(&p, &acct, &buy("EFA", "5", "100"))); // gross 2500.00, net 2500.00
    let v = check(&p, &acct, &buy("EFA", "5.0001", "100"));
    assert!(v.has(DenialCode::MaxGross) && v.has(DenialCode::MaxNet), "{:?}", v.reasons);
}

#[test]
fn max_turnover_is_a_fraction_of_the_capital_base() {
    let p = policy(); // 0.5 * 5000 = 2500 (raw equity would allow 10000)
    let rich = account("20000", "20000", vec![]);
    let with = |t: &str| DayCounters { orders_today: 1, turnover_today: d(t) };
    let o = buy("SPY", "10", "100"); // 1000
    assert_allowed(&PreTradeGuard::check(&p, &rich, &o, &with("1500")));
    assert_only(&PreTradeGuard::check(&p, &rich, &o, &with("1500.01")), DenialCode::MaxTurnoverPerDay);
}

#[test]
fn the_cash_reserve_fraction_uses_the_capital_base_but_the_cash_test_uses_actual_cash() {
    let p = policy(); // reserve 5% of the base 5000 = 250 (raw equity would demand 1000)
    let rich_cash = account("20000", "1250", vec![]);
    assert_allowed(&check(&p, &rich_cash, &buy("SPY", "10", "100"))); // cash 250.00 exactly
    assert_only(&check(&p, &rich_cash, &buy("SPY", "10.0001", "100")), DenialCode::CashReserve); // 249.99
    // Big equity does not conjure cash: the account holds 100, so a 200 buy leaves -100.
    let little_cash = account("20000", "100", vec![]);
    assert_only(&check(&p, &little_cash, &buy("SPY", "2", "100")), DenialCode::CashReserve);
}

#[test]
fn equity_invalid_still_looks_at_broker_equity_not_the_allocation() {
    // A non-positive broker equity is refused even though the allocation is 5000.
    assert_only(&check(&policy(), &account("0", "5000", vec![]), &buy("SPY", "1", "100")), DenialCode::EquityInvalid);
}
