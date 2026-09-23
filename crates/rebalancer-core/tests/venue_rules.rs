//! Venue facts decide shorting; GROSS decides leverage (signed plans only).
//!
//! Spec: VENUE_FACTS.md, "What this changes in the code", items 1 and 2. A short is denied when the MANDATE forbids it or
//! when the venue facts for that instrument ([`InstrumentRules`], supplied by the caller) forbid it, are absent
//! (unknown, fail closed) or need a locate that was not supplied. Leverage is denied only when the projected gross
//! exceeds the effective cap `min(mandate leverage, mandate max gross, instrument max leverage)`. A venue's
//! `max_position_units` refuses an order whole. Buying power stays required for any order that needs margin.
//!
//! Mandate used (unless a test edits it): allocated 20000 USD, shorting ON, leverage 3x, max_gross 3, max_net 3,
//! max_position 3, order notional up to 20000, 50 orders and 5x turnover per day, 5% cash reserve (1000).
//! The property tests at the end are seeded SplitMix64 and re-derive every limit from the generated numbers; they never
//! call the guard to vouch for the guard.

mod common;

use std::collections::BTreeMap;

use broker_adapters::alpaca::{self, AssetTable};
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::Side;
use common::*;
use rebalancer_core::dec_math::{abs, add, div_floor, mul, neg, sub};
use rebalancer_core::guard::{
    AccountView, DenialCode, MarginContext, Position, PreTradeGuard, PricePoint, ProposedOrder, Verdict,
};
use rebalancer_core::planner::{OrderPlan, OrderPlanner, PlanConfig, PlanError, SleeveTarget, TargetWeight};
use rebalancer_core::policy::{Limits, Policy, PolicyBody};
use rebalancer_core::venue::{AlpacaRules, InstrumentRules, InstrumentVenueRule, KrakenRules, ShortPolicy, VenueRuleBook};
use rebalancer_core::Dec;
use serde_json::{json, Value};

const HUGE_BP: &str = "1000000000";

fn pol(edit: impl FnOnce(&mut Value)) -> Policy {
    policy_with(|m| {
        m["capital"]["allocated"]["amount"] = json!("20000.00");
        m["universe"]["instrument_allow"] = json!(["SPY", "EFA", "IEF", "DBC", "VNQ", "BTC/USD", "ETH/USD"]);
        m["universe"]["shorting"] = json!(true);
        m["universe"]["leverage_max_gross"] = json!(3.0);
        m["exposure"]["max_gross"] = json!(3.0);
        m["exposure"]["max_net"] = json!(3.0);
        m["exposure"]["max_position"] = json!(3.0);
        m["exposure"]["max_order_notional"]["amount"] = json!("20000.00");
        m["exposure"]["max_orders_per_day"] = json!(50);
        m["exposure"]["max_turnover_per_day"] = json!(5.0);
        edit(m);
    })
}

/// Mandate leverage limit `lev`, with the exposure caps (gross, net, position) all `cap`; the mandate validator
/// requires `cap <= lev`.
fn caps(lev: f64, cap: f64) -> Policy {
    pol(|m| {
        m["universe"]["leverage_max_gross"] = json!(lev);
        m["exposure"]["max_gross"] = json!(cap);
        m["exposure"]["max_net"] = json!(cap);
        m["exposure"]["max_position"] = json!(cap);
    })
}

fn one_x() -> Policy {
    caps(1.0, 1.0)
}

/// A compiled policy whose limit set is edited after compilation (a hand-built or loosened `Limits`).
fn patched(mut p: Policy, f: impl FnOnce(&mut Limits)) -> Policy {
    match &mut p.body {
        PolicyBody::Valid(l) => f(l),
        PolicyBody::Invalid(r) => panic!("policy is invalid: {r:?}"),
    }
    p
}

fn flat() -> AccountView {
    account("20000", "20000", vec![])
}

/// Equity 20000 with 25000 of gross already held (SPY 20000, EFA 5000), cash 0: 1.25x levered.
fn levered() -> AccountView {
    account("20000", "0", vec![pos("SPY", "40", "20000"), pos("EFA", "62.5", "5000")])
}

fn cs(p: &Policy, a: &AccountView, o: &ProposedOrder, bp: Option<&str>, r: &InstrumentRules) -> Verdict {
    PreTradeGuard::check_signed(p, a, o, &day0(), &MarginContext { buying_power_left: bp.map(d) }, r)
}

fn allowed(sym: &str) -> InstrumentRules {
    InstrumentRules::new().with_rule("alpaca", sym, InstrumentVenueRule::shortable())
}

fn only(v: &Verdict, code: DenialCode) {
    assert_eq!(v.codes(), vec![code], "{:?}", v.reasons);
}

// ---------------------------------------------------------------------------------------------------------------
// Shorting: the mandate AND the venue facts
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_short_within_1x_gross_is_allowed_when_the_venue_allows_and_the_mandate_permits() {
    let v = cs(&one_x(), &flat(), &sell("EFA", "25", "80"), Some("50000"), &allowed("EFA"));
    assert!(v.allow, "{:?}", v.reasons);
    // The same order is margin use as DATA, and is still fine: leverage is a gross test.
    assert_eq!(rebalancer_core::guard::margin_use(&flat(), &sell("EFA", "25", "80"), d("80")), Ok(true));
}

#[test]
fn a_short_is_denied_when_the_mandate_forbids_shorting_whatever_the_venue_says() {
    let p = pol(|m| m["universe"]["shorting"] = json!(false));
    let v = cs(&p, &flat(), &sell("EFA", "25", "80"), Some("50000"), &allowed("EFA"));
    only(&v, DenialCode::ShortingForbidden);
    assert!(v.reasons[0].message.contains("shorting is off"), "{}", v.reasons[0].message);
    // Mandate off AND venue forbids: still ONE denial, and it names both causes.
    let r = InstrumentRules::new().with_rule("alpaca", "EFA", InstrumentVenueRule::never_shortable("not shortable"));
    let v = cs(&p, &flat(), &sell("EFA", "25", "80"), Some("50000"), &r);
    only(&v, DenialCode::ShortingForbidden);
    assert!(v.reasons[0].message.contains("shorting is off") && v.reasons[0].message.contains("not shortable"), "{}", v.reasons[0].message);
}

#[test]
fn a_short_is_denied_when_the_venue_rule_forbids_it() {
    let r = InstrumentRules::new().with_rule("alpaca", "EFA", InstrumentVenueRule::never_shortable("asset is not shortable"));
    let v = cs(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), Some("50000"), &r);
    only(&v, DenialCode::ShortingForbidden);
    assert!(v.reasons[0].message.contains("asset is not shortable"), "{}", v.reasons[0].message);
    // A rule for ANOTHER instrument or venue does not help.
    let other = InstrumentRules::new()
        .with_rule("alpaca", "SPY", InstrumentVenueRule::shortable())
        .with_rule("kraken", "EFA", InstrumentVenueRule::shortable());
    only(&cs(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), Some("50000"), &other), DenialCode::ShortingForbidden);
}

#[test]
fn a_short_with_no_venue_rule_is_denied_fail_closed() {
    let v = cs(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), Some("50000"), &InstrumentRules::new());
    only(&v, DenialCode::ShortingForbidden);
    assert!(v.reasons[0].message.contains("no venue rule"), "{}", v.reasons[0].message);
    // `check_margin` supplies no venue facts, so it denies the same short.
    let v = PreTradeGuard::check_margin(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), &day0(), &MarginContext { buying_power_left: Some(d("50000")) });
    only(&v, DenialCode::ShortingForbidden);
    // Keys are normalised: case and whitespace do not matter.
    let r = InstrumentRules::new().with_rule(" Alpaca ", " efa ", InstrumentVenueRule::shortable());
    assert!(cs(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), Some("50000"), &r).allow);
}

#[test]
fn a_short_that_needs_a_locate_is_denied_without_one() {
    let need = InstrumentRules::new().with_rule("alpaca", "EFA", InstrumentVenueRule::short_needs_locate());
    let v = cs(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), Some("50000"), &need);
    only(&v, DenialCode::ShortingForbidden);
    assert!(v.reasons[0].message.contains("locate"), "{}", v.reasons[0].message);
    // A locate for a different instrument does not count.
    let wrong = need.clone().with_locate("alpaca", "SPY");
    only(&cs(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), Some("50000"), &wrong), DenialCode::ShortingForbidden);
    // With the locate on record it goes through.
    let located = need.with_locate("alpaca", "EFA");
    assert!(cs(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), Some("50000"), &located).allow);
    // A locate alone (no rule) is not permission.
    let only_locate = InstrumentRules::new().with_locate("alpaca", "EFA");
    only(&cs(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), Some("50000"), &only_locate), DenialCode::ShortingForbidden);
    // A locate does not override a Forbidden rule either.
    let forbidden = InstrumentRules::new()
        .with_rule("alpaca", "EFA", InstrumentVenueRule::never_shortable("no"))
        .with_locate("alpaca", "EFA");
    only(&cs(&pol(|_| {}), &flat(), &sell("EFA", "25", "80"), Some("50000"), &forbidden), DenialCode::ShortingForbidden);
}

#[test]
fn a_never_shortable_instrument_is_refused_but_longs_and_reductions_need_no_rule() {
    // Crypto can never be sold short (Alpaca): a Forbidden rule.
    let r = InstrumentRules::new().with_rule("kraken", "BTC/USD", InstrumentVenueRule::never_shortable("crypto cannot be sold short"));
    let p = pol(|_| {});
    let v = cs(&p, &flat(), &sell("BTC/USD", "0.1", "60000"), Some("50000"), &r);
    only(&v, DenialCode::ShortingForbidden);
    assert!(v.reasons[0].message.contains("crypto cannot be sold short"), "{}", v.reasons[0].message);
    // Buying it needs no rule at all (no venue fact is consulted for a long).
    assert!(cs(&p, &flat(), &buy("BTC/USD", "0.1", "60000"), Some("50000"), &InstrumentRules::new()).allow);
    // Selling what is held is a reduction: it needs no rule and consults none.
    let held = account("20000", "14000", vec![pos("BTC/USD", "0.1", "6000")]);
    assert!(cs(&p, &held, &sell("BTC/USD", "0.1", "60000"), None, &InstrumentRules::new()).allow);
    // One unit more than held opens a short (a single-order flip): refused.
    only(&cs(&p, &held, &sell("BTC/USD", "0.10001", "60000"), Some("50000"), &r), DenialCode::ShortingForbidden);
    // Deepening an existing short is opening a short too.
    let short = account("20000", "26000", vec![pos("EFA", "-25", "-2000")]);
    only(&cs(&p, &short, &sell("EFA", "1", "80"), Some("50000"), &InstrumentRules::new()), DenialCode::ShortingForbidden);
    // Covering a short is a reduction and needs no rule.
    assert!(cs(&p, &short, &buy("EFA", "25", "80"), None, &InstrumentRules::new()).allow);
}

// ---------------------------------------------------------------------------------------------------------------
// Leverage: only when GROSS exceeds the effective cap; the cap is the lowest of three sources
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_short_is_not_leverage_but_gross_above_the_cap_is() {
    // 1x mandate. Holds SPY 15000 long; selling 62.5 EFA (5000) reaches gross 20000 = the cap exactly: allowed.
    let a = account("20000", "5000", vec![pos("SPY", "30", "15000")]);
    let ok = cs(&one_x(), &a, &sell("EFA", "62.5", "80"), Some("50000"), &allowed("EFA"));
    assert!(ok.allow, "{:?}", ok.reasons);
    // One share more takes gross to 20000.80: LEVERAGE_FORBIDDEN (and the mandate's own MAX_GROSS).
    let v = cs(&one_x(), &a, &sell("EFA", "62.51", "80"), Some("50000"), &allowed("EFA"));
    assert!(v.has(DenialCode::LeverageForbidden), "{:?}", v.reasons);
    // A reduction is never leverage even when the book is already above the cap.
    let over = account("20000", "-5000", vec![pos("SPY", "50", "25000")]);
    assert!(cs(&one_x(), &over, &sell("SPY", "10", "500"), None, &InstrumentRules::new()).allow);
}

fn buy_ief(qty: &str) -> ProposedOrder {
    buy("IEF", qty, "100")
}

#[test]
fn the_effective_cap_takes_the_mandates_leverage_limit() {
    // Leverage limit 1.5 (cap 30000). The compiled limit set is then loosened everywhere else (as a hand-built
    // `Limits` could be), so only `leverage_max_gross` is left to bind.
    let p = patched(caps(1.5, 1.5), |l| {
        l.max_gross = d("3");
        l.max_net = d("3");
        l.max_position = d("3");
    });
    let ok = cs(&p, &levered(), &buy_ief("50"), Some(HUGE_BP), &InstrumentRules::new()); // gross 30000.00
    assert!(ok.allow, "{:?}", ok.reasons);
    let v = cs(&p, &levered(), &buy_ief("50.01"), Some(HUGE_BP), &InstrumentRules::new());
    only(&v, DenialCode::LeverageForbidden);
    // Compiled normally, the same limit also shows up as the mandate's MAX_GROSS.
    let v = cs(&caps(1.5, 1.5), &levered(), &buy_ief("50.01"), Some(HUGE_BP), &InstrumentRules::new());
    assert!(v.has(DenialCode::LeverageForbidden) && v.has(DenialCode::MaxGross), "{:?}", v.reasons);
}

#[test]
fn the_effective_cap_takes_the_mandates_exposure_max_gross() {
    // leverage limit 3, exposure.max_gross 1.5: cap 30000.
    let p = caps(3.0, 1.5);
    assert!(cs(&p, &levered(), &buy_ief("50"), Some(HUGE_BP), &InstrumentRules::new()).allow);
    let v = cs(&p, &levered(), &buy_ief("50.01"), Some(HUGE_BP), &InstrumentRules::new());
    assert!(v.has(DenialCode::LeverageForbidden), "{:?}", v.reasons);
    // And a limit set whose leverage_max_gross was loosened but whose max_gross is 1.5 is capped by max_gross alone.
    let p2 = patched(caps(1.5, 1.5), |l| l.leverage_max_gross = d("3"));
    assert!(cs(&p2, &levered(), &buy_ief("50.01"), Some(HUGE_BP), &InstrumentRules::new()).has(DenialCode::LeverageForbidden));
}

#[test]
fn the_effective_cap_takes_the_instruments_venue_max_leverage() {
    // Mandate 3x (60000), IEF's venue rule says 1.5x (30000): the venue cap binds for IEF only.
    let r = InstrumentRules::new().with_rule("alpaca", "IEF", InstrumentVenueRule::shortable().with_max_leverage(d("1.5")));
    let p = pol(|_| {});
    assert!(cs(&p, &levered(), &buy_ief("50"), Some(HUGE_BP), &r).allow);
    let v = cs(&p, &levered(), &buy_ief("50.01"), Some(HUGE_BP), &r);
    only(&v, DenialCode::LeverageForbidden);
    assert!(!v.has(DenialCode::MaxGross), "the mandate's own cap (3x) is not what binds");
    assert!(v.reasons[0].message.contains("1.5"), "{}", v.reasons[0].message);
    // An instrument with no venue cap (DBC) reaches the same gross under the mandate's 3x.
    assert!(cs(&p, &levered(), &buy("DBC", "200.4", "25"), Some(HUGE_BP), &r).allow);
    // A venue cap LOOSER than the mandate cannot loosen it.
    let loose = InstrumentRules::new().with_rule("alpaca", "IEF", InstrumentVenueRule::shortable().with_max_leverage(d("10")));
    assert!(cs(&caps(1.5, 1.5),&levered(), &buy_ief("50.01"), Some(HUGE_BP), &loose).has(DenialCode::LeverageForbidden));
}

// ---------------------------------------------------------------------------------------------------------------
// The venue's position-size limit refuses, never clips
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn max_position_units_refuses_an_order_that_would_exceed_it() {
    let r = InstrumentRules::new().with_rule("alpaca", "SPY", InstrumentVenueRule::shortable().with_max_position_units(d("30")));
    let p = pol(|_| {});
    // Long side: 30 units exactly is fine, a hair over is refused with MAX_POSITION.
    assert!(cs(&p, &flat(), &buy("SPY", "30", "500"), Some(HUGE_BP), &r).allow);
    let v = cs(&p, &flat(), &buy("SPY", "30.01", "500"), Some(HUGE_BP), &r);
    only(&v, DenialCode::MaxPosition);
    assert!(v.reasons[0].message.contains("maximum position size"), "{}", v.reasons[0].message);
    // The existing position counts.
    let held = account("20000", "10000", vec![pos("SPY", "20", "10000")]);
    assert!(cs(&p, &held, &buy("SPY", "10", "500"), Some(HUGE_BP), &r).allow);
    only(&cs(&p, &held, &buy("SPY", "10.01", "500"), Some(HUGE_BP), &r), DenialCode::MaxPosition);
    // Short side: the absolute position is bounded too.
    assert!(cs(&p, &flat(), &sell("SPY", "30", "500"), Some(HUGE_BP), &r).allow);
    only(&cs(&p, &flat(), &sell("SPY", "30.01", "500"), Some(HUGE_BP), &r), DenialCode::MaxPosition);
    // A position already over the venue limit can still be reduced.
    let over = account("20000", "0", vec![pos("SPY", "40", "20000")]);
    assert!(cs(&p, &over, &sell("SPY", "1", "500"), None, &r).allow);
    // No limit for an instrument without one.
    assert!(cs(&p, &flat(), &buy("EFA", "100", "80"), Some(HUGE_BP), &r).allow);
}

// ---------------------------------------------------------------------------------------------------------------
// Buying power stays required for margin
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn buying_power_is_still_required_and_is_the_third_limit() {
    let p = pol(|_| {}); // reserve 5% of 20000 = 1000
    let r = allowed("EFA");
    let short = sell("EFA", "25", "80"); // 2000
    // No figure, an order that needs margin: refused.
    only(&cs(&p, &flat(), &short, None, &r), DenialCode::CashReserve);
    // A levered long with no figure is refused too (gross 25800 > equity 20000).
    only(&cs(&p, &levered(), &buy("EFA", "10", "80"), None, &InstrumentRules::new()), DenialCode::CashReserve);
    // With a figure, the broker's number is the limit even when the mandate and the venue allow more.
    assert!(cs(&p, &flat(), &short, Some("3000"), &r).allow);
    only(&cs(&p, &flat(), &short, Some("2999.99999999"), &r), DenialCode::CashReserve);
    // A long inside equity needs none.
    assert!(cs(&p, &flat(), &buy("SPY", "4", "500"), None, &InstrumentRules::new()).allow);
}

// ---------------------------------------------------------------------------------------------------------------
// The planner carries the rule book into the guard
// ---------------------------------------------------------------------------------------------------------------

struct Env {
    pairs: PairTable,
    assets: AssetTable,
    opts: alpaca::PrepareOptions,
}

impl Env {
    fn new() -> Env {
        Env {
            pairs: PairTable::builtin(),
            assets: AssetTable::builtin(),
            opts: alpaca::PrepareOptions { allow_extended_hours: false, min_notional: d("1"), own_tag_prefix: None, refuse_builtin_assets: false },
        }
    }

    fn plan(&self, targets: &[SleeveTarget], account: &AccountView, policy: &Policy, cfg: &PlanConfig, rules: InstrumentRules) -> Result<OrderPlan, PlanError> {
        let kraken = KrakenRules { pairs: &self.pairs };
        let alpaca = AlpacaRules { assets: &self.assets, options: &self.opts };
        let book = VenueRuleBook::new().with("kraken", &kraken).with("alpaca", &alpaca).with_instrument_rules(rules);
        OrderPlanner::plan(targets, account, &plan_prices(), &book, policy, cfg)
    }

    fn ok(&self, targets: &[SleeveTarget], account: &AccountView, policy: &Policy, cfg: &PlanConfig, rules: InstrumentRules) -> OrderPlan {
        self.plan(targets, account, policy, cfg, rules).unwrap_or_else(|e| panic!("plan failed: {e}"))
    }
}

fn plan_prices() -> BTreeMap<String, PricePoint> {
    [("SPY", "500"), ("EFA", "80"), ("IEF", "95"), ("DBC", "25"), ("VNQ", "90"), ("BTC/USD", "60000"), ("ETH/USD", "3000")]
        .iter()
        .map(|(s, p)| (s.to_string(), PricePoint { price: d(p), as_of: now() }))
        .collect()
}

fn sleeve(id: &str, venue: &str, class: &str, weights: &[(&str, &str)]) -> Vec<SleeveTarget> {
    vec![SleeveTarget {
        sleeve: id.into(),
        share: d("1"),
        venue: venue.into(),
        asset_class: class.into(),
        weights: weights.iter().map(|(s, w)| TargetWeight { symbol: s.to_string(), weight: d(w) }).collect(),
    }]
}

fn ls(weights: &[(&str, &str)]) -> Vec<SleeveTarget> {
    sleeve("ls", "alpaca", "us_etf", weights)
}

fn cfg() -> PlanConfig {
    let mut c = PlanConfig::new(at("2026-10-01T14:00:00Z"), d("1"), d("10"), d("0.02"), d("0.0025")).with_signed_sleeve("ls", d("2"));
    c.buying_power = Some(d("100000"));
    c
}

fn denial_codes(plan: &OrderPlan) -> Vec<DenialCode> {
    plan.denied.iter().flat_map(|x| x.reasons.iter().map(|r| r.code)).collect()
}

fn brief(plan: &OrderPlan) -> Vec<(String, Side, String)> {
    plan.orders.iter().map(|o| (o.symbol.clone(), o.side, o.quantity.to_string())).collect()
}

#[test]
fn the_planner_denies_a_short_with_no_venue_rule_and_still_places_the_long() {
    let env = Env::new();
    let plan = env.ok(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &flat(), &pol(|_| {}), &cfg(), InstrumentRules::new());
    assert_eq!(denial_codes(&plan), vec![DenialCode::ShortingForbidden], "{:#?}", plan.denied);
    assert_eq!(plan.denied[0].order.symbol, "EFA");
    assert_eq!(brief(&plan), vec![("SPY".to_string(), Side::Buy, "4".to_string())]);
    // With the venue's fact supplied, the same plan places both, at a 1x mandate.
    let both = env.ok(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &flat(), &one_x(), &cfg(), allowed("EFA"));
    assert!(both.denied.is_empty(), "{:#?}", both.denied);
    assert_eq!(both.orders.len(), 2);
    // The venue facts are an input: they change the digest.
    assert_ne!(plan.inputs_digest, env.ok(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &flat(), &pol(|_| {}), &cfg(), allowed("EFA")).inputs_digest);
}

#[test]
fn the_planner_honours_locates_and_never_shortable_crypto() {
    let env = Env::new();
    let need = InstrumentRules::new().with_rule("alpaca", "EFA", InstrumentVenueRule::short_needs_locate());
    let targets = ls(&[("EFA", "-0.1")]);
    let plan = env.ok(&targets, &flat(), &pol(|_| {}), &cfg(), need.clone());
    assert_eq!(denial_codes(&plan), vec![DenialCode::ShortingForbidden]);
    assert!(plan.orders.is_empty());
    let plan = env.ok(&targets, &flat(), &pol(|_| {}), &cfg(), need.with_locate("alpaca", "EFA"));
    assert!(plan.denied.is_empty() && plan.orders.len() == 1, "{:#?}", plan);
    // A crypto sleeve that wants BTC/USD short: the venue says never.
    let mut c = PlanConfig::new(at("2026-10-01T14:00:00Z"), d("1"), d("10"), d("0.02"), d("0.0025")).with_signed_sleeve("cx", d("1"));
    c.buying_power = Some(d("100000"));
    let r = InstrumentRules::new().with_rule("kraken", "BTC/USD", InstrumentVenueRule::never_shortable("crypto cannot be sold short"));
    let plan = env.ok(&sleeve("cx", "kraken", "crypto_spot", &[("BTC/USD", "-0.5")]), &flat(), &pol(|_| {}), &c, r);
    assert_eq!(denial_codes(&plan), vec![DenialCode::ShortingForbidden], "{:#?}", plan);
    assert!(plan.orders.is_empty());
}

#[test]
fn the_planner_refuses_an_order_above_the_venue_position_limit_instead_of_clipping_it() {
    let env = Env::new();
    // Wants 4 SPY (2000); the venue allows 3 units. The order is refused whole: no 3-share order appears.
    let r = InstrumentRules::new().with_rule("alpaca", "SPY", InstrumentVenueRule::shortable().with_max_position_units(d("3")));
    let plan = env.ok(&ls(&[("SPY", "0.1")]), &flat(), &pol(|_| {}), &cfg(), r);
    assert!(plan.orders.is_empty(), "{:#?}", plan.orders);
    assert_eq!(denial_codes(&plan), vec![DenialCode::MaxPosition]);
    assert_eq!(plan.denied[0].order.quantity, d("4"));
}

#[test]
fn the_planner_applies_the_instruments_venue_leverage_cap_and_buying_power_stays_required() {
    let env = Env::new();
    // +-0.9: gross 36000 (1.8x). EFA is sold first (symbol order), then SPY is bought to gross 36000 > SPY's venue cap 1.5x.
    let r = InstrumentRules::new()
        .with_rule("alpaca", "EFA", InstrumentVenueRule::shortable())
        .with_rule("alpaca", "SPY", InstrumentVenueRule::shortable().with_max_leverage(d("1.5")));
    let plan = env.ok(&ls(&[("SPY", "0.9"), ("EFA", "-0.9")]), &flat(), &pol(|_| {}), &cfg(), r.clone());
    assert_eq!(denial_codes(&plan), vec![DenialCode::LeverageForbidden], "{:#?}", plan.denied);
    assert_eq!(plan.denied[0].order.symbol, "SPY");
    assert_eq!(brief(&plan), vec![("EFA".to_string(), Side::Sell, "225".to_string())]);
    // Buying power is still required for a plan with a short target, venue rules or not.
    let mut no_bp = cfg();
    no_bp.buying_power = None;
    let e = env.plan(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &flat(), &pol(|_| {}), &no_bp, r).unwrap_err();
    assert!(matches!(e, PlanError::BuyingPowerRequired { .. }), "{e:?}");
}

// ---------------------------------------------------------------------------------------------------------------
// Property tests (seeded SplitMix64; every case is reproducible from its seed)
// ---------------------------------------------------------------------------------------------------------------

const ETFS: [&str; 5] = ["SPY", "EFA", "IEF", "DBC", "VNQ"];
const CASES: u64 = 600;

fn dec_of(f: f64) -> Dec {
    Dec::parse(&format!("{f}")).unwrap()
}

fn px(rng: &mut SplitMix64, s: &str) -> Dec {
    let (lo, hi) = match s {
        "SPY" => (300, 600),
        "EFA" => (60, 100),
        "IEF" => (80, 110),
        "DBC" => (20, 30),
        _ => (70, 110),
    };
    d(&format!("{}.{:02}", rng.range(lo, hi), rng.range(0, 99)))
}

#[derive(Clone)]
struct Spec {
    rule: Option<InstrumentVenueRule>,
    locate: bool,
}

impl Spec {
    /// May this instrument be sold short as far as the venue facts go?
    fn short_ok(&self) -> bool {
        match &self.rule {
            None => false,
            Some(r) => match &r.short {
                ShortPolicy::Allowed => true,
                ShortPolicy::NeedsLocate => self.locate,
                ShortPolicy::Forbidden { .. } => false,
            },
        }
    }

    fn max_leverage(&self) -> Option<Dec> {
        self.rule.as_ref().and_then(|r| r.max_leverage)
    }

    fn max_units(&self) -> Option<Dec> {
        self.rule.as_ref().and_then(|r| r.max_position_units)
    }
}

fn random_specs(rng: &mut SplitMix64, tight: bool) -> BTreeMap<String, Spec> {
    let mut m = BTreeMap::new();
    for s in ETFS {
        let mut rule = match rng.range(0, 9) {
            0 | 1 => None,
            2 => Some(InstrumentVenueRule::never_shortable("not shortable")),
            3..=6 => Some(InstrumentVenueRule::shortable()),
            _ => Some(InstrumentVenueRule::short_needs_locate()),
        };
        let locate = rng.chance(50);
        if let Some(r) = rule.as_mut() {
            if rng.chance(30) {
                r.max_leverage = Some(dec_of(*rng.pick(&[0.5, 1.0, 1.5, 2.0])));
            }
            if rng.chance(if tight { 60 } else { 30 }) {
                r.max_position_units = Some(Dec::from_i64(rng.range(if tight { 1 } else { 5 }, 60) as i64));
            }
        }
        m.insert(s.to_string(), Spec { rule, locate });
    }
    m
}

fn rules_of(specs: &BTreeMap<String, Spec>) -> InstrumentRules {
    let mut r = InstrumentRules::new();
    for (s, sp) in specs {
        if let Some(rule) = &sp.rule {
            r = r.with_rule("alpaca", s, rule.clone());
        }
        if sp.locate {
            r = r.with_locate("alpaca", s);
        }
    }
    r
}

struct Case {
    seed: u64,
    shorting: bool,
    lev: f64,
    max_gross: f64,
    cb: Dec,
    policy: Policy,
    account: AccountView,
    prices: BTreeMap<String, PricePoint>,
    targets: Vec<SleeveTarget>,
    cfg: PlanConfig,
    specs: BTreeMap<String, Spec>,
}

fn generate(seed: u64) -> Case {
    let mut rng = SplitMix64(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0x0BAD_5EED);
    let shorting = rng.chance(80);
    let lev = *rng.pick(&[1.0, 1.5, 2.0, 3.0]);
    // The mandate validator requires max_gross (and net, position) <= the leverage limit.
    let max_gross = *rng.pick(&[1.0, 1.5, 2.0, 3.0].iter().copied().filter(|g| *g <= lev).collect::<Vec<f64>>());
    let policy = pol(|m| {
        m["universe"]["shorting"] = json!(shorting);
        m["universe"]["leverage_max_gross"] = json!(lev);
        m["exposure"]["max_gross"] = json!(max_gross);
        m["exposure"]["max_net"] = json!(max_gross);
        m["exposure"]["max_position"] = json!(max_gross);
        m["capital"]["min_cash_reserve"] = json!(0.0);
        m["exposure"]["max_turnover_per_day"] = json!(50.0);
        m["exposure"]["max_orders_per_day"] = json!(1000);
    });
    let equity: u64 = rng.range(15_000, 30_000);
    let cb = d(&equity.min(20_000).to_string());
    let mut prices = BTreeMap::new();
    for s in ETFS {
        prices.insert(s.to_string(), PricePoint { price: px(&mut rng, s), as_of: now() });
    }
    let mut positions: Vec<Position> = Vec::new();
    let mut invested = Dec::ZERO;
    for s in ETFS {
        if !rng.chance(35) {
            continue;
        }
        let p = prices[s].price;
        let pct = rng.range(1, 25);
        let mut qty = div_floor(mul(cb, d(&format!("0.{pct:02}"))).unwrap(), p, 6).unwrap();
        if qty.is_zero() {
            continue;
        }
        if rng.chance(35) {
            qty = neg(qty).unwrap();
        }
        let mv = mul(qty, p).unwrap();
        invested = add(invested, mv).unwrap();
        positions.push(pos(s, &qty.to_string(), &mv.to_string()));
    }
    let equity_dec = d(&equity.to_string());
    let mut cash = sub(equity_dec, invested).unwrap();
    if cash.is_negative() {
        cash = Dec::ZERO;
    }
    let account = AccountView { account_id: format!("acct-{seed}"), ccy: "USD".into(), equity: equity_dec, cash, positions, halted: false, now: now() };
    let mut weights = Vec::new();
    for s in ETFS {
        let mag = d(*rng.pick(&["0", "0", "0.1", "0.3", "0.5", "0.8"]));
        weights.push(TargetWeight { symbol: s.to_string(), weight: if rng.chance(45) { neg(mag).unwrap() } else { mag } });
    }
    let targets = vec![SleeveTarget { sleeve: "ls".into(), share: d("1"), venue: "alpaca".into(), asset_class: "us_etf".into(), weights }];
    let mut cfg = PlanConfig::new(at("2026-10-01T14:00:00Z"), d(*rng.pick(&["1", "0.5"])), d("0"), d("0"), d("0.001")).with_signed_sleeve("ls", d("1"));
    cfg.buying_power = Some(d(HUGE_BP));
    let tight = rng.chance(30);
    let specs = random_specs(&mut rng, tight);
    Case { seed, shorting, lev, max_gross, cb, policy, account, prices, targets, cfg, specs }
}

fn run(env: &Env, c: &Case, rules: InstrumentRules) -> Result<OrderPlan, PlanError> {
    let kraken = KrakenRules { pairs: &env.pairs };
    let alpaca = AlpacaRules { assets: &env.assets, options: &env.opts };
    let book = VenueRuleBook::new().with("kraken", &kraken).with("alpaca", &alpaca).with_instrument_rules(rules);
    OrderPlanner::plan(&c.targets, &c.account, &c.prices, &book, &c.policy, &c.cfg)
}

/// Replay the accepted orders with plain decimal arithmetic and check every venue/mandate property.
fn oracle(c: &Case, plan: &OrderPlan) -> Result<(), String> {
    let mut qty: BTreeMap<String, Dec> = c.account.positions.iter().map(|p| (p.symbol.clone(), p.quantity)).collect();
    for o in &plan.orders {
        let ctx = |what: &str| format!("seed {}: {what} for {o:?}", c.seed);
        let spec = c.specs.get(&o.symbol).ok_or_else(|| ctx("unknown symbol"))?;
        let held = qty.get(&o.symbol).copied().unwrap_or(Dec::ZERO);
        let long_held = if held.is_positive() { held } else { Dec::ZERO };
        let opens_short = o.side == Side::Sell && o.quantity > long_held;
        if opens_short {
            if !c.shorting {
                return Err(ctx("a short was opened although the mandate forbids shorting"));
            }
            if !spec.short_ok() {
                return Err(ctx("a short was opened although the venue rule is not Allowed (or NeedsLocate with a locate)"));
            }
        }
        let reducing = match o.side {
            Side::Sell => held.is_positive() && o.quantity <= held,
            Side::Buy => held.is_negative() && o.quantity <= neg(held).unwrap(),
        };
        let after_qty = if o.side == Side::Buy { add(held, o.quantity).unwrap() } else { sub(held, o.quantity).unwrap() };
        if !reducing {
            // Effective gross cap: the lowest of mandate leverage, mandate max gross, the instrument's venue cap.
            let mut mult = dec_of(c.lev).min(dec_of(c.max_gross));
            if let Some(v) = spec.max_leverage() {
                mult = mult.min(v);
            }
            let cap = mul(mult, c.cb).unwrap();
            let mut gross = abs(mul(after_qty, c.prices[&o.symbol].price).unwrap()).unwrap();
            for (s, q) in &qty {
                if *s != o.symbol {
                    gross = add(gross, abs(mul(*q, c.prices[s].price).unwrap()).unwrap()).unwrap();
                }
            }
            if gross > cap {
                return Err(ctx(&format!("gross {gross} above the effective cap {cap}")));
            }
            if let Some(u) = spec.max_units() {
                if abs(after_qty).unwrap() > u {
                    return Err(ctx(&format!("position {after_qty} above the venue's max_position_units {u}")));
                }
            }
        }
        qty.insert(o.symbol.clone(), after_qty);
    }
    // A SHORTING_FORBIDDEN denial must be explained by the mandate or by the venue facts.
    for dn in &plan.denied {
        if dn.reasons.iter().any(|r| r.code == DenialCode::ShortingForbidden) {
            let spec = &c.specs[&dn.order.symbol];
            if c.shorting && spec.short_ok() {
                return Err(format!("seed {}: SHORTING_FORBIDDEN for {:?} although mandate and venue allow it", c.seed, dn.order));
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct Cov {
    plans: u32,
    shorts_opened: u32,
    venue_short_denials: u32,
    leverage_denials: u32,
    unit_denials: u32,
    located_shorts: u32,
}

#[test]
fn no_signed_plan_shorts_against_the_venue_rules_or_breaks_the_effective_cap() {
    let env = Env::new();
    let mut cov = Cov::default();
    for seed in 0..CASES {
        let c = generate(seed);
        let Ok(plan) = run(&env, &c, rules_of(&c.specs)) else { continue };
        if let Err(msg) = oracle(&c, &plan) {
            panic!("{msg}\nplan: {plan:#?}");
        }
        cov.plans += 1;
        for o in &plan.orders {
            let held = c.account.position(&o.symbol).map_or(Dec::ZERO, |p| p.quantity);
            if o.side == Side::Sell && o.quantity > std::cmp::max(held, Dec::ZERO) {
                cov.shorts_opened += 1;
                cov.located_shorts += u32::from(matches!(c.specs[&o.symbol].rule.as_ref().map(|r| &r.short), Some(ShortPolicy::NeedsLocate)));
            }
        }
        cov.venue_short_denials += u32::from(plan.denied.iter().any(|dn| c.shorting && !c.specs[&dn.order.symbol].short_ok() && dn.reasons.iter().any(|r| r.code == DenialCode::ShortingForbidden)));
        cov.leverage_denials += u32::from(plan.denied.iter().any(|dn| dn.reasons.iter().any(|r| r.code == DenialCode::LeverageForbidden)));
        cov.unit_denials += u32::from(plan.denied.iter().any(|dn| dn.reasons.iter().any(|r| r.code == DenialCode::MaxPosition)));
    }
    // The generator must actually exercise every branch, or the oracle proves little.
    assert!(cov.plans > 300, "too few plans: {}", cov.plans);
    assert!(cov.shorts_opened > 60, "too few shorts opened: {}", cov.shorts_opened);
    assert!(cov.located_shorts > 5, "too few located shorts: {}", cov.located_shorts);
    assert!(cov.venue_short_denials > 40, "too few venue short denials: {}", cov.venue_short_denials);
    assert!(cov.leverage_denials > 10, "too few leverage denials: {}", cov.leverage_denials);
    assert!(cov.unit_denials > 10, "too few position-unit denials: {}", cov.unit_denials);
}

#[test]
fn removing_every_venue_fact_can_only_remove_shorts_never_add_them() {
    // With no venue facts at all, no plan may contain a short; the same case with facts may.
    let env = Env::new();
    let mut compared = 0;
    for seed in 0..CASES {
        let c = generate(seed);
        let Ok(bare) = run(&env, &c, InstrumentRules::new()) else { continue };
        for o in &bare.orders {
            let held = c.account.position(&o.symbol).map_or(Dec::ZERO, |p| p.quantity);
            assert!(!(o.side == Side::Sell && o.quantity > std::cmp::max(held, Dec::ZERO)), "seed {seed}: a short was planned with NO venue facts: {o:?}");
        }
        compared += 1;
    }
    assert!(compared > 300, "too few comparable cases: {compared}");
}

#[test]
fn the_long_only_path_ignores_venue_facts_entirely() {
    let env = Env::new();
    let mut compared = 0;
    for seed in 0..CASES {
        let mut rng = SplitMix64(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x10_4761);
        let policy = pol(|m| {
            m["universe"]["shorting"] = json!(false);
            m["universe"]["leverage_max_gross"] = json!(1.0);
            m["exposure"]["max_gross"] = json!(1.0);
            m["exposure"]["max_position"] = json!(*rng.pick(&[0.3, 0.6, 1.0]));
        });
        let equity: u64 = rng.range(15_000, 30_000);
        let prices: BTreeMap<String, PricePoint> = ETFS.iter().map(|s| (s.to_string(), PricePoint { price: px(&mut rng, s), as_of: now() })).collect();
        let mut positions = Vec::new();
        let mut invested = Dec::ZERO;
        for s in ETFS {
            if rng.chance(40) {
                let p = prices[s].price;
                let q = div_floor(mul(d("2000"), d(&format!("{}", rng.range(1, 3)))).unwrap(), p, 4).unwrap();
                let mv = mul(q, p).unwrap();
                invested = add(invested, mv).unwrap();
                positions.push(pos(s, &q.to_string(), &mv.to_string()));
            }
        }
        let equity_dec = d(&equity.to_string());
        let cash = std::cmp::max(sub(equity_dec, invested).unwrap(), Dec::ZERO);
        let account = AccountView { account_id: format!("acct-{seed}"), ccy: "USD".into(), equity: equity_dec, cash, positions, halted: false, now: now() };
        let weights: Vec<TargetWeight> = ETFS.iter().map(|s| TargetWeight { symbol: s.to_string(), weight: d(*rng.pick(&["0", "0", "0.1", "0.2"])) }).collect();
        let targets = vec![SleeveTarget { sleeve: "etf".into(), share: d("1"), venue: "alpaca".into(), asset_class: "us_etf".into(), weights }];
        let plain = PlanConfig::new(at("2026-10-01T14:00:00Z"), d(*rng.pick(&["1", "0.5"])), d("5"), d("0.02"), d("0.001"));
        let specs = random_specs(&mut rng, true);
        let kraken = KrakenRules { pairs: &env.pairs };
        let alpaca = AlpacaRules { assets: &env.assets, options: &env.opts };
        let bare = VenueRuleBook::new().with("kraken", &kraken).with("alpaca", &alpaca);
        let rich = VenueRuleBook::new().with("kraken", &kraken).with("alpaca", &alpaca).with_instrument_rules(rules_of(&specs));
        let a = OrderPlanner::plan(&targets, &account, &prices, &bare, &policy, &plain);
        let b = OrderPlanner::plan(&targets, &account, &prices, &rich, &policy, &plain);
        assert_eq!(a, b, "seed {seed}: venue facts changed a long-only plan");
        compared += u32::from(a.is_ok());
    }
    assert!(compared > 400, "too few valid long-only plans: {compared}");
}
