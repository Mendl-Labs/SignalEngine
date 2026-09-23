//! Signed sleeves (shorts, gross above 1x): opt-in planner and guard behaviour, against the adapters' own rule tables.
//!
//! Mandate used throughout (unless a test edits it): allocated 20000 USD, shorting ON, leverage 3x, max_gross 3,
//! max_net 3, max_position 3 (all multiples of the 20000 capital base), order notional up to 20000, 50 orders and 5x
//! turnover per day, 5% cash reserve (1000). Prices: SPY 500, EFA 80, IEF 95, DBC 25, VNQ 90.

mod common;

use std::collections::BTreeMap;

use broker_adapters::alpaca::{self, AssetTable};
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::Side;
use common::*;
use rebalancer_core::guard::{
    margin_use, AccountView, DayCounters, DenialCode, MarginContext, PreTradeGuard, PricePoint, ProposedOrder,
};
use rebalancer_core::planner::{
    client_tag, client_tag_close_leg, OrderPlan, OrderPlanner, PlanConfig, PlanError, PlannedOrder, SkipReason, SleeveTarget, TargetWeight,
    WeightBounds, MAX_ABS_WEIGHT_CAP,
};
use rebalancer_core::policy::Policy;
use rebalancer_core::venue::{AlpacaRules, KrakenRules, SizeRefusal, VenueRuleBook};
use rebalancer_core::Dec;
use serde_json::{json, Value};

fn signed_policy(edit: impl FnOnce(&mut Value)) -> Policy {
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

fn prices() -> BTreeMap<String, PricePoint> {
    [("SPY", "500"), ("EFA", "80"), ("IEF", "95"), ("DBC", "25"), ("VNQ", "90"), ("BTC/USD", "60000"), ("ETH/USD", "3000")]
        .iter()
        .map(|(s, p)| (s.to_string(), PricePoint { price: d(p), as_of: now() }))
        .collect()
}

fn sleeve(id: &str, share: &str, weights: &[(&str, &str)]) -> SleeveTarget {
    SleeveTarget {
        sleeve: id.into(),
        share: d(share),
        venue: "alpaca".into(),
        asset_class: "us_etf".into(),
        weights: weights.iter().map(|(s, w)| TargetWeight { symbol: s.to_string(), weight: d(w) }).collect(),
    }
}

/// The signed sleeve "ls", 100% of capital.
fn ls(weights: &[(&str, &str)]) -> Vec<SleeveTarget> {
    vec![sleeve("ls", "1", weights)]
}

fn acct(cash: &str, positions: Vec<rebalancer_core::guard::Position>) -> AccountView {
    account("20000", cash, positions)
}

fn plain_cfg() -> PlanConfig {
    PlanConfig::new(at("2026-10-01T14:00:00Z"), d("1"), d("10"), d("0.02"), d("0.0025"))
}

/// Signed sleeve "ls" with |w| <= 2, plus a generous buying power.
fn cfg() -> PlanConfig {
    let mut c = plain_cfg().with_signed_sleeve("ls", d("2"));
    c.buying_power = Some(d("100000"));
    c
}

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

    fn whole_share_spy() -> Env {
        let mut e = Env::new();
        e.assets = AssetTable::from_assets_json(r#"[{"symbol":"SPY","tradable":true,"fractionable":false,"status":"active"}]"#).unwrap();
        e
    }

    fn plan(&self, targets: &[SleeveTarget], account: &AccountView, policy: &Policy, cfg: &PlanConfig) -> Result<OrderPlan, PlanError> {
        let kraken = KrakenRules { pairs: &self.pairs };
        let alpaca = AlpacaRules { assets: &self.assets, options: &self.opts };
        let book = VenueRuleBook::new().with("kraken", &kraken).with("alpaca", &alpaca).with_instrument_rules(shortable_everywhere());
        OrderPlanner::plan(targets, account, &prices(), &book, policy, cfg)
    }

    fn ok(&self, targets: &[SleeveTarget], account: &AccountView, policy: &Policy, cfg: &PlanConfig) -> OrderPlan {
        self.plan(targets, account, policy, cfg).unwrap_or_else(|e| panic!("plan failed: {e}"))
    }
}

fn find<'a>(plan: &'a OrderPlan, symbol: &str) -> &'a PlannedOrder {
    plan.orders.iter().find(|o| o.symbol == symbol).unwrap_or_else(|| panic!("no order for {symbol}: {:#?}", plan))
}

fn line<'a>(plan: &'a OrderPlan, symbol: &str) -> &'a rebalancer_core::planner::InstrumentLine {
    plan.lines.iter().find(|l| l.symbol == symbol).unwrap()
}

fn brief(plan: &OrderPlan) -> Vec<(String, Side, String)> {
    plan.orders.iter().map(|o| (o.symbol.clone(), o.side, o.quantity.to_string())).collect()
}

fn denial_codes(plan: &OrderPlan) -> Vec<DenialCode> {
    plan.denied.iter().flat_map(|x| x.reasons.iter().map(|r| r.code)).collect()
}

// ---------------------------------------------------------------------------------------------------------------
// Long and short targets, gross above 1x
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn long_and_short_targets_are_planned_and_margin_is_data() {
    // SPY +0.1 -> +2000 (4 shares), EFA -0.1 -> -2000 (25 shares). Gross 4000 is below equity 20000: only the short
    // uses margin.
    let plan = Env::new().ok(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &acct("20000", vec![]), &signed_policy(|_| {}), &cfg());
    assert!(plan.denied.is_empty(), "{:#?}", plan.denied);
    assert_eq!(
        brief(&plan),
        vec![("EFA".to_string(), Side::Sell, "25".to_string()), ("SPY".to_string(), Side::Buy, "4".to_string())],
        "increases are ordered by (venue, symbol)"
    );
    assert_eq!(line(&plan, "EFA").target_notional, d("-2000"));
    assert_eq!(line(&plan, "SPY").target_notional, d("2000"));
    assert!(plan.margin.signed && plan.margin.needs_margin, "a short target needs margin");
    assert_eq!(plan.margin.target_gross, d("4000"));
    assert_eq!(plan.margin.margin_order_tags, vec![find(&plan, "EFA").tag.clone()], "the short uses margin, the long does not");
    // buying power: 100000 - (2000 + 5) - (2000 + 5)
    assert_eq!(plan.margin.buying_power_left, Some(d("95990")));
}

#[test]
fn gross_above_one_times_equity_is_planned_up_to_the_cap_and_refused_above_it() {
    // +-0.9 of 20000 = 18000 each: gross 36000 = 1.8x equity. Every order uses margin (short, then levered long).
    let plan = Env::new().ok(&ls(&[("SPY", "0.9"), ("EFA", "-0.9")]), &acct("20000", vec![]), &signed_policy(|_| {}), &cfg());
    assert!(plan.denied.is_empty(), "{:#?}", plan.denied);
    assert_eq!(find(&plan, "SPY").quantity, d("36"));
    assert_eq!(find(&plan, "EFA").quantity, d("225"));
    assert_eq!(plan.margin.projected_gross, d("36000"));
    assert_eq!(plan.margin.margin_order_tags.len(), 2, "the short and the levered long both use margin");
    // Exactly at the mandate's cap (3x = 60000; each order is at the 20000 order-notional limit): allowed.
    let mut c = cfg();
    c.buying_power = Some(d("200000"));
    let at_cap = Env::new().ok(&ls(&[("SPY", "1"), ("EFA", "-1"), ("IEF", "1")]), &acct("20000", vec![]), &signed_policy(|_| {}), &c);
    assert!(at_cap.denied.is_empty(), "{:#?}", at_cap.denied);
    assert_eq!(at_cap.margin.target_gross, d("60000"));
    // One notch over: the whole plan is refused (no partial book).
    let err = Env::new().plan(&ls(&[("SPY", "1"), ("EFA", "-1"), ("IEF", "1.01")]), &acct("20000", vec![]), &signed_policy(|_| {}), &c).unwrap_err();
    assert_eq!(err, PlanError::GrossAboveCap { gross: d("60200"), cap: d("60000") });
}

#[test]
fn the_gross_cap_is_the_mandates_and_a_no_leverage_mandate_refuses_a_levered_plan() {
    let p = signed_policy(|m| {
        m["universe"]["leverage_max_gross"] = json!(1.0);
        m["exposure"]["max_gross"] = json!(1.0);
        m["exposure"]["max_net"] = json!(1.0);
        m["exposure"]["max_position"] = json!(1.0);
    });
    let err = Env::new().plan(&ls(&[("SPY", "0.7"), ("DBC", "0.7")]), &acct("20000", vec![]), &p, &cfg()).unwrap_err();
    assert!(matches!(err, PlanError::GrossAboveCap { .. }), "{err:?}");
}

// ---------------------------------------------------------------------------------------------------------------
// max_abs_weight and opt-in validation
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn max_abs_weight_bounds_a_signed_sleeve_and_replaces_the_unit_rules_only_for_it() {
    let env = Env::new();
    let pol = signed_policy(|_| {});
    let run = |targets: Vec<SleeveTarget>, c: &PlanConfig| env.plan(&targets, &acct("20000", vec![]), &pol, c);
    // At the bound: fine. Just over, either sign: refused with the sleeve, symbol and bound.
    assert!(run(ls(&[("SPY", "2")]), &cfg()).is_ok());
    assert!(run(ls(&[("SPY", "-2")]), &cfg()).is_ok());
    for bad in ["2.000000001", "-2.000000001", "3", "-5"] {
        let e = run(ls(&[("SPY", bad)]), &cfg()).unwrap_err();
        assert_eq!(e, PlanError::BadSignedWeight { sleeve: "ls".into(), symbol: "SPY".into(), max: d("2") }, "{bad}");
    }
    // No sum rule for a signed sleeve (3 in total here is still within the gross cap of 3x per instrument sizes).
    assert!(run(ls(&[("SPY", "1.5"), ("EFA", "-1.5")]), &cfg()).is_ok());
    // The bound itself: (0, MAX_ABS_WEIGHT_CAP].
    assert_eq!(MAX_ABS_WEIGHT_CAP, 3);
    for bad in ["0", "-1", "3.000000001", "4"] {
        let c = plain_cfg().with_signed_sleeve("ls", d(bad));
        assert!(matches!(run(ls(&[("SPY", "0.1")]), &c), Err(PlanError::BadMaxAbsWeight { .. })), "{bad}");
    }
    let c3 = plain_cfg().with_signed_sleeve("ls", d("3"));
    assert!(run(ls(&[("SPY", "0")]), &c3).is_ok(), "the hard cap itself is allowed");
    // A misspelt opt-in fails closed instead of silently leaving the sleeve long-only.
    let typo = plain_cfg().with_signed_sleeve("lss", d("2"));
    assert_eq!(run(ls(&[("SPY", "0.1")]), &typo).unwrap_err(), PlanError::UnknownSignedSleeve("lss".into()));
    // Per sleeve: a long-only sleeve next to a signed one keeps today's rules (share 0.5 each).
    let mixed = |w: &[(&str, &str)]| vec![sleeve("ls", "0.5", &[("SPY", "1.5")]), sleeve("etf", "0.5", w)];
    assert!(matches!(run(mixed(&[("EFA", "-0.1")]), &cfg()), Err(PlanError::BadWeight { .. })), "negative weight in a long-only sleeve");
    assert!(matches!(run(mixed(&[("EFA", "1.1")]), &cfg()), Err(PlanError::BadWeight { .. })));
    assert!(matches!(run(mixed(&[("EFA", "0.6"), ("IEF", "0.6")]), &cfg()), Err(PlanError::WeightsExceedOne(_))));
    // Without the opt-in a negative weight is the same refusal as always.
    assert!(matches!(run(ls(&[("SPY", "-0.1")]), &plain_cfg()), Err(PlanError::BadWeight { .. })));
    assert_eq!(plain_cfg().bounds_for("ls"), WeightBounds::LongOnlyUnit);
    assert_eq!(cfg().bounds_for(" ls "), WeightBounds::Signed { max_abs_weight: d("2") });
}

// ---------------------------------------------------------------------------------------------------------------
// Permissions: the guard decides, with the existing codes
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn shorting_off_in_the_mandate_denies_the_short_with_the_existing_code() {
    let pol = signed_policy(|m| m["universe"]["shorting"] = json!(false));
    let plan = Env::new().ok(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &acct("20000", vec![]), &pol, &cfg());
    assert_eq!(denial_codes(&plan), vec![DenialCode::ShortingForbidden], "{:#?}", plan.denied);
    assert_eq!(plan.denied[0].order.symbol, "EFA");
    assert_eq!(brief(&plan), vec![("SPY".to_string(), Side::Buy, "4".to_string())], "the long leg still goes");
}

#[test]
fn a_short_within_the_gross_cap_is_not_leverage_even_at_one_x() {
    // leverage 1x, shorting permitted, gross 4000 of 20000: a short by itself is not leverage (VENUE_FACTS.md), so both
    // legs go. (Before venue rules, the short was denied LEVERAGE_FORBIDDEN as "margin use".)
    let pol = signed_policy(|m| {
        m["universe"]["leverage_max_gross"] = json!(1.0);
        m["exposure"]["max_gross"] = json!(1.0);
        m["exposure"]["max_net"] = json!(1.0);
        m["exposure"]["max_position"] = json!(1.0);
    });
    let plan = Env::new().ok(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &acct("20000", vec![]), &pol, &cfg());
    assert!(plan.denied.is_empty(), "{:#?}", plan.denied);
    assert_eq!(
        brief(&plan),
        vec![("EFA".to_string(), Side::Sell, "25".to_string()), ("SPY".to_string(), Side::Buy, "4".to_string())]
    );
}

// ---------------------------------------------------------------------------------------------------------------
// Buying power
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_margin_plan_without_buying_power_is_refused() {
    let env = Env::new();
    let pol = signed_policy(|_| {});
    let no_bp = plain_cfg().with_signed_sleeve("ls", d("2"));
    // A short target.
    let e = env.plan(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &acct("20000", vec![]), &pol, &no_bp).unwrap_err();
    assert!(matches!(e, PlanError::BuyingPowerRequired { .. }), "{e:?}");
    // A levered long-only book (gross 1.8x equity).
    let e = env.plan(&ls(&[("SPY", "0.9"), ("EFA", "0.9")]), &acct("20000", vec![]), &pol, &no_bp).unwrap_err();
    assert_eq!(e, PlanError::BuyingPowerRequired { gross: d("36000") });
    // Not a margin plan: long only, gross within equity -> no buying power needed, plain cash rules.
    let plan = env.ok(&ls(&[("SPY", "0.5"), ("EFA", "0.3")]), &acct("20000", vec![]), &pol, &no_bp);
    assert!(plan.margin.signed && !plan.margin.needs_margin && plan.margin.margin_order_tags.is_empty());
    assert_eq!(plan.orders.len(), 2);
    // A negative figure is a caller bug.
    let mut neg = no_bp.clone();
    neg.buying_power = Some(d("-1"));
    assert!(matches!(env.plan(&ls(&[("SPY", "0.1")]), &acct("20000", vec![]), &pol, &neg), Err(PlanError::BadParam(_))));
}

#[test]
fn buying_power_limits_increases_and_scales_them_by_one_common_factor() {
    // Wants 2005 + 2005 of increases (SPY long, EFA short); buying power 3000 less the 1000 reserve leaves 2000.
    let mut c = cfg();
    c.buying_power = Some(d("3000"));
    let plan = Env::new().ok(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &acct("20000", vec![]), &signed_policy(|_| {}), &c);
    assert!(plan.denied.is_empty(), "{:#?}", plan.denied);
    let spent = plan.orders.iter().fold(Dec::ZERO, |a, o| a.checked_add(o.notional).unwrap().checked_add(o.est_fee).unwrap());
    assert!(spent <= d("2000"), "increases plus fees {spent} must fit buying power minus reserve");
    assert!(spent > d("1999"), "and use nearly all of it, got {spent}");
    // Same factor for both legs: EFA was 25 shares, SPY 4; both scaled to the same fraction (~0.498).
    let efa = find(&plan, "EFA");
    let spy = find(&plan, "SPY");
    assert!(efa.quantity < d("25") && spy.quantity < d("4"));
    let f_efa = efa.notional.to_f64() / 2000.0;
    let f_spy = spy.notional.to_f64() / 2000.0;
    assert!((f_efa - f_spy).abs() < 0.001, "one common factor, got {f_efa} vs {f_spy}");
    assert!(plan.margin.buying_power_left.unwrap() >= d("1000"), "the reserve stays uncommitted");
    // No room at all (buying power below the reserve): every increase is skipped, none is invented.
    c.buying_power = Some(d("900"));
    let none = Env::new().ok(&ls(&[("SPY", "0.1"), ("EFA", "-0.1")]), &acct("20000", vec![]), &signed_policy(|_| {}), &c);
    assert!(none.orders.is_empty() && none.denied.is_empty());
    assert!(none.skipped.iter().all(|s| s.reason == SkipReason::NoCashAvailable), "{:?}", none.skipped);
}

#[test]
fn with_buying_power_cash_is_not_the_budget() {
    // Cash is only 500 but the broker reports 100000 of buying power (a margin book): the plan is sized on buying power.
    let plan = Env::new().ok(&ls(&[("SPY", "0.9"), ("EFA", "-0.9")]), &acct("500", vec![]), &signed_policy(|_| {}), &cfg());
    assert_eq!(plan.orders.len(), 2, "{:#?}", plan);
    assert_eq!(find(&plan, "SPY").quantity, d("36"));
}

// ---------------------------------------------------------------------------------------------------------------
// Crossing zero: two legs
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn long_to_short_is_a_close_leg_then_an_open_leg() {
    // Holds SPY 4 (2000 long); the target is -2000 (-0.1).
    let a = acct("18000", vec![pos("SPY", "4", "2000")]);
    let plan = Env::new().ok(&ls(&[("SPY", "-0.1")]), &a, &signed_policy(|_| {}), &cfg());
    assert!(plan.denied.is_empty(), "{:#?}", plan.denied);
    assert_eq!(
        brief(&plan),
        vec![("SPY".to_string(), Side::Sell, "4".to_string()), ("SPY".to_string(), Side::Sell, "4".to_string())],
        "sell to close, then sell to open"
    );
    let sched = at("2026-10-01T14:00:00Z");
    assert_eq!(plan.orders[0].tag, client_tag_close_leg("acct-1", sched, "ls", "SPY", Side::Sell));
    assert_eq!(plan.orders[1].tag, client_tag("acct-1", sched, "ls", "SPY", Side::Sell));
    assert_ne!(plan.orders[0].tag, plan.orders[1].tag);
    assert_eq!(plan.margin.margin_order_tags, vec![plan.orders[1].tag.clone()], "closing is not margin use, opening the short is");
}

#[test]
fn short_to_long_is_a_cover_then_a_buy_and_needs_no_margin() {
    // Holds EFA -25 (-2000 short); the target is +2000. No buying power supplied: not a margin plan.
    let a = acct("22000", vec![pos("EFA", "-25", "-2000")]);
    let no_bp = plain_cfg().with_signed_sleeve("ls", d("2"));
    let plan = Env::new().ok(&ls(&[("EFA", "0.1")]), &a, &signed_policy(|_| {}), &no_bp);
    assert!(plan.denied.is_empty(), "{:#?}", plan.denied);
    assert_eq!(
        brief(&plan),
        vec![("EFA".to_string(), Side::Buy, "25".to_string()), ("EFA".to_string(), Side::Buy, "25".to_string())]
    );
    assert!(!plan.margin.needs_margin && plan.margin.margin_order_tags.is_empty());
}

#[test]
fn the_open_leg_is_not_attempted_when_the_close_leg_is_not_accepted() {
    // The daily order limit (50) is already used: the close leg (a reduction still counts) is denied.
    let a = acct("18000", vec![pos("SPY", "4", "2000")]);
    let mut c = cfg();
    c.day = DayCounters { orders_today: 50, turnover_today: Dec::ZERO };
    let plan = Env::new().ok(&ls(&[("SPY", "-0.1")]), &a, &signed_policy(|_| {}), &c);
    assert!(plan.orders.is_empty(), "{:#?}", plan.orders);
    assert_eq!(denial_codes(&plan), vec![DenialCode::MaxOrdersPerDay]);
    assert_eq!(plan.skipped.len(), 1);
    assert_eq!(plan.skipped[0].reason, SkipReason::FlipCloseLegNotPlaced);
}

#[test]
fn a_venue_refusal_of_the_open_leg_leaves_the_account_flat() {
    // SPY trades in whole shares here; the short target is 0.8 of a share, which rounds to zero: close only.
    let a = acct("18000", vec![pos("SPY", "4", "2000")]);
    let mut c = cfg();
    c.min_trade_abs = d("0");
    let plan = Env::whole_share_spy().ok(&ls(&[("SPY", "-0.02")]), &a, &signed_policy(|_| {}), &c);
    assert_eq!(brief(&plan), vec![("SPY".to_string(), Side::Sell, "4".to_string())]);
    assert!(plan.skipped.iter().any(|s| s.symbol == "SPY" && s.reason == SkipReason::VenueRefused(SizeRefusal::RoundsToZero)), "{:?}", plan.skipped);
}

// ---------------------------------------------------------------------------------------------------------------
// Held shorts
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_held_short_is_managed_in_a_signed_sleeve_and_still_refused_in_a_long_only_one() {
    let env = Env::new();
    let pol = signed_policy(|_| {});
    let held = vec![pos("EFA", "-25", "-2000"), pos("SPY", "-1", "-500")];
    // Signed sleeve owns EFA: already at the -2000 target -> nothing to do, and NOT skipped as a held short.
    let a = acct("22500", held.clone());
    let plan = env.ok(&[sleeve("ls", "0.5", &[("EFA", "-0.2")]), sleeve("etf", "0.5", &[("SPY", "0.1")])], &a, &pol, &cfg());
    assert!(plan.orders.is_empty() && plan.denied.is_empty(), "{:#?}", plan);
    assert_eq!(line(&plan, "EFA").held, d("-25"));
    assert!(!plan.skipped.iter().any(|s| s.symbol == "EFA"), "{:?}", plan.skipped);
    // The long-only sleeve's SPY short is untouched, exactly as before.
    assert!(plan.skipped.iter().any(|s| s.symbol == "SPY" && s.reason == SkipReason::ShortPositionHeld), "{:?}", plan.skipped);
    // A smaller short target buys back part of it (a reduction): 12.5 of 25 shares.
    let plan = env.ok(&ls(&[("EFA", "-0.05")]), &acct("22000", vec![pos("EFA", "-25", "-2000")]), &pol, &cfg());
    assert_eq!(brief(&plan), vec![("EFA".to_string(), Side::Buy, "12.5".to_string())]);
    assert!(plan.margin.margin_order_tags.is_empty(), "covering is not margin use");
    // A zero target covers all of it, and needs no buying power.
    let no_bp = plain_cfg().with_signed_sleeve("ls", d("2"));
    let plan = env.ok(&ls(&[("EFA", "0")]), &acct("22000", vec![pos("EFA", "-25", "-2000")]), &pol, &no_bp);
    assert_eq!(brief(&plan), vec![("EFA".to_string(), Side::Buy, "25".to_string())]);
    // The same account in a plan with NO signed sleeve: refused, as it always was.
    let plan = env.ok(&[sleeve("etf", "1", &[("EFA", "0.1")])], &acct("22000", vec![pos("EFA", "-25", "-2000")]), &pol, &plain_cfg());
    assert!(plan.orders.is_empty());
    assert_eq!(plan.skipped[0].reason, SkipReason::ShortPositionHeld);
}

// ---------------------------------------------------------------------------------------------------------------
// Zero targets, risk scale, rounding
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn zero_targets_close_everything_without_needing_margin_or_buying_power() {
    let a = acct("20000", vec![pos("SPY", "4", "2000"), pos("EFA", "-25", "-2000")]);
    let no_bp = plain_cfg().with_signed_sleeve("ls", d("2"));
    let plan = Env::new().ok(&ls(&[("SPY", "0"), ("EFA", "0"), ("IEF", "0")]), &a, &signed_policy(|_| {}), &no_bp);
    assert!(!plan.margin.needs_margin);
    assert_eq!(
        brief(&plan),
        vec![("EFA".to_string(), Side::Buy, "25".to_string()), ("SPY".to_string(), Side::Sell, "4".to_string())],
        "both are reductions: ordered by symbol"
    );
    // Flat account, zero targets: nothing at all.
    let plan = Env::new().ok(&ls(&[("SPY", "0")]), &acct("20000", vec![]), &signed_policy(|_| {}), &no_bp);
    assert!(plan.orders.is_empty() && plan.denied.is_empty() && plan.skipped.is_empty());
}

#[test]
fn risk_scale_multiplies_signed_targets_and_rounds_toward_zero() {
    let w = "0.1234567891234";
    let neg_w = "-0.1234567891234";
    let pol = signed_policy(|_| {});
    let run = |scale: &str| {
        let mut c = cfg();
        c.risk_scale = d(scale);
        c.min_trade_abs = d("0");
        Env::new().ok(&ls(&[("SPY", w), ("EFA", neg_w)]), &acct("20000", vec![]), &pol, &c)
    };
    let full = run("1");
    // 20000 * 0.1234567891234 = 2469.135782468: the long floors to ...46, the short rounds toward zero to -...46
    // (a plain floor would make it -...47, i.e. a slightly bigger short).
    assert_eq!(line(&full, "SPY").target_notional, d("2469.13578246"));
    assert_eq!(line(&full, "EFA").target_notional, d("-2469.13578246"));
    let half = run("0.5");
    assert_eq!(line(&half, "SPY").target_notional, d("1234.56789123"));
    assert_eq!(line(&half, "EFA").target_notional, d("-1234.56789123"));
    // Short and long are sized symmetrically and never above target.
    for o in &full.orders {
        assert!(o.notional <= d("2469.13578246"), "{o:?}");
    }
    assert!(find(&half, "EFA").quantity < find(&full, "EFA").quantity);
    assert_eq!(find(&full, "EFA").side, Side::Sell);
}

#[test]
fn the_digest_covers_the_signed_inputs_and_is_untouched_for_long_only() {
    let env = Env::new();
    let pol = signed_policy(|_| {});
    let a = env.ok(&ls(&[("SPY", "0.1")]), &acct("20000", vec![]), &pol, &cfg());
    let mut c2 = cfg();
    c2.buying_power = Some(d("99999"));
    let b = env.ok(&ls(&[("SPY", "0.1")]), &acct("20000", vec![]), &pol, &c2);
    let mut c3 = plain_cfg().with_signed_sleeve("ls", d("2.5"));
    c3.buying_power = Some(d("100000"));
    let c = env.ok(&ls(&[("SPY", "0.1")]), &acct("20000", vec![]), &pol, &c3);
    assert_ne!(a.inputs_digest, b.inputs_digest, "buying power is an input");
    assert_ne!(a.inputs_digest, c.inputs_digest, "max_abs_weight is an input");
    // A buying power on a plan with no signed sleeve changes nothing (it has no effect there).
    let mut lo = plain_cfg();
    lo.buying_power = Some(d("1"));
    let x = env.ok(&[sleeve("ls", "1", &[("SPY", "0.1")])], &acct("20000", vec![]), &pol, &lo);
    let y = env.ok(&[sleeve("ls", "1", &[("SPY", "0.1")])], &acct("20000", vec![]), &pol, &plain_cfg());
    assert_eq!(x, y);
    assert!(!x.margin.signed);
}

// ---------------------------------------------------------------------------------------------------------------
// The guard on its own
// ---------------------------------------------------------------------------------------------------------------

fn no_leverage() -> Policy {
    signed_policy(|m| {
        m["universe"]["leverage_max_gross"] = json!(1.0);
        m["exposure"]["max_gross"] = json!(1.0);
        m["exposure"]["max_net"] = json!(1.0);
        m["exposure"]["max_position"] = json!(1.0);
    })
}

fn ctx(bp: Option<&str>) -> MarginContext {
    MarginContext { buying_power_left: bp.map(d) }
}

fn cm(p: &Policy, a: &AccountView, o: &ProposedOrder, bp: Option<&str>) -> rebalancer_core::guard::Verdict {
    PreTradeGuard::check_signed(p, a, o, &day0(), &ctx(bp), &shortable_everywhere())
}

#[test]
fn the_guard_derives_margin_use_itself_even_when_the_caller_says_false() {
    let p = no_leverage();
    let flat = acct("20000", vec![]);
    let short = sell("EFA", "25", "80"); // uses_margin: false as built
    // The plain check is unchanged: it does not derive anything.
    assert!(PreTradeGuard::check(&p, &flat, &short, &day0()).allow);
    // With a margin context the short is margin use (data), but gross stays within the 1x cap: not leverage.
    let v = cm(&p, &flat, &short, Some("50000"));
    assert!(v.allow, "{:?}", v.reasons);
    // The caller's own flag still denies through the plain entry point; the signed one tests gross instead.
    let mut flagged = buy("SPY", "1", "500");
    flagged.uses_margin = true;
    assert!(PreTradeGuard::check(&p, &flat, &flagged, &day0()).has(DenialCode::LeverageForbidden));
    assert!(!cm(&p, &flat, &flagged, Some("50000")).has(DenialCode::LeverageForbidden));
    // A levered long: equity 20000, holds SPY 36 (18000), cash 2000; buying 26 EFA (2080) takes gross to 20080.
    let held = acct("2000", vec![pos("SPY", "36", "18000")]);
    assert_eq!(margin_use(&held, &buy("EFA", "26", "80"), d("80")), Ok(true));
    assert_eq!(margin_use(&held, &buy("EFA", "25", "80"), d("80")), Ok(false), "exactly gross = equity, cash 0: not margin");
    assert!(cm(&p, &held, &buy("EFA", "26", "80"), Some("50000")).has(DenialCode::LeverageForbidden));
    assert!(!cm(&p, &held, &buy("EFA", "25", "80"), Some("50000")).has(DenialCode::LeverageForbidden));
    // Negative cash (borrowing) is margin use even when gross is small.
    let broke = acct("100", vec![pos("SPY", "2", "1000")]);
    assert_eq!(margin_use(&broke, &buy("EFA", "2", "80"), d("80")), Ok(true));
}

#[test]
fn reductions_are_never_margin_use() {
    let p = no_leverage();
    // Covering a short, selling a long: reductions on any book.
    let short_book = acct("22000", vec![pos("SPY", "-4", "-2000")]);
    assert_eq!(margin_use(&short_book, &buy("SPY", "4", "500"), d("500")), Ok(false));
    assert!(cm(&p, &short_book, &buy("SPY", "4", "500"), None).allow);
    let long_book = acct("18000", vec![pos("SPY", "4", "2000")]);
    assert_eq!(margin_use(&long_book, &sell("SPY", "4", "500"), d("500")), Ok(false));
    assert!(cm(&p, &long_book, &sell("SPY", "4", "500"), None).allow);
    // One share more than held is no longer a reduction: it opens a short.
    assert_eq!(margin_use(&long_book, &sell("SPY", "5", "500"), d("500")), Ok(true));
    // Selling MORE of an existing short deepens it.
    assert_eq!(margin_use(&short_book, &sell("SPY", "1", "500"), d("500")), Ok(true));
}

#[test]
fn shorting_off_is_still_denied_by_check_margin() {
    let p = signed_policy(|m| m["universe"]["shorting"] = json!(false));
    let v = cm(&p, &acct("20000", vec![]), &sell("EFA", "25", "80"), Some("50000"));
    assert_eq!(v.codes(), vec![DenialCode::ShortingForbidden], "{:?}", v.reasons);
}

#[test]
fn buying_power_replaces_the_cash_test_and_missing_buying_power_denies_margin_use() {
    let p = signed_policy(|_| {}); // reserve 5% of 20000 = 1000
    let flat = acct("20000", vec![]);
    let mut short = sell("EFA", "25", "80"); // 2000 notional
    short.est_fee = d("5");
    // Left after the order must stay at or above the reserve: 3005 - 2000 - 5 = 1000.
    assert!(cm(&p, &flat, &short, Some("3005")).allow);
    let v = cm(&p, &flat, &short, Some("3004.99999999"));
    assert_eq!(v.codes(), vec![DenialCode::CashReserve], "{:?}", v.reasons);
    // No figure and the order needs margin: nothing to verify it against.
    let v = cm(&p, &flat, &short, None);
    assert_eq!(v.codes(), vec![DenialCode::CashReserve], "{:?}", v.reasons);
    // No figure and the order does not need margin: the plain cash test.
    assert!(cm(&p, &flat, &buy("SPY", "4", "500"), None).allow);
    let poor = acct("1000", vec![]);
    assert_eq!(cm(&p, &poor, &buy("SPY", "1", "500"), None).codes(), vec![DenialCode::CashReserve]);
    // With a buying power figure cash no longer limits: cash is 1000 but 50000 of buying power covers the buy
    // (the buy itself borrows: cash goes to -1000, which the 3x mandate allows).
    assert!(cm(&p, &poor, &buy("SPY", "4", "500"), Some("50000")).has(DenialCode::CashReserve) == false);
    // A reduction is exempt from the funding test whatever the figure.
    let long_book = acct("18000", vec![pos("SPY", "4", "2000")]);
    assert!(cm(&p, &long_book, &sell("SPY", "4", "500"), Some("0")).allow);
}

#[test]
fn a_flip_in_one_order_only_counts_its_opening_part_against_buying_power() {
    // Sell 8 against a long 4 (a single-order flip; the planner never emits one, the guard must still be exact):
    // 4 closes, 4 opens, so only 4 * 500 = 2000 of buying power is used.
    let p = signed_policy(|_| {});
    let a = acct("18000", vec![pos("SPY", "4", "2000")]);
    let flip = sell("SPY", "8", "500");
    assert!(cm(&p, &a, &flip, Some("3000")).allow, "3000 - 2000 = 1000 >= reserve");
    assert!(cm(&p, &a, &flip, Some("2999")).has(DenialCode::CashReserve));
}

#[test]
fn the_planners_orders_carry_margin_use_as_the_guard_derives_it() {
    // Every accepted order's flag must equal margin_use on the account it was checked against (spot check on the
    // levered long-short plan: the short, then the levered long).
    let plan = Env::new().ok(&ls(&[("SPY", "0.9"), ("EFA", "-0.9")]), &acct("20000", vec![]), &signed_policy(|_| {}), &cfg());
    let flagged: Vec<&str> =
        plan.orders.iter().filter(|o| plan.margin.margin_order_tags.contains(&o.tag)).map(|o| o.symbol.as_str()).collect();
    assert_eq!(flagged, vec!["EFA", "SPY"]);
    // Kraken rules are wired for a crypto sleeve too: a signed crypto sleeve may short BTC.
    let crypto = SleeveTarget {
        sleeve: "cx".into(),
        share: d("1"),
        venue: "kraken".into(),
        asset_class: "crypto_spot".into(),
        weights: vec![TargetWeight { symbol: "BTC/USD".into(), weight: d("-0.5") }],
    };
    let c = plain_cfg().with_signed_sleeve("cx", d("1"));
    let mut c = c;
    c.buying_power = Some(d("100000"));
    let plan = Env::new().ok(&[crypto], &acct("20000", vec![]), &signed_policy(|_| {}), &c);
    assert_eq!(find(&plan, "BTC/USD").side, Side::Sell);
    assert_eq!(plan.margin.margin_order_tags.len(), 1);
}
