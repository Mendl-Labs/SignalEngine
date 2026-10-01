//! Example-based tests of the order planner, run against the REAL adapter rule tables (Kraken built-in pair table,
//! Alpaca built-in asset table or JSON rows), so the venue rules exercised are the adapters' own.

mod common;

use std::collections::BTreeMap;

use broker_adapters::alpaca::{self, AssetTable};
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::Side;
use chrono::NaiveDate;
use common::*;
use rebalancer_core::guard::{AccountView, DenialCode, PricePoint};
use rebalancer_core::planner::{
    client_tag, OrderPlan, OrderPlanner, PlanConfig, PlanError, SkipReason, SleeveTarget, TargetWeight,
};
use rebalancer_core::policy::Policy;
use rebalancer_core::venue::{AlpacaRules, KrakenRules, SizeRefusal, VenueRuleBook, VenueRules};
use rebalancer_core::Dec;
use reference_rules::{CryptoDecision, EtfDecision, InstrumentDecision, Signal};
use serde_json::json;

const ETFS: [&str; 5] = ["SPY", "EFA", "IEF", "DBC", "VNQ"];

/// Mandate that allows all seven instruments and a turnover of 1.0 of equity, otherwise the baseline.
fn universe_policy() -> Policy {
    universe_policy_with(|_| {})
}

fn universe_policy_with(edit: impl FnOnce(&mut serde_json::Value)) -> Policy {
    policy_with(|m| {
        m["universe"]["instrument_allow"] = json!(["SPY", "EFA", "IEF", "DBC", "VNQ", "BTC/USD", "ETH/USD"]);
        m["exposure"]["max_turnover_per_day"] = json!(1.0);
        edit(m);
    })
}

fn prices() -> BTreeMap<String, PricePoint> {
    [("SPY", "500"), ("EFA", "80"), ("IEF", "95"), ("DBC", "25"), ("VNQ", "90"), ("BTC/USD", "60000"), ("ETH/USD", "3000")]
        .iter()
        .map(|(s, p)| (s.to_string(), PricePoint { price: d(p), as_of: now() }))
        .collect()
}

fn etf_target(share: &str, weights: [&str; 5]) -> SleeveTarget {
    SleeveTarget {
        sleeve: "etf".into(),
        share: d(share),
        venue: "alpaca".into(),
        asset_class: "us_etf".into(),
        weights: ETFS.iter().zip(weights).map(|(s, w)| TargetWeight { symbol: s.to_string(), weight: d(w) }).collect(),
    }
}

fn crypto_target(share: &str, btc: &str, eth: &str) -> SleeveTarget {
    SleeveTarget {
        sleeve: "crypto".into(),
        share: d(share),
        venue: "kraken".into(),
        asset_class: "crypto_spot".into(),
        weights: vec![
            TargetWeight { symbol: "BTC/USD".into(), weight: d(btc) },
            TargetWeight { symbol: "ETH/USD".into(), weight: d(eth) },
        ],
    }
}

fn both_sleeves() -> Vec<SleeveTarget> {
    vec![etf_target("0.5", ["0.2"; 5]), crypto_target("0.5", "0.5", "0.5")]
}

fn cfg() -> PlanConfig {
    PlanConfig::new(at("2026-10-01T14:00:00Z"), d("1"), d("10"), d("0.02"), d("0.0025"))
}

/// The adapters' own rule tables.
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
            opts: alpaca::PrepareOptions {
                allow_extended_hours: false,
                min_notional: d("1"),
                own_tag_prefix: None,
                refuse_builtin_assets: false,
            },
        }
    }

    fn with_assets_json(json: &str) -> Env {
        let mut e = Env::new();
        e.assets = AssetTable::from_assets_json(json).unwrap();
        e
    }

    fn plan(
        &self,
        targets: &[SleeveTarget],
        account: &AccountView,
        prices: &BTreeMap<String, PricePoint>,
        policy: &Policy,
        cfg: &PlanConfig,
    ) -> Result<OrderPlan, PlanError> {
        let kraken = KrakenRules { pairs: &self.pairs };
        let alpaca = AlpacaRules { assets: &self.assets, options: &self.opts };
        let book = VenueRuleBook::new().with("kraken", &kraken).with("alpaca", &alpaca);
        OrderPlanner::plan(targets, account, prices, &book, policy, cfg)
    }

    fn plan_ok(&self, targets: &[SleeveTarget], account: &AccountView, policy: &Policy, cfg: &PlanConfig) -> OrderPlan {
        self.plan(targets, account, &prices(), policy, cfg).expect("plan")
    }
}

fn cost(o: &rebalancer_core::planner::PlannedOrder) -> Dec {
    o.notional.checked_add(o.est_fee).unwrap()
}

fn find<'a>(plan: &'a OrderPlan, symbol: &str) -> &'a rebalancer_core::planner::PlannedOrder {
    plan.orders.iter().find(|o| o.symbol == symbol).unwrap_or_else(|| panic!("no order for {symbol}: {:?}", plan.orders))
}

fn skip_reason<'a>(plan: &'a OrderPlan, symbol: &str) -> &'a SkipReason {
    &plan.skipped.iter().find(|s| s.symbol == symbol).unwrap_or_else(|| panic!("{symbol} not skipped: {:?}", plan.skipped)).reason
}

// ---------------------------------------------------------------------------------------------------------------
// Basic plans
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn flat_account_buys_are_scaled_to_the_cash_above_the_reserve() {
    let plan = Env::new().plan_ok(&both_sleeves(), &flat(), &universe_policy(), &cfg());
    assert_eq!(plan.orders.len(), 7, "{plan:#?}");
    assert!(plan.orders.iter().all(|o| o.side == Side::Buy));
    assert!(plan.denied.is_empty(), "{:?}", plan.denied);
    let total = plan.orders.iter().map(cost).fold(Dec::ZERO, |a, b| a.checked_add(b).unwrap());
    assert!(total <= d("4750"), "buys {total} must fit cash 5000 minus the 250 reserve");
    assert!(total > d("4740"), "and use nearly all of it, got {total}");
    // Unscaled targets: SPY 500 (1 share), ETH 1250 (0.41666666). Scaled ones are smaller.
    assert!(find(&plan, "SPY").quantity < d("1"));
    assert!(find(&plan, "ETH/USD").quantity < d("0.41666666"));
    assert_eq!(plan.capital_base, d("5000"));
    assert_eq!(plan.inputs_digest.len(), 64);
}

#[test]
fn when_cash_is_ample_buys_are_the_exact_floor_of_target_over_price() {
    let acct = account("50000", "50000", vec![]);
    // capital_base = min(50000, allocated 5000) = 5000, so targets are the same as above; cash is ample.
    let plan = Env::new().plan_ok(&both_sleeves(), &acct, &universe_policy(), &cfg());
    assert_eq!(find(&plan, "SPY").quantity, d("1")); // 500 / 500
    assert_eq!(find(&plan, "EFA").quantity, d("6.25")); // 500 / 80
    assert_eq!(find(&plan, "IEF").quantity, d("5.263157894")); // 500 / 95 floored at 9 dp
    assert_eq!(find(&plan, "DBC").quantity, d("20"));
    assert_eq!(find(&plan, "VNQ").quantity, d("5.555555555"));
    assert_eq!(find(&plan, "BTC/USD").quantity, d("0.02083333")); // 1250 / 60000 floored at 8 dp
    assert_eq!(find(&plan, "ETH/USD").quantity, d("0.41666666"));
    for o in &plan.orders {
        assert!(o.notional <= d("1250"), "{o:?}");
    }
    assert_eq!(find(&plan, "SPY").est_fee, d("1.25")); // ceil8(500 * 0.0025)
}

#[test]
fn sells_come_first_partial_trims_and_full_exits_never_exceed_the_held_quantity() {
    // Holds SPY 4 (2000) and VNQ 3 (270); the ETF sleeve is 100% of capital and wants VNQ at zero.
    let acct = account("5000", "2730", vec![pos("SPY", "4", "2000"), pos("VNQ", "3", "270")]);
    let targets = vec![etf_target("1", ["0.2", "0.2", "0.2", "0.2", "0"])];
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &cfg());
    let sides: Vec<Side> = plan.orders.iter().map(|o| o.side).collect();
    assert_eq!(sides, vec![Side::Sell, Side::Sell, Side::Buy, Side::Buy, Side::Buy], "{plan:#?}");
    let symbols: Vec<&str> = plan.orders.iter().map(|o| o.symbol.as_str()).collect();
    assert_eq!(symbols, vec!["SPY", "VNQ", "DBC", "EFA", "IEF"]);
    assert_eq!(find(&plan, "SPY").quantity, d("2")); // 2000 -> 1000
    assert_eq!(find(&plan, "VNQ").quantity, d("3")); // full exit sells exactly the held quantity
    assert!(plan.denied.is_empty(), "{:?}", plan.denied);
}

#[test]
fn a_position_already_at_target_produces_no_order() {
    let acct = account("5000", "4000", vec![pos("SPY", "2", "1000")]);
    let targets = vec![etf_target("1", ["0.2", "0", "0", "0", "0"])];
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &cfg());
    assert!(plan.orders.is_empty() && plan.denied.is_empty(), "{plan:#?}");
    let spy = plan.lines.iter().find(|l| l.symbol == "SPY").unwrap();
    assert_eq!(spy.target_notional, d("1000"));
    assert_eq!(spy.current_notional, d("1000"));
}

// ---------------------------------------------------------------------------------------------------------------
// Minimum trade thresholds
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn min_trade_abs_boundary() {
    // Target SPY 1000; hold 1.98 shares (990): delta exactly 10.
    let acct = account("5000", "4010", vec![pos("SPY", "1.98", "990")]);
    let targets = vec![etf_target("1", ["0.2", "0", "0", "0", "0"])];
    let mut c = cfg();
    c.min_trade_pct = d("0");
    c.min_trade_abs = d("10");
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &c);
    assert_eq!(find(&plan, "SPY").quantity, d("0.02"), "a delta exactly at the minimum trades");
    c.min_trade_abs = d("10.01");
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &c);
    assert!(plan.orders.is_empty());
    assert!(matches!(skip_reason(&plan, "SPY"), SkipReason::BelowMinTradeAbs { .. }));
}

#[test]
fn min_trade_pct_boundary_is_a_fraction_of_the_target() {
    // Target SPY 1000, threshold 2% = 20. Hold 980 -> delta 20 trades; 980.01 -> 19.99 does not.
    let targets = vec![etf_target("1", ["0.2", "0", "0", "0", "0"])];
    let mut c = cfg();
    c.min_trade_abs = d("0");
    let acct = account("5000", "4020", vec![pos("SPY", "1.96", "980")]);
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &c);
    assert_eq!(find(&plan, "SPY").quantity, d("0.04"));
    let acct = account("5000", "4020", vec![pos("SPY", "1.96002", "980.01")]);
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &c);
    assert!(plan.orders.is_empty());
    assert!(matches!(skip_reason(&plan, "SPY"), SkipReason::BelowMinTradePct { .. }));
}

#[test]
fn a_full_exit_uses_the_current_value_as_the_percentage_reference() {
    // Target 0 but held 1000: the pct threshold is 2% of 1000 = 20, not 2% of zero. The whole position sells.
    let acct = account("5000", "4000", vec![pos("SPY", "2", "1000")]);
    let targets = vec![etf_target("1", ["0", "0", "0", "0", "0"])];
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &cfg());
    assert_eq!(find(&plan, "SPY").quantity, d("2"));
    // Dust below the absolute minimum stays (no order).
    let dust = account("5000", "4995", vec![pos("SPY", "0.01", "5")]);
    let plan = Env::new().plan_ok(&targets, &dust, &universe_policy(), &cfg());
    assert!(plan.orders.is_empty());
    assert!(matches!(skip_reason(&plan, "SPY"), SkipReason::BelowMinTradeAbs { .. }));
}

// ---------------------------------------------------------------------------------------------------------------
// Venue rules
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn whole_share_assets_round_down_and_tiny_targets_are_refused() {
    let env = Env::with_assets_json(r#"[{"symbol":"SPY","tradable":true,"fractionable":false,"status":"active"}]"#);
    let acct = account("50000", "50000", vec![]);
    let targets = vec![etf_target("0.5", ["0.5", "0", "0", "0", "0"])]; // 5000 * .5 * .5 = 1250 -> 2.5 shares
    let plan = env.plan_ok(&targets, &acct, &universe_policy(), &cfg());
    assert_eq!(find(&plan, "SPY").quantity, d("2"), "2.5 shares round DOWN to 2");
    // 5000 * 0.001 * 0.5 = 2.5 USD -> 0.005 shares -> rounds to zero.
    let tiny = vec![etf_target("0.001", ["0.5", "0", "0", "0", "0"])];
    let mut c = cfg();
    c.min_trade_abs = d("0");
    let plan = env.plan_ok(&tiny, &acct, &universe_policy(), &c);
    assert!(plan.orders.is_empty());
    assert_eq!(skip_reason(&plan, "SPY"), &SkipReason::VenueRefused(SizeRefusal::RoundsToZero));
    // Missing asset row: the instrument is refused, never guessed.
    let empty = Env::with_assets_json(r#"[{"symbol":"IEF","tradable":true,"fractionable":true}]"#);
    let plan = empty.plan_ok(&targets, &acct, &universe_policy(), &cfg());
    assert!(matches!(skip_reason(&plan, "SPY"), SkipReason::VenueRefused(SizeRefusal::UnknownInstrument(_))));
}

#[test]
fn kraken_minimum_volume_is_respected_not_bumped_up() {
    // 5000 * 0.001 * 0.5 = 2.5 USD of BTC = 0.00004166 BTC, below Kraken's ordermin 0.0001.
    let targets = vec![crypto_target("0.001", "0.5", "0")];
    let mut c = cfg();
    c.min_trade_abs = d("0");
    let plan = Env::new().plan_ok(&targets, &flat(), &universe_policy(), &c);
    assert!(plan.orders.is_empty(), "{plan:#?}");
    match skip_reason(&plan, "BTC/USD") {
        SkipReason::VenueRefused(SizeRefusal::BelowMinQuantity { min }) => assert_eq!(*min, d("0.0001")),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn a_venue_that_rounds_up_is_a_hard_error() {
    struct RoundsUp;
    impl VenueRules for RoundsUp {
        fn round_quantity(&self, _: &str, _: Side, q: Dec, _: Dec) -> Result<Dec, SizeRefusal> {
            Ok(q.checked_add(d("1")).unwrap())
        }
        fn fingerprint(&self, _: &str) -> String {
            "rounds-up".into()
        }
    }
    let r = RoundsUp;
    let book = VenueRuleBook::new().with("alpaca", &r).with("kraken", &r);
    let err = OrderPlanner::plan(&both_sleeves(), &flat(), &prices(), &book, &universe_policy(), &cfg()).unwrap_err();
    assert!(matches!(err, PlanError::VenueRoundedUp { .. }), "{err:?}");
}

#[test]
fn a_venue_without_rules_skips_its_instruments() {
    let kraken_only = PairTable::builtin();
    let k = KrakenRules { pairs: &kraken_only };
    let book = VenueRuleBook::new().with("kraken", &k);
    let plan = OrderPlanner::plan(&both_sleeves(), &flat(), &prices(), &book, &universe_policy(), &cfg()).unwrap();
    assert!(plan.orders.iter().all(|o| o.venue == "kraken"));
    assert_eq!(skip_reason(&plan, "SPY"), &SkipReason::NoVenueRules);
}

// ---------------------------------------------------------------------------------------------------------------
// risk_scale, sleeve shares, capital base
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn risk_scale_multiplies_every_target() {
    let acct = account("50000", "50000", vec![]);
    let targets = vec![etf_target("1", ["0.2", "0", "0", "0", "0"])];
    let full = Env::new().plan_ok(&targets, &acct, &universe_policy(), &cfg());
    assert_eq!(find(&full, "SPY").quantity, d("2")); // 1000
    let mut c = cfg();
    c.risk_scale = d("0.5");
    let half = Env::new().plan_ok(&targets, &acct, &universe_policy(), &c);
    assert_eq!(find(&half, "SPY").quantity, d("1")); // 500
    assert_eq!(half.risk_scale, d("0.5"));
}

#[test]
fn risk_scale_below_the_held_value_sells() {
    // Holding 2 SPY (1000) at scale 0.5 -> target 500: sell 1.
    let acct = account("5000", "4000", vec![pos("SPY", "2", "1000")]);
    let targets = vec![etf_target("1", ["0.2", "0", "0", "0", "0"])];
    let mut c = cfg();
    c.risk_scale = d("0.5");
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &c);
    assert_eq!((plan.orders[0].side, plan.orders[0].quantity), (Side::Sell, d("1")));
}

#[test]
fn risk_scale_must_be_in_the_open_closed_unit_interval() {
    for bad in ["0", "-0.1", "1.0001", "2"] {
        let mut c = cfg();
        c.risk_scale = d(bad);
        let err = Env::new().plan(&both_sleeves(), &flat(), &prices(), &universe_policy(), &c).unwrap_err();
        assert!(matches!(err, PlanError::BadRiskScale(_)), "{bad}: {err:?}");
    }
    let mut c = cfg();
    c.risk_scale = d("1");
    assert!(Env::new().plan(&both_sleeves(), &flat(), &prices(), &universe_policy(), &c).is_ok());
    c.risk_scale = d("0.000001");
    assert!(Env::new().plan(&both_sleeves(), &flat(), &prices(), &universe_policy(), &c).is_ok());
}

#[test]
fn sleeve_input_validation() {
    let env = Env::new();
    let run = |t: Vec<SleeveTarget>| env.plan(&t, &flat(), &prices(), &universe_policy(), &cfg()).unwrap_err();
    // shares above 1 in total
    assert!(matches!(run(vec![etf_target("0.6", ["0.2"; 5]), crypto_target("0.5", "0.5", "0.5")]), PlanError::SharesExceedOne(_)));
    // a share of 0 or above 1
    assert!(matches!(run(vec![etf_target("0", ["0.2"; 5])]), PlanError::BadShare(_)));
    assert!(matches!(run(vec![etf_target("1.1", ["0.2"; 5])]), PlanError::BadShare(_)));
    // weights above 1 in total, or a single weight outside [0, 1]
    assert!(matches!(run(vec![etf_target("1", ["0.3"; 5])]), PlanError::WeightsExceedOne(_)));
    assert!(matches!(run(vec![etf_target("1", ["-0.1", "0", "0", "0", "0"])]), PlanError::BadWeight { .. }));
    // duplicate sleeve id, duplicate symbol
    assert!(matches!(run(vec![etf_target("0.5", ["0.2"; 5]), etf_target("0.5", ["0.2"; 5])]), PlanError::DuplicateSleeve(_)));
    let mut dup = etf_target("1", ["0.1"; 5]);
    dup.weights.push(TargetWeight { symbol: "spy".into(), weight: d("0.1") });
    assert!(matches!(run(vec![dup]), PlanError::DuplicateWeight { .. }));
    // shares summing to exactly 1 are fine
    assert!(env.plan(&both_sleeves(), &flat(), &prices(), &universe_policy(), &cfg()).is_ok());
}

#[test]
fn an_instrument_in_two_sleeves_gets_the_sum_of_share_times_weight() {
    let mut a = etf_target("0.5", ["0.4", "0", "0", "0", "0"]);
    a.sleeve = "a".into();
    let mut b = etf_target("0.5", ["0.2", "0", "0", "0", "0"]);
    b.sleeve = "b".into();
    // SPY: 0.5*0.4 + 0.5*0.2 = 0.3 of 5000 = 1500 = 3 shares (cap raised so the guard does not interfere).
    let p = universe_policy_with(|m| m["exposure"]["max_position"] = json!(0.5));
    let plan = Env::new().plan_ok(&[b, a], &account("50000", "50000", vec![]), &p, &cfg());
    assert_eq!(plan.orders.len(), 1);
    assert_eq!(plan.orders[0].quantity, d("3"));
    assert_eq!(plan.orders[0].sleeve, "a+b");
    // The same instrument on different venues is a conflict.
    let mut c = etf_target("0.5", ["0.2", "0", "0", "0", "0"]);
    c.sleeve = "c".into();
    c.venue = "kraken".into();
    let mut a2 = etf_target("0.5", ["0.2", "0", "0", "0", "0"]);
    a2.sleeve = "a".into();
    let err = Env::new().plan(&[a2, c], &flat(), &prices(), &p, &cfg()).unwrap_err();
    assert!(matches!(err, PlanError::InstrumentConflict(_)), "{err:?}");
}

#[test]
fn capital_base_is_the_smaller_of_broker_equity_and_the_mandate_allocation() {
    let targets = vec![etf_target("1", ["0.2", "0", "0", "0", "0"])];
    // Equity above the allocation: sized on 5000 (allocation), not 20000.
    let rich = account("20000", "20000", vec![]);
    let plan = Env::new().plan_ok(&targets, &rich, &universe_policy(), &cfg());
    assert_eq!(plan.capital_base, d("5000"));
    assert_eq!(find(&plan, "SPY").quantity, d("2"));
    // Equity below the allocation (after a loss): sized on equity.
    let poor = account("2500", "2500", vec![]);
    let plan = Env::new().plan_ok(&targets, &poor, &universe_policy(), &cfg());
    assert_eq!(plan.capital_base, d("2500"));
    assert_eq!(find(&plan, "SPY").quantity, d("1"));
    // Non-positive equity is refused.
    let err = Env::new().plan(&targets, &account("0", "0", vec![]), &prices(), &universe_policy(), &cfg()).unwrap_err();
    assert!(matches!(err, PlanError::EquityInvalid(_)));
}

// ---------------------------------------------------------------------------------------------------------------
// Cash handling
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn no_cash_above_the_reserve_means_no_buys() {
    // 4800 of unmanaged QQQ, 200 cash: below the 250 reserve.
    let mut q = pos("QQQ", "12", "4800");
    q.venue = "alpaca".into();
    let acct = account("5000", "200", vec![q]);
    let targets = vec![etf_target("1", ["0.2", "0", "0", "0", "0"])];
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &cfg());
    assert!(plan.orders.is_empty());
    assert_eq!(skip_reason(&plan, "SPY"), &SkipReason::NoCashAvailable);
    // The unmanaged QQQ position was left alone and is not a planned line.
    assert!(plan.lines.iter().all(|l| l.symbol != "QQQ"));
}

#[test]
fn sell_proceeds_can_fund_buys_only_when_credited() {
    // Hold 4 SPY (2000) and 2750 cash; sleeve wants EFA 1000 (12.5 sh) and SPY 0 -> sells 4 SPY.
    let acct = account("4750", "2750", vec![pos("SPY", "4", "2000")]);
    let targets = vec![etf_target("1", ["0", "0.2", "0", "0", "0"])];
    let mut c = cfg();
    let credited = Env::new().plan_ok(&targets, &acct, &universe_policy(), &c);
    assert_eq!(credited.orders.len(), 2);
    c.credit_sell_proceeds = false;
    let uncredited = Env::new().plan_ok(&targets, &acct, &universe_policy(), &c);
    assert_eq!(uncredited.orders.len(), 2, "cash 2750 alone already covers 1000");
    // Make the starting cash too small to cover the buy without the sale proceeds.
    let mut q = pos("QQQ", "5.375", "2150");
    q.venue = "alpaca".into();
    let tight = account("4750", "600", vec![pos("SPY", "4", "2000"), q]);
    c.credit_sell_proceeds = true;
    let with = Env::new().plan_ok(&targets, &tight, &universe_policy(), &c);
    assert_eq!(find(&with, "EFA").quantity, d("11.875"), "sale proceeds fund the full 950 buy: {with:#?}");
    c.credit_sell_proceeds = false;
    let without = Env::new().plan_ok(&targets, &tight, &universe_policy(), &c);
    let spent = without.orders.iter().filter(|o| o.side == Side::Buy).map(cost).fold(Dec::ZERO, |a, b| a.checked_add(b).unwrap());
    assert!(spent <= d("362.5"), "600 cash - 237.5 reserve: {spent}");
    assert!(spent > d("300"), "but it still buys what fits: {spent}");
}

// ---------------------------------------------------------------------------------------------------------------
// The guard inside the planner
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn denied_orders_are_dropped_and_recorded_with_reasons() {
    // Per-instrument cap 10% = 500: the ~1180 crypto buys breach it; the ETF buys (<= 500 after cash scaling) fit.
    let p = universe_policy_with(|m| {
        m["exposure"]["max_position"] = json!(0.1);
        m["exposure"]["max_asset_class"] = json!({});
    });
    let plan = Env::new().plan_ok(&both_sleeves(), &flat(), &p, &cfg());
    let denied: Vec<&str> = plan.denied.iter().map(|d| d.order.symbol.as_str()).collect();
    assert_eq!(denied, vec!["BTC/USD", "ETH/USD"]);
    for dn in &plan.denied {
        assert_eq!(dn.reasons.iter().map(|r| r.code).collect::<Vec<_>>(), vec![DenialCode::MaxPosition]);
    }
    assert_eq!(plan.orders.len(), 5, "the five ETF orders still pass: {:?}", plan.orders);
}

#[test]
fn the_guard_sees_the_running_state_not_just_the_starting_account() {
    // Two orders per day: the first two (sorted) pass, the rest are denied by the counter.
    let p = universe_policy_with(|m| m["exposure"]["max_orders_per_day"] = json!(2));
    let acct = account("50000", "50000", vec![]);
    let plan = Env::new().plan_ok(&both_sleeves(), &acct, &p, &cfg());
    assert_eq!(plan.orders.len(), 2);
    assert_eq!(plan.denied.len(), 5);
    assert!(plan.denied.iter().all(|d| d.reasons.iter().any(|r| r.code == DenialCode::MaxOrdersPerDay)));
    // Orders already placed today count.
    let mut c = cfg();
    c.day.orders_today = 1;
    let plan = Env::new().plan_ok(&both_sleeves(), &acct, &p, &c);
    assert_eq!(plan.orders.len(), 1);
}

#[test]
fn a_halted_account_plans_sells_only() {
    let mut acct = account("5000", "2730", vec![pos("SPY", "4", "2000"), pos("VNQ", "3", "270")]);
    acct.halted = true;
    let targets = vec![etf_target("1", ["0.2", "0.2", "0.2", "0.2", "0"])];
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &cfg());
    assert!(plan.orders.iter().all(|o| o.side == Side::Sell), "{:?}", plan.orders);
    assert_eq!(plan.orders.len(), 2);
    assert_eq!(plan.denied.len(), 3);
    assert!(plan.denied.iter().all(|d| d.reasons.iter().any(|r| r.code == DenialCode::AccountHalted)));
}

#[test]
fn a_stale_price_is_denied_by_the_guard_and_a_missing_one_is_skipped() {
    let mut px = prices();
    px.get_mut("SPY").unwrap().as_of = now() - chrono::Duration::seconds(301);
    px.remove("EFA");
    let targets = vec![etf_target("1", ["0.2", "0.2", "0", "0", "0"])];
    let plan = Env::new().plan(&targets, &account("50000", "50000", vec![]), &px, &universe_policy(), &cfg()).unwrap();
    assert!(plan.orders.is_empty());
    assert_eq!(plan.denied.len(), 1);
    assert_eq!(plan.denied[0].reasons[0].code, DenialCode::PriceStale);
    assert_eq!(skip_reason(&plan, "EFA"), &SkipReason::NoPrice);
}

#[test]
fn an_unusable_mandate_denies_every_order_with_a_recorded_reason() {
    let bare = Policy::compile(&body_with(|_| {})); // no envelope
    let plan = Env::new().plan_ok(&both_sleeves(), &flat(), &bare, &cfg());
    assert!(plan.orders.is_empty());
    assert_eq!(plan.denied.len(), 7);
    assert!(plan.denied.iter().all(|d| d.reasons[0].code == DenialCode::MandateNotActive));
}

#[test]
fn a_short_position_is_never_touched() {
    let acct = account("5000", "5500", vec![pos("SPY", "-1", "-500")]);
    let targets = vec![etf_target("1", ["0.2", "0", "0", "0", "0"])];
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy_with(|m| m["universe"]["shorting"] = json!(true)), &cfg());
    assert!(plan.orders.is_empty());
    assert_eq!(skip_reason(&plan, "SPY"), &SkipReason::ShortPositionHeld);
}

// ---------------------------------------------------------------------------------------------------------------
// Tags, determinism, digest
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn client_tags_are_deterministic_unique_and_within_broker_limits() {
    let t0 = at("2026-10-01T14:00:00Z");
    let a = client_tag("acct-1", t0, "etf", "SPY", Side::Buy);
    assert_eq!(a, client_tag("acct-1", t0, "etf", "SPY", Side::Buy));
    assert!(a.starts_with("rb1:20261001T140000Z:SPY:buy:"), "{a}");
    let variants = [
        client_tag("acct-2", t0, "etf", "SPY", Side::Buy),
        client_tag("acct-1", at("2026-10-01T14:00:01Z"), "etf", "SPY", Side::Buy),
        client_tag("acct-1", t0, "crypto", "SPY", Side::Buy),
        client_tag("acct-1", t0, "etf", "EFA", Side::Buy),
        client_tag("acct-1", t0, "etf", "SPY", Side::Sell),
    ];
    let mut all: Vec<String> = variants.to_vec();
    all.push(a);
    let unique: std::collections::BTreeSet<_> = all.iter().collect();
    assert_eq!(unique.len(), all.len(), "{all:?}");
    for t in &all {
        assert!(t.len() <= 128 && t.bytes().all(|b| (0x20..=0x7e).contains(&b)), "{t}");
    }
    // Symbols that share a readable prefix still differ by hash.
    assert_ne!(
        client_tag("a", t0, "s", "BTC/USD", Side::Buy),
        client_tag("a", t0, "s", "BTC-USD", Side::Buy)
    );
}

#[test]
fn replanning_the_same_inputs_is_identical_including_tags_and_digest() {
    let env = Env::new();
    let a = env.plan_ok(&both_sleeves(), &flat(), &universe_policy(), &cfg());
    let b = env.plan_ok(&both_sleeves(), &flat(), &universe_policy(), &cfg());
    assert_eq!(a, b);
    let tags: Vec<&str> = a.orders.iter().map(|o| o.tag.as_str()).collect();
    let unique: std::collections::BTreeSet<_> = tags.iter().collect();
    assert_eq!(unique.len(), tags.len());
    // A later schedule slot yields different tags (a new run), same digest inputs otherwise differ.
    let mut later = cfg();
    later.scheduled_for = at("2026-10-02T14:00:00Z");
    let c = env.plan_ok(&both_sleeves(), &flat(), &universe_policy(), &later);
    assert!(a.orders.iter().zip(&c.orders).all(|(x, y)| x.tag != y.tag));
    assert_ne!(a.inputs_digest, c.inputs_digest);
}

#[test]
fn the_plan_is_stable_under_input_ordering() {
    let acct = account("5000", "2730", vec![pos("SPY", "4", "2000"), pos("VNQ", "3", "270")]);
    let mut acct_rev = acct.clone();
    acct_rev.positions.reverse();
    let t1 = both_sleeves();
    let mut t2 = both_sleeves();
    t2.reverse();
    for t in &mut t2 {
        t.weights.reverse();
    }
    let env = Env::new();
    let a = env.plan_ok(&t1, &acct, &universe_policy(), &cfg());
    let b = env.plan_ok(&t2, &acct_rev, &universe_policy(), &cfg());
    assert_eq!(a, b);
}

#[test]
fn the_digest_changes_with_any_input() {
    let env = Env::new();
    let base = env.plan_ok(&both_sleeves(), &flat(), &universe_policy(), &cfg());
    // price
    let mut px = prices();
    px.get_mut("SPY").unwrap().price = d("501");
    let p = env.plan(&both_sleeves(), &flat(), &px, &universe_policy(), &cfg()).unwrap();
    assert_ne!(base.inputs_digest, p.inputs_digest);
    // cash
    let p = env.plan_ok(&both_sleeves(), &account("5000", "4999", vec![]), &universe_policy(), &cfg());
    assert_ne!(base.inputs_digest, p.inputs_digest);
    // mandate
    let p = env.plan_ok(&both_sleeves(), &flat(), &universe_policy_with(|m| m["exposure"]["max_position"] = json!(0.3)), &cfg());
    assert_ne!(base.inputs_digest, p.inputs_digest);
    // a parameter
    let mut c = cfg();
    c.fee_rate = d("0.003");
    let p = env.plan_ok(&both_sleeves(), &flat(), &universe_policy(), &c);
    assert_ne!(base.inputs_digest, p.inputs_digest);
    // a venue rule
    let mut env2 = Env::new();
    env2.pairs.apply_overrides_json(r#"{"XBTUSD": {"ordermin": "0.0002"}}"#).unwrap();
    let p = env2.plan_ok(&both_sleeves(), &flat(), &universe_policy(), &cfg());
    assert_ne!(base.inputs_digest, p.inputs_digest);
    // equal decimals written with a different scale do not change it
    let mut c = cfg();
    c.risk_scale = d("1.000");
    assert_eq!(base.inputs_digest, env.plan_ok(&both_sleeves(), &flat(), &universe_policy(), &c).inputs_digest);
}

// ---------------------------------------------------------------------------------------------------------------
// reference-rules integration
// ---------------------------------------------------------------------------------------------------------------

fn decision(symbol: &str, weight: f64) -> InstrumentDecision {
    InstrumentDecision {
        symbol: symbol.into(),
        close: 1.0,
        sma: 1.0,
        signal: if weight > 0.0 { Signal::Long } else { Signal::Cash },
        weight,
    }
}

#[test]
fn sleeve_targets_are_built_from_reference_rule_decisions() {
    let etf = EtfDecision {
        decision_date: NaiveDate::from_ymd_opt(2026, 9, 30).unwrap(),
        month_end_dates: vec![],
        instruments: ETFS.iter().map(|s| decision(s, if *s == "VNQ" { 0.0 } else { 0.2 })).collect(),
    };
    let crypto = CryptoDecision {
        decision_date: NaiveDate::from_ymd_opt(2026, 9, 30).unwrap(),
        window_start: vec![],
        instruments: vec![decision("BTC", 0.5), decision("ETH", 0.0)],
    };
    let e = SleeveTarget::from_etf("etf", d("0.5"), "alpaca", "us_etf", &etf).unwrap();
    assert_eq!(e.weights[0], TargetWeight { symbol: "SPY".into(), weight: d("0.2") });
    assert_eq!(e.weights[4].weight, d("0"));
    let c = SleeveTarget::from_crypto("crypto", d("0.5"), "kraken", "crypto_spot", "USD", &crypto).unwrap();
    assert_eq!(c.weights[0], TargetWeight { symbol: "BTC/USD".into(), weight: d("0.5") });
    let plan = Env::new().plan_ok(&[e, c], &account("50000", "50000", vec![]), &universe_policy(), &cfg());
    assert!(plan.orders.iter().all(|o| o.symbol != "VNQ" && o.symbol != "ETH/USD"));
    assert_eq!(plan.orders.len(), 5);
}

#[test]
fn venue_rule_adapters_use_the_adapters_own_rules() {
    let env = Env::new();
    let k = KrakenRules { pairs: &env.pairs };
    assert_eq!(k.round_quantity("BTC/USD", Side::Buy, d("0.123456789"), d("60000")).unwrap(), d("0.12345678"));
    assert!(matches!(
        k.round_quantity("BTC/USD", Side::Buy, d("0.00009"), d("60000")),
        Err(SizeRefusal::BelowMinQuantity { .. })
    ));
    assert!(matches!(k.round_quantity("DOGE/USD", Side::Buy, d("1"), d("1")), Err(SizeRefusal::UnknownInstrument(_))));
    // costmin 0.5: 0.0001 BTC at a price of 100 is worth 0.01.
    assert!(matches!(k.round_quantity("BTC/USD", Side::Buy, d("0.0001"), d("100")), Err(SizeRefusal::BelowMinCost { .. })));
    let a = AlpacaRules { assets: &env.assets, options: &env.opts };
    assert_eq!(a.round_quantity("SPY", Side::Buy, d("1.2345678919"), d("500")).unwrap(), d("1.234567891"));
    assert!(matches!(a.round_quantity("SPY", Side::Buy, d("0.001"), d("500")), Err(SizeRefusal::BelowMinCost { .. })));
    assert!(a.fingerprint("SPY").contains("SPY"));
}

#[test]
fn fingerprints_are_stable_and_venue_book_is_case_insensitive() {
    let env = Env::new();
    let k = KrakenRules { pairs: &env.pairs };
    assert_eq!(k.fingerprint("BTC/USD"), k.fingerprint("BTC/USD"));
    let book = VenueRuleBook::new().with(" Kraken ", &k);
    assert!(book.get("KRAKEN").is_some() && book.get("alpaca").is_none());
}

// ---------------------------------------------------------------------------------------------------------------
// Capital base shared with the guard (owner change: one base for sizing AND the guard's percentage limits)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_cash_reserve_is_a_fraction_of_the_capital_base_and_the_cash_is_the_actual_cash() {
    // Equity 20000 (much of it not held as modelled cash), cash 300, allocation 5000: reserve = 5% of 5000 = 250,
    // so 50 is available. With the raw equity the reserve would be 1000 and nothing could be bought.
    let acct = account("20000", "300", vec![]);
    let targets = vec![crypto_target("1", "0", "0.5")]; // ETH target = 0.5 * 5000 = 2500 (way above the cash)
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy(), &cfg());
    assert_eq!(plan.capital_base, d("5000"));
    let eth = find(&plan, "ETH/USD");
    let spent = cost(eth);
    assert!(spent <= d("50"), "only 300 - 250 = 50 is available, planned {spent}");
    assert!(spent > d("49"), "and the plan uses it: {spent}");
    // Actual cash still binds when the reserve is met: cash 250 exactly leaves nothing to spend.
    let tight = account("20000", "250", vec![]);
    let plan = Env::new().plan_ok(&targets, &tight, &universe_policy(), &cfg());
    assert!(plan.orders.is_empty());
    assert_eq!(skip_reason(&plan, "ETH/USD"), &SkipReason::NoCashAvailable);
}

#[test]
fn the_guard_agrees_with_the_planner_when_equity_exceeds_the_allocation() {
    // 20000 of equity, all cash. ETF sleeve 100%, SPY weight 1: target 5000 (the base), but max_position is 25% of
    // the base = 1250. The planner sizes on the base and the guard denies the oversized order for the same base.
    let targets = vec![etf_target("1", ["1", "0", "0", "0", "0"])];
    let acct = account("20000", "20000", vec![]);
    let plan = Env::new().plan_ok(&targets, &acct, &universe_policy_with(|m| m["exposure"]["max_order_notional"]["amount"] = json!("5000.00")), &cfg());
    assert_eq!(plan.capital_base, d("5000"));
    assert!(plan.orders.is_empty(), "{:?}", plan.orders);
    assert_eq!(plan.denied.len(), 1);
    assert!(plan.denied[0].reasons.iter().any(|r| r.code == DenialCode::MaxPosition), "{:?}", plan.denied);
}
