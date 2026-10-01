//! Seeded property tests of the planner (no `rand`: SplitMix64). Every case is reproducible from its seed, which
//! is printed on failure. The oracle below re-derives the mandate limits from the JSON and replays the plan with
//! plain decimal arithmetic; it does NOT call the guard, so a wrong guard cannot vouch for itself.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use broker_adapters::alpaca::{self, AssetTable};
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::Side;
use common::*;
use rebalancer_core::dec_math::{abs, add, mul, sub};
use rebalancer_core::guard::{AccountView, Position, PricePoint};
use rebalancer_core::planner::{OrderPlan, OrderPlanner, PlanConfig, SleeveTarget, TargetWeight};
use rebalancer_core::policy::Policy;
use rebalancer_core::venue::{AlpacaRules, KrakenRules, VenueRuleBook, VenueRules};
use rebalancer_core::Dec;
use serde_json::{json, Value};

const ETFS: [&str; 5] = ["SPY", "EFA", "IEF", "DBC", "VNQ"];
const ALL: [&str; 7] = ["SPY", "EFA", "IEF", "DBC", "VNQ", "BTC/USD", "ETH/USD"];
const CASES: u64 = 400;

fn dec_of(f: f64) -> Dec {
    Dec::parse(&format!("{f}")).unwrap()
}

// ---------------------------------------------------------------------------------------------------------------
// Case generation
// ---------------------------------------------------------------------------------------------------------------

#[derive(Clone)]
struct Limits {
    max_position: f64,
    crypto_cap: f64,
    max_gross: f64,
    reserve: f64,
    notional: f64,
    orders: u64,
    turnover: f64,
}

struct Case {
    seed: u64,
    limits: Limits,
    policy: Policy,
    account: AccountView,
    prices: BTreeMap<String, PricePoint>,
    targets: Vec<SleeveTarget>,
    cfg: PlanConfig,
    whole_shares: bool,
}

fn mandate_edit(l: &Limits) -> impl FnOnce(&mut Value) + '_ {
    move |m| {
        m["universe"]["instrument_allow"] = json!(ALL);
        m["exposure"]["max_position"] = json!(l.max_position);
        m["exposure"]["max_asset_class"] = json!({"crypto_spot": l.crypto_cap});
        m["universe"]["leverage_max_gross"] = json!(l.max_gross.max(1.0));
        m["exposure"]["max_gross"] = json!(l.max_gross);
        m["exposure"]["max_net"] = json!(l.max_gross);
        m["exposure"]["max_order_notional"]["amount"] = json!(format!("{:.2}", l.notional));
        m["exposure"]["max_orders_per_day"] = json!(l.orders);
        m["exposure"]["max_turnover_per_day"] = json!(l.turnover);
        m["capital"]["min_cash_reserve"] = json!(l.reserve);
    }
}

fn pick_dec(rng: &mut SplitMix64, items: &[&str]) -> Dec {
    d(items[rng.range(0, items.len() as u64 - 1) as usize])
}

fn price_for(rng: &mut SplitMix64, symbol: &str) -> Dec {
    let (lo, hi) = match symbol {
        "SPY" => (300, 600),
        "EFA" => (60, 100),
        "IEF" => (80, 110),
        "DBC" => (20, 30),
        "VNQ" => (70, 110),
        "BTC/USD" => (20_000, 100_000),
        "ETH/USD" => (1_000, 5_000),
        _ => (300, 500),
    };
    let whole = rng.range(lo, hi);
    let cents = rng.range(0, 99);
    d(&format!("{whole}.{cents:02}"))
}

fn generate(seed: u64, permissive: bool) -> Case {
    let mut rng = SplitMix64(seed.wrapping_mul(0x1234_5678_9ABC_DEF1) ^ 0xC0FF_EE00);
    let limits = if permissive {
        Limits { max_position: 1.0, crypto_cap: 1.0, max_gross: 2.0, reserve: *rng.pick(&[0.0, 0.01, 0.05, 0.1]), notional: 5000.0, orders: 50, turnover: 3.0 }
    } else {
        let max_gross = *rng.pick(&[0.6, 0.8, 1.0]);
        let positions: Vec<f64> = [0.1, 0.15, 0.2, 0.25, 0.3, 0.5].into_iter().filter(|p| *p <= max_gross).collect();
        let classes: Vec<f64> = [0.2, 0.3, 0.4, 0.6].into_iter().filter(|p| *p <= max_gross).collect();
        Limits {
            max_position: *rng.pick(&positions),
            crypto_cap: *rng.pick(&classes),
            max_gross,
            reserve: *rng.pick(&[0.0, 0.01, 0.05, 0.1, 0.2]),
            notional: *rng.pick(&[300.0, 1000.0, 1500.0, 5000.0]),
            orders: *rng.pick(&[1, 2, 3, 5, 8, 20]),
            turnover: *rng.pick(&[0.1, 0.3, 0.5, 1.0]),
        }
    };
    let policy = policy_with(mandate_edit(&limits));
    assert!(policy.limits().is_some(), "seed {seed}: generated mandate must validate: {:?}", policy.body);

    let equity = rng.range(1_000, 20_000);
    let mut prices = BTreeMap::new();
    for s in ALL.iter().chain(["QQQ"].iter()) {
        let px = price_for(&mut rng, s);
        prices.insert(s.to_string(), PricePoint { price: px, as_of: now() });
    }
    let mut positions = Vec::new();
    let mut invested = Dec::ZERO;
    for s in ALL.iter().chain(["QQQ"].iter()) {
        if !rng.chance(40) {
            continue;
        }
        let px = prices[*s].price;
        let frac = rng.range(1, 100); // percent of a 10% slice
        // Existing positions are sized against the capital base (min(equity, allocation 5000)), like the caps the
        // guard checks them against; sizing them on the raw equity would put many accounts over the gross cap
        // before the plan starts (which the guard rightly refuses).
        let target_value = mul(d(&equity.min(5_000).to_string()), d(&format!("0.{:03}", frac))).unwrap();
        let qty = rebalancer_core::dec_math::div_floor(target_value, px, 6).unwrap();
        if qty.is_zero() {
            continue;
        }
        let short = ETFS.contains(s) && rng.chance(5);
        let qty = if short { rebalancer_core::dec_math::neg(qty).unwrap() } else { qty };
        let mv = mul(qty, px).unwrap();
        invested = add(invested, mv).unwrap();
        let (venue, class) = venue_and_class(s);
        positions.push(Position { symbol: s.to_string(), venue: venue.into(), asset_class: class.into(), quantity: qty, market_value: mv });
    }
    let equity_dec = d(&equity.to_string());
    let cash = sub(equity_dec, invested).unwrap();
    let mut account = AccountView {
        account_id: format!("acct-{seed}"),
        ccy: "USD".into(),
        equity: equity_dec,
        cash,
        positions,
        halted: !permissive && rng.chance(8),
        now: now(),
    };
    if account.cash.is_negative() {
        account.cash = Dec::ZERO; // never a negative cash balance
    }
    if !permissive {
        if rng.chance(6) {
            if let Some(p) = prices.get_mut("SPY") {
                p.as_of = now() - chrono::Duration::seconds(400);
            }
        }
        if rng.chance(4) {
            prices.remove("DBC");
        }
    }

    let etf_share = *rng.pick(&["0.2", "0.4", "0.5", "0.6", "1"]);
    let crypto_options: Vec<&str> = ["0", "0.2", "0.4", "0.5"].into_iter().filter(|c| d(c).checked_add(d(etf_share)).unwrap() <= d("1")).collect();
    let crypto_share = *rng.pick(&crypto_options);
    let mut targets = vec![SleeveTarget {
        sleeve: "etf".into(),
        share: d(etf_share),
        venue: "alpaca".into(),
        asset_class: "us_etf".into(),
        weights: ETFS
            .iter()
            .map(|s| TargetWeight { symbol: s.to_string(), weight: if rng.chance(65) { d("0.2") } else { d("0") } })
            .collect(),
    }];
    if crypto_share != "0" {
        targets.push(SleeveTarget {
            sleeve: "crypto".into(),
            share: d(crypto_share),
            venue: "kraken".into(),
            asset_class: "crypto_spot".into(),
            weights: vec![
                TargetWeight { symbol: "BTC/USD".into(), weight: if rng.chance(65) { d("0.5") } else { d("0") } },
                TargetWeight { symbol: "ETH/USD".into(), weight: if rng.chance(65) { d("0.5") } else { d("0") } },
            ],
        });
    }
    let mut cfg = PlanConfig::new(
        at("2026-10-01T14:00:00Z"),
        pick_dec(&mut rng, &["1", "0.75", "0.5", "0.25", "0.1"]),
        pick_dec(&mut rng, &["0", "5", "25"]),
        pick_dec(&mut rng, &["0", "0.02", "0.1"]),
        pick_dec(&mut rng, &["0", "0.001", "0.0026", "0.01"]),
    );
    cfg.credit_sell_proceeds = rng.chance(70);
    if !permissive && rng.chance(20) {
        cfg.day.orders_today = 2;
        cfg.day.turnover_today = d("500");
    }
    Case { seed, limits, policy, account, prices, targets, cfg, whole_shares: rng.chance(30) }
}

struct Env {
    pairs: PairTable,
    assets: AssetTable,
    opts: alpaca::PrepareOptions,
}

fn env(whole_shares: bool) -> Env {
    let assets = if whole_shares {
        let rows: Vec<Value> = ETFS.iter().map(|s| json!({"symbol": s, "tradable": true, "fractionable": false, "status": "active"})).collect();
        AssetTable::from_assets_json(&Value::Array(rows).to_string()).unwrap()
    } else {
        AssetTable::builtin()
    };
    Env {
        pairs: PairTable::builtin(),
        assets,
        opts: alpaca::PrepareOptions { allow_extended_hours: false, min_notional: d("1"), own_tag_prefix: None, refuse_builtin_assets: false },
    }
}

fn run(env: &Env, c: &Case) -> OrderPlan {
    run_with(env, &c.targets, &c.account, &c.prices, &c.policy, &c.cfg)
}

fn run_with(
    env: &Env,
    targets: &[SleeveTarget],
    account: &AccountView,
    prices: &BTreeMap<String, PricePoint>,
    policy: &Policy,
    cfg: &PlanConfig,
) -> OrderPlan {
    let k = KrakenRules { pairs: &env.pairs };
    let a = AlpacaRules { assets: &env.assets, options: &env.opts };
    let book = VenueRuleBook::new().with("kraken", &k).with("alpaca", &a);
    OrderPlanner::plan(targets, account, prices, &book, policy, cfg).unwrap_or_else(|e| panic!("plan failed: {e}"))
}

// ---------------------------------------------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------------------------------------------

struct Replay {
    qty: BTreeMap<String, Dec>,
    mv: BTreeMap<String, Dec>,
    class: BTreeMap<String, String>,
    cash: Dec,
    orders: u64,
    turnover: Dec,
}

fn replay_start(c: &Case) -> Replay {
    let mut r = Replay { qty: BTreeMap::new(), mv: BTreeMap::new(), class: BTreeMap::new(), cash: c.account.cash, orders: u64::from(c.cfg.day.orders_today), turnover: c.cfg.day.turnover_today };
    for p in &c.account.positions {
        r.qty.insert(p.symbol.clone(), p.quantity);
        r.mv.insert(p.symbol.clone(), p.market_value);
        r.class.insert(p.symbol.clone(), p.asset_class.clone());
    }
    r
}

fn gross_of(r: &Replay) -> Dec {
    r.mv.values().fold(Dec::ZERO, |a, v| add(a, abs(*v).unwrap()).unwrap())
}

/// Replays the plan and returns the first violated rule, if any. Also returns the final gross exposure.
fn oracle(c: &Case, plan: &OrderPlan) -> Result<Dec, String> {
    let l = &c.limits;
    // Independent restatement of the capital base (NOT a call into the crate): every generated mandate keeps the
    // baseline allocation of 5000, so the base is min(broker equity, 5000).
    let e = std::cmp::min(c.account.equity, d("5000"));
    let mut r = replay_start(c);
    let cap = |ratio: f64| mul(dec_of(ratio), e).unwrap();
    let mut tags = BTreeSet::new();
    for o in &plan.orders {
        let ctx = |what: &str| format!("seed {}: {what} for {o:?}", c.seed);
        if !tags.insert(o.tag.clone()) {
            return Err(ctx("duplicate tag"));
        }
        if !ALL.contains(&o.symbol.as_str()) {
            return Err(ctx("instrument outside the allow list"));
        }
        let px = c.prices.get(&o.symbol).ok_or_else(|| ctx("no price for an ordered instrument"))?;
        let age = c.account.now.signed_duration_since(px.as_of).num_seconds();
        if !(-5..=300).contains(&age) {
            return Err(ctx("order sized on a stale price"));
        }
        if px.price != o.price || mul(o.quantity, o.price).unwrap() != o.notional {
            return Err(ctx("order price or notional inconsistent"));
        }
        if !o.quantity.is_positive() {
            return Err(ctx("non-positive quantity"));
        }
        let held = r.qty.get(&o.symbol).copied().unwrap_or(Dec::ZERO);
        let reducing = o.side == Side::Sell;
        if c.account.halted && !reducing {
            return Err(ctx("a buy while halted"));
        }
        if o.side == Side::Sell && (!held.is_positive() || o.quantity > held) {
            return Err(ctx(&format!("sells {} but holds {held}", o.quantity)));
        }
        let signed = if o.side == Side::Buy { o.notional } else { rebalancer_core::dec_math::neg(o.notional).unwrap() };
        let mv_before = r.mv.get(&o.symbol).copied().unwrap_or(Dec::ZERO);
        let mv_after = add(mv_before, signed).unwrap();
        r.class.entry(o.symbol.clone()).or_insert_with(|| o.asset_class.clone());
        let cash_after = sub(sub(r.cash, signed).unwrap(), o.est_fee).unwrap();
        if !reducing {
            if o.notional > dec_of(l.notional) {
                return Err(ctx("order notional above the cap"));
            }
            if mv_after > cap(l.max_position) {
                return Err(ctx("position cap"));
            }
            let mut class_gross = abs(mv_after).unwrap();
            let mut gross = abs(mv_after).unwrap();
            let mut net = mv_after;
            for (sym, mv) in &r.mv {
                if *sym == o.symbol {
                    continue;
                }
                gross = add(gross, abs(*mv).unwrap()).unwrap();
                net = add(net, *mv).unwrap();
                if r.class[sym] == o.asset_class {
                    class_gross = add(class_gross, abs(*mv).unwrap()).unwrap();
                }
            }
            if o.asset_class == "crypto_spot" && class_gross > cap(l.crypto_cap) {
                return Err(ctx("asset-class cap"));
            }
            if gross > cap(l.max_gross) {
                return Err(ctx("gross cap"));
            }
            if abs(net).unwrap() > cap(l.max_gross) {
                return Err(ctx("net cap"));
            }
            if cash_after < mul(dec_of(l.reserve), e).unwrap() {
                return Err(ctx("cash reserve"));
            }
        }
        if !(c.account.halted && reducing) {
            if r.orders + 1 > l.orders {
                return Err(ctx("orders per day"));
            }
            if add(r.turnover, o.notional).unwrap() > cap(l.turnover) {
                return Err(ctx("turnover"));
            }
        }
        r.orders += 1;
        r.turnover = add(r.turnover, o.notional).unwrap();
        r.cash = cash_after;
        r.qty.insert(o.symbol.clone(), add(held, if o.side == Side::Buy { o.quantity } else { rebalancer_core::dec_math::neg(o.quantity).unwrap() }).unwrap());
        r.mv.insert(o.symbol.clone(), mv_after);
    }
    Ok(gross_of(&r))
}

fn buys_within_cash(c: &Case, plan: &OrderPlan) -> Result<(), String> {
    let mut budget = c.account.cash;
    if c.cfg.credit_sell_proceeds {
        for o in plan.orders.iter().filter(|o| o.side == Side::Sell) {
            budget = add(budget, sub(o.notional, o.est_fee).unwrap()).unwrap();
        }
    }
    let spent = plan
        .orders
        .iter()
        .filter(|o| o.side == Side::Buy)
        .fold(Dec::ZERO, |a, o| add(a, add(o.notional, o.est_fee).unwrap()).unwrap());
    let reserve = mul(dec_of(c.limits.reserve), std::cmp::min(c.account.equity, d("5000"))).unwrap();
    if spent.is_positive() && spent > sub(budget, reserve).unwrap() {
        return Err(format!("seed {}: buys cost {spent} but only {} is available (cash budget {budget}, reserve {reserve})", c.seed, sub(budget, reserve).unwrap()));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------------------------------------------

#[derive(Default)]
struct Coverage {
    with_orders: u32,
    with_sells: u32,
    with_buys: u32,
    with_denied: u32,
    with_skipped: u32,
    halted_with_orders: u32,
    whole_share_orders: u32,
}

#[test]
fn a_plan_never_violates_the_policy_and_never_oversells_or_overspends() {
    let mut cov = Coverage::default();
    for seed in 0..CASES {
        let c = generate(seed, false);
        let env = env(c.whole_shares);
        let plan = run(&env, &c);
        if let Err(msg) = oracle(&c, &plan) {
            panic!("{msg}\nplan: {plan:#?}");
        }
        if let Err(msg) = buys_within_cash(&c, &plan) {
            panic!("{msg}\nplan: {plan:#?}");
        }
        // Denied orders are not in the plan; skipped instruments have no order.
        for dn in &plan.denied {
            assert!(!plan.orders.iter().any(|o| o.tag == dn.order.tag), "seed {seed}: denied order is also planned");
            assert!(!dn.reasons.is_empty());
        }
        // Sells first, then buys.
        let first_buy = plan.orders.iter().position(|o| o.side == Side::Buy).unwrap_or(plan.orders.len());
        assert!(plan.orders[first_buy..].iter().all(|o| o.side == Side::Buy), "seed {seed}: a sell after a buy");
        // Total held-quantity accounting per symbol.
        for s in ALL {
            let held = c.account.position(s).map_or(Dec::ZERO, |p| p.quantity);
            let sold = plan.orders.iter().filter(|o| o.symbol == s && o.side == Side::Sell).fold(Dec::ZERO, |a, o| add(a, o.quantity).unwrap());
            assert!(sold <= std::cmp::max(held, Dec::ZERO), "seed {seed}: sold {sold} {s} but held {held}");
        }
        // Every quantity is exactly what the adapters' own rules would send (rounding is idempotent).
        let k = KrakenRules { pairs: &env.pairs };
        let a = AlpacaRules { assets: &env.assets, options: &env.opts };
        for o in &plan.orders {
            let rules: &dyn VenueRules = if o.venue == "kraken" { &k } else { &a };
            assert_eq!(rules.round_quantity(&o.symbol, o.side, o.quantity, o.price), Ok(o.quantity), "seed {seed}: {o:?}");
            if c.whole_shares && o.venue == "alpaca" {
                assert_eq!(o.quantity.round_dp(0, broker_adapters::decimal::Rounding::Floor).unwrap(), o.quantity, "seed {seed}: fractional whole-share order");
                cov.whole_share_orders += 1;
            }
        }
        cov.with_orders += u32::from(!plan.orders.is_empty());
        cov.with_sells += u32::from(plan.orders.iter().any(|o| o.side == Side::Sell));
        cov.with_buys += u32::from(plan.orders.iter().any(|o| o.side == Side::Buy));
        cov.with_denied += u32::from(!plan.denied.is_empty());
        cov.with_skipped += u32::from(!plan.skipped.is_empty());
        cov.halted_with_orders += u32::from(c.account.halted && !plan.orders.is_empty());
    }
    // The generator must actually exercise the interesting paths, or the properties above prove little.
    let n = CASES as u32;
    assert!(cov.with_orders > n * 6 / 10, "too few cases with orders: {}", cov.with_orders);
    assert!(cov.with_sells > n / 10, "too few cases with sells: {}", cov.with_sells);
    assert!(cov.with_buys > n * 5 / 10, "too few cases with buys: {}", cov.with_buys);
    assert!(cov.with_denied > n / 10, "too few cases with denials: {}", cov.with_denied);
    assert!(cov.with_skipped > n / 10, "too few cases with skips: {}", cov.with_skipped);
    assert!(cov.halted_with_orders > 3, "too few halted cases with orders: {}", cov.halted_with_orders);
    assert!(cov.whole_share_orders > 20, "too few whole-share orders: {}", cov.whole_share_orders);
}

#[test]
fn replanning_the_same_inputs_is_identical() {
    for seed in 0..CASES {
        let c = generate(seed, false);
        let env = env(c.whole_shares);
        let a = run(&env, &c);
        let b = run(&env, &c);
        assert_eq!(a, b, "seed {seed}");
        assert_eq!(a.inputs_digest, b.inputs_digest);
    }
}

fn shuffle<T>(rng: &mut SplitMix64, items: &mut [T]) {
    for i in (1..items.len()).rev() {
        let j = rng.range(0, i as u64) as usize;
        items.swap(i, j);
    }
}

#[test]
fn the_plan_is_stable_under_input_ordering() {
    for seed in 0..CASES {
        let c = generate(seed, false);
        let env = env(c.whole_shares);
        let base = run(&env, &c);
        let mut rng = SplitMix64(seed ^ 0xDEAD_BEEF);
        let mut targets = c.targets.clone();
        shuffle(&mut rng, &mut targets);
        for t in &mut targets {
            shuffle(&mut rng, &mut t.weights);
        }
        let mut account = c.account.clone();
        shuffle(&mut rng, &mut account.positions);
        let shuffled = run_with(&env, &targets, &account, &c.prices, &c.policy, &c.cfg);
        assert_eq!(base, shuffled, "seed {seed}: reordering inputs changed the plan");
    }
}

#[test]
fn scaling_risk_scale_down_never_increases_gross_exposure() {
    // With a permissive mandate the guard never binds, so any difference is the planner's own doing.
    let scales = ["1", "0.75", "0.5", "0.25", "0.1"];
    let tolerance = d("0.01"); // flooring dust on fractional venues (whole-share venues are excluded)
    for seed in 0..CASES {
        let mut c = generate(seed, true);
        c.whole_shares = false;
        let env = env(false);
        let mut last: Option<(Dec, Dec, &str)> = None;
        for s in scales {
            c.cfg.risk_scale = d(s);
            let plan = run(&env, &c);
            assert!(plan.denied.is_empty(), "seed {seed} scale {s}: permissive mandate denied {:?}", plan.denied);
            let gross = oracle(&c, &plan).unwrap_or_else(|m| panic!("{m}"));
            let targets = plan.lines.iter().fold(Dec::ZERO, |a, l| add(a, l.target_notional).unwrap());
            if let Some((prev_gross, prev_targets, prev_s)) = last {
                assert!(targets <= prev_targets, "seed {seed}: total target rose from scale {prev_s} to {s}");
                assert!(
                    gross <= add(prev_gross, tolerance).unwrap(),
                    "seed {seed}: gross exposure rose from {prev_gross} at scale {prev_s} to {gross} at scale {s}"
                );
            }
            last = Some((gross, targets, s));
        }
    }
}
