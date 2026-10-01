//! Seeded property tests of the SIGNED planner path (no `rand`: SplitMix64). Every case is reproducible from its seed,
//! printed on failure. As in `properties.rs`, the oracle re-derives limits from the generated numbers and replays the
//! plan with plain decimal arithmetic; it never calls the guard, so a wrong guard cannot vouch for itself.
//!
//! Long-only equivalence to the pre-extension planner is `long_only_golden.rs` (hashes generated from the unmodified
//! code). The equivalence property here is the complementary one: opting a sleeve in with non-negative weights and
//! ample funding changes nothing about what is traded.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use broker_adapters::alpaca::{self, AssetTable};
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::Side;
use common::*;
use rebalancer_core::dec_math::{abs, add, div_floor, mul, neg, sub};
use rebalancer_core::guard::{AccountView, Position, PricePoint};
use rebalancer_core::planner::{OrderPlan, OrderPlanner, PlanConfig, PlanError, SleeveTarget, TargetWeight};
use rebalancer_core::policy::Policy;
use rebalancer_core::venue::{AlpacaRules, KrakenRules, VenueRuleBook};
use rebalancer_core::Dec;
use broker_adapters::decimal::Rounding;
use serde_json::{json, Value};

const ETFS: [&str; 5] = ["SPY", "EFA", "IEF", "DBC", "VNQ"];
const CRYPTO: [&str; 2] = ["BTC/USD", "ETH/USD"];
const ALLOW: [&str; 7] = ["SPY", "EFA", "IEF", "DBC", "VNQ", "BTC/USD", "ETH/USD"];
const CASES: u64 = 500;
const HUGE_BP: &str = "1000000000";

fn dec_of(f: f64) -> Dec {
    Dec::parse(&format!("{f}")).unwrap()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Tight and loose mandates, random holdings (long and short), random and sometimes missing buying power.
    Random,
    /// Loose mandate, huge buying power, nothing capped: no denial can be the reason a plan stops short of target.
    Permissive,
    /// Permissive, but long-only holdings and non-negative weights summing to at most 1 (usable as a long-only plan).
    Nonneg,
}

struct Case {
    seed: u64,
    shorting: bool,
    max_gross: f64,
    reserve: f64,
    policy: Policy,
    account: AccountView,
    prices: BTreeMap<String, PricePoint>,
    targets: Vec<SleeveTarget>,
    cfg: PlanConfig,
    whole: bool,
    cb: Dec,
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
    d(&format!("{}.{:02}", rng.range(lo, hi), rng.range(0, 99)))
}

fn pick_dec(rng: &mut SplitMix64, items: &[&str]) -> Dec {
    d(items[rng.range(0, items.len() as u64 - 1) as usize])
}

fn generate(seed: u64, kind: Kind) -> Case {
    let mut rng = SplitMix64(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0xFEED_FACE);
    let (max_gross, reserve, shorting) = match kind {
        Kind::Random => (*rng.pick(&[1.5, 2.0, 3.0]), *rng.pick(&[0.0, 0.05, 0.1]), rng.chance(88)),
        _ => (3.0, 0.0, true),
    };
    let turnover = if kind == Kind::Random { *rng.pick(&[1.0, 3.0, 8.0]) } else { 1000.0 };
    let orders = if kind == Kind::Random { *rng.pick(&[4u64, 10, 50]) } else { 1000 };
    let policy = policy_with(|m: &mut Value| {
        m["capital"]["allocated"]["amount"] = json!("20000.00");
        m["universe"]["instrument_allow"] = json!(ALLOW);
        m["universe"]["shorting"] = json!(shorting);
        m["universe"]["leverage_max_gross"] = json!(max_gross);
        m["exposure"]["max_gross"] = json!(max_gross);
        m["exposure"]["max_net"] = json!(max_gross);
        m["exposure"]["max_position"] = json!(max_gross);
        m["exposure"]["max_order_notional"]["amount"] = json!("20000.00");
        m["exposure"]["max_orders_per_day"] = json!(orders);
        m["exposure"]["max_turnover_per_day"] = json!(turnover);
        m["capital"]["min_cash_reserve"] = json!(reserve);
    });
    let equity: u64 = if kind == Kind::Nonneg { 40_000 } else { rng.range(15_000, 30_000) };
    let cb = d(&equity.min(20_000).to_string());

    let mut prices = BTreeMap::new();
    for s in ALLOW.iter().chain(["QQQ"].iter()) {
        prices.insert(s.to_string(), PricePoint { price: price_for(&mut rng, s), as_of: now() });
    }
    let mut positions = Vec::new();
    let mut invested = Dec::ZERO;
    // Only the random kind holds things the sleeves do not manage (QQQ, crypto without a crypto sleeve): those count in
    // the guard's account-wide gross and can make it deny, which the permissive kinds must not have.
    let held_universe: Vec<&str> = if kind == Kind::Random { ETFS.iter().chain(CRYPTO.iter()).chain(["QQQ"].iter()).copied().collect() } else { ETFS.to_vec() };
    for s in held_universe {
        if !rng.chance(40) {
            continue;
        }
        let px = prices[s].price;
        let pct = rng.range(1, if kind == Kind::Nonneg { 10 } else { 30 });
        let value = mul(cb, d(&format!("0.{pct:02}"))).unwrap();
        let mut qty = div_floor(value, px, 6).unwrap();
        if qty.is_zero() {
            continue;
        }
        let is_crypto = CRYPTO.contains(&s);
        if kind != Kind::Nonneg && !is_crypto && rng.chance(35) {
            qty = neg(qty).unwrap();
        }
        let mv = mul(qty, px).unwrap();
        invested = add(invested, mv).unwrap();
        let (venue, class) = venue_and_class(s);
        positions.push(Position { symbol: s.to_string(), venue: venue.into(), asset_class: class.into(), quantity: qty, market_value: mv });
    }
    let equity_dec = d(&equity.to_string());
    let mut cash = sub(equity_dec, invested).unwrap();
    if cash.is_negative() {
        cash = Dec::ZERO;
    }
    let account = AccountView { account_id: format!("acct-{seed}"), ccy: "USD".into(), equity: equity_dec, cash, positions, halted: false, now: now() };

    let ls_share = if kind == Kind::Nonneg { "1" } else { *rng.pick(&["0.5", "1"]) };
    let mut weights = Vec::new();
    for s in ETFS {
        let w = if kind == Kind::Nonneg {
            pick_dec(&mut rng, &["0", "0", "0.1", "0.2"])
        } else {
            let mag = pick_dec(&mut rng, &["0", "0", "0.1", "0.2", "0.3", "0.5", "0.8"]);
            if rng.chance(45) { neg(mag).unwrap() } else { mag }
        };
        weights.push(TargetWeight { symbol: s.to_string(), weight: w });
    }
    let mut targets = vec![SleeveTarget { sleeve: "ls".into(), share: d(ls_share), venue: "alpaca".into(), asset_class: "us_etf".into(), weights }];
    if kind != Kind::Nonneg && ls_share == "0.5" && rng.chance(70) {
        targets.push(SleeveTarget {
            sleeve: "cx".into(),
            share: d("0.4"),
            venue: "kraken".into(),
            asset_class: "crypto_spot".into(),
            weights: CRYPTO.iter().map(|s| TargetWeight { symbol: s.to_string(), weight: pick_dec(&mut rng, &["0", "0.25", "0.5"]) }).collect(),
        });
    }
    let mut cfg = PlanConfig::new(
        at("2026-10-01T14:00:00Z"),
        pick_dec(&mut rng, &["1", "0.75", "0.5", "0.25"]),
        pick_dec(&mut rng, &["0", "5", "25"]),
        pick_dec(&mut rng, &["0", "0.02", "0.1"]),
        pick_dec(&mut rng, &["0", "0.001", "0.0026"]),
    )
    .with_signed_sleeve("ls", d("1"));
    cfg.credit_sell_proceeds = rng.chance(70);
    cfg.buying_power = match kind {
        Kind::Random => match rng.range(0, 9) {
            0 | 1 => None,
            2 | 3 | 4 => Some(Dec::new(i128::from(rng.range(0, 3 * cb_units(cb))), 0).unwrap()),
            _ => Some(d(HUGE_BP)),
        },
        _ => Some(d(HUGE_BP)),
    };
    Case { seed, shorting, max_gross, reserve, policy, account, prices, targets, cfg, whole: rng.chance(30), cb }
}

fn cb_units(cb: Dec) -> u64 {
    cb.to_fixed(0).unwrap().parse().unwrap()
}

struct Env {
    pairs: PairTable,
    assets: AssetTable,
    opts: alpaca::PrepareOptions,
}

fn env(whole: bool) -> Env {
    let assets = if whole {
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

fn run_with(
    env: &Env,
    targets: &[SleeveTarget],
    account: &AccountView,
    prices: &BTreeMap<String, PricePoint>,
    policy: &Policy,
    cfg: &PlanConfig,
) -> Result<OrderPlan, PlanError> {
    let k = KrakenRules { pairs: &env.pairs };
    let a = AlpacaRules { assets: &env.assets, options: &env.opts };
    let book = VenueRuleBook::new().with("kraken", &k).with("alpaca", &a).with_instrument_rules(shortable_everywhere());
    OrderPlanner::plan(targets, account, prices, &book, policy, cfg)
}

fn run(env: &Env, c: &Case) -> Result<OrderPlan, PlanError> {
    run_with(env, &c.targets, &c.account, &c.prices, &c.policy, &c.cfg)
}

// ---------------------------------------------------------------------------------------------------------------
// Independent restatement of the targets
// ---------------------------------------------------------------------------------------------------------------

fn trunc8(v: Dec) -> Dec {
    let down = |x: Dec| x.round_dp(8, Rounding::Floor).unwrap();
    if v.is_negative() {
        neg(down(abs(v).unwrap())).unwrap()
    } else {
        down(v)
    }
}

/// `capital_base * sum(share * weight) * risk_scale`, rounded toward zero at 8 dp, for one symbol.
fn expected_target(c: &Case, symbol: &str) -> Dec {
    let mut w = Dec::ZERO;
    for t in &c.targets {
        for tw in &t.weights {
            if tw.symbol == symbol {
                w = add(w, mul(t.share, tw.weight).unwrap()).unwrap();
            }
        }
    }
    trunc8(mul(mul(c.cb, w).unwrap(), c.cfg.risk_scale).unwrap())
}

fn managed(c: &Case) -> Vec<String> {
    let mut v: BTreeSet<String> = BTreeSet::new();
    for t in &c.targets {
        for w in &t.weights {
            v.insert(w.symbol.clone());
        }
    }
    v.into_iter().collect()
}

fn target_gross(c: &Case) -> Dec {
    managed(c).iter().fold(Dec::ZERO, |a, s| add(a, abs(expected_target(c, s)).unwrap()).unwrap())
}

fn held_qty(c: &Case, s: &str) -> Dec {
    c.account.position(s).map_or(Dec::ZERO, |p| p.quantity)
}

/// Does the independent recomputation say the targets need margin (a short target, or gross above equity)?
fn needs_margin(c: &Case) -> bool {
    let m = managed(c);
    let mut projected = target_gross(c);
    for p in &c.account.positions {
        if !m.contains(&p.symbol) {
            projected = add(projected, abs(mul(p.quantity, c.prices[&p.symbol].price).unwrap()).unwrap()).unwrap();
        }
    }
    m.iter().any(|s| expected_target(c, s).is_negative()) || projected > c.account.equity
}

// ---------------------------------------------------------------------------------------------------------------
// The replay oracle
// ---------------------------------------------------------------------------------------------------------------

fn oracle(c: &Case, plan: &OrderPlan) -> Result<(), String> {
    let cap_gross = mul(dec_of(c.max_gross), c.cb).unwrap();
    let cap_pos = cap_gross;
    let reserve = mul(dec_of(c.reserve), c.cb).unwrap();
    let mut qty: BTreeMap<String, Dec> = BTreeMap::new();
    let mut mv: BTreeMap<String, Dec> = BTreeMap::new();
    for p in &c.account.positions {
        qty.insert(p.symbol.clone(), p.quantity);
        mv.insert(p.symbol.clone(), mul(p.quantity, c.prices[&p.symbol].price).unwrap());
    }
    let mut bp = c.cfg.buying_power;
    let mut tags = BTreeSet::new();
    let mut closed: BTreeSet<String> = BTreeSet::new();
    for o in &plan.orders {
        let ctx = |what: &str| format!("seed {}: {what} for {o:?}", c.seed);
        if !tags.insert(o.tag.clone()) {
            return Err(ctx("duplicate tag"));
        }
        if !o.quantity.is_positive() {
            return Err(ctx("non-positive quantity"));
        }
        let px = c.prices[&o.symbol].price;
        if px != o.price || mul(o.quantity, px).unwrap() != o.notional {
            return Err(ctx("price or notional inconsistent"));
        }
        let held = qty.get(&o.symbol).copied().unwrap_or(Dec::ZERO);
        let (reducing, crosses) = match o.side {
            Side::Sell => (held.is_positive() && o.quantity <= held, held.is_positive() && o.quantity > held),
            Side::Buy => (held.is_negative() && o.quantity <= neg(held).unwrap(), held.is_negative() && o.quantity > neg(held).unwrap()),
        };
        // A single order may cross zero only as the open leg of a flip, sized to also cover the dust its close leg
        // left behind on a whole-share venue.
        if crosses && !closed.contains(&o.symbol) {
            return Err(ctx("an order crosses zero without a preceding close leg (must be two legs)"));
        }
        if reducing {
            closed.insert(o.symbol.clone());
        }
        let signed = if o.side == Side::Buy { o.notional } else { neg(o.notional).unwrap() };
        let mv_before = mv.get(&o.symbol).copied().unwrap_or(Dec::ZERO);
        let mv_after = add(mv_before, signed).unwrap();
        if !reducing {
            if o.side == Side::Sell && !c.shorting {
                return Err(ctx("a short was opened although the mandate forbids shorting"));
            }
            if abs(mv_after).unwrap() > cap_pos {
                return Err(ctx("position cap"));
            }
            let mut gross = abs(mv_after).unwrap();
            let mut net = mv_after;
            for (s, v) in &mv {
                if *s != o.symbol {
                    gross = add(gross, abs(*v).unwrap()).unwrap();
                    net = add(net, *v).unwrap();
                }
            }
            if gross > cap_gross {
                return Err(ctx("gross cap"));
            }
            if abs(net).unwrap() > cap_gross {
                return Err(ctx("net cap"));
            }
            if let Some(b) = bp {
                let left = sub(sub(b, o.notional).unwrap(), o.est_fee).unwrap();
                if left < reserve {
                    return Err(ctx("buying power (less reserve) exceeded"));
                }
                bp = Some(left);
            }
        }
        qty.insert(o.symbol.clone(), add(held, if o.side == Side::Buy { o.quantity } else { neg(o.quantity).unwrap() }).unwrap());
        mv.insert(o.symbol.clone(), mv_after);
    }
    // No plan ever ends with a position that no sleeve and no held position could explain: an unmanaged symbol is untouched.
    let m = managed(c);
    for o in &plan.orders {
        if !m.contains(&o.symbol) {
            return Err(format!("seed {}: traded an unmanaged symbol {}", c.seed, o.symbol));
        }
    }
    // Direction and interval: per managed symbol, all orders share one side, that side is the one toward the target,
    // and the final value lies between the starting value and the target (never past it).
    for s in &m {
        let px = c.prices[s].price;
        let start = mul(held_qty(c, s), px).unwrap();
        let target = expected_target(c, s);
        let fin = mul(qty.get(s).copied().unwrap_or(Dec::ZERO), px).unwrap();
        let sides: BTreeSet<&str> = plan.orders.iter().filter(|o| o.symbol == *s).map(|o| o.side.as_str()).collect();
        if sides.len() > 1 {
            return Err(format!("seed {}: {s} traded both ways: {sides:?}", c.seed));
        }
        if let Some(side) = sides.iter().next() {
            let toward_up = target > start;
            if (*side == Side::Buy.as_str()) != toward_up {
                return Err(format!("seed {}: {s} traded {side:?} but start {start} -> target {target}", c.seed));
            }
        }
        let (lo, hi) = if start <= target { (start, target) } else { (target, start) };
        if fin < lo || fin > hi {
            return Err(format!("seed {}: {s} ended at {fin}, outside [{lo}, {hi}] (start {start}, target {target})", c.seed));
        }
    }
    // Long-only sleeve instruments (crypto here): never below zero, never sold beyond the holding.
    let long_only: BTreeSet<&str> = c.targets.iter().filter(|t| t.sleeve == "cx").flat_map(|t| t.weights.iter().map(|w| w.symbol.as_str())).collect();
    for s in long_only {
        let end = qty.get(s).copied().unwrap_or(Dec::ZERO);
        if end.is_negative() {
            return Err(format!("seed {}: long-only {s} ended short at {end}", c.seed));
        }
        let sold = plan.orders.iter().filter(|o| o.symbol == s && o.side == Side::Sell).fold(Dec::ZERO, |a, o| add(a, o.quantity).unwrap());
        if sold > std::cmp::max(held_qty(c, s), Dec::ZERO) {
            return Err(format!("seed {}: long-only {s} sold {sold} but held {}", c.seed, held_qty(c, s)));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------------------------------------------

#[derive(Default)]
struct Coverage {
    ok: u32,
    with_shorts: u32,
    with_flips: u32,
    with_cover: u32,
    with_denied: u32,
    gross_refused: u32,
    bp_refused: u32,
    bp_tight_cut: u32,
    no_short_mandate_denials: u32,
    levered: u32,
}

#[test]
fn signed_plans_respect_every_limit_and_never_trade_the_wrong_way() {
    let mut cov = Coverage::default();
    for seed in 0..CASES {
        let c = generate(seed, Kind::Random);
        let e = env(c.whole);
        let result = run(&e, &c);
        let cap = mul(dec_of(c.max_gross), c.cb).unwrap();
        let gross = target_gross(&c);
        // Gross cap: refused exactly when the targets' gross is above it.
        if gross > cap {
            match result {
                Err(PlanError::GrossAboveCap { gross: g, cap: cp }) => {
                    assert_eq!((g, cp), (gross, cap), "seed {seed}");
                    cov.gross_refused += 1;
                }
                other => panic!("seed {seed}: gross {gross} above cap {cap} must be refused, got {other:?}"),
            }
            continue;
        }
        // Buying power: required exactly when the targets need margin.
        if c.cfg.buying_power.is_none() {
            if needs_margin(&c) {
                match result {
                    Err(PlanError::BuyingPowerRequired { .. }) => {
                        cov.bp_refused += 1;
                        continue;
                    }
                    other => panic!("seed {seed}: a margin plan without buying power must be refused, got {other:?}"),
                }
            }
        }
        let plan = result.unwrap_or_else(|e| panic!("seed {seed}: unexpected refusal {e:?}"));
        if let Err(msg) = oracle(&c, &plan) {
            panic!("{msg}\nplan: {plan:#?}");
        }
        for dn in &plan.denied {
            assert!(!plan.orders.iter().any(|o| o.tag == dn.order.tag), "seed {seed}: denied order is also planned");
            assert!(!dn.reasons.is_empty());
        }
        // Reductions come before increases: once an increase (sell into a short or buy into a long) appears, no
        // reduction follows. Reductions here are the orders flagged reducing by the replay (held-based).
        let mut held: BTreeMap<String, Dec> = c.account.positions.iter().map(|p| (p.symbol.clone(), p.quantity)).collect();
        let mut seen_increase = false;
        for o in &plan.orders {
            let h = held.get(&o.symbol).copied().unwrap_or(Dec::ZERO);
            // A pure reduction: toward zero and not past it (the open leg of a flip that also covers dust is not one).
            let reducing = (o.side == Side::Sell && h.is_positive() && o.quantity <= h)
                || (o.side == Side::Buy && h.is_negative() && o.quantity <= neg(h).unwrap());
            assert!(!(reducing && seen_increase), "seed {seed}: a reduction after an increase: {:?}", plan.orders);
            seen_increase |= !reducing;
            let dq = if o.side == Side::Buy { o.quantity } else { neg(o.quantity).unwrap() };
            held.insert(o.symbol.clone(), add(h, dq).unwrap());
        }
        // Margin bookkeeping agrees with the orders: every flagged tag is a real order; a plan without margin need and
        // without a short has none flagged unless it borrowed.
        for t in &plan.margin.margin_order_tags {
            assert!(plan.orders.iter().any(|o| &o.tag == t), "seed {seed}: flagged tag is not an order");
        }
        if let (Some(bp), Some(left)) = (plan.margin.buying_power, plan.margin.buying_power_left) {
            assert!(left <= bp && !left.is_negative(), "seed {seed}: buying power left {left} of {bp}");
        }
        cov.ok += 1;
        let start_short = |s: &str| held_qty(&c, s).is_negative();
        cov.with_shorts += u32::from(plan.orders.iter().any(|o| o.side == Side::Sell && !held_qty(&c, &o.symbol).is_positive()));
        cov.with_flips += u32::from(ETFS.iter().any(|s| {
            let n = plan.orders.iter().filter(|o| o.symbol == *s).count();
            n == 2 && !held_qty(&c, s).is_zero()
        }));
        cov.with_cover += u32::from(plan.orders.iter().any(|o| o.side == Side::Buy && start_short(&o.symbol)));
        cov.with_denied += u32::from(!plan.denied.is_empty());
        cov.no_short_mandate_denials += u32::from(!c.shorting && plan.denied.iter().any(|d| d.reasons.iter().any(|r| r.code.as_str() == "SHORTING_FORBIDDEN")));
        cov.levered += u32::from(plan.margin.projected_gross > c.account.equity);
        if c.cfg.buying_power.is_some_and(|b| b < d(HUGE_BP)) && plan.orders.len() + plan.denied.len() < plan.lines.iter().filter(|l| l.target_notional != l.current_notional).count() {
            cov.bp_tight_cut += 1;
        }
    }
    // The generator must actually exercise the signed paths, or the properties above prove little.
    assert!(cov.ok > 250, "too few plans: {}", cov.ok);
    assert!(cov.with_shorts > 60, "too few plans that open or deepen shorts: {}", cov.with_shorts);
    assert!(cov.with_flips > 15, "too few flips through zero: {}", cov.with_flips);
    assert!(cov.with_cover > 30, "too few short covers: {}", cov.with_cover);
    assert!(cov.with_denied > 20, "too few plans with denials: {}", cov.with_denied);
    assert!(cov.gross_refused > 10, "too few gross-cap refusals: {}", cov.gross_refused);
    assert!(cov.bp_refused > 5, "too few missing-buying-power refusals: {}", cov.bp_refused);
    assert!(cov.no_short_mandate_denials > 3, "too few shorting-forbidden denials: {}", cov.no_short_mandate_denials);
    assert!(cov.levered > 30, "too few levered plans: {}", cov.levered);
    let _ = cov.bp_tight_cut;
}

fn apply_plan(c: &Case, plan: &OrderPlan) -> AccountView {
    let mut a = c.account.clone();
    for o in &plan.orders {
        let signed_qty = if o.side == Side::Buy { o.quantity } else { neg(o.quantity).unwrap() };
        let signed_val = if o.side == Side::Buy { o.notional } else { neg(o.notional).unwrap() };
        match a.positions.iter_mut().find(|p| p.symbol == o.symbol) {
            Some(p) => {
                p.quantity = add(p.quantity, signed_qty).unwrap();
                p.market_value = mul(p.quantity, o.price).unwrap();
            }
            None => a.positions.push(Position {
                symbol: o.symbol.clone(),
                venue: o.venue.clone(),
                asset_class: o.asset_class.clone(),
                quantity: signed_qty,
                market_value: signed_val,
            }),
        }
        a.cash = sub(sub(a.cash, signed_val).unwrap(), o.est_fee).unwrap();
    }
    a
}

#[test]
fn replanning_from_the_post_plan_holdings_trades_nothing_more() {
    let mut traded = 0;
    let mut flips = 0;
    for seed in 0..CASES {
        let c = generate(seed, Kind::Permissive);
        let e = env(c.whole);
        let Ok(plan) = run(&e, &c) else { continue };
        assert!(plan.denied.is_empty(), "seed {seed}: a permissive mandate must not deny: {:#?}", plan.denied);
        let after = apply_plan(&c, &plan);
        let mut cfg2 = c.cfg.clone();
        cfg2.buying_power = plan.margin.buying_power_left;
        let again = run_with(&e, &c.targets, &after, &c.prices, &c.policy, &cfg2).unwrap_or_else(|er| panic!("seed {seed}: re-plan refused: {er:?}"));
        assert!(again.orders.is_empty() && again.denied.is_empty(), "seed {seed}: re-planning traded again:\nfirst: {:#?}\nsecond: {:#?}", plan.orders, again);
        traded += u32::from(!plan.orders.is_empty());
        flips += u32::from(ETFS.iter().any(|s| plan.orders.iter().filter(|o| o.symbol == *s).count() == 2));
    }
    assert!(traded > 250, "too few cases that traded: {traded}");
    assert!(flips > 10, "too few flips exercised: {flips}");
}

#[test]
fn opting_a_sleeve_in_with_nonnegative_weights_and_ample_funding_changes_no_order() {
    let mut compared = 0;
    for seed in 0..CASES {
        let c = generate(seed, Kind::Nonneg);
        let e = env(c.whole);
        let signed = run(&e, &c).unwrap_or_else(|er| panic!("seed {seed}: {er:?}"));
        // The very same sleeve, long-only: today's rules, today's cash budget (cash is ample by construction).
        let mut cfg = c.cfg.clone();
        cfg.signed_sleeves.clear();
        cfg.buying_power = None;
        let plain = run_with(&e, &c.targets, &c.account, &c.prices, &c.policy, &cfg);
        let Ok(plain) = plain else {
            // Weights summing above 1 are refused as long-only; nothing to compare.
            assert!(matches!(plain, Err(PlanError::WeightsExceedOne(_))), "seed {seed}: {plain:?}");
            continue;
        };
        assert_eq!(signed.orders, plain.orders, "seed {seed}");
        assert_eq!(signed.denied, plain.denied, "seed {seed}");
        assert_eq!(signed.skipped, plain.skipped, "seed {seed}");
        assert_eq!(signed.lines, plain.lines, "seed {seed}");
        assert!(signed.margin.margin_order_tags.is_empty(), "seed {seed}: a non-negative, unlevered plan used margin");
        compared += 1;
    }
    assert!(compared > 150, "too few comparable cases: {compared}");
}

fn shuffle<T>(rng: &mut SplitMix64, items: &mut [T]) {
    for i in (1..items.len()).rev() {
        let j = rng.range(0, i as u64) as usize;
        items.swap(i, j);
    }
}

#[test]
fn signed_plans_are_deterministic_and_stable_under_input_ordering() {
    for seed in 0..CASES {
        let c = generate(seed, Kind::Random);
        let e = env(c.whole);
        let a = run(&e, &c);
        let b = run(&e, &c);
        assert_eq!(a, b, "seed {seed}");
        let mut rng = SplitMix64(seed ^ 0xBADC_0FFE);
        let mut targets = c.targets.clone();
        shuffle(&mut rng, &mut targets);
        for t in &mut targets {
            shuffle(&mut rng, &mut t.weights);
        }
        let mut account = c.account.clone();
        shuffle(&mut rng, &mut account.positions);
        let shuffled = run_with(&e, &targets, &account, &c.prices, &c.policy, &c.cfg);
        assert_eq!(a, shuffled, "seed {seed}: reordering the inputs changed the plan");
    }
}

#[test]
fn scaling_risk_scale_down_shrinks_every_signed_target_toward_zero() {
    let scales = ["1", "0.75", "0.5", "0.25", "0.1"];
    for seed in 0..CASES {
        let mut c = generate(seed, Kind::Permissive);
        let e = env(false);
        let mut last: Option<BTreeMap<String, Dec>> = None;
        let mut last_gross: Option<Dec> = None;
        for s in scales {
            c.cfg.risk_scale = d(s);
            let Ok(plan) = run(&e, &c) else { break };
            let now: BTreeMap<String, Dec> = plan.lines.iter().map(|l| (l.symbol.clone(), l.target_notional)).collect();
            for (sym, t) in &now {
                // Independent value, and the sign never flips as the scale falls.
                assert_eq!(*t, expected_target(&c, sym), "seed {seed} scale {s} {sym}");
                if let Some(prev) = last.as_ref().and_then(|m| m.get(sym)) {
                    assert!(abs(*t).unwrap() <= abs(*prev).unwrap(), "seed {seed}: |target| of {sym} rose as the scale fell");
                    assert!(t.is_zero() || prev.is_zero() || t.is_negative() == prev.is_negative(), "seed {seed}: {sym} changed sign");
                }
            }
            if let Some(g) = last_gross {
                assert!(plan.margin.target_gross <= g, "seed {seed}: target gross rose from {g} to {} at scale {s}", plan.margin.target_gross);
            }
            last_gross = Some(plan.margin.target_gross);
            last = Some(now);
        }
    }
}
