//! PARITY TEST 1 (design 5.4 #1): target parity, the trade filter and the gross cap.
//!
//! Random books (1-4 sleeves, long-only and signed, caps, risk scales, held positions, unmanaged positions) go through
//! `portfolio_construct::construct` (f64, the backtester's specification) and through the real
//! `OrderPlanner::plan` (exact decimals, with a fake account). Asserted, per book:
//!
//! * `lines[].target_notional` equal within 1e-8 (one Decimal quantum of the planner's 8-decimal rounding);
//! * the trade filter's skip set identical (which instruments, and abs-versus-pct), except instruments whose delta is
//!   within 1e-9 (relative) of a filter threshold or of zero;
//! * the plan-level refusals identical (gross cap for signed plans, buying power required), except books within 1e-9
//!   (relative) of the cap / of the equity boundary.
//!
//! The random books use a seeded SplitMix64 (no OS randomness). Boundary cases are enumerated explicitly below, and the
//! places where the two are DOCUMENTED to differ are pinned as tests of their own (the ledger's
//! `t1_*_pinned` entries): the planner's per-order denial versus the whole-book refusal of council R1/R2, the
//! one-quantum band around a limit, and the zero risk scale.

mod common;

use std::collections::BTreeMap;

use broker_adapters::{Dec, Side};
use common::replay::{at, envelope, BASELINE};
use common::*;
use mandate_core::mandate::MandateBody;
use portfolio_construct as pc;
use rebalancer_core::dec_math::mul;
use rebalancer_core::guard::{AccountView, DenialCode, PricePoint, Position};
use rebalancer_core::planner::{OrderPlan, OrderPlanner, PlanConfig, PlanError, SkipReason, SleeveTarget, TargetWeight};
use rebalancer_core::policy::Policy;
use rebalancer_core::venue::{SizeRefusal, VenueRuleBook, VenueRules};
use serde_json::{json, Value};

/// The instrument pool every book draws from: (symbol, asset class). One venue, `alpaca`, for all of them.
const POOL: [(&str, &str); 6] =
    [("SPY", "us_etf"), ("EFA", "us_etf"), ("IEF", "us_etf"), ("VNQ", "us_etf"), ("BTC/USD", "crypto_spot"), ("ETH/USD", "crypto_spot")];

/// Quantities are rounded DOWN to 8 decimals (like every real venue table: crypto 8, whole shares 0) and otherwise pass
/// through; venue rounding is not what this test is about (test 4 compares the lot tables). NOTE the scale: a rule that
/// returned the planner's 18-decimal wish unchanged would overflow the planner's exact `div_floor` in the cash-scaling
/// step (`Math(Overflow)`, a fail-closed plan error): see the ledger's note on `DEC_SCALE_18_QUANTITY`.
struct PlainRules;

impl VenueRules for PlainRules {
    fn round_quantity(&self, _symbol: &str, _side: Side, quantity: Dec, _price: Dec) -> Result<Dec, SizeRefusal> {
        let q = quantity.round_dp(8, broker_adapters::decimal::Rounding::Floor).map_err(|_| SizeRefusal::Other("overflow".into()))?;
        if q.is_zero() || q.is_negative() {
            return Err(SizeRefusal::RoundsToZero);
        }
        Ok(q)
    }
    fn fingerprint(&self, symbol: &str) -> String {
        format!("plain8:{symbol}")
    }
}

#[derive(Clone, Debug)]
struct InstCase {
    /// Price, scale 4.
    price: i128,
    /// Signed held units, scale 4.
    held: i128,
}

#[derive(Clone, Debug)]
struct SleeveCase {
    id: String,
    /// Share of capital, scale 4.
    share: i128,
    /// `Some(max_abs_weight)` (scale 2) for a signed sleeve.
    signed_max: Option<i128>,
    /// `(instrument index into POOL, weight scale 4)`.
    weights: Vec<(usize, i128)>,
}

#[derive(Clone, Debug)]
struct Case {
    /// Cents.
    equity: i128,
    cash: i128,
    allocated: i128,
    insts: Vec<InstCase>,
    sleeves: Vec<SleeveCase>,
    /// The risk scale is `risk_a * risk_b` (scale 4 each): approval constant times ladder.
    risk_a: i128,
    risk_b: i128,
    /// Mandate `max_gross` (= leverage cap = net cap = position cap = class caps), scale 2.
    max_gross: i128,
    /// Trade filter: absolute minimum in cents, percentage minimum scale 4.
    min_abs: i128,
    min_pct: i128,
    /// Buying power in cents, for signed plans.
    buying_power: Option<i128>,
}

impl Case {
    fn signed_plan(&self) -> bool {
        self.sleeves.iter().any(|s| s.signed_max.is_some())
    }
    fn named(&self) -> Vec<bool> {
        let mut n = vec![false; POOL.len()];
        for s in &self.sleeves {
            for (i, _) in &s.weights {
                n[*i] = true;
            }
        }
        n
    }
}

fn money(cents: i128) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

fn mandate_for(c: &Case) -> MandateBody {
    let mg = f64_of(c.max_gross, 2);
    let mut v: Value = serde_json::from_str(BASELINE).expect("baseline parses");
    v["universe"]["venues"] = json!(["alpaca"]);
    v["universe"]["asset_classes"] = json!(["us_etf", "crypto_spot"]);
    v["universe"]["instrument_allow"] = json!(POOL.iter().map(|p| p.0).collect::<Vec<_>>());
    v["universe"]["shorting"] = json!(c.signed_plan());
    v["universe"]["leverage_max_gross"] = json!(mg);
    v["capital"]["allocated"]["amount"] = json!(money(c.allocated));
    v["exposure"]["max_gross"] = json!(mg);
    v["exposure"]["max_net"] = json!(mg);
    v["exposure"]["max_position"] = json!(mg);
    v["exposure"]["max_asset_class"] = json!({"us_etf": mg, "crypto_spot": mg});
    v["exposure"]["max_order_notional"]["amount"] = json!(money(c.allocated));
    serde_json::from_value(v).expect("mandate parses")
}

fn risk_dec(c: &Case) -> Dec {
    dec(c.risk_a * c.risk_b, 8)
}

/// The planner side: the real `OrderPlanner::plan` with a fake account.
fn plan_of(c: &Case) -> Result<OrderPlan, PlanError> {
    plan_of_with(c, &mandate_for(c))
}

fn plan_of_with(c: &Case, mandate: &MandateBody) -> Result<OrderPlan, PlanError> {
    plan_with_rules(c, mandate, &PlainRules)
}

fn plan_with_rules(c: &Case, mandate: &MandateBody, rules: &dyn VenueRules) -> Result<OrderPlan, PlanError> {
    let policy = Policy::compile(mandate).with_envelope(envelope());
    let now = at("2026-01-01T12:00:00Z");
    let mut positions = Vec::new();
    let mut prices = BTreeMap::new();
    for (i, (sym, class)) in POOL.iter().enumerate() {
        let px = dec(c.insts[i].price, 4);
        prices.insert((*sym).to_string(), PricePoint { price: px, as_of: now });
        if c.insts[i].held != 0 {
            let q = dec(c.insts[i].held, 4);
            positions.push(Position { symbol: (*sym).into(), venue: "alpaca".into(), asset_class: (*class).into(), quantity: q, market_value: mul(q, px).expect("no overflow") });
        }
    }
    let account = AccountView { account_id: "a".into(), ccy: "USD".into(), equity: dec(c.equity, 2), cash: dec(c.cash, 2), positions, halted: false, now };
    let targets: Vec<SleeveTarget> = c
        .sleeves
        .iter()
        .map(|s| SleeveTarget {
            sleeve: s.id.clone(),
            share: dec(s.share, 4),
            venue: "alpaca".into(),
            asset_class: "mixed".into(),
            weights: s.weights.iter().map(|(i, w)| TargetWeight { symbol: POOL[*i].0.into(), weight: dec(*w, 4) }).collect(),
        })
        .collect();
    // asset class is per instrument, but `SleeveTarget` carries one class per sleeve: give each sleeve the class of its
    // first instrument and keep instruments of a sleeve within one class by construction (see `gen_case`)
    let targets: Vec<SleeveTarget> = targets
        .into_iter()
        .zip(&c.sleeves)
        .map(|(mut t, s)| {
            t.asset_class = POOL[s.weights[0].0].1.to_string();
            t
        })
        .collect();
    let mut cfg = PlanConfig::new(now, risk_dec(c), dec(c.min_abs, 2), dec(c.min_pct, 4), d("0.0026"));
    for s in &c.sleeves {
        if let Some(m) = s.signed_max {
            cfg = cfg.with_signed_sleeve(&s.id, dec(m, 2));
        }
    }
    cfg.buying_power = c.buying_power.map(|b| dec(b, 2));
    let book = VenueRuleBook::new().with("alpaca", rules);
    OrderPlanner::plan(&targets, &account, &prices, &book, &policy, &cfg)
}

/// The specification side: `portfolio_construct::construct`, configured `PlannerFaithful` (today's plan-level checks).
fn construct_with(c: &Case, policy: pc::LimitPolicy, cap: Option<f64>) -> Result<pc::ConstructOutput, pc::ConstructRefusal> {
    construct_with_unmanaged(c, policy, cap, None)
}

/// `unmanaged_override`: replace the glue's `unmanaged_gross` (to show what the wrong caller contract does).
fn construct_with_unmanaged(c: &Case, policy: pc::LimitPolicy, cap: Option<f64>, unmanaged_override: Option<f64>) -> Result<pc::ConstructOutput, pc::ConstructRefusal> {
    let sleeves: Vec<pc::SleeveTargets> = c
        .sleeves
        .iter()
        .map(|s| {
            let w: Vec<(usize, f64)> = s.weights.iter().map(|(i, w)| (*i, f64_of(*w, 4))).collect();
            match s.signed_max {
                Some(m) => pc::SleeveTargets::signed(&s.id, f64_of(s.share, 4), f64_of(m, 2), w),
                None => pc::SleeveTargets::long_only(&s.id, f64_of(s.share, 4), w),
            }
        })
        .collect();
    let named = c.named();
    let mut signed_named = vec![false; POOL.len()];
    for s in c.sleeves.iter().filter(|s| s.signed_max.is_some()) {
        for (i, _) in &s.weights {
            signed_named[*i] = true;
        }
    }
    let mut unmanaged = 0.0;
    let facts: Vec<pc::InstrumentFacts> = POOL
        .iter()
        .enumerate()
        .map(|(i, (sym, class))| {
            let (px, held) = (f64_of(c.insts[i].price, 4), f64_of(c.insts[i].held, 4));
            // The planner counts as UNMANAGED every position it has no line for: an instrument no sleeve names, and also
            // one a LONG-ONLY sleeve names while a short is held in it (`ShortPositionHeld`: skipped, never touched, but
            // still exposure). `unmanaged_gross` is the caller's contract, so the glue must do the same.
            if !named[i] || (held < 0.0 && !signed_named[i]) {
                unmanaged += (held * px).abs();
            }
            pc::InstrumentFacts::new(sym, "alpaca", class, px).with_held(held)
        })
        .collect();
    let mg = f64_of(c.max_gross, 2);
    let mut limits = pc::Limits::unlimited().with_max_gross(cap.unwrap_or(mg)).with_policy(policy);
    if policy == pc::LimitPolicy::RefuseWholeBook {
        limits = limits
            .with_max_net(mg)
            .with_max_position(mg)
            .with_class_cap("us_etf", mg)
            .with_class_cap("crypto_spot", mg)
            .with_shorting(c.signed_plan());
    }
    let funding = match c.buying_power {
        Some(b) => pc::Funding::BuyingPower { buying_power: f64_of(b, 2), reserve_fraction: 0.05, fee_rate: 0.0026 },
        None => pc::Funding::Cash { cash: f64_of(c.cash, 2), reserve_fraction: 0.05, fee_rate: 0.0026, credit_sell_proceeds: true },
    };
    pc::construct(&pc::ConstructInputs {
        equity: f64_of(c.equity, 2),
        allocated_capital: Some(f64_of(c.allocated, 2)),
        sleeves: &sleeves,
        risk_scale: pc::RiskScale::new(f64_of(c.risk_a, 4), f64_of(c.risk_b, 4)),
        limits: &limits,
        instruments: &facts,
        margin: &pc::NoMargin,
        trade_filter: pc::TradeFilter::new(f64_of(c.min_abs, 2), f64_of(c.min_pct, 4)),
        rounding: None,
        funding,
        target_dp: Some(pc::TARGET_DP),
        unmanaged_gross: unmanaged_override.unwrap_or(unmanaged),
    })
}

fn construct_of(c: &Case) -> Result<pc::ConstructOutput, pc::ConstructRefusal> {
    construct_with(c, pc::LimitPolicy::PlannerFaithful, None)
}

// -------------------------------------------------------------------------------------------------------------
// Comparison
// -------------------------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    GrossAboveCap,
    BuyingPowerRequired,
}

fn outcome_of_plan(r: &Result<OrderPlan, PlanError>) -> Result<Outcome, String> {
    match r {
        Ok(_) => Ok(Outcome::Ok),
        Err(PlanError::GrossAboveCap { .. }) => Ok(Outcome::GrossAboveCap),
        Err(PlanError::BuyingPowerRequired { .. }) => Ok(Outcome::BuyingPowerRequired),
        Err(e) => Err(format!("planner returned an unexpected error {e:?}")),
    }
}

fn outcome_of_construct(r: &Result<pc::ConstructOutput, pc::ConstructRefusal>) -> Result<Outcome, String> {
    match r {
        Ok(_) => Ok(Outcome::Ok),
        Err(pc::ConstructRefusal::GrossAboveCap { .. }) => Ok(Outcome::GrossAboveCap),
        Err(pc::ConstructRefusal::BuyingPowerRequired { .. }) => Ok(Outcome::BuyingPowerRequired),
        Err(e) => Err(format!("construct returned an unexpected refusal {e:?}")),
    }
}

/// Numbers of the plan-level boundaries in f64: `(gross, cap, projected gross, equity)`.
fn boundary_numbers(c: &Case) -> (f64, f64, f64, f64) {
    let unconstrained = Case { buying_power: Some(c.equity * 1000), ..c.clone() };
    let out = construct_with(&unconstrained, pc::LimitPolicy::PlannerFaithful, Some(f64::INFINITY)).expect("unlimited construct");
    (out.gross, f64_of(c.max_gross, 2) * out.capital_base, out.projected_gross, f64_of(c.equity, 2))
}

#[derive(Default, Debug)]
struct Stats {
    cases: usize,
    both_ok_signed: usize,
    both_ok_long_only: usize,
    both_gross_refuse: usize,
    both_bp_required: usize,
    boundary_skipped_outcome: usize,
    lines_compared: usize,
    max_target_diff: f64,
    filter_abs_compared: usize,
    filter_pct_compared: usize,
    filter_instruments_boundary_skipped: usize,
    instruments_traded_compared: usize,
}

/// One Decimal quantum (1e-8) plus f64 noise: the two sides may land on ADJACENT quanta when the exact decimal target is
/// within a few ulps of a quantum boundary (the f64 side snaps values at most 4 ulps below a boundary onto it, as an exact
/// decimal tie would be; the planner floors the exact value). The slack is 1e-14 relative, i.e. tens of ulps.
fn quantum_tol(target: f64) -> f64 {
    1.0e-8 + 1.0e-14 * target.abs()
}

/// Compare one book. `Err(message)` is a real disagreement.
fn compare(c: &Case, st: &mut Stats) -> Result<(), String> {
    st.cases += 1;
    let p = plan_of(c);
    let k = construct_of(c);
    let (op, ok_) = (outcome_of_plan(&p)?, outcome_of_construct(&k)?);
    let (gross, cap, projected, equity) = boundary_numbers(c);
    let signed = c.signed_plan();
    let near = |a: f64, b: f64| (a - b).abs() <= 1e-9 * b.abs().max(1.0);
    let boundary_outcome = signed && (near(gross, cap) || near(projected, equity));
    if op != ok_ {
        if boundary_outcome {
            st.boundary_skipped_outcome += 1;
            return Ok(());
        }
        return Err(format!("plan-level outcome differs: planner {op:?} vs construct {ok_:?} (gross {gross}, cap {cap}, projected {projected}, equity {equity})"));
    }
    match op {
        Outcome::GrossAboveCap => st.both_gross_refuse += 1,
        Outcome::BuyingPowerRequired => st.both_bp_required += 1,
        Outcome::Ok => {}
    }
    let (Ok(plan), Ok(out)) = (p, k) else { return Ok(()) };
    if signed {
        st.both_ok_signed += 1;
    } else {
        st.both_ok_long_only += 1;
    }
    // ---- capital base and lines
    let cb_diff = (plan.capital_base.to_f64() - out.capital_base).abs();
    if cb_diff > quantum_tol(out.capital_base) {
        return Err(format!("capital base differs by {cb_diff}"));
    }
    let planner_lines: BTreeMap<&str, f64> = plan.lines.iter().map(|l| (l.symbol.as_str(), l.target_notional.to_f64())).collect();
    let construct_lines: BTreeMap<&str, f64> = out.lines.iter().map(|l| (POOL[l.instrument].0, l.target_notional)).collect();
    if planner_lines.keys().ne(construct_lines.keys()) {
        return Err(format!("the managed instruments differ: planner {:?} vs construct {:?}", planner_lines.keys().collect::<Vec<_>>(), construct_lines.keys().collect::<Vec<_>>()));
    }
    for (sym, t) in &planner_lines {
        let diff = (t - construct_lines[sym]).abs();
        if diff > quantum_tol(*t) {
            return Err(format!("target of {sym} differs: planner {t} vs construct {} (diff {diff})", construct_lines[sym]));
        }
        st.max_target_diff = st.max_target_diff.max(diff);
        st.lines_compared += 1;
    }
    // ---- the trade filter's skip set
    let mut planner_skips: BTreeMap<String, char> = BTreeMap::new();
    for s in &plan.skipped {
        match s.reason {
            SkipReason::BelowMinTradeAbs { .. } => {
                planner_skips.insert(s.symbol.clone(), 'A');
            }
            SkipReason::BelowMinTradePct { .. } => {
                planner_skips.insert(s.symbol.clone(), 'P');
            }
            _ => {}
        }
    }
    let mut construct_skips: BTreeMap<String, char> = BTreeMap::new();
    for s in &out.skipped {
        match s.reason {
            pc::SkipReason::BelowMinAbs { .. } => {
                construct_skips.insert(s.symbol.clone(), 'A');
            }
            pc::SkipReason::BelowMinPct { .. } => {
                construct_skips.insert(s.symbol.clone(), 'P');
            }
            _ => {}
        }
    }
    let (min_abs, min_pct) = (f64_of(c.min_abs, 2), f64_of(c.min_pct, 4));
    for l in &out.lines {
        let sym = POOL[l.instrument].0.to_string();
        let delta = (l.target_notional - l.current_notional).abs();
        let reference = if l.target_notional == 0.0 { l.current_notional.abs() } else { l.target_notional.abs() };
        let scale = l.target_notional.abs().max(l.current_notional.abs()).max(1.0);
        let boundary = delta <= 1e-9 * scale
            || (delta - min_abs).abs() <= 1e-9 * min_abs.max(1.0)
            || (delta - min_pct * reference).abs() <= 1e-9 * (min_pct * reference).max(1.0);
        if boundary {
            st.filter_instruments_boundary_skipped += 1;
            continue;
        }
        let (a, b) = (planner_skips.get(&sym), construct_skips.get(&sym));
        if a != b {
            return Err(format!("the trade filter differs for {sym}: planner {a:?} vs construct {b:?} (delta {delta}, min_abs {min_abs}, min_pct*ref {})", min_pct * reference));
        }
        match a {
            Some('A') => st.filter_abs_compared += 1,
            Some('P') => st.filter_pct_compared += 1,
            _ => st.instruments_traded_compared += 1,
        }
    }
    Ok(())
}

// -------------------------------------------------------------------------------------------------------------
// Generator
// -------------------------------------------------------------------------------------------------------------

/// Split `total` into `n` positive parts summing to at most `total`.
fn split(r: &mut SplitMix64, total: i64, n: usize) -> Vec<i128> {
    let w: Vec<i64> = (0..n).map(|_| ri(r, 1, 100)).collect();
    let s: i64 = w.iter().sum();
    w.iter().map(|x| i128::from((total * x / s).max(1))).collect()
}

fn gen_case(r: &mut SplitMix64) -> Case {
    let equity = i128::from(ri(r, 100_000, 100_000_000));
    let allocated = if r.chance(50) { equity * 10 } else { equity * i128::from(ri(r, 30, 150)) / 100 };
    let cash = equity * i128::from(ri(r, 0, 100)) / 100;
    let insts: Vec<InstCase> = POOL
        .iter()
        .map(|_| {
            let price = i128::from(ri(r, 10_000, 10_000_000));
            let held = if r.chance(45) {
                0
            } else {
                let f = i128::from(ri(r, 1, 50));
                let units = equity * f * 10_000 / price;
                if r.chance(5) {
                    -units
                } else {
                    units
                }
            };
            InstCase { price, held }
        })
        .collect();
    let n_sleeves = ri(r, 1, 4) as usize;
    let total_share = ri(r, 3000, 10_000);
    let shares = split(r, total_share, n_sleeves);
    let mut sleeves = Vec::new();
    for (k, share) in shares.into_iter().enumerate() {
        let signed = r.chance(40);
        // a sleeve's instruments come from ONE asset class (the planner's `SleeveTarget` carries one class)
        let class_pool: Vec<usize> = if r.chance(50) { vec![0, 1, 2, 3] } else { vec![4, 5] };
        let n_inst = ri(r, 1, class_pool.len().min(4) as i64) as usize;
        let mut chosen: Vec<usize> = Vec::new();
        while chosen.len() < n_inst {
            let i = class_pool[ri(r, 0, class_pool.len() as i64 - 1) as usize];
            if !chosen.contains(&i) {
                chosen.push(i);
            }
        }
        let (signed_max, weights) = if signed {
            let m = [50i64, 100, 150, 200, 300][ri(r, 0, 4) as usize];
            let ws = chosen.iter().map(|i| (*i, i128::from(ri(r, -m * 100, m * 100)))).collect();
            (Some(i128::from(m)), ws)
        } else {
            let t = if r.chance(15) { 10_000 } else { ri(r, 1000, 10_000) };
            let mut ws: Vec<(usize, i128)> = chosen.iter().copied().zip(split(r, t, chosen.len())).collect();
            if r.chance(10) {
                ws[0].1 = 0;
            }
            (None, ws)
        };
        sleeves.push(SleeveCase { id: format!("s{k}"), share, signed_max, weights });
    }
    let signed_any = sleeves.iter().any(|s| s.signed_max.is_some());
    let mut case = Case {
        equity,
        cash,
        allocated,
        insts,
        sleeves,
        risk_a: i128::from(ri(r, 1000, 10_000)),
        risk_b: if r.chance(50) { 10_000 } else { i128::from(ri(r, 2500, 10_000)) },
        max_gross: [100i128, 125, 150, 200, 300][ri(r, 0, 4) as usize],
        min_abs: if r.chance(70) { 1000 } else { i128::from(ri(r, 0, 50_000)) },
        min_pct: if r.chance(70) { 200 } else { i128::from(ri(r, 0, 1000)) },
        buying_power: if signed_any && r.chance(50) { Some(equity * 20) } else { None },
    };
    // Half of the books hold positions NEAR their targets (within +-4%), so that the trade filter's absolute floor and
    // percentage band both decide many instruments (with random holdings almost every delta is far above both).
    if r.chance(50) {
        let probe = Case { buying_power: Some(case.equity * 1000), ..case.clone() };
        if let Ok(out) = construct_with(&probe, pc::LimitPolicy::PlannerFaithful, Some(f64::INFINITY)) {
            for l in &out.lines {
                if r.chance(80) {
                    let bp = ri(r, -400, 400) as f64 / 10_000.0;
                    let px = f64_of(case.insts[l.instrument].price, 4);
                    let units = l.target_notional * (1.0 + bp) / px;
                    case.insts[l.instrument].held = (units * 1e4).round() as i128;
                }
            }
        }
    }
    case
}

#[test]
fn twenty_thousand_random_books_agree_on_targets_filter_and_refusals() {
    let mut r = SplitMix64::new(0x5EED_2026_0925_0001);
    let mut st = Stats::default();
    for i in 0..20_000 {
        let c = gen_case(&mut r);
        if let Err(msg) = compare(&c, &mut st) {
            panic!("case {i} disagrees: {msg}\n{c:#?}");
        }
    }
    println!("TARGET-PARITY {st:?}");
    // non-vacuity: every branch the parity claim is about was exercised
    assert_eq!(st.cases, 20_000);
    assert!(st.both_ok_long_only > 3_000, "long-only books: {}", st.both_ok_long_only);
    assert!(st.both_ok_signed > 1_500, "signed books: {}", st.both_ok_signed);
    assert!(st.both_gross_refuse > 100, "gross-cap refusals: {}", st.both_gross_refuse);
    assert!(st.both_bp_required > 200, "buying-power refusals: {}", st.both_bp_required);
    assert!(st.lines_compared > 30_000, "lines: {}", st.lines_compared);
    assert!(st.filter_abs_compared > 500 && st.filter_pct_compared > 500, "filter skips compared: abs {} pct {}", st.filter_abs_compared, st.filter_pct_compared);
    assert!(st.instruments_traded_compared > 5_000, "traded instruments compared: {}", st.instruments_traded_compared);
    assert!(st.max_target_diff <= 2.0e-8, "max target difference {} (one quantum is 1e-8; f64 noise on top)", st.max_target_diff);
    assert!(st.boundary_skipped_outcome <= 20, "boundary-skipped outcomes must stay rare: {}", st.boundary_skipped_outcome);
}

// -------------------------------------------------------------------------------------------------------------
// Enumerated boundary cases
// -------------------------------------------------------------------------------------------------------------

/// One long-only ETF sleeve, share 1, `weights` scale 4 on SPY/EFA, equity 100_000.00, allocated far above, no held
/// position, planner-default filter, prices 100.
fn simple(weights: &[(usize, i128)]) -> Case {
    Case {
        equity: 10_000_000,
        cash: 10_000_000,
        allocated: 100_000_000_000,
        insts: POOL.iter().map(|_| InstCase { price: 1_000_000, held: 0 }).collect(),
        sleeves: vec![SleeveCase { id: "s0".into(), share: 10_000, signed_max: None, weights: weights.to_vec() }],
        risk_a: 10_000,
        risk_b: 10_000,
        max_gross: 100,
        min_abs: 1000,
        min_pct: 200,
        buying_power: None,
    }
}

/// A signed sleeve whose gross is `gross_weight_sum` (scale 4) times the capital base of 100_000.00.
fn signed_gross(w0: i128, w1: i128, max_gross: i128) -> Case {
    let mut c = simple(&[(0, w0), (1, w1)]);
    c.sleeves[0].signed_max = Some(300);
    c.max_gross = max_gross;
    c.buying_power = Some(c.equity * 20);
    c
}

fn outcomes(c: &Case) -> (Outcome, Outcome) {
    (outcome_of_plan(&plan_of(c)).expect("planner outcome"), outcome_of_construct(&construct_of(c)).expect("construct outcome"))
}

/// The case's mandate with EVERY cap (gross, leverage, net, position, class) set to exactly `mg`.
fn mandate_with_cap(c: &Case, mg: f64) -> MandateBody {
    let mut v = serde_json::to_value(mandate_for(c)).expect("mandate serializes");
    v["universe"]["leverage_max_gross"] = json!(mg.max(1.0));
    v["exposure"]["max_gross"] = json!(mg);
    v["exposure"]["max_net"] = json!(mg);
    v["exposure"]["max_position"] = json!(mg);
    v["exposure"]["max_asset_class"] = json!({"us_etf": mg, "crypto_spot": mg});
    serde_json::from_value(v).expect("mandate parses")
}

fn outcomes_at_cap(c: &Case, mg: f64) -> (Result<OrderPlan, PlanError>, Result<pc::ConstructOutput, pc::ConstructRefusal>) {
    (plan_of_with(c, &mandate_with_cap(c, mg)), construct_with(c, pc::LimitPolicy::PlannerFaithful, Some(mg)))
}

#[test]
fn gross_cap_boundary_exact_and_relative_neighbours_agree() {
    // gross = |1.0| + |-0.5| = 1.5 x the capital base, cap 1.5: exactly at the cap is permitted by both
    let at_cap = signed_gross(10_000, -5_000, 150);
    assert_eq!(outcomes(&at_cap), (Outcome::Ok, Outcome::Ok), "gross exactly at the cap is permitted by both");
    // weights are scale 4: 1e-4 over / under the cap is 6.7e-5 relative, far outside any tolerance band
    assert_eq!(outcomes(&signed_gross(10_001, -5_000, 150)), (Outcome::GrossAboveCap, Outcome::GrossAboveCap));
    assert_eq!(outcomes(&signed_gross(9_999, -5_000, 150)), (Outcome::Ok, Outcome::Ok));
    // one part in a BILLION either side, through a mandate cap of 1.5 x (1 -/+ 1e-9): the design's own boundary (1e-9)
    let (p, k) = outcomes_at_cap(&at_cap, 1.5 * (1.0 - 1e-9));
    assert!(matches!(p, Err(PlanError::GrossAboveCap { .. })), "planner refuses 1e-9 over the cap: {p:?}");
    assert!(matches!(k, Err(pc::ConstructRefusal::GrossAboveCap { .. })), "construct refuses 1e-9 over the cap: {k:?}");
    let (p, k) = outcomes_at_cap(&at_cap, 1.5 * (1.0 + 1e-9));
    assert!(p.is_ok() && k.is_ok(), "both permit 1e-9 under the cap: {p:?} / {k:?}");
}

#[test]
fn trade_filter_ties_and_neighbours_agree() {
    // one long-only SPY target of 0.2 x 100_000.00 = 20_000.00 at price 100.0000: absolute floor 10.00, band 2% = 400.00
    // (label, held units scale 4, expect skipped, exactly on a threshold)
    // (label, held units scale 4, min_pct scale 4, expect skipped, exactly on a threshold); the absolute floor is tested
    // with the band switched off (it would otherwise bind first: 2% of 20_000 is 400 >> 10)
    let cases: [(&str, i128, i128, bool, bool); 4] = [
        ("delta exactly min_abs (19_990.00 held)", 199_9000, 0, false, true),
        ("delta one cent under min_abs (19_990.01 held)", 199_9001, 0, true, false),
        ("delta exactly the 2% band (19_600.00 held)", 196_0000, 200, false, true),
        ("delta one cent under the band (19_600.01 held)", 196_0001, 200, true, false),
    ];
    for (label, held, min_pct, expect_skip, on_boundary) in cases {
        let mut c = simple(&[(0, 2000)]);
        c.min_pct = min_pct;
        c.insts[0].held = held;
        let mut st = Stats::default();
        compare(&c, &mut st).unwrap_or_else(|e| panic!("{label}: {e}"));
        let plan = plan_of(&c).expect("plan");
        let skipped = plan.skipped.iter().any(|s| matches!(s.reason, SkipReason::BelowMinTradeAbs { .. } | SkipReason::BelowMinTradePct { .. }));
        assert_eq!(skipped, expect_skip, "{label}: planner filter");
        let out = construct_of(&c).expect("construct");
        let skipped_k = out.skipped.iter().any(|s| matches!(s.reason, pc::SkipReason::BelowMinAbs { .. } | pc::SkipReason::BelowMinPct { .. }));
        assert_eq!(skipped_k, expect_skip, "{label}: construct filter");
        // exact ties are excluded from the RANDOM comparison (they are asserted here, one by one)
        assert_eq!(st.filter_instruments_boundary_skipped, usize::from(on_boundary), "{label}");
    }
}

#[test]
fn full_exit_uses_the_current_holding_as_the_band_reference() {
    // target 0 (weight 0: managed, so it is sold), held 199.9 x 100 = 19_990.00: the band reference is |current|
    let mut c = simple(&[(0, 0)]);
    c.insts[0].held = 199_9000;
    let plan = plan_of(&c).expect("plan");
    let out = construct_of(&c).expect("construct");
    assert!(plan.skipped.iter().all(|s| !matches!(s.reason, SkipReason::BelowMinTradeAbs { .. } | SkipReason::BelowMinTradePct { .. })), "a full exit is not filtered away");
    assert!(out.skipped.iter().all(|s| !matches!(s.reason, pc::SkipReason::BelowMinAbs { .. } | pc::SkipReason::BelowMinPct { .. })));
    assert_eq!(out.trades.len(), 1);
    // a dust holding below the absolute floor IS filtered by both (never sold): 0.05 units x 100 = 5.00 < 10.00
    let mut dust = simple(&[(0, 0)]);
    dust.insts[0].held = 500;
    let plan = plan_of(&dust).expect("plan");
    let out = construct_of(&dust).expect("construct");
    assert!(plan.skipped.iter().any(|s| matches!(s.reason, SkipReason::BelowMinTradeAbs { .. })));
    assert!(out.skipped.iter().any(|s| matches!(s.reason, pc::SkipReason::BelowMinAbs { .. })));
}

// -------------------------------------------------------------------------------------------------------------
// PINNED EXPECTED-DIVERGENCES (each is a ledger entry: a fix that changes either side must fail one of these)
// -------------------------------------------------------------------------------------------------------------

/// Ledger `DECIMAL_QUANTUM_EDGE_BAND`: the planner compares exact decimals, construct compares with a 1e-12 relative
/// tolerance. A mandate gross cap of 1.4999999999999 against a book whose gross is exactly 1.5 x the capital base
/// exceeds the cap by ONE Decimal quantum (1e-8 currency units on a 100_000.00 base, 6.7e-14 relative): the planner
/// REFUSES it, construct PERMITS it. The design's parity claim excludes exactly this band (within 1e-9 of the
/// boundary); this test pins that the band is real, one quantum wide, and where the two disagree.
#[test]
fn t1_decimal_quantum_edge_band_is_a_pinned_expected_divergence() {
    let c = signed_gross(10_000, -5_000, 150);
    let (p, k) = outcomes_at_cap(&c, 1.4999999999999);
    match p {
        Err(PlanError::GrossAboveCap { gross, cap }) => {
            let excess = rebalancer_core::dec_math::sub(gross, cap).expect("no overflow");
            assert_eq!(excess, dec(1, 8), "the planner's excess over the cap is exactly one 8-decimal quantum");
        }
        other => panic!("the planner must refuse (exact decimals): {other:?}"),
    }
    assert!(k.is_ok(), "construct permits within its 1e-12 relative tolerance: {k:?}");
}

/// Ledger `PER_ORDER_DENIAL_VS_WHOLE_BOOK_REFUSAL` (position cap; council R1/R2 are rulings, not yet code in
/// SignalEngine): the book that `construct` under R1/R2 semantics (`RefuseWholeBook`) refuses whole is PLANNED by
/// today's planner, and the per-order guard then denies only the offending order and places the rest. `construct` in
/// `PlannerFaithful` mode reproduces the planner, so the difference is exactly "R1/R2 are not in the planner".
#[test]
fn t1_r1_r2_per_order_denial_vs_whole_book_refusal_pinned() {
    // one long-only ETF sleeve, share 1.0: SPY 0.5, EFA 0.2, IEF 0.2 of a 100_000 account, position cap 0.25
    let c = simple(&[(0, 5000), (1, 2000), (2, 2000)]);
    let mut m = serde_json::to_value(mandate_for(&c)).expect("mandate serializes");
    m["exposure"]["max_position"] = json!(0.25);
    m["exposure"]["max_turnover_per_day"] = json!(5.0);
    let tight: MandateBody = serde_json::from_value(m).expect("mandate parses");
    let plan = plan_of_with(&c, &tight).expect("today's planner plans the book (no plan-level position check)");
    let placed: Vec<&str> = plan.orders.iter().map(|o| o.symbol.as_str()).collect();
    let denied: Vec<(&str, Vec<DenialCode>)> = plan.denied.iter().map(|d| (d.order.symbol.as_str(), d.reasons.iter().map(|r| r.code).collect())).collect();
    assert_eq!(placed, vec!["EFA", "IEF"], "the two orders inside the cap are placed");
    assert_eq!(denied, vec![("SPY", vec![DenialCode::MaxPosition])], "only the offending order is denied, per order");
    // the specification, planner-faithful: the same targets, no refusal
    let faithful = construct_with(&c, pc::LimitPolicy::PlannerFaithful, None).expect("planner-faithful construct permits");
    assert_eq!(faithful.lines.len(), 3);
    // the specification under R1/R2: the whole book is refused (never a partial book)
    let sleeves = vec![pc::SleeveTargets::long_only("s0", 1.0, vec![(0, 0.5), (1, 0.2), (2, 0.2)])];
    let facts: Vec<pc::InstrumentFacts> = POOL.iter().map(|(s, cl)| pc::InstrumentFacts::new(s, "alpaca", cl, 100.0)).collect();
    let limits = pc::Limits::long_only_unit().with_max_position(0.25).with_policy(pc::LimitPolicy::RefuseWholeBook);
    let refused = pc::construct(&pc::ConstructInputs {
        equity: 100_000.0,
        allocated_capital: None,
        sleeves: &sleeves,
        risk_scale: pc::RiskScale::ONE,
        limits: &limits,
        instruments: &facts,
        margin: &pc::NoMargin,
        trade_filter: pc::TradeFilter::PLANNER_DEFAULT,
        rounding: None,
        funding: pc::Funding::Unconstrained,
        target_dp: Some(8),
        unmanaged_gross: 0.0,
    });
    assert!(matches!(refused, Err(pc::ConstructRefusal::PositionAboveCap { .. })), "R1/R2: the whole book is refused: {refused:?}");
}

/// Ledger `PER_ORDER_DENIAL_VS_WHOLE_BOOK_REFUSAL` (shorting): a signed sleeve under a mandate that forbids shorting is
/// planned, the long leg is placed and only the short order is denied; R1/R2 semantics refuse the whole book.
#[test]
fn t1_shorting_forbidden_is_a_per_order_denial_not_a_whole_book_refusal_pinned() {
    let mut c = signed_gross(5_000, -3_000, 200);
    c.sleeves[0].weights = vec![(0, 5000), (1, -3000)];
    let mut m = serde_json::to_value(mandate_for(&c)).expect("mandate serializes");
    m["universe"]["shorting"] = json!(false);
    let no_short: MandateBody = serde_json::from_value(m).expect("mandate parses");
    let plan = plan_of_with(&c, &no_short).expect("planned: shorting is the guard's call, not the planner's");
    assert!(plan.orders.iter().any(|o| o.symbol == "SPY" && o.side == Side::Buy), "the long leg is placed");
    assert!(
        plan.denied.iter().any(|d| d.order.symbol == "EFA" && d.reasons.iter().any(|r| r.code == DenialCode::ShortingForbidden)),
        "the short leg is denied per order: {:?}",
        plan.denied
    );
    let sleeves = vec![pc::SleeveTargets::signed("s0", 1.0, 3.0, vec![(0, 0.5), (1, -0.3)])];
    let facts: Vec<pc::InstrumentFacts> = POOL.iter().map(|(s, cl)| pc::InstrumentFacts::new(s, "alpaca", cl, 100.0)).collect();
    let limits = pc::Limits::unlimited().with_max_gross(2.0).with_shorting(false).with_policy(pc::LimitPolicy::RefuseWholeBook);
    let r = pc::construct(&pc::ConstructInputs {
        equity: 100_000.0,
        allocated_capital: None,
        sleeves: &sleeves,
        risk_scale: pc::RiskScale::ONE,
        limits: &limits,
        instruments: &facts,
        margin: &pc::NoMargin,
        trade_filter: pc::TradeFilter::PLANNER_DEFAULT,
        rounding: None,
        funding: pc::Funding::BuyingPower { buying_power: 1.0e9, reserve_fraction: 0.05, fee_rate: 0.0026 },
        target_dp: Some(8),
        unmanaged_gross: 0.0,
    });
    assert!(matches!(r, Err(pc::ConstructRefusal::ShortingForbidden { .. })), "R1/R2: {r:?}");
}

/// Ledger `ZERO_RISK_SCALE`: `construct` allows a ladder scale of 0 (a halted book flattens: every target is 0); the
/// planner refuses a risk scale of 0 (`BadRiskScale`), because in the live pipeline a halt flattens through a different
/// path and never asks the planner for a zero-scale plan.
#[test]
fn t1_zero_risk_scale_planner_refuses_construct_flattens_pinned() {
    let mut c = simple(&[(0, 2000)]);
    c.insts[0].held = 200_0000;
    let mut zero = c.clone();
    zero.risk_a = 0;
    let p = plan_of(&zero);
    assert!(matches!(p, Err(PlanError::BadRiskScale(_))), "{p:?}");
    let sleeves = vec![pc::SleeveTargets::long_only("s0", 1.0, vec![(0, 0.2)])];
    let facts = vec![pc::InstrumentFacts::new("SPY", "alpaca", "us_etf", 100.0).with_held(200.0)];
    let limits = pc::Limits::long_only_unit().with_policy(pc::LimitPolicy::PlannerFaithful);
    let out = pc::construct(&pc::ConstructInputs {
        equity: 100_000.0,
        allocated_capital: None,
        sleeves: &sleeves,
        risk_scale: pc::RiskScale::new(1.0, 0.0),
        limits: &limits,
        instruments: &facts,
        margin: &pc::NoMargin,
        trade_filter: pc::TradeFilter::PLANNER_DEFAULT,
        rounding: None,
        funding: pc::Funding::Unconstrained,
        target_dp: Some(8),
        unmanaged_gross: 0.0,
    })
    .expect("a zero ladder scale is allowed");
    assert_eq!(out.target_notional[0], 0.0);
    assert_eq!(out.trades.len(), 1);
    assert_eq!(out.trades[0].side, pc::Side::Sell);
}

/// Ledger `UNMANAGED_GROSS_CALLER_CONTRACT`: the planner counts as UNMANAGED every position it has no plan line for,
/// which includes an instrument a long-only sleeve NAMES while a short is held in it (`ShortPositionHeld`: skipped,
/// never touched, but still exposure). `construct`'s `unmanaged_gross` is the CALLER's number, so a caller that counts
/// only the instruments no sleeve names gets a different `needs_margin`: here the planner (and the correct caller)
/// require buying power, the naive caller does not. Found while writing this test's glue.
#[test]
fn t1_short_held_in_a_long_only_instrument_counts_as_unmanaged_exposure_pinned() {
    let mut c = simple(&[(0, 10_000)]);
    c.sleeves[0].share = 5_000;
    c.sleeves[0].signed_max = Some(300);
    c.sleeves.push(SleeveCase { id: "s1".into(), share: 3_000, signed_max: None, weights: vec![(4, 10_000)] });
    c.insts[4].held = -600_0000; // short 600 BTC at 100.0 = 60_000 of exposure, in an instrument the long-only sleeve names
    c.max_gross = 300;
    let planned = plan_of(&c);
    assert!(matches!(planned, Err(PlanError::BuyingPowerRequired { .. })), "planner: 50_000 target + 60_000 held short > 100_000 equity needs margin: {planned:?}");
    let right = construct_of(&c);
    assert!(matches!(right, Err(pc::ConstructRefusal::BuyingPowerRequired { .. })), "the correct caller contract agrees: {right:?}");
    let naive = construct_with_unmanaged(&c, pc::LimitPolicy::PlannerFaithful, None, Some(0.0));
    assert!(naive.is_ok(), "a caller that forgets the skipped short sees no margin need: {naive:?}");
}

/// Rules that return the planner's wish UNCHANGED (18 decimals): not a real venue table (real ones round to at most 8-9
/// decimals), a probe of the planner's exact arithmetic.
struct Scale18Rules;

impl VenueRules for Scale18Rules {
    fn round_quantity(&self, _symbol: &str, _side: Side, quantity: Dec, _price: Dec) -> Result<Dec, SizeRefusal> {
        Ok(quantity)
    }
    fn fingerprint(&self, symbol: &str) -> String {
        format!("scale18:{symbol}")
    }
}

/// Ledger `PLANNER_CASH_SCALING_OVERFLOW_AT_SCALE_18` (LATENT, not reachable with the shipped venue tables): when the buys
/// do not fit the cash, the planner computes `div_floor(usable, total, 18)`; with a `total` at scale 18 and a `usable`
/// budget above about 170 units of currency the exact `i128` numerator overflows and the plan FAILS CLOSED
/// (`PlanError::Math(Overflow)`, run code `RUN_PLAN_ERROR`, nothing traded). Every real `VenueRules` returns a quantity
/// of at most 8-9 decimals (Kraken lot decimals, Alpaca whole/fractional shares, OANDA unit precision), which keeps
/// `total` at a small scale, so the shipped pipeline is unaffected; a new venue table that returned an unrounded
/// quantity would fail closed on every cash-limited plan.
#[test]
fn t1_planner_cash_scaling_overflows_for_a_scale_18_quantity_pinned() {
    let mut c = simple(&[(0, 10_000)]);
    c.cash = 1_000_000; // 10_000.00 of cash for a 100_000.00 target: the buy must be scaled down
    let ok8 = plan_with_rules(&c, &mandate_for(&c), &PlainRules).expect("8-decimal quantities scale fine");
    assert_eq!(ok8.orders.len(), 1);
    let bad = plan_with_rules(&c, &mandate_for(&c), &Scale18Rules);
    assert!(matches!(bad, Err(PlanError::Math(_))), "an 18-decimal quantity overflows the exact cash-scaling step: {bad:?}");
}
