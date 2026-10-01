//! PARITY TEST 4 (design 5.4 #4, the decisive one): whole-pipeline replay.
//!
//! A synthetic history is fed DAY BY DAY to the real driver + `run_once` (Live mode, so orders are placed and the run
//! re-reads the account, re-plans on real cash and reconciles) over a `SimBroker` that fills every order at the close
//! the run sized on, with ZERO fees. The resulting equity, positions and orders are compared with `simulate_book`
//! configured like the live pipeline: `portfolio-construct` behind the `Construct` boundary (planner trade filter, whole
//! shares / 8-decimal crypto FLOOR rounding by a lot table, `min(equity, allocated)`, the mandate's cash reserve and the
//! planner's estimated fee in the buy budget, the live executor's sells-then-buys order), `BookCadence::PerSleeve` (only
//! due sleeves), and a STATED execution delay.
//!
//! # The stated delays (council Ruling 4, pre-registered)
//! * ETF sleeve: `execution_delay_bars = 1` in its own bars. The pipeline's run of day `D` sees bars up to `D - 1`; the
//!   ETF decision of the month-end session `L` is computable from the first session `F` of the next month (`NextMonthBar`),
//!   the run of day `F + 1` acts, at the close of `F`: one own bar after the decision bar.
//! * Crypto sleeve: `execution_delay_bars = 0`. The run of day `D` decides on bar `D - 1` and fills at that close.
//!
//! # Comparison keys
//! The run of day `D` corresponds to the account-clock bar dated `D - 1`. Per replayed day with such a bar: equity after
//! the run (relative 1e-9), positions (ETF shares exactly, crypto within one 1e-8 quantum), the orders of the run (symbol,
//! side, quantity), the plan's target weights against `target_weights[k]`, and the decision dates.
//!
//! # Mixed accounts (per-sleeve delay, weightsim 0.3)
//! `SleeveSpec::with_execution_delay` gives each sleeve its own delay in its OWN bars, so the mixed ETF + crypto account is
//! configured exactly as the council ruled: ETF 1, crypto 0 (book-level default 0). It is asserted to agree with the
//! pipeline on equity, positions, orders and target weights, and to equal the test-side EMULATION (`DelayedEtf`, which
//! decides one own bar later under a book delay of 0) that the first version of this test used before the feature existed.
//! A wrong ETF delay (0) is asserted NOT to agree, so the agreement above is not vacuous.

mod common;

use std::collections::BTreeMap;

use broker_adapters::Side;
use chrono::{Duration, NaiveDate};
use common::adapters::{from_naive, se_symbol, to_naive, CryptoRule, DelayedEtf, EtfRule, PcConstruct};
use common::replay::{account, crypto_sleeve, etf_sleeve, replay_mandate, DayLog, Rig};
use common::world::{all_days, date, World};
use rebalancer_run::record::{ExecutionMode, OutcomeKind};
use weightsim as ws;
use weightsim::{BarTime, Book, BookCadence, BookConfig, BookPanel, BookResult, OnRefusal, SessionKind, ShareSpec, SleeveSpec};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    EtfOnly,
    CryptoOnly,
    Mixed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EtfTiming {
    /// The book's native delay (one integer for the whole book).
    Native(usize),
    /// Book delay 0 and the ETF rule decides one own bar later (`DelayedEtf`).
    Emulated,
}

const RESERVE: f64 = 0.05;
const CASH: &str = "100000";

fn world() -> World {
    World::build(date(2017, 1, 1), date(2020, 6, 30))
}

/// The run window: the first run is the day after the first session of March 2019 (the ETF's regular action day, which
/// is also where the account is flat and enters), the last is 2020-03-31.
fn window(world: &World) -> (NaiveDate, NaiveDate, NaiveDate) {
    let f = world.cal.first_session(2019, 3);
    (f + Duration::days(1), date(2020, 3, 31), world.cal.last_session(2019, 2))
}

/// The window of the NATIVE mixed comparison: `(first run day, last run day, account start bar)`.
///
/// Why a dedicated window: a native ETF delay of 1 needs the ETF's month-end DECISION bar `L` inside the book (the account
/// must start at `L` for the entry decision to exist), while the pipeline's first run (day `F + 1`, bar `F`) also starts the
/// crypto sleeve, which a book starting at `L` would already have started one bar earlier. The two coincide when the crypto
/// rule is ALL CASH at `L` (nothing bought at `L`, so both accounts are flat and identical at `F`). So the window is the
/// first month, from March 2019, whose last session has both crypto signals off (a property of the synthetic world, found
/// here rather than hard-coded). The pipeline's ENTRY on the decision in force is what the book's `L` decision reproduces.
fn mixed_native_window(world: &World) -> (NaiveDate, NaiveDate, NaiveDate) {
    let (mut y, mut m) = (2019, 3);
    loop {
        let (py, pm) = if m == 1 { (y - 1, 12) } else { (y, m - 1) };
        let l = world.cal.last_session(py, pm);
        let upto = world.crypto_days.partition_point(|d| *d <= l);
        let dates: Vec<ws::Date> = world.crypto_days[..upto].iter().map(|d| from_naive(*d)).collect();
        let cols: Vec<&[f64]> = (0..2).map(|i| &world.crypto[i][..upto]).collect();
        let w = common::adapters::crypto_weights(&dates, &cols).expect("crypto decision");
        if w.iter().all(|x| *x == 0.0) {
            return (world.cal.first_session(y, m) + Duration::days(1), date(2020, 3, 31), l);
        }
        (y, m) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
        assert!(y < 2020, "no all-cash month-end found");
    }
}

fn sleeves_of(kind: Kind) -> Vec<rebalancer_run::data::SleeveSpec> {
    match kind {
        Kind::EtfOnly => vec![etf_sleeve("1")],
        Kind::CryptoOnly => vec![crypto_sleeve("1")],
        Kind::Mixed => vec![etf_sleeve("0.6"), crypto_sleeve("0.4")],
    }
}

fn se_replay(world: &World, kind: Kind, allocated: &str) -> Vec<DayLog> {
    let (from, to, _) = window(world);
    se_replay_in(world, kind, allocated, from, to)
}

fn se_replay_in(world: &World, kind: Kind, allocated: &str, from: NaiveDate, to: NaiveDate) -> Vec<DayLog> {
    let acct = account(sleeves_of(kind), ExecutionMode::Live, replay_mandate(allocated, RESERVE));
    let rig = Rig::new(world, acct, CASH);
    let logs = rig.replay(from, to);
    for l in &logs {
        assert_eq!(l.rec.outcome.kind, OutcomeKind::Completed, "{}: {} {}", l.day, l.rec.outcome.code, l.rec.outcome.message);
    }
    logs
}

/// The backtester side. `start_bar` is the account's first bar.
struct WsSpec {
    kind: Kind,
    etf: EtfTiming,
    /// `Some(construct)` = the live-faithful adapter; `None` = weightsim's own `MinimalConstruct` (Budget cash).
    live_faithful: bool,
    allocated: Option<f64>,
    start_bar: NaiveDate,
    end_bar: NaiveDate,
}

fn ws_run(world: &World, s: &WsSpec) -> BookResult {
    let mut series = Vec::new();
    let holidays: Vec<ws::Date> = world.cal.holidays.iter().map(|d| from_naive(*d)).collect();
    let mut symbols: Vec<String> = Vec::new();
    if s.kind != Kind::CryptoOnly {
        for (i, name) in World::etf_symbols().iter().enumerate() {
            let rows: Vec<(ws::Date, f64)> = world.etf_days.iter().zip(&world.etf[i]).filter(|(d, _)| **d <= s.end_bar).map(|(d, p)| (from_naive(*d), *p)).collect();
            series.push((name.clone(), SessionKind::exchange("US", holidays.clone()), rows));
            symbols.push(name.clone());
        }
    }
    let etf_n = symbols.len();
    if s.kind != Kind::EtfOnly {
        for (i, name) in ["BTC", "ETH"].iter().enumerate() {
            let rows: Vec<(ws::Date, f64)> = world.crypto_days.iter().zip(&world.crypto[i]).filter(|(d, _)| **d <= s.end_bar).map(|(d, p)| (from_naive(*d), *p)).collect();
            series.push(((*name).to_string(), SessionKind::Continuous, rows));
            symbols.push(se_symbol(name));
        }
    }
    let panel = BookPanel::from_dated_series(series).expect("panel");
    let mut sleeves = Vec::new();
    let share_etf = if s.kind == Kind::Mixed { 0.6 } else { 1.0 };
    let share_cry = if s.kind == Kind::Mixed { 0.4 } else { 1.0 };
    if s.kind != Kind::CryptoOnly {
        let universe: Vec<usize> = (0..5).collect();
        sleeves.push(match s.etf {
            EtfTiming::Native(d) => SleeveSpec::from_rule("etf", EtfRule, universe, ShareSpec::Fixed(share_etf)).with_execution_delay(d),
            EtfTiming::Emulated => SleeveSpec::from_rule("etf", DelayedEtf, universe, ShareSpec::Fixed(share_etf)),
        });
    }
    if s.kind != Kind::EtfOnly {
        sleeves.push(SleeveSpec::from_rule("crypto", CryptoRule, (etf_n..etf_n + 2).collect(), ShareSpec::Fixed(share_cry)).with_execution_delay(0));
    }
    let book = Book::new(sleeves);
    let mut cfg = BookConfig::default();
    cfg.sim.on_refusal = OnRefusal::HoldPrevious;
    cfg.sim.initial_equity = 100_000.0;
    // the book-level value is only the DEFAULT now; every sleeve above states its own delay
    cfg.sim.execution_delay_bars = 0;
    cfg.cadence = BookCadence::PerSleeve;
    cfg.account_start = Some(BarTime::from_date(from_naive(s.start_bar)));
    cfg.allocated_capital = s.allocated;
    cfg.trade_filter = Some(ws::TradeFilter { min_abs: 10.0, min_pct: 0.02 });
    cfg.cash_policy = ws::CashPolicy::Budget;
    if s.live_faithful {
        let pc = PcConstruct::for_instruments(&symbols, RESERVE, 0.0026);
        ws::simulate_book_with(&panel, &book, &cfg, &pc).expect("simulate_book_with")
    } else {
        ws::simulate_book(&panel, &book, &cfg).expect("simulate_book")
    }
}

#[derive(Debug, Default)]
struct Report {
    days: usize,
    bars_compared: usize,
    runs_with_orders: usize,
    orders_compared: usize,
    max_rel_equity: f64,
    max_qty_diff: f64,
    qty_exact: usize,
    qty_total: usize,
    max_weight_diff: f64,
    plans_compared: usize,
    denied_orders: usize,
    first_mismatch: Option<String>,
}

const EQUITY_REL_TOL: f64 = 1e-9;
const WEIGHT_TOL: f64 = 1e-9;
/// One crypto quantum (8 decimals) plus float slack; whole-share ETF quantities must match EXACTLY.
const CRYPTO_QTY_TOL: f64 = 1.5e-8;

/// Compare the pipeline's day logs with a weightsim result. Every check is recorded; a failed check is reported in
/// `first_mismatch` (and asserted by the caller) so that the divergence-hunting tests can print what differs.
fn compare(logs: &[DayLog], res: &BookResult, kind: Kind, check_orders_and_positions: bool) -> Report {
    let mut rep = Report::default();
    let by_bar: BTreeMap<NaiveDate, usize> = res.times.iter().enumerate().map(|(k, t)| (to_naive(t.date()), k)).collect();
    let symbols: Vec<String> = res.instruments.iter().map(|n| se_symbol(n)).collect();
    let miss = |rep: &mut Report, msg: String| {
        if rep.first_mismatch.is_none() {
            rep.first_mismatch = Some(msg);
        }
    };
    for l in logs {
        rep.days += 1;
        rep.denied_orders += l.rec.plan.as_ref().map_or(0, |p| p.denied.len());
        let Some(&k) = by_bar.get(&l.day.pred_opt().expect("has a predecessor")) else { continue };
        rep.bars_compared += 1;
        // ---- equity after the run
        let se_eq = l.equity_after.to_f64();
        let rel = (se_eq - res.equity[k]).abs() / res.equity[k];
        rep.max_rel_equity = rep.max_rel_equity.max(rel);
        if rel > EQUITY_REL_TOL {
            miss(&mut rep, format!("{}: equity SE {se_eq} vs weightsim {} (rel {rel:e})", l.day, res.equity[k]));
        }
        if !check_orders_and_positions {
            continue;
        }
        // ---- positions
        let row = res.inst_row(&res.units, k);
        for (j, sym) in symbols.iter().enumerate() {
            let se_q = l.holdings_after.get(&sym.to_uppercase()).map_or(0.0, |q| q.to_f64());
            let diff = (se_q - row[j]).abs();
            rep.max_qty_diff = rep.max_qty_diff.max(diff);
            rep.qty_total += 1;
            if diff == 0.0 {
                rep.qty_exact += 1;
            }
            let tol = if World::is_etf(sym) { 0.0 } else { CRYPTO_QTY_TOL };
            if diff > tol {
                miss(&mut rep, format!("{}: position {sym} SE {se_q} vs weightsim {} (diff {diff:e})", l.day, row[j]));
            }
        }
        // ---- orders of the run: SE fills against the weightsim unit deltas of the bar
        let prev: Vec<f64> = if k == 0 { vec![0.0; symbols.len()] } else { res.inst_row(&res.units, k - 1).to_vec() };
        let mut want: Vec<(String, bool, f64)> = Vec::new();
        for (j, sym) in symbols.iter().enumerate() {
            let dq = row[j] - prev[j];
            if dq.abs() > 1e-12 {
                want.push((sym.clone(), dq > 0.0, dq.abs()));
            }
        }
        want.sort_by(|a, b| a.0.cmp(&b.0));
        let mut got: Vec<(String, bool, f64)> = l.fills.iter().map(|(s, side, q)| (s.clone(), *side == Side::Buy, q.to_f64())).collect();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        if !got.is_empty() {
            rep.runs_with_orders += 1;
        }
        let same = want.len() == got.len()
            && want.iter().zip(&got).all(|(w, g)| {
                let tol = if World::is_etf(&w.0) { 0.0 } else { CRYPTO_QTY_TOL };
                w.0 == g.0 && w.1 == g.1 && (w.2 - g.2).abs() <= tol
            });
        rep.orders_compared += want.len();
        if !same {
            miss(&mut rep, format!("{}: orders SE {got:?} vs weightsim {want:?}", l.day));
        }
        // ---- the plan's targets against target_weights[k] (planned sleeves' instruments only)
        if let Some(plan) = &l.rec.plan {
            let tw = res.inst_row(&res.target_weights, k);
            for line in &plan.lines {
                let Some(j) = symbols.iter().position(|s| s.eq_ignore_ascii_case(&line.symbol)) else { continue };
                let w_se = line.target_notional.to_f64() / plan.capital_base.to_f64();
                let diff = (w_se - tw[j]).abs();
                rep.max_weight_diff = rep.max_weight_diff.max(diff);
                if diff > WEIGHT_TOL {
                    miss(&mut rep, format!("{}: target weight {} SE {w_se} vs weightsim {} (diff {diff:e})", l.day, line.symbol, tw[j]));
                }
            }
            rep.plans_compared += 1;
        }
    }
    let _ = kind;
    rep
}

fn print(name: &str, r: &Report) {
    println!(
        "REPLAY {name}: days {} bars {} runs_with_orders {} orders {} max_rel_equity {:.3e} max_qty_diff {:.3e} qty_exact {}/{} max_weight_diff {:.3e} plans {} guard_denials {} first_mismatch {:?}",
        r.days, r.bars_compared, r.runs_with_orders, r.orders_compared, r.max_rel_equity, r.max_qty_diff, r.qty_exact, r.qty_total, r.max_weight_diff, r.plans_compared, r.denied_orders, r.first_mismatch
    );
}

fn assert_clean(name: &str, r: &Report) {
    print(name, r);
    assert!(r.first_mismatch.is_none(), "{name}: {}", r.first_mismatch.clone().unwrap_or_default());
}

/// Debug aid: what the pipeline did on and around `day`.
fn dump(logs: &[DayLog], day: NaiveDate) {
    for l in logs.iter().filter(|l| l.day >= day - Duration::days(2) && l.day <= day + Duration::days(1)) {
        println!("DUMP {} outcome {} fills {:?} holdings {:?} equity {}", l.day, l.rec.outcome.code, l.fills, l.holdings_after, l.equity_after);
        for d in &l.rec.decisions {
            println!("DUMP   decision {} date {} pending {} planned {} evidence {:?}", d.sleeve, d.decision_date, d.pending, d.planned, d.instruments);
        }
        if let Some(p) = &l.rec.plan {
            println!("DUMP   plan equity {} cb {} lines {:?} orders {} denied {} skipped {:?}", p.equity, p.capital_base, p.lines.iter().map(|x| (x.symbol.clone(), x.held.to_string(), x.target_notional.to_string())).collect::<Vec<_>>(), p.orders.len(), p.denied.len(), p.skipped);
        }
    }
}

// -------------------------------------------------------------------------------------------------------------
// Single-sleeve accounts: full agreement, delay stated per test
// -------------------------------------------------------------------------------------------------------------

/// ETF-only account, stated delay: ETF `execution_delay_bars = 1` (own bars).
#[test]
fn etf_only_account_replay_agrees_with_simulate_book_at_delay_1() {
    let w = world();
    let (_, to, start) = window(&w);
    let logs = se_replay(&w, Kind::EtfOnly, "1000000000");
    let res = ws_run(&w, &WsSpec { kind: Kind::EtfOnly, etf: EtfTiming::Native(1), live_faithful: true, allocated: None, start_bar: start, end_bar: to.pred_opt().unwrap(), });
    let rep = compare(&logs, &res, Kind::EtfOnly, true);
    assert_clean("etf_only_d1", &rep);
    assert!(rep.runs_with_orders >= 12, "the ETF sleeve rebalances monthly: {} runs with orders", rep.runs_with_orders);
    assert!(rep.orders_compared >= 40 && rep.bars_compared > 240, "non-vacuous: {rep:?}");
    assert_eq!(rep.denied_orders, 0, "no guard denial: the plan-level arithmetic is what is compared");
    // exactly one action per month, on the pipeline side (the F1 question, settled at the order level)
    let acting: Vec<NaiveDate> = logs.iter().filter(|l| !l.fills.is_empty()).map(|l| l.day).collect();
    assert!(acting.len() <= 14, "at most one acting run per month: {acting:?}");
    let mut months = std::collections::BTreeSet::new();
    for d in &acting {
        assert!(months.insert((chrono::Datelike::year(d), chrono::Datelike::month(d))), "two acting runs in one month: {d}");
    }
}

/// Crypto-only account, stated delay: `execution_delay_bars = 0`.
#[test]
fn crypto_only_account_replay_agrees_with_simulate_book_at_delay_0() {
    let w = world();
    let (from, to, _) = window(&w);
    let logs = se_replay(&w, Kind::CryptoOnly, "1000000000");
    let res = ws_run(&w, &WsSpec { kind: Kind::CryptoOnly, etf: EtfTiming::Native(0), live_faithful: true, allocated: None, start_bar: from.pred_opt().unwrap(), end_bar: to.pred_opt().unwrap() });
    let rep = compare(&logs, &res, Kind::CryptoOnly, true);
    if let Some(m) = &rep.first_mismatch {
        if let Some(day) = m.get(..10).and_then(|d| d.parse::<NaiveDate>().ok()) {
            dump(&logs, day);
            for r in res.refusals.iter().take(5) {
                println!("DUMP ws refusal bar {} {} {} {}", r.bar, r.time, r.code, r.message);
            }
            println!("DUMP ws refusals total {}", res.refusals.len());
            {
                let upto = w.crypto_days.partition_point(|d| *d <= day - Duration::days(1));
                let dates: Vec<ws::Date> = w.crypto_days[..upto].iter().map(|d| from_naive(*d)).collect();
                let cols: Vec<&[f64]> = (0..2).map(|i| &w.crypto[i][..upto]).collect();
                println!("DUMP direct crypto_weights up to {}: {:?} (last close BTC {})", w.crypto_days[upto - 1], common::adapters::crypto_weights(&dates, &cols), w.crypto[0][upto - 1]);
                let sma: f64 = w.crypto[0][upto - 100..upto].iter().sum::<f64>() / 100.0;
                println!("DUMP direct BTC sma100 {sma}");
            }
            for k in 0..res.n_bars() {
                let d = to_naive(res.times[k].date());
                if d >= day - Duration::days(3) && d <= day + Duration::days(1) {
                    println!("DUMP ws bar {d} target {:?} units {:?} planned {:?} due {:?} decision {:?} cash {} equity {} marks {:?}", res.inst_row(&res.target_weights, k), res.inst_row(&res.units, k), res.sleeve_row(&res.planned, k), res.sleeve_row(&res.due, k), res.sleeve_row(&res.decision, k), res.cash[k], res.equity[k], res.inst_row(&res.marks, k));
                }
            }
        }
    }
    assert_clean("crypto_only_d0", &rep);
    assert!(rep.runs_with_orders >= 20, "{rep:?}");
    assert!(rep.orders_compared >= 40 && rep.bars_compared > 380, "non-vacuous: {rep:?}");
    assert_eq!(rep.denied_orders, 0);
}

/// Ledger `GUARD_DENIAL_WHEN_ALLOCATION_BINDS`: `min(equity, allocated)` binds (the equity grows past the allocation).
/// The capital base is capped on both sides and the TARGETS agree on every run; but the per-order guard measures
/// exposure limits against the capital base while the drifted positions carry the excess equity, so it DENIES orders the
/// backtester (which has no guard) places. Pinned: denials happen, only with exposure codes, and the positions diverge
/// only after the first denial.
#[test]
fn t4_allocated_capital_binding_makes_the_guard_deny_orders_the_backtester_places_pinned() {
    let w = world();
    let (from, to, _) = window(&w);
    // allocate slightly above the starting cash: as the crypto trend gains, equity passes it and the cap binds
    let logs = se_replay(&w, Kind::CryptoOnly, "100500");
    let res = ws_run(&w, &WsSpec { kind: Kind::CryptoOnly, etf: EtfTiming::Native(0), live_faithful: true, allocated: Some(100_500.0), start_bar: from.pred_opt().unwrap(), end_bar: to.pred_opt().unwrap() });
    let rep = compare(&logs, &res, Kind::CryptoOnly, true);
    print("crypto_only_allocated", &rep);
    let binding = logs.iter().filter(|l| l.equity_after.to_f64() > 100_500.0).count();
    assert!(binding > 20, "the allocation must bind on many days: {binding}");
    let mut codes: BTreeMap<String, usize> = BTreeMap::new();
    for l in &logs {
        for d in l.rec.plan.iter().flat_map(|p| p.denied.iter()) {
            for c in &d.codes {
                *codes.entry((*c).to_string()).or_default() += 1;
            }
        }
    }
    println!("REPLAY crypto_only_allocated: guard denial codes {codes:?}");
    if let Some((l, d)) = logs.iter().find_map(|l| l.rec.plan.as_ref().and_then(|p| p.denied.first().map(|d| (l, d)))) {
        println!("REPLAY crypto_only_allocated: first denial on {} {} {:?} qty {} notional {} equity {} cb {}: {:?}", l.day, d.order.symbol, d.order.side, d.order.quantity, d.order.notional, l.rec.plan.as_ref().unwrap().equity, l.rec.plan.as_ref().unwrap().capital_base, d.reasons.iter().map(|r| r.message.clone()).collect::<Vec<_>>());
    }
    assert!(rep.denied_orders > 0, "the guard must deny orders while the allocation binds");
    assert!(codes.keys().all(|c| ["MAX_GROSS", "MAX_POSITION", "MAX_ASSET_CLASS", "MAX_NET", "MAX_TURNOVER_PER_DAY"].contains(&c.as_str())), "only exposure-limit denials: {codes:?}");
    // the plan-level TARGET weights agree on every run even here (targets are sized on the same capital base)
    assert!(rep.max_weight_diff <= WEIGHT_TOL, "targets agree while the allocation binds: {:e}", rep.max_weight_diff);
    // ... and the accounts agree until the first denial
    let first_denial = logs.iter().position(|l| l.rec.plan.as_ref().is_some_and(|p| !p.denied.is_empty())).expect("a denial");
    let pre = compare(&logs[..first_denial], &res, Kind::CryptoOnly, true);
    assert!(first_denial > 20, "the first denial comes after the accounts have traded for a while ({first_denial})");
    assert!(pre.first_mismatch.is_none(), "before the first guard denial the accounts agree: {:?}", pre.first_mismatch);
}

// -------------------------------------------------------------------------------------------------------------
// Mixed ETF + crypto account
// -------------------------------------------------------------------------------------------------------------

fn assert_only_due_at_order_level(logs: &[DayLog]) {
    // F1 at the ORDER level: on a day the ETF is not pending, the pipeline places no ETF order at all
    let etf_orders_on_non_action_days = logs
        .iter()
        .filter(|l| !l.rec.decisions.iter().any(|d| d.sleeve == "etf" && d.planned))
        .flat_map(|l| l.fills.iter())
        .filter(|(s, _, _)| World::is_etf(s))
        .count();
    assert_eq!(etf_orders_on_non_action_days, 0, "only-due: no ETF order on a day the ETF decision is not pending");
    let etf_planned_days = logs.iter().filter(|l| l.rec.decisions.iter().any(|d| d.sleeve == "etf" && d.planned)).count();
    assert!(etf_planned_days <= 14, "the ETF sleeve is planned about once a month, not on every run ({etf_planned_days} of {})", logs.len());
}

/// The council's configuration, NATIVE: ETF `with_execution_delay(1)`, crypto `with_execution_delay(0)`, book default 0.
/// The whole mixed account agrees with the pipeline: equity, positions, orders, target weights.
#[test]
fn mixed_account_replay_agrees_with_native_per_sleeve_execution_delay() {
    let w = world();
    let (from, to, start) = mixed_native_window(&w);
    let logs = se_replay_in(&w, Kind::Mixed, "1000000000", from, to);
    let res = ws_run(&w, &WsSpec { kind: Kind::Mixed, etf: EtfTiming::Native(1), live_faithful: true, allocated: None, start_bar: start, end_bar: to.pred_opt().unwrap() });
    let rep = compare(&logs, &res, Kind::Mixed, true);
    assert_clean("mixed_native_per_sleeve_delay_etf1_crypto0", &rep);
    assert!(rep.orders_compared >= 60 && rep.bars_compared > 300, "non-vacuous: {rep:?}");
    assert!(rep.plans_compared > 300, "{rep:?}");
    assert_eq!(rep.denied_orders, 0);
    assert_only_due_at_order_level(&logs);
}

/// The first version of the mixed test (before the feature existed) EMULATED the ETF delay: a rule that decides one own
/// bar later under a book delay of 0. It still agrees with the pipeline, and the native per-sleeve delay reproduces it
/// bar for bar (equity, units), so nothing was lost by switching.
#[test]
fn mixed_account_native_per_sleeve_delay_equals_the_test_side_emulation() {
    let w = world();
    let (from, to, start) = mixed_native_window(&w);
    let logs = se_replay_in(&w, Kind::Mixed, "1000000000", from, to);
    let mk = |etf, start_bar| ws_run(&w, &WsSpec { kind: Kind::Mixed, etf, live_faithful: true, allocated: None, start_bar, end_bar: to.pred_opt().unwrap() });
    let native = mk(EtfTiming::Native(1), start);
    let emulated = mk(EtfTiming::Emulated, from.pred_opt().unwrap());
    let rep = compare(&logs, &emulated, Kind::Mixed, true);
    assert_clean("mixed_emulated_etf_delay", &rep);
    // the native book starts one bar earlier (at the decision bar): compare on the bars both have
    let mut max_eq: f64 = 0.0;
    let mut max_units: f64 = 0.0;
    let mut compared = 0;
    for (ke, t) in emulated.times.iter().enumerate() {
        let Some(kn) = native.times.iter().position(|x| x == t) else { continue };
        compared += 1;
        max_eq = max_eq.max((native.equity[kn] - emulated.equity[ke]).abs() / native.equity[kn]);
        for (a, b) in native.inst_row(&native.units, kn).iter().zip(emulated.inst_row(&emulated.units, ke)) {
            max_units = max_units.max((a - b).abs());
        }
    }
    assert!(compared > 300, "{compared}");
    println!("REPLAY mixed native-vs-emulated: max rel equity diff {max_eq:.3e}, max units diff {max_units:.3e}");
    assert!(max_eq <= 1e-12 && max_units <= 1e-9, "native per-sleeve delay == emulation: {max_eq:e} / {max_units:e}");
}

/// Sensitivity of the comparison itself: with the ETF delay set to 0 the ETF sleeve trades one own bar EARLIER than the
/// pipeline and the accounts do not agree, while the crypto sleeve's decisions (target weights) stay identical. This is what
/// the agreement above would have looked like if the delay were wrong.
#[test]
fn mixed_account_with_the_wrong_etf_delay_does_not_agree() {
    let w = world();
    let (from, to, start) = window(&w);
    let _ = from;
    let logs = se_replay(&w, Kind::Mixed, "1000000000");
    // the account starts at the ETF decision bar (last session of February) so that the first ETF decision is inside the book
    let res = ws_run(&w, &WsSpec { kind: Kind::Mixed, etf: EtfTiming::Native(0), live_faithful: true, allocated: None, start_bar: start, end_bar: to.pred_opt().unwrap() });
    let by_bar: BTreeMap<NaiveDate, usize> = res.times.iter().enumerate().map(|(k, t)| (to_naive(t.date()), k)).collect();
    let mut crypto_weights_compared = 0;
    let mut max_diff: f64 = 0.0;
    for l in &logs {
        let Some(&k) = by_bar.get(&l.day.pred_opt().unwrap()) else { continue };
        let Some(plan) = &l.rec.plan else { continue };
        let tw = res.inst_row(&res.target_weights, k);
        for line in plan.lines.iter().filter(|x| !World::is_etf(&x.symbol)) {
            let j = res.instruments.iter().position(|n| se_symbol(n) == line.symbol).expect("crypto instrument");
            max_diff = max_diff.max((line.target_notional.to_f64() / plan.capital_base.to_f64() - tw[j]).abs());
            crypto_weights_compared += 1;
        }
    }
    println!("REPLAY mixed_etf_delay0: crypto target weights compared {crypto_weights_compared}, max diff {max_diff:.3e}");
    assert!(crypto_weights_compared > 300);
    assert!(max_diff <= WEIGHT_TOL, "the crypto sleeve is unaffected by the ETF sleeve's delay ({max_diff:e})");
    let mut shifts = 0;
    for l in &logs {
        let Some(d) = l.rec.decisions.iter().find(|d| d.sleeve == "etf" && d.planned) else { continue };
        let f = l.day.pred_opt().unwrap();
        let own_prev = w.etf_days[w.etf_days.partition_point(|x| *x < f) - 1];
        assert_eq!(d.decision_date, own_prev, "{}: the acted decision is the previous own bar's (month-end) decision", l.day);
        let Some(&kl) = by_bar.get(&own_prev) else { continue };
        let changed = |k: usize| (0..5).any(|j| (res.inst_row(&res.units, k)[j] - if k == 0 { 0.0 } else { res.inst_row(&res.units, k - 1)[j] }).abs() > 1e-12);
        if l.fills.iter().any(|(s, _, _)| World::is_etf(s)) {
            assert!(changed(kl), "{}: delay 0 rebalanced the ETF sleeve at the decision bar {own_prev}", l.day);
            shifts += 1;
        }
    }
    assert!(shifts >= 10, "{shifts}");
    let rep = compare(&logs, &res, Kind::Mixed, true);
    print("mixed_etf_delay0", &rep);
    assert!(rep.first_mismatch.is_some(), "a wrong ETF delay must NOT reproduce the pipeline");
}

// -------------------------------------------------------------------------------------------------------------
// What the FIRST-ORDER stand-ins cost: weightsim's own construction (no lot rounding, cash reserve 0, fee = cost)
// -------------------------------------------------------------------------------------------------------------

/// Ledger `LOT_ROUNDING_AND_FEE_RESERVE_STAND_INS`: weightsim's native `Budget` construction (fractional units, no cash
/// reserve, buys sized with the account's own fee of 0) against the pipeline (whole shares / 8 dp floored, 5% cash
/// reserve, buys sized with the 0.26% estimated fee). The equity paths differ; the measured size is pinned so that a
/// change of either side is noticed.
#[test]
fn t4_native_minimal_construct_versus_pipeline_measures_the_stand_in_gap_pinned() {
    let w = world();
    let (_, to, start) = window(&w);
    let logs = se_replay(&w, Kind::EtfOnly, "1000000000");
    let res = ws_run(&w, &WsSpec { kind: Kind::EtfOnly, etf: EtfTiming::Native(1), live_faithful: false, allocated: None, start_bar: start, end_bar: to.pred_opt().unwrap() });
    let rep = compare(&logs, &res, Kind::EtfOnly, false);
    print("etf_only_native_minimal_construct", &rep);
    assert!(rep.max_rel_equity > 1e-6, "the stand-ins are NOT the live arithmetic (equity differs by {:e})", rep.max_rel_equity);
    assert!(rep.max_rel_equity < 0.05, "and the gap is bounded (cash reserve 5% + fee estimate + whole shares): {:e}", rep.max_rel_equity);
}

/// Ledger `FILLS_SAME_CLOSE_VS_VENUE`: the backtester fills at the close the decision was sized on; a venue fills at the
/// price of the moment. The fake broker's adverse fill offset makes the equity path diverge from the same-close
/// backtest by (offset x traded notional): pinned, so that "same-close" is never mistaken for a live guarantee.
#[test]
fn t4_fill_price_offset_moves_equity_off_the_same_close_backtest_pinned() {
    let w = world();
    let (from, to, _) = window(&w);
    let acct = account(sleeves_of(Kind::CryptoOnly), ExecutionMode::Live, replay_mandate("1000000000", RESERVE));
    let rig = Rig::new(&w, acct, CASH);
    rig.broker.set_slippage_bps(10); // 10 bps adverse on every fill
    let logs = rig.replay(from, to);
    let res = ws_run(&w, &WsSpec { kind: Kind::CryptoOnly, etf: EtfTiming::Native(0), live_faithful: true, allocated: None, start_bar: from.pred_opt().unwrap(), end_bar: to.pred_opt().unwrap() });
    let rep = compare(&logs, &res, Kind::CryptoOnly, false);
    print("crypto_only_slippage_10bps", &rep);
    assert!(rep.first_mismatch.is_some(), "a venue fill price other than the sized close diverges from the same-close backtest");
    // the loss versus the frictionless backtest is bounded by 10 bps of everything traded
    let last = logs.last().unwrap().equity_after.to_f64();
    let base = res.equity[res.n_bars() - 1];
    assert!(last < base, "adverse fills lose money versus the same-close backtest ({last} vs {base})");
    assert!((base - last) / base < 0.05);
}

/// Sanity: the day loop covers every calendar day of the window (no run skipped).
#[test]
fn t4_every_calendar_day_of_the_window_is_a_run() {
    let w = world();
    let (from, to, _) = window(&w);
    let logs = se_replay(&w, Kind::EtfOnly, "1000000000");
    assert_eq!(logs.len(), all_days(from, to).len());
    let noop = logs.iter().filter(|l| l.rec.outcome.code == "RUN_NOTHING_PENDING").count();
    assert!(noop as f64 > 0.9 * logs.len() as f64, "an ETF-only account is a no-op on almost every day ({noop} of {})", logs.len());
}

/// Ledger `F64_FULL_EXIT_ABOVE_HOLDING`: `portfolio_construct`'s lot rounding snaps a value at most 4 ulps below a quantum
/// UP onto it (so that an exact decimal tie behaves as the planner's). Applied to the SELL of a full exit of
/// float-accumulated units it sizes the order ONE ULP ABOVE the holding: the trade would leave a -9e-16 "short". A
/// long-only caller that applies it unclamped never touches that instrument again (`ShortPositionHeld`). The live
/// planner cannot do this (exact decimals, `min(wish, held)`). Found by this replay; the adapter clamps; the Core crate
/// should clamp reductions to the holding after rounding.
#[test]
fn t4_f64_full_exit_can_be_sized_above_the_holding_pinned() {
    use portfolio_construct as pc;
    let held = 4.949445489999999_f64; // 4.94944549 accumulated in f64: one ulp below the 8-decimal quantum
    let rounder = pc::LotRounder::new().with("BTC/USD", pc::LotRule::new(8));
    let sleeves = vec![pc::SleeveTargets::long_only("s0", 1.0, vec![(0, 0.0)])];
    let facts = vec![pc::InstrumentFacts::new("BTC/USD", "alpaca", "crypto_spot", 10_000.0).with_held(held)];
    let limits = pc::Limits::long_only_unit().with_policy(pc::LimitPolicy::PlannerFaithful);
    let out = pc::construct(&pc::ConstructInputs {
        equity: 100_000.0,
        allocated_capital: None,
        sleeves: &sleeves,
        risk_scale: pc::RiskScale::ONE,
        limits: &limits,
        instruments: &facts,
        margin: &pc::NoMargin,
        trade_filter: pc::TradeFilter::NONE,
        rounding: Some(&rounder),
        funding: pc::Funding::Unconstrained,
        target_dp: Some(8),
        unmanaged_gross: 0.0,
    })
    .expect("construct");
    let sell = &out.trades[0];
    assert_eq!(sell.side, pc::Side::Sell);
    assert!(sell.quantity > held, "the sell is sized ABOVE the holding: {} > {held}", sell.quantity);
    assert!(sell.quantity - held < 1e-12, "by float noise only ({:e})", sell.quantity - held);
    // the exact-decimal planner: a sell never exceeds the held quantity
    // (asserted in rebalancer-core's own planner tests: `full exit sells the whole held quantity`)
}
