//! PARITY TEST 3 (design 5.4 #3): cadence, over 30 years of dates.
//!
//! The driver's run days and the pipeline's pending sleeves (post-#37: EVERY sleeve is evaluated on every run and only
//! PENDING sleeves are planned) against the backtester's cadence: `weightsim`'s `simulate_book` flags in both cadence
//! modes and `portfolio_construct::schedule` (`due`, `plan_flags`).
//!
//! # The time mapping (the whole comparison rests on it)
//! The driver runs at 00:10Z of calendar day `D` and sees only bars dated strictly before `D`. So the run of day `D`
//! corresponds to the account-clock bar dated `D - 1`. The crypto rule decides on bar `D - 1` (fill: same close, delay 0).
//! The ETF rule decides at the last session `L` of a month but, under `MonthEndMode::NextMonthBar`, the decision is
//! computable only once a bar of the next month exists, i.e. at the run after the first session `F` of the new month:
//! day `F + 1`, bar `F`. That is exactly `execution_delay_bars = 1` in the ETF sleeve's OWN bars (decision bar `L`, fill
//! bar `F`), the pre-registered delay of council Ruling 4. A weightsim book has ONE delay for the whole book, so the mixed
//! book is run with the ETF's delay 1; the crypto sleeve is `EveryBar`, whose FLAGS do not depend on the delay (its
//! TARGETS would: that is the KNOWN GAP test 4 measures).
//!
//! # What must agree, and what is allowed to differ
//! With `BookCadence::PerSleeve` the two are EXACTLY equal on every day (the window is aligned so that the pipeline's
//! ENTRY run coincides with the backtest's first effective bar; a mid-month entry is a separate, pinned difference).
//! With `AllSleevesOnAnyDue` (the pre-#37 driver, finding F1) the backtester plans the ETF sleeve on every bar on which
//! its market is open; the pipeline no longer does. Every difference is classified into the documented wall-clock versus
//! data-calendar cases below, and the classification must account for ALL of them.
//!
//! # Cost
//! The ETF sleeve is run for the full 30 years in an ETF-only account (a no-op run costs one evaluation). The mixed
//! ETF+crypto account, in which every run is a full pipeline run because crypto is pending daily, is run for 3 years and
//! must agree with the ETF-only account day by day.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{Datelike, Duration, NaiveDate, Weekday};
use common::adapters::{from_naive, to_naive, CadenceCrypto, CadenceEtf};
use common::replay::{account, crypto_sleeve, etf_sleeve, replay_mandate, slot, Rig, ACCOUNT_ID};
use common::world::{all_days, date, World};
use portfolio_construct::schedule::{due, plan_flags, BookCadence as PcCadence, Cadence, CivilDate};
use rebalancer_run::data::SleeveKind;
use rebalancer_run::driver::{find_due_runs, ActiveAccount, InMemoryAccountSource};
use rebalancer_run::record::{ExecutionMode, OutcomeKind};
use weightsim::{AllocatorSpec, BarTime, Book, BookCadence, BookConfig, BookPanel, OnRefusal, SessionKind, ShareSpec, SleeveSpec};

fn civil(d: NaiveDate) -> CivilDate {
    CivilDate::new(d.year(), d.month() as u8, d.day() as u8).expect("valid date")
}

/// One driver day as the pipeline classified it.
#[derive(Clone, Debug)]
struct SeDay {
    etf_pending: Option<bool>,
    crypto_pending: Option<bool>,
    etf_entry: bool,
    etf_decision: Option<NaiveDate>,
}

/// Tick every day of `from..=to` through the real driver + pipeline (Assisted mode: tickets, nothing placed).
fn drive(world: &World, sleeves: Vec<rebalancer_run::data::SleeveSpec>, from: NaiveDate, to: NaiveDate) -> Vec<(NaiveDate, SeDay)> {
    let acct = account(sleeves, ExecutionMode::Assisted, replay_mandate("1000000000", 0.05));
    let rig = Rig::new(world, acct, "100000");
    all_days(from, to)
        .into_iter()
        .map(|day| {
            let rec = rig.tick(day);
            assert_eq!(rec.outcome.kind, OutcomeKind::Completed, "{day}: every synthetic run completes ({})", rec.outcome.message);
            let etf = rec.decisions.iter().find(|d| d.sleeve == "etf");
            let cry = rec.decisions.iter().find(|d| d.sleeve == "crypto");
            (
                day,
                SeDay {
                    etf_pending: etf.map(|d| d.pending),
                    crypto_pending: cry.map(|d| d.pending),
                    etf_entry: etf.is_some_and(|d| d.entry),
                    etf_decision: etf.map(|d| d.decision_date),
                },
            )
        })
        .collect()
}

/// The backtester's book over the same world: ETF sleeve 0.6 (monthly, on decision), crypto sleeve 0.4 (daily, every
/// bar), the cadence stand-in rules (constant weights: this test is about WHEN, not WHAT). The account starts at bar
/// `start_bar`; the ETF's own delay is 1 (see the module docs).
struct CoreRun {
    dates: Vec<NaiveDate>,
    etf_planned: Vec<bool>,
    crypto_planned: Vec<bool>,
    run: Vec<bool>,
}

fn core_flags(world: &World, mode: BookCadence, start_bar: NaiveDate) -> CoreRun {
    let holidays: Vec<weightsim::Date> = world.cal.holidays.iter().map(|d| from_naive(*d)).collect();
    let mut series = Vec::new();
    for (i, s) in World::etf_symbols().iter().enumerate() {
        series.push((
            s.clone(),
            SessionKind::exchange("US", holidays.clone()),
            world.etf_days.iter().zip(&world.etf[i]).map(|(d, p)| (from_naive(*d), *p)).collect::<Vec<_>>(),
        ));
    }
    for (i, s) in ["BTC", "ETH"].iter().enumerate() {
        series.push(((*s).to_string(), SessionKind::Continuous, world.crypto_days.iter().zip(&world.crypto[i]).map(|(d, p)| (from_naive(*d), *p)).collect::<Vec<_>>()));
    }
    let panel = BookPanel::from_dated_series(series).expect("panel");
    let etf = SleeveSpec::from_rule("etf", CadenceEtf, vec![0, 1, 2, 3, 4], ShareSpec::Fixed(0.6));
    let cry = SleeveSpec::from_rule("crypto", CadenceCrypto, vec![5, 6], ShareSpec::Fixed(0.4));
    let book = Book::new(vec![etf, cry]).with_allocator(AllocatorSpec::Fixed);
    let mut cfg = BookConfig::default();
    cfg.sim.on_refusal = OnRefusal::HoldPrevious;
    cfg.sim.execution_delay_bars = 1;
    cfg.sim.initial_equity = 100_000.0;
    cfg.cadence = mode;
    cfg.account_start = Some(BarTime::from_date(from_naive(start_bar)));
    let res = weightsim::simulate_book(&panel, &book, &cfg).expect("simulate_book");
    let n = res.n_bars();
    CoreRun {
        dates: res.times.iter().map(|t| to_naive(t.date())).collect(),
        etf_planned: (0..n).map(|k| res.planned[k * 2]).collect(),
        crypto_planned: (0..n).map(|k| res.planned[k * 2 + 1]).collect(),
        run: res.run.clone(),
    }
}

/// The same flags from `portfolio_construct::schedule` alone (no simulator): what the cadence SPECIFICATION says. The
/// ETF sleeve decides on its own calendar's last bar of each month AT OR AFTER the account's first bar and becomes
/// effective one own bar later.
fn schedule_flags(world: &World, dates: &[NaiveDate], start_bar: NaiveDate, mode: PcCadence) -> (Vec<bool>, Vec<bool>) {
    let sessions = &world.etf_days;
    let mut effective: BTreeSet<NaiveDate> = BTreeSet::new();
    for t in 0..sessions.len() {
        if sessions[t] < start_bar {
            continue;
        }
        let next = sessions.get(t + 1).map(|d| civil(*d));
        if due(Cadence::LastBarOfMonth, civil(sessions[t]), next) {
            if let Some(f) = sessions.get(t + 1) {
                effective.insert(*f);
            }
        }
    }
    let mut etf = Vec::new();
    let mut cry = Vec::new();
    for d in dates {
        let flags = plan_flags(mode, &[effective.contains(d), due(Cadence::Daily, civil(*d), None)], &[world.cal.is_session(*d), true]);
        etf.push(flags[0]);
        cry.push(flags[1]);
    }
    (etf, cry)
}

/// The first run day of the aligned window: the run after the first session of January 1996, whose decision is the
/// month-end of December 1995 (also the first decision of the backtest account that starts on that month-end).
fn aligned_window(world: &World) -> (NaiveDate, NaiveDate, NaiveDate) {
    let start_bar = world.cal.last_session(1995, 12);
    let from = world.cal.first_session(1996, 1) + Duration::days(1);
    (from, date(2026, 1, 1), start_bar)
}

#[test]
fn thirty_years_of_run_days_and_pending_sleeves_versus_the_backtester_cadence() {
    let world = World::build(date(1994, 1, 1), date(2026, 6, 30));
    let (from, to, start_bar) = aligned_window(&world);
    let cal = &world.cal;
    let n_days = all_days(from, to).len();
    assert!(n_days > 10_900, "thirty years of daily runs: {n_days}");

    // ---- kinds: the cadence of each sleeve kind is a property of the kind, not of the calendar
    assert_eq!(SleeveKind::EtfTrend.cadence(), rebalancer_run::data::Cadence::OnDecision);
    assert_eq!(SleeveKind::CryptoTrend.cadence(), rebalancer_run::data::Cadence::Daily);

    // ---- (1) the pipeline's ETF sleeve over 30 years, against an INDEPENDENT calendar oracle (no reference-rules)
    let se = drive(&world, vec![etf_sleeve("1")], from, to);
    assert_eq!(se.len(), n_days);
    let mut actions: Vec<(NaiveDate, NaiveDate)> = Vec::new(); // (run day, decision date), including the entry
    for (day, s) in &se {
        if s.etf_pending == Some(true) {
            actions.push((*day, s.etf_decision.expect("decision")));
        }
    }
    assert_eq!(se.iter().filter(|(_, s)| s.etf_entry).map(|(d, _)| *d).collect::<Vec<_>>(), vec![from], "exactly one ENTRY, on the first run");
    let mut expected_actions = Vec::new();
    let (mut y, mut m) = (from.year(), from.month());
    loop {
        let f = cal.first_session(y, m);
        let action = f + Duration::days(1);
        if action > to {
            break;
        }
        let (py, pm) = if m == 1 { (y - 1, 12) } else { (y, m - 1) };
        expected_actions.push((action, cal.last_session(py, pm)));
        if m == 12 {
            y += 1;
            m = 1;
        } else {
            m += 1;
        }
    }
    assert_eq!(actions, expected_actions, "the ETF acts exactly once per month, on the run after the first session, on the previous last session's decision");

    // ---- (2) PerSleeve: EXACT equality with weightsim and with the schedule module, every single day
    let per = core_flags(&world, BookCadence::PerSleeve, start_bar);
    let (sched_etf, sched_cry) = schedule_flags(&world, &per.dates, start_bar, PcCadence::PerSleeve);
    assert_eq!(per.etf_planned, sched_etf, "weightsim PerSleeve ETF flags == schedule::plan_flags");
    assert_eq!(per.crypto_planned, sched_cry, "weightsim PerSleeve crypto flags == schedule::plan_flags");
    let by_bar: BTreeMap<NaiveDate, usize> = per.dates.iter().enumerate().map(|(k, d)| (*d, k)).collect();
    let mut per_sleeve_diffs = Vec::new();
    for (day, s) in &se {
        let k = by_bar[&day.pred_opt().unwrap()];
        if s.etf_pending != Some(per.etf_planned[k]) {
            per_sleeve_diffs.push(*day);
        }
    }
    assert!(per_sleeve_diffs.is_empty(), "PerSleeve cadence must equal the pipeline's pending gating on every day; differs on {per_sleeve_diffs:?}");
    println!("CADENCE per_sleeve: {} run days compared, 0 differences, {} ETF actions", n_days, actions.len());

    // ---- (3) AllSleevesOnAnyDue (the pre-#37 driver, F1): classified differences
    let all = core_flags(&world, BookCadence::AllSleevesOnAnyDue, start_bar);
    assert_eq!(per.dates, all.dates);
    let (sched_etf_all, _) = schedule_flags(&world, &all.dates, start_bar, PcCadence::AllSleevesOnAnyDue);
    assert_eq!(all.etf_planned, sched_etf_all, "weightsim AllSleevesOnAnyDue ETF flags == schedule::plan_flags");
    assert!(all.run.iter().all(|r| *r) && all.crypto_planned.iter().all(|c| *c), "crypto is due every bar: a driver run on every bar");
    let (mut f1_extra, mut agree_planned, mut open_in_window) = (0usize, 0usize, 0usize);
    for (day, s) in &se {
        let k = by_bar[&day.pred_opt().unwrap()];
        open_in_window += usize::from(all.etf_planned[k]);
        match (all.etf_planned[k], s.etf_pending == Some(true)) {
            (true, false) => {
                assert!(cal.is_session(day.pred_opt().unwrap()), "{day}: the F1 re-plan needs an open ETF market");
                f1_extra += 1;
            }
            (true, true) => agree_planned += 1,
            (false, true) => panic!("{day}: the pipeline plans the ETF sleeve where the backtester never does"),
            (false, false) => {}
        }
    }
    println!("CADENCE all_on_any_due: ETF planned on {open_in_window} bars; agreeing {agree_planned}; F1 extra re-plans {f1_extra}; the pipeline (only-due) plans it {} times", actions.len());
    assert_eq!(f1_extra + agree_planned, open_in_window);
    assert_eq!(agree_planned, actions.len(), "every pipeline action is a bar the backtester also plans");
    assert!(f1_extra > 6_000, "F1 would re-plan the ETF sleeve about every session ({f1_extra})");

    // ---- (4) the documented wall-clock versus data-calendar cases (counted from the independent calendar)
    // (a) run days versus data days: every kind is due on every date, so an ETF-only account is RUN every calendar day
    let source = InMemoryAccountSource::new();
    source.set_accounts(vec![ActiveAccount { sleeves: vec![etf_sleeve("1")], ..account(vec![], ExecutionMode::Assisted, replay_mandate("1000000000", 0.05)) }]);
    for day in all_days(from, to) {
        let due_runs = find_due_runs(&source, slot(day)).expect("enumeration");
        assert_eq!(due_runs.len(), 1, "an ETF-only account is due on {day}");
        assert_eq!(due_runs[0].sleeves.len(), 1);
        assert_eq!(due_runs[0].account_id, ACCOUNT_ID);
    }
    let data_days = world.etf_days.iter().filter(|d| **d >= from.pred_opt().unwrap() && **d < to).count();
    let non_session_runs = n_days - data_days;
    println!("CADENCE run days {n_days} vs ETF data bars {data_days}: {non_session_runs} run days are weekends/holidays (no-op runs for an ETF-only account)");
    assert!(non_session_runs > 3_000);
    // (b) the wall-clock predicate (`CalendarMonthEnd`, the pre-#37 ETF cadence still shipped in Core's schedule module)
    // against the data calendar (`LastBarOfMonth`): they differ exactly in the months whose last calendar day is not a session
    let cal_month_ends: Vec<NaiveDate> = all_days(from, to).into_iter().filter(|d| due(Cadence::CalendarMonthEnd, civil(*d), None)).collect();
    let month_end_not_session = cal_month_ends.iter().filter(|d| !cal.is_session(**d)).count();
    let mut data_month_ends_disagree = 0usize;
    for t in 0..world.etf_days.len() {
        let d = world.etf_days[t];
        if d < from || d > to {
            continue;
        }
        let data = due(Cadence::LastBarOfMonth, civil(d), world.etf_days.get(t + 1).map(|x| civil(*x)));
        let wall = due(Cadence::CalendarMonthEnd, civil(d), None);
        if data != wall {
            data_month_ends_disagree += 1;
        }
    }
    println!("CADENCE calendar month-ends {}; not a session {month_end_not_session}; sessions on which the two month-end predicates disagree {data_month_ends_disagree}", cal_month_ends.len());
    assert_eq!(data_month_ends_disagree, month_end_not_session, "each weekend/holiday month-end contributes exactly one session on which the predicates disagree");
    assert!(month_end_not_session > 80, "weekend and holiday month-ends over 30 years: {month_end_not_session}");
    // (c) the pipeline's ETF action days NEVER coincide with the wall-clock month-end (the U3 month-late lag is gone), and
    // are always on day 2 or later of the month
    let cme: BTreeSet<NaiveDate> = cal_month_ends.iter().copied().collect();
    assert!(actions.iter().all(|(d, _)| !cme.contains(d) && d.day() >= 2), "no ETF action on a wall-clock month-end");
    // (d) actions that land on a weekend day (the first session was a Friday): the order is queued over the weekend
    let weekend_actions = actions.iter().filter(|(d, _)| matches!(d.weekday(), Weekday::Sat | Weekday::Sun)).count();
    println!("CADENCE ETF actions {} (on a Saturday or Sunday: {weekend_actions})", actions.len());
    assert!(weekend_actions > 20 && weekend_actions < actions.len());
}

#[test]
fn mixed_account_pending_flags_match_the_etf_only_account_and_the_backtester_for_three_years() {
    let world = World::build(date(1994, 1, 1), date(2026, 6, 30));
    let (from, _, start_bar) = aligned_window(&world);
    let to = date(1998, 12, 31);
    let alone = drive(&world, vec![etf_sleeve("1")], from, to);
    let mixed = drive(&world, vec![etf_sleeve("0.6"), crypto_sleeve("0.4")], from, to);
    let per = core_flags(&world, BookCadence::PerSleeve, start_bar);
    let by_bar: BTreeMap<NaiveDate, usize> = per.dates.iter().enumerate().map(|(k, d)| (*d, k)).collect();
    for ((day, a), (_, m)) in alone.iter().zip(&mixed) {
        assert_eq!(m.etf_pending, a.etf_pending, "{day}: a daily crypto sleeve does not change when the ETF sleeve is pending (only-due)");
        assert_eq!(m.etf_decision, a.etf_decision, "{day}");
        assert_eq!(m.crypto_pending, Some(true), "{day}: crypto is pending on every run");
        let k = by_bar[&day.pred_opt().unwrap()];
        assert_eq!(m.etf_pending, Some(per.etf_planned[k]), "{day}: ETF pending == weightsim PerSleeve planned");
        assert_eq!(m.crypto_pending, Some(per.crypto_planned[k]), "{day}: crypto pending == weightsim PerSleeve planned");
    }
    assert!(mixed.iter().filter(|(_, s)| s.etf_pending == Some(true)).count() >= 36);
}

/// Ledger `ENTRY_IN_FORCE_DECISION`: a MID-MONTH account start. The pipeline plans the ETF sleeve at its first run on the
/// decision in force (council Ruling 9(a): the analogue of the reference tool's `--initial`); a backtest account waits
/// for its first month-end. Pinned: the pipeline's first plan is on day 1 (decision = the newest completed month-end),
/// the backtest's first planned bar is the first session of the NEXT month.
#[test]
fn t3_mid_month_entry_plans_on_day_one_while_the_backtest_waits_a_month_pinned() {
    let world = World::build(date(1994, 1, 1), date(2026, 6, 30));
    let from = date(2010, 3, 16);
    let to = date(2010, 5, 20);
    let se = drive(&world, vec![etf_sleeve("1")], from, to);
    let first = &se[0].1;
    assert!(first.etf_entry && first.etf_pending == Some(true), "the entry is planned on the very first run");
    assert_eq!(first.etf_decision, Some(world.cal.last_session(2010, 2)), "on the newest completed month-end");
    let next_action = se.iter().skip(1).find(|(_, s)| s.etf_pending == Some(true)).expect("a later action");
    assert_eq!(next_action.0, world.cal.first_session(2010, 4) + Duration::days(1));
    let per = core_flags(&world, BookCadence::PerSleeve, from.pred_opt().unwrap());
    let first_core = per.dates.iter().zip(&per.etf_planned).find(|(_, p)| **p).map(|(d, _)| *d).expect("planned bar");
    assert_eq!(first_core, world.cal.first_session(2010, 4), "the backtest's first ETF bar is the first session of April");
    assert!(first_core > from, "the backtest waited");
}
