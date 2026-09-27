//! PARITY TEST 6: the ETF run slot meets a CLOSED market (slices S-3 and S-4 of the paper-pilot plan).
//!
//! The driver runs every calendar day at 15:00Z. The ETF decision becomes computable on the day after the first
//! session of a new month, and that day can be a weekend or a market holiday: then the venue refuses the orders
//! ("market closed"). The whole pipeline (driver, `run_once`, the Live sells-then-buys execution, reconciliation) is
//! replayed over the stateful `SimBroker`, whose market is closed on the synthetic calendar's non-session days, and
//! must (a) NOT count the decision as acted on a closed day, (b) say so (Warning `ALERT_DECISION_NOT_ACTED`, code
//! `RUN_DECISION_NOT_ACTED`), (c) send nothing, (d) act exactly once, on the first open day, and (e) act at once,
//! with no alert, when the action day is a session.
//!
//! The two fixture boundaries are found by the calendar, not by hand: November 2019 (the first session is Fri 11-01, so
//! the action day is Saturday 11-02) and April 2021 (the first session is Thu 04-01, so the action day is Good Friday
//! 04-02, then a weekend). The synthetic calendar is the U3 tests' (general knowledge, not an exchange calendar).

mod common;

use chrono::{Duration, NaiveDate};
use common::replay::{account, etf_sleeve, replay_mandate, Rig, ACCOUNT_ID};
use common::world::{all_days, date, World};
use rebalancer_run::record::{AlertSeverity, ExecutionMode, OutcomeKind};
use rebalancer_run::stores::RunStore;

fn world() -> World {
    World::build(date(2017, 1, 1), date(2021, 12, 31))
}

struct Boundary {
    /// The first day the decision is computable (the day after the first session of the month).
    action_day: NaiveDate,
    /// The first OPEN day on or after `action_day`.
    first_open_day: NaiveDate,
    /// The month-end decision being acted on.
    decision: NaiveDate,
    previous_decision: NaiveDate,
}

fn boundary(world: &World, y: i32, m: u32) -> Boundary {
    let (py, pm) = if m == 1 { (y - 1, 12) } else { (y, m - 1) };
    let (ppy, ppm) = if pm == 1 { (py - 1, 12) } else { (py, pm - 1) };
    let action_day = world.cal.first_session(y, m) + Duration::days(1);
    let mut first_open_day = action_day;
    while !world.cal.is_session(first_open_day) {
        first_open_day += Duration::days(1);
    }
    Boundary { action_day, first_open_day, decision: world.cal.last_session(py, pm), previous_decision: world.cal.last_session(ppy, ppm) }
}

/// Drive an ETF-only LIVE account across the boundary, with the broker's market closed on every non-session day.
fn drive<'w>(world: &'w World, b: &Boundary) -> (Rig<'w>, Vec<(NaiveDate, rebalancer_run::record::RunRecord, usize)>) {
    let acct = account(vec![etf_sleeve("1")], ExecutionMode::Live, replay_mandate("100000", 0.05));
    let rig = Rig::new(world, acct, "100000");
    // Start on a session about three weeks before the boundary so the previous month's decision is acted (the entry).
    let mut start = b.action_day - Duration::days(24);
    while !world.cal.is_session(start) {
        start += Duration::days(1);
    }
    let mut log = Vec::new();
    for day in all_days(start, b.first_open_day + Duration::days(2)) {
        rig.broker.set_market_closed(!world.cal.is_session(day));
        let fills_before = rig.broker.fill_count();
        let rec = rig.tick(day);
        let fills = rig.broker.fill_count() - fills_before;
        log.push((day, rec, fills));
    }
    (rig, log)
}

fn not_acted_alerts(rig: &Rig<'_>) -> Vec<rebalancer_run::record::Alert> {
    rig.notifier.alerts().into_iter().filter(|a| a.code.as_str() == "ALERT_DECISION_NOT_ACTED").collect()
}

fn check_closed_action_day(y: i32, m: u32, why: &str) {
    let w = world();
    let b = boundary(&w, y, m);
    assert!(!w.cal.is_session(b.action_day), "{why}: the fixture is meant to have a CLOSED action day ({})", b.action_day);
    assert!(b.first_open_day > b.action_day);
    let (rig, log) = drive(&w, &b);

    // Before the action day: the previous decision was acted on (the entry), nothing pending, nothing alerted.
    for (day, rec, _) in log.iter().filter(|(d, _, _)| *d < b.action_day) {
        assert_eq!(rec.outcome.kind, OutcomeKind::Completed, "{day}: {:?}", rec.outcome);
        assert!(rec.outcome.code == "RUN_COMPLETED" || rec.outcome.code == "RUN_NOTHING_PENDING", "{day}: {}", rec.outcome.code);
    }

    let closed_days: Vec<NaiveDate> = all_days(b.action_day, b.first_open_day - Duration::days(1));
    assert!(!closed_days.is_empty());
    for day in &closed_days {
        let (_, rec, fills) = log.iter().find(|(d, _, _)| d == day).unwrap();
        assert_eq!((rec.outcome.kind, rec.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_DECISION_NOT_ACTED"), "{why} {day}: {:?}", rec.outcome);
        let dec = rec.decisions.iter().find(|d| d.sleeve == "etf").unwrap();
        assert_eq!(dec.decision_date, b.decision, "{day}");
        assert!(dec.pending && dec.planned && !dec.acted, "{why} {day}: pending, planned, NOT acted on a closed day: {dec:?}");
        assert_eq!(dec.last_acted_decision, Some(b.previous_decision), "{day}: D_acted is still the previous month");
        assert_eq!(*fills, 0, "{why} {day}: nothing was filled on a closed day");
        assert!(!rec.placed.is_empty() && rec.placed.iter().all(|p| p.refused_market_closed()), "{day}: {:?}", rec.placed.iter().map(|p| (&p.symbol, p.outcome, &p.detail)).collect::<Vec<_>>());
    }
    // The alerts: one per closed run, Warning (wait for the session), never Critical for a closed market.
    let alerts = not_acted_alerts(&rig);
    assert_eq!(alerts.len(), closed_days.len(), "{why}: one alert per closed action-day run");
    assert!(alerts.iter().all(|a| a.severity == AlertSeverity::Warning));

    // The first open day acts, once, on the decision that was waiting; the record moves D_acted.
    let (_, rec, fills) = log.iter().find(|(d, _, _)| *d == b.first_open_day).unwrap();
    assert_eq!((rec.outcome.kind, rec.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{why}: {:?}", rec.outcome);
    let dec = rec.decisions.iter().find(|d| d.sleeve == "etf").unwrap();
    assert!(dec.pending && dec.planned && dec.acted && dec.decision_date == b.decision, "{why}: {dec:?}");
    assert!(*fills > 0, "{why}: the orders were filled on the first open day");
    assert_eq!(rig.runs.last_acted_decision(ACCOUNT_ID, "etf").unwrap(), Some(b.decision));
    assert_eq!(not_acted_alerts(&rig).len(), closed_days.len(), "{why}: no alert on the recovery day");

    // ...and only once: the days after it are no-ops.
    for (day, rec, fills) in log.iter().filter(|(d, _, _)| *d > b.first_open_day) {
        assert_eq!(rec.outcome.code, "RUN_NOTHING_PENDING", "{why} {day}");
        assert_eq!(*fills, 0, "{why} {day}");
    }
}

#[test]
fn a_weekend_action_day_is_not_acted_and_the_next_session_acts_once_november_2019() {
    check_closed_action_day(2019, 11, "Nov 2019 (Saturday action day)");
}

#[test]
fn a_holiday_then_weekend_action_day_is_not_acted_and_the_next_session_acts_once_april_2021() {
    let w = world();
    let b = boundary(&w, 2021, 4);
    assert_eq!(b.action_day, date(2021, 4, 2), "Good Friday");
    assert_eq!(b.first_open_day, date(2021, 4, 5), "the Monday after the Easter weekend");
    check_closed_action_day(2021, 4, "Apr 2021 (Good Friday, then the weekend)");
}

#[test]
fn an_open_action_day_acts_at_once_with_no_alert_october_2019() {
    let w = world();
    let b = boundary(&w, 2019, 10);
    assert!(w.cal.is_session(b.action_day), "Wed 2019-10-02 is a session");
    assert_eq!(b.action_day, b.first_open_day);
    let (rig, log) = drive(&w, &b);
    let (_, rec, fills) = log.iter().find(|(d, _, _)| *d == b.action_day).unwrap();
    assert_eq!((rec.outcome.kind, rec.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{:?}", rec.outcome);
    assert!(rec.decisions.iter().find(|d| d.sleeve == "etf").unwrap().acted);
    assert!(*fills > 0);
    assert!(not_acted_alerts(&rig).is_empty(), "an open action day never alerts");
    assert_eq!(rig.runs.last_acted_decision(ACCOUNT_ID, "etf").unwrap(), Some(b.decision));
}
