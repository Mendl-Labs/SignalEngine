//! P1-02 in the run pipeline and the monitor: under `DayStartRule::UtcMidnight` the day-start is the first reading
//! at or after 00:00 UTC inside the anchor window. A day without one pauses NEW BUYS (sells still go), alerts once,
//! and is persisted; the monitor's midnight reading is what anchors a run taken later in the day; and the one-day
//! loss limit fires from the midnight value.

mod common;

use broker_adapters::Side;
use chrono::{DateTime, NaiveDate, Utc};
use common::harness::*;
use common::*;
use rebalancer_risk::state::{AccountStatus, DayStartRule, HaltReason};
use rebalancer_risk::store::StateStore;
use rebalancer_run::monitor::{monitor_once, MonitorCode, MonitorContext, MonitorReport};
use rebalancer_run::pipeline::DAY_START_MISSING;
use rebalancer_run::record::{ExecutionMode, OutcomeKind, PlacedOutcome};
use rebalancer_run::stores::InMemoryEquityLog;

const RULE: DayStartRule = DayStartRule::UtcMidnight { max_lag_secs: 1800 };

fn anchored(mut h: Harness) -> Harness {
    h.cfg.day_start = RULE;
    h
}

fn monitor(h: &Harness, log: &InMemoryEquityLog, at_: DateTime<Utc>) -> MonitorReport {
    h.env.clock.set(at_);
    let rules = kraken_rules(&h.env.pairs);
    let bk = book(&rules);
    let broker = h.env.broker();
    let ctx = MonitorContext {
        account_id: ACCOUNT,
        mode: ExecutionMode::Live,
        mandate: h.mandate.as_ref().unwrap(),
        broker: &broker,
        state_store: &h.states,
        runs: &h.runs,
        notifier: &h.notifier,
        kill_flag: &h.kill,
        equity_log: log,
        clock: &h.env.clock,
        venue_rules: &bk,
        config: &h.cfg,
    };
    monitor_once(&ctx)
}

fn day_start_alerts(h: &Harness) -> usize {
    h.notifier.alerts().iter().filter(|a| a.message.starts_with(DAY_START_MISSING)).count()
}

fn jan(dd: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2021, 1, dd).unwrap()
}

#[test]
fn a_run_inside_the_window_anchors_the_day_and_trades_as_before() {
    let h = anchored(Harness::new());
    let r = h.live(0); // 00:10 UTC: 10 minutes after midnight
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{:?}", r.outcome);
    assert!(r.placed.iter().any(|p| p.side == Side::Buy && p.carried()), "{:?}", r.placed);
    let s = h.states.load(ACCOUNT).unwrap().unwrap();
    assert_eq!(s.trading_day(), Some(jan(1)));
    assert_eq!(s.day_start_equity(), Some(d("10000")));
    assert_eq!(day_start_alerts(&h), 0);
}

#[test]
fn a_day_without_a_midnight_reading_pauses_new_buys_alerts_once_and_stays_paused_all_day() {
    let mut h = anchored(Harness::new());
    h.lateness_secs = 3600; // the first reading of 2021-01-01 is at 01:10, outside the 30-minute window
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_DECISION_NOT_ACTED"), "{:?}", r.outcome);
    let buys: Vec<_> = r.placed.iter().filter(|p| p.side == Side::Buy).collect();
    assert!(!buys.is_empty(), "the plan called for buys");
    for b in &buys {
        assert_eq!(b.outcome, PlacedOutcome::NotSent, "{b:?}");
        assert!(b.detail.contains(DAY_START_MISSING), "{}", b.detail);
    }
    assert_eq!(h.env.order_requests(), 0, "no buy reached the exchange");
    assert!(r.decisions.iter().all(|d| !d.acted), "the decision stays pending");
    let s = h.states.load(ACCOUNT).unwrap().unwrap();
    assert!(s.day_start_missing(jan(1)), "the missing day is persisted");
    assert_eq!(s.day_start_equity(), None, "nothing was invented");
    assert_eq!(day_start_alerts(&h), 1);
    let a = h.notifier.alerts().into_iter().find(|a| a.message.starts_with(DAY_START_MISSING)).unwrap();
    assert_eq!((a.code.as_str(), a.severity.as_str()), ("ALERT_RUN_FAILED", "warning"));
    assert!(a.message.contains("01:10:00"), "it names the late first reading: {}", a.message);

    // A later run the same day (a "restart": state read back from the store) is still paused, and does not alert
    // the missing anchor again.
    let r2 = h.live(2);
    assert_eq!(r2.outcome.code, "RUN_DECISION_NOT_ACTED");
    assert!(r2.placed.iter().filter(|p| p.side == Side::Buy).all(|p| p.outcome == PlacedOutcome::NotSent));
    assert_eq!(day_start_alerts(&h), 1);

    // The next UTC day is anchored at 00:10 and the decision is acted on.
    h.lateness_secs = 0;
    h.data.set_panel("crypto", panel_on(jan(2), true, true));
    let r3 = h.live(24);
    assert_eq!(r3.outcome.code, "RUN_COMPLETED", "{:?}", r3.outcome);
    assert!(r3.placed.iter().any(|p| p.side == Side::Buy && p.carried()));
    let s = h.states.load(ACCOUNT).unwrap().unwrap();
    assert_eq!((s.trading_day(), s.day_start_missing(jan(2))), (Some(jan(2)), false));
}

#[test]
fn sells_still_go_while_new_buys_are_paused() {
    let mut h = anchored(Harness::new());
    h.env.hold("ETH", "1"); // ETH trends down below: the rule sells it; BTC trends up: the rule buys it
    h.data.set_panel("crypto", panel(true, false));
    h.lateness_secs = 3600;
    let r = h.live(0);
    let sells: Vec<_> = r.placed.iter().filter(|p| p.side == Side::Sell).collect();
    assert!(!sells.is_empty() && sells.iter().all(|p| p.carried()), "{:?}", r.placed);
    assert_eq!(h.env.bal("ETH"), d("0"));
    assert!(r.placed.iter().filter(|p| p.side == Side::Buy).all(|p| p.detail.contains(DAY_START_MISSING)), "{:?}", r.placed);
    assert_eq!(r.outcome.code, "RUN_DECISION_NOT_ACTED");
}

#[test]
fn assisted_mode_writes_no_buy_ticket_while_buys_are_paused() {
    let mut h = anchored(Harness::new());
    h.lateness_secs = 3600;
    let r = h.run(ExecutionMode::Assisted, 0);
    assert!(r.tickets.iter().all(|t| t.side != Side::Buy), "{:?}", r.tickets);
    assert_eq!(r.outcome.code, "RUN_DECISION_NOT_ACTED", "{:?}", r.outcome);
    assert!(r.outcome.message.contains("not ticketed"), "{}", r.outcome.message);
}

#[test]
fn the_monitors_midnight_reading_anchors_a_run_taken_later_in_the_day() {
    let mut h = anchored(Harness::new());
    let log = InMemoryEquityLog::new();
    let m = monitor(&h, &log, at("2021-01-01T00:03:00Z"));
    assert_eq!(m.code, MonitorCode::Observed, "{}", m.summary());
    assert!(!m.day_start_missing);
    // The run is an hour late; without the monitor its own reading would start the day outside the window.
    h.lateness_secs = 3600;
    let r = h.live(0);
    assert_eq!(r.outcome.code, "RUN_COMPLETED", "{:?}", r.outcome);
    assert_eq!(day_start_alerts(&h), 0);
}

#[test]
fn a_late_first_monitor_reading_alerts_once_and_the_later_run_pauses_buys_without_alerting_again() {
    let h = anchored(Harness::new());
    let log = InMemoryEquityLog::new();
    let m = monitor(&h, &log, at("2021-01-01T02:00:00Z")); // the pod was down across midnight
    assert!(m.day_start_missing, "{}", m.summary());
    assert!(m.message.starts_with(DAY_START_MISSING));
    assert_eq!(day_start_alerts(&h), 1);
    let m2 = monitor(&h, &log, at("2021-01-01T02:05:00Z"));
    assert!(m2.day_start_missing);
    assert_eq!(day_start_alerts(&h), 1, "once per UTC day");
    let r = h.live(5); // 05:10
    assert_eq!(r.outcome.code, "RUN_DECISION_NOT_ACTED", "{:?}", r.outcome);
    assert!(r.placed.iter().filter(|p| p.side == Side::Buy).all(|p| p.detail.contains(DAY_START_MISSING)));
    assert_eq!(day_start_alerts(&h), 1);
}

#[test]
fn the_one_day_loss_limit_fires_on_a_monitor_reading_measured_from_midnight() {
    let h = anchored(Harness::risk_only());
    h.price("BTC/USD", "50000");
    h.env.hold("BTC", "0.1");
    h.env.rig.handle.set_balance(ACCOUNT, "USD", "5000");
    let log = InMemoryEquityLog::new();
    monitor(&h, &log, at("2021-01-02T00:01:00Z"));
    assert_eq!(h.states.load(ACCOUNT).unwrap().unwrap().day_start_equity(), Some(d("10000")));
    h.price("BTC/USD", "46900"); // 9690: 3.1% below the 00:01 value
    let r = monitor(&h, &log, at("2021-01-02T16:00:00Z"));
    assert_eq!((r.code, r.message.as_str()), (MonitorCode::Halted, "HALT_DAILY_LOSS"), "{}", r.summary());
    let s = h.states.load(ACCOUNT).unwrap().unwrap();
    assert_eq!(s.status(), AccountStatus::Halted);
    assert_eq!(s.halt_record().unwrap().reason, HaltReason::DailyLoss);
}

#[test]
fn the_historical_rule_is_the_default_and_a_late_run_is_not_paused_under_it() {
    let mut h = Harness::new();
    assert_eq!(h.cfg.day_start, DayStartRule::FirstObservation);
    h.lateness_secs = 3600;
    let r = h.live(0);
    assert_eq!(r.outcome.code, "RUN_COMPLETED", "{:?}", r.outcome);
    assert_eq!(day_start_alerts(&h), 0);
}
