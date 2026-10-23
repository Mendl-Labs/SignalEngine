//! The continuous equity monitor (P1-01) against the fake exchange: a crash between two decision days fires the
//! ladder with no run and no pending decision; the daily-loss limit fires inside one day; one broker read per call;
//! no order unless a halt rung fired (and then only the flatten, only in Live mode); kill flag, halted accounts, a
//! broker outage, a failing equity log, the paper-only policy; and `monitor_all`'s eligibility, lock, runtime and
//! one-alert-per-failure-streak rules.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use broker_adapters::{BrokerError, CancelOutcome, OrderReport, OrderRequest, PlaceOutcome, Quote};
use chrono::{DateTime, Utc};
use common::harness::*;
use common::*;
use rebalancer_core::policy::MandateStatus;
use rebalancer_risk::state::{AccountState, AccountStatus, HaltReason};
use rebalancer_risk::store::StateStore;
use rebalancer_run::broker::{Broker, SnapshotError};
use rebalancer_run::driver::{AccountLock, AccountRuntime, ActiveAccount};
use rebalancer_run::flatten::FlattenVerdict;
use rebalancer_run::monitor::{monitor_all, monitor_once, monitored_accounts, summarize, FailureStreaks, MonitorCode, MonitorContext, MonitorReport};
use rebalancer_run::pipeline::VenuePolicy;
use rebalancer_run::record::{ExecutionMode, OutcomeKind};
use rebalancer_run::stores::{InMemoryEquityLog, MONITOR_SNAPSHOT_SOURCE};
use rebalancer_run::testkit::InMemoryAccountLock;
use rebalancer_run::view::BrokerSnapshot;

/// A risk-only account: 5000 USD + 0.1 BTC at 50000 = 10000 of equity, nothing managed by a sleeve.
fn risk_account() -> Harness {
    let h = Harness::risk_only();
    h.price("BTC/USD", "50000");
    h.env.hold("BTC", "0.1");
    h.env.rig.handle.set_balance(ACCOUNT, "USD", "5000");
    assert_eq!(h.equity(), d("10000"));
    h
}

fn state_of(h: &Harness) -> AccountState {
    h.states.load(ACCOUNT).unwrap().expect("state was saved")
}

/// Wraps a broker: counts snapshot reads and can be switched "down" (every snapshot fails, nothing else changes).
struct WatchedBroker<'a> {
    inner: &'a dyn Broker,
    down: AtomicBool,
    snapshots: AtomicU32,
}

impl<'a> WatchedBroker<'a> {
    fn new(inner: &'a dyn Broker) -> Self {
        Self { inner, down: AtomicBool::new(false), snapshots: AtomicU32::new(0) }
    }
    fn snapshots(&self) -> u32 {
        self.snapshots.load(Ordering::SeqCst)
    }
}

impl Broker for WatchedBroker<'_> {
    fn venue(&self) -> &'static str {
        self.inner.venue()
    }
    fn environment(&self) -> rebalancer_run::broker::VenueEnvironment {
        self.inner.environment()
    }
    fn snapshot(&self, now: DateTime<Utc>) -> Result<BrokerSnapshot, SnapshotError> {
        self.snapshots.fetch_add(1, Ordering::SeqCst);
        if self.down.load(Ordering::SeqCst) {
            return Err(SnapshotError::Broker(BrokerError::Http(503)));
        }
        self.inner.snapshot(now)
    }
    fn place(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        self.inner.place(req)
    }
    fn get_order(&self, id: &str) -> Result<OrderReport, BrokerError> {
        self.inner.get_order(id)
    }
    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.inner.open_orders()
    }
    fn find_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        self.inner.find_by_tag(tag)
    }
    fn cancel_and_settle(&self, id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        self.inner.cancel_and_settle(id)
    }
    fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.inner.quote(symbol)
    }
}

/// One monitor reading of the harness account at `at`, through `broker`.
fn monitor_with(h: &Harness, broker: &dyn Broker, log: &InMemoryEquityLog, mode: ExecutionMode, at: DateTime<Utc>) -> MonitorReport {
    h.env.clock.set(at);
    let rules = kraken_rules(&h.env.pairs);
    let bk = book(&rules);
    let ctx = MonitorContext {
        account_id: ACCOUNT,
        mode,
        mandate: h.mandate.as_ref().expect("a mandate"),
        broker,
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

fn monitor(h: &Harness, log: &InMemoryEquityLog, mode: ExecutionMode, at: DateTime<Utc>) -> MonitorReport {
    monitor_with(h, &h.env.broker(), log, mode, at)
}

// ---------------------------------------------------------------------------------------------------------------
// The gap the monitor closes: no decision pending, the ladder still sees the equity
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_crash_between_decision_days_fires_the_halt_rung_with_no_run_and_flattens_in_live_mode() {
    let h = risk_account();
    let r0 = h.live(0);
    assert_eq!(r0.outcome.kind, OutcomeKind::Completed, "{:?}", r0.outcome);
    assert_eq!(state_of(&h).hwm(), Some(d("10000")));
    let runs_before = h.runs.records().len();

    // The next day, between two decision days: BTC halves to 30000, equity 8000, a 20% drawdown (the halt rung).
    h.price("BTC/USD", "30000");
    assert_eq!(h.equity(), d("8000"));
    let log = InMemoryEquityLog::new();
    let r = monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T03:00:00Z"));

    assert_eq!(r.code, MonitorCode::Halted, "{}", r.summary());
    assert_eq!(r.message, "HALT_DRAWDOWN_LADDER");
    assert_eq!(r.equity, Some(d("8000")));
    let halt = state_of(&h);
    assert_eq!(halt.status(), AccountStatus::Halted);
    assert_eq!(halt.halt_record().unwrap().reason, HaltReason::DrawdownLadder);
    let f = r.flatten.as_ref().expect("the halt rung flattens in Live mode");
    assert_eq!(f.verdict, FlattenVerdict::Flat, "{}", f.summary());
    assert_eq!(h.env.bal("BTC"), d("0"), "the flatten sold the BTC");
    // An alert reached a person, and it says where the halt came from.
    let alerts = h.notifier.alerts();
    assert!(alerts.iter().any(|a| a.code.as_str() == "ALERT_HALT" && a.severity.as_str() == "critical" && a.message.contains("continuous monitor")), "{alerts:?}");
    assert!(alerts.iter().filter(|a| a.code.as_str() == "ALERT_HALT").all(|a| a.run_key.ends_with("|monitor")), "{alerts:?}");
    // The decision path was not touched: no run record was written by the monitor.
    assert_eq!(h.runs.records().len(), runs_before);
    // The reading is in the append-only equity log.
    let readings = log.readings();
    assert_eq!(readings.len(), 1);
    assert_eq!(readings[0].source, MONITOR_SNAPSHOT_SOURCE);
    assert_eq!(readings[0].equity, d("8000"));
    assert_eq!(readings[0].account_id, ACCOUNT);

    // The next run is refused (halted), and the monitor keeps reading without sending anything.
    let before = h.env.order_requests();
    let refused = h.live(30);
    assert_eq!(refused.outcome.code, "RUN_ACCOUNT_HALTED");
    let again = monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T07:00:00Z"));
    assert_eq!(again.code, MonitorCode::AccountHalted, "{}", again.summary());
    assert_eq!(h.env.order_requests(), before, "a halted account gets no order from the monitor");
    assert_eq!(log.readings().len(), 2, "a halted account is still read and logged");
}

#[test]
fn the_daily_loss_limit_fires_inside_one_day_on_monitor_readings() {
    // With one run a day the daily-loss limit compared a reading to itself. The monitor's first reading after
    // 00:00 UTC becomes the day's start, and a later reading the same day is measured against it.
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    let first = monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T00:02:00Z"));
    assert_eq!(first.code, MonitorCode::Observed, "{}", first.summary());
    let s = state_of(&h);
    assert_eq!(s.day_start_equity(), Some(d("10000")));
    assert_eq!(s.trading_day(), Some(chrono::NaiveDate::from_ymd_opt(2021, 1, 2).unwrap()));

    // 2.99%: no halt.
    h.price("BTC/USD", "47000.1");
    let near = monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T09:00:00Z"));
    assert_eq!(near.code, MonitorCode::Observed);
    assert_eq!(state_of(&h).status(), AccountStatus::Active);
    // 3.1% (equity 9690): the daily-loss limit (3%) halts and flattens.
    h.price("BTC/USD", "46900");
    let r = monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T14:00:00Z"));
    assert_eq!((r.code, r.message.as_str()), (MonitorCode::Halted, "HALT_DAILY_LOSS"), "{}", r.summary());
    assert_eq!(state_of(&h).halt_record().unwrap().reason, HaltReason::DailyLoss);
    assert_eq!(h.env.bal("BTC"), d("0"));
    // The day-start was not moved by the later readings of the same day.
    assert_eq!(state_of(&h).day_start_equity(), Some(d("10000")));
}

#[test]
fn a_quiet_reading_reads_the_broker_once_and_sends_nothing() {
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    let inner = h.env.broker();
    let watched = WatchedBroker::new(&inner);
    let before = h.env.order_requests();
    for (i, t) in ["2021-01-02T00:05:00Z", "2021-01-02T00:10:00Z", "2021-01-02T00:15:00Z"].iter().enumerate() {
        let r = monitor_with(&h, &watched, &log, ExecutionMode::Live, at(t));
        assert_eq!(r.code, MonitorCode::Observed, "{}", r.summary());
        assert_eq!(r.reads, 1);
        assert_eq!(watched.snapshots(), i as u32 + 1, "exactly one broker read per reading");
        assert!(r.flatten.is_none());
        assert!(r.alerts.is_empty(), "{:?}", r.alerts);
    }
    assert_eq!(h.env.order_requests(), before, "no order of any kind");
    assert_eq!(log.readings().len(), 3);
    assert_eq!(state_of(&h).status(), AccountStatus::Active);
}

#[test]
fn a_shrink_rung_only_changes_the_posture_and_sends_nothing() {
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T00:05:00Z"));
    // Equity 9000: a 10% drawdown, the shrink rung (scale 0.5). The daily limit is a day-start comparison: lift the
    // day-start out of the way by moving to the next day first.
    h.price("BTC/USD", "40000");
    let before = h.env.order_requests();
    let r = monitor(&h, &log, ExecutionMode::Live, at("2021-01-03T00:05:00Z"));
    assert_eq!(r.code, MonitorCode::Observed, "{}", r.summary());
    assert!(r.decision.as_ref().unwrap().codes().contains(&"RISK_DRAWDOWN_SHRINK"), "{:?}", r.decision);
    let s = state_of(&h);
    assert_eq!(s.status(), AccountStatus::Shrunk);
    assert_eq!(s.risk_scale(), d("0.5"));
    assert_eq!(h.env.order_requests(), before, "the next run applies the shrink to its targets; the monitor sells nothing");
    assert!(r.flatten.is_none());
}

#[test]
fn a_halt_in_assisted_or_paper_mode_sends_nothing_and_tells_a_person_to_flatten_by_hand() {
    for mode in [ExecutionMode::Assisted, ExecutionMode::Paper] {
        let h = risk_account();
        let log = InMemoryEquityLog::new();
        monitor(&h, &log, mode, at("2021-01-02T00:05:00Z"));
        h.price("BTC/USD", "30000");
        let before = h.env.order_requests();
        let r = monitor(&h, &log, mode, at("2021-01-03T00:05:00Z"));
        assert_eq!(r.code, MonitorCode::Halted, "{mode:?}: {}", r.summary());
        assert!(r.flatten.is_none());
        assert_eq!(h.env.order_requests(), before, "{mode:?}: nothing may be sent");
        assert_eq!(h.env.bal("BTC"), d("0.1"), "{mode:?}: nothing was sold");
        assert_eq!(state_of(&h).status(), AccountStatus::Halted);
        let codes = h.notifier.codes();
        assert!(codes.contains(&"ALERT_HALT") && codes.contains(&"ALERT_FLATTEN_INCOMPLETE"), "{mode:?}: {codes:?}");
    }
}

#[test]
fn a_flattening_account_has_its_flatten_retried_in_live_mode() {
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T00:05:00Z"));
    // A halt whose flatten never finished (a crash after the state was saved).
    let s = state_of(&h);
    let (flattening, _) = s.begin_flatten(HaltReason::DrawdownLadder, "a crashed flatten", at("2021-01-02T00:06:00Z"));
    h.states.save(s.version(), &flattening).unwrap();
    let r = monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T00:10:00Z"));
    assert_eq!(r.code, MonitorCode::FlattenResumed, "{}", r.summary());
    assert_eq!(state_of(&h).status(), AccountStatus::Halted);
    assert_eq!(h.env.bal("BTC"), d("0"));
    // Outside Live mode a flattening account is only read.
    let h2 = risk_account();
    monitor(&h2, &log, ExecutionMode::Paper, at("2021-01-02T00:05:00Z"));
    let s2 = state_of(&h2);
    let (f2, _) = s2.begin_flatten(HaltReason::DrawdownLadder, "a crashed flatten", at("2021-01-02T00:06:00Z"));
    h2.states.save(s2.version(), &f2).unwrap();
    let r2 = monitor(&h2, &log, ExecutionMode::Paper, at("2021-01-02T00:10:00Z"));
    assert_eq!(r2.code, MonitorCode::AccountHalted);
    assert_eq!(h2.env.bal("BTC"), d("0.1"));
}

// ---------------------------------------------------------------------------------------------------------------
// Fail closed: nothing invented, nothing sent
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_kill_flag_stops_the_monitor_before_anything_is_read() {
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    let inner = h.env.broker();
    let watched = WatchedBroker::new(&inner);
    h.kill.set(true);
    h.price("BTC/USD", "30000");
    let r = monitor_with(&h, &watched, &log, ExecutionMode::Live, at("2021-01-02T03:00:00Z"));
    assert_eq!(r.code, MonitorCode::KillFlagSet);
    assert_eq!((r.reads, watched.snapshots()), (0, 0));
    assert!(log.readings().is_empty());
    assert!(h.states.load(ACCOUNT).unwrap().is_none(), "nothing saved");
    h.kill.set(false);
    h.kill.set_unreadable(true);
    let r = monitor_with(&h, &watched, &log, ExecutionMode::Live, at("2021-01-02T03:05:00Z"));
    assert_eq!(r.code, MonitorCode::KillFlagUnreadable);
    assert!(r.code.is_failure());
    assert_eq!(watched.snapshots(), 0);
}

#[test]
fn a_broker_outage_saves_nothing_and_invents_no_value() {
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T00:05:00Z"));
    let before = state_of(&h);
    let inner = h.env.broker();
    let watched = WatchedBroker::new(&inner);
    watched.down.store(true, Ordering::SeqCst);
    let r = monitor_with(&h, &watched, &log, ExecutionMode::Live, at("2021-01-02T00:10:00Z"));
    assert_eq!(r.code, MonitorCode::BrokerUnreachable, "{}", r.summary());
    assert!(r.equity.is_none() && r.decision.is_none());
    assert_eq!(state_of(&h), before, "the state is exactly as it was: no reading was invented");
    assert_eq!(log.readings().len(), 1, "only the earlier, real reading is logged");
}

#[test]
fn a_failing_equity_log_never_stops_the_risk_step() {
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T00:05:00Z"));
    log.set_failing(true);
    h.price("BTC/USD", "30000");
    let r = monitor(&h, &log, ExecutionMode::Live, at("2021-01-03T00:05:00Z"));
    assert!(r.log_error.is_some());
    assert_eq!(r.code, MonitorCode::Halted, "{}", r.summary());
    assert_eq!(state_of(&h).status(), AccountStatus::Halted);
}

#[test]
fn the_paper_only_policy_refuses_a_non_paper_connection_before_reading_it() {
    let mut h = risk_account();
    h.cfg.venue_policy = VenuePolicy::PaperOnly;
    let log = InMemoryEquityLog::new();
    let inner = h.env.broker(); // the fake Kraken connection reports Live
    let watched = WatchedBroker::new(&inner);
    let r = monitor_with(&h, &watched, &log, ExecutionMode::Live, at("2021-01-02T00:05:00Z"));
    assert_eq!(r.code, MonitorCode::VenueNotPaper, "{}", r.summary());
    assert_eq!(watched.snapshots(), 0);
    assert!(log.readings().is_empty());
}

#[test]
fn an_invalid_mandate_is_refused_before_reading() {
    let h = risk_account().with_mandate(|v| v["loss"]["daily_loss_limit"] = serde_json::json!(0.0));
    let log = InMemoryEquityLog::new();
    let inner = h.env.broker();
    let watched = WatchedBroker::new(&inner);
    let r = monitor_with(&h, &watched, &log, ExecutionMode::Live, at("2021-01-02T00:05:00Z"));
    assert_eq!(r.code, MonitorCode::MandateInvalid, "{}", r.summary());
    assert_eq!(watched.snapshots(), 0);
}

#[test]
fn an_unreadable_state_store_is_refused_before_reading() {
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    let inner = h.env.broker();
    let watched = WatchedBroker::new(&inner);
    h.states.fail_next_calls(1);
    let r = monitor_with(&h, &watched, &log, ExecutionMode::Live, at("2021-01-02T00:05:00Z"));
    assert_eq!(r.code, MonitorCode::StateStoreError, "{}", r.summary());
    assert_eq!(watched.snapshots(), 0);
}

// ---------------------------------------------------------------------------------------------------------------
// The monitor and the run share one state: the run keeps the monitor's day-start
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_run_later_the_same_day_keeps_the_monitors_day_start() {
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    // run_day() is 2021-01-01; the harness runs are at 00:10 + n hours on that day.
    monitor(&h, &log, ExecutionMode::Live, at("2021-01-01T00:02:00Z"));
    assert_eq!(state_of(&h).day_start_equity(), Some(d("10000")));
    h.price("BTC/USD", "46900"); // equity 9690: 3.1% below the monitor's day-start
    let r = h.live(5);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_DAILY_LOSS"), "{:?}", r.outcome);
}

// ---------------------------------------------------------------------------------------------------------------
// monitor_all
// ---------------------------------------------------------------------------------------------------------------

fn active(h: &Harness, id: &str, status: MandateStatus, approved: bool) -> ActiveAccount {
    let mut envelope = active_envelope();
    envelope.status = status;
    ActiveAccount {
        account_id: id.to_string(),
        tenant_id: "11111111-1111-1111-1111-111111111111".to_string(),
        mandate: h.mandate.clone().unwrap(),
        envelope,
        plan_approved: approved,
        sleeves: Vec::new(),
        mode: ExecutionMode::Live,
    }
}

#[test]
fn only_active_approved_accounts_are_monitored() {
    let h = risk_account();
    let accounts = vec![
        active(&h, "a-active", MandateStatus::Active, true),
        active(&h, "b-unapproved", MandateStatus::Active, false),
        active(&h, "c-draft", MandateStatus::Draft, true),
    ];
    let ids: Vec<&str> = monitored_accounts(&accounts).iter().map(|a| a.account_id.as_str()).collect();
    assert_eq!(ids, ["a-active"]);
}

#[test]
fn monitor_all_reads_every_protected_account_and_alerts_once_per_failure_streak() {
    let h = risk_account();
    let log = InMemoryEquityLog::new();
    let lock = InMemoryAccountLock::new();
    let broker = h.env.broker();
    let rules = kraken_rules(&h.env.pairs);
    let bk = book(&rules);
    let mut runtimes: BTreeMap<String, AccountRuntime<'_>> = BTreeMap::new();
    runtimes.insert(ACCOUNT.to_string(), AccountRuntime { broker: &broker, data: &h.data, venue_rules: &bk });
    let accounts = vec![active(&h, ACCOUNT, MandateStatus::Active, true), active(&h, "no-runtime", MandateStatus::Active, true)];
    let mut streaks = FailureStreaks::new();
    let pass = |streaks: &mut FailureStreaks, t: &str| {
        h.env.clock.set(at(t));
        monitor_all(&accounts, &runtimes, &h.states, &h.runs, &h.notifier, &h.kill, &log, &h.env.clock, &lock, &h.cfg, streaks)
    };

    let first = pass(&mut streaks, "2021-01-02T00:05:00Z");
    let codes: Vec<(&str, MonitorCode)> = first.iter().map(|r| (r.account_id.as_str(), r.code)).collect();
    assert_eq!(codes, [(ACCOUNT, MonitorCode::Observed), ("no-runtime", MonitorCode::NoRuntime)]);
    assert!(summarize(&first).contains("MONITOR_NO_RUNTIME=1"), "{}", summarize(&first));
    let failed: Vec<_> = h.notifier.alerts().into_iter().filter(|a| a.message.starts_with("MONITOR_NO_RUNTIME")).collect();
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].code.as_str(), "ALERT_RUN_FAILED");
    assert_eq!(failed[0].severity.as_str(), "warning");

    // The same failure on the next passes does not alert again.
    pass(&mut streaks, "2021-01-02T00:10:00Z");
    pass(&mut streaks, "2021-01-02T00:15:00Z");
    assert_eq!(h.notifier.alerts().iter().filter(|a| a.message.starts_with("MONITOR_NO_RUNTIME")).count(), 1);
    assert!(streaks.is_failing("no-runtime"));
    assert!(!streaks.is_failing(ACCOUNT));
    assert_eq!(log.readings().len(), 3, "one reading per pass for the account with a runtime");

    // An account whose lock is held (a run in progress) is skipped, not read and not alerted.
    let guard = lock.try_lock(ACCOUNT).unwrap().expect("free");
    let busy = pass(&mut streaks, "2021-01-02T00:20:00Z");
    assert_eq!(busy[0].code, MonitorCode::LockBusy);
    assert!(!busy[0].code.is_failure());
    drop(guard);
    assert_eq!(log.readings().len(), 3);
    assert!(!lock.is_held(ACCOUNT), "the monitor releases the lock after each reading");
}

#[test]
fn a_failure_streak_ends_on_a_good_reading_and_a_new_streak_alerts_again() {
    let mut streaks = FailureStreaks::new();
    let report = |code: MonitorCode| {
        let h = risk_account();
        let log = InMemoryEquityLog::new();
        let mut r = monitor(&h, &log, ExecutionMode::Live, at("2021-01-02T00:05:00Z"));
        r.code = code;
        r
    };
    assert!(streaks.observe(&report(MonitorCode::BrokerUnreachable)));
    assert!(!streaks.observe(&report(MonitorCode::BrokerUnreachable)));
    assert!(streaks.observe(&report(MonitorCode::StateStoreError)), "a different failure is a new streak");
    assert!(!streaks.observe(&report(MonitorCode::LockBusy)), "a busy lock neither alerts nor ends the streak");
    assert!(streaks.is_failing(ACCOUNT));
    assert!(!streaks.observe(&report(MonitorCode::Observed)));
    assert!(!streaks.is_failing(ACCOUNT));
    assert!(streaks.observe(&report(MonitorCode::BrokerUnreachable)));
}

// ---------------------------------------------------------------------------------------------------------------
// Pins
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_monitor_codes_are_stable() {
    let codes: Vec<&str> = MonitorCode::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(
        codes,
        [
            "MONITOR_OBSERVED",
            "MONITOR_HALTED",
            "MONITOR_FLATTEN_RESUMED",
            "MONITOR_ACCOUNT_HALTED",
            "MONITOR_KILL_FLAG_SET",
            "MONITOR_KILL_FLAG_UNREADABLE",
            "MONITOR_VENUE_NOT_PAPER",
            "MONITOR_MANDATE_INVALID",
            "MONITOR_BROKER_UNREACHABLE",
            "MONITOR_STATE_STORE_ERROR",
            "MONITOR_RUN_STORE_UNAVAILABLE",
            "MONITOR_LOCK_BUSY",
            "MONITOR_LOCK_UNAVAILABLE",
            "MONITOR_NO_RUNTIME",
            "MONITOR_PANICKED",
        ]
    );
    assert_eq!(codes.iter().collect::<std::collections::BTreeSet<_>>().len(), codes.len());
    let not_failures = [MonitorCode::Observed, MonitorCode::Halted, MonitorCode::FlattenResumed, MonitorCode::AccountHalted, MonitorCode::KillFlagSet, MonitorCode::LockBusy];
    for c in MonitorCode::ALL {
        assert_eq!(c.is_failure(), !not_failures.contains(&c), "{c}");
    }
}

#[test]
fn the_monitor_source_never_builds_or_places_an_order_itself() {
    // The only orders a monitor reading can cause are the flatten's (crate::flatten), after a halt rung fired.
    let src = include_str!("../src/monitor.rs");
    for forbidden in ["OrderRequest", ".place(", "cancel_and_settle", "run_once(", "journal_order", "RunStore::finish", ".finish("] {
        assert!(!src.contains(forbidden), "monitor.rs must not contain {forbidden:?}");
    }
    assert!(src.contains("flatten(ctx.broker"), "the flatten is the one order path");
}
