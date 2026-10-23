//! The continuous equity monitor (P1-01, IMPLEMENTATION_PLAN_V2 Phase 1; PLATFORM_ENFORCEMENT_AUDIT gap G8).
//!
//! # Why it exists
//! `run_once` reads the broker only when a sleeve has a pending decision (`pipeline::step_decisions` ends a run as
//! `RUN_NOTHING_PENDING` before the broker is read). For an ETF plan that is about once a month, so the drawdown
//! ladder and the daily-loss limit never saw a crash between two decision days, and with one reading a day the
//! daily-loss limit compared a reading to itself. The monitor reads the equity of every protected account on its
//! own schedule (the service's `REBALANCER_MONITOR_SECS`, default every 5 minutes, around the clock), whether or not
//! a decision is pending, and runs the SAME risk step the pipeline runs (`rebalancer_risk::overlay::step`) on every
//! reading. The decision path is unchanged: the monitor never touches `run_once`, its run records or its run keys.
//!
//! # What one call ([`monitor_once`]) does, in order
//! 1. `venue_policy`: under `VenuePolicy::PaperOnly` a broker that is not a verified paper connection is refused
//!    before anything is read (`MONITOR_VENUE_NOT_PAPER`).
//! 2. `kill_flag`: set means skip (`MONITOR_KILL_FLAG_SET`, nothing read, nothing sent: the kill flag stops the
//!    whole platform, the monitor included); unreadable means fail closed (`MONITOR_KILL_FLAG_UNREADABLE`).
//! 3. `risk_policy`: the ladder and daily-loss limit are compiled from the mandate exactly as the pipeline compiles
//!    them; an invalid mandate is `MONITOR_MANDATE_INVALID` (nothing read).
//! 4. `state`: the account state is loaded (an unreadable store is `MONITOR_STATE_STORE_ERROR`, nothing read).
//! 5. `read`: ONE broker read. Unreachable is `MONITOR_BROKER_UNREACHABLE`: nothing is saved and no value is
//!    invented; the next reading tries again.
//! 6. `log`: the reading is appended to the equity log (`rebalancer_equity_snapshots`, source `monitor`,
//!    append-only). A failed append is reported and never stops the risk step.
//! 7. A `Halted` account stops here (`MONITOR_ACCOUNT_HALTED`: read and logged, nothing else; only a human resume
//!    leaves a halt). A `Flattening` account in Live mode has its flatten retried (`MONITOR_FLATTEN_RESUMED`), as
//!    the pipeline's own `halted_check` step does; in Assisted / Paper mode it stops here.
//! 8. `risk`: `observe` + `evaluate_risk` + `apply_decision` on the BROKER equity, with the UTC date of the reading
//!    as the trading day (the pipeline's `trading_day` is the UTC date of its slot, so both agree). The new state is
//!    saved (compare-and-swap); a shrink rung only changes the posture, which the next run applies to its targets.
//!    The day-start follows `RunConfig::day_start` (the same rule the runs use). Under `DayStartRule::UtcMidnight`
//!    the monitor's first reading after 00:00 UTC is the day's start; if that first reading came after the anchor
//!    window the day has no start, the reading that found it raises one `ALERT_RUN_FAILED` (Warning,
//!    `DAY_START_MISSING`, once per UTC day) and the runs pause new buys for the rest of that day.
//! 9. A halt rung or the daily-loss limit (`RiskAction::HaltFlatten`) alerts (`ALERT_HALT`, Critical) and, in Live
//!    mode, flattens with the pipeline's own `flatten` (same tags, keyed by the halt time, so a run or a later
//!    reading that resumes it never sends an order twice); in Assisted / Paper mode the account goes straight to
//!    `Halted` and a person is told to flatten by hand (`ALERT_FLATTEN_INCOMPLETE`), exactly as in the pipeline.
//!
//! # The one rule about orders
//! The monitor places NO order of its own. The only orders it can cause are the flatten's reducing orders after a
//! halt rung or the daily-loss limit fired (the ladder's own risk-reducing action), and only in Live mode. A source
//! scan in `tests/monitor.rs` pins that this module never builds or places an order itself.
//!
//! # Many accounts ([`monitor_all`])
//! Every account the account source enumerates with an ACTIVE mandate and an approved plan (the same eligibility as
//! `driver::find_due_runs`, without the time slot) and a registered runtime is monitored, each under the same
//! per-account lock the runs take, a panic in one never stopping the others. A failure (broker unreachable, store
//! down, ...) is alerted ONCE per failure streak (`ALERT_RUN_FAILED`, Warning, message `MONITOR_<CODE>`) by
//! [`FailureStreaks`], so a five-minute cadence cannot flood the append-only alert table.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{self, AssertUnwindSafe};

use chrono::{DateTime, Utc};
use mandate_core::mandate::MandateBody;
use rebalancer_core::policy::{MandateStatus, Policy};
use rebalancer_core::venue::VenueRuleBook;
use rebalancer_core::Dec;
use rebalancer_risk::overlay::{step_with, RiskAction, RiskDecision, RiskPolicy};
use rebalancer_risk::state::{AccountState, AccountStatus, DayStartRule, HaltReason, Transition};
use rebalancer_risk::store::{StateStore, StoreError};

use crate::broker::{Broker, VenueEnvironment};
use crate::clock::Clock;
use crate::driver::{AccountLock, AccountRuntime, ActiveAccount};
use crate::flatten::{flatten, FlattenReport, FlattenSpec, FlattenVerdict};
use crate::pipeline::{day_start_missing_message, RunConfig, VenuePolicy};
use crate::record::{Alert, AlertCode, AlertSeverity, ExecutionMode};
use crate::stores::{EquityLog, EquityReading, KillFlag, Notifier, RunStore, MONITOR_SNAPSHOT_SOURCE};
use crate::view::BrokerSnapshot;

/// Stable machine codes of one monitor reading. Logs and alerts key on them; never rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MonitorCode {
    /// The equity was read, logged and the risk step evaluated; no halt.
    Observed,
    /// This reading tripped a halt rung or the daily-loss limit.
    Halted,
    /// The account was already flattening and its flatten was retried (Live).
    FlattenResumed,
    /// The account is halted (or flattening outside Live mode): read and logged, nothing else.
    AccountHalted,
    KillFlagSet,
    KillFlagUnreadable,
    VenueNotPaper,
    MandateInvalid,
    BrokerUnreachable,
    StateStoreError,
    /// The run store could not list our own order ids, so a flatten could not start (the state stays Flattening).
    RunStoreUnavailable,
    /// A run (or another process) holds the account's lock: skipped, the run reads equity itself.
    LockBusy,
    LockUnavailable,
    NoRuntime,
    Panicked,
}

impl MonitorCode {
    pub const ALL: [MonitorCode; 15] = [
        MonitorCode::Observed,
        MonitorCode::Halted,
        MonitorCode::FlattenResumed,
        MonitorCode::AccountHalted,
        MonitorCode::KillFlagSet,
        MonitorCode::KillFlagUnreadable,
        MonitorCode::VenueNotPaper,
        MonitorCode::MandateInvalid,
        MonitorCode::BrokerUnreachable,
        MonitorCode::StateStoreError,
        MonitorCode::RunStoreUnavailable,
        MonitorCode::LockBusy,
        MonitorCode::LockUnavailable,
        MonitorCode::NoRuntime,
        MonitorCode::Panicked,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            MonitorCode::Observed => "MONITOR_OBSERVED",
            MonitorCode::Halted => "MONITOR_HALTED",
            MonitorCode::FlattenResumed => "MONITOR_FLATTEN_RESUMED",
            MonitorCode::AccountHalted => "MONITOR_ACCOUNT_HALTED",
            MonitorCode::KillFlagSet => "MONITOR_KILL_FLAG_SET",
            MonitorCode::KillFlagUnreadable => "MONITOR_KILL_FLAG_UNREADABLE",
            MonitorCode::VenueNotPaper => "MONITOR_VENUE_NOT_PAPER",
            MonitorCode::MandateInvalid => "MONITOR_MANDATE_INVALID",
            MonitorCode::BrokerUnreachable => "MONITOR_BROKER_UNREACHABLE",
            MonitorCode::StateStoreError => "MONITOR_STATE_STORE_ERROR",
            MonitorCode::RunStoreUnavailable => "MONITOR_RUN_STORE_UNAVAILABLE",
            MonitorCode::LockBusy => "MONITOR_LOCK_BUSY",
            MonitorCode::LockUnavailable => "MONITOR_LOCK_UNAVAILABLE",
            MonitorCode::NoRuntime => "MONITOR_NO_RUNTIME",
            MonitorCode::Panicked => "MONITOR_PANICKED",
        }
    }

    /// Did the monitor fail to check the account? (A kill flag, a halted account or a busy lock is not a failure:
    /// each is the platform working as designed.)
    pub fn is_failure(self) -> bool {
        matches!(
            self,
            MonitorCode::KillFlagUnreadable
                | MonitorCode::VenueNotPaper
                | MonitorCode::MandateInvalid
                | MonitorCode::BrokerUnreachable
                | MonitorCode::StateStoreError
                | MonitorCode::RunStoreUnavailable
                | MonitorCode::LockUnavailable
                | MonitorCode::NoRuntime
                | MonitorCode::Panicked
        )
    }
}

impl std::fmt::Display for MonitorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What one reading of one account did. Kept for the service log; the durable traces are the equity log row, the
/// saved account state (and its database event log) and the alerts.
#[derive(Debug, Clone, PartialEq)]
pub struct MonitorReport {
    pub account_id: String,
    pub at: DateTime<Utc>,
    pub code: MonitorCode,
    pub message: String,
    /// The broker-reported equity of this reading (`None` when nothing was read).
    pub equity: Option<Dec>,
    /// The equity log append failed (the risk step still ran).
    pub log_error: Option<String>,
    pub decision: Option<RiskDecision>,
    pub status_before: Option<AccountStatus>,
    pub status_after: Option<AccountStatus>,
    pub transitions: Vec<Transition>,
    pub flatten: Option<FlattenReport>,
    pub alerts: Vec<Alert>,
    pub alert_delivery_failures: Vec<String>,
    /// Broker reads this call made (1 for a normal reading; the flatten's own reads are not counted here).
    pub reads: u32,
    /// Under `DayStartRule::UtcMidnight`: the reading's UTC day has no 00:00 UTC day-start (the one-day loss limit
    /// cannot fire today and the runs pause new buys).
    pub day_start_missing: bool,
}

impl MonitorReport {
    fn new(account_id: &str, at: DateTime<Utc>) -> Self {
        Self {
            account_id: account_id.to_string(),
            at,
            code: MonitorCode::Observed,
            message: String::new(),
            equity: None,
            log_error: None,
            decision: None,
            status_before: None,
            status_after: None,
            transitions: Vec::new(),
            flatten: None,
            alerts: Vec::new(),
            alert_delivery_failures: Vec::new(),
            reads: 0,
            day_start_missing: false,
        }
    }

    fn not_attempted(account_id: &str, at: DateTime<Utc>, code: MonitorCode, message: String) -> Self {
        let mut r = Self::new(account_id, at);
        r.code = code;
        r.message = message;
        r
    }

    /// One log line.
    pub fn summary(&self) -> String {
        let equity = self.equity.map_or("-".to_string(), |e| e.to_string());
        let risk = self.decision.as_ref().map_or("-".to_string(), |d| d.codes().join(","));
        let status = match (self.status_before, self.status_after) {
            (Some(a), Some(b)) if a != b => format!("{} -> {}", a.as_str(), b.as_str()),
            (Some(a), _) => a.as_str().to_string(),
            _ => "-".to_string(),
        };
        let mut line = format!("{}: {} equity {equity} risk {risk} status {status}", self.account_id, self.code.as_str());
        if !self.message.is_empty() {
            line.push_str(&format!(" ({})", self.message));
        }
        if let Some(e) = &self.log_error {
            line.push_str(&format!(" [equity log append failed: {e}]"));
        }
        line
    }
}

/// Everything one reading of one account needs. The stores, clock, notifier and kill flag are the same objects the
/// runs use; the broker and venue rules are the account's runtime.
pub struct MonitorContext<'a> {
    pub account_id: &'a str,
    pub mode: ExecutionMode,
    pub mandate: &'a MandateBody,
    pub broker: &'a dyn Broker,
    pub state_store: &'a dyn StateStore,
    /// Read only for our own order ids (the flatten must know which open orders are ours).
    pub runs: &'a dyn RunStore,
    pub notifier: &'a dyn Notifier,
    pub kill_flag: &'a dyn KillFlag,
    pub equity_log: &'a dyn EquityLog,
    pub clock: &'a dyn Clock,
    pub venue_rules: &'a VenueRuleBook<'a>,
    /// The runs' own configuration: the venue policy, the recovery fraction and the flatten's polling budget.
    pub config: &'a RunConfig,
}

struct Monitor<'a> {
    ctx: &'a MonitorContext<'a>,
    report: MonitorReport,
    state: AccountState,
}

/// Read one account's equity once and run the risk step on it. See the module docs.
pub fn monitor_once(ctx: &MonitorContext<'_>) -> MonitorReport {
    let now = ctx.clock.now();
    let mut m = Monitor { ctx, report: MonitorReport::new(ctx.account_id, now), state: AccountState::new(ctx.account_id) };
    m.execute();
    m.report.status_after = Some(m.state.status());
    m.report
}

impl Monitor<'_> {
    fn now(&self) -> DateTime<Utc> {
        self.ctx.clock.now()
    }

    fn done(&mut self, code: MonitorCode, message: impl Into<String>) {
        self.report.code = code;
        self.report.message = message.into();
    }

    fn alert(&mut self, code: AlertCode, severity: AlertSeverity, message: String, dedupe: &str) {
        let at = self.now();
        let a = Alert {
            code,
            severity,
            account_id: self.ctx.account_id.to_string(),
            run_key: monitor_key(self.ctx.account_id, self.report.at),
            message,
            dedupe_key: format!("{}:{dedupe}", self.ctx.account_id),
            at,
        };
        if let Err(e) = self.ctx.notifier.notify(&a) {
            self.report.alert_delivery_failures.push(format!("{}: {e}", a.code.as_str()));
        }
        self.report.alerts.push(a);
    }

    fn save_state(&mut self, next: AccountState) -> Result<(), StoreError> {
        if next == self.state {
            return Ok(());
        }
        let saved = self.ctx.state_store.save(self.state.version(), &next)?;
        self.state = saved;
        Ok(())
    }

    fn execute(&mut self) {
        let ctx = self.ctx;
        // 1. venue policy: a non-paper connection under the paper-only policy is never read.
        if ctx.config.venue_policy == VenuePolicy::PaperOnly && ctx.broker.environment() != VenueEnvironment::Paper {
            let env = ctx.broker.environment();
            return self.done(
                MonitorCode::VenueNotPaper,
                format!("the paper-only policy is in force and the {} broker connection reports {}: not read", ctx.broker.venue(), env.as_str()),
            );
        }
        // 2. kill flag.
        match ctx.kill_flag.is_set() {
            Ok(false) => {}
            Ok(true) => return self.done(MonitorCode::KillFlagSet, "the kill flag is set: nothing read, nothing sent"),
            Err(e) => return self.done(MonitorCode::KillFlagUnreadable, format!("the kill flag could not be read: {e}")),
        }
        // 3. the mandate's ladder and daily-loss limit, compiled exactly as the pipeline compiles them.
        let risk_policy = match RiskPolicy::from_mandate_with_recovery(ctx.mandate, ctx.config.recovery_fraction) {
            Ok(p) => p,
            Err(e) => return self.done(MonitorCode::MandateInvalid, e.to_string()),
        };
        // 4. the account state.
        match ctx.state_store.load(ctx.account_id) {
            Ok(Some(s)) => self.state = s,
            Ok(None) => self.state = AccountState::new(ctx.account_id),
            Err(e) => return self.done(MonitorCode::StateStoreError, format!("the account state could not be loaded: {e}")),
        }
        self.report.status_before = Some(self.state.status());
        // 5. one broker read; nothing is invented when it fails.
        let now = self.now();
        self.report.reads += 1;
        let snap = match ctx.broker.snapshot(now) {
            Ok(s) => s,
            Err(e) => return self.done(MonitorCode::BrokerUnreachable, format!("the broker account could not be read: {e}; nothing was evaluated or saved")),
        };
        self.report.equity = Some(snap.equity);
        // 6. the append-only equity log.
        self.log_reading(&snap);
        // 7. a halt is sticky: only a human resume leaves it.
        match self.state.status() {
            AccountStatus::Halted => {
                let why = self.state.halt_record().map_or("unknown".to_string(), |h| format!("{} at {}", h.reason.code(), h.at));
                return self.done(MonitorCode::AccountHalted, format!("the account is halted ({why}): read and logged only"));
            }
            AccountStatus::Flattening => {
                if ctx.mode != ExecutionMode::Live {
                    return self.done(MonitorCode::AccountHalted, format!("the account is flattening and {} mode sends nothing: read and logged only", ctx.mode.as_str()));
                }
                let code = if self.finish_flatten("a flatten was in progress") { MonitorCode::FlattenResumed } else { MonitorCode::RunStoreUnavailable };
                let msg = self.report.flatten.as_ref().map_or("the flatten could not start".to_string(), |f| f.summary());
                return self.done(code, msg);
            }
            AccountStatus::Active | AccountStatus::Shrunk => {}
        }
        // 8. the risk step on the broker's equity, with the reading's UTC date as the trading day.
        let prior_day = self.state.trading_day();
        let rule = ctx.config.day_start;
        let (next, decision, transition) = step_with(&self.state, &snap.equity_snapshot(), now.date_naive(), &risk_policy, rule);
        self.report.decision = Some(decision.clone());
        if let Some(t) = transition {
            self.report.transitions.push(t);
        }
        if let Err(e) = self.save_state(next) {
            if decision.action == RiskAction::HaltFlatten {
                // The halt could not be recorded, so nothing may be sent on it; say so loudly. The next reading (or
                // run) re-evaluates on fresh equity.
                let reason = decision.halt_reason.unwrap_or(HaltReason::EquityInvalid);
                self.alert(
                    AlertCode::RunFailed,
                    AlertSeverity::Critical,
                    format!("MONITOR_STATE_STORE_ERROR: the continuous monitor found {} but could not save the halt ({e}); nothing was sent", reason.code()),
                    "monitor:halt_not_saved",
                );
            }
            return self.done(MonitorCode::StateStoreError, format!("the account state could not be saved: {e}"));
        }
        if decision.action != RiskAction::HaltFlatten {
            let mut message = String::new();
            if let DayStartRule::UtcMidnight { max_lag_secs } = rule {
                let day = snap.taken_at.date_naive();
                if self.state.day_start_missing(day) {
                    self.report.day_start_missing = true;
                    // Only the reading that STARTED the day outside the window alerts: once per UTC day, and the
                    // missing day is persisted state, so a restart does not alert again.
                    let first_at = (prior_day != Some(day)).then_some(snap.taken_at);
                    message = day_start_missing_message(day, max_lag_secs, first_at);
                    if first_at.is_some() {
                        self.alert(AlertCode::RunFailed, AlertSeverity::Warning, message.clone(), &format!("day_start_missing:{day}"));
                    }
                }
            }
            return self.done(MonitorCode::Observed, message);
        }
        // 9. a halt rung or the daily-loss limit: alert, then flatten (Live) or halt and tell a person (otherwise).
        let reasons = decision.reasons.iter().map(|r| format!("{}: {}", r.code, r.message)).collect::<Vec<_>>().join(" | ");
        let reason = decision.halt_reason.unwrap_or(HaltReason::EquityInvalid);
        self.alert(AlertCode::Halt, AlertSeverity::Critical, format!("{}: {reasons} (found by the continuous monitor, no decision was pending)", reason.code()), "halt");
        if ctx.mode == ExecutionMode::Live {
            if !self.finish_flatten(&reasons) {
                return self.done(MonitorCode::RunStoreUnavailable, format!("{}: halted, but the flatten could not start; it is retried on the next reading or run", reason.code()));
            }
        } else {
            let (halted, tr) = self.state.complete_flatten(true);
            if let Some(t) = tr {
                self.report.transitions.push(t);
            }
            if let Err(e) = self.save_state(halted) {
                return self.done(MonitorCode::StateStoreError, format!("{}: the halt could not be completed: {e}", reason.code()));
            }
            self.alert(
                AlertCode::FlattenIncomplete,
                AlertSeverity::Critical,
                format!("{} mode: no orders were sent, so nothing was flattened; flatten the account by hand", ctx.mode.as_str()),
                "flatten",
            );
        }
        self.done(MonitorCode::Halted, reason.code());
    }

    fn log_reading(&mut self, snap: &BrokerSnapshot) {
        let reading = EquityReading {
            account_id: self.ctx.account_id.to_string(),
            taken_at: snap.taken_at,
            equity: snap.equity,
            cash: snap.cash,
            derived_equity: snap.derived_equity,
            source: MONITOR_SNAPSHOT_SOURCE,
        };
        if let Err(e) = self.ctx.equity_log.append(&reading) {
            self.report.log_error = Some(e);
        }
    }

    /// Run the flatten for the halt in force (the same `flatten`, tags and polling budget as the pipeline's
    /// `finish_flatten`), record it and move the state on. `false` when the flatten could not even start (our own
    /// order ids are unknown); the account then stays `Flattening` and is retried.
    fn finish_flatten(&mut self, why: &str) -> bool {
        let ctx = self.ctx;
        let Some(halt) = self.state.halt_record().cloned() else { return true };
        let known_ids: BTreeSet<String> = match ctx.runs.known_order_ids(ctx.account_id) {
            Ok(ids) => ids,
            Err(e) => {
                self.alert(
                    AlertCode::FlattenIncomplete,
                    AlertSeverity::Critical,
                    format!("halt ({why}): the flatten could not start because our own order ids could not be read ({e}); it is retried on the next reading or run"),
                    "flatten",
                );
                return false;
            }
        };
        let attempt_key = halt.at.format("%Y%m%dT%H%M%SZ").to_string();
        let universe: BTreeSet<String> = Policy::compile(ctx.mandate).limits().map(|l| l.instrument_allow.clone()).unwrap_or_default();
        let cfg = ctx.config;
        let mut spec = FlattenSpec::new(ctx.account_id, &attempt_key, &universe, ctx.venue_rules, &known_ids);
        spec.max_rounds = cfg.flatten_max_rounds;
        spec.max_polls = cfg.max_polls;
        spec.poll_secs = cfg.poll_secs;
        let report = flatten(ctx.broker, ctx.clock, &spec);
        let ok = report.verdict != FlattenVerdict::HaltAndAlert;
        let summary = report.summary();
        self.report.flatten = Some(report);
        let (next, tr) = self.state.complete_flatten(ok);
        if let Some(t) = tr {
            self.report.transitions.push(t);
        }
        if let Err(e) = self.save_state(next) {
            self.alert(AlertCode::FlattenIncomplete, AlertSeverity::Critical, format!("halt ({why}): the flatten ran ({summary}) but its result could not be saved: {e}"), "flatten");
            return true;
        }
        if !ok {
            self.alert(AlertCode::FlattenIncomplete, AlertSeverity::Critical, format!("halt ({why}): the flatten did not verify flat: {summary}"), "flatten");
        }
        true
    }
}

/// The run-key text of a monitor reading's alerts: `<account>|<reading time>|monitor` (the shape of a run key, with
/// `monitor` where the sleeve set would be, so an alert says it came from the monitor and no run record is implied).
pub fn monitor_key(account_id: &str, at: DateTime<Utc>) -> String {
    format!("{account_id}|{}|monitor", at.to_rfc3339())
}

// -------------------------------------------------------------------------------------------------------------
// Many accounts
// -------------------------------------------------------------------------------------------------------------

/// The accounts the monitor protects: an ACTIVE mandate and an approved plan, the same eligibility as
/// `driver::find_due_runs` (without the time slot: the monitor runs around the clock).
pub fn monitored_accounts(accounts: &[ActiveAccount]) -> Vec<&ActiveAccount> {
    accounts.iter().filter(|a| a.envelope.status == MandateStatus::Active && a.plan_approved).collect()
}

/// One alert per failure streak per account: the first failing reading after a good one (or a different failure
/// code) alerts; repeats of the same failure do not; a good reading ends the streak.
#[derive(Debug, Default)]
pub struct FailureStreaks {
    failing: BTreeMap<String, MonitorCode>,
}

impl FailureStreaks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold in one report. `true` when it starts a new failure streak (the caller alerts).
    pub fn observe(&mut self, report: &MonitorReport) -> bool {
        if report.code.is_failure() {
            let new_streak = self.failing.get(&report.account_id) != Some(&report.code);
            self.failing.insert(report.account_id.clone(), report.code);
            new_streak
        } else {
            if report.code != MonitorCode::LockBusy {
                self.failing.remove(&report.account_id);
            }
            false
        }
    }

    pub fn is_failing(&self, account_id: &str) -> bool {
        self.failing.contains_key(account_id)
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Monitor every protected account once ([`monitored_accounts`]), each under its per-account lock, a lock conflict,
/// a missing runtime or a panic in one never stopping the others. The first failure of each streak raises one
/// `ALERT_RUN_FAILED` (Warning) through `notifier`.
#[allow(clippy::too_many_arguments)]
pub fn monitor_all<'a, L: AccountLock>(
    accounts: &[ActiveAccount],
    runtimes: &BTreeMap<String, AccountRuntime<'a>>,
    state_store: &dyn StateStore,
    runs: &dyn RunStore,
    notifier: &dyn Notifier,
    kill_flag: &dyn KillFlag,
    equity_log: &dyn EquityLog,
    clock: &dyn Clock,
    lock: &L,
    config: &RunConfig,
    streaks: &mut FailureStreaks,
) -> Vec<MonitorReport> {
    let mut out = Vec::new();
    for account in monitored_accounts(accounts) {
        let id = account.account_id.as_str();
        let mut report = match runtimes.get(id) {
            None => MonitorReport::not_attempted(id, clock.now(), MonitorCode::NoRuntime, "no broker runtime is registered for this account".to_string()),
            Some(rt) => match lock.try_lock(id) {
                Ok(None) => MonitorReport::not_attempted(id, clock.now(), MonitorCode::LockBusy, "another process holds the account's lock".to_string()),
                Err(e) => MonitorReport::not_attempted(id, clock.now(), MonitorCode::LockUnavailable, e),
                Ok(Some(guard)) => {
                    let ctx = MonitorContext {
                        account_id: id,
                        mode: account.mode,
                        mandate: &account.mandate,
                        broker: rt.broker,
                        state_store,
                        runs,
                        notifier,
                        kill_flag,
                        equity_log,
                        clock,
                        venue_rules: rt.venue_rules,
                        config,
                    };
                    let result = panic::catch_unwind(AssertUnwindSafe(|| monitor_once(&ctx)));
                    drop(guard);
                    match result {
                        Ok(r) => r,
                        Err(payload) => MonitorReport::not_attempted(id, clock.now(), MonitorCode::Panicked, panic_message(&*payload)),
                    }
                }
            },
        };
        if streaks.observe(&report) {
            let alert = Alert {
                code: AlertCode::RunFailed,
                severity: AlertSeverity::Warning,
                account_id: id.to_string(),
                run_key: monitor_key(id, report.at),
                message: format!(
                    "{}: the continuous monitor could not check this account ({}); the drawdown ladder and the daily-loss limit are checked only when a run reads the account until it recovers",
                    report.code.as_str(),
                    report.message
                ),
                dedupe_key: format!("{id}:monitor:{}", report.code.as_str()),
                at: clock.now(),
            };
            if let Err(e) = notifier.notify(&alert) {
                report.alert_delivery_failures.push(format!("{}: {e}", alert.code.as_str()));
            }
            report.alerts.push(alert);
        }
        out.push(report);
    }
    out
}

/// Counts by code, for one service log line per monitor pass.
pub fn summarize(reports: &[MonitorReport]) -> String {
    let mut by: BTreeMap<&'static str, usize> = BTreeMap::new();
    for r in reports {
        *by.entry(r.code.as_str()).or_default() += 1;
    }
    let parts: Vec<String> = by.iter().map(|(c, n)| format!("{c}={n}")).collect();
    format!("{} account(s) monitored: {}", reports.len(), if parts.is_empty() { "none".to_string() } else { parts.join(", ") })
}
