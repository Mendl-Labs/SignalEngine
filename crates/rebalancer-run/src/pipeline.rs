//! `run_once`: the rebalancer's run pipeline. One call is one scheduled run; it returns the immutable [`RunRecord`]
//! that was also written to the `RunStore`. Everything external is a trait (broker, data, state store, clock, run
//! store, notifier, kill flag), so the whole pipeline runs offline against the fake exchange.
//!
//! # Steps, in order (each is recorded in `RunRecord::steps`)
//! 1. `acquire_run_key`: `RunStore::begin`. A finished key returns the FIRST record untouched (no-op); a live lease
//!    refuses (`RUN_BUSY`); an expired lease resumes (attempt + 1).
//! 2. `kill_flag`: set means refuse (`RUN_KILL_FLAG_SET`); unreadable means fail closed.
//! 3. `mandate`: no mandate, not active, or invalid means refuse with no order of any kind. An EXPIRED mandate
//!    continues, and the guard then admits only reducing orders (SPEC B3). Assisted and Paper runs may rehearse
//!    against a DRAFT mandate; Live never does.
//! 4. `read_account`: the broker is the source of truth; unreachable means no trading and an alert. The account
//!    state (HWM, day-start, status) is loaded. A `Halted` account is refused here; a `Flattening` one resumes its
//!    flatten (Live) and stops.
//! 5. `cleanup` (Live): our own stale open orders are cancelled and the account re-read.
//! 6. `reconcile_pre`: foreign orders, drift against the previous run's post-run snapshot (plus the fills of any
//!    crashed attempt's orders), stale view, equity cross-check. Any unexplained finding halts the account (no
//!    flatten: we do not know what is true) and alerts.
//! 7. `risk`: HWM and day-start are folded in and the ladder / daily-loss limit evaluated on BROKER equity. A halt
//!    rung alerts, and in Live mode flattens (verify flat, then `Halted`); the run stops.
//! 8. `targets`: the reference rules on validated data. ANY refusal (`RuleError`, data error) means no trade + alert.
//! 9. `plan`: `OrderPlanner` with the risk scale applied (targets scale by the shrink rung's factor, both ways).
//!    Every guard denial is kept in the record.
//! 10. `execute` by mode: Assisted writes tickets and places nothing; Paper sends every order validate-only; Live
//!     sends the sells (each tag looked up first, so an order that exists is never re-sent), waits for them to
//!     settle, RE-READS the account, re-plans on the real cash and sends the buys. After an unknown outcome the order
//!     is looked up by tag, never blindly retried in the same run.
//! 11. `reconcile_post` (Paper/Live): re-read the account and reconcile against the expected effect of our fills;
//!     any mismatch halts and alerts.
//! 12. `finish`: the record is written (immutable) and returned.
//!
//! The rebalancer never places an order when the account is halting: the status is checked before every send. The
//! only orders sent in a halt are flatten's SELLs of held quantities.
//!
//! # Design decisions a reviewer must confirm
//! * Shrink means "targets scaled by the rung's scale" in BOTH directions (a flat Shrunk account may buy at reduced
//!   size). Literal "reduce-only" would lock a flat account in Shrunk forever, since only equity growth releases it.
//! * A reconciliation halt does NOT flatten. Only the ladder's halt rung and the daily-loss limit do.
//! * In Assisted mode a person trades the account, so foreign orders and drift against our baseline are downgraded
//!   to information there; the risk overlay, stale-view and equity checks still apply.
//! * A halt raised in Assisted or Paper mode cannot flatten (nothing may be sent): the account goes straight to
//!   `Halted` and the alert says to flatten by hand.

use std::collections::{BTreeMap, BTreeSet};

use broker_adapters::{BrokerError, Dec, OrderReport, OrderRequest, OrderStatus, PlaceOutcome, Side};
use chrono::{DateTime, NaiveDate, Utc};
use mandate_core::mandate::{self, MandateBody};
use rebalancer_core::guard::{DayCounters, PricePoint};
use rebalancer_core::planner::{OrderPlan, OrderPlanner, PlanConfig, PlannedOrder, SleeveTarget};
use rebalancer_core::policy::{DeploymentLimits, MandateEnvelope, MandateStatus, Policy, Standing};
use rebalancer_core::venue::VenueRuleBook;
use rebalancer_risk::overlay::{step, RiskAction, RiskPolicy};
use rebalancer_risk::state::{AccountState, AccountStatus, HaltReason};
use rebalancer_risk::store::{StateStore, StoreError};
use reference_rules::{decide_crypto_trend, decide_etf_trend, latest_decision_date, data_fingerprint, Options, ETF_SYMBOLS};

use crate::broker::Broker;
use crate::clock::Clock;
use crate::data::{DataSource, SleeveKind, SleeveSpec};
use crate::flatten::{cancel_own_open_orders, flatten, FlattenSpec, FlattenVerdict};
use crate::record::*;
use crate::recon::{reconcile, ExpectedOrder, ReconBaseline, ReconCode, ReconInput, ReconReport, ReconTolerances, ReconVerdict, Severity};
use crate::stores::{Begin, JournalEntry, KillFlag, Notifier, RunStore};
use crate::view::BrokerSnapshot;

/// Stable machine codes of a run's outcome (halts use `HaltReason::code()` instead).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RunCode {
    Completed,
    KillFlagSet,
    NoActiveMandate,
    MandateNotActive,
    MandateInvalid,
    AccountHalted,
    Busy,
    StoreUnavailable,
    KillFlagUnreadable,
    RiskPolicyInvalid,
    BrokerUnreachable,
    StateStoreError,
    CleanupFailed,
    DataError,
    RuleError,
    PlanError,
}

impl RunCode {
    pub const ALL: [RunCode; 16] = [
        RunCode::Completed,
        RunCode::KillFlagSet,
        RunCode::NoActiveMandate,
        RunCode::MandateNotActive,
        RunCode::MandateInvalid,
        RunCode::AccountHalted,
        RunCode::Busy,
        RunCode::StoreUnavailable,
        RunCode::KillFlagUnreadable,
        RunCode::RiskPolicyInvalid,
        RunCode::BrokerUnreachable,
        RunCode::StateStoreError,
        RunCode::CleanupFailed,
        RunCode::DataError,
        RunCode::RuleError,
        RunCode::PlanError,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RunCode::Completed => "RUN_COMPLETED",
            RunCode::KillFlagSet => "RUN_KILL_FLAG_SET",
            RunCode::NoActiveMandate => "RUN_NO_ACTIVE_MANDATE",
            RunCode::MandateNotActive => "RUN_MANDATE_NOT_ACTIVE",
            RunCode::MandateInvalid => "RUN_MANDATE_INVALID",
            RunCode::AccountHalted => "RUN_ACCOUNT_HALTED",
            RunCode::Busy => "RUN_BUSY",
            RunCode::StoreUnavailable => "RUN_STORE_UNAVAILABLE",
            RunCode::KillFlagUnreadable => "RUN_KILL_FLAG_UNREADABLE",
            RunCode::RiskPolicyInvalid => "RUN_RISK_POLICY_INVALID",
            RunCode::BrokerUnreachable => "RUN_BROKER_UNREACHABLE",
            RunCode::StateStoreError => "RUN_STATE_STORE_ERROR",
            RunCode::CleanupFailed => "RUN_CLEANUP_FAILED",
            RunCode::DataError => "RUN_DATA_ERROR",
            RunCode::RuleError => "RUN_RULE_ERROR",
            RunCode::PlanError => "RUN_PLAN_ERROR",
        }
    }
}

/// Tunables of a run. None of these is a mandate field; each is a documented default a deployment may change.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Drop trades smaller than this many account-currency units.
    pub min_trade_abs: Dec,
    /// Drop trades smaller than this fraction of the target.
    pub min_trade_pct: Dec,
    /// Estimated fee as a fraction of notional (Kraken's entry tier is 0.0026).
    pub fee_rate: Dec,
    pub tolerances: ReconTolerances,
    /// A shrink rung is released when the drawdown is back within this fraction of the rung (default 0.5).
    pub recovery_fraction: Dec,
    pub max_price_age_secs: i64,
    /// How long an in-progress run keeps its key before another attempt may resume it.
    pub lease_secs: i64,
    pub max_polls: u32,
    pub poll_secs: u64,
    pub flatten_max_rounds: u32,
    /// The deployment's own limits; effective limit = min(mandate, deployment).
    pub deployment: Option<DeploymentLimits>,
}

impl Default for RunConfig {
    fn default() -> Self {
        let d = |s: &str| Dec::parse(s).unwrap_or(Dec::ZERO);
        Self {
            min_trade_abs: d("10"),
            min_trade_pct: d("0.02"),
            fee_rate: d("0.0026"),
            tolerances: ReconTolerances::default(),
            recovery_fraction: d("0.5"),
            max_price_age_secs: 300,
            lease_secs: 900,
            max_polls: 3,
            poll_secs: 1,
            flatten_max_rounds: 4,
            deployment: None,
        }
    }
}

pub struct RunContext<'a> {
    pub account_id: &'a str,
    /// When this run was scheduled to start (part of the run key and of every order tag).
    pub scheduled_for: DateTime<Utc>,
    /// The account-local trading day (the caller's calendar decides), used for day-start equity and day counters.
    pub trading_day: NaiveDate,
    pub mode: ExecutionMode,
    pub sleeves: &'a [SleeveSpec],
    /// `None` = the account has no mandate at all.
    pub mandate: Option<&'a MandateBody>,
    pub envelope: Option<&'a MandateEnvelope>,
    pub broker: &'a dyn Broker,
    pub data: &'a dyn DataSource,
    pub state_store: &'a dyn StateStore,
    pub clock: &'a dyn Clock,
    pub runs: &'a dyn RunStore,
    pub notifier: &'a dyn Notifier,
    pub kill_flag: &'a dyn KillFlag,
    pub venue_rules: &'a VenueRuleBook<'a>,
    pub config: &'a RunConfig,
}

/// Internal early exit: the outcome has already been set on the record.
struct Stop;

struct Run<'a> {
    ctx: &'a RunContext<'a>,
    rec: RunRecord,
    state: AccountState,
    snapshot: Option<BrokerSnapshot>,
    policy: Option<Policy>,
    risk_policy: Option<RiskPolicy>,
    known_ids: BTreeSet<String>,
    /// Pre-trade baseline (after any crashed attempt's fills), the anchor of the post-run expectation.
    pre_baseline: Option<ReconBaseline>,
    /// Broker ids whose fills are ALREADY inside `pre_baseline` (orders of a crashed earlier attempt), so the
    /// post-run expectation does not count them a second time when this attempt adopts them.
    applied_ids: BTreeSet<String>,
}

fn summarize(s: &BrokerSnapshot) -> SnapshotSummary {
    let mut holdings = BTreeMap::new();
    let mut marks = BTreeMap::new();
    for h in &s.holdings {
        holdings.insert(h.symbol.to_uppercase(), h.quantity);
        if let Some(m) = h.mark {
            marks.insert(h.symbol.to_uppercase(), m);
        }
    }
    SnapshotSummary {
        taken_at: s.taken_at,
        equity: s.equity,
        cash: s.cash,
        derived_equity: s.derived_equity,
        holdings,
        marks,
        open_order_ids: s.open_orders.iter().map(|o| o.broker_order_id.clone()).collect(),
    }
}

fn is_anomaly(e: &BrokerError) -> bool {
    matches!(e, BrokerError::Malformed(m) if m.contains("overfill anomaly"))
}

/// Run the pipeline once. See the module docs.
pub fn run_once(ctx: &RunContext<'_>) -> RunRecord {
    let started_at = ctx.clock.now();
    let ids: Vec<&str> = ctx.sleeves.iter().map(|s| s.id.as_str()).collect();
    let key = RunKey::new(ctx.account_id, ctx.scheduled_for, &ids);
    let mut run = Run {
        ctx,
        rec: RunRecord {
            key: key.clone(),
            mode: ctx.mode,
            attempt: 1,
            trading_day: ctx.trading_day,
            scheduled_for: ctx.scheduled_for,
            started_at,
            finished_at: started_at,
            outcome: RunOutcome { kind: OutcomeKind::Completed, code: RunCode::Completed.as_str().to_string(), message: String::new() },
            mandate_hash: String::new(),
            mandate_version: None,
            mandate_standing: "none".to_string(),
            deployment_digest: None,
            data_fingerprints: Vec::new(),
            pre_snapshot: None,
            post_snapshot: None,
            recon: Vec::new(),
            state_before: None,
            state_after: None,
            transitions: Vec::new(),
            risk: None,
            targets: Vec::new(),
            plan: None,
            replan: None,
            tickets: Vec::new(),
            placed: Vec::new(),
            cleanup: Vec::new(),
            flatten: None,
            alerts: Vec::new(),
            alert_delivery_failures: Vec::new(),
            steps: Vec::new(),
        },
        state: AccountState::new(ctx.account_id),
        snapshot: None,
        policy: None,
        risk_policy: None,
        known_ids: BTreeSet::new(),
        pre_baseline: None,
        applied_ids: BTreeSet::new(),
    };

    // 1. Acquire the run key.
    match ctx.runs.begin(&key, ctx.trading_day, started_at, ctx.config.lease_secs) {
        Ok(Begin::Started { attempt }) => {
            run.rec.attempt = attempt;
            run.note("acquire_run_key", format!("attempt {attempt}"));
        }
        Ok(Begin::AlreadyDone(first)) => return *first,
        Ok(Begin::Busy { started_at }) => {
            run.note("acquire_run_key", format!("another attempt started at {started_at} still holds the lease"));
            run.rec.outcome = RunOutcome { kind: OutcomeKind::Refused, code: RunCode::Busy.as_str().to_string(), message: "the run key is in progress".to_string() };
            run.rec.finished_at = ctx.clock.now();
            return run.rec; // not persisted: the other attempt owns the key
        }
        Err(e) => {
            run.note("acquire_run_key", format!("run store failed: {e}"));
            run.rec.outcome = RunOutcome { kind: OutcomeKind::FailedClosed, code: RunCode::StoreUnavailable.as_str().to_string(), message: e.to_string() };
            run.alert(AlertCode::RunFailed, AlertSeverity::Critical, format!("the run store is unavailable: {e}; nothing was traded"), "failed:store");
            run.rec.finished_at = ctx.clock.now();
            return run.rec; // cannot persist: fail closed
        }
    }

    let _ = run.execute();
    run.finish()
}

impl Run<'_> {
    fn now(&self) -> DateTime<Utc> {
        self.ctx.clock.now()
    }

    fn note(&mut self, step: &'static str, note: String) {
        let at = self.now();
        self.rec.steps.push(StepNote { step, at, note });
    }

    fn alert(&mut self, code: AlertCode, severity: AlertSeverity, message: String, dedupe: &str) {
        let a = Alert {
            code,
            severity,
            account_id: self.ctx.account_id.to_string(),
            run_key: self.rec.key.canonical(),
            message,
            dedupe_key: format!("{}:{dedupe}", self.ctx.account_id),
            at: self.now(),
        };
        if let Err(e) = self.ctx.notifier.notify(&a) {
            self.rec.alert_delivery_failures.push(format!("{}: {e}", a.code.as_str()));
        }
        self.rec.alerts.push(a);
    }

    fn stop(&mut self, kind: OutcomeKind, code: &str, message: impl Into<String>) -> Stop {
        self.rec.outcome = RunOutcome { kind, code: code.to_string(), message: message.into() };
        Stop
    }

    /// Fail closed: nothing was (or will be) traded because something is in doubt; alert.
    fn fail_closed(&mut self, code: RunCode, message: impl Into<String>) -> Stop {
        let message = message.into();
        self.alert(AlertCode::RunFailed, AlertSeverity::Critical, format!("{}: {message}; nothing was traded", code.as_str()), &format!("failed:{}", code.as_str()));
        self.stop(OutcomeKind::FailedClosed, code.as_str(), message)
    }

    fn finish(mut self) -> RunRecord {
        self.rec.state_after = Some(self.state.status());
        self.rec.finished_at = self.now();
        if let Err(e) = self.ctx.runs.finish(self.rec.clone()) {
            // The record could not be written. Nothing more can be done about the trades already made; say so.
            let msg = format!("the run record could not be written: {e}");
            self.rec.steps.push(StepNote { step: "finish", at: self.rec.finished_at, note: msg.clone() });
            self.alert(AlertCode::RunFailed, AlertSeverity::Critical, msg, "failed:record");
        }
        self.rec
    }

    fn execute(&mut self) -> Result<(), Stop> {
        self.step_kill_flag()?;
        self.step_mandate()?;
        self.step_read_account()?;
        self.step_halted_check()?;
        self.step_cleanup()?;
        self.step_recon_pre()?;
        self.step_risk()?;
        let targets = self.step_targets()?;
        let plan = self.step_plan(&targets)?;
        self.step_execute(&targets, plan)?;
        self.step_recon_post()?;
        self.stop(OutcomeKind::Completed, RunCode::Completed.as_str(), format!("{} run completed", self.ctx.mode.as_str()));
        Ok(())
    }

    // -------------------------------------------------------------------------------------------------------
    // 2. kill flag
    // -------------------------------------------------------------------------------------------------------

    fn step_kill_flag(&mut self) -> Result<(), Stop> {
        match self.ctx.kill_flag.is_set() {
            Ok(false) => {
                self.note("kill_flag", "not set".to_string());
                Ok(())
            }
            Ok(true) => {
                self.note("kill_flag", "SET".to_string());
                Err(self.stop(OutcomeKind::Refused, RunCode::KillFlagSet.as_str(), "the kill flag is set: no run"))
            }
            Err(e) => {
                self.note("kill_flag", format!("unreadable: {e}"));
                Err(self.fail_closed(RunCode::KillFlagUnreadable, format!("the kill flag could not be read: {e}")))
            }
        }
    }

    // -------------------------------------------------------------------------------------------------------
    // 3. mandate
    // -------------------------------------------------------------------------------------------------------

    fn step_mandate(&mut self) -> Result<(), Stop> {
        let ctx = self.ctx;
        let Some(body) = ctx.mandate else {
            self.note("mandate", "no mandate".to_string());
            self.alert(AlertCode::MandateUnusable, AlertSeverity::Warning, "the account has no mandate: no run".to_string(), "mandate");
            return Err(self.stop(OutcomeKind::Refused, RunCode::NoActiveMandate.as_str(), "no active mandate: no order of any kind"));
        };
        self.rec.mandate_hash = mandate::canonical_hash(body);
        let mut policy = Policy::compile(body).with_max_price_age_secs(ctx.config.max_price_age_secs);
        if let Some(env) = ctx.envelope {
            self.rec.mandate_version = Some(env.version);
            policy = policy.with_envelope(env.clone());
        }
        if let Some(dep) = &ctx.config.deployment {
            self.rec.deployment_digest = Some(dep.digest());
            policy = policy.with_deployment(dep);
        }
        let standing = policy.standing(self.now());
        match standing {
            Standing::Invalid => {
                self.rec.mandate_standing = "invalid".to_string();
                self.note("mandate", "invalid".to_string());
                self.alert(AlertCode::MandateUnusable, AlertSeverity::Critical, "the mandate is invalid: no run".to_string(), "mandate");
                return Err(self.stop(OutcomeKind::Refused, RunCode::MandateInvalid.as_str(), "the mandate did not validate"));
            }
            Standing::NotActive(why) => {
                let rehearsal = ctx.mode != ExecutionMode::Live && ctx.envelope.is_some_and(|e| e.status == MandateStatus::Draft);
                if !rehearsal {
                    self.rec.mandate_standing = format!("not active: {why}");
                    self.note("mandate", format!("not active: {why}"));
                    self.alert(AlertCode::MandateUnusable, AlertSeverity::Warning, format!("the mandate is not active ({why}): no run"), "mandate");
                    return Err(self.stop(OutcomeKind::Refused, RunCode::MandateNotActive.as_str(), why));
                }
                // A draft mandate may be rehearsed in Assisted / Paper mode only: planned as if active, never Live.
                if let Some(env) = ctx.envelope {
                    policy = policy.with_envelope(MandateEnvelope { status: MandateStatus::Active, ..env.clone() });
                }
                self.rec.mandate_standing = "draft (rehearsal)".to_string();
                self.note("mandate", "draft mandate, rehearsal only".to_string());
            }
            Standing::Expired => {
                self.rec.mandate_standing = "expired".to_string();
                self.note("mandate", "expired: only reducing orders may pass the guard".to_string());
            }
            Standing::Active => {
                self.rec.mandate_standing = "active".to_string();
                self.note("mandate", "active".to_string());
            }
        }
        match RiskPolicy::from_mandate_with_recovery(body, ctx.config.recovery_fraction) {
            Ok(r) => self.risk_policy = Some(r),
            Err(e) => return Err(self.fail_closed(RunCode::RiskPolicyInvalid, e.to_string())),
        }
        self.policy = Some(policy);
        Ok(())
    }

    // -------------------------------------------------------------------------------------------------------
    // 4. read the account (source of truth) and the state
    // -------------------------------------------------------------------------------------------------------

    fn read_snapshot(&mut self, step: &'static str) -> Result<BrokerSnapshot, Stop> {
        let now = self.now();
        match self.ctx.broker.snapshot(now) {
            Ok(s) => Ok(s),
            Err(e) => {
                self.note(step, format!("broker read failed: {e}"));
                Err(self.fail_closed(RunCode::BrokerUnreachable, format!("the broker account could not be read: {e}")))
            }
        }
    }

    fn step_read_account(&mut self) -> Result<(), Stop> {
        let snap = self.read_snapshot("read_account")?;
        self.rec.pre_snapshot = Some(summarize(&snap));
        self.note("read_account", format!("equity {} cash {} holdings {}", snap.equity, snap.cash, snap.holdings.len()));
        self.snapshot = Some(snap);
        match self.ctx.state_store.load(self.ctx.account_id) {
            Ok(Some(s)) => self.state = s,
            Ok(None) => self.state = AccountState::new(self.ctx.account_id),
            Err(e) => return Err(self.fail_closed(RunCode::StateStoreError, format!("the account state could not be loaded: {e}"))),
        }
        self.rec.state_before = Some(self.state.status());
        match self.ctx.runs.known_order_ids(self.ctx.account_id) {
            Ok(ids) => self.known_ids = ids,
            Err(e) => return Err(self.fail_closed(RunCode::StoreUnavailable, format!("the run store failed: {e}"))),
        }
        Ok(())
    }

    /// A halted account is refused; a flattening one resumes its flatten (Live) and stops.
    fn step_halted_check(&mut self) -> Result<(), Stop> {
        match self.state.status() {
            AccountStatus::Halted => {
                let why = self.state.halt_record().map_or("unknown".to_string(), |h| format!("{} at {}", h.reason.code(), h.at));
                self.note("halted_check", format!("halted ({why})"));
                self.alert(AlertCode::StillHalted, AlertSeverity::Warning, format!("the account is halted ({why}); no run until a person resumes it"), "halted");
                Err(self.stop(OutcomeKind::Refused, RunCode::AccountHalted.as_str(), format!("the account is halted ({why})")))
            }
            AccountStatus::Flattening => {
                self.note("halted_check", "flattening: resuming the flatten".to_string());
                if self.ctx.mode == ExecutionMode::Live {
                    self.finish_flatten("a flatten was in progress");
                    let code = self.state.halt_record().map_or(HaltReason::Manual.code(), |h| h.reason.code());
                    Err(self.stop(OutcomeKind::Halted, code, "the account is halting: flatten resumed"))
                } else {
                    self.alert(AlertCode::StillHalted, AlertSeverity::Warning, "the account is flattening and this run may not send orders".to_string(), "halted");
                    Err(self.stop(OutcomeKind::Refused, RunCode::AccountHalted.as_str(), "the account is flattening; only a Live run can finish it"))
                }
            }
            AccountStatus::Active | AccountStatus::Shrunk => Ok(()),
        }
    }

    // -------------------------------------------------------------------------------------------------------
    // 5. cleanup of our own stale open orders (Live)
    // -------------------------------------------------------------------------------------------------------

    fn step_cleanup(&mut self) -> Result<(), Stop> {
        if self.ctx.mode != ExecutionMode::Live {
            self.note("cleanup", "skipped: this mode sends no orders and cancels nothing".to_string());
            return Ok(());
        }
        let has_own = self
            .snapshot
            .as_ref()
            .is_some_and(|s| s.open_orders.iter().any(|o| crate::broker::is_own_order(o, &self.known_ids)));
        if !has_own {
            self.note("cleanup", "no stale orders of ours".to_string());
            return Ok(());
        }
        let cfg = self.ctx.config;
        let (cancels, failures, _foreign) = cancel_own_open_orders(self.ctx.broker, self.ctx.clock, &self.known_ids, cfg.max_polls, cfg.poll_secs);
        self.rec.cleanup = cancels;
        self.note("cleanup", format!("cancelled {} stale order(s), {} failure(s)", self.rec.cleanup.len(), failures.len()));
        if failures.iter().any(|f| f.code.is_blocking() || f.code == crate::flatten::FlattenCode::ReadFailed) {
            let list = failures.iter().map(|f| format!("{}: {}", f.code, f.message)).collect::<Vec<_>>().join(" | ");
            return Err(self.fail_closed(RunCode::CleanupFailed, format!("our stale open orders could not be cancelled: {list}")));
        }
        let snap = self.read_snapshot("cleanup")?;
        self.rec.pre_snapshot = Some(summarize(&snap));
        self.snapshot = Some(snap);
        Ok(())
    }

    // -------------------------------------------------------------------------------------------------------
    // 6. reconcile before trading
    // -------------------------------------------------------------------------------------------------------

    /// Orders of crashed attempts of this account, read back from the broker by tag (or by id), as expectations.
    fn in_flight_orders(&mut self) -> Result<Vec<ExpectedOrder>, Stop> {
        let entries = match self.ctx.runs.in_flight(self.ctx.account_id) {
            Ok(e) => e,
            Err(e) => return Err(self.fail_closed(RunCode::StoreUnavailable, format!("the run store failed: {e}"))),
        };
        let mut out = Vec::new();
        for e in entries {
            let reports = match self.ctx.broker.find_by_tag(&e.tag) {
                Ok(r) => r,
                Err(err) => return Err(self.fail_closed(RunCode::BrokerUnreachable, format!("could not look up {} from an earlier attempt: {err}", e.tag))),
            };
            let (symbol, side, planned) = reports
                .first()
                .map_or((String::new(), Side::Buy, Dec::ZERO), |r| (r.symbol.clone(), r.side.unwrap_or(Side::Buy), r.quantity));
            if let Some(id) = &e.broker_order_id {
                self.known_ids.insert(id.clone());
            }
            for r in &reports {
                self.known_ids.insert(r.broker_order_id.clone());
            }
            out.push(ExpectedOrder {
                tag: e.tag.clone(),
                symbol,
                side,
                planned_quantity: planned,
                broker_order_id: e.broker_order_id.clone().or_else(|| reports.first().map(|r| r.broker_order_id.clone())),
                reports,
                anomalies: Vec::new(),
                expect_exists: true,
            });
        }
        Ok(out)
    }

    fn downgrade_for_mode(&self, mut report: ReconReport) -> ReconReport {
        if self.ctx.mode == ExecutionMode::Assisted {
            for f in &mut report.findings {
                if matches!(f.code, ReconCode::ForeignOrder | ReconCode::PositionDrift | ReconCode::BalanceDrift) {
                    f.severity = Severity::Info;
                }
            }
            report.verdict = if report.findings.iter().any(|f| f.severity == Severity::Halt) { ReconVerdict::HaltAndAlert } else { ReconVerdict::Ok };
        }
        report
    }

    fn step_recon_pre(&mut self) -> Result<(), Stop> {
        let last = match self.ctx.runs.last_snapshot(self.ctx.account_id) {
            Ok(s) => s,
            Err(e) => return Err(self.fail_closed(RunCode::StoreUnavailable, format!("the run store failed: {e}"))),
        };
        let in_flight = self.in_flight_orders()?;
        let mut baseline = last.map(|s| s.baseline());
        // The fills of a crashed attempt's orders are inside the baseline from here on: either they were just applied
        // to the previous run's snapshot, or (no previous run) the baseline is the current snapshot, which has them.
        for e in &in_flight {
            self.applied_ids.extend(e.reports.iter().map(|r| r.broker_order_id.clone()));
        }
        if let Some(b) = &baseline {
            baseline = match b.after_orders(&in_flight) {
                Some(next) => Some(next),
                None => return Err(self.halt_reconciliation("the fills of an earlier attempt could not be applied to the baseline")),
            };
        }
        let snap = self.snapshot.clone().ok_or(Stop)?;
        let now = self.now();
        let report = reconcile(&ReconInput {
            view: &snap,
            now,
            known_order_ids: &self.known_ids,
            baseline: baseline.as_ref(),
            orders: &in_flight,
            tolerances: &self.ctx.config.tolerances,
        });
        let report = self.downgrade_for_mode(report);
        self.note("reconcile_pre", format!("{}: {}", report.verdict.as_str(), report.codes().join(",")));
        let halt = report.verdict == ReconVerdict::HaltAndAlert;
        let summary = report.halt_summary();
        self.rec.recon.push(StageRecon { stage: "pre", report });
        if halt {
            return Err(self.halt_reconciliation(&summary));
        }
        self.pre_baseline = Some(baseline.unwrap_or_else(|| ReconBaseline::from_snapshot(&snap)));
        Ok(())
    }

    /// Halt WITHOUT flatten (we do not know what is true), alert, and stop.
    fn halt_reconciliation(&mut self, detail: &str) -> Stop {
        let now = self.now();
        let (next, tr) = self.state.halt(HaltReason::Reconciliation, detail, now);
        if let Some(t) = tr {
            self.rec.transitions.push(t);
        }
        if let Err(stop) = self.save_state(next) {
            return stop;
        }
        self.alert(AlertCode::Halt, AlertSeverity::Critical, format!("HALT_RECONCILIATION: {detail}"), "halt");
        self.stop(OutcomeKind::Halted, HaltReason::Reconciliation.code(), detail.to_string())
    }

    fn save_state(&mut self, next: AccountState) -> Result<(), Stop> {
        if next == self.state {
            return Ok(());
        }
        match self.ctx.state_store.save(self.state.version(), &next) {
            Ok(saved) => {
                self.state = saved;
                Ok(())
            }
            Err(e @ StoreError::VersionConflict { .. }) => Err(self.fail_closed(RunCode::StateStoreError, format!("another run changed the account state: {e}"))),
            Err(e) => Err(self.fail_closed(RunCode::StateStoreError, format!("the account state could not be saved: {e}"))),
        }
    }

    // -------------------------------------------------------------------------------------------------------
    // 7. risk
    // -------------------------------------------------------------------------------------------------------

    fn step_risk(&mut self) -> Result<(), Stop> {
        let snap = self.snapshot.clone().ok_or(Stop)?;
        let risk_policy = self.risk_policy.clone().ok_or(Stop)?;
        let (next, decision, transition) = step(&self.state, &snap.equity_snapshot(), self.ctx.trading_day, &risk_policy);
        self.note("risk", format!("{} scale {}", decision.codes().join(","), decision.risk_scale));
        self.rec.risk = Some(decision.clone());
        if let Some(t) = transition {
            self.rec.transitions.push(t);
        }
        self.save_state(next)?;
        if decision.action != RiskAction::HaltFlatten {
            return Ok(());
        }
        let reasons = decision.reasons.iter().map(|r| format!("{}: {}", r.code, r.message)).collect::<Vec<_>>().join(" | ");
        let reason = decision.halt_reason.unwrap_or(HaltReason::EquityInvalid);
        self.alert(AlertCode::Halt, AlertSeverity::Critical, format!("{}: {reasons}", reason.code()), "halt");
        if self.ctx.mode == ExecutionMode::Live {
            self.finish_flatten(&reasons);
        } else {
            // Nothing may be sent in Assisted / Paper mode: the account is halted at once and a person is told to
            // flatten by hand.
            let (halted, tr) = self.state.complete_flatten(true);
            if let Some(t) = tr {
                self.rec.transitions.push(t);
            }
            self.save_state(halted)?;
            self.alert(
                AlertCode::FlattenIncomplete,
                AlertSeverity::Critical,
                format!("{} mode: no orders were sent, so nothing was flattened; flatten the account by hand", self.ctx.mode.as_str()),
                "flatten",
            );
        }
        Err(self.stop(OutcomeKind::Halted, reason.code(), reasons))
    }

    /// Run the flatten for the halt in force, record it, and move the state on. Saves the state; a save failure is
    /// recorded as an alert (the flatten already happened).
    fn finish_flatten(&mut self, why: &str) {
        let Some(halt) = self.state.halt_record().cloned() else { return };
        let attempt_key = halt.at.format("%Y%m%dT%H%M%SZ").to_string();
        let universe: BTreeSet<String> = self.policy.as_ref().and_then(|p| p.limits()).map(|l| l.instrument_allow.clone()).unwrap_or_default();
        let cfg = self.ctx.config;
        let mut spec = FlattenSpec::new(self.ctx.account_id, &attempt_key, &universe, self.ctx.venue_rules, &self.known_ids);
        spec.max_rounds = cfg.flatten_max_rounds;
        spec.max_polls = cfg.max_polls;
        spec.poll_secs = cfg.poll_secs;
        let report = flatten(self.ctx.broker, self.ctx.clock, &spec);
        self.note("flatten", report.summary());
        let ok = report.verdict != FlattenVerdict::HaltAndAlert;
        let summary = report.summary();
        // Fresh orders the flatten placed are ours from now on.
        for o in &report.orders {
            if let Some(id) = &o.broker_order_id {
                self.known_ids.insert(id.clone());
            }
        }
        self.rec.flatten = Some(report);
        let (next, tr) = self.state.complete_flatten(ok);
        if let Some(t) = tr {
            self.rec.transitions.push(t);
        }
        if self.save_state(next).is_err() {
            return;
        }
        if !ok {
            self.alert(AlertCode::FlattenIncomplete, AlertSeverity::Critical, format!("halt ({why}): the flatten did not verify flat: {summary}"), "flatten");
        }
    }

    // -------------------------------------------------------------------------------------------------------
    // 8. targets from the reference rules
    // -------------------------------------------------------------------------------------------------------

    fn step_targets(&mut self) -> Result<Vec<SleeveTarget>, Stop> {
        let as_of = self.ctx.scheduled_for.date_naive();
        let mut targets = Vec::new();
        for s in self.ctx.sleeves {
            let data = match self.ctx.data.sleeve_data(s, as_of) {
                Ok(d) => d,
                Err(e) => {
                    self.note("targets", format!("data error for {}: {e}", s.id));
                    return Err(self.fail_closed(RunCode::DataError, format!("sleeve {}: {e}", s.id)));
                }
            };
            let fingerprint = data_fingerprint(&data.panel);
            self.rec.data_fingerprints.push((s.id.clone(), fingerprint.clone()));
            let decided = match s.kind {
                SleeveKind::EtfTrend => latest_decision_date(&data.panel, &ETF_SYMBOLS)
                    .and_then(|date| decide_etf_trend(&data.panel, date, &Options::etf_live(as_of)).map(|d| (date, d)))
                    .map(|(date, d)| (date, SleeveTarget::from_etf(&s.id, s.share, &s.venue, &s.asset_class, &d))),
                SleeveKind::CryptoTrend => {
                    let date = as_of.pred_opt().unwrap_or(as_of);
                    decide_crypto_trend(&data.panel, date, &Options::crypto_live(as_of))
                        .map(|d| (date, SleeveTarget::from_crypto(&s.id, s.share, &s.venue, &s.asset_class, &s.quote, &d)))
                }
            };
            let (date, target) = match decided {
                Ok((date, Ok(t))) => (date, t),
                Ok((_, Err(e))) => return Err(self.fail_closed(RunCode::RuleError, format!("sleeve {}: weight conversion failed: {e}", s.id))),
                Err(e) => {
                    self.note("targets", format!("rule refused for {}: {e}", s.id));
                    return Err(self.fail_closed(RunCode::RuleError, format!("sleeve {}: the reference rule refused: {e}", s.id)));
                }
            };
            self.rec.targets.push(TargetSummary {
                sleeve: s.id.clone(),
                decision_date: date,
                data_fingerprint: fingerprint,
                weights: target.weights.iter().map(|w| (w.symbol.clone(), w.weight)).collect(),
            });
            targets.push(target);
        }
        self.note("targets", format!("{} sleeve(s)", targets.len()));
        Ok(targets)
    }

    // -------------------------------------------------------------------------------------------------------
    // 9. plan
    // -------------------------------------------------------------------------------------------------------

    fn prices_for(&mut self, targets: &[SleeveTarget], snap: &BrokerSnapshot) -> Result<BTreeMap<String, PricePoint>, Stop> {
        let mut symbols: BTreeSet<String> = targets.iter().flat_map(|t| t.weights.iter().map(|w| w.symbol.to_uppercase())).collect();
        symbols.extend(snap.holdings.iter().map(|h| h.symbol.to_uppercase()));
        let symbols: Vec<String> = symbols.into_iter().collect();
        match self.ctx.data.prices(&symbols, self.now()) {
            Ok(p) => Ok(p),
            Err(e) => Err(self.fail_closed(RunCode::DataError, format!("prices: {e}"))),
        }
    }

    fn plan_with(&mut self, targets: &[SleeveTarget], snap: &BrokerSnapshot, day: DayCounters, credit_sells: bool) -> Result<OrderPlan, Stop> {
        let prices = self.prices_for(targets, snap)?;
        let policy = self.policy.clone().ok_or(Stop)?;
        let risk_scale = self.state.risk_scale();
        let view = snap.account_view(self.ctx.account_id, self.state.status().is_halt(), self.now());
        let cfg = self.ctx.config;
        let mut pc = PlanConfig::new(self.ctx.scheduled_for, risk_scale, cfg.min_trade_abs, cfg.min_trade_pct, cfg.fee_rate);
        pc.credit_sell_proceeds = credit_sells;
        pc.day = day;
        match OrderPlanner::plan(targets, &view, &prices, self.ctx.venue_rules, &policy, &pc) {
            Ok(p) => Ok(p),
            Err(e) => Err(self.fail_closed(RunCode::PlanError, format!("planning failed: {e}"))),
        }
    }

    fn summarize_plan(plan: &OrderPlan) -> PlanSummary {
        PlanSummary {
            inputs_digest: plan.inputs_digest.clone(),
            equity: plan.equity,
            capital_base: plan.capital_base,
            risk_scale: plan.risk_scale,
            orders: plan.orders.clone(),
            denied: plan
                .denied
                .iter()
                .map(|d| DeniedSummary { order: d.order.clone(), codes: d.reasons.iter().map(|r| r.code.as_str()).collect(), reasons: d.reasons.clone() })
                .collect(),
            skipped: plan.skipped.clone(),
            lines: plan.lines.clone(),
        }
    }

    fn day_counters(&mut self) -> Result<DayCounters, Stop> {
        match self.ctx.runs.day_counters(self.ctx.account_id, self.ctx.trading_day) {
            Ok(c) => Ok(c),
            Err(e) => Err(self.fail_closed(RunCode::StoreUnavailable, format!("the run store failed: {e}"))),
        }
    }

    fn step_plan(&mut self, targets: &[SleeveTarget]) -> Result<OrderPlan, Stop> {
        let snap = self.snapshot.clone().ok_or(Stop)?;
        let day = self.day_counters()?;
        let plan = self.plan_with(targets, &snap, day, true)?;
        self.rec.plan = Some(Self::summarize_plan(&plan));
        self.note("plan", format!("{} order(s), {} denied, {} skipped, scale {}", plan.orders.len(), plan.denied.len(), plan.skipped.len(), plan.risk_scale));
        Ok(plan)
    }

    // -------------------------------------------------------------------------------------------------------
    // 10. execute
    // -------------------------------------------------------------------------------------------------------

    fn step_execute(&mut self, targets: &[SleeveTarget], plan: OrderPlan) -> Result<(), Stop> {
        match self.ctx.mode {
            ExecutionMode::Assisted => {
                self.rec.tickets = plan.orders.clone();
                self.note("execute", format!("assisted: {} ticket(s) written, nothing placed", self.rec.tickets.len()));
                Ok(())
            }
            ExecutionMode::Paper => {
                for o in &plan.orders {
                    let p = self.execute_order(o, Phase::Rehearsal);
                    self.rec.placed.push(p);
                }
                self.note("execute", format!("paper: {} order(s) sent validate-only", plan.orders.len()));
                Ok(())
            }
            ExecutionMode::Live => {
                for o in plan.orders.iter().filter(|o| o.side == Side::Sell) {
                    let p = self.execute_order(o, Phase::Sells);
                    self.rec.placed.push(p);
                }
                // Buys are sized on what the account REALLY holds now: re-read the account after the sells settled.
                let had_buys = plan.orders.iter().any(|o| o.side == Side::Buy);
                if !had_buys {
                    self.note("execute", "live: sells only".to_string());
                    return Ok(());
                }
                let snap = self.read_snapshot("execute")?;
                let day = self.day_counters()?;
                let replan = self.plan_with(targets, &snap, day, false)?;
                self.rec.replan = Some(Self::summarize_plan(&replan));
                self.note("execute", format!("live: re-read cash {}, {} buy(s) after the sells", snap.cash, replan.orders.iter().filter(|o| o.side == Side::Buy).count()));
                for o in replan.orders.iter().filter(|o| o.side == Side::Buy) {
                    let p = self.execute_order(o, Phase::Buys);
                    self.rec.placed.push(p);
                }
                Ok(())
            }
        }
    }

    fn finalize(placed: &mut PlacedOrder, r: OrderReport) {
        placed.status = Some(r.status);
        placed.executed_quantity = r.executed_quantity;
        let adopted = placed.outcome == PlacedOutcome::AdoptedExisting;
        if !adopted {
            placed.outcome = if r.status == OrderStatus::Filled {
                PlacedOutcome::Filled
            } else if r.executed_quantity.is_positive() && r.status.is_terminal() {
                PlacedOutcome::PartiallyFilled
            } else if r.status.is_terminal() {
                PlacedOutcome::NothingExecuted
            } else {
                PlacedOutcome::Unsettled
            };
        }
        placed.reports.push(r);
    }

    /// Poll an accepted order to its end; cancel it if it will not end (we send market orders only).
    fn settle_into(&mut self, placed: &mut PlacedOrder, id: &str) {
        placed.broker_order_id = Some(id.to_string());
        self.known_ids.insert(id.to_string());
        let cfg = self.ctx.config;
        for i in 0..cfg.max_polls {
            match self.ctx.broker.get_order(id) {
                Ok(r) if r.status.is_terminal() => return Self::finalize(placed, r),
                Ok(r) => {
                    placed.status = Some(r.status);
                    if i + 1 < cfg.max_polls {
                        self.ctx.clock.sleep_secs(cfg.poll_secs);
                    }
                }
                Err(e) => {
                    let anomaly = is_anomaly(&e);
                    placed.anomalies.push(e.to_string());
                    if anomaly {
                        placed.outcome = PlacedOutcome::Unsettled;
                    }
                    return;
                }
            }
        }
        match self.ctx.broker.cancel_and_settle(id) {
            Ok((_, r)) => Self::finalize(placed, r),
            Err(e) => {
                placed.anomalies.push(e.to_string());
                placed.outcome = PlacedOutcome::Unsettled;
            }
        }
    }

    fn adopt(&mut self, placed: &mut PlacedOrder, found: Vec<OrderReport>) {
        placed.outcome = PlacedOutcome::AdoptedExisting;
        let id = found[0].broker_order_id.clone();
        // From here the record describes the order that EXISTS, not this attempt's plan for it.
        placed.detail = format!("an order with this tag already existed at the broker ({id}, quantity {}): adopted, not re-sent; this attempt planned {}", found[0].quantity, placed.planned_quantity);
        placed.planned_quantity = found[0].quantity;
        if let Err(e) = self.ctx.runs.journal_order(&self.rec.key, JournalEntry { tag: placed.tag.clone(), broker_order_id: Some(id.clone()), notional: Dec::ZERO }) {
            placed.anomalies.push(format!("journal: {e}"));
        }
        // Read its current state (it may still be open).
        self.settle_into(placed, &id);
    }

    fn execute_order(&mut self, o: &PlannedOrder, phase: Phase) -> PlacedOrder {
        let rehearsal = phase == Phase::Rehearsal;
        let mut placed = PlacedOrder {
            phase,
            tag: o.tag.clone(),
            symbol: o.symbol.clone(),
            side: o.side,
            planned_quantity: o.quantity,
            price: o.price,
            outcome: PlacedOutcome::NotSent,
            broker_order_id: None,
            status: None,
            executed_quantity: Dec::ZERO,
            reports: Vec::new(),
            anomalies: Vec::new(),
            detail: String::new(),
        };
        // Never send anything unless the account may add risk (a halting account only ever gets flatten's sells).
        if !self.state.status().may_add_risk() {
            placed.detail = format!("refused: the account is {}", self.state.status().as_str());
            return placed;
        }
        // Never send an order whose tag already exists at the broker (an earlier attempt of this run placed it).
        match self.ctx.broker.find_by_tag(&o.tag) {
            Err(e) => {
                placed.detail = format!("not sent: the tag look-up failed ({e}), so it cannot be known whether the order exists");
                return placed;
            }
            Ok(found) if !found.is_empty() => {
                if rehearsal {
                    placed.anomalies.push(format!("PAPER_ORDER_EXISTS: {} order(s) already carry this tag in a paper run", found.len()));
                }
                self.adopt(&mut placed, found);
                return placed;
            }
            Ok(_) => {}
        }
        if !rehearsal {
            // Record the INTENT before sending, so a crash between the send and the answer is recoverable.
            let intent = JournalEntry { tag: o.tag.clone(), broker_order_id: None, notional: o.notional };
            if let Err(e) = self.ctx.runs.journal_order(&self.rec.key, intent) {
                placed.detail = format!("not sent: the intent could not be journaled ({e})");
                return placed;
            }
        }
        let mut req = OrderRequest::market(&o.tag, &o.symbol, o.side, o.quantity);
        req.reference_price = Some(o.price);
        req.validate_only = rehearsal;
        match self.ctx.broker.place(&req) {
            Ok(PlaceOutcome::Accepted { broker_order_id, .. }) => {
                if rehearsal {
                    placed.anomalies.push(format!("PAPER_ORDER_EXISTS: a validate-only order came back as accepted ({broker_order_id})"));
                }
                if !rehearsal {
                    let entry = JournalEntry { tag: o.tag.clone(), broker_order_id: Some(broker_order_id.clone()), notional: o.notional };
                    if let Err(e) = self.ctx.runs.journal_order(&self.rec.key, entry) {
                        placed.anomalies.push(format!("journal: {e}"));
                    }
                }
                self.settle_into(&mut placed, &broker_order_id);
            }
            Ok(PlaceOutcome::ValidatedOnly { .. }) => placed.outcome = PlacedOutcome::Validated,
            Ok(PlaceOutcome::Rejected { errors, .. }) => {
                placed.outcome = PlacedOutcome::Rejected;
                placed.detail = errors.iter().map(|e| e.code.clone()).collect::<Vec<_>>().join(", ");
            }
            Ok(PlaceOutcome::UnknownOutcome { reason, .. }) => {
                // Look the order up by its tag. Never re-send in this run.
                match self.ctx.broker.find_by_tag(&o.tag) {
                    Ok(found) if !found.is_empty() => {
                        if rehearsal {
                            placed.anomalies.push("PAPER_ORDER_EXISTS: a validate-only request created an order".to_string());
                        }
                        self.adopt(&mut placed, found);
                        placed.detail = format!("outcome was unknown ({reason}); found by tag");
                    }
                    Ok(_) => {
                        placed.outcome = PlacedOutcome::UnknownNotFound;
                        placed.detail = format!("outcome was unknown ({reason}) and no order carries the tag: treated as not placed, not retried");
                    }
                    Err(e) => {
                        placed.outcome = PlacedOutcome::UnknownNotFound;
                        placed.detail = format!("outcome was unknown ({reason}) and the look-up failed ({e}): not retried");
                    }
                }
            }
            Err(e) => {
                placed.outcome = PlacedOutcome::NotSent;
                placed.detail = format!("not sent: {e}");
            }
        }
        placed
    }

    // -------------------------------------------------------------------------------------------------------
    // 11. reconcile after trading
    // -------------------------------------------------------------------------------------------------------

    fn step_recon_post(&mut self) -> Result<(), Stop> {
        if self.ctx.mode == ExecutionMode::Assisted {
            self.note("reconcile_post", "skipped: nothing was placed".to_string());
            return Ok(());
        }
        let live = self.ctx.mode == ExecutionMode::Live;
        // A last look-up by tag for orders whose outcome was unknown: a delayed request may have landed since.
        let mut expected: Vec<ExpectedOrder> = Vec::new();
        let mut placed = std::mem::take(&mut self.rec.placed);
        for p in &mut placed {
            if p.outcome == PlacedOutcome::UnknownNotFound {
                if let Ok(found) = self.ctx.broker.find_by_tag(&p.tag) {
                    if !found.is_empty() {
                        p.broker_order_id = Some(found[0].broker_order_id.clone());
                        p.reports = found;
                        p.detail.push_str(" | the order appeared after the run's own look-up");
                    }
                }
            }
            let sent = !matches!(p.outcome, PlacedOutcome::NotSent | PlacedOutcome::Rejected | PlacedOutcome::Validated);
            expected.push(ExpectedOrder {
                tag: p.tag.clone(),
                symbol: p.symbol.clone(),
                side: p.side,
                planned_quantity: p.planned_quantity,
                broker_order_id: p.broker_order_id.clone(),
                reports: p.reports.clone(),
                anomalies: p.anomalies.clone(),
                expect_exists: (live && sent) || !p.anomalies.is_empty(),
            });
        }
        let unsettled: Vec<String> = placed.iter().filter(|p| p.outcome == PlacedOutcome::Unsettled).map(|p| p.tag.clone()).collect();
        self.rec.placed = placed;
        if !unsettled.is_empty() {
            let msg = format!("orders still live or unreadable after the run: {}", unsettled.join(", "));
            self.note("reconcile_post", msg.clone());
            return Err(self.halt_reconciliation(&msg));
        }
        let snap = match self.ctx.broker.snapshot(self.now()) {
            Ok(s) => s,
            Err(e) => {
                self.note("reconcile_post", format!("re-read failed: {e}"));
                return Err(self.halt_reconciliation(&format!("the account could not be re-read after trading: {e}")));
            }
        };
        self.rec.post_snapshot = Some(summarize(&snap));
        // Orders whose fills are already inside the baseline (a crashed attempt's) must not be counted twice.
        let live_orders: Vec<ExpectedOrder> = expected
            .iter()
            .filter(|e| e.expect_exists && e.reports.iter().all(|r| !self.applied_ids.contains(&r.broker_order_id)))
            .cloned()
            .collect();
        let base = self.pre_baseline.clone().ok_or(Stop)?;
        let expected_after = if live {
            match base.after_orders(&live_orders) {
                Some(b) => b,
                None => return Err(self.halt_reconciliation("the fills of this run could not be applied to the baseline")),
            }
        } else {
            base
        };
        let report = reconcile(&ReconInput {
            view: &snap,
            now: self.now(),
            known_order_ids: &self.known_ids,
            baseline: Some(&expected_after),
            orders: &expected,
            tolerances: &self.ctx.config.tolerances,
        });
        self.note("reconcile_post", format!("{}: {}", report.verdict.as_str(), report.codes().join(",")));
        let halt = report.verdict == ReconVerdict::HaltAndAlert;
        let summary = report.halt_summary();
        self.rec.recon.push(StageRecon { stage: "post", report });
        if halt {
            return Err(self.halt_reconciliation(&summary));
        }
        self.snapshot = Some(snap);
        Ok(())
    }
}
