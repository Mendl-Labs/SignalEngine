//! The multi-account driver loop (WP4.8, product-mandate/IMPLEMENTATION_PLAN.md section 1a.2): `run_once` handles
//! exactly one account per call. Nothing before this module enumerated "every active tenant-account with a
//! rebalance due right now" and dispatched a run for each -- for one pilot account that gap doesn't show; for a
//! second tenant it is the difference between a service and a script.
//!
//! Two functions:
//! * [`find_due_runs`]: reads [`AccountSource::active_accounts`] and returns the [`RunSpec`]s that are due at or
//!   before `now`. Pure enumeration: it does NOT consult the [`crate::stores::RunStore`], so calling it twice
//!   yields the same candidates. Exactly-once is [`run_all_due`]'s job (see below), not this function's -- the
//!   existing run-key uniqueness `(account_id, scheduled_for, sleeve_set)` already makes a duplicate `run_once`
//!   call a no-op (`Begin::AlreadyDone`), so over-enumerating is safe, just wasted work without the lock.
//! * [`run_all_due`]: calls [`crate::pipeline::run_once`] once per candidate, catching and logging (returning, not
//!   propagating) a failure on any ONE candidate so it never aborts the others -- the same per-row convention
//!   `deployment_lifecycle_scheduler.rs` (BacktestingEngine's `program` crate) uses for its own tick passes.
//!
//! # Concurrency safety: why a per-account advisory lock, not the state store's compare-and-swap alone
//! Two processes (two service replicas, or an overlapping tick) must never both run the same account at the same
//! moment. Two mechanisms are available and this module uses BOTH, deliberately:
//! * **Primary: a per-account lock**, taken by `run_all_due` before calling `run_once` and held for that account's
//!   whole run (mirrors `deployment_lifecycle_scheduler.rs`'s `try_acquire_tick_lock`: a session-level Postgres
//!   advisory lock on a dedicated connection, released by dropping the connection -- so a crashed holder can never
//!   wedge another process out). This is CHEAP to check up front and stops the second process before it does any
//!   work at all: without it, two processes racing the same due account would both read the broker, both build a
//!   plan, and only the LOSER of `RunStore::begin` would discover the collision, after having already made a
//!   redundant (for Live mode: possibly costly, since a broker read against a rate-limited API is not free) round
//!   trip. [`AccountLock`] is the trait; [`crate::testkit::InMemoryAccountLock`] is the in-process test double;
//!   `rebalancer-store::PgAccountLock` (Part 1) is the production implementation.
//! * **Backstop: the run store's own run-key uniqueness**, unchanged from `run_once`'s existing contract. Even if
//!   the lock were somehow bypassed (a bug, a lock backend outage that fails open, a third process that doesn't go
//!   through `run_all_due` at all), `RunStore::begin`'s `(account_id, scheduled_for, sleeve_set)` uniqueness is
//!   the thing that actually prevents two attempts from BOTH placing orders -- a Postgres implementation maps it to
//!   `INSERT ... ON CONFLICT DO NOTHING`, atomic regardless of what took or didn't take the advisory lock first.
//!
//! The lock is the efficient common case; the run-key is the correctness guarantee neither this module nor a
//! Postgres outage can accidentally turn off.

use std::collections::BTreeMap;
use std::panic::{self, AssertUnwindSafe};

use chrono::{DateTime, NaiveDate, Utc};
use mandate_core::mandate::MandateBody;
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_core::venue::VenueRuleBook;

use crate::broker::Broker;
use crate::clock::Clock;
use crate::data::{DataSource, SleeveKind, SleeveSpec};
use crate::decision::EvalCache;
use crate::pipeline::{run_once, RunConfig, RunContext};
use crate::record::{ExecutionMode, RunRecord};
use crate::stores::{KillFlag, Notifier, RunStore};
use rebalancer_risk::store::StateStore;

// -------------------------------------------------------------------------------------------------------------
// What the driver reads: active tenant-accounts
// -------------------------------------------------------------------------------------------------------------

/// One tenant-account as the driver loop needs to see it: enough to decide whether it is due and, if so, to build
/// a [`RunContext`] for it. Deliberately does NOT carry broker/data connections (see [`AccountRuntime`]) -- which
/// adapter and which credentials an account uses is a runtime concern (WP3.4's tenant-scoped credential loader),
/// not something this enumeration-only trait should own.
#[derive(Debug, Clone, PartialEq)]
pub struct ActiveAccount {
    pub account_id: String,
    pub tenant_id: String,
    pub mandate: MandateBody,
    pub envelope: MandateEnvelope,
    /// WP2.5's Proposal/Plan API is not built yet; this is the one field this module needs from it: an immutable
    /// plan exists, bound to `envelope.version`, and a human approved it. `find_due_runs` treats `false` exactly
    /// like a mandate that is not active (excluded, not just refused inside the pipeline) -- the pipeline itself
    /// has no "plan approved" concept to refuse on, so skipping it here is the only place that check can happen.
    pub plan_approved: bool,
    pub sleeves: Vec<SleeveSpec>,
    pub mode: ExecutionMode,
}

/// The source of every tenant-account the driver should consider. A Postgres implementation (once WP1.2/1.3's
/// mandate storage lands) reads `mandates`/`strategy_plans`/`account` rows across every tenant; the in-memory
/// implementation below is what the tests in this crate and `rebalancer-service`'s own tests use.
pub trait AccountSource {
    fn active_accounts(&self) -> Result<Vec<ActiveAccount>, String>;
}

/// The in-memory `AccountSource` used by tests (and, until WP1.3's mandate API exists, usable as a config-file-fed
/// stand-in by a real deployment -- see `rebalancer-service`'s own module docs).
#[derive(Default)]
pub struct InMemoryAccountSource {
    accounts: std::sync::Mutex<Vec<ActiveAccount>>,
}

impl InMemoryAccountSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_account(self, account: ActiveAccount) -> Self {
        self.accounts.lock().unwrap_or_else(|e| e.into_inner()).push(account);
        self
    }

    pub fn set_accounts(&self, accounts: Vec<ActiveAccount>) {
        *self.accounts.lock().unwrap_or_else(|e| e.into_inner()) = accounts;
    }
}

impl AccountSource for InMemoryAccountSource {
    fn active_accounts(&self) -> Result<Vec<ActiveAccount>, String> {
        Ok(self.accounts.lock().unwrap_or_else(|e| e.into_inner()).clone())
    }
}

// -------------------------------------------------------------------------------------------------------------
// Due-ness: every configured sleeve is EVALUATED on every run slot; whether its decision is ACTED on is a
// per-account, data-driven question the pipeline answers (`pipeline::step_decisions`), not a calendar predicate.
// -------------------------------------------------------------------------------------------------------------

/// The single daily UTC time every sleeve's run slot is anchored to, matching the existing test fixtures'
/// convention (`00:10:00Z`, see `rebalancer-run/tests/common/harness.rs::slot_time`). Not a mandate field: a
/// deployment setting, like `RunConfig`'s tunables.
pub const DAILY_RUN_HOUR_UTC: u32 = 0;
pub const DAILY_RUN_MINUTE_UTC: u32 = 10;

/// Is `kind`'s sleeve evaluated on the run slot of calendar date `date`? Every kind is, on every date.
///
/// This used to make the ETF sleeve due only on the last CALENDAR day of the month
/// (`reference_rules::is_calendar_month_end`, a wall-clock question with no data and no holidays). That predicate
/// disagreed with the data-driven month-end the rule itself computes (`latest_decision_date`, which under
/// `MonthEndMode::NextMonthBar` needs a bar of the NEXT month), so the run on the calendar month-end always saw the
/// PREVIOUS month's decision and the sleeve acted a month late (finding U3). No refusal fires in that situation, so
/// nothing in the rule's own checks catches it. Now the run is daily, the rule is evaluated each time, and the
/// pipeline plans the ETF sleeve only when its computable decision is newer than the last one acted on. The
/// `match` stays exhaustive so a new sleeve kind forces a decision here.
fn sleeve_due_on(kind: SleeveKind, _date: NaiveDate) -> bool {
    match kind {
        SleeveKind::CryptoTrend | SleeveKind::EtfTrend => true,
    }
}

/// The most recent due slot at or before `now`, if any of `sleeves` is due on that slot's date (see
/// [`sleeve_due_on`]: with the current kinds, every account with at least one sleeve is due every day). Only the
/// CURRENT day's slot is considered (not a backlog of older missed slots): catching up on a run missed days ago is
/// the dead-man's-switch / heartbeat's job (`crate::stores::find_missed`, already built), which alerts a person
/// before anything trades against stale slots -- silently backfilling old slots here would place trades no one was
/// told about missing in the first place.
fn slot_for(sleeves: &[SleeveSpec], now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let today = now.date_naive();
    let slot = today.and_hms_opt(DAILY_RUN_HOUR_UTC, DAILY_RUN_MINUTE_UTC, 0)?.and_utc();
    if now < slot {
        return None;
    }
    if !sleeves.iter().any(|s| sleeve_due_on(s.kind, today)) {
        return None;
    }
    Some(slot)
}

// -------------------------------------------------------------------------------------------------------------
// RunSpec: one due candidate, self-contained (everything `run_once` needs except the shared/per-account infra)
// -------------------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct RunSpec {
    pub account_id: String,
    pub tenant_id: String,
    pub scheduled_for: DateTime<Utc>,
    pub trading_day: NaiveDate,
    pub mode: ExecutionMode,
    pub sleeves: Vec<SleeveSpec>,
    pub mandate: MandateBody,
    pub envelope: MandateEnvelope,
}

/// Every active, plan-approved tenant-account with a rebalance due at or before `now`. Excludes: a mandate that is
/// not `Active` (draft/superseded/revoked/expired -- the pipeline's own rehearsal allowance for a draft mandate in
/// Assisted/Paper mode is a per-run concern the pipeline still applies; the driver's job is only "should a run be
/// attempted at all", and an inactive mandate should not even be attempted for a scheduled, unattended tick), an
/// account whose plan is not approved, and an account with no sleeve due today (none configured).
///
/// Each [`RunSpec`] carries ALL of the account's CONFIGURED sleeves: which of them are pending on the day is decided
/// by the pipeline from the data, and the run key is over the configured set so one slot is one run.
pub fn find_due_runs(source: &dyn AccountSource, now: DateTime<Utc>) -> Result<Vec<RunSpec>, String> {
    let accounts = source.active_accounts()?;
    let mut out = Vec::new();
    for a in accounts {
        if a.envelope.status != MandateStatus::Active || !a.plan_approved {
            continue;
        }
        let Some(scheduled_for) = slot_for(&a.sleeves, now) else { continue };
        out.push(RunSpec {
            account_id: a.account_id,
            tenant_id: a.tenant_id,
            scheduled_for,
            trading_day: scheduled_for.date_naive(),
            mode: a.mode,
            sleeves: a.sleeves,
            mandate: a.mandate,
            envelope: a.envelope,
        });
    }
    Ok(out)
}

// -------------------------------------------------------------------------------------------------------------
// Per-account concurrency lock
// -------------------------------------------------------------------------------------------------------------

/// A held per-account lock. Dropping it releases the lock (a Postgres implementation drops the dedicated
/// connection the session-level advisory lock lives on, exactly like `deployment_lifecycle_scheduler.rs`'s
/// `try_acquire_tick_lock` -- so a crash mid-run frees the lock the moment the connection dies, never wedging a
/// later attempt).
pub trait AccountLock {
    /// Borrows from `&self` (a Postgres implementation's guard holds the dedicated connection the session-level
    /// lock lives on), hence the GAT rather than a plain associated type.
    type Guard<'a>
    where
        Self: 'a;
    /// `Ok(None)` means another process already holds this account's lock: the caller must not run it.
    /// `Err` means the lock could not even be asked for; the caller must treat that as "do not run it" too (fail
    /// closed, same direction as every other seam in this pipeline).
    fn try_lock<'a>(&'a self, account_id: &str) -> Result<Option<Self::Guard<'a>>, String>;
}

// -------------------------------------------------------------------------------------------------------------
// Per-account broker/data/venue-rules: constructed by the caller (real credentials, real adapters), looked up here
// -------------------------------------------------------------------------------------------------------------

/// What `run_all_due` needs for ONE account beyond the shared infra (stores, clock, notifier, kill flag, which are
/// the same object for every account since they are multi-tenant Postgres stores keyed by `tenant_id`/`account_id`
/// already). Broker and data connections are per-account (different credentials, sometimes a different venue
/// adapter entirely), so the caller builds them -- this is exactly the shape WP3.4's tenant-scoped credential
/// loader will feed, just not built yet; `run_all_due` only needs somewhere to look them up by account id.
pub struct AccountRuntime<'a> {
    pub broker: &'a dyn Broker,
    pub data: &'a dyn DataSource,
    pub venue_rules: &'a VenueRuleBook<'a>,
}

// -------------------------------------------------------------------------------------------------------------
// run_all_due
// -------------------------------------------------------------------------------------------------------------

/// Why a candidate produced no `RunRecord`. Distinct from a `RunRecord` whose OWN outcome is a refusal or a
/// failed-closed run (that is `run_once` working correctly and IS recorded) -- these are cases the pipeline was
/// never even reached for this candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DueRunError {
    /// Another process holds this account's lock right now.
    LockBusy,
    /// The lock could not be acquired (backend unreachable): fails closed, same as everything else in this crate.
    LockUnavailable(String),
    /// No `AccountRuntime` (broker/data) is registered for this account id.
    NoRuntime,
    /// `run_once` panicked. Caught so one account's adapter bug can never abort the tick for every other account;
    /// the panic message is preserved for the log line the caller writes.
    Panicked(String),
}

impl std::fmt::Display for DueRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DueRunError::LockBusy => write!(f, "DRIVER_LOCK_BUSY: another process is already running this account"),
            DueRunError::LockUnavailable(e) => write!(f, "DRIVER_LOCK_UNAVAILABLE: {e}"),
            DueRunError::NoRuntime => write!(f, "DRIVER_NO_RUNTIME: no broker/data is registered for this account"),
            DueRunError::Panicked(msg) => write!(f, "DRIVER_PANICKED: {msg}"),
        }
    }
}

/// One candidate's outcome: either it ran (a `RunRecord`, whatever that run's own outcome was) or it could not be
/// attempted at all (a [`DueRunError`], never propagated -- see the module docs on the per-row convention).
pub struct DueOutcome {
    pub spec: RunSpec,
    pub result: Result<RunRecord, DueRunError>,
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

/// Run every due candidate, in order, each independently: a lock-busy account, a missing runtime, or a panic
/// inside `run_once` for one candidate is recorded in that candidate's [`DueOutcome`] and never stops the loop.
///
/// `runtimes` is looked up by `RunSpec::account_id`; `state_store`/`runs`/`notifier`/`kill_flag`/`clock` are shared
/// across every account (multi-tenant Postgres stores partition internally by `tenant_id`/`account_id`, same as
/// every other seam in this pipeline). See the module docs for why `lock` AND the run store's own run-key together
/// are both in play, not either alone.
#[allow(clippy::too_many_arguments)]
pub fn run_all_due<'a, L: AccountLock>(
    due: Vec<RunSpec>,
    runtimes: &BTreeMap<String, AccountRuntime<'a>>,
    state_store: &dyn StateStore,
    runs: &dyn RunStore,
    notifier: &dyn Notifier,
    kill_flag: &dyn KillFlag,
    clock: &dyn Clock,
    lock: &L,
    config: &RunConfig,
) -> Vec<DueOutcome> {
    let mut out = Vec::with_capacity(due.len());
    // One evaluation memo per tick: accounts that hold the same sleeve kind share one data fetch and one decision.
    let cache = EvalCache::new();
    for spec in due {
        let guard = match lock.try_lock(&spec.account_id) {
            Ok(Some(g)) => g,
            Ok(None) => {
                out.push(DueOutcome { spec, result: Err(DueRunError::LockBusy) });
                continue;
            }
            Err(e) => {
                out.push(DueOutcome { spec, result: Err(DueRunError::LockUnavailable(e)) });
                continue;
            }
        };
        let Some(rt) = runtimes.get(&spec.account_id) else {
            drop(guard);
            out.push(DueOutcome { spec, result: Err(DueRunError::NoRuntime) });
            continue;
        };
        let venue_rules: &VenueRuleBook<'_> = rt.venue_rules;
        let ctx = RunContext {
            account_id: &spec.account_id,
            scheduled_for: spec.scheduled_for,
            trading_day: spec.trading_day,
            mode: spec.mode,
            sleeves: &spec.sleeves,
            mandate: Some(&spec.mandate),
            envelope: Some(&spec.envelope),
            broker: rt.broker,
            data: rt.data,
            state_store,
            clock,
            runs,
            notifier,
            kill_flag,
            venue_rules,
            config,
            cache: Some(&cache),
        };
        let result = panic::catch_unwind(AssertUnwindSafe(|| run_once(&ctx)));
        drop(guard);
        let result = result.map_err(|payload| DueRunError::Panicked(panic_message(&*payload)));
        out.push(DueOutcome { spec, result });
    }
    out
}

/// How many candidates actually completed the pipeline (whatever THAT run's own outcome was) vs. could not be
/// attempted at all. A log line / the service's heartbeat uses this; tests use it for the "one failure never stops
/// the others" property.
pub struct RunAllDueSummary {
    pub ran: usize,
    pub not_attempted: usize,
}

pub fn summarize(outcomes: &[DueOutcome]) -> RunAllDueSummary {
    let ran = outcomes.iter().filter(|o| o.result.is_ok()).count();
    RunAllDueSummary { ran, not_attempted: outcomes.len() - ran }
}
