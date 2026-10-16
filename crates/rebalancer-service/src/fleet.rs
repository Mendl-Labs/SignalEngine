//! The set of per-account runtimes, kept current while the service runs (`REBALANCER_SERVE_PROPOSAL_PLANS=true`).
//!
//! Before this module the service built its account runtimes ONCE at startup, so a plan approved later never ran
//! until a restart and zero eligible accounts at startup crash-looped the pod. [`Fleet::reconcile`] is the periodic
//! re-enumeration: given the accounts the source enumerates NOW, it
//! * keeps the runtime of every account whose plan is unchanged,
//! * builds a runtime (credential fetch, connect, verify) for each newly eligible account,
//! * rebuilds a runtime whose plan was replaced (its asset table was refreshed for the old plan's sleeves),
//! * drops the runtime of every account no longer eligible,
//! * and treats a connect failure as that one account's problem: it gets no runtime, a typed [`Failure`] is
//!   reported (the caller logs it and alerts once per failure streak), and the next attempt is deferred with an
//!   exponential backoff. No account's failure stops another account from being reconciled.
//!
//! The fleet is generic over the runtime type `R` (a [`crate::runtime::PilotAlpaca`] in the service, a string in
//! tests) and holds no credential: `connect` is a closure the caller supplies, and the failures it reports carry a
//! stable code and a redaction-safe detail (the `Display` of the typed errors, which never contain a key).
//!
//! [`ReenumerateSchedule`] decides when a cycle is due, and [`reenumerate_secs_from_lookup`] parses
//! `REBALANCER_REENUMERATE_SECS` (default 300).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rebalancer_store::{PilotAccount, PlanOrigin};
use uuid::Uuid;

use crate::credentials::{CredentialError, PaperAlpacaKeys, PaperCredentialProvider};
use crate::runtime::RuntimeError;

pub const ENV_REENUMERATE_SECS: &str = "REBALANCER_REENUMERATE_SECS";
pub const DEFAULT_REENUMERATE_SECS: u64 = 300;
/// The longest a failing account waits between attempts: one hour.
pub const MAX_BACKOFF_SECS: i64 = 3600;

/// Why one account could not get a runtime this cycle. `code` is stable; `detail` never contains a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectFailure {
    pub code: String,
    pub detail: String,
}

impl fmt::Display for ConnectFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

impl ConnectFailure {
    pub fn from_credential(e: &CredentialError) -> Self {
        let code = match e {
            CredentialError::Missing(_) => "CREDENTIALS_MISSING".to_string(),
            CredentialError::NotProvided { reason, .. } => (*reason).to_string(),
        };
        Self { code, detail: e.to_string() }
    }

    pub fn from_runtime(e: &RuntimeError) -> Self {
        let code = match e {
            RuntimeError::Connect(_) => "PILOT_RUNTIME_CONNECT",
            RuntimeError::FingerprintMismatch { .. } => "PILOT_RUNTIME_FINGERPRINT_MISMATCH",
            RuntimeError::Verify(_) => "PILOT_RUNTIME_VERIFY",
            RuntimeError::AssetRefresh { .. } => "PILOT_RUNTIME_ASSET_REFRESH",
        };
        Self { code: code.to_string(), detail: e.to_string() }
    }
}

/// Fetch the account's paper key through `credentials`, then hand it to `build` (the connect step). Either step
/// failing is a typed [`ConnectFailure`]; the keys never leave this function except into `build`.
pub fn connect_account<R>(
    account: &PilotAccount,
    credentials: &dyn PaperCredentialProvider,
    build: impl FnOnce(&PaperAlpacaKeys, &PilotAccount) -> Result<R, RuntimeError>,
) -> Result<R, ConnectFailure> {
    let keys = credentials.paper_alpaca_keys(&account.active.tenant_id, &account.active.account_id).map_err(|e| ConnectFailure::from_credential(&e))?;
    build(&keys, account).map_err(|e| ConnectFailure::from_runtime(&e))
}

/// Delay before the next attempt for an account that has failed `consecutive` times in a row: `base * 2^(n-1)`,
/// capped at `max`.
pub fn backoff_secs(base_secs: i64, max_secs: i64, consecutive: u32) -> i64 {
    let shift = consecutive.saturating_sub(1).min(20);
    base_secs.max(1).saturating_mul(1i64 << shift).min(max_secs.max(1))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FailState {
    plan_id: Uuid,
    consecutive: u32,
    next_attempt: DateTime<Utc>,
}

/// One account that failed to get a runtime in a cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub account_id: String,
    pub tenant_id: String,
    pub plan_id: Uuid,
    pub origin: PlanOrigin,
    pub failure: ConnectFailure,
    pub consecutive: u32,
    /// True on the first failure of a streak: the caller raises its alert on this one only.
    pub first_in_streak: bool,
    pub retry_at: DateTime<Utc>,
}

/// What one [`Fleet::reconcile`] did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReconcileReport {
    pub added: Vec<String>,
    /// Accounts whose plan changed and whose runtime was rebuilt (also counted in `added`).
    pub replaced: Vec<String>,
    pub dropped: Vec<String>,
    pub failed: Vec<Failure>,
    /// Accounts skipped this cycle because their backoff has not elapsed.
    pub deferred: Vec<String>,
    pub kept: usize,
}

impl ReconcileReport {
    pub fn summary_line(&self, fleet_len: usize) -> String {
        format!(
            "re-enumerated: {fleet_len} runtime(s) now; +{} added ({} replaced), -{} dropped, {} kept, {} failed, {} deferred by backoff",
            self.added.len(),
            self.replaced.len(),
            self.dropped.len(),
            self.kept,
            self.failed.len(),
            self.deferred.len()
        )
    }
}

struct Slot<R> {
    runtime: R,
    plan_id: Uuid,
}

/// The live runtimes, keyed by account id, plus the failure/backoff bookkeeping. See the module docs.
pub struct Fleet<R> {
    slots: BTreeMap<String, Slot<R>>,
    failing: BTreeMap<String, FailState>,
    backoff_base_secs: i64,
}

impl<R> Fleet<R> {
    /// `backoff_base_secs`: the delay after a first failure (the service passes the re-enumeration interval, so the
    /// first retry is the next cycle); it doubles per consecutive failure up to [`MAX_BACKOFF_SECS`].
    pub fn new(backoff_base_secs: i64) -> Self {
        Self { slots: BTreeMap::new(), failing: BTreeMap::new(), backoff_base_secs }
    }

    /// Seed with runtimes the startup already built: `(account_id, plan_id, runtime)`.
    pub fn with_connected(backoff_base_secs: i64, connected: impl IntoIterator<Item = (String, Uuid, R)>) -> Self {
        let mut f = Self::new(backoff_base_secs);
        for (account_id, plan_id, runtime) in connected {
            f.slots.insert(account_id, Slot { runtime, plan_id });
        }
        f
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn contains(&self, account_id: &str) -> bool {
        self.slots.contains_key(account_id)
    }

    pub fn get(&self, account_id: &str) -> Option<&R> {
        self.slots.get(account_id).map(|s| &s.runtime)
    }

    /// `(account id, runtime)` in account-id order.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &R)> {
        self.slots.iter().map(|(k, s)| (k, &s.runtime))
    }

    /// Bring the fleet in line with `accounts` (what the source enumerates now). `connect` builds one account's
    /// runtime; it is called only for accounts that need one and are not in backoff.
    pub fn reconcile(&mut self, accounts: &[PilotAccount], now: DateTime<Utc>, mut connect: impl FnMut(&PilotAccount) -> Result<R, ConnectFailure>) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        let wanted: BTreeSet<&str> = accounts.iter().map(|a| a.active.account_id.as_str()).collect();

        let gone: Vec<String> = self.slots.keys().filter(|k| !wanted.contains(k.as_str())).cloned().collect();
        for id in gone {
            self.slots.remove(&id);
            report.dropped.push(id);
        }
        self.failing.retain(|k, _| wanted.contains(k.as_str()));

        for account in accounts {
            let id = account.active.account_id.clone();
            let mut replaced = false;
            if let Some(slot) = self.slots.get(&id) {
                if slot.plan_id == account.plan_id {
                    report.kept += 1;
                    continue;
                }
                // The plan changed: the old runtime was verified against the old plan's sleeves. Rebuild, never reuse.
                self.slots.remove(&id);
                replaced = true;
            }
            if let Some(f) = self.failing.get(&id) {
                if f.plan_id != account.plan_id {
                    self.failing.remove(&id);
                } else if now < f.next_attempt {
                    report.deferred.push(id);
                    continue;
                }
            }
            match connect(account) {
                Ok(runtime) => {
                    self.failing.remove(&id);
                    self.slots.insert(id.clone(), Slot { runtime, plan_id: account.plan_id });
                    if replaced {
                        report.replaced.push(id.clone());
                    }
                    report.added.push(id);
                }
                Err(failure) => {
                    let consecutive = self.failing.get(&id).map_or(0, |f| f.consecutive) + 1;
                    let retry_at = now + ChronoDuration::seconds(backoff_secs(self.backoff_base_secs, MAX_BACKOFF_SECS, consecutive));
                    self.failing.insert(id.clone(), FailState { plan_id: account.plan_id, consecutive, next_attempt: retry_at });
                    report.failed.push(Failure {
                        account_id: id,
                        tenant_id: account.active.tenant_id.clone(),
                        plan_id: account.plan_id,
                        origin: account.origin,
                        failure,
                        consecutive,
                        first_in_streak: consecutive == 1,
                        retry_at,
                    });
                }
            }
        }
        report
    }
}

/// `REBALANCER_REENUMERATE_SECS`: unset or blank is [`DEFAULT_REENUMERATE_SECS`]; anything that is not a positive
/// integer is an error (the service refuses to start, like every other malformed setting).
pub fn reenumerate_secs_from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<u64, String> {
    match lookup(ENV_REENUMERATE_SECS).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        None => Ok(DEFAULT_REENUMERATE_SECS),
        Some(v) => match v.parse::<u64>() {
            Ok(n) if n > 0 => Ok(n),
            _ => Err(format!("{ENV_REENUMERATE_SECS} is {v:?}; it must be a positive whole number of seconds")),
        },
    }
}

/// When the next re-enumeration is due. The service enumerates at startup, so that moment counts as the last one.
#[derive(Debug, Clone)]
pub struct ReenumerateSchedule {
    interval: ChronoDuration,
    last: DateTime<Utc>,
}

impl ReenumerateSchedule {
    pub fn new(interval_secs: u64, started_at: DateTime<Utc>) -> Self {
        Self { interval: ChronoDuration::seconds(interval_secs.min(1_000_000_000_000) as i64), last: started_at }
    }

    /// True (and the schedule advances) when at least one interval has passed since the last enumeration.
    pub fn due(&mut self, now: DateTime<Utc>) -> bool {
        if now - self.last < self.interval {
            return false;
        }
        self.last = now;
        true
    }
}
