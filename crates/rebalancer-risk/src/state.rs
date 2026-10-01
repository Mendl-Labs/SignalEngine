//! `AccountState`: the per-account risk state machine.
//!
//! ```text
//!            shrink rung hit                       halt_flatten rung / daily loss
//!   Active ------------------> Shrunk ------------------------------------------> Flattening
//!     ^   <------------------     |                                                    |
//!     |   recovery (hysteresis)   +--------- halt_flatten rung / daily loss ---------->+
//!     |                                                                                | verified flat
//!     |                                                                                v
//!     +------------------------ resume(HumanApproval) (ONLY exit) ----------------- Halted
//!
//!   Active/Shrunk --halt()--> Halted          (recon failure, manual halt: no flatten)
//! ```
//!
//! * **Flattening is transient.** It is entered when a halt requires liquidation and left only by
//!   [`AccountState::complete_flatten`] with `verified_flat = true` (to `Halted`). A failed flatten leaves the
//!   account in `Flattening` (it may not trade, it must be retried and alerted), so "Halted" always means "no
//!   exposure beyond dust" for a flatten halt.
//! * **Halted is never exited by code**, only by [`AccountState::resume`] with a [`HumanApproval`]. Every other
//!   transition function is a no-op on a `Halted` or `Flattening` account (property-tested over random paths).
//! * **The high-water mark only ratchets up while the account is Active or Shrunk.** It is frozen while halted or
//!   flattening, and reset on resume (see [`AccountState::resume`]).
//! * **Values are immutable.** Every transition returns a NEW state; the store assigns the version number when the
//!   state is saved (compare-and-swap, see `crate::store`). Fields are private so the only ways to change a state
//!   are the functions below, each of which respects the rules above.

use std::collections::BTreeSet;

use chrono::{DateTime, NaiveDate, Utc};
use rebalancer_core::Dec;

use crate::approval::HumanApproval;
use crate::overlay::EquitySnapshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccountStatus {
    /// Trading normally at full size.
    Active,
    /// A shrink rung is in force: targets are scaled by `risk_scale`.
    Shrunk,
    /// A halt is in force and the account is being liquidated. Only reducing orders may be sent.
    Flattening,
    /// A halt is in force and the account is flat (or the halt needs no flatten). Nothing may be sent.
    Halted,
}

impl AccountStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            AccountStatus::Active => "active",
            AccountStatus::Shrunk => "shrunk",
            AccountStatus::Flattening => "flattening",
            AccountStatus::Halted => "halted",
        }
    }

    /// May the rebalancer open or add to positions?
    pub fn may_add_risk(self) -> bool {
        matches!(self, AccountStatus::Active | AccountStatus::Shrunk)
    }

    /// A halt is in force (Flattening or Halted).
    pub fn is_halt(self) -> bool {
        matches!(self, AccountStatus::Flattening | AccountStatus::Halted)
    }
}

/// Why the account halted. Stable machine codes ([`HaltReason::code`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HaltReason {
    DailyLoss,
    DrawdownLadder,
    /// Reconciliation found a foreign order, unexplained drift, a missing order, a duplicate fill or a stale view.
    Reconciliation,
    /// A person or the agent asked for a halt (fail-safe direction, allowed at any autonomy level).
    Manual,
    /// The broker reported non-positive or unusable equity, or the risk arithmetic overflowed.
    EquityInvalid,
}

impl HaltReason {
    pub fn code(self) -> &'static str {
        match self {
            HaltReason::DailyLoss => "HALT_DAILY_LOSS",
            HaltReason::DrawdownLadder => "HALT_DRAWDOWN_LADDER",
            HaltReason::Reconciliation => "HALT_RECONCILIATION",
            HaltReason::Manual => "HALT_MANUAL",
            HaltReason::EquityInvalid => "HALT_EQUITY_INVALID",
        }
    }

    pub const ALL: [HaltReason; 5] = [
        HaltReason::DailyLoss,
        HaltReason::DrawdownLadder,
        HaltReason::Reconciliation,
        HaltReason::Manual,
        HaltReason::EquityInvalid,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HaltRecord {
    pub reason: HaltReason,
    pub at: DateTime<Utc>,
    /// Human-readable detail (the risk reasons or the reconciliation findings).
    pub detail: String,
    pub equity_at_halt: Option<Dec>,
    pub hwm_at_halt: Option<Dec>,
    /// How many flatten attempts have ended without verifying flat since this halt began.
    pub failed_flatten_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeRecord {
    pub approver: String,
    pub at: DateTime<Utc>,
    pub note: String,
    pub equity_at_resume: Dec,
    /// The high-water mark that was in force when the account halted (discarded by the reset).
    pub hwm_before: Option<Dec>,
    pub halt_reason: HaltReason,
    pub halted_at: DateTime<Utc>,
}

/// What a state-changing call did, for the run record and alerts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub from: AccountStatus,
    pub to: AccountStatus,
    /// Stable machine code: a `RiskCode`, a `HaltReason` code or `RESUMED`.
    pub code: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResumeError {
    #[error("RESUME_NOT_HALTED: the account is {0}, only a halted account can be resumed")]
    NotHalted(&'static str),
    #[error("RESUME_FLATTEN_INCOMPLETE: the account is still flattening; finish (or verify) the flatten first")]
    FlattenIncomplete,
    #[error("RESUME_EQUITY_INVALID: the resume equity snapshot {0} is not positive")]
    EquityInvalid(Dec),
}

impl ResumeError {
    pub fn code(&self) -> &'static str {
        match self {
            ResumeError::NotHalted(_) => "RESUME_NOT_HALTED",
            ResumeError::FlattenIncomplete => "RESUME_FLATTEN_INCOMPLETE",
            ResumeError::EquityInvalid(_) => "RESUME_EQUITY_INVALID",
        }
    }
}

/// Plain-data form of an [`AccountState`] for a persistence layer (a Postgres store maps rows to this and back).
/// `AccountState::from_record` is the trust boundary: it accepts what the database says, so only a store
/// implementation should call it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountStateRecord {
    pub account_id: String,
    pub version: u64,
    pub status: AccountStatus,
    pub risk_scale: Dec,
    pub shrink_rung: Option<usize>,
    pub hwm: Option<Dec>,
    pub day_start_equity: Option<Dec>,
    pub trading_day: Option<NaiveDate>,
    pub last_equity: Option<Dec>,
    pub last_equity_at: Option<DateTime<Utc>>,
    pub halt: Option<HaltRecord>,
    pub resumes: Vec<ResumeRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountState {
    account_id: String,
    version: u64,
    status: AccountStatus,
    risk_scale: Dec,
    shrink_rung: Option<usize>,
    hwm: Option<Dec>,
    day_start_equity: Option<Dec>,
    trading_day: Option<NaiveDate>,
    last_equity: Option<Dec>,
    last_equity_at: Option<DateTime<Utc>>,
    halt: Option<HaltRecord>,
    resumes: Vec<ResumeRecord>,
}

fn one() -> Dec {
    Dec::from_i64(1)
}

impl AccountState {
    /// A fresh account: Active at full size, version 0 (the version of a state that was never saved).
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            version: 0,
            status: AccountStatus::Active,
            risk_scale: one(),
            shrink_rung: None,
            hwm: None,
            day_start_equity: None,
            trading_day: None,
            last_equity: None,
            last_equity_at: None,
            halt: None,
            resumes: Vec::new(),
        }
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }
    /// The compare-and-swap version. Assigned by the store on save; a never-saved state has version 0.
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn status(&self) -> AccountStatus {
        self.status
    }
    /// The multiplier the planner must apply to every target: 1 when Active, the rung's scale when Shrunk, and 0
    /// when Flattening or Halted (nothing may be added).
    pub fn risk_scale(&self) -> Dec {
        match self.status {
            AccountStatus::Active | AccountStatus::Shrunk => self.risk_scale,
            AccountStatus::Flattening | AccountStatus::Halted => Dec::ZERO,
        }
    }
    /// Index (into the mandate's ladder) of the shrink rung in force, if any.
    pub fn shrink_rung(&self) -> Option<usize> {
        self.shrink_rung
    }
    pub fn hwm(&self) -> Option<Dec> {
        self.hwm
    }
    pub fn day_start_equity(&self) -> Option<Dec> {
        self.day_start_equity
    }
    pub fn trading_day(&self) -> Option<NaiveDate> {
        self.trading_day
    }
    pub fn last_equity(&self) -> Option<Dec> {
        self.last_equity
    }
    pub fn last_equity_at(&self) -> Option<DateTime<Utc>> {
        self.last_equity_at
    }
    pub fn halt_record(&self) -> Option<&HaltRecord> {
        self.halt.as_ref()
    }
    pub fn resumes(&self) -> &[ResumeRecord] {
        &self.resumes
    }

    // ----------------------------------------------------------------------------------------------------------
    // Persistence boundary
    // ----------------------------------------------------------------------------------------------------------

    pub fn to_record(&self) -> AccountStateRecord {
        AccountStateRecord {
            account_id: self.account_id.clone(),
            version: self.version,
            status: self.status,
            risk_scale: self.risk_scale,
            shrink_rung: self.shrink_rung,
            hwm: self.hwm,
            day_start_equity: self.day_start_equity,
            trading_day: self.trading_day,
            last_equity: self.last_equity,
            last_equity_at: self.last_equity_at,
            halt: self.halt.clone(),
            resumes: self.resumes.clone(),
        }
    }

    /// Rebuild a state from stored data. Refuses an incoherent record (a halt status without a halt record, a
    /// non-halt status with one, a scale outside (0, 1]) rather than trusting it.
    pub fn from_record(r: AccountStateRecord) -> Result<Self, String> {
        if r.status.is_halt() != r.halt.is_some() {
            return Err(format!("status {} and halt record presence disagree", r.status.as_str()));
        }
        if r.risk_scale <= Dec::ZERO || r.risk_scale > one() {
            return Err(format!("risk_scale {} is outside (0, 1]", r.risk_scale));
        }
        if (r.status == AccountStatus::Shrunk) != r.shrink_rung.is_some() {
            return Err("shrink_rung must be present exactly when the status is shrunk".to_string());
        }
        Ok(Self {
            account_id: r.account_id,
            version: r.version,
            status: r.status,
            risk_scale: r.risk_scale,
            shrink_rung: r.shrink_rung,
            hwm: r.hwm,
            day_start_equity: r.day_start_equity,
            trading_day: r.trading_day,
            last_equity: r.last_equity,
            last_equity_at: r.last_equity_at,
            halt: r.halt,
            resumes: r.resumes,
        })
    }

    /// Set by stores when they persist a state: the new version. Crate-private on purpose.
    pub(crate) fn with_version(mut self, version: u64) -> Self {
        self.version = version;
        self
    }

    /// Is `new` a legal successor of `old`? Stores call this before persisting (defence in depth): leaving a halt
    /// (`Flattening`/`Halted` to `Active`/`Shrunk`) is legal only when exactly one resume record was appended,
    /// and `Halted` can only be reached from `Flattening` or by a direct halt from `Active`/`Shrunk`.
    pub fn transition_allowed(old: &AccountState, new: &AccountState) -> bool {
        if old.account_id != new.account_id {
            return false;
        }
        if old.status.is_halt() && new.status.may_add_risk() {
            return new.resumes.len() == old.resumes.len() + 1 && old.resumes == new.resumes[..old.resumes.len()];
        }
        if old.status == AccountStatus::Halted && new.status == AccountStatus::Flattening {
            return false;
        }
        // The resume history is append-only.
        new.resumes.len() >= old.resumes.len() && old.resumes[..] == new.resumes[..old.resumes.len()]
    }

    // ----------------------------------------------------------------------------------------------------------
    // Observation
    // ----------------------------------------------------------------------------------------------------------

    /// Fold in a broker-reported equity snapshot and the caller's account-local trading day:
    /// * `last_equity` is updated;
    /// * the high-water mark ratchets up (or is set on the first observation) ONLY while Active or Shrunk;
    /// * when `day` is later than the recorded trading day, `day_start_equity` becomes this equity (the first
    ///   observation of the day is the day's start). A `day` earlier than the recorded one is ignored (a clock that
    ///   went backwards must not re-base the daily-loss reference).
    ///
    /// Design note: day-start is "equity at the first run of the day", not the previous close. A loss that happens
    /// between the last run of one day and the first run of the next is caught by the drawdown ladder, not by the
    /// daily-loss limit.
    pub fn observe(&self, snapshot: &EquitySnapshot, day: NaiveDate) -> AccountState {
        let mut s = self.clone();
        let equity = snapshot.equity();
        s.last_equity = Some(equity);
        s.last_equity_at = Some(snapshot.at());
        if s.status.may_add_risk() {
            s.hwm = Some(match s.hwm {
                Some(h) if h >= equity => h,
                _ => equity,
            });
        }
        let new_day = match s.trading_day {
            None => true,
            Some(d) => day > d,
        };
        if new_day {
            s.trading_day = Some(day);
            s.day_start_equity = Some(equity);
        }
        s
    }

    // ----------------------------------------------------------------------------------------------------------
    // Halting (fail-safe direction: any automation may call these)
    // ----------------------------------------------------------------------------------------------------------

    /// Halt WITHOUT flattening (reconciliation failure, manual halt). `Active`/`Shrunk` to `Halted`. A no-op when
    /// already halted or flattening (the first halt reason is kept; a flatten in progress must finish).
    pub fn halt(&self, reason: HaltReason, detail: &str, at: DateTime<Utc>) -> (AccountState, Option<Transition>) {
        if self.status.is_halt() {
            return (self.clone(), None);
        }
        let mut s = self.clone();
        let from = s.status;
        s.status = AccountStatus::Halted;
        s.halt = Some(self.new_halt_record(reason, detail, at));
        (s, Some(Transition { from, to: AccountStatus::Halted, code: reason.code() }))
    }

    /// Halt and start liquidating: `Active`/`Shrunk` to `Flattening`. A no-op when already halted or flattening.
    pub fn begin_flatten(&self, reason: HaltReason, detail: &str, at: DateTime<Utc>) -> (AccountState, Option<Transition>) {
        if self.status.is_halt() {
            return (self.clone(), None);
        }
        let mut s = self.clone();
        let from = s.status;
        s.status = AccountStatus::Flattening;
        s.halt = Some(self.new_halt_record(reason, detail, at));
        (s, Some(Transition { from, to: AccountStatus::Flattening, code: reason.code() }))
    }

    /// End a flatten attempt. `verified_flat = true` moves `Flattening` to `Halted`; `false` keeps `Flattening`
    /// and counts the failed attempt (the caller alerts and retries on the next run). A no-op in any other status.
    pub fn complete_flatten(&self, verified_flat: bool) -> (AccountState, Option<Transition>) {
        if self.status != AccountStatus::Flattening {
            return (self.clone(), None);
        }
        let mut s = self.clone();
        if verified_flat {
            s.status = AccountStatus::Halted;
            let code = s.halt.as_ref().map_or("HALT_UNKNOWN", |h| h.reason.code());
            (s, Some(Transition { from: AccountStatus::Flattening, to: AccountStatus::Halted, code }))
        } else {
            if let Some(h) = s.halt.as_mut() {
                h.failed_flatten_attempts = h.failed_flatten_attempts.saturating_add(1);
            }
            (s, None)
        }
    }

    fn new_halt_record(&self, reason: HaltReason, detail: &str, at: DateTime<Utc>) -> HaltRecord {
        HaltRecord {
            reason,
            at,
            detail: detail.to_string(),
            equity_at_halt: self.last_equity,
            hwm_at_halt: self.hwm,
            failed_flatten_attempts: 0,
        }
    }

    // ----------------------------------------------------------------------------------------------------------
    // Shrink posture (called by `overlay::apply_decision`)
    // ----------------------------------------------------------------------------------------------------------

    /// Enter or keep a shrink posture. Only valid for Active/Shrunk; a no-op otherwise.
    pub(crate) fn with_shrink(&self, scale: Dec, rung: usize) -> (AccountState, Option<Transition>) {
        if !self.status.may_add_risk() {
            return (self.clone(), None);
        }
        let mut s = self.clone();
        let from = s.status;
        s.status = AccountStatus::Shrunk;
        s.risk_scale = scale;
        s.shrink_rung = Some(rung);
        let t = (from != AccountStatus::Shrunk).then_some(Transition { from, to: AccountStatus::Shrunk, code: "RISK_DRAWDOWN_SHRINK" });
        (s, t)
    }

    /// Return to full size after a recovery. Only valid for Active/Shrunk; a no-op otherwise.
    pub(crate) fn with_full_size(&self) -> (AccountState, Option<Transition>) {
        if !self.status.may_add_risk() {
            return (self.clone(), None);
        }
        let mut s = self.clone();
        let from = s.status;
        s.status = AccountStatus::Active;
        s.risk_scale = one();
        s.shrink_rung = None;
        let t = (from != AccountStatus::Active).then_some(Transition { from, to: AccountStatus::Active, code: "RISK_RECOVERED" });
        (s, t)
    }

    // ----------------------------------------------------------------------------------------------------------
    // Resume: the only way out of a halt
    // ----------------------------------------------------------------------------------------------------------

    /// Resume a halted account. Requires a [`HumanApproval`] (see `crate::approval`), consumed here.
    ///
    /// What resume does, all documented policy a reviewer must confirm:
    /// * status becomes `Active` at full size (`risk_scale` 1); any shrink posture is discarded;
    /// * the **high-water mark is reset to the equity at resume**. Keeping the old mark would make the account
    ///   look ~20% under water and re-halt on the very next run; the person who resumes has looked at the account
    ///   and accepts the loss as the new baseline. The old mark is kept in the resume record;
    /// * the **day-start equity is re-based** to the equity at resume, for the trading day `day`, so the daily-loss
    ///   limit measures from the resume;
    /// * the halt record is cleared and a [`ResumeRecord`] (who, when, note, equity, old mark, halt reason) is
    ///   appended to the history.
    ///
    /// Refuses (`ResumeError`) unless the status is `Halted`: an account still `Flattening` must finish the flatten
    /// first, and an account that is not halted has nothing to resume.
    pub fn resume(
        &self,
        approval: HumanApproval,
        equity: &EquitySnapshot,
        day: NaiveDate,
    ) -> Result<(AccountState, Transition), ResumeError> {
        match self.status {
            AccountStatus::Halted => {}
            AccountStatus::Flattening => return Err(ResumeError::FlattenIncomplete),
            other => return Err(ResumeError::NotHalted(other.as_str())),
        }
        if !equity.equity().is_positive() {
            return Err(ResumeError::EquityInvalid(equity.equity()));
        }
        let halt = self.halt.clone().ok_or(ResumeError::NotHalted("halted without a halt record"))?;
        let mut s = self.clone();
        s.status = AccountStatus::Active;
        s.risk_scale = one();
        s.shrink_rung = None;
        s.hwm = Some(equity.equity());
        s.day_start_equity = Some(equity.equity());
        s.trading_day = Some(day);
        s.last_equity = Some(equity.equity());
        s.last_equity_at = Some(equity.at());
        s.halt = None;
        s.resumes.push(ResumeRecord {
            approver: approval.approver().to_string(),
            at: approval.at(),
            note: approval.note().to_string(),
            equity_at_resume: equity.equity(),
            hwm_before: halt.hwm_at_halt,
            halt_reason: halt.reason,
            halted_at: halt.at,
        });
        Ok((s, Transition { from: AccountStatus::Halted, to: AccountStatus::Active, code: "RESUMED" }))
    }
}

/// Every stable machine code this crate can emit, for pinning in tests and for alert routing tables.
pub fn all_state_codes() -> BTreeSet<&'static str> {
    let mut v: BTreeSet<&'static str> = HaltReason::ALL.iter().map(|r| r.code()).collect();
    v.extend(["RESUMED", "RESUME_NOT_HALTED", "RESUME_FLATTEN_INCOMPLETE", "RESUME_EQUITY_INVALID"]);
    v
}
