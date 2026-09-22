//! The human-approval token: the ONLY thing that lets a halted account resume.
//!
//! SPEC principle 3 and `mandate.autonomy.never = [resume_after_halt]`: stopping is the fail-safe direction and any
//! automation may do it; resuming is a human act. This module makes that hard to get wrong by accident and easy to
//! audit, in three layers:
//!
//! 1. **Unconstructible type.** [`HumanApproval`] has private fields, no `Clone`, no `Default`, no serde impls and a
//!    `pub(crate)` constructor. Code outside this crate cannot build one by struct literal or by calling `new`
//!    (proved by the `compile_fail` doctests below). [`crate::state::AccountState::resume`] consumes it by value, so
//!    one approval resumes at most one halt.
//! 2. **One gated issuer.** The only public way to get one is [`issue_human_approval`], compiled only with the cargo
//!    feature `human-endpoint`. That feature is to be enabled ONLY by the crate that hosts the authenticated-human
//!    HTTP endpoint (the future "resume" API handler). The rebalancer, the agent tools and every other automation
//!    crate depend on `rebalancer-risk` WITHOUT it, so they cannot even name the function.
//! 3. **A runtime principal check.** The issuer takes an [`AuthenticatedPrincipal`] (implemented by the API layer
//!    from its verified session: a Clerk user for a person, a different kind for an API key or the agent) and
//!    refuses anything that is not [`PrincipalKind::Human`], and refuses an empty note.
//!
//! # How the future API layer obtains a token
//! The resume endpoint authenticates the caller with the platform's user session (never an API key, never the
//! agent's service principal), builds its `AuthenticatedPrincipal` from that session, and calls
//! `issue_human_approval(&principal, note, now)`. It then calls `AccountState::resume(approval, ...)` and
//! `StateStore::save`. The audit trail (who, when, note, equity) is recorded inside the state.
//!
//! # Honest limits
//! Rust cannot stop a crate that enables the feature from calling the function, and cargo unifies features across
//! one workspace build, so if the API crate and the rebalancer are built in one invocation the feature is on for
//! both. The protection is therefore: (a) the rebalancer crate never calls the function (grep-able, and pinned by
//! a test in `rebalancer-run` that no non-test code path resumes), (b) the type cannot be forged, and (c) the
//! release build of the rebalancer image is built without the feature (`cargo build -p rebalancer-run --release`
//! does not enable it). Reviewers should confirm (c) in CI.
//!
//! ```compile_fail
//! // A token cannot be built by struct literal outside this crate (private fields).
//! use rebalancer_risk::approval::HumanApproval;
//! let _forged = HumanApproval { approver: String::from("agent"), at: chrono::DateTime::UNIX_EPOCH, note: String::from("please") };
//! ```
//!
//! ```compile_fail
//! // ...nor by a constructor: `new` is pub(crate).
//! use rebalancer_risk::approval::HumanApproval;
//! let _forged = HumanApproval::new("agent", chrono::DateTime::UNIX_EPOCH, "please");
//! ```
//!
//! ```compile_fail
//! // ...and it cannot be cloned, so one approval cannot be replayed.
//! use rebalancer_risk::approval::HumanApproval;
//! fn dup(a: &HumanApproval) -> HumanApproval { a.clone() }
//! ```

use chrono::{DateTime, Utc};

/// Who is calling. Only `Human` may obtain an approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    /// A person authenticated through the platform's user session.
    Human,
    /// The agent's own principal (or any model-driven caller).
    Agent,
    /// A programmatic API key.
    ApiKey,
    /// Another service (the scheduler, the rebalancer itself, a watchdog).
    Service,
}

/// The authenticated caller, as the API layer knows it. Implemented by the API layer only.
pub trait AuthenticatedPrincipal {
    /// A stable identifier of the person (for example the Clerk user id); recorded in the audit trail.
    fn subject(&self) -> &str;
    fn kind(&self) -> PrincipalKind;
}

/// Why an approval was refused. Stable machine codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ApprovalDenied {
    #[error("APPROVAL_NOT_HUMAN: only an authenticated person may approve a resume")]
    NotHuman,
    #[error("APPROVAL_NOTE_REQUIRED: a resume needs a non-empty note recording why it is safe")]
    NoteRequired,
    #[error("APPROVAL_SUBJECT_REQUIRED: the principal has no identifier to record")]
    SubjectRequired,
}

impl ApprovalDenied {
    pub fn code(self) -> &'static str {
        match self {
            ApprovalDenied::NotHuman => "APPROVAL_NOT_HUMAN",
            ApprovalDenied::NoteRequired => "APPROVAL_NOTE_REQUIRED",
            ApprovalDenied::SubjectRequired => "APPROVAL_SUBJECT_REQUIRED",
        }
    }
}

/// A person's approval to resume one halted account. See the module docs.
#[derive(Debug)]
#[must_use = "an approval exists to be passed to AccountState::resume"]
pub struct HumanApproval {
    approver: String,
    at: DateTime<Utc>,
    note: String,
}

impl HumanApproval {
    /// Crate-private: nothing outside `rebalancer-risk` can call this.
    #[cfg_attr(not(any(test, feature = "human-endpoint")), allow(dead_code))]
    pub(crate) fn new(approver: &str, at: DateTime<Utc>, note: &str) -> Self {
        Self { approver: approver.to_string(), at, note: note.to_string() }
    }

    pub fn approver(&self) -> &str {
        &self.approver
    }

    pub fn at(&self) -> DateTime<Utc> {
        self.at
    }

    pub fn note(&self) -> &str {
        &self.note
    }
}

/// Mint an approval for an authenticated human. Compiled only with the `human-endpoint` feature (see the module
/// docs): enable it only in the crate that hosts the authenticated-human resume endpoint.
#[cfg(feature = "human-endpoint")]
pub fn issue_human_approval(
    principal: &dyn AuthenticatedPrincipal,
    note: &str,
    now: DateTime<Utc>,
) -> Result<HumanApproval, ApprovalDenied> {
    if principal.kind() != PrincipalKind::Human {
        return Err(ApprovalDenied::NotHuman);
    }
    if principal.subject().trim().is_empty() {
        return Err(ApprovalDenied::SubjectRequired);
    }
    if note.trim().is_empty() {
        return Err(ApprovalDenied::NoteRequired);
    }
    Ok(HumanApproval::new(principal.subject().trim(), now, note.trim()))
}
