//! Plan provenance for run records (paper-pilot plan section 2.3 item 3): which plan a run executed
//! under and against which venue environment, so a run row can never be read without knowing it was a
//! paper-environment pilot run.
//!
//! Neither `RunStore::finish` nor `RunRecord` carries a plan, and this slice is not allowed to change the
//! pipeline, so the account source that knows the plan (`PgAccountSource`) publishes it here and
//! `PgRunStore::finish` reads it, exactly the way [`crate::tenants::AccountTenants`] carries the tenant.
//! [`VenueEnvironment`] has NO live variant on purpose: this crate cannot record a live environment, and
//! the database CHECK on `rebalancer_runs` refuses a pilot run that is not `paper`.

use std::collections::BTreeMap;
use std::sync::RwLock;

use uuid::Uuid;

/// Where a plan came from. Only the owner-authored pilot exists in this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanOrigin {
    OwnerPilot,
}

impl PlanOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            PlanOrigin::OwnerPilot => "owner_pilot",
        }
    }
}

/// The venue environment a run traded in. Paper only, by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueEnvironment {
    Paper,
}

impl VenueEnvironment {
    pub fn as_str(self) -> &'static str {
        match self {
            VenueEnvironment::Paper => "paper",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanProvenance {
    pub origin: PlanOrigin,
    pub plan_id: Uuid,
    pub venue_environment: VenueEnvironment,
}

/// `account_id -> provenance`, shared between the account source (writer) and the run store (reader).
#[derive(Default)]
pub struct PlanProvenanceRegistry {
    inner: RwLock<BTreeMap<String, PlanProvenance>>,
}

impl PlanProvenanceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the whole registry (the account source calls this on every enumeration, so an account
    /// that lost its plan stops carrying provenance).
    pub fn set_all(&self, entries: impl IntoIterator<Item = (String, PlanProvenance)>) {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        g.clear();
        g.extend(entries);
    }

    pub fn register(&self, account_id: &str, provenance: PlanProvenance) {
        self.inner.write().unwrap_or_else(|e| e.into_inner()).insert(account_id.to_string(), provenance);
    }

    pub fn get(&self, account_id: &str) -> Option<PlanProvenance> {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).get(account_id).copied()
    }
}
