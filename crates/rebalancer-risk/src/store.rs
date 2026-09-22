//! The state store: compare-and-swap persistence of [`AccountState`].
//!
//! The contract (a Postgres implementation will use `UPDATE ... SET version = version + 1 WHERE account_id = $1 AND
//! version = $2` and check the row count):
//! * [`StateStore::load`] returns the stored state, or `None` for an account that has never been saved.
//! * [`StateStore::save`]`(expected_version, new_state)` succeeds only when the stored version equals
//!   `expected_version` (a never-saved account is version 0). It stores `new_state` with version
//!   `expected_version + 1` and returns that stored state. Otherwise it returns
//!   [`StoreError::VersionConflict`] and changes nothing. So two runs that read version 7 and both try to save
//!   cannot both win: exactly one does, the other must reload and re-evaluate.
//! * A store SHOULD refuse an illegal transition ([`AccountState::transition_allowed`]): leaving a halt without
//!   exactly one appended resume record. The in-memory store does.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::state::AccountState;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("STORE_VERSION_CONFLICT: expected version {expected} but the stored version is {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("STORE_ILLEGAL_TRANSITION: {0}")]
    IllegalTransition(String),
    #[error("STORE_UNAVAILABLE: {0}")]
    Unavailable(String),
}

impl StoreError {
    pub fn code(&self) -> &'static str {
        match self {
            StoreError::VersionConflict { .. } => "STORE_VERSION_CONFLICT",
            StoreError::IllegalTransition(_) => "STORE_ILLEGAL_TRANSITION",
            StoreError::Unavailable(_) => "STORE_UNAVAILABLE",
        }
    }
}

pub trait StateStore {
    fn load(&self, account_id: &str) -> Result<Option<AccountState>, StoreError>;
    fn save(&self, expected_version: u64, new_state: &AccountState) -> Result<AccountState, StoreError>;
}

#[derive(Default)]
struct Inner {
    states: BTreeMap<String, AccountState>,
    /// Test hook: fail the next `n` calls (loads and saves) with `Unavailable`.
    fail_next: u32,
    saves: u64,
}

/// In-memory store for tests and single-process use. Thread-safe.
#[derive(Default)]
pub struct InMemoryStateStore {
    inner: Mutex<Inner>,
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make the next `n` store calls fail with `Unavailable` (fail-closed drills).
    pub fn fail_next_calls(&self, n: u32) {
        self.lock().fail_next = n;
    }

    /// Number of successful saves so far.
    pub fn saves(&self) -> u64 {
        self.lock().saves
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl StateStore for InMemoryStateStore {
    fn load(&self, account_id: &str) -> Result<Option<AccountState>, StoreError> {
        let mut g = self.lock();
        if g.fail_next > 0 {
            g.fail_next -= 1;
            return Err(StoreError::Unavailable("injected failure".into()));
        }
        Ok(g.states.get(account_id).cloned())
    }

    fn save(&self, expected_version: u64, new_state: &AccountState) -> Result<AccountState, StoreError> {
        let mut g = self.lock();
        if g.fail_next > 0 {
            g.fail_next -= 1;
            return Err(StoreError::Unavailable("injected failure".into()));
        }
        let key = new_state.account_id().to_string();
        let actual = g.states.get(&key).map_or(0, AccountState::version);
        if actual != expected_version {
            return Err(StoreError::VersionConflict { expected: expected_version, actual });
        }
        if let Some(old) = g.states.get(&key) {
            if !AccountState::transition_allowed(old, new_state) {
                return Err(StoreError::IllegalTransition(format!(
                    "{} to {} without a recorded human resume",
                    old.status().as_str(),
                    new_state.status().as_str()
                )));
            }
        }
        let stored = new_state.clone().with_version(expected_version + 1);
        g.states.insert(key, stored.clone());
        g.saves += 1;
        Ok(stored)
    }
}
