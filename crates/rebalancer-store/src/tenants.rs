//! `AccountTenants`: an in-memory `account_id -> tenant_id` registry shared by every store in this
//! crate, so `tenant_id` can be denormalized onto rows without any trait method (`StateStore::load`,
//! `RunStore::begin`, `Notifier::notify`, ...) carrying one -- see the migration's own file header
//! comment (`databaseschema-internal/migrations/2026-09-22-010000_create_rebalancer_service_tables`)
//! for the full reasoning. The service's main loop (or a test) keeps this current from the same
//! `ActiveAccount` list `find_due_runs` reads, since that list already carries `tenant_id` for every
//! account it names.

use std::collections::BTreeMap;
use std::sync::RwLock;

use uuid::Uuid;

/// Written when an account's tenant is not yet known to the registry. Chosen instead of leaving
/// `tenant_id` NULL: the column stays `NOT NULL` and meaningful ("this row's tenant was not resolved
/// at write time"), and a query can filter it out explicitly (`WHERE tenant_id <> '000...'`) rather
/// than needing a NULL-aware predicate everywhere. Should not occur in the driver loop's normal path:
/// every due account came from an `AccountSource` that already knows its `tenant_id`.
pub const UNKNOWN_TENANT: Uuid = Uuid::nil();

#[derive(Default)]
pub struct AccountTenants {
    inner: RwLock<BTreeMap<String, Uuid>>,
}

impl AccountTenants {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, account_id: &str, tenant_id: Uuid) {
        self.inner.write().unwrap_or_else(|e| e.into_inner()).insert(account_id.to_string(), tenant_id);
    }

    /// Replace the whole registry at once (the service calls this each tick with the current due-scan
    /// account list, before running any store call for those accounts).
    pub fn set_all(&self, accounts: impl IntoIterator<Item = (String, Uuid)>) {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        g.clear();
        g.extend(accounts);
    }

    /// [`UNKNOWN_TENANT`] when `account_id` has never been registered.
    pub fn get(&self, account_id: &str) -> Uuid {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).get(account_id).copied().unwrap_or(UNKNOWN_TENANT)
    }
}
