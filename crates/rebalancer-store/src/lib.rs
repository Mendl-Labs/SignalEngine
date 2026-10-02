//! Postgres implementations of `rebalancer-run`'s / `rebalancer-risk`'s persistence traits, so the
//! rebalancer can run as a multi-tenant SERVICE (WP4.8 of product-mandate/IMPLEMENTATION_PLAN.md)
//! instead of only ever being driven by an in-memory test harness.
//!
//! * [`state_store::PgStateStore`] -- `rebalancer_risk::store::StateStore` (`AccountState` CAS).
//! * [`run_store::PgRunStore`] -- `rebalancer_run::stores::RunStore` (the immutable run record).
//! * [`notifier::PgNotifier`] -- `rebalancer_run::stores::Notifier`.
//! * [`kill_flag::PgKillFlag`] -- `rebalancer_run::stores::KillFlag` (global, see that module's docs).
//! * [`account_lock::PgAccountLock`] -- `rebalancer_run::driver::AccountLock`, the primary
//!   concurrency-safety mechanism `run_all_due` uses.
//! * [`account_source::PgAccountSource`] -- `rebalancer_run::driver::AccountSource` for the paper pilot: the allow-listed
//!   tenant/account(s), their active signed mandate and owner-authored pilot plan, paper-only, no broker built.
//! * [`ledger`] / `PgRunStore::last_acted_decision`: the append-only, tenant-scoped, monotone decision ledger
//!   (`D_acted`), written in the same transaction as `PgRunStore::finish`.
//! * [`latency_store::PgLatencyStore`] -- `rebalancer_run::latency::LatencyStore` (W9.1, the first-seen-latency /
//!   revision recorder; platform-level, no tenant: vendor bars are tenant-independent). Its migration lives in THIS
//!   crate (`migrations/2026-10-02-000000_create_rebalancer_bar_observations`) for the owner to copy into
//!   databaseschema-internal; see that file's header.
//! * [`tenants::AccountTenants`] -- the in-memory account-id -> tenant-id registry every store above
//!   shares, since none of the traits it implements carry a tenant id (see the migration's own file
//!   header for the full reasoning).
//! * [`pg`] -- the connection pool bootstrap and the sync/async bridge (`rebalancer-run`'s traits are
//!   deliberately synchronous; `diesel-async` is not).
//!
//! Backed by `databaseschema-internal/migrations/2026-09-22-010000_create_rebalancer_service_tables` and, for the
//! decision ledger, pilot plans and run provenance, `2026-09-27-000000_create_rebalancer_pilot_ledger`.
//! Additive only; every store here fails CLOSED on a database error (`Err`, never a silently-empty
//! success), matching every other seam in `rebalancer-run`'s pipeline.

pub mod account_lock;
pub mod account_source;
pub mod json;
pub mod kill_flag;
pub mod latency_store;
pub mod ledger;
pub mod notifier;
pub mod pg;
pub mod provenance;
pub mod run_store;
pub mod state_store;
pub mod tenants;

pub use account_lock::PgAccountLock;
pub use account_source::{AccountSourceError, Enumeration, Exclusion, ExclusionReason, PilotAccount, PilotAllowList, PgAccountSource, PlanExecution};
pub use kill_flag::PgKillFlag;
pub use latency_store::PgLatencyStore;
pub use notifier::PgNotifier;
pub use provenance::{PlanOrigin, PlanProvenance, PlanProvenanceRegistry, VenueEnvironment};
pub use run_store::PgRunStore;
pub use state_store::PgStateStore;
pub use tenants::AccountTenants;
