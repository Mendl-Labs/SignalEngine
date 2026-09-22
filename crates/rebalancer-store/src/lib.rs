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
//! * [`tenants::AccountTenants`] -- the in-memory account-id -> tenant-id registry every store above
//!   shares, since none of the traits it implements carry a tenant id (see the migration's own file
//!   header for the full reasoning).
//! * [`pg`] -- the connection pool bootstrap and the sync/async bridge (`rebalancer-run`'s traits are
//!   deliberately synchronous; `diesel-async` is not).
//!
//! Backed by `databaseschema-internal/migrations/2026-09-22-010000_create_rebalancer_service_tables`.
//! Additive only; every store here fails CLOSED on a database error (`Err`, never a silently-empty
//! success), matching every other seam in `rebalancer-run`'s pipeline.

pub mod account_lock;
pub mod json;
pub mod kill_flag;
pub mod notifier;
pub mod pg;
pub mod run_store;
pub mod state_store;
pub mod tenants;

pub use account_lock::PgAccountLock;
pub use kill_flag::PgKillFlag;
pub use notifier::PgNotifier;
pub use run_store::PgRunStore;
pub use state_store::PgStateStore;
pub use tenants::AccountTenants;
