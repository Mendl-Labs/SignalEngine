//! Connection pool bootstrap and the sync-to-async bridge every store in this crate uses.
//!
//! `rebalancer-run`'s traits (`StateStore`, `RunStore`, `Notifier`, `KillFlag`) are deliberately
//! synchronous -- the pipeline's own module doc says "no async runtime" -- but `diesel-async` (the
//! version this crate matches, see `Cargo.toml`'s own comment) is an async client. [`Bridge`] owns a
//! dedicated current-thread Tokio runtime and `block_on`s every query on it, so a `PgStateStore` etc.
//! satisfies its trait's plain `fn` signature without the caller ever touching `async`. This is the
//! same shape `program::deployment_lifecycle_scheduler` uses at its own call sites (an async
//! `diesel_async::AsyncPgConnection` query awaited inside an async fn) -- the difference here is only
//! that `run_once`'s call stack is sync all the way down, so the `await` has to happen somewhere, and
//! it happens inside this crate rather than being pushed onto the caller.
//!
//! Each store owns its OWN `Bridge` (not a shared one): `Runtime::block_on` panics if called from
//! inside another Tokio runtime, so if a caller ever DOES run these stores from an async context, a
//! shared runtime would risk exactly that panic on the very first nested call. A dedicated
//! current-thread runtime per store avoids that class of bug entirely, at the cost of one extra OS
//! thread's worth of bookkeeping per store instance -- a handful of stores per process, not a
//! per-request cost.

use diesel_async::pooled_connection::{deadpool, AsyncDieselConnectionManager};
use diesel_async::AsyncPgConnection;

pub type Pool = deadpool::Pool<AsyncPgConnection>;

/// Build a connection pool from a `DATABASE_URL`-shaped Postgres URL. `max_size` should be at least
/// the number of stores sharing this pool plus a little headroom for the advisory-lock connections
/// `PgAccountLock` checks out and holds for the duration of one account's run.
pub fn create_pool(database_url: &str, max_size: usize) -> Result<Pool, String> {
    let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::Pool::builder(config).max_size(max_size).build().map_err(|e| format!("building the connection pool: {e}"))
}

/// The sync/async bridge described in the module docs.
pub struct Bridge {
    pool: Pool,
    rt: tokio::runtime::Runtime,
}

impl Bridge {
    pub fn new(pool: Pool) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("building the store's dedicated Tokio runtime: {e}"))?;
        Ok(Self { pool, rt })
    }

    /// Run an async block to completion on this store's dedicated runtime, checking out a pooled
    /// connection first. `f` gets the connection; its own error type must convert into `String` (every
    /// store in this crate maps a pool/connection failure straight into its trait's own
    /// `..._UNAVAILABLE` error variant).
    pub fn block_on<F, Fut, T>(&self, f: F) -> Result<T, String>
    where
        F: FnOnce(deadpool::Object<AsyncPgConnection>) -> Fut,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        self.rt.block_on(async {
            let conn = self.pool.get().await.map_err(|e| format!("checking out a connection: {e}"))?;
            f(conn).await
        })
    }

    /// Open a FRESH, UNPOOLED connection instead of checking one out of the shared pool.
    /// `PgAccountLock` needs this: a session-level `pg_advisory_lock` belongs to the connection that
    /// took it, so the connection must be held, not returned to the pool, for as long as the lock is
    /// held -- returning it to the pool would let some unrelated caller inherit a session-level lock it
    /// never asked for. Releasing the lock needs no async call back into this bridge: Postgres frees a
    /// session-level advisory lock as soon as it notices the connection is gone, so an ordinary
    /// (synchronous) `Drop` of the returned connection is enough -- the same pattern
    /// `deployment_lifecycle_scheduler.rs`'s own `try_acquire_tick_lock` uses (`drop(lock_conn)`, no
    /// explicit unlock call).
    pub fn open_dedicated(&self, database_url: &str) -> Result<AsyncPgConnection, String> {
        use diesel_async::AsyncConnection;
        let url = database_url.to_string();
        self.rt.block_on(async move { AsyncPgConnection::establish(&url).await.map_err(|e| format!("opening a dedicated connection: {e}")) })
    }

    /// Run one async block on this bridge's runtime without going through the pool at all (used by
    /// [`PgAccountLock`](crate::account_lock::PgAccountLock) to run the `pg_try_advisory_lock` query on
    /// the dedicated connection it just opened).
    pub fn block_on_raw<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        self.rt.block_on(f())
    }

    /// A cloneable handle onto this bridge's runtime, for a caller that needs to `block_on` it again
    /// later from OUTSIDE this struct's own methods -- used by `PgLockGuard`'s `Drop` (see its own doc
    /// comment for why a dedicated connection's background driver task needs one more poll on its
    /// OWN runtime to actually close its socket, not just have its handle dropped).
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.rt.handle().clone()
    }
}
