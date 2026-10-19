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
//! Every `Bridge` drives its queries on ONE process-wide multi-thread runtime (see `shared_runtime`): pools are
//! shared across stores, and a pooled connection's driver task lives on the runtime that created it, so per-store
//! current-thread runtimes made a cross-store connection hang forever. Every call is also time-bounded.

use std::sync::OnceLock;
use std::time::Duration;

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

/// How long a store call may wait for a pooled connection before failing (instead of blocking the tick loop).
pub const POOL_CHECKOUT_TIMEOUT: Duration = Duration::from_secs(15);
/// How long one store call's query work may take once it has a connection.
pub const QUERY_TIMEOUT: Duration = Duration::from_secs(60);

/// THE runtime every [`Bridge`] drives its queries on. It must be ONE multi-thread runtime shared by all stores:
/// a pooled `AsyncPgConnection` spawns its socket-driver task on the runtime that CREATED it, and the pools are
/// shared across stores (`pool.clone()`). With a current-thread runtime per store, a connection created under store
/// A's runtime and later checked out by store B hangs forever (A's runtime is only polled while A is inside
/// `block_on`), which froze the rebalancer's tick loop after its first tick. Worker threads keep every driver task
/// polled no matter which store's caller is blocked.
fn shared_runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    static RT: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("rebalancer-store-rt")
            .enable_all()
            .build()
            .map_err(|e| format!("building the shared store Tokio runtime: {e}"))
    })
    .as_ref()
    .map_err(|e| e.clone())
}

/// Run `fut` but give up after `limit` with a message naming `what`.
pub async fn within<T>(limit: Duration, what: &str, fut: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
    match tokio::time::timeout(limit, fut).await {
        Ok(r) => r,
        Err(_) => Err(format!("timed out after {}s {what}", limit.as_secs())),
    }
}

/// The sync/async bridge described in the module docs.
pub struct Bridge {
    pool: Pool,
    rt: &'static tokio::runtime::Runtime,
}

impl Bridge {
    pub fn new(pool: Pool) -> Result<Self, String> {
        Ok(Self { pool, rt: shared_runtime()? })
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
            let conn = within(POOL_CHECKOUT_TIMEOUT, "waiting for a pooled connection", async { self.pool.get().await.map_err(|e| format!("checking out a connection: {e}")) }).await?;
            within(QUERY_TIMEOUT, "running the store query", f(conn)).await
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
        self.rt.block_on(async move { within(POOL_CHECKOUT_TIMEOUT, "opening a dedicated connection", async { AsyncPgConnection::establish(&url).await.map_err(|e| format!("opening a dedicated connection: {e}")) }).await })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_bridges_share_one_runtime() {
        let a = Bridge::new(create_pool("postgres://u:p@127.0.0.1:1/db", 1).unwrap()).unwrap();
        let b = Bridge::new(create_pool("postgres://u:p@127.0.0.1:1/db", 1).unwrap()).unwrap();
        assert!(std::ptr::eq(a.rt, b.rt));
    }

    /// A task spawned through store A's runtime keeps running while only store B blocks (the connection-driver case
    /// that hung with one current-thread runtime per store).
    #[test]
    fn task_spawned_via_one_bridge_progresses_while_another_blocks() {
        let a = Bridge::new(create_pool("postgres://u:p@127.0.0.1:1/db", 1).unwrap()).unwrap();
        let b = Bridge::new(create_pool("postgres://u:p@127.0.0.1:1/db", 1).unwrap()).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<u32>();
        a.handle().spawn(async move {
            let _ = tx.send(7);
        });
        let got = b.block_on_raw(|| async move { within(Duration::from_secs(10), "waiting for A's task", async { rx.await.map_err(|e| e.to_string()) }).await });
        assert_eq!(got, Ok(7));
    }

    #[test]
    fn within_times_out_instead_of_blocking_forever() {
        let b = Bridge::new(create_pool("postgres://u:p@127.0.0.1:1/db", 1).unwrap()).unwrap();
        let got: Result<(), String> = b.block_on_raw(|| async { within(Duration::from_millis(50), "never finishing", std::future::pending::<Result<(), String>>()).await });
        assert!(got.unwrap_err().contains("timed out"));
    }
}
