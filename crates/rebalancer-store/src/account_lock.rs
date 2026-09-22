//! `PgAccountLock`: the Postgres-backed [`AccountLock`] (`rebalancer_run::driver::AccountLock`), the
//! PRIMARY concurrency-safety mechanism `run_all_due` uses to stop two processes (two service
//! replicas, or an overlapping tick) from both running the same account at once -- see
//! `rebalancer-run/src/driver.rs`'s own module doc for why this AND the run store's run-key
//! uniqueness are both in play.
//!
//! A lease ROW in `rebalancer_account_locks`, acquired with an
//! `INSERT ... ON CONFLICT (account_id) DO UPDATE ... WHERE locked_until < NOW()` (only succeeds when
//! there is no row for this account, or the row's lease has expired) and released with a plain
//! `DELETE ... WHERE account_id = $1 AND locked_by = $2` -- the same kind of pooled-connection call
//! every other store in this crate already makes reliably (`PgStateStore`, `PgRunStore`, `PgNotifier`,
//! `PgKillFlag`), none of which needs a dedicated connection or special `Drop` handling. See the
//! migration's own file header
//! (`databaseschema-internal/migrations/2026-09-22-020000_create_rebalancer_account_locks`) for why
//! this replaced an earlier session-level `pg_advisory_lock` design: releasing that reliably from a
//! synchronous `Drop`, on the small dedicated Tokio runtime this crate's traits require (see `pg.rs`),
//! turned out to be unreliable (this crate's own round-trip test caught it hanging) -- a lease row
//! sidesteps that whole class of problem by never needing a dedicated connection at all.
//!
//! `locked_until` is the crash-safety backstop: if a process dies holding the lock (never reaching its
//! `Drop`), the lease still expires on its own and a later `try_lock` reclaims it -- no operator action
//! needed, matching AD8's "no deploy needed" spirit. `locked_by` is a random per-acquisition token so a
//! guard's release can only ever delete the SPECIFIC lease it itself took, never a different, later
//! holder's (the case where this guard's own lease expired and someone else already reclaimed it before
//! this guard got around to releasing).

use diesel::sql_types::Text;
use diesel_async::RunQueryDsl;
use rebalancer_run::driver::AccountLock;

use crate::pg::{Bridge, Pool};

/// How long a lease is held before it is eligible for reclaim by a later `try_lock`, if the holder
/// never releases it (crash, panic before `Drop` runs is still fine -- `Drop` runs on an ordinary
/// panic unwind; this backstop is for a harder crash: SIGKILL, a lost VM, power loss). Generous on
/// purpose: it only matters when a process died mid-run, and a too-short lease would let a second
/// process start work on an account the first is still (slowly) running.
const LEASE_SECS: i64 = 3600;

#[derive(diesel::QueryableByName)]
struct AcquiredRow {
    /// Unused beyond confirming the `RETURNING` clause produced a row (query success == acquired);
    /// kept as a real column (not `SELECT 1`) so a reviewer can see the query's own intent -- "did a
    /// row for THIS account come back" -- at the SQL call site.
    #[diesel(sql_type = Text)]
    #[allow(dead_code)]
    account_id: String,
}

/// A random per-acquisition token. Built from `Uuid::new_v4` (already a direct dependency of this
/// crate for `tenant_id`) rather than adding a `rand` dependency for one call site.
fn random_token() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

pub struct PgAccountLock {
    bridge: Bridge,
}

impl PgAccountLock {
    pub fn new(database_url: impl Into<String>) -> Result<Self, String> {
        let pool = crate::pg::create_pool(&database_url.into(), 4)?;
        Self::with_pool(pool)
    }

    pub fn with_pool(pool: Pool) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)? })
    }
}

pub struct PgLockGuard<'a> {
    lock: &'a PgAccountLock,
    account_id: String,
    token: String,
}

impl Drop for PgLockGuard<'_> {
    fn drop(&mut self) {
        let account_id = self.account_id.clone();
        let token = self.token.clone();
        // Best-effort: never panic out of a `Drop`. A failed release still self-heals once
        // `locked_until` passes (see the module docs on the crash-safety backstop).
        let _ = self.lock.bridge.block_on(move |mut conn| async move {
            diesel::sql_query("DELETE FROM rebalancer_account_locks WHERE account_id = $1 AND locked_by = $2")
                .bind::<Text, _>(&account_id)
                .bind::<Text, _>(&token)
                .execute(&mut conn)
                .await
                .map(|_| ())
                .map_err(|e| format!("release: {e}"))
        });
    }
}

impl AccountLock for PgAccountLock {
    type Guard<'a> = PgLockGuard<'a>;

    fn try_lock<'a>(&'a self, account_id: &str) -> Result<Option<Self::Guard<'a>>, String> {
        let token = random_token();
        let account_id_owned = account_id.to_string();
        let token_for_query = token.clone();
        let acquired: Vec<AcquiredRow> = self.bridge.block_on(move |mut conn| async move {
            diesel::sql_query(
                "INSERT INTO rebalancer_account_locks (account_id, locked_by, locked_until, updated_at) \
                 VALUES ($1, $2, NOW() + ($3 || ' seconds')::interval, NOW()) \
                 ON CONFLICT (account_id) DO UPDATE SET \
                     locked_by = EXCLUDED.locked_by, locked_until = EXCLUDED.locked_until, updated_at = NOW() \
                 WHERE rebalancer_account_locks.locked_until < NOW() \
                 RETURNING account_id",
            )
            .bind::<Text, _>(&account_id_owned)
            .bind::<Text, _>(&token_for_query)
            .bind::<Text, _>(LEASE_SECS.to_string())
            .get_results(&mut conn)
            .await
            .map_err(|e| format!("acquire: {e}"))
        })?;

        if acquired.is_empty() {
            return Ok(None);
        }
        Ok(Some(PgLockGuard { lock: self, account_id: account_id.to_string(), token }))
    }
}
