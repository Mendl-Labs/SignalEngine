//! `PgKillFlag`: the Postgres-backed [`KillFlag`], against the singleton `rebalancer_kill_flags` row
//! (AD8: "a database kill flag read at the start of every run", no deploy needed to stop it --
//! `UPDATE rebalancer_kill_flags SET is_set = true`). Deliberately global, not per-tenant: the trait's
//! own signature (`fn is_set(&self) -> Result<bool, String>`) carries no account/tenant parameter, and
//! this table is the one exception to this crate's "tenant_id on every table" convention -- see the
//! migration's own file header for why.

use diesel::sql_types::Bool;
use diesel_async::RunQueryDsl;
use rebalancer_run::stores::KillFlag;

use crate::pg::{Bridge, Pool};

#[derive(diesel::QueryableByName)]
struct FlagRow {
    #[diesel(sql_type = Bool)]
    is_set: bool,
}

pub struct PgKillFlag {
    bridge: Bridge,
}

impl PgKillFlag {
    pub fn new(pool: Pool) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)? })
    }
}

impl KillFlag for PgKillFlag {
    fn is_set(&self) -> Result<bool, String> {
        self.bridge.block_on(|mut conn| async move {
            let rows: Vec<FlagRow> = diesel::sql_query("SELECT is_set FROM rebalancer_kill_flags WHERE id = TRUE")
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("KILL_FLAG_UNREADABLE: {e}"))?;
            // The singleton row is seeded by the migration itself; a missing row is unreadable, not
            // "not set" -- the pipeline's own fail-closed contract for this trait (an `Err` means
            // "could not read it: the run fails closed", per rebalancer-run/src/stores.rs's own doc).
            rows.into_iter().next().map(|r| r.is_set).ok_or_else(|| "KILL_FLAG_UNREADABLE: the singleton row is missing".to_string())
        })
    }
}
