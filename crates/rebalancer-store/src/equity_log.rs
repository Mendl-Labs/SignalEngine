//! `PgEquityLog`: the Postgres-backed [`EquityLog`], against `rebalancer_equity_snapshots`. One plain INSERT per
//! continuous-monitor reading, with `source = 'monitor'` and no `run_key` (a monitor reading belongs to no run).
//!
//! The table is append-only in the database (`databaseschema-internal` migration
//! `2026-10-22-173107_lock_rebalancer_trade_history`: UPDATE, DELETE and TRUNCATE are refused by trigger, and the
//! `rebalancer_svc` role holds only SELECT and INSERT on it). This store only ever INSERTs. The run store's own
//! `pre_run` / `post_run` rows (`run_store.rs`, `insert_equity_snapshots`) are untouched, and the recon baseline is
//! read from `rebalancer_runs`, never from this table, so monitor rows cannot change what a run reconciles against.
//!
//! An account whose tenant is not registered is refused (`EQUITY_LOG_UNKNOWN_TENANT`) rather than written under the
//! nil tenant: the rows are permanent.

use diesel::sql_types::{Text, Timestamptz};
use diesel_async::RunQueryDsl;
use rebalancer_run::stores::{EquityLog, EquityReading};

use crate::pg::{Bridge, Pool};
use crate::tenants::{AccountTenants, UNKNOWN_TENANT};

pub struct PgEquityLog {
    bridge: Bridge,
    tenants: std::sync::Arc<AccountTenants>,
}

impl PgEquityLog {
    pub fn new(pool: Pool, tenants: std::sync::Arc<AccountTenants>) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)?, tenants })
    }
}

impl EquityLog for PgEquityLog {
    fn append(&self, reading: &EquityReading) -> Result<(), String> {
        let tenant_id = self.tenants.get(&reading.account_id);
        if tenant_id == UNKNOWN_TENANT {
            return Err(format!("EQUITY_LOG_UNKNOWN_TENANT: no tenant is registered for account {}; the reading is not written", reading.account_id));
        }
        let account_id = reading.account_id.clone();
        let taken_at = reading.taken_at;
        let equity = reading.equity.to_string();
        let cash = reading.cash.to_string();
        let derived_equity = reading.derived_equity.to_string();
        let source = reading.source.to_string();
        self.bridge.block_on(move |mut conn| async move {
            diesel::sql_query(
                "INSERT INTO rebalancer_equity_snapshots \
                 (tenant_id, account_id, taken_at, equity, cash, derived_equity, source, run_key) \
                 VALUES ($1, $2, $3, $4::numeric, $5::numeric, $6::numeric, $7, NULL)",
            )
            .bind::<diesel::sql_types::Uuid, _>(tenant_id)
            .bind::<Text, _>(&account_id)
            .bind::<Timestamptz, _>(taken_at)
            .bind::<Text, _>(&equity)
            .bind::<Text, _>(&cash)
            .bind::<Text, _>(&derived_equity)
            .bind::<Text, _>(&source)
            .execute(&mut conn)
            .await
            .map(|_| ())
            .map_err(|e| format!("EQUITY_LOG_UNAVAILABLE: {e}"))
        })
    }
}
