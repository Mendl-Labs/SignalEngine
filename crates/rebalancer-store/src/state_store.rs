//! `PgStateStore`: the Postgres-backed [`StateStore`], against `rebalancer_account_state`
//! (`databaseschema-internal/migrations/2026-09-22-010000_create_rebalancer_service_tables`).
//!
//! Compare-and-swap, exactly as `rebalancer-risk/src/store.rs`'s own module doc specifies: a
//! never-saved account is version 0; `save(expected_version, new_state)` succeeds only when the
//! stored version equals `expected_version`, in which case it stores `new_state` at
//! `expected_version + 1`. A first save (`expected_version == 0`, no row yet) is an
//! `INSERT ... ON CONFLICT (account_id) DO NOTHING`; every later save is an
//! `UPDATE ... WHERE account_id = $1 AND version = $2`. Either way the affected-row count IS the
//! compare-and-swap: 1 row means this call won the race, 0 means someone else's save (or, for the
//! first save, someone else's insert) got there first, and this call changes nothing and returns
//! [`StoreError::VersionConflict`] with the row's actual current version.
//!
//! Also re-checks [`AccountState::transition_allowed`] before writing (defence in depth, matching the
//! in-memory store's own behaviour) -- a store must refuse an illegal transition even if some future
//! caller bypassed the pipeline's own state machine.

use std::sync::Mutex;

use broker_adapters::Dec;
use diesel::sql_types::{BigInt, Date, Jsonb, Nullable, Text, Timestamptz};
use diesel_async::RunQueryDsl;
use rebalancer_risk::state::{AccountState, AccountStateRecord};
use rebalancer_risk::store::{StateStore, StoreError};
use uuid::Uuid;

use crate::json::{self, status_from_str, status_to_str};
use crate::pg::{Bridge, Pool};
use crate::tenants::AccountTenants;

fn parse_dec(s: &str) -> Result<Dec, String> {
    Dec::parse(s).map_err(|e| format!("bad decimal {s:?}: {e}"))
}

#[derive(diesel::QueryableByName)]
struct StateRow {
    #[diesel(sql_type = Text)]
    account_id: String,
    #[diesel(sql_type = BigInt)]
    version: i64,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Text)]
    risk_scale: String,
    #[diesel(sql_type = Nullable<diesel::sql_types::Integer>)]
    shrink_rung: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    hwm: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    day_start_equity: Option<String>,
    #[diesel(sql_type = Nullable<Date>)]
    trading_day: Option<chrono::NaiveDate>,
    #[diesel(sql_type = Nullable<Text>)]
    last_equity: Option<String>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    last_equity_at: Option<chrono::DateTime<chrono::Utc>>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    halt: Option<serde_json::Value>,
    #[diesel(sql_type = Jsonb)]
    resumes: serde_json::Value,
}

fn row_to_state(r: StateRow) -> Result<AccountState, String> {
    let record = AccountStateRecord {
        account_id: r.account_id,
        version: r.version.max(0) as u64,
        status: status_from_str(&r.status)?,
        risk_scale: parse_dec(&r.risk_scale)?,
        shrink_rung: r.shrink_rung.map(|v| v as usize),
        hwm: r.hwm.as_deref().map(parse_dec).transpose()?,
        day_start_equity: r.day_start_equity.as_deref().map(parse_dec).transpose()?,
        trading_day: r.trading_day,
        last_equity: r.last_equity.as_deref().map(parse_dec).transpose()?,
        last_equity_at: r.last_equity_at,
        halt: r.halt.as_ref().map(json::halt_record_from_json).transpose()?,
        resumes: json::resumes_from_json(&r.resumes)?,
    };
    AccountState::from_record(record)
}

pub struct PgStateStore {
    bridge: Bridge,
    tenants: std::sync::Arc<AccountTenants>,
    /// Serializes save() end-to-end (read-current, check transition, write) PER PROCESS: Postgres's
    /// own row-level locking (the UPDATE's WHERE version = $2) is what actually makes the
    /// compare-and-swap correct ACROSS processes; this mutex only avoids two threads of the SAME
    /// process racing between the read and the write, matching the in-memory store's single global
    /// `Mutex` -- it is a convenience for the common case, not the source of the safety property.
    write_lock: Mutex<()>,
}

impl PgStateStore {
    pub fn new(pool: Pool, tenants: std::sync::Arc<AccountTenants>) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)?, tenants, write_lock: Mutex::new(()) })
    }
}

impl StateStore for PgStateStore {
    fn load(&self, account_id: &str) -> Result<Option<AccountState>, StoreError> {
        self.bridge
            .block_on(|mut conn| async move {
                let rows: Vec<StateRow> = diesel::sql_query(
                    "SELECT account_id, version, status, risk_scale::text AS risk_scale, shrink_rung, \
                     hwm::text AS hwm, day_start_equity::text AS day_start_equity, trading_day, \
                     last_equity::text AS last_equity, last_equity_at, halt, resumes \
                     FROM rebalancer_account_state WHERE account_id = $1",
                )
                .bind::<Text, _>(account_id)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("load: {e}"))?;
                match rows.into_iter().next() {
                    None => Ok(None),
                    Some(r) => row_to_state(r).map(Some),
                }
            })
            .map_err(StoreError::Unavailable)
    }

    fn save(&self, expected_version: u64, new_state: &AccountState) -> Result<AccountState, StoreError> {
        let _guard = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());
        let old = StateStore::load(self, new_state.account_id())?;
        let actual_version = old.as_ref().map_or(0, AccountState::version);
        if actual_version != expected_version {
            return Err(StoreError::VersionConflict { expected: expected_version, actual: actual_version });
        }
        if let Some(o) = &old {
            if !AccountState::transition_allowed(o, new_state) {
                return Err(StoreError::IllegalTransition(format!(
                    "{} to {} without a recorded human resume",
                    o.status().as_str(),
                    new_state.status().as_str()
                )));
            }
        }

        let record = new_state.to_record();
        let tenant_id: Uuid = self.tenants.get(new_state.account_id());
        let account_id = new_state.account_id().to_string();
        let halt_json = record.halt.as_ref().map(json::halt_record_to_json);
        let resumes_json = json::resumes_to_json(&record.resumes);
        let status = status_to_str(record.status).to_string();
        let risk_scale = record.risk_scale.to_string();
        let shrink_rung = record.shrink_rung.map(|v| v as i32);
        let hwm_text = record.hwm.map(|d| d.to_string());
        let day_start_text = record.day_start_equity.map(|d| d.to_string());
        let last_equity_text = record.last_equity.map(|d| d.to_string());
        let last_equity_at = record.last_equity_at;
        let trading_day = record.trading_day;

        let affected: usize = self
            .bridge
            .block_on(move |mut conn| async move {
                if actual_version == 0 {
                    diesel::sql_query(
                        "INSERT INTO rebalancer_account_state \
                         (tenant_id, account_id, version, status, risk_scale, shrink_rung, hwm, \
                          day_start_equity, trading_day, last_equity, last_equity_at, halt, resumes, updated_at) \
                         VALUES ($1, $2, 1, $3, $4::numeric, $5, $6::numeric, $7::numeric, $8, $9::numeric, $10, $11, $12, NOW()) \
                         ON CONFLICT (account_id) DO NOTHING",
                    )
                    .bind::<diesel::sql_types::Uuid, _>(tenant_id)
                    .bind::<Text, _>(&account_id)
                    .bind::<Text, _>(&status)
                    .bind::<Text, _>(&risk_scale)
                    .bind::<Nullable<diesel::sql_types::Integer>, _>(shrink_rung)
                    .bind::<Nullable<Text>, _>(&hwm_text)
                    .bind::<Nullable<Text>, _>(&day_start_text)
                    .bind::<Nullable<Date>, _>(trading_day)
                    .bind::<Nullable<Text>, _>(&last_equity_text)
                    .bind::<Nullable<Timestamptz>, _>(last_equity_at)
                    .bind::<Nullable<Jsonb>, _>(&halt_json)
                    .bind::<Jsonb, _>(&resumes_json)
                    .execute(&mut conn)
                    .await
                    .map_err(|e| format!("insert: {e}"))
                } else {
                    diesel::sql_query(
                        "UPDATE rebalancer_account_state SET \
                         version = version + 1, tenant_id = $1, status = $3, risk_scale = $4::numeric, \
                         shrink_rung = $5, hwm = $6::numeric, day_start_equity = $7::numeric, \
                         trading_day = $8, last_equity = $9::numeric, last_equity_at = $10, \
                         halt = $11, resumes = $12, updated_at = NOW() \
                         WHERE account_id = $2 AND version = $13",
                    )
                    .bind::<diesel::sql_types::Uuid, _>(tenant_id)
                    .bind::<Text, _>(&account_id)
                    .bind::<Text, _>(&status)
                    .bind::<Text, _>(&risk_scale)
                    .bind::<Nullable<diesel::sql_types::Integer>, _>(shrink_rung)
                    .bind::<Nullable<Text>, _>(&hwm_text)
                    .bind::<Nullable<Text>, _>(&day_start_text)
                    .bind::<Nullable<Date>, _>(trading_day)
                    .bind::<Nullable<Text>, _>(&last_equity_text)
                    .bind::<Nullable<Timestamptz>, _>(last_equity_at)
                    .bind::<Nullable<Jsonb>, _>(&halt_json)
                    .bind::<Jsonb, _>(&resumes_json)
                    .bind::<BigInt, _>(actual_version as i64)
                    .execute(&mut conn)
                    .await
                    .map_err(|e| format!("update: {e}"))
                }
            })
            .map_err(StoreError::Unavailable)?;

        if affected != 1 {
            // Someone else won the race between our read and our write. Report the CURRENT stored
            // version so the caller can reload and re-evaluate, exactly like the in-memory store.
            let now_version = StateStore::load(self, new_state.account_id())?.map_or(0, |s| s.version());
            return Err(StoreError::VersionConflict { expected: expected_version, actual: now_version });
        }

        let mut stored_record = new_state.to_record();
        stored_record.version = expected_version + 1;
        AccountState::from_record(stored_record).map_err(StoreError::IllegalTransition)
    }
}
