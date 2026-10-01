//! The Postgres decision ledger (`rebalancer_decision_ledger`, migration
//! `databaseschema-internal/migrations/2026-09-27-000000_create_rebalancer_pilot_ledger`): the persistent
//! `D_acted` of an `OnDecision` sleeve (council Ruling 1: a sleeve is PLANNED iff `D_computable > D_acted`).
//!
//! Semantics, mirrored from `InMemoryRunStore` (`rebalancer-run/src/stores.rs`) exactly:
//! * `D_acted(account, sleeve)` is the newest `acted` decision date since the last `reset` row (or `None`).
//! * `finish` advances it only for decisions flagged `acted`, and only FORWARD: an older or equal decision
//!   leaves it unchanged (no row is written; the run record still finishes).
//! * The table is append-only and monotone in the database as well (trigger), so a bug here cannot rewrite
//!   history; this module's own checks exist to give typed, fail-closed answers before the trigger has to.
//!
//! Everything here is raw SQL over `diesel::sql_query` (the public schema has no such table; this crate must
//! still compile without it). All functions take the connection the caller's transaction is running on.

use chrono::{DateTime, NaiveDate, Utc};
use diesel::sql_types::{Bool, Date, Integer, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

pub const LEDGER_TABLE: &str = "rebalancer_decision_ledger";
pub const LEDGER_MIGRATION: &str = "2026-09-27-000000_create_rebalancer_pilot_ledger";

/// Why the ledger cannot be used for this account right now. Both are "fail closed" conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LedgerFault {
    /// The table does not exist (the migration was not applied).
    Missing,
    /// The ledger already holds rows of this account under a DIFFERENT tenant than the one the caller
    /// resolved: a registry mix-up or an attempt across tenants. Reading "no rows" here would look like an
    /// entry and re-buy the sleeve, so it is an error, never `None`.
    ForeignTenant,
}

pub(crate) fn missing_message(account_id: &str, sleeve_id: &str) -> String {
    format!(
        "the decision ledger table {LEDGER_TABLE} does not exist (account {account_id}, sleeve {sleeve_id}); apply the databaseschema-internal migration {LEDGER_MIGRATION}; the ETF sleeve cannot be planned until then"
    )
}

pub(crate) fn foreign_tenant_message(account_id: &str, sleeve_id: &str) -> String {
    format!(
        "the decision ledger already holds rows of account {account_id} (sleeve {sleeve_id}) under a different tenant than the one resolved for this run; refusing to read or write it (tenant scoping)"
    )
}

#[derive(diesel::QueryableByName)]
struct PresentRow {
    #[diesel(sql_type = Bool)]
    present: bool,
}

#[derive(diesel::QueryableByName)]
struct ExistsRow {
    #[diesel(sql_type = Bool)]
    found: bool,
}

#[derive(diesel::QueryableByName)]
struct DateRow {
    #[diesel(sql_type = Nullable<Date>)]
    d: Option<NaiveDate>,
}

async fn table_present(conn: &mut AsyncPgConnection) -> Result<bool, diesel::result::Error> {
    let row: PresentRow = diesel::sql_query(format!("SELECT to_regclass('{LEDGER_TABLE}') IS NOT NULL AS present")).get_result(conn).await?;
    Ok(row.present)
}

/// Rows of `account_id` that belong to another tenant than `tenant`.
async fn foreign_tenant_rows(conn: &mut AsyncPgConnection, tenant: Uuid, account_id: &str) -> Result<bool, diesel::result::Error> {
    let row: ExistsRow = diesel::sql_query(format!("SELECT EXISTS (SELECT 1 FROM {LEDGER_TABLE} WHERE account_id = $1 AND tenant_id <> $2) AS found"))
        .bind::<Text, _>(account_id)
        .bind::<SqlUuid, _>(tenant)
        .get_result(conn)
        .await?;
    Ok(row.found)
}

/// `D_acted`: the newest acted decision of (account, sleeve) in the latest epoch (since the last reset).
async fn newest_acted(conn: &mut AsyncPgConnection, account_id: &str, sleeve_id: &str) -> Result<Option<NaiveDate>, diesel::result::Error> {
    let row: DateRow = diesel::sql_query(format!(
        "SELECT MAX(decision_date) AS d FROM {LEDGER_TABLE} \
         WHERE account_id = $1 AND sleeve_id = $2 AND kind = 'acted' \
           AND epoch = (SELECT COALESCE(MAX(epoch), 0) FROM {LEDGER_TABLE} WHERE account_id = $1 AND sleeve_id = $2)"
    ))
    .bind::<Text, _>(account_id)
    .bind::<Text, _>(sleeve_id)
    .get_result(conn)
    .await?;
    Ok(row.d)
}

/// The same per-(account, sleeve) advisory lock the guard trigger takes, so a check-then-insert here and a
/// concurrent writer serialise. Transaction-scoped: released at COMMIT/ROLLBACK.
async fn lock_sleeve(conn: &mut AsyncPgConnection, account_id: &str, sleeve_id: &str) -> Result<(), diesel::result::Error> {
    diesel::sql_query("SELECT pg_advisory_xact_lock(hashtextextended('rebalancer_decision_ledger:' || $1 || '/' || $2, 0))")
        .bind::<Text, _>(account_id)
        .bind::<Text, _>(sleeve_id)
        .execute(conn)
        .await?;
    Ok(())
}

pub(crate) enum ReadOutcome {
    Value(Option<NaiveDate>),
    Fault(LedgerFault),
}

/// `last_acted_decision`: read `D_acted`, tenant-checked. Read-only.
pub(crate) async fn read_d_acted(conn: &mut AsyncPgConnection, tenant: Uuid, account_id: &str, sleeve_id: &str) -> Result<ReadOutcome, diesel::result::Error> {
    if !table_present(conn).await? {
        return Ok(ReadOutcome::Fault(LedgerFault::Missing));
    }
    if foreign_tenant_rows(conn, tenant, account_id).await? {
        return Ok(ReadOutcome::Fault(LedgerFault::ForeignTenant));
    }
    Ok(ReadOutcome::Value(newest_acted(conn, account_id, sleeve_id).await?))
}

/// One decision a finished run acted on, as the ledger stores it.
#[derive(Debug, Clone)]
pub(crate) struct ActedRow {
    pub sleeve_id: String,
    pub decision_date: NaiveDate,
    pub computable_decision_date: NaiveDate,
    pub newest_bar_date: NaiveDate,
    pub lag_sessions: i32,
    pub entry: bool,
    pub data_fingerprint: Option<String>,
}

/// The run that produced the rows, and how it ran.
#[derive(Debug, Clone)]
pub(crate) struct ActedRun {
    pub tenant: Uuid,
    pub account_id: String,
    pub run_scheduled_for: DateTime<Utc>,
    pub run_sleeve_set: String,
    pub mode: &'static str,
    pub acted_at: DateTime<Utc>,
}

/// Append the acted decisions of one finished run. MUST run inside the transaction that finishes the run
/// (the guard trigger requires the run row to be `done`). Monotone: a decision not strictly newer than the
/// current `D_acted` writes nothing, exactly like `InMemoryRunStore::finish`. `Some(fault)` = the ledger cannot be used.
pub(crate) async fn append_acted(conn: &mut AsyncPgConnection, run: &ActedRun, rows: &[ActedRow]) -> Result<Option<LedgerFault>, diesel::result::Error> {
    if !table_present(conn).await? {
        return Ok(Some(LedgerFault::Missing));
    }
    for row in rows {
        lock_sleeve(conn, &run.account_id, &row.sleeve_id).await?;
        if foreign_tenant_rows(conn, run.tenant, &run.account_id).await? {
            return Ok(Some(LedgerFault::ForeignTenant));
        }
        if let Some(acted) = newest_acted(conn, &run.account_id, &row.sleeve_id).await? {
            if row.decision_date <= acted {
                continue;
            }
        }
        diesel::sql_query(format!(
            "INSERT INTO {LEDGER_TABLE} \
             (tenant_id, account_id, sleeve_id, kind, decision_date, computable_decision_date, newest_bar_date, lag_sessions, entry, \
              data_fingerprint, run_scheduled_for, run_sleeve_set, mode, acted_at) \
             VALUES ($1, $2, $3, 'acted', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
        ))
        .bind::<SqlUuid, _>(run.tenant)
        .bind::<Text, _>(&run.account_id)
        .bind::<Text, _>(&row.sleeve_id)
        .bind::<Date, _>(row.decision_date)
        .bind::<Date, _>(row.computable_decision_date)
        .bind::<Date, _>(row.newest_bar_date)
        .bind::<Integer, _>(row.lag_sessions)
        .bind::<Bool, _>(row.entry)
        .bind::<Nullable<Text>, _>(&row.data_fingerprint)
        .bind::<Timestamptz, _>(run.run_scheduled_for)
        .bind::<Text, _>(&run.run_sleeve_set)
        .bind::<Text, _>(run.mode)
        .bind::<Timestamptz, _>(run.acted_at)
        .execute(conn)
        .await?;
    }
    Ok(None)
}

/// Append a `reset` row (Ruling 9b): a human cleared `D_acted`, so the sleeve re-enters on the decision in
/// force. Runs in its own transaction; the database refuses a reset with nothing to reset.
pub(crate) async fn append_reset(
    conn: &mut AsyncPgConnection,
    tenant: Uuid,
    account_id: &str,
    sleeve_id: &str,
    actor: &str,
    reason: &str,
    at: DateTime<Utc>,
) -> Result<Option<LedgerFault>, diesel::result::Error> {
    if !table_present(conn).await? {
        return Ok(Some(LedgerFault::Missing));
    }
    lock_sleeve(conn, account_id, sleeve_id).await?;
    if foreign_tenant_rows(conn, tenant, account_id).await? {
        return Ok(Some(LedgerFault::ForeignTenant));
    }
    diesel::sql_query(format!("INSERT INTO {LEDGER_TABLE} (tenant_id, account_id, sleeve_id, kind, actor, reason, acted_at) VALUES ($1, $2, $3, 'reset', $4, $5, $6)"))
        .bind::<SqlUuid, _>(tenant)
        .bind::<Text, _>(account_id)
        .bind::<Text, _>(sleeve_id)
        .bind::<Text, _>(actor)
        .bind::<Text, _>(reason)
        .bind::<Timestamptz, _>(at)
        .execute(conn)
        .await?;
    Ok(None)
}
