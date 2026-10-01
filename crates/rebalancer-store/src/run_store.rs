//! `PgRunStore`: the Postgres-backed [`RunStore`], against `rebalancer_runs` + `rebalancer_run_journal`
//! (`databaseschema-internal/migrations/2026-09-22-010000_create_rebalancer_service_tables`).
//!
//! # Scope of `RunRecord` fidelity (read this before trusting `get`/`AlreadyDone`)
//! `RunRecord` (`rebalancer-run/src/record.rs`) has ~30 fields, several nested several levels deep
//! (risk decisions, reconciliation reports, plan summaries, flatten reports, ...), and none of its
//! transitive field types derive `serde::Serialize`/`Deserialize` (`rebalancer-core`/`rebalancer-risk`/
//! `rebalancer-run` are pure, dependency-minimal crates with no serde dependency at all -- see each
//! crate's own `Cargo.toml`). Full structural round-tripping of the WHOLE record would mean either (a)
//! adding serde derives across three crates this work was told to leave alone ("match the existing
//! trait shapes exactly, don't redesign them"), or (b) hand-writing bespoke JSON mapping for every one
//! of those nested types, including several with `&'static str` "stable code" fields
//! (`DeniedSummary::codes`, `StepNote::step`) that cannot come back from arbitrary stored text without
//! leaking memory per read.
//!
//! This store makes a deliberate, documented scope cut instead: it persists FULL structural fidelity
//! for exactly the fields `RunStore`'s OTHER methods (`in_flight`, `known_order_ids`, `day_counters`,
//! `last_snapshot`, `summaries`) actually read -- the run key, lease/attempt state, outcome, mandate
//! metadata, both broker snapshots, and the placed orders' operationally load-bearing fields (tag,
//! symbol, side, quantities, phase, outcome, broker id, executed quantity) -- and reconstructs those
//! fields faithfully in `get()`/`Begin::AlreadyDone` too. The "narrative" fields that exist for a human
//! reading a finished run (recon reports, the risk decision, targets, plan/replan detail, tickets,
//! cleanup, flatten report, alerts, the step log) are captured in full via `record_debug`
//! (`format!("{:?}", record)`, a TEXT column) for a person or an export job reading the database
//! directly, but are NOT parsed back into the typed `RunRecord` `get()` returns -- that `RunRecord`
//! carries them as empty/default. The round-trip test in this crate's test suite asserts fidelity on
//! exactly the fields this scope cut promises, not on the narrative ones.
//!
//! # Why one table, not two, for "in progress" and "finished"
//! `begin`'s lease-expiry/attempt-bump logic and `finish`'s terminal write both need to see and change
//! the SAME row atomically (an `UPDATE ... WHERE account_id = $1 AND scheduled_for = $2 AND sleeve_set
//! = $3 AND status = 'in_progress'` is what makes "this call, and only this call, gets to move the row
//! from in_progress to done" true under concurrent callers) -- splitting them would reopen exactly the
//! race the run-key uniqueness exists to close.
//!
//! # The decision ledger (`D_acted`) and the atomic `finish`
//! `last_acted_decision` reads `rebalancer_decision_ledger` (see [`crate::ledger`]); `finish` writes the run's
//! terminal row, its equity snapshots and one ledger row per `acted` decision in ONE transaction, so `D_acted`
//! can never advance without the run record (or the reverse), and a crash between the two leaves neither. A
//! missing ledger table, or a ledger that holds the account under another tenant, is
//! `DecisionLedgerUnavailable` (fail closed), never `None`.

use std::collections::BTreeSet;
use std::sync::Arc;

use broker_adapters::{Dec, Side};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use diesel::sql_types::{Date, Integer, Jsonb, Nullable, Text, Timestamptz};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use rebalancer_core::guard::DayCounters;
use rebalancer_core::planner::PlannedOrder;
use rebalancer_run::record::{
    ExecutionMode, OutcomeKind, Phase, PlacedOrder, PlacedOutcome, RunKey, RunOutcome, RunRecord, SnapshotSummary,
};
use rebalancer_run::stores::{Begin, JournalEntry, RunStore, RunStoreError, RunSummary};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::ledger::{self, ActedRow, ActedRun, LedgerFault, ReadOutcome};
use crate::pg::{Bridge, Pool};
use crate::provenance::PlanProvenanceRegistry;
use crate::tenants::{AccountTenants, UNKNOWN_TENANT};

fn unavailable(e: impl std::fmt::Display) -> RunStoreError {
    RunStoreError::Unavailable(e.to_string())
}

// ---------------------------------------------------------------------------------------------------
// Small enum <-> text mappings (all finite, all pinned by the pipeline's own tests --
// rebalancer-run/tests/pipeline.rs::run_alert_and_outcome_codes_are_pinned_and_unique).
// ---------------------------------------------------------------------------------------------------

fn mode_to_str(m: ExecutionMode) -> &'static str {
    m.as_str()
}
fn mode_from_str(s: &str) -> Result<ExecutionMode, String> {
    match s {
        "assisted" => Ok(ExecutionMode::Assisted),
        "paper" => Ok(ExecutionMode::Paper),
        "live" => Ok(ExecutionMode::Live),
        other => Err(format!("unknown execution mode {other:?}")),
    }
}

fn outcome_to_str(k: OutcomeKind) -> &'static str {
    k.as_str()
}
fn outcome_from_str(s: &str) -> Result<OutcomeKind, String> {
    match s {
        "COMPLETED" => Ok(OutcomeKind::Completed),
        "REFUSED" => Ok(OutcomeKind::Refused),
        "FAILED_CLOSED" => Ok(OutcomeKind::FailedClosed),
        "HALTED" => Ok(OutcomeKind::Halted),
        other => Err(format!("unknown outcome kind {other:?}")),
    }
}

fn phase_to_str(p: Phase) -> &'static str {
    match p {
        Phase::Sells => "sells",
        Phase::Buys => "buys",
        Phase::Rehearsal => "rehearsal",
    }
}
fn phase_from_str(s: &str) -> Result<Phase, String> {
    match s {
        "sells" => Ok(Phase::Sells),
        "buys" => Ok(Phase::Buys),
        "rehearsal" => Ok(Phase::Rehearsal),
        other => Err(format!("unknown phase {other:?}")),
    }
}

fn placed_outcome_to_str(o: PlacedOutcome) -> &'static str {
    match o {
        PlacedOutcome::Filled => "filled",
        PlacedOutcome::PartiallyFilled => "partially_filled",
        PlacedOutcome::NothingExecuted => "nothing_executed",
        PlacedOutcome::Validated => "validated",
        PlacedOutcome::Rejected => "rejected",
        PlacedOutcome::NotSent => "not_sent",
        PlacedOutcome::AdoptedExisting => "adopted_existing",
        PlacedOutcome::UnknownNotFound => "unknown_not_found",
        PlacedOutcome::Unsettled => "unsettled",
    }
}
fn placed_outcome_from_str(s: &str) -> Result<PlacedOutcome, String> {
    Ok(match s {
        "filled" => PlacedOutcome::Filled,
        "partially_filled" => PlacedOutcome::PartiallyFilled,
        "nothing_executed" => PlacedOutcome::NothingExecuted,
        "validated" => PlacedOutcome::Validated,
        "rejected" => PlacedOutcome::Rejected,
        "not_sent" => PlacedOutcome::NotSent,
        "adopted_existing" => PlacedOutcome::AdoptedExisting,
        "unknown_not_found" => PlacedOutcome::UnknownNotFound,
        "unsettled" => PlacedOutcome::Unsettled,
        other => return Err(format!("unknown placed outcome {other:?}")),
    })
}

fn side_to_str(s: Side) -> &'static str {
    s.as_str()
}
fn side_from_str(s: &str) -> Result<Side, String> {
    match s {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        other => Err(format!("unknown side {other:?}")),
    }
}

// ---------------------------------------------------------------------------------------------------
// JSON shapes: SnapshotSummary and the operational subset of PlacedOrder
// ---------------------------------------------------------------------------------------------------

fn snapshot_to_json(s: &SnapshotSummary) -> Value {
    let dec_map = |m: &std::collections::BTreeMap<String, Dec>| -> Value {
        Value::Object(m.iter().map(|(k, v)| (k.clone(), Value::String(v.to_string()))).collect())
    };
    json!({
        "taken_at": s.taken_at.to_rfc3339(),
        "equity": s.equity.to_string(),
        "cash": s.cash.to_string(),
        "derived_equity": s.derived_equity.to_string(),
        "holdings": dec_map(&s.holdings),
        "marks": dec_map(&s.marks),
        "open_order_ids": s.open_order_ids,
    })
}

fn snapshot_from_json(v: &Value) -> Result<SnapshotSummary, String> {
    let dec = |x: &Value| -> Result<Dec, String> { Dec::parse(x.as_str().ok_or("expected string decimal")?).map_err(|e| e.to_string()) };
    let map = |key: &str| -> Result<std::collections::BTreeMap<String, Dec>, String> {
        v[key].as_object().ok_or_else(|| format!("expected object at {key}"))?.iter().map(|(k, val)| Ok((k.clone(), dec(val)?))).collect()
    };
    Ok(SnapshotSummary {
        taken_at: crate::json::dt_from_json(&v["taken_at"])?,
        equity: dec(&v["equity"])?,
        cash: dec(&v["cash"])?,
        derived_equity: dec(&v["derived_equity"])?,
        holdings: map("holdings")?,
        marks: map("marks")?,
        open_order_ids: v["open_order_ids"].as_array().ok_or("expected open_order_ids array")?.iter().map(|x| x.as_str().unwrap_or_default().to_string()).collect(),
    })
}

/// Only the fields `known_order_ids`/`day_counters`/a useful `get()` need; see the module docs.
fn placed_to_json(p: &PlacedOrder) -> Value {
    json!({
        "phase": phase_to_str(p.phase),
        "tag": p.tag,
        "symbol": p.symbol,
        "side": side_to_str(p.side),
        "planned_quantity": p.planned_quantity.to_string(),
        "price": p.price.to_string(),
        "outcome": placed_outcome_to_str(p.outcome),
        "broker_order_id": p.broker_order_id,
        "executed_quantity": p.executed_quantity.to_string(),
    })
}

fn placed_from_json(v: &Value) -> Result<PlacedOrder, String> {
    let dec = |x: &Value| -> Result<Dec, String> { Dec::parse(x.as_str().ok_or("expected string decimal")?).map_err(|e| e.to_string()) };
    Ok(PlacedOrder {
        phase: phase_from_str(v["phase"].as_str().ok_or("placed.phase missing")?)?,
        tag: v["tag"].as_str().unwrap_or_default().to_string(),
        symbol: v["symbol"].as_str().unwrap_or_default().to_string(),
        side: side_from_str(v["side"].as_str().ok_or("placed.side missing")?)?,
        planned_quantity: dec(&v["planned_quantity"])?,
        price: dec(&v["price"])?,
        outcome: placed_outcome_from_str(v["outcome"].as_str().ok_or("placed.outcome missing")?)?,
        broker_order_id: v["broker_order_id"].as_str().map(str::to_string),
        // `status`/`reports`/`anomalies`/`detail` are narrative-only for this store's documented scope
        // cut (see the module docs): not persisted, defaulted on read.
        status: None,
        executed_quantity: dec(&v["executed_quantity"])?,
        reports: Vec::new(),
        anomalies: Vec::new(),
        detail: String::new(),
    })
}

fn placed_vec_to_json(placed: &[PlacedOrder]) -> Value {
    Value::Array(placed.iter().map(placed_to_json).collect())
}
fn placed_vec_from_json(v: &Value) -> Result<Vec<PlacedOrder>, String> {
    v.as_array().ok_or("expected placed array")?.iter().map(placed_from_json).collect()
}

/// The tickets an assisted run issued (`RunRecord::tickets`), so the owner reads structured rows instead of a
/// `{:?}` dump. Write-only here: `get()` keeps returning the record's narrative fields empty (module docs).
fn tickets_to_json(tickets: &[PlannedOrder]) -> Value {
    Value::Array(
        tickets
            .iter()
            .map(|t| {
                json!({
                    "tag": t.tag,
                    "sleeve": t.sleeve,
                    "venue": t.venue,
                    "asset_class": t.asset_class,
                    "symbol": t.symbol,
                    "side": side_to_str(t.side),
                    "quantity": t.quantity.to_string(),
                    "price": t.price.to_string(),
                    "notional": t.notional.to_string(),
                    "est_fee": t.est_fee.to_string(),
                })
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------------------------------

#[derive(diesel::QueryableByName, Clone)]
struct RunRow {
    #[diesel(sql_type = Text)]
    account_id: String,
    #[diesel(sql_type = Timestamptz)]
    scheduled_for: DateTime<Utc>,
    #[diesel(sql_type = Text)]
    sleeve_set: String,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Integer)]
    attempt: i32,
    #[diesel(sql_type = Date)]
    trading_day: NaiveDate,
    #[diesel(sql_type = Text)]
    mode: String,
    #[diesel(sql_type = Timestamptz)]
    started_at: DateTime<Utc>,
    #[diesel(sql_type = Timestamptz)]
    lease_until: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    finished_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Text>)]
    outcome_kind: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    outcome_code: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    outcome_message: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    mandate_hash: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    mandate_version: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    mandate_standing: Option<String>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pre_snapshot: Option<Value>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    post_snapshot: Option<Value>,
    #[diesel(sql_type = Jsonb)]
    placed: Value,
}

fn row_to_summary(r: &RunRow) -> RunSummary {
    RunSummary {
        key: RunKey { account_id: r.account_id.clone(), scheduled_for: r.scheduled_for, sleeve_set: r.sleeve_set.clone() },
        attempt: r.attempt.max(0) as u32,
        scheduled_for: r.scheduled_for,
        started_at: r.started_at,
        finished_at: r.finished_at,
        outcome: match (&r.outcome_kind, &r.outcome_code) {
            (Some(k), Some(c)) => outcome_from_str(k).ok().map(|k| (k, c.clone())),
            _ => None,
        },
    }
}

fn row_to_record(r: RunRow) -> Result<RunRecord, String> {
    let key = RunKey { account_id: r.account_id.clone(), scheduled_for: r.scheduled_for, sleeve_set: r.sleeve_set.clone() };
    let mode = mode_from_str(&r.mode)?;
    let outcome = RunOutcome {
        kind: r.outcome_kind.as_deref().map(outcome_from_str).transpose()?.unwrap_or(OutcomeKind::FailedClosed),
        code: r.outcome_code.clone().unwrap_or_default(),
        message: r.outcome_message.clone().unwrap_or_default(),
    };
    Ok(RunRecord {
        key,
        mode,
        attempt: r.attempt.max(0) as u32,
        trading_day: r.trading_day,
        scheduled_for: r.scheduled_for,
        started_at: r.started_at,
        finished_at: r.finished_at.unwrap_or(r.started_at),
        outcome,
        mandate_hash: r.mandate_hash.clone().unwrap_or_default(),
        mandate_version: r.mandate_version.map(|v| v as u32),
        mandate_standing: r.mandate_standing.clone().unwrap_or_default(),
        deployment_digest: None,
        data_fingerprints: Vec::new(),
        pre_snapshot: r.pre_snapshot.as_ref().map(snapshot_from_json).transpose()?,
        post_snapshot: r.post_snapshot.as_ref().map(snapshot_from_json).transpose()?,
        recon: Vec::new(),
        state_before: None,
        state_after: None,
        transitions: Vec::new(),
        risk: None,
        decisions: Vec::new(),
        targets: Vec::new(),
        plan: None,
        replan: None,
        tickets: Vec::new(),
        placed: placed_vec_from_json(&r.placed)?,
        cleanup: Vec::new(),
        flatten: None,
        alerts: Vec::new(),
        alert_delivery_failures: Vec::new(),
        steps: Vec::new(),
    })
}

#[derive(diesel::QueryableByName)]
struct JournalRow {
    #[diesel(sql_type = Text)]
    tag: String,
    #[diesel(sql_type = Nullable<Text>)]
    broker_order_id: Option<String>,
    #[diesel(sql_type = Text)]
    notional: String,
}

fn journal_row_to_entry(r: JournalRow) -> Result<JournalEntry, String> {
    Ok(JournalEntry { tag: r.tag, broker_order_id: r.broker_order_id, notional: Dec::parse(&r.notional).map_err(|e| e.to_string())? })
}

// ---------------------------------------------------------------------------------------------------
// PgRunStore
// ---------------------------------------------------------------------------------------------------

pub struct PgRunStore {
    bridge: Bridge,
    tenants: Arc<AccountTenants>,
    provenance: Option<Arc<PlanProvenanceRegistry>>,
}

/// Why a `finish` transaction did not commit (every variant rolls the whole transaction back).
#[derive(Debug)]
enum FinishError {
    Db(String),
    /// The run row was not `in_progress` any more (someone finished it first).
    AlreadyFinished,
    /// An `acted` decision needs the ledger and the ledger table does not exist.
    LedgerMissing,
    /// The ledger holds this account under another tenant.
    LedgerForeignTenant,
}

impl From<diesel::result::Error> for FinishError {
    fn from(e: diesel::result::Error) -> Self {
        FinishError::Db(e.to_string())
    }
}

/// Appends the finished run's pre/post broker snapshots to `rebalancer_equity_snapshots` (see that table's own
/// migration comment: a flat time series for a dashboard, distinct from the per-run JSONB detail already on
/// `rebalancer_runs`). Runs inside `finish`'s transaction: a failure here rolls the whole finish back.
async fn insert_equity_snapshots(conn: &mut AsyncPgConnection, tenant_id: Uuid, record: &RunRecord) -> Result<(), diesel::result::Error> {
    let account_id = record.key.account_id.clone();
    let run_key = record.key.canonical();
    let mut rows: Vec<(&str, DateTime<Utc>, Dec, Dec, Dec)> = Vec::new();
    if let Some(s) = &record.pre_snapshot {
        rows.push(("pre_run", s.taken_at, s.equity, s.cash, s.derived_equity));
    }
    if let Some(s) = &record.post_snapshot {
        rows.push(("post_run", s.taken_at, s.equity, s.cash, s.derived_equity));
    }
    for (source, taken_at, equity, cash, derived_equity) in rows {
        diesel::sql_query(
            "INSERT INTO rebalancer_equity_snapshots              (tenant_id, account_id, taken_at, equity, cash, derived_equity, source, run_key)              VALUES ($1, $2, $3, $4::numeric, $5::numeric, $6::numeric, $7, $8)",
        )
        .bind::<diesel::sql_types::Uuid, _>(tenant_id)
        .bind::<Text, _>(&account_id)
        .bind::<Timestamptz, _>(taken_at)
        .bind::<Text, _>(equity.to_string())
        .bind::<Text, _>(cash.to_string())
        .bind::<Text, _>(derived_equity.to_string())
        .bind::<Text, _>(source)
        .bind::<Text, _>(&run_key)
        .execute(conn)
        .await?;
    }
    Ok(())
}

/// The decisions of `record` that count as ACTED, in the ledger's shape. The pipeline decides what `acted`
/// means (`SleeveDecision::acted`); this only carries it over.
fn acted_rows(record: &RunRecord) -> Vec<ActedRow> {
    let mut rows: Vec<ActedRow> = record
        .decisions
        .iter()
        .filter(|d| d.acted)
        .map(|d| ActedRow {
            sleeve_id: d.sleeve.clone(),
            decision_date: d.decision_date,
            computable_decision_date: d.computable_decision_date,
            newest_bar_date: d.newest_bar_date,
            lag_sessions: i32::try_from(d.lag_sessions).unwrap_or(i32::MAX),
            entry: d.entry,
            data_fingerprint: record.targets.iter().find(|t| t.sleeve == d.sleeve).map(|t| t.data_fingerprint.clone()),
        })
        .collect();
    // A stable order, so two writers of one account take the per-sleeve locks in the same order.
    rows.sort_by(|a, b| a.sleeve_id.cmp(&b.sleeve_id));
    rows
}

impl PgRunStore {
    pub fn new(pool: Pool, tenants: Arc<AccountTenants>) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)?, tenants, provenance: None })
    }

    /// Record plan provenance (`plan_origin`, `plan_id`, `venue_environment`) on every finished run of an account
    /// the registry knows (the account source fills it). Without it those columns stay NULL.
    pub fn with_plan_provenance(mut self, registry: Arc<PlanProvenanceRegistry>) -> Self {
        self.provenance = Some(registry);
        self
    }

    /// Append a ledger `reset` row (council Ruling 9b): after a halt-and-flatten and a human resume, `D_acted` is
    /// cleared so the sleeve re-enters on the decision in force. NEVER called by the service loop: it is the store
    /// side of the human-only owner command. The database refuses a reset when nothing was acted since the last one.
    pub fn reset_decision_ledger(&self, account_id: &str, sleeve_id: &str, actor: &str, reason: &str) -> Result<(), RunStoreError> {
        let tenant = self.tenants.get(account_id);
        if tenant == UNKNOWN_TENANT {
            return Err(RunStoreError::DecisionLedgerUnavailable(format!("no tenant is registered for account {account_id}; refusing to write its ledger")));
        }
        let (account, sleeve, actor_s, reason_s) = (account_id.to_string(), sleeve_id.to_string(), actor.to_string(), reason.to_string());
        let outcome: Result<Option<LedgerFault>, FinishError> = self
            .bridge
            .block_on(move |mut conn| async move {
                Ok(conn
                    .transaction::<_, FinishError, _>(|conn| {
                        async move { Ok(ledger::append_reset(conn, tenant, &account, &sleeve, &actor_s, &reason_s, Utc::now()).await?) }.scope_boxed()
                    })
                    .await)
            })
            .map_err(unavailable)?;
        match outcome {
            Ok(None) => Ok(()),
            Ok(Some(LedgerFault::Missing)) => Err(RunStoreError::DecisionLedgerUnavailable(ledger::missing_message(account_id, sleeve_id))),
            Ok(Some(LedgerFault::ForeignTenant)) => Err(RunStoreError::DecisionLedgerUnavailable(ledger::foreign_tenant_message(account_id, sleeve_id))),
            Err(e) => Err(unavailable(format!("reset_decision_ledger: {e:?}"))),
        }
    }

    fn fetch_one(&self, account_id: &str, scheduled_for: DateTime<Utc>, sleeve_set: &str) -> Result<Option<RunRow>, RunStoreError> {
        let account_id = account_id.to_string();
        let sleeve_set = sleeve_set.to_string();
        self.bridge
            .block_on(move |mut conn| async move {
                let rows: Vec<RunRow> = diesel::sql_query(
                    "SELECT account_id, scheduled_for, sleeve_set, status, attempt, trading_day, mode, \
                     started_at, lease_until, finished_at, outcome_kind, outcome_code, outcome_message, \
                     mandate_hash, mandate_version, mandate_standing, pre_snapshot, post_snapshot, placed \
                     FROM rebalancer_runs WHERE account_id = $1 AND scheduled_for = $2 AND sleeve_set = $3",
                )
                .bind::<Text, _>(&account_id)
                .bind::<Timestamptz, _>(scheduled_for)
                .bind::<Text, _>(&sleeve_set)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("fetch: {e}"))?;
                Ok(rows.into_iter().next())
            })
            .map_err(unavailable)
    }
}

impl RunStore for PgRunStore {
    fn begin(&self, key: &RunKey, trading_day: NaiveDate, started_at: DateTime<Utc>, lease_secs: i64) -> Result<Begin, RunStoreError> {
        let lease_until = started_at + Duration::seconds(lease_secs);
        let tenant_id: Uuid = self.tenants.get(&key.account_id);
        let account_id = key.account_id.clone();
        let scheduled_for = key.scheduled_for;
        let sleeve_set = key.sleeve_set.clone();

        // 1. Try to create the row (first attempt of this key ever).
        let inserted: usize = self
            .bridge
            .block_on({
                let account_id = account_id.clone();
                let sleeve_set = sleeve_set.clone();
                move |mut conn| async move {
                    diesel::sql_query(
                        "INSERT INTO rebalancer_runs \
                         (tenant_id, account_id, scheduled_for, sleeve_set, status, attempt, trading_day, mode, started_at, lease_until) \
                         VALUES ($1, $2, $3, $4, 'in_progress', 1, $5, 'assisted', $6, $7) \
                         ON CONFLICT (account_id, scheduled_for, sleeve_set) DO NOTHING",
                    )
                    .bind::<diesel::sql_types::Uuid, _>(tenant_id)
                    .bind::<Text, _>(&account_id)
                    .bind::<Timestamptz, _>(scheduled_for)
                    .bind::<Text, _>(&sleeve_set)
                    .bind::<Date, _>(trading_day)
                    .bind::<Timestamptz, _>(started_at)
                    .bind::<Timestamptz, _>(lease_until)
                    .execute(&mut conn)
                    .await
                    .map_err(|e| format!("begin insert: {e}"))
                }
            })
            .map_err(unavailable)?;
        if inserted == 1 {
            return Ok(Begin::Started { attempt: 1 });
        }

        // 2. A row already exists: read it and decide.
        let existing = self.fetch_one(&account_id, scheduled_for, &sleeve_set)?.ok_or_else(|| unavailable("row vanished between insert and read"))?;
        if existing.status == "done" {
            let record = row_to_record(existing).map_err(unavailable)?;
            return Ok(Begin::AlreadyDone(Box::new(record)));
        }
        if started_at < existing.lease_until {
            return Ok(Begin::Busy { started_at: existing.started_at });
        }
        // Lease expired: resume as the next attempt. Atomic on the WHERE clause, so a second racing
        // caller that also saw the expired lease gets 0 rows affected here and falls through to Busy.
        let next_attempt = existing.attempt + 1;
        let updated: usize = self
            .bridge
            .block_on({
                let account_id = account_id.clone();
                let sleeve_set = sleeve_set.clone();
                let prev_lease = existing.lease_until;
                move |mut conn| async move {
                    diesel::sql_query(
                        "UPDATE rebalancer_runs SET attempt = $1, trading_day = $2, started_at = $3, lease_until = $4 \
                         WHERE account_id = $5 AND scheduled_for = $6 AND sleeve_set = $7 AND status = 'in_progress' AND lease_until = $8",
                    )
                    .bind::<Integer, _>(next_attempt)
                    .bind::<Date, _>(trading_day)
                    .bind::<Timestamptz, _>(started_at)
                    .bind::<Timestamptz, _>(lease_until)
                    .bind::<Text, _>(&account_id)
                    .bind::<Timestamptz, _>(scheduled_for)
                    .bind::<Text, _>(&sleeve_set)
                    .bind::<Timestamptz, _>(prev_lease)
                    .execute(&mut conn)
                    .await
                    .map_err(|e| format!("begin resume: {e}"))
                }
            })
            .map_err(unavailable)?;
        if updated == 1 {
            Ok(Begin::Started { attempt: next_attempt.max(1) as u32 })
        } else {
            // Someone else resumed (or finished) it first between our read and our write.
            let now = self.fetch_one(&account_id, scheduled_for, &sleeve_set)?.ok_or_else(|| unavailable("row vanished during resume race"))?;
            if now.status == "done" {
                let record = row_to_record(now).map_err(unavailable)?;
                Ok(Begin::AlreadyDone(Box::new(record)))
            } else {
                Ok(Begin::Busy { started_at: now.started_at })
            }
        }
    }

    fn journal_order(&self, key: &RunKey, entry: JournalEntry) -> Result<(), RunStoreError> {
        let tenant_id = self.tenants.get(&key.account_id);
        let (account_id, scheduled_for, sleeve_set) = (key.account_id.clone(), key.scheduled_for, key.sleeve_set.clone());
        let (tag, broker_order_id, notional) = (entry.tag, entry.broker_order_id, entry.notional.to_string());
        self.bridge
            .block_on(move |mut conn| async move {
                diesel::sql_query(
                    "INSERT INTO rebalancer_run_journal (tenant_id, account_id, scheduled_for, sleeve_set, tag, broker_order_id, notional, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7::numeric, NOW()) \
                     ON CONFLICT (tenant_id, account_id, scheduled_for, sleeve_set, tag) \
                     DO UPDATE SET broker_order_id = EXCLUDED.broker_order_id, notional = EXCLUDED.notional, updated_at = NOW()",
                )
                .bind::<diesel::sql_types::Uuid, _>(tenant_id)
                .bind::<Text, _>(&account_id)
                .bind::<Timestamptz, _>(scheduled_for)
                .bind::<Text, _>(&sleeve_set)
                .bind::<Text, _>(&tag)
                .bind::<Nullable<Text>, _>(&broker_order_id)
                .bind::<Text, _>(&notional)
                .execute(&mut conn)
                .await
                .map(|_| ())
                .map_err(|e| format!("journal_order: {e}"))
            })
            .map_err(|e| {
                // NOT_STARTED is what the in-memory store returns when there is no in-progress row for
                // this key; the FK constraint gives us that same signal here (a journal row can only be
                // inserted once its run row exists, per the FK's own comment in the migration).
                if e.contains("foreign key") {
                    RunStoreError::NotStarted(key.canonical())
                } else {
                    unavailable(e)
                }
            })
    }

    fn in_flight(&self, account_id: &str) -> Result<Vec<JournalEntry>, RunStoreError> {
        let account_id = account_id.to_string();
        self.bridge
            .block_on(move |mut conn| async move {
                let rows: Vec<JournalRow> = diesel::sql_query(
                    "SELECT j.tag, j.broker_order_id, j.notional::text AS notional \
                     FROM rebalancer_run_journal j \
                     JOIN rebalancer_runs r ON r.account_id = j.account_id AND r.scheduled_for = j.scheduled_for AND r.sleeve_set = j.sleeve_set \
                     WHERE j.account_id = $1 AND r.status = 'in_progress'",
                )
                .bind::<Text, _>(&account_id)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("in_flight: {e}"))?;
                rows.into_iter().map(journal_row_to_entry).collect::<Result<Vec<_>, _>>()
            })
            .map_err(unavailable)
    }

    fn finish(&self, record: RunRecord) -> Result<(), RunStoreError> {
        let existing = self.fetch_one(&record.key.account_id, record.key.scheduled_for, &record.key.sleeve_set)?;
        match &existing {
            None => return Err(RunStoreError::NotStarted(record.key.canonical())),
            Some(r) if r.status == "done" => return Err(RunStoreError::AlreadyFinished(record.key.canonical())),
            Some(_) => {}
        }

        let tenant_id = self.tenants.get(&record.key.account_id);
        let acted = acted_rows(&record);
        let acted_sleeves = acted.iter().map(|r| r.sleeve_id.as_str()).collect::<Vec<_>>().join("+");
        if !acted.is_empty() && tenant_id == UNKNOWN_TENANT {
            // The ledger is tenant-scoped and its rows are permanent: never write one for an account whose
            // tenant was not resolved. Nothing was written yet, so the run stays in progress.
            return Err(RunStoreError::DecisionLedgerUnavailable(format!(
                "no tenant is registered for account {}; refusing to write its decision ledger",
                record.key.account_id
            )));
        }
        let provenance = self.provenance.as_ref().and_then(|r| r.get(&record.key.account_id));
        let key_canonical = record.key.canonical();
        let account_for_msg = record.key.account_id.clone();
        let account_id = record.key.account_id.clone();
        let scheduled_for = record.key.scheduled_for;
        let sleeve_set = record.key.sleeve_set.clone();
        let mode = mode_to_str(record.mode);
        let outcome_kind = outcome_to_str(record.outcome.kind).to_string();
        let outcome_code = record.outcome.code.clone();
        let outcome_message = record.outcome.message.clone();
        let mandate_hash = record.mandate_hash.clone();
        let mandate_version = record.mandate_version.map(|v| v as i32);
        let mandate_standing = record.mandate_standing.clone();
        let pre_json = record.pre_snapshot.as_ref().map(snapshot_to_json);
        let post_json = record.post_snapshot.as_ref().map(snapshot_to_json);
        let placed_json = placed_vec_to_json(&record.placed);
        let tickets_json = tickets_to_json(&record.tickets);
        let finished_at = record.finished_at;
        let record_debug = format!("{record:?}");
        let (plan_origin, plan_id, venue_environment) = match provenance {
            Some(p) => (Some(p.origin.as_str().to_string()), Some(p.plan_id), Some(p.venue_environment.as_str().to_string())),
            None => (None, None, None),
        };
        let acted_run = ActedRun { tenant: tenant_id, account_id: account_id.clone(), run_scheduled_for: scheduled_for, run_sleeve_set: sleeve_set.clone(), mode, acted_at: finished_at };

        // ONE transaction: the terminal run row, its equity snapshots and the ledger rows commit together or
        // not at all. The ledger guard trigger insists the run row is already `done` when a ledger row arrives,
        // so the UPDATE comes first and the ledger rows cannot exist without the run record.
        let outcome: Result<(), FinishError> = self
            .bridge
            .block_on(move |mut conn| async move {
                Ok(conn
                    .transaction::<(), FinishError, _>(|conn| {
                        async move {
                            let updated = diesel::sql_query(
                                "UPDATE rebalancer_runs SET \
                                 tenant_id = $1, status = 'done', mode = $4, finished_at = $5, \
                                 outcome_kind = $6, outcome_code = $7, outcome_message = $8, \
                                 mandate_hash = $9, mandate_version = $10, mandate_standing = $11, \
                                 pre_snapshot = $12, post_snapshot = $13, placed = $14, record_debug = $15, \
                                 tickets = $17, plan_origin = $18, plan_id = $19, venue_environment = $20 \
                                 WHERE account_id = $2 AND scheduled_for = $3 AND sleeve_set = $16 AND status = 'in_progress'",
                            )
                            .bind::<diesel::sql_types::Uuid, _>(tenant_id)
                            .bind::<Text, _>(&account_id)
                            .bind::<Timestamptz, _>(scheduled_for)
                            .bind::<Text, _>(mode)
                            .bind::<Timestamptz, _>(finished_at)
                            .bind::<Text, _>(&outcome_kind)
                            .bind::<Text, _>(&outcome_code)
                            .bind::<Text, _>(&outcome_message)
                            .bind::<Text, _>(&mandate_hash)
                            .bind::<Nullable<Integer>, _>(mandate_version)
                            .bind::<Text, _>(&mandate_standing)
                            .bind::<Nullable<Jsonb>, _>(&pre_json)
                            .bind::<Nullable<Jsonb>, _>(&post_json)
                            .bind::<Jsonb, _>(&placed_json)
                            .bind::<Text, _>(&record_debug)
                            .bind::<Text, _>(&sleeve_set)
                            .bind::<Jsonb, _>(&tickets_json)
                            .bind::<Nullable<Text>, _>(&plan_origin)
                            .bind::<Nullable<diesel::sql_types::Uuid>, _>(plan_id)
                            .bind::<Nullable<Text>, _>(&venue_environment)
                            .execute(conn)
                            .await?;
                            if updated != 1 {
                                // Someone finished it in the tiny window between the existence check above and this
                                // write (the WHERE clause's own atomicity decides the winner; this is the LOSER's
                                // view). Roll back: nothing of this call may persist.
                                return Err(FinishError::AlreadyFinished);
                            }
                            insert_equity_snapshots(conn, tenant_id, &record).await?;
                            if !acted.is_empty() {
                                match ledger::append_acted(conn, &acted_run, &acted).await? {
                                    None => {}
                                    Some(LedgerFault::Missing) => return Err(FinishError::LedgerMissing),
                                    Some(LedgerFault::ForeignTenant) => return Err(FinishError::LedgerForeignTenant),
                                }
                            }
                            Ok(())
                        }
                        .scope_boxed()
                    })
                    .await)
            })
            .map_err(unavailable)?;

        match outcome {
            Ok(()) => Ok(()),
            Err(FinishError::AlreadyFinished) => Err(RunStoreError::AlreadyFinished(key_canonical)),
            Err(FinishError::LedgerMissing) => Err(RunStoreError::DecisionLedgerUnavailable(ledger::missing_message(&account_for_msg, &acted_sleeves))),
            Err(FinishError::LedgerForeignTenant) => Err(RunStoreError::DecisionLedgerUnavailable(ledger::foreign_tenant_message(&account_for_msg, &acted_sleeves))),
            Err(FinishError::Db(e)) => Err(unavailable(format!("finish: {e}"))),
        }
    }

    fn last_snapshot(&self, account_id: &str) -> Result<Option<SnapshotSummary>, RunStoreError> {
        let account_id = account_id.to_string();
        self.bridge
            .block_on(move |mut conn| async move {
                #[derive(diesel::QueryableByName)]
                struct SnapRow {
                    #[diesel(sql_type = Jsonb)]
                    snap: Value,
                }
                let rows: Vec<SnapRow> = diesel::sql_query(
                    "SELECT COALESCE(post_snapshot, pre_snapshot) AS snap FROM rebalancer_runs \
                     WHERE account_id = $1 AND status = 'done' AND (post_snapshot IS NOT NULL OR pre_snapshot IS NOT NULL) \
                     ORDER BY finished_at DESC LIMIT 1",
                )
                .bind::<Text, _>(&account_id)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("last_snapshot: {e}"))?;
                match rows.into_iter().next() {
                    None => Ok(None),
                    Some(r) => snapshot_from_json(&r.snap).map(Some),
                }
            })
            .map_err(unavailable)
    }

    fn known_order_ids(&self, account_id: &str) -> Result<BTreeSet<String>, RunStoreError> {
        let account_id = account_id.to_string();
        self.bridge
            .block_on(move |mut conn| async move {
                #[derive(diesel::QueryableByName)]
                struct IdRow {
                    #[diesel(sql_type = Text)]
                    id: String,
                }
                let rows: Vec<IdRow> = diesel::sql_query(
                    "SELECT DISTINCT (elem->>'broker_order_id') AS id \
                     FROM rebalancer_runs, jsonb_array_elements(placed) elem \
                     WHERE account_id = $1 AND status = 'done' AND elem->>'broker_order_id' IS NOT NULL \
                     UNION \
                     SELECT DISTINCT broker_order_id AS id FROM rebalancer_run_journal \
                     WHERE account_id = $1 AND broker_order_id IS NOT NULL",
                )
                .bind::<Text, _>(&account_id)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("known_order_ids: {e}"))?;
                Ok(rows.into_iter().map(|r| r.id).collect())
            })
            .map_err(unavailable)
    }

    fn day_counters(&self, account_id: &str, day: NaiveDate) -> Result<DayCounters, RunStoreError> {
        let account_id = account_id.to_string();
        self.bridge
            .block_on(move |mut conn| async move {
                #[derive(diesel::QueryableByName)]
                struct DoneRow {
                    #[diesel(sql_type = Jsonb)]
                    placed: Value,
                }
                let done: Vec<DoneRow> = diesel::sql_query(
                    "SELECT placed FROM rebalancer_runs WHERE account_id = $1 AND trading_day = $2 AND status = 'done' AND mode = 'live'",
                )
                .bind::<Text, _>(&account_id)
                .bind::<Date, _>(day)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("day_counters (done): {e}"))?;

                let mut orders: u32 = 0;
                let mut turnover = Dec::ZERO;
                for row in &done {
                    let placed = placed_vec_from_json(&row.placed)?;
                    for p in placed.iter().filter(|p| matches!(p.phase, Phase::Sells | Phase::Buys)) {
                        if p.broker_order_id.is_some() && p.outcome != PlacedOutcome::AdoptedExisting {
                            orders = orders.saturating_add(1);
                            let value = p.executed_quantity.checked_mul(p.price).unwrap_or(Dec::ZERO);
                            turnover = turnover.checked_add(value).unwrap_or(turnover);
                        }
                    }
                }

                #[derive(diesel::QueryableByName)]
                struct JRow {
                    #[diesel(sql_type = Text)]
                    notional: String,
                }
                let journaled: Vec<JRow> = diesel::sql_query(
                    "SELECT j.notional::text AS notional FROM rebalancer_run_journal j \
                     JOIN rebalancer_runs r ON r.account_id = j.account_id AND r.scheduled_for = j.scheduled_for AND r.sleeve_set = j.sleeve_set \
                     WHERE j.account_id = $1 AND r.trading_day = $2 AND r.status = 'in_progress'",
                )
                .bind::<Text, _>(&account_id)
                .bind::<Date, _>(day)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("day_counters (journal): {e}"))?;
                for j in &journaled {
                    orders = orders.saturating_add(1);
                    let n = Dec::parse(&j.notional).map_err(|e| e.to_string())?;
                    turnover = turnover.checked_add(n).unwrap_or(turnover);
                }

                Ok(DayCounters { orders_today: orders, turnover_today: turnover })
            })
            .map_err(unavailable)
    }

    fn summaries(&self, account_id: &str) -> Result<Vec<RunSummary>, RunStoreError> {
        let account_id = account_id.to_string();
        self.bridge
            .block_on(move |mut conn| async move {
                let rows: Vec<RunRow> = diesel::sql_query(
                    "SELECT account_id, scheduled_for, sleeve_set, status, attempt, trading_day, mode, \
                     started_at, lease_until, finished_at, outcome_kind, outcome_code, outcome_message, \
                     mandate_hash, mandate_version, mandate_standing, pre_snapshot, post_snapshot, placed \
                     FROM rebalancer_runs WHERE account_id = $1 ORDER BY scheduled_for ASC",
                )
                .bind::<Text, _>(&account_id)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("summaries: {e}"))?;
                Ok(rows.iter().map(row_to_summary).collect())
            })
            .map_err(unavailable)
    }

    fn get(&self, key: &RunKey) -> Result<Option<RunRecord>, RunStoreError> {
        match self.fetch_one(&key.account_id, key.scheduled_for, &key.sleeve_set)? {
            None => Ok(None),
            Some(r) if r.status != "done" => Ok(None),
            Some(r) => row_to_record(r).map(Some).map_err(unavailable),
        }
    }

    /// `D_acted` from the decision ledger: the newest acted decision date of (account, sleeve) since the last
    /// `reset` row, or `None` when the account never acted on the sleeve (an entry).
    ///
    /// FAILS CLOSED, with `DecisionLedgerUnavailable`, whenever it cannot answer TRUSTWORTHILY: the ledger table is
    /// missing (the migration was not applied), no tenant is registered for the account, or the ledger already
    /// holds the account under another tenant. The two wrong guesses ("nothing acted" plans the ETF sleeve on
    /// every run; "everything acted" never plans it) are never made. A database failure is `Unavailable`, which the
    /// pipeline also treats as a fail-closed run.
    fn last_acted_decision(&self, account_id: &str, sleeve_id: &str) -> Result<Option<NaiveDate>, RunStoreError> {
        let tenant = self.tenants.get(account_id);
        if tenant == UNKNOWN_TENANT {
            return Err(RunStoreError::DecisionLedgerUnavailable(format!(
                "no tenant is registered for account {account_id} (sleeve {sleeve_id}); the decision ledger is tenant-scoped, so it cannot be read"
            )));
        }
        let (account, sleeve) = (account_id.to_string(), sleeve_id.to_string());
        let outcome = self
            .bridge
            .block_on(move |mut conn| async move { ledger::read_d_acted(&mut conn, tenant, &account, &sleeve).await.map_err(|e| format!("last_acted_decision: {e}")) })
            .map_err(unavailable)?;
        match outcome {
            ReadOutcome::Value(v) => Ok(v),
            ReadOutcome::Fault(LedgerFault::Missing) => Err(RunStoreError::DecisionLedgerUnavailable(ledger::missing_message(account_id, sleeve_id))),
            ReadOutcome::Fault(LedgerFault::ForeignTenant) => Err(RunStoreError::DecisionLedgerUnavailable(ledger::foreign_tenant_message(account_id, sleeve_id))),
        }
    }
}

