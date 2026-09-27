//! Shared harness of the env-gated pilot database tests (`pilot_ledger_db.rs`, `pilot_account_source_db.rs`).
//!
//! # Gate
//! `REBALANCER_PILOT_TEST_DB` = a SUPERUSER connection URL of a scratch Postgres server (any database on it, e.g.
//! `postgres://postgres@127.0.0.1:15441/postgres`). When it is unset every test prints `SKIPPED: ...` and returns.
//! When it is set, `REBALANCER_PILOT_TEST_MIGRATIONS` is REQUIRED (a missing one is a hard failure, never a skip): a
//! `:`-separated list of Diesel migration directories applied in order, each directory's sub-directories sorted by
//! name, each `up.sql` run as one transaction. The intended value is the public schema's migrations directory
//! followed by databaseschema-internal's, i.e. the real production baseline plus every internal migration.
//!
//! Each test creates its OWN database (`rbpilot_<random>`), applies the migrations, and drops it (and its role) on
//! drop. Nothing else on the server is touched. The stores under test connect as a RESTRICTED role
//! (`rbpilot_svc_<random>`) holding exactly the grants the PR proposes for the service, so the tests also prove the
//! least-privilege claim; the superuser is used only to seed and to inspect.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{DateTime, NaiveDate, Utc};
use diesel::sql_types::{Nullable, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use mandate_core::mandate::{canonical_hash, MandateBody};
use mandate_core::strategy_library::{entry_hash, seed_library};
use rebalancer_run::data::{Cadence, SleeveKind};
use rebalancer_run::record::{ExecutionMode, OutcomeKind, RunKey, RunOutcome, RunRecord, SleeveDecision, SnapshotSummary, TargetSummary};
use serde_json::{json, Value};
use uuid::Uuid;

pub const ENV_DB: &str = "REBALANCER_PILOT_TEST_DB";
pub const ENV_MIGRATIONS: &str = "REBALANCER_PILOT_TEST_MIGRATIONS";
pub const PILOT_MIGRATION_DIR: &str = "2026-09-27-000000_create_rebalancer_pilot_ledger";

pub fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

/// `postgres://user@host:port/db?x` with the database replaced.
fn with_database(url: &str, db: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let scheme_end = base.find("://").expect("a postgres URL") + 3;
    let slash = base[scheme_end..].find('/').map(|i| i + scheme_end).unwrap_or(base.len());
    let mut out = format!("{}/{}", &base[..slash], db);
    if let Some(q) = query {
        out.push('?');
        out.push_str(q);
    }
    out
}

/// The same URL with the user replaced (trust authentication on the scratch server: no password).
fn with_user(url: &str, user: &str) -> String {
    let scheme_end = url.find("://").expect("a postgres URL") + 3;
    let rest = &url[scheme_end..];
    let slash = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..slash];
    let host = authority.rsplit_once('@').map(|(_, h)| h).unwrap_or(authority);
    format!("{}{}@{}{}", &url[..scheme_end], user, host, &rest[slash..])
}

#[derive(Clone, Copy)]
pub struct Opts {
    /// Apply databaseschema-internal's `2026-09-27-000000_create_rebalancer_pilot_ledger` (default true). `false` builds
    /// the database exactly as it is in production today (the ledger table does not exist).
    pub pilot_migration: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Opts { pilot_migration: true }
    }
}

pub struct TestDb {
    admin_url: String,
    pub name: String,
    role: String,
    /// Superuser URL of the scratch database (seeding and inspection only).
    pub url: String,
    /// Restricted-role URL: what every store under test connects with.
    pub svc_url: String,
}

/// `None` (after printing `SKIPPED`) when the gate variable is unset.
pub fn create(test: &str, opts: Opts) -> Option<TestDb> {
    let Ok(admin_url) = std::env::var(ENV_DB) else {
        eprintln!("SKIPPED: {test}: {ENV_DB} is not set (a superuser URL of a scratch Postgres; see tests/common/mod.rs)");
        return None;
    };
    let dirs = std::env::var(ENV_MIGRATIONS).unwrap_or_else(|_| panic!("{ENV_DB} is set but {ENV_MIGRATIONS} is not: refusing to run against an unmigrated server"));
    let id = Uuid::new_v4().simple().to_string();
    let name = format!("rbpilot_{}", &id[..12]);
    let role = format!("rbpilot_svc_{}", &id[..12]);
    let url = with_database(&admin_url, &name);
    let svc_url = with_user(&url, &role);
    let db = TestDb { admin_url: admin_url.clone(), name: name.clone(), role: role.clone(), url: url.clone(), svc_url };
    let r = rt();
    r.block_on(async {
        let mut admin = AsyncPgConnection::establish(&admin_url).await.expect("connect to the scratch server");
        admin.batch_execute(&format!("CREATE DATABASE {name}")).await.expect("create the scratch database");
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect to the scratch database");
        for dir in dirs.split(':').filter(|d| !d.is_empty()) {
            let mut subs: Vec<PathBuf> = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("migrations dir {dir}: {e}")).filter_map(|e| e.ok()).map(|e| e.path()).filter(|p| p.join("up.sql").is_file()).collect();
            subs.sort();
            for sub in subs {
                let n = sub.file_name().unwrap().to_string_lossy().to_string();
                if n == PILOT_MIGRATION_DIR && !opts.pilot_migration {
                    continue;
                }
                let sql = std::fs::read_to_string(sub.join("up.sql")).unwrap();
                conn.batch_execute(&sql).await.unwrap_or_else(|e| panic!("applying migration {n}: {e}"));
            }
        }
        // The restricted service role: exactly the grants proposed in the migration PR (scripts/rebalancer_pilot_ledger/role.sql).
        conn.batch_execute(&format!(
            "CREATE ROLE {role} LOGIN;
             GRANT CONNECT ON DATABASE {name} TO {role};
             GRANT USAGE ON SCHEMA public TO {role};
             GRANT SELECT ON mandates, strategy_plans TO {role};
             GRANT SELECT (id, tenant_id, exchange, label, is_testnet, is_enabled, is_valid, last_validated_at, deleted_at, created_at) ON exchange_credentials TO {role};
             GRANT SELECT, INSERT, UPDATE ON rebalancer_account_state, rebalancer_equity_snapshots, rebalancer_runs, rebalancer_run_journal, rebalancer_alerts, rebalancer_kill_flags TO {role};
             GRANT SELECT, INSERT, UPDATE, DELETE ON rebalancer_account_locks TO {role};"
        ))
        .await
        .expect("create the restricted role");
        if opts.pilot_migration {
            conn.batch_execute(&format!("GRANT SELECT ON rebalancer_pilot_plans TO {role}; GRANT SELECT, INSERT ON rebalancer_decision_ledger TO {role};")).await.expect("grant the pilot tables");
        }
    });
    Some(db)
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let (admin_url, name, role) = (self.admin_url.clone(), self.name.clone(), self.role.clone());
        let _ = std::thread::spawn(move || {
            rt().block_on(async {
                if let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await {
                    let _ = admin.batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)")).await;
                    let _ = admin.batch_execute(&format!("DROP ROLE IF EXISTS {role}")).await;
                }
            });
        })
        .join();
    }
}

#[derive(diesel::QueryableByName)]
struct Scalar {
    #[diesel(sql_type = Nullable<Text>)]
    v: Option<String>,
}

impl TestDb {
    /// Run statements as the SUPERUSER (seeding). Panics on error.
    pub fn exec(&self, sql: &str) {
        self.try_exec(sql).unwrap_or_else(|e| panic!("exec failed: {e}\n{sql}"))
    }

    /// Run statements as the superuser and return the error text, if any.
    pub fn try_exec(&self, sql: &str) -> Result<(), String> {
        let url = self.url.clone();
        let sql = sql.to_string();
        rt().block_on(async move {
            let mut c = AsyncPgConnection::establish(&url).await.map_err(|e| e.to_string())?;
            c.batch_execute(&sql).await.map_err(|e| e.to_string())
        })
    }

    /// Run statements as the RESTRICTED role and return the error text, if any.
    pub fn try_exec_as_svc(&self, sql: &str) -> Result<(), String> {
        let url = self.svc_url.clone();
        let sql = sql.to_string();
        rt().block_on(async move {
            let mut c = AsyncPgConnection::establish(&url).await.map_err(|e| e.to_string())?;
            c.batch_execute(&sql).await.map_err(|e| e.to_string())
        })
    }

    /// `SELECT <expr> AS v` as text, first row (None for no row or NULL). The query MUST alias its column `v`.
    pub fn scalar(&self, sql: &str) -> Option<String> {
        let url = self.url.clone();
        let sql = sql.to_string();
        rt().block_on(async move {
            let mut c = AsyncPgConnection::establish(&url).await.expect("connect");
            let rows: Vec<Scalar> = diesel::sql_query(sql).get_results(&mut c).await.expect("query");
            rows.into_iter().next().and_then(|r| r.v)
        })
    }

    pub fn count(&self, sql_from_where: &str) -> i64 {
        self.scalar(&format!("SELECT count(*)::text AS v FROM {sql_from_where}")).unwrap().parse().unwrap()
    }

    pub fn pool(&self, size: usize) -> rebalancer_store::pg::Pool {
        rebalancer_store::pg::create_pool(&self.svc_url, size).expect("pool")
    }

    // ------------------------------------------------------------------------------------------------
    // Seeding (superuser; triggers stay ON unless a test switches them off explicitly)
    // ------------------------------------------------------------------------------------------------

    pub fn seed_tenant(&self, name: &str) -> Uuid {
        let id = Uuid::new_v4();
        self.exec(&format!("INSERT INTO tenants (id, company_name) VALUES ('{id}', '{name}')"));
        id
    }

    pub fn seed_credential(&self, tenant: Uuid, exchange: &str, is_testnet: bool) -> Uuid {
        let id = Uuid::new_v4();
        self.exec(&format!(
            "INSERT INTO exchange_credentials (id, tenant_id, exchange, label, api_key_encrypted, api_secret_encrypted, is_testnet) \
             VALUES ('{id}', '{tenant}', '{exchange}', 'label-{}', 'aes:SECRET-KEY-MUST-NOT-BE-READ', 'aes:SECRET-SECRET-MUST-NOT-BE-READ', {is_testnet})",
            &id.simple().to_string()[..8]
        ));
        id
    }

    /// A mandate row. `level` patches the fixture's autonomy level; `signed = false` leaves the grant metadata NULL.
    pub fn seed_mandate(&self, tenant: Uuid, account: Uuid, version: i32, status: &str, level: &str, signed: bool, extra_may_place_orders: bool) -> (Uuid, MandateBody) {
        let mut body = mandate_json(level);
        if !extra_may_place_orders {
            body["autonomy"]["may"] = json!(["cancel_orders", "halt", "flatten"]);
        }
        let parsed: MandateBody = serde_json::from_value(body.clone()).unwrap();
        let hash = canonical_hash(&parsed);
        let id = Uuid::new_v4();
        let (granted, effective, review) = if signed { ("now()", "now() - interval '1 day'", "now() + interval '90 days'") } else { ("NULL", "NULL", "NULL") };
        self.exec(&format!(
            "INSERT INTO mandates (id, tenant_id, account_id, version, status, body, body_hash, granted_at, effective_from, review_by) \
             VALUES ('{id}', '{tenant}', '{account}', {version}, '{status}', $j${body}$j$::jsonb, '{hash}', {granted}, {effective}, {review})"
        ));
        (id, parsed)
    }

    /// A pilot plan through the real insert path (all triggers and CHECKs on). Returns the SQL error text on refusal.
    pub fn try_seed_plan(&self, tenant: Uuid, account: Uuid, mandate: Uuid, version: i32, execution: &str) -> Result<Uuid, String> {
        let id = Uuid::new_v4();
        let (entry_hash, body) = pilot_plan_body("alpaca", "etf_trend");
        self.try_exec(&format!(
            "INSERT INTO rebalancer_pilot_plans (id, tenant_id, account_id, mandate_id, mandate_version, execution, authored_by, reason, acknowledgements, body, entry_id, entry_version, entry_hash, credential_fingerprint) \
             VALUES ('{id}', '{tenant}', '{account}', '{mandate}', {version}, '{execution}', 'user_owner', 'owner authored paper pilot plan for the etf sleeve', $j${ack}$j$::jsonb, $j${body}$j$::jsonb, 'etf_trend_faber', 1, '{entry_hash}', '0123456789abcdef')",
            ack = ack_json()
        ))?;
        Ok(id)
    }

    pub fn seed_plan(&self, tenant: Uuid, account: Uuid, mandate: Uuid, version: i32, execution: &str) -> Uuid {
        self.try_seed_plan(tenant, account, mandate, version, execution).unwrap_or_else(|e| panic!("seed_plan: {e}"))
    }

    /// A plan inserted with every trigger switched off for the statement (superuser), to build rows the guard triggers
    /// would refuse, so the ACCOUNT SOURCE's own refusals are tested independently of them. CHECK constraints stay on.
    pub fn seed_plan_bypassing_triggers(&self, tenant: Uuid, account: Uuid, mandate: Uuid, version: i32, execution: &str, venue: &str, kind: &str, entry_hash_override: Option<&str>) -> Uuid {
        let id = Uuid::new_v4();
        let (entry_hash, body) = pilot_plan_body(venue, kind);
        let entry_hash = entry_hash_override.map(str::to_string).unwrap_or(entry_hash);
        let body_hash = self.scalar(&format!("SELECT encode(sha256(convert_to($j${body}$j$::jsonb::text,'UTF8')),'hex') AS v")).unwrap();
        self.exec(&format!(
            "BEGIN; SET LOCAL session_replication_role = replica;
             INSERT INTO rebalancer_pilot_plans (id, tenant_id, account_id, mandate_id, mandate_version, execution, authored_by, reason, acknowledgements, body, body_hash, entry_id, entry_version, entry_hash, credential_fingerprint) \
             VALUES ('{id}', '{tenant}', '{account}', '{mandate}', {version}, '{execution}', 'user_owner', 'owner authored paper pilot plan for the etf sleeve', $j${ack}$j$::jsonb, $j${body}$j$::jsonb, '{body_hash}', 'etf_trend_faber', 1, '{entry_hash}', '0123456789abcdef');
             COMMIT;",
            ack = ack_json()
        ));
        id
    }

    /// The whole happy path: tenant, paper Alpaca credential, active signed L2 mandate, one plan of `execution`.
    pub fn seed_pilot(&self, execution: &str) -> Pilot {
        let tenant = self.seed_tenant(&format!("Tenant {}", Uuid::new_v4().simple()));
        let account = self.seed_credential(tenant, "alpaca_paper", true);
        let (mandate, body) = self.seed_mandate(tenant, account, 1, "active", "L2", true, true);
        let plan = self.seed_plan(tenant, account, mandate, 1, execution);
        Pilot { tenant, account, mandate, plan, body }
    }
}

pub struct Pilot {
    pub tenant: Uuid,
    pub account: Uuid,
    pub mandate: Uuid,
    pub plan: Uuid,
    pub body: MandateBody,
}

pub fn ack_json() -> Value {
    json!({
        "not_proposal_derived": true,
        "entry_evidence": "reference_only",
        "entry_citation_check": "failed",
        "no_edge_claimed": true,
        "paper_only": true,
        "entry_on_decision_in_force": "2026-08-31"
    })
}

/// The library entry hash of `etf_trend_faber` v1 (what the platform itself computes) and a plan body around it.
pub fn pilot_plan_body(venue: &str, kind: &str) -> (String, Value) {
    let lib = seed_library().unwrap();
    let entry = lib.iter().find(|e| e.id == "etf_trend_faber" && e.version == 1).expect("etf_trend_faber v1 in the seed library");
    let hash = entry_hash(entry);
    let body = json!({"sleeves": [{
        "sleeve_id": "etf", "entry_id": "etf_trend_faber", "entry_version": 1, "entry_hash": hash,
        "kind": kind, "share": "1.0", "venue": venue, "asset_class": "us_etf", "quote": ""
    }]});
    (hash, body)
}

/// The mandate-core fixture (`crates/mandate-core/tests/fixtures/baseline_mandate.json`) at a chosen autonomy level.
pub fn mandate_json(level: &str) -> Value {
    let raw = include_str!("../../../mandate-core/tests/fixtures/baseline_mandate.json");
    let mut v: Value = serde_json::from_str(raw).unwrap();
    v["autonomy"]["level"] = json!(level);
    v
}

// ---------------------------------------------------------------------------------------------------
// Run records
// ---------------------------------------------------------------------------------------------------

pub fn ts(s: &str) -> DateTime<Utc> {
    s.parse().unwrap()
}

pub fn day(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

pub fn decision(sleeve: &str, decision_date: NaiveDate, acted: bool, entry: bool, lag: u32) -> SleeveDecision {
    SleeveDecision {
        sleeve: sleeve.to_string(),
        kind: SleeveKind::EtfTrend,
        cadence: Cadence::OnDecision,
        decision_date,
        computable_decision_date: decision_date,
        newest_bar_date: decision_date + chrono::Duration::days(1 + i64::from(lag)),
        lag_sessions: lag,
        last_acted_decision: None,
        pending: true,
        entry,
        planned: true,
        acted,
        instruments: vec![],
    }
}

pub struct RecordSpec<'a> {
    pub account: &'a str,
    pub scheduled_for: DateTime<Utc>,
    pub sleeve_set: &'a str,
    pub mode: ExecutionMode,
    pub outcome: OutcomeKind,
    pub decisions: Vec<SleeveDecision>,
}

pub fn record(spec: RecordSpec<'_>) -> RunRecord {
    use broker_adapters::Dec;
    let snap = SnapshotSummary {
        taken_at: spec.scheduled_for,
        equity: Dec::parse("5000.00").unwrap(),
        cash: Dec::parse("5000.00").unwrap(),
        derived_equity: Dec::parse("5000.00").unwrap(),
        holdings: BTreeMap::new(),
        marks: BTreeMap::new(),
        open_order_ids: vec![],
    };
    let targets = spec
        .decisions
        .iter()
        .filter(|d| d.planned)
        .map(|d| TargetSummary { sleeve: d.sleeve.clone(), decision_date: d.decision_date, data_fingerprint: format!("fp-{}", d.decision_date), weights: vec![] })
        .collect();
    RunRecord {
        key: RunKey { account_id: spec.account.to_string(), scheduled_for: spec.scheduled_for, sleeve_set: spec.sleeve_set.to_string() },
        mode: spec.mode,
        attempt: 1,
        trading_day: spec.scheduled_for.date_naive(),
        scheduled_for: spec.scheduled_for,
        started_at: spec.scheduled_for,
        finished_at: spec.scheduled_for + chrono::Duration::seconds(30),
        outcome: RunOutcome { kind: spec.outcome, code: "RUN_COMPLETED".to_string(), message: "test".to_string() },
        mandate_hash: "a".repeat(64),
        mandate_version: Some(1),
        mandate_standing: "active".to_string(),
        deployment_digest: None,
        data_fingerprints: vec![],
        pre_snapshot: Some(snap.clone()),
        post_snapshot: Some(snap),
        recon: vec![],
        state_before: None,
        state_after: None,
        transitions: vec![],
        risk: None,
        decisions: spec.decisions,
        targets,
        plan: None,
        replan: None,
        tickets: vec![],
        placed: vec![],
        cleanup: vec![],
        flatten: None,
        alerts: vec![],
        alert_delivery_failures: vec![],
        steps: vec![],
    }
}
