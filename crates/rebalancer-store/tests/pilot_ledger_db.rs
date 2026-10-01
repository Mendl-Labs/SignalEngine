//! The Postgres decision ledger behind `PgRunStore` (paper-pilot slice S-2): round trip, monotonicity, append-only,
//! idempotent and ATOMIC `finish`, tenant scoping, fail-closed behaviour when the table is missing, reset, and the run
//! provenance columns. Env-gated: see `tests/common/mod.rs` (`REBALANCER_PILOT_TEST_DB`, `REBALANCER_PILOT_TEST_MIGRATIONS`);
//! every test prints `SKIPPED` and returns when the gate is unset.

mod common;

use std::sync::Arc;

use broker_adapters::{Dec, Side};
use chrono::NaiveDate;
use common::{create, day, decision, record, ts, Opts, RecordSpec, TestDb};
use rebalancer_core::planner::PlannedOrder;
use rebalancer_run::record::{ExecutionMode, OutcomeKind, RunKey};
use rebalancer_run::stores::{Begin, RunStore, RunStoreError};
use rebalancer_store::{AccountTenants, PgRunStore, PlanOrigin, PlanProvenance, PlanProvenanceRegistry, VenueEnvironment};
use uuid::Uuid;

fn new_store(db: &TestDb, tenants: &Arc<AccountTenants>) -> PgRunStore {
    PgRunStore::new(db.pool(4), tenants.clone()).expect("store")
}

fn key(account: &str, sched: &str) -> RunKey {
    RunKey { account_id: account.to_string(), scheduled_for: ts(sched), sleeve_set: "etf".to_string() }
}

/// begin + finish one run with the given decisions.
fn run(store: &PgRunStore, account: &str, sched: &str, mode: ExecutionMode, outcome: OutcomeKind, decisions: Vec<rebalancer_run::record::SleeveDecision>) -> Result<(), RunStoreError> {
    let k = key(account, sched);
    match store.begin(&k, ts(sched).date_naive(), ts(sched), 900).expect("begin") {
        Begin::Started { .. } => {}
        other => panic!("expected Started, got {other:?}"),
    }
    store.finish(record(RecordSpec { account, scheduled_for: ts(sched), sleeve_set: "etf", mode, outcome, decisions }))
}

fn acted(sleeve: &str, d: NaiveDate) -> rebalancer_run::record::SleeveDecision {
    decision(sleeve, d, true, false, 1)
}

fn setup(test: &str, opts: Opts) -> Option<(TestDb, Arc<AccountTenants>, String, Uuid)> {
    let db = create(test, opts)?;
    let tenants = Arc::new(AccountTenants::new());
    let tenant = db.seed_tenant("Tenant A");
    let account = Uuid::new_v4().to_string();
    tenants.register(&account, tenant);
    Some((db, tenants, account, tenant))
}

fn ledger_dates(db: &TestDb, account: &str) -> String {
    db.scalar(&format!("SELECT COALESCE(string_agg(decision_date::text, ',' ORDER BY seq), '') AS v FROM rebalancer_decision_ledger WHERE account_id = '{account}' AND kind = 'acted'")).unwrap()
}

#[test]
fn entry_then_month_boundary_round_trips_through_the_ledger_and_survives_a_restart() {
    let Some((db, tenants, account, tenant)) = setup("entry_then_month_boundary", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), None, "a never-acted sleeve is an entry");

    // Entry on the decision in force (Ruling 9a), then the first month boundary.
    run(&store, &account, "2026-10-08T15:00:00Z", ExecutionMode::Assisted, OutcomeKind::Completed, vec![decision("etf", day(2026, 8, 31), true, true, 5)]).unwrap();
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 8, 31)));
    drop(store);

    // "Restart": a fresh store object over a fresh pool reads the same D_acted.
    let store = new_store(&db, &tenants);
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 8, 31)), "D_acted survives a restart");
    run(&store, &account, "2026-11-03T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![decision("etf", day(2026, 10, 30), true, false, 1)]).unwrap();
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 10, 30)));

    assert_eq!(ledger_dates(&db, &account), "2026-08-31,2026-10-30");
    // The structured Ruling 7 / R20 fields landed in real columns.
    assert_eq!(
        db.scalar(&format!("SELECT (entry::text || '|' || lag_sessions::text || '|' || mode || '|' || data_fingerprint || '|' || epoch::text || '|' || tenant_id::text || '|' || computable_decision_date::text) AS v FROM rebalancer_decision_ledger WHERE account_id = '{account}' ORDER BY seq LIMIT 1")).unwrap(),
        format!("true|5|assisted|fp-2026-08-31|0|{tenant}|2026-08-31")
    );
    assert_eq!(
        db.scalar(&format!("SELECT (entry::text || '|' || lag_sessions::text || '|' || mode || '|' || newest_bar_date::text) AS v FROM rebalancer_decision_ledger WHERE account_id = '{account}' ORDER BY seq DESC LIMIT 1")).unwrap(),
        "false|1|live|2026-11-01"
    );
}

#[test]
fn only_decisions_flagged_acted_advance_d_acted() {
    let Some((db, tenants, account, _)) = setup("only_acted", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    // Planned but NOT acted (the pipeline's call: e.g. a run that failed closed) writes nothing.
    run(&store, &account, "2026-10-08T15:00:00Z", ExecutionMode::Live, OutcomeKind::FailedClosed, vec![decision("etf", day(2026, 8, 31), false, true, 5)]).unwrap();
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), None);
    assert_eq!(db.count(&format!("rebalancer_decision_ledger WHERE account_id = '{account}'")), 0);
    // Two sleeves, one acted: only that one advances.
    run(&store, &account, "2026-10-09T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 8, 31)), decision("fx", day(2026, 8, 31), false, true, 1)]).unwrap();
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 8, 31)));
    assert_eq!(store.last_acted_decision(&account, "fx").unwrap(), None);
}

#[test]
fn an_older_or_equal_decision_never_overwrites_d_acted() {
    let Some((db, tenants, account, _)) = setup("monotone", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    run(&store, &account, "2026-11-03T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 10, 30))]).unwrap();
    // A later run carrying an OLDER acted decision: the run finishes, D_acted stays, no row is written.
    run(&store, &account, "2026-11-04T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 9, 30))]).unwrap();
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 10, 30)));
    // An EQUAL decision (a second run acting on the same one): also unchanged, no duplicate row.
    run(&store, &account, "2026-11-05T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 10, 30))]).unwrap();
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 10, 30)));
    assert_eq!(ledger_dates(&db, &account), "2026-10-30");
    assert_eq!(db.count(&format!("rebalancer_runs WHERE account_id = '{account}' AND status = 'done'")), 3, "every run still finished");
}

#[test]
fn records_finishing_out_of_order_never_move_d_acted_backwards() {
    let Some((db, tenants, account, _)) = setup("out_of_order", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    let (ka, kb) = (key(&account, "2026-10-02T15:00:00Z"), key(&account, "2026-11-03T15:00:00Z"));
    store.begin(&ka, day(2026, 10, 2), ts("2026-10-02T15:00:00Z"), 900).unwrap();
    store.begin(&kb, day(2026, 11, 3), ts("2026-11-03T15:00:00Z"), 900).unwrap();
    let rec = |sched: &str, d: NaiveDate| record(RecordSpec { account: &account, scheduled_for: ts(sched), sleeve_set: "etf", mode: ExecutionMode::Live, outcome: OutcomeKind::Completed, decisions: vec![acted("etf", d)] });
    store.finish(rec("2026-11-03T15:00:00Z", day(2026, 10, 30))).unwrap(); // the newer decision finishes FIRST
    store.finish(rec("2026-10-02T15:00:00Z", day(2026, 9, 30))).unwrap(); // the older one after
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 10, 30)));
    assert_eq!(ledger_dates(&db, &account), "2026-10-30");
}

#[test]
fn a_repeated_run_key_is_idempotent_and_writes_one_ledger_row() {
    let Some((db, tenants, account, _)) = setup("idempotent", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    run(&store, &account, "2026-10-08T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 8, 31))]).unwrap();
    // Finishing the same run key again is refused (records are immutable) and adds nothing.
    let again = store.finish(record(RecordSpec { account: &account, scheduled_for: ts("2026-10-08T15:00:00Z"), sleeve_set: "etf", mode: ExecutionMode::Live, outcome: OutcomeKind::Completed, decisions: vec![acted("etf", day(2026, 8, 31))] }));
    assert!(matches!(again, Err(RunStoreError::AlreadyFinished(_))), "{again:?}");
    assert_eq!(db.count(&format!("rebalancer_decision_ledger WHERE account_id = '{account}'")), 1);
    // begin() on the finished key is the duplicate-slot no-op.
    match store.begin(&key(&account, "2026-10-08T15:00:00Z"), day(2026, 10, 8), ts("2026-10-08T15:05:00Z"), 900).unwrap() {
        Begin::AlreadyDone(_) => {}
        other => panic!("expected AlreadyDone, got {other:?}"),
    }
    // Equity snapshots were written once, by the one finish.
    assert_eq!(db.count(&format!("rebalancer_equity_snapshots WHERE account_id = '{account}'")), 2);
}

#[test]
fn finish_is_atomic_a_failing_ledger_write_rolls_back_the_run_record() {
    let Some((db, tenants, account, _)) = setup("atomic", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    let sched = "2026-10-08T15:00:00Z";
    let k = key(&account, sched);
    store.begin(&k, day(2026, 10, 8), ts(sched), 900).unwrap();

    // The ledger insert FAILS (a sleeve id longer than the column), after the run row was updated and the equity
    // snapshots inserted inside the same transaction: none of it may persist.
    let long = "s".repeat(200);
    let bad = record(RecordSpec { account: &account, scheduled_for: ts(sched), sleeve_set: "etf", mode: ExecutionMode::Live, outcome: OutcomeKind::Completed, decisions: vec![acted(&long, day(2026, 8, 31))] });
    let err = store.finish(bad).unwrap_err();
    assert!(matches!(err, RunStoreError::Unavailable(_)), "{err:?}");
    assert_eq!(db.scalar(&format!("SELECT status AS v FROM rebalancer_runs WHERE account_id = '{account}'")).unwrap(), "in_progress", "the run row was rolled back");
    assert_eq!(db.count(&format!("rebalancer_equity_snapshots WHERE account_id = '{account}'")), 0, "the equity snapshots were rolled back");
    assert_eq!(db.count(&format!("rebalancer_decision_ledger WHERE account_id = '{account}'")), 0);
    assert!(store.get(&k).unwrap().is_none(), "no finished record exists");

    // The retry with a good record succeeds and lands exactly once.
    store.finish(record(RecordSpec { account: &account, scheduled_for: ts(sched), sleeve_set: "etf", mode: ExecutionMode::Live, outcome: OutcomeKind::Completed, decisions: vec![acted("etf", day(2026, 8, 31))] })).unwrap();
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 8, 31)));
    assert_eq!(db.count(&format!("rebalancer_equity_snapshots WHERE account_id = '{account}'")), 2);
}

#[test]
fn a_crash_between_the_run_record_and_the_ledger_leaves_neither() {
    // Same property from the other side: the ledger write is refused by the DATABASE (a foreign-tenant row already
    // holds the account), which aborts the transaction the run row was updated in.
    let Some((db, tenants, account, _)) = setup("crash_between", Opts::default()) else { return };
    let other_tenant = db.seed_tenant("Tenant B");
    // Tenant B (wrongly) owns a ledger row for this account id.
    db.exec(&format!(
        "INSERT INTO rebalancer_runs (tenant_id, account_id, scheduled_for, sleeve_set, status, trading_day, mode, started_at, lease_until, finished_at) \
         VALUES ('{other_tenant}', '{account}', '2026-09-01T15:00:00Z', 'etf', 'done', '2026-09-01', 'live', now(), now(), now());
         INSERT INTO rebalancer_decision_ledger (tenant_id, account_id, sleeve_id, kind, decision_date, run_scheduled_for, run_sleeve_set, mode, acted_at) \
         VALUES ('{other_tenant}', '{account}', 'etf', 'acted', '2026-07-31', '2026-09-01T15:00:00Z', 'etf', 'live', now());"
    ));
    let store = new_store(&db, &tenants);
    let sched = "2026-10-08T15:00:00Z";
    let err = run(&store, &account, sched, ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 8, 31))]).unwrap_err();
    assert!(matches!(err, RunStoreError::DecisionLedgerUnavailable(_)), "{err:?}");
    assert_eq!(db.scalar(&format!("SELECT status AS v FROM rebalancer_runs WHERE account_id = '{account}' AND scheduled_for = '{sched}'")).unwrap(), "in_progress");
    assert_eq!(db.count(&format!("rebalancer_equity_snapshots WHERE account_id = '{account}'")), 0);
    assert_eq!(db.count(&format!("rebalancer_decision_ledger WHERE account_id = '{account}'")), 1, "only tenant B's pre-existing row");
}

#[test]
fn tenant_a_can_neither_read_nor_advance_tenant_bs_ledger() {
    let Some((db, _tenants, account_b, tenant_b)) = setup("tenant_scoping", Opts::default()) else { return };
    let tenant_a = db.seed_tenant("Tenant A2");
    // Tenant B legitimately acts on its own account.
    let tenants_b = Arc::new(AccountTenants::new());
    tenants_b.register(&account_b, tenant_b);
    let store_b = new_store(&db, &tenants_b);
    run(&store_b, &account_b, "2026-10-08T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 8, 31))]).unwrap();
    assert_eq!(store_b.last_acted_decision(&account_b, "etf").unwrap(), Some(day(2026, 8, 31)));

    // A store whose registry (wrongly or maliciously) says the SAME account belongs to tenant A.
    let tenants_a = Arc::new(AccountTenants::new());
    tenants_a.register(&account_b, tenant_a);
    let store_a = new_store(&db, &tenants_a);
    // READ: an error, never `None` (which would look like an entry and re-buy) and never B's date.
    match store_a.last_acted_decision(&account_b, "etf") {
        Err(RunStoreError::DecisionLedgerUnavailable(msg)) => assert!(!msg.contains("2026"), "the refusal must not leak another tenant's dates: {msg}"),
        other => panic!("tenant A must not read tenant B's ledger, got {other:?}"),
    }
    // WRITE: refused, and the run stays in progress (nothing of tenant A's finish persisted).
    let sched = "2026-11-03T15:00:00Z";
    let err = run(&store_a, &account_b, sched, ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 10, 30))]).unwrap_err();
    assert!(matches!(err, RunStoreError::DecisionLedgerUnavailable(_)), "{err:?}");
    assert_eq!(ledger_dates(&db, &account_b), "2026-08-31", "tenant B's ledger is untouched");
    // Tenant A's OWN account is unaffected by B's rows.
    let account_a = Uuid::new_v4().to_string();
    tenants_a.register(&account_a, tenant_a);
    assert_eq!(store_a.last_acted_decision(&account_a, "etf").unwrap(), None);
    run(&store_a, &account_a, "2026-10-08T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 9, 30))]).unwrap();
    assert_eq!(store_a.last_acted_decision(&account_a, "etf").unwrap(), Some(day(2026, 9, 30)));
    assert_eq!(store_b.last_acted_decision(&account_b, "etf").unwrap(), Some(day(2026, 8, 31)), "and B's account still reads its own value");
}

#[test]
fn an_account_with_no_registered_tenant_never_reads_or_writes_the_ledger() {
    let Some((db, tenants, _account, _)) = setup("unknown_tenant", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    let stranger = Uuid::new_v4().to_string(); // never registered
    assert!(matches!(store.last_acted_decision(&stranger, "etf"), Err(RunStoreError::DecisionLedgerUnavailable(_))));
    let err = run(&store, &stranger, "2026-10-08T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 8, 31))]).unwrap_err();
    assert!(matches!(err, RunStoreError::DecisionLedgerUnavailable(_)), "{err:?}");
    assert_eq!(db.count(&format!("rebalancer_decision_ledger WHERE account_id = '{stranger}'")), 0);
    assert_eq!(db.scalar(&format!("SELECT status AS v FROM rebalancer_runs WHERE account_id = '{stranger}'")).unwrap(), "in_progress");
}

#[test]
fn d_acted_is_read_for_the_right_account_and_the_right_sleeve() {
    let Some((db, tenants, account_x, tenant)) = setup("right_account", Opts::default()) else { return };
    let account_y = Uuid::new_v4().to_string();
    tenants.register(&account_y, tenant);
    let store = new_store(&db, &tenants);
    run(&store, &account_x, "2026-10-08T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 10, 30)), acted("fx", day(2026, 9, 30))]).unwrap();
    run(&store, &account_y, "2026-10-08T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 8, 31))]).unwrap();
    assert_eq!(store.last_acted_decision(&account_x, "etf").unwrap(), Some(day(2026, 10, 30)));
    assert_eq!(store.last_acted_decision(&account_x, "fx").unwrap(), Some(day(2026, 9, 30)));
    assert_eq!(store.last_acted_decision(&account_y, "etf").unwrap(), Some(day(2026, 8, 31)));
    assert_eq!(store.last_acted_decision(&account_y, "fx").unwrap(), None, "another account's sleeve is not this account's");
    assert_eq!(store.last_acted_decision(&account_y, "nope").unwrap(), None);
}

#[test]
fn a_missing_ledger_table_fails_closed_with_a_typed_error() {
    // The database exactly as production is today: none of the pilot migration.
    let Some((db, tenants, account, _)) = setup("missing_table_old_schema", Opts { pilot_migration: false }) else { return };
    let store = new_store(&db, &tenants);
    match store.last_acted_decision(&account, "etf") {
        Err(RunStoreError::DecisionLedgerUnavailable(msg)) => {
            assert!(msg.contains("rebalancer_decision_ledger") && msg.contains("2026-09-27-000000_create_rebalancer_pilot_ledger"), "names the table and the migration: {msg}");
            assert!(msg.contains(&account) && msg.contains("etf"), "names the account and the sleeve: {msg}");
        }
        other => panic!("must fail closed with DecisionLedgerUnavailable, got {other:?}"),
    }
}

#[test]
fn dropping_the_ledger_table_fails_reads_and_acting_finishes_closed_and_leaves_the_run_in_progress() {
    let Some((db, tenants, account, _)) = setup("missing_table_dropped", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    db.exec("DROP TABLE rebalancer_decision_ledger");
    assert!(matches!(store.last_acted_decision(&account, "etf"), Err(RunStoreError::DecisionLedgerUnavailable(_))));
    let sched = "2026-10-08T15:00:00Z";
    let err = run(&store, &account, sched, ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 8, 31))]).unwrap_err();
    match &err {
        RunStoreError::DecisionLedgerUnavailable(m) => assert!(m.contains("rebalancer_decision_ledger"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(db.scalar(&format!("SELECT status AS v FROM rebalancer_runs WHERE account_id = '{account}'")).unwrap(), "in_progress", "the finish rolled back");
    // A run that acted on nothing (a crypto-only account, say) does not touch the ledger and still finishes.
    let sched2 = "2026-10-09T15:00:00Z";
    run(&store, &account, sched2, ExecutionMode::Live, OutcomeKind::Completed, vec![]).unwrap();
    assert_eq!(db.scalar(&format!("SELECT status AS v FROM rebalancer_runs WHERE account_id = '{account}' AND scheduled_for = '{sched2}'")).unwrap(), "done");
}

#[test]
fn the_ledger_is_append_only_for_everyone_and_the_service_role_cannot_even_try() {
    let Some((db, tenants, account, _)) = setup("append_only", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    run(&store, &account, "2026-10-08T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 8, 31))]).unwrap();
    // Even the superuser: the trigger refuses.
    for stmt in ["UPDATE rebalancer_decision_ledger SET decision_date = '2030-01-01'", "DELETE FROM rebalancer_decision_ledger", "TRUNCATE rebalancer_decision_ledger"] {
        let e = db.try_exec(stmt).unwrap_err();
        assert!(e.contains("append-only"), "{stmt}: {e}");
    }
    // The restricted role has no UPDATE/DELETE/TRUNCATE privilege at all.
    for stmt in ["UPDATE rebalancer_decision_ledger SET decision_date = '2030-01-01'", "DELETE FROM rebalancer_decision_ledger", "TRUNCATE rebalancer_decision_ledger"] {
        let e = db.try_exec_as_svc(stmt).unwrap_err();
        assert!(e.contains("permission denied"), "{stmt}: {e}");
    }
    assert_eq!(ledger_dates(&db, &account), "2026-08-31");
    // The ledger's own monotone rule holds against a direct INSERT too (the database is the backstop).
    let e = db
        .try_exec(&format!(
            "INSERT INTO rebalancer_runs (tenant_id, account_id, scheduled_for, sleeve_set, status, trading_day, mode, started_at, lease_until, finished_at) \
             SELECT tenant_id, account_id, '2026-11-03T15:00:00Z', 'etf', 'done', '2026-11-03', 'live', now(), now(), now() FROM rebalancer_runs WHERE account_id = '{account}' LIMIT 1;
             INSERT INTO rebalancer_decision_ledger (tenant_id, account_id, sleeve_id, kind, decision_date, run_scheduled_for, run_sleeve_set, mode, acted_at) \
             SELECT tenant_id, account_id, 'etf', 'acted', '2026-07-31', '2026-11-03T15:00:00Z', 'etf', 'live', now() FROM rebalancer_runs WHERE account_id = '{account}' LIMIT 1;"
        ))
        .unwrap_err();
    assert!(e.contains("monotone"), "{e}");
}

#[test]
fn a_reset_reopens_the_sleeve_on_the_decision_in_force_and_keeps_the_history() {
    let Some((db, tenants, account, _)) = setup("reset", Opts::default()) else { return };
    let store = new_store(&db, &tenants);
    run(&store, &account, "2026-10-08T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 8, 31))]).unwrap();
    run(&store, &account, "2026-11-03T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![acted("etf", day(2026, 10, 30))]).unwrap();
    // After a halt-and-flatten and a human resume (Ruling 9b): D_acted is cleared, the sleeve is an entry again.
    store.reset_decision_ledger(&account, "etf", "owner", "resume after the kill drill").unwrap();
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), None);
    // ... and re-enters on the decision in force, an OLDER date than the history: allowed in the new epoch.
    run(&store, &account, "2026-11-04T15:00:00Z", ExecutionMode::Live, OutcomeKind::Completed, vec![decision("etf", day(2026, 10, 30), true, true, 2)]).unwrap();
    assert_eq!(store.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 10, 30)));
    assert_eq!(db.count(&format!("rebalancer_decision_ledger WHERE account_id = '{account}'")), 4, "2 acted + reset + re-entry: nothing was edited or removed");
    assert_eq!(db.scalar(&format!("SELECT epoch::text AS v FROM rebalancer_decision_ledger WHERE account_id = '{account}' ORDER BY seq DESC LIMIT 1")).unwrap(), "1");
    // A reset with nothing acted since the last one, or with a blank actor/reason, is refused.
    let other = Uuid::new_v4().to_string();
    tenants.register(&other, db.seed_tenant("Tenant C"));
    assert!(store.reset_decision_ledger(&other, "etf", "owner", "nothing to reset here").is_err());
    assert!(store.reset_decision_ledger(&account, "etf", " ", "a reason that is long enough").is_err());
    assert!(store.reset_decision_ledger(&account, "etf", "owner", "short").is_err());
    // A store with no tenant for the account cannot reset it.
    let stranger = Uuid::new_v4().to_string();
    assert!(matches!(store.reset_decision_ledger(&stranger, "etf", "owner", "a reason that is long enough"), Err(RunStoreError::DecisionLedgerUnavailable(_))));
}

#[test]
fn tickets_and_plan_provenance_are_persisted_on_the_run_row() {
    let Some((db, tenants, account, _)) = setup("tickets_provenance", Opts::default()) else { return };
    let registry = Arc::new(PlanProvenanceRegistry::new());
    let plan_id = Uuid::new_v4();
    registry.register(&account, PlanProvenance { origin: PlanOrigin::OwnerPilot, plan_id, venue_environment: VenueEnvironment::Paper });
    let store = new_store(&db, &tenants).with_plan_provenance(registry);
    let sched = "2026-10-08T15:00:00Z";
    let k = key(&account, sched);
    store.begin(&k, day(2026, 10, 8), ts(sched), 900).unwrap();
    let mut rec = record(RecordSpec { account: &account, scheduled_for: ts(sched), sleeve_set: "etf", mode: ExecutionMode::Assisted, outcome: OutcomeKind::Completed, decisions: vec![decision("etf", day(2026, 8, 31), true, true, 5)] });
    rec.tickets = vec![PlannedOrder {
        tag: "rb1:etf:SPY:1".into(),
        sleeve: "etf".into(),
        venue: "alpaca".into(),
        asset_class: "us_etf".into(),
        symbol: "SPY".into(),
        side: Side::Buy,
        quantity: Dec::parse("1.5").unwrap(),
        price: Dec::parse("660.25").unwrap(),
        notional: Dec::parse("990.375").unwrap(),
        est_fee: Dec::parse("0").unwrap(),
    }];
    store.finish(rec).unwrap();
    assert_eq!(db.scalar(&format!("SELECT (tickets->0->>'symbol' || '|' || (tickets->0->>'side') || '|' || (tickets->0->>'quantity') || '|' || jsonb_array_length(tickets)::text) AS v FROM rebalancer_runs WHERE account_id = '{account}'")).unwrap(), "SPY|buy|1.5|1");
    assert_eq!(db.scalar(&format!("SELECT (plan_origin || '|' || plan_id::text || '|' || venue_environment) AS v FROM rebalancer_runs WHERE account_id = '{account}'")).unwrap(), format!("owner_pilot|{plan_id}|paper"));
    // The database CHECK refuses a pilot run that claims a live environment, whatever wrote it.
    let e = db.try_exec(&format!("UPDATE rebalancer_runs SET venue_environment = 'live' WHERE account_id = '{account}'")).unwrap_err();
    assert!(e.contains("rebalancer_runs_provenance_check"), "{e}");
    // An account the registry does not know carries no provenance and an empty ticket list.
    let plain = Uuid::new_v4().to_string();
    tenants.register(&plain, db.seed_tenant("Tenant D"));
    run(&store, &plain, sched, ExecutionMode::Live, OutcomeKind::Completed, vec![]).unwrap();
    assert_eq!(db.scalar(&format!("SELECT (coalesce(plan_origin, 'none') || '|' || jsonb_array_length(tickets)::text) AS v FROM rebalancer_runs WHERE account_id = '{plain}'")).unwrap(), "none|0");
}

#[test]
fn concurrent_finishes_of_two_runs_leave_the_newest_decision_and_never_error() {
    let Some((db, tenants, account, _)) = setup("concurrent", Opts::default()) else { return };
    let store_a = Arc::new(new_store(&db, &tenants));
    let store_b = Arc::new(new_store(&db, &tenants));
    let (sa, sb) = ("2026-10-02T15:00:00Z", "2026-11-03T15:00:00Z");
    store_a.begin(&key(&account, sa), day(2026, 10, 2), ts(sa), 900).unwrap();
    store_a.begin(&key(&account, sb), day(2026, 11, 3), ts(sb), 900).unwrap();
    let rec = |sched: &str, d: NaiveDate, acct: &str| record(RecordSpec { account: acct, scheduled_for: ts(sched), sleeve_set: "etf", mode: ExecutionMode::Live, outcome: OutcomeKind::Completed, decisions: vec![acted("etf", d)] });
    let (r1, r2) = (rec(sa, day(2026, 9, 30), &account), rec(sb, day(2026, 10, 30), &account));
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let (b1, b2) = (barrier.clone(), barrier.clone());
    let (s1, s2) = (store_a.clone(), store_b.clone());
    let t1 = std::thread::spawn(move || {
        b1.wait();
        s1.finish(r1)
    });
    let t2 = std::thread::spawn(move || {
        b2.wait();
        s2.finish(r2)
    });
    t1.join().unwrap().expect("the older run's finish");
    t2.join().unwrap().expect("the newer run's finish");
    assert_eq!(store_a.last_acted_decision(&account, "etf").unwrap(), Some(day(2026, 10, 30)));
    let dates = ledger_dates(&db, &account);
    assert!(dates == "2026-10-30" || dates == "2026-09-30,2026-10-30", "monotone in whichever order they landed: {dates}");
}
