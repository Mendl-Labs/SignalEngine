//! Part 3(e) of WP4.8: the Postgres-backed stores round-trip correctly -- state survives a "restart"
//! (drop and recreate the in-process store objects, reload from the DB, confirm identical state) --
//! plus the per-account advisory lock's cross-"process" exclusion (two SEPARATE `PgAccountLock`
//! instances, each opening its own connection, exactly as two real OS processes would).
//!
//! Skips (does not fail) when `DATABASE_URL` is not set, matching
//! `program::deployment_lifecycle_scheduler`'s own test convention for the same reason: these need a
//! real Postgres instance, not a mock. See the worktree's own report for the exact throwaway-Postgres
//! commands this suite was run against.
//!
//! Every test starts by applying `databaseschema-internal/migrations/
//! 2026-09-22-010000_create_rebalancer_service_tables/up.sql` (idempotent-enough for a throwaway DB:
//! each test truncates its own tables first) and cleans its own rows at the end via `account_id`
//! prefixes unique to that test, so the tests may run in any order against the SAME throwaway
//! database.

use std::sync::Arc;

use chrono::{NaiveDate, Utc};
use rebalancer_risk::state::{AccountState, HaltReason};
use rebalancer_risk::store::StateStore;
use rebalancer_run::driver::AccountLock;
use rebalancer_run::record::{ExecutionMode, RunKey, RunRecord};
use rebalancer_run::stores::{Begin, JournalEntry, RunStore};
use rebalancer_store::{AccountTenants, PgAccountLock, PgKillFlag, PgNotifier, PgRunStore, PgStateStore};
use uuid::Uuid;

fn database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok()
}

fn skip_msg() {
    eprintln!("skipping: DATABASE_URL not set (see the worktree's report for the throwaway-Postgres setup commands)");
}

async fn raw_conn(url: &str) -> diesel_async::AsyncPgConnection {
    use diesel_async::AsyncConnection;
    diesel_async::AsyncPgConnection::establish(url).await.expect("connect for test setup")
}

/// Deletes this test's own rows (by an `account_id` prefix) so tests can share one throwaway database
/// without interfering with each other.
fn cleanup(url: &str, account_id_prefix: &str) {
    // Local, not `PgStateStore`/`PgRunStore`'s own `.load()` method -- kept scoped to this function so
    // it never shadows those trait methods' resolution anywhere else in the file (see the
    // `StateStore::load(&store, ...)` disambiguation elsewhere in this file for the same reason).
    use diesel_async::RunQueryDsl;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let prefix = format!("{account_id_prefix}%");
    rt.block_on(async {
        let mut conn = raw_conn(url).await;
        for table in ["rebalancer_run_journal", "rebalancer_runs", "rebalancer_equity_snapshots", "rebalancer_account_state", "rebalancer_alerts"] {
            diesel::sql_query(format!("DELETE FROM {table} WHERE account_id LIKE $1"))
                .bind::<diesel::sql_types::Text, _>(&prefix)
                .execute(&mut conn)
                .await
                .unwrap_or_else(|e| panic!("cleanup {table}: {e}"));
        }
    });
}

fn tenants() -> Arc<AccountTenants> {
    Arc::new(AccountTenants::new())
}

// ---------------------------------------------------------------------------------------------------
// StateStore round-trip
// ---------------------------------------------------------------------------------------------------

#[test]
fn state_store_survives_a_restart() {
    let Some(url) = database_url() else { return skip_msg() };
    let account_id = "rt-state-acct-1";
    cleanup(&url, account_id);
    let tn = tenants();
    tn.register(account_id, Uuid::new_v4());

    // "Process 1": save an Active state, then a halt (so halt/resumes JSON round-trip is exercised
    // too), then drop the store object entirely.
    {
        let pool = rebalancer_store::pg::create_pool(&url, 4).unwrap();
        let store = PgStateStore::new(pool, tn.clone()).unwrap();

        assert!(store.load(account_id).unwrap().is_none(), "a never-saved account must load as None");

        let fresh = AccountState::new(account_id);
        let saved = store.save(0, &fresh).unwrap();
        assert_eq!(saved.version(), 1);

        let (halted, _transition) = saved.halt(HaltReason::DailyLoss, "test halt for round-trip coverage", Utc::now());
        let saved2 = store.save(1, &halted).unwrap();
        assert_eq!(saved2.version(), 2);
        assert!(saved2.halt_record().is_some());
        // store dropped here (end of scope)
    }

    // "Process 2": a FRESH store object, fresh connection pool, same database. Must see exactly what
    // process 1 wrote.
    {
        let pool = rebalancer_store::pg::create_pool(&url, 4).unwrap();
        let store2 = PgStateStore::new(pool, tn.clone()).unwrap();
        let reloaded = store2.load(account_id).unwrap().expect("the halted state must still be there after the 'restart'");
        assert_eq!(reloaded.version(), 2);
        assert_eq!(reloaded.status().as_str(), "halted");
        let halt = reloaded.halt_record().expect("halt record survived the restart");
        assert_eq!(halt.reason, HaltReason::DailyLoss);
        assert_eq!(halt.detail, "test halt for round-trip coverage");

        // A version-conflict save is still correctly refused after the restart (the compare-and-swap
        // property itself survives, not just the data).
        let stale = AccountState::new(account_id); // version 0, but the DB is at version 2
        let err = store2.save(0, &stale).unwrap_err();
        assert!(matches!(err, rebalancer_risk::store::StoreError::VersionConflict { expected: 0, actual: 2 }), "{err:?}");
    }

    cleanup(&url, account_id);
}

// ---------------------------------------------------------------------------------------------------
// RunStore round-trip
// ---------------------------------------------------------------------------------------------------

fn sample_record(account_id: &str, scheduled_for: chrono::DateTime<Utc>) -> RunRecord {
    use broker_adapters::{Dec, Side};
    use rebalancer_run::record::{OutcomeKind, Phase, PlacedOrder, PlacedOutcome, RunOutcome, SnapshotSummary};
    use std::collections::BTreeMap;

    let snap = SnapshotSummary {
        taken_at: scheduled_for,
        equity: Dec::parse("10050.25").unwrap(),
        cash: Dec::parse("5000.00").unwrap(),
        derived_equity: Dec::parse("10050.25").unwrap(),
        holdings: BTreeMap::from([("BTC/USD".to_string(), Dec::parse("0.08").unwrap())]),
        marks: BTreeMap::from([("BTC/USD".to_string(), Dec::parse("60000").unwrap())]),
        open_order_ids: vec![],
    };
    let placed = PlacedOrder {
        phase: Phase::Buys,
        tag: "rb1:test-tag".to_string(),
        symbol: "BTC/USD".to_string(),
        side: Side::Buy,
        planned_quantity: Dec::parse("0.08").unwrap(),
        price: Dec::parse("60000").unwrap(),
        outcome: PlacedOutcome::Filled,
        broker_order_id: Some("BROKER-ORDER-1".to_string()),
        status: None,
        executed_quantity: Dec::parse("0.08").unwrap(),
        reports: vec![],
        anomalies: vec![],
        detail: String::new(),
    };
    RunRecord {
        key: RunKey { account_id: account_id.to_string(), scheduled_for, sleeve_set: "crypto".to_string() },
        mode: ExecutionMode::Live,
        attempt: 1,
        trading_day: scheduled_for.date_naive(),
        scheduled_for,
        started_at: scheduled_for,
        finished_at: scheduled_for + chrono::Duration::seconds(5),
        outcome: RunOutcome { kind: OutcomeKind::Completed, code: "RUN_COMPLETED".to_string(), message: "live run completed".to_string() },
        mandate_hash: "a".repeat(64),
        mandate_version: Some(3),
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
        targets: vec![],
        plan: None,
        replan: None,
        tickets: vec![],
        placed: vec![placed],
        cleanup: vec![],
        flatten: None,
        alerts: vec![],
        alert_delivery_failures: vec![],
        steps: vec![],
    }
}

#[test]
fn run_store_survives_a_restart_and_the_run_key_stays_exactly_once() {
    let Some(url) = database_url() else { return skip_msg() };
    let account_id = "rt-run-acct-1";
    cleanup(&url, account_id);
    let tn = tenants();
    tn.register(account_id, Uuid::new_v4());
    let scheduled_for = "2026-09-22T00:10:00Z".parse().unwrap();
    let key = RunKey { account_id: account_id.to_string(), scheduled_for, sleeve_set: "crypto".to_string() };
    let day = NaiveDate::from_ymd_opt(2026, 9, 22).unwrap();

    // "Process 1": begin, journal an intent, finish.
    {
        let pool = rebalancer_store::pg::create_pool(&url, 4).unwrap();
        let store = PgRunStore::new(pool, tn.clone()).unwrap();

        match store.begin(&key, day, scheduled_for, 900).unwrap() {
            Begin::Started { attempt } => assert_eq!(attempt, 1),
            other => panic!("expected Started, got {other:?}"),
        }
        store.journal_order(&key, JournalEntry { tag: "rb1:test-tag".to_string(), broker_order_id: None, notional: broker_adapters::Dec::parse("4800").unwrap() }).unwrap();
        let in_flight = store.in_flight(account_id).unwrap();
        assert_eq!(in_flight.len(), 1);
        assert_eq!(in_flight[0].tag, "rb1:test-tag");

        // A duplicate begin() call BEFORE finish (simulating a second racing attempt) must see Busy,
        // not start a second attempt.
        match store.begin(&key, day, scheduled_for, 900).unwrap() {
            Begin::Busy { .. } => {}
            other => panic!("expected Busy while the lease is live, got {other:?}"),
        }

        store.finish(sample_record(account_id, scheduled_for)).unwrap();

        // A second finish is rejected (records are immutable).
        let err = store.finish(sample_record(account_id, scheduled_for)).unwrap_err();
        assert!(matches!(err, rebalancer_run::stores::RunStoreError::AlreadyFinished(_)), "{err:?}");
    }

    // "Process 2": fresh store, same database. Restart survival + the run-key's own exactly-once.
    {
        let pool = rebalancer_store::pg::create_pool(&url, 4).unwrap();
        let store2 = PgRunStore::new(pool, tn.clone()).unwrap();

        // begin() on the SAME key after a "restart" must return AlreadyDone with the record process 1
        // wrote -- the exactly-once guarantee itself survives the restart.
        match store2.begin(&key, day, scheduled_for, 900).unwrap() {
            Begin::AlreadyDone(rec) => {
                assert_eq!(rec.outcome.kind, rebalancer_run::record::OutcomeKind::Completed);
                assert_eq!(rec.key, key);
            }
            other => panic!("expected AlreadyDone after a restart, got {other:?}"),
        }

        let fetched = store2.get(&key).unwrap().expect("the finished record must be readable after a restart");
        assert_eq!(fetched.mode, ExecutionMode::Live);
        assert_eq!(fetched.mandate_hash, "a".repeat(64));
        assert_eq!(fetched.mandate_version, Some(3));
        assert_eq!(fetched.placed.len(), 1);
        assert_eq!(fetched.placed[0].broker_order_id.as_deref(), Some("BROKER-ORDER-1"));
        assert_eq!(fetched.placed[0].executed_quantity, broker_adapters::Dec::parse("0.08").unwrap());
        assert!(fetched.post_snapshot.is_some());
        assert_eq!(fetched.post_snapshot.as_ref().unwrap().equity, broker_adapters::Dec::parse("10050.25").unwrap());

        let ids = store2.known_order_ids(account_id).unwrap();
        assert!(ids.contains("BROKER-ORDER-1"), "known_order_ids must survive the restart: {ids:?}");

        let counters = store2.day_counters(account_id, day).unwrap();
        assert_eq!(counters.orders_today, 1);

        let last = store2.last_snapshot(account_id).unwrap().expect("last_snapshot must survive the restart");
        assert_eq!(last.equity, broker_adapters::Dec::parse("10050.25").unwrap());

        // The in-flight journal entry is gone now that the run is done (in_flight only returns
        // entries of IN-PROGRESS runs -- see PgRunStore's own module doc).
        assert!(store2.in_flight(account_id).unwrap().is_empty());
    }

    cleanup(&url, account_id);
}

// ---------------------------------------------------------------------------------------------------
// PgAccountLock: cross-"process" exclusion
// ---------------------------------------------------------------------------------------------------

#[test]
fn account_lock_excludes_a_second_holder_and_frees_when_the_first_drops() {
    let Some(url) = database_url() else { return skip_msg() };
    let account_id = "rt-lock-acct-1";

    // Two SEPARATE PgAccountLock instances, each with its own connection -- exactly what two real
    // service processes would be.
    let lock_a = PgAccountLock::new(url.clone()).unwrap();
    let lock_b = PgAccountLock::new(url.clone()).unwrap();

    let held = lock_a.try_lock(account_id).unwrap();
    let first = held.expect("nobody else holds it yet, so the first attempt must succeed");

    let second = lock_b.try_lock(account_id).unwrap();
    assert!(second.is_none(), "a second holder must be refused while the first is alive");

    // A DIFFERENT account id is unaffected (the lock is per-account, not global).
    let other = lock_b.try_lock("rt-lock-acct-2").unwrap();
    assert!(other.is_some(), "locking a different account must not be blocked by acct-1's lock");
    drop(other);

    drop(first);
    // Postgres releases session locks when it notices the client gone; not instantaneous (mirrors
    // deployment_lifecycle_scheduler.rs's own equivalent test and its own comment on this).
    let mut reacquired = false;
    for _ in 0..50 {
        if lock_b.try_lock(account_id).unwrap().is_some() {
            reacquired = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(reacquired, "the lock must free once the first holder's connection is dropped");
}

// ---------------------------------------------------------------------------------------------------
// PgKillFlag and PgNotifier: smaller, but real, Postgres round-trips
// ---------------------------------------------------------------------------------------------------

#[test]
fn kill_flag_reads_the_singleton_row_and_survives_a_restart() {
    use rebalancer_run::stores::KillFlag;
    let Some(url) = database_url() else { return skip_msg() };

    let pool = rebalancer_store::pg::create_pool(&url, 2).unwrap();
    let flag = PgKillFlag::new(pool).unwrap();
    let before = flag.is_set().expect("the seeded singleton row must be readable");

    // Flip it directly (as an operator's `UPDATE` would, per AD8: no deploy needed to stop it), then
    // read again with a FRESH store object ("restart").
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        use diesel_async::{AsyncConnection, RunQueryDsl};
        let mut conn = diesel_async::AsyncPgConnection::establish(&url).await.unwrap();
        diesel::sql_query("UPDATE rebalancer_kill_flags SET is_set = NOT is_set WHERE id = TRUE").execute(&mut conn).await.unwrap();
    });

    let pool2 = rebalancer_store::pg::create_pool(&url, 2).unwrap();
    let flag2 = PgKillFlag::new(pool2).unwrap();
    let after = flag2.is_set().expect("readable after a restart");
    assert_eq!(after, !before, "the flip must be visible to a freshly constructed store");

    // Restore it so this test is idempotent across repeated runs against the same throwaway database.
    let rt2 = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt2.block_on(async {
        use diesel_async::{AsyncConnection, RunQueryDsl};
        let mut conn = diesel_async::AsyncPgConnection::establish(&url).await.unwrap();
        diesel::sql_query("UPDATE rebalancer_kill_flags SET is_set = $1 WHERE id = TRUE").bind::<diesel::sql_types::Bool, _>(before).execute(&mut conn).await.unwrap();
    });
}

#[test]
fn notifier_persists_an_alert_that_a_fresh_store_can_read_back() {
    use rebalancer_run::record::{Alert, AlertCode, AlertSeverity};
    use rebalancer_run::stores::Notifier;

    let Some(url) = database_url() else { return skip_msg() };
    let account_id = "rt-notify-acct-1";
    cleanup(&url, account_id);
    let tn = tenants();
    tn.register(account_id, Uuid::new_v4());

    {
        let pool = rebalancer_store::pg::create_pool(&url, 2).unwrap();
        let notifier = PgNotifier::new(pool, tn.clone()).unwrap();
        let alert = Alert {
            code: AlertCode::Halt,
            severity: AlertSeverity::Critical,
            account_id: account_id.to_string(),
            run_key: "test-run-key".to_string(),
            message: "round-trip test alert".to_string(),
            dedupe_key: "halt".to_string(),
            at: Utc::now(),
        };
        notifier.notify(&alert).expect("insert must succeed");
    }

    // Read back directly (this crate exposes no `list_alerts` -- the persistence contract is just
    // "notify() must not silently drop it"), with a FRESH connection ("restart").
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let count: i64 = rt.block_on(async {
        use diesel_async::{AsyncConnection, RunQueryDsl};
        #[derive(diesel::QueryableByName)]
        struct CountRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }
        let mut conn = diesel_async::AsyncPgConnection::establish(&url).await.unwrap();
        let rows: Vec<CountRow> = diesel::sql_query("SELECT COUNT(*) AS n FROM rebalancer_alerts WHERE account_id = $1 AND code = 'ALERT_HALT'")
            .bind::<diesel::sql_types::Text, _>(account_id)
            .get_results(&mut conn)
            .await
            .unwrap();
        rows.into_iter().next().map(|r| r.n).unwrap_or(0)
    });
    assert_eq!(count, 1, "the alert must be readable after a restart");

    cleanup(&url, account_id);
}
