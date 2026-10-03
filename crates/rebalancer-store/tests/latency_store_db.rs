//! `PgLatencyStore` against a scratch Postgres: env-gated exactly like `pilot_ledger_db.rs` (see `tests/common/mod.rs`
//! for `REBALANCER_PILOT_TEST_DB` / `REBALANCER_PILOT_TEST_MIGRATIONS`; every test prints `SKIPPED` and returns when
//! the gate is unset). On top of the real migrations each test applies this crate's own
//! `migrations/2026-10-02-000000_create_rebalancer_bar_observations/up.sql`, so the file the owner will copy into
//! databaseschema-internal is the file under test. Not run locally on the Windows development machine (no scratch
//! Postgres); CI is the gate when it provides one.

mod common;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use common::{create, Opts};
use rebalancer_run::latency::policy::POLICY_VERSION;
use rebalancer_run::latency::{
    summary, BarValues, FieldChange, FirstSeenRow, LatencyStore, RevisionRow,
};
use rebalancer_store::latency_store::{FIRST_SEEN_TABLE, MIGRATION, REVISIONS_TABLE};
use rebalancer_store::PgLatencyStore;

const UP_SQL: &str =
    include_str!("../migrations/2026-10-02-000000_create_rebalancer_bar_observations/up.sql");

fn d(y: i32, m: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, day).unwrap()
}

fn at(date: NaiveDate, h: u32, mi: u32) -> DateTime<Utc> {
    date.and_hms_opt(h, mi, 0).unwrap().and_utc()
}

fn values(close: f64) -> BarValues {
    BarValues {
        open: Some(close - 1.0),
        high: Some(close + 1.0),
        low: Some(close - 2.0),
        close,
        volume: Some(1000.0),
    }
}

fn first_seen(instrument: &str, date: NaiveDate, seen: DateTime<Utc>, close: f64) -> FirstSeenRow {
    let nominal_close_at = at(date, 20, 0);
    FirstSeenRow {
        source: "massive".into(),
        instrument: instrument.into(),
        bar_date: date,
        nominal_close_at,
        first_seen_at: seen,
        latency_secs: seen.signed_duration_since(nominal_close_at).num_seconds(),
        values: values(close),
        run_id: seen.to_rfc3339(),
        response_sha256: Some("0".repeat(64)),
        policy_version: POLICY_VERSION.into(),
    }
}

#[test]
fn first_sightings_revisions_known_and_evidence_round_trip_and_the_tables_are_append_only() {
    let Some(db) = create("latency_store_db", Opts::default()) else {
        return;
    };
    db.exec(UP_SQL);
    let store =
        PgLatencyStore::new(rebalancer_store::pg::create_pool(&db.url, 4).unwrap()).unwrap();
    let date = d(2026, 9, 25);
    let t1 = at(date, 20, 15);

    assert_eq!(store.known("massive", "SPY", date).unwrap(), None);

    let row = first_seen("SPY", date, t1, 100.0);
    store.record_first_seen(&row).unwrap();
    let k = store
        .known("massive", "SPY", date)
        .unwrap()
        .expect("on file");
    assert_eq!(
        (k.first_seen_at, k.nominal_close_at, k.latest, k.revisions),
        (t1, at(date, 20, 0), values(100.0), 0)
    );

    // idempotent: a second first sighting of the same key changes nothing
    let mut again = row.clone();
    again.first_seen_at = t1 + Duration::minutes(5);
    again.latency_secs += 300;
    again.values = values(999.0);
    store.record_first_seen(&again).unwrap();
    assert_eq!(
        store.known("massive", "SPY", date).unwrap().unwrap().latest,
        values(100.0)
    );
    assert_eq!(
        db.count(&format!("{FIRST_SEEN_TABLE} WHERE instrument = 'SPY'")),
        1
    );

    // a revision: known() reports the latest values and the count
    let t2 = t1 + Duration::minutes(5);
    let mut new = values(100.0);
    new.close = 100.25;
    store
        .record_revision(&RevisionRow {
            source: "massive".into(),
            instrument: "SPY".into(),
            bar_date: date,
            seen_at: t2,
            old: values(100.0),
            new,
            changed: vec![FieldChange {
                field: "close",
                old: Some(100.0),
                new: Some(100.25),
            }],
            run_id: t2.to_rfc3339(),
            response_sha256: None,
            policy_version: POLICY_VERSION.into(),
        })
        .unwrap();
    let k = store.known("massive", "SPY", date).unwrap().unwrap();
    assert_eq!(
        (k.latest.close, k.revisions, k.first_seen_at),
        (100.25, 1, t1)
    );
    // a second revision that drops the volume: the latest values carry the absence, not the first sighting's value
    let mut newer = new;
    newer.volume = None;
    store
        .record_revision(&RevisionRow {
            source: "massive".into(),
            instrument: "SPY".into(),
            bar_date: date,
            seen_at: t2 + Duration::minutes(5),
            old: new,
            new: newer,
            changed: vec![FieldChange {
                field: "volume",
                old: Some(1000.0),
                new: None,
            }],
            run_id: "t3".into(),
            response_sha256: None,
            policy_version: POLICY_VERSION.into(),
        })
        .unwrap();
    let k = store.known("massive", "SPY", date).unwrap().unwrap();
    assert_eq!(
        (k.latest.volume, k.latest.close, k.revisions),
        (None, 100.25, 2)
    );
    assert_eq!(db.scalar(&format!("SELECT array_to_string(changed_fields, ',') AS v FROM {REVISIONS_TABLE} ORDER BY seq DESC LIMIT 1")).as_deref(), Some("volume"));

    // a revision of a bar never first seen is refused by the foreign key
    let orphan = RevisionRow {
        source: "massive".into(),
        instrument: "EFA".into(),
        bar_date: date,
        seen_at: t2,
        old: values(1.0),
        new: values(2.0),
        changed: vec![FieldChange {
            field: "close",
            old: Some(1.0),
            new: Some(2.0),
        }],
        run_id: "t".into(),
        response_sha256: None,
        policy_version: POLICY_VERSION.into(),
    };
    assert!(store
        .record_revision(&orphan)
        .unwrap_err()
        .starts_with("LATENCY_STORE_UNAVAILABLE"));

    // evidence and the read side, over two instruments and two sources
    for k in 1..=3 {
        store
            .record_first_seen(&first_seen(
                "EFA",
                d(2026, 9, 20 + k),
                at(d(2026, 9, 20 + k), 20, 10 + k),
                70.0,
            ))
            .unwrap();
    }
    let mut other = first_seen("SPY", date, t1, 100.0);
    other.source = "other".into();
    store.record_first_seen(&other).unwrap();
    let ev = store.evidence("massive").unwrap();
    assert_eq!(ev.len(), 2);
    assert_eq!(
        (
            ev[0].instrument.as_str(),
            ev[0].latencies_secs.as_slice(),
            ev[0].revisions
        ),
        ("EFA", &[11 * 60, 12 * 60, 13 * 60][..], 0)
    );
    assert_eq!(
        (
            ev[1].instrument.as_str(),
            ev[1].latencies_secs.as_slice(),
            ev[1].revisions
        ),
        ("SPY", &[15 * 60][..], 2)
    );
    let s = summary(&store, "massive").unwrap();
    assert_eq!(
        (
            s[0].sessions_observed,
            s[0].p50_latency_secs,
            s[0].threshold_met
        ),
        (3, Some(12 * 60), false)
    );
    assert_eq!((s[1].sessions_observed, s[1].revisions), (1, 2));
    assert_eq!(store.evidence("other").unwrap().len(), 1);
    assert!(store.evidence("none").unwrap().is_empty());

    // append-only: the triggers refuse an update, a delete and a truncate on both tables
    for stmt in [
        format!("UPDATE {FIRST_SEEN_TABLE} SET close = 1 WHERE instrument = 'SPY'"),
        format!("DELETE FROM {REVISIONS_TABLE}"),
        format!("TRUNCATE {FIRST_SEEN_TABLE}"),
        format!("UPDATE {REVISIONS_TABLE} SET new_close = 1"),
    ] {
        assert!(db.try_exec(&stmt).is_err(), "must be refused: {stmt}");
    }
    // the stored latency must agree with the stored instants
    assert!(db
        .try_exec(&format!(
            "INSERT INTO {FIRST_SEEN_TABLE} (source, instrument, bar_date, nominal_close_at, first_seen_at, latency_secs, close, run_id, policy_version) \
             VALUES ('massive', 'VNQ', '2026-09-25', '2026-09-25T20:00:00Z', '2026-09-25T20:15:00Z', 1, 10, 't', 'v')"
        ))
        .is_err());
}

#[test]
fn without_the_migration_every_call_fails_closed_naming_it() {
    let Some(db) = create("latency_store_db_missing", Opts::default()) else {
        return;
    };
    let store =
        PgLatencyStore::new(rebalancer_store::pg::create_pool(&db.url, 2).unwrap()).unwrap();
    let e = store.known("massive", "SPY", d(2026, 9, 25)).unwrap_err();
    assert!(
        e.starts_with("LATENCY_STORE_UNAVAILABLE") && e.contains(MIGRATION),
        "{e}"
    );
    let e = store.evidence("massive").unwrap_err();
    assert!(e.contains(MIGRATION), "{e}");
    let e = store
        .record_first_seen(&first_seen(
            "SPY",
            d(2026, 9, 25),
            at(d(2026, 9, 25), 20, 15),
            100.0,
        ))
        .unwrap_err();
    assert!(e.contains(MIGRATION), "{e}");
}
