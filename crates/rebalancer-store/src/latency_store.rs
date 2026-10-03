//! `PgLatencyStore`: the Postgres-backed `rebalancer_run::latency::LatencyStore` (W9.1, the first-seen-latency /
//! revision recorder), against `rebalancer_bar_first_seen` and `rebalancer_bar_revisions`.
//!
//! The migration is `crates/rebalancer-store/migrations/2026-10-02-000000_create_rebalancer_bar_observations`
//! in THIS repository, written in databaseschema-internal's Diesel layout for the owner to copy there verbatim (see
//! its header for why it lives here). Until it is applied every call fails with `LATENCY_STORE_UNAVAILABLE` naming
//! that directory; the recorder is an observer, so that is logged and alerted once and no run is touched.
//!
//! Semantics mirror `InMemoryLatencyStore` (`rebalancer-run/src/latency.rs`) exactly:
//! * `known` returns the first sighting plus the LATEST values on file (the newest revision's `new_*`, else the
//!   first sighting's) and the revision count.
//! * `record_first_seen` is `INSERT ... ON CONFLICT (source, instrument, bar_date) DO NOTHING`: a concurrent writer
//!   of the same bar is not an error and the first write wins.
//! * `record_revision` appends; the foreign key refuses a revision of a bar never first seen.
//! * `evidence` is one latency per first sighting plus the revision count, per instrument, sorted by instrument.
//!
//! Raw SQL over `diesel::sql_query`, like every store in this crate (the public schema has no such tables; this
//! crate must compile without them).

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use diesel::sql_types::{Array, BigInt, Date, Double, Nullable, Text, Timestamptz};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use rebalancer_run::latency::{
    BarValues, FirstSeenRow, InstrumentEvidence, KnownBar, LatencyStore, RevisionRow,
};

use crate::pg::{Bridge, Pool};

pub const FIRST_SEEN_TABLE: &str = "rebalancer_bar_first_seen";
pub const REVISIONS_TABLE: &str = "rebalancer_bar_revisions";
pub const MIGRATION: &str = "2026-10-02-000000_create_rebalancer_bar_observations";

fn unavailable(e: impl std::fmt::Display) -> String {
    let text = e.to_string();
    if text.contains("does not exist") {
        format!("LATENCY_STORE_UNAVAILABLE: {text} (apply the rebalancer-store migration {MIGRATION} to databaseschema-internal; the recorder observes nothing until then)")
    } else {
        format!("LATENCY_STORE_UNAVAILABLE: {text}")
    }
}

#[derive(diesel::QueryableByName)]
struct KnownRow {
    #[diesel(sql_type = Timestamptz)]
    first_seen_at: DateTime<Utc>,
    #[diesel(sql_type = Timestamptz)]
    nominal_close_at: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Double>)]
    open: Option<f64>,
    #[diesel(sql_type = Nullable<Double>)]
    high: Option<f64>,
    #[diesel(sql_type = Nullable<Double>)]
    low: Option<f64>,
    #[diesel(sql_type = Double)]
    close: f64,
    #[diesel(sql_type = Nullable<Double>)]
    volume: Option<f64>,
    #[diesel(sql_type = Nullable<Double>)]
    new_open: Option<f64>,
    #[diesel(sql_type = Nullable<Double>)]
    new_high: Option<f64>,
    #[diesel(sql_type = Nullable<Double>)]
    new_low: Option<f64>,
    /// NULL exactly when no revision exists (new_close is NOT NULL on a revision row).
    #[diesel(sql_type = Nullable<Double>)]
    new_close: Option<f64>,
    #[diesel(sql_type = Nullable<Double>)]
    new_volume: Option<f64>,
    #[diesel(sql_type = BigInt)]
    revisions: i64,
}

#[derive(diesel::QueryableByName)]
struct LatencyRow {
    #[diesel(sql_type = Text)]
    instrument: String,
    #[diesel(sql_type = BigInt)]
    latency_secs: i64,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = Text)]
    instrument: String,
    #[diesel(sql_type = BigInt)]
    n: i64,
}

pub struct PgLatencyStore {
    bridge: Bridge,
}

impl PgLatencyStore {
    pub fn new(pool: Pool) -> Result<Self, String> {
        Ok(Self {
            bridge: Bridge::new(pool)?,
        })
    }
}

async fn known_query(
    conn: &mut AsyncPgConnection,
    source: &str,
    instrument: &str,
    bar_date: NaiveDate,
) -> Result<Option<KnownRow>, diesel::result::Error> {
    let rows: Vec<KnownRow> = diesel::sql_query(format!(
        "SELECT f.first_seen_at, f.nominal_close_at, f.open, f.high, f.low, f.close, f.volume, \
                r.new_open, r.new_high, r.new_low, r.new_close, r.new_volume, \
                (SELECT count(*) FROM {REVISIONS_TABLE} x WHERE x.source = f.source AND x.instrument = f.instrument AND x.bar_date = f.bar_date) AS revisions \
         FROM {FIRST_SEEN_TABLE} f \
         LEFT JOIN LATERAL ( \
             SELECT new_open, new_high, new_low, new_close, new_volume FROM {REVISIONS_TABLE} r \
             WHERE r.source = f.source AND r.instrument = f.instrument AND r.bar_date = f.bar_date \
             ORDER BY r.seq DESC LIMIT 1 \
         ) r ON TRUE \
         WHERE f.source = $1 AND f.instrument = $2 AND f.bar_date = $3"
    ))
    .bind::<Text, _>(source)
    .bind::<Text, _>(instrument)
    .bind::<Date, _>(bar_date)
    .get_results(conn)
    .await?;
    Ok(rows.into_iter().next())
}

impl LatencyStore for PgLatencyStore {
    fn known(
        &self,
        source: &str,
        instrument: &str,
        bar_date: NaiveDate,
    ) -> Result<Option<KnownBar>, String> {
        let (source, instrument) = (source.to_string(), instrument.to_string());
        self.bridge.block_on(move |mut conn| async move {
            let row = known_query(&mut conn, &source, &instrument, bar_date)
                .await
                .map_err(unavailable)?;
            Ok(row.map(|r| {
                let latest = match r.new_close {
                    Some(close) => BarValues {
                        open: r.new_open,
                        high: r.new_high,
                        low: r.new_low,
                        close,
                        volume: r.new_volume,
                    },
                    None => BarValues {
                        open: r.open,
                        high: r.high,
                        low: r.low,
                        close: r.close,
                        volume: r.volume,
                    },
                };
                KnownBar {
                    first_seen_at: r.first_seen_at,
                    nominal_close_at: r.nominal_close_at,
                    latest,
                    revisions: u32::try_from(r.revisions).unwrap_or(u32::MAX),
                }
            }))
        })
    }

    fn record_first_seen(&self, row: &FirstSeenRow) -> Result<(), String> {
        let row = row.clone();
        self.bridge.block_on(move |mut conn| async move {
            diesel::sql_query(format!(
                "INSERT INTO {FIRST_SEEN_TABLE} \
                 (source, instrument, bar_date, nominal_close_at, first_seen_at, latency_secs, open, high, low, close, volume, run_id, response_sha256, policy_version) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) \
                 ON CONFLICT (source, instrument, bar_date) DO NOTHING"
            ))
            .bind::<Text, _>(&row.source)
            .bind::<Text, _>(&row.instrument)
            .bind::<Date, _>(row.bar_date)
            .bind::<Timestamptz, _>(row.nominal_close_at)
            .bind::<Timestamptz, _>(row.first_seen_at)
            .bind::<BigInt, _>(row.latency_secs)
            .bind::<Nullable<Double>, _>(row.values.open)
            .bind::<Nullable<Double>, _>(row.values.high)
            .bind::<Nullable<Double>, _>(row.values.low)
            .bind::<Double, _>(row.values.close)
            .bind::<Nullable<Double>, _>(row.values.volume)
            .bind::<Text, _>(&row.run_id)
            .bind::<Nullable<Text>, _>(&row.response_sha256)
            .bind::<Text, _>(&row.policy_version)
            .execute(&mut conn)
            .await
            .map(|_| ())
            .map_err(unavailable)
        })
    }

    fn record_revision(&self, row: &RevisionRow) -> Result<(), String> {
        let row = row.clone();
        let changed: Vec<String> = row.changed.iter().map(|c| c.field.to_string()).collect();
        self.bridge.block_on(move |mut conn| async move {
            diesel::sql_query(format!(
                "INSERT INTO {REVISIONS_TABLE} \
                 (source, instrument, bar_date, seen_at, old_open, old_high, old_low, old_close, old_volume, \
                  new_open, new_high, new_low, new_close, new_volume, changed_fields, run_id, response_sha256, policy_version) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)"
            ))
            .bind::<Text, _>(&row.source)
            .bind::<Text, _>(&row.instrument)
            .bind::<Date, _>(row.bar_date)
            .bind::<Timestamptz, _>(row.seen_at)
            .bind::<Nullable<Double>, _>(row.old.open)
            .bind::<Nullable<Double>, _>(row.old.high)
            .bind::<Nullable<Double>, _>(row.old.low)
            .bind::<Double, _>(row.old.close)
            .bind::<Nullable<Double>, _>(row.old.volume)
            .bind::<Nullable<Double>, _>(row.new.open)
            .bind::<Nullable<Double>, _>(row.new.high)
            .bind::<Nullable<Double>, _>(row.new.low)
            .bind::<Double, _>(row.new.close)
            .bind::<Nullable<Double>, _>(row.new.volume)
            .bind::<Array<Text>, _>(&changed)
            .bind::<Text, _>(&row.run_id)
            .bind::<Nullable<Text>, _>(&row.response_sha256)
            .bind::<Text, _>(&row.policy_version)
            .execute(&mut conn)
            .await
            .map(|_| ())
            .map_err(unavailable)
        })
    }

    fn evidence(&self, source: &str) -> Result<Vec<InstrumentEvidence>, String> {
        let source = source.to_string();
        self.bridge.block_on(move |mut conn| async move {
            let latencies: Vec<LatencyRow> = diesel::sql_query(format!("SELECT instrument, latency_secs FROM {FIRST_SEEN_TABLE} WHERE source = $1 ORDER BY instrument, bar_date"))
                .bind::<Text, _>(&source)
                .get_results(&mut conn)
                .await
                .map_err(unavailable)?;
            let counts: Vec<CountRow> = diesel::sql_query(format!("SELECT instrument, count(*)::bigint AS n FROM {REVISIONS_TABLE} WHERE source = $1 GROUP BY instrument"))
                .bind::<Text, _>(&source)
                .get_results(&mut conn)
                .await
                .map_err(unavailable)?;
            let mut by_instrument: BTreeMap<String, InstrumentEvidence> = BTreeMap::new();
            for l in latencies {
                by_instrument
                    .entry(l.instrument.clone())
                    .or_insert_with(|| InstrumentEvidence { instrument: l.instrument.clone(), latencies_secs: Vec::new(), revisions: 0 })
                    .latencies_secs
                    .push(l.latency_secs);
            }
            for c in counts {
                if let Some(e) = by_instrument.get_mut(&c.instrument) {
                    e.revisions = u32::try_from(c.n).unwrap_or(u32::MAX);
                }
            }
            Ok(by_instrument.into_values().collect())
        })
    }
}
