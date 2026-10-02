//! W9.1: the first-seen-latency / revision recorder (COUNCIL_DATA_GATE_2026_09_26.md R20, R25 precondition (6)
//! and work item G0, R26 item 4; COUNCIL_ETF_TIMING_AND_CADENCE.md Ruling 7 and Ruling 8(6);
//! SENIOR_RESEARCHER_GAP_CLOSURE_PLAN.md section 9.4 W9.1).
//!
//! Before any supervised paper cycle the councils want 20+ sessions of evidence about WHEN each vendor daily bar
//! first became visible relative to its nominal session close, and WHETHER a bar's values were revised after it was
//! first seen. Nobody has measured either (every parity report pulled settled bars days later). This module is the
//! recorder: on every driver tick it asks the vendor for the newest bars of every instrument the sleeves use, writes
//! down the first sighting of each `(instrument, bar date)` with its latency, and appends a revision row whenever a
//! bar already on file comes back with different values. It is an OBSERVER: nothing here feeds a decision, and a
//! failure here is reported (log + a Warning alert) and never fails or delays a run.
//!
//! # Pre-registered policy ([`policy`])
//! The constants and rules in [`policy`] were fixed on 2026-10-02, BEFORE any data was collected, and carry
//! [`policy::POLICY_VERSION`]; every row written records that version. Changing any of them is an amendment (a new
//! version string), never an edit of collected rows. In short:
//! * **What is sampled per tick:** the last [`policy::BARS_SAMPLED_PER_RUN`] bars the vendor shows whose NOMINAL
//!   CLOSE has already passed ([`policy::is_observable`]). A bar whose nominal close is still ahead (crypto's
//!   in-progress UTC day, an ETF session before 16:00 New York) is a FORMING bar: it is not sampled, so its continuous
//!   intraday changes never count as revisions and a first sighting can never precede the close.
//! * **Latency:** `first_seen_at - nominal_close_at` in whole seconds ([`policy::latency_secs`]), where
//!   `first_seen_at` is the clock of the tick that first saw the bar. The tick interval is therefore the resolution:
//!   the bar appeared somewhere in the interval ending at that tick. Nominal closes are the VENUE convention supplied by
//!   the source (`market-data`: 16:00 America/New_York of the session date for US ETFs, DST-aware, the vendor's
//!   15-minute delay NOT subtracted and early closes NOT modelled; 00:00 UTC of the next day for crypto).
//! * **Revision:** any of open, high, low, close, volume differing from the LATEST values on file (the first
//!   sighting, or the newest revision after it) by exact IEEE-754 equality; an absent field versus a present one is a
//!   difference too ([`policy::detect_revision`]). Every revision is a new row with the old and the new values; the
//!   first-sighting row is never updated.
//! * **Evidence threshold:** an instrument has enough evidence once [`policy::EVIDENCE_SESSIONS_REQUIRED`] distinct
//!   bar dates have a first sighting on file ([`policy::summarize_instrument`]); p50/p90/max use the nearest-rank
//!   percentile ([`policy::percentile_nearest_rank`]).
//!
//! # Shape
//! * [`RecentBarsSource`]: what a vendor source must provide (the newest bars as shown RIGHT NOW, forming bars
//!   included, each with its nominal close). `market-data` implements it for `MassiveDataSource`.
//! * [`LatencyStore`]: the append-only persistence seam. [`InMemoryLatencyStore`] is the in-process reference;
//!   `rebalancer-store::PgLatencyStore` is the Postgres implementation.
//! * [`record_tick`]: one observation pass over a list of instruments, pure given the two traits, returning a
//!   [`TickReport`] and never propagating an error.
//! * [`LatencyRecorder`]: `record_tick` plus the Warning alert on a NEW failure text (not on every tick a failure
//!   persists) through the pipeline's own [`Notifier`].
//! * [`summary`]: the read side the paper-cycle gate reads: per instrument, sessions observed, p50/p90/max latency,
//!   revision count and whether the threshold is met.

use std::collections::BTreeMap;
use std::sync::Mutex;

use chrono::{DateTime, NaiveDate, Utc};
use reference_rules::{CRYPTO_SYMBOLS, ETF_SYMBOLS};

use crate::data::{SleeveKind, SleeveSpec};
use crate::record::{Alert, AlertCode, AlertSeverity};
use crate::stores::Notifier;

// -------------------------------------------------------------------------------------------------------------
// The pre-registered policy, as data
// -------------------------------------------------------------------------------------------------------------

/// The policy of the recorder. FIXED 2026-10-02 BEFORE ANY DATA WAS COLLECTED (council R25: "pre-register the
/// policy"); see the module docs for the prose. A change to any item here is a new `POLICY_VERSION`.
pub mod policy {
    use chrono::{DateTime, Utc};

    use super::{BarValues, FieldChange, InstrumentEvidence, InstrumentLatencySummary, VendorBar};

    /// Recorded on every row. Bump on ANY change below.
    pub const POLICY_VERSION: &str = "latency-recorder/v1/2026-10-02";
    /// Of the bars the vendor shows, the newest this many whose nominal close has passed are sampled on each tick.
    pub const BARS_SAMPLED_PER_RUN: usize = 5;
    /// How far back the source asks the vendor for bars (calendar days), so that `BARS_SAMPLED_PER_RUN` sessions are
    /// covered across a weekend plus a holiday.
    pub const LOOKBACK_CALENDAR_DAYS: i64 = 10;
    /// Distinct bar dates with a first sighting on file before an instrument's evidence is "enough" (Ruling 8(6)).
    pub const EVIDENCE_SESSIONS_REQUIRED: usize = 20;
    /// The percentiles the read side reports.
    pub const P50: f64 = 0.50;
    pub const P90: f64 = 0.90;

    /// `first_seen_at - nominal_close_at`, whole seconds (truncated toward zero). Negative only if a source ever hands
    /// over a bar before its nominal close; [`is_observable`] prevents that for everything this module samples.
    pub fn latency_secs(first_seen_at: DateTime<Utc>, nominal_close_at: DateTime<Utc>) -> i64 {
        first_seen_at
            .signed_duration_since(nominal_close_at)
            .num_seconds()
    }

    /// A bar is observable (sampled) once its nominal close has passed on the tick's clock.
    pub fn is_observable(bar: &VendorBar, now: DateTime<Utc>) -> bool {
        bar.nominal_close_at <= now
    }

    /// The newest `BARS_SAMPLED_PER_RUN` observable bars of what the vendor showed, oldest first.
    pub fn sample(bars: &[VendorBar], now: DateTime<Utc>) -> Vec<&VendorBar> {
        let observable: Vec<&VendorBar> = bars.iter().filter(|b| is_observable(b, now)).collect();
        let skip = observable.len().saturating_sub(BARS_SAMPLED_PER_RUN);
        observable.into_iter().skip(skip).collect()
    }

    fn differs(a: Option<f64>, b: Option<f64>) -> bool {
        // Exact IEEE-754 equality on present values; present versus absent is a difference. (The sources never hand
        // over NaN: the vendor parser refuses a non-finite number.)
        a != b
    }

    /// `Some(changes)` when `seen` differs from `known` in any field (exact equality), `None` when nothing changed.
    pub fn detect_revision(known: &BarValues, seen: &BarValues) -> Option<Vec<FieldChange>> {
        let mut out = Vec::new();
        let mut check = |field: &'static str, old: Option<f64>, new: Option<f64>| {
            if differs(old, new) {
                out.push(FieldChange { field, old, new });
            }
        };
        check("open", known.open, seen.open);
        check("high", known.high, seen.high);
        check("low", known.low, seen.low);
        check("close", Some(known.close), Some(seen.close));
        check("volume", known.volume, seen.volume);
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    /// Nearest-rank percentile: the `ceil(q * n)`-th smallest value (1-based), `None` for an empty sample. `q` is
    /// clamped to `(0, 1]`.
    pub fn percentile_nearest_rank(latencies_secs: &[i64], q: f64) -> Option<i64> {
        if latencies_secs.is_empty() {
            return None;
        }
        let mut sorted = latencies_secs.to_vec();
        sorted.sort_unstable();
        let q = if q.is_finite() {
            q.clamp(f64::MIN_POSITIVE, 1.0)
        } else {
            1.0
        };
        let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
        Some(sorted[rank - 1])
    }

    /// The read model of one instrument from its raw evidence (one latency per distinct bar date first seen).
    pub fn summarize_instrument(evidence: &InstrumentEvidence) -> InstrumentLatencySummary {
        let l = &evidence.latencies_secs;
        InstrumentLatencySummary {
            instrument: evidence.instrument.clone(),
            sessions_observed: l.len(),
            p50_latency_secs: percentile_nearest_rank(l, P50),
            p90_latency_secs: percentile_nearest_rank(l, P90),
            max_latency_secs: l.iter().copied().max(),
            revisions: evidence.revisions,
            threshold_met: l.len() >= EVIDENCE_SESSIONS_REQUIRED,
            sessions_required: EVIDENCE_SESSIONS_REQUIRED,
            policy_version: POLICY_VERSION,
        }
    }
}

// -------------------------------------------------------------------------------------------------------------
// Types
// -------------------------------------------------------------------------------------------------------------

/// One instrument the recorder watches. `kind` tells the source which timestamp and nominal-close convention the
/// instrument follows; `quote` is the crypto quote currency (ignored for ETFs), as on [`SleeveSpec`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instrument {
    pub symbol: String,
    pub kind: SleeveKind,
    pub quote: String,
}

/// The values of one daily bar as the vendor showed them. `close` is mandatory (the parser refuses a bar without
/// one); the others are recorded when the vendor sends them.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BarValues {
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: f64,
    pub volume: Option<f64>,
}

/// One bar as a source showed it on a tick, with the venue's nominal close for its date.
#[derive(Debug, Clone, PartialEq)]
pub struct VendorBar {
    pub bar_date: NaiveDate,
    pub nominal_close_at: DateTime<Utc>,
    pub values: BarValues,
}

/// What a source hands back for one instrument on one tick.
#[derive(Debug, Clone, PartialEq)]
pub struct RecentBars {
    /// Oldest first; forming bars included (the recorder filters with [`policy::is_observable`]).
    pub bars: Vec<VendorBar>,
    /// SHA-256 (lowercase hex) of the raw vendor response, when the source has one (R26 item 6: replayability).
    pub response_sha256: Option<String>,
}

/// The vendor side of the recorder.
pub trait RecentBarsSource {
    /// Stable id of the source (`massive`, ...), recorded on every row.
    fn source_id(&self) -> &'static str;
    /// The newest daily bars the vendor shows RIGHT NOW for `instrument` (no completeness filter: the recorder must
    /// see a bar the moment the vendor does).
    fn recent_bars(
        &self,
        instrument: &Instrument,
        now: DateTime<Utc>,
    ) -> Result<RecentBars, String>;
}

/// A bar already on file: its first sighting and the newest values known (after any revisions).
#[derive(Debug, Clone, PartialEq)]
pub struct KnownBar {
    pub first_seen_at: DateTime<Utc>,
    pub nominal_close_at: DateTime<Utc>,
    pub latest: BarValues,
    pub revisions: u32,
}

/// The first sighting of `(source, instrument, bar_date)`. Written once, never updated.
#[derive(Debug, Clone, PartialEq)]
pub struct FirstSeenRow {
    pub source: String,
    pub instrument: String,
    pub bar_date: NaiveDate,
    pub nominal_close_at: DateTime<Utc>,
    pub first_seen_at: DateTime<Utc>,
    pub latency_secs: i64,
    pub values: BarValues,
    /// The driver tick that saw it (there is no per-tick run key; this is the tick's clock, RFC 3339).
    pub run_id: String,
    pub response_sha256: Option<String>,
    pub policy_version: String,
}

/// One field that changed between the values on file and the values seen.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FieldChange {
    pub field: &'static str,
    pub old: Option<f64>,
    pub new: Option<f64>,
}

/// A later sighting of a bar with different values. Appended, never merged into the first-sighting row.
#[derive(Debug, Clone, PartialEq)]
pub struct RevisionRow {
    pub source: String,
    pub instrument: String,
    pub bar_date: NaiveDate,
    pub seen_at: DateTime<Utc>,
    pub old: BarValues,
    pub new: BarValues,
    pub changed: Vec<FieldChange>,
    pub run_id: String,
    pub response_sha256: Option<String>,
    pub policy_version: String,
}

/// The raw evidence of one instrument the read side summarises: one latency per distinct bar date first seen, and
/// the number of revision rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentEvidence {
    pub instrument: String,
    pub latencies_secs: Vec<i64>,
    pub revisions: u32,
}

/// What the paper-cycle gate reads (R25 precondition (6)).
#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentLatencySummary {
    pub instrument: String,
    /// Distinct bar dates with a first sighting on file.
    pub sessions_observed: usize,
    pub p50_latency_secs: Option<i64>,
    pub p90_latency_secs: Option<i64>,
    pub max_latency_secs: Option<i64>,
    pub revisions: u32,
    pub threshold_met: bool,
    pub sessions_required: usize,
    pub policy_version: &'static str,
}

/// The persistence seam. Every method fails with `Err(String)`; the recorder reports and continues.
pub trait LatencyStore {
    fn known(
        &self,
        source: &str,
        instrument: &str,
        bar_date: NaiveDate,
    ) -> Result<Option<KnownBar>, String>;
    /// Idempotent on `(source, instrument, bar_date)`: a second insert of the same key (a concurrent writer) is not
    /// an error and changes nothing.
    fn record_first_seen(&self, row: &FirstSeenRow) -> Result<(), String>;
    fn record_revision(&self, row: &RevisionRow) -> Result<(), String>;
    /// Every instrument of `source` with a first sighting on file, sorted by instrument.
    fn evidence(&self, source: &str) -> Result<Vec<InstrumentEvidence>, String>;
}

// -------------------------------------------------------------------------------------------------------------
// Which instruments
// -------------------------------------------------------------------------------------------------------------

/// The instruments a sleeve kind's rule reads (the reference rules' own symbol lists).
pub fn instruments_for_kind(kind: SleeveKind, quote: &str) -> Vec<Instrument> {
    match kind {
        SleeveKind::EtfTrend => ETF_SYMBOLS
            .iter()
            .map(|s| Instrument {
                symbol: (*s).to_string(),
                kind,
                quote: String::new(),
            })
            .collect(),
        SleeveKind::CryptoTrend => CRYPTO_SYMBOLS
            .iter()
            .map(|s| Instrument {
                symbol: (*s).to_string(),
                kind,
                quote: quote.to_string(),
            })
            .collect(),
    }
}

/// Every instrument the given sleeves use, deduplicated and sorted.
pub fn instruments_for_sleeves<'a>(
    sleeves: impl IntoIterator<Item = &'a SleeveSpec>,
) -> Vec<Instrument> {
    let mut out: Vec<Instrument> = sleeves
        .into_iter()
        .flat_map(|s| instruments_for_kind(s.kind, &s.quote))
        .collect();
    out.sort();
    out.dedup();
    out
}

// -------------------------------------------------------------------------------------------------------------
// One tick
// -------------------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentReport {
    pub instrument: String,
    /// Observable bars sampled on this tick.
    pub sampled: usize,
    pub first_seen: usize,
    pub revisions: usize,
    /// The first error met for this instrument (fetch or store); later bars of the instrument were skipped.
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub run_id: String,
    pub source: String,
    pub instruments: Vec<InstrumentReport>,
}

impl TickReport {
    pub fn errors(&self) -> Vec<String> {
        self.instruments
            .iter()
            .filter_map(|i| i.error.as_ref().map(|e| format!("{}: {e}", i.instrument)))
            .collect()
    }

    pub fn first_seen(&self) -> usize {
        self.instruments.iter().map(|i| i.first_seen).sum()
    }

    pub fn revisions(&self) -> usize {
        self.instruments.iter().map(|i| i.revisions).sum()
    }

    /// One log line.
    pub fn summary_line(&self) -> String {
        let errors = self.errors();
        format!(
            "latency recorder {}: {} instrument(s), {} first sighting(s), {} revision(s), {} error(s){}",
            self.run_id,
            self.instruments.len(),
            self.first_seen(),
            self.revisions(),
            errors.len(),
            if errors.is_empty() { String::new() } else { format!(": {}", errors.join("; ")) }
        )
    }
}

/// The tick identifier every row of a tick carries.
pub fn run_id_for(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// One observation pass: for every instrument, fetch what the vendor shows, sample the observable bars, write first
/// sightings and revisions. Never panics on a trait error and never propagates one; everything is in the report.
pub fn record_tick(
    source: &dyn RecentBarsSource,
    store: &dyn LatencyStore,
    instruments: &[Instrument],
    now: DateTime<Utc>,
) -> TickReport {
    let run_id = run_id_for(now);
    let source_id = source.source_id().to_string();
    let mut reports = Vec::with_capacity(instruments.len());
    for inst in instruments {
        let mut rep = InstrumentReport {
            instrument: inst.symbol.clone(),
            sampled: 0,
            first_seen: 0,
            revisions: 0,
            error: None,
        };
        match source.recent_bars(inst, now) {
            Err(e) => rep.error = Some(format!("fetch: {e}")),
            Ok(recent) => {
                let sampled = policy::sample(&recent.bars, now);
                rep.sampled = sampled.len();
                for bar in sampled {
                    if let Err(e) = observe_bar(
                        store,
                        &source_id,
                        inst,
                        bar,
                        now,
                        &run_id,
                        recent.response_sha256.as_deref(),
                        &mut rep,
                    ) {
                        rep.error = Some(e);
                        break;
                    }
                }
            }
        }
        reports.push(rep);
    }
    TickReport {
        run_id,
        source: source_id,
        instruments: reports,
    }
}

#[allow(clippy::too_many_arguments)]
fn observe_bar(
    store: &dyn LatencyStore,
    source: &str,
    inst: &Instrument,
    bar: &VendorBar,
    now: DateTime<Utc>,
    run_id: &str,
    response_sha256: Option<&str>,
    rep: &mut InstrumentReport,
) -> Result<(), String> {
    let known = store
        .known(source, &inst.symbol, bar.bar_date)
        .map_err(|e| format!("store lookup {}: {e}", bar.bar_date))?;
    match known {
        None => {
            let row = FirstSeenRow {
                source: source.to_string(),
                instrument: inst.symbol.clone(),
                bar_date: bar.bar_date,
                nominal_close_at: bar.nominal_close_at,
                first_seen_at: now,
                latency_secs: policy::latency_secs(now, bar.nominal_close_at),
                values: bar.values,
                run_id: run_id.to_string(),
                response_sha256: response_sha256.map(str::to_string),
                policy_version: policy::POLICY_VERSION.to_string(),
            };
            store
                .record_first_seen(&row)
                .map_err(|e| format!("store first sighting {}: {e}", bar.bar_date))?;
            rep.first_seen += 1;
        }
        Some(k) => {
            if let Some(changed) = policy::detect_revision(&k.latest, &bar.values) {
                let row = RevisionRow {
                    source: source.to_string(),
                    instrument: inst.symbol.clone(),
                    bar_date: bar.bar_date,
                    seen_at: now,
                    old: k.latest,
                    new: bar.values,
                    changed,
                    run_id: run_id.to_string(),
                    response_sha256: response_sha256.map(str::to_string),
                    policy_version: policy::POLICY_VERSION.to_string(),
                };
                store
                    .record_revision(&row)
                    .map_err(|e| format!("store revision {}: {e}", bar.bar_date))?;
                rep.revisions += 1;
            }
        }
    }
    Ok(())
}

/// The read side: one summary per instrument with evidence on file for `source`.
pub fn summary(
    store: &dyn LatencyStore,
    source: &str,
) -> Result<Vec<InstrumentLatencySummary>, String> {
    Ok(store
        .evidence(source)?
        .iter()
        .map(policy::summarize_instrument)
        .collect())
}

// -------------------------------------------------------------------------------------------------------------
// The recorder with its alert
// -------------------------------------------------------------------------------------------------------------

/// The `account_id` the recorder's alerts carry: it is platform-wide, not an account's (a Postgres notifier writes
/// the unknown-tenant marker for it, by that crate's own convention).
pub const RECORDER_ACCOUNT_ID: &str = "latency-recorder";

/// [`record_tick`] plus one Warning alert per NEW failure text. A failure that persists tick after tick (a missing
/// table, a vendor outage) is logged by the caller every tick but alerted once; recovery clears the memory so the
/// next distinct failure alerts again.
pub struct LatencyRecorder<'a> {
    source: &'a dyn RecentBarsSource,
    store: &'a dyn LatencyStore,
    last_failure: Mutex<Option<String>>,
}

impl<'a> LatencyRecorder<'a> {
    pub fn new(source: &'a dyn RecentBarsSource, store: &'a dyn LatencyStore) -> Self {
        Self {
            source,
            store,
            last_failure: Mutex::new(None),
        }
    }

    /// Observe `instruments` at `now`; alert through `notifier` on a new failure. Returns the report either way.
    pub fn observe(
        &self,
        instruments: &[Instrument],
        now: DateTime<Utc>,
        notifier: &dyn Notifier,
    ) -> TickReport {
        let report = record_tick(self.source, self.store, instruments, now);
        let errors = report.errors();
        let mut last = self.last_failure.lock().unwrap_or_else(|e| e.into_inner());
        if errors.is_empty() {
            *last = None;
            return report;
        }
        let text = errors.join("; ");
        if last.as_deref() != Some(text.as_str()) {
            *last = Some(text.clone());
            let alert = Alert {
                code: AlertCode::LatencyRecorderFailed,
                severity: AlertSeverity::Warning,
                account_id: RECORDER_ACCOUNT_ID.to_string(),
                run_key: report.run_id.clone(),
                message: format!("the first-seen-latency recorder failed (observer only, no run affected): {text}"),
                dedupe_key: format!("{RECORDER_ACCOUNT_ID}:failed"),
                at: now,
            };
            // A notifier failure is not the recorder's to escalate: the caller logs the report's errors every tick.
            let _ = notifier.notify(&alert);
        }
        report
    }
}

// -------------------------------------------------------------------------------------------------------------
// In-memory store (tests, and the reference semantics the Postgres store mirrors)
// -------------------------------------------------------------------------------------------------------------

type Key = (String, String, NaiveDate);

#[derive(Default)]
struct Inner {
    first: BTreeMap<Key, FirstSeenRow>,
    revisions: BTreeMap<Key, Vec<RevisionRow>>,
    fail_next: u32,
}

/// Append-only in memory. `fail_next_calls` injects store failures for the recorder's own tests.
#[derive(Default)]
pub struct InMemoryLatencyStore {
    inner: Mutex<Inner>,
}

impl InMemoryLatencyStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn fail_next_calls(&self, n: u32) {
        self.lock().fail_next = n;
    }

    pub fn first_seen_rows(&self) -> Vec<FirstSeenRow> {
        self.lock().first.values().cloned().collect()
    }

    pub fn revision_rows(&self) -> Vec<RevisionRow> {
        self.lock().revisions.values().flatten().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn check(g: &mut Inner) -> Result<(), String> {
        if g.fail_next > 0 {
            g.fail_next -= 1;
            return Err("injected failure".to_string());
        }
        Ok(())
    }
}

impl LatencyStore for InMemoryLatencyStore {
    fn known(
        &self,
        source: &str,
        instrument: &str,
        bar_date: NaiveDate,
    ) -> Result<Option<KnownBar>, String> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let key: Key = (source.to_string(), instrument.to_string(), bar_date);
        let Some(first) = g.first.get(&key) else {
            return Ok(None);
        };
        let revs = g.revisions.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        let latest = revs.last().map(|r| r.new).unwrap_or(first.values);
        Ok(Some(KnownBar {
            first_seen_at: first.first_seen_at,
            nominal_close_at: first.nominal_close_at,
            latest,
            revisions: revs.len() as u32,
        }))
    }

    fn record_first_seen(&self, row: &FirstSeenRow) -> Result<(), String> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        g.first
            .entry((row.source.clone(), row.instrument.clone(), row.bar_date))
            .or_insert_with(|| row.clone());
        Ok(())
    }

    fn record_revision(&self, row: &RevisionRow) -> Result<(), String> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let key: Key = (row.source.clone(), row.instrument.clone(), row.bar_date);
        if !g.first.contains_key(&key) {
            return Err(format!(
                "no first sighting of {} {} {} to revise",
                row.source, row.instrument, row.bar_date
            ));
        }
        g.revisions.entry(key).or_default().push(row.clone());
        Ok(())
    }

    fn evidence(&self, source: &str) -> Result<Vec<InstrumentEvidence>, String> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let mut by_instrument: BTreeMap<String, InstrumentEvidence> = BTreeMap::new();
        for ((s, inst, _), row) in g.first.iter() {
            if s != source {
                continue;
            }
            by_instrument
                .entry(inst.clone())
                .or_insert_with(|| InstrumentEvidence {
                    instrument: inst.clone(),
                    latencies_secs: Vec::new(),
                    revisions: 0,
                })
                .latencies_secs
                .push(row.latency_secs);
        }
        for ((s, inst, _), rows) in g.revisions.iter() {
            if s != source {
                continue;
            }
            if let Some(e) = by_instrument.get_mut(inst) {
                e.revisions += rows.len() as u32;
            }
        }
        Ok(by_instrument.into_values().collect())
    }
}
