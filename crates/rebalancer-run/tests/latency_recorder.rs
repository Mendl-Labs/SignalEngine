//! W9.1: the first-seen-latency / revision recorder, pure and in-memory. The policy functions (latency, sampling,
//! revision detection including the no-change case, percentiles, the 20-session threshold), the tick itself against
//! a scripted source and the in-memory store, the never-fails property, and the alert-once behaviour.

use std::collections::BTreeMap;
use std::sync::Mutex;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rebalancer_core::Dec;
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::latency::policy::{
    self, BARS_SAMPLED_PER_RUN, EVIDENCE_SESSIONS_REQUIRED, POLICY_VERSION,
};
use rebalancer_run::latency::{
    instruments_for_kind, instruments_for_sleeves, record_tick, summary, BarValues,
    InMemoryLatencyStore, Instrument, InstrumentEvidence, LatencyRecorder, LatencyStore,
    RecentBars, RecentBarsSource, VendorBar, RECORDER_ACCOUNT_ID,
};
use rebalancer_run::testkit::RecordingNotifier;

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

/// An ETF bar of `date` with its nominal close at 20:00Z (EDT convention of the summer dates used below).
fn etf_bar(date: NaiveDate, close: f64) -> VendorBar {
    VendorBar {
        bar_date: date,
        nominal_close_at: at(date, 20, 0),
        values: values(close),
    }
}

fn etf(symbol: &str) -> Instrument {
    Instrument {
        symbol: symbol.to_string(),
        kind: SleeveKind::EtfTrend,
        quote: String::new(),
    }
}

/// A scripted vendor: bars per instrument, editable between ticks; `fail` makes every fetch fail.
#[derive(Default)]
struct ScriptedSource {
    bars: Mutex<BTreeMap<String, Vec<VendorBar>>>,
    fail: Mutex<Option<String>>,
    calls: Mutex<usize>,
}

impl ScriptedSource {
    fn set(&self, symbol: &str, bars: Vec<VendorBar>) {
        self.bars.lock().unwrap().insert(symbol.to_string(), bars);
    }
    fn set_close(&self, symbol: &str, date: NaiveDate, close: f64) {
        for b in self.bars.lock().unwrap().get_mut(symbol).unwrap() {
            if b.bar_date == date {
                b.values.close = close;
            }
        }
    }
    fn set_volume(&self, symbol: &str, date: NaiveDate, volume: Option<f64>) {
        for b in self.bars.lock().unwrap().get_mut(symbol).unwrap() {
            if b.bar_date == date {
                b.values.volume = volume;
            }
        }
    }
    fn fail_with(&self, msg: Option<&str>) {
        *self.fail.lock().unwrap() = msg.map(str::to_string);
    }
    fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
    }
}

impl RecentBarsSource for ScriptedSource {
    fn source_id(&self) -> &'static str {
        "scripted"
    }
    fn recent_bars(
        &self,
        instrument: &Instrument,
        _now: DateTime<Utc>,
    ) -> Result<RecentBars, String> {
        *self.calls.lock().unwrap() += 1;
        if let Some(m) = self.fail.lock().unwrap().clone() {
            return Err(m);
        }
        let bars = self
            .bars
            .lock()
            .unwrap()
            .get(&instrument.symbol)
            .cloned()
            .ok_or_else(|| format!("no bars for {}", instrument.symbol))?;
        Ok(RecentBars {
            bars,
            response_sha256: Some("ab".repeat(32)),
        })
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Policy functions
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn policy_is_pinned() {
    assert_eq!(POLICY_VERSION, "latency-recorder/v1/2026-10-02");
    assert_eq!(BARS_SAMPLED_PER_RUN, 5);
    assert_eq!(EVIDENCE_SESSIONS_REQUIRED, 20);
    assert_eq!(policy::LOOKBACK_CALENDAR_DAYS, 10);
}

#[test]
fn latency_is_first_seen_minus_nominal_close_in_seconds() {
    let close = at(d(2026, 10, 1), 20, 0);
    assert_eq!(
        policy::latency_secs(close + Duration::minutes(17), close),
        17 * 60
    );
    assert_eq!(policy::latency_secs(close, close), 0);
    assert_eq!(
        policy::latency_secs(close - Duration::seconds(30), close),
        -30,
        "signed: a source handing over an early bar shows as negative"
    );
}

#[test]
fn only_bars_whose_nominal_close_has_passed_are_observable_and_the_newest_five_are_sampled() {
    let dates: Vec<NaiveDate> = (1..=8).map(|k| d(2026, 9, k)).collect();
    let bars: Vec<VendorBar> = dates.iter().map(|x| etf_bar(*x, 100.0)).collect();
    // 19:59Z on the 8th: the 8th's bar is forming (close at 20:00Z), the 7 others are observable; the newest 5 win.
    let now = at(d(2026, 9, 8), 19, 59);
    let sampled = policy::sample(&bars, now);
    assert_eq!(
        sampled.iter().map(|b| b.bar_date).collect::<Vec<_>>(),
        dates[2..7].to_vec()
    );
    // one minute later the 8th is observable and enters the sample; the oldest drops out
    let sampled = policy::sample(&bars, at(d(2026, 9, 8), 20, 0));
    assert_eq!(
        sampled.iter().map(|b| b.bar_date).collect::<Vec<_>>(),
        dates[3..8].to_vec()
    );
    assert!(
        policy::is_observable(&bars[7], at(d(2026, 9, 8), 20, 0)),
        "at exactly the nominal close"
    );
    assert!(!policy::is_observable(&bars[7], at(d(2026, 9, 8), 19, 59)));
    // fewer than five observable: all of them
    assert_eq!(policy::sample(&bars[..2], now).len(), 2);
    assert!(policy::sample(&[], now).is_empty());
}

#[test]
fn revision_detection_is_exact_and_reports_no_change_as_none() {
    let a = values(100.0);
    assert_eq!(
        policy::detect_revision(&a, &a),
        None,
        "identical values are not a revision"
    );
    let mut b = a;
    b.close = 100.0 + 1e-9;
    let changed = policy::detect_revision(&a, &b)
        .expect("a one-nanodollar difference is a revision: exact equality");
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].field, "close");
    assert_eq!(
        (changed[0].old, changed[0].new),
        (Some(100.0), Some(100.0 + 1e-9))
    );
    // volume: exact as well; present versus absent is a difference
    let mut c = a;
    c.volume = Some(1000.5);
    assert_eq!(policy::detect_revision(&a, &c).unwrap()[0].field, "volume");
    let mut e = a;
    e.volume = None;
    assert_eq!(policy::detect_revision(&a, &e).unwrap()[0].field, "volume");
    // several fields at once, in field order
    let mut f = a;
    f.open = Some(1.0);
    f.high = Some(2.0);
    f.close = 3.0;
    let fields: Vec<&str> = policy::detect_revision(&a, &f)
        .unwrap()
        .iter()
        .map(|c| c.field)
        .collect();
    assert_eq!(fields, ["open", "high", "close"]);
    // -0.0 == 0.0 under IEEE equality (a zero volume printed either way is the same value)
    let mut z1 = a;
    z1.volume = Some(0.0);
    let mut z2 = a;
    z2.volume = Some(-0.0);
    assert_eq!(policy::detect_revision(&z1, &z2), None);
}

#[test]
fn nearest_rank_percentiles() {
    assert_eq!(policy::percentile_nearest_rank(&[], 0.5), None);
    assert_eq!(policy::percentile_nearest_rank(&[7], 0.5), Some(7));
    assert_eq!(policy::percentile_nearest_rank(&[7], 0.9), Some(7));
    let s: Vec<i64> = (1..=10).collect();
    assert_eq!(policy::percentile_nearest_rank(&s, 0.5), Some(5));
    assert_eq!(policy::percentile_nearest_rank(&s, 0.9), Some(9));
    assert_eq!(policy::percentile_nearest_rank(&s, 1.0), Some(10));
    let unsorted = [30, 10, 20];
    assert_eq!(
        policy::percentile_nearest_rank(&unsorted, 0.5),
        Some(20),
        "the input need not be sorted"
    );
    assert_eq!(
        policy::percentile_nearest_rank(&s, 0.0),
        Some(1),
        "q is clamped above zero"
    );
    assert_eq!(
        policy::percentile_nearest_rank(&s, f64::NAN),
        Some(10),
        "a non-finite q is the maximum"
    );
}

#[test]
fn threshold_is_met_at_exactly_twenty_sessions_and_not_before() {
    let ev = |n: usize| InstrumentEvidence {
        instrument: "SPY".into(),
        latencies_secs: (0..n as i64).map(|k| 900 + k).collect(),
        revisions: 2,
    };
    let s = policy::summarize_instrument(&ev(19));
    assert!(!s.threshold_met);
    assert_eq!(s.sessions_observed, 19);
    assert_eq!(s.sessions_required, 20);
    let s = policy::summarize_instrument(&ev(20));
    assert!(s.threshold_met);
    assert_eq!(
        (s.p50_latency_secs, s.p90_latency_secs, s.max_latency_secs),
        (Some(909), Some(917), Some(919))
    );
    assert_eq!(s.revisions, 2);
    assert_eq!(s.policy_version, POLICY_VERSION);
    let s = policy::summarize_instrument(&ev(0));
    assert!(!s.threshold_met);
    assert_eq!(
        (s.p50_latency_secs, s.p90_latency_secs, s.max_latency_secs),
        (None, None, None)
    );
}

#[test]
fn instruments_come_from_the_reference_rules_symbol_lists_and_are_deduplicated() {
    let etfs = instruments_for_kind(SleeveKind::EtfTrend, "");
    assert_eq!(
        etfs.iter().map(|i| i.symbol.as_str()).collect::<Vec<_>>(),
        ["SPY", "EFA", "IEF", "DBC", "VNQ"]
    );
    let crypto = instruments_for_kind(SleeveKind::CryptoTrend, "USD");
    assert_eq!(
        crypto.iter().map(|i| i.symbol.as_str()).collect::<Vec<_>>(),
        ["BTC", "ETH"]
    );
    assert!(crypto.iter().all(|i| i.quote == "USD"));

    let sleeve = |id: &str, kind: SleeveKind| SleeveSpec {
        id: id.into(),
        kind,
        share: Dec::parse("0.5").unwrap(),
        venue: "v".into(),
        asset_class: "a".into(),
        quote: "USD".into(),
    };
    let sleeves = [
        sleeve("etf-a", SleeveKind::EtfTrend),
        sleeve("etf-b", SleeveKind::EtfTrend),
        sleeve("crypto", SleeveKind::CryptoTrend),
    ];
    let all = instruments_for_sleeves(sleeves.iter());
    assert_eq!(
        all.len(),
        7,
        "two ETF sleeves share the same five instruments; crypto adds two"
    );
    assert!(all.windows(2).all(|w| w[0] < w[1]), "sorted and unique");
}

// ---------------------------------------------------------------------------------------------------------------
// The tick against the in-memory store
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn first_sightings_are_written_once_with_latency_and_later_identical_sightings_write_nothing() {
    let src = ScriptedSource::default();
    let store = InMemoryLatencyStore::new();
    let dates: Vec<NaiveDate> = (1..=6).map(|k| d(2026, 9, k)).collect();
    src.set("SPY", dates.iter().map(|x| etf_bar(*x, 100.0)).collect());

    // Tick at 20:15Z on the 6th: all six bars closed, the newest five are sampled and first seen with 15 min latency
    // for the 6th, more for the older ones.
    let t1 = at(d(2026, 9, 6), 20, 15);
    let r = record_tick(&src, &store, &[etf("SPY")], t1);
    assert_eq!(r.errors(), Vec::<String>::new());
    assert_eq!(
        (r.first_seen(), r.revisions(), r.instruments[0].sampled),
        (5, 0, 5)
    );
    assert_eq!(r.run_id, "2026-09-06T20:15:00Z");
    assert_eq!(r.source, "scripted");

    let rows = store.first_seen_rows();
    assert_eq!(rows.len(), 5);
    let newest = rows.iter().find(|r| r.bar_date == d(2026, 9, 6)).unwrap();
    assert_eq!(newest.latency_secs, 15 * 60);
    assert_eq!(newest.first_seen_at, t1);
    assert_eq!(newest.nominal_close_at, at(d(2026, 9, 6), 20, 0));
    assert_eq!(newest.values, values(100.0));
    assert_eq!(newest.run_id, "2026-09-06T20:15:00Z");
    assert_eq!(newest.policy_version, POLICY_VERSION);
    assert_eq!(
        newest.response_sha256.as_deref(),
        Some("ab".repeat(32).as_str())
    );
    assert!(rows
        .iter()
        .all(|r| r.source == "scripted" && r.instrument == "SPY"));
    assert!(
        !rows.iter().any(|r| r.bar_date == d(2026, 9, 1)),
        "the sixth-newest bar is outside the sample"
    );

    // The next tick sees the same values: nothing is written, the first-sighting rows are unchanged.
    let t2 = t1 + Duration::minutes(5);
    let r = record_tick(&src, &store, &[etf("SPY")], t2);
    assert_eq!((r.first_seen(), r.revisions()), (0, 0));
    assert_eq!(
        store.first_seen_rows(),
        rows,
        "append-only: the first sighting is never touched"
    );
    assert!(store.revision_rows().is_empty());
}

#[test]
fn a_changed_value_appends_a_revision_with_old_and_new_and_a_second_change_compares_against_the_latest(
) {
    let src = ScriptedSource::default();
    let store = InMemoryLatencyStore::new();
    src.set(
        "SPY",
        vec![etf_bar(d(2026, 9, 5), 100.0), etf_bar(d(2026, 9, 6), 100.0)],
    );
    let t1 = at(d(2026, 9, 6), 20, 15);
    record_tick(&src, &store, &[etf("SPY")], t1);

    // The vendor restates the 6th's close.
    src.set_close("SPY", d(2026, 9, 6), 100.25);
    let t2 = t1 + Duration::minutes(5);
    let r = record_tick(&src, &store, &[etf("SPY")], t2);
    assert_eq!((r.first_seen(), r.revisions()), (0, 1));
    let revs = store.revision_rows();
    assert_eq!(revs.len(), 1);
    assert_eq!(revs[0].bar_date, d(2026, 9, 6));
    assert_eq!(revs[0].seen_at, t2);
    assert_eq!((revs[0].old.close, revs[0].new.close), (100.0, 100.25));
    assert_eq!(revs[0].changed.len(), 1);
    assert_eq!(revs[0].changed[0].field, "close");
    assert_eq!(revs[0].run_id, "2026-09-06T20:20:00Z");
    assert_eq!(
        store
            .first_seen_rows()
            .iter()
            .find(|r| r.bar_date == d(2026, 9, 6))
            .unwrap()
            .values
            .close,
        100.0,
        "the first sighting keeps the original value"
    );

    // Same restated value again: nothing (the comparison is against the LATEST values, not the first sighting).
    let r = record_tick(&src, &store, &[etf("SPY")], t2 + Duration::minutes(5));
    assert_eq!(r.revisions(), 0);
    assert_eq!(store.revision_rows().len(), 1);

    // A second restatement (volume only) is a second row whose `old` is the first revision's `new`.
    src.set_volume("SPY", d(2026, 9, 6), Some(1001.0));
    let r = record_tick(&src, &store, &[etf("SPY")], t2 + Duration::minutes(10));
    assert_eq!(r.revisions(), 1);
    let revs = store.revision_rows();
    assert_eq!(revs.len(), 2);
    assert_eq!(revs[1].old.close, 100.25);
    assert_eq!(
        (revs[1].old.volume, revs[1].new.volume),
        (Some(1000.0), Some(1001.0))
    );
    assert_eq!(revs[1].changed[0].field, "volume");

    let known = store
        .known("scripted", "SPY", d(2026, 9, 6))
        .unwrap()
        .unwrap();
    assert_eq!(known.revisions, 2);
    assert_eq!(known.latest.volume, Some(1001.0));
    assert_eq!(known.first_seen_at, t1);
}

#[test]
fn a_forming_bar_is_never_sampled_so_its_intraday_changes_are_not_revisions() {
    let src = ScriptedSource::default();
    let store = InMemoryLatencyStore::new();
    // crypto-style: the bar of the 6th closes at 00:00Z on the 7th
    let bar = |date: NaiveDate, close: f64| VendorBar {
        bar_date: date,
        nominal_close_at: at(date + Duration::days(1), 0, 0),
        values: values(close),
    };
    src.set(
        "BTC",
        vec![bar(d(2026, 9, 5), 60000.0), bar(d(2026, 9, 6), 60100.0)],
    );
    let btc = Instrument {
        symbol: "BTC".into(),
        kind: SleeveKind::CryptoTrend,
        quote: "USD".into(),
    };

    // 23:55Z on the 6th: the 6th is still forming
    let r = record_tick(&src, &store, std::slice::from_ref(&btc), at(d(2026, 9, 6), 23, 55));
    assert_eq!((r.instruments[0].sampled, r.first_seen()), (1, 1));
    assert!(store
        .first_seen_rows()
        .iter()
        .all(|r| r.bar_date == d(2026, 9, 5)));
    // it keeps changing while forming: still nothing about it on file
    src.set_close("BTC", d(2026, 9, 6), 60200.0);
    let r = record_tick(&src, &store, std::slice::from_ref(&btc), at(d(2026, 9, 6), 23, 59));
    assert_eq!((r.first_seen(), r.revisions()), (0, 0));
    // 00:05Z on the 7th: now it is observable, first seen with a 5-minute latency
    let r = record_tick(&src, &store, &[btc], at(d(2026, 9, 7), 0, 5));
    assert_eq!((r.first_seen(), r.revisions()), (1, 0));
    let row = store
        .first_seen_rows()
        .into_iter()
        .find(|r| r.bar_date == d(2026, 9, 6))
        .unwrap();
    assert_eq!(row.latency_secs, 5 * 60);
    assert_eq!(
        row.values.close, 60200.0,
        "the value at first sighting is what the vendor showed after the close"
    );
}

#[test]
fn a_fetch_failure_or_a_store_failure_is_reported_and_never_propagated_and_other_instruments_continue(
) {
    let src = ScriptedSource::default();
    let store = InMemoryLatencyStore::new();
    src.set("SPY", vec![etf_bar(d(2026, 9, 6), 100.0)]);
    src.set("EFA", vec![etf_bar(d(2026, 9, 6), 70.0)]);
    let now = at(d(2026, 9, 6), 20, 15);

    // an instrument the source has no bars for: its own error, the other instrument is unaffected
    let r = record_tick(&src, &store, &[etf("QQQ"), etf("SPY")], now);
    assert_eq!(
        r.instruments[0].error.as_deref(),
        Some("fetch: no bars for QQQ")
    );
    assert_eq!(r.instruments[1].error, None);
    assert_eq!(r.first_seen(), 1);
    assert_eq!(r.errors(), vec!["QQQ: fetch: no bars for QQQ".to_string()]);
    assert!(
        r.summary_line()
            .contains("1 error(s): QQQ: fetch: no bars for QQQ"),
        "{}",
        r.summary_line()
    );

    // the whole vendor down: every instrument reports, nothing panics
    src.fail_with(Some("HTTP 503"));
    let r = record_tick(
        &src,
        &store,
        &[etf("SPY"), etf("EFA")],
        now + Duration::minutes(5),
    );
    assert_eq!(r.errors().len(), 2);
    src.fail_with(None);

    // the store down: the error is per instrument and the tick still returns
    store.fail_next_calls(1);
    let r = record_tick(
        &src,
        &store,
        &[etf("EFA"), etf("SPY")],
        now + Duration::minutes(10),
    );
    assert!(
        r.instruments[0]
            .error
            .as_deref()
            .unwrap()
            .starts_with("store lookup 2026-09-06: injected failure"),
        "{:?}",
        r.instruments[0].error
    );
    assert_eq!(r.instruments[1].error, None);
    assert_eq!(
        store.first_seen_rows().len(),
        1,
        "EFA could not be looked up, so nothing about it was written on this tick"
    );
    let r = record_tick(&src, &store, &[etf("EFA")], now + Duration::minutes(15));
    assert_eq!((r.first_seen(), r.errors().len()), (1, 0));
    assert_eq!(
        store.first_seen_rows().len(),
        2,
        "EFA is first seen on the next tick, once the store is back"
    );
}

#[test]
fn the_recorder_alerts_once_per_distinct_failure_and_again_after_recovery() {
    let src = ScriptedSource::default();
    let store = InMemoryLatencyStore::new();
    let notifier = RecordingNotifier::new();
    src.set("SPY", vec![etf_bar(d(2026, 9, 6), 100.0)]);
    let rec = LatencyRecorder::new(&src, &store);
    let t0 = at(d(2026, 9, 6), 20, 15);

    rec.observe(&[etf("SPY")], t0, &notifier);
    assert_eq!(notifier.count(), 0, "a clean tick raises nothing");

    src.fail_with(Some("HTTP 503"));
    rec.observe(&[etf("SPY")], t0 + Duration::minutes(5), &notifier);
    rec.observe(&[etf("SPY")], t0 + Duration::minutes(10), &notifier);
    rec.observe(&[etf("SPY")], t0 + Duration::minutes(15), &notifier);
    assert_eq!(
        notifier.count(),
        1,
        "the same failure persisting is alerted once, not on every tick"
    );
    let a = &notifier.alerts()[0];
    assert_eq!(a.code.as_str(), "ALERT_LATENCY_RECORDER_FAILED");
    assert_eq!(a.severity.as_str(), "warning");
    assert_eq!(a.account_id, RECORDER_ACCOUNT_ID);
    assert_eq!(a.run_key, "2026-09-06T20:20:00Z");
    assert!(
        a.message.contains("observer only") && a.message.contains("HTTP 503"),
        "{}",
        a.message
    );
    assert_eq!(a.dedupe_key, "latency-recorder:failed");

    src.fail_with(Some("HTTP 429"));
    rec.observe(&[etf("SPY")], t0 + Duration::minutes(20), &notifier);
    assert_eq!(
        notifier.count(),
        2,
        "a different failure text is a new alert"
    );

    src.fail_with(None);
    let r = rec.observe(&[etf("SPY")], t0 + Duration::minutes(25), &notifier);
    assert!(r.errors().is_empty());
    assert_eq!(notifier.count(), 2);
    src.fail_with(Some("HTTP 429"));
    rec.observe(&[etf("SPY")], t0 + Duration::minutes(30), &notifier);
    assert_eq!(
        notifier.count(),
        3,
        "after a recovery the same text alerts again"
    );

    // the notifier itself failing does not change the report
    notifier.set_failing(true);
    src.fail_with(Some("HTTP 500"));
    let r = rec.observe(&[etf("SPY")], t0 + Duration::minutes(35), &notifier);
    assert_eq!(r.errors().len(), 1);
    assert_eq!(
        src.calls(),
        8,
        "one vendor call per observe call: the recorder never retries on its own"
    );
}

#[test]
fn the_read_side_reports_per_instrument_sessions_percentiles_revisions_and_the_threshold() {
    let src = ScriptedSource::default();
    let store = InMemoryLatencyStore::new();
    // 25 sessions of SPY, each first seen 15 minutes after its close, and 3 of EFA; one SPY revision.
    let spy_dates: Vec<NaiveDate> = (0..25).map(|k| d(2026, 8, 1) + Duration::days(k)).collect();
    for (i, date) in spy_dates.iter().enumerate() {
        src.set(
            "SPY",
            spy_dates[..=i].iter().map(|x| etf_bar(*x, 100.0)).collect(),
        );
        record_tick(&src, &store, &[etf("SPY")], at(*date, 20, 15));
    }
    src.set_close("SPY", spy_dates[24], 101.0);
    record_tick(&src, &store, &[etf("SPY")], at(spy_dates[24], 20, 20));
    let efa_dates: Vec<NaiveDate> = (0..3).map(|k| d(2026, 8, 1) + Duration::days(k)).collect();
    for (i, date) in efa_dates.iter().enumerate() {
        src.set(
            "EFA",
            efa_dates[..=i].iter().map(|x| etf_bar(*x, 70.0)).collect(),
        );
        record_tick(&src, &store, &[etf("EFA")], at(*date, 20, 30 + i as u32));
    }

    let s = summary(&store, "scripted").unwrap();
    assert_eq!(s.len(), 2);
    let efa = &s[0];
    assert_eq!(efa.instrument, "EFA");
    assert_eq!(efa.sessions_observed, 3);
    assert!(!efa.threshold_met);
    assert_eq!(
        (
            efa.p50_latency_secs,
            efa.p90_latency_secs,
            efa.max_latency_secs
        ),
        (Some(31 * 60), Some(32 * 60), Some(32 * 60))
    );
    assert_eq!(efa.revisions, 0);
    let spy = &s[1];
    assert_eq!(spy.instrument, "SPY");
    assert_eq!(spy.sessions_observed, 25);
    assert!(spy.threshold_met);
    assert_eq!(
        (
            spy.p50_latency_secs,
            spy.p90_latency_secs,
            spy.max_latency_secs
        ),
        (Some(15 * 60), Some(15 * 60), Some(15 * 60))
    );
    assert_eq!(spy.revisions, 1);
    assert!(summary(&store, "another-source").unwrap().is_empty());
}

#[test]
fn the_in_memory_store_is_idempotent_on_first_sightings_and_refuses_a_revision_without_one() {
    let store = InMemoryLatencyStore::new();
    let now = at(d(2026, 9, 6), 20, 15);
    let row = rebalancer_run::latency::FirstSeenRow {
        source: "s".into(),
        instrument: "SPY".into(),
        bar_date: d(2026, 9, 6),
        nominal_close_at: at(d(2026, 9, 6), 20, 0),
        first_seen_at: now,
        latency_secs: 900,
        values: values(100.0),
        run_id: "t".into(),
        response_sha256: None,
        policy_version: POLICY_VERSION.into(),
    };
    store.record_first_seen(&row).unwrap();
    let mut again = row.clone();
    again.first_seen_at = now + Duration::minutes(5);
    store.record_first_seen(&again).unwrap();
    assert_eq!(
        store.first_seen_rows(),
        vec![row.clone()],
        "the first write wins"
    );
    let rev = rebalancer_run::latency::RevisionRow {
        source: "s".into(),
        instrument: "EFA".into(),
        bar_date: d(2026, 9, 6),
        seen_at: now,
        old: values(1.0),
        new: values(2.0),
        changed: vec![],
        run_id: "t".into(),
        response_sha256: None,
        policy_version: POLICY_VERSION.into(),
    };
    assert!(store.record_revision(&rev).is_err());
    assert_eq!(store.known("s", "EFA", d(2026, 9, 6)).unwrap(), None);
}
