//! The two-source gate (W9.2, shadow mode): the pure comparison per R18 refusal condition, the R19 tolerance
//! boundaries (one basis point inside and outside each level), symmetry under a source swap, monotonicity in the
//! tolerance, sleeve isolation, and "shadow returns the primary unchanged". Every panel is synthetic.

mod common;

use std::sync::{Arc, Mutex};

use chrono::{Duration, NaiveDate};
use common::*;
use market_data::gate::{compare, Comparison, DataGateMode, Policy, Tolerance, TwoSourceGate, POLICY_VERSION};
use market_data::{FetchedSleeve, MassiveError, SleeveError, SleeveFetcher, SleevesFrom};
use rebalancer_run::data::{DataSource, GateVerdict, SleeveKind, SleeveSpec};
use rebalancer_run::decision::evaluate;
use reference_rules::{completed_month_end_dates, data_fingerprint, latest_decision_date, Panel, PriceSeries, ETF_SYMBOLS};

// ---------------------------------------------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------------------------------------------

fn panel_of(world: &World, tickers: &[&str], symbol_of: impl Fn(&str) -> String) -> Panel {
    let series = tickers
        .iter()
        .map(|t| {
            let bars = world.of(t);
            PriceSeries::new(symbol_of(t), bars.iter().map(|(d, _)| *d).collect(), bars.iter().map(|(_, c)| *c).collect()).unwrap()
        })
        .collect();
    Panel::new(series).unwrap()
}

fn etf_world() -> World {
    World::etf(d(2018, 6, 1), as_of() - Duration::days(1))
}

fn crypto_world() -> World {
    World::crypto(d(2019, 6, 1), as_of() - Duration::days(1))
}

fn etf_panel(w: &World) -> Panel {
    panel_of(w, &ETF_SYMBOLS, |t| t.to_string())
}

fn crypto_panel(w: &World) -> Panel {
    panel_of(w, &["X:BTCUSD", "X:ETHUSD"], |t| t.trim_start_matches("X:").trim_end_matches("USD").to_string())
}

fn etf_pair() -> (Panel, Panel) {
    let w = etf_world();
    (etf_panel(&w), etf_panel(&w))
}

fn crypto_pair() -> (Panel, Panel) {
    let w = crypto_world();
    (crypto_panel(&w), crypto_panel(&w))
}

/// Rebuild a panel with one close changed.
fn with_close(panel: &Panel, symbol: &str, date: NaiveDate, f: impl Fn(f64) -> f64) -> Panel {
    Panel::new(
        panel
            .iter()
            .map(|s| {
                let closes: Vec<f64> = s.dates().iter().zip(s.closes()).map(|(d, c)| if s.symbol() == symbol && *d == date { f(*c) } else { *c }).collect();
                PriceSeries::new(s.symbol().to_string(), s.dates().to_vec(), closes).unwrap()
            })
            .collect(),
    )
    .unwrap()
}

/// Rebuild a panel without the bars of `symbol` that `drop` selects.
fn without(panel: &Panel, symbol: &str, drop: impl Fn(NaiveDate) -> bool) -> Panel {
    Panel::new(
        panel
            .iter()
            .map(|s| {
                if s.symbol() != symbol {
                    return s.clone();
                }
                let kept: Vec<(NaiveDate, f64)> = s.dates().iter().zip(s.closes()).filter(|(d, _)| !drop(**d)).map(|(d, c)| (*d, *c)).collect();
                PriceSeries::new(s.symbol().to_string(), kept.iter().map(|x| x.0).collect(), kept.iter().map(|x| x.1).collect()).unwrap()
            })
            .collect(),
    )
    .unwrap()
}

/// Multiply every close of `symbol` from `date` on by `k` (a split reflected from that day).
fn split_from(panel: &Panel, symbol: &str, date: NaiveDate, k: f64) -> Panel {
    Panel::new(
        panel
            .iter()
            .map(|s| {
                let closes: Vec<f64> = s.dates().iter().zip(s.closes()).map(|(d, c)| if s.symbol() == symbol && *d >= date { *c * k } else { *c }).collect();
                PriceSeries::new(s.symbol().to_string(), s.dates().to_vec(), closes).unwrap()
            })
            .collect(),
    )
    .unwrap()
}

fn month_ends(panel: &Panel, symbol: &str) -> Vec<NaiveDate> {
    completed_month_end_dates(panel.get(symbol).unwrap())
}

fn codes(c: &Comparison) -> Vec<(String, Option<String>, Option<NaiveDate>)> {
    let mut v: Vec<_> = c.reasons.iter().map(|r| (r.code.clone(), r.symbol.clone(), r.date)).collect();
    v.sort();
    v.dedup();
    v
}

fn has(c: &Comparison, code: &str, symbol: &str, date: Option<NaiveDate>) -> bool {
    c.reasons.iter().any(|r| r.code == code && r.symbol.as_deref() == Some(symbol) && (date.is_none() || r.date == date))
}

const P: &Policy = &Policy::r19();

// ---------------------------------------------------------------------------------------------------------------
// Agreement
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn identical_panels_pass_for_both_kinds_and_record_every_decision_input() {
    let (p, s) = etf_pair();
    let c = compare(SleeveKind::EtfTrend, as_of(), &p, &s, P);
    assert_eq!(c.verdict, GateVerdict::Pass, "{:?}", c.reasons);
    assert!(c.reasons.is_empty());
    assert_eq!(c.primary_decision_date, Some(latest_decision_date(&p, &ETF_SYMBOLS).unwrap()));
    assert_eq!(c.primary_decision_date, c.secondary_decision_date);
    assert_eq!(c.instruments.len(), 5);
    for i in &c.instruments {
        assert_eq!(i.comparisons.len(), 10, "ten month-end closes per ETF: {:?}", i.comparisons.iter().map(|x| x.date).collect::<Vec<_>>());
        assert!(i.comparisons.iter().all(|x| x.diff_bps == Some(0.0) && x.primary_close.is_some() && x.secondary_close.is_some()));
        assert_eq!(i.max_diff_bps, Some(0.0));
    }

    let (p, s) = crypto_pair();
    let c = compare(SleeveKind::CryptoTrend, as_of(), &p, &s, P);
    assert_eq!(c.verdict, GateVerdict::Pass, "{:?}", c.reasons);
    assert_eq!(c.primary_decision_date, Some(as_of() - Duration::days(1)));
    for i in &c.instruments {
        assert_eq!(i.comparisons.len(), 100);
        assert_eq!(i.comparisons.iter().filter(|x| x.position == rebalancer_run::data::BarPosition::DecisionDay).count(), 1);
        assert_eq!(i.comparisons.last().unwrap().date, as_of() - Duration::days(1));
    }
}

// ---------------------------------------------------------------------------------------------------------------
// R19 tolerance boundaries, one basis point inside and outside each level
// ---------------------------------------------------------------------------------------------------------------

fn etf_at(bps: f64) -> Comparison {
    let (p, s) = etf_pair();
    let me = *month_ends(&p, "SPY").last().unwrap();
    let s = with_close(&s, "SPY", me, |c| c * (1.0 + bps / 10_000.0));
    compare(SleeveKind::EtfTrend, as_of(), &p, &s, P)
}

#[test]
fn etf_month_end_flag_and_refuse_boundaries() {
    assert_eq!(etf_at(24.0).verdict, GateVerdict::Pass);
    assert_eq!(etf_at(25.0).verdict, GateVerdict::Pass, "exactly the flag level is not beyond it");
    let c = etf_at(26.0);
    assert_eq!(c.verdict, GateVerdict::Flag);
    assert!(has(&c, "FLAG_L1_OVER_FLAG", "SPY", None));
    assert_eq!(etf_at(49.0).verdict, GateVerdict::Flag);
    assert_eq!(etf_at(50.0).verdict, GateVerdict::Flag, "exactly the refuse level is not beyond it");
    let c = etf_at(51.0);
    assert_eq!(c.verdict, GateVerdict::Refuse);
    assert!(has(&c, "REFUSE_L1_OVER_TOLERANCE", "SPY", None));
    // only SPY is affected; the other four instruments pass
    assert!(c.instruments.iter().filter(|i| i.symbol != "SPY").all(|i| i.verdict == GateVerdict::Pass));
}

#[test]
fn a_non_month_end_etf_close_is_not_a_decision_input_and_never_flags_by_tolerance() {
    let (p, s) = etf_pair();
    let me = *month_ends(&p, "SPY").last().unwrap();
    // the session two bars before the month-end is not an input
    let spy = p.get("SPY").unwrap();
    let idx = spy.position_of(me).unwrap();
    let other = spy.dates()[idx - 2];
    let s = with_close(&s, "SPY", other, |c| c * 1.03);
    let c = compare(SleeveKind::EtfTrend, as_of(), &p, &s, P);
    assert_eq!(c.verdict, GateVerdict::Pass, "{:?}", c.reasons);
}

fn crypto_at(days_back: i64, bps: f64) -> Comparison {
    let (p, s) = crypto_pair();
    let date = as_of() - Duration::days(days_back);
    let s = with_close(&s, "BTC", date, |c| c * (1.0 + bps / 10_000.0));
    compare(SleeveKind::CryptoTrend, as_of(), &p, &s, P)
}

#[test]
fn crypto_decision_day_and_window_boundaries() {
    // decision day (yesterday): flag 50, refuse 150
    assert_eq!(crypto_at(1, 49.0).verdict, GateVerdict::Pass);
    assert_eq!(crypto_at(1, 50.0).verdict, GateVerdict::Pass);
    assert_eq!(crypto_at(1, 51.0).verdict, GateVerdict::Flag);
    assert_eq!(crypto_at(1, 149.0).verdict, GateVerdict::Flag);
    assert_eq!(crypto_at(1, 150.0).verdict, GateVerdict::Flag);
    let c = crypto_at(1, 151.0);
    assert_eq!(c.verdict, GateVerdict::Refuse);
    assert!(has(&c, "REFUSE_L1_OVER_TOLERANCE", "BTC", Some(as_of() - Duration::days(1))));
    // any other window bar: flag 100, refuse 500
    assert_eq!(crypto_at(7, 99.0).verdict, GateVerdict::Pass);
    assert_eq!(crypto_at(7, 100.0).verdict, GateVerdict::Pass);
    assert_eq!(crypto_at(7, 101.0).verdict, GateVerdict::Flag);
    assert_eq!(crypto_at(7, 499.0).verdict, GateVerdict::Flag);
    assert_eq!(crypto_at(7, 500.0).verdict, GateVerdict::Flag);
    assert_eq!(crypto_at(7, 501.0).verdict, GateVerdict::Refuse);
    // the 100th-oldest bar is an input, the 101st is not
    assert_eq!(crypto_at(100, 501.0).verdict, GateVerdict::Refuse);
    assert_eq!(crypto_at(101, 501.0).verdict, GateVerdict::Pass);
}

// ---------------------------------------------------------------------------------------------------------------
// R18.3 refusal conditions
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_missing_decision_bar_on_either_source_refuses() {
    let (p, s) = etf_pair();
    let me = *month_ends(&p, "EFA").last().unwrap();
    let c = compare(SleeveKind::EtfTrend, as_of(), &p, &without(&s, "EFA", |x| x == me), P);
    assert_eq!(c.verdict, GateVerdict::Refuse);
    assert!(has(&c, "REFUSE_MISSING_BAR", "EFA", Some(me)), "{:?}", codes(&c));
    // ... and on the primary (the union of both sources' month-ends is compared)
    let c = compare(SleeveKind::EtfTrend, as_of(), &without(&p, "EFA", |x| x == me), &s, P);
    assert_eq!(c.verdict, GateVerdict::Refuse);
    assert!(has(&c, "REFUSE_MISSING_BAR", "EFA", Some(me)), "{:?}", codes(&c));

    let (p, s) = crypto_pair();
    let day = as_of() - Duration::days(40);
    let c = compare(SleeveKind::CryptoTrend, as_of(), &without(&p, "ETH", |x| x == day), &s, P);
    assert_eq!(c.verdict, GateVerdict::Refuse);
    assert!(has(&c, "REFUSE_MISSING_BAR", "ETH", Some(day)));
    assert!(c.instruments.iter().find(|i| i.symbol == "BTC").unwrap().verdict == GateVerdict::Pass);
}

#[test]
fn a_month_end_decision_date_mismatch_refuses() {
    let (p, s) = etf_pair();
    // the secondary has not published the first session of the newest month yet: its newest completed month-end is
    // one month older, so its decision date differs
    let dd = latest_decision_date(&p, &ETF_SYMBOLS).unwrap();
    let mut s2 = s.clone();
    for sym in ETF_SYMBOLS {
        s2 = without(&s2, sym, |x| x > dd);
    }
    let c = compare(SleeveKind::EtfTrend, as_of(), &p, &s2, P);
    assert_eq!(c.verdict, GateVerdict::Refuse);
    assert!(c.reasons.iter().any(|r| r.code == "REFUSE_DATE_MISMATCH"), "{:?}", codes(&c));
    assert_ne!(c.primary_decision_date, c.secondary_decision_date);
}

#[test]
fn a_split_reflected_on_one_source_only_refuses_and_on_both_does_not() {
    let (p, s) = etf_pair();
    let spy = p.get("SPY").unwrap();
    let ends = month_ends(&p, "SPY");
    // a 2:1 split between the 7th and 8th of the ten month-ends, on an ordinary session
    let at = spy.dates()[spy.position_of(ends[ends.len() - 3]).unwrap() + 5];
    let one_side = split_from(&s, "SPY", at, 0.5);
    let c = compare(SleeveKind::EtfTrend, as_of(), &p, &one_side, P);
    assert_eq!(c.verdict, GateVerdict::Refuse);
    assert!(has(&c, "REFUSE_SPLIT_ONE_SOURCE", "SPY", Some(at)), "{:?}", codes(&c));
    // the halved month-ends after the split also disagree beyond tolerance, as they should
    assert!(has(&c, "REFUSE_L1_OVER_TOLERANCE", "SPY", None));

    let both = split_from(&p, "SPY", at, 0.5);
    let c = compare(SleeveKind::EtfTrend, as_of(), &both, &one_side, P);
    assert_eq!(c.verdict, GateVerdict::Pass, "the same split on both sources is not a disagreement: {:?}", codes(&c));
}

#[test]
fn a_gap_beyond_the_core_guard_on_either_source_refuses() {
    let (p, s) = etf_pair();
    let ends = month_ends(&p, "IEF");
    let start = ends[ends.len() - 4] + Duration::days(3);
    // four consecutive weekdays missing (guard: more than 3)
    let hole = |x: NaiveDate| x > start && x <= start + Duration::days(7);
    let holed = without(&s, "IEF", hole);
    assert!(holed.get("IEF").unwrap().len() < s.get("IEF").unwrap().len());
    let c = compare(SleeveKind::EtfTrend, as_of(), &p, &holed, P);
    assert_eq!(c.verdict, GateVerdict::Refuse);
    assert!(has(&c, "REFUSE_GAP", "IEF", None), "{:?}", codes(&c));
    let c = compare(SleeveKind::EtfTrend, as_of(), &holed, &s, P);
    assert!(has(&c, "REFUSE_GAP", "IEF", None), "{:?}", codes(&c));
    // a two-day hole is inside the guard
    let small = without(&s, "IEF", |x| x > start && x <= start + Duration::days(2));
    let c = compare(SleeveKind::EtfTrend, as_of(), &p, &small, P);
    assert!(!has(&c, "REFUSE_GAP", "IEF", None), "{:?}", codes(&c));
}

#[test]
fn a_bar_dated_on_or_after_the_run_date_refuses() {
    let (p, s) = crypto_pair();
    let mut series: Vec<PriceSeries> = s.iter().cloned().collect();
    let btc = series.iter_mut().find(|x| x.symbol() == "BTC").unwrap();
    let mut dates = btc.dates().to_vec();
    let mut closes = btc.closes().to_vec();
    dates.push(as_of());
    closes.push(closes[closes.len() - 1]);
    *btc = PriceSeries::new("BTC", dates, closes).unwrap();
    let s2 = Panel::new(series).unwrap();
    let c = compare(SleeveKind::CryptoTrend, as_of(), &p, &s2, P);
    assert_eq!(c.verdict, GateVerdict::Refuse);
    assert!(has(&c, "REFUSE_BAR_AT_OR_AFTER_AS_OF", "BTC", Some(as_of())), "{:?}", codes(&c));
}

// ---------------------------------------------------------------------------------------------------------------
// Properties: symmetry under a source swap, monotonicity in the tolerance
// ---------------------------------------------------------------------------------------------------------------

fn scenarios() -> Vec<(SleeveKind, Panel, Panel)> {
    let (p, s) = etf_pair();
    let me = *month_ends(&p, "SPY").last().unwrap();
    let spy = p.get("SPY").unwrap();
    let ends = month_ends(&p, "SPY");
    let split_at = spy.dates()[spy.position_of(ends[ends.len() - 3]).unwrap() + 5];
    let dd = latest_decision_date(&p, &ETF_SYMBOLS).unwrap();
    let mut truncated = s.clone();
    for sym in ETF_SYMBOLS {
        truncated = without(&truncated, sym, |x| x > dd);
    }
    let gap_start = ends[ends.len() - 4] + Duration::days(3);
    let (cp, cs) = crypto_pair();
    vec![
        (SleeveKind::EtfTrend, p.clone(), s.clone()),
        (SleeveKind::EtfTrend, p.clone(), with_close(&s, "SPY", me, |c| c * 1.003)),
        (SleeveKind::EtfTrend, p.clone(), with_close(&s, "SPY", me, |c| c * 1.0075)),
        (SleeveKind::EtfTrend, p.clone(), without(&s, "EFA", |x| x == me)),
        (SleeveKind::EtfTrend, p.clone(), split_from(&s, "SPY", split_at, 0.5)),
        (SleeveKind::EtfTrend, p.clone(), truncated),
        (SleeveKind::EtfTrend, p.clone(), without(&s, "IEF", |x| x > gap_start && x <= gap_start + Duration::days(7))),
        (SleeveKind::CryptoTrend, cp.clone(), cs.clone()),
        (SleeveKind::CryptoTrend, cp.clone(), with_close(&cs, "BTC", as_of() - Duration::days(1), |c| c * 1.02)),
        (SleeveKind::CryptoTrend, cp.clone(), without(&cs, "ETH", |x| x == as_of() - Duration::days(40))),
    ]
}

#[test]
fn the_verdict_is_symmetric_under_a_source_swap() {
    let mut seen = std::collections::BTreeSet::new();
    for (kind, a, b) in scenarios() {
        let ab = compare(kind, as_of(), &a, &b, P);
        let ba = compare(kind, as_of(), &b, &a, P);
        assert_eq!(ab.verdict, ba.verdict, "{kind:?}: {:?} vs {:?}", codes(&ab), codes(&ba));
        assert_eq!(codes(&ab), codes(&ba), "{kind:?}");
        assert_eq!((ab.primary_decision_date, ab.secondary_decision_date), (ba.secondary_decision_date, ba.primary_decision_date));
        for (x, y) in ab.instruments.iter().zip(&ba.instruments) {
            assert_eq!((x.verdict, x.max_diff_bps), (y.verdict, y.max_diff_bps), "{kind:?} {}", x.symbol);
        }
        seen.insert(ab.verdict);
    }
    assert_eq!(seen.len(), 3, "the scenarios cover PASS, FLAG and REFUSE");
}

#[test]
fn the_verdict_is_monotone_in_the_tolerance() {
    let wider = Policy {
        etf_month_end: Tolerance { flag_bps: 100.0, refuse_bps: 1000.0 },
        crypto_decision_day: Tolerance { flag_bps: 500.0, refuse_bps: 5000.0 },
        crypto_window: Tolerance { flag_bps: 500.0, refuse_bps: 5000.0 },
        etf_max_missing_weekdays: 10,
        split_ratio_tolerance: 0.5,
    };
    for (kind, a, b) in scenarios() {
        let tight = compare(kind, as_of(), &a, &b, P).verdict;
        let loose = compare(kind, as_of(), &a, &b, &wider).verdict;
        assert!(loose <= tight, "{kind:?}: wider tolerances gave {loose:?} where R19 gave {tight:?}");
    }
    // a close exactly at a level, moved by the policy: the verdict follows the policy
    let (p, s) = etf_pair();
    let me = *month_ends(&p, "SPY").last().unwrap();
    let s = with_close(&s, "SPY", me, |c| c * 1.003);
    assert_eq!(compare(SleeveKind::EtfTrend, as_of(), &p, &s, P).verdict, GateVerdict::Flag);
    assert_eq!(compare(SleeveKind::EtfTrend, as_of(), &p, &s, &wider).verdict, GateVerdict::Pass);
}

#[test]
fn the_policy_is_versioned_and_hashed() {
    assert_eq!(POLICY_VERSION, "R19-2026-09-26");
    assert_eq!(Policy::r19().hash().len(), 64);
    assert_ne!(Policy::r19().hash(), Policy { etf_month_end: Tolerance { flag_bps: 26.0, refuse_bps: 50.0 }, ..Policy::r19() }.hash());
}

// ---------------------------------------------------------------------------------------------------------------
// The decorator: shadow returns the primary unchanged, records the verdict, isolates sleeves
// ---------------------------------------------------------------------------------------------------------------

/// A fetcher serving fixed panels per kind (or an error), counting calls.
struct Fixed {
    id: &'static str,
    etf: Mutex<Result<Panel, MassiveError>>,
    crypto: Mutex<Result<Panel, MassiveError>>,
    calls: Mutex<Vec<SleeveKind>>,
}

impl Fixed {
    fn new(id: &'static str, etf: Result<Panel, MassiveError>, crypto: Result<Panel, MassiveError>) -> Arc<Self> {
        Arc::new(Self { id, etf: Mutex::new(etf), crypto: Mutex::new(crypto), calls: Mutex::new(Vec::new()) })
    }
    fn calls(&self) -> Vec<SleeveKind> {
        self.calls.lock().unwrap().clone()
    }
}

impl SleeveFetcher for Fixed {
    fn source_id(&self) -> &'static str {
        self.id
    }
    fn fetch_sleeve(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<FetchedSleeve, SleeveError> {
        self.calls.lock().unwrap().push(sleeve.kind);
        let r = match sleeve.kind {
            SleeveKind::EtfTrend => self.etf.lock().unwrap().clone(),
            SleeveKind::CryptoTrend => self.crypto.lock().unwrap().clone(),
        };
        r.map(|panel| FetchedSleeve { panel, provenance: vec![], gate: None }).map_err(|error| SleeveError { sleeve: sleeve.kind, as_of, error })
    }
}

fn unavailable() -> MassiveError {
    MassiveError::Unavailable { detail: "vendor down".into(), attempts: 3 }
}

#[test]
fn shadow_mode_returns_the_primary_panel_unchanged_whatever_the_verdict_and_records_the_report() {
    let (p, s) = etf_pair();
    let me = *month_ends(&p, "SPY").last().unwrap();
    let bad = with_close(&s, "SPY", me, |c| c * 1.02); // 200 bp: REFUSE
    let (cp, cs) = crypto_pair();
    let primary = Fixed::new("massive", Ok(p.clone()), Ok(cp.clone()));
    let etf_secondary = Fixed::new("alpaca", Ok(bad), Err(unavailable()));
    let crypto_secondary = Fixed::new("kraken", Err(unavailable()), Ok(cs));
    let gate = TwoSourceGate::shadow(primary.clone())
        .with_secondary(SleeveKind::EtfTrend, etf_secondary.clone())
        .with_secondary(SleeveKind::CryptoTrend, crypto_secondary.clone());
    assert_eq!(gate.source_id(), "massive", "the gate presents as the primary");

    let f = gate.fetch_sleeve(&etf_sleeve(), as_of()).unwrap();
    assert_eq!(data_fingerprint(&f.panel), data_fingerprint(&p), "the PRIMARY panel, byte for byte");
    let r = f.gate.expect("a report");
    assert_eq!(r.verdict, GateVerdict::Refuse);
    assert_eq!((r.primary_source.as_str(), r.secondary_source.as_str()), ("massive", "alpaca"));
    assert_eq!(r.primary_fingerprint, data_fingerprint(&p));
    assert!(r.secondary_fingerprint.is_some());
    assert_eq!(r.policy_version, POLICY_VERSION);
    assert_eq!(r.policy_hash, Policy::r19().hash());
    assert_eq!(r.mode.as_str(), "shadow");
    assert!(r.reasons.iter().any(|x| x.code == "REFUSE_L1_OVER_TOLERANCE" && x.symbol.as_deref() == Some("SPY") && x.date == Some(me)));
    assert_eq!(r.refusals().len(), 1, "one refusal per (instrument, date): {:?}", r.refusals());
    assert!(r.summary().contains("REFUSE"), "{}", r.summary());

    // the same thing seen through the DataSource seam and the pipeline's evaluation: the decision is the primary's
    let source = SleevesFrom(&gate);
    let data = source.sleeve_data(&etf_sleeve(), as_of()).unwrap();
    assert_eq!(data_fingerprint(&data.panel), data_fingerprint(&p));
    assert_eq!(data.gate.as_ref().map(|g| g.verdict), Some(GateVerdict::Refuse));
    let ev = evaluate(&source, None, &etf_sleeve(), as_of()).unwrap();
    assert_eq!(ev.fingerprint, data_fingerprint(&p));
    assert_eq!(ev.data_gate.as_ref().map(|g| g.verdict), Some(GateVerdict::Refuse));
}

#[test]
fn a_secondary_failure_is_a_refusal_in_the_report_never_an_error_and_never_a_fallback_to_single_source() {
    let (p, _) = etf_pair();
    let (cp, cs) = crypto_pair();
    let primary = Fixed::new("massive", Ok(p.clone()), Ok(cp));
    let etf_secondary = Fixed::new("alpaca", Err(unavailable()), Err(unavailable()));
    let crypto_secondary = Fixed::new("kraken", Err(unavailable()), Ok(cs));
    let gate = TwoSourceGate::shadow(primary).with_secondary(SleeveKind::EtfTrend, etf_secondary).with_secondary(SleeveKind::CryptoTrend, crypto_secondary);

    let f = gate.fetch_sleeve(&etf_sleeve(), as_of()).unwrap();
    assert_eq!(data_fingerprint(&f.panel), data_fingerprint(&p));
    let r = f.gate.unwrap();
    assert_eq!(r.verdict, GateVerdict::Refuse);
    assert_eq!(r.reasons.len(), 1);
    assert_eq!(r.reasons[0].code, "REFUSE_SECONDARY_UNAVAILABLE");
    assert!(r.reasons[0].detail.contains("Transient"), "{}", r.reasons[0].detail);
    assert_eq!(r.secondary_fingerprint, None);

    // a kind with no secondary configured is a refusal too (never single-source)
    let gate2 = TwoSourceGate::shadow(Fixed::new("massive", Ok(p.clone()), Err(unavailable())));
    let r = gate2.fetch_sleeve(&etf_sleeve(), as_of()).unwrap().gate.unwrap();
    assert_eq!(r.reasons[0].code, "REFUSE_SECONDARY_NOT_CONFIGURED");
    assert_eq!(r.secondary_source, "none");
}

#[test]
fn an_etf_comparison_failure_does_not_touch_the_crypto_verdict_and_vice_versa() {
    let (p, s) = etf_pair();
    let me = *month_ends(&p, "SPY").last().unwrap();
    let (cp, cs) = crypto_pair();
    // ETF secondary disagrees; crypto secondary is fine
    let gate = TwoSourceGate::shadow(Fixed::new("massive", Ok(p.clone()), Ok(cp.clone())))
        .with_secondary(SleeveKind::EtfTrend, Fixed::new("alpaca", Ok(with_close(&s, "SPY", me, |c| c * 1.02)), Err(unavailable())))
        .with_secondary(SleeveKind::CryptoTrend, Fixed::new("kraken", Err(unavailable()), Ok(cs.clone())));
    assert_eq!(gate.fetch_sleeve(&etf_sleeve(), as_of()).unwrap().gate.unwrap().verdict, GateVerdict::Refuse);
    assert_eq!(gate.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap().gate.unwrap().verdict, GateVerdict::Pass);

    // crypto secondary down; ETF fine
    let kraken = Fixed::new("kraken", Err(unavailable()), Err(unavailable()));
    let alpaca = Fixed::new("alpaca", Ok(s.clone()), Err(unavailable()));
    let gate = TwoSourceGate::shadow(Fixed::new("massive", Ok(p.clone()), Ok(cp.clone())))
        .with_secondary(SleeveKind::EtfTrend, alpaca.clone())
        .with_secondary(SleeveKind::CryptoTrend, kraken.clone());
    assert_eq!(gate.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap().gate.unwrap().verdict, GateVerdict::Refuse);
    assert_eq!(gate.fetch_sleeve(&etf_sleeve(), as_of()).unwrap().gate.unwrap().verdict, GateVerdict::Pass);
    // each secondary was asked only for its own kind
    assert_eq!(alpaca.calls(), vec![SleeveKind::EtfTrend]);
    assert_eq!(kraken.calls(), vec![SleeveKind::CryptoTrend]);
}

#[test]
fn the_primary_failing_is_still_the_callers_failure_and_the_secondary_is_not_asked() {
    let (_, s) = etf_pair();
    let secondary = Fixed::new("alpaca", Ok(s), Err(unavailable()));
    let gate = TwoSourceGate::shadow(Fixed::new("massive", Err(MassiveError::StaleData { symbol: "SPY".into(), newest: None, as_of: as_of(), detail: "x".into() }), Err(unavailable())))
        .with_secondary(SleeveKind::EtfTrend, secondary.clone());
    let e = gate.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), market_data::ErrorKind::StaleData);
    assert!(secondary.calls().is_empty());
}

#[test]
fn the_mode_flag_rejects_enforce() {
    assert_eq!(DataGateMode::from_lookup(|_| None), Ok(DataGateMode::Off));
    assert_eq!(DataGateMode::from_lookup(|_| Some("shadow".into())), Ok(DataGateMode::Shadow));
    assert!(DataGateMode::from_lookup(|_| Some("enforce".into())).unwrap_err().contains("SHADOW"));
}
