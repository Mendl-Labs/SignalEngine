//! The `DataSource` contract, shared with the test doubles the pipeline's own tests use:
//! * `ClosedBarsOnly` below is the "assumed live provider" of `rebalancer-run/tests/etf_pending_decision.rs` (the
//!   `Vendor` there): at the run date only bars dated STRICTLY BEFORE it exist, bounded look-back, `DATA_UNAVAILABLE`
//!   when nothing is left;
//! * `FixtureData` (`rebalancer_run::testkit`) is the fixed-panel double.
//!
//! `MassiveDataSource`, fed a synthetic vendor whose data is that same world, must return the same panel for the same
//! `(kind, as_of)`, whatever the sleeve's id, venue or share, and the pipeline's `evaluate` must reach the same
//! decisions through either.

mod common;

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use common::*;
use market_data::{ETF_HISTORY_DAYS, CRYPTO_HISTORY_DAYS};
use rebalancer_core::guard::PricePoint;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveKind, SleeveSpec};
use rebalancer_run::decision::{evaluate, EvalCache, EvalError};
use rebalancer_run::testkit::FixtureData;
use reference_rules::{Panel, PriceSeries, CRYPTO_SYMBOLS, ETF_SYMBOLS};

/// Only complete bars exist at the run date: everything dated before it inside a look-back window.
struct ClosedBarsOnly {
    world: World,
}

impl DataSource for ClosedBarsOnly {
    fn sleeve_data(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        let (syms, window): (Vec<(String, String)>, i64) = match sleeve.kind {
            SleeveKind::EtfTrend => (ETF_SYMBOLS.iter().map(|s| (s.to_string(), s.to_string())).collect(), ETF_HISTORY_DAYS),
            SleeveKind::CryptoTrend => (CRYPTO_SYMBOLS.iter().map(|s| (s.to_string(), format!("X:{s}USD"))).collect(), CRYPTO_HISTORY_DAYS),
        };
        let cutoff = as_of.pred_opt().unwrap();
        let mut series = Vec::new();
        for (sym, key) in syms {
            let bars: Vec<_> = self.world.of(&key).iter().filter(|(x, _)| *x >= as_of - Duration::days(window) && *x <= cutoff).copied().collect();
            if bars.is_empty() {
                return Err(DataError::new("DATA_UNAVAILABLE", "no bars before the run date"));
            }
            series.push(PriceSeries::new(sym, bars.iter().map(|b| b.0).collect(), bars.iter().map(|b| b.1).collect()).unwrap());
        }
        Ok(SleeveData { panel: Panel::new(series).unwrap() })
    }

    fn prices(&self, _symbols: &[String], _now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError> {
        Ok(BTreeMap::new())
    }
}

/// What any `DataSource` must satisfy for a sleeve and run date. `sleeve_independent`: the panel may not depend on the
/// sleeve id, venue, asset class or share (true for a real source; `FixtureData` is keyed by sleeve id by design).
fn assert_contract(name: &str, src: &dyn DataSource, sleeve: &SleeveSpec, as_of: NaiveDate, symbols: &[&str], sleeve_independent: bool) {
    let a = src.sleeve_data(sleeve, as_of).unwrap_or_else(|e| panic!("{name}: {e}"));

    // 1. the expected instruments, validated series, no bar at or after the run date
    let mut want: Vec<&str> = symbols.to_vec();
    want.sort();
    assert_eq!(a.panel.symbols(), want, "{name}: instruments");
    for s in a.panel.iter() {
        assert!(s.last_date() < as_of, "{name}: {} has a bar dated {} >= the run date {as_of}", s.symbol(), s.last_date());
        assert!(s.closes().iter().all(|c| c.is_finite() && *c > 0.0));
        assert!(s.dates().windows(2).all(|w| w[0] < w[1]));
    }
    // 2. deterministic
    let b = src.sleeve_data(sleeve, as_of).unwrap();
    assert_eq!(a.panel, b.panel, "{name}: two calls, two panels");
    // 3. not varied by sleeve id / tenant-scoped fields
    if sleeve_independent {
        let mut other = sleeve.clone();
        other.id = "some-other-account-sleeve".into();
        other.venue = "another-venue".into();
        other.asset_class = "another-class".into();
        other.share = dec("0.05");
        assert_eq!(src.sleeve_data(&other, as_of).unwrap().panel, a.panel, "{name}: the panel must not vary by sleeve id, venue, asset class or share");
    }
    // 4. the pipeline's evaluation accepts it
    let ev = evaluate(src, None, sleeve, as_of).unwrap_or_else(|e| panic!("{name}: evaluate: {e:?}"));
    assert_eq!(ev.as_of, as_of);
}

fn massive_on(world: &World, as_of: NaiveDate) -> Harness {
    let h = Harness::new(as_of);
    h.serve(world.clone());
    h
}

#[test]
fn every_source_meets_the_contract_for_both_sleeve_kinds() {
    let world = standard_world();
    for as_of in [d(2020, 6, 17), d(2020, 6, 1), d(2020, 5, 26), d(2020, 7, 6), d(2020, 12, 15)] {
        let etf: Vec<&str> = ETF_SYMBOLS.to_vec();
        let crypto: Vec<&str> = CRYPTO_SYMBOLS.to_vec();

        let m = massive_on(&world, as_of);
        assert_contract("massive/etf", &m.src, &etf_sleeve(), as_of, &etf, true);
        assert_contract("massive/crypto", &m.src, &crypto_sleeve(), as_of, &crypto, true);

        let c = ClosedBarsOnly { world: world.clone() };
        assert_contract("closed-bars-only/etf", &c, &etf_sleeve(), as_of, &etf, true);
        assert_contract("closed-bars-only/crypto", &c, &crypto_sleeve(), as_of, &crypto, true);

        let f = FixtureData::new().with_panel("etf", c.sleeve_data(&etf_sleeve(), as_of).unwrap().panel).with_panel("crypto", c.sleeve_data(&crypto_sleeve(), as_of).unwrap().panel);
        assert_contract("fixture/etf", &f, &etf_sleeve(), as_of, &etf, false);
        assert_contract("fixture/crypto", &f, &crypto_sleeve(), as_of, &crypto, false);
    }
}

#[test]
fn massive_returns_the_same_panel_and_decision_as_the_closed_bars_only_provider_every_day() {
    let world = standard_world();
    let reference = ClosedBarsOnly { world: world.clone() };
    let h = Harness::new(d(2020, 3, 2));
    h.serve(world);
    let mut day = d(2020, 3, 2);
    let mut compared = 0;
    while day <= d(2020, 8, 31) {
        h.clock.set(at(day, 0, 10));
        for sleeve in [etf_sleeve(), crypto_sleeve()] {
            let mine = h.src.sleeve_data(&sleeve, day).unwrap_or_else(|e| panic!("{day} {:?}: {e}", sleeve.kind));
            let theirs = reference.sleeve_data(&sleeve, day).unwrap();
            assert_eq!(mine.panel, theirs.panel, "{day} {:?}: same panel", sleeve.kind);

            let (c1, c2) = (EvalCache::new(), EvalCache::new());
            let e1 = evaluate(&h.src, Some(&c1), &sleeve, day);
            let e2 = evaluate(&reference, Some(&c2), &sleeve, day);
            match (e1, e2) {
                (Ok(x), Ok(y)) => {
                    assert_eq!(x.fingerprint, y.fingerprint, "{day} {:?}", sleeve.kind);
                    assert_eq!(x.decision_date, y.decision_date);
                    assert_eq!(x.newest_bar_date, y.newest_bar_date);
                    assert_eq!(x.lag_sessions, y.lag_sessions);
                    let sig = |e: &rebalancer_run::decision::Evaluation| e.instruments.iter().map(|i| (i.symbol.clone(), i.weight)).collect::<Vec<_>>();
                    assert_eq!(sig(&x), sig(&y));
                }
                (Err(x), Err(y)) => assert_eq!(format!("{x:?}"), format!("{y:?}"), "{day}: same refusal"),
                (x, y) => panic!("{day} {:?}: one refused and one decided: {x:?} vs {y:?}", sleeve.kind),
            }
            compared += 1;
        }
        day += Duration::days(1);
    }
    assert!(compared > 300);
}

#[test]
fn accounts_share_one_fetch_per_tick_through_the_eval_cache() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let cache = EvalCache::new();
    let mut second = etf_sleeve();
    second.id = "etf-of-another-account".into(); // same kind, venue, asset class, quote: the cache key is the same
    let a = evaluate(&h.src, Some(&cache), &etf_sleeve(), as_of()).unwrap();
    assert_eq!(h.transport.request_count(), 5);
    let b = evaluate(&h.src, Some(&cache), &second, as_of()).unwrap();
    assert_eq!(h.transport.request_count(), 5, "the second account is served from the tick's memo: no new vendor request");
    assert_eq!(a.fingerprint, b.fingerprint);
    assert_eq!(cache.sizes(), (1, 1));

    // a different venue is a different cache key, hence a second fetch, and the source returns the same data
    let mut other_venue = etf_sleeve();
    other_venue.venue = "somewhere-else".into();
    let c = evaluate(&h.src, Some(&cache), &other_venue, as_of()).unwrap();
    assert_eq!(h.transport.request_count(), 10);
    assert_eq!(a.fingerprint, c.fingerprint, "the panel does not depend on the venue: the second fetch returns the same data");
}

#[test]
fn a_failed_fetch_is_remembered_for_the_tick_and_keeps_its_typed_code() {
    let mut world = standard_world();
    world.truncate_after("X:BTCUSD", d(2020, 6, 10));
    let h = Harness::new(as_of());
    h.serve(world);
    let cache = EvalCache::new();
    for _ in 0..3 {
        match evaluate(&h.src, Some(&cache), &crypto_sleeve(), as_of()) {
            Err(EvalError::Data(e)) => assert_eq!(e.code, "DATA_STALE"),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(h.transport.request_count(), 1, "one throttled/failed fetch is not repeated by every account (BTC fails before ETH is asked)");
}

#[test]
fn a_source_error_reaches_the_pipeline_as_a_data_error_with_a_stable_code() {
    let h = Harness::new(as_of());
    h.transport.enqueue_json(403, market_data::testing::NOT_AUTHORIZED_ERROR_BODY);
    match evaluate(&h.src, None, &etf_sleeve(), as_of()) {
        Err(EvalError::Data(e)) => {
            assert_eq!(e.code, "DATA_NOT_AUTHORIZED");
            assert!(e.message.contains("etf_trend"), "sleeve-scoped: {}", e.message);
        }
        other => panic!("{other:?}"),
    }
}
