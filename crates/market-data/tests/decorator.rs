//! The decorator seam. A future two-source gate wraps `SleeveFetcher`s; these tests prove that seam carries what a
//! gate needs: typed failure classes, sleeve-scoped errors, provenance, and a way back to a `DataSource`.

mod common;

use std::sync::Mutex;

use chrono::NaiveDate;
use common::*;
use market_data::{ErrorKind, FailureClass, FetchedSleeve, MassiveError, Provenance, SleeveError, SleeveFetcher, SleevesFrom, WithPrices};
use rebalancer_run::data::{DataSource, SleeveKind, SleeveSpec};
use rebalancer_run::decision::{evaluate, EvalError};
use rebalancer_run::testkit::FixtureData;

/// A stand-in for the gate: wraps any fetcher, records what passed through, and (like the real gate will) re-checks
/// the provenance before letting the panel through.
struct Recording<F: SleeveFetcher> {
    inner: F,
    seen: Mutex<Vec<Provenance>>,
    failures: Mutex<Vec<(SleeveKind, ErrorKind, FailureClass)>>,
}

impl<F: SleeveFetcher> Recording<F> {
    fn new(inner: F) -> Self {
        Self { inner, seen: Mutex::new(Vec::new()), failures: Mutex::new(Vec::new()) }
    }
}

impl<F: SleeveFetcher> SleeveFetcher for Recording<F> {
    fn source_id(&self) -> &'static str {
        self.inner.source_id()
    }

    fn fetch_sleeve(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<FetchedSleeve, SleeveError> {
        match self.inner.fetch_sleeve(sleeve, as_of) {
            Ok(f) => {
                self.seen.lock().unwrap().extend(f.provenance.iter().cloned());
                Ok(f)
            }
            Err(e) => {
                self.failures.lock().unwrap().push((e.sleeve, e.kind(), e.class()));
                Err(e)
            }
        }
    }
}

#[test]
fn a_decorator_sees_provenance_and_the_panel_flows_through_to_the_pipeline() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let gate = Recording::new(&h.src);
    // `&F` and `Arc<F>` are fetchers too, so a decorator can borrow or share the source.
    let source = SleevesFrom(gate);
    let ev = evaluate(&source, None, &etf_sleeve(), as_of()).expect("decision through the decorator");
    assert_eq!(ev.instruments.len(), 5);
    let seen = source.0.seen.lock().unwrap();
    assert_eq!(seen.len(), 5);
    assert!(seen.iter().all(|p| p.source_id == "massive" && !p.raw_sha256.is_empty() && p.as_of == as_of()));
    assert_eq!(source.0.source_id(), "massive");
}

#[test]
fn typed_failures_survive_the_decorator_and_map_to_stable_codes() {
    let mut world = standard_world();
    world.remove_date("EFA", d(2020, 4, 30));
    let h = Harness::new(as_of());
    h.serve(world);
    let gate = Recording::new(&h.src);

    // typed, at the fetcher level: the failure class and the sleeve scope are data, not text
    let err = gate.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(err.sleeve, SleeveKind::EtfTrend);
    assert_eq!(err.as_of, as_of());
    assert_eq!(err.kind(), ErrorKind::MissingBar);
    assert_eq!(err.class(), FailureClass::Deterministic);
    assert_eq!(gate.failures.lock().unwrap().as_slice(), &[(SleeveKind::EtfTrend, ErrorKind::MissingBar, FailureClass::Deterministic)]);

    // the crypto sleeve of the same source is unaffected: failures are sleeve-scoped
    assert!(gate.fetch_sleeve(&crypto_sleeve(), as_of()).is_ok());

    // through the pipeline's seam the same failure is a DataError with the stable code
    let source = SleevesFrom(gate);
    match evaluate(&source, None, &etf_sleeve(), as_of()) {
        Err(EvalError::Data(e)) => assert_eq!(e.code, "DATA_MISSING_BAR"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn every_error_kind_has_a_distinct_code_and_a_class() {
    let all = [
        MassiveError::Unavailable { detail: "x".into(), attempts: 1 },
        MassiveError::RateLimited { attempts: 1, local_budget: false },
        MassiveError::NotAuthorized { status: 403, detail: "x".into() },
        MassiveError::Malformed { instrument: "SPY".into(), detail: "x".into() },
        MassiveError::MissingBar { symbol: "SPY".into(), date: d(2020, 1, 2) },
        MassiveError::StaleData { symbol: "SPY".into(), newest: None, as_of: d(2020, 1, 2), detail: "x".into() },
        MassiveError::InsufficientHistory { symbol: "SPY".into(), needed: 10, have: 1 },
        MassiveError::Unsupported { detail: "x".into() },
    ];
    let mut codes: Vec<&str> = all.iter().map(|e| e.kind().code()).collect();
    codes.sort();
    codes.dedup();
    assert_eq!(codes.len(), all.len());
    let transient: Vec<_> = all.iter().filter(|e| e.class() == FailureClass::Transient).map(|e| e.kind()).collect();
    assert_eq!(transient, vec![ErrorKind::Unavailable, ErrorKind::RateLimited]);
    let settling: Vec<_> = all.iter().filter(|e| e.class() == FailureClass::Settling).map(|e| e.kind()).collect();
    assert_eq!(settling, vec![ErrorKind::StaleData]);
    for e in &all {
        let de: rebalancer_run::data::DataError = e.clone().into();
        assert_eq!(de.code, e.kind().code());
    }
}

#[test]
fn sleeves_from_massive_and_prices_from_elsewhere() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let prices = FixtureData::new().with_price_fn(|s| if s == "SPY" { Some(dec("321.5")) } else { None });
    let combined = WithPrices { sleeves: SleevesFrom(&h.src), prices };
    assert!(combined.sleeve_data(&etf_sleeve(), as_of()).is_ok());
    let p = combined.prices(&["SPY".to_string(), "QQQ".to_string()], at(as_of(), 0, 10)).unwrap();
    assert_eq!(p.len(), 1);
    assert_eq!(p["SPY"].price, dec("321.5"));

    // alone, the fetcher adapter refuses prices with a stable code rather than returning an empty map
    let alone = SleevesFrom(&h.src);
    assert_eq!(alone.prices(&["SPY".to_string()], at(as_of(), 0, 10)).unwrap_err().code, "DATA_UNSUPPORTED");
}
