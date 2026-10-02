//! T4 (paper-pilot plan, section 6, revised 2026-09-27 per section 8.5): the supervised READ-ONLY dry run.
//!
//! The platform computes a decision and logs it, but places nothing. This replaces the old "compare an assisted
//! ticket with `ticket.py`" step: nothing here is a human placing an order the platform decided, and no production
//! "dry-run mode" is added to the pipeline for it (there is no such thing to add: `rebalancer_run::decision::evaluate`
//! is ALREADY a pure, broker-independent function -- it takes a `DataSource` and a sleeve spec, never a `Broker`, and
//! is exactly the code path `run_once` itself uses to decide what a sleeve wants before ever touching the account).
//! This file is the minimal TEST-ONLY harness that exercises it directly and proves, against the REAL fake Alpaca
//! exchange, that doing so touches the exchange not at all.
//!
//! Coordination note (S-9's task instructions): the concurrent S-6 agent's PR
//! (`Mendl-Labs/SignalEngine#42`, branch `feat/pilot-account-runtime`) wires a real `AccountRuntime` into
//! `rebalancer-service` but does not add a dry-run mode of its own -- its `runtime.rs` module doc and test list
//! (`crates/rebalancer-service/tests/runtime_construction.rs`) cover connecting the real paper broker/data source,
//! not a decision-only path. There is nothing to duplicate; this file was built independently, in this crate's own
//! scope (`rebalancer-run/tests`), which is where `evaluate()` already lives.
//!
//! Pass condition (plan section 6, T4): the platform's decision equals the independent reference computation
//! (`ticket.py`'s own logic is `reference_rules::decide_etf_trend`, the same rule this test calls directly as the
//! oracle) in held set and notional within rounding; no order is placed; the account read is verified (`PA` prefix,
//! fingerprint).

mod common;

use std::collections::BTreeMap;

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::PaperOnlyAlpaca;
use chrono::{DateTime, NaiveDate, Utc};
use common::*;
use fake_broker::alpaca_rig::AlpacaRig;
use rebalancer_core::guard::PricePoint;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveKind, SleeveSpec};
use rebalancer_run::decision::evaluate;
use reference_rules::{decide_etf_trend, latest_decision_date, Options, Panel, PriceSeries, ETF_SYMBOLS};

const LADDER: &str = include_str!("../../reference-rules/tests/data/ladder_candles.csv");

fn etf_panel() -> Panel {
    let mut rows: BTreeMap<&str, (Vec<NaiveDate>, Vec<f64>)> = BTreeMap::new();
    for line in LADDER.lines().skip(1) {
        let f: Vec<&str> = line.trim().split(',').collect();
        if ETF_SYMBOLS.contains(&f[0]) {
            let e = rows.entry(f[0]).or_default();
            e.0.push(NaiveDate::parse_from_str(f[1], "%Y-%m-%d").unwrap());
            e.1.push(f[2].parse().unwrap());
        }
    }
    Panel::new(rows.into_iter().map(|(s, (dates, closes))| PriceSeries::new(s, dates, closes).unwrap()).collect()).unwrap()
}

fn etf_sleeve() -> SleeveSpec {
    SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: d("1"), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "USD".into() }
}

struct ReadOnlyVendor {
    panel: Panel,
}

impl DataSource for ReadOnlyVendor {
    fn sleeve_data(&self, _sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        let cutoff = as_of.pred_opt().unwrap();
        let series: Vec<PriceSeries> = self
            .panel
            .iter()
            .filter_map(|s| {
                let hi = s.dates().partition_point(|dd| *dd <= cutoff);
                (hi > 0).then(|| PriceSeries::new(s.symbol(), s.dates()[..hi].to_vec(), s.closes()[..hi].to_vec()).unwrap())
            })
            .collect();
        Ok(SleeveData::new(Panel::new(series).map_err(|e| DataError::new("DATA_UNAVAILABLE", &e.to_string()))?))
    }

    fn prices(&self, _symbols: &[String], _now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError> {
        // The decision-computation path never needs sizing prices; a dry run does not size or place anything.
        Ok(BTreeMap::new())
    }
}

fn as_of_after_the_fixture() -> NaiveDate {
    let panel = etf_panel();
    panel.iter().map(|s| *s.dates().last().unwrap()).max().unwrap() + chrono::Duration::days(1)
}

#[test]
fn t4_the_computed_decision_matches_the_reference_rule_exactly_and_nothing_is_placed() {
    let panel = etf_panel();
    let as_of = as_of_after_the_fixture();
    let data = ReadOnlyVendor { panel: panel.clone() };

    // The platform's own decision-computation path (exactly what `run_once` calls before ever touching a broker).
    let evaluation = evaluate(&data, None, &etf_sleeve(), as_of).expect("the fixture panel yields a decision");

    // The independent oracle: `ticket.py`'s own logic is this same reference rule, called directly (not through the
    // pipeline), on the same panel and the same `as_of`.
    let want = decide_etf_trend(&panel, evaluation.decision_date, &Options::etf_live(as_of)).expect("the oracle computes on the same inputs");

    fn held_set(v: &[reference_rules::InstrumentDecision]) -> Vec<(String, broker_adapters::Dec)> {
        let mut v: Vec<_> = v.iter().filter(|i| i.weight != 0.0).map(|i| (i.symbol.clone(), d(&format!("{:.8}", i.weight)))).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }
    let rebalancer_run::decision::RuleDecision::Etf(got_decision) = &evaluation.rule else { panic!("expected an ETF decision") };
    let (got_set, want_set) = (held_set(&got_decision.instruments), held_set(&want.instruments));
    assert_eq!(got_set, want_set, "the platform's held set and weights must equal ticket.py's reference computation exactly");
    assert!(!got_set.is_empty(), "fixture sanity: the comparison is not vacuous");
    assert_eq!(evaluation.decision_date, latest_decision_date(&panel, &ETF_SYMBOLS).unwrap());

    // Nothing was placed: the decision-computation path took a `DataSource` only, no `Broker` argument exists to
    // have called `place_order` on in the first place -- there is no code path here that COULD place an order.
    // (Rust's type system, not a runtime flag, is the proof: `evaluate`'s signature has no broker parameter.)
}

#[test]
fn t4_the_account_read_is_verified_pa_prefix_and_fingerprint_while_the_decision_computation_touches_the_exchange_not_at_all() {
    let rig = AlpacaRig::new();

    // Account identity verification (the OTHER half of T4's pass condition), entirely read-only: GET /v2/account,
    // the PA-prefix check, and the credential fingerprint -- through the SAME `PaperOnlyAlpaca` wrapper the real
    // pilot startup check uses, against the real fake exchange (never a POST).
    let paper = PaperOnlyAlpaca::new(&rig.key_id, &rig.secret, PAPER_BASE_URL, rig.transport.clone(), None, false).unwrap();
    let acct = paper.verify_paper_account().unwrap();
    assert!(acct.account_number.as_deref().is_some_and(|n| n.starts_with("PA")), "{acct:?}");
    assert_eq!(paper.key_id_fingerprint(), rig.adapter.key_id_fingerprint(), "the fingerprint is stable and never depends on which wrapper reads it");
    assert_eq!(paper.key_id_fingerprint().len(), 16, "8 bytes of SHA-256, hex-encoded");

    // Now the decision-computation path, against a FRESH read of the exchange's own request log: it adds nothing.
    let requests_before = rig.handle.requests().len();
    let panel = etf_panel();
    let as_of = as_of_after_the_fixture();
    let data = ReadOnlyVendor { panel };
    let _ = evaluate(&data, None, &etf_sleeve(), as_of).unwrap();
    assert_eq!(rig.handle.requests().len(), requests_before, "the decision computation never touched the exchange");
    assert!(rig.handle.orders().is_empty(), "nothing was ever placed");
}
