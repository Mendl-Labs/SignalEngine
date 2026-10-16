//! Certified-envelopes Phase 4: the market-data layer fetches and compares the sleeve's OWN instruments (its
//! implementation point), never the compile-time constants; at the published point (`instruments` empty) nothing
//! changes (the existing `happy.rs` / `gate.rs` suites pin that byte-for-byte). Every series is synthetic.

mod common;

use chrono::Duration;
use common::*;
use market_data::gate::{compare, compare_with, Policy};
use market_data::SleeveFetcher;
use rebalancer_run::data::{GateVerdict, SleeveKind, SleeveSpec};
use reference_rules::{Panel, PriceSeries, ETF_SYMBOLS};

const P: &Policy = &Policy::r19();

fn proxy_instruments() -> Vec<String> {
    ["IVV", "EFA", "IEF", "DBC", "VNQ"].iter().map(|s| s.to_string()).collect()
}

fn proxy_sleeve() -> SleeveSpec {
    SleeveSpec { instruments: proxy_instruments(), tranche_sessions: 3, lookback: Some(8), ..etf_sleeve() }
}

/// The standard world plus an `IVV` vendor ticker that prints exactly SPY's bars.
fn world_with_ivv() -> World {
    let mut w = standard_world();
    let spy = w.bars["SPY"].clone();
    w.bars.insert("IVV".to_string(), spy);
    w
}

#[test]
fn a_proxy_account_fetches_the_proxy_symbol_and_not_the_published_one() {
    let h = Harness::new(as_of());
    h.serve(world_with_ivv());
    let fetched = h.src.fetch_sleeve(&proxy_sleeve(), as_of()).expect("the proxy panel fetches");

    assert_eq!(fetched.panel.symbols(), vec!["DBC", "EFA", "IEF", "IVV", "VNQ"], "IVV, not SPY");
    assert!(fetched.panel.get("SPY").is_err());
    let reqs = h.transport.requests();
    assert_eq!(reqs.len(), 5, "one request per instrument of the point");
    assert!(reqs.iter().any(|r| r.url.contains("/ticker/IVV/")), "IVV is requested");
    assert!(reqs.iter().all(|r| !r.url.contains("/ticker/SPY/")), "SPY is never requested for a proxy account");
    assert_eq!(fetched.provenance.len(), 5);
    assert!(fetched.provenance.iter().any(|p| p.instrument == "IVV" && p.vendor_ticker == "IVV"));
}

#[test]
fn the_published_sleeve_still_fetches_the_published_five_in_the_published_order() {
    let h = Harness::new(as_of());
    h.serve(world_with_ivv());
    let fetched = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap();
    assert_eq!(fetched.provenance.iter().map(|p| p.instrument.clone()).collect::<Vec<_>>(), ETF_SYMBOLS.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    assert!(h.transport.requests().iter().all(|r| !r.url.contains("/ticker/IVV/")));
}

#[test]
fn a_proxy_that_the_vendor_does_not_serve_fails_the_fetch_instead_of_falling_back_to_the_published_symbol() {
    let h = Harness::new(as_of());
    h.serve(standard_world()); // no IVV bars
    assert!(h.src.fetch_sleeve(&proxy_sleeve(), as_of()).is_err());
}

fn panel_of(w: &World, tickers: &[&str]) -> Panel {
    Panel::new(
        tickers
            .iter()
            .map(|t| {
                let bars = w.of(t);
                PriceSeries::new(*t, bars.iter().map(|(d, _)| *d).collect(), bars.iter().map(|(_, c)| *c).collect()).unwrap()
            })
            .collect(),
    )
    .unwrap()
}

#[test]
fn the_gate_compares_the_points_own_instruments_and_window() {
    let w = {
        let mut w = World::etf(d(2018, 6, 1), as_of() - Duration::days(1));
        let spy = w.bars["SPY"].clone();
        w.bars.insert("IVV".to_string(), spy);
        w
    };
    let proxy = panel_of(&w, &["IVV", "EFA", "IEF", "DBC", "VNQ"]);
    let symbols = proxy_instruments();

    let c = compare_with(SleeveKind::EtfTrend, &symbols, 8, as_of(), &proxy, &proxy, P);
    assert_eq!(c.verdict, GateVerdict::Pass, "{:?}", c.reasons);
    assert_eq!(c.instruments.iter().map(|i| i.symbol.as_str()).collect::<Vec<_>>(), vec!["IVV", "EFA", "IEF", "DBC", "VNQ"]);
    assert!(c.instruments.iter().all(|i| i.comparisons.len() == 8), "an 8-month-end lookback compares 8 closes per instrument");

    // The published-point comparison over the same panels cannot see SPY: it refuses, which is why the gate must be
    // handed the point's instruments.
    let published = compare(SleeveKind::EtfTrend, as_of(), &proxy, &proxy, P);
    assert_eq!(published.verdict, GateVerdict::Refuse);
}
