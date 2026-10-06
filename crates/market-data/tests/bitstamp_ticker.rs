//! `BitstampTickerSource` offline. Responses are DOCUMENTED-SHAPE fixtures (`fixtures/bitstamp/`), not live recordings.

use std::sync::Arc;

use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpResponse, TransportError};
use chrono::{DateTime, TimeZone, Utc};
use market_data::BitstampTickerSource;
use rebalancer_core::Dec;
use rebalancer_run::data::{DataSource, SleeveKind, SleeveSpec};

fn fixture(name: &str) -> String {
    let path = format!("{}/tests/fixtures/bitstamp/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn dec(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

/// A transport answering the two public ticker paths from the fixtures, and 404 for anything else.
fn transport() -> Arc<FakeTransport> {
    let btc = fixture("ticker_btcusd.json");
    let eth = fixture("ticker_ethusd.json");
    let t = Arc::new(FakeTransport::new());
    t.set_handler(move |req| {
        let body = if req.url.ends_with("/api/v2/ticker/btcusd/") {
            btc.clone()
        } else if req.url.ends_with("/api/v2/ticker/ethusd/") {
            eth.clone()
        } else {
            return Ok(HttpResponse { status: 404, body: String::new() });
        };
        Ok(HttpResponse { status: 200, body })
    });
    t
}

fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_643_640_200, 0).unwrap()
}

fn symbols(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

#[test]
fn prices_are_the_mid_of_best_bid_and_ask_stamped_with_the_ticker_time() {
    let t = transport();
    let src = BitstampTickerSource::new(t.clone());
    let prices = src.prices(&symbols(&["BTC/USD", "ETH/USD"]), now()).unwrap();

    let btc = &prices["BTC/USD"];
    assert_eq!(btc.price, dec("2199.985"), "mid of 2188.97 and 2211.00");
    assert_eq!(btc.as_of, Utc.timestamp_opt(1_643_640_186, 0).unwrap());

    assert_eq!(prices["ETH/USD"].price, dec("1500"), "mid of 1499.00 and 1501.00");
    assert_eq!(t.request_count(), 2);
    for r in t.requests() {
        assert!(r.headers.iter().all(|(k, _)| !k.to_lowercase().starts_with("x-auth")), "public ticker sends no key");
        assert!(r.body.is_none());
    }
}

#[test]
fn a_symbol_outside_btc_and_eth_is_absent_and_costs_no_request() {
    let t = transport();
    let src = BitstampTickerSource::new(t.clone());
    let prices = src.prices(&symbols(&["DOGE/USD", "BTC/USD"]), now()).unwrap();
    assert_eq!(prices.keys().map(String::as_str).collect::<Vec<_>>(), vec!["BTC/USD"]);
    assert_eq!(t.request_count(), 1);
}

#[test]
fn an_outage_is_an_error_not_an_absent_price() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_error(TransportError::Timeout);
    let src = BitstampTickerSource::new(t);
    let err = src.prices(&symbols(&["BTC/USD"]), now()).unwrap_err();
    assert_eq!(err.code, "BITSTAMP_UNAVAILABLE");
}

#[test]
fn a_failed_ticker_response_is_an_error() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(500, "");
    let src = BitstampTickerSource::new(t);
    assert_eq!(src.prices(&symbols(&["ETH/USD"]), now()).unwrap_err().code, "BITSTAMP_UNAVAILABLE");
}

#[test]
fn sleeves_are_refused_because_the_ticker_has_no_history() {
    let src = BitstampTickerSource::new(transport());
    let spec = SleeveSpec {
        id: "crypto".to_string(),
        kind: SleeveKind::CryptoTrend,
        share: dec("1"),
        venue: "bitstamp".to_string(),
        asset_class: "crypto".to_string(),
        quote: "USD".to_string(),
    };
    let err = src.sleeve_data(&spec, chrono::NaiveDate::from_ymd_opt(2026, 10, 6).unwrap()).unwrap_err();
    assert_eq!(err.code, "BITSTAMP_NO_HISTORY");
}
