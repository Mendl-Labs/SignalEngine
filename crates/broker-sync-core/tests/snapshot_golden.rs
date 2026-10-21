//! Golden tests for snapshot normalisation (ruling R54-E2). Fixtures in `tests/fixtures/alpaca/` are hand-written to
//! Alpaca's documented response shapes (copies of the broker-adapters fixtures); nothing here uses the network, a
//! real credential or a secret. The key strings below are obvious test constants, as in the adapter's own tests.

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::{AlpacaAdapter, AlpacaConfig, AlpacaCredentials, Environment as AlpacaEnv};
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpMethod, TransportError};
use broker_adapters::Dec;
use broker_sync_core::alpaca::AlpacaReader;
use broker_sync_core::fake::FakeReader;
use broker_sync_core::stubs::{BitstampReader, KrakenReader, OandaReader};
use broker_sync_core::{
    build_snapshot, AccountRef, AccountSummary, Broker, BrokerReader, Environment, OrderSide, Position, ReadError,
    SourceStatus,
};
use chrono::{DateTime, TimeZone, Utc};
use std::sync::Arc;

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!("fixtures/alpaca/", $name))
    };
}

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 20, 14, 32, 0).unwrap()
}

fn reader() -> (AlpacaReader, Arc<FakeTransport>) {
    let t = Arc::new(FakeTransport::new());
    let cfg = AlpacaConfig::new(AlpacaEnv::Paper, PAPER_BASE_URL).unwrap();
    let creds = AlpacaCredentials::new(AlpacaEnv::Paper, "PKTESTFIXTUREKEY0001", "unit-test-secret-not-a-real-key-9f3a").unwrap();
    let adapter = AlpacaAdapter::new(cfg, creds, t.clone()).unwrap();
    (AlpacaReader::new(adapter), t)
}

/// Queue the three reads of one cycle in call order: account, positions, open orders.
fn queue(t: &FakeTransport, account: &str, positions: &str, orders: &str) {
    t.enqueue_json(200, account);
    t.enqueue_json(200, positions);
    t.enqueue_json(200, orders);
}

#[test]
fn alpaca_paper_account_with_long_positions_and_open_orders() {
    let (r, t) = reader();
    queue(&t, fixture!("account_ok.json"), fixture!("positions_ok.json"), fixture!("orders_open.json"));
    let s = r.read_snapshot(now()).unwrap();

    assert_eq!(s.broker, Broker::Alpaca);
    assert_eq!(s.account, AccountRef("PA3TESTFIXT1".to_string()));
    assert_eq!(s.environment, Environment::Paper);
    assert_eq!(s.currency, "USD");
    assert_eq!(s.equity, d("100210.55"));
    assert_eq!(s.cash, d("52840.17"));
    assert_eq!(s.taken_at, now());
    assert_eq!(s.source_status, SourceStatus::Complete);

    // positions are sorted by symbol and carry price and value
    let symbols: Vec<&str> = s.positions.iter().map(|p| p.symbol.as_str()).collect();
    assert_eq!(symbols, ["EFA", "IEF", "SPY"]);
    let spy = s.position("SPY").unwrap();
    assert_eq!(spy.qty, d("12.345678901"));
    assert_eq!(spy.price, Some(d("512.10")));
    assert_eq!(spy.value, Some(d("6321.55")));

    // open orders: one tagged limit buy, one partially filled sell
    assert_eq!(s.open_orders.len(), 2);
    let buy = &s.open_orders[0];
    assert_eq!(buy.symbol, "SPY");
    assert_eq!(buy.side, Some(OrderSide::Buy));
    assert_eq!(buy.client_tag.as_deref(), Some("mvp1:run1:SPY:buy"));
    assert_eq!(buy.qty, d("10"));
    assert_eq!(buy.filled_qty, d("0"));
    let sell = &s.open_orders[1];
    assert_eq!(sell.side, Some(OrderSide::Sell));
    assert_eq!(sell.qty, d("5"));
    assert_eq!(sell.filled_qty, d("2"));
}

#[test]
fn alpaca_short_position_has_negative_quantity() {
    let (r, t) = reader();
    queue(&t, fixture!("account_ok.json"), fixture!("positions_short.json"), fixture!("orders_empty.json"));
    let s = r.read_snapshot(now()).unwrap();
    let dbc = s.position("DBC").unwrap();
    assert_eq!(dbc.qty, d("-10"));
    assert_eq!(dbc.value, Some(d("-221.00")));
    assert!(s.open_orders.is_empty());
}

#[test]
fn alpaca_empty_account_is_a_complete_snapshot_with_no_positions() {
    let (r, t) = reader();
    queue(&t, fixture!("account_ok.json"), fixture!("positions_empty.json"), fixture!("orders_empty.json"));
    let s = r.read_snapshot(now()).unwrap();
    assert!(s.positions.is_empty());
    assert!(s.open_orders.is_empty());
    assert_eq!(s.source_status, SourceStatus::Complete);
}

#[test]
fn alpaca_reader_issues_exactly_three_get_requests() {
    let (r, t) = reader();
    queue(&t, fixture!("account_ok.json"), fixture!("positions_ok.json"), fixture!("orders_empty.json"));
    r.read_snapshot(now()).unwrap();
    let reqs = t.requests();
    assert_eq!(reqs.len(), 3);
    assert!(reqs.iter().all(|q| q.method == HttpMethod::Get), "a non-GET request was sent: {reqs:?}");
}

#[test]
fn rejected_key_is_key_rejected_and_never_a_zero_snapshot() {
    let (r, t) = reader();
    t.enqueue_json(401, fixture!("error_401_unauthorized.json"));
    assert!(matches!(r.read_snapshot(now()), Err(ReadError::KeyRejected(_))));
}

#[test]
fn rate_limit_carries_retry_after() {
    let (r, t) = reader();
    t.enqueue_json_with_headers(429, fixture!("error_429.json"), &[("Retry-After", "7")]);
    assert_eq!(r.read_snapshot(now()), Err(ReadError::RateLimited { retry_after_secs: Some(7) }));
}

#[test]
fn timeout_is_unreachable() {
    let (r, t) = reader();
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(r.read_snapshot(now()), Err(ReadError::Unreachable(_))));
}

#[test]
fn malformed_account_body_is_malformed() {
    let (r, t) = reader();
    t.enqueue_json(200, "this is not json");
    assert!(matches!(r.read_snapshot(now()), Err(ReadError::Malformed(_))));
}

#[test]
fn failure_on_a_later_read_fails_the_whole_cycle() {
    let (r, t) = reader();
    t.enqueue_json(200, fixture!("account_ok.json"));
    t.enqueue_json(500, "{}");
    assert!(matches!(r.read_snapshot(now()), Err(ReadError::Unreachable(_))));
}

fn summary() -> AccountSummary {
    AccountSummary {
        account: AccountRef("ACC-1".to_string()),
        environment: Environment::Live,
        currency: "usd".to_string(),
        equity: d("1000"),
        cash: d("400"),
    }
}

fn pos(symbol: &str, qty: &str, price: Option<&str>, value: Option<&str>) -> Position {
    Position { symbol: symbol.to_string(), qty: d(qty), price: price.map(d), value: value.map(d) }
}

#[test]
fn unpriced_position_makes_the_snapshot_partial_and_is_not_defaulted_to_zero() {
    let positions = vec![pos("ZZZ", "3", None, None), pos("AAA", "1", Some("10"), Some("10"))];
    let s = build_snapshot(Broker::Alpaca, summary(), positions, vec![], now());
    assert_eq!(s.currency, "USD");
    assert_eq!(s.environment, Environment::Live);
    assert_eq!(s.positions[0].symbol, "AAA");
    let zzz = s.position("ZZZ").unwrap();
    assert_eq!(zzz.price, None);
    assert_eq!(zzz.value, None);
    match s.source_status {
        SourceStatus::Partial { notes } => {
            assert_eq!(notes.len(), 1);
            assert!(notes[0].starts_with("ZZZ:"));
        }
        other => panic!("expected Partial, got {other:?}"),
    }
}

#[test]
fn fake_reader_snapshot_and_read_counting() {
    let f = FakeReader::new(Broker::Kraken, summary(), vec![pos("BTC", "0.5", Some("60000"), Some("30000"))], vec![]);
    let s = f.read_snapshot(now()).unwrap();
    assert_eq!(s.broker, Broker::Kraken);
    assert_eq!(f.read_count(), 3);
    f.fail_positions(ReadError::Unreachable("down".to_string()));
    assert_eq!(f.read_snapshot(now()), Err(ReadError::Unreachable("down".to_string())));
}

#[test]
fn failing_fake_never_yields_a_snapshot() {
    let f = FakeReader::failing(Broker::Alpaca, ReadError::KeyRejected("HTTP 401".to_string()));
    assert!(f.read_snapshot(now()).is_err());
}

#[test]
fn stub_readers_are_unsupported_and_name_their_pr() {
    let readers: Vec<(Box<dyn BrokerReader>, Broker)> = vec![
        (Box::new(KrakenReader), Broker::Kraken),
        (Box::new(OandaReader), Broker::Oanda),
        (Box::new(BitstampReader), Broker::Bitstamp),
    ];
    for (r, b) in readers {
        assert_eq!(r.broker(), b);
        match r.read_snapshot(now()) {
            Err(ReadError::Unsupported { broker, pr }) => {
                assert_eq!(broker, b.as_str());
                assert!(pr.contains("R54"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
