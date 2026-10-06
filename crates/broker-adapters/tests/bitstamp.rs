//! Bitstamp adapter, offline. Every response is a fake HTTP reply. Fixtures under `fixtures/bitstamp/` are DOCUMENTED
//! SHAPES taken from the API page, not recordings from a live server. No test here reaches the network or uses a real key.

use std::collections::HashSet;
use std::sync::Arc;

use broker_adapters::bitstamp::auth::{BitstampCredentials, MillisClock, NonceMinter, ENV_API_KEY, ENV_API_SECRET};
use broker_adapters::bitstamp::order::{build_order, market_symbol};
use broker_adapters::bitstamp::{fetch_ticker, paths, BitstampAdapter, Environment, FailClosed, WithdrawalPermissionCheck, HOST};
use broker_adapters::bitstamp::market::ticker_path;
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpMethod, TransportError};
use broker_adapters::types::{BrokerAdapter, OrderRequest, OrderStatus, Side};
use broker_adapters::{BrokerError, Dec, ErrorClass};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const KEY: &str = "FAKEKEY-0000-NOT-A-REAL-KEY";
const SECRET: &str = "FAKESECRET-0000-NOT-A-REAL-SECRET";
const NOW_MS: u64 = 1_700_000_000_123;

fn fixture(name: &str) -> String {
    let path = format!("{}/tests/fixtures/bitstamp/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn dec(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

/// Stands in for the withdrawal-permission check that another PR builds: this test gate says "verified".
struct GateVerifiedForTests;

impl WithdrawalPermissionCheck for GateVerifiedForTests {
    fn check(&self, _creds: &BitstampCredentials) -> Result<(), BrokerError> {
        Ok(())
    }
}

struct FixedClock(u64);

impl MillisClock for FixedClock {
    fn now_millis(&self) -> u64 {
        self.0
    }
}

fn creds() -> BitstampCredentials {
    BitstampCredentials::new(KEY, SECRET).unwrap()
}

fn adapter(t: &Arc<FakeTransport>) -> BitstampAdapter {
    BitstampAdapter::new(creds(), t.clone(), Box::new(GateVerifiedForTests)).with_clock(Arc::new(FixedClock(NOW_MS)))
}

/// Neither the key nor the secret may appear in any rendering of an error.
fn assert_no_secret(e: &BrokerError) {
    for text in [format!("{e}"), format!("{e:?}")] {
        assert!(!text.contains(KEY), "key leaked into error: {text}");
        assert!(!text.contains(SECRET), "secret leaked into error: {text}");
    }
}

fn header<'a>(req: &'a broker_adapters::transport::HttpRequest, name: &str) -> Option<&'a str> {
    req.header(name)
}

#[test]
fn environment_is_live_and_the_broker_is_named_bitstamp() {
    let t = Arc::new(FakeTransport::new());
    let a = adapter(&t);
    assert_eq!(a.environment(), Environment::Live);
    assert_eq!(Environment::Live.as_str(), "live");
    assert_eq!(a.broker_name(), "bitstamp");
}

#[test]
fn credentials_come_from_the_environment_names_only_and_missing_values_are_reported_without_contents() {
    let both = BitstampCredentials::from_lookup(|n| match n {
        ENV_API_KEY => Some(KEY.to_string()),
        ENV_API_SECRET => Some(SECRET.to_string()),
        _ => None,
    });
    assert!(both.is_ok());

    let missing = BitstampCredentials::from_lookup(|n| (n == ENV_API_KEY).then(|| KEY.to_string())).unwrap_err();
    let text = format!("{missing}");
    assert!(text.contains(ENV_API_SECRET), "{text}");
    assert!(!text.contains(KEY), "{text}");

    assert!(BitstampCredentials::new("   ", SECRET).is_err());
    assert!(BitstampCredentials::new(KEY, "").is_err());
}

#[test]
fn debug_output_of_credentials_and_adapter_never_shows_the_key_or_secret() {
    let t = Arc::new(FakeTransport::new());
    let a = adapter(&t);
    for text in [format!("{:?}", creds()), format!("{a:?}")] {
        assert!(!text.contains(KEY), "{text}");
        assert!(!text.contains(SECRET), "{text}");
    }
}

#[test]
fn without_a_verified_withdrawal_check_private_calls_are_refused_and_nothing_is_sent() {
    let t = Arc::new(FakeTransport::new());
    let a = BitstampAdapter::new(creds(), t.clone(), Box::new(FailClosed)).with_clock(Arc::new(FixedClock(NOW_MS)));
    let errs = [
        a.get_balances().unwrap_err(),
        a.open_orders().unwrap_err(),
        a.get_order("123").unwrap_err(),
        a.cancel_order("123").unwrap_err(),
    ];
    for e in &errs {
        assert!(matches!(e, BrokerError::Credentials(_)), "{e:?}");
        assert_no_secret(e);
    }
    assert_eq!(t.request_count(), 0, "a refused private call must not reach the transport");
}

#[test]
fn balances_parse_from_the_documented_shape_and_the_request_is_signed_as_documented() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &fixture("account_balances.json"));
    let b = adapter(&t).get_balances().unwrap();
    assert_eq!(b.spot("USD"), dec("90.00"), "Spot = the documented free `available` balance");
    assert_eq!(b.spot("btc"), dec("0.50000000"));
    assert_eq!(b.spot("ETH"), Dec::ZERO);

    let reqs = t.requests();
    assert_eq!(reqs.len(), 1);
    let r = &reqs[0];
    assert_eq!(r.method, HttpMethod::Post);
    assert_eq!(r.url, format!("https://{HOST}{}", paths::ACCOUNT_BALANCES));
    assert_eq!(header(r, "X-Auth"), Some(format!("BITSTAMP {KEY}").as_str()));
    assert_eq!(header(r, "X-Auth-Version"), Some("v2"));
    assert_eq!(header(r, "X-Auth-Timestamp"), Some("1700000000123"));
    assert!(r.body.is_none() && header(r, "Content-Type").is_none(), "no body means no Content-Type");

    let nonce = header(r, "X-Auth-Nonce").unwrap();
    assert_eq!(nonce.len(), 36, "nonce must be 36 characters");
    assert_eq!(nonce, nonce.to_lowercase(), "nonce must be lowercase");
    assert_eq!(nonce.matches('-').count(), 4);
}

#[test]
fn the_signature_matches_an_independent_recomputation_of_the_documented_message() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &fixture("account_balances.json"));
    adapter(&t).get_balances().unwrap();
    let r = &t.requests()[0];
    let nonce = header(r, "X-Auth-Nonce").unwrap();
    // BITSTAMP + key + verb + host + path + query("") + content-type("") + nonce + timestamp + "v2" + body("")
    let message = format!("BITSTAMP {KEY}POST{HOST}{}{nonce}{NOW_MS}v2", paths::ACCOUNT_BALANCES);
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(message.as_bytes());
    let expected: String = mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(header(r, "X-Auth-Signature"), Some(expected.as_str()));
}

#[test]
fn a_form_body_carries_its_content_type_and_the_key_never_appears_in_url_or_body() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &fixture("cancel_order.json"));
    adapter(&t).cancel_order("1453282316578816").unwrap();
    let r = &t.requests()[0];
    assert_eq!(r.url, format!("https://{HOST}{}", paths::CANCEL_ORDER));
    assert_eq!(r.body.as_deref(), Some("id=1453282316578816"));
    assert_eq!(header(r, "Content-Type"), Some("application/x-www-form-urlencoded"));
    assert!(!r.url.contains(KEY) && !r.body.as_deref().unwrap_or("").contains(KEY));
}

#[test]
fn nonces_are_unique_within_one_millisecond_and_well_formed() {
    let m = NonceMinter::new();
    let mut seen = HashSet::new();
    for _ in 0..5_000 {
        let n = m.mint(NOW_MS);
        assert_eq!(n.len(), 36);
        assert!(n.bytes().all(|b| b == b'-' || b.is_ascii_digit() || (b'a'..=b'f').contains(&b)), "{n}");
        assert!(seen.insert(n), "duplicate nonce");
    }
}

#[test]
fn a_documented_error_body_becomes_a_typed_exchange_error_without_the_key() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(400, &fixture("error_response_code.json"));
    let e = adapter(&t).get_balances().unwrap_err();
    match &e {
        BrokerError::Exchange(v) => {
            assert_eq!(v.len(), 1);
            assert_eq!(v[0].code, "400.001");
            assert_eq!(v[0].class, ErrorClass::Other);
        }
        other => panic!("expected a typed exchange error, got {other:?}"),
    }
    assert_no_secret(&e);
}

#[test]
fn http_status_classes_map_to_typed_errors() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(401, "");
    match adapter(&t).get_balances().unwrap_err() {
        BrokerError::Exchange(v) => assert_eq!(v[0].class, ErrorClass::Auth),
        other => panic!("{other:?}"),
    }

    t.enqueue_json(429, "");
    assert!(matches!(adapter(&t).get_balances().unwrap_err(), BrokerError::RateLimited { .. }));

    t.enqueue_json(503, "");
    match adapter(&t).get_balances().unwrap_err() {
        BrokerError::Exchange(v) => {
            assert_eq!(v[0].class, ErrorClass::ServiceUnavailable);
            assert!(v[0].outcome_unknown());
        }
        other => panic!("{other:?}"),
    }

    t.enqueue_json(200, "not json");
    assert!(matches!(adapter(&t).get_balances().unwrap_err(), BrokerError::Malformed(_)));

    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(adapter(&t).get_balances().unwrap_err(), BrokerError::Transport(_)));
}

#[test]
fn open_orders_parse_and_a_partially_filled_order_reports_its_executed_quantity() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &fixture("open_orders.json"));
    let open = adapter(&t).open_orders().unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].broker_order_id, "1234123412341234");
    assert_eq!(open[0].symbol, "BTC/USD");
    assert_eq!(open[0].status, OrderStatus::Open);
    assert_eq!(open[0].executed_quantity, Dec::ZERO);
    assert_eq!(open[0].side, None, "side codes are UNVERIFIED, so none is guessed");

    t.enqueue_json(200, &fixture("open_orders_partially_filled.json"));
    let partial = adapter(&t).open_orders().unwrap();
    assert_eq!(partial[0].status, OrderStatus::PartiallyFilled);
    assert_eq!(partial[0].executed_quantity, dec("0.30000000"));
    assert_eq!(partial[0].quantity, dec("0.50000000"));
}

#[test]
fn order_status_reads_an_open_order_and_refuses_what_it_cannot_derive() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &fixture("order_status_open.json"));
    let r = adapter(&t).get_order("1458532827766784").unwrap();
    assert_eq!(r.status, OrderStatus::Open);
    assert_eq!(r.quantity, dec("100.00"));
    let req = &t.requests()[0];
    assert_eq!(req.url, format!("https://{HOST}{}", paths::ORDER_STATUS));
    assert_eq!(req.body.as_deref(), Some("id=1458532827766784"));

    t.enqueue_json(200, &fixture("order_status_with_transactions.json"));
    assert!(matches!(adapter(&t).get_order("1458532827766784").unwrap_err(), BrokerError::Unsupported(_)));

    t.enqueue_json(200, &fixture("order_status_finished.json"));
    assert!(matches!(adapter(&t).get_order("1458532827766784").unwrap_err(), BrokerError::Malformed(_)));

    let before = t.request_count();
    assert!(matches!(adapter(&t).get_order("abc"), Err(BrokerError::InvalidRequest(_))));
    assert_eq!(t.request_count(), before, "a malformed id is refused before any request");
}

#[test]
fn cancel_parses_the_documented_answer_and_refuses_an_unknown_status() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &fixture("cancel_order.json"));
    let c = adapter(&t).cancel_order("1453282316578816").unwrap();
    assert_eq!(c.canceled_count, 1);
    assert!(!c.pending);

    t.enqueue_json(200, r#"{"id": 1, "status": "Pending"}"#);
    assert!(matches!(adapter(&t).cancel_order("1").unwrap_err(), BrokerError::Malformed(_)));
}

#[test]
fn the_ticker_gives_best_bid_ask_and_timestamp_and_the_mid_is_exact() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &fixture("ticker_btcusd.json"));
    let q = fetch_ticker(&*t, "BTC/USD").unwrap();
    assert_eq!(q.bid, dec("2188.97"));
    assert_eq!(q.ask, dec("2211.00"));
    assert_eq!(q.timestamp_secs, 1_643_640_186);
    assert_eq!(q.mid().unwrap(), dec("2199.985"));

    let r = &t.requests()[0];
    assert_eq!(r.method, HttpMethod::Get);
    assert_eq!(r.url, format!("https://www.bitstamp.net{}", ticker_path("btcusd")));
    assert!(r.headers.iter().all(|(k, _)| !k.to_lowercase().starts_with("x-auth")), "public ticker sends no key");
}

#[test]
fn get_quote_through_the_adapter_needs_no_verified_gate_and_no_key() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &fixture("ticker_btcusd.json"));
    let a = BitstampAdapter::new(creds(), t.clone(), Box::new(FailClosed));
    let q = a.get_quote("BTC/USD").unwrap();
    assert_eq!(q.bid, dec("2188.97"));
    assert_eq!(q.ask, dec("2211.00"));
    assert_eq!(q.last, dec("2211.00"));
}

#[test]
fn a_crossed_or_failed_ticker_is_refused() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &fixture("ticker_crossed.json"));
    assert!(matches!(fetch_ticker(&*t, "BTC/USD").unwrap_err(), BrokerError::Malformed(_)));

    t.enqueue_json(500, "");
    assert!(matches!(fetch_ticker(&*t, "BTC/USD").unwrap_err(), BrokerError::Exchange(_)));

    assert!(matches!(fetch_ticker(&*t, "DOGE/USD"), Err(BrokerError::UnknownSymbol(_))));
}

#[test]
fn the_order_request_has_the_documented_shape_for_market_and_limit_orders() {
    let m = build_order(&OrderRequest::market("run1:BTC/USD:buy", "BTC/USD", Side::Buy, dec("0.5"))).unwrap();
    assert_eq!(m.path, "/api/v2/buy/market/btcusd/");
    assert_eq!(
        m.params,
        vec![
            ("amount".to_string(), "0.50000000".to_string()),
            ("client_order_id".to_string(), "run1:BTC/USD:buy".to_string()),
        ]
    );

    let l = build_order(&OrderRequest::limit("run1:ETH/USD:sell", "ETH/USD", Side::Sell, dec("45"), dec("2211.00"))).unwrap();
    assert_eq!(l.path, "/api/v2/sell/ethusd/");
    assert_eq!(
        l.params,
        vec![
            ("price".to_string(), "2211.00".to_string()),
            ("amount".to_string(), "45.00000000".to_string()),
            ("client_order_id".to_string(), "run1:ETH/USD:sell".to_string()),
        ]
    );
}

#[test]
fn order_requests_that_the_documentation_cannot_express_are_refused_not_dropped() {
    let base = OrderRequest::market("t", "BTC/USD", Side::Buy, dec("1"));

    let mut validate = base.clone();
    validate.validate_only = true;
    assert!(matches!(build_order(&validate), Err(BrokerError::Unsupported(_))));

    let mut post_only = base.clone();
    post_only.post_only = true;
    assert!(matches!(build_order(&post_only), Err(BrokerError::Unsupported(_))));

    let mut reduce = base.clone();
    reduce.reduce_only = true;
    assert!(matches!(build_order(&reduce), Err(BrokerError::Unsupported(_))));

    let mut tif = base.clone();
    tif.time_in_force = Some(broker_adapters::types::TimeInForce::Ioc);
    assert!(matches!(build_order(&tif), Err(BrokerError::Unsupported(_))));

    assert!(matches!(build_order(&OrderRequest::market("t", "DOGE/USD", Side::Buy, dec("1"))), Err(BrokerError::UnknownSymbol(_))));
    assert!(matches!(build_order(&OrderRequest::market("t", "BTC/USD", Side::Buy, dec("0"))), Err(BrokerError::InvalidRequest(_))));
    assert!(matches!(build_order(&OrderRequest::market("", "BTC/USD", Side::Buy, dec("1"))), Err(BrokerError::InvalidRequest(_))));
    assert!(matches!(build_order(&OrderRequest::market("t", "BTC/USD", Side::Buy, dec("0.123456789"))), Err(BrokerError::InvalidRequest(_))));
    assert!(matches!(
        build_order(&OrderRequest::limit("t", "BTC/USD", Side::Buy, dec("1"), dec("-5"))),
        Err(BrokerError::InvalidPrice(_))
    ));
}

#[test]
fn place_order_never_sends_anything_because_placement_is_disabled() {
    let t = Arc::new(FakeTransport::new());
    let a = adapter(&t);
    let plain = OrderRequest::market("t", "BTC/USD", Side::Buy, dec("1"));
    assert!(matches!(a.place_order(&plain), Err(BrokerError::Unsupported(_))));

    let mut validate = plain.clone();
    validate.validate_only = true;
    assert!(matches!(a.place_order(&validate), Err(BrokerError::Unsupported(_))));

    let limit = OrderRequest::limit("t", "ETH/USD", Side::Sell, dec("1"), dec("2000"));
    assert!(matches!(a.place_order(&limit), Err(BrokerError::Unsupported(_))));

    assert_eq!(t.request_count(), 0, "no order request may reach the transport");
}

#[test]
fn tag_lookup_is_refused_because_no_order_is_ever_placed() {
    let t = Arc::new(FakeTransport::new());
    assert!(matches!(adapter(&t).find_orders_by_tag("run1:BTC/USD:buy"), Err(BrokerError::Unsupported(_))));
    assert_eq!(t.request_count(), 0);
}

#[test]
fn the_supported_pairs_are_exactly_btc_and_eth_against_usd() {
    assert_eq!(market_symbol("BTC/USD").unwrap(), "btcusd");
    assert_eq!(market_symbol("ETH/USD").unwrap(), "ethusd");
    assert!(market_symbol("BTC/EUR").is_err());
    assert!(market_symbol("SOL/USD").is_err());
}
