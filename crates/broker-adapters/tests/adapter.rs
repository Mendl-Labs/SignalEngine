//! End-to-end adapter behaviour over recorded-style Kraken JSON fixtures and `FakeTransport`.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use broker_adapters::kraken::auth::{sign_with_secret, KrakenCredentials};
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::kraken::parse::{classify, derive_status, parse_envelope};
use broker_adapters::kraken::{KrakenAdapter, KrakenConfig};
use broker_adapters::nonce::{InMemoryNonceStore, NonceGenerator};
use broker_adapters::testing::{FakeTransport, ManualClock};
use broker_adapters::transport::{HttpMethod, HttpRequest, TransportError};
use broker_adapters::types::{
    BalanceKind, BrokerAdapter, OrderKind, OrderRequest, OrderStatus, PlaceOutcome, Side,
};
use broker_adapters::{BrokerError, Dec, ErrorClass};
use std::sync::Arc;

const SECRET_B64_SRC: &[u8] = b"unit-test-secret-not-a-real-key";

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!("fixtures/", $name))
    };
}

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

fn setup_with(config: KrakenConfig) -> (KrakenAdapter, Arc<FakeTransport>) {
    let transport = Arc::new(FakeTransport::new());
    let creds = KrakenCredentials::new("TEST-API-KEY", &B64.encode(SECRET_B64_SRC)).unwrap();
    let nonces = NonceGenerator::new(Arc::new(InMemoryNonceStore::new()), Arc::new(ManualClock::new(1_000_000)));
    let adapter = KrakenAdapter::new(config, creds, transport.clone(), nonces, PairTable::builtin());
    (adapter, transport)
}

fn setup() -> (KrakenAdapter, Arc<FakeTransport>) {
    setup_with(KrakenConfig::default())
}

/// Substitute the userref the adapter assigned to `tag` into a fixture.
fn with_userref(adapter: &KrakenAdapter, tag: &str, fixture: &str) -> String {
    fixture.replace("__USERREF__", &adapter.reserve_userref(tag).unwrap().to_string())
}

fn path_of(req: &HttpRequest) -> String {
    let after_host = req.url.strip_prefix("https://api.kraken.com").unwrap();
    after_host.split('?').next().unwrap().to_string()
}

fn body_param(req: &HttpRequest, key: &str) -> Option<String> {
    req.body.as_ref()?.split('&').find_map(|kv| kv.strip_prefix(&format!("{key}=")).map(str::to_string))
}

fn assert_validly_signed(req: &HttpRequest) {
    let body = req.body.as_ref().expect("private request has a body");
    let nonce: u64 = body_param(req, "nonce").expect("nonce in body").parse().unwrap();
    assert!(body.starts_with("nonce="), "nonce must be the first field: {body}");
    let expected = sign_with_secret(SECRET_B64_SRC, &path_of(req), nonce, body);
    assert_eq!(req.header("API-Sign"), Some(expected.as_str()));
    assert_eq!(req.header("API-Key"), Some("TEST-API-KEY"));
}

fn limit_req(tag: &str) -> OrderRequest {
    OrderRequest::limit(tag, "BTC/USD", Side::Buy, d("0.0025"), d("61234.5"))
}

// ---------------------------------------------------------------- reads

#[test]
fn balances_are_parsed_mapped_and_split_into_spot_and_earn() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("balance_ok.json"));
    let b = a.get_balances().unwrap();
    assert_eq!(b.spot("USD"), d("171288.6158"));
    assert_eq!(b.spot("BTC"), d("0.42315"));
    assert_eq!(b.spot("ETH"), d("3.1"), "staked ETH2.S must not count as spot ETH");
    assert_eq!(b.spot("USDT"), d("250"));
    assert!(b.entries.iter().any(|e| e.raw_asset == "ETH2.S" && e.kind == BalanceKind::Earn));
    assert!(b.entries.iter().any(|e| e.raw_asset == "XBT.M" && e.kind == BalanceKind::Earn && e.asset == "BTC"));
    let reqs = t.requests();
    assert_eq!(reqs.len(), 1);
    assert_eq!(path_of(&reqs[0]), "/0/private/Balance");
    assert_eq!(reqs[0].method, HttpMethod::Post);
    assert_validly_signed(&reqs[0]);
}

#[test]
fn trade_balance_is_parsed() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("trade_balance_ok.json"));
    let tb = a.trade_balance(Some("ZUSD")).unwrap();
    assert_eq!(tb.equity, Some(d("171288.6158")));
    assert_eq!(tb.equivalent_balance, Some(d("171288.6158")));
    assert_eq!(tb.margin_level, None);
    let r = &t.requests()[0];
    assert_eq!(path_of(r), "/0/private/TradeBalance");
    assert_eq!(body_param(r, "asset").as_deref(), Some("ZUSD"));
    assert_validly_signed(r);
}

#[test]
fn ticker_is_public_unsigned_and_parsed_via_rest_name() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("ticker_ok.json"));
    let q = a.get_quote("BTC/USD").unwrap();
    assert_eq!((q.bid, q.ask, q.last), (d("61234.5"), d("61234.6"), d("61234.5")));
    assert_eq!(q.symbol, "BTC/USD");
    let r = &t.requests()[0];
    assert_eq!(r.method, HttpMethod::Get);
    assert_eq!(r.url, "https://api.kraken.com/0/public/Ticker?pair=XBTUSD");
    assert!(r.header("API-Key").is_none() && r.header("API-Sign").is_none(), "public calls are not signed");
}

#[test]
fn crossed_or_unknown_ticker_is_an_error() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("ticker_crossed.json"));
    assert!(matches!(a.get_quote("BTC/USD"), Err(BrokerError::Malformed(_))));
    assert!(matches!(a.get_quote("NOPE/USD"), Err(BrokerError::UnknownSymbol(_))));
}

// ---------------------------------------------------------------- placement

#[test]
fn accepted_order_carries_txid_rounded_values_and_a_signed_request() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("add_order_ok.json"));
    let mut req = OrderRequest::limit("run1:BTC/USD:buy", "BTC/USD", Side::Buy, d("0.002500009"), d("61234.56"));
    req.post_only = true;
    let out = a.place_order(&req).unwrap();
    let userref = a.reserve_userref("run1:BTC/USD:buy").unwrap();
    match out {
        PlaceOutcome::Accepted { broker_order_id, sent, description, warnings } => {
            assert_eq!(broker_order_id, "OUF4EM-FRGI2-MQMWZD");
            assert_eq!(sent.quantity, d("0.0025"));
            assert_eq!(sent.price, Some(d("61234.5")));
            assert_eq!(sent.userref, userref);
            assert!(!sent.validate_only);
            assert!(description.unwrap().contains("XBTUSD"));
            assert!(warnings.is_empty());
        }
        other => panic!("{other:?}"),
    }
    let r = &t.requests()[0];
    assert_eq!(path_of(r), "/0/private/AddOrder");
    assert_validly_signed(r);
    assert_eq!(body_param(r, "pair").as_deref(), Some("XBTUSD"));
    assert_eq!(body_param(r, "volume").as_deref(), Some("0.00250000"));
    assert_eq!(body_param(r, "price").as_deref(), Some("61234.5"));
    assert_eq!(body_param(r, "oflags").as_deref(), Some("post"));
    assert_eq!(body_param(r, "userref"), Some(userref.to_string()));
    assert!(body_param(r, "validate").is_none());
}

#[test]
fn validate_only_response_is_reported_as_validated_and_request_carries_validate_true() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("add_order_validate_ok.json"));
    let mut req = limit_req("v1");
    req.validate_only = true;
    match a.place_order(&req).unwrap() {
        PlaceOutcome::ValidatedOnly { description, sent, .. } => {
            assert!(description.unwrap().starts_with("buy 0.00250000 XBTUSD"));
            assert!(sent.validate_only);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(body_param(&t.requests()[0], "validate").as_deref(), Some("true"));
}

#[test]
fn force_validate_config_validates_every_order() {
    let (a, t) = setup_with(KrakenConfig { force_validate: true, ..KrakenConfig::default() });
    t.enqueue_json(200, fixture!("add_order_validate_ok.json"));
    let out = a.place_order(&limit_req("paper1")).unwrap(); // request did NOT ask for validate
    assert!(matches!(out, PlaceOutcome::ValidatedOnly { .. }));
    assert_eq!(body_param(&t.requests()[0], "validate").as_deref(), Some("true"));
}

#[test]
fn validate_request_that_comes_back_with_a_txid_is_flagged_unknown_not_accepted() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("add_order_ok.json"));
    let mut req = limit_req("v2");
    req.validate_only = true;
    match a.place_order(&req).unwrap() {
        PlaceOutcome::UnknownOutcome { reason, .. } => assert!(reason.contains("OUF4EM-FRGI2-MQMWZD")),
        other => panic!("{other:?}"),
    }
}

#[test]
fn success_without_txid_on_a_live_order_is_unknown_outcome() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("add_order_no_txid.json"));
    assert!(matches!(a.place_order(&limit_req("n1")).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
}

#[test]
fn warnings_do_not_fail_the_call_and_are_surfaced() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("add_order_with_warning.json"));
    match a.place_order(&limit_req("w1")).unwrap() {
        PlaceOutcome::Accepted { warnings, .. } => assert_eq!(warnings, vec!["WOrder:Something advisory".to_string()]),
        other => panic!("{other:?}"),
    }
}

fn rejected_class(fixture_body: &str) -> Vec<ErrorClass> {
    let (a, t) = setup();
    t.enqueue_json(200, fixture_body);
    match a.place_order(&limit_req("rej")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => errors.iter().map(|e| e.class).collect(),
        other => panic!("expected Rejected, got {other:?}"),
    }
}

#[test]
fn error_array_is_classified_on_placement() {
    assert_eq!(rejected_class(fixture!("error_insufficient_funds.json")), vec![ErrorClass::InsufficientFunds]);
    assert_eq!(rejected_class(fixture!("error_invalid_nonce.json")), vec![ErrorClass::InvalidNonce]);
    assert_eq!(rejected_class(fixture!("error_invalid_arguments.json")), vec![ErrorClass::InvalidArguments]);
    assert_eq!(rejected_class(fixture!("error_invalid_key.json")), vec![ErrorClass::Auth]);
    assert_eq!(rejected_class(fixture!("error_rate_limit.json")), vec![ErrorClass::RateLimited]);
    // the warning entry is split off; both errors are kept in order
    assert_eq!(
        rejected_class(fixture!("error_multiple.json")),
        vec![ErrorClass::InsufficientFunds, ErrorClass::InvalidArguments]
    );
}

#[test]
fn rejection_keeps_the_exact_error_code_text() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("error_insufficient_funds.json"));
    match a.place_order(&limit_req("rej2")).unwrap() {
        PlaceOutcome::Rejected { errors, sent } => {
            assert_eq!(errors[0].code, "EOrder:Insufficient funds");
            assert_eq!(sent.broker_pair, "XBTUSD");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn ambiguous_failures_on_placement_are_unknown_outcome_never_rejected() {
    // exchange says it is unavailable
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("error_service_unavailable.json"));
    assert!(matches!(a.place_order(&limit_req("u1")).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
    // timeout
    let (a, t) = setup();
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(a.place_order(&limit_req("u2")).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
    // i/o error mid-flight
    let (a, t) = setup();
    t.enqueue_error(TransportError::Io("connection reset".into()));
    assert!(matches!(a.place_order(&limit_req("u3")).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
    // gateway error page
    let (a, t) = setup();
    t.enqueue_json(502, "<html>Bad Gateway</html>");
    assert!(matches!(a.place_order(&limit_req("u4")).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
    // 200 with garbage
    let (a, t) = setup();
    t.enqueue_json(200, "not json at all");
    assert!(matches!(a.place_order(&limit_req("u5")).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
    // 200 JSON but no `error` key
    let (a, t) = setup();
    t.enqueue_json(200, r#"{"result":{"txid":["OX"]}}"#);
    assert!(matches!(a.place_order(&limit_req("u6")).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
}

#[test]
fn connect_failure_is_a_definite_not_sent_error() {
    let (a, t) = setup();
    t.enqueue_error(TransportError::ConnectFailed("dns".into()));
    match a.place_order(&limit_req("c1")) {
        Err(BrokerError::Transport(e)) => assert!(e.request_definitely_not_sent()),
        other => panic!("{other:?}"),
    }
}

#[test]
fn local_refusals_send_nothing() {
    let (a, t) = setup();
    let tiny = OrderRequest::limit("tiny", "BTC/USD", Side::Buy, d("0.00001"), d("61000"));
    assert!(matches!(a.place_order(&tiny), Err(BrokerError::BelowMinQuantity { .. })));
    let mut ro = limit_req("ro");
    ro.reduce_only = true;
    assert!(matches!(a.place_order(&ro), Err(BrokerError::Unsupported(_))));
    assert_eq!(t.request_count(), 0);
}

#[test]
fn nonces_in_consecutive_requests_strictly_increase() {
    let (a, t) = setup();
    for _ in 0..3 {
        t.enqueue_json(200, fixture!("balance_ok.json"));
        a.get_balances().unwrap();
    }
    let ns: Vec<u64> = t.requests().iter().map(|r| body_param(r, "nonce").unwrap().parse().unwrap()).collect();
    assert!(ns.windows(2).all(|w| w[0] < w[1]), "{ns:?}");
}

// ---------------------------------------------------------------- order state

fn query_one(fixture_body: &str, tag: &str, txid: &str) -> Result<broker_adapters::OrderReport, BrokerError> {
    let (a, t) = setup();
    t.enqueue_json(200, &with_userref(&a, tag, fixture_body));
    a.get_order(txid)
}

#[test]
fn canceled_order_with_executed_volume_is_partially_filled_then_canceled_and_keeps_the_fill() {
    let r = query_one(fixture!("query_orders_canceled_partial.json"), "tag-pc", "OQCLML-BW3P3-BUCMWZ").unwrap();
    assert_eq!(r.status, OrderStatus::PartiallyFilledThenCanceled);
    assert!(r.status.is_terminal() && r.status.has_fills());
    assert_eq!(r.raw_status, "canceled");
    assert_eq!(r.quantity, d("1.25"));
    assert_eq!(r.executed_quantity, d("0.5"));
    assert_eq!(r.avg_price, Some(d("61000")));
    assert_eq!(r.cost, Some(d("30500")));
    assert_eq!(r.fee, Some(d("48.8")));
    assert_eq!(r.reason.as_deref(), Some("User requested"));
    assert_eq!(r.symbol, "BTC/USD");
    assert_eq!(r.side, Some(Side::Buy));
    assert_eq!(r.kind, Some(OrderKind::Limit { price: d("61000") }));
    assert_eq!(r.tag.as_deref(), Some("tag-pc"));
    assert_eq!(r.close_time, Some(1758463260.5678));
}

#[test]
fn canceled_with_nothing_executed_is_plain_canceled() {
    let r = query_one(fixture!("query_orders_canceled_zero.json"), "tag-cz", "OZERO1-AAAAA-BBBBBB").unwrap();
    assert_eq!(r.status, OrderStatus::Canceled);
    assert!(!r.status.has_fills());
    assert_eq!(r.executed_quantity, Dec::ZERO);
    assert_eq!(r.avg_price, None);
    assert_eq!(r.symbol, "ETH/USD");
    assert_eq!(r.side, Some(Side::Sell));
}

#[test]
fn closed_with_zero_fill_is_canceled_and_closed_with_full_fill_is_filled() {
    let r = query_one(fixture!("query_orders_closed_zero.json"), "tag-c0", "OZERO2-AAAAA-BBBBBB").unwrap();
    assert_eq!(r.status, OrderStatus::Canceled);
    let r = query_one(fixture!("query_orders_filled.json"), "tag-f", "OABC1-XYZ23-DEF456").unwrap();
    assert_eq!(r.status, OrderStatus::Filled);
    assert_eq!(r.executed_quantity, d("0.0025"));
    assert_eq!(r.avg_price, Some(d("61234.5")));
    assert_eq!(r.cost, Some(d("153.08625")));
    assert_eq!(r.fee, Some(d("0.24494")));
    assert_eq!(r.kind, Some(OrderKind::Market));
}

#[test]
fn expired_partial_open_partial_and_pending() {
    let r = query_one(fixture!("query_orders_expired_partial.json"), "t1", "OEXPI1-AAAAA-BBBBBB").unwrap();
    assert_eq!(r.status, OrderStatus::PartiallyFilledThenExpired);
    assert_eq!(r.executed_quantity, d("0.25"));
    let r = query_one(fixture!("query_orders_open_partial.json"), "t2", "OOPEN1-AAAAA-BBBBBB").unwrap();
    assert_eq!(r.status, OrderStatus::PartiallyFilled);
    assert!(!r.status.is_terminal() && r.status.has_fills());
    let r = query_one(fixture!("query_orders_pending.json"), "t3", "OPEND1-AAAAA-BBBBBB").unwrap();
    assert_eq!(r.status, OrderStatus::Pending);
}

#[test]
fn status_derivation_table() {
    let (v, z, half) = (d("1"), d("0"), d("0.5"));
    let cases = [
        ("pending", z, OrderStatus::Pending),
        ("open", z, OrderStatus::Open),
        ("open", half, OrderStatus::PartiallyFilled),
        ("closed", v, OrderStatus::Filled),
        ("closed", z, OrderStatus::Canceled),
        ("closed", half, OrderStatus::PartiallyFilledThenCanceled),
        ("canceled", z, OrderStatus::Canceled),
        ("canceled", half, OrderStatus::PartiallyFilledThenCanceled),
        ("canceled", v, OrderStatus::Filled),
        ("expired", z, OrderStatus::Expired),
        ("expired", half, OrderStatus::PartiallyFilledThenExpired),
    ];
    for (raw, exec, want) in cases {
        assert_eq!(derive_status(raw, v, exec).unwrap(), want, "{raw} exec={exec}");
    }
    assert!(derive_status("bogus", v, z).is_err());
    assert!(derive_status("open", v, d("-1")).is_err());
}

#[test]
fn missing_vol_exec_or_unknown_status_fail_closed() {
    assert!(matches!(
        query_one(fixture!("query_orders_missing_vol_exec.json"), "x", "OBAD01-AAAAA-BBBBBB"),
        Err(BrokerError::Malformed(_))
    ));
    assert!(matches!(
        query_one(fixture!("query_orders_unknown_status.json"), "x", "OBAD02-AAAAA-BBBBBB"),
        Err(BrokerError::Malformed(_))
    ));
}

#[test]
fn foreign_userref_has_no_tag_and_rest_name_pair_is_mapped() {
    let r = query_one(fixture!("query_orders_foreign.json"), "mine", "OFORE1-AAAAA-BBBBBB").unwrap();
    assert_eq!(r.userref, Some(123));
    assert_eq!(r.tag, None);
    assert_eq!(r.symbol, "BTC/USD");
}

#[test]
fn get_order_for_an_id_missing_from_the_response_is_not_found() {
    let (a, t) = setup();
    t.enqueue_json(200, &with_userref(&a, "t", fixture!("query_orders_filled.json")));
    assert!(matches!(a.get_order("OTHER-ID-000000"), Err(BrokerError::NotFound(_))));
}

#[test]
fn open_orders_lists_ours_and_foreign() {
    let (a, t) = setup();
    t.enqueue_json(200, &with_userref(&a, "ours", fixture!("open_orders_ok.json")));
    let orders = a.open_orders().unwrap();
    assert_eq!(orders.len(), 2);
    let ours = orders.iter().find(|o| o.broker_order_id == "OOPEN1-AAAAA-BBBBBB").unwrap();
    assert_eq!(ours.tag.as_deref(), Some("ours"));
    assert_eq!(ours.status, OrderStatus::PartiallyFilled);
    let foreign = orders.iter().find(|o| o.broker_order_id == "OFORE1-AAAAA-BBBBBB").unwrap();
    assert_eq!(foreign.tag, None);
    assert_eq!(foreign.userref, None);
    assert_eq!(path_of(&t.requests()[0]), "/0/private/OpenOrders");
    t.enqueue_json(200, fixture!("open_orders_empty.json"));
    assert!(a.open_orders().unwrap().is_empty());
}

#[test]
fn find_orders_by_tag_queries_open_and_closed_by_userref_and_drops_other_userrefs() {
    let (a, t) = setup();
    let userref = a.reserve_userref("run9:BTC/USD:buy").unwrap();
    t.enqueue_json(200, fixture!("open_orders_empty.json"));
    t.enqueue_json(200, &with_userref(&a, "run9:BTC/USD:buy", fixture!("closed_orders_by_userref.json")));
    let found = a.find_orders_by_tag("run9:BTC/USD:buy").unwrap();
    // the stray userref-999 order the (hypothetically ignored) server filter returned is dropped
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].broker_order_id, "OQCLML-BW3P3-BUCMWZ");
    assert_eq!(found[0].status, OrderStatus::PartiallyFilledThenCanceled);
    let reqs = t.requests();
    assert_eq!(reqs.len(), 2);
    assert_eq!(path_of(&reqs[0]), "/0/private/OpenOrders");
    assert_eq!(path_of(&reqs[1]), "/0/private/ClosedOrders");
    for r in &reqs {
        assert_eq!(body_param(r, "userref"), Some(userref.to_string()));
        assert_validly_signed(r);
    }
    assert!(matches!(a.find_orders_by_tag("never-assigned"), Err(BrokerError::UnknownTag(_))));
}

#[test]
fn paginated_closed_orders_refuse_to_under_report() {
    let (a, t) = setup();
    let tag = "run9:ETH/USD:sell";
    t.enqueue_json(200, fixture!("open_orders_empty.json"));
    t.enqueue_json(200, &with_userref(&a, tag, fixture!("closed_orders_paginated.json")));
    assert!(matches!(a.find_orders_by_tag(tag), Err(BrokerError::Malformed(_))));
}

// ---------------------------------------------------------------- cancel

#[test]
fn cancel_order_and_cancel_and_settle_capture_the_partial_fill() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("cancel_ok.json"));
    let c = a.cancel_order("OQCLML-BW3P3-BUCMWZ").unwrap();
    assert_eq!((c.canceled_count, c.pending), (1, false));
    assert_eq!(body_param(&t.requests()[0], "txid").as_deref(), Some("OQCLML-BW3P3-BUCMWZ"));

    let (a, t) = setup();
    t.enqueue_json(200, fixture!("cancel_pending.json"));
    t.enqueue_json(200, &with_userref(&a, "tag-pc", fixture!("query_orders_canceled_partial.json")));
    let (c, report) = a.cancel_and_settle("OQCLML-BW3P3-BUCMWZ").unwrap();
    assert!(c.pending);
    assert_eq!(report.status, OrderStatus::PartiallyFilledThenCanceled);
    assert_eq!(report.executed_quantity, d("0.5"));
    let paths: Vec<String> = t.requests().iter().map(path_of).collect();
    assert_eq!(paths, ["/0/private/CancelOrder", "/0/private/QueryOrders"]);
}

#[test]
fn cancel_by_tag_sends_the_userref() {
    let (a, t) = setup();
    let userref = a.reserve_userref("cancel-me").unwrap();
    t.enqueue_json(200, fixture!("cancel_ok.json"));
    a.cancel_by_tag("cancel-me").unwrap();
    assert_eq!(body_param(&t.requests()[0], "txid"), Some(userref.to_string()));
    assert!(matches!(a.cancel_by_tag("unknown"), Err(BrokerError::UnknownTag(_))));
}

// ---------------------------------------------------------------- errors on reads

#[test]
fn exchange_errors_on_read_calls_surface_with_their_class() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("error_invalid_nonce.json"));
    match a.get_balances() {
        Err(BrokerError::Exchange(errs)) => {
            assert_eq!(errs[0].class, ErrorClass::InvalidNonce);
            assert_eq!(errs[0].code, "EAPI:Invalid nonce");
        }
        other => panic!("{other:?}"),
    }
    t.enqueue_json(503, "<html>maintenance</html>");
    assert!(matches!(a.get_balances(), Err(BrokerError::Http(503))));
}

#[test]
fn empty_error_array_is_success_and_missing_error_array_is_malformed() {
    assert!(parse_envelope(r#"{"error":[],"result":{}}"#).is_ok());
    assert!(matches!(parse_envelope(r#"{"result":{}}"#), Err(BrokerError::Malformed(_))));
    assert!(matches!(parse_envelope(r#"{"error":"oops"}"#), Err(BrokerError::Malformed(_))));
    assert!(matches!(parse_envelope(r#"{"error":[42]}"#), Err(BrokerError::Malformed(_))));
    // an entry with an unknown prefix is treated as an error, not ignored
    assert!(matches!(parse_envelope(r#"{"error":["Xsomething"],"result":{}}"#), Err(BrokerError::Exchange(_))));
}

#[test]
fn error_classification_table() {
    let cases = [
        ("EAPI:Invalid nonce", ErrorClass::InvalidNonce),
        ("EAPI:Invalid signature", ErrorClass::Auth),
        ("EGeneral:Permission denied", ErrorClass::Auth),
        ("EAPI:Rate limit exceeded", ErrorClass::RateLimited),
        ("EOrder:Rate limit exceeded", ErrorClass::RateLimited),
        ("EOrder:Insufficient funds", ErrorClass::InsufficientFunds),
        ("EGeneral:Invalid arguments:ordertype", ErrorClass::InvalidArguments),
        ("EService:Unavailable", ErrorClass::ServiceUnavailable),
        ("EService:Busy", ErrorClass::ServiceUnavailable),
        ("EGeneral:Internal error", ErrorClass::ServiceUnavailable),
        ("EOrder:Unknown order", ErrorClass::UnknownOrder),
        ("EOrder:Order minimum not met", ErrorClass::OrderRejected),
        ("ESomething:New", ErrorClass::Other),
    ];
    for (code, want) in cases {
        assert_eq!(classify(code), want, "{code}");
    }
}

// ---------------------------------------------------------------- misc

#[test]
fn adapter_debug_output_has_no_secret_material() {
    let (a, _t) = setup();
    let text = format!("{a:?}");
    assert!(!text.contains("TEST-API-KEY"));
    assert!(!text.contains(&B64.encode(SECRET_B64_SRC)));
    assert!(!text.contains("unit-test-secret"));
    assert!(text.contains("<redacted>"));
}

#[test]
fn userref_snapshot_roundtrips_into_a_new_adapter() {
    let (a, _t) = setup();
    let r1 = a.reserve_userref("tag-1").unwrap();
    let snapshot = a.userref_snapshot().to_json();
    let (b, _t2) = setup();
    let b = b.with_userref_map(broker_adapters::kraken::userref::UserrefMap::from_json(&snapshot).unwrap());
    assert_eq!(b.reserve_userref("tag-1").unwrap(), r1);
    assert_eq!(b.userref_snapshot().len(), 1);
}

// ---------------------------------------------------------------- cancel_and_settle when there is nothing to cancel

const UNKNOWN_ORDER: &str = include_str!("fixtures/error_unknown_order.json");

fn request_paths(t: &FakeTransport) -> Vec<String> {
    t.requests().iter().map(path_of).collect()
}

#[test]
fn cancel_and_settle_returns_the_filled_report_when_the_order_filled_before_the_cancel() {
    let (a, t) = setup();
    t.enqueue_json(200, UNKNOWN_ORDER); // CancelOrder: nothing cancelable, it already filled
    t.enqueue_json(200, &with_userref(&a, "tag-f", fixture!("query_orders_filled.json")));
    let (outcome, report) = a.cancel_and_settle("OABC1-XYZ23-DEF456").unwrap();
    assert_eq!((outcome.canceled_count, outcome.pending), (0, false), "nothing was canceled");
    assert_eq!(report.status, OrderStatus::Filled);
    assert_eq!(report.executed_quantity, d("0.0025"));
    assert_eq!(report.fee, Some(d("0.24494")));
    assert_eq!(report.tag.as_deref(), Some("tag-f"));
    assert_eq!(request_paths(&t), ["/0/private/CancelOrder", "/0/private/QueryOrders"]);
}

#[test]
fn cancel_and_settle_keeps_the_partial_fill_of_an_order_that_was_already_canceled() {
    let (a, t) = setup();
    t.enqueue_json(200, UNKNOWN_ORDER);
    t.enqueue_json(200, &with_userref(&a, "tag-pc", fixture!("query_orders_canceled_partial.json")));
    let (outcome, report) = a.cancel_and_settle("OQCLML-BW3P3-BUCMWZ").unwrap();
    assert_eq!(outcome.canceled_count, 0);
    assert_eq!(report.status, OrderStatus::PartiallyFilledThenCanceled);
    assert_eq!(report.executed_quantity, d("0.5"));
}

#[test]
fn cancel_and_settle_reports_a_still_open_order_honestly_when_the_cancel_was_refused() {
    let (a, t) = setup();
    t.enqueue_json(200, UNKNOWN_ORDER);
    t.enqueue_json(200, &with_userref(&a, "t", fixture!("query_orders_open_partial.json")));
    let (outcome, report) = a.cancel_and_settle("OOPEN1-AAAAA-BBBBBB").unwrap();
    assert_eq!(outcome.canceled_count, 0);
    assert_eq!(report.status, OrderStatus::PartiallyFilled, "the caller sees it is still live");
}

#[test]
fn cancel_and_settle_gives_a_specific_error_when_the_requery_finds_nothing() {
    // re-query answers with an unknown-order error
    let (a, t) = setup();
    t.enqueue_json(200, UNKNOWN_ORDER);
    t.enqueue_json(200, UNKNOWN_ORDER);
    match a.cancel_and_settle("OGONE1-AAAAA-BBBBBB") {
        Err(BrokerError::CancelTargetNotFound(id)) => assert_eq!(id, "OGONE1-AAAAA-BBBBBB"),
        other => panic!("{other:?}"),
    }
    // re-query answers with an empty result
    let (a, t) = setup();
    t.enqueue_json(200, UNKNOWN_ORDER);
    t.enqueue_json(200, r#"{"error":[],"result":{}}"#);
    assert!(matches!(a.cancel_and_settle("OGONE1-AAAAA-BBBBBB"), Err(BrokerError::CancelTargetNotFound(_))));
    assert_eq!(t.request_count(), 2);
}

#[test]
fn cancel_and_settle_does_not_swallow_other_cancel_failures_and_does_not_requery() {
    for (body, want_nonce) in [
        (fixture!("error_invalid_nonce.json"), true),
        (fixture!("error_service_unavailable.json"), false),
        (r#"{"error":["EOrder:Unknown order","EAPI:Invalid nonce"]}"#, true),
    ] {
        let (a, t) = setup();
        t.enqueue_json(200, body);
        match a.cancel_and_settle("OQCLML-BW3P3-BUCMWZ") {
            Err(BrokerError::Exchange(errs)) => {
                assert_eq!(errs.iter().any(|e| e.class == ErrorClass::InvalidNonce), want_nonce, "{body}");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(t.request_count(), 1, "no follow-up query after {body}");
    }
    let (a, t) = setup();
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(a.cancel_and_settle("OQCLML-BW3P3-BUCMWZ"), Err(BrokerError::Transport(TransportError::Timeout))));
    assert_eq!(t.request_count(), 1);
}

#[test]
fn cancel_and_settle_propagates_a_failing_or_anomalous_requery() {
    let (a, t) = setup();
    t.enqueue_json(200, UNKNOWN_ORDER);
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(a.cancel_and_settle("OX"), Err(BrokerError::Transport(TransportError::Timeout))));
    // an overfilled re-query must halt the caller, not settle
    let (a, t) = setup();
    t.enqueue_json(200, UNKNOWN_ORDER);
    let body = with_userref(&a, "t", fixture!("query_orders_filled.json")).replace("\"vol_exec\":\"0.00250000\"", "\"vol_exec\":\"0.00500000\"");
    t.enqueue_json(200, &body);
    match a.cancel_and_settle("OABC1-XYZ23-DEF456") {
        Err(BrokerError::Malformed(m)) => assert!(m.contains("overfill anomaly"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn cancel_and_settle_still_takes_the_normal_path_when_the_cancel_succeeds() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("cancel_ok.json"));
    t.enqueue_json(200, &with_userref(&a, "tag-pc", fixture!("query_orders_canceled_partial.json")));
    let (outcome, report) = a.cancel_and_settle("OQCLML-BW3P3-BUCMWZ").unwrap();
    assert_eq!(outcome.canceled_count, 1);
    assert_eq!(report.status, OrderStatus::PartiallyFilledThenCanceled);
}

// ---------------------------------------------------------------- overfill through the adapter

#[test]
fn an_overfilled_order_report_is_an_error_through_get_order() {
    let (a, t) = setup();
    let body = with_userref(&a, "t", fixture!("query_orders_filled.json")).replace("\"vol_exec\":\"0.00250000\"", "\"vol_exec\":\"0.00300000\"");
    t.enqueue_json(200, &body);
    match a.get_order("OABC1-XYZ23-DEF456") {
        Err(BrokerError::Malformed(m)) => {
            assert!(m.contains("overfill anomaly") && m.contains("OABC1-XYZ23-DEF456"), "{m}");
        }
        other => panic!("{other:?}"),
    }
}
