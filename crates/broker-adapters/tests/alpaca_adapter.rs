//! End-to-end Alpaca adapter behaviour over recorded-style JSON fixtures and `FakeTransport`.
//!
//! The fixtures in `tests/fixtures/alpaca/` are HAND-WRITTEN to Alpaca's documented response
//! shapes (and the field names in SignalEngine's real-payload tests); they are not recordings.

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::{
    AlpacaAdapter, AlpacaConfig, AlpacaCredentials, AssetInfo, Environment, OrderListFilter, PositionSide,
};
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpMethod, HttpRequest, TransportError};
use broker_adapters::types::{
    BalanceKind, BrokerAdapter, OrderKind, OrderRequest, OrderStatus, PlaceOutcome, Side, TimeInForce,
};
use broker_adapters::{BrokerError, Dec, ErrorClass};
use std::sync::Arc;

const KEY: &str = "PKTESTFIXTUREKEY0001";
const SECRET: &str = "unit-test-secret-not-a-real-key-9f3a";
const TAG: &str = "mvp1:run1:SPY:buy";
const ORDER_ID: &str = "61e69015-8549-4bfd-b9c3-01e75843f47d";

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!("fixtures/alpaca/", $name))
    };
}

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

fn setup_with(f: impl FnOnce(&mut AlpacaConfig)) -> (AlpacaAdapter, Arc<FakeTransport>) {
    let t = Arc::new(FakeTransport::new());
    let mut cfg = AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).unwrap();
    f(&mut cfg);
    let creds = AlpacaCredentials::new(Environment::Paper, KEY, SECRET).unwrap();
    (AlpacaAdapter::new(cfg, creds, t.clone()).unwrap(), t)
}

fn setup() -> (AlpacaAdapter, Arc<FakeTransport>) {
    setup_with(|_| {})
}

/// `"Get /v2/account"`-style summary, host stripped.
fn line(r: &HttpRequest) -> String {
    format!("{:?} {}", r.method, r.url.strip_prefix(PAPER_BASE_URL).expect("request went to the paper host"))
}

fn lines(t: &FakeTransport) -> Vec<String> {
    t.requests().iter().map(line).collect()
}

fn body_json(r: &HttpRequest) -> serde_json::Value {
    serde_json::from_str(r.body.as_ref().expect("request has a body")).unwrap()
}

fn market_req() -> OrderRequest {
    OrderRequest::market(TAG, "SPY", Side::Buy, d("1.5"))
}

/// Script account + clock (both fine) for the pre-order checks of a market order.
fn preflight_ok(t: &FakeTransport) {
    t.enqueue_json(200, fixture!("account_ok.json"));
    t.enqueue_json(200, fixture!("clock_open.json"));
}

/// Place the standard market order with a scripted answer to the POST.
fn place_with(status: u16, body: &str) -> (Result<PlaceOutcome, BrokerError>, Arc<FakeTransport>) {
    let (a, t) = setup();
    preflight_ok(&t);
    t.enqueue_json(status, body);
    (a.place_order(&market_req()), t)
}

// ---------------------------------------------------------------- reads

#[test]
fn account_is_parsed_exactly() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_ok.json"));
    let acct = a.get_account().unwrap();
    assert_eq!(acct.cash, d("52840.17"));
    assert_eq!(acct.equity, d("100210.55"));
    assert_eq!(acct.buying_power, d("152840.17"));
    assert_eq!(acct.status, "ACTIVE");
    assert!(!acct.trading_blocked && !acct.account_blocked && !acct.pattern_day_trader);
    assert_eq!(acct.account_number.as_deref(), Some("PA3TESTFIXT1"));
    assert_eq!(acct.blocked_reason(), None);
    assert_eq!(lines(&t), ["Get /v2/account"]);
    let r = &t.requests()[0];
    assert_eq!(r.header("APCA-API-KEY-ID"), Some(KEY));
    assert_eq!(r.header("APCA-API-SECRET-KEY"), Some(SECRET));
    assert_eq!(r.header("Accept"), Some("application/json"));
    assert!(r.body.is_none());
}

#[test]
fn pattern_day_trader_is_reported_but_does_not_block() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_pdt.json"));
    let acct = a.verify_account().unwrap();
    assert!(acct.pattern_day_trader);
}

#[test]
fn positions_are_parsed_with_exact_fractional_quantities_and_sides() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("positions_ok.json"));
    let p = a.get_positions().unwrap();
    assert_eq!(p.len(), 3);
    assert_eq!(p[0].symbol, "SPY");
    assert_eq!(p[0].qty, d("12.345678901"));
    assert_eq!(p[0].avg_entry_price, d("512.10"));
    assert_eq!(p[0].market_value, d("6321.55"));
    assert_eq!(p[0].side, PositionSide::Long);
    assert_eq!(lines(&t), ["Get /v2/positions"]);

    t.enqueue_json(200, fixture!("positions_short.json"));
    let s = a.get_positions().unwrap();
    assert_eq!(s[0].side, PositionSide::Short);
    assert_eq!(s[0].signed_qty(), d("-10"));

    t.enqueue_json(200, fixture!("positions_empty.json"));
    assert!(a.get_positions().unwrap().is_empty());
}

#[test]
fn balances_are_cash_plus_one_spot_entry_per_position() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_ok.json"));
    t.enqueue_json(200, fixture!("positions_ok.json"));
    let b = a.get_balances().unwrap();
    assert_eq!(b.spot("USD"), d("52840.17"));
    assert_eq!(b.spot("SPY"), d("12.345678901"));
    assert_eq!(b.spot("EFA"), d("40"));
    assert_eq!(b.spot("IEF"), d("1.5"));
    assert!(b.entries.iter().all(|e| e.kind == BalanceKind::Spot));
    assert_eq!(lines(&t), ["Get /v2/account", "Get /v2/positions"]);
}

#[test]
fn clock_and_calendar_are_parsed() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("clock_open.json"));
    let c = a.get_clock().unwrap();
    assert!(c.is_open);
    assert!((c.timestamp_epoch - 1_789_998_312.345678).abs() < 1e-3, "{}", c.timestamp_epoch);
    assert_eq!(c.next_close_epoch, 1_790_020_800.0);
    t.enqueue_json(200, fixture!("clock_closed.json"));
    let c = a.get_clock().unwrap();
    assert!(!c.is_open);
    assert_eq!(c.next_open, "2026-09-21T09:30:00-04:00");
    assert_eq!(c.next_open_epoch, 1_789_997_400.0);

    t.enqueue_json(200, fixture!("calendar_ok.json"));
    let days = a.get_calendar("2026-11-25", "2026-11-30").unwrap();
    assert_eq!(days.len(), 3);
    assert_eq!(days[1].date, "2026-11-27");
    assert_eq!(days[1].close, "13:00", "early close day");
    assert_eq!(t.requests()[2].url, "https://paper-api.alpaca.markets/v2/calendar?start=2026-11-25&end=2026-11-30");
}

#[test]
fn bad_calendar_dates_are_refused_without_a_request() {
    let (a, t) = setup();
    for (s, e) in [("2026-13-01", "2026-12-01"), ("2026-1-1", "2026-12-01"), ("2026-12-01", "2026-11-01"), ("x", "y")] {
        assert!(matches!(a.get_calendar(s, e), Err(BrokerError::InvalidRequest(_))), "{s} {e}");
    }
    assert_eq!(t.request_count(), 0);
}

#[test]
fn malformed_reads_fail_closed() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_missing_blocked_flag.json"));
    assert!(matches!(a.get_account(), Err(BrokerError::Malformed(_))));
    t.enqueue_json(200, "not json");
    assert!(matches!(a.get_positions(), Err(BrokerError::Malformed(_))));
    t.enqueue_json(200, r#"{"not":"an array"}"#);
    assert!(matches!(a.get_positions(), Err(BrokerError::Malformed(_))));
    t.enqueue_json(200, r#"[{"symbol":"SPY","side":"long","avg_entry_price":"1","market_value":"1"}]"#);
    assert!(matches!(a.get_positions(), Err(BrokerError::Malformed(_))), "position without qty");
}

// ---------------------------------------------------------------- assets

#[test]
fn refresh_asset_loads_the_row_and_changes_rounding() {
    let (a, t) = setup();
    assert_eq!(a.asset("BRK.A"), None);
    t.enqueue_json(200, fixture!("asset_whole_only.json"));
    let info: AssetInfo = a.refresh_asset("brk.a").unwrap();
    assert!(!info.fractionable);
    assert_eq!(lines(&t), ["Get /v2/assets/BRK.A"]);
    // now a fractional request is floored to whole shares on the wire
    preflight_ok(&t);
    t.enqueue_json(200, fixture!("order_accepted_market.json"));
    let out = a.place_order(&OrderRequest::market(TAG, "BRK.A", Side::Buy, d("3.9"))).unwrap();
    assert!(matches!(out, PlaceOutcome::Accepted { .. }));
    let post = &t.requests()[3];
    assert_eq!(body_json(post)["qty"], "3");
    assert_eq!(body_json(post)["symbol"], "BRK.A");
}

#[test]
fn refresh_replaces_a_builtin_row_and_unknown_symbols_are_refused_locally() {
    let (a, t) = setup();
    assert_eq!(a.asset("SPY").unwrap().source, broker_adapters::alpaca::AssetSource::Builtin);
    t.enqueue_json(200, fixture!("asset_spy.json"));
    a.refresh_asset("SPY").unwrap();
    assert_eq!(a.asset("SPY").unwrap().source, broker_adapters::alpaca::AssetSource::Api);

    // AAPL is not in the built-in table: refused, nothing sent
    let n = t.request_count();
    match a.place_order(&OrderRequest::market(TAG, "AAPL", Side::Buy, d("1"))) {
        Err(BrokerError::UnknownSymbol(s)) => assert_eq!(s, "AAPL"),
        other => panic!("{other:?}"),
    }
    assert_eq!(t.request_count(), n);
}

#[test]
fn refresh_asset_404_is_unknown_symbol_and_wrong_symbol_is_malformed() {
    let (a, t) = setup();
    t.enqueue_json(404, r#"{"code":40410000,"message":"asset not found for ZZZZ"}"#);
    assert!(matches!(a.refresh_asset("ZZZZ"), Err(BrokerError::UnknownSymbol(_))));
    t.enqueue_json(200, fixture!("asset_spy.json"));
    assert!(matches!(a.refresh_asset("EFA"), Err(BrokerError::Malformed(_))));
    assert!(a.asset("EFA").unwrap().source == broker_adapters::alpaca::AssetSource::Builtin);
}

// ---------------------------------------------------------------- placement

#[test]
fn market_order_runs_the_preflight_then_posts_the_exact_body() {
    let (a, t) = setup();
    preflight_ok(&t);
    t.enqueue_json(200, fixture!("order_accepted_market.json"));
    match a.place_order(&market_req()).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, sent, description, warnings } => {
            assert_eq!(broker_order_id, ORDER_ID);
            assert_eq!(sent.quantity, d("1.5"));
            assert_eq!(sent.broker_pair, "SPY");
            assert_eq!(sent.price, None);
            assert!(!sent.validate_only);
            assert_eq!(description.as_deref(), Some("buy 1.5 SPY market day"));
            assert!(warnings.is_empty());
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(lines(&t), ["Get /v2/account", "Get /v2/clock", "Post /v2/orders"]);
    let post = &t.requests()[2];
    assert_eq!(post.method, HttpMethod::Post);
    assert_eq!(post.header("Content-Type"), Some("application/json"));
    assert_eq!(post.header("APCA-API-KEY-ID"), Some(KEY));
    let j = body_json(post);
    assert_eq!(
        j,
        serde_json::json!({
            "symbol": "SPY", "qty": "1.5", "side": "buy", "type": "market",
            "time_in_force": "day", "client_order_id": TAG
        })
    );
}

#[test]
fn limit_order_skips_the_clock_and_sends_price_and_gtc() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_ok.json"));
    t.enqueue_json(200, fixture!("order_accepted_limit.json"));
    let mut req = OrderRequest::limit(TAG, "SPY", Side::Buy, d("10"), d("512.109"));
    req.time_in_force = Some(TimeInForce::Gtc);
    match a.place_order(&req).unwrap() {
        PlaceOutcome::Accepted { sent, .. } => assert_eq!(sent.price, Some(d("512.1"))),
        other => panic!("{other:?}"),
    }
    assert_eq!(lines(&t), ["Get /v2/account", "Post /v2/orders"]);
    let j = body_json(&t.requests()[1]);
    assert_eq!(j["type"], "limit");
    assert_eq!(j["limit_price"], "512.1");
    assert_eq!(j["time_in_force"], "gtc");
    assert_eq!(j["qty"], "10");
}

#[test]
fn quantity_is_rounded_down_on_the_wire_and_never_up() {
    let (a, t) = setup();
    preflight_ok(&t);
    t.enqueue_json(200, fixture!("order_accepted_market.json"));
    let req = OrderRequest::market(TAG, "SPY", Side::Buy, d("1.2345678999"));
    match a.place_order(&req).unwrap() {
        PlaceOutcome::Accepted { sent, .. } => assert_eq!(sent.quantity, d("1.234567899")),
        other => panic!("{other:?}"),
    }
    assert_eq!(body_json(&t.requests()[2])["qty"], "1.234567899");
}

#[test]
fn a_200_with_status_rejected_is_a_definite_rejection() {
    let (out, _t) = place_with(200, fixture!("order_rejected.json"));
    match out.unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert_eq!(errors[0].class, ErrorClass::OrderRejected),
        other => panic!("{other:?}"),
    }
}

fn rejected_errors(status: u16, body: &str) -> Vec<(ErrorClass, String)> {
    match place_with(status, body).0.unwrap() {
        PlaceOutcome::Rejected { errors, sent } => {
            assert_eq!(sent.quantity, d("1.5"));
            errors.into_iter().map(|e| (e.class, e.code)).collect()
        }
        other => panic!("expected Rejected for HTTP {status}, got {other:?}"),
    }
}

#[test]
fn definite_rejections_carry_the_class_and_the_broker_message() {
    let r = rejected_errors(422, fixture!("error_422_not_fractionable.json"));
    assert_eq!(r[0].0, ErrorClass::InvalidArguments);
    assert!(r[0].1.contains("is not fractionable") && r[0].1.contains("422") && r[0].1.contains("42210000"), "{}", r[0].1);

    let r = rejected_errors(422, fixture!("error_422_qty_invalid.json"));
    assert_eq!(r[0].0, ErrorClass::InvalidArguments);

    let r = rejected_errors(422, fixture!("error_422_insufficient_qty.json"));
    assert_eq!(r[0].0, ErrorClass::InsufficientFunds);

    // Alpaca answers insufficient buying power with HTTP 403 (FROM-MEMORY-OF-DOCS): not an auth error
    let r = rejected_errors(403, fixture!("error_403_buying_power.json"));
    assert_eq!(r[0].0, ErrorClass::InsufficientFunds);
    assert!(r[0].1.contains("insufficient buying power"));

    let r = rejected_errors(403, fixture!("error_403_pdt.json"));
    assert_eq!(r[0].0, ErrorClass::OrderRejected);
}

#[test]
fn auth_failures_are_auth_class_and_definite() {
    let r = rejected_errors(401, fixture!("error_401_unauthorized.json"));
    assert_eq!(r[0].0, ErrorClass::Auth);
    let r = rejected_errors(403, fixture!("error_403_forbidden.json"));
    assert_eq!(r[0].0, ErrorClass::Auth);
    assert!(r[0].1.contains("forbidden"));
}

#[test]
fn rate_limit_on_placement_is_a_retryable_error_carrying_retry_after() {
    let (a, t) = setup();
    preflight_ok(&t);
    t.enqueue_json_with_headers(429, fixture!("error_429.json"), &[("Retry-After", "30")]);
    match a.place_order(&market_req()) {
        Err(BrokerError::RateLimited { retry_after_secs, message }) => {
            assert_eq!(retry_after_secs, Some(30));
            assert_eq!(message, "rate limit exceeded");
        }
        other => panic!("{other:?}"),
    }
    // header names are case-insensitive, and a missing header is None
    let (a, t) = setup();
    preflight_ok(&t);
    t.enqueue_json_with_headers(429, fixture!("error_429.json"), &[("retry-after", "2")]);
    assert!(matches!(a.place_order(&market_req()), Err(BrokerError::RateLimited { retry_after_secs: Some(2), .. })));
    let (a, t) = setup();
    preflight_ok(&t);
    t.enqueue_json(429, fixture!("error_429.json"));
    assert!(matches!(a.place_order(&market_req()), Err(BrokerError::RateLimited { retry_after_secs: None, .. })));
}

#[test]
fn rate_limit_on_reads_carries_retry_after_and_displays_it() {
    let (a, t) = setup();
    t.enqueue_json_with_headers(429, fixture!("error_429.json"), &[("Retry-After", "7")]);
    let e = a.get_positions().unwrap_err();
    assert!(matches!(e, BrokerError::RateLimited { retry_after_secs: Some(7), .. }));
    assert!(e.to_string().contains("retry after 7s"), "{e}");
}

#[test]
fn ambiguous_failures_on_placement_are_unknown_outcome_never_rejected() {
    let unknown = |out: Result<PlaceOutcome, BrokerError>| match out.unwrap() {
        PlaceOutcome::UnknownOutcome { reason, sent } => {
            assert_eq!(sent.quantity, d("1.5"));
            reason
        }
        other => panic!("expected UnknownOutcome, got {other:?}"),
    };
    // 5xx with a JSON body, 5xx with an HTML page
    assert!(unknown(place_with(500, fixture!("error_500.json")).0).contains("500"));
    assert!(unknown(place_with(502, "<html>Bad Gateway</html>").0).contains("502"));
    assert!(unknown(place_with(504, "").0).contains("504"));
    // 200 with garbage, or with an order we cannot use
    assert!(unknown(place_with(200, "not json at all").0).contains("not JSON"));
    assert!(unknown(place_with(200, "{}").0).contains("unusable"));
    assert!(unknown(place_with(200, fixture!("order_unknown_status.json")).0).contains("unusable"));
    // 200 but the order carries someone else's client_order_id, or none
    assert!(unknown(place_with(200, fixture!("order_wrong_client_id.json")).0).contains("different client_order_id"));
    assert!(unknown(place_with(200, fixture!("order_no_client_id.json")).0).contains("different client_order_id"));
    // an unexpected status we have no rule for
    assert!(unknown(place_with(418, "teapot").0).contains("418"));
}

#[test]
fn timeouts_and_io_errors_on_the_post_are_unknown_outcome() {
    for err in [TransportError::Timeout, TransportError::Io("connection reset by peer".into())] {
        let (a, t) = setup();
        preflight_ok(&t);
        t.enqueue_error(err);
        assert!(matches!(a.place_order(&market_req()).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
    }
}

#[test]
fn connect_failure_on_the_post_is_a_definite_not_sent_error() {
    let (a, t) = setup();
    preflight_ok(&t);
    t.enqueue_error(TransportError::ConnectFailed("dns".into()));
    match a.place_order(&market_req()) {
        Err(BrokerError::Transport(e)) => assert!(e.request_definitely_not_sent()),
        other => panic!("{other:?}"),
    }
}

#[test]
fn local_refusals_send_nothing() {
    let (a, t) = setup();
    let cases: Vec<OrderRequest> = vec![
        OrderRequest::market(TAG, "BTC/USD", Side::Buy, d("1")),
        OrderRequest::market(TAG, "SPY", Side::Buy, d("0.0000000001")),
        OrderRequest::market("", "SPY", Side::Buy, d("1")),
        OrderRequest::market(&"x".repeat(129), "SPY", Side::Buy, d("1")),
        {
            let mut r = market_req();
            r.validate_only = true;
            r
        },
        {
            let mut r = market_req();
            r.reduce_only = true;
            r
        },
    ];
    for c in cases {
        assert!(a.place_order(&c).is_err(), "{c:?}");
    }
    assert_eq!(t.request_count(), 0);
}

#[test]
fn own_tag_prefix_refuses_foreign_tags_locally() {
    let (a, t) = setup_with(|c| c.own_tag_prefix = Some("mvp1:".into()));
    let req = OrderRequest::market("someone:else", "SPY", Side::Buy, d("1"));
    assert!(matches!(a.place_order(&req), Err(BrokerError::InvalidRequest(_))));
    assert_eq!(t.request_count(), 0);
}

// ---------------------------------------------------------------- idempotency after an unknown outcome

#[test]
fn lookup_by_client_order_id_after_a_timeout_finds_the_order_that_did_arrive() {
    let (a, t) = setup();
    preflight_ok(&t);
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(a.place_order(&market_req()).unwrap(), PlaceOutcome::UnknownOutcome { .. }));

    // the order DID reach Alpaca and filled
    t.enqueue_json(200, fixture!("order_filled.json"));
    let found = a.find_orders_by_tag(TAG).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].broker_order_id, ORDER_ID);
    assert_eq!(found[0].tag.as_deref(), Some(TAG));
    assert_eq!(found[0].status, OrderStatus::Filled);
    assert_eq!(found[0].executed_quantity, d("1.5"));
    assert_eq!(found[0].avg_price, Some(d("154.03")));
    assert_eq!(found[0].cost, Some(d("231.045")));
    let lookup = t.requests().last().unwrap().clone();
    assert_eq!(lookup.method, HttpMethod::Get);
    assert_eq!(
        lookup.url,
        "https://paper-api.alpaca.markets/v2/orders:by_client_order_id?client_order_id=mvp1%3Arun1%3ASPY%3Abuy"
    );
}

#[test]
fn lookup_by_client_order_id_after_a_timeout_reports_none_when_it_never_arrived() {
    let (a, t) = setup();
    t.enqueue_json(404, fixture!("error_404_order.json"));
    assert!(a.find_orders_by_tag(TAG).unwrap().is_empty());
    t.enqueue_json(404, fixture!("error_404_order.json"));
    assert!(a.get_order_by_tag(TAG).unwrap().is_none());
}

#[test]
fn lookup_never_trusts_a_response_for_a_different_tag_and_reports_real_errors() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("order_wrong_client_id.json"));
    assert!(matches!(a.find_orders_by_tag(TAG), Err(BrokerError::Malformed(_))));
    t.enqueue_json(500, fixture!("error_500.json"));
    assert!(matches!(a.find_orders_by_tag(TAG), Err(BrokerError::Http(500))));
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(a.find_orders_by_tag(TAG), Err(BrokerError::Transport(TransportError::Timeout))));
    assert!(matches!(a.find_orders_by_tag(""), Err(BrokerError::InvalidRequest(_))));
}

#[test]
fn resending_a_tag_that_already_exists_is_reported_as_unknown_never_as_a_fresh_rejection() {
    let (a, t) = setup();
    preflight_ok(&t);
    t.enqueue_json(422, fixture!("error_422_duplicate_client_order_id.json"));
    match a.place_order(&market_req()).unwrap() {
        PlaceOutcome::UnknownOutcome { reason, .. } => {
            assert!(reason.contains("already exists") && reason.contains("look it up by tag"), "{reason}");
        }
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------- order state

fn get_one(fixture_body: &str) -> Result<broker_adapters::OrderReport, BrokerError> {
    let (a, t) = setup();
    t.enqueue_json(200, fixture_body);
    let r = a.get_order(ORDER_ID);
    assert_eq!(lines(&t), [format!("Get /v2/orders/{ORDER_ID}")]);
    r
}

#[test]
fn every_status_maps_to_the_documented_neutral_status() {
    let cases: [(&str, OrderStatus, &str, &str); 11] = [
        (fixture!("order_pending_new.json"), OrderStatus::Pending, "pending_new", "0"),
        (fixture!("order_accepted_market.json"), OrderStatus::Pending, "accepted", "0"),
        (fixture!("order_accepted_limit.json"), OrderStatus::Open, "new", "0"),
        (fixture!("order_partially_filled.json"), OrderStatus::PartiallyFilled, "partially_filled", "4"),
        (fixture!("order_filled.json"), OrderStatus::Filled, "filled", "1.5"),
        (fixture!("order_canceled_partial.json"), OrderStatus::PartiallyFilledThenCanceled, "canceled", "4"),
        (fixture!("order_expired_partial.json"), OrderStatus::PartiallyFilledThenExpired, "expired", "2.5"),
        (fixture!("order_done_for_day_partial.json"), OrderStatus::PartiallyFilledThenExpired, "done_for_day", "3"),
        (fixture!("order_canceled_zero.json"), OrderStatus::Canceled, "canceled", "0"),
        (fixture!("order_expired_zero.json"), OrderStatus::Expired, "expired", "0"),
        (fixture!("order_rejected.json"), OrderStatus::Rejected, "rejected", "0"),
    ];
    for (body, want, raw, executed) in cases {
        let r = get_one(body).unwrap();
        assert_eq!(r.status, want, "{raw}");
        assert_eq!(r.raw_status, raw);
        assert_eq!(r.executed_quantity, d(executed), "{raw}");
        assert_eq!(r.executed_quantity.is_positive(), want.has_fills(), "{raw}");
    }
}

#[test]
fn partial_fill_then_cancel_keeps_the_executed_quantity_and_price() {
    let r = get_one(fixture!("order_canceled_partial.json")).unwrap();
    assert_eq!(r.status, OrderStatus::PartiallyFilledThenCanceled);
    assert!(r.status.is_terminal() && r.status.has_fills());
    assert_eq!(r.quantity, d("10"));
    assert_eq!(r.executed_quantity, d("4"));
    assert_eq!(r.avg_price, Some(d("512.30")));
    assert_eq!(r.cost, Some(d("2049.2")));
    assert_eq!(r.fee, None);
    assert_eq!(r.symbol, "SPY");
    assert_eq!(r.side, Some(Side::Buy));
    assert_eq!(r.kind, Some(OrderKind::Limit { price: d("512.40") }));
    assert_eq!(r.tag.as_deref(), Some(TAG));
    assert_eq!(r.userref, None);
    // close time is the cancel time (not the earlier partial-fill time)
    assert_eq!(r.close_time, Some(1_789_998_000.0));
    assert!((r.open_time.unwrap() - 1_789_997_465.12).abs() < 1e-3);
}

#[test]
fn filled_market_order_details_and_nothing_executed_has_no_price() {
    let r = get_one(fixture!("order_filled.json")).unwrap();
    assert_eq!(r.kind, Some(OrderKind::Market));
    assert_eq!(r.avg_price, Some(d("154.03")));
    assert_eq!(r.cost, Some(d("231.045")));
    assert!(r.close_time.is_some());
    let r = get_one(fixture!("order_pending_new.json")).unwrap();
    assert_eq!(r.avg_price, None);
    assert_eq!(r.cost, None);
    assert_eq!(r.close_time, None, "an open order has no close time");
    let r = get_one(fixture!("order_rejected.json")).unwrap();
    assert!(r.reason.as_deref().unwrap().contains("rejected"));
}

#[test]
fn notional_orders_have_no_requested_quantity() {
    let r = get_one(fixture!("order_notional_filled.json")).unwrap();
    assert_eq!(r.status, OrderStatus::Filled);
    assert_eq!(r.executed_quantity, d("0.6493"));
    assert_eq!(r.quantity, d("0.6493"), "documented: quantity mirrors filled_qty when qty is null");
}

#[test]
fn missing_filled_qty_or_unknown_status_fail_closed() {
    assert!(matches!(get_one(fixture!("order_missing_filled_qty.json")), Err(BrokerError::Malformed(_))));
    assert!(matches!(get_one(fixture!("order_unknown_status.json")), Err(BrokerError::Malformed(_))));
    assert!(matches!(get_one("{}"), Err(BrokerError::Malformed(_))));
}

#[test]
fn get_order_validates_the_id_and_the_response() {
    let (a, t) = setup();
    for bad in ["", "../account", "abc/def", "id?x=1", "a b"] {
        assert!(matches!(a.get_order(bad), Err(BrokerError::InvalidRequest(_))), "{bad:?}");
        assert!(matches!(a.cancel_order(bad), Err(BrokerError::InvalidRequest(_))), "{bad:?}");
    }
    assert_eq!(t.request_count(), 0);
    // a response for a different order id is refused
    t.enqueue_json(200, fixture!("order_filled.json"));
    assert!(matches!(a.get_order("aaaaaaaa-0000-4000-8000-000000000000"), Err(BrokerError::Malformed(_))));
    t.enqueue_json(404, fixture!("error_404_order.json"));
    assert!(matches!(a.get_order(ORDER_ID), Err(BrokerError::NotFound(_))));
}

#[test]
fn open_orders_lists_ours_and_foreign_and_uses_the_status_filter() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("orders_open.json"));
    let o = a.open_orders().unwrap();
    assert_eq!(o.len(), 2);
    assert_eq!(o[0].status, OrderStatus::Open);
    assert_eq!(o[1].status, OrderStatus::PartiallyFilled);
    assert_eq!(o[1].executed_quantity, d("2"));
    assert_eq!(lines(&t), ["Get /v2/orders?status=open&limit=500&direction=desc&nested=false"]);
    // without a configured prefix every client_order_id is reported as the tag
    assert!(o[0].tag.is_some() && o[1].tag.is_some());

    // with a prefix, the order that does not carry it is foreign (no tag)
    let (a, t) = setup_with(|c| c.own_tag_prefix = Some("mvp1:".into()));
    t.enqueue_json(200, fixture!("orders_open.json"));
    let o = a.open_orders().unwrap();
    assert_eq!(o[0].tag.as_deref(), Some(TAG));
    assert_eq!(o[1].tag, None);

    t.enqueue_json(200, fixture!("orders_empty.json"));
    assert!(a.open_orders().unwrap().is_empty());
}

#[test]
fn a_full_page_is_refused_rather_than_under_reported() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("orders_open.json")); // two orders
    assert!(matches!(a.list_orders(OrderListFilter::Closed, 2, &[]), Err(BrokerError::Malformed(_))));
    assert_eq!(lines(&t), ["Get /v2/orders?status=closed&limit=2&direction=desc&nested=false"]);
    t.enqueue_json(200, fixture!("orders_open.json"));
    let r = a.list_orders(OrderListFilter::All, 3, &["spy", "EFA"]).unwrap();
    assert_eq!(r.len(), 2);
    assert!(t.requests().last().unwrap().url.ends_with("&symbols=SPY%2CEFA"));
    assert!(matches!(a.list_orders(OrderListFilter::All, 0, &[]), Err(BrokerError::InvalidRequest(_))));
    assert!(matches!(a.list_orders(OrderListFilter::All, 501, &[]), Err(BrokerError::InvalidRequest(_))));
}

// ---------------------------------------------------------------- cancel

#[test]
fn cancel_uses_delete_not_post_and_reports_pending() {
    let (a, t) = setup();
    t.enqueue_json(204, "");
    let c = a.cancel_order(ORDER_ID).unwrap();
    assert_eq!((c.canceled_count, c.pending), (1, true));
    let r = &t.requests()[0];
    assert_eq!(r.method, HttpMethod::Delete);
    assert_eq!(line(r), format!("Delete /v2/orders/{ORDER_ID}"));
    assert!(r.body.is_none());
}

#[test]
fn cancel_errors_are_specific() {
    let (a, t) = setup();
    t.enqueue_json(404, fixture!("error_404_order.json"));
    assert!(matches!(a.cancel_order(ORDER_ID), Err(BrokerError::NotFound(_))));
    // already filled / canceled: 422, a definite refusal (follow with get_order)
    t.enqueue_json(422, fixture!("error_422_not_cancelable.json"));
    match a.cancel_order(ORDER_ID) {
        Err(BrokerError::Exchange(e)) => {
            assert_eq!(e[0].class, ErrorClass::OrderRejected);
            assert!(e[0].code.contains("not cancelable"));
        }
        other => panic!("{other:?}"),
    }
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(a.cancel_order(ORDER_ID), Err(BrokerError::Transport(TransportError::Timeout))));
}

#[test]
fn cancel_and_settle_captures_the_partial_fill_that_happened_before_the_cancel() {
    let (a, t) = setup();
    t.enqueue_json(204, "");
    t.enqueue_json(200, fixture!("order_canceled_partial.json"));
    let (c, report) = a.cancel_and_settle(ORDER_ID).unwrap();
    assert!(c.pending);
    assert_eq!(report.status, OrderStatus::PartiallyFilledThenCanceled);
    assert_eq!(report.executed_quantity, d("4"));
    assert_eq!(report.avg_price, Some(d("512.30")));
    assert_eq!(lines(&t), [format!("Delete /v2/orders/{ORDER_ID}"), format!("Get /v2/orders/{ORDER_ID}")]);
}

// ---------------------------------------------------------------- pre-order checks

#[test]
fn blocked_or_inactive_accounts_are_refused_before_any_order_is_sent() {
    for (fx, needle) in [
        (fixture!("account_trading_blocked.json"), "trading_blocked=true"),
        (fixture!("account_account_blocked.json"), "account_blocked=true"),
        (fixture!("account_not_active.json"), "status=ACCOUNT_UPDATED"),
    ] {
        let (a, t) = setup();
        t.enqueue_json(200, fx);
        match a.place_order(&market_req()) {
            Err(BrokerError::AccountBlocked(why)) => assert!(why.contains(needle), "{why}"),
            other => panic!("{needle}: {other:?}"),
        }
        assert_eq!(lines(&t), ["Get /v2/account"], "no clock read, no POST");
    }
    // limit orders are refused too
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_trading_blocked.json"));
    let lim = OrderRequest::limit(TAG, "SPY", Side::Buy, d("2"), d("500"));
    assert!(matches!(a.place_order(&lim), Err(BrokerError::AccountBlocked(_))));
    assert_eq!(t.request_count(), 1);
}

#[test]
fn a_market_order_while_the_market_is_closed_returns_a_specific_wait_error() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_ok.json"));
    t.enqueue_json(200, fixture!("clock_closed.json"));
    match a.place_order(&market_req()) {
        Err(BrokerError::MarketClosed { next_open, next_close }) => {
            assert_eq!(next_open, "2026-09-21T09:30:00-04:00");
            assert_eq!(next_close, "2026-09-21T16:00:00-04:00");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(lines(&t), ["Get /v2/account", "Get /v2/clock"], "nothing was POSTed");
}

#[test]
fn allow_extended_hours_skips_the_closed_market_refusal() {
    let (a, t) = setup_with(|c| c.allow_extended_hours = true);
    t.enqueue_json(200, fixture!("account_ok.json"));
    t.enqueue_json(200, fixture!("order_accepted_market.json"));
    assert!(matches!(a.place_order(&market_req()).unwrap(), PlaceOutcome::Accepted { .. }));
    assert_eq!(lines(&t), ["Get /v2/account", "Post /v2/orders"], "no clock read");
    // market orders are never sent with extended_hours
    assert!(body_json(&t.requests()[1]).get("extended_hours").is_none());

    // a limit order gets the flag
    t.enqueue_json(200, fixture!("account_ok.json"));
    t.enqueue_json(200, fixture!("order_accepted_limit.json"));
    let lim = OrderRequest::limit(TAG, "SPY", Side::Buy, d("10"), d("512.10"));
    a.place_order(&lim).unwrap();
    assert_eq!(body_json(t.requests().last().unwrap())["extended_hours"], true);
}

#[test]
fn a_limit_order_is_not_refused_when_the_market_is_closed() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_ok.json"));
    t.enqueue_json(200, fixture!("order_accepted_limit.json"));
    let lim = OrderRequest::limit(TAG, "SPY", Side::Buy, d("10"), d("512.10"));
    assert!(matches!(a.place_order(&lim).unwrap(), PlaceOutcome::Accepted { .. }));
    assert_eq!(lines(&t), ["Get /v2/account", "Post /v2/orders"]);
}

#[test]
fn preflight_failures_are_errors_that_mean_nothing_was_sent() {
    // timeout reading the account: a Preflight error, NOT Transport(Timeout) (which would look like an unknown outcome)
    let (a, t) = setup();
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(a.place_order(&market_req()), Err(BrokerError::Preflight(_))));
    assert_eq!(t.request_count(), 1);
    // 5xx reading the clock
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_ok.json"));
    t.enqueue_json(503, "<html>maintenance</html>");
    assert!(matches!(a.place_order(&market_req()), Err(BrokerError::Preflight(_))));
    assert_eq!(t.request_count(), 2);
    // an account response we cannot read fails closed
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("account_missing_blocked_flag.json"));
    assert!(matches!(a.place_order(&market_req()), Err(BrokerError::Preflight(_))));
    // auth failure while checking is reported as auth
    let (a, t) = setup();
    t.enqueue_json(401, fixture!("error_401_unauthorized.json"));
    match a.place_order(&market_req()) {
        Err(BrokerError::Exchange(e)) => assert_eq!(e[0].class, ErrorClass::Auth),
        other => panic!("{other:?}"),
    }
    // rate limit while checking
    let (a, t) = setup();
    t.enqueue_json_with_headers(429, "{}", &[("Retry-After", "9")]);
    assert!(matches!(a.place_order(&market_req()), Err(BrokerError::RateLimited { retry_after_secs: Some(9), .. })));
}

#[test]
fn quotes_are_explicitly_unsupported() {
    let (a, t) = setup();
    assert!(matches!(a.get_quote("SPY"), Err(BrokerError::Unsupported(_))));
    assert_eq!(t.request_count(), 0);
    assert_eq!(a.broker_name(), "alpaca");
}

// ---------------------------------------------------------------- flatten

#[test]
fn close_position_deletes_the_symbol_and_returns_the_closing_order() {
    let (a, t) = setup();
    t.enqueue_json(200, fixture!("order_close_position.json"));
    let r = a.close_position("spy").unwrap().unwrap();
    assert_eq!(r.side, Some(Side::Sell));
    assert_eq!(r.quantity, d("12.345678901"));
    assert_eq!(r.status, OrderStatus::Pending);
    assert_eq!(lines(&t), ["Delete /v2/positions/SPY"]);
    assert_eq!(t.requests()[0].method, HttpMethod::Delete);

    t.enqueue_json(404, fixture!("error_404_position.json"));
    assert!(a.close_position("EFA").unwrap().is_none(), "no position: already flat");
    t.enqueue_json(403, fixture!("error_403_buying_power.json").replace("insufficient buying power", "insufficient qty available for order").as_str());
    assert!(matches!(a.close_position("SPY"), Err(BrokerError::Exchange(_))));
    assert!(matches!(a.close_position("BTC/USD"), Err(BrokerError::Unsupported(_))));
}

#[test]
fn flatten_all_cancels_orders_and_reports_each_position() {
    let (a, t) = setup();
    t.enqueue_json(207, fixture!("flatten_ok_207.json"));
    let rep = a.flatten_all().unwrap();
    assert!(rep.all_ok());
    assert_eq!(rep.entries.len(), 2);
    assert_eq!(rep.entries[0].symbol, "SPY");
    assert_eq!(rep.entries[0].order.as_ref().unwrap().quantity, d("12.345678901"));
    assert_eq!(rep.entries[1].order.as_ref().unwrap().quantity, d("40"));
    let r = &t.requests()[0];
    assert_eq!(r.method, HttpMethod::Delete);
    assert_eq!(line(r), "Delete /v2/positions?cancel_orders=true");
}

#[test]
fn flatten_all_surfaces_a_position_that_could_not_be_closed() {
    let (a, t) = setup();
    t.enqueue_json(207, fixture!("flatten_partial_207.json"));
    let rep = a.flatten_all().unwrap();
    assert!(!rep.all_ok());
    let failed = rep.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].symbol, "EFA");
    assert_eq!(failed[0].http_status, 403);
    assert!(failed[0].error.as_deref().unwrap().contains("insufficient qty"));
    assert!(failed[0].order.is_none());
}

#[test]
fn flatten_with_no_positions_is_ok_and_failures_are_errors() {
    let (a, t) = setup();
    t.enqueue_json(207, "[]");
    let rep = a.flatten_all().unwrap();
    assert!(rep.all_ok() && rep.entries.is_empty());
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(a.flatten_all(), Err(BrokerError::Transport(TransportError::Timeout))));
    t.enqueue_json(500, fixture!("error_500.json"));
    assert!(matches!(a.flatten_all(), Err(BrokerError::Http(500))));
    t.enqueue_json(207, r#"[{"symbol":"SPY","status":200,"body":{"id":"x"}}]"#);
    assert!(matches!(a.flatten_all(), Err(BrokerError::Malformed(_))), "an order body we cannot parse is not a success");
    t.enqueue_json(207, "{}");
    assert!(matches!(a.flatten_all(), Err(BrokerError::Malformed(_))));
}

// ---------------------------------------------------------------- read errors

#[test]
fn http_errors_on_reads_surface_with_their_class() {
    let (a, t) = setup();
    t.enqueue_json(401, fixture!("error_401_unauthorized.json"));
    match a.get_account() {
        Err(BrokerError::Exchange(e)) => {
            assert_eq!(e[0].class, ErrorClass::Auth);
            assert!(e[0].code.contains("request is not authorized"));
        }
        other => panic!("{other:?}"),
    }
    t.enqueue_json(503, "<html>maintenance</html>");
    assert!(matches!(a.get_account(), Err(BrokerError::Http(503))));
    t.enqueue_error(TransportError::ConnectFailed("dns".into()));
    assert!(matches!(a.get_account(), Err(BrokerError::Transport(_))));
}

#[test]
fn cancel_and_settle_returns_the_final_report_when_there_is_nothing_to_cancel() {
    // already filled: DELETE answers 422 "order is not cancelable"; the re-query shows the truth
    let (a, t) = setup();
    t.enqueue_json(422, fixture!("error_422_not_cancelable.json"));
    t.enqueue_json(200, fixture!("order_filled.json"));
    let (c, report) = a.cancel_and_settle(ORDER_ID).unwrap();
    assert_eq!((c.canceled_count, c.pending), (0, false));
    assert_eq!(report.status, OrderStatus::Filled);
    assert_eq!(lines(&t), [format!("Delete /v2/orders/{ORDER_ID}"), format!("Get /v2/orders/{ORDER_ID}")]);

    // already canceled after a partial fill: the executed quantity is kept
    let (a, t) = setup();
    t.enqueue_json(422, fixture!("error_422_not_cancelable.json"));
    t.enqueue_json(200, fixture!("order_canceled_partial.json"));
    let (c, report) = a.cancel_and_settle(ORDER_ID).unwrap();
    assert_eq!(c.canceled_count, 0);
    assert_eq!(report.status, OrderStatus::PartiallyFilledThenCanceled);
    assert_eq!(report.executed_quantity, d("4"));
}

#[test]
fn cancel_and_settle_gives_a_specific_error_when_the_order_is_unknown_to_alpaca() {
    let (a, t) = setup();
    t.enqueue_json(404, fixture!("error_404_order.json"));
    t.enqueue_json(404, fixture!("error_404_order.json"));
    assert!(matches!(a.cancel_and_settle(ORDER_ID), Err(BrokerError::CancelTargetNotFound(_))));
    assert_eq!(t.request_count(), 2);
}

#[test]
fn cancel_and_settle_does_not_swallow_other_cancel_failures() {
    let (a, t) = setup();
    t.enqueue_error(TransportError::Timeout);
    assert!(matches!(a.cancel_and_settle(ORDER_ID), Err(BrokerError::Transport(TransportError::Timeout))));
    t.enqueue_json(503, "<html>maintenance</html>");
    assert!(matches!(a.cancel_and_settle(ORDER_ID), Err(BrokerError::Http(503))));
    t.enqueue_json(401, fixture!("error_401_unauthorized.json"));
    assert!(matches!(a.cancel_and_settle(ORDER_ID), Err(BrokerError::Exchange(_))));
    assert_eq!(t.request_count(), 3, "no re-query after these");
}

// ---------------------------------------------------------------- overfill (filled_qty > qty)

fn overfilled(fixture_body: &str, from: &str, to: &str) -> String {
    let out = fixture_body.replace(from, to);
    assert_ne!(out, fixture_body, "fixture edit {from} -> {to} did not apply");
    out
}

#[test]
fn an_overfilled_order_report_is_an_error_through_every_read_path_and_never_an_inflated_fill() {
    // filled: qty 1.5, filled_qty 3 (a duplicated fill report)
    let filled = overfilled(fixture!("order_filled.json"), "\"filled_qty\": \"1.5\"", "\"filled_qty\": \"3\"");
    match get_one(&filled) {
        Err(BrokerError::Malformed(m)) => assert!(m.contains("overfill anomaly") && m.contains(ORDER_ID), "{m}"),
        other => panic!("{other:?}"),
    }
    // by tag
    let (a, t) = setup();
    t.enqueue_json(200, &filled);
    assert!(matches!(a.find_orders_by_tag(TAG), Err(BrokerError::Malformed(m)) if m.contains("overfill anomaly")));
    // one overfilled order poisons the whole open-orders listing, so the caller halts
    let (a, t) = setup();
    t.enqueue_json(200, &format!("[{filled}]"));
    assert!(matches!(a.open_orders(), Err(BrokerError::Malformed(m)) if m.contains("overfill anomaly")));
    // canceled after a partial fill whose filled_qty exceeds qty (qty 10 in the fixture)
    let canceled = overfilled(fixture!("order_canceled_partial.json"), "\"filled_qty\": \"4\"", "\"filled_qty\": \"11\"");
    assert!(matches!(get_one(&canceled), Err(BrokerError::Malformed(m)) if m.contains("overfill anomaly")));
}

#[test]
fn cancel_and_settle_halts_on_an_overfilled_requery() {
    let filled = overfilled(fixture!("order_filled.json"), "\"filled_qty\": \"1.5\"", "\"filled_qty\": \"3\"");
    let (a, t) = setup();
    t.enqueue_json(422, fixture!("error_422_not_cancelable.json"));
    t.enqueue_json(200, &filled);
    assert!(matches!(a.cancel_and_settle(ORDER_ID), Err(BrokerError::Malformed(m)) if m.contains("overfill anomaly")));
}

#[test]
fn exact_fills_and_notional_orders_are_unchanged_by_the_overfill_guard() {
    assert_eq!(get_one(fixture!("order_filled.json")).unwrap().status, OrderStatus::Filled);
    let n = get_one(fixture!("order_notional_filled.json")).unwrap();
    assert_eq!(n.status, OrderStatus::Filled);
    assert_eq!(n.executed_quantity, d("0.6493"));
}
