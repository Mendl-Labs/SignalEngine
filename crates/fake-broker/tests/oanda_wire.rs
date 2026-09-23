//! The fake OANDA front end on its own, spoken to with hand-built requests (no adapter): auth, routing, order and
//! position mechanics, margin, faults. These pin the model the adapter drills lean on, so a drill failure can be
//! told apart from a fake bug. The fake is from documentation, not from OANDA: see the module docs of `fake_broker::oanda`.

use broker_adapters::transport::{HttpMethod, HttpRequest, HttpTransport, TransportError};
use broker_adapters::Dec;
use fake_broker::oanda::{FakeOanda, FakeOandaBuilder, InstrumentSpec, OrderScript, DEFAULT_ACCOUNT_ID, DEFAULT_TOKEN};
use fake_broker::Fault;
use serde_json::{json, Value};

const BASE: &str = "https://api-fxpractice.oanda.com";

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

fn acct(tail: &str) -> String {
    format!("/v3/accounts/{DEFAULT_ACCOUNT_ID}{tail}")
}

fn req(method: HttpMethod, path: &str, body: Option<Value>) -> HttpRequest {
    HttpRequest {
        method,
        url: format!("{BASE}{path}"),
        headers: vec![("Authorization".into(), format!("Bearer {DEFAULT_TOKEN}"))],
        body: body.map(|b| b.to_string()),
    }
}

fn send(t: &dyn HttpTransport, r: HttpRequest) -> (u16, Value) {
    let resp = t.execute(&r).expect("transport ok");
    (resp.status, serde_json::from_str(&resp.body).unwrap_or(Value::Null))
}

fn market(units: &str, cid: &str) -> Value {
    json!({"order": {"type": "MARKET", "instrument": "EUR_USD", "units": units, "timeInForce": "FOK", "positionFill": "DEFAULT",
        "clientExtensions": {"id": cid, "tag": "t"}}})
}

fn setup() -> (FakeOanda, std::sync::Arc<fake_broker::oanda::OandaTransport>, fake_broker::oanda::OandaHandle) {
    let f = FakeOanda::standard();
    let t = f.transport();
    let h = f.handle();
    (f, t, h)
}

#[test]
fn auth_account_and_host_are_checked() {
    let (_f, t, _h) = setup();
    // no header, wrong token, wrong account, unknown path, other host
    let mut r = req(HttpMethod::Get, &acct("/summary"), None);
    r.headers.clear();
    assert_eq!(send(&*t, r).0, 401);
    let mut r = req(HttpMethod::Get, &acct("/summary"), None);
    r.headers = vec![("Authorization".into(), "Bearer nope".into())];
    assert_eq!(send(&*t, r).0, 401);
    assert_eq!(send(&*t, req(HttpMethod::Get, "/v3/accounts/101-001-9999999-001/summary", None)).0, 404);
    assert_eq!(send(&*t, req(HttpMethod::Get, &acct("/nonsense"), None)).0, 404);
    let mut r = req(HttpMethod::Get, &acct("/summary"), None);
    r.url = format!("https://api-fxtrade.oanda.com{}", acct("/summary"));
    assert!(matches!(t.execute(&r), Err(TransportError::ConnectFailed(_))), "the fake answers only its own host");
    assert_eq!(send(&*t, req(HttpMethod::Get, &acct("/summary"), None)).0, 200);
}

#[test]
fn a_market_buy_fills_at_the_ask_and_a_sell_at_the_bid() {
    let (_f, t, h) = setup();
    let (s, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("1000", "c1"))));
    assert_eq!(s, 201);
    assert_eq!(b["orderCreateTransaction"]["clientExtensions"]["id"], "c1");
    assert_eq!(b["orderFillTransaction"]["units"], "1000");
    assert_eq!(b["orderFillTransaction"]["price"], "1.10052");
    assert!(b.get("orderCancelTransaction").is_none());
    assert_eq!(h.position_units("EUR_USD"), d("1000"));
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("-400", "c2"))));
    assert_eq!(b["orderFillTransaction"]["units"], "-400");
    assert_eq!(b["orderFillTransaction"]["price"], "1.10048");
    assert_eq!(h.position_units("EUR_USD"), d("600"));
    h.assert_invariants();
}

#[test]
fn netting_flips_through_zero_and_books_realised_pl() {
    let (_f, t, h) = setup();
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("1000", "a"))));
    assert_eq!(h.position_avg("EUR_USD"), Some(d("1.10052")));
    h.set_price("EUR_USD", "1.11052", "1.11056");
    // sell 1500: closes 1000 at the bid (1.11052) for +10.00, opens 500 short at 1.11052
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("-1500", "b"))));
    assert_eq!(b["orderFillTransaction"]["pl"], "10.0000");
    assert_eq!(h.position_units("EUR_USD"), d("-500"));
    assert_eq!(h.position_avg("EUR_USD"), Some(d("1.11052")));
    assert_eq!(h.balance(), d("100010.0000"));
    // unrealised on the short: (ask - avg) * units = (1.11056 - 1.11052) * -500 = -0.02
    assert_eq!(h.unrealized_pl(), d("-0.02"));
    assert_eq!(h.nav(), d("100009.98"));
    h.assert_invariants();
}

#[test]
fn a_position_averages_its_entries() {
    let (_f, t, h) = setup();
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("1000", "a"))));
    h.set_price("EUR_USD", "1.12000", "1.12004");
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("1000", "b"))));
    assert_eq!(h.position_units("EUR_USD"), d("2000"));
    assert_eq!(h.position_avg("EUR_USD"), Some(d("1.11028")));
}

#[test]
fn insufficient_margin_cancels_the_order_at_creation() {
    let (_f, t, h) = setup();
    // 100000 USD at 2 percent margin supports about 5 million notional; ask for 6 million
    let (s, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("6000000", "big"))));
    assert_eq!(s, 201);
    assert_eq!(b["orderCancelTransaction"]["reason"], "INSUFFICIENT_MARGIN");
    assert!(b.get("orderFillTransaction").is_none());
    assert_eq!(h.position_units("EUR_USD"), Dec::ZERO);
    // and 4 million fits
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("4000000", "ok"))));
    assert!(b.get("orderFillTransaction").is_some());
    assert!(h.margin_used() > d("80000") && h.margin_used() < d("100000"), "{}", h.margin_used());
    // reducing is always allowed even when margin is tight
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("-1000000", "red"))));
    assert!(b.get("orderFillTransaction").is_some());
    h.assert_invariants();
}

#[test]
fn invalid_orders_are_rejected_with_a_400_and_create_nothing() {
    let (_f, t, h) = setup();
    for (body, reason) in [
        (market("0", "x"), "UNITS_INVALID"),
        (market("10.5", "x"), "UNITS_PRECISION_EXCEEDED"),
        (market("abc", "x"), "UNITS_INVALID"),
        (market("999999999999", "x"), "UNITS_LIMIT_EXCEEDED"),
        (json!({"order": {"type": "MARKET", "instrument": "XXX_YYY", "units": "1", "timeInForce": "FOK", "positionFill": "DEFAULT"}}), "INSTRUMENT_INVALID"),
        (json!({"order": {"type": "MARKET", "instrument": "EUR_USD", "units": "1", "timeInForce": "GTC", "positionFill": "DEFAULT"}}), "TIME_IN_FORCE_INVALID"),
        (json!({"order": {"type": "MARKET", "instrument": "EUR_USD", "units": "1", "timeInForce": "FOK", "positionFill": "DEFAULT", "price": "1.1"}}), "PRICE_NOT_ALLOWED"),
        (json!({"order": {"type": "STOP", "instrument": "EUR_USD", "units": "1"}}), "ORDER_TYPE_INVALID"),
    ] {
        let (s, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(body)));
        assert_eq!(s, 400, "{reason}");
        assert_eq!(b["errorCode"], reason);
        assert_eq!(b["orderRejectTransaction"]["rejectReason"], reason);
    }
    assert!(h.orders().is_empty());
    assert_eq!(send(&*t, req(HttpMethod::Post, &acct("/orders"), None)).0, 400);
}

#[test]
fn limit_orders_rest_until_the_price_reaches_them_and_can_be_cancelled() {
    let (_f, t, h) = setup();
    let lim = json!({"order": {"type": "LIMIT", "instrument": "EUR_USD", "units": "2000", "price": "1.09500", "timeInForce": "GTC", "positionFill": "DEFAULT",
        "clientExtensions": {"id": "lim1"}}});
    let (s, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(lim.clone())));
    assert_eq!(s, 201);
    assert!(b.get("orderFillTransaction").is_none() && b.get("orderCancelTransaction").is_none());
    let id = b["orderCreateTransaction"]["id"].as_str().unwrap().to_string();
    let (_, p) = send(&*t, req(HttpMethod::Get, &acct("/pendingOrders"), None));
    assert_eq!(p["orders"].as_array().unwrap().len(), 1);
    // lookup by client id, percent-encoded @
    let (s, o) = send(&*t, req(HttpMethod::Get, &acct("/orders/%40lim1"), None));
    assert_eq!((s, o["order"]["state"].as_str()), (200, Some("PENDING")));
    // price falls through the limit: it fills at the ask
    h.set_price("EUR_USD", "1.09400", "1.09404");
    let (_, o) = send(&*t, req(HttpMethod::Get, &acct(&format!("/orders/{id}")), None));
    assert_eq!(o["order"]["state"], "FILLED");
    let fid = o["order"]["fillingTransactionID"].as_str().unwrap();
    let (_, tx) = send(&*t, req(HttpMethod::Get, &acct(&format!("/transactions/{fid}")), None));
    assert_eq!((tx["transaction"]["type"].as_str(), tx["transaction"]["price"].as_str()), (Some("ORDER_FILL"), Some("1.09404")));
    assert_eq!(h.position_units("EUR_USD"), d("2000"));
    // a second limit that we cancel
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(json!({"order": {"type": "LIMIT", "instrument": "EUR_USD", "units": "-500", "price": "1.20000",
        "timeInForce": "GTC", "positionFill": "DEFAULT", "clientExtensions": {"id": "lim2"}}}))));
    let id2 = b["orderCreateTransaction"]["id"].as_str().unwrap().to_string();
    let (s, c) = send(&*t, req(HttpMethod::Put, &acct(&format!("/orders/{id2}/cancel")), None));
    assert_eq!(s, 200);
    assert_eq!(c["orderCancelTransaction"]["reason"], "CLIENT_REQUEST");
    // cancelling again, or a filled order, is a 404 with a cancel-reject transaction
    let (s, c) = send(&*t, req(HttpMethod::Put, &acct(&format!("/orders/{id2}/cancel")), None));
    assert_eq!(s, 404);
    assert!(c.get("orderCancelRejectTransaction").is_some());
    assert_eq!(send(&*t, req(HttpMethod::Put, &acct(&format!("/orders/{id}/cancel")), None)).0, 404);
    // unknown client id
    assert_eq!(send(&*t, req(HttpMethod::Get, &acct("/orders/%40nobody"), None)).0, 404);
    h.assert_invariants();
}

#[test]
fn close_position_closes_one_side_with_all() {
    let (_f, t, h) = setup();
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("3000", "o"))));
    let close = json!({"longUnits": "ALL", "longClientExtensions": {"id": "cl1", "tag": "t"}});
    let (s, b) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(close.clone())));
    assert_eq!(s, 201);
    assert_eq!(b["longOrderFillTransaction"]["units"], "-3000");
    assert_eq!(b["longOrderCreateTransaction"]["clientExtensions"]["id"], "cl1");
    assert_eq!(h.position_units("EUR_USD"), Dec::ZERO);
    // nothing left: a reject transaction under the same prefix
    let (s, b) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(close)));
    assert_eq!(s, 400);
    assert!(b.get("longOrderRejectTransaction").is_some());
    // the wrong side is refused too
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("-100", "s"))));
    let (s, _) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(json!({"longUnits": "ALL"}))));
    assert_eq!(s, 400);
    let (s, b) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(json!({"shortUnits": "ALL"}))));
    assert_eq!(s, 201);
    assert_eq!(b["shortOrderFillTransaction"]["units"], "100");
    h.assert_invariants();
}

#[test]
fn pricing_reports_two_sided_prices_and_home_conversions() {
    let (_f, t, h) = setup();
    let (s, b) = send(&*t, req(HttpMethod::Get, &acct("/pricing?instruments=EUR_USD,USD_JPY&includeHomeConversions=true"), None));
    assert_eq!(s, 200);
    assert_eq!(b["prices"][0]["bids"][0]["price"], "1.10048");
    assert_eq!(b["prices"][1]["asks"][0]["price"], "148.512");
    let convs = b["homeConversions"].as_array().unwrap();
    let jpy = convs.iter().find(|c| c["currency"] == "JPY").unwrap();
    // 1 / 148.505 = 0.0067335..., floored to six decimals
    assert_eq!(jpy["positionValue"], "0.006733");
    let usd = convs.iter().find(|c| c["currency"] == "USD").unwrap();
    assert_eq!(usd["positionValue"], "1.0");
    h.set_market_open(false);
    let (_, b) = send(&*t, req(HttpMethod::Get, &acct("/pricing?instruments=EUR_USD"), None));
    assert_eq!(b["prices"][0]["tradeable"], false);
    assert_eq!(b["prices"][0]["bids"].as_array().unwrap().len(), 0);
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("100", "h"))));
    assert_eq!(b["orderCancelTransaction"]["reason"], "MARKET_HALTED");
    assert_eq!(send(&*t, req(HttpMethod::Get, &acct("/pricing?instruments=NOPE_USD"), None)).0, 400);
}

#[test]
fn scripted_answers_and_liquidity_caps() {
    let (_f, t, h) = setup();
    h.script_next_order(OrderScript::Reject("INSTRUMENT_NOT_TRADEABLE".into()));
    let (s, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("100", "r"))));
    assert_eq!((s, b["errorCode"].as_str()), (400, Some("INSTRUMENT_NOT_TRADEABLE")));
    h.script_next_order(OrderScript::CancelOnCreate("FAKE_SCRIPTED".into()));
    let (s, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("100", "c"))));
    assert_eq!((s, b["orderCancelTransaction"]["reason"].as_str()), (201, Some("FAKE_SCRIPTED")));
    assert_eq!(h.position_units("EUR_USD"), Dec::ZERO);
    // liquidity: FOK for more than is there is cancelled; IOC fills what is there
    h.set_liquidity("EUR_USD", Some("400"));
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("1000", "f"))));
    assert_eq!(b["orderCancelTransaction"]["reason"], "INSUFFICIENT_LIQUIDITY");
    let ioc = json!({"order": {"type": "MARKET", "instrument": "EUR_USD", "units": "1000", "timeInForce": "IOC", "positionFill": "DEFAULT", "clientExtensions": {"id": "i"}}});
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(ioc)));
    assert_eq!(b["orderFillTransaction"]["units"], "400");
    assert_eq!(h.position_units("EUR_USD"), d("400"));
    h.assert_invariants();
}

#[test]
fn the_fake_does_not_reject_a_reused_client_id_unless_asked_to() {
    let (_f, t, h) = setup();
    for _ in 0..2 {
        assert_eq!(send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("100", "same")))).0, 201);
    }
    assert_eq!(h.orders_with_client_id("same").len(), 2, "hostile by default: only the ADAPTER can prevent a double submit");
    assert_eq!(h.position_units("EUR_USD"), d("200"));
    // strict mode refuses a client id that is still pending
    let (_f, t, h) = setup();
    h.reject_duplicate_client_ids(true);
    let lim = json!({"order": {"type": "LIMIT", "instrument": "EUR_USD", "units": "10", "price": "1.00000", "timeInForce": "GTC", "positionFill": "DEFAULT", "clientExtensions": {"id": "p"}}});
    assert_eq!(send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(lim.clone()))).0, 201);
    let (s, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(lim)));
    assert_eq!((s, b["errorCode"].as_str()), (400, Some("CLIENT_ORDER_ID_ALREADY_EXISTS")));
}

#[test]
fn faults_lose_requests_or_answers_and_the_log_says_which() {
    let (_f, t, h) = setup();
    let path = h.path("/orders");
    // request lost before it reaches the exchange
    h.inject_fault(Fault::timeout().on_path(&path));
    assert!(matches!(t.execute(&req(HttpMethod::Post, &path, Some(market("100", "a")))), Err(TransportError::Timeout)));
    assert!(h.orders().is_empty());
    // applied, answer lost
    h.inject_fault(Fault::timeout().after_apply().on_path(&path));
    assert!(matches!(t.execute(&req(HttpMethod::Post, &path, Some(market("100", "b")))), Err(TransportError::Timeout)));
    assert_eq!(h.orders_with_client_id("b").len(), 1);
    // HTTP faults
    h.inject_fault(Fault::rate_limit().on_path(&path));
    assert_eq!(send(&*t, req(HttpMethod::Post, &path, Some(market("100", "c")))).0, 429);
    h.inject_fault(Fault::http(503).on_path(&path));
    assert_eq!(send(&*t, req(HttpMethod::Post, &path, Some(market("100", "d")))).0, 503);
    assert!(h.orders_with_client_id("c").is_empty() && h.orders_with_client_id("d").is_empty());
    // delayed: the caller sees a timeout, the order lands later
    h.inject_fault(Fault::timeout().delayed().on_path(&path));
    assert!(t.execute(&req(HttpMethod::Post, &path, Some(market("100", "e")))).is_err());
    assert!(h.orders_with_client_id("e").is_empty());
    let late = h.deliver_delayed();
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].status, 201);
    assert_eq!(h.orders_with_client_id("e").len(), 1);
    let reqs = h.requests();
    assert_eq!(reqs.iter().filter(|r| r.reached_exchange && r.method == HttpMethod::Post).count(), 2, "b (after apply), e (delivered late), and nothing else");
    assert_eq!(h.order_affecting_requests(), 2);
    assert!(h.dump_log().contains("FAULT"));
    h.assert_invariants();
}

#[test]
fn builder_refuses_an_instrument_without_a_usd_leg() {
    let r = std::panic::catch_unwind(|| {
        FakeOandaBuilder::new().instrument(InstrumentSpec::fx("EUR_GBP", 5), "0.85", "0.85005").build();
    });
    assert!(r.is_err());
}
