//! The fake OANDA front end on its own, spoken to with hand-built requests (no adapter): auth, routing, order and
//! position mechanics, margin, faults, and the behaviours MEASURED on a real practice account (2026-09-23) that the fake
//! must reproduce: pending-only lookup by client id, non-unique client ids, the transaction stream, one-sided closes,
//! the unknown-instrument error shape, the 128-character client id limit. These pin the model the adapter drills lean on,
//! so a drill failure can be told apart from a fake bug. Beyond those points the fake is from documentation, not from
//! OANDA: see the module docs of `fake_broker::oanda`.

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
fn an_unknown_instrument_is_an_invalid_parameter_exception_with_no_reject_transaction() {
    // MEASURED: HTTP 400, errorCode oanda::rest::core::InvalidParameterException, NO reject transaction (and so no id used).
    let (_f, t, h) = setup();
    let before = h.last_transaction_id();
    let (s, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(json!({"order": {"type": "MARKET", "instrument": "XXX_YYY", "units": "1",
        "timeInForce": "FOK", "positionFill": "DEFAULT"}}))));
    assert_eq!(s, 400);
    assert_eq!(b["errorCode"], "oanda::rest::core::InvalidParameterException");
    assert_eq!(b["errorMessage"], "Invalid value specified for 'order.instrument'");
    assert!(b.get("orderRejectTransaction").is_none() && b.get("lastTransactionID").is_none());
    assert_eq!(h.last_transaction_id(), before, "nothing entered the transaction stream");
    assert!(h.orders().is_empty());
}

#[test]
fn a_client_id_of_129_characters_is_refused_and_128_is_accepted() {
    // MEASURED: 128 accepted, 129 -> HTTP 400 CLIENT_ORDER_ID_INVALID (a MARKET_ORDER_REJECT with rejectReason == errorCode).
    let (_f, t, h) = setup();
    let (s, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("1", &"x".repeat(129)))));
    assert_eq!((s, b["errorCode"].as_str(), b["orderRejectTransaction"]["rejectReason"].as_str()), (400, Some("CLIENT_ORDER_ID_INVALID"), Some("CLIENT_ORDER_ID_INVALID")));
    assert!(h.orders().is_empty());
    let odd = format!("{}{}", "a:b.c-d_e/f g", "x".repeat(128 - 13));
    assert_eq!(odd.chars().count(), 128);
    let (s, _) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("1", &odd))));
    assert_eq!(s, 201, "128 characters with : . - _ / and a space are accepted");
    let (s, _) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("1", "caf\u{e9}"))));
    assert_eq!(s, 201, "a non-ASCII letter is accepted too");
}

#[test]
fn instruments_report_maximum_position_size_zero_meaning_no_cap_unless_a_cap_is_set() {
    let f = FakeOandaBuilder::new()
        .balance("1000")
        .instrument(InstrumentSpec::fx("EUR_USD", 5), "1.10048", "1.10052")
        .instrument(InstrumentSpec::fx("GBP_USD", 5).with_max_position("25000"), "1.26996", "1.27004")
        .build();
    let (_, b) = send(&*f.transport(), req(HttpMethod::Get, &acct("/instruments"), None));
    let rows = b["instruments"].as_array().unwrap();
    let cap = |n: &str| rows.iter().find(|r| r["name"] == n).unwrap()["maximumPositionSize"].clone();
    assert_eq!(cap("EUR_USD"), "0", "measured: the string \"0\" on every instrument");
    assert_eq!(cap("GBP_USD"), "25000");
}

#[test]
fn the_transaction_stream_is_consecutive_complete_and_agrees_with_the_summary() {
    let (_f, t, h) = setup();
    let last0 = h.last_transaction_id();
    let (_, s) = send(&*t, req(HttpMethod::Get, &acct("/summary"), None));
    assert_eq!(s["lastTransactionID"], last0.to_string());
    assert_eq!(s["account"]["lastTransactionID"], last0.to_string());
    // a fill (create + fill), a cancel-at-creation (create + cancel), a reject, a resting limit and its cancel
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("1000", "s1"))));
    h.set_market_open(false);
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("100", "s2"))));
    h.set_market_open(true);
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("0", "s3"))));
    let lim = json!({"order": {"type": "LIMIT", "instrument": "EUR_USD", "units": "5", "price": "1.00000", "timeInForce": "GTC", "positionFill": "DEFAULT", "clientExtensions": {"id": "s4"}}});
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(lim)));
    let oid = b["orderCreateTransaction"]["id"].as_str().unwrap().to_string();
    send(&*t, req(HttpMethod::Put, &acct(&format!("/orders/{oid}/cancel")), None));
    send(&*t, req(HttpMethod::Put, &acct(&format!("/orders/{oid}/cancel")), None));

    let (st, page) = send(&*t, req(HttpMethod::Get, &acct(&format!("/transactions/sinceid?id={last0}")), None));
    assert_eq!(st, 200);
    let txns = page["transactions"].as_array().unwrap();
    let last = h.last_transaction_id();
    assert_eq!(page["lastTransactionID"], last.to_string());
    let ids: Vec<u64> = txns.iter().map(|x| x["id"].as_str().unwrap().parse().unwrap()).collect();
    assert_eq!(ids, (last0 + 1..=last).collect::<Vec<u64>>(), "every id after the checkpoint, once, in order (rejects included)");
    let kinds: Vec<&str> = txns.iter().map(|x| x["type"].as_str().unwrap()).collect();
    assert_eq!(
        kinds,
        ["MARKET_ORDER", "ORDER_FILL", "MARKET_ORDER", "ORDER_CANCEL", "MARKET_ORDER_REJECT", "LIMIT_ORDER", "ORDER_CANCEL", "ORDER_CANCEL_REJECT"]
    );
    // MEASURED placement of the client id: clientExtensions.id on the order, clientOrderID on fill / cancel / cancel-reject
    for (t, want) in txns.iter().zip(["s1", "s1", "s2", "s2", "s3", "s4", "s4", ""]) {
        let kind = t["type"].as_str().unwrap();
        let carried = match kind {
            "MARKET_ORDER" | "LIMIT_ORDER" | "MARKET_ORDER_REJECT" => t["clientExtensions"]["id"].as_str(),
            _ => t["clientOrderID"].as_str(),
        };
        if want.is_empty() {
            assert_eq!(carried, None, "{kind}: cancelled by numeric id, so it names none");
        } else {
            assert_eq!(carried, Some(want), "{kind}");
        }
    }
    // after a later id only later transactions come back; at the end nothing
    let (_, later) = send(&*t, req(HttpMethod::Get, &acct(&format!("/transactions/sinceid?id={}", last0 + 2)), None));
    assert_eq!(later["transactions"].as_array().unwrap().len() as u64, last - (last0 + 2));
    let (_, end) = send(&*t, req(HttpMethod::Get, &acct(&format!("/transactions/sinceid?id={last}")), None));
    assert!(end["transactions"].as_array().unwrap().is_empty());
    assert_eq!(end["lastTransactionID"], last.to_string());
    assert_eq!(send(&*t, req(HttpMethod::Get, &acct("/transactions/sinceid?id=abc"), None)).0, 400);
    h.assert_invariants();
}

#[test]
fn external_activity_and_a_truncating_server_can_be_simulated() {
    let (_f, t, h) = setup();
    let last0 = h.last_transaction_id();
    h.add_external_transactions(5);
    assert_eq!(h.last_transaction_id(), last0 + 5);
    h.set_sinceid_page_limit(Some(2));
    let (_, page) = send(&*t, req(HttpMethod::Get, &acct(&format!("/transactions/sinceid?id={last0}")), None));
    assert_eq!(page["transactions"].as_array().unwrap().len(), 2);
    assert_eq!(page["lastTransactionID"], (last0 + 5).to_string(), "the account's real last id: the page is visibly short");
    let small = FakeOandaBuilder::new().first_transaction_id(2).balance("1").build();
    assert_eq!(small.handle().last_transaction_id(), 1);
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
    // MEASURED: once it is FILLED the lookup by client id is a 404 NO_SUCH_ORDER
    let (s, o) = send(&*t, req(HttpMethod::Get, &acct("/orders/%40lim1"), None));
    assert_eq!((s, o["errorCode"].as_str()), (404, Some("NO_SUCH_ORDER")));
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
    // unknown client id, and (MEASURED) a CANCELLED order's client id is unknown too
    assert_eq!(send(&*t, req(HttpMethod::Get, &acct("/orders/%40nobody"), None)).0, 404);
    let (s, o) = send(&*t, req(HttpMethod::Get, &acct("/orders/%40lim2"), None));
    assert_eq!((s, o["errorCode"].as_str()), (404, Some("NO_SUCH_ORDER")));
    // MEASURED: cancelling again is 404 ORDER_DOESNT_EXIST with a cancel-reject transaction naming the client id when addressed by it
    let (s, c) = send(&*t, req(HttpMethod::Put, &acct("/orders/%40lim2/cancel"), None));
    assert_eq!((s, c["orderCancelRejectTransaction"]["clientOrderID"].as_str(), c["errorCode"].as_str()), (404, Some("lim2"), Some("ORDER_DOESNT_EXIST")));
    h.assert_invariants();
}

#[test]
fn close_position_needs_all_for_the_side_that_exists_and_none_for_the_other() {
    let (_f, t, h) = setup();
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("3000", "o"))));
    let close = json!({"longUnits": "ALL", "shortUnits": "NONE", "longClientExtensions": {"id": "cl1", "tag": "t"}});
    let (s, b) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(close.clone())));
    assert_eq!(s, 200);
    assert_eq!(b["longOrderFillTransaction"]["units"], "-3000");
    assert_eq!(b["longOrderFillTransaction"]["reason"], "MARKET_ORDER_POSITION_CLOSEOUT");
    assert_eq!(b["longOrderCreateTransaction"]["reason"], "POSITION_CLOSEOUT");
    assert_eq!(b["longOrderCreateTransaction"]["clientExtensions"]["id"], "cl1");
    assert_eq!(h.position_units("EUR_USD"), Dec::ZERO);
    // MEASURED: nothing open at all -> HTTP 404 CLOSEOUT_POSITION_DOESNT_EXIST with a reject transaction
    let (s, b) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(close)));
    assert_eq!((s, b["errorCode"].as_str()), (404, Some("CLOSEOUT_POSITION_DOESNT_EXIST")));
    assert_eq!(b["longOrderRejectTransaction"]["rejectReason"], "CLOSEOUT_POSITION_DOESNT_EXIST");
    // MEASURED: ALL for a side that does not exist -> HTTP 400 (long ALL against a short)
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("-100", "s"))));
    let (s, b) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(json!({"longUnits": "ALL", "shortUnits": "NONE"}))));
    assert_eq!((s, b["errorCode"].as_str()), (400, Some("CLOSEOUT_POSITION_DOESNT_EXIST")));
    assert!(b.get("longOrderRejectTransaction").is_some());
    assert_eq!(h.position_units("EUR_USD"), d("-100"), "the refused close changed nothing");
    // ALL for both sides of a one-sided position is refused too, and nothing closes
    let (s, _) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(json!({"longUnits": "ALL", "shortUnits": "ALL"}))));
    assert_eq!(s, 400);
    assert_eq!(h.position_units("EUR_USD"), d("-100"));
    // the short side, correctly
    let (s, b) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(json!({"longUnits": "NONE", "shortUnits": "ALL"}))));
    assert_eq!(s, 200);
    assert_eq!(b["shortOrderFillTransaction"]["units"], "100");
    // bodies that make no sense
    for bad in [json!({}), json!({"longUnits": "NONE", "shortUnits": "NONE"}), json!({"longUnits": "5"})] {
        assert_eq!(send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"), Some(bad))).0, 400);
    }
    h.assert_invariants();
}

#[test]
fn a_close_can_be_made_to_leave_no_client_id_on_its_transactions() {
    // UNMEASURED at real OANDA (whether longClientExtensions is echoed): switchable so the adapter is shown to cope with both.
    let (_f, t, h) = setup();
    h.set_echo_close_client_ids(false);
    send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("10", "o"))));
    let (s, b) = send(&*t, req(HttpMethod::Put, &acct("/positions/EUR_USD/close"),
        Some(json!({"longUnits": "ALL", "shortUnits": "NONE", "longClientExtensions": {"id": "cl1"}}))));
    assert_eq!(s, 200);
    assert!(b["longOrderCreateTransaction"].get("clientExtensions").is_none());
    assert!(b["longOrderFillTransaction"].get("clientOrderID").is_none());
}

#[test]
fn numeric_order_lookup_of_finished_orders_is_switchable() {
    // UNMEASURED at real OANDA (by client id it is measured: pending only).
    let (_f, t, h) = setup();
    let (_, b) = send(&*t, req(HttpMethod::Post, &acct("/orders"), Some(market("10", "n"))));
    let id = b["orderCreateTransaction"]["id"].as_str().unwrap().to_string();
    let (s, o) = send(&*t, req(HttpMethod::Get, &acct(&format!("/orders/{id}")), None));
    assert_eq!((s, o["order"]["state"].as_str()), (200, Some("FILLED")));
    h.set_historic_order_lookup(false);
    let (s, o) = send(&*t, req(HttpMethod::Get, &acct(&format!("/orders/{id}")), None));
    assert_eq!((s, o["errorCode"].as_str()), (404, Some("NO_SUCH_ORDER")));
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
    // MEASURED: two FILLS carry the same clientOrderID, and the lookup by that id finds neither (both are filled)
    let fills: Vec<Value> = h.transactions().into_iter().filter(|x| x["type"] == "ORDER_FILL").collect();
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0]["clientOrderID"], fills[1]["clientOrderID"]);
    assert_eq!(send(&*t, req(HttpMethod::Get, &acct("/orders/%40same"), None)).0, 404);
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
