//! End-to-end OANDA adapter behaviour over JSON fixtures and `FakeTransport`.
//!
//! PROVENANCE, two kinds of fixture (see `tests/fixtures/oanda/README.md`):
//! * `rfx!(..)` = RECORDED from an OANDA practice account on 2026-09-23 (`tests/fixtures/oanda/real/`, account and user ids
//!   removed). Where a test says DERIVED it starts from a recording and adds one field, and says which.
//! * `fx!(..)` = AUTHORED FROM DOCUMENTATION for shapes that have NOT been measured (account summary, instruments, positions,
//!   pricing, finished orders read by numeric id, cancel-at-creation, generic error bodies). These tests prove the adapter
//!   handles the shapes we BELIEVE OANDA sends; they cannot prove OANDA sends them.
//!
//! Every adapter is built with a recent-transaction window of `WINDOW` ids so a scan page is small and easy to state.

use broker_adapters::oanda::{
    CloseOutcome, Environment, InstrumentTable, OandaAdapter, OandaConfig, OandaCredentials, PRACTICE_BASE_URL,
};
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpMethod, HttpRequest, HttpResponseDetailed, TransportError};
use broker_adapters::types::{BalanceKind, BrokerAdapter, OrderKind, OrderRequest, OrderStatus, PlaceOutcome, Side, TimeInForce};
use broker_adapters::{BrokerError, Dec, ErrorClass};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

const TOKEN: &str = "tok-9f3a-unit-test-not-a-real-token";
const ACCT: &str = "101-001-1234567-001";
const TAG: &str = "rb1:run1:EUR/USD:buy";
const SELL_TAG: &str = "rb1:run1:EUR/USD:sell";
/// `TAG` as it appears in a URL path after `@` (percent-encoded).
const TAG_PATH: &str = "%40rb1%3Arun1%3AEUR%2FUSD%3Abuy";
/// Client ids of the RECORDED responses.
const R_TAG: &str = "lookup-filled-20260923T211402Z";
const R_LIMIT_TAG: &str = "limit-20260923T211402Z";
const R_SCAN_TAG: &str = "txscan-20260923T211519Z";
/// The recent-transaction window every test adapter scans (production default: 400).
const WINDOW: u64 = 10;
/// `lastTransactionID` of `account_summary_ok.json`.
const LAST: u64 = 6400;

macro_rules! fx {
    ($name:literal) => {
        include_str!(concat!("fixtures/oanda/", $name))
    };
}

/// A RECORDED response from the second session (summary, instruments, pricing, positions; taken while the smoke test ran).
macro_rules! sfx {
    ($name:literal) => {
        include_str!(concat!("fixtures/oanda/real/oanda_smoke__", $name, ".json"))
    };
}

/// A RECORDED response (real practice account, 2026-09-23).
macro_rules! rfx {
    ($name:literal) => {
        include_str!(concat!("fixtures/oanda/real/oanda_211402__", $name, ".json"))
    };
}

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

// ---------------------------------------------------------------- routing transport

#[derive(Clone)]
enum Reply {
    Http(u16, String, Vec<(String, String)>),
    Err(TransportError),
    /// Answers in order; the last answer repeats.
    Seq(Arc<Mutex<VecDeque<(u16, String)>>>),
}

#[derive(Clone)]
struct Route {
    method: HttpMethod,
    path: String,
    reply: Reply,
}

#[derive(Clone, Default)]
struct Routes(Vec<Route>);

impl Routes {
    fn on(mut self, method: HttpMethod, path: &str, status: u16, body: &str) -> Self {
        self.0.push(Route { method, path: path.to_string(), reply: Reply::Http(status, body.to_string(), Vec::new()) });
        self
    }
    fn get(self, path: &str, status: u16, body: &str) -> Self {
        self.on(HttpMethod::Get, path, status, body)
    }
    fn post(self, path: &str, status: u16, body: &str) -> Self {
        self.on(HttpMethod::Post, path, status, body)
    }
    fn put(self, path: &str, status: u16, body: &str) -> Self {
        self.on(HttpMethod::Put, path, status, body)
    }
    fn get_seq(mut self, path: &str, seq: &[(u16, String)]) -> Self {
        let q: VecDeque<(u16, String)> = seq.iter().cloned().collect();
        self.0.push(Route { method: HttpMethod::Get, path: path.to_string(), reply: Reply::Seq(Arc::new(Mutex::new(q))) });
        self
    }
    fn fail(mut self, method: HttpMethod, path: &str, e: TransportError) -> Self {
        self.0.push(Route { method, path: path.to_string(), reply: Reply::Err(e) });
        self
    }
    fn with_header(mut self, name: &str, value: &str) -> Self {
        if let Some(Route { reply: Reply::Http(_, _, h), .. }) = self.0.last_mut() {
            h.push((name.to_string(), value.to_string()));
        }
        self
    }
    /// First matching route wins, so put the specific one first. An unrouted request is a test bug.
    fn install(self, t: &FakeTransport) {
        let routes = Mutex::new(self.0);
        t.set_handler_detailed(move |req: &HttpRequest| {
            let path = req.url.strip_prefix(PRACTICE_BASE_URL).unwrap_or_else(|| panic!("request left the practice host: {}", req.url));
            let routes = routes.lock().unwrap();
            for r in routes.iter() {
                if r.method == req.method && r.path == path {
                    return match &r.reply {
                        Reply::Http(status, body, headers) => Ok(HttpResponseDetailed { status: *status, body: body.clone(), headers: headers.clone() }),
                        Reply::Err(e) => Err(e.clone()),
                        Reply::Seq(q) => {
                            let mut q = q.lock().unwrap();
                            let (status, body) = if q.len() > 1 { q.pop_front().unwrap() } else { q.front().unwrap().clone() };
                            Ok(HttpResponseDetailed { status, body, headers: Vec::new() })
                        }
                    };
                }
            }
            panic!("unrouted request {:?} {path}", req.method)
        });
    }
}

fn p(tail: &str) -> String {
    format!("/v3/accounts/{ACCT}{tail}")
}

fn tag_lookup(tag_path: &str) -> String {
    p(&format!("/orders/{tag_path}"))
}

fn setup_with(f: impl FnOnce(OandaConfig) -> OandaConfig) -> (OandaAdapter, Arc<FakeTransport>) {
    let t = Arc::new(FakeTransport::new());
    let cfg = f(OandaConfig::practice(PRACTICE_BASE_URL).unwrap().with_restart_scan_window(WINDOW).unwrap());
    let creds = OandaCredentials::new(Environment::Practice, TOKEN, ACCT).unwrap();
    let a = OandaAdapter::new(cfg, creds, t.clone()).unwrap();
    a.set_instruments(InstrumentTable::from_instruments_json(fx!("instruments_ok.json")).unwrap());
    (a, t)
}

fn setup() -> (OandaAdapter, Arc<FakeTransport>) {
    setup_with(|c| c)
}

fn line(r: &HttpRequest) -> String {
    format!("{:?} {}", r.method, r.url.strip_prefix(PRACTICE_BASE_URL).expect("request went to the practice host"))
}

fn lines(t: &FakeTransport) -> Vec<String> {
    t.requests().iter().map(line).collect()
}

fn count(t: &FakeTransport, method: HttpMethod, path_prefix: &str) -> usize {
    t.requests().iter().filter(|r| r.method == method && line(r).contains(path_prefix)).count()
}

fn posts(t: &FakeTransport) -> Vec<HttpRequest> {
    t.requests().into_iter().filter(|r| r.method == HttpMethod::Post).collect()
}

fn body_json(r: &HttpRequest) -> serde_json::Value {
    serde_json::from_str(r.body.as_ref().expect("request has a body")).unwrap()
}

fn buy_req() -> OrderRequest {
    OrderRequest::market(TAG, "EUR/USD", Side::Buy, d("1000"))
}


/// `%40<tag>` with the tag percent-encoded like the adapter does (RFC 3986 unreserved characters stay).
fn pending_lookup(tag: &str) -> String {
    let mut out = String::from("%40");
    for b in tag.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    p(&format!("/orders/{out}"))
}

/// `GET /transactions/sinceid?id=<after>`.
fn scan_path(after: u64) -> String {
    p(&format!("/transactions/sinceid?id={after}"))
}

/// A COMPLETE sinceid page for ids `after+1 ..= last`: the given records where their ids match, an unrelated
/// `DAILY_FINANCING` transaction (no client id) everywhere else.
fn page(after: u64, last: u64, records: &[Value]) -> String {
    let mut out = Vec::new();
    for id in after + 1..=last {
        let want = id.to_string();
        match records.iter().find(|r| r["id"].as_str() == Some(want.as_str())) {
            Some(r) => out.push(r.clone()),
            None => out.push(json!({"id": want, "type": "DAILY_FINANCING", "financing": "0.0000"})),
        }
    }
    json!({"transactions": out, "lastTransactionID": last.to_string()}).to_string()
}

/// The authored account summary with another `lastTransactionID`.
fn summary_with_last(last: u64) -> String {
    let mut v: Value = serde_json::from_str(fx!("account_summary_ok.json")).unwrap();
    v["lastTransactionID"] = json!(last.to_string());
    v["account"]["lastTransactionID"] = json!(last.to_string());
    v.to_string()
}

fn real_json(body: &str) -> Value {
    serde_json::from_str(body).unwrap()
}

/// The two transactions of the RECORDED `transactions_sinceid` page (ids 51 and 52, tag `R_SCAN_TAG`).
fn real_txns() -> Vec<Value> {
    real_json(rfx!("transactions_sinceid"))["transactions"].as_array().unwrap().clone()
}

/// DERIVED from a recording: add the client id where OANDA puts it (`clientExtensions.id` on the create transaction,
/// `clientOrderID` on the fill) to a body that was recorded without one.
fn with_client_id(body: &str, tag: &str) -> String {
    let mut v: Value = serde_json::from_str(body).unwrap();
    for key in ["orderCreateTransaction", "longOrderCreateTransaction", "shortOrderCreateTransaction"] {
        if let Some(t) = v.get_mut(key) {
            t["clientExtensions"] = json!({"id": tag});
        }
    }
    for key in ["orderFillTransaction", "longOrderFillTransaction", "shortOrderFillTransaction"] {
        if let Some(t) = v.get_mut(key) {
            t["clientOrderID"] = json!(tag);
        }
    }
    v.to_string()
}

/// DERIVED from the recorded flat position (`GET /positions/EUR_USD` after EUR_USD was traded and closed: HTTP 200, both sides
/// at "0"): the same body for another instrument.
fn flat_position_body(inst: &str) -> String {
    sfx!("position_flat_previously_traded").replace("\"instrument\":\"EUR_USD\"", &format!("\"instrument\":\"{inst}\""))
}

/// AUTHORED (unmeasured shape): a single-position body.
fn position_body(inst: &str, long: &str, short: &str) -> String {
    let side = |u: &str| {
        if u == "0" {
            json!({"units": "0", "pl": "0.0000", "unrealizedPL": "0.0000"})
        } else {
            json!({"units": u, "averagePrice": "1.00000", "pl": "0.0000", "unrealizedPL": "0.0000"})
        }
    };
    json!({"position": {"instrument": inst, "pl": "0.0000", "unrealizedPL": "0.0000", "marginUsed": "1.0000", "long": side(long), "short": side(short)},
           "lastTransactionID": LAST.to_string()})
    .to_string()
}

/// AUTHORED: an EUR_USD-only instrument table with an optional `maximumPositionSize`.
fn table_with_cap(cap: Option<&str>) -> InstrumentTable {
    let mut row = json!({"name": "EUR_USD", "type": "CURRENCY", "displayName": "EUR/USD", "pipLocation": -4, "displayPrecision": 5,
        "tradeUnitsPrecision": 0, "minimumTradeSize": "1", "maximumOrderUnits": "100000000", "marginRate": "0.02"});
    if let Some(c) = cap {
        row["maximumPositionSize"] = json!(c);
    }
    InstrumentTable::from_instruments_json(&json!({"instruments": [row]}).to_string()).unwrap()
}

/// Routes for a placement whose POST answers `(status, body)`: account fine (checkpoint `LAST`), the recent-window scan
/// finds nothing, and the follow-up scan since the checkpoint (only used after an ambiguous answer) finds nothing.
fn place_routes(post_status: u16, post_body: &str) -> Routes {
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .get(&scan_path(LAST), 200, &page(LAST, LAST, &[]))
        .post(&p("/orders"), post_status, post_body)
}

/// Like [`place_routes`] but the POST fails in the transport.
fn place_routes_with_post_error(e: TransportError) -> Routes {
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .get(&scan_path(LAST), 200, &page(LAST, LAST, &[]))
        .fail(HttpMethod::Post, &p("/orders"), e)
}

fn place(post_status: u16, post_body: &str) -> (Result<PlaceOutcome, BrokerError>, Arc<FakeTransport>) {
    let (a, t) = setup();
    place_routes(post_status, post_body).install(&t);
    (a.place_order(&buy_req()), t)
}

fn expect_rejected(out: Result<PlaceOutcome, BrokerError>) -> Vec<broker_adapters::ExchangeError> {
    match out.unwrap() {
        PlaceOutcome::Rejected { errors, .. } => errors,
        other => panic!("expected Rejected, got {other:?}"),
    }
}

fn expect_unknown(out: Result<PlaceOutcome, BrokerError>) -> String {
    match out.unwrap() {
        PlaceOutcome::UnknownOutcome { reason, .. } => reason,
        other => panic!("expected UnknownOutcome, got {other:?}"),
    }
}

// ---------------------------------------------------------------- fixture hygiene

#[test]
fn every_json_fixture_is_labelled_as_authored_from_documentation() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/oanda");
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        // The deliberately truncated / non-JSON fixtures cannot carry a label.
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
        let label = v["_fixture_provenance"].as_str().unwrap_or_else(|| panic!("{path:?} has no provenance label"));
        assert_eq!(label, "authored from documentation, not recorded from a live account", "{path:?}");
        checked += 1;
    }
    assert!(checked > 60, "checked only {checked} fixtures");
}

// ---------------------------------------------------------------- reads: account, instruments, positions

#[test]
fn account_summary_is_parsed_exactly_and_the_request_is_well_formed() {
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 200, fx!("account_summary_ok.json")).install(&t);
    let s = a.get_account_summary().unwrap();
    assert_eq!(s.id, ACCT);
    assert_eq!(s.currency, "USD");
    assert_eq!(s.balance, d("100000.0000"));
    assert_eq!(s.nav, d("100250.5000"));
    assert_eq!(s.unrealized_pl, d("250.5000"));
    assert_eq!(s.margin_used, d("1100.0000"));
    assert_eq!(s.margin_available, d("99150.5000"));
    assert_eq!(s.position_value, Some(d("55000.0000")));
    assert_eq!(s.margin_closeout_percent, Some(d("0.00549")));
    assert!(!s.hedging_enabled);
    assert_eq!(s.open_position_count, Some(2));
    assert!(a.check_account(&s).is_ok());

    let reqs = t.requests();
    assert_eq!(lines(&t), [format!("Get {}", p("/summary"))]);
    assert_eq!(reqs[0].header("Authorization"), Some(format!("Bearer {TOKEN}").as_str()));
    assert_eq!(reqs[0].header("Accept"), Some("application/json"));
    assert_eq!(reqs[0].header("Accept-Datetime-Format"), Some("UNIX"));
    assert!(reqs[0].body.is_none());
}

#[test]
fn hedging_accounts_and_the_wrong_account_are_refused() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_hedging.json"))
        .install(&t);
    match a.verify_account() {
        Err(BrokerError::AccountBlocked(m)) => assert!(m.contains("hedgingEnabled"), "{m}"),
        other => panic!("{other:?}"),
    }
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 200, fx!("account_summary_other_account.json")).install(&t);
    match a.verify_account() {
        Err(BrokerError::Credentials(m)) => assert!(m.contains("101-001-7654321-001") && m.contains(ACCT), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_summary_missing_a_safety_field_or_truncated_fails_closed() {
    for (name, body) in [
        ("missing NAV", fx!("account_summary_missing_nav.json")),
        ("missing hedgingEnabled", fx!("account_summary_missing_hedging_flag.json")),
        ("truncated JSON", fx!("account_summary_truncated.json")),
        ("empty object", "{}"),
        ("not JSON", "<html>"),
    ] {
        let (a, t) = setup();
        Routes::default().get(&p("/summary"), 200, body).install(&t);
        assert!(matches!(a.get_account_summary(), Err(BrokerError::Malformed(_))), "{name}");
    }
}

#[test]
fn instruments_load_from_the_broker_list_and_one_bad_row_fails_the_table() {
    let (a, t) = setup();
    Routes::default().get(&p("/instruments"), 200, fx!("instruments_ok.json")).install(&t);
    assert_eq!(a.refresh_instruments().unwrap(), 6);
    let i = a.instrument("eur/usd").unwrap();
    assert_eq!((i.display_precision, i.trade_units_precision), (5, 0));
    assert_eq!((i.minimum_trade_size, i.maximum_order_units, i.margin_rate), (d("1"), d("100000000"), d("0.02")));
    assert_eq!(a.instrument("USD_JPY").unwrap().display_precision, 3);
    assert_eq!(a.instrument("DE30_EUR").unwrap().trade_units_precision, 1);
    assert_eq!(a.instruments().len(), 6);

    for body in [fx!("instruments_malformed_row.json"), fx!("instruments_bad_min.json")] {
        let (a, t) = setup();
        Routes::default().get(&p("/instruments"), 200, body).install(&t);
        let before = a.instruments().len();
        assert!(matches!(a.refresh_instruments(), Err(BrokerError::Malformed(_))));
        assert_eq!(a.instruments().len(), before, "a failed refresh leaves the old table alone");
    }
}

#[test]
fn positions_carry_signed_units_for_long_and_short() {
    let (a, t) = setup();
    Routes::default().get(&p("/openPositions"), 200, fx!("open_positions_ok.json")).install(&t);
    let pos = a.get_open_positions().unwrap();
    assert_eq!(pos.len(), 2);
    // sorted by instrument
    assert_eq!(pos[0].instrument, "EUR_USD");
    assert_eq!((pos[0].long_units, pos[0].short_units, pos[0].net_units()), (d("10000"), d("0"), d("10000")));
    assert_eq!(pos[0].long_average_price, Some(d("1.10000")));
    assert_eq!(pos[0].unrealized_pl, d("290.5000"));
    assert_eq!(pos[1].instrument, "GBP_USD");
    assert_eq!((pos[1].long_units, pos[1].short_units, pos[1].net_units()), (d("0"), d("-5000"), d("-5000")));
    assert_eq!(pos[1].short_average_price, Some(d("1.27000")));
    assert!(!pos[0].is_hedged() && !pos[1].is_hedged());
    assert_eq!(pos[1].unrealized_pl, d("-40.0000"));

    Routes::default().get(&p("/openPositions"), 200, fx!("open_positions_empty.json")).install(&t);
    assert!(a.get_open_positions().unwrap().is_empty());
    Routes::default().get(&p("/openPositions"), 200, fx!("open_positions_hedged.json")).install(&t);
    assert!(a.get_open_positions().unwrap()[0].is_hedged());
    Routes::default().get(&p("/openPositions"), 200, fx!("open_positions_bad_sign.json")).install(&t);
    assert!(matches!(a.get_open_positions(), Err(BrokerError::Malformed(m)) if m.contains("negative")));
}

#[test]
fn single_position_reads_and_a_404_means_no_position() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/positions/EUR_USD"), 200, fx!("position_single_long.json"))
        .get(&p("/positions/GBP_USD"), 200, fx!("position_single_short.json"))
        .get(&p("/positions/AUD_USD"), 404, sfx!("position_never_traded_404"))
        .install(&t);
    assert_eq!(a.get_position("EUR/USD").unwrap().unwrap().net_units(), d("10000"));
    assert_eq!(a.get_position("gbp_usd").unwrap().unwrap().net_units(), d("-5000"));
    assert!(a.get_position("AUD_USD").unwrap().is_none());
    assert!(matches!(a.get_position("nonsense"), Err(BrokerError::UnknownSymbol(_))));
    // asking for one instrument and receiving another is refused
    Routes::default().get(&p("/positions/GBP_USD"), 200, fx!("position_single_long.json")).install(&t);
    assert!(matches!(a.get_position("GBP_USD"), Err(BrokerError::Malformed(_))));
}

#[test]
fn balances_are_currency_balance_plus_net_units_by_canonical_symbol() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&p("/openPositions"), 200, fx!("open_positions_ok.json"))
        .install(&t);
    let b = a.get_balances().unwrap();
    assert_eq!(b.spot("USD"), d("100000.0000"));
    assert_eq!(b.spot("EUR/USD"), d("10000"));
    assert_eq!(b.spot("GBP/USD"), d("-5000"), "a short is a negative quantity");
    assert!(b.entries.iter().all(|e| e.kind == BalanceKind::Spot));
    assert_eq!(a.broker_name(), "oanda");
}

#[test]
fn balances_refuse_a_hedging_account_and_a_hedged_position() {
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 200, fx!("account_summary_hedging.json")).install(&t);
    assert!(matches!(a.get_balances(), Err(BrokerError::AccountBlocked(_))));
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&p("/openPositions"), 200, fx!("open_positions_hedged.json"))
        .install(&t);
    assert!(matches!(a.get_balances(), Err(BrokerError::Unsupported(_))));
}

// ---------------------------------------------------------------- pricing / quote

#[test]
fn quote_is_top_of_book_and_last_is_the_exact_mid() {
    let (a, t) = setup();
    let pricing_path = p("/pricing?instruments=EUR_USD&includeHomeConversions=true");
    Routes::default().get(&pricing_path, 200, fx!("pricing_ok.json")).install(&t);
    let q = a.get_quote("EUR/USD").unwrap();
    assert_eq!((q.symbol.as_str(), q.bid, q.ask, q.last), ("EUR/USD", d("1.10048"), d("1.10052"), d("1.10050")));
    assert_eq!(lines(&t), [format!("Get {pricing_path}")]);
}

#[test]
fn pricing_with_several_instruments_parses_prices_and_home_conversions() {
    let (a, t) = setup();
    let path = p("/pricing?instruments=EUR_USD%2CGBP_USD%2CUSD_JPY&includeHomeConversions=true");
    Routes::default().get(&path, 200, fx!("pricing_ok.json")).install(&t);
    let pr = a.get_pricing(&["eur/usd", "GBP_USD", "USDJPY"]).unwrap();
    assert_eq!(pr.prices.len(), 3);
    assert_eq!(pr.price("USD_JPY").unwrap().mid(), Some(d("148.505")));
    assert_eq!(pr.conversion("JPY").unwrap().position_value, d("0.006736"));
    assert_eq!(pr.conversion("usd").unwrap().position_value, d("1.0"));
    assert!(pr.price("AUD_USD").is_none() && pr.conversion("CHF").is_none());
    assert!(matches!(a.get_pricing(&[]), Err(BrokerError::InvalidRequest(_))));
}

#[test]
fn a_closed_crossed_or_missing_quote_is_refused() {
    let path = p("/pricing?instruments=EUR_USD&includeHomeConversions=true");
    let (a, t) = setup();
    Routes::default().get(&path, 200, fx!("pricing_closed.json")).install(&t);
    assert!(matches!(a.get_quote("EUR_USD"), Err(BrokerError::PairNotTradable { .. })));
    Routes::default().get(&path, 200, fx!("pricing_crossed.json")).install(&t);
    assert!(matches!(a.get_quote("EUR_USD"), Err(BrokerError::Malformed(m)) if m.contains("crossed")));
    Routes::default().get(&path, 200, fx!("pricing_missing_instrument.json")).install(&t);
    assert!(matches!(a.get_quote("EUR_USD"), Err(BrokerError::Malformed(_))));
}

// ---------------------------------------------------------------- reads: orders

#[test]
fn pending_orders_report_ours_and_foreign_and_unit_less_orders() {
    let (a, t) = setup();
    Routes::default().get(&p("/pendingOrders"), 200, fx!("pending_orders_ok.json")).install(&t);
    let o = a.open_orders().unwrap();
    assert_eq!(o.len(), 4);
    let ours = &o[0];
    assert_eq!(ours.broker_order_id, "6390");
    assert_eq!(ours.tag.as_deref(), Some("rb1:run1:EUR/USD:limit"));
    assert_eq!(ours.symbol, "EUR/USD");
    assert_eq!(ours.side, Some(Side::Buy));
    assert_eq!(ours.status, OrderStatus::Open);
    assert_eq!(ours.quantity, d("2000"));
    assert_eq!(ours.executed_quantity, Dec::ZERO);
    assert!(matches!(ours.kind, Some(OrderKind::Limit { price }) if price == d("1.09500")));
    assert_eq!(ours.avg_price, None);
    // a linked stop-loss has no units and no instrument
    let sl = &o[1];
    assert_eq!((sl.quantity, sl.side, sl.kind, sl.tag.clone()), (Dec::ZERO, None, None, None));
    assert_eq!(sl.raw_status, "PENDING/STOP_LOSS");
    // foreign: no client id / a foreign client id (no prefix filter configured -> reported verbatim)
    assert_eq!(o[2].tag, None);
    assert_eq!(o[2].side, Some(Side::Sell));
    assert_eq!(o[3].tag.as_deref(), Some("manual-ticket-9"));
    assert_eq!(o[3].symbol, "USD/JPY");
}

#[test]
fn own_tag_prefix_hides_the_tag_of_foreign_orders() {
    let (a, t) = setup_with(|c| c.with_own_tag_prefix("rb1:").unwrap());
    Routes::default().get(&p("/pendingOrders"), 200, fx!("pending_orders_ok.json")).install(&t);
    let o = a.open_orders().unwrap();
    assert_eq!(o[0].tag.as_deref(), Some("rb1:run1:EUR/USD:limit"));
    assert_eq!(o[3].tag, None, "manual-ticket-9 is foreign");
    Routes::default().get(&p("/pendingOrders"), 200, fx!("pending_orders_empty.json")).install(&t);
    assert!(a.open_orders().unwrap().is_empty());
}

#[test]
fn a_filled_market_order_is_completed_from_its_fill_transaction() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/orders/6372"), 200, fx!("order_filled_market.json"))
        .get(&p("/transactions/6373"), 200, fx!("txn_fill_buy_commission.json"))
        .install(&t);
    let r = a.get_order("6372").unwrap();
    assert_eq!(r.status, OrderStatus::Filled);
    assert_eq!(r.raw_status, "FILLED");
    assert_eq!((r.quantity, r.executed_quantity), (d("1000"), d("1000")));
    assert_eq!(r.avg_price, Some(d("1.10052")));
    assert_eq!(r.fee, Some(d("0.5000")));
    assert_eq!(r.cost, None, "OANDA reports no cost figure and none is invented");
    assert_eq!(r.side, Some(Side::Buy));
    assert!(matches!(r.kind, Some(OrderKind::Market)));
    assert_eq!(r.tag.as_deref(), Some(TAG));
    assert_eq!(r.symbol, "EUR/USD");
    assert_eq!(r.userref, None);
    assert!(r.open_time.is_some() && r.close_time.is_some());
    assert_eq!(lines(&t), [format!("Get {}", p("/orders/6372")), format!("Get {}", p("/transactions/6373"))]);
}

#[test]
fn a_filled_sell_reports_the_magnitude_and_the_sell_side() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/orders/6382"), 200, fx!("order_filled_market_sell.json"))
        .get(&p("/transactions/6383"), 200, fx!("txn_fill_sell.json"))
        .install(&t);
    let r = a.get_order("6382").unwrap();
    assert_eq!((r.side, r.quantity, r.executed_quantity, r.avg_price), (Some(Side::Sell), d("1000"), d("1000"), Some(d("1.09948"))));
    assert_eq!(r.status, OrderStatus::Filled);
}

#[test]
fn a_partial_fill_keeps_the_executed_quantity() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/orders/6372"), 200, fx!("order_filled_partial.json"))
        .get(&p("/transactions/6374"), 200, fx!("txn_fill_partial.json"))
        .install(&t);
    let r = a.get_order("6372").unwrap();
    assert_eq!(r.status, OrderStatus::PartiallyFilledThenCanceled);
    assert!(r.status.has_fills() && r.status.is_terminal());
    assert_eq!((r.quantity, r.executed_quantity), (d("1000"), d("400")));
}

#[test]
fn contradictory_fills_fail_closed() {
    // overfill: the rebalancer's flatten halts on this exact phrase
    let (a, t) = setup();
    Routes::default()
        .get(&p("/orders/6372"), 200, fx!("order_filled_overfill.json"))
        .get(&p("/transactions/6375"), 200, fx!("txn_fill_overfill.json"))
        .install(&t);
    assert!(matches!(a.get_order("6372"), Err(BrokerError::Malformed(m)) if m.contains("overfill anomaly")));
    // a fill that belongs to another order
    Routes::default()
        .get(&p("/orders/6372"), 200, fx!("order_filled_wrong_txn_order.json"))
        .get(&p("/transactions/6376"), 200, fx!("txn_fill_wrong_order.json"))
        .install(&t);
    assert!(matches!(a.get_order("6372"), Err(BrokerError::Malformed(m)) if m.contains("belongs to order")));
    // a fill in the opposite direction
    Routes::default()
        .get(&p("/orders/6372"), 200, fx!("order_filled_sign_flip.json"))
        .get(&p("/transactions/6377"), 200, fx!("txn_fill_sign_flip.json"))
        .install(&t);
    assert!(matches!(a.get_order("6372"), Err(BrokerError::Malformed(m)) if m.contains("opposite sign")));
    // FILLED but no filling transaction named
    Routes::default().get(&p("/orders/6372"), 200, fx!("order_filled_no_txn_id.json")).install(&t);
    assert!(matches!(a.get_order("6372"), Err(BrokerError::Malformed(m)) if m.contains("fillingTransactionID")));
    // a fill transaction without a price
    Routes::default()
        .get(&p("/orders/6372"), 200, fx!("order_filled_market.json"))
        .get(&p("/transactions/6373"), 200, fx!("txn_fill_no_price.json"))
        .install(&t);
    assert!(matches!(a.get_order("6372"), Err(BrokerError::Malformed(_))));
    // the transaction is not a fill at all
    Routes::default()
        .get(&p("/orders/6372"), 200, fx!("order_filled_market.json"))
        .get(&p("/transactions/6373"), 200, fx!("txn_cancel_client_request.json"))
        .install(&t);
    assert!(matches!(a.get_order("6372"), Err(BrokerError::Malformed(m)) if m.contains("ORDER_FILL")));
    // an unknown order state
    Routes::default().get(&p("/orders/6372"), 200, fx!("order_unknown_state.json")).install(&t);
    assert!(matches!(a.get_order("6372"), Err(BrokerError::Malformed(m)) if m.contains("REPLACED")));
}

#[test]
fn a_failure_to_read_the_fill_is_an_error_not_a_report_without_a_fill() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/orders/6372"), 200, fx!("order_filled_market.json"))
        .get(&p("/transactions/6373"), 500, fx!("error_500.json"))
        .install(&t);
    assert!(matches!(a.get_order("6372"), Err(BrokerError::Http(500))));
    Routes::default()
        .get(&p("/orders/6372"), 200, fx!("order_filled_market.json"))
        .fail(HttpMethod::Get, &p("/transactions/6373"), TransportError::Timeout)
        .install(&t);
    assert!(matches!(a.get_order("6372"), Err(BrokerError::Transport(TransportError::Timeout))));
}

#[test]
fn cancelled_orders_carry_the_cancel_reason_from_the_cancelling_transaction() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/orders/6390"), 200, fx!("order_cancelled_client_request.json"))
        .get(&p("/transactions/6391"), 200, fx!("txn_cancel_client_request.json"))
        .install(&t);
    let r = a.get_order("6390").unwrap();
    assert_eq!((r.status, r.reason.as_deref(), r.executed_quantity), (OrderStatus::Canceled, Some("CLIENT_REQUEST"), Dec::ZERO));
    assert_eq!(r.avg_price, None);

    Routes::default()
        .get(&p("/orders/6390"), 200, fx!("order_cancelled_expired.json"))
        .get(&p("/transactions/6392"), 200, fx!("txn_cancel_expired.json"))
        .install(&t);
    let r = a.get_order("6390").unwrap();
    assert_eq!((r.status, r.reason.as_deref()), (OrderStatus::Expired, Some("TIME_IN_FORCE_EXPIRED")));

    // No cancelling transaction named: still cancelled, reason unknown.
    Routes::default().get(&p("/orders/6390"), 200, fx!("order_cancelled_no_txn.json")).install(&t);
    let r = a.get_order("6390").unwrap();
    assert_eq!((r.status, r.reason), (OrderStatus::Canceled, None));

    // The cancelling transaction cannot be read: reason unknown, order still cancelled with nothing executed.
    Routes::default()
        .get(&p("/orders/6390"), 200, fx!("order_cancelled_client_request.json"))
        .get(&p("/transactions/6391"), 500, fx!("error_500.json"))
        .install(&t);
    let r = a.get_order("6390").unwrap();
    assert_eq!((r.status, r.reason, r.executed_quantity), (OrderStatus::Canceled, None, Dec::ZERO));
}

#[test]
fn a_pending_limit_order_is_open_with_its_price() {
    let (a, t) = setup();
    Routes::default().get(&p("/orders/6390"), 200, fx!("order_pending_limit.json")).install(&t);
    let r = a.get_order("6390").unwrap();
    assert_eq!(r.status, OrderStatus::Open);
    assert!(!r.status.is_terminal());
    assert!(matches!(r.kind, Some(OrderKind::Limit { price }) if price == d("1.09500")));
    assert_eq!(lines(&t).len(), 1, "no transaction is fetched for a live order");
}

#[test]
fn asking_for_one_order_and_receiving_another_is_refused() {
    let (a, t) = setup();
    Routes::default().get(&p("/orders/9999"), 200, fx!("order_pending_limit.json")).install(&t);
    assert!(matches!(a.get_order("9999"), Err(BrokerError::Malformed(m)) if m.contains("asked for order 9999")));
}

#[test]
fn order_ids_that_could_escape_the_path_are_refused_before_sending() {
    let (a, t) = setup();
    let too_long = "1".repeat(65);
    for bad in ["", "6390/../x", "6390?x=1", "63 90", "6390#", "%40tag", too_long.as_str()] {
        assert!(matches!(a.get_order(bad), Err(BrokerError::InvalidRequest(_))), "{bad:?}");
        assert!(matches!(a.cancel_order(bad), Err(BrokerError::InvalidRequest(_))), "{bad:?}");
        assert!(matches!(a.cancel_and_settle(bad), Err(BrokerError::InvalidRequest(_))), "{bad:?}");
    }
    assert_eq!(t.request_count(), 0);
}

// ---------------------------------------------------------------- placement: request

#[test]
fn a_market_buy_reads_the_summary_scans_the_recent_window_then_sends_exactly_one_post() {
    // The response is the RECORDED real one (place_filled); the checkpoint is the summary's lastTransactionID.
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(29))
        .get(&scan_path(29 - WINDOW), 200, &page(29 - WINDOW, 29, &[]))
        .post(&p("/orders"), 201, rfx!("place_filled"))
        .install(&t);
    match a.place_order(&OrderRequest::market(R_TAG, "EUR/USD", Side::Buy, d("1"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, sent, warnings, description } => {
            assert_eq!(broker_order_id, "30");
            assert_eq!((sent.broker_pair.as_str(), sent.side, sent.quantity, sent.price, sent.userref), ("EUR_USD", Side::Buy, d("1"), None, 0));
            assert!(warnings.is_empty(), "{warnings:?}");
            assert_eq!(description.as_deref(), Some("buy 1 EUR_USD market FOK"));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(lines(&t), [format!("Get {}", p("/summary")), format!("Get {}", scan_path(29 - WINDOW)), format!("Post {}", p("/orders"))]);
    let post = &posts(&t)[0];
    assert_eq!(post.header("Content-Type"), Some("application/json"));
    assert_eq!(post.header("Authorization"), Some(format!("Bearer {TOKEN}").as_str()));
    let expected: Value = serde_json::from_str(
        r#"{"order":{"type":"MARKET","instrument":"EUR_USD","units":"1","timeInForce":"FOK","positionFill":"DEFAULT",
            "clientExtensions":{"id":"lookup-filled-20260923T211402Z","tag":"mendl-rb"}}}"#,
    )
    .unwrap();
    assert_eq!(body_json(post), expected);
    assert_eq!(a.tag_checkpoint(R_TAG), Some(29), "the checkpoint of the first attempt stays registered");
}

#[test]
fn a_market_sell_sends_negative_units_and_the_recorded_short_fill_is_matched_to_it() {
    // DERIVED from the recorded sell_short: the recording sent no client id, so one is added to the create and fill exactly
    // where OANDA puts it in place_filled (clientExtensions.id / clientOrderID).
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(40))
        .get(&scan_path(40 - WINDOW), 200, &page(40 - WINDOW, 40, &[]))
        .post(&p("/orders"), 201, &with_client_id(rfx!("sell_short"), SELL_TAG))
        .install(&t);
    let out = a.place_order(&OrderRequest::market(SELL_TAG, "USD/JPY", Side::Sell, d("3"))).unwrap();
    assert!(matches!(out, PlaceOutcome::Accepted { ref broker_order_id, ref sent, .. } if broker_order_id == "41" && sent.side == Side::Sell && sent.quantity == d("3")), "{out:?}");
    let body = body_json(&posts(&t)[0]);
    assert_eq!(body["order"]["units"], "-3");
    assert_eq!(body["order"]["instrument"], "USD_JPY");
    assert_eq!(body["order"]["clientExtensions"]["id"], SELL_TAG);
}

#[test]
fn the_reference_price_never_turns_a_market_order_into_a_limit_order() {
    // Regression for the legacy connector's defect: a priced market signal became LIMIT/GTC.
    let (a, t) = setup();
    place_routes(201, rfx!("place_filled")).install(&t);
    let mut req = OrderRequest::market(R_TAG, "EUR/USD", Side::Buy, d("1"));
    req.reference_price = Some(d("1.10050"));
    a.place_order(&req).unwrap();
    let body = body_json(&posts(&t)[0]);
    assert_eq!(body["order"]["type"], "MARKET");
    assert_eq!(body["order"]["timeInForce"], "FOK");
    assert!(body["order"].get("price").is_none(), "{body}");
    assert!(!posts(&t)[0].body.as_ref().unwrap().contains("LIMIT"));
}

#[test]
fn a_limit_order_looks_for_a_pending_order_first_then_goes_out_as_limit_gtc_with_a_rounded_price() {
    // RECORDED: place_limit (create only), get_by_client_id_missing (404 NO_SUCH_ORDER on the pending lookup).
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(33))
        .get(&pending_lookup(R_LIMIT_TAG), 404, rfx!("get_by_client_id_missing"))
        .get(&scan_path(33 - WINDOW), 200, &page(33 - WINDOW, 33, &[]))
        .post(&p("/orders"), 201, rfx!("place_limit"))
        .install(&t);
    let req = OrderRequest::limit(R_LIMIT_TAG, "EUR/USD", Side::Buy, d("1"), d("0.500004"));
    match a.place_order(&req).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, sent, warnings, description } => {
            assert_eq!(broker_order_id, "34");
            assert_eq!(sent.price, Some(d("0.50000")));
            assert!(warnings.is_empty(), "{warnings:?}");
            assert_eq!(description.as_deref(), Some("buy 1 EUR_USD limit GTC"));
        }
        other => panic!("{other:?}"),
    }
    let body = body_json(&posts(&t)[0]);
    assert_eq!((body["order"]["type"].as_str(), body["order"]["price"].as_str(), body["order"]["timeInForce"].as_str()), (Some("LIMIT"), Some("0.50000"), Some("GTC")));
    // the pending lookup is for LIMIT orders only: a market order never asks for @tag
    assert_eq!(count(&t, HttpMethod::Get, "/orders/"), 1);
}

#[test]
fn a_market_order_never_asks_for_a_pending_order_by_tag() {
    let (a, t) = setup();
    place_routes(201, rfx!("place_filled")).install(&t);
    a.place_order(&OrderRequest::market(R_TAG, "EUR/USD", Side::Buy, d("1"))).unwrap();
    assert_eq!(count(&t, HttpMethod::Get, "/orders/"), 0, "OANDA finds only PENDING orders by client id, so it is pointless for FOK market orders");
}

#[test]
fn local_refusals_send_nothing_at_all() {
    let (a, t) = setup();
    let too_long = "t".repeat(129);
    let cases: Vec<(OrderRequest, &str)> = vec![
        (OrderRequest::market(TAG, "AUD_CAD", Side::Buy, d("1000")), "unknown instrument"),
        (OrderRequest::market(TAG, "EUR_USD", Side::Buy, d("0.5")), "rounds to zero"),
        (OrderRequest::market("", "EUR_USD", Side::Buy, d("1000")), "empty tag"),
        (OrderRequest::market(&too_long, "EUR_USD", Side::Buy, d("1000")), "a 129 character client id (OANDA: CLIENT_ORDER_ID_INVALID)"),
        (OrderRequest::market("caf\u{e9}", "EUR_USD", Side::Buy, d("1000")), "non-ASCII client id (stricter than OANDA, which accepted it)"),
        (OrderRequest::market(TAG, "EUR_USD", Side::Buy, d("0")), "zero quantity"),
        (OrderRequest::market(TAG, "nonsense", Side::Buy, d("1000")), "bad name"),
        ({
            let mut r = buy_req();
            r.time_in_force = Some(TimeInForce::Gtc);
            r
        }, "GTC market (OANDA: TIME_IN_FORCE_INVALID)"),
        ({
            let mut r = buy_req();
            r.validate_only = true;
            r
        }, "validate_only"),
        ({
            let mut r = buy_req();
            r.post_only = true;
            r
        }, "post_only"),
        (OrderRequest::market(TAG, "EUR_USD", Side::Buy, d("999999999999")), "above max (OANDA: UNITS_LIMIT_EXCEEDED)"),
    ];
    for (req, why) in cases {
        assert!(a.place_order(&req).is_err(), "{why}");
    }
    assert_eq!(t.request_count(), 0, "a locally refused order must not reach the transport");
}

#[test]
fn a_client_id_of_128_characters_with_the_measured_punctuation_is_accepted_locally() {
    let (a, _t) = setup();
    let id = format!("{}{}", "rb1:a.b-c_d/e f", "x".repeat(128 - 15));
    assert_eq!(id.len(), 128);
    let prepared = a.prepare(&OrderRequest::market(&id, "EUR_USD", Side::Buy, d("1"))).unwrap();
    assert_eq!(prepared.client_id, id, "never truncated or hashed");
}

#[test]
fn an_adapter_with_an_empty_instrument_table_cannot_trade_anything() {
    let t = Arc::new(FakeTransport::new());
    let a = OandaAdapter::new(
        OandaConfig::practice(PRACTICE_BASE_URL).unwrap(),
        OandaCredentials::new(Environment::Practice, TOKEN, ACCT).unwrap(),
        t.clone(),
    )
    .unwrap();
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::UnknownSymbol(_))));
    assert_eq!(t.request_count(), 0);
}

#[test]
fn own_tag_prefix_is_enforced_at_placement() {
    let (a, t) = setup_with(|c| c.with_own_tag_prefix("rb1:").unwrap());
    let req = OrderRequest::market("someone-else:1", "EUR_USD", Side::Buy, d("1000"));
    assert!(matches!(a.place_order(&req), Err(BrokerError::InvalidRequest(_))));
    assert_eq!(t.request_count(), 0);
}

#[test]
fn reduce_only_goes_out_as_position_fill_reduce_only() {
    let (a, t) = setup();
    place_routes(201, &with_client_id(rfx!("sell_short"), SELL_TAG)).install(&t);
    let mut req = OrderRequest::market(SELL_TAG, "USD_JPY", Side::Sell, d("3"));
    req.reduce_only = true;
    a.place_order(&req).unwrap();
    assert_eq!(body_json(&posts(&t)[0])["order"]["positionFill"], "REDUCE_ONLY");
}

// ---------------------------------------------------------------- placement: outcomes

#[test]
fn a_partial_fill_is_accepted_with_a_warning_naming_the_shortfall() {
    let (out, _) = place(201, fx!("create_market_partial_fill.json"));
    match out.unwrap() {
        PlaceOutcome::Accepted { warnings, .. } => assert_eq!(warnings, ["filled 400 of 1000 units"]),
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_order_cancelled_at_creation_is_a_definite_rejection_with_its_reason() {
    // AUTHORED bodies: the cancel-at-creation shape is not recorded.
    for (fixture, reason, class) in [
        (fx!("create_market_cancelled_margin.json"), "INSUFFICIENT_MARGIN", ErrorClass::InsufficientFunds),
        (fx!("create_market_cancelled_liquidity.json"), "INSUFFICIENT_LIQUIDITY", ErrorClass::OrderRejected),
        (fx!("create_market_cancelled_halted.json"), "MARKET_HALTED", ErrorClass::OrderRejected),
    ] {
        let (out, t) = place(201, fixture);
        let errs = expect_rejected(out);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].code.contains(reason) && errs[0].code.starts_with("oanda:201"), "{}", errs[0].code);
        assert_eq!(errs[0].class, class, "{reason}");
        assert_eq!(posts(&t).len(), 1);
    }
}

#[test]
fn the_measured_reject_reasons_map_to_the_right_class_and_never_retry() {
    // RECORDED bodies: HTTP 400, orderRejectTransaction, rejectReason == errorCode.
    for (fixture, reason) in [
        (rfx!("reject_too_big"), "UNITS_LIMIT_EXCEEDED"),
        (rfx!("reject_zero"), "UNITS_INVALID"),
        (rfx!("reject_market_gtc"), "TIME_IN_FORCE_INVALID"),
        (rfx!("reject_fractional"), "UNITS_PRECISION_EXCEEDED"),
    ] {
        let (out, t) = place(400, fixture);
        let errs = expect_rejected(out);
        assert!(errs[0].code.starts_with(&format!("oanda:400 {reason}:")), "{}", errs[0].code);
        assert_eq!(errs[0].class, ErrorClass::InvalidArguments, "{reason}");
        assert_eq!(posts(&t).len(), 1, "exactly one POST, never a retry");
        assert_eq!(follow_up_scans(&t), 0, "a refusal is definitive: no follow-up scan");
    }
}

/// Number of follow-up scans (`sinceid` from the checkpoint LAST) a test transport saw.
fn follow_up_scans(t: &FakeTransport) -> usize {
    t.requests().iter().filter(|r| r.url.ends_with(&scan_path(LAST))).count()
}

#[test]
fn an_unknown_instrument_is_a_definite_invalid_arguments_rejection_without_a_reject_transaction() {
    // RECORDED: HTTP 400 oanda::rest::core::InvalidParameterException, no reject transaction.
    let (out, t) = place(400, rfx!("reject_bad_instrument"));
    let errs = expect_rejected(out);
    assert!(errs[0].code.starts_with("oanda:400 oanda::rest::core::InvalidParameterException:"), "{}", errs[0].code);
    assert!(errs[0].code.contains("order.instrument"));
    assert_eq!(errs[0].class, ErrorClass::InvalidArguments);
    assert_eq!(posts(&t).len(), 1);
}

#[test]
fn a_definitive_refusal_leaves_the_tag_unused_so_the_same_tag_can_be_sent_again() {
    let (a, t) = setup();
    place_routes(400, rfx!("reject_zero")).install(&t);
    expect_rejected(a.place_order(&buy_req()));
    assert_eq!(a.tag_checkpoint(TAG), None, "a refusal created nothing: no checkpoint is kept");
    place_routes(201, fx!("create_market_buy_filled.json")).install(&t);
    assert!(matches!(a.place_order(&buy_req()).unwrap(), PlaceOutcome::Accepted { .. }));
    assert_eq!(posts(&t).len(), 2);
}

#[test]
fn a_400_with_authored_reject_bodies_is_a_definite_rejection() {
    for (fixture, reason, class) in [
        (fx!("error_400_insufficient_margin.json"), "INSUFFICIENT_MARGIN", ErrorClass::InsufficientFunds),
        (fx!("error_400_market_halted.json"), "MARKET_HALTED", ErrorClass::OrderRejected),
        (fx!("error_400_plain.json"), "Invalid value", ErrorClass::InvalidArguments),
    ] {
        let (out, t) = place(400, fixture);
        let errs = expect_rejected(out);
        assert!(errs[0].code.contains(reason) && errs[0].code.starts_with("oanda:400"), "{}", errs[0].code);
        assert_eq!(errs[0].class, class, "{reason}");
        assert_eq!(posts(&t).len(), 1, "exactly one POST, never a retry");
    }
}

#[test]
fn auth_failures_are_definite_rejections_of_class_auth() {
    for (status, body) in [(401, fx!("error_401.json")), (403, fx!("error_403.json"))] {
        let (out, _) = place(status, body);
        let errs = expect_rejected(out);
        assert_eq!(errs[0].class, ErrorClass::Auth);
        assert!(errs[0].code.starts_with(&format!("oanda:{status}")));
    }
}

#[test]
fn rate_limiting_means_not_sent_and_reports_retry_after() {
    let (a, t) = setup();
    place_routes(429, fx!("error_429.json")).with_header("Retry-After", "7").install(&t);
    match a.place_order(&buy_req()) {
        Err(BrokerError::RateLimited { retry_after_secs: Some(7), .. }) => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(posts(&t).len(), 1);
    assert_eq!(a.tag_checkpoint(TAG), None, "a 429 was refused before processing: nothing to look for later");
}

#[test]
fn server_errors_timeouts_and_ambiguous_answers_are_unknown_outcomes_never_retried_and_carry_the_checkpoint() {
    // 5xx and a non-2xx oddity: the follow-up scan (empty here) finds nothing, so the outcome stays unknown.
    for (status, body) in [(500, fx!("error_500.json")), (503, fx!("error_503_html.txt")), (502, ""), (404, fx!("error_404_account.json")), (418, "teapot")] {
        let (out, t) = place(status, body);
        let reason = expect_unknown(out);
        assert!(reason.contains(&status.to_string()), "{reason}");
        assert!(reason.contains(&format!("[oanda-tag-checkpoint={LAST}]")), "a later lookup needs the checkpoint: {reason}");
        assert_eq!(posts(&t).len(), 1, "HTTP {status}: a placement is never blindly retried");
        assert_eq!(follow_up_scans(&t), 1, "HTTP {status}: one follow-up scan since the checkpoint");
    }
    // transport failures after the request may have left
    for e in [TransportError::Timeout, TransportError::Io("connection reset".into())] {
        let (a, t) = setup();
        Routes::default()
            .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
            .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
            .fail(HttpMethod::Post, &p("/orders"), e.clone())
            .get(&scan_path(LAST), 200, &page(LAST, LAST, &[]))
            .install(&t);
        let reason = expect_unknown(a.place_order(&buy_req()));
        assert!(!reason.is_empty());
        assert_eq!(posts(&t).len(), 1, "{e:?}");
    }
    // a connect failure means the request never left: Err, nothing to look up, and the tag is not registered
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .fail(HttpMethod::Post, &p("/orders"), TransportError::ConnectFailed("refused".into()))
        .install(&t);
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::Transport(TransportError::ConnectFailed(_)))));
    assert_eq!(a.tag_checkpoint(TAG), None);
}

#[test]
fn a_success_that_cannot_be_trusted_is_an_unknown_outcome() {
    for (why, body) in [
        ("truncated JSON", fx!("create_json_but_truncated.json")),
        ("not JSON", "OK"),
        ("no create transaction", fx!("create_no_create_txn.json")),
        ("client id of someone else", fx!("create_wrong_client_id.json")),
        ("fill of another order", fx!("create_fill_wrong_order.json")),
        ("fill in the wrong direction", fx!("create_fill_wrong_direction.json")),
        ("cancel of another order", fx!("create_cancel_wrong_order.json")),
        ("empty object", "{}"),
    ] {
        let (out, t) = place(201, body);
        let reason = expect_unknown(out);
        assert!(!reason.is_empty(), "{why}");
        assert_eq!(posts(&t).len(), 1, "{why}");
    }
}

#[test]
fn a_market_order_with_neither_fill_nor_cancel_is_accepted_with_a_poll_warning() {
    let (out, _) = place(201, fx!("create_market_no_fill_no_cancel.json"));
    match out.unwrap() {
        PlaceOutcome::Accepted { broker_order_id, warnings, .. } => {
            assert_eq!(broker_order_id, "6372");
            assert_eq!(warnings.len(), 1);
            assert!(warnings[0].contains("poll get_order"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_http_200_is_read_like_a_201() {
    let (out, _) = place(200, fx!("create_market_buy_filled.json"));
    assert!(matches!(out.unwrap(), PlaceOutcome::Accepted { .. }));
}

// ---------------------------------------------------------------- placement: pre-checks

#[test]
fn account_trouble_before_the_post_means_nothing_was_sent() {
    // summary: 500
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 500, fx!("error_500.json")).install(&t);
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::Preflight(m)) if m.contains("500")));
    assert!(posts(&t).is_empty());
    // summary: timeout (must not surface as a bare Transport(Timeout) that looks like an unknown outcome)
    let (a, t) = setup();
    Routes::default().fail(HttpMethod::Get, &p("/summary"), TransportError::Timeout).install(&t);
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::Preflight(_))));
    assert!(posts(&t).is_empty());
    // summary: 401 -> an auth error, still nothing sent
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 401, fx!("error_401.json")).install(&t);
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::Exchange(e)) if e[0].class == ErrorClass::Auth));
    assert!(posts(&t).is_empty());
    // hedging account
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 200, fx!("account_summary_hedging.json")).install(&t);
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::AccountBlocked(_))));
    assert!(posts(&t).is_empty());
    // the wrong account answered
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 200, fx!("account_summary_other_account.json")).install(&t);
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::Credentials(_))));
    assert!(posts(&t).is_empty());
    // garbage summary
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 200, "{}").install(&t);
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::Preflight(_))));
    assert!(posts(&t).is_empty());
}

#[test]
fn a_summary_without_a_last_transaction_id_cannot_give_a_checkpoint_so_nothing_is_sent() {
    let (a, t) = setup();
    let mut v: Value = serde_json::from_str(fx!("account_summary_ok.json")).unwrap();
    v.as_object_mut().unwrap().remove("lastTransactionID");
    v["account"].as_object_mut().unwrap().remove("lastTransactionID");
    Routes::default().get(&p("/summary"), 200, &v.to_string()).post(&p("/orders"), 201, rfx!("place_filled")).install(&t);
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::Preflight(m)) if m.contains("lastTransactionID")));
    assert!(posts(&t).is_empty());
}

#[test]
fn a_tag_scan_that_cannot_be_completed_means_nothing_is_sent() {
    let ok_summary = |routes: Routes| Routes([Routes::default().get(&p("/summary"), 200, fx!("account_summary_ok.json")).0, routes.0].concat());
    let scan = scan_path(LAST - WINDOW);
    let short_page = {
        // a page that stops short of the account's last id (a server that truncates)
        let mut v: Value = serde_json::from_str(&page(LAST - WINDOW, LAST, &[])).unwrap();
        v["transactions"].as_array_mut().unwrap().truncate(4);
        v.to_string()
    };
    for (why, routes) in [
        ("500", Routes::default().get(&scan, 500, fx!("error_500.json"))),
        ("timeout", Routes::default().fail(HttpMethod::Get, &scan, TransportError::Timeout)),
        ("401", Routes::default().get(&scan, 401, fx!("error_401.json"))),
        ("not a page", Routes::default().get(&scan, 200, "{\"transactions\": 5}")),
        ("a page cut short by the server", Routes::default().get(&scan, 200, &short_page)),
        ("a page that starts too late", Routes::default().get(&scan, 200, &page(LAST - WINDOW + 2, LAST, &[]))),
    ] {
        let (a, t) = setup();
        ok_summary(routes).post(&p("/orders"), 201, fx!("create_market_buy_filled.json")).install(&t);
        match a.place_order(&buy_req()) {
            Err(BrokerError::Preflight(m)) => assert!(m.contains("nothing was sent"), "{why}: {m}"),
            other => panic!("{why}: {other:?}"),
        }
        assert!(posts(&t).is_empty(), "{why}: a tag we could not check must not be sent");
        assert_eq!(a.tag_checkpoint(TAG), None, "{why}");
    }
}

#[test]
fn a_limit_order_whose_pending_lookup_cannot_be_completed_sends_nothing() {
    for (why, routes) in [
        ("500", Routes::default().get(&pending_lookup(R_LIMIT_TAG), 500, fx!("error_500.json"))),
        ("timeout", Routes::default().fail(HttpMethod::Get, &pending_lookup(R_LIMIT_TAG), TransportError::Timeout)),
        ("401", Routes::default().get(&pending_lookup(R_LIMIT_TAG), 401, fx!("error_401.json"))),
        ("malformed order", Routes::default().get(&pending_lookup(R_LIMIT_TAG), 200, "{\"order\": 5}")),
        // a 404 that is NOT NO_SUCH_ORDER (an unknown account) must not be read as "no such order"
        ("404 of another kind", Routes::default().get(&pending_lookup(R_LIMIT_TAG), 404, fx!("error_404_account.json"))),
    ] {
        let (a, t) = setup();
        Routes([Routes::default().get(&p("/summary"), 200, fx!("account_summary_ok.json")).0, routes.0].concat())
            .post(&p("/orders"), 201, rfx!("place_limit"))
            .install(&t);
        let req = OrderRequest::limit(R_LIMIT_TAG, "EUR/USD", Side::Buy, d("1"), d("0.5"));
        match a.place_order(&req) {
            Err(BrokerError::Preflight(m)) => assert!(m.contains("nothing was sent"), "{why}: {m}"),
            other => panic!("{why}: {other:?}"),
        }
        assert!(posts(&t).is_empty(), "{why}");
    }
}

// ---------------------------------------------------------------- idempotency: the transaction-stream protocol

#[test]
fn a_lost_response_is_found_in_the_transaction_stream_and_the_order_is_not_placed_again() {
    // RECORDED: transactions_sinceid is exactly what OANDA returned after checkpoint 50 (a MARKET_ORDER and its ORDER_FILL,
    // both carrying the tag).
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(50))
        .get(&scan_path(50 - WINDOW), 200, &page(50 - WINDOW, 50, &[]))
        .fail(HttpMethod::Post, &p("/orders"), TransportError::Timeout)
        .get(&scan_path(50), 200, rfx!("transactions_sinceid"))
        .install(&t);
    let out = a.place_order(&OrderRequest::market(R_SCAN_TAG, "EUR/USD", Side::Buy, d("1"))).unwrap();
    match out {
        PlaceOutcome::Accepted { broker_order_id, warnings, .. } => {
            assert_eq!(broker_order_id, "51");
            assert!(warnings.is_empty(), "the order was found after THIS call's own POST: {warnings:?}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(posts(&t).len(), 1);
    // a careless second call with the same tag: the scan since the checkpoint finds it again; nothing is sent
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(52))
        .get(&scan_path(50), 200, rfx!("transactions_sinceid"))
        .install(&t);
    match a.place_order(&OrderRequest::market(R_SCAN_TAG, "EUR/USD", Side::Buy, d("1"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, warnings, .. } => {
            assert_eq!(broker_order_id, "51");
            assert!(warnings[0].contains("already existed"), "{warnings:?}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(posts(&t).len(), 1, "still exactly one POST in total");
}

#[test]
fn a_lost_request_is_replaced_exactly_once_only_after_a_scan_since_the_first_checkpoint_found_nothing() {
    let (a, t) = setup();
    // attempt 1: the POST times out; the follow-up scan (since 6400) finds nothing
    place_routes_with_post_error(TransportError::Timeout).install(&t);
    let reason = expect_unknown(a.place_order(&buy_req()));
    assert!(reason.ends_with(&format!("[oanda-tag-checkpoint={LAST}]")), "{reason}");
    assert_eq!(posts(&t).len(), 1);
    assert_eq!(a.tag_checkpoint(TAG), Some(LAST));
    // attempt 2 (the caller retries with the SAME tag): the account has moved on, but the scan still starts at the FIRST
    // checkpoint, finds nothing, and only then is the order sent again
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(LAST + 5))
        .get(&scan_path(LAST), 200, &page(LAST, LAST + 5, &[]))
        .post(&p("/orders"), 201, fx!("create_market_buy_filled.json"))
        .install(&t);
    assert!(matches!(a.place_order(&buy_req()).unwrap(), PlaceOutcome::Accepted { .. }));
    assert_eq!(posts(&t).len(), 2, "exactly one POST for each of the two attempts");
    let l = lines(&t);
    let scan_at = l.iter().rposition(|x| x.contains(&format!("sinceid?id={LAST}"))).expect("a scan since the first checkpoint");
    let post_at = l.iter().rposition(|x| x.starts_with("Post ")).expect("the second POST");
    assert!(scan_at < post_at, "the scan comes BEFORE the re-send: {l:?}");
    assert_eq!(a.tag_checkpoint(TAG), Some(LAST), "the checkpoint never moves forward");
}

#[test]
fn nothing_is_replaced_when_the_scan_since_the_checkpoint_cannot_prove_absence() {
    for (why, scan_reply) in [
        ("500", (500u16, fx!("error_500.json").to_string())),
        // the server says the account is at 6410 but returns nothing: an incomplete page proves nothing
        ("an empty page for a moved account", (200, json!({"transactions": [], "lastTransactionID": (LAST + 10).to_string()}).to_string())),
    ] {
        let (a, t) = setup();
        place_routes_with_post_error(TransportError::Timeout).install(&t);
        expect_unknown(a.place_order(&buy_req()));
        Routes::default()
            .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
            .get(&scan_path(LAST), scan_reply.0, &scan_reply.1)
            .post(&p("/orders"), 201, fx!("create_market_buy_filled.json"))
            .install(&t);
        let reason = expect_unknown(a.place_order(&buy_req()));
        assert!(reason.contains("earlier attempt") && reason.contains(&format!("[oanda-tag-checkpoint={LAST}]")), "{why}: {reason}");
        assert_eq!(posts(&t).len(), 1, "{why}: no second POST without a provable scan");
    }
}

#[test]
fn a_duplicate_tag_in_the_stream_is_an_alert_not_an_adoption() {
    // RECORDED: place_filled (orders 30/31) and duplicate_post (orders 32/33) carry the SAME client id. OANDA does not stop
    // the second fill, so if the stream shows two orders for one tag a human must look.
    let mut recs: Vec<Value> = Vec::new();
    for (body, key, fkey) in [(rfx!("place_filled"), "orderCreateTransaction", "orderFillTransaction"), (rfx!("duplicate_post"), "orderCreateTransaction", "orderFillTransaction")] {
        let v: Value = serde_json::from_str(body).unwrap();
        recs.push(v[key].clone());
        recs.push(v[fkey].clone());
    }
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(33))
        .get(&scan_path(33 - WINDOW), 200, &page(33 - WINDOW, 33, &recs))
        .post(&p("/orders"), 201, rfx!("place_filled"))
        .install(&t);
    let reason = expect_unknown(a.place_order(&OrderRequest::market(R_TAG, "EUR/USD", Side::Buy, d("1"))));
    assert!(reason.contains("DUPLICATE_TAG") && reason.contains("30") && reason.contains("32"), "{reason}");
    assert!(posts(&t).is_empty());
    // and the lookup reports BOTH orders
    Routes::default()
        .get(&pending_lookup(R_TAG), 404, rfx!("get_by_client_id_filled"))
        .get(&p("/summary"), 200, &summary_with_last(33))
        .get(&scan_path(33 - WINDOW), 200, &page(33 - WINDOW, 33, &recs))
        .install(&t);
    let found = a.find_orders_by_tag(R_TAG).unwrap();
    let ids: Vec<&str> = found.iter().map(|r| r.broker_order_id.as_str()).collect();
    assert_eq!(ids, ["30", "32"]);
    assert!(found.iter().all(|r| r.status == OrderStatus::Filled && r.executed_quantity == d("1")));
}

#[test]
fn restart_with_no_checkpoint_finds_an_earlier_fill_in_the_recent_window_and_sends_nothing() {
    // A brand-new adapter (no attempts registered), the tag was filled recently: it is found in the last WINDOW ids.
    let (a, t) = setup();
    let recs: Vec<Value> = real_txns();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(52))
        .get(&scan_path(52 - WINDOW), 200, &page(52 - WINDOW, 52, &recs))
        .install(&t);
    match a.place_order(&OrderRequest::market(R_SCAN_TAG, "EUR/USD", Side::Buy, d("1"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, warnings, description, .. } => {
            assert_eq!(broker_order_id, "51");
            assert!(warnings[0].contains("already existed"), "{warnings:?}");
            assert!(description.unwrap().contains("buy 1 EUR_USD"));
        }
        other => panic!("{other:?}"),
    }
    assert!(posts(&t).is_empty(), "restart idempotency: nothing may be sent twice");
    assert_eq!(a.tag_checkpoint(R_SCAN_TAG), None, "nothing was sent, so no checkpoint was taken");
}

#[test]
fn a_tag_that_belongs_to_a_different_order_is_refused_not_adopted() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(52))
        .get(&scan_path(52 - WINDOW), 200, &page(52 - WINDOW, 52, &real_txns()))
        .install(&t);
    // same tag, other quantity
    let errs = expect_rejected(a.place_order(&OrderRequest::market(R_SCAN_TAG, "EUR/USD", Side::Buy, d("2"))));
    assert!(errs[0].code.contains("differs from this request") && errs[0].code.contains("51"), "{}", errs[0].code);
    assert_eq!(errs[0].class, ErrorClass::InvalidArguments);
    // other side
    let errs = expect_rejected(a.place_order(&OrderRequest::market(R_SCAN_TAG, "EUR/USD", Side::Sell, d("1"))));
    assert!(errs[0].code.contains("differs"), "{}", errs[0].code);
    assert!(posts(&t).is_empty());
}

#[test]
fn a_tag_whose_order_was_cancelled_is_refused_with_use_a_new_tag() {
    // RECORDED transactions: place_limit's LIMIT_ORDER (34) and cancel_by_client_id's ORDER_CANCEL (35), same client id.
    let create = real_json(rfx!("place_limit"))["orderCreateTransaction"].clone();
    let cancel = real_json(rfx!("cancel_by_client_id"))["orderCancelTransaction"].clone();
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(35))
        .get(&pending_lookup(R_LIMIT_TAG), 404, rfx!("get_limit_cancelled"))
        .get(&scan_path(35 - WINDOW), 200, &page(35 - WINDOW, 35, &[create, cancel]))
        .install(&t);
    let req = OrderRequest::limit(R_LIMIT_TAG, "EUR/USD", Side::Buy, d("1"), d("0.5"));
    let errs = expect_rejected(a.place_order(&req));
    assert!(errs[0].code.contains("use a new tag") && errs[0].code.contains("34") && errs[0].code.contains("CLIENT_REQUEST"), "{}", errs[0].code);
    assert!(posts(&t).is_empty());
}

#[test]
fn a_resting_limit_order_with_the_tag_is_adopted_through_the_pending_lookup_even_when_it_is_older_than_the_window() {
    // RECORDED: get_limit_pending (order 34, PENDING). The scan window would not reach it; the pending lookup does.
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(5000))
        .get(&pending_lookup(R_LIMIT_TAG), 200, rfx!("get_limit_pending"))
        .install(&t);
    let req = OrderRequest::limit(R_LIMIT_TAG, "EUR/USD", Side::Buy, d("1"), d("0.5"));
    match a.place_order(&req).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, warnings, .. } => {
            assert_eq!(broker_order_id, "34");
            assert!(warnings[0].contains("already existed"));
        }
        other => panic!("{other:?}"),
    }
    assert!(posts(&t).is_empty());
    // a resting order for a different request is refused
    let req = OrderRequest::limit(R_LIMIT_TAG, "EUR/USD", Side::Buy, d("9"), d("0.5"));
    assert!(expect_rejected(a.place_order(&req))[0].code.contains("differs"));
}

#[test]
fn a_seeded_checkpoint_gives_exact_coverage_after_a_restart() {
    // The caller persisted the checkpoint (from the UnknownOutcome reason / tag_checkpoint) and restores it.
    let (a, t) = setup();
    a.seed_tag_checkpoint(R_SCAN_TAG, 50);
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(52))
        .get(&scan_path(50), 200, rfx!("transactions_sinceid"))
        .install(&t);
    assert!(matches!(a.place_order(&OrderRequest::market(R_SCAN_TAG, "EUR/USD", Side::Buy, d("1"))).unwrap(), PlaceOutcome::Accepted { .. }));
    assert!(posts(&t).is_empty());
    // seeding never moves a checkpoint forward
    a.seed_tag_checkpoint(R_SCAN_TAG, 80);
    assert_eq!(a.tag_checkpoint(R_SCAN_TAG), Some(50));
    a.seed_tag_checkpoint(R_SCAN_TAG, 40);
    assert_eq!(a.tag_checkpoint(R_SCAN_TAG), Some(40));
}

#[test]
fn a_window_that_reaches_the_start_of_the_account_is_a_proof_and_a_shorter_one_is_not() {
    // account at 6: window 10 reaches back to the creation transaction -> full history
    let (a, t) = setup_with(|c| c.with_strict_unseen_tags(true));
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(6))
        .get(&scan_path(1), 200, &page(1, 6, &[]))
        .post(&p("/orders"), 201, fx!("create_market_buy_filled.json"))
        .install(&t);
    assert!(matches!(a.place_order(&buy_req()).unwrap(), PlaceOutcome::Accepted { .. }), "strict mode still sends when the whole history was scanned");
    // account at 6400: only the recent window was scanned -> strict mode refuses to call the tag unused
    let (a, t) = setup_with(|c| c.with_strict_unseen_tags(true));
    place_routes(201, fx!("create_market_buy_filled.json")).install(&t);
    let reason = expect_unknown(a.place_order(&buy_req()));
    assert!(reason.contains("strict mode") && reason.contains("nothing was sent"), "{reason}");
    assert!(posts(&t).is_empty());
    // the same lookup by tag is inconclusive, not "not found"
    Routes::default()
        .get(&pending_lookup(TAG), 404, rfx!("get_by_client_id_missing"))
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .install(&t);
    assert!(matches!(a.find_orders_by_tag(TAG), Err(BrokerError::LookupInconclusive(_))));
    // strict mode off (the default): the same scan is "not found in the recent window"
    let (a, t) = setup();
    Routes::default()
        .get(&pending_lookup(TAG), 404, rfx!("get_by_client_id_missing"))
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .install(&t);
    assert!(a.find_orders_by_tag(TAG).unwrap().is_empty());
}

#[test]
fn lookup_by_tag_scans_since_the_checkpoint_when_this_process_sent_the_tag_and_needs_no_summary() {
    let (a, t) = setup();
    place_routes_with_post_error(TransportError::Timeout).install(&t);
    expect_unknown(a.place_order(&buy_req()));
    Routes::default()
        .get(&pending_lookup(TAG), 404, rfx!("get_by_client_id_missing"))
        .get(&scan_path(LAST), 200, &page(LAST, LAST + 3, &[]))
        .install(&t);
    assert!(a.find_orders_by_tag(TAG).unwrap().is_empty(), "provably nothing since the first attempt's checkpoint");
    let tail: Vec<String> = lines(&t).into_iter().rev().take(2).collect();
    assert!(tail.iter().all(|l| !l.contains("/summary")), "{tail:?}");
}

#[test]
fn lookup_by_tag_reports_what_the_scan_found_and_what_is_resting() {
    // filled market order found in the recent window (RECORDED transactions)
    let (a, t) = setup();
    Routes::default()
        .get(&pending_lookup(R_SCAN_TAG), 404, rfx!("get_by_client_id_filled"))
        .get(&p("/summary"), 200, &summary_with_last(52))
        .get(&scan_path(52 - WINDOW), 200, &page(52 - WINDOW, 52, &real_txns()))
        .install(&t);
    let found = a.find_orders_by_tag(R_SCAN_TAG).unwrap();
    assert_eq!(found.len(), 1);
    let r = &found[0];
    assert_eq!((r.broker_order_id.as_str(), r.status, r.tag.as_deref(), r.side), ("51", OrderStatus::Filled, Some(R_SCAN_TAG), Some(Side::Buy)));
    assert_eq!((r.quantity, r.executed_quantity, r.avg_price), (d("1"), d("1"), Some(d("1.13846"))));
    assert!(matches!(r.kind, Some(OrderKind::Market)));

    // a resting limit order (RECORDED get_limit_pending) is found by the pending lookup even though the scan sees nothing
    let (a, t) = setup();
    Routes::default()
        .get(&pending_lookup(R_LIMIT_TAG), 200, rfx!("get_limit_pending"))
        .get(&p("/summary"), 200, &summary_with_last(5000))
        .get(&scan_path(5000 - WINDOW), 200, &page(5000 - WINDOW, 5000, &[]))
        .install(&t);
    let found = a.find_orders_by_tag(R_LIMIT_TAG).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!((found[0].broker_order_id.as_str(), found[0].status), ("34", OrderStatus::Open));
    // ... and even when the history cannot be read
    Routes::default()
        .get(&pending_lookup(R_LIMIT_TAG), 200, rfx!("get_limit_pending"))
        .get(&p("/summary"), 200, &summary_with_last(5000))
        .get(&scan_path(5000 - WINDOW), 200, fx!("error_500.json"))
        .install(&t);
    assert_eq!(a.find_orders_by_tag(R_LIMIT_TAG).unwrap().len(), 1);
}

#[test]
fn lookup_by_tag_is_inconclusive_never_empty_when_the_scan_is_incomplete_and_passes_other_errors_through() {
    let (a, t) = setup();
    let mut cut: Value = serde_json::from_str(&page(LAST - WINDOW, LAST, &[])).unwrap();
    cut["transactions"].as_array_mut().unwrap().truncate(3);
    Routes::default()
        .get(&pending_lookup(TAG), 404, rfx!("get_by_client_id_missing"))
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &cut.to_string())
        .install(&t);
    assert!(matches!(a.find_orders_by_tag(TAG), Err(BrokerError::LookupInconclusive(m)) if m.contains("incomplete")));
    // a pending lookup that fails is an error, not "no order"
    Routes::default().get(&pending_lookup(TAG), 500, fx!("error_500.json")).install(&t);
    assert!(matches!(a.find_orders_by_tag(TAG), Err(BrokerError::Http(500))));
    Routes::default().get(&pending_lookup(TAG), 401, fx!("error_401.json")).install(&t);
    assert!(matches!(a.find_orders_by_tag(TAG), Err(BrokerError::Exchange(e)) if e[0].class == ErrorClass::Auth));
    // an unusable tag is refused before anything is sent
    let before = t.request_count();
    let too_long = "x".repeat(129);
    for bad in ["", too_long.as_str(), "caf\u{e9}"] {
        assert!(matches!(a.find_orders_by_tag(bad), Err(BrokerError::InvalidRequest(_))));
    }
    assert_eq!(t.request_count(), before);
}

#[test]
fn the_pending_lookup_percent_encodes_the_at_sign_and_reserved_characters() {
    let (a, t) = setup();
    Routes::default().get(&tag_lookup(TAG_PATH), 404, rfx!("get_by_client_id_missing")).install(&t);
    assert!(a.get_pending_order_by_tag(TAG).unwrap().is_none());
    let url = &t.requests()[0].url;
    assert!(url.ends_with(&format!("/orders/{TAG_PATH}")), "{url}");
    assert!(!url.contains('@') && !url[PRACTICE_BASE_URL.len()..].contains(':'));
    let decoded = TAG_PATH.replace("%40", "@").replace("%3A", ":").replace("%2F", "/");
    assert_eq!(decoded, format!("@{TAG}"));
    // a lookup that returns someone else's order is refused
    Routes::default()
        .get(&tag_lookup(TAG_PATH), 200, fx!("order_other_client_id.json"))
        .get(&p("/transactions/6373"), 200, fx!("txn_fill_buy.json"))
        .install(&t);
    assert!(matches!(a.get_pending_order_by_tag(TAG), Err(BrokerError::Malformed(m)) if m.contains("does not carry")));
}

// ---------------------------------------------------------------- maximumPositionSize

fn cap_adapter(cap: Option<&str>) -> (OandaAdapter, Arc<FakeTransport>) {
    let (a, t) = setup();
    a.set_instruments(table_with_cap(cap));
    (a, t)
}

fn cap_routes(net_long: &str, net_short: &str) -> Routes {
    place_routes(201, fx!("create_market_buy_filled.json")).get(&p("/positions/EUR_USD"), 200, &position_body("EUR_USD", net_long, net_short))
}

/// Send one order and say whether the POST left. `Err(InvalidRequest)` naming maximumPositionSize is the cap refusal.
fn try_order(a: &OandaAdapter, t: &FakeTransport, side: Side, units: &str) -> (Result<PlaceOutcome, BrokerError>, bool) {
    let before = posts(t).len();
    let out = a.place_order(&OrderRequest::market(TAG, "EUR_USD", side, d(units)));
    (out, posts(t).len() > before)
}

fn assert_cap_refused(r: (Result<PlaceOutcome, BrokerError>, bool), what: &str) {
    match r {
        (Err(BrokerError::InvalidRequest(m)), false) => assert!(m.contains("maximumPositionSize") && m.contains("refusing to truncate"), "{what}: {m}"),
        other => panic!("{what}: expected the cap refusal and no POST, got {other:?}"),
    }
}

fn assert_sent(r: (Result<PlaceOutcome, BrokerError>, bool), what: &str) {
    assert!(r.1, "{what}: the POST should have left, got {:?}", r.0);
    assert!(!matches!(r.0, Err(BrokerError::InvalidRequest(_))), "{what}: {:?}", r.0);
}

#[test]
fn maximum_position_size_zero_or_absent_means_no_cap_and_the_position_is_not_even_read() {
    for cap in [Some("0"), Some("0.0"), None] {
        let (a, t) = cap_adapter(cap);
        assert_eq!(a.instrument("EUR_USD").unwrap().maximum_position_size, None, "{cap:?}: zero / absent parse to NO cap, never a zero limit");
        cap_routes("1000000000", "0").install(&t);
        let (out, posted) = try_order(&a, &t, Side::Buy, "99999999");
        assert!(matches!(out.unwrap(), PlaceOutcome::Accepted { .. }), "{cap:?}");
        assert!(posted);
        assert_eq!(count(&t, HttpMethod::Get, "/positions/"), 0, "{cap:?}: no cap, so no extra read");
    }
}

#[test]
fn a_positive_maximum_position_size_refuses_an_order_that_would_exceed_it_and_never_truncates() {
    let (a, t) = cap_adapter(Some("5000"));
    assert_eq!(a.instrument("EUR_USD").unwrap().maximum_position_size, Some(d("5000")));
    // long 4000: buying 1000 reaches exactly the cap (allowed), buying 1001 does not
    cap_routes("4000", "0").install(&t);
    assert_sent(try_order(&a, &t, Side::Buy, "1000"), "buy to exactly the cap");
    assert_cap_refused(try_order(&a, &t, Side::Buy, "1001"), "buy past the cap");
    // short side: net -4500, selling 500 reaches -5000 (allowed), 501 crosses -5001
    cap_routes("0", "-4500").install(&t);
    assert_sent(try_order(&a, &t, Side::Sell, "500"), "sell to exactly the cap");
    assert_cap_refused(try_order(&a, &t, Side::Sell, "501"), "sell past the cap");
    // flipping through zero counts the resulting net: long 4000, sell 9500 -> net -5500
    cap_routes("4000", "0").install(&t);
    assert_cap_refused(try_order(&a, &t, Side::Sell, "9500"), "flip past the cap on the other side");
    assert_sent(try_order(&a, &t, Side::Sell, "9000"), "flip to exactly -5000");
    // a 404 on the position read means flat
    place_routes(201, fx!("create_market_buy_filled.json")).get(&p("/positions/EUR_USD"), 404, sfx!("position_never_traded_404")).install(&t);
    assert_sent(try_order(&a, &t, Side::Buy, "5000"), "from flat to the cap");
    assert_cap_refused(try_order(&a, &t, Side::Buy, "5001"), "from flat past the cap");
    assert_eq!(a.tag_checkpoint("never-used"), None);
}

#[test]
fn reducing_a_position_that_is_already_over_the_cap_is_allowed() {
    let (a, t) = cap_adapter(Some("5000"));
    cap_routes("6000", "0").install(&t);
    assert_sent(try_order(&a, &t, Side::Sell, "500"), "a reduction from an over-cap position");
    assert_cap_refused(try_order(&a, &t, Side::Buy, "1"), "growing an over-cap position");
}

#[test]
fn a_position_read_failure_with_a_cap_means_nothing_is_sent() {
    let (a, t) = cap_adapter(Some("5000"));
    place_routes(201, fx!("create_market_buy_filled.json")).get(&p("/positions/EUR_USD"), 500, fx!("error_500.json")).install(&t);
    assert!(matches!(a.place_order(&buy_req()), Err(BrokerError::Preflight(m)) if m.contains("nothing was sent")));
    assert!(posts(&t).is_empty());
}

#[test]
fn a_negative_or_garbage_maximum_position_size_fails_the_instrument_table() {
    for bad in ["-5", "lots"] {
        let row = json!({"name":"EUR_USD","type":"CURRENCY","displayPrecision":5,"tradeUnitsPrecision":0,"minimumTradeSize":"1",
            "maximumOrderUnits":"100000000","marginRate":"0.02","maximumPositionSize": bad});
        assert!(matches!(InstrumentTable::from_instruments_json(&json!({"instruments":[row]}).to_string()), Err(BrokerError::Malformed(_))), "{bad}");
    }
}

// ---------------------------------------------------------------- get_order: the transaction-stream fallback

#[test]
fn get_order_rebuilds_a_finished_order_from_the_stream_when_the_broker_will_not_serve_it_by_id() {
    // RECORDED transactions of the cancelled limit order: 34 LIMIT_ORDER, 35 ORDER_CANCEL (CLIENT_REQUEST), 36 ORDER_CANCEL_REJECT.
    let create = real_json(rfx!("place_limit"))["orderCreateTransaction"].clone();
    let cancel = real_json(rfx!("cancel_by_client_id"))["orderCancelTransaction"].clone();
    let cancel_reject = real_json(rfx!("cancel_again"))["orderCancelRejectTransaction"].clone();
    let (a, t) = setup();
    Routes::default()
        .get(&p("/orders/34"), 404, rfx!("get_by_client_id_missing"))
        .get(&scan_path(33), 200, &page(33, 36, &[create, cancel, cancel_reject]))
        .install(&t);
    let r = a.get_order("34").unwrap();
    assert_eq!((r.status, r.reason.as_deref(), r.executed_quantity, r.tag.as_deref()), (OrderStatus::Canceled, Some("CLIENT_REQUEST"), Dec::ZERO, Some(R_LIMIT_TAG)));
    assert!(matches!(r.kind, Some(OrderKind::Limit { price }) if price == d("0.50000")));

    // and a filled market order (RECORDED place_filled): the fill comes from the same page
    let v = real_json(rfx!("place_filled"));
    let (a, t) = setup();
    Routes::default()
        .get(&p("/orders/30"), 404, rfx!("get_by_client_id_missing"))
        .get(&scan_path(29), 200, &page(29, 31, &[v["orderCreateTransaction"].clone(), v["orderFillTransaction"].clone()]))
        .install(&t);
    let r = a.get_order("30").unwrap();
    assert_eq!((r.status, r.executed_quantity, r.avg_price), (OrderStatus::Filled, d("1"), Some(d("1.13835"))));

    // an id that is in no page is NotFound (so cancel_and_settle can say CancelTargetNotFound)
    let (a, t) = setup();
    Routes::default()
        .get(&p("/orders/999"), 404, rfx!("get_by_client_id_missing"))
        .get(&scan_path(998), 200, &page(998, 998, &[]))
        .install(&t);
    assert!(matches!(a.get_order("999"), Err(BrokerError::NotFound(_))));
    // an incomplete stream is never read as a finished order
    let (a, t) = setup();
    let mut cut: Value = serde_json::from_str(&page(33, 36, &[])).unwrap();
    cut["transactions"].as_array_mut().unwrap().truncate(1);
    Routes::default()
        .get(&p("/orders/34"), 404, rfx!("get_by_client_id_missing"))
        .get(&scan_path(33), 200, &cut.to_string())
        .install(&t);
    // the first record of the cut page is a filler, not the order, so it is "not found", not a wrong report
    assert!(a.get_order("34").is_err());
}
// ---------------------------------------------------------------- cancel

#[test]
fn cancel_puts_to_the_cancel_endpoint_and_reports_a_synchronous_cancel() {
    // RECORDED 200 body (cancel_by_client_id): orderCancelTransaction, reason CLIENT_REQUEST.
    let (a, t) = setup();
    Routes::default().put(&p("/orders/34/cancel"), 200, rfx!("cancel_by_client_id")).install(&t);
    let out = a.cancel_order("34").unwrap();
    assert_eq!((out.canceled_count, out.pending), (1, false));
    assert_eq!(lines(&t), [format!("Put {}", p("/orders/34/cancel"))]);
    assert!(t.requests()[0].body.is_none());
    assert_eq!(t.requests()[0].header("Authorization"), Some(format!("Bearer {TOKEN}").as_str()));
}

#[test]
fn cancel_errors_are_classified() {
    let (a, t) = setup();
    // RECORDED: cancelling again is HTTP 404 ORDER_DOESNT_EXIST with an orderCancelRejectTransaction
    Routes::default().put(&p("/orders/34/cancel"), 404, rfx!("cancel_again")).install(&t);
    assert!(matches!(a.cancel_order("34"), Err(BrokerError::NotFound(_))));
    Routes::default().put(&p("/orders/34/cancel"), 401, fx!("error_401.json")).install(&t);
    assert!(matches!(a.cancel_order("34"), Err(BrokerError::Exchange(e)) if e[0].class == ErrorClass::Auth));
    Routes::default().put(&p("/orders/34/cancel"), 429, fx!("error_429.json")).with_header("Retry-After", "3").install(&t);
    assert!(matches!(a.cancel_order("34"), Err(BrokerError::RateLimited { retry_after_secs: Some(3), .. })));
    Routes::default().put(&p("/orders/34/cancel"), 503, fx!("error_503_html.txt")).install(&t);
    assert!(matches!(a.cancel_order("34"), Err(BrokerError::Http(503))));
    Routes::default().put(&p("/orders/34/cancel"), 200, fx!("cancel_no_txn.json")).install(&t);
    assert!(matches!(a.cancel_order("34"), Err(BrokerError::Malformed(_))));
    Routes::default().fail(HttpMethod::Put, &p("/orders/34/cancel"), TransportError::Timeout).install(&t);
    assert!(matches!(a.cancel_order("34"), Err(BrokerError::Transport(TransportError::Timeout))));
}

#[test]
fn cancel_and_settle_returns_the_order_as_it_ended() {
    let (a, t) = setup();
    Routes::default()
        .put(&p("/orders/6390/cancel"), 200, fx!("cancel_ok.json"))
        .get(&p("/orders/6390"), 200, fx!("order_cancelled_client_request.json"))
        .get(&p("/transactions/6391"), 200, fx!("txn_cancel_client_request.json"))
        .install(&t);
    let (out, report) = a.cancel_and_settle("6390").unwrap();
    assert_eq!((out.canceled_count, out.pending, report.status), (1, false, OrderStatus::Canceled));
}

#[test]
fn cancel_and_settle_survives_a_broker_that_will_not_serve_the_cancelled_order_by_id() {
    // RECORDED: the cancel 200, then the order is read back; if GET /orders/34 is a 404 the stream (34, 35) answers.
    let create = real_json(rfx!("place_limit"))["orderCreateTransaction"].clone();
    let cancel = real_json(rfx!("cancel_by_client_id"))["orderCancelTransaction"].clone();
    let (a, t) = setup();
    Routes::default()
        .put(&p("/orders/34/cancel"), 200, rfx!("cancel_by_client_id"))
        .get(&p("/orders/34"), 404, rfx!("get_by_client_id_missing"))
        .get(&scan_path(33), 200, &page(33, 35, &[create, cancel]))
        .install(&t);
    let (out, report) = a.cancel_and_settle("34").unwrap();
    assert_eq!((out.canceled_count, report.status, report.reason.as_deref()), (1, OrderStatus::Canceled, Some("CLIENT_REQUEST")));
}

#[test]
fn cancel_of_an_order_that_already_filled_reports_the_fill_with_a_zero_cancel_count() {
    let (a, t) = setup();
    Routes::default()
        .put(&p("/orders/6372/cancel"), 404, rfx!("cancel_again"))
        .get(&p("/orders/6372"), 200, fx!("order_filled_market.json"))
        .get(&p("/transactions/6373"), 200, fx!("txn_fill_buy.json"))
        .install(&t);
    let (out, report) = a.cancel_and_settle("6372").unwrap();
    assert_eq!((out.canceled_count, out.pending), (0, false));
    assert_eq!((report.status, report.executed_quantity), (OrderStatus::Filled, d("1000")));
}

#[test]
fn cancel_of_an_unknown_order_is_target_not_found() {
    let (a, t) = setup();
    Routes::default()
        .put(&p("/orders/9999/cancel"), 404, rfx!("cancel_again"))
        .get(&p("/orders/9999"), 404, rfx!("get_by_client_id_missing"))
        .get(&scan_path(9998), 200, &page(9998, 9998, &[]))
        .install(&t);
    assert!(matches!(a.cancel_and_settle("9999"), Err(BrokerError::CancelTargetNotFound(id)) if id == "9999"));
    // any other failure is passed through, not swallowed
    Routes::default().put(&p("/orders/9999/cancel"), 500, fx!("error_500.json")).install(&t);
    assert!(matches!(a.cancel_and_settle("9999"), Err(BrokerError::Http(500))));
}

// ---------------------------------------------------------------- close / flatten one instrument

const CLOSE_TAG: &str = "rb1:fl:20260921T150000Z:EURUSD:1:abc";

/// Summary, the recent-window scan, the position read and the close.
fn close_routes(inst: &str, position: &str, put_status: u16, put_body: &str) -> Routes {
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .get(&scan_path(LAST), 200, &page(LAST, LAST, &[]))
        .get(&p(&format!("/positions/{inst}")), 200, position)
        .put(&p(&format!("/positions/{inst}/close")), put_status, put_body)
}

fn close_routes_seq(inst: &str, position_seq: &[(u16, String)], put_status: u16, put_body: &str) -> Routes {
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .get(&scan_path(LAST), 200, &page(LAST, LAST, &[]))
        .get_seq(&p(&format!("/positions/{inst}")), position_seq)
        .put(&p(&format!("/positions/{inst}/close")), put_status, put_body)
}

fn close_routes_err(inst: &str, position: &str, e: TransportError) -> Routes {
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .get(&scan_path(LAST), 200, &page(LAST, LAST, &[]))
        .get(&p(&format!("/positions/{inst}")), 200, position)
        .fail(HttpMethod::Put, &p(&format!("/positions/{inst}/close")), e)
}

fn puts(t: &FakeTransport) -> Vec<HttpRequest> {
    t.requests().into_iter().filter(|r| r.method == HttpMethod::Put).collect()
}

#[test]
fn closing_a_long_sends_all_for_the_long_side_and_none_for_the_short_side() {
    // RECORDED response (close_long_only): 14 long units closed by selling 14.
    let (a, t) = setup();
    close_routes("EUR_USD", &position_body("EUR_USD", "14", "0"), 200, rfx!("close_long_only")).install(&t);
    match a.close_position("EUR/USD", CLOSE_TAG).unwrap() {
        CloseOutcome::Closed { broker_order_id, fill } => {
            assert_eq!(broker_order_id, "49");
            assert_eq!((fill.units, fill.price, fill.pl), (d("-14"), d("1.13781"), Some(d("-0.0072"))));
        }
        other => panic!("{other:?}"),
    }
    let put = &puts(&t)[0];
    let expected: Value = serde_json::from_str(
        r#"{"longUnits":"ALL","shortUnits":"NONE","longClientExtensions":{"id":"rb1:fl:20260921T150000Z:EURUSD:1:abc","tag":"mendl-rb"}}"#,
    )
    .unwrap();
    assert_eq!(body_json(put), expected);
    assert!(line(put).ends_with("/positions/EUR_USD/close"));
    assert_eq!(a.tag_checkpoint(CLOSE_TAG), Some(LAST));
}

#[test]
fn closing_a_short_sends_all_for_the_short_side_and_none_for_the_long_side() {
    // RECORDED response (close_short): a 3-unit USD_JPY short closed.
    let (a, t) = setup();
    close_routes("USD_JPY", &position_body("USD_JPY", "0", "-3"), 200, rfx!("close_short")).install(&t);
    match a.close_position("USD_JPY", CLOSE_TAG).unwrap() {
        CloseOutcome::Closed { fill, broker_order_id } => {
            assert_eq!((broker_order_id.as_str(), fill.units, fill.price), ("43", d("3"), d("158.329")));
        }
        other => panic!("{other:?}"),
    }
    let body = body_json(&puts(&t)[0]);
    assert_eq!(body["shortUnits"], "ALL");
    assert_eq!(body["longUnits"], "NONE", "ALL for a side that does not exist is HTTP 400 at OANDA: never sent");
    assert_eq!(body["shortClientExtensions"]["id"], CLOSE_TAG);
    assert!(body.get("longClientExtensions").is_none());
}

#[test]
fn nothing_to_close_sends_no_put() {
    let (a, t) = setup();
    close_routes("EUR_USD", &position_body("EUR_USD", "0", "0"), 200, rfx!("close_long_only")).install(&t);
    assert_eq!(a.close_position("EUR_USD", CLOSE_TAG).unwrap(), CloseOutcome::NothingToClose);
    assert!(puts(&t).is_empty());
    // a 404 on the position read also means no position
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .get(&p("/positions/EUR_USD"), 404, sfx!("position_never_traded_404"))
        .install(&t);
    assert_eq!(a.close_position("EUR_USD", CLOSE_TAG).unwrap(), CloseOutcome::NothingToClose);
    assert!(puts(&t).is_empty());
}

#[test]
fn closing_twice_with_the_same_tag_adopts_the_first_close_when_the_stream_names_it() {
    // DERIVED from the recorded close_short: the recording sent no client extensions; they are added where OANDA puts them
    // on a market order and its fill. (Whether a real close echoes longClientExtensions is UNMEASURED.)
    let body = with_client_id(rfx!("close_short"), CLOSE_TAG);
    let v: Value = serde_json::from_str(&body).unwrap();
    let recs = [v["shortOrderCreateTransaction"].clone(), v["shortOrderFillTransaction"].clone()];
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &summary_with_last(44))
        .get(&scan_path(44 - WINDOW), 200, &page(44 - WINDOW, 44, &recs))
        .install(&t);
    match a.close_position("USD_JPY", CLOSE_TAG).unwrap() {
        CloseOutcome::AlreadyDone { report } => assert_eq!((report.broker_order_id.as_str(), report.status), ("43", OrderStatus::Filled)),
        other => panic!("{other:?}"),
    }
    assert!(puts(&t).is_empty(), "no second close is sent");
}

#[test]
fn a_hedged_position_is_refused_without_sending_a_close() {
    let (a, t) = setup();
    close_routes("EUR_USD", &position_body("EUR_USD", "10", "-10"), 200, rfx!("close_long_only")).install(&t);
    assert!(matches!(a.close_position("EUR_USD", CLOSE_TAG), Err(BrokerError::Unsupported(_))));
    assert!(puts(&t).is_empty());
}

#[test]
fn a_closeout_that_the_broker_says_does_not_exist_is_settled_by_re_reading_the_position() {
    // RECORDED: 404 close_nothing (nothing open) and 400 close_wrong_side (ALL for an absent side), both
    // CLOSEOUT_POSITION_DOESNT_EXIST. We read the position moments ago, so the broker's answer means it changed.
    for (status, body) in [(404u16, rfx!("close_nothing")), (400, rfx!("close_wrong_side"))] {
        // the position is gone on the re-read: already flat
        let (a, t) = setup();
        close_routes_seq("USD_JPY", &[(200, position_body("USD_JPY", "0", "-3")), (200, flat_position_body("USD_JPY"))], status, body).install(&t);
        match a.close_position("USD_JPY", CLOSE_TAG).unwrap() {
            CloseOutcome::AlreadyFlat { detail } => assert!(detail.contains("no position to close"), "{detail}"),
            other => panic!("{status}: {other:?}"),
        }
        assert_eq!(puts(&t).len(), 1);
        // the position is still there on the re-read: a contradiction, unknown
        let (a, t) = setup();
        close_routes("USD_JPY", &position_body("USD_JPY", "0", "-3"), status, body).install(&t);
        match a.close_position("USD_JPY", CLOSE_TAG).unwrap() {
            CloseOutcome::UnknownOutcome { reason } => assert!(reason.contains("still shows") && reason.contains(&format!("[oanda-tag-checkpoint={LAST}]")), "{reason}"),
            other => panic!("{status}: {other:?}"),
        }
    }
}

#[test]
fn close_outcomes_are_parsed_into_rejected_and_unknown() {
    // a 400 with a reject transaction under the long/short prefix (AUTHORED body)
    let (a, t) = setup();
    close_routes("EUR_USD", &position_body("EUR_USD", "10000", "0"), 400, fx!("close_rejected_400.json")).install(&t);
    match a.close_position("EUR_USD", CLOSE_TAG).unwrap() {
        CloseOutcome::Rejected { errors } => {
            assert!(errors[0].code.contains("MARKET_HALTED") && errors[0].code.starts_with("oanda:400"), "{}", errors[0].code);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(a.tag_checkpoint(CLOSE_TAG), None, "a refused close created nothing");
    // cancelled at creation
    let (a, t) = setup();
    close_routes("EUR_USD", &position_body("EUR_USD", "10000", "0"), 200, fx!("close_cancelled.json")).install(&t);
    assert!(matches!(a.close_position("EUR_USD", CLOSE_TAG).unwrap(), CloseOutcome::Rejected { errors } if errors[0].code.contains("MARKET_HALTED")));
    // untrustworthy successes and server trouble: the follow-up scan finds nothing and the position is still open
    for (why, status, body) in [
        ("other tag", 200, fx!("close_wrong_tag.json")),
        ("no transactions", 200, fx!("close_no_transactions.json")),
        ("not JSON", 200, "OK"),
        ("500", 500, fx!("error_500.json")),
        ("503", 503, fx!("error_503_html.txt")),
        ("404 that is not a closeout error", 404, fx!("error_404_account.json")),
    ] {
        let (a, t) = setup();
        close_routes("EUR_USD", &position_body("EUR_USD", "10000", "0"), status, body).install(&t);
        match a.close_position("EUR_USD", CLOSE_TAG).unwrap() {
            CloseOutcome::UnknownOutcome { reason } => assert!(reason.contains(&format!("[oanda-tag-checkpoint={LAST}]")), "{why}: {reason}"),
            other => panic!("{why}: {other:?}"),
        }
        assert_eq!(puts(&t).len(), 1, "{why}: never retried");
    }
    // rate limit: not sent
    let (a, t) = setup();
    close_routes("EUR_USD", &position_body("EUR_USD", "10000", "0"), 429, fx!("error_429.json")).install(&t);
    assert!(matches!(a.close_position("EUR_USD", CLOSE_TAG), Err(BrokerError::RateLimited { .. })));
    assert_eq!(a.tag_checkpoint(CLOSE_TAG), None);
}

#[test]
fn a_close_whose_answer_is_lost_is_settled_from_the_stream_or_from_the_position() {
    let long_open = position_body("EUR_USD", "10000", "0");
    // 1. the stream names the close (extensions echoed): found, not repeated
    let body = with_client_id(rfx!("close_long_only"), CLOSE_TAG);
    let v: Value = serde_json::from_str(&body).unwrap();
    let recs = [
        { let mut c = v["longOrderCreateTransaction"].clone(); c["id"] = json!("6401"); c },
        { let mut f = v["longOrderFillTransaction"].clone(); f["id"] = json!("6402"); f["orderID"] = json!("6401"); f },
    ];
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .get(&p("/positions/EUR_USD"), 200, &long_open)
        .fail(HttpMethod::Put, &p("/positions/EUR_USD/close"), TransportError::Timeout)
        .get(&scan_path(LAST), 200, &page(LAST, LAST + 2, &recs))
        .install(&t);
    match a.close_position("EUR_USD", CLOSE_TAG).unwrap() {
        CloseOutcome::Closed { broker_order_id, fill } => assert_eq!((broker_order_id.as_str(), fill.units), ("6401", d("-14"))),
        other => panic!("{other:?}"),
    }
    assert_eq!(puts(&t).len(), 1);

    // 2. the stream does not name it (extensions NOT echoed) but the position is flat afterwards: reported as already flat
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 200, &page(LAST - WINDOW, LAST, &[]))
        .get_seq(&p("/positions/EUR_USD"), &[(200, long_open.clone()), (200, flat_position_body("EUR_USD"))])
        .fail(HttpMethod::Put, &p("/positions/EUR_USD/close"), TransportError::Timeout)
        .get(&scan_path(LAST), 200, &page(LAST, LAST + 2, &[]))
        .install(&t);
    match a.close_position("EUR_USD", CLOSE_TAG).unwrap() {
        CloseOutcome::AlreadyFlat { detail } => assert!(detail.contains("now flat") && detail.contains("unverified"), "{detail}"),
        other => panic!("{other:?}"),
    }

    // 3. still open and not in the stream: unknown, and the next call may send the close again (a repeated ALL is harmless)
    let (a, t) = setup();
    close_routes_err("EUR_USD", &long_open, TransportError::Io("reset".into())).install(&t);
    assert!(matches!(a.close_position("EUR_USD", CLOSE_TAG).unwrap(), CloseOutcome::UnknownOutcome { .. }));
    close_routes("EUR_USD", &long_open, 200, rfx!("close_long_only")).install(&t);
    assert!(matches!(a.close_position("EUR_USD", CLOSE_TAG).unwrap(), CloseOutcome::Closed { .. }));
}

#[test]
fn a_close_whose_tag_scan_fails_sends_nothing() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, fx!("account_summary_ok.json"))
        .get(&scan_path(LAST - WINDOW), 500, fx!("error_500.json"))
        .install(&t);
    assert!(matches!(a.close_position("EUR_USD", CLOSE_TAG), Err(BrokerError::Preflight(m)) if m.contains("nothing was sent")));
    assert!(puts(&t).is_empty());
    assert_eq!(count(&t, HttpMethod::Get, "/positions/"), 0);
}

// ---------------------------------------------------------------- HTTP error shapes on reads

#[test]
fn read_errors_map_status_codes_to_typed_errors() {
    let (a, t) = setup();
    let path = p("/summary");
    for (status, body, check) in [
        (401, fx!("error_401.json"), 0),
        (403, fx!("error_403.json"), 0),
        (404, fx!("error_404_account.json"), 1),
        (429, fx!("error_429.json"), 2),
        (500, fx!("error_500.json"), 3),
        (503, fx!("error_503_html.txt"), 3),
        (418, "teapot", 3),
    ] {
        Routes::default().get(&path, status, body).install(&t);
        let e = a.get_account_summary().unwrap_err();
        match check {
            0 => assert!(matches!(&e, BrokerError::Exchange(v) if v[0].class == ErrorClass::Auth && v[0].code.starts_with(&format!("oanda:{status}"))), "{status}: {e:?}"),
            1 => assert!(matches!(e, BrokerError::NotFound(_)), "{status}"),
            2 => assert!(matches!(e, BrokerError::RateLimited { retry_after_secs: None, .. }), "{status}"),
            _ => assert!(matches!(e, BrokerError::Http(s) if s == status), "{status}: {e:?}"),
        }
    }
}

#[test]
fn a_gateway_page_is_never_echoed_into_an_error() {
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 503, fx!("error_503_html.txt")).install(&t);
    let e = a.get_account_summary().unwrap_err();
    assert!(!format!("{e} {e:?}").contains("secret-gateway-page"));
    let (out, _) = place(400, fx!("error_503_html.txt"));
    let errs = expect_rejected(out);
    assert!(!errs[0].code.contains("secret-gateway-page"), "{}", errs[0].code);
}

// ---------------------------------------------------------------- secrets in errors

#[test]
fn a_server_that_echoes_the_token_does_not_leak_it_into_our_errors() {
    let echo = format!(r#"{{"errorCode":"BAD_AUTH","errorMessage":"rejected header Authorization: Bearer {TOKEN}"}}"#);
    // read path
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 401, &echo).install(&t);
    let e = a.get_account_summary().unwrap_err();
    let shown = format!("{e} {e:?}");
    assert!(!shown.contains(TOKEN) && shown.contains("<redacted>"), "{shown}");
    // placement path (401 => Rejected with the scrubbed text)
    let (out, _) = place(401, &echo);
    let errs = expect_rejected(out);
    assert!(!errs[0].code.contains(TOKEN), "{}", errs[0].code);
    // 400 echo
    let echo400 = format!(r#"{{"errorMessage":"invalid header {TOKEN}"}}"#);
    let (out, _) = place(400, &echo400);
    let errs = expect_rejected(out);
    assert!(!errs[0].code.contains(TOKEN), "{}", errs[0].code);
    // rate limit message
    let (a, t) = setup();
    Routes::default().get(&p("/summary"), 429, &echo400).install(&t);
    let e = a.get_account_summary().unwrap_err();
    assert!(!format!("{e} {e:?}").contains(TOKEN));
    // and the adapter's own Debug
    let (a, _) = setup();
    assert!(!format!("{a:?}").contains(TOKEN));
}

// ---------------------------------------------------------------- flat is not held (MEASURED 2026-09-23)
//
// GET /positions/<i> for an instrument traded before and flat now is HTTP 200 with both sides at "0"; for one never traded it is
// 404 NO_SUCH_POSITION. Both mean "nothing held". Only a 404 that is NOT NO_SUCH_POSITION is an error.

#[test]
fn get_position_maps_flat_and_never_traded_to_none_and_a_held_position_to_some() {
    let (a, t) = setup();
    Routes::default()
        .get(&p("/positions/EUR_USD"), 200, sfx!("position_flat_previously_traded"))
        .get(&p("/positions/GBP_JPY"), 404, sfx!("position_never_traded_404"))
        .get(&p("/positions/GBP_USD"), 200, fx!("position_single_short.json"))
        .get(&p("/positions/USD_JPY"), 404, fx!("error_404_account.json"))
        .install(&t);
    assert!(a.get_position("EUR_USD").unwrap().is_none(), "RECORDED: flat but previously traded is 200 with zero units: NOT held");
    assert!(a.get_position("GBP_JPY").unwrap().is_none(), "RECORDED: never traded is 404 NO_SUCH_POSITION");
    assert_eq!(a.get_position("GBP_USD").unwrap().unwrap().net_units(), d("-5000"));
    // a 404 that is not NO_SUCH_POSITION (an unknown account) is never read as "flat"
    assert!(matches!(a.get_position("USD_JPY"), Err(BrokerError::NotFound(_))));
}

#[test]
fn flat_entries_of_the_recorded_all_positions_list_are_no_balances() {
    // DERIVED: the recorded summary with the placeholder account id replaced by the test account's, and the recorded
    // GET /positions body (two flat previously-traded entries) served where the adapter reads open positions.
    let (a, t) = setup();
    Routes::default()
        .get(&p("/summary"), 200, &sfx!("account_summary").replace("ACCOUNT_ID", ACCT))
        .get(&p("/openPositions"), 200, sfx!("positions_all_with_flat_entries"))
        .install(&t);
    let b = a.get_balances().unwrap();
    assert_eq!(b.entries.len(), 1, "USD only: a flat entry is not a spot balance of 0 EUR/USD");
    assert_eq!(b.spot("USD"), d("99999.9917"));
    assert_eq!(b.spot("EUR/USD"), Dec::ZERO);
}

#[test]
fn close_position_on_a_flat_previously_traded_instrument_is_nothing_to_close_and_sends_no_put() {
    let (a, t) = setup();
    close_routes("EUR_USD", sfx!("position_flat_previously_traded"), 200, rfx!("close_long_only")).install(&t);
    assert_eq!(a.close_position("EUR_USD", CLOSE_TAG).unwrap(), CloseOutcome::NothingToClose);
    assert!(puts(&t).is_empty(), "the flat record must not be mistaken for a position to close");
    assert_eq!(a.tag_checkpoint(CLOSE_TAG), None);
}

#[test]
fn a_flat_previously_traded_instrument_does_not_block_a_new_order_even_with_a_position_cap() {
    let (a, t) = cap_adapter(Some("5000"));
    place_routes(201, fx!("create_market_buy_filled.json")).get(&p("/positions/EUR_USD"), 200, sfx!("position_flat_previously_traded")).install(&t);
    assert_sent(try_order(&a, &t, Side::Buy, "5000"), "from a flat previously-traded instrument up to the cap");
    assert_cap_refused(try_order(&a, &t, Side::Buy, "5001"), "past the cap from flat");
}

// ---------------------------------------------------------------- reads over the RECORDED summary, instruments and pricing

#[test]
fn the_recorded_summary_instruments_and_pricing_drive_the_adapter_reads() {
    // DERIVED only in the account id (sanitised to ACCOUNT_ID in the recording): everything else is the real body.
    let (a, t) = setup();
    let summary = sfx!("account_summary").replace("ACCOUNT_ID", ACCT);
    Routes::default()
        .get(&p("/summary"), 200, &summary)
        .get(&p("/instruments"), 200, sfx!("instruments"))
        .get(&p("/pricing?instruments=EUR_USD&includeHomeConversions=true"), 200, sfx!("pricing_home_conversions"))
        .get(&p("/openPositions"), 200, sfx!("open_positions_empty"))
        .install(&t);
    let s = a.verify_account().unwrap();
    assert_eq!((s.nav, s.balance, s.last_transaction_id.as_deref()), (d("99999.9917"), d("99999.9917"), Some("54")));
    assert_eq!(a.refresh_instruments().unwrap(), 2);
    assert_eq!(a.instrument("EUR/USD").unwrap().maximum_position_size, None);
    let q = a.get_quote("EUR/USD").unwrap();
    assert_eq!((q.bid, q.ask, q.last), (d("1.13841"), d("1.13860"), d("1.138505")));
    let b = a.get_balances().unwrap();
    assert_eq!((b.entries.len(), b.spot("USD")), (1, d("99999.9917")));
    assert!(a.get_open_positions().unwrap().is_empty());
}
