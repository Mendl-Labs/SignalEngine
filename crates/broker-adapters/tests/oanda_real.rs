//! Parse tests over RECORDED responses from an OANDA practice account (2026-09-23, account and user ids removed). Two
//! recording sessions: `oanda_211402__*` (orders, cancels, closes, rejects, the transaction stream) and `oanda_smoke__*`
//! (summary, instruments, pricing, positions, taken while the smoke test ran).
//!
//! PROVENANCE: the files in `tests/fixtures/oanda/real/` are real, not authored (see the README there). The HTTP status of a
//! response is not stored in the file, so it is stated in [`RECORDED`], from the recording notes in
//! `product-mandate/VENUE_FACTS.md`. A test fails if a recorded file stops parsing, and another if a file is added to the
//! directory without being listed here.

use broker_adapters::oanda::parse::{
    self, classify_http_failure, parse_cancel_response, parse_order_resource, parse_order_resources, parse_order_transactions,
    parse_transactions_page, HttpFailure, INVALID_PARAMETER_EXCEPTION,
};
use broker_adapters::oanda::{InstrumentTable, PriceQuote};
use broker_adapters::{Dec, ErrorClass};
use serde_json::Value;

macro_rules! rfx {
    ($name:literal) => {
        include_str!(concat!("fixtures/oanda/real/oanda_211402__", $name, ".json"))
    };
}

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

/// What a recorded response is, so every file is parsed by the right function. `status` is what OANDA answered (200/201/400/404);
/// 200 vs 201 is not in the recording and is irrelevant to the adapter, which treats every 2xx alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// `POST /orders` success: create (+ fill) transactions under `order`.
    PlaceOk,
    /// A close success: `long|shortOrderCreate|Fill`.
    CloseOk(&'static str),
    /// HTTP 400/404 whose body is an error (classified by `classify_http_failure`).
    Error,
    /// `GET /orders/@id` of a pending order.
    OrderResource,
    /// Cancel 200.
    CancelOk,
    /// `GET /transactions/sinceid`.
    Sinceid,
    /// `GET /orders`: an order list.
    OrderList,
    /// `GET /transactions`: paged listing (page URLs).
    TxnListing,
    /// `GET /summary`.
    Summary,
    /// `GET /instruments`.
    Instruments,
    /// `GET /pricing?includeHomeConversions=true`.
    Pricing,
    /// `GET /positions` or `GET /openPositions`.
    PositionList,
    /// `GET /positions/<instrument>` (200).
    SinglePosition,
}

/// (file stem after `oanda_211402__`, HTTP status, shape).
const RECORDED: &[(&str, u16, Shape)] = &[
    ("place_filled", 201, Shape::PlaceOk),
    ("duplicate_post", 201, Shape::PlaceOk),
    ("place_limit", 201, Shape::PlaceOk),
    ("sell_short", 201, Shape::PlaceOk),
    ("close_short", 200, Shape::CloseOk("shortOrder")),
    ("close_long_only", 200, Shape::CloseOk("longOrder")),
    ("close_nothing", 404, Shape::Error),
    ("close_wrong_side", 400, Shape::Error),
    ("get_by_client_id_filled", 404, Shape::Error),
    ("get_by_client_id_missing", 404, Shape::Error),
    ("get_limit_cancelled", 404, Shape::Error),
    ("get_limit_pending", 200, Shape::OrderResource),
    ("cancel_by_client_id", 200, Shape::CancelOk),
    ("cancel_again", 404, Shape::Error),
    ("reject_too_big", 400, Shape::Error),
    ("reject_zero", 400, Shape::Error),
    ("reject_market_gtc", 400, Shape::Error),
    ("reject_fractional", 400, Shape::Error),
    ("reject_bad_instrument", 400, Shape::Error),
    ("transactions_sinceid", 200, Shape::Sinceid),
    ("transactions_list", 200, Shape::TxnListing),
    ("list_orders_all", 200, Shape::OrderList),
    // second session (smoke test run)
    ("position_flat_previously_traded", 200, Shape::SinglePosition),
    ("position_never_traded_404", 404, Shape::Error),
    ("positions_all_with_flat_entries", 200, Shape::PositionList),
    ("open_positions_empty", 200, Shape::PositionList),
    ("account_summary", 200, Shape::Summary),
    ("instruments", 200, Shape::Instruments),
    ("pricing_home_conversions", 200, Shape::Pricing),
];

fn body_of(stem: &str) -> String {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/oanda/real");
    let first = dir.join(format!("oanda_211402__{stem}.json"));
    let path = if first.exists() { first } else { dir.join(format!("oanda_smoke__{stem}.json")) };
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"))
}

fn json(stem: &str) -> Value {
    serde_json::from_str(&body_of(stem)).unwrap_or_else(|e| panic!("{stem}: {e}"))
}

// ---------------------------------------------------------------- the guard: nothing stops parsing, nothing is unlisted

#[test]
fn every_recorded_fixture_still_parses_and_every_file_is_listed() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/oanda/real");
    let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.ends_with(".json"))
        .map(|n| n.trim_start_matches("oanda_211402__").trim_start_matches("oanda_smoke__").trim_end_matches(".json").to_string())
        .collect();
    on_disk.sort();
    let mut listed: Vec<String> = RECORDED.iter().map(|(s, _, _)| s.to_string()).collect();
    listed.sort();
    assert_eq!(on_disk, listed, "a recorded fixture is missing from RECORDED, or listed without a file");

    for (stem, status, shape) in RECORDED {
        let body = body_of(stem);
        assert!(!body.contains("101-"), "{stem}: an account id survived sanitising");
        assert!(body.contains("ACCOUNT_ID") || !body.contains("accountID"), "{stem}: an accountID is not the placeholder");
        match shape {
            Shape::PlaceOk => {
                let t = parse_order_transactions(&body, "order").unwrap_or_else(|e| panic!("{stem}: {e}"));
                assert!(t.create.is_some(), "{stem}: no create transaction");
            }
            Shape::CloseOk(prefix) => {
                let t = parse_order_transactions(&body, prefix).unwrap_or_else(|e| panic!("{stem}: {e}"));
                assert!(t.create.is_some() && t.fill.is_some(), "{stem}: a close success has a create and a fill");
            }
            Shape::Error => {
                let f = classify_http_failure(*status, &body, None, &[]);
                assert_eq!(f.status(), *status, "{stem}");
                assert!(f.api().code.is_some(), "{stem}: an error body carries errorCode");
            }
            Shape::OrderResource => {
                parse_order_resource(&body).unwrap_or_else(|e| panic!("{stem}: {e}"));
            }
            Shape::CancelOk => {
                parse_cancel_response(&body).unwrap_or_else(|e| panic!("{stem}: {e}"));
            }
            Shape::Sinceid => {
                let page = parse_transactions_page(&body, 50).unwrap_or_else(|e| panic!("{stem}: {e}"));
                page.completeness().unwrap_or_else(|e| panic!("{stem}: {e}"));
            }
            Shape::OrderList => {
                assert!(parse_order_resources(&body).unwrap().is_empty(), "{stem}");
            }
            Shape::TxnListing => {
                let v = json(stem);
                assert!(v["pages"].is_array() && v["lastTransactionID"].is_string(), "{stem}");
            }
            Shape::Summary => {
                parse::parse_account_summary(&body).unwrap_or_else(|e| panic!("{stem}: {e}"));
            }
            Shape::Instruments => {
                InstrumentTable::from_instruments_json(&body).unwrap_or_else(|e| panic!("{stem}: {e}"));
            }
            Shape::Pricing => {
                parse::parse_pricing(&body).unwrap_or_else(|e| panic!("{stem}: {e}"));
            }
            Shape::PositionList => {
                parse::parse_open_positions(&body).unwrap_or_else(|e| panic!("{stem}: {e}"));
            }
            Shape::SinglePosition => {
                parse::parse_single_position(&body).unwrap_or_else(|e| panic!("{stem}: {e}"));
            }
        }
    }
}

#[test]
fn the_provenance_note_is_present() {
    let readme = std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/oanda/real/README.md")).unwrap();
    assert!(readme.contains("recorded from") || readme.contains("RECORDED from"), "{readme}");
    assert!(readme.contains("2026-09-23") && readme.contains("account and user ids"), "the README must say when and what was removed");
    let top = std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/oanda/README.md")).unwrap();
    assert!(top.contains("AUTHORED FROM DOCUMENTATION") && top.contains("UNMEASURED"));
}

// ---------------------------------------------------------------- placement: what OANDA sends

#[test]
fn a_market_order_create_and_fill_carry_the_client_id_in_the_two_different_places() {
    let t = parse_order_transactions(rfx!("place_filled"), "order").unwrap();
    let (c, f) = (t.create.unwrap(), t.fill.unwrap());
    assert_eq!((c.id.as_str(), c.kind.as_str(), c.units, c.instrument.as_deref()), ("30", "MARKET_ORDER", Some(d("1")), Some("EUR_USD")));
    // MEASURED: the MARKET_ORDER names the client id as clientExtensions.id ...
    assert_eq!(c.client_id.as_deref(), Some("lookup-filled-20260923T211402Z"));
    // ... and the ORDER_FILL as the top-level clientOrderID
    assert_eq!(f.client_order_id.as_deref(), Some("lookup-filled-20260923T211402Z"));
    assert_eq!((f.id.as_str(), f.order_id.as_str(), f.units, f.price), ("31", "30", d("1"), d("1.13835")));
    assert_eq!(f.reason.as_deref(), Some("MARKET_ORDER"));
    assert!(t.reject.is_none() && t.cancel.is_none());
}

#[test]
fn a_resting_limit_order_has_a_create_transaction_and_nothing_else() {
    let t = parse_order_transactions(rfx!("place_limit"), "order").unwrap();
    let c = t.create.unwrap();
    assert_eq!((c.kind.as_str(), c.price, c.time_in_force.as_deref()), ("LIMIT_ORDER", Some(d("0.50000")), Some("GTC")));
    assert!(t.fill.is_none() && t.cancel.is_none() && t.reject.is_none());
}

#[test]
fn a_market_sell_with_no_position_is_a_short_fill_with_negative_units() {
    let t = parse_order_transactions(rfx!("sell_short"), "order").unwrap();
    let (c, f) = (t.create.unwrap(), t.fill.unwrap());
    assert_eq!((c.units, f.units, f.instrument.as_str(), f.price), (Some(d("-3")), d("-3"), "USD_JPY", d("158.302")));
}

#[test]
fn a_client_id_is_not_unique_the_same_id_filled_twice() {
    let first = parse_order_transactions(rfx!("place_filled"), "order").unwrap();
    let second = parse_order_transactions(rfx!("duplicate_post"), "order").unwrap();
    let (a, b) = (first.fill.unwrap(), second.fill.unwrap());
    assert_eq!(a.client_order_id, b.client_order_id, "the same clientOrderID on two different fills");
    assert_ne!(a.order_id, b.order_id);
    assert_eq!((a.units, b.units), (d("1"), d("1")), "so the position doubled: the id deduplicates nothing");
}

// ---------------------------------------------------------------- lookups and cancels

#[test]
fn a_pending_order_is_found_by_client_id_but_a_filled_or_cancelled_one_is_a_404() {
    let o = parse_order_resource(rfx!("get_limit_pending")).unwrap();
    assert_eq!((o.id.as_str(), o.state.as_str(), o.kind.as_str()), ("34", "PENDING", "LIMIT"));
    assert_eq!(o.client_id.as_deref(), Some("limit-20260923T211402Z"));
    for stem in ["get_by_client_id_filled", "get_by_client_id_missing", "get_limit_cancelled"] {
        let f = classify_http_failure(404, &body_of(stem), None, &[]);
        assert!(matches!(&f, HttpFailure::NotFound { api } if api.code.as_deref() == Some("NO_SUCH_ORDER")), "{stem}: {f:?}");
    }
}

#[test]
fn cancel_succeeds_once_and_then_is_a_404_with_a_cancel_reject_transaction() {
    let c = parse_cancel_response(rfx!("cancel_by_client_id")).unwrap();
    assert_eq!((c.order_id.as_str(), c.reason.as_str(), c.client_order_id.as_deref()), ("34", "CLIENT_REQUEST", Some("limit-20260923T211402Z")));
    let f = classify_http_failure(404, rfx!("cancel_again"), None, &[]);
    match f {
        HttpFailure::NotFound { api } => {
            assert_eq!(api.code.as_deref(), Some("ORDER_DOESNT_EXIST"));
            assert_eq!(api.reject_reason.as_deref(), Some("ORDER_DOESNT_EXIST"));
        }
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------- position close

#[test]
fn a_close_success_names_the_closeout_and_the_fill_under_the_side_prefix() {
    let t = parse_order_transactions(rfx!("close_short"), "shortOrder").unwrap();
    let (c, f) = (t.create.unwrap(), t.fill.unwrap());
    assert_eq!((c.id.as_str(), c.units, f.order_id.as_str(), f.units, f.instrument.as_str()), ("43", Some(d("3")), "43", d("3"), "USD_JPY"));
    assert_eq!(f.reason.as_deref(), Some("MARKET_ORDER_POSITION_CLOSEOUT"));
    assert_eq!(f.pl, Some(d("-0.0005")));
    // the recording sent no client extensions, so none is echoed
    assert!(c.client_id.is_none() && f.client_order_id.is_none());

    let t = parse_order_transactions(rfx!("close_long_only"), "longOrder").unwrap();
    let f = t.fill.unwrap();
    assert_eq!((f.units, f.instrument.as_str()), (d("-14"), "EUR_USD"), "closing 14 long units sells 14");
    // a parse of the wrong prefix finds nothing rather than mistaking one side for the other
    assert!(parse_order_transactions(rfx!("close_long_only"), "shortOrder").unwrap().is_empty());
}

#[test]
fn a_close_of_a_missing_side_or_of_nothing_is_closeout_position_doesnt_exist() {
    for (stem, status, prefix) in [("close_wrong_side", 400u16, "longOrder"), ("close_nothing", 404, "shortOrder")] {
        let body = body_of(stem);
        let f = classify_http_failure(status, &body, None, &[]);
        assert_eq!(f.api().code.as_deref(), Some("CLOSEOUT_POSITION_DOESNT_EXIST"), "{stem}");
        assert_eq!(f.api().reject_reason.as_deref(), Some("CLOSEOUT_POSITION_DOESNT_EXIST"), "{stem}");
        let t = parse_order_transactions(&body, prefix).unwrap();
        assert_eq!(t.reject.unwrap().reason, "CLOSEOUT_POSITION_DOESNT_EXIST", "{stem}");
        assert_eq!(f.exchange_error().map(|e| e.class).unwrap_or(ErrorClass::Other), if status == 400 { ErrorClass::UnknownOrder } else { ErrorClass::Other });
    }
}

// ---------------------------------------------------------------- reject shapes

#[test]
fn measured_rejects_are_400_with_a_reject_transaction_whose_reason_equals_the_error_code() {
    for stem in ["reject_too_big", "reject_zero", "reject_market_gtc", "reject_fractional"] {
        let body = body_of(stem);
        let f = classify_http_failure(400, &body, None, &[]);
        let HttpFailure::Rejected { class, api, .. } = &f else { panic!("{stem}: {f:?}") };
        assert_eq!(api.reject_reason, api.code, "{stem}: rejectReason == errorCode");
        assert_eq!(*class, ErrorClass::InvalidArguments, "{stem}: {api:?}");
        let t = parse_order_transactions(&body, "order").unwrap();
        assert_eq!(Some(t.reject.unwrap().reason), api.reject_reason, "{stem}");
        assert!(t.create.is_none());
    }
    let reasons: Vec<String> = ["reject_too_big", "reject_zero", "reject_market_gtc", "reject_fractional"]
        .iter()
        .map(|s| classify_http_failure(400, &body_of(s), None, &[]).api().reject_reason.clone().unwrap())
        .collect();
    assert_eq!(reasons, ["UNITS_LIMIT_EXCEEDED", "UNITS_INVALID", "TIME_IN_FORCE_INVALID", "UNITS_PRECISION_EXCEEDED"]);
}

#[test]
fn an_unknown_instrument_is_a_400_invalid_parameter_exception_with_no_reject_transaction() {
    let body = rfx!("reject_bad_instrument");
    let f = classify_http_failure(400, body, None, &[]);
    let HttpFailure::Rejected { class, api, .. } = &f else { panic!("{f:?}") };
    assert_eq!(api.code.as_deref(), Some(INVALID_PARAMETER_EXCEPTION));
    assert!(api.reject_reason.is_none(), "no reject transaction in this body");
    assert_eq!(*class, ErrorClass::InvalidArguments);
    assert!(api.message.contains("order.instrument"));
    let e = f.exchange_error().unwrap();
    assert_eq!(e.class, ErrorClass::InvalidArguments);
    assert!(e.code.starts_with("oanda:400 ") && e.code.contains("InvalidParameterException"), "{}", e.code);
    assert!(parse_order_transactions(body, "order").unwrap().is_empty());
}

// ---------------------------------------------------------------- the transaction stream

#[test]
fn the_sinceid_stream_carries_the_market_order_and_its_fill_with_the_tag_and_is_complete_after_the_checkpoint() {
    let page = parse_transactions_page(rfx!("transactions_sinceid"), 50).unwrap();
    assert_eq!((page.after_id, page.last_transaction_id, page.records.len()), (50, 52, 2));
    page.completeness().unwrap();
    let kinds: Vec<&str> = page.records.iter().map(|r| r.kind.as_str()).collect();
    assert_eq!(kinds, ["MARKET_ORDER", "ORDER_FILL"]);
    assert!(page.records.iter().all(|r| r.client_id.as_deref() == Some("txscan-20260923T211519Z")), "found by tag");
    assert_eq!(page.records[1].order_id.as_deref(), Some("51"));
    // the same page asked for from an earlier checkpoint is NOT complete: the transactions in between are missing
    assert!(parse_transactions_page(rfx!("transactions_sinceid"), 45).unwrap().completeness().is_err());
    // and it is not a valid answer to a later one
    assert!(parse_transactions_page(rfx!("transactions_sinceid"), 51).is_err());
}

#[test]
fn a_paged_listing_returns_page_urls_not_transactions() {
    let v = json("transactions_list");
    assert!(v["pages"][0].as_str().unwrap().contains("/transactions/idrange?"), "{v}");
    assert!(v.get("transactions").is_none(), "the listing has no transactions: the adapter cannot use it directly");
    let _ = parse::parse_order_resources(rfx!("list_orders_all")).unwrap();
}

// ---------------------------------------------------------------- second session: positions, summary, instruments, pricing

#[test]
fn a_flat_previously_traded_instrument_is_a_200_with_zero_units_and_is_flat_not_held() {
    // MEASURED: GET /positions/EUR_USD after EUR_USD was traded and closed is HTTP 200 with both sides at "0" and a non-zero pl.
    let p = parse::parse_single_position(&body_of("position_flat_previously_traded")).unwrap();
    assert_eq!((p.instrument.as_str(), p.long_units, p.short_units, p.net_units()), ("EUR_USD", d("0"), d("0"), d("0")));
    assert!(p.is_flat() && !p.is_hedged());
    assert_eq!(p.unrealized_pl, d("0.0000"));
    assert!(p.margin_used.is_none() && p.long_average_price.is_none(), "a flat record carries no margin or average price");
}

#[test]
fn a_never_traded_instrument_is_a_404_no_such_position() {
    let f = classify_http_failure(404, &body_of("position_never_traded_404"), None, &[]);
    match f {
        HttpFailure::NotFound { api } => assert_eq!(api.code.as_deref(), Some("NO_SUCH_POSITION")),
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_all_positions_list_carries_flat_entries_that_are_not_holdings() {
    // MEASURED: GET /positions lists USD_JPY and EUR_USD, both flat; GET /openPositions is empty.
    let raw: Value = serde_json::from_str(&body_of("positions_all_with_flat_entries")).unwrap();
    let rows = raw["positions"].as_array().unwrap();
    let units: Vec<(&str, &str, &str)> =
        rows.iter().map(|r| (r["instrument"].as_str().unwrap(), r["long"]["units"].as_str().unwrap(), r["short"]["units"].as_str().unwrap())).collect();
    assert_eq!(units, [("USD_JPY", "0", "0"), ("EUR_USD", "0", "0")]);
    assert!(parse::parse_open_positions(&body_of("positions_all_with_flat_entries")).unwrap().is_empty(), "flat entries are dropped");
    assert!(parse::parse_open_positions(&body_of("open_positions_empty")).unwrap().is_empty());
}

#[test]
fn the_real_account_summary_parses_with_the_fields_the_adapter_depends_on() {
    // First REAL summary. Assumed shape held: top-level and account.lastTransactionID, NAV, hedgingEnabled, marginAvailable.
    let s = parse::parse_account_summary(&body_of("account_summary")).unwrap();
    assert_eq!((s.id.as_str(), s.currency.as_str(), s.hedging_enabled), ("ACCOUNT_ID", "USD", false));
    assert_eq!((s.balance, s.nav, s.unrealized_pl, s.margin_used, s.margin_available), (d("99999.9917"), d("99999.9917"), d("0"), d("0"), d("99999.9917")));
    assert_eq!((s.position_value, s.margin_closeout_percent, s.margin_rate), (Some(d("0")), Some(d("0")), Some(d("0.02"))));
    assert_eq!((s.open_trade_count, s.open_position_count, s.pending_order_count), (Some(0), Some(0), Some(0)));
    assert_eq!(s.last_transaction_id.as_deref(), Some("54"), "the idempotency checkpoint");
    // MEASURED: the summary has no positionAggregationMode key
    assert!(!body_of("account_summary").contains("positionAggregationMode"));
}

#[test]
fn the_real_instruments_parse_and_maximum_position_size_zero_means_no_cap() {
    let t = InstrumentTable::from_instruments_json(&body_of("instruments")).unwrap();
    assert_eq!(t.len(), 2);
    let e = t.lookup("EUR_USD").unwrap();
    assert_eq!((e.display_precision, e.trade_units_precision, e.minimum_trade_size, e.maximum_order_units, e.margin_rate), (5, 0, d("1"), d("100000000"), d("0.02")));
    assert_eq!(e.maximum_position_size, None, "the recorded \"0\" is NO cap");
    let j = t.lookup("USD_JPY").unwrap();
    assert_eq!((j.display_precision, j.margin_rate, j.maximum_position_size), (3, d("0.05"), None));
    // the recorded rows carry `tags` as objects, `financing` and trailing-stop distances: all ignored without complaint
    assert!(body_of("instruments").contains("\"tags\":[{\"type\""));
}

#[test]
fn the_real_pricing_parses_with_home_conversions_and_numeric_liquidity() {
    let p = parse::parse_pricing(&body_of("pricing_home_conversions")).unwrap();
    let q: &PriceQuote = p.price("EUR_USD").unwrap();
    assert!(q.tradeable);
    assert_eq!(q.status.as_deref(), Some("tradeable"));
    assert_eq!((q.bid, q.ask, q.closeout_bid, q.closeout_ask), (Some(d("1.13841")), Some(d("1.13860")), Some(d("1.13832")), Some(d("1.13870"))));
    assert_eq!(q.mid(), Some(d("1.138505")));
    assert_eq!(p.conversion("EUR").unwrap().position_value, d("1.1385"));
    assert_eq!(p.conversion("EUR").unwrap().account_gain, Some(d("1.127115")));
    assert_eq!(p.conversion("USD").unwrap().position_value, d("1"));
    // times are RFC 3339 here (no Accept-Datetime-Format was sent): informational only, so absent rather than an error
    assert!(q.time.is_none());
}
