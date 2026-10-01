//! Pure OANDA request building: instrument normalisation, unit and price rounding, sign, time in
//! force, position fill, the idempotency key, and the "a market order stays a market order" rule.

use broker_adapters::oanda::instrument::{canonical_symbol, normalize_instrument, quote_currency};
use broker_adapters::oanda::order::{check_position_cap, prepare_order, validate_client_id, PrepareOptions};
use broker_adapters::oanda::{InstrumentInfo, InstrumentTable};
use broker_adapters::{BrokerError, Dec, OrderKind, OrderRequest, Side, TimeInForce};
use serde_json::Value;

const TAG: &str = "rb1:run1:EUR/USD:buy";

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

fn info(name: &str, disp: u32, units: u32, min: &str, max: &str) -> InstrumentInfo {
    InstrumentInfo {
        name: name.to_string(),
        kind: "CURRENCY".into(),
        display_precision: disp,
        trade_units_precision: units,
        minimum_trade_size: d(min),
        maximum_order_units: d(max),
        margin_rate: d("0.02"),
        maximum_position_size: None,
    }
}

fn eurusd() -> InstrumentInfo {
    info("EUR_USD", 5, 0, "1", "100000000")
}

fn prep(req: &OrderRequest, i: &InstrumentInfo) -> Result<broker_adapters::oanda::PreparedOrder, BrokerError> {
    prepare_order(req, i, &PrepareOptions::default())
}

fn order_of(v: &Value) -> &Value {
    &v["order"]
}

// ---------------------------------------------------------------- instrument naming

#[test]
fn every_accepted_spelling_normalises_to_the_underscore_name() {
    for (input, want) in [
        ("EUR_USD", "EUR_USD"),
        ("EUR/USD", "EUR_USD"),
        ("EUR-USD", "EUR_USD"),
        ("EURUSD", "EUR_USD"),
        ("eur_usd", "EUR_USD"),
        ("eur/usd", "EUR_USD"),
        ("  EUR_USD  ", "EUR_USD"),
        ("Eur-Usd", "EUR_USD"),
        ("USD_JPY", "USD_JPY"),
        ("XAU_USD", "XAU_USD"),
        ("SPX500_USD", "SPX500_USD"),
        ("DE30/EUR", "DE30_EUR"),
        ("USDJPY", "USD_JPY"),
    ] {
        assert_eq!(normalize_instrument(input).unwrap(), want, "{input:?}");
    }
    assert_eq!(canonical_symbol("EURUSD").unwrap(), "EUR/USD");
    assert_eq!(canonical_symbol("SPX500_USD").unwrap(), "SPX500/USD");
    assert_eq!(quote_currency("EUR_USD"), Some("USD"));
}

#[test]
fn unknown_shapes_are_refused_not_guessed() {
    for bad in [
        "", " ", "EUR", "EURUS", "EURUSDX", "EUR/USD/X", "EUR_USD_", "_USD", "EUR_", "EUR_US", "EUR_USDT", "E_USD", "1UR_USD", "EUR USD",
        "EUR.USD", "\u{20ac}UR_USD", "EUR__USD", "EUR/USD_X", "EURUSD1", "EUR1SD", "SPX500USD", "EUR_USD\n", "EUR_1SD", "EUR:USD", "EUR_USD;",
        "EUR_US$", "../../x", "EUR_USD/../x",
    ] {
        let r = normalize_instrument(bad);
        // A trailing newline/space is trimmed by design; anything else must be refused.
        if bad.trim() == "EUR_USD" {
            assert_eq!(r.unwrap(), "EUR_USD");
        } else {
            assert!(matches!(r, Err(BrokerError::UnknownSymbol(_))), "{bad:?} must be refused, got {r:?}");
        }
    }
}

#[test]
fn the_instrument_table_looks_up_by_any_spelling_and_has_no_builtin_rows() {
    let t = InstrumentTable::new();
    assert!(t.is_empty() && t.lookup("EUR_USD").is_none());
    let t = InstrumentTable::from_rows([eurusd()]);
    for s in ["EUR_USD", "EUR/USD", "eurusd", "EUR-USD"] {
        assert!(t.lookup(s).is_some(), "{s}");
    }
    assert!(t.lookup("GBP_USD").is_none() && t.lookup("garbage!").is_none());
}

// ---------------------------------------------------------------- units: rounding and sign

#[test]
fn buy_units_are_positive_and_sell_units_are_negative_strings() {
    let buy = prep(&OrderRequest::market(TAG, "EUR/USD", Side::Buy, d("1000")), &eurusd()).unwrap();
    assert_eq!(buy.units, d("1000"));
    assert_eq!(order_of(&buy.to_json())["units"], "1000");
    let sell = prep(&OrderRequest::market(TAG, "EUR/USD", Side::Sell, d("1000")), &eurusd()).unwrap();
    assert_eq!(sell.units, d("-1000"));
    assert_eq!(sell.quantity, d("1000"), "the reported quantity is the magnitude");
    assert_eq!(order_of(&sell.to_json())["units"], "-1000");
    assert_eq!(sell.sent().side, Side::Sell);
    assert_eq!(sell.sent().quantity, d("1000"));
}

#[test]
fn units_round_down_to_the_instrument_precision_never_up() {
    let whole = eurusd();
    for (req, want) in [("1234.9", "1234"), ("1234.0001", "1234"), ("1.999999", "1"), ("99999.5", "99999")] {
        let p = prep(&OrderRequest::market(TAG, "EUR_USD", Side::Buy, d(req)), &whole).unwrap();
        assert_eq!(p.quantity, d(want), "{req}");
        assert!(p.quantity <= d(req), "never larger than the wish");
    }
    // a CFD with one unit decimal and a 0.1 minimum
    let cfd = info("DE30_EUR", 1, 1, "0.1", "2500");
    let p = prep(&OrderRequest::market(TAG, "DE30/EUR", Side::Sell, d("2.57")), &cfd).unwrap();
    assert_eq!(p.quantity, d("2.5"));
    assert_eq!(order_of(&p.to_json())["units"], "-2.5");
    assert!(matches!(
        prep(&OrderRequest::market(TAG, "DE30/EUR", Side::Buy, d("0.05")), &cfd),
        Err(BrokerError::QuantityRoundsToZero { .. })
    ));
}

#[test]
fn zero_below_minimum_and_above_maximum_are_refused_never_bumped() {
    let i = info("EUR_USD", 5, 0, "10", "5000");
    assert!(matches!(prep(&OrderRequest::market(TAG, "EUR_USD", Side::Buy, d("0.4")), &i), Err(BrokerError::QuantityRoundsToZero { .. })));
    match prep(&OrderRequest::market(TAG, "EUR_USD", Side::Buy, d("9")), &i) {
        Err(BrokerError::BelowMinQuantity { min, rounded, symbol }) => {
            assert_eq!((min, rounded), (d("10"), d("9")));
            assert_eq!(symbol, "EUR/USD");
        }
        other => panic!("{other:?}"),
    }
    assert!(prep(&OrderRequest::market(TAG, "EUR_USD", Side::Buy, d("10")), &i).is_ok());
    assert!(prep(&OrderRequest::market(TAG, "EUR_USD", Side::Buy, d("5000")), &i).is_ok());
    match prep(&OrderRequest::market(TAG, "EUR_USD", Side::Buy, d("5001")), &i) {
        Err(BrokerError::InvalidRequest(m)) => assert!(m.contains("maximumOrderUnits") && m.contains("refusing to truncate"), "{m}"),
        other => panic!("{other:?}"),
    }
    for q in ["0", "-5", "-0.1"] {
        assert!(matches!(prep(&OrderRequest::market(TAG, "EUR_USD", Side::Buy, d(q)), &i), Err(BrokerError::InvalidRequest(_))), "{q}");
    }
}

#[test]
fn a_symbol_that_does_not_match_the_instrument_row_is_refused() {
    assert!(matches!(prep(&OrderRequest::market(TAG, "GBP_USD", Side::Buy, d("1000")), &eurusd()), Err(BrokerError::InvalidRequest(_))));
    assert!(matches!(prep(&OrderRequest::market(TAG, "nonsense", Side::Buy, d("1000")), &eurusd()), Err(BrokerError::UnknownSymbol(_))));
}

// ---------------------------------------------------------------- market stays market

#[test]
fn a_market_order_with_a_reference_price_is_still_a_market_order() {
    // The legacy connector turned any priced signal into LIMIT/GTC. Regression test: it must not.
    for side in [Side::Buy, Side::Sell] {
        let mut req = OrderRequest::market(TAG, "EUR/USD", side, d("1000"));
        req.reference_price = Some(d("1.10050"));
        let p = prep(&req, &eurusd()).unwrap();
        assert!(p.is_market() && p.limit_price.is_none());
        let body = p.to_json();
        let o = order_of(&body);
        assert_eq!(o["type"], "MARKET");
        assert_eq!(o["timeInForce"], "FOK");
        assert_eq!(o["positionFill"], "DEFAULT");
        assert!(o.get("price").is_none(), "no price may be sent on a market order: {body}");
        assert!(!body.to_string().contains("1.10050"), "the reference price must not leak into the body: {body}");
        assert_eq!(p.sent().price, None);
    }
}

#[test]
fn a_limit_order_is_never_turned_into_a_market_order() {
    let req = OrderRequest::limit(TAG, "EUR/USD", Side::Buy, d("1000"), d("1.09500"));
    let body = prep(&req, &eurusd()).unwrap().to_json();
    assert_eq!(order_of(&body)["type"], "LIMIT");
    assert_eq!(order_of(&body)["price"], "1.09500");
    assert_eq!(order_of(&body)["timeInForce"], "GTC");
}

#[test]
fn the_body_is_exactly_the_documented_shape() {
    let body = prep(&OrderRequest::market(TAG, "EUR/USD", Side::Sell, d("2500")), &eurusd()).unwrap().to_json();
    let expected: Value = serde_json::from_str(
        r#"{"order":{"type":"MARKET","instrument":"EUR_USD","units":"-2500","timeInForce":"FOK","positionFill":"DEFAULT",
            "clientExtensions":{"id":"rb1:run1:EUR/USD:buy","tag":"mendl-rb"}}}"#,
    )
    .unwrap();
    assert_eq!(body, expected);
    // the top level has only `order`
    assert_eq!(body.as_object().unwrap().len(), 1);
}

// ---------------------------------------------------------------- time in force, position fill

#[test]
fn time_in_force_is_always_explicit_and_bad_combinations_are_refused() {
    let tif = |kind_limit: bool, tif: Option<TimeInForce>| -> Result<String, BrokerError> {
        let mut req = if kind_limit {
            OrderRequest::limit(TAG, "EUR_USD", Side::Buy, d("1000"), d("1.1"))
        } else {
            OrderRequest::market(TAG, "EUR_USD", Side::Buy, d("1000"))
        };
        req.time_in_force = tif;
        prep(&req, &eurusd()).map(|p| p.time_in_force.to_string())
    };
    assert_eq!(tif(false, None).unwrap(), "FOK");
    assert_eq!(tif(false, Some(TimeInForce::Ioc)).unwrap(), "IOC");
    assert!(matches!(tif(false, Some(TimeInForce::Gtc)), Err(BrokerError::InvalidRequest(_))), "a market order cannot rest");
    assert_eq!(tif(true, None).unwrap(), "GTC");
    assert_eq!(tif(true, Some(TimeInForce::Gtc)).unwrap(), "GTC");
    assert_eq!(tif(true, Some(TimeInForce::Ioc)).unwrap(), "IOC");
}

#[test]
fn reduce_only_maps_to_position_fill_and_unsupported_flags_are_refused() {
    let mut req = OrderRequest::market(TAG, "EUR_USD", Side::Sell, d("1000"));
    req.reduce_only = true;
    let body = prep(&req, &eurusd()).unwrap().to_json();
    assert_eq!(order_of(&body)["positionFill"], "REDUCE_ONLY");

    let mut req = OrderRequest::market(TAG, "EUR_USD", Side::Sell, d("1000"));
    req.validate_only = true;
    assert!(matches!(prep(&req, &eurusd()), Err(BrokerError::Unsupported(_))));
    let mut req = OrderRequest::limit(TAG, "EUR_USD", Side::Sell, d("1000"), d("1.1"));
    req.post_only = true;
    assert!(matches!(prep(&req, &eurusd()), Err(BrokerError::Unsupported(_))));
}

// ---------------------------------------------------------------- limit price rounding

#[test]
fn limit_prices_round_in_the_direction_never_worse_than_requested() {
    let i = eurusd();
    let buy = prep(&OrderRequest::limit(TAG, "EUR_USD", Side::Buy, d("1000"), d("1.234567")), &i).unwrap();
    assert_eq!(buy.limit_price, Some(d("1.23456")), "a buy rounds DOWN");
    let sell = prep(&OrderRequest::limit(TAG, "EUR_USD", Side::Sell, d("1000"), d("1.234561")), &i).unwrap();
    assert_eq!(sell.limit_price, Some(d("1.23457")), "a sell rounds UP");
    let exact = prep(&OrderRequest::limit(TAG, "EUR_USD", Side::Sell, d("1000"), d("1.23456")), &i).unwrap();
    assert_eq!(exact.limit_price, Some(d("1.23456")));
    assert_eq!(order_of(&exact.to_json())["price"], "1.23456");
    // JPY-style three decimals
    let jpy = info("USD_JPY", 3, 0, "1", "100000000");
    let p = prep(&OrderRequest::limit(TAG, "USD_JPY", Side::Buy, d("1000"), d("148.5049")), &jpy).unwrap();
    assert_eq!(order_of(&p.to_json())["price"], "148.504");
    // whole-number price is padded to the precision, not sent as a bare integer
    let p = prep(&OrderRequest::limit(TAG, "EUR_USD", Side::Buy, d("1000"), d("2")), &i).unwrap();
    assert_eq!(order_of(&p.to_json())["price"], "2.00000");
    for bad in ["0", "-1.1"] {
        assert!(matches!(prep(&OrderRequest::limit(TAG, "EUR_USD", Side::Buy, d("1000"), d(bad)), &i), Err(BrokerError::InvalidPrice(_))), "{bad}");
    }
    // a price that rounds to zero
    assert!(matches!(prep(&OrderRequest::limit(TAG, "EUR_USD", Side::Buy, d("1000"), d("0.000004")), &i), Err(BrokerError::InvalidPrice(_))));
}

#[test]
fn a_limit_request_keeps_its_kind_in_the_sent_record() {
    let p = prep(&OrderRequest::limit(TAG, "EUR_USD", Side::Buy, d("1000"), d("1.1")), &eurusd()).unwrap();
    assert_eq!(p.sent().price, Some(d("1.10000")));
    assert!(matches!(OrderRequest::limit(TAG, "EUR_USD", Side::Buy, d("1"), d("1.1")).kind, OrderKind::Limit { .. }));
}

// ---------------------------------------------------------------- idempotency key

#[test]
fn the_tag_is_the_client_extensions_id_verbatim() {
    for tag in [TAG, "rb1:fl:20260921T150000Z:EURUSD:1:0123456789abcdef", "a b", "x".repeat(128).as_str()] {
        let p = prep(&OrderRequest::market(tag, "EUR_USD", Side::Buy, d("1000")), &eurusd()).unwrap();
        assert_eq!(p.client_id, tag);
        let body = p.to_json();
        assert_eq!(order_of(&body)["clientExtensions"]["id"], tag, "never truncated or hashed");
        assert_eq!(order_of(&body)["clientExtensions"]["tag"], "mendl-rb");
    }
}

#[test]
fn bad_tags_are_refused() {
    let long = "x".repeat(129);
    for bad in ["", long.as_str(), "caf\u{e9}", "tab\there", "new\nline"] {
        assert!(matches!(validate_client_id(bad, None), Err(BrokerError::InvalidRequest(_))), "{bad:?}");
        assert!(prep(&OrderRequest::market(bad, "EUR_USD", Side::Buy, d("1000")), &eurusd()).is_err(), "{bad:?}");
    }
    assert!(validate_client_id("rb1:x", Some("rb1:")).is_ok());
    assert!(matches!(validate_client_id("other:x", Some("rb1:")), Err(BrokerError::InvalidRequest(_))));
    let opts = PrepareOptions { own_tag_prefix: Some("rb1:".into()), ..PrepareOptions::default() };
    assert!(prepare_order(&OrderRequest::market("other:x", "EUR_USD", Side::Buy, d("1000")), &eurusd(), &opts).is_err());
}

// ---------------------------------------------------------------- maximumPositionSize

fn capped(cap: &str) -> InstrumentInfo {
    InstrumentInfo { maximum_position_size: Some(d(cap)), ..eurusd() }
}

#[test]
fn maximum_position_size_parses_zero_and_absent_as_no_cap_and_a_positive_value_as_the_cap() {
    // MEASURED on a practice account: "0" on every instrument, meaning no cap.
    let row = |cap: Option<&str>| {
        let mut r = serde_json::json!({"name": "EUR_USD", "type": "CURRENCY", "displayPrecision": 5, "tradeUnitsPrecision": 0,
            "minimumTradeSize": "1", "maximumOrderUnits": "100000000", "marginRate": "0.02"});
        if let Some(c) = cap {
            r["maximumPositionSize"] = serde_json::json!(c);
        }
        serde_json::json!({"instruments": [r]}).to_string()
    };
    let cap_of = |cap: Option<&str>| InstrumentTable::from_instruments_json(&row(cap)).unwrap().lookup("EUR_USD").unwrap().maximum_position_size;
    assert_eq!(cap_of(Some("0")), None, "a zero cap is NOT a limit of zero units");
    assert_eq!(cap_of(Some("0.0")), None);
    assert_eq!(cap_of(None), None, "absent = no cap");
    assert_eq!(cap_of(Some("25000")), Some(d("25000")));
}

#[test]
fn a_zero_or_absent_cap_never_refuses_and_a_positive_one_refuses_instead_of_truncating() {
    let none = eurusd();
    assert!(check_position_cap(&none, d("0"), d("99999999")).is_ok(), "no cap: nothing to check");
    let c = capped("5000");
    assert!(check_position_cap(&c, d("0"), d("5000")).is_ok(), "exactly the cap is allowed");
    assert!(matches!(check_position_cap(&c, d("0"), d("5001")), Err(BrokerError::InvalidRequest(m)) if m.contains("maximumPositionSize") && m.contains("refusing to truncate")));
    assert!(check_position_cap(&c, d("-4000"), d("-1000")).is_ok());
    assert!(check_position_cap(&c, d("-4000"), d("-1001")).is_err(), "a short is capped too");
    // net arithmetic: long 4000 sold 9000 is -5000 (allowed), 9001 is -5001 (refused)
    assert!(check_position_cap(&c, d("4000"), d("-9000")).is_ok());
    assert!(check_position_cap(&c, d("4000"), d("-9001")).is_err());
    // a reduction is always allowed, even from over the cap; growing it is not
    assert!(check_position_cap(&c, d("6000"), d("-1")).is_ok());
    assert!(check_position_cap(&c, d("6000"), d("1")).is_err());
    assert!(check_position_cap(&c, d("6000"), d("-12000")).is_err(), "reducing through zero to -6000 grows the short side beyond the cap");
}
