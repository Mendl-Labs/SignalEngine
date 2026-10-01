//! Pure order preparation for Alpaca: rounding, minimums, time-in-force, price ticks, tags,
//! the asset table and its built-in fallback. Nothing here touches a transport.

use broker_adapters::alpaca::assets::{AssetInfo, AssetSource, AssetTable};
use broker_adapters::alpaca::order::{normalize_symbol, prepare_order, PrepareOptions};
use broker_adapters::types::{OrderRequest, Side, TimeInForce};
use broker_adapters::{BrokerError, Dec};

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!("fixtures/alpaca/", $name))
    };
}

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

fn opts() -> PrepareOptions {
    PrepareOptions { allow_extended_hours: false, min_notional: d("1"), own_tag_prefix: None, refuse_builtin_assets: false }
}

fn spy() -> AssetInfo {
    AssetInfo::parse_json(fixture!("asset_spy.json")).unwrap()
}
fn whole_only() -> AssetInfo {
    AssetInfo::parse_json(fixture!("asset_whole_only.json")).unwrap()
}

fn mkt(tag: &str, sym: &str, side: Side, qty: &str) -> OrderRequest {
    OrderRequest::market(tag, sym, side, d(qty))
}

// ---------------------------------------------------------------- quantity rounding

#[test]
fn fractionable_quantity_is_rounded_down_to_nine_decimal_places() {
    let p = prepare_order(&mkt("t", "SPY", Side::Buy, "1.2345678919"), &spy(), &opts()).unwrap();
    assert_eq!(p.quantity, d("1.234567891"));
    assert!(p.fractional);
    // never rounds up, even when the tenth digit is 9
    let p = prepare_order(&mkt("t", "SPY", Side::Buy, "0.9999999999"), &spy(), &opts()).unwrap();
    assert_eq!(p.quantity, d("0.999999999"));
    // exact 9 dp survives untouched
    let p = prepare_order(&mkt("t", "SPY", Side::Buy, "3.000000001"), &spy(), &opts()).unwrap();
    assert_eq!(p.quantity, d("3.000000001"));
    assert_eq!(p.to_json()["qty"], "3.000000001");
}

#[test]
fn rounding_never_exceeds_the_request_and_loses_less_than_one_unit() {
    let unit = d("0.000000001");
    for raw in ["0.123456789012", "17.0000000009", "250.999999999999", "0.000000002", "9999.5", "0.5"] {
        let req = d(raw);
        let q = prepare_order(&OrderRequest::market("t", "SPY", Side::Buy, req), &spy(), &opts()).unwrap().quantity;
        let diff = req.checked_add(Dec::new(-q.units(), q.scale()).unwrap()).unwrap();
        assert!(!diff.is_negative(), "{raw}: rounded UP to {q}");
        assert!(diff < unit, "{raw}: lost {diff} (>= 1e-9) going to {q}");
    }
}

#[test]
fn whole_share_symbols_round_down_to_integers() {
    let a = whole_only();
    let p = prepare_order(&mkt("t", "BRK.A", Side::Buy, "3.999"), &a, &opts()).unwrap();
    assert_eq!(p.quantity, d("3"));
    assert!(!p.fractional);
    assert_eq!(p.to_json()["qty"], "3");
    // half a share is not enough
    match prepare_order(&mkt("t", "BRK.A", Side::Buy, "0.9999"), &a, &opts()) {
        Err(BrokerError::QuantityRoundsToZero { symbol, .. }) => assert_eq!(symbol, "BRK.A"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn quantity_below_the_nine_dp_floor_rounds_to_zero_and_is_refused() {
    match prepare_order(&mkt("t", "SPY", Side::Buy, "0.0000000009"), &spy(), &opts()) {
        Err(BrokerError::QuantityRoundsToZero { rounded, .. }) => assert!(rounded.is_zero()),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        prepare_order(&mkt("t", "SPY", Side::Buy, "0"), &spy(), &opts()),
        Err(BrokerError::InvalidRequest(_))
    ));
    assert!(matches!(
        prepare_order(&mkt("t", "SPY", Side::Buy, "-1"), &spy(), &opts()),
        Err(BrokerError::InvalidRequest(_))
    ));
}

#[test]
fn min_order_size_and_trade_increment_from_the_asset_row_are_honoured() {
    let a = AssetInfo::parse_json(fixture!("asset_min_order.json")).unwrap(); // min 5, increment 0.5
    assert_eq!(prepare_order(&mkt("t", "MINO", Side::Buy, "7.3"), &a, &opts()).unwrap().quantity, d("7"));
    match prepare_order(&mkt("t", "MINO", Side::Buy, "4.9"), &a, &opts()) {
        Err(BrokerError::BelowMinQuantity { min, rounded, .. }) => {
            assert_eq!(min, d("5"));
            assert_eq!(rounded, d("4.5"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn min_notional_is_enforced_when_a_price_is_known_and_recorded_when_not() {
    let mut req = mkt("t", "SPY", Side::Buy, "0.001");
    req.reference_price = Some(d("500")); // $0.50 < $1
    match prepare_order(&req, &spy(), &opts()) {
        Err(BrokerError::BelowMinCost { cost, min, .. }) => {
            assert_eq!(cost, d("0.5"));
            assert_eq!(min, d("1"));
        }
        other => panic!("{other:?}"),
    }
    req.reference_price = Some(d("1000")); // exactly $1 passes
    assert!(prepare_order(&req, &spy(), &opts()).is_ok());
    req.reference_price = None;
    assert!(prepare_order(&req, &spy(), &opts()).unwrap().cost_check_skipped);
    // a limit order uses its own (rounded) price
    let lim = OrderRequest::limit("t", "SPY", Side::Buy, d("0.001"), d("400"));
    assert!(matches!(prepare_order(&lim, &spy(), &opts()), Err(BrokerError::BelowMinCost { .. })));
}

// ---------------------------------------------------------------- time in force

#[test]
fn time_in_force_is_always_sent_and_market_orders_are_day() {
    let p = prepare_order(&mkt("t", "SPY", Side::Buy, "2"), &spy(), &opts()).unwrap();
    assert_eq!(p.time_in_force, "day");
    assert_eq!(p.to_json()["time_in_force"], "day");
    // the caller may not ask for something else on a market order
    let mut req = mkt("t", "SPY", Side::Buy, "2");
    req.time_in_force = Some(TimeInForce::Gtc);
    assert!(matches!(prepare_order(&req, &spy(), &opts()), Err(BrokerError::InvalidRequest(_))));
}

#[test]
fn limit_orders_default_to_day_and_map_gtc_and_ioc() {
    let mut req = OrderRequest::limit("t", "SPY", Side::Buy, d("2"), d("500"));
    assert_eq!(prepare_order(&req, &spy(), &opts()).unwrap().time_in_force, "day");
    req.time_in_force = Some(TimeInForce::Gtc);
    assert_eq!(prepare_order(&req, &spy(), &opts()).unwrap().time_in_force, "gtc");
    req.time_in_force = Some(TimeInForce::Ioc);
    assert_eq!(prepare_order(&req, &spy(), &opts()).unwrap().time_in_force, "ioc");
}

#[test]
fn fractional_quantity_with_gtc_or_ioc_is_refused_not_silently_changed() {
    for tif in [TimeInForce::Gtc, TimeInForce::Ioc] {
        let mut req = OrderRequest::limit("t", "SPY", Side::Buy, d("1.5"), d("500"));
        req.time_in_force = Some(tif);
        match prepare_order(&req, &spy(), &opts()) {
            Err(BrokerError::InvalidRequest(m)) => assert!(m.contains("fractional") && m.contains("day"), "{m}"),
            other => panic!("{other:?}"),
        }
    }
    // but a fractional request that rounds to a WHOLE number is not fractional
    let mut req = OrderRequest::limit("t", "BRK.A", Side::Buy, d("2.7"), d("500"));
    req.time_in_force = Some(TimeInForce::Gtc);
    assert_eq!(prepare_order(&req, &whole_only(), &opts()).unwrap().time_in_force, "gtc");
}

#[test]
fn extended_hours_flag_is_only_sent_on_limit_day_orders() {
    let mut o = opts();
    o.allow_extended_hours = true;
    let lim = OrderRequest::limit("t", "SPY", Side::Buy, d("2"), d("500"));
    let p = prepare_order(&lim, &spy(), &o).unwrap();
    assert!(p.extended_hours);
    assert_eq!(p.to_json()["extended_hours"], true);
    let mut gtc = lim.clone();
    gtc.time_in_force = Some(TimeInForce::Gtc);
    assert!(!prepare_order(&gtc, &spy(), &o).unwrap().extended_hours);
    let m = prepare_order(&mkt("t", "SPY", Side::Buy, "2"), &spy(), &o).unwrap();
    assert!(!m.extended_hours);
    assert!(m.to_json().get("extended_hours").is_none());
    // and never without the config flag
    assert!(prepare_order(&lim, &spy(), &opts()).unwrap().to_json().get("extended_hours").is_none());
}

// ---------------------------------------------------------------- limit price

#[test]
fn limit_prices_round_to_the_tick_in_the_direction_never_worse_than_requested() {
    let buy = OrderRequest::limit("t", "SPY", Side::Buy, d("2"), d("512.109"));
    assert_eq!(prepare_order(&buy, &spy(), &opts()).unwrap().limit_price, Some(d("512.1")));
    let sell = OrderRequest::limit("t", "SPY", Side::Sell, d("2"), d("512.101"));
    assert_eq!(prepare_order(&sell, &spy(), &opts()).unwrap().limit_price, Some(d("512.11")));
    // sub-dollar stocks use a 0.0001 tick
    let sub = OrderRequest::limit("t", "SPY", Side::Buy, d("100"), d("0.123456"));
    assert_eq!(prepare_order(&sub, &spy(), &opts()).unwrap().limit_price, Some(d("0.1234")));
    // an asset-supplied increment wins
    let a = AssetInfo::parse_json(fixture!("asset_min_order.json")).unwrap(); // price_increment 0.05
    let r = OrderRequest::limit("t", "MINO", Side::Buy, d("10"), d("20.07"));
    assert_eq!(prepare_order(&r, &a, &opts()).unwrap().limit_price, Some(d("20.05")));
    let r = OrderRequest::limit("t", "MINO", Side::Sell, d("10"), d("20.01"));
    assert_eq!(prepare_order(&r, &a, &opts()).unwrap().limit_price, Some(d("20.05")));
    assert!(matches!(
        prepare_order(&OrderRequest::limit("t", "SPY", Side::Buy, d("2"), d("0")), &spy(), &opts()),
        Err(BrokerError::InvalidPrice(_))
    ));
}

#[test]
fn request_body_uses_exact_decimal_strings() {
    let lim = OrderRequest::limit("mvp1:x", "spy", Side::Sell, d("0.000000001"), d("1234567.891"));
    let mut o = opts();
    o.min_notional = d("0"); // isolate the body format from the notional check
    let j = prepare_order(&lim, &spy(), &o).unwrap().to_json();
    assert_eq!(j["symbol"], "SPY");
    assert_eq!(j["qty"], "0.000000001");
    assert_eq!(j["limit_price"], "1234567.9"); // a sell rounds UP to the tick
    assert_eq!(j["side"], "sell");
    assert_eq!(j["type"], "limit");
    assert_eq!(j["client_order_id"], "mvp1:x");
    // strings, not JSON numbers
    assert!(j["qty"].is_string() && j["limit_price"].is_string());
}

// ---------------------------------------------------------------- tags and unsupported flags

#[test]
fn client_order_id_length_and_charset_are_enforced_and_never_truncated() {
    let ok = "a".repeat(128);
    assert_eq!(prepare_order(&mkt(&ok, "SPY", Side::Buy, "2"), &spy(), &opts()).unwrap().client_order_id, ok);
    for bad in ["".to_string(), "a".repeat(129), "tag\nwith-newline".to_string(), "tag-é".to_string()] {
        assert!(matches!(
            prepare_order(&mkt(&bad, "SPY", Side::Buy, "2"), &spy(), &opts()),
            Err(BrokerError::InvalidRequest(_))
        ));
    }
}

#[test]
fn own_tag_prefix_is_required_when_configured() {
    let mut o = opts();
    o.own_tag_prefix = Some("mvp1:".into());
    assert!(prepare_order(&mkt("mvp1:run1", "SPY", Side::Buy, "2"), &spy(), &o).is_ok());
    assert!(matches!(
        prepare_order(&mkt("other:run1", "SPY", Side::Buy, "2"), &spy(), &o),
        Err(BrokerError::InvalidRequest(_))
    ));
}

#[test]
fn flags_alpaca_equities_cannot_honour_are_refused_not_dropped() {
    for f in [
        |r: &mut OrderRequest| r.validate_only = true,
        |r: &mut OrderRequest| r.reduce_only = true,
        |r: &mut OrderRequest| r.post_only = true,
    ] {
        let mut req = OrderRequest::limit("t", "SPY", Side::Buy, d("2"), d("500"));
        f(&mut req);
        assert!(matches!(prepare_order(&req, &spy(), &opts()), Err(BrokerError::Unsupported(_))), "{req:?}");
    }
}

#[test]
fn symbols_are_normalised_and_crypto_pairs_are_refused() {
    assert_eq!(normalize_symbol(" spy ").unwrap(), "SPY");
    assert_eq!(normalize_symbol("brk.b").unwrap(), "BRK.B");
    assert!(matches!(normalize_symbol("BTC/USD"), Err(BrokerError::Unsupported(_))));
    for bad in ["", "S PY", "SPY;DROP", "../etc", "-SPY", "WAYTOOLONGSYMBOL123"] {
        assert!(matches!(normalize_symbol(bad), Err(BrokerError::InvalidRequest(_))), "{bad:?}");
    }
}

// ---------------------------------------------------------------- asset table

#[test]
fn asset_rows_parse_from_api_json() {
    let a = spy();
    assert!(a.tradable && a.fractionable);
    assert_eq!(a.source, AssetSource::Api);
    assert_eq!(a.min_order_size, Dec::ZERO);
    let w = whole_only();
    assert!(!w.fractionable);
    let m = AssetInfo::parse_json(fixture!("asset_min_order.json")).unwrap();
    assert_eq!((m.min_order_size, m.min_trade_increment, m.price_increment), (d("5"), Some(d("0.5")), Some(d("0.05"))));
}

#[test]
fn a_row_without_tradable_is_malformed_and_missing_fractionable_means_whole_shares() {
    assert!(matches!(AssetInfo::parse_json(fixture!("asset_missing_tradable.json")), Err(BrokerError::Malformed(_))));
    let a = AssetInfo::parse_json(r#"{"symbol":"X","tradable":true}"#).unwrap();
    assert!(!a.fractionable);
    assert!(AssetInfo::parse_json("not json").is_err());
}

#[test]
fn inactive_or_untradable_assets_are_refused() {
    let a = AssetInfo::parse_json(fixture!("asset_inactive.json")).unwrap();
    match prepare_order(&mkt("t", "DEAD", Side::Buy, "2"), &a, &opts()) {
        Err(BrokerError::PairNotTradable { symbol, status }) => {
            assert_eq!(symbol, "DEAD");
            assert!(status.contains("tradable=false"), "{status}");
        }
        other => panic!("{other:?}"),
    }
    let a = AssetInfo::parse_json(r#"{"symbol":"X","tradable":true,"status":"inactive","fractionable":true}"#).unwrap();
    assert!(matches!(prepare_order(&mkt("t", "X", Side::Buy, "2"), &a, &opts()), Err(BrokerError::PairNotTradable { .. })));
}

#[test]
fn builtin_fallback_has_the_five_sleeve_symbols_marked_unverified() {
    let t = AssetTable::builtin();
    assert_eq!(t.len(), 5);
    for s in ["SPY", "EFA", "IEF", "DBC", "VNQ", "spy"] {
        let a = t.lookup(s).unwrap_or_else(|| panic!("{s}"));
        assert_eq!(a.source, AssetSource::Builtin, "{s}");
        assert!(a.tradable);
    }
    assert!(t.lookup("AAPL").is_none());
    assert!(AssetTable::empty().is_empty());
}

#[test]
fn builtin_rows_can_be_refused_and_are_replaced_by_api_rows() {
    let builtin = AssetTable::builtin().lookup("SPY").unwrap().clone();
    let mut o = opts();
    o.refuse_builtin_assets = true;
    assert!(matches!(prepare_order(&mkt("t", "SPY", Side::Buy, "2"), &builtin, &o), Err(BrokerError::Config(_))));
    // an API-sourced row passes the same check
    assert!(prepare_order(&mkt("t", "SPY", Side::Buy, "2"), &spy(), &o).is_ok());
    // and upsert replaces a builtin row
    let mut t = AssetTable::builtin();
    t.upsert(spy());
    assert_eq!(t.lookup("SPY").unwrap().source, AssetSource::Api);
    assert_eq!(t.len(), 5);
}

#[test]
fn assets_list_json_loads_into_a_table() {
    let t = AssetTable::from_assets_json(fixture!("assets_list.json")).unwrap();
    assert_eq!(t.len(), 3);
    assert!(!t.lookup("brk.a").unwrap().fractionable);
    assert!(t.lookup("IEF").unwrap().fractionable);
    assert_eq!(AssetTable::from_assets_json(fixture!("asset_spy.json")).unwrap().len(), 1);
    assert!(AssetTable::from_assets_json("42").is_err());
}
