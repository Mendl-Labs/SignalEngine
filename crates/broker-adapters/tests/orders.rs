//! Order preparation: rounding, minimum sizes, flags, userref mapping, pair table loading.

use broker_adapters::kraken::order::{prepare_order, PrepareOptions};
use broker_adapters::kraken::pairs::{normalize_asset, PairSource, PairTable};
use broker_adapters::kraken::userref::{candidate, UserrefEntry, UserrefMap};
use broker_adapters::types::{BalanceKind, OrderKind, OrderRequest, Side, TimeInForce};
use broker_adapters::{BrokerError, Dec};

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

fn prep(req: &OrderRequest) -> Result<broker_adapters::kraken::order::PreparedOrder, BrokerError> {
    let table = PairTable::builtin();
    let pair = table.lookup(&req.symbol).ok_or_else(|| BrokerError::UnknownSymbol(req.symbol.clone()))?;
    prepare_order(req, pair, 777, &PrepareOptions::default())
}

fn params(req: &OrderRequest) -> Vec<(String, String)> {
    prep(req).unwrap().to_params().unwrap()
}

fn get<'a>(p: &'a [(String, String)], k: &str) -> Option<&'a str> {
    p.iter().find(|(kk, _)| kk == k).map(|(_, v)| v.as_str())
}

// ---------------------------------------------------------------- volume rounding

#[test]
fn volume_is_rounded_down_to_lot_decimals_never_up() {
    let r = OrderRequest::market("t", "BTC/USD", Side::Buy, d("0.123456789999"));
    let p = params(&r);
    assert_eq!(get(&p, "volume"), Some("0.12345678"));
    // 0.00019999999 must not round up to 0.0002
    let r = OrderRequest::market("t", "BTC/USD", Side::Sell, d("0.000199999999"));
    assert_eq!(get(&params(&r), "volume"), Some("0.00019999"));
}

#[test]
fn volume_exactly_representable_keeps_full_fixed_width() {
    let r = OrderRequest::market("t", "ETH/USD", Side::Buy, d("1.5"));
    assert_eq!(get(&params(&r), "volume"), Some("1.50000000"));
}

#[test]
fn volume_that_rounds_to_zero_is_refused() {
    let r = OrderRequest::market("t", "BTC/USD", Side::Buy, d("0.000000009"));
    assert!(matches!(prep(&r), Err(BrokerError::QuantityRoundsToZero { .. })));
}

#[test]
fn non_positive_volume_is_refused() {
    for q in ["0", "-1"] {
        let r = OrderRequest::market("t", "BTC/USD", Side::Buy, d(q));
        assert!(matches!(prep(&r), Err(BrokerError::InvalidRequest(_))), "{q}");
    }
}

// ---------------------------------------------------------------- minimum size

#[test]
fn below_minimum_order_size_is_refused_not_bumped() {
    // builtin BTC minimum is 0.0001
    let r = OrderRequest::limit("t", "BTC/USD", Side::Buy, d("0.00009999"), d("60000"));
    match prep(&r) {
        Err(BrokerError::BelowMinQuantity { min, rounded, .. }) => {
            assert_eq!(min, d("0.0001"));
            assert_eq!(rounded, d("0.00009999"));
        }
        other => panic!("expected BelowMinQuantity, got {other:?}"),
    }
}

#[test]
fn exactly_the_minimum_is_allowed() {
    let r = OrderRequest::limit("t", "BTC/USD", Side::Buy, d("0.0001"), d("60000"));
    assert!(prep(&r).is_ok()); // cost 6.0 >= costmin 0.5
}

#[test]
fn rounding_that_pushes_below_minimum_is_refused() {
    // 0.000100009 is above the minimum as requested but stays 0.00010000 at 8 dp: allowed;
    // with an override raising ordermin to 0.00010001 the ROUNDED volume is what is checked.
    let mut table = PairTable::builtin();
    table.apply_overrides_json(r#"{"XBTUSD": {"ordermin": "0.00010001"}}"#).unwrap();
    let pair = table.lookup("XBTUSD").unwrap();
    let r = OrderRequest::market("t", "BTC/USD", Side::Buy, d("0.000100009"));
    let e = prepare_order(&r, pair, 1, &PrepareOptions::default()).unwrap_err();
    assert!(matches!(e, BrokerError::BelowMinQuantity { .. }), "{e:?}");
}

#[test]
fn minimum_cost_is_checked_for_limit_orders_and_for_market_with_reference_price() {
    // 0.0001 BTC at 4000 = 0.4 < 0.5 costmin
    let r = OrderRequest::limit("t", "BTC/USD", Side::Buy, d("0.0001"), d("4000"));
    assert!(matches!(prep(&r), Err(BrokerError::BelowMinCost { .. })));
    let mut m = OrderRequest::market("t", "BTC/USD", Side::Buy, d("0.0001"));
    m.reference_price = Some(d("4000"));
    assert!(matches!(prep(&m), Err(BrokerError::BelowMinCost { .. })));
    // without a reference price the check is skipped and that is recorded
    m.reference_price = None;
    assert!(prep(&m).unwrap().cost_check_skipped);
}

// ---------------------------------------------------------------- price rounding

#[test]
fn buy_limit_price_rounds_down_sell_rounds_up() {
    // BTC/USD has 1 price decimal
    let buy = OrderRequest::limit("t", "BTC/USD", Side::Buy, d("0.01"), d("61234.56"));
    assert_eq!(get(&params(&buy), "price"), Some("61234.5"));
    let sell = OrderRequest::limit("t", "BTC/USD", Side::Sell, d("0.01"), d("61234.51"));
    assert_eq!(get(&params(&sell), "price"), Some("61234.6"));
    // ETH/USD has 2
    let buy = OrderRequest::limit("t", "ETH/USD", Side::Buy, d("1"), d("3000.129"));
    assert_eq!(get(&params(&buy), "price"), Some("3000.12"));
    let sell = OrderRequest::limit("t", "ETH/USD", Side::Sell, d("1"), d("3000.121"));
    assert_eq!(get(&params(&sell), "price"), Some("3000.13"));
}

#[test]
fn price_on_the_tick_is_unchanged_and_fixed_width() {
    let r = OrderRequest::limit("t", "BTC/USD", Side::Buy, d("0.01"), d("37500"));
    assert_eq!(get(&params(&r), "price"), Some("37500.0"));
}

#[test]
fn tick_size_coarser_than_decimals_is_honoured() {
    let mut table = PairTable::builtin();
    table.apply_overrides_json(r#"{"XBTUSD": {"tick_size": "0.5"}}"#).unwrap();
    let pair = table.lookup("XBTUSD").unwrap();
    let buy = OrderRequest::limit("t", "BTC/USD", Side::Buy, d("0.01"), d("100.7"));
    let sell = OrderRequest::limit("t", "BTC/USD", Side::Sell, d("0.01"), d("100.7"));
    let pb = prepare_order(&buy, pair, 1, &PrepareOptions::default()).unwrap();
    let ps = prepare_order(&sell, pair, 1, &PrepareOptions::default()).unwrap();
    assert_eq!(pb.price.unwrap(), d("100.5"));
    assert_eq!(ps.price.unwrap(), d("101"));
}

#[test]
fn non_positive_price_refused() {
    let r = OrderRequest::limit("t", "BTC/USD", Side::Buy, d("0.01"), d("0"));
    assert!(matches!(prep(&r), Err(BrokerError::InvalidPrice(_))));
    let r = OrderRequest::limit("t", "BTC/USD", Side::Buy, d("0.01"), d("0.04"));
    assert!(matches!(prep(&r), Err(BrokerError::InvalidPrice(_)))); // floors to 0.0 at 1 dp
}

// ---------------------------------------------------------------- params, flags

#[test]
fn limit_order_param_list_is_complete_and_ordered() {
    let mut r = OrderRequest::limit("t", "BTC/USD", Side::Buy, d("0.0025"), d("61234.5"));
    r.time_in_force = Some(TimeInForce::Ioc);
    r.post_only = true;
    r.validate_only = true;
    let p = params(&r);
    let keys: Vec<&str> = p.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["pair", "type", "ordertype", "volume", "price", "timeinforce", "oflags", "userref", "validate"]);
    assert_eq!(get(&p, "pair"), Some("XBTUSD"));
    assert_eq!(get(&p, "type"), Some("buy"));
    assert_eq!(get(&p, "ordertype"), Some("limit"));
    assert_eq!(get(&p, "volume"), Some("0.00250000"));
    assert_eq!(get(&p, "timeinforce"), Some("IOC"));
    assert_eq!(get(&p, "oflags"), Some("post"));
    assert_eq!(get(&p, "userref"), Some("777"));
    assert_eq!(get(&p, "validate"), Some("true"));
}

#[test]
fn market_order_has_no_price_and_no_validate_by_default() {
    let r = OrderRequest::market("t", "ETH/USD", Side::Sell, d("0.5"));
    let p = params(&r);
    assert_eq!(get(&p, "ordertype"), Some("market"));
    assert!(get(&p, "price").is_none() && get(&p, "validate").is_none() && get(&p, "reduce_only").is_none());
    assert_eq!(get(&p, "pair"), Some("ETHUSD"));
}

#[test]
fn force_validate_overrides_the_request() {
    let table = PairTable::builtin();
    let pair = table.lookup("BTC/USD").unwrap();
    let r = OrderRequest::market("t", "BTC/USD", Side::Buy, d("0.001"));
    let p = prepare_order(&r, pair, 5, &PrepareOptions { force_validate: true, ..Default::default() }).unwrap();
    assert!(p.validate && get(&p.to_params().unwrap(), "validate") == Some("true"));
}

#[test]
fn incoherent_flags_are_refused() {
    let mut r = OrderRequest::market("t", "BTC/USD", Side::Buy, d("0.001"));
    r.post_only = true;
    assert!(matches!(prep(&r), Err(BrokerError::InvalidRequest(_))));
    let mut r = OrderRequest::market("t", "BTC/USD", Side::Buy, d("0.001"));
    r.time_in_force = Some(TimeInForce::Gtc);
    assert!(matches!(prep(&r), Err(BrokerError::InvalidRequest(_))));
}

#[test]
fn reduce_only_is_refused_on_spot_and_sent_only_when_enabled() {
    let mut r = OrderRequest::market("t", "BTC/USD", Side::Sell, d("0.001"));
    r.reduce_only = true;
    assert!(matches!(prep(&r), Err(BrokerError::Unsupported(_))));
    let table = PairTable::builtin();
    let pair = table.lookup("BTC/USD").unwrap();
    let p = prepare_order(&r, pair, 5, &PrepareOptions { allow_reduce_only: true, ..Default::default() }).unwrap();
    assert_eq!(get(&p.to_params().unwrap(), "reduce_only"), Some("true"));
}

#[test]
fn pair_that_is_not_online_is_refused() {
    let mut table = PairTable::builtin();
    table.apply_overrides_json(r#"{"ETHUSD": {"status": "cancel_only"}}"#).unwrap();
    let pair = table.lookup("ETH/USD").unwrap();
    let r = OrderRequest::market("t", "ETH/USD", Side::Buy, d("1"));
    assert!(matches!(
        prepare_order(&r, pair, 1, &PrepareOptions::default()),
        Err(BrokerError::PairNotTradable { .. })
    ));
}

#[test]
fn unknown_symbol_is_refused() {
    let r = OrderRequest::market("t", "DOGE/USD", Side::Buy, d("100"));
    assert!(matches!(prep(&r), Err(BrokerError::UnknownSymbol(_))));
}

// ---------------------------------------------------------------- symbol / asset mapping

#[test]
fn symbol_aliases_resolve_to_the_same_pair() {
    let t = PairTable::builtin();
    for name in ["BTC/USD", "btc/usd", "XBTUSD", "XXBTZUSD", "XBT/USD"] {
        assert_eq!(t.lookup(name).unwrap().canonical, "BTC/USD", "{name}");
    }
    for name in ["ETH/USD", "ETHUSD", "XETHZUSD"] {
        assert_eq!(t.lookup(name).unwrap().canonical, "ETH/USD", "{name}");
    }
    assert!(t.lookup("BTCUSD").is_none()); // not a Kraken name; must be explicit rather than guessed
}

#[test]
fn asset_code_normalisation() {
    assert_eq!(normalize_asset("XXBT"), ("BTC".to_string(), BalanceKind::Spot));
    assert_eq!(normalize_asset("XBT"), ("BTC".to_string(), BalanceKind::Spot));
    assert_eq!(normalize_asset("XETH"), ("ETH".to_string(), BalanceKind::Spot));
    assert_eq!(normalize_asset("ZUSD"), ("USD".to_string(), BalanceKind::Spot));
    assert_eq!(normalize_asset("ZEUR"), ("EUR".to_string(), BalanceKind::Spot));
    assert_eq!(normalize_asset("USDT"), ("USDT".to_string(), BalanceKind::Spot));
    assert_eq!(normalize_asset("ETH2.S"), ("ETH2".to_string(), BalanceKind::Earn));
    assert_eq!(normalize_asset("XBT.M"), ("BTC".to_string(), BalanceKind::Earn));
    // real tickers that merely start with X/Z must not be mangled
    assert_eq!(normalize_asset("ZEUS").0, "ZEUS");
    assert_eq!(normalize_asset("XION").0, "XION");
}

#[test]
fn pair_table_from_asset_pairs_response_and_overrides() {
    let json = r#"{"error":[],"result":{
      "XXBTZUSD":{"altname":"XBTUSD","wsname":"XBT/USD","base":"XXBT","quote":"ZUSD","pair_decimals":1,"lot_decimals":8,
                  "ordermin":"0.0001","costmin":"0.5","tick_size":"0.1","status":"online"},
      "XETHZUSD":{"altname":"ETHUSD","wsname":"ETH/USD","base":"XETH","quote":"ZUSD","pair_decimals":2,"lot_decimals":8,
                  "ordermin":"0.002","costmin":"0.5","tick_size":"0.01","status":"online"},
      "XBTUSD.d":{"altname":"XBTUSD.d","base":"XXBT","quote":"ZUSD","pair_decimals":1,"lot_decimals":8,"ordermin":"0.0001"}
    }}"#;
    let mut t = PairTable::from_asset_pairs_json(json).unwrap();
    assert_eq!(t.pairs().len(), 2, "dark-pool rows are skipped");
    let btc = t.lookup("BTC/USD").unwrap();
    assert_eq!(btc.source, PairSource::AssetPairs);
    assert_eq!(btc.rest_name, "XXBTZUSD");
    assert_eq!(btc.status.as_deref(), Some("online"));
    t.apply_overrides_json(r#"{"XXBTZUSD": {"ordermin": "0.0005"}}"#).unwrap();
    let btc = t.lookup("BTC/USD").unwrap();
    assert_eq!(btc.order_min, d("0.0005"));
    assert_eq!(btc.source, PairSource::Override);
    assert!(t.apply_overrides_json(r#"{"NOPE": {"ordermin": "1"}}"#).is_err());
    assert!(PairTable::from_asset_pairs_json("[1,2]").is_err());
}

#[test]
fn builtin_table_is_marked_unverified_builtin() {
    assert!(PairTable::builtin().pairs().iter().all(|p| p.source == PairSource::Builtin));
}

// ---------------------------------------------------------------- userref mapping

#[test]
fn userref_is_deterministic_in_range_and_idempotent() {
    let mut m = UserrefMap::new();
    let a = m.assign("run-2026-09-21T14:00Z:sleeveA:BTC/USD:buy").unwrap();
    assert_eq!(a, candidate("run-2026-09-21T14:00Z:sleeveA:BTC/USD:buy"));
    assert!(a >= 1);
    assert_eq!(m.assign("run-2026-09-21T14:00Z:sleeveA:BTC/USD:buy").unwrap(), a);
    // a fresh map derives the same value: determinism across restarts when there was no collision
    let mut m2 = UserrefMap::new();
    assert_eq!(m2.assign("run-2026-09-21T14:00Z:sleeveA:BTC/USD:buy").unwrap(), a);
    assert_eq!(m.tag_for(a), Some("run-2026-09-21T14:00Z:sleeveA:BTC/USD:buy"));
    assert!(UserrefMap::new().assign("").is_err());
}

#[test]
fn userref_values_are_always_in_1_to_i32_max() {
    for i in 0..5000 {
        let v = candidate(&format!("tag-{i}"));
        assert!(v >= 1, "{i}: {v}");
    }
}

#[test]
fn userref_collisions_are_probed_and_stay_stable_across_reload() {
    // Force a collision by pre-seeding the map with another tag on tag B's candidate value.
    let tag_b = "tag-b";
    let cand = candidate(tag_b);
    let mut m = UserrefMap::from_entries([UserrefEntry { tag: "squatter".into(), userref: cand }]).unwrap();
    let b = m.assign(tag_b).unwrap();
    assert_ne!(b, cand, "collision must be resolved, not shared");
    assert_eq!(b, if cand == i32::MAX { 1 } else { cand + 1 });
    assert_eq!(m.tag_for(cand), Some("squatter"));
    assert_eq!(m.tag_for(b), Some(tag_b));
    // Persist and reload: the probed value is preserved (the hash alone could not recover it).
    let json = m.to_json();
    let mut reloaded = UserrefMap::from_json(&json).unwrap();
    assert_eq!(reloaded.get_userref(tag_b), Some(b));
    assert_eq!(reloaded.assign(tag_b).unwrap(), b);
    assert_eq!(reloaded, m);
}

#[test]
fn userref_from_entries_rejects_conflicts_and_bad_values() {
    let e = |t: &str, r: i32| UserrefEntry { tag: t.into(), userref: r };
    assert!(UserrefMap::from_entries([e("a", 5), e("b", 5)]).is_err(), "two tags one userref");
    assert!(UserrefMap::from_entries([e("a", 5), e("a", 6)]).is_err(), "one tag two userrefs");
    assert!(UserrefMap::from_entries([e("a", 0)]).is_err());
    assert!(UserrefMap::from_entries([e("a", -3)]).is_err());
    assert!(UserrefMap::from_entries([e("a", 5), e("a", 5)]).is_ok(), "exact duplicate is harmless");
}

#[test]
fn many_tags_get_unique_userrefs() {
    let mut m = UserrefMap::new();
    let mut seen = std::collections::BTreeSet::new();
    for i in 0..20_000 {
        let r = m.assign(&format!("run-{i}:BTC/USD:buy")).unwrap();
        assert!(seen.insert(r), "userref {r} reused");
    }
    assert_eq!(m.len(), 20_000);
}

#[test]
fn kind_helper_smoke() {
    // OrderKind carries the price so incoherent requests are unrepresentable.
    assert!(matches!(OrderRequest::limit("t", "BTC/USD", Side::Buy, d("1"), d("2")).kind, OrderKind::Limit { .. }));
}
