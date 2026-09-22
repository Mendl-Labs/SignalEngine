//! Order, balance and cancel endpoints of the Kraken front end, driven with raw signed requests.
//! Money is compared as exact decimals, never as floats.

mod common;

use broker_adapters::Dec;
use common::*;
use fake_broker::testkit::{d, KrakenRig};
use fake_broker::{FakeBroker, FillPolicy, FillStep, OrderRule, PairSpec, DEFAULT_ACCOUNT};
use serde_json::Value;

fn dd(v: &Value) -> Dec {
    Dec::parse(v.as_str().unwrap_or_else(|| panic!("not a string: {v}"))).unwrap()
}

fn market(side: &str, pair: &str, vol: &str) -> Vec<(&'static str, String)> {
    vec![("pair", pair.to_string()), ("type", side.to_string()), ("ordertype", "market".to_string()), ("volume", vol.to_string())]
}

fn as_refs<'a>(v: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    v.iter().map(|(k, s)| (*k, s.as_str())).collect()
}

fn place(w: &Wire, params: &[(&'static str, String)]) -> String {
    let r = w.ok(ADD, &as_refs(params));
    r["txid"][0].as_str().unwrap().to_string()
}

fn limit(side: &str, pair: &str, vol: &str, price: &str) -> Vec<(&'static str, String)> {
    let mut v = market(side, pair, vol);
    v[2].1 = "limit".to_string();
    v.push(("price", price.to_string()));
    v
}

fn query(w: &Wire, txid: &str) -> Value {
    w.ok(QUERY, &[("txid", txid)])[txid].clone()
}

// ---------------------------------------------------------------- validate-only

#[test]
fn validate_only_returns_a_description_and_no_txid_and_creates_nothing() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let mut p = limit("buy", "XBTUSD", "0.0025", "59000.0");
    p.push(("validate", "true".to_string()));
    let r = w.ok(ADD, &as_refs(&p));
    assert_eq!(r["descr"]["order"], "buy 0.00250000 XBTUSD @ limit 59000.0");
    assert!(r.get("txid").is_none(), "validate-only must not return a txid: {r}");
    assert!(rig.handle.orders(DEFAULT_ACCOUNT).is_empty());
    assert_eq!(rig.handle.available(DEFAULT_ACCOUNT, "USD"), d("100000"), "nothing reserved");
    assert_eq!(w.ok(OPEN, &[])["open"].as_object().unwrap().len(), 0);
    rig.handle.assert_invariants();
}

#[test]
fn validate_only_still_checks_arguments_and_funds() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let mut too_big = market("buy", "XBTUSD", "100");
    too_big.push(("validate", "true".to_string()));
    assert_eq!(w.err(ADD, &as_refs(&too_big)), ["EOrder:Insufficient funds"]);
    let mut bad = market("buy", "XBTUSD", "0.00001");
    bad.push(("validate", "true".to_string()));
    assert_eq!(w.err(ADD, &as_refs(&bad)), ["EOrder:Order minimum not met"]);
}

#[test]
fn validate_only_does_not_consume_a_scripted_rule_but_a_reject_rule_applies() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.script_orders(OrderRule::next(FillPolicy::reject("EOrder:Insufficient funds")));
    let mut v = market("buy", "XBTUSD", "0.001");
    v.push(("validate", "true".to_string()));
    assert_eq!(w.err(ADD, &as_refs(&v)), ["EOrder:Insufficient funds"], "validation reports the scripted refusal");
    // the rule is still there for the real order
    assert_eq!(w.err(ADD, &as_refs(&market("buy", "XBTUSD", "0.001"))), ["EOrder:Insufficient funds"]);
    // and now it is spent
    place(&w, &market("buy", "XBTUSD", "0.001"));
}

// ---------------------------------------------------------------- fills and fees

#[test]
fn market_buy_fills_at_the_ask_with_the_taker_fee_and_exact_balances() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let txid = place(&w, &market("buy", "XBTUSD", "0.01"));
    let o = query(&w, &txid);
    assert_eq!(o["status"], "closed");
    assert_eq!(dd(&o["vol"]), d("0.01"));
    assert_eq!(dd(&o["vol_exec"]), d("0.01"));
    assert_eq!(dd(&o["price"]), d("60000.1"), "ask, not last");
    assert_eq!(dd(&o["cost"]), d("600.001"));
    assert_eq!(dd(&o["fee"]), d("1.5600026"), "0.26 percent taker fee on the cost, in USD");
    assert_eq!(o["descr"]["type"], "buy");
    assert_eq!(o["descr"]["ordertype"], "market");
    assert_eq!(o["descr"]["pair"], "XBTUSD");
    assert!(o["closetm"].is_number());
    assert_eq!(rig.handle.balance("main", "BTC"), d("0.01"));
    assert_eq!(rig.handle.balance("main", "USD"), d("99398.4389974"));
    rig.handle.assert_invariants();
}

#[test]
fn market_sell_fills_at_the_bid() {
    let rig = KrakenRig::new();
    rig.handle.set_balance("main", "BTC", "1");
    let w = Wire::new(&rig);
    let txid = place(&w, &market("sell", "XBTUSD", "0.5"));
    let o = query(&w, &txid);
    assert_eq!(dd(&o["price"]), d("60000"));
    assert_eq!(dd(&o["cost"]), d("30000"));
    assert_eq!(dd(&o["fee"]), d("78"));
    assert_eq!(rig.handle.balance("main", "BTC"), d("0.5"));
    assert_eq!(rig.handle.balance("main", "USD"), d("129922"), "proceeds minus fee");
    rig.handle.assert_invariants();
}

#[test]
fn a_crossing_limit_fills_at_the_touch_not_at_the_limit() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let txid = place(&w, &limit("buy", "XBTUSD", "0.01", "61000.0"));
    let o = query(&w, &txid);
    assert_eq!(o["status"], "closed");
    assert_eq!(dd(&o["price"]), d("60000.1"), "price improvement");
    assert_eq!(o["descr"]["ordertype"], "limit");
    assert_eq!(dd(&o["descr"]["price"]), d("61000"));
    assert_eq!(dd(&o["fee"]), d("1.5600026"), "taker: it crossed at placement");
}

#[test]
fn a_resting_limit_fills_when_the_market_moves_through_it_and_pays_the_maker_fee() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let txid = place(&w, &limit("buy", "XBTUSD", "0.01", "59000.0"));
    assert_eq!(query(&w, &txid)["status"], "open");
    assert_eq!(rig.handle.balance("main", "BTC"), d("0"));
    assert_eq!(rig.handle.available("main", "USD"), d("99408.466"), "590 + 0.26 percent reserved");
    assert_eq!(rig.handle.balance("main", "USD"), d("100000"), "Kraken's Balance includes funds held by open orders");

    rig.handle.set_price("BTC/USD", "59500"); // ask 59500.1: still above the limit
    assert_eq!(query(&w, &txid)["status"], "open");
    rig.handle.set_price("BTC/USD", "58999.9"); // ask 59000.0 touches the limit
    let o = query(&w, &txid);
    assert_eq!(o["status"], "closed");
    assert_eq!(dd(&o["price"]), d("59000"), "resting orders fill at their limit");
    assert_eq!(dd(&o["fee"]), d("0.944"), "maker fee 0.16 percent of 590");
    assert_eq!(rig.handle.balance("main", "USD"), d("99409.056"));
    assert_eq!(rig.handle.balance("main", "BTC"), d("0.01"));
    rig.handle.assert_invariants();
}

#[test]
fn fee_rates_are_configurable_per_exchange_and_per_account() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.set_fees("0", "0.001");
    let t1 = place(&w, &market("buy", "XBTUSD", "0.01"));
    assert_eq!(dd(&query(&w, &t1)["fee"]), d("0.600001"));
    rig.handle.set_account_fees("main", Some((d("0"), d("0"))));
    let t2 = place(&w, &market("buy", "XBTUSD", "0.01"));
    assert_eq!(dd(&query(&w, &t2)["fee"]), d("0"));
    rig.handle.assert_invariants();
}

#[test]
fn funds_held_by_open_orders_are_not_available_to_new_ones_until_cancelled() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    // 1.6 BTC @ 60000 = 96000 (+0.26 percent = 96249.6) rests; a second 0.1 BTC (6015.6) no longer fits.
    let big = place(&w, &limit("buy", "XBTUSD", "1.6", "60000.0"));
    assert_eq!(query(&w, &big)["status"], "open");
    assert_eq!(w.err(ADD, &as_refs(&limit("buy", "XBTUSD", "0.1", "60000.0"))), ["EOrder:Insufficient funds"]);
    w.ok(CANCEL, &[("txid", &big)]);
    place(&w, &limit("buy", "XBTUSD", "0.1", "60000.0"));
    // selling more than is free is refused too
    rig.handle.set_balance("main", "BTC", "1");
    place(&w, &limit("sell", "XBTUSD", "0.8", "70000.0"));
    assert_eq!(w.err(ADD, &as_refs(&limit("sell", "XBTUSD", "0.3", "70000.0"))), ["EOrder:Insufficient funds"]);
    rig.handle.assert_invariants();
}

// ---------------------------------------------------------------- argument validation

#[test]
fn argument_validation_matches_the_pair_rules() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let err = |p: Vec<(&'static str, String)>| w.err(ADD, &as_refs(&p));

    assert_eq!(err(market("buy", "XBTUSD", "0.123456789")), ["EGeneral:Invalid arguments:volume"], "more than lot_decimals");
    assert_eq!(err(market("buy", "XBTUSD", "0")), ["EGeneral:Invalid arguments:volume"]);
    assert_eq!(err(market("buy", "XBTUSD", "abc")), ["EGeneral:Invalid arguments:volume"]);
    assert_eq!(err(market("buy", "XBTUSD", "0.00001")), ["EOrder:Order minimum not met"]);
    assert_eq!(err(market("buy", "ETHUSD", "0.001")), ["EOrder:Order minimum not met"]);
    assert_eq!(err(limit("buy", "XBTUSD", "0.01", "59000.05")), ["EGeneral:Invalid arguments:price"], "more than pair_decimals");
    assert_eq!(err(limit("buy", "ETHUSD", "0.01", "3000.005")), ["EGeneral:Invalid arguments:price"]);
    assert_eq!(err(limit("buy", "XBTUSD", "0.01", "0")), ["EGeneral:Invalid arguments:price"]);
    assert_eq!(err(limit("buy", "XBTUSD", "0.0001", "100.0")), ["EOrder:Cost minimum not met"], "0.01 USD < 0.5");
    assert_eq!(err(market("buy", "FOOUSD", "1")), ["EQuery:Unknown asset pair"]);
    assert_eq!(err(market("hold", "XBTUSD", "1")), ["EGeneral:Invalid arguments:type"]);
    let mut p = market("buy", "XBTUSD", "1");
    p[2].1 = "stop-loss".into();
    assert_eq!(err(p), ["EGeneral:Invalid arguments:ordertype"]);
    let mut p = market("buy", "XBTUSD", "1");
    p[2].1 = "limit".into();
    assert_eq!(err(p), ["EGeneral:Invalid arguments:price"], "limit without a price");
    let mut p = market("buy", "XBTUSD", "0.01");
    p.push(("timeinforce", "GTD".into()));
    assert_eq!(err(p), ["EGeneral:Invalid arguments:timeinforce"]);
    let mut p = market("buy", "XBTUSD", "0.01");
    p.push(("oflags", "viqc".into()));
    assert_eq!(err(p), ["EGeneral:Invalid arguments:oflags"]);
    let mut p = market("buy", "XBTUSD", "0.01");
    p.push(("reduce_only", "true".into()));
    assert_eq!(err(p), ["EGeneral:Invalid arguments:reduce_only"]);
    let mut p = market("buy", "XBTUSD", "0.01");
    p.push(("userref", "abc".into()));
    assert_eq!(err(p), ["EGeneral:Invalid arguments:userref"]);
    let mut p = market("buy", "XBTUSD", "0.01");
    p.push(("validate", "maybe".into()));
    assert_eq!(err(p), ["EGeneral:Invalid arguments:validate"]);
    assert!(rig.handle.orders("main").is_empty(), "no refused order left anything behind");
    // ... and the boundaries that ARE valid
    place(&w, &market("buy", "XBTUSD", "0.0001"));
    place(&w, &limit("buy", "ETHUSD", "0.002", "3000.00"));
}

#[test]
fn post_only_orders_that_would_cross_are_refused_and_resting_ones_accepted() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let mut crossing = limit("buy", "XBTUSD", "0.01", "61000.0");
    crossing.push(("oflags", "post".into()));
    assert_eq!(w.err(ADD, &as_refs(&crossing)), ["EOrder:Post only order"]);
    let mut resting = limit("buy", "XBTUSD", "0.01", "59000.0");
    resting.push(("oflags", "fciq,post".into()));
    let txid = place(&w, &resting);
    assert_eq!(query(&w, &txid)["oflags"], "fciq,post");
    let mut market_post = market("buy", "XBTUSD", "0.01");
    market_post.push(("oflags", "post".into()));
    assert_eq!(w.err(ADD, &as_refs(&market_post)), ["EGeneral:Invalid arguments:oflags"]);
}

#[test]
fn immediate_or_cancel_limits_never_rest() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let mut away = limit("buy", "XBTUSD", "0.01", "59000.0");
    away.push(("timeinforce", "IOC".into()));
    let t = place(&w, &away);
    let o = query(&w, &t);
    assert_eq!(o["status"], "canceled");
    assert_eq!(dd(&o["vol_exec"]), d("0"));
    assert!(rig.handle.live_orders("main").is_empty());
    let mut through = limit("buy", "XBTUSD", "0.01", "61000.0");
    through.push(("timeinforce", "IOC".into()));
    assert_eq!(query(&w, &place(&w, &through))["status"], "closed");
    let mut bad = market("buy", "XBTUSD", "0.01");
    bad.push(("timeinforce", "IOC".into()));
    assert_eq!(w.err(ADD, &as_refs(&bad)), ["EGeneral:Invalid arguments:timeinforce"]);
}

#[test]
fn pair_trading_status_restricts_order_entry() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.set_pair_status("BTC/USD", "cancel_only");
    assert_eq!(w.err(ADD, &as_refs(&market("buy", "XBTUSD", "0.01"))), ["EService:Market in cancel_only mode"]);
    rig.handle.set_pair_status("BTC/USD", "limit_only");
    assert_eq!(w.err(ADD, &as_refs(&market("buy", "XBTUSD", "0.01"))), ["EService:Market in limit_only mode"]);
    place(&w, &limit("buy", "XBTUSD", "0.01", "59000.0"));
    rig.handle.set_pair_status("BTC/USD", "online");
    place(&w, &market("buy", "XBTUSD", "0.01"));
    let pairs = w.get(&format!("{BASE}/0/public/AssetPairs?pair=XBTUSD"));
    assert_eq!(pairs["result"]["XXBTZUSD"]["status"], "online");
}

// ---------------------------------------------------------------- userref and listings

#[test]
fn userref_is_echoed_and_filters_open_query_and_closed_orders() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let with = |userref: &str, mut p: Vec<(&'static str, String)>| {
        p.push(("userref", userref.to_string()));
        p
    };
    let a = place(&w, &with("111", limit("buy", "XBTUSD", "0.01", "59000.0")));
    let b = place(&w, &with("222", limit("buy", "XBTUSD", "0.01", "58000.0")));
    let c = place(&w, &limit("buy", "XBTUSD", "0.01", "57000.0")); // no userref
    assert_eq!(query(&w, &a)["userref"], 111);
    assert_eq!(query(&w, &b)["userref"], 222);
    assert!(query(&w, &c)["userref"].is_null());

    let open = |filter: &[(&str, &str)]| -> Vec<String> {
        let r = w.ok(OPEN, filter);
        let mut v: Vec<String> = r["open"].as_object().unwrap().keys().cloned().collect();
        v.sort();
        v
    };
    let mut all = vec![a.clone(), b.clone(), c.clone()];
    all.sort();
    assert_eq!(open(&[]), all);
    assert_eq!(open(&[("userref", "111")]), vec![a.clone()]);
    assert_eq!(open(&[("userref", "222")]), vec![b.clone()]);
    assert!(open(&[("userref", "333")]).is_empty());
    assert_eq!(w.err(OPEN, &[("userref", "x")]), ["EGeneral:Invalid arguments:userref"]);

    // QueryOrders: the userref filter is applied on top of the txid list
    assert!(w.ok(QUERY, &[("txid", &a), ("userref", "111")]).get(&a).is_some());
    assert_eq!(w.ok(QUERY, &[("txid", &a), ("userref", "222")]).as_object().unwrap().len(), 0);
    let both = w.ok(QUERY, &[("txid", &format!("{a},{b}"))]);
    assert_eq!(both.as_object().unwrap().len(), 2);

    // closed orders, filtered too
    w.ok(CANCEL, &[("txid", &a)]);
    w.ok(CANCEL, &[("txid", &b)]);
    let closed = w.ok(CLOSED, &[("userref", "111")]);
    assert_eq!(closed["count"], 1);
    assert_eq!(closed["closed"].as_object().unwrap().keys().collect::<Vec<_>>(), vec![&a]);
    assert_eq!(w.ok(CLOSED, &[])["count"], 2);
}

#[test]
fn querying_an_unknown_order_or_another_accounts_order_is_an_error() {
    let secret_b = "c2Vjb25kLWFjY291bnQtc2VjcmV0";
    let broker = FakeBroker::builder()
        .pair(PairSpec::btc_usd(), "60000")
        .account(fake_broker::AccountSpec::new("main").balance("USD", "100000"))
        .account(fake_broker::AccountSpec::new("other").key("KEY-B", secret_b).balance("USD", "100000"))
        .build();
    let other_orders = {
        let h = broker.handle();
        h.add_foreign_order("other", fake_broker::ForeignOrder::limit("BTC/USD", broker_adapters::Side::Buy, "0.01", "59000.0"))
    };
    let rig = KrakenRig::with_broker(broker);
    let w = Wire::new(&rig);
    assert_eq!(w.err(QUERY, &[("txid", "OAAAAA-BBBBB-CCCCCC")]), ["EOrder:Unknown order"]);
    assert_eq!(w.err(QUERY, &[("txid", &other_orders)]), ["EOrder:Unknown order"], "not visible across accounts");
    assert_eq!(w.err(CANCEL, &[("txid", &other_orders)]), ["EOrder:Unknown order"], "nor cancellable across accounts");
    assert!(rig.handle.order(&other_orders).unwrap().status.is_live());
    assert_eq!(w.ok(OPEN, &[])["open"].as_object().unwrap().len(), 0);
}

#[test]
fn closed_orders_are_paginated_with_the_total_count() {
    let broker = fake_broker::FakeBrokerBuilder::standard().closed_page_size(2).build();
    let rig = KrakenRig::with_broker(broker);
    let w = Wire::new(&rig);
    for _ in 0..3 {
        place(&w, &market("buy", "XBTUSD", "0.001"));
        rig.handle.advance_secs(1);
    }
    let page1 = w.ok(CLOSED, &[]);
    assert_eq!(page1["count"], 3, "count is the total, not the page size");
    assert_eq!(page1["closed"].as_object().unwrap().len(), 2);
    let page2 = w.ok(CLOSED, &[("ofs", "2")]);
    assert_eq!(page2["closed"].as_object().unwrap().len(), 1);
    let ids: std::collections::BTreeSet<_> = page1["closed"].as_object().unwrap().keys().chain(page2["closed"].as_object().unwrap().keys()).collect();
    assert_eq!(ids.len(), 3, "pages do not overlap");
}

// ---------------------------------------------------------------- balances

#[test]
fn balance_uses_kraken_asset_codes_and_leaves_out_empty_assets() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.set_balance("main", "ETH2.S", "1.5");
    rig.handle.set_balance("main", "USDT", "250");
    rig.handle.set_balance("main", "EUR", "0");
    place(&w, &market("buy", "XBTUSD", "0.01"));
    let b = w.ok(BALANCE, &[]);
    let keys: std::collections::BTreeSet<&str> = b.as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(keys, ["ETH2.S", "USDT", "XXBT", "ZUSD"].into_iter().collect());
    assert_eq!(dd(&b["XXBT"]), d("0.01"));
    // sell it all again: the BTC line disappears
    place(&w, &market("sell", "XBTUSD", "0.01"));
    let b = w.ok(BALANCE, &[]);
    assert!(b.get("XXBT").is_none(), "{b}");
}

#[test]
fn trade_balance_reports_mark_to_market_equity() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.set_balance("main", "BTC", "0.5");
    rig.handle.set_balance("main", "ETH", "2");
    // 100000 + 0.5 * 60000 + 2 * 3000 = 136000 (last prices; Kraken marks holdings, not the touch)
    let tb = w.ok(TRADE_BALANCE, &[]);
    assert_eq!(dd(&tb["e"]), d("136000"));
    assert_eq!(dd(&tb["eb"]), d("136000"));
    rig.handle.set_price("BTC/USD", "50000");
    assert_eq!(dd(&w.ok(TRADE_BALANCE, &[("asset", "ZUSD")])["e"]), d("131000"));
    assert_eq!(w.err(TRADE_BALANCE, &[("asset", "ZJPY")]), ["EGeneral:Invalid arguments:asset"]);
}

// ---------------------------------------------------------------- cancel

#[test]
fn cancel_by_txid_and_by_userref_and_the_partial_fill_stays() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let mut p = limit("buy", "XBTUSD", "0.01", "59000.0");
    p.push(("userref", "77".into()));
    let a = place(&w, &p);
    let mut p2 = limit("buy", "XBTUSD", "0.02", "58000.0");
    p2.push(("userref", "77".into()));
    let b = place(&w, &p2);
    rig.handle.script_fill(&a, "0.004", None).unwrap();

    // cancel by userref: both orders
    let r = w.ok(CANCEL, &[("txid", "77")]);
    assert_eq!(r["count"], 2);
    assert!(r.get("pending").is_none());
    let oa = query(&w, &a);
    assert_eq!(oa["status"], "canceled");
    assert_eq!(oa["reason"], "User requested");
    assert_eq!(dd(&oa["vol_exec"]), d("0.004"), "cancel keeps what already executed");
    assert_eq!(dd(&oa["cost"]), d("236"), "0.004 at the 59000 limit");
    let ob = query(&w, &b);
    assert_eq!(ob["status"], "canceled");
    assert_eq!(dd(&ob["vol_exec"]), d("0"));
    assert_eq!(rig.handle.balance("main", "BTC"), d("0.004"));
    assert_eq!(rig.handle.available("main", "USD"), rig.handle.balance("main", "USD"), "reservations released");
    rig.handle.assert_invariants();
}

#[test]
fn cancelling_something_that_is_not_open_is_an_unknown_order_error() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let filled = place(&w, &market("buy", "XBTUSD", "0.01"));
    assert_eq!(w.err(CANCEL, &[("txid", &filled)]), ["EOrder:Unknown order"], "already closed");
    assert_eq!(w.err(CANCEL, &[("txid", "OAAAAA-BBBBB-CCCCCC")]), ["EOrder:Unknown order"]);
    assert_eq!(w.err(CANCEL, &[("txid", "12345")]), ["EOrder:Unknown order"], "userref with no open orders");
    assert_eq!(w.err(CANCEL, &[]), ["EGeneral:Invalid arguments:txid"]);
    let lim = place(&w, &limit("buy", "XBTUSD", "0.01", "59000.0"));
    w.ok(CANCEL, &[("txid", &lim)]);
    assert_eq!(w.err(CANCEL, &[("txid", &lim)]), ["EOrder:Unknown order"], "cancelling twice");
}

#[test]
fn a_deferred_cancel_answers_pending_and_the_order_can_still_fill_before_it_is_processed() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let t = place(&w, &limit("buy", "XBTUSD", "0.01", "59000.0"));
    rig.handle.defer_next_cancels(1);
    let r = w.ok(CANCEL, &[("txid", &t)]);
    assert_eq!(r["count"], 1);
    assert_eq!(r["pending"][0], t.as_str());
    assert_eq!(query(&w, &t)["status"], "open", "not yet cancelled");
    rig.handle.script_fill(&t, "0.004", None).unwrap(); // the race: it fills while the cancel is in flight
    assert_eq!(rig.handle.settle_pending_cancels(), 1);
    let o = query(&w, &t);
    assert_eq!(o["status"], "canceled");
    assert_eq!(dd(&o["vol_exec"]), d("0.004"));
    rig.handle.assert_invariants();
}

#[test]
fn scripted_order_rules_pick_orders_by_pair_side_and_userref() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.script_orders(
        OrderRule::next(FillPolicy::partial(vec![FillStep::fraction("0.5")])).pair("ETH/USD").side(broker_adapters::Side::Buy),
    );
    // BTC order does not match the ETH rule
    let btc = place(&w, &market("buy", "XBTUSD", "0.01"));
    assert_eq!(query(&w, &btc)["status"], "closed");
    let eth = place(&w, &market("buy", "ETHUSD", "0.1"));
    let o = query(&w, &eth);
    assert_eq!(o["status"], "open");
    assert_eq!(dd(&o["vol_exec"]), d("0.05"));
    // the rule was single-use
    let eth2 = place(&w, &market("buy", "ETHUSD", "0.1"));
    assert_eq!(query(&w, &eth2)["status"], "closed");
}
