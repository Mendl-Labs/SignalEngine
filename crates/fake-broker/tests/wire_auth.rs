//! Authentication and transport-level behaviour of the Kraken front end, driven with hand-built
//! signed requests (no adapter) so the raw responses are visible.

mod common;

use broker_adapters::kraken::auth::{build_private_request, KrakenCredentials};
use broker_adapters::transport::{HttpMethod, HttpRequest, TransportError};
use common::*;
use fake_broker::testkit::KrakenRig;
use fake_broker::{default_secret_b64, AccountSpec, FakeBroker, KeyPermissions, DEFAULT_API_KEY};

#[test]
fn a_correctly_signed_request_is_accepted() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let r = w.ok(BALANCE, &[]);
    assert_eq!(r["ZUSD"], "100000");
}

#[test]
fn wrong_secret_is_rejected_with_invalid_signature() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let other = KrakenCredentials::new(DEFAULT_API_KEY, &base64_of(b"some-other-secret")).unwrap();
    let req = build_private_request(&other, BASE, BALANCE, 5, &[]);
    let body = w.send(&req).unwrap().body;
    assert_eq!(errors(&serde_json::from_str(&body).unwrap()), ["EAPI:Invalid signature"]);
}

fn base64_of(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[test]
fn the_signature_covers_path_and_body() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let params = pairs(&[("pair", "XBTUSD"), ("type", "buy"), ("ordertype", "market"), ("volume", "0.001")]);

    // signed for AddOrder, delivered to CancelOrder
    let mut req = build_private_request(&creds(), BASE, ADD, 10, &params);
    req.url = format!("{BASE}{CANCEL}");
    let v: serde_json::Value = serde_json::from_str(&w.send(&req).unwrap().body).unwrap();
    assert_eq!(errors(&v), ["EAPI:Invalid signature"], "path is part of the signed message");

    // body edited after signing
    let mut req = build_private_request(&creds(), BASE, ADD, 11, &params);
    req.body = Some(req.body.unwrap().replace("volume=0.001", "volume=1.000"));
    let v: serde_json::Value = serde_json::from_str(&w.send(&req).unwrap().body).unwrap();
    assert_eq!(errors(&v), ["EAPI:Invalid signature"], "body is part of the signed message");

    // nonce edited after signing (the body keeps a different nonce than was signed)
    let mut req = build_private_request(&creds(), BASE, BALANCE, 12, &[]);
    req.body = Some("nonce=13".to_string());
    let v: serde_json::Value = serde_json::from_str(&w.send(&req).unwrap().body).unwrap();
    assert_eq!(errors(&v), ["EAPI:Invalid signature"]);

    assert!(rig.handle.orders("main").is_empty(), "nothing was applied");
}

#[test]
fn missing_signature_missing_key_and_unknown_key() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let good = build_private_request(&creds(), BASE, BALANCE, 20, &[]);

    let mut no_sign = good.clone();
    no_sign.headers.retain(|(k, _)| k != "API-Sign");
    let v: serde_json::Value = serde_json::from_str(&w.send(&no_sign).unwrap().body).unwrap();
    assert_eq!(errors(&v), ["EAPI:Invalid signature"]);

    let mut no_key = good.clone();
    no_key.headers.retain(|(k, _)| k != "API-Key");
    let v: serde_json::Value = serde_json::from_str(&w.send(&no_key).unwrap().body).unwrap();
    assert_eq!(errors(&v), ["EAPI:Invalid key"]);

    let stranger = KrakenCredentials::new("STRANGER-KEY", &default_secret_b64()).unwrap();
    let req = build_private_request(&stranger, BASE, BALANCE, 21, &[]);
    let v: serde_json::Value = serde_json::from_str(&w.send(&req).unwrap().body).unwrap();
    assert_eq!(errors(&v), ["EAPI:Invalid key"]);

    let mut no_nonce = good;
    no_nonce.body = Some(String::new());
    let v: serde_json::Value = serde_json::from_str(&w.send(&no_nonce).unwrap().body).unwrap();
    assert_eq!(errors(&v), ["EAPI:Invalid nonce"]);
}

#[test]
fn a_revoked_key_is_invalid() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    w.ok(BALANCE, &[]);
    rig.handle.revoke_key(DEFAULT_API_KEY);
    assert_eq!(w.err(BALANCE, &[]), ["EAPI:Invalid key"]);
}

// ---------------------------------------------------------------- nonce

#[test]
fn nonce_must_be_strictly_greater_than_the_highest_seen() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let b = |n| errors(&w.post_with_nonce(BALANCE, n, &[]));
    assert!(b(100).is_empty());
    assert_eq!(b(100), ["EAPI:Invalid nonce"], "equal is rejected");
    assert_eq!(b(99), ["EAPI:Invalid nonce"], "lower is rejected");
    assert_eq!(b(1), ["EAPI:Invalid nonce"]);
    assert!(b(101).is_empty(), "the next integer is fine");
    assert_eq!(b(101), ["EAPI:Invalid nonce"]);
    assert!(b(1_000_000).is_empty(), "gaps are fine");
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), Some(1_000_000));
}

#[test]
fn a_rejected_nonce_does_not_lower_or_raise_the_highest() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    w.ok_nonce(500);
    assert_eq!(errors(&w.post_with_nonce(BALANCE, 400, &[])), ["EAPI:Invalid nonce"]);
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), Some(500));
    assert!(errors(&w.post_with_nonce(BALANCE, 501, &[])).is_empty());
}

impl Wire<'_> {
    fn ok_nonce(&self, n: u64) {
        assert!(errors(&self.post_with_nonce(BALANCE, n, &[])).is_empty());
    }
}

#[test]
fn a_bad_signature_does_not_consume_the_nonce() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    w.ok_nonce(100);
    let other = KrakenCredentials::new(DEFAULT_API_KEY, &base64_of(b"some-other-secret")).unwrap();
    let req = build_private_request(&other, BASE, BALANCE, 900, &[]);
    let v: serde_json::Value = serde_json::from_str(&w.send(&req).unwrap().body).unwrap();
    assert_eq!(errors(&v), ["EAPI:Invalid signature"]);
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), Some(100), "an unauthenticated request must not move the nonce");
    w.ok_nonce(101);
}

#[test]
fn the_nonce_is_consumed_by_a_request_that_fails_later() {
    // Kraken accepts the nonce before it looks at the arguments: replaying it is an error even
    // though the first attempt did nothing useful.
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let bad_args = [("pair", "NOPE"), ("type", "buy"), ("ordertype", "market"), ("volume", "1")];
    assert_eq!(errors(&w.post_with_nonce(ADD, 700, &bad_args)), ["EQuery:Unknown asset pair"]);
    assert_eq!(errors(&w.post_with_nonce(ADD, 700, &bad_args)), ["EAPI:Invalid nonce"]);
}

#[test]
fn nonces_are_tracked_per_api_key() {
    let secret_b = base64_of(b"second-account-secret");
    let broker = FakeBroker::builder()
        .pair(fake_broker::PairSpec::btc_usd(), "60000")
        .account(AccountSpec::new("a").balance("USD", "10"))
        .account(AccountSpec::new("b").key("KEY-B", &secret_b).balance("USD", "20"))
        .build();
    let rig = KrakenRig::with_broker(broker);
    let w = Wire::new(&rig);
    w.ok_nonce(1_000_000);
    let cb = KrakenCredentials::new("KEY-B", &secret_b).unwrap();
    let req = build_private_request(&cb, BASE, BALANCE, 5, &[]);
    let v: serde_json::Value = serde_json::from_str(&w.send(&req).unwrap().body).unwrap();
    assert!(errors(&v).is_empty(), "key B has its own nonce space: {v}");
    assert_eq!(v["result"]["ZUSD"], "20", "and its own account");
    assert_eq!(rig.handle.highest_nonce("KEY-B"), Some(5));
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), Some(1_000_000));
}

#[test]
fn a_nonce_that_is_not_an_integer_is_invalid() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    for text in ["abc", "-5", "1.5", "18446744073709551616"] {
        let body = format!("nonce={text}");
        let req = HttpRequest {
            method: HttpMethod::Post,
            url: format!("{BASE}{BALANCE}"),
            headers: vec![
                ("API-Key".into(), DEFAULT_API_KEY.into()),
                ("API-Sign".into(), sign_text(BALANCE, text, &body)),
            ],
            body: Some(body),
        };
        let v: serde_json::Value = serde_json::from_str(&w.send(&req).unwrap().body).unwrap();
        assert_eq!(errors(&v), ["EAPI:Invalid nonce"], "nonce {text:?}");
    }
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), None);
}

// ---------------------------------------------------------------- permissions, methods, hosts

#[test]
fn read_only_keys_cannot_place_or_cancel() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.set_key_permissions(DEFAULT_API_KEY, KeyPermissions::read_only());
    assert!(errors(&w.post(BALANCE, &[])).is_empty(), "a read-only key can still look");
    let add = [("pair", "XBTUSD"), ("type", "buy"), ("ordertype", "market"), ("volume", "0.001")];
    assert_eq!(w.err(ADD, &add), ["EGeneral:Permission denied"]);
    assert_eq!(w.err(CANCEL, &[("txid", "1")]), ["EGeneral:Permission denied"]);
    assert!(rig.handle.orders("main").is_empty());
    rig.handle.set_key_permissions(
        DEFAULT_API_KEY,
        KeyPermissions { query_funds: false, ..KeyPermissions::all() },
    );
    assert_eq!(w.err(BALANCE, &[]), ["EGeneral:Permission denied"]);
    assert_eq!(w.err(TRADE_BALANCE, &[]), ["EGeneral:Permission denied"]);
}

#[test]
fn private_endpoints_need_post_and_unknown_methods_are_404() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let req = HttpRequest { method: HttpMethod::Get, url: format!("{BASE}{BALANCE}"), headers: Vec::new(), body: None };
    assert_eq!(w.send(&req).unwrap().status, 405);
    let req = HttpRequest { method: HttpMethod::Get, url: format!("{BASE}/0/public/Nope"), headers: Vec::new(), body: None };
    assert_eq!(w.send(&req).unwrap().status, 404);
}

#[test]
fn a_request_for_another_host_fails_to_connect() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let req = build_private_request(&creds(), "https://staging.example.test", BALANCE, 5, &[]);
    match w.send(&req) {
        Err(TransportError::ConnectFailed(m)) => assert!(m.contains("staging.example.test"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert!(!rig.handle.requests()[0].reached_exchange);
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), None);
}

#[test]
fn public_endpoints_need_no_credentials() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    let t = w.get(&format!("{BASE}/0/public/Ticker?pair=XBTUSD"));
    assert!(errors(&t).is_empty());
    let e = &t["result"]["XXBTZUSD"];
    assert_eq!(e["b"][0], "60000.00000");
    assert_eq!(e["a"][0], "60000.10000");
    assert_eq!(e["c"][0], "60000.00000");
    let t = w.get(&format!("{BASE}/0/public/Ticker?pair=NOPE"));
    assert_eq!(errors(&t), ["EQuery:Unknown asset pair"]);
    // the canonical `BTC/USD` is not a Kraken pair name; the ws name and rest name are
    assert!(!errors(&w.get(&format!("{BASE}/0/public/Ticker?pair=BTC%2FUSD"))).is_empty());
    assert!(errors(&w.get(&format!("{BASE}/0/public/Ticker?pair=XBT%2FUSD"))).is_empty());
    assert!(errors(&w.get(&format!("{BASE}/0/public/Ticker?pair=XXBTZUSD"))).is_empty());
    let both = w.get(&format!("{BASE}/0/public/Ticker?pair=XBTUSD,ETHUSD"));
    assert_eq!(both["result"].as_object().unwrap().len(), 2);
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), None, "public calls do not touch nonces");
}

#[test]
fn asset_pairs_agree_with_the_adapters_builtin_table() {
    // The fake's pair rows are written independently of the adapter's built-in ones. If they
    // ever drift apart, adapter-side rounding tests would silently stop being meaningful.
    let rig = KrakenRig::new();
    let from_fake = rig.pair_table_from_exchange();
    let builtin = broker_adapters::kraken::pairs::PairTable::builtin();
    for name in ["BTC/USD", "ETH/USD"] {
        let (a, b) = (from_fake.lookup(name).unwrap(), builtin.lookup(name).unwrap());
        assert_eq!(a.altname, b.altname, "{name}");
        assert_eq!(a.rest_name, b.rest_name, "{name}");
        assert_eq!(a.ws_name, b.ws_name, "{name}");
        assert_eq!(a.pair_decimals, b.pair_decimals, "{name}");
        assert_eq!(a.lot_decimals, b.lot_decimals, "{name}");
        assert_eq!(a.order_min, b.order_min, "{name}");
        assert_eq!(a.cost_min, b.cost_min, "{name}");
        assert_eq!(a.tick_size, b.tick_size, "{name}");
    }
    assert_eq!(from_fake.lookup("BTC/USD").unwrap().canonical, "BTC/USD", "XXBT/ZUSD normalises to BTC/USD");
}

#[test]
fn paths_used_by_the_fake_match_the_adapters_constants() {
    use broker_adapters::kraken::paths as ap;
    use fake_broker::kraken::wire::paths as fp;
    assert_eq!(
        [ap::ADD_ORDER, ap::CANCEL_ORDER, ap::QUERY_ORDERS, ap::OPEN_ORDERS, ap::CLOSED_ORDERS, ap::BALANCE, ap::TRADE_BALANCE, ap::TICKER],
        [fp::ADD_ORDER, fp::CANCEL_ORDER, fp::QUERY_ORDERS, fp::OPEN_ORDERS, fp::CLOSED_ORDERS, fp::BALANCE, fp::TRADE_BALANCE, fp::TICKER]
    );
}

#[test]
fn the_event_log_records_requests_responses_and_control_actions() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.note("start");
    w.ok(BALANCE, &[]);
    rig.handle.set_price("BTC/USD", "61000");
    let events = rig.handle.events();
    assert_eq!(events.len(), 3);
    let reqs = rig.handle.requests();
    assert_eq!(reqs.len(), 1);
    let r = &reqs[0];
    assert_eq!(r.path, BALANCE);
    assert_eq!(r.api_key.as_deref(), Some(DEFAULT_API_KEY));
    assert_eq!(r.param("nonce"), Some("1001"));
    assert!(r.reached_exchange && r.fault.is_none());
    assert!(r.produced_errors().is_empty());
    let text = rig.handle.dump_log();
    assert!(text.contains("CONTROL start") && text.contains("set_price BTC/USD 61000") && text.contains(BALANCE), "{text}");
    assert!(!text.contains(&default_secret_b64()), "no secret material in the log");
}
