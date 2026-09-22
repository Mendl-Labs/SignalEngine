//! Fault injection at the wire: what the caller sees versus what the exchange did. The ordering
//! (before or after the request is applied) is the whole point, so every fault kind is checked
//! for both.

mod common;

use broker_adapters::transport::TransportError;
use common::*;
use fake_broker::testkit::{d, KrakenRig};
use fake_broker::{Delivered, Fault, FaultKind, Timing, DEFAULT_ACCOUNT, DEFAULT_API_KEY};

const BUY: [(&str, &str); 4] = [("pair", "XBTUSD"), ("type", "buy"), ("ordertype", "market"), ("volume", "0.01")];

fn send_buy(rig: &KrakenRig, nonce: u64) -> Result<broker_adapters::transport::HttpResponse, TransportError> {
    let req = broker_adapters::kraken::auth::build_private_request(&creds(), BASE, ADD, nonce, &pairs(&BUY));
    use broker_adapters::transport::HttpTransport;
    rig.transport.execute(&req)
}

#[test]
fn timeout_before_apply_leaves_no_trace_on_the_exchange() {
    let rig = KrakenRig::new();
    rig.handle.inject_fault(Fault::timeout().on_path(ADD));
    assert!(matches!(send_buy(&rig, 10), Err(TransportError::Timeout)));
    assert!(rig.handle.orders(DEFAULT_ACCOUNT).is_empty(), "the order was never applied");
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), None, "the nonce was never consumed");
    assert_eq!(rig.handle.balance("main", "BTC"), broker_adapters::Dec::ZERO);
    let r = &rig.handle.requests()[0];
    assert!(!r.reached_exchange && r.produced.is_none());
    assert_eq!(r.fault, Some((FaultKind::Timeout, Timing::BeforeApply)));
    // the same nonce is still usable afterwards
    assert!(send_buy(&rig, 10).is_ok());
    assert_eq!(rig.handle.orders(DEFAULT_ACCOUNT).len(), 1);
}

#[test]
fn timeout_after_apply_places_the_order_and_loses_the_response() {
    let rig = KrakenRig::new();
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(ADD));
    assert!(matches!(send_buy(&rig, 10), Err(TransportError::Timeout)));
    let orders = rig.handle.orders(DEFAULT_ACCOUNT);
    assert_eq!(orders.len(), 1, "the exchange DID place the order");
    assert_eq!(rig.handle.balance("main", "BTC"), d("0.01"));
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), Some(10), "and consumed the nonce");
    let r = &rig.handle.requests()[0];
    assert!(r.reached_exchange);
    assert!(matches!(r.delivered, Delivered::Transport(TransportError::Timeout)));
    let (status, body) = r.produced.clone().unwrap();
    assert_eq!(status, 200);
    assert!(body.contains(&orders[0].txid), "the lost response carried the txid: {body}");
    // replaying the SAME signed request cannot double-place: the nonce is spent
    let replay = send_buy(&rig, 10).unwrap();
    assert!(replay.body.contains("EAPI:Invalid nonce"));
    assert_eq!(rig.handle.orders(DEFAULT_ACCOUNT).len(), 1);
}

#[test]
fn every_fault_kind_has_a_distinct_before_and_after_effect() {
    struct Case {
        name: &'static str,
        fault: fn() -> Fault,
        can_be_after: bool,
    }
    let cases = [
        Case { name: "timeout", fault: Fault::timeout, can_be_after: true },
        Case { name: "io", fault: Fault::io_error, can_be_after: true },
        Case { name: "http 502", fault: || Fault::http(502), can_be_after: true },
        Case { name: "http 503 with kraken body", fault: || Fault::http_with_body(503, r#"{"error":["EService:Unavailable"]}"#), can_be_after: true },
        Case { name: "malformed", fault: Fault::malformed_body, can_be_after: true },
        Case { name: "exchange error", fault: || Fault::exchange_error("EService:Unavailable"), can_be_after: true },
        Case { name: "rate limit", fault: Fault::rate_limit, can_be_after: true },
        Case { name: "connect failed", fault: Fault::connect_failed, can_be_after: false },
    ];
    for c in cases {
        // BEFORE: nothing happens at the exchange
        let rig = KrakenRig::new();
        rig.handle.inject_fault((c.fault)().on_path(ADD));
        let seen_before = send_buy(&rig, 10);
        assert!(rig.handle.orders(DEFAULT_ACCOUNT).is_empty(), "{}: BeforeApply must not place", c.name);
        assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), None, "{}: BeforeApply must not consume the nonce", c.name);
        assert!(!rig.handle.requests()[0].reached_exchange, "{}", c.name);

        if c.can_be_after {
            // AFTER: the order exists, the caller sees the same kind of failure
            let rig = KrakenRig::new();
            rig.handle.inject_fault((c.fault)().after_apply().on_path(ADD));
            let seen_after = send_buy(&rig, 10);
            assert_eq!(rig.handle.orders(DEFAULT_ACCOUNT).len(), 1, "{}: AfterApply must place", c.name);
            assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), Some(10), "{}: AfterApply consumes the nonce", c.name);
            assert!(rig.handle.requests()[0].reached_exchange, "{}", c.name);
            assert_eq!(format!("{seen_before:?}"), format!("{seen_after:?}"), "{}: the caller cannot tell before from after", c.name);
        }
    }
}

#[test]
fn what_each_fault_looks_like_to_the_caller() {
    let rig = KrakenRig::new();
    let h = &rig.handle;
    h.inject_fault(Fault::connect_failed());
    assert!(matches!(send_buy(&rig, 1), Err(TransportError::ConnectFailed(_))));
    h.inject_fault(Fault::io_error());
    assert!(matches!(send_buy(&rig, 1), Err(TransportError::Io(_))));
    h.inject_fault(Fault::http(502));
    let r = send_buy(&rig, 1).unwrap();
    assert_eq!(r.status, 502);
    assert!(r.body.contains("502"));
    h.inject_fault(Fault::malformed_body());
    let r = send_buy(&rig, 1).unwrap();
    assert_eq!(r.status, 200);
    assert!(serde_json::from_str::<serde_json::Value>(&r.body).is_err(), "not valid JSON: {}", r.body);
    h.inject_fault(Fault::rate_limit());
    assert_eq!(send_buy(&rig, 1).unwrap().body, r#"{"error":["EAPI:Rate limit exceeded"]}"#);
    h.inject_fault(Fault::exchange_error("EService:Busy"));
    assert_eq!(send_buy(&rig, 1).unwrap().body, r#"{"error":["EService:Busy"]}"#);
    assert!(h.pending_faults().is_empty(), "each fault fired once");
    assert!(rig.handle.orders(DEFAULT_ACCOUNT).is_empty());
}

#[test]
fn faults_fire_for_the_next_n_matching_requests_then_the_exchange_recovers() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.inject_fault(Fault::exchange_error("EService:Unavailable").times(3));
    for _ in 0..3 {
        assert_eq!(w.err(BALANCE, &[]), ["EService:Unavailable"]);
    }
    assert!(w.ok(BALANCE, &[])["ZUSD"].is_string(), "recovered");
    assert!(rig.handle.pending_faults().is_empty());
    // failed requests were not applied, so their nonces were not consumed (the Wire counter kept counting)
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), Some(1004));
}

#[test]
fn skip_matcher_and_forever_and_clear() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.inject_fault(Fault::rate_limit().on_path(ADD).after_requests(1).times(2));
    w.ok(BALANCE, &[]); // other path: untouched
    place_ok(&w); // first AddOrder passes (skipped)
    assert_eq!(w.err(ADD, &BUY), ["EAPI:Rate limit exceeded"]);
    assert_eq!(w.err(ADD, &BUY), ["EAPI:Rate limit exceeded"]);
    place_ok(&w);
    rig.handle.inject_fault(Fault::exchange_error("EService:Unavailable").forever());
    for _ in 0..20 {
        assert_eq!(w.err(BALANCE, &[]), ["EService:Unavailable"]);
    }
    rig.handle.clear_faults();
    w.ok(BALANCE, &[]);
    assert_eq!(rig.handle.orders(DEFAULT_ACCOUNT).len(), 2);
}

fn place_ok(w: &Wire) {
    w.ok(ADD, &BUY);
}

#[test]
fn faults_apply_to_public_endpoints_and_never_to_the_other_process() {
    let rig = KrakenRig::new();
    let w = Wire::new(&rig);
    rig.handle.inject_fault(Fault::timeout()); // any path, once
    let req = broker_adapters::transport::HttpRequest {
        method: broker_adapters::transport::HttpMethod::Get,
        url: format!("{BASE}/0/public/Ticker?pair=XBTUSD"),
        headers: Vec::new(),
        body: None,
    };
    use broker_adapters::transport::HttpTransport;
    assert_eq!(rig.transport.execute(&req).unwrap_err(), TransportError::Timeout);

    rig.handle.inject_fault(Fault::timeout());
    let r = rig.handle.other_process_call(DEFAULT_API_KEY, BALANCE, &[], 5);
    assert_eq!(r.status, 200, "the other process is not subject to the adapter's faults");
    assert_eq!(rig.handle.pending_faults().len(), 1, "and did not consume the fault");
    assert!(rig.transport.execute(&req).is_err(), "the next adapter request does");
    let _ = w;
}

#[test]
fn faults_are_recorded_in_the_event_log() {
    let rig = KrakenRig::new();
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(ADD));
    let _ = send_buy(&rig, 10);
    let text = rig.handle.dump_log();
    assert!(text.contains("inject_fault"), "{text}");
    assert!(text.contains("FAULT Timeout/AfterApply"), "{text}");
    assert!(text.contains("exchange had produced"), "the lost response is visible to the test: {text}");
}

#[test]
fn a_delayed_request_lands_later_and_is_authenticated_then() {
    let rig = KrakenRig::new();
    rig.handle.inject_fault(Fault::timeout().delayed().on_path(ADD));
    assert!(matches!(send_buy(&rig, 10), Err(TransportError::Timeout)));
    assert!(rig.handle.orders(DEFAULT_ACCOUNT).is_empty(), "not applied while in flight");
    assert_eq!(rig.handle.delayed_count(), 1);
    assert_eq!(rig.handle.highest_nonce(DEFAULT_API_KEY), None);

    let late = rig.handle.deliver_delayed();
    assert_eq!(late.len(), 1);
    assert!(late[0].body.contains("txid"), "{}", late[0].body);
    assert_eq!(rig.handle.orders(DEFAULT_ACCOUNT).len(), 1);
    assert_eq!(rig.handle.delayed_count(), 0);
    assert!(rig.handle.deliver_delayed().is_empty(), "delivered once");

    // A newer request that got there first makes the late one lose.
    rig.handle.inject_fault(Fault::timeout().delayed().on_path(ADD));
    assert!(send_buy(&rig, 20).is_err());
    let w = Wire::new(&rig);
    w.ok_nonce_at(30);
    let late = rig.handle.deliver_delayed();
    assert!(late[0].body.contains("EAPI:Invalid nonce"), "{}", late[0].body);
    assert_eq!(rig.handle.orders(DEFAULT_ACCOUNT).len(), 1, "the second delayed order never landed");
    let r = rig.handle.requests().into_iter().find(|r| r.origin == fake_broker::Origin::Delayed && r.produced_errors().len() == 1);
    assert!(r.is_some(), "the refusal is in the log with origin Delayed");
}

trait NonceAt {
    fn ok_nonce_at(&self, n: u64);
}
impl NonceAt for Wire<'_> {
    fn ok_nonce_at(&self, n: u64) {
        assert!(errors(&self.post_with_nonce(BALANCE, n, &[])).is_empty());
    }
}
