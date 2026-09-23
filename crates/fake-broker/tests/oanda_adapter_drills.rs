//! Rung 2: the REAL `OandaAdapter` against the fake OANDA exchange, over the adapter's own `HttpTransport` trait.
//! Place / look up / cancel / close, the transaction-stream idempotency protocol (lost response, lost request, restart with
//! and without a checkpoint, a reused client id, a delayed request, a truncating server), maximumPositionSize, margin and
//! market-hours refusals, partial fills, netting through zero, and consistency of what the adapter reads with the exchange's
//! own books.
//!
//! What a green run means: the adapter agrees with THIS model of OANDA. On the points measured on a real practice account
//! (2026-09-23: pending-only lookup by client id, non-unique client ids, the transaction stream, one-sided closes, reject
//! shapes) the fake reproduces what OANDA did; elsewhere it is a model from documentation. It says nothing about the live host.

use broker_adapters::oanda::{CloseOutcome, InstrumentTable};
use broker_adapters::transport::{HttpMethod, TransportError};
use broker_adapters::{BrokerAdapter, BrokerError, Dec, ErrorClass, OrderRequest, OrderStatus, PlaceOutcome, Side, TimeInForce};
use fake_broker::oanda::OrderScript;
use fake_broker::oanda_rig::OandaRig;
use fake_broker::Fault;

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

fn buy(tag: &str, units: &str) -> OrderRequest {
    OrderRequest::market(tag, "EUR/USD", Side::Buy, d(units))
}

fn sell(tag: &str, units: &str) -> OrderRequest {
    OrderRequest::market(tag, "EUR/USD", Side::Sell, d(units))
}

fn accepted_id(o: PlaceOutcome) -> String {
    match o {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("expected Accepted, got {other:?}"),
    }
}

// ---------------------------------------------------------------- basics

#[test]
fn the_instrument_table_comes_over_the_wire_from_the_exchange() {
    let rig = OandaRig::new();
    let t: InstrumentTable = rig.adapter.instruments();
    assert_eq!(t.len(), 4);
    assert!(t.lookup("EUR/USD").is_some() && t.lookup("USDJPY").is_some());
    assert_eq!(rig.adapter.instrument("USD_JPY").unwrap().display_precision, 3);
    assert_eq!(rig.adapter.verify_account().unwrap().id, rig.account_id);
}

#[test]
fn place_lookup_and_read_back_a_market_buy() {
    let rig = OandaRig::new();
    let out = rig.adapter.place_order(&buy("t:buy1", "1500")).unwrap();
    let id = accepted_id(out);
    let r = rig.adapter.get_order(&id).unwrap();
    assert_eq!((r.status, r.side, r.quantity, r.executed_quantity), (OrderStatus::Filled, Some(Side::Buy), d("1500"), d("1500")));
    assert_eq!(r.avg_price, Some(d("1.10052")), "a buy fills at the ask");
    assert_eq!(r.tag.as_deref(), Some("t:buy1"));
    let by_tag = rig.adapter.find_orders_by_tag("t:buy1").unwrap();
    assert_eq!(by_tag.len(), 1);
    assert_eq!(by_tag[0].broker_order_id, id);
    assert!(rig.adapter.find_orders_by_tag("t:none").unwrap().is_empty());
    assert_eq!(rig.handle.position_units("EUR_USD"), d("1500"));
    rig.handle.assert_invariants();
}

#[test]
fn what_the_adapter_reports_equals_the_exchange_books_including_shorts() {
    let rig = OandaRig::new();
    rig.adapter.place_order(&buy("t:a", "10000")).unwrap();
    rig.adapter.place_order(&OrderRequest::market("t:b", "GBP_USD", Side::Sell, d("5000"))).unwrap();
    rig.adapter.place_order(&OrderRequest::market("t:c", "USD/JPY", Side::Buy, d("2000"))).unwrap();
    let b = rig.adapter.get_balances().unwrap();
    assert_eq!(b.spot("USD"), rig.handle.balance());
    assert_eq!(b.spot("EUR/USD"), d("10000"));
    assert_eq!(b.spot("GBP/USD"), d("-5000"), "short");
    assert_eq!(b.spot("USD/JPY"), d("2000"));
    let pos = rig.adapter.get_open_positions().unwrap();
    assert_eq!(pos.len(), 3);
    for p in &pos {
        assert_eq!(p.net_units(), rig.handle.position_units(&p.instrument), "{}", p.instrument);
    }
    let s = rig.adapter.get_account_summary().unwrap();
    assert_eq!(s.nav, rig.handle.nav().round_dp(4, broker_adapters::decimal::Rounding::HalfUp).unwrap());
    assert_eq!(s.margin_used, rig.handle.margin_used().round_dp(4, broker_adapters::decimal::Rounding::HalfUp).unwrap());
    assert_eq!(s.balance, rig.handle.balance());
    rig.handle.assert_invariants();
}

#[test]
fn a_round_trip_books_the_spread_and_pl_and_the_reads_agree() {
    let rig = OandaRig::new();
    rig.adapter.place_order(&buy("t:in", "1000")).unwrap();
    rig.handle.set_price("EUR_USD", "1.11052", "1.11056");
    let out = rig.adapter.place_order(&sell("t:out", "1000")).unwrap();
    let r = rig.adapter.get_order(&accepted_id(out)).unwrap();
    assert_eq!((r.avg_price, r.side), (Some(d("1.11052")), Some(Side::Sell)));
    assert_eq!(rig.handle.position_units("EUR_USD"), Dec::ZERO);
    assert_eq!(rig.handle.balance(), d("100010.0000"), "(1.11052 - 1.10052) * 1000");
    assert!(rig.adapter.get_open_positions().unwrap().is_empty());
    rig.handle.assert_invariants();
}

#[test]
fn quote_is_the_top_of_book_with_the_mid_as_last() {
    let rig = OandaRig::new();
    let q = rig.adapter.get_quote("EUR/USD").unwrap();
    assert_eq!((q.bid, q.ask, q.last), (d("1.10048"), d("1.10052"), d("1.10050")));
    rig.handle.set_market_open(false);
    assert!(matches!(rig.adapter.get_quote("EUR/USD"), Err(BrokerError::PairNotTradable { .. })));
}

#[test]
fn a_market_order_stays_a_market_order_at_the_exchange_even_with_a_reference_price() {
    let rig = OandaRig::new();
    let mut req = buy("t:m", "100");
    req.reference_price = Some(d("1.10000"));
    rig.adapter.place_order(&req).unwrap();
    let o = &rig.handle.orders()[0];
    assert!(o.limit_price.is_none(), "the exchange saw a MARKET order");
    assert_eq!(o.time_in_force, "FOK");
    assert_eq!(o.position_fill, "DEFAULT");
    assert_eq!(o.client_id.as_deref(), Some("t:m"));
    assert_eq!(o.client_tag.as_deref(), Some("mendl-rb"));
}

// ---------------------------------------------------------------- refusals at the exchange

#[test]
fn insufficient_margin_is_a_definite_rejection_and_nothing_executes() {
    let rig = OandaRig::new();
    let out = rig.adapter.place_order(&buy("t:big", "6000000")).unwrap();
    match out {
        PlaceOutcome::Rejected { errors, .. } => {
            assert_eq!(errors[0].class, ErrorClass::InsufficientFunds);
            assert!(errors[0].code.contains("INSUFFICIENT_MARGIN"), "{}", errors[0].code);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.position_units("EUR_USD"), Dec::ZERO);
    // the order exists at the exchange as CANCELLED; its tag is now burnt: a re-place is refused, not resent
    let again = rig.adapter.place_order(&buy("t:big", "6000000")).unwrap();
    assert!(matches!(again, PlaceOutcome::Rejected { ref errors, .. } if errors[0].code.contains("use a new tag")), "{again:?}");
    assert_eq!(rig.handle.applied(HttpMethod::Post, "/orders").len(), 1);
    rig.handle.assert_invariants();
}

#[test]
fn a_closed_market_rejects_market_orders() {
    let rig = OandaRig::new();
    rig.handle.set_market_open(false);
    match rig.adapter.place_order(&buy("t:h", "100")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert!(errors[0].code.contains("MARKET_HALTED"), "{}", errors[0].code),
        other => panic!("{other:?}"),
    }
}

#[test]
fn scripted_rejects_and_cancels_are_parsed() {
    let rig = OandaRig::new();
    rig.handle.script_next_order(OrderScript::Reject("INSTRUMENT_NOT_TRADEABLE".into()));
    match rig.adapter.place_order(&buy("t:r", "100")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert!(errors[0].code.starts_with("oanda:400") && errors[0].code.contains("INSTRUMENT_NOT_TRADEABLE")),
        other => panic!("{other:?}"),
    }
    assert!(rig.handle.orders().is_empty(), "a reject creates no order, so the tag is still free");
    // ... and the same tag can be used again
    assert!(matches!(rig.adapter.place_order(&buy("t:r", "100")).unwrap(), PlaceOutcome::Accepted { .. }));
}

#[test]
fn a_hedging_account_is_refused_before_anything_is_sent() {
    let rig = OandaRig::new();
    rig.handle.set_hedging(true);
    assert!(matches!(rig.adapter.place_order(&buy("t:x", "100")), Err(BrokerError::AccountBlocked(_))));
    assert_eq!(rig.handle.order_affecting_requests(), 0);
}

#[test]
fn a_wrong_token_or_account_is_refused_by_the_exchange() {
    use broker_adapters::oanda::{Environment, OandaAdapter, OandaConfig, OandaCredentials, PRACTICE_BASE_URL};
    let rig = OandaRig::new();
    let mk = |token: &str, acct: &str| {
        OandaAdapter::new(
            OandaConfig::practice(PRACTICE_BASE_URL).unwrap(),
            OandaCredentials::new(Environment::Practice, token, acct).unwrap(),
            rig.transport.clone(),
        )
        .unwrap()
    };
    let bad_token = mk("some-other-token", &rig.account_id);
    assert!(matches!(bad_token.get_account_summary(), Err(BrokerError::Exchange(e)) if e[0].class == ErrorClass::Auth));
    let bad_acct = mk(&rig.token, "101-001-9999999-001");
    assert!(matches!(bad_acct.get_account_summary(), Err(BrokerError::NotFound(_))));
}

// ---------------------------------------------------------------- unknown outcomes, restarts, idempotency
//
// The protocol under test (module docs of `broker_adapters::oanda`): a checkpoint (`lastTransactionID`) before the first send,
// a scan of `transactions/sinceid` for the tag after any ambiguous outcome, and a re-send only after a complete scan from the
// FIRST checkpoint found nothing. The fake is HOSTILE about ids: it accepts a reused client id and produces a second fill, and
// `GET /orders/@tag` finds only pending orders, exactly as measured on a real practice account.

fn posts_applied(rig: &OandaRig) -> usize {
    rig.handle.applied(HttpMethod::Post, "/orders").len()
}

/// Index (in the request log) of the last `sinceid` scan and of the last POST that reached the exchange.
fn last_scan_and_post(rig: &OandaRig) -> (Option<usize>, Option<usize>) {
    let reqs = rig.handle.requests();
    let scan = reqs.iter().rposition(|r| r.method == HttpMethod::Get && r.path.ends_with("/transactions/sinceid"));
    let post = reqs.iter().rposition(|r| r.reached_exchange && r.method == HttpMethod::Post && r.path.ends_with("/orders"));
    (scan, post)
}

#[test]
fn response_lost_after_the_order_was_applied_the_scan_finds_it_and_it_is_not_placed_again() {
    let rig = OandaRig::new();
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(&rig.handle.path("/orders")));
    // the answer is lost, but place_order itself scans the stream from its checkpoint and finds the fill
    let out = rig.adapter.place_order(&buy("t:lost", "700")).unwrap();
    let id = accepted_id(out);
    assert_eq!(rig.handle.orders_with_client_id("t:lost").len(), 1, "the order really exists");
    let by_tag = rig.adapter.find_orders_by_tag("t:lost").unwrap();
    assert_eq!(by_tag.len(), 1);
    assert_eq!((by_tag[0].broker_order_id.as_str(), by_tag[0].status, by_tag[0].executed_quantity), (id.as_str(), OrderStatus::Filled, d("700")));
    // MEASURED: the fill is invisible to GET /orders/@tag (only pending orders are found by client id)
    let (s, _) = raw_get(&rig, &format!("/orders/%40{}", "t%3Alost"));
    assert_eq!(s, 404);
    // even a careless second place() with the same tag sends nothing
    let second = rig.adapter.place_order(&buy("t:lost", "700")).unwrap();
    assert!(matches!(&second, PlaceOutcome::Accepted { broker_order_id, warnings, .. } if *broker_order_id == id && warnings[0].contains("already existed")), "{second:?}");
    assert_eq!(posts_applied(&rig), 1, "exactly one POST ever reached the exchange");
    assert_eq!(rig.handle.position_units("EUR_USD"), d("700"), "no double position");
    rig.handle.assert_invariants();
}

/// A plain GET against the fake (bypassing the adapter): status and parsed body.
fn raw_get(rig: &OandaRig, tail: &str) -> (u16, serde_json::Value) {
    use broker_adapters::transport::{HttpRequest, HttpTransport};
    let req = HttpRequest {
        method: HttpMethod::Get,
        url: format!("https://api-fxpractice.oanda.com/v3/accounts/{}{tail}", rig.account_id),
        headers: vec![("Authorization".into(), format!("Bearer {}", rig.token))],
        body: None,
    };
    let r = rig.transport.execute(&req).unwrap();
    (r.status, serde_json::from_str(&r.body).unwrap_or(serde_json::Value::Null))
}

#[test]
fn request_lost_before_it_arrived_is_scanned_for_found_absent_and_only_then_replaced_exactly_once() {
    let rig = OandaRig::new();
    rig.handle.inject_fault(Fault::io_error().on_path(&rig.handle.path("/orders")));
    let out = rig.adapter.place_order(&buy("t:gone", "300")).unwrap();
    let PlaceOutcome::UnknownOutcome { reason, .. } = &out else { panic!("{out:?}") };
    let checkpoint = rig.adapter.tag_checkpoint("t:gone").expect("the first attempt's checkpoint is registered");
    assert!(reason.ends_with(&format!("[oanda-tag-checkpoint={checkpoint}]")), "{reason}");
    assert!(rig.handle.orders().is_empty(), "the request never arrived");
    // the lookup covers everything since the checkpoint and finds nothing: PROVABLY not placed
    assert!(rig.adapter.find_orders_by_tag("t:gone").unwrap().is_empty());
    // only now is it sent again, once
    let id = accepted_id(rig.adapter.place_order(&buy("t:gone", "300")).unwrap());
    assert!(!id.is_empty());
    assert_eq!(rig.handle.orders_with_client_id("t:gone").len(), 1);
    assert_eq!(posts_applied(&rig), 1);
    assert_eq!(rig.handle.position_units("EUR_USD"), d("300"));
    let (scan, post) = last_scan_and_post(&rig);
    assert!(scan.unwrap() < post.unwrap(), "the scan since the checkpoint preceded the re-send");
    assert_eq!(rig.adapter.tag_checkpoint("t:gone"), Some(checkpoint), "the checkpoint of the FIRST attempt never moves");
    rig.handle.assert_invariants();
}

#[test]
fn an_error_answer_after_the_order_was_placed_is_resolved_from_the_stream_never_resent() {
    for fault in [Fault::http(502), Fault::http(500), Fault::malformed_body(), Fault::exchange_error("boom")] {
        let rig = OandaRig::new();
        rig.handle.inject_fault(fault.after_apply().on_path(&rig.handle.path("/orders")));
        let out = rig.adapter.place_order(&buy("t:e", "200")).unwrap();
        assert!(matches!(out, PlaceOutcome::Accepted { .. }), "{out:?}");
        assert_eq!(rig.adapter.find_orders_by_tag("t:e").unwrap().len(), 1);
        assert_eq!(posts_applied(&rig), 1);
    }
}

#[test]
fn an_order_cancelled_at_creation_whose_answer_was_lost_is_resolved_as_rejected_from_the_stream() {
    let rig = OandaRig::new();
    rig.handle.set_market_open(false);
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(&rig.handle.path("/orders")));
    match rig.adapter.place_order(&buy("t:halt", "100")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert!(errors[0].code.contains("MARKET_HALTED") && errors[0].code.starts_with("oanda:scan"), "{}", errors[0].code),
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.position_units("EUR_USD"), Dec::ZERO);
    // the tag now belongs to a dead order: a re-place says so instead of sending
    rig.handle.set_market_open(true);
    let again = rig.adapter.place_order(&buy("t:halt", "100")).unwrap();
    assert!(matches!(again, PlaceOutcome::Rejected { ref errors, .. } if errors[0].code.contains("use a new tag")), "{again:?}");
    assert_eq!(posts_applied(&rig), 1);
}

#[test]
fn restart_mid_run_with_no_checkpoint_finds_the_tags_in_the_recent_window_and_does_not_double_submit() {
    let mut rig = OandaRig::new();
    // run 1 places two orders, then "dies" after the second was applied but before its answer arrived
    rig.adapter.place_order(&buy("rb:run7:EUR/USD:buy", "1000")).unwrap();
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(&rig.handle.path("/orders")));
    let _ = rig.adapter.place_order(&OrderRequest::market("rb:run7:GBP/USD:sell", "GBP/USD", Side::Sell, d("500"))).unwrap();
    assert_eq!(posts_applied(&rig), 2);
    // process restart: a brand-new adapter, nothing persisted, so NO checkpoints
    rig.restart_adapter();
    assert_eq!(rig.adapter.tag_checkpoint("rb:run7:EUR/USD:buy"), None);
    // run 2 replays the same plan with the same tags
    let a = rig.adapter.place_order(&buy("rb:run7:EUR/USD:buy", "1000")).unwrap();
    let b = rig.adapter.place_order(&OrderRequest::market("rb:run7:GBP/USD:sell", "GBP/USD", Side::Sell, d("500"))).unwrap();
    assert!(matches!(a, PlaceOutcome::Accepted { ref warnings, .. } if warnings[0].contains("already existed")), "{a:?}");
    assert!(matches!(b, PlaceOutcome::Accepted { ref warnings, .. } if warnings[0].contains("already existed")), "{b:?}");
    assert_eq!(posts_applied(&rig), 2, "the replay sent nothing");
    assert_eq!(rig.handle.position_units("EUR_USD"), d("1000"));
    assert_eq!(rig.handle.position_units("GBP_USD"), d("-500"));
    // and the lookup finds both without any state
    assert_eq!(rig.adapter.find_orders_by_tag("rb:run7:EUR/USD:buy").unwrap().len(), 1);
    rig.handle.assert_invariants();
}

#[test]
fn restart_when_the_first_attempt_is_older_than_the_window_is_the_documented_limit_and_two_things_close_it() {
    // KNOWN LIMIT (module docs, "Restart with no checkpoint"): a restarted process holds no checkpoint, scans only the last
    // `restart_scan_window` transaction ids, and a tag older than that is invisible. Here: 1000 unrelated transactions
    // (default window 400) push the first attempt out of the window.
    let mut rig = OandaRig::new();
    rig.adapter.place_order(&buy("t:old", "100")).unwrap();
    let checkpoint = rig.adapter.tag_checkpoint("t:old").unwrap();
    rig.handle.add_external_transactions(1000);
    rig.restart_adapter();
    assert!(rig.adapter.find_orders_by_tag("t:old").unwrap().is_empty(), "NOT found: outside the window (this is the limit, asserted so nobody mistakes it for a guarantee)");

    // (1) a persisted checkpoint restores exact coverage: the tag is found and nothing is sent twice
    rig.adapter.seed_tag_checkpoint("t:old", checkpoint);
    let found = rig.adapter.find_orders_by_tag("t:old").unwrap();
    assert_eq!(found.len(), 1);
    assert!(matches!(rig.adapter.place_order(&buy("t:old", "100")).unwrap(), PlaceOutcome::Accepted { ref warnings, .. } if warnings[0].contains("already existed")));
    assert_eq!(posts_applied(&rig), 1);
    assert_eq!(rig.handle.position_units("EUR_USD"), d("100"));

    // (2) strict mode turns the invisible case into an alert instead of a send
    let short_history = fake_broker::oanda::FakeOandaBuilder::standard().first_transaction_id(50).build();
    let mut strict = OandaRig::with_fake(short_history, |c| c.with_strict_unseen_tags(true));
    strict.adapter.place_order(&buy("t:old", "100")).unwrap();
    strict.handle.add_external_transactions(1000);
    strict.restart_adapter();
    assert!(matches!(strict.adapter.find_orders_by_tag("t:old"), Err(BrokerError::LookupInconclusive(_))));
    let out = strict.adapter.place_order(&buy("t:old", "100")).unwrap();
    assert!(matches!(&out, PlaceOutcome::UnknownOutcome { reason, .. } if reason.contains("strict mode")), "{out:?}");
    assert_eq!(posts_applied(&strict), 1, "strict mode did not send again");
}

#[test]
fn a_reused_client_id_is_accepted_by_the_hostile_fake_yet_the_adapter_never_double_submits() {
    let rig = OandaRig::new();
    // the exchange itself would happily fill the same id again (MEASURED at real OANDA) ...
    assert!(rig.handle.orders_with_client_id("t:dup").is_empty());
    let first = accepted_id(rig.adapter.place_order(&buy("t:dup", "100")).unwrap());
    // ... but the adapter looks in the stream first, so every further attempt adopts the first order
    for _ in 0..3 {
        let again = rig.adapter.place_order(&buy("t:dup", "100")).unwrap();
        assert!(matches!(&again, PlaceOutcome::Accepted { broker_order_id, .. } if *broker_order_id == first), "{again:?}");
    }
    assert_eq!(rig.handle.orders_with_client_id("t:dup").len(), 1);
    assert_eq!(posts_applied(&rig), 1);
    assert_eq!(rig.handle.position_units("EUR_USD"), d("100"));
    // a different QUANTITY under the same tag is refused, not sent as a second order
    let clash = rig.adapter.place_order(&buy("t:dup", "250")).unwrap();
    assert!(matches!(clash, PlaceOutcome::Rejected { ref errors, .. } if errors[0].code.contains("differs")), "{clash:?}");
    assert_eq!(posts_applied(&rig), 1);
    // and once the exchange HAS been made to hold two orders under one tag (a double submit from elsewhere), the adapter alerts
    let raw = serde_json::json!({"order": {"type": "MARKET", "instrument": "EUR_USD", "units": "100", "timeInForce": "FOK", "positionFill": "DEFAULT",
        "clientExtensions": {"id": "t:dup"}}});
    let req = broker_adapters::transport::HttpRequest {
        method: HttpMethod::Post,
        url: format!("https://api-fxpractice.oanda.com/v3/accounts/{}/orders", rig.account_id),
        headers: vec![("Authorization".into(), format!("Bearer {}", rig.token))],
        body: Some(raw.to_string()),
    };
    use broker_adapters::transport::HttpTransport;
    assert_eq!(rig.transport.execute(&req).unwrap().status, 201, "the fake, like OANDA, accepts the reused id");
    let out = rig.adapter.place_order(&buy("t:dup", "100")).unwrap();
    assert!(matches!(&out, PlaceOutcome::UnknownOutcome { reason, .. } if reason.contains("DUPLICATE_TAG")), "{out:?}");
    assert_eq!(rig.adapter.find_orders_by_tag("t:dup").unwrap().len(), 2, "the lookup reports both");
    assert_eq!(rig.handle.position_units("EUR_USD"), d("200"));
}

#[test]
fn a_delayed_request_that_lands_after_the_scan_is_the_known_limit_and_is_at_least_detected() {
    // KNOWN LIMIT (module docs, "Residual risks"): a request delayed in the network can be processed AFTER the scan that found
    // nothing. Nothing the client does can close that window; the drill shows what happens and that the double is DETECTED.
    let rig = OandaRig::new();
    rig.handle.inject_fault(Fault::timeout().delayed().on_path(&rig.handle.path("/orders")));
    let out = rig.adapter.place_order(&buy("t:late", "400")).unwrap();
    assert!(matches!(out, PlaceOutcome::UnknownOutcome { .. }), "{out:?}");
    assert!(rig.handle.orders().is_empty(), "the first request is still in flight");
    // the scan since the checkpoint finds nothing, so the caller retries and the second request is placed
    assert!(rig.adapter.find_orders_by_tag("t:late").unwrap().is_empty());
    let second = accepted_id(rig.adapter.place_order(&buy("t:late", "400")).unwrap());
    assert_eq!(rig.handle.position_units("EUR_USD"), d("400"));
    // now the delayed first request lands: the account is double
    let late = rig.handle.deliver_delayed();
    assert_eq!(late.len(), 1);
    assert_eq!(rig.handle.orders_with_client_id("t:late").len(), 2, "the residual: two orders under one tag");
    assert_eq!(rig.handle.position_units("EUR_USD"), d("800"));
    // detection: the lookup returns both, and a further place() alerts instead of adopting one of them
    let found = rig.adapter.find_orders_by_tag("t:late").unwrap();
    assert_eq!(found.len(), 2);
    assert!(found.iter().any(|r| r.broker_order_id == second));
    let alert = rig.adapter.place_order(&buy("t:late", "400")).unwrap();
    assert!(matches!(&alert, PlaceOutcome::UnknownOutcome { reason, .. } if reason.contains("DUPLICATE_TAG")), "{alert:?}");
}

#[test]
fn a_server_that_truncates_the_stream_never_lets_the_adapter_conclude_absence() {
    // no earlier attempt: nothing is sent
    let rig = OandaRig::new();
    rig.handle.set_sinceid_page_limit(Some(3));
    assert!(matches!(rig.adapter.place_order(&buy("t:trunc", "100")), Err(BrokerError::Preflight(m)) if m.contains("nothing was sent")));
    assert!(matches!(rig.adapter.find_orders_by_tag("t:trunc"), Err(BrokerError::LookupInconclusive(_))));
    assert_eq!(rig.handle.order_affecting_requests(), 0);
    // an earlier attempt exists and the follow-up scan is truncated: unknown, and the replay does not send
    let rig = OandaRig::new();
    rig.handle.inject_fault(Fault::io_error().on_path(&rig.handle.path("/orders")));
    assert!(matches!(rig.adapter.place_order(&buy("t:trunc2", "100")).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
    rig.handle.add_external_transactions(10);
    rig.handle.set_sinceid_page_limit(Some(3));
    let out = rig.adapter.place_order(&buy("t:trunc2", "100")).unwrap();
    assert!(matches!(&out, PlaceOutcome::UnknownOutcome { reason, .. } if reason.contains("earlier attempt") && reason.contains("incomplete")), "{out:?}");
    assert_eq!(posts_applied(&rig), 0);
    // the stream is whole again: NOW it is provably absent and the order is sent
    rig.handle.set_sinceid_page_limit(None);
    assert!(matches!(rig.adapter.place_order(&buy("t:trunc2", "100")).unwrap(), PlaceOutcome::Accepted { .. }));
    assert_eq!(posts_applied(&rig), 1);
}

#[test]
fn a_broker_that_will_not_serve_finished_orders_by_id_is_read_from_the_stream() {
    // UNMEASURED at real OANDA whether GET /orders/<numeric id> serves finished orders; both answers must work.
    let rig = OandaRig::new();
    rig.handle.set_historic_order_lookup(false);
    let id = accepted_id(rig.adapter.place_order(&buy("t:hist", "150")).unwrap());
    let r = rig.adapter.get_order(&id).unwrap();
    assert_eq!((r.status, r.executed_quantity, r.avg_price, r.tag.as_deref()), (OrderStatus::Filled, d("150"), Some(d("1.10052")), Some("t:hist")));
    // a cancelled limit order too, through cancel_and_settle
    let lim = accepted_id(rig.adapter.place_order(&OrderRequest::limit("t:hist-l", "EUR/USD", Side::Buy, d("100"), d("1.05000"))).unwrap());
    let (out, report) = rig.adapter.cancel_and_settle(&lim).unwrap();
    assert_eq!((out.canceled_count, report.status, report.reason.as_deref()), (1, OrderStatus::Canceled, Some("CLIENT_REQUEST")));
    assert!(matches!(rig.adapter.cancel_and_settle("99999"), Err(BrokerError::CancelTargetNotFound(_))));
}

#[test]
fn a_preflight_failure_sends_nothing_and_leaves_the_tag_free() {
    let rig = OandaRig::new();
    rig.handle.inject_fault(Fault::http(503).on_path(&rig.handle.path("/summary")));
    assert!(matches!(rig.adapter.place_order(&buy("t:p", "100")), Err(BrokerError::Preflight(_))));
    assert_eq!(rig.handle.order_affecting_requests(), 0);
    assert!(matches!(rig.adapter.place_order(&buy("t:p", "100")).unwrap(), PlaceOutcome::Accepted { .. }));
    assert_eq!(rig.handle.orders_with_client_id("t:p").len(), 1);
}

#[test]
fn rate_limiting_and_an_unreachable_exchange_mean_not_sent_and_leave_no_checkpoint() {
    let rig = OandaRig::new();
    rig.handle.inject_fault(Fault::rate_limit().on_path(&rig.handle.path("/orders")));
    assert!(matches!(rig.adapter.place_order(&buy("t:rl", "100")), Err(BrokerError::RateLimited { .. })));
    assert!(rig.handle.orders().is_empty());
    assert_eq!(rig.adapter.tag_checkpoint("t:rl"), None);
    rig.handle.inject_fault(Fault::connect_failed().on_path(&rig.handle.path("/orders")));
    assert!(matches!(rig.adapter.place_order(&buy("t:rl", "100")), Err(BrokerError::Transport(TransportError::ConnectFailed(_)))));
    assert!(rig.handle.orders().is_empty());
    assert_eq!(rig.adapter.tag_checkpoint("t:rl"), None);
}

#[test]
fn an_unknown_instrument_at_the_exchange_is_a_definite_rejection_that_creates_nothing() {
    // The adapter's own table says the instrument exists, the exchange disagrees (MEASURED shape: HTTP 400
    // InvalidParameterException, no reject transaction).
    let rig = OandaRig::new();
    let mut info = rig.adapter.instrument("EUR_USD").unwrap();
    info.name = "XXX_USD".into();
    rig.adapter.upsert_instrument(info);
    let before = rig.handle.last_transaction_id();
    match rig.adapter.place_order(&OrderRequest::market("t:x", "XXX/USD", Side::Buy, d("100"))).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => {
            assert_eq!(errors[0].class, ErrorClass::InvalidArguments);
            assert!(errors[0].code.contains("InvalidParameterException"), "{}", errors[0].code);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.last_transaction_id(), before, "nothing entered the stream");
    assert_eq!(rig.adapter.tag_checkpoint("t:x"), None);
}

#[test]
fn maximum_position_size_is_enforced_by_the_adapter_and_never_truncates() {
    use fake_broker::oanda::{FakeOandaBuilder, InstrumentSpec};
    let fake = FakeOandaBuilder::new()
        .balance("100000")
        .instrument(InstrumentSpec::fx("EUR_USD", 5).with_max_position("5000"), "1.10048", "1.10052")
        .instrument(InstrumentSpec::fx("GBP_USD", 5), "1.26996", "1.27004")
        .build();
    let rig = OandaRig::with_fake(fake, |c| c);
    assert_eq!(rig.adapter.instrument("EUR_USD").unwrap().maximum_position_size, Some(d("5000")));
    assert_eq!(rig.adapter.instrument("GBP_USD").unwrap().maximum_position_size, None, "the fake reports \"0\": no cap");
    rig.adapter.place_order(&buy("cap:1", "3000")).unwrap();
    let e = rig.adapter.place_order(&buy("cap:2", "2001")).unwrap_err();
    assert!(matches!(e, BrokerError::InvalidRequest(ref m) if m.contains("maximumPositionSize")), "{e:?}");
    assert_eq!(rig.handle.position_units("EUR_USD"), d("3000"), "nothing was sent and nothing truncated");
    assert_eq!(posts_applied(&rig), 1);
    rig.adapter.place_order(&buy("cap:3", "2000")).unwrap();
    assert_eq!(rig.handle.position_units("EUR_USD"), d("5000"));
    // no cap on GBP_USD: any size the exchange allows
    rig.adapter.place_order(&OrderRequest::market("cap:4", "GBP/USD", Side::Buy, d("9000000"))).unwrap();
    // selling down from the cap is a reduction
    rig.adapter.place_order(&sell("cap:5", "1000")).unwrap();
    assert_eq!(rig.handle.position_units("EUR_USD"), d("4000"));
}

// ---------------------------------------------------------------- partial fills, limits, cancel

#[test]
fn an_ioc_order_that_is_only_partly_filled_reports_the_executed_quantity() {
    let rig = OandaRig::new();
    rig.handle.set_liquidity("EUR_USD", Some("400"));
    let mut req = buy("t:ioc", "1000");
    req.time_in_force = Some(TimeInForce::Ioc);
    match rig.adapter.place_order(&req).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, warnings, .. } => {
            assert_eq!(warnings, ["filled 400 of 1000 units"]);
            let r = rig.adapter.get_order(&broker_order_id).unwrap();
            assert_eq!((r.status, r.quantity, r.executed_quantity), (OrderStatus::PartiallyFilledThenCanceled, d("1000"), d("400")));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.position_units("EUR_USD"), d("400"));
    // a FOK for more than is available is a definite rejection
    match rig.adapter.place_order(&buy("t:fok", "1000")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert!(errors[0].code.contains("INSUFFICIENT_LIQUIDITY")),
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.position_units("EUR_USD"), d("400"));
}

#[test]
fn a_limit_order_rests_shows_in_open_orders_fills_on_a_price_move_and_can_be_cancelled() {
    let rig = OandaRig::new();
    let lim = OrderRequest::limit("t:lim", "EUR/USD", Side::Buy, d("2000"), d("1.09500"));
    let id = accepted_id(rig.adapter.place_order(&lim).unwrap());
    let open = rig.adapter.open_orders().unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!((open[0].broker_order_id.as_str(), open[0].status, open[0].tag.as_deref()), (id.as_str(), OrderStatus::Open, Some("t:lim")));
    // cancel
    let (out, report) = rig.adapter.cancel_and_settle(&id).unwrap();
    assert_eq!((out.canceled_count, report.status, report.reason.as_deref()), (1, OrderStatus::Canceled, Some("CLIENT_REQUEST")));
    assert!(rig.adapter.open_orders().unwrap().is_empty());
    // a new limit that fills when the price moves
    let id2 = accepted_id(rig.adapter.place_order(&OrderRequest::limit("t:lim2", "EUR/USD", Side::Buy, d("1000"), d("1.09500"))).unwrap());
    rig.handle.set_price("EUR_USD", "1.09400", "1.09404");
    let r = rig.adapter.get_order(&id2).unwrap();
    assert_eq!((r.status, r.executed_quantity, r.avg_price), (OrderStatus::Filled, d("1000"), Some(d("1.09404"))));
    // cancelling an order that already filled: the cancel is refused, the fill is reported, count 0
    let (out, report) = rig.adapter.cancel_and_settle(&id2).unwrap();
    assert_eq!((out.canceled_count, out.pending, report.status), (0, false, OrderStatus::Filled));
    // and an unknown id
    assert!(matches!(rig.adapter.cancel_and_settle("99999"), Err(BrokerError::CancelTargetNotFound(_))));
    rig.handle.assert_invariants();
}

#[test]
fn foreign_orders_are_visible_but_not_ours_when_a_prefix_is_configured() {
    let rig = OandaRig::with_config(|c| c.with_own_tag_prefix("rb1:").unwrap());
    let foreign = rig.handle.add_foreign_limit("GBP_USD", "-3000", "1.30000");
    rig.adapter.place_order(&OrderRequest::limit("rb1:x", "EUR/USD", Side::Buy, d("100"), d("1.05000"))).unwrap();
    let open = rig.adapter.open_orders().unwrap();
    assert_eq!(open.len(), 2);
    let f = open.iter().find(|o| o.broker_order_id == foreign).unwrap();
    assert_eq!((f.tag.clone(), f.side, f.symbol.as_str()), (None, Some(Side::Sell), "GBP/USD"));
    assert_eq!(open.iter().filter(|o| o.tag.is_some()).count(), 1);
    // placement with a foreign-looking tag is refused locally
    assert!(rig.adapter.place_order(&buy("other:1", "100")).is_err());
}

// ---------------------------------------------------------------- close / flatten one instrument
//
// MEASURED: a close sends ALL for the side that exists and NONE for the other; ALL for an absent side is a 400 at the exchange
// (and at the fake), so every successful close below is also proof that the adapter sent only the side that exists.

#[test]
fn close_position_flattens_long_and_short_one_sided_and_is_idempotent_by_tag() {
    let rig = OandaRig::new();
    rig.adapter.place_order(&buy("t:l", "8000")).unwrap();
    rig.adapter.place_order(&OrderRequest::market("t:s", "GBP/USD", Side::Sell, d("3000"))).unwrap();
    match rig.adapter.close_position("EUR_USD", "t:close-eur").unwrap() {
        CloseOutcome::Closed { fill, .. } => assert_eq!((fill.units, fill.price), (d("-8000"), d("1.10048"))),
        other => panic!("{other:?}"),
    }
    match rig.adapter.close_position("GBP_USD", "t:close-gbp").unwrap() {
        CloseOutcome::Closed { fill, .. } => assert_eq!((fill.units, fill.price), (d("3000"), d("1.27004"))),
        other => panic!("{other:?}"),
    }
    assert!(rig.adapter.get_open_positions().unwrap().is_empty());
    // the same tag again adopts the earlier close (the fake echoes the extensions); a fresh tag on a flat account has nothing to do
    assert!(matches!(rig.adapter.close_position("EUR_USD", "t:close-eur").unwrap(), CloseOutcome::AlreadyDone { .. }));
    assert_eq!(rig.adapter.close_position("EUR_USD", "t:close-eur-2").unwrap(), CloseOutcome::NothingToClose);
    assert_eq!(rig.handle.applied(HttpMethod::Put, "/close").len(), 2);
    // no request of the adapter was ever answered 400 CLOSEOUT_POSITION_DOESNT_EXIST: it never sent ALL for an absent side
    assert!(rig.handle.requests().iter().filter(|r| r.path.ends_with("/close")).all(|r| r.produced.as_ref().is_some_and(|(s, _)| *s == 200)));
    rig.handle.assert_invariants();
}

#[test]
fn a_close_whose_answer_is_lost_is_found_in_the_stream_when_the_extensions_are_echoed() {
    let rig = OandaRig::new();
    rig.adapter.place_order(&buy("t:l", "4000")).unwrap();
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(&rig.handle.path("/positions/EUR_USD/close")));
    match rig.adapter.close_position("EUR_USD", "t:c").unwrap() {
        CloseOutcome::Closed { fill, .. } => assert_eq!(fill.units, d("-4000")),
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.position_units("EUR_USD"), Dec::ZERO, "the close was applied");
    assert_eq!(rig.adapter.find_orders_by_tag("t:c").unwrap().len(), 1);
    // a restart-and-retry adopts it from the recent window; nothing is sent
    let mut rig = rig;
    rig.restart_adapter();
    assert!(matches!(rig.adapter.close_position("EUR_USD", "t:c").unwrap(), CloseOutcome::AlreadyDone { .. }));
    assert_eq!(rig.handle.applied(HttpMethod::Put, "/close").len(), 1);
}

#[test]
fn a_close_whose_answer_is_lost_is_settled_from_the_position_when_the_extensions_are_not_echoed() {
    // UNMEASURED at real OANDA whether longClientExtensions is echoed onto the closeout transactions. If it is not, the tag is
    // invisible in the stream and the position itself must decide.
    let rig = OandaRig::new();
    rig.handle.set_echo_close_client_ids(false);
    rig.adapter.place_order(&buy("t:l", "4000")).unwrap();
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(&rig.handle.path("/positions/EUR_USD/close")));
    match rig.adapter.close_position("EUR_USD", "t:c").unwrap() {
        CloseOutcome::AlreadyFlat { detail } => assert!(detail.contains("now flat") && detail.contains("unverified"), "{detail}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.position_units("EUR_USD"), Dec::ZERO);
    // a retry of the same tag finds the position flat and sends nothing
    assert_eq!(rig.adapter.close_position("EUR_USD", "t:c").unwrap(), CloseOutcome::NothingToClose);
    assert_eq!(rig.handle.applied(HttpMethod::Put, "/close").len(), 1);
    // the request that never arrived leaves the position open: unknown, and the retry closes it exactly once
    rig.adapter.place_order(&buy("t:l2", "1000")).unwrap();
    rig.handle.inject_fault(Fault::io_error().on_path(&rig.handle.path("/positions/EUR_USD/close")));
    assert!(matches!(rig.adapter.close_position("EUR_USD", "t:c2").unwrap(), CloseOutcome::UnknownOutcome { .. }));
    assert_eq!(rig.handle.position_units("EUR_USD"), d("1000"));
    assert!(matches!(rig.adapter.close_position("EUR_USD", "t:c2").unwrap(), CloseOutcome::Closed { .. }));
    assert_eq!(rig.handle.position_units("EUR_USD"), Dec::ZERO);
    assert_eq!(rig.handle.applied(HttpMethod::Put, "/close").len(), 2);
    rig.handle.assert_invariants();
}

#[test]
fn a_close_that_is_refused_is_reported_and_leaves_the_position() {
    let rig = OandaRig::new();
    rig.adapter.place_order(&buy("t:l", "4000")).unwrap();
    rig.handle.script_next_order(OrderScript::Reject("MARKET_HALTED".into()));
    match rig.adapter.close_position("EUR_USD", "t:c1").unwrap() {
        CloseOutcome::Rejected { errors } => assert!(errors[0].code.contains("MARKET_HALTED")),
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.position_units("EUR_USD"), d("4000"));
    assert_eq!(rig.adapter.tag_checkpoint("t:c1"), None, "a refused close created nothing");
    rig.handle.set_market_open(false);
    match rig.adapter.close_position("EUR_USD", "t:c2").unwrap() {
        CloseOutcome::Rejected { errors } => assert!(errors[0].code.contains("MARKET_HALTED")),
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.position_units("EUR_USD"), d("4000"));
}

#[test]
fn netting_through_zero_keeps_the_adapters_reads_consistent() {
    let rig = OandaRig::new();
    rig.adapter.place_order(&buy("t:1", "1000")).unwrap();
    rig.adapter.place_order(&sell("t:2", "2500")).unwrap();
    assert_eq!(rig.handle.position_units("EUR_USD"), d("-1500"));
    let pos = rig.adapter.get_position("EUR_USD").unwrap().unwrap();
    assert_eq!((pos.long_units, pos.short_units, pos.net_units()), (d("0"), d("-1500"), d("-1500")));
    assert!(!pos.is_hedged());
    rig.adapter.place_order(&buy("t:3", "1500")).unwrap();
    assert!(rig.adapter.get_position("EUR_USD").unwrap().is_none());
    rig.handle.assert_invariants();
}

#[test]
fn reduce_only_orders_never_open_or_grow_a_position() {
    let rig = OandaRig::new();
    rig.adapter.place_order(&buy("t:1", "1000")).unwrap();
    let mut too_big = sell("t:r1", "2000");
    too_big.reduce_only = true;
    assert!(matches!(rig.adapter.place_order(&too_big).unwrap(), PlaceOutcome::Rejected { .. }));
    assert_eq!(rig.handle.position_units("EUR_USD"), d("1000"));
    let mut ok = sell("t:r2", "600");
    ok.reduce_only = true;
    assert!(matches!(rig.adapter.place_order(&ok).unwrap(), PlaceOutcome::Accepted { .. }));
    assert_eq!(rig.handle.position_units("EUR_USD"), d("400"));
    assert_eq!(rig.handle.orders().last().unwrap().position_fill, "REDUCE_ONLY");
}
