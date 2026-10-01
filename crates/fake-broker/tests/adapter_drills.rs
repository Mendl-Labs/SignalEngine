//! Kill-drill and journey scenarios that need only a broker (SPEC section 5 B1-B10, WP4.4
//! restart / duplicate-run drills), driving the REAL `KrakenAdapter` against the stateful fake.
//!
//! Each test states the drill, scripts what happens at the exchange with `scenarios::*`, and
//! asserts on BOTH the adapter's behaviour and the exchange's own books / event log.

use broker_adapters::kraken::KrakenConfig;
use broker_adapters::nonce::{Clock, NonceStore};
use broker_adapters::transport::TransportError;
use broker_adapters::{
    Balances, BrokerAdapter, BrokerError, Dec, ErrorClass, OrderReport, OrderRequest, OrderStatus, PlaceOutcome, Side,
};
use fake_broker::money::sub;
use fake_broker::scenarios;
use fake_broker::testkit::{d, KrakenRig};
use fake_broker::{Fault, FillPolicy, OrderRule};

const ADD: &str = "/0/private/AddOrder";

fn buy(tag: &str, qty: &str) -> OrderRequest {
    OrderRequest::market(tag, "BTC/USD", Side::Buy, d(qty))
}

fn last_nonce(rig: &KrakenRig) -> u64 {
    rig.nonce_store.last().unwrap().unwrap()
}

fn unknown_outcome(out: PlaceOutcome) -> String {
    match out {
        PlaceOutcome::UnknownOutcome { reason, .. } => reason,
        other => panic!("expected UnknownOutcome, got {other:?}"),
    }
}

// ================================================================= unknown outcome (double-send)

#[test]
fn order_placed_but_response_timed_out_adapter_reports_unknown_and_lookup_by_userref_finds_it() {
    let rig = KrakenRig::new();
    let tag = "run7:BTC/USD:buy";
    scenarios::order_placed_response_lost(&rig.handle);

    let out = rig.adapter.place_order(&buy(tag, "0.01")).unwrap();
    assert!(unknown_outcome(out).contains("timed out"));
    // What the caller cannot see: the exchange placed and filled the order.
    assert_eq!(rig.handle.orders("main").len(), 1);
    assert_eq!(rig.balance("BTC"), d("0.01"));

    // The safe reaction: look up by tag, find the order, do NOT re-send.
    let found = rig.adapter.find_orders_by_tag(tag).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].status, OrderStatus::Filled);
    assert_eq!(found[0].executed_quantity, d("0.01"));
    assert_eq!(found[0].tag.as_deref(), Some(tag));

    assert_eq!(rig.handle.applied_requests_to(ADD).len(), 1, "exactly one AddOrder reached the exchange");
    rig.handle.assert_invariants();
}

#[test]
fn a_blind_retry_after_a_lost_response_doubles_the_position_negative_control() {
    // Kraken does not dedupe on userref, so the drill above is only meaningful because a naive
    // caller WOULD double-send. This proves the harness can see that failure.
    let rig = KrakenRig::new();
    let tag = "run7:BTC/USD:buy";
    scenarios::order_placed_response_lost(&rig.handle);
    unknown_outcome(rig.adapter.place_order(&buy(tag, "0.01")).unwrap());
    let retry = rig.adapter.place_order(&buy(tag, "0.01")).unwrap();
    assert!(matches!(retry, PlaceOutcome::Accepted { .. }), "the exchange happily accepts the duplicate");
    assert_eq!(rig.handle.orders("main").len(), 2);
    assert_eq!(rig.balance("BTC"), d("0.02"), "double exposure");
    assert_eq!(rig.adapter.find_orders_by_tag(tag).unwrap().len(), 2, "same userref on both");
    assert_eq!(rig.handle.applied_requests_to(ADD).len(), 2);
}

#[test]
fn request_lost_before_it_reached_the_exchange_lookup_finds_nothing_and_resend_is_safe() {
    let rig = KrakenRig::new();
    let tag = "run8:BTC/USD:buy";
    scenarios::order_request_lost(&rig.handle);
    unknown_outcome(rig.adapter.place_order(&buy(tag, "0.01")).unwrap());
    assert!(rig.handle.orders("main").is_empty());
    assert!(rig.adapter.find_orders_by_tag(tag).unwrap().is_empty(), "nothing placed: safe to re-send");

    match rig.adapter.place_order(&buy(tag, "0.01")).unwrap() {
        PlaceOutcome::Accepted { .. } => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.orders("main").len(), 1);
    assert_eq!(rig.handle.count_requests(ADD), 2);
    assert_eq!(rig.handle.applied_requests_to(ADD).len(), 1);
    assert_eq!(rig.adapter.find_orders_by_tag(tag).unwrap().len(), 1);
}

/// The full reconcile-then-decide procedure a rebalancer must follow, as a test-local helper.
fn place_idempotently(rig: &KrakenRig, req: &OrderRequest) -> Result<Vec<OrderReport>, BrokerError> {
    match rig.adapter.place_order(req)? {
        PlaceOutcome::Accepted { broker_order_id, .. } => Ok(vec![rig.adapter.get_order(&broker_order_id)?]),
        PlaceOutcome::UnknownOutcome { .. } => {
            let found = rig.adapter.find_orders_by_tag(&req.tag)?;
            if !found.is_empty() {
                return Ok(found);
            }
            match rig.adapter.place_order(req)? {
                PlaceOutcome::Accepted { broker_order_id, .. } => Ok(vec![rig.adapter.get_order(&broker_order_id)?]),
                other => panic!("second attempt: {other:?}"),
            }
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn reconcile_then_decide_places_exactly_one_order_under_every_ambiguous_failure() {
    type Script = fn(&fake_broker::FakeBrokerHandle);
    let scripts: Vec<(&str, Script)> = vec![
        ("timeout after apply", |h| h.inject_fault(Fault::timeout().after_apply().on_path(ADD))),
        ("timeout before apply", |h| h.inject_fault(Fault::timeout().on_path(ADD))),
        ("io error after apply", |h| h.inject_fault(Fault::io_error().after_apply().on_path(ADD))),
        ("502 after apply", |h| h.inject_fault(Fault::http(502).after_apply().on_path(ADD))),
        ("502 before apply", |h| h.inject_fault(Fault::http(502).on_path(ADD))),
        ("malformed body after apply", |h| h.inject_fault(Fault::malformed_body().after_apply().on_path(ADD))),
        ("EService:Unavailable after apply", |h| scenarios::exchange_answers_error_after_placing(h, "EService:Unavailable")),
        ("EGeneral:Internal error after apply", |h| scenarios::exchange_answers_error_after_placing(h, "EGeneral:Internal error")),
        ("EService:Unavailable before apply", |h| h.inject_fault(Fault::exchange_error("EService:Unavailable").on_path(ADD))),
    ];
    for (name, script) in scripts {
        let rig = KrakenRig::new();
        script(&rig.handle);
        let reports = place_idempotently(&rig, &buy("run9:BTC/USD:buy", "0.01")).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(reports.len(), 1, "{name}");
        assert_eq!(reports[0].status, OrderStatus::Filled, "{name}");
        assert_eq!(rig.handle.orders("main").len(), 1, "{name}: exactly one order at the exchange");
        assert_eq!(rig.balance("BTC"), d("0.01"), "{name}");
        rig.handle.assert_invariants();
    }
}

#[test]
fn definite_failures_are_not_unknown_outcomes() {
    // connection refused: definitely not sent
    let rig = KrakenRig::new();
    scenarios::broker_unreachable_for(&rig.handle, 1);
    match rig.adapter.place_order(&buy("c", "0.01")) {
        Err(BrokerError::Transport(t)) => assert!(t.request_definitely_not_sent()),
        other => panic!("{other:?}"),
    }
    assert!(rig.handle.orders("main").is_empty());

    // rate limited: a definite rejection, nothing placed
    let rig = KrakenRig::new();
    scenarios::rate_limited_for(&rig.handle, 1);
    match rig.adapter.place_order(&buy("r", "0.01")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert_eq!(errors[0].class, ErrorClass::RateLimited),
        other => panic!("{other:?}"),
    }
    assert!(rig.handle.orders("main").is_empty());
    assert!(matches!(rig.adapter.place_order(&buy("r", "0.01")).unwrap(), PlaceOutcome::Accepted { .. }), "recovered");
}

// ================================================================= outage and recovery

#[test]
fn exchange_unavailable_for_n_calls_then_recovers_no_trades_during_the_outage() {
    let rig = KrakenRig::new();
    scenarios::exchange_unavailable_for(&rig.handle, 4);
    // reads fail with a ServiceUnavailable class
    for _ in 0..2 {
        match rig.adapter.get_balances() {
            Err(BrokerError::Exchange(e)) => assert_eq!(e[0].class, ErrorClass::ServiceUnavailable),
            other => panic!("{other:?}"),
        }
    }
    // an order sent during the outage is an UnknownOutcome (Unavailable can mean either)
    unknown_outcome(rig.adapter.place_order(&buy("out1", "0.01")).unwrap());
    // the lookup itself fails while the exchange is still down
    assert!(rig.adapter.find_orders_by_tag("out1").is_err());
    // recovered: nothing was placed, so re-sending is safe
    assert!(rig.adapter.find_orders_by_tag("out1").unwrap().is_empty());
    assert!(rig.handle.pending_faults().is_empty());
    assert!(matches!(rig.adapter.place_order(&buy("out1", "0.01")).unwrap(), PlaceOutcome::Accepted { .. }));
    assert_eq!(rig.handle.orders("main").len(), 1);
}

#[test]
fn broker_unreachable_means_no_trades_at_all_and_the_log_proves_it() {
    // WP7 gate 4: "broker unreachable means no trades and an alert" (the alert is the caller's).
    let rig = KrakenRig::new();
    rig.handle.inject_fault(Fault::connect_failed().forever());
    assert!(matches!(rig.adapter.get_balances(), Err(BrokerError::Transport(TransportError::ConnectFailed(_)))));
    assert!(rig.adapter.place_order(&buy("x", "0.01")).is_err());
    scenarios::assert_no_order_activity(&rig.handle);
    assert!(rig.handle.orders("main").is_empty());
    assert_eq!(rig.handle.requests().iter().filter(|r| r.reached_exchange).count(), 0, "nothing reached the exchange");
    rig.handle.clear_faults();
    assert!(rig.adapter.get_balances().is_ok());
}

// ================================================================= nonce

#[test]
fn nonce_collision_from_another_process_is_a_definite_rejection_that_heals() {
    let rig = KrakenRig::new();
    rig.adapter.get_balances().unwrap(); // the adapter is at nonce T
    let last = last_nonce(&rig);
    // another process using the same key jumps 3 ahead of the adapter
    let r = scenarios::nonce_collision(&rig.handle, &rig.api_key, last + 3);
    assert!(r.body.contains(r#""error":[]"#), "{}", r.body);

    // each failed attempt still burns one adapter nonce, so it catches up after 3 failures
    let mut failures = 0;
    loop {
        match rig.adapter.place_order(&buy("nc", "0.01")).unwrap() {
            PlaceOutcome::Rejected { errors, .. } => {
                assert_eq!(errors[0].class, ErrorClass::InvalidNonce);
                assert!(!errors[0].outcome_unknown(), "a nonce rejection means nothing was processed");
                assert!(rig.handle.orders("main").is_empty(), "no order was created by a rejected nonce");
                failures += 1;
                assert!(failures < 10, "never recovered");
            }
            PlaceOutcome::Accepted { .. } => break,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(failures, 3);
    assert_eq!(rig.handle.orders("main").len(), 1);
}

#[test]
fn a_moving_clock_lets_the_adapter_jump_past_another_process_at_once() {
    let rig = KrakenRig::new();
    rig.adapter.get_balances().unwrap();
    let last = last_nonce(&rig);
    scenarios::nonce_collision(&rig.handle, &rig.api_key, last + 1_000_000);
    assert!(matches!(rig.adapter.get_balances(), Err(BrokerError::Exchange(_))));
    rig.handle.advance_secs(1); // +1e9 ns, past the other process's nonce
    assert!(rig.adapter.get_balances().is_ok());
}

#[test]
fn reads_report_invalid_nonce_with_its_class_too() {
    let rig = KrakenRig::new();
    rig.adapter.get_balances().unwrap();
    let last = last_nonce(&rig);
    scenarios::nonce_collision(&rig.handle, &rig.api_key, last + 50);
    match rig.adapter.get_balances() {
        Err(BrokerError::Exchange(errs)) => {
            assert_eq!(errs[0].class, ErrorClass::InvalidNonce);
            assert_eq!(errs[0].code, "EAPI:Invalid nonce");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn nonces_survive_a_restart_when_persisted_and_collide_when_the_store_is_lost() {
    // Persisted store: a restart carries on after the highest nonce, even if the clock jumped back.
    let mut rig = KrakenRig::new();
    for _ in 0..3 {
        rig.adapter.get_balances().unwrap();
    }
    rig.handle.clock().set_nanos(rig.handle.clock().now_nanos() - 5_000_000_000); // clock steps back 5 s
    rig.restart_adapter();
    assert!(rig.adapter.get_balances().is_ok(), "last+1 beats a clock that went backwards");

    // Lost store + clock that went backwards = the exchange rejects (this is what real Kraken does).
    rig.restart_adapter_losing_everything();
    match rig.adapter.place_order(&buy("lost-store", "0.01")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert_eq!(errors[0].class, ErrorClass::InvalidNonce),
        other => panic!("{other:?}"),
    }
    assert!(rig.handle.orders("main").is_empty());
}

#[test]
fn a_restart_keeps_nonces_strictly_increasing_as_the_exchange_saw_them() {
    let mut rig = KrakenRig::new();
    for _ in 0..3 {
        rig.adapter.get_balances().unwrap();
        rig.restart_adapter();
    }
    let nonces: Vec<u64> = rig.handle.requests().iter().map(|r| r.param("nonce").unwrap().parse().unwrap()).collect();
    assert_eq!(nonces.len(), 3);
    assert!(nonces.windows(2).all(|w| w[0] < w[1]), "{nonces:?}");
}

// ================================================================= insufficient funds

#[test]
fn insufficient_funds_real_and_scripted_leave_no_order_and_the_run_can_continue() {
    let rig = KrakenRig::new();
    // real: reservation by a resting order starves the next one
    assert!(matches!(rig.adapter.place_order(&OrderRequest::limit("big", "BTC/USD", Side::Buy, d("1.6"), d("60000"))).unwrap(), PlaceOutcome::Accepted { .. }));
    match rig.adapter.place_order(&buy("starved", "0.1")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert_eq!(errors[0].class, ErrorClass::InsufficientFunds),
        other => panic!("{other:?}"),
    }
    assert!(rig.adapter.find_orders_by_tag("starved").unwrap().is_empty());
    // scripted: same shape, for drills that need it regardless of balances
    scenarios::insufficient_funds_next_order(&rig.handle);
    assert!(matches!(rig.adapter.place_order(&buy("scripted", "0.001")).unwrap(), PlaceOutcome::Rejected { .. }));
    assert!(matches!(rig.adapter.place_order(&buy("scripted", "0.001")).unwrap(), PlaceOutcome::Accepted { .. }));
}

// ================================================================= foreign orders and drift

#[test]
fn a_foreign_order_appears_the_adapter_reports_it_untagged_and_our_lookups_ignore_it() {
    let rig = KrakenRig::new();
    rig.handle.set_balance("main", "ETH", "5");
    let (mine, _) = match rig.adapter.place_order(&OrderRequest::limit("mine", "BTC/USD", Side::Buy, d("0.01"), d("59000"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, sent, .. } => (broker_order_id, sent),
        other => panic!("{other:?}"),
    };
    assert_eq!(rig.adapter.open_orders().unwrap().len(), 1);

    let foreign = scenarios::foreign_order_appears(&rig.handle, "main", "ETH/USD", Side::Sell, "1", "4000.00");
    let open = rig.adapter.open_orders().unwrap();
    assert_eq!(open.len(), 2);
    let f = open.iter().find(|o| o.broker_order_id == foreign).unwrap();
    assert_eq!(f.tag, None, "no userref of ours");
    assert_eq!(f.userref, None);
    assert_eq!(f.symbol, "ETH/USD");
    assert_eq!(f.status, OrderStatus::Open);
    let m = open.iter().find(|o| o.broker_order_id == mine).unwrap();
    assert_eq!(m.tag.as_deref(), Some("mine"));
    // a caller's rule "every open order must be ours" therefore fires:
    let unexplained: Vec<_> = open.iter().filter(|o| o.tag.is_none()).collect();
    assert_eq!(unexplained.len(), 1);

    assert_eq!(rig.adapter.find_orders_by_tag("mine").unwrap().len(), 1, "lookups by tag exclude it");
}

#[test]
fn a_foreign_order_with_a_userref_we_never_assigned_is_still_foreign() {
    let rig = KrakenRig::new();
    rig.handle.set_balance("main", "ETH", "5");
    rig.handle.add_foreign_order("main", fake_broker::ForeignOrder::limit("ETH/USD", Side::Sell, "1", "4000.00").userref(4242));
    let open = rig.adapter.open_orders().unwrap();
    assert_eq!(open[0].userref, Some(4242));
    assert_eq!(open[0].tag, None, "the userref is not in our table");
}

#[test]
fn a_partially_filled_foreign_order_moves_balances_the_adapter_cannot_explain_by_its_own_orders() {
    let rig = KrakenRig::new();
    rig.handle.set_balance("main", "ETH", "5");
    rig.handle.add_foreign_order("main", fake_broker::ForeignOrder::limit("ETH/USD", Side::Sell, "2", "4000.00").filled("0.5"));
    let b = rig.adapter.get_balances().unwrap();
    assert_eq!(b.spot("ETH"), d("4.5"));
    assert!(b.spot("USD") > d("100000"), "proceeds arrived");
    assert!(rig.adapter.find_orders_by_tag("anything-of-ours").is_err(), "and no order of ours explains it");
    rig.handle.assert_invariants();
}

fn expected_btc_from_our_orders(rig: &KrakenRig, tags: &[&str]) -> Dec {
    let mut total = Dec::ZERO;
    for t in tags {
        for r in rig.adapter.find_orders_by_tag(t).unwrap() {
            let signed = match r.side {
                Some(Side::Sell) => d("-1"),
                _ => d("1"),
            };
            total = total.checked_add(r.executed_quantity.checked_mul(signed).unwrap()).unwrap();
        }
    }
    total
}

#[test]
fn unexplained_balance_drift_shows_up_as_a_mismatch_between_reports_and_balances() {
    let rig = KrakenRig::new();
    rig.adapter.place_order(&buy("a", "0.01")).unwrap();
    rig.adapter.place_order(&buy("b", "0.02")).unwrap();
    let explained = expected_btc_from_our_orders(&rig, &["a", "b"]);
    assert_eq!(rig.adapter.get_balances().unwrap().spot("BTC"), explained, "reconciled before the drift");

    scenarios::balance_drift(&rig.handle, "main", "BTC", "-0.004");
    let after = rig.adapter.get_balances().unwrap().spot("BTC");
    assert_eq!(after, d("0.026"));
    assert_ne!(after, expected_btc_from_our_orders(&rig, &["a", "b"]), "0.004 BTC vanished with no order behind it");
    rig.handle.assert_invariants(); // the fake's ledger records the drift as an external movement
}

// ================================================================= dropped / duplicated fill reports

#[test]
fn a_dropped_fill_report_makes_the_order_look_cancelled_while_balances_prove_it_filled() {
    let rig = KrakenRig::new();
    let txid = match rig.adapter.place_order(&buy("dropped", "0.01")).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    rig.handle.drop_fill_report(&txid, 0).unwrap();
    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!(r.status, OrderStatus::Canceled, "closed with vol_exec 0 reads as cancelled");
    assert_eq!(r.executed_quantity, Dec::ZERO);
    assert_eq!(rig.adapter.get_balances().unwrap().spot("BTC"), d("0.01"), "but the broker balance shows the fill");
    // restoring the report restores agreement
    rig.handle.restore_fill_report(&txid, 0).unwrap();
    assert_eq!(rig.adapter.get_order(&txid).unwrap().status, OrderStatus::Filled);
    rig.handle.assert_invariants();
}

#[test]
fn a_duplicated_fill_report_double_counts_execution_against_the_balance() {
    let rig = KrakenRig::new();
    rig.handle.arm_fill_report_glitch(fake_broker::ReportGlitch::Duplicated);
    let txid = match rig.adapter.place_order(&buy("dup", "0.01")).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    // (was: the report was accepted with executed_quantity 0.02; the adapter now refuses it, see
    // `a_fill_report_reporting_more_than_the_order_volume_is_an_anomaly_error_not_an_inflated_fill`)
    assert!(matches!(rig.adapter.get_order(&txid), Err(BrokerError::Malformed(_))), "the report says twice what really executed");
    assert_eq!(rig.adapter.get_balances().unwrap().spot("BTC"), d("0.01"), "broker balance is the truth (AD2)");
    assert_eq!(rig.handle.order(&txid).unwrap().vol_exec(), d("0.01"));
}

// ================================================================= B4 / B5 / B6 / B7 / B10

/// A minimal flatten used only to prove the fake supports the kill drill: cancel everything,
/// then market-sell every held asset, looking each order up by tag before sending.
fn flatten(rig: &KrakenRig, run: &str, pass: u32) -> Result<(), String> {
    let a = &rig.adapter;
    for o in a.open_orders().map_err(|e| e.to_string())? {
        match a.cancel_order(&o.broker_order_id) {
            Ok(_) => {}
            Err(BrokerError::Exchange(e)) if e[0].class == ErrorClass::UnknownOrder => {} // raced a fill
            Err(e) => return Err(e.to_string()),
        }
    }
    let balances: Balances = a.get_balances().map_err(|e| e.to_string())?;
    for (asset, symbol) in [("BTC", "BTC/USD"), ("ETH", "ETH/USD")] {
        let qty = balances.spot(asset);
        if !qty.is_positive() {
            continue;
        }
        let tag = format!("{run}:{symbol}:flatten:{pass}");
        if let Ok(found) = a.find_orders_by_tag(&tag) {
            if !found.is_empty() {
                continue; // an earlier attempt of this very pass already placed it
            }
        }
        let req = OrderRequest::market(&tag, symbol, Side::Sell, qty);
        match a.place_order(&req).map_err(|e| e.to_string())? {
            PlaceOutcome::Accepted { .. } => {}
            PlaceOutcome::UnknownOutcome { reason, .. } => return Err(format!("unknown outcome: {reason}")),
            other => return Err(format!("{other:?}")),
        }
    }
    Ok(())
}

fn is_flat(rig: &KrakenRig) -> bool {
    let b = rig.adapter.get_balances().unwrap();
    !b.spot("BTC").is_positive() && !b.spot("ETH").is_positive() && rig.adapter.open_orders().unwrap().is_empty()
}

fn holding_rig() -> KrakenRig {
    let rig = KrakenRig::new();
    rig.adapter.place_order(&buy("seed-btc", "0.5")).unwrap();
    rig.adapter.place_order(&OrderRequest::market("seed-eth", "ETH/USD", Side::Buy, d("4"))).unwrap();
    rig
}

#[test]
fn b4_daily_loss_breach_broker_marked_equity_trips_and_flatten_reaches_flat() {
    let rig = holding_rig();
    rig.adapter.place_order(&OrderRequest::limit("stale-limit", "BTC/USD", Side::Buy, d("0.01"), d("50000"))).unwrap();
    let day_start = rig.handle.equity("main");

    // the recorded connector moves equity down 3.1 percent
    let target = scenarios::equity_below(&rig.handle, "main", day_start, "0.031");
    let tb = rig.adapter.trade_balance(Some("ZUSD")).unwrap();
    assert_eq!(tb.equity, Some(target), "the broker's own equity figure carries the loss exactly");
    let loss = day_start.checked_add(target.checked_mul(d("-1")).unwrap()).unwrap();
    assert_eq!(loss, day_start.checked_mul(d("0.031")).unwrap());

    // halt: cancel, flatten, verify flat
    flatten(&rig, "kill-b4", 1).unwrap();
    assert!(is_flat(&rig), "flat and nothing resting");
    assert!(rig.handle.live_orders("main").is_empty());
    rig.handle.assert_invariants();
}

#[test]
fn b5_drawdown_ladder_boundaries_are_exact_in_broker_equity() {
    let rig = holding_rig();
    let hwm = rig.handle.equity("main");
    for (label, ratio) in [
        ("just below rung 1", "0.0999999"),
        ("exactly rung 1", "0.10"),
        ("just above rung 1", "0.1000001"),
        ("exactly rung 2", "0.20"),
    ] {
        let target = scenarios::equity_below(&rig.handle, "main", hwm, ratio);
        let eq = rig.adapter.trade_balance(None).unwrap().equity.unwrap();
        assert_eq!(eq, target, "{label}");
        // the ratio recovered in exact decimal arithmetic is the boundary value itself
        let drawdown = hwm.checked_add(eq.checked_mul(d("-1")).unwrap()).unwrap();
        assert_eq!(drawdown, hwm.checked_mul(d(ratio)).unwrap(), "{label}");
    }
}

#[test]
fn b6_flatten_is_idempotent_across_a_crash_mid_flatten() {
    let mut rig = holding_rig();
    // the first flatten sell is APPLIED but its response is lost; the "process" crashes there
    scenarios::order_placed_response_lost(&rig.handle);
    let crash = flatten(&rig, "kill-b6", 1);
    assert!(crash.unwrap_err().contains("unknown outcome"));
    let sells_after_crash = rig.handle.applied_requests_to(ADD).iter().filter(|r| r.param("type") == Some("sell")).count();
    assert_eq!(sells_after_crash, 1);

    // restart (nonce store and userref table persisted) and run the SAME flatten again
    rig.restart_adapter();
    flatten(&rig, "kill-b6", 1).unwrap();
    assert!(is_flat(&rig));
    let sells: Vec<_> = rig.handle.applied_requests_to(ADD).into_iter().filter(|r| r.param("type") == Some("sell")).collect();
    assert_eq!(sells.len(), 2, "one per asset: BTC (from the crashed attempt) and ETH (after restart); BTC was NOT sold twice");
    let btc_sells = sells.iter().filter(|r| r.param("pair") == Some("XBTUSD")).count();
    assert_eq!(btc_sells, 1);

    // and a third run on a flat account is a no-op success
    let before = rig.handle.count_requests(ADD);
    flatten(&rig, "kill-b6", 1).unwrap();
    assert_eq!(rig.handle.count_requests(ADD), before, "no AddOrder at all");
    rig.handle.assert_invariants();
}

#[test]
fn b6_flatten_handles_partial_fills_and_a_cancel_that_raced_a_fill() {
    let rig = holding_rig();
    // a resting order that fills completely just before flatten cancels it
    let (txid, _) = match rig.adapter.place_order(&OrderRequest::limit("resting", "BTC/USD", Side::Buy, d("0.01"), d("50000"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, sent, .. } => (broker_order_id, sent),
        other => panic!("{other:?}"),
    };
    rig.handle.script_fill(&txid, "0.01", None).unwrap();
    // and the flatten sell itself fills only partially at first (leaving a resting remainder)
    rig.handle.script_orders(OrderRule::next(FillPolicy::partial(vec![fake_broker::FillStep::fraction("0.5")])).side(Side::Sell).pair("BTC/USD"));
    flatten(&rig, "kill-b6b", 1).unwrap();
    assert!(!is_flat(&rig), "the partially filled sell still holds half of the BTC");
    // second pass: cancel the resting remainder, sell what is left
    flatten(&rig, "kill-b6b", 2).unwrap();
    assert!(is_flat(&rig));
    rig.handle.assert_invariants();
}

#[test]
fn b7_the_broker_reported_loss_is_visible_even_when_our_own_ledger_would_be_wrong() {
    // The platform ledger is deliberately "corrupted" (it believes it holds 0.01 BTC less than
    // the exchange does). Broker-marked equity, from TradeBalance, does not depend on it.
    let rig = holding_rig();
    let ledger_view_of_btc = sub(expected_btc_from_our_orders(&rig, &["seed-btc"]), d("0.01"));
    let broker_btc = rig.adapter.get_balances().unwrap().spot("BTC");
    assert_ne!(ledger_view_of_btc, broker_btc, "ledger and broker disagree: a reconciliation mismatch");
    let hwm = rig.handle.equity("main");
    scenarios::market_move(&rig.handle, "BTC/USD", "-0.30");
    scenarios::market_move(&rig.handle, "ETH/USD", "-0.30");
    let eq = rig.adapter.trade_balance(None).unwrap().equity.unwrap();
    assert!(eq < hwm, "the loss is in the broker figure");
    assert_eq!(eq, rig.handle.equity("main"));
}

#[test]
fn b10_shadow_mode_a_validate_only_run_changes_no_exchange_state() {
    let rig = KrakenRig::with_config(KrakenConfig { force_validate: true, ..KrakenConfig::default() });
    rig.handle.set_balance("main", "BTC", "0.3");
    let before_balances = rig.handle.balances("main");
    // a whole "rebalance": several buys and sells, a cancel attempt and reads
    for (i, side) in [Side::Buy, Side::Sell, Side::Buy].into_iter().enumerate() {
        let out = rig.adapter.place_order(&OrderRequest::market(&format!("shadow{i}"), "BTC/USD", side, d("0.05"))).unwrap();
        assert!(matches!(out, PlaceOutcome::ValidatedOnly { .. }));
    }
    rig.adapter.get_balances().unwrap();
    rig.adapter.open_orders().unwrap();
    scenarios::assert_no_order_activity(&rig.handle);
    assert_eq!(rig.handle.balances("main"), before_balances);
    assert!(rig.handle.orders("main").is_empty());
}

#[test]
fn b1_the_exchange_side_of_no_mandate_no_live_order_is_simply_that_no_order_request_is_sent() {
    // The mandate check lives in the rebalancer; at the broker the observable is the request log.
    let rig = KrakenRig::new();
    rig.adapter.get_balances().unwrap();
    rig.adapter.open_orders().unwrap();
    scenarios::assert_no_order_activity(&rig.handle);
    assert_eq!(scenarios::order_affecting_requests(&rig.handle), 0);
    rig.adapter.place_order(&buy("x", "0.001")).unwrap();
    assert_eq!(scenarios::order_affecting_requests(&rig.handle), 1);
}

// ================================================================= restart / userref persistence

#[test]
fn lookup_by_tag_after_a_restart_needs_the_persisted_userref_table() {
    let mut rig = KrakenRig::new();
    scenarios::order_placed_response_lost(&rig.handle);
    unknown_outcome(rig.adapter.place_order(&buy("persist-me", "0.01")).unwrap());

    // process restarted WITH the userref table persisted before the send (as documented)
    rig.restart_adapter();
    assert_eq!(rig.adapter.find_orders_by_tag("persist-me").unwrap().len(), 1);

    // restarted WITHOUT it: the adapter cannot map the tag until the caller re-assigns it
    rig.restart_adapter_losing_userrefs();
    assert!(matches!(rig.adapter.find_orders_by_tag("persist-me"), Err(BrokerError::UnknownTag(_))));
    // reserve_userref is deterministic for a tag (same hash, no collision in a fresh table), so a
    // caller that re-reserves gets the same userref and finds the order again.
    rig.adapter.reserve_userref("persist-me").unwrap();
    let found = rig.adapter.find_orders_by_tag("persist-me").unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].tag.as_deref(), Some("persist-me"));
}

#[test]
fn duplicate_run_drill_the_second_run_finds_the_first_runs_orders_and_places_nothing() {
    let rig = KrakenRig::new();
    let run_tags = ["run-2026-10-01:BTC/USD:buy", "run-2026-10-01:ETH/USD:buy"];
    let orders = [
        OrderRequest::market(run_tags[0], "BTC/USD", Side::Buy, d("0.01")),
        OrderRequest::market(run_tags[1], "ETH/USD", Side::Buy, d("0.5")),
    ];
    for o in &orders {
        assert!(matches!(rig.adapter.place_order(o).unwrap(), PlaceOutcome::Accepted { .. }));
    }
    let placed_before = rig.handle.applied_requests_to(ADD).len();
    // duplicate trigger of the same run (same run key => same tags): look before sending
    for o in &orders {
        let existing = rig.adapter.find_orders_by_tag(&o.tag).unwrap();
        assert_eq!(existing.len(), 1, "already done: skip");
    }
    assert_eq!(rig.handle.applied_requests_to(ADD).len(), placed_before);
    assert_eq!(rig.handle.orders("main").len(), 2);
}

#[test]
fn the_fakes_clock_only_moves_when_told_and_time_shows_up_in_order_reports() {
    let rig = KrakenRig::new();
    let t0 = rig.handle.clock().now_nanos();
    let (id1, id2) = {
        let a = match rig.adapter.place_order(&buy("t1", "0.001")).unwrap() {
            PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
            o => panic!("{o:?}"),
        };
        rig.handle.advance_secs(90);
        let b = match rig.adapter.place_order(&buy("t2", "0.001")).unwrap() {
            PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
            o => panic!("{o:?}"),
        };
        (a, b)
    };
    assert_eq!(rig.handle.clock().now_nanos(), t0 + 90_000_000_000);
    let (r1, r2) = (rig.adapter.get_order(&id1).unwrap(), rig.adapter.get_order(&id2).unwrap());
    let (o1, o2) = (r1.open_time.unwrap(), r2.open_time.unwrap());
    assert!((o2 - o1 - 90.0).abs() < 1e-3, "{o1} {o2}");
    assert_eq!(rig.adapter.get_balances().unwrap().spot("BTC"), d("0.002"));
}

// ================================================================= delayed delivery (order still in flight)

#[test]
fn a_timed_out_order_that_lands_later_loses_to_the_nonce_rule_once_the_lookup_has_run() {
    // The nastiest unknown outcome: the request has NOT been applied when we look it up, but it
    // is still in flight. If it landed after our re-send we would hold two positions. Kraken's
    // strictly increasing nonce prevents that: the lookup used a higher nonce, so the stale
    // request is refused when it finally arrives.
    let rig = KrakenRig::new();
    let tag = "run10:BTC/USD:buy";
    rig.handle.inject_fault(Fault::timeout().delayed().on_path(ADD));
    unknown_outcome(rig.adapter.place_order(&buy(tag, "0.01")).unwrap());
    assert_eq!(rig.handle.delayed_count(), 1);

    assert!(rig.adapter.find_orders_by_tag(tag).unwrap().is_empty(), "not applied yet, so the lookup finds nothing");
    let late = rig.handle.deliver_delayed();
    assert_eq!(late.len(), 1);
    assert!(late[0].body.contains("EAPI:Invalid nonce"), "the late request is refused: {}", late[0].body);
    assert!(rig.handle.orders("main").is_empty(), "so re-sending is safe after all");

    match rig.adapter.place_order(&buy(tag, "0.01")).unwrap() {
        PlaceOutcome::Accepted { .. } => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.handle.orders("main").len(), 1);
    let log = rig.handle.dump_log();
    assert!(log.contains("[delayed-delivery]"), "{log}");
}

#[test]
fn a_delayed_request_that_arrives_before_any_newer_request_is_applied_normally() {
    // Without an intervening request the late order DOES land: the lookup-before-resend step is
    // what makes the difference, and this test would catch a fake that always refused it.
    let rig = KrakenRig::new();
    rig.handle.inject_fault(Fault::timeout().delayed().on_path(ADD));
    unknown_outcome(rig.adapter.place_order(&buy("late", "0.01")).unwrap());
    let late = rig.handle.deliver_delayed();
    assert!(late[0].body.contains("txid"), "{}", late[0].body);
    assert_eq!(rig.handle.orders("main").len(), 1);
    assert_eq!(rig.adapter.find_orders_by_tag("late").unwrap().len(), 1);
}

// ================================================================= more adapter <-> exchange behaviour

#[test]
fn ioc_and_post_only_flags_from_the_adapter_are_understood_by_the_exchange() {
    use broker_adapters::TimeInForce;
    let rig = KrakenRig::new();
    // IOC limit away from the touch: accepted, immediately cancelled with nothing executed
    let mut ioc = OrderRequest::limit("ioc", "BTC/USD", Side::Buy, d("0.01"), d("59000"));
    ioc.time_in_force = Some(TimeInForce::Ioc);
    let txid = match rig.adapter.place_order(&ioc).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!((r.status, r.executed_quantity), (OrderStatus::Canceled, Dec::ZERO));

    // post-only that would cross: definite rejection
    let mut post = OrderRequest::limit("post", "BTC/USD", Side::Buy, d("0.01"), d("61000"));
    post.post_only = true;
    match rig.adapter.place_order(&post).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => {
            assert_eq!(errors[0].code, "EOrder:Post only order");
            assert_eq!(errors[0].class, ErrorClass::OrderRejected);
        }
        other => panic!("{other:?}"),
    }
    // post-only that rests
    let mut rest = OrderRequest::limit("rest", "BTC/USD", Side::Buy, d("0.01"), d("59000"));
    rest.post_only = true;
    assert!(matches!(rig.adapter.place_order(&rest).unwrap(), PlaceOutcome::Accepted { .. }));
}

#[test]
fn eth_and_btc_orders_share_one_account_and_keep_separate_books() {
    let rig = KrakenRig::new();
    rig.adapter.place_order(&buy("b", "0.01")).unwrap();
    rig.adapter.place_order(&OrderRequest::market("e", "ETH/USD", Side::Buy, d("0.5"))).unwrap();
    let b = rig.adapter.get_balances().unwrap();
    assert_eq!((b.spot("BTC"), b.spot("ETH")), (d("0.01"), d("0.5")));
    let e = &rig.adapter.find_orders_by_tag("e").unwrap()[0];
    assert_eq!(e.symbol, "ETH/USD");
    assert_eq!(e.avg_price, Some(d("3000.01")));
}

#[test]
fn cancel_and_settle_settles_to_the_final_report_when_the_order_already_filled() {
    // Was `observation_cancel_and_settle_errors_when_the_order_already_filled`. When the order
    // filled before the cancel landed, the cancel fails with EOrder:Unknown order; `cancel_and_settle`
    // now re-queries and returns the final report with nothing canceled.
    let rig = KrakenRig::new();
    let txid = match rig.adapter.place_order(&OrderRequest::limit("cs", "BTC/USD", Side::Buy, d("0.01"), d("59000"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    rig.handle.script_fill(&txid, "0.01", None).unwrap();
    let (outcome, settled) = rig.adapter.cancel_and_settle(&txid).unwrap();
    assert_eq!((outcome.canceled_count, outcome.pending), (0, false), "nothing was canceled");
    assert_eq!(settled.status, OrderStatus::Filled);
    assert_eq!(settled.executed_quantity, d("0.01"));
    assert_eq!(rig.adapter.get_order(&txid).unwrap().status, OrderStatus::Filled);
}

#[test]
fn a_fill_report_reporting_more_than_the_order_volume_is_an_anomaly_error_not_an_inflated_fill() {
    // Was `observation_a_fill_report_reporting_more_than_the_order_volume_is_accepted_as_filled`.
    // `executed_quantity > quantity` (impossible on a healthy exchange; here a duplicated fill
    // report) is now `Malformed("overfill anomaly ...")`, so the caller halts and reconciles
    // against balances (AD2) instead of double counting.
    let rig = KrakenRig::new();
    rig.handle.arm_fill_report_glitch(fake_broker::ReportGlitch::Duplicated);
    let txid = match rig.adapter.place_order(&buy("over", "0.01")).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    match rig.adapter.get_order(&txid) {
        Err(BrokerError::Malformed(m)) => assert!(m.contains("overfill anomaly"), "{m}"),
        other => panic!("expected an overfill anomaly, got {other:?}"),
    }
}

#[test]
fn concurrent_callers_never_trip_the_strict_nonce_rule_because_the_adapter_serialises_send() {
    // Kraken rejects any request whose nonce is not above the highest seen. Eight threads
    // sharing one adapter must therefore never see EAPI:Invalid nonce.
    let rig = KrakenRig::new();
    std::thread::scope(|s| {
        for t in 0..8 {
            let rig = &rig;
            s.spawn(move || {
                for i in 0..15 {
                    if i % 3 == 0 {
                        let out = rig.adapter.place_order(&buy(&format!("thread{t}:{i}"), "0.001")).unwrap();
                        assert!(matches!(out, PlaceOutcome::Accepted { .. }), "{out:?}");
                    } else {
                        rig.adapter.get_balances().unwrap();
                    }
                }
            });
        }
    });
    assert_eq!(rig.handle.orders("main").len(), 8 * 5);
    let invalid: usize = rig.handle.requests().iter().filter(|r| r.produced_errors().iter().any(|e| e.contains("Invalid nonce"))).count();
    assert_eq!(invalid, 0);
    let nonces: Vec<u64> = rig.handle.requests().iter().map(|r| r.param("nonce").unwrap().parse().unwrap()).collect();
    assert!(nonces.windows(2).all(|w| w[0] < w[1]), "arrival order equals nonce order");
    rig.handle.assert_invariants();
}
