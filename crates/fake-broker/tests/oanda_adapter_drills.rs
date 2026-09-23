//! Rung 2: the REAL `OandaAdapter` against the fake OANDA exchange, over the adapter's own `HttpTransport` trait.
//! Place / look up / cancel / close, unknown outcomes then lookup by tag, restart mid-run idempotency, margin and
//! market-hours refusals, partial fills, netting through zero, and consistency of what the adapter reads with the
//! exchange's own books.
//!
//! What a green run means: the adapter agrees with THIS model of OANDA, which was written from the same documentation
//! as the adapter. It says nothing about the real service (see the module docs of `broker_adapters::oanda`).

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

#[test]
fn response_lost_after_the_order_was_placed_then_lookup_by_tag_finds_it_and_a_replace_does_not_resend() {
    let rig = OandaRig::new();
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(&rig.handle.path("/orders")));
    let out = rig.adapter.place_order(&buy("t:lost", "700")).unwrap();
    assert!(matches!(out, PlaceOutcome::UnknownOutcome { .. }), "{out:?}");
    // the order really exists
    assert_eq!(rig.handle.orders_with_client_id("t:lost").len(), 1);
    // the caller's contract: look it up by tag, never blind-retry
    let found = rig.adapter.find_orders_by_tag("t:lost").unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!((found[0].status, found[0].executed_quantity), (OrderStatus::Filled, d("700")));
    // even a careless second place() with the same tag sends nothing: it adopts
    let second = rig.adapter.place_order(&buy("t:lost", "700")).unwrap();
    assert_eq!(accepted_id(second), found[0].broker_order_id);
    assert_eq!(rig.handle.applied(HttpMethod::Post, "/orders").len(), 1, "exactly one POST ever reached the exchange");
    assert_eq!(rig.handle.position_units("EUR_USD"), d("700"), "no double position");
    rig.handle.assert_invariants();
}

#[test]
fn request_lost_before_it_arrived_is_found_absent_and_a_replace_places_it_once() {
    let rig = OandaRig::new();
    rig.handle.inject_fault(Fault::io_error().on_path(&rig.handle.path("/orders")));
    assert!(matches!(rig.adapter.place_order(&buy("t:gone", "300")).unwrap(), PlaceOutcome::UnknownOutcome { .. }));
    assert!(rig.adapter.find_orders_by_tag("t:gone").unwrap().is_empty(), "nothing exists");
    let id = accepted_id(rig.adapter.place_order(&buy("t:gone", "300")).unwrap());
    assert!(!id.is_empty());
    assert_eq!(rig.handle.orders_with_client_id("t:gone").len(), 1);
    assert_eq!(rig.handle.position_units("EUR_USD"), d("300"));
}

#[test]
fn an_error_answer_after_the_order_was_placed_is_an_unknown_outcome_and_the_order_is_found() {
    for fault in [Fault::http(502), Fault::http(500), Fault::malformed_body(), Fault::exchange_error("boom")] {
        let rig = OandaRig::new();
        rig.handle.inject_fault(fault.after_apply().on_path(&rig.handle.path("/orders")));
        let out = rig.adapter.place_order(&buy("t:e", "200")).unwrap();
        assert!(matches!(out, PlaceOutcome::UnknownOutcome { .. }), "{out:?}");
        assert_eq!(rig.adapter.find_orders_by_tag("t:e").unwrap().len(), 1);
        assert_eq!(rig.handle.applied(HttpMethod::Post, "/orders").len(), 1);
    }
}

#[test]
fn restart_mid_run_with_no_state_carried_over_does_not_double_submit() {
    let mut rig = OandaRig::new();
    // run 1 places two orders, then "dies" after the second was applied but before it was recorded
    rig.adapter.place_order(&buy("rb:run7:EUR/USD:buy", "1000")).unwrap();
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(&rig.handle.path("/orders")));
    let _ = rig.adapter.place_order(&OrderRequest::market("rb:run7:GBP/USD:sell", "GBP/USD", Side::Sell, d("500"))).unwrap();
    assert_eq!(rig.handle.applied(HttpMethod::Post, "/orders").len(), 2);
    // process restart: a brand-new adapter, nothing persisted
    rig.restart_adapter();
    // run 2 replays the same plan with the same tags
    let a = rig.adapter.place_order(&buy("rb:run7:EUR/USD:buy", "1000")).unwrap();
    let b = rig.adapter.place_order(&OrderRequest::market("rb:run7:GBP/USD:sell", "GBP/USD", Side::Sell, d("500"))).unwrap();
    assert!(matches!(a, PlaceOutcome::Accepted { ref warnings, .. } if warnings[0].contains("already existed")));
    assert!(matches!(b, PlaceOutcome::Accepted { .. }));
    assert_eq!(rig.handle.applied(HttpMethod::Post, "/orders").len(), 2, "the replay sent nothing");
    assert_eq!(rig.handle.position_units("EUR_USD"), d("1000"));
    assert_eq!(rig.handle.position_units("GBP_USD"), d("-500"));
    rig.handle.assert_invariants();
}

#[test]
fn the_fake_being_strict_about_ids_changes_nothing_because_the_adapter_looks_first() {
    // Same drill with an exchange that refuses reused PENDING client ids: still exactly one order.
    let rig = OandaRig::new();
    rig.handle.reject_duplicate_client_ids(true);
    rig.adapter.place_order(&buy("t:s", "100")).unwrap();
    rig.adapter.place_order(&buy("t:s", "100")).unwrap();
    assert_eq!(rig.handle.orders_with_client_id("t:s").len(), 1);
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
fn rate_limiting_and_an_unreachable_exchange_mean_not_sent() {
    let rig = OandaRig::new();
    rig.handle.inject_fault(Fault::rate_limit().on_path(&rig.handle.path("/orders")));
    assert!(matches!(rig.adapter.place_order(&buy("t:rl", "100")), Err(BrokerError::RateLimited { .. })));
    assert!(rig.handle.orders().is_empty());
    rig.handle.inject_fault(Fault::connect_failed().on_path(&rig.handle.path("/orders")));
    assert!(matches!(rig.adapter.place_order(&buy("t:rl", "100")), Err(BrokerError::Transport(TransportError::ConnectFailed(_)))));
    assert!(rig.handle.orders().is_empty());
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

#[test]
fn close_position_flattens_long_and_short_and_is_idempotent_by_tag() {
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
    // the same tag again adopts the earlier close; a fresh tag on a flat account has nothing to do
    assert!(matches!(rig.adapter.close_position("EUR_USD", "t:close-eur").unwrap(), CloseOutcome::AlreadyDone { .. }));
    assert_eq!(rig.adapter.close_position("EUR_USD", "t:close-eur-2").unwrap(), CloseOutcome::NothingToClose);
    assert_eq!(rig.handle.applied(HttpMethod::Put, "/close").len(), 2);
    rig.handle.assert_invariants();
}

#[test]
fn a_close_whose_answer_is_lost_is_found_by_tag_and_never_sent_twice() {
    let rig = OandaRig::new();
    rig.adapter.place_order(&buy("t:l", "4000")).unwrap();
    rig.handle.inject_fault(Fault::timeout().after_apply().on_path(&rig.handle.path("/positions/EUR_USD/close")));
    assert!(matches!(rig.adapter.close_position("EUR_USD", "t:c").unwrap(), CloseOutcome::UnknownOutcome { .. }));
    assert_eq!(rig.handle.position_units("EUR_USD"), Dec::ZERO, "the close was applied");
    // the by-tag lookup finds the close order; a restart-and-retry adopts it
    assert_eq!(rig.adapter.find_orders_by_tag("t:c").unwrap().len(), 1);
    let mut rig = rig;
    rig.restart_adapter();
    assert!(matches!(rig.adapter.close_position("EUR_USD", "t:c").unwrap(), CloseOutcome::AlreadyDone { .. }));
    assert_eq!(rig.handle.applied(HttpMethod::Put, "/close").len(), 1);
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
