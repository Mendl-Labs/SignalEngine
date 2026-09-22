//! The REAL `KrakenAdapter` driven against the stateful fake exchange: order lifecycle, fills,
//! partial fills, cancels, validate-only and the adapter's rounding versus the exchange's rules.

use broker_adapters::kraken::KrakenConfig;
use broker_adapters::{
    BrokerAdapter, BrokerError, Dec, ErrorClass, OrderKind, OrderReport, OrderRequest, OrderStatus, PlaceOutcome, Side,
};
use fake_broker::rng::SplitMix64;
use fake_broker::scenarios;
use fake_broker::testkit::{d, KrakenRig};

fn accepted(out: PlaceOutcome) -> (String, broker_adapters::SentOrder) {
    match out {
        PlaceOutcome::Accepted { broker_order_id, sent, .. } => (broker_order_id, sent),
        other => panic!("expected Accepted, got {other:?}"),
    }
}

fn buy(tag: &str, qty: &str) -> OrderRequest {
    OrderRequest::market(tag, "BTC/USD", Side::Buy, d(qty))
}

fn limit_buy(tag: &str, qty: &str, price: &str) -> OrderRequest {
    OrderRequest::limit(tag, "BTC/USD", Side::Buy, d(qty), d(price))
}

// ---------------------------------------------------------------- lifecycle

#[test]
fn market_buy_place_poll_and_read_balances() {
    let rig = KrakenRig::new();
    let (txid, sent) = accepted(rig.adapter.place_order(&buy("run1:BTC/USD:buy", "0.01")).unwrap());
    assert_eq!(sent.quantity, d("0.01"));
    assert_eq!(sent.broker_pair, "XBTUSD");

    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!(r.status, OrderStatus::Filled);
    assert_eq!(r.executed_quantity, d("0.01"));
    assert_eq!(r.avg_price, Some(d("60000.1")));
    assert_eq!(r.cost, Some(d("600.001")));
    assert_eq!(r.fee, Some(d("1.5600026")));
    assert_eq!(r.tag.as_deref(), Some("run1:BTC/USD:buy"));
    assert_eq!(r.symbol, "BTC/USD");
    assert_eq!(r.side, Some(Side::Buy));
    assert_eq!(r.kind, Some(OrderKind::Market));

    let b = rig.adapter.get_balances().unwrap();
    assert_eq!(b.spot("BTC"), d("0.01"));
    assert_eq!(b.spot("USD"), d("99398.4389974"));
    let q = rig.adapter.get_quote("BTC/USD").unwrap();
    assert_eq!((q.bid, q.ask, q.last), (d("60000"), d("60000.1"), d("60000")));
    rig.handle.assert_invariants();
}

#[test]
fn the_adapters_view_of_a_fill_equals_the_exchanges_own_books() {
    let rig = KrakenRig::new();
    let (txid, _) = accepted(rig.adapter.place_order(&buy("t", "0.0123")).unwrap());
    let report = rig.adapter.get_order(&txid).unwrap();
    let order = rig.handle.order(&txid).unwrap();
    assert_eq!(report.executed_quantity, order.vol_exec());
    assert_eq!(report.cost, Some(order.cost()));
    assert_eq!(report.fee, Some(order.fee()));
    assert_eq!(report.avg_price, order.avg_price());
}

#[test]
fn a_resting_limit_shows_open_then_fills_when_the_market_moves() {
    let rig = KrakenRig::new();
    let (txid, _) = accepted(rig.adapter.place_order(&limit_buy("rest", "0.01", "59000")).unwrap());
    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!(r.status, OrderStatus::Open);
    assert_eq!(r.executed_quantity, Dec::ZERO);
    assert_eq!(r.avg_price, None);
    assert_eq!(rig.adapter.open_orders().unwrap().len(), 1);

    rig.handle.set_price("BTC/USD", "58000");
    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!(r.status, OrderStatus::Filled);
    assert_eq!(r.avg_price, Some(d("59000")));
    assert!(rig.adapter.open_orders().unwrap().is_empty());
    assert_eq!(rig.balance("BTC"), d("0.01"));
}

#[test]
fn validate_only_places_nothing_and_reserves_nothing() {
    let rig = KrakenRig::new();
    let mut req = limit_buy("v", "0.01", "59000");
    req.validate_only = true;
    match rig.adapter.place_order(&req).unwrap() {
        PlaceOutcome::ValidatedOnly { description, sent, .. } => {
            assert!(sent.validate_only);
            assert_eq!(description.as_deref(), Some("buy 0.01000000 XBTUSD @ limit 59000.0"));
        }
        other => panic!("{other:?}"),
    }
    assert!(rig.handle.orders("main").is_empty());
    assert_eq!(rig.handle.available("main", "USD"), d("100000"));
    assert!(rig.adapter.find_orders_by_tag("v").unwrap().is_empty());
    scenarios::assert_no_order_activity(&rig.handle);
    // it does not consume a scripted rule either
    scenarios::insufficient_funds_next_order(&rig.handle);
    assert!(matches!(rig.adapter.place_order(&req).unwrap(), PlaceOutcome::Rejected { .. }), "reports the scripted refusal");
    assert!(matches!(rig.adapter.place_order(&buy("real", "0.01")).unwrap(), PlaceOutcome::Rejected { .. }), "still armed");
    assert!(matches!(rig.adapter.place_order(&buy("real2", "0.01")).unwrap(), PlaceOutcome::Accepted { .. }));
}

#[test]
fn paper_mode_force_validate_never_creates_an_order() {
    // SPEC B10 in miniature: a shadow/paper adapter changes no order state at the exchange.
    let rig = KrakenRig::with_config(KrakenConfig { force_validate: true, ..KrakenConfig::default() });
    for i in 0..5 {
        let out = rig.adapter.place_order(&buy(&format!("paper{i}"), "0.01")).unwrap();
        assert!(matches!(out, PlaceOutcome::ValidatedOnly { .. }), "{out:?}");
    }
    assert!(rig.handle.orders("main").is_empty());
    assert_eq!(rig.balance("USD"), d("100000"));
    assert_eq!(rig.handle.count_requests("/0/private/AddOrder"), 5, "the exchange did see (and validate) them");
    scenarios::assert_no_order_activity(&rig.handle);
}

// ---------------------------------------------------------------- partial fills and cancel

#[test]
fn partial_fill_then_cancel_keeps_the_executed_quantity() {
    let rig = KrakenRig::new();
    scenarios::partial_fills_next_order(&rig.handle, &["0.4"]);
    let (txid, _) = accepted(rig.adapter.place_order(&limit_buy("pc", "0.01", "59000")).unwrap());

    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!(r.status, OrderStatus::PartiallyFilled);
    assert_eq!(r.executed_quantity, d("0.004"));
    assert_eq!(rig.balance("BTC"), d("0.004"));

    let (outcome, settled) = rig.adapter.cancel_and_settle(&txid).unwrap();
    assert_eq!((outcome.canceled_count, outcome.pending), (1, false));
    assert_eq!(settled.status, OrderStatus::PartiallyFilledThenCanceled);
    assert!(settled.status.is_terminal() && settled.status.has_fills());
    assert_eq!(settled.executed_quantity, d("0.004"), "the executed part is real and stays");
    assert_eq!(settled.avg_price, Some(d("59000")));
    assert_eq!(settled.cost, Some(d("236")));
    assert_eq!(settled.fee, Some(d("0.3776")), "maker fee on a resting limit");
    assert_eq!(settled.reason.as_deref(), Some("User requested"));
    assert_eq!(rig.balance("BTC"), d("0.004"), "cancel does not undo the fill");
    assert_eq!(rig.balance("USD"), d("99763.6224"));
    assert_eq!(rig.handle.available("main", "USD"), d("99763.6224"), "reservation released");
    // and the closed-order listing finds it by tag
    let found = rig.adapter.find_orders_by_tag("pc").unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].status, OrderStatus::PartiallyFilledThenCanceled);
    rig.handle.assert_invariants();
}

#[test]
fn a_scripted_partial_fill_sequence_at_moving_prices_accumulates_exactly() {
    let rig = KrakenRig::new();
    scenarios::partial_fills_next_order(&rig.handle, &["0.25", "0.25"]);
    let (txid, _) = accepted(rig.adapter.place_order(&buy("seq", "0.01")).unwrap());
    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!((r.status, r.executed_quantity), (OrderStatus::PartiallyFilled, d("0.0025")));
    assert_eq!(r.avg_price, Some(d("60000.1")));

    rig.handle.set_price("BTC/USD", "61000");
    rig.handle.apply_next_fill(&txid).unwrap();
    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!(r.executed_quantity, d("0.005"));
    // (0.0025 * 60000.1 + 0.0025 * 61000.1) / 0.005
    assert_eq!(r.cost, Some(d("302.5005")));
    assert_eq!(r.avg_price, Some(d("60500.1")));

    // the script is exhausted at 50 percent: the order stays open until it is finished by hand
    assert!(rig.handle.apply_next_fill(&txid).is_err());
    rig.handle.script_fill(&txid, "0.005", None).unwrap();
    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!((r.status, r.executed_quantity), (OrderStatus::Filled, d("0.01")));
    assert_eq!(r.cost, Some(d("607.501")), "0.005 more at the 61000.1 touch");
    assert_eq!(r.avg_price, Some(d("60750.1")));
    rig.handle.assert_invariants();
}

#[test]
fn cancel_racing_a_fill_reports_unknown_order_and_the_follow_up_read_shows_the_truth() {
    let rig = KrakenRig::new();
    let (txid, _) = accepted(rig.adapter.place_order(&limit_buy("race", "0.01", "59000")).unwrap());
    rig.handle.script_fill(&txid, "0.01", None).unwrap(); // filled just before our cancel lands
    match rig.adapter.cancel_order(&txid) {
        Err(BrokerError::Exchange(errs)) => assert_eq!(errs[0].class, ErrorClass::UnknownOrder),
        other => panic!("{other:?}"),
    }
    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!(r.status, OrderStatus::Filled, "so a caller must always re-read after a cancel");
    assert_eq!(rig.balance("BTC"), d("0.01"));
}

#[test]
fn a_cancel_that_is_accepted_but_still_pending_can_lose_the_race_to_a_fill() {
    let rig = KrakenRig::new();
    let (txid, _) = accepted(rig.adapter.place_order(&limit_buy("pending-cancel", "0.01", "59000")).unwrap());
    rig.handle.defer_next_cancels(1);
    let outcome = rig.adapter.cancel_order(&txid).unwrap();
    assert!(outcome.pending, "the exchange has not processed the cancel yet");
    assert_eq!(rig.adapter.get_order(&txid).unwrap().status, OrderStatus::Open);
    rig.handle.script_fill(&txid, "0.004", None).unwrap();
    rig.handle.settle_pending_cancels();
    let r = rig.adapter.get_order(&txid).unwrap();
    assert_eq!(r.status, OrderStatus::PartiallyFilledThenCanceled);
    assert_eq!(r.executed_quantity, d("0.004"));
}

#[test]
fn cancel_by_tag_cancels_our_orders_only() {
    let rig = KrakenRig::new();
    rig.handle.set_balance("main", "ETH", "10");
    let foreign = scenarios::foreign_order_appears(&rig.handle, "main", "ETH/USD", Side::Sell, "1", "4000.00");
    let (mine, _) = accepted(rig.adapter.place_order(&limit_buy("mine", "0.01", "59000")).unwrap());
    let c = rig.adapter.cancel_by_tag("mine").unwrap();
    assert_eq!(c.canceled_count, 1);
    assert_eq!(rig.adapter.get_order(&mine).unwrap().status, OrderStatus::Canceled);
    assert!(rig.handle.order(&foreign).unwrap().status.is_live(), "someone else's order is untouched");
    assert!(matches!(rig.adapter.cancel_by_tag("mine"), Err(BrokerError::Exchange(_))), "nothing left to cancel");
}

#[test]
fn exchange_side_expiry_and_cancel_are_reported_with_executed_quantity() {
    let rig = KrakenRig::new();
    let (a, _) = accepted(rig.adapter.place_order(&limit_buy("exp", "0.01", "59000")).unwrap());
    rig.handle.script_fill(&a, "0.003", None).unwrap();
    rig.handle.expire_order(&a).unwrap();
    let r = rig.adapter.get_order(&a).unwrap();
    assert_eq!((r.status, r.executed_quantity), (OrderStatus::PartiallyFilledThenExpired, d("0.003")));

    let (b, _) = accepted(rig.adapter.place_order(&limit_buy("exp2", "0.01", "59000")).unwrap());
    rig.handle.expire_order(&b).unwrap();
    assert_eq!(rig.adapter.get_order(&b).unwrap().status, OrderStatus::Expired);

    let (c, _) = accepted(rig.adapter.place_order(&limit_buy("kicked", "0.01", "59000")).unwrap());
    rig.handle.script_fill(&c, "0.001", None).unwrap();
    rig.handle.cancel_from_exchange(&c, "Insufficient margin").unwrap();
    let r = rig.adapter.get_order(&c).unwrap();
    assert_eq!(r.status, OrderStatus::PartiallyFilledThenCanceled);
    assert_eq!(r.reason.as_deref(), Some("Insufficient margin"));
}

#[test]
fn pending_orders_are_reported_pending_until_activated() {
    let rig = KrakenRig::new();
    rig.handle.script_orders(fake_broker::OrderRule::next(fake_broker::FillPolicy::Pending));
    let (txid, _) = accepted(rig.adapter.place_order(&buy("pend", "0.01")).unwrap());
    assert_eq!(rig.adapter.get_order(&txid).unwrap().status, OrderStatus::Pending);
    assert_eq!(rig.adapter.open_orders().unwrap().len(), 1, "pending orders show in OpenOrders");
    assert_eq!(rig.balance("BTC"), Dec::ZERO);
    rig.handle.activate_order(&txid).unwrap();
    assert_eq!(rig.adapter.get_order(&txid).unwrap().status, OrderStatus::Filled, "a market order fills once active");
}

// ---------------------------------------------------------------- rejections

#[test]
fn insufficient_funds_is_a_definite_rejection_and_nothing_is_created() {
    let rig = KrakenRig::new();
    match rig.adapter.place_order(&buy("big", "2")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => {
            assert_eq!(errors[0].code, "EOrder:Insufficient funds");
            assert_eq!(errors[0].class, ErrorClass::InsufficientFunds);
            assert!(!errors[0].outcome_unknown());
        }
        other => panic!("{other:?}"),
    }
    assert!(rig.handle.orders("main").is_empty());
    assert!(rig.adapter.find_orders_by_tag("big").unwrap().is_empty());
}

#[test]
fn scripted_rejections_surface_with_their_classes() {
    let rig = KrakenRig::new();
    scenarios::invalid_arguments_next_order(&rig.handle);
    match rig.adapter.place_order(&buy("a", "0.01")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert_eq!(errors[0].class, ErrorClass::InvalidArguments),
        other => panic!("{other:?}"),
    }
    rig.handle.script_orders(fake_broker::OrderRule::next(fake_broker::FillPolicy::reject("EOrder:Order minimum not met")));
    match rig.adapter.place_order(&buy("b", "0.01")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert_eq!(errors[0].class, ErrorClass::OrderRejected),
        other => panic!("{other:?}"),
    }
    assert!(rig.handle.orders("main").is_empty());
}

#[test]
fn a_cancel_only_pair_is_refused_by_the_exchange_when_the_adapters_table_is_stale() {
    let rig = KrakenRig::new();
    rig.handle.set_pair_status("BTC/USD", "cancel_only");
    // the builtin table has no status, so the adapter sends the order and the exchange refuses
    match rig.adapter.place_order(&buy("co", "0.01")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => {
            assert_eq!(errors[0].code, "EService:Market in cancel_only mode");
            assert_eq!(errors[0].class, ErrorClass::OrderRejected, "not ServiceUnavailable: the order definitely was not placed");
        }
        other => panic!("{other:?}"),
    }
    // with a live table the adapter refuses locally and sends nothing
    let table = rig.pair_table_from_exchange();
    let creds = broker_adapters::kraken::auth::KrakenCredentials::new(&rig.api_key, &rig.secret_b64).unwrap();
    let nonces = broker_adapters::nonce::NonceGenerator::new(rig.nonce_store.clone(), rig.clock.clone());
    let live = broker_adapters::kraken::KrakenAdapter::new(KrakenConfig::default(), creds, rig.transport.clone(), nonces, table);
    let before = rig.handle.count_requests("/0/private/AddOrder");
    assert!(matches!(live.place_order(&buy("co2", "0.01")), Err(BrokerError::PairNotTradable { .. })));
    assert_eq!(rig.handle.count_requests("/0/private/AddOrder"), before);
}

#[test]
fn permission_denied_is_an_auth_error_and_never_retryable_state() {
    let rig = KrakenRig::new();
    rig.handle.set_key_permissions(&rig.api_key, fake_broker::KeyPermissions::read_only());
    match rig.adapter.place_order(&buy("ro", "0.01")).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert_eq!(errors[0].class, ErrorClass::Auth),
        other => panic!("{other:?}"),
    }
    assert!(rig.adapter.get_balances().is_ok(), "reads still work");
    assert!(rig.handle.orders("main").is_empty());
}

// ---------------------------------------------------------------- adapter rounding vs exchange rules

#[test]
fn whatever_the_adapter_sends_the_exchange_accepts_rounding_and_minimums_property() {
    // 400 random orders with silly precision. Either the adapter refuses locally (sending
    // nothing), or the exchange accepts: never an exchange rejection about precision or minimums.
    let rig = KrakenRig::new();
    rig.handle.set_balance("main", "USD", "100000000");
    rig.handle.set_balance("main", "BTC", "1000");
    rig.handle.set_balance("main", "ETH", "10000");
    let mut rng = SplitMix64::new(0xC0FFEE);
    let (mut sent, mut refused_locally) = (0, 0);
    for i in 0..400u64 {
        let (symbol, base_px) = if rng.chance(1, 2) { ("BTC/USD", 60000u64) } else { ("ETH/USD", 3000u64) };
        let side = if rng.chance(1, 2) { Side::Buy } else { Side::Sell };
        let digits = 1 + rng.below(12) as u32;
        let qty_units = 1 + rng.below(10_u64.pow(6));
        let qty = Dec::new(i128::from(qty_units), digits.clamp(4, 12)).unwrap();
        let tag = format!("prop:{i}");
        let req = if rng.chance(1, 2) {
            OrderRequest::market(&tag, symbol, side, qty)
        } else {
            // limit prices well away from the touch so buys rest and sells rest, with up to 5 decimals
            let off = 1 + rng.below(5000);
            let px_units = match side {
                Side::Buy => base_px * 100_000 - off * 1_000 - rng.below(1_000),
                Side::Sell => base_px * 100_000 + off * 1_000 + rng.below(1_000),
            };
            OrderRequest::limit(&tag, symbol, side, qty, Dec::new(i128::from(px_units), 5).unwrap())
        };
        match rig.adapter.place_order(&req) {
            Ok(PlaceOutcome::Accepted { .. }) => sent += 1,
            Ok(other) => panic!("the exchange refused what the adapter sent: {other:?} for {req:?}"),
            Err(BrokerError::QuantityRoundsToZero { .. } | BrokerError::BelowMinQuantity { .. } | BrokerError::BelowMinCost { .. }) => {
                refused_locally += 1
            }
            Err(e) => panic!("unexpected {e:?} for {req:?}"),
        }
    }
    assert!(sent > 100 && refused_locally > 20, "the property exercised both paths ({sent} sent, {refused_locally} refused)");
    rig.handle.assert_invariants();
}

fn assert_report_matches_exchange(rig: &KrakenRig, r: &OrderReport) {
    let o = rig.handle.order(&r.broker_order_id).unwrap();
    assert_eq!(r.executed_quantity, o.vol_exec());
}

#[test]
fn reports_of_many_orders_agree_with_the_exchanges_books() {
    let rig = KrakenRig::new();
    for i in 0..10 {
        let (t, _) = accepted(rig.adapter.place_order(&buy(&format!("m{i}"), "0.001")).unwrap());
        assert_report_matches_exchange(&rig, &rig.adapter.get_order(&t).unwrap());
    }
    rig.handle.assert_invariants();
}
