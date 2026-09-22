//! Exchange-core semantics through the control API and the real adapter: determinism, money
//! conservation under random activity, market moves, equity, glitches and scripting.

use broker_adapters::{BrokerAdapter, Dec, OrderRequest, OrderStatus, PlaceOutcome, Side};
use fake_broker::money::{add, mul, sub};
use fake_broker::rng::SplitMix64;
use fake_broker::testkit::{d, KrakenRig};
use fake_broker::{FakeBroker, FakeBrokerBuilder, FillPolicy, FillStep, ForeignOrder, OrderRule, PairSpec};

// ---------------------------------------------------------------- determinism

fn scripted_session(seed: u64) -> (String, Vec<String>) {
    let broker = FakeBrokerBuilder::standard().seed(seed).build();
    let rig = KrakenRig::with_broker(broker);
    let mut txids = Vec::new();
    for i in 0..5 {
        if let PlaceOutcome::Accepted { broker_order_id, .. } =
            rig.adapter.place_order(&OrderRequest::market(&format!("t{i}"), "BTC/USD", Side::Buy, d("0.001"))).unwrap()
        {
            txids.push(broker_order_id);
        }
        rig.handle.advance_secs(7);
        rig.handle.move_price_pct("BTC/USD", "0.01");
    }
    (rig.handle.dump_log(), txids)
}

#[test]
fn identical_sessions_produce_identical_logs_and_ids_and_the_seed_changes_the_ids() {
    let (log_a, ids_a) = scripted_session(1);
    let (log_b, ids_b) = scripted_session(1);
    assert_eq!(log_a, log_b, "no wall-clock time, no randomness outside the seed");
    assert_eq!(ids_a, ids_b);
    let (_, ids_c) = scripted_session(2);
    assert_ne!(ids_a, ids_c);
    assert!(ids_a.iter().all(|t| t.len() == 19 && t.starts_with('O') && t.as_bytes()[6] == b'-' && t.as_bytes()[12] == b'-'), "{ids_a:?}");
}

// ---------------------------------------------------------------- market controls

#[test]
fn price_quote_spread_and_percentage_moves() {
    let rig = KrakenRig::new();
    let h = &rig.handle;
    assert_eq!(h.quote("BTC/USD"), (d("60000"), d("60000.1"), d("60000")));
    h.set_spread("BTC/USD", "2.5");
    assert_eq!(h.quote("BTC/USD"), (d("60000"), d("60002.5"), d("60000")));
    h.set_price("BTC/USD", "61000");
    assert_eq!(h.quote("BTC/USD"), (d("61000"), d("61002.5"), d("61000")));
    h.set_quote("BTC/USD", "60990", "61010", "61001");
    assert_eq!(h.quote("BTC/USD"), (d("60990"), d("61010"), d("61001")));
    let q = rig.adapter.get_quote("BTC/USD").unwrap();
    assert_eq!((q.bid, q.ask, q.last), (d("60990"), d("61010"), d("61001")));

    h.set_price("ETH/USD", "3000");
    assert_eq!(h.move_price_pct("ETH/USD", "-0.031"), d("2907"), "3000 * 0.969");
    assert_eq!(h.move_price_pct("ETH/USD", "0.10001"), d("3197.73"), "2907 * 1.10001 = 3197.72907, rounded to the 0.01 tick");
}

#[test]
fn a_market_sell_fills_at_the_bid_of_a_wide_book() {
    let rig = KrakenRig::new();
    rig.handle.set_balance("main", "BTC", "1");
    rig.handle.set_quote("BTC/USD", "59900", "60100", "60000");
    let o = match rig.adapter.place_order(&OrderRequest::market("w", "BTC/USD", Side::Sell, d("0.1"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => rig.adapter.get_order(&broker_order_id).unwrap(),
        other => panic!("{other:?}"),
    };
    assert_eq!(o.avg_price, Some(d("59900")));
}

#[test]
fn equity_is_marked_at_last_prices_and_set_equity_moves_usd_cash() {
    let rig = KrakenRig::new();
    let h = &rig.handle;
    h.set_balance("main", "BTC", "1");
    h.set_balance("main", "ETH", "10");
    assert_eq!(h.equity("main"), d("190000"), "100000 + 60000 + 30000");
    h.set_price("BTC/USD", "50000");
    assert_eq!(h.equity("main"), d("180000"));
    assert_eq!(h.equity_in("main", "USD"), h.equity("main"));
    h.set_equity("main", "170000.5");
    assert_eq!(h.equity("main"), d("170000.5"));
    assert_eq!(h.balance("main", "USD"), d("90000.5"), "the difference was moved through USD cash");
    h.assert_invariants();
    // a deposit larger than any loss also works, and a withdrawal past zero cash is refused loudly
    h.set_equity("main", "300000");
    assert_eq!(h.balance("main", "USD"), d("220000"));
}

#[test]
#[should_panic(expected = "negative")]
fn withdrawing_more_cash_than_exists_is_refused_loudly() {
    let rig = KrakenRig::new();
    rig.handle.adjust_balance("main", "USD", "-100000.01");
}

// ---------------------------------------------------------------- rules and scripting

#[test]
fn full_fill_policy_fills_a_resting_limit_at_the_scripted_price() {
    let rig = KrakenRig::new();
    rig.handle.script_orders(OrderRule::next(FillPolicy::full_fill_at("59500.0")));
    let r = match rig.adapter.place_order(&OrderRequest::limit("ff", "BTC/USD", Side::Buy, d("0.01"), d("59900"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => rig.adapter.get_order(&broker_order_id).unwrap(),
        other => panic!("{other:?}"),
    };
    assert_eq!((r.status, r.avg_price), (OrderStatus::Filled, Some(d("59500"))));
    assert_eq!(r.cost, Some(d("595")));
}

#[test]
fn a_no_fill_market_order_stays_open_and_never_fills_by_itself() {
    let rig = KrakenRig::new();
    rig.handle.set_default_fill_policy(FillPolicy::NoFill);
    let id = match rig.adapter.place_order(&OrderRequest::market("nf", "BTC/USD", Side::Buy, d("0.01"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    rig.handle.set_price("BTC/USD", "1000");
    assert_eq!(rig.adapter.get_order(&id).unwrap().status, OrderStatus::Open, "scripted orders ignore the market");
    rig.handle.script_fill(&id, "0.01", Some(d("60000"))).unwrap();
    assert_eq!(rig.adapter.get_order(&id).unwrap().status, OrderStatus::Filled);
    rig.handle.set_default_fill_policy(FillPolicy::Auto);
}

#[test]
fn partial_script_steps_can_carry_absolute_quantities_prices_and_a_remainder() {
    let rig = KrakenRig::new();
    rig.handle.script_orders(OrderRule::next(FillPolicy::partial(vec![
        FillStep::qty("0.002").at("59000.0"),
        FillStep::fraction("0.5"),
        FillStep::remainder(),
    ])));
    let id = match rig.adapter.place_order(&OrderRequest::limit("steps", "BTC/USD", Side::Buy, d("0.01"), d("59900"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    let o = rig.handle.order(&id).unwrap();
    assert_eq!((o.vol_exec(), o.avg_price()), (d("0.002"), Some(d("59000"))));
    rig.handle.apply_next_fill(&id).unwrap(); // 0.5 of the volume = 0.005 at the limit (maker)
    assert_eq!(rig.handle.order(&id).unwrap().vol_exec(), d("0.007"));
    rig.handle.apply_next_fill(&id).unwrap(); // the remaining 0.003
    let o = rig.handle.order(&id).unwrap();
    assert_eq!(o.status, fake_broker::OrderStatus::Closed);
    assert_eq!(o.fills.len(), 3);
    assert_eq!(rig.handle.fills("main").len(), 3);
    assert_eq!(o.cost(), d("597.2"), "0.002*59000 + 0.008*59900");
    rig.handle.assert_invariants();
}

#[test]
#[should_panic(expected = "script error")]
fn a_script_step_larger_than_the_order_is_a_loud_authoring_error() {
    let rig = KrakenRig::new();
    rig.handle.script_orders(OrderRule::next(FillPolicy::partial(vec![FillStep::qty("1")])));
    let _ = rig.adapter.place_order(&OrderRequest::market("bad", "BTC/USD", Side::Buy, d("0.01")));
}

// ---------------------------------------------------------------- fill-report glitches

#[test]
fn fill_report_glitches_distort_reports_but_never_the_books() {
    let rig = KrakenRig::new();
    let h = &rig.handle;
    h.set_default_fill_policy(FillPolicy::NoFill);
    let id = match rig.adapter.place_order(&OrderRequest::limit("g", "BTC/USD", Side::Buy, d("0.01"), d("59000"))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    h.script_fill(&id, "0.002", None).unwrap();
    h.script_fill(&id, "0.003", None).unwrap();
    let report = |rig: &KrakenRig| rig.adapter.get_order(&id).unwrap();
    assert_eq!(report(&rig).executed_quantity, d("0.005"));

    h.drop_fill_report(&id, 0).unwrap();
    let r = report(&rig);
    assert_eq!((r.executed_quantity, r.cost), (d("0.003"), Some(d("177"))));
    assert_eq!(h.balance("main", "BTC"), d("0.005"), "balances unaffected");

    h.restore_fill_report(&id, 0).unwrap();
    h.duplicate_fill_report(&id, 1).unwrap();
    let r = report(&rig);
    assert_eq!(r.executed_quantity, d("0.008"), "0.002 + 2 * 0.003");
    assert_eq!(h.order(&id).unwrap().vol_exec(), d("0.005"));
    assert!(h.drop_fill_report(&id, 9).is_err());
    h.assert_invariants();
}

// ---------------------------------------------------------------- foreign orders and funds

#[test]
fn foreign_orders_reserve_funds_and_are_visible_in_the_control_api() {
    let rig = KrakenRig::new();
    let h = &rig.handle;
    let id = h.add_foreign_order("main", ForeignOrder::limit("BTC/USD", Side::Buy, "1.5", "60000.0").userref(9));
    assert!(h.order(&id).unwrap().foreign);
    assert_eq!(h.available("main", "USD"), d("9766"), "100000 - 1.5 * 60000 * 1.0026");
    // our own order now cannot use the funds
    match rig.adapter.place_order(&OrderRequest::market("ours", "BTC/USD", Side::Buy, d("0.2"))).unwrap() {
        PlaceOutcome::Rejected { errors, .. } => assert_eq!(errors[0].code, "EOrder:Insufficient funds"),
        other => panic!("{other:?}"),
    }
    assert_eq!(h.orders_with_userref("main", 9).len(), 1);
}

#[test]
#[should_panic(expected = "foreign order refused")]
fn an_impossible_foreign_order_is_a_loud_authoring_error() {
    let rig = KrakenRig::new();
    rig.handle.add_foreign_order("main", ForeignOrder::limit("BTC/USD", Side::Sell, "5", "70000.0")); // no BTC to sell
}

// ---------------------------------------------------------------- cross-account isolation

#[test]
fn two_accounts_do_not_see_or_move_each_other() {
    let b_secret = "c2Vjb25kLWFjY291bnQtc2VjcmV0";
    let broker = FakeBroker::builder()
        .pair(PairSpec::btc_usd(), "60000")
        .account(fake_broker::AccountSpec::new("main").balance("USD", "1000"))
        .account(fake_broker::AccountSpec::new("other").key("KEY-B", b_secret).balance("USD", "500"))
        .build();
    let h = broker.handle();
    let id = h.add_foreign_order("other", ForeignOrder::limit("BTC/USD", Side::Buy, "0.001", "50000.0"));
    let rig = KrakenRig::with_broker(broker);
    assert_eq!(rig.adapter.get_balances().unwrap().spot("USD"), d("1000"));
    assert!(rig.adapter.open_orders().unwrap().is_empty());
    h.set_price("BTC/USD", "49000");
    assert_eq!(h.order(&id).unwrap().status, fake_broker::OrderStatus::Closed, "other's order filled");
    assert_eq!(rig.adapter.get_balances().unwrap().spot("BTC"), Dec::ZERO, "and did not touch main");
    h.assert_invariants();
}

// ---------------------------------------------------------------- random activity keeps the books balanced

#[test]
fn random_activity_conserves_money_and_never_breaks_an_invariant() {
    for seed in [1u64, 2, 3, 0xDEAD_BEEF] {
        let rig = KrakenRig::new();
        let h = &rig.handle;
        h.set_balance("main", "BTC", "5");
        h.set_balance("main", "ETH", "50");
        let mut rng = SplitMix64::new(seed);
        let mut live: Vec<String> = Vec::new();
        let mut usd_external = d("100000"); // opening deposit

        for step in 0..300u64 {
            match rng.below(10) {
                0..=2 => {
                    // market order, either side, either pair
                    let (sym, unit) = if rng.chance(1, 2) { ("BTC/USD", "0.0001") } else { ("ETH/USD", "0.002") };
                    let qty = mul(d(unit), Dec::from_i64(1 + rng.below(50) as i64));
                    let side = if rng.chance(1, 2) { Side::Buy } else { Side::Sell };
                    let _ = rig.adapter.place_order(&OrderRequest::market(&format!("r{step}"), sym, side, qty));
                }
                3..=4 => {
                    // resting limit within 3000 of the last price
                    let side = if rng.chance(1, 2) { Side::Buy } else { Side::Sell };
                    let last = h.price("BTC/USD");
                    let delta = Dec::from_i64(rng.below(3000) as i64);
                    let px = match side {
                        Side::Buy => sub(last, delta),
                        Side::Sell => add(last, delta),
                    };
                    let qty = mul(d("0.001"), Dec::from_i64(1 + rng.below(20) as i64));
                    if let Ok(PlaceOutcome::Accepted { broker_order_id, .. }) =
                        rig.adapter.place_order(&OrderRequest::limit(&format!("l{step}"), "BTC/USD", side, qty, px))
                    {
                        live.push(broker_order_id);
                    }
                }
                5 => {
                    let pct = ["-0.02", "-0.01", "0.01", "0.02", "0.005"][rng.below(5) as usize];
                    h.move_price_pct("BTC/USD", pct);
                    h.move_price_pct("ETH/USD", pct);
                }
                6 => {
                    if let Some(id) = live.pop() {
                        let _ = rig.adapter.cancel_order(&id);
                    }
                }
                7 => {
                    // scripted partial fill of a live order
                    if let Some(o) = live.last().and_then(|id| h.order(id)) {
                        if o.status.is_live() && o.remaining() > d("0.0002") {
                            let _ = h.script_fill(&o.txid, "0.0001", None);
                        }
                    }
                }
                8 => {
                    // external movement of USD cash
                    let amount = Dec::from_i64(100 * (1 + rng.below(20) as i64));
                    if rng.chance(1, 2) {
                        h.adjust_balance("main", "USD", amount);
                        usd_external = add(usd_external, amount);
                    } else if h.balance("main", "USD") > amount {
                        h.adjust_balance("main", "USD", mul(amount, Dec::from_i64(-1)));
                        usd_external = sub(usd_external, amount);
                    }
                }
                _ => h.advance_secs(1 + rng.below(30)),
            }
            if let Err(e) = h.check_invariants() {
                panic!("seed {seed} step {step}: {e}
{}", h.dump_log());
            }
        }

        // Independent recomputation of the USD balance from opening cash, external flows and the
        // fill history (not using the exchange's internal ledger).
        let mut usd = usd_external;
        for o in h.orders("main") {
            for f in &o.fills {
                usd = match o.side {
                    Side::Buy => sub(usd, add(f.cost, f.fee)),
                    Side::Sell => add(usd, sub(f.cost, f.fee)),
                };
            }
        }
        assert_eq!(h.balance("main", "USD"), usd, "seed {seed}");
        // and the adapter's reads agree with the exchange's books
        let b = rig.adapter.get_balances().unwrap();
        for asset in ["BTC", "ETH", "USD"] {
            assert_eq!(b.spot(asset), h.balance("main", asset), "seed {seed} {asset}");
        }
        assert!(!h.fills("main").is_empty(), "seed {seed} traded");
    }
}
