//! Reconciliation: every finding code fires when it should and stays quiet at the tolerance boundary.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use broker_adapters::{Dec, OrderKind, OrderReport, OrderStatus, Side};
use chrono::Duration;
use common::*;
use rebalancer_run::recon::{
    reconcile, ExpectedOrder, ReconBaseline, ReconCode, ReconInput, ReconReport, ReconTolerances, ReconVerdict, Severity,
};
use rebalancer_run::view::{BrokerSnapshot, Holding, UnvaluedHolding};

fn holding(symbol: &str, qty: &str, mark: &str) -> Holding {
    let q = d(qty);
    let m = d(mark);
    Holding { symbol: symbol.into(), asset_class: "crypto_spot".into(), quantity: q, mark: Some(m), market_value: q.checked_mul(m).unwrap() }
}

/// A consistent snapshot: equity = cash + holdings (so the cross-check passes unless a test breaks it).
fn snapshot(cash: &str, holdings: Vec<Holding>) -> BrokerSnapshot {
    let derived = holdings.iter().fold(d(cash), |a, h| a.checked_add(h.market_value).unwrap());
    BrokerSnapshot {
        venue: "kraken".into(),
        ccy: "USD".into(),
        equity: derived,
        cash: d(cash),
        holdings,
        unvalued: vec![],
        open_orders: vec![],
        excluded_balances: vec![],
        taken_at: t0(),
        derived_equity: derived,
    }
}

fn report(id: &str, tag: Option<&str>, status: OrderStatus, qty: &str, exec: &str) -> OrderReport {
    OrderReport {
        broker_order_id: id.into(),
        userref: None,
        tag: tag.map(str::to_string),
        symbol: "BTC/USD".into(),
        side: Some(Side::Buy),
        kind: Some(OrderKind::Market),
        status,
        raw_status: "x".into(),
        reason: None,
        quantity: d(qty),
        executed_quantity: d(exec),
        avg_price: Some(d("60000")),
        cost: Some(d(exec).checked_mul(d("60000")).unwrap()),
        fee: Some(Dec::ZERO),
        open_time: None,
        close_time: None,
    }
}

fn run(view: &BrokerSnapshot, baseline: Option<&ReconBaseline>, orders: &[ExpectedOrder], ids: &BTreeSet<String>) -> ReconReport {
    reconcile(&ReconInput { view, now: t0(), known_order_ids: ids, baseline, orders, tolerances: &ReconTolerances::default() })
}

fn quiet(view: &BrokerSnapshot) -> ReconReport {
    run(view, None, &[], &no_ids())
}

fn expected(tag: &str, side: Side, planned: &str, id: Option<&str>, reports: Vec<OrderReport>) -> ExpectedOrder {
    ExpectedOrder {
        tag: tag.into(),
        symbol: "BTC/USD".into(),
        side,
        planned_quantity: d(planned),
        broker_order_id: id.map(str::to_string),
        reports,
        anomalies: vec![],
        expect_exists: true,
    }
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn recon_code_strings_are_pinned_and_unique() {
    let expected = [
        "RECON_FOREIGN_ORDER",
        "RECON_POSITION_DRIFT",
        "RECON_BALANCE_DRIFT",
        "RECON_ORDER_MISSING",
        "RECON_DUPLICATE_FILL",
        "RECON_STALE_VIEW",
        "RECON_EQUITY_MISMATCH",
        "RECON_UNVALUED_HOLDING",
        "RECON_NEGATIVE_CASH",
        "RECON_NO_BASELINE",
        "RECON_OWN_OPEN_ORDER",
    ];
    let actual: Vec<&str> = ReconCode::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(actual, expected);
    assert_eq!(actual.iter().collect::<BTreeSet<_>>().len(), actual.len());
    assert_eq!((ReconVerdict::Ok.as_str(), ReconVerdict::HaltAndAlert.as_str()), ("OK", "HALT_AND_ALERT"));
}

#[test]
fn a_clean_first_run_view_is_ok_with_only_the_no_baseline_note() {
    let r = quiet(&snapshot("5000", vec![holding("BTC/USD", "0.05", "60000")]));
    assert_eq!(r.verdict, ReconVerdict::Ok);
    assert_eq!(r.codes(), ["RECON_NO_BASELINE"]);
    assert!(r.findings.iter().all(|f| f.severity == Severity::Info));
}

// ---------------------------------------------------------------------------------------------------------------
// Foreign orders
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_foreign_open_order_halts_and_our_own_do_not() {
    let mut v = snapshot("5000", vec![]);
    v.open_orders = vec![report("O1", None, OrderStatus::Open, "0.01", "0")];
    let r = quiet(&v);
    assert_eq!(r.verdict, ReconVerdict::HaltAndAlert);
    assert!(r.has(ReconCode::ForeignOrder));
    assert!(r.halt_summary().contains("RECON_FOREIGN_ORDER"), "{}", r.halt_summary());

    // A tag with our prefix is ours.
    v.open_orders = vec![report("O2", Some("rb1:20260921T150000Z:BTCUSD:buy:abcd"), OrderStatus::Open, "0.01", "0")];
    let r = quiet(&v);
    assert_eq!(r.verdict, ReconVerdict::Ok);
    assert!(r.has(ReconCode::OwnOpenOrder) && !r.has(ReconCode::ForeignOrder));

    // A tag that does NOT carry our prefix is foreign even though it has a tag.
    v.open_orders = vec![report("O3", Some("other-app:1"), OrderStatus::Open, "0.01", "0")];
    assert_eq!(quiet(&v).verdict, ReconVerdict::HaltAndAlert);

    // An order without our tag whose id we recorded when placing it is ours (a restart lost the tag table).
    v.open_orders = vec![report("O4", None, OrderStatus::Open, "0.01", "0")];
    let ids: BTreeSet<String> = ["O4".to_string()].into();
    assert_eq!(run(&v, None, &[], &ids).verdict, ReconVerdict::Ok);
}

// ---------------------------------------------------------------------------------------------------------------
// Drift: tolerance boundary is max(1.00, 0.1% of equity)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn cash_drift_is_judged_against_max_of_one_unit_and_a_tenth_of_a_percent_of_equity() {
    // equity 5000 -> limit max(1, 5.000) = 5
    let base = ReconBaseline { cash: d("5000"), holdings: BTreeMap::new(), marks: BTreeMap::new() };
    let within = snapshot("5005", vec![]); // +5.00: at the limit, allowed (strictly greater halts)
    let r = run(&within, Some(&base), &[], &no_ids());
    assert_eq!(r.verdict, ReconVerdict::Ok, "{:?}", r.findings);
    let over = snapshot("5005.01", vec![]);
    assert!(run(&over, Some(&base), &[], &no_ids()).has(ReconCode::BalanceDrift));
    let under = snapshot("4994.99", vec![]);
    let r = run(&under, Some(&base), &[], &no_ids());
    assert_eq!(r.verdict, ReconVerdict::HaltAndAlert);
    assert!(r.has(ReconCode::BalanceDrift));
    // A small account uses the absolute floor: equity 500 -> limit 1.
    let small_base = ReconBaseline { cash: d("500"), holdings: BTreeMap::new(), marks: BTreeMap::new() };
    assert_eq!(run(&snapshot("501", vec![]), Some(&small_base), &[], &no_ids()).verdict, ReconVerdict::Ok);
    assert!(run(&snapshot("501.01", vec![]), Some(&small_base), &[], &no_ids()).has(ReconCode::BalanceDrift));
}

#[test]
fn position_drift_is_valued_at_the_mark_and_uses_the_same_limit() {
    let base = ReconBaseline {
        cash: d("2000"),
        holdings: [("BTC/USD".to_string(), d("0.05"))].into(),
        marks: [("BTC/USD".to_string(), d("60000"))].into(),
    };
    // equity 5000, limit 5: 0.00008 BTC = 4.80 is fine, 0.0001 BTC = 6.00 halts.
    let ok = snapshot("2000", vec![holding("BTC/USD", "0.05008", "60000")]);
    assert_eq!(run(&ok, Some(&base), &[], &no_ids()).verdict, ReconVerdict::Ok);
    let bad = snapshot("2000", vec![holding("BTC/USD", "0.0501", "60000")]);
    let r = run(&bad, Some(&base), &[], &no_ids());
    assert_eq!(r.verdict, ReconVerdict::HaltAndAlert);
    assert!(r.has(ReconCode::PositionDrift), "{:?}", r.findings);
    // An asset that appears out of nowhere is drift (expected zero).
    let extra = snapshot("2000", vec![holding("BTC/USD", "0.05", "60000"), holding("ETH/USD", "1", "3000")]);
    let r = run(&extra, Some(&base), &[], &no_ids());
    assert!(r.findings.iter().any(|f| f.code == ReconCode::PositionDrift && f.symbol.as_deref() == Some("ETH/USD")), "{:?}", r.findings);
    // An asset that vanished is valued at the baseline's mark.
    let gone = snapshot("2000", vec![]);
    assert!(run(&gone, Some(&base), &[], &no_ids()).has(ReconCode::PositionDrift));
}

#[test]
fn drift_without_a_price_to_judge_it_halts() {
    let base = ReconBaseline { cash: d("0"), holdings: [("XYZ/USD".to_string(), d("1"))].into(), marks: BTreeMap::new() };
    let v = snapshot("0", vec![]);
    let r = run(&v, Some(&base), &[], &no_ids());
    assert!(r.has(ReconCode::PositionDrift), "{:?}", r.findings);
}

// ---------------------------------------------------------------------------------------------------------------
// Stale views, equity cross-check, unvalued holdings, negative cash
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_view_older_than_the_limit_or_from_the_future_is_stale() {
    let mut v = snapshot("5000", vec![]);
    let at_now = |secs: i64| {
        let mut w = v.clone();
        w.taken_at = t0() - Duration::seconds(secs);
        run(&w, None, &[], &no_ids())
    };
    assert_eq!(at_now(120).verdict, ReconVerdict::Ok, "exactly at the limit");
    assert!(at_now(121).has(ReconCode::StaleView));
    assert_eq!(at_now(-5).verdict, ReconVerdict::Ok, "5 s of clock skew is tolerated");
    assert!(at_now(-6).has(ReconCode::StaleView));
    v.taken_at = t0();
    assert_eq!(quiet(&v).verdict, ReconVerdict::Ok);
}

#[test]
fn broker_equity_that_disagrees_with_cash_plus_holdings_halts() {
    let mut v = snapshot("5000", vec![holding("BTC/USD", "0.05", "60000")]); // derived 8000
    v.equity = d("8080"); // 1% off: at the limit (limit = max(1, 1% of 8080) = 80.80), gap 80 -> ok
    assert_eq!(quiet(&v).verdict, ReconVerdict::Ok);
    v.equity = d("8090"); // gap 90 > 80.90? no: limit 80.90, gap 90 -> mismatch
    let r = quiet(&v);
    assert!(r.has(ReconCode::EquityMismatch), "{:?}", r.findings);
    assert_eq!(r.verdict, ReconVerdict::HaltAndAlert);
}

#[test]
fn unvalued_holdings_and_negative_cash_halt() {
    let mut v = snapshot("5000", vec![]);
    v.unvalued = vec![UnvaluedHolding { asset: "DOGE".into(), quantity: d("100"), reason: "no price".into() }];
    let r = quiet(&v);
    assert!(r.has(ReconCode::UnvaluedHolding));
    assert_eq!(r.verdict, ReconVerdict::HaltAndAlert);
    let mut v = snapshot("-10", vec![]);
    v.equity = d("-10");
    assert!(quiet(&v).has(ReconCode::NegativeCash));
}

// ---------------------------------------------------------------------------------------------------------------
// Our own orders: missing, duplicate fills
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn an_accepted_order_the_broker_does_not_know_is_missing_and_halts_but_an_unresolved_unknown_only_notes() {
    let v = snapshot("5000", vec![]);
    let accepted = expected("rb1:x", Side::Buy, "0.01", Some("O1"), vec![]);
    let r = run(&v, None, &[accepted], &no_ids());
    assert!(r.has(ReconCode::OrderMissing));
    assert_eq!(r.verdict, ReconVerdict::HaltAndAlert);
    let unknown = expected("rb1:y", Side::Buy, "0.01", None, vec![]);
    let r = run(&v, None, &[unknown], &no_ids());
    assert!(r.has(ReconCode::OrderMissing));
    assert_eq!(r.verdict, ReconVerdict::Ok, "an order that was never placed is not an emergency");
    // Paper / assisted orders do not exist at the broker: never "missing".
    let mut paper = expected("rb1:z", Side::Buy, "0.01", None, vec![]);
    paper.expect_exists = false;
    assert_eq!(run(&v, None, &[paper], &no_ids()).findings.iter().filter(|f| f.code == ReconCode::OrderMissing).count(), 0);
}

#[test]
fn duplicate_fills_are_detected_three_ways() {
    let v = snapshot("5000", vec![]);
    // 1. An adapter anomaly recorded by the pipeline.
    let mut a = expected("rb1:a", Side::Buy, "0.01", Some("O1"), vec![report("O1", None, OrderStatus::Filled, "0.01", "0.01")]);
    a.anomalies = vec!["overfill anomaly: vol_exec 0.02 exceeds vol 0.01".into()];
    assert!(run(&v, None, &[a], &no_ids()).has(ReconCode::DuplicateFill));
    // 2. More executed than planned under one tag.
    let b = expected("rb1:b", Side::Buy, "0.01", Some("O1"), vec![report("O1", None, OrderStatus::Filled, "0.02", "0.02")]);
    assert!(run(&v, None, &[b], &no_ids()).has(ReconCode::DuplicateFill));
    // 3. Two filled orders under one tag (double placement).
    let c = expected(
        "rb1:c",
        Side::Buy,
        "0.01",
        Some("O1"),
        vec![report("O1", None, OrderStatus::PartiallyFilledThenCanceled, "0.01", "0.005"), report("O2", None, OrderStatus::Filled, "0.005", "0.005")],
    );
    // Together they executed exactly the planned 0.01, so only the "two filled orders" rule can see this.
    let r = run(&v, None, &[c], &no_ids());
    assert!(r.has(ReconCode::DuplicateFill), "{:?}", r.findings);
    // A clean single fill and a repeated report of the SAME order are fine.
    let ok = expected(
        "rb1:d",
        Side::Buy,
        "0.01",
        Some("O1"),
        vec![report("O1", None, OrderStatus::Filled, "0.01", "0.01"), report("O1", None, OrderStatus::Filled, "0.01", "0.01")],
    );
    assert_eq!(run(&v, None, &[ok], &no_ids()).verdict, ReconVerdict::Ok);
}

// ---------------------------------------------------------------------------------------------------------------
// Baselines
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_baseline_after_our_fills_moves_cash_and_quantities_by_what_the_broker_reported() {
    let pre = snapshot("5000", vec![holding("ETH/USD", "1", "3000")]);
    let base = ReconBaseline::from_snapshot(&pre);
    assert_eq!(base.cash, d("5000"));
    assert_eq!(base.holdings["ETH/USD"], d("1"));
    assert_eq!(base.marks["ETH/USD"], d("3000"));
    let mut buy = report("O1", None, OrderStatus::Filled, "0.01", "0.01"); // cost 600 at the helper's 60000
    buy.fee = Some(d("1.56"));
    let mut sell = report("O2", None, OrderStatus::Filled, "0.5", "0.5");
    sell.symbol = "ETH/USD".into();
    sell.side = Some(Side::Sell);
    sell.cost = Some(d("1500"));
    sell.fee = Some(d("3.90"));
    let mut sell_order = expected("rb1:s", Side::Sell, "0.5", Some("O2"), vec![sell.clone(), sell]); // a repeated report counts once
    sell_order.symbol = "ETH/USD".into();
    let buy_order = expected("rb1:b", Side::Buy, "0.01", Some("O1"), vec![buy]);
    let after = base.after_orders(&[buy_order, sell_order]).unwrap();
    assert_eq!(after.holdings["BTC/USD"], d("0.01"));
    assert_eq!(after.holdings["ETH/USD"], d("0.5"));
    // 5000 - (600 + 1.56) + (1500 - 3.90) = 5894.54
    assert_eq!(after.cash, d("5894.54"));
    // An order with nothing executed changes nothing.
    let none = expected("rb1:n", Side::Buy, "0.01", Some("O3"), vec![report("O3", None, OrderStatus::Canceled, "0.01", "0")]);
    assert_eq!(base.after_orders(&[none]).unwrap(), base);
}

#[test]
fn the_post_run_view_matching_the_expected_baseline_is_clean_and_a_mismatch_halts() {
    let pre = snapshot("5000", vec![]);
    let base = ReconBaseline::from_snapshot(&pre);
    let buy = expected("rb1:b", Side::Buy, "0.01", Some("O1"), vec![report("O1", None, OrderStatus::Filled, "0.01", "0.01")]);
    let expected_after = base.after_orders(std::slice::from_ref(&buy)).unwrap();
    let good = snapshot("4400", vec![holding("BTC/USD", "0.01", "60000")]);
    let r = run(&good, Some(&expected_after), std::slice::from_ref(&buy), &no_ids());
    assert_eq!(r.verdict, ReconVerdict::Ok, "{:?}", r.findings);
    // The exchange holds LESS than the fill report says (a dropped fill).
    let short = snapshot("5000", vec![]);
    let r = run(&short, Some(&expected_after), &[buy], &no_ids());
    assert_eq!(r.verdict, ReconVerdict::HaltAndAlert);
    assert!(r.has(ReconCode::PositionDrift) && r.has(ReconCode::BalanceDrift));
}
