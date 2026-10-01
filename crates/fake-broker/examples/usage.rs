//! The intended usage pattern for the future rebalancer's tests: a real `KrakenAdapter` on a
//! fake exchange; place an order, script what the exchange does with it, read the books, and
//! check the event log. Run with `cargo run -p fake-broker --example usage`.

use broker_adapters::{BrokerAdapter, Dec, OrderRequest, PlaceOutcome, Side};
use fake_broker::scenarios;
use fake_broker::testkit::{d, KrakenRig};
use fake_broker::{FillPolicy, FillStep, OrderRule};

fn main() {
    // 1. A fake Kraken (BTC/USD 60000, ETH/USD 3000, account "main" with 100000 USD) plus the
    //    real adapter wired to it through HttpTransport. The adapter's nonces follow the fake's
    //    manual clock, so nothing here depends on wall-clock time.
    let rig = KrakenRig::new();

    // 2. Script the exchange BEFORE placing: the next order will fill in two pieces.
    rig.handle.script_orders(OrderRule::next(FillPolicy::partial(vec![
        FillStep::fraction("0.4"),
        FillStep::remainder(),
    ])));

    // 3. Place an order through the adapter, exactly as the rebalancer will.
    let req = OrderRequest::limit("run-2026-10-01:BTC/USD:buy", "BTC/USD", Side::Buy, d("0.01"), d("60500"));
    let txid = match rig.adapter.place_order(&req).expect("adapter call") {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("expected Accepted, got {other:?}"),
    };

    // 4. Read it back: 40 percent has filled, the rest is open.
    let report = rig.adapter.get_order(&txid).unwrap();
    println!("after placement: {:?}, executed {} of {}", report.status, report.executed_quantity, report.quantity);

    // 5. Script the rest of the fill, move the market, and read balances.
    rig.handle.apply_next_fill(&txid).expect("second scripted fill");
    let report = rig.adapter.get_order(&txid).unwrap();
    println!("after second fill: {:?}, avg price {:?}, fee {:?}", report.status, report.avg_price, report.fee);

    let balances = rig.adapter.get_balances().unwrap();
    println!("balances: BTC {} USD {}", balances.spot("BTC"), balances.spot("USD"));
    assert_eq!(balances.spot("BTC"), d("0.01"));

    // 6. Break something on purpose: the next order is placed but its response is lost.
    scenarios::order_placed_response_lost(&rig.handle);
    let lost = OrderRequest::market("run-2026-10-01:ETH/USD:buy", "ETH/USD", Side::Buy, d("0.5"));
    match rig.adapter.place_order(&lost).unwrap() {
        PlaceOutcome::UnknownOutcome { reason, .. } => println!("unknown outcome ({reason}); reconciling by tag"),
        other => panic!("{other:?}"),
    }
    let found = rig.adapter.find_orders_by_tag(&lost.tag).unwrap();
    println!("found {} order(s) with that tag: status {:?}", found.len(), found[0].status);

    // 7. The event log and the exchange's own books are there for assertions.
    let add_orders = rig.handle.applied_requests_to("/0/private/AddOrder").len();
    println!("AddOrder requests that reached the exchange: {add_orders}");
    assert_eq!(add_orders, 2);
    assert_eq!(rig.handle.balance("main", "ETH"), d("0.5"));
    rig.handle.assert_invariants(); // balances == deposits + fills, no negative balances
    assert!(rig.handle.equity("main") > Dec::ZERO);
    println!("\n--- event log ---\n{}", rig.handle.dump_log());
}
