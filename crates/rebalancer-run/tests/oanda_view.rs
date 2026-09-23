//! OANDA account view: `oanda_snapshot` over the adapter's fixtures (hand-authored from documentation, not recorded),
//! and `OandaBroker::snapshot_with_margin` against the fake OANDA exchange with the real adapter. Margin-aware, signed
//! positions, account currency, NAV, balance, unrealised P&L, margin used and available; nothing invented.

mod common;

use broker_adapters::oanda::parse::{parse_account_summary, parse_open_positions, parse_pricing};
use broker_adapters::oanda::{AccountSummary, OandaPosition, Pricing};
use broker_adapters::{BrokerAdapter, Dec};
use common::*;
use fake_broker::oanda_rig::OandaRig;
use rebalancer_run::broker::{Broker, OandaBroker, SnapshotError};
use rebalancer_run::view::{oanda_snapshot, OandaViewInput, ViewError};

macro_rules! fx {
    ($name:literal) => {
        include_str!(concat!("../../broker-adapters/tests/fixtures/oanda/", $name))
    };
}

fn account(name: &str) -> AccountSummary {
    parse_account_summary(match name {
        "ok" => fx!("account_summary_ok.json"),
        "hedging" => fx!("account_summary_hedging.json"),
        "eur" => fx!("account_summary_eur.json"),
        other => panic!("{other}"),
    })
    .unwrap()
}

fn positions() -> Vec<OandaPosition> {
    parse_open_positions(fx!("open_positions_ok.json")).unwrap()
}

fn pricing() -> Pricing {
    parse_pricing(fx!("pricing_ok.json")).unwrap()
}

fn snap(a: &AccountSummary, p: &[OandaPosition], pr: &Pricing) -> Result<rebalancer_run::view::OandaSnapshot, ViewError> {
    oanda_snapshot(&OandaViewInput { account: a, positions: p, open_orders: Vec::new(), pricing: pr, asset_class: "fx_spot" }, t0())
}

fn pos(instrument: &str, long: &str, short: &str, upl: &str) -> OandaPosition {
    OandaPosition {
        instrument: instrument.to_string(),
        long_units: d(long),
        short_units: d(short),
        long_average_price: None,
        short_average_price: None,
        unrealized_pl: d(upl),
        margin_used: None,
    }
}

// ---------------------------------------------------------------------------------------------------------------
// From fixtures
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_snapshot_reports_broker_nav_balance_signed_holdings_and_the_margin_figures() {
    let s = snap(&account("ok"), &positions(), &pricing()).unwrap();
    let b = &s.snapshot;
    assert_eq!((b.venue.as_str(), b.ccy.as_str()), ("oanda", "USD"));
    assert_eq!(b.equity, d("100250.5000"), "NAV as the broker reports it");
    assert_eq!(b.cash, d("100000.0000"), "the realised balance, not free cash");
    assert_eq!(b.taken_at, t0());
    assert!(b.unvalued.is_empty() && b.excluded_balances.is_empty());
    assert_eq!(b.holdings.len(), 2);
    let eur = b.holding("EUR/USD").unwrap();
    assert_eq!(eur.quantity, d("10000"));
    assert_eq!(eur.mark, Some(d("1.10050")), "mid of 1.10048 / 1.10052");
    assert_eq!(eur.market_value, d("11005.0"), "10000 * 1.10050 * 1 (quote currency is the account currency)");
    assert_eq!(eur.asset_class, "fx_spot");
    let gbp = b.holding("gbp/usd").unwrap();
    assert_eq!(gbp.quantity, d("-5000"), "a short is negative");
    assert_eq!(gbp.mark, Some(d("1.27000")));
    assert_eq!(gbp.market_value, d("-6350.0"), "signed notional");
    // our cross-check is all broker numbers: balance + sum(position unrealizedPL) = 100000 + 290.5 - 40
    assert_eq!(b.derived_equity, d("100250.5"));
    assert_eq!(b.derived_equity, b.equity);
    // margin figures, exactly as reported
    let m = &s.margin;
    assert_eq!(m.currency, "USD");
    assert_eq!((m.nav, m.balance, m.unrealized_pl), (d("100250.5"), d("100000"), d("250.5")));
    assert_eq!((m.margin_used, m.margin_available), (d("1100"), d("99150.5")));
    assert_eq!(m.position_value, Some(d("55000")));
    assert_eq!(m.margin_closeout_percent, Some(d("0.00549")));
    assert_eq!((m.open_position_count, m.pending_order_count), (Some(2), Some(1)));
}

#[test]
fn holdings_are_sorted_and_flow_into_the_guard_view_with_their_signs() {
    let s = snap(&account("ok"), &positions(), &pricing()).unwrap().snapshot;
    let symbols: Vec<&str> = s.holdings.iter().map(|h| h.symbol.as_str()).collect();
    assert_eq!(symbols, ["EUR/USD", "GBP/USD"]);
    let v = s.account_view("acct-1", false, t0());
    assert_eq!((v.ccy.as_str(), v.equity, v.cash), ("USD", d("100250.5"), d("100000")));
    let gbp = v.positions.iter().find(|p| p.symbol == "GBP/USD").unwrap();
    assert_eq!((gbp.venue.as_str(), gbp.quantity, gbp.market_value), ("oanda", d("-5000"), d("-6350.0")));
    assert_eq!(s.equity_snapshot().equity(), d("100250.5"), "risk uses the broker's NAV");
}

#[test]
fn a_quote_currency_other_than_the_account_currency_uses_the_brokers_home_conversion() {
    let p = vec![pos("USD_JPY", "2000", "0", "3.0000")];
    let s = snap(&account("ok"), &p, &pricing()).unwrap().snapshot;
    let h = s.holding("USD/JPY").unwrap();
    assert_eq!(h.mark, Some(d("148.505")));
    // 2000 * 148.505 * 0.006736 (the fixture's JPY positionValue factor) = 2000.65936, about 2000 USD of notional
    assert_eq!(h.market_value, d("2000.65936"));
    assert_eq!(s.derived_equity, d("100003"), "balance 100000 + position unrealizedPL 3");
}

#[test]
fn a_position_that_cannot_be_priced_or_converted_is_unvalued_never_guessed() {
    // no price for GBP_USD in this pricing response
    let only_eur = parse_pricing(&fx!("pricing_ok.json").replace("GBP_USD", "AUD_USD")).unwrap();
    let s = snap(&account("ok"), &positions(), &only_eur).unwrap().snapshot;
    assert_eq!(s.holdings.len(), 1);
    assert_eq!(s.unvalued.len(), 1);
    assert_eq!((s.unvalued[0].asset.as_str(), s.unvalued[0].quantity), ("GBP/USD", d("-5000")));
    assert!(s.unvalued[0].reason.contains("no usable price"), "{}", s.unvalued[0].reason);
    // the market is closed: both sides empty, so the position is valued at the broker's own closeout prices ...
    let closed = parse_pricing(fx!("pricing_closed.json")).unwrap();
    let s = snap(&account("ok"), &[pos("EUR_USD", "1000", "0", "1")], &closed).unwrap().snapshot;
    assert!(s.unvalued.is_empty());
    assert_eq!(s.holding("EUR/USD").unwrap().mark, Some(d("1.10050")), "mid of closeoutBid 1.10048 / closeoutAsk 1.10052");
    // ... and with no closeout prices either it is unvalued
    let no_closeout = fx!("pricing_closed.json").replace("closeoutBid", "x1").replace("closeoutAsk", "x2");
    let s = snap(&account("ok"), &[pos("EUR_USD", "1000", "0", "1")], &parse_pricing(&no_closeout).unwrap()).unwrap().snapshot;
    assert!(s.holdings.is_empty() && s.unvalued.len() == 1);
    // crossed
    let crossed = parse_pricing(fx!("pricing_crossed.json")).unwrap();
    let s = snap(&account("ok"), &[pos("EUR_USD", "1000", "0", "1")], &crossed).unwrap().snapshot;
    assert!(s.holdings.is_empty() && s.unvalued.len() == 1);
    // JPY quote but no JPY conversion in the response
    let no_jpy = fx!("pricing_ok.json").replace("\"currency\": \"JPY\"", "\"currency\": \"CHF\"");
    let s = snap(&account("ok"), &[pos("USD_JPY", "1000", "0", "1")], &parse_pricing(&no_jpy).unwrap()).unwrap().snapshot;
    assert!(s.holdings.is_empty());
    assert!(s.unvalued[0].reason.contains("no conversion"), "{}", s.unvalued[0].reason);
    // the equity cross-check still counts the position's P&L (it is a broker number)
    assert_eq!(s.derived_equity, d("100001"));
}

#[test]
fn a_hedging_account_or_a_hedged_position_is_refused() {
    assert!(matches!(snap(&account("hedging"), &positions(), &pricing()), Err(ViewError::AccountBlocked(m)) if m.contains("hedging")));
    let hedged = vec![pos("EUR_USD", "1000", "-400", "5")];
    assert!(matches!(snap(&account("ok"), &hedged, &pricing()), Err(ViewError::AccountBlocked(m)) if m.contains("hedged")));
}

#[test]
fn a_flat_account_has_no_holdings_and_a_zero_net_row_is_ignored() {
    let s = snap(&account("ok"), &[], &parse_pricing(fx!("pricing_missing_instrument.json")).unwrap()).unwrap().snapshot;
    assert!(s.holdings.is_empty() && s.unvalued.is_empty());
    assert_eq!(s.derived_equity, d("100000"));
    let zero = vec![pos("EUR_USD", "0", "0", "0")];
    let s = snap(&account("ok"), &zero, &pricing()).unwrap().snapshot;
    assert!(s.holdings.is_empty());
}

#[test]
fn the_account_currency_comes_from_the_broker_and_is_used_for_conversion() {
    // A EUR account: EUR_USD's quote (USD) is NOT the account currency, so USD needs a conversion the response lacks
    // (pricing_ok has USD -> 1.0, which the fixture defines as a USD->USD factor): it converts with that factor.
    let s = snap(&account("eur"), &[pos("EUR_USD", "1000", "0", "1")], &pricing()).unwrap().snapshot;
    assert_eq!(s.ccy, "EUR");
    assert_eq!(s.holding("EUR/USD").unwrap().market_value, d("1100.5000"), "1000 * 1.1005 * conversion(USD)=1.0");
    // and without a USD conversion at all, it is unvalued
    let no_usd = parse_pricing(&fx!("pricing_ok.json").replace("\"currency\": \"USD\"", "\"currency\": \"CHF\"")).unwrap();
    let s = snap(&account("eur"), &[pos("EUR_USD", "1000", "0", "1")], &no_usd).unwrap().snapshot;
    assert!(s.holdings.is_empty() && s.unvalued.len() == 1);
}

// ---------------------------------------------------------------------------------------------------------------
// Against the fake exchange with the real adapter
// ---------------------------------------------------------------------------------------------------------------

fn rig() -> OandaRig {
    OandaRig::with_config(|c| c.with_own_tag_prefix("rb1:").unwrap())
}

fn place(rig: &OandaRig, tag: &str, sym: &str, side: broker_adapters::Side, units: &str) {
    use broker_adapters::{BrokerAdapter, OrderRequest};
    rig.adapter.place_order(&OrderRequest::market(tag, sym, side, d(units))).unwrap();
}

fn round4(x: Dec) -> Dec {
    x.round_dp(4, broker_adapters::decimal::Rounding::HalfUp).unwrap()
}

#[test]
fn the_broker_wrapper_snapshot_agrees_with_the_exchange_books() {
    use broker_adapters::Side;
    let rig = rig();
    place(&rig, "rb1:a", "EUR/USD", Side::Buy, "10000");
    place(&rig, "rb1:b", "GBP/USD", Side::Sell, "5000");
    place(&rig, "rb1:c", "USD/JPY", Side::Buy, "2000");
    rig.handle.set_price("EUR_USD", "1.10248", "1.10252");
    let now = t0();
    let broker = OandaBroker::fx(&rig.adapter);
    assert_eq!(broker.venue(), "oanda");
    let o = broker.snapshot_with_margin(now).unwrap();
    let s = &o.snapshot;
    assert_eq!((s.venue.as_str(), s.ccy.as_str()), ("oanda", "USD"));
    assert_eq!(s.equity, round4(rig.handle.nav()));
    assert_eq!(s.cash, round4(rig.handle.balance()));
    assert_eq!(o.margin.margin_used, round4(rig.handle.margin_used()));
    assert_eq!(o.margin.unrealized_pl, round4(rig.handle.unrealized_pl()));
    assert_eq!(o.margin.margin_available, round4(rig.handle.nav().checked_add(Dec::new(-rig.handle.margin_used().units(), rig.handle.margin_used().scale()).unwrap()).unwrap()));
    assert_eq!(s.quantity_of("EUR/USD"), d("10000"));
    assert_eq!(s.quantity_of("GBP/USD"), d("-5000"));
    assert_eq!(s.quantity_of("USD/JPY"), d("2000"));
    // the EUR holding is marked at the new mid and valued at signed notional
    assert_eq!(s.holding("EUR/USD").unwrap().market_value, d("11025.0"), "10000 * 1.10250");
    assert!(s.holding("GBP/USD").unwrap().market_value.is_negative());
    // JPY leg: about 2000 USD of notional (factor is floored to 6 dp by the fake)
    let jpy = s.holding("USD/JPY").unwrap().market_value;
    assert!(jpy > d("1999") && jpy < d("2001"), "{jpy}");
    // the cross-check: balance + position P&L is within a cent of NAV (rounding of the broker's own 4 dp figures)
    let gap = s.equity.checked_add(Dec::new(-s.derived_equity.units(), s.derived_equity.scale()).unwrap()).unwrap();
    assert!(gap < d("0.01") && gap > d("-0.01"), "{gap}");
    // the trait method returns the same snapshot
    assert_eq!(broker.snapshot(now).unwrap(), o.snapshot);
    rig.handle.assert_invariants();
}

#[test]
fn a_flat_account_snapshots_without_asking_for_prices() {
    let rig = rig();
    let o = OandaBroker::fx(&rig.adapter).snapshot_with_margin(t0()).unwrap();
    assert!(o.snapshot.holdings.is_empty());
    assert_eq!((o.snapshot.equity, o.snapshot.cash, o.snapshot.derived_equity), (d("100000"), d("100000"), d("100000")));
    assert_eq!((o.margin.margin_used, o.margin.margin_available), (d("0"), d("100000")));
    assert!(rig.handle.requests().iter().all(|r| !r.path.contains("/pricing")), "no pricing request for a flat account");
}

#[test]
fn open_orders_appear_in_the_snapshot_with_ours_tagged_and_foreign_untagged() {
    use broker_adapters::{BrokerAdapter, OrderRequest, Side};
    let rig = rig();
    rig.adapter.place_order(&OrderRequest::limit("rb1:lim", "EUR/USD", Side::Buy, d("100"), d("1.05000"))).unwrap();
    rig.handle.add_foreign_limit("GBP_USD", "-300", "1.40000");
    let s = OandaBroker::fx(&rig.adapter).snapshot(t0()).unwrap();
    assert_eq!(s.open_orders.len(), 2);
    assert_eq!(s.open_orders.iter().filter(|o| o.tag.as_deref().is_some_and(|t| t.starts_with("rb1:"))).count(), 1);
    assert_eq!(s.open_orders.iter().filter(|o| o.tag.is_none()).count(), 1);
}

#[test]
fn snapshot_failures_are_typed() {
    use broker_adapters::BrokerError;
    use broker_adapters::transport::TransportError;
    use fake_broker::Fault;
    let rig = rig();
    rig.handle.set_hedging(true);
    assert!(matches!(OandaBroker::fx(&rig.adapter).snapshot(t0()), Err(SnapshotError::View(ViewError::AccountBlocked(_)))));
    rig.handle.set_hedging(false);
    rig.handle.inject_fault(Fault::timeout().on_path(&rig.handle.path("/openPositions")));
    assert!(matches!(OandaBroker::fx(&rig.adapter).snapshot(t0()), Err(SnapshotError::Broker(BrokerError::Transport(TransportError::Timeout)))));
    place(&rig, "rb1:x", "EUR/USD", broker_adapters::Side::Buy, "1000");
    rig.handle.inject_fault(Fault::http(503).on_path(&rig.handle.path("/pricing")));
    assert!(matches!(OandaBroker::fx(&rig.adapter).snapshot(t0()), Err(SnapshotError::Broker(BrokerError::Http(503)))));
    assert_eq!(SnapshotError::View(ViewError::AccountBlocked("x".into())).code(), "VIEW_ACCOUNT_BLOCKED");
}

// ---------------------------------------------------------------- flat previously-traded entries (RECORDED, 2026-09-23)

#[test]
fn a_flat_previously_traded_position_record_is_not_a_holding_in_the_snapshot() {
    // RECORDED: GET /positions/EUR_USD for an instrument traded before and flat now is a record with both sides at zero
    // units; the recorded summary and pricing are used too. The record is handed to the snapshot as-is (a consumer that
    // fed it every entry of GET /positions would do the same).
    use broker_adapters::oanda::parse::parse_single_position;
    let flat = parse_single_position(include_str!("../../broker-adapters/tests/fixtures/oanda/real/oanda_smoke__position_flat_previously_traded.json")).unwrap();
    assert!(flat.is_flat());
    let acct = parse_account_summary(include_str!("../../broker-adapters/tests/fixtures/oanda/real/oanda_smoke__account_summary.json")).unwrap();
    let pr = parse_pricing(include_str!("../../broker-adapters/tests/fixtures/oanda/real/oanda_smoke__pricing_home_conversions.json")).unwrap();
    let s = snap(&acct, &[flat], &pr).unwrap();
    assert!(s.snapshot.holdings.is_empty() && s.snapshot.unvalued.is_empty(), "a flat record is neither a holding nor an unvalued one");
    assert_eq!(s.snapshot.cash, Dec::parse("99999.9917").unwrap());
    assert_eq!(s.snapshot.derived_equity, s.snapshot.equity);
}

#[test]
fn the_broker_snapshot_after_a_position_was_opened_and_closed_has_no_holdings() {
    let rig = OandaRig::new();
    rig.adapter.place_order(&broker_adapters::OrderRequest::market("v:in", "EUR/USD", broker_adapters::Side::Buy, Dec::parse("500").unwrap())).unwrap();
    rig.adapter.close_position("EUR_USD", "v:out").unwrap();
    let b = OandaBroker::fx(&rig.adapter);
    let s = b.snapshot_with_margin(t0()).unwrap();
    assert!(s.snapshot.holdings.is_empty() && s.snapshot.unvalued.is_empty());
}
