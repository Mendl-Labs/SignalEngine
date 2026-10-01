//! Account-view builders: equity definitions per venue, against the fake Kraken exchange (real adapter) and
//! against the Alpaca fixtures.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::parse::{parse_account, parse_orders, parse_positions};
use broker_adapters::alpaca::{AlpacaAdapter, AlpacaConfig, AlpacaCredentials, Environment};
use broker_adapters::kraken::parse::TradeBalance;
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpResponse, TransportError};
use broker_adapters::{BalanceEntry, BalanceKind, Balances};
use common::*;
use rebalancer_run::broker::{AlpacaBroker, Broker, SnapshotError};
use rebalancer_run::view::{alpaca_snapshot, kraken_snapshot, KrakenViewInput, ViewError};

macro_rules! alpaca_fixture {
    ($name:literal) => {
        include_str!(concat!("../../broker-adapters/tests/fixtures/alpaca/", $name))
    };
}

fn spot(raw: &str, asset: &str, amount: &str) -> BalanceEntry {
    BalanceEntry { raw_asset: raw.into(), asset: asset.into(), amount: d(amount), kind: BalanceKind::Spot }
}

fn earn(raw: &str, asset: &str, amount: &str) -> BalanceEntry {
    BalanceEntry { raw_asset: raw.into(), asset: asset.into(), amount: d(amount), kind: BalanceKind::Earn }
}

fn tb(equity: Option<&str>, eb: Option<&str>) -> TradeBalance {
    TradeBalance { equity: equity.map(d), equivalent_balance: eb.map(d), ..TradeBalance::default() }
}

// ---------------------------------------------------------------------------------------------------------------
// Kraken (real adapter against the fake exchange)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_kraken_snapshot_reads_broker_equity_cash_and_marks_holdings_at_the_last_price() {
    let env = Env::with_usd("5000");
    env.hold("BTC", "0.05");
    env.hold("ETH", "1.5");
    let s = env.snapshot();
    assert_eq!((s.venue.as_str(), s.ccy.as_str()), ("kraken", "USD"));
    assert_eq!(s.cash, d("5000"));
    assert_eq!(s.equity, d("12500")); // 5000 + 0.05*60000 + 1.5*3000, as the broker (TradeBalance) reports it
    assert_eq!(s.derived_equity, d("12500"), "our arithmetic over the broker's numbers agrees");
    let btc = s.holding("BTC/USD").unwrap();
    assert_eq!((btc.quantity, btc.mark, btc.market_value), (d("0.05"), Some(d("60000")), d("3000")));
    assert_eq!(btc.asset_class, "crypto_spot");
    assert_eq!(s.holding("eth/usd").unwrap().market_value, d("4500"));
    assert!(s.unvalued.is_empty() && s.open_orders.is_empty());
    assert_eq!(s.taken_at, env.clock_now());
}

#[test]
fn the_snapshot_equity_is_the_brokers_number_not_our_arithmetic() {
    // The exchange's equity moves with the last price; a held asset re-marks the account.
    let env = Env::with_usd("5000");
    env.hold("BTC", "0.1");
    assert_eq!(env.snapshot().equity, d("11000"));
    env.rig.handle.set_price("BTC/USD", "30000");
    let s = env.snapshot();
    assert_eq!(s.equity, d("8000"));
    let e = s.equity_snapshot();
    assert_eq!((e.equity(), e.ccy(), e.venue()), (d("8000"), "USD", "kraken"));
}

#[test]
fn account_view_carries_the_broker_numbers_to_the_guard_and_planner() {
    let env = Env::with_usd("5000");
    env.hold("BTC", "0.05");
    let s = env.snapshot();
    let v = s.account_view("acct-1", true, env.clock_now());
    assert_eq!((v.account_id.as_str(), v.ccy.as_str(), v.equity, v.cash, v.halted), ("acct-1", "USD", d("8000"), d("5000"), true));
    assert_eq!(v.positions.len(), 1);
    let p = &v.positions[0];
    assert_eq!((p.symbol.as_str(), p.venue.as_str(), p.asset_class.as_str(), p.quantity, p.market_value), ("BTC/USD", "kraken", "crypto_spot", d("0.05"), d("3000")));
}

#[test]
fn foreign_open_orders_appear_in_the_snapshot_untagged() {
    let env = Env::with_usd("5000");
    let id = fake_broker::scenarios::foreign_order_appears(&env.rig.handle, ACCOUNT, "BTC/USD", broker_adapters::Side::Buy, "0.01", "50000");
    let s = env.snapshot();
    assert_eq!(s.open_orders.len(), 1);
    assert_eq!((s.open_orders[0].broker_order_id.as_str(), s.open_orders[0].tag.clone()), (id.as_str(), None));
}

#[test]
fn the_kraken_builder_falls_back_to_the_equivalent_balance_and_refuses_when_neither_exists() {
    let balances = Balances { entries: vec![spot("ZUSD", "USD", "100")] };
    let marks = BTreeMap::new();
    let build = |t: &TradeBalance| {
        kraken_snapshot(
            &KrakenViewInput { balances: &balances, trade_balance: t, open_orders: vec![], marks: &marks, quote_asset: "USD", asset_class: "crypto_spot" },
            t0(),
        )
    };
    assert_eq!(build(&tb(Some("101"), Some("99"))).unwrap().equity, d("101"), "equity (e) wins");
    assert_eq!(build(&tb(None, Some("99"))).unwrap().equity, d("99"), "eb is the fallback");
    let e = build(&tb(None, None)).unwrap_err();
    assert_eq!(e, ViewError::EquityMissing);
    assert_eq!(e.code(), "VIEW_EQUITY_MISSING");
}

#[test]
fn earn_balances_and_unpriceable_assets_are_kept_out_and_listed() {
    let balances = Balances {
        entries: vec![spot("ZUSD", "USD", "1000"), spot("XXBT", "BTC", "0.01"), spot("XXDG", "DOGE", "5000"), earn("XBT.M", "BTC", "0.5"), spot("ZEUR", "EUR", "10")],
    };
    let marks: BTreeMap<String, broker_adapters::Dec> = [("BTC/USD".to_string(), d("60000"))].into();
    let s = kraken_snapshot(
        &KrakenViewInput { balances: &balances, trade_balance: &tb(Some("2000"), None), open_orders: vec![], marks: &marks, quote_asset: "USD", asset_class: "crypto_spot" },
        t0(),
    )
    .unwrap();
    assert_eq!(s.cash, d("1000"));
    assert_eq!(s.holdings.len(), 1);
    assert_eq!(s.holdings[0].symbol, "BTC/USD");
    assert_eq!(s.holdings[0].quantity, d("0.01"), "the staked 0.5 BTC is not part of the spot holding");
    let unvalued: Vec<&str> = s.unvalued.iter().map(|u| u.asset.as_str()).collect();
    assert_eq!(unvalued, ["DOGE", "EUR"]);
    assert_eq!(s.excluded_balances.len(), 1);
    assert_eq!(s.derived_equity, d("1600"), "cash 1000 + 0.01 BTC at 60000; the broker says 2000");
}

#[test]
fn zero_balances_are_ignored_and_duplicate_spot_rows_add_up() {
    let balances = Balances { entries: vec![spot("ZUSD", "USD", "10"), spot("XXBT", "BTC", "0"), spot("XBT", "BTC", "0.02"), spot("XXBT", "BTC", "0.03")] };
    let marks: BTreeMap<String, broker_adapters::Dec> = [("BTC/USD".to_string(), d("100"))].into();
    let s = kraken_snapshot(
        &KrakenViewInput { balances: &balances, trade_balance: &tb(Some("15"), None), open_orders: vec![], marks: &marks, quote_asset: "usd", asset_class: "crypto_spot" },
        t0(),
    )
    .unwrap();
    assert_eq!(s.holdings[0].quantity, d("0.05"));
    assert_eq!(s.ccy, "USD");
}

// ---------------------------------------------------------------------------------------------------------------
// Alpaca (recorded-style fixtures)
// ---------------------------------------------------------------------------------------------------------------

fn etf(_: &str) -> String {
    "us_etf".to_string()
}

#[test]
fn an_alpaca_snapshot_uses_account_equity_and_cash_and_the_brokers_position_values() {
    let account = parse_account(alpaca_fixture!("account_ok.json")).unwrap();
    let positions = parse_positions(alpaca_fixture!("positions_ok.json")).unwrap();
    let orders = parse_orders(alpaca_fixture!("orders_empty.json"), Some("rb1:")).unwrap();
    let s = alpaca_snapshot(&account, &positions, orders, &etf, t0()).unwrap();
    assert_eq!((s.venue.as_str(), s.ccy.as_str()), ("alpaca", "USD"));
    assert_eq!((s.equity, s.cash), (d("100210.55"), d("52840.17")));
    assert_eq!(s.holdings.len(), positions.len());
    let spy = s.holding("SPY").unwrap();
    assert_eq!((spy.quantity, spy.market_value, spy.mark, spy.asset_class.as_str()), (d("12.345678901"), d("6321.55"), Some(d("512.10")), "us_etf"));
    let sum = s.holdings.iter().fold(s.cash, |a, h| a.checked_add(h.market_value).unwrap());
    assert_eq!(s.derived_equity, sum);
    assert!(s.holdings.windows(2).all(|w| w[0].symbol <= w[1].symbol), "holdings are sorted");
}

#[test]
fn an_alpaca_short_position_has_a_negative_quantity_and_value() {
    let account = parse_account(alpaca_fixture!("account_ok.json")).unwrap();
    let positions = parse_positions(alpaca_fixture!("positions_short.json")).unwrap();
    let s = alpaca_snapshot(&account, &positions, vec![], &etf, t0()).unwrap();
    let dbc = s.holding("DBC").unwrap();
    assert_eq!((dbc.quantity, dbc.market_value), (d("-10"), d("-221.00")));
    assert_eq!(s.account_view("a", false, t0()).positions[0].quantity, d("-10"));
}

#[test]
fn blocked_or_inactive_alpaca_accounts_are_refused() {
    for f in [alpaca_fixture!("account_trading_blocked.json"), alpaca_fixture!("account_account_blocked.json"), alpaca_fixture!("account_not_active.json")] {
        let account = parse_account(f).unwrap();
        let e = alpaca_snapshot(&account, &[], vec![], &etf, t0()).unwrap_err();
        assert!(matches!(e, ViewError::AccountBlocked(_)), "{e:?}");
        assert_eq!(e.code(), "VIEW_ACCOUNT_BLOCKED");
    }
}

#[test]
fn open_orders_without_our_prefix_are_untagged_foreign_orders_in_the_alpaca_view() {
    let account = parse_account(alpaca_fixture!("account_ok.json")).unwrap();
    let orders = parse_orders(alpaca_fixture!("orders_open.json"), Some("rb1:")).unwrap();
    assert!(!orders.is_empty());
    let s = alpaca_snapshot(&account, &[], orders, &etf, t0()).unwrap();
    assert!(s.open_orders.iter().all(|o| o.tag.is_none()), "no fixture order carries the rb1: prefix");
}

#[test]
fn the_alpaca_broker_reads_account_positions_and_open_orders_in_that_order() {
    let t = Arc::new(FakeTransport::new());
    t.set_handler(|req| {
        let path = req.url.strip_prefix(PAPER_BASE_URL).unwrap_or(&req.url);
        let body = if path.starts_with("/v2/account") {
            alpaca_fixture!("account_ok.json")
        } else if path.starts_with("/v2/positions") {
            alpaca_fixture!("positions_ok.json")
        } else if path.starts_with("/v2/orders") {
            alpaca_fixture!("orders_empty.json")
        } else {
            return Err(TransportError::Io(format!("unexpected request {path}")));
        };
        Ok(HttpResponse { status: 200, body: body.to_string() })
    });
    let cfg = AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).unwrap();
    let creds = AlpacaCredentials::new(Environment::Paper, "PKTESTFIXTUREKEY0001", "unit-test-secret-not-a-real-key-9f3a").unwrap();
    let adapter = AlpacaAdapter::new(cfg, creds, t.clone()).unwrap();
    let broker = AlpacaBroker::us_etf(&adapter);
    let s = broker.snapshot(t0()).unwrap();
    assert_eq!(s.equity, d("100210.55"));
    let paths: Vec<String> = t.requests().iter().map(|r| r.url.strip_prefix(PAPER_BASE_URL).unwrap().split('?').next().unwrap().to_string()).collect();
    assert_eq!(paths, ["/v2/account", "/v2/positions", "/v2/orders"]);
    // A blocked account surfaces as a coded snapshot error, not a panic and not a stale number.
    let t2 = Arc::new(FakeTransport::new());
    t2.set_handler(|req| {
        let body = if req.url.contains("/v2/account") {
            alpaca_fixture!("account_trading_blocked.json")
        } else {
            "[]" // no positions, no open orders
        };
        Ok(HttpResponse { status: 200, body: body.to_string() })
    });
    let cfg = AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).unwrap();
    let creds = AlpacaCredentials::new(Environment::Paper, "PKTESTFIXTUREKEY0001", "unit-test-secret-not-a-real-key-9f3a").unwrap();
    let adapter = AlpacaAdapter::new(cfg, creds, t2).unwrap();
    let e = AlpacaBroker::us_etf(&adapter).snapshot(t0()).unwrap_err();
    assert!(matches!(e, SnapshotError::View(ViewError::AccountBlocked(_))), "{e:?}");
    assert_eq!(e.code(), "VIEW_ACCOUNT_BLOCKED");
}

#[test]
fn a_broker_outage_is_a_coded_snapshot_error() {
    let env = Env::with_usd("5000");
    fake_broker::scenarios::broker_unreachable_for(&env.rig.handle, 3);
    let e = env.broker().snapshot(t0()).unwrap_err();
    assert_eq!(e.code(), "BROKER_READ_FAILED");
    assert!(matches!(e, SnapshotError::Broker(_)));
}
