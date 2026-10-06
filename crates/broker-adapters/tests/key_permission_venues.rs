//! Per-venue read-only permission reads, over fixtures and `FakeTransport`. No network, no real key.

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::{AlpacaAdapter, AlpacaConfig, AlpacaCredentials, Environment as AlpacaEnv};
use broker_adapters::key_permissions::{test_before_store, KeyGateError, KeyPermissions, KeyRefusal};
use broker_adapters::oanda::{Environment as OandaEnv, OandaAdapter, OandaConfig, OandaCredentials, PRACTICE_BASE_URL};
use broker_adapters::testing::FakeTransport;
use broker_adapters::types::BrokerAdapter;
use std::cell::Cell;
use std::sync::Arc;

const ALPACA_KEY: &str = "PKTESTFIXTUREKEY0001";
const ALPACA_SECRET: &str = "unit-test-secret-not-a-real-key-9f3a";
const OANDA_TOKEN: &str = "tok-9f3a-unit-test-not-a-real-token";
const OANDA_ACCT: &str = "101-001-1234567-001";

fn alpaca(body: &str) -> (AlpacaAdapter, Arc<FakeTransport>) {
    let t = Arc::new(FakeTransport::new());
    // Two reads: one direct, one through the gate.
    t.enqueue_json(200, body).enqueue_json(200, body);
    let cfg = AlpacaConfig::new(AlpacaEnv::Paper, PAPER_BASE_URL).unwrap();
    let creds = AlpacaCredentials::new(AlpacaEnv::Paper, ALPACA_KEY, ALPACA_SECRET).unwrap();
    (AlpacaAdapter::new(cfg, creds, t.clone()).unwrap(), t)
}

#[test]
fn alpaca_clear_account_is_still_refused_because_withdrawal_is_not_reported() {
    let (a, t) = alpaca(include_str!("fixtures/alpaca/account_ok.json"));
    let p = a.read_key_permissions().unwrap();
    assert_eq!(p.trading_enabled, Some(true));
    assert_eq!(p.withdrawal_enabled, None, "alpaca's account object has no withdrawal field");
    assert_eq!(t.request_count(), 1, "one read-only GET /v2/account");

    let stored = Cell::new(0);
    let err = test_before_store(&a, || -> Result<(), String> {
        stored.set(1);
        Ok(())
    })
    .unwrap_err();
    assert!(matches!(err, KeyGateError::Refused { reason: KeyRefusal::WithdrawalNotReported, .. }), "{err:?}");
    assert_eq!(stored.get(), 0);
}

#[test]
fn alpaca_trading_blocked_account_reports_trading_disabled() {
    let (a, _t) = alpaca(include_str!("fixtures/alpaca/account_trading_blocked.json"));
    let p = a.read_key_permissions().unwrap();
    assert_eq!(p, KeyPermissions { trading_enabled: Some(false), withdrawal_enabled: None });
}

#[test]
fn oanda_summary_reports_no_permission_fields_and_is_refused() {
    let t = Arc::new(FakeTransport::new());
    let body = include_str!("fixtures/oanda/account_summary_ok.json");
    t.enqueue_json(200, body).enqueue_json(200, body);
    let cfg = OandaConfig::practice(PRACTICE_BASE_URL).unwrap();
    let creds = OandaCredentials::new(OandaEnv::Practice, OANDA_TOKEN, OANDA_ACCT).unwrap();
    let a = OandaAdapter::new(cfg, creds, t.clone()).unwrap();

    let p = a.read_key_permissions().unwrap();
    assert_eq!(p, KeyPermissions { trading_enabled: None, withdrawal_enabled: None });
    assert_eq!(t.request_count(), 1, "one read-only account summary GET");

    let stored = Cell::new(0);
    let err = test_before_store(&a, || -> Result<(), String> {
        stored.set(1);
        Ok(())
    })
    .unwrap_err();
    assert!(matches!(err, KeyGateError::Refused { reason: KeyRefusal::WithdrawalNotReported, .. }), "{err:?}");
    assert_eq!(stored.get(), 0);
    let text = err.to_string();
    assert!(!text.contains(OANDA_TOKEN) && !text.contains(ALPACA_SECRET), "{text}");
}
