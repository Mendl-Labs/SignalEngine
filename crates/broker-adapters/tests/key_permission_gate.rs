//! Test-before-store gate, driven by a FAKE broker only. No network, no real key.

use broker_adapters::key_permissions::{test_before_store, KeyGateError, KeyPermissions, KeyRefusal};
use broker_adapters::types::{
    Balances, BrokerAdapter, CancelOutcome, OrderReport, OrderRequest, PlaceOutcome, Quote,
};
use broker_adapters::BrokerError;
use std::cell::Cell;

/// A fake venue whose permission read returns a canned result. Every other call is unsupported.
struct FakeBroker {
    permissions: Result<KeyPermissions, BrokerError>,
}

impl BrokerAdapter for FakeBroker {
    fn broker_name(&self) -> &'static str {
        "fakevenue"
    }
    fn get_balances(&self) -> Result<Balances, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn get_quote(&self, _symbol: &str) -> Result<Quote, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn place_order(&self, _req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        panic!("the key gate must never place an order");
    }
    fn get_order(&self, _id: &str) -> Result<OrderReport, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn find_orders_by_tag(&self, _tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn cancel_order(&self, _id: &str) -> Result<CancelOutcome, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn read_key_permissions(&self) -> Result<KeyPermissions, BrokerError> {
        match &self.permissions {
            Ok(p) => Ok(*p),
            Err(e) => Err(BrokerError::Malformed(e.to_string())),
        }
    }
}

/// A venue that does NOT override `read_key_permissions`: the trait default must refuse.
struct NoPermissionEndpoint;

impl BrokerAdapter for NoPermissionEndpoint {
    fn broker_name(&self) -> &'static str {
        "noendpoint"
    }
    fn get_balances(&self) -> Result<Balances, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn get_quote(&self, _symbol: &str) -> Result<Quote, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn place_order(&self, _req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        panic!("the key gate must never place an order");
    }
    fn get_order(&self, _id: &str) -> Result<OrderReport, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn find_orders_by_tag(&self, _tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
    fn cancel_order(&self, _id: &str) -> Result<CancelOutcome, BrokerError> {
        Err(BrokerError::Unsupported("fake".into()))
    }
}

const FAKE_KEY: &str = "FAKEKEY-do-not-echo-7731";

fn perms(trading: Option<bool>, withdrawal: Option<bool>) -> KeyPermissions {
    KeyPermissions { trading_enabled: trading, withdrawal_enabled: withdrawal }
}

fn broker(p: KeyPermissions) -> FakeBroker {
    FakeBroker { permissions: Ok(p) }
}

/// Runs the gate with a store closure that records whether it was called.
fn run(b: &dyn BrokerAdapter, stored: &Cell<u32>) -> Result<(), KeyGateError> {
    test_before_store(b, || -> Result<(), String> {
        stored.set(stored.get() + 1);
        Ok(())
    })
}

#[test]
fn all_clear_passes_and_stores_once() {
    let stored = Cell::new(0);
    let r = run(&broker(perms(Some(true), Some(false))), &stored);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(stored.get(), 1);
}

#[test]
fn withdrawal_enabled_is_refused_and_stores_nothing() {
    let stored = Cell::new(0);
    let err = run(&broker(perms(Some(true), Some(true))), &stored).unwrap_err();
    assert!(matches!(err, KeyGateError::Refused { reason: KeyRefusal::WithdrawalEnabled, .. }), "{err:?}");
    assert_eq!(stored.get(), 0);
}

#[test]
fn trading_disabled_is_refused_and_stores_nothing() {
    let stored = Cell::new(0);
    let err = run(&broker(perms(Some(false), Some(false))), &stored).unwrap_err();
    assert!(matches!(err, KeyGateError::Refused { reason: KeyRefusal::TradingDisabled, .. }), "{err:?}");
    assert_eq!(stored.get(), 0);
}

#[test]
fn missing_withdrawal_field_is_refused_and_stores_nothing() {
    let stored = Cell::new(0);
    let err = run(&broker(perms(Some(true), None)), &stored).unwrap_err();
    assert!(matches!(err, KeyGateError::Refused { reason: KeyRefusal::WithdrawalNotReported, .. }), "{err:?}");
    assert_eq!(stored.get(), 0);
}

#[test]
fn missing_trading_field_is_refused_and_stores_nothing() {
    let stored = Cell::new(0);
    let err = run(&broker(perms(None, Some(false))), &stored).unwrap_err();
    assert!(matches!(err, KeyGateError::Refused { reason: KeyRefusal::TradingNotReported, .. }), "{err:?}");
    assert_eq!(stored.get(), 0);
}

#[test]
fn probe_error_is_refused_and_stores_nothing() {
    let b = FakeBroker { permissions: Err(BrokerError::Http(500)) };
    let stored = Cell::new(0);
    let err = run(&b, &stored).unwrap_err();
    assert!(matches!(err, KeyGateError::ProbeFailed { .. }), "{err:?}");
    assert_eq!(stored.get(), 0);
}

#[test]
fn venue_without_permission_endpoint_refuses_and_says_so() {
    let stored = Cell::new(0);
    let err = run(&NoPermissionEndpoint, &stored).unwrap_err();
    let text = err.to_string();
    assert!(matches!(err, KeyGateError::ProbeFailed { .. }), "{err:?}");
    assert!(text.contains("no read-only key-permission endpoint"), "{text}");
    assert_eq!(stored.get(), 0);
}

#[test]
fn store_failure_after_passing_check_is_reported() {
    let r = test_before_store(&broker(perms(Some(true), Some(false))), || -> Result<(), String> {
        Err("disk full".into())
    });
    assert!(matches!(r, Err(KeyGateError::StoreFailed { .. })), "{r:?}");
}

#[test]
fn error_text_contains_no_key_material() {
    // The store closure would have received the key. Refusal text must never echo it.
    let stored = Cell::new(0);
    let secret_holder = FAKE_KEY.to_string();
    let b = broker(perms(Some(true), Some(true)));
    let err = test_before_store(&b, || -> Result<(), String> {
        stored.set(stored.get() + 1);
        let _ = &secret_holder;
        Ok(())
    })
    .unwrap_err();
    let text = format!("{err} {err:?}");
    assert!(!text.contains(FAKE_KEY), "error leaked key material: {text}");
    assert_eq!(stored.get(), 0);
}

#[test]
fn trait_default_refuses_without_network_for_every_venue_without_override() {
    let r = NoPermissionEndpoint.read_key_permissions();
    assert!(matches!(r, Err(BrokerError::Unsupported(_))), "{r:?}");
}
