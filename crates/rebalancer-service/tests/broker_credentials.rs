//! The credential-broker client (certified-envelopes Phase 0), against a fake transport: no network, no key.

use std::collections::BTreeMap;
use std::sync::Arc;

use broker_adapters::alpaca::PAPER_BASE_URL;
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::TransportError;
use rebalancer_service::broker_credentials::{
    BrokerConfig, BrokerConfigError, HttpBrokerCredentials, BROKER_BAD_ID, BROKER_NOT_PAPER_URL, BROKER_REFUSED, BROKER_UNAUTHORIZED, BROKER_UNAVAILABLE, ENV_BROKER_TOKEN, ENV_BROKER_URL,
};
use rebalancer_service::credentials::{CredentialError, PaperCredentialProvider};
use serde_json::json;

const TENANT: &str = "3f2a9c1e-5b7d-4e8a-9c0d-1a2b3c4d5e6f";
const ACCOUNT: &str = "8d7c6b5a-4f3e-4d2c-8b1a-0f9e8d7c6b5a";
const TOKEN: &str = "test-broker-token-do-not-log";
const KEY_ID: &str = "PKTESTKEYID123456";
const SECRET: &str = "test-secret-value-do-not-log";

fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: BTreeMap<String, String> = pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
    move |k| map.get(k).cloned()
}

fn config() -> BrokerConfig {
    BrokerConfig::from_lookup(true, env(&[(ENV_BROKER_URL, "http://broker.internal:8080/"), (ENV_BROKER_TOKEN, TOKEN)])).unwrap().unwrap()
}

fn client(fake: &Arc<FakeTransport>) -> HttpBrokerCredentials {
    HttpBrokerCredentials::new(config(), fake.clone())
}

fn ok_body(base_url: &str) -> String {
    json!({ "exchange": "alpaca_paper", "key_id": KEY_ID, "secret": SECRET, "base_url": base_url, "key_id_fingerprint": "abcdef0123456789" }).to_string()
}

fn reason(e: CredentialError) -> &'static str {
    match e {
        CredentialError::NotProvided { reason, .. } => reason,
        other => panic!("expected NotProvided, got {other}"),
    }
}

#[test]
fn happy_path_returns_the_pair_and_sends_a_bearer_get_to_the_broker() {
    let fake = Arc::new(FakeTransport::new());
    fake.enqueue_json(200, &ok_body(PAPER_BASE_URL));
    let keys = client(&fake).paper_alpaca_keys(TENANT, ACCOUNT).expect("keys");
    assert_eq!(keys.key_id, KEY_ID);
    assert_eq!(keys.secret, SECRET);
    let reqs = fake.requests();
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].url, format!("http://broker.internal:8080/internal/credentials/paper/{TENANT}/{ACCOUNT}"));
    assert_eq!(reqs[0].header("authorization"), Some(format!("Bearer {TOKEN}").as_str()));
    // The request type redacts the bearer in Debug.
    assert!(!format!("{:?}", reqs[0]).contains(TOKEN));
}

#[test]
fn a_403_is_a_typed_refusal_and_never_a_key() {
    let fake = Arc::new(FakeTransport::new());
    fake.enqueue_json(403, &json!({ "error": { "code": "NOT_PAPER_CREDENTIAL" } }).to_string());
    assert_eq!(reason(client(&fake).paper_alpaca_keys(TENANT, ACCOUNT).unwrap_err()), BROKER_REFUSED);
}

#[test]
fn a_401_and_an_unreachable_broker_are_typed_not_provided() {
    let fake = Arc::new(FakeTransport::new());
    fake.enqueue_json(401, "{}");
    assert_eq!(reason(client(&fake).paper_alpaca_keys(TENANT, ACCOUNT).unwrap_err()), BROKER_UNAUTHORIZED);
    let fake = Arc::new(FakeTransport::new());
    fake.enqueue_error(TransportError::ConnectFailed("refused".into()));
    assert_eq!(reason(client(&fake).paper_alpaca_keys(TENANT, ACCOUNT).unwrap_err()), BROKER_UNAVAILABLE);
}

#[test]
fn a_non_paper_base_url_is_refused_even_with_a_200() {
    for url in ["https://api.example.invalid", "https://live-venue.invalid/v2", ""] {
        let fake = Arc::new(FakeTransport::new());
        fake.enqueue_json(200, &ok_body(url));
        let err = client(&fake).paper_alpaca_keys(TENANT, ACCOUNT).unwrap_err();
        assert_eq!(reason(err), BROKER_NOT_PAPER_URL, "base_url {url:?}");
    }
}

#[test]
fn ids_that_are_not_uuids_never_reach_the_network() {
    let fake = Arc::new(FakeTransport::new());
    let err = client(&fake).paper_alpaca_keys("../../etc", ACCOUNT).unwrap_err();
    assert_eq!(reason(err), BROKER_BAD_ID);
    assert_eq!(fake.request_count(), 0);
}

#[test]
fn flag_on_without_both_settings_refuses_to_start_and_flag_off_does_not_need_them() {
    assert_eq!(BrokerConfig::from_lookup(true, env(&[])).unwrap_err(), BrokerConfigError::MissingWithProposalPlans(ENV_BROKER_URL));
    assert_eq!(BrokerConfig::from_lookup(true, env(&[(ENV_BROKER_URL, "http://b")])).unwrap_err(), BrokerConfigError::MissingWithProposalPlans(ENV_BROKER_TOKEN));
    assert_eq!(BrokerConfig::from_lookup(true, env(&[(ENV_BROKER_TOKEN, TOKEN)])).unwrap_err(), BrokerConfigError::MissingWithProposalPlans(ENV_BROKER_URL));
    assert_eq!(BrokerConfig::from_lookup(true, env(&[(ENV_BROKER_URL, "  "), (ENV_BROKER_TOKEN, TOKEN)])).unwrap_err(), BrokerConfigError::MissingWithProposalPlans(ENV_BROKER_URL));
    assert_eq!(BrokerConfig::from_lookup(true, env(&[(ENV_BROKER_URL, "ftp://b"), (ENV_BROKER_TOKEN, TOKEN)])).unwrap_err(), BrokerConfigError::BadUrl);
    assert!(BrokerConfig::from_lookup(false, env(&[])).unwrap().is_none());
}

#[test]
fn debug_output_never_contains_the_token() {
    let cfg = config();
    assert!(!format!("{cfg:?}").contains(TOKEN));
    let fake = Arc::new(FakeTransport::new());
    assert!(!format!("{:?}", client(&fake)).contains(TOKEN));
}
