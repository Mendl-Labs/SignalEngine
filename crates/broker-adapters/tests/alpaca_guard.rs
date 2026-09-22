//! Live-vs-paper guard, credential handling and redaction for the Alpaca adapter. All offline.

use broker_adapters::alpaca::config::{LIVE_BASE_URL, PAPER_BASE_URL};
use broker_adapters::alpaca::{AlpacaAdapter, AlpacaConfig, AlpacaCredentials, Environment};
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpMethod, HttpRequest};
use broker_adapters::BrokerError;
use std::sync::Arc;

const PAPER_KEY: &str = "PKTESTFIXTUREKEY0001";
const LIVE_KEY: &str = "AKTESTFIXTUREKEY0002";
const SECRET: &str = "unit-test-secret-not-a-real-key-9f3a";

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!("fixtures/alpaca/", $name))
    };
}

fn adapter(env: Environment, url: &str, key: &str) -> Result<(AlpacaAdapter, Arc<FakeTransport>), BrokerError> {
    let t = Arc::new(FakeTransport::new());
    let cfg = AlpacaConfig::new(env, url)?;
    let creds = AlpacaCredentials::new(env, key, SECRET)?;
    Ok((AlpacaAdapter::new(cfg, creds, t.clone())?, t))
}

// ---------------------------------------------------------------- host guard

#[test]
fn paper_needs_the_paper_host_and_live_needs_the_live_host() {
    assert!(AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).is_ok());
    assert!(AlpacaConfig::new(Environment::Live, LIVE_BASE_URL).is_ok());
}

#[test]
fn a_live_environment_can_never_be_pointed_at_the_paper_host_and_vice_versa() {
    for (env, url) in [
        (Environment::Live, PAPER_BASE_URL),
        (Environment::Paper, LIVE_BASE_URL),
        (Environment::Live, "https://PAPER-API.alpaca.markets"),
        (Environment::Live, "https://paper-api.alpaca.markets/"),
    ] {
        match AlpacaConfig::new(env, url) {
            Err(BrokerError::Config(m)) => assert!(m.contains("mismatch"), "{m}"),
            other => panic!("{env:?} {url}: {other:?}"),
        }
    }
}

#[test]
fn adapter_constructor_rechecks_the_host_even_if_a_public_field_was_edited() {
    let mut cfg = AlpacaConfig::new(Environment::Live, LIVE_BASE_URL).unwrap();
    cfg.base_url = PAPER_BASE_URL.to_string(); // bypass attempt: fields are public
    let creds = AlpacaCredentials::new(Environment::Live, LIVE_KEY, SECRET).unwrap();
    let r = AlpacaAdapter::new(cfg, creds, Arc::new(FakeTransport::new()));
    assert!(matches!(r, Err(BrokerError::Config(_))), "{r:?}");
}

#[test]
fn look_alike_hosts_userinfo_paths_ports_and_schemes_are_refused() {
    let bad = [
        "https://api.alpaca.markets.evil.com",
        "https://evilapi.alpaca.markets",
        "https://paper-api.alpaca.markets@evil.example",
        "https://evil.example@paper-api.alpaca.markets",
        "https://paper-api.alpaca.markets/v2",
        "https://paper-api.alpaca.markets?x=1",
        "https://paper-api.alpaca.markets:8443",
        "http://paper-api.alpaca.markets",
        "ftp://paper-api.alpaca.markets",
        "paper-api.alpaca.markets",
        "https://data.alpaca.markets",
        "https://broker-api.alpaca.markets",
        "https://",
        "",
    ];
    for url in bad {
        assert!(AlpacaConfig::new(Environment::Paper, url).is_err(), "{url:?} must be refused");
    }
    for url in ["https://api.alpaca.markets.evil.com", "https://api.alpaca.markets:444", "http://api.alpaca.markets"] {
        assert!(AlpacaConfig::new(Environment::Live, url).is_err(), "{url:?} must be refused");
    }
}

#[test]
fn urls_are_normalised_and_loopback_is_paper_only() {
    let c = AlpacaConfig::new(Environment::Paper, "https://Paper-Api.Alpaca.Markets/").unwrap();
    assert_eq!(c.base_url, "https://paper-api.alpaca.markets");
    let c = AlpacaConfig::new(Environment::Paper, "https://paper-api.alpaca.markets:443").unwrap();
    assert_eq!(c.base_url, "https://paper-api.alpaca.markets");
    // the fake broker
    let c = AlpacaConfig::new(Environment::Paper, "http://127.0.0.1:8080/").unwrap();
    assert_eq!(c.base_url, "http://127.0.0.1:8080");
    assert!(AlpacaConfig::new(Environment::Paper, "http://localhost:9000").is_ok());
    // a live environment never talks to a local host
    assert!(AlpacaConfig::new(Environment::Live, "http://127.0.0.1:8080").is_err());
}

// ---------------------------------------------------------------- credentials vs environment

#[test]
fn credentials_marked_live_are_refused_by_a_paper_adapter_and_vice_versa() {
    let t = Arc::new(FakeTransport::new());
    let live_creds = AlpacaCredentials::new(Environment::Live, LIVE_KEY, SECRET).unwrap();
    let paper_cfg = AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).unwrap();
    match AlpacaAdapter::new(paper_cfg, live_creds, t.clone()) {
        Err(BrokerError::Credentials(m)) => {
            assert!(m.contains("marked live") && m.contains("configured for paper"), "{m}");
            assert!(!m.contains(LIVE_KEY) && !m.contains(SECRET));
        }
        other => panic!("{other:?}"),
    }
    let paper_creds = AlpacaCredentials::new(Environment::Paper, PAPER_KEY, SECRET).unwrap();
    let live_cfg = AlpacaConfig::new(Environment::Live, LIVE_BASE_URL).unwrap();
    assert!(matches!(AlpacaAdapter::new(live_cfg, paper_creds, t.clone()), Err(BrokerError::Credentials(_))));
    assert_eq!(t.request_count(), 0);
}

#[test]
fn a_key_id_prefix_that_contradicts_the_mark_is_refused() {
    // paper keys start PK, live keys AK (FROM-MEMORY-OF-DOCS); unknown prefixes are allowed
    assert!(matches!(AlpacaCredentials::new(Environment::Live, PAPER_KEY, SECRET), Err(BrokerError::Credentials(_))));
    assert!(matches!(AlpacaCredentials::new(Environment::Paper, LIVE_KEY, SECRET), Err(BrokerError::Credentials(_))));
    assert!(AlpacaCredentials::new(Environment::Paper, "CKSOMETHINGELSE", SECRET).is_ok());
}

#[test]
fn empty_or_header_unsafe_credentials_are_refused_without_echoing_them() {
    for (k, s) in [("", SECRET), (PAPER_KEY, ""), ("  ", SECRET), ("PK\nX-Injected: 1", SECRET), (PAPER_KEY, "sec ret"), (PAPER_KEY, "sec\r\nret")] {
        match AlpacaCredentials::new(Environment::Paper, k, s) {
            Err(BrokerError::Credentials(m)) => {
                assert!(!m.contains("Injected") && !m.contains("sec ret"), "{m}");
            }
            other => panic!("{k:?} {s:?}: {other:?}"),
        }
    }
}

#[test]
fn a_live_adapter_refuses_an_account_that_looks_like_paper() {
    let (a, t) = adapter(Environment::Live, LIVE_BASE_URL, LIVE_KEY).unwrap();
    t.enqueue_json(200, fixture!("account_ok.json")); // account_number PA3TESTFIXT1
    match a.verify_account() {
        Err(BrokerError::Credentials(m)) => assert!(m.contains("LIVE") && m.contains("PA"), "{m}"),
        other => panic!("{other:?}"),
    }
    let (a, t) = adapter(Environment::Live, LIVE_BASE_URL, LIVE_KEY).unwrap();
    t.enqueue_json(200, fixture!("account_live.json"));
    assert!(a.verify_account().is_ok());
}

#[test]
fn requests_go_only_to_the_configured_host_with_the_documented_auth_headers() {
    let (a, t) = adapter(Environment::Live, LIVE_BASE_URL, LIVE_KEY).unwrap();
    t.enqueue_json(200, fixture!("account_live.json"));
    a.get_account().unwrap();
    let r = &t.requests()[0];
    assert_eq!(r.method, HttpMethod::Get);
    assert_eq!(r.url, "https://api.alpaca.markets/v2/account");
    assert_eq!(r.header("APCA-API-KEY-ID"), Some(LIVE_KEY));
    assert_eq!(r.header("APCA-API-SECRET-KEY"), Some(SECRET));
    assert!(!r.url.contains("paper"));

    let (a, t) = adapter(Environment::Paper, PAPER_BASE_URL, PAPER_KEY).unwrap();
    t.enqueue_json(200, fixture!("account_ok.json"));
    a.get_account().unwrap();
    assert_eq!(t.requests()[0].url, "https://paper-api.alpaca.markets/v2/account");
}

// ---------------------------------------------------------------- redaction

#[test]
fn debug_output_never_contains_key_or_secret() {
    let (a, t) = adapter(Environment::Paper, PAPER_BASE_URL, PAPER_KEY).unwrap();
    t.enqueue_json(200, fixture!("account_ok.json"));
    a.get_account().unwrap();

    let creds = AlpacaCredentials::new(Environment::Paper, PAPER_KEY, SECRET).unwrap();
    let request_debug = format!("{:?}", t.requests()[0]);
    for text in [format!("{a:?}"), format!("{creds:?}"), request_debug, format!("{:#?}", t.requests())] {
        assert!(!text.contains(PAPER_KEY), "key leaked: {text}");
        assert!(!text.contains(SECRET), "secret leaked: {text}");
        assert!(text.contains("<redacted>"), "{text}");
    }
    // the fingerprint is stable and not the key
    assert_eq!(creds.key_id_fingerprint(), a.key_id_fingerprint());
    assert_eq!(creds.key_id_fingerprint().len(), 16);
}

#[test]
fn http_request_debug_redacts_both_alpaca_headers_but_not_others() {
    let req = HttpRequest {
        method: HttpMethod::Get,
        url: "https://paper-api.alpaca.markets/v2/account".into(),
        headers: vec![
            ("APCA-API-KEY-ID".into(), "KEYVALUE".into()),
            ("apca-api-secret-key".into(), "SECRETVALUE".into()),
            ("Accept".into(), "application/json".into()),
        ],
        body: None,
    };
    let text = format!("{req:?}");
    assert!(!text.contains("KEYVALUE") && !text.contains("SECRETVALUE"), "{text}");
    assert!(text.contains("application/json"));
}

#[test]
fn error_values_do_not_carry_credentials() {
    let (a, t) = adapter(Environment::Paper, PAPER_BASE_URL, PAPER_KEY).unwrap();
    t.enqueue_json(401, fixture!("error_401_unauthorized.json"));
    let e = a.get_account().unwrap_err();
    let text = format!("{e} {e:?}");
    assert!(!text.contains(PAPER_KEY) && !text.contains(SECRET), "{text}");
}
