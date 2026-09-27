//! The paper-only Alpaca adapter (slice S-8 of the paper-pilot plan): strict `PK` key ids, paper host only, `PA`
//! account numbers only, and no way to name the live environment. All offline.

use broker_adapters::alpaca::config::{LIVE_BASE_URL, PAPER_BASE_URL};
use broker_adapters::alpaca::paper_only::{require_paper_account_number, require_paper_key_id, PAPER_ACCOUNT_PREFIX, PAPER_KEY_PREFIX};
use broker_adapters::alpaca::{Environment, PaperOnlyAlpaca};
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::HttpMethod;
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

fn build(key: &str, url: &str) -> Result<(PaperOnlyAlpaca, Arc<FakeTransport>), BrokerError> {
    let t = Arc::new(FakeTransport::new());
    Ok((PaperOnlyAlpaca::new(key, SECRET, url, t.clone(), Some("rb1:"), true)?, t))
}

#[test]
fn the_prefixes_are_pinned() {
    assert_eq!((PAPER_KEY_PREFIX, PAPER_ACCOUNT_PREFIX), ("PK", "PA"));
}

// ---------------------------------------------------------------- key id

#[test]
fn a_pk_key_is_accepted() {
    assert!(require_paper_key_id(PAPER_KEY).is_ok());
    assert!(require_paper_key_id("  PKPADDED  ").is_ok(), "surrounding whitespace is trimmed like the credentials constructor does");
    assert!(build(PAPER_KEY, PAPER_BASE_URL).is_ok());
}

#[test]
fn an_ak_key_a_wrong_prefix_and_an_empty_key_are_refused_without_echoing_them() {
    for key in [LIVE_KEY, "AK", "CKSOMETHINGELSE", "pkLOWERCASE", "XPKNOTAPREFIX", "", "   "] {
        match require_paper_key_id(key) {
            Err(BrokerError::Credentials(m)) => {
                assert!(!m.contains(key.trim()) || key.trim().len() <= 2, "the error must not echo the key: {m}");
                assert!(m.contains("paper-only"), "{m}");
            }
            other => panic!("{key:?}: {other:?}"),
        }
        assert!(build(key, PAPER_BASE_URL).is_err(), "{key:?} must not build an adapter");
    }
    // the live prefix is named as such (an operator who pasted a live key needs to know)
    match require_paper_key_id(LIVE_KEY) {
        Err(BrokerError::Credentials(m)) => assert!(m.contains("live prefix (AK)"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_refused_key_never_reaches_the_transport() {
    let t = Arc::new(FakeTransport::new());
    assert!(PaperOnlyAlpaca::new(LIVE_KEY, SECRET, PAPER_BASE_URL, t.clone(), None, false).is_err());
    assert_eq!(t.request_count(), 0);
}

// ---------------------------------------------------------------- host

#[test]
fn only_the_paper_host_or_a_loopback_host_is_accepted() {
    assert!(build(PAPER_KEY, PAPER_BASE_URL).is_ok());
    assert!(build(PAPER_KEY, "http://127.0.0.1:8080").is_ok(), "the fake broker");
    for url in [
        LIVE_BASE_URL,
        "https://api.alpaca.markets/",
        "https://API.ALPACA.MARKETS",
        "https://api.alpaca.markets.evil.com",
        "https://paper-api.alpaca.markets@api.alpaca.markets",
        "https://paper-api.alpaca.markets.evil.com",
        "http://paper-api.alpaca.markets",
        "https://data.alpaca.markets",
        "https://example.com",
        "",
    ] {
        assert!(build(PAPER_KEY, url).is_err(), "{url:?} must be refused for a paper-only adapter");
    }
}

#[test]
fn a_live_key_on_the_live_host_is_refused_twice_over() {
    // wrong key AND wrong host: either alone is enough
    assert!(build(LIVE_KEY, LIVE_BASE_URL).is_err());
    assert!(build(PAPER_KEY, LIVE_BASE_URL).is_err(), "a paper key on the live host");
    assert!(build(LIVE_KEY, PAPER_BASE_URL).is_err(), "a live key on the paper host");
}

// ---------------------------------------------------------------- the built adapter

#[test]
fn the_built_adapter_is_paper_talks_only_to_the_paper_host_and_has_the_pilot_settings() {
    let (a, t) = build(PAPER_KEY, PAPER_BASE_URL).unwrap();
    assert_eq!(a.adapter().environment(), Environment::Paper);
    assert_eq!(a.adapter().base_url(), PAPER_BASE_URL);
    assert_eq!(a.adapter().config().own_tag_prefix.as_deref(), Some("rb1:"));
    assert!(a.adapter().config().refuse_builtin_assets);
    assert!(!a.adapter().config().allow_extended_hours, "an ETF order outside the session must be refused, not queued");
    t.enqueue_json(200, fixture!("account_ok.json"));
    a.adapter().get_account().unwrap();
    let r = &t.requests()[0];
    assert_eq!(r.method, HttpMethod::Get);
    assert_eq!(r.url, "https://paper-api.alpaca.markets/v2/account");
    assert!(!r.url.contains("//api."), "never the live host");
}

#[test]
fn debug_and_errors_do_not_leak_the_key_or_secret() {
    let (a, _t) = build(PAPER_KEY, PAPER_BASE_URL).unwrap();
    let text = format!("{a:?} {:#?}", a);
    assert!(!text.contains(PAPER_KEY) && !text.contains(SECRET), "{text}");
    let e = PaperOnlyAlpaca::new(LIVE_KEY, SECRET, PAPER_BASE_URL, Arc::new(FakeTransport::new()), None, false).unwrap_err();
    let text = format!("{e} {e:?}");
    assert!(!text.contains(LIVE_KEY) && !text.contains(SECRET), "{text}");
    assert_eq!(a.key_id_fingerprint().len(), 16);
}

// ---------------------------------------------------------------- account number

#[test]
fn a_paper_account_number_is_required_and_a_missing_or_live_looking_one_is_refused() {
    // PA number: ok
    let (a, t) = build(PAPER_KEY, PAPER_BASE_URL).unwrap();
    t.enqueue_json(200, fixture!("account_ok.json")); // PA3TESTFIXT1
    let acct = a.verify_paper_account().unwrap();
    assert_eq!(acct.account_number.as_deref(), Some("PA3TESTFIXT1"));

    // a live-looking number: refused (the general paper check would have let it through)
    let (a, t) = build(PAPER_KEY, PAPER_BASE_URL).unwrap();
    t.enqueue_json(200, fixture!("account_live.json")); // 912345678
    match a.verify_paper_account() {
        Err(BrokerError::Credentials(m)) => assert!(m.contains("paper prefix (PA)"), "{m}"),
        other => panic!("{other:?}"),
    }

    // no account number at all: refused (positive evidence required)
    let (a, t) = build(PAPER_KEY, PAPER_BASE_URL).unwrap();
    let no_number = fixture!("account_ok.json").replace("\"account_number\": \"PA3TESTFIXT1\",", "");
    assert!(!no_number.contains("account_number"), "the fixture edit worked");
    t.enqueue_json(200, &no_number);
    match a.verify_paper_account() {
        Err(BrokerError::Credentials(m)) => assert!(m.contains("no account number"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_blocked_paper_account_is_still_refused_by_the_general_check() {
    let (a, t) = build(PAPER_KEY, PAPER_BASE_URL).unwrap();
    t.enqueue_json(200, fixture!("account_trading_blocked.json"));
    assert!(matches!(a.verify_paper_account(), Err(BrokerError::AccountBlocked(_))));
}

#[test]
fn the_account_number_check_is_a_pure_function_too() {
    let (a, t) = build(PAPER_KEY, PAPER_BASE_URL).unwrap();
    t.enqueue_json(200, fixture!("account_ok.json"));
    let mut acct = a.adapter().get_account().unwrap();
    assert!(require_paper_account_number(&acct).is_ok());
    acct.account_number = Some("pa-lowercase".into());
    assert!(require_paper_account_number(&acct).is_err(), "the prefix is case-sensitive");
    acct.account_number = Some("AK123".into());
    assert!(require_paper_account_number(&acct).is_err());
    acct.account_number = None;
    assert!(require_paper_account_number(&acct).is_err());
}

// ---------------------------------------------------------------- the module cannot name the live environment

/// Source lines with comment lines removed.
fn code_lines(src: &str) -> Vec<&str> {
    src.lines().filter(|l| !l.trim_start().starts_with("//")).collect()
}

#[test]
fn the_paper_only_module_never_names_the_live_environment_host_or_url() {
    let src = include_str!("../src/alpaca/paper_only.rs");
    let code = code_lines(src);
    assert!(code.len() > 40, "the scan is not vacuous ({} code lines)", code.len());
    for line in &code {
        for forbidden in ["Environment::Live", "LIVE_BASE_URL", "LIVE_HOST", "api.alpaca.markets", "Live"] {
            assert!(!line.contains(forbidden), "paper_only.rs names {forbidden:?}: {line}");
        }
    }
    // and the scan does catch a planted violation
    let planted = "let e = Environment::Live;\n// Environment::Live in a comment is fine\n";
    let hits: Vec<_> = code_lines(planted).into_iter().filter(|l| l.contains("Environment::Live")).collect();
    assert_eq!(hits.len(), 1);
}
