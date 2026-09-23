//! OANDA environment / host / credential guards. Nothing here touches a network: the only transport
//! is `FakeTransport`, and several tests assert that it received NO request.

use broker_adapters::oanda::config::{LIVE_HOST, PRACTICE_HOST};
use broker_adapters::oanda::{Environment, LiveTradingAck, OandaAdapter, OandaConfig, OandaCredentials, OandaToken, LIVE_BASE_URL, PRACTICE_BASE_URL};
use broker_adapters::testing::FakeTransport;
use broker_adapters::BrokerError;
use std::sync::Arc;

const TOKEN: &str = "tok-9f3a-unit-test-not-a-real-token";
const PRACTICE_ACCT: &str = "101-001-1234567-001";
const LIVE_ACCT: &str = "001-001-7654321-001";

fn ack() -> LiveTradingAck {
    LiveTradingAck::confirm_real_money()
}

fn config_err(r: Result<OandaConfig, BrokerError>) -> String {
    match r {
        Err(BrokerError::Config(m)) => m,
        Err(other) => panic!("expected a Config error, got {other:?}"),
        Ok(c) => panic!("expected a refusal, got {c:?}"),
    }
}

// ---------------------------------------------------------------- environment names

#[test]
fn environment_names_are_strict_and_there_is_no_default() {
    assert_eq!(Environment::from_name("practice").unwrap(), Environment::Practice);
    assert_eq!(Environment::from_name(" PRACTICE ").unwrap(), Environment::Practice);
    assert_eq!(Environment::from_name("live").unwrap(), Environment::Live);
    assert_eq!(Environment::from_name("Live").unwrap(), Environment::Live);
    for bad in ["", " ", "paper", "demo", "prod", "production", "fxtrade", "fxpractice", "practise", "live-", "livee", "true", "0", "1", "practice,live"] {
        assert!(matches!(Environment::from_name(bad), Err(BrokerError::Config(_))), "{bad:?} must be refused, not defaulted");
    }
}

// ---------------------------------------------------------------- practice config

#[test]
fn practice_config_accepts_the_practice_host_and_loopback_only() {
    for ok in [
        "https://api-fxpractice.oanda.com",
        "https://api-fxpractice.oanda.com/",
        "https://API-FXPRACTICE.OANDA.COM",
        "https://api-fxpractice.oanda.com:443",
        "  https://api-fxpractice.oanda.com  ",
        "http://127.0.0.1:8080",
        "http://localhost:9999",
        "https://127.0.0.1",
        "http://[::1]:7000",
    ] {
        let c = OandaConfig::practice(ok).unwrap_or_else(|e| panic!("{ok:?}: {e}"));
        assert_eq!(c.environment(), Environment::Practice);
    }
    assert_eq!(OandaConfig::practice("https://API-FXPRACTICE.OANDA.COM/").unwrap().base_url(), PRACTICE_BASE_URL);
}

#[test]
fn practice_config_refuses_the_live_host_and_every_other_host() {
    let live_msg = config_err(OandaConfig::practice(LIVE_BASE_URL));
    assert!(live_msg.contains("mismatch") && live_msg.contains("live"), "{live_msg}");
    for bad in [
        // the live endpoint in any spelling
        "https://API-FXTRADE.OANDA.COM",
        "https://api-fxtrade.oanda.com/",
        "https://api-fxtrade.oanda.com:443",
        // look-alikes and tricks
        "https://api-fxpractice.oanda.com.evil.test",
        "https://evil-api-fxpractice.oanda.com",
        "https://api-fxpractice.oanda.com.",
        "https://api-fxpractice.oanda.com@evil.test",
        "https://evil.test@api-fxpractice.oanda.com",
        "https://api-fxpractice.oanda.com\\@evil.test",
        "https://api-fxpractice%2eoanda.com",
        "https://api-fxpractice.oanda.com:8443",
        "https://api-fxpractice.oanda.com:80",
        "http://api-fxpractice.oanda.com",
        "https://api-fxpractice.oanda.com/v3",
        "https://api-fxpractice.oanda.com?x=1",
        "https://api-fxpractice.oanda.com#x",
        "https://stream-fxpractice.oanda.com",
        "https://api-fxpractice.oanda.co",
        "https://api-fxpractice.oanda.com\u{200b}",
        "https://api-fxpractice.oanda.cоm", // Cyrillic o
        "ftp://api-fxpractice.oanda.com",
        "api-fxpractice.oanda.com",
        "//api-fxpractice.oanda.com",
        "",
        "https://",
        "https://:443",
        "https://localhost.evil.test",
        "https://127.0.0.1.evil.test",
        "https://0.0.0.0",
        "https://192.168.1.1",
        "https://example.com",
    ] {
        assert!(matches!(OandaConfig::practice(bad), Err(BrokerError::Config(_))), "{bad:?} must be refused");
    }
}

#[test]
fn a_practice_config_never_ends_up_on_the_live_host() {
    // Property over a pile of hostile inputs: if the constructor accepts, the URL is not live.
    let mut inputs: Vec<String> = vec![
        LIVE_BASE_URL.into(),
        format!("https://{LIVE_HOST}"),
        format!("https://{}", LIVE_HOST.to_uppercase()),
        format!("https://{LIVE_HOST}:443/"),
        format!("https://user:pw@{LIVE_HOST}"),
        format!("https://{PRACTICE_HOST}@{LIVE_HOST}"),
        format!("https://{LIVE_HOST}@{PRACTICE_HOST}"),
        format!("https://{PRACTICE_HOST}:443@{LIVE_HOST}"),
        format!("https://{PRACTICE_HOST}#@{LIVE_HOST}"),
        format!("https://{PRACTICE_HOST}\\@{LIVE_HOST}"),
    ];
    for suffix in ["", "/", "/v3", ":443", ":80", ".", "\t", " x"] {
        inputs.push(format!("https://{LIVE_HOST}{suffix}"));
    }
    for i in &inputs {
        if let Ok(c) = OandaConfig::practice(i) {
            assert_eq!(c.environment(), Environment::Practice);
            assert!(!c.base_url().contains(LIVE_HOST), "{i:?} produced {}", c.base_url());
        }
    }
}

// ---------------------------------------------------------------- live config

#[test]
fn live_config_needs_the_ack_and_the_live_host_and_nothing_else() {
    let c = OandaConfig::live(LIVE_BASE_URL, ack()).unwrap();
    assert_eq!(c.environment(), Environment::Live);
    assert_eq!(c.base_url(), LIVE_BASE_URL);
    assert_eq!(OandaConfig::live("https://API-FXTRADE.OANDA.COM/", ack()).unwrap().base_url(), LIVE_BASE_URL);
    assert_eq!(OandaConfig::live("https://api-fxtrade.oanda.com:443", ack()).unwrap().base_url(), LIVE_BASE_URL);

    let m = config_err(OandaConfig::live(PRACTICE_BASE_URL, ack()));
    assert!(m.contains("mismatch") && m.contains("practice"), "{m}");
    for bad in [
        "http://127.0.0.1:8080",
        "https://localhost",
        "http://[::1]",
        "http://api-fxtrade.oanda.com",
        "https://api-fxtrade.oanda.com:8443",
        "https://api-fxtrade.oanda.com.evil.test",
        "https://evil-api-fxtrade.oanda.com",
        "https://api-fxtrade.oanda.com@evil.test",
        "https://api-fxtrade.oanda.com/v3",
        "https://stream-fxtrade.oanda.com",
        "https://api-fxtrade.oanda.com.",
        "",
    ] {
        assert!(matches!(OandaConfig::live(bad, ack()), Err(BrokerError::Config(_))), "{bad:?} must be refused for live");
    }
}

#[test]
fn own_tag_prefix_and_client_tag_are_validated() {
    let c = OandaConfig::practice(PRACTICE_BASE_URL).unwrap();
    assert!(c.clone().with_own_tag_prefix("").is_err());
    assert_eq!(c.clone().with_own_tag_prefix("rb1:").unwrap().own_tag_prefix(), Some("rb1:"));
    assert!(c.clone().with_client_tag("").is_err());
    assert!(c.clone().with_client_tag(&"x".repeat(129)).is_err());
    assert!(c.clone().with_client_tag("caf\u{e9}").is_err());
    assert_eq!(c.with_client_tag("mendl-rb").unwrap().client_tag(), "mendl-rb");
}

// ---------------------------------------------------------------- credentials

#[test]
fn credentials_are_marked_and_contradictions_are_refused() {
    assert!(OandaCredentials::new(Environment::Practice, TOKEN, PRACTICE_ACCT).is_ok());
    assert!(OandaCredentials::new(Environment::Live, TOKEN, LIVE_ACCT).is_ok());
    // A live mark with a practice-looking account, and the reverse.
    let e = OandaCredentials::new(Environment::Live, TOKEN, PRACTICE_ACCT).unwrap_err().to_string();
    assert!(e.contains("practice prefix"), "{e}");
    let e = OandaCredentials::new(Environment::Practice, TOKEN, LIVE_ACCT).unwrap_err().to_string();
    assert!(e.contains("live prefix"), "{e}");
    let too_long = "1".repeat(33);
    for bad_acct in ["", "  ", "101-001-1234567-001/../x", "101 001", "acct", "101-001-1234567-001?x=1", too_long.as_str()] {
        assert!(matches!(OandaCredentials::new(Environment::Practice, TOKEN, bad_acct), Err(BrokerError::Credentials(_))), "{bad_acct:?}");
    }
    for bad_token in ["", "   ", "has space", "new\nline", "caf\u{e9}", "tab\there"] {
        assert!(matches!(OandaCredentials::new(Environment::Practice, bad_token, PRACTICE_ACCT), Err(BrokerError::Credentials(_))), "{bad_token:?}");
    }
}

#[test]
fn the_adapter_refuses_credentials_marked_for_the_other_environment_and_sends_nothing() {
    let t = Arc::new(FakeTransport::new());
    let practice_cfg = OandaConfig::practice(PRACTICE_BASE_URL).unwrap();
    let live_cfg = OandaConfig::live(LIVE_BASE_URL, ack()).unwrap();
    let practice_creds = || OandaCredentials::new(Environment::Practice, TOKEN, PRACTICE_ACCT).unwrap();
    let live_creds = || OandaCredentials::new(Environment::Live, TOKEN, LIVE_ACCT).unwrap();

    // practice config + live credentials
    let e = OandaAdapter::new(practice_cfg.clone(), live_creds(), t.clone()).unwrap_err().to_string();
    assert!(e.contains("marked live") && e.contains("configured for practice"), "{e}");
    // live config + practice credentials (a live host with a practice-looking setup)
    let e = OandaAdapter::new(live_cfg.clone(), practice_creds(), t.clone()).unwrap_err().to_string();
    assert!(e.contains("marked practice") && e.contains("configured for live"), "{e}");
    // matching pairs work
    assert!(OandaAdapter::new(practice_cfg, practice_creds(), t.clone()).is_ok());
    assert!(OandaAdapter::new(live_cfg, live_creds(), t.clone()).is_ok());
    assert_eq!(t.request_count(), 0, "constructing an adapter never sends anything");
}

#[test]
fn every_request_of_a_practice_adapter_goes_to_the_practice_host() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(500, "{}");
    let a = OandaAdapter::new(
        OandaConfig::practice(PRACTICE_BASE_URL).unwrap(),
        OandaCredentials::new(Environment::Practice, TOKEN, PRACTICE_ACCT).unwrap(),
        t.clone(),
    )
    .unwrap();
    let _ = a.get_account_summary();
    let reqs = t.requests();
    assert_eq!(reqs.len(), 1);
    assert!(reqs[0].url.starts_with("https://api-fxpractice.oanda.com/v3/accounts/"), "{}", reqs[0].url);
    assert!(!reqs[0].url.contains(LIVE_HOST));
    assert_eq!(a.environment(), Environment::Practice);
}

// ---------------------------------------------------------------- redaction

#[test]
fn the_token_is_redacted_in_debug_output_and_never_in_error_texts() {
    let tok = OandaToken::new(TOKEN).unwrap();
    assert_eq!(format!("{tok:?}"), "<redacted>");
    assert!(!format!("{tok:#?}").contains(TOKEN));

    let creds = OandaCredentials::new(Environment::Practice, TOKEN, PRACTICE_ACCT).unwrap();
    let dbg = format!("{creds:?} {creds:#?}");
    assert!(!dbg.contains(TOKEN), "{dbg}");
    assert!(dbg.contains("<redacted>") && dbg.contains(&creds.token_fingerprint()));

    let t = Arc::new(FakeTransport::new());
    let cfg = OandaConfig::practice(PRACTICE_BASE_URL).unwrap();
    let a = OandaAdapter::new(cfg.clone(), creds, t).unwrap();
    let dbg = format!("{a:?} {a:#?} {cfg:?}");
    assert!(!dbg.contains(TOKEN), "{dbg}");

    // Constructor errors do not echo the offending input.
    let secret_like = "bad token with spaces SECRET-XYZ";
    let e = OandaCredentials::new(Environment::Practice, secret_like, PRACTICE_ACCT).unwrap_err();
    assert!(!e.to_string().contains("SECRET-XYZ") && !format!("{e:?}").contains("SECRET-XYZ"));
    let e = OandaToken::new("x y SECRET-XYZ").unwrap_err();
    assert!(!e.to_string().contains("SECRET-XYZ"));
}

#[test]
fn a_recorded_request_debug_redacts_the_authorization_header() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(500, "{}");
    let a = OandaAdapter::new(
        OandaConfig::practice(PRACTICE_BASE_URL).unwrap(),
        OandaCredentials::new(Environment::Practice, TOKEN, PRACTICE_ACCT).unwrap(),
        t.clone(),
    )
    .unwrap();
    let _ = a.get_account_summary();
    let req = &t.requests()[0];
    assert_eq!(req.header("Authorization"), Some(format!("Bearer {TOKEN}").as_str()), "the header itself is sent");
    let dbg = format!("{req:?}");
    assert!(!dbg.contains(TOKEN) && dbg.contains("<redacted>"), "{dbg}");
}

// ---------------------------------------------------------------- tag-scan options

#[test]
fn the_restart_scan_window_defaults_to_400_is_bounded_and_strict_mode_is_off_by_default() {
    use broker_adapters::oanda::DEFAULT_RESTART_SCAN_WINDOW;
    let c = OandaConfig::practice(PRACTICE_BASE_URL).unwrap();
    assert_eq!((c.restart_scan_window(), DEFAULT_RESTART_SCAN_WINDOW, c.strict_unseen_tags()), (400, 400, false));
    assert_eq!(c.clone().with_restart_scan_window(10).unwrap().restart_scan_window(), 10);
    assert_eq!(c.clone().with_restart_scan_window(100_000).unwrap().restart_scan_window(), 100_000);
    for bad in [0, 9, 100_001, u64::MAX] {
        assert!(matches!(c.clone().with_restart_scan_window(bad), Err(BrokerError::Config(_))), "{bad}");
    }
    assert!(c.with_strict_unseen_tags(true).strict_unseen_tags());
}
