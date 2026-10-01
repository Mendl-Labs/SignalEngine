//! The API key appears in exactly one place: the `Authorization: Bearer` header of an outgoing request. Never in a URL,
//! an error, a `DataError`, a provenance record, a `Debug` rendering or a log line.

mod common;

use std::sync::Arc;

use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpResponse, TransportError};
use common::*;
use market_data::testing::NOT_AUTHORIZED_MESSAGE_BODY;
use market_data::{EnvKeyProvider, KeyError, KeyProvider, MassiveDataSource, SecretString, SleeveFetcher, StaticKeyProvider};
use rebalancer_run::data::DataSource;

fn no_key(text: &str, what: &str) {
    assert!(!text.contains(KEY), "{what} leaks the key: {text}");
    // not even a long prefix of it (a truncated echo must not leave half a key behind)
    assert!(!text.contains(&KEY[..12]), "{what} leaks part of the key: {text}");
}

#[test]
fn the_key_is_only_ever_in_the_authorization_header_of_a_successful_fetch() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let etf = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap();
    let crypto = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap();

    let bearer = format!("Bearer {KEY}");
    for r in h.transport.requests() {
        assert!(!r.url.contains(KEY), "URL: {}", r.url);
        assert!(!r.url.to_ascii_lowercase().contains("apikey"), "URL: {}", r.url);
        for (name, value) in &r.headers {
            if name.eq_ignore_ascii_case("authorization") {
                assert_eq!(value, &bearer);
            } else {
                assert!(!value.contains(KEY), "header {name} carries the key");
            }
        }
        assert!(r.body.is_none());
        // the request's own Debug is redacted by the shared transport type
        assert!(!format!("{r:?}").contains(KEY));
    }
    for p in etf.provenance.iter().chain(&crypto.provenance) {
        no_key(&format!("{p:?}"), "provenance");
    }
    for p in h.src.recent_provenance() {
        no_key(&format!("{p:#?}"), "recent provenance");
    }
    no_key(&format!("{:?}", etf.panel), "panel");
}

#[test]
fn the_source_and_its_key_holders_never_debug_print_the_key() {
    let h = Harness::new(as_of());
    no_key(&format!("{:?}", h.src), "MassiveDataSource Debug");
    no_key(&format!("{:#?}", h.src), "MassiveDataSource pretty Debug");
    no_key(&format!("{:?}", SecretString::new(KEY)), "SecretString");
    no_key(&format!("{:?}", StaticKeyProvider::new(KEY).unwrap()), "StaticKeyProvider");
    no_key(&format!("{:?}", StaticKeyProvider::new(KEY).unwrap().api_key().unwrap()), "the secret a provider hands out");
}

/// A provider whose own Debug prints the key: the source must not print it.
struct LeakyProvider;
impl std::fmt::Debug for LeakyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LeakyProvider({KEY})")
    }
}
impl KeyProvider for LeakyProvider {
    fn api_key(&self) -> Result<SecretString, KeyError> {
        Ok(SecretString::new(KEY))
    }
}

#[test]
fn the_source_debug_does_not_delegate_to_a_leaky_key_provider() {
    let src = MassiveDataSource::new(LeakyProvider, Arc::new(FakeTransport::new()));
    no_key(&format!("{src:?}"), "source Debug with a leaky provider");
}

#[test]
fn transport_errors_that_echo_the_key_are_scrubbed() {
    let h = Harness::new(as_of());
    let leak = format!("connect to https://api.massive.com/v2/aggs?apiKey={KEY} failed; Authorization: Bearer {KEY}");
    h.transport.set_handler(move |_| Err(TransportError::Io(leak.clone())));
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    no_key(&e.to_string(), "error Display");
    no_key(&format!("{e:?}"), "error Debug");
    let de: rebalancer_run::data::DataError = e.into();
    no_key(&de.message, "DataError message");
    no_key(&format!("{de:?}"), "DataError Debug");
}

#[test]
fn a_403_that_echoes_the_key_is_scrubbed() {
    let h = Harness::new(as_of());
    let body = format!(r#"{{"status":"NOT_AUTHORIZED","message":"Unknown API Key {KEY}; see ?apiKey={KEY}"}}"#);
    h.transport.enqueue_json(403, &body);
    let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    no_key(&e.to_string(), "403 error");
    let de: rebalancer_run::data::DataError = e.into();
    no_key(&de.message, "403 DataError");
    assert!(de.message.contains("NOT") || de.message.contains("not") || de.message.contains("Unknown"), "the explanation survives: {}", de.message);
}

#[test]
fn a_malformed_body_that_echoes_the_key_is_scrubbed() {
    let h = Harness::new(as_of());
    h.transport.enqueue_json(200, &format!(r#"{{"status":"{KEY}"}}"#));
    let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    no_key(&e.to_string(), "status echo");

    let h = Harness::new(as_of());
    h.transport.enqueue_json(200, &format!(r#"{{"status":"OK","ticker":"{KEY}"}}"#));
    let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    no_key(&e.to_string(), "ticker echo");
}

#[test]
fn a_next_url_that_would_put_the_key_in_the_path_is_refused_not_followed() {
    let h = Harness::new(as_of());
    let world = standard_world();
    let bars = move |t: &str| -> Vec<(i64, f64)> { world.of(t).iter().filter(|(x, _)| *x < as_of()).map(|(x, c)| (market_data::testing::crypto_ts(*x), *c)).collect() };
    h.transport.set_handler(move |_| {
        Ok(HttpResponse { status: 200, body: market_data::testing::page_json("X:BTCUSD", &bars("X:BTCUSD"), Some(&format!("https://api.massive.com/v2/aggs/{KEY}/next?cursor=1"))) })
    });
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    no_key(&e.to_string(), "refused next_url");
    assert_eq!(h.transport.request_count(), 1);
    for r in h.transport.requests() {
        assert!(!r.url.contains(KEY));
    }
}

#[test]
fn a_missing_key_is_a_not_authorized_error_and_sends_nothing() {
    struct NoKey;
    impl KeyProvider for NoKey {
        fn api_key(&self) -> Result<SecretString, KeyError> {
            Err(KeyError::Missing("MASSIVE_API_KEY".into()))
        }
    }
    let t = Arc::new(FakeTransport::new());
    let src = MassiveDataSource::new(NoKey, t.clone());
    let e = src.sleeve_data(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.code, "DATA_NOT_AUTHORIZED");
    assert!(e.message.contains("MASSIVE_API_KEY"), "{}", e.message);
    assert_eq!(t.request_count(), 0);
}

#[test]
fn the_env_key_provider_reads_the_variable_once_and_redacts_it() {
    let var = "MARKET_DATA_TEST_KEY_ENV_A";
    std::env::set_var(var, format!("  {KEY}  "));
    let p = EnvKeyProvider::from_var(var).unwrap();
    std::env::set_var(var, "changed-after-construction");
    assert_eq!(p.api_key().unwrap().expose(), KEY, "read at construction, trimmed, not re-read");
    no_key(&format!("{p:?}"), "EnvKeyProvider Debug");
    assert!(format!("{p:?}").contains(var), "the variable NAME may be shown");
    std::env::remove_var(var);

    assert_eq!(EnvKeyProvider::from_var("MARKET_DATA_TEST_KEY_ENV_UNSET").unwrap_err(), KeyError::Missing("MARKET_DATA_TEST_KEY_ENV_UNSET".into()));
    std::env::set_var("MARKET_DATA_TEST_KEY_ENV_B", "   ");
    assert_eq!(EnvKeyProvider::from_var("MARKET_DATA_TEST_KEY_ENV_B").unwrap_err(), KeyError::Empty);
    std::env::set_var("MARKET_DATA_TEST_KEY_ENV_C", "abc\ndef");
    assert_eq!(EnvKeyProvider::from_var("MARKET_DATA_TEST_KEY_ENV_C").unwrap_err(), KeyError::NotHeaderSafe);
    assert_eq!(EnvKeyProvider::DEFAULT_VAR, "MASSIVE_API_KEY");
}

#[test]
fn an_env_key_reaches_the_request_header() {
    let var = "MARKET_DATA_TEST_KEY_ENV_D";
    std::env::set_var(var, KEY);
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(403, NOT_AUTHORIZED_MESSAGE_BODY);
    let src = MassiveDataSource::new(EnvKeyProvider::from_var(var).unwrap(), t.clone());
    let _ = src.sleeve_data(&crypto_sleeve(), as_of());
    assert_eq!(t.requests()[0].header("authorization"), Some(format!("Bearer {KEY}").as_str()));
    std::env::remove_var(var);
}
