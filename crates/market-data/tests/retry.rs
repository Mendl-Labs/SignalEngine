//! Transient failures are retried with bounded exponential backoff on the injected clock (nothing here sleeps for
//! real); 403 and malformed responses never are; the per-tick request budget caps the total.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broker_adapters::transport::{HttpResponse, TransportError};
use common::*;
use market_data::testing::{FixedJitter, NOT_AUTHORIZED_ERROR_BODY, NOT_AUTHORIZED_MESSAGE_BODY, RATE_LIMIT_BODY};
use market_data::{BudgetConfig, ErrorKind, FailureClass, MassiveConfig, MassiveDataSource, MassiveError, RetryPolicy, SleeveFetcher, StaticKeyProvider};

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// Serve `world` but answer the first `n` requests with `first` (a status and body) instead.
fn flaky(h: &Harness, n: usize, first: impl Fn(usize) -> Result<HttpResponse, TransportError> + Send + Sync + 'static) {
    let world = standard_world();
    let count = Arc::new(AtomicUsize::new(0));
    h.transport.set_handler(move |req| {
        let i = count.fetch_add(1, Ordering::SeqCst);
        if i < n {
            first(i)
        } else {
            Ok(HttpResponse { status: 200, body: world.respond(req) })
        }
    });
}

fn resp(status: u16, body: &str) -> Result<HttpResponse, TransportError> {
    Ok(HttpResponse { status, body: body.to_string() })
}

#[test]
fn a_429_then_success_backs_off_once_per_failure_and_succeeds() {
    let h = Harness::new(as_of());
    flaky(&h, 2, |i| if i == 0 { resp(429, "") } else { resp(429, RATE_LIMIT_BODY) });
    let data = h.crypto(as_of()).expect("succeeds on the third attempt");
    assert_eq!(bars_of(&data.panel, "BTC").last().unwrap().0, d(2020, 6, 16));
    assert_eq!(h.transport.request_count(), 4, "3 attempts for BTC + 1 for ETH");
    assert_eq!(h.clock.sleeps(), vec![ms(500), ms(1000)], "500 ms then 1 s (jitter factor 1.0), no real sleeping");
}

#[test]
fn a_429_that_never_clears_exhausts_the_attempts_and_reports_rate_limited() {
    let h = Harness::new(as_of());
    flaky(&h, usize::MAX, |_| resp(429, RATE_LIMIT_BODY));
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.error, MassiveError::RateLimited { attempts: 4, local_budget: false });
    assert_eq!(e.kind(), ErrorKind::RateLimited);
    assert_eq!(e.class(), FailureClass::Transient);
    assert_eq!(h.transport.request_count(), 4);
    assert_eq!(h.clock.sleeps(), vec![ms(500), ms(1000), ms(2000)], "no sleep after the final attempt");
    let de: rebalancer_run::data::DataError = e.into();
    assert_eq!(de.code, "DATA_RATE_LIMITED");
}

#[test]
fn backoff_is_capped() {
    let cfg = MassiveConfig { retry: RetryPolicy { max_attempts: 7, base_delay: Duration::from_secs(1), max_delay: Duration::from_secs(3) }, ..MassiveConfig::default() };
    let h = Harness::with_config(as_of(), cfg);
    flaky(&h, usize::MAX, |_| resp(429, ""));
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.error, MassiveError::RateLimited { attempts: 7, local_budget: false });
    assert_eq!(h.clock.sleeps(), vec![ms(1000), ms(2000), ms(3000), ms(3000), ms(3000), ms(3000)]);
}

#[test]
fn jitter_scales_each_delay_but_never_past_the_cap() {
    let cfg = MassiveConfig { retry: RetryPolicy { max_attempts: 5, base_delay: Duration::from_secs(1), max_delay: Duration::from_secs(3) }, ..MassiveConfig::default() };
    let transport = Arc::new(broker_adapters::testing::FakeTransport::new());
    let clock = Arc::new(market_data::testing::ManualMarketClock::at_run_slot(as_of()));
    let src = MassiveDataSource::new(StaticKeyProvider::new(KEY).unwrap(), transport.clone())
        .with_config(cfg)
        .unwrap()
        .with_clock(clock.clone())
        .with_jitter(Arc::new(FixedJitter(0.5)));
    transport.set_handler(|_| resp(503, ""));
    let _ = src.fetch_sleeve(&crypto_sleeve(), as_of());
    assert_eq!(clock.sleeps(), vec![ms(500), ms(1000), ms(1500), ms(1500)]);

    // a broken jitter source cannot push a delay past the cap
    let clock2 = Arc::new(market_data::testing::ManualMarketClock::at_run_slot(as_of()));
    let src2 = MassiveDataSource::new(StaticKeyProvider::new(KEY).unwrap(), transport.clone())
        .with_config(MassiveConfig { retry: RetryPolicy { max_attempts: 6, base_delay: Duration::from_secs(1), max_delay: Duration::from_secs(3) }, ..MassiveConfig::default() })
        .unwrap()
        .with_clock(clock2.clone())
        .with_jitter(Arc::new(FixedJitter(1000.0)));
    let _ = src2.fetch_sleeve(&crypto_sleeve(), as_of());
    assert!(clock2.sleeps().iter().all(|d| *d <= Duration::from_secs(3)), "{:?}", clock2.sleeps());
}

#[test]
fn server_errors_are_retried_then_reported_unavailable() {
    for status in [500u16, 502, 503, 504, 599] {
        let h = Harness::new(as_of());
        flaky(&h, 2, move |_| resp(status, "upstream trouble"));
        assert!(h.crypto(as_of()).is_ok(), "HTTP {status} twice then success");
        assert_eq!(h.clock.sleeps(), vec![ms(500), ms(1000)], "HTTP {status}");

        let h = Harness::new(as_of());
        flaky(&h, usize::MAX, move |_| resp(status, "upstream trouble"));
        let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
        assert!(matches!(e.error, MassiveError::Unavailable { attempts: 4, .. }), "HTTP {status}: {e}");
        assert_eq!(e.class(), FailureClass::Transient);
        assert_eq!(h.transport.request_count(), 4);
    }
}

#[test]
fn timeouts_and_connection_failures_are_retried() {
    for err in [TransportError::Timeout, TransportError::ConnectFailed("dns".into()), TransportError::Io("reset".into())] {
        let h = Harness::new(as_of());
        let e2 = err.clone();
        flaky(&h, 1, move |_| Err(e2.clone()));
        assert!(h.crypto(as_of()).is_ok(), "{err}");
        assert_eq!(h.clock.sleeps(), vec![ms(500)]);

        let h = Harness::new(as_of());
        let e3 = err.clone();
        flaky(&h, usize::MAX, move |_| Err(e3.clone()));
        let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Unavailable, "{err}");
        assert_eq!(h.transport.request_count(), 4);
    }
}

#[test]
fn a_403_of_either_body_shape_is_never_retried() {
    for (shape, body) in [("message", NOT_AUTHORIZED_MESSAGE_BODY), ("error", NOT_AUTHORIZED_ERROR_BODY), ("empty", ""), ("html", "<h1>Forbidden</h1>")] {
        let h = Harness::new(as_of());
        h.transport.enqueue_json(403, body);
        let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::NotAuthorized, "{shape}");
        assert_eq!(e.class(), FailureClass::Deterministic);
        assert!(matches!(e.error, MassiveError::NotAuthorized { status: 403, .. }));
        assert_eq!(h.transport.request_count(), 1, "{shape}: one request only");
        assert!(h.clock.sleeps().is_empty(), "{shape}: no backoff");
        let de: rebalancer_run::data::DataError = e.into();
        assert_eq!(de.code, "DATA_NOT_AUTHORIZED");
        if body.contains("not entitled") {
            assert!(de.message.contains("not entitled"), "{}", de.message);
        }
    }
}

#[test]
fn a_401_is_not_authorized_and_not_retried() {
    let h = Harness::new(as_of());
    h.transport.enqueue_json(401, r#"{"status":"ERROR","error":"Unknown API Key"}"#);
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert!(matches!(e.error, MassiveError::NotAuthorized { status: 401, .. }));
    assert_eq!(h.transport.request_count(), 1);
}

#[test]
fn a_403_after_a_429_stops_immediately() {
    let h = Harness::new(as_of());
    h.transport.enqueue_json(429, "");
    h.transport.enqueue_json(403, NOT_AUTHORIZED_MESSAGE_BODY);
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotAuthorized);
    assert_eq!(h.transport.request_count(), 2);
    assert_eq!(h.clock.sleeps(), vec![ms(500)]);
}

#[test]
fn malformed_bodies_are_not_retried_even_when_the_status_is_200() {
    let h = Harness::new(as_of());
    h.transport.enqueue_json(200, "not json");
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Malformed);
    assert_eq!(h.transport.request_count(), 1);
}

#[test]
fn a_max_attempts_of_one_never_retries() {
    let cfg = MassiveConfig { retry: RetryPolicy { max_attempts: 1, ..RetryPolicy::default() }, ..MassiveConfig::default() };
    let h = Harness::with_config(as_of(), cfg);
    flaky(&h, usize::MAX, |_| resp(503, ""));
    assert!(h.src.fetch_sleeve(&crypto_sleeve(), as_of()).is_err());
    assert_eq!(h.transport.request_count(), 1);
    assert!(h.clock.sleeps().is_empty());
}

#[test]
fn the_request_budget_caps_all_requests_and_refuses_locally() {
    let cfg = MassiveConfig { budget: BudgetConfig { max_requests: 3, window: Duration::from_secs(60) }, ..MassiveConfig::default() };
    let h = Harness::with_config(as_of(), cfg);
    h.serve(standard_world());
    // the crypto sleeve needs 2 requests: fine
    assert!(h.crypto(as_of()).is_ok());
    assert_eq!(h.transport.request_count(), 2);
    // a second crypto sleeve fetch: BTC takes the 3rd request, ETH finds the budget spent
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.error, MassiveError::RateLimited { attempts: 0, local_budget: true });
    assert_eq!(h.transport.request_count(), 3, "the refused request was never sent");
    // a new window
    h.clock.set(at(as_of(), 0, 12));
    assert!(h.crypto(as_of()).is_ok());
}

#[test]
fn retries_spend_the_budget_too() {
    let cfg = MassiveConfig {
        budget: BudgetConfig { max_requests: 3, window: Duration::from_secs(3600) },
        retry: RetryPolicy { max_attempts: 10, base_delay: Duration::from_millis(1), max_delay: Duration::from_millis(1) },
        ..MassiveConfig::default()
    };
    let h = Harness::with_config(as_of(), cfg);
    flaky(&h, usize::MAX, |_| resp(429, ""));
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.error, MassiveError::RateLimited { attempts: 3, local_budget: true });
    assert_eq!(h.transport.request_count(), 3, "a throttling vendor is not hammered past the budget");
}

#[test]
fn invalid_configuration_is_refused_at_construction() {
    let t = Arc::new(broker_adapters::testing::FakeTransport::new());
    let build = |cfg: MassiveConfig| MassiveDataSource::new(StaticKeyProvider::new(KEY).unwrap(), t.clone()).with_config(cfg);
    assert!(build(MassiveConfig::default()).is_ok());
    assert!(build(MassiveConfig { base_url: "http://api.massive.com".into(), ..MassiveConfig::default() }).is_err(), "no clear-text key");
    assert!(build(MassiveConfig { base_url: "https://api.massive.com/v2".into(), ..MassiveConfig::default() }).is_err());
    assert!(build(MassiveConfig { retry: RetryPolicy { max_attempts: 0, ..RetryPolicy::default() }, ..MassiveConfig::default() }).is_err());
    assert!(build(MassiveConfig { max_pages: 0, ..MassiveConfig::default() }).is_err());
    assert!(build(MassiveConfig { budget: BudgetConfig { max_requests: 0, window: Duration::from_secs(1) }, ..MassiveConfig::default() }).is_err());
    assert!(build(MassiveConfig { crypto_history_days: 99, ..MassiveConfig::default() }).is_err());
    assert!(build(MassiveConfig { etf_history_days: 100, ..MassiveConfig::default() }).is_err());
}

#[test]
fn a_response_served_from_an_intermediary_cache_is_not_trusted() {
    use broker_adapters::transport::HttpResponseDetailed;
    // An `Age` header above 60 s says a cache answered: retried (the request asks for revalidation), never used.
    let h = Harness::new(as_of());
    let world = standard_world();
    let n = Arc::new(AtomicUsize::new(0));
    let n2 = n.clone();
    h.transport.set_handler_detailed(move |req| {
        let i = n2.fetch_add(1, Ordering::SeqCst);
        let headers = match i {
            0 => vec![("Age".to_string(), "3600".to_string())],
            1 => vec![("age".to_string(), "5".to_string())], // a small Age is fine
            _ => vec![],
        };
        Ok(HttpResponseDetailed { status: 200, body: world.respond(req), headers })
    });
    assert!(h.crypto(as_of()).is_ok());
    assert_eq!(h.transport.request_count(), 3, "BTC twice (the first was cache-served), ETH once");
    assert_eq!(h.clock.sleeps(), vec![ms(500)]);

    // always cache-served: gives up as unavailable rather than returning the stale answer
    let h = Harness::new(as_of());
    let world = standard_world();
    h.transport.set_handler_detailed(move |req| Ok(HttpResponseDetailed { status: 200, body: world.respond(req), headers: vec![("Age".to_string(), "61".to_string())] }));
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Unavailable);
    // exactly 60 is the boundary: accepted
    let h = Harness::new(as_of());
    let world = standard_world();
    h.transport.set_handler_detailed(move |req| Ok(HttpResponseDetailed { status: 200, body: world.respond(req), headers: vec![("Age".to_string(), "60".to_string())] }));
    assert!(h.crypto(as_of()).is_ok());
}
