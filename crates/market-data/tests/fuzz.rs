//! Total-function fuzz: whatever bytes the vendor (or a proxy in front of it) sends, the source answers with data or a
//! typed error, never a panic; and every `Ok` satisfies the data contract. Deterministic seeds, no external fuzzer.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use broker_adapters::transport::{HttpResponse, TransportError};
use chrono::Duration;
use common::*;
use market_data::testing::{crypto_ts, page_json};
use market_data::{BudgetConfig, MassiveConfig, RetryPolicy};
use rebalancer_run::data::DataSource;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

/// A harness whose budget never runs out and whose retries are cheap.
fn fuzz_harness() -> Harness {
    let cfg = MassiveConfig {
        budget: BudgetConfig { max_requests: u32::MAX, window: std::time::Duration::from_secs(1) },
        retry: RetryPolicy { max_attempts: 2, base_delay: std::time::Duration::from_millis(1), max_delay: std::time::Duration::from_millis(2) },
        ..MassiveConfig::default()
    };
    Harness::with_config(as_of(), cfg)
}

/// The data contract an `Ok` must meet.
fn check_contract(data: &rebalancer_run::data::SleeveData) {
    for s in data.panel.iter() {
        assert!(s.last_date() < as_of(), "{}: a bar at or after the run date", s.symbol());
        for w in s.dates().windows(2) {
            assert!(w[0] < w[1], "{}: not ascending", s.symbol());
        }
        assert!(s.closes().iter().all(|c| c.is_finite() && *c > 0.0), "{}: bad close", s.symbol());
    }
}

#[test]
fn arbitrary_bytes_and_statuses_never_panic() {
    let h = fuzz_harness();
    let rng = std::sync::Mutex::new(Rng(0xDEAD_BEEF_CAFE_F00D));
    h.transport.set_handler(move |_| {
        let mut r = rng.lock().unwrap();
        let n = r.below(200);
        let bytes: Vec<u8> = (0..n).map(|_| r.next() as u8).collect();
        let status = [200u16, 200, 200, 429, 500, 403, 404, 0, 999, 204][r.below(10)];
        if r.below(20) == 0 {
            return Err(TransportError::Timeout);
        }
        Ok(HttpResponse { status, body: String::from_utf8_lossy(&bytes).into_owned() })
    });
    for _ in 0..800 {
        let _ = h.src.sleeve_data(&etf_sleeve(), as_of());
        let _ = h.src.sleeve_data(&crypto_sleeve(), as_of());
    }
}

#[test]
fn arbitrary_json_shaped_bodies_never_panic() {
    let h = fuzz_harness();
    let pieces = [
        r#"{"#, r#"}"#, r#"["#, r#"]"#, r#","#, r#":"#, r#""status":"OK""#, r#""ticker":"SPY""#, r#""ticker":"X:BTCUSD""#, r#""adjusted":true"#, r#""resultsCount":3"#,
        r#""results":"#, r#""t":1"#, r#""c":1.5"#, r#""c":-1"#, r#""c":"x""#, r#""next_url":"https://api.massive.com/v2/x?cursor=1&apiKey=k""#, r#""next_url":5"#, "null", "true", "1e999", "-0",
        "\u{0}", "\u{feff}", "18446744073709551616", "9223372036854775808",
    ];
    let rng = std::sync::Mutex::new(Rng(0x1234_5678_9ABC_DEF1));
    h.transport.set_handler(move |_| {
        let mut r = rng.lock().unwrap();
        let n = 1 + r.below(14);
        let body: String = (0..n).map(|_| pieces[r.below(pieces.len())]).collect::<Vec<_>>().join(if r.below(2) == 0 { "" } else { "," });
        Ok(HttpResponse { status: 200, body })
    });
    for _ in 0..1500 {
        let _ = h.src.sleeve_data(&etf_sleeve(), as_of());
        let _ = h.src.sleeve_data(&crypto_sleeve(), as_of());
    }
}

/// Take a VALID response and damage it at random: whatever comes back as `Ok` must still meet the contract.
#[test]
fn mutated_valid_responses_never_panic_and_any_success_meets_the_contract() {
    let h = fuzz_harness();
    let world = standard_world();
    let rng = std::sync::Mutex::new(Rng(0x0BAD_C0DE_1357_9BDF));
    let oks = Arc::new(AtomicUsize::new(0));
    h.transport.set_handler(move |req| {
        let (ticker, from, _) = parse_range_url(&req.url).unwrap();
        let mut body = world.respond(req).into_bytes();
        let mut r = rng.lock().unwrap();
        let _ = (ticker, from);
        for _ in 0..r.below(4) {
            if body.is_empty() {
                break;
            }
            let i = r.below(body.len());
            match r.below(4) {
                0 => body[i] = r.next() as u8,
                1 => {
                    body.remove(i);
                }
                2 => body.insert(i, r.next() as u8),
                _ => body.truncate(i),
            }
        }
        Ok(HttpResponse { status: 200, body: String::from_utf8_lossy(&body).into_owned() })
    });
    for _ in 0..250 {
        for sleeve in [etf_sleeve(), crypto_sleeve()] {
            if let Ok(data) = h.src.sleeve_data(&sleeve, as_of()) {
                check_contract(&data);
                oks.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    assert!(oks.load(Ordering::SeqCst) > 0, "some mutations are harmless (they hit ignored fields), so some fetches succeed");
}

#[test]
fn random_timestamps_and_closes_never_panic_and_any_success_meets_the_contract() {
    let h = fuzz_harness();
    let rng = std::sync::Mutex::new(Rng(0x5555_AAAA_1111_7777));
    h.transport.set_handler(move |req| {
        let mut r = rng.lock().unwrap();
        let (ticker, _, _) = parse_range_url(&req.url).unwrap();
        let n = r.below(160);
        let mut t: i64 = crypto_ts(as_of() - Duration::days(150));
        let bars: Vec<(i64, f64)> = (0..n)
            .map(|_| {
                let step = [0i64, 86_400_000, 86_400_000, 86_400_000, 2 * 86_400_000, -86_400_000, 1, i64::MAX / 2][r.below(8)];
                t = t.saturating_add(step);
                let c = [1.5, 100.0, 0.0, -2.0, f64::MIN_POSITIVE, f64::MAX, 1e-300][r.below(7)];
                (t, c)
            })
            .collect();
        Ok(HttpResponse { status: 200, body: page_json(&ticker, &bars, None) })
    });
    for _ in 0..400 {
        for sleeve in [etf_sleeve(), crypto_sleeve()] {
            if let Ok(data) = h.src.sleeve_data(&sleeve, as_of()) {
                check_contract(&data);
            }
        }
    }
}

#[test]
fn hostile_next_urls_never_panic_and_never_leak_the_key_into_a_request() {
    let h = fuzz_harness();
    let world = standard_world();
    let rng = std::sync::Mutex::new(Rng(0x0F0F_F0F0_1234_4321));
    let parts = ["https://", "api.massive.com", "/v2/aggs", "?", "&", "=", "cursor", "apiKey", "APIKEY", KEY, "%", "%41", "#", "@", ":", "//", " ", "\n", "\u{1F600}", "..", "/"];
    h.transport.set_handler(move |req| {
        let mut r = rng.lock().unwrap();
        let (ticker, from, _) = match parse_range_url(&req.url) {
            Some(x) => x,
            None => ("X:BTCUSD".to_string(), as_of() - Duration::days(150), as_of()),
        };
        let bars: Vec<(i64, f64)> = world.of(&ticker).iter().filter(|(x, _)| *x >= from && *x < as_of()).map(|(x, c)| (crypto_ts(*x), *c)).collect();
        let n = 1 + r.below(10);
        let next: String = (0..n).map(|_| parts[r.below(parts.len())]).collect();
        let next_json = serde_json::to_string(&next).unwrap();
        let body = page_json(&ticker, &bars, None).replacen("\"results\"", &format!("\"next_url\":{next_json},\"results\""), 1);
        Ok(HttpResponse { status: 200, body })
    });
    for _ in 0..1000 {
        let _ = h.src.sleeve_data(&crypto_sleeve(), as_of());
    }
    for r in h.transport.requests() {
        assert!(!r.url.contains(KEY), "{}", r.url);
        assert!(r.url.starts_with("https://api.massive.com/"), "{}", r.url);
    }
}
