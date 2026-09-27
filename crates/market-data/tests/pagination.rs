//! `next_url` pagination: a vendor-supplied URL is never followed as given. Any key parameter is stripped, the key is
//! re-sent as the Bearer header, foreign hosts are refused, and the number of pages is capped.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use broker_adapters::transport::{HttpRequest, HttpResponse};
use chrono::Duration;
use common::*;
use market_data::testing::{crypto_ts, page_json};
use market_data::{ErrorKind, MassiveConfig, MassiveError, SleeveFetcher};

fn is_followup(url: &str) -> bool {
    url.contains("cursor=")
}

fn ticker_of(url: &str) -> &'static str {
    if url.contains("X:ETHUSD") {
        "X:ETHUSD"
    } else {
        "X:BTCUSD"
    }
}

/// The bars of `ticker` dated in `[lo, hi)` days before the run date, as (t, close).
fn slice(world: &World, ticker: &str, lo_days_back: i64, hi_days_back: i64) -> Vec<(i64, f64)> {
    world.of(ticker).iter().filter(|(x, _)| *x >= as_of() - Duration::days(lo_days_back) && *x < as_of() - Duration::days(hi_days_back)).map(|(x, c)| (crypto_ts(*x), *c)).collect()
}

/// A vendor that splits each coin's 150 bars in two pages of 75. The first page's `next_url` is `next_url_for(ticker)`;
/// the second page is served for any URL carrying a `cursor`. `hook` may replace a response (it sees the request and the
/// running count of follow-up requests).
fn two_page_vendor(h: &Harness, next_url_for: impl Fn(&str) -> String + Send + Sync + 'static, hook: impl Fn(&HttpRequest, usize) -> Option<HttpResponse> + Send + Sync + 'static) {
    let world = standard_world();
    let followups = AtomicUsize::new(0);
    h.transport.set_handler(move |req: &HttpRequest| {
        let n = if is_followup(&req.url) { followups.fetch_add(1, Ordering::SeqCst) } else { 0 };
        if let Some(r) = hook(req, n) {
            return Ok(r);
        }
        let ticker = ticker_of(&req.url);
        if is_followup(&req.url) {
            Ok(HttpResponse { status: 200, body: page_json(ticker, &slice(&world, ticker, 75, 0), None) })
        } else {
            Ok(HttpResponse { status: 200, body: page_json(ticker, &slice(&world, ticker, 150, 75), Some(&next_url_for(ticker))) })
        }
    });
}

fn no_hook(_: &HttpRequest, _: usize) -> Option<HttpResponse> {
    None
}

fn next_with_key(t: &str) -> String {
    format!("https://api.massive.com/v2/aggs/ticker/{t}/range/1/day/2020-01-01/2020-06-16?cursor=YWJjZGVm&apiKey={KEY}")
}

#[test]
fn a_next_url_carrying_the_key_is_scrubbed_and_the_key_is_sent_as_the_bearer_header() {
    let h = Harness::new(as_of());
    two_page_vendor(&h, next_with_key, no_hook);
    let fetched = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).expect("both pages are stitched");
    assert_eq!(fetched.panel.get("BTC").unwrap().len(), 150);

    let reqs = h.transport.requests();
    assert_eq!(reqs.len(), 4, "two pages per coin");
    for r in &reqs {
        assert!(!r.url.contains(KEY) && !r.url.to_ascii_lowercase().contains("apikey"), "no key in any URL: {}", r.url);
        assert_eq!(r.header("authorization"), Some(format!("Bearer {KEY}").as_str()), "the key is re-added as the Bearer header on every page");
    }
    assert_eq!(reqs[1].url, "https://api.massive.com/v2/aggs/ticker/X:BTCUSD/range/1/day/2020-01-01/2020-06-16?cursor=YWJjZGVm", "the cursor is kept, the key parameter dropped");

    let p = &fetched.provenance[0];
    assert!(p.next_url_scrubbed, "the removal is recorded");
    assert_eq!(p.request_paths.len(), 2);
    assert_eq!(p.request_paths[1], "/v2/aggs/ticker/X:BTCUSD/range/1/day/2020-01-01/2020-06-16?cursor=<elided>");
    assert_eq!(p.raw_sha256.len(), 2);
    assert!(!format!("{p:?}").contains(KEY));
}

#[test]
fn the_key_parameter_is_removed_in_any_case_and_position() {
    for variant in [
        format!("https://api.massive.com/v2/x/@T@?apiKey={KEY}&cursor=c1"),
        format!("https://api.massive.com/v2/x/@T@?cursor=c1&APIKEY={KEY}"),
        format!("https://api.massive.com/v2/x/@T@?cursor=c1&api_key={KEY}&limit=10"),
        format!("https://api.massive.com/v2/x/@T@?cursor=c1&extra={KEY}"),
    ] {
        let h = Harness::new(as_of());
        let v = variant.clone();
        two_page_vendor(&h, move |t| v.replace("@T@", t), no_hook);
        h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap();
        for r in h.transport.requests() {
            assert!(!r.url.contains(KEY), "{variant} -> {}", r.url);
        }
        assert!(h.transport.requests()[1].url.contains("cursor=c1"));
    }
}

#[test]
fn a_next_url_without_a_key_is_followed_unchanged() {
    let h = Harness::new(as_of());
    two_page_vendor(&h, |t| format!("https://api.massive.com/v2/aggs/ticker/{t}/range/1/day/2020-01-01/2020-06-16?cursor=YWJj"), no_hook);
    let fetched = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap();
    assert!(!fetched.provenance[0].next_url_scrubbed);
    assert_eq!(h.transport.requests()[1].url, "https://api.massive.com/v2/aggs/ticker/X:BTCUSD/range/1/day/2020-01-01/2020-06-16?cursor=YWJj");
}

#[test]
fn a_next_url_to_another_host_is_never_followed_and_never_receives_the_key() {
    for evil in [
        "https://evil.example/v2/aggs?cursor=1",
        "http://api.massive.com/v2/aggs?cursor=1",
        "https://api.massive.com.evil.example/v2/aggs?cursor=1",
        "https://api.massive.com@evil.example/v2/aggs?cursor=1",
        "https://api.massive.com:8443/v2/aggs?cursor=1",
        "//evil.example/x?cursor=1",
        "/relative/path?cursor=1",
        "javascript:alert(1)",
    ] {
        let h = Harness::new(as_of());
        let e = evil.to_string();
        two_page_vendor(&h, move |_| e.clone(), no_hook);
        let err = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Malformed, "{evil}");
        assert_eq!(h.transport.request_count(), 1, "{evil}: nothing was requested after the refused URL");
        assert!(h.transport.requests().iter().all(|r| r.url.starts_with("https://api.massive.com/")), "{evil}");
    }
}

#[test]
fn a_next_url_that_leads_back_to_a_fetched_page_is_a_loop() {
    let h = Harness::new(as_of());
    let world = standard_world();
    h.transport.set_handler(move |req| {
        let ticker = ticker_of(&req.url);
        // the "next" page is the same URL as this one (which carries no cursor, so add one to make it a valid next_url)
        let me = req.url.clone();
        Ok(HttpResponse { status: 200, body: page_json(ticker, &slice(&world, ticker, 150, 0), Some(&me)) })
    });
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Malformed);
    assert_eq!(h.transport.request_count(), 1, "the repeated URL is not requested again");
}

#[test]
fn the_page_count_is_capped() {
    let cfg = MassiveConfig { max_pages: 3, ..MassiveConfig::default() };
    let h = Harness::with_config(as_of(), cfg);
    let n = Arc::new(AtomicUsize::new(0));
    let n2 = n.clone();
    h.transport.set_handler(move |req| {
        let i = n2.fetch_add(1, Ordering::SeqCst);
        // every page has a fresh cursor and one more day of (ascending) data, forever
        let day = as_of() - Duration::days(150 - i as i64);
        let ticker = ticker_of(&req.url);
        let next = format!("https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/day/2020-01-01/2020-06-16?cursor=p{i}");
        Ok(HttpResponse { status: 200, body: page_json(ticker, &[(crypto_ts(day), 10.0 + i as f64)], Some(&next)) })
    });
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Malformed);
    match &e.error {
        MassiveError::Malformed { detail, .. } => assert!(detail.contains("more than 3 pages"), "{detail}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(h.transport.request_count(), 3, "exactly max_pages requests, then refusal");
}

#[test]
fn pages_must_stay_ascending_across_the_boundary() {
    // page 2 starts one day BEFORE the end of page 1: a repeated date
    let h = Harness::new(as_of());
    let world = standard_world();
    h.transport.set_handler(move |req| {
        let t = ticker_of(&req.url);
        if is_followup(&req.url) {
            Ok(HttpResponse { status: 200, body: page_json(t, &slice(&world, t, 76, 0), None) })
        } else {
            Ok(HttpResponse { status: 200, body: page_json(t, &slice(&world, t, 150, 75), Some(&format!("https://api.massive.com/v2/aggs/ticker/{t}/range/1/day/2020-01-01/2020-06-16?cursor=z"))) })
        }
    });
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Malformed);
}

#[test]
fn a_later_page_for_another_ticker_is_refused() {
    let h = Harness::new(as_of());
    let world = standard_world();
    h.transport.set_handler(move |req| {
        if is_followup(&req.url) {
            Ok(HttpResponse { status: 200, body: page_json("X:ETHUSD", &slice(&world, "X:ETHUSD", 75, 0), None) })
        } else {
            let t = ticker_of(&req.url);
            Ok(HttpResponse { status: 200, body: page_json(t, &slice(&world, t, 150, 75), Some("https://api.massive.com/v2/aggs/ticker/X:BTCUSD/range/1/day/2020-01-01/2020-06-16?cursor=z")) })
        }
    });
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Malformed);
}

#[test]
fn retries_apply_to_later_pages_too() {
    let h = Harness::new(as_of());
    two_page_vendor(
        &h,
        |t| format!("https://api.massive.com/v2/aggs/ticker/{t}/range/1/day/2020-01-01/2020-06-16?cursor=YWJj"),
        |req, n| if is_followup(&req.url) && n == 0 { Some(HttpResponse { status: 429, body: String::new() }) } else { None },
    );
    assert!(h.crypto(as_of()).is_ok());
    assert_eq!(h.clock.sleeps().len(), 1, "one backoff, for the throttled second page");
}
