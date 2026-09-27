//! Response validation: everything the vendor could send that is not a valid daily-aggregates page must come back as a
//! typed error, once (no retry), never as data.

mod common;

use broker_adapters::transport::HttpResponse;
use chrono::Duration;
use common::*;
use market_data::testing::{crypto_ts, empty_page_json, page_json, stock_ts};
use market_data::{ErrorKind, MassiveError, SleeveFetcher};

/// Valid SPY bars (New York midnight stamps) from 2020-05-01 to 2020-06-16.
fn spy_bars() -> Vec<(i64, f64)> {
    let w = standard_world();
    w.of("SPY").iter().filter(|(x, _)| *x >= d(2020, 5, 1) && *x < as_of()).map(|(x, c)| (stock_ts(*x), *c)).collect()
}

fn btc_bars() -> Vec<(i64, f64)> {
    let w = standard_world();
    w.of("X:BTCUSD").iter().filter(|(x, _)| *x >= as_of() - Duration::days(150) && *x < as_of()).map(|(x, c)| (crypto_ts(*x), *c)).collect()
}

/// Serve exactly ONE scripted 200 response and fetch the ETF sleeve: the first instrument is SPY.
fn etf_with(body: &str) -> (Harness, Result<market_data::FetchedSleeve, market_data::SleeveError>) {
    let h = Harness::new(as_of());
    h.transport.enqueue_json(200, body);
    let r = h.src.fetch_sleeve(&etf_sleeve(), as_of());
    (h, r)
}

fn crypto_with(body: &str) -> (Harness, Result<market_data::FetchedSleeve, market_data::SleeveError>) {
    let h = Harness::new(as_of());
    h.transport.enqueue_json(200, body);
    let r = h.src.fetch_sleeve(&crypto_sleeve(), as_of());
    (h, r)
}

fn assert_malformed_once(h: &Harness, r: Result<market_data::FetchedSleeve, market_data::SleeveError>, why: &str) {
    let e = r.expect_err(why);
    assert_eq!(e.kind(), ErrorKind::Malformed, "{why}: {e}");
    assert_eq!(h.transport.request_count(), 1, "{why}: malformed is never retried");
    assert!(h.clock.sleeps().is_empty(), "{why}: no backoff for a deterministic failure");
}

#[test]
fn dates_that_are_not_strictly_ascending_are_refused() {
    let mut swapped = spy_bars();
    swapped.swap(3, 4);
    let (h, r) = etf_with(&page_json("SPY", &swapped, None));
    assert_malformed_once(&h, r, "swapped bars");
    match etf_with(&page_json("SPY", &swapped, None)).1.unwrap_err().error {
        MassiveError::Malformed { detail, .. } => assert!(detail.contains("ascending"), "{detail}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_duplicate_date_is_refused() {
    let mut dup = spy_bars();
    dup.insert(5, dup[5]);
    let (h, r) = etf_with(&page_json("SPY", &dup, None));
    assert_malformed_once(&h, r, "duplicate bar");
    let mut dup_btc = btc_bars();
    dup_btc.push(*dup_btc.last().unwrap());
    let (h, r) = crypto_with(&page_json("X:BTCUSD", &dup_btc, None));
    assert_malformed_once(&h, r, "duplicate crypto bar");
}

#[test]
fn a_descending_series_is_refused() {
    let mut rev = btc_bars();
    rev.reverse();
    let (h, r) = crypto_with(&page_json("X:BTCUSD", &rev, None));
    assert_malformed_once(&h, r, "descending");
}

#[test]
fn bad_closes_are_refused() {
    for (why, bad) in [("zero", 0.0), ("negative", -3.5), ("negative zero", -0.0), ("nan", f64::NAN), ("infinity", f64::INFINITY), ("neg infinity", f64::NEG_INFINITY)] {
        let mut bars = spy_bars();
        bars[7].1 = bad;
        let (h, r) = etf_with(&page_json("SPY", &bars, None));
        assert_malformed_once(&h, r, why);
    }
    // a bad close on the FORMING bar (which would be dropped) still poisons the response
    let mut bars = btc_bars();
    bars.push((crypto_ts(as_of()), -1.0));
    let (h, r) = crypto_with(&page_json("X:BTCUSD", &bars, None));
    assert_malformed_once(&h, r, "bad forming bar");
}

#[test]
fn json_level_garbage_in_a_bar_is_refused() {
    let good = page_json("SPY", &spy_bars(), None);
    let c0 = format!(r#""c":{}"#, spy_bars()[0].1);
    assert!(good.contains(&c0));
    for (why, body) in [
        ("string close", good.replacen(&c0, r#""c":"12.5""#, 1)),
        ("null close", good.replacen(&c0, r#""c":null"#, 1)),
        ("huge close", good.replacen(&c0, r#""c":1e999"#, 1)),
        ("missing close", good.replacen(&c0, r#""x":1"#, 1)),
        ("array close", good.replacen(&c0, r#""c":[1]"#, 1)),
    ] {
        let (h, r) = etf_with(&body);
        assert_malformed_once(&h, r, why);
    }
}

#[test]
fn the_wrong_symbol_is_refused() {
    let (h, r) = etf_with(&page_json("QQQ", &spy_bars(), None));
    assert_malformed_once(&h, r, "QQQ served for SPY");
    let (h, r) = crypto_with(&page_json("X:ETHUSD", &btc_bars(), None));
    assert_malformed_once(&h, r, "ETH served for BTC");
    let (h, r) = crypto_with(&page_json("BTC", &btc_bars(), None));
    assert_malformed_once(&h, r, "bare BTC served for X:BTCUSD");
    let (h, r) = etf_with(&page_json("spy", &spy_bars(), None));
    assert_malformed_once(&h, r, "case-different symbol");
    // the ticker field missing entirely
    let no_ticker = page_json("SPY", &spy_bars(), None).replacen(r#""ticker":"SPY","#, "", 1);
    let (h, r) = etf_with(&no_ticker);
    assert_malformed_once(&h, r, "no ticker");
}

#[test]
fn a_symbol_swapped_in_a_later_instrument_is_refused_too() {
    // SPY fine, then EFA's request answered with IEF's page.
    let w = standard_world();
    let h = Harness::new(as_of());
    let bars = |s: &str| -> Vec<(i64, f64)> { w.of(s).iter().filter(|(x, _)| *x >= d(2019, 3, 1) && *x < as_of()).map(|(x, c)| (stock_ts(*x), *c)).collect() };
    h.transport.enqueue_json(200, &page_json("SPY", &bars("SPY"), None));
    h.transport.enqueue_json(200, &page_json("IEF", &bars("IEF"), None));
    let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Malformed);
    assert_eq!(e.error.instrument(), Some("EFA"));
}

#[test]
fn status_adjusted_and_counts_are_checked() {
    let good = page_json("SPY", &spy_bars(), None);
    for (why, body) in [
        ("status ERROR", good.replace(r#""status":"OK""#, r#""status":"ERROR""#)),
        ("status NOT_AUTHORIZED", good.replace(r#""status":"OK""#, r#""status":"NOT_AUTHORIZED""#)),
        ("status missing", good.replace(r#""status":"OK","#, "")),
        ("unadjusted", good.replace(r#""adjusted":true"#, r#""adjusted":false"#)),
        ("adjusted missing", good.replace(r#""adjusted":true,"#, "")),
        ("resultsCount too big", good.replacen(&format!(r#""resultsCount":{}"#, spy_bars().len()), &format!(r#""resultsCount":{}"#, spy_bars().len() + 1), 1)),
        ("resultsCount too small", good.replacen(&format!(r#""resultsCount":{}"#, spy_bars().len()), r#""resultsCount":1"#, 1)),
        ("results not an array", good.replacen(r#""results":["#, r#""results":{"a":["#, 1).replacen("}]}", "}]}}", 1)),
    ] {
        let (h, r) = etf_with(&body);
        assert_malformed_once(&h, r, why);
    }
}

#[test]
fn delayed_status_is_valid() {
    // the delayed stocks plan reports DELAYED on aggregates
    let mut world = standard_world();
    world.honor_to = true;
    let h = Harness::new(as_of());
    h.transport.set_handler(move |req| {
        let body = world.respond(req).replace(r#""status":"OK""#, r#""status":"DELAYED""#);
        Ok(HttpResponse { status: 200, body })
    });
    assert!(h.etf(as_of()).is_ok());
}

#[test]
fn not_json_bodies_are_refused() {
    for (why, body) in [("empty", ""), ("html", "<html>502</html>"), ("truncated", r#"{"ticker":"SPY","status":"OK","resu"#), ("array", "[]"), ("null", "null"), ("number", "7"), ("bare string", r#""ok""#)] {
        let (h, r) = etf_with(body);
        assert_malformed_once(&h, r, why);
    }
}

#[test]
fn unexpected_http_statuses_are_refused_once() {
    for status in [204u16, 301, 302, 400, 404, 405, 418, 422] {
        let h = Harness::new(as_of());
        h.transport.enqueue_json(status, "{}");
        let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Malformed, "HTTP {status}");
        assert_eq!(h.transport.request_count(), 1, "HTTP {status} is not retried");
    }
}

#[test]
fn empty_results_are_insufficient_history_not_a_panel() {
    let (h, r) = etf_with(&empty_page_json("SPY"));
    let e = r.unwrap_err();
    assert_eq!(e.error, MassiveError::InsufficientHistory { symbol: "SPY".into(), needed: 10, have: 0 });
    assert_eq!(h.transport.request_count(), 1);
    let (_, r) = crypto_with(&empty_page_json("X:BTCUSD"));
    assert_eq!(r.unwrap_err().error, MassiveError::InsufficientHistory { symbol: "BTC".into(), needed: 100, have: 0 });
    // `"results":[]` and a missing results key with a zero count are both empty
    let (_, r) = etf_with(r#"{"ticker":"SPY","queryCount":0,"resultsCount":0,"adjusted":true,"status":"OK","results":[]}"#);
    assert_eq!(r.unwrap_err().kind(), ErrorKind::InsufficientHistory);
}

#[test]
fn a_response_that_is_only_forming_bars_is_insufficient_not_data() {
    // The vendor sends only today's bar: after dropping it nothing is left.
    let (_, r) = crypto_with(&page_json("X:BTCUSD", &[(crypto_ts(as_of()), 9000.0)], None));
    assert_eq!(r.unwrap_err().kind(), ErrorKind::InsufficientHistory);
}

#[test]
fn a_timestamp_outside_any_convention_is_refused() {
    let mut bars = btc_bars();
    bars[2].0 += 1; // one millisecond past UTC midnight
    let (h, r) = crypto_with(&page_json("X:BTCUSD", &bars, None));
    assert_malformed_once(&h, r, "off-by-a-millisecond stamp");
    let mut bars = spy_bars();
    bars[2].0 += Duration::hours(1).num_milliseconds();
    let (h, r) = etf_with(&page_json("SPY", &bars, None));
    assert_malformed_once(&h, r, "an hour past New York midnight");
    let mut bars = btc_bars();
    bars[0].0 = i64::MAX;
    let (h, r) = crypto_with(&page_json("X:BTCUSD", &bars, None));
    assert_malformed_once(&h, r, "absurd stamp");
}

#[test]
fn a_good_page_after_all_these_is_accepted() {
    // sanity: the scripted-single-page harness accepts valid data for the first instrument (it then fails on the
    // second because nothing else is scripted, with the transport's own error, not a validation error).
    let (h, r) = crypto_with(&page_json("X:BTCUSD", &btc_bars(), None));
    let e = r.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Unavailable, "{e}");
    assert!(h.transport.request_count() >= 2, "BTC was accepted, ETH was requested next");
}
