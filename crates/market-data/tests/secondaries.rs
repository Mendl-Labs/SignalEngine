//! The two secondary readers of the gate (`AlpacaBarsSource`, `KrakenOhlcSource`): request shape, the documented
//! response shapes (hand-written, never recorded), the completeness rule, provenance, and the typed failures. Offline.

mod common;

use std::sync::Arc;

use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpResponse, TransportError};
use chrono::{Datelike, Duration, NaiveDate};
use common::*;
use market_data::testing::ManualMarketClock;
use market_data::{AlpacaBarsSource, ErrorKind, KrakenOhlcSource, SleeveFetcher};
use reference_rules::{CRYPTO_SMA_DAYS, ETF_SYMBOLS};
use serde_json::{json, Value};

const KEY_ID: &str = "PKDATATESTKEY0001";
const SECRET: &str = "data-secret-not-real-7c1d";

/// Alpaca's documented daily-bar stamp: midnight New York as an RFC 3339 UTC instant.
fn alpaca_t(date: NaiveDate) -> String {
    let off = market_data::time::new_york_midnight_utc_offset_hours(date).unwrap();
    format!("{date}T{off:02}:00:00Z")
}

/// A `/v2/stocks/bars` page for every ETF from `world`, bars in `[start, end]`, optionally split after `per_page`
/// bars per symbol (second page carries the rest).
fn alpaca_page(world: &World, start: NaiveDate, end: NaiveDate, page: usize, per_page: usize) -> String {
    let mut bars = serde_json::Map::new();
    let mut more = false;
    for sym in ETF_SYMBOLS {
        let all: Vec<&(NaiveDate, f64)> = world.of(sym).iter().filter(|(d, _)| *d >= start && *d <= end).collect();
        let chunk: Vec<Value> = all
            .iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|(d, c)| json!({"t": alpaca_t(*d), "o": c, "h": c, "l": c, "c": c, "v": 1000, "n": 10, "vw": c}))
            .collect();
        if all.len() > (page + 1) * per_page {
            more = true;
        }
        bars.insert(sym.to_string(), Value::Array(chunk));
    }
    json!({"bars": bars, "next_page_token": if more { Value::String(format!("tok{}", page + 1)) } else { Value::Null }}).to_string()
}

fn parse_q(url: &str, key: &str) -> Option<String> {
    url.split('?').nth(1)?.split('&').find_map(|kv| kv.strip_prefix(&format!("{key}=")).map(str::to_string))
}

fn alpaca(per_page: usize) -> (AlpacaBarsSource, Arc<FakeTransport>, Arc<ManualMarketClock>) {
    let t = Arc::new(FakeTransport::new());
    let clock = Arc::new(ManualMarketClock::at_run_slot(as_of()));
    let world = standard_world();
    t.set_handler(move |req| {
        let start: NaiveDate = parse_q(&req.url, "start").unwrap().parse().unwrap();
        let end: NaiveDate = parse_q(&req.url, "end").unwrap().parse().unwrap();
        let page = parse_q(&req.url, "page_token").map(|t| t.trim_start_matches("tok").parse::<usize>().unwrap()).unwrap_or(0);
        Ok(HttpResponse { status: 200, body: alpaca_page(&world, start, end, page, per_page) })
    });
    let src = AlpacaBarsSource::new(KEY_ID, SECRET, t.clone()).unwrap().with_clock(clock.clone());
    (src, t, clock)
}

#[test]
fn alpaca_asks_for_split_adjusted_daily_bars_of_the_five_etfs_with_the_data_headers_and_follows_pages() {
    let (src, t, _) = alpaca(200);
    let f = src.fetch_sleeve(&etf_sleeve(), as_of()).expect("the ETF sleeve");
    assert_eq!(src.source_id(), "alpaca");
    let reqs = t.requests();
    assert!(reqs.len() >= 2, "more than one page was needed: {}", reqs.len());
    let first = &reqs[0];
    assert!(first.url.starts_with("https://data.alpaca.markets/v2/stocks/bars?"), "{}", first.url);
    assert_eq!(parse_q(&first.url, "symbols").as_deref(), Some("SPY,EFA,IEF,DBC,VNQ"));
    assert_eq!(parse_q(&first.url, "timeframe").as_deref(), Some("1Day"));
    assert_eq!(parse_q(&first.url, "adjustment").as_deref(), Some("split"));
    assert_eq!(parse_q(&first.url, "feed").as_deref(), Some("sip"));
    assert_eq!(parse_q(&first.url, "end").as_deref(), Some("2020-06-16"));
    assert_eq!(first.header("APCA-API-KEY-ID"), Some(KEY_ID));
    assert_eq!(first.header("APCA-API-SECRET-KEY"), Some(SECRET));
    assert!(parse_q(&first.url, "page_token").is_none());
    assert_eq!(parse_q(&reqs[1].url, "page_token").as_deref(), Some("tok1"));

    // the panel: five instruments, every session up to 2020-06-16 (as_of is the 17th), same closes as the world
    assert_eq!(f.panel.symbols().len(), 5);
    let spy = f.panel.get("SPY").unwrap();
    assert_eq!(spy.last_date(), d(2020, 6, 16));
    let world = standard_world();
    let expected: Vec<(NaiveDate, f64)> = world.of("SPY").iter().filter(|(x, _)| *x >= spy.first_date() && *x <= spy.last_date()).copied().collect();
    assert_eq!(bars_of(&f.panel, "SPY"), expected);
    // provenance: the request records elide the page token, carry one hash per page, and name no secret
    let p = f.provenance.iter().find(|p| p.instrument == "SPY").unwrap();
    assert_eq!(p.source_id, "alpaca");
    assert_eq!(p.request_paths.len(), reqs.len());
    assert!(p.request_paths[1].contains("page_token=<elided>"));
    assert_eq!(p.raw_sha256.len(), reqs.len());
    assert!(p.request_paths.iter().all(|x| !x.contains(SECRET) && !x.contains(KEY_ID)));
    assert_eq!(p.last_close, spy.closes()[spy.len() - 1]);
    assert!(!format!("{src:?}").contains(SECRET));
}

#[test]
fn alpaca_applies_the_completeness_rule_and_refuses_the_crypto_sleeve() {
    // the vendor also returns the forming bar of the run date and one dated after it: both are dropped
    let t = Arc::new(FakeTransport::new());
    let clock = Arc::new(ManualMarketClock::at_run_slot(as_of()));
    let world = World::etf(d(2018, 6, 1), as_of() + Duration::days(1));
    t.set_handler(move |req| {
        let start: NaiveDate = parse_q(&req.url, "start").unwrap().parse().unwrap();
        Ok(HttpResponse { status: 200, body: alpaca_page(&world, start, as_of() + Duration::days(1), 0, 100_000) })
    });
    let src = AlpacaBarsSource::new(KEY_ID, SECRET, t.clone()).unwrap().with_clock(clock);
    let f = src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap();
    for s in f.panel.iter() {
        assert!(s.last_date() < as_of(), "{}: {}", s.symbol(), s.last_date());
    }
    let p = f.provenance.iter().find(|p| p.instrument == "SPY").unwrap();
    assert!(p.dropped_incomplete.iter().all(|x| *x >= as_of()));
    assert!(!p.dropped_incomplete.is_empty());

    let e = src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Unsupported);
}

#[test]
fn alpaca_failures_are_typed_and_a_bad_stamp_is_refused_not_re_dated() {
    let t = Arc::new(FakeTransport::new());
    let src = AlpacaBarsSource::new(KEY_ID, SECRET, t.clone()).unwrap().with_clock(Arc::new(ManualMarketClock::at_run_slot(as_of())));
    let kind = |src: &AlpacaBarsSource| src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err().kind();

    t.enqueue_json(429, r#"{"message":"too many requests"}"#);
    assert_eq!(kind(&src), ErrorKind::RateLimited);
    t.enqueue_json(403, &format!(r#"{{"message":"forbidden for {KEY_ID}"}}"#));
    let e = src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotAuthorized);
    assert!(!e.to_string().contains(KEY_ID), "{e}");
    t.enqueue_json(503, "down");
    assert_eq!(kind(&src), ErrorKind::Unavailable);
    t.enqueue_error(TransportError::Timeout);
    assert_eq!(kind(&src), ErrorKind::Unavailable);
    t.enqueue_json(200, "not json");
    assert_eq!(kind(&src), ErrorKind::Malformed);
    t.enqueue_json(200, r#"{"bars":{"SPY":[{"t":"2020-06-16T00:00:00Z","c":300.0}]},"next_page_token":null}"#);
    let e = src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Malformed, "a crypto-convention stamp on a stock bar is refused: {e}");
    t.enqueue_json(200, r#"{"bars":{"SPY":[]},"next_page_token":null}"#);
    assert_eq!(kind(&src), ErrorKind::InsufficientHistory);
    assert!(AlpacaBarsSource::new("", SECRET, t.clone()).is_err());
    assert!(AlpacaBarsSource::new(KEY_ID, SECRET, t.clone()).unwrap().with_feed("bogus").is_err());
    let from_env = AlpacaBarsSource::from_lookup(
        |k| match k {
            "ALPACA_DATA_KEY_ID" => Some(KEY_ID.into()),
            "ALPACA_DATA_KEY_SECRET" => Some(SECRET.into()),
            "ALPACA_DATA_FEED" => Some("IEX".into()),
            _ => None,
        },
        t.clone(),
    )
    .unwrap();
    assert!(format!("{from_env:?}").contains("iex"));
}

// ---------------------------------------------------------------------------------------------------------------
// Kraken
// ---------------------------------------------------------------------------------------------------------------

/// Kraken's documented OHLC shape: `[time, open, high, low, close, vwap, volume, count]`, strings for prices.
fn kraken_body(rest_name: &str, bars: &[(NaiveDate, f64)]) -> String {
    let rows: Vec<Value> = bars
        .iter()
        .map(|(d, c)| json!([d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp(), c.to_string(), c.to_string(), c.to_string(), c.to_string(), c.to_string(), "12.5", 100]))
        .collect();
    let last = bars.last().map(|(d, _)| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp()).unwrap_or(0);
    json!({"error": [], "result": { rest_name: rows, "last": last }}).to_string()
}

fn kraken() -> (KrakenOhlcSource, Arc<FakeTransport>) {
    let t = Arc::new(FakeTransport::new());
    let world = standard_world();
    t.set_handler(move |req| {
        let pair = parse_q(&req.url, "pair").unwrap();
        let (ticker, rest) = match pair.as_str() {
            "XBTUSD" => ("X:BTCUSD", "XXBTZUSD"),
            "ETHUSD" => ("X:ETHUSD", "XETHZUSD"),
            other => panic!("unexpected pair {other}"),
        };
        // 720 candles ending with the FORMING day (the run date), as the live endpoint does
        let all: Vec<(NaiveDate, f64)> = world.of(ticker).iter().filter(|(d, _)| *d < as_of()).copied().collect();
        let mut bars: Vec<(NaiveDate, f64)> = all.iter().rev().take(719).rev().copied().collect();
        bars.push((as_of(), 1.0));
        Ok(HttpResponse { status: 200, body: kraken_body(rest, &bars) })
    });
    let src = KrakenOhlcSource::new(t.clone()).with_clock(Arc::new(ManualMarketClock::at_run_slot(as_of())));
    (src, t)
}

#[test]
fn kraken_reads_daily_public_ohlc_per_pair_dates_candles_by_their_utc_start_and_drops_the_forming_day() {
    let (src, t) = kraken();
    let f = src.fetch_sleeve(&crypto_sleeve(), as_of()).expect("the crypto sleeve");
    assert_eq!(src.source_id(), "kraken");
    let reqs = t.requests();
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[0].url, "https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1440");
    assert_eq!(reqs[1].url, "https://api.kraken.com/0/public/OHLC?pair=ETHUSD&interval=1440");
    assert!(reqs.iter().all(|r| r.header("API-Key").is_none()), "public endpoint: no credentials");
    let btc = f.panel.get("BTC").unwrap();
    assert_eq!(btc.last_date(), as_of() - Duration::days(1), "the forming day is dropped");
    assert!(btc.len() >= CRYPTO_SMA_DAYS);
    assert!(btc.first_date() >= as_of() - Duration::days(market_data::CRYPTO_HISTORY_DAYS));
    let world = standard_world();
    let expected: Vec<(NaiveDate, f64)> = world.of("X:BTCUSD").iter().filter(|(x, _)| *x >= btc.first_date() && *x < as_of()).copied().collect();
    assert_eq!(bars_of(&f.panel, "BTC"), expected);
    let p = f.provenance.iter().find(|p| p.instrument == "ETH").unwrap();
    assert_eq!((p.source_id, p.vendor_ticker.as_str()), ("kraken", "ETHUSD"));
    assert_eq!(p.dropped_incomplete, vec![as_of()]);
    assert_eq!(p.raw_sha256.len(), 1);
    assert_eq!(as_of().weekday(), chrono::Weekday::Wed);
}

#[test]
fn kraken_failures_are_typed_and_the_etf_sleeve_is_refused() {
    let t = Arc::new(FakeTransport::new());
    let src = KrakenOhlcSource::new(t.clone()).with_clock(Arc::new(ManualMarketClock::at_run_slot(as_of())));
    let kind = |src: &KrakenOhlcSource| src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err().kind();
    t.enqueue_json(200, r#"{"error":["EAPI:Rate limit exceeded"]}"#);
    assert_eq!(kind(&src), ErrorKind::RateLimited);
    t.enqueue_json(200, r#"{"error":["EService:Unavailable"]}"#);
    assert_eq!(kind(&src), ErrorKind::Unavailable);
    t.enqueue_json(200, r#"{"error":["EQuery:Unknown asset pair"]}"#);
    assert_eq!(kind(&src), ErrorKind::Malformed);
    t.enqueue_json(502, "bad gateway");
    assert_eq!(kind(&src), ErrorKind::Unavailable);
    t.enqueue_error(TransportError::ConnectFailed("refused".into()));
    assert_eq!(kind(&src), ErrorKind::Unavailable);
    // a candle not at 00:00 UTC is refused, never re-dated
    let noon = as_of().pred_opt().unwrap().and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp();
    t.enqueue_json(200, &format!(r#"{{"error":[],"result":{{"XXBTZUSD":[[{noon},"1","1","1","9000.0","1","1",1]],"last":{noon}}}}}"#));
    assert_eq!(kind(&src), ErrorKind::Malformed);
    // only the forming candle: nothing complete
    let today = as_of().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
    t.enqueue_json(200, &format!(r#"{{"error":[],"result":{{"XXBTZUSD":[[{today},"1","1","1","9000.0","1","1",1]],"last":{today}}}}}"#));
    assert_eq!(kind(&src), ErrorKind::InsufficientHistory);
    assert_eq!(src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err().kind(), ErrorKind::Unsupported);
    let mut eur = crypto_sleeve();
    eur.quote = "EUR".into();
    assert_eq!(src.fetch_sleeve(&eur, as_of()).unwrap_err().kind(), ErrorKind::Unsupported);
}
