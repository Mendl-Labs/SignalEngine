//! The latency recorder's view of the vendor (W9.1): `observe_recent_bars` and the `RecentBarsSource` impl return the
//! bars AS SHOWN, forming bar included, with the venue's nominal close, the OHLCV values and the response hash; the
//! request is the same range endpoint over the policy's lookback; errors stay typed by code; and the decision path's
//! own completeness rule is untouched.

mod common;

use chrono::Duration;
use common::*;
use market_data::observe::vendor_ticker;
use market_data::time::BarClock;
use market_data::{ErrorKind, MassiveError};
use rebalancer_run::data::SleeveKind;
use rebalancer_run::latency::policy::{self, LOOKBACK_CALENDAR_DAYS};
use rebalancer_run::latency::{Instrument, RecentBarsSource};

fn spy() -> Instrument {
    Instrument {
        symbol: "SPY".into(),
        kind: SleeveKind::EtfTrend,
        quote: String::new(),
    }
}

fn btc() -> Instrument {
    Instrument {
        symbol: "BTC".into(),
        kind: SleeveKind::CryptoTrend,
        quote: "USD".into(),
    }
}

#[test]
fn instruments_map_to_the_same_vendor_tickers_and_clocks_the_sleeve_fetch_uses() {
    assert_eq!(
        vendor_ticker(&spy()),
        ("SPY".to_string(), BarClock::StockMidnightNewYork)
    );
    assert_eq!(
        vendor_ticker(&btc()),
        ("X:BTCUSD".to_string(), BarClock::MidnightUtc)
    );
}

#[test]
fn observed_bars_are_unfiltered_with_nominal_closes_values_and_the_response_hash() {
    // Wednesday 2020-06-17, 19:59Z: the synthetic vendor serves every bar after `from` (honor_to = false), so the
    // bar dated today is in the response. 2020-06-17 is in daylight time: the ETF nominal close is 20:00Z.
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let now = at(as_of(), 19, 59);
    h.clock.set(now);

    let got = h.src.recent_bars(&spy(), now).expect("observe");
    let dates: Vec<_> = got.bars.iter().map(|b| b.bar_date).collect();
    assert!(
        dates.contains(&as_of()),
        "today's (forming) bar is handed over as shown: {dates:?}"
    );
    assert!(dates.windows(2).all(|w| w[0] < w[1]), "ascending");
    let today = got.bars.iter().find(|b| b.bar_date == as_of()).unwrap();
    assert_eq!(
        today.nominal_close_at,
        at(as_of(), 20, 0),
        "16:00 New York in EDT"
    );
    assert!(
        !policy::is_observable(today, now),
        "and the recorder's policy keeps it out until 20:00Z"
    );
    assert!(policy::is_observable(today, at(as_of(), 20, 0)));
    let yesterday = got
        .bars
        .iter()
        .find(|b| b.bar_date == d(2020, 6, 16))
        .unwrap();
    assert_eq!(yesterday.nominal_close_at, at(d(2020, 6, 16), 20, 0));
    // the synthetic page sets o = h = l = c and v = 1000 (market_data::testing::page_json)
    let want_close = standard_world()
        .of("SPY")
        .iter()
        .find(|(x, _)| *x == d(2020, 6, 16))
        .unwrap()
        .1;
    assert_eq!(yesterday.values.close, want_close);
    assert_eq!(
        (
            yesterday.values.open,
            yesterday.values.high,
            yesterday.values.low,
            yesterday.values.volume
        ),
        (
            Some(want_close),
            Some(want_close),
            Some(want_close),
            Some(1000.0)
        )
    );
    assert_eq!(
        got.response_sha256.as_ref().map(String::len),
        Some(64),
        "the raw response hash, lowercase hex"
    );
    assert!(got
        .response_sha256
        .as_ref()
        .unwrap()
        .chars()
        .all(|c| c.is_ascii_hexdigit()));

    // The request: the range endpoint over the policy's lookback up to and including today, key only in the header.
    let reqs = h.transport.requests();
    assert_eq!(reqs.len(), 1);
    let from = as_of() - Duration::days(LOOKBACK_CALENDAR_DAYS);
    assert_eq!(reqs[0].url, format!("https://api.massive.com/v2/aggs/ticker/SPY/range/1/day/{from}/2020-06-17?adjusted=true&sort=asc&limit=50000"));
    assert_eq!(
        reqs[0].header("authorization"),
        Some(format!("Bearer {KEY}").as_str())
    );
    assert!(!reqs[0].url.contains(KEY));
}

#[test]
fn crypto_nominal_close_is_midnight_utc_of_the_next_day_and_the_forming_day_is_handed_over() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let now = at(as_of(), 23, 55);
    h.clock.set(now);
    let got = h.src.recent_bars(&btc(), now).expect("observe");
    let today = got
        .bars
        .iter()
        .find(|b| b.bar_date == as_of())
        .expect("today's UTC day as the vendor shows it");
    assert_eq!(today.nominal_close_at, at(d(2020, 6, 18), 0, 0));
    assert!(!policy::is_observable(today, now));
    assert!(policy::is_observable(today, at(d(2020, 6, 18), 0, 0)));
    let yesterday = got
        .bars
        .iter()
        .find(|b| b.bar_date == d(2020, 6, 16))
        .unwrap();
    assert_eq!(yesterday.nominal_close_at, at(as_of(), 0, 0));
    assert!(h.transport.requests()[0]
        .url
        .contains("/v2/aggs/ticker/X:BTCUSD/range/1/day/"));
}

#[test]
fn the_decision_fetch_still_drops_the_forming_bar_after_the_observer_ran() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    h.clock.set(at(as_of(), 23, 55));
    h.src.recent_bars(&btc(), at(as_of(), 23, 55)).unwrap();
    h.clock.set(at(as_of(), 0, 10));
    let data = h.crypto(as_of()).expect("crypto sleeve");
    let got = bars_of(&data.panel, "BTC");
    assert_eq!(
        got.last().unwrap().0,
        d(2020, 6, 16),
        "the rules' completeness filter is untouched by the observer"
    );
    assert!(
        h.src.recent_provenance().iter().all(|p| p.as_of == as_of()),
        "observations leave no provenance entries of their own"
    );
}

#[test]
fn errors_keep_their_code_and_a_bad_ticker_is_refused_before_any_request() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let now = at(as_of(), 19, 59);
    let e = h
        .src
        .observe_recent_bars(
            "SPY QQQ",
            BarClock::StockMidnightNewYork,
            d(2020, 6, 7),
            as_of(),
        )
        .unwrap_err();
    assert!(matches!(e, MassiveError::Unsupported { .. }));
    assert_eq!(h.transport.requests().len(), 0);

    h.transport.set_handler(|_| {
        Ok(broker_adapters::transport::HttpResponse {
            status: 403,
            body: market_data::testing::NOT_AUTHORIZED_MESSAGE_BODY.to_string(),
        })
    });
    let e = h.src.recent_bars(&spy(), now).unwrap_err();
    assert!(e.starts_with(ErrorKind::NotAuthorized.code()), "{e}");
    assert!(!e.contains(KEY), "never the key");
}

#[test]
fn an_empty_vendor_answer_is_zero_bars_not_an_error() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let now = at(as_of(), 19, 59);
    let unknown = Instrument {
        symbol: "QQQ".into(),
        kind: SleeveKind::EtfTrend,
        quote: String::new(),
    };
    let got = h
        .src
        .recent_bars(&unknown, now)
        .expect("the synthetic vendor answers the documented empty shape");
    assert!(got.bars.is_empty());
}
