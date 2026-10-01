//! What "complete bar" means, and the typed failures for bars that are missing, stale or too few.
//! See `market_data::time` for the rule: date strictly before the run date AND the bar's period over by the clock.

mod common;

use std::time::Duration as StdDuration;

use chrono::Duration;
use common::*;
use market_data::testing::stock_ts;
use market_data::time::BarClock;
use market_data::{Completeness, ErrorKind, FailureClass, MassiveConfig, MassiveError, SleeveFetcher};
use rebalancer_run::data::DataSource;

fn err_kind(r: Result<rebalancer_run::data::SleeveData, rebalancer_run::data::DataError>) -> String {
    r.expect_err("must fail").code
}

// --- the date rule -------------------------------------------------------------------------------------------------

#[test]
fn a_bar_dated_the_run_date_is_dropped_even_when_the_clock_says_it_is_over() {
    // The clock is a day past the run date (so the run date's own bar is complete by the clock), but the run is FOR
    // `as_of`: the date rule alone must exclude it. Isolates `date < as_of` from the clock rule.
    let h = Harness::new(as_of());
    h.serve(standard_world());
    h.clock.set(at(as_of() + Duration::days(2), 12, 0));
    let crypto = h.crypto(as_of()).unwrap();
    assert_eq!(bars_of(&crypto.panel, "BTC").last().unwrap().0, d(2020, 6, 16));
    assert!(bars_of(&crypto.panel, "ETH").iter().all(|(x, _)| *x < as_of()));
    let etf = h.etf(as_of()).unwrap();
    for s in etf.panel.iter() {
        assert!(s.last_date() < as_of(), "{}: {}", s.symbol(), s.last_date());
        assert_eq!(s.last_date(), d(2020, 6, 16));
    }
}

#[test]
fn a_bar_dated_after_the_run_date_is_dropped() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    h.clock.set(at(as_of() + Duration::days(40), 12, 0));
    let crypto = h.crypto(as_of()).unwrap();
    assert!(bars_of(&crypto.panel, "BTC").iter().all(|(x, _)| *x < as_of()));
}

// --- the clock rule ------------------------------------------------------------------------------------------------

#[test]
fn the_clock_rule_drops_a_crypto_bar_whose_day_is_not_over() {
    // The caller says the run date is the 17th, but the clock still reads the 16th at 23:00Z: the 16th bar is still
    // growing. The date rule alone would pass it; the clock rule must not.
    let h = Harness::new(as_of());
    h.serve(standard_world());
    h.clock.set(at(d(2020, 6, 16), 23, 0));
    let e = h.crypto(as_of()).expect_err("yesterday's bar is not complete yet");
    assert_eq!(e.code, "DATA_STALE", "{}", e.message);
    // one minute after midnight it is complete
    h.clock.set(at(as_of(), 0, 0));
    assert!(h.crypto(as_of()).is_ok());
}

#[test]
fn the_clock_rule_boundary_for_a_stock_session_is_1615_utc_in_summer() {
    // 2020-06-16 (EDT): the session ends 20:00Z, the plan is 15 minutes delayed: complete from 20:15Z. The run date is
    // the 17th. Before 20:15Z the 16th bar is dropped (the ETFs then end on the 15th); from 20:15Z it is kept.
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let newest = |h: &Harness| h.etf(as_of()).unwrap().panel.get("SPY").unwrap().last_date();
    h.clock.set(at(d(2020, 6, 16), 20, 14) + Duration::seconds(59));
    assert_eq!(newest(&h), d(2020, 6, 15));
    h.clock.set(at(d(2020, 6, 16), 20, 15));
    assert_eq!(newest(&h), d(2020, 6, 16));
}

#[test]
fn the_clock_rule_boundary_for_a_stock_session_is_2115_utc_in_winter() {
    // 2020-12-15 (EST): the session ends 21:00Z, complete from 21:15Z.
    let run = d(2020, 12, 16);
    let h = Harness::new(run);
    h.serve(standard_world());
    let newest = |h: &Harness| h.etf(run).unwrap().panel.get("SPY").unwrap().last_date();
    h.clock.set(at(d(2020, 12, 15), 21, 14) + Duration::seconds(59));
    assert_eq!(newest(&h), d(2020, 12, 14));
    h.clock.set(at(d(2020, 12, 15), 21, 15));
    assert_eq!(newest(&h), d(2020, 12, 15));
}

#[test]
fn the_settle_margin_delays_completeness() {
    let cfg = MassiveConfig { completeness: Completeness { stock_settle: StdDuration::ZERO, crypto_settle: StdDuration::from_secs(15 * 60) }, ..MassiveConfig::default() };
    let h = Harness::with_config(as_of(), cfg); // clock at 00:10Z
    h.serve(standard_world());
    assert_eq!(err_kind(h.crypto(as_of())), "DATA_STALE", "00:10Z is inside the 15-minute settle margin");
    h.clock.set(at(as_of(), 0, 15));
    assert!(h.crypto(as_of()).is_ok());
}

// --- timezone conventions ------------------------------------------------------------------------------------------

#[test]
fn stock_dates_follow_new_york_midnight_across_both_offsets() {
    // Sessions in EST (January) and EDT (June) both come back on their own dates.
    let world = World::etf(d(2018, 6, 1), d(2020, 12, 31));
    let h = Harness::new(d(2020, 1, 21));
    h.serve(world);
    let dates: Vec<_> = bars_of(&h.etf(d(2020, 1, 21)).unwrap().panel, "SPY").iter().map(|(x, _)| *x).collect();
    assert!(dates.contains(&d(2020, 1, 17)) && dates.contains(&d(2020, 1, 15)), "EST bars (05:00Z stamps)");
    assert!(dates.contains(&d(2019, 7, 15)), "EDT bars (04:00Z stamps)");
    // the stamps really are 05:00Z and 04:00Z
    let est = stock_ts(d(2020, 1, 15));
    let edt = stock_ts(d(2019, 7, 15));
    assert_eq!(est % 86_400_000, 5 * 3_600_000);
    assert_eq!(edt % 86_400_000, 4 * 3_600_000);
}

#[test]
fn a_stock_stamp_in_the_crypto_convention_is_refused_not_re_dated() {
    // Bars stamped 00:00Z under a stock ticker: the wrong convention. Refuse; never guess the date.
    let h = Harness::new(as_of());
    let world = standard_world();
    h.transport.set_handler(move |req| {
        let (ticker, from, _) = parse_range_url(&req.url).unwrap();
        let bars: Vec<(i64, f64)> = world.of(&ticker).iter().filter(|(x, _)| *x >= from).map(|(x, c)| (market_data::testing::crypto_ts(*x), *c)).collect();
        Ok(broker_adapters::transport::HttpResponse { status: 200, body: market_data::testing::page_json(&ticker, &bars, None) })
    });
    let e = h.etf(as_of()).expect_err("stock bars stamped at UTC midnight");
    assert_eq!(e.code, "DATA_MALFORMED", "{}", e.message);
}

#[test]
fn a_crypto_stamp_at_new_york_midnight_is_refused() {
    let h = Harness::new(as_of());
    let world = standard_world();
    h.transport.set_handler(move |req| {
        let (ticker, from, _) = parse_range_url(&req.url).unwrap();
        let bars: Vec<(i64, f64)> = world.of(&ticker).iter().filter(|(x, _)| *x >= from).map(|(x, c)| (stock_ts(*x), *c)).collect();
        Ok(broker_adapters::transport::HttpResponse { status: 200, body: market_data::testing::page_json(&ticker, &bars, None) })
    });
    assert_eq!(err_kind(h.crypto(as_of())), "DATA_MALFORMED");
}

#[test]
fn fetch_daily_bars_uses_the_same_conventions() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let b = h.src.fetch_daily_bars("SPY", BarClock::StockMidnightNewYork, d(2020, 5, 1), d(2020, 6, 16), as_of()).unwrap();
    assert_eq!(*b.dates.last().unwrap(), d(2020, 6, 16));
    assert!(b.dates.iter().all(|x| *x < as_of()));
    let c = h.src.fetch_daily_bars("X:BTCUSD", BarClock::MidnightUtc, d(2020, 5, 18), d(2020, 6, 16), as_of()).unwrap();
    assert_eq!(c.dates.len(), 30);
    assert_eq!(c.provenance.bar_count, 30);
    assert!(h.src.fetch_daily_bars("SPY/../x", BarClock::MidnightUtc, d(2020, 5, 18), d(2020, 6, 16), as_of()).is_err(), "no path injection");
}

// --- missing / stale / short history (typed) -----------------------------------------------------------------------

#[test]
fn a_missing_month_end_bar_is_a_typed_missing_bar() {
    // 2020-04-30 (a Thursday, April's last session) is absent for EFA only.
    let mut world = standard_world();
    world.remove_date("EFA", d(2020, 4, 30));
    let h = Harness::new(as_of());
    h.serve(world);
    let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::MissingBar);
    assert_eq!(e.class(), FailureClass::Deterministic);
    assert_eq!(e.error, MassiveError::MissingBar { symbol: "EFA".into(), date: d(2020, 4, 30) });
    let de: rebalancer_run::data::DataError = e.into();
    assert_eq!(de.code, "DATA_MISSING_BAR");
    assert!(de.message.contains("EFA") && de.message.contains("2020-04-30"), "{}", de.message);
}

#[test]
fn any_missing_session_inside_the_window_is_caught_by_the_other_etfs() {
    let mut world = standard_world();
    world.remove_date("SPY", d(2020, 3, 12));
    let h = Harness::new(as_of());
    h.serve(world);
    let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.error, MassiveError::MissingBar { symbol: "SPY".into(), date: d(2020, 3, 12) });
}

#[test]
fn the_newest_session_missing_for_one_etf_is_stale_not_missing() {
    let mut world = standard_world();
    world.remove_date("IEF", d(2020, 6, 16));
    let h = Harness::new(as_of());
    h.serve(world);
    let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::StaleData);
    assert_eq!(e.class(), FailureClass::Settling);
    assert_eq!(e.error.instrument(), Some("IEF"));
}

#[test]
fn a_missing_crypto_day_inside_the_last_100_is_a_typed_missing_bar() {
    let mut world = standard_world();
    world.remove_date("X:ETHUSD", d(2020, 5, 1));
    let h = Harness::new(as_of());
    h.serve(world);
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.error, MassiveError::MissingBar { symbol: "ETH".into(), date: d(2020, 5, 1) });
}

#[test]
fn the_newest_crypto_bar_missing_is_stale() {
    let mut world = standard_world();
    world.remove_date("X:BTCUSD", d(2020, 6, 16));
    let h = Harness::new(as_of());
    h.serve(world);
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::StaleData);
    assert_eq!(err_kind(h.crypto(as_of())), "DATA_STALE");
}

#[test]
fn a_vendor_that_stopped_publishing_is_stale_for_both_asset_classes() {
    let mut world = standard_world();
    world.truncate_after("SPY", d(2020, 6, 5));
    for s in ["EFA", "IEF", "DBC", "VNQ"] {
        world.truncate_after(s, d(2020, 6, 5));
    }
    let h = Harness::new(as_of());
    h.serve(world);
    assert_eq!(err_kind(h.etf(as_of())), "DATA_STALE", "12 days old is beyond the 5-day staleness limit");

    let mut world = standard_world();
    world.truncate_after("X:BTCUSD", d(2020, 6, 14));
    let h = Harness::new(as_of());
    h.serve(world);
    assert_eq!(err_kind(h.crypto(as_of())), "DATA_STALE");
}

#[test]
fn an_etf_a_few_sessions_behind_but_inside_the_limit_is_accepted_by_the_source() {
    // The five ETFs all end on Friday 2020-06-12 and the run date is Tuesday the 16th: 4 days, inside the 5-day limit.
    // (The reference rule applies its own checks on top; the source only refuses what it can prove wrong.)
    let mut world = standard_world();
    for s in ["SPY", "EFA", "IEF", "DBC", "VNQ"] {
        world.truncate_after(s, d(2020, 6, 12));
    }
    let h = Harness::new(d(2020, 6, 16));
    h.serve(world);
    let data = h.etf(d(2020, 6, 16)).unwrap();
    assert_eq!(data.panel.get("SPY").unwrap().last_date(), d(2020, 6, 12));
}

#[test]
fn too_few_crypto_bars_is_insufficient_history() {
    let world = World::crypto(d(2020, 4, 1), d(2020, 12, 31)); // 77 completed days before 2020-06-17
    let h = Harness::new(as_of());
    h.serve(world);
    let e = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.error, MassiveError::InsufficientHistory { symbol: "BTC".into(), needed: 100, have: 77 });
    assert_eq!(err_kind(h.crypto(as_of())), "DATA_INSUFFICIENT_HISTORY");
}

#[test]
fn too_few_etf_month_ends_is_insufficient_history() {
    // From 2019-10-01: completed month-ends Oct..May = 8.
    let world = World::etf(d(2019, 10, 1), d(2020, 12, 31));
    let h = Harness::new(as_of());
    h.serve(world);
    let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.error, MassiveError::InsufficientHistory { symbol: "SPY".into(), needed: 10, have: 8 });
}

#[test]
fn an_etf_the_vendor_knows_nothing_about_is_insufficient_history() {
    let mut world = standard_world();
    world.bars.remove("VNQ");
    let h = Harness::new(as_of());
    h.serve(world);
    let e = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap_err();
    assert_eq!(e.error, MassiveError::InsufficientHistory { symbol: "VNQ".into(), needed: 10, have: 0 });
}

#[test]
fn ragged_first_dates_are_compared_from_the_latest_start() {
    // One ETF listed later than the others is not a missing bar for the days before it existed, as long as it still
    // has ten completed month-ends.
    let mut world = standard_world();
    let vnq: Vec<_> = world.of("VNQ").iter().copied().filter(|(x, _)| *x >= d(2019, 3, 4)).collect();
    world.bars.insert("VNQ".into(), vnq);
    let h = Harness::new(as_of());
    h.serve(world);
    assert!(h.etf(as_of()).is_ok());
}

// --- limits of the calendar ----------------------------------------------------------------------------------------

#[test]
fn extreme_run_dates_never_panic() {
    // No scripted vendor: whatever the source computes for these dates, it must answer with an error, not a panic.
    let h = Harness::new(as_of());
    for day in [chrono::NaiveDate::MIN, chrono::NaiveDate::MIN + Duration::days(5), chrono::NaiveDate::MAX, chrono::NaiveDate::MAX - Duration::days(5), d(1970, 1, 1), d(2006, 6, 1)] {
        assert!(h.src.sleeve_data(&etf_sleeve(), day).is_err(), "{day}");
        assert!(h.src.sleeve_data(&crypto_sleeve(), day).is_err(), "{day}");
    }
}

#[test]
fn only_usd_crypto_quotes_are_supported_and_nothing_is_requested_for_others() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let mut s = crypto_sleeve();
    s.quote = "EUR".into();
    let e = h.src.fetch_sleeve(&s, as_of()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Unsupported);
    assert_eq!(h.transport.request_count(), 0);
}

#[test]
fn prices_are_not_supplied_and_say_so() {
    let h = Harness::new(as_of());
    let e = h.src.prices(&["SPY".to_string()], at(as_of(), 0, 10)).unwrap_err();
    assert_eq!(e.code, "DATA_UNSUPPORTED");
    assert_eq!(h.transport.request_count(), 0);
}
