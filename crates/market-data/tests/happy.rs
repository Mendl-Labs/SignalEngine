//! The happy paths: an ETF sleeve and a crypto sleeve fetched from a synthetic vendor, the exact requests made, the
//! provenance left behind, and the reference rules accepting the result.

mod common;

use chrono::Duration;
use common::*;
use market_data::{SleeveFetcher, CRYPTO_HISTORY_DAYS, ETF_HISTORY_DAYS};
use rebalancer_run::data::DataSource;
use rebalancer_run::decision::evaluate;
use reference_rules::{data_fingerprint, Panel, CRYPTO_SYMBOLS, ETF_SYMBOLS};

#[test]
fn etf_sleeve_happy_path() {
    let h = Harness::new(as_of());
    let world = standard_world();
    h.serve(world.clone());
    let data = h.etf(as_of()).expect("etf fetch");

    assert_eq!(data.panel.symbols(), vec!["DBC", "EFA", "IEF", "SPY", "VNQ"]);
    let from = as_of() - Duration::days(ETF_HISTORY_DAYS);
    for sym in ETF_SYMBOLS {
        let got = bars_of(&data.panel, sym);
        let want: Vec<_> = world.of(sym).iter().copied().filter(|(x, _)| *x >= from && *x < as_of()).collect();
        assert_eq!(got, want, "{sym}: exactly the vendor's bars from {from} up to the day before the run date");
        assert_eq!(got.last().unwrap().0, d(2020, 6, 16), "{sym}: yesterday's session is the newest bar");
    }
}

#[test]
fn etf_requests_are_the_documented_range_calls_with_the_key_only_in_the_bearer_header() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    h.etf(as_of()).unwrap();

    let reqs = h.transport.requests();
    assert_eq!(reqs.len(), 5, "one request per ETF, one page each");
    let from = as_of() - Duration::days(ETF_HISTORY_DAYS);
    for (req, sym) in reqs.iter().zip(ETF_SYMBOLS) {
        assert_eq!(
            req.url,
            format!("https://api.massive.com/v2/aggs/ticker/{sym}/range/1/day/{from}/2020-06-16?adjusted=true&sort=asc&limit=50000"),
            "range endpoint, adjusted=true, ascending, up to the day before the run date"
        );
        assert_eq!(req.method, broker_adapters::transport::HttpMethod::Get);
        assert_eq!(req.header("authorization"), Some(format!("Bearer {KEY}").as_str()));
        assert_eq!(req.header("cache-control"), Some("no-cache, no-store"));
        assert_eq!(req.header("pragma"), Some("no-cache"));
        assert!(req.body.is_none());
    }
}

#[test]
fn crypto_sleeve_happy_path_drops_todays_live_bar() {
    let h = Harness::new(as_of());
    let world = standard_world(); // the synthetic vendor also serves today's bar and the future (honor_to = false)
    assert!(world.of("X:BTCUSD").iter().any(|(x, _)| *x == as_of()), "the vendor has a bar dated today");
    h.serve(world.clone());
    let data = h.crypto(as_of()).expect("crypto fetch");

    assert_eq!(data.panel.symbols(), vec!["BTC", "ETH"]);
    let from = as_of() - Duration::days(CRYPTO_HISTORY_DAYS);
    for (sym, ticker) in [("BTC", "X:BTCUSD"), ("ETH", "X:ETHUSD")] {
        let got = bars_of(&data.panel, sym);
        let want: Vec<_> = world.of(ticker).iter().copied().filter(|(x, _)| *x >= from && *x < as_of()).collect();
        assert_eq!(got, want);
        assert_eq!(got.last().unwrap().0, d(2020, 6, 16), "yesterday is the newest bar; today's growing bar is gone");
        assert_eq!(got.len(), CRYPTO_HISTORY_DAYS as usize);
    }
    let reqs = h.transport.requests();
    assert_eq!(reqs.len(), 2);
    assert!(reqs[0].url.starts_with("https://api.massive.com/v2/aggs/ticker/X:BTCUSD/range/1/day/"), "{}", reqs[0].url);
    assert!(reqs[1].url.starts_with("https://api.massive.com/v2/aggs/ticker/X:ETHUSD/range/1/day/"), "{}", reqs[1].url);
}

#[test]
fn weekend_and_holiday_gaps_are_left_as_gaps() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let etf = h.etf(as_of()).unwrap();
    let spy = bars_of(&etf.panel, "SPY");
    let dates: Vec<_> = spy.iter().map(|(x, _)| *x).collect();
    assert!(!dates.contains(&d(2020, 5, 25)), "Memorial Day has no bar");
    assert!(!dates.contains(&d(2020, 6, 13)) && !dates.contains(&d(2020, 6, 14)), "no weekend bars");
    assert!(dates.contains(&d(2020, 5, 22)) && dates.contains(&d(2020, 5, 26)));
    for w in dates.windows(2) {
        assert!(w[0] < w[1]);
    }

    let crypto = h.crypto(as_of()).unwrap();
    let btc: Vec<_> = bars_of(&crypto.panel, "BTC").iter().map(|(x, _)| *x).collect();
    assert!(btc.contains(&d(2020, 6, 13)) && btc.contains(&d(2020, 6, 14)), "crypto has weekend bars");
}

#[test]
fn provenance_describes_each_fetch_without_secrets() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let fetched = h.src.fetch_sleeve(&etf_sleeve(), as_of()).unwrap();
    assert_eq!(fetched.provenance.len(), 5);
    let from = as_of() - Duration::days(ETF_HISTORY_DAYS);
    for (p, sym) in fetched.provenance.iter().zip(ETF_SYMBOLS) {
        assert_eq!(p.instrument, sym);
        assert_eq!(p.vendor_ticker, sym);
        assert_eq!(p.source_id, "massive");
        assert_eq!(p.request_paths, vec![format!("/v2/aggs/ticker/{sym}/range/1/day/{from}/2020-06-16?adjusted=true&sort=asc&limit=50000")]);
        assert_eq!(p.fetched_at, at(as_of(), 0, 10));
        assert_eq!(p.as_of, as_of());
        let series = fetched.panel.get(sym).unwrap();
        assert_eq!(p.first_bar, series.first_date());
        assert_eq!(p.last_bar, d(2020, 6, 16));
        assert_eq!(p.last_close, *series.closes().last().unwrap());
        assert_eq!(p.bar_count, series.len());
        assert_eq!(p.raw_sha256.len(), 1);
        assert_eq!(p.raw_sha256[0].len(), 64);
        assert!(p.request_ids.iter().any(|i| i.starts_with("req-")), "{:?}", p.request_ids);
        assert!(!p.next_url_scrubbed);
        // compatible with the rules' own fingerprint: the fingerprint of this instrument's one-series panel
        let one = Panel::new(vec![series.clone()]).unwrap();
        assert_eq!(p.fingerprint, data_fingerprint(&one));
        let text = format!("{p:?}");
        assert!(!text.contains(KEY) && !text.contains("apiKey") && !text.contains("Bearer"));
        assert!(p.request_paths.iter().all(|r| !r.contains("http") && !r.to_ascii_lowercase().contains("apikey")));
    }
    // the source also remembers them
    assert_eq!(h.src.recent_provenance().len(), 5);
}

#[test]
fn crypto_provenance_names_the_vendor_ticker_and_the_dropped_forming_bar() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let fetched = h.src.fetch_sleeve(&crypto_sleeve(), as_of()).unwrap();
    let btc = &fetched.provenance[0];
    assert_eq!(btc.instrument, "BTC");
    assert_eq!(btc.vendor_ticker, "X:BTCUSD");
    assert_eq!(btc.last_bar, d(2020, 6, 16));
    assert_eq!(btc.dropped_incomplete.first(), Some(&as_of()), "today's bar was sent and dropped");
    assert!(btc.dropped_incomplete.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn the_reference_rules_accept_the_panels() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let etf = evaluate(&h.src, None, &etf_sleeve(), as_of()).expect("etf decision");
    assert_eq!(etf.decision_date, d(2020, 5, 29), "the newest completed month-end (May's last session)");
    assert_eq!(etf.instruments.len(), 5);
    let crypto = evaluate(&h.src, None, &crypto_sleeve(), as_of()).expect("crypto decision");
    assert_eq!(crypto.decision_date, d(2020, 6, 16));
    assert_eq!(crypto.instruments.len(), CRYPTO_SYMBOLS.len());
}

#[test]
fn identical_inputs_give_identical_panels_and_fingerprints() {
    let world = standard_world();
    let run = || {
        let h = Harness::new(as_of());
        h.serve(world.clone());
        let e = evaluate(&h.src, None, &etf_sleeve(), as_of()).unwrap();
        let c = evaluate(&h.src, None, &crypto_sleeve(), as_of()).unwrap();
        (e.fingerprint.clone(), c.fingerprint.clone(), h.etf(as_of()).unwrap().panel, h.crypto(as_of()).unwrap().panel)
    };
    let a = run();
    let b = run();
    assert_eq!(a.0, b.0);
    assert_eq!(a.1, b.1);
    assert_eq!(a.2, b.2);
    assert_eq!(a.3, b.3);
}

#[test]
fn the_panel_depends_on_the_run_date_not_on_when_it_was_fetched() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let at_slot = h.crypto(as_of()).unwrap().panel;
    let etf_slot = h.etf(as_of()).unwrap().panel;
    // fetched again 30 hours later (today's bar is by now complete, but the run date says it is not part of this run)
    h.clock.set(at(as_of() + Duration::days(1), 6, 0));
    assert_eq!(h.crypto(as_of()).unwrap().panel, at_slot);
    assert_eq!(h.etf(as_of()).unwrap().panel, etf_slot);
}

#[test]
fn the_sleeve_id_venue_asset_class_and_share_do_not_change_the_panel_or_the_requests() {
    let h = Harness::new(as_of());
    h.serve(standard_world());
    let a = h.src.sleeve_data(&etf_sleeve(), as_of()).unwrap();
    let n1 = h.transport.request_count();
    let mut other = etf_sleeve();
    other.id = "another-tenant-sleeve".into();
    other.venue = "somewhere".into();
    other.asset_class = "whatever".into();
    other.share = dec("0.1");
    let b = h.src.sleeve_data(&other, as_of()).unwrap();
    assert_eq!(a.panel, b.panel);
    let reqs = h.transport.requests();
    let (first, second) = reqs.split_at(n1);
    let urls = |v: &[broker_adapters::transport::HttpRequest]| v.iter().map(|r| r.url.clone()).collect::<Vec<_>>();
    assert_eq!(urls(first), urls(second));
}

#[test]
fn a_gap_in_the_middle_of_history_outside_the_decision_windows_does_not_fail_the_source() {
    // 120 days back a crypto day is missing: outside the last 100 the rule needs, inside the 150 requested.
    let mut world = standard_world();
    world.remove_date("X:BTCUSD", as_of() - Duration::days(120));
    let h = Harness::new(as_of());
    h.serve(world);
    let data = h.crypto(as_of()).expect("a hole older than the 100-day window is the rule's business, not a fetch failure");
    assert_eq!(bars_of(&data.panel, "BTC").len(), CRYPTO_HISTORY_DAYS as usize - 1);
}
