//! LIVE smoke test against the real Massive API. Off by default: it needs the network, the owner's key and (to build the
//! real transport) the `live` feature. It never runs in CI and nothing in this repository sets `MASSIVE_LIVE_TEST`.
//!
//! ```text
//! MASSIVE_LIVE_TEST=1 MASSIVE_API_KEY=... cargo test -p market-data --features live --test live -- --nocapture
//! ```
//! When `MASSIVE_LIVE_TEST` is not `1` the test prints `SKIPPED` and passes without touching anything. (Rust has no
//! runtime "skip"; `--nocapture` is what makes the line visible.)
//!
//! What it checks, for the last 30 days of SPY and BTC through the real transport: structural invariants only (never a
//! price level): ascending unique dates, all strictly before today, finite positive closes, the documented timestamp
//! conventions (enforced by the source: a wrong stamp would be an error), crypto has every one of the 30 days, stocks
//! have a plausible number of sessions and a recent newest bar, provenance carries no key.

use chrono::Duration;
use market_data::time::BarClock;

#[cfg(feature = "live")]
fn real_source() -> market_data::MassiveDataSource {
    use broker_adapters::transport::reqwest_transport::ReqwestTransport;
    use std::sync::Arc;
    let keys = market_data::EnvKeyProvider::from_env().expect("MASSIVE_API_KEY must be set for the live test");
    let transport = ReqwestTransport::new().expect("build the real transport");
    market_data::MassiveDataSource::new(keys, Arc::new(transport))
}

#[cfg(not(feature = "live"))]
fn real_source() -> market_data::MassiveDataSource {
    panic!("MASSIVE_LIVE_TEST=1 needs the real transport: run with `--features live`");
}

#[test]
fn live_spy_and_btc_thirty_days() {
    if std::env::var("MASSIVE_LIVE_TEST").as_deref() != Ok("1") {
        println!("SKIPPED: live Massive test (set MASSIVE_LIVE_TEST=1, MASSIVE_API_KEY and run with --features live)");
        eprintln!("SKIPPED: live Massive test (set MASSIVE_LIVE_TEST=1, MASSIVE_API_KEY and run with --features live)");
        return;
    }
    let src = real_source();
    let today = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now()).date_naive();
    let from = today - Duration::days(30);
    let to = today - Duration::days(1);

    let check_common = |what: &str, dates: &[chrono::NaiveDate], closes: &[f64]| {
        assert_eq!(dates.len(), closes.len(), "{what}");
        assert!(!dates.is_empty(), "{what}: no bars");
        assert!(dates.windows(2).all(|w| w[0] < w[1]), "{what}: dates must be strictly ascending");
        assert!(dates.iter().all(|d| *d < today && *d >= from), "{what}: every bar must be dated in [{from}, {to}]");
        assert!(closes.iter().all(|c| c.is_finite() && *c > 0.0), "{what}: closes must be finite and positive");
    };

    let spy = src.fetch_daily_bars("SPY", BarClock::StockMidnightNewYork, from, to, today).expect("SPY fetch");
    check_common("SPY", &spy.dates, &spy.closes);
    assert!((15..=22).contains(&spy.dates.len()), "SPY: {} sessions in 30 days looks wrong", spy.dates.len());
    assert!((today - *spy.dates.last().unwrap()).num_days() <= 5, "SPY: newest bar too old");

    let btc = src.fetch_daily_bars("X:BTCUSD", BarClock::MidnightUtc, from, to, today).expect("BTC fetch");
    check_common("BTC", &btc.dates, &btc.closes);
    assert_eq!(btc.dates.len(), 30, "BTC trades every day: 30 bars for 30 days");
    assert_eq!(*btc.dates.last().unwrap(), to, "yesterday's UTC bar is the newest complete bar");

    for p in [&spy.provenance, &btc.provenance] {
        assert_eq!(p.source_id, "massive");
        assert!(!p.request_paths.is_empty() && p.request_paths.iter().all(|r| r.starts_with("/v2/aggs/ticker/") && !r.to_ascii_lowercase().contains("apikey")));
        assert!(p.raw_sha256.iter().all(|h| h.len() == 64));
        if let Ok(k) = std::env::var("MASSIVE_API_KEY") {
            assert!(!format!("{p:?}").contains(k.trim()), "provenance must not contain the key");
        }
    }
    println!(
        "LIVE OK: SPY {} bars {}..{} (request ids {:?}); BTC {} bars {}..{}",
        spy.dates.len(),
        spy.dates[0],
        spy.dates.last().unwrap(),
        spy.provenance.request_ids,
        btc.dates.len(),
        btc.dates[0],
        btc.dates.last().unwrap()
    );
}
