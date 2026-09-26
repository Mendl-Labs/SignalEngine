//! Shared harness for the market-data tests: a synthetic "vendor" (a `FakeTransport` handler answering in the
//! documented Massive shape from an in-memory world), a manual clock at the 00:10Z run slot, and small builders.
//!
//! Everything is hand-made synthetic data. Nothing here comes from a recorded vendor response.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpRequest, HttpResponse};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use market_data::testing::{crypto_ts, page_json, stock_ts, FixedJitter, ManualMarketClock};
use market_data::{MassiveConfig, MassiveDataSource, StaticKeyProvider};
use rebalancer_core::Dec;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveKind, SleeveSpec};
use reference_rules::{Panel, CRYPTO_SYMBOLS, ETF_SYMBOLS};

/// A recognisable fake key: every test that checks for leaks searches for this value.
pub const KEY: &str = "TESTKEY-9f3b7c1d2e4a";

pub fn d(y: i32, m: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, day).unwrap()
}

pub fn dec(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

pub fn at(date: NaiveDate, h: u32, m: u32) -> DateTime<Utc> {
    date.and_hms_opt(h, m, 0).unwrap().and_utc()
}

pub fn etf_sleeve() -> SleeveSpec {
    SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: dec("0.5"), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "USD".into() }
}

pub fn crypto_sleeve() -> SleeveSpec {
    SleeveSpec { id: "crypto".into(), kind: SleeveKind::CryptoTrend, share: dec("0.5"), venue: "kraken".into(), asset_class: "crypto_spot".into(), quote: "USD".into() }
}

// ---------------------------------------------------------------------------------------------------------------
// Synthetic worlds
// ---------------------------------------------------------------------------------------------------------------

fn nth_weekday(y: i32, m: u32, wd: Weekday, n: u32) -> NaiveDate {
    let mut day = d(y, m, 1);
    while day.weekday() != wd {
        day += Duration::days(1);
    }
    day + Duration::days(7 * (n as i64 - 1))
}

fn last_monday_of_may(y: i32) -> NaiveDate {
    let mut day = d(y, 5, 31);
    while day.weekday() != Weekday::Mon {
        day -= Duration::days(1);
    }
    day
}

/// A small US holiday set (New Year, MLK, Presidents, Memorial, July 4, Labor, Thanksgiving, Christmas). Not an
/// exchange calendar; it only has to leave realistic holes in the synthetic weekday series.
pub fn is_holiday(day: NaiveDate) -> bool {
    let y = day.year();
    [
        d(y, 1, 1),
        nth_weekday(y, 1, Weekday::Mon, 3),
        nth_weekday(y, 2, Weekday::Mon, 3),
        last_monday_of_may(y),
        d(y, 7, 4),
        nth_weekday(y, 9, Weekday::Mon, 1),
        nth_weekday(y, 11, Weekday::Thu, 4),
        d(y, 12, 25),
    ]
    .contains(&day)
}

pub fn is_session(day: NaiveDate) -> bool {
    !matches!(day.weekday(), Weekday::Sat | Weekday::Sun) && !is_holiday(day)
}

fn close_of(idx: usize, i: usize, base: f64) -> f64 {
    let x = i as f64;
    let v = base * (1.0 + 0.25 * ((x / 37.0) + idx as f64).sin() + 0.0004 * x);
    (v * 100.0).round() / 100.0
}

pub type Bars = Vec<(NaiveDate, f64)>;

/// The synthetic vendor's data: bars by vendor ticker.
#[derive(Clone, Default)]
pub struct World {
    pub bars: BTreeMap<String, Bars>,
    /// A vendor that honours the request's `to` date. `false` (the default) is the worst case: it returns every bar it
    /// has after `from`, forming bar included, so the SOURCE's own completeness filter is what gets exercised.
    pub honor_to: bool,
}

impl World {
    /// Five ETFs on the weekday-minus-holidays calendar from `from` to `to` inclusive.
    pub fn etf(from: NaiveDate, to: NaiveDate) -> World {
        let mut w = World::default();
        for (idx, sym) in ETF_SYMBOLS.iter().enumerate() {
            let mut day = from;
            let mut i = 0usize;
            let mut bars = Vec::new();
            while day <= to {
                if is_session(day) {
                    bars.push((day, close_of(idx, i, 50.0 + 20.0 * idx as f64)));
                    i += 1;
                }
                day += Duration::days(1);
            }
            w.bars.insert((*sym).to_string(), bars);
        }
        w
    }

    /// BTC and ETH, every calendar day from `from` to `to` inclusive.
    pub fn crypto(from: NaiveDate, to: NaiveDate) -> World {
        let mut w = World::default();
        for (idx, sym) in CRYPTO_SYMBOLS.iter().enumerate() {
            let mut day = from;
            let mut i = 0usize;
            let mut bars = Vec::new();
            while day <= to {
                bars.push((day, close_of(idx, i, 9000.0 / (1.0 + 20.0 * idx as f64))));
                i += 1;
                day += Duration::days(1);
            }
            w.bars.insert(format!("X:{sym}USD"), bars);
        }
        w
    }

    pub fn merge(mut self, other: World) -> World {
        self.bars.extend(other.bars);
        self
    }

    pub fn of(&self, ticker: &str) -> &Bars {
        &self.bars[ticker]
    }

    pub fn remove_date(&mut self, ticker: &str, date: NaiveDate) {
        self.bars.get_mut(ticker).unwrap().retain(|(x, _)| *x != date);
    }

    pub fn set_close(&mut self, ticker: &str, date: NaiveDate, close: f64) {
        for (x, c) in self.bars.get_mut(ticker).unwrap() {
            if *x == date {
                *c = close;
            }
        }
    }

    pub fn truncate_after(&mut self, ticker: &str, last: NaiveDate) {
        self.bars.get_mut(ticker).unwrap().retain(|(x, _)| *x <= last);
    }

    pub fn ts(ticker: &str, date: NaiveDate) -> i64 {
        if ticker.starts_with("X:") {
            crypto_ts(date)
        } else {
            stock_ts(date)
        }
    }

    /// What the synthetic vendor answers for one request URL.
    pub fn respond(&self, req: &HttpRequest) -> String {
        let (ticker, from, to) = parse_range_url(&req.url).expect("the source only ever asks for the range endpoint");
        let bars: Vec<(i64, f64)> = self
            .bars
            .get(&ticker)
            .map(|v| v.iter().filter(|(x, _)| *x >= from && (!self.honor_to || *x <= to)).map(|(x, c)| (Self::ts(&ticker, *x), *c)).collect())
            .unwrap_or_default();
        if bars.is_empty() {
            market_data::testing::empty_page_json(&ticker)
        } else {
            page_json(&ticker, &bars, None)
        }
    }
}

/// `(ticker, from, to)` of `https://host/v2/aggs/ticker/{T}/range/1/day/{from}/{to}?...`.
pub fn parse_range_url(url: &str) -> Option<(String, NaiveDate, NaiveDate)> {
    let rest = url.split_once("/v2/aggs/ticker/")?.1;
    let (ticker, tail) = rest.split_once("/range/1/day/")?;
    let range = tail.split('?').next()?;
    let (from, to) = range.split_once('/')?;
    Some((ticker.to_string(), from.parse().ok()?, to.parse().ok()?))
}

// ---------------------------------------------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------------------------------------------

pub struct Harness {
    pub src: MassiveDataSource,
    pub transport: Arc<FakeTransport>,
    pub clock: Arc<ManualMarketClock>,
    pub world: Arc<Mutex<World>>,
}

impl Harness {
    /// A source whose clock stands at 00:10Z of `run_date` and whose jitter factor is 1.0 (delays are exactly the
    /// exponential schedule).
    pub fn new(run_date: NaiveDate) -> Harness {
        Self::with_config(run_date, MassiveConfig::default())
    }

    pub fn with_config(run_date: NaiveDate, cfg: MassiveConfig) -> Harness {
        let transport = Arc::new(FakeTransport::new());
        let clock = Arc::new(ManualMarketClock::at_run_slot(run_date));
        let src = MassiveDataSource::new(StaticKeyProvider::new(KEY).unwrap(), transport.clone())
            .with_config(cfg)
            .expect("valid config")
            .with_clock(clock.clone())
            .with_jitter(Arc::new(FixedJitter(1.0)));
        Harness { src, transport, clock, world: Arc::new(Mutex::new(World::default())) }
    }

    /// Answer every request from `world` (the world stays editable through `self.world`).
    pub fn serve(&self, world: World) -> &Self {
        *self.world.lock().unwrap() = world;
        let w = self.world.clone();
        self.transport.set_handler(move |req| Ok(HttpResponse { status: 200, body: w.lock().unwrap().respond(req) }));
        self
    }

    pub fn etf(&self, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        self.src.sleeve_data(&etf_sleeve(), as_of)
    }

    pub fn crypto(&self, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        self.src.sleeve_data(&crypto_sleeve(), as_of)
    }
}

pub fn bars_of(panel: &Panel, symbol: &str) -> Bars {
    let s = panel.get(symbol).unwrap();
    s.dates().iter().copied().zip(s.closes().iter().copied()).collect()
}

/// A standard 2019-2020 pair of worlds and an `as_of` inside them (Wednesday 2020-06-17: ETF sessions to Tuesday the 16th).
pub fn standard_world() -> World {
    World::etf(d(2018, 6, 1), d(2020, 12, 31)).merge(World::crypto(d(2019, 6, 1), d(2020, 12, 31)))
}

pub const AS_OF: (i32, u32, u32) = (2020, 6, 17);

pub fn as_of() -> NaiveDate {
    d(AS_OF.0, AS_OF.1, AS_OF.2)
}
