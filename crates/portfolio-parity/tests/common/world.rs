//! The synthetic world, the synthetic trading calendar and the ASSUMED 00:10Z data provider.
//!
//! Nothing here is vendor data. Closes are deterministic formulas (a slow drift plus a per-instrument cycle, so the
//! trend rules flip from time to time), rounded to CENTS so that the same number is exactly representable as an exact
//! decimal (the pipeline's `Dec`) and, to one correctly rounded division, as an `f64` (the backtester's side). The
//! calendar is weekdays minus the US holidays of the U3 tests (general knowledge, not cross-checked against an
//! exchange calendar).

use std::collections::{BTreeMap, BTreeSet};
use std::f64::consts::PI;

use broker_adapters::Dec;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use rebalancer_core::guard::PricePoint;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveKind, SleeveSpec};
use reference_rules::{Panel, PriceSeries, CRYPTO_SYMBOLS, ETF_SYMBOLS};

pub fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
}

pub fn all_days(from: NaiveDate, to: NaiveDate) -> Vec<NaiveDate> {
    let mut v = Vec::new();
    let mut d = from;
    while d <= to {
        v.push(d);
        d += Duration::days(1);
    }
    v
}

fn nth_weekday(y: i32, m: u32, wd: Weekday, n: u32) -> NaiveDate {
    let mut d = date(y, m, 1);
    while d.weekday() != wd {
        d += Duration::days(1);
    }
    d + Duration::days(7 * (i64::from(n) - 1))
}

pub fn month_end_date(y: i32, m: u32) -> NaiveDate {
    if m == 12 {
        date(y, 12, 31)
    } else {
        date(y, m + 1, 1).pred_opt().expect("has a predecessor")
    }
}

fn last_weekday(y: i32, m: u32, wd: Weekday) -> NaiveDate {
    let mut d = month_end_date(y, m);
    while d.weekday() != wd {
        d = d.pred_opt().expect("has a predecessor");
    }
    d
}

/// Western Easter Sunday (anonymous Gregorian algorithm).
fn easter(y: i32) -> NaiveDate {
    let (a, b, c) = (y % 19, y / 100, y % 100);
    let (d, e) = (b / 4, b % 4);
    let f = (b + 8) / 25;
    let g = (b - f + 1) / 3;
    let h = (19 * a + b - d - g + 15) % 30;
    let (i, k) = (c / 4, c % 4);
    let l = (32 + 2 * e + 2 * i - h - k) % 7;
    let m = (a + 11 * h + 22 * l) / 451;
    let month = (h + l - 7 * m + 114) / 31;
    let day = (h + l - 7 * m + 114) % 31 + 1;
    date(y, month as u32, day as u32)
}

/// Weekdays minus holidays. A holiday that falls on a weekend is simply not observed.
pub struct Calendar {
    pub holidays: BTreeSet<NaiveDate>,
}

impl Calendar {
    pub fn us(from_year: i32, to_year: i32) -> Self {
        let mut holidays = BTreeSet::new();
        for y in from_year..=to_year {
            holidays.insert(date(y, 1, 1));
            holidays.insert(nth_weekday(y, 1, Weekday::Mon, 3)); // MLK
            holidays.insert(nth_weekday(y, 2, Weekday::Mon, 3)); // Presidents
            holidays.insert(easter(y) - Duration::days(2)); // Good Friday
            holidays.insert(last_weekday(y, 5, Weekday::Mon)); // Memorial
            holidays.insert(date(y, 7, 4));
            holidays.insert(nth_weekday(y, 9, Weekday::Mon, 1)); // Labor
            holidays.insert(nth_weekday(y, 11, Weekday::Thu, 4)); // Thanksgiving
            holidays.insert(date(y, 12, 25));
        }
        Self { holidays }
    }

    pub fn is_session(&self, d: NaiveDate) -> bool {
        !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) && !self.holidays.contains(&d)
    }

    pub fn sessions(&self, from: NaiveDate, to: NaiveDate) -> Vec<NaiveDate> {
        all_days(from, to).into_iter().filter(|d| self.is_session(*d)).collect()
    }

    pub fn last_session(&self, y: i32, m: u32) -> NaiveDate {
        let mut d = month_end_date(y, m);
        while !self.is_session(d) {
            d = d.pred_opt().expect("has a predecessor");
        }
        d
    }

    pub fn first_session(&self, y: i32, m: u32) -> NaiveDate {
        let mut d = date(y, m, 1);
        while !self.is_session(d) {
            d += Duration::days(1);
        }
        d
    }
}

fn month_index(d: NaiveDate) -> f64 {
    f64::from((d.year() - 2018) * 12 + d.month0() as i32)
}

fn cents(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Slow exponential drift plus a ~7-month cycle per ETF (different phases): month-end signals flip from month to month.
pub fn etf_close(i: usize, d: NaiveDate) -> f64 {
    let phase = [0.0, 1.7, 3.1, 4.6, 5.9][i] + 0.5 * i as f64;
    let base = [250.0, 60.0, 105.0, 16.0, 90.0][i];
    let m = month_index(d);
    let cyc = (2.0 * PI * (m + phase) / 7.0).sin();
    cents(base * (0.004 * m).exp() * (1.0 + 0.10 * cyc) + 0.002 * base * (f64::from(d.day()) / 31.0))
}

/// A ~210-day cycle with a slow drift: BTC and ETH cross their 100-day averages every few months.
pub fn crypto_close(i: usize, d: NaiveDate) -> f64 {
    let base = [9000.0, 300.0][i];
    // the drift term is clamped at k = -1000 (before ~April 2015) so that the 30-year cadence world stays positive
    let k = (d - date(2018, 1, 1)).num_days() as f64;
    cents(base * (1.0 + 0.25 * (2.0 * PI * (k + 40.0 * i as f64) / 210.0).sin() + 0.0005 * k.max(-1000.0)))
}

/// The world: what the market printed. ETF bars on sessions only, crypto bars every calendar day.
pub struct World {
    pub cal: Calendar,
    pub etf_days: Vec<NaiveDate>,
    /// `etf[i][bar]`, `i` indexes `ETF_SYMBOLS`.
    pub etf: Vec<Vec<f64>>,
    pub crypto_days: Vec<NaiveDate>,
    pub crypto: Vec<Vec<f64>>,
}

impl World {
    pub fn build(from: NaiveDate, to: NaiveDate) -> World {
        let cal = Calendar::us(from.year() - 1, to.year() + 1);
        let etf_days = cal.sessions(from, to);
        let etf = (0..ETF_SYMBOLS.len()).map(|i| etf_days.iter().map(|d| etf_close(i, *d)).collect()).collect();
        let crypto_days = all_days(from, to);
        let crypto = (0..CRYPTO_SYMBOLS.len()).map(|i| crypto_days.iter().map(|d| crypto_close(i, *d)).collect()).collect();
        World { cal, etf_days, etf, crypto_days, crypto }
    }

    pub fn etf_symbols() -> Vec<String> {
        ETF_SYMBOLS.iter().map(|s| (*s).to_string()).collect()
    }

    /// The tradable symbol on the venue (`BTC` becomes `BTC/USD`).
    pub fn crypto_pairs() -> Vec<String> {
        CRYPTO_SYMBOLS.iter().map(|s| format!("{s}/USD")).collect()
    }

    /// Close of `symbol` (ETF ticker or `BTC/USD` pair) at the newest bar dated `<= day`; `None` before the first bar.
    pub fn close_at_or_before(&self, symbol: &str, day: NaiveDate) -> Option<f64> {
        if let Some(i) = ETF_SYMBOLS.iter().position(|s| *s == symbol) {
            let n = self.etf_days.partition_point(|d| *d <= day);
            return n.checked_sub(1).map(|b| self.etf[i][b]);
        }
        let base = symbol.strip_suffix("/USD").unwrap_or(symbol);
        let i = CRYPTO_SYMBOLS.iter().position(|s| *s == base)?;
        let n = self.crypto_days.partition_point(|d| *d <= day);
        n.checked_sub(1).map(|b| self.crypto[i][b])
    }

    pub fn is_etf(symbol: &str) -> bool {
        ETF_SYMBOLS.contains(&symbol)
    }
}

/// `x` as the exact decimal string of a cents-rounded close.
pub fn price_dec(x: f64) -> Dec {
    Dec::parse(&format!("{x:.2}")).expect("price decimal")
}

fn slice_series(symbol: &str, days: &[NaiveDate], closes: &[f64], lo: usize, hi: usize) -> PriceSeries {
    PriceSeries::new(symbol, days[lo..hi].to_vec(), closes[lo..hi].to_vec()).expect("valid series")
}

/// The ASSUMED live provider (there is none in the repository, see the U3 tests): at 00:10Z of `as_of` only bars dated
/// STRICTLY BEFORE `as_of` exist. Bounded look-back windows keep long runs fast; they are far above what the rules
/// need (ten month-ends, 100 days).
pub struct Vendor<'w> {
    pub world: &'w World,
    pub etf_window_days: i64,
    pub crypto_window_days: i64,
}

impl<'w> Vendor<'w> {
    pub fn new(world: &'w World) -> Self {
        Self { world, etf_window_days: 500, crypto_window_days: 150 }
    }
}

impl DataSource for Vendor<'_> {
    fn sleeve_data(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        let cutoff = as_of.pred_opt().expect("has a predecessor");
        let (days, cols, syms, window): (&[NaiveDate], &[Vec<f64>], Vec<String>, i64) = match sleeve.kind {
            SleeveKind::EtfTrend => (&self.world.etf_days, &self.world.etf, ETF_SYMBOLS.iter().map(|s| (*s).to_string()).collect(), self.etf_window_days),
            SleeveKind::CryptoTrend => (&self.world.crypto_days, &self.world.crypto, CRYPTO_SYMBOLS.iter().map(|s| (*s).to_string()).collect(), self.crypto_window_days),
        };
        let hi = days.partition_point(|d| *d <= cutoff);
        let lo = days.partition_point(|d| *d < cutoff - Duration::days(window));
        if hi <= lo {
            return Err(DataError::new("DATA_UNAVAILABLE", "no bars before the run date"));
        }
        let series: Vec<PriceSeries> = syms.iter().enumerate().map(|(i, s)| slice_series(s, days, &cols[i], lo, hi)).collect();
        let panel = Panel::new(series).map_err(|e| DataError::new("DATA_UNAVAILABLE", &e.to_string()))?;
        Ok(SleeveData { panel })
    }

    fn prices(&self, symbols: &[String], now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError> {
        let cutoff = now.date_naive().pred_opt().expect("has a predecessor");
        let mut out = BTreeMap::new();
        for s in symbols {
            if let Some(px) = self.world.close_at_or_before(s, cutoff) {
                out.insert(s.clone(), PricePoint { price: price_dec(px), as_of: now });
            }
        }
        Ok(out)
    }
}
