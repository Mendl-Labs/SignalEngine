//! Offline test doubles and synthetic response builders. Public so a later decorator (the two-source gate) and the
//! service wiring can reuse them; nothing here touches the network, a real clock or vendor data.
//!
//! Every response built here is written by hand to the DOCUMENTED Massive shape (see `aggs.rs`); none is copied from a
//! recorded vendor response (those are private and must never enter this public repository).

use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};

use crate::runtime::{Jitter, MarketClock};
use crate::time::new_york_midnight_utc_offset_hours;

/// A clock that only moves when told to, or when something sleeps on it. Sleeping RECORDS the delay and advances the
/// clock by it; nothing ever waits.
pub struct ManualMarketClock {
    now: Mutex<DateTime<Utc>>,
    sleeps: Mutex<Vec<Duration>>,
}

impl ManualMarketClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self { now: Mutex::new(now), sleeps: Mutex::new(Vec::new()) }
    }

    /// The daily run slot of `day`: 00:10 UTC.
    pub fn at_run_slot(day: NaiveDate) -> Self {
        Self::new(day.and_hms_opt(0, 10, 0).expect("valid time").and_utc())
    }

    pub fn set(&self, now: DateTime<Utc>) {
        *self.now.lock().unwrap_or_else(|e| e.into_inner()) = now;
    }

    /// Every sleep so far, in order.
    pub fn sleeps(&self) -> Vec<Duration> {
        self.sleeps.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn total_slept(&self) -> Duration {
        self.sleeps().iter().sum()
    }
}

impl MarketClock for ManualMarketClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn sleep(&self, d: Duration) {
        self.sleeps.lock().unwrap_or_else(|e| e.into_inner()).push(d);
        let mut n = self.now.lock().unwrap_or_else(|e| e.into_inner());
        *n += chrono::Duration::from_std(d).unwrap_or(chrono::Duration::zero());
    }
}

/// A jitter that always answers the same factor.
pub struct FixedJitter(pub f64);

impl Jitter for FixedJitter {
    fn factor(&self) -> f64 {
        self.0
    }
}

/// `t` (ms) of the stock daily bar of `date`: midnight New York (04:00 UTC in daylight time, 05:00 in standard).
pub fn stock_ts(date: NaiveDate) -> i64 {
    let off = new_york_midnight_utc_offset_hours(date).expect("year supported");
    date.and_hms_opt(off, 0, 0).expect("valid time").and_utc().timestamp_millis()
}

/// `t` (ms) of the crypto daily bar of `date`: 00:00 UTC.
pub fn crypto_ts(date: NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0).expect("valid time").and_utc().timestamp_millis()
}

/// A page in the documented shape. `bars` are `(t in ms, close)`. Status `OK`, `adjusted: true`, counts consistent.
pub fn page_json(ticker: &str, bars: &[(i64, f64)], next_url: Option<&str>) -> String {
    let results: Vec<String> = bars
        .iter()
        .map(|(t, c)| format!(r#"{{"v":1000.0,"vw":{c},"o":{c},"c":{c},"h":{c},"l":{c},"t":{t},"n":10}}"#))
        .collect();
    let next = next_url.map(|u| format!(r#","next_url":"{u}""#)).unwrap_or_default();
    format!(
        r#"{{"ticker":"{ticker}","queryCount":{n},"resultsCount":{n},"adjusted":true,"status":"OK","request_id":"req-{ticker_id}-{n}","count":{n},"results":[{r}]{next}}}"#,
        n = bars.len(),
        ticker_id = ticker.replace(':', "_"),
        r = results.join(","),
    )
}

/// The documented empty-result shape (no `results` key).
pub fn empty_page_json(ticker: &str) -> String {
    format!(r#"{{"ticker":"{ticker}","queryCount":0,"resultsCount":0,"adjusted":true,"status":"OK","request_id":"req-empty","count":0}}"#)
}

/// The `403` body of the aggregates/snapshot families: `{"status":"NOT_AUTHORIZED","message":...}`.
pub const NOT_AUTHORIZED_MESSAGE_BODY: &str = r#"{"status":"NOT_AUTHORIZED","request_id":"req-403a","message":"You are not entitled to this data. Please upgrade your plan."}"#;
/// The other documented `403` shape: `{"status":"ERROR","error":...}`.
pub const NOT_AUTHORIZED_ERROR_BODY: &str = r#"{"status":"ERROR","request_id":"req-403b","error":"You are not entitled to this data. Please upgrade your plan."}"#;
/// A `429` body (the vendor also sends an empty one).
pub const RATE_LIMIT_BODY: &str = r#"{"status":"ERROR","request_id":"req-429","error":"You've exceeded the maximum requests per minute, please wait or upgrade your subscription to continue."}"#;
