//! `KrakenOhlcSource`: the crypto SECONDARY of the two-source gate (R21a: Kraken public OHLC, the execution venue, 720
//! daily candles) as a [`SleeveFetcher`]. `GET https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1440`, no
//! credentials (public endpoint). The broker-adapters Kraken module covers trading and the `Ticker` endpoint only;
//! this is the first OHLC reader, written to the documented shape and fixture-tested, NOT yet run against the real
//! endpoint.
//!
//! A candle's `time` is the UTC start of its day; the bar is dated by that day (`bar_date(BarClock::MidnightUtc, ..)`)
//! and kept only when complete (`crate::time`): the newest candle Kraken returns is the forming day and is dropped.
//! Kraken's `last` field and the volume/vwap/count columns are ignored; `close` is column 4.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use broker_adapters::transport::{HttpMethod, HttpRequest, HttpTransport};
use chrono::{Duration as ChronoDuration, NaiveDate};
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use reference_rules::{data_fingerprint, Panel, PriceSeries, CRYPTO_SYMBOLS};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::{MassiveError, SleeveError};
use crate::runtime::{MarketClock, SystemClock};
use crate::source::{FetchedSleeve, Provenance, SleeveFetcher, CRYPTO_HISTORY_DAYS};
use crate::time::{bar_date, is_complete, BarClock};

pub const SOURCE_ID: &str = "kraken";
pub const DEFAULT_BASE_URL: &str = "https://api.kraken.com";
/// Daily candles.
pub const INTERVAL_MINUTES: u32 = 1440;

/// The altname Kraken takes as `pair` for each canonical crypto symbol (USD quote).
pub fn kraken_pair(symbol: &str) -> Option<&'static str> {
    match symbol {
        "BTC" => Some("XBTUSD"),
        "ETH" => Some("ETHUSD"),
        _ => None,
    }
}

/// `(candles as (date, close), request record, raw-body hash)` of one pair.
type PairCandles = (Vec<(NaiveDate, f64)>, String, String);

pub struct KrakenOhlcSource {
    base_url: String,
    history_days: i64,
    settle: Duration,
    transport: Arc<dyn HttpTransport>,
    clock: Arc<dyn MarketClock>,
}

impl fmt::Debug for KrakenOhlcSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KrakenOhlcSource").field("base_url", &self.base_url).finish_non_exhaustive()
    }
}

impl KrakenOhlcSource {
    pub fn new(transport: Arc<dyn HttpTransport>) -> Self {
        Self { base_url: DEFAULT_BASE_URL.to_string(), history_days: CRYPTO_HISTORY_DAYS, settle: Duration::ZERO, transport, clock: Arc::new(SystemClock) }
    }

    pub fn with_base_url(mut self, base_url: &str) -> Self {
        self.base_url = base_url.trim().trim_end_matches('/').to_string();
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn MarketClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_settle(mut self, settle: Duration) -> Self {
        self.settle = settle;
        self
    }

    /// One pair's candles as `(date, close)`, ascending as the vendor sends them, plus the request record and hash.
    fn fetch_pair(&self, symbol: &str, pair: &str) -> Result<PairCandles, MassiveError> {
        let path = format!("/0/public/OHLC?pair={pair}&interval={INTERVAL_MINUTES}");
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: format!("{}{path}", self.base_url),
            headers: vec![("Accept".to_string(), "application/json".to_string()), ("Cache-Control".to_string(), "no-cache, no-store".to_string())],
            body: None,
        };
        let resp = self.transport.execute_detailed(&req).map_err(|e| MassiveError::Unavailable { detail: e.to_string(), attempts: 1 })?;
        let hash = hex::encode(Sha256::digest(resp.body.as_bytes()));
        let malformed = |detail: String| MassiveError::Malformed { instrument: symbol.to_string(), detail };
        match resp.status {
            200 => {}
            429 => return Err(MassiveError::RateLimited { attempts: 1, local_budget: false }),
            500..=599 => return Err(MassiveError::Unavailable { detail: format!("HTTP {}", resp.status), attempts: 1 }),
            other => return Err(malformed(format!("unexpected HTTP status {other}"))),
        }
        let v: Value = serde_json::from_str(&resp.body).map_err(|e| malformed(format!("body is not JSON: {e}")))?;
        if let Some(errs) = v.get("error").and_then(Value::as_array).filter(|a| !a.is_empty()) {
            let text: Vec<&str> = errs.iter().filter_map(Value::as_str).collect();
            let joined = text.join("; ");
            return Err(if joined.contains("Rate limit") {
                MassiveError::RateLimited { attempts: 1, local_budget: false }
            } else if joined.starts_with("EService") {
                MassiveError::Unavailable { detail: joined, attempts: 1 }
            } else {
                malformed(format!("Kraken error: {joined}"))
            });
        }
        let result = v.get("result").and_then(Value::as_object).ok_or_else(|| malformed("no `result` object".to_string()))?;
        let (_, candles) = result.iter().find(|(k, _)| k.as_str() != "last").ok_or_else(|| malformed("no pair key in `result`".to_string()))?;
        let candles = candles.as_array().ok_or_else(|| malformed("the pair's candles are not an array".to_string()))?;
        let mut out = Vec::with_capacity(candles.len());
        for c in candles {
            let row = c.as_array().ok_or_else(|| malformed("a candle is not an array".to_string()))?;
            let secs = row.first().and_then(Value::as_i64).ok_or_else(|| malformed("a candle has no integer time".to_string()))?;
            let date = bar_date(BarClock::MidnightUtc, secs.checked_mul(1000).ok_or_else(|| malformed(format!("candle time {secs} out of range")))?).map_err(malformed)?;
            let close: f64 = row
                .get(4)
                .and_then(|x| x.as_str().and_then(|s| s.parse().ok()).or_else(|| x.as_f64()))
                .ok_or_else(|| malformed(format!("candle {date} has no parseable close")))?;
            if !(close.is_finite() && close > 0.0) {
                return Err(malformed(format!("close {close} on {date} is not a positive finite number")));
            }
            out.push((date, close));
        }
        Ok((out, path, hash))
    }

    fn fetch_crypto(&self, quote: &str, as_of: NaiveDate) -> Result<FetchedSleeve, MassiveError> {
        if quote != "USD" {
            return Err(MassiveError::Unsupported { detail: format!("crypto quote currency {quote:?} is not supported (USD only)") });
        }
        let from = as_of.checked_sub_signed(ChronoDuration::days(self.history_days)).ok_or_else(|| MassiveError::Unsupported { detail: "date out of range".to_string() })?;
        let now = self.clock.now();
        let mut series = Vec::with_capacity(CRYPTO_SYMBOLS.len());
        let mut provenance = Vec::with_capacity(CRYPTO_SYMBOLS.len());
        for sym in CRYPTO_SYMBOLS {
            let pair = kraken_pair(sym).ok_or_else(|| MassiveError::Unsupported { detail: format!("no Kraken pair for {sym}") })?;
            let (bars, path, hash) = self.fetch_pair(sym, pair)?;
            let fetched_at = self.clock.now();
            let mut dates = Vec::new();
            let mut closes = Vec::new();
            let mut dropped = Vec::new();
            for (d, c) in bars {
                if d < from {
                    continue;
                }
                if let Some(prev) = dates.last() {
                    if d <= *prev {
                        return Err(MassiveError::Malformed { instrument: sym.to_string(), detail: format!("candle dates are not strictly ascending: {prev} then {d}") });
                    }
                }
                if is_complete(BarClock::MidnightUtc, d, as_of, now, self.settle) {
                    dates.push(d);
                    closes.push(c);
                } else {
                    dropped.push(d);
                }
            }
            if dates.is_empty() {
                return Err(MassiveError::InsufficientHistory { symbol: sym.to_string(), needed: 1, have: 0 });
            }
            let s = PriceSeries::new(sym.to_string(), dates, closes.clone()).map_err(|e| MassiveError::Malformed { instrument: sym.to_string(), detail: e.to_string() })?;
            let fingerprint = Panel::new(vec![s.clone()]).map(|p| data_fingerprint(&p)).map_err(|e| MassiveError::Malformed { instrument: sym.to_string(), detail: e.to_string() })?;
            provenance.push(Provenance {
                instrument: sym.to_string(),
                vendor_ticker: pair.to_string(),
                source_id: SOURCE_ID,
                request_paths: vec![path],
                request_ids: Vec::new(),
                raw_sha256: vec![hash],
                fetched_at,
                as_of,
                first_bar: s.first_date(),
                last_bar: s.last_date(),
                last_close: closes[closes.len() - 1],
                bar_count: s.len(),
                dropped_incomplete: dropped,
                fingerprint,
                next_url_scrubbed: false,
            });
            series.push(s);
        }
        let panel = Panel::new(series).map_err(|e| MassiveError::Malformed { instrument: String::new(), detail: e.to_string() })?;
        Ok(FetchedSleeve { panel, provenance, gate: None })
    }
}

impl SleeveFetcher for KrakenOhlcSource {
    fn source_id(&self) -> &'static str {
        SOURCE_ID
    }

    fn fetch_sleeve(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<FetchedSleeve, SleeveError> {
        let r = match sleeve.kind {
            SleeveKind::CryptoTrend => self.fetch_crypto(&sleeve.quote, as_of),
            SleeveKind::EtfTrend => Err(MassiveError::Unsupported { detail: "Kraken OHLC serves the crypto sleeve only; the ETF secondary is Alpaca".to_string() }),
        };
        r.map_err(|error| SleeveError { sleeve: sleeve.kind, as_of, error })
    }
}
