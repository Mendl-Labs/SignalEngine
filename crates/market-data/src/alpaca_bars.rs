//! `AlpacaBarsSource`: the ETF SECONDARY of the two-source gate (R21a: Alpaca daily bars, split-adjusted) as a
//! [`SleeveFetcher`]. Minimal and read-only: `GET https://data.alpaca.markets/v2/stocks/bars` (multi-symbol, daily,
//! `adjustment=split`, paginated by `page_token`), platform DATA credentials (`ALPACA_DATA_KEY_ID` /
//! `ALPACA_DATA_KEY_SECRET`, never a tenant's brokerage key), the same completeness rule as the primary
//! (`crate::time`), the same provenance shape. No broker-adapters Alpaca client existed for market data (the adapter
//! covers trading only, and its `get_quote` is `Unsupported`), so this is the first; it was written to the documented
//! response shape and is fixture-tested, NOT yet run against the real endpoint.
//!
//! Validation: every bar's `t` must be exactly midnight New York (`bar_date(BarClock::StockMidnightNewYork, ..)`),
//! strictly ascending within a symbol; a symbol with no complete bar is `InsufficientHistory`. Failures reuse the
//! crate's typed taxonomy (`MassiveError`, whose classes are the gate's vocabulary; the name predates this source).
//! No retries: the gate is shadow-only, and a transient failure is recorded as `REFUSE_SECONDARY_UNAVAILABLE`
//! (Transient) for the owner to see.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use broker_adapters::transport::{HttpMethod, HttpRequest, HttpTransport};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use reference_rules::{data_fingerprint, Panel, PriceSeries, ETF_SYMBOLS};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::{MassiveError, SleeveError};
use crate::runtime::{MarketClock, SystemClock};
use crate::source::{FetchedSleeve, Provenance, SleeveFetcher, ETF_HISTORY_DAYS};
use crate::time::{bar_date, is_complete, BarClock};

pub const SOURCE_ID: &str = "alpaca";
pub const DEFAULT_BASE_URL: &str = "https://data.alpaca.markets";
pub const ENV_KEY_ID: &str = "ALPACA_DATA_KEY_ID";
pub const ENV_KEY_SECRET: &str = "ALPACA_DATA_KEY_SECRET";
pub const ENV_FEED: &str = "ALPACA_DATA_FEED";
/// R21a accepts SIP for the paper key; `iex` is the free fallback.
pub const DEFAULT_FEED: &str = "sip";
const KEY_HEADER: &str = "APCA-API-KEY-ID";
const SECRET_HEADER: &str = "APCA-API-SECRET-KEY";
const MAX_PAGES: u32 = 10;
const PAGE_LIMIT: u32 = 10_000;

pub struct AlpacaBarsSource {
    key_id: String,
    secret: String,
    base_url: String,
    feed: String,
    history_days: i64,
    settle: Duration,
    transport: Arc<dyn HttpTransport>,
    clock: Arc<dyn MarketClock>,
}

impl fmt::Debug for AlpacaBarsSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlpacaBarsSource").field("base_url", &self.base_url).field("feed", &self.feed).field("key_id", &"<redacted>").finish_non_exhaustive()
    }
}

impl AlpacaBarsSource {
    pub fn new(key_id: &str, secret: &str, transport: Arc<dyn HttpTransport>) -> Result<Self, MassiveError> {
        let key_id = key_id.trim().to_string();
        let secret = secret.trim().to_string();
        if key_id.is_empty() || secret.is_empty() {
            return Err(MassiveError::NotAuthorized { status: 0, detail: format!("{ENV_KEY_ID} / {ENV_KEY_SECRET} are empty") });
        }
        Ok(Self {
            key_id,
            secret,
            base_url: DEFAULT_BASE_URL.to_string(),
            feed: DEFAULT_FEED.to_string(),
            history_days: ETF_HISTORY_DAYS,
            settle: Duration::ZERO,
            transport,
            clock: Arc::new(SystemClock),
        })
    }

    /// From `ALPACA_DATA_KEY_ID`, `ALPACA_DATA_KEY_SECRET` and (optional) `ALPACA_DATA_FEED`.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>, transport: Arc<dyn HttpTransport>) -> Result<Self, MassiveError> {
        let key = lookup(ENV_KEY_ID).unwrap_or_default();
        let secret = lookup(ENV_KEY_SECRET).unwrap_or_default();
        let mut s = Self::new(&key, &secret, transport)?;
        if let Some(feed) = lookup(ENV_FEED).map(|f| f.trim().to_ascii_lowercase()).filter(|f| !f.is_empty()) {
            s = s.with_feed(&feed)?;
        }
        Ok(s)
    }

    pub fn with_base_url(mut self, base_url: &str) -> Self {
        self.base_url = base_url.trim().trim_end_matches('/').to_string();
        self
    }

    pub fn with_feed(mut self, feed: &str) -> Result<Self, MassiveError> {
        match feed {
            "sip" | "iex" => {
                self.feed = feed.to_string();
                Ok(self)
            }
            other => Err(MassiveError::Unsupported { detail: format!("{ENV_FEED}={other:?} is not sip or iex") }),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn MarketClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_settle(mut self, settle: Duration) -> Self {
        self.settle = settle;
        self
    }

    fn scrub(&self, s: &str) -> String {
        s.replace(&self.secret, "<redacted>").replace(&self.key_id, "<redacted>")
    }

    fn request(&self, url: &str) -> HttpRequest {
        HttpRequest {
            method: HttpMethod::Get,
            url: url.to_string(),
            headers: vec![
                (KEY_HEADER.to_string(), self.key_id.clone()),
                (SECRET_HEADER.to_string(), self.secret.clone()),
                ("Accept".to_string(), "application/json".to_string()),
                ("Cache-Control".to_string(), "no-cache, no-store".to_string()),
            ],
            body: None,
        }
    }

    /// Every page of the multi-symbol daily request: `symbol -> (bar date, close)` in the vendor's order, plus the
    /// request records and raw-body hashes.
    fn fetch_pages(&self, from: NaiveDate, to: NaiveDate) -> Result<(Vec<(String, Vec<(NaiveDate, f64)>)>, Vec<String>, Vec<String>, DateTime<Utc>), MassiveError> {
        let symbols = ETF_SYMBOLS.join(",");
        let path = format!("/v2/stocks/bars?symbols={symbols}&timeframe=1Day&start={from}&end={to}&adjustment=split&feed={}&limit={PAGE_LIMIT}&sort=asc", self.feed);
        let mut by_symbol: Vec<(String, Vec<(NaiveDate, f64)>)> = ETF_SYMBOLS.iter().map(|s| (s.to_string(), Vec::new())).collect();
        let mut paths = Vec::new();
        let mut hashes = Vec::new();
        let mut page_token: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let record = match &page_token {
                Some(_) => format!("{path}&page_token=<elided>"),
                None => path.clone(),
            };
            let url = match &page_token {
                Some(t) => format!("{}{path}&page_token={t}", self.base_url),
                None => format!("{}{path}", self.base_url),
            };
            let resp = self.transport.execute_detailed(&self.request(&url)).map_err(|e| MassiveError::Unavailable { detail: self.scrub(&e.to_string()), attempts: 1 })?;
            paths.push(record);
            hashes.push(hex::encode(Sha256::digest(resp.body.as_bytes())));
            let malformed = |detail: String| MassiveError::Malformed { instrument: "alpaca".to_string(), detail: self.scrub(&detail) };
            match resp.status {
                200 => {}
                429 => return Err(MassiveError::RateLimited { attempts: 1, local_budget: false }),
                401 | 403 => return Err(MassiveError::NotAuthorized { status: resp.status, detail: self.scrub(&resp.body.chars().take(200).collect::<String>()) }),
                500..=599 => return Err(MassiveError::Unavailable { detail: format!("HTTP {}", resp.status), attempts: 1 }),
                other => return Err(malformed(format!("unexpected HTTP status {other}"))),
            }
            let v: Value = serde_json::from_str(&resp.body).map_err(|e| malformed(format!("body is not JSON: {e}")))?;
            let bars = v.get("bars").and_then(Value::as_object).ok_or_else(|| malformed("no `bars` object in the body".to_string()))?;
            for (sym, list) in bars {
                let Some(slot) = by_symbol.iter_mut().find(|(s, _)| s == sym) else {
                    return Err(malformed(format!("unexpected symbol {sym:?} in the response")));
                };
                let list = list.as_array().ok_or_else(|| malformed(format!("{sym}: `bars` entry is not an array")))?;
                for b in list {
                    let t = b.get("t").and_then(Value::as_str).ok_or_else(|| malformed(format!("{sym}: a bar has no `t`")))?;
                    let ms = DateTime::parse_from_rfc3339(t).map_err(|e| malformed(format!("{sym}: `t` {t:?} is not RFC 3339: {e}")))?.timestamp_millis();
                    let date = bar_date(BarClock::StockMidnightNewYork, ms).map_err(|e| malformed(format!("{sym}: {e}")))?;
                    let c = b.get("c").and_then(Value::as_f64).ok_or_else(|| malformed(format!("{sym}: a bar has no numeric `c`")))?;
                    if !(c.is_finite() && c > 0.0) {
                        return Err(malformed(format!("{sym}: close {c} on {date} is not a positive finite number")));
                    }
                    slot.1.push((date, c));
                }
            }
            match v.get("next_page_token").and_then(Value::as_str).filter(|s| !s.is_empty()) {
                None => return Ok((by_symbol, paths, hashes, self.clock.now())),
                Some(t) => page_token = Some(t.to_string()),
            }
        }
        Err(MassiveError::Malformed { instrument: "alpaca".to_string(), detail: format!("more than {MAX_PAGES} pages: refusing to follow page_token further") })
    }

    fn fetch_etf(&self, as_of: NaiveDate) -> Result<FetchedSleeve, MassiveError> {
        let unsupported = || MassiveError::Unsupported { detail: format!("run date {as_of} is too close to the calendar's limits") };
        let from = as_of.checked_sub_signed(ChronoDuration::days(self.history_days)).ok_or_else(unsupported)?;
        let to = as_of.checked_sub_signed(ChronoDuration::days(1)).ok_or_else(unsupported)?;
        let (by_symbol, paths, hashes, fetched_at) = self.fetch_pages(from, to)?;
        let now = self.clock.now();
        let mut series = Vec::with_capacity(by_symbol.len());
        let mut provenance = Vec::with_capacity(by_symbol.len());
        for (sym, bars) in by_symbol {
            let mut dates = Vec::with_capacity(bars.len());
            let mut closes = Vec::with_capacity(bars.len());
            let mut dropped = Vec::new();
            for (d, c) in bars {
                if let Some(prev) = dates.last() {
                    if d <= *prev {
                        return Err(MassiveError::Malformed { instrument: sym.clone(), detail: format!("bar dates are not strictly ascending: {prev} then {d}") });
                    }
                }
                if is_complete(BarClock::StockMidnightNewYork, d, as_of, now, self.settle) {
                    dates.push(d);
                    closes.push(c);
                } else {
                    dropped.push(d);
                }
            }
            if dates.is_empty() {
                return Err(MassiveError::InsufficientHistory { symbol: sym, needed: 1, have: 0 });
            }
            let s = PriceSeries::new(sym.clone(), dates.clone(), closes.clone()).map_err(|e| MassiveError::Malformed { instrument: sym.clone(), detail: e.to_string() })?;
            let fingerprint = Panel::new(vec![s.clone()]).map(|p| data_fingerprint(&p)).map_err(|e| MassiveError::Malformed { instrument: sym.clone(), detail: e.to_string() })?;
            provenance.push(Provenance {
                instrument: sym.clone(),
                vendor_ticker: sym.clone(),
                source_id: SOURCE_ID,
                request_paths: paths.clone(),
                request_ids: Vec::new(),
                raw_sha256: hashes.clone(),
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

impl SleeveFetcher for AlpacaBarsSource {
    fn source_id(&self) -> &'static str {
        SOURCE_ID
    }

    fn fetch_sleeve(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<FetchedSleeve, SleeveError> {
        let r = match sleeve.kind {
            SleeveKind::EtfTrend => self.fetch_etf(as_of),
            SleeveKind::CryptoTrend => Err(MassiveError::Unsupported { detail: "Alpaca bars serve the ETF sleeve only; the crypto secondary is Kraken".to_string() }),
        };
        r.map_err(|error| SleeveError { sleeve: sleeve.kind, as_of, error })
    }
}
