//! `MassiveDataSource`: the first real market-data source of the rebalancer. See the crate docs (`lib.rs`) for the
//! design, the completeness rule (`time.rs`) and the error taxonomy (`error.rs`).

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broker_adapters::transport::{HttpMethod, HttpRequest, HttpResponseDetailed, HttpTransport};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use rebalancer_core::guard::PricePoint;
use rebalancer_run::data::{DataError, DataGateReport, DataSource, SleeveData, SleeveKind, SleeveSpec};
use reference_rules::options::{CRYPTO_MAX_STALE_DAYS, ETF_MAX_STALE_DAYS};
use reference_rules::{completed_month_end_dates, data_fingerprint, Panel, PriceSeries, CRYPTO_SMA_DAYS, CRYPTO_SYMBOLS, ETF_SMA_MONTH_ENDS, ETF_SYMBOLS};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::aggs::{bounded, parse_aggs_page, AggBar};
use crate::error::{MassiveError, SleeveError};
use crate::runtime::{Budget, BudgetConfig, Jitter, MarketClock, RetryPolicy, SystemClock, SystemJitter};
use crate::secret::{scrub, KeyProvider, SecretString};
use crate::time::{bar_date, is_complete, nominal_close_at, BarClock};
use crate::url::{base_authority, record_of, sanitize_next_url};

/// The source id every [`Provenance`] carries.
pub const SOURCE_ID: &str = "massive";
pub const DEFAULT_BASE_URL: &str = "https://api.massive.com";
/// History requested for an ETF sleeve: the live ticket's window (`asof - 520 days`), which holds far more than the
/// ten completed month-ends the rule needs.
pub const ETF_HISTORY_DAYS: i64 = 520;
/// History requested for a crypto sleeve: the rule needs 100 completed daily bars.
pub const CRYPTO_HISTORY_DAYS: i64 = 150;
/// A response carrying an `Age` header above this many seconds came out of an intermediary cache and is not accepted.
pub const MAX_RESPONSE_AGE_SECS: u64 = 60;
/// Provenance records kept in memory (oldest dropped first).
const PROVENANCE_KEPT: usize = 128;

const QUERY: &str = "adjusted=true&sort=asc&limit=50000";

/// Margins added to the completeness rule of `time.rs` for a vendor that finalises late. Both default to zero because
/// the real delay is NOT measured (MASSIVE_API_ENTITLEMENTS section 9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Completeness {
    pub stock_settle: Duration,
    pub crypto_settle: Duration,
}

#[derive(Debug, Clone)]
pub struct MassiveConfig {
    /// `https://host[:port]`. `next_url`s are only followed on this host.
    pub base_url: String,
    pub etf_history_days: i64,
    pub crypto_history_days: i64,
    pub retry: RetryPolicy,
    pub budget: BudgetConfig,
    /// Pages followed per instrument (one page holds up to 50,000 bars, so 2+ pages never happen in practice).
    pub max_pages: u32,
    pub completeness: Completeness,
}

impl Default for MassiveConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            etf_history_days: ETF_HISTORY_DAYS,
            crypto_history_days: CRYPTO_HISTORY_DAYS,
            retry: RetryPolicy::default(),
            budget: BudgetConfig::default(),
            max_pages: 5,
            completeness: Completeness::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid base URL: {0}")]
    BaseUrl(String),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl MassiveConfig {
    fn validate(&self) -> Result<String, ConfigError> {
        let authority = base_authority(&self.base_url).map_err(ConfigError::BaseUrl)?;
        let bad = |m: &str| Err(ConfigError::Invalid(m.to_string()));
        if self.retry.max_attempts < 1 {
            return bad("retry.max_attempts must be at least 1");
        }
        if self.budget.max_requests < 1 || self.budget.window.is_zero() {
            return bad("the request budget must allow at least one request per non-empty window");
        }
        if self.max_pages < 1 {
            return bad("max_pages must be at least 1");
        }
        if self.etf_history_days < 300 {
            return bad("etf_history_days must be at least 300 (ten completed month-ends)");
        }
        if self.crypto_history_days < CRYPTO_SMA_DAYS as i64 {
            return bad("crypto_history_days must be at least 100");
        }
        Ok(authority)
    }
}

/// Where a sleeve's bars came from, per instrument. Nothing in it is secret: request paths carry no key and no cursor
/// value, and the raw responses appear only as hashes.
#[derive(Debug, Clone, PartialEq)]
pub struct Provenance {
    /// The canonical symbol of the panel (`SPY`, `BTC`).
    pub instrument: String,
    /// The vendor's ticker (`SPY`, `X:BTCUSD`).
    pub vendor_ticker: String,
    pub source_id: &'static str,
    /// Path (+ query, cursor elided) of every request made, in order: the first page then any `next_url` pages.
    pub request_paths: Vec<String>,
    /// `x-request-id` headers and body `request_id`s seen (deduplicated, in order).
    pub request_ids: Vec<String>,
    /// SHA-256 (lowercase hex) of each raw response body.
    pub raw_sha256: Vec<String>,
    pub fetched_at: DateTime<Utc>,
    pub as_of: NaiveDate,
    /// First and last COMPLETE bar returned, and the last bar's close (the decision-day close of a crypto sleeve).
    pub first_bar: NaiveDate,
    pub last_bar: NaiveDate,
    pub last_close: f64,
    pub bar_count: usize,
    /// Bars the vendor sent that were dropped as not provably complete.
    pub dropped_incomplete: Vec<NaiveDate>,
    /// `reference_rules::data_fingerprint` of this instrument's returned series (a one-series panel).
    pub fingerprint: String,
    /// The vendor's `next_url` carried a key parameter, which was removed before following it.
    pub next_url_scrubbed: bool,
}

/// A validated sleeve panel with its provenance.
#[derive(Debug, Clone)]
pub struct FetchedSleeve {
    pub panel: Panel,
    pub provenance: Vec<Provenance>,
    /// Set by the two-source gate (`crate::gate`) when one wrapped the fetch; `None` from a plain source.
    pub gate: Option<DataGateReport>,
}

/// The complete bars of one ticker (see `MassiveDataSource::fetch_daily_bars`).
#[derive(Debug, Clone)]
pub struct DailyBars {
    pub dates: Vec<NaiveDate>,
    pub closes: Vec<f64>,
    pub provenance: Provenance,
}

/// The TYPED fetch seam. A two-source gate is a decorator over this: it holds two fetchers (a primary and a
/// secondary), compares their panels and provenance, and keeps every failure typed (`SleeveError` carries the class
/// of failure, the sleeve kind and the run date). [`SleevesFrom`] turns any fetcher into a `DataSource`.
pub trait SleeveFetcher: Send + Sync {
    /// Stable id of the source (`massive`, later `alpaca`, `kraken`).
    fn source_id(&self) -> &'static str;
    fn fetch_sleeve(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<FetchedSleeve, SleeveError>;
}

impl<T: SleeveFetcher + ?Sized> SleeveFetcher for &T {
    fn source_id(&self) -> &'static str {
        (**self).source_id()
    }
    fn fetch_sleeve(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<FetchedSleeve, SleeveError> {
        (**self).fetch_sleeve(sleeve, as_of)
    }
}

impl<T: SleeveFetcher + ?Sized> SleeveFetcher for Arc<T> {
    fn source_id(&self) -> &'static str {
        (**self).source_id()
    }
    fn fetch_sleeve(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<FetchedSleeve, SleeveError> {
        (**self).fetch_sleeve(sleeve, as_of)
    }
}

/// Any [`SleeveFetcher`] as a `DataSource`. Sizing prices are not a fetcher's job: `prices` refuses (`Unsupported`);
/// compose with [`WithPrices`] to take them from somewhere else (the broker's quotes).
pub struct SleevesFrom<F: SleeveFetcher>(pub F);

impl<F: SleeveFetcher> DataSource for SleevesFrom<F> {
    fn sleeve_data(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        self.0.fetch_sleeve(sleeve, as_of).map(|f| SleeveData { panel: f.panel, gate: f.gate }).map_err(DataError::from)
    }

    fn prices(&self, _symbols: &[String], _now: DateTime<Utc>) -> Result<std::collections::BTreeMap<String, PricePoint>, DataError> {
        Err(prices_unsupported())
    }
}

/// Sleeve panels from one `DataSource`, sizing prices from another.
pub struct WithPrices<S: DataSource, P: DataSource> {
    pub sleeves: S,
    pub prices: P,
}

impl<S: DataSource, P: DataSource> DataSource for WithPrices<S, P> {
    fn sleeve_data(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        self.sleeves.sleeve_data(sleeve, as_of)
    }

    fn prices(&self, symbols: &[String], now: DateTime<Utc>) -> Result<std::collections::BTreeMap<String, PricePoint>, DataError> {
        self.prices.prices(symbols, now)
    }
}

fn prices_unsupported() -> DataError {
    MassiveError::Unsupported {
        detail: "this source supplies daily bars only, not sizing prices (Massive stocks are 15-minute delayed and a daily close is a day old): compose it with a price source, see WithPrices".to_string(),
    }
    .into()
}

/// One instrument's request plan.
struct Plan {
    /// Canonical symbol of the panel.
    symbol: String,
    /// The vendor's ticker.
    ticker: String,
    clock: BarClock,
    from: NaiveDate,
    to: NaiveDate,
    settle: Duration,
}

/// Every page of one instrument's range request after validation, BEFORE the completeness filter.
struct RawFetch {
    /// `(bar date, the vendor's values)`, strictly ascending.
    bars: Vec<(NaiveDate, AggBar)>,
    request_paths: Vec<String>,
    request_ids: Vec<String>,
    raw_sha256: Vec<String>,
    scrubbed: bool,
    fetched_at: DateTime<Utc>,
}

/// One bar as the vendor shows it right now (see `MassiveDataSource::observe_recent_bars`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ObservedBar {
    pub date: NaiveDate,
    /// The venue's nominal close of this bar's date ([`nominal_close_at`]).
    pub nominal_close_at: DateTime<Utc>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: f64,
    pub volume: Option<f64>,
}

/// The unfiltered bars of one ticker plus what identifies the responses they came from.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedBars {
    /// Oldest first; may include a forming bar.
    pub bars: Vec<ObservedBar>,
    /// SHA-256 (lowercase hex) of each raw response body, one per page.
    pub raw_sha256: Vec<String>,
    pub request_ids: Vec<String>,
    pub fetched_at: DateTime<Utc>,
}

/// What one instrument's fetch produced, after validation and the completeness filter.
struct InstrumentFetch {
    symbol: String,
    ticker: String,
    dates: Vec<NaiveDate>,
    closes: Vec<f64>,
    dropped: Vec<NaiveDate>,
    request_paths: Vec<String>,
    request_ids: Vec<String>,
    raw_sha256: Vec<String>,
    fetched_at: DateTime<Utc>,
    scrubbed: bool,
}

enum Outcome {
    Done(HttpResponseDetailed),
    /// Worth another attempt: the text says why (for the final error).
    Retry { rate_limited: bool, detail: String },
}

pub struct MassiveDataSource {
    cfg: MassiveConfig,
    authority: String,
    keys: Box<dyn KeyProvider>,
    transport: Arc<dyn HttpTransport>,
    clock: Arc<dyn MarketClock>,
    jitter: Arc<dyn Jitter>,
    budget: Budget,
    provenance: Mutex<VecDeque<Provenance>>,
}

impl fmt::Debug for MassiveDataSource {
    /// Never prints the key provider (a custom provider could print its key) or anything else secret.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MassiveDataSource").field("base_url", &self.cfg.base_url).field("key", &"<redacted>").finish_non_exhaustive()
    }
}

impl MassiveDataSource {
    /// A source with the default configuration, the real clock and real jitter. `transport` is the HTTP boundary
    /// (`broker_adapters::transport::HttpTransport`); tests pass `FakeTransport`.
    pub fn new(keys: impl KeyProvider + 'static, transport: Arc<dyn HttpTransport>) -> Self {
        let cfg = MassiveConfig::default();
        let authority = cfg.validate().unwrap_or_else(|_| "api.massive.com".to_string());
        Self {
            budget: Budget::new(cfg.budget),
            cfg,
            authority,
            keys: Box::new(keys),
            transport,
            clock: Arc::new(SystemClock),
            jitter: Arc::new(SystemJitter::default()),
            provenance: Mutex::new(VecDeque::new()),
        }
    }

    pub fn with_config(mut self, cfg: MassiveConfig) -> Result<Self, ConfigError> {
        self.authority = cfg.validate()?;
        self.budget = Budget::new(cfg.budget);
        self.cfg = cfg;
        Ok(self)
    }

    pub fn with_clock(mut self, clock: Arc<dyn MarketClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_jitter(mut self, jitter: Arc<dyn Jitter>) -> Self {
        self.jitter = jitter;
        self
    }

    /// The provenance of the most recent fetches (oldest first, bounded).
    pub fn recent_provenance(&self) -> Vec<Provenance> {
        self.provenance.lock().unwrap_or_else(|e| e.into_inner()).iter().cloned().collect()
    }

    fn key(&self) -> Result<SecretString, MassiveError> {
        self.keys.api_key().map_err(|e| MassiveError::NotAuthorized { status: 0, detail: e.to_string() })
    }

    fn build_request(&self, url: &str, key: &SecretString) -> HttpRequest {
        HttpRequest {
            method: HttpMethod::Get,
            url: url.to_string(),
            headers: vec![
                ("Authorization".to_string(), format!("Bearer {}", key.expose())),
                ("Accept".to_string(), "application/json".to_string()),
                // Cache bypass: see the crate docs ("Caches") for what is and is not verified.
                ("Cache-Control".to_string(), "no-cache, no-store".to_string()),
                ("Pragma".to_string(), "no-cache".to_string()),
            ],
            body: None,
        }
    }

    fn classify(&self, r: HttpResponseDetailed, key: &SecretString, label: &str) -> Result<Outcome, MassiveError> {
        let sc = |s: &str| scrub(s, Some(key.expose()));
        match r.status {
            200 => {
                let age = r.header("age").and_then(|a| a.trim().parse::<u64>().ok());
                if let Some(age) = age.filter(|a| *a > MAX_RESPONSE_AGE_SECS) {
                    return Ok(Outcome::Retry { rate_limited: false, detail: format!("the response was served from a cache (Age: {age}s)") });
                }
                Ok(Outcome::Done(r))
            }
            429 => Ok(Outcome::Retry { rate_limited: true, detail: "HTTP 429".to_string() }),
            500..=599 => Ok(Outcome::Retry { rate_limited: false, detail: format!("HTTP {}", r.status) }),
            401 | 403 => {
                // Two documented body shapes: {"status":"NOT_AUTHORIZED","message":..} and {"status":"ERROR","error":..}.
                // The status code decides; the text only explains.
                let text = serde_json::from_str::<Value>(&r.body)
                    .ok()
                    .and_then(|v| v.get("message").or_else(|| v.get("error")).and_then(Value::as_str).map(|m| bounded(&sc(m))))
                    .unwrap_or_else(|| "no explanation in the body".to_string());
                Err(MassiveError::NotAuthorized { status: r.status, detail: text })
            }
            other => Err(MassiveError::Malformed { instrument: label.to_string(), detail: format!("unexpected HTTP status {other}") }),
        }
    }

    /// One request with bounded retries. Never sleeps on a real clock in tests (the injected `MarketClock` sleeps).
    fn send(&self, url: &str, key: &SecretString, label: &str) -> Result<HttpResponseDetailed, MassiveError> {
        let req = self.build_request(url, key);
        let max = self.cfg.retry.max_attempts.max(1);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            if !self.budget.take(self.clock.now()) {
                return Err(MassiveError::RateLimited { attempts: attempt - 1, local_budget: true });
            }
            let outcome = match self.transport.execute_detailed(&req) {
                Ok(r) => self.classify(r, key, label)?,
                Err(e) => Outcome::Retry { rate_limited: false, detail: scrub(&e.to_string(), Some(key.expose())) },
            };
            match outcome {
                Outcome::Done(r) => return Ok(r),
                Outcome::Retry { rate_limited, detail } => {
                    if attempt >= max {
                        return Err(if rate_limited { MassiveError::RateLimited { attempts: attempt, local_budget: false } } else { MassiveError::Unavailable { detail, attempts: attempt } });
                    }
                    self.clock.sleep(self.cfg.retry.delay_after(attempt, self.jitter.factor()));
                }
            }
        }
    }

    /// Every page of one instrument's range request, parsed and date-validated (strictly ascending, by the clock's
    /// convention), WITHOUT the completeness filter. `fetch_instrument` applies the filter for the rules;
    /// `observe_recent_bars` deliberately does not (it exists to see a bar the moment the vendor shows it).
    fn fetch_pages(&self, plan: &Plan, key: &SecretString) -> Result<RawFetch, MassiveError> {
        let malformed = |detail: String| MassiveError::Malformed { instrument: plan.symbol.clone(), detail: scrub(&detail, Some(key.expose())) };
        let path = format!("/v2/aggs/ticker/{}/range/1/day/{}/{}", plan.ticker, plan.from, plan.to);
        let mut url = format!("https://{}{path}?{QUERY}", self.authority);
        let mut record = record_of(&path, QUERY);

        let mut raw: Vec<AggBar> = Vec::new();
        let mut request_paths = Vec::new();
        let mut request_ids: Vec<String> = Vec::new();
        let mut raw_sha256 = Vec::new();
        let mut scrubbed = false;
        let mut seen: BTreeSet<String> = BTreeSet::new();

        for page_no in 1..=self.cfg.max_pages {
            if !seen.insert(url.clone()) {
                return Err(malformed("next_url leads back to a page already fetched".to_string()));
            }
            let resp = self.send(&url, key, &plan.symbol)?;
            request_paths.push(record.clone());
            raw_sha256.push(hex::encode(Sha256::digest(resp.body.as_bytes())));
            let mut note_id = |id: &str| {
                if !id.is_empty() && !request_ids.iter().any(|x| x == id) {
                    request_ids.push(id.to_string());
                }
            };
            if let Some(h) = resp.header("x-request-id") {
                note_id(&h.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_').take(64).collect::<String>());
            }
            let page = parse_aggs_page(resp.body.as_bytes(), &plan.ticker, Some(key.expose())).map_err(malformed)?;
            if let Some(id) = &page.request_id {
                note_id(id);
            }
            raw.extend(page.bars);
            match page.next_url {
                None => break,
                Some(next) => {
                    if page_no == self.cfg.max_pages {
                        return Err(malformed(format!("more than {} pages: refusing to follow next_url further", self.cfg.max_pages)));
                    }
                    let s = sanitize_next_url(&next, &self.authority, Some(key.expose())).map_err(malformed)?;
                    scrubbed |= s.scrubbed;
                    url = s.url;
                    record = s.record;
                }
            }
        }

        // Dates by the documented convention; strictly ascending across ALL pages (a duplicate is not ascending).
        let mut bars: Vec<(NaiveDate, AggBar)> = Vec::with_capacity(raw.len());
        for b in raw {
            let d = bar_date(plan.clock, b.t).map_err(malformed)?;
            if let Some((prev, _)) = bars.last() {
                if d <= *prev {
                    return Err(malformed(format!("bar dates are not strictly ascending: {prev} then {d}")));
                }
            }
            bars.push((d, b));
        }
        Ok(RawFetch { bars, request_paths, request_ids, raw_sha256, scrubbed, fetched_at: self.clock.now() })
    }

    fn fetch_instrument(&self, plan: &Plan, as_of: NaiveDate, key: &SecretString) -> Result<InstrumentFetch, MassiveError> {
        let raw = self.fetch_pages(plan, key)?;

        // The completeness filter: only bars proven complete are kept.
        let now = self.clock.now();
        let mut kept_dates = Vec::with_capacity(raw.bars.len());
        let mut kept_closes = Vec::with_capacity(raw.bars.len());
        let mut dropped = Vec::new();
        for (d, b) in raw.bars {
            if is_complete(plan.clock, d, as_of, now, plan.settle) {
                kept_dates.push(d);
                kept_closes.push(b.close);
            } else {
                dropped.push(d);
            }
        }
        Ok(InstrumentFetch {
            symbol: plan.symbol.clone(),
            ticker: plan.ticker.clone(),
            dates: kept_dates,
            closes: kept_closes,
            dropped,
            request_paths: raw.request_paths,
            request_ids: raw.request_ids,
            raw_sha256: raw.raw_sha256,
            fetched_at: raw.fetched_at,
            scrubbed: raw.scrubbed,
        })
    }

    /// The daily bars of ONE vendor ticker in `[from, to]` exactly as the vendor shows them RIGHT NOW: validated
    /// (shape, ticker, adjusted flag, stamp convention, ascending dates) but NOT filtered for completeness, each with
    /// its nominal close ([`nominal_close_at`]). For the first-seen-latency / revision recorder ONLY
    /// (`rebalancer_run::latency`, W9.1): an observer must see a bar the moment the vendor publishes it, forming bar
    /// included, and the recorder's own pre-registered policy decides what to sample. Nothing returned here may feed a
    /// rule; `sleeve_data` / `fetch_daily_bars` keep the completeness rule. No provenance is remembered for these
    /// fetches (the response hashes are returned instead), so an observer polling every tick cannot push a decision
    /// fetch's provenance out of the bounded log.
    pub fn observe_recent_bars(&self, ticker: &str, clock: BarClock, from: NaiveDate, to: NaiveDate) -> Result<ObservedBars, MassiveError> {
        if ticker.is_empty() || !ticker.chars().all(|c| c.is_ascii_alphanumeric() || c == ':' || c == '.' || c == '-') {
            return Err(MassiveError::Unsupported { detail: format!("ticker {:?} is not a plain vendor ticker", bounded(ticker)) });
        }
        let key = self.key()?;
        let plan = Plan { symbol: ticker.to_string(), ticker: ticker.to_string(), clock, from, to, settle: Duration::ZERO };
        let raw = self.fetch_pages(&plan, &key)?;
        let mut bars = Vec::with_capacity(raw.bars.len());
        for (date, b) in raw.bars {
            let nominal_close_at = nominal_close_at(clock, date)
                .ok_or_else(|| MassiveError::Unsupported { detail: format!("{ticker}: no nominal close can be computed for a bar dated {date}") })?;
            bars.push(ObservedBar { date, nominal_close_at, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume });
        }
        Ok(ObservedBars { bars, raw_sha256: raw.raw_sha256, request_ids: raw.request_ids, fetched_at: raw.fetched_at })
    }

    fn window(&self, as_of: NaiveDate, days: i64) -> Result<(NaiveDate, NaiveDate), MassiveError> {
        let unsupported = || MassiveError::Unsupported { detail: format!("run date {as_of} is too close to the calendar's limits to compute a window") };
        let from = as_of.checked_sub_signed(ChronoDuration::days(days)).ok_or_else(unsupported)?;
        let to = as_of.checked_sub_signed(ChronoDuration::days(1)).ok_or_else(unsupported)?;
        Ok((from, to))
    }

    fn series_of(f: &InstrumentFetch) -> Result<PriceSeries, MassiveError> {
        PriceSeries::new(f.symbol.clone(), f.dates.clone(), f.closes.clone()).map_err(|e| MassiveError::Malformed { instrument: f.symbol.clone(), detail: format!("the validated bars do not form a series: {e}") })
    }

    fn provenance_of(f: &InstrumentFetch, s: &PriceSeries, as_of: NaiveDate) -> Result<Provenance, MassiveError> {
        let fingerprint = Panel::new(vec![s.clone()]).map(|p| data_fingerprint(&p)).map_err(|e| MassiveError::Malformed { instrument: f.symbol.clone(), detail: e.to_string() })?;
        Ok(Provenance {
            instrument: f.symbol.clone(),
            vendor_ticker: f.ticker.clone(),
            source_id: SOURCE_ID,
            request_paths: f.request_paths.clone(),
            request_ids: f.request_ids.clone(),
            raw_sha256: f.raw_sha256.clone(),
            fetched_at: f.fetched_at,
            as_of,
            first_bar: s.first_date(),
            last_bar: s.last_date(),
            last_close: s.closes()[s.len() - 1],
            bar_count: s.len(),
            dropped_incomplete: f.dropped.clone(),
            fingerprint,
            next_url_scrubbed: f.scrubbed,
        })
    }

    fn remember(&self, provenance: &[Provenance]) {
        let mut log = self.provenance.lock().unwrap_or_else(|e| e.into_inner());
        for p in provenance {
            if log.len() >= PROVENANCE_KEPT {
                log.pop_front();
            }
            log.push_back(p.clone());
        }
    }

    fn finish(&self, fetched: Vec<InstrumentFetch>, as_of: NaiveDate) -> Result<FetchedSleeve, MassiveError> {
        let mut series = Vec::with_capacity(fetched.len());
        let mut provenance = Vec::with_capacity(fetched.len());
        for f in &fetched {
            let s = Self::series_of(f)?;
            provenance.push(Self::provenance_of(f, &s, as_of)?);
            series.push(s);
        }
        let panel = Panel::new(series).map_err(|e| MassiveError::Malformed { instrument: String::new(), detail: e.to_string() })?;
        self.remember(&provenance);
        Ok(FetchedSleeve { panel, provenance, gate: None })
    }

    /// The complete daily bars of ONE vendor ticker in `[from, to]` (the same fetch, validation and completeness
    /// rule a sleeve fetch uses, without any sleeve-specific window checks). For a secondary-source cross-check of one
    /// instrument, and for the live smoke test. `clock` is the timestamp convention of the ticker's asset class.
    pub fn fetch_daily_bars(&self, ticker: &str, clock: BarClock, from: NaiveDate, to: NaiveDate, as_of: NaiveDate) -> Result<DailyBars, MassiveError> {
        if ticker.is_empty() || !ticker.chars().all(|c| c.is_ascii_alphanumeric() || c == ':' || c == '.' || c == '-') {
            return Err(MassiveError::Unsupported { detail: format!("ticker {:?} is not a plain vendor ticker", bounded(ticker)) });
        }
        let key = self.key()?;
        let settle = match clock {
            BarClock::StockMidnightNewYork => self.cfg.completeness.stock_settle,
            BarClock::MidnightUtc => self.cfg.completeness.crypto_settle,
        };
        let plan = Plan { symbol: ticker.to_string(), ticker: ticker.to_string(), clock, from, to, settle };
        let f = self.fetch_instrument(&plan, as_of, &key)?;
        if f.dates.is_empty() {
            return Err(MassiveError::InsufficientHistory { symbol: f.symbol, needed: 1, have: 0 });
        }
        let s = Self::series_of(&f)?;
        let provenance = Self::provenance_of(&f, &s, as_of)?;
        self.remember(std::slice::from_ref(&provenance));
        Ok(DailyBars { dates: f.dates, closes: f.closes, provenance })
    }

    fn fetch_etf(&self, as_of: NaiveDate) -> Result<FetchedSleeve, MassiveError> {
        let (from, to) = self.window(as_of, self.cfg.etf_history_days)?;
        let key = self.key()?;
        let mut fetched = Vec::with_capacity(ETF_SYMBOLS.len());
        for sym in ETF_SYMBOLS {
            let plan = Plan { symbol: sym.to_string(), ticker: sym.to_string(), clock: BarClock::StockMidnightNewYork, from, to, settle: self.cfg.completeness.stock_settle };
            let f = self.fetch_instrument(&plan, as_of, &key)?;
            let insufficient = |have: usize| MassiveError::InsufficientHistory { symbol: f.symbol.clone(), needed: ETF_SMA_MONTH_ENDS, have };
            if f.dates.is_empty() {
                return Err(insufficient(0));
            }
            let have = completed_month_end_dates(&Self::series_of(&f)?).len();
            if have < ETF_SMA_MONTH_ENDS {
                return Err(insufficient(have));
            }
            let newest = f.dates[f.dates.len() - 1];
            if (as_of - newest).num_days() > ETF_MAX_STALE_DAYS {
                return Err(MassiveError::StaleData {
                    symbol: f.symbol.clone(),
                    newest: Some(newest),
                    as_of,
                    detail: format!("the newest complete bar is more than {ETF_MAX_STALE_DAYS} days old"),
                });
            }
            fetched.push(f);
        }

        // The five ETFs trade the same sessions: a date present for one and absent for another (from the latest first
        // bar on) is a missing bar, provable without any exchange calendar. If it is the NEWEST session the vendor has
        // simply not published it for that ETF yet: stale, not missing.
        let max_first = fetched.iter().map(|f| f.dates[0]).max();
        let newest_any = fetched.iter().map(|f| f.dates[f.dates.len() - 1]).max();
        if let (Some(max_first), Some(newest_any)) = (max_first, newest_any) {
            let union: BTreeSet<NaiveDate> = fetched.iter().flat_map(|f| f.dates.iter().copied()).filter(|d| *d >= max_first).collect();
            for d in union {
                for f in &fetched {
                    if f.dates.binary_search(&d).is_err() {
                        return Err(if d == newest_any {
                            MassiveError::StaleData { symbol: f.symbol.clone(), newest: f.dates.last().copied(), as_of, detail: format!("the newest session ({d}) is present for other ETFs but not for this one") }
                        } else {
                            MassiveError::MissingBar { symbol: f.symbol.clone(), date: d }
                        });
                    }
                }
            }
        }
        self.finish(fetched, as_of)
    }

    fn fetch_crypto(&self, quote: &str, as_of: NaiveDate) -> Result<FetchedSleeve, MassiveError> {
        if quote != "USD" {
            return Err(MassiveError::Unsupported { detail: format!("crypto quote currency {:?} is not supported (USD only)", bounded(quote)) });
        }
        let (from, to) = self.window(as_of, self.cfg.crypto_history_days)?;
        let key = self.key()?;
        let mut fetched = Vec::with_capacity(CRYPTO_SYMBOLS.len());
        for sym in CRYPTO_SYMBOLS {
            let plan = Plan { symbol: sym.to_string(), ticker: format!("X:{sym}{quote}"), clock: BarClock::MidnightUtc, from, to, settle: self.cfg.completeness.crypto_settle };
            let f = self.fetch_instrument(&plan, as_of, &key)?;
            if f.dates.len() < CRYPTO_SMA_DAYS {
                return Err(MassiveError::InsufficientHistory { symbol: f.symbol.clone(), needed: CRYPTO_SMA_DAYS, have: f.dates.len() });
            }
            let newest = f.dates[f.dates.len() - 1];
            if (as_of - newest).num_days() > CRYPTO_MAX_STALE_DAYS {
                return Err(MassiveError::StaleData {
                    symbol: f.symbol.clone(),
                    newest: Some(newest),
                    as_of,
                    detail: "yesterday's UTC bar is not (yet) available as a complete bar".to_string(),
                });
            }
            // Crypto trades every day: each of the last 100 calendar days must have its bar.
            for k in (1..=CRYPTO_SMA_DAYS as i64).rev() {
                let d = as_of.checked_sub_signed(ChronoDuration::days(k)).ok_or_else(|| MassiveError::Unsupported { detail: "date out of range".to_string() })?;
                if f.dates.binary_search(&d).is_err() {
                    return Err(MassiveError::MissingBar { symbol: f.symbol.clone(), date: d });
                }
            }
            fetched.push(f);
        }
        self.finish(fetched, as_of)
    }
}

impl SleeveFetcher for MassiveDataSource {
    fn source_id(&self) -> &'static str {
        SOURCE_ID
    }

    /// The panel depends ONLY on `(sleeve.kind, sleeve.quote (crypto), as_of)`: never on the sleeve id, venue,
    /// asset class, share or tenant, which is what lets the per-tick `EvalCache` share one fetch between accounts.
    fn fetch_sleeve(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<FetchedSleeve, SleeveError> {
        let r = match sleeve.kind {
            SleeveKind::EtfTrend => self.fetch_etf(as_of),
            SleeveKind::CryptoTrend => self.fetch_crypto(&sleeve.quote, as_of),
        };
        r.map_err(|error| SleeveError { sleeve: sleeve.kind, as_of, error })
    }
}

impl DataSource for MassiveDataSource {
    fn sleeve_data(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        self.fetch_sleeve(sleeve, as_of).map(|f| SleeveData { panel: f.panel, gate: f.gate }).map_err(DataError::from)
    }

    fn prices(&self, _symbols: &[String], _now: DateTime<Utc>) -> Result<std::collections::BTreeMap<String, PricePoint>, DataError> {
        Err(prices_unsupported())
    }
}
