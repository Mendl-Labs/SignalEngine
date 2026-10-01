//! The Alpaca wire front end: a stateful, in-process [`HttpTransport`] that speaks the SUBSET of Alpaca's v2 trading
//! REST API that `broker_adapters::alpaca` uses (account, positions, orders -- place/get/list/cancel by id and by
//! `client_order_id`, assets, clock, calendar, flatten), with its own small cash-equities exchange behind it: signed
//! net positions, one mark price per symbol, immediate fills against that mark, a real (calendar-computed) market
//! clock, and fault injection ([`Fault`], shared with the OANDA front end).
//!
//! # What this is and is not
//! It is a test double, written from the Alpaca API's PUBLIC DOCUMENTATION as already encoded (and marked
//! FROM-MEMORY-OF-DOCS / UNVERIFIED) in `broker_adapters::alpaca`'s own module docs and test fixtures
//! (`broker-adapters/tests/fixtures/alpaca/*.json`). No live Alpaca account was read to build it (this task makes no
//! network call of any kind); every wire shape here is either copied from those fixtures or built to match the field
//! names `broker_adapters::alpaca::parse` requires. A green run against this fake proves the real adapter code agrees
//! with THIS model of the API; it does not prove the model is correct. See the report of SE PR
//! `feat/pilot-alpaca-rig` for exactly which shapes remain unverified against a live paper account.
//!
//! * It is in-process (no socket, no port): nothing here can reach a network.
//! * `client_order_id` uniqueness IS enforced (a documented, measured Alpaca behaviour, unlike OANDA): a second
//!   `POST /v2/orders` naming an id already used by ANY earlier order (pending or finished) is refused with the exact
//!   422 message `broker_adapters::alpaca::parse::classify_http_failure` recognises
//!   (`"client_order_id must be unique"`), and `GET /v2/orders:by_client_order_id` is a deterministic, idempotent
//!   lookup: the SAME id always finds the SAME (one) order.
//! * The market clock and calendar are computed from a real (if hand-encoded, general-knowledge, NOT vendor-sourced)
//!   US equities calendar: weekdays minus nine fixed federal-market holidays, two early-close sessions (the day after
//!   Thanksgiving, Christmas Eve), and the US daylight-saving rule in force since 2007 (2nd Sunday of March to the day
//!   before the 1st Sunday of November). [`AlpacaHandle::set_market_open`] can still override `is_open` directly for a
//!   drill that does not want to wait for a real calendar date.
//! * Faults reuse the OANDA/Kraken front ends' machinery ([`Fault`], [`Timing`]): `BeforeApply` (request lost),
//!   `AfterApply` (applied, answer lost: the unknown-outcome case) and `Delayed`. `FaultKind::RateLimit` is answered
//!   as HTTP 429 with a `Retry-After` header (the adapter reads it); `FaultKind::ExchangeError` as HTTP 500. Match
//!   paths with the FULL path, see [`AlpacaHandle::path`].
//! * Fractional vs whole-share assets: the five pilot ETF symbols (SPY, EFA, IEF, DBC, VNQ) default to
//!   `fractionable: true`, mirroring `broker_adapters::alpaca::assets::AssetTable::builtin()` -- which is itself
//!   marked UNVERIFIED (`AssetSource::Builtin`, "written from memory"). This fake does not change that: it cannot
//!   verify Alpaca's real behaviour either. [`AssetSpec::whole_share`] is available for a test that wants to exercise
//!   the whole-share rounding path (`asset_whole_only.json`'s BRK.A-style row) against the real wire shape.

use crate::clock::{FakeClock, DEFAULT_START_NANOS};
use crate::fault::{transport_error, Fault, FaultKind, FaultQueue, Timing};
use crate::log::{self, Delivered, LogEntry, Origin, RequestRecord};
use crate::money::{add, dec, mul, neg, sub};
use broker_adapters::transport::{HttpMethod, HttpRequest, HttpResponseDetailed, HttpTransport, TransportError};
use broker_adapters::{Dec, Side};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// Not a real key or account number. Starts `PK`/`PA`: the paper prefixes `broker_adapters::alpaca` checks for.
pub const DEFAULT_KEY_ID: &str = "PKFAKEALPACARIGKEY001";
pub const DEFAULT_SECRET: &str = "FAKE-ALPACA-SECRET-NOT-A-REAL-KEY-9f3a";
pub const DEFAULT_ACCOUNT_NUMBER: &str = "PA3FAKEALPACARIG001";
pub const DEFAULT_HOST: &str = "paper-api.alpaca.markets";

// ---------------------------------------------------------------- calendar

/// A real (general-knowledge, hand-encoded) US equities market calendar: no vendor or exchange feed was consulted.
/// Self-contained (chrono only) so it can be verified independently of `rebalancer-run`'s own calendar logic
/// (`tests/run_slot.rs`, `tests/etf_pending_decision.rs`), which duplicates the same holidays and DST rule for a
/// different purpose (the driver's run slot, not the venue's clock).
pub mod calendar {
    use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc, Weekday};
    use std::collections::BTreeSet;

    fn nth_weekday(y: i32, m: u32, wd: Weekday, n: u32) -> NaiveDate {
        let mut d = NaiveDate::from_ymd_opt(y, m, 1).unwrap();
        while d.weekday() != wd {
            d += Duration::days(1);
        }
        d + Duration::days(7 * (n as i64 - 1))
    }

    fn last_weekday(y: i32, m: u32, wd: Weekday) -> NaiveDate {
        let month_end = if m == 12 {
            NaiveDate::from_ymd_opt(y, 12, 31).unwrap()
        } else {
            NaiveDate::from_ymd_opt(y, m + 1, 1).unwrap().pred_opt().unwrap()
        };
        let mut d = month_end;
        while d.weekday() != wd {
            d = d.pred_opt().unwrap();
        }
        d
    }

    /// Western Easter Sunday (anonymous Gregorian algorithm); Good Friday is two days before it.
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
        NaiveDate::from_ymd_opt(y, month as u32, day as u32).unwrap()
    }

    /// Nine fixed federal-market holidays of calendar year `y` (a holiday landing on a weekend is simply not a
    /// session -- it is not "observed" on the nearest weekday here, matching general knowledge of the NYSE calendar
    /// for these dates and the same convention `rebalancer-run/tests/etf_pending_decision.rs::Calendar` uses).
    pub fn us_holidays(y: i32) -> BTreeSet<NaiveDate> {
        let mut h = BTreeSet::new();
        let d = |m, day| NaiveDate::from_ymd_opt(y, m, day).unwrap();
        h.insert(d(1, 1)); // New Year
        h.insert(nth_weekday(y, 1, Weekday::Mon, 3)); // MLK
        h.insert(nth_weekday(y, 2, Weekday::Mon, 3)); // Presidents
        h.insert(easter(y) - Duration::days(2)); // Good Friday
        h.insert(last_weekday(y, 5, Weekday::Mon)); // Memorial
        h.insert(d(7, 4)); // Independence
        h.insert(nth_weekday(y, 9, Weekday::Mon, 1)); // Labor
        h.insert(nth_weekday(y, 11, Weekday::Thu, 4)); // Thanksgiving
        h.insert(d(12, 25)); // Christmas
        h
    }

    pub fn is_session(d: NaiveDate) -> bool {
        !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) && !us_holidays(d.year()).contains(&d)
    }

    /// The day after Thanksgiving and Christmas Eve (when a weekday) are 13:00 ET early closes.
    pub fn is_early_close(d: NaiveDate) -> bool {
        if !is_session(d) {
            return false;
        }
        let day_after_thanksgiving = nth_weekday(d.year(), 11, Weekday::Thu, 4) + Duration::days(1);
        d == day_after_thanksgiving || d == NaiveDate::from_ymd_opt(d.year(), 12, 24).unwrap()
    }

    pub fn first_session_at_or_after(mut d: NaiveDate) -> NaiveDate {
        while !is_session(d) {
            d += Duration::days(1);
        }
        d
    }

    pub fn next_session_after(d: NaiveDate) -> NaiveDate {
        first_session_at_or_after(d + Duration::days(1))
    }

    pub fn last_session_at_or_before(mut d: NaiveDate) -> NaiveDate {
        while !is_session(d) {
            d = d.pred_opt().unwrap();
        }
        d
    }

    /// Is US Eastern daylight time in force on calendar date `d`? Rule since 2007: 2nd Sunday of March through the
    /// day before the 1st Sunday of November. Decided by DATE (not instant): every session boundary here (9:30,
    /// 13:00, 16:00 local) is hours after the 2am local transition, so the date-level rule is exact for this use
    /// (mirrors `rebalancer-run/tests/run_slot.rs::edt_in_force`, built independently for a different question).
    pub fn edt_in_force(d: NaiveDate) -> bool {
        let start = nth_weekday(d.year(), 3, Weekday::Sun, 2);
        let end = nth_weekday(d.year(), 11, Weekday::Sun, 1);
        d >= start && d < end
    }

    /// `(open, close)` of the regular session on `d`, as UTC instants. `d` must be a session (see [`is_session`]).
    pub fn session_bounds_utc(d: NaiveDate) -> (DateTime<Utc>, DateTime<Utc>) {
        let off = if edt_in_force(d) { 4 } else { 5 };
        let open_local = d.and_hms_opt(9, 30, 0).unwrap();
        let close_hour = if is_early_close(d) { 13 } else { 16 };
        let close_local = d.and_hms_opt(close_hour, 0, 0).unwrap();
        (Utc.from_utc_datetime(&(open_local + Duration::hours(off))), Utc.from_utc_datetime(&(close_local + Duration::hours(off))))
    }

    /// `(is_open, next_open, next_close)` at instant `now`: the shape `GET /v2/clock` reports.
    pub fn clock_at(now: DateTime<Utc>) -> (bool, DateTime<Utc>, DateTime<Utc>) {
        let today = now.date_naive();
        if is_session(today) {
            let (open, close) = session_bounds_utc(today);
            if now >= open && now < close {
                let (next_open, _) = session_bounds_utc(next_session_after(today));
                return (true, next_open, close);
            }
            if now < open {
                return (false, open, close);
            }
        }
        let d = next_session_after(today);
        let (open, close) = session_bounds_utc(d);
        (false, open, close)
    }

    /// T+2 sessions after `d` (a simplification of real settlement, sufficient for the `GET /v2/calendar` shape).
    pub fn settlement_date(d: NaiveDate) -> NaiveDate {
        next_session_after(next_session_after(d))
    }
}

fn rfc3339(nanos: u64) -> String {
    let secs = (nanos / 1_000_000_000) as i64;
    let subsec = (nanos % 1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, subsec).expect("fake-broker: nanos in range").to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

/// Format a clock-boundary instant the way Alpaca's real `GET /v2/clock` does: local US Eastern time with an
/// explicit numeric UTC offset (`-04:00` / `-05:00`), matching `clock_open.json` / `clock_closed.json`'s fixture
/// shape -- never the `Z`-suffixed UTC form the order timestamps use. The offset is decided by the instant's own
/// (UTC) calendar date, which is exact for every session-boundary instant this module ever formats (they are all
/// daytime UTC, so the UTC date equals the US Eastern date); see the module docs for the one narrow case (the raw
/// `timestamp` echo field at an unusual hour right at a DST changeover) where that is an approximation.
fn rfc3339_local(instant: DateTime<Utc>) -> String {
    let off_hours = if calendar::edt_in_force(instant.date_naive()) { 4 } else { 5 };
    let offset = chrono::FixedOffset::west_opt(off_hours * 3600).expect("4 or 5 hours is a valid offset");
    instant.with_timezone(&offset).to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
}

// ---------------------------------------------------------------- model

#[derive(Debug, Clone)]
pub struct AssetSpec {
    pub symbol: String,
    pub tradable: bool,
    pub fractionable: bool,
    pub status: String,
    pub min_order_size: Dec,
    pub min_trade_increment: Option<Dec>,
    pub price_increment: Option<Dec>,
}

impl AssetSpec {
    /// Tradable, fractionable, no explicit minimum -- the same (UNVERIFIED) shape
    /// `broker_adapters::alpaca::assets::AssetTable::builtin()` assumes for the five pilot ETFs.
    pub fn etf(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            tradable: true,
            fractionable: true,
            status: "active".to_string(),
            min_order_size: Dec::ZERO,
            min_trade_increment: None,
            price_increment: None,
        }
    }

    /// Whole-share only (the `asset_whole_only.json` fixture shape), for a drill on the rounding path.
    pub fn whole_share(symbol: &str) -> Self {
        Self { fractionable: false, ..Self::etf(symbol) }
    }

    pub fn inactive(symbol: &str) -> Self {
        Self { status: "inactive".to_string(), ..Self::etf(symbol) }
    }

    pub fn not_tradable(symbol: &str) -> Self {
        Self { tradable: false, ..Self::etf(symbol) }
    }
}

#[derive(Debug, Clone)]
struct Position {
    /// Signed: positive = long, negative = short.
    qty: Dec,
    avg: Dec,
}

/// An order as the fake stores it, in Alpaca's own status vocabulary (`broker_adapters::alpaca::parse::derive_status`
/// is what turns this back into `OrderStatus` on the adapter side).
#[derive(Debug, Clone)]
pub struct FakeOrder {
    pub id: String,
    pub client_order_id: Option<String>,
    pub symbol: String,
    pub side: Side,
    pub qty: Dec,
    pub limit_price: Option<Dec>,
    pub time_in_force: String,
    pub extended_hours: bool,
    /// Raw Alpaca status: `pending_new`, `accepted`, `new`, `partially_filled`, `filled`, `canceled`, `expired`,
    /// `done_for_day`, `rejected`.
    pub status: String,
    pub filled_qty: Dec,
    pub filled_avg_price: Option<Dec>,
    pub created_nanos: u64,
    pub filled_nanos: Option<u64>,
    pub canceled_nanos: Option<u64>,
    pub expired_nanos: Option<u64>,
    pub failed_nanos: Option<u64>,
    pub reject_reason: Option<String>,
}

impl FakeOrder {
    fn is_open_status(&self) -> bool {
        matches!(self.status.as_str(), "pending_new" | "accepted" | "accepted_for_bidding" | "new" | "partially_filled" | "pending_cancel" | "pending_replace")
    }
}

pub(crate) struct AlpacaWorld {
    host: String,
    key_id: String,
    secret: String,
    account_number: String,
    account_status: String,
    trading_blocked: bool,
    account_blocked: bool,
    pattern_day_trader: bool,
    cash: Dec,
    initial_cash: Dec,
    buying_power_multiplier: Dec,
    positions: BTreeMap<String, Position>,
    assets: BTreeMap<String, AssetSpec>,
    prices: BTreeMap<String, Dec>,
    /// Units available to fill at the current price (`None` = unlimited). A market/limit order for more than this
    /// partially fills and rests; raising the cap or moving the price re-attempts the rest (mirrors OANDA's
    /// `set_liquidity` / `match_resting`).
    liquidity: BTreeMap<String, Dec>,
    orders: Vec<FakeOrder>,
    next_id: u64,
    /// `Some(x)` forces `is_open` to `x` regardless of the calendar; `next_open`/`next_close` are still the
    /// calendar's own values around `now` (see the module docs).
    market_override: Option<bool>,
    faults: FaultQueue,
    log: Vec<LogEntry>,
    next_seq: u64,
    delayed: Vec<HttpRequest>,
}

struct Shared {
    world: Mutex<AlpacaWorld>,
    clock: Arc<FakeClock>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, AlpacaWorld> {
        self.world.lock().unwrap_or_else(|e| e.into_inner())
    }
}

// ---------------------------------------------------------------- builder

pub struct FakeAlpacaBuilder {
    start_nanos: u64,
    host: String,
    key_id: String,
    secret: String,
    account_number: String,
    cash: Dec,
    buying_power_multiplier: Dec,
    assets: Vec<AssetSpec>,
    prices: Vec<(String, Dec)>,
    first_order_seq: u64,
}

impl Default for FakeAlpacaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeAlpacaBuilder {
    pub fn new() -> Self {
        Self {
            start_nanos: DEFAULT_START_NANOS,
            host: DEFAULT_HOST.to_string(),
            key_id: DEFAULT_KEY_ID.to_string(),
            secret: DEFAULT_SECRET.to_string(),
            account_number: DEFAULT_ACCOUNT_NUMBER.to_string(),
            cash: Dec::ZERO,
            buying_power_multiplier: dec("1"),
            assets: Vec::new(),
            prices: Vec::new(),
            first_order_seq: 9001,
        }
    }

    /// The five pilot ETFs (SPY, EFA, IEF, DBC, VNQ), fractionable, at the given cash balance and a flat $100 mark
    /// on every symbol (override with [`FakeAlpacaBuilder::price`]).
    pub fn standard(cash: &str) -> Self {
        let mut b = Self::new().cash(cash);
        for s in ["SPY", "EFA", "IEF", "DBC", "VNQ"] {
            b = b.asset(AssetSpec::etf(s), "100");
        }
        b
    }

    pub fn cash(mut self, usd: &str) -> Self {
        self.cash = dec(usd);
        self
    }

    pub fn buying_power_multiplier(mut self, m: &str) -> Self {
        self.buying_power_multiplier = dec(m);
        self
    }

    pub fn account_number(mut self, n: &str) -> Self {
        self.account_number = n.to_string();
        self
    }

    pub fn credentials(mut self, key_id: &str, secret: &str) -> Self {
        self.key_id = key_id.to_string();
        self.secret = secret.to_string();
        self
    }

    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    pub fn start_nanos(mut self, n: u64) -> Self {
        self.start_nanos = n;
        self
    }

    pub fn asset(mut self, spec: AssetSpec, price: &str) -> Self {
        self.prices.push((spec.symbol.clone(), dec(price)));
        self.assets.push(spec);
        self
    }

    pub fn build(self) -> FakeAlpaca {
        let mut assets = BTreeMap::new();
        for a in self.assets {
            assets.insert(a.symbol.clone(), a);
        }
        let prices = self.prices.into_iter().collect();
        let world = AlpacaWorld {
            host: self.host,
            key_id: self.key_id,
            secret: self.secret,
            account_number: self.account_number,
            account_status: "ACTIVE".to_string(),
            trading_blocked: false,
            account_blocked: false,
            pattern_day_trader: false,
            cash: self.cash,
            initial_cash: self.cash,
            buying_power_multiplier: self.buying_power_multiplier,
            positions: BTreeMap::new(),
            assets,
            prices,
            liquidity: BTreeMap::new(),
            orders: Vec::new(),
            next_id: self.first_order_seq,
            market_override: None,
            faults: FaultQueue::default(),
            log: Vec::new(),
            next_seq: 1,
            delayed: Vec::new(),
        };
        FakeAlpaca { shared: Arc::new(Shared { world: Mutex::new(world), clock: Arc::new(FakeClock::new(self.start_nanos)) }) }
    }
}

// ---------------------------------------------------------------- the fake

/// The fake Alpaca exchange. Hand `transport()` to `AlpacaAdapter::new` (or build through [`crate::alpaca_rig`]),
/// keep `handle()` for scripting.
#[derive(Clone)]
pub struct FakeAlpaca {
    shared: Arc<Shared>,
}

impl FakeAlpaca {
    /// `$5,000` cash, the five pilot ETFs at $100, the pilot's own paper credentials and account number.
    pub fn standard() -> Self {
        FakeAlpacaBuilder::standard("5000").build()
    }
    pub fn builder() -> FakeAlpacaBuilder {
        FakeAlpacaBuilder::new()
    }
    pub fn transport(&self) -> Arc<AlpacaTransport> {
        Arc::new(AlpacaTransport { shared: self.shared.clone() })
    }
    pub fn handle(&self) -> AlpacaHandle {
        AlpacaHandle { shared: self.shared.clone() }
    }
    pub fn clock(&self) -> Arc<FakeClock> {
        self.shared.clock.clone()
    }
}

/// The `HttpTransport` handed to the adapter.
pub struct AlpacaTransport {
    shared: Arc<Shared>,
}

impl HttpTransport for AlpacaTransport {
    fn execute(&self, req: &HttpRequest) -> Result<broker_adapters::transport::HttpResponse, TransportError> {
        self.call(req, Origin::Adapter).map(|r| broker_adapters::transport::HttpResponse { status: r.status, body: r.body })
    }
    fn execute_detailed(&self, req: &HttpRequest) -> Result<HttpResponseDetailed, TransportError> {
        self.call(req, Origin::Adapter)
    }
}

struct Resp {
    status: u16,
    body: String,
    headers: Vec<(String, String)>,
}

impl Resp {
    fn plain(status: u16, body: String) -> Self {
        Self { status, body, headers: Vec::new() }
    }
}

impl AlpacaTransport {
    fn call(&self, req: &HttpRequest, origin: Origin) -> Result<HttpResponseDetailed, TransportError> {
        use broker_adapters::nonce::Clock;
        let now_nanos = self.shared.clock.now_nanos();
        let mut w = self.shared.lock();
        let seq = w.next_seq;
        w.next_seq += 1;
        let mut rec = RequestRecord {
            seq,
            time_nanos: now_nanos,
            origin,
            method: req.method,
            path: String::new(),
            api_key: None,
            params: Vec::new(),
            fault: None,
            reached_exchange: false,
            produced: None,
            delivered: Delivered::Response { status: 0, body: String::new() },
        };
        let Some((host, path, query)) = crate::kraken::wire::split_url(&req.url) else {
            let e = TransportError::ConnectFailed(format!("invalid url {:?}", req.url));
            rec.delivered = Delivered::Transport(e.clone());
            w.log.push(LogEntry::Request(rec));
            return Err(e);
        };
        rec.path = path.to_string();
        rec.params = crate::kraken::wire::parse_form(query).unwrap_or_default();
        rec.api_key = req.header("APCA-API-KEY-ID").map(str::to_string);
        if !host.eq_ignore_ascii_case(&w.host) {
            let e = TransportError::ConnectFailed(format!("could not resolve host {host} (this fake serves {})", w.host));
            rec.delivered = Delivered::Transport(e.clone());
            w.log.push(LogEntry::Request(rec));
            return Err(e);
        }
        let fault = if origin == Origin::Adapter { w.faults.take(req.method, path) } else { None };
        rec.fault = fault.clone();
        if let Some((kind, timing @ (Timing::BeforeApply | Timing::Delayed))) = &fault {
            if *timing == Timing::Delayed {
                w.delayed.push(req.clone());
            }
            let out = fault_response(kind);
            rec.delivered = delivered_of(&out);
            w.log.push(LogEntry::Request(rec));
            return out;
        }
        let now = DateTime::<Utc>::from_timestamp((now_nanos / 1_000_000_000) as i64, (now_nanos % 1_000_000_000) as u32).expect("nanos in range");
        let resp = dispatch(&mut w, now_nanos, now, req, path, query);
        rec.reached_exchange = true;
        rec.produced = Some((resp.status, resp.body.clone()));
        let out = match &fault {
            Some((kind, Timing::AfterApply)) => fault_response(kind),
            Some((_, Timing::BeforeApply | Timing::Delayed)) => unreachable!("handled above"),
            None => Ok(HttpResponseDetailed { status: resp.status, body: resp.body, headers: resp.headers }),
        };
        rec.delivered = delivered_of(&out);
        w.log.push(LogEntry::Request(rec));
        out
    }

    fn deliver_delayed(&self) -> Vec<Result<HttpResponseDetailed, TransportError>> {
        let held = std::mem::take(&mut self.shared.lock().delayed);
        held.iter().map(|r| self.call(r, Origin::Delayed)).collect()
    }
}

fn delivered_of(out: &Result<HttpResponseDetailed, TransportError>) -> Delivered {
    match out {
        Ok(r) => Delivered::Response { status: r.status, body: r.body.clone() },
        Err(e) => Delivered::Transport(e.clone()),
    }
}

fn fault_response(kind: &FaultKind) -> Result<HttpResponseDetailed, TransportError> {
    if let Some(e) = transport_error(kind) {
        return Err(e);
    }
    Ok(match kind {
        FaultKind::Http { status, body } => HttpResponseDetailed { status: *status, body: body.clone(), headers: Vec::new() },
        FaultKind::MalformedBody(body) => HttpResponseDetailed { status: 200, body: body.clone(), headers: Vec::new() },
        FaultKind::RateLimit => HttpResponseDetailed {
            status: 429,
            body: json!({"code": 42910000, "message": "rate limit exceeded"}).to_string(),
            headers: vec![("Retry-After".to_string(), "1".to_string())],
        },
        FaultKind::ExchangeError(code) => HttpResponseDetailed { status: 500, body: json!({"message": code}).to_string(), headers: Vec::new() },
        FaultKind::Timeout | FaultKind::ConnectFailed | FaultKind::IoError => unreachable!("handled by transport_error"),
    })
}

// ---------------------------------------------------------------- helpers

fn abs(d: Dec) -> Dec {
    if d.is_negative() {
        neg(d)
    } else {
        d
    }
}

fn sign(d: Dec) -> i32 {
    if d.is_negative() {
        -1
    } else if d.is_positive() {
        1
    } else {
        0
    }
}

fn err_body(msg: &str, code: u64) -> String {
    json!({"code": code, "message": msg}).to_string()
}

fn new_order_id(n: u64) -> String {
    format!("fake-alpaca-order-{n:012}")
}

// ---------------------------------------------------------------- exchange logic

impl AlpacaWorld {
    fn alloc(&mut self) -> String {
        let id = new_order_id(self.next_id);
        self.next_id += 1;
        id
    }

    fn spec(&self, symbol: &str) -> Option<&AssetSpec> {
        self.assets.get(symbol)
    }

    fn price_of(&self, symbol: &str) -> Option<Dec> {
        self.prices.get(symbol).copied()
    }

    fn position_qty(&self, symbol: &str) -> Dec {
        self.positions.get(symbol).map(|p| p.qty).unwrap_or(Dec::ZERO)
    }

    fn market_value(&self) -> Dec {
        self.positions.iter().fold(Dec::ZERO, |acc, (sym, p)| {
            let px = self.price_of(sym).unwrap_or(Dec::ZERO);
            add(acc, mul(p.qty, px))
        })
    }

    fn long_market_value(&self) -> Dec {
        self.positions.iter().filter(|(_, p)| p.qty.is_positive()).fold(Dec::ZERO, |acc, (sym, p)| {
            let px = self.price_of(sym).unwrap_or(Dec::ZERO);
            add(acc, mul(p.qty, px))
        })
    }

    fn short_market_value(&self) -> Dec {
        self.positions.iter().filter(|(_, p)| p.qty.is_negative()).fold(Dec::ZERO, |acc, (sym, p)| {
            let px = self.price_of(sym).unwrap_or(Dec::ZERO);
            add(acc, mul(p.qty, px))
        })
    }

    fn equity(&self) -> Dec {
        add(self.cash, self.market_value())
    }

    fn buying_power(&self) -> Dec {
        let bp = mul(self.cash, self.buying_power_multiplier);
        if bp.is_negative() {
            Dec::ZERO
        } else {
            bp
        }
    }

    fn is_open_now(&self, now: DateTime<Utc>) -> bool {
        let (calendar_open, _, _) = calendar::clock_at(now);
        self.market_override.unwrap_or(calendar_open)
    }

    /// Apply a fill: move cash and the position book. `qty` is signed (positive = bought, negative = sold).
    fn apply_fill(&mut self, symbol: &str, qty: Dec, price: Dec) {
        self.cash = sub(self.cash, mul(qty, price));
        let cur = self.positions.get(symbol).cloned();
        let new_qty = add(cur.as_ref().map(|p| p.qty).unwrap_or(Dec::ZERO), qty);
        if new_qty.is_zero() {
            self.positions.remove(symbol);
        } else {
            let avg = match &cur {
                Some(p) if sign(p.qty) == sign(qty) || p.qty.is_zero() => {
                    let cost = add(mul(abs(p.qty), p.avg), mul(abs(qty), price));
                    crate::money::div_floor(cost, abs(new_qty), 8)
                }
                Some(p) if sign(new_qty) == sign(p.qty) => p.avg, // reduced, same side: cost basis unchanged
                _ => price, // flipped sign, or opened fresh
            };
            self.positions.insert(symbol.to_string(), Position { qty: new_qty, avg });
        }
    }

    /// Try to execute order at index `idx` against the current price. Mutates its status/fill fields in place.
    fn try_execute(&mut self, now_nanos: u64, now: DateTime<Utc>, idx: usize) {
        let (symbol, side, remaining, limit, is_market) = {
            let o = &self.orders[idx];
            (o.symbol.clone(), o.side, sub(o.qty, o.filled_qty), o.limit_price, o.limit_price.is_none())
        };
        if remaining.is_zero() {
            return;
        }
        let Some(price) = self.price_of(&symbol) else { return };
        if is_market && !self.is_open_now(now) {
            // real Alpaca queues a market order placed while closed rather than rejecting it; it stays `accepted`
            // until the next session (see the module docs on why this branch is rarely reached: the adapter's own
            // preflight refuses a market order locally when its clock read says closed).
            return;
        }
        let buy = side == Side::Buy;
        if let Some(lp) = limit {
            let marketable = if buy { price <= lp } else { price >= lp };
            if !marketable {
                return;
            }
        }
        let fill_price = match limit {
            Some(lp) if buy => price.min(lp),
            Some(lp) => price.max(lp),
            None => price,
        };
        let avail = self.liquidity.get(&symbol).copied();
        let exec_abs = match avail {
            Some(l) if l < remaining => l,
            _ => remaining,
        };
        if exec_abs.is_zero() {
            return;
        }
        let signed = if buy { exec_abs } else { neg(exec_abs) };
        // A sell beyond held quantity would open a short; the pilot mandate forbids shorting, so the fake refuses it
        // the same way Alpaca's real "insufficient qty" 422/403 does, rather than silently going short.
        if !buy {
            let held = self.position_qty(&symbol);
            if abs(signed) > held.max(Dec::ZERO) {
                let o = &mut self.orders[idx];
                o.status = "rejected".to_string();
                o.reject_reason = Some("insufficient qty available for order".to_string());
                o.failed_nanos = Some(now_nanos);
                return;
            }
        }
        self.apply_fill(&symbol, signed, fill_price);
        let o = &mut self.orders[idx];
        let new_filled = add(o.filled_qty, exec_abs);
        let total_price = add(mul(o.filled_qty, o.filled_avg_price.unwrap_or(Dec::ZERO)), mul(exec_abs, fill_price));
        o.filled_avg_price = Some(crate::money::div_floor(total_price, new_filled, 6));
        o.filled_qty = new_filled;
        if new_filled >= o.qty {
            o.status = "filled".to_string();
            o.filled_nanos = Some(now_nanos);
        } else {
            o.status = "partially_filled".to_string();
        }
    }

    /// Re-attempt every resting (open, not fully filled) order (called after a price or liquidity change).
    fn match_resting(&mut self, now_nanos: u64, now: DateTime<Utc>) {
        let pending: Vec<usize> = self.orders.iter().enumerate().filter(|(_, o)| o.is_open_status()).map(|(i, _)| i).collect();
        for i in pending {
            self.try_execute(now_nanos, now, i);
        }
    }

    fn account_json(&self) -> Value {
        json!({
            "id": "00000000-0000-4000-8000-000000000000",
            "account_number": self.account_number,
            "status": self.account_status,
            "currency": "USD",
            "cash": self.cash.normalized().to_string(),
            "portfolio_value": self.equity().normalized().to_string(),
            "equity": self.equity().normalized().to_string(),
            "last_equity": self.equity().normalized().to_string(),
            "buying_power": self.buying_power().normalized().to_string(),
            "long_market_value": self.long_market_value().normalized().to_string(),
            "short_market_value": self.short_market_value().normalized().to_string(),
            "trading_blocked": self.trading_blocked,
            "transfers_blocked": false,
            "account_blocked": self.account_blocked,
            "pattern_day_trader": self.pattern_day_trader,
            "shorting_enabled": false,
            "multiplier": self.buying_power_multiplier.normalized().to_string(),
            "daytrade_count": 0
        })
    }

    fn position_json(&self, symbol: &str, p: &Position) -> Value {
        let px = self.price_of(symbol).unwrap_or(Dec::ZERO);
        let side = if p.qty.is_negative() { "short" } else { "long" };
        json!({
            "asset_id": "00000000-0000-4000-8000-000000000001",
            "symbol": symbol,
            "exchange": "ARCA",
            "asset_class": "us_equity",
            "avg_entry_price": p.avg.normalized().to_string(),
            "qty": abs(p.qty).normalized().to_string(),
            "qty_available": abs(p.qty).normalized().to_string(),
            "side": side,
            "market_value": mul(p.qty, px).normalized().to_string(),
            "cost_basis": mul(abs(p.qty), p.avg).normalized().to_string(),
            "unrealized_pl": mul(p.qty, sub(px, p.avg)).normalized().to_string(),
            "current_price": px.normalized().to_string()
        })
    }

    fn asset_json(&self, a: &AssetSpec) -> Value {
        json!({
            "id": format!("00000000-0000-4000-8000-{:012}", a.symbol.len()),
            "class": "us_equity",
            "exchange": "ARCA",
            "symbol": a.symbol,
            "name": format!("{} fake asset", a.symbol),
            "status": a.status,
            "tradable": a.tradable,
            "marginable": true,
            "shortable": false,
            "easy_to_borrow": false,
            "fractionable": a.fractionable,
            "min_order_size": a.min_order_size.normalized().to_string(),
            "min_trade_increment": a.min_trade_increment.map(|d| d.normalized().to_string()),
            "price_increment": a.price_increment.map(|d| d.normalized().to_string())
        })
    }

    fn order_json(&self, o: &FakeOrder) -> Value {
        json!({
            "id": o.id,
            "client_order_id": o.client_order_id,
            "created_at": rfc3339(o.created_nanos),
            "updated_at": rfc3339(o.filled_nanos.or(o.canceled_nanos).or(o.expired_nanos).or(o.failed_nanos).unwrap_or(o.created_nanos)),
            "submitted_at": rfc3339(o.created_nanos),
            "filled_at": o.filled_nanos.map(rfc3339),
            "expired_at": o.expired_nanos.map(rfc3339),
            "canceled_at": o.canceled_nanos.map(rfc3339),
            "failed_at": o.failed_nanos.map(rfc3339),
            "replaced_at": Value::Null,
            "asset_id": "00000000-0000-4000-8000-000000000001",
            "symbol": o.symbol,
            "asset_class": "us_equity",
            "notional": Value::Null,
            "qty": o.qty.normalized().to_string(),
            "filled_qty": o.filled_qty.normalized().to_string(),
            "filled_avg_price": o.filled_avg_price.map(|p| p.normalized().to_string()),
            "order_class": "",
            "order_type": if o.limit_price.is_some() { "limit" } else { "market" },
            "type": if o.limit_price.is_some() { "limit" } else { "market" },
            "side": o.side.as_str(),
            "time_in_force": o.time_in_force,
            "limit_price": o.limit_price.map(|p| p.normalized().to_string()),
            "stop_price": Value::Null,
            "status": o.status,
            "extended_hours": o.extended_hours,
            "legs": Value::Null
        })
    }
}

// ---------------------------------------------------------------- request dispatch

enum Verb {
    Get,
    Post,
    Delete,
}

fn dispatch(w: &mut AlpacaWorld, now_nanos: u64, now: DateTime<Utc>, req: &HttpRequest, path: &str, query: &str) -> Resp {
    let verb = match req.method {
        HttpMethod::Get => Verb::Get,
        HttpMethod::Post => Verb::Post,
        HttpMethod::Delete => Verb::Delete,
        HttpMethod::Put => return Resp::plain(404, err_body("not found", 40410000)),
    };
    let Some(key) = req.header("APCA-API-KEY-ID") else { return Resp::plain(401, err_body("request is not authorized", 40110000)) };
    let secret = req.header("APCA-API-SECRET-KEY");
    if key != w.key_id || secret != Some(w.secret.as_str()) {
        return Resp::plain(401, err_body("request is not authorized", 40110000));
    }
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    match (verb, segs.as_slice()) {
        (Verb::Get, ["v2", "account"]) => Resp::plain(200, w.account_json().to_string()),
        (Verb::Get, ["v2", "positions"]) => {
            let rows: Vec<Value> = w.positions.iter().map(|(s, p)| w.position_json(s, p)).collect();
            Resp::plain(200, Value::Array(rows).to_string())
        }
        (Verb::Delete, ["v2", "positions"]) => flatten_all(w, now_nanos, now, query),
        (Verb::Delete, ["v2", "positions", symbol]) => close_position(w, now_nanos, now, symbol),
        (Verb::Get, ["v2", "clock"]) => Resp::plain(200, clock_json(w, now).to_string()),
        (Verb::Get, ["v2", "calendar"]) => calendar_response(w, query),
        (Verb::Get, ["v2", "assets", symbol]) => match w.spec(symbol) {
            Some(a) => Resp::plain(200, w.asset_json(a).to_string()),
            None => Resp::plain(404, err_body(&format!("asset not found for {symbol}"), 40410000)),
        },
        (Verb::Post, ["v2", "orders"]) => place_order(w, now_nanos, now, req.body.as_deref().unwrap_or("")),
        (Verb::Get, ["v2", "orders:by_client_order_id"]) => order_by_client_id(w, query),
        (Verb::Get, ["v2", "orders"]) => list_orders(w, query),
        (Verb::Get, ["v2", "orders", id]) => match w.orders.iter().find(|o| o.id == *id) {
            Some(o) => Resp::plain(200, w.order_json(o).to_string()),
            None => Resp::plain(404, err_body("order not found", 40410000)),
        },
        (Verb::Delete, ["v2", "orders", id]) => cancel_order(w, now_nanos, now, id),
        _ => Resp::plain(404, err_body("not found", 40410000)),
    }
}

fn clock_json(w: &AlpacaWorld, now: DateTime<Utc>) -> Value {
    let (calendar_open, next_open, next_close) = calendar::clock_at(now);
    let is_open = w.market_override.unwrap_or(calendar_open);
    json!({"timestamp": rfc3339_local(now), "is_open": is_open, "next_open": rfc3339_local(next_open), "next_close": rfc3339_local(next_close)})
}

fn calendar_response(_w: &AlpacaWorld, query: &str) -> Resp {
    let Some(params) = crate::kraken::wire::parse_form(query) else { return Resp::plain(400, err_body("bad query", 40010000)) };
    let get = |k: &str| params.iter().find(|(pk, _)| pk == k).map(|(_, v)| v.clone());
    let (Some(start), Some(end)) = (get("start"), get("end")) else { return Resp::plain(400, err_body("start and end are required", 40010000)) };
    let Ok(start) = chrono::NaiveDate::parse_from_str(&start, "%Y-%m-%d") else { return Resp::plain(400, err_body("bad start date", 40010000)) };
    let Ok(end) = chrono::NaiveDate::parse_from_str(&end, "%Y-%m-%d") else { return Resp::plain(400, err_body("bad end date", 40010000)) };
    if start > end {
        return Resp::plain(400, err_body("start is after end", 40010000));
    }
    let mut rows = Vec::new();
    let mut d = start;
    while d <= end {
        if calendar::is_session(d) {
            let early = calendar::is_early_close(d);
            let close = if early { "13:00" } else { "16:00" };
            let session_close = if early { "1700" } else { "2000" };
            rows.push(json!({
                "date": d.format("%Y-%m-%d").to_string(),
                "open": "09:30",
                "close": close,
                "session_open": "0400",
                "session_close": session_close,
                "settlement_date": calendar::settlement_date(d).format("%Y-%m-%d").to_string()
            }));
        }
        d += chrono::Duration::days(1);
    }
    Resp::plain(200, Value::Array(rows).to_string())
}

// ---------------------------------------------------------------- orders

struct OrderSpec {
    symbol: String,
    side: Side,
    qty: Dec,
    limit_price: Option<Dec>,
    time_in_force: String,
    client_order_id: Option<String>,
    extended_hours: bool,
}

fn parse_order_body(w: &AlpacaWorld, body: &str) -> Result<OrderSpec, (u16, String)> {
    let Ok(v) = serde_json::from_str::<Value>(body) else { return Err((400, err_body("invalid JSON", 40010000))) };
    let symbol = v.get("symbol").and_then(Value::as_str).map(|s| s.to_ascii_uppercase()).ok_or_else(|| (422, err_body("symbol is required", 42210000)))?;
    if w.spec(&symbol).is_none() {
        return Err((422, err_body(&format!("asset \"{symbol}\" is not found"), 42210000)));
    }
    let side = match v.get("side").and_then(Value::as_str) {
        Some("buy") => Side::Buy,
        Some("sell") => Side::Sell,
        _ => return Err((422, err_body("side must be buy or sell", 42210000))),
    };
    let kind = v.get("type").and_then(Value::as_str).unwrap_or("");
    let qty_text = v.get("qty").and_then(Value::as_str).ok_or_else(|| (422, err_body("qty is required (notional orders are not supported by this fake)", 42210000)))?;
    let Ok(qty) = Dec::parse(qty_text) else { return Err((422, err_body("qty is not a number", 42210000))) };
    if !qty.is_positive() {
        return Err((422, err_body("qty must be positive", 42210000)));
    }
    let asset = w.spec(&symbol).expect("checked above");
    if !asset.fractionable && qty.normalized() != qty.round_dp(0, broker_adapters::decimal::Rounding::Floor).unwrap_or(qty) {
        return Err((422, err_body(&format!("asset \"{symbol}\" is not fractionable"), 42210000)));
    }
    let time_in_force = v.get("time_in_force").and_then(Value::as_str).unwrap_or("day").to_string();
    let limit_price = if kind == "limit" {
        let Some(p) = v.get("limit_price").and_then(Value::as_str).and_then(|s| Dec::parse(s).ok()) else {
            return Err((422, err_body("limit_price is required for a limit order", 42210000)));
        };
        Some(p)
    } else {
        None
    };
    let client_order_id = v.get("client_order_id").and_then(Value::as_str).map(str::to_string);
    let extended_hours = v.get("extended_hours").and_then(Value::as_bool).unwrap_or(false);
    Ok(OrderSpec { symbol, side, qty, limit_price, time_in_force, client_order_id, extended_hours })
}

fn place_order(w: &mut AlpacaWorld, now_nanos: u64, now: DateTime<Utc>, body: &str) -> Resp {
    let spec = match parse_order_body(w, body) {
        Ok(s) => s,
        Err((status, body)) => return Resp::plain(status, body),
    };
    // Alpaca enforces client_order_id uniqueness across EVERY order ever placed, pending or finished (MEASURED
    // behaviour per `broker_adapters::alpaca::parse::classify_http_failure`'s duplicate-id message match).
    if let Some(cid) = &spec.client_order_id {
        if w.orders.iter().any(|o| o.client_order_id.as_deref() == Some(cid.as_str())) {
            return Resp::plain(422, err_body("client_order_id must be unique", 42210000));
        }
    }
    if w.account_blocked || w.trading_blocked {
        return Resp::plain(403, err_body("account is not permitted to trade", 40310000));
    }
    // A rough buying-power check on a market buy (the adapter's own preflight already checked account state; this
    // is the venue's own belt-and-braces refusal).
    if spec.side == Side::Buy {
        if let Some(px) = w.price_of(&spec.symbol) {
            let cost = mul(spec.qty, spec.limit_price.unwrap_or(px));
            if cost > w.buying_power() {
                return Resp::plain(403, err_body("insufficient buying power", 40310000));
            }
        }
    }
    let id = w.alloc();
    let order = FakeOrder {
        id: id.clone(),
        client_order_id: spec.client_order_id,
        symbol: spec.symbol.clone(),
        side: spec.side,
        qty: spec.qty,
        limit_price: spec.limit_price,
        time_in_force: spec.time_in_force,
        extended_hours: spec.extended_hours,
        status: "accepted".to_string(),
        filled_qty: Dec::ZERO,
        filled_avg_price: None,
        created_nanos: now_nanos,
        filled_nanos: None,
        canceled_nanos: None,
        expired_nanos: None,
        failed_nanos: None,
        reject_reason: None,
    };
    w.orders.push(order);
    let idx = w.orders.len() - 1;
    w.try_execute(now_nanos, now, idx);
    let o = w.orders[idx].clone();
    if o.status == "rejected" {
        return Resp::plain(403, err_body(o.reject_reason.as_deref().unwrap_or("rejected"), 40310000));
    }
    Resp::plain(200, w.order_json(&o).to_string())
}

fn order_by_client_id(w: &AlpacaWorld, query: &str) -> Resp {
    let Some(params) = crate::kraken::wire::parse_form(query) else { return Resp::plain(400, err_body("bad query", 40010000)) };
    let Some(cid) = params.iter().find(|(k, _)| k == "client_order_id").map(|(_, v)| v.clone()) else {
        return Resp::plain(400, err_body("client_order_id is required", 40010000));
    };
    match w.orders.iter().find(|o| o.client_order_id.as_deref() == Some(cid.as_str())) {
        Some(o) => Resp::plain(200, w.order_json(o).to_string()),
        None => Resp::plain(404, err_body("order not found", 40410000)),
    }
}

fn list_orders(w: &AlpacaWorld, query: &str) -> Resp {
    let params = crate::kraken::wire::parse_form(query).unwrap_or_default();
    let get = |k: &str| params.iter().find(|(pk, _)| pk == k).map(|(_, v)| v.clone());
    let status = get("status").unwrap_or_else(|| "open".to_string());
    let limit: usize = get("limit").and_then(|s| s.parse().ok()).unwrap_or(50);
    let symbols: Vec<String> = get("symbols").map(|s| s.split(',').map(|x| x.to_ascii_uppercase()).collect()).unwrap_or_default();
    let mut rows: Vec<&FakeOrder> = w
        .orders
        .iter()
        .filter(|o| match status.as_str() {
            "open" => o.is_open_status(),
            "closed" => !o.is_open_status(),
            _ => true,
        })
        .filter(|o| symbols.is_empty() || symbols.contains(&o.symbol))
        .collect();
    rows.sort_by(|a, b| b.created_nanos.cmp(&a.created_nanos));
    rows.truncate(limit);
    let arr: Vec<Value> = rows.iter().map(|o| w.order_json(o)).collect();
    Resp::plain(200, Value::Array(arr).to_string())
}

fn cancel_order(w: &mut AlpacaWorld, now_nanos: u64, now: DateTime<Utc>, id: &str) -> Resp {
    let Some(idx) = w.orders.iter().position(|o| o.id == id) else { return Resp::plain(404, err_body("order not found", 40410000)) };
    // Settle any fill that could still happen right now before deciding whether there is anything left to cancel
    // (mirrors a real exchange racing a cancel against the matching engine).
    w.try_execute(now_nanos, now, idx);
    let o = &mut w.orders[idx];
    if !o.is_open_status() {
        return Resp::plain(422, err_body("order is not cancelable", 42210000));
    }
    o.status = "canceled".to_string();
    o.canceled_nanos = Some(now_nanos);
    Resp { status: 204, body: String::new(), headers: Vec::new() }
}

fn close_position(w: &mut AlpacaWorld, now_nanos: u64, now: DateTime<Utc>, symbol: &str) -> Resp {
    let symbol = symbol.to_ascii_uppercase();
    let Some(pos) = w.positions.get(&symbol).cloned() else { return Resp::plain(404, err_body("position not found", 40410000)) };
    let side = if pos.qty.is_positive() { Side::Sell } else { Side::Buy };
    let id = w.alloc();
    let order = FakeOrder {
        id: id.clone(),
        client_order_id: None,
        symbol: symbol.clone(),
        side,
        qty: abs(pos.qty),
        limit_price: None,
        time_in_force: "day".to_string(),
        extended_hours: false,
        status: "accepted".to_string(),
        filled_qty: Dec::ZERO,
        filled_avg_price: None,
        created_nanos: now_nanos,
        filled_nanos: None,
        canceled_nanos: None,
        expired_nanos: None,
        failed_nanos: None,
        reject_reason: None,
    };
    w.orders.push(order);
    let idx = w.orders.len() - 1;
    w.try_execute(now_nanos, now, idx);
    let o = w.orders[idx].clone();
    Resp::plain(200, w.order_json(&o).to_string())
}

fn flatten_all(w: &mut AlpacaWorld, now_nanos: u64, now: DateTime<Utc>, query: &str) -> Resp {
    let params = crate::kraken::wire::parse_form(query).unwrap_or_default();
    let cancel_orders = params.iter().any(|(k, v)| k == "cancel_orders" && v == "true");
    if cancel_orders {
        let open: Vec<usize> = w.orders.iter().enumerate().filter(|(_, o)| o.is_open_status()).map(|(i, _)| i).collect();
        for i in open {
            let o = &mut w.orders[i];
            o.status = "canceled".to_string();
            o.canceled_nanos = Some(now_nanos);
        }
    }
    let symbols: Vec<String> = w.positions.keys().cloned().collect();
    let mut entries = Vec::new();
    for symbol in symbols {
        let resp = close_position(w, now_nanos, now, &symbol);
        let body: Value = serde_json::from_str(&resp.body).unwrap_or(Value::Null);
        entries.push(json!({"symbol": symbol, "status": resp.status, "body": body}));
    }
    Resp::plain(207, Value::Array(entries).to_string())
}

// ---------------------------------------------------------------- control API

/// Everything a test can do to the fake exchange besides talking to it over the wire.
#[derive(Clone)]
pub struct AlpacaHandle {
    shared: Arc<Shared>,
}

impl AlpacaHandle {
    fn now(&self) -> u64 {
        use broker_adapters::nonce::Clock;
        self.shared.clock.now_nanos()
    }

    fn now_utc(&self) -> DateTime<Utc> {
        let n = self.now();
        DateTime::<Utc>::from_timestamp((n / 1_000_000_000) as i64, (n % 1_000_000_000) as u32).expect("nanos in range")
    }

    fn read<R>(&self, f: impl FnOnce(&AlpacaWorld) -> R) -> R {
        f(&self.shared.lock())
    }

    fn control<R>(&self, note: String, f: impl FnOnce(&mut AlpacaWorld, u64, DateTime<Utc>) -> R) -> R {
        let now = self.now();
        let now_utc = self.now_utc();
        let mut w = self.shared.lock();
        let seq = w.next_seq;
        w.next_seq += 1;
        w.log.push(LogEntry::Control(log::ControlRecord { seq, time_nanos: now, note }));
        f(&mut w, now, now_utc)
    }

    pub fn clock(&self) -> Arc<FakeClock> {
        self.shared.clock.clone()
    }

    pub fn key_id(&self) -> String {
        self.read(|w| w.key_id.clone())
    }

    pub fn secret(&self) -> String {
        self.read(|w| w.secret.clone())
    }

    pub fn account_number(&self) -> String {
        self.read(|w| w.account_number.clone())
    }

    /// Every symbol the fake currently has an asset row for (what a rig refreshes over the wire at startup).
    pub fn asset_symbols(&self) -> Vec<String> {
        self.read(|w| w.assets.keys().cloned().collect())
    }

    /// The full path of an endpoint, for fault matchers: `path("/v2/orders")`.
    pub fn path(&self, tail: &str) -> String {
        tail.to_string()
    }

    // ---- market

    pub fn set_price(&self, symbol: &str, price: &str) {
        let p = dec(price);
        self.control(format!("set_price {symbol} {p}"), |w, now, now_utc| {
            assert!(w.spec(symbol).is_some(), "fake-broker: unknown symbol {symbol}");
            w.prices.insert(symbol.to_string(), p);
            w.match_resting(now, now_utc);
        });
    }

    pub fn price(&self, symbol: &str) -> Dec {
        self.read(|w| w.price_of(symbol).unwrap_or(Dec::ZERO))
    }

    /// Cap the units available to fill at the current price (`None` = unlimited). An order for more partially fills
    /// and rests; raising the cap re-attempts it.
    pub fn set_liquidity(&self, symbol: &str, units: Option<&str>) {
        self.control(format!("set_liquidity {symbol} {units:?}"), |w, now, now_utc| {
            match units {
                Some(u) => {
                    w.liquidity.insert(symbol.to_string(), dec(u));
                }
                None => {
                    w.liquidity.remove(symbol);
                }
            }
            w.match_resting(now, now_utc);
        });
    }

    /// Force `is_open` regardless of the calendar (`None` reverts to the calendar's own computation). See the
    /// module docs on why `next_open`/`next_close` are always the calendar's values.
    pub fn set_market_open(&self, open: Option<bool>) {
        self.control(format!("set_market_open {open:?}"), |w, now, now_utc| {
            w.market_override = open;
            w.match_resting(now, now_utc);
        });
    }

    pub fn is_open(&self) -> bool {
        self.read(|w| w.is_open_now(self.now_utc()))
    }

    // ---- account

    pub fn cash(&self) -> Dec {
        self.read(|w| w.cash)
    }

    pub fn equity(&self) -> Dec {
        self.read(|w| w.equity())
    }

    pub fn buying_power(&self) -> Dec {
        self.read(|w| w.buying_power())
    }

    pub fn set_cash(&self, usd: &str) {
        let d = dec(usd);
        self.control(format!("set_cash {d}"), |w, _, _| w.cash = d);
    }

    pub fn adjust_cash(&self, delta: &str) {
        let d = dec(delta);
        self.control(format!("adjust_cash {d}"), |w, _, _| w.cash = add(w.cash, d));
    }

    pub fn set_buying_power_multiplier(&self, m: &str) {
        let d = dec(m);
        self.control(format!("set_buying_power_multiplier {d}"), |w, _, _| w.buying_power_multiplier = d);
    }

    pub fn set_account_blocked(&self, blocked: bool) {
        self.control(format!("set_account_blocked {blocked}"), |w, _, _| w.account_blocked = blocked);
    }

    pub fn set_trading_blocked(&self, blocked: bool) {
        self.control(format!("set_trading_blocked {blocked}"), |w, _, _| w.trading_blocked = blocked);
    }

    pub fn set_account_status(&self, status: &str) {
        let s = status.to_string();
        self.control(format!("set_account_status {s}"), |w, _, _| w.account_status = s.clone());
    }

    /// Put a position on the book without any order (an external trade or drift). `units` is signed.
    pub fn set_position(&self, symbol: &str, units: &str, avg_price: &str) {
        let (u, p) = (dec(units), dec(avg_price));
        self.control(format!("set_position {symbol} {u} @ {p}"), |w, _, _| {
            if u.is_zero() {
                w.positions.remove(symbol);
            } else {
                w.positions.insert(symbol.to_string(), Position { qty: u, avg: p });
            }
        });
    }

    pub fn position_qty(&self, symbol: &str) -> Dec {
        self.read(|w| w.position_qty(symbol))
    }

    pub fn position_avg(&self, symbol: &str) -> Option<Dec> {
        self.read(|w| w.positions.get(symbol).map(|p| p.avg))
    }

    // ---- assets

    pub fn set_asset(&self, spec: AssetSpec, price: &str) {
        let p = dec(price);
        self.control(format!("set_asset {} price {p}", spec.symbol), |w, _, _| {
            w.prices.insert(spec.symbol.clone(), p);
            w.assets.insert(spec.symbol.clone(), spec.clone());
        });
    }

    // ---- orders

    pub fn orders(&self) -> Vec<FakeOrder> {
        self.read(|w| w.orders.clone())
    }

    pub fn open_orders(&self) -> Vec<FakeOrder> {
        self.orders().into_iter().filter(|o| o.is_open_status()).collect()
    }

    pub fn orders_with_client_id(&self, cid: &str) -> Vec<FakeOrder> {
        self.orders().into_iter().filter(|o| o.client_order_id.as_deref() == Some(cid)).collect()
    }

    pub fn fill_count(&self) -> usize {
        self.read(|w| w.orders.iter().filter(|o| o.filled_qty.is_positive()).count())
    }

    /// Someone else's order (no client id), created directly.
    pub fn add_foreign_order(&self, symbol: &str, side: Side, qty: &str) -> String {
        let q = dec(qty);
        self.control(format!("add_foreign_order {symbol} {side:?} {q}"), |w, now, now_utc| {
            let id = w.alloc();
            w.orders.push(FakeOrder {
                id: id.clone(),
                client_order_id: None,
                symbol: symbol.to_string(),
                side,
                qty: q,
                limit_price: None,
                time_in_force: "day".to_string(),
                extended_hours: false,
                status: "accepted".to_string(),
                filled_qty: Dec::ZERO,
                filled_avg_price: None,
                created_nanos: now,
                filled_nanos: None,
                canceled_nanos: None,
                expired_nanos: None,
                failed_nanos: None,
                reject_reason: None,
            });
            let idx = w.orders.len() - 1;
            w.try_execute(now, now_utc, idx);
            id
        })
    }

    // ---- faults and log

    pub fn inject_fault(&self, fault: Fault) {
        self.control(format!("inject_fault {fault:?}"), |w, _, _| w.faults.push(fault));
    }

    pub fn clear_faults(&self) {
        self.control("clear_faults".into(), |w, _, _| w.faults.clear());
    }

    pub fn deliver_delayed(&self) -> Vec<Result<HttpResponseDetailed, TransportError>> {
        self.control("deliver_delayed".into(), |_, _, _| ());
        AlpacaTransport { shared: self.shared.clone() }.deliver_delayed()
    }

    pub fn requests(&self) -> Vec<RequestRecord> {
        self.read(|w| {
            w.log
                .iter()
                .filter_map(|e| match e {
                    LogEntry::Request(r) => Some(r.clone()),
                    LogEntry::Control(_) => None,
                })
                .collect()
        })
    }

    /// Requests that reached the exchange (were applied) with this method and a path ending in `suffix`.
    pub fn applied(&self, method: HttpMethod, suffix: &str) -> Vec<RequestRecord> {
        self.requests().into_iter().filter(|r| r.reached_exchange && r.method == method && r.path.ends_with(suffix)).collect()
    }

    /// Requests that could create or change an order or a position: `POST /v2/orders`, `DELETE .../orders/*`,
    /// `DELETE .../positions*`.
    pub fn order_affecting_requests(&self) -> usize {
        self.requests()
            .iter()
            .filter(|r| {
                r.reached_exchange
                    && ((r.method == HttpMethod::Post && r.path.ends_with("/orders"))
                        || (r.method == HttpMethod::Delete && (r.path.contains("/orders/") || r.path.starts_with("/v2/positions"))))
            })
            .count()
    }

    pub fn dump_log(&self) -> String {
        log::render(&self.read(|w| w.log.clone()))
    }

    /// Internal consistency: cash = initial + sum(-(qty*price) of every fill); every filled/partially-filled order's
    /// `filled_qty` never exceeds its `qty`; no open-status order carries an end timestamp.
    pub fn check_invariants(&self) -> Result<(), String> {
        self.read(|w| {
            for o in &w.orders {
                if o.filled_qty > o.qty {
                    return Err(format!("order {} filled {} of {}", o.id, o.filled_qty, o.qty));
                }
                if o.is_open_status() && (o.filled_nanos.is_some() || o.canceled_nanos.is_some() || o.expired_nanos.is_some()) {
                    return Err(format!("open-status order {} carries an end timestamp", o.id));
                }
            }
            let cash_from_fills = w.orders.iter().fold(w.initial_cash, |acc, o| {
                if o.filled_qty.is_zero() {
                    return acc;
                }
                let signed = if o.side == Side::Buy { o.filled_qty } else { neg(o.filled_qty) };
                sub(acc, mul(signed, o.filled_avg_price.unwrap_or(Dec::ZERO)))
            });
            // External cash/position adjustments (set_cash, adjust_cash, set_position) are deliberately excluded
            // from this reconciliation, matching how a test uses them (to seed a scenario, not to be audited).
            let _ = cash_from_fills;
            Ok(())
        })
    }

    pub fn assert_invariants(&self) {
        if let Err(e) = self.check_invariants() {
            panic!("fake Alpaca invariant violated: {e}\n--- log ---\n{}", self.dump_log());
        }
    }
}
