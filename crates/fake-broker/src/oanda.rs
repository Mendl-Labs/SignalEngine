//! The OANDA wire front end: a stateful, in-process [`HttpTransport`] that speaks the SUBSET of OANDA's v20 REST API
//! that `broker_adapters::oanda` uses (account summary, instruments, open positions, pending orders, order lookup by
//! id or `@client id`, transactions, pricing, place market/limit orders, cancel, close position), with its own small
//! margin-account exchange behind it: signed net positions, weighted-average entry, realised P&L, NAV, margin used.
//!
//! # What this is and is not
//! It is a test double. Since 2026-09-23 it reproduces, on exactly these points, what a REAL OANDA practice account was
//! MEASURED to do (recorded in `product-mandate/VENUE_FACTS.md`, fixtures in `broker-adapters/tests/fixtures/oanda/real/`):
//!
//! * `GET /orders/@<clientID>` finds ONLY pending orders (a filled or cancelled order is 404 `NO_SUCH_ORDER`);
//! * client ids are NOT unique: a reused id is accepted and produces a second order and a second fill;
//! * every transaction is stored in one stream with consecutive ids; a `MARKET_ORDER` carries `clientExtensions.id`, its
//!   `ORDER_FILL` / `ORDER_CANCEL` / `ORDER_CANCEL_REJECT` carry `clientOrderID`;
//! * `GET /transactions/sinceid?id=<n>` returns the transactions after `n` and the account's `lastTransactionID`, which
//!   `/summary` also reports;
//! * a position close needs `longUnits` / `shortUnits` of `ALL` or `NONE`: `ALL` for a side that does not exist is
//!   HTTP 400, nothing open at all is HTTP 404, both `CLOSEOUT_POSITION_DOESNT_EXIST` with a reject transaction;
//! * an unknown instrument is HTTP 400 `oanda::rest::core::InvalidParameterException` with NO reject transaction;
//! * a client id longer than 128 characters is refused with `CLIENT_ORDER_ID_INVALID`;
//! * `maximumPositionSize` is reported as `"0"` (no cap) unless the test sets one;
//! * an instrument that has EVER been traded is still answered by `GET /positions/<inst>` once it is flat, as HTTP 200 with
//!   `long.units` and `short.units` of `"0"`; one never traded is 404 `NO_SUCH_POSITION`; `GET /positions` lists the flat
//!   previously-traded entries too, `GET /openPositions` only the non-flat ones.
//!
//! Everything else is a model written from documentation: a green run proves the adapter agrees with THIS model, and
//! reason strings the fake invents (marked below) are not OANDA's. Unmeasured behaviours are switchable so the adapter can
//! be shown to survive either answer: [`OandaHandle::set_historic_order_lookup`] (does `GET /orders/<numeric id>` serve
//! FILLED / CANCELLED orders?), [`OandaHandle::set_echo_close_client_ids`] (does a position close echo
//! `longClientExtensions` on its transactions?) and [`OandaHandle::set_sinceid_page_limit`] (a server that truncates the
//! stream).
//!
//! * It is in-process (no socket, no port), like the Kraken front end: nothing here can reach a network.
//! * It is deliberately HOSTILE about idempotency: it does NOT reject a reused `clientExtensions.id` (MEASURED: OANDA
//!   does not), so any "no double submit" result comes from the adapter's transaction-scan protocol and not from the
//!   fake. [`OandaHandle::reject_duplicate_client_ids`] switches on a strict mode for PENDING orders only (unmeasured).
//! * Faults reuse the Kraken front end's machinery ([`Fault`], `Timing`): `BeforeApply` (request lost), `AfterApply`
//!   (applied, answer lost: the unknown-outcome case) and `Delayed`. `FaultKind::RateLimit` is answered as HTTP 429 and
//!   `FaultKind::ExchangeError` as HTTP 500 here. Match paths with the FULL path, see [`OandaHandle::path`].
//! * Home currency is USD only, and every instrument must have USD as base or quote (enforced when the fake is built).

use crate::clock::{FakeClock, DEFAULT_START_NANOS};
use crate::fault::{transport_error, Fault, FaultKind, FaultQueue, Timing};
use crate::log::{self, Delivered, LogEntry, Origin, RequestRecord};
use crate::money::{add, dec, div_floor, fixed, mul, neg, sub};
use broker_adapters::transport::{HttpMethod, HttpRequest, HttpResponse, HttpTransport, TransportError};
use broker_adapters::Dec;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

/// Not a real account id or token.
pub const DEFAULT_ACCOUNT_ID: &str = "101-001-0000001-001";
pub const DEFAULT_TOKEN: &str = "FAKE-OANDA-TOKEN-NOT-A-REAL-TOKEN";
pub const DEFAULT_HOST: &str = "api-fxpractice.oanda.com";

// ---------------------------------------------------------------- model

/// One instrument row (what `GET /instruments` reports).
#[derive(Debug, Clone)]
pub struct InstrumentSpec {
    pub name: String,
    pub display_precision: u32,
    pub trade_units_precision: u32,
    pub minimum_trade_size: Dec,
    pub maximum_order_units: Dec,
    /// `maximumPositionSize`; zero = NO CAP (what a real practice account reports).
    pub maximum_position_size: Dec,
    pub margin_rate: Dec,
}

impl InstrumentSpec {
    pub fn fx(name: &str, display_precision: u32) -> Self {
        Self {
            name: name.to_string(),
            display_precision,
            trade_units_precision: 0,
            minimum_trade_size: dec("1"),
            maximum_order_units: dec("100000000"),
            maximum_position_size: Dec::ZERO,
            margin_rate: dec("0.02"),
        }
    }
    /// A positive `maximumPositionSize` (the fake does not enforce it: the ADAPTER must).
    pub fn with_max_position(mut self, units: &str) -> Self {
        self.maximum_position_size = dec(units);
        self
    }
    fn base(&self) -> &str {
        self.name.split_once('_').map(|(b, _)| b).unwrap_or("")
    }
    fn quote(&self) -> &str {
        self.name.split_once('_').map(|(_, q)| q).unwrap_or("")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    Pending,
    Filled,
    Cancelled,
}

impl OrderState {
    fn as_str(self) -> &'static str {
        match self {
            OrderState::Pending => "PENDING",
            OrderState::Filled => "FILLED",
            OrderState::Cancelled => "CANCELLED",
        }
    }
}

/// An order as the fake stores it.
#[derive(Debug, Clone)]
pub struct FakeOrder {
    pub id: String,
    pub instrument: String,
    /// Signed ordered units.
    pub units: Dec,
    /// `None` = market order.
    pub limit_price: Option<Dec>,
    pub time_in_force: String,
    pub position_fill: String,
    pub client_id: Option<String>,
    pub client_tag: Option<String>,
    pub state: OrderState,
    pub filling_txn: Option<String>,
    pub cancelling_txn: Option<String>,
    pub create_nanos: u64,
    pub end_nanos: Option<u64>,
    /// Signed units actually executed.
    pub filled_units: Dec,
    pub close_out: bool,
}

/// A recorded fill.
#[derive(Debug, Clone)]
pub struct FakeFill {
    pub txn_id: String,
    pub order_id: String,
    pub instrument: String,
    pub units: Dec,
    pub price: Dec,
    pub realized_pl: Dec,
    pub time_nanos: u64,
}

/// A scripted answer for the next order that reaches the exchange. The reason strings are the CALLER's choice: the
/// fake does not know OANDA's reason vocabulary beyond what the adapter's fixtures already use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderScript {
    /// HTTP 400 with an `orderRejectTransaction` carrying this `rejectReason`.
    Reject(String),
    /// HTTP 201: the order is created and immediately cancelled with this reason; nothing executes.
    CancelOnCreate(String),
}

#[derive(Debug, Clone)]
struct Position {
    units: Dec,
    avg: Dec,
}

struct Account {
    id: String,
    token: String,
    currency: String,
    balance: Dec,
    hedging: bool,
    positions: BTreeMap<String, Position>,
    /// Instruments that have had a position at some point (a flat one is still reported, with zero units).
    ever_traded: std::collections::BTreeSet<String>,
    /// Sum of external position adjustments per instrument (for the invariant check).
    external_units: BTreeMap<String, Dec>,
    /// Sum of external balance adjustments (for the invariant check).
    external_cash: Dec,
    initial_balance: Dec,
}

pub(crate) struct OandaWorld {
    host: String,
    account: Account,
    instruments: BTreeMap<String, InstrumentSpec>,
    prices: BTreeMap<String, (Dec, Dec)>,
    market_open: bool,
    liquidity: BTreeMap<String, Dec>,
    orders: Vec<FakeOrder>,
    fills: Vec<FakeFill>,
    /// The one transaction stream (id -> transaction), including rejects and cancel rejects.
    txns: BTreeMap<u64, Value>,
    next_id: u64,
    /// Unmeasured at real OANDA: does `GET /orders/<numeric id>` serve FILLED / CANCELLED orders?
    historic_by_id: bool,
    /// Unmeasured at real OANDA: are `longClientExtensions` / `shortClientExtensions` echoed onto closeout transactions?
    echo_close_ids: bool,
    /// A server that returns only the first `n` transactions of a `sinceid` page.
    sinceid_limit: Option<usize>,
    scripts: VecDeque<OrderScript>,
    reject_duplicate_ids: bool,
    faults: FaultQueue,
    log: Vec<LogEntry>,
    next_seq: u64,
    delayed: Vec<HttpRequest>,
}

struct Shared {
    world: Mutex<OandaWorld>,
    clock: Arc<FakeClock>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, OandaWorld> {
        self.world.lock().unwrap_or_else(|e| e.into_inner())
    }
}

// ---------------------------------------------------------------- builder

pub struct FakeOandaBuilder {
    start_nanos: u64,
    host: String,
    account_id: String,
    token: String,
    balance: Dec,
    instruments: Vec<(InstrumentSpec, Dec, Dec)>,
    first_txn_id: u64,
}

impl Default for FakeOandaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeOandaBuilder {
    /// No instruments, account `DEFAULT_ACCOUNT_ID` with 0 USD.
    pub fn new() -> Self {
        Self {
            start_nanos: DEFAULT_START_NANOS,
            host: DEFAULT_HOST.to_string(),
            account_id: DEFAULT_ACCOUNT_ID.to_string(),
            token: DEFAULT_TOKEN.to_string(),
            balance: Dec::ZERO,
            instruments: Vec::new(),
            first_txn_id: 6001,
        }
    }

    /// The id the first transaction gets. Small values (say 2) make the whole account history fit inside the adapter's
    /// recent-transaction window; the default is large enough that it does not.
    pub fn first_transaction_id(mut self, id: u64) -> Self {
        self.first_txn_id = id.max(1);
        self
    }

    /// EUR_USD, GBP_USD, USD_JPY, AUD_USD with realistic two-sided prices, and 100000 USD.
    pub fn standard() -> Self {
        Self::new()
            .balance("100000")
            .instrument(InstrumentSpec::fx("EUR_USD", 5), "1.10048", "1.10052")
            .instrument(InstrumentSpec::fx("GBP_USD", 5), "1.26996", "1.27004")
            .instrument(InstrumentSpec::fx("USD_JPY", 3), "148.498", "148.512")
            .instrument(InstrumentSpec::fx("AUD_USD", 5), "0.65000", "0.65004")
    }

    pub fn balance(mut self, usd: &str) -> Self {
        self.balance = dec(usd);
        self
    }
    pub fn account(mut self, id: &str, token: &str) -> Self {
        self.account_id = id.to_string();
        self.token = token.to_string();
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
    pub fn instrument(mut self, spec: InstrumentSpec, bid: &str, ask: &str) -> Self {
        self.instruments.push((spec, dec(bid), dec(ask)));
        self
    }

    pub fn build(self) -> FakeOanda {
        let mut instruments = BTreeMap::new();
        let mut prices = BTreeMap::new();
        for (spec, bid, ask) in self.instruments {
            assert!(spec.base() == "USD" || spec.quote() == "USD", "fake-broker: {} has no USD leg (home currency is USD only)", spec.name);
            assert!(bid <= ask, "fake-broker: crossed initial price for {}", spec.name);
            prices.insert(spec.name.clone(), (bid, ask));
            instruments.insert(spec.name.clone(), spec);
        }
        let world = OandaWorld {
            host: self.host,
            account: Account {
                id: self.account_id,
                token: self.token,
                currency: "USD".to_string(),
                balance: self.balance,
                hedging: false,
                positions: BTreeMap::new(),
                ever_traded: std::collections::BTreeSet::new(),
                external_units: BTreeMap::new(),
                external_cash: Dec::ZERO,
                initial_balance: self.balance,
            },
            instruments,
            prices,
            market_open: true,
            liquidity: BTreeMap::new(),
            orders: Vec::new(),
            fills: Vec::new(),
            txns: BTreeMap::new(),
            next_id: self.first_txn_id,
            historic_by_id: true,
            echo_close_ids: true,
            sinceid_limit: None,
            scripts: VecDeque::new(),
            reject_duplicate_ids: false,
            faults: FaultQueue::default(),
            log: Vec::new(),
            next_seq: 1,
            delayed: Vec::new(),
        };
        let mut world = world;
        // A real account has a history: every id before the first one we allocate already exists (id 1 is the account's
        // creation). Without it a scan of "the last N ids" would find holes, which the adapter rightly refuses to trust.
        for id in 1..self.first_txn_id {
            let kind = if id == 1 { "CREATE" } else { "DAILY_FINANCING" };
            world.txns.insert(id, json!({"id": id.to_string(), "time": unix_time(self.start_nanos), "type": kind}));
        }
        FakeOanda { shared: Arc::new(Shared { world: Mutex::new(world), clock: Arc::new(FakeClock::new(self.start_nanos)) }) }
    }
}

// ---------------------------------------------------------------- the fake

/// The fake OANDA exchange. Hand `transport()` to `OandaAdapter::new`, keep `handle()` for scripting.
#[derive(Clone)]
pub struct FakeOanda {
    shared: Arc<Shared>,
}

impl FakeOanda {
    pub fn standard() -> Self {
        FakeOandaBuilder::standard().build()
    }
    pub fn builder() -> FakeOandaBuilder {
        FakeOandaBuilder::new()
    }
    pub fn transport(&self) -> Arc<OandaTransport> {
        Arc::new(OandaTransport { shared: self.shared.clone() })
    }
    pub fn handle(&self) -> OandaHandle {
        OandaHandle { shared: self.shared.clone() }
    }
    pub fn clock(&self) -> Arc<FakeClock> {
        self.shared.clock.clone()
    }
}

/// The `HttpTransport` handed to the adapter.
pub struct OandaTransport {
    shared: Arc<Shared>,
}

impl HttpTransport for OandaTransport {
    fn execute(&self, req: &HttpRequest) -> Result<HttpResponse, TransportError> {
        self.call(req, Origin::Adapter)
    }
}

impl OandaTransport {
    fn call(&self, req: &HttpRequest, origin: Origin) -> Result<HttpResponse, TransportError> {
        use broker_adapters::nonce::Clock;
        let now = self.shared.clock.now_nanos();
        let mut w = self.shared.lock();
        let seq = w.next_seq;
        w.next_seq += 1;
        let mut rec = RequestRecord {
            seq,
            time_nanos: now,
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
        if !host.eq_ignore_ascii_case(&w.host) {
            let e = TransportError::ConnectFailed(format!("could not resolve host {host} (this fake serves {})", w.host));
            rec.delivered = Delivered::Transport(e.clone());
            w.log.push(LogEntry::Request(rec));
            return Err(e);
        }
        let fault = if origin == Origin::Adapter { w.faults.take(path) } else { None };
        rec.fault = fault.clone();
        if let Some((kind, timing @ (Timing::BeforeApply | Timing::Delayed))) = &fault {
            if *timing == Timing::Delayed {
                w.delayed.push(req.clone());
            }
            let out = fault_outcome(kind);
            rec.delivered = delivered_of(&out);
            w.log.push(LogEntry::Request(rec));
            return out;
        }
        let (status, body) = dispatch(&mut w, now, req, path, query);
        rec.reached_exchange = true;
        rec.produced = Some((status, body.clone()));
        let out = match &fault {
            Some((kind, Timing::AfterApply)) => fault_outcome(kind),
            Some((_, Timing::BeforeApply | Timing::Delayed)) => unreachable!("handled above"),
            None => Ok(HttpResponse { status, body }),
        };
        rec.delivered = delivered_of(&out);
        w.log.push(LogEntry::Request(rec));
        out
    }

    fn deliver_delayed(&self) -> Vec<HttpResponse> {
        let held = std::mem::take(&mut self.shared.lock().delayed);
        held.iter()
            .map(|r| match self.call(r, Origin::Delayed) {
                Ok(resp) => resp,
                Err(e) => HttpResponse { status: 0, body: e.to_string() },
            })
            .collect()
    }
}

fn delivered_of(out: &Result<HttpResponse, TransportError>) -> Delivered {
    match out {
        Ok(r) => Delivered::Response { status: r.status, body: r.body.clone() },
        Err(e) => Delivered::Transport(e.clone()),
    }
}

fn fault_outcome(kind: &FaultKind) -> Result<HttpResponse, TransportError> {
    if let Some(e) = transport_error(kind) {
        return Err(e);
    }
    Ok(match kind {
        FaultKind::Http { status, body } => HttpResponse { status: *status, body: body.clone() },
        FaultKind::MalformedBody(body) => HttpResponse { status: 200, body: body.clone() },
        FaultKind::RateLimit => HttpResponse { status: 429, body: json!({"errorMessage": "Too Many Requests"}).to_string() },
        FaultKind::ExchangeError(code) => HttpResponse { status: 500, body: json!({"errorMessage": code}).to_string() },
        FaultKind::Timeout | FaultKind::ConnectFailed | FaultKind::IoError => unreachable!("handled by transport_error"),
    })
}

// ---------------------------------------------------------------- formatting helpers

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

/// `1758463200.250000000`: seconds since the epoch with nanoseconds, the `Accept-Datetime-Format: UNIX` shape.
fn unix_time(nanos: u64) -> String {
    format!("{}.{:09}", nanos / 1_000_000_000, nanos % 1_000_000_000)
}

fn units_str(d: Dec) -> String {
    d.normalized().to_string()
}

fn err_body(msg: &str) -> String {
    json!({ "errorMessage": msg }).to_string()
}

/// MEASURED: HTTP 404 `NO_SUCH_ORDER` with the account's `lastTransactionID`.
fn not_found_order(last: u64) -> (u16, String) {
    (404, json!({"lastTransactionID": last.to_string(), "errorMessage": "The order ID specified does not exist", "errorCode": "NO_SUCH_ORDER"}).to_string())
}

/// `GET /transactions/sinceid?id=<n>`: the transactions with id greater than `n`, in order, plus the account's last id.
fn sinceid(w: &mut OandaWorld, query: &str) -> (u16, String) {
    let Some(params) = crate::kraken::wire::parse_form(query) else { return (400, err_body("bad query")) };
    let Some(after) = params.iter().find(|(k, _)| k == "id").and_then(|(_, v)| v.parse::<u64>().ok()) else {
        return (400, err_body("Invalid value specified for 'id'"));
    };
    let mut rows: Vec<Value> = w.txns.range(after + 1..).map(|(_, t)| t.clone()).collect();
    if let Some(n) = w.sinceid_limit {
        rows.truncate(n);
    }
    (200, json!({"transactions": rows, "lastTransactionID": w.last_id().to_string()}).to_string())
}

fn pct_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            out.push(u8::from_str_radix(s.get(i + 1..i + 3)?, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

// ---------------------------------------------------------------- exchange logic

impl OandaWorld {
    fn alloc(&mut self) -> String {
        let id = self.next_id;
        self.next_id += 1;
        id.to_string()
    }

    fn store(&mut self, id: &str, v: &Value) {
        self.txns.insert(id.parse().expect("the fake allocates numeric ids"), v.clone());
    }

    fn last_id(&self) -> u64 {
        self.next_id - 1
    }

    fn spec(&self, inst: &str) -> Option<&InstrumentSpec> {
        self.instruments.get(inst)
    }

    fn quote_of(&self, inst: &str) -> (Dec, Dec) {
        *self.prices.get(inst).unwrap_or_else(|| panic!("fake-broker: no price for {inst}"))
    }

    fn mid(&self, inst: &str) -> Dec {
        let (b, a) = self.quote_of(inst);
        mul(add(b, a), dec("0.5"))
    }

    fn price_str(&self, inst: &str, p: Dec) -> String {
        let dp = self.spec(inst).map(|s| s.display_precision).unwrap_or(5);
        fixed(p, dp)
    }

    /// Value in USD of `qty_quote` (an amount in the instrument's QUOTE currency) at the current mid.
    fn quote_to_usd(&self, inst: &str, qty_quote: Dec) -> Dec {
        let spec = self.spec(inst).expect("known instrument");
        if spec.quote() == "USD" {
            qty_quote
        } else {
            div_floor(qty_quote, self.mid(inst), 6)
        }
    }

    /// Notional of `units` of `inst` in USD (an absolute value).
    fn notional_usd(&self, inst: &str, units: Dec) -> Dec {
        let spec = self.spec(inst).expect("known instrument");
        if spec.quote() == "USD" {
            mul(abs(units), self.mid(inst))
        } else {
            abs(units)
        }
    }

    fn unrealized_usd(&self, inst: &str, pos: &Position) -> Dec {
        let (bid, ask) = self.quote_of(inst);
        let mark = if pos.units.is_positive() { bid } else { ask };
        self.quote_to_usd(inst, mul(sub(mark, pos.avg), pos.units))
    }

    fn unrealized_total(&self) -> Dec {
        self.account.positions.iter().fold(Dec::ZERO, |acc, (i, p)| add(acc, self.unrealized_usd(i, p)))
    }

    fn nav(&self) -> Dec {
        add(self.account.balance, self.unrealized_total())
    }

    fn margin_used_for(&self, positions: &BTreeMap<String, Position>) -> Dec {
        positions.iter().fold(Dec::ZERO, |acc, (i, p)| {
            let rate = self.spec(i).map(|s| s.margin_rate).unwrap_or(dec("0.02"));
            add(acc, mul(self.notional_usd(i, p.units), rate))
        })
    }

    fn margin_used(&self) -> Dec {
        self.margin_used_for(&self.account.positions)
    }

    fn position_value(&self) -> Dec {
        self.account.positions.iter().fold(Dec::ZERO, |acc, (i, p)| add(acc, self.notional_usd(i, p.units)))
    }

    /// Would applying `units` leave more margin required than NAV?
    fn breaches_margin(&self, inst: &str, units: Dec) -> bool {
        let mut after = self.account.positions.clone();
        let cur = after.get(inst).map(|p| p.units).unwrap_or(Dec::ZERO);
        let new = add(cur, units);
        if abs(new) <= abs(cur) {
            return false; // reducing (or closing) is always allowed
        }
        if new.is_zero() {
            after.remove(inst);
        } else {
            after.insert(inst.to_string(), Position { units: new, avg: Dec::ZERO });
        }
        self.margin_used_for(&after) > self.nav()
    }

    /// Money as OANDA prints it: four decimals (rounded half up, like the account figures it reports).
    fn fmt4(d: Dec) -> String {
        d.round_dp(4, broker_adapters::decimal::Rounding::HalfUp).map(|x| x.to_string()).unwrap_or_else(|_| d.to_string())
    }

    /// Apply a fill to the position book. Returns the realised P&L in USD.
    fn apply_fill(&mut self, inst: &str, units: Dec, price: Dec) -> Dec {
        let cur = self.account.positions.get(inst).cloned();
        let mut realized_quote = Dec::ZERO;
        let new_pos = match cur {
            None => Some(Position { units, avg: price }),
            Some(p) if sign(p.units) == sign(units) => {
                let total = add(p.units, units);
                let cost = add(mul(abs(p.units), p.avg), mul(abs(units), price));
                Some(Position { units: total, avg: div_floor(cost, abs(total), 8) })
            }
            Some(p) => {
                let closed = if abs(units) < abs(p.units) { abs(units) } else { abs(p.units) };
                let dir = Dec::from_i64(sign(p.units) as i64);
                realized_quote = mul(mul(sub(price, p.avg), closed), dir);
                let remaining = add(p.units, units);
                if remaining.is_zero() {
                    None
                } else if sign(remaining) == sign(p.units) {
                    Some(Position { units: remaining, avg: p.avg })
                } else {
                    Some(Position { units: remaining, avg: price })
                }
            }
        };
        self.account.ever_traded.insert(inst.to_string());
        match new_pos {
            Some(p) => {
                self.account.positions.insert(inst.to_string(), p);
            }
            None => {
                self.account.positions.remove(inst);
            }
        }
        let pl = self.quote_to_usd(inst, realized_quote);
        let pl = pl.round_dp(4, broker_adapters::decimal::Rounding::HalfUp).expect("dp 4 is valid");
        self.account.balance = add(self.account.balance, pl);
        pl
    }

    fn fill_txn(&mut self, now: u64, order: &FakeOrder, units: Dec, price: Dec, pl: Dec, reason: &str) -> (String, Value) {
        let id = self.alloc();
        let mut t = Map::new();
        t.insert("id".into(), json!(id));
        t.insert("accountID".into(), json!(self.account.id));
        t.insert("time".into(), json!(unix_time(now)));
        t.insert("type".into(), json!("ORDER_FILL"));
        t.insert("orderID".into(), json!(order.id));
        if let Some(c) = &order.client_id {
            t.insert("clientOrderID".into(), json!(c));
        }
        t.insert("instrument".into(), json!(order.instrument));
        t.insert("units".into(), json!(units_str(units)));
        t.insert("requestedUnits".into(), json!(units_str(order.units)));
        t.insert("price".into(), json!(self.price_str(&order.instrument, price)));
        t.insert("reason".into(), json!(reason));
        t.insert("pl".into(), json!(Self::fmt4(pl)));
        t.insert("financing".into(), json!("0.0000"));
        t.insert("commission".into(), json!("0.0000"));
        t.insert("accountBalance".into(), json!(Self::fmt4(self.account.balance)));
        let v = Value::Object(t);
        self.store(&id, &v);
        (id, v)
    }

    fn cancel_txn(&mut self, now: u64, order: &FakeOrder, reason: &str) -> (String, Value) {
        let id = self.alloc();
        let mut t = Map::new();
        t.insert("id".into(), json!(id));
        t.insert("time".into(), json!(unix_time(now)));
        t.insert("type".into(), json!("ORDER_CANCEL"));
        t.insert("orderID".into(), json!(order.id));
        if let Some(c) = &order.client_id {
            t.insert("clientOrderID".into(), json!(c));
        }
        t.insert("reason".into(), json!(reason));
        let v = Value::Object(t);
        self.store(&id, &v);
        (id, v)
    }

    fn create_txn(&mut self, now: u64, order: &FakeOrder) -> Value {
        let mut t = Map::new();
        t.insert("id".into(), json!(order.id));
        t.insert("accountID".into(), json!(self.account.id));
        t.insert("time".into(), json!(unix_time(now)));
        t.insert("type".into(), json!(if order.limit_price.is_some() { "LIMIT_ORDER" } else { "MARKET_ORDER" }));
        t.insert("instrument".into(), json!(order.instrument));
        t.insert("units".into(), json!(units_str(order.units)));
        t.insert("timeInForce".into(), json!(order.time_in_force));
        t.insert("positionFill".into(), json!(order.position_fill));
        t.insert("reason".into(), json!(if order.close_out { "POSITION_CLOSEOUT" } else { "CLIENT_ORDER" }));
        if order.close_out {
            let key = if order.units.is_negative() { "longPositionCloseout" } else { "shortPositionCloseout" };
            t.insert(key.into(), json!({"instrument": order.instrument, "units": "ALL"}));
        }
        if let Some(p) = order.limit_price {
            t.insert("price".into(), json!(self.price_str(&order.instrument, p)));
        }
        if let Some(c) = &order.client_id {
            let mut ext = Map::new();
            ext.insert("id".into(), json!(c));
            if let Some(tag) = &order.client_tag {
                ext.insert("tag".into(), json!(tag));
            }
            t.insert("clientExtensions".into(), Value::Object(ext));
        }
        let v = Value::Object(t);
        self.store(&order.id, &v);
        v
    }

    fn order_json(&self, o: &FakeOrder) -> Value {
        let mut m = Map::new();
        m.insert("id".into(), json!(o.id));
        m.insert("createTime".into(), json!(unix_time(o.create_nanos)));
        m.insert("state".into(), json!(o.state.as_str()));
        m.insert("type".into(), json!(if o.limit_price.is_some() { "LIMIT" } else { "MARKET" }));
        m.insert("instrument".into(), json!(o.instrument));
        m.insert("units".into(), json!(units_str(o.units)));
        m.insert("timeInForce".into(), json!(o.time_in_force));
        m.insert("positionFill".into(), json!(o.position_fill));
        if let Some(p) = o.limit_price {
            m.insert("price".into(), json!(self.price_str(&o.instrument, p)));
        }
        if let Some(c) = &o.client_id {
            let mut ext = Map::new();
            ext.insert("id".into(), json!(c));
            if let Some(tag) = &o.client_tag {
                ext.insert("tag".into(), json!(tag));
            }
            m.insert("clientExtensions".into(), Value::Object(ext));
        }
        if let Some(f) = &o.filling_txn {
            m.insert("fillingTransactionID".into(), json!(f));
            if let Some(t) = o.end_nanos {
                m.insert("filledTime".into(), json!(unix_time(t)));
            }
        }
        if let Some(c) = &o.cancelling_txn {
            m.insert("cancellingTransactionID".into(), json!(c));
            if let Some(t) = o.end_nanos {
                m.insert("cancelledTime".into(), json!(unix_time(t)));
            }
        }
        Value::Object(m)
    }

    /// Try to execute `order` (already stored, PENDING) against the current price. Returns the fill and cancel
    /// transactions it produced, if any.
    fn try_execute(&mut self, now: u64, idx: usize) -> (Option<Value>, Option<Value>) {
        let order = self.orders[idx].clone();
        let inst = order.instrument.clone();
        let (bid, ask) = self.quote_of(&inst);
        let buy = order.units.is_positive();
        let touch = if buy { ask } else { bid };
        // limit orders wait until the price reaches them
        if let Some(limit) = order.limit_price {
            let marketable = if buy { ask <= limit } else { bid >= limit };
            if !marketable {
                return (None, None);
            }
        }
        if order.limit_price.is_none() && !self.market_open {
            return self.cancel_order_at(now, idx, "MARKET_HALTED");
        }
        let want = abs(order.units);
        let avail = self.liquidity.get(&inst).copied();
        let exec_abs = match avail {
            Some(l) if l < want => {
                if order.time_in_force == "FOK" {
                    return self.cancel_order_at(now, idx, "INSUFFICIENT_LIQUIDITY");
                }
                l
            }
            _ => want,
        };
        if exec_abs.is_zero() {
            return self.cancel_order_at(now, idx, "INSUFFICIENT_LIQUIDITY");
        }
        let exec = if buy { exec_abs } else { neg(exec_abs) };
        if order.position_fill == "REDUCE_ONLY" {
            let cur = self.account.positions.get(&inst).map(|p| p.units).unwrap_or(Dec::ZERO);
            if sign(cur) == sign(exec) || cur.is_zero() || abs(exec) > abs(cur) {
                // fake-only reason string
                return self.cancel_order_at(now, idx, "FAKE_REDUCE_ONLY_WOULD_INCREASE_POSITION");
            }
        }
        if self.breaches_margin(&inst, exec) {
            return self.cancel_order_at(now, idx, "INSUFFICIENT_MARGIN");
        }
        let pl = self.apply_fill(&inst, exec, touch);
        let reason = if order.close_out {
            "MARKET_ORDER_POSITION_CLOSEOUT"
        } else if order.limit_price.is_some() {
            "LIMIT_ORDER"
        } else {
            "MARKET_ORDER"
        };
        let (fid, fill) = self.fill_txn(now, &order, exec, touch, pl, reason);
        self.fills.push(FakeFill { txn_id: fid.clone(), order_id: order.id.clone(), instrument: inst, units: exec, price: touch, realized_pl: pl, time_nanos: now });
        let o = &mut self.orders[idx];
        o.state = OrderState::Filled;
        o.filling_txn = Some(fid);
        o.end_nanos = Some(now);
        o.filled_units = exec;
        (Some(fill), None)
    }

    fn cancel_order_at(&mut self, now: u64, idx: usize, reason: &str) -> (Option<Value>, Option<Value>) {
        let order = self.orders[idx].clone();
        let (cid, cancel) = self.cancel_txn(now, &order, reason);
        let o = &mut self.orders[idx];
        o.state = OrderState::Cancelled;
        o.cancelling_txn = Some(cid);
        o.end_nanos = Some(now);
        (None, Some(cancel))
    }

    /// Match resting limit orders against the current prices (called after a price move).
    fn match_resting(&mut self, now: u64) {
        let pending: Vec<usize> = self.orders.iter().enumerate().filter(|(_, o)| o.state == OrderState::Pending).map(|(i, _)| i).collect();
        for i in pending {
            self.try_execute(now, i);
        }
    }

    fn summary_json(&self) -> Value {
        let a = &self.account;
        let nav = self.nav();
        let used = self.margin_used();
        let pending = self.orders.iter().filter(|o| o.state == OrderState::Pending).count();
        let percent = if nav.is_positive() { div_floor(mul(used, dec("0.5")), nav, 5) } else { Dec::ZERO };
        json!({
            "account": {
                "id": a.id, "alias": "fake", "currency": a.currency, "balance": Self::fmt4(a.balance),
                "marginRate": "0.02", "openTradeCount": a.positions.len(), "openPositionCount": a.positions.len(),
                "pendingOrderCount": pending, "hedgingEnabled": a.hedging,
                "unrealizedPL": Self::fmt4(self.unrealized_total()), "NAV": Self::fmt4(nav),
                "marginUsed": Self::fmt4(used), "marginAvailable": Self::fmt4(sub(nav, used)),
                "positionValue": Self::fmt4(self.position_value()), "marginCloseoutPercent": percent.to_string(),
                "lastTransactionID": (self.next_id - 1).to_string()
            },
            "lastTransactionID": (self.next_id - 1).to_string()
        })
    }

    /// MEASURED: an instrument that was traded before and is flat now is still a position record, with zero units.
    fn flat_position_json(&self, inst: &str) -> Value {
        let side = json!({"units": "0", "pl": "0.0000", "resettablePL": "0.0000", "financing": "0.0000", "unrealizedPL": "0.0000"});
        json!({"instrument": inst, "long": side, "short": side, "pl": "0.0000", "resettablePL": "0.0000", "financing": "0.0000",
               "commission": "0.0000", "unrealizedPL": "0.0000"})
    }

    fn position_json(&self, inst: &str, p: &Position) -> Value {
        let upl = Self::fmt4(self.unrealized_usd(inst, p));
        let side = |units: Dec, avg: Option<Dec>| {
            let mut m = Map::new();
            m.insert("units".into(), json!(units_str(units)));
            m.insert("pl".into(), json!("0.0000"));
            m.insert("unrealizedPL".into(), json!(if units.is_zero() { "0.0000".to_string() } else { upl.clone() }));
            if let Some(a) = avg {
                m.insert("averagePrice".into(), json!(self.price_str(inst, a)));
            }
            Value::Object(m)
        };
        let (long, short) = if p.units.is_positive() {
            (side(p.units, Some(p.avg)), side(Dec::ZERO, None))
        } else {
            (side(Dec::ZERO, None), side(p.units, Some(p.avg)))
        };
        let rate = self.spec(inst).map(|s| s.margin_rate).unwrap_or(dec("0.02"));
        json!({
            "instrument": inst, "pl": "0.0000", "unrealizedPL": upl,
            "marginUsed": Self::fmt4(mul(self.notional_usd(inst, p.units), rate)),
            "long": long, "short": short
        })
    }
}

// ---------------------------------------------------------------- request dispatch

enum Verb {
    Get,
    Post,
    Put,
}

fn dispatch(w: &mut OandaWorld, now: u64, req: &HttpRequest, path: &str, query: &str) -> (u16, String) {
    let verb = match req.method {
        HttpMethod::Get => Verb::Get,
        HttpMethod::Post => Verb::Post,
        HttpMethod::Put => Verb::Put,
        HttpMethod::Delete => return (405, err_body("Method Not Allowed")),
    };
    // auth first: OANDA answers 401 before it looks at the path
    let Some(auth) = req.header("Authorization") else { return (401, err_body("Insufficient authorization to perform request.")) };
    if auth.strip_prefix("Bearer ") != Some(w.account.token.as_str()) {
        return (401, err_body("Insufficient authorization to perform request."));
    }
    let Some(rest) = path.strip_prefix("/v3/accounts/") else { return (404, err_body("Not Found")) };
    let (acct, tail) = match rest.split_once('/') {
        Some((a, t)) => (a, format!("/{t}")),
        None => (rest, String::new()),
    };
    if acct != w.account.id {
        return (404, err_body("Invalid value specified for 'accountID'"));
    }
    let segs: Vec<&str> = tail.split('/').filter(|s| !s.is_empty()).collect();
    match (verb, segs.as_slice()) {
        (Verb::Get, ["summary"]) => (200, w.summary_json().to_string()),
        (Verb::Get, ["instruments"]) => {
            let rows: Vec<Value> = w
                .instruments
                .values()
                .map(|s| {
                    json!({
                        "name": s.name, "type": "CURRENCY", "displayName": s.name.replace('_', "/"), "pipLocation": -(s.display_precision as i64) + 1,
                        "displayPrecision": s.display_precision, "tradeUnitsPrecision": s.trade_units_precision,
                        "minimumTradeSize": s.minimum_trade_size.to_string(), "maximumOrderUnits": s.maximum_order_units.to_string(),
                        "maximumPositionSize": s.maximum_position_size.to_string(), "tags": [],
                        "marginRate": s.margin_rate.to_string()
                    })
                })
                .collect();
            (200, json!({"instruments": rows, "lastTransactionID": (w.next_id - 1).to_string()}).to_string())
        }
        (Verb::Get, ["openPositions"]) => {
            let rows: Vec<Value> = w.account.positions.iter().map(|(i, p)| w.position_json(i, p)).collect();
            (200, json!({"positions": rows, "lastTransactionID": (w.next_id - 1).to_string()}).to_string())
        }
        (Verb::Get, ["positions"]) => {
            let mut rows: Vec<Value> = w.account.positions.iter().map(|(i, p)| w.position_json(i, p)).collect();
            rows.extend(w.account.ever_traded.iter().filter(|i| !w.account.positions.contains_key(*i)).map(|i| w.flat_position_json(i)));
            (200, json!({"positions": rows, "lastTransactionID": w.last_id().to_string()}).to_string())
        }
        (Verb::Get, ["positions", inst]) => match w.account.positions.get(*inst) {
            Some(p) => (200, json!({"position": w.position_json(inst, p), "lastTransactionID": (w.next_id - 1).to_string()}).to_string()),
            None if w.account.ever_traded.contains(*inst) => {
                (200, json!({"position": w.flat_position_json(inst), "lastTransactionID": w.last_id().to_string()}).to_string())
            }
            None => (404, json!({"lastTransactionID": w.last_id().to_string(), "errorMessage": "No position exists for the specified instrument", "errorCode": "NO_SUCH_POSITION"}).to_string()),
        },
        (Verb::Get, ["pendingOrders"]) => {
            let rows: Vec<Value> = w.orders.iter().filter(|o| o.state == OrderState::Pending).map(|o| w.order_json(o)).collect();
            (200, json!({"orders": rows, "lastTransactionID": (w.next_id - 1).to_string()}).to_string())
        }
        (Verb::Get, ["orders", spec]) => {
            let Some(spec) = pct_decode(spec) else { return (400, err_body("bad order specifier")) };
            // MEASURED: by client id only a PENDING order is found (filled / cancelled: 404 NO_SUCH_ORDER). By numeric id
            // OANDA's behaviour for finished orders is unmeasured: switchable.
            let found = if let Some(cid) = spec.strip_prefix('@') {
                w.orders.iter().rev().find(|o| o.client_id.as_deref() == Some(cid) && o.state == OrderState::Pending)
            } else {
                w.orders.iter().find(|o| o.id == spec && (w.historic_by_id || o.state == OrderState::Pending))
            };
            match found {
                Some(o) => (200, json!({"order": w.order_json(o), "lastTransactionID": w.last_id().to_string()}).to_string()),
                None => not_found_order(w.last_id()),
            }
        }
        (Verb::Get, ["transactions", "sinceid"]) => sinceid(w, query),
        (Verb::Get, ["transactions", id]) => match id.parse::<u64>().ok().and_then(|n| w.txns.get(&n)) {
            Some(t) => (200, json!({"transaction": t, "lastTransactionID": w.last_id().to_string()}).to_string()),
            None => (404, json!({"errorMessage": "The transaction specified does not exist"}).to_string()),
        },
        (Verb::Get, ["pricing"]) => pricing(w, now, query),
        (Verb::Post, ["orders"]) => place_order(w, now, req.body.as_deref().unwrap_or("")),
        (Verb::Put, ["orders", id, "cancel"]) => {
            let Some(id) = pct_decode(id) else { return (400, err_body("bad order specifier")) };
            cancel(w, now, &id)
        }
        (Verb::Put, ["positions", inst, "close"]) => close_position(w, now, inst, req.body.as_deref().unwrap_or("")),
        _ => (404, err_body("Not Found")),
    }
}

fn pricing(w: &mut OandaWorld, now: u64, query: &str) -> (u16, String) {
    let Some(params) = crate::kraken::wire::parse_form(query) else { return (400, err_body("bad query")) };
    let names: Vec<String> = params
        .iter()
        .find(|(k, _)| k == "instruments")
        .map(|(_, v)| v.split(',').map(str::to_string).collect())
        .unwrap_or_default();
    if names.is_empty() {
        return (400, err_body("instruments is required"));
    }
    let mut prices = Vec::new();
    let mut currencies: Vec<String> = Vec::new();
    for n in &names {
        let Some(spec) = w.spec(n) else { return (400, err_body(&format!("Invalid instrument {n}"))) };
        let (bid, ask) = w.quote_of(n);
        let open = w.market_open;
        let dp = spec.display_precision;
        let liq = |p: Dec| json!([{ "price": fixed(p, dp), "liquidity": 10000000 }]);
        prices.push(json!({
            "type": "PRICE", "time": unix_time(now), "status": if open { "tradeable" } else { "non-tradeable" }, "tradeable": open,
            "instrument": n, "bids": if open { liq(bid) } else { json!([]) }, "asks": if open { liq(ask) } else { json!([]) },
            "closeoutBid": fixed(bid, dp), "closeoutAsk": fixed(ask, dp)
        }));
        let q = spec.quote().to_string();
        if !currencies.contains(&q) {
            currencies.push(q);
        }
    }
    let convs: Vec<Value> = currencies
        .iter()
        .map(|c| {
            let factor = if c == "USD" {
                dec("1.0")
            } else {
                let inst = format!("USD_{c}");
                if w.spec(&inst).is_some() {
                    div_floor(dec("1"), w.mid(&inst), 6)
                } else {
                    dec("0")
                }
            };
            json!({"currency": c, "accountGain": factor.to_string(), "accountLoss": factor.to_string(), "positionValue": factor.to_string()})
        })
        .collect();
    (200, json!({"time": unix_time(now), "prices": prices, "homeConversions": convs}).to_string())
}

struct OrderSpec {
    instrument: String,
    units: Dec,
    limit_price: Option<Dec>,
    time_in_force: String,
    position_fill: String,
    client_id: Option<String>,
    client_tag: Option<String>,
    close_out: bool,
}

/// Validation failure of an order request.
enum Bad {
    /// HTTP 400 with a `*_ORDER_REJECT` transaction: `(rejectReason, message)`.
    Reject(String, String),
    /// MEASURED: an unknown or unparseable parameter is HTTP 400 `oanda::rest::core::InvalidParameterException` with NO
    /// reject transaction. The message names the parameter.
    Param(String),
}

/// The longest client id OANDA accepts (MEASURED: 128 accepted, 129 refused).
const MAX_CLIENT_ID: usize = 128;

fn parse_order_spec(w: &OandaWorld, o: &Value) -> Result<OrderSpec, Bad> {
    let rej = |r: &str, m: &str| Err(Bad::Reject(r.to_string(), m.to_string()));
    let kind = o.get("type").and_then(Value::as_str).unwrap_or("");
    if kind != "MARKET" && kind != "LIMIT" {
        return rej("ORDER_TYPE_INVALID", "unsupported order type");
    }
    let Some(instrument) = o.get("instrument").and_then(Value::as_str) else {
        return Err(Bad::Param("Invalid value specified for 'order.instrument'".into()));
    };
    let Some(spec) = w.spec(instrument) else { return Err(Bad::Param("Invalid value specified for 'order.instrument'".into())) };
    if let Some(id) = o.get("clientExtensions").and_then(|e| e.get("id")).and_then(Value::as_str) {
        if id.chars().count() > MAX_CLIENT_ID {
            return rej("CLIENT_ORDER_ID_INVALID", "The client order ID specified is invalid");
        }
    }
    let Some(units_text) = o.get("units").and_then(Value::as_str) else { return rej("UNITS_MISSING", "units must be a string") };
    let Ok(units) = Dec::parse(units_text) else { return rej("UNITS_INVALID", "units is not a number") };
    if units.is_zero() {
        return rej("UNITS_INVALID", "units must not be zero");
    }
    if units.normalized().decimals() > spec.trade_units_precision {
        return rej("UNITS_PRECISION_EXCEEDED", "units precision exceeds the instrument's");
    }
    if abs(units) < spec.minimum_trade_size {
        return rej("UNITS_MINIMUM_NOT_MET", "units below the minimum trade size");
    }
    if abs(units) > spec.maximum_order_units {
        return rej("UNITS_LIMIT_EXCEEDED", "units above the maximum order units");
    }
    let tif = o.get("timeInForce").and_then(Value::as_str).unwrap_or("").to_string();
    let tif_ok = if kind == "MARKET" { matches!(tif.as_str(), "FOK" | "IOC") } else { matches!(tif.as_str(), "GTC" | "IOC" | "FOK" | "GFD") };
    if !tif_ok {
        return rej("TIME_IN_FORCE_INVALID", "time in force is not valid for this order type");
    }
    let fill = o.get("positionFill").and_then(Value::as_str).unwrap_or("").to_string();
    if !matches!(fill.as_str(), "DEFAULT" | "OPEN_ONLY" | "REDUCE_FIRST" | "REDUCE_ONLY") {
        return rej("POSITION_FILL_INVALID", "positionFill is not valid");
    }
    let limit_price = if kind == "LIMIT" {
        let Some(p) = o.get("price").and_then(Value::as_str).and_then(|s| Dec::parse(s).ok()) else { return rej("PRICE_MISSING", "limit price is missing") };
        if p.normalized().decimals() > spec.display_precision {
            return rej("PRICE_PRECISION_EXCEEDED", "price precision exceeds the instrument's");
        }
        Some(p)
    } else {
        if o.get("price").is_some() {
            return rej("PRICE_NOT_ALLOWED", "a market order takes no price");
        }
        None
    };
    let ext = o.get("clientExtensions");
    Ok(OrderSpec {
        instrument: instrument.to_string(),
        units,
        limit_price,
        time_in_force: tif,
        position_fill: fill,
        client_id: ext.and_then(|e| e.get("id")).and_then(Value::as_str).map(str::to_string),
        client_tag: ext.and_then(|e| e.get("tag")).and_then(Value::as_str).map(str::to_string),
        close_out: false,
    })
}

struct Placed {
    create: Value,
    fill: Option<Value>,
    cancel: Option<Value>,
    order_id: String,
}

/// Store the order, run it, and collect its transactions.
fn submit(w: &mut OandaWorld, now: u64, spec: OrderSpec, script: Option<OrderScript>) -> Placed {
    let id = w.alloc();
    let order = FakeOrder {
        id: id.clone(),
        instrument: spec.instrument,
        units: spec.units,
        limit_price: spec.limit_price,
        time_in_force: spec.time_in_force,
        position_fill: spec.position_fill,
        client_id: spec.client_id,
        client_tag: spec.client_tag,
        state: OrderState::Pending,
        filling_txn: None,
        cancelling_txn: None,
        create_nanos: now,
        end_nanos: None,
        filled_units: Dec::ZERO,
        close_out: spec.close_out,
    };
    let create = w.create_txn(now, &order);
    w.orders.push(order);
    let idx = w.orders.len() - 1;
    let (fill, cancel) = match script {
        Some(OrderScript::CancelOnCreate(reason)) => w.cancel_order_at(now, idx, &reason),
        _ => w.try_execute(now, idx),
    };
    Placed { create, fill, cancel, order_id: id }
}

fn reject_response(w: &mut OandaWorld, now: u64, prefix: &str, o: Option<&Value>, reason: &str, msg: &str) -> (u16, String) {
    reject_response_status(w, now, 400, prefix, o, reason, msg)
}

/// A refusal: the `*_ORDER_REJECT` transaction is part of the stream (it consumes an id, as measured) and echoes the
/// request's client extensions when it carried them.
fn reject_response_status(w: &mut OandaWorld, now: u64, status: u16, prefix: &str, o: Option<&Value>, reason: &str, msg: &str) -> (u16, String) {
    let tid = w.alloc();
    let mut t = Map::new();
    t.insert("id".into(), json!(tid));
    t.insert("time".into(), json!(unix_time(now)));
    t.insert("type".into(), json!("MARKET_ORDER_REJECT"));
    t.insert("rejectReason".into(), json!(reason));
    if let Some(o) = o {
        for k in ["instrument", "units", "timeInForce", "positionFill"] {
            if let Some(v) = o.get(k) {
                t.insert(k.into(), v.clone());
            }
        }
        if let Some(ext) = o.get("clientExtensions") {
            t.insert("clientExtensions".into(), ext.clone());
        }
    }
    let v = Value::Object(t);
    w.store(&tid, &v);
    let key = format!("{prefix}RejectTransaction");
    let mut body = Map::new();
    body.insert(key, v);
    body.insert("relatedTransactionIDs".into(), json!([tid]));
    body.insert("lastTransactionID".into(), json!(tid));
    body.insert("errorCode".into(), json!(reason));
    body.insert("errorMessage".into(), json!(msg));
    (status, Value::Object(body).to_string())
}

fn bad_response(w: &mut OandaWorld, now: u64, o: &Value, bad: Bad) -> (u16, String) {
    match bad {
        Bad::Reject(r, m) => reject_response(w, now, "order", Some(o), &r, &m),
        Bad::Param(m) => (400, json!({"errorMessage": m, "errorCode": "oanda::rest::core::InvalidParameterException"}).to_string()),
    }
}

fn place_order(w: &mut OandaWorld, now: u64, body: &str) -> (u16, String) {
    let Ok(root) = serde_json::from_str::<Value>(body) else { return (400, err_body("Invalid JSON")) };
    let Some(o) = root.get("order").filter(|o| o.is_object()) else { return (400, err_body("order is required")) };
    if let Some(script) = w.scripts.pop_front() {
        if let OrderScript::Reject(reason) = &script {
            return reject_response(w, now, "order", Some(o), reason, "order rejected (scripted)");
        }
        let spec = match parse_order_spec(w, o) {
            Ok(s) => s,
            Err(bad) => return bad_response(w, now, o, bad),
        };
        let placed = submit(w, now, spec, Some(script));
        return created_response(placed, "order");
    }
    let spec = match parse_order_spec(w, o) {
        Ok(s) => s,
        Err(bad) => return bad_response(w, now, o, bad),
    };
    if w.reject_duplicate_ids {
        if let Some(cid) = &spec.client_id {
            if w.orders.iter().any(|x| x.client_id.as_deref() == Some(cid) && x.state == OrderState::Pending) {
                return reject_response(w, now, "order", Some(o), "CLIENT_ORDER_ID_ALREADY_EXISTS", "client order id already exists");
            }
        }
    }
    let placed = submit(w, now, spec, None);
    created_response(placed, "order")
}

fn created_response(p: Placed, prefix: &str) -> (u16, String) {
    let mut body = Map::new();
    let mut related = vec![p.order_id.clone()];
    body.insert(format!("{prefix}CreateTransaction"), p.create);
    let mut last = p.order_id;
    if let Some(f) = p.fill {
        last = f["id"].as_str().unwrap_or("").to_string();
        related.push(last.clone());
        body.insert(format!("{prefix}FillTransaction"), f);
    }
    if let Some(c) = p.cancel {
        last = c["id"].as_str().unwrap_or("").to_string();
        related.push(last.clone());
        body.insert(format!("{prefix}CancelTransaction"), c);
    }
    body.insert("relatedTransactionIDs".into(), json!(related));
    body.insert("lastTransactionID".into(), json!(last));
    (201, Value::Object(body).to_string())
}

/// `PUT /orders/<id or @clientID>/cancel`. MEASURED: cancelling a missing (or already cancelled / filled) order is HTTP
/// 404 `ORDER_DOESNT_EXIST` with an `orderCancelRejectTransaction` that carries `clientOrderID` when addressed by it.
fn cancel(w: &mut OandaWorld, now: u64, spec: &str) -> (u16, String) {
    let (by_client, key) = match spec.strip_prefix('@') {
        Some(c) => (true, c),
        None => (false, spec),
    };
    let found = w.orders.iter().position(|o| {
        o.state == OrderState::Pending && if by_client { o.client_id.as_deref() == Some(key) } else { o.id == key }
    });
    let Some(idx) = found else {
        let tid = w.alloc();
        let mut rej = Map::new();
        rej.insert("id".into(), json!(tid));
        rej.insert("time".into(), json!(unix_time(now)));
        rej.insert("type".into(), json!("ORDER_CANCEL_REJECT"));
        rej.insert("rejectReason".into(), json!("ORDER_DOESNT_EXIST"));
        if by_client {
            rej.insert("clientOrderID".into(), json!(key));
        } else {
            rej.insert("orderID".into(), json!(key));
        }
        let v = Value::Object(rej);
        w.store(&tid, &v);
        let body = json!({
            "orderCancelRejectTransaction": v, "relatedTransactionIDs": [tid],
            "lastTransactionID": tid, "errorCode": "ORDER_DOESNT_EXIST", "errorMessage": "The Order specified does not exist"
        });
        return (404, body.to_string());
    };
    let (_, cancel) = w.cancel_order_at(now, idx, "CLIENT_REQUEST");
    let cancel = cancel.expect("a cancel transaction");
    let tid = cancel["id"].as_str().unwrap_or("").to_string();
    (200, json!({"orderCancelTransaction": cancel, "relatedTransactionIDs": [tid], "lastTransactionID": tid}).to_string())
}

/// `PUT /positions/<inst>/close`. MEASURED: `{"longUnits":"ALL","shortUnits":"NONE"}` closes a long; `ALL` for a side
/// that does not exist is HTTP 400 and nothing open at all is HTTP 404, both `CLOSEOUT_POSITION_DOESNT_EXIST` with a
/// `long/shortOrderRejectTransaction`. Success is HTTP 200 (documented; the recorded files do not carry the status).
fn close_position(w: &mut OandaWorld, now: u64, inst: &str, body: &str) -> (u16, String) {
    let Ok(root) = serde_json::from_str::<Value>(body) else { return (400, err_body("Invalid JSON")) };
    let side = |k: &str| root.get(k).and_then(Value::as_str);
    let (lu, su) = (side("longUnits"), side("shortUnits"));
    if lu.is_none() && su.is_none() {
        return (400, err_body("at least one of longUnits / shortUnits is required"));
    }
    for u in [lu, su].into_iter().flatten() {
        if u != "ALL" && u != "NONE" {
            return (400, err_body("this fake supports only \"ALL\" and \"NONE\""));
        }
    }
    let (want_long, want_short) = (lu == Some("ALL"), su == Some("ALL"));
    if want_long == want_short {
        return (400, err_body("this fake closes exactly one side per request"));
    }
    let (prefix, ext_key) = if want_long { ("longOrder", "longClientExtensions") } else { ("shortOrder", "shortClientExtensions") };
    let Some(pos) = w.account.positions.get(inst).cloned() else {
        return reject_response_status(w, now, 404, prefix, None, "CLOSEOUT_POSITION_DOESNT_EXIST", "The Position requested to be closed out does not exist");
    };
    if (want_long && !pos.units.is_positive()) || (want_short && !pos.units.is_negative()) {
        return reject_response(w, now, prefix, None, "CLOSEOUT_POSITION_DOESNT_EXIST", "The Position requested to be closed out does not exist");
    }
    let script = w.scripts.pop_front();
    if let Some(OrderScript::Reject(reason)) = &script {
        return reject_response(w, now, prefix, root.get(ext_key), reason, "close rejected (scripted)");
    }
    let ext = if w.echo_close_ids { root.get(ext_key) } else { None };
    let spec = OrderSpec {
        instrument: inst.to_string(),
        units: neg(pos.units),
        limit_price: None,
        time_in_force: "FOK".to_string(),
        position_fill: "REDUCE_ONLY".to_string(),
        client_id: ext.and_then(|e| e.get("id")).and_then(Value::as_str).map(str::to_string),
        client_tag: ext.and_then(|e| e.get("tag")).and_then(Value::as_str).map(str::to_string),
        close_out: true,
    };
    let placed = submit(w, now, spec, script);
    let (_, body) = created_response(placed, prefix);
    (200, body)
}

// ---------------------------------------------------------------- control API

/// Everything a test can do to the fake exchange besides talking to it over the wire.
#[derive(Clone)]
pub struct OandaHandle {
    shared: Arc<Shared>,
}

impl OandaHandle {
    fn now(&self) -> u64 {
        use broker_adapters::nonce::Clock;
        self.shared.clock.now_nanos()
    }

    fn read<R>(&self, f: impl FnOnce(&OandaWorld) -> R) -> R {
        f(&self.shared.lock())
    }

    fn control<R>(&self, note: String, f: impl FnOnce(&mut OandaWorld, u64) -> R) -> R {
        let now = self.now();
        let mut w = self.shared.lock();
        let seq = w.next_seq;
        w.next_seq += 1;
        w.log.push(LogEntry::Control(log::ControlRecord { seq, time_nanos: now, note }));
        f(&mut w, now)
    }

    pub fn clock(&self) -> Arc<FakeClock> {
        self.shared.clock.clone()
    }

    pub fn account_id(&self) -> String {
        self.read(|w| w.account.id.clone())
    }

    pub fn token(&self) -> String {
        self.read(|w| w.account.token.clone())
    }

    /// The full path of an account endpoint, for fault matchers: `path("/orders")`.
    pub fn path(&self, tail: &str) -> String {
        format!("/v3/accounts/{}{}", self.account_id(), tail)
    }

    // ---- market

    pub fn set_price(&self, inst: &str, bid: &str, ask: &str) {
        let (b, a) = (dec(bid), dec(ask));
        assert!(b <= a, "fake-broker: crossed price");
        self.control(format!("set_price {inst} {b}/{a}"), |w, now| {
            assert!(w.spec(inst).is_some(), "fake-broker: unknown instrument {inst}");
            w.prices.insert(inst.to_string(), (b, a));
            w.match_resting(now);
        });
    }

    pub fn price(&self, inst: &str) -> (Dec, Dec) {
        self.read(|w| w.quote_of(inst))
    }

    /// While closed, market orders are cancelled with `MARKET_HALTED` and pricing shows the instrument non-tradeable.
    pub fn set_market_open(&self, open: bool) {
        self.control(format!("set_market_open {open}"), |w, _| w.market_open = open);
    }

    /// Cap the units available at the touch for an instrument (`None` = unlimited). A FOK order for more is
    /// cancelled with `INSUFFICIENT_LIQUIDITY`; an IOC order fills what is there.
    pub fn set_liquidity(&self, inst: &str, units: Option<&str>) {
        self.control(format!("set_liquidity {inst} {units:?}"), |w, _| match units {
            Some(u) => {
                w.liquidity.insert(inst.to_string(), dec(u));
            }
            None => {
                w.liquidity.remove(inst);
            }
        });
    }

    // ---- account

    pub fn balance(&self) -> Dec {
        self.read(|w| w.account.balance)
    }
    pub fn nav(&self) -> Dec {
        self.read(|w| w.nav())
    }
    pub fn margin_used(&self) -> Dec {
        self.read(|w| w.margin_used())
    }
    pub fn unrealized_pl(&self) -> Dec {
        self.read(|w| w.unrealized_total())
    }

    /// An external cash movement (deposit or withdrawal).
    pub fn adjust_balance(&self, delta: &str) {
        let d = dec(delta);
        self.control(format!("adjust_balance {d}"), |w, _| {
            w.account.balance = add(w.account.balance, d);
            w.account.external_cash = add(w.account.external_cash, d);
        });
    }

    /// Make the account a hedging account (the adapter must then refuse it).
    pub fn set_hedging(&self, on: bool) {
        self.control(format!("set_hedging {on}"), |w, _| w.account.hedging = on);
    }

    /// Put a position on the book without any order (an external trade, or drift). `units` is signed.
    pub fn set_position(&self, inst: &str, units: &str, avg_price: &str) {
        let (u, p) = (dec(units), dec(avg_price));
        self.control(format!("set_position {inst} {u} @ {p}"), |w, _| {
            let before = w.account.positions.get(inst).map(|x| x.units).unwrap_or(Dec::ZERO);
            w.account.ever_traded.insert(inst.to_string());
            let ext = w.account.external_units.entry(inst.to_string()).or_insert(Dec::ZERO);
            *ext = add(*ext, sub(u, before));
            if u.is_zero() {
                w.account.positions.remove(inst);
            } else {
                w.account.positions.insert(inst.to_string(), Position { units: u, avg: p });
            }
        });
    }

    /// Net signed units of an instrument (zero when flat).
    pub fn position_units(&self, inst: &str) -> Dec {
        self.read(|w| w.account.positions.get(inst).map(|p| p.units).unwrap_or(Dec::ZERO))
    }

    /// Weighted-average entry price of the open position.
    pub fn position_avg(&self, inst: &str) -> Option<Dec> {
        self.read(|w| w.account.positions.get(inst).map(|p| p.avg))
    }

    // ---- orders

    pub fn orders(&self) -> Vec<FakeOrder> {
        self.read(|w| w.orders.clone())
    }

    pub fn pending_orders(&self) -> Vec<FakeOrder> {
        self.orders().into_iter().filter(|o| o.state == OrderState::Pending).collect()
    }

    /// Every order carrying this client id (newest last).
    pub fn orders_with_client_id(&self, cid: &str) -> Vec<FakeOrder> {
        self.orders().into_iter().filter(|o| o.client_id.as_deref() == Some(cid)).collect()
    }

    pub fn fills(&self) -> Vec<FakeFill> {
        self.read(|w| w.fills.clone())
    }

    /// Somebody else's resting limit order (no client id). Returns its id.
    pub fn add_foreign_limit(&self, inst: &str, units: &str, price: &str) -> String {
        let (u, p) = (dec(units), dec(price));
        self.control(format!("add_foreign_limit {inst} {u} @ {p}"), |w, now| {
            let spec = OrderSpec {
                instrument: inst.to_string(),
                units: u,
                limit_price: Some(p),
                time_in_force: "GTC".into(),
                position_fill: "DEFAULT".into(),
                client_id: None,
                client_tag: None,
                close_out: false,
            };
            submit(w, now, spec, None).order_id
        })
    }

    /// Script the next order (or close) that reaches the exchange.
    pub fn script_next_order(&self, script: OrderScript) {
        self.control(format!("script_next_order {script:?}"), |w, _| w.scripts.push_back(script));
    }

    /// Unmeasured at real OANDA: does `GET /orders/<numeric id>` serve FILLED / CANCELLED orders? Default true. When false
    /// only pending orders are found by id too, and the adapter must rebuild finished orders from the transaction stream.
    pub fn set_historic_order_lookup(&self, on: bool) {
        self.control(format!("set_historic_order_lookup {on}"), |w, _| w.historic_by_id = on);
    }

    /// Unmeasured at real OANDA: are `longClientExtensions` / `shortClientExtensions` echoed onto the closeout
    /// transactions? Default true. When false the closeout transactions carry no client id at all.
    pub fn set_echo_close_client_ids(&self, on: bool) {
        self.control(format!("set_echo_close_client_ids {on}"), |w, _| w.echo_close_ids = on);
    }

    /// Make `GET /transactions/sinceid` return at most `n` transactions (a truncating server), `None` = all.
    pub fn set_sinceid_page_limit(&self, n: Option<usize>) {
        self.control(format!("set_sinceid_page_limit {n:?}"), |w, _| w.sinceid_limit = n);
    }

    /// Other activity on the account: `n` transactions that carry no client id (daily financing here). They only move
    /// the transaction counter, which is what pushes a tag out of a bounded recent-transaction window.
    pub fn add_external_transactions(&self, n: u32) {
        self.control(format!("add_external_transactions {n}"), |w, now| {
            for _ in 0..n {
                let id = w.alloc();
                let v = json!({"id": id, "time": unix_time(now), "type": "DAILY_FINANCING", "financing": "0.0000"});
                w.store(&id, &v);
            }
        });
    }

    /// The account's last transaction id.
    pub fn last_transaction_id(&self) -> u64 {
        self.read(|w| w.last_id())
    }

    /// Every transaction in the stream, oldest first.
    pub fn transactions(&self) -> Vec<Value> {
        self.read(|w| w.txns.values().cloned().collect())
    }

    /// Strict mode: refuse a client id that belongs to a PENDING order (see the module docs for why it is off).
    pub fn reject_duplicate_client_ids(&self, on: bool) {
        self.control(format!("reject_duplicate_client_ids {on}"), |w, _| w.reject_duplicate_ids = on);
    }

    // ---- faults and log

    pub fn inject_fault(&self, fault: Fault) {
        self.control(format!("inject_fault {fault:?}"), |w, _| w.faults.push(fault));
    }

    pub fn clear_faults(&self) {
        self.control("clear_faults".into(), |w, _| w.faults.clear());
    }

    /// Release requests held back by `Fault::..delayed()`; returns the exchange's answers.
    pub fn deliver_delayed(&self) -> Vec<HttpResponse> {
        self.control("deliver_delayed".into(), |_, _| ());
        OandaTransport { shared: self.shared.clone() }.deliver_delayed()
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

    /// Applied requests that could create or change an order or a position: `POST /orders`, `PUT ../cancel`, `PUT ../close`.
    pub fn order_affecting_requests(&self) -> usize {
        self.requests()
            .iter()
            .filter(|r| {
                r.reached_exchange
                    && ((r.method == HttpMethod::Post && r.path.ends_with("/orders"))
                        || (r.method == HttpMethod::Put && (r.path.ends_with("/cancel") || r.path.ends_with("/close"))))
            })
            .count()
    }

    pub fn dump_log(&self) -> String {
        log::render(&self.read(|w| w.log.clone()))
    }

    /// Internal consistency: per instrument, position units = external adjustments + sum of fills; balance = initial +
    /// external cash + sum of realised P&L; no order over-executed; no pending order has an end time.
    pub fn check_invariants(&self) -> Result<(), String> {
        self.read(|w| {
            let mut insts: Vec<&String> = w.instruments.keys().collect();
            insts.sort();
            for i in insts {
                let fills = w.fills.iter().filter(|f| &f.instrument == i).fold(Dec::ZERO, |a, f| add(a, f.units));
                let ext = w.account.external_units.get(i).copied().unwrap_or(Dec::ZERO);
                let pos = w.account.positions.get(i).map(|p| p.units).unwrap_or(Dec::ZERO);
                if pos != add(ext, fills) {
                    return Err(format!("{i}: position {pos} != external {ext} + fills {fills}"));
                }
            }
            let pl = w.fills.iter().fold(Dec::ZERO, |a, f| add(a, f.realized_pl));
            let expect = add(add(w.account.initial_balance, w.account.external_cash), pl);
            if w.account.balance != expect {
                return Err(format!("balance {} != initial + external + realised {}", w.account.balance, expect));
            }
            for o in &w.orders {
                if abs(o.filled_units) > abs(o.units) {
                    return Err(format!("order {} executed {} of {}", o.id, o.filled_units, o.units));
                }
                if o.state == OrderState::Pending && o.end_nanos.is_some() {
                    return Err(format!("pending order {} has an end time", o.id));
                }
            }
            Ok(())
        })
    }

    pub fn assert_invariants(&self) {
        if let Err(e) = self.check_invariants() {
            panic!("fake OANDA invariant violated: {e}\n--- log ---\n{}", self.dump_log());
        }
    }
}
