//! SPECIFICATION tests for the ETF sleeve's timing (finding U3, the joint quant + engineering council of
//! 2026-09-25, Rulings 1, 3, 7, 9, 11): the ETF decision is acted on ONCE, on the first driver run after the first bar
//! of the new month exists (one session after the month's last close), never a month late, and only on the run that
//! finds it PENDING.
//!
//! These REPLACE the five U3 characterisation tests of PR #36 (`etf_due_timing.rs`), which pinned the month-late
//! behaviour on purpose and said they had to change with the timing. No test in this file asserts a decision one
//! month behind.
//!
//! # What is under test
//! * every driver run EVALUATES the ETF sleeve (`D_computable` = `latest_decision_date` on the run's panel,
//!   `MonthEndMode::NextMonthBar` retained); `D_acted` is the last decision the account ACTED on (a Completed run that
//!   planned the sleeve); the sleeve is PLANNED iff `D_computable > D_acted` (no `D_acted`: an entry, planned once);
//! * only pending sleeves are handed to the planner (crypto is daily, so it always is);
//! * the run key is over the CONFIGURED sleeves; a no-op run never reads the broker; per-tick memoisation.
//!
//! # The data-provider assumption (still: there is no real provider in this repository)
//! `Vendor` below is an ASSUMED live provider: at 00:10Z of `as_of` only bars dated STRICTLY BEFORE `as_of` exist (the
//! reference tool's `load_api` also drops `date >= today`), optionally with a per-symbol LAG (bars hidden until a
//! given run date) and a bounded look-back window. These tests prove what the code does under that assumption, not
//! what a real vendor delivers at 00:10Z (work item W2 measures that).
//!
//! # The synthetic calendar
//! Weekdays minus holidays, encoded from general knowledge (New Year, MLK, Presidents, Good Friday, Memorial, July 4,
//! Labor, Thanksgiving, Christmas; a holiday on a weekend is simply not observed). It has NOT been cross-checked
//! against an exchange calendar. All data is synthetic; nothing here is vendor data.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::f64::consts::PI;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::{AlpacaAdapter, AlpacaConfig, AlpacaCredentials, AssetTable, Environment, PrepareOptions};
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::{HttpResponse, TransportError};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use common::*;
use rebalancer_core::guard::PricePoint;
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_core::venue::{AlpacaRules, VenueRuleBook};
use rebalancer_risk::state::{AccountState, HaltReason};
use rebalancer_risk::store::{InMemoryStateStore, StateStore};
use rebalancer_run::broker::{AlpacaBroker, Broker};
use rebalancer_run::clock::ManualClock;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveKind, SleeveSpec};
use rebalancer_run::decision::{evaluate, EvalCache};
use rebalancer_run::driver::{find_due_runs, run_all_due, AccountRuntime, ActiveAccount, InMemoryAccountSource};
use rebalancer_run::pipeline::{run_once, RunConfig, RunContext};
use rebalancer_run::record::{ExecutionMode, OutcomeKind, RunKey, RunRecord, SleeveDecision};
use rebalancer_run::stores::{InMemoryRunStore, RunStore};
use rebalancer_run::testkit::{InMemoryAccountLock, NoLedgerRunStore, RecordingNotifier, SwitchKillFlag};
use reference_rules::{decide_etf_trend, MonthEndMode, Options, Panel, PriceSeries, ETF_SYMBOLS};
use serde_json::{json, Value};

const ACCOUNT_FIXTURE: &str = include_str!("../../broker-adapters/tests/fixtures/alpaca/account_ok.json");
const POSITIONS_FIXTURE: &str = include_str!("../../broker-adapters/tests/fixtures/alpaca/positions_ok.json");

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn slot(day: NaiveDate) -> DateTime<Utc> {
    // DAILY_RUN_HOUR_UTC:DAILY_RUN_MINUTE_UTC = 00:10Z, the driver's fixed daily slot.
    at(&format!("{day}T00:10:00Z"))
}

fn all_days(from: NaiveDate, to: NaiveDate) -> Vec<NaiveDate> {
    let mut v = Vec::new();
    let mut d = from;
    while d <= to {
        v.push(d);
        d += Duration::days(1);
    }
    v
}

// ---------------------------------------------------------------------------------------------------------------
// Synthetic trading calendar (weekdays minus holidays) and the independent oracle built on it
// ---------------------------------------------------------------------------------------------------------------

fn nth_weekday(y: i32, m: u32, wd: Weekday, n: u32) -> NaiveDate {
    let mut d = date(y, m, 1);
    while d.weekday() != wd {
        d += Duration::days(1);
    }
    d + Duration::days(7 * (n as i64 - 1))
}

/// The last calendar day of month (y, m).
fn month_end_date(y: i32, m: u32) -> NaiveDate {
    if m == 12 {
        date(y, 12, 31)
    } else {
        date(y, m + 1, 1).pred_opt().unwrap()
    }
}

fn last_weekday(y: i32, m: u32, wd: Weekday) -> NaiveDate {
    let mut d = month_end_date(y, m);
    while d.weekday() != wd {
        d = d.pred_opt().unwrap();
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

struct Calendar {
    holidays: BTreeSet<NaiveDate>,
}

impl Calendar {
    fn us(from_year: i32, to_year: i32) -> Self {
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

    fn is_session(&self, d: NaiveDate) -> bool {
        !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) && !self.holidays.contains(&d)
    }

    fn sessions(&self, from: NaiveDate, to: NaiveDate) -> Vec<NaiveDate> {
        all_days(from, to).into_iter().filter(|d| self.is_session(*d)).collect()
    }

    /// The last session of the month (y, m): independent of the reference rule (it walks the calendar).
    fn last_session(&self, y: i32, m: u32) -> NaiveDate {
        let mut d = month_end_date(y, m);
        while !self.is_session(d) {
            d = d.pred_opt().unwrap();
        }
        d
    }

    fn first_session(&self, y: i32, m: u32) -> NaiveDate {
        let mut d = date(y, m, 1);
        while !self.is_session(d) {
            d += Duration::days(1);
        }
        d
    }

    fn prev_month(y: i32, m: u32) -> (i32, u32) {
        if m == 1 {
            (y - 1, 12)
        } else {
            (y, m - 1)
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Synthetic world: what the market printed. `Vendor` decides what a 00:10Z run may see of it.
// ---------------------------------------------------------------------------------------------------------------

fn month_index(d: NaiveDate) -> f64 {
    ((d.year() - 2018) * 12 + d.month0() as i32) as f64
}

/// Slow exponential drift plus a ~7-month cycle per ETF (different phases), so the month-end trend signal flips from
/// month to month and adjacent decisions differ. Always positive over 1988-2031.
fn etf_close(i: usize, d: NaiveDate) -> f64 {
    let phase = [0.0, 1.7, 3.1, 4.6, 5.9][i] + 0.5 * i as f64;
    let base = [250.0, 60.0, 105.0, 16.0, 90.0][i];
    let m = month_index(d);
    let cyc = (2.0 * PI * (m + phase) / 7.0).sin();
    base * (0.004 * m).exp() * (1.0 + 0.10 * cyc) + 0.002 * base * (d.day() as f64 / 31.0)
}

fn etf_world(cal: &Calendar, from: NaiveDate, to: NaiveDate) -> Panel {
    let days = cal.sessions(from, to);
    Panel::new(ETF_SYMBOLS.iter().enumerate().map(|(i, s)| PriceSeries::new(*s, days.clone(), days.iter().map(|d| etf_close(i, *d)).collect()).unwrap()).collect()).unwrap()
}

fn crypto_close(i: usize, d: NaiveDate) -> f64 {
    let base = [9000.0, 300.0][i];
    let k = (d - date(2018, 1, 1)).num_days() as f64;
    base * (1.0 + 0.25 * (2.0 * PI * (k + 40.0 * i as f64) / 210.0).sin() + 0.0005 * k)
}

fn crypto_world(from: NaiveDate, to: NaiveDate) -> Panel {
    let days = all_days(from, to);
    Panel::new(["BTC", "ETH"].iter().enumerate().map(|(i, s)| PriceSeries::new(*s, days.clone(), days.iter().map(|d| crypto_close(i, *d)).collect()).unwrap()).collect()).unwrap()
}

/// Bars dated `>= from_bar` (of `symbol`, or of every ETF when `None`) are NOT delivered to a run whose `as_of` is
/// before `visible_on`: a vendor that is late with the new month's first bar.
#[derive(Clone, Debug)]
struct Delay {
    symbol: Option<&'static str>,
    from_bar: NaiveDate,
    visible_on: NaiveDate,
}

fn slice(s: &PriceSeries, from: Option<NaiveDate>, to: NaiveDate) -> Option<PriceSeries> {
    let hi = s.dates().partition_point(|d| *d <= to);
    let lo = from.map_or(0, |f| s.dates().partition_point(|d| *d < f));
    if lo >= hi {
        return None;
    }
    PriceSeries::new(s.symbol(), s.dates()[lo..hi].to_vec(), s.closes()[lo..hi].to_vec()).ok()
}

struct Vendor {
    etf: Panel,
    crypto: Option<Panel>,
    /// Return only bars within this many calendar days before the cutoff (keeps the 40-year property run fast).
    window_days: Option<i64>,
    delays: Mutex<Vec<Delay>>,
    unavailable: AtomicBool,
    price_outage: AtomicBool,
    etf_fetches: AtomicUsize,
    crypto_fetches: AtomicUsize,
    newest: Mutex<BTreeMap<String, NaiveDate>>,
}

impl Vendor {
    /// ETF (and crypto) worlds for 2018-2020, every ETF session per the synthetic calendar.
    fn standard(cal: &Calendar) -> Self {
        Self::build(etf_world(cal, date(2018, 1, 1), date(2020, 6, 30)), Some(crypto_world(date(2018, 1, 1), date(2020, 6, 30))), None)
    }

    fn build(etf: Panel, crypto: Option<Panel>, window_days: Option<i64>) -> Self {
        Self {
            etf,
            crypto,
            window_days,
            delays: Mutex::new(Vec::new()),
            unavailable: AtomicBool::new(false),
            price_outage: AtomicBool::new(false),
            etf_fetches: AtomicUsize::new(0),
            crypto_fetches: AtomicUsize::new(0),
            newest: Mutex::new(BTreeMap::new()),
        }
    }

    fn set_delays(&self, delays: Vec<Delay>) {
        *self.delays.lock().unwrap() = delays;
    }

    fn etf_fetches(&self) -> usize {
        self.etf_fetches.load(Ordering::SeqCst)
    }

    fn crypto_fetches(&self) -> usize {
        self.crypto_fetches.load(Ordering::SeqCst)
    }
}

impl DataSource for Vendor {
    fn sleeve_data(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        let etf = sleeve.kind == SleeveKind::EtfTrend;
        let counter = if etf { &self.etf_fetches } else { &self.crypto_fetches };
        counter.fetch_add(1, Ordering::SeqCst);
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(DataError::new("DATA_UNAVAILABLE", "vendor outage"));
        }
        let world = if etf { Some(&self.etf) } else { self.crypto.as_ref() }.ok_or_else(|| DataError::new("DATA_UNAVAILABLE", "no such world"))?;
        let cutoff = as_of.pred_opt().unwrap();
        let delays = self.delays.lock().unwrap().clone();
        let series: Vec<PriceSeries> = world
            .iter()
            .filter_map(|s| {
                let mut to = cutoff;
                if etf {
                    for dl in &delays {
                        if as_of < dl.visible_on && dl.symbol.is_none_or(|x| x == s.symbol()) {
                            to = to.min(dl.from_bar.pred_opt().unwrap());
                        }
                    }
                }
                slice(s, self.window_days.map(|w| cutoff - Duration::days(w)), to)
            })
            .collect();
        if series.is_empty() {
            return Err(DataError::new("DATA_UNAVAILABLE", "empty panel"));
        }
        let panel = Panel::new(series).map_err(|e| DataError::new("DATA_UNAVAILABLE", &e.to_string()))?;
        if let Some(first) = panel.iter().next() {
            self.newest.lock().unwrap().insert(sleeve.id.clone(), first.last_date());
        }
        Ok(SleeveData { panel })
    }

    fn prices(&self, symbols: &[String], now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError> {
        if self.price_outage.load(Ordering::SeqCst) {
            return Err(DataError::new("DATA_UNAVAILABLE", "price outage"));
        }
        Ok(symbols
            .iter()
            .filter_map(|s| {
                let p = match s.as_str() {
                    "BTC/USD" => "9000",
                    "ETH/USD" => "300",
                    "SPY" | "EFA" | "IEF" | "DBC" | "VNQ" => "100",
                    _ => return None,
                };
                Some((s.clone(), PricePoint { price: d(p), as_of: now }))
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Account plumbing: an Alpaca (ETF) account in Assisted mode (tickets, nothing placed), as tests/etf_assisted.rs
// ---------------------------------------------------------------------------------------------------------------

fn etf_sleeve(share: &str) -> SleeveSpec {
    SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: d(share), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "USD".into() }
}

fn crypto_sleeve(share: &str) -> SleeveSpec {
    SleeveSpec { id: "crypto".into(), kind: SleeveKind::CryptoTrend, share: d(share), venue: "alpaca".into(), asset_class: "crypto_spot".into(), quote: "USD".into() }
}

fn mandate() -> mandate_core::mandate::MandateBody {
    let mut v: Value = serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap();
    v["universe"]["venues"] = json!(["alpaca"]);
    v["universe"]["asset_classes"] = json!(["us_etf", "crypto_spot"]);
    v["universe"]["instrument_allow"] = json!(["SPY", "EFA", "IEF", "DBC", "VNQ", "BTC/USD", "ETH/USD"]);
    v["exposure"]["max_asset_class"] = json!({"us_etf": 1.0, "crypto_spot": 1.0});
    v["exposure"]["max_position"] = json!(0.5);
    v["exposure"]["max_turnover_per_day"] = json!(2.0);
    serde_json::from_value(v).unwrap()
}

fn envelope() -> MandateEnvelope {
    MandateEnvelope { version: 1, status: MandateStatus::Active, effective_from: at("1980-01-01T00:00:00Z"), review_by: at("2099-01-01T00:00:00Z") }
}

fn account_named(id: &str, sleeves: Vec<SleeveSpec>) -> ActiveAccount {
    ActiveAccount { account_id: id.into(), tenant_id: "tenant".into(), mandate: mandate(), envelope: envelope(), plan_approved: true, sleeves, mode: ExecutionMode::Assisted }
}

fn account(sleeves: Vec<SleeveSpec>) -> ActiveAccount {
    account_named("alpaca-acct", sleeves)
}

/// The Alpaca account: 5000 of cash and nothing else, or (with `holdings`) the recorded-style positions fixture
/// (SPY, EFA, IEF long) with equity adjusted so cash + positions == equity.
fn account_json(holdings: bool) -> String {
    let acct = ACCOUNT_FIXTURE.replace("\"cash\": \"52840.17\"", "\"cash\": \"5000\"");
    if holdings {
        acct.replace("\"equity\": \"100210.55\"", "\"equity\": \"14764.65\"")
            .replace("\"portfolio_value\": \"100210.55\"", "\"portfolio_value\": \"14764.65\"")
            .replace("\"long_market_value\": \"47370.38\"", "\"long_market_value\": \"9764.65\"")
    } else {
        acct.replace("\"equity\": \"100210.55\"", "\"equity\": \"5000\"")
            .replace("\"portfolio_value\": \"100210.55\"", "\"portfolio_value\": \"5000\"")
            .replace("\"long_market_value\": \"47370.38\"", "\"long_market_value\": \"0\"")
    }
}

struct Rig<'a> {
    broker: &'a dyn Broker,
    book: &'a VenueRuleBook<'a>,
    transport: Arc<FakeTransport>,
    /// While set, every broker request fails to connect.
    down: Arc<AtomicBool>,
}

impl Rig<'_> {
    fn requests(&self) -> usize {
        self.transport.request_count()
    }
}

fn with_alpaca<R>(holdings: bool, f: impl FnOnce(&Rig<'_>) -> R) -> R {
    let acct = account_json(holdings);
    let positions = if holdings { POSITIONS_FIXTURE.to_string() } else { "[]".to_string() };
    let transport = Arc::new(FakeTransport::new());
    let down = Arc::new(AtomicBool::new(false));
    let down_in_handler = down.clone();
    transport.set_handler(move |req| {
        if down_in_handler.load(Ordering::SeqCst) {
            return Err(TransportError::ConnectFailed("broker down".into()));
        }
        let body = if req.url.contains("/v2/account") {
            acct.clone()
        } else if req.url.contains("/v2/positions") {
            positions.clone()
        } else {
            "[]".to_string()
        };
        Ok(HttpResponse { status: 200, body })
    });
    let cfg = AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).unwrap();
    let creds = AlpacaCredentials::new(Environment::Paper, "PKTESTFIXTUREKEY0001", "unit-test-secret-not-a-real-key-9f3a").unwrap();
    let adapter = AlpacaAdapter::new(cfg, creds, transport.clone()).unwrap();
    let broker = AlpacaBroker::us_etf(&adapter);
    let assets = AssetTable::builtin();
    let opts = PrepareOptions { allow_extended_hours: false, min_notional: d("1"), own_tag_prefix: Some("rb1:".into()), refuse_builtin_assets: false };
    let rules = AlpacaRules { assets: &assets, options: &opts };
    let book = VenueRuleBook::new().with("alpaca", &rules);
    f(&Rig { broker: &broker, book: &book, transport, down })
}

/// One simulated deployment: the real driver (`find_due_runs` + `run_all_due`) over shared stores, one tick per day.
struct Sim<'a> {
    rig: &'a Rig<'a>,
    runs: &'a dyn RunStore,
    states: InMemoryStateStore,
    notifier: RecordingNotifier,
    kill: SwitchKillFlag,
    cfg: RunConfig,
    clock: ManualClock,
    lock: InMemoryAccountLock,
    source: InMemoryAccountSource,
}

impl<'a> Sim<'a> {
    fn new(rig: &'a Rig<'a>, runs: &'a dyn RunStore, accounts: Vec<ActiveAccount>) -> Self {
        let source = InMemoryAccountSource::new();
        source.set_accounts(accounts);
        Self {
            rig,
            runs,
            states: InMemoryStateStore::new(),
            notifier: RecordingNotifier::new(),
            kill: SwitchKillFlag::new(),
            cfg: RunConfig::default(),
            clock: ManualClock::new(at("2000-01-01T00:00:00Z")),
            lock: InMemoryAccountLock::new(),
            source,
        }
    }

    /// One driver tick at the 00:10Z slot of `day`: enumerate what is due, run it. Returns `(account, record)`.
    fn tick_all(&self, day: NaiveDate, data: &dyn DataSource) -> Vec<(String, RunRecord)> {
        let now = slot(day);
        self.clock.set(now);
        let due = find_due_runs(&self.source, now).expect("enumeration must not fail");
        let mut runtimes = BTreeMap::new();
        for spec in &due {
            runtimes.insert(spec.account_id.clone(), AccountRuntime { broker: self.rig.broker, data, venue_rules: self.rig.book });
        }
        run_all_due(due, &runtimes, &self.states, self.runs, &self.notifier, &self.kill, &self.clock, &self.lock, &self.cfg)
            .into_iter()
            .map(|o| (o.spec.account_id.clone(), o.result.unwrap_or_else(|e| panic!("{}: not attempted: {e}", o.spec.account_id))))
            .collect()
    }

    /// One tick for a single-account simulation.
    fn tick(&self, day: NaiveDate, data: &dyn DataSource) -> RunRecord {
        let mut v = self.tick_all(day, data);
        assert_eq!(v.len(), 1, "exactly one account is due on {day}");
        v.remove(0).1
    }

    /// Tick every calendar day in `from..=to`, feeding each record to `f`.
    fn drive(&self, data: &dyn DataSource, from: NaiveDate, to: NaiveDate, mut f: impl FnMut(NaiveDate, RunRecord)) {
        for day in all_days(from, to) {
            let rec = self.tick(day, data);
            f(day, rec);
        }
    }

    fn collect(&self, data: &dyn DataSource, from: NaiveDate, to: NaiveDate) -> Vec<(NaiveDate, RunRecord)> {
        let mut v = Vec::new();
        self.drive(data, from, to, |day, rec| v.push((day, rec)));
        v
    }

    /// A run made by hand at an arbitrary scheduled time (the driver only ever produces the 00:10Z slot).
    fn run_direct(&self, acct: &ActiveAccount, scheduled_for: DateTime<Utc>, data: &dyn DataSource) -> RunRecord {
        self.clock.set(scheduled_for);
        let ctx = RunContext {
            account_id: &acct.account_id,
            scheduled_for,
            trading_day: scheduled_for.date_naive(),
            mode: acct.mode,
            sleeves: &acct.sleeves,
            mandate: Some(&acct.mandate),
            envelope: Some(&acct.envelope),
            broker: self.rig.broker,
            data,
            state_store: &self.states,
            clock: &self.clock,
            runs: self.runs,
            notifier: &self.notifier,
            kill_flag: &self.kill,
            venue_rules: self.rig.book,
            config: &self.cfg,
            cache: None,
        };
        run_once(&ctx)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Reading a record in the terms the finding is about
// ---------------------------------------------------------------------------------------------------------------

fn etf_planned(r: &RunRecord) -> bool {
    r.targets.iter().any(|t| t.sleeve == "etf")
}

fn etf_decision(r: &RunRecord) -> &SleeveDecision {
    r.decisions.iter().find(|d| d.sleeve == "etf").expect("the record carries the ETF decision")
}

fn is_etf(symbol: &str) -> bool {
    ETF_SYMBOLS.contains(&symbol)
}

/// Tickets (Assisted mode) on ETF instruments.
fn etf_tickets(r: &RunRecord) -> Vec<String> {
    r.tickets.iter().filter(|t| is_etf(&t.symbol)).map(|t| t.symbol.clone()).collect()
}

/// ETF instruments the planner managed in this run (one line per instrument some target named).
fn etf_lines(r: &RunRecord) -> Vec<String> {
    r.plan.iter().flat_map(|p| p.lines.iter()).filter(|l| is_etf(&l.symbol)).map(|l| l.symbol.clone()).collect()
}

fn find(recs: &[(NaiveDate, RunRecord)], day: NaiveDate) -> &RunRecord {
    &recs.iter().find(|(d, _)| *d == day).unwrap_or_else(|| panic!("no run on {day}")).1
}

/// Days in `[from, to]` on which the ETF sleeve was planned, with the decision date planned.
fn plans_between(recs: &[(NaiveDate, RunRecord)], from: NaiveDate, to: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    recs.iter().filter(|(d, r)| *d >= from && *d <= to && etf_planned(r)).map(|(d, r)| (*d, etf_decision(r).decision_date)).collect()
}

/// The rule's decision at month-end `me` computed on the FULL world with the month-end date asserted (Explicit): what
/// "decide at the last close of the month" means. Independent of the pipeline and of the driver.
fn long_at_month_end(world: &Panel, me: NaiveDate) -> Vec<String> {
    let dec = decide_etf_trend(world, me, &Options::etf_replay(MonthEndMode::Explicit)).unwrap();
    dec.instruments.iter().filter(|i| i.weight > 0.0).map(|i| i.symbol.clone()).collect()
}

fn assert_no_etf_touch(r: &RunRecord, why: &str) {
    assert!(!etf_planned(r), "{why}: the ETF sleeve must not be planned");
    assert!(etf_tickets(r).is_empty(), "{why}: no ETF ticket");
    assert!(etf_lines(r).is_empty(), "{why}: the planner must not manage any ETF instrument");
}

// ---------------------------------------------------------------------------------------------------------------
// The synthetic data must be discriminating: adjacent month-end decisions differ, or a one-month lag is invisible
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn precondition_adjacent_month_end_decisions_differ_so_a_lag_would_be_visible() {
    let cal = Calendar::us(2017, 2021);
    let world = etf_world(&cal, date(2018, 1, 1), date(2020, 6, 30));
    for (correct, previous) in [(date(2019, 9, 30), date(2019, 8, 30)), (date(2020, 1, 31), date(2019, 12, 31)), (date(2020, 2, 28), date(2020, 1, 31)), (date(2021, 5, 28), date(2021, 4, 30))] {
        // 2021 lies outside the 2018-2020 world used elsewhere: build what is needed for it here.
        let w = if correct.year() == 2021 { etf_world(&cal, date(2019, 1, 1), date(2021, 7, 30)) } else { world.clone() };
        assert_ne!(long_at_month_end(&w, correct), long_at_month_end(&w, previous), "fixture must distinguish {correct} from {previous}");
        assert!(!long_at_month_end(&w, correct).is_empty(), "{correct}: a decision with at least one long, so a plan shows ETF tickets");
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Acceptance 1: ETF-only, runs every day: exactly one ETF plan, on 2019-10-02
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn accept_1_etf_only_runs_daily_and_plans_exactly_once_on_the_second_with_the_september_decision() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        // 2019-09-05 is the account's first run: an ENTRY on the decision in force (August). Everything below is the
        // window the acceptance criterion is about.
        let recs = sim.collect(&vendor, date(2019, 9, 5), date(2019, 10, 8));

        let first = &recs[0].1;
        assert!(etf_planned(first) && etf_decision(first).entry, "the first run is an entry");
        assert_eq!(etf_decision(first).decision_date, date(2019, 8, 30));

        let plans = plans_between(&recs, date(2019, 9, 25), date(2019, 10, 8));
        assert_eq!(plans, vec![(date(2019, 10, 2), date(2019, 9, 30))], "exactly one ETF plan in the window: on 10-02, on September's decision");

        for day in [date(2019, 9, 30), date(2019, 10, 1)] {
            let r = find(&recs, day);
            assert_no_etf_touch(r, &format!("{day}"));
            assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_NOTHING_PENDING"), "{day}");
        }
        // On the plan day the ETF tickets ARE the September decision.
        let plan_day = find(&recs, date(2019, 10, 2));
        assert_eq!((plan_day.outcome.kind, plan_day.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"));
        let mut want = long_at_month_end(&etf_world(&cal, date(2018, 1, 1), date(2020, 6, 30)), date(2019, 9, 30));
        want.sort_unstable();
        let mut got = etf_tickets(plan_day);
        got.sort_unstable();
        assert_eq!(got, want, "the tickets are the 09-30 month-end decision, not August's");
        // The September decision is acted on; nothing more that month.
        assert_eq!(runs.last_acted_decision("alpaca-acct", "etf").unwrap(), Some(date(2019, 9, 30)));
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Acceptance 2: weekend month-end
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn accept_2_weekend_month_end_plans_on_march_3_with_the_february_28_decision() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let recs = sim.collect(&vendor, date(2020, 2, 5), date(2020, 3, 6));
        let plans = plans_between(&recs, date(2020, 2, 12), date(2020, 3, 6));
        assert_eq!(plans, vec![(date(2020, 3, 3), date(2020, 2, 28))], "2020-02-29 is a Saturday: the run of Tue 03-03 (first March bar Mon 03-02) plans February 28's decision");
        for day in [date(2020, 2, 29), date(2020, 3, 1), date(2020, 3, 2)] {
            assert_no_etf_touch(find(&recs, day), &format!("{day}"));
        }
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Acceptance 3: holiday-shifted month-ends (Monday holidays)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn accept_3a_monday_holiday_on_the_last_calendar_day_plans_once_the_day_after_the_first_new_month_bar() {
    // 2021-05-31 is Memorial Day (Monday): the last session of May is Fri 05-28, the first June bar is Tue 06-01, so
    // the decision is computable at the run of Wed 06-02 (the day after the first new-month bar's session).
    let cal = Calendar::us(2017, 2022);
    assert!(!cal.is_session(date(2021, 5, 31)) && cal.last_session(2021, 5) == date(2021, 5, 28) && cal.first_session(2021, 6) == date(2021, 6, 1));
    let vendor = Vendor::build(etf_world(&cal, date(2019, 1, 1), date(2021, 9, 30)), None, None);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let recs = sim.collect(&vendor, date(2021, 5, 5), date(2021, 6, 8));
        assert_eq!(plans_between(&recs, date(2021, 5, 15), date(2021, 6, 8)), vec![(date(2021, 6, 2), date(2021, 5, 28))]);
        for day in [date(2021, 5, 31), date(2021, 6, 1)] {
            assert_no_etf_touch(find(&recs, day), &format!("{day}"));
        }
    });
}

#[test]
fn accept_3b_monday_holiday_on_the_first_of_the_month_delays_the_plan_to_the_day_after_the_first_bar() {
    // 2019-09-02 is Labor Day (Monday): the last August session is Fri 08-30, the first September bar is Tue 09-03,
    // so the decision is computable at the run of Wed 09-04.
    let cal = Calendar::us(2017, 2021);
    assert!(!cal.is_session(date(2019, 9, 2)) && cal.first_session(2019, 9) == date(2019, 9, 3));
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let recs = sim.collect(&vendor, date(2019, 8, 6), date(2019, 9, 10));
        assert_eq!(plans_between(&recs, date(2019, 8, 20), date(2019, 9, 10)), vec![(date(2019, 9, 4), date(2019, 8, 30))]);
        for day in [date(2019, 9, 1), date(2019, 9, 2), date(2019, 9, 3)] {
            assert_no_etf_touch(find(&recs, day), &format!("{day}"));
        }
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Acceptance 4: vendor lag (assert only the RECORDED lag fields; the alert itself is work item W7)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn accept_4_vendor_lag_no_plan_until_the_bar_exists_then_exactly_one_plan_and_the_lag_fields_say_so() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    // The first October bar (10-01) is not delivered to runs before 10-05: three runs (10-02, 10-03, 10-04) see no
    // new-month bar.
    vendor.set_delays(vec![Delay { symbol: None, from_bar: date(2019, 10, 1), visible_on: date(2019, 10, 5) }]);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let recs = sim.collect(&vendor, date(2019, 9, 5), date(2019, 10, 9));
        assert_eq!(plans_between(&recs, date(2019, 9, 25), date(2019, 10, 9)), vec![(date(2019, 10, 5), date(2019, 9, 30))], "no plan while the bar is missing, exactly one on the first day it exists");

        let sessions_after_aug_30 = cal.sessions(date(2019, 8, 31), date(2019, 9, 30)).len() as u32;
        for day in [date(2019, 10, 2), date(2019, 10, 3), date(2019, 10, 4)] {
            let r = find(&recs, day);
            assert_no_etf_touch(r, &format!("{day}"));
            let dec = etf_decision(r);
            assert_eq!(dec.computable_decision_date, date(2019, 8, 30), "{day}: the newest COMPLETED month is still August");
            assert_eq!(dec.newest_bar_date, date(2019, 9, 30), "{day}: the newest bar delivered is the last September session");
            assert_eq!(dec.lag_sessions, sessions_after_aug_30, "{day}: the August decision is now {sessions_after_aug_30} sessions old on the data");
            assert!(!dec.pending, "{day}");
        }
        let plan_day = find(&recs, date(2019, 10, 5));
        let dec = etf_decision(plan_day);
        assert_eq!((dec.computable_decision_date, dec.decision_date, dec.newest_bar_date), (date(2019, 9, 30), date(2019, 9, 30), date(2019, 10, 4)));
        assert_eq!(dec.lag_sessions, 4, "acted four sessions (10-01..10-04) after the decision, not the design point of one");
        assert!(dec.pending && dec.planned && dec.acted && !dec.entry);
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Acceptance 5: missed runs
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn accept_5_driver_down_for_days_then_back_acts_once_on_the_september_decision() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let mut recs = sim.collect(&vendor, date(2019, 9, 5), date(2019, 10, 1));
        // The driver is down 10-02 .. 10-06 (no tick at all), back on 10-07.
        recs.extend(sim.collect(&vendor, date(2019, 10, 7), date(2019, 10, 9)));
        assert_eq!(plans_between(&recs, date(2019, 9, 25), date(2019, 10, 9)), vec![(date(2019, 10, 7), date(2019, 9, 30))], "one plan, on the first run back, on the September decision");
        let dec = etf_decision(find(&recs, date(2019, 10, 7)));
        assert_eq!(dec.lag_sessions, 4, "the recorded lag says it was acted late: four sessions after the decision (10-01..10-04)");
        for day in [date(2019, 10, 8), date(2019, 10, 9)] {
            assert_no_etf_touch(find(&recs, day), &format!("{day}"));
        }
        assert_eq!(runs.records().iter().filter(|r| r.scheduled_for >= slot(date(2019, 10, 2)) && r.scheduled_for < slot(date(2019, 10, 7))).count(), 0, "no backlog of missed slots was run");
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Acceptance 7: the run key is over the CONFIGURED sleeves: one slot, one plan
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn accept_7_same_slot_two_different_pending_sets_yields_exactly_one_plan() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sleeves = vec![etf_sleeve("0.5"), crypto_sleeve("0.5")];
        let sim = Sim::new(rig, &runs, vec![account(sleeves)]);
        sim.collect(&vendor, date(2019, 9, 5), date(2019, 10, 1));

        // First attempt of the 10-02 slot: the vendor is late with the first October bar, so only CRYPTO is pending.
        vendor.set_delays(vec![Delay { symbol: None, from_bar: date(2019, 10, 1), visible_on: date(2019, 10, 3) }]);
        let first = sim.tick(date(2019, 10, 2), &vendor);
        assert_eq!(first.targets.iter().map(|t| t.sleeve.as_str()).collect::<Vec<_>>(), ["crypto"]);
        let requests_after_first = rig.requests();
        let finished_before = runs.records().len();

        // Second attempt of the SAME slot: the bar has arrived, so ETF+crypto would now both be pending.
        vendor.set_delays(vec![]);
        let second = sim.tick(date(2019, 10, 2), &vendor);
        assert_eq!(second, first, "the slot's finished record is returned untouched: the second attempt planned nothing");
        assert_eq!(runs.records().len(), finished_before, "still exactly one run for the slot");
        assert_eq!(rig.requests(), requests_after_first, "and the second attempt did not touch the broker");
        assert_eq!(first.key.sleeve_set, "crypto+etf", "the key is over the configured sleeves");

        // The ETF decision is not lost: the next day's run plans it.
        let next = sim.tick(date(2019, 10, 3), &vendor);
        assert!(etf_planned(&next) && etf_decision(&next).decision_date == date(2019, 9, 30));
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Acceptance 8 (partial): a failed-closed run does not advance D_acted; a Completed one does
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn accept_8_a_run_that_fails_closed_does_not_advance_d_acted_and_the_next_completed_run_does() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        sim.collect(&vendor, date(2019, 9, 5), date(2019, 10, 1));
        assert_eq!(runs.last_acted_decision("alpaca-acct", "etf").unwrap(), Some(date(2019, 8, 30)));

        // 10-02: the decision is pending but the broker is down: the run fails closed BEFORE anything is planned.
        rig.down.store(true, Ordering::SeqCst);
        let r = sim.tick(date(2019, 10, 2), &vendor);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_BROKER_UNREACHABLE"));
        let dec = etf_decision(&r);
        assert!(dec.pending && !dec.planned && !dec.acted);
        assert_eq!(runs.last_acted_decision("alpaca-acct", "etf").unwrap(), Some(date(2019, 8, 30)), "D_acted did not move");

        // 10-03: the broker is back but the PRICE feed is down: the run gets as far as planning the sleeve (targets are
        // built) and then fails closed. The sleeve WAS planned in the record, and still nothing counts as acted.
        rig.down.store(false, Ordering::SeqCst);
        vendor.price_outage.store(true, Ordering::SeqCst);
        let r = sim.tick(date(2019, 10, 3), &vendor);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_DATA_ERROR"));
        let dec = etf_decision(&r);
        assert!(dec.pending && dec.planned && !dec.acted, "planned but the run did not complete: not acted");
        assert!(r.tickets.is_empty());
        assert_eq!(runs.last_acted_decision("alpaca-acct", "etf").unwrap(), Some(date(2019, 8, 30)), "D_acted did not move");
        assert!(sim.notifier.codes().iter().filter(|c| **c == "ALERT_RUN_FAILED").count() >= 2);

        // 10-04: everything is back: the SAME decision is still pending, is planned, the run Completes: D_acted advances.
        vendor.price_outage.store(false, Ordering::SeqCst);
        let r = sim.tick(date(2019, 10, 4), &vendor);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"));
        let dec = etf_decision(&r);
        assert_eq!(dec.decision_date, date(2019, 9, 30));
        assert!(dec.pending && dec.planned && dec.acted);
        assert_eq!(runs.last_acted_decision("alpaca-acct", "etf").unwrap(), Some(date(2019, 9, 30)));

        // 10-05: nothing left to act on.
        let r = sim.tick(date(2019, 10, 5), &vendor);
        assert_eq!(r.outcome.code, "RUN_NOTHING_PENDING");
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Acceptance 9: four of five ETFs have the first new-month bar, one lacks it
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn accept_9_one_etf_missing_the_first_new_month_bar_fails_closed_and_recovers_the_next_day() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    // VNQ's first October bar is delivered only from the run of 10-04.
    vendor.set_delays(vec![Delay { symbol: Some("VNQ"), from_bar: date(2019, 10, 1), visible_on: date(2019, 10, 4) }]);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let recs = sim.collect(&vendor, date(2019, 9, 5), date(2019, 10, 7));
        for day in [date(2019, 10, 2), date(2019, 10, 3)] {
            let r = find(&recs, day);
            assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_RULE_ERROR"), "{day}: SPY says September is complete, VNQ says August: MonthEndMismatch");
            assert!(r.outcome.message.contains("VNQ") && r.outcome.message.contains("SPY"), "{}", r.outcome.message);
            assert_no_etf_touch(r, &format!("{day}"));
        }
        assert!(sim.notifier.codes().iter().filter(|c| **c == "ALERT_RUN_FAILED").count() >= 2, "the failed days are alerted");
        // Recovery: the day the missing bar exists the decision is planned, once.
        assert_eq!(plans_between(&recs, date(2019, 9, 25), date(2019, 10, 7)), vec![(date(2019, 10, 4), date(2019, 9, 30))]);
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Acceptance 10: property test over every date 1990-2030
// ---------------------------------------------------------------------------------------------------------------

/// Drive an ETF-only account through EVERY calendar day from December of `first_year - 1` to the end of `last_year`
/// and check the calendar-oracle expectation for every month of `first_year..=last_year`.
fn property_over_years(first_year: i32, last_year: i32) {
    let cal = Calendar::us(1987, 2032);
    // The vendor returns only the trailing 420 calendar days (the rule needs 10 month-ends): keeps daily runs fast.
    let vendor = Vendor::build(etf_world(&cal, date(first_year - 2, 1, 1), date(last_year + 1, 1, 31)), None, Some(420));
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let mut plans: BTreeMap<(i32, u32), Vec<(NaiveDate, NaiveDate)>> = BTreeMap::new();
        let mut days = 0usize;
        // Start in December of the previous year so the entry action happens before the window under test.
        sim.drive(&vendor, date(first_year - 1, 12, 1), date(last_year, 12, 31), |day, rec| {
            days += 1;
            assert_eq!(rec.outcome.kind, OutcomeKind::Completed, "{day}: {:?}", rec.outcome);
            if etf_planned(&rec) {
                plans.entry((day.year(), day.month())).or_default().push((day, etf_decision(&rec).decision_date));
            }
        });
        assert!(days as i64 >= (last_year - first_year + 1) as i64 * 365, "every calendar day was run ({days})");
        for y in first_year..=last_year {
            for m in 1..=12u32 {
                let (py, pm) = Calendar::prev_month(y, m);
                // The oracle walks the calendar: act the day after the first session of the month; decide the last
                // session of the previous month.
                let expected = (cal.first_session(y, m) + Duration::days(1), cal.last_session(py, pm));
                assert_eq!(plans.get(&(y, m)), Some(&vec![expected]), "{y}-{m:02}: exactly one ETF action, on the day after the first session, on the previous month's last session");
            }
        }
        let extra: Vec<_> = plans.keys().filter(|(y, _)| !(first_year..=last_year).contains(y)).collect();
        assert!(extra.iter().all(|k| **k == (first_year - 1, 12)), "no action outside the window except the entry a month earlier: {extra:?}");
    });
}

// One test per span so the four run in parallel (each drives ~3,650-4,000 daily runs through the real pipeline).
#[test]
fn accept_10_property_exactly_one_etf_action_per_calendar_month_1990_to_1999() {
    property_over_years(1990, 1999);
}

#[test]
fn accept_10_property_exactly_one_etf_action_per_calendar_month_2000_to_2009() {
    property_over_years(2000, 2009);
}

#[test]
fn accept_10_property_exactly_one_etf_action_per_calendar_month_2010_to_2019() {
    property_over_years(2010, 2019);
}

#[test]
fn accept_10_property_exactly_one_etf_action_per_calendar_month_2020_to_2030() {
    property_over_years(2020, 2030);
}

// ---------------------------------------------------------------------------------------------------------------
// F1: an ETF + crypto account: only the pending sleeves reach the planner
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn f1_etf_plus_crypto_account_plans_crypto_daily_and_the_etf_sleeve_only_on_pending_decision_runs() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("0.5"), crypto_sleeve("0.5")])]);
        // Two month boundaries: 09-30 (plan 10-02) and 10-31 (Thu; first November bar Fri 11-01; plan Sat 11-02).
        let recs = sim.collect(&vendor, date(2019, 9, 5), date(2019, 11, 6));
        let plan_days: Vec<NaiveDate> = recs.iter().filter(|(_, r)| etf_planned(r)).map(|(d, _)| *d).collect();
        assert_eq!(plan_days, vec![date(2019, 9, 5), date(2019, 10, 2), date(2019, 11, 2)], "ETF: the entry, then once per boundary");

        for (day, r) in &recs {
            assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{day}: crypto keeps every run alive");
            let crypto: Vec<&str> = r.targets.iter().filter(|t| t.sleeve == "crypto").map(|t| t.sleeve.as_str()).collect();
            assert_eq!(crypto, ["crypto"], "{day}: the crypto sleeve is planned on every run");
            assert_eq!(r.decisions.iter().find(|d| d.sleeve == "crypto").map(|d| (d.pending, d.planned, d.lag_sessions)), Some((true, true, 0)), "{day}");
            if plan_days.contains(day) {
                // On an ETF-pending day the crypto sleeve is pending too (daily): the planner receives BOTH sleeves.
                assert_eq!(r.targets.len(), 2, "{day}");
                assert_eq!(etf_lines(r).len(), 5, "{day}: the planner manages all five ETFs");
                assert!(r.plan.as_ref().unwrap().lines.iter().any(|l| l.symbol.ends_with("/USD")), "{day}: and the crypto instruments");
            } else {
                // Otherwise it receives only crypto: ETF instruments are unmanaged, so untouched.
                assert_eq!(r.targets.len(), 1, "{day}");
                assert_no_etf_touch(r, &format!("{day}"));
                assert!(!etf_decision(r).pending && !etf_decision(r).planned, "{day}");
            }
        }
    });
}

#[test]
fn the_planner_leaves_held_etf_instruments_alone_when_the_etf_sleeve_is_not_pending() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(true, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("0.5"), crypto_sleeve("0.5")])]);
        // Entry day: the ETF sleeve is planned, and the planner sees the account's real SPY / EFA / IEF holdings.
        let entry = sim.tick(date(2019, 9, 5), &vendor);
        assert_eq!((entry.outcome.kind, entry.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{:?}", entry.outcome);
        let held: BTreeMap<String, bool> = entry.plan.as_ref().unwrap().lines.iter().filter(|l| is_etf(&l.symbol)).map(|l| (l.symbol.clone(), l.held.is_positive())).collect();
        assert_eq!((held.get("SPY"), held.get("EFA"), held.get("IEF"), held.get("DBC")), (Some(&true), Some(&true), Some(&true), Some(&false)), "planned ETF sleeve: the planner manages what the account holds");

        // Next days: not pending. The account STILL holds SPY / EFA / IEF, the planner is not given the ETF sleeve, so
        // it emits no line, no order and no skip for any of them (planner.rs: "Instruments the sleeves do not name
        // are left alone (unmanaged)").
        for day in all_days(date(2019, 9, 6), date(2019, 9, 12)) {
            let r = sim.tick(day, &vendor);
            assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{day}: {:?}", r.outcome);
            assert!(r.pre_snapshot.as_ref().unwrap().holdings.get("SPY").is_some_and(|q| q.is_positive()), "{day}: the broker really reports the ETF holdings");
            assert_no_etf_touch(&r, &format!("{day}"));
            let plan = r.plan.as_ref().unwrap();
            assert!(plan.skipped.iter().all(|s| !is_etf(&s.symbol)), "{day}: no skip recorded for an unmanaged ETF: {:?}", plan.skipped);
            assert!(plan.orders.iter().chain(plan.denied.iter().map(|d| &d.order)).all(|o| !is_etf(&o.symbol)), "{day}");
        }
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Entry, recorded decision fields
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_new_account_plans_the_decision_in_force_once_as_an_entry_mid_month() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let first = sim.tick(date(2019, 9, 17), &vendor);
        let dec = etf_decision(&first);
        assert!(dec.pending && dec.entry && dec.planned && dec.acted, "{dec:?}");
        assert_eq!((dec.decision_date, dec.last_acted_decision), (date(2019, 8, 30), None));
        assert!(!etf_tickets(&first).is_empty(), "the ETF exposure starts on day one, not at the next month-end");
        let second = sim.tick(date(2019, 9, 18), &vendor);
        assert_eq!(second.outcome.code, "RUN_NOTHING_PENDING");
        assert_eq!(etf_decision(&second).last_acted_decision, Some(date(2019, 8, 30)));
    });
}

#[test]
fn the_record_carries_the_structured_decision_fields_and_per_instrument_evidence() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let recs = sim.collect(&vendor, date(2019, 9, 5), date(2019, 10, 2));
        let r = find(&recs, date(2019, 10, 2));
        let dec = etf_decision(r);
        assert_eq!(dec.kind, SleeveKind::EtfTrend);
        assert_eq!(
            (dec.decision_date, dec.computable_decision_date, dec.newest_bar_date, dec.lag_sessions, dec.last_acted_decision),
            (date(2019, 9, 30), date(2019, 9, 30), date(2019, 10, 1), 1, Some(date(2019, 8, 30))),
            "acted on the first run after the first October bar: one session after the decision (the pre-registered delay)"
        );
        assert!(dec.pending && dec.planned && dec.acted && !dec.entry);
        assert_eq!(r.data_fingerprints.len(), 1);
        assert_eq!(r.targets[0].decision_date, date(2019, 9, 30));
        assert_eq!(r.targets[0].data_fingerprint, r.data_fingerprints[0].1);

        assert_eq!(dec.instruments.iter().map(|i| i.symbol.as_str()).collect::<Vec<_>>(), ETF_SYMBOLS.to_vec());
        for (i, ev) in dec.instruments.iter().enumerate() {
            assert_eq!(ev.close, etf_close(i, date(2019, 9, 30)), "{}: the close compared is the 09-30 close", ev.symbol);
            assert!((ev.margin_bps - (ev.close / ev.sma - 1.0) * 10_000.0).abs() < 1e-9, "{}", ev.symbol);
            assert_eq!(ev.weight > 0.0, ev.margin_bps > 0.0, "{}: long iff the close is above the average", ev.symbol);
        }
        // A non-pending day records its decision too (that is what a monitor reads), without a target.
        let idle = find(&recs, date(2019, 9, 20));
        assert_eq!(etf_decision(idle).decision_date, date(2019, 8, 30));
        assert!(idle.targets.is_empty() && !etf_decision(idle).pending);
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Pre-flight: a no-op run never reads the broker; a halted account is not short-circuited
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_run_with_nothing_pending_does_not_touch_the_broker() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let entry = sim.tick(date(2019, 9, 5), &vendor);
        assert!(rig.requests() > 0 && entry.step_names().contains(&"read_account"), "a run that plans reads the broker");

        for day in all_days(date(2019, 9, 6), date(2019, 9, 30)) {
            let before = rig.requests();
            let r = sim.tick(day, &vendor);
            assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_NOTHING_PENDING"), "{day}");
            assert_eq!(rig.requests(), before, "{day}: a no-op run makes ZERO broker requests");
            assert_eq!(r.step_names(), ["acquire_run_key", "kill_flag", "mandate", "decisions"], "{day}: it stops at the pre-flight, before read_account");
            assert!(r.pre_snapshot.is_none() && r.plan.is_none() && r.tickets.is_empty());
        }
        // Pending again on 10-02: the broker is read.
        let before = rig.requests();
        let r = sim.tick(date(2019, 10, 2), &vendor);
        assert!(rig.requests() > before && etf_planned(&r));
    });
}

#[test]
fn a_halted_account_is_not_short_circuited_by_the_pre_flight() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        sim.tick(date(2019, 9, 5), &vendor);
        let current = sim.states.load("alpaca-acct").unwrap().unwrap_or_else(|| AccountState::new("alpaca-acct"));
        let (halted, _) = current.halt(HaltReason::Manual, "requested", at("2019-09-06T00:00:00Z"));
        sim.states.save(current.version(), &halted).unwrap();
        // Nothing is pending on 09-10, yet the halted account goes through the normal steps: refused + reminder alert.
        let r = sim.tick(date(2019, 9, 10), &vendor);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_ACCOUNT_HALTED"), "{:?}", r.outcome);
        assert!(r.step_names().contains(&"read_account"));
        assert!(sim.notifier.codes().contains(&"ALERT_STILL_HALTED"));
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Ledger unavailable (the Postgres store before W6): FAIL CLOSED, never act daily, never act every run
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_store_without_a_decision_ledger_fails_the_etf_run_closed_every_day_and_never_acts() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = NoLedgerRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        // Across a month boundary, so neither "act daily" nor "act every run" nor "act on the pending day" can hide.
        let recs = sim.collect(&vendor, date(2019, 9, 25), date(2019, 10, 8));
        for (day, r) in &recs {
            assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_DECISION_LEDGER_UNAVAILABLE"), "{day}: {:?}", r.outcome);
            assert!(r.outcome.message.contains("decision ledger"), "{}", r.outcome.message);
            assert!(r.targets.is_empty() && r.plan.is_none() && r.tickets.is_empty() && r.placed.is_empty(), "{day}: nothing planned, nothing ticketed");
            assert!(!r.decisions.iter().any(|d| d.acted), "{day}");
        }
        assert_eq!(rig.requests(), 0, "and it failed before the broker was read");
        assert_eq!(sim.notifier.codes().iter().filter(|c| **c == "ALERT_RUN_FAILED").count(), recs.len(), "one Critical alert per failed run");
    });
}

#[test]
fn a_store_without_a_ledger_still_runs_a_crypto_only_account_because_a_daily_sleeve_never_asks() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = NoLedgerRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![crypto_sleeve("1")])]);
        for (day, r) in sim.collect(&vendor, date(2019, 9, 25), date(2019, 9, 30)) {
            assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{day}: {:?}", r.outcome);
            assert!(r.decisions[0].planned && r.decisions[0].last_acted_decision.is_none(), "{day}");
        }
    });
    // ...while an account holding BOTH sleeves fails closed as a whole (the run is one unit).
    with_alpaca(false, |rig| {
        let runs = NoLedgerRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("0.5"), crypto_sleeve("0.5")])]);
        let r = sim.tick(date(2019, 9, 26), &vendor);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_DECISION_LEDGER_UNAVAILABLE"));
        assert!(r.targets.is_empty(), "crypto was not planned either");
    });
}

#[test]
fn the_crypto_sleeve_is_daily_it_is_planned_on_every_run_even_two_runs_on_the_same_date() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let acct = account(vec![crypto_sleeve("1")]);
        let sim = Sim::new(rig, &runs, vec![acct.clone()]);
        let day = date(2019, 9, 26);
        let a = sim.run_direct(&acct, slot(day), &vendor);
        let b = sim.run_direct(&acct, at(&format!("{day}T12:00:00Z")), &vendor);
        for r in [&a, &b] {
            assert_eq!(r.outcome.code, "RUN_COMPLETED");
            assert!(r.decisions[0].pending && r.decisions[0].planned, "a Daily sleeve is pending on every run, whatever was acted before");
        }
        assert_eq!(a.decisions[0].decision_date, b.decisions[0].decision_date, "the same decision date on both runs: gating by decision date would have skipped the second");
    });
}

// ---------------------------------------------------------------------------------------------------------------
// D_acted is monotone by construction (store level)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn d_acted_never_moves_backwards_whatever_order_records_finish_in() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        let recs = sim.collect(&vendor, date(2019, 9, 5), date(2019, 10, 2));
        assert_eq!(runs.last_acted_decision("alpaca-acct", "etf").unwrap(), Some(date(2019, 9, 30)));

        // A record for an OLDER decision (a replayed / re-ordered write) finishes afterwards: D_acted must not regress.
        let mut old = find(&recs, date(2019, 10, 2)).clone();
        old.key = RunKey::new("alpaca-acct", slot(date(2019, 8, 1)), &["etf"]);
        old.scheduled_for = old.key.scheduled_for;
        for d in &mut old.decisions {
            d.decision_date = date(2019, 7, 31);
            d.acted = true;
        }
        runs.begin(&old.key, old.trading_day, old.started_at, 900).unwrap();
        runs.finish(old).unwrap();
        assert_eq!(runs.last_acted_decision("alpaca-acct", "etf").unwrap(), Some(date(2019, 9, 30)), "monotone: an older acted decision is ignored");

        // And a NEWER one moves it forward.
        let mut newer = find(&recs, date(2019, 10, 2)).clone();
        newer.key = RunKey::new("alpaca-acct", slot(date(2019, 11, 3)), &["etf"]);
        newer.scheduled_for = newer.key.scheduled_for;
        for d in &mut newer.decisions {
            d.decision_date = date(2019, 10, 31);
            d.acted = true;
        }
        runs.begin(&newer.key, newer.trading_day, newer.started_at, 900).unwrap();
        runs.finish(newer).unwrap();
        assert_eq!(runs.last_acted_decision("alpaca-acct", "etf").unwrap(), Some(date(2019, 10, 31)));
    });
}

#[test]
fn a_replayed_older_slot_is_never_pending_once_a_newer_decision_was_acted() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let sim = Sim::new(rig, &runs, vec![account(vec![etf_sleeve("1")])]);
        sim.collect(&vendor, date(2019, 9, 5), date(2019, 10, 2));
        // Re-run a slot from before the September decision was acted (a reprocessed/late slot): D_computable (August)
        // is not newer than D_acted (September), so nothing is planned.
        let r = sim.tick(date(2019, 9, 20), &vendor);
        assert_eq!(r.outcome.code, "RUN_NOTHING_PENDING", "that slot already finished: the original record is returned");
        let acct = account(vec![etf_sleeve("1")]);
        let replay = sim.run_direct(&acct, at("2019-09-21T03:00:00Z"), &vendor); // a new key: really evaluated
        assert_eq!(replay.outcome.code, "RUN_NOTHING_PENDING");
        assert!(!etf_decision(&replay).pending);
        assert_eq!(runs.last_acted_decision("alpaca-acct", "etf").unwrap(), Some(date(2019, 9, 30)));
    });
}

// ---------------------------------------------------------------------------------------------------------------
// In-tick memoisation: many accounts share one fetch and one decision per tick
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn many_accounts_share_one_fetch_per_sleeve_kind_per_tick_and_the_cache_is_dropped_between_ticks() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let accounts: Vec<ActiveAccount> = (1..=5).map(|i| account_named(&format!("acct-{i}"), vec![etf_sleeve("0.5"), crypto_sleeve("0.5")])).collect();
        let sim = Sim::new(rig, &runs, accounts);

        let out = sim.tick_all(date(2019, 9, 5), &vendor);
        assert_eq!(out.len(), 5);
        assert!(out.iter().all(|(_, r)| r.outcome.code == "RUN_COMPLETED" && etf_planned(r)), "every account ran and planned its entry");
        assert_eq!((vendor.etf_fetches(), vendor.crypto_fetches()), (1, 1), "five accounts, ONE fetch per sleeve kind");

        // A new tick starts with an empty cache: a corrected bar is re-fetched, never served stale.
        sim.tick_all(date(2019, 9, 6), &vendor);
        assert_eq!((vendor.etf_fetches(), vendor.crypto_fetches()), (2, 2));
    });
}

#[test]
fn the_cache_key_includes_the_run_date_and_a_repeat_question_is_answered_from_memory() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    let cache = EvalCache::new();
    let sleeve = etf_sleeve("1");

    let a = evaluate(&vendor, Some(&cache), &sleeve, date(2019, 10, 1)).unwrap(); // newest bar 09-30: August is the newest complete month
    let b = evaluate(&vendor, Some(&cache), &sleeve, date(2019, 10, 2)).unwrap(); // first October bar exists: September
    assert_eq!((a.decision_date, b.decision_date), (date(2019, 8, 30), date(2019, 9, 30)), "different run dates are different questions");
    assert_eq!(vendor.etf_fetches(), 2);
    assert_ne!(a.fingerprint, b.fingerprint);

    let b2 = evaluate(&vendor, Some(&cache), &sleeve, date(2019, 10, 2)).unwrap();
    assert_eq!(vendor.etf_fetches(), 2, "the repeat did not fetch again");
    assert!(Arc::ptr_eq(&b, &b2), "and got the memoised decision");
    assert_eq!(cache.sizes(), (2, 2));

    // Without a cache every call fetches.
    evaluate(&vendor, None, &sleeve, date(2019, 10, 2)).unwrap();
    assert_eq!(vendor.etf_fetches(), 3);
}

#[test]
fn a_vendor_error_is_memoised_for_the_tick_so_it_is_not_asked_again_by_every_account() {
    let cal = Calendar::us(2017, 2021);
    let vendor = Vendor::standard(&cal);
    vendor.unavailable.store(true, Ordering::SeqCst);
    with_alpaca(false, |rig| {
        let runs = InMemoryRunStore::new();
        let accounts: Vec<ActiveAccount> = (1..=3).map(|i| account_named(&format!("acct-{i}"), vec![etf_sleeve("1")])).collect();
        let sim = Sim::new(rig, &runs, accounts);
        let out = sim.tick_all(date(2019, 9, 5), &vendor);
        assert!(out.iter().all(|(_, r)| (r.outcome.kind, r.outcome.code.as_str()) == (OutcomeKind::FailedClosed, "RUN_DATA_ERROR")));
        assert_eq!(vendor.etf_fetches(), 1, "one failed fetch served all three accounts");
        // Next tick the vendor is back: a fresh fetch, all recover.
        vendor.unavailable.store(false, Ordering::SeqCst);
        let out = sim.tick_all(date(2019, 9, 6), &vendor);
        assert!(out.iter().all(|(_, r)| etf_planned(r)), "recovered on the next tick (an entry each)");
        assert_eq!(vendor.etf_fetches(), 2);
    });
}
