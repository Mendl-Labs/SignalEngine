//! T1 (paper-pilot plan, section 6): the direct-to-paper-orders journey against the fake Alpaca exchange (slice S-9).
//!
//! Per the plan's 2026-09-27 revision (section 1, 8.5): assisted mode is DROPPED from this pilot's critical path.
//! This test exercises `execution = 'paper_orders'` from the very first run -- `ExecutionMode::Live` against
//! Alpaca's PAPER environment (the pipeline's only mode that places real orders; Alpaca has no validate-only) -- an
//! ETF sleeve ENTRY, then TWO real month-end boundaries, all through the REAL `AlpacaAdapter`/`AlpacaBroker` talking
//! to the fake exchange built in `crates/fake-broker/src/alpaca.rs` via `AlpacaRig` (no mock of the adapter or the
//! broker anywhere).
//!
//! Assertions, per the plan's own list: no order is ever placed for tenant B; the ETF decision is acted exactly
//! once per boundary; the resulting order set equals exactly what the planner computed (no silent divergence).
//! Tenant isolation is a REAL negative test (not a comment): tenant B's mandate is active, plan-approved and its ETF
//! sleeve is genuinely DUE on every boundary the test drives through -- `find_due_runs` legitimately returns it as a
//! candidate -- but no `AccountRuntime` is registered for it (mirroring the pilot service's `PILOT_TENANT_ID` /
//! `PILOT_ACCOUNT_ID` single-account binding), and tenant B's OWN separate fake Alpaca exchange is asserted to have
//! received ZERO requests of any kind across the whole run.
//!
//! Fast and DB-free (everything here is `InMemoryRunStore`/`InMemoryStateStore`): no `REBALANCER_PILOT_TEST_DB` gate
//! is needed, unlike `rebalancer-store`'s own Postgres-backed tests (#40's convention).

mod common;

use std::collections::BTreeMap;
use std::f64::consts::PI;
use std::sync::atomic::Ordering;

use broker_adapters::alpaca::{AssetTable, PrepareOptions};
use broker_adapters::transport::HttpMethod;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use common::*;
use fake_broker::alpaca_rig::AlpacaRig;
use rebalancer_core::guard::PricePoint;
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_core::venue::{AlpacaRules, VenueRuleBook};
use rebalancer_risk::store::InMemoryStateStore;
use rebalancer_run::broker::{AlpacaBroker, Broker};
use rebalancer_run::clock::ManualClock;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveKind, SleeveSpec};
use rebalancer_run::driver::{find_due_runs, run_all_due, AccountRuntime, ActiveAccount, DueRunError, InMemoryAccountSource};
use rebalancer_run::pipeline::RunConfig;
use rebalancer_run::record::{ExecutionMode, OutcomeKind, RunRecord};
use rebalancer_run::stores::{InMemoryRunStore, RunStore};
use rebalancer_run::testkit::{InMemoryAccountLock, RecordingNotifier, SwitchKillFlag};
use reference_rules::{decide_etf_trend, MonthEndMode, Options, Panel, PriceSeries, ETF_SYMBOLS};
use serde_json::{json, Value};

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn slot(day: NaiveDate) -> DateTime<Utc> {
    at(&format!("{day}T15:00:00Z"))
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
// A synthetic US trading calendar and ETF world (the same construction proven in
// `rebalancer-run/tests/etf_pending_decision.rs`, rebuilt here so this file is self-contained)
// ---------------------------------------------------------------------------------------------------------------

fn nth_weekday(y: i32, m: u32, wd: Weekday, n: u32) -> NaiveDate {
    let mut d = date(y, m, 1);
    while d.weekday() != wd {
        d += Duration::days(1);
    }
    d + Duration::days(7 * (n as i64 - 1))
}

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

fn last_weekday(y: i32, m: u32, wd: Weekday) -> NaiveDate {
    let month_end = if m == 12 { date(y, 12, 31) } else { date(y, m + 1, 1).pred_opt().unwrap() };
    let mut d = month_end;
    while d.weekday() != wd {
        d = d.pred_opt().unwrap();
    }
    d
}

struct Calendar {
    holidays: std::collections::BTreeSet<NaiveDate>,
}

impl Calendar {
    fn us(from_year: i32, to_year: i32) -> Self {
        let mut holidays = std::collections::BTreeSet::new();
        for y in from_year..=to_year {
            holidays.insert(date(y, 1, 1));
            holidays.insert(nth_weekday(y, 1, Weekday::Mon, 3));
            holidays.insert(nth_weekday(y, 2, Weekday::Mon, 3));
            holidays.insert(easter(y) - Duration::days(2));
            holidays.insert(last_weekday(y, 5, Weekday::Mon));
            holidays.insert(date(y, 7, 4));
            holidays.insert(nth_weekday(y, 9, Weekday::Mon, 1));
            holidays.insert(nth_weekday(y, 11, Weekday::Thu, 4));
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
}

fn month_index(d: NaiveDate) -> f64 {
    ((d.year() - 2018) * 12 + d.month0() as i32) as f64
}

/// Slow drift plus a ~7-month cycle per ETF (different phases), so month-end decisions differ month to month.
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

fn long_at_month_end(world: &Panel, me: NaiveDate) -> Vec<String> {
    let dec = decide_etf_trend(world, me, &Options::etf_replay(MonthEndMode::Explicit)).unwrap();
    dec.instruments.iter().filter(|i| i.weight > 0.0).map(|i| i.symbol.clone()).collect()
}

/// A plain vendor: bars dated strictly before `as_of` only (the forming-bar rule), no delays. Prices always report
/// as of `now` at a flat mark (the sizing side does not need real quotes for this test).
struct Vendor {
    world: Panel,
    fetches: std::sync::atomic::AtomicUsize,
}

impl Vendor {
    fn new(world: Panel) -> Self {
        Self { world, fetches: std::sync::atomic::AtomicUsize::new(0) }
    }
}

impl DataSource for Vendor {
    fn sleeve_data(&self, _sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let cutoff = as_of.pred_opt().unwrap();
        let series: Vec<PriceSeries> = self
            .world
            .iter()
            .filter_map(|s| {
                let hi = s.dates().partition_point(|d| *d <= cutoff);
                if hi == 0 {
                    return None;
                }
                PriceSeries::new(s.symbol(), s.dates()[..hi].to_vec(), s.closes()[..hi].to_vec()).ok()
            })
            .collect();
        if series.is_empty() {
            return Err(DataError::new("DATA_UNAVAILABLE", "empty panel"));
        }
        Ok(SleeveData::new(Panel::new(series).map_err(|e| DataError::new("DATA_UNAVAILABLE", &e.to_string()))?))
    }

    fn prices(&self, symbols: &[String], now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError> {
        Ok(symbols
            .iter()
            .filter_map(|s| self.world.get(s).ok().map(|series| (s.clone(), PricePoint { price: d(&format!("{:.2}", series.closes().last().copied().unwrap_or(100.0))), as_of: now })))
            .collect())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Mandate at L2, direct paper_orders execution (section 3.2/3.5 of the paper-pilot plan)
// ---------------------------------------------------------------------------------------------------------------

fn etf_sleeve() -> SleeveSpec {
    SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: d("1"), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "USD".into() }
}

/// A mandate at autonomy L2, universe/exposure scoped to Alpaca's five pilot ETFs, capital matching the fake
/// exchange's $5,000 cash so the pilot's sizing arithmetic is realistic.
fn pilot_mandate() -> mandate_core::mandate::MandateBody {
    let mut v: Value = serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap();
    v["autonomy"]["level"] = json!("L2");
    v["universe"]["venues"] = json!(["alpaca"]);
    v["universe"]["asset_classes"] = json!(["us_etf"]);
    v["universe"]["instrument_allow"] = json!(["SPY", "EFA", "IEF", "DBC", "VNQ"]);
    v["exposure"]["max_asset_class"] = json!({"us_etf": 1.0});
    v["exposure"]["max_position"] = json!(0.25);
    v["exposure"]["max_turnover_per_day"] = json!(2.0);
    v["exposure"]["max_order_notional"] = json!({"amount": "1500.00", "ccy": "USD"});
    v["capital"]["allocated"]["amount"] = json!("5000.00");
    serde_json::from_value(v).unwrap()
}

fn envelope() -> MandateEnvelope {
    MandateEnvelope { version: 1, status: MandateStatus::Active, effective_from: at("2019-01-01T00:00:00Z"), review_by: at("2099-01-01T00:00:00Z") }
}

fn active_account(account_id: &str, tenant_id: &str) -> ActiveAccount {
    ActiveAccount {
        account_id: account_id.into(),
        tenant_id: tenant_id.into(),
        mandate: pilot_mandate(),
        envelope: envelope(),
        plan_approved: true,
        sleeves: vec![etf_sleeve()],
        mode: ExecutionMode::Live, // execution = 'paper_orders' maps to the Live pipeline on Alpaca's paper host
    }
}

// ---------------------------------------------------------------------------------------------------------------
// One simulated tenant: its own fake Alpaca exchange + real adapter/broker
// ---------------------------------------------------------------------------------------------------------------

struct Tenant {
    rig: AlpacaRig,
}

impl Tenant {
    fn new() -> Self {
        let rig = AlpacaRig::with_config(|mut c| {
            c.own_tag_prefix = Some("rb1:".to_string());
            c.refuse_builtin_assets = false;
            c
        });
        Self { rig }
    }

    fn broker(&self) -> AlpacaBroker<'_> {
        AlpacaBroker::us_etf(&self.rig.adapter)
    }
}

fn book<'a>(rules: &'a AlpacaRules<'a>) -> VenueRuleBook<'a> {
    VenueRuleBook::new().with("alpaca", rules)
}

// ---------------------------------------------------------------------------------------------------------------
// The two-tenant simulation
// ---------------------------------------------------------------------------------------------------------------

const ACCOUNT_A: &str = "dragonstone-etf-pilot";
const TENANT_A: &str = "dragonstone";
const ACCOUNT_B: &str = "other-tenant-etf";
const TENANT_B: &str = "other-tenant";

struct Sim<'a> {
    source: InMemoryAccountSource,
    runs: InMemoryRunStore,
    states: InMemoryStateStore,
    notifier: RecordingNotifier,
    kill: SwitchKillFlag,
    cfg: RunConfig,
    clock: ManualClock,
    lock: InMemoryAccountLock,
    broker_a: &'a dyn Broker,
    book_a: &'a VenueRuleBook<'a>,
    /// The fake EXCHANGE's own clock (separate from the pipeline's `ManualClock` above): `GET /v2/clock` must see
    /// the same instant as the run, or the venue's own (correct) market-hours refusal fires on every historical
    /// date this journey drives through.
    venue_clock_a: std::sync::Arc<fake_broker::FakeClock>,
}

impl<'a> Sim<'a> {
    /// Deliberately takes only tenant A's broker/rules: the pilot service's own single-account binding
    /// (`PILOT_TENANT_ID`/`PILOT_ACCOUNT_ID`) means no `AccountRuntime` for a second tenant is ever constructed,
    /// let alone registered -- not merely "registered but unused".
    fn new(broker_a: &'a dyn Broker, book_a: &'a VenueRuleBook<'a>, venue_clock_a: std::sync::Arc<fake_broker::FakeClock>) -> Self {
        let source = InMemoryAccountSource::new();
        source.set_accounts(vec![active_account(ACCOUNT_A, TENANT_A), active_account(ACCOUNT_B, TENANT_B)]);
        Self {
            source,
            runs: InMemoryRunStore::new(),
            states: InMemoryStateStore::new(),
            notifier: RecordingNotifier::new(),
            kill: SwitchKillFlag::new(),
            cfg: RunConfig::default(),
            clock: ManualClock::new(at("2000-01-01T00:00:00Z")),
            lock: InMemoryAccountLock::new(),
            broker_a,
            book_a,
            venue_clock_a,
        }
    }

    /// One driver tick at the ETF slot: enumerate what is due across BOTH tenants, but register an `AccountRuntime`
    /// for account A ONLY. Returns every candidate's outcome, keyed by account id, so the caller can assert on
    /// tenant B's specifically.
    fn tick(&self, day: NaiveDate, data_a: &dyn DataSource) -> BTreeMap<String, Result<RunRecord, DueRunError>> {
        let now = slot(day);
        self.clock.set(now);
        self.venue_clock_a.set_nanos(now.timestamp_nanos_opt().expect("in range") as u64);
        let due = find_due_runs(&self.source, now).expect("enumeration must not fail");
        // Confirm the test is not vacuous: tenant B really is a due, eligible-looking candidate on every tick.
        assert!(due.iter().any(|s| s.account_id == ACCOUNT_B), "{day}: tenant B must be a genuinely due candidate");
        let mut runtimes = BTreeMap::new();
        for spec in &due {
            if spec.account_id == ACCOUNT_A {
                runtimes.insert(ACCOUNT_A.to_string(), AccountRuntime { broker: self.broker_a, data: data_a, venue_rules: self.book_a });
            }
            // Deliberately no entry for ACCOUNT_B: the pilot service never holds credentials for it.
        }
        run_all_due(due, &runtimes, &self.states, &self.runs, &self.notifier, &self.kill, &self.clock, &self.lock, &self.cfg)
            .into_iter()
            .map(|o| (o.spec.account_id.clone(), o.result))
            .collect()
    }
}

fn record_of<'a>(out: &'a BTreeMap<String, Result<RunRecord, DueRunError>>, account_id: &str) -> &'a RunRecord {
    match out.get(account_id) {
        Some(Ok(r)) => r,
        other => panic!("{account_id}: expected a RunRecord, got {other:?}"),
    }
}

fn etf_decision(r: &RunRecord) -> &rebalancer_run::record::SleeveDecision {
    r.decisions.iter().find(|d| d.sleeve == "etf").expect("the ETF decision is in the record")
}

/// The planner's computed order set (symbol, side, quantity), from the FIRST plan of a Live run (before any
/// re-plan). Buys and sells only (never `Rehearsal`, which this pipeline mode never produces).
fn planned_set(r: &RunRecord) -> Vec<(String, broker_adapters::Side, broker_adapters::Dec)> {
    let mut v: Vec<_> = r.plan.as_ref().expect("a plan exists").orders.iter().map(|o| (o.symbol.clone(), o.side, o.quantity)).collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

/// What was actually carried out, in the same shape, from `r.placed` (every leg must be `carried()`: no silent
/// divergence between what the planner wanted and what happened at the venue).
fn placed_set(r: &RunRecord) -> Vec<(String, broker_adapters::Side, broker_adapters::Dec)> {
    assert!(r.placed.iter().all(|p| p.carried()), "every placed leg must be fully carried out: {:?}", r.placed.iter().map(|p| (&p.symbol, p.outcome)).collect::<Vec<_>>());
    let mut v: Vec<_> = r.placed.iter().map(|p| (p.symbol.clone(), p.side, p.planned_quantity)).collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

// ---------------------------------------------------------------------------------------------------------------
// T1
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn t1_direct_to_paper_orders_journey_entry_then_two_boundaries_tenant_b_never_touched() {
    let cal = Calendar::us(2017, 2021);
    let world = etf_world(&cal, date(2018, 1, 1), date(2020, 6, 30));
    let vendor_a = Vendor::new(world);

    let tenant_a = Tenant::new();
    // Tenant B gets its OWN fake Alpaca exchange, but deliberately no `AlpacaRig`/adapter is ever built for it: the
    // real pilot service never constructs a broker connection for a second tenant at all (`PILOT_TENANT_ID` /
    // `PILOT_ACCOUNT_ID` gate startup to one account), so building an unused adapter here would itself generate
    // request traffic (asset refresh) that has nothing to do with the property under test.
    let exchange_b = fake_broker::alpaca::FakeAlpaca::standard();
    let handle_b = exchange_b.handle();
    let assets = AssetTable::builtin();
    let opts = PrepareOptions { allow_extended_hours: false, min_notional: d("1"), own_tag_prefix: Some("rb1:".into()), refuse_builtin_assets: false, allow_crypto: false };
    let rules = AlpacaRules { assets: &assets, options: &opts };
    let book_a = book(&rules);
    let broker_a = tenant_a.broker();
    let sim = Sim::new(&broker_a, &book_a, tenant_a.rig.clock.clone());

    // ---- Entry: the account's first run (2019-09-05) acts on the August decision (an entry, per Ruling 9a).
    let entry = sim.tick(date(2019, 9, 5), &vendor_a);
    let a0 = record_of(&entry, ACCOUNT_A);
    assert_eq!((a0.outcome.kind, a0.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{:?}", a0.outcome);
    let dec0 = etf_decision(a0);
    assert!(dec0.entry && dec0.pending && dec0.planned && dec0.acted);
    assert_eq!(dec0.decision_date, date(2019, 8, 30));
    let entry_want = long_at_month_end(&etf_world(&cal, date(2018, 1, 1), date(2020, 6, 30)), date(2019, 8, 30));
    assert!(!entry_want.is_empty(), "fixture sanity: the entry decision holds something");
    assert_eq!(planned_set(a0).iter().map(|(s, ..)| s.clone()).collect::<Vec<_>>(), entry_want, "the entry buys exactly the August decision's longs");
    assert_eq!(placed_set(a0), planned_set(a0), "every planned leg on the entry was carried out exactly");
    assert_eq!(sim.runs.last_acted_decision(ACCOUNT_A, "etf").unwrap(), Some(date(2019, 8, 30)));

    // ---- Nothing more until the first boundary: drive the days in between, asserting no ETF touch and tenant B
    // untouched throughout.
    let mut boundary1_rec: Option<RunRecord> = None;
    for day in all_days(date(2019, 9, 6), date(2019, 10, 8)) {
        let out = sim.tick(day, &vendor_a);
        let a = record_of(&out, ACCOUNT_A);
        if day == date(2019, 10, 2) {
            // The council's expected action day: the first run after the first October bar acts on September.
            assert_eq!((a.outcome.kind, a.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{day}: {:?}", a.outcome);
            let dec = etf_decision(a);
            assert!(!dec.entry && dec.pending && dec.planned && dec.acted, "{day}: {dec:?}");
            assert_eq!(dec.decision_date, date(2019, 9, 30));
            boundary1_rec = Some(a.clone());
        } else {
            assert!(a.outcome.code == "RUN_NOTHING_PENDING" || !etf_decision(a).planned, "{day}: unexpected ETF touch: {:?}", a.outcome);
        }
        // Tenant B: always a genuinely due candidate (asserted inside `tick`), never given a runtime.
        assert_eq!(out.get(ACCOUNT_B), Some(&Err(DueRunError::NoRuntime)), "{day}: tenant B must never be attempted");
    }
    let b1 = boundary1_rec.expect("the first boundary happened on 2019-10-02");
    assert_eq!(planned_set(&b1), placed_set(&b1), "boundary 1: the order set placed equals exactly what the planner computed");
    assert_eq!(sim.runs.last_acted_decision(ACCOUNT_A, "etf").unwrap(), Some(date(2019, 9, 30)), "boundary 1 acted exactly once (D_acted advanced exactly to it)");

    // ---- Second boundary: the October decision becomes COMPUTABLE the day after the first November bar (Fri
    // 2019-11-01), i.e. Saturday 2019-11-02 -- and the driver evaluates every calendar day, not just sessions, so
    // it is PLANNED (but refused: MARKET_CLOSED) on both the Saturday and the Sunday, then finally ACTED on the
    // first open session, Monday 2019-11-04. This is exactly the plan's own "Nov 2019 Saturday action day" case
    // (`portfolio-parity/tests/t6_market_closed_retry.rs`, at the construction level; here it is the same calendar
    // fact exercised through the REAL Alpaca wire clock).
    let mut boundary2_rec: Option<RunRecord> = None;
    let mut market_closed_warnings = 0usize;
    let orders_before_boundary_2 = tenant_a.rig.handle.applied(HttpMethod::Post, "/v2/orders").len();
    for day in all_days(date(2019, 10, 9), date(2019, 11, 8)) {
        let out = sim.tick(day, &vendor_a);
        let a = record_of(&out, ACCOUNT_A);
        if day == date(2019, 11, 4) {
            assert_eq!((a.outcome.kind, a.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{day}: {:?}", a.outcome);
            let dec = etf_decision(a);
            assert!(dec.pending && dec.planned && dec.acted);
            assert_eq!(dec.decision_date, date(2019, 10, 31));
            boundary2_rec = Some(a.clone());
        } else if matches!(day, d if d == date(2019, 11, 2) || d == date(2019, 11, 3)) {
            // The weekend the decision is pending but the market is closed: planned, refused, NOT acted, Warning.
            assert_eq!((a.outcome.kind, a.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_DECISION_NOT_ACTED"), "{day}: {:?}", a.outcome);
            let dec = etf_decision(a);
            assert!(dec.pending && dec.planned && !dec.acted, "{day}: {dec:?}");
            assert_eq!(dec.decision_date, date(2019, 10, 31));
            assert!(a.placed.iter().all(|p| p.refused_market_closed()), "{day}: {:?}", a.placed);
            assert!(a.alerts.iter().any(|al| al.code.as_str() == "ALERT_DECISION_NOT_ACTED" && al.severity == rebalancer_run::record::AlertSeverity::Warning), "{day}: {:?}", a.alerts);
            market_closed_warnings += 1;
            assert_eq!(
                tenant_a.rig.handle.applied(HttpMethod::Post, "/v2/orders").len(),
                orders_before_boundary_2,
                "{day}: a closed-market refusal is caught by the adapter's own preflight and never reaches the exchange"
            );
        } else {
            assert!(a.outcome.code == "RUN_NOTHING_PENDING" || !etf_decision(a).planned, "{day}: unexpected ETF touch: {:?}", a.outcome);
        }
        assert_eq!(out.get(ACCOUNT_B), Some(&Err(DueRunError::NoRuntime)), "{day}: tenant B must never be attempted");
    }
    assert_eq!(market_closed_warnings, 2, "exactly the Saturday and Sunday were closed-market gaps");
    let b2 = boundary2_rec.expect("the second boundary was finally acted on 2019-11-04");
    assert_eq!(planned_set(&b2), placed_set(&b2), "boundary 2: the order set placed equals exactly what the planner computed");
    assert_eq!(sim.runs.last_acted_decision(ACCOUNT_A, "etf").unwrap(), Some(date(2019, 10, 31)), "boundary 2 acted exactly once, moving D_acted to it (never twice, never skipped)");

    // ---- Every order that reached tenant A's exchange carries the pilot's own tag prefix, and used the venue's
    // REAL client_order_id idempotency (no duplicate tag, no order lost).
    let orders_a = tenant_a.rig.handle.orders();
    assert!(!orders_a.is_empty());
    assert!(orders_a.iter().all(|o| o.client_order_id.as_deref().is_some_and(|c| c.starts_with("rb1:"))), "{orders_a:?}");
    let tags: Vec<&str> = orders_a.iter().filter_map(|o| o.client_order_id.as_deref()).collect();
    let unique: std::collections::BTreeSet<&str> = tags.iter().copied().collect();
    assert_eq!(tags.len(), unique.len(), "no duplicate client_order_id was ever produced");

    // ---- Tenant B's own exchange: literally zero requests over the whole journey (entry + two boundaries + every
    // day in between). This is the real tenant-isolation assertion: B was due, eligible-looking, and never touched.
    assert!(handle_b.requests().is_empty(), "tenant B's exchange saw {} requests: {:#?}", handle_b.requests().len(), handle_b.requests());
    assert!(handle_b.orders().is_empty(), "no order was ever placed for tenant B");
    assert_eq!(sim.runs.last_acted_decision(ACCOUNT_B, "etf").unwrap(), None, "tenant B's decision ledger never advanced");
    assert!(sim.runs.summaries(ACCOUNT_B).unwrap().is_empty(), "no run record was ever stored for tenant B");

    // ---- Post-run reconciliation stayed clean throughout (no drift halt from the pilot's own trading).
    assert!(a0.recon.iter().chain(b1.recon.iter()).chain(b2.recon.iter()).all(|s| s.report.verdict == rebalancer_run::recon::ReconVerdict::Ok), "{:?} {:?} {:?}", a0.recon, b1.recon, b2.recon);
}
