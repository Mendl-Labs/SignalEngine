//! Finding U3 (live timing of the ETF sleeve): CHARACTERISATION tests of WHEN the ETF trend sleeve is due
//! (`driver::find_due_runs` / `run_all_due`, i.e. `sleeve_due_on` + `slot_for`, DAILY_RUN 00:10Z) versus WHICH
//! month-end decision the pipeline (`step_targets`: `latest_decision_date` + `decide_etf_trend` with
//! `Options::etf_live(as_of)`, `MonthEndMode::NextMonthBar`) computes on that slot.
//!
//! CHARACTERISATION: documents current behaviour, see finding U3; not a statement that it is correct.
//! Every assertion below pins what the code does TODAY. If someone changes the driver's due rule, the pipeline's
//! decision-date choice or the reference rule's month-end mode, these tests fail loudly on purpose: the change is
//! then a deliberate timing change and must update this file together with the sleeve's published timing
//! (decide at the last close of the month, earn from the next bar).
//!
//! # The data-provider assumption (there is no real provider in this repository)
//! `DataSource` (crates/rebalancer-run/src/data.rs) has exactly one implementation in this repo:
//! `testkit::FixtureData`, which ignores `as_of` and returns whatever panel it was given. The real data gate
//! (WP2.3) is only described in `data.rs`'s doc comment ("last complete bar, no forming bar, retry on 429, bypass
//! caches"). These tests therefore ASSUME a provider that returns exactly the bars that exist at 00:10Z of the run
//! date: every session up to and including the previous UTC day's close, and NO bar dated on or after `as_of`
//! (this is also what the reference tool does: `ticket.py::load_api` drops `df.index.date >= today`). See
//! `ClosedBarsOnly` below. A provider that also returned a partial/forming bar for `as_of` would be refused by the
//! rule anyway (`FormingBar`, bar_date >= as_of), so the assumption is the only one under which a run can succeed.
//!
//! Calendar used: weekday-only ETF bars (no holidays), crypto bars every day. Dates chosen so that the last
//! CALENDAR day of the month is a weekday (2019-09-30 Mon, 2020-01-31 Fri) or a weekend (2020-02-29 Sat).

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::{AlpacaAdapter, AlpacaConfig, AlpacaCredentials, AssetTable, Environment, PrepareOptions};
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::HttpResponse;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use common::*;
use rebalancer_core::guard::PricePoint;
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_core::venue::{AlpacaRules, VenueRuleBook};
use rebalancer_risk::store::InMemoryStateStore;
use rebalancer_run::broker::{AlpacaBroker, Broker};
use rebalancer_run::clock::ManualClock;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveKind, SleeveSpec};
use rebalancer_run::driver::{find_due_runs, run_all_due, AccountRuntime, ActiveAccount, InMemoryAccountSource};
use rebalancer_run::pipeline::{run_once, RunConfig, RunContext};
use rebalancer_run::record::{ExecutionMode, OutcomeKind, RunRecord};
use rebalancer_run::stores::InMemoryRunStore;
use rebalancer_run::testkit::{InMemoryAccountLock, RecordingNotifier, SwitchKillFlag};
use reference_rules::{decide_etf_trend, MonthEndMode, Options, Panel, PriceSeries, ETF_SYMBOLS};
use serde_json::{json, Value};

const ACCOUNT_FIXTURE: &str = include_str!("../../broker-adapters/tests/fixtures/alpaca/account_ok.json");

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn slot(day: NaiveDate) -> DateTime<Utc> {
    // DAILY_RUN_HOUR_UTC:DAILY_RUN_MINUTE_UTC = 00:10Z, the driver's fixed daily slot.
    at(&format!("{day}T00:10:00Z"))
}

// ---------------------------------------------------------------------------------------------------------------
// Synthetic world: what the market printed. The provider below decides what a 00:10Z run may see of it.
// ---------------------------------------------------------------------------------------------------------------

const WORLD_START: (i32, u32, u32) = (2018, 1, 1);
const WORLD_END: (i32, u32, u32) = (2020, 4, 30);

fn all_days(from: NaiveDate, to: NaiveDate) -> Vec<NaiveDate> {
    let mut v = Vec::new();
    let mut d = from;
    while d <= to {
        v.push(d);
        d += Duration::days(1);
    }
    v
}

fn month_index(d: NaiveDate) -> f64 {
    ((d.year() - 2018) * 12 + d.month0() as i32) as f64
}

/// Slow drift plus a ~7-month cycle per ETF (different phases), so the month-end trend signal flips from month to
/// month and the decisions of adjacent months differ. Intra-month wiggle is tiny relative to the monthly cycle.
fn etf_close(i: usize, d: NaiveDate) -> f64 {
    let phase = [0.0, 1.7, 3.1, 4.6, 5.9][i] + 0.5 * i as f64;
    let base = [250.0, 60.0, 105.0, 16.0, 90.0][i];
    let m = month_index(d);
    let cyc = (2.0 * std::f64::consts::PI * (m + phase) / 7.0).sin();
    let day = d.day() as f64;
    base * (1.0 + 0.10 * cyc + 0.004 * m) + 0.002 * base * (day / 31.0)
}

fn etf_world() -> Panel {
    let days: Vec<NaiveDate> = all_days(date(WORLD_START.0, WORLD_START.1, WORLD_START.2), date(WORLD_END.0, WORLD_END.1, WORLD_END.2))
        .into_iter()
        .filter(|d| !matches!(d.weekday(), Weekday::Sat | Weekday::Sun))
        .collect();
    Panel::new(
        ETF_SYMBOLS
            .iter()
            .enumerate()
            .map(|(i, s)| PriceSeries::new(*s, days.clone(), days.iter().map(|d| etf_close(i, *d)).collect()).unwrap())
            .collect(),
    )
    .unwrap()
}

fn crypto_close(i: usize, d: NaiveDate) -> f64 {
    let base = [9000.0, 300.0][i];
    let k = (d - date(2018, 1, 1)).num_days() as f64;
    base * (1.0 + 0.25 * (2.0 * std::f64::consts::PI * (k + 40.0 * i as f64) / 210.0).sin() + 0.0005 * k)
}

fn crypto_world() -> Panel {
    let days = all_days(date(WORLD_START.0, WORLD_START.1, WORLD_START.2), date(WORLD_END.0, WORLD_END.1, WORLD_END.2));
    Panel::new(
        ["BTC", "ETH"]
            .iter()
            .enumerate()
            .map(|(i, s)| PriceSeries::new(*s, days.clone(), days.iter().map(|d| crypto_close(i, *d)).collect()).unwrap())
            .collect(),
    )
    .unwrap()
}

/// ASSUMED live provider: at 00:10Z of `as_of` only bars dated STRICTLY BEFORE `as_of` exist (the previous
/// session's close and earlier); the bar of `as_of` itself (still forming or not yet started) is never returned.
/// Records the newest bar it handed out per sleeve so the tests can report it.
struct ClosedBarsOnly {
    etf: Panel,
    crypto: Panel,
    newest: Mutex<BTreeMap<String, NaiveDate>>,
}

impl ClosedBarsOnly {
    fn new() -> Self {
        Self { etf: etf_world(), crypto: crypto_world(), newest: Mutex::new(BTreeMap::new()) }
    }

    fn newest_bar(&self, sleeve: &str) -> Option<NaiveDate> {
        self.newest.lock().unwrap().get(sleeve).copied()
    }
}

impl DataSource for ClosedBarsOnly {
    fn sleeve_data(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError> {
        let world = match sleeve.kind {
            SleeveKind::EtfTrend => &self.etf,
            SleeveKind::CryptoTrend => &self.crypto,
        };
        let panel = world.truncated_to(as_of.pred_opt().unwrap());
        let first = panel.iter().next().ok_or_else(|| DataError::new("DATA_UNAVAILABLE", "empty panel"))?;
        self.newest.lock().unwrap().insert(sleeve.id.clone(), first.last_date());
        Ok(SleeveData { panel })
    }

    fn prices(&self, symbols: &[String], now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError> {
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
// Account plumbing: an Alpaca (ETF) account in Assisted mode, exactly as tests/etf_assisted.rs builds it
// ---------------------------------------------------------------------------------------------------------------

fn etf_sleeve(share: &str) -> SleeveSpec {
    SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: d(share), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "USD".into() }
}

fn crypto_sleeve_on_alpaca(share: &str) -> SleeveSpec {
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
    MandateEnvelope { version: 1, status: MandateStatus::Active, effective_from: at("2018-01-01T00:00:00Z"), review_by: at("2031-01-01T00:00:00Z") }
}

fn account(sleeves: Vec<SleeveSpec>) -> ActiveAccount {
    ActiveAccount {
        account_id: "alpaca-acct".into(),
        tenant_id: "tenant".into(),
        mandate: mandate(),
        envelope: envelope(),
        plan_approved: true,
        sleeves,
        mode: ExecutionMode::Assisted,
    }
}

/// Run `f` with an Alpaca paper broker holding 5000 of cash (read-only fixture transport) and its venue rules.
fn with_alpaca<R>(f: impl FnOnce(&dyn Broker, &VenueRuleBook<'_>) -> R) -> R {
    let acct = ACCOUNT_FIXTURE
        .replace("\"cash\": \"52840.17\"", "\"cash\": \"5000\"")
        .replace("\"equity\": \"100210.55\"", "\"equity\": \"5000\"")
        .replace("\"portfolio_value\": \"100210.55\"", "\"portfolio_value\": \"5000\"");
    let transport = Arc::new(FakeTransport::new());
    transport.set_handler(move |req| {
        let body = if req.url.contains("/v2/account") { acct.clone() } else { "[]".to_string() };
        Ok(HttpResponse { status: 200, body })
    });
    let cfg = AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).unwrap();
    let creds = AlpacaCredentials::new(Environment::Paper, "PKTESTFIXTUREKEY0001", "unit-test-secret-not-a-real-key-9f3a").unwrap();
    let adapter = AlpacaAdapter::new(cfg, creds, transport).unwrap();
    let broker = AlpacaBroker::us_etf(&adapter);
    let assets = AssetTable::builtin();
    let opts = PrepareOptions { allow_extended_hours: false, min_notional: d("1"), own_tag_prefix: Some("rb1:".into()), refuse_builtin_assets: false };
    let rules = AlpacaRules { assets: &assets, options: &opts };
    let book = VenueRuleBook::new().with("alpaca", &rules);
    f(&broker, &book)
}

/// What a run on one daily slot looked like, in the terms the finding is about.
#[derive(Debug, Clone, PartialEq)]
struct Seen {
    run_date: NaiveDate,
    /// Does `find_due_runs` (the real driver) consider this account due on this slot?
    due: bool,
    /// Newest bar handed to the ETF rule (None if the account has no ETF sleeve).
    etf_newest_bar: Option<NaiveDate>,
    /// `decision_date` the pipeline recorded for the ETF sleeve (None if the run stopped before recording it).
    etf_decision_date: Option<NaiveDate>,
    kind: OutcomeKind,
    code: String,
    /// ETF symbols with a positive target weight, in symbol order.
    etf_long: Vec<String>,
    /// Crypto `decision_date` (crypto is decided on yesterday's bar), if the account has a crypto sleeve.
    crypto_decision_date: Option<NaiveDate>,
}

/// Run the account on the slot of `run_date`. If the driver considers it due, the run goes through the real
/// `find_due_runs` + `run_all_due`; if not, the same `run_once` call is made by hand ("forced"), to show what the
/// pipeline WOULD decide on a day the driver never runs.
fn run_on(sleeves: &[SleeveSpec], run_date: NaiveDate) -> Seen {
    let now = slot(run_date);
    let source = InMemoryAccountSource::new().with_account(account(sleeves.to_vec()));
    let due = find_due_runs(&source, now).expect("enumeration must not fail");
    let data = ClosedBarsOnly::new();
    let (runs, states, notifier, kill, cfg, clock) = (InMemoryRunStore::new(), InMemoryStateStore::new(), RecordingNotifier::new(), SwitchKillFlag::new(), RunConfig::default(), ManualClock::new(now));
    let rec: RunRecord = with_alpaca(|broker, book| {
        if let Some(spec) = due.first() {
            assert_eq!(due.len(), 1);
            assert_eq!(spec.scheduled_for, now, "the driver schedules the run at 00:10Z of the due date");
            let mut runtimes = BTreeMap::new();
            runtimes.insert("alpaca-acct".to_string(), AccountRuntime { broker, data: &data, venue_rules: book });
            let lock = InMemoryAccountLock::new();
            let mut out = run_all_due(due.clone(), &runtimes, &states, &runs, &notifier, &kill, &clock, &lock, &cfg);
            out.remove(0).result.expect("run_once was reached")
        } else {
            let (m, e) = (mandate(), envelope());
            let ctx = RunContext {
                account_id: "alpaca-acct",
                scheduled_for: now,
                trading_day: run_date,
                mode: ExecutionMode::Assisted,
                sleeves,
                mandate: Some(&m),
                envelope: Some(&e),
                broker,
                data: &data,
                state_store: &states,
                clock: &clock,
                runs: &runs,
                notifier: &notifier,
                kill_flag: &kill,
                venue_rules: book,
                config: &cfg,
            };
            run_once(&ctx)
        }
    });
    let target = |id: &str| rec.targets.iter().find(|t| t.sleeve == id);
    Seen {
        run_date,
        due: !due.is_empty(),
        etf_newest_bar: data.newest_bar("etf"),
        etf_decision_date: target("etf").map(|t| t.decision_date),
        kind: rec.outcome.kind,
        code: rec.outcome.code.clone(),
        etf_long: target("etf").map(|t| t.weights.iter().filter(|(_, w)| w.is_positive()).map(|(s, _)| s.clone()).collect()).unwrap_or_default(),
        crypto_decision_date: target("crypto").map(|t| t.decision_date),
    }
}

/// The rule's decision at month-end `me` computed on the FULL world with the month-end date asserted (Explicit):
/// what "decide at the last close of the month" means. Independent of the pipeline and of the driver.
fn long_at_month_end(me: NaiveDate) -> Vec<String> {
    let world = etf_world();
    let d = decide_etf_trend(&world, me, &Options::etf_replay(MonthEndMode::Explicit)).unwrap();
    d.instruments.iter().filter(|i| i.weight > 0.0).map(|i| i.symbol.clone()).collect()
}

// ---------------------------------------------------------------------------------------------------------------
// The synthetic data must be discriminating: adjacent month-end decisions differ, or a one-month lag is invisible
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn precondition_adjacent_month_end_decisions_differ_so_a_lag_is_visible() {
    // (month-end that SHOULD be acted on, the previous month-end that the driver's run actually decides on)
    for (correct, previous) in [(date(2019, 9, 30), date(2019, 8, 30)), (date(2020, 1, 31), date(2019, 12, 31)), (date(2020, 2, 28), date(2020, 1, 31))] {
        assert_ne!(long_at_month_end(correct), long_at_month_end(previous), "fixture must distinguish {correct} from {previous}");
    }
}

// ---------------------------------------------------------------------------------------------------------------
// (a) ETF-only account, run on the calendar month-end slot
// ---------------------------------------------------------------------------------------------------------------

// CHARACTERISATION: documents current behaviour, see finding U3; not a statement that it is correct.
#[test]
fn u3_a_etf_only_on_the_calendar_month_end_slot_the_run_succeeds_but_decides_the_previous_month() {
    let sleeves = [etf_sleeve("1")];

    // 2019-09-30 is a Monday. At 00:10Z the newest bar is Friday 09-27 (the 30th's session has not happened and
    // no October bar exists), so the newest COMPLETED month is August: decision_date = 2019-08-30, NOT 09-30.
    let sep = run_on(&sleeves, date(2019, 9, 30));
    assert_eq!(
        sep,
        Seen {
            run_date: date(2019, 9, 30),
            due: true,
            etf_newest_bar: Some(date(2019, 9, 27)),
            etf_decision_date: Some(date(2019, 8, 30)),
            kind: OutcomeKind::Completed,
            code: "RUN_COMPLETED".into(),
            etf_long: long_at_month_end(date(2019, 8, 30)),
            crypto_decision_date: None,
        }
    );

    // 2020-01-31 is a Friday: newest bar Thursday 01-30, decision_date = 2019-12-31 (December), not 01-31.
    let jan = run_on(&sleeves, date(2020, 1, 31));
    assert_eq!(
        jan,
        Seen {
            run_date: date(2020, 1, 31),
            due: true,
            etf_newest_bar: Some(date(2020, 1, 30)),
            etf_decision_date: Some(date(2019, 12, 31)),
            kind: OutcomeKind::Completed,
            code: "RUN_COMPLETED".into(),
            etf_long: long_at_month_end(date(2019, 12, 31)),
            crypto_decision_date: None,
        }
    );

    // 2020-02-29 is a Saturday: newest bar Friday 02-28 (the true last session of February, already printed),
    // yet still decision_date = 2020-01-31, because no March bar exists (NextMonthBar).
    let feb = run_on(&sleeves, date(2020, 2, 29));
    assert_eq!(
        feb,
        Seen {
            run_date: date(2020, 2, 29),
            due: true,
            etf_newest_bar: Some(date(2020, 2, 28)),
            etf_decision_date: Some(date(2020, 1, 31)),
            kind: OutcomeKind::Completed,
            code: "RUN_COMPLETED".into(),
            etf_long: long_at_month_end(date(2020, 1, 31)),
            crypto_decision_date: None,
        }
    );

    // The decision each run DID act on differs from the month-end decision the rule publishes for that date.
    assert_ne!(sep.etf_long, long_at_month_end(date(2019, 9, 30)));
    assert_ne!(jan.etf_long, long_at_month_end(date(2020, 1, 31)));
    assert_ne!(feb.etf_long, long_at_month_end(date(2020, 2, 28)));
}

// ---------------------------------------------------------------------------------------------------------------
// (b) ETF-only account, run on the first day of the next month (and the days after it)
// ---------------------------------------------------------------------------------------------------------------

// CHARACTERISATION: documents current behaviour, see finding U3; not a statement that it is correct.
#[test]
fn u3_b_etf_only_the_driver_never_runs_the_day_after_month_end_and_even_a_forced_run_there_is_still_a_month_behind() {
    let sleeves = [etf_sleeve("1")];

    // 2019-10-01 (Tue): the September 30 session has printed, but no October bar exists yet.
    let oct1 = run_on(&sleeves, date(2019, 10, 1));
    assert!(!oct1.due, "the driver does not consider an ETF-only account due on the 1st");
    assert_eq!(oct1.etf_newest_bar, Some(date(2019, 9, 30)));
    assert_eq!(oct1.etf_decision_date, Some(date(2019, 8, 30)), "forced: the newest bar is the month's last, but NextMonthBar still refuses to call September complete");
    assert_eq!((oct1.kind, oct1.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"));
    assert_eq!(oct1.etf_long, long_at_month_end(date(2019, 8, 30)));

    // 2019-10-02 (Wed): the first October bar (Oct 1) exists; ONLY now does the pipeline compute September's decision.
    let oct2 = run_on(&sleeves, date(2019, 10, 2));
    assert!(!oct2.due, "the driver does not consider an ETF-only account due on the 2nd either");
    assert_eq!(oct2.etf_newest_bar, Some(date(2019, 10, 1)));
    assert_eq!(oct2.etf_decision_date, Some(date(2019, 9, 30)));
    assert_eq!(oct2.etf_long, long_at_month_end(date(2019, 9, 30)));

    // Same shape at a weekend month-end: 2020-03-01 (Sun) newest bar Fri 02-28, decision still January's;
    // 2020-03-02 (Mon) newest bar is still Fri 02-28 (no March bar until the 2nd's session has printed).
    let mar1 = run_on(&sleeves, date(2020, 3, 1));
    assert!(!mar1.due);
    assert_eq!((mar1.etf_newest_bar, mar1.etf_decision_date), (Some(date(2020, 2, 28)), Some(date(2020, 1, 31))));
    let mar2 = run_on(&sleeves, date(2020, 3, 2));
    assert!(!mar2.due);
    assert_eq!((mar2.etf_newest_bar, mar2.etf_decision_date), (Some(date(2020, 2, 28)), Some(date(2020, 1, 31))));
    let mar3 = run_on(&sleeves, date(2020, 3, 3));
    assert!(!mar3.due);
    assert_eq!((mar3.etf_newest_bar, mar3.etf_decision_date), (Some(date(2020, 3, 2)), Some(date(2020, 2, 28))));
    assert_eq!(mar3.etf_long, long_at_month_end(date(2020, 2, 28)));

    // What the driver actually does next for an ETF-only account after the 2020-02-29 run: nothing until
    // 2020-03-31, whose 00:10Z panel ends 03-30 and yields February's decision (a month after Feb 28's close).
    let mar31 = run_on(&sleeves, date(2020, 3, 31));
    assert!(mar31.due);
    assert_eq!((mar31.etf_newest_bar, mar31.etf_decision_date), (Some(date(2020, 3, 30)), Some(date(2020, 2, 28))));
    assert_eq!((mar31.kind, mar31.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"));
    assert_eq!(mar31.etf_long, long_at_month_end(date(2020, 2, 28)));
}

// CHARACTERISATION: documents current behaviour, see finding U3; not a statement that it is correct.
#[test]
fn u3_b_etf_only_due_slots_are_exactly_the_calendar_month_ends_and_nothing_else() {
    let acct = account(vec![etf_sleeve("1")]);
    let source = InMemoryAccountSource::new().with_account(acct);
    let mut due_days = Vec::new();
    for day in all_days(date(2019, 9, 1), date(2020, 3, 31)) {
        let due = find_due_runs(&source, slot(day)).unwrap();
        if !due.is_empty() {
            assert_eq!(due[0].scheduled_for, slot(day));
            assert_eq!(due[0].trading_day, day);
            due_days.push(day);
        }
    }
    assert_eq!(
        due_days,
        vec![date(2019, 9, 30), date(2019, 10, 31), date(2019, 11, 30), date(2019, 12, 31), date(2020, 1, 31), date(2020, 2, 29), date(2020, 3, 31)]
    );
    // Includes weekend month-ends (2019-11-30 Sat, 2020-02-29 Sat): due, but no new bar can exist for them.
    assert_eq!(due_days[2].weekday(), Weekday::Sat);
}

// ---------------------------------------------------------------------------------------------------------------
// (c) account with ETF + crypto sleeves, driver runs it every day
// ---------------------------------------------------------------------------------------------------------------

// CHARACTERISATION: documents current behaviour, see finding U3; not a statement that it is correct.
#[test]
fn u3_c_etf_plus_crypto_account_is_due_daily_and_the_etf_decision_arrives_on_the_second_day_of_the_month() {
    let sleeves = [etf_sleeve("0.5"), crypto_sleeve_on_alpaca("0.5")];

    let mut seen = Vec::new();
    for day in all_days(date(2019, 9, 28), date(2019, 10, 3)) {
        seen.push(run_on(&sleeves, day));
    }
    // Due on every slot (the crypto sleeve is daily).
    assert!(seen.iter().all(|x| x.due));
    // ETF decision_date per run day, and the newest ETF bar the rule saw (weekend days keep Friday's bar).
    let got: Vec<(NaiveDate, Option<NaiveDate>, Option<NaiveDate>)> = seen.iter().map(|x| (x.run_date, x.etf_newest_bar, x.etf_decision_date)).collect();
    assert_eq!(
        got,
        vec![
            (date(2019, 9, 28), Some(date(2019, 9, 27)), Some(date(2019, 8, 30))), // Sat
            (date(2019, 9, 29), Some(date(2019, 9, 27)), Some(date(2019, 8, 30))), // Sun
            (date(2019, 9, 30), Some(date(2019, 9, 27)), Some(date(2019, 8, 30))), // Mon: last calendar day, still August
            (date(2019, 10, 1), Some(date(2019, 9, 30)), Some(date(2019, 8, 30))), // Tue: September's last bar printed, still August
            (date(2019, 10, 2), Some(date(2019, 10, 1)), Some(date(2019, 9, 30))), // Wed: first run that decides September
            (date(2019, 10, 3), Some(date(2019, 10, 2)), Some(date(2019, 9, 30))),
        ]
    );
    // Crypto is decided every day on yesterday's bar (its own rule; unaffected by month boundaries).
    for x in &seen {
        assert_eq!(x.crypto_decision_date, Some(x.run_date.pred_opt().unwrap()));
    }
    // The ETF sleeve's targets follow the same one-month-behind rule until the 2nd of the month.
    assert_eq!(seen[2].etf_long, long_at_month_end(date(2019, 8, 30)));
    assert_eq!(seen[3].etf_long, long_at_month_end(date(2019, 8, 30)));
    assert_eq!(seen[4].etf_long, long_at_month_end(date(2019, 9, 30)));
    assert_ne!(seen[3].etf_long, seen[4].etf_long, "the target flips only on the 2nd of the month");
    // The runs complete: nothing in this path fails closed; the lag is silent.
    assert!(seen.iter().all(|x| x.kind == OutcomeKind::Completed && x.code == "RUN_COMPLETED"), "{seen:#?}");

    // Weekend month-end: 2020-02-29 (Sat). The decision for February (bar Fri 02-28) is reached on the first run
    // after a March bar exists: Mar 3 00:10Z (bar Mon 03-02); Mar 1 and Mar 2 still carry January's decision.
    let feb: Vec<(NaiveDate, Option<NaiveDate>)> = all_days(date(2020, 2, 28), date(2020, 3, 3)).into_iter().map(|day| {
        let x = run_on(&sleeves, day);
        assert!(x.due);
        (day, x.etf_decision_date)
    }).collect();
    assert_eq!(
        feb,
        vec![
            (date(2020, 2, 28), Some(date(2020, 1, 31))), // newest bar 02-27: February not complete, January's decision
            (date(2020, 2, 29), Some(date(2020, 1, 31))),
            (date(2020, 3, 1), Some(date(2020, 1, 31))),
            (date(2020, 3, 2), Some(date(2020, 1, 31))),
            (date(2020, 3, 3), Some(date(2020, 2, 28))),
        ]
    );
}
