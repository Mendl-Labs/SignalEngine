//! R41 MOMENTUM cells through the pipeline: a `SleeveKind::EtfMomentum` sleeve whose `instruments` are a
//! pre-registered universe of 9 or 11 ETFs plans exactly the linear-rank weights `(K + 1 - r) / (K (K + 1) / 2)`
//! (R41-A(2)) against the fake Alpaca exchange and the REAL adapter (asset table refreshed per instrument,
//! `refuse_builtin_assets` ON, as the service runs). The decision clock is the completed daily bar (Amendment 14,
//! `Daily` schedule / `OnDecision` rebalance): a run that sees no new bar plans nothing.

mod common;

use std::collections::BTreeMap;

use broker_adapters::alpaca::{AssetTable, PrepareOptions};
use chrono::{Datelike, NaiveDate, Weekday};
use common::*;
use fake_broker::alpaca::{AssetSpec, FakeAlpaca};
use fake_broker::alpaca_rig::AlpacaRig;
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_core::venue::{AlpacaRules, VenueRuleBook};
use rebalancer_risk::store::InMemoryStateStore;
use rebalancer_run::broker::AlpacaBroker;
use rebalancer_run::clock::ManualClock;
use rebalancer_run::data::{Cadence, SleeveKind, SleeveSpec};
use rebalancer_run::pipeline::{run_once, RunConfig, RunContext};
use rebalancer_run::record::{ExecutionMode, OutcomeKind, RunRecord};
use rebalancer_run::stores::InMemoryRunStore;
use rebalancer_run::testkit::{FixtureData, RecordingNotifier, SwitchKillFlag};
use reference_rules::{Panel, PriceSeries};
use serde_json::{json, Value};

const SEC9: [&str; 9] = ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"];
const AC11: [&str; 11] = ["SPY", "EFA", "IEF", "DBC", "VNQ", "IWM", "EEM", "TLT", "LQD", "HYG", "GLD"];
const SLEEVE_ID: &str = "etf_cross_sectional_momentum";
const LOOKBACK: usize = 126;

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

/// The trailing-126-bar return level of instrument `i` of `n`: a permutation (7 is coprime to 9 and 11), so every
/// return is distinct and the expected ranks do not depend on the tie-break. 0 = the lowest return.
fn level(i: usize, n: usize) -> usize {
    (i * 7) % n
}

/// Weekday bars from 2018-01-01 to `end`. Instrument `i` closes at exactly 100 on the last bar and
/// `100 / (1 + r_i)` 126 bars earlier, where `r_i = 0.02 * (level - 3)` (some negative: a rank weight is still
/// positive), on a geometric path between (so the series is smooth and positive).
fn panel(tickers: &[&str], end: NaiveDate) -> Panel {
    let mut days = Vec::new();
    let mut d = date(2018, 1, 1);
    while d <= end {
        if !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) {
            days.push(d);
        }
        d = d.succ_opt().unwrap();
    }
    let k = days.len();
    let n = tickers.len();
    let series = tickers
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let r = 0.02 * (level(i, n) as f64 - 3.0);
            let g = (1.0 + r).powf(-1.0 / LOOKBACK as f64);
            let closes: Vec<f64> = (0..k).map(|j| 100.0 * g.powf((k - 1 - j) as f64)).collect();
            PriceSeries::new(*t, days.clone(), closes).unwrap()
        })
        .collect();
    Panel::new(series).unwrap()
}

fn mandate(tickers: &[&str]) -> mandate_core::mandate::MandateBody {
    let mut v: Value = serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap();
    v["autonomy"]["level"] = json!("L2");
    v["universe"]["venues"] = json!(["alpaca"]);
    v["universe"]["asset_classes"] = json!(["us_etf"]);
    v["universe"]["instrument_allow"] = json!(tickers);
    v["exposure"]["max_asset_class"] = json!({"us_etf": 1.0});
    v["exposure"]["max_position"] = json!(0.25);
    v["exposure"]["max_turnover_per_day"] = json!(2.0);
    v["exposure"]["max_order_notional"] = json!({"amount": "1500.00", "ccy": "USD"});
    v["capital"]["allocated"]["amount"] = json!("5000.00");
    serde_json::from_value(v).unwrap()
}

fn momentum_sleeve(tickers: &[&str]) -> SleeveSpec {
    SleeveSpec {
        id: SLEEVE_ID.into(),
        kind: SleeveKind::EtfMomentum,
        share: d("1"),
        venue: "alpaca".into(),
        asset_class: "us_etf".into(),
        quote: String::new(),
        instruments: tickers.iter().map(|s| s.to_string()).collect(),
        tranche_sessions: 0,
        lookback: None,
    }
}

/// A fake exchange that lists exactly the cell's instruments, $5,000 cash, a flat $100 mark.
fn rig_for(tickers: &[&str]) -> AlpacaRig {
    let mut builder = FakeAlpaca::builder().cash("5000");
    for t in tickers {
        builder = builder.asset(AssetSpec::etf(t), "100");
    }
    AlpacaRig::with_fake(builder.build(), |mut c| {
        c.own_tag_prefix = Some("rb1:".to_string());
        c.refuse_builtin_assets = true;
        c
    })
}

struct Stores {
    runs: InMemoryRunStore,
    states: InMemoryStateStore,
}

impl Stores {
    fn new() -> Self {
        Self { runs: InMemoryRunStore::new(), states: InMemoryStateStore::new() }
    }
}

/// One scheduled run at `now` on a panel whose newest bar is `bar_end`.
fn run_at(rig: &AlpacaRig, stores: &Stores, tickers: &[&str], bar_end: NaiveDate, now: &str) -> RunRecord {
    let mut assets = AssetTable::empty();
    for t in tickers {
        assets.upsert(rig.adapter.refresh_asset(t).expect("the fake lists the instrument"));
    }
    let opts = PrepareOptions { allow_extended_hours: false, min_notional: d("1"), own_tag_prefix: Some("rb1:".into()), refuse_builtin_assets: true, allow_crypto: false };
    let rules = AlpacaRules { assets: &assets, options: &opts };
    let book = VenueRuleBook::new().with("alpaca", &rules);
    let broker = AlpacaBroker::us_etf(&rig.adapter);

    let now = at(now);
    rig.clock.set_nanos(now.timestamp_nanos_opt().expect("in range") as u64);
    let data = FixtureData::new().with_panel(SLEEVE_ID, panel(tickers, bar_end)).with_price_fn(|_| Some(d("100.00")));
    let sleeves = [momentum_sleeve(tickers)];
    let envelope = MandateEnvelope { version: 1, status: MandateStatus::Active, effective_from: at("2019-01-01T00:00:00Z"), review_by: at("2099-01-01T00:00:00Z") };
    let mandate = mandate(tickers);
    let clock = ManualClock::new(now);
    let (notifier, kill, cfg) = (RecordingNotifier::new(), SwitchKillFlag::new(), RunConfig::default());
    let ctx = RunContext {
        account_id: "momentum-acct",
        scheduled_for: now,
        trading_day: now.date_naive(),
        mode: ExecutionMode::Live,
        sleeves: &sleeves,
        mandate: Some(&mandate),
        envelope: Some(&envelope),
        broker: &broker,
        data: &data,
        state_store: &stores.states,
        clock: &clock,
        runs: &stores.runs,
        notifier: &notifier,
        kill_flag: &kill,
        venue_rules: &book,
        config: &cfg,
        cache: None,
    };
    run_once(&ctx)
}

/// The linear-rank weight of instrument `i` of `n` (all `n` ranked): level 0 is the lowest return = rank `n`.
fn expected_weight(i: usize, n: usize) -> f64 {
    let denom = (n * (n + 1) / 2) as f64;
    (level(i, n) + 1) as f64 / denom
}

fn check_linear_rank_targets(tickers: &[&str]) {
    let n = tickers.len();
    let rig = rig_for(tickers);
    let r = run_at(&rig, &Stores::new(), tickers, date(2019, 9, 4), "2019-09-05T15:00:00Z");
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "N = {n}: {:?}", r.outcome);

    // The decision: the newest completed bar, an entry (never acted before), OnDecision cadence.
    let dec = &r.decisions[0];
    assert_eq!((dec.kind, dec.cadence, dec.decision_date, dec.pending, dec.entry), (SleeveKind::EtfMomentum, Cadence::OnDecision, date(2019, 9, 4), true, true));

    // The target: every instrument is ranked and holds its rank weight (rounded down once, as every weight is).
    assert_eq!(r.targets.len(), 1);
    let weights: BTreeMap<&str, broker_adapters::Dec> = r.targets[0].weights.iter().map(|(s, w)| (s.as_str(), *w)).collect();
    assert_eq!(weights.len(), n, "the target names every instrument of the universe");
    let mut total = 0.0;
    let mut target_sum = 0.0;
    for (i, t) in tickers.iter().enumerate() {
        let want = expected_weight(i, n);
        // The target carries the rule's weight floored to 12 decimals (so the sleeve's sum can never exceed 1).
        let got = weights[t].to_f64();
        assert!(got <= want && want - got < 2e-12, "N = {n}: {t} (level {}): target {got}, rule {want}", level(i, n));
        target_sum += got;
        assert_eq!(dec.instruments[i].weight.to_bits(), want.to_bits(), "the record carries the rule's own f64 weight for {t}");
        total += want;
    }
    assert!((total - 1.0).abs() < 1e-12, "N = {n}: gross is 100% by construction, got {total}");
    assert!(target_sum <= 1.0 && target_sum > 1.0 - 1e-10, "N = {n}: the target never exceeds 100%, got {target_sum}");
    // Concentration, disclosed (R41-A(2)): the top weight is 2 / (K + 1).
    let top = dec.instruments.iter().map(|i| i.weight).fold(0.0, f64::max);
    assert!((top - 2.0 / (n as f64 + 1.0)).abs() < 1e-12);

    // The plan: one buy per instrument, sized by its weight, carried out on the fake exchange.
    let plan = r.plan.as_ref().expect("a plan exists");
    assert_eq!(plan.orders.len(), n);
    for o in &plan.orders {
        let i = tickers.iter().position(|t| *t == o.symbol).expect("a universe symbol");
        let notional = o.quantity.to_f64() * 100.0;
        let per_name = 5000.0 * expected_weight(i, n);
        assert!(notional <= per_name + 0.01 && notional >= 0.9 * per_name, "N = {n}: {} planned ${notional:.2}, its weight is ${per_name:.2}", o.symbol);
    }
    assert!(r.placed.iter().all(|p| p.carried()), "{:?}", r.placed.iter().map(|p| (&p.symbol, p.outcome)).collect::<Vec<_>>());
    for t in tickers {
        assert!(rig.handle.position_qty(t).is_positive(), "{t} was bought");
    }
}

#[test]
fn a_nine_instrument_momentum_cell_plans_the_linear_rank_targets() {
    check_linear_rank_targets(&SEC9);
}

#[test]
fn an_eleven_instrument_momentum_cell_plans_the_linear_rank_targets() {
    check_linear_rank_targets(&AC11);
}

#[test]
fn the_decision_clock_is_the_completed_bar_and_a_run_that_sees_no_new_bar_plans_nothing() {
    let tickers = &SEC9;
    let rig = rig_for(tickers);
    let stores = Stores::new();
    let first = run_at(&rig, &stores, tickers, date(2019, 9, 4), "2019-09-05T15:00:00Z");
    assert_eq!(first.outcome.code, "RUN_COMPLETED", "{:?}", first.outcome);
    // The next day's run still sees 2019-09-04 as the newest bar (a holiday, or a vendor that has not published the
    // 5th yet): the decision was already acted on, so nothing is pending and the broker is not read.
    let again = run_at(&rig, &stores, tickers, date(2019, 9, 4), "2019-09-06T15:00:00Z");
    assert_eq!((again.outcome.kind, again.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_NOTHING_PENDING"), "{:?}", again.outcome);
    assert!(!again.decisions[0].pending && !again.decisions[0].entry);
    // A new completed bar is a new decision: pending, and not an entry.
    let next = run_at(&rig, &stores, tickers, date(2019, 9, 6), "2019-09-09T15:00:00Z");
    assert_eq!(next.decisions[0].decision_date, date(2019, 9, 6));
    assert!(next.decisions[0].pending && !next.decisions[0].entry, "{:?}", next.decisions[0]);
}

#[test]
fn a_stale_or_forming_bar_refuses_the_run_and_trades_nothing() {
    let tickers = &SEC9;
    // The newest bar is more than ETF_MAX_STALE_DAYS old: the rule refuses and nothing is placed.
    let rig = rig_for(tickers);
    let stale = run_at(&rig, &Stores::new(), tickers, date(2019, 8, 29), "2019-09-05T15:00:00Z");
    assert_ne!(stale.outcome.code, "RUN_COMPLETED", "{:?}", stale.outcome);
    assert!(stale.placed.is_empty());
    // A bar dated on the run date is a forming bar: refused.
    let rig = rig_for(tickers);
    let forming = run_at(&rig, &Stores::new(), tickers, date(2019, 9, 5), "2019-09-05T15:00:00Z");
    assert_ne!(forming.outcome.code, "RUN_COMPLETED", "{:?}", forming.outcome);
    assert!(forming.placed.is_empty());
}
