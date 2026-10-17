//! R41 cells through the pipeline: a trend sleeve whose `instruments` are a pre-registered universe of 9 or 11 ETFs
//! plans exactly `1 / N` per instrument the rule holds, against the fake Alpaca exchange and the REAL adapter (the
//! asset table is refreshed per instrument and `refuse_builtin_assets` is ON, exactly as the service runs). The
//! published five-ETF sleeve is exercised by the other ETF tests and is untouched by this file.

mod common;

use std::collections::BTreeMap;

use broker_adapters::alpaca::{AssetTable, PrepareOptions};
use chrono::{Datelike, NaiveDate, Weekday};
use common::*;
use fake_broker::alpaca::{AssetSpec, FakeAlpaca};
use fake_broker::alpaca_rig::AlpacaRig;
use rebalancer_core::dec_math::{ratio_to_dec, RatioRound};
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_core::venue::{AlpacaRules, VenueRuleBook};
use rebalancer_risk::store::InMemoryStateStore;
use rebalancer_run::broker::AlpacaBroker;
use rebalancer_run::clock::ManualClock;
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::pipeline::{run_once, RunConfig, RunContext};
use rebalancer_run::record::{ExecutionMode, OutcomeKind};
use rebalancer_run::stores::InMemoryRunStore;
use rebalancer_run::testkit::{FixtureData, RecordingNotifier, SwitchKillFlag};
use reference_rules::{Panel, PriceSeries};
use serde_json::{json, Value};

const SEC9: [&str; 9] = ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"];
const AC11: [&str; 11] = ["SPY", "EFA", "IEF", "DBC", "VNQ", "IWM", "EEM", "TLT", "LQD", "HYG", "GLD"];

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

/// Weekday bars to 2019-09-04. The first `up` symbols rise into a last close of exactly 100 (above their average,
/// so the rule holds them); the rest fall into 100 (below it, so the rule leaves them in cash). 100 is the fake
/// exchange's flat mark, so the sizing price and the venue price agree.
fn panel(tickers: &[&str], up: usize) -> Panel {
    let mut days = Vec::new();
    let mut d = date(2018, 1, 1);
    while d <= date(2019, 9, 4) {
        if !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) {
            days.push(d);
        }
        d = d.succ_opt().unwrap();
    }
    let k = days.len();
    let series = tickers
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let closes: Vec<f64> = (0..k).map(|j| if i < up { 100.0 - (k - 1 - j) as f64 * 0.05 } else { 100.0 + (k - 1 - j) as f64 * 0.05 }).collect();
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

fn cell_sleeve(tickers: &[&str]) -> SleeveSpec {
    SleeveSpec {
        id: "etf_trend_faber".into(),
        kind: SleeveKind::EtfTrend,
        share: d("1"),
        venue: "alpaca".into(),
        asset_class: "us_etf".into(),
        quote: "USD".into(),
        instruments: tickers.iter().map(|s| s.to_string()).collect(),
        tranche_sessions: 0,
        lookback: None,
    }
}

fn run_cell(tickers: &[&str], up: usize) -> (rebalancer_run::record::RunRecord, AlpacaRig) {
    // A fake exchange that lists exactly the cell's instruments, $5,000 cash, a flat $100 mark; the adapter refuses
    // built-in (unverified) asset rows, and the planner's asset table is refreshed per instrument, as the service does.
    let mut builder = FakeAlpaca::builder().cash("5000");
    for t in tickers {
        builder = builder.asset(AssetSpec::etf(t), "100");
    }
    let rig = AlpacaRig::with_fake(builder.build(), |mut c| {
        c.own_tag_prefix = Some("rb1:".to_string());
        c.refuse_builtin_assets = true;
        c
    });
    let mut assets = AssetTable::empty();
    for t in tickers {
        assets.upsert(rig.adapter.refresh_asset(t).expect("the fake lists the instrument"));
    }
    let opts = PrepareOptions { allow_extended_hours: false, min_notional: d("1"), own_tag_prefix: Some("rb1:".into()), refuse_builtin_assets: true, allow_crypto: false };
    let rules = AlpacaRules { assets: &assets, options: &opts };
    let book = VenueRuleBook::new().with("alpaca", &rules);
    let broker = AlpacaBroker::us_etf(&rig.adapter);

    let now = at("2019-09-05T15:00:00Z");
    rig.clock.set_nanos(now.timestamp_nanos_opt().expect("in range") as u64);
    let data = FixtureData::new().with_panel("etf_trend_faber", panel(tickers, up)).with_price_fn(|_| Some(d("100.00")));
    let sleeves = [cell_sleeve(tickers)];
    let envelope = MandateEnvelope { version: 1, status: MandateStatus::Active, effective_from: at("2019-01-01T00:00:00Z"), review_by: at("2099-01-01T00:00:00Z") };
    let mandate = mandate(tickers);
    let clock = ManualClock::new(now);
    let (runs, states, notifier, kill, cfg) = (InMemoryRunStore::new(), InMemoryStateStore::new(), RecordingNotifier::new(), SwitchKillFlag::new(), RunConfig::default());
    let ctx = RunContext {
        account_id: "cell-acct",
        scheduled_for: now,
        trading_day: date(2019, 9, 5),
        mode: ExecutionMode::Live,
        sleeves: &sleeves,
        mandate: Some(&mandate),
        envelope: Some(&envelope),
        broker: &broker,
        data: &data,
        state_store: &states,
        clock: &clock,
        runs: &runs,
        notifier: &notifier,
        kill_flag: &kill,
        venue_rules: &book,
        config: &cfg,
        cache: None,
    };
    let record = run_once(&ctx);
    (record, rig)
}

fn check_one_over_n(tickers: &[&str]) {
    let n = tickers.len();
    let up = n - 2;
    let (r, rig) = run_cell(tickers, up);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "N = {n}: {:?}", r.outcome);

    // The target: exactly 1/N (rounded down once, as every weight is) on the instruments the rule holds, zero on the rest.
    let want_w = ratio_to_dec(1.0 / n as f64, RatioRound::Down).unwrap();
    assert_eq!(r.targets.len(), 1);
    let weights: BTreeMap<&str, broker_adapters::Dec> = r.targets[0].weights.iter().map(|(s, w)| (s.as_str(), *w)).collect();
    assert_eq!(weights.len(), n, "the target names every instrument of the universe");
    for (i, t) in tickers.iter().enumerate() {
        if i < up {
            assert_eq!(weights[t], want_w, "N = {n}: {t} is held at 1/N");
        } else {
            assert!(!weights[t].is_positive(), "N = {n}: {t} is in cash");
        }
    }
    let gross: f64 = r.targets[0].weights.iter().map(|(_, w)| w.to_f64()).sum();
    assert!(gross <= 1.0, "N = {n}: max gross stays at or under 100%, got {gross}");

    // The evidence the record keeps carries the rule's own f64 weight, bit for bit.
    let held: Vec<_> = r.decisions[0].instruments.iter().filter(|i| i.weight > 0.0).collect();
    assert_eq!(held.len(), up);
    assert!(held.iter().all(|i| i.weight.to_bits() == (1.0_f64 / n as f64).to_bits()));

    // The plan: one buy per held instrument, all the same size (same price, same weight), each close to 1/N of the
    // $5,000 account (a cash reserve may keep it a little under), carried out on the fake exchange.
    let plan = r.plan.as_ref().expect("a plan exists");
    let mut planned: Vec<(String, f64)> = plan.orders.iter().map(|o| (o.symbol.clone(), o.quantity.to_f64() * 100.0)).collect();
    planned.sort_by(|a, b| a.0.cmp(&b.0));
    let mut want_symbols: Vec<String> = tickers[..up].iter().map(|s| s.to_string()).collect();
    want_symbols.sort();
    assert_eq!(planned.iter().map(|(s, _)| s.clone()).collect::<Vec<_>>(), want_symbols);
    let per_name = 5000.0 / n as f64;
    for (s, notional) in &planned {
        assert!(*notional <= per_name + 0.01 && *notional >= 0.9 * per_name, "N = {n}: {s} planned ${notional:.2}, 1/N is ${per_name:.2}");
        assert!((notional - planned[0].1).abs() < 0.01, "N = {n}: equal weights give equal sizes");
    }
    assert!(r.placed.iter().all(|p| p.carried()), "{:?}", r.placed.iter().map(|p| (&p.symbol, p.outcome)).collect::<Vec<_>>());
    for t in tickers {
        let held_units = rig.handle.position_qty(t);
        if tickers[..up].contains(t) {
            assert!(held_units.is_positive(), "{t} was bought");
        } else {
            assert!(!held_units.is_positive(), "{t} stays in cash");
        }
    }
}

#[test]
fn a_nine_instrument_cell_plans_one_ninth_per_held_instrument() {
    check_one_over_n(&SEC9);
}

#[test]
fn an_eleven_instrument_cell_plans_one_eleventh_per_held_instrument() {
    check_one_over_n(&AC11);
}
