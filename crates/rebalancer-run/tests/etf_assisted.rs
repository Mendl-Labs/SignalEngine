//! The ETF trend sleeve through the pipeline on the Alpaca side: an ASSISTED run (tickets, nothing placed) over the
//! reference tool's real month-end data, with the Alpaca account read through the adapter and recorded-style fixtures.
//! The fake exchange speaks Kraken only, so Alpaca is exercised here in Assisted mode only (see the report).

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::{AlpacaAdapter, AlpacaConfig, AlpacaCredentials, AssetTable, Environment, PrepareOptions};
use broker_adapters::testing::FakeTransport;
use broker_adapters::transport::HttpResponse;
use chrono::NaiveDate;
use common::*;
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
use reference_rules::{decide_etf_trend, latest_decision_date, Options, Panel, PriceSeries, ETF_SYMBOLS};
use serde_json::{json, Value};

const LADDER: &str = include_str!("../../reference-rules/tests/data/ladder_candles.csv");
const ACCOUNT_FIXTURE: &str = include_str!("../../broker-adapters/tests/fixtures/alpaca/account_ok.json");

fn etf_panel() -> Panel {
    let mut rows: BTreeMap<&str, (Vec<NaiveDate>, Vec<f64>)> = BTreeMap::new();
    for line in LADDER.lines().skip(1) {
        let f: Vec<&str> = line.trim().split(',').collect();
        if ETF_SYMBOLS.contains(&f[0]) {
            let e = rows.entry(f[0]).or_default();
            e.0.push(NaiveDate::parse_from_str(f[1], "%Y-%m-%d").unwrap());
            e.1.push(f[2].parse().unwrap());
        }
    }
    Panel::new(rows.into_iter().map(|(s, (dates, closes))| PriceSeries::new(s, dates, closes).unwrap()).collect()).unwrap()
}

fn mandate() -> mandate_core::mandate::MandateBody {
    let mut v: Value = serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap();
    v["universe"]["venues"] = json!(["alpaca"]);
    v["universe"]["asset_classes"] = json!(["us_etf"]);
    v["universe"]["instrument_allow"] = json!(["SPY", "EFA", "IEF", "DBC", "VNQ"]);
    v["exposure"]["max_asset_class"] = json!({"us_etf": 1.0});
    v["exposure"]["max_position"] = json!(0.25);
    v["exposure"]["max_turnover_per_day"] = json!(2.0);
    serde_json::from_value(v).unwrap()
}

#[test]
fn the_etf_trend_sleeve_produces_tickets_from_real_month_end_data_on_an_alpaca_account() {
    // An Alpaca paper account holding 5000 of cash and nothing else.
    let account = ACCOUNT_FIXTURE
        .replace("\"cash\": \"52840.17\"", "\"cash\": \"5000\"")
        .replace("\"equity\": \"100210.55\"", "\"equity\": \"5000\"")
        .replace("\"portfolio_value\": \"100210.55\"", "\"portfolio_value\": \"5000\"");
    let transport = Arc::new(FakeTransport::new());
    transport.set_handler(move |req| {
        let body = if req.url.contains("/v2/account") { account.clone() } else { "[]".to_string() };
        Ok(HttpResponse { status: 200, body })
    });
    let cfg = AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).unwrap();
    let creds = AlpacaCredentials::new(Environment::Paper, "PKTESTFIXTUREKEY0001", "unit-test-secret-not-a-real-key-9f3a").unwrap();
    let adapter = AlpacaAdapter::new(cfg, creds, transport.clone()).unwrap();
    let broker = AlpacaBroker::us_etf(&adapter);

    // The rule's decision on the same data, computed directly.
    let panel = etf_panel();
    let as_of = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
    let date = latest_decision_date(&panel, &ETF_SYMBOLS).unwrap();
    let decision = decide_etf_trend(&panel, date, &Options::etf_live(as_of)).unwrap();
    let long: Vec<&str> = decision.instruments.iter().filter(|i| i.weight > 0.0).map(|i| i.symbol.as_str()).collect();
    assert!(!long.is_empty() && long.len() < 5, "the fixture date should give a mixed decision, got {long:?}");

    let closes: BTreeMap<String, rebalancer_run::Dec> = ETF_SYMBOLS
        .iter()
        .map(|s| {
            let last = *panel.get(s).unwrap().closes().last().unwrap();
            (s.to_string(), d(&format!("{last:.2}")))
        })
        .collect();
    let data = FixtureData::new().with_panel("etf", panel).with_price_fn(move |sym| closes.get(sym).copied());
    let sleeves = [SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: d("1"), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "USD".into() }];
    let assets = AssetTable::builtin();
    let opts = PrepareOptions { allow_extended_hours: false, min_notional: d("1"), own_tag_prefix: Some("rb1:".into()), refuse_builtin_assets: false };
    let rules = AlpacaRules { assets: &assets, options: &opts };
    let book = VenueRuleBook::new().with("alpaca", &rules);
    let envelope = MandateEnvelope {
        version: 1,
        status: MandateStatus::Active,
        effective_from: at("2026-01-01T00:00:00Z"),
        review_by: at("2027-01-01T00:00:00Z"),
    };
    let mandate = mandate();
    let clock = ManualClock::new(at("2026-09-02T14:00:00Z"));
    let (runs, states, notifier, kill, cfg) = (InMemoryRunStore::new(), InMemoryStateStore::new(), RecordingNotifier::new(), SwitchKillFlag::new(), RunConfig::default());
    let ctx = RunContext {
        account_id: "alpaca-acct",
        scheduled_for: at("2026-09-02T14:00:00Z"),
        trading_day: as_of,
        mode: ExecutionMode::Assisted,
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
    let r = run_once(&ctx);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert_eq!(r.mandate_standing, "active");
    assert_eq!(r.targets.len(), 1);
    assert_eq!(r.targets[0].decision_date, date);
    let target_symbols: Vec<&str> = r.targets[0].weights.iter().filter(|(_, w)| w.is_positive()).map(|(s, _)| s.as_str()).collect();
    assert_eq!(target_symbols, long, "the record carries the rule's decision");
    // One ticket per instrument the rule wants held, all buys (the account is all cash), none placed.
    let ticketed: Vec<&str> = r.tickets.iter().map(|t| t.symbol.as_str()).collect();
    let mut want = long.clone();
    want.sort_unstable();
    let mut got = ticketed.clone();
    got.sort_unstable();
    assert_eq!(got, want, "{:?}", r.plan.as_ref().unwrap().skipped);
    assert!(r.tickets.iter().all(|t| t.side == broker_adapters::Side::Buy && t.venue == "alpaca" && t.asset_class == "us_etf"));
    assert!(r.placed.is_empty());
    // Only reads reached Alpaca: no POST, no DELETE.
    assert!(transport.requests().iter().all(|q| format!("{:?}", q.method) == "Get"), "{:?}", transport.requests().iter().map(|q| (format!("{:?}", q.method), q.url.clone())).collect::<Vec<_>>());
    assert_eq!(r.pre_snapshot.as_ref().unwrap().equity, d("5000"));
    assert_eq!(r.data_fingerprints.len(), 1);
}
