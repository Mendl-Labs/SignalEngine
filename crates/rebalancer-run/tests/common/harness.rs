//! A full pipeline harness: the fake Kraken exchange + real adapter, in-memory stores, a recording notifier and a
//! kill-flag switch, plus a mandate adapted from mandate-core's baseline for a Kraken crypto-trend account.
#![allow(dead_code)]

use chrono::{DateTime, Duration, NaiveDate, Utc};
use mandate_core::mandate::MandateBody;
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_risk::store::InMemoryStateStore;
use reference_rules::{Panel, PriceSeries};
use rebalancer_run::broker::Broker;
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::pipeline::{run_once, RunConfig, RunContext};
use rebalancer_run::record::{ExecutionMode, RunRecord};
use rebalancer_run::stores::InMemoryRunStore;
use rebalancer_run::testkit::{FixtureData, RecordingNotifier, SwitchKillFlag};
use serde_json::{json, Value};

use super::*;

pub const BASELINE: &str = include_str!("../../../mandate-core/tests/fixtures/baseline_mandate.json");

/// 2021-01-01: the crypto rule wants the newest bar to be the day before the run date.
pub fn run_day() -> NaiveDate {
    NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()
}

pub fn slot_time(n: i64) -> DateTime<Utc> {
    at("2021-01-01T00:10:00Z") + Duration::hours(n)
}

/// 130 daily bars ending on `end`, rising (`up`) or falling; the last close is `last`. A rising series closes above
/// its 100-day average (signal Long), a falling one below it (Cash).
pub fn synth_series(symbol: &str, end: NaiveDate, up: bool, last: f64) -> PriceSeries {
    let n = 130;
    let dates: Vec<NaiveDate> = (0..n).map(|i| end - Duration::days((n - 1 - i) as i64)).collect();
    let closes: Vec<f64> = (0..n)
        .map(|i| {
            let f = i as f64 / (n - 1) as f64;
            if up {
                last * (0.7 + 0.3 * f)
            } else {
                last * (1.3 - 0.3 * f)
            }
        })
        .collect();
    PriceSeries::new(symbol, dates, closes).unwrap()
}

pub fn panel(btc_up: bool, eth_up: bool) -> Panel {
    panel_on(run_day(), btc_up, eth_up)
}

/// A panel that is valid for a run on `as_of` (its newest bar is the day before).
pub fn panel_on(as_of: NaiveDate, btc_up: bool, eth_up: bool) -> Panel {
    let end = as_of.pred_opt().unwrap();
    Panel::new(vec![synth_series("BTC", end, btc_up, 60000.0), synth_series("ETH", end, eth_up, 3000.0)]).unwrap()
}

/// The baseline mandate adapted to a Kraken-only crypto account: 10000 allocated, 50% per instrument, the whole
/// account may be crypto, generous notional and turnover so only the limit under test binds.
pub fn crypto_mandate_json() -> Value {
    let mut v: Value = serde_json::from_str(BASELINE).unwrap();
    v["universe"]["venues"] = json!(["kraken"]);
    v["universe"]["asset_classes"] = json!(["crypto_spot"]);
    v["universe"]["instrument_allow"] = json!(["BTC/USD", "ETH/USD"]);
    v["capital"]["allocated"]["amount"] = json!("10000.00");
    v["exposure"]["max_position"] = json!(0.5);
    v["exposure"]["max_asset_class"] = json!({"crypto_spot": 1.0});
    v["exposure"]["max_order_notional"]["amount"] = json!("6000.00");
    v["exposure"]["max_turnover_per_day"] = json!(2.0);
    v
}

pub fn mandate_from(v: Value) -> MandateBody {
    serde_json::from_value(v).expect("mandate parses")
}

pub fn active_envelope() -> MandateEnvelope {
    MandateEnvelope {
        version: 3,
        status: MandateStatus::Active,
        effective_from: at("2020-12-01T00:00:00Z"),
        review_by: at("2021-06-01T00:00:00Z"),
    }
}

pub fn crypto_sleeve(share: &str) -> SleeveSpec {
    SleeveSpec {
        id: "crypto".into(),
        kind: SleeveKind::CryptoTrend,
        share: d(share),
        venue: "kraken".into(),
        asset_class: "crypto_spot".into(),
        quote: "USD".into(),
    }
}

pub struct Harness {
    pub env: Env,
    pub runs: InMemoryRunStore,
    pub states: InMemoryStateStore,
    pub notifier: RecordingNotifier,
    pub kill: SwitchKillFlag,
    pub data: FixtureData,
    pub mandate: Option<MandateBody>,
    pub envelope: Option<MandateEnvelope>,
    pub sleeves: Vec<SleeveSpec>,
    pub cfg: RunConfig,
    /// The account-local trading day handed to every run.
    pub day: NaiveDate,
    /// Seconds after the scheduled time that the run actually starts.
    pub lateness_secs: i64,
}

impl Harness {
    /// 10000 USD in the account, BTC 60000 and ETH 3000, both trending up, one crypto sleeve at 100%.
    pub fn new() -> Harness {
        let env = Env::with_usd("10000");
        let handle = env.rig.handle.clone();
        let data = FixtureData::new().with_panel("crypto", panel(true, true)).with_price_fn(move |sym| match sym {
            "BTC/USD" | "ETH/USD" => Some(handle.price(sym)),
            _ => None,
        });
        Harness {
            env,
            runs: InMemoryRunStore::new(),
            states: InMemoryStateStore::new(),
            notifier: RecordingNotifier::new(),
            kill: SwitchKillFlag::new(),
            data,
            mandate: Some(mandate_from(crypto_mandate_json())),
            envelope: Some(active_envelope()),
            sleeves: vec![crypto_sleeve("1")],
            cfg: RunConfig::default(),
            day: run_day(),
            lateness_secs: 0,
        }
    }

    /// No sleeves: the run manages nothing and only the risk overlay, reconciliation and flatten are in play.
    pub fn risk_only() -> Harness {
        let mut h = Harness::new();
        h.sleeves = vec![];
        h
    }

    pub fn with_mandate(mut self, edit: impl FnOnce(&mut Value)) -> Harness {
        let mut v = crypto_mandate_json();
        edit(&mut v);
        self.mandate = Some(mandate_from(v));
        self
    }

    pub fn run(&self, mode: ExecutionMode, n: i64) -> RunRecord {
        self.run_with(&self.env.broker(), mode, n)
    }

    pub fn live(&self, n: i64) -> RunRecord {
        self.run(ExecutionMode::Live, n)
    }

    pub fn run_with(&self, broker: &dyn Broker, mode: ExecutionMode, n: i64) -> RunRecord {
        self.run_with_stores(broker, &self.states, mode, n)
    }

    pub fn run_with_stores(&self, broker: &dyn Broker, states: &dyn rebalancer_risk::store::StateStore, mode: ExecutionMode, n: i64) -> RunRecord {
        self.env.clock.set(slot_time(n) + Duration::seconds(self.lateness_secs));
        let rules = kraken_rules(&self.env.pairs);
        let bk = book(&rules);
        let ctx = RunContext {
            account_id: ACCOUNT,
            scheduled_for: slot_time(n),
            trading_day: self.day,
            mode,
            sleeves: &self.sleeves,
            mandate: self.mandate.as_ref(),
            envelope: self.envelope.as_ref(),
            broker,
            data: &self.data,
            state_store: states,
            clock: &self.env.clock,
            runs: &self.runs,
            notifier: &self.notifier,
            kill_flag: &self.kill,
            venue_rules: &bk,
            config: &self.cfg,
        };
        run_once(&ctx)
    }

    /// Set the exchange price of a pair (a market move).
    pub fn price(&self, pair: &str, p: &str) {
        self.env.rig.handle.set_price(pair, p);
    }

    pub fn equity(&self) -> rebalancer_run::Dec {
        self.env.rig.handle.equity(ACCOUNT)
    }
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}
