//! Driver-level replay of a synthetic history through the REAL run machinery: `find_due_runs`, `run_all_due`,
//! `run_once` (kill flag, mandate, decisions, broker read, reconciliation, risk overlay, targets, planner, guard,
//! execution, reconciliation) over a `SimBroker`, in-memory stores and the assumed 00:10Z `Vendor`.

use std::collections::BTreeMap;

use broker_adapters::{Dec, Side};
use chrono::{DateTime, NaiveDate, Utc};
use mandate_core::mandate::MandateBody;
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_core::venue::{SizeRefusal, VenueRuleBook, VenueRules};
use rebalancer_risk::store::InMemoryStateStore;
use rebalancer_run::clock::ManualClock;
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::driver::{find_due_runs, run_all_due, AccountRuntime, ActiveAccount, InMemoryAccountSource};
use rebalancer_run::pipeline::RunConfig;
use rebalancer_run::record::{ExecutionMode, RunRecord};
use rebalancer_run::stores::InMemoryRunStore;
use rebalancer_run::testkit::{InMemoryAccountLock, RecordingNotifier, SwitchKillFlag};
use serde_json::{json, Value};

use super::broker::SimBroker;
use super::world::{Vendor, World};
use super::d;

pub const BASELINE: &str = include_str!("../../../mandate-core/tests/fixtures/baseline_mandate.json");
pub const ACCOUNT_ID: &str = "parity-acct";

pub fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).expect("rfc3339").with_timezone(&Utc)
}

/// The driver's fixed daily slot, 00:10Z, of `day`.
pub fn slot(day: NaiveDate) -> DateTime<Utc> {
    at(&format!("{day}T00:10:00Z"))
}

/// A mandate that lets the plan through (so the plan/target/filter/rounding arithmetic is what is compared, and the
/// per-order guard is not the thing that decides): every venue/asset class/instrument of the replay allowed, position
/// cap 0.5, class and gross caps 1, generous turnover and order counts, a ladder that a replay drawdown cannot reach.
pub fn replay_mandate(allocated: &str, min_cash_reserve: f64) -> MandateBody {
    let mut v: Value = serde_json::from_str(BASELINE).expect("baseline parses");
    v["universe"]["venues"] = json!(["alpaca"]);
    v["universe"]["asset_classes"] = json!(["us_etf", "crypto_spot"]);
    v["universe"]["instrument_allow"] = json!(["SPY", "EFA", "IEF", "DBC", "VNQ", "BTC/USD", "ETH/USD"]);
    v["capital"]["allocated"]["amount"] = json!(allocated);
    v["capital"]["min_cash_reserve"] = json!(min_cash_reserve);
    v["exposure"]["max_position"] = json!(0.5);
    v["exposure"]["max_asset_class"] = json!({"us_etf": 1.0, "crypto_spot": 1.0});
    v["exposure"]["max_order_notional"]["amount"] = json!(allocated);
    v["exposure"]["max_orders_per_day"] = json!(50);
    v["exposure"]["max_turnover_per_day"] = json!(4.0);
    v["loss"]["daily_loss_limit"] = json!(0.5);
    v["loss"]["drawdown_ladder"] = json!([
        {"at": 0.90, "action": "shrink", "scale": 0.5},
        {"at": 0.95, "action": "halt_flatten"}
    ]);
    serde_json::from_value(v).expect("mandate parses")
}

pub fn envelope() -> MandateEnvelope {
    MandateEnvelope { version: 1, status: MandateStatus::Active, effective_from: at("1980-01-01T00:00:00Z"), review_by: at("2099-01-01T00:00:00Z") }
}

pub fn etf_sleeve(share: &str) -> SleeveSpec {
    SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: d(share), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "USD".into() }
}

pub fn crypto_sleeve(share: &str) -> SleeveSpec {
    SleeveSpec { id: "crypto".into(), kind: SleeveKind::CryptoTrend, share: d(share), venue: "alpaca".into(), asset_class: "crypto_spot".into(), quote: "USD".into() }
}

pub fn account(sleeves: Vec<SleeveSpec>, mode: ExecutionMode, mandate: MandateBody) -> ActiveAccount {
    ActiveAccount { account_id: ACCOUNT_ID.into(), tenant_id: "tenant".into(), mandate, envelope: envelope(), plan_approved: true, sleeves, mode }
}

/// The planner-side lot table: whole shares for the ETFs, 8 decimals for the crypto pairs, ALWAYS rounded down, nothing
/// bumped up to a minimum, an unknown symbol refused. (The f64 side's stand-in is `portfolio_construct::LotRounder`.)
pub struct FloorRules;

impl FloorRules {
    fn dp(symbol: &str) -> Option<u32> {
        match symbol.to_uppercase().as_str() {
            "SPY" | "EFA" | "IEF" | "DBC" | "VNQ" => Some(0),
            "BTC/USD" | "ETH/USD" => Some(8),
            _ => None,
        }
    }
}

impl VenueRules for FloorRules {
    fn round_quantity(&self, symbol: &str, _side: Side, quantity: Dec, _price: Dec) -> Result<Dec, SizeRefusal> {
        let dp = Self::dp(symbol).ok_or_else(|| SizeRefusal::UnknownInstrument(symbol.to_string()))?;
        let q = quantity.round_dp(dp, broker_adapters::decimal::Rounding::Floor).map_err(|_| SizeRefusal::Other("overflow".into()))?;
        if q.is_zero() || q.is_negative() {
            return Err(SizeRefusal::RoundsToZero);
        }
        Ok(q)
    }

    fn fingerprint(&self, symbol: &str) -> String {
        format!("floor:{symbol}:{:?}", Self::dp(symbol))
    }
}

/// One replayed day: the run's record plus what the broker looked like afterwards and the fills of that run.
pub struct DayLog {
    pub day: NaiveDate,
    pub rec: RunRecord,
    pub equity_after: Dec,
    pub cash_after: Dec,
    pub holdings_after: BTreeMap<String, Dec>,
    pub fills: Vec<(String, Side, Dec)>,
}

pub struct Rig<'w> {
    pub world: &'w World,
    pub vendor: Vendor<'w>,
    pub broker: SimBroker,
    pub clock: ManualClock,
    pub runs: InMemoryRunStore,
    pub states: InMemoryStateStore,
    pub notifier: RecordingNotifier,
    pub kill: SwitchKillFlag,
    pub cfg: RunConfig,
    pub lock: InMemoryAccountLock,
    pub source: InMemoryAccountSource,
}

impl<'w> Rig<'w> {
    /// A rig with one account holding `cash` and nothing else.
    pub fn new(world: &'w World, account: ActiveAccount, cash: &str) -> Rig<'w> {
        let source = InMemoryAccountSource::new();
        source.set_accounts(vec![account]);
        let class_of = |s: &str| if World::is_etf(s) { "us_etf".to_string() } else { "crypto_spot".to_string() };
        Rig {
            world,
            vendor: Vendor::new(world),
            broker: SimBroker::new(d(cash), class_of),
            clock: ManualClock::new(at("2000-01-01T00:00:00Z")),
            runs: InMemoryRunStore::new(),
            states: InMemoryStateStore::new(),
            notifier: RecordingNotifier::new(),
            kill: SwitchKillFlag::new(),
            cfg: RunConfig::default(),
            lock: InMemoryAccountLock::new(),
            source,
        }
    }

    fn refresh_prices(&self, day: NaiveDate) {
        let cutoff = day.pred_opt().expect("has a predecessor");
        let mut px = Vec::new();
        for s in World::etf_symbols().into_iter().chain(World::crypto_pairs()) {
            if let Some(p) = self.world.close_at_or_before(&s, cutoff) {
                px.push((s, super::world::price_dec(p)));
            }
        }
        self.broker.set_prices(px);
    }

    /// One driver tick at the 00:10Z slot of `day`: the broker's prices are the closes the run may see, then
    /// `find_due_runs` + `run_all_due`. Returns the single account's record.
    pub fn tick(&self, day: NaiveDate) -> RunRecord {
        let now = slot(day);
        self.clock.set(now);
        self.refresh_prices(day);
        let due = find_due_runs(&self.source, now).expect("enumeration must not fail");
        assert_eq!(due.len(), 1, "exactly one account is due on {day}");
        let rules = FloorRules;
        let book = VenueRuleBook::new().with("alpaca", &rules);
        let mut runtimes = BTreeMap::new();
        for spec in &due {
            runtimes.insert(spec.account_id.clone(), AccountRuntime { broker: &self.broker, data: &self.vendor, venue_rules: &book });
        }
        let mut out = run_all_due(due, &runtimes, &self.states, &self.runs, &self.notifier, &self.kill, &self.clock, &self.lock, &self.cfg);
        out.remove(0).result.unwrap_or_else(|e| panic!("{day}: not attempted: {e}"))
    }

    /// Tick every calendar day of `from..=to`.
    pub fn replay(&self, from: NaiveDate, to: NaiveDate) -> Vec<DayLog> {
        let mut logs = Vec::new();
        for day in super::world::all_days(from, to) {
            let f0 = self.broker.fill_count();
            let rec = self.tick(day);
            let fills = self.broker.fills_since(f0);
            logs.push(DayLog { day, rec, equity_after: self.broker.equity(), cash_after: self.broker.cash(), holdings_after: self.broker.holdings(), fills });
        }
        logs
    }
}
