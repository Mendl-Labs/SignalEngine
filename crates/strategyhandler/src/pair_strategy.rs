//! Live pairs-trading / statistical-arbitrage strategy support.
//!
//! `PythonBridgeStrategy` (see `lib.rs`) deliberately isolates every
//! `(symbol, exchange)` leg behind its OWN worker process, bar accumulator,
//! and position state -- a real production incident (a shared-state version
//! caused a ~13.7x mis-sized, wrong-symbol live order) proved this isolation
//! is load-bearing for ordinary multi-asset portfolios, not an oversight.
//! A genuine pairs strategy needs the opposite property: BOTH legs' prices
//! visible to one joint decision, the same relationship
//! `compute_all_signals_multi_venue` already gives the backtest engine (see
//! `BacktestingEngine/strategy/src/traits.rs`).
//!
//! Rather than punch a hole in `PythonBridgeStrategy`'s per-leg isolation, or
//! extend `pythonbridge-worker`'s wire protocol (currently a single-series
//! `push_bar(price, volume, timestamp)` / `compute_signal() -> i8` pair, with
//! no multi-series message type at all -- see `pythonbridge_worker::client`),
//! this strategy takes a third path: it computes the pair's SPREAD in Rust
//! (`price_a - hedge_ratio * price_b`, the same relationship the backtest's
//! `compute_all_signals_multi_venue`-authored strategy trades, flattened to
//! one derived scalar series) and feeds that single series through the
//! existing, unmodified, already-proven single-symbol worker protocol. One
//! worker process, fed one synthetic series, for the whole pair -- no
//! wire-protocol changes, and the existing per-leg isolation for ordinary
//! multi-asset deployments is completely untouched (this is a new, sibling
//! `Strategy` implementation, not a modification of `PythonBridgeStrategy`).
//!
//! Honest limitation this implies: the live decision is "trade the spread's
//! own scalar time series" rather than the backtest's fully general "joint
//! function of both legs' raw series" -- for a strategy whose
//! `compute_signals_multi_venue` logic reduces to exactly a hedge-ratio
//! spread (the standard Engle-Granger pairs-trading construction this
//! platform's `get_pair_cointegration` tool is built around), these are the
//! same thing. A strategy doing something more exotic with the two legs
//! jointly would need the wire-protocol extension this file deliberately
//! avoids -- flagged as explicit future work, not silently assumed away.

use std::collections::VecDeque;
use std::error::Error;

use async_trait::async_trait;
use serde_json::Value;
use ultra_signal::{hash_symbol, ExchangeId, Signal, SignalAction};

use crate::{
    accumulate_tick, bar_bucket, resolve_leverage, resolve_position_size_pct,
    size_order_from_capital, BarAccumulator, MarketData, Strategy, StrategyConfig,
    DEFAULT_CANDLE_INTERVAL_MINUTES,
};

/// How this deployment's hedge ratio is determined -- mirrors
/// `strategy::traits::HedgeRatioMode` on the BacktestingEngine side (kept as
/// a separate, string-driven type here rather than sharing that crate
/// directly, since `strategyhandler` has no other dependency on
/// BacktestingEngine's `strategy` crate and pulling one in just for this enum
/// isn't worth the coupling).
#[derive(Debug, Clone, Copy, PartialEq)]
enum HedgeRatioMode {
    Fixed(f64),
    Dynamic,
}

/// Parsed `pair_spec` declaration for a live deployment. Populated once at
/// `initialize()` from `config.parameters["pair_spec"]` -- the JSON shape
/// BacktestingEngine's deployment publisher writes (mirroring the Python
/// `pair_spec()` dict shape the backtest engine reads, see
/// `strategy/src/strategies/python_strategy.rs`).
#[derive(Debug, Clone)]
struct PairSpec {
    symbol_a: String,
    exchange_a: String,
    symbol_b: String,
    exchange_b: String,
    hedge_ratio_mode: HedgeRatioMode,
}

fn parse_pair_spec(parameters: &std::collections::HashMap<String, Value>) -> Option<PairSpec> {
    let spec = parameters.get("pair_spec")?.as_object()?;
    let get_str = |key: &str| -> Option<String> { spec.get(key)?.as_str().map(|s| s.to_string()) };
    let symbol_a = get_str("symbol_a")?;
    let exchange_a = get_str("exchange_a")?;
    let symbol_b = get_str("symbol_b")?;
    let exchange_b = get_str("exchange_b")?;
    let mode_str = get_str("hedge_ratio_mode").unwrap_or_else(|| "fixed".to_string());
    let hedge_ratio_mode = if mode_str.eq_ignore_ascii_case("dynamic") {
        HedgeRatioMode::Dynamic
    } else {
        let ratio = spec.get("hedge_ratio").and_then(|v| v.as_f64()).unwrap_or(1.0);
        HedgeRatioMode::Fixed(ratio)
    };
    Some(PairSpec { symbol_a, exchange_a, symbol_b, exchange_b, hedge_ratio_mode })
}

/// Rolling window (bars) of trailing price history kept per leg for
/// `HedgeRatioMode::Dynamic` re-estimation. Matches
/// `backtest::pair_simulation::PairBacktestConfig::default().lookback_window`
/// so a live deployment's re-estimation cadence is consistent with what its
/// own backtest validated.
const DEFAULT_LOOKBACK_WINDOW: usize = 60;

/// One leg's most recently closed bar, awaiting its sibling leg's bar for
/// the same bucket before a joint spread value can be computed.
#[derive(Debug, Clone, Copy)]
struct PendingBar {
    bucket: i64,
    close: f64,
    timestamp: i64,
}

/// Live pairs-trading strategy: one shared worker process trading the
/// spread between two declared legs. See module doc for the full design.
pub struct PairPythonBridgeStrategy {
    config: StrategyConfig,
    pair_spec: Option<PairSpec>,
    worker: Option<pythonbridge_worker::client::WorkerProcess>,
    candle_interval_minutes: i64,
    resolved_params: std::collections::HashMap<String, f64>,

    acc_a: BarAccumulator,
    acc_b: BarAccumulator,
    pending_a: Option<PendingBar>,
    pending_b: Option<PendingBar>,

    price_history_a: VecDeque<f64>,
    price_history_b: VecDeque<f64>,
    hedge_ratio: f64,

    /// Current spread position: `Some(1)` = long-spread (long A, short B),
    /// `Some(-1)` = short-spread, `None` = flat.
    last_side: Option<i8>,
    last_qty_a: f64,
    last_qty_b: f64,

    bars_computed: u32,
}

impl PairPythonBridgeStrategy {
    pub fn new(config: StrategyConfig) -> Self {
        Self {
            config,
            pair_spec: None,
            worker: None,
            candle_interval_minutes: DEFAULT_CANDLE_INTERVAL_MINUTES,
            resolved_params: std::collections::HashMap::new(),
            acc_a: BarAccumulator::default(),
            acc_b: BarAccumulator::default(),
            pending_a: None,
            pending_b: None,
            price_history_a: VecDeque::with_capacity(DEFAULT_LOOKBACK_WINDOW + 1),
            price_history_b: VecDeque::with_capacity(DEFAULT_LOOKBACK_WINDOW + 1),
            hedge_ratio: 1.0,
            last_side: None,
            last_qty_a: 0.0,
            last_qty_b: 0.0,
            bars_computed: 0,
        }
    }

    /// Which leg `(symbol, exchange)` refers to, if either. `None` for a
    /// tick belonging to neither declared leg (shouldn't happen if the
    /// dispatch loop only routes this deployment's own declared symbols/
    /// exchanges, but defensive rather than assuming that holds).
    fn leg_for(&self, symbol: &str, exchange: &str) -> Option<Leg> {
        let spec = self.pair_spec.as_ref()?;
        if symbol == spec.symbol_a && exchange == spec.exchange_a {
            Some(Leg::A)
        } else if symbol == spec.symbol_b && exchange == spec.exchange_b {
            Some(Leg::B)
        } else {
            None
        }
    }

    fn push_price_history(&mut self, leg: Leg, close: f64) {
        let (history, cap) = match leg {
            Leg::A => (&mut self.price_history_a, DEFAULT_LOOKBACK_WINDOW),
            Leg::B => (&mut self.price_history_b, DEFAULT_LOOKBACK_WINDOW),
        };
        history.push_back(close);
        while history.len() > cap {
            history.pop_front();
        }
    }

    /// Re-estimate the hedge ratio via OLS over trailing history when the
    /// deployment declared `Dynamic` mode and enough history has
    /// accumulated; otherwise keeps the current ratio unchanged (fail-safe:
    /// never trades on a degenerate re-estimate).
    fn maybe_reestimate_hedge_ratio(&mut self) {
        let Some(spec) = &self.pair_spec else { return };
        if spec.hedge_ratio_mode != HedgeRatioMode::Dynamic {
            return;
        }
        if self.price_history_a.len() < DEFAULT_LOOKBACK_WINDOW || self.price_history_b.len() < DEFAULT_LOOKBACK_WINDOW {
            return;
        }
        let y: Vec<f64> = self.price_history_a.iter().copied().collect();
        let x: Vec<f64> = self.price_history_b.iter().copied().collect();
        if let Some(ols) = quant_diagnostics::ols_simple(&y, &x) {
            self.hedge_ratio = ols.beta;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    A,
    B,
}

/// Given both legs' pending closed bars, decide whether to pair them now
/// (buckets match closely enough) or drop the stale one and keep waiting.
/// Pure/testable without a real worker process.
///
/// Exact bucket match is required to actually pair -- anything else means
/// one leg's bar closed a full interval apart from the other's, which is
/// stale enough that computing a spread from them would be comparing prices
/// from two different points in time, not a real joint reading. The older
/// side is dropped so the newer one can wait to be paired with a fresher
/// bar from the other leg instead of pairing with garbage.
fn try_pair_bars(pending_a: PendingBar, pending_b: PendingBar) -> PairOutcome {
    if pending_a.bucket == pending_b.bucket {
        PairOutcome::Paired
    } else if pending_a.bucket < pending_b.bucket {
        PairOutcome::DropStale(Leg::A)
    } else {
        PairOutcome::DropStale(Leg::B)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairOutcome {
    Paired,
    DropStale(Leg),
}

/// `price_a - hedge_ratio * price_b` -- the Engle-Granger spread this
/// strategy trades. Mirrors `backtest::pair_simulation`'s own spread
/// definition exactly, so a `Fixed` hedge ratio validated in backtest means
/// the same thing live.
fn compute_spread(price_a: f64, price_b: f64, hedge_ratio: f64) -> f64 {
    price_a - hedge_ratio * price_b
}

#[async_trait]
impl Strategy for PairPythonBridgeStrategy {
    fn config(&self) -> &StrategyConfig {
        &self.config
    }

    async fn initialize(&mut self) -> Result<(), Box<dyn Error>> {
        let pair_spec = parse_pair_spec(&self.config.parameters)
            .ok_or("PairPythonBridgeStrategy requires a pair_spec entry in config.parameters")?;

        let source_code = self
            .config
            .parameters
            .get("python_source_code")
            .and_then(|v| v.as_str())
            .ok_or("PairPythonBridgeStrategy requires python_source_code in config.parameters")?
            .to_string();

        self.candle_interval_minutes = self
            .config
            .parameters
            .get("candle_interval_minutes")
            .and_then(|v| v.as_i64())
            .unwrap_or(DEFAULT_CANDLE_INTERVAL_MINUTES);

        if let HedgeRatioMode::Fixed(r) = pair_spec.hedge_ratio_mode {
            self.hedge_ratio = r;
        }

        let mut parameters: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for (k, v) in &self.config.parameters {
            if let Some(f) = v.as_f64() {
                parameters.insert(k.clone(), f);
            }
        }

        const WINDOW_SIZE: usize = 200;
        const TIMEOUT_SECS: u64 = 30;
        let binary_path = pythonbridge_worker::client::default_binary_path();
        let mut worker = pythonbridge_worker::client::WorkerProcess::spawn(&binary_path)?;
        self.resolved_params = worker.initialize(source_code, parameters, TIMEOUT_SECS, WINDOW_SIZE)?;
        self.worker = Some(worker);
        self.pair_spec = Some(pair_spec);

        Ok(())
    }

    async fn generate_signals(&mut self, market_data: &MarketData) -> Result<Vec<Signal>, Box<dyn Error>> {
        if self.worker.is_none() {
            return Err("PairPythonBridgeStrategy not initialized".into());
        }

        let Some(leg) = self.leg_for(&market_data.symbol, &market_data.exchange) else {
            return Ok(Vec::new());
        };

        let bucket = bar_bucket(market_data.timestamp, self.candle_interval_minutes);
        let acc = match leg {
            Leg::A => &mut self.acc_a,
            Leg::B => &mut self.acc_b,
        };
        let closed = accumulate_tick(acc, bucket, market_data.mid_price, market_data.volume, market_data.timestamp);
        let Some((close, _volume, timestamp)) = closed else {
            return Ok(Vec::new());
        };

        self.push_price_history(leg, close);
        match leg {
            Leg::A => self.pending_a = Some(PendingBar { bucket, close, timestamp }),
            Leg::B => self.pending_b = Some(PendingBar { bucket, close, timestamp }),
        }

        let (Some(pa), Some(pb)) = (self.pending_a, self.pending_b) else {
            return Ok(Vec::new());
        };

        match try_pair_bars(pa, pb) {
            PairOutcome::DropStale(Leg::A) => {
                self.pending_a = None;
                return Ok(Vec::new());
            }
            PairOutcome::DropStale(Leg::B) => {
                self.pending_b = None;
                return Ok(Vec::new());
            }
            PairOutcome::Paired => {}
        }

        self.pending_a = None;
        self.pending_b = None;
        self.maybe_reestimate_hedge_ratio();

        let spread = compute_spread(pa.close, pb.close, self.hedge_ratio);
        let worker = self.worker.as_mut().expect("checked Some at function entry");
        worker.push_bar(spread, 1.0, pa.timestamp.max(pb.timestamp))?;
        self.bars_computed += 1;
        let raw_signal = worker.compute_signal()?;

        let Some(spec) = self.pair_spec.clone() else {
            return Ok(Vec::new());
        };

        let capital_allocation = self.config.parameters.get("capital_allocation").and_then(|v| v.as_f64());
        let position_size_pct = resolve_position_size_pct(&self.resolved_params, &self.config.parameters);
        let leverage = resolve_leverage(&self.resolved_params, &self.config.parameters);
        let strategy_id = self.config.id.parse::<u16>().unwrap_or(1);

        let symbol_hash_a = hash_symbol(&spec.symbol_a);
        let symbol_hash_b = hash_symbol(&spec.symbol_b);
        let exchange_id_a = ExchangeId::from_venue_name(&spec.exchange_a);
        let exchange_id_b = ExchangeId::from_venue_name(&spec.exchange_b);

        let mk_pair_signals = |action_a: SignalAction, action_b: SignalAction, qty_a: f64, qty_b: f64| -> Vec<Signal> {
            vec![
                Signal::new(strategy_id, symbol_hash_a, exchange_id_a, action_a, qty_a, pa.close),
                Signal::new(strategy_id, symbol_hash_b, exchange_id_b, action_b, qty_b, pb.close),
            ]
        };

        match raw_signal {
            1 if self.last_side.is_none() => {
                let qty_a = size_order_from_capital(capital_allocation, position_size_pct, leverage, pa.close, 0.01);
                let qty_b = self.hedge_ratio.abs() * qty_a;
                self.last_side = Some(1);
                self.last_qty_a = qty_a;
                self.last_qty_b = qty_b;
                Ok(mk_pair_signals(SignalAction::Buy, SignalAction::Sell, qty_a, qty_b))
            }
            -1 if self.last_side.is_none() => {
                let qty_a = size_order_from_capital(capital_allocation, position_size_pct, leverage, pa.close, 0.01);
                let qty_b = self.hedge_ratio.abs() * qty_a;
                self.last_side = Some(-1);
                self.last_qty_a = qty_a;
                self.last_qty_b = qty_b;
                Ok(mk_pair_signals(SignalAction::Sell, SignalAction::Buy, qty_a, qty_b))
            }
            2 => match self.last_side.take() {
                Some(1) => Ok(mk_pair_signals(SignalAction::Sell, SignalAction::Buy, self.last_qty_a, self.last_qty_b)),
                Some(-1) => Ok(mk_pair_signals(SignalAction::Buy, SignalAction::Sell, self.last_qty_a, self.last_qty_b)),
                _ => Ok(Vec::new()),
            },
            _ => Ok(Vec::new()),
        }
    }

    fn update_state(&mut self, _market_data: &MarketData) {}

    async fn shutdown(&mut self) -> Result<(), Box<dyn Error>> {
        self.worker = None;
        Ok(())
    }

    fn update_config(&mut self, config: StrategyConfig) {
        self.config = config;
    }

    fn bars_since_init(&self) -> Option<u32> {
        Some(self.bars_computed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(mode: HedgeRatioMode) -> PairSpec {
        PairSpec {
            symbol_a: "AAA".to_string(),
            exchange_a: "kraken".to_string(),
            symbol_b: "BBB".to_string(),
            exchange_b: "kraken".to_string(),
            hedge_ratio_mode: mode,
        }
    }

    #[test]
    fn parse_pair_spec_reads_fixed_ratio() {
        let mut params = std::collections::HashMap::new();
        params.insert("pair_spec".to_string(), serde_json::json!({
            "symbol_a": "AAA", "exchange_a": "kraken",
            "symbol_b": "BBB", "exchange_b": "kraken",
            "hedge_ratio_mode": "fixed", "hedge_ratio": 1.5,
        }));
        let parsed = parse_pair_spec(&params).expect("should parse");
        assert_eq!(parsed.symbol_a, "AAA");
        assert_eq!(parsed.symbol_b, "BBB");
        assert_eq!(parsed.hedge_ratio_mode, HedgeRatioMode::Fixed(1.5));
    }

    #[test]
    fn parse_pair_spec_reads_dynamic_mode() {
        let mut params = std::collections::HashMap::new();
        params.insert("pair_spec".to_string(), serde_json::json!({
            "symbol_a": "AAA", "exchange_a": "kraken",
            "symbol_b": "BBB", "exchange_b": "kraken",
            "hedge_ratio_mode": "dynamic",
        }));
        let parsed = parse_pair_spec(&params).expect("should parse");
        assert_eq!(parsed.hedge_ratio_mode, HedgeRatioMode::Dynamic);
    }

    #[test]
    fn parse_pair_spec_missing_key_returns_none() {
        let params = std::collections::HashMap::new();
        assert!(parse_pair_spec(&params).is_none());
    }

    #[test]
    fn parse_pair_spec_missing_required_field_returns_none() {
        let mut params = std::collections::HashMap::new();
        params.insert("pair_spec".to_string(), serde_json::json!({
            "symbol_a": "AAA", "exchange_a": "kraken",
            // symbol_b missing
        }));
        assert!(parse_pair_spec(&params).is_none());
    }

    #[test]
    fn try_pair_bars_matches_equal_buckets() {
        let a = PendingBar { bucket: 5, close: 100.0, timestamp: 1000 };
        let b = PendingBar { bucket: 5, close: 50.0, timestamp: 1000 };
        assert_eq!(try_pair_bars(a, b), PairOutcome::Paired);
    }

    #[test]
    fn try_pair_bars_drops_the_older_leg_on_mismatch() {
        let stale_a = PendingBar { bucket: 3, close: 100.0, timestamp: 1000 };
        let fresh_b = PendingBar { bucket: 5, close: 50.0, timestamp: 3000 };
        assert_eq!(try_pair_bars(stale_a, fresh_b), PairOutcome::DropStale(Leg::A));

        let fresh_a = PendingBar { bucket: 5, close: 100.0, timestamp: 3000 };
        let stale_b = PendingBar { bucket: 3, close: 50.0, timestamp: 1000 };
        assert_eq!(try_pair_bars(fresh_a, stale_b), PairOutcome::DropStale(Leg::B));
    }

    #[test]
    fn compute_spread_matches_engle_granger_definition() {
        let s = compute_spread(110.0, 50.0, 2.0);
        assert!((s - (110.0 - 2.0 * 50.0)).abs() < 1e-9);
    }

    #[test]
    fn leg_for_matches_declared_legs_and_rejects_unknown() {
        let config = StrategyConfig {
            id: "1".to_string(),
            name: "test".to_string(),
            enabled: true,
            symbols: vec![],
            exchanges: vec![],
            max_position_size: 0.0,
            risk_limit: 0.0,
            parameters: std::collections::HashMap::new(),
        };
        let mut strat = PairPythonBridgeStrategy::new(config);
        strat.pair_spec = Some(spec(HedgeRatioMode::Fixed(1.0)));

        assert_eq!(strat.leg_for("AAA", "kraken"), Some(Leg::A));
        assert_eq!(strat.leg_for("BBB", "kraken"), Some(Leg::B));
        assert_eq!(strat.leg_for("CCC", "kraken"), None);
    }

    #[test]
    fn maybe_reestimate_hedge_ratio_is_a_noop_for_fixed_mode() {
        let config = StrategyConfig {
            id: "1".to_string(),
            name: "test".to_string(),
            enabled: true,
            symbols: vec![],
            exchanges: vec![],
            max_position_size: 0.0,
            risk_limit: 0.0,
            parameters: std::collections::HashMap::new(),
        };
        let mut strat = PairPythonBridgeStrategy::new(config);
        strat.pair_spec = Some(spec(HedgeRatioMode::Fixed(3.0)));
        strat.hedge_ratio = 3.0;
        for i in 0..100 {
            strat.price_history_a.push_back(100.0 + i as f64);
            strat.price_history_b.push_back(50.0 + 0.5 * i as f64);
        }
        strat.maybe_reestimate_hedge_ratio();
        assert_eq!(strat.hedge_ratio, 3.0, "Fixed mode must never re-estimate");
    }

    #[test]
    fn maybe_reestimate_hedge_ratio_waits_for_enough_history_in_dynamic_mode() {
        let config = StrategyConfig {
            id: "1".to_string(),
            name: "test".to_string(),
            enabled: true,
            symbols: vec![],
            exchanges: vec![],
            max_position_size: 0.0,
            risk_limit: 0.0,
            parameters: std::collections::HashMap::new(),
        };
        let mut strat = PairPythonBridgeStrategy::new(config);
        strat.pair_spec = Some(spec(HedgeRatioMode::Dynamic));
        strat.hedge_ratio = 1.0;
        // Fewer than DEFAULT_LOOKBACK_WINDOW points -- should not update yet.
        for i in 0..10 {
            strat.price_history_a.push_back(100.0 + i as f64);
            strat.price_history_b.push_back(50.0 + 0.5 * i as f64);
        }
        strat.maybe_reestimate_hedge_ratio();
        assert_eq!(strat.hedge_ratio, 1.0);
    }

    #[test]
    fn maybe_reestimate_hedge_ratio_updates_once_enough_history_in_dynamic_mode() {
        let config = StrategyConfig {
            id: "1".to_string(),
            name: "test".to_string(),
            enabled: true,
            symbols: vec![],
            exchanges: vec![],
            max_position_size: 0.0,
            risk_limit: 0.0,
            parameters: std::collections::HashMap::new(),
        };
        let mut strat = PairPythonBridgeStrategy::new(config);
        strat.pair_spec = Some(spec(HedgeRatioMode::Dynamic));
        strat.hedge_ratio = 1.0;
        // y = 2*x exactly -- OLS should recover beta ~= 2.0.
        for i in 0..DEFAULT_LOOKBACK_WINDOW {
            let x = 50.0 + i as f64;
            strat.price_history_a.push_back(2.0 * x);
            strat.price_history_b.push_back(x);
        }
        strat.maybe_reestimate_hedge_ratio();
        assert!((strat.hedge_ratio - 2.0).abs() < 1e-6, "expected hedge_ratio ~2.0, got {}", strat.hedge_ratio);
    }
}
