//! Paper Trading Connector
//!
//! Implements the `ExchangeConnector` trait by wrapping SimulationEngine's
//! `MockExchangeConnector`. This enables paper trading mode where orders
//! are simulated against a realistic mock order book instead of hitting
//! a real exchange.

use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use simulation_engine::{
    MockExchangeConnector, MockExchangeConfig, PriceLevel,
    SimulationOrder, OrderSide as SimOrderSide, OrderType as SimOrderType,
    SimulationMetadata, ExecutionStatus as SimExecStatus,
    SimBigDecimal,
};

use crate::core::types::{
    ExecutionResult, ExecutionStatus, ExecutionFill, ExecutionError,
    ExecutionMetrics, ExchangeConfig, ExchangeOrder, ExchangeLimits,
    OrderType, TimeInForce, OrderSide, CancelResult, CancelStatus,
    EditOrderParams, EditResult, EditStatus, OrderStatus, OrderUpdate,
    UpdateType, HealthStatus, HealthState,
};
use crate::signal::{Signal, SignalAction};
use crate::core::traits::ExchangeConnector;

/// Configuration for paper trading
#[derive(Debug, Clone)]
pub struct PaperTradingConfig {
    /// Simulated slippage in basis points
    pub slippage_bps: f64,
    /// Probability of a fill being randomly shrunk to 10-80% of the
    /// requested quantity, independent of the mock order book's actual
    /// depth (0.0 to 1.0). Separate from, and stacks on top of, the
    /// legitimate per-level depth clip in `simulate_market_order` — this
    /// is pure simulated noise, not a liquidity model. Defaults to 0 so
    /// paper-trading deployments (whose whole purpose is validating real
    /// strategy sizing behavior) aren't corrupted by unexplained random
    /// fill-size variance.
    pub partial_fill_probability: f64,
    /// Base latency for simulated execution in milliseconds
    pub base_latency_ms: f64,
    /// Taker fee rate in basis points (default 10 bps = 0.10%)
    pub taker_fee_bps: f64,
    /// Optional realism config for advanced cost modeling
    pub realism: Option<PaperRealismConfig>,
}

impl Default for PaperTradingConfig {
    fn default() -> Self {
        Self {
            slippage_bps: 5.0,
            partial_fill_probability: 0.0,
            base_latency_ms: 10.0,
            taker_fee_bps: 10.0,
            realism: None,
        }
    }
}

/// Advanced realism config for paper trading (mirrors backtest RealismConfig subset)
#[derive(Debug, Clone)]
pub struct PaperRealismConfig {
    /// Market impact coefficient (sqrt model). 0.3 is calibrated default.
    pub market_impact_k: f64,
    /// Estimate spread from order book depth
    pub spread_from_book: bool,
    /// Cap on spread-based cost in bps
    pub spread_cap_bps: f64,
    /// Enable adverse selection penalty
    pub adverse_selection: bool,
    /// Max adverse selection penalty in bps
    pub adverse_selection_max_bps: f64,
}

impl Default for PaperRealismConfig {
    fn default() -> Self {
        Self {
            market_impact_k: 0.3,
            spread_from_book: true,
            spread_cap_bps: 50.0,
            adverse_selection: true,
            adverse_selection_max_bps: 20.0,
        }
    }
}

/// Paper trading connector wrapping SimulationEngine's MockExchangeConnector
pub struct PaperTradingConnector {
    mock: MockExchangeConnector,
    config: PaperTradingConfig,
    total_orders: AtomicU64,
    successful_orders: AtomicU64,
    failed_orders: AtomicU64,
    update_callback: Mutex<Option<Arc<dyn Fn(OrderUpdate) + Send + Sync>>>,
    book_initialized: AtomicBool,
    last_mid_price: Mutex<HashMap<String, f64>>,
    last_spread_bps: Mutex<HashMap<String, f64>>,
    last_book_depth: Mutex<HashMap<String, f64>>,
}

impl PaperTradingConnector {
    /// Create a new paper trading connector
    pub fn new(config: PaperTradingConfig) -> Self {
        let mock_config = MockExchangeConfig {
            base_latency_ms: config.base_latency_ms,
            latency_jitter_ms: config.base_latency_ms * 0.2,
            fill_probability: 1.0 - config.partial_fill_probability * 0.1,
            slippage_bps: config.slippage_bps,
            partial_fill_probability: config.partial_fill_probability,
            rejection_rate: 0.0, // paper trading should not reject
            order_book_depth: 20,
        };

        Self {
            mock: MockExchangeConnector::new(mock_config),
            config,
            total_orders: AtomicU64::new(0),
            successful_orders: AtomicU64::new(0),
            failed_orders: AtomicU64::new(0),
            update_callback: Mutex::new(None),
            book_initialized: AtomicBool::new(false),
            last_mid_price: Mutex::new(HashMap::new()),
            last_spread_bps: Mutex::new(HashMap::new()),
            last_book_depth: Mutex::new(HashMap::new()),
        }
    }

    /// Initialize order book for a symbol at a given mid-price
    pub async fn initialize_book(&self, symbol: &str, mid_price: f64) {
        let price = SimBigDecimal::from_str(&mid_price.to_string())
            .unwrap_or_else(|_| SimBigDecimal::from(0));
        self.mock.initialize_order_book(symbol, price).await;
    }

    /// Apply realistic fill costs using the realism config.
    /// Returns adjusted fill price accounting for market impact, spread, and adverse selection.
    fn apply_paper_fill_costs(&self, base_price: f64, is_buy: bool, order_value: f64, symbol: &str) -> f64 {
        let realism = match &self.config.realism {
            Some(r) => r,
            None => return base_price,
        };

        let canonical = Self::canonical_symbol(symbol);
        let mut total_bps = 0.0f64;

        // Market impact: sqrt model based on order size relative to book depth
        if realism.market_impact_k > 0.0 {
            let book_depth = self.last_book_depth.lock().unwrap()
                .get(&canonical).copied().unwrap_or(1_000_000.0);
            let mid = self.last_mid_price.lock().unwrap()
                .get(&canonical).copied().unwrap_or(base_price);
            if book_depth > 0.0 && mid > 0.0 {
                let participation = order_value / (book_depth * mid);
                total_bps += realism.market_impact_k * participation.min(1.0).sqrt() * 10_000.0;
            }
        }

        // Spread cost from live book
        if realism.spread_from_book {
            let spread_bps = self.last_spread_bps.lock().unwrap()
                .get(&canonical).copied().unwrap_or(0.0);
            total_bps += spread_bps.min(realism.spread_cap_bps) * 0.5;
        }

        // Adverse selection: penalize if price recently moved against the trade direction
        if realism.adverse_selection {
            let mid = self.last_mid_price.lock().unwrap()
                .get(&canonical).copied().unwrap_or(base_price);
            if mid > 0.0 {
                let price_move_bps = (base_price - mid) / mid * 10_000.0;
                let adversely_selected = (is_buy && price_move_bps > 0.0) || (!is_buy && price_move_bps < 0.0);
                if adversely_selected {
                    total_bps += price_move_bps.abs().min(realism.adverse_selection_max_bps);
                }
            }
        }

        let adjustment = base_price * total_bps / 10_000.0;
        if is_buy { base_price + adjustment } else { base_price - adjustment }
    }

    /// Canonical symbol form used by both `update_book` and order lookup, so a
    /// signal that arrives as `BTC/USD` matches a book pushed in as `BTC-USD`.
    fn canonical_symbol(symbol: &str) -> String {
        symbol.replace('/', "-").to_uppercase()
    }

    /// Convert a Signal to a SimulationOrder
    fn signal_to_sim_order(signal: &Signal) -> SimulationOrder {
        let (side, order_type) = match signal.action {
            SignalAction::Buy => (SimOrderSide::Buy, SimOrderType::Market),
            SignalAction::Sell => (SimOrderSide::Sell, SimOrderType::Market),
            SignalAction::BuyLimit => (SimOrderSide::Buy, SimOrderType::Limit),
            SignalAction::SellLimit => (SimOrderSide::Sell, SimOrderType::Limit),
            SignalAction::BuyStop => (SimOrderSide::Buy, SimOrderType::StopLoss),
            SignalAction::SellStop => (SimOrderSide::Sell, SimOrderType::StopLoss),
        };

        let quantity = SimBigDecimal::from_str(&signal.quantity.to_string())
            .unwrap_or_else(|_| SimBigDecimal::from(0));
        let price = signal.price.map(|p| {
            SimBigDecimal::from_str(&p.to_string()).unwrap_or_default()
        });

        SimulationOrder {
            id: Uuid::new_v4(),
            symbol: Self::canonical_symbol(&signal.symbol),
            side,
            order_type,
            quantity,
            price,
            timestamp: Utc::now(),
            simulation_metadata: SimulationMetadata {
                test_scenario: format!("paper_trade_{}", signal.strategy_id),
                latency_trace_id: Uuid::new_v4(),
                expected_latency_ns: None,
                component_routing: vec!["paper_connector".to_string()],
            },
        }
    }

    /// Convert MockExecutionResult to ExecutionResult
    fn convert_result(
        signal: &Signal,
        sim_result: &simulation_engine::MockExecutionResult,
        taker_fee_bps: f64,
    ) -> ExecutionResult {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let status = match sim_result.status {
            SimExecStatus::Filled => ExecutionStatus::Filled,
            SimExecStatus::PartiallyFilled => ExecutionStatus::PartiallyFilled,
            SimExecStatus::Rejected => ExecutionStatus::Rejected,
            SimExecStatus::Cancelled => ExecutionStatus::Cancelled,
        };

        let filled_qty: f64 = sim_result.filled_quantity.to_string().parse().unwrap_or(0.0);
        let avg_price: f64 = sim_result.average_price.to_string().parse().unwrap_or(0.0);
        let fee = avg_price * filled_qty * (taker_fee_bps / 10_000.0);

        let fill = ExecutionFill {
            fill_id: Uuid::new_v4().to_string(),
            order_id: sim_result.order_id.to_string(),
            exchange_order_id: sim_result.order_id.to_string(),
            symbol: signal.symbol.clone(),
            side: match signal.action {
                SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop => OrderSide::Buy,
                _ => OrderSide::Sell,
            },
            quantity: filled_qty,
            price: avg_price,
            fee,
            fee_asset: "USD".to_string(),
            timestamp: now_ns,
            trade_id: Uuid::new_v4().to_string(),
            is_maker: false,
            exchange_timestamp_ns: Some(now_ns),
            exchange_sequence: None,
        };

        let fills = if filled_qty > 0.0 { vec![fill] } else { vec![] };

        let reject_reason = if status == ExecutionStatus::Rejected {
            Some("Simulated rejection".to_string())
        } else {
            None
        };

        ExecutionResult {
            order_id: sim_result.order_id.to_string(),
            exchange_order_id: Some(sim_result.order_id.to_string()),
            exchange: "paper".to_string(),
            status,
            filled_quantity: filled_qty,
            remaining_quantity: signal.quantity - filled_qty,
            avg_fill_price: avg_price,
            total_fees: fee,
            fills,
            reject_reason,
            submitted_at: now_ns,
            updated_at: now_ns,
            latency_ns: sim_result.execution_latency.as_nanos() as u64,
            exchange_timestamp_ns: Some(now_ns),
            exchange_sequence: None,
        }
    }
}

#[async_trait]
impl ExchangeConnector for PaperTradingConnector {
    fn exchange_name(&self) -> &str {
        "paper"
    }

    async fn initialize(&mut self, _config: ExchangeConfig) -> Result<(), ExecutionError> {
        Ok(())
    }

    async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
        self.total_orders.fetch_add(1, Ordering::Relaxed);

        let sim_order = Self::signal_to_sim_order(signal);
        let sim_result = self.mock.execute_order(&sim_order).await
            .map_err(|e| ExecutionError::Exchange(format!("Paper trading error: {}", e)))?;

        let mut result = Self::convert_result(signal, &sim_result, self.config.taker_fee_bps);

        // Apply realism cost model if enabled and we got a fill
        if self.config.realism.is_some() && result.filled_quantity > 0.0 {
            let is_buy = matches!(signal.action, SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop);
            let order_value = result.avg_fill_price * result.filled_quantity;
            let adjusted_price = self.apply_paper_fill_costs(
                result.avg_fill_price, is_buy, order_value, &signal.symbol
            );
            let price_diff = (adjusted_price - result.avg_fill_price).abs();
            let additional_cost = price_diff * result.filled_quantity;
            result.avg_fill_price = adjusted_price;
            result.total_fees += additional_cost;
            let fill_count = result.fills.len().max(1) as f64;
            for fill in &mut result.fills {
                fill.price = adjusted_price;
                fill.fee += additional_cost / fill_count;
            }
        }

        if result.status == ExecutionStatus::Filled || result.status == ExecutionStatus::PartiallyFilled {
            self.successful_orders.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_orders.fetch_add(1, Ordering::Relaxed);
        }

        // Fire async update callback
        if result.filled_quantity > 0.0 {
            if let Some(cb) = self.update_callback.lock().unwrap().as_ref() {
                let now_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                let update_type = if result.status == ExecutionStatus::Filled {
                    UpdateType::CompleteFill
                } else {
                    UpdateType::PartialFill
                };
                cb(OrderUpdate {
                    order_id: result.order_id.clone(),
                    exchange_order_id: result.exchange_order_id.clone().unwrap_or_default(),
                    update_type,
                    status: result.status.clone(),
                    filled_quantity: Some(result.filled_quantity),
                    fill_price: Some(result.avg_fill_price),
                    timestamp: now_ns,
                    exchange_timestamp_ns: Some(now_ns),
                    exchange_sequence: None,
                });
            }
        }

        Ok(result)
    }

    async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        let mut results = Vec::with_capacity(signals.len());
        for signal in signals {
            results.push(self.execute_order(signal).await?);
        }
        Ok(results)
    }

    async fn cancel_order(&self, order_id: &str) -> Result<CancelResult, ExecutionError> {
        // Paper trading: cancels always succeed
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Ok(CancelResult {
            order_id: order_id.to_string(),
            exchange_order_id: Some(order_id.to_string()),
            status: CancelStatus::Cancelled,
            cancelled_at: now_ns,
        })
    }

    async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError> {
        Ok(vec![])
    }

    async fn edit_order(&self, params: EditOrderParams) -> Result<EditResult, ExecutionError> {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Ok(EditResult {
            original_order_id: params.order_id,
            new_order_id: Some(Uuid::new_v4().to_string()),
            status: EditStatus::Success,
            orders_cancelled: 1,
            volume: params.volume.map(|v| v.to_string()),
            price: params.price.map(|p| p.to_string()),
            price2: params.price2.map(|p| p.to_string()),
            description: None,
            edited_at: now_ns,
            latency_ns: (self.config.base_latency_ms * 1_000_000.0) as u64,
        })
    }

    async fn get_order_status(&self, order_id: &str) -> Result<Option<OrderStatus>, ExecutionError> {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Ok(Some(OrderStatus {
            order_id: order_id.to_string(),
            exchange_order_id: Some(order_id.to_string()),
            status: ExecutionStatus::Filled,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            last_updated: now_ns,
        }))
    }

    fn get_metrics(&self) -> ExecutionMetrics {
        ExecutionMetrics {
            exchange: "paper".to_string(),
            total_orders: self.total_orders.load(Ordering::Relaxed),
            successful_orders: self.successful_orders.load(Ordering::Relaxed),
            failed_orders: self.failed_orders.load(Ordering::Relaxed),
            ..Default::default()
        }
    }

    async fn subscribe_to_updates(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        *self.update_callback.lock().unwrap() = Some(Arc::from(callback));
    }

    async fn health_check(&self) -> Result<HealthStatus, ExecutionError> {
        Ok(HealthStatus {
            exchange: "paper".to_string(),
            status: HealthState::Healthy,
            latency_ns: (self.config.base_latency_ms * 1_000_000.0) as u64,
            last_check: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            error_message: None,
        })
    }

    fn get_limits(&self) -> ExchangeLimits {
        ExchangeLimits {
            max_orders_per_second: 1000,
            max_batch_size: 100,
            min_order_size: 0.0001,
            max_order_size: 1_000_000.0,
            tick_size: 0.01,
            supported_order_types: vec![
                OrderType::Market,
                OrderType::Limit,
                OrderType::Stop,
            ],
            supported_time_in_force: vec![
                TimeInForce::GoodTillCancelled,
                TimeInForce::ImmediateOrCancel,
            ],
        }
    }

    fn validate_order(&self, signal: &Signal) -> Result<(), ExecutionError> {
        if signal.quantity <= 0.0 {
            return Err(ExecutionError::Validation("Quantity must be positive".into()));
        }
        Ok(())
    }

    fn update_book(&self, symbol: &str, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) {
        let canonical = Self::canonical_symbol(symbol);

        // Track market microstructure for realism cost model
        if let (Some(&(best_bid, _)), Some(&(best_ask, _))) = (bids.first(), asks.first()) {
            let mid = (best_bid + best_ask) / 2.0;
            if mid > 0.0 {
                let spread_bps = (best_ask - best_bid) / mid * 10_000.0;
                *self.last_mid_price.lock().unwrap().entry(canonical.clone()).or_insert(0.0) = mid;
                *self.last_spread_bps.lock().unwrap().entry(canonical.clone()).or_insert(0.0) = spread_bps;
            }
            let total_depth: f64 = bids.iter().chain(asks.iter()).map(|(_, q)| q).sum();
            *self.last_book_depth.lock().unwrap().entry(canonical.clone()).or_insert(0.0) = total_depth;
        }

        if !self.book_initialized.load(Ordering::Relaxed) {
            self.book_initialized.store(true, Ordering::Relaxed);
        }

        let to_levels = |pairs: Vec<(f64, f64)>| -> Vec<PriceLevel> {
            pairs.into_iter().map(|(p, q)| PriceLevel {
                price: SimBigDecimal::from_str(&p.to_string()).unwrap_or_default(),
                quantity: SimBigDecimal::from_str(&q.to_string()).unwrap_or_default(),
            }).collect()
        };
        self.mock.update_order_book(&canonical, to_levels(bids), to_levels(asks));
    }

    fn convert_signal(&self, signal: &Signal) -> Result<ExchangeOrder, ExecutionError> {
        let (side, order_type) = match signal.action {
            SignalAction::Buy => (OrderSide::Buy, OrderType::Market),
            SignalAction::Sell => (OrderSide::Sell, OrderType::Market),
            SignalAction::BuyLimit => (OrderSide::Buy, OrderType::Limit),
            SignalAction::SellLimit => (OrderSide::Sell, OrderType::Limit),
            SignalAction::BuyStop => (OrderSide::Buy, OrderType::Stop),
            SignalAction::SellStop => (OrderSide::Sell, OrderType::Stop),
        };
        Ok(ExchangeOrder {
            symbol: signal.symbol.clone(),
            side,
            order_type,
            quantity: signal.quantity,
            price: signal.price,
            time_in_force: TimeInForce::GoodTillCancelled,
            client_order_id: signal.id.clone(),
            metadata: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::ExchangeConnector;

    fn test_signal(action: SignalAction, qty: f64, price: Option<f64>) -> Signal {
        Signal {
            id: "sig-001".into(),
            strategy_id: "strat-1".into(),
            symbol: "BTC/USD".into(),
            exchange: "paper".into(),
            action,
            quantity: qty,
            price,
            confidence: 0.9,
            timestamp: 0,
            metadata: HashMap::new(),
        }
    }

    // ── PaperTradingConfig ──────────────────────────────────────

    #[test]
    fn test_config_default() {
        let cfg = PaperTradingConfig::default();
        assert!((cfg.slippage_bps - 5.0).abs() < f64::EPSILON);
        assert!((cfg.partial_fill_probability - 0.0).abs() < f64::EPSILON);
        assert!((cfg.base_latency_ms - 10.0).abs() < f64::EPSILON);
    }

    // ── PaperTradingConnector ───────────────────────────────────

    #[test]
    fn test_connector_exchange_name() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        assert_eq!(conn.exchange_name(), "paper");
    }

    #[test]
    fn test_connector_get_limits() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let limits = conn.get_limits();
        assert_eq!(limits.max_orders_per_second, 1000);
        assert_eq!(limits.max_batch_size, 100);
        assert_eq!(limits.supported_order_types.len(), 3);
    }

    #[test]
    fn test_validate_order_positive_quantity() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let signal = test_signal(SignalAction::Buy, 1.0, None);
        assert!(conn.validate_order(&signal).is_ok());
    }

    #[test]
    fn test_validate_order_zero_quantity() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let signal = test_signal(SignalAction::Buy, 0.0, None);
        assert!(conn.validate_order(&signal).is_err());
    }

    #[test]
    fn test_validate_order_negative_quantity() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let signal = test_signal(SignalAction::Sell, -1.0, None);
        assert!(conn.validate_order(&signal).is_err());
    }

    #[test]
    fn test_convert_signal_buy_market() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let signal = test_signal(SignalAction::Buy, 0.5, None);
        let order = conn.convert_signal(&signal).unwrap();
        assert_eq!(order.symbol, "BTC/USD");
        assert!(matches!(order.side, OrderSide::Buy));
        assert!(matches!(order.order_type, OrderType::Market));
        assert!((order.quantity - 0.5).abs() < f64::EPSILON);
        assert!(order.price.is_none());
    }

    #[test]
    fn test_convert_signal_sell_limit() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let signal = test_signal(SignalAction::SellLimit, 2.0, Some(50000.0));
        let order = conn.convert_signal(&signal).unwrap();
        assert!(matches!(order.side, OrderSide::Sell));
        assert!(matches!(order.order_type, OrderType::Limit));
        assert_eq!(order.price, Some(50000.0));
    }

    #[test]
    fn test_convert_signal_buy_stop() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let signal = test_signal(SignalAction::BuyStop, 1.0, Some(48000.0));
        let order = conn.convert_signal(&signal).unwrap();
        assert!(matches!(order.side, OrderSide::Buy));
        assert!(matches!(order.order_type, OrderType::Stop));
    }

    #[tokio::test]
    async fn test_cancel_order_always_succeeds() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let result = conn.cancel_order("order-123").await.unwrap();
        assert_eq!(result.order_id, "order-123");
        assert!(matches!(result.status, CancelStatus::Cancelled));
    }

    #[tokio::test]
    async fn test_health_check_healthy() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let status = conn.health_check().await.unwrap();
        assert_eq!(status.exchange, "paper");
        assert!(matches!(status.status, HealthState::Healthy));
        assert!(status.error_message.is_none());
    }

    #[tokio::test]
    async fn test_get_order_status() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let status = conn.get_order_status("xyz").await.unwrap();
        assert!(status.is_some());
        let s = status.unwrap();
        assert_eq!(s.order_id, "xyz");
    }

    #[test]
    fn test_get_metrics_initial() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let metrics = conn.get_metrics();
        assert_eq!(metrics.exchange, "paper");
        assert_eq!(metrics.total_orders, 0);
        assert_eq!(metrics.successful_orders, 0);
        assert_eq!(metrics.failed_orders, 0);
    }

    #[tokio::test]
    async fn test_execute_order_market_buy() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        // Initialize the book under the canonical symbol that execute_order will look up.
        conn.initialize_book("BTC-USD", 50000.0).await;

        let signal = test_signal(SignalAction::Buy, 0.1, None);
        let result = conn.execute_order(&signal).await.unwrap();

        assert_eq!(result.exchange, "paper");
        assert!(result.filled_quantity > 0.0);
        let metrics = conn.get_metrics();
        assert_eq!(metrics.total_orders, 1);
    }

    #[tokio::test]
    async fn test_edit_order() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        let params = EditOrderParams {
            order_id: "ord-1".into(),
            pair: "BTC/USD".into(),
            volume: Some(2.0),
            price: Some(51000.0),
            price2: None,
            oflags: None,
            validate: false,
        };
        let result = conn.edit_order(params).await.unwrap();
        assert_eq!(result.original_order_id, "ord-1");
        assert!(result.new_order_id.is_some());
        assert!(matches!(result.status, EditStatus::Success));
    }

    #[tokio::test]
    async fn test_paper_fill_costs_market_impact() {
        let config = PaperTradingConfig {
            realism: Some(PaperRealismConfig {
                market_impact_k: 0.3,
                spread_from_book: false,
                spread_cap_bps: 50.0,
                adverse_selection: false,
                adverse_selection_max_bps: 20.0,
            }),
            ..Default::default()
        };
        let conn = PaperTradingConnector::new(config);

        // Set up book depth and mid price
        conn.update_book("BTC/USD", vec![(50000.0, 10.0)], vec![(50001.0, 10.0)]);

        // Small order: low impact
        let small_price = conn.apply_paper_fill_costs(50000.0, true, 1000.0, "BTC/USD");
        // Large order: higher impact
        let large_price = conn.apply_paper_fill_costs(50000.0, true, 100_000.0, "BTC/USD");

        assert!(large_price > small_price, "Larger order should have worse fill");
        assert!(small_price > 50000.0, "Buy should get filled above base price");
    }

    #[tokio::test]
    async fn test_paper_fill_costs_spread() {
        let config = PaperTradingConfig {
            realism: Some(PaperRealismConfig {
                market_impact_k: 0.0,
                spread_from_book: true,
                spread_cap_bps: 50.0,
                adverse_selection: false,
                adverse_selection_max_bps: 20.0,
            }),
            ..Default::default()
        };
        let conn = PaperTradingConnector::new(config);

        // Wide spread book
        conn.update_book("BTC/USD", vec![(49900.0, 5.0)], vec![(50100.0, 5.0)]);

        let buy_price = conn.apply_paper_fill_costs(50000.0, true, 5000.0, "BTC/USD");
        let sell_price = conn.apply_paper_fill_costs(50000.0, false, 5000.0, "BTC/USD");

        assert!(buy_price > 50000.0, "Buy pays spread cost");
        assert!(sell_price < 50000.0, "Sell pays spread cost");
    }

    #[tokio::test]
    async fn test_fill_event_callback() {
        use std::sync::atomic::AtomicU32;

        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        conn.initialize_book("BTC-USD", 50000.0).await;

        let fill_count = Arc::new(AtomicU32::new(0));
        let count_clone = fill_count.clone();

        conn.subscribe_to_updates(Box::new(move |update| {
            assert!(update.filled_quantity.unwrap_or(0.0) > 0.0);
            count_clone.fetch_add(1, Ordering::Relaxed);
        })).await;

        let signal = test_signal(SignalAction::Buy, 0.1, None);
        let result = conn.execute_order(&signal).await.unwrap();

        assert!(result.filled_quantity > 0.0);
        assert_eq!(fill_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_no_realism_matches_base_behavior() {
        let conn = PaperTradingConnector::new(PaperTradingConfig::default());
        // Without realism, apply_paper_fill_costs is a no-op
        let price = conn.apply_paper_fill_costs(50000.0, true, 10000.0, "BTC/USD");
        assert!((price - 50000.0).abs() < f64::EPSILON);
    }
}
