//! Mock Exchange for Integration Testing
//!
//! Provides a fully functional mock exchange that simulates:
//! - Order matching with configurable fill behavior
//! - Latency simulation (mean + jitter)
//! - Partial fills and rejections
//! - Rate limiting
//! - Position tracking

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use tokio::sync::RwLock;

use crate::core::types::*;
use crate::signal::{Signal, SignalAction};
use crate::optimizations::timestamp::nano_timestamp;

/// Configuration for mock exchange behavior
#[derive(Debug, Clone)]
pub struct MockExchangeConfig {
    /// Exchange name
    pub name: String,
    /// Mean latency in microseconds
    pub latency_us: u64,
    /// Latency jitter as percentage (0.0 - 1.0)
    pub latency_jitter: f64,
    /// Fill behavior configuration
    pub fill_behavior: MockFillBehavior,
    /// Rate limit (orders per second)
    pub rate_limit_per_sec: Option<u32>,
    /// Should simulate network errors occasionally
    pub simulate_errors: bool,
    /// Error rate (0.0 - 1.0)
    pub error_rate: f64,
    /// Available balance per asset
    pub initial_balances: HashMap<String, f64>,
    /// Maker fee rate
    pub maker_fee: f64,
    /// Taker fee rate
    pub taker_fee: f64,
    /// Max execution history size (memory bound)
    pub max_history_size: usize,
}

impl Default for MockExchangeConfig {
    fn default() -> Self {
        let mut balances = HashMap::new();
        balances.insert("USD".to_string(), 100_000.0);
        balances.insert("BTC".to_string(), 10.0);
        balances.insert("ETH".to_string(), 100.0);
        
        Self {
            name: "MockExchange".to_string(),
            latency_us: 1000, // 1ms default
            latency_jitter: 0.2,
            fill_behavior: MockFillBehavior::default(),
            rate_limit_per_sec: Some(100),
            simulate_errors: false,
            error_rate: 0.01,
            initial_balances: balances,
            maker_fee: 0.001,  // 0.1%
            taker_fee: 0.002,  // 0.2%
            max_history_size: 10_000, // Memory bound for testing
        }
    }
}

/// Configuration for how orders get filled
#[derive(Debug, Clone)]
pub struct MockFillBehavior {
    /// Probability of immediate full fill (0.0 - 1.0)
    pub full_fill_rate: f64,
    /// Probability of partial fill (0.0 - 1.0)
    pub partial_fill_rate: f64,
    /// Min partial fill percentage
    pub min_partial_pct: f64,
    /// Max partial fill percentage
    pub max_partial_pct: f64,
    /// Probability of rejection
    pub reject_rate: f64,
    /// Slippage in basis points for market orders
    pub slippage_bps: f64,
    /// Should simulate price improvement
    pub allow_price_improvement: bool,
}

impl Default for MockFillBehavior {
    fn default() -> Self {
        Self {
            full_fill_rate: 0.85,
            partial_fill_rate: 0.10,
            min_partial_pct: 0.3,
            max_partial_pct: 0.9,
            reject_rate: 0.05,
            slippage_bps: 5.0,
            allow_price_improvement: true,
        }
    }
}

/// Mock order stored in the exchange
#[derive(Debug, Clone)]
pub struct MockOrder {
    pub order_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub price: Option<f64>,
    pub status: ExecutionStatus,
    pub created_at: u128,
    pub fills: Vec<ExecutionFill>,
}

/// Mock exchange for integration testing
pub struct MockExchange {
    config: MockExchangeConfig,
    /// Active orders
    orders: Arc<RwLock<HashMap<String, MockOrder>>>,
    /// Balances by asset
    balances: Arc<RwLock<HashMap<String, f64>>>,
    /// Positions by symbol
    positions: Arc<RwLock<HashMap<String, f64>>>,
    /// Order counter for ID generation
    order_counter: AtomicU64,
    /// Orders submitted in last second (for rate limiting)
    orders_this_second: AtomicU64,
    /// Last rate limit window start
    rate_limit_window: AtomicU64,
    /// Is exchange "connected"
    connected: AtomicBool,
    /// Execution history for assertions
    execution_history: Arc<RwLock<Vec<ExecutionResult>>>,
    /// Metrics
    total_orders: AtomicU64,
    total_fills: AtomicU64,
    total_rejects: AtomicU64,
}

impl MockExchange {
    pub fn new(config: MockExchangeConfig) -> Self {
        let balances = Arc::new(RwLock::new(config.initial_balances.clone()));
        
        Self {
            config,
            orders: Arc::new(RwLock::new(HashMap::new())),
            balances,
            positions: Arc::new(RwLock::new(HashMap::new())),
            order_counter: AtomicU64::new(1),
            orders_this_second: AtomicU64::new(0),
            rate_limit_window: AtomicU64::new(0),
            connected: AtomicBool::new(true),
            execution_history: Arc::new(RwLock::new(Vec::new())),
            total_orders: AtomicU64::new(0),
            total_fills: AtomicU64::new(0),
            total_rejects: AtomicU64::new(0),
        }
    }

    /// Create with default configuration
    pub fn default_instance() -> Self {
        Self::new(MockExchangeConfig::default())
    }

    /// Simulate network disconnection
    pub fn disconnect(&self) {
        self.connected.store(false, Ordering::SeqCst);
    }

    /// Simulate network reconnection
    pub fn reconnect(&self) {
        self.connected.store(true, Ordering::SeqCst);
    }

    /// Get execution history for testing assertions
    pub async fn get_execution_history(&self) -> Vec<ExecutionResult> {
        self.execution_history.read().await.clone()
    }

    /// Get current position for a symbol
    pub async fn get_position(&self, symbol: &str) -> f64 {
        self.positions.read().await.get(symbol).copied().unwrap_or(0.0)
    }

    /// Get current balance for an asset
    pub async fn get_balance(&self, asset: &str) -> f64 {
        self.balances.read().await.get(asset).copied().unwrap_or(0.0)
    }

    /// Get all open orders
    pub async fn get_open_orders(&self) -> Vec<MockOrder> {
        self.orders.read().await
            .values()
            .filter(|o| matches!(o.status, ExecutionStatus::Pending | ExecutionStatus::Submitted | ExecutionStatus::PartiallyFilled))
            .cloned()
            .collect()
    }

    /// Clear all state (for test reset)
    pub async fn reset(&self) {
        self.orders.write().await.clear();
        *self.balances.write().await = self.config.initial_balances.clone();
        self.positions.write().await.clear();
        self.execution_history.write().await.clear();
        self.order_counter.store(1, Ordering::SeqCst);
        self.total_orders.store(0, Ordering::SeqCst);
        self.total_fills.store(0, Ordering::SeqCst);
        self.total_rejects.store(0, Ordering::SeqCst);
    }

    /// Get metrics
    pub fn get_metrics(&self) -> MockExchangeMetrics {
        MockExchangeMetrics {
            total_orders: self.total_orders.load(Ordering::Relaxed),
            total_fills: self.total_fills.load(Ordering::Relaxed),
            total_rejects: self.total_rejects.load(Ordering::Relaxed),
        }
    }

    /// Simulate latency
    async fn simulate_latency(&self) {
        let base = self.config.latency_us as f64;
        let jitter = base * self.config.latency_jitter * (rand::random::<f64>() * 2.0 - 1.0);
        let latency_us = (base + jitter).max(0.0) as u64;
        tokio::time::sleep(tokio::time::Duration::from_micros(latency_us)).await;
    }

    /// Check rate limit
    fn check_rate_limit(&self) -> bool {
        if let Some(limit) = self.config.rate_limit_per_sec {
            let now = nano_timestamp() as u64;
            let window_start = self.rate_limit_window.load(Ordering::Relaxed);
            
            // Reset window if more than 1 second has passed
            if now - window_start > 1_000_000_000 {
                self.rate_limit_window.store(now, Ordering::Relaxed);
                self.orders_this_second.store(1, Ordering::Relaxed);
                return true;
            }
            
            let count = self.orders_this_second.fetch_add(1, Ordering::Relaxed);
            count < limit as u64
        } else {
            true
        }
    }

    /// Determine fill outcome
    fn determine_fill_outcome(&self) -> FillOutcome {
        let roll = rand::random::<f64>();
        let behavior = &self.config.fill_behavior;
        
        if roll < behavior.reject_rate {
            FillOutcome::Rejected
        } else if roll < behavior.reject_rate + behavior.partial_fill_rate {
            let fill_pct = behavior.min_partial_pct 
                + rand::random::<f64>() * (behavior.max_partial_pct - behavior.min_partial_pct);
            FillOutcome::PartialFill(fill_pct)
        } else {
            FillOutcome::FullFill
        }
    }

    /// Generate order ID
    fn generate_order_id(&self) -> String {
        let id = self.order_counter.fetch_add(1, Ordering::Relaxed);
        format!("MOCK-{}-{}", self.config.name, id)
    }

    /// Calculate fill price with slippage
    fn calculate_fill_price(&self, signal: &Signal) -> f64 {
        let base_price = signal.price.unwrap_or(100.0);
        let slippage_pct = self.config.fill_behavior.slippage_bps / 10000.0;
        
        let slippage_direction = match signal.action {
            SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop => 1.0,
            SignalAction::Sell | SignalAction::SellLimit | SignalAction::SellStop => -1.0,
        };
        
        // Apply slippage (market orders get more)
        let slippage_multiplier = match signal.action {
            SignalAction::Buy | SignalAction::Sell => 1.0,
            _ => 0.3,
        };
        
        let actual_slippage = slippage_pct * slippage_direction * slippage_multiplier * rand::random::<f64>();
        base_price * (1.0 + actual_slippage)
    }

    /// Update position after fill
    async fn update_position(&self, symbol: &str, side: &OrderSide, quantity: f64) {
        let mut positions = self.positions.write().await;
        let current = positions.get(symbol).copied().unwrap_or(0.0);
        
        let delta = match side {
            OrderSide::Buy => quantity,
            OrderSide::Sell => -quantity,
        };
        
        positions.insert(symbol.to_string(), current + delta);
    }

    /// Update balance after fill
    async fn update_balance(&self, asset: &str, delta: f64) {
        let mut balances = self.balances.write().await;
        let current = balances.get(asset).copied().unwrap_or(0.0);
        balances.insert(asset.to_string(), current + delta);
    }
    
    /// Parse symbol into base/quote assets
    fn parse_symbol(&self, symbol: &str) -> (String, String) {
        // Handle common formats: BTC/USD, BTCUSD, BTC-USD
        let parts: Vec<&str> = symbol.split(|c| c == '/' || c == '-').collect();
        if parts.len() == 2 {
            (parts[0].to_string(), parts[1].to_string())
        } else if symbol.len() >= 6 {
            // Assume XXXYYY format
            let (base, quote) = symbol.split_at(3);
            (base.to_string(), quote.to_string())
        } else {
            (symbol.to_string(), "USD".to_string())
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FillOutcome {
    FullFill,
    PartialFill(f64),
    Rejected,
}

#[derive(Debug, Clone)]
pub struct MockExchangeMetrics {
    pub total_orders: u64,
    pub total_fills: u64,
    pub total_rejects: u64,
}

impl MockExchange {
    /// Execute a signal (standalone method, not trait impl)
    pub async fn execute_signal(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
        let start_time = nano_timestamp() as u128;
        
        // Check connection
        if !self.connected.load(Ordering::SeqCst) {
            return Err(ExecutionError::Connection("Exchange disconnected".to_string()));
        }
        
        // Check rate limit
        if !self.check_rate_limit() {
            self.total_rejects.fetch_add(1, Ordering::Relaxed);
            return Err(ExecutionError::RateLimit("Rate limit exceeded".to_string()));
        }
        
        // Simulate latency
        self.simulate_latency().await;
        
        // Track order
        self.total_orders.fetch_add(1, Ordering::Relaxed);
        
        // Determine outcome
        let outcome = self.determine_fill_outcome();
        let order_id = self.generate_order_id();
        let exchange_order_id = format!("EX-{}", order_id);
        
        let side = match signal.action {
            SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop => OrderSide::Buy,
            SignalAction::Sell | SignalAction::SellLimit | SignalAction::SellStop => OrderSide::Sell,
        };
        
        let now = nano_timestamp() as u128;
        let latency = (now - start_time) as u64;
        
        let result = match outcome {
            FillOutcome::FullFill => {
                let fill_price = self.calculate_fill_price(signal);
                let fee = signal.quantity * fill_price * self.config.taker_fee;
                
                // Update position and balance
                self.update_position(&signal.symbol, &side, signal.quantity).await;
                
                // Update balances based on side
                let (base, quote) = self.parse_symbol(&signal.symbol);
                match side {
                    OrderSide::Buy => {
                        self.update_balance(&base, signal.quantity).await;
                        self.update_balance(&quote, -(signal.quantity * fill_price + fee)).await;
                    }
                    OrderSide::Sell => {
                        self.update_balance(&base, -signal.quantity).await;
                        self.update_balance(&quote, signal.quantity * fill_price - fee).await;
                    }
                }
                
                self.total_fills.fetch_add(1, Ordering::Relaxed);
                
                let fill = ExecutionFill {
                    fill_id: format!("FILL-{}", order_id),
                    order_id: order_id.clone(),
                    exchange_order_id: exchange_order_id.clone(),
                    symbol: signal.symbol.clone(),
                    side: side.clone(),
                    quantity: signal.quantity,
                    price: fill_price,
                    fee,
                    fee_asset: quote.clone(),
                    timestamp: now,
                    trade_id: format!("TRADE-{}", order_id),
                    is_maker: false,
                    exchange_timestamp_ns: Some(now),
                    exchange_sequence: None,
                };
                
                ExecutionResult {
                    order_id: order_id.clone(),
                    exchange_order_id: Some(exchange_order_id),
                    exchange: self.config.name.clone(),
                    status: ExecutionStatus::Filled,
                    filled_quantity: signal.quantity,
                    remaining_quantity: 0.0,
                    avg_fill_price: fill_price,
                    total_fees: fee,
                    fills: vec![fill],
                    reject_reason: None,
                    submitted_at: start_time,
                    updated_at: now,
                    latency_ns: latency,
                    exchange_timestamp_ns: Some(now), // Mock exchange timestamp
                    exchange_sequence: None,
                }
            }
            FillOutcome::PartialFill(fill_pct) => {
                let filled_qty = signal.quantity * fill_pct;
                let fill_price = self.calculate_fill_price(signal);
                let fee = filled_qty * fill_price * self.config.taker_fee;
                
                // Update position for partial fill
                self.update_position(&signal.symbol, &side, filled_qty).await;
                
                let (base, quote) = self.parse_symbol(&signal.symbol);
                match side {
                    OrderSide::Buy => {
                        self.update_balance(&base, filled_qty).await;
                        self.update_balance(&quote, -(filled_qty * fill_price + fee)).await;
                    }
                    OrderSide::Sell => {
                        self.update_balance(&base, -filled_qty).await;
                        self.update_balance(&quote, filled_qty * fill_price - fee).await;
                    }
                }
                
                self.total_fills.fetch_add(1, Ordering::Relaxed);
                
                let fill = ExecutionFill {
                    fill_id: format!("FILL-{}", order_id),
                    order_id: order_id.clone(),
                    exchange_order_id: exchange_order_id.clone(),
                    symbol: signal.symbol.clone(),
                    side: side.clone(),
                    quantity: filled_qty,
                    price: fill_price,
                    fee,
                    fee_asset: quote.clone(),
                    timestamp: now,
                    trade_id: format!("TRADE-{}", order_id),
                    is_maker: false,
                    exchange_timestamp_ns: Some(now),
                    exchange_sequence: None,
                };
                
                ExecutionResult {
                    order_id: order_id.clone(),
                    exchange_order_id: Some(exchange_order_id),
                    exchange: self.config.name.clone(),
                    status: ExecutionStatus::PartiallyFilled,
                    filled_quantity: filled_qty,
                    remaining_quantity: signal.quantity - filled_qty,
                    avg_fill_price: fill_price,
                    total_fees: fee,
                    fills: vec![fill],
                    reject_reason: None,
                    submitted_at: start_time,
                    updated_at: now,
                    latency_ns: latency,
                    exchange_timestamp_ns: Some(now), // Mock exchange timestamp
                    exchange_sequence: None,
                }
            }
            FillOutcome::Rejected => {
                self.total_rejects.fetch_add(1, Ordering::Relaxed);
                
                ExecutionResult {
                    order_id: order_id.clone(),
                    exchange_order_id: Some(exchange_order_id),
                    exchange: self.config.name.clone(),
                    status: ExecutionStatus::Rejected,
                    filled_quantity: 0.0,
                    remaining_quantity: signal.quantity,
                    avg_fill_price: 0.0,
                    total_fees: 0.0,
                    fills: vec![],
                    reject_reason: Some("Order rejected by mock exchange".to_string()),
                    submitted_at: start_time,
                    updated_at: now,
                    latency_ns: latency,
                    exchange_timestamp_ns: None, // No exchange timestamp for rejects
                    exchange_sequence: None,
                }
            }
        };
        
        // Store execution history (bounded)
        {
            let mut history = self.execution_history.write().await;
            if history.len() >= self.config.max_history_size {
                history.remove(0); // Remove oldest
            }
            history.push(result.clone());
        }
        
        Ok(result)
    }
    
    /// Cancel a specific order by ID
    pub async fn cancel_order(&self, order_id: &str) -> Result<CancelResult, ExecutionError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(ExecutionError::Connection("Exchange disconnected".to_string()));
        }
        
        self.simulate_latency().await;
        
        let mut orders = self.orders.write().await;
        if let Some(order) = orders.get_mut(order_id) {
            order.status = ExecutionStatus::Cancelled;
            
            Ok(CancelResult {
                order_id: order_id.to_string(),
                exchange_order_id: Some(order.client_order_id.clone()),
                status: CancelStatus::Cancelled,
                cancelled_at: nano_timestamp() as u128,
            })
        } else {
            Ok(CancelResult {
                order_id: order_id.to_string(),
                exchange_order_id: None,
                status: CancelStatus::NotFound,
                cancelled_at: nano_timestamp() as u128,
            })
        }
    }
    
    /// Cancel all orders, optionally filtered by symbol
    pub async fn cancel_all_orders(&self, symbol: Option<&str>) -> Result<Vec<CancelResult>, ExecutionError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(ExecutionError::Connection("Exchange disconnected".to_string()));
        }
        
        self.simulate_latency().await;
        
        let mut orders = self.orders.write().await;
        let mut results = Vec::new();
        
        for (id, order) in orders.iter_mut() {
            if symbol.is_none() || symbol == Some(&order.symbol) {
                if matches!(order.status, ExecutionStatus::Pending | ExecutionStatus::Submitted | ExecutionStatus::PartiallyFilled) {
                    order.status = ExecutionStatus::Cancelled;
                    results.push(CancelResult {
                        order_id: id.clone(),
                        exchange_order_id: Some(order.client_order_id.clone()),
                        status: CancelStatus::Cancelled,
                        cancelled_at: nano_timestamp() as u128,
                    });
                }
            }
        }
        
        Ok(results)
    }
    
    /// Edit/modify an existing order
    pub async fn edit_order(&self, params: &EditOrderParams) -> Result<ExecutionResult, ExecutionError> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(ExecutionError::Connection("Exchange disconnected".to_string()));
        }
        
        self.simulate_latency().await;
        
        let mut orders = self.orders.write().await;
        if let Some(order) = orders.get_mut(&params.order_id) {
            // Update order fields
            if let Some(vol) = params.volume {
                order.quantity = vol;
            }
            if let Some(price) = params.price {
                order.price = Some(price);
            }
            
            let now = nano_timestamp() as u128;
            
            Ok(ExecutionResult {
                order_id: params.order_id.clone(),
                exchange_order_id: Some(order.client_order_id.clone()),
                exchange: self.config.name.clone(),
                status: order.status.clone(),
                filled_quantity: order.filled_quantity,
                remaining_quantity: order.quantity - order.filled_quantity,
                avg_fill_price: order.price.unwrap_or(0.0),
                total_fees: 0.0,
                fills: order.fills.clone(),
                reject_reason: None,
                submitted_at: order.created_at,
                updated_at: now,
                latency_ns: 0,
                exchange_timestamp_ns: Some(now),
                exchange_sequence: None,
            })
        } else {
            Err(ExecutionError::OrderNotFound(params.order_id.clone()))
        }
    }
    
    /// Get the exchange name
    pub fn name(&self) -> &str {
        &self.config.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_signal(symbol: &str, action: SignalAction, qty: f64, price: f64) -> Signal {
        Signal {
            id: format!("test-{}", nano_timestamp()),
            strategy_id: "test".to_string(),
            symbol: symbol.to_string(),
            exchange: "MockExchange".to_string(),
            action,
            quantity: qty,
            price: Some(price),
            confidence: 0.9,
            timestamp: nano_timestamp() as u64,
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_basic_buy_order() {
        let config = MockExchangeConfig {
            fill_behavior: MockFillBehavior {
                full_fill_rate: 1.0,
                partial_fill_rate: 0.0,
                reject_rate: 0.0,
                ..Default::default()
            },
            ..Default::default()
        };
        let exchange = MockExchange::new(config);
        
        let signal = create_test_signal("BTC/USD", SignalAction::Buy, 0.1, 50000.0);
        let result = exchange.execute_signal(&signal).await.unwrap();
        
        assert_eq!(result.status, ExecutionStatus::Filled);
        assert_eq!(result.filled_quantity, 0.1);
        assert!(result.avg_fill_price > 0.0);
    }

    #[tokio::test]
    async fn test_partial_fill() {
        let config = MockExchangeConfig {
            fill_behavior: MockFillBehavior {
                full_fill_rate: 0.0,
                partial_fill_rate: 1.0,
                min_partial_pct: 0.5,
                max_partial_pct: 0.5,
                reject_rate: 0.0,
                ..Default::default()
            },
            ..Default::default()
        };
        let exchange = MockExchange::new(config);
        
        let signal = create_test_signal("BTC/USD", SignalAction::Buy, 1.0, 50000.0);
        let result = exchange.execute_signal(&signal).await.unwrap();
        
        assert_eq!(result.status, ExecutionStatus::PartiallyFilled);
        assert!((result.filled_quantity - 0.5).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_rejection() {
        let config = MockExchangeConfig {
            fill_behavior: MockFillBehavior {
                full_fill_rate: 0.0,
                partial_fill_rate: 0.0,
                reject_rate: 1.0,
                ..Default::default()
            },
            ..Default::default()
        };
        let exchange = MockExchange::new(config);
        
        let signal = create_test_signal("BTC/USD", SignalAction::Buy, 1.0, 50000.0);
        let result = exchange.execute_signal(&signal).await.unwrap();
        
        assert_eq!(result.status, ExecutionStatus::Rejected);
        assert!(result.reject_reason.is_some());
    }

    #[tokio::test]
    async fn test_disconnect_reconnect() {
        let exchange = MockExchange::default_instance();
        
        // Should work when connected
        let signal = create_test_signal("BTC/USD", SignalAction::Buy, 0.1, 50000.0);
        assert!(exchange.execute_signal(&signal).await.is_ok());
        
        // Disconnect
        exchange.disconnect();
        assert!(exchange.execute_signal(&signal).await.is_err());
        
        // Reconnect
        exchange.reconnect();
        assert!(exchange.execute_signal(&signal).await.is_ok());
    }

    #[tokio::test]
    async fn test_position_tracking() {
        let config = MockExchangeConfig {
            fill_behavior: MockFillBehavior {
                full_fill_rate: 1.0,
                partial_fill_rate: 0.0,
                reject_rate: 0.0,
                ..Default::default()
            },
            ..Default::default()
        };
        let exchange = MockExchange::new(config);
        
        // Buy
        let buy_signal = create_test_signal("BTC/USD", SignalAction::Buy, 1.0, 50000.0);
        exchange.execute_signal(&buy_signal).await.unwrap();
        
        let position = exchange.get_position("BTC/USD").await;
        assert!((position - 1.0).abs() < 0.001);
        
        // Sell
        let sell_signal = create_test_signal("BTC/USD", SignalAction::Sell, 0.5, 51000.0);
        exchange.execute_signal(&sell_signal).await.unwrap();
        
        let position = exchange.get_position("BTC/USD").await;
        assert!((position - 0.5).abs() < 0.001);
    }
}
