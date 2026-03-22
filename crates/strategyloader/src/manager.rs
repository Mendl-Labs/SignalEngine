//! Strategy Manager - orchestrates strategy loading and execution
//!
//! This module integrates strategyloader with SignalEngine's runtime,
//! managing the lifecycle of loaded strategies.
//!
//! # Signal Flow
//! 1. Market data arrives via `on_market_data()`
//! 2. Router determines which strategies are interested
//! 3. Each strategy generates signals via `PortfolioStrategy::on_market_data()`
//! 4. Signals are stored in the signal store
//! 5. Signals are routed to registered handlers (e.g., execution handler)

use std::sync::Arc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use crossbeam::channel::Sender;
use uuid::Uuid;

// Ultra-logger integration
use signalengine::{SignalEngineLogger, TradingContext};

use crate::{
    StrategyLoader, StrategyInstance, StrategyFactory,
    PortfolioStrategy, StrategyState, StrategyStateRegistry,
    MarketDataRouter, MarketDataEvent, Signal,
};

/// Signal status for tracking lifecycle
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalStatus {
    Pending,
    Routed,
    Sent,
    Filled,
    Executed,
    Rejected,
    Cancelled,
}

/// Signal info with execution tracking
#[derive(Debug, Clone)]
pub struct SignalInfo {
    pub signal: Signal,
    pub status: SignalStatus,
    pub execution_price: Option<f64>,
    pub executed_quantity: Option<f64>,
    pub fees: Option<f64>,
    pub created_at_ms: i64,
}

/// Signal store for tracking all generated signals
pub struct SignalStore {
    signals: parking_lot::RwLock<HashMap<String, SignalInfo>>,
    total_signals: AtomicU64,
}

impl Default for SignalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalStore {
    pub fn new() -> Self {
        Self {
            signals: parking_lot::RwLock::new(HashMap::new()),
            total_signals: AtomicU64::new(0),
        }
    }
    
    /// Store a signal
    pub fn store(&self, signal: Signal) {
        let signal_id = format!("{}-{}", signal.strategy_id, signal.timestamp_ms);
        let info = SignalInfo {
            signal,
            status: SignalStatus::Pending,
            execution_price: None,
            executed_quantity: None,
            fees: None,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        
        let mut signals = self.signals.write();
        signals.insert(signal_id, info);
        self.total_signals.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Get a signal by ID
    pub fn get(&self, signal_id: &str) -> Option<SignalInfo> {
        let signals = self.signals.read();
        signals.get(signal_id).cloned()
    }
    
    /// Record execution result
    pub fn record_execution(
        &self,
        signal_id: &str,
        execution_price: f64,
        executed_qty: f64,
        fees: f64,
    ) -> bool {
        let mut signals = self.signals.write();
        if let Some(info) = signals.get_mut(signal_id) {
            info.status = SignalStatus::Filled;
            info.execution_price = Some(execution_price);
            info.executed_quantity = Some(executed_qty);
            info.fees = Some(fees);
            true
        } else {
            false
        }
    }
    
    /// Get total signal count
    pub fn total_count(&self) -> u64 {
        self.total_signals.load(Ordering::Relaxed)
    }
    
    /// Get pending signals
    pub fn get_pending(&self) -> Vec<SignalInfo> {
        let signals = self.signals.read();
        signals.values()
            .filter(|s| s.status == SignalStatus::Pending)
            .cloned()
            .collect()
    }
}

/// Central manager for all loaded strategies
/// 
/// This is the unified strategy manager that:
/// - Loads strategies from the database via `StrategyLoader`
/// - Routes market data to interested strategies
/// - Stores generated signals for tracking
/// - Routes signals to execution handlers
pub struct StrategyManager {
    /// Loaded strategy executors
    strategies: RwLock<HashMap<Uuid, Arc<dyn PortfolioStrategy>>>,
    
    /// Strategy state tracking
    state_registry: RwLock<StrategyStateRegistry>,
    
    /// Market data router
    router: RwLock<MarketDataRouter>,
    
    /// Strategy loader (for reloading)
    loader: Arc<dyn StrategyLoader>,
    
    /// Signal store for tracking all generated signals
    signal_store: Arc<SignalStore>,
    
    /// Signal handlers for routing signals to execution
    signal_handlers: parking_lot::RwLock<Vec<Sender<Signal>>>,
    
    /// Performance metrics
    signals_generated: AtomicU64,
    signals_routed: AtomicU64,
    
    /// Ultra-logger instance
    logger: Arc<SignalEngineLogger>,
}

impl StrategyManager {
    /// Create new strategy manager with loader
    pub async fn new(loader: Arc<dyn StrategyLoader>) -> Self {
        let logger = Arc::new(SignalEngineLogger::new("StrategyManager").await);
        
        logger.info("Initializing StrategyManager").await;
        
        Self {
            strategies: RwLock::new(HashMap::new()),
            state_registry: RwLock::new(StrategyStateRegistry::new()),
            router: RwLock::new(MarketDataRouter::new()),
            loader,
            signal_store: Arc::new(SignalStore::new()),
            signal_handlers: parking_lot::RwLock::new(Vec::new()),
            signals_generated: AtomicU64::new(0),
            signals_routed: AtomicU64::new(0),
            logger,
        }
    }
    
    /// Register a signal handler for routing signals to execution
    pub async fn add_signal_handler(&self, handler: Sender<Signal>) {
        let mut handlers = self.signal_handlers.write();
        handlers.push(handler);
        self.logger.info(&format!("Added signal handler (total: {})", handlers.len())).await;
    }
    
    /// Get the signal store for external access
    pub fn signal_store(&self) -> Arc<SignalStore> {
        Arc::clone(&self.signal_store)
    }
}

/// Performance metrics for the strategy manager
#[derive(Debug, Clone)]
pub struct ManagerMetrics {
    pub signals_generated: u64,
    pub signals_routed: u64,
    pub total_signals_stored: u64,
}

impl StrategyManager {
    /// Get metrics
    pub fn get_metrics(&self) -> ManagerMetrics {
        ManagerMetrics {
            signals_generated: self.signals_generated.load(Ordering::Relaxed),
            signals_routed: self.signals_routed.load(Ordering::Relaxed),
            total_signals_stored: self.signal_store.total_count(),
        }
    }
    
    /// Load all active strategies from the configured source
    pub async fn load_strategies(&self) -> Result<usize, crate::error::StrategyLoaderError> {
        let ctx = TradingContext::new("StrategyManager")
            .with_operation("load_strategies");
        
        self.logger.info_ctx("Loading active strategies from database", ctx.clone()).await;
        
        let start = std::time::Instant::now();
        let instances = self.loader.load_active_strategies().await?;
        let load_duration_ms = start.elapsed().as_millis();
        
        self.logger.debug(&format!(
            "Fetched {} strategy instances in {}ms",
            instances.len(),
            load_duration_ms
        )).await;
        
        let mut count = 0;
        
        for instance in instances {
            if self.register_strategy(instance).await {
                count += 1;
            }
        }
        
        self.logger.info_ctx(
            &format!("Strategy loading complete: {} loaded in {}ms", count, start.elapsed().as_millis()),
            ctx
        ).await;
        
        Ok(count)
    }
    
    /// Register a single strategy
    pub async fn register_strategy(&self, instance: StrategyInstance) -> bool {
        let strategy_id = instance.id;
        let strategy_name = instance.name.clone();
        let strategy_type = format!("{:?}", instance.strategy_type);
        let asset_count = instance.assets.len();
        let symbols: Vec<String> = instance.assets.iter().map(|a| a.symbol.clone()).collect();
        
        self.logger.debug(&format!(
            "Registering strategy '{}' ({}) type={} assets={}",
            strategy_name, strategy_id, strategy_type, asset_count
        )).await;
        
        // Create executor from instance
        let Some(executor) = StrategyFactory::create(instance.clone()) else {
            self.logger.warn(&format!(
                "Failed to create executor for strategy '{}' ({})",
                strategy_name, strategy_id
            )).await;
            return false;
        };
        
        // Create state for strategy (registered via state_registry)
        let _state = StrategyState::new(&instance);
        
        // Register with all components
        {
            let mut strategies = self.strategies.write().await;
            strategies.insert(strategy_id, executor);
        }
        
        {
            let mut registry = self.state_registry.write().await;
            registry.register(&instance);
        }
        
        {
            let mut router = self.router.write().await;
            router.register_strategy(&instance);
        }
        
        let ctx = TradingContext::new("StrategyManager")
            .with_operation("register");
        self.logger.info_ctx(
            &format!(
                "Strategy registered: '{}' ({}) type={} symbols={:?}",
                strategy_name, strategy_id, strategy_type, symbols
            ),
            ctx
        ).await;
        
        true
    }
    
    /// Unregister a strategy
    pub async fn unregister_strategy(&self, strategy_id: Uuid) -> bool {
        self.logger.debug(&format!("Unregistering strategy {}", strategy_id)).await;
        
        let found = {
            let mut strategies = self.strategies.write().await;
            strategies.remove(&strategy_id).is_some()
        };
        
        if found {
            let mut registry = self.state_registry.write().await;
            registry.unregister(strategy_id);
            
            let mut router = self.router.write().await;
            router.unregister_strategy(strategy_id);
            
            self.logger.info(&format!("Strategy {} unregistered successfully", strategy_id)).await;
        } else {
            self.logger.warn(&format!("Strategy {} not found for unregistration", strategy_id)).await;
        }
        
        found
    }
    
    /// Reload a specific strategy
    pub async fn reload_strategy(&self, strategy_id: Uuid) -> Result<bool, crate::error::StrategyLoaderError> {
        self.logger.info(&format!("Reloading strategy {}", strategy_id)).await;
        
        let start = std::time::Instant::now();
        
        // First unregister the old one
        self.unregister_strategy(strategy_id).await;
        
        // Load fresh from source
        let instance = self.loader.load_strategy(strategy_id).await?;
        
        match instance {
            Some(inst) => {
                let success = self.register_strategy(inst).await;
                self.logger.info(&format!(
                    "Strategy {} reload complete: success={} duration={}ms",
                    strategy_id, success, start.elapsed().as_millis()
                )).await;
                Ok(success)
            },
            None => {
                self.logger.warn(&format!("Strategy {} not found in loader", strategy_id)).await;
                Ok(false)
            },
        }
    }
    
    /// Process a market data event, routing to subscribed strategies
    /// Also stores and routes generated signals
    pub async fn on_market_data(&self, event: &MarketDataEvent) -> Vec<Signal> {
        let start = std::time::Instant::now();
        let mut all_signals = Vec::new();
        
        // Get strategy IDs interested in this event
        let strategy_ids = {
            let router = self.router.read().await;
            router.route(&event.symbol, &event.exchange)
        };
        
        // Process each interested strategy
        let strategies = self.strategies.read().await;
        let mut state_registry = self.state_registry.write().await;
        
        for strategy_id in strategy_ids {
            let Some(executor) = strategies.get(&strategy_id) else {
                continue;
            };
            
            let Some(state) = state_registry.get_mut(strategy_id) else {
                continue;
            };
            
            // Generate signals
            let signals = executor.on_market_data(event, state);
            
            // Log each signal generated
            for signal in &signals {
                let direction_str = if signal.direction > 0.0 { "BUY" } else { "SELL" };
                self.logger.log_signal(
                    signal.timestamp_ms as u64,
                    &signal.symbol,
                    direction_str,
                    &format!("confidence={:.2}", signal.confidence)
                ).await;
            }
            
            all_signals.extend(signals);
        }
        
        // Store and route all generated signals
        let mut routed_count = 0;
        let mut route_errors = 0;
        
        for signal in &all_signals {
            self.signals_generated.fetch_add(1, Ordering::Relaxed);
            
            // Store signal
            self.signal_store.store(signal.clone());
            
            // Route to all handlers
            let handlers = self.signal_handlers.read();
            for handler in handlers.iter() {
                if let Err(e) = handler.try_send(signal.clone()) {
                    self.logger.warn(&format!(
                        "Failed to route signal {} to handler: {}",
                        signal.strategy_id, e
                    )).await;
                    route_errors += 1;
                } else {
                    self.signals_routed.fetch_add(1, Ordering::Relaxed);
                    routed_count += 1;
                }
            }
        }
        
        let elapsed_us = start.elapsed().as_micros();
        
        if !all_signals.is_empty() {
            self.logger.debug(&format!(
                "Market data {} processed: {} signals, {} routed, {} errors, {}μs",
                event.symbol, all_signals.len(), routed_count, route_errors, elapsed_us
            )).await;
        }
        
        all_signals
    }
    
    /// Record a fill for a strategy
    pub async fn on_fill(
        &self,
        strategy_id: Uuid,
        symbol: &str,
        exchange: &str,
        quantity: f64,
        price: f64,
        is_buy: bool,
    ) {
        let side = if is_buy { "BUY" } else { "SELL" };
        let notional = quantity * price;
        
        let ctx = TradingContext::new("StrategyManager")
            .with_operation("fill")
            .with_symbol(symbol)
            .with_exchange(exchange);
        
        self.logger.info_ctx(
            &format!("Processing fill: {} {} {} @ {} (notional: {:.2})", side, quantity, symbol, price, notional),
            ctx.clone()
        ).await;
        
        let strategies = self.strategies.read().await;
        let mut state_registry = self.state_registry.write().await;
        
        if let (Some(executor), Some(state)) = (
            strategies.get(&strategy_id),
            state_registry.get_mut(strategy_id)
        ) {
            executor.on_fill(symbol, exchange, quantity, price, is_buy, state);
            
            self.logger.debug(&format!(
                "Fill processed for strategy {}, state updated",
                strategy_id
            )).await;
        } else {
            self.logger.warn(&format!(
                "Cannot process fill - strategy {} not found",
                strategy_id
            )).await;
        }
    }
    
    /// Get required market data subscriptions
    pub async fn get_required_subscriptions(&self) -> Vec<(String, String)> {
        let router = self.router.read().await;
        router.get_required_subscriptions()
    }
    
    /// Get strategy state (for monitoring/debugging)
    pub async fn get_strategy_state(&self, strategy_id: Uuid) -> Option<StrategyState> {
        let registry = self.state_registry.read().await;
        registry.get(strategy_id).cloned()
    }
    
    /// Get all strategy states (for monitoring)
    pub async fn get_all_states(&self) -> Vec<(Uuid, StrategyState)> {
        let registry = self.state_registry.read().await;
        registry.iter()
            .map(|(id, state)| (*id, state.clone()))
            .collect()
    }
    
    /// Reset daily metrics for all strategies (call at start of trading day)
    pub async fn reset_daily_metrics(&self) {
        let mut registry = self.state_registry.write().await;
        registry.reset_all_daily();
        self.logger.info("Reset daily metrics for all strategies").await;
    }
    
    /// Check and update cooldowns for all strategies
    pub async fn check_cooldowns(&self) {
        let mut registry = self.state_registry.write().await;
        registry.check_all_cooldowns();
    }
    
    /// Get count of loaded strategies
    pub async fn strategy_count(&self) -> usize {
        let strategies = self.strategies.read().await;
        strategies.len()
    }
    
    /// List all loaded strategy IDs and names
    pub async fn list_strategies(&self) -> Vec<(Uuid, String)> {
        let strategies = self.strategies.read().await;
        strategies.iter()
            .map(|(id, executor)| (*id, executor.name().to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{StrategyType, MomentumParams, StrategyParameters, TradingAsset, AssetRiskLimits, PortfolioRiskLimits};
    
    // Mock loader for testing
    struct MockLoader {
        strategies: Vec<StrategyInstance>,
    }
    
    #[async_trait::async_trait]
    impl StrategyLoader for MockLoader {
        async fn load_active_strategies(&self) -> crate::error::Result<Vec<StrategyInstance>> {
            Ok(self.strategies.clone())
        }
        
        async fn load_strategy(&self, id: Uuid) -> crate::error::Result<Option<StrategyInstance>> {
            Ok(self.strategies.iter().find(|s| s.id == id).cloned())
        }
        
        async fn load_strategy_by_name(&self, name: &str, _version: &str) -> crate::error::Result<Option<StrategyInstance>> {
            Ok(self.strategies.iter().find(|s| s.name == name).cloned())
        }
    }
    
    fn create_test_instance() -> StrategyInstance {
        StrategyInstance {
            id: Uuid::new_v4(),
            name: "Test Strategy".into(),
            strategy_type: StrategyType::Custom,
            version: "1.0.0".into(),
            parameters: StrategyParameters::Momentum(MomentumParams::default()),
            assets: vec![TradingAsset {
                symbol: "BTC/USD".into(),
                exchange: "kraken".into(),
                weight: 1.0,
                risk_limits: AssetRiskLimits::default(),
            }],
            portfolio_risk: PortfolioRiskLimits::default(),
            enabled: true,
            description: None,
            python_source: None,
            metadata: Default::default(),
        }
    }
    
    #[tokio::test]
    async fn test_strategy_manager_load() {
        let instance = create_test_instance();
        let loader = MockLoader {
            strategies: vec![instance],
        };
        
        let manager = StrategyManager::new(Arc::new(loader)).await;
        let count = manager.load_strategies().await.unwrap();
        
        assert_eq!(count, 1);
        assert_eq!(manager.strategy_count().await, 1);
    }
    
    #[tokio::test]
    async fn test_strategy_manager_market_data() {
        let instance = create_test_instance();
        let loader = MockLoader {
            strategies: vec![instance],
        };
        
        let manager = StrategyManager::new(Arc::new(loader)).await;
        manager.load_strategies().await.unwrap();
        
        // Send market data - need enough history before signals are generated
        for i in 0..100 {
            let event = MarketDataEvent {
                symbol: "BTC/USD".into(),
                exchange: "kraken".into(),
                timestamp_ms: i * 1000,
                price: 50000.0 + (i as f64 * 10.0), // Gradual price increase
                volume: Some(100.0),
                bid: None,
                ask: None,
            };
            
            let _signals = manager.on_market_data(&event).await;
        }
        
        // Check state was updated
        let states = manager.get_all_states().await;
        assert_eq!(states.len(), 1);
        
        let (_, state) = &states[0];
        let asset_state = state.get_asset("BTC/USD", "kraken");
        assert!(asset_state.is_some());
        assert!(asset_state.unwrap().price_history.len() > 0);
    }
}
