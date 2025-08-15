// Simplified Strategy Handler compatible with the ultra-low latency signal system

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use std::thread;
use std::error::Error;
use std::fmt;
use async_trait::async_trait;
use crossbeam::channel::Sender;
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use orderbook::{Orderbook, OrderbookMetrics};
use portfolio::CryptoWallet;
use ultra_signal::{Signal, SignalAction, OrderSide, ExchangeId};
use exchangemetricaggregator::ExchangeMetricsAggregator;
use smartorderrouter::{SmartOrderRouter, ExecutionUrgency, RoutingAlgorithm};
use tracing::{info, warn, error, debug};

/// Strategy handler specific errors
#[derive(Debug)]
pub enum StrategyError {
    LockError(String),
    SignalNotFound(String),
    StrategyNotFound(String),
    InvalidSignal(String),
    TimeError(String),
}

impl fmt::Display for StrategyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StrategyError::LockError(msg) => write!(f, "Lock error: {}", msg),
            StrategyError::SignalNotFound(id) => write!(f, "Signal not found: {}", id),
            StrategyError::StrategyNotFound(name) => write!(f, "Strategy not found: {}", name),
            StrategyError::InvalidSignal(msg) => write!(f, "Invalid signal: {}", msg),
            StrategyError::TimeError(msg) => write!(f, "Time error: {}", msg),
        }
    }
}

impl Error for StrategyError {}

/// Signal status tracking
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalStatus {
    Pending,
    Routed,
    Filled,
    Rejected,
    Cancelled,
}

/// Signal information storage
#[derive(Debug, Clone)]
pub struct SignalInfo {
    pub signal: Signal,
    pub status: SignalStatus,
    pub execution_price: Option<f64>,
    pub executed_quantity: Option<f64>,
    pub fees: Option<f64>,
    pub created_at: SystemTime,
}

/// Simple signal store for the strategy manager
#[derive(Debug)]
pub struct SignalStore {
    signals: RwLock<HashMap<String, SignalInfo>>,
    total_signals: RwLock<u64>,
}

impl SignalStore {
    pub fn new() -> Self {
        Self {
            signals: RwLock::new(HashMap::new()),
            total_signals: RwLock::new(0),
        }
    }
    
    pub fn store(&self, signal: Signal) -> Result<(), StrategyError> {
        let signal_info = SignalInfo {
            signal: signal.clone(),
            status: SignalStatus::Pending,
            execution_price: None,
            executed_quantity: None,
            fees: None,
            created_at: SystemTime::now(),
        };
        
        let mut signals = self.signals.write()
            .map_err(|e| StrategyError::LockError(format!("Failed to acquire signals write lock: {}", e)))?;
        signals.insert(signal.id.to_string(), signal_info);
        
        let mut total = self.total_signals.write()
            .map_err(|e| StrategyError::LockError(format!("Failed to acquire total_signals write lock: {}", e)))?;
        *total += 1;
        
        Ok(())
    }
    
    pub fn get(&self, signal_id: &str) -> Result<Option<SignalInfo>, StrategyError> {
        let signals = self.signals.read()
            .map_err(|e| StrategyError::LockError(format!("Failed to acquire signals read lock: {}", e)))?;
        Ok(signals.get(signal_id).cloned())
    }
    
    pub fn record_execution(&self, signal_id: &str, execution_price: f64, executed_qty: f64, fees: f64) -> Result<(), StrategyError> {
        let mut signals = self.signals.write()
            .map_err(|e| StrategyError::LockError(format!("Failed to acquire signals write lock: {}", e)))?;
        
        if let Some(signal_info) = signals.get_mut(signal_id) {
            signal_info.status = SignalStatus::Filled;
            signal_info.execution_price = Some(execution_price);
            signal_info.executed_quantity = Some(executed_qty);
            signal_info.fees = Some(fees);
            Ok(())
        } else {
            Err(StrategyError::SignalNotFound(signal_id.to_string()))
        }
    }
    
    pub fn get_total_signals(&self) -> Result<u64, StrategyError> {
        let total = self.total_signals.read()
            .map_err(|e| StrategyError::LockError(format!("Failed to acquire total_signals read lock: {}", e)))?;
        Ok(*total)
    }
}

/// Signal routing system
#[derive(Debug)]
pub struct SignalRouter {
    routes: HashMap<String, Sender<Signal>>,
    default_handler: Option<Sender<Signal>>,
}

impl SignalRouter {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            default_handler: None,
        }
    }
    
    pub fn add_route(&mut self, strategy_id: String, handler: Sender<Signal>) {
        self.routes.insert(strategy_id, handler);
    }
    
    pub fn add_default_handler(&mut self, handler: Sender<Signal>) {
        self.default_handler = Some(handler);
    }
    
    pub fn route(&self, signal: Signal) -> Result<(), String> {
        if let Some(handler) = self.routes.get(&signal.strategy_id.to_string()) {
            handler.send(signal).map_err(|e| format!("Failed to route signal: {}", e))
        } else if let Some(default) = &self.default_handler {
            default.send(signal).map_err(|e| format!("Failed to route to default handler: {}", e))
        } else {
            Err("No route found for signal".to_string())
        }
    }
}

/// Strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub symbols: Vec<String>,
    pub exchanges: Vec<String>,
    pub parameters: HashMap<String, serde_json::Value>,
}

/// Market data snapshot adapted for strategy handler
#[derive(Debug, Clone)]
pub struct MarketData {
    pub symbol: String,
    pub exchange: String,
    pub timestamp: u64,
    pub mid_price: f64,
    pub best_bid: f64,
    pub best_ask: f64,
    pub spread: f64,
    pub volume: f64,
}

impl From<&OrderbookMetrics> for MarketData {
    fn from(metrics: &OrderbookMetrics) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
            
        Self {
            symbol: "".to_string(), // Will be set externally
            exchange: "".to_string(), // Will be set externally
            timestamp,
            mid_price: metrics.mid_price,
            best_bid: metrics.best_bid,
            best_ask: metrics.best_ask,
            spread: metrics.spread,
            volume: 0.0, // Not available in OrderbookMetrics
        }
    }
}

/// Base strategy trait for the strategy handler
#[async_trait]
pub trait Strategy: Send + Sync {
    fn config(&self) -> &StrategyConfig;
    async fn initialize(&mut self) -> Result<(), Box<dyn Error>>;
    async fn generate_signals(&mut self, market_data: &MarketData) -> Result<Vec<Signal>, Box<dyn Error>>;
    fn update_state(&mut self, market_data: &MarketData);
    async fn shutdown(&mut self) -> Result<(), Box<dyn Error>>;
}

/// Simple market making strategy implementation
pub struct SimpleMarketMakingStrategy {
    config: StrategyConfig,
    last_quotes: HashMap<String, (f64, f64)>, // symbol -> (bid, ask)
}

impl SimpleMarketMakingStrategy {
    pub fn new(config: StrategyConfig) -> Self {
        Self {
            config,
            last_quotes: HashMap::new(),
        }
    }
    
    fn calculate_quotes(&self, market_data: &MarketData) -> (f64, f64) {
        let spread_pct = self.config.parameters
            .get("spread_pct")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.002); // 0.2% default spread
        
        let spread_amount = market_data.mid_price * spread_pct * 0.5;
        let bid = market_data.mid_price - spread_amount;
        let ask = market_data.mid_price + spread_amount;
        
        (bid, ask)
    }
}

#[async_trait]
impl Strategy for SimpleMarketMakingStrategy {
    fn config(&self) -> &StrategyConfig {
        &self.config
    }
    
    async fn initialize(&mut self) -> Result<(), Box<dyn Error>> {
        println!("Initializing simple market making strategy: {}", self.config.name);
        Ok(())
    }
    
    async fn generate_signals(&mut self, market_data: &MarketData) -> Result<Vec<Signal>, Box<dyn Error>> {
        let mut signals = Vec::new();
        
        let (new_bid, new_ask) = self.calculate_quotes(market_data);
        let last_quotes = self.last_quotes.get(&market_data.symbol).copied().unwrap_or((0.0, 0.0));
        
        // Check if quotes have changed significantly (0.01% threshold)
        let bid_changed = (new_bid - last_quotes.0).abs() / new_bid > 0.0001;
        let ask_changed = (new_ask - last_quotes.1).abs() / new_ask > 0.0001;
        
        if bid_changed || ask_changed || last_quotes.0 == 0.0 {
            let symbol_hash = ultra_signal::hash_symbol(&market_data.symbol);
            let exchange_id = ExchangeId::Binance; // Default to Binance
            let strategy_id = self.config.id.parse::<u16>().unwrap_or(1);
            let base_quantity = 0.01; // Base quantity for orders
            
            // Generate buy limit order
            if new_bid > 0.0 {
                let buy_signal = Signal::new(
                    strategy_id,
                    symbol_hash,
                    exchange_id,
                    SignalAction::BuyLimit,
                    base_quantity,
                    new_bid,
                );
                signals.push(buy_signal);
            }
            
            // Generate sell limit order
            if new_ask > 0.0 {
                let sell_signal = Signal::new(
                    strategy_id,
                    symbol_hash,
                    exchange_id,
                    SignalAction::SellLimit,
                    base_quantity,
                    new_ask,
                );
                signals.push(sell_signal);
            }
            
            self.last_quotes.insert(market_data.symbol.clone(), (new_bid, new_ask));
        }
        
        Ok(signals)
    }
    
    fn update_state(&mut self, _market_data: &MarketData) {
        // State is updated in generate_signals
    }
    
    async fn shutdown(&mut self) -> Result<(), Box<dyn Error>> {
        println!("Shutting down simple market making strategy: {}", self.config.name);
        Ok(())
    }
}

/// Simplified strategy manager
pub struct StrategyManager {
    strategies: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Strategy>>>>>>,
    orderbooks: DashMap<(String, String), Arc<RwLock<Orderbook>>>, // Use RwLock for consistency
    signal_store: Arc<SignalStore>,
    signal_router: Arc<Mutex<SignalRouter>>,
    running: Arc<Mutex<bool>>,
}

impl StrategyManager {
    pub fn new(orderbooks: DashMap<(String, String), Arc<RwLock<Orderbook>>>) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            strategies: Arc::new(RwLock::new(HashMap::new())),
            orderbooks,
            signal_store: Arc::new(SignalStore::new()),
            signal_router: Arc::new(Mutex::new(SignalRouter::new())),
            running: Arc::new(Mutex::new(false)),
        })
    }
    
    pub fn add_strategy(&self, mut strategy: Box<dyn Strategy>) -> Result<(), Box<dyn Error>> {
        let config = strategy.config().clone();
        
        // Initialize the strategy
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(strategy.initialize())?;
        
        let mut strategies = self.strategies.write()
            .map_err(|e| format!("Failed to acquire strategies write lock: {}", e))?;
        strategies.insert(config.id.clone(), Arc::new(Mutex::new(strategy)));
        
        info!("Added strategy: {} ({})", config.name, config.id);
        Ok(())
    }
    
    pub fn add_signal_route(&self, strategy_id: String, handler: Sender<Signal>) {
        if let Ok(mut router) = self.signal_router.lock() {
            router.add_route(strategy_id, handler);
        } else {
            error!("Failed to acquire signal router lock for adding route");
        }
    }
    
    pub fn signal_store(&self) -> Arc<SignalStore> {
        Arc::clone(&self.signal_store)
    }
    
    /// Process market data from orderbook and generate signals
    pub fn process_market_data(&self, symbol: &str, exchange: &str) -> Result<Vec<Signal>, Box<dyn Error>> {
        let mut all_signals = Vec::new();
        
        // Get market data from orderbook
        let market_data = self.get_market_data(symbol, exchange)?;
        
        // Process through all enabled strategies
        let strategies = self.strategies.read()
            .map_err(|e| format!("Failed to acquire strategies read lock: {}", e))?;
        let runtime = tokio::runtime::Runtime::new()?;
        
        for (strategy_id, strategy_arc) in strategies.iter() {
            let mut strategy = strategy_arc.lock()
                .map_err(|e| format!("Failed to acquire strategy lock for {}: {}", strategy_id, e))?;
            if strategy.config().enabled {
                match runtime.block_on(strategy.generate_signals(&market_data)) {
                    Ok(mut signals) => {
                        // Store and route signals
                        for signal in &signals {
                            if let Err(e) = self.signal_store.store(signal.clone()) {
                                error!("Failed to store signal: {}", e);
                            }
                            
                            if let Ok(router) = self.signal_router.lock() {
                                if let Err(e) = router.route(signal.clone()) {
                                    error!("Failed to route signal: {}", e);
                                }
                            } else {
                                error!("Failed to acquire signal router lock for routing");
                            }
                        }
                        
                        all_signals.append(&mut signals);
                        strategy.update_state(&market_data);
                    }
                    Err(e) => {
                        error!("Strategy {} error: {}", strategy_id, e);
                    }
                }
            }
        }
        
        Ok(all_signals)
    }
    
    fn get_market_data(&self, symbol: &str, exchange: &str) -> Result<MarketData, Box<dyn Error>> {
        let key = (symbol.to_string(), exchange.to_string());
        
        if let Some(orderbook_arc) = self.orderbooks.get(&key) {
            let orderbook = orderbook_arc.read()
                .map_err(|e| format!("Failed to acquire orderbook read lock for {}/{}: {}", symbol, exchange, e))?;
            let metrics = orderbook.metrics()?;
            
            let mut market_data = MarketData::from(&metrics);
            market_data.symbol = symbol.to_string();
            market_data.exchange = exchange.to_string();
            
            Ok(market_data)
        } else {
            Err(format!("No orderbook found for {}/{}", symbol, exchange).into())
        }
    }
    
    /// Report signal execution back to the store
    pub fn report_execution(&self, signal_id: &str, execution_price: f64, executed_qty: f64, fees: f64) {
        if let Err(e) = self.signal_store.record_execution(signal_id, execution_price, executed_qty, fees) {
            eprintln!("Failed to record execution for signal {}: {}", signal_id, e);
        }
    }
}

/// Factory function to create strategies from configuration
pub fn create_strategy(config: StrategyConfig) -> Result<Box<dyn Strategy>, Box<dyn Error>> {
    // For now, only support simple market making
    Ok(Box::new(SimpleMarketMakingStrategy::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_strategy_manager_creation() {
        let orderbooks = DashMap::new();
        let manager = StrategyManager::new(orderbooks);
        assert!(manager.is_ok());
    }
    
    #[tokio::test]
    async fn test_strategy_creation_and_addition() {
        let mut params = HashMap::new();
        params.insert("spread_pct".to_string(), serde_json::json!(0.002));
        
        let config = StrategyConfig {
            id: "test-strategy-1".to_string(),
            name: "Test Market Making".to_string(),
            enabled: true,
            symbols: vec!["BTC/USD".to_string()],
            exchanges: vec!["binance".to_string()],
            parameters: params,
        };
        
        let strategy = create_strategy(config);
        assert!(strategy.is_ok());
        
        let orderbooks = DashMap::new();
        let manager = StrategyManager::new(orderbooks).expect("Failed to create strategy manager");
        let result = manager.add_strategy(strategy.expect("Strategy creation failed"));
        assert!(result.is_ok());
    }
}
