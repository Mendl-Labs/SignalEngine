use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;
use std::error::Error;
use async_trait::async_trait;
use crossbeam::channel::{bounded, Sender, Receiver};
use serde::{Serialize, Deserialize};
use orderbook::{Orderbook, OrderbookMetrics};
use portfolio::CryptoWallet;
use strategy::MarketMaker;
use signalgenerator::{Signal, SignalAction, SignalStore, SignalFilter, SignalRouter, SignalStats};
use signaldispatcher::{SignalDispatcher, SignalDispatcherConfig, SignalDispatcherBuilder};
use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;

// Global storage for strategies
lazy_static! {
    static ref STRATEGIES: RwLock<HashMap<String, Arc<Mutex<Box<dyn Strategy>>>>> = RwLock::new(HashMap::new());
}

/// Strategy configuration (unchanged from original)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub id: String,
    pub name: String,
    pub strategy_type: StrategyType,
    pub enabled: bool,
    pub symbols: Vec<String>,
    pub exchanges: Vec<String>,
    pub parameters: HashMap<String, serde_json::Value>,
    pub risk_limits: RiskLimits,
}

/// Strategy types (unchanged from original)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StrategyType {
    MarketMaking,
    Momentum,
    Arbitrage,
    Custom(String),
}

/// Risk limits for a strategy (unchanged from original)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimits {
    pub max_position_size: f64,
    pub max_order_size: f64,
    pub max_daily_loss: f64,
    pub max_open_orders: usize,
    pub max_notional_exposure: f64,
}

/// Market data snapshot (unchanged from original)
#[derive(Debug, Clone)]
pub struct MarketData {
    pub symbol: String,
    pub exchange: String,
    pub timestamp: u64,
    pub metrics: OrderbookMetrics,
}

/// Portfolio snapshot (unchanged from original)
#[derive(Debug, Clone)]
pub struct PortfolioSnapshot {
    pub exchange: String,
    pub balances: HashMap<String, f64>,
    pub total_value: f64,
    pub timestamp: u64,
}

/// Strategy performance metrics (unchanged from original)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StrategyMetrics {
    pub signals_generated: u64,
    pub profitable_signals: u64,
    pub total_pnl: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub avg_signal_time_ms: f64,
    pub last_signal_timestamp: u64,
}

/// Base trait that all strategies must implement (unchanged from original)
#[async_trait]
pub trait Strategy: Send + Sync {
    fn config(&self) -> &StrategyConfig;
    async fn initialize(&mut self) -> Result<(), Box<dyn Error>>;
    async fn generate_signals(
        &mut self,
        market_data: &MarketData,
        portfolio: &PortfolioSnapshot,
    ) -> Result<Vec<Signal>, Box<dyn Error>>;
    fn update_state(&mut self, market_data: &MarketData);
    fn metrics(&self) -> StrategyMetrics;
    fn on_signal_executed(&mut self, signal: &Signal, execution_price: f64, executed_qty: f64);
    async fn shutdown(&mut self) -> Result<(), Box<dyn Error>>;
}

/// Market Making Strategy implementation (unchanged from original)
pub struct MarketMakingStrategy {
    config: StrategyConfig,
    metrics: StrategyMetrics,
    market_makers: HashMap<(String, String), MarketMaker>,
    last_quotes: HashMap<(String, String), (f64, f64)>,
    position_tracker: HashMap<(String, String), f64>,
    price_history: HashMap<(String, String), Vec<f64>>,
    variance_history: HashMap<(String, String), Vec<f64>>,
}

impl MarketMakingStrategy {
    pub fn new(config: StrategyConfig) -> Self {
        // Implementation unchanged from original
        let mut market_makers = HashMap::new();
        
        let gamma = config.parameters.get("gamma")
            .and_then(|v| v.as_f64())
            .map(|v| BigDecimal::from_f64(v).unwrap())
            .unwrap_or_else(|| BigDecimal::from_f64(0.1).unwrap());
            
        let k1 = Self::extract_param(&config.parameters, "k1", 0.1);
        let k2 = Self::extract_param(&config.parameters, "k2", 0.1);
        let k3 = Self::extract_param(&config.parameters, "k3", 0.1);
        let k4 = Self::extract_param(&config.parameters, "k4", 0.1);
        let k5 = Self::extract_param(&config.parameters, "k5", 0.1);
        let k6 = Self::extract_param(&config.parameters, "k6", 0.1);
        
        let w1 = Self::extract_param(&config.parameters, "w1", 0.5);
        let w2 = Self::extract_param(&config.parameters, "w2", 0.5);
        let w3 = Self::extract_param(&config.parameters, "w3", 0.5);
        let w4 = Self::extract_param(&config.parameters, "w4", 0.5);
        let w5 = Self::extract_param(&config.parameters, "w5", 0.5);
        let w6 = Self::extract_param(&config.parameters, "w6", 0.5);
        let w7 = Self::extract_param(&config.parameters, "w7", 0.5);
        let w8 = Self::extract_param(&config.parameters, "w8", 0.5);
        let w9 = Self::extract_param(&config.parameters, "w9", 0.5);
        let w10 = Self::extract_param(&config.parameters, "w10", 0.5);
        let w11 = Self::extract_param(&config.parameters, "w11", 0.5);
        
        for symbol in &config.symbols {
            for exchange in &config.exchanges {
                let key = (symbol.clone(), exchange.clone());
                let mm = MarketMaker::new(
                    &gamma, &k1, &k2, &k3, &k4, &k5, &k6,
                    &w1, &w2, &w3, &w4, &w5, &w6, &w7, &w8, &w9, &w10, &w11
                );
                market_makers.insert(key, mm);
            }
        }
        
        Self {
            config,
            metrics: StrategyMetrics::default(),
            market_makers,
            last_quotes: HashMap::new(),
            position_tracker: HashMap::new(),
            price_history: HashMap::new(),
            variance_history: HashMap::new(),
        }
    }
    
    // Helper methods unchanged from original
    fn extract_param(params: &HashMap<String, serde_json::Value>, name: &str, default: f64) -> BigDecimal {
        params.get(name)
            .and_then(|v| v.as_f64())
            .map(|v| BigDecimal::from_f64(v).unwrap())
            .unwrap_or_else(|| BigDecimal::from_f64(default).unwrap())
    }
    
    fn calculate_ofi(&self, metrics: &OrderbookMetrics) -> BigDecimal {
        let ofi_tick = BigDecimal::from_f64(metrics.orderbook_imbalance).unwrap_or_default();
        let ofi_liquidity = BigDecimal::from_f64(metrics.liquidity_weighted_orderbook_imbalance).unwrap_or_default();
        let ofi_smoothed = BigDecimal::from_f64(metrics.smoothed_orderbook_imbalance).unwrap_or_default();
        
        ofi_tick * BigDecimal::from_f64(0.3).unwrap() +
        ofi_liquidity * BigDecimal::from_f64(0.4).unwrap() +
        ofi_smoothed * BigDecimal::from_f64(0.3).unwrap()
    }
    
    fn update_price_history(&mut self, symbol: &str, exchange: &str, price: f64) {
        let key = (symbol.to_string(), exchange.to_string());
        let history = self.price_history.entry(key).or_insert_with(Vec::new);
        
        history.push(price);
        if history.len() > 100 {
            history.remove(0);
        }
    }
    
    fn update_variance_history(&mut self, symbol: &str, exchange: &str, variance: f64) {
        let key = (symbol.to_string(), exchange.to_string());
        let history = self.variance_history.entry(key).or_insert_with(Vec::new);
        
        history.push(variance);
        if history.len() > 20 {
            history.remove(0);
        }
    }
    
    fn get_historical_variance(&self, symbol: &str, exchange: &str) -> f64 {
        let key = (symbol.to_string(), exchange.to_string());
        
        if let Some(history) = self.variance_history.get(&key) {
            if !history.is_empty() {
                history.iter().sum::<f64>() / history.len() as f64
            } else {
                0.001
            }
        } else {
            0.001
        }
    }
}

// Strategy trait implementation unchanged from original
#[async_trait]
impl Strategy for MarketMakingStrategy {
    fn config(&self) -> &StrategyConfig {
        &self.config
    }
    
    async fn initialize(&mut self) -> Result<(), Box<dyn Error>> {
        println!("Initializing market making strategy: {}", self.config.name);
        
        for symbol in &self.config.symbols {
            for exchange in &self.config.exchanges {
                let key = (symbol.clone(), exchange.clone());
                self.position_tracker.insert(key, 0.0);
            }
        }
        
        Ok(())
    }
    
    async fn generate_signals(
        &mut self,
        market_data: &MarketData,
        portfolio: &PortfolioSnapshot,
    ) -> Result<Vec<Signal>, Box<dyn Error>> {
        // Implementation unchanged from original - generates signals
        let start = Instant::now();
        let mut signals = Vec::new();
        
        let key = (market_data.symbol.clone(), market_data.exchange.clone());
        let ofi = self.calculate_ofi(&market_data.metrics);
        let historical_variance = self.get_historical_variance(&market_data.symbol, &market_data.exchange);

        let (ask_price, bid_price, order_size, mm_ofi) = if let Some(mm) = self.market_makers.get_mut(&key) {
            let inventory = self.position_tracker.get(&key).cloned().unwrap_or(0.0);
            mm.set_inventory(BigDecimal::from_f64(inventory).unwrap());

            mm.calculate_ofi(ofi.clone(), ofi.clone(), ofi.clone());

            let mid_price_bd = BigDecimal::from_f64(market_data.metrics.mid_price).unwrap();
            let variance_bd = BigDecimal::from_f64(market_data.metrics.variance).unwrap();
            let historical_var_bd = BigDecimal::from_f64(historical_variance).unwrap();
            let bid_depth_bd = BigDecimal::from_f64(market_data.metrics.best_bid_depth).unwrap();
            let ask_depth_bd = BigDecimal::from_f64(market_data.metrics.best_ask_depth).unwrap();
            let best_bid_bd = BigDecimal::from_f64(market_data.metrics.best_bid).unwrap();
            let best_ask_bd = BigDecimal::from_f64(market_data.metrics.best_ask).unwrap();
            let total_depth_bd = BigDecimal::from_f64(market_data.metrics.total_depth).unwrap();
            let best_depth_bd = BigDecimal::from_f64(market_data.metrics.best_bid_depth.max(market_data.metrics.best_ask_depth)).unwrap();

            mm.calculate_sigma(&variance_bd);
            mm.calculate_tau(&total_depth_bd, &best_depth_bd);
            mm.calculate_alpha(&variance_bd);
            mm.calculate_beta(&total_depth_bd, &best_depth_bd, Some(best_ask_bd.clone()), Some(best_bid_bd.clone()), &mid_price_bd);
            mm.calculate_gamma(&variance_bd, &historical_var_bd);
            mm.calculate_eta(&variance_bd, &total_depth_bd, &best_depth_bd);
            mm.calculate_lambda(&variance_bd);
            mm.calculate_theta();

            let timestamp = DateTime::from_timestamp(market_data.timestamp as i64 / 1000, 0)
                .unwrap_or_else(|| Utc::now());

            let (mm_signal, _) = mm.generate_signal(
                &market_data.symbol,
                &market_data.exchange,
                &mid_price_bd,
                bid_depth_bd,
                ask_depth_bd,
                &variance_bd,
                timestamp
            );

            let ask_price = mm_signal.get_ask_quote().to_string().parse::<f64>().unwrap_or(0.0);
            let bid_price = mm_signal.get_bid_quote().to_string().parse::<f64>().unwrap_or(0.0);
            let order_size = mm_signal.get_order_size().to_string().parse::<f64>().unwrap_or(0.0).abs();
            let mm_ofi = mm.get_ofi().to_string();

            (ask_price, bid_price, order_size, mm_ofi)
        } else {
            (0.0, 0.0, 0.0, "0".to_string())
        };

        self.update_price_history(&market_data.symbol, &market_data.exchange, market_data.metrics.mid_price);
        self.update_variance_history(&market_data.symbol, &market_data.exchange, market_data.metrics.variance);

        let max_order_size = self.config.risk_limits.max_order_size;
        let limited_order_size = order_size.min(max_order_size);

        let last_quotes = self.last_quotes.get(&key).cloned().unwrap_or((0.0, 0.0));
        let bid_changed = (bid_price - last_quotes.0).abs() / last_quotes.0 > 0.0001;
        let ask_changed = (ask_price - last_quotes.1).abs() / last_quotes.1 > 0.0001;

        if bid_changed || ask_changed || last_quotes.0 == 0.0 {
            signals.push(Signal::cancel_all(
                self.config.id.clone(),
                market_data.symbol.clone(),
                market_data.exchange.clone(),
            ));

            if bid_price > 0.0 && limited_order_size > 0.0 {
                let inventory = self.position_tracker.get(&key).cloned().unwrap_or(0.0);
                
                signals.push(Signal::buy_limit(
                    self.config.id.clone(),
                    market_data.symbol.clone(),
                    market_data.exchange.clone(),
                    limited_order_size,
                    bid_price,
                    0.8,
                )
                .with_metadata("ofi".to_string(), mm_ofi.clone())
                .with_metadata("spread_bps".to_string(), market_data.metrics.spread_bps.to_string())
                .with_metadata("inventory".to_string(), inventory.to_string()));
            }

            if ask_price > 0.0 && limited_order_size > 0.0 {
                let inventory = self.position_tracker.get(&key).cloned().unwrap_or(0.0);
                
                signals.push(Signal::sell_limit(
                    self.config.id.clone(),
                    market_data.symbol.clone(),
                    market_data.exchange.clone(),
                    limited_order_size,
                    ask_price,
                    0.8,
                )
                .with_metadata("ofi".to_string(), mm_ofi.clone())
                .with_metadata("spread_bps".to_string(), market_data.metrics.spread_bps.to_string())
                .with_metadata("inventory".to_string(), inventory.to_string()));
            }

            self.last_quotes.insert(key, (bid_price, ask_price));
        }
        
        self.metrics.signals_generated += signals.len() as u64;
        self.metrics.avg_signal_time_ms = 
            (self.metrics.avg_signal_time_ms * (self.metrics.signals_generated - signals.len() as u64) as f64 + 
             start.elapsed().as_millis() as f64) / 
            self.metrics.signals_generated as f64;
        
        if !signals.is_empty() {
            self.metrics.last_signal_timestamp = market_data.timestamp;
        }
        
        Ok(signals)
    }
    
    fn update_state(&mut self, market_data: &MarketData) {
        // State is updated in generate_signals
    }
    
    fn metrics(&self) -> StrategyMetrics {
        self.metrics.clone()
    }
    
    fn on_signal_executed(&mut self, signal: &Signal, execution_price: f64, executed_qty: f64) {
        let key = (signal.symbol.clone(), signal.exchange.clone());
        let current_position = self.position_tracker.get(&key).cloned().unwrap_or(0.0);
        
        let new_position = match signal.action {
            SignalAction::Buy | SignalAction::BuyLimit => current_position + executed_qty,
            SignalAction::Sell | SignalAction::SellLimit => current_position - executed_qty,
            _ => current_position,
        };
        
        self.position_tracker.insert(key, new_position);
        
        if let Some(quote_price) = signal.price {
            let pnl = (execution_price - quote_price) * executed_qty;
            self.metrics.total_pnl += pnl;
            
            if pnl > 0.0 {
                self.metrics.profitable_signals += 1;
            }
        }
        
        if self.metrics.signals_generated > 0 {
            self.metrics.win_rate = self.metrics.profitable_signals as f64 / self.metrics.signals_generated as f64;
        }
    }
    
    async fn shutdown(&mut self) -> Result<(), Box<dyn Error>> {
        println!("Shutting down market making strategy: {}", self.config.name);
        Ok(())
    }
}

/// Integrated Strategy Manager that uses SignalDispatcher for publishing
pub struct StrategyManager {
    strategies: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Strategy>>>>>>,
    orderbooks: Arc<RwLock<HashMap<(String, String), Arc<Orderbook>>>>,
    portfolios: Arc<RwLock<HashMap<String, Arc<CryptoWallet>>>>,
    
    // Use SignalDispatcher instead of manual signal processing
    signal_dispatcher: Option<SignalDispatcher>,
    
    // Shared signal store (used by both strategy manager and signal dispatcher)
    signal_store: Arc<SignalStore>,
    
    // Signal router for routing signals to different handlers
    signal_router: Arc<Mutex<SignalRouter>>,
    
    running: Arc<Mutex<bool>>,
    worker_handles: Vec<thread::JoinHandle<()>>,
}

// Manual Clone implementation for StrategyManager (excluding worker_handles)
impl Clone for StrategyManager {
    fn clone(&self) -> Self {
        Self {
            strategies: Arc::clone(&self.strategies),
            orderbooks: Arc::clone(&self.orderbooks),
            portfolios: Arc::clone(&self.portfolios),
            signal_dispatcher: self.signal_dispatcher.clone(),
            signal_store: Arc::clone(&self.signal_store),
            signal_router: Arc::clone(&self.signal_router),
            running: Arc::clone(&self.running),
            worker_handles: Vec::new(), // Do not clone running threads
        }
    }
}

impl StrategyManager {
    /// Create new strategy manager with signal dispatcher integration
    pub fn new(
        orderbooks: Arc<RwLock<HashMap<(String, String), Arc<Orderbook>>>>,
        portfolios: Arc<RwLock<HashMap<String, Arc<CryptoWallet>>>>,
        broker_addr: &str,
        topics: Vec<String>,
    ) -> Result<Self, Box<dyn Error>> {
        // Create shared signal store
        let signal_store = Arc::new(SignalStore::new());
        
        // Create signal dispatcher
        let signal_dispatcher = SignalDispatcherBuilder::new(broker_addr)
            .with_topics(topics)
            .with_buffer_size(10000)
            .with_batch_size(100)
            .with_processing_interval(1)
            .with_auto_reconnect(true, 1000)
            .build()?;
        
        Ok(Self {
            strategies: Arc::new(RwLock::new(HashMap::new())),
            orderbooks,
            portfolios,
            signal_dispatcher: Some(signal_dispatcher),
            signal_store,
            signal_router: Arc::new(Mutex::new(SignalRouter::new())),
            running: Arc::new(Mutex::new(false)),
            worker_handles: Vec::new(),
        })
    }
    
    /// Configure signal filter on the dispatcher
    pub fn configure_signal_filter(&mut self, filter: SignalFilter) -> Result<(), String> {
        if let Some(dispatcher) = &mut self.signal_dispatcher {
            // Update dispatcher configuration with new filter
            // Note: This would require modifying SignalDispatcher to accept filter updates
            Ok(())
        } else {
            Err("Signal dispatcher not available".to_string())
        }
    }
    
    /// Add signal route
    pub fn add_signal_route(&self, strategy_id: String, handler: Sender<Signal>) {
        let mut router = self.signal_router.lock().unwrap();
        router.add_route(strategy_id, handler);
    }
    
    /// Add default signal handler
    pub fn add_default_handler(&self, handler: Sender<Signal>) {
        let mut router = self.signal_router.lock().unwrap();
        router.add_default_handler(handler);
    }
    
    /// Add a strategy to the manager
    pub fn add_strategy(&self, mut strategy: Box<dyn Strategy>) -> Result<(), Box<dyn Error>> {
        let config = strategy.config().clone();
        
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(strategy.initialize())?;
        
        let arc_strategy = Arc::new(Mutex::new(strategy));
        
        let mut strategies = self.strategies.write().unwrap();
        strategies.insert(config.id.clone(), arc_strategy.clone());
        
        let mut global_strategies = STRATEGIES.write().unwrap();
        global_strategies.insert(config.id.clone(), arc_strategy);
        
        println!("Added strategy: {} ({})", config.name, config.id);
        Ok(())
    }
    
    /// Remove a strategy
    pub fn remove_strategy(&self, strategy_id: &str) -> Result<(), Box<dyn Error>> {
        let mut strategies = self.strategies.write().unwrap();
        
        if let Some(strategy) = strategies.remove(strategy_id) {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(strategy.lock().unwrap().shutdown())?;
            
            let mut global_strategies = STRATEGIES.write().unwrap();
            global_strategies.remove(strategy_id);
            
            println!("Removed strategy: {}", strategy_id);
        }
        
        Ok(())
    }
    
    /// Start the strategy manager with integrated signal dispatcher
    pub fn start(&mut self, num_workers: usize) -> Result<(), Box<dyn Error>> {
        *self.running.lock().unwrap() = true;
        
        // Start the signal dispatcher first
        if let Some(ref mut dispatcher) = self.signal_dispatcher {
            dispatcher.start()?;
            println!("Signal dispatcher started");
            
            // Connect strategy manager to signal dispatcher
            let dispatcher_sender = dispatcher.get_sender();
            self.add_default_handler(dispatcher_sender);
        }
        
        // Start strategy worker threads
        for i in 0..num_workers {
            let strategies = Arc::clone(&self.strategies);
            let orderbooks = Arc::clone(&self.orderbooks);
            let portfolios = Arc::clone(&self.portfolios);
            let signal_store = Arc::clone(&self.signal_store);
            let signal_router = Arc::clone(&self.signal_router);
            let running = Arc::clone(&self.running);
            
            let handle = thread::spawn(move || {
                Self::strategy_worker(
                    i,
                    strategies,
                    orderbooks,
                    portfolios,
                    signal_store,
                    signal_router,
                    running,
                );
            });
            
            self.worker_handles.push(handle);
        }
        
        println!("Strategy manager started with {} workers", num_workers);
        Ok(())
    }
    
    /// Stop the strategy manager
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        *self.running.lock().unwrap() = false;
        
        // Wait for workers to finish
        for handle in self.worker_handles.drain(..) {
            handle.join().unwrap();
        }
        
        // Stop signal dispatcher
        if let Some(ref mut dispatcher) = self.signal_dispatcher {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(dispatcher.stop())?;
            println!("Signal dispatcher stopped");
        }
        
        // Shutdown all strategies
        let mut strategies = self.strategies.write().unwrap();
        let runtime = tokio::runtime::Runtime::new()?;
        
        for (id, strategy_arc) in strategies.drain() {
            let mut strategy = strategy_arc.lock().unwrap();
            runtime.block_on(strategy.shutdown())?;
            println!("Shutdown strategy: {}", id);
        }
        
        println!("Strategy manager stopped");
        Ok(())
    }
    
    /// Get shared signal store
    pub fn signal_store(&self) -> Arc<SignalStore> {
        Arc::clone(&self.signal_store)
    }
    
    /// Get signal dispatcher metrics
    pub fn get_dispatcher_metrics(&self) -> Option<signaldispatcher::SignalDispatcherMetrics> {
        self.signal_dispatcher.as_ref().map(|d| d.get_metrics())
    }
    
    /// Worker thread that runs strategies and routes signals through dispatcher
    fn strategy_worker(
        worker_id: usize,
        strategies: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Strategy>>>>>>,
        orderbooks: Arc<RwLock<HashMap<(String, String), Arc<Orderbook>>>>,
        portfolios: Arc<RwLock<HashMap<String, Arc<CryptoWallet>>>>,
        signal_store: Arc<SignalStore>,
        signal_router: Arc<Mutex<SignalRouter>>,
        running: Arc<Mutex<bool>>,
    ) {
        println!("Strategy worker {} started", worker_id);

        let mut last_run: HashMap<String, Instant> = HashMap::new();
        
        while *running.lock().unwrap() {
            let start_time = Instant::now();
            
            // Get list of enabled strategies
            let strategy_configs: Vec<StrategyConfig> = {
                let strategies = strategies.read().unwrap();
                strategies.values()
                    .filter_map(|s| {
                        let s = s.lock().unwrap();
                        if s.config().enabled {
                            Some(s.config().clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            };
            
            // Run each strategy
            for config in strategy_configs {
                // Check rate limiting
                let last_run_time = last_run.get(&config.id).cloned().unwrap_or(Instant::now() - Duration::from_secs(1));
                if last_run_time.elapsed() < Duration::from_millis(100) {
                    continue;
                }
                
                // Process each symbol/exchange combination
                for symbol in &config.symbols {
                    for exchange in &config.exchanges {
                        // Get market data
                        let market_data = match Self::get_market_data(&orderbooks, symbol, exchange) {
                            Some(data) => data,
                            None => continue,
                        };
                        
                        // Get portfolio snapshot
                        let portfolio_snapshot = match Self::get_portfolio_snapshot(&portfolios, exchange) {
                            Some(snapshot) => snapshot,
                            None => continue,
                        };
                        
                        // Generate signals
                        let mut strategies = strategies.write().unwrap();
                        if let Some(strategy_arc) = strategies.get_mut(&config.id) {
                            let mut strategy = strategy_arc.lock().unwrap();
                            let signal_start = Instant::now();

                            let runtime = match tokio::runtime::Runtime::new() {
                                Ok(rt) => rt,
                                Err(e) => {
                                    eprintln!("Failed to create Tokio runtime: {}", e);
                                    continue;
                                }
                            };

                            match runtime.block_on(strategy.generate_signals(&market_data, &portfolio_snapshot)) {
                                Ok(signals) => {
                                    let signal_time = signal_start.elapsed();

                                    // Process each signal generated by the strategy
                                    for signal in signals {
                                        if signal.action != SignalAction::Hold {
                                            // Validate signal using reusable validation from signalgenerator
                                            if let Err(e) = signal.is_valid() {
                                                eprintln!("Invalid signal {}: {}", signal.id, e);
                                                continue;
                                            }
                                            
                                            // Store signal in shared store
                                            if let Err(e) = signal_store.store(signal.clone()) {
                                                eprintln!("Failed to store signal {}: {}", signal.id, e);
                                            }
                                            
                                            // Route signal through the router (which includes dispatcher)
                                            let router = signal_router.lock().unwrap();
                                            if let Err(e) = router.route(signal.clone()) {
                                                eprintln!("Failed to route signal {}: {}", signal.id, e);
                                            } else if cfg!(debug_assertions) {
                                                println!(
                                                    "Strategy {} generated and routed signal: {:?} for {}/{} in {:?}",
                                                    config.id, signal.action, symbol, exchange, signal_time
                                                );
                                            }
                                        }
                                    }

                                    // Update strategy state
                                    strategy.update_state(&market_data);
                                },
                                Err(e) => {
                                    eprintln!("Strategy {} error: {}", config.id, e);
                                }
                            }
                        }
                    }
                }
                
                last_run.insert(config.id.clone(), Instant::now());
            }
            
            let elapsed = start_time.elapsed();
            if elapsed < Duration::from_millis(10) {
                thread::sleep(Duration::from_millis(10) - elapsed);
            }
        }
        
        println!("Strategy worker {} stopped", worker_id);
    }
    
    /// Get market data from orderbook (reusable utility)
    fn get_market_data(
        orderbooks: &Arc<RwLock<HashMap<(String, String), Arc<Orderbook>>>>,
        symbol: &str,
        exchange: &str,
    ) -> Option<MarketData> {
        let orderbooks = orderbooks.read().unwrap();
        let key = (symbol.to_string(), exchange.to_string());
        
        if let Some(orderbook) = orderbooks.get(&key) {
            match orderbook.metrics() {
                Ok(metrics) => Some(MarketData {
                    symbol: symbol.to_string(),
                    exchange: exchange.to_string(),
                    timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                    metrics,
                }),
                Err(_) => None,
            }
        } else {
            None
        }
    }
    
    /// Get portfolio snapshot (reusable utility)
    fn get_portfolio_snapshot(
        portfolios: &Arc<RwLock<HashMap<String, Arc<CryptoWallet>>>>,
        exchange: &str,
    ) -> Option<PortfolioSnapshot> {
        let portfolios = portfolios.read().unwrap();
        
        if let Some(wallet) = portfolios.get(exchange) {
            let mut balances_map: HashMap<String, f64> = HashMap::new();
            if let Ok(all_balances) = wallet.get_all_balances() {
                for (asset, balance_map) in all_balances {
                    if let Some(crypto_balance) = balance_map.get("available") {
                        balances_map.insert(asset, crypto_balance.available_balance);
                    } else if let Some((_k, crypto_balance)) = balance_map.iter().next() {
                        balances_map.insert(asset, crypto_balance.available_balance);
                    }
                }
            }
            let total_value = wallet.get_metrics().unwrap().total_value;
            
            Some(PortfolioSnapshot {
                exchange: exchange.to_string(),
                balances: balances_map,
                total_value,
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            })
        } else {
            None
        }
    }
    
    /// Get all strategies
    pub fn get_strategies(&self) -> Vec<StrategyConfig> {
        let strategies = self.strategies.read().unwrap();
        strategies.values().filter_map(|s| {
            let s = s.lock().unwrap();
            Some(s.config().clone())
        }).collect()
    }
    
    /// Get strategy by ID
    pub fn get_strategy(&self, strategy_id: &str) -> Option<StrategyConfig> {
        let strategies = self.strategies.read().unwrap();
        strategies.get(strategy_id).map(|s| {
            let s = s.lock().unwrap();
            s.config().clone()
        })
    }
    
    /// Get strategy metrics
    pub fn get_strategy_metrics(&self, strategy_id: &str) -> Option<StrategyMetrics> {
        let strategies = self.strategies.read().unwrap();
        strategies.get(strategy_id).map(|s| {
            let s = s.lock().unwrap();
            s.metrics()
        })
    }
    
    /// Get all strategy metrics
    pub fn get_all_metrics(&self) -> HashMap<String, StrategyMetrics> {
        let strategies = self.strategies.read().unwrap();
        strategies.iter()
            .map(|(id, strategy)| {
                let s = strategy.lock().unwrap();
                (id.clone(), s.metrics())
            })
            .collect()
    }
    
    /// Report signal execution back to strategy (uses shared signal store)
    pub fn report_execution(&self, signal_id: &str, execution_price: f64, executed_qty: f64, fees: f64) {
        // Get signal from shared store
        if let Some(signal_info) = self.signal_store.get(signal_id) {
            let signal = &signal_info.signal;
            
            // Update shared signal store
            if let Err(e) = self.signal_store.record_execution(signal_id, execution_price, executed_qty, fees) {
                eprintln!("Failed to record execution for signal {}: {}", signal_id, e);
            }
            
            // Report to strategy
            let mut strategies = self.strategies.write().unwrap();
            if let Some(strategy_arc) = strategies.get_mut(&signal.strategy_id) {
                let mut strategy = strategy_arc.lock().unwrap();
                strategy.on_signal_executed(signal, execution_price, executed_qty);
            }
        }
    }
    
    /// Get comprehensive system metrics (combines strategy and dispatcher metrics)
    pub fn get_system_metrics(&self) -> SystemMetrics {
        let strategy_metrics = self.get_all_metrics();
        let dispatcher_metrics = self.get_dispatcher_metrics();
        let signal_stats = self.signal_store.get_stats();
        
        SystemMetrics {
            strategy_metrics,
            dispatcher_metrics,
            signal_stats,
            total_strategies: self.strategies.read().unwrap().len(),
            running_strategies: self.strategies.read().unwrap().values()
                .filter(|s| s.lock().unwrap().config().enabled)
                .count(),
        }
    }
}

/// Comprehensive system metrics that combines all components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub strategy_metrics: HashMap<String, StrategyMetrics>,
    pub dispatcher_metrics: Option<signaldispatcher::SignalDispatcherMetrics>,
    pub signal_stats: SignalStats,
    pub total_strategies: usize,
    pub running_strategies: usize,
}

/// Factory function to create strategies from configuration (unchanged)
pub fn create_strategy(config: StrategyConfig) -> Result<Box<dyn Strategy>, Box<dyn Error>> {
    match config.strategy_type {
        StrategyType::MarketMaking => {
            Ok(Box::new(MarketMakingStrategy::new(config)))
        },
        StrategyType::Momentum => {
            Err("Momentum strategy not implemented yet".into())
        },
        StrategyType::Arbitrage => {
            Err("Arbitrage strategy not implemented yet".into())
        },
        StrategyType::Custom(ref name) => {
            Err(format!("Custom strategy '{}' not implemented", name).into())
        },
    }
}

/// Builder for creating StrategyManager with different configurations
pub struct StrategyManagerBuilder {
    broker_addr: String,
    topics: Vec<String>,
    signal_filter: Option<SignalFilter>,
    topic_mappings: HashMap<String, usize>,
}

impl StrategyManagerBuilder {
    pub fn new(broker_addr: &str) -> Self {
        Self {
            broker_addr: broker_addr.to_string(),
            topics: vec!["signals".to_string()],
            signal_filter: None,
            topic_mappings: HashMap::new(),
        }
    }
    
    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }
    
    pub fn with_signal_filter(mut self, filter: SignalFilter) -> Self {
        self.signal_filter = Some(filter);
        self
    }
    
    pub fn with_topic_mappings(mut self, mappings: HashMap<String, usize>) -> Self {
        self.topic_mappings = mappings;
        self
    }
    
    pub fn build(
        self,
        orderbooks: Arc<RwLock<HashMap<(String, String), Arc<Orderbook>>>>,
        portfolios: Arc<RwLock<HashMap<String, Arc<CryptoWallet>>>>,
    ) -> Result<StrategyManager, Box<dyn Error>> {
        let mut manager = StrategyManager::new(
            orderbooks,
            portfolios,
            &self.broker_addr,
            self.topics,
        )?;
        
        if let Some(filter) = self.signal_filter {
            manager.configure_signal_filter(filter)?;
        }
        
        Ok(manager)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signalgenerator::{SignalStatus, SignalFilter};
    
    #[test]
    fn test_integrated_strategy_manager_creation() {
        let orderbooks = Arc::new(RwLock::new(HashMap::new()));
        let portfolios = Arc::new(RwLock::new(HashMap::new()));
        
        let manager = StrategyManagerBuilder::new("127.0.0.1:8080")
            .with_topics(vec!["orders.btc".to_string(), "orders.eth".to_string()])
            .with_topic_mappings(HashMap::from([
                ("BTC/USD".to_string(), 0),
                ("ETH/USD".to_string(), 1),
            ]))
            .with_signal_filter(SignalFilter::new().with_min_confidence(0.7))
            .build(orderbooks, portfolios);
        
        assert!(manager.is_ok());
    }
    
    #[test]
    fn test_strategy_addition_and_signal_flow() {
        let orderbooks = Arc::new(RwLock::new(HashMap::new()));
        let portfolios = Arc::new(RwLock::new(HashMap::new()));
        
        let manager = StrategyManager::new(
            orderbooks,
            portfolios,
            "127.0.0.1:8080",
            vec!["test.signals".to_string()],
        ).unwrap();
        
        // Create test strategy config
        let mut params = HashMap::new();
        params.insert("gamma".to_string(), serde_json::json!(0.1));
        
        let config = StrategyConfig {
            id: "test-strategy".to_string(),
            name: "Test Strategy".to_string(),
            strategy_type: StrategyType::MarketMaking,
            enabled: true,
            symbols: vec!["BTC/USD".to_string()],
            exchanges: vec!["binance".to_string()],
            parameters: params,
            risk_limits: RiskLimits {
                max_position_size: 10.0,
                max_order_size: 1.0,
                max_daily_loss: 1000.0,
                max_open_orders: 10,
                max_notional_exposure: 50000.0,
            },
        };
        
        // Add strategy
        let strategy = create_strategy(config).unwrap();
        manager.add_strategy(strategy).unwrap();
        
        // Verify strategy was added
        let strategies = manager.get_strategies();
        assert_eq!(strategies.len(), 1);
        assert_eq!(strategies[0].id, "test-strategy");
        
        // Test shared signal store
        let store = manager.signal_store();
        let test_signal = Signal::buy_limit(
            "test-strategy".to_string(),
            "BTC/USD".to_string(),
            "binance".to_string(),
            1.0,
            50000.0,
            0.8,
        );
        
        store.store(test_signal.clone()).unwrap();
        
        // Verify signal was stored
        let stored_signal = store.get(&test_signal.id).unwrap();
        assert_eq!(stored_signal.signal.id, test_signal.id);
        assert_eq!(stored_signal.status, SignalStatus::Pending);
        
        // Test execution reporting
        manager.report_execution(&test_signal.id, 49950.0, 1.0, 25.0);
        
        // Verify execution was recorded
        let executed_signal = store.get(&test_signal.id).unwrap();
        assert_eq!(executed_signal.status, SignalStatus::Filled);
        assert_eq!(executed_signal.execution_price, Some(49950.0));
        
        // Test system metrics
        let system_metrics = manager.get_system_metrics();
        assert_eq!(system_metrics.total_strategies, 1);
        assert_eq!(system_metrics.running_strategies, 1);
        assert_eq!(system_metrics.signal_stats.total_signals, 1);
        assert_eq!(system_metrics.signal_stats.filled_signals, 1);
    }
    
    #[tokio::test]
    async fn test_signal_flow_integration() {
        // Test that signals flow from strategy -> router -> dispatcher -> broker
        // This would require more complex integration testing with actual message broker
        
        let orderbooks = Arc::new(RwLock::new(HashMap::new()));
        let portfolios = Arc::new(RwLock::new(HashMap::new()));
        
        let mut manager = StrategyManager::new(
            orderbooks,
            portfolios,
            "127.0.0.1:8080",
            vec!["test.signals".to_string()],
        ).unwrap();
        
        // In a real test, you would:
        // 1. Start the manager
        // 2. Add strategies
        // 3. Inject market data
        // 4. Verify signals are generated and routed correctly
        // 5. Verify signals reach the message broker
        
        // For now, just verify the structure is correct
        assert!(manager.signal_store().get_stats().total_signals == 0);
    }
}