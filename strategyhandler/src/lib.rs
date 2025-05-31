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
use signalgenerator::{Signal, SignalAction, SignalStore, SignalFilter, SignalRouter};
use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;

// Global storage for strategies
lazy_static! {
    static ref STRATEGIES: RwLock<HashMap<String, Arc<Mutex<Box<dyn Strategy>>>>> = RwLock::new(HashMap::new());
}

/// Strategy configuration
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

/// Strategy types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StrategyType {
    MarketMaking,
    Momentum,
    Arbitrage,
    Custom(String),
}

/// Risk limits for a strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimits {
    pub max_position_size: f64,
    pub max_order_size: f64,
    pub max_daily_loss: f64,
    pub max_open_orders: usize,
    pub max_notional_exposure: f64,
}

/// Market data snapshot for strategy calculations
#[derive(Debug, Clone)]
pub struct MarketData {
    pub symbol: String,
    pub exchange: String,
    pub timestamp: u64,
    pub metrics: OrderbookMetrics,
}

/// Portfolio snapshot for strategy calculations
#[derive(Debug, Clone)]
pub struct PortfolioSnapshot {
    pub exchange: String,
    pub balances: HashMap<String, f64>,
    pub total_value: f64,
    pub timestamp: u64,
}

/// Strategy performance metrics
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

/// Base trait that all strategies must implement
#[async_trait]
pub trait Strategy: Send + Sync {
    /// Get strategy configuration
    fn config(&self) -> &StrategyConfig;
    
    /// Initialize strategy with historical data if needed
    async fn initialize(&mut self) -> Result<(), Box<dyn Error>>;
    
    /// Generate trading signals based on market data and portfolio
    async fn generate_signals(
        &mut self,
        market_data: &MarketData,
        portfolio: &PortfolioSnapshot,
    ) -> Result<Vec<Signal>, Box<dyn Error>>;
    
    /// Update strategy state (called after each tick)
    fn update_state(&mut self, market_data: &MarketData);
    
    /// Get current metrics
    fn metrics(&self) -> StrategyMetrics;
    
    /// Handle signal execution feedback
    fn on_signal_executed(&mut self, signal: &Signal, execution_price: f64, executed_qty: f64);
    
    /// Shutdown strategy gracefully
    async fn shutdown(&mut self) -> Result<(), Box<dyn Error>>;
}

/// Market Making Strategy implementation
pub struct MarketMakingStrategy {
    config: StrategyConfig,
    metrics: StrategyMetrics,
    market_makers: HashMap<(String, String), MarketMaker>,
    last_quotes: HashMap<(String, String), (f64, f64)>, // (bid, ask)
    position_tracker: HashMap<(String, String), f64>,
    price_history: HashMap<(String, String), Vec<f64>>,
    variance_history: HashMap<(String, String), Vec<f64>>,
}

impl MarketMakingStrategy {
    pub fn new(config: StrategyConfig) -> Self {
        // Initialize market makers for each symbol/exchange pair
        let mut market_makers = HashMap::new();
        
        // Extract parameters from config
        let gamma = config.parameters.get("gamma")
            .and_then(|v| v.as_f64())
            .map(|v| BigDecimal::from_f64(v).unwrap())
            .unwrap_or_else(|| BigDecimal::from_f64(0.1).unwrap());
            
        // Extract k parameters
        let k1 = Self::extract_param(&config.parameters, "k1", 0.1);
        let k2 = Self::extract_param(&config.parameters, "k2", 0.1);
        let k3 = Self::extract_param(&config.parameters, "k3", 0.1);
        let k4 = Self::extract_param(&config.parameters, "k4", 0.1);
        let k5 = Self::extract_param(&config.parameters, "k5", 0.1);
        let k6 = Self::extract_param(&config.parameters, "k6", 0.1);
        
        // Extract weight parameters
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
        
        // Create market makers for each symbol/exchange combination
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
    
    fn extract_param(params: &HashMap<String, serde_json::Value>, name: &str, default: f64) -> BigDecimal {
        params.get(name)
            .and_then(|v| v.as_f64())
            .map(|v| BigDecimal::from_f64(v).unwrap())
            .unwrap_or_else(|| BigDecimal::from_f64(default).unwrap())
    }
    
    fn calculate_ofi(&self, metrics: &OrderbookMetrics) -> BigDecimal {
        // Calculate Order Flow Imbalance components
        let ofi_tick = BigDecimal::from_f64(metrics.orderbook_imbalance).unwrap_or_default();
        let ofi_liquidity = BigDecimal::from_f64(metrics.liquidity_weighted_orderbook_imbalance).unwrap_or_default();
        let ofi_smoothed = BigDecimal::from_f64(metrics.smoothed_orderbook_imbalance).unwrap_or_default();
        
        // Weighted combination
        ofi_tick * BigDecimal::from_f64(0.3).unwrap() +
        ofi_liquidity * BigDecimal::from_f64(0.4).unwrap() +
        ofi_smoothed * BigDecimal::from_f64(0.3).unwrap()
    }
    
    fn update_price_history(&mut self, symbol: &str, exchange: &str, price: f64) {
        let key = (symbol.to_string(), exchange.to_string());
        let history = self.price_history.entry(key).or_insert_with(Vec::new);
        
        history.push(price);
        
        // Keep only last 100 prices
        if history.len() > 100 {
            history.remove(0);
        }
    }
    
    fn update_variance_history(&mut self, symbol: &str, exchange: &str, variance: f64) {
        let key = (symbol.to_string(), exchange.to_string());
        let history = self.variance_history.entry(key).or_insert_with(Vec::new);
        
        history.push(variance);
        
        // Keep only last 20 variance values
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
                0.001 // Default variance
            }
        } else {
            0.001 // Default variance
        }
    }
}

#[async_trait]
impl Strategy for MarketMakingStrategy {
    fn config(&self) -> &StrategyConfig {
        &self.config
    }
    
    async fn initialize(&mut self) -> Result<(), Box<dyn Error>> {
        println!("Initializing market making strategy: {}", self.config.name);
        
        // Initialize position tracker
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
        let start = Instant::now();
        let mut signals = Vec::new();
        
        let key = (market_data.symbol.clone(), market_data.exchange.clone());
        
        // Calculate OFI before mutable borrow
        let ofi = self.calculate_ofi(&market_data.metrics);

        // Calculate historical variance before mutable borrow
        let historical_variance = self.get_historical_variance(&market_data.symbol, &market_data.exchange);

        // Get the market maker for this symbol/exchange
        let (ask_price, bid_price, order_size, mm_ofi) = if let Some(mm) = self.market_makers.get_mut(&key) {
            // Update market maker state
            let inventory = self.position_tracker.get(&key).cloned().unwrap_or(0.0);
            mm.set_inventory(BigDecimal::from_f64(inventory).unwrap());

            // Use precomputed OFI
            mm.calculate_ofi(ofi.clone(), ofi.clone(), ofi.clone());

            // Calculate parameters
            let mid_price_bd = BigDecimal::from_f64(market_data.metrics.mid_price).unwrap();
            let variance_bd = BigDecimal::from_f64(market_data.metrics.variance).unwrap();
            let historical_var_bd = BigDecimal::from_f64(historical_variance).unwrap();
            let bid_depth_bd = BigDecimal::from_f64(market_data.metrics.best_bid_depth).unwrap();
            let ask_depth_bd = BigDecimal::from_f64(market_data.metrics.best_ask_depth).unwrap();
            let best_bid_bd = BigDecimal::from_f64(market_data.metrics.best_bid).unwrap();
            let best_ask_bd = BigDecimal::from_f64(market_data.metrics.best_ask).unwrap();
            let total_depth_bd = BigDecimal::from_f64(market_data.metrics.total_depth).unwrap();
            let best_depth_bd = BigDecimal::from_f64(market_data.metrics.best_bid_depth.max(market_data.metrics.best_ask_depth)).unwrap();

            // Update all market maker parameters
            mm.calculate_sigma(&variance_bd);
            mm.calculate_tau(&total_depth_bd, &best_depth_bd);
            mm.calculate_alpha(&variance_bd);
            mm.calculate_beta(&total_depth_bd, &best_depth_bd, Some(best_ask_bd.clone()), Some(best_bid_bd.clone()), &mid_price_bd);
            mm.calculate_gamma(&variance_bd, &historical_var_bd);
            mm.calculate_eta(&variance_bd, &total_depth_bd, &best_depth_bd);
            mm.calculate_lambda(&variance_bd);
            mm.calculate_theta();

            // Generate signal
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

            // Convert MM signal to strategy signals
            let ask_price = mm_signal.get_ask_quote().to_string().parse::<f64>().unwrap_or(0.0);
            let bid_price = mm_signal.get_bid_quote().to_string().parse::<f64>().unwrap_or(0.0);
            let order_size = mm_signal.get_order_size().to_string().parse::<f64>().unwrap_or(0.0).abs();
            let mm_ofi = mm.get_ofi().to_string();

            (ask_price, bid_price, order_size, mm_ofi)
        } else {
            (0.0, 0.0, 0.0, "0".to_string())
        };

        // Update history
        self.update_price_history(&market_data.symbol, &market_data.exchange, market_data.metrics.mid_price);
        self.update_variance_history(&market_data.symbol, &market_data.exchange, market_data.metrics.variance);

        // Check risk limits
        let max_order_size = self.config.risk_limits.max_order_size;
        let limited_order_size = order_size.min(max_order_size);

        // Generate signals only if quotes have changed significantly
        let last_quotes = self.last_quotes.get(&key).cloned().unwrap_or((0.0, 0.0));
        let bid_changed = (bid_price - last_quotes.0).abs() / last_quotes.0 > 0.0001;
        let ask_changed = (ask_price - last_quotes.1).abs() / last_quotes.1 > 0.0001;

        if bid_changed || ask_changed || last_quotes.0 == 0.0 {
            // Cancel existing orders first
            signals.push(Signal::cancel_all(
                self.config.id.clone(),
                market_data.symbol.clone(),
                market_data.exchange.clone(),
            ));

            // Place new bid
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

            // Place new ask
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

            // Update last quotes
            self.last_quotes.insert(key, (bid_price, ask_price));
        }
        
        // Update metrics
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
        // Update position tracker
        let key = (signal.symbol.clone(), signal.exchange.clone());
        let current_position = self.position_tracker.get(&key).cloned().unwrap_or(0.0);
        
        let new_position = match signal.action {
            SignalAction::Buy | SignalAction::BuyLimit => current_position + executed_qty,
            SignalAction::Sell | SignalAction::SellLimit => current_position - executed_qty,
            _ => current_position,
        };
        
        self.position_tracker.insert(key, new_position);
        
        // Update metrics
        if let Some(quote_price) = signal.price {
            let pnl = (execution_price - quote_price) * executed_qty;
            self.metrics.total_pnl += pnl;
            
            if pnl > 0.0 {
                self.metrics.profitable_signals += 1;
            }
        }
        
        // Update win rate
        if self.metrics.signals_generated > 0 {
            self.metrics.win_rate = self.metrics.profitable_signals as f64 / self.metrics.signals_generated as f64;
        }
    }
    
    async fn shutdown(&mut self) -> Result<(), Box<dyn Error>> {
        println!("Shutting down market making strategy: {}", self.config.name);
        Ok(())
    }
}

/// Strategy manager that runs multiple strategies concurrently
pub struct StrategyManager {
    strategies: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Strategy>>>>>>,
    orderbooks: Arc<RwLock<HashMap<(String, String), Arc<Orderbook>>>>,
    portfolios: Arc<RwLock<HashMap<String, Arc<CryptoWallet>>>>,
    signal_sender: Sender<Signal>,
    signal_receiver: Receiver<Signal>,
    signal_store: Arc<SignalStore>,
    signal_filter: Arc<SignalFilter>,
    signal_router: Arc<Mutex<SignalRouter>>,
    running: Arc<Mutex<bool>>,
    worker_handles: Vec<thread::JoinHandle<()>>,
}

impl StrategyManager {
    /// Create new strategy manager
    pub fn new(
        orderbooks: Arc<RwLock<HashMap<(String, String), Arc<Orderbook>>>>,
        portfolios: Arc<RwLock<HashMap<String, Arc<CryptoWallet>>>>,
    ) -> Self {
        let (signal_sender, signal_receiver) = bounded(1000);
        
        // Create default signal filter
        let signal_filter = Arc::new(SignalFilter::new());
        
        Self {
            strategies: Arc::new(RwLock::new(HashMap::new())),
            orderbooks,
            portfolios,
            signal_sender,
            signal_receiver,
            signal_store: Arc::new(SignalStore::new()),
            signal_filter,
            signal_router: Arc::new(Mutex::new(SignalRouter::new())),
            running: Arc::new(Mutex::new(false)),
            worker_handles: Vec::new(),
        }
    }
    
    /// Configure signal filter
    pub fn configure_filter(&mut self, filter: SignalFilter) {
        self.signal_filter = Arc::new(filter);
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
        
        // Initialize strategy
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(strategy.initialize())?;
        
        // Wrap the strategy
        let arc_strategy = Arc::new(Mutex::new(strategy));
        
        // Add to strategies map
        let mut strategies = self.strategies.write().unwrap();
        strategies.insert(config.id.clone(), arc_strategy.clone());
        
        // Also add to global storage
        let mut global_strategies = STRATEGIES.write().unwrap();
        global_strategies.insert(config.id.clone(), arc_strategy);
        
        println!("Added strategy: {} ({})", config.name, config.id);
        Ok(())
    }
    
    /// Remove a strategy
    pub fn remove_strategy(&self, strategy_id: &str) -> Result<(), Box<dyn Error>> {
        let mut strategies = self.strategies.write().unwrap();
        
        if let Some(strategy) = strategies.remove(strategy_id) {
            // Shutdown strategy
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(strategy.lock().unwrap().shutdown())?;
            
            // Remove from global storage
            let mut global_strategies = STRATEGIES.write().unwrap();
            global_strategies.remove(strategy_id);
            
            println!("Removed strategy: {}", strategy_id);
        }
        
        Ok(())
    }
    
    /// Start the strategy manager
    pub fn start(&mut self, num_workers: usize) -> Result<(), Box<dyn Error>> {
        *self.running.lock().unwrap() = true;
        
        // Start signal processor thread
        let signal_receiver = self.signal_receiver.clone();
        let signal_store = Arc::clone(&self.signal_store);
        let signal_filter = Arc::clone(&self.signal_filter);
        let signal_router = Arc::clone(&self.signal_router);
        let running = Arc::clone(&self.running);
        
        let handle = thread::spawn(move || {
            Self::signal_processor(
                signal_receiver,
                signal_store,
                signal_filter,
                signal_router,
                running,
            );
        });
        self.worker_handles.push(handle);
        
        // Start strategy worker threads
        for i in 0..num_workers {
            let strategies = Arc::clone(&self.strategies);
            let orderbooks = Arc::clone(&self.orderbooks);
            let portfolios = Arc::clone(&self.portfolios);
            let signal_sender = self.signal_sender.clone();
            let running = Arc::clone(&self.running);
            
            let handle = thread::spawn(move || {
                Self::strategy_worker(
                    i,
                    strategies,
                    orderbooks,
                    portfolios,
                    signal_sender,
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
    
    /// Get signal store
    pub fn signal_store(&self) -> Arc<SignalStore> {
        Arc::clone(&self.signal_store)
    }
    
    /// Signal processor thread
    fn signal_processor(
        receiver: Receiver<Signal>,
        store: Arc<SignalStore>,
        filter: Arc<SignalFilter>,
        router: Arc<Mutex<SignalRouter>>,
        running: Arc<Mutex<bool>>,
    ) {
        println!("Signal processor started");
        
        while *running.lock().unwrap() {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(signal) => {
                    // Validate signal
                    if let Err(e) = signal.is_valid() {
                        eprintln!("Invalid signal {}: {}", signal.id, e);
                        continue;
                    }
                    
                    // Apply filter
                    if !filter.passes(&signal) {
                        if cfg!(debug_assertions) {
                            println!("Signal {} filtered out", signal.id);
                        }
                        continue;
                    }
                    
                    // Store signal
                    if let Err(e) = store.store(signal.clone()) {
                        eprintln!("Failed to store signal {}: {}", signal.id, e);
                    }
                    
                    // Route signal
                    let router = router.lock().unwrap();
                    if let Err(e) = router.route(signal.clone()) {
                        eprintln!("Failed to route signal {}: {}", signal.id, e);
                    }
                }
                Err(_) => continue,
            }
        }
        
        println!("Signal processor stopped");
    }
    
    /// Get market data from orderbook
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
    
    /// Get portfolio snapshot
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
    
    /// Worker thread that runs strategies
    fn strategy_worker(
        worker_id: usize,
        strategies: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Strategy>>>>>>,
        orderbooks: Arc<RwLock<HashMap<(String, String), Arc<Orderbook>>>>,
        portfolios: Arc<RwLock<HashMap<String, Arc<CryptoWallet>>>>,
        signal_sender: Sender<Signal>,
        running: Arc<Mutex<bool>>,
    ) {
        println!("Strategy worker {} started", worker_id);

        // Track last run time for each strategy
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

                            // Create a Tokio runtime for async execution
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

                                    // Send signals
                                    for signal in signals {
                                        if signal.action != SignalAction::Hold {
                                            if let Err(e) = signal_sender.try_send(signal.clone()) {
                                                eprintln!("Failed to send signal: {}", e);
                                            } else if cfg!(debug_assertions) {
                                                println!(
                                                    "Strategy {} generated signal: {:?} for {}/{} in {:?}",
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
    
    /// Report signal execution back to strategy
    pub fn report_execution(&self, signal_id: &str, execution_price: f64, executed_qty: f64, fees: f64) {
        // Get signal from store
        if let Some(signal_info) = self.signal_store.get(signal_id) {
            let signal = &signal_info.signal;
            
            // Update signal store
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
}

/// Factory function to create strategies from configuration
pub fn create_strategy(config: StrategyConfig) -> Result<Box<dyn Strategy>, Box<dyn Error>> {
    match config.strategy_type {
        StrategyType::MarketMaking => {
            Ok(Box::new(MarketMakingStrategy::new(config)))
        },
        StrategyType::Momentum => {
            // Placeholder for momentum strategy
            Err("Momentum strategy not implemented yet".into())
        },
        StrategyType::Arbitrage => {
            // Placeholder for arbitrage strategy
            Err("Arbitrage strategy not implemented yet".into())
        },
        StrategyType::Custom(ref name) => {
            Err(format!("Custom strategy '{}' not implemented", name).into())
        },
    }
}

/// Example usage and tests
#[cfg(test)]
mod tests {
    use super::*;
    use signalgenerator::{SignalStatus};
    
    #[test]
    fn test_strategy_config() {
        let mut params = HashMap::new();
        params.insert("gamma".to_string(), serde_json::json!(0.1));
        params.insert("k1".to_string(), serde_json::json!(0.15));
        
        let config = StrategyConfig {
            id: "test-mm-1".to_string(),
            name: "Test Market Maker".to_string(),
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
        
        let strategy = create_strategy(config.clone()).unwrap();
        assert_eq!(strategy.config().id, "test-mm-1");
    }
    
    #[tokio::test]
    async fn test_market_making_strategy() {
        let mut params = HashMap::new();
        params.insert("gamma".to_string(), serde_json::json!(0.1));
        
        let config = StrategyConfig {
            id: "mm-test".to_string(),
            name: "MM Test".to_string(),
            strategy_type: StrategyType::MarketMaking,
            enabled: true,
            symbols: vec!["ETH/USD".to_string()],
            exchanges: vec!["coinbase".to_string()],
            parameters: params,
            risk_limits: RiskLimits {
                max_position_size: 100.0,
                max_order_size: 10.0,
                max_daily_loss: 5000.0,
                max_open_orders: 20,
                max_notional_exposure: 100000.0,
            },
        };
        
        let mut strategy = MarketMakingStrategy::new(config);
        strategy.initialize().await.unwrap();
        
        // Create test market data with proper metrics
        let market_data = MarketData {
            symbol: "ETH/USD".to_string(),
            exchange: "coinbase".to_string(),
            timestamp: 1234567890000,
            metrics: OrderbookMetrics {
                best_bid: 2990.0,
                best_ask: 3010.0,
                mid_price: 3000.0,
                spread: 20.0,
                spread_bps: 6.67,
                best_bid_depth: 50.0,
                best_ask_depth: 45.0,
                total_bid_depth: 100.0,
                total_ask_depth: 95.0,
                total_depth: 195.0,
                orderbook_imbalance: 0.05,
                variance: 0.001,
                liquidity_weighted_orderbook_imbalance: 0.06,
                smoothed_orderbook_imbalance: 0.055,
                ..Default::default()
            },
        };
        
        // Create test portfolio
        let mut balances = HashMap::new();
        balances.insert("ETH".to_string(), 10.0);
        balances.insert("USD".to_string(), 30000.0);
        
        let portfolio = PortfolioSnapshot {
            exchange: "coinbase".to_string(),
            balances,
            total_value: 60000.0,
            timestamp: 1234567890000,
        };
        
        // Generate signals
        let signals = strategy.generate_signals(&market_data, &portfolio).await.unwrap();
        
        // Should generate at least cancel signal
        assert!(!signals.is_empty());
        
        // Verify signal types
        assert!(signals.iter().any(|s| s.action == SignalAction::CancelAll));
        
        // Check metrics
        let metrics = strategy.metrics();
        assert_eq!(metrics.signals_generated, signals.len() as u64);
    }
    
    #[test]
    fn test_strategy_manager_with_signal_processing() {
        use std::sync::Arc;
        
        // Create mock orderbooks and portfolios
        let orderbooks = Arc::new(RwLock::new(HashMap::new()));
        let portfolios = Arc::new(RwLock::new(HashMap::new()));
        
        // Create strategy manager
        let mut manager = StrategyManager::new(orderbooks, portfolios);
        
        // Configure signal filter
        let filter = SignalFilter::new()
            .with_min_confidence(0.7)
            .with_max_order_size(100.0);
        manager.configure_filter(filter);
        
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
        
        // Test signal store
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
        
        // Test signal execution reporting
        manager.report_execution(&test_signal.id, 49950.0, 1.0, 25.0);
        
        // Verify execution was recorded
        let executed_signal = store.get(&test_signal.id).unwrap();
        assert_eq!(executed_signal.status, SignalStatus::Filled);
        assert_eq!(executed_signal.execution_price, Some(49950.0));
        assert_eq!(executed_signal.executed_quantity, Some(1.0));
        assert_eq!(executed_signal.fees, Some(25.0));
        
        // Check stats
        let stats = store.get_stats();
        assert_eq!(stats.total_signals, 1);
        assert_eq!(stats.filled_signals, 1);
    }
}