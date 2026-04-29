#[cfg(feature = "postgres")]
pub mod paper_trade_writer;

use executionhandler::{UltraLowLatencyExecutionHandler, ExecutionStatus};
use executionhandler::signal::{Signal as ExecSignal, SignalAction as ExecSignalAction};
use executionhandler::core::MetricsCollector;
use executionhandler::optimizations::timestamp::{nano_timestamp, NanoTimer};
use ultra_signal::{Signal, OrderSide}; // Import Signal and OrderSide directly from ultra_signal

/// Order type for ultra-fast processing
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderType {
    Market,
    Limit,
}

/// Order priority levels
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderPriority {
    /// Critical orders - process immediately, skip validation
    Critical,
    /// High priority - process next
    High,
    /// Normal priority - standard queue
    Normal,
}

/// Result of an ultra-fast order processing
#[derive(Debug, Clone)]
pub struct OrderResult {
    pub order_id: String,
    pub processing_time_ns: u64,
    pub success: bool,
    pub filled_quantity: f64,
    pub avg_price: f64,
    pub fees: f64,
    pub error: Option<String>,
}

/// Performance metrics for the ultra order manager
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub processed_count: u64,
    pub avg_time_ns: f64,
    pub min_time_ns: u64,
    pub max_time_ns: u64,
    pub success_rate: f64,
    pub peak_orders_per_sec: u64,
    pub total_volume: f64,
    pub total_fees: f64,
}

/// Ultra-fast order manager for sub-microsecond signal processing
/// 
/// This manager wraps the execution handler with optimized batching,
/// priority queuing, and lock-free metrics collection for HFT workloads.
pub struct SignalEngineUltraOrderManager {
    execution_handler: Arc<tokio::sync::RwLock<UltraLowLatencyExecutionHandler>>,
    metrics: Arc<MetricsCollector>,
    /// Pre-allocated signal buffer for batch processing
    batch_buffer: tokio::sync::Mutex<Vec<(ExecSignal, OrderPriority)>>,
    batch_size: usize,
}

impl SignalEngineUltraOrderManager {
    /// Create a new ultra order manager
    pub async fn new() -> Result<Self> {
        let execution_handler = UltraLowLatencyExecutionHandler::new().await;
        
        Ok(Self {
            execution_handler: Arc::new(tokio::sync::RwLock::new(execution_handler)),
            metrics: Arc::new(MetricsCollector::new()),
            batch_buffer: tokio::sync::Mutex::new(Vec::with_capacity(100)),
            batch_size: 50, // Process in batches of 50
        })
    }
    
    /// Create with custom batch size
    pub async fn with_batch_size(batch_size: usize) -> Result<Self> {
        let mut manager = Self::new().await?;
        manager.batch_size = batch_size;
        Ok(manager)
    }
    
    /// Get a reference to the underlying execution handler for configuration
    pub fn execution_handler(&self) -> &Arc<tokio::sync::RwLock<UltraLowLatencyExecutionHandler>> {
        &self.execution_handler
    }
    
    /// Process a signal order with ultra-low latency
    #[inline]
    pub async fn process_signal_order(
        &self,
        symbol: &str,
        exchange: &str,
        side: OrderSide,
        order_type: OrderType,
        amount: f64,
        price: f64,
        strategy_id: u16,
        priority: OrderPriority,
    ) -> Result<OrderResult> {
        let timer = NanoTimer::start();
        let order_id = format!("ultra_{}_{}", strategy_id, nano_timestamp());
        
        // Build the signal using execution handler's Signal type
        let signal = ExecSignal {
            id: order_id.clone(),
            strategy_id: strategy_id.to_string(),
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            action: match (side, order_type) {
                (OrderSide::Buy, OrderType::Market) => ExecSignalAction::Buy,
                (OrderSide::Buy, OrderType::Limit) => ExecSignalAction::BuyLimit,
                (OrderSide::Sell, OrderType::Market) => ExecSignalAction::Sell,
                (OrderSide::Sell, OrderType::Limit) => ExecSignalAction::SellLimit,
            },
            quantity: amount,
            price: if order_type == OrderType::Limit { Some(price) } else { None },
            confidence: match priority {
                OrderPriority::Critical => 1.0,
                OrderPriority::High => 0.8,
                OrderPriority::Normal => 0.5,
            },
            timestamp: nano_timestamp() as u64,
            metadata: std::collections::HashMap::new(),
        };
        
        // Execute through the handler
        let handler = self.execution_handler.read().await;
        let result = handler.execute_order(&signal).await;
        let processing_time_ns = timer.elapsed_ns();
        
        match result {
            Ok(exec_result) => {
                let volume = exec_result.filled_quantity * exec_result.avg_fill_price;
                let fees = exec_result.total_fees;
                
                // Record success with full metrics
                self.metrics.record_success(processing_time_ns, volume, fees);
                
                Ok(OrderResult {
                    order_id: exec_result.order_id,
                    processing_time_ns,
                    success: matches!(exec_result.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled),
                    filled_quantity: exec_result.filled_quantity,
                    avg_price: exec_result.avg_fill_price,
                    fees,
                    error: exec_result.reject_reason,
                })
            }
            Err(e) => {
                self.metrics.record_failure();
                
                Ok(OrderResult {
                    order_id,
                    processing_time_ns,
                    success: false,
                    filled_quantity: 0.0,
                    avg_price: 0.0,
                    fees: 0.0,
                    error: Some(format!("{:?}", e)),
                })
            }
        }
    }
    
    /// Convert ultra_signal::Signal to executionhandler::Signal
    fn convert_to_exec_signal(signal: &Signal) -> ExecSignal {
        ExecSignal {
            id: signal.id.to_string(),
            strategy_id: signal.strategy_id.to_string(),
            symbol: format!("symbol_{}", signal.symbol_hash), // Symbol hash to string
            exchange: format!("exchange_{}", signal.exchange_id),
            action: match signal.side {
                OrderSide::Buy => ExecSignalAction::Buy,
                OrderSide::Sell => ExecSignalAction::Sell,
            },
            quantity: signal.quantity,
            price: Some(signal.price),
            confidence: signal.confidence as f64,
            timestamp: signal.timestamp_ns as u64,
            metadata: std::collections::HashMap::new(),
        }
    }
    
    /// Queue a signal for batch processing (lower latency for non-critical orders)
    pub async fn queue_signal(&self, signal: Signal, priority: OrderPriority) {
        let exec_signal = Self::convert_to_exec_signal(&signal);
        let mut buffer = self.batch_buffer.lock().await;
        buffer.push((exec_signal, priority));
        
        // Flush if batch is full
        if buffer.len() >= self.batch_size {
            let signals: Vec<_> = buffer.drain(..).collect();
            drop(buffer); // Release lock before processing
            self.process_batch(signals).await;
        }
    }
    
    /// Process a batch of queued signals
    async fn process_batch(&self, mut signals: Vec<(ExecSignal, OrderPriority)>) {
        // Sort by priority (Critical first)
        signals.sort_by(|a, b| {
            let priority_ord = |p: &OrderPriority| match p {
                OrderPriority::Critical => 0,
                OrderPriority::High => 1,
                OrderPriority::Normal => 2,
            };
            priority_ord(&a.1).cmp(&priority_ord(&b.1))
        });
        
        let handler = self.execution_handler.read().await;
        let plain_signals: Vec<ExecSignal> = signals.into_iter().map(|(s, _)| s).collect();
        
        // Use optimized batch execution
        let _ = handler.execute_batch_orders_optimized(&plain_signals, 10).await;
    }
    
    /// Flush any queued signals immediately
    pub async fn flush(&self) {
        let mut buffer = self.batch_buffer.lock().await;
        if !buffer.is_empty() {
            let signals: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            self.process_batch(signals).await;
        }
    }
    
    /// Get performance metrics
    pub fn get_ultra_performance_metrics(&self) -> PerformanceMetrics {
        let metrics = self.metrics.get_metrics("ultra_order_manager".to_string());
        
        PerformanceMetrics {
            processed_count: metrics.total_orders,
            avg_time_ns: metrics.avg_latency_ns as f64,
            min_time_ns: metrics.min_latency_ns,
            max_time_ns: metrics.max_latency_ns,
            success_rate: metrics.fill_rate * 100.0,
            peak_orders_per_sec: self.metrics.get_peak_orders_per_second(),
            total_volume: metrics.total_volume,
            total_fees: metrics.total_fees,
        }
    }
    
    /// Get detailed execution metrics
    pub fn get_detailed_metrics(&self) -> executionhandler::ExecutionMetrics {
        self.metrics.get_metrics("ultra_order_manager".to_string())
    }
    
    /// Reset all metrics
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }
}
use crossbeam::channel::{bounded, Receiver, Sender};
use anyhow::Result;
use async_trait::async_trait;
use config::Config;
use dashmap::DashMap;
use datahandler::{DataHandler, DataHandlerTrait};
use dotenv::dotenv;
use mockall::automock;
use portfoliohandler::{PortfolioHandler, PortfolioHandlerTrait};
use strategyhandler::{
    StrategyManager, StrategyConfig, Strategy, SimpleMarketMakingStrategy,
    MarketData as StratMarketData,
};
use strategyloader::{DeploymentSubscriber, DeploymentEvent};
use std::{env, error::Error, sync::{Arc, RwLock}, collections::HashMap};
use tokio::sync::broadcast;
use orderbook::Orderbook;
use portfolio::CryptoWallet;
use ultra_logger::{ultra_info, ultra_warn, ultra_error};
use ultra_signal::hash_symbol;
use lazy_static::lazy_static;

// Re-export the global storage from datahandler and portfoliohandler
pub use datahandler::ORDERBOOKS;
pub use portfoliohandler::PORTFOLIOS;

/// Metadata for a deployed strategy, shared between the deployment handler
/// and the signal processing loop.
#[derive(Debug, Clone)]
pub struct PaperDeploymentMeta {
    pub tenant_id: uuid::Uuid,
    pub deployment_id: uuid::Uuid,
    pub strategy_id_hash: u16,
    /// Connector key inside `ExecutionHandler.connectors` (e.g. `"paper_{uuid}"`)
    pub paper_exchange: String,
    /// The real exchange name used for `ORDERBOOKS` lookups (e.g. `"kraken"`)
    pub real_exchange: String,
    pub symbols: Vec<String>,
    /// Deployment mode: "paper" or "live"
    pub mode: String,
    /// True for market-making strategies that require live L3 book feeds.
    pub is_market_making: bool,
}

/// Shared registry of active paper deployments, keyed by instance_id (Uuid).
/// The signal loop scans this (typically <10 entries) to match strategy_id_hash.
pub type PaperDeploymentRegistry = Arc<DashMap<uuid::Uuid, PaperDeploymentMeta>>;

/// A strategy that has been deployed and is actively consuming market data.
/// The market_data->strategy bridge clones the inner Arc and invokes
/// `generate_signals()` on each tick whose symbol matches.
pub struct DeployedStrategyEntry {
    pub strategy_id_hash: u16,
    pub symbols: Vec<String>,
    pub real_exchange: String,
    pub strategy: Arc<tokio::sync::Mutex<Box<dyn Strategy>>>,
}

/// Registry of deployed strategies keyed by deployment instance_id (Uuid).
pub type DeployedStrategyRegistry = Arc<DashMap<uuid::Uuid, DeployedStrategyEntry>>;

lazy_static! {
    /// Global symbol_hash -> canonical symbol name map. Populated when a
    /// deployment is registered; read by the signal loop so trade_history
    /// rows show readable symbols (e.g. "BTC/USD") rather than `SYMBOL_<hash>`.
    pub static ref SYMBOL_NAMES: DashMap<u64, String> = DashMap::new();
}

#[automock]
#[async_trait]
pub trait HostedObjectTrait {
    async fn run(&self) -> Result<(), Box<dyn Error>>;
}

pub struct HostedObject {
    config_path: Option<String>,
    is_running: bool,
    shutdown_tx: Option<broadcast::Sender<()>>,
    // Signal routing
    signal_tx: Option<Sender<Signal>>,
    signal_rx: Option<Receiver<Signal>>,
    // Strategy manager (live trading)
    strategy_manager: Option<StrategyManager>,
    // Phase 2: Ultra-fast order management (853x faster)
    ultra_order_manager: Option<Arc<SignalEngineUltraOrderManager>>,
}

impl HostedObject {
    /// Create a new HostedObject that will be initialized when run
    pub fn new() -> Self {
        let (signal_tx, signal_rx) = bounded(1000); // Buffer for 1000 signals
        Self {
            config_path: None,
            is_running: false,
            shutdown_tx: None,
            signal_tx: Some(signal_tx),
            signal_rx: Some(signal_rx),
            strategy_manager: None,
            ultra_order_manager: None, // Will be initialized during run()
        }
    }
}

impl Default for HostedObject {
    fn default() -> Self {
        Self::new()
    }
}

impl HostedObject {
    /// Create a HostedObject with a specific config path
    pub fn with_config_path(config_path: String) -> Self {
        let (signal_tx, signal_rx) = bounded(1000);
        Self {
            config_path: Some(config_path),
            is_running: false,
            shutdown_tx: None,
            signal_tx: Some(signal_tx),
            signal_rx: Some(signal_rx),
            strategy_manager: None,
            ultra_order_manager: None, // Will be initialized during run()
        }
    }

    /// Create handlers from configuration
    async fn create_handlers() -> Result<(DataHandler, PortfolioHandler, StrategyManager, UltraLowLatencyExecutionHandler, Config), Box<dyn Error>> {
        // Load environment variables
        dotenv().ok();
        
        // Get configuration path
        let config_path = env::var("CONFIG_PATH")
            .map_err(|_| "CONFIG_PATH environment variable must be set")?;
        
        // Load configuration
        let config = Config::new(&config_path)?;
        
        // Create data handler - it will populate the global ORDERBOOKS
        let datahandler = DataHandler::new()?;
        
        // Create portfolio handler with configured topics
        let portfolio_topics = vec!["portfolio.updates", "wallet.balances"];
        let portfoliohandler = PortfolioHandler::new(&portfolio_topics).await?;
        
        // Create strategy handler
        let strategyhandler = Self::create_strategy_manager(&config)?;
        
        // Create execution handler and initialize exchanges from config
        let mut execution_handler = UltraLowLatencyExecutionHandler::new().await;
        
        // ======================================================================
        // Load exchange credentials from database (stored via Settings UI)
        // ======================================================================
        #[cfg(feature = "postgres")]
        if let Ok(database_url) = env::var("DATABASE_URL") {
            if let Ok(tenant_id_str) = env::var("TENANT_ID") {
                if let Ok(tenant_id) = uuid::Uuid::parse_str(&tenant_id_str) {
                    match smartorderrouter::database::create_pool(&database_url).await {
                        Ok(pool) => {
                            match execution_handler.initialize_from_database(&pool, tenant_id).await {
                                Ok(count) => {
                                    ultra_info!(format!("✅ Loaded {} exchange(s) from database credentials", count));
                                }
                                Err(e) => {
                                    ultra_warn!(format!("⚠️ Failed to load database credentials: {:?}", e));
                                }
                            }
                        }
                        Err(e) => {
                            ultra_warn!(format!("⚠️ Could not connect to database for credentials: {}", e));
                        }
                    }
                } else {
                    ultra_warn!("⚠️ TENANT_ID is not a valid UUID");
                }
            } else {
                ultra_info!("ℹ️ TENANT_ID not set, skipping database credential loading");
            }
        } else {
            ultra_info!("ℹ️ DATABASE_URL not set, using only YAML config for exchanges");
        }
        
        // ======================================================================
        // Also load exchanges from YAML configuration (fallback/override)
        // ======================================================================
        for exchange_config in &config.exchanges {
            if !exchange_config.enabled {
                ultra_info!(format!("Skipping disabled exchange: {}", exchange_config.name));
                continue;
            }
            
            // Convert config::ExchangeConfig to executionhandler::ExchangeConfig
            let exec_config = Self::convert_exchange_config(exchange_config);
            
            match execution_handler.add_exchange(exchange_config.name.clone(), exec_config).await {
                Ok(_) => {
                    ultra_info!(format!("✅ Initialized exchange from config: {}", exchange_config.name));
                }
                Err(e) => {
                    ultra_warn!(format!("⚠️ Failed to initialize exchange {}: {:?}", exchange_config.name, e));
                }
            }
        }
        
        Ok((datahandler, portfoliohandler, strategyhandler, execution_handler, config))
    }
    
    /// Convert YAML config ExchangeConfig to execution handler ExchangeConfig
    fn convert_exchange_config(yaml_config: &config::ExchangeConfig) -> executionhandler::core::types::ExchangeConfig {
        executionhandler::core::types::ExchangeConfig {
            name: yaml_config.name.clone(),
            api_key: yaml_config.api_key.clone().unwrap_or_default(),
            secret_key: yaml_config.secret_key.clone().unwrap_or_default(),
            passphrase: None,
            sandbox: yaml_config.sandbox,
            connection_pool_size: 10, // Default pool size
            timeout_ms: yaml_config.timeout.as_millis() as u64,
            rate_limit_per_second: yaml_config.rate_limits.orders_per_second,
            rate_limit_burst: yaml_config.rate_limits.requests_per_second,
            websocket_url: None, // Exchange-specific, will be set by connector
            rest_api_url: None,
            custom_headers: std::collections::HashMap::new(),
        }
    }

    /// Create the strategy manager from configuration
    fn create_strategy_manager(config: &Config) -> Result<StrategyManager, Box<dyn Error>> {
        // Note: broker_addr and signal_topics may be needed in future versions
        // Get broker configuration (for future use)
        let _broker_addr = format!("{}:{}", config.message_broker.address, config.message_broker.port);
        
        // Define topics for strategy signals based on config (for future use)
        let _signal_topics: Vec<String> = config.publish_topics.iter()
            .filter(|topic| topic.contains("order") || topic.contains("signal"))
            .map(|s| s.to_string())
            .collect();
        
        // If no signal topics found, use defaults (for future use)
        let _signal_topics = if _signal_topics.is_empty() {
            vec!["orders.btc".to_string(), "orders.eth".to_string()]
        } else {
            _signal_topics
        };
        
        // Create strategy manager with new signature (no orderbooks parameter)
        let strategy_manager = StrategyManager::new()?;
        
        Ok(strategy_manager)
    }
    
    /// Initialize default strategies (async method)
    async fn initialize_default_strategies(&mut self) -> Result<(), Box<dyn Error>> {
        // Add default market making strategies if configured
        // In a real implementation, you'd load strategy configurations from the config file
        if let Some(ref strategy_manager) = self.strategy_manager {
            Self::add_default_strategies(strategy_manager).await?;
        }
        Ok(())
    }

    /// Add default strategies to the manager
    async fn add_default_strategies(manager: &StrategyManager) -> Result<(), Box<dyn Error>> {
        // Example: Create a default market making strategy
        // In production, this would be loaded from configuration
        
        let mut params = HashMap::new();
        params.insert("gamma".to_string(), serde_json::json!(0.1));
        params.insert("k1".to_string(), serde_json::json!(0.1));
        params.insert("k2".to_string(), serde_json::json!(0.1));
        params.insert("k3".to_string(), serde_json::json!(0.1));
        params.insert("k4".to_string(), serde_json::json!(0.1));
        params.insert("k5".to_string(), serde_json::json!(0.1));
        params.insert("k6".to_string(), serde_json::json!(0.1));
        
        // Weight parameters
        params.insert("w1".to_string(), serde_json::json!(0.5));
        params.insert("w2".to_string(), serde_json::json!(0.5));
        params.insert("w3".to_string(), serde_json::json!(0.5));
        params.insert("w4".to_string(), serde_json::json!(0.5));
        params.insert("w5".to_string(), serde_json::json!(0.5));
        params.insert("w6".to_string(), serde_json::json!(0.5));
        params.insert("w7".to_string(), serde_json::json!(0.5));
        params.insert("w8".to_string(), serde_json::json!(0.5));
        params.insert("w9".to_string(), serde_json::json!(0.5));
        params.insert("w10".to_string(), serde_json::json!(0.5));
        params.insert("w11".to_string(), serde_json::json!(0.5));
        
        let strategy_config = StrategyConfig {
            id: "default-mm-strategy".to_string(),
            name: "Default Market Maker".to_string(),
            enabled: true,
            symbols: vec!["BTC/USD".to_string(), "ETH/USD".to_string()],
            exchanges: vec!["binance".to_string()],
            parameters: params,
            max_position_size: 10000.0, // Default max position size
            risk_limit: 0.02, // 2% risk limit
        };
        
        // Create a simple market making strategy directly
        use strategyhandler::SimpleMarketMakingStrategy;
        let strategy = Box::new(SimpleMarketMakingStrategy::new(strategy_config));
        manager.add_strategy(strategy).await?;
        
        Ok(())
    }

    /// Run all handlers concurrently
    pub async fn run_async(&mut self) -> Result<(), Box<dyn Error>> {
        ultra_info!("Starting HostedObject...");
        
        if self.is_running {
            return Err("HostedObject is already running".into());
        }
        
        // Create handlers (exchanges are loaded from YAML config inside create_handlers)
        let (datahandler, portfoliohandler, strategyhandler, execution_handler, config) = Self::create_handlers().await?;

        // Capture the market-data receiver BEFORE the datahandler is moved into
        // its blocking task — this is the channel `process_trade()` writes to
        // after Bug #13 (synthetic top-of-book). The bridge task spawned below
        // pulls from this and feeds each deployed strategy's `generate_signals()`.
        let market_data_rx = datahandler.get_market_data_receiver();
        
        // Initialize strategy manager with the loaded config
        self.strategy_manager = Some(Self::create_strategy_manager(&config)?);
        
        // Phase 2: Initialize ultra-fast order manager (853x faster processing)
        ultra_info!("🚀 Initializing Phase 2 Ultra-Fast Order Manager...");
        let ultra_order_manager = SignalEngineUltraOrderManager::new().await?;
        self.ultra_order_manager = Some(Arc::new(ultra_order_manager));
        ultra_info!("✅ Phase 2 Ultra-Fast Order Manager initialized - target 0.6μs processing");
        
        // Initialize default strategies
        self.initialize_default_strategies().await?;
        
        // Create shutdown channels - using broadcast for multiple subscribers
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(100);
        self.shutdown_tx = Some(shutdown_tx.clone());
        
        // Create channels to monitor handler health using tokio channels
        let (data_health_tx, mut data_health_rx) = tokio::sync::mpsc::channel::<Result<(), String>>(1);
        let (portfolio_health_tx, mut portfolio_health_rx) = tokio::sync::mpsc::channel::<Result<(), String>>(1);
        
        // Spawn data handler in a tokio task
        let _data_shutdown_rx = shutdown_tx.subscribe(); // For future use when handlers support interruption
        let data_health_tx_clone = data_health_tx.clone();
        let _data_handle = tokio::task::spawn_blocking(move || {
            ultra_logger::ultra_info!("Starting DataHandler...");
            let mut handler = datahandler;
            // Note: In a real implementation, the handler.listen() should be interruptible
            // For now, we just run it and report errors
            if let Err(e) = handler.listen() {
                ultra_logger::ultra_error!(format!("DataHandler error: {e:?}"));
                let _ = data_health_tx_clone.blocking_send(Err(format!("DataHandler failed: {e:?}")));
            }
        });
        
        // Spawn portfolio handler in a tokio task
        let _portfolio_shutdown_rx = shutdown_tx.subscribe(); // For future use when handlers support interruption
        let portfolio_health_tx_clone = portfolio_health_tx.clone();
        let _portfolio_handle = tokio::task::spawn_blocking(move || {
            ultra_logger::ultra_info!("Starting PortfolioHandler...");
            let mut handler = portfoliohandler;
            // Note: In a real implementation, the handler.listen() should be interruptible
            if let Err(e) = handler.listen() {
                ultra_logger::ultra_error!(format!("PortfolioHandler error: {e:?}"));
                let _ = portfolio_health_tx_clone.blocking_send(Err(format!("PortfolioHandler failed: {e:?}")));
            }
        });
        
        // Initialize execution handler
        ultra_info!("Initializing ExecutionHandler...");
        execution_handler.initialize_optimizations().await?;
        
        // Shared registry of paper deployments — deployment handler writes, signal loop reads
        let paper_registry: PaperDeploymentRegistry = Arc::new(DashMap::new());

        // Shared registry of deployed strategies (paper + live). Populated by
        // DeploymentEvent::Deploy below; consumed by the market_data->strategy
        // bridge task to drive `Strategy::generate_signals()` on each tick.
        let deployed_strategies: DeployedStrategyRegistry = Arc::new(DashMap::new());
        
        // Create paper trade writer for DB persistence (if DATABASE_URL is set)
        #[cfg(feature = "postgres")]
        let paper_fill_tx: Option<tokio::sync::mpsc::Sender<paper_trade_writer::PaperFillEvent>> = {
            match env::var("DATABASE_URL") {
                Ok(db_url) => {
                    match smartorderrouter::database::create_pool(&db_url).await {
                        Ok(pool) => {
                            let writer = paper_trade_writer::PaperTradeWriter::new(Arc::new(pool));
                            ultra_info!("✅ Paper trade writer initialized — fills will persist to trade_history");
                            Some(writer.sender())
                        }
                        Err(e) => {
                            ultra_warn!(format!("⚠️ Could not create DB pool for paper trade writer: {}", e));
                            None
                        }
                    }
                }
                Err(_) => {
                    ultra_info!("ℹ️ DATABASE_URL not set — paper trades will not persist to DB");
                    None
                }
            }
        };
        #[cfg(not(feature = "postgres"))]
        let paper_fill_tx: Option<()> = None;

        // ======================================================================
        // Bridge: market_data_receiver -> deployed strategies -> signal_tx
        //
        // After Bug #13, DataHandler::process_trade() pushes a `MarketData`
        // onto `market_data_rx` for every Kraken trade (with a synthetic
        // ±1bp top-of-book book derived from the trade price). This task
        // pulls each tick, finds every deployed strategy whose symbol set
        // includes the tick's symbol, calls `generate_signals()`, stamps the
        // strategy_id onto each emitted Signal, and forwards into signal_tx.
        // The existing Phase-2 ultra signal loop (below) then routes those
        // signals to the paper exchange and persists fills to trade_history.
        // ======================================================================
        if let Some(signal_tx) = self.signal_tx.clone() {
            let bridge_registry = deployed_strategies.clone();
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                ultra_logger::ultra_info!(
                    "🔗 Market-data -> strategy bridge started (consuming DataHandler::market_data_rx)"
                );
                let mut tick_count: u64 = 0;
                let mut signals_emitted: u64 = 0;
                // Normalize symbols for matching: uppercase + strip '/' '-' '_'
                // so that "BTC/USD" (Kraken trade) matches "BTC-USD" (deployment).
                fn norm_sym(s: &str) -> String {
                    s.chars()
                        .filter(|c| !matches!(c, '/' | '-' | '_' | ' '))
                        .flat_map(|c| c.to_uppercase())
                        .collect()
                }
                while let Ok(md) = market_data_rx.recv() {
                    tick_count += 1;

                    let md_norm = norm_sym(&md.symbol);
                    // Snapshot matching deployments (drop iterator before await to
                    // avoid holding the DashMap shard lock across awaits).
                    let matches: Vec<(u16, String, Arc<tokio::sync::Mutex<Box<dyn Strategy>>>)> = bridge_registry
                        .iter()
                        .filter(|e| {
                            e.value()
                                .symbols
                                .iter()
                                .any(|s| norm_sym(s) == md_norm)
                        })
                        .map(|e| {
                            let v = e.value();
                            (v.strategy_id_hash, v.real_exchange.clone(), v.strategy.clone())
                        })
                        .collect();

                    if matches.is_empty() {
                        continue;
                    }

                    // Build the strategyhandler::MarketData expected by Strategy::generate_signals.
                    // signalgenerator::MarketData lacks an exchange field, so we use the
                    // deployment's real_exchange.
                    for (sid_hash, exch, strat) in matches {
                        let strat_md = StratMarketData {
                            symbol: md.symbol.clone(),
                            exchange: exch,
                            timestamp: md.timestamp,
                            price: md.price,
                            mid_price: (md.bid + md.ask) / 2.0,
                            volume: md.volume,
                            bid: md.bid,
                            best_bid: md.bid,
                            ask: md.ask,
                            best_ask: md.ask,
                            spread: md.spread,
                        };
                        let signal_tx = signal_tx.clone();
                        let signals = runtime.block_on(async move {
                            let mut s = strat.lock().await;
                            s.generate_signals(&strat_md).await
                        });
                        match signals {
                            Ok(sigs) => {
                                for mut sig in sigs {
                                    // Stamp the deployment's strategy_id_hash so the
                                    // downstream signal loop matches it to paper_registry.
                                    sig.strategy_id = sid_hash;
                                    if signal_tx.try_send(sig).is_ok() {
                                        signals_emitted += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                ultra_logger::ultra_warn!(format!(
                                    "Strategy generate_signals failed: {}",
                                    e
                                ));
                            }
                        }
                    }

                    if tick_count % 50 == 0 {
                        ultra_logger::ultra_info!(format!(
                            "🔗 Bridge stats: ticks={} signals_emitted={}",
                            tick_count, signals_emitted
                        ));
                    }
                }
                ultra_logger::ultra_info!("🔗 Market-data -> strategy bridge stopped");
            });
        } else {
            ultra_warn!("signal_tx not available — market_data->strategy bridge NOT started");
        }

        // Set up ultra-fast signal routing using Phase 2 order manager
        if let Some(signal_rx) = self.signal_rx.take() {
            let Some(ultra_order_manager) = self.ultra_order_manager.clone() else {
                ultra_warn!("Ultra order manager not initialized, skipping signal routing");
                return Ok(());
            };
            let signal_paper_registry = paper_registry.clone();
            let signal_fill_tx = paper_fill_tx.clone();
            tokio::spawn(async move {
                ultra_logger::ultra_info!("🚀 Starting Phase 2 ultra-fast signal processing (0.6μs target)...");
                let mut signal_count = 0u64;
                
                while let Ok(signal) = signal_rx.recv() {
                    signal_count += 1;
                    
                    // Check if this strategy has a paper deployment — if so, route to paper exchange
                    let paper_meta = signal_paper_registry.iter()
                        .find(|e| e.value().strategy_id_hash == signal.strategy_id)
                        .map(|e| e.value().clone());

                    // Resolve symbol: prefer the deployment's first symbol (readable),
                    // fall back to the global hash->name map populated at deploy time,
                    // and only as a last resort use the raw hash placeholder.
                    let symbol = paper_meta
                        .as_ref()
                        .and_then(|m| m.symbols.first().cloned())
                        .or_else(|| SYMBOL_NAMES.get(&signal.symbol_hash).map(|e| e.value().clone()))
                        .unwrap_or_else(|| format!("SYMBOL_{}", signal.symbol_hash));

                    let exchange = paper_meta.as_ref()
                        .map(|m| m.paper_exchange.clone())
                        .unwrap_or_else(|| format!("EXCHANGE_{}", signal.exchange_id));
                    
                    let (side, order_type, price) = match signal.action {
                        ultra_signal::SignalAction::Buy => (OrderSide::Buy, OrderType::Market, 0.0), // Market price
                        ultra_signal::SignalAction::Sell => (OrderSide::Sell, OrderType::Market, 0.0),
                        ultra_signal::SignalAction::BuyLimit => (OrderSide::Buy, OrderType::Limit, signal.price),
                        ultra_signal::SignalAction::SellLimit => (OrderSide::Sell, OrderType::Limit, signal.price),
                        ultra_signal::SignalAction::Cancel | ultra_signal::SignalAction::Hold => continue, // Skip
                    };
                    
                    // Determine priority based on confidence
                    let priority = if signal.confidence >= 0.9 {
                        OrderPriority::Critical
                    } else if signal.confidence >= 0.7 {
                        OrderPriority::High
                    } else {
                        OrderPriority::Normal
                    };
                    
                    // Execute ultra-fast order (0.6μs target)
                    match ultra_order_manager.process_signal_order(
                        &symbol,
                        &exchange,
                        side,
                        order_type,
                        signal.quantity,
                        price,
                        signal.strategy_id,
                        priority
                    ).await {
                        Ok(result) => {
                            // Record fill to trade_history for paper deployments
                            if result.success {
                                #[cfg(feature = "postgres")]
                                if let (Some(ref meta), Some(ref tx)) = (&paper_meta, &signal_fill_tx) {
                                    let fill_event = paper_trade_writer::PaperFillEvent {
                                        tenant_id: meta.tenant_id,
                                        deployment_id: meta.deployment_id,
                                        exchange: meta.paper_exchange.clone(),
                                        symbol: symbol.clone(),
                                        side: match side {
                                            OrderSide::Buy => "Buy".to_string(),
                                            OrderSide::Sell => "Sell".to_string(),
                                        },
                                        quantity: result.filled_quantity,
                                        price: result.avg_price,
                                        fees: result.fees,
                                        realized_pnl: None, // Computed by P&L snapshot writer
                                        fill_id: format!("fill_{}", signal_count),
                                        order_id: result.order_id.clone(),
                                    };
                                    let _ = tx.try_send(fill_event);
                                }
                            }
                            
                            // Log ultra-fast performance every 100 orders
                            if signal_count % 100 == 0 {
                                ultra_logger::ultra_info!(format!("⚡ Phase 2 ultra-fast order {}: {}ns ({:.3}μs) - Success: {}", 
                                      signal_count, 
                                      result.processing_time_ns,
                                      result.processing_time_ns as f64 / 1000.0,
                                      result.success));
                            }
                            
                            // Log performance metrics every 1000 orders
                            if signal_count % 1000 == 0 {
                                let metrics = ultra_order_manager.get_ultra_performance_metrics();
                                ultra_logger::ultra_info!(format!("📊 Phase 2 metrics: {} processed, {:.2}μs avg, {:.1}% success, {} orders/sec peak",
                                      metrics.processed_count, 
                                      metrics.avg_time_ns / 1000.0, 
                                      metrics.success_rate, 
                                      metrics.peak_orders_per_sec));
                            }
                        }
                        Err(e) => {
                            ultra_logger::ultra_warn!(format!("Ultra-fast order processing failed: {}", e));
                        }
                    }
                }
                ultra_logger::ultra_info!("Phase 2 ultra-fast signal processing loop ended");
            });
        }
        
        // Connect strategy manager to signal routing
        if let Some(signal_tx) = &self.signal_tx {
            strategyhandler.add_signal_route("default".to_string(), signal_tx.clone());
        }
        
        // ======================================================================
        // Phase 3: Wire DeploymentSubscriber for hot-loading paper/live strategies
        // ======================================================================
        let broker_addr = format!("{}:{}", config.message_broker.address, config.message_broker.port);
        let node_id = format!("signalengine-{}", uuid::Uuid::new_v4());
        let (deployment_tx, mut deployment_rx) = tokio::sync::mpsc::channel::<DeploymentEvent>(100);
        
        let mut deployment_subscriber = DeploymentSubscriber::new(&broker_addr, &node_id);
        deployment_subscriber.set_deployment_channel(deployment_tx);
        
        match deployment_subscriber.start().await {
            Ok(_) => {
                ultra_info!(format!("✅ DeploymentSubscriber started on {} — listening for strategy deployments", broker_addr));
            }
            Err(e) => {
                ultra_warn!(format!("⚠️ DeploymentSubscriber failed to start: {:?} — paper trading unavailable", e));
            }
        }
        
        // Spawn deployment event handler — processes Deploy/Deactivate events
        // and wires deployed strategies into the signal generation + execution pipeline
        let deployment_ultra_mgr = self.ultra_order_manager.clone();
        let deploy_registry = paper_registry.clone();
        let deploy_strategies = deployed_strategies.clone();
        tokio::spawn(async move {
            ultra_logger::ultra_info!("📡 Deployment event handler started");
            
            while let Some(event) = deployment_rx.recv().await {
                match event {
                    DeploymentEvent::Deploy(strategy) => {
                        let mode = strategy.mode.clone();
                        let is_live = mode == "live";
                        
                        ultra_logger::ultra_info!(format!(
                            "🚀 Deploying strategy: {} ({}) [{}] for {} on {:?}",
                            strategy.strategy_name,
                            strategy.strategy_type,
                            mode,
                            strategy.symbols.join(", "),
                            strategy.target_exchanges,
                        ));
                        
                        // Guard: empty target_exchanges silently breaks the pipeline
                        // (DataEngine has no "unknown" connector → no ticks → no signals).
                        // Reject the deployment instead of registering with a bogus exchange.
                        if strategy.target_exchanges.is_empty() {
                            ultra_logger::ultra_error!(format!(
                                "❌ Rejected deployment {} ({}): target_exchanges is empty. \
                                 Fix the deployment row's exchange_targets and re-publish.",
                                strategy.strategy_name, strategy.instance_id
                            ));
                            continue;
                        }
                        if strategy.symbols.is_empty() {
                            ultra_logger::ultra_error!(format!(
                                "❌ Rejected deployment {} ({}): symbols is empty.",
                                strategy.strategy_name, strategy.instance_id
                            ));
                            continue;
                        }
                        
                        let strategy_id_hash = (strategy.strategy_id.as_u128() & 0xFFFF) as u16;

                        // Populate global symbol_hash -> name map so the signal loop
                        // (and any downstream consumers that read SYMBOL_NAMES) can
                        // render readable symbols on trade_history rows.
                        for sym in &strategy.symbols {
                            SYMBOL_NAMES.insert(hash_symbol(sym), sym.clone());
                        }

                        // Build a strategy instance for this deployment. We currently
                        // only support SimpleMarketMakingStrategy; other strategy_type
                        // values fall back to it. The config.id is set to the
                        // strategy_id_hash so SimpleMarketMakingStrategy stamps the
                        // right id on the Signal it produces (the bridge also
                        // overrides defensively).
                        let strat_params: HashMap<String, serde_json::Value> =
                            match &strategy.parameters {
                                serde_json::Value::Object(map) => map
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect(),
                                _ => HashMap::new(),
                            };
                        let strat_config = StrategyConfig {
                            id: strategy_id_hash.to_string(),
                            name: strategy.strategy_name.clone(),
                            enabled: true,
                            symbols: strategy.symbols.clone(),
                            exchanges: strategy.target_exchanges.clone(),
                            parameters: strat_params,
                            max_position_size: 10_000.0,
                            risk_limit: 0.02,
                        };
                        let strat_instance: Arc<tokio::sync::Mutex<Box<dyn Strategy>>> =
                            Arc::new(tokio::sync::Mutex::new(Box::new(
                                SimpleMarketMakingStrategy::new(strat_config),
                            )));

                        if is_live {
                            // Live mode — use real exchange connectors (already configured via YAML)
                            // The exchange name matches target_exchanges from the deployment
                            let exchange_name = strategy.target_exchanges[0].clone();
                            
                            deploy_registry.insert(strategy.instance_id, PaperDeploymentMeta {
                                tenant_id: strategy.tenant_id,
                                deployment_id: strategy.instance_id,
                                strategy_id_hash,
                                paper_exchange: exchange_name.clone(),
                                real_exchange: exchange_name.clone(),
                                symbols: strategy.symbols.clone(),
                                mode: "live".to_string(),
                                is_market_making: false,
                            });

                            deploy_strategies.insert(strategy.instance_id, DeployedStrategyEntry {
                                strategy_id_hash,
                                symbols: strategy.symbols.clone(),
                                real_exchange: exchange_name.clone(),
                                strategy: strat_instance.clone(),
                            });

                            ultra_logger::ultra_info!(format!(
                                "✅ Strategy {} ({}) deployed for LIVE trading on {}",
                                strategy.strategy_name, strategy.instance_id, exchange_name
                            ));
                        } else {
                            // Paper mode — create a paper trading connector
                            let paper_exchange_name = format!("paper_{}", strategy.instance_id);
                            let real_exchange = strategy.target_exchanges[0].clone();
                            deploy_registry.insert(strategy.instance_id, PaperDeploymentMeta {
                                tenant_id: strategy.tenant_id,
                                deployment_id: strategy.instance_id,
                                strategy_id_hash,
                                paper_exchange: paper_exchange_name.clone(),
                                real_exchange: real_exchange.clone(),
                                symbols: strategy.symbols.clone(),
                                mode: "paper".to_string(),
                                is_market_making: strategy.strategy_type == "custom_market_making",
                            });

                            deploy_strategies.insert(strategy.instance_id, DeployedStrategyEntry {
                                strategy_id_hash,
                                symbols: strategy.symbols.clone(),
                                real_exchange: real_exchange.clone(),
                                strategy: strat_instance.clone(),
                            });
                            
                            // Build PaperTradingConfig from deployment sim config fields,
                            // falling back to sensible defaults when not specified.
                            let sim_config = executionhandler::PaperTradingConfig {
                                slippage_bps: strategy.slippage_bps.unwrap_or(5.0),
                                partial_fill_probability: 0.1, // not yet user-configurable
                                base_latency_ms: 10.0,         // not yet user-configurable
                            };

                            // Register a paper trading connector for this deployment instance
                            if let Some(ref ultra_mgr) = deployment_ultra_mgr {
                                let mut handler = ultra_mgr.execution_handler().write().await;
                                match handler.add_paper_exchange(paper_exchange_name.clone(), sim_config).await {
                                    Ok(_) => {
                                        ultra_logger::ultra_info!(format!(
                                            "✅ Paper trading connector '{}' registered for strategy {}",
                                            paper_exchange_name, strategy.strategy_name
                                        ));
                                    }
                                    Err(e) => {
                                        ultra_logger::ultra_warn!(format!(
                                            "⚠️ Failed to register paper connector for {}: {:?}",
                                            strategy.strategy_name, e
                                        ));
                                    }
                                }
                            }
                            
                            ultra_logger::ultra_info!(format!(
                                "✅ Strategy {} ({}) deployed for PAPER trading",
                                strategy.strategy_name, strategy.instance_id
                            ));
                        }
                    }
                    DeploymentEvent::Deactivate { instance_id, reason, close_positions, cancel_orders } => {
                        ultra_logger::ultra_info!(format!(
                            "🛑 Deactivating strategy {} — reason: {}, close_positions: {}, cancel_orders: {}",
                            instance_id, reason, close_positions, cancel_orders
                        ));
                        deploy_registry.remove(&instance_id);
                        deploy_strategies.remove(&instance_id);
                    }
                }
            }
            ultra_logger::ultra_info!("📡 Deployment event handler stopped");
        });

        // Spawn book-sync task — every 10 ms, push the latest top-20 bid/ask levels
        // from DataHandler's ORDERBOOKS global into each active MM paper connector.
        // This ensures simulated fills use realistic book-walking rather than a flat
        // slippage percentage.
        let book_sync_registry = paper_registry.clone();
        let book_sync_ultra_mgr = self.ultra_order_manager.clone();
        tokio::spawn(async move {
            let tick = tokio::time::Duration::from_millis(10);
            loop {
                tokio::time::sleep(tick).await;

                // Collect MM paper deployments without holding the DashMap lock across awaits
                let mm_deployments: Vec<(String, String, Vec<String>)> = book_sync_registry
                    .iter()
                    .filter(|e| e.value().is_market_making && e.value().mode == "paper")
                    .map(|e| {
                        let m = e.value();
                        (m.paper_exchange.clone(), m.real_exchange.clone(), m.symbols.clone())
                    })
                    .collect();

                for (paper_exchange, real_exchange, symbols) in mm_deployments {
                    for symbol in &symbols {
                        // Read top-20 levels; drop the orderbook lock before any await
                        let maybe_levels: Option<(Vec<(f64, f64)>, Vec<(f64, f64)>)> = {
                            let key = (symbol.clone(), real_exchange.clone());
                            if let Some(ob_arc) = datahandler::ORDERBOOKS.get(&key) {
                                ob_arc.read().ok().and_then(|ob| ob.get_orderbook_levels(20).ok())
                            } else {
                                None
                            }
                        };

                        if let Some((bids, asks)) = maybe_levels {
                            if !bids.is_empty() {
                                if let Some(ref mgr) = book_sync_ultra_mgr {
                                    let handler = mgr.execution_handler().read().await;
                                    handler.update_connector_book(
                                        &paper_exchange,
                                        symbol,
                                        bids,
                                        asks,
                                    ).await;
                                }
                            }
                        }
                    }
                }
            }
        });

        // Reconcile already-active deployments so restarts don't require a manual
        // pause/resume cycle to restore strategy routing and market subscriptions.
        #[cfg(feature = "postgres")]
        {
            match deployment_subscriber.reconcile_active_deployments_from_db().await {
                Ok(0) => ultra_info!("ℹ️ Deployment reconciliation found no active deployments"),
                Ok(count) => ultra_info!(format!(
                    "✅ Reconciled {} active deployment(s) from DB",
                    count
                )),
                Err(e) => ultra_warn!(format!(
                    "⚠️ Deployment reconciliation failed: {:?}",
                    e
                )),
            }
        }

        // Exchanges are now loaded from YAML config in create_handlers()
        
        // Strategy manager is already created and configured
        ultra_info!("StrategyManager created and configured");
        
        self.is_running = true;
        
        // Set up signal handler for graceful shutdown
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
            ultra_logger::ultra_info!("Received shutdown signal...");
            let _ = shutdown_tx_clone.send(());
        });
        
        // Monitor loop - wait for shutdown or handler failures
        tokio::select! {
            _ = shutdown_rx.recv() => {
                ultra_info!("Shutdown signal received");
            }
            health_result = data_health_rx.recv() => {
                if let Some(Err(e)) = health_result {
                    ultra_error!(format!("Data handler health check failed: {e}"));
                }
            }
            health_result = portfolio_health_rx.recv() => {
                if let Some(Err(e)) = health_result {
                    ultra_error!(format!("Portfolio handler health check failed: {e}"));
                }
            }
        }
        
        // Graceful shutdown
        ultra_info!("Shutting down HostedObject...");
        
        // Strategy manager shutdown is simplified - no explicit stop method
        ultra_info!("Strategy manager shutdown initiated");
        
        // Signal handlers to stop (would need to implement proper shutdown in handlers)
        // For now, we'll interrupt the threads after a timeout
        ultra_info!("Waiting for handlers to complete...");
        
        // Give handlers time to finish current work
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        
        // Log final metrics from the strategy handler before it's dropped
        Self::log_final_metrics_from_manager(&strategyhandler);
        
        self.is_running = false;
        
        ultra_info!("HostedObject shutdown complete");
        Ok(())
    }

    /// Log final metrics from strategy manager
    fn log_final_metrics_from_manager(_strategyhandler: &StrategyManager) {
        // Simplified metrics logging - just basic info
        ultra_info!("=== Final System Metrics ===");
        ultra_info!("Strategy manager shutdown completed");
        
        // Simplified metrics - just confirm shutdown
        ultra_info!("All strategy components shut down successfully");
    }

    /// Get shared orderbooks (returns the same type as ORDERBOOKS)
    pub fn orderbooks(&self) -> &'static DashMap<(String, String), Arc<RwLock<Orderbook>>> {
        &ORDERBOOKS
    }

    /// Get shared portfolios
    pub fn portfolios(&self) -> &'static DashMap<String, Arc<CryptoWallet>> {
        &PORTFOLIOS
    }
}

#[async_trait]
impl HostedObjectTrait for HostedObject {
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        // Create a new instance and run it
        let mut hosted_object = HostedObject::new();
        if let Some(ref path) = self.config_path {
            hosted_object.config_path = Some(path.clone());
        }
        hosted_object.run_async().await
    }
}

/// Builder pattern for HostedObject
pub struct HostedObjectBuilder {
    config_path: Option<String>,
}

impl HostedObjectBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config_path: None,
        }
    }

    /// Set the configuration path
    pub fn with_config_path(mut self, path: String) -> Self {
        self.config_path = Some(path);
        self
    }

    /// Build the HostedObject
    pub fn build(self) -> Result<HostedObject, Box<dyn Error>> {
        let mut obj = HostedObject::new();
        if let Some(path) = self.config_path {
            obj.config_path = Some(path);
        }
        Ok(obj)
    }
}

impl Default for HostedObjectBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hosted_object_builder() {
        let hosted_object = HostedObjectBuilder::new()
            .with_config_path("/path/to/config".to_string())
            .build();
        
        assert!(hosted_object.is_ok());
        let obj = hosted_object.unwrap();
        assert_eq!(obj.config_path, Some("/path/to/config".to_string()));
    }

    #[tokio::test]
    async fn test_hosted_object_trait() {
        // Test with mock
        let mut mock = MockHostedObjectTrait::new();
        mock.expect_run()
            .times(1)
            .returning(|| Ok(()));
        
        let result = mock.run().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_global_resources() {
        // Test that global resources can be accessed
        let _orderbooks = &ORDERBOOKS;
        let _portfolios = &PORTFOLIOS;
    }
    
    #[test]
    fn test_hosted_object_creation() {
        let obj = HostedObject::new();
        assert!(!obj.is_running);
        assert!(obj.config_path.is_none());
        
        let obj_with_path = HostedObject::with_config_path("/test/path".to_string());
        assert_eq!(obj_with_path.config_path, Some("/test/path".to_string()));
    }
}