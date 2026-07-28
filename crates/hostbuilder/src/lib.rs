#[cfg(feature = "postgres")]
pub mod paper_trade_writer;
#[cfg(feature = "postgres")]
pub mod market_health_writer;
pub mod cross_venue_coordinator;

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
    StrategyManager, StrategyConfig, Strategy,
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

#[cfg(feature = "postgres")]
use dataloader::{MassiveDataProvider, MarketDataProvider, DataRequest, CandleGranularity, Exchange, MarketData};

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
    /// Connector key inside `ExecutionHandler.connectors` (e.g. `"paper_{uuid}"`).
    /// For a dual-venue deployment this is leg 0's connector (live mode:
    /// venues[0]'s own name; paper mode: a dedicated per-leg paper connector).
    pub paper_exchange: String,
    /// Leg 1's connector key for a dual-venue PAPER deployment (`None` for
    /// single-venue deployments and for live mode, where each venue's own
    /// name already is its connector key -- see
    /// `cross_venue_coordinator::connector_key_for_leg`).
    pub paper_exchange_2: Option<String>,
    /// The real exchange venue(s) used for `ORDERBOOKS` lookups (e.g.
    /// `["kraken"]`, or `["kraken", "coinbase"]` for a dual-venue strategy).
    /// Capped at 2 venues per live deployment -- real-money coordination
    /// complexity grows sharply past a two-legged trade. `venues[0]` is the
    /// single value every pre-existing single-venue deployment used to store
    /// as `real_exchange`.
    pub venues: Vec<String>,
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
    /// Venue(s) this deployment is configured for -- see
    /// `PaperDeploymentMeta::venues` for the capped-at-2 rationale.
    pub venues: Vec<String>,
    pub strategy: Arc<tokio::sync::Mutex<Box<dyn Strategy>>>,
}

/// Registry of deployed strategies keyed by deployment instance_id (Uuid).
pub type DeployedStrategyRegistry = Arc<DashMap<uuid::Uuid, DeployedStrategyEntry>>;

/// Normalize a symbol for matching: uppercase + strip separators, so
/// "BTC/USD" (Kraken trade) matches "BTC-USD" (deployment).
fn norm_sym(s: &str) -> String {
    s.chars()
        .filter(|c| !matches!(c, '/' | '-' | '_' | ' '))
        .flat_map(|c| c.to_uppercase())
        .collect()
}

/// Uppercase-only compare for exchange names (no separators to strip).
fn canon_exch(s: &str) -> String {
    s.to_uppercase()
}

/// Decide whether a market-data tick belongs to a deployment, and if so,
/// which of the deployment's configured venues it should be tagged with.
///
/// Single-venue deployments (the common case) match by symbol alone, same
/// as the original symbol-only bridge behavior -- the tick's true origin
/// doesn't matter when there's only one configured venue to route to.
/// Multi-venue deployments additionally require the tick's own originating
/// exchange to match one of the configured venues, since a dual-venue
/// strategy needs to know WHICH of its two venues a given tick belongs to
/// (arbitrage/stat-arb only works if the two legs' data streams are kept
/// distinct). The returned venue is always the deployment's OWN canonical
/// venue string, never the tick's raw exchange label, so downstream
/// exchange_id mapping (see strategyhandler) keeps working exactly as it
/// did before venues-list support was added.
fn match_deployment_venue(
    deployment_venues: &[String],
    deployment_symbols: &[String],
    tick_symbol: &str,
    tick_exchange: &str,
) -> Option<String> {
    if !deployment_symbols.iter().any(|s| norm_sym(s) == norm_sym(tick_symbol)) {
        return None;
    }
    if deployment_venues.len() <= 1 {
        return deployment_venues.first().cloned();
    }
    let tick_exch = canon_exch(tick_exchange);
    deployment_venues
        .iter()
        .find(|venue| canon_exch(venue) == tick_exch)
        .cloned()
}

lazy_static! {
    /// Global symbol_hash -> canonical symbol name map. Populated when a
    /// deployment is registered; read by the signal loop so trade_history
    /// rows show readable symbols (e.g. "BTC/USD") rather than `SYMBOL_<hash>`.
    pub static ref SYMBOL_NAMES: DashMap<u64, String> = DashMap::new();

    /// Deployment id -> most recent signal-emission time. Written by the
    /// market-data bridge (hot loop: in-memory only), flushed to
    /// `deployed_strategies.last_signal_at` by the market-health writer's 30s
    /// tick. This is the write path behind the dashboard's "Last signal"
    /// recency indicator, which was previously never populated.
    pub static ref LAST_SIGNAL_EMITTED: DashMap<uuid::Uuid, chrono::DateTime<chrono::Utc>> = DashMap::new();

    /// Deployment id -> most recent bar count reported by the strategy
    /// (`Strategy::bars_since_init`). Written by the market-data bridge (hot
    /// loop: in-memory only), flushed to `deployed_strategies.bars_accumulated`
    /// by the market-health writer's 30s tick -- the write path behind the
    /// dashboard's "still warming up" indicator.
    pub static ref LAST_BARS_ACCUMULATED: DashMap<uuid::Uuid, u32> = DashMap::new();
}

/// How many historical bars to warm-start with, comfortably under the
/// worker's own `WINDOW_SIZE=200` rolling-window cap set in
/// `worker.initialize()` (strategyhandler).
#[cfg(feature = "postgres")]
const WARM_START_BAR_COUNT: i64 = 100;

/// Fetch up to `WARM_START_BAR_COUNT` historical bars to pre-seed a
/// strategy's bar buffer on init, instead of starting from zero bars after
/// every restart (a strategy's bar history lives only in the Python
/// worker's in-memory state). Reuses BacktestingEngine's `dataloader` crate
/// (Massive/Polygon.io) -- the same provider forex backtests already use as
/// a proxy for Oanda (no historical Oanda candle API exists in this
/// codebase), so this isn't a new/different data source for Oanda-traded
/// deployments, just the same one reused live.
///
/// Returns `None` on ANY failure (missing `MASSIVE_API_KEY`, network error,
/// no data returned, or a `candle_interval_minutes` with no matching
/// granularity) -- a live deployment must never fail to start because a
/// nice-to-have historical fetch failed. Returns `Some(vec![])` is never
/// produced; an empty successful fetch is treated the same as a failure.
#[cfg(feature = "postgres")]
async fn fetch_warm_start_bars(
    exchange: &str,
    symbol: &str,
    asset_class: Option<&str>,
    candle_interval_minutes: i64,
) -> Option<Vec<(f64, f64, i64)>> {
    let provider = match MassiveDataProvider::from_env() {
        Ok(p) => p,
        Err(e) => {
            ultra_warn!(format!(
                "Warm-start skipped for {}/{}: MASSIVE_API_KEY unavailable ({})",
                exchange, symbol, e
            ));
            return None;
        }
    };

    let granularity = CandleGranularity::from_minutes(candle_interval_minutes);
    let to = chrono::Utc::now().date_naive();
    // Generous padding (weekends/holidays/thin forex sessions can leave gaps)
    // -- 3x the raw span comfortably covers WARM_START_BAR_COUNT real bars.
    let span_minutes = candle_interval_minutes.max(1) * WARM_START_BAR_COUNT * 3;
    let from = to - chrono::Duration::minutes(span_minutes);

    let mut request = DataRequest::new(Exchange::from_str(exchange), symbol, from, to)
        .with_granularity(granularity);
    if let Some(ac) = asset_class {
        request = request.with_asset_class(ac);
    }

    let bars = match provider.fetch(&request).await {
        Ok(bars) => bars,
        Err(e) => {
            ultra_warn!(format!(
                "Warm-start skipped for {}/{}: historical fetch failed ({})",
                exchange, symbol, e
            ));
            return None;
        }
    };

    let mut triples: Vec<(f64, f64, i64)> = bars
        .into_iter()
        .filter_map(|md| match md {
            MarketData::Candle(c) => {
                let ts_ns = c.timestamp.timestamp_nanos_opt()?;
                Some((c.close, c.volume, ts_ns))
            }
            _ => None,
        })
        .collect();

    if triples.is_empty() {
        ultra_warn!(format!(
            "Warm-start skipped for {}/{}: provider returned no candle data",
            exchange, symbol
        ));
        return None;
    }

    // Provider contract guarantees timestamp order, but keep only the most
    // recent WARM_START_BAR_COUNT in case padding pulled in more.
    if triples.len() > WARM_START_BAR_COUNT as usize {
        triples.drain(0..(triples.len() - WARM_START_BAR_COUNT as usize));
    }

    ultra_info!(format!(
        "\u{1f4c8} Warm-started {} historical bars for {}/{}, spanning {}..{}",
        triples.len(), symbol, exchange, from, to
    ));

    Some(triples)
}

#[cfg(not(feature = "postgres"))]
async fn fetch_warm_start_bars(
    _exchange: &str,
    _symbol: &str,
    _asset_class: Option<&str>,
    _candle_interval_minutes: i64,
) -> Option<Vec<(f64, f64, i64)>> {
    None
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

        // Capture the bar receiver so the bar bridge task (spawned below) can
        // consume completed OHLCV bars from the BarAggregator-backed channel.
        let bar_rx = datahandler.get_bar_receiver();
        
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
        let (paper_fill_tx, deploy_db_pool): (
            Option<tokio::sync::mpsc::Sender<paper_trade_writer::PaperFillEvent>>,
            Option<Arc<smartorderrouter::DbPool>>,
        ) = {
            // Kubernetes env values are not shell-expanded; resolve $(VAR) placeholders
            // from sibling POSTGRES_* env vars (same logic as strategyloader uses).
            fn resolve_db_url() -> Option<String> {
                let raw = std::env::var("DATABASE_URL").ok()?;
                let mut resolved = raw.clone();
                for key in ["POSTGRES_USER","POSTGRES_PASSWORD","POSTGRES_HOST","POSTGRES_PORT","POSTGRES_DB"] {
                    if let Ok(v) = std::env::var(key) {
                        resolved = resolved.replace(&format!("$({})", key), &v);
                    }
                }
                if resolved.contains("$(") { None } else { Some(resolved) }
            }
            match resolve_db_url() {
                Some(db_url) => {
                    match smartorderrouter::database::create_pool(&db_url).await {
                        Ok(pool) => {
                            let pool_arc = Arc::new(pool);
                            let writer = paper_trade_writer::PaperTradeWriter::new(pool_arc.clone());
                            market_health_writer::spawn(pool_arc.clone(), paper_registry.clone());
                            ultra_info!("✅ Paper trade writer initialized — fills will persist to trade_history");
                            ultra_info!("✅ Market-data health writer spawned (30s cadence, incl. deployment heartbeat)");
                            (Some(writer.sender()), Some(pool_arc))
                        }
                        Err(e) => {
                            ultra_warn!(format!("⚠️ Could not create DB pool for paper trade writer: {}", e));
                            (None, None)
                        }
                    }
                }
                None => {
                    ultra_info!("ℹ️ DATABASE_URL not set or unresolved — paper trades will not persist to DB");
                    (None, None)
                }
            }
        };
        #[cfg(not(feature = "postgres"))]
        let paper_fill_tx: Option<()> = None;
        #[cfg(not(feature = "postgres"))]
        let deploy_db_pool: Option<()> = None;

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
            let bridge_paper_registry = paper_registry.clone();
            let bridge_ultra_order_manager = self.ultra_order_manager.clone();
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                ultra_logger::ultra_info!(
                    "🔗 Market-data -> strategy bridge started (consuming DataHandler::market_data_rx)"
                );
                let mut tick_count: u64 = 0;
                let mut signals_emitted: u64 = 0;
                while let Ok(md) = market_data_rx.recv() {
                    tick_count += 1;

                    // Snapshot matching deployments (drop iterator before await to
                    // avoid holding the DashMap shard lock across awaits). Halted
                    // deployments (a prior dual-venue partial fill requiring human
                    // intervention -- see cross_venue_coordinator) stop generating
                    // signals entirely, not just stop having them executed.
                    let matches: Vec<(uuid::Uuid, u16, String, Vec<String>, Arc<tokio::sync::Mutex<Box<dyn Strategy>>>)> = bridge_registry
                        .iter()
                        .filter(|e| !cross_venue_coordinator::HALTED_DEPLOYMENTS.contains_key(e.key()))
                        .filter_map(|e| {
                            let v = e.value();
                            match_deployment_venue(&v.venues, &v.symbols, &md.symbol, &md.exchange)
                                .map(|venue| (*e.key(), v.strategy_id_hash, venue, v.venues.clone(), v.strategy.clone()))
                        })
                        .collect();

                    if matches.is_empty() {
                        continue;
                    }

                    // Build the strategyhandler::MarketData expected by Strategy::generate_signals,
                    // stamping the deployment's own matched venue string (resolved above),
                    // not the tick's raw exchange label.
                    for (deployment_id, sid_hash, exch, venues, strat) in matches {
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
                        let (signals, bars_since_init) = runtime.block_on(async move {
                            let mut s = strat.lock().await;
                            let result = s.generate_signals(&strat_md).await;
                            (result, s.bars_since_init())
                        });
                        if let Some(bars) = bars_since_init {
                            // In-memory only (hot loop); the market-health
                            // writer flushes this to bars_accumulated every 30s.
                            LAST_BARS_ACCUMULATED.insert(deployment_id, bars);
                        }
                        match signals {
                            Ok(mut sigs) => {
                                for sig in sigs.iter_mut() {
                                    // Stamp the deployment's strategy_id_hash so both the
                                    // downstream signal loop and the dual-venue coordinator
                                    // below can match it to paper_registry.
                                    sig.strategy_id = sid_hash;
                                }
                                if !sigs.is_empty() && signals_emitted < 10 {
                                    ultra_logger::ultra_info!(format!(
                                        "🔗 Bridge: strategy emitted {} signals (sid_hash={}, sym={}, bid={}, ask={})",
                                        sigs.len(), sid_hash, md.symbol, md.bid, md.ask
                                    ));
                                }

                                // A correlated 2-leg pair (dual-venue arbitrage/stat-arb
                                // decision) is submitted through the coordinator --
                                // sequenced leg1-then-leg2, halt-and-alert on a leg-2
                                // failure -- instead of the independent per-signal path
                                // below. Only possible for a multi-venue deployment;
                                // single-venue deployments never produce a batch shape
                                // is_correlated_pair recognizes as a pair.
                                let pair = if venues.len() == 2 {
                                    cross_venue_coordinator::is_correlated_pair(&sigs)
                                } else {
                                    None
                                };

                                if let Some((leg1, leg2)) = pair {
                                    let meta = bridge_paper_registry
                                        .get(&deployment_id)
                                        .map(|e| e.value().clone());
                                    match (meta, bridge_ultra_order_manager.clone()) {
                                        (Some(meta), Some(mgr)) => {
                                            let symbol = md.symbol.clone();
                                            let outcome = runtime.block_on(async move {
                                                cross_venue_coordinator::execute_dual_venue_pair(
                                                    deployment_id, &symbol, &meta, leg1, leg2, &mgr,
                                                ).await
                                            });
                                            ultra_logger::ultra_info!(format!(
                                                "🔗 Dual-venue pair for deployment {} (sid_hash={}): {:?}",
                                                deployment_id, sid_hash, outcome
                                            ));
                                            if outcome == cross_venue_coordinator::DualVenueOutcome::BothFilled {
                                                LAST_SIGNAL_EMITTED.insert(deployment_id, chrono::Utc::now());
                                            }
                                        }
                                        _ => {
                                            ultra_logger::ultra_warn!(format!(
                                                "Dual-venue pair detected for deployment {} but no paper_meta/order \
                                                 manager available -- dropping both legs without submitting either.",
                                                deployment_id
                                            ));
                                        }
                                    }
                                    continue;
                                }

                                let mut any_sent = false;
                                for sig in sigs {
                                    if signal_tx.try_send(sig).is_ok() {
                                        signals_emitted += 1;
                                        any_sent = true;
                                    }
                                }
                                if any_sent {
                                    // In-memory only (hot loop); the market-health
                                    // writer flushes this to last_signal_at every 30s.
                                    LAST_SIGNAL_EMITTED.insert(deployment_id, chrono::Utc::now());
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

        // ======================================================================
        // Bridge: bar_receiver -> deployed strategies
        //
        // Consumes completed OHLCV bars produced by DataEngine's BarAggregator
        // (one bar per symbol per second) and forwards them to any deployed
        // strategy that exposes `on_bar()`.  Until `strategyhandler::Strategy`
        // grows a `on_bar()` method, this task acts as a reliable drain so the
        // bar channel never fills and back-pressures the data pipeline.
        // ======================================================================
        {
            let bar_bridge_registry = deployed_strategies.clone();
            tokio::task::spawn_blocking(move || {
                ultra_logger::ultra_info!("📊 Bar bridge started — consuming BarAggregator output");
                let mut bar_count: u64 = 0;
                while let Ok(bar) = bar_rx.recv() {
                    bar_count += 1;
                    // Log periodically; strategies will consume via on_bar once
                    // strategyhandler::Strategy is extended.
                    if bar_count % 60 == 1 {
                        ultra_logger::ultra_info!(format!(
                            "📊 Bar ({}/{}): o={:.4} h={:.4} l={:.4} c={:.4} v={:.4} trades={} deployed={}",
                            bar.symbol, bar.exchange,
                            bar.open, bar.high, bar.low, bar.close, bar.volume,
                            bar.trade_count,
                            bar_bridge_registry.len(),
                        ));
                    }
                    // Future: iterate bar_bridge_registry and call strategy.on_bar(&bar)
                    let _ = bar_bridge_registry.len(); // keep registry arc alive
                }
                ultra_logger::ultra_info!("📊 Bar bridge stopped");
            });
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

                    if signal_count <= 10 {
                        ultra_logger::ultra_info!(format!(
                            "🎯 Signal #{} received: sid={}, action={:?}, qty={}, price={}, paper_meta={}",
                            signal_count, signal.strategy_id, signal.action, signal.quantity, signal.price,
                            paper_meta.is_some()
                        ));
                    }

                    // Resolve symbol: prefer the per-signal hash->name lookup (populated
                    // at deploy time for every one of the deployment's symbols) so each
                    // trade is labeled with the asset it was ACTUALLY for; fall back to
                    // the deployment's first symbol only when that lookup fails (e.g. an
                    // unrecognized/legacy hash). Trying paper_meta's first symbol FIRST
                    // used to mean every trade on every asset in a multi-asset portfolio
                    // got mislabeled with just the first configured symbol -- price/qty
                    // were correct (tied to the real signal), only the recorded symbol
                    // was wrong for every asset but the first.
                    let symbol = SYMBOL_NAMES.get(&signal.symbol_hash).map(|e| e.value().clone())
                        .or_else(|| paper_meta.as_ref().and_then(|m| m.symbols.first().cloned()))
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
                            if signal_count <= 10 {
                                ultra_logger::ultra_info!(format!(
                                    "🎯 Order #{} result: success={}, filled_qty={}, avg_price={}, order_id={}, err={:?}",
                                    signal_count, result.success, result.filled_quantity, result.avg_price, result.order_id, result.error
                                ));
                            }
                            // Record fill to trade_history for paper deployments.
                            // Defense-in-depth: only emit when the fill is real
                            // (success + nonzero qty + nonzero price). Previously
                            // a hardcoded $50K fallback in the mock exchange
                            // produced phantom fills that corrupted live P&L.
                            let real_fill = result.success
                                && result.filled_quantity > 0.0
                                && result.avg_price > 0.0;
                            if !real_fill {
                                ultra_logger::ultra_warn!(format!(
                                    "Skipping PaperFillEvent: symbol={} success={} filled_qty={} avg_price={} reason={:?}",
                                    symbol, result.success, result.filled_quantity, result.avg_price, result.error
                                ));
                            }
                            if real_fill {
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
        // Lazily-loaded per-tenant exchange credentials live in the DB; the pool
        // is captured here so the Deploy handler can refresh connectors without
        // a pod restart when users edit their API keys in the Settings UI.
        #[cfg(feature = "postgres")]
        let deploy_db_pool_for_handler = deploy_db_pool.clone();
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

                        // Build a strategy instance for this deployment.
                        // create_strategy() dispatches to PythonBridgeStrategy
                        // (runs the deployment's actual Python compute_signals())
                        // when python_source_code is available, falling back to
                        // SimpleMarketMakingStrategy only for genuine
                        // market-making deployments that have none. The config.id
                        // is set to the strategy_id_hash so the strategy stamps
                        // the right id on the Signal it produces (the bridge also
                        // overrides defensively).
                        let mut strat_params: HashMap<String, serde_json::Value> =
                            match &strategy.parameters {
                                serde_json::Value::Object(map) => map
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect(),
                                _ => HashMap::new(),
                            };
                        // Surface capital context to the strategy instance so order
                        // sizing scales with the deployment's actual allocated capital
                        // instead of a fixed unit quantity regardless of account size.
                        let has_capital_context = strategy.capital_allocation > 0.0;
                        if has_capital_context {
                            strat_params.insert(
                                "capital_allocation".to_string(),
                                serde_json::json!(strategy.capital_allocation),
                            );
                        }
                        if let Some(pct) = strategy.position_size_pct {
                            strat_params.insert(
                                "position_size_pct".to_string(),
                                serde_json::json!(pct),
                            );
                        }
                        // Always present (defaults to 1.0 = unleveraged) -- see
                        // `size_order_from_capital`'s leverage parameter.
                        strat_params.insert(
                            "leverage".to_string(),
                            serde_json::json!(strategy.leverage),
                        );
                        if let Some(source) = &strategy.python_source_code {
                            strat_params.insert(
                                "python_source_code".to_string(),
                                serde_json::json!(source),
                            );
                        }
                        if let Some(interval) = strategy.candle_interval_minutes {
                            strat_params.insert(
                                "candle_interval_minutes".to_string(),
                                serde_json::json!(interval),
                            );
                        }
                        // Warm-start every (symbol, exchange) leg's bar
                        // buffer from historical data instead of starting
                        // from zero bars, matching PythonBridgeStrategy's
                        // fully independent per-leg worker/bar state (see
                        // PythonBridgeStrategy::legs's doc for the cross-leg
                        // contamination bug this fixed). Deployments are
                        // either "N symbols on 1 exchange" (multi-asset
                        // portfolio) or "1 symbol on up to 2 exchanges"
                        // (dual-venue arbitrage) -- the full symbols x
                        // target_exchanges cross product covers both shapes
                        // without special-casing either, since one of the
                        // two lists always has length 1 in practice.
                        if let Some(interval) = strategy.candle_interval_minutes {
                            let mut warm_start_by_leg = serde_json::Map::new();
                            for symbol in &strategy.symbols {
                                for exchange in &strategy.target_exchanges {
                                    if let Some(bars) = fetch_warm_start_bars(
                                        exchange,
                                        symbol,
                                        strategy.asset_class.as_deref(),
                                        interval,
                                    ).await {
                                        warm_start_by_leg.insert(
                                            strategyhandler::warm_start_leg_key(symbol, exchange),
                                            serde_json::json!(bars),
                                        );
                                    }
                                }
                            }
                            if !warm_start_by_leg.is_empty() {
                                strat_params.insert(
                                    "warm_start_bars_by_leg".to_string(),
                                    serde_json::Value::Object(warm_start_by_leg),
                                );
                            }
                        }
                        let strat_config = StrategyConfig {
                            id: strategy_id_hash.to_string(),
                            name: strategy.strategy_name.clone(),
                            enabled: true,
                            symbols: strategy.symbols.clone(),
                            exchanges: strategy.target_exchanges.clone(),
                            parameters: strat_params,
                            max_position_size: if has_capital_context {
                                strategy.capital_allocation
                            } else {
                                10_000.0
                            },
                            risk_limit: strategy.position_size_pct.unwrap_or(0.02),
                        };
                        let strat_instance: Arc<tokio::sync::Mutex<Box<dyn Strategy>>> =
                            match strategyhandler::create_strategy(strat_config) {
                                Ok(s) => Arc::new(tokio::sync::Mutex::new(s)),
                                Err(e) => {
                                    ultra_logger::ultra_error!(format!(
                                        "❌ Rejected deployment {} ({}): failed to construct strategy: {}",
                                        strategy.strategy_name, strategy.instance_id, e
                                    ));
                                    continue;
                                }
                            };

                        // SimpleMarketMakingStrategy's initialize() is a no-op
                        // (just logs), but PythonBridgeStrategy's is where the
                        // pythonbridge-worker child process actually gets
                        // spawned -- every deployment must be initialized
                        // before its first generate_signals() call or a
                        // Python-backed deployment will error on every tick.
                        if let Err(e) = strat_instance.lock().await.initialize().await {
                            ultra_logger::ultra_error!(format!(
                                "❌ Rejected deployment {} ({}): strategy initialize() failed: {}",
                                strategy.strategy_name, strategy.instance_id, e
                            ));
                            continue;
                        }

                        if is_live {
                            // Live mode — use real exchange connectors (already configured via YAML)
                            // Cap at 2 venues per live deployment: real-money coordination
                            // complexity grows sharply past a two-legged trade, and no
                            // cross-venue execution coordinator exists yet for N > 2 (see
                            // DeployedStrategyEntry::venues doc).
                            let venues: Vec<String> = strategy.target_exchanges.iter().take(2).cloned().collect();
                            // NOTE: order EXECUTION dispatch (the Phase-2 signal loop below)
                            // is not yet venue-aware per-signal — it routes every signal for
                            // this deployment to a single connector key. Until that lands,
                            // multi-venue live deployments load credentials for every
                            // configured venue (so market data/signals are correctly
                            // venue-tagged), but orders still execute against venues[0]'s
                            // connector regardless of which venue a signal was meant for.
                            // This is a known, explicitly-flagged limitation, not a silent gap.
                            let exchange_name = venues[0].clone();

                            // Lazily (re)load the per-tenant credential for each configured
                            // venue from the DB and register/refresh its connector. This is
                            // idempotent — every Deploy event rebuilds the connector with the
                            // latest stored keys so users who rotate their API keys in the
                            // Settings UI take effect on the next deploy without a pod
                            // restart. If any configured venue lacks an enabled credential,
                            // reject the whole deployment (fail-closed).
                            #[cfg(feature = "postgres")]
                            {
                                if let (Some(ref ultra_mgr), Some(ref pool)) =
                                    (deployment_ultra_mgr.as_ref(), deploy_db_pool_for_handler.as_ref())
                                {
                                    let mut rejected = false;
                                    for venue in &venues {
                                        let mut handler = ultra_mgr.execution_handler().write().await;
                                        // live_only=true: a live deployment must
                                        // never bind to a testnet/sandbox key.
                                        match handler
                                            .ensure_exchange_for_tenant(pool, strategy.tenant_id, venue, true)
                                            .await
                                        {
                                            Ok(true) => {
                                                ultra_logger::ultra_info!(format!(
                                                    "🔑 Loaded {} credential for tenant {} (deployment {})",
                                                    venue, strategy.tenant_id, strategy.instance_id
                                                ));
                                            }
                                            Ok(false) => {
                                                ultra_logger::ultra_error!(format!(
                                                    "❌ Rejected live deployment {} ({}): no enabled \
                                                     non-testnet credential for exchange '{}' on tenant {}. \
                                                     Add production API keys via the Settings UI and re-publish.",
                                                    strategy.strategy_name,
                                                    strategy.instance_id,
                                                    venue,
                                                    strategy.tenant_id,
                                                ));
                                                rejected = true;
                                                break;
                                            }
                                            Err(e) => {
                                                ultra_logger::ultra_error!(format!(
                                                    "❌ Rejected live deployment {} ({}): credential load \
                                                     failed for exchange '{}': {:?}",
                                                    strategy.strategy_name,
                                                    strategy.instance_id,
                                                    venue,
                                                    e,
                                                ));
                                                rejected = true;
                                                break;
                                            }
                                        }
                                    }
                                    if rejected {
                                        continue;
                                    }
                                } else {
                                    ultra_logger::ultra_error!(format!(
                                        "❌ Rejected live deployment {} ({}): no DB pool or order \
                                         manager available — cannot verify exchange credential.",
                                        strategy.strategy_name, strategy.instance_id
                                    ));
                                    continue;
                                }
                            }

                            deploy_registry.insert(strategy.instance_id, PaperDeploymentMeta {
                                tenant_id: strategy.tenant_id,
                                deployment_id: strategy.instance_id,
                                strategy_id_hash,
                                paper_exchange: exchange_name.clone(),
                                // Live mode never needs a second stored connector name --
                                // each venue's own name IS its connector key (see
                                // `ExecutionHandler.connectors`, keyed by credential.exchange).
                                paper_exchange_2: None,
                                venues: venues.clone(),
                                symbols: strategy.symbols.clone(),
                                mode: "live".to_string(),
                                is_market_making: false,
                            });

                            deploy_strategies.insert(strategy.instance_id, DeployedStrategyEntry {
                                strategy_id_hash,
                                symbols: strategy.symbols.clone(),
                                venues: venues.clone(),
                                strategy: strat_instance.clone(),
                            });

                            ultra_logger::ultra_info!(format!(
                                "✅ Strategy {} ({}) deployed for LIVE trading on {}",
                                strategy.strategy_name, strategy.instance_id, exchange_name
                            ));
                        } else {
                            // Paper mode — create a paper trading connector.
                            // Capped at 2 venues, same rationale as the live-mode branch above.
                            let paper_exchange_name = format!("paper_{}", strategy.instance_id);
                            let venues: Vec<String> = strategy.target_exchanges.iter().take(2).cloned().collect();
                            // For a genuinely dual-venue deployment, provision a SECOND
                            // paper connector so each leg fills against its own venue's
                            // real order book (via the book-sync task above) instead of
                            // both legs sharing one synthetic book. Single-venue
                            // deployments keep today's one-connector behavior exactly.
                            let paper_exchange_name_2 = if venues.len() == 2 {
                                Some(format!("paper_{}_leg1", strategy.instance_id))
                            } else {
                                None
                            };
                            deploy_registry.insert(strategy.instance_id, PaperDeploymentMeta {
                                tenant_id: strategy.tenant_id,
                                deployment_id: strategy.instance_id,
                                strategy_id_hash,
                                paper_exchange: paper_exchange_name.clone(),
                                paper_exchange_2: paper_exchange_name_2.clone(),
                                venues: venues.clone(),
                                symbols: strategy.symbols.clone(),
                                mode: "paper".to_string(),
                                is_market_making: strategyloader::is_market_making_strategy_type(&strategy.strategy_type),
                            });

                            deploy_strategies.insert(strategy.instance_id, DeployedStrategyEntry {
                                strategy_id_hash,
                                symbols: strategy.symbols.clone(),
                                venues: venues.clone(),
                                strategy: strat_instance.clone(),
                            });

                            // Build PaperTradingConfig from deployment sim config fields,
                            // falling back to sensible defaults when not specified.
                            let sim_config = executionhandler::PaperTradingConfig {
                                slippage_bps: strategy.slippage_bps.unwrap_or(5.0),
                                ..Default::default()
                            };

                            // Register paper trading connector(s) for this deployment instance.
                            if let Some(ref ultra_mgr) = deployment_ultra_mgr {
                                let mut handler = ultra_mgr.execution_handler().write().await;
                                match handler.add_paper_exchange(paper_exchange_name.clone(), sim_config.clone()).await {
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
                                if let Some(ref leg1_name) = paper_exchange_name_2 {
                                    match handler.add_paper_exchange(leg1_name.clone(), sim_config).await {
                                        Ok(_) => {
                                            ultra_logger::ultra_info!(format!(
                                                "✅ Second-leg paper trading connector '{}' registered for strategy {}",
                                                leg1_name, strategy.strategy_name
                                            ));
                                        }
                                        Err(e) => {
                                            ultra_logger::ultra_warn!(format!(
                                                "⚠️ Failed to register second-leg paper connector for {}: {:?}",
                                                strategy.strategy_name, e
                                            ));
                                        }
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
            let mut last_heartbeat = std::time::Instant::now();
            let mut sync_count: u64 = 0;
            loop {
                tokio::time::sleep(tick).await;

                // Collect ALL paper deployments (MM and non-MM) without holding the
                // DashMap lock across awaits. Without this, non-MM paper strategies
                // fill against the MockExchange default book (~$50000 mid) and never
                // realize P&L because every fill is at the same flat price.
                let mm_deployments: Vec<(String, Vec<String>, Vec<String>)> = book_sync_registry
                    .iter()
                    .filter(|e| e.value().mode == "paper")
                    .map(|e| {
                        let m = e.value();
                        (m.paper_exchange.clone(), m.venues.clone(), m.symbols.clone())
                    })
                    .collect();

                // Helper: canonicalize symbols/exchanges so "BTC/USD" matches
                // "BTC-USD" and "kraken" matches "Kraken". DataEngine and the
                // strategy/deployment subscribe layer don't always agree on form.
                fn canon(s: &str) -> String {
                    s.replace('/', "-").to_uppercase()
                }

                for (paper_exchange, venues, symbols) in mm_deployments {
                    // NOTE: today's paper connector is a single synthetic book per
                    // deployment. For a dual-venue deployment this pushes both
                    // venues' books into the SAME connector (last write per tick
                    // wins) -- a reasonable best-effort approximation for now, but
                    // not a true per-leg simulation. Per-leg paper connectors are
                    // part of the still-pending cross-venue execution coordinator.
                    for real_exchange in &venues {
                        let canon_real_exchange = canon(real_exchange);
                        for symbol in &symbols {
                            let canon_symbol = canon(symbol);
                            // Find the orderbook by canonical match — DataHandler may
                            // have stored it under a slightly different symbol/exchange
                            // string than the deployment metadata uses.
                            let maybe_levels: Option<(Vec<(f64, f64)>, Vec<(f64, f64)>)> = {
                                let mut found = None;
                                for entry in datahandler::ORDERBOOKS.iter() {
                                    let (sym, exch) = entry.key();
                                    if canon(sym) == canon_symbol
                                        && canon(exch) == canon_real_exchange
                                    {
                                        found = entry.value().clone().read().ok()
                                            .and_then(|ob| ob.get_orderbook_levels(20).ok());
                                        break;
                                    }
                                }
                                found
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
                                        sync_count += 1;
                                    }
                                }
                            }
                        }
                    }
                }

                // Heartbeat every 30s: report sync activity and snapshot of which
                // (symbol, exchange) keys DataHandler has loaded. Without this it's
                // impossible to tell whether book-sync is actually pushing prices
                // into mock connectors or silently looking up the wrong key.
                if last_heartbeat.elapsed() >= std::time::Duration::from_secs(30) {
                    let mut keys: Vec<String> = datahandler::ORDERBOOKS
                        .iter()
                        .map(|e| {
                            let (s, x) = e.key();
                            format!("({},{})", s, x)
                        })
                        .collect();
                    keys.sort();
                    keys.truncate(10);
                    ultra_logger::ultra_info!(format!(
                        "📚 book-sync heartbeat: {} pushes since start, ORDERBOOKS keys=[{}]",
                        sync_count,
                        keys.join(", ")
                    ));
                    last_heartbeat = std::time::Instant::now();
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

    // --- match_deployment_venue (Gap 3a/3b: venues-list + exchange-aware matching) ---

    #[test]
    fn match_deployment_venue_matches_by_symbol_alone_for_single_venue_deployments() {
        let venues = vec!["kraken".to_string()];
        let symbols = vec!["BTC/USD".to_string()];
        // Tick claims to be from a totally different exchange -- single-venue
        // deployments don't care, preserving pre-venues-list behavior.
        let result = match_deployment_venue(&venues, &symbols, "BTC-USD", "massive");
        assert_eq!(result, Some("kraken".to_string()));
    }

    #[test]
    fn match_deployment_venue_rejects_a_non_matching_symbol() {
        let venues = vec!["kraken".to_string()];
        let symbols = vec!["BTC/USD".to_string()];
        assert_eq!(match_deployment_venue(&venues, &symbols, "ETH/USD", "kraken"), None);
    }

    #[test]
    fn match_deployment_venue_normalizes_symbol_separators_and_case() {
        let venues = vec!["kraken".to_string()];
        let symbols = vec!["btc-usd".to_string()];
        assert_eq!(
            match_deployment_venue(&venues, &symbols, "BTC/USD", "kraken"),
            Some("kraken".to_string())
        );
    }

    #[test]
    fn match_deployment_venue_picks_the_matching_leg_for_dual_venue_deployments() {
        let venues = vec!["kraken".to_string(), "coinbase".to_string()];
        let symbols = vec!["BTC/USD".to_string()];
        assert_eq!(
            match_deployment_venue(&venues, &symbols, "BTC/USD", "coinbase"),
            Some("coinbase".to_string())
        );
        assert_eq!(
            match_deployment_venue(&venues, &symbols, "BTC/USD", "kraken"),
            Some("kraken".to_string())
        );
    }

    #[test]
    fn match_deployment_venue_normalizes_exchange_case_for_dual_venue_deployments() {
        let venues = vec!["Kraken".to_string(), "Coinbase".to_string()];
        let symbols = vec!["BTC/USD".to_string()];
        assert_eq!(
            match_deployment_venue(&venues, &symbols, "BTC/USD", "COINBASE"),
            Some("Coinbase".to_string())
        );
    }

    #[test]
    fn match_deployment_venue_drops_a_tick_from_neither_configured_venue() {
        let venues = vec!["kraken".to_string(), "coinbase".to_string()];
        let symbols = vec!["BTC/USD".to_string()];
        // A dual-venue deployment must not silently attribute a tick from an
        // unconfigured exchange to one of its two legs -- that would corrupt
        // the arbitrage/stat-arb comparison the whole strategy depends on.
        assert_eq!(match_deployment_venue(&venues, &symbols, "BTC/USD", "binance"), None);
    }

    #[test]
    fn match_deployment_venue_handles_a_deployment_with_no_configured_venues() {
        let venues: Vec<String> = vec![];
        let symbols = vec!["BTC/USD".to_string()];
        assert_eq!(match_deployment_venue(&venues, &symbols, "BTC/USD", "kraken"), None);
    }
}