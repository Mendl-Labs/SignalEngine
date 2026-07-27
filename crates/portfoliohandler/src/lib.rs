use config::Config;
use dashmap::DashMap;
use dotenv::dotenv;
use lazy_static::lazy_static;
use portfolio::{CryptoWallet, Position};
use protocol::broker::messages::{portfolio_message, PortfolioMessage};
use serde::{Deserialize, Serialize};
use signalengine::{SignalEngineLogger, TradingContext};
use std::{collections::HashMap, env, error::Error, sync::{Arc, RwLock}, time::{Instant, SystemTime, UNIX_EPOCH}};
use subscriber::{ConnectionConfig, Subscriber};

// Global storage for portfolios - properly structured as a HashMap
lazy_static! {
    pub static ref PORTFOLIOS: DashMap<String, Arc<CryptoWallet>> = DashMap::new();
    static ref LAST_UPDATE: RwLock<HashMap<String, Instant>> = RwLock::new(HashMap::new());
}

pub trait PortfolioHandlerTrait {
    fn listen(&mut self) -> Result<(), Box<dyn Error>>;
}

#[derive(Clone)]
pub struct PortfolioHandler {
    subscriber: Subscriber,
    logger: Arc<SignalEngineLogger>,
}

impl PortfolioHandler {
    pub async fn new(topic_names: &[&str]) -> Result<Self, Box<dyn Error>> {
        dotenv().ok();
        let config_path = env::var("CONFIG_PATH")
            .map_err(|_| "CONFIG_PATH environment variable must be set. Set it to the path of your config file.")?;
        let config = Config::new(&config_path)?;
        let addr = format!("{}:{}", config.message_broker.address, config.message_broker.port);
        let connection_config = ConnectionConfig::new(&addr);
        
        // Filter to only use the requested topics if they exist in the config
        let filtered_topics: Vec<&str> = config.subscribe_topics.iter()
            .filter(|topic| topic_names.contains(&topic.as_str()))
            .map(String::as_str)
            .collect();
        
        // Ensure we have at least one topic to subscribe to
        if filtered_topics.is_empty() {
            return Err(format!("None of the requested topics {:?} found in configuration", topic_names).into());
        }
        
        let logger = Arc::new(SignalEngineLogger::new("PortfolioHandler").await);
        logger.info(&format!("Initializing PortfolioHandler with topics: {:?}", filtered_topics)).await;
        
        // Create the subscriber with only the filtered topics
        let subscriber = Subscriber::new(
            connection_config,
            &filtered_topics,
        ).expect("Failed to create subscriber");
        
        Ok(Self { 
            subscriber,
            logger,
        })
    }

    // Log portfolio stats for monitoring (async version)
    async fn _log_portfolio_stats(&self) -> Result<(), Box<dyn Error>> {        
        for entry in PORTFOLIOS.iter() {
            let exchange = entry.key();
            let portfolio = entry.value();
            if let Ok(metrics) = portfolio.get_metrics() {
                self.logger.log_portfolio_update(
                    exchange,
                    metrics.total_value,
                    0.0 // PnL calculation would go here
                ).await;
                
                let context = TradingContext::new("PortfolioHandler")
                    .with_operation("portfolio_stats")
                    .with_exchange(exchange);
                
                self.logger.info_ctx(
                    &format!("Portfolio - Value: {:.2}, Positions: {}", 
                        metrics.total_value, metrics.positions.len()), 
                    context.clone()
                ).await;
                
                // Log top positions
                for (i, position) in metrics.positions.iter().take(5).enumerate() {
                    self.logger.debug_ctx(
                        &format!("Position {}: {} - {:.8} @ ${:.2} = ${:.2}",
                            i + 1,
                            position.symbol,
                            position.quantity,
                            position.market_price,
                            position.market_value
                        ),
                        context.clone().with_symbol(&position.symbol)
                    ).await;
                }
            } else {
                self.logger.error(&format!("Failed to get metrics for portfolio on exchange {}", exchange)).await;
            }
        }
        
        Ok(())
    }
    
    // Synchronous version for use in non-async contexts
    fn log_portfolio_stats_sync(logger: &Arc<SignalEngineLogger>) -> Result<(), Box<dyn Error>> {
        for entry in PORTFOLIOS.iter() {
            let exchange = entry.key().clone();
            let portfolio = entry.value();
            if let Ok(metrics) = portfolio.get_metrics() {
                // Spawn async logging tasks without blocking
                let logger_clone = logger.clone();
                let exchange_clone = exchange.clone();
                tokio::spawn(async move {
                    logger_clone.log_portfolio_update(
                        &exchange_clone,
                        metrics.total_value,
                        0.0
                    ).await;
                    
                    let context = TradingContext::new("PortfolioHandler")
                        .with_operation("portfolio_stats")
                        .with_exchange(&exchange_clone);
                    
                    logger_clone.info_ctx(
                        &format!("Portfolio - Value: {:.2}, Positions: {}", 
                            metrics.total_value, metrics.positions.len()), 
                        context
                    ).await;
                });
            }
        }
        Ok(())
    }
    
    // Get or create a portfolio for the given exchange
    fn get_or_create_portfolio(&self, exchange: &str) -> Arc<CryptoWallet> {        
        // Check again in case another thread created it
        if let Some(portfolio) = PORTFOLIOS.get(exchange) {
            return Arc::clone(&*portfolio);
        }
        
        // Create new portfolio
        let portfolio = Arc::new(CryptoWallet::new());
        PORTFOLIOS.insert(exchange.to_string(), Arc::clone(&portfolio));
        
        // Also initialize the last update timestamp
        if let Ok(mut last_update) = LAST_UPDATE.write() {
            last_update.insert(exchange.to_string(), Instant::now());
        }
        // If lock fails, we continue - timestamp update is not critical for portfolio creation
        
        portfolio
    }
    
    // Update the last update timestamp for an exchange
    fn update_timestamp(&self, exchange: &str) {
        if let Ok(mut last_update) = LAST_UPDATE.write() {
            last_update.insert(exchange.to_string(), Instant::now());
        }
        // If lock fails, we continue - timestamp update is not critical for operation
    }
}

impl PortfolioHandlerTrait for PortfolioHandler {
    fn listen(&mut self) -> Result<(), Box<dyn Error>> {
        // Start the subscriber
        self.subscriber.start().expect("Failed to start subscriber");
        
        // Log connection status and subscribed topics
        let logger = self.logger.clone();
        tokio::spawn(async move {
            logger.info("Subscriber started, connected to message broker").await;
        });
        
        // Track performance metrics
        let mut last_stats_time = std::time::Instant::now();
        let stats_interval = std::time::Duration::from_secs(60); // Log stats every minute
        let mut message_count = 0;
        
        // Main processing loop - designed for low latency
        loop {
            // Use poll_all_messages to efficiently batch-process available messages
            // Balance between latency and throughput with a moderate batch size
            let messages = self.subscriber.poll_all_messages(100);
            
            if !messages.is_empty() {
                message_count += messages.len();
                
                // Process each message with minimal overhead
                for (topic_idx, message) in messages {
                    // Get message timestamp for latency tracking
                    let recv_time = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64;
                    
                    let msg_time = message.get_timestamp();
                    let latency_ns = if msg_time > 0 { recv_time - msg_time } else { 0 };
                    
                    // Log excessive latency (adjust threshold as needed)
                    if latency_ns > 1_000_000 { // 1ms
                        let logger = self.logger.clone();
                        let topic_idx_copy = topic_idx;
                        let latency_us = latency_ns / 1000;
                        tokio::spawn(async move {
                            logger.warn(&format!("High latency detected: {}µs for topic index {}", 
                                      latency_us, topic_idx_copy)).await;
                        });
                    }
                    
                    // Process message data
                    self.process_message(topic_idx, message.get_data())
                        .unwrap_or_else(|e| {
                            let logger = self.logger.clone();
                            let error_msg = e.to_string();
                            tokio::spawn(async move {
                                logger.error(&format!("Error processing message: {}", error_msg)).await;
                            });
                        });
                }
            } else {
                // No messages available, yield to the OS briefly to avoid CPU spin
                // Using a very short sleep to balance responsiveness and CPU usage
                spin_sleep::sleep(std::time::Duration::from_nanos(100));
            }
            
            // Periodically check health and log statistics
            if last_stats_time.elapsed() >= stats_interval {
                if let Ok(health) = self.subscriber.get_health_metrics() {
                    let logger = self.logger.clone();
                    let connected = health.get_connected();
                    let error_count = health.get_error_count();
                    let msg_count = message_count;
                    tokio::spawn(async move {
                        logger.info(&format!(
                            "Broker connection: {}, Messages processed: {}, Errors: {}", 
                            if connected { "Connected" } else { "Disconnected" },
                            msg_count,
                            error_count
                        )).await;
                    });
                    
                    // If connection is stale (no messages for a while), reconnect.
                    // Portfolio-topic traffic is inherently bursty (only fires on
                    // trades/fills), not continuous like market data ticks, so a
                    // short threshold checked every stats_interval (60s) treated
                    // a perfectly healthy but quiet connection as "stale" on
                    // essentially every cycle -- observed in prod as a permanent
                    // reconnect loop. Use a threshold well above one idle trading
                    // lull instead of one just above the check cadence.
                    const PORTFOLIO_STALE_THRESHOLD_MS: u64 = 300_000; // 5 minutes
                    if self.subscriber.is_stale(PORTFOLIO_STALE_THRESHOLD_MS) {
                        let logger = self.logger.clone();
                        tokio::spawn(async move {
                            logger.warn("Connection appears stale, attempting reconnect").await;
                        });
                        
                        if let Err(e) = self.subscriber.reconnect() {
                            let logger = self.logger.clone();
                            let error_msg = format!("{:?}", e);
                            tokio::spawn(async move {
                                logger.error(&format!("Reconnection failed: {}", error_msg)).await;
                            });
                        }
                    }
                }

                // Log portfolio statistics inline (can't spawn due to self borrow)
                if let Err(e) = Self::log_portfolio_stats_sync(&self.logger) {
                    let logger = self.logger.clone();
                    let error_msg = e.to_string();
                    tokio::spawn(async move {
                        logger.error(&format!("Error logging portfolio stats: {}", error_msg)).await;
                    });
                }
                
                // Reset metrics for next interval
                message_count = 0;
                last_stats_time = std::time::Instant::now();
            }
        }
    }
}

impl PortfolioHandler {
    #[inline]
    fn process_message(&self, topic_idx: usize, data: &[u8]) -> Result<(), Box<dyn Error>> {
        // Get the topic name
        let topic_name = self.subscriber.get_topic_name(topic_idx).unwrap_or_default();
        
        // Get current timestamp
        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        // Decode the PortfolioMessage using JSON
        if let Ok(portfolio_msg) = serde_json::from_slice::<PortfolioMessage>(data) {            
            // Process based on payload type
            match portfolio_msg.payload {
                Some(portfolio_message::Payload::WalletsPayload(wallets)) => {
                    // Get or create portfolio for this exchange
                    let portfolio = self.get_or_create_portfolio(&wallets.exchange);
                    
                    // Process wallet balance updates
                    match portfolio.process_wallets_message(
                        &wallets.exchange,
                        &wallets.wallets,
                        current_timestamp
                    ) {
                        Ok(_) => {
                            // Update the timestamp for this exchange
                            self.update_timestamp(&wallets.exchange);
                            
                            if cfg!(debug_assertions) {
                                // Log some stats about the update
                                let total_symbols = wallets.wallets.len();
                                let logger = self.logger.clone();
                                let exchange = wallets.exchange.clone();
                                let wallets_data = wallets.wallets.clone();
                                
                                tokio::spawn(async move {
                                    let context = TradingContext::new("PortfolioHandler")
                                        .with_operation("wallet_update")
                                        .with_exchange(&exchange);
                                    
                                    logger.debug_ctx(
                                        &format!("Updated portfolio for {} with {} symbols", exchange, total_symbols),
                                        context.clone()
                                    ).await;
                                    
                                    // Optionally log individual balances with non-zero values
                                    for wallet in &wallets_data {
                                        if wallet.balance > 0.0 {
                                            logger.debug_ctx(
                                                &format!("{}: {:.8}", wallet.symbol, wallet.balance),
                                                context.clone().with_symbol(&wallet.symbol)
                                            ).await;
                                        }
                                    }
                                });
                            }
                        },
                        Err(e) => {
                            let logger = self.logger.clone();
                            let exchange = wallets.exchange.clone();
                            let error_msg = e.to_string();
                            tokio::spawn(async move {
                                logger.error(&format!("Failed to update portfolio for {}: {}", exchange, error_msg)).await;
                            });
                        }
                    }
                },
                Some(portfolio_message::Payload::Position(wallet)) => {
                    // Handle single wallet position update
                    let logger = self.logger.clone();
                    let wallet_copy = wallet.clone();
                    tokio::spawn(async move {
                        logger.debug(&format!("Received position update for wallet: {:?}", wallet_copy)).await;
                    });
                },
                Some(portfolio_message::Payload::Balance(wallet)) => {
                    // Handle single wallet balance update
                    let logger = self.logger.clone();
                    let wallet_copy = wallet.clone();
                    tokio::spawn(async move {
                        logger.debug(&format!("Received balance update for wallet: {:?}", wallet_copy)).await;
                    });
                },
                Some(portfolio_message::Payload::Update(execution_report)) => {
                    // Handle execution report update
                    let logger = self.logger.clone();
                    let report_copy = execution_report.clone();
                    tokio::spawn(async move {
                        logger.info(&format!("Received execution update: {:?}", report_copy)).await;
                    });
                },
                Some(portfolio_message::Payload::Risk(risk_alert)) => {
                    // Handle risk alert
                    let logger = self.logger.clone();
                    let alert_copy = risk_alert.clone();
                    tokio::spawn(async move {
                        logger.warn(&format!("Received risk alert: {:?}", alert_copy)).await;
                    });
                },
                None => {
                    let logger = self.logger.clone();
                    let topic = topic_name.to_string();
                    tokio::spawn(async move {
                        logger.error(&format!("Empty portfolio message payload for topic {}", topic)).await;
                    });
                    return Err("Empty payload".into());
                }
            }
        } else {
            let logger = self.logger.clone();
            let topic = topic_name.to_string();
            tokio::spawn(async move {
                logger.error(&format!("Failed to decode PortfolioMessage for topic {}", topic)).await;
            });
            return Err("Failed to decode message".into());
        }
        
        Ok(())
    }
    
    // Update portfolio prices for valuation
    pub async fn update_portfolio_prices(&self, prices: HashMap<String, f64>) -> Result<(), Box<dyn Error>> {        
        for entry in PORTFOLIOS.iter() {
            let exchange = entry.key();
            let portfolio = entry.value();
            if let Err(e) = portfolio.update_metrics(&prices) {
                let logger = self.logger.clone();
                let exchange_copy = exchange.clone();
                let error_msg = e.to_string();
                tokio::spawn(async move {
                    logger.error(&format!("Failed to update metrics for {}: {}", exchange_copy, error_msg)).await;
                });
            }
        }
        
        Ok(())
    }
    
    // Get portfolio for a specific exchange
    pub fn get_portfolio(&self, exchange: &str) -> Option<Arc<CryptoWallet>> {
        PORTFOLIOS.get(exchange).map(|ref_val| Arc::clone(&*ref_val))
    }
    
    // Get all portfolios
    pub fn get_all_portfolios(&self) -> HashMap<String, Arc<CryptoWallet>> {
        PORTFOLIOS.iter().map(|entry| (entry.key().clone(), Arc::clone(entry.value()))).collect()
    }
    
    // Helper method to get aggregated portfolio metrics across all exchanges
    pub fn get_aggregated_metrics(&self) -> Result<AggregatedMetrics, Box<dyn Error>> {
        let mut total_value = 0.0;
        let mut value_by_exchange = HashMap::new();
        let mut all_positions = Vec::new();
        
        for entry in PORTFOLIOS.iter() {
            let exchange = entry.key();
            let portfolio = entry.value();
            if let Ok(metrics) = portfolio.get_metrics() {
                total_value += metrics.total_value;
                value_by_exchange.insert(exchange.clone(), metrics.total_value);
                all_positions.extend(metrics.positions);
            }
        }
        
        // Sort all positions by value - handle NaN values gracefully
        all_positions.sort_by(|a, b| {
            b.market_value.partial_cmp(&a.market_value)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        
        Ok(AggregatedMetrics {
            total_portfolio_value: total_value,
            value_by_exchange,
            top_positions: all_positions.into_iter().take(20).collect(),
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        })
    }
    
    // Check if we have recent updates from an exchange
    pub fn is_exchange_stale(&self, exchange: &str, max_age_ms: u64) -> bool {
        let last_update = match LAST_UPDATE.read() {
            Ok(guard) => guard,
            Err(_) => return true, // If lock fails, assume stale to be safe
        };
        
        if let Some(last_time) = last_update.get(exchange) {
            last_time.elapsed().as_millis() as u64 > max_age_ms
        } else {
            true // No update recorded, consider it stale
        }
    }
}

// Additional struct for aggregated metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedMetrics {
    pub total_portfolio_value: f64,
    pub value_by_exchange: HashMap<String, f64>,
    pub top_positions: Vec<Position>,
    pub last_updated: u64,
}