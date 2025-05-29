use config::Config;
use dotenv::dotenv;
use lazy_static::lazy_static;
use portfolio::{CryptoWallet, Position};
use prost::Message;
use protocol::broker::messages::MarketMessage;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, error::Error, sync::{Arc, RwLock}, time::{Instant, SystemTime, UNIX_EPOCH}};
use subscriber::{ConnectionConfig, Subscriber};

// Global storage for portfolios
lazy_static! {
    static ref PORTFOLIOS: RwLock<HashMap<String, Arc<CryptoWallet>>> = RwLock::new(HashMap::new());
    static ref LAST_UPDATE: RwLock<HashMap<String, Instant>> = RwLock::new(HashMap::new());
}

pub trait PortfolioHandlerTrait {
    fn listen(&mut self) -> Result<(), Box<dyn Error>>;
}

pub struct PortfolioHandler {
    subscriber: Subscriber
}

impl PortfolioHandler {
    pub fn new(topic_names: &[&str]) -> Result<Self, Box<dyn Error>> {
        dotenv().ok();
        let config_path = env::var("CONFIG_PATH").expect("CONFIG_PATH must be set");
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
        
        // Create the subscriber with only the filtered topics
        let subscriber = Subscriber::new(
            connection_config,
            &filtered_topics,
        ).expect("Failed to create subscriber");
        
        Ok(Self { 
            subscriber
        })
    }

    // Log portfolio stats for monitoring
    fn log_portfolio_stats(&self) -> Result<(), Box<dyn Error>> {
        let portfolios = PORTFOLIOS.read().unwrap();
        
        for (exchange, portfolio) in portfolios.iter() {
            if let Ok(metrics) = portfolio.get_metrics() {
                println!("Exchange: {}, Portfolio Value: {}, PnL: {}, Positions: {}",
                    exchange,
                    metrics.total_value,
                    metrics.value_by_exchange.get(exchange).unwrap_or(&0.0),
                    metrics.positions.len()
                );
            } else {
                eprintln!("Failed to get metrics for portfolio on exchange {}", exchange);
            }
        }
        
        Ok(())
    }
    
    // Get or create a portfolio for the given exchange
    fn get_or_create_portfolio(&self, exchange: &str) -> Arc<CryptoWallet> {
        let mut portfolios = PORTFOLIOS.write().unwrap();
        
        // If the portfolio doesn't exist, create a new one
        if !portfolios.contains_key(exchange) {
            let portfolio = Arc::new(CryptoWallet::new());
            portfolios.insert(exchange.to_string(), portfolio.clone());
            
            // Also initialize the last update timestamp
            let mut last_update = LAST_UPDATE.write().unwrap();
            last_update.insert(exchange.to_string(), Instant::now());
            
            return portfolio;
        }
        
        // Return the existing portfolio
        portfolios.get(exchange).unwrap().clone()
    }
    
    // Update the last update timestamp for an exchange
    fn update_timestamp(&self, exchange: &str) {
        let mut last_update = LAST_UPDATE.write().unwrap();
        last_update.insert(exchange.to_string(), Instant::now());
    }
}

impl PortfolioHandlerTrait for PortfolioHandler {
    fn listen(&mut self) -> Result<(), Box<dyn Error>> {
        // Start the subscriber
        self.subscriber.start().expect("Failed to start subscriber");
        
        // Log connection status and subscribed topics
        println!("Subscriber started, connected to message broker");
        
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
                        eprintln!("High latency detected: {}µs for topic index {}", 
                                  latency_ns / 1000, topic_idx);
                    }
                    
                    // Process message data - replace with your actual processing logic
                    self.process_message(topic_idx, message.get_data())
                        .unwrap_or_else(|e| eprintln!("Error processing message: {}", e));
                }
            } else {
                // No messages available, yield to the OS briefly to avoid CPU spin
                // Using a very short sleep to balance responsiveness and CPU usage
                spin_sleep::sleep(std::time::Duration::from_nanos(100));
            }
            
            // Periodically check health and log statistics
            if last_stats_time.elapsed() >= stats_interval {
                if let Ok(health) = self.subscriber.get_health_metrics() {
                    println!(
                        "Broker connection: {}, Messages processed: {}, Errors: {}", 
                        if health.get_connected() { "Connected" } else { "Disconnected" },
                        message_count,
                        health.get_error_count()
                    );
                    
                    // If connection is stale (no messages for a while), reconnect
                    if self.subscriber.is_stale(5000) {
                        eprintln!("Connection appears stale, attempting reconnect");
                        if let Err(e) = self.subscriber.reconnect() {
                            eprintln!("Reconnection failed: {:?}", e);
                        }
                    }
                }

                // Add this to log portfolio statistics
                if let Err(e) = self.log_portfolio_stats() {
                    eprintln!("Error logging portfolio stats: {}", e);
                }
                
                // Reset metrics for next interval
                message_count = 0;
                last_stats_time = std::time::Instant::now();
            }
        }
    }
}

// Add this implementation to your PortfolioHandler impl block

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
        
        // Decode the MarketMessage
        if let Ok(market_msg) = MarketMessage::decode(data) {            
            // Now process based on whether it contains trades, orders, or wallets
            match market_msg.payload {
                Some(protocol::broker::messages::market_message::Payload::WalletsPayload(wallets)) => {
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
                                println!(
                                    "Updated portfolio for {} with {} symbols",
                                    wallets.exchange,
                                    total_symbols
                                );
                                
                                // Optionally log individual balances with non-zero values
                                for wallet in &wallets.wallets {
                                    if wallet.balance > 0.0 {
                                        println!(
                                            "  {}: {:.8}",
                                            wallet.symbol,
                                            wallet.balance,
                                        );
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!(
                                "Failed to update portfolio for {}: {}",
                                wallets.exchange,
                                e
                            );
                        }
                    }
                },
                Some(protocol::broker::messages::market_message::Payload::TradesPayload(trades)) => {
                    // Optionally update market prices based on trades
                    // This would help with portfolio valuation
                    for trade in &trades.trades {
                        // Find which portfolio this trade belongs to
                        let portfolio = self.get_or_create_portfolio(&trade.exchange);
                        
                        // Update the market price for this symbol
                        if let Err(e) = portfolio.set_market_price(&trade.symbol, trade.price as f64) {
                            eprintln!(
                                "Failed to update market price for {}: {}",
                                trade.symbol,
                                e
                            );
                        }
                    }
                    
                    if cfg!(debug_assertions) {
                        println!(
                            "Processed {} trades from topic {}",
                            trades.trades.len(),
                            topic_name
                        );
                    }
                },
                Some(protocol::broker::messages::market_message::Payload::OrdersPaylaod(orders)) => {
                    // Orders don't directly affect portfolio balances
                    // but we can log them for monitoring
                    if cfg!(debug_assertions) {
                        println!(
                            "Received {} order updates from topic {} (not affecting portfolio)",
                            orders.orders.len(),
                            topic_name
                        );
                    }
                },
                None => {
                    eprintln!("Empty market message payload for topic {}", topic_name);
                    return Err("Empty payload".into());
                }
            }
        } else {
            eprintln!("Failed to decode MarketMessage for topic {}", topic_name);
            return Err("Failed to decode message".into());
        }
        
        Ok(())
    }
    
    // Additional helper method to update portfolio prices for valuation
    #[inline]
    fn update_portfolio_prices(&self, prices: HashMap<String, f64>) -> Result<(), Box<dyn Error>> {
        let portfolios = PORTFOLIOS.read().unwrap();
        
        for (exchange, portfolio) in portfolios.iter() {
            if let Err(e) = portfolio.update_metrics(&prices) {
                eprintln!("Failed to update metrics for {}: {}", exchange, e);
            }
        }
        
        Ok(())
    }
    
    // Helper method to get aggregated portfolio metrics across all exchanges
    pub fn get_aggregated_metrics(&self) -> Result<AggregatedMetrics, Box<dyn Error>> {
        let portfolios = PORTFOLIOS.read().unwrap();
        let mut total_value = 0.0;
        let mut value_by_exchange = HashMap::new();
        let mut all_positions = Vec::new();
        
        for (exchange, portfolio) in portfolios.iter() {
            if let Ok(metrics) = portfolio.get_metrics() {
                total_value += metrics.total_value;
                value_by_exchange.insert(exchange.clone(), metrics.total_value);
                all_positions.extend(metrics.positions);
            }
        }
        
        // Sort all positions by value
        all_positions.sort_by(|a, b| b.market_value.partial_cmp(&a.market_value).unwrap());
        
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
}

// Additional struct for aggregated metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedMetrics {
    pub total_portfolio_value: f64,
    pub value_by_exchange: HashMap<String, f64>,
    pub top_positions: Vec<Position>,
    pub last_updated: u64,
}