use config::Config;
use dotenv::dotenv;
use lazy_static::lazy_static;
use orderbook::Orderbook;
use prost::Message;
use protocol::broker::messages::{market_message, MarketMessage};
use std::{collections::HashMap, env, error::Error, hash::{DefaultHasher, Hash, Hasher}, sync::{Arc, RwLock}, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use subscriber::{ConnectionConfig, Subscriber};

// Global storage for orderbooks
lazy_static! {
    static ref ORDERBOOKS: RwLock<HashMap<(String, String), Arc<Orderbook>>> = RwLock::new(HashMap::new());
    static ref LAST_UPDATE: RwLock<HashMap<(String, String), Instant>> = RwLock::new(HashMap::new());
}

pub trait DataHandlerTrait {
    fn listen(&mut self) -> Result<(), Box<dyn Error>>;
}
pub struct DataHandler {
    subscriber: Subscriber
}

impl DataHandler {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        dotenv().ok();
        let config_path = env::var("CONFIG_PATH").expect("CONFIG_PATH must be set");
        let config = Config::new(&config_path)?;
        let addr = format!("{}:{}", config.message_broker.address, config.message_broker.port);
        let connection_config = ConnectionConfig::new(&addr);
        let subscriber = Subscriber::new(
            connection_config,
            &config.subscribe_topics.iter().map(String::as_str).collect::<Vec<_>>(),
        ).expect("Failed to create subscriber");
        Ok(Self { subscriber })
    }

    // Helper method to get or create an orderbook for a symbol/exchange pair
    fn get_or_create_orderbook(&self, symbol: &str, exchange: &str) -> Result<Arc<Orderbook>, Box<dyn Error>> {
        let key = (symbol.to_string(), exchange.to_string());
        
        // Try with a read lock first
        {
            let orderbooks = ORDERBOOKS.read().unwrap();
            if let Some(orderbook) = orderbooks.get(&key) {
                return Ok(Arc::clone(orderbook));
            }
        }
        
        // Not found, acquire write lock and create
        let mut orderbooks = ORDERBOOKS.write().unwrap();
        
        // Check again in case another thread created it while we were waiting
        if let Some(orderbook) = orderbooks.get(&key) {
            return Ok(Arc::clone(orderbook));
        }
        
        // Create a new orderbook with reasonable capacity for an HFT system
        const DEFAULT_CAPACITY: usize = 100000;
        let orderbook = Arc::new(Orderbook::new(
            symbol.to_string(),
            exchange.to_string(),
            DEFAULT_CAPACITY
        ));
        
        // Store in the map
        orderbooks.insert(key, Arc::clone(&orderbook));
        
        Ok(orderbook)
    }

    // Update metrics for all orderbooks, but throttle to avoid excessive CPU usage
    fn update_all_orderbooks(&self) -> Result<(), Box<dyn Error>> {
        // Get current time
        let now = Instant::now();
        
        // Minimum time between updates to avoid excessive CPU usage
        const MIN_UPDATE_INTERVAL: Duration = Duration::from_millis(100);
        
        // Get a list of all orderbooks
        let orderbooks: Vec<(String, String, Arc<Orderbook>)> = {
            let orderbooks_map = ORDERBOOKS.read().unwrap();
            orderbooks_map.iter()
                .map(|((symbol, exchange), orderbook)| 
                    (symbol.clone(), exchange.clone(), Arc::clone(orderbook)))
                .collect()
        };
        
        for (symbol, exchange, orderbook) in orderbooks {
            let key = (symbol.clone(), exchange.clone());
            let should_update = {
                let last_updates = LAST_UPDATE.read().unwrap();
                if let Some(last_update) = last_updates.get(&key) {
                    now.duration_since(*last_update) >= MIN_UPDATE_INTERVAL
                } else {
                    true
                }
            };
            
            if should_update {
                // Update the orderbook and record the time
                if let Err(e) = orderbook.update() {
                    eprintln!("Error updating orderbook for {}/{}: {}", symbol, exchange, e);
                } else {
                    let mut last_updates = LAST_UPDATE.write().unwrap();
                    last_updates.insert(key, now);
                }
            }
        }
        
        Ok(())
    }
    
    // Log orderbook stats for monitoring
    fn log_orderbook_stats(&self) -> Result<(), Box<dyn Error>> {
        let orderbooks = ORDERBOOKS.read().unwrap();
        
        for ((symbol, exchange), orderbook) in orderbooks.iter() {
            if let Ok(metrics) = orderbook.metrics() {
                println!(
                    "{}/{} - Bid: {:.2} @ {:.2}, Ask: {:.2} @ {:.2}, Spread: {:.2} bps, Depth: {:.2}/{:.2}",
                    symbol, exchange,
                    metrics.best_bid, metrics.best_bid_depth,
                    metrics.best_ask, metrics.best_ask_depth,
                    metrics.spread_bps,
                    metrics.total_bid_depth, metrics.total_ask_depth
                );
            }
        }
        
        Ok(())
    }
}

impl DataHandlerTrait for DataHandler {
    fn listen(&mut self) -> Result<(), Box<dyn Error>> {
        // Start the subscriber
        self.subscriber.start().expect("Failed to start subscriber");
        
        // Log connection status and subscribed topics
        println!("Subscriber started, connected to message broker");
        println!("Subscribed to the following topics:");
        
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

                // Add this to log orderbook statistics
                if let Err(e) = self.log_orderbook_stats() {
                    eprintln!("Error logging orderbook stats: {}", e);
                }
                
                // Reset metrics for next interval
                message_count = 0;
                last_stats_time = std::time::Instant::now();
            }
        }
    }
}

// Add a method to process individual messages
impl DataHandler {
    #[inline]
    fn process_message(&self, topic_idx: usize, data: &[u8]) -> Result<(), Box<dyn Error>> {
        // Get the topic name
        let topic_name = self.subscriber.get_topic_name(topic_idx).unwrap_or_default();
        
        // Get current timestamp for order timestamps
        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        // Decode the MarketMessage
        if let Ok(market_msg) = MarketMessage::decode(data) {
            // Now process based on whether it contains trades or orders
            match market_msg.payload {
                Some(market_message::Payload::TradesPayload(trades)) => {
                    // Process each trade
                    for trade in trades.trades {
                        // Get or create orderbook for this symbol/exchange
                        let orderbook = self.get_or_create_orderbook(&trade.symbol, &trade.exchange)?;
                        
                        // Execute market order based on trade side
                        match trade.side.as_str() {
                            "buy" => {
                                // For buy trades, execute market buy order
                                let result = orderbook.match_market_bid(trade.price as f64, trade.qty as f64)?;
                                
                                // Log execution details for monitoring
                                if result.executed_quantity > 0.0 {
                                    println!(
                                        "Executed market buy: {} @ {} (avg price: {:.2}) for {}/{}",
                                        result.executed_quantity,
                                        trade.price,
                                        result.average_price,
                                        trade.symbol,
                                        trade.exchange
                                    );
                                }
                            },
                            "sell" => {
                                // For sell trades, execute market sell order
                                let result = orderbook.match_market_ask(trade.price as f64, trade.qty as f64)?;
                                
                                // Log execution details for monitoring
                                if result.executed_quantity > 0.0 {
                                    println!(
                                        "Executed market sell: {} @ {} (avg price: {:.2}) for {}/{}",
                                        result.executed_quantity,
                                        trade.price,
                                        result.average_price,
                                        trade.symbol,
                                        trade.exchange
                                    );
                                }
                            },
                            _ => {
                                eprintln!("Unknown trade side: {}", trade.side);
                            }
                        }
                    }
                },
                Some(market_message::Payload::OrdersPaylaod(orders)) => {
                    // Process order updates
                    for order in orders.orders {
                        // Get or create orderbook for this symbol/exchange
                        let orderbook = self.get_or_create_orderbook(&order.symbol, &order.exchange)?;
                        
                        // Convert order id to u64 for orderbook
                        let order_id = u64::from_str_radix(&order.unique_id, 16)
                            .unwrap_or_else(|_| {
                                // Hash string to u64 if it's not a valid hex
                                let mut hasher = DefaultHasher::new();
                                order.unique_id.hash(&mut hasher);
                                hasher.finish()
                            });
                        
                        // Process based on event type
                        match order.event.as_str() {
                            "new" => {
                                // New order
                                match order.side.as_str() {
                                    "buy" => {
                                        if let Ok(_idx) = orderbook.add_limit_bid(
                                            order.price_level as f64,
                                            order_id,
                                            order.quantity as f64,
                                            current_timestamp
                                        ) {
                                            // Successful add - log at debug level
                                            if cfg!(debug_assertions) {
                                                println!(
                                                    "Added bid: {} @ {:.2} (ID: {}) for {}/{}",
                                                    order.quantity,
                                                    order.price_level,
                                                    order.unique_id,
                                                    order.symbol,
                                                    order.exchange
                                                );
                                            }
                                        }
                                    },
                                    "sell" => {
                                        if let Ok(_idx) = orderbook.add_limit_ask(
                                            order.price_level as f64,
                                            order_id,
                                            order.quantity as f64,
                                            current_timestamp
                                        ) {
                                            // Successful add - log at debug level
                                            if cfg!(debug_assertions) {
                                                println!(
                                                    "Added ask: {} @ {:.2} (ID: {}) for {}/{}",
                                                    order.quantity,
                                                    order.price_level,
                                                    order.unique_id,
                                                    order.symbol,
                                                    order.exchange
                                                );
                                            }
                                        }
                                    },
                                    _ => {
                                        eprintln!("Unknown order side: {}", order.side);
                                    }
                                }
                            },
                            "modify" => {
                                // Modify order
                                match order.side.as_str() {
                                    "buy" => {
                                        if let Ok(updated) = orderbook.edit_limit_bid(
                                            order.price_level as f64,
                                            order_id,
                                            order.quantity as f64
                                        ) {
                                            if updated && cfg!(debug_assertions) {
                                                println!(
                                                    "Modified bid: {} @ {:.2} (ID: {}) for {}/{}",
                                                    order.quantity,
                                                    order.price_level,
                                                    order.unique_id,
                                                    order.symbol,
                                                    order.exchange
                                                );
                                            }
                                        }
                                    },
                                    "sell" => {
                                        if let Ok(updated) = orderbook.edit_limit_ask(
                                            order.price_level as f64,
                                            order_id,
                                            order.quantity as f64
                                        ) {
                                            if updated && cfg!(debug_assertions) {
                                                println!(
                                                    "Modified ask: {} @ {:.2} (ID: {}) for {}/{}",
                                                    order.quantity,
                                                    order.price_level,
                                                    order.unique_id,
                                                    order.symbol,
                                                    order.exchange
                                                );
                                            }
                                        }
                                    },
                                    _ => {
                                        eprintln!("Unknown order side: {}", order.side);
                                    }
                                }
                            },
                            "cancel" => {
                                // Cancel order
                                match order.side.as_str() {
                                    "buy" => {
                                        if let Ok(removed) = orderbook.remove_limit_bid(
                                            order.price_level as f64,
                                            order_id
                                        ) {
                                            if removed && cfg!(debug_assertions) {
                                                println!(
                                                    "Canceled bid: (ID: {}) for {}/{}",
                                                    order.unique_id,
                                                    order.symbol,
                                                    order.exchange
                                                );
                                            }
                                        }
                                    },
                                    "sell" => {
                                        if let Ok(removed) = orderbook.remove_limit_ask(
                                            order.price_level as f64,
                                            order_id
                                        ) {
                                            if removed && cfg!(debug_assertions) {
                                                println!(
                                                    "Canceled ask: (ID: {}) for {}/{}",
                                                    order.unique_id,
                                                    order.symbol,
                                                    order.exchange
                                                );
                                            }
                                        }
                                    },
                                    _ => {
                                        eprintln!("Unknown order side: {}", order.side);
                                    }
                                }
                            },
                            _ => {
                                eprintln!("Unknown order event: {}", order.event);
                            }
                        }
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
        
        // Update orderbook metrics periodically
        self.update_all_orderbooks()?;
        
        Ok(())
    }
}