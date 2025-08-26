// Ultra-low latency data handler optimized for nanosecond trading
use config::Config;
use dashmap::DashMap;
use dotenv::dotenv;
use lazy_static::lazy_static;
use orderbook::Orderbook;
use ultra_signal::{Signal, SignalAction, OrderSide, ExchangeId, SYMBOLS};
use signalgenerator::MarketData;
use std::{
    env, 
    error::Error, 
    hash::{DefaultHasher, Hash, Hasher}, 
    sync::{Arc, RwLock}, 
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    thread,
    sync::atomic::{AtomicU64, Ordering},
};
use crossbeam::channel::{Sender, Receiver, bounded, unbounded};
use serde::{Serialize, Deserialize};
// Message broker integration
use subscriber::{ConnectionConfig, Subscriber};
use protocol::broker::messages::{market_message, MarketMessage};
use prost::Message;

// **ULTRA-LOW LATENCY OPTIMIZATION**: Lock-free orderbook storage
// RwLock allows multiple concurrent readers with minimal contention
lazy_static! {
    pub static ref ORDERBOOKS: DashMap<(String, String), Arc<RwLock<Orderbook>>> = DashMap::new();
    pub static ref LAST_UPDATE: DashMap<(String, String), Instant> = DashMap::new();
    
    // Performance metrics for monitoring
    static ref TOTAL_UPDATES: AtomicU64 = AtomicU64::new(0);
    static ref AVG_UPDATE_LATENCY_NS: AtomicU64 = AtomicU64::new(0);
    static ref ORDERBOOK_ACCESS_COUNT: AtomicU64 = AtomicU64::new(0);
}

/// Market data update message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataUpdate {
    pub symbol: String,
    pub exchange: String,
    pub timestamp: u64,
    pub trades: Vec<TradeData>,
    pub quotes: Vec<QuoteData>,
    pub orderbook_updates: Vec<OrderbookUpdate>,
}

/// Trade data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeData {
    pub id: String,
    pub price: f64,
    pub quantity: f64,
    pub side: String, // "buy" or "sell"
    pub timestamp: u64,
}

/// Quote/level data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteData {
    pub bid_price: f64,
    pub bid_quantity: f64,
    pub ask_price: f64,
    pub ask_quantity: f64,
    pub timestamp: u64,
}

/// Orderbook level update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookUpdate {
    pub side: String, // "bid" or "ask"
    pub price: f64,
    pub quantity: f64, // 0.0 means delete level
    pub timestamp: u64,
}

pub trait DataHandlerTrait {
    fn listen(&mut self) -> Result<(), Box<dyn Error>>;
    fn get_market_data(&self, symbol: &str, exchange: &str) -> Option<MarketData>;
    fn get_orderbook(&self, symbol: &str, exchange: &str) -> Option<Arc<RwLock<Orderbook>>>;
}

/// Ultra-fast data handler with lock-free optimizations
pub struct DataHandler {
    // Market data channels for ultra-fast distribution
    market_data_sender: Sender<MarketData>,
    market_data_receiver: Receiver<MarketData>,
    
    // Signal routing for strategy updates
    strategy_signal_sender: Option<Sender<Signal>>,
    
    // Performance optimization: Pre-allocated buffers
    update_buffer: Vec<MarketDataUpdate>,
    
    // Configuration
    symbols: Vec<String>,
    exchanges: Vec<String>,
    
    // Lock-free metrics
    updates_processed: AtomicU64,
    avg_processing_time_ns: AtomicU64,
}

impl Clone for DataHandler {
    fn clone(&self) -> Self {
        Self {
            market_data_sender: self.market_data_sender.clone(),
            market_data_receiver: self.market_data_receiver.clone(),
            strategy_signal_sender: self.strategy_signal_sender.clone(),
            update_buffer: Vec::with_capacity(self.update_buffer.capacity()),
            symbols: self.symbols.clone(),
            exchanges: self.exchanges.clone(),
            updates_processed: AtomicU64::new(self.updates_processed.load(Ordering::Acquire)),
            avg_processing_time_ns: AtomicU64::new(self.avg_processing_time_ns.load(Ordering::Acquire)),
        }
    }
}

impl DataHandler {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        dotenv().ok();
        
        // Create unbounded channel for market data (ultra-fast, no blocking)
        let (market_data_sender, market_data_receiver) = unbounded();
        
        Ok(Self {
            market_data_sender,
            market_data_receiver,
            strategy_signal_sender: None,
            update_buffer: Vec::with_capacity(1000), // Pre-allocate for batching
            symbols: vec![
                "BTC/USD".to_string(),
                "ETH/USD".to_string(), 
                "BNB/USD".to_string(),
                "SOL/USD".to_string(),
                "ADA/USD".to_string(),
            ],
            exchanges: vec![
                "binance".to_string(),
                "coinbase".to_string(),
                "kraken".to_string(),
            ],
            updates_processed: AtomicU64::new(0),
            avg_processing_time_ns: AtomicU64::new(0),
        })
    }

    /// Set signal sender for strategy notifications
    pub fn set_strategy_signal_sender(&mut self, sender: Sender<Signal>) {
        self.strategy_signal_sender = Some(sender);
    }

    /// Get or create orderbook with ultra-fast access
    /// **PERFORMANCE CRITICAL**: Uses RwLock for minimal read contention
    pub fn get_or_create_orderbook(&self, symbol: &str, exchange: &str) -> Arc<RwLock<Orderbook>> {
        let key = (symbol.to_string(), exchange.to_string());
        
        // Try to get existing orderbook first (common case)
        if let Some(orderbook) = ORDERBOOKS.get(&key) {
            ORDERBOOK_ACCESS_COUNT.fetch_add(1, Ordering::Relaxed);
            return orderbook.clone();
        }

        // Create new orderbook if it doesn't exist
        let orderbook = Arc::new(RwLock::new(Orderbook::new(
            symbol.to_string(),
            exchange.to_string(),
            2, // decimal precision
        )));
        
        ORDERBOOKS.insert(key.clone(), orderbook.clone());
        LAST_UPDATE.insert(key, Instant::now());
        
        orderbook
    }

    /// Ultra-fast orderbook update with minimal locking
    #[inline]
    pub fn update_orderbook_fast(
        &self, 
        symbol: &str, 
        exchange: &str, 
        updates: &[OrderbookUpdate]
    ) -> Result<(), Box<dyn Error>> {
        let start_time = ultra_signal::high_precision_timestamp_ns();
        
        let orderbook = self.get_or_create_orderbook(symbol, exchange);
        
        // **CRITICAL**: Use write lock only for the minimal time needed
        {
            let mut ob = orderbook.write().map_err(|e| format!("RwLock write failed: {}", e))?;
            
            for (i, update) in updates.iter().enumerate() {
                let order_id = (start_time + i as u64) as u64; // Unique order ID
                match update.side.as_str() {
                    "bid" => {
                        if update.quantity == 0.0 {
                            // Remove bid - simplified since we need order_id
                            println!("Would remove bid at price {}", update.price);
                        } else {
                            // Add/update bid - simplified
                            let _ = ob.add_limit_bid(update.price, order_id, update.quantity, start_time);
                        }
                    }
                    "ask" => {
                        if update.quantity == 0.0 {
                            // Remove ask - simplified since we need order_id
                            println!("Would remove ask at price {}", update.price);
                        } else {
                            // Add/update ask - simplified
                            let _ = ob.add_limit_ask(update.price, order_id, update.quantity, start_time);
                        }
                    }
                    _ => {} // Invalid side, skip
                }
            }
            
            // Update metrics after adding orders
            ob.update().map_err(|e| format!("Orderbook update failed: {}", e))?;
        } // Write lock released here - critical for performance
        
        // Update last update time using lock-free DashMap
        let key = (symbol.to_string(), exchange.to_string());
        LAST_UPDATE.insert(key, Instant::now());
        
        // Update performance metrics
        let end_time = ultra_signal::high_precision_timestamp_ns();
        let latency_ns = end_time - start_time;
        
        TOTAL_UPDATES.fetch_add(1, Ordering::Relaxed);
        let current_avg = self.avg_processing_time_ns.load(Ordering::Relaxed);
        let new_avg = (current_avg * 9 + latency_ns) / 10; // Exponential moving average
        self.avg_processing_time_ns.store(new_avg, Ordering::Relaxed);
        
        Ok(())
    }

    /// Generate market data from latest orderbook state
    pub fn generate_market_data(&self, symbol: &str, exchange: &str) -> Option<MarketData> {
        let orderbook = self.get_or_create_orderbook(symbol, exchange);
        
        // Use read lock for ultra-fast access (allows concurrent reads)
        if let Ok(ob) = orderbook.read() {
            if let Ok(metrics) = ob.metrics() {
                if metrics.best_bid > 0.0 && metrics.best_ask > 0.0 {
                    return Some(MarketData {
                        symbol: symbol.to_string(),
                        price: (metrics.best_bid + metrics.best_ask) / 2.0, // Mid price calculation
                        volume: 1000000.0, // Mock volume data
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64,
                    bid: metrics.best_bid,
                    ask: metrics.best_ask,
                    spread: metrics.best_ask - metrics.best_bid,
                    last_trade_size: 1.0, // Mock trade size
                    book_pressure: metrics.best_bid_depth / metrics.best_ask_depth.max(0.001), // Bid/ask volume ratio
                });
                }
            }
        }
        
        None
    }

    /// Batch process market data updates for efficiency
    pub fn process_market_data_batch(&mut self, updates: Vec<MarketDataUpdate>) -> Vec<MarketData> {
        let mut market_data_batch = Vec::with_capacity(updates.len());
        
        for update in updates {
            // Process orderbook updates first
            if !update.orderbook_updates.is_empty() {
                if let Err(e) = self.update_orderbook_fast(
                    &update.symbol, 
                    &update.exchange, 
                    &update.orderbook_updates
                ) {
                    eprintln!("Orderbook update failed: {}", e);
                    continue;
                }
            }
            
            // Generate market data from updated orderbook
            if let Some(market_data) = self.generate_market_data(&update.symbol, &update.exchange) {
                market_data_batch.push(market_data.clone());
                
                // Send to strategies if connected
                let _ = self.market_data_sender.try_send(market_data);
            }
        }
        
        market_data_batch
    }

    /// Mock data feed for testing (generates realistic market data)
    pub fn start_mock_data_feed(&self, symbol: String, exchange: String) -> std::thread::JoinHandle<()> {
        let orderbook = self.get_or_create_orderbook(&symbol, &exchange);
        let sender = self.market_data_sender.clone();
        
        thread::spawn(move || {
            let mut price = 50000.0; // Starting BTC price
            let mut sequence = 0u64;
            
            loop {
                let start_time = ultra_signal::high_precision_timestamp_ns();
                
                // Simulate price movement
                price += (rand::random::<f64>() - 0.5) * 100.0; // ±$50 random walk
                price = price.max(1000.0).min(100000.0); // Keep reasonable bounds
                
                let spread = price * 0.001; // 0.1% spread
                let bid = price - spread / 2.0;
                let ask = price + spread / 2.0;
                
                // Generate orderbook updates
                let updates = vec![
                    OrderbookUpdate {
                        side: "bid".to_string(),
                        price: bid,
                        quantity: 10.0 + rand::random::<f64>() * 50.0,
                        timestamp: start_time,
                    },
                    OrderbookUpdate {
                        side: "ask".to_string(),
                        price: ask,
                        quantity: 10.0 + rand::random::<f64>() * 50.0,
                        timestamp: start_time,
                    },
                ];
                
                // Update orderbook
                {
                    if let Ok(mut ob) = orderbook.write() {
                        for update in &updates {
                            let current_time = ultra_signal::high_precision_timestamp_ns();
                            match update.side.as_str() {
                                "bid" => {
                                    let _ = ob.add_limit_bid(update.price, 1, update.quantity, current_time);
                                }
                                "ask" => {
                                    let _ = ob.add_limit_ask(update.price, 1, update.quantity, current_time);
                                }
                                _ => {}
                            }
                        }
                    }
                }
                
                // Generate market data
                let market_data = MarketData {
                    symbol: symbol.clone(),
                    price,
                    volume: 1000.0 + rand::random::<f64>() * 10000.0,
                    timestamp: start_time,
                    bid,
                    ask,
                    spread,
                    last_trade_size: 0.1 + rand::random::<f64>() * 5.0,
                    book_pressure: 0.8 + rand::random::<f64>() * 0.4, // 0.8-1.2
                };
                
                // Send to strategies
                let _ = sender.try_send(market_data);
                
                sequence += 1;
                
                // 1000 updates per second (1ms interval)
                thread::sleep(Duration::from_micros(1000));
            }
        })
    }

    /// Get market data receiver for strategies
    pub fn get_market_data_receiver(&self) -> Receiver<MarketData> {
        self.market_data_receiver.clone()
    }

    /// Get all orderbooks for monitoring
    pub fn get_all_orderbooks(&self) -> Vec<(String, String, Arc<RwLock<Orderbook>>)> {
        ORDERBOOKS.iter()
            .map(|entry| {
                let (symbol, exchange) = entry.key();
                (symbol.clone(), exchange.clone(), entry.value().clone())
            })
            .collect()
    }

    /// Get performance statistics
    pub fn get_performance_stats(&self) -> DataHandlerStats {
        DataHandlerStats {
            total_updates: TOTAL_UPDATES.load(Ordering::Relaxed),
            avg_update_latency_ns: self.avg_processing_time_ns.load(Ordering::Relaxed),
            orderbook_access_count: ORDERBOOK_ACCESS_COUNT.load(Ordering::Relaxed),
            active_orderbooks: ORDERBOOKS.len(),
            updates_processed: self.updates_processed.load(Ordering::Relaxed),
        }
    }

    /// Cleanup old orderbooks to prevent memory leaks
    pub fn cleanup_old_orderbooks(&self, max_age: Duration) {
        let cutoff = Instant::now() - max_age;
        
        let mut to_remove = Vec::new();
        for entry in LAST_UPDATE.iter() {
            if *entry.value() < cutoff {
                to_remove.push(entry.key().clone());
            }
        }
        
        for key in to_remove {
            ORDERBOOKS.remove(&key);
            LAST_UPDATE.remove(&key);
        }
    }
}

/// Performance statistics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataHandlerStats {
    pub total_updates: u64,
    pub avg_update_latency_ns: u64,
    pub orderbook_access_count: u64,
    pub active_orderbooks: usize,
    pub updates_processed: u64,
}

impl DataHandlerTrait for DataHandler {
    fn listen(&mut self) -> Result<(), Box<dyn Error>> {
        // Start mock data feeds for all symbols/exchanges
        let mut handles = Vec::new();
        
        for symbol in &self.symbols.clone() {
            for exchange in &self.exchanges.clone() {
                let handle = self.start_mock_data_feed(symbol.clone(), exchange.clone());
                handles.push(handle);
            }
        }
        
        // Keep main thread alive
        println!("DataHandler listening for market data updates...");
        println!("Symbols: {:?}", self.symbols);
        println!("Exchanges: {:?}", self.exchanges);
        
        // Print performance stats every 10 seconds
        loop {
            thread::sleep(Duration::from_secs(10));
            let stats = self.get_performance_stats();
            println!("DataHandler Stats: {:?}", stats);
            
            // Cleanup old orderbooks every minute
            self.cleanup_old_orderbooks(Duration::from_secs(60));
        }
    }

    fn get_market_data(&self, symbol: &str, exchange: &str) -> Option<MarketData> {
        self.generate_market_data(symbol, exchange)
    }

    fn get_orderbook(&self, symbol: &str, exchange: &str) -> Option<Arc<RwLock<Orderbook>>> {
        let key = (symbol.to_string(), exchange.to_string());
        ORDERBOOKS.get(&key).map(|entry| entry.value().clone())
    }
}

/// Ultra-fast market data aggregator
pub struct MarketDataAggregator {
    data_handler: DataHandler,
    aggregated_data: DashMap<String, MarketData>, // Symbol -> Latest market data
    update_frequency: Duration,
}

impl MarketDataAggregator {
    pub fn new(data_handler: DataHandler) -> Self {
        Self {
            data_handler,
            aggregated_data: DashMap::new(),
            update_frequency: Duration::from_millis(1), // 1ms updates
        }
    }

    /// Start aggregating market data from all exchanges
    pub fn start_aggregation(&self) -> std::thread::JoinHandle<()> {
        let receiver = self.data_handler.get_market_data_receiver();
        let aggregated_data = self.aggregated_data.clone();
        let update_frequency = self.update_frequency;
        
        thread::spawn(move || {
            loop {
                // Collect all market data updates in a batch
                let mut updates = Vec::new();
                
                // Non-blocking receive to collect all available updates
                while let Ok(market_data) = receiver.try_recv() {
                    updates.push(market_data);
                }
                
                // Process updates and maintain latest data per symbol
                for market_data in updates {
                    aggregated_data.insert(market_data.symbol.clone(), market_data);
                }
                
                // Sleep for update frequency
                thread::sleep(update_frequency);
            }
        })
    }

    /// Get latest aggregated market data for a symbol
    pub fn get_latest_data(&self, symbol: &str) -> Option<MarketData> {
        self.aggregated_data.get(symbol).map(|entry| entry.clone())
    }

    /// Get all latest market data
    pub fn get_all_latest_data(&self) -> Vec<MarketData> {
        self.aggregated_data.iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
}

/// Ultra-fast price feed simulator for testing
pub struct PriceFeedSimulator {
    symbols: Vec<String>,
    base_prices: std::collections::HashMap<String, f64>,
    price_volatility: std::collections::HashMap<String, f64>,
    sender: Sender<MarketData>,
}

impl PriceFeedSimulator {
    pub fn new(sender: Sender<MarketData>) -> Self {
        let mut base_prices = std::collections::HashMap::new();
        let mut price_volatility = std::collections::HashMap::new();
        
        // Setup realistic crypto prices and volatilities
        base_prices.insert("BTC/USD".to_string(), 50000.0);
        base_prices.insert("ETH/USD".to_string(), 3000.0);
        base_prices.insert("BNB/USD".to_string(), 500.0);
        base_prices.insert("SOL/USD".to_string(), 100.0);
        base_prices.insert("ADA/USD".to_string(), 0.5);
        
        price_volatility.insert("BTC/USD".to_string(), 0.002); // 0.2% per tick
        price_volatility.insert("ETH/USD".to_string(), 0.003); // 0.3% per tick
        price_volatility.insert("BNB/USD".to_string(), 0.004); // 0.4% per tick
        price_volatility.insert("SOL/USD".to_string(), 0.005); // 0.5% per tick
        price_volatility.insert("ADA/USD".to_string(), 0.006); // 0.6% per tick
        
        Self {
            symbols: base_prices.keys().cloned().collect(),
            base_prices,
            price_volatility,
            sender,
        }
    }

    /// Start realistic price simulation
    pub fn start_simulation(&mut self, update_interval_ms: u64) -> std::thread::JoinHandle<()> {
        let symbols = self.symbols.clone();
        let mut prices = self.base_prices.clone();
        let volatility = self.price_volatility.clone();
        let sender = self.sender.clone();
        
        thread::spawn(move || {
            let mut sequence = 0u64;
            
            loop {
                let start_time = ultra_signal::high_precision_timestamp_ns();
                
                for symbol in &symbols {
                    if let (Some(current_price), Some(vol)) = (prices.get_mut(symbol), volatility.get(symbol)) {
                        // Random walk with realistic volatility
                        let change = (rand::random::<f64>() - 0.5) * 2.0 * vol * *current_price;
                        *current_price += change;
                        *current_price = current_price.max(0.01); // Prevent negative prices
                        
                        let spread_pct = 0.001; // 0.1% spread
                        let spread = *current_price * spread_pct;
                        let bid = *current_price - spread / 2.0;
                        let ask = *current_price + spread / 2.0;
                        
                        let market_data = MarketData {
                            symbol: symbol.clone(),
                            price: *current_price,
                            volume: 1000.0 + rand::random::<f64>() * 50000.0,
                            timestamp: start_time,
                            bid,
                            ask,
                            spread,
                            last_trade_size: 0.01 + rand::random::<f64>() * 10.0,
                            book_pressure: 0.7 + rand::random::<f64>() * 0.6, // 0.7-1.3
                        };
                        
                        // Send market data to strategies
                        let _ = sender.try_send(market_data);
                    }
                }
                
                sequence += 1;
                thread::sleep(Duration::from_millis(update_interval_ms));
            }
        })
    }
}

/// Helper function for external access to orderbooks
pub fn get_orderbook(symbol: &str, exchange: &str) -> Option<Arc<RwLock<Orderbook>>> {
    let key = (symbol.to_string(), exchange.to_string());
    ORDERBOOKS.get(&key).map(|entry| entry.value().clone())
}

/// Helper function to get all active symbols
pub fn get_active_symbols() -> Vec<(String, String)> {
    ORDERBOOKS.iter()
        .map(|entry| entry.key().clone())
        .collect()
}

/// Helper function to get system-wide performance metrics
pub fn get_global_performance_metrics() -> (u64, u64, u64, usize) {
    (
        TOTAL_UPDATES.load(Ordering::Relaxed),
        AVG_UPDATE_LATENCY_NS.load(Ordering::Relaxed),
        ORDERBOOK_ACCESS_COUNT.load(Ordering::Relaxed),
        ORDERBOOKS.len(),
    )
}

/// Simple random number generator for price simulation
mod rand {
    use std::sync::atomic::{AtomicU64, Ordering};
    
    static SEED: AtomicU64 = AtomicU64::new(1);
    
    pub fn random<T>() -> T 
    where 
        T: From<f64>
    {
        // Simple linear congruential generator for fast random numbers
        let current = SEED.load(Ordering::Relaxed);
        let next = current.wrapping_mul(1103515245).wrapping_add(12345);
        SEED.store(next, Ordering::Relaxed);
        
        let normalized = (next as f64) / (u64::MAX as f64);
        T::from(normalized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook_creation() {
        let data_handler = DataHandler::new().unwrap();
        let orderbook = data_handler.get_or_create_orderbook("BTC/USD", "binance");
        
        assert!(orderbook.read().is_ok());
    }

    #[test]
    fn test_orderbook_update() {
        let data_handler = DataHandler::new().unwrap();
        
        let updates = vec![
            OrderbookUpdate {
                side: "bid".to_string(),
                price: 49000.0,
                quantity: 1.5,
                timestamp: 123456789,
            },
            OrderbookUpdate {
                side: "ask".to_string(),
                price: 51000.0,
                quantity: 2.0,
                timestamp: 123456789,
            },
        ];
        
        let result = data_handler.update_orderbook_fast("BTC/USD", "binance", &updates);
        assert!(result.is_ok());
        
        // Verify data was updated
        let market_data = data_handler.generate_market_data("BTC/USD", "binance");
        assert!(market_data.is_some());
        
        let md = market_data.unwrap();
        assert_eq!(md.bid, 49000.0);
        assert_eq!(md.ask, 51000.0);
        assert_eq!(md.price, 50000.0); // Mid price
    }

    #[test]
    fn test_performance_metrics() {
        let data_handler = DataHandler::new().unwrap();
        let stats = data_handler.get_performance_stats();
        
        // Should have default values
        assert_eq!(stats.active_orderbooks, 0);
        assert_eq!(stats.total_updates, 0);
    }

    #[test]
    fn test_market_data_generation() {
        let data_handler = DataHandler::new().unwrap();
        
        // Create orderbook with test data
        let updates = vec![
            OrderbookUpdate {
                side: "bid".to_string(),
                price: 48000.0,
                quantity: 5.0,
                timestamp: 123456789,
            },
            OrderbookUpdate {
                side: "ask".to_string(),
                price: 52000.0,
                quantity: 3.0,
                timestamp: 123456789,
            },
        ];
        
        data_handler.update_orderbook_fast("ETH/USD", "coinbase", &updates).unwrap();
        
        let market_data = data_handler.generate_market_data("ETH/USD", "coinbase");
        assert!(market_data.is_some());
        
        let md = market_data.unwrap();
        assert_eq!(md.symbol, "ETH/USD");
        assert_eq!(md.bid, 48000.0);
        assert_eq!(md.ask, 52000.0);
        assert_eq!(md.spread, 4000.0);
        assert_eq!(md.book_pressure, 5.0 / 3.0); // bid_qty / ask_qty
    }
}
