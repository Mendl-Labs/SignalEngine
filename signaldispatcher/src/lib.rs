use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crossbeam::channel::{bounded, Sender, Receiver, TryRecvError};
use async_trait::async_trait;

// Import from other modules
use publisher::{Publisher, PublisherConfig, PublisherMetrics};
use serde::{Deserialize, Serialize};
use signalgenerator::{Signal, SignalAction, SignalStatus, SignalFilter, SignalStore, SignalStats};
use protocol::broker::messages::{Order, Orders};

/// Configuration for the signal dispatcher
#[derive(Clone)]
pub struct SignalDispatcherConfig {
    /// Publisher configuration
    pub publisher_config: PublisherConfig,
    /// Buffer size for signal queue
    pub signal_buffer_size: usize,
    /// Maximum batch size for processing signals
    pub max_batch_size: usize,
    /// Processing interval in milliseconds
    pub processing_interval_ms: u64,
    /// Enable auto-reconnect on publisher errors
    pub auto_reconnect: bool,
    /// Reconnect delay in milliseconds
    pub reconnect_delay_ms: u64,
    /// Signal filter for pre-filtering
    pub signal_filter: Option<SignalFilter>,
    /// Topic mappings: symbol -> topic_index
    pub topic_mappings: HashMap<String, usize>,
    /// Default topic index if symbol not found in mappings
    pub default_topic_index: usize,
}

impl Default for SignalDispatcherConfig {
    fn default() -> Self {
        Self {
            publisher_config: PublisherConfig::default(),
            signal_buffer_size: 10000,
            max_batch_size: 100,
            processing_interval_ms: 1,
            auto_reconnect: true,
            reconnect_delay_ms: 1000,
            signal_filter: None,
            topic_mappings: HashMap::new(),
            default_topic_index: 0,
        }
    }
}

/// Metrics for signal dispatcher
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct SignalDispatcherMetrics {
    pub signals_received: u64,
    pub signals_filtered: u64,
    pub signals_dispatched: u64,
    pub signals_failed: u64,
    pub orders_created: u64,
    pub trades_created: u64,
    pub cancellations_sent: u64,
    pub avg_dispatch_latency_us: u64,
    pub max_dispatch_latency_us: u64,
    pub publisher_reconnects: u64,
    pub last_signal_time: u64,
}

/// Signal to order conversion result
enum OrderConversion {
    Order(Order),
    CancelAll(String), // symbol
    Skip,
}

/// Signal dispatcher that publishes signals to the message broker
pub struct SignalDispatcher {
    /// Configuration
    config: SignalDispatcherConfig,
    
    /// Publisher instance
    publisher: Arc<Mutex<Publisher>>,
    
    /// Signal receiver
    signal_receiver: Receiver<Signal>,
    
    /// Signal sender (for external use)
    signal_sender: Sender<Signal>,
    
    /// Signal store for tracking
    signal_store: Arc<SignalStore>,
    
    /// Metrics
    metrics: Arc<Mutex<SignalDispatcherMetrics>>,
    
    /// Running flag
    running: Arc<Mutex<bool>>,
    
    /// Worker thread handle
    worker_handle: Option<JoinHandle<()>>,
    
    /// Logger function
    log_fn: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

// Manual Clone implementation (excluding worker_handle)
impl Clone for SignalDispatcher {
    fn clone(&self) -> Self {
        SignalDispatcher {
            config: self.config.clone(),
            publisher: self.publisher.clone(),
            signal_receiver: self.signal_receiver.clone(),
            signal_sender: self.signal_sender.clone(),
            signal_store: self.signal_store.clone(),
            metrics: self.metrics.clone(),
            running: self.running.clone(),
            worker_handle: None, // Do not clone the JoinHandle
            log_fn: self.log_fn.clone(),
        }
    }
}

impl SignalDispatcher {
    /// Create a new signal dispatcher
    pub fn new(config: SignalDispatcherConfig) -> Result<Self, String> {
        // Create publisher
        let publisher = Publisher::new(config.publisher_config.clone())
            .map_err(|e| format!("Failed to create publisher: {:?}", e))?;
        
        // Create signal channel
        let (signal_sender, signal_receiver) = bounded(config.signal_buffer_size);
        
        Ok(Self {
            config,
            publisher: Arc::new(Mutex::new(publisher)),
            signal_receiver,
            signal_sender,
            signal_store: Arc::new(SignalStore::new()),
            metrics: Arc::new(Mutex::new(SignalDispatcherMetrics::default())),
            running: Arc::new(Mutex::new(false)),
            worker_handle: None,
            log_fn: None,
        })
    }
    
    /// Set logger function
    pub fn set_logger<F>(&mut self, log_function: F)
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.log_fn = Some(Arc::new(log_function));
        
        // Also set logger on publisher
        if let Ok(mut publisher) = self.publisher.lock() {
            let log_fn_clone = self.log_fn.as_ref().unwrap().clone();
            publisher.set_logger(move |msg| {
                log_fn_clone(&format!("[Publisher] {}", msg));
            });
        }
    }
    
    /// Get signal sender for submitting signals
    pub fn get_sender(&self) -> Sender<Signal> {
        self.signal_sender.clone()
    }
    
    /// Get signal store
    pub fn get_store(&self) -> Arc<SignalStore> {
        self.signal_store.clone()
    }
    
    /// Get metrics
    pub fn get_metrics(&self) -> SignalDispatcherMetrics {
        self.metrics.lock().unwrap().clone()
    }
    
    /// Reset metrics
    pub fn reset_metrics(&self) {
        let mut metrics = self.metrics.lock().unwrap();
        *metrics = SignalDispatcherMetrics::default();
    }
    
    fn log(&self, message: &str) {
        if let Some(log_fn) = &self.log_fn {
            log_fn(&format!("[SignalDispatcher] {}", message));
        }
    }
    
    /// Convert signal to order/trade
    fn convert_signal_to_order(&self, signal: &Signal) -> OrderConversion {
        match signal.action {
            SignalAction::Buy | SignalAction::BuyLimit => {
                OrderConversion::Order(Order {
                    unique_id: signal.id.clone(),
                    symbol: signal.symbol.clone(),
                    exchange: signal.exchange.clone(),
                    price_level: signal.price.unwrap_or(0.0) as f32,
                    quantity: signal.quantity as f32,
                    side: "BUY".to_string(),
                    event: "NEW".to_string(),
                })
            },
            SignalAction::Sell | SignalAction::SellLimit => {
                OrderConversion::Order(Order {
                    unique_id: signal.id.clone(),
                    symbol: signal.symbol.clone(),
                    exchange: signal.exchange.clone(),
                    price_level: signal.price.unwrap_or(0.0) as f32,
                    quantity: signal.quantity as f32,
                    side: "SELL".to_string(),
                    event: "NEW".to_string(),
                })
            },
            SignalAction::Cancel => {
                // For individual cancels, create a cancel order event
                OrderConversion::Order(Order {
                    unique_id: signal.id.clone(),
                    symbol: signal.symbol.clone(),
                    exchange: signal.exchange.clone(),
                    price_level: 0.0,
                    quantity: 0.0,
                    side: "".to_string(),
                    event: "CANCEL".to_string(),
                })
            },
            SignalAction::CancelAll => {
                OrderConversion::CancelAll(signal.symbol.clone())
            },
            SignalAction::Hold => {
                OrderConversion::Skip
            },
        }
    }
    
    /// Get topic index for symbol
    fn get_topic_index(&self, symbol: &str) -> usize {
        self.config.topic_mappings
            .get(symbol)
            .copied()
            .unwrap_or(self.config.default_topic_index)
    }
    
    /// Process a batch of signals
    fn process_signal_batch(&self, signals: Vec<Signal>) -> Result<(), String> {
        if signals.is_empty() {
            return Ok(());
        }
        
        let start_time = Instant::now();
        let mut orders_batch = Vec::new();
        let mut cancel_all_symbols = Vec::new();
        
        // Convert signals to orders/trades
        for signal in signals {
            // Update metrics
            {
                let mut metrics = self.metrics.lock().unwrap();
                metrics.signals_received += 1;
                metrics.last_signal_time = current_time_ns();
            }
            
            // Apply filter if configured
            if let Some(filter) = &self.config.signal_filter {
                if !filter.passes(&signal) {
                    self.log(&format!("Signal {} filtered out", signal.id));
                    let mut metrics = self.metrics.lock().unwrap();
                    metrics.signals_filtered += 1;
                    continue;
                }
            }
            
            // Store signal
            self.signal_store.store(signal.clone())
                .unwrap_or_else(|e| self.log(&format!("Failed to store signal: {}", e)));
            
            // Update signal status to submitted
            self.signal_store.update_status(&signal.id, SignalStatus::Submitted)
                .unwrap_or_else(|e| self.log(&format!("Failed to update signal status: {}", e)));
            
            // Convert to order/trade
            match self.convert_signal_to_order(&signal) {
                OrderConversion::Order(order) => {
                    orders_batch.push((self.get_topic_index(&signal.symbol), order, signal.id));
                },
                OrderConversion::CancelAll(symbol) => {
                    cancel_all_symbols.push((self.get_topic_index(&symbol), symbol, signal.id));
                },
                OrderConversion::Skip => {
                    self.log(&format!("Skipping HOLD signal {}", signal.id));
                }
            }
        }
        
        let mut total_dispatched = 0;
        let mut total_failed = 0;
        
        // Publish orders in batches by topic
        if !orders_batch.is_empty() {
            // Group by topic
            let mut orders_by_topic: HashMap<usize, Vec<(Order, String)>> = HashMap::new();
            for (topic_idx, order, signal_id) in orders_batch {
                orders_by_topic.entry(topic_idx)
                    .or_insert_with(Vec::new)
                    .push((order, signal_id));
            }
            
            // Publish each topic's batch
            for (topic_idx, orders_with_ids) in orders_by_topic {
                let (orders, signal_ids): (Vec<_>, Vec<_>) = orders_with_ids.into_iter().unzip();
                let orders_msg = Orders { orders };
                
                match self.publisher.lock().unwrap().publish_orders(topic_idx, orders_msg) {
                    Ok(_) => {
                        total_dispatched += signal_ids.len();
                        let mut metrics = self.metrics.lock().unwrap();
                        metrics.orders_created += signal_ids.len() as u64;
                        
                        // Update signal statuses
                        for signal_id in signal_ids {
                            self.signal_store.update_status(&signal_id, SignalStatus::Submitted).ok();
                        }
                    },
                    Err(e) => {
                        total_failed += signal_ids.len();
                        self.log(&format!("Failed to publish orders: {:?}", e));
                        
                        // Update signal statuses
                        for signal_id in signal_ids {
                            self.signal_store.update_status(
                                &signal_id, 
                                SignalStatus::Rejected(format!("Publisher error: {:?}", e))
                            ).ok();
                        }
                    }
                }
            }
        }
        
        // Handle cancel all commands
        for (topic_idx, symbol, signal_id) in cancel_all_symbols {
            // Create a special cancel all order
            let cancel_order = Order {
                unique_id: format!("CANCEL_ALL_{}", signal_id),
                symbol: symbol.clone(),
                exchange: "".to_string(),
                price_level: 0.0,
                quantity: 0.0,
                side: "".to_string(),
                event: "CANCEL_ALL".to_string(),
            };
            
            match self.publisher.lock().unwrap().publish_order(topic_idx, cancel_order) {
                Ok(_) => {
                    total_dispatched += 1;
                    let mut metrics = self.metrics.lock().unwrap();
                    metrics.cancellations_sent += 1;
                    
                    self.signal_store.update_status(&signal_id, SignalStatus::Submitted).ok();
                },
                Err(e) => {
                    total_failed += 1;
                    self.log(&format!("Failed to publish cancel all for {}: {:?}", symbol, e));
                    
                    self.signal_store.update_status(
                        &signal_id,
                        SignalStatus::Rejected(format!("Publisher error: {:?}", e))
                    ).ok();
                }
            }
        }

        // Update metrics
        let dispatch_time = start_time.elapsed().as_micros() as u64;
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.signals_dispatched += total_dispatched as u64;
            metrics.signals_failed += total_failed as u64;
            
            // Update latency metrics with exponential moving average
            const ALPHA: f64 = 0.1;
            let cur_avg = metrics.avg_dispatch_latency_us as f64;
            metrics.avg_dispatch_latency_us = 
                ((1.0 - ALPHA) * cur_avg + ALPHA * dispatch_time as f64) as u64;
            
            metrics.max_dispatch_latency_us = 
                metrics.max_dispatch_latency_us.max(dispatch_time);
        }
        
        if total_dispatched > 0 {
            self.log(&format!(
                "Dispatched {} signals in {}us", 
                total_dispatched, 
                dispatch_time
            ));
        }
        
        Ok(())
    }
    
    /// Worker thread function
    fn worker_thread(
        signal_receiver: Receiver<Signal>,
        publisher: Arc<Mutex<Publisher>>,
        signal_store: Arc<SignalStore>,
        config: SignalDispatcherConfig,
        metrics: Arc<Mutex<SignalDispatcherMetrics>>,
        running: Arc<Mutex<bool>>,
        log_fn: Option<Arc<dyn Fn(&str) + Send + Sync>>,
    ) {
        let log = |msg: &str| {
            if let Some(f) = &log_fn {
                f(&format!("[SignalDispatcher Worker] {}", msg));
            }
        };
        
        log("Worker thread started");
        
        let mut signal_buffer = Vec::with_capacity(config.max_batch_size);
        let processing_interval = Duration::from_millis(config.processing_interval_ms);
        let mut last_process_time = Instant::now();
        
        loop {
            // Check if we should stop
            if !*running.lock().unwrap() {
                log("Received stop signal");
                break;
            }
            
            // Collect signals up to batch size
            let mut collected = 0;
            loop {
                match signal_receiver.try_recv() {
                    Ok(signal) => {
                        signal_buffer.push(signal);
                        collected += 1;
                        
                        if signal_buffer.len() >= config.max_batch_size {
                            break;
                        }
                    },
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        log("Signal channel disconnected");
                        *running.lock().unwrap() = false;
                        break;
                    }
                }
            }
            
            // Process batch if we have signals or enough time has passed
            let should_process = !signal_buffer.is_empty() && 
                (signal_buffer.len() >= config.max_batch_size || 
                 last_process_time.elapsed() >= processing_interval);
            
            if should_process {
                // Clone the dispatcher components for processing
                let dispatcher = SignalDispatcher {
                    config: config.clone(),
                    publisher: publisher.clone(),
                    signal_receiver: signal_receiver.clone(),
                    signal_sender: bounded(1).0, // Dummy sender
                    signal_store: signal_store.clone(),
                    metrics: metrics.clone(),
                    running: running.clone(),
                    worker_handle: None,
                    log_fn: log_fn.clone(),
                };
                
                // Process the batch
                let batch = signal_buffer.drain(..).collect::<Vec<_>>();
                if let Err(e) = dispatcher.process_signal_batch(batch) {
                    log(&format!("Error processing signal batch: {}", e));
                    
                    // Check if publisher is connected
                    if let Ok(publisher_guard) = publisher.lock() {
                        if !publisher_guard.is_connected() && config.auto_reconnect {
                            log("Publisher disconnected, attempting reconnect...");
                            drop(publisher_guard);
                            
                            // Try to reconnect
                            thread::sleep(Duration::from_millis(config.reconnect_delay_ms));
                            
                            if let Ok(mut publisher_guard) = publisher.lock() {
                                if let Err(e) = publisher_guard.start() {
                                    log(&format!("Failed to reconnect publisher: {:?}", e));
                                } else {
                                    log("Publisher reconnected successfully");
                                    let mut metrics = metrics.lock().unwrap();
                                    metrics.publisher_reconnects += 1;
                                }
                            }
                        }
                    }
                }
                
                last_process_time = Instant::now();
            }
            
            // Sleep if no signals
            if collected == 0 {
                thread::sleep(Duration::from_micros(100));
            }
        }
        
        // Process any remaining signals before shutting down
        if !signal_buffer.is_empty() {
            log(&format!("Processing {} remaining signals before shutdown", signal_buffer.len()));
            
            let dispatcher = SignalDispatcher {
                config,
                publisher,
                signal_receiver: signal_receiver.clone(),
                signal_sender: bounded(1).0, // Dummy sender
                signal_store,
                metrics,
                running,
                worker_handle: None,
                log_fn: log_fn.clone(),
            };
            
            let batch = signal_buffer.drain(..).collect::<Vec<_>>();
            let _ = dispatcher.process_signal_batch(batch);
        }
        
        log("Worker thread stopped");
    }
    
    /// Start the signal dispatcher
    pub fn start(&mut self) -> Result<(), String> {
        // Check if already running
        {
            let mut running = self.running.lock().unwrap();
            if *running {
                return Ok(());
            }
            *running = true;
        }
        
        self.log("Starting signal dispatcher");
        
        // Start the publisher
        {
            let mut publisher = self.publisher.lock().unwrap();
            publisher.start()
                .map_err(|e| format!("Failed to start publisher: {:?}", e))?;
        }
        
        // Clone components for worker thread
        let signal_receiver = self.signal_receiver.clone();
        let publisher = self.publisher.clone();
        let signal_store = self.signal_store.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let running = self.running.clone();
        let log_fn = self.log_fn.clone();
        
        // Start worker thread
        let handle = thread::spawn(move || {
            Self::worker_thread(
                signal_receiver,
                publisher,
                signal_store,
                config,
                metrics,
                running,
                log_fn,
            );
        });
        
        self.worker_handle = Some(handle);
        
        self.log("Signal dispatcher started successfully");
        
        Ok(())
    }
    
    /// Stop the signal dispatcher
    pub async fn stop(&mut self) -> Result<(), String> {
        self.log("Stopping signal dispatcher");
        
        // Signal worker to stop
        {
            let mut running = self.running.lock().unwrap();
            if !*running {
                return Ok(());
            }
            *running = false;
        }
        
        // Wait for worker thread
        if let Some(handle) = self.worker_handle.take() {
            handle.join()
                .map_err(|_| "Failed to join worker thread".to_string())?;
        }
        
        // Stop the publisher
        {
            let mut publisher = self.publisher.lock().unwrap();
            publisher.stop().await
                .map_err(|e| format!("Failed to stop publisher: {:?}", e))?;
        }
        
        self.log("Signal dispatcher stopped successfully");
        
        Ok(())
    }
    
    /// Submit a signal for dispatching
    pub fn submit_signal(&self, signal: Signal) -> Result<(), String> {
        // Validate signal
        signal.is_valid()?;
        
        // Send to channel
        self.signal_sender.send(signal)
            .map_err(|e| format!("Failed to submit signal: {}", e))
    }
    
    /// Submit multiple signals
    pub fn submit_signals(&self, signals: Vec<Signal>) -> Result<usize, String> {
        let mut success_count = 0;
        
        for signal in signals {
            if self.submit_signal(signal).is_ok() {
                success_count += 1;
            }
        }
        
        Ok(success_count)
    }
    
    /// Check if dispatcher is running
    pub fn is_running(&self) -> bool {
        *self.running.lock().unwrap()
    }
    
    /// Get publisher metrics
    pub fn get_publisher_metrics(&self) -> Result<PublisherMetrics, String> {
        self.publisher.lock().unwrap()
            .get_metrics()
            .map_err(|e| format!("Failed to get publisher metrics: {:?}", e))
    }
    
    /// Get signal statistics
    pub fn get_signal_stats(&self) -> SignalStats {
        self.signal_store.get_stats()
    }
}

/// Async trait for signal submission
#[async_trait]
pub trait SignalDispatcherTrait {
    async fn submit_signal_async(&self, signal: Signal) -> Result<(), String>;
    async fn submit_signals_async(&self, signals: Vec<Signal>) -> Result<usize, String>;
}

#[async_trait]
impl SignalDispatcherTrait for SignalDispatcher {
    async fn submit_signal_async(&self, signal: Signal) -> Result<(), String> {
        self.submit_signal(signal)
    }
    
    async fn submit_signals_async(&self, signals: Vec<Signal>) -> Result<usize, String> {
        self.submit_signals(signals)
    }
}

/// Builder for SignalDispatcher
pub struct SignalDispatcherBuilder {
    config: SignalDispatcherConfig,
}

impl SignalDispatcherBuilder {
    pub fn new(broker_addr: &str) -> Self {
        let mut config = SignalDispatcherConfig::default();
        config.publisher_config = PublisherConfig::new(broker_addr);
        
        Self { config }
    }
    
    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.config.publisher_config = self.config.publisher_config.with_topics(topics);
        self
    }
    
    pub fn with_topic_mappings(mut self, mappings: HashMap<String, usize>) -> Self {
        self.config.topic_mappings = mappings;
        self
    }
    
    pub fn with_signal_filter(mut self, filter: SignalFilter) -> Self {
        self.config.signal_filter = Some(filter);
        self
    }
    
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.config.signal_buffer_size = size;
        self
    }
    
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.config.max_batch_size = size;
        self.config.publisher_config = self.config.publisher_config.with_batch_size(size);
        self
    }
    
    pub fn with_processing_interval(mut self, interval_ms: u64) -> Self {
        self.config.processing_interval_ms = interval_ms;
        self
    }
    
    pub fn with_auto_reconnect(mut self, enabled: bool, delay_ms: u64) -> Self {
        self.config.auto_reconnect = enabled;
        self.config.reconnect_delay_ms = delay_ms;
        self
    }
    
    pub fn build(self) -> Result<SignalDispatcher, String> {
        SignalDispatcher::new(self.config)
    }
}

// Helper function to get current time in nanoseconds
#[inline]
fn current_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::signal::{Signal, SignalAction};
    
    #[test]
    fn test_signal_dispatcher_builder() {
        let dispatcher = SignalDispatcherBuilder::new("127.0.0.1:8080")
            .with_topics(vec!["orders.btc".to_string(), "orders.eth".to_string()])
            .with_topic_mappings(HashMap::from([
                ("BTC/USD".to_string(), 0),
                ("ETH/USD".to_string(), 1),
            ]))
            .with_buffer_size(5000)
            .with_batch_size(50)
            .with_processing_interval(5)
            .with_auto_reconnect(true, 2000)
            .build();
        
        assert!(dispatcher.is_ok());
    }
    
    #[test]
    fn test_signal_submission() {
        let dispatcher = SignalDispatcherBuilder::new("127.0.0.1:8080")
            .with_topics(vec!["test.topic".to_string()])
            .build()
            .unwrap();
        
        let signal = Signal::buy_limit(
            "test_strategy".to_string(),
            "BTC/USD".to_string(),
            "binance".to_string(),
            1.0,
            50000.0,
            0.85,
        );
        
        let result = dispatcher.submit_signal(signal);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_signal_conversion() {
        let dispatcher = SignalDispatcherBuilder::new("127.0.0.1:8080")
            .build()
            .unwrap();
        
        // Test buy limit conversion
        let buy_signal = Signal::buy_limit(
            "test".to_string(),
            "BTC/USD".to_string(),
            "binance".to_string(),
            1.0,
            50000.0,
            0.9,
        );
        
        match dispatcher.convert_signal_to_order(&buy_signal) {
            OrderConversion::Order(order) => {
                assert_eq!(order.side, "BUY");
                assert_eq!(order.price_level, 50000.0);
                assert_eq!(order.quantity, 1.0);
                assert_eq!(order.event, "NEW");
            },
            _ => panic!("Expected Order conversion"),
        }
        
        // Test cancel all conversion
        let cancel_signal = Signal::cancel_all(
            "test".to_string(),
            "ETH/USD".to_string(),
            "coinbase".to_string(),
        );
        
        match dispatcher.convert_signal_to_order(&cancel_signal) {
            OrderConversion::CancelAll(symbol) => {
                assert_eq!(symbol, "ETH/USD");
            },
            _ => panic!("Expected CancelAll conversion"),
        }
    }
}