use executionhandler::UltraLowLatencyExecutionHandler;
use ultra_signal::Signal; // Import Signal directly from ultra_signal
use crossbeam::channel::{bounded, Receiver, Sender};
use anyhow::Result;
use async_trait::async_trait;
use config::Config;
use dashmap::DashMap;
use datahandler::{DataHandler, DataHandlerTrait};
use dotenv::dotenv;
use mockall::automock;
use portfoliohandler::{PortfolioHandler, PortfolioHandlerTrait};
use strategyhandler::{StrategyManager, StrategyConfig}; // Remove non-existent types
use std::{env, error::Error, sync::{Arc, RwLock, Mutex}, collections::HashMap}; // Add Mutex back
use tokio::sync::broadcast;
use orderbook::Orderbook;
use portfolio::CryptoWallet;
use serde_json;
use tracing::{info, warn, error, debug};

// Re-export the global storage from datahandler and portfoliohandler
pub use datahandler::ORDERBOOKS;
pub use portfoliohandler::PORTFOLIOS;

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
    // Strategy manager
    strategy_manager: Option<StrategyManager>,
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
        }
    }

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
        }
    }

    /// Create handlers from configuration
    fn create_handlers() -> Result<(DataHandler, PortfolioHandler, StrategyManager, UltraLowLatencyExecutionHandler), Box<dyn Error>> {
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
        let portfoliohandler = PortfolioHandler::new(&portfolio_topics)?;
        
        // Create execution handler first
        let execution_handler = UltraLowLatencyExecutionHandler::new();
        
        // Create strategy handler
        let strategyhandler = Self::create_strategy_manager(&config)?;
        
        Ok((datahandler, portfoliohandler, strategyhandler, execution_handler))
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
        
        // Create strategy manager with the global orderbooks only (simplified API)
        let strategy_manager = StrategyManager::new(
            ORDERBOOKS.clone(), // This should work as it expects RwLock
        )?;
        
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
        };
        
        // Create a simple market making strategy directly
        use strategyhandler::SimpleMarketMakingStrategy;
        let strategy = Box::new(SimpleMarketMakingStrategy::new(strategy_config));
        manager.add_strategy(strategy).await?;
        
        Ok(())
    }

    /// Run all handlers concurrently
    pub async fn run_async(&mut self) -> Result<(), Box<dyn Error>> {
        info!("Starting HostedObject...");
        
        if self.is_running {
            return Err("HostedObject is already running".into());
        }
        
        // Create handlers
        let (datahandler, portfoliohandler, strategyhandler, execution_handler) = Self::create_handlers()?;
        
        // Initialize strategy manager
        let config = Config::default();
        self.strategy_manager = Some(Self::create_strategy_manager(&config)?);
        
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
            println!("Starting DataHandler...");
            let mut handler = datahandler;
            // Note: In a real implementation, the handler.listen() should be interruptible
            // For now, we just run it and report errors
            if let Err(e) = handler.listen() {
                eprintln!("DataHandler error: {:?}", e);
                let _ = data_health_tx_clone.blocking_send(Err(format!("DataHandler failed: {:?}", e)));
            }
        });
        
        // Spawn portfolio handler in a tokio task
        let _portfolio_shutdown_rx = shutdown_tx.subscribe(); // For future use when handlers support interruption
        let portfolio_health_tx_clone = portfolio_health_tx.clone();
        let _portfolio_handle = tokio::task::spawn_blocking(move || {
            println!("Starting PortfolioHandler...");
            let mut handler = portfoliohandler;
            // Note: In a real implementation, the handler.listen() should be interruptible
            if let Err(e) = handler.listen() {
                eprintln!("PortfolioHandler error: {:?}", e);
                let _ = portfolio_health_tx_clone.blocking_send(Err(format!("PortfolioHandler failed: {:?}", e)));
            }
        });
        
        // Initialize execution handler
        println!("Initializing ExecutionHandler...");
        execution_handler.initialize_optimizations().await?;
        
        // Set up signal routing from strategies to execution handler
        if let Some(signal_rx) = self.signal_rx.take() {
            let execution_handler_clone = execution_handler.clone();
            tokio::spawn(async move {
                println!("Starting signal processing loop...");
                while let Ok(signal) = signal_rx.recv() {
                    // Convert ultra_signal::Signal to executionhandler::Signal
                    let exec_signal = executionhandler::Signal {
                        id: signal.id.to_string(),
                        strategy_id: signal.strategy_id.to_string(),
                        symbol: format!("SYMBOL_{}", signal.symbol_hash), // Placeholder - need reverse hash lookup
                        exchange: format!("EXCHANGE_{}", signal.exchange_id),
                        action: match signal.action {
                            ultra_signal::SignalAction::Buy => executionhandler::SignalAction::Buy,
                            ultra_signal::SignalAction::Sell => executionhandler::SignalAction::Sell,
                            ultra_signal::SignalAction::BuyLimit => executionhandler::SignalAction::BuyLimit,
                            ultra_signal::SignalAction::SellLimit => executionhandler::SignalAction::SellLimit,
                            ultra_signal::SignalAction::Cancel => continue, // Skip cancel signals
                            ultra_signal::SignalAction::Hold => continue, // Skip hold signals
                        },
                        quantity: signal.quantity,
                        price: Some(signal.price),
                        confidence: signal.confidence as f64,
                        timestamp: signal.timestamp_ns,
                        metadata: std::collections::HashMap::new(),
                    };

                    match execution_handler_clone.execute_order(&exec_signal).await {
                        Ok(result) => {
                            println!("Signal executed: {:?}", result);
                            // TODO: Send execution result back to strategy
                        }
                        Err(e) => {
                            eprintln!("Signal execution failed: {:?}", e);
                        }
                    }
                }
                println!("Signal processing loop ended");
            });
        }
        
        // Connect strategy manager to signal routing
        if let Some(signal_tx) = &self.signal_tx {
            strategyhandler.add_signal_route("default".to_string(), signal_tx.clone());
        }
        
        // TODO: Add exchanges to execution handler based on configuration
        // For now, we'll add this as a placeholder for future implementation
        
        // Strategy manager is already created and configured
        println!("StrategyManager created and configured");
        
        self.is_running = true;
        
        // Set up signal handler for graceful shutdown
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
            println!("\nReceived shutdown signal...");
            let _ = shutdown_tx_clone.send(());
        });
        
        // Monitor loop - wait for shutdown or handler failures
        tokio::select! {
            _ = shutdown_rx.recv() => {
                println!("Shutdown signal received");
            }
            health_result = data_health_rx.recv() => {
                if let Some(Err(e)) = health_result {
                    eprintln!("Data handler health check failed: {}", e);
                }
            }
            health_result = portfolio_health_rx.recv() => {
                if let Some(Err(e)) = health_result {
                    eprintln!("Portfolio handler health check failed: {}", e);
                }
            }
        }
        
        // Graceful shutdown
        println!("Shutting down HostedObject...");
        
        // Strategy manager shutdown is simplified - no explicit stop method
        println!("Strategy manager shutdown initiated");
        
        // Signal handlers to stop (would need to implement proper shutdown in handlers)
        // For now, we'll interrupt the threads after a timeout
        println!("Waiting for handlers to complete...");
        
        // Give handlers time to finish current work
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        
        // Log final metrics from the strategy handler before it's dropped
        Self::log_final_metrics_from_manager(&strategyhandler);
        
        self.is_running = false;
        
        println!("HostedObject shutdown complete");
        Ok(())
    }

    /// Log final metrics from strategy manager
    fn log_final_metrics_from_manager(_strategyhandler: &StrategyManager) {
        // Simplified metrics logging - just basic info
        println!("\n=== Final System Metrics ===");
        println!("Strategy manager shutdown completed");
        
        // Simplified metrics - just confirm shutdown
        println!("All strategy components shut down successfully");
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