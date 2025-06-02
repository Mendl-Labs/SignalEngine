use anyhow::Result;
use async_trait::async_trait;
use config::Config;
use datahandler::{DataHandler, DataHandlerTrait};
use dotenv::dotenv;
use mockall::automock;
use portfoliohandler::{PortfolioHandler, PortfolioHandlerTrait};
use strategyhandler::{StrategyManager, StrategyConfig, StrategyType, RiskLimits, create_strategy};
use std::{env, error::Error, sync::{Arc, RwLock}, thread, collections::HashMap};
use tokio::sync::mpsc;
use orderbook::Orderbook;
use portfolio::CryptoWallet;
use serde_json;

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
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl HostedObject {
    /// Create a new HostedObject that will be initialized when run
    pub fn new() -> Self {
        Self {
            config_path: None,
            is_running: false,
            shutdown_tx: None,
        }
    }

    /// Create a HostedObject with a specific config path
    pub fn with_config_path(config_path: String) -> Self {
        Self {
            config_path: Some(config_path),
            is_running: false,
            shutdown_tx: None,
        }
    }

    /// Create handlers from configuration
    fn create_handlers() -> Result<(DataHandler, PortfolioHandler, StrategyManager), Box<dyn Error>> {
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
        
        // Create strategy handler
        let strategyhandler = Self::create_strategy_manager(&config)?;
        
        Ok((datahandler, portfoliohandler, strategyhandler))
    }

    /// Create the strategy manager from configuration
    fn create_strategy_manager(config: &Config) -> Result<StrategyManager, Box<dyn Error>> {
        // Get broker configuration
        let broker_addr = format!("{}:{}", config.message_broker.address, config.message_broker.port);
        
        // Define topics for strategy signals based on config
        let signal_topics: Vec<String> = config.publish_topics.iter()
            .filter(|topic| topic.contains("order") || topic.contains("signal"))
            .map(|s| s.to_string())
            .collect();
        
        // If no signal topics found, use defaults
        let signal_topics = if signal_topics.is_empty() {
            vec!["orders.btc".to_string(), "orders.eth".to_string()]
        } else {
            signal_topics
        };
        
        // Create strategy manager with shared orderbooks and portfolios
        let strategy_manager = StrategyManager::new(
            ORDERBOOKS.clone(),
            PORTFOLIOS.clone(),
            &broker_addr,
            signal_topics,
        )?;
        
        // Add default market making strategies if configured
        // In a real implementation, you'd load strategy configurations from the config file
        Self::add_default_strategies(&strategy_manager)?;
        
        Ok(strategy_manager)
    }

    /// Add default strategies to the manager
    fn add_default_strategies(manager: &StrategyManager) -> Result<(), Box<dyn Error>> {
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
            strategy_type: StrategyType::MarketMaking,
            enabled: true,
            symbols: vec!["BTC/USD".to_string(), "ETH/USD".to_string()],
            exchanges: vec!["binance".to_string()],
            parameters: params,
            risk_limits: RiskLimits {
                max_position_size: 10.0,
                max_order_size: 1.0,
                max_daily_loss: 1000.0,
                max_open_orders: 20,
                max_notional_exposure: 100000.0,
            },
        };
        
        let strategy = create_strategy(strategy_config)?;
        manager.add_strategy(strategy)?;
        
        Ok(())
    }

    /// Run all handlers concurrently
    pub async fn run_async(&mut self) -> Result<(), Box<dyn Error>> {
        println!("Starting HostedObject...");
        
        if self.is_running {
            return Err("HostedObject is already running".into());
        }
        
        // Create handlers
        let (datahandler, portfoliohandler, mut strategyhandler) = Self::create_handlers()?;
        
        // Create shutdown channels
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel(100);
        self.shutdown_tx = Some(shutdown_tx.clone());
        
        // Create channels to monitor handler health using tokio channels
        let (data_health_tx, mut data_health_rx) = tokio::sync::mpsc::channel(1);
        let (portfolio_health_tx, mut portfolio_health_rx) = tokio::sync::mpsc::channel(1);
        
        // Spawn data handler in a separate thread
        let _data_handle = {
            let health_tx: tokio::sync::mpsc::Sender<Result<(), String>> = data_health_tx.clone();
            thread::spawn(move || {
                println!("Starting DataHandler...");
                let mut handler = datahandler;
                if let Err(e) = handler.listen() {
                    eprintln!("DataHandler error: {:?}", e);
                    let _ = health_tx.blocking_send(Err(format!("DataHandler failed: {:?}", e)));
                }
            })
        };
        
        // Spawn portfolio handler in a separate thread
        let _portfolio_handle = {
            let health_tx: tokio::sync::mpsc::Sender<Result<(), String>> = portfolio_health_tx.clone();
            thread::spawn(move || {
                println!("Starting PortfolioHandler...");
                let mut handler = portfoliohandler;
                if let Err(e) = handler.listen() {
                    eprintln!("PortfolioHandler error: {:?}", e);
                    let _ = health_tx.blocking_send(Err(format!("PortfolioHandler failed: {:?}", e)));
                }
            })
        };
        
        // Start strategy manager
        println!("Starting StrategyManager...");
        strategyhandler.start(4)?; // Start with 4 worker threads
        
        self.is_running = true;
        
        // Set up signal handler for graceful shutdown
        let shutdown_tx_clone = shutdown_tx.clone();
        ctrlc::set_handler(move || {
            println!("\nReceived shutdown signal...");
            let _ = shutdown_tx_clone.blocking_send(());
        }).expect("Error setting Ctrl-C handler");
        
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
        
        // Stop strategy manager first (it publishes signals)
        strategyhandler.stop()?;
        
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
    fn log_final_metrics_from_manager(strategyhandler: &StrategyManager) {
        // Log strategy metrics
        let system_metrics = strategyhandler.get_system_metrics();
        println!("\n=== Final System Metrics ===");
        println!("Total strategies: {}", system_metrics.total_strategies);
        println!("Running strategies: {}", system_metrics.running_strategies);
        
        // Log signal statistics
        let signal_stats = &system_metrics.signal_stats;
        println!("\nSignal Statistics:");
        println!("  Total signals: {}", signal_stats.total_signals);
        println!("  Filled signals: {}", signal_stats.filled_signals);
        println!("  Win rate: {:.2}%", signal_stats.win_rate * 100.0);
        println!("  Total PnL: ${:.2}", signal_stats.total_pnl);
        
        // Log dispatcher metrics if available
        if let Some(dispatcher_metrics) = &system_metrics.dispatcher_metrics {
            println!("\nDispatcher Metrics:");
            println!("  Signals dispatched: {}", dispatcher_metrics.signals_dispatched);
            println!("  Signals failed: {}", dispatcher_metrics.signals_failed);
            println!("  Avg dispatch latency: {}μs", dispatcher_metrics.avg_dispatch_latency_us);
        }
        
        // Log strategy-specific metrics
        println!("\nStrategy Performance:");
        for (strategy_id, metrics) in &system_metrics.strategy_metrics {
            println!("  Strategy {}: PnL=${:.2}, Win Rate={:.2}%, Signals={}",
                strategy_id,
                metrics.total_pnl,
                metrics.win_rate * 100.0,
                metrics.signals_generated
            );
        }
    }

    /// Get shared orderbooks
    pub fn orderbooks(&self) -> Arc<RwLock<HashMap<(String, String), Arc<Orderbook>>>> {
        ORDERBOOKS.clone()
    }

    /// Get shared portfolios
    pub fn portfolios(&self) -> Arc<RwLock<HashMap<String, Arc<CryptoWallet>>>> {
        PORTFOLIOS.clone()
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
        let orderbooks = ORDERBOOKS.clone();
        let portfolios = PORTFOLIOS.clone();
        
        // Should be able to lock and use them
        {
            let ob_map = orderbooks.read().unwrap();
            assert!(ob_map.is_empty());
        }
        
        {
            let port_map = portfolios.read().unwrap();
            assert!(port_map.is_empty());
        }
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