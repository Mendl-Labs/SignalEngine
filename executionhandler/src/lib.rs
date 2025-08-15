pub mod core;
pub mod exchanges;
pub mod optimizations;
pub mod signal;
pub mod circuit_breaker;
pub mod position_tracker;
pub mod monitoring;

// Re-export main types and traits for easy access
pub use core::{
    ExchangeConnector, ExchangeAuth, ExchangeWebSocket, NanoOptimized,
    ExecutionResult, ExecutionStatus, ExecutionFill, ExecutionError,
    ExecutionMetrics, ExchangeConfig, OrderUpdate, HealthStatus,
    ExchangeLimits, CancelResult, OrderStatus, PooledOrder,
};

pub use exchanges::ExchangeFactory;
pub use signal::{Signal, SignalAction};
pub use circuit_breaker::{CircuitBreaker, ExchangeCircuitBreakerManager};
pub use position_tracker::{PositionTracker, Position, PositionSide, PortfolioPnL};
pub use monitoring::{PerformanceMonitor, PerformanceMetrics, Alert, AlertType, TradingLogger};
pub use optimizations::{
    timestamp::{nano_timestamp, NanoTimer},
    memory_pool::{get_thread_local_order, return_thread_local_order, preallocate_thread_local},
    cpu_affinity::{set_cpu_affinity, get_optimal_trading_core, CoreAssignment, set_high_priority},
    lock_free::{LockFreeRingBuffer, AtomicMetrics, SPSCQueue},
    simd_metrics::{simd_calculate_percentiles, VectorizedMetrics, LatencyHistogram},
};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::monitoring::MonitoringThresholds;

/// Ultra-low latency multi-exchange execution handler
#[derive(Clone)]
pub struct UltraLowLatencyExecutionHandler {
    connectors: Arc<RwLock<HashMap<String, Box<dyn ExchangeConnector>>>>,
    default_exchange: Option<String>,
    global_metrics: Arc<core::MetricsCollector>,
    core_assignment: optimizations::CoreAssignment,
    circuit_breakers: Arc<RwLock<ExchangeCircuitBreakerManager>>,
    position_tracker: Arc<PositionTracker>,
    performance_monitor: Arc<PerformanceMonitor>,
}

impl UltraLowLatencyExecutionHandler {
    /// Create a new multi-exchange execution handler
    pub fn new() -> Self {
        let core_assignment = optimizations::CoreAssignment::optimal_assignment();
        
        Self {
            connectors: Arc::new(RwLock::new(HashMap::new())),
            default_exchange: None,
            global_metrics: Arc::new(core::MetricsCollector::new()),
            core_assignment,
            circuit_breakers: Arc::new(RwLock::new(ExchangeCircuitBreakerManager::new())),
            position_tracker: Arc::new(PositionTracker::new()),
            performance_monitor: Arc::new(PerformanceMonitor::new(MonitoringThresholds::default())),
        }
    }

    /// Add an exchange connector
    pub async fn add_exchange(&mut self, exchange_name: String, config: ExchangeConfig) -> Result<(), ExecutionError> {
        let connector = ExchangeFactory::create_connector(&exchange_name, config).await?;
        
        let mut connectors = self.connectors.write().await;
        connectors.insert(exchange_name.clone(), connector);
        
        // Set first exchange as default if none set
        if self.default_exchange.is_none() {
            self.default_exchange = Some(exchange_name);
        }
        
        Ok(())
    }

    /// Remove an exchange connector
    pub async fn remove_exchange(&mut self, exchange_name: &str) -> Result<(), ExecutionError> {
        let mut connectors = self.connectors.write().await;
        connectors.remove(exchange_name);
        
        // Update default if removed
        if self.default_exchange.as_deref() == Some(exchange_name) {
            self.default_exchange = connectors.keys().next().cloned();
        }
        
        Ok(())
    }

    /// List all available exchanges
    pub async fn list_exchanges(&self) -> Vec<String> {
        let connectors = self.connectors.read().await;
        connectors.keys().cloned().collect()
    }

    /// Set the default exchange for orders without explicit exchange
    pub fn set_default_exchange(&mut self, exchange_name: String) {
        self.default_exchange = Some(exchange_name);
    }

    /// Execute order on specified exchange
    pub async fn execute_order_on_exchange(&self, signal: &Signal, exchange_name: &str) -> Result<ExecutionResult, ExecutionError> {
        let connectors = self.connectors.read().await;
        let connector = connectors.get(exchange_name)
            .ok_or_else(|| ExecutionError::Unknown(format!("Exchange not found: {}", exchange_name)))?;
        
        let timer = optimizations::NanoTimer::start();
        
        // Log order attempt
        TradingLogger::log_order_execution(
            exchange_name,
            &signal.symbol,
            &format!("{:?}", signal.action),
            signal.quantity,
            signal.price.unwrap_or(0.0),
            &signal.id,
            0, // Will update after execution
            "INITIATED"
        );

        let result = connector.execute_order(signal).await;
        let latency_ns = timer.elapsed_ns();
        
        // Record performance metrics
        self.performance_monitor.record_latency("execution", "order_placement", latency_ns).await;

        match &result {
            Ok(execution_result) => {
                self.global_metrics.record_success(latency_ns);
                
                // Log successful execution
                TradingLogger::log_order_execution(
                    exchange_name,
                    &signal.symbol,
                    &format!("{:?}", signal.action),
                    signal.quantity,
                    signal.price.unwrap_or(0.0),
                    &execution_result.order_id,
                    latency_ns,
                    &format!("{:?}", execution_result.status)
                );
                
                // Update position tracking with execution fills
                if !execution_result.fills.is_empty() {
                    if let Some(fill) = execution_result.fills.first() {
                        let old_position = self.position_tracker.get_position(&signal.symbol, exchange_name)
                            .unwrap_or_default()
                            .map(|p| p.quantity)
                            .unwrap_or(0.0);

                        let fill_quantity = if signal.action == SignalAction::Buy { 
                            fill.quantity 
                        } else { 
                            -fill.quantity 
                        };
                        
                        if let Err(e) = self.position_tracker.update_fill(
                            &signal.symbol,
                            exchange_name,
                            fill_quantity,
                            fill.price,
                            fill.fee
                        ) {
                            TradingLogger::log_error("position_tracker", "update_fill", &e, Some(&signal.symbol));
                        } else {
                            // Log position update
                            let new_position = self.position_tracker.get_position(&signal.symbol, exchange_name)
                                .unwrap_or_default()
                                .map(|p| p.quantity)
                                .unwrap_or(0.0);

                            TradingLogger::log_position_update(
                                &signal.symbol,
                                exchange_name,
                                old_position,
                                new_position,
                                fill.price,
                                0.0 // Realized PnL would be calculated in position tracker
                            );
                        }
                    }
                }
            }
            Err(error) => {
                self.global_metrics.record_failure();
                
                // Log error and record monitoring metric
                let error_msg = format!("{:?}", error);
                TradingLogger::log_error("execution", "order_placement", &error_msg, Some(&signal.symbol));
                self.performance_monitor.record_error("execution", "order_placement", &error_msg).await;
                
                // Log failed execution
                TradingLogger::log_order_execution(
                    exchange_name,
                    &signal.symbol,
                    &format!("{:?}", signal.action),
                    signal.quantity,
                    signal.price.unwrap_or(0.0),
                    &signal.id,
                    latency_ns,
                    "FAILED"
                );
            }
        }
        
        result
    }

    /// Execute order using signal's exchange or default
    pub async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
        let exchange_name = if !signal.exchange.is_empty() {
            signal.exchange.as_str()
        } else if let Some(ref default) = self.default_exchange {
            default.as_str()
        } else {
            return Err(ExecutionError::Unknown("No exchange specified and no default set".to_string()));
        };
        
        self.execute_order_on_exchange(signal, exchange_name).await
    }

    /// Execute batch orders across multiple exchanges
    pub async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        // Group signals by exchange
        let mut exchange_groups: HashMap<String, Vec<&Signal>> = HashMap::new();
        
        for signal in signals {
            let exchange = if !signal.exchange.is_empty() {
                signal.exchange.clone()
            } else if let Some(ref default) = self.default_exchange {
                default.clone()
            } else {
                return Err(ExecutionError::Unknown("No exchange specified and no default set".to_string()));
            };
            
            exchange_groups.entry(exchange).or_default().push(signal);
        }
        
        // Execute each group sequentially to avoid lifetime issues
        let connectors = self.connectors.read().await;
        let mut all_results = Vec::new();
        
        for (exchange_name, exchange_signals) in exchange_groups {
            if let Some(connector) = connectors.get(&exchange_name) {
                let signals_vec: Vec<Signal> = exchange_signals.into_iter().cloned().collect();
                
                let mut batch_results = connector.execute_batch_orders(&signals_vec).await?;
                all_results.append(&mut batch_results);
            }
        }
        
        Ok(all_results)
    }

    /// Cancel order on specific exchange
    pub async fn cancel_order_on_exchange(&self, order_id: &str, exchange_name: &str) -> Result<CancelResult, ExecutionError> {
        let connectors = self.connectors.read().await;
        let connector = connectors.get(exchange_name)
            .ok_or_else(|| ExecutionError::Unknown(format!("Exchange not found: {}", exchange_name)))?;
        
        connector.cancel_order(order_id).await
    }

    /// Cancel all orders across all exchanges
    pub async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError> {
        let connectors = self.connectors.read().await;
        let mut all_results = Vec::new();
        
        for (_exchange_name, connector) in connectors.iter() {
            match connector.cancel_all_orders().await {
                Ok(mut results) => all_results.append(&mut results),
                Err(_) => continue, // Skip failed exchanges
            }
        }
        
        Ok(all_results)
    }

    /// Get aggregated metrics across all exchanges
    pub async fn get_global_metrics(&self) -> ExecutionMetrics {
        self.global_metrics.get_metrics("Global".to_string())
    }

    /// Get metrics for specific exchange
    pub async fn get_exchange_metrics(&self, exchange_name: &str) -> Option<ExecutionMetrics> {
        let connectors = self.connectors.read().await;
        connectors.get(exchange_name).map(|c| c.get_metrics())
    }

    /// Get health status for all exchanges
    pub async fn health_check_all(&self) -> HashMap<String, HealthStatus> {
        let connectors = self.connectors.read().await;
        let mut results = HashMap::new();
        
        for (exchange_name, connector) in connectors.iter() {
            match connector.health_check().await {
                Ok(status) => { results.insert(exchange_name.clone(), status); }
                Err(_) => continue,
            }
        }
        
        results
    }

    /// Initialize system optimizations
    pub async fn initialize_optimizations(&self) -> Result<(), ExecutionError> {
        // Set CPU affinity for trading threads
        if let Err(e) = optimizations::set_cpu_affinity(self.core_assignment.primary_execution) {
            log::warn!("Failed to set CPU affinity: {}", e);
        }
        
        // Set high process priority
        if let Err(e) = optimizations::set_high_priority() {
            log::warn!("Failed to set high priority: {}", e);
        }
        
        // Preallocate memory pools
        optimizations::preallocate_thread_local(1000);
        
        // Initialize global memory pool
        core::initialize_order_pool(10000);
        
        Ok(())
    }

    /// Create configuration template for supported exchange
    pub fn create_exchange_config(exchange_name: &str) -> Result<ExchangeConfig, ExecutionError> {
        ExchangeFactory::create_config_template(exchange_name)
    }

    /// Get supported exchanges
    pub fn supported_exchanges() -> Vec<&'static str> {
        ExchangeFactory::supported_exchanges()
    }

    // Position tracking methods

    /// Get current position for a symbol on an exchange
    pub fn get_position(&self, symbol: &str, exchange: &str) -> Result<Option<position_tracker::Position>, ExecutionError> {
        self.position_tracker.get_position(symbol, exchange)
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {}", e)))
    }

    /// Get all current positions
    pub fn get_all_positions(&self) -> Result<HashMap<(String, String), position_tracker::Position>, ExecutionError> {
        self.position_tracker.get_all_positions()
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {}", e)))
    }

    /// Get net position for a symbol across all exchanges
    pub fn get_net_position(&self, symbol: &str) -> Result<f64, ExecutionError> {
        self.position_tracker.get_net_position(symbol)
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {}", e)))
    }

    /// Get total portfolio PnL
    pub fn get_portfolio_pnl(&self) -> Result<position_tracker::PortfolioPnL, ExecutionError> {
        self.position_tracker.get_total_pnl()
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {}", e)))
    }

    /// Get PnL by exchange
    pub fn get_pnl_by_exchange(&self) -> Result<HashMap<String, f64>, ExecutionError> {
        self.position_tracker.get_pnl_by_exchange()
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {}", e)))
    }

    /// Update market price for PnL calculations
    pub fn update_market_price(&self, symbol: &str, price: f64) -> Result<(), ExecutionError> {
        self.position_tracker.update_market_price(symbol, price)
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {}", e)))
    }

    /// Get positions that need market price updates (stale positions)
    pub fn get_stale_positions(&self, max_age_ms: u64) -> Result<Vec<(String, String)>, ExecutionError> {
        self.position_tracker.get_stale_positions(max_age_ms)
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {}", e)))
    }

    // Performance monitoring methods

    /// Get current performance metrics
    pub async fn get_performance_metrics(&self) -> monitoring::PerformanceMetrics {
        self.performance_monitor.get_metrics().await
    }

    /// Get recent alerts
    pub async fn get_recent_alerts(&self, max_age_ms: u64) -> Vec<monitoring::Alert> {
        self.performance_monitor.get_recent_alerts(max_age_ms).await
    }

    /// Generate comprehensive performance report
    pub async fn generate_performance_report(&self) -> monitoring::PerformanceReport {
        self.performance_monitor.generate_report().await
    }

    /// Acknowledge alert by index
    pub async fn acknowledge_alert(&self, alert_index: usize) -> Result<(), ExecutionError> {
        self.performance_monitor.acknowledge_alert(alert_index).await
            .map_err(|e| ExecutionError::Unknown(format!("Alert acknowledgment error: {}", e)))
    }

    /// Record custom performance metric
    pub async fn record_custom_metric(&self, component: &str, operation: &str, latency_ns: u64) {
        self.performance_monitor.record_latency(component, operation, latency_ns).await;
    }

    /// Update memory usage tracking
    pub async fn update_memory_usage(&self, component: &str, memory_bytes: u64) {
        self.performance_monitor.record_memory_usage(component, memory_bytes).await;
    }

    /// Update throughput tracking
    pub async fn update_throughput(&self, component: &str, ops_per_second: f64) {
        self.performance_monitor.record_throughput(component, ops_per_second).await;
    }
}

impl Default for UltraLowLatencyExecutionHandler {
    fn default() -> Self {
        Self::new()
    }
}

// For backwards compatibility, provide the old interface
pub use core::types::KrakenCredentials;

impl UltraLowLatencyExecutionHandler {
    /// Legacy constructor for backwards compatibility with Kraken-only setup
    pub fn new_kraken(
        _exchange: String,
        credentials: KrakenCredentials,
        connection_pool_size: Option<usize>,
        timeout_ms: Option<u64>,
    ) -> Self {
        let mut handler = Self::new();
        
        // Create Kraken config from legacy parameters
        let config = ExchangeConfig {
            name: "Kraken".to_string(),
            api_key: credentials.api_key,
            secret_key: credentials.secret_key,
            passphrase: None,
            sandbox: false,
            connection_pool_size: connection_pool_size.unwrap_or(10),
            timeout_ms: timeout_ms.unwrap_or(5000),
            rate_limit_per_second: 20,
            rate_limit_burst: 60,
            websocket_url: Some("wss://ws.kraken.com".to_string()),
            rest_api_url: Some("https://api.kraken.com".to_string()),
            custom_headers: HashMap::new(),
        };
        
        // Note: This is async in practice, but for compatibility we return the handler
        // In real usage, you'd need to call add_exchange separately
        handler.default_exchange = Some("Kraken".to_string());
        handler
    }

    /// Legacy start method for backwards compatibility
    pub async fn start(&self) -> Result<(), ExecutionError> {
        self.initialize_optimizations().await
    }

    /// Legacy get_metrics method for backwards compatibility  
    pub fn get_metrics(&self) -> ExecutionMetrics {
        // Return default metrics for compatibility
        ExecutionMetrics {
            exchange: "Legacy".to_string(),
            ..Default::default()
        }
    }

    /// Legacy methods for backwards compatibility
    pub async fn add_execution_callback<F>(&self, _callback: F)
    where
        F: Fn(&ExecutionResult) + Send + Sync + 'static,
    {
        // Placeholder for backwards compatibility
    }

    pub async fn add_fill_callback<F>(&self, _callback: F)
    where
        F: Fn(&ExecutionFill) + Send + Sync + 'static,
    {
        // Placeholder for backwards compatibility  
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<bool, ExecutionError> {
        if let Some(ref exchange) = self.default_exchange {
            let result = self.cancel_order_on_exchange(order_id, exchange).await?;
            Ok(matches!(result.status, core::types::CancelStatus::Cancelled))
        } else {
            Err(ExecutionError::Unknown("No default exchange set".to_string()))
        }
    }

    pub async fn get_order_status(&self, order_id: &str) -> Result<Option<OrderStatus>, ExecutionError> {
        if let Some(ref exchange) = self.default_exchange {
            let connectors = self.connectors.read().await;
            if let Some(connector) = connectors.get(exchange) {
                connector.get_order_status(order_id).await
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handler_creation() {
        let handler = UltraLowLatencyExecutionHandler::new();
        assert!(handler.list_exchanges().await.is_empty());
    }

    #[tokio::test]
    async fn test_exchange_management() {
        let mut handler = UltraLowLatencyExecutionHandler::new();
        
        // Test adding exchange
        let config = ExchangeFactory::create_config_template("kraken").unwrap();
        handler.add_exchange("Kraken".to_string(), config).await.unwrap();
        
        let exchanges = handler.list_exchanges().await;
        assert_eq!(exchanges.len(), 1);
        assert_eq!(exchanges[0], "Kraken");
        
        // Test removing exchange
        handler.remove_exchange("Kraken").await.unwrap();
        assert!(handler.list_exchanges().await.is_empty());
    }

    #[tokio::test]
    async fn test_supported_exchanges() {
        let supported = UltraLowLatencyExecutionHandler::supported_exchanges();
        assert!(supported.contains(&"kraken"));
        assert!(supported.contains(&"binance"));
        assert!(supported.contains(&"coinbase"));
    }

    #[test]
    fn test_nano_timestamp() {
        let ts1 = optimizations::nano_timestamp();
        let ts2 = optimizations::nano_timestamp();
        assert!(ts2 >= ts1);
    }

    #[test]
    fn test_memory_pool() {
        let order1 = optimizations::get_thread_local_order();
        let order2 = optimizations::get_thread_local_order();
        
        optimizations::return_thread_local_order(order1);
        optimizations::return_thread_local_order(order2);
    }

    #[test]
    fn test_metrics_calculation() {
        let latencies = vec![100.0, 200.0, 300.0, 400.0, 500.0];
        let metrics = optimizations::simd_calculate_percentiles(&latencies);
        
        assert_eq!(metrics.count, 5);
        assert_eq!(metrics.min_ns, 100);
        assert_eq!(metrics.max_ns, 500);
    }

    #[test]
    fn test_legacy_kraken_constructor() {
        let credentials = KrakenCredentials {
            api_key: "test_key".to_string(),
            secret_key: "test_secret".to_string(),
        };

        let handler = UltraLowLatencyExecutionHandler::new_kraken(
            "Kraken".to_string(),
            credentials,
            Some(5),
            Some(3000),
        );

        assert_eq!(handler.default_exchange, Some("Kraken".to_string()));
    }
}
