use async_trait::async_trait;
use crate::signal::Signal;
use crate::core::types::*;

/// Core trait that all exchanges must implement for ultra-low latency execution
#[async_trait]
pub trait ExchangeConnector: Send + Sync {
    /// Get the exchange name (e.g., "Kraken", "Binance", "Coinbase")
    fn exchange_name(&self) -> &str;

    /// Initialize connection pools and prepare for trading
    async fn initialize(&mut self, config: ExchangeConfig) -> Result<(), ExecutionError>;

    /// Execute a single order with nanosecond precision timing
    async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError>;

    /// Execute multiple orders in parallel for maximum throughput
    async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError>;

    /// Execute multiple orders sequentially (safe, reliable)
    async fn execute_batch_orders_sequential(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        let mut results = Vec::with_capacity(signals.len());
        
        for signal in signals {
            match self.execute_order(signal).await {
                Ok(result) => results.push(result),
                Err(e) => {
                    // Continue processing other orders even if one fails
                    results.push(ExecutionResult {
                        order_id: format!("failed_{}", signal.id),
                        exchange_order_id: None,
                        exchange: self.exchange_name().to_string(),
                        status: ExecutionStatus::Rejected,
                        filled_quantity: 0.0,
                        remaining_quantity: signal.quantity,
                        avg_fill_price: 0.0,
                        total_fees: 0.0,
                        fills: vec![],
                        reject_reason: Some(format!("Order execution failed: {}", e)),
                        submitted_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos(),
                        updated_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos(),
                        latency_ns: 0,
                        exchange_timestamp_ns: None,
                        exchange_sequence: None,
                    });
                }
            }
        }
        
        Ok(results)
    }

    /// Execute multiple orders in parallel (high performance)
    async fn execute_batch_orders_parallel(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        // Create futures for all orders
        let futures = signals.iter().map(|signal| {
            async move {
                match self.execute_order(signal).await {
                    Ok(result) => result,
                    Err(e) => ExecutionResult {
                        order_id: format!("failed_{}", signal.id),
                        exchange_order_id: None,
                        exchange: self.exchange_name().to_string(),
                        status: ExecutionStatus::Rejected,
                        filled_quantity: 0.0,
                        remaining_quantity: signal.quantity,
                        avg_fill_price: 0.0,
                        total_fees: 0.0,
                        fills: vec![],
                        reject_reason: Some(format!("Order execution failed: {}", e)),
                        submitted_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos(),
                        updated_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos(),
                        latency_ns: 0,
                        exchange_timestamp_ns: None,
                        exchange_sequence: None,
                    }
                }
            }
        });

        // Execute all orders concurrently
        let results: Vec<ExecutionResult> = futures::future::join_all(futures).await;
        Ok(results)
    }

    /// Execute multiple orders with optimized batching (hybrid approach)
    async fn execute_batch_orders_optimized(&self, signals: &[Signal], batch_size: usize) -> Result<Vec<ExecutionResult>, ExecutionError> {
        let mut all_results = Vec::with_capacity(signals.len());
        
        // Process in batches to balance throughput and resource usage
        for batch in signals.chunks(batch_size) {
            let batch_results = self.execute_batch_orders_parallel(batch).await?;
            all_results.extend(batch_results);
            
            // Optional: Add small delay between batches to prevent overwhelming the exchange
            if batch.len() == batch_size && all_results.len() < signals.len() {
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        }
        
        Ok(all_results)
    }

    /// Cancel a specific order by ID
    async fn cancel_order(&self, order_id: &str) -> Result<CancelResult, ExecutionError>;

    /// Cancel all active orders for this exchange
    async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError>;

    /// Edit/modify an existing order's parameters
    /// Note: On Kraken, this cancels the original order and creates a new one
    async fn edit_order(&self, params: EditOrderParams) -> Result<EditResult, ExecutionError>;

    /// Get current order status with minimal latency
    async fn get_order_status(&self, order_id: &str) -> Result<Option<OrderStatus>, ExecutionError>;

    /// Get real-time performance metrics
    fn get_metrics(&self) -> ExecutionMetrics;

    /// Subscribe to real-time order updates via WebSocket
    async fn subscribe_to_updates(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>);

    /// Check if the exchange connection is healthy
    async fn health_check(&self) -> Result<HealthStatus, ExecutionError>;

    /// Get exchange-specific limits and capabilities
    fn get_limits(&self) -> ExchangeLimits;

    /// Validate order parameters before submission
    fn validate_order(&self, signal: &Signal) -> Result<(), ExecutionError>;

    /// Convert internal signal to exchange-specific format
    fn convert_signal(&self, signal: &Signal) -> Result<ExchangeOrder, ExecutionError>;

    /// Feed a live order book snapshot into the connector for a given symbol.
    ///
    /// Paper connectors use this to populate their internal `MockExchangeConnector`
    /// with real market depth so that simulated fills use realistic book-walking
    /// rather than a flat slippage percentage.
    ///
    /// Live connectors receive order book data directly from the exchange WebSocket
    /// and can leave this as the default no-op.
    fn update_book(&self, _symbol: &str, _bids: Vec<(f64, f64)>, _asks: Vec<(f64, f64)>) {}
}

/// Trait for exchange-specific authentication
#[async_trait]
pub trait ExchangeAuth: Send + Sync {
    /// Sign a request with exchange-specific authentication
    async fn sign_request(&self, method: &str, path: &str, body: &str, timestamp: u64) -> Result<AuthHeaders, ExecutionError>;

    /// Refresh authentication tokens if needed
    async fn refresh_auth(&mut self) -> Result<(), ExecutionError>;

    /// Validate authentication credentials
    async fn validate_credentials(&self) -> Result<bool, ExecutionError>;
}

/// Trait for exchange-specific WebSocket handling
#[async_trait]
pub trait ExchangeWebSocket: Send + Sync {
    /// Connect to exchange WebSocket feeds
    async fn connect(&mut self, channels: Vec<String>) -> Result<(), ExecutionError>;

    /// Disconnect from WebSocket feeds
    async fn disconnect(&mut self) -> Result<(), ExecutionError>;

    /// Subscribe to specific trading pairs or order updates
    async fn subscribe(&mut self, subscription: WebSocketSubscription) -> Result<(), ExecutionError>;

    /// Unsubscribe from feeds
    async fn unsubscribe(&mut self, subscription: WebSocketSubscription) -> Result<(), ExecutionError>;

    /// Process incoming WebSocket messages
    async fn process_message(&self, message: &str) -> Result<Vec<OrderUpdate>, ExecutionError>;
}

/// Trait for nanosecond-level optimizations
pub trait NanoOptimized {
    /// Get nanosecond-precision timestamp
    fn nano_timestamp() -> u128;

    /// Pre-allocate memory for hot path operations
    fn preallocate_buffers(&mut self, capacity: usize);

    /// Use memory pool for order objects to avoid allocations
    fn get_pooled_order(&self) -> PooledOrder;

    /// Return order object to memory pool
    fn return_pooled_order(&self, order: PooledOrder);

    /// Enable CPU affinity for critical threads
    fn set_cpu_affinity(&self, core_id: usize) -> Result<(), ExecutionError>;

    /// Use SIMD operations for bulk calculations
    fn simd_calculate_metrics(&self, latencies: &[f64]) -> MetricsSnapshot;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::Signal;

    /// Mock connector that succeeds on even-indexed signals, fails on odd
    struct MockConnector;

    #[async_trait]
    impl ExchangeConnector for MockConnector {
        fn exchange_name(&self) -> &str { "mock" }
        async fn initialize(&mut self, _config: ExchangeConfig) -> Result<(), ExecutionError> { Ok(()) }
        async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
            let id_num: u64 = signal.id.parse().unwrap_or(0);
            if id_num % 2 == 0 {
                Ok(ExecutionResult {
                    order_id: signal.id.clone(),
                    exchange_order_id: Some(format!("exch_{}", signal.id)),
                    exchange: "mock".into(),
                    status: ExecutionStatus::Filled,
                    filled_quantity: signal.quantity,
                    remaining_quantity: 0.0,
                    avg_fill_price: 100.0,
                    total_fees: 0.1,
                    fills: vec![],
                    reject_reason: None,
                    submitted_at: 0,
                    updated_at: 0,
                    latency_ns: 500,
                    exchange_timestamp_ns: None,
                    exchange_sequence: None,
                })
            } else {
                Err(ExecutionError::Rejected("odd id".into()))
            }
        }
        async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
            self.execute_batch_orders_sequential(signals).await
        }
        async fn cancel_order(&self, _id: &str) -> Result<CancelResult, ExecutionError> { unimplemented!() }
        async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError> { unimplemented!() }
        async fn edit_order(&self, _p: EditOrderParams) -> Result<EditResult, ExecutionError> { unimplemented!() }
        async fn get_order_status(&self, _id: &str) -> Result<Option<OrderStatus>, ExecutionError> { unimplemented!() }
        fn get_metrics(&self) -> ExecutionMetrics {
            ExecutionMetrics {
                exchange: "mock".into(),
                total_orders: 0, successful_orders: 0, failed_orders: 0, cancelled_orders: 0,
                avg_latency_ns: 0, min_latency_ns: 0, max_latency_ns: 0,
                p50_latency_ns: 0, p95_latency_ns: 0, p99_latency_ns: 0, p999_latency_ns: 0,
                total_volume: 0.0, total_fees: 0.0, fill_rate: 0.0, error_rate: 0.0,
                orders_per_second: 0.0, last_updated: 0,
                websocket_connected: false, connection_pool_utilization: 0.0, rate_limit_utilization: 0.0,
            }
        }
        async fn subscribe_to_updates(&self, _cb: Box<dyn Fn(OrderUpdate) + Send + Sync>) {}
        async fn health_check(&self) -> Result<HealthStatus, ExecutionError> { unimplemented!() }
        fn get_limits(&self) -> ExchangeLimits { unimplemented!() }
        fn validate_order(&self, _s: &Signal) -> Result<(), ExecutionError> { Ok(()) }
        fn convert_signal(&self, _s: &Signal) -> Result<ExchangeOrder, ExecutionError> { unimplemented!() }
    }

    fn make_signal(id: &str) -> Signal {
        Signal {
            id: id.to_string(),
            strategy_id: String::new(),
            symbol: "BTC/USD".into(),
            exchange: "mock".into(),
            action: crate::signal::SignalAction::Buy,
            quantity: 1.0,
            price: Some(100.0),
            confidence: 1.0,
            timestamp: 0,
            metadata: std::collections::HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_sequential_batch_continues_on_failure() {
        let conn = MockConnector;
        let signals = vec![make_signal("0"), make_signal("1"), make_signal("2")];
        let results = conn.execute_batch_orders_sequential(&signals).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].status, ExecutionStatus::Filled);
        assert_eq!(results[1].status, ExecutionStatus::Rejected);
        assert!(results[1].reject_reason.is_some());
        assert_eq!(results[2].status, ExecutionStatus::Filled);
    }

    #[tokio::test]
    async fn test_parallel_batch_returns_all() {
        let conn = MockConnector;
        let signals = vec![make_signal("0"), make_signal("2"), make_signal("4")];
        let results = conn.execute_batch_orders_parallel(&signals).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.status == ExecutionStatus::Filled));
    }

    #[tokio::test]
    async fn test_optimized_batch_respects_batch_size() {
        let conn = MockConnector;
        let signals: Vec<Signal> = (0..5).map(|i| make_signal(&(i * 2).to_string())).collect();
        let results = conn.execute_batch_orders_optimized(&signals, 2).await.unwrap();
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_sequential_batch_empty_input() {
        let conn = MockConnector;
        let results = conn.execute_batch_orders_sequential(&[]).await.unwrap();
        assert!(results.is_empty());
    }
}
