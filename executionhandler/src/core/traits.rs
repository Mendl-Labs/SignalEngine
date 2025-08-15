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

    /// Cancel a specific order by ID
    async fn cancel_order(&self, order_id: &str) -> Result<CancelResult, ExecutionError>;

    /// Cancel all active orders for this exchange
    async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError>;

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
