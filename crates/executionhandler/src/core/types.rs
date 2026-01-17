use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Exchange-agnostic execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub order_id: String,
    pub exchange_order_id: Option<String>,
    pub exchange: String,
    pub status: ExecutionStatus,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub avg_fill_price: f64,
    pub total_fees: f64,
    pub fills: Vec<ExecutionFill>,
    pub reject_reason: Option<String>,
    pub submitted_at: u128, // Nanosecond precision (local)
    pub updated_at: u128,   // Nanosecond precision (local)
    pub latency_ns: u64,    // Nanosecond latency
    /// Exchange-provided timestamp for MiFID II compliance (nanoseconds since epoch)
    #[serde(default)]
    pub exchange_timestamp_ns: Option<u128>,
    /// Exchange-provided sequence number
    #[serde(default)]
    pub exchange_sequence: Option<u64>,
}

/// Order execution status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExecutionStatus {
    Pending,
    Submitted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
}

/// Individual fill information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionFill {
    pub fill_id: String,
    pub order_id: String,
    pub exchange_order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub price: f64,
    pub fee: f64,
    pub fee_asset: String,
    pub timestamp: u128, // Nanosecond precision (local)
    pub trade_id: String,
    pub is_maker: bool,
    /// Exchange-provided timestamp for MiFID II compliance (nanoseconds since epoch)
    /// This is the timestamp from the exchange's matching engine, not our local time
    #[serde(default)]
    pub exchange_timestamp_ns: Option<u128>,
    /// Exchange-provided sequence number for ordering
    #[serde(default)]
    pub exchange_sequence: Option<u64>,
}

/// Order side enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Exchange configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub name: String,
    pub api_key: String,
    pub secret_key: String,
    pub passphrase: Option<String>, // For exchanges like Coinbase Pro
    pub sandbox: bool,
    pub connection_pool_size: usize,
    pub timeout_ms: u64,
    pub rate_limit_per_second: u32,
    pub rate_limit_burst: u32,
    pub websocket_url: Option<String>,
    pub rest_api_url: Option<String>,
    pub custom_headers: HashMap<String, String>,
}

/// Exchange-specific error types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionError {
    Connection(String),
    Authentication(String),
    Validation(String),
    Exchange(String),
    Timeout(String),
    RateLimit(String),
    InsufficientFunds(String),
    OrderNotFound(String),
    MarketClosed(String),
    InvalidSymbol(String),
    InvalidParameter(String),
    NetworkError(String),
    SerializationError(String),
    /// Order rejected due to risk controls (kill switch, circuit breaker, etc.)
    Rejected(String),
    Unknown(String),
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionError::Connection(msg) => write!(f, "Connection error: {}", msg),
            ExecutionError::Authentication(msg) => write!(f, "Authentication error: {}", msg),
            ExecutionError::Validation(msg) => write!(f, "Validation error: {}", msg),
            ExecutionError::Exchange(msg) => write!(f, "Exchange error: {}", msg),
            ExecutionError::Timeout(msg) => write!(f, "Timeout error: {}", msg),
            ExecutionError::RateLimit(msg) => write!(f, "Rate limit error: {}", msg),
            ExecutionError::InsufficientFunds(msg) => write!(f, "Insufficient funds: {}", msg),
            ExecutionError::OrderNotFound(msg) => write!(f, "Order not found: {}", msg),
            ExecutionError::MarketClosed(msg) => write!(f, "Market closed: {}", msg),
            ExecutionError::InvalidSymbol(msg) => write!(f, "Invalid symbol: {}", msg),
            ExecutionError::InvalidParameter(msg) => write!(f, "Invalid parameter: {}", msg),
            ExecutionError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            ExecutionError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            ExecutionError::Rejected(msg) => write!(f, "Order rejected: {}", msg),
            ExecutionError::Unknown(msg) => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl std::error::Error for ExecutionError {}

/// Order cancellation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelResult {
    pub order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: CancelStatus,
    pub cancelled_at: u128, // Nanosecond precision
}

/// Cancellation status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CancelStatus {
    Cancelled,
    AlreadyFilled,
    NotFound,
    Failed(String),
}

/// Parameters for editing an existing order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditOrderParams {
    /// Original order ID (txid) to edit
    pub order_id: String,
    /// Trading pair (e.g., "XBTUSD")
    pub pair: String,
    /// New order quantity (optional)
    pub volume: Option<f64>,
    /// New limit price (optional)
    pub price: Option<f64>,
    /// New secondary price for stop-loss-limit, take-profit-limit, etc. (optional)
    pub price2: Option<f64>,
    /// Order flags (e.g., "post" for post-only)
    pub oflags: Option<String>,
    /// Validate only, do not submit (default: false)
    pub validate: bool,
}

impl EditOrderParams {
    /// Create new edit params with just the required fields
    pub fn new(order_id: impl Into<String>, pair: impl Into<String>) -> Self {
        Self {
            order_id: order_id.into(),
            pair: pair.into(),
            volume: None,
            price: None,
            price2: None,
            oflags: None,
            validate: false,
        }
    }
    
    /// Set new volume
    pub fn with_volume(mut self, volume: f64) -> Self {
        self.volume = Some(volume);
        self
    }
    
    /// Set new price
    pub fn with_price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }
    
    /// Set new secondary price
    pub fn with_price2(mut self, price2: f64) -> Self {
        self.price2 = Some(price2);
        self
    }
    
    /// Set order flags
    pub fn with_oflags(mut self, oflags: impl Into<String>) -> Self {
        self.oflags = Some(oflags.into());
        self
    }
    
    /// Set validate only mode
    pub fn validate_only(mut self) -> Self {
        self.validate = true;
        self
    }
}

/// Result of editing an order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditResult {
    /// Original order ID that was edited
    pub original_order_id: String,
    /// New order ID after edit (Kraken creates new order)
    pub new_order_id: Option<String>,
    /// Edit status
    pub status: EditStatus,
    /// Number of orders cancelled (0 or 1)
    pub orders_cancelled: u32,
    /// Updated volume
    pub volume: Option<String>,
    /// Updated price
    pub price: Option<String>,
    /// Updated price2
    pub price2: Option<String>,
    /// Order description
    pub description: Option<String>,
    /// Timestamp of edit in nanoseconds
    pub edited_at: u128,
    /// Latency of edit operation in nanoseconds
    pub latency_ns: u64,
}

/// Edit operation status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EditStatus {
    /// Order successfully edited
    Success,
    /// Edit validated but not submitted
    Validated,
    /// Original order not found
    NotFound,
    /// Edit failed with reason
    Failed(String),
}

/// Real-time order status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderStatus {
    pub order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: ExecutionStatus,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub avg_fill_price: f64,
    pub last_updated: u128, // Nanosecond precision
}

/// Real-time order update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub order_id: String,
    pub exchange_order_id: String,
    pub update_type: UpdateType,
    pub status: ExecutionStatus,
    pub filled_quantity: Option<f64>,
    pub fill_price: Option<f64>,
    pub timestamp: u128, // Nanosecond precision (local)
    /// Exchange-provided timestamp for MiFID II compliance (nanoseconds since epoch)
    #[serde(default)]
    pub exchange_timestamp_ns: Option<u128>,
    /// Exchange-provided sequence number for ordering
    #[serde(default)]
    pub exchange_sequence: Option<u64>,
}

/// Update type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateType {
    StatusChange,
    PartialFill,
    CompleteFill,
    Cancellation,
    Rejection,
}

/// Exchange health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub exchange: String,
    pub status: HealthState,
    pub latency_ns: u64,
    pub last_check: u128,
    pub error_message: Option<String>,
}

/// Health state enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
    Maintenance,
}

/// Exchange limits and capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeLimits {
    pub max_orders_per_second: u32,
    pub max_batch_size: usize,
    pub min_order_size: f64,
    pub max_order_size: f64,
    pub tick_size: f64,
    pub supported_order_types: Vec<OrderType>,
    pub supported_time_in_force: Vec<TimeInForce>,
}

/// Order type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
    Iceberg,
    PostOnly,
}

/// Time in force enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeInForce {
    GoodTillCancelled,
    ImmediateOrCancel,
    FillOrKill,
    GoodTillTime(u128),
}

/// Exchange-specific order format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeOrder {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub time_in_force: TimeInForce,
    pub client_order_id: String,
    pub metadata: HashMap<String, String>,
}

/// Authentication headers for exchange requests
#[derive(Debug, Clone)]
pub struct AuthHeaders {
    pub headers: HashMap<String, String>,
    pub signature: String,
    pub timestamp: u128,
}

/// WebSocket subscription configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketSubscription {
    pub channel: String,
    pub symbol: Option<String>,
    pub depth: Option<u32>,
}

/// Memory pooled order object for zero-allocation hot path
pub struct PooledOrder {
    pub inner: ExchangeOrder,
    pub pool_id: usize,
}

/// Nanosecond-precision metrics snapshot
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub count: u64,
    pub min_ns: u64,
    pub max_ns: u64,
    pub mean_ns: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub calculated_at: u128,
}

/// Enhanced execution metrics with nanosecond precision
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExecutionMetrics {
    pub exchange: String,
    pub total_orders: u64,
    pub successful_orders: u64,
    pub failed_orders: u64,
    pub cancelled_orders: u64,
    
    // Nanosecond precision metrics
    pub avg_latency_ns: u64,
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
    pub p50_latency_ns: u64,
    pub p95_latency_ns: u64,
    pub p99_latency_ns: u64,
    pub p999_latency_ns: u64,
    
    pub total_volume: f64,
    pub total_fees: f64,
    pub fill_rate: f64,
    pub error_rate: f64,
    pub orders_per_second: f64,
    pub last_updated: u128, // Nanosecond precision
    
    // Connection health metrics
    pub websocket_connected: bool,
    pub connection_pool_utilization: f64,
    pub rate_limit_utilization: f64,
}

/// Kraken API credentials for legacy compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KrakenCredentials {
    pub api_key: String,
    pub secret_key: String,
}
