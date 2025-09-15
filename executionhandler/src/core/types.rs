use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

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
    pub submitted_at: u128, // Nanosecond precision
    pub updated_at: u128,   // Nanosecond precision
    pub latency_ns: u64,    // Nanosecond latency
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
    pub timestamp: u128, // Nanosecond precision
    pub trade_id: String,
    pub is_maker: bool,
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
    pub timestamp: u128, // Nanosecond precision
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
