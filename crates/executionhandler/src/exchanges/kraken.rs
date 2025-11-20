use async_trait::async_trait;
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::signal::Signal;

use crate::core::{
    ExchangeConnector, ExchangeAuth, ExchangeWebSocket, NanoOptimized,
    types::*,
};
use crate::optimizations::{
    timestamp::{nano_timestamp, NanoTimer},
    memory_pool::{get_thread_local_order, return_thread_local_order},
    lock_free::{AtomicMetrics, SPSCQueue},
    simd_metrics::VectorizedMetrics,
};

/// Ultra-low latency Kraken exchange connector
pub struct KrakenConnector {
    exchange_name: String,
    config: Option<ExchangeConfig>,
    http_client: Option<Client>,
    auth: Option<KrakenAuth>,
    websocket: Option<KrakenWebSocket>,
    metrics: Arc<AtomicMetrics>,
    vectorized_metrics: Arc<RwLock<VectorizedMetrics>>,
    _order_updates: Arc<SPSCQueue<OrderUpdate>>,
    active_orders: Arc<RwLock<HashMap<String, OrderStatus>>>,
}

impl Default for KrakenConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl KrakenConnector {
    pub fn new() -> Self {
        Self {
            exchange_name: "Kraken".to_string(),
            config: None,
            http_client: None,
            auth: None,
            websocket: None,
            metrics: Arc::new(AtomicMetrics::new()),
            vectorized_metrics: Arc::new(RwLock::new(VectorizedMetrics::new(10000))),
            _order_updates: Arc::new(SPSCQueue::<OrderUpdate>::new()),
            active_orders: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ExchangeConnector for KrakenConnector {
    fn exchange_name(&self) -> &str {
        &self.exchange_name
    }

    async fn initialize(&mut self, config: ExchangeConfig) -> Result<(), ExecutionError> {
        // Validate configuration
        if config.api_key.is_empty() || config.secret_key.is_empty() {
            return Err(ExecutionError::Authentication("Missing API credentials".to_string()));
        }

        // Create HTTP client with optimized settings
        let http_client = Client::builder()
            .pool_max_idle_per_host(config.connection_pool_size)
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| ExecutionError::Connection(e.to_string()))?;

        // Initialize authentication
        let auth = KrakenAuth::new(config.api_key.clone(), config.secret_key.clone());

        // Initialize WebSocket connection
        let websocket_url = config.websocket_url.clone()
            .unwrap_or_else(|| "wss://ws.kraken.com".to_string());
        let websocket = KrakenWebSocket::new(websocket_url);

        self.config = Some(config);
        self.http_client = Some(http_client);
        self.auth = Some(auth);
        self.websocket = Some(websocket);

        // Start WebSocket connection
        if let Some(ref mut ws) = self.websocket {
            ws.connect(vec!["ownTrades".to_string(), "openOrders".to_string()]).await?;
        }

        Ok(())
    }

    async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
        let timer = NanoTimer::start();
        
        // Get pooled order object for zero allocation
        let pooled_order = get_thread_local_order();
        
        // Validate order
        self.validate_order(signal)?;
        
        // Convert signal to exchange format
        let exchange_order = self.convert_signal(signal)?;
        
        // Get timestamp at the last possible moment
        let submit_timestamp = nano_timestamp();
        
        // Execute the order with minimal latency
        let result = self.execute_kraken_order(&exchange_order, submit_timestamp).await;
        
        // Return pooled object
        return_thread_local_order(pooled_order);
        
        // Record metrics
        let latency_ns = timer.elapsed_ns();
        match &result {
            Ok(_) => self.metrics.record_order_success(latency_ns),
            Err(_) => self.metrics.record_order_failure(),
        }
        
        // Add to vectorized metrics
        {
            let mut vm = self.vectorized_metrics.write().await;
            vm.add_latency(latency_ns);
        }
        
        result.map(|mut r| {
            r.latency_ns = latency_ns;
            r
        })
    }

    async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        let batch_timer = NanoTimer::start();
        let mut results = Vec::with_capacity(signals.len());
        
        // For now, execute sequentially to avoid Send issues
        // In production, consider using channels or other async-safe patterns
        for signal in signals {
            let result = self.execute_order(signal).await?;
            results.push(result);
        }

        let batch_latency_ns = batch_timer.elapsed_ns();
        log::debug!("Batch of {} orders completed in {}ns", signals.len(), batch_latency_ns);
        
        Ok(results)
    }

    async fn cancel_order(&self, order_id: &str) -> Result<CancelResult, ExecutionError> {
        let timer = NanoTimer::start();
        
        let client = self.http_client.as_ref()
            .ok_or_else(|| ExecutionError::Connection("HTTP client not initialized".to_string()))?;
        
        let auth = self.auth.as_ref()
            .ok_or_else(|| ExecutionError::Authentication("Authentication not initialized".to_string()))?;
        
        // Prepare cancel request
        let mut params = HashMap::new();
        params.insert("txid".to_string(), order_id.to_string());
        
        let nonce = nano_timestamp() as u64; // Convert to u64 for signature compatibility
        let body = format!("nonce={}&txid={}", nonce, order_id);
        let path = "/0/private/CancelOrder";
        
        // Sign request
        let auth_headers = auth.sign_request("POST", path, &body, nonce).await?;
        
        // Build request
        let request_builder = client
            .post(format!("https://api.kraken.com{}", path))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("API-Key", &auth_headers.headers["API-Key"])
            .header("API-Sign", &auth_headers.signature)
            .body(body);
        
        // Submit cancel request
        let response = request_builder.send().await
            .map_err(|e| ExecutionError::NetworkError(e.to_string()))?;
        
        let response_text = response.text().await
            .map_err(|e| ExecutionError::NetworkError(e.to_string()))?;
        
        // Parse response
        let kraken_response: KrakenCancelResponse = serde_json::from_str(&response_text)
            .map_err(|e| ExecutionError::SerializationError(e.to_string()))?;
        
        if !kraken_response.error.is_empty() {
            return Err(ExecutionError::Exchange(kraken_response.error.join(", ")));
        }
        
        let _cancel_latency = timer.elapsed_ns();
        
        Ok(CancelResult {
            order_id: order_id.to_string(),
            exchange_order_id: Some(order_id.to_string()),
            status: CancelStatus::Cancelled,
            cancelled_at: nano_timestamp(),
        })
    }

    async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError> {
        let active_orders = self.active_orders.read().await;
        let order_ids: Vec<String> = active_orders.keys().cloned().collect();
        drop(active_orders);
        
        let mut results = Vec::new();
        for order_id in order_ids {
            match self.cancel_order(&order_id).await {
                Ok(result) => results.push(result),
                Err(_) => continue, // Skip failed cancellations
            }
        }
        
        Ok(results)
    }

    async fn get_order_status(&self, order_id: &str) -> Result<Option<OrderStatus>, ExecutionError> {
        let active_orders = self.active_orders.read().await;
        Ok(active_orders.get(order_id).cloned())
    }

    fn get_metrics(&self) -> ExecutionMetrics {
        let atomic_snapshot = self.metrics.get_snapshot();
        
        ExecutionMetrics {
            exchange: self.exchange_name.clone(),
            total_orders: atomic_snapshot.total_orders,
            successful_orders: atomic_snapshot.successful_orders,
            failed_orders: atomic_snapshot.failed_orders,
            cancelled_orders: 0, // TODO: Track cancelled orders
            avg_latency_ns: atomic_snapshot.avg_latency_ns,
            min_latency_ns: atomic_snapshot.min_latency_ns,
            max_latency_ns: atomic_snapshot.max_latency_ns,
            p50_latency_ns: 0, // TODO: Calculate from vectorized metrics
            p95_latency_ns: 0,
            p99_latency_ns: 0,
            p999_latency_ns: 0,
            total_volume: 0.0, // TODO: Track volume
            total_fees: 0.0,   // TODO: Track fees
            fill_rate: atomic_snapshot.success_rate,
            error_rate: 1.0 - atomic_snapshot.success_rate,
            orders_per_second: 0.0, // TODO: Calculate rate
            last_updated: nano_timestamp(),
            websocket_connected: self.websocket.as_ref().is_some_and(|ws| ws.is_connected()),
            connection_pool_utilization: 0.0, // TODO: Track pool utilization
            rate_limit_utilization: 0.0,      // TODO: Track rate limiting
        }
    }

    async fn subscribe_to_updates(&self, _callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        // Implementation would set up callback for order updates
        // This is a placeholder for the async callback mechanism
    }

    async fn health_check(&self) -> Result<HealthStatus, ExecutionError> {
        let timer = NanoTimer::start();
        
        // Simple ping to Kraken's server time endpoint
        if let Some(client) = &self.http_client {
            match client.get("https://api.kraken.com/0/public/Time").send().await {
                Ok(_) => {
                    let latency_ns = timer.elapsed_ns();
                    Ok(HealthStatus {
                        exchange: self.exchange_name.clone(),
                        status: HealthState::Healthy,
                        latency_ns,
                        last_check: nano_timestamp(),
                        error_message: None,
                    })
                }
                Err(e) => {
                    Ok(HealthStatus {
                        exchange: self.exchange_name.clone(),
                        status: HealthState::Unhealthy,
                        latency_ns: timer.elapsed_ns(),
                        last_check: nano_timestamp(),
                        error_message: Some(e.to_string()),
                    })
                }
            }
        } else {
            Err(ExecutionError::Connection("HTTP client not initialized".to_string()))
        }
    }

    fn get_limits(&self) -> ExchangeLimits {
        ExchangeLimits {
            max_orders_per_second: 20,
            max_batch_size: 50,
            min_order_size: 0.0001,
            max_order_size: 1_000_000.0,
            tick_size: 0.01,
            supported_order_types: vec![OrderType::Market, OrderType::Limit],
            supported_time_in_force: vec![TimeInForce::GoodTillCancelled, TimeInForce::ImmediateOrCancel],
        }
    }

    fn validate_order(&self, signal: &Signal) -> Result<(), ExecutionError> {
        if signal.quantity <= 0.0 {
            return Err(ExecutionError::Validation("Quantity must be positive".to_string()));
        }
        
        if signal.symbol.is_empty() {
            return Err(ExecutionError::Validation("Symbol is required".to_string()));
        }
        
        // Add more validation rules as needed
        Ok(())
    }

    fn convert_signal(&self, signal: &Signal) -> Result<ExchangeOrder, ExecutionError> {
        use crate::signal::SignalAction;
        
        let (side, order_type, price) = match signal.action {
            SignalAction::Buy => (OrderSide::Buy, OrderType::Market, None),
            SignalAction::Sell => (OrderSide::Sell, OrderType::Market, None),
            SignalAction::BuyLimit => (OrderSide::Buy, OrderType::Limit, signal.price),
            SignalAction::SellLimit => (OrderSide::Sell, OrderType::Limit, signal.price),
            SignalAction::BuyStop => (OrderSide::Buy, OrderType::Stop, signal.price),
            SignalAction::SellStop => (OrderSide::Sell, OrderType::Stop, signal.price),
        };
        
        // Convert symbol format (BTC/USD -> XXBTZUSD)
        let kraken_symbol = self.convert_symbol(&signal.symbol)?;
        
        Ok(ExchangeOrder {
            symbol: kraken_symbol,
            side,
            order_type,
            quantity: signal.quantity,
            price,
            time_in_force: TimeInForce::GoodTillCancelled,
            client_order_id: signal.id.clone(),
            metadata: HashMap::new(),
        })
    }
}

impl KrakenConnector {
    async fn execute_kraken_order(&self, order: &ExchangeOrder, submit_timestamp: u128) -> Result<ExecutionResult, ExecutionError> {
        let client = self.http_client.as_ref()
            .ok_or_else(|| ExecutionError::Connection("HTTP client not initialized".to_string()))?;
        
        let auth = self.auth.as_ref()
            .ok_or_else(|| ExecutionError::Authentication("Authentication not initialized".to_string()))?;
        
        // Build Kraken-specific order request
        let kraken_request = self.build_kraken_order_request(order)?;
        let body = self.serialize_kraken_request(&kraken_request)?;
        let path = "/0/private/AddOrder";
        
        let nonce = nano_timestamp() as u64; // Convert to u64 for signature compatibility
        let auth_headers = auth.sign_request("POST", path, &body, nonce).await?;
        
        // Submit order with minimal latency
        let response = client
            .post(format!("https://api.kraken.com{}", path))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("API-Key", &auth_headers.headers["API-Key"])
            .header("API-Sign", &auth_headers.signature)
            .body(body)
            .send().await
            .map_err(|e| ExecutionError::NetworkError(e.to_string()))?;
        
        let response_text = response.text().await
            .map_err(|e| ExecutionError::NetworkError(e.to_string()))?;
        
        // Parse response
        let kraken_response: KrakenOrderResponse = serde_json::from_str(&response_text)
            .map_err(|e| ExecutionError::SerializationError(e.to_string()))?;
        
        if !kraken_response.error.is_empty() {
            return Err(ExecutionError::Exchange(kraken_response.error.join(", ")));
        }
        
        // Convert to ExecutionResult
        self.convert_kraken_response(kraken_response, order, submit_timestamp)
    }

    fn convert_symbol(&self, symbol: &str) -> Result<String, ExecutionError> {
        let kraken_symbol = match symbol {
            "BTC/USD" => "XXBTZUSD",
            "ETH/USD" => "XETHZUSD",
            "LTC/USD" => "XLTCZUSD",
            "XRP/USD" => "XXRPZUSD",
            _ => return Err(ExecutionError::InvalidSymbol(format!("Unsupported symbol: {}", symbol))),
        };
        Ok(kraken_symbol.to_string())
    }

    fn build_kraken_order_request(&self, order: &ExchangeOrder) -> Result<KrakenOrderRequest, ExecutionError> {
        let order_type = match order.order_type {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            _ => return Err(ExecutionError::Validation("Unsupported order type".to_string())),
        };

        let side = match order.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        Ok(KrakenOrderRequest {
            pair: order.symbol.clone(),
            r#type: side.to_string(),
            ordertype: order_type.to_string(),
            volume: order.quantity.to_string(),
            price: order.price.map(|p| p.to_string()),
            userref: Some(order.client_order_id.clone()),
        })
    }

    fn serialize_kraken_request(&self, request: &KrakenOrderRequest) -> Result<String, ExecutionError> {
        let mut params = vec![
            format!("pair={}", request.pair),
            format!("type={}", request.r#type),
            format!("ordertype={}", request.ordertype),
            format!("volume={}", request.volume),
        ];

        if let Some(ref price) = request.price {
            params.push(format!("price={}", price));
        }

        if let Some(ref userref) = request.userref {
            params.push(format!("userref={}", userref));
        }

        let nonce = nano_timestamp();
        params.insert(0, format!("nonce={}", nonce));

        Ok(params.join("&"))
    }

    fn convert_kraken_response(&self, response: KrakenOrderResponse, order: &ExchangeOrder, submit_timestamp: u128) -> Result<ExecutionResult, ExecutionError> {
        let now = nano_timestamp();
        let order_id = response.result.txid.first()
            .ok_or_else(|| ExecutionError::Exchange("No transaction ID in response".to_string()))?;

        Ok(ExecutionResult {
            order_id: order.client_order_id.clone(),
            exchange_order_id: Some(order_id.clone()),
            exchange: self.exchange_name.clone(),
            status: ExecutionStatus::Submitted,
            filled_quantity: 0.0,
            remaining_quantity: order.quantity,
            avg_fill_price: 0.0,
            total_fees: 0.0,
            fills: Vec::new(),
            reject_reason: None,
            submitted_at: submit_timestamp,
            updated_at: now,
            latency_ns: 0, // Will be set by caller
        })
    }
}

// Kraken-specific data structures
#[derive(Debug, Clone, Serialize, Deserialize)]
struct KrakenOrderRequest {
    pair: String,
    r#type: String,
    ordertype: String,
    volume: String,
    price: Option<String>,
    userref: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KrakenOrderResponse {
    error: Vec<String>,
    result: KrakenOrderResult,
}

#[derive(Debug, Deserialize)]
struct KrakenOrderResult {
    txid: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct KrakenCancelResponse {
    error: Vec<String>,
    _result: Option<serde_json::Value>,
}

// Authentication implementation
struct KrakenAuth {
    api_key: String,
    secret_key: String,
}

impl KrakenAuth {
    fn new(api_key: String, secret_key: String) -> Self {
        Self { api_key, secret_key }
    }
}

#[async_trait]
impl ExchangeAuth for KrakenAuth {
    async fn sign_request(&self, _method: &str, path: &str, body: &str, timestamp: u64) -> Result<AuthHeaders, ExecutionError> {
        let nonce = timestamp.to_string();
        let post_data = format!("nonce={}&{}", nonce, body);
        
        // Create SHA256 hash of nonce + POST data
        let mut hasher = Sha256::new();
        hasher.update(post_data.as_bytes());
        let hash_digest = hasher.finalize();
        
        // Create HMAC-SHA512 signature
        let secret_decoded = base64::engine::general_purpose::STANDARD
            .decode(&self.secret_key)
            .map_err(|e| ExecutionError::Authentication(format!("Invalid secret key: {}", e)))?;
        
        let mut mac = Hmac::<Sha512>::new_from_slice(&secret_decoded)
            .map_err(|e| ExecutionError::Authentication(format!("HMAC error: {}", e)))?;
        
        let message = format!("{}{}", path, std::str::from_utf8(&hash_digest)
            .map_err(|e| ExecutionError::Authentication(format!("UTF8 error: {}", e)))?);
        
        mac.update(message.as_bytes());
        let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        
        let mut headers = HashMap::new();
        headers.insert("API-Key".to_string(), self.api_key.clone());
        
        Ok(AuthHeaders {
            headers,
            signature,
            timestamp: timestamp.into(),
        })
    }

    async fn refresh_auth(&mut self) -> Result<(), ExecutionError> {
        // Kraken doesn't require token refresh
        Ok(())
    }

    async fn validate_credentials(&self) -> Result<bool, ExecutionError> {
        // Would make a test API call to validate credentials
        Ok(!self.api_key.is_empty() && !self.secret_key.is_empty())
    }
}

// WebSocket implementation
struct KrakenWebSocket {
    _url: String,
    connected: bool,
}

impl KrakenWebSocket {
    fn new(url: String) -> Self {
        Self { _url: url, connected: false }
    }

    fn is_connected(&self) -> bool {
        self.connected
    }
}

#[async_trait]
impl ExchangeWebSocket for KrakenWebSocket {
    async fn connect(&mut self, _channels: Vec<String>) -> Result<(), ExecutionError> {
        // WebSocket connection implementation
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), ExecutionError> {
        self.connected = false;
        Ok(())
    }

    async fn subscribe(&mut self, _subscription: WebSocketSubscription) -> Result<(), ExecutionError> {
        // Subscription implementation
        Ok(())
    }

    async fn unsubscribe(&mut self, _subscription: WebSocketSubscription) -> Result<(), ExecutionError> {
        // Unsubscription implementation
        Ok(())
    }

    async fn process_message(&self, _message: &str) -> Result<Vec<OrderUpdate>, ExecutionError> {
        // Message processing implementation
        Ok(Vec::new())
    }
}

// Nanosecond optimization implementation for Kraken
impl NanoOptimized for KrakenConnector {
    fn nano_timestamp() -> u128 {
        crate::optimizations::timestamp::nano_timestamp()
    }

    fn preallocate_buffers(&mut self, capacity: usize) {
        crate::optimizations::memory_pool::preallocate_thread_local(capacity);
    }

    fn get_pooled_order(&self) -> PooledOrder {
        get_thread_local_order()
    }

    fn return_pooled_order(&self, order: PooledOrder) {
        return_thread_local_order(order);
    }

    fn set_cpu_affinity(&self, core_id: usize) -> Result<(), ExecutionError> {
        crate::optimizations::cpu_affinity::set_cpu_affinity(core_id)
    }

    fn simd_calculate_metrics(&self, latencies: &[f64]) -> crate::core::types::MetricsSnapshot {
        crate::optimizations::simd_metrics::simd_calculate_percentiles(latencies)
    }
}
