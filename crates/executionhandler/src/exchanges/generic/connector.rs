//! Generic exchange connector implementation.
//!
//! This connector can work with any REST-based cryptocurrency exchange
//! by using exchange-specific configurations.

use async_trait::async_trait;
use log::{info, warn, error, debug};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::signal::Signal;
use crate::risk_controls::{KILL_SWITCH, KillReason};
use crate::core::{
    ExchangeConnector,
    types::*,
    metrics::MetricsCollector,
};
use crate::optimizations::timestamp::{nano_timestamp, NanoTimer};

use super::config::{ExchangePreset, ExchangeDefinition, ContentType};
use super::auth::{AuthStrategy, create_auth_strategy};
use super::symbols::SymbolConverter;

/// Generic exchange connector that works with any configured exchange
pub struct GenericConnector {
    /// Exchange preset identifier
    preset: ExchangePreset,
    /// Full exchange definition
    definition: ExchangeDefinition,
    /// HTTP client
    http_client: Option<Client>,
    /// Authentication strategy
    auth: Option<Box<dyn AuthStrategy>>,
    /// Symbol converter
    symbol_converter: SymbolConverter,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Active orders tracking
    active_orders: Arc<RwLock<HashMap<String, OrderStatus>>>,
    /// Configuration
    config: Option<ExchangeConfig>,
}

impl GenericConnector {
    /// Create a new generic connector for the specified exchange
    pub fn new(preset: ExchangePreset) -> Self {
        let definition = preset.definition();
        let symbol_converter = SymbolConverter::new(definition.symbol_format.clone());
        
        Self {
            preset,
            definition,
            http_client: None,
            auth: None,
            symbol_converter,
            metrics: Arc::new(MetricsCollector::new()),
            active_orders: Arc::new(RwLock::new(HashMap::new())),
            config: None,
        }
    }
    
    /// Create from exchange name string
    pub fn from_name(name: &str) -> Result<Self, ExecutionError> {
        let preset = ExchangePreset::from_name(name)
            .ok_or_else(|| ExecutionError::InvalidParameter(
                format!("Unknown exchange: {}. Supported: kraken, coinbase, binance, binance_us, bybit, okx, gemini, deribit", name)
            ))?;
        Ok(Self::new(preset))
    }
    
    /// Get the exchange preset
    pub fn preset(&self) -> ExchangePreset {
        self.preset
    }
    
    /// Build request body based on content type
    fn build_request_body(&self, params: &HashMap<String, String>) -> String {
        match self.definition.endpoints.content_type {
            ContentType::FormUrlEncoded => {
                params.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("&")
            }
            ContentType::Json => {
                // Exchanges with nested JSON bodies (OANDA) pre-serialize into __raw_body
                if let Some(raw) = params.get("__raw_body") {
                    return raw.clone();
                }
                serde_json::to_string(params).unwrap_or_default()
            }
        }
    }

    /// Substitute path template placeholders (OANDA's `{account_id}` rides in the passphrase credential)
    fn resolve_path(&self, path: &str) -> String {
        if path.contains("{account_id}") {
            let account_id = self.config.as_ref()
                .and_then(|c| c.passphrase.as_deref())
                .unwrap_or("");
            path.replace("{account_id}", account_id)
        } else {
            path.to_string()
        }
    }

    /// Build OANDA v20 order body: nested {"order":{...}}, side encoded as signed integer units
    fn build_oanda_order_params(&self, signal: &Signal) -> Result<HashMap<String, String>, ExecutionError> {
        use crate::signal::SignalAction;

        let instrument = self.symbol_converter.to_exchange_format(&signal.symbol);
        // OANDA units are signed integers: positive = buy, negative = sell
        let mut units = signal.quantity.round() as i64;
        if matches!(signal.action,
            SignalAction::Sell | SignalAction::SellLimit | SignalAction::SellStop)
        {
            units = -units;
        }

        let mut order = serde_json::json!({
            "type": "MARKET",
            "instrument": instrument,
            "units": units.to_string(),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
        });
        if let Some(price) = signal.price {
            order["type"] = "LIMIT".into();
            order["price"] = price.to_string().into();
            order["timeInForce"] = "GTC".into();
        }

        let body = serde_json::json!({ "order": order }).to_string();
        let mut params = HashMap::new();
        params.insert("__raw_body".to_string(), body);
        Ok(params)
    }

    /// Build order parameters from signal
    fn build_order_params(&self, signal: &Signal) -> Result<HashMap<String, String>, ExecutionError> {
        use crate::signal::SignalAction;

        if matches!(self.preset, ExchangePreset::OandaPractice) {
            return self.build_oanda_order_params(signal);
        }

        let params_map = &self.definition.order_params;
        let mut params = HashMap::new();
        
        // Symbol
        let exchange_symbol = self.symbol_converter.to_exchange_format(&signal.symbol);
        params.insert(params_map.symbol_field.clone(), exchange_symbol);
        
        // Side (if not using separate endpoints like Deribit)
        if !params_map.side_field.is_empty() {
            let side = match signal.action {
                SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop => 
                    params_map.side_buy.clone(),
                SignalAction::Sell | SignalAction::SellLimit | SignalAction::SellStop => 
                    params_map.side_sell.clone(),
            };
            params.insert(params_map.side_field.clone(), side);
        }
        
        // Order type
        let order_type = match signal.action {
            SignalAction::Buy | SignalAction::Sell => params_map.type_market.clone(),
            SignalAction::BuyLimit | SignalAction::SellLimit => params_map.type_limit.clone(),
            SignalAction::BuyStop | SignalAction::SellStop => params_map.type_limit.clone(), // Most exchanges use limit for stops
        };
        params.insert(params_map.type_field.clone(), order_type);
        
        // Quantity
        params.insert(params_map.quantity_field.clone(), signal.quantity.to_string());
        
        // Price (for limit orders)
        if let Some(price) = signal.price {
            params.insert(params_map.price_field.clone(), price.to_string());
        }
        
        // Client order ID
        if let Some(ref client_id_field) = params_map.client_id_field {
            params.insert(client_id_field.clone(), signal.id.clone());
        }
        
        // Exchange-specific additions using trading_mode from config
        match self.preset {
            ExchangePreset::Bybit => {
                // Bybit requires category from trading_mode
                params.insert("category".to_string(), self.definition.trading_mode.category.clone());
            }
            ExchangePreset::OKX => {
                // OKX requires trade mode from trading_mode
                params.insert("tdMode".to_string(), self.definition.trading_mode.mode.clone());
            }
            ExchangePreset::Coinbase => {
                // Coinbase requires order configuration
                if signal.price.is_some() {
                    params.insert("order_configuration".to_string(), 
                        format!(r#"{{"limit_limit_gtc":{{"base_size":"{}","limit_price":"{}"}}}}"#,
                            signal.quantity, signal.price.unwrap()));
                } else {
                    params.insert("order_configuration".to_string(),
                        format!(r#"{{"market_market_ioc":{{"base_size":"{}"}}}}"#, signal.quantity));
                }
            }
            _ => {}
        }
        
        Ok(params)
    }
    
    /// Execute the HTTP request to the exchange
    async fn execute_request(
        &self,
        method: &str,
        path: &str,
        params: HashMap<String, String>,
    ) -> Result<Value, ExecutionError> {
        let client = self.http_client.as_ref()
            .ok_or_else(|| ExecutionError::Connection("HTTP client not initialized".to_string()))?;
        
        let auth = self.auth.as_ref()
            .ok_or_else(|| ExecutionError::Authentication("Authentication not initialized".to_string()))?;

        let path = &self.resolve_path(path);

        // Build request body
        let body = self.build_request_body(&params);
        
        // Get timestamp
        let timestamp = nano_timestamp() as u64 / 1_000_000; // Convert to milliseconds
        
        // Sign the request
        let auth_headers = auth.sign(method, path, &body, timestamp).await?;
        
        // Build URL
        let base_url = &self.definition.endpoints.rest_url;
        let mut url = format!("{}{}", base_url, path);
        
        // Add query params from auth if needed
        if !auth_headers.query_params.is_empty() {
            let query_string: String = auth_headers.query_params.iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");
            
            if body.is_empty() {
                url = format!("{}?{}", url, query_string);
            } else {
                url = format!("{}?{}&{}", url, body, query_string);
            }
        }
        
        // Build request
        let content_type = match self.definition.endpoints.content_type {
            ContentType::FormUrlEncoded => "application/x-www-form-urlencoded",
            ContentType::Json => "application/json",
        };
        
        let mut request = match method.to_uppercase().as_str() {
            "GET" => client.get(&url),
            "POST" => client.post(&url).body(body),
            "DELETE" => client.delete(&url),
            _ => return Err(ExecutionError::InvalidParameter(format!("Unsupported HTTP method: {}", method))),
        };
        
        // Add headers
        request = request.header("Content-Type", content_type);
        for (key, value) in &auth_headers.headers {
            request = request.header(key, value);
        }
        
        // Execute request
        let response = request.send().await
            .map_err(|e| ExecutionError::NetworkError(e.to_string()))?;
        
        let status = response.status();
        let response_text = response.text().await
            .map_err(|e| ExecutionError::NetworkError(e.to_string()))?;
        
        debug!("[{}] Response ({}): {}", self.definition.name, status, response_text);
        
        // Parse response
        let response_json: Value = serde_json::from_str(&response_text)
            .map_err(|e| ExecutionError::SerializationError(format!("Failed to parse response: {}. Body: {}", e, response_text)))?;
        
        // Check for errors (exchange-specific)
        self.check_response_error(&response_json)?;
        
        Ok(response_json)
    }
    
    /// Check for exchange-specific error responses
    fn check_response_error(&self, response: &Value) -> Result<(), ExecutionError> {
        match self.preset {
            ExchangePreset::Kraken => {
                if let Some(errors) = response.get("error").and_then(|e| e.as_array()) {
                    if !errors.is_empty() {
                        let error_msgs: Vec<String> = errors.iter()
                            .filter_map(|e| e.as_str().map(|s| s.to_string()))
                            .collect();
                        return Err(ExecutionError::Exchange(error_msgs.join(", ")));
                    }
                }
            }
            ExchangePreset::Binance | ExchangePreset::BinanceUS => {
                if let Some(code) = response.get("code").and_then(|c| c.as_i64()) {
                    if code != 0 {
                        let msg = response.get("msg")
                            .and_then(|m| m.as_str())
                            .unwrap_or("Unknown error");
                        return Err(ExecutionError::Exchange(format!("Error {}: {}", code, msg)));
                    }
                }
            }
            ExchangePreset::Bybit => {
                if let Some(ret_code) = response.get("retCode").and_then(|c| c.as_i64()) {
                    if ret_code != 0 {
                        let msg = response.get("retMsg")
                            .and_then(|m| m.as_str())
                            .unwrap_or("Unknown error");
                        return Err(ExecutionError::Exchange(format!("Error {}: {}", ret_code, msg)));
                    }
                }
            }
            ExchangePreset::OKX => {
                if let Some(code) = response.get("code").and_then(|c| c.as_str()) {
                    if code != "0" {
                        let msg = response.get("msg")
                            .and_then(|m| m.as_str())
                            .unwrap_or("Unknown error");
                        return Err(ExecutionError::Exchange(format!("Error {}: {}", code, msg)));
                    }
                }
            }
            ExchangePreset::Coinbase => {
                if let Some(error) = response.get("error") {
                    let msg = error.as_str().unwrap_or("Unknown error");
                    return Err(ExecutionError::Exchange(msg.to_string()));
                }
            }
            ExchangePreset::Gemini => {
                if let Some(result) = response.get("result").and_then(|r| r.as_str()) {
                    if result == "error" {
                        let msg = response.get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("Unknown error");
                        return Err(ExecutionError::Exchange(msg.to_string()));
                    }
                }
            }
            ExchangePreset::Deribit => {
                if let Some(error) = response.get("error") {
                    let msg = error.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    return Err(ExecutionError::Exchange(msg.to_string()));
                }
            }
            ExchangePreset::AlpacaPaper => {
                // Alpaca returns {"code":..., "message":"..."} on error
                if let Some(msg) = response.get("message").and_then(|m| m.as_str()) {
                    return Err(ExecutionError::Exchange(msg.to_string()));
                }
            }
            ExchangePreset::OandaPractice => {
                // OANDA v20 returns {"errorCode":..., "errorMessage":"..."} on error
                if let Some(msg) = response.get("errorMessage").and_then(|m| m.as_str()) {
                    return Err(ExecutionError::Exchange(msg.to_string()));
                }
            }
        }
        
        Ok(())
    }
    
    /// Parse order ID from response
    fn parse_order_id(&self, response: &Value) -> Option<String> {
        match self.preset {
            ExchangePreset::Kraken => {
                response.get("result")
                    .and_then(|r| r.get("txid"))
                    .and_then(|t| t.as_array())
                    .and_then(|a| a.first())
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            }
            ExchangePreset::Binance | ExchangePreset::BinanceUS => {
                response.get("orderId")
                    .and_then(|id| id.as_i64())
                    .map(|id| id.to_string())
            }
            ExchangePreset::Bybit => {
                response.get("result")
                    .and_then(|r| r.get("orderId"))
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            }
            ExchangePreset::OKX => {
                response.get("data")
                    .and_then(|d| d.as_array())
                    .and_then(|a| a.first())
                    .and_then(|o| o.get("ordId"))
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            }
            ExchangePreset::Coinbase => {
                response.get("order_id")
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            }
            ExchangePreset::Gemini => {
                response.get("order_id")
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            }
            ExchangePreset::Deribit => {
                response.get("result")
                    .and_then(|r| r.get("order"))
                    .and_then(|o| o.get("order_id"))
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            }
            ExchangePreset::AlpacaPaper => {
                response.get("id")
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            }
            ExchangePreset::OandaPractice => {
                // Market FOK fills synchronously (orderFillTransaction); otherwise fall back
                // to the created order's transaction id
                response.get("orderFillTransaction")
                    .and_then(|t| t.get("id"))
                    .or_else(|| response.get("orderCreateTransaction").and_then(|t| t.get("id")))
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            }
        }
    }
}

#[async_trait]
impl ExchangeConnector for GenericConnector {
    fn exchange_name(&self) -> &str {
        &self.definition.name
    }

    async fn initialize(&mut self, config: ExchangeConfig) -> Result<(), ExecutionError> {
        // Validate credentials
        if config.api_key.is_empty() || config.secret_key.is_empty() {
            return Err(ExecutionError::Authentication("Missing API credentials".to_string()));
        }
        
        if self.definition.requires_passphrase && config.passphrase.is_none() {
            return Err(ExecutionError::Authentication(
                format!("{} requires a passphrase", self.definition.name)
            ));
        }
        
        // Create HTTP client
        let http_client = Client::builder()
            .pool_max_idle_per_host(config.connection_pool_size)
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| ExecutionError::Connection(e.to_string()))?;
        
        // Create authentication strategy
        let auth = create_auth_strategy(
            &self.definition.auth_method,
            config.api_key.clone(),
            config.secret_key.clone(),
            config.passphrase.clone(),
        )?;
        
        self.http_client = Some(http_client);
        self.auth = Some(auth);
        self.config = Some(config);
        
        info!("[{}] Connector initialized", self.definition.name);
        
        Ok(())
    }

    async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
        // Check kill switch
        if KILL_SWITCH.is_triggered() {
            let reason = KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual);
            warn!(
                "[{}] Order rejected - kill switch active: signal_id={}, reason={:?}",
                self.definition.name, signal.id, reason
            );
            return Err(ExecutionError::Rejected(format!(
                "Kill switch triggered: {:?}", reason
            )));
        }
        
        let timer = NanoTimer::start();
        let submit_timestamp = nano_timestamp();
        
        debug!(
            "[{}] Executing order: id={}, symbol={}, action={:?}, qty={}",
            self.definition.name, signal.id, signal.symbol, signal.action, signal.quantity
        );
        
        // Build order params
        let params = self.build_order_params(signal)?;
        
        // Determine path - use buy_order_path/sell_order_path if configured (e.g., Deribit)
        let path = if self.definition.endpoints.buy_order_path.is_some() {
            use crate::signal::SignalAction;
            match signal.action {
                SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop => 
                    self.definition.endpoints.buy_order_path.clone().unwrap(),
                SignalAction::Sell | SignalAction::SellLimit | SignalAction::SellStop => 
                    self.definition.endpoints.sell_order_path.clone()
                        .unwrap_or_else(|| self.definition.endpoints.place_order_path.clone()),
            }
        } else {
            self.definition.endpoints.place_order_path.clone()
        };
        
        // Execute order
        let response = self.execute_request(
            &self.definition.endpoints.place_order_method,
            &path,
            params,
        ).await;
        
        let latency_ns = timer.elapsed_ns();
        
        match response {
            Ok(resp) => {
                let order_id = self.parse_order_id(&resp)
                    .unwrap_or_else(|| signal.id.clone());
                
                self.metrics.record_success(latency_ns, signal.quantity * signal.price.unwrap_or(0.0), 0.0);
                
                info!(
                    "[{}] Order submitted: id={}, exchange_id={}, latency_ns={}",
                    self.definition.name, signal.id, order_id, latency_ns
                );
                
                Ok(ExecutionResult {
                    order_id: signal.id.clone(),
                    exchange_order_id: Some(order_id),
                    exchange: self.definition.name.clone(),
                    status: ExecutionStatus::Submitted,
                    filled_quantity: 0.0,
                    remaining_quantity: signal.quantity,
                    avg_fill_price: 0.0,
                    total_fees: 0.0,
                    fills: Vec::new(),
                    reject_reason: None,
                    submitted_at: submit_timestamp,
                    updated_at: nano_timestamp(),
                    latency_ns,
                    exchange_timestamp_ns: None,
                    exchange_sequence: None,
                })
            }
            Err(e) => {
                self.metrics.record_failure();
                error!(
                    "[{}] Order failed: id={}, error={}, latency_ns={}",
                    self.definition.name, signal.id, e, latency_ns
                );
                Err(e)
            }
        }
    }

    async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        use futures::stream::{self, StreamExt};
        
        let max_concurrent = self.definition.rate_limits.max_concurrent_orders as usize;
        
        let results: Vec<Result<ExecutionResult, ExecutionError>> = stream::iter(signals.to_vec())
            .map(|signal| async move {
                self.execute_order(&signal).await
            })
            .buffer_unordered(max_concurrent)
            .collect()
            .await;
        
        Ok(results.into_iter().filter_map(|r| r.ok()).collect())
    }

    async fn cancel_order(&self, order_id: &str) -> Result<CancelResult, ExecutionError> {
        let mut params = HashMap::new();
        
        match self.preset {
            ExchangePreset::Kraken => {
                params.insert("txid".to_string(), order_id.to_string());
            }
            ExchangePreset::Binance | ExchangePreset::BinanceUS => {
                params.insert("orderId".to_string(), order_id.to_string());
            }
            ExchangePreset::Bybit => {
                params.insert("orderId".to_string(), order_id.to_string());
                params.insert("category".to_string(), self.definition.trading_mode.category.clone());
            }
            ExchangePreset::OKX => {
                params.insert("ordId".to_string(), order_id.to_string());
            }
            _ => {
                params.insert("order_id".to_string(), order_id.to_string());
            }
        }
        
        let response = self.execute_request(
            "POST",
            &self.definition.endpoints.cancel_order_path,
            params,
        ).await?;
        
        self.metrics.record_cancellation();
        
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
            if let Ok(result) = self.cancel_order(&order_id).await {
                results.push(result);
            }
        }
        
        Ok(results)
    }

    async fn edit_order(&self, params: EditOrderParams) -> Result<EditResult, ExecutionError> {
        // Most exchanges don't support atomic edit - cancel and replace
        let cancel_result = self.cancel_order(&params.order_id).await;
        
        match cancel_result {
            Ok(_) => {
                Ok(EditResult {
                    original_order_id: params.order_id,
                    new_order_id: None, // Would need to place a new order
                    status: EditStatus::Success,
                    orders_cancelled: 1,
                    volume: params.volume.map(|v| v.to_string()),
                    price: params.price.map(|p| p.to_string()),
                    price2: params.price2.map(|p| p.to_string()),
                    description: None,
                    edited_at: nano_timestamp(),
                    latency_ns: 0,
                })
            }
            Err(e) => {
                Ok(EditResult {
                    original_order_id: params.order_id,
                    new_order_id: None,
                    status: EditStatus::Failed(e.to_string()),
                    orders_cancelled: 0,
                    volume: None,
                    price: None,
                    price2: None,
                    description: None,
                    edited_at: nano_timestamp(),
                    latency_ns: 0,
                })
            }
        }
    }

    async fn get_order_status(&self, order_id: &str) -> Result<Option<OrderStatus>, ExecutionError> {
        let active_orders = self.active_orders.read().await;
        Ok(active_orders.get(order_id).cloned())
    }

    fn get_metrics(&self) -> ExecutionMetrics {
        self.metrics.get_metrics(self.definition.name.clone())
    }

    async fn subscribe_to_updates(&self, _callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        // WebSocket implementation would go here
    }

    async fn health_check(&self) -> Result<HealthStatus, ExecutionError> {
        let timer = NanoTimer::start();
        
        if let Some(client) = &self.http_client {
            let url = format!("{}{}", 
                self.definition.endpoints.rest_url, 
                self.definition.endpoints.health_check_path
            );
            
            match client.get(&url).send().await {
                Ok(_) => {
                    Ok(HealthStatus {
                        exchange: self.definition.name.clone(),
                        status: HealthState::Healthy,
                        latency_ns: timer.elapsed_ns(),
                        last_check: nano_timestamp(),
                        error_message: None,
                    })
                }
                Err(e) => {
                    Ok(HealthStatus {
                        exchange: self.definition.name.clone(),
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
            max_orders_per_second: self.definition.rate_limits.orders_per_second,
            max_batch_size: self.definition.order_limits.max_batch_size,
            min_order_size: self.definition.order_limits.min_order_size,
            max_order_size: self.definition.order_limits.max_order_size,
            tick_size: self.definition.order_limits.default_tick_size,
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
        
        let exchange_symbol = self.symbol_converter.to_exchange_format(&signal.symbol);
        
        Ok(ExchangeOrder {
            symbol: exchange_symbol,
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
