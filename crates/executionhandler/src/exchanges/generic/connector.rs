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

    /// Parse a fill status out of an order-status response.
    ///
    /// 2026-08-30: `execute_order` previously always returned
    /// `filled_quantity: 0.0, avg_fill_price: 0.0` regardless of exchange --
    /// the initial order-placement response is only an acknowledgement, not
    /// a fill confirmation, for every exchange configured here. Since
    /// `hostbuilder`'s fill-recording gate only fires when
    /// `filled_quantity > 0.0 && avg_price > 0.0`, this meant no live trade
    /// on ANY exchange -- not just Alpaca -- ever reached `trade_history`.
    /// This follows up the placement call with a real status check.
    ///
    /// Returns `None` when this preset's response format hasn't been
    /// verified against the exchange's real API yet -- `execute_order`
    /// falls back to today's honest "Submitted, unconfirmed" result in that
    /// case. Silently guessing at an unverified field name risks parsing a
    /// still-open order as filled (or vice versa), which would corrupt
    /// `trade_history`/P&L with confidently-wrong data -- worse than
    /// admitting the fill is unconfirmed. Only extend the match arms below
    /// once a preset's real response shape has actually been checked
    /// against its docs (see the Alpaca arm for the verified pattern).
    fn parse_fill_status(&self, response: &Value) -> Option<(ExecutionStatus, f64, f64)> {
        match self.preset {
            // Verified against https://docs.alpaca.markets/reference/getorderbyorderid
            // (2026-08-30): `status`, `filled_qty`, `filled_avg_price` are the
            // real field names; `filled_qty`/`filled_avg_price` are JSON
            // strings (Alpaca quotes all its numeric fields), and
            // `filled_avg_price` is `null` until at least one fill exists.
            ExchangePreset::AlpacaPaper => {
                let status_str = response.get("status").and_then(|s| s.as_str())?;
                let filled_qty: f64 = response.get("filled_qty")
                    .and_then(|q| q.as_str())
                    .and_then(|q| q.parse().ok())
                    .unwrap_or(0.0);
                let avg_price: f64 = response.get("filled_avg_price")
                    .and_then(|p| p.as_str())
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(0.0);
                let status = match status_str {
                    "filled" => ExecutionStatus::Filled,
                    "partially_filled" => ExecutionStatus::PartiallyFilled,
                    "canceled" | "expired" | "done_for_day" => ExecutionStatus::Cancelled,
                    "rejected" => ExecutionStatus::Rejected,
                    _ => ExecutionStatus::Submitted, // new / accepted / pending_new / etc.
                };
                Some((status, filled_qty, avg_price))
            }
            // Not yet verified against each exchange's real order-status
            // response -- see this function's own doc comment for why an
            // unverified guess isn't a safe default here.
            ExchangePreset::Kraken
            | ExchangePreset::Coinbase
            | ExchangePreset::BinanceUS
            | ExchangePreset::Binance
            | ExchangePreset::Bybit
            | ExchangePreset::OKX
            | ExchangePreset::Gemini
            | ExchangePreset::Deribit
            | ExchangePreset::OandaPractice => None,
        }
    }

    /// Follow up an order placement with a real status check so a fast
    /// (near-instant) fill is actually confirmed and recorded, instead of
    /// permanently reporting zero fill quantity. Presets without verified
    /// `parse_fill_status` handling short-circuit to `None` on the first
    /// attempt (`parse_fill_status` always returns `None` for them) --
    /// this loop costs them nothing beyond the one no-op iteration.
    ///
    /// Alpaca equities orders route through a real brokerage and don't
    /// always fill within the same round-trip as placement (unlike a
    /// market order against a liquid crypto pair, which usually already
    /// has); retry a few times with a short backoff before giving up and
    /// leaving the order as unconfirmed-but-submitted -- the trade_updates
    /// WebSocket listener (see `subscribe_to_updates`) is the authoritative
    /// source for a fill that lands after this window closes.
    async fn check_order_fill(&self, order_id: &str) -> Option<(ExecutionStatus, f64, f64)> {
        let attempts = match self.preset {
            ExchangePreset::AlpacaPaper => 3,
            _ => 1,
        };
        let path = format!("{}/{}", self.definition.endpoints.order_status_path, order_id);
        for attempt in 0..attempts {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
            match self.execute_request("GET", &path, HashMap::new()).await {
                Ok(response) => {
                    if let Some(result) = self.parse_fill_status(&response) {
                        if result.0 == ExecutionStatus::Filled || result.0 == ExecutionStatus::PartiallyFilled {
                            return Some(result);
                        }
                        // Not filled yet on this attempt -- keep polling
                        // (Alpaca) or stop (everyone else, single attempt).
                    } else {
                        return None; // unverified preset, no point retrying
                    }
                }
                Err(e) => {
                    warn!("[{}] Order status check failed for {}: {}", self.definition.name, order_id, e);
                    return None;
                }
            }
        }
        None
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

                // Placement acknowledgement alone doesn't confirm a fill for
                // any exchange configured here -- check the order-placement
                // response itself first (some exchanges echo fill state
                // directly on it), then fall back to a real status check.
                // See `check_order_fill`'s doc comment for why an
                // unconfirmed fill honestly reports zero instead of a
                // guessed value.
                let immediate_fill = self.parse_fill_status(&resp)
                    .filter(|(s, _, _)| *s == ExecutionStatus::Filled || *s == ExecutionStatus::PartiallyFilled);
                let (status, filled_quantity, avg_fill_price) = match immediate_fill {
                    Some(result) => result,
                    None => self.check_order_fill(&order_id).await
                        .unwrap_or((ExecutionStatus::Submitted, 0.0, 0.0)),
                };

                Ok(ExecutionResult {
                    order_id: signal.id.clone(),
                    exchange_order_id: Some(order_id),
                    exchange: self.definition.name.clone(),
                    status,
                    filled_quantity,
                    remaining_quantity: (signal.quantity - filled_quantity).max(0.0),
                    avg_fill_price,
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

    async fn subscribe_to_updates(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        // Only Alpaca is wired up so far -- its fills don't reliably land
        // within `check_order_fill`'s short poll window (a real brokerage
        // routing to a real market, not a synchronous match), so this is
        // the authoritative fallback for anything that fills after that
        // window closes. Every other preset either fills fast enough for
        // `check_order_fill` alone (crypto market orders against a liquid
        // pair) or hasn't had its trade-update stream verified yet -- see
        // `parse_fill_status`'s doc comment for the same "don't guess"
        // reasoning applied to this stream's message format.
        if self.preset != ExchangePreset::AlpacaPaper {
            return;
        }
        let Some(config) = self.config.as_ref() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let api_key = config.api_key.clone();
        let secret_key = config.secret_key.clone();
        // Verified against https://docs.alpaca.markets/docs/websocket-streaming
        // (2026-08-30): trade updates are a genuine WebSocket at
        // {rest_host}/stream, NOT Server-Sent Events -- the premise this
        // exchange was blocked from live trading under was factually wrong
        // (see LIVE_UNSUPPORTED_EXCHANGES in BacktestingEngine's
        // deployment.rs). `rest_url` is `https://...alpaca.markets`;
        // derive the wss:// equivalent rather than hardcoding paper vs
        // live, so this keeps working if this connector is ever pointed at
        // a live (non-paper) Alpaca preset.
        let ws_url = format!(
            "{}/stream",
            self.definition.endpoints.rest_url.replacen("https://", "wss://", 1)
        );
        let exchange_name = self.definition.name.clone();

        tokio::spawn(async move {
            loop {
                match tokio_tungstenite::connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        info!("[{}] trade_updates WebSocket connected", exchange_name);
                        use futures_util::{SinkExt, StreamExt};
                        let (mut write, mut read) = ws_stream.split();

                        let auth_msg = serde_json::json!({
                            "action": "auth", "key": api_key, "secret": secret_key,
                        });
                        let listen_msg = serde_json::json!({
                            "action": "listen", "data": {"streams": ["trade_updates"]},
                        });
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(auth_msg.to_string())).await.is_err() {
                            error!("[{}] trade_updates auth send failed, reconnecting", exchange_name);
                            continue;
                        }
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(listen_msg.to_string())).await.is_err() {
                            error!("[{}] trade_updates listen send failed, reconnecting", exchange_name);
                            continue;
                        }

                        while let Some(msg) = read.next().await {
                            let text = match msg {
                                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                                Ok(_) => continue,
                                Err(e) => {
                                    warn!("[{}] trade_updates WebSocket error: {}", exchange_name, e);
                                    break;
                                }
                            };
                            let Ok(parsed) = serde_json::from_str::<Value>(&text) else { continue };
                            if parsed.get("stream").and_then(|s| s.as_str()) != Some("trade_updates") {
                                continue; // auth/listen acknowledgements, etc.
                            }
                            let Some(data) = parsed.get("data") else { continue };
                            let event = data.get("event").and_then(|e| e.as_str()).unwrap_or("");
                            let update_type = match event {
                                "fill" => UpdateType::CompleteFill,
                                "partial_fill" => UpdateType::PartialFill,
                                "canceled" | "expired" => UpdateType::Cancellation,
                                "rejected" => UpdateType::Rejection,
                                _ => continue, // new / pending_new / etc. -- no fill to record
                            };
                            let order_obj = data.get("order");
                            let order_id = order_obj.and_then(|o| o.get("client_order_id"))
                                .and_then(|id| id.as_str()).unwrap_or("").to_string();
                            let exchange_order_id = order_obj.and_then(|o| o.get("id"))
                                .and_then(|id| id.as_str()).unwrap_or("").to_string();
                            // The order object Alpaca echoes back on every
                            // trade_updates event carries its own symbol/side --
                            // this WebSocket push is the only place that
                            // information is available at all (OrderUpdate has
                            // no other source for it), and a caller writing a
                            // trade_history row needs both.
                            let symbol = order_obj.and_then(|o| o.get("symbol"))
                                .and_then(|s| s.as_str()).map(|s| s.to_string());
                            let side = order_obj.and_then(|o| o.get("side"))
                                .and_then(|s| s.as_str()).map(|s| s.to_string());
                            let qty: f64 = data.get("qty").and_then(|q| q.as_str())
                                .and_then(|q| q.parse().ok()).unwrap_or(0.0);
                            let price: f64 = data.get("price").and_then(|p| p.as_str())
                                .and_then(|p| p.parse().ok()).unwrap_or(0.0);
                            let status = match update_type {
                                UpdateType::CompleteFill => ExecutionStatus::Filled,
                                UpdateType::PartialFill => ExecutionStatus::PartiallyFilled,
                                UpdateType::Cancellation => ExecutionStatus::Cancelled,
                                UpdateType::Rejection => ExecutionStatus::Rejected,
                                UpdateType::StatusChange => ExecutionStatus::Submitted,
                            };
                            callback(OrderUpdate {
                                order_id,
                                exchange_order_id,
                                update_type,
                                status,
                                filled_quantity: Some(qty),
                                fill_price: Some(price),
                                timestamp: nano_timestamp(),
                                exchange_timestamp_ns: None,
                                exchange_sequence: None,
                                symbol,
                                side,
                            });
                        }
                        warn!("[{}] trade_updates WebSocket disconnected, reconnecting in 3s", exchange_name);
                    }
                    Err(e) => {
                        error!("[{}] trade_updates connect failed: {}, retrying in 3s", exchange_name, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
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

#[cfg(test)]
mod fill_status_tests {
    use super::*;

    fn alpaca() -> GenericConnector {
        GenericConnector::new(ExchangePreset::AlpacaPaper)
    }

    #[test]
    fn parses_a_real_alpaca_filled_order() {
        // Real shape per https://docs.alpaca.markets/reference/getorderbyorderid
        let resp: Value = serde_json::from_str(r#"{
            "id": "904837e3-3b76-47ec-b432-046db621571b",
            "status": "filled",
            "filled_qty": "1.5",
            "filled_avg_price": "154.03"
        }"#).unwrap();
        let (status, qty, price) = alpaca().parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(qty, 1.5);
        assert_eq!(price, 154.03);
    }

    #[test]
    fn parses_a_real_alpaca_partially_filled_order() {
        let resp: Value = serde_json::from_str(r#"{
            "status": "partially_filled",
            "filled_qty": "0.4",
            "filled_avg_price": "99.5"
        }"#).unwrap();
        let (status, qty, price) = alpaca().parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::PartiallyFilled);
        assert_eq!(qty, 0.4);
        assert_eq!(price, 99.5);
    }

    #[test]
    fn treats_a_still_pending_alpaca_order_as_unfilled_not_missing() {
        // filled_avg_price is null until at least one fill exists -- must
        // not error out or silently report a phantom fill, and must NOT
        // be mistaken for the "unverified preset" None case (that's a real,
        // parseable zero-fill result, not "we don't know").
        let resp: Value = serde_json::from_str(r#"{
            "status": "new",
            "filled_qty": "0",
            "filled_avg_price": null
        }"#).unwrap();
        let (status, qty, price) = alpaca().parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Submitted);
        assert_eq!(qty, 0.0);
        assert_eq!(price, 0.0);
    }

    #[test]
    fn treats_a_rejected_alpaca_order_as_rejected_not_a_fill() {
        let resp: Value = serde_json::from_str(r#"{
            "status": "rejected",
            "filled_qty": "0",
            "filled_avg_price": null
        }"#).unwrap();
        let (status, _, _) = alpaca().parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Rejected);
    }

    #[test]
    fn unverified_presets_never_guess_a_fill() {
        // Kraken (and every other not-yet-verified preset) must return
        // None regardless of response shape -- see parse_fill_status's own
        // doc comment for why an unverified guess is unsafe here.
        let resp: Value = serde_json::from_str(r#"{"status": "closed", "vol_exec": "1.5", "price": "100"}"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Kraken);
        assert!(conn.parse_fill_status(&resp).is_none());
    }
}
