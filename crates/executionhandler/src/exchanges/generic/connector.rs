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

/// Bybit v5 WebSocket private-stream auth signature. Verified 2026-08-31
/// against https://bybit-exchange.github.io/docs/v5/ws/connect:
/// HMAC-SHA256(secret, "GET/realtime{expires}"), hex-encoded. `expires`
/// must be a future millisecond timestamp. Pulled out as a pure function
/// (distinct from `auth.rs`'s REST-signing `HmacSha256Auth`, which uses a
/// completely different message for Bybit's REST endpoints) so it's
/// unit-testable without a live WebSocket connection.
fn bybit_ws_signature(secret_key: &str, expires: i64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let sign_payload = format!("GET/realtime{}", expires);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(sign_payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// OKX v5 WebSocket login signature. Verified 2026-08-31 against
/// https://www.okx.com/docs-v5/en/#overview-websocket-login:
/// HMAC-SHA256(secret, "{timestamp}GET/users/self/verify"), base64-
/// encoded, where `timestamp` is plain Unix EPOCH SECONDS -- NOT the
/// ISO-8601-milliseconds format OKX's REST API requires (see
/// `TimestampFormat::Iso8601Millis` in `auth.rs`). The WS login and the
/// REST signing scheme genuinely use different timestamp conventions
/// despite both being OKX HMAC-SHA256.
fn okx_ws_signature(secret_key: &str, timestamp_secs: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use base64::Engine;
    let sign_payload = format!("{}GET/users/self/verify", timestamp_secs);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(sign_payload.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

/// Gemini's WS handshake auth payload+signature -- verified 2026-08-31
/// against https://docs.gemini.com/websocket/authentication: the same
/// base64-JSON-payload (`{"request":...,"nonce":...}`) + HMAC-SHA384
/// scheme as this crate's REST `GeminiAuth` (auth.rs), computed as a
/// standalone pure function since `GeminiAuth`'s async trait object
/// can't be moved into `subscribe_to_updates`'s `'static` spawned
/// reconnect loop, and a fresh nonce is required on every reconnect
/// attempt anyway (Gemini rejects a replayed nonce).
fn gemini_ws_auth_headers(secret_key: &str, request_path: &str, nonce: u64) -> (String, String) {
    use hmac::{Hmac, Mac};
    use sha2::Sha384;
    use base64::Engine;
    let payload = serde_json::json!({"request": request_path, "nonce": nonce});
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(payload.to_string().as_bytes());
    let mut mac = Hmac::<Sha384>::new_from_slice(secret_key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(payload_b64.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    (payload_b64, signature)
}

/// Builds a CDP JWT for Coinbase's WebSocket "user" channel -- verified
/// 2026-08-31 against
/// https://docs.cdp.coinbase.com/get-started/authentication/jwt-authentication:
/// header `{alg, kid, nonce, typ}`, claims `{iss, sub, nbf, exp, aud}`
/// (the `uri`/`uris` claim used for REST requests is omitted entirely
/// for WebSocket auth per that doc). ES256 (ECDSA P-256 + SHA-256), raw
/// r||s signature bytes (not DER), base64url without padding for every
/// segment -- standard JWS compact serialization.
///
/// `secret_key_pem` must be a PKCS8 PEM-encoded EC private key (the
/// "-----BEGIN EC PRIVATE KEY-----..." string CDP issues for ES256
/// keys) -- returns `None` if it can't be parsed as one, rather than
/// panicking or silently producing an unsigned/garbage token.
fn coinbase_cdp_jwt(api_key: &str, secret_key_pem: &str) -> Option<String> {
    use p256::ecdsa::{SigningKey, Signature};
    use p256::ecdsa::signature::Signer;
    use p256::pkcs8::DecodePrivateKey;
    use base64::Engine;

    let signing_key = SigningKey::from_pkcs8_pem(secret_key_pem).ok()?;

    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok()?.as_secs();
    let nonce = format!("{:016x}{:016x}", now, nano_timestamp());
    let header = serde_json::json!({"alg": "ES256", "kid": api_key, "nonce": nonce, "typ": "JWT"});
    let claims = serde_json::json!({
        "iss": "cdp", "sub": api_key, "nbf": now, "exp": now + 120, "aud": ["cdp_service"],
    });

    let b64url = |v: &Value| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string().as_bytes());
    let signing_input = format!("{}.{}", b64url(&header), b64url(&claims));
    let signature: Signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.to_bytes());

    Some(format!("{}.{}", signing_input, sig_b64))
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

    /// Build a GET query string from `params`. Always `k=v&k=v` form,
    /// regardless of this exchange's `content_type` -- a GET request has
    /// no body in practice, so business params always ride the query
    /// string even for exchanges whose POST bodies are JSON (Bybit, OKX,
    /// Deribit). Kept separate from `build_request_body` so POST behavior
    /// (which does depend on `content_type`) is untouched.
    fn build_query_string(&self, params: &HashMap<String, String>) -> String {
        params.iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&")
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
            ExchangePreset::Binance | ExchangePreset::BinanceUS => {
                // Without this, Binance's default response type (ACK) omits
                // status/executedQty/cummulativeQuoteQty entirely, so a
                // fast market fill can only ever be confirmed via the
                // separate check_order_fill follow-up GET -- FULL makes
                // the placement response itself parseable by
                // parse_fill_status, per
                // https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints#new-order-trade
                params.insert("newOrderRespType".to_string(), "FULL".to_string());
            }
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

        // Timestamp must exist before body-building for Kraken: its
        // nonce (injected below) has to be the exact same value used to
        // compute the signature, not independently re-derived inside
        // auth.sign.
        let timestamp = nano_timestamp() as u64 / 1_000_000; // Convert to milliseconds

        // Kraken requires `nonce` as an actual request parameter, not
        // just an input to the signature -- docs.kraken.com/api/docs/rest-api/add-order.
        // This was previously missing entirely, so every private Kraken
        // call was rejected server-side regardless of signature
        // correctness.
        let mut params = params;
        if matches!(self.preset, ExchangePreset::Kraken) {
            params.entry("nonce".to_string()).or_insert_with(|| timestamp.to_string());
        }

        let is_get = method.eq_ignore_ascii_case("GET");
        // GET requests have no body in practice -- business params always
        // ride the query string as `k=v&k=v`, regardless of this
        // exchange's POST content_type (Bybit/OKX/Deribit are JSON on
        // POST but still take GET params as a plain query string).
        let body = if is_get {
            self.build_query_string(&params)
        } else {
            self.build_request_body(&params)
        };

        // Sign the request
        let auth_headers = auth.sign(method, path, &body, timestamp).await?;

        // Build URL
        let base_url = &self.definition.endpoints.rest_url;
        let mut url = format!("{}{}", base_url, path);

        if is_get {
            // Business params (body) and any auth-added query params
            // (timestamp/signature for Query-location schemes) both
            // belong in the URL for GET. Previously `body` was silently
            // dropped here, so any GET call needing business params
            // (Binance/Bybit/OKX/Deribit order-status checks, and
            // Deribit's order-placement itself, which is also GET) sent
            // them nowhere.
            let mut query_parts = Vec::new();
            if !body.is_empty() {
                query_parts.push(body.clone());
            }
            if !auth_headers.query_params.is_empty() {
                let auth_query: String = auth_headers.query_params.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("&");
                query_parts.push(auth_query);
            }
            if !query_parts.is_empty() {
                url = format!("{}?{}", url, query_parts.join("&"));
            }
        } else if !auth_headers.query_params.is_empty() {
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

        // Gemini signs the full payload into a header and requires an
        // empty wire body (Content-Length: 0) -- sending `body` again
        // would be a protocol violation, not just redundant.
        let wire_body = if auth_headers.force_empty_body { String::new() } else { body };

        let mut request = match method.to_uppercase().as_str() {
            "GET" => client.get(&url),
            "POST" => client.post(&url).body(wire_body),
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
    /// Returns `None` when this preset's response can't be confidently
    /// parsed as a confirmed fill -- `execute_order` falls back to today's
    /// honest "Submitted, unconfirmed" result in that case. Silently
    /// guessing at an unverified field name risks parsing a still-open
    /// order as filled (or vice versa), which would corrupt
    /// `trade_history`/P&L with confidently-wrong data -- worse than
    /// admitting the fill is unconfirmed. Each arm below cites the doc it
    /// was verified against (2026-08-31 pass, live-fetched, not recalled
    /// from memory) -- see the Alpaca arm for the original pattern.
    fn parse_fill_status(&self, response: &Value) -> Option<(ExecutionStatus, f64, f64)> {
        /// Reads a numeric field regardless of whether this exchange
        /// encoded it as a JSON number or a quoted string -- most REST
        /// exchanges here stringify numerics; Deribit's JSON-RPC shape
        /// uses native numbers.
        fn num_field(v: &Value) -> Option<f64> {
            v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        }

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
            // Verified against https://docs.kraken.com/api/docs/rest-api/get-order-info
            // (2026-08-31): QueryOrders responds with `result` keyed by
            // txid, e.g. `{"result":{"<txid>":{"status":...,"vol_exec":...,"price":...}}}`.
            // Since this connector only ever queries one txid at a time,
            // take the single entry rather than re-deriving the key (the
            // response nests it as a map key, not a field we control).
            // `price` here is the average price of executed trades (per
            // Kraken's own field description), not the limit price --
            // that's `descr.price`, deliberately not read here.
            ExchangePreset::Kraken => {
                let order = response.get("result")?.as_object()?.values().next()?;
                let status_str = order.get("status").and_then(|s| s.as_str())?;
                let filled_qty = order.get("vol_exec").and_then(num_field).unwrap_or(0.0);
                let avg_price = order.get("price").and_then(num_field).unwrap_or(0.0);
                let status = match status_str {
                    "closed" if filled_qty > 0.0 => ExecutionStatus::Filled,
                    "closed" => ExecutionStatus::Cancelled, // closed with zero fill = expired/cancelled
                    "canceled" | "expired" => ExecutionStatus::Cancelled,
                    _ if filled_qty > 0.0 => ExecutionStatus::PartiallyFilled, // "open" with partial exec
                    _ => ExecutionStatus::Submitted, // "open" / "pending"
                };
                Some((status, filled_qty, avg_price))
            }
            // Verified against https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api/orders/get-order
            // (2026-08-31): `{"order":{"status":"OPEN"|"FILLED"|"CANCELLED"|"EXPIRED"|"FAILED",
            // "filled_size":"...","average_filled_price":"..."}}`, both numeric fields quoted strings.
            ExchangePreset::Coinbase => {
                let order = response.get("order")?;
                let status_str = order.get("status").and_then(|s| s.as_str())?;
                let filled_qty = order.get("filled_size").and_then(num_field).unwrap_or(0.0);
                let avg_price = order.get("average_filled_price").and_then(num_field).unwrap_or(0.0);
                let status = match status_str {
                    "FILLED" => ExecutionStatus::Filled,
                    "CANCELLED" | "EXPIRED" => ExecutionStatus::Cancelled,
                    "FAILED" => ExecutionStatus::Rejected,
                    _ if filled_qty > 0.0 => ExecutionStatus::PartiallyFilled, // OPEN with partial fill
                    _ => ExecutionStatus::Submitted, // OPEN / PENDING
                };
                Some((status, filled_qty, avg_price))
            }
            // Verified against https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints#query-order-user_data
            // (2026-08-31): `status`, `executedQty` (base asset filled),
            // `cummulativeQuoteQty` (quote asset spent) -- there is no
            // direct average-price field, so avg = quote/base when
            // executedQty > 0. Same shape whether this response came from
            // the follow-up status GET or an immediate `newOrderRespType=FULL`
            // placement ack (see `build_order_params`).
            ExchangePreset::Binance | ExchangePreset::BinanceUS => {
                let status_str = response.get("status").and_then(|s| s.as_str())?;
                let filled_qty = response.get("executedQty").and_then(num_field).unwrap_or(0.0);
                let quote_qty = response.get("cummulativeQuoteQty").and_then(num_field).unwrap_or(0.0);
                let avg_price = if filled_qty > 0.0 { quote_qty / filled_qty } else { 0.0 };
                let status = match status_str {
                    "FILLED" => ExecutionStatus::Filled,
                    "PARTIALLY_FILLED" => ExecutionStatus::PartiallyFilled,
                    "CANCELED" | "EXPIRED" | "PENDING_CANCEL" => ExecutionStatus::Cancelled,
                    "REJECTED" => ExecutionStatus::Rejected,
                    _ => ExecutionStatus::Submitted, // NEW
                };
                Some((status, filled_qty, avg_price))
            }
            // Verified against https://bybit-exchange.github.io/docs/v5/order/order-list
            // (2026-08-31): `{"result":{"list":[{"orderStatus":...,"cumExecQty":"...","avgPrice":"..."}]}}`.
            ExchangePreset::Bybit => {
                let order = response.get("result")?.get("list")?.as_array()?.first()?;
                let status_str = order.get("orderStatus").and_then(|s| s.as_str())?;
                let filled_qty = order.get("cumExecQty").and_then(num_field).unwrap_or(0.0);
                let avg_price = order.get("avgPrice").and_then(num_field).unwrap_or(0.0);
                let status = match status_str {
                    "Filled" => ExecutionStatus::Filled,
                    "PartiallyFilled" => ExecutionStatus::PartiallyFilled,
                    "Cancelled" | "Deactivated" => ExecutionStatus::Cancelled,
                    "Rejected" => ExecutionStatus::Rejected,
                    _ => ExecutionStatus::Submitted, // New / Created / PartiallyFilledCanceled edge case
                };
                Some((status, filled_qty, avg_price))
            }
            // Verified against https://www.okx.com/docs-v5/en/#order-book-trading-trade-get-order-details
            // (2026-08-31): `{"data":[{"state":...,"accFillSz":"...","avgPx":"..."}]}`.
            ExchangePreset::OKX => {
                let order = response.get("data")?.as_array()?.first()?;
                let status_str = order.get("state").and_then(|s| s.as_str())?;
                let filled_qty = order.get("accFillSz").and_then(num_field).unwrap_or(0.0);
                let avg_price = order.get("avgPx").and_then(num_field).unwrap_or(0.0);
                let status = match status_str {
                    "filled" => ExecutionStatus::Filled,
                    "partially_filled" => ExecutionStatus::PartiallyFilled,
                    "canceled" => ExecutionStatus::Cancelled,
                    _ => ExecutionStatus::Submitted, // live / partially_canceled edge case, no reject state here
                };
                Some((status, filled_qty, avg_price))
            }
            // Verified against https://docs.gemini.com/rest/orders (2026-08-31):
            // no single `status` enum -- derived from `is_live`,
            // `is_cancelled`, `executed_amount`, `remaining_amount`.
            // Same shape for both the order-creation response
            // (`/v1/order/new`) and the status response (`/v1/order/status`).
            ExchangePreset::Gemini => {
                let is_cancelled = response.get("is_cancelled").and_then(|v| v.as_bool()).unwrap_or(false);
                let is_live = response.get("is_live").and_then(|v| v.as_bool()).unwrap_or(false);
                let filled_qty = response.get("executed_amount").and_then(num_field).unwrap_or(0.0);
                let avg_price = response.get("avg_execution_price").and_then(num_field).unwrap_or(0.0);
                let remaining = response.get("remaining_amount").and_then(num_field).unwrap_or(f64::NAN);
                let status = if is_cancelled {
                    ExecutionStatus::Cancelled
                } else if filled_qty > 0.0 && remaining == 0.0 {
                    ExecutionStatus::Filled
                } else if filled_qty > 0.0 {
                    ExecutionStatus::PartiallyFilled
                } else if is_live {
                    ExecutionStatus::Submitted
                } else {
                    ExecutionStatus::Rejected
                };
                Some((status, filled_qty, avg_price))
            }
            // Verified against https://docs.deribit.com/#private-get_order_state
            // (2026-08-31). Two response shapes share this parser:
            // - order placement (`/private/buy`, `/private/sell`):
            //   `{"result":{"order":{"order_state":...,"filled_amount":...,"average_price":...}}}`
            // - status check (`get_order_state`):
            //   `{"result":{"order_state":...,"filled_amount":...,"average_price":...}}` (no "order" nesting)
            // `filled_amount`/`average_price` are native JSON numbers here,
            // not strings (Deribit's JSON-RPC convention, unlike the
            // REST-conventional exchanges above).
            ExchangePreset::Deribit => {
                let result = response.get("result")?;
                let order = result.get("order").unwrap_or(result);
                let status_str = order.get("order_state").and_then(|s| s.as_str())?;
                let filled_qty = order.get("filled_amount").and_then(num_field).unwrap_or(0.0);
                let avg_price = order.get("average_price").and_then(num_field).unwrap_or(0.0);
                let status = match status_str {
                    "filled" => ExecutionStatus::Filled,
                    "cancelled" => ExecutionStatus::Cancelled,
                    "rejected" => ExecutionStatus::Rejected,
                    _ if filled_qty > 0.0 => ExecutionStatus::PartiallyFilled, // "open" with partial fill
                    _ => ExecutionStatus::Submitted, // "open" / "untriggered"
                };
                Some((status, filled_qty, avg_price))
            }
            // Verified against https://developer.oanda.com/rest-live-v20/order-df/#OrderFillTransaction
            // (2026-08-31): a synchronous market-order fill is reported
            // directly on the ORDER-CREATE response as
            // `{"orderFillTransaction":{"units":"...","price":"..."}}`
            // (units is a signed string -- negative for a sell). Only
            // this shape is handled: the follow-up status-GET's plain
            // `{"order":{"state":...}}` shape doesn't reliably carry fill
            // price/quantity fields this codebase has verified, so it's
            // deliberately left unconfirmed (`None`) rather than guessed --
            // in practice this rarely matters, since OANDA market orders
            // (the only order type this connector places, see
            // `build_oanda_order_params`) fill synchronously and are
            // already caught by this arm on the placement response itself.
            ExchangePreset::OandaPractice => {
                let fill = response.get("orderFillTransaction")?;
                let filled_qty = fill.get("units").and_then(num_field).unwrap_or(0.0).abs();
                let avg_price = fill.get("price").and_then(num_field).unwrap_or(0.0);
                if filled_qty > 0.0 && avg_price > 0.0 {
                    Some((ExecutionStatus::Filled, filled_qty, avg_price))
                } else {
                    None
                }
            }
        }
    }

    /// Builds the (method, path, params) for this preset's order-status
    /// check. Each exchange's real status endpoint has a genuinely
    /// different shape -- some take the order id as a path segment
    /// (Coinbase, OANDA), some as a query param alongside other required
    /// fields (Binance needs `symbol`, Bybit needs `category`, OKX needs
    /// `instId`), and some as a POST body param (Kraken, Gemini). Mirrors
    /// the per-exchange param-naming already established in
    /// `cancel_order` for consistency.
    fn build_status_check_request(&self, order_id: &str, symbol: &str) -> (&'static str, String, HashMap<String, String>) {
        let base_path = self.definition.endpoints.order_status_path.clone();
        match self.preset {
            ExchangePreset::Kraken => {
                let mut params = HashMap::new();
                params.insert("txid".to_string(), order_id.to_string());
                ("POST", base_path, params)
            }
            ExchangePreset::Coinbase | ExchangePreset::AlpacaPaper | ExchangePreset::OandaPractice => {
                ("GET", format!("{}/{}", base_path, order_id), HashMap::new())
            }
            ExchangePreset::Binance | ExchangePreset::BinanceUS => {
                let mut params = HashMap::new();
                params.insert("symbol".to_string(), symbol.to_string());
                params.insert("orderId".to_string(), order_id.to_string());
                ("GET", base_path, params)
            }
            ExchangePreset::Bybit => {
                let mut params = HashMap::new();
                params.insert("category".to_string(), self.definition.trading_mode.category.clone());
                params.insert("orderId".to_string(), order_id.to_string());
                ("GET", base_path, params)
            }
            ExchangePreset::OKX => {
                let mut params = HashMap::new();
                params.insert("instId".to_string(), symbol.to_string());
                params.insert("ordId".to_string(), order_id.to_string());
                ("GET", base_path, params)
            }
            ExchangePreset::Gemini => {
                let mut params = HashMap::new();
                params.insert("order_id".to_string(), order_id.to_string());
                ("POST", base_path, params)
            }
            ExchangePreset::Deribit => {
                let mut params = HashMap::new();
                params.insert("order_id".to_string(), order_id.to_string());
                ("GET", base_path, params)
            }
        }
    }

    /// Follow up an order placement with a real status check so a fast
    /// (near-instant) fill is actually confirmed and recorded, instead of
    /// permanently reporting zero fill quantity. `symbol` is the
    /// exchange-formatted instrument (needed by Binance/OKX's status
    /// endpoints, unused by presets whose status check is order-id-only).
    ///
    /// Alpaca equities orders route through a real brokerage and don't
    /// always fill within the same round-trip as placement (unlike a
    /// market order against a liquid crypto pair, which usually already
    /// has); retry a few times with a short backoff before giving up and
    /// leaving the order as unconfirmed-but-submitted -- the trade_updates
    /// WebSocket listener (see `subscribe_to_updates`) is the authoritative
    /// source for a fill that lands after this window closes.
    async fn check_order_fill(&self, order_id: &str, symbol: &str) -> Option<(ExecutionStatus, f64, f64)> {
        let attempts = match self.preset {
            ExchangePreset::AlpacaPaper => 3,
            _ => 1,
        };
        let (method, path, params) = self.build_status_check_request(order_id, symbol);
        for attempt in 0..attempts {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
            match self.execute_request(method, &path, params.clone()).await {
                Ok(response) => {
                    if let Some(result) = self.parse_fill_status(&response) {
                        if result.0 == ExecutionStatus::Filled || result.0 == ExecutionStatus::PartiallyFilled {
                            return Some(result);
                        }
                        // Not filled yet on this attempt -- keep polling
                        // (Alpaca) or stop (everyone else, single attempt).
                    } else {
                        return None; // unparseable/unverified response shape, no point retrying
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

    /// Alpaca trade_updates WebSocket -- verified against
    /// https://docs.alpaca.markets/docs/websocket-streaming (2026-08-30).
    /// Alpaca equities orders route through a real brokerage and don't
    /// always fill within `check_order_fill`'s short poll window; this is
    /// the authoritative fallback for a fill that lands after that window
    /// closes.
    async fn spawn_alpaca_trade_updates(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        let Some(config) = self.config.as_ref() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let api_key = config.api_key.clone();
        let secret_key = config.secret_key.clone();
        // rest_url is `https://...alpaca.markets`; derive the wss://
        // equivalent rather than hardcoding paper vs live, so this keeps
        // working if this connector is ever pointed at a live (non-paper)
        // Alpaca preset.
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

    /// Binance/Binance US user data stream -- verified against
    /// https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream
    /// (2026-08-31).
    async fn spawn_binance_user_data_stream(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        let Some(config) = self.config.as_ref() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let Some(http_client) = self.http_client.clone() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let api_key = config.api_key.clone();
        let rest_url = self.definition.endpoints.rest_url.clone();
        let ws_base = self.definition.endpoints.websocket_url.clone();
        let exchange_name = self.definition.name.clone();

        tokio::spawn(async move {
            loop {
                // POST /api/v3/userDataStream needs only the X-MBX-APIKEY
                // header, not a signed query string -- a USER_STREAM
                // endpoint category, distinct from the SIGNED endpoints
                // every other Binance call in this crate uses, so this
                // bypasses execute_request/auth entirely rather than
                // fighting its signing path for an endpoint that doesn't
                // want one.
                let listen_key = match http_client
                    .post(format!("{}/api/v3/userDataStream", rest_url))
                    .header("X-MBX-APIKEY", &api_key)
                    .send()
                    .await
                {
                    Ok(resp) => match resp.json::<Value>().await {
                        Ok(body) => match body.get("listenKey").and_then(|k| k.as_str()) {
                            Some(k) => k.to_string(),
                            None => {
                                error!("[{}] userDataStream response missing listenKey: {}", exchange_name, body);
                                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                                continue;
                            }
                        },
                        Err(e) => {
                            error!("[{}] Failed to parse userDataStream response: {}", exchange_name, e);
                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                            continue;
                        }
                    },
                    Err(e) => {
                        error!("[{}] Failed to obtain listenKey: {}, retrying in 3s", exchange_name, e);
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                        continue;
                    }
                };

                let ws_url = format!("{}/{}", ws_base, listen_key);
                match tokio_tungstenite::connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        info!("[{}] user data stream connected", exchange_name);
                        use futures_util::StreamExt;
                        let (write, mut read) = ws_stream.split();
                        drop(write); // this stream is receive-only once connected

                        // listenKey closes after 60 minutes without a
                        // keepalive -- Binance recommends renewing every 30.
                        let keepalive_client = http_client.clone();
                        let keepalive_rest_url = rest_url.clone();
                        let keepalive_api_key = api_key.clone();
                        let keepalive_listen_key = listen_key.clone();
                        let keepalive_exchange_name = exchange_name.clone();
                        let keepalive_handle = tokio::spawn(async move {
                            loop {
                                tokio::time::sleep(std::time::Duration::from_secs(30 * 60)).await;
                                if let Err(e) = keepalive_client
                                    .put(format!("{}/api/v3/userDataStream?listenKey={}", keepalive_rest_url, keepalive_listen_key))
                                    .header("X-MBX-APIKEY", &keepalive_api_key)
                                    .send()
                                    .await
                                {
                                    warn!("[{}] listenKey keepalive failed: {}", keepalive_exchange_name, e);
                                }
                            }
                        });

                        while let Some(msg) = read.next().await {
                            let text = match msg {
                                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                                Ok(_) => continue,
                                Err(e) => {
                                    warn!("[{}] user data stream error: {}", exchange_name, e);
                                    break;
                                }
                            };
                            let Ok(parsed) = serde_json::from_str::<Value>(&text) else { continue };
                            if parsed.get("e").and_then(|e| e.as_str()) != Some("executionReport") {
                                continue; // outboundAccountPosition, balanceUpdate, etc.
                            }
                            let order_status = parsed.get("X").and_then(|s| s.as_str()).unwrap_or("");
                            let update_type = match order_status {
                                "FILLED" => UpdateType::CompleteFill,
                                "PARTIALLY_FILLED" => UpdateType::PartialFill,
                                "CANCELED" | "EXPIRED" | "PENDING_CANCEL" => UpdateType::Cancellation,
                                "REJECTED" => UpdateType::Rejection,
                                _ => continue, // NEW, etc. -- no fill to record
                            };
                            let status = match update_type {
                                UpdateType::CompleteFill => ExecutionStatus::Filled,
                                UpdateType::PartialFill => ExecutionStatus::PartiallyFilled,
                                UpdateType::Cancellation => ExecutionStatus::Cancelled,
                                UpdateType::Rejection => ExecutionStatus::Rejected,
                                UpdateType::StatusChange => ExecutionStatus::Submitted,
                            };
                            // "c" is the client order ID -- build_order_params
                            // sets this to signal.id, matching every other
                            // exchange's OrderUpdate.order_id convention here.
                            let order_id = parsed.get("c").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let exchange_order_id = parsed.get("i").map(|v| v.to_string()).unwrap_or_default();
                            let symbol = parsed.get("s").and_then(|v| v.as_str()).map(|s| s.to_string());
                            let side = parsed.get("S").and_then(|v| v.as_str()).map(|s| s.to_string());
                            // z/Z are cumulative for the whole order, not this
                            // event alone -- matches the REST executedQty/
                            // cummulativeQuoteQty convention parse_fill_status
                            // already uses for consistency.
                            let cum_qty: f64 = parsed.get("z").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let cum_quote: f64 = parsed.get("Z").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let avg_price = if cum_qty > 0.0 { cum_quote / cum_qty } else { 0.0 };

                            callback(OrderUpdate {
                                order_id,
                                exchange_order_id,
                                update_type,
                                status,
                                filled_quantity: Some(cum_qty),
                                fill_price: Some(avg_price),
                                timestamp: nano_timestamp(),
                                exchange_timestamp_ns: None,
                                exchange_sequence: None,
                                symbol,
                                side,
                            });
                        }
                        keepalive_handle.abort();
                        warn!("[{}] user data stream disconnected, reconnecting in 3s", exchange_name);
                    }
                    Err(e) => {
                        error!("[{}] user data stream connect failed: {}, retrying in 3s", exchange_name, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    /// Bybit v5 private stream (order topic) -- verified against
    /// https://bybit-exchange.github.io/docs/v5/ws/connect and
    /// https://bybit-exchange.github.io/docs/v5/websocket/private/order
    /// (2026-08-31).
    async fn spawn_bybit_private_stream(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        let Some(config) = self.config.as_ref() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let api_key = config.api_key.clone();
        let secret_key = config.secret_key.clone();
        let ws_url = self.definition.endpoints.websocket_url.clone();
        let exchange_name = self.definition.name.clone();

        tokio::spawn(async move {
            loop {
                match tokio_tungstenite::connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        info!("[{}] private WebSocket connected", exchange_name);
                        use futures_util::{SinkExt, StreamExt};
                        let (mut write, mut read) = ws_stream.split();

                        let expires = (nano_timestamp() / 1_000_000) as i64 + 10_000;
                        let signature = bybit_ws_signature(&secret_key, expires);
                        let auth_msg = serde_json::json!({
                            "op": "auth", "args": [api_key, expires, signature],
                        });
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(auth_msg.to_string())).await.is_err() {
                            error!("[{}] auth send failed, reconnecting", exchange_name);
                            continue;
                        }
                        let subscribe_msg = serde_json::json!({"op": "subscribe", "args": ["order"]});
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string())).await.is_err() {
                            error!("[{}] subscribe send failed, reconnecting", exchange_name);
                            continue;
                        }

                        // Bybit closes the connection after 10 minutes without
                        // a ping/pong -- send one every 20s as documented.
                        loop {
                            tokio::select! {
                                msg = read.next() => {
                                    let Some(msg) = msg else { break };
                                    let text = match msg {
                                        Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                                        Ok(_) => continue,
                                        Err(e) => {
                                            warn!("[{}] private WebSocket error: {}", exchange_name, e);
                                            break;
                                        }
                                    };
                                    let Ok(parsed) = serde_json::from_str::<Value>(&text) else { continue };
                                    if parsed.get("topic").and_then(|t| t.as_str()) != Some("order") {
                                        continue; // auth/subscribe acks, pong, etc.
                                    }
                                    let Some(data) = parsed.get("data").and_then(|d| d.as_array()) else { continue };
                                    for order in data {
                                        let order_status = order.get("orderStatus").and_then(|s| s.as_str()).unwrap_or("");
                                        let update_type = match order_status {
                                            "Filled" => UpdateType::CompleteFill,
                                            "PartiallyFilled" => UpdateType::PartialFill,
                                            "Cancelled" | "Deactivated" => UpdateType::Cancellation,
                                            "Rejected" => UpdateType::Rejection,
                                            _ => continue,
                                        };
                                        let status = match update_type {
                                            UpdateType::CompleteFill => ExecutionStatus::Filled,
                                            UpdateType::PartialFill => ExecutionStatus::PartiallyFilled,
                                            UpdateType::Cancellation => ExecutionStatus::Cancelled,
                                            UpdateType::Rejection => ExecutionStatus::Rejected,
                                            UpdateType::StatusChange => ExecutionStatus::Submitted,
                                        };
                                        // orderLinkId is Bybit's client-supplied
                                        // ref (build_order_params sets this to
                                        // signal.id); orderId is Bybit's own.
                                        let order_id = order.get("orderLinkId").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                        let exchange_order_id = order.get("orderId").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                        let symbol = order.get("symbol").and_then(|v| v.as_str()).map(|s| s.to_string());
                                        let side = order.get("side").and_then(|v| v.as_str()).map(|s| s.to_string());
                                        let cum_qty: f64 = order.get("cumExecQty").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        let avg_price: f64 = order.get("avgPrice").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        callback(OrderUpdate {
                                            order_id, exchange_order_id, update_type, status,
                                            filled_quantity: Some(cum_qty), fill_price: Some(avg_price),
                                            timestamp: nano_timestamp(), exchange_timestamp_ns: None, exchange_sequence: None,
                                            symbol, side,
                                        });
                                    }
                                }
                                _ = tokio::time::sleep(std::time::Duration::from_secs(20)) => {
                                    if write.send(tokio_tungstenite::tungstenite::Message::Text(
                                        serde_json::json!({"op": "ping"}).to_string()
                                    )).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        warn!("[{}] private WebSocket disconnected, reconnecting in 3s", exchange_name);
                    }
                    Err(e) => {
                        error!("[{}] private WebSocket connect failed: {}, retrying in 3s", exchange_name, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    /// OKX v5 private stream (orders channel, SPOT) -- verified against
    /// https://www.okx.com/docs-v5/en/#overview-websocket-login (2026-08-31).
    async fn spawn_okx_private_stream(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        let Some(config) = self.config.as_ref() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let api_key = config.api_key.clone();
        let secret_key = config.secret_key.clone();
        let passphrase = config.passphrase.clone().unwrap_or_default();
        let ws_url = self.definition.endpoints.websocket_url.clone();
        let exchange_name = self.definition.name.clone();

        tokio::spawn(async move {
            loop {
                match tokio_tungstenite::connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        info!("[{}] private WebSocket connected", exchange_name);
                        use futures_util::{SinkExt, StreamExt};
                        let (mut write, mut read) = ws_stream.split();

                        let timestamp = (nano_timestamp() / 1_000_000_000).to_string();
                        let signature = okx_ws_signature(&secret_key, &timestamp);
                        let login_msg = serde_json::json!({
                            "op": "login",
                            "args": [{"apiKey": api_key, "passphrase": passphrase, "timestamp": timestamp, "sign": signature}],
                        });
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(login_msg.to_string())).await.is_err() {
                            error!("[{}] login send failed, reconnecting", exchange_name);
                            continue;
                        }
                        let subscribe_msg = serde_json::json!({
                            "op": "subscribe", "args": [{"channel": "orders", "instType": "SPOT"}],
                        });
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string())).await.is_err() {
                            error!("[{}] subscribe send failed, reconnecting", exchange_name);
                            continue;
                        }

                        loop {
                            tokio::select! {
                                msg = read.next() => {
                                    let Some(msg) = msg else { break };
                                    let text = match msg {
                                        Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                                        Ok(_) => continue,
                                        Err(e) => {
                                            warn!("[{}] private WebSocket error: {}", exchange_name, e);
                                            break;
                                        }
                                    };
                                    let Ok(parsed) = serde_json::from_str::<Value>(&text) else { continue };
                                    let is_orders_channel = parsed.get("arg")
                                        .and_then(|a| a.get("channel"))
                                        .and_then(|c| c.as_str()) == Some("orders");
                                    if !is_orders_channel {
                                        continue; // login/subscribe acks, etc.
                                    }
                                    let Some(data) = parsed.get("data").and_then(|d| d.as_array()) else { continue };
                                    for order in data {
                                        let order_state = order.get("state").and_then(|s| s.as_str()).unwrap_or("");
                                        let update_type = match order_state {
                                            "filled" => UpdateType::CompleteFill,
                                            "partially_filled" => UpdateType::PartialFill,
                                            "canceled" => UpdateType::Cancellation,
                                            _ => continue,
                                        };
                                        let status = match update_type {
                                            UpdateType::CompleteFill => ExecutionStatus::Filled,
                                            UpdateType::PartialFill => ExecutionStatus::PartiallyFilled,
                                            UpdateType::Cancellation => ExecutionStatus::Cancelled,
                                            UpdateType::Rejection => ExecutionStatus::Rejected,
                                            UpdateType::StatusChange => ExecutionStatus::Submitted,
                                        };
                                        let order_id = order.get("clOrdId").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                        let exchange_order_id = order.get("ordId").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                        let symbol = order.get("instId").and_then(|v| v.as_str()).map(|s| s.to_string());
                                        let side = order.get("side").and_then(|v| v.as_str()).map(|s| s.to_string());
                                        // accFillSz/avgPx (cumulative), matching
                                        // parse_fill_status's REST field choice
                                        // for OKX, not fillSz (a single event).
                                        let fill_sz: f64 = order.get("accFillSz").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        let avg_px: f64 = order.get("avgPx").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        callback(OrderUpdate {
                                            order_id, exchange_order_id, update_type, status,
                                            filled_quantity: Some(fill_sz), fill_price: Some(avg_px),
                                            timestamp: nano_timestamp(), exchange_timestamp_ns: None, exchange_sequence: None,
                                            symbol, side,
                                        });
                                    }
                                }
                                _ = tokio::time::sleep(std::time::Duration::from_secs(25)) => {
                                    if write.send(tokio_tungstenite::tungstenite::Message::Text("ping".to_string())).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        warn!("[{}] private WebSocket disconnected, reconnecting in 3s", exchange_name);
                    }
                    Err(e) => {
                        error!("[{}] private WebSocket connect failed: {}, retrying in 3s", exchange_name, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    /// Kraken WebSocket v2 executions channel -- verified against
    /// https://docs.kraken.com/api/docs/rest-api/get-websockets-token and
    /// https://docs.kraken.com/api/docs/websocket-v2/executions/ (2026-08-31).
    async fn spawn_kraken_executions_stream(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        if self.config.is_none() {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        }
        // A private REST call, reused via this connector's own signed
        // execute_request (correctly signed since the nonce fix -- see
        // execute_request's Kraken-specific nonce injection).
        let token = match self.execute_request("POST", "/0/private/GetWebSocketsToken", HashMap::new()).await {
            Ok(resp) => match resp.get("result").and_then(|r| r.get("token")).and_then(|t| t.as_str()) {
                Some(t) => t.to_string(),
                None => {
                    error!("[{}] GetWebSocketsToken response missing token: {}", self.definition.name, resp);
                    return;
                }
            },
            Err(e) => {
                error!("[{}] Failed to obtain WebSockets token: {}", self.definition.name, e);
                return;
            }
        };
        let exchange_name = self.definition.name.clone();

        tokio::spawn(async move {
            // The token is fetched once, outside the reconnect loop --
            // Kraken's own docs say it "does not expire once a connection
            // ... is maintained," but a fresh token needs a signed REST
            // call this spawned 'static task can't make on its own (no
            // access to GenericConnector's auth/http_client here). A
            // disconnect that outlasts the ~15-minute token window will
            // need this whole subscribe_to_updates call restarted (i.e. a
            // process restart) to recover -- an accepted v1 limitation,
            // not a silent gap.
            let ws_url = "wss://ws-auth.kraken.com/v2";
            loop {
                match tokio_tungstenite::connect_async(ws_url).await {
                    Ok((ws_stream, _)) => {
                        info!("[{}] executions WebSocket connected", exchange_name);
                        use futures_util::{SinkExt, StreamExt};
                        let (mut write, mut read) = ws_stream.split();

                        let subscribe_msg = serde_json::json!({
                            "method": "subscribe",
                            "params": {"channel": "executions", "token": token, "snapshot": false},
                        });
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string())).await.is_err() {
                            error!("[{}] subscribe send failed, reconnecting", exchange_name);
                            continue;
                        }

                        while let Some(msg) = read.next().await {
                            let text = match msg {
                                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                                Ok(_) => continue,
                                Err(e) => {
                                    warn!("[{}] executions WebSocket error: {}", exchange_name, e);
                                    break;
                                }
                            };
                            let Ok(parsed) = serde_json::from_str::<Value>(&text) else { continue };
                            if parsed.get("channel").and_then(|c| c.as_str()) != Some("executions") {
                                continue; // subscribe acks, heartbeats, etc.
                            }
                            let Some(entries) = parsed.get("data").and_then(|d| d.as_array()) else { continue };
                            for exec in entries {
                                let order_status = exec.get("order_status").and_then(|s| s.as_str()).unwrap_or("");
                                let update_type = match order_status {
                                    "filled" => UpdateType::CompleteFill,
                                    "partially_filled" => UpdateType::PartialFill,
                                    "canceled" | "expired" => UpdateType::Cancellation,
                                    "rejected" => UpdateType::Rejection,
                                    _ => continue,
                                };
                                let status = match update_type {
                                    UpdateType::CompleteFill => ExecutionStatus::Filled,
                                    UpdateType::PartialFill => ExecutionStatus::PartiallyFilled,
                                    UpdateType::Cancellation => ExecutionStatus::Cancelled,
                                    UpdateType::Rejection => ExecutionStatus::Rejected,
                                    UpdateType::StatusChange => ExecutionStatus::Submitted,
                                };
                                // Kraken's executions push doesn't echo back a
                                // client-supplied reference (userref) the way
                                // every other exchange here does -- order_id
                                // is Kraken's own txid for both fields, so a
                                // caller matching this back to a locally-
                                // submitted signal needs to track that
                                // mapping itself (e.g. from the placement
                                // response's txid) rather than relying on
                                // OrderUpdate.order_id alone.
                                let order_id = exec.get("order_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let symbol = exec.get("symbol").and_then(|v| v.as_str()).map(|s| s.to_string());
                                let side = exec.get("side").and_then(|v| v.as_str()).map(|s| s.to_string());
                                let cum_qty = exec.get("cum_qty").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let avg_price = exec.get("avg_price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                callback(OrderUpdate {
                                    order_id: order_id.clone(),
                                    exchange_order_id: order_id,
                                    update_type, status,
                                    filled_quantity: Some(cum_qty), fill_price: Some(avg_price),
                                    timestamp: nano_timestamp(), exchange_timestamp_ns: None, exchange_sequence: None,
                                    symbol, side,
                                });
                            }
                        }
                        warn!("[{}] executions WebSocket disconnected, reconnecting in 3s", exchange_name);
                    }
                    Err(e) => {
                        error!("[{}] executions WebSocket connect failed: {}, retrying in 3s", exchange_name, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    /// Gemini order events -- verified 2026-08-31 against
    /// https://docs.gemini.com/websocket/authentication and
    /// https://developer.gemini.com/websocket/order-events: auth headers
    /// (X-GEMINI-APIKEY/PAYLOAD/SIGNATURE) are sent on the WebSocket
    /// HANDSHAKE request itself, not a post-connect message -- the same
    /// base64-JSON-payload + HMAC-SHA384 scheme as this crate's REST
    /// GeminiAuth (auth.rs), just delivered as headers instead of a
    /// signed body. Order push messages share the same shape as the REST
    /// order-status response (is_live/is_cancelled/executed_amount/
    /// avg_execution_price), matching parse_fill_status's Gemini arm.
    async fn spawn_gemini_order_events(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        let Some(config) = self.config.as_ref() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let api_key = config.api_key.clone();
        let secret_key = config.secret_key.clone();
        let exchange_name = self.definition.name.clone();

        tokio::spawn(async move {
            loop {
                // A fresh, strictly-increasing nonce is required on every
                // reconnect (Gemini rejects a replayed nonce), so the auth
                // headers are rebuilt each time through this loop rather
                // than computed once outside it.
                let nonce = (nano_timestamp() / 1_000_000) as u64;
                let (payload_b64, signature) = gemini_ws_auth_headers(&secret_key, "/v1/order/events", nonce);

                let request = {
                    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
                    use tokio_tungstenite::tungstenite::http::HeaderValue;
                    let mut req = match "wss://api.gemini.com/v1/order/events".into_client_request() {
                        Ok(r) => r,
                        Err(e) => {
                            error!("[{}] failed to build request: {}, retrying in 3s", exchange_name, e);
                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                            continue;
                        }
                    };
                    let headers = req.headers_mut();
                    match (
                        HeaderValue::from_str(&api_key),
                        HeaderValue::from_str(&payload_b64),
                        HeaderValue::from_str(&signature),
                    ) {
                        (Ok(k), Ok(p), Ok(s)) => {
                            headers.insert("X-GEMINI-APIKEY", k);
                            headers.insert("X-GEMINI-PAYLOAD", p);
                            headers.insert("X-GEMINI-SIGNATURE", s);
                        }
                        _ => {
                            error!("[{}] failed to build auth headers, retrying in 3s", exchange_name);
                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                            continue;
                        }
                    }
                    req
                };

                match tokio_tungstenite::connect_async(request).await {
                    Ok((ws_stream, _)) => {
                        info!("[{}] order events WebSocket connected", exchange_name);
                        use futures_util::StreamExt;
                        let (_write, mut read) = ws_stream.split();

                        while let Some(msg) = read.next().await {
                            let text = match msg {
                                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                                Ok(_) => continue,
                                Err(e) => {
                                    warn!("[{}] order events WebSocket error: {}", exchange_name, e);
                                    break;
                                }
                            };
                            let Ok(parsed) = serde_json::from_str::<Value>(&text) else { continue };
                            // Gemini pushes either a single order object or
                            // an array of them depending on event type;
                            // handle both without assuming which.
                            let orders: Vec<&Value> = match &parsed {
                                Value::Array(items) => items.iter().collect(),
                                Value::Object(_) => vec![&parsed],
                                _ => continue,
                            };
                            for order in orders {
                                let is_cancelled = order.get("is_cancelled").and_then(|v| v.as_bool()).unwrap_or(false);
                                let filled_qty: f64 = order.get("executed_amount").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let remaining: f64 = order.get("remaining_amount").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(f64::NAN);
                                let update_type = if is_cancelled {
                                    UpdateType::Cancellation
                                } else if filled_qty > 0.0 && remaining == 0.0 {
                                    UpdateType::CompleteFill
                                } else if filled_qty > 0.0 {
                                    UpdateType::PartialFill
                                } else {
                                    continue; // still-live with no fill yet -- nothing to record
                                };
                                let status = match update_type {
                                    UpdateType::CompleteFill => ExecutionStatus::Filled,
                                    UpdateType::PartialFill => ExecutionStatus::PartiallyFilled,
                                    UpdateType::Cancellation => ExecutionStatus::Cancelled,
                                    UpdateType::Rejection => ExecutionStatus::Rejected,
                                    UpdateType::StatusChange => ExecutionStatus::Submitted,
                                };
                                let avg_price: f64 = order.get("avg_execution_price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let order_id = order.get("client_order_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let exchange_order_id = order.get("order_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let symbol = order.get("symbol").and_then(|v| v.as_str()).map(|s| s.to_string());
                                let side = order.get("side").and_then(|v| v.as_str()).map(|s| s.to_string());
                                callback(OrderUpdate {
                                    order_id, exchange_order_id, update_type, status,
                                    filled_quantity: Some(filled_qty), fill_price: Some(avg_price),
                                    timestamp: nano_timestamp(), exchange_timestamp_ns: None, exchange_sequence: None,
                                    symbol, side,
                                });
                            }
                        }
                        warn!("[{}] order events WebSocket disconnected, reconnecting in 3s", exchange_name);
                    }
                    Err(e) => {
                        error!("[{}] order events WebSocket connect failed: {}, retrying in 3s", exchange_name, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    /// Deribit user orders stream -- verified 2026-08-31 against
    /// https://docs.deribit.com/#public-auth (client_credentials grant)
    /// and https://docs.deribit.com/subscriptions/user/userordersinstrument_nameinterval
    /// (channel shape, field names). Subscribes to
    /// `user.orders.{kind}.any.raw` using this connector's own configured
    /// `trading_mode.category` as `kind` -- Deribit's docs confirm a
    /// per-instrument channel (`user.orders.{instrument_name}.raw`), and
    /// a currency-scoped variant is documented by direct analogy with the
    /// sibling `user.trades.(kind).(currency).(interval)` channel, but
    /// this crate hasn't independently confirmed "any" as a valid
    /// currency wildcard for ORDERS specifically with the same primary-
    /// doc confidence as the rest of this function -- flagged here
    /// rather than silently assumed correct.
    async fn spawn_deribit_orders_stream(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        let Some(config) = self.config.as_ref() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let client_id = config.api_key.clone();
        let client_secret = config.secret_key.clone();
        let kind = self.definition.trading_mode.category.clone();
        let exchange_name = self.definition.name.clone();

        tokio::spawn(async move {
            loop {
                match tokio_tungstenite::connect_async("wss://www.deribit.com/ws/api/v2").await {
                    Ok((ws_stream, _)) => {
                        info!("[{}] user orders WebSocket connected", exchange_name);
                        use futures_util::{SinkExt, StreamExt};
                        let (mut write, mut read) = ws_stream.split();

                        let auth_msg = serde_json::json!({
                            "jsonrpc": "2.0", "id": 1, "method": "public/auth",
                            "params": {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
                        });
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(auth_msg.to_string())).await.is_err() {
                            error!("[{}] auth send failed, reconnecting", exchange_name);
                            continue;
                        }
                        // Deribit's response to our auth call arrives as a
                        // normal message on the same stream; wait for it
                        // (and the access_token it carries) before
                        // subscribing.
                        let access_token = loop {
                            match read.next().await {
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => {
                                    let Ok(parsed) = serde_json::from_str::<Value>(&t) else { continue };
                                    if let Some(token) = parsed.get("result").and_then(|r| r.get("access_token")).and_then(|v| v.as_str()) {
                                        break Some(token.to_string());
                                    }
                                    if parsed.get("error").is_some() {
                                        error!("[{}] auth rejected: {}", exchange_name, parsed);
                                        break None;
                                    }
                                }
                                Some(Ok(_)) => continue,
                                _ => break None,
                            }
                        };
                        let Some(access_token) = access_token else {
                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                            continue;
                        };

                        let channel = format!("user.orders.{}.any.raw", kind);
                        let subscribe_msg = serde_json::json!({
                            "jsonrpc": "2.0", "id": 2, "method": "private/subscribe",
                            "params": {"channels": [channel], "access_token": access_token},
                        });
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string())).await.is_err() {
                            error!("[{}] subscribe send failed, reconnecting", exchange_name);
                            continue;
                        }

                        while let Some(msg) = read.next().await {
                            let text = match msg {
                                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                                Ok(_) => continue,
                                Err(e) => {
                                    warn!("[{}] user orders WebSocket error: {}", exchange_name, e);
                                    break;
                                }
                            };
                            let Ok(parsed) = serde_json::from_str::<Value>(&text) else { continue };
                            if parsed.get("method").and_then(|m| m.as_str()) != Some("subscription") {
                                continue; // subscribe ack, auth response, etc.
                            }
                            let Some(order) = parsed.get("params").and_then(|p| p.get("data")) else { continue };
                            let order_state = order.get("order_state").and_then(|s| s.as_str()).unwrap_or("");
                            let filled_amount = order.get("filled_amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
                            let update_type = match order_state {
                                "filled" => UpdateType::CompleteFill,
                                "cancelled" => UpdateType::Cancellation,
                                "rejected" => UpdateType::Rejection,
                                _ if filled_amount > 0.0 => UpdateType::PartialFill,
                                _ => continue,
                            };
                            let status = match update_type {
                                UpdateType::CompleteFill => ExecutionStatus::Filled,
                                UpdateType::PartialFill => ExecutionStatus::PartiallyFilled,
                                UpdateType::Cancellation => ExecutionStatus::Cancelled,
                                UpdateType::Rejection => ExecutionStatus::Rejected,
                                UpdateType::StatusChange => ExecutionStatus::Submitted,
                            };
                            let order_id = order.get("order_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            // "label" is Deribit's client-supplied reference
                            // (build_order_params sets this to signal.id via
                            // client_id_field), matching every other
                            // exchange's OrderUpdate.order_id convention --
                            // falls back to Deribit's own order_id if a
                            // label was never set on this order.
                            let client_order_id = order.get("label").and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty()).map(|s| s.to_string()).unwrap_or_else(|| order_id.clone());
                            let symbol = order.get("instrument_name").and_then(|v| v.as_str()).map(|s| s.to_string());
                            let side = order.get("direction").and_then(|v| v.as_str()).map(|s| s.to_string());
                            let avg_price = order.get("average_price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                            callback(OrderUpdate {
                                order_id: client_order_id, exchange_order_id: order_id, update_type, status,
                                filled_quantity: Some(filled_amount), fill_price: Some(avg_price),
                                timestamp: nano_timestamp(), exchange_timestamp_ns: None, exchange_sequence: None,
                                symbol, side,
                            });
                        }
                        warn!("[{}] user orders WebSocket disconnected, reconnecting in 3s", exchange_name);
                    }
                    Err(e) => {
                        error!("[{}] user orders WebSocket connect failed: {}, retrying in 3s", exchange_name, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    /// OANDA transaction stream -- a chunked HTTP GET, not a WebSocket
    /// (this is genuinely OANDA's real-time interface for this data, not
    /// a workaround). Verified 2026-08-31: streaming uses a SEPARATE
    /// hostname from the REST API (stream-fxpractice.oanda.com vs
    /// api-fxpractice.oanda.com), the same Bearer-token auth as REST, and
    /// a response body of newline-delimited JSON transaction objects
    /// (plus periodic HEARTBEAT lines to ignore). Uses its own HTTP
    /// client rather than `self.http_client` -- that one is built with a
    /// finite request timeout appropriate for REST calls, which would
    /// forcibly kill this intentionally long-lived streaming connection.
    async fn spawn_oanda_transaction_stream(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        let Some(config) = self.config.as_ref() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let Ok(streaming_client) = Client::builder().build() else {
            error!("[{}] failed to build streaming HTTP client", self.definition.name);
            return;
        };
        let api_key = config.api_key.clone();
        let account_id = config.passphrase.clone().unwrap_or_default();
        let stream_url = format!(
            "{}/v3/accounts/{}/transactions/stream",
            self.definition.endpoints.rest_url.replacen("api-", "stream-", 1),
            account_id,
        );
        let exchange_name = self.definition.name.clone();

        tokio::spawn(async move {
            loop {
                match streaming_client.get(&stream_url).header("Authorization", format!("Bearer {}", api_key)).send().await {
                    Ok(mut response) => {
                        if !response.status().is_success() {
                            error!("[{}] transaction stream returned {}, retrying in 3s", exchange_name, response.status());
                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                            continue;
                        }
                        info!("[{}] transaction stream connected", exchange_name);
                        let mut buf = String::new();
                        loop {
                            match response.chunk().await {
                                Ok(Some(bytes)) => {
                                    buf.push_str(&String::from_utf8_lossy(&bytes));
                                    while let Some(newline_pos) = buf.find('\n') {
                                        let line: String = buf.drain(..=newline_pos).collect();
                                        let line = line.trim();
                                        if line.is_empty() { continue; }
                                        let Ok(parsed) = serde_json::from_str::<Value>(line) else { continue };
                                        if parsed.get("type").and_then(|t| t.as_str()) != Some("ORDER_FILL") {
                                            continue; // HEARTBEAT, other transaction types, etc.
                                        }
                                        let units: f64 = parsed.get("units").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        let price: f64 = parsed.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        if units == 0.0 || price == 0.0 { continue; }
                                        let instrument = parsed.get("instrument").and_then(|v| v.as_str()).map(|s| s.to_string());
                                        let side = Some(if units > 0.0 { "buy".to_string() } else { "sell".to_string() });
                                        // OANDA doesn't echo back a client-
                                        // supplied reference the way most
                                        // other exchanges here do (this
                                        // connector's build_oanda_order_params
                                        // never sets clientExtensions) --
                                        // OANDA's own order id is used for
                                        // both OrderUpdate fields, so a
                                        // caller matching this back to a
                                        // locally-submitted signal needs its
                                        // own mapping (e.g. from the
                                        // placement response's transaction id).
                                        let order_id = parsed.get("orderID").and_then(|v| v.as_str())
                                            .or_else(|| parsed.get("id").and_then(|v| v.as_str()))
                                            .unwrap_or("").to_string();
                                        callback(OrderUpdate {
                                            order_id: order_id.clone(),
                                            exchange_order_id: order_id,
                                            update_type: UpdateType::CompleteFill,
                                            status: ExecutionStatus::Filled,
                                            filled_quantity: Some(units.abs()),
                                            fill_price: Some(price),
                                            timestamp: nano_timestamp(),
                                            exchange_timestamp_ns: None,
                                            exchange_sequence: None,
                                            symbol: instrument,
                                            side,
                                        });
                                    }
                                }
                                Ok(None) => break, // stream ended
                                Err(e) => {
                                    warn!("[{}] transaction stream read error: {}", exchange_name, e);
                                    break;
                                }
                            }
                        }
                        warn!("[{}] transaction stream disconnected, reconnecting in 3s", exchange_name);
                    }
                    Err(e) => {
                        error!("[{}] transaction stream connect failed: {}, retrying in 3s", exchange_name, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    /// Coinbase Advanced Trade "user" channel -- CDP JWT auth (ES256).
    /// Verified 2026-08-31 against
    /// https://docs.cdp.coinbase.com/get-started/authentication/jwt-authentication
    /// for the JWT header/claims shape. This is a DIFFERENT credential
    /// type from the legacy HMAC key+secret+passphrase this crate's REST
    /// auth (auth.rs's HmacSha256WithPassphrase) already uses for
    /// Coinbase's REST endpoints -- `config.secret_key` must hold a CDP
    /// API key's PKCS8 PEM-encoded EC private key (the multi-line
    /// "-----BEGIN EC PRIVATE KEY-----..." string CDP issues) for this to
    /// work, not the legacy HMAC secret. A tenant using this connector's
    /// REST order-placement path with a legacy key today would need a
    /// separate, newer CDP key specifically to also get real-time fill
    /// confirmation here -- this crate doesn't currently model two
    /// credential types per exchange connection, so this reinterprets
    /// the same config field rather than adding a new one. That's a real
    /// scope gap worth closing later, not a bug in what's implemented.
    ///
    /// The exact "user" channel push shape (field names inside
    /// events[].orders[]) is assumed to mirror the REST Get Order
    /// response this crate's parse_fill_status already parses for
    /// Coinbase (status/filled_size/average_filled_price) -- not
    /// independently confirmed against a live push message the way the
    /// four higher-confidence exchanges' formats were.
    async fn spawn_coinbase_user_channel(&self, callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        let Some(config) = self.config.as_ref() else {
            error!("[{}] subscribe_to_updates called before initialize()", self.definition.name);
            return;
        };
        let api_key = config.api_key.clone();
        let secret_key_pem = config.secret_key.clone();
        let exchange_name = self.definition.name.clone();

        tokio::spawn(async move {
            loop {
                let jwt = match coinbase_cdp_jwt(&api_key, &secret_key_pem) {
                    Some(j) => j,
                    None => {
                        error!(
                            "[{}] failed to build CDP JWT -- secret_key must be a CDP key's PKCS8 PEM private key, not a legacy HMAC secret",
                            exchange_name
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

                match tokio_tungstenite::connect_async("wss://advanced-trade-ws.coinbase.com").await {
                    Ok((ws_stream, _)) => {
                        info!("[{}] user channel WebSocket connected", exchange_name);
                        use futures_util::{SinkExt, StreamExt};
                        let (mut write, mut read) = ws_stream.split();

                        // product_ids omitted -- the "user" channel is
                        // documented as account-scoped (open orders and
                        // positions); this connector doesn't know a fixed
                        // product_id ahead of time since any symbol could
                        // trade, and omitting the field wasn't
                        // independently confirmed against a live
                        // connection to return updates for every product
                        // rather than none.
                        let subscribe_msg = serde_json::json!({
                            "type": "subscribe", "channel": "user", "jwt": jwt,
                        });
                        if write.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string())).await.is_err() {
                            error!("[{}] subscribe send failed, reconnecting", exchange_name);
                            continue;
                        }

                        while let Some(msg) = read.next().await {
                            let text = match msg {
                                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                                Ok(_) => continue,
                                Err(e) => {
                                    warn!("[{}] user channel WebSocket error: {}", exchange_name, e);
                                    break;
                                }
                            };
                            let Ok(parsed) = serde_json::from_str::<Value>(&text) else { continue };
                            if parsed.get("channel").and_then(|c| c.as_str()) != Some("user") {
                                continue; // subscriptions ack, heartbeat, etc.
                            }
                            let Some(events) = parsed.get("events").and_then(|e| e.as_array()) else { continue };
                            for event in events {
                                let Some(orders) = event.get("orders").and_then(|o| o.as_array()) else { continue };
                                for order in orders {
                                    let order_status = order.get("status").and_then(|s| s.as_str()).unwrap_or("");
                                    let filled_size: f64 = order.get("filled_size").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let update_type = match order_status {
                                        "FILLED" => UpdateType::CompleteFill,
                                        "CANCELLED" | "EXPIRED" => UpdateType::Cancellation,
                                        "FAILED" => UpdateType::Rejection,
                                        _ if filled_size > 0.0 => UpdateType::PartialFill,
                                        _ => continue,
                                    };
                                    let status = match update_type {
                                        UpdateType::CompleteFill => ExecutionStatus::Filled,
                                        UpdateType::PartialFill => ExecutionStatus::PartiallyFilled,
                                        UpdateType::Cancellation => ExecutionStatus::Cancelled,
                                        UpdateType::Rejection => ExecutionStatus::Rejected,
                                        UpdateType::StatusChange => ExecutionStatus::Submitted,
                                    };
                                    let order_id = order.get("client_order_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    let exchange_order_id = order.get("order_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    let symbol = order.get("product_id").and_then(|v| v.as_str()).map(|s| s.to_string());
                                    let side = order.get("order_side").and_then(|v| v.as_str()).map(|s| s.to_string());
                                    let avg_price: f64 = order.get("average_filled_price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    callback(OrderUpdate {
                                        order_id, exchange_order_id, update_type, status,
                                        filled_quantity: Some(filled_size), fill_price: Some(avg_price),
                                        timestamp: nano_timestamp(), exchange_timestamp_ns: None, exchange_sequence: None,
                                        symbol, side,
                                    });
                                }
                            }
                        }
                        warn!("[{}] user channel WebSocket disconnected, reconnecting in 3s", exchange_name);
                    }
                    Err(e) => {
                        error!("[{}] user channel WebSocket connect failed: {}, retrying in 3s", exchange_name, e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
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
                    None => {
                        let exchange_symbol = self.symbol_converter.to_exchange_format(&signal.symbol);
                        self.check_order_fill(&order_id, &exchange_symbol).await
                            .unwrap_or((ExecutionStatus::Submitted, 0.0, 0.0))
                    }
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
        match self.preset {
            ExchangePreset::AlpacaPaper => self.spawn_alpaca_trade_updates(callback).await,
            ExchangePreset::Binance | ExchangePreset::BinanceUS => self.spawn_binance_user_data_stream(callback).await,
            ExchangePreset::Bybit => self.spawn_bybit_private_stream(callback).await,
            ExchangePreset::OKX => self.spawn_okx_private_stream(callback).await,
            ExchangePreset::Kraken => self.spawn_kraken_executions_stream(callback).await,
            ExchangePreset::Gemini => self.spawn_gemini_order_events(callback).await,
            ExchangePreset::Deribit => self.spawn_deribit_orders_stream(callback).await,
            ExchangePreset::OandaPractice => self.spawn_oanda_transaction_stream(callback).await,
            ExchangePreset::Coinbase => self.spawn_coinbase_user_channel(callback).await,
        }
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

    // ========== Kraken ==========

    #[test]
    fn parses_a_real_kraken_closed_order() {
        // Real shape per https://docs.kraken.com/api/docs/rest-api/get-order-info
        // -- keyed by txid, single entry for a single-txid query.
        let resp: Value = serde_json::from_str(r#"{
            "error": [],
            "result": {"OABC1-XYZ23-DEF456": {"status": "closed", "vol_exec": "1.5", "price": "50000.0"}}
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Kraken);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(qty, 1.5);
        assert_eq!(price, 50000.0);
    }

    #[test]
    fn kraken_placement_ack_has_no_fill_fields_and_returns_none() {
        // AddOrder's own response (no "status"/"vol_exec" anywhere) --
        // must fall through to None so execute_order correctly falls back
        // to the follow-up QueryOrders check instead of a phantom fill.
        let resp: Value = serde_json::from_str(r#"{
            "error": [],
            "result": {"descr": {"order": "buy 1.5 XBTUSD"}, "txid": ["OABC1-XYZ23-DEF456"]}
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Kraken);
        assert!(conn.parse_fill_status(&resp).is_none());
    }

    #[test]
    fn kraken_canceled_order_is_not_reported_as_filled() {
        let resp: Value = serde_json::from_str(r#"{
            "error": [],
            "result": {"OABC1-XYZ23-DEF456": {"status": "canceled", "vol_exec": "0", "price": "0"}}
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Kraken);
        let (status, _, _) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Cancelled);
    }

    // ========== Coinbase ==========

    #[test]
    fn parses_a_real_coinbase_filled_order() {
        // https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api/orders/get-order
        let resp: Value = serde_json::from_str(r#"{
            "order": {"order_id": "abc", "status": "FILLED", "filled_size": "0.5", "average_filled_price": "60000.00"}
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Coinbase);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(qty, 0.5);
        assert_eq!(price, 60000.0);
    }

    #[test]
    fn coinbase_open_order_with_no_fill_is_submitted_not_filled() {
        let resp: Value = serde_json::from_str(r#"{
            "order": {"status": "OPEN", "filled_size": "0", "average_filled_price": "0"}
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Coinbase);
        let (status, _, _) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Submitted);
    }

    // ========== Binance ==========

    #[test]
    fn parses_a_real_binance_filled_order_deriving_avg_price_from_quote_qty() {
        // https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints#query-order-user_data
        // -- no direct avg-price field; avg = cummulativeQuoteQty / executedQty.
        let resp: Value = serde_json::from_str(r#"{
            "symbol": "BTCUSDT", "status": "FILLED", "executedQty": "2.0", "cummulativeQuoteQty": "100000.0"
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Binance);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(qty, 2.0);
        assert_eq!(price, 50000.0);
    }

    #[test]
    fn binance_us_shares_the_same_parser_as_binance() {
        let resp: Value = serde_json::from_str(r#"{"status": "PARTIALLY_FILLED", "executedQty": "0.5", "cummulativeQuoteQty": "25000.0"}"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::BinanceUS);
        let (status, qty, _) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::PartiallyFilled);
        assert_eq!(qty, 0.5);
    }

    #[test]
    fn binance_zero_executed_qty_never_divides_by_zero() {
        let resp: Value = serde_json::from_str(r#"{"status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0"}"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Binance);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Submitted);
        assert_eq!(qty, 0.0);
        assert_eq!(price, 0.0);
    }

    // ========== Bybit ==========

    #[test]
    fn parses_a_real_bybit_filled_order() {
        // https://bybit-exchange.github.io/docs/v5/order/order-list
        let resp: Value = serde_json::from_str(r#"{
            "retCode": 0, "result": {"list": [{"orderId": "x", "orderStatus": "Filled", "cumExecQty": "1.2", "avgPrice": "45000.5"}]}
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Bybit);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(qty, 1.2);
        assert_eq!(price, 45000.5);
    }

    #[test]
    fn bybit_empty_list_returns_none_not_a_panic() {
        let resp: Value = serde_json::from_str(r#"{"retCode": 0, "result": {"list": []}}"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Bybit);
        assert!(conn.parse_fill_status(&resp).is_none());
    }

    // ========== OKX ==========

    #[test]
    fn parses_a_real_okx_filled_order() {
        // https://www.okx.com/docs-v5/en/#order-book-trading-trade-get-order-details
        let resp: Value = serde_json::from_str(r#"{
            "code": "0", "data": [{"ordId": "x", "state": "filled", "accFillSz": "0.8", "avgPx": "3000.25"}]
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::OKX);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(qty, 0.8);
        assert_eq!(price, 3000.25);
    }

    // ========== Gemini ==========

    #[test]
    fn parses_a_real_gemini_fully_filled_order() {
        // https://docs.gemini.com/rest/orders -- status derived from
        // is_live/is_cancelled/executed_amount/remaining_amount, no enum field.
        let resp: Value = serde_json::from_str(r#"{
            "is_live": false, "is_cancelled": false,
            "executed_amount": "2.0", "remaining_amount": "0", "avg_execution_price": "150.25"
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Gemini);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(qty, 2.0);
        assert_eq!(price, 150.25);
    }

    #[test]
    fn gemini_partial_fill_still_live() {
        let resp: Value = serde_json::from_str(r#"{
            "is_live": true, "is_cancelled": false,
            "executed_amount": "0.5", "remaining_amount": "1.5", "avg_execution_price": "150.0"
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Gemini);
        let (status, qty, _) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::PartiallyFilled);
        assert_eq!(qty, 0.5);
    }

    #[test]
    fn gemini_cancelled_order_is_cancelled_even_with_partial_fill() {
        let resp: Value = serde_json::from_str(r#"{
            "is_live": false, "is_cancelled": true,
            "executed_amount": "0.5", "remaining_amount": "1.5", "avg_execution_price": "150.0"
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Gemini);
        let (status, _, _) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Cancelled);
    }

    // ========== Deribit ==========

    #[test]
    fn parses_a_real_deribit_placement_response_with_nested_order() {
        // https://docs.deribit.com/#private-buy -- placement response
        // nests the order one level deeper than get_order_state.
        let resp: Value = serde_json::from_str(r#"{
            "result": {"order": {"order_id": "x", "order_state": "filled", "filled_amount": 10.0, "average_price": 50000.0}, "trades": []}
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Deribit);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(qty, 10.0);
        assert_eq!(price, 50000.0);
    }

    #[test]
    fn parses_a_real_deribit_get_order_state_response_without_nesting() {
        // https://docs.deribit.com/#private-get_order_state
        let resp: Value = serde_json::from_str(r#"{
            "result": {"order_id": "x", "order_state": "open", "filled_amount": 3.0, "average_price": 49000.0}
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::Deribit);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::PartiallyFilled); // "open" with filled_amount > 0
        assert_eq!(qty, 3.0);
        assert_eq!(price, 49000.0);
    }

    // ========== OANDA ==========

    #[test]
    fn parses_a_real_oanda_synchronous_market_fill() {
        // https://developer.oanda.com/rest-live-v20/order-df/#OrderFillTransaction
        // -- units is a signed string; a sell reports negative units.
        let resp: Value = serde_json::from_str(r#"{
            "orderCreateTransaction": {"id": "1"},
            "orderFillTransaction": {"id": "2", "units": "-100", "price": "1.10523"}
        }"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::OandaPractice);
        let (status, qty, price) = conn.parse_fill_status(&resp).unwrap();
        assert_eq!(status, ExecutionStatus::Filled);
        assert_eq!(qty, 100.0); // abs() of the signed units
        assert_eq!(price, 1.10523);
    }

    #[test]
    fn oanda_follow_up_status_shape_without_fill_transaction_is_left_unconfirmed() {
        // The follow-up GET's plain order resource doesn't carry a
        // verified fill price/qty field -- must not guess.
        let resp: Value = serde_json::from_str(r#"{"order": {"id": "1", "state": "FILLED"}}"#).unwrap();
        let conn = GenericConnector::new(ExchangePreset::OandaPractice);
        assert!(conn.parse_fill_status(&resp).is_none());
    }

    // ========== build_status_check_request ==========

    #[test]
    fn kraken_status_check_posts_txid_as_body_param() {
        let conn = GenericConnector::new(ExchangePreset::Kraken);
        let (method, path, params) = conn.build_status_check_request("TXID123", "XBTUSD");
        assert_eq!(method, "POST");
        assert_eq!(path, "/0/private/QueryOrders");
        assert_eq!(params.get("txid").unwrap(), "TXID123");
    }

    #[test]
    fn binance_status_check_needs_symbol_and_order_id_as_query_params() {
        let conn = GenericConnector::new(ExchangePreset::Binance);
        let (method, path, params) = conn.build_status_check_request("999", "BTCUSDT");
        assert_eq!(method, "GET");
        assert_eq!(path, "/api/v3/order");
        assert_eq!(params.get("symbol").unwrap(), "BTCUSDT");
        assert_eq!(params.get("orderId").unwrap(), "999");
    }

    #[test]
    fn bybit_status_check_includes_category_from_trading_mode() {
        let conn = GenericConnector::new(ExchangePreset::Bybit);
        let (_, _, params) = conn.build_status_check_request("999", "BTCUSDT");
        assert_eq!(params.get("category").unwrap(), "spot");
        assert_eq!(params.get("orderId").unwrap(), "999");
    }

    #[test]
    fn okx_status_check_uses_inst_id_and_ord_id_field_names() {
        let conn = GenericConnector::new(ExchangePreset::OKX);
        let (_, _, params) = conn.build_status_check_request("999", "BTC-USDT");
        assert_eq!(params.get("instId").unwrap(), "BTC-USDT");
        assert_eq!(params.get("ordId").unwrap(), "999");
    }

    #[test]
    fn coinbase_and_oanda_and_alpaca_embed_order_id_in_the_path() {
        for preset in [ExchangePreset::Coinbase, ExchangePreset::OandaPractice, ExchangePreset::AlpacaPaper] {
            let conn = GenericConnector::new(preset);
            let (method, path, params) = conn.build_status_check_request("ORDID", "SYM");
            assert_eq!(method, "GET");
            assert!(path.ends_with("/ORDID"), "preset {:?}: path {} should end with the order id", preset, path);
            assert!(params.is_empty());
        }
    }

    #[test]
    fn gemini_status_check_posts_order_id_as_body_param() {
        let conn = GenericConnector::new(ExchangePreset::Gemini);
        let (method, path, params) = conn.build_status_check_request("999", "btcusd");
        assert_eq!(method, "POST");
        assert_eq!(path, "/v1/order/status");
        assert_eq!(params.get("order_id").unwrap(), "999");
    }

    #[test]
    fn deribit_status_check_is_get_with_order_id_query_param() {
        let conn = GenericConnector::new(ExchangePreset::Deribit);
        let (method, _, params) = conn.build_status_check_request("999", "BTC-PERPETUAL");
        assert_eq!(method, "GET");
        assert_eq!(params.get("order_id").unwrap(), "999");
    }

    // ========== WebSocket auth signatures ==========

    #[test]
    fn bybit_ws_signature_is_deterministic_hex() {
        let sig1 = bybit_ws_signature("my_secret", 1700000010000);
        let sig2 = bybit_ws_signature("my_secret", 1700000010000);
        assert_eq!(sig1, sig2);
        assert!(sig1.chars().all(|c| c.is_ascii_hexdigit()));
        // HMAC-SHA256 hex digest is 64 chars (32 bytes).
        assert_eq!(sig1.len(), 64);
    }

    #[test]
    fn bybit_ws_signature_changes_with_expires() {
        let sig1 = bybit_ws_signature("my_secret", 1700000010000);
        let sig2 = bybit_ws_signature("my_secret", 1700000020000);
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn bybit_ws_signature_matches_manual_hmac_over_get_realtime_expires() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let expected = {
            let mut mac = Hmac::<Sha256>::new_from_slice(b"my_secret").unwrap();
            mac.update(b"GET/realtime1700000010000");
            hex::encode(mac.finalize().into_bytes())
        };
        assert_eq!(bybit_ws_signature("my_secret", 1700000010000), expected);
    }

    #[test]
    fn okx_ws_signature_is_deterministic_base64() {
        let sig1 = okx_ws_signature("my_secret", "1700000010");
        let sig2 = okx_ws_signature("my_secret", "1700000010");
        assert_eq!(sig1, sig2);
        use base64::Engine;
        assert!(base64::engine::general_purpose::STANDARD.decode(&sig1).is_ok());
    }

    #[test]
    fn okx_ws_signature_changes_with_timestamp() {
        let sig1 = okx_ws_signature("my_secret", "1700000010");
        let sig2 = okx_ws_signature("my_secret", "1700000020");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn okx_ws_signature_matches_manual_hmac_over_timestamp_get_verify() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        use base64::Engine;
        let expected = {
            let mut mac = Hmac::<Sha256>::new_from_slice(b"my_secret").unwrap();
            mac.update(b"1700000010GET/users/self/verify");
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
        };
        assert_eq!(okx_ws_signature("my_secret", "1700000010"), expected);
    }

    // ========== Gemini WS auth ==========

    #[test]
    fn gemini_ws_auth_headers_payload_embeds_request_path_and_nonce() {
        use base64::Engine;
        let (payload_b64, _signature) = gemini_ws_auth_headers("secret", "/v1/order/events", 1700000000000);
        let decoded = base64::engine::general_purpose::STANDARD.decode(&payload_b64).unwrap();
        let payload: Value = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(payload["request"], "/v1/order/events");
        assert_eq!(payload["nonce"], 1700000000000_u64);
    }

    #[test]
    fn gemini_ws_auth_headers_signature_is_hex_sha384() {
        let (_payload, signature) = gemini_ws_auth_headers("secret", "/v1/order/events", 1700000000000);
        // SHA384 hex digest is 96 hex chars (48 bytes).
        assert_eq!(signature.len(), 96);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn gemini_ws_auth_headers_changes_with_nonce() {
        let (payload1, sig1) = gemini_ws_auth_headers("secret", "/v1/order/events", 1);
        let (payload2, sig2) = gemini_ws_auth_headers("secret", "/v1/order/events", 2);
        assert_ne!(payload1, payload2);
        assert_ne!(sig1, sig2);
    }

    // ========== Coinbase CDP JWT ==========

    fn generate_test_p256_pem() -> String {
        use p256::ecdsa::SigningKey;
        use p256::pkcs8::EncodePrivateKey;
        let signing_key = SigningKey::random(&mut rand::rngs::OsRng);
        signing_key.to_pkcs8_pem(Default::default()).unwrap().to_string()
    }

    #[test]
    fn coinbase_cdp_jwt_rejects_a_non_pem_secret() {
        assert!(coinbase_cdp_jwt("my_api_key", "not a pem key").is_none());
        assert!(coinbase_cdp_jwt("my_api_key", "").is_none());
    }

    #[test]
    fn coinbase_cdp_jwt_produces_three_dot_separated_base64url_segments() {
        let pem = generate_test_p256_pem();
        let jwt = coinbase_cdp_jwt("organizations/org/apiKeys/key", &pem).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3, "expected header.claims.signature, got: {}", jwt);
        use base64::Engine;
        for part in &parts {
            assert!(base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(part).is_ok(), "segment not valid base64url: {}", part);
        }
    }

    #[test]
    fn coinbase_cdp_jwt_header_and_claims_have_the_documented_shape() {
        use base64::Engine;
        let pem = generate_test_p256_pem();
        let jwt = coinbase_cdp_jwt("organizations/org/apiKeys/key", &pem).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        let header: Value = serde_json::from_slice(&base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        let claims: Value = serde_json::from_slice(&base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();

        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["typ"], "JWT");
        assert_eq!(header["kid"], "organizations/org/apiKeys/key");
        assert!(header["nonce"].is_string());

        assert_eq!(claims["iss"], "cdp");
        assert_eq!(claims["sub"], "organizations/org/apiKeys/key");
        assert_eq!(claims["aud"], serde_json::json!(["cdp_service"]));
        // No uri/uris claim for WebSocket auth, per CDP's own docs.
        assert!(claims.get("uri").is_none());
        assert!(claims.get("uris").is_none());
        let nbf = claims["nbf"].as_u64().unwrap();
        let exp = claims["exp"].as_u64().unwrap();
        assert_eq!(exp - nbf, 120);
    }

    #[test]
    fn coinbase_cdp_jwt_signature_verifies_against_the_same_keys_public_half() {
        // The real correctness test: sign with the private key, verify
        // with the corresponding public key -- if the signing-input
        // construction or signature encoding is wrong, this fails even
        // though the JWT still "looks" well-formed.
        use p256::ecdsa::{SigningKey, VerifyingKey, Signature};
        use p256::ecdsa::signature::Verifier;
        use p256::pkcs8::DecodePrivateKey;
        use base64::Engine;

        let pem = generate_test_p256_pem();
        let signing_key = SigningKey::from_pkcs8_pem(&pem).unwrap();
        let verifying_key = VerifyingKey::from(&signing_key);

        let jwt = coinbase_cdp_jwt("my_key", &pem).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let sig_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        let signature = Signature::from_slice(&sig_bytes).unwrap();

        assert!(verifying_key.verify(signing_input.as_bytes(), &signature).is_ok());
    }

    #[test]
    fn coinbase_cdp_jwt_signature_does_not_verify_against_a_different_keys_public_half() {
        use p256::ecdsa::{SigningKey, VerifyingKey, Signature};
        use p256::ecdsa::signature::Verifier;
        use base64::Engine;

        let pem = generate_test_p256_pem();
        let other_key = SigningKey::random(&mut rand::rngs::OsRng);
        let other_verifying_key = VerifyingKey::from(&other_key);

        let jwt = coinbase_cdp_jwt("my_key", &pem).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let sig_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        let signature = Signature::from_slice(&sig_bytes).unwrap();

        assert!(other_verifying_key.verify(signing_input.as_bytes(), &signature).is_err());
    }

    #[test]
    fn bybit_and_okx_ws_signatures_diverge_on_the_same_secret_and_timestamp() {
        // Different message formats (GET/realtime{ts} vs {ts}GET/users/self/verify)
        // and different encodings (hex vs base64) -- confirms neither
        // function is accidentally a copy of the other.
        let bybit_sig = bybit_ws_signature("shared_secret", 1700000010000);
        let okx_sig = okx_ws_signature("shared_secret", "1700000010");
        assert_ne!(bybit_sig, okx_sig);
    }
}
