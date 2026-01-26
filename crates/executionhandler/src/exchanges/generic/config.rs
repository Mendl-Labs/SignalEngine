//! Exchange configuration definitions for the generic connector.
//!
//! Each supported exchange has a preset configuration that defines:
//! - API endpoints (REST and WebSocket)
//! - Authentication method and signing algorithm
//! - Symbol format conversion rules
//! - Rate limits and capabilities

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Supported exchange presets
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExchangePreset {
    Kraken,
    Coinbase,
    BinanceUS,
    Binance,
    Bybit,
    OKX,
    Gemini,
    Deribit,
}

impl ExchangePreset {
    /// Get the exchange definition for this preset
    pub fn definition(&self) -> ExchangeDefinition {
        match self {
            ExchangePreset::Kraken => kraken_definition(),
            ExchangePreset::Coinbase => coinbase_definition(),
            ExchangePreset::BinanceUS => binance_us_definition(),
            ExchangePreset::Binance => binance_definition(),
            ExchangePreset::Bybit => bybit_definition(),
            ExchangePreset::OKX => okx_definition(),
            ExchangePreset::Gemini => gemini_definition(),
            ExchangePreset::Deribit => deribit_definition(),
        }
    }
    
    /// Parse from exchange name string
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "kraken" => Some(Self::Kraken),
            "coinbase" | "coinbase_pro" | "coinbase-pro" => Some(Self::Coinbase),
            "binance_us" | "binance-us" | "binanceus" => Some(Self::BinanceUS),
            "binance" => Some(Self::Binance),
            "bybit" => Some(Self::Bybit),
            "okx" | "okex" => Some(Self::OKX),
            "gemini" => Some(Self::Gemini),
            "deribit" => Some(Self::Deribit),
            _ => None,
        }
    }
    
    /// Get the display name
    pub fn display_name(&self) -> &'static str {
        match self {
            ExchangePreset::Kraken => "Kraken",
            ExchangePreset::Coinbase => "Coinbase",
            ExchangePreset::BinanceUS => "Binance US",
            ExchangePreset::Binance => "Binance",
            ExchangePreset::Bybit => "Bybit",
            ExchangePreset::OKX => "OKX",
            ExchangePreset::Gemini => "Gemini",
            ExchangePreset::Deribit => "Deribit",
        }
    }
}

/// Authentication method used by the exchange
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    /// HMAC-SHA256 signature (Binance, Bybit, OKX)
    HmacSha256 {
        /// Header name for API key
        api_key_header: String,
        /// Header name for signature
        signature_header: String,
        /// Header name for timestamp
        timestamp_header: String,
        /// Whether timestamp is in milliseconds (true) or seconds (false)
        timestamp_ms: bool,
        /// How to include signature in request (query, header, body)
        signature_location: SignatureLocation,
    },
    /// HMAC-SHA512 signature (Kraken)
    HmacSha512 {
        api_key_header: String,
        signature_header: String,
        /// Whether to include nonce in body
        use_nonce: bool,
    },
    /// HMAC-SHA256 with passphrase (Coinbase)
    HmacSha256WithPassphrase {
        api_key_header: String,
        signature_header: String,
        passphrase_header: String,
        timestamp_header: String,
    },
    /// RSA signature (some exchanges)
    Rsa {
        api_key_header: String,
        signature_header: String,
    },
    /// Ed25519 signature (Deribit)
    Ed25519 {
        client_id_param: String,
        signature_param: String,
    },
}

/// Where to place the signature in the request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignatureLocation {
    /// Signature in query parameters
    Query,
    /// Signature in headers
    Header,
    /// Signature in request body
    Body,
}

/// Endpoint configuration for an exchange
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointConfig {
    /// Base REST API URL
    pub rest_url: String,
    /// WebSocket URL for order updates
    pub websocket_url: String,
    /// Path for placing orders
    pub place_order_path: String,
    /// Separate path for buy orders (Deribit-style exchanges)
    pub buy_order_path: Option<String>,
    /// Separate path for sell orders (Deribit-style exchanges)
    pub sell_order_path: Option<String>,
    /// Path for canceling orders
    pub cancel_order_path: String,
    /// Path for order status
    pub order_status_path: String,
    /// Path for account balance
    pub balance_path: String,
    /// Path for health check / server time
    pub health_check_path: String,
    /// HTTP method for placing orders (POST for most, GET for some)
    pub place_order_method: String,
    /// Content type for requests
    pub content_type: ContentType,
}

/// Request content type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContentType {
    /// application/x-www-form-urlencoded
    FormUrlEncoded,
    /// application/json
    Json,
}

/// Complete exchange definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeDefinition {
    /// Exchange name
    pub name: String,
    /// Authentication method
    pub auth_method: AuthMethod,
    /// API endpoints
    pub endpoints: EndpointConfig,
    /// Symbol format (e.g., "BTC/USD" -> exchange format)
    pub symbol_format: SymbolFormat,
    /// Rate limits
    pub rate_limits: RateLimits,
    /// Order parameters mapping
    pub order_params: OrderParamsMapping,
    /// Whether the exchange requires a passphrase
    pub requires_passphrase: bool,
    /// Trading mode configuration (spot, margin, futures, etc.)
    pub trading_mode: TradingMode,
    /// Order size and quantity limits
    pub order_limits: OrderLimits,
}

/// Symbol format rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolFormat {
    /// Separator between base and quote (e.g., "/", "-", "", "_")
    pub separator: String,
    /// Whether to use uppercase
    pub uppercase: bool,
    /// Custom mappings for specific symbols (e.g., BTC -> XBT)
    pub custom_mappings: HashMap<String, String>,
    /// Prefix to add (e.g., "X" for Kraken crypto)
    pub base_prefix: String,
    /// Suffix to add to quote currency (e.g., "Z" for Kraken fiat)
    pub quote_prefix: String,
}

/// Rate limit configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimits {
    /// Maximum requests per second
    pub requests_per_second: u32,
    /// Burst limit
    pub burst: u32,
    /// Order-specific rate limit
    pub orders_per_second: u32,
    /// Maximum concurrent orders in a batch
    pub max_concurrent_orders: u32,
}

/// Trading mode configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingMode {
    /// Trading category (spot, linear, inverse, option)
    pub category: String,
    /// Trade mode (cash, cross, isolated)
    pub mode: String,
    /// Additional mode-specific parameters
    pub extra_params: HashMap<String, String>,
}

impl Default for TradingMode {
    fn default() -> Self {
        Self {
            category: "spot".to_string(),
            mode: "cash".to_string(),
            extra_params: HashMap::new(),
        }
    }
}

/// Order limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderLimits {
    /// Minimum order size (base currency)
    pub min_order_size: f64,
    /// Maximum order size (base currency)
    pub max_order_size: f64,
    /// Default tick size (can be overridden per symbol)
    pub default_tick_size: f64,
    /// Maximum orders in a single batch request
    pub max_batch_size: usize,
}

impl Default for OrderLimits {
    fn default() -> Self {
        Self {
            min_order_size: 0.0001,
            max_order_size: 1_000_000.0,
            default_tick_size: 0.01,
            max_batch_size: 50,
        }
    }
}

/// Order parameter field mappings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderParamsMapping {
    /// Field name for symbol/pair
    pub symbol_field: String,
    /// Field name for side (buy/sell)
    pub side_field: String,
    /// Field name for order type
    pub type_field: String,
    /// Field name for quantity
    pub quantity_field: String,
    /// Field name for price
    pub price_field: String,
    /// Field name for client order ID
    pub client_id_field: Option<String>,
    /// Side values
    pub side_buy: String,
    pub side_sell: String,
    /// Order type values
    pub type_market: String,
    pub type_limit: String,
}

// ============================================================================
// Exchange Definitions
// ============================================================================

fn kraken_definition() -> ExchangeDefinition {
    ExchangeDefinition {
        name: "Kraken".to_string(),
        auth_method: AuthMethod::HmacSha512 {
            api_key_header: "API-Key".to_string(),
            signature_header: "API-Sign".to_string(),
            use_nonce: true,
        },
        endpoints: EndpointConfig {
            rest_url: "https://api.kraken.com".to_string(),
            websocket_url: "wss://ws.kraken.com".to_string(),
            place_order_path: "/0/private/AddOrder".to_string(),
            buy_order_path: None,
            sell_order_path: None,
            cancel_order_path: "/0/private/CancelOrder".to_string(),
            order_status_path: "/0/private/QueryOrders".to_string(),
            balance_path: "/0/private/Balance".to_string(),
            health_check_path: "/0/public/Time".to_string(),
            place_order_method: "POST".to_string(),
            content_type: ContentType::FormUrlEncoded,
        },
        symbol_format: SymbolFormat {
            separator: "".to_string(),
            uppercase: true,
            custom_mappings: [
                ("BTC".to_string(), "XBT".to_string()),
            ].into_iter().collect(),
            base_prefix: "X".to_string(),
            quote_prefix: "Z".to_string(),
        },
        rate_limits: RateLimits {
            requests_per_second: 15,
            burst: 20,
            orders_per_second: 10,
            max_concurrent_orders: 10,
        },
        order_params: OrderParamsMapping {
            symbol_field: "pair".to_string(),
            side_field: "type".to_string(),
            type_field: "ordertype".to_string(),
            quantity_field: "volume".to_string(),
            price_field: "price".to_string(),
            client_id_field: Some("userref".to_string()),
            side_buy: "buy".to_string(),
            side_sell: "sell".to_string(),
            type_market: "market".to_string(),
            type_limit: "limit".to_string(),
        },
        requires_passphrase: false,
        trading_mode: TradingMode::default(),
        order_limits: OrderLimits {
            min_order_size: 0.0001,
            max_order_size: 500_000.0,
            default_tick_size: 0.1,
            max_batch_size: 50,
        },
    }
}

fn coinbase_definition() -> ExchangeDefinition {
    ExchangeDefinition {
        name: "Coinbase".to_string(),
        auth_method: AuthMethod::HmacSha256WithPassphrase {
            api_key_header: "CB-ACCESS-KEY".to_string(),
            signature_header: "CB-ACCESS-SIGN".to_string(),
            passphrase_header: "CB-ACCESS-PASSPHRASE".to_string(),
            timestamp_header: "CB-ACCESS-TIMESTAMP".to_string(),
        },
        endpoints: EndpointConfig {
            rest_url: "https://api.coinbase.com".to_string(),
            websocket_url: "wss://ws-feed.exchange.coinbase.com".to_string(),
            place_order_path: "/api/v3/brokerage/orders".to_string(),
            buy_order_path: None,
            sell_order_path: None,
            cancel_order_path: "/api/v3/brokerage/orders/batch_cancel".to_string(),
            order_status_path: "/api/v3/brokerage/orders/historical".to_string(),
            balance_path: "/api/v3/brokerage/accounts".to_string(),
            health_check_path: "/api/v3/brokerage/time".to_string(),
            place_order_method: "POST".to_string(),
            content_type: ContentType::Json,
        },
        symbol_format: SymbolFormat {
            separator: "-".to_string(),
            uppercase: true,
            custom_mappings: HashMap::new(),
            base_prefix: "".to_string(),
            quote_prefix: "".to_string(),
        },
        rate_limits: RateLimits {
            requests_per_second: 10,
            burst: 15,
            orders_per_second: 10,
            max_concurrent_orders: 10,
        },
        order_params: OrderParamsMapping {
            symbol_field: "product_id".to_string(),
            side_field: "side".to_string(),
            type_field: "order_type".to_string(),
            quantity_field: "base_size".to_string(),
            price_field: "limit_price".to_string(),
            client_id_field: Some("client_order_id".to_string()),
            side_buy: "BUY".to_string(),
            side_sell: "SELL".to_string(),
            type_market: "MARKET".to_string(),
            type_limit: "LIMIT".to_string(),
        },
        requires_passphrase: true,
        trading_mode: TradingMode::default(),
        order_limits: OrderLimits {
            min_order_size: 0.001,
            max_order_size: 10_000.0,
            default_tick_size: 0.01,
            max_batch_size: 100,
        },
    }
}

fn binance_us_definition() -> ExchangeDefinition {
    ExchangeDefinition {
        name: "Binance US".to_string(),
        auth_method: AuthMethod::HmacSha256 {
            api_key_header: "X-MBX-APIKEY".to_string(),
            signature_header: "signature".to_string(),
            timestamp_header: "timestamp".to_string(),
            timestamp_ms: true,
            signature_location: SignatureLocation::Query,
        },
        endpoints: EndpointConfig {
            rest_url: "https://api.binance.us".to_string(),
            websocket_url: "wss://stream.binance.us:9443/ws".to_string(),
            place_order_path: "/api/v3/order".to_string(),
            buy_order_path: None,
            sell_order_path: None,
            cancel_order_path: "/api/v3/order".to_string(),
            order_status_path: "/api/v3/order".to_string(),
            balance_path: "/api/v3/account".to_string(),
            health_check_path: "/api/v3/ping".to_string(),
            place_order_method: "POST".to_string(),
            content_type: ContentType::FormUrlEncoded,
        },
        symbol_format: SymbolFormat {
            separator: "".to_string(),
            uppercase: true,
            custom_mappings: HashMap::new(),
            base_prefix: "".to_string(),
            quote_prefix: "".to_string(),
        },
        rate_limits: RateLimits {
            requests_per_second: 10,
            burst: 100,
            orders_per_second: 10,
            max_concurrent_orders: 10,
        },
        order_params: OrderParamsMapping {
            symbol_field: "symbol".to_string(),
            side_field: "side".to_string(),
            type_field: "type".to_string(),
            quantity_field: "quantity".to_string(),
            price_field: "price".to_string(),
            client_id_field: Some("newClientOrderId".to_string()),
            side_buy: "BUY".to_string(),
            side_sell: "SELL".to_string(),
            type_market: "MARKET".to_string(),
            type_limit: "LIMIT".to_string(),
        },
        requires_passphrase: false,
        trading_mode: TradingMode::default(),
        order_limits: OrderLimits {
            min_order_size: 0.00001,
            max_order_size: 9_000_000.0,
            default_tick_size: 0.01,
            max_batch_size: 5,
        },
    }
}

fn binance_definition() -> ExchangeDefinition {
    let mut def = binance_us_definition();
    def.name = "Binance".to_string();
    def.endpoints.rest_url = "https://api.binance.com".to_string();
    def.endpoints.websocket_url = "wss://stream.binance.com:9443/ws".to_string();
    def.rate_limits.requests_per_second = 20;
    def.rate_limits.orders_per_second = 10;
    def.rate_limits.max_concurrent_orders = 10;
    def.order_limits.max_batch_size = 5;
    def
}

fn bybit_definition() -> ExchangeDefinition {
    ExchangeDefinition {
        name: "Bybit".to_string(),
        auth_method: AuthMethod::HmacSha256 {
            api_key_header: "X-BAPI-API-KEY".to_string(),
            signature_header: "X-BAPI-SIGN".to_string(),
            timestamp_header: "X-BAPI-TIMESTAMP".to_string(),
            timestamp_ms: true,
            signature_location: SignatureLocation::Header,
        },
        endpoints: EndpointConfig {
            rest_url: "https://api.bybit.com".to_string(),
            websocket_url: "wss://stream.bybit.com/v5/private".to_string(),
            place_order_path: "/v5/order/create".to_string(),
            buy_order_path: None,
            sell_order_path: None,
            cancel_order_path: "/v5/order/cancel".to_string(),
            order_status_path: "/v5/order/realtime".to_string(),
            balance_path: "/v5/account/wallet-balance".to_string(),
            health_check_path: "/v5/market/time".to_string(),
            place_order_method: "POST".to_string(),
            content_type: ContentType::Json,
        },
        symbol_format: SymbolFormat {
            separator: "".to_string(),
            uppercase: true,
            custom_mappings: HashMap::new(),
            base_prefix: "".to_string(),
            quote_prefix: "".to_string(),
        },
        rate_limits: RateLimits {
            requests_per_second: 10,
            burst: 20,
            orders_per_second: 10,
            max_concurrent_orders: 10,
        },
        order_params: OrderParamsMapping {
            symbol_field: "symbol".to_string(),
            side_field: "side".to_string(),
            type_field: "orderType".to_string(),
            quantity_field: "qty".to_string(),
            price_field: "price".to_string(),
            client_id_field: Some("orderLinkId".to_string()),
            side_buy: "Buy".to_string(),
            side_sell: "Sell".to_string(),
            type_market: "Market".to_string(),
            type_limit: "Limit".to_string(),
        },
        requires_passphrase: false,
        trading_mode: TradingMode {
            category: "spot".to_string(),
            mode: "cash".to_string(),
            extra_params: HashMap::new(),
        },
        order_limits: OrderLimits {
            min_order_size: 0.000001,
            max_order_size: 200.0,
            default_tick_size: 0.01,
            max_batch_size: 20,
        },
    }
}

fn okx_definition() -> ExchangeDefinition {
    ExchangeDefinition {
        name: "OKX".to_string(),
        auth_method: AuthMethod::HmacSha256WithPassphrase {
            api_key_header: "OK-ACCESS-KEY".to_string(),
            signature_header: "OK-ACCESS-SIGN".to_string(),
            passphrase_header: "OK-ACCESS-PASSPHRASE".to_string(),
            timestamp_header: "OK-ACCESS-TIMESTAMP".to_string(),
        },
        endpoints: EndpointConfig {
            rest_url: "https://www.okx.com".to_string(),
            websocket_url: "wss://ws.okx.com:8443/ws/v5/private".to_string(),
            place_order_path: "/api/v5/trade/order".to_string(),
            buy_order_path: None,
            sell_order_path: None,
            cancel_order_path: "/api/v5/trade/cancel-order".to_string(),
            order_status_path: "/api/v5/trade/order".to_string(),
            balance_path: "/api/v5/account/balance".to_string(),
            health_check_path: "/api/v5/public/time".to_string(),
            place_order_method: "POST".to_string(),
            content_type: ContentType::Json,
        },
        symbol_format: SymbolFormat {
            separator: "-".to_string(),
            uppercase: true,
            custom_mappings: HashMap::new(),
            base_prefix: "".to_string(),
            quote_prefix: "".to_string(),
        },
        rate_limits: RateLimits {
            requests_per_second: 20,
            burst: 60,
            orders_per_second: 20,
            max_concurrent_orders: 20,
        },
        order_params: OrderParamsMapping {
            symbol_field: "instId".to_string(),
            side_field: "side".to_string(),
            type_field: "ordType".to_string(),
            quantity_field: "sz".to_string(),
            price_field: "px".to_string(),
            client_id_field: Some("clOrdId".to_string()),
            side_buy: "buy".to_string(),
            side_sell: "sell".to_string(),
            type_market: "market".to_string(),
            type_limit: "limit".to_string(),
        },
        requires_passphrase: true,
        trading_mode: TradingMode {
            category: "SPOT".to_string(),
            mode: "cash".to_string(),
            extra_params: HashMap::new(),
        },
        order_limits: OrderLimits {
            min_order_size: 0.00001,
            max_order_size: 100_000.0,
            default_tick_size: 0.01,
            max_batch_size: 20,
        },
    }
}

fn gemini_definition() -> ExchangeDefinition {
    ExchangeDefinition {
        name: "Gemini".to_string(),
        auth_method: AuthMethod::HmacSha512 {
            api_key_header: "X-GEMINI-APIKEY".to_string(),
            signature_header: "X-GEMINI-SIGNATURE".to_string(),
            use_nonce: true,
        },
        endpoints: EndpointConfig {
            rest_url: "https://api.gemini.com".to_string(),
            websocket_url: "wss://api.gemini.com/v1/order/events".to_string(),
            place_order_path: "/v1/order/new".to_string(),
            buy_order_path: None,
            sell_order_path: None,
            cancel_order_path: "/v1/order/cancel".to_string(),
            order_status_path: "/v1/order/status".to_string(),
            balance_path: "/v1/balances".to_string(),
            health_check_path: "/v1/symbols".to_string(),
            place_order_method: "POST".to_string(),
            content_type: ContentType::Json,
        },
        symbol_format: SymbolFormat {
            separator: "".to_string(),
            uppercase: false, // Gemini uses lowercase
            custom_mappings: HashMap::new(),
            base_prefix: "".to_string(),
            quote_prefix: "".to_string(),
        },
        rate_limits: RateLimits {
            requests_per_second: 5,
            burst: 10,
            orders_per_second: 5,
            max_concurrent_orders: 5,
        },
        order_params: OrderParamsMapping {
            symbol_field: "symbol".to_string(),
            side_field: "side".to_string(),
            type_field: "type".to_string(),
            quantity_field: "amount".to_string(),
            price_field: "price".to_string(),
            client_id_field: Some("client_order_id".to_string()),
            side_buy: "buy".to_string(),
            side_sell: "sell".to_string(),
            type_market: "exchange market".to_string(),
            type_limit: "exchange limit".to_string(),
        },
        requires_passphrase: false,
        trading_mode: TradingMode::default(),
        order_limits: OrderLimits {
            min_order_size: 0.00001,
            max_order_size: 10_000.0,
            default_tick_size: 0.01,
            max_batch_size: 50,
        },
    }
}

fn deribit_definition() -> ExchangeDefinition {
    ExchangeDefinition {
        name: "Deribit".to_string(),
        auth_method: AuthMethod::HmacSha256 {
            api_key_header: "Authorization".to_string(),
            signature_header: "sig".to_string(),
            timestamp_header: "timestamp".to_string(),
            timestamp_ms: true,
            signature_location: SignatureLocation::Query,
        },
        endpoints: EndpointConfig {
            rest_url: "https://www.deribit.com".to_string(),
            websocket_url: "wss://www.deribit.com/ws/api/v2".to_string(),
            place_order_path: "/api/v2/private/buy".to_string(), // default to buy, use buy_order_path/sell_order_path
            buy_order_path: Some("/api/v2/private/buy".to_string()),
            sell_order_path: Some("/api/v2/private/sell".to_string()),
            cancel_order_path: "/api/v2/private/cancel".to_string(),
            order_status_path: "/api/v2/private/get_order_state".to_string(),
            balance_path: "/api/v2/private/get_account_summary".to_string(),
            health_check_path: "/api/v2/public/test".to_string(),
            place_order_method: "GET".to_string(), // Deribit uses GET with query params
            content_type: ContentType::FormUrlEncoded,
        },
        symbol_format: SymbolFormat {
            separator: "-".to_string(),
            uppercase: true,
            custom_mappings: HashMap::new(),
            base_prefix: "".to_string(),
            quote_prefix: "".to_string(),
        },
        rate_limits: RateLimits {
            requests_per_second: 20,
            burst: 50,
            orders_per_second: 20,
            max_concurrent_orders: 20,
        },
        order_params: OrderParamsMapping {
            symbol_field: "instrument_name".to_string(),
            side_field: "".to_string(), // Deribit uses separate /buy and /sell endpoints
            type_field: "type".to_string(),
            quantity_field: "amount".to_string(),
            price_field: "price".to_string(),
            client_id_field: Some("label".to_string()),
            side_buy: "buy".to_string(),
            side_sell: "sell".to_string(),
            type_market: "market".to_string(),
            type_limit: "limit".to_string(),
        },
        requires_passphrase: false,
        trading_mode: TradingMode {
            category: "future".to_string(),
            mode: "".to_string(),
            extra_params: HashMap::new(),
        },
        order_limits: OrderLimits {
            min_order_size: 10.0, // Deribit uses contract size, min 10 USD
            max_order_size: 10_000_000.0,
            default_tick_size: 0.5,
            max_batch_size: 20,
        },
    }
}
