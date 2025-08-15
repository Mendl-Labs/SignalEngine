use async_trait::async_trait;
use std::collections::HashMap;
use crate::core::{ExchangeConnector, ExchangeConfig, ExecutionError};
use crate::exchanges::{kraken::KrakenConnector, binance::BinanceConnector, coinbase::CoinbaseConnector};

/// Factory for creating exchange connectors
pub struct ExchangeFactory;

impl ExchangeFactory {
    /// Create a connector for the specified exchange
    pub async fn create_connector(exchange_name: &str, config: ExchangeConfig) -> Result<Box<dyn ExchangeConnector>, ExecutionError> {
        match exchange_name.to_lowercase().as_str() {
            "kraken" => {
                let mut connector = KrakenConnector::new();
                connector.initialize(config).await?;
                Ok(Box::new(connector))
            }
            "binance" => {
                let mut connector = BinanceConnector::new();
                connector.initialize(config).await?;
                Ok(Box::new(connector))
            }
            "coinbase" | "coinbase_pro" => {
                let mut connector = CoinbaseConnector::new();
                connector.initialize(config).await?;
                Ok(Box::new(connector))
            }
            _ => Err(ExecutionError::Unknown(format!("Unsupported exchange: {}", exchange_name)))
        }
    }

    /// Get list of supported exchanges
    pub fn supported_exchanges() -> Vec<&'static str> {
        vec!["kraken", "binance", "coinbase"]
    }

    /// Create configuration template for an exchange
    pub fn create_config_template(exchange_name: &str) -> Result<ExchangeConfig, ExecutionError> {
        match exchange_name.to_lowercase().as_str() {
            "kraken" => Ok(ExchangeConfig {
                name: "Kraken".to_string(),
                api_key: "YOUR_KRAKEN_API_KEY".to_string(),
                secret_key: "YOUR_KRAKEN_SECRET_KEY".to_string(),
                passphrase: None,
                sandbox: true,
                connection_pool_size: 10,
                timeout_ms: 5000,
                rate_limit_per_second: 20,
                rate_limit_burst: 60,
                websocket_url: Some("wss://ws.kraken.com".to_string()),
                rest_api_url: Some("https://api.kraken.com".to_string()),
                custom_headers: HashMap::new(),
            }),
            "binance" => Ok(ExchangeConfig {
                name: "Binance".to_string(),
                api_key: "YOUR_BINANCE_API_KEY".to_string(),
                secret_key: "YOUR_BINANCE_SECRET_KEY".to_string(),
                passphrase: None,
                sandbox: true,
                connection_pool_size: 15,
                timeout_ms: 3000,
                rate_limit_per_second: 100,
                rate_limit_burst: 200,
                websocket_url: Some("wss://stream.binance.com:9443".to_string()),
                rest_api_url: Some("https://api.binance.com".to_string()),
                custom_headers: HashMap::new(),
            }),
            "coinbase" => Ok(ExchangeConfig {
                name: "Coinbase Pro".to_string(),
                api_key: "YOUR_COINBASE_API_KEY".to_string(),
                secret_key: "YOUR_COINBASE_SECRET_KEY".to_string(),
                passphrase: Some("YOUR_COINBASE_PASSPHRASE".to_string()),
                sandbox: true,
                connection_pool_size: 8,
                timeout_ms: 4000,
                rate_limit_per_second: 10,
                rate_limit_burst: 30,
                websocket_url: Some("wss://ws-feed.pro.coinbase.com".to_string()),
                rest_api_url: Some("https://api.pro.coinbase.com".to_string()),
                custom_headers: HashMap::new(),
            }),
            _ => Err(ExecutionError::Unknown(format!("Unsupported exchange: {}", exchange_name)))
        }
    }

    /// Validate exchange configuration
    pub fn validate_config(config: &ExchangeConfig) -> Result<(), ExecutionError> {
        if config.api_key.is_empty() {
            return Err(ExecutionError::Validation("API key is required".to_string()));
        }
        
        if config.secret_key.is_empty() {
            return Err(ExecutionError::Validation("Secret key is required".to_string()));
        }
        
        if config.connection_pool_size == 0 {
            return Err(ExecutionError::Validation("Connection pool size must be > 0".to_string()));
        }
        
        if config.timeout_ms == 0 {
            return Err(ExecutionError::Validation("Timeout must be > 0".to_string()));
        }
        
        // Exchange-specific validation
        match config.name.to_lowercase().as_str() {
            "coinbase" | "coinbase_pro" => {
                if config.passphrase.is_none() {
                    return Err(ExecutionError::Validation("Coinbase requires passphrase".to_string()));
                }
            }
            _ => {}
        }
        
        Ok(())
    }
}
