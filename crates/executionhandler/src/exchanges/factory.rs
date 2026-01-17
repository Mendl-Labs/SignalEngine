use std::collections::HashMap;
use log::{info, warn, error, debug};
use crate::core::{ExchangeConnector, ExchangeConfig, ExecutionError};
use crate::exchanges::{kraken::KrakenConnector};

/// Factory for creating exchange connectors
pub struct ExchangeFactory;

impl ExchangeFactory {
    /// Create a connector for the specified exchange
    pub async fn create_connector(exchange_name: &str, config: ExchangeConfig) -> Result<Box<dyn ExchangeConnector>, ExecutionError> {
        info!(
            "[FACTORY] Creating connector: exchange={}, sandbox={}, pool_size={}, timeout_ms={}",
            exchange_name, config.sandbox, config.connection_pool_size, config.timeout_ms
        );
        
        match exchange_name.to_lowercase().as_str() {
            "kraken" => {
                debug!("[FACTORY] Initializing Kraken connector");
                let mut connector = KrakenConnector::new();
                match connector.initialize(config).await {
                    Ok(_) => {
                        info!("[FACTORY] Kraken connector created successfully");
                        Ok(Box::new(connector))
                    }
                    Err(e) => {
                        error!("[FACTORY] Failed to initialize Kraken connector: {}", e);
                        Err(e)
                    }
                }
            }
            _ => {
                warn!("[FACTORY] Unsupported exchange requested: {}", exchange_name);
                Err(ExecutionError::Unknown(format!("Unsupported exchange: {}. Only Kraken is currently supported.", exchange_name)))
            }
        }
    }

    /// Get list of supported exchanges
    pub fn supported_exchanges() -> Vec<&'static str> {
        vec!["kraken"]
    }

    /// Create configuration template for an exchange
    pub fn create_config_template(exchange_name: &str) -> Result<ExchangeConfig, ExecutionError> {
        match exchange_name.to_lowercase().as_str() {
            "kraken" => Ok(ExchangeConfig {
                name: "Kraken".to_string(),
                api_key: std::env::var("KRAKEN_API_KEY")
                    .map_err(|_| ExecutionError::Authentication("KRAKEN_API_KEY environment variable not set".to_string()))?,
                secret_key: std::env::var("KRAKEN_SECRET_KEY")
                    .map_err(|_| ExecutionError::Authentication("KRAKEN_SECRET_KEY environment variable not set".to_string()))?,
                passphrase: None,
                sandbox: std::env::var("KRAKEN_SANDBOX").unwrap_or_else(|_| "true".to_string()).parse().unwrap_or(true),
                connection_pool_size: 10,
                timeout_ms: 5000,
                rate_limit_per_second: 20,
                rate_limit_burst: 60,
                websocket_url: Some("wss://ws.kraken.com".to_string()),
                rest_api_url: Some("https://api.kraken.com".to_string()),
                custom_headers: HashMap::new(),
            }),
            _ => Err(ExecutionError::Unknown(format!("Unsupported exchange: {}. Only Kraken is currently supported.", exchange_name)))
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
