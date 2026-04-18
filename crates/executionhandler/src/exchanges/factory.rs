use std::collections::HashMap;
use log::{info, warn, error, debug};
use crate::core::{ExchangeConnector, ExchangeConfig, ExecutionError};
use crate::exchanges::kraken::KrakenConnector;
use crate::exchanges::generic::{GenericConnector, ExchangePreset};
use crate::paper_connector::{PaperTradingConnector, PaperTradingConfig};
use smartorderrouter::ExchangeCredential;

/// Factory for creating exchange connectors
pub struct ExchangeFactory;

impl ExchangeFactory {
    /// Create a connector for the specified exchange
    /// 
    /// Supports all 8 exchanges via the GenericConnector:
    /// - kraken, coinbase, binance, binance_us, bybit, okx, gemini, deribit
    pub async fn create_connector(exchange_name: &str, config: ExchangeConfig) -> Result<Box<dyn ExchangeConnector>, ExecutionError> {
        info!(
            "[FACTORY] Creating connector: exchange={}, sandbox={}, pool_size={}, timeout_ms={}",
            exchange_name, config.sandbox, config.connection_pool_size, config.timeout_ms
        );
        
        let exchange_lower = exchange_name.to_lowercase();
        
        // Paper trading connector — no real exchange needed
        // Matches "paper" or "paper_<instance_id>" for per-deployment isolation
        if exchange_lower == "paper" || exchange_lower.starts_with("paper_") {
            info!("[FACTORY] Creating paper trading connector for: {}", exchange_name);
            let connector = PaperTradingConnector::new(PaperTradingConfig::default());
            return Ok(Box::new(connector));
        }
        
        // Check if this is a supported exchange using the generic connector
        if let Some(preset) = ExchangePreset::from_name(&exchange_lower) {
            debug!("[FACTORY] Using GenericConnector for {} (preset: {:?})", exchange_name, preset);
            
            let mut connector = GenericConnector::new(preset);
            match connector.initialize(config).await {
                Ok(_) => {
                    info!("[FACTORY] {} connector created successfully via GenericConnector", exchange_name);
                    Ok(Box::new(connector))
                }
                Err(e) => {
                    error!("[FACTORY] Failed to initialize {} connector: {}", exchange_name, e);
                    Err(e)
                }
            }
        } else {
            warn!("[FACTORY] Unsupported exchange requested: {}", exchange_name);
            Err(ExecutionError::Unknown(format!(
                "Unsupported exchange: {}. Supported exchanges: {}",
                exchange_name,
                Self::supported_exchanges().join(", ")
            )))
        }
    }
    
    /// Create a connector using the legacy Kraken-specific implementation
    /// This is kept for backward compatibility and testing
    #[deprecated(note = "Use create_connector with exchange name 'kraken' instead")]
    pub async fn create_kraken_connector(config: ExchangeConfig) -> Result<Box<dyn ExchangeConnector>, ExecutionError> {
        debug!("[FACTORY] Using legacy KrakenConnector");
        let mut connector = KrakenConnector::new();
        match connector.initialize(config).await {
            Ok(_) => {
                info!("[FACTORY] Legacy Kraken connector created successfully");
                Ok(Box::new(connector))
            }
            Err(e) => {
                error!("[FACTORY] Failed to initialize legacy Kraken connector: {}", e);
                Err(e)
            }
        }
    }

    /// Get list of supported exchanges
    pub fn supported_exchanges() -> Vec<&'static str> {
        vec![
            "kraken",
            "coinbase", 
            "binance",
            "binance_us",
            "bybit",
            "okx",
            "gemini",
            "deribit",
            "paper",
        ]
    }

    /// Create configuration template for an exchange
    pub fn create_config_template(exchange_name: &str) -> Result<ExchangeConfig, ExecutionError> {
        let exchange_lower = exchange_name.to_lowercase();
        
        // Get the preset to retrieve exchange-specific info
        let preset = ExchangePreset::from_name(&exchange_lower)
            .ok_or_else(|| ExecutionError::Unknown(format!(
                "Unsupported exchange: {}. Supported: {}",
                exchange_name,
                Self::supported_exchanges().join(", ")
            )))?;
        
        let definition = preset.definition();
        let env_prefix = exchange_lower.to_uppercase().replace("_", "");
        
        Ok(ExchangeConfig {
            name: definition.name.clone(),
            api_key: std::env::var(format!("{}_API_KEY", env_prefix))
                .map_err(|_| ExecutionError::Authentication(
                    format!("{}_API_KEY environment variable not set", env_prefix)
                ))?,
            secret_key: std::env::var(format!("{}_SECRET_KEY", env_prefix))
                .map_err(|_| ExecutionError::Authentication(
                    format!("{}_SECRET_KEY environment variable not set", env_prefix)
                ))?,
            passphrase: if definition.requires_passphrase {
                Some(std::env::var(format!("{}_PASSPHRASE", env_prefix))
                    .map_err(|_| ExecutionError::Authentication(
                        format!("{}_PASSPHRASE environment variable not set (required for {})", 
                            env_prefix, definition.name)
                    ))?)
            } else {
                std::env::var(format!("{}_PASSPHRASE", env_prefix)).ok()
            },
            sandbox: std::env::var(format!("{}_SANDBOX", env_prefix))
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            connection_pool_size: 10,
            timeout_ms: 5000,
            rate_limit_per_second: definition.rate_limits.requests_per_second,
            rate_limit_burst: definition.rate_limits.burst,
            websocket_url: Some(definition.endpoints.websocket_url.clone()),
            rest_api_url: Some(definition.endpoints.rest_url.clone()),
            custom_headers: HashMap::new(),
        })
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
        let config_name_lower = config.name.to_lowercase();
        
        // Check if exchange requires passphrase
        if let Some(preset) = ExchangePreset::from_name(&config_name_lower) {
            let definition = preset.definition();
            if definition.requires_passphrase && config.passphrase.is_none() {
                return Err(ExecutionError::Validation(
                    format!("{} requires a passphrase", definition.name)
                ));
            }
        }
        
        Ok(())
    }
    
    /// Create a connector from database-stored credentials
    /// 
    /// This is the primary method for production use - loads credentials from the
    /// database (stored via the UI) and creates a properly configured connector.
    pub async fn create_connector_from_credential(
        credential: &ExchangeCredential,
    ) -> Result<Box<dyn ExchangeConnector>, ExecutionError> {
        info!(
            "[FACTORY] Creating connector from database credential: exchange={}, label={}, testnet={}",
            credential.exchange, credential.label, credential.is_testnet
        );
        
        // Map exchange name to get the preset
        let exchange_lower = credential.exchange.to_lowercase();
        let preset = ExchangePreset::from_name(&exchange_lower)
            .ok_or_else(|| ExecutionError::Unknown(format!(
                "Unsupported exchange: {}. Supported: {}",
                credential.exchange,
                Self::supported_exchanges().join(", ")
            )))?;
        
        let definition = preset.definition();
        
        // Build config from credential
        let config = ExchangeConfig {
            name: credential.exchange.clone(),
            api_key: credential.api_key.clone(),
            secret_key: credential.api_secret.clone(),
            passphrase: credential.passphrase.clone(),
            sandbox: credential.is_testnet,
            connection_pool_size: 10,
            timeout_ms: 5000,
            rate_limit_per_second: definition.rate_limits.requests_per_second,
            rate_limit_burst: definition.rate_limits.burst,
            websocket_url: Some(definition.endpoints.websocket_url.clone()),
            rest_api_url: Some(definition.endpoints.rest_url.clone()),
            custom_headers: HashMap::new(),
        };
        
        // Validate the config
        Self::validate_config(&config)?;
        
        // Create the connector using the generic connector
        let mut connector = GenericConnector::new(preset);
        match connector.initialize(config).await {
            Ok(_) => {
                info!(
                    "[FACTORY] {} connector created successfully from credential '{}'", 
                    credential.exchange, credential.label
                );
                Ok(Box::new(connector))
            }
            Err(e) => {
                error!(
                    "[FACTORY] Failed to initialize {} connector from credential '{}': {}", 
                    credential.exchange, credential.label, e
                );
                Err(e)
            }
        }
    }
}
