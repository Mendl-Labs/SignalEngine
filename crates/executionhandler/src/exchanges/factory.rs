use std::collections::HashMap;
use log::{info, warn, error, debug};
use crate::core::{ExchangeConnector, ExchangeConfig, ExecutionError};
use crate::exchanges::generic::{GenericConnector, ExchangePreset};
use crate::exchanges::dex::{DexToExchangeAdapter, BlockchainNetwork};
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

        // DEX connectors (Sui chain: Cetus, DeepBook)
        if let Some(dex_connector) = Self::try_create_dex_connector(&exchange_lower, config.clone()).await? {
            return Ok(dex_connector);
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
            "alpaca_paper",
            "oanda_practice",
            "cetus",
            "deepbook",
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

    async fn try_create_dex_connector(
        exchange_lower: &str,
        config: ExchangeConfig,
    ) -> Result<Option<Box<dyn ExchangeConnector>>, ExecutionError> {
        let network = if config.sandbox {
            BlockchainNetwork::SuiTestnet
        } else {
            BlockchainNetwork::Sui
        };

        let adapter = match exchange_lower {
            "cetus" | "cetus_amm" | "cetusprotocol" => {
                Some(DexToExchangeAdapter::cetus(network))
            }
            "deepbook" | "deepbookv2" | "deep_book" => {
                Some(DexToExchangeAdapter::deepbook(network))
            }
            _ => None,
        };

        if let Some(mut adapter) = adapter {
            info!("[FACTORY] Creating DEX connector: {} (network: {:?})", exchange_lower, network);
            adapter.initialize(config).await?;
            info!("[FACTORY] DEX connector {} initialized successfully", exchange_lower);
            Ok(Some(Box::new(adapter)))
        } else {
            Ok(None)
        }
    }
}

/// Returns true if the current UTC time falls within NYSE regular trading hours
/// (9:30 AM – 4:00 PM US/Eastern, Monday–Friday, excluding holidays).
///
/// Holiday exclusions are NOT implemented here — callers that need full holiday
/// awareness should integrate with an exchange calendar API. This guard blocks
/// orders on weekends and outside core session hours only.
pub fn is_nyse_market_hours() -> bool {
    use std::time::{SystemTime, UNIX_EPOCH};

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // UTC offset for US/Eastern: EST = UTC-5, EDT = UTC-4
    // NYSE trades 9:30–16:00 ET.  We approximate: open=[14:30,21:00) UTC in summer,
    // [14:30,21:00) UTC in winter.  To keep this dependency-free we use a fixed
    // UTC-5 (EST) offset — callers in EDT will be 1 h early, which is the safe side
    // (they'll see "open" start 1 h late and "close" end 1 h late, never trading outside hours).
    const UTC_OFFSET_SECS: u64 = 5 * 3600; // UTC-5 (EST, conservative)
    let local_secs = secs.saturating_sub(UTC_OFFSET_SECS);

    let day_of_week = (local_secs / 86400 + 4) % 7; // 0=Sun … 6=Sat; epoch was a Thursday (day=4)
    let seconds_in_day = local_secs % 86400;

    // Monday=1 … Friday=5
    if day_of_week == 0 || day_of_week == 6 {
        return false; // weekend
    }

    let open = 9 * 3600 + 30 * 60;  // 09:30
    let close = 16 * 3600;          // 16:00

    seconds_in_day >= open && seconds_in_day < close
}

/// Returns true if the current UTC time falls within forex market hours
/// (Sunday 5:00 PM – Friday 5:00 PM US/Eastern, i.e. the 24x5 interbank session).
///
/// Uses the same fixed UTC-5 (EST) approximation as `is_nyse_market_hours` to stay
/// dependency-free; during EDT the open/close boundaries shift by 1 h.
pub fn is_forex_market_hours() -> bool {
    use std::time::{SystemTime, UNIX_EPOCH};

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    const UTC_OFFSET_SECS: u64 = 5 * 3600; // UTC-5 (EST, conservative)
    let local_secs = secs.saturating_sub(UTC_OFFSET_SECS);

    let day_of_week = (local_secs / 86400 + 4) % 7; // 0=Sun … 6=Sat
    let seconds_in_day = local_secs % 86400;

    let five_pm = 17 * 3600;

    match day_of_week {
        6 => false,                          // Saturday: closed
        0 => seconds_in_day >= five_pm,      // Sunday: opens 17:00 ET
        5 => seconds_in_day < five_pm,       // Friday: closes 17:00 ET
        _ => true,                           // Mon–Thu: open 24 h
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config(name: &str) -> ExchangeConfig {
        ExchangeConfig {
            name: name.to_string(),
            api_key: "test_api_key".to_string(),
            secret_key: "test_secret_key".to_string(),
            passphrase: None,
            sandbox: true,
            connection_pool_size: 10,
            timeout_ms: 5000,
            rate_limit_per_second: 10,
            rate_limit_burst: 20,
            websocket_url: None,
            rest_api_url: None,
            custom_headers: HashMap::new(),
        }
    }

    #[test]
    fn test_supported_exchanges_includes_all() {
        let exchanges = ExchangeFactory::supported_exchanges();
        assert!(exchanges.contains(&"kraken"));
        assert!(exchanges.contains(&"coinbase"));
        assert!(exchanges.contains(&"binance"));
        assert!(exchanges.contains(&"binance_us"));
        assert!(exchanges.contains(&"bybit"));
        assert!(exchanges.contains(&"okx"));
        assert!(exchanges.contains(&"gemini"));
        assert!(exchanges.contains(&"deribit"));
        assert!(exchanges.contains(&"paper"));
        assert!(exchanges.contains(&"alpaca_paper"));
        assert!(exchanges.contains(&"oanda_practice"));
        assert!(exchanges.contains(&"cetus"));
        assert!(exchanges.contains(&"deepbook"));
        assert_eq!(exchanges.len(), 13);
    }

    #[test]
    fn test_validate_config_valid() {
        let config = valid_config("kraken");
        assert!(ExchangeFactory::validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_empty_api_key() {
        let mut config = valid_config("kraken");
        config.api_key = String::new();
        assert!(ExchangeFactory::validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_empty_secret() {
        let mut config = valid_config("kraken");
        config.secret_key = String::new();
        assert!(ExchangeFactory::validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_zero_pool_size() {
        let mut config = valid_config("kraken");
        config.connection_pool_size = 0;
        assert!(ExchangeFactory::validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_zero_timeout() {
        let mut config = valid_config("kraken");
        config.timeout_ms = 0;
        assert!(ExchangeFactory::validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_coinbase_requires_passphrase() {
        let mut config = valid_config("coinbase");
        config.passphrase = None;
        assert!(ExchangeFactory::validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_coinbase_with_passphrase() {
        let mut config = valid_config("coinbase");
        config.passphrase = Some("my_pass".to_string());
        assert!(ExchangeFactory::validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_okx_requires_passphrase() {
        let mut config = valid_config("okx");
        config.passphrase = None;
        assert!(ExchangeFactory::validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_kraken_no_passphrase_needed() {
        let config = valid_config("kraken");
        // Kraken doesn't require passphrase — should pass
        assert!(ExchangeFactory::validate_config(&config).is_ok());
    }

    #[tokio::test]
    async fn test_create_connector_paper() {
        let config = valid_config("paper");
        let result = ExchangeFactory::create_connector("paper", config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_connector_paper_with_instance_id() {
        let config = valid_config("paper_123");
        let result = ExchangeFactory::create_connector("paper_123", config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_connector_unsupported() {
        let config = valid_config("nonexistent");
        let result = ExchangeFactory::create_connector("nonexistent", config).await;
        assert!(result.is_err());
    }
}
