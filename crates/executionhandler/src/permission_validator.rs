//! Exchange API Key Permission Validator
//!
//! Validates user-provided API keys by calling each exchange's permission
//! check endpoint to ensure:
//! 1. The key is valid and works
//! 2. The key has trading permissions
//! 3. The key does NOT have withdrawal permissions (CRITICAL for SaaS)
//!
//! Each exchange has different APIs for checking permissions:
//! - Some have explicit permission endpoints
//! - Some require checking account info
//! - Some we infer from error responses

use std::collections::HashMap;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use anyhow::{Result, anyhow, bail};
use reqwest::Client;

use crate::exchanges::generic::{
    GenericConnector, ExchangePreset, ExchangeDefinition,
    AuthStrategy, create_auth_strategy, AuthHeaders,
};

/// Result of API key permission validation
#[derive(Debug, Clone, Serialize)]
pub struct PermissionValidation {
    /// Exchange name
    pub exchange: String,
    /// Whether the key is valid and working
    pub is_valid: bool,
    /// Whether we successfully verified permissions with the exchange
    pub verified_with_exchange: bool,
    /// Detected permissions
    pub permissions: DetectedPermissions,
    /// Error messages if validation failed
    pub errors: Vec<String>,
    /// Warning messages (non-fatal)
    pub warnings: Vec<String>,
    /// Raw permission data from exchange (for debugging)
    pub raw_permissions: Option<serde_json::Value>,
}

/// Detected permission levels
#[derive(Debug, Clone, Default, Serialize)]
pub struct DetectedPermissions {
    /// Can read account balances and history
    pub can_read: bool,
    /// Can place spot orders
    pub can_trade_spot: bool,
    /// Can trade futures/derivatives
    pub can_trade_futures: bool,
    /// Can trade margin
    pub can_trade_margin: bool,
    /// CAN WITHDRAW - THIS MUST BE FALSE FOR SAAS
    pub can_withdraw: bool,
    /// Can transfer between accounts
    pub can_transfer: bool,
    /// Can access API key management
    pub can_manage_keys: bool,
    /// Raw permission strings from exchange
    pub raw_permissions: Vec<String>,
}

impl DetectedPermissions {
    /// Check if key has any dangerous permissions
    pub fn has_dangerous_permissions(&self) -> bool {
        self.can_withdraw || self.can_transfer || self.can_manage_keys
    }

    /// Get list of dangerous permissions detected
    pub fn dangerous_permissions_list(&self) -> Vec<&'static str> {
        let mut dangerous = vec![];
        if self.can_withdraw {
            dangerous.push("WITHDRAW");
        }
        if self.can_transfer {
            dangerous.push("TRANSFER");
        }
        if self.can_manage_keys {
            dangerous.push("API_KEY_MANAGEMENT");
        }
        dangerous
    }
}

/// API Key Permission Validator
pub struct PermissionValidator {
    client: Client,
}

impl PermissionValidator {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Validate API key permissions for any supported exchange
    pub async fn validate(
        &self,
        exchange: &str,
        api_key: &str,
        api_secret: &str,
        passphrase: Option<&str>,
    ) -> Result<PermissionValidation> {
        let exchange_lower = exchange.to_lowercase();
        
        match exchange_lower.as_str() {
            "kraken" => self.validate_kraken(api_key, api_secret).await,
            "coinbase" => self.validate_coinbase(api_key, api_secret, passphrase).await,
            "binance" | "binance_us" => self.validate_binance(api_key, api_secret, &exchange_lower).await,
            "bybit" => self.validate_bybit(api_key, api_secret).await,
            "okx" => self.validate_okx(api_key, api_secret, passphrase).await,
            "gemini" => self.validate_gemini(api_key, api_secret).await,
            "deribit" => self.validate_deribit(api_key, api_secret).await,
            _ => bail!("Unsupported exchange: {}", exchange),
        }
    }

    // ========================================================================
    // KRAKEN
    // ========================================================================
    
    /// Validate Kraken API key
    /// Kraken returns permissions in the QueryPrivateKey endpoint
    async fn validate_kraken(
        &self,
        api_key: &str,
        api_secret: &str,
    ) -> Result<PermissionValidation> {
        let mut validation = PermissionValidation {
            exchange: "kraken".to_string(),
            is_valid: false,
            verified_with_exchange: false,
            permissions: DetectedPermissions::default(),
            errors: vec![],
            warnings: vec![],
            raw_permissions: None,
        };

        // Kraken uses POST for all private endpoints
        let url = "https://api.kraken.com/0/private/GetWebSocketsToken";
        let nonce = chrono::Utc::now().timestamp_millis().to_string();
        let post_data = format!("nonce={}", nonce);

        // Create signature
        let path = "/0/private/GetWebSocketsToken";
        let signature = self.kraken_signature(path, &nonce, &post_data, api_secret)?;

        let response = self.client
            .post(url)
            .header("API-Key", api_key)
            .header("API-Sign", &signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        validation.raw_permissions = Some(body.clone());

        // Check for errors
        if let Some(errors) = body.get("error").and_then(|e| e.as_array()) {
            if !errors.is_empty() {
                let error_msgs: Vec<String> = errors
                    .iter()
                    .filter_map(|e| e.as_str().map(|s| s.to_string()))
                    .collect();
                
                // Check for permission-specific errors
                for err in &error_msgs {
                    if err.contains("EAPI:Invalid key") {
                        validation.errors.push("Invalid API key".to_string());
                        return Ok(validation);
                    }
                    if err.contains("EAPI:Invalid signature") {
                        validation.errors.push("Invalid API secret".to_string());
                        return Ok(validation);
                    }
                }
                
                validation.errors.extend(error_msgs);
                return Ok(validation);
            }
        }

        // If we got a token, the key is valid for at least websocket access
        validation.is_valid = true;
        validation.verified_with_exchange = true;
        validation.permissions.can_read = true;

        // Now query balance to check trade permissions
        let balance_result = self.kraken_check_balance(api_key, api_secret).await;
        if balance_result.is_ok() {
            validation.permissions.can_trade_spot = true;
        }

        // Kraken doesn't have a direct permission query, so we check if withdrawal works
        // by attempting to get withdrawal info (not actually withdrawing)
        let withdraw_info = self.kraken_check_withdraw_permission(api_key, api_secret).await;
        if withdraw_info.is_ok() {
            validation.permissions.can_withdraw = true;
            validation.errors.push(
                "SECURITY RISK: This API key has withdrawal permissions. \
                Please create a new key with only trading permissions.".to_string()
            );
        }

        Ok(validation)
    }

    fn kraken_signature(&self, path: &str, nonce: &str, post_data: &str, secret: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::{Sha256, Sha512, Digest};

        let secret_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            secret
        )?;

        // SHA256(nonce + POST data)
        let mut sha256 = Sha256::new();
        sha256.update(nonce.as_bytes());
        sha256.update(post_data.as_bytes());
        let sha256_result = sha256.finalize();

        // Combine path + SHA256 result
        let mut message = path.as_bytes().to_vec();
        message.extend_from_slice(&sha256_result);

        // HMAC-SHA512
        let mut mac = Hmac::<Sha512>::new_from_slice(&secret_bytes)
            .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
        mac.update(&message);
        let result = mac.finalize();

        Ok(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            result.into_bytes()
        ))
    }

    async fn kraken_check_balance(&self, api_key: &str, api_secret: &str) -> Result<()> {
        let url = "https://api.kraken.com/0/private/Balance";
        let nonce = chrono::Utc::now().timestamp_millis().to_string();
        let post_data = format!("nonce={}", nonce);
        let path = "/0/private/Balance";
        let signature = self.kraken_signature(path, &nonce, &post_data, api_secret)?;

        let response = self.client
            .post(url)
            .header("API-Key", api_key)
            .header("API-Sign", &signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        
        if let Some(errors) = body.get("error").and_then(|e| e.as_array()) {
            if !errors.is_empty() {
                bail!("Balance check failed");
            }
        }
        
        Ok(())
    }

    async fn kraken_check_withdraw_permission(&self, api_key: &str, api_secret: &str) -> Result<()> {
        // Try to get withdrawal addresses (read operation, not actual withdrawal)
        let url = "https://api.kraken.com/0/private/WithdrawAddresses";
        let nonce = chrono::Utc::now().timestamp_millis().to_string();
        let post_data = format!("nonce={}&asset=XBT", nonce);
        let path = "/0/private/WithdrawAddresses";
        let signature = self.kraken_signature(path, &nonce, &post_data, api_secret)?;

        let response = self.client
            .post(url)
            .header("API-Key", api_key)
            .header("API-Sign", &signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        
        if let Some(errors) = body.get("error").and_then(|e| e.as_array()) {
            if !errors.is_empty() {
                let error_str = errors.iter()
                    .filter_map(|e| e.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                
                // Permission denied = good, key doesn't have withdraw
                if error_str.contains("permission") || error_str.contains("Permission") {
                    bail!("No withdraw permission (good)");
                }
            }
        }
        
        // If we got here without error, the key has withdraw access
        Ok(())
    }

    // ========================================================================
    // COINBASE
    // ========================================================================

    async fn validate_coinbase(
        &self,
        api_key: &str,
        api_secret: &str,
        passphrase: Option<&str>,
    ) -> Result<PermissionValidation> {
        let mut validation = PermissionValidation {
            exchange: "coinbase".to_string(),
            is_valid: false,
            verified_with_exchange: false,
            permissions: DetectedPermissions::default(),
            errors: vec![],
            warnings: vec![],
            raw_permissions: None,
        };

        let passphrase = match passphrase {
            Some(p) => p,
            None => {
                validation.errors.push("Coinbase requires a passphrase".to_string());
                return Ok(validation);
            }
        };

        // Coinbase Advanced Trade API - get account info
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let method = "GET";
        let path = "/api/v3/brokerage/accounts";
        let url = format!("https://api.coinbase.com{}", path);

        let signature = self.coinbase_signature(&timestamp, method, path, "", api_secret)?;

        let response = self.client
            .get(&url)
            .header("CB-ACCESS-KEY", api_key)
            .header("CB-ACCESS-SIGN", &signature)
            .header("CB-ACCESS-TIMESTAMP", &timestamp)
            .header("CB-ACCESS-PASSPHRASE", passphrase)
            .header("CB-VERSION", "2024-01-01")
            .send()
            .await?;

        let status = response.status();
        let body: serde_json::Value = response.json().await?;
        validation.raw_permissions = Some(body.clone());

        if !status.is_success() {
            if let Some(message) = body.get("message").and_then(|m| m.as_str()) {
                validation.errors.push(message.to_string());
            } else {
                validation.errors.push(format!("HTTP {}", status));
            }
            return Ok(validation);
        }

        validation.is_valid = true;
        validation.verified_with_exchange = true;
        validation.permissions.can_read = true;

        // Check for portfolio permissions (indicates trading)
        if body.get("accounts").is_some() {
            validation.permissions.can_trade_spot = true;
        }

        // Check withdrawal by attempting to list withdrawal methods
        let withdraw_check = self.coinbase_check_withdraw(api_key, api_secret, passphrase).await;
        if withdraw_check.is_ok() {
            validation.permissions.can_withdraw = true;
            validation.errors.push(
                "SECURITY RISK: This API key has withdrawal permissions. \
                Please create a new key with only trading permissions.".to_string()
            );
        }

        Ok(validation)
    }

    fn coinbase_signature(&self, timestamp: &str, method: &str, path: &str, body: &str, secret: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let message = format!("{}{}{}{}", timestamp, method, path, body);
        
        let secret_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            secret
        )?;

        let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
            .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
        mac.update(message.as_bytes());
        let result = mac.finalize();

        Ok(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            result.into_bytes()
        ))
    }

    async fn coinbase_check_withdraw(&self, api_key: &str, api_secret: &str, passphrase: &str) -> Result<()> {
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let method = "GET";
        let path = "/api/v3/brokerage/payment_methods";
        let url = format!("https://api.coinbase.com{}", path);

        let signature = self.coinbase_signature(&timestamp, method, path, "", api_secret)?;

        let response = self.client
            .get(&url)
            .header("CB-ACCESS-KEY", api_key)
            .header("CB-ACCESS-SIGN", &signature)
            .header("CB-ACCESS-TIMESTAMP", &timestamp)
            .header("CB-ACCESS-PASSPHRASE", passphrase)
            .header("CB-VERSION", "2024-01-01")
            .send()
            .await?;

        if response.status().is_success() {
            // Has access to payment methods = likely has withdraw
            Ok(())
        } else {
            bail!("No withdraw permission")
        }
    }

    // ========================================================================
    // BINANCE / BINANCE US
    // ========================================================================

    async fn validate_binance(
        &self,
        api_key: &str,
        api_secret: &str,
        exchange: &str,
    ) -> Result<PermissionValidation> {
        let mut validation = PermissionValidation {
            exchange: exchange.to_string(),
            is_valid: false,
            verified_with_exchange: false,
            permissions: DetectedPermissions::default(),
            errors: vec![],
            warnings: vec![],
            raw_permissions: None,
        };

        let base_url = if exchange == "binance_us" {
            "https://api.binance.us"
        } else {
            "https://api.binance.com"
        };

        // Binance has an API key permissions endpoint
        let timestamp = chrono::Utc::now().timestamp_millis();
        let query = format!("timestamp={}", timestamp);
        let signature = self.binance_signature(&query, api_secret)?;
        
        let url = format!("{}/sapi/v1/account/apiRestrictions?{}&signature={}", 
            base_url, query, signature);

        let response = self.client
            .get(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await?;

        let status = response.status();
        let body: serde_json::Value = response.json().await?;
        validation.raw_permissions = Some(body.clone());

        if !status.is_success() {
            if let Some(msg) = body.get("msg").and_then(|m| m.as_str()) {
                validation.errors.push(msg.to_string());
            }
            return Ok(validation);
        }

        validation.is_valid = true;
        validation.verified_with_exchange = true;

        // Parse Binance permission response
        // Example response:
        // {
        //   "ipRestrict": false,
        //   "createTime": 1623840271000,
        //   "enableWithdrawals": false,   // CRITICAL
        //   "enableInternalTransfer": false,
        //   "permitsUniversalTransfer": false,
        //   "enableVanillaOptions": false,
        //   "enableReading": true,
        //   "enableFutures": false,
        //   "enableMargin": false,
        //   "enableSpotAndMarginTrading": true,
        //   ...
        // }

        if body.get("enableReading").and_then(|v| v.as_bool()).unwrap_or(false) {
            validation.permissions.can_read = true;
            validation.permissions.raw_permissions.push("READ".to_string());
        }

        if body.get("enableSpotAndMarginTrading").and_then(|v| v.as_bool()).unwrap_or(false) {
            validation.permissions.can_trade_spot = true;
            validation.permissions.raw_permissions.push("SPOT_TRADE".to_string());
        }

        if body.get("enableMargin").and_then(|v| v.as_bool()).unwrap_or(false) {
            validation.permissions.can_trade_margin = true;
            validation.permissions.raw_permissions.push("MARGIN".to_string());
        }

        if body.get("enableFutures").and_then(|v| v.as_bool()).unwrap_or(false) {
            validation.permissions.can_trade_futures = true;
            validation.permissions.raw_permissions.push("FUTURES".to_string());
        }

        // CRITICAL CHECKS
        if body.get("enableWithdrawals").and_then(|v| v.as_bool()).unwrap_or(false) {
            validation.permissions.can_withdraw = true;
            validation.permissions.raw_permissions.push("WITHDRAW".to_string());
            validation.errors.push(
                "SECURITY RISK: enableWithdrawals is TRUE. \
                Please disable withdrawal permission in your Binance API settings.".to_string()
            );
        }

        if body.get("enableInternalTransfer").and_then(|v| v.as_bool()).unwrap_or(false) {
            validation.permissions.can_transfer = true;
            validation.permissions.raw_permissions.push("INTERNAL_TRANSFER".to_string());
            validation.warnings.push(
                "Warning: Internal transfer is enabled. Consider disabling for security.".to_string()
            );
        }

        if body.get("permitsUniversalTransfer").and_then(|v| v.as_bool()).unwrap_or(false) {
            validation.permissions.can_transfer = true;
            validation.permissions.raw_permissions.push("UNIVERSAL_TRANSFER".to_string());
            validation.warnings.push(
                "Warning: Universal transfer is enabled. Consider disabling for security.".to_string()
            );
        }

        // Check IP restriction
        if !body.get("ipRestrict").and_then(|v| v.as_bool()).unwrap_or(false) {
            validation.warnings.push(
                "Recommendation: Enable IP restriction on your API key for additional security.".to_string()
            );
        }

        Ok(validation)
    }

    fn binance_signature(&self, query: &str, secret: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
        mac.update(query.as_bytes());
        let result = mac.finalize();

        Ok(hex::encode(result.into_bytes()))
    }

    // ========================================================================
    // BYBIT
    // ========================================================================

    async fn validate_bybit(
        &self,
        api_key: &str,
        api_secret: &str,
    ) -> Result<PermissionValidation> {
        let mut validation = PermissionValidation {
            exchange: "bybit".to_string(),
            is_valid: false,
            verified_with_exchange: false,
            permissions: DetectedPermissions::default(),
            errors: vec![],
            warnings: vec![],
            raw_permissions: None,
        };

        // Bybit V5 API - Query API Key Info
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let recv_window = "5000";
        
        let param_str = format!("{}{}{}",timestamp, api_key, recv_window);
        let signature = self.bybit_signature(&param_str, api_secret)?;

        let url = "https://api.bybit.com/v5/user/query-api";

        let response = self.client
            .get(url)
            .header("X-BAPI-API-KEY", api_key)
            .header("X-BAPI-SIGN", &signature)
            .header("X-BAPI-TIMESTAMP", &timestamp)
            .header("X-BAPI-RECV-WINDOW", recv_window)
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        validation.raw_permissions = Some(body.clone());

        let ret_code = body.get("retCode").and_then(|v| v.as_i64()).unwrap_or(-1);
        if ret_code != 0 {
            let msg = body.get("retMsg").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            validation.errors.push(msg.to_string());
            return Ok(validation);
        }

        validation.is_valid = true;
        validation.verified_with_exchange = true;

        // Parse Bybit permissions from result.permissions array
        // Example:
        // {
        //   "retCode": 0,
        //   "result": {
        //     "permissions": {
        //       "Spot": ["SpotTrade"],
        //       "Wallet": ["AccountTransfer", "SubMemberTransferList"],
        //       ...
        //     }
        //   }
        // }

        if let Some(result) = body.get("result") {
            if let Some(permissions) = result.get("permissions") {
                // Check Spot permissions
                if let Some(spot) = permissions.get("Spot").and_then(|v| v.as_array()) {
                    for perm in spot {
                        if let Some(p) = perm.as_str() {
                            validation.permissions.raw_permissions.push(format!("Spot:{}", p));
                            if p == "SpotTrade" {
                                validation.permissions.can_trade_spot = true;
                            }
                        }
                    }
                }

                // Check Contract (futures) permissions
                if let Some(contract) = permissions.get("Contract").and_then(|v| v.as_array()) {
                    for perm in contract {
                        if let Some(p) = perm.as_str() {
                            validation.permissions.raw_permissions.push(format!("Contract:{}", p));
                            if p == "ContractTrade" {
                                validation.permissions.can_trade_futures = true;
                            }
                        }
                    }
                }

                // Check Wallet permissions - CRITICAL
                if let Some(wallet) = permissions.get("Wallet").and_then(|v| v.as_array()) {
                    for perm in wallet {
                        if let Some(p) = perm.as_str() {
                            validation.permissions.raw_permissions.push(format!("Wallet:{}", p));
                            
                            // These are dangerous
                            if p == "Withdraw" {
                                validation.permissions.can_withdraw = true;
                                validation.errors.push(
                                    "SECURITY RISK: Withdraw permission detected. \
                                    Please remove this permission from your API key.".to_string()
                                );
                            }
                            if p.contains("Transfer") {
                                validation.permissions.can_transfer = true;
                                validation.warnings.push(format!(
                                    "Warning: Transfer permission '{}' detected.", p
                                ));
                            }
                        }
                    }
                }

                validation.permissions.can_read = true;
            }

            // Check IP restriction
            if let Some(ips) = result.get("ips").and_then(|v| v.as_str()) {
                if ips == "*" || ips.is_empty() {
                    validation.warnings.push(
                        "Recommendation: Restrict API key to specific IP addresses.".to_string()
                    );
                }
            }
        }

        Ok(validation)
    }

    fn bybit_signature(&self, param_str: &str, secret: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
        mac.update(param_str.as_bytes());
        let result = mac.finalize();

        Ok(hex::encode(result.into_bytes()))
    }

    // ========================================================================
    // OKX
    // ========================================================================

    async fn validate_okx(
        &self,
        api_key: &str,
        api_secret: &str,
        passphrase: Option<&str>,
    ) -> Result<PermissionValidation> {
        let mut validation = PermissionValidation {
            exchange: "okx".to_string(),
            is_valid: false,
            verified_with_exchange: false,
            permissions: DetectedPermissions::default(),
            errors: vec![],
            warnings: vec![],
            raw_permissions: None,
        };

        let passphrase = match passphrase {
            Some(p) => p,
            None => {
                validation.errors.push("OKX requires a passphrase".to_string());
                return Ok(validation);
            }
        };

        // OKX API - Get account config
        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        let method = "GET";
        let path = "/api/v5/account/config";
        
        let signature = self.okx_signature(&timestamp, method, path, "", api_secret)?;

        let url = format!("https://www.okx.com{}", path);

        let response = self.client
            .get(&url)
            .header("OK-ACCESS-KEY", api_key)
            .header("OK-ACCESS-SIGN", &signature)
            .header("OK-ACCESS-TIMESTAMP", &timestamp)
            .header("OK-ACCESS-PASSPHRASE", passphrase)
            .header("Content-Type", "application/json")
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        validation.raw_permissions = Some(body.clone());

        let code = body.get("code").and_then(|v| v.as_str()).unwrap_or("");
        if code != "0" {
            let msg = body.get("msg").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            validation.errors.push(msg.to_string());
            return Ok(validation);
        }

        validation.is_valid = true;
        validation.verified_with_exchange = true;
        validation.permissions.can_read = true;

        // OKX returns permission info in data array
        if let Some(data) = body.get("data").and_then(|d| d.as_array()).and_then(|a| a.first()) {
            // Check permission level
            // perm: "read_only", "trade", "withdraw"
            if let Some(perm) = data.get("perm").and_then(|p| p.as_str()) {
                validation.permissions.raw_permissions.push(format!("perm:{}", perm));
                
                match perm {
                    "read_only" => {
                        validation.permissions.can_read = true;
                    }
                    "trade" => {
                        validation.permissions.can_read = true;
                        validation.permissions.can_trade_spot = true;
                    }
                    "withdraw" => {
                        validation.permissions.can_read = true;
                        validation.permissions.can_trade_spot = true;
                        validation.permissions.can_withdraw = true;
                        validation.errors.push(
                            "SECURITY RISK: This API key has 'withdraw' permission level. \
                            Please create a new key with 'trade' permission only.".to_string()
                        );
                    }
                    _ => {}
                }
            }

            // Check IP restriction
            if let Some(ip) = data.get("ip").and_then(|i| i.as_str()) {
                if ip.is_empty() {
                    validation.warnings.push(
                        "Recommendation: Set IP restriction on your OKX API key.".to_string()
                    );
                }
            }
        }

        Ok(validation)
    }

    fn okx_signature(&self, timestamp: &str, method: &str, path: &str, body: &str, secret: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let message = format!("{}{}{}{}", timestamp, method, path, body);

        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
        mac.update(message.as_bytes());
        let result = mac.finalize();

        Ok(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            result.into_bytes()
        ))
    }

    // ========================================================================
    // GEMINI
    // ========================================================================

    async fn validate_gemini(
        &self,
        api_key: &str,
        api_secret: &str,
    ) -> Result<PermissionValidation> {
        let mut validation = PermissionValidation {
            exchange: "gemini".to_string(),
            is_valid: false,
            verified_with_exchange: false,
            permissions: DetectedPermissions::default(),
            errors: vec![],
            warnings: vec![],
            raw_permissions: None,
        };

        // Gemini - Get account details
        let nonce = chrono::Utc::now().timestamp_millis();
        let path = "/v1/account";
        
        let payload = serde_json::json!({
            "request": path,
            "nonce": nonce
        });
        let payload_str = serde_json::to_string(&payload)?;
        let payload_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            payload_str.as_bytes()
        );

        let signature = self.gemini_signature(&payload_b64, api_secret)?;

        let url = format!("https://api.gemini.com{}", path);

        let response = self.client
            .post(&url)
            .header("X-GEMINI-APIKEY", api_key)
            .header("X-GEMINI-PAYLOAD", &payload_b64)
            .header("X-GEMINI-SIGNATURE", &signature)
            .header("Content-Type", "text/plain")
            .header("Content-Length", "0")
            .send()
            .await?;

        let status = response.status();
        let body: serde_json::Value = response.json().await?;
        validation.raw_permissions = Some(body.clone());

        if !status.is_success() {
            let msg = body.get("message").and_then(|m| m.as_str())
                .or_else(|| body.get("reason").and_then(|r| r.as_str()))
                .unwrap_or("Unknown error");
            validation.errors.push(msg.to_string());
            return Ok(validation);
        }

        validation.is_valid = true;
        validation.verified_with_exchange = true;
        validation.permissions.can_read = true;

        // Gemini returns account info, check for trading capability
        if body.get("account").is_some() {
            validation.permissions.can_trade_spot = true;
        }

        // Check roles if present
        if let Some(roles) = body.get("roles").and_then(|r| r.as_array()) {
            for role in roles {
                if let Some(r) = role.as_str() {
                    validation.permissions.raw_permissions.push(format!("role:{}", r));
                    
                    if r.contains("withdraw") || r.contains("Withdraw") {
                        validation.permissions.can_withdraw = true;
                        validation.errors.push(
                            "SECURITY RISK: Withdrawal role detected. \
                            Please create a new API key without withdrawal permissions.".to_string()
                        );
                    }
                    if r.contains("transfer") || r.contains("Transfer") {
                        validation.permissions.can_transfer = true;
                        validation.warnings.push(format!("Warning: Transfer role '{}' detected.", r));
                    }
                }
            }
        }

        // Try to check withdrawal capability
        let withdraw_check = self.gemini_check_withdraw(api_key, api_secret).await;
        if withdraw_check.is_ok() {
            validation.permissions.can_withdraw = true;
            if !validation.errors.iter().any(|e| e.contains("SECURITY RISK")) {
                validation.errors.push(
                    "SECURITY RISK: This API key appears to have withdrawal access.".to_string()
                );
            }
        }

        Ok(validation)
    }

    fn gemini_signature(&self, payload_b64: &str, secret: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::Sha384;

        let secret_bytes = secret.as_bytes();
        
        let mut mac = Hmac::<Sha384>::new_from_slice(secret_bytes)
            .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
        mac.update(payload_b64.as_bytes());
        let result = mac.finalize();

        Ok(hex::encode(result.into_bytes()))
    }

    async fn gemini_check_withdraw(&self, api_key: &str, api_secret: &str) -> Result<()> {
        // Try to get withdraw addresses
        let nonce = chrono::Utc::now().timestamp_millis();
        let path = "/v1/addresses/btc";
        
        let payload = serde_json::json!({
            "request": path,
            "nonce": nonce
        });
        let payload_str = serde_json::to_string(&payload)?;
        let payload_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            payload_str.as_bytes()
        );

        let signature = self.gemini_signature(&payload_b64, api_secret)?;

        let url = format!("https://api.gemini.com{}", path);

        let response = self.client
            .post(&url)
            .header("X-GEMINI-APIKEY", api_key)
            .header("X-GEMINI-PAYLOAD", &payload_b64)
            .header("X-GEMINI-SIGNATURE", &signature)
            .header("Content-Type", "text/plain")
            .header("Content-Length", "0")
            .send()
            .await?;

        if response.status().is_success() {
            Ok(()) // Has withdraw access
        } else {
            bail!("No withdraw access")
        }
    }

    // ========================================================================
    // DERIBIT
    // ========================================================================

    async fn validate_deribit(
        &self,
        api_key: &str,
        api_secret: &str,
    ) -> Result<PermissionValidation> {
        let mut validation = PermissionValidation {
            exchange: "deribit".to_string(),
            is_valid: false,
            verified_with_exchange: false,
            permissions: DetectedPermissions::default(),
            errors: vec![],
            warnings: vec![],
            raw_permissions: None,
        };

        // Deribit - Authenticate and get key info
        let timestamp = chrono::Utc::now().timestamp_millis();
        let nonce = uuid::Uuid::new_v4().to_string();
        let signature_data = format!("{}\n{}\n", timestamp, nonce);
        
        let signature = self.deribit_signature(&signature_data, api_secret)?;

        let url = format!(
            "https://www.deribit.com/api/v2/public/auth?\
            grant_type=client_signature&\
            client_id={}&\
            timestamp={}&\
            nonce={}&\
            signature={}",
            api_key, timestamp, nonce, signature
        );

        let response = self.client.get(&url).send().await?;
        let body: serde_json::Value = response.json().await?;
        validation.raw_permissions = Some(body.clone());

        if let Some(error) = body.get("error") {
            let msg = error.get("message").and_then(|m| m.as_str()).unwrap_or("Auth failed");
            validation.errors.push(msg.to_string());
            return Ok(validation);
        }

        validation.is_valid = true;
        validation.verified_with_exchange = true;

        // Parse Deribit auth response for scope
        // Example result.scope: "account:read_write trade:read_write wallet:read"
        if let Some(result) = body.get("result") {
            if let Some(scope) = result.get("scope").and_then(|s| s.as_str()) {
                let scopes: Vec<&str> = scope.split_whitespace().collect();
                
                for s in &scopes {
                    validation.permissions.raw_permissions.push(s.to_string());
                    
                    if s.starts_with("account:") {
                        validation.permissions.can_read = true;
                    }
                    if s.starts_with("trade:") {
                        validation.permissions.can_trade_futures = true;
                    }
                    if s.contains("wallet:read_write") || s.contains("wallet:withdraw") {
                        validation.permissions.can_withdraw = true;
                        validation.errors.push(
                            "SECURITY RISK: wallet:read_write scope detected (allows withdrawals). \
                            Please create a key with only 'wallet:read' scope.".to_string()
                        );
                    }
                    if s.contains("mainaccount") {
                        validation.permissions.can_manage_keys = true;
                        validation.warnings.push(
                            "Warning: Main account scope detected.".to_string()
                        );
                    }
                }
            }

            // Check enabled_features
            if let Some(features) = result.get("enabled_features").and_then(|f| f.as_array()) {
                for feature in features {
                    if let Some(f) = feature.as_str() {
                        if f == "withdrawal" {
                            validation.permissions.can_withdraw = true;
                        }
                    }
                }
            }
        }

        Ok(validation)
    }

    fn deribit_signature(&self, data: &str, secret: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
        mac.update(data.as_bytes());
        let result = mac.finalize();

        Ok(hex::encode(result.into_bytes()))
    }
}

impl Default for PermissionValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detected_permissions_dangerous() {
        let mut perms = DetectedPermissions::default();
        assert!(!perms.has_dangerous_permissions());
        
        perms.can_withdraw = true;
        assert!(perms.has_dangerous_permissions());
        assert!(perms.dangerous_permissions_list().contains(&"WITHDRAW"));
    }

    #[test]
    fn test_permission_validation_creation() {
        let validation = PermissionValidation {
            exchange: "test".to_string(),
            is_valid: true,
            verified_with_exchange: true,
            permissions: DetectedPermissions::default(),
            errors: vec![],
            warnings: vec![],
            raw_permissions: None,
        };
        
        assert!(validation.is_valid);
        assert!(!validation.permissions.can_withdraw);
    }
}
