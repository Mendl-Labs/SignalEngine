//! Database-backed credential provider
//!
//! Loads exchange credentials from the exchange_credentials table,
//! decrypts them, and provides them to exchange connectors.
//!
//! This bridges the credentials stored via the BacktestingEngine API
//! to the SignalEngine execution handlers.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use uuid::Uuid;
use anyhow::{Result, Context, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD};

use super::credential_manager::{ApiCredentials, SecretString};

/// Decrypted credential entry
#[derive(Debug, Clone)]
pub struct DecryptedCredential {
    pub id: Uuid,
    pub exchange: String,
    pub label: String,
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
    pub is_testnet: bool,
}

/// Provider that loads credentials from database
/// 
/// This is designed to be populated by the smartorderrouter module
/// which has database access, then passed to execution handlers.
pub struct DatabaseCredentialProvider {
    /// Cache of credentials by exchange name
    credentials: RwLock<HashMap<String, Vec<DecryptedCredential>>>,
    /// Tenant ID this provider is scoped to
    tenant_id: Uuid,
}

impl DatabaseCredentialProvider {
    /// Create a new provider for a specific tenant
    pub fn new(tenant_id: Uuid) -> Self {
        Self {
            credentials: RwLock::new(HashMap::new()),
            tenant_id,
        }
    }

    /// Get the tenant ID
    pub fn tenant_id(&self) -> Uuid {
        self.tenant_id
    }

    /// Load credentials into the cache
    /// Called by smartorderrouter after fetching from database
    pub fn load_credentials(&self, credentials: Vec<DecryptedCredential>) {
        let mut cache = self.credentials.write();
        cache.clear();
        
        for cred in credentials {
            cache.entry(cred.exchange.to_lowercase())
                .or_insert_with(Vec::new)
                .push(cred);
        }
    }

    /// Add a single credential to the cache
    pub fn add_credential(&self, cred: DecryptedCredential) {
        let mut cache = self.credentials.write();
        cache.entry(cred.exchange.to_lowercase())
            .or_insert_with(Vec::new)
            .push(cred);
    }

    /// Get credentials for an exchange
    /// Returns the first enabled credential found
    pub fn get_credentials(&self, exchange: &str) -> Option<ApiCredentials> {
        let cache = self.credentials.read();
        let exchange_lower = exchange.to_lowercase();
        
        cache.get(&exchange_lower)
            .and_then(|creds| creds.first())
            .map(|cred| {
                let mut api_creds = ApiCredentials::new(
                    cred.api_key.clone(),
                    cred.api_secret.clone(),
                );
                if let Some(ref passphrase) = cred.passphrase {
                    api_creds = api_creds.with_passphrase(passphrase.clone());
                }
                api_creds
            })
    }

    /// Get credentials by label (for multiple credentials per exchange)
    pub fn get_credentials_by_label(&self, exchange: &str, label: &str) -> Option<ApiCredentials> {
        let cache = self.credentials.read();
        let exchange_lower = exchange.to_lowercase();
        
        cache.get(&exchange_lower)
            .and_then(|creds| creds.iter().find(|c| c.label == label))
            .map(|cred| {
                let mut api_creds = ApiCredentials::new(
                    cred.api_key.clone(),
                    cred.api_secret.clone(),
                );
                if let Some(ref passphrase) = cred.passphrase {
                    api_creds = api_creds.with_passphrase(passphrase.clone());
                }
                api_creds
            })
    }

    /// Get all credentials for an exchange (for multi-account trading)
    pub fn get_all_credentials(&self, exchange: &str) -> Vec<ApiCredentials> {
        let cache = self.credentials.read();
        let exchange_lower = exchange.to_lowercase();
        
        cache.get(&exchange_lower)
            .map(|creds| {
                creds.iter().map(|cred| {
                    let mut api_creds = ApiCredentials::new(
                        cred.api_key.clone(),
                        cred.api_secret.clone(),
                    );
                    if let Some(ref passphrase) = cred.passphrase {
                        api_creds = api_creds.with_passphrase(passphrase.clone());
                    }
                    api_creds
                }).collect()
            })
            .unwrap_or_default()
    }

    /// Check if credentials exist for an exchange
    pub fn has_credentials(&self, exchange: &str) -> bool {
        let cache = self.credentials.read();
        cache.get(&exchange.to_lowercase())
            .map(|creds| !creds.is_empty())
            .unwrap_or(false)
    }

    /// List configured exchanges
    pub fn configured_exchanges(&self) -> Vec<String> {
        let cache = self.credentials.read();
        cache.keys().cloned().collect()
    }

    /// Clear all cached credentials
    pub fn clear(&self) {
        let mut cache = self.credentials.write();
        cache.clear();
    }
}

/// Decrypt a value encrypted by the BacktestingEngine API.
///
/// Supports two formats:
/// - `aes:<base64(nonce+ciphertext)>` — AES-256-GCM with 12-byte nonce, key
///   sourced from `CREDENTIALS_ENCRYPTION_KEY` env (64 hex chars / 32 bytes).
/// - `enc:<base64>` — legacy plain base64 (kept for rows created before AES
///   was introduced).
///
/// Returns `None` (and logs) for any decode/decrypt failure rather than
/// panicking — one bad row should not take the engine down.
pub fn decrypt_credential_value(encrypted: &str) -> Option<String> {
    if let Some(encoded) = encrypted.strip_prefix("aes:") {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};
        let hex_key = match std::env::var("CREDENTIALS_ENCRYPTION_KEY") {
            Ok(v) => v,
            Err(_) => {
                log::error!("CREDENTIALS_ENCRYPTION_KEY env var not set; cannot decrypt aes: credential");
                return None;
            }
        };
        let key_bytes = hex::decode(hex_key.trim()).ok()?;
        if key_bytes.len() != 32 {
            log::error!("CREDENTIALS_ENCRYPTION_KEY must be 32 bytes (64 hex chars), got {}", key_bytes.len());
            return None;
        }
        let combined = STANDARD.decode(encoded).ok()?;
        if combined.len() < 12 {
            return None;
        }
        let (nonce_bytes, ciphertext) = combined.split_at(12);
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = cipher.decrypt(nonce, ciphertext).ok()?;
        String::from_utf8(plaintext).ok()
    } else if let Some(encoded) = encrypted.strip_prefix("enc:") {
        STANDARD.decode(encoded).ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decrypt_credential_value() {
        // Encrypt like the API does
        let original = "my-api-key";
        let encrypted = format!("enc:{}", STANDARD.encode(original));
        
        let decrypted = decrypt_credential_value(&encrypted);
        assert_eq!(decrypted, Some(original.to_string()));
    }

    #[test]
    fn test_credential_provider() {
        let provider = DatabaseCredentialProvider::new(Uuid::new_v4());
        
        // Initially no credentials
        assert!(!provider.has_credentials("kraken"));
        assert!(provider.get_credentials("kraken").is_none());
        
        // Add credential
        provider.add_credential(DecryptedCredential {
            id: Uuid::new_v4(),
            exchange: "kraken".to_string(),
            label: "main".to_string(),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            passphrase: None,
            is_testnet: false,
        });
        
        // Now has credentials
        assert!(provider.has_credentials("kraken"));
        assert!(provider.has_credentials("KRAKEN")); // Case insensitive
        
        let creds = provider.get_credentials("kraken").unwrap();
        assert_eq!(creds.api_key.expose_secret(), "test-key");
        assert_eq!(creds.api_secret.expose_secret(), "test-secret");
    }
}
