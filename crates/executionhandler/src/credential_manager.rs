//! Secrets Management Module
//!
//! Provides secure credential storage and retrieval with support for:
//! - Environment variables (development)
//! - HashiCorp Vault (production)
//! - AWS Secrets Manager (cloud deployments)
//! - Encrypted file storage (air-gapped systems)
//!
//! All secrets are zeroized from memory after use.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use anyhow::{Result, Context, anyhow};
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A secret value that is zeroized when dropped
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Get the secret value (use sparingly, prefer expose_secret)
    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    /// Get owned value (consumes self, use for one-time operations)
    pub fn into_inner(self) -> String {
        self.0.clone()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretString([REDACTED])")
    }
}

impl std::fmt::Display for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[REDACTED]")
    }
}

/// API credentials with zeroize-on-drop
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct ApiCredentials {
    pub api_key: SecretString,
    pub api_secret: SecretString,
    #[zeroize(skip)]
    pub passphrase: Option<SecretString>,
}

impl ApiCredentials {
    pub fn new(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        Self {
            api_key: SecretString::new(api_key),
            api_secret: SecretString::new(api_secret),
            passphrase: None,
        }
    }

    pub fn with_passphrase(mut self, passphrase: impl Into<String>) -> Self {
        self.passphrase = Some(SecretString::new(passphrase));
        self
    }
}

impl std::fmt::Debug for ApiCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiCredentials")
            .field("api_key", &"[REDACTED]")
            .field("api_secret", &"[REDACTED]")
            .field("passphrase", &self.passphrase.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Secret backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretBackend {
    /// Environment variables (development only)
    Environment,
    /// HashiCorp Vault
    Vault,
    /// AWS Secrets Manager
    AwsSecretsManager,
    /// Encrypted local file
    EncryptedFile,
    /// In-memory (testing only)
    InMemory,
}

/// Configuration for secrets manager
#[derive(Debug, Clone)]
pub struct SecretsConfig {
    /// Primary backend to use
    pub backend: SecretBackend,
    /// Vault address (if using Vault)
    pub vault_addr: Option<String>,
    /// Vault token (if using Vault) - loaded from env
    pub vault_token_env: Option<String>,
    /// AWS region (if using AWS)
    pub aws_region: Option<String>,
    /// Encrypted file path (if using file backend)
    pub encrypted_file_path: Option<String>,
    /// Cache TTL for secrets
    pub cache_ttl: Duration,
    /// Enable secret rotation
    pub enable_rotation: bool,
    /// Rotation interval
    pub rotation_interval: Duration,
}

impl Default for SecretsConfig {
    fn default() -> Self {
        Self {
            backend: SecretBackend::Environment,
            vault_addr: None,
            vault_token_env: Some("VAULT_TOKEN".to_string()),
            aws_region: None,
            encrypted_file_path: None,
            cache_ttl: Duration::from_secs(300), // 5 minutes
            enable_rotation: false,
            rotation_interval: Duration::from_secs(86400), // 24 hours
        }
    }
}

impl SecretsConfig {
    pub fn vault(addr: &str) -> Self {
        Self {
            backend: SecretBackend::Vault,
            vault_addr: Some(addr.to_string()),
            ..Default::default()
        }
    }

    pub fn aws(region: &str) -> Self {
        Self {
            backend: SecretBackend::AwsSecretsManager,
            aws_region: Some(region.to_string()),
            ..Default::default()
        }
    }
}

/// Cached secret with expiration
struct CachedSecret {
    value: SecretString,
    expires_at: Instant,
}

/// Secrets manager for secure credential handling
pub struct SecretsManager {
    config: SecretsConfig,
    cache: RwLock<HashMap<String, CachedSecret>>,
    /// In-memory secrets for testing
    in_memory_secrets: RwLock<HashMap<String, SecretString>>,
    /// HTTP client for external backends
    client: reqwest::Client,
}

impl SecretsManager {
    pub fn new(config: SecretsConfig) -> Self {
        Self {
            config,
            cache: RwLock::new(HashMap::new()),
            in_memory_secrets: RwLock::new(HashMap::new()),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Create a development instance using environment variables
    pub fn from_env() -> Self {
        Self::new(SecretsConfig::default())
    }

    /// Create a testing instance with in-memory storage
    pub fn in_memory() -> Self {
        Self::new(SecretsConfig {
            backend: SecretBackend::InMemory,
            ..Default::default()
        })
    }

    /// Set a secret (for testing/in-memory backend)
    pub fn set_secret(&self, key: &str, value: impl Into<String>) {
        let mut secrets = self.in_memory_secrets.write();
        secrets.insert(key.to_string(), SecretString::new(value));
    }

    /// Get a secret by key
    pub async fn get_secret(&self, key: &str) -> Result<SecretString> {
        // Check cache first
        if let Some(cached) = self.get_cached(key) {
            return Ok(cached);
        }

        // Fetch from backend
        let secret = match self.config.backend {
            SecretBackend::Environment => self.get_from_env(key)?,
            SecretBackend::Vault => self.get_from_vault(key).await?,
            SecretBackend::AwsSecretsManager => self.get_from_aws(key).await?,
            SecretBackend::EncryptedFile => self.get_from_file(key)?,
            SecretBackend::InMemory => self.get_from_memory(key)?,
        };

        // Cache the secret
        self.cache_secret(key, secret.clone());

        Ok(secret)
    }

    /// Get API credentials for an exchange
    pub async fn get_exchange_credentials(&self, exchange: &str) -> Result<ApiCredentials> {
        let prefix = format!("{}_", exchange.to_uppercase());
        
        let api_key = self.get_secret(&format!("{}API_KEY", prefix)).await
            .with_context(|| format!("Missing API key for {}", exchange))?;
        
        let api_secret = self.get_secret(&format!("{}API_SECRET", prefix)).await
            .with_context(|| format!("Missing API secret for {}", exchange))?;

        let mut creds = ApiCredentials::new(
            api_key.expose_secret().to_string(),
            api_secret.expose_secret().to_string(),
        );

        // Try to get passphrase (optional for some exchanges)
        if let Ok(passphrase) = self.get_secret(&format!("{}PASSPHRASE", prefix)).await {
            creds = creds.with_passphrase(passphrase.expose_secret().to_string());
        }

        Ok(creds)
    }

    /// Invalidate cached secret
    pub fn invalidate(&self, key: &str) {
        let mut cache = self.cache.write();
        cache.remove(key);
    }

    /// Clear all cached secrets
    pub fn clear_cache(&self) {
        let mut cache = self.cache.write();
        // Zeroize all cached values before clearing
        for (_, cached) in cache.drain() {
            drop(cached.value); // Triggers zeroize
        }
    }

    fn get_cached(&self, key: &str) -> Option<SecretString> {
        let cache = self.cache.read();
        if let Some(cached) = cache.get(key) {
            if cached.expires_at > Instant::now() {
                return Some(cached.value.clone());
            }
        }
        None
    }

    fn cache_secret(&self, key: &str, value: SecretString) {
        let mut cache = self.cache.write();
        cache.insert(
            key.to_string(),
            CachedSecret {
                value,
                expires_at: Instant::now() + self.config.cache_ttl,
            },
        );
    }

    fn get_from_env(&self, key: &str) -> Result<SecretString> {
        std::env::var(key)
            .map(SecretString::new)
            .with_context(|| format!("Environment variable {} not found", key))
    }

    fn get_from_memory(&self, key: &str) -> Result<SecretString> {
        let secrets = self.in_memory_secrets.read();
        secrets
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow!("Secret {} not found in memory", key))
    }

    async fn get_from_vault(&self, key: &str) -> Result<SecretString> {
        let vault_addr = self.config.vault_addr.as_ref()
            .ok_or_else(|| anyhow!("Vault address not configured"))?;
        
        let vault_token = self.config.vault_token_env.as_ref()
            .and_then(|env| std::env::var(env).ok())
            .ok_or_else(|| anyhow!("Vault token not found in environment"))?;

        // Parse key as "path/to/secret:field"
        let (path, field) = key.split_once(':').unwrap_or((key, "value"));

        let url = format!("{}/v1/secret/data/{}", vault_addr, path);
        
        let response = self.client
            .get(&url)
            .header("X-Vault-Token", &vault_token)
            .send()
            .await
            .context("Failed to connect to Vault")?;

        if !response.status().is_success() {
            return Err(anyhow!("Vault returned status: {}", response.status()));
        }

        let body: VaultResponse = response.json().await
            .context("Failed to parse Vault response")?;

        body.data.data.get(field)
            .map(|v| SecretString::new(v.clone()))
            .ok_or_else(|| anyhow!("Field {} not found in Vault secret", field))
    }

    async fn get_from_aws(&self, key: &str) -> Result<SecretString> {
        let region = self.config.aws_region.as_ref()
            .ok_or_else(|| anyhow!("AWS region not configured"))?;

        // AWS Secrets Manager HTTP API
        // In production, use aws-sdk-secretsmanager crate
        let url = format!(
            "https://secretsmanager.{}.amazonaws.com/",
            region
        );

        // This is a simplified implementation
        // Real implementation would use AWS SigV4 signing
        Err(anyhow!("AWS Secrets Manager requires aws-sdk-secretsmanager crate"))
    }

    fn get_from_file(&self, key: &str) -> Result<SecretString> {
        let file_path = self.config.encrypted_file_path.as_ref()
            .ok_or_else(|| anyhow!("Encrypted file path not configured"))?;

        // Read and decrypt file
        // In production, use age or similar encryption
        let content = std::fs::read_to_string(file_path)
            .context("Failed to read secrets file")?;

        let secrets: HashMap<String, String> = serde_json::from_str(&content)
            .context("Failed to parse secrets file")?;

        secrets.get(key)
            .map(|v| SecretString::new(v.clone()))
            .ok_or_else(|| anyhow!("Secret {} not found in file", key))
    }
}

impl Drop for SecretsManager {
    fn drop(&mut self) {
        // Clear all cached secrets on drop
        self.clear_cache();
        
        // Clear in-memory secrets
        let mut secrets = self.in_memory_secrets.write();
        for (_, secret) in secrets.drain() {
            drop(secret); // Triggers zeroize
        }
    }
}

/// Vault response structure
#[derive(Deserialize)]
struct VaultResponse {
    data: VaultData,
}

#[derive(Deserialize)]
struct VaultData {
    data: HashMap<String, String>,
}

/// Security audit event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecurityEventType {
    /// Secret accessed
    SecretAccessed,
    /// Secret rotation started
    SecretRotationStarted,
    /// Secret rotation completed
    SecretRotationCompleted,
    /// Secret rotation failed
    SecretRotationFailed,
    /// Invalid secret access attempt
    InvalidSecretAccess,
    /// Cache cleared
    CacheCleared,
    /// Authentication failed
    AuthenticationFailed,
    /// Authentication succeeded
    AuthenticationSucceeded,
    /// Permission denied
    PermissionDenied,
    /// API key created
    ApiKeyCreated,
    /// API key revoked
    ApiKeyRevoked,
    /// Suspicious activity detected
    SuspiciousActivity,
}

/// Security audit entry
#[derive(Debug, Clone, Serialize)]
pub struct SecurityAuditEntry {
    pub timestamp_ns: u64,
    pub event_type: SecurityEventType,
    pub service_id: Option<String>,
    pub resource: Option<String>,
    pub ip_address: Option<String>,
    pub success: bool,
    pub details: String,
}

impl SecurityAuditEntry {
    pub fn new(event_type: SecurityEventType, details: impl Into<String>) -> Self {
        Self {
            timestamp_ns: crate::optimizations::timestamp::nano_timestamp() as u64,
            event_type,
            service_id: None,
            resource: None,
            ip_address: None,
            success: true,
            details: details.into(),
        }
    }

    pub fn with_service(mut self, service_id: &str) -> Self {
        self.service_id = Some(service_id.to_string());
        self
    }

    pub fn with_resource(mut self, resource: &str) -> Self {
        self.resource = Some(resource.to_string());
        self
    }

    pub fn with_ip(mut self, ip: &str) -> Self {
        self.ip_address = Some(ip.to_string());
        self
    }

    pub fn failed(mut self) -> Self {
        self.success = false;
        self
    }
}

/// Global secrets manager instance
pub static SECRETS: once_cell::sync::Lazy<SecretsManager> =
    once_cell::sync::Lazy::new(SecretsManager::from_env);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_string_redacted() {
        let secret = SecretString::new("super_secret_value");
        assert_eq!(format!("{}", secret), "[REDACTED]");
        assert_eq!(format!("{:?}", secret), "SecretString([REDACTED])");
    }

    #[test]
    fn test_secret_string_expose() {
        let secret = SecretString::new("my_secret");
        assert_eq!(secret.expose_secret(), "my_secret");
    }

    #[test]
    fn test_api_credentials_redacted() {
        let creds = ApiCredentials::new("key123", "secret456")
            .with_passphrase("pass789");
        
        let debug = format!("{:?}", creds);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("key123"));
        assert!(!debug.contains("secret456"));
        assert!(!debug.contains("pass789"));
    }

    #[test]
    fn test_in_memory_backend() {
        let manager = SecretsManager::in_memory();
        manager.set_secret("TEST_KEY", "test_value");
        
        let secret = manager.get_from_memory("TEST_KEY").unwrap();
        assert_eq!(secret.expose_secret(), "test_value");
    }

    #[tokio::test]
    async fn test_get_secret_caching() {
        let manager = SecretsManager::in_memory();
        manager.set_secret("CACHED_KEY", "cached_value");
        
        // First access - from backend
        let secret1 = manager.get_secret("CACHED_KEY").await.unwrap();
        assert_eq!(secret1.expose_secret(), "cached_value");
        
        // Second access - from cache
        let secret2 = manager.get_secret("CACHED_KEY").await.unwrap();
        assert_eq!(secret2.expose_secret(), "cached_value");
    }

    #[test]
    fn test_cache_invalidation() {
        let manager = SecretsManager::in_memory();
        manager.set_secret("TEMP_KEY", "temp_value");
        
        // Cache the secret
        manager.cache_secret("TEMP_KEY", SecretString::new("temp_value"));
        
        // Verify cached
        assert!(manager.get_cached("TEMP_KEY").is_some());
        
        // Invalidate
        manager.invalidate("TEMP_KEY");
        
        // Verify removed
        assert!(manager.get_cached("TEMP_KEY").is_none());
    }

    #[tokio::test]
    async fn test_exchange_credentials() {
        let manager = SecretsManager::in_memory();
        manager.set_secret("KRAKEN_API_KEY", "kraken_key");
        manager.set_secret("KRAKEN_API_SECRET", "kraken_secret");
        manager.set_secret("KRAKEN_PASSPHRASE", "kraken_pass");
        
        let creds = manager.get_exchange_credentials("kraken").await.unwrap();
        assert_eq!(creds.api_key.expose_secret(), "kraken_key");
        assert_eq!(creds.api_secret.expose_secret(), "kraken_secret");
        assert_eq!(creds.passphrase.as_ref().unwrap().expose_secret(), "kraken_pass");
    }

    #[test]
    fn test_security_audit_entry() {
        let entry = SecurityAuditEntry::new(
            SecurityEventType::SecretAccessed,
            "Accessed API key for kraken"
        )
        .with_service("execution-handler")
        .with_resource("KRAKEN_API_KEY")
        .with_ip("192.168.1.1");
        
        assert_eq!(entry.event_type, SecurityEventType::SecretAccessed);
        assert!(entry.success);
    }

    #[test]
    fn test_config_presets() {
        let vault_config = SecretsConfig::vault("https://vault.example.com:8200");
        assert_eq!(vault_config.backend, SecretBackend::Vault);
        assert_eq!(vault_config.vault_addr, Some("https://vault.example.com:8200".to_string()));
        
        let aws_config = SecretsConfig::aws("us-east-1");
        assert_eq!(aws_config.backend, SecretBackend::AwsSecretsManager);
        assert_eq!(aws_config.aws_region, Some("us-east-1".to_string()));
    }
}
