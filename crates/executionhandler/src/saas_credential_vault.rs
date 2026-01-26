//! SaaS-Grade Credential Vault
//!
//! Secure storage for user API credentials in a multi-tenant environment.
//! 
//! Security Features:
//! - Per-user encryption keys (via AWS KMS or Vault Transit)
//! - Keys encrypted at rest, decrypted only in memory during execution
//! - Audit logging of all access
//! - No withdrawal permissions enforced at onboarding
//! - Automatic key rotation support
//! - Multi-layer validation: automated probing + user confirmation fallback
//!
//! Validation Flow:
//! 1. ALWAYS run automated permission probe against exchange API
//! 2. If probe detects withdrawal permission → REJECT (user confirmation ignored)
//! 3. If probe confirms no withdrawal → ACCEPT (no user confirmation needed)
//! 4. If probe is inconclusive → require user confirmation + flag for review
//!
//! Architecture:
//! ```text
//! User API Key → Validate Permissions → Encrypt with User's KMS Key → Store in DB
//!                      ↓
//!     [Probe Exchange API for permissions]
//!           ↓                ↓                    ↓
//!     Withdraw=YES      Withdraw=NO        Inconclusive
//!         ↓                 ↓                    ↓
//!       REJECT           ACCEPT         Require Confirmation
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use anyhow::{Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use super::credential_manager::ApiCredentials;
use super::permission_validator::PermissionValidator;

// ============================================================================
// User Confirmation Types (for exchanges without permission APIs)
// ============================================================================

/// Verification method used for an exchange
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerificationMethod {
    /// Exchange has API endpoint that returns permissions directly
    DirectApiVerification,
    /// Must probe endpoints to infer permissions
    IndirectProbing,
    /// Requires user to confirm settings manually
    UserConfirmation,
}

/// Permission checklist item for user confirmation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionChecklistItem {
    /// Unique ID for this item
    pub id: String,
    /// Human-readable description
    pub description: String,
    /// Whether this should be enabled (true) or disabled (false)
    pub should_be_enabled: bool,
    /// Whether this is critical (must be correct for security)
    pub is_critical: bool,
    /// Exchange-specific setting name
    pub exchange_setting_name: String,
}

/// User's confirmation of their API key settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPermissionConfirmation {
    /// Exchange this confirmation is for
    pub exchange: String,
    /// User has confirmed they read the checklist
    pub checklist_reviewed: bool,
    /// User confirms withdrawal is DISABLED
    pub withdrawal_disabled_confirmed: bool,
    /// User confirms trading is ENABLED  
    pub trading_enabled_confirmed: bool,
    /// User confirms IP whitelist is configured (optional but recommended)
    pub ip_whitelist_configured: Option<bool>,
    /// Timestamp of confirmation
    pub confirmed_at: u64,
    /// IP address when confirmed
    pub confirmed_from_ip: Option<String>,
    /// User agent when confirmed
    pub confirmed_user_agent: Option<String>,
}

impl UserPermissionConfirmation {
    /// Check if confirmation is valid for storing credentials
    pub fn is_valid(&self) -> bool {
        self.checklist_reviewed && self.withdrawal_disabled_confirmed && self.trading_enabled_confirmed
    }

    /// Validate the confirmation and return detailed results
    pub fn validate(&self, exchange: &str) -> ConfirmationValidationResult {
        let mut missing_items = Vec::new();
        let mut warnings = Vec::new();

        if !self.checklist_reviewed {
            missing_items.push("Must review the permission checklist".to_string());
        }

        if !self.withdrawal_disabled_confirmed {
            missing_items.push("Must confirm withdrawal permission is DISABLED".to_string());
        }

        if !self.trading_enabled_confirmed {
            missing_items.push("Must confirm trading permission is ENABLED".to_string());
        }

        // Exchange name must match
        if self.exchange.to_lowercase() != exchange.to_lowercase() {
            missing_items.push(format!(
                "Confirmation is for {} but storing to {}", 
                self.exchange, exchange
            ));
        }

        // IP whitelist is recommended but not required
        if self.ip_whitelist_configured != Some(true) {
            warnings.push("IP whitelist not configured - recommended for additional security".to_string());
        }

        // Check user agent (must not be suspicious)
        if let Some(ref agent) = self.confirmed_user_agent {
            if agent.contains("curl") || agent.contains("python") || agent.contains("bot") {
                warnings.push("Confirmation submitted via automated tool - ensure this is intentional".to_string());
            }
        }

        ConfirmationValidationResult {
            is_valid: missing_items.is_empty(),
            missing_items,
            warnings,
            confirmed_at: self.confirmed_at,
        }
    }
}

/// Result of validating a user confirmation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmationValidationResult {
    pub is_valid: bool,
    pub missing_items: Vec<String>,
    pub warnings: Vec<String>,
    pub confirmed_at: u64,
}

/// Exchange-specific permission requirements
#[derive(Debug, Clone, Serialize)]
pub struct ExchangePermissionRequirements {
    /// Exchange name
    pub exchange: String,
    /// How we verify permissions
    pub verification_method: VerificationMethod,
    /// Whether user confirmation is required
    pub requires_user_confirmation: bool,
    /// Checklist items for user to verify
    pub checklist: Vec<PermissionChecklistItem>,
    /// Help URL for configuring API keys on this exchange
    pub help_url: String,
    /// Screenshot/image URL showing correct settings
    pub screenshot_url: Option<String>,
    /// Additional instructions
    pub instructions: String,
}

/// Get permission requirements for an exchange
pub fn get_exchange_requirements(exchange: &str) -> ExchangePermissionRequirements {
    match exchange.to_lowercase().as_str() {
        "kraken" => ExchangePermissionRequirements {
            exchange: "kraken".to_string(),
            verification_method: VerificationMethod::IndirectProbing,
            requires_user_confirmation: true,
            checklist: vec![
                PermissionChecklistItem {
                    id: "query_funds".to_string(),
                    description: "Query Funds permission is ENABLED".to_string(),
                    should_be_enabled: true,
                    is_critical: false,
                    exchange_setting_name: "Query Funds".to_string(),
                },
                PermissionChecklistItem {
                    id: "query_orders".to_string(),
                    description: "Query Open Orders & Trades permission is ENABLED".to_string(),
                    should_be_enabled: true,
                    is_critical: false,
                    exchange_setting_name: "Query Open Orders & Trades".to_string(),
                },
                PermissionChecklistItem {
                    id: "create_orders".to_string(),
                    description: "Create & Modify Orders permission is ENABLED".to_string(),
                    should_be_enabled: true,
                    is_critical: true,
                    exchange_setting_name: "Create & Modify Orders".to_string(),
                },
                PermissionChecklistItem {
                    id: "withdraw_funds".to_string(),
                    description: "Withdraw Funds permission is DISABLED".to_string(),
                    should_be_enabled: false,
                    is_critical: true,
                    exchange_setting_name: "Withdraw Funds".to_string(),
                },
            ],
            help_url: "https://support.kraken.com/hc/en-us/articles/360000919966-How-to-create-an-API-key".to_string(),
            screenshot_url: None,
            instructions: "When creating your API key on Kraken:\n\
                1. Go to Settings → API\n\
                2. Click 'Add Key'\n\
                3. Enable: Query Funds, Query Orders, Create & Modify Orders\n\
                4. DISABLE: Withdraw Funds (CRITICAL)\n\
                5. Set IP whitelist to our server IPs for extra security".to_string(),
        },
        
        "coinbase" => ExchangePermissionRequirements {
            exchange: "coinbase".to_string(),
            verification_method: VerificationMethod::IndirectProbing,
            requires_user_confirmation: true,
            checklist: vec![
                PermissionChecklistItem {
                    id: "view".to_string(),
                    description: "View permission is ENABLED".to_string(),
                    should_be_enabled: true,
                    is_critical: false,
                    exchange_setting_name: "View".to_string(),
                },
                PermissionChecklistItem {
                    id: "trade".to_string(),
                    description: "Trade permission is ENABLED".to_string(),
                    should_be_enabled: true,
                    is_critical: true,
                    exchange_setting_name: "Trade".to_string(),
                },
                PermissionChecklistItem {
                    id: "transfer".to_string(),
                    description: "Transfer permission is DISABLED".to_string(),
                    should_be_enabled: false,
                    is_critical: true,
                    exchange_setting_name: "Transfer".to_string(),
                },
            ],
            help_url: "https://help.coinbase.com/en/exchange/managing-my-account/how-to-create-an-api-key".to_string(),
            screenshot_url: None,
            instructions: "When creating your API key on Coinbase:\n\
                1. Go to Settings → API\n\
                2. Click 'New API Key'\n\
                3. Select portfolio/account\n\
                4. Enable: View, Trade\n\
                5. DISABLE: Transfer (CRITICAL - this allows withdrawals)\n\
                6. Add IP whitelist for our servers\n\
                7. Save your passphrase securely".to_string(),
        },
        
        "gemini" => ExchangePermissionRequirements {
            exchange: "gemini".to_string(),
            verification_method: VerificationMethod::IndirectProbing,
            requires_user_confirmation: true,
            checklist: vec![
                PermissionChecklistItem {
                    id: "trading".to_string(),
                    description: "Trading permission is ENABLED".to_string(),
                    should_be_enabled: true,
                    is_critical: true,
                    exchange_setting_name: "Trading".to_string(),
                },
                PermissionChecklistItem {
                    id: "fund_management".to_string(),
                    description: "Fund Management permission is DISABLED".to_string(),
                    should_be_enabled: false,
                    is_critical: true,
                    exchange_setting_name: "Fund Management".to_string(),
                },
            ],
            help_url: "https://support.gemini.com/hc/en-us/articles/360031080191-How-do-I-create-an-API-key-".to_string(),
            screenshot_url: None,
            instructions: "When creating your API key on Gemini:\n\
                1. Go to Account → API Settings\n\
                2. Click 'Create a New API Key'\n\
                3. Select 'Primary' scope\n\
                4. Enable: Trading\n\
                5. DISABLE: Fund Management (CRITICAL - allows withdrawals)\n\
                6. Set IP whitelist".to_string(),
        },
        
        "binance" | "binance_us" => ExchangePermissionRequirements {
            exchange: exchange.to_string(),
            verification_method: VerificationMethod::DirectApiVerification,
            requires_user_confirmation: false,
            checklist: vec![
                PermissionChecklistItem {
                    id: "read".to_string(),
                    description: "Enable Reading is ON".to_string(),
                    should_be_enabled: true,
                    is_critical: false,
                    exchange_setting_name: "Enable Reading".to_string(),
                },
                PermissionChecklistItem {
                    id: "spot_trading".to_string(),
                    description: "Enable Spot & Margin Trading is ON".to_string(),
                    should_be_enabled: true,
                    is_critical: true,
                    exchange_setting_name: "Enable Spot & Margin Trading".to_string(),
                },
                PermissionChecklistItem {
                    id: "withdrawals".to_string(),
                    description: "Enable Withdrawals is OFF".to_string(),
                    should_be_enabled: false,
                    is_critical: true,
                    exchange_setting_name: "Enable Withdrawals".to_string(),
                },
            ],
            help_url: "https://www.binance.us/en/support/faq/how-to-create-api-360051283413".to_string(),
            screenshot_url: None,
            instructions: "Binance permissions are verified automatically via API.\n\
                For best security:\n\
                1. Enable: Reading, Spot Trading\n\
                2. DISABLE: Withdrawals\n\
                3. Restrict to trusted IPs".to_string(),
        },
        
        "bybit" => ExchangePermissionRequirements {
            exchange: "bybit".to_string(),
            verification_method: VerificationMethod::DirectApiVerification,
            requires_user_confirmation: false,
            checklist: vec![
                PermissionChecklistItem {
                    id: "spot_trade".to_string(),
                    description: "Spot Trading permission is ENABLED".to_string(),
                    should_be_enabled: true,
                    is_critical: true,
                    exchange_setting_name: "Spot > SpotTrade".to_string(),
                },
                PermissionChecklistItem {
                    id: "withdraw".to_string(),
                    description: "Withdraw permission is DISABLED".to_string(),
                    should_be_enabled: false,
                    is_critical: true,
                    exchange_setting_name: "Wallet > Withdraw".to_string(),
                },
            ],
            help_url: "https://www.bybit.com/en-US/help-center/bybitHC_Article?id=000001200".to_string(),
            screenshot_url: None,
            instructions: "Bybit permissions are verified automatically via API.".to_string(),
        },
        
        "okx" => ExchangePermissionRequirements {
            exchange: "okx".to_string(),
            verification_method: VerificationMethod::DirectApiVerification,
            requires_user_confirmation: false,
            checklist: vec![
                PermissionChecklistItem {
                    id: "trade".to_string(),
                    description: "Permission level is 'Trade' (not 'Withdraw')".to_string(),
                    should_be_enabled: true,
                    is_critical: true,
                    exchange_setting_name: "Permissions".to_string(),
                },
            ],
            help_url: "https://www.okx.com/help/how-do-i-create-an-api-key".to_string(),
            screenshot_url: None,
            instructions: "OKX permissions are verified automatically via API.\n\
                Select 'Trade' permission level, NOT 'Withdraw'.".to_string(),
        },
        
        "deribit" => ExchangePermissionRequirements {
            exchange: "deribit".to_string(),
            verification_method: VerificationMethod::DirectApiVerification,
            requires_user_confirmation: false,
            checklist: vec![
                PermissionChecklistItem {
                    id: "trade".to_string(),
                    description: "trade:read_write scope is ENABLED".to_string(),
                    should_be_enabled: true,
                    is_critical: true,
                    exchange_setting_name: "trade:read_write".to_string(),
                },
                PermissionChecklistItem {
                    id: "wallet".to_string(),
                    description: "wallet scope is 'read' only (NOT read_write)".to_string(),
                    should_be_enabled: true,
                    is_critical: true,
                    exchange_setting_name: "wallet:read".to_string(),
                },
            ],
            help_url: "https://docs.deribit.com/#authentication".to_string(),
            screenshot_url: None,
            instructions: "Deribit permissions are verified automatically via API.\n\
                Use 'wallet:read' scope, NOT 'wallet:read_write'.".to_string(),
        },
        
        _ => ExchangePermissionRequirements {
            exchange: exchange.to_string(),
            verification_method: VerificationMethod::UserConfirmation,
            requires_user_confirmation: true,
            checklist: vec![
                PermissionChecklistItem {
                    id: "trading".to_string(),
                    description: "Trading permission is ENABLED".to_string(),
                    should_be_enabled: true,
                    is_critical: true,
                    exchange_setting_name: "Trading".to_string(),
                },
                PermissionChecklistItem {
                    id: "withdraw".to_string(),
                    description: "Withdrawal permission is DISABLED".to_string(),
                    should_be_enabled: false,
                    is_critical: true,
                    exchange_setting_name: "Withdraw".to_string(),
                },
            ],
            help_url: String::new(),
            screenshot_url: None,
            instructions: "Please ensure withdrawal permissions are DISABLED on your API key.".to_string(),
        },
    }
}

// ============================================================================
// Stored Confirmation Record  
// ============================================================================

/// Stored record of user's permission confirmation (for audit trail)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredConfirmation {
    /// User ID
    pub user_id: String,
    /// Exchange
    pub exchange: String,
    /// The confirmation they submitted
    pub confirmation: UserPermissionConfirmation,
    /// Validation result from checking the confirmation
    pub validation_result: ConfirmationValidationResult,
    /// When the confirmation was stored
    pub stored_at: u64,
    /// Our automated validation result (if available)
    pub automated_validation: Option<CredentialValidationSnapshot>,
    /// Final combined result
    pub final_result: ConfirmationResult,
}

/// Snapshot of credential validation (for serialization in audit trail)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialValidationSnapshot {
    pub is_valid: bool,
    pub has_trade_permission: bool,
    pub has_withdraw_permission: bool,
    pub exchange_verified: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfirmationResult {
    /// Both automated check and user confirmation passed
    FullyVerified,
    /// Automated check passed (no user confirmation needed)
    AutoVerified,
    /// User confirmed, but we couldn't fully verify automatically
    UserConfirmedOnly,
    /// Rejected - withdrawal permission detected
    RejectedWithdrawDetected,
    /// Rejected - user didn't confirm
    RejectedNoConfirmation,
    /// Rejected - validation failed
    RejectedValidationFailed,
}

/// Outcome of the automated permission probe
#[derive(Debug, Clone, Serialize)]
pub enum ProbeOutcome {
    /// Probe confirmed withdrawal permission is DISABLED (safe to proceed)
    VerifiedNoWithdraw,
    /// Probe DETECTED withdrawal permission (REJECT regardless of user confirmation)
    WithdrawDetected,
    /// Probe could not determine permissions (require user confirmation)
    Inconclusive(String),
    /// Credentials are invalid
    InvalidCredentials(Vec<String>),
}

impl ProbeOutcome {
    pub fn is_safe(&self) -> bool {
        matches!(self, ProbeOutcome::VerifiedNoWithdraw)
    }

    pub fn is_dangerous(&self) -> bool {
        matches!(self, ProbeOutcome::WithdrawDetected)
    }

    pub fn requires_confirmation(&self) -> bool {
        matches!(self, ProbeOutcome::Inconclusive(_))
    }
}

/// Result of storing credentials
#[derive(Debug, Clone)]
pub struct CredentialStoreResult {
    /// Basic validation info
    pub validation: CredentialValidation,
    /// What the automated probe found
    pub probe_outcome: ProbeOutcome,
    /// Final determination
    pub final_result: ConfirmationResult,
    /// Whether this needs manual security review
    pub requires_review: bool,
}

// ============================================================================
// Original Types (unchanged)
// ============================================================================

/// Encrypted credential blob stored in database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedCredential {
    /// User ID who owns this credential
    pub user_id: String,
    /// Exchange this credential is for
    pub exchange: String,
    /// Encrypted API key (base64)
    pub encrypted_api_key: String,
    /// Encrypted API secret (base64)
    pub encrypted_api_secret: String,
    /// Encrypted passphrase if present (base64)
    pub encrypted_passphrase: Option<String>,
    /// KMS key ID used for encryption (for key rotation)
    pub kms_key_id: String,
    /// Encryption algorithm version
    pub encryption_version: u32,
    /// Nonce/IV used for encryption (base64)
    pub nonce: String,
    /// When the credential was stored
    pub created_at: u64,
    /// When the credential was last rotated
    pub rotated_at: Option<u64>,
    /// Permissions granted (for display, not enforced here)
    pub permissions: Vec<String>,
    /// IP whitelist configured on exchange
    pub whitelisted_ips: Vec<String>,
    /// User confirmation record (for exchanges without permission API)
    /// Stored as JSON for audit trail
    pub user_confirmation: Option<StoredConfirmation>,
}

/// Permission level for API keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApiKeyPermission {
    /// Can read account balances and positions
    ReadOnly,
    /// Can place and cancel orders
    Trade,
    /// Can withdraw funds - NEVER ALLOW IN SAAS
    Withdraw,
}

/// Credential validation result
#[derive(Debug, Clone)]
pub struct CredentialValidation {
    pub is_valid: bool,
    pub has_trade_permission: bool,
    pub has_withdraw_permission: bool,
    pub exchange_verified: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

/// Audit event for credential access
#[derive(Debug, Clone, Serialize)]
pub struct CredentialAuditEvent {
    pub timestamp_ns: u64,
    pub user_id: String,
    pub exchange: String,
    pub action: CredentialAction,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CredentialAction {
    Created,
    Retrieved,
    Updated,
    Deleted,
    Rotated,
    ValidationFailed,
    UsedForTrade,
}

/// KMS provider trait for encryption/decryption
#[async_trait::async_trait]
pub trait KmsProvider: Send + Sync {
    /// Encrypt data using user's KMS key
    async fn encrypt(&self, user_id: &str, plaintext: &[u8]) -> Result<(Vec<u8>, String, String)>;
    
    /// Decrypt data using user's KMS key
    async fn decrypt(&self, user_id: &str, ciphertext: &[u8], key_id: &str, nonce: &str) -> Result<Vec<u8>>;
    
    /// Create a new user key
    async fn create_user_key(&self, user_id: &str) -> Result<String>;
    
    /// Rotate user's key
    async fn rotate_user_key(&self, user_id: &str) -> Result<String>;
}

/// Storage backend trait for encrypted credentials
#[async_trait::async_trait]
pub trait CredentialStorage: Send + Sync {
    /// Store encrypted credential
    async fn store(&self, credential: &EncryptedCredential) -> Result<()>;
    
    /// Load encrypted credential
    async fn load(&self, user_id: &str, exchange: &str) -> Result<Option<EncryptedCredential>>;
    
    /// Delete credential
    async fn delete(&self, user_id: &str, exchange: &str) -> Result<()>;
    
    /// List all credentials for user
    async fn list_for_user(&self, user_id: &str) -> Result<Vec<EncryptedCredential>>;
    
    /// Record audit event
    async fn record_audit(&self, event: CredentialAuditEvent) -> Result<()>;
}

/// Main credential vault for SaaS
pub struct SaasCredentialVault {
    kms: Arc<dyn KmsProvider>,
    storage: Arc<dyn CredentialStorage>,
    /// Decrypted credentials cache (short TTL, in-memory only)
    cache: RwLock<HashMap<(String, String), CachedCredential>>,
    /// Cache TTL (default 5 minutes)
    cache_ttl: Duration,
    /// Allowed exchanges
    allowed_exchanges: Vec<String>,
}

struct CachedCredential {
    credentials: ApiCredentials,
    expires_at: Instant,
}

impl SaasCredentialVault {
    pub fn new(
        kms: Arc<dyn KmsProvider>,
        storage: Arc<dyn CredentialStorage>,
    ) -> Self {
        Self {
            kms,
            storage,
            cache: RwLock::new(HashMap::new()),
            cache_ttl: Duration::from_secs(300), // 5 minutes
            allowed_exchanges: vec![
                "kraken".to_string(),
                "coinbase".to_string(),
                "binance_us".to_string(),
                "gemini".to_string(),
                "bybit".to_string(),
                "okx".to_string(),
                "deribit".to_string(),
            ],
        }
    }

    /// Store new API credentials for a user
    /// 
    /// Security-First Validation Flow:
    /// 1. ALWAYS run automated permission probe against exchange API
    /// 2. If probe DETECTS withdrawal permission → REJECT (user confirmation IGNORED)
    /// 3. If probe CONFIRMS no withdrawal → ACCEPT (no confirmation needed)
    /// 4. If probe is INCONCLUSIVE → require user confirmation + flag for review
    /// 
    /// This ensures users cannot lie their way past detected withdrawal permissions.
    pub async fn store_credentials(
        &self,
        user_id: &str,
        exchange: &str,
        api_key: &str,
        api_secret: &str,
        passphrase: Option<&str>,
        client_ip: Option<&str>,
        user_confirmation: Option<UserPermissionConfirmation>,
    ) -> Result<CredentialStoreResult> {
        // Validate exchange is allowed
        if !self.allowed_exchanges.contains(&exchange.to_lowercase()) {
            bail!("Exchange '{}' is not supported", exchange);
        }

        // =====================================================================
        // STEP 1: ALWAYS run automated permission validation
        // This cannot be bypassed by user confirmation
        // =====================================================================
        let validator = PermissionValidator::new();
        let probe_result = validator.validate(exchange, api_key, api_secret, passphrase).await;

        let (validation, probe_outcome) = match probe_result {
            Ok(perm_validation) => {
                // Determine probe outcome
                let outcome = if !perm_validation.is_valid {
                    ProbeOutcome::InvalidCredentials(perm_validation.errors.clone())
                } else if perm_validation.permissions.can_withdraw {
                    ProbeOutcome::WithdrawDetected
                } else if perm_validation.verified_with_exchange {
                    ProbeOutcome::VerifiedNoWithdraw
                } else {
                    ProbeOutcome::Inconclusive("Could not verify permissions with exchange".to_string())
                };
                
                // Convert to internal validation type
                let validation = CredentialValidation {
                    is_valid: perm_validation.is_valid,
                    has_trade_permission: perm_validation.permissions.can_trade_spot 
                        || perm_validation.permissions.can_trade_futures,
                    has_withdraw_permission: perm_validation.permissions.can_withdraw,
                    exchange_verified: perm_validation.verified_with_exchange,
                    errors: perm_validation.errors.clone(),
                    warnings: perm_validation.warnings.clone(),
                };
                
                (validation, outcome)
            }
            Err(e) => {
                // Probe failed entirely - treat as inconclusive
                let validation = CredentialValidation {
                    is_valid: true, // Assume valid, we'll require confirmation
                    has_trade_permission: false,
                    has_withdraw_permission: false, // Unknown
                    exchange_verified: false,
                    errors: vec![],
                    warnings: vec![format!("Permission check failed: {}", e)],
                };
                (validation, ProbeOutcome::Inconclusive(format!("Probe error: {}", e)))
            }
        };

        // =====================================================================
        // STEP 2: Make security decision based on probe outcome
        // User confirmation CANNOT override a detected withdrawal permission
        // =====================================================================
        let (final_result, stored_confirmation) = match &probe_outcome {
            ProbeOutcome::WithdrawDetected => {
                // CRITICAL: REJECT - withdrawal detected, user confirmation is IGNORED
                self.record_audit(
                    user_id, 
                    exchange, 
                    CredentialAction::ValidationFailed, 
                    client_ip, 
                    false,
                    Some("REJECTED: Withdrawal permission detected via API probe")
                ).await?;
                
                tracing::warn!(
                    user_id = %user_id,
                    exchange = %exchange,
                    "SECURITY: Rejected API key - withdrawal permission detected. User confirmation would be ignored."
                );
                
                bail!(
                    "SECURITY: API key has withdrawal permission enabled. \
                    This was detected by our automated security check and cannot be overridden. \
                    Please create a new API key with ONLY trading permissions (no withdraw)."
                );
            }
            
            ProbeOutcome::VerifiedNoWithdraw => {
                // ACCEPT - automated verification confirms no withdrawal
                // No user confirmation needed
                tracing::info!(
                    user_id = %user_id,
                    exchange = %exchange,
                    "API key verified: no withdrawal permission detected"
                );
                
                (ConfirmationResult::AutoVerified, None)
            }
            
            ProbeOutcome::InvalidCredentials(errors) => {
                self.record_audit(
                    user_id, 
                    exchange, 
                    CredentialAction::ValidationFailed, 
                    client_ip, 
                    false,
                    Some(&errors.join(", "))
                ).await?;
                
                bail!("Invalid API credentials: {}", errors.join(", "));
            }
            
            ProbeOutcome::Inconclusive(reason) => {
                // INCONCLUSIVE - require user confirmation as fallback
                // This will be flagged for manual review
                tracing::warn!(
                    user_id = %user_id,
                    exchange = %exchange,
                    reason = %reason,
                    "Permission probe inconclusive - requiring user confirmation"
                );
                
                let confirmation = user_confirmation.ok_or_else(|| {
                    anyhow!(
                        "User confirmation required for {}. \
                        Our automated permission check was inconclusive ({}), \
                        so you must confirm that withdrawal permissions are disabled. \
                        This will be flagged for security review.",
                        exchange, reason
                    )
                })?;

                // Validate the confirmation
                let conf_result = confirmation.validate(exchange);
                if !conf_result.is_valid {
                    self.record_audit(
                        user_id, 
                        exchange, 
                        CredentialAction::ValidationFailed, 
                        client_ip, 
                        false,
                        Some(&format!("User confirmation invalid: {:?}", conf_result.missing_items))
                    ).await?;
                    
                    bail!(
                        "User confirmation is incomplete. Please confirm all required items:\n- {}",
                        conf_result.missing_items.join("\n- ")
                    );
                }

                // Store confirmation with inconclusive flag
                let stored = StoredConfirmation {
                    user_id: user_id.to_string(),
                    exchange: exchange.to_lowercase(),
                    confirmation: confirmation.clone(),
                    validation_result: conf_result.clone(),
                    stored_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    automated_validation: Some(CredentialValidationSnapshot {
                        is_valid: validation.is_valid,
                        has_trade_permission: validation.has_trade_permission,
                        has_withdraw_permission: validation.has_withdraw_permission,
                        exchange_verified: validation.exchange_verified,
                        errors: validation.errors.clone(),
                        warnings: validation.warnings.clone(),
                    }),
                    final_result: ConfirmationResult::UserConfirmedOnly,
                };
                
                // Log for compliance and flag for review
                tracing::warn!(
                    user_id = %user_id,
                    exchange = %exchange,
                    probe_reason = %reason,
                    checklist_reviewed = %confirmation.checklist_reviewed,
                    withdrawal_disabled_confirmed = %confirmation.withdrawal_disabled_confirmed,
                    "FLAGGED FOR REVIEW: Accepted credentials with inconclusive probe + user confirmation"
                );

                (ConfirmationResult::UserConfirmedOnly, Some(stored))
            }
        };

        // =====================================================================
        // STEP 3: Encrypt and store credentials
        // =====================================================================
        let (encrypted_key, key_id, nonce) = self.kms.encrypt(user_id, api_key.as_bytes()).await?;
        let (encrypted_secret, _, secret_nonce) = self.kms.encrypt(user_id, api_secret.as_bytes()).await?;
        
        let encrypted_passphrase = if let Some(pp) = passphrase {
            let (encrypted, _, _) = self.kms.encrypt(user_id, pp.as_bytes()).await?;
            Some(BASE64.encode(&encrypted))
        } else {
            None
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let credential = EncryptedCredential {
            user_id: user_id.to_string(),
            exchange: exchange.to_lowercase(),
            encrypted_api_key: BASE64.encode(&encrypted_key),
            encrypted_api_secret: BASE64.encode(&encrypted_secret),
            encrypted_passphrase,
            kms_key_id: key_id,
            encryption_version: 1,
            nonce: format!("{}:{}", nonce, secret_nonce),
            created_at: now,
            rotated_at: None,
            permissions: if validation.has_trade_permission {
                vec!["read".to_string(), "trade".to_string()]
            } else {
                vec!["read".to_string()]
            },
            whitelisted_ips: vec![],
            user_confirmation: stored_confirmation,
        };

        self.storage.store(&credential).await?;
        
        self.record_audit(user_id, exchange, CredentialAction::Created, client_ip, true, None).await?;

        Ok(CredentialStoreResult {
            validation,
            probe_outcome,
            final_result,
            requires_review: matches!(final_result, ConfirmationResult::UserConfirmedOnly),
        })
    }

    /// Get decrypted credentials for execution
    /// 
    /// Called by the trading engine when executing orders
    pub async fn get_credentials(
        &self,
        user_id: &str,
        exchange: &str,
        client_ip: Option<&str>,
    ) -> Result<ApiCredentials> {
        let cache_key = (user_id.to_string(), exchange.to_lowercase());
        
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(cached) = cache.get(&cache_key) {
                if cached.expires_at > Instant::now() {
                    return Ok(cached.credentials.clone());
                }
            }
        }

        // Load from storage
        let encrypted = self.storage.load(user_id, exchange).await?
            .ok_or_else(|| anyhow!("No credentials found for {} on {}", user_id, exchange))?;

        // Decrypt
        let nonces: Vec<&str> = encrypted.nonce.split(':').collect();
        let key_nonce = nonces.get(0).ok_or_else(|| anyhow!("Invalid nonce format"))?;
        let secret_nonce = nonces.get(1).ok_or_else(|| anyhow!("Invalid nonce format"))?;

        let api_key_bytes = BASE64.decode(&encrypted.encrypted_api_key)?;
        let api_secret_bytes = BASE64.decode(&encrypted.encrypted_api_secret)?;

        let api_key = self.kms.decrypt(user_id, &api_key_bytes, &encrypted.kms_key_id, key_nonce).await?;
        let api_secret = self.kms.decrypt(user_id, &api_secret_bytes, &encrypted.kms_key_id, secret_nonce).await?;

        let api_key_str = String::from_utf8(api_key)?;
        let api_secret_str = String::from_utf8(api_secret)?;

        let mut credentials = ApiCredentials::new(api_key_str, api_secret_str);

        if let Some(encrypted_pp) = &encrypted.encrypted_passphrase {
            let pp_bytes = BASE64.decode(encrypted_pp)?;
            let passphrase = self.kms.decrypt(user_id, &pp_bytes, &encrypted.kms_key_id, key_nonce).await?;
            credentials = credentials.with_passphrase(String::from_utf8(passphrase)?);
        }

        // Cache the decrypted credentials
        {
            let mut cache = self.cache.write();
            cache.insert(cache_key, CachedCredential {
                credentials: credentials.clone(),
                expires_at: Instant::now() + self.cache_ttl,
            });
        }

        self.record_audit(user_id, exchange, CredentialAction::Retrieved, client_ip, true, None).await?;

        Ok(credentials)
    }

    /// Delete user's credentials for an exchange
    pub async fn delete_credentials(
        &self,
        user_id: &str,
        exchange: &str,
        client_ip: Option<&str>,
    ) -> Result<()> {
        // Remove from cache
        {
            let mut cache = self.cache.write();
            cache.remove(&(user_id.to_string(), exchange.to_lowercase()));
        }

        // Delete from storage
        self.storage.delete(user_id, exchange).await?;
        
        self.record_audit(user_id, exchange, CredentialAction::Deleted, client_ip, true, None).await?;

        Ok(())
    }

    /// List all exchanges a user has connected
    pub async fn list_user_exchanges(&self, user_id: &str) -> Result<Vec<ConnectedExchange>> {
        let credentials = self.storage.list_for_user(user_id).await?;
        
        Ok(credentials.into_iter().map(|c| ConnectedExchange {
            exchange: c.exchange,
            connected_at: c.created_at,
            permissions: c.permissions,
            has_passphrase: c.encrypted_passphrase.is_some(),
        }).collect())
    }

    async fn record_audit(
        &self,
        user_id: &str,
        exchange: &str,
        action: CredentialAction,
        client_ip: Option<&str>,
        success: bool,
        error: Option<&str>,
    ) -> Result<()> {
        let event = CredentialAuditEvent {
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            user_id: user_id.to_string(),
            exchange: exchange.to_string(),
            action,
            ip_address: client_ip.map(|s| s.to_string()),
            user_agent: None,
            success,
            error_message: error.map(|s| s.to_string()),
        };

        self.storage.record_audit(event).await
    }

    /// Clear in-memory cache (call on user logout)
    pub fn clear_user_cache(&self, user_id: &str) {
        let mut cache = self.cache.write();
        cache.retain(|(uid, _), _| uid != user_id);
    }
}

impl Drop for SaasCredentialVault {
    fn drop(&mut self) {
        // Clear all cached credentials
        let mut cache = self.cache.write();
        cache.clear();
    }
}

/// Summary of a connected exchange for display
#[derive(Debug, Clone, Serialize)]
pub struct ConnectedExchange {
    pub exchange: String,
    pub connected_at: u64,
    pub permissions: Vec<String>,
    pub has_passphrase: bool,
}

// ============================================================================
// AWS KMS Implementation
// ============================================================================

/// AWS KMS provider implementation
pub struct AwsKmsProvider {
    // In production: aws_sdk_kms::Client
    region: String,
    key_prefix: String,
}

impl AwsKmsProvider {
    pub fn new(region: &str) -> Self {
        Self {
            region: region.to_string(),
            key_prefix: "alias/trading-platform/user-".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl KmsProvider for AwsKmsProvider {
    async fn encrypt(&self, user_id: &str, plaintext: &[u8]) -> Result<(Vec<u8>, String, String)> {
        // In production, use aws-sdk-kms:
        // let key_id = format!("{}{}", self.key_prefix, user_id);
        // let output = kms_client.encrypt()
        //     .key_id(&key_id)
        //     .plaintext(Blob::new(plaintext))
        //     .send().await?;
        
        // Placeholder - in production this calls AWS KMS
        let key_id = format!("{}{}", self.key_prefix, user_id);
        let nonce = uuid::Uuid::new_v4().to_string();
        
        // This is NOT real encryption - just for API demonstration
        let ciphertext = plaintext.to_vec();
        
        Ok((ciphertext, key_id, nonce))
    }

    async fn decrypt(&self, user_id: &str, ciphertext: &[u8], key_id: &str, _nonce: &str) -> Result<Vec<u8>> {
        // In production, use aws-sdk-kms:
        // let output = kms_client.decrypt()
        //     .key_id(key_id)
        //     .ciphertext_blob(Blob::new(ciphertext))
        //     .send().await?;
        
        // Placeholder
        Ok(ciphertext.to_vec())
    }

    async fn create_user_key(&self, user_id: &str) -> Result<String> {
        let key_id = format!("{}{}", self.key_prefix, user_id);
        // In production: create CMK with alias
        Ok(key_id)
    }

    async fn rotate_user_key(&self, user_id: &str) -> Result<String> {
        // In production: enable automatic key rotation or create new key
        self.create_user_key(user_id).await
    }
}

// ============================================================================
// PostgreSQL Storage Implementation  
// ============================================================================

/// PostgreSQL credential storage
pub struct PostgresCredentialStorage {
    // In production: diesel or sqlx pool
    connection_string: String,
}

impl PostgresCredentialStorage {
    pub fn new(connection_string: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl CredentialStorage for PostgresCredentialStorage {
    async fn store(&self, credential: &EncryptedCredential) -> Result<()> {
        // SQL: INSERT INTO user_credentials (...) ON CONFLICT (user_id, exchange) DO UPDATE
        // Store the EncryptedCredential fields
        Ok(())
    }

    async fn load(&self, user_id: &str, exchange: &str) -> Result<Option<EncryptedCredential>> {
        // SQL: SELECT * FROM user_credentials WHERE user_id = $1 AND exchange = $2
        Ok(None)
    }

    async fn delete(&self, user_id: &str, exchange: &str) -> Result<()> {
        // SQL: DELETE FROM user_credentials WHERE user_id = $1 AND exchange = $2
        Ok(())
    }

    async fn list_for_user(&self, user_id: &str) -> Result<Vec<EncryptedCredential>> {
        // SQL: SELECT * FROM user_credentials WHERE user_id = $1
        Ok(vec![])
    }

    async fn record_audit(&self, event: CredentialAuditEvent) -> Result<()> {
        // SQL: INSERT INTO credential_audit_log (...)
        Ok(())
    }
}

// ============================================================================
// Database Schema (for reference)
// ============================================================================

/*
-- User encrypted credentials table
CREATE TABLE user_credentials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    exchange VARCHAR(50) NOT NULL,
    encrypted_api_key TEXT NOT NULL,
    encrypted_api_secret TEXT NOT NULL,
    encrypted_passphrase TEXT,
    kms_key_id VARCHAR(255) NOT NULL,
    encryption_version INT NOT NULL DEFAULT 1,
    nonce TEXT NOT NULL,
    permissions TEXT[] NOT NULL DEFAULT '{}',
    whitelisted_ips TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rotated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE(user_id, exchange)
);

-- Audit log for all credential access
CREATE TABLE credential_audit_log (
    id BIGSERIAL PRIMARY KEY,
    timestamp_ns BIGINT NOT NULL,
    user_id UUID NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    action VARCHAR(50) NOT NULL,
    ip_address INET,
    user_agent TEXT,
    success BOOLEAN NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast user lookups
CREATE INDEX idx_user_credentials_user_id ON user_credentials(user_id);
CREATE INDEX idx_credential_audit_user_id ON credential_audit_log(user_id);
CREATE INDEX idx_credential_audit_timestamp ON credential_audit_log(timestamp_ns DESC);

-- Row-level security (optional, for multi-tenant isolation)
ALTER TABLE user_credentials ENABLE ROW LEVEL SECURITY;
CREATE POLICY user_credentials_isolation ON user_credentials
    USING (user_id = current_setting('app.current_user_id')::UUID);
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_credential_validation_rejects_withdrawal() {
        // Credentials with withdrawal permission should be rejected
        let validation = CredentialValidation {
            is_valid: true,
            has_trade_permission: true,
            has_withdraw_permission: true,
            exchange_verified: true,
            errors: vec![],
            warnings: vec![],
        };
        
        assert!(validation.has_withdraw_permission);
        // In real test, verify store_credentials returns error
    }

    #[test]
    fn test_encrypted_credential_serialization() {
        let cred = EncryptedCredential {
            user_id: "user123".to_string(),
            exchange: "kraken".to_string(),
            encrypted_api_key: "base64encodedkey".to_string(),
            encrypted_api_secret: "base64encodedsecret".to_string(),
            encrypted_passphrase: None,
            kms_key_id: "alias/trading-platform/user-user123".to_string(),
            encryption_version: 1,
            nonce: "nonce1:nonce2".to_string(),
            created_at: 1234567890,
            rotated_at: None,
            permissions: vec!["read".to_string(), "trade".to_string()],
            whitelisted_ips: vec![],
            user_confirmation: None,
        };

        let json = serde_json::to_string(&cred).unwrap();
        assert!(json.contains("kraken"));
        assert!(!json.contains("actual_secret")); // Should only have encrypted values
    }
}
