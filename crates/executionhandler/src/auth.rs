/// Internal service authentication system
/// 
/// Provides JWT-based authentication for internal trading system services
/// to prevent unauthorized access to trading operations.
/// 
/// Security Features:
/// - HMAC-SHA256 signatures (cryptographically secure)
/// - Rate limiting for failed authentication attempts
/// - Automatic lockout after threshold
/// - Constant-time signature comparison
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use serde::{Deserialize, Serialize};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use dashmap::DashMap;

type HmacSha256 = Hmac<Sha256>;

/// Configuration for authentication rate limiting
#[derive(Debug, Clone)]
pub struct AuthRateLimitConfig {
    /// Maximum failed attempts before lockout
    pub max_failed_attempts: u32,
    /// Lockout duration after max failures
    pub lockout_duration: Duration,
    /// Window for counting failed attempts
    pub attempt_window: Duration,
    /// Whether to enable rate limiting
    pub enabled: bool,
}

impl Default for AuthRateLimitConfig {
    fn default() -> Self {
        Self {
            max_failed_attempts: 5,
            lockout_duration: Duration::from_secs(300), // 5 minutes
            attempt_window: Duration::from_secs(60),    // 1 minute window
            enabled: true,
        }
    }
}

/// Tracks failed authentication attempts for rate limiting
#[repr(C, align(64))]
pub struct FailedAttemptTracker {
    /// Number of failed attempts in current window
    failed_count: AtomicU32,
    /// Window start timestamp (unix millis)
    window_start_ms: AtomicU64,
    /// Lockout end timestamp (unix millis), 0 = not locked out
    lockout_until_ms: AtomicU64,
}

impl FailedAttemptTracker {
    pub fn new() -> Self {
        Self {
            failed_count: AtomicU32::new(0),
            window_start_ms: AtomicU64::new(0),
            lockout_until_ms: AtomicU64::new(0),
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64
    }

    /// Check if currently locked out
    pub fn is_locked_out(&self) -> bool {
        let lockout_until = self.lockout_until_ms.load(Ordering::Acquire);
        if lockout_until == 0 {
            return false;
        }
        Self::now_ms() < lockout_until
    }

    /// Record a failed attempt, returns whether now locked out
    pub fn record_failure(&self, config: &AuthRateLimitConfig) -> bool {
        if !config.enabled {
            return false;
        }

        let now = Self::now_ms();
        let window_start = self.window_start_ms.load(Ordering::Acquire);
        let window_end = window_start + config.attempt_window.as_millis() as u64;

        // Reset window if expired
        if now > window_end {
            self.window_start_ms.store(now, Ordering::Release);
            self.failed_count.store(1, Ordering::Release);
            return false;
        }

        // Increment failure count
        let count = self.failed_count.fetch_add(1, Ordering::AcqRel) + 1;

        // Check if threshold exceeded
        if count >= config.max_failed_attempts {
            let lockout_until = now + config.lockout_duration.as_millis() as u64;
            self.lockout_until_ms.store(lockout_until, Ordering::Release);
            return true;
        }

        false
    }

    /// Clear failed attempts (call on successful auth)
    pub fn clear(&self) {
        self.failed_count.store(0, Ordering::Release);
        self.lockout_until_ms.store(0, Ordering::Release);
    }

    /// Get remaining lockout time
    pub fn lockout_remaining(&self) -> Option<Duration> {
        let lockout_until = self.lockout_until_ms.load(Ordering::Acquire);
        if lockout_until == 0 {
            return None;
        }
        let now = Self::now_ms();
        if now >= lockout_until {
            return None;
        }
        Some(Duration::from_millis(lockout_until - now))
    }

    /// Get current failed attempt count
    pub fn failed_count(&self) -> u32 {
        self.failed_count.load(Ordering::Acquire)
    }
}

impl Default for FailedAttemptTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceClaims {
    /// Service identifier (e.g., "signal-generator", "execution-handler")
    pub service_id: String,
    /// Service permissions (e.g., ["read_positions", "submit_orders"])
    pub permissions: Vec<String>,
    /// Token expiration time (UNIX timestamp)
    pub exp: u64,
    /// Issued at (UNIX timestamp)
    pub iat: u64,
    /// Issuer (always "trading-platform")
    pub iss: String,
    /// Unique token ID for revocation support
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub jti: String,
}

#[derive(Debug)]
pub enum AuthError {
    InvalidToken,
    ExpiredToken,
    InsufficientPermissions,
    UnknownService,
    TokenGenerationFailed,
    RateLimited { retry_after: Duration },
    SignatureVerificationFailed,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::InvalidToken => write!(f, "Invalid authentication token"),
            AuthError::ExpiredToken => write!(f, "Authentication token has expired"),
            AuthError::InsufficientPermissions => write!(f, "Insufficient permissions for this operation"),
            AuthError::UnknownService => write!(f, "Unknown service identifier"),
            AuthError::TokenGenerationFailed => write!(f, "Failed to generate authentication token"),
            AuthError::RateLimited { retry_after } => {
                write!(f, "Too many failed attempts, retry after {} seconds", retry_after.as_secs())
            }
            AuthError::SignatureVerificationFailed => write!(f, "Token signature verification failed"),
        }
    }
}

impl std::error::Error for AuthError {}

pub struct TradingAuthenticator {
    /// Secret key for HMAC-SHA256 signing
    secret_key: Vec<u8>,
    /// Registered services and their permissions
    service_registry: HashMap<String, Vec<String>>,
    /// Token validity duration
    token_duration: Duration,
    /// Rate limiting configuration
    rate_limit_config: AuthRateLimitConfig,
    /// Failed attempt trackers per service/IP
    attempt_trackers: DashMap<String, FailedAttemptTracker>,
    /// Token revocation list (JTI -> revocation time)
    revoked_tokens: DashMap<String, u64>,
}

impl TradingAuthenticator {
    pub fn new(secret_key: String) -> Self {
        Self::with_config(secret_key, AuthRateLimitConfig::default())
    }

    pub fn with_config(secret_key: String, rate_limit_config: AuthRateLimitConfig) -> Self {
        let mut service_registry = HashMap::new();
        
        // Register core trading services with their permissions
        service_registry.insert("signal-generator".to_string(), vec![
            "generate_signals".to_string(),
            "read_market_data".to_string(),
        ]);
        
        service_registry.insert("execution-handler".to_string(), vec![
            "submit_orders".to_string(),
            "cancel_orders".to_string(),
            "read_positions".to_string(),
            "update_positions".to_string(),
        ]);
        
        service_registry.insert("portfolio-handler".to_string(), vec![
            "read_portfolio".to_string(),
            "update_portfolio".to_string(),
            "calculate_pnl".to_string(),
        ]);
        
        service_registry.insert("signal-dispatcher".to_string(), vec![
            "dispatch_signals".to_string(),
            "read_signals".to_string(),
        ]);
        
        service_registry.insert("risk-manager".to_string(), vec![
            "check_risk_limits".to_string(),
            "block_orders".to_string(),
            "read_positions".to_string(),
        ]);
        
        // Derive key using HKDF-like expansion for better security
        let secret_bytes = Self::derive_key(secret_key.as_bytes());
        
        Self {
            secret_key: secret_bytes,
            service_registry,
            token_duration: Duration::from_secs(3600), // 1 hour token validity
            rate_limit_config,
            attempt_trackers: DashMap::new(),
            revoked_tokens: DashMap::new(),
        }
    }

    /// Derive a fixed-size key from variable input using SHA-256
    fn derive_key(input: &[u8]) -> Vec<u8> {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(b"trading-platform-auth-key-v1");
        hasher.update(input);
        hasher.finalize().to_vec()
    }
    
    /// Generate authentication token for a service
    pub fn generate_token(&self, service_id: &str) -> Result<String, AuthError> {
        let permissions = self.service_registry.get(service_id)
            .ok_or(AuthError::UnknownService)?
            .clone();
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthError::TokenGenerationFailed)?
            .as_secs();
        
        // Generate unique token ID for revocation support
        let jti = format!("{:016x}{:016x}", now, rand::random::<u64>());
        
        let claims = ServiceClaims {
            service_id: service_id.to_string(),
            permissions,
            exp: now + self.token_duration.as_secs(),
            iat: now,
            iss: "trading-platform".to_string(),
            jti,
        };
        
        // Create JWT-like token with proper HMAC-SHA256 signature
        let header = r#"{"alg":"HS256","typ":"JWT"}"#;
        let payload = serde_json::to_string(&claims)
            .map_err(|_| AuthError::TokenGenerationFailed)?;
        
        let header_b64 = URL_SAFE_NO_PAD.encode(header);
        let payload_b64 = URL_SAFE_NO_PAD.encode(&payload);
        let signing_input = format!("{}.{}", header_b64, payload_b64);
        
        let signature = self.create_hmac_signature(signing_input.as_bytes());
        let signature_b64 = URL_SAFE_NO_PAD.encode(&signature);
        
        Ok(format!("{}.{}.{}", header_b64, payload_b64, signature_b64))
    }
    
    /// Validate authentication token and return claims
    pub fn validate_token(&self, token: &str) -> Result<ServiceClaims, AuthError> {
        self.validate_token_with_client(token, "default")
    }

    /// Validate token with client identifier for rate limiting
    pub fn validate_token_with_client(&self, token: &str, client_id: &str) -> Result<ServiceClaims, AuthError> {
        // Check rate limiting
        if let Some(tracker) = self.attempt_trackers.get(client_id) {
            if tracker.is_locked_out() {
                if let Some(remaining) = tracker.lockout_remaining() {
                    return Err(AuthError::RateLimited { retry_after: remaining });
                }
            }
        }

        let result = self.validate_token_internal(token);

        // Update rate limiting based on result
        match &result {
            Ok(_) => {
                // Clear failed attempts on success
                if let Some(tracker) = self.attempt_trackers.get(client_id) {
                    tracker.clear();
                }
            }
            Err(AuthError::InvalidToken | AuthError::SignatureVerificationFailed) => {
                // Record failed attempt
                let tracker = self.attempt_trackers
                    .entry(client_id.to_string())
                    .or_insert_with(FailedAttemptTracker::new);
                tracker.record_failure(&self.rate_limit_config);
            }
            _ => {}
        }

        result
    }

    fn validate_token_internal(&self, token: &str) -> Result<ServiceClaims, AuthError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(AuthError::InvalidToken);
        }
        
        let header_b64 = parts[0];
        let payload_b64 = parts[1];
        let signature_b64 = parts[2];
        
        // Verify signature first (constant-time comparison)
        let signing_input = format!("{}.{}", header_b64, payload_b64);
        let expected_signature = self.create_hmac_signature(signing_input.as_bytes());
        let provided_signature = URL_SAFE_NO_PAD.decode(signature_b64)
            .map_err(|_| AuthError::InvalidToken)?;
        
        if !constant_time_eq(&expected_signature, &provided_signature) {
            return Err(AuthError::SignatureVerificationFailed);
        }
        
        // Decode and parse payload
        let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64)
            .map_err(|_| AuthError::InvalidToken)?;
        let payload_str = String::from_utf8(payload_bytes)
            .map_err(|_| AuthError::InvalidToken)?;
        
        let claims: ServiceClaims = serde_json::from_str(&payload_str)
            .map_err(|_| AuthError::InvalidToken)?;
        
        // Check if token is revoked
        if !claims.jti.is_empty() && self.revoked_tokens.contains_key(&claims.jti) {
            return Err(AuthError::InvalidToken);
        }
        
        // Check expiration
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthError::InvalidToken)?
            .as_secs();
        
        if claims.exp < now {
            return Err(AuthError::ExpiredToken);
        }
        
        // Verify service is still registered
        if !self.service_registry.contains_key(&claims.service_id) {
            return Err(AuthError::UnknownService);
        }
        
        Ok(claims)
    }

    /// Create HMAC-SHA256 signature
    fn create_hmac_signature(&self, data: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(&self.secret_key)
            .expect("HMAC can take key of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }
    
    /// Check if token has specific permission
    pub fn check_permission(&self, token: &str, required_permission: &str) -> Result<(), AuthError> {
        let claims = self.validate_token(token)?;
        
        if claims.permissions.contains(&required_permission.to_string()) {
            Ok(())
        } else {
            Err(AuthError::InsufficientPermissions)
        }
    }
    
    /// Register a new service with permissions
    pub fn register_service(&mut self, service_id: String, permissions: Vec<String>) {
        self.service_registry.insert(service_id, permissions);
    }

    /// Revoke a token by its JTI
    pub fn revoke_token(&self, jti: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        self.revoked_tokens.insert(jti.to_string(), now);
    }

    /// Clean up expired revocations (call periodically)
    pub fn cleanup_revocations(&self, max_age: Duration) {
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs() - max_age.as_secs();
        
        self.revoked_tokens.retain(|_, &mut revoked_at| revoked_at > cutoff);
    }

    /// Get rate limit status for a client
    pub fn get_rate_limit_status(&self, client_id: &str) -> Option<(u32, Option<Duration>)> {
        self.attempt_trackers.get(client_id).map(|tracker| {
            (tracker.failed_count(), tracker.lockout_remaining())
        })
    }
}

/// Constant-time byte comparison to prevent timing attacks
#[inline]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

/// Authentication middleware for HTTP requests
#[derive(Clone)]
pub struct AuthMiddleware {
    authenticator: std::sync::Arc<TradingAuthenticator>,
}

impl AuthMiddleware {
    pub fn new(authenticator: TradingAuthenticator) -> Self {
        Self {
            authenticator: std::sync::Arc::new(authenticator),
        }
    }
    
    /// Extract and validate token from Authorization header
    pub fn authenticate_request(&self, auth_header: Option<&str>) -> Result<ServiceClaims, AuthError> {
        let auth_header = auth_header.ok_or(AuthError::InvalidToken)?;
        
        if !auth_header.starts_with("Bearer ") {
            return Err(AuthError::InvalidToken);
        }
        
        let token = &auth_header[7..]; // Remove "Bearer " prefix
        self.authenticator.validate_token(token)
    }
    
    /// Check if request has required permission
    pub fn require_permission(&self, auth_header: Option<&str>, permission: &str) -> Result<ServiceClaims, AuthError> {
        let claims = self.authenticate_request(auth_header)?;
        
        if claims.permissions.contains(&permission.to_string()) {
            Ok(claims)
        } else {
            Err(AuthError::InsufficientPermissions)
        }
    }
}

/// Global authentication system
pub static AUTHENTICATOR: std::sync::LazyLock<TradingAuthenticator> = std::sync::LazyLock::new(|| {
    let secret_key = std::env::var("TRADING_AUTH_SECRET")
        .unwrap_or_else(|_| "default-dev-secret-key-change-in-production".to_string());
    TradingAuthenticator::new(secret_key)
});

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_token_generation_and_validation() {
        let auth = TradingAuthenticator::new("test-secret".to_string());
        
        // Generate token
        let token = auth.generate_token("execution-handler").unwrap();
        assert!(!token.is_empty());
        
        // Should be JWT format (3 parts separated by dots)
        assert_eq!(token.split('.').count(), 3);
        
        // Validate token
        let claims = auth.validate_token(&token).unwrap();
        assert_eq!(claims.service_id, "execution-handler");
        assert!(claims.permissions.contains(&"submit_orders".to_string()));
        assert!(!claims.jti.is_empty()); // Should have JTI
    }
    
    #[test]
    fn test_permission_checking() {
        let auth = TradingAuthenticator::new("test-secret".to_string());
        let token = auth.generate_token("execution-handler").unwrap();
        
        // Should have permission
        assert!(auth.check_permission(&token, "submit_orders").is_ok());
        
        // Should not have permission
        assert!(auth.check_permission(&token, "admin_access").is_err());
    }
    
    #[test]
    fn test_invalid_token() {
        let auth = TradingAuthenticator::new("test-secret".to_string());
        
        assert!(auth.validate_token("invalid-token").is_err());
        assert!(auth.validate_token("").is_err());
        assert!(auth.validate_token("a.b").is_err()); // Only 2 parts
    }
    
    #[test]
    fn test_unknown_service() {
        let auth = TradingAuthenticator::new("test-secret".to_string());
        
        assert!(auth.generate_token("unknown-service").is_err());
    }

    #[test]
    fn test_signature_tampering_detection() {
        let auth = TradingAuthenticator::new("test-secret".to_string());
        let token = auth.generate_token("execution-handler").unwrap();
        
        // Tamper with payload
        let mut parts: Vec<&str> = token.split('.').collect();
        parts[1] = "dGFtcGVyZWQ"; // "tampered" base64
        let tampered_token = parts.join(".");
        
        match auth.validate_token(&tampered_token) {
            Err(AuthError::SignatureVerificationFailed) | Err(AuthError::InvalidToken) => (),
            other => panic!("Expected signature verification failure, got: {:?}", other),
        }
    }

    #[test]
    fn test_different_secrets_produce_different_signatures() {
        let auth1 = TradingAuthenticator::new("secret-1".to_string());
        let auth2 = TradingAuthenticator::new("secret-2".to_string());
        
        let token1 = auth1.generate_token("execution-handler").unwrap();
        
        // Token from auth1 should not validate with auth2
        assert!(auth2.validate_token(&token1).is_err());
    }

    #[test]
    fn test_rate_limiting_lockout() {
        let config = AuthRateLimitConfig {
            max_failed_attempts: 3,
            lockout_duration: Duration::from_secs(10),
            attempt_window: Duration::from_secs(60),
            enabled: true,
        };
        let auth = TradingAuthenticator::with_config("test-secret".to_string(), config);
        
        let client_id = "test-client";
        
        // Make failed attempts
        for _ in 0..3 {
            let _ = auth.validate_token_with_client("invalid-token", client_id);
        }
        
        // Should be locked out now
        match auth.validate_token_with_client("invalid-token", client_id) {
            Err(AuthError::RateLimited { retry_after }) => {
                assert!(retry_after.as_secs() > 0);
            }
            other => panic!("Expected rate limited error, got: {:?}", other),
        }
    }

    #[test]
    fn test_rate_limiting_clears_on_success() {
        let config = AuthRateLimitConfig {
            max_failed_attempts: 5,
            lockout_duration: Duration::from_secs(10),
            attempt_window: Duration::from_secs(60),
            enabled: true,
        };
        let auth = TradingAuthenticator::with_config("test-secret".to_string(), config);
        
        let client_id = "test-client-2";
        
        // Make some failed attempts (but not enough to lock out)
        for _ in 0..2 {
            let _ = auth.validate_token_with_client("invalid-token", client_id);
        }
        
        // Check status
        let (count, _) = auth.get_rate_limit_status(client_id).unwrap();
        assert_eq!(count, 2);
        
        // Successful validation should clear
        let token = auth.generate_token("execution-handler").unwrap();
        assert!(auth.validate_token_with_client(&token, client_id).is_ok());
        
        let (count, _) = auth.get_rate_limit_status(client_id).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_token_revocation() {
        let auth = TradingAuthenticator::new("test-secret".to_string());
        let token = auth.generate_token("execution-handler").unwrap();
        
        // Token should be valid
        let claims = auth.validate_token(&token).unwrap();
        let jti = claims.jti.clone();
        
        // Revoke the token
        auth.revoke_token(&jti);
        
        // Token should now be invalid
        assert!(auth.validate_token(&token).is_err());
    }

    #[test]
    fn test_failed_attempt_tracker() {
        let tracker = FailedAttemptTracker::new();
        let config = AuthRateLimitConfig {
            max_failed_attempts: 3,
            lockout_duration: Duration::from_secs(10),
            attempt_window: Duration::from_secs(60),
            enabled: true,
        };
        
        assert!(!tracker.is_locked_out());
        assert_eq!(tracker.failed_count(), 0);
        
        // Record failures
        assert!(!tracker.record_failure(&config)); // 1
        assert!(!tracker.record_failure(&config)); // 2
        assert!(tracker.record_failure(&config));  // 3 - should lock
        
        assert!(tracker.is_locked_out());
        assert!(tracker.lockout_remaining().is_some());
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn test_auth_middleware() {
        let auth = TradingAuthenticator::new("middleware-test-secret".to_string());
        let middleware = AuthMiddleware::new(auth);
        
        // Generate a valid token using the same authenticator inside the middleware
        let inner_auth = TradingAuthenticator::new("middleware-test-secret".to_string());
        let token = inner_auth.generate_token("execution-handler").unwrap();
        let auth_header = format!("Bearer {}", token);
        
        // Valid header
        let claims = middleware.authenticate_request(Some(&auth_header)).unwrap();
        assert_eq!(claims.service_id, "execution-handler");
        
        // Missing header
        assert!(middleware.authenticate_request(None).is_err());
        
        // Invalid prefix
        assert!(middleware.authenticate_request(Some("Basic token")).is_err());
    }
}