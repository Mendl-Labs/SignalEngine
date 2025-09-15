/// Internal service authentication system
/// 
/// Provides JWT-based authentication for internal trading system services
/// to prevent unauthorized access to trading operations.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug)]
pub enum AuthError {
    InvalidToken,
    ExpiredToken,
    InsufficientPermissions,
    UnknownService,
    TokenGenerationFailed,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::InvalidToken => write!(f, "Invalid authentication token"),
            AuthError::ExpiredToken => write!(f, "Authentication token has expired"),
            AuthError::InsufficientPermissions => write!(f, "Insufficient permissions for this operation"),
            AuthError::UnknownService => write!(f, "Unknown service identifier"),
            AuthError::TokenGenerationFailed => write!(f, "Failed to generate authentication token"),
        }
    }
}

impl std::error::Error for AuthError {}

pub struct TradingAuthenticator {
    /// Secret key for JWT signing (in production, load from secure environment)
    secret_key: String,
    /// Registered services and their permissions
    service_registry: HashMap<String, Vec<String>>,
    /// Token validity duration
    token_duration: Duration,
}

impl TradingAuthenticator {
    pub fn new(secret_key: String) -> Self {
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
        
        Self {
            secret_key,
            service_registry,
            token_duration: Duration::from_hours(1), // 1 hour token validity
        }
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
        
        let claims = ServiceClaims {
            service_id: service_id.to_string(),
            permissions,
            exp: now + self.token_duration.as_secs(),
            iat: now,
            iss: "trading-platform".to_string(),
        };
        
        // Simple JWT-like token (in production, use proper JWT library)
        let payload = serde_json::to_string(&claims)
            .map_err(|_| AuthError::TokenGenerationFailed)?;
        
        let signature = self.create_signature(&payload);
        let token = format!("{}:{}", self.simple_base64_encode(&payload), signature);
        
        Ok(token)
    }
    
    /// Validate authentication token and return claims
    pub fn validate_token(&self, token: &str) -> Result<ServiceClaims, AuthError> {
        let parts: Vec<&str> = token.split(':').collect();
        if parts.len() != 2 {
            return Err(AuthError::InvalidToken);
        }
        
        let payload = self.simple_base64_decode(parts[0])
            .map_err(|_| AuthError::InvalidToken)?;
        let payload_str = String::from_utf8(payload)
            .map_err(|_| AuthError::InvalidToken)?;
        
        // Verify signature
        let expected_signature = self.create_signature(&payload_str);
        if parts[1] != expected_signature {
            return Err(AuthError::InvalidToken);
        }
        
        let claims: ServiceClaims = serde_json::from_str(&payload_str)
            .map_err(|_| AuthError::InvalidToken)?;
        
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
    
    /// Create HMAC-like signature (simplified for demo - use proper HMAC in production)
    fn create_signature(&self, data: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        self.secret_key.hash(&mut hasher);
        
        format!("{:x}", hasher.finish())
    }
    
    /// Simple base64-like encoding (simplified for demo)
    fn simple_base64_encode(&self, data: &str) -> String {
        data.bytes().map(|b| format!("{:02x}", b)).collect::<String>()
    }
    
    /// Simple base64-like decoding (simplified for demo)  
    fn simple_base64_decode(&self, encoded: &str) -> Result<Vec<u8>, ()> {
        if encoded.len() % 2 != 0 {
            return Err(());
        }
        
        let mut result = Vec::new();
        for chunk in encoded.chars().collect::<Vec<char>>().chunks(2) {
            let hex_str: String = chunk.iter().collect();
            if let Ok(byte) = u8::from_str_radix(&hex_str, 16) {
                result.push(byte);
            } else {
                return Err(());
            }
        }
        Ok(result)
    }
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
        
        // Validate token
        let claims = auth.validate_token(&token).unwrap();
        assert_eq!(claims.service_id, "execution-handler");
        assert!(claims.permissions.contains(&"submit_orders".to_string()));
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
    }
    
    #[test]
    fn test_unknown_service() {
        let auth = TradingAuthenticator::new("test-secret".to_string());
        
        assert!(auth.generate_token("unknown-service").is_err());
    }
}