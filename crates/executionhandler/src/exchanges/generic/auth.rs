//! Authentication strategies for different exchanges.
//!
//! Each exchange has its own signing method:
//! - HMAC-SHA256: Binance, Bybit, OKX, Deribit
//! - HMAC-SHA512: Kraken, Gemini
//! - HMAC-SHA256 + Passphrase: Coinbase, OKX

use async_trait::async_trait;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Sha256, Sha512, Digest};
use std::collections::HashMap;

use crate::core::types::ExecutionError;
use super::config::{AuthMethod, SignatureLocation};

/// Authentication headers result
#[derive(Debug, Clone)]
pub struct AuthHeaders {
    pub headers: HashMap<String, String>,
    pub query_params: HashMap<String, String>,
    pub body_params: HashMap<String, String>,
}

impl AuthHeaders {
    pub fn new() -> Self {
        Self {
            headers: HashMap::new(),
            query_params: HashMap::new(),
            body_params: HashMap::new(),
        }
    }
}

impl Default for AuthHeaders {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for authentication strategies
#[async_trait]
pub trait AuthStrategy: Send + Sync {
    /// Sign a request and return the authentication headers/params
    async fn sign(
        &self,
        method: &str,
        path: &str,
        body: &str,
        timestamp: u64,
    ) -> Result<AuthHeaders, ExecutionError>;
}

/// HMAC-SHA256 authentication (Binance, Bybit)
pub struct HmacSha256Auth {
    api_key: String,
    secret_key: String,
    api_key_header: String,
    signature_header: String,
    timestamp_header: String,
    timestamp_ms: bool,
    signature_location: SignatureLocation,
}

impl HmacSha256Auth {
    pub fn new(
        api_key: String,
        secret_key: String,
        auth_method: &AuthMethod,
    ) -> Result<Self, ExecutionError> {
        match auth_method {
            AuthMethod::HmacSha256 {
                api_key_header,
                signature_header,
                timestamp_header,
                timestamp_ms,
                signature_location,
            } => Ok(Self {
                api_key,
                secret_key,
                api_key_header: api_key_header.clone(),
                signature_header: signature_header.clone(),
                timestamp_header: timestamp_header.clone(),
                timestamp_ms: *timestamp_ms,
                signature_location: signature_location.clone(),
            }),
            _ => Err(ExecutionError::Authentication(
                "Invalid auth method for HmacSha256Auth".to_string(),
            )),
        }
    }
    
    fn compute_signature(&self, message: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret_key.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }
}

#[async_trait]
impl AuthStrategy for HmacSha256Auth {
    async fn sign(
        &self,
        _method: &str,
        _path: &str,
        body: &str,
        timestamp: u64,
    ) -> Result<AuthHeaders, ExecutionError> {
        let mut auth = AuthHeaders::new();
        
        // Add API key header
        auth.headers.insert(self.api_key_header.clone(), self.api_key.clone());
        
        // Add timestamp
        let ts_str = if self.timestamp_ms {
            timestamp.to_string()
        } else {
            (timestamp / 1000).to_string()
        };
        
        // Build signature message based on exchange
        // For Binance-style: timestamp + query_string
        let message = if body.is_empty() {
            format!("{}={}", self.timestamp_header, ts_str)
        } else {
            format!("{}&{}={}", body, self.timestamp_header, ts_str)
        };
        
        let signature = self.compute_signature(&message);
        
        match self.signature_location {
            SignatureLocation::Query => {
                auth.query_params.insert(self.timestamp_header.clone(), ts_str);
                auth.query_params.insert(self.signature_header.clone(), signature);
            }
            SignatureLocation::Header => {
                auth.headers.insert(self.timestamp_header.clone(), ts_str);
                auth.headers.insert(self.signature_header.clone(), signature);
            }
            SignatureLocation::Body => {
                auth.body_params.insert(self.timestamp_header.clone(), ts_str);
                auth.body_params.insert(self.signature_header.clone(), signature);
            }
        }
        
        Ok(auth)
    }
}

/// HMAC-SHA512 authentication (Kraken, Gemini)
pub struct HmacSha512Auth {
    api_key: String,
    secret_key: Vec<u8>, // Base64 decoded secret
    api_key_header: String,
    signature_header: String,
    use_nonce: bool,
}

impl HmacSha512Auth {
    pub fn new(
        api_key: String,
        secret_key: String,
        auth_method: &AuthMethod,
    ) -> Result<Self, ExecutionError> {
        match auth_method {
            AuthMethod::HmacSha512 {
                api_key_header,
                signature_header,
                use_nonce,
            } => {
                // Kraken secret is base64 encoded
                let decoded_secret = base64::engine::general_purpose::STANDARD
                    .decode(&secret_key)
                    .map_err(|e| ExecutionError::Authentication(format!("Invalid secret key: {}", e)))?;
                
                Ok(Self {
                    api_key,
                    secret_key: decoded_secret,
                    api_key_header: api_key_header.clone(),
                    signature_header: signature_header.clone(),
                    use_nonce: *use_nonce,
                })
            }
            _ => Err(ExecutionError::Authentication(
                "Invalid auth method for HmacSha512Auth".to_string(),
            )),
        }
    }
    
    fn compute_kraken_signature(&self, path: &str, nonce: u64, body: &str) -> String {
        // Kraken signature: HMAC-SHA512(path + SHA256(nonce + body), secret)
        let nonce_body = format!("{}{}", nonce, body);
        let mut sha256 = Sha256::new();
        sha256.update(nonce_body.as_bytes());
        let sha256_result = sha256.finalize();
        
        let mut message = path.as_bytes().to_vec();
        message.extend_from_slice(&sha256_result);
        
        let mut mac = Hmac::<Sha512>::new_from_slice(&self.secret_key)
            .expect("HMAC can take key of any size");
        mac.update(&message);
        let result = mac.finalize();
        
        base64::engine::general_purpose::STANDARD.encode(result.into_bytes())
    }
}

#[async_trait]
impl AuthStrategy for HmacSha512Auth {
    async fn sign(
        &self,
        _method: &str,
        path: &str,
        body: &str,
        timestamp: u64,
    ) -> Result<AuthHeaders, ExecutionError> {
        let mut auth = AuthHeaders::new();
        
        // Add API key header
        auth.headers.insert(self.api_key_header.clone(), self.api_key.clone());
        
        // Compute signature
        let nonce = if self.use_nonce { timestamp } else { 0 };
        let signature = self.compute_kraken_signature(path, nonce, body);
        
        auth.headers.insert(self.signature_header.clone(), signature);
        
        Ok(auth)
    }
}

/// HMAC-SHA256 with passphrase (Coinbase, OKX)
pub struct HmacSha256PassphraseAuth {
    api_key: String,
    secret_key: Vec<u8>,
    passphrase: String,
    api_key_header: String,
    signature_header: String,
    passphrase_header: String,
    timestamp_header: String,
}

impl HmacSha256PassphraseAuth {
    pub fn new(
        api_key: String,
        secret_key: String,
        passphrase: String,
        auth_method: &AuthMethod,
    ) -> Result<Self, ExecutionError> {
        match auth_method {
            AuthMethod::HmacSha256WithPassphrase {
                api_key_header,
                signature_header,
                passphrase_header,
                timestamp_header,
            } => {
                // Secret is base64 encoded
                let decoded_secret = base64::engine::general_purpose::STANDARD
                    .decode(&secret_key)
                    .map_err(|e| ExecutionError::Authentication(format!("Invalid secret key: {}", e)))?;
                
                Ok(Self {
                    api_key,
                    secret_key: decoded_secret,
                    passphrase,
                    api_key_header: api_key_header.clone(),
                    signature_header: signature_header.clone(),
                    passphrase_header: passphrase_header.clone(),
                    timestamp_header: timestamp_header.clone(),
                })
            }
            _ => Err(ExecutionError::Authentication(
                "Invalid auth method for HmacSha256PassphraseAuth".to_string(),
            )),
        }
    }
    
    fn compute_signature(&self, timestamp: &str, method: &str, path: &str, body: &str) -> String {
        // Coinbase/OKX: HMAC-SHA256(timestamp + method + path + body, secret)
        let message = format!("{}{}{}{}", timestamp, method, path, body);
        
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret_key)
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        let result = mac.finalize();
        
        base64::engine::general_purpose::STANDARD.encode(result.into_bytes())
    }
}

#[async_trait]
impl AuthStrategy for HmacSha256PassphraseAuth {
    async fn sign(
        &self,
        method: &str,
        path: &str,
        body: &str,
        timestamp: u64,
    ) -> Result<AuthHeaders, ExecutionError> {
        let mut auth = AuthHeaders::new();
        
        // Timestamp as ISO string or seconds
        let ts_str = (timestamp / 1000).to_string();
        
        // Compute signature
        let signature = self.compute_signature(&ts_str, method, path, body);
        
        // Add headers
        auth.headers.insert(self.api_key_header.clone(), self.api_key.clone());
        auth.headers.insert(self.signature_header.clone(), signature);
        auth.headers.insert(self.passphrase_header.clone(), self.passphrase.clone());
        auth.headers.insert(self.timestamp_header.clone(), ts_str);
        
        Ok(auth)
    }
}

/// Plain API-key header auth (Alpaca) — injects key+secret as headers, no signing.
pub struct ApiKeyHeaderAuth {
    api_key: String,
    api_secret: String,
    api_key_header: String,
    api_secret_header: String,
}

#[async_trait]
impl AuthStrategy for ApiKeyHeaderAuth {
    async fn sign(
        &self,
        _method: &str,
        _path: &str,
        _body: &str,
        _timestamp: u64,
    ) -> Result<AuthHeaders, ExecutionError> {
        let mut auth = AuthHeaders::new();
        auth.headers.insert(self.api_key_header.clone(), self.api_key.clone());
        auth.headers.insert(self.api_secret_header.clone(), self.api_secret.clone());
        Ok(auth)
    }
}

/// Factory function to create the appropriate auth strategy
pub fn create_auth_strategy(
    auth_method: &AuthMethod,
    api_key: String,
    secret_key: String,
    passphrase: Option<String>,
) -> Result<Box<dyn AuthStrategy>, ExecutionError> {
    match auth_method {
        AuthMethod::HmacSha256 { .. } => {
            Ok(Box::new(HmacSha256Auth::new(api_key, secret_key, auth_method)?))
        }
        AuthMethod::HmacSha512 { .. } => {
            Ok(Box::new(HmacSha512Auth::new(api_key, secret_key, auth_method)?))
        }
        AuthMethod::HmacSha256WithPassphrase { .. } => {
            let pass = passphrase.ok_or_else(|| {
                ExecutionError::Authentication("Passphrase required for this exchange".to_string())
            })?;
            Ok(Box::new(HmacSha256PassphraseAuth::new(api_key, secret_key, pass, auth_method)?))
        }
        AuthMethod::ApiKeyHeader { api_key_header, api_secret_header } => {
            Ok(Box::new(ApiKeyHeaderAuth {
                api_key,
                api_secret: secret_key,
                api_key_header: api_key_header.clone(),
                api_secret_header: api_secret_header.clone(),
            }))
        }
        AuthMethod::Rsa { .. } | AuthMethod::Ed25519 { .. } => {
            Err(ExecutionError::Authentication(
                "RSA and Ed25519 auth not yet implemented".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::config::{AuthMethod, SignatureLocation};

    fn binance_auth_method() -> AuthMethod {
        AuthMethod::HmacSha256 {
            api_key_header: "X-MBX-APIKEY".to_string(),
            signature_header: "signature".to_string(),
            timestamp_header: "timestamp".to_string(),
            timestamp_ms: true,
            signature_location: SignatureLocation::Query,
        }
    }

    fn kraken_auth_method() -> AuthMethod {
        AuthMethod::HmacSha512 {
            api_key_header: "API-Key".to_string(),
            signature_header: "API-Sign".to_string(),
            use_nonce: true,
        }
    }

    fn coinbase_auth_method() -> AuthMethod {
        AuthMethod::HmacSha256WithPassphrase {
            api_key_header: "CB-ACCESS-KEY".to_string(),
            signature_header: "CB-ACCESS-SIGN".to_string(),
            passphrase_header: "CB-ACCESS-PASSPHRASE".to_string(),
            timestamp_header: "CB-ACCESS-TIMESTAMP".to_string(),
        }
    }

    // ========== AuthHeaders ==========

    #[test]
    fn test_auth_headers_new_is_empty() {
        let ah = AuthHeaders::new();
        assert!(ah.headers.is_empty());
        assert!(ah.query_params.is_empty());
        assert!(ah.body_params.is_empty());
    }

    #[test]
    fn test_auth_headers_default() {
        let ah = AuthHeaders::default();
        assert!(ah.headers.is_empty());
    }

    // ========== HmacSha256Auth ==========

    #[test]
    fn test_hmac_sha256_signature_deterministic() {
        let auth = HmacSha256Auth::new(
            "api_key".to_string(),
            "secret123".to_string(),
            &binance_auth_method(),
        ).unwrap();
        let sig1 = auth.compute_signature("test_message");
        let sig2 = auth.compute_signature("test_message");
        assert_eq!(sig1, sig2);
        assert!(!sig1.is_empty());
    }

    #[test]
    fn test_hmac_sha256_different_messages_different_sigs() {
        let auth = HmacSha256Auth::new(
            "key".to_string(),
            "secret".to_string(),
            &binance_auth_method(),
        ).unwrap();
        let sig1 = auth.compute_signature("message_a");
        let sig2 = auth.compute_signature("message_b");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_hmac_sha256_wrong_auth_method() {
        let result = HmacSha256Auth::new(
            "key".to_string(),
            "secret".to_string(),
            &kraken_auth_method(),
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_hmac_sha256_sign_includes_api_key() {
        let auth = HmacSha256Auth::new(
            "my_api_key".to_string(),
            "my_secret".to_string(),
            &binance_auth_method(),
        ).unwrap();
        let headers = auth.sign("GET", "/api/v3/order", "", 1700000000000).await.unwrap();
        assert_eq!(headers.headers.get("X-MBX-APIKEY").unwrap(), "my_api_key");
        assert!(headers.query_params.contains_key("timestamp"));
        assert!(headers.query_params.contains_key("signature"));
    }

    // ========== HmacSha512Auth (Kraken) ==========

    #[test]
    fn test_hmac_sha512_requires_base64_secret() {
        // Valid base64 secret
        let valid = HmacSha512Auth::new(
            "key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(b"test_secret"),
            &kraken_auth_method(),
        );
        assert!(valid.is_ok());
    }

    #[test]
    fn test_hmac_sha512_invalid_base64_secret() {
        let result = HmacSha512Auth::new(
            "key".to_string(),
            "not-valid-base64!!!".to_string(),
            &kraken_auth_method(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_hmac_sha512_wrong_auth_method() {
        let result = HmacSha512Auth::new(
            "key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(b"secret"),
            &binance_auth_method(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_kraken_signature_deterministic() {
        let auth = HmacSha512Auth::new(
            "key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(b"test_secret"),
            &kraken_auth_method(),
        ).unwrap();
        let sig1 = auth.compute_kraken_signature("/0/private/AddOrder", 12345, "nonce=12345&ordertype=limit");
        let sig2 = auth.compute_kraken_signature("/0/private/AddOrder", 12345, "nonce=12345&ordertype=limit");
        assert_eq!(sig1, sig2);
        // Result is base64-encoded
        assert!(base64::engine::general_purpose::STANDARD.decode(&sig1).is_ok());
    }

    #[tokio::test]
    async fn test_hmac_sha512_sign_includes_api_key() {
        let auth = HmacSha512Auth::new(
            "kraken_key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(b"kraken_secret"),
            &kraken_auth_method(),
        ).unwrap();
        let headers = auth.sign("POST", "/0/private/AddOrder", "nonce=12345", 12345).await.unwrap();
        assert_eq!(headers.headers.get("API-Key").unwrap(), "kraken_key");
        assert!(headers.headers.contains_key("API-Sign"));
    }

    // ========== HmacSha256PassphraseAuth (Coinbase) ==========

    #[test]
    fn test_passphrase_auth_requires_base64_secret() {
        let valid = HmacSha256PassphraseAuth::new(
            "key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(b"secret"),
            "my_passphrase".to_string(),
            &coinbase_auth_method(),
        );
        assert!(valid.is_ok());
    }

    #[test]
    fn test_passphrase_auth_invalid_base64() {
        let result = HmacSha256PassphraseAuth::new(
            "key".to_string(),
            "not-valid!!!".to_string(),
            "pass".to_string(),
            &coinbase_auth_method(),
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_passphrase_auth_sign_includes_all_headers() {
        let auth = HmacSha256PassphraseAuth::new(
            "cb_key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(b"cb_secret"),
            "cb_pass".to_string(),
            &coinbase_auth_method(),
        ).unwrap();
        let headers = auth.sign("POST", "/api/v3/orders", "{}", 1700000000000).await.unwrap();
        assert_eq!(headers.headers.get("CB-ACCESS-KEY").unwrap(), "cb_key");
        assert_eq!(headers.headers.get("CB-ACCESS-PASSPHRASE").unwrap(), "cb_pass");
        assert!(headers.headers.contains_key("CB-ACCESS-SIGN"));
        assert!(headers.headers.contains_key("CB-ACCESS-TIMESTAMP"));
    }

    // ========== create_auth_strategy factory ==========

    #[test]
    fn test_create_auth_strategy_sha256() {
        let result = create_auth_strategy(
            &binance_auth_method(),
            "key".to_string(),
            "secret".to_string(),
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_auth_strategy_sha512() {
        let result = create_auth_strategy(
            &kraken_auth_method(),
            "key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(b"secret"),
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_auth_strategy_passphrase_required() {
        let result = create_auth_strategy(
            &coinbase_auth_method(),
            "key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(b"secret"),
            None, // missing passphrase
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_create_auth_strategy_passphrase_provided() {
        let result = create_auth_strategy(
            &coinbase_auth_method(),
            "key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(b"secret"),
            Some("passphrase".to_string()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_auth_strategy_rsa_unimplemented() {
        let result = create_auth_strategy(
            &AuthMethod::Rsa {
                api_key_header: "X-Key".to_string(),
                signature_header: "X-Sig".to_string(),
            },
            "key".to_string(),
            "secret".to_string(),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_create_auth_strategy_ed25519_unimplemented() {
        let result = create_auth_strategy(
            &AuthMethod::Ed25519 {
                client_id_param: "client_id".to_string(),
                signature_param: "signature".to_string(),
            },
            "key".to_string(),
            "secret".to_string(),
            None,
        );
        assert!(result.is_err());
    }
}
