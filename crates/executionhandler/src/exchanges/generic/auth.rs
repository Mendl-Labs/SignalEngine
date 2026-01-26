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
        AuthMethod::Rsa { .. } | AuthMethod::Ed25519 { .. } => {
            Err(ExecutionError::Authentication(
                "RSA and Ed25519 auth not yet implemented".to_string(),
            ))
        }
    }
}
