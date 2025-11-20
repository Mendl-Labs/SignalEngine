//! SUI Wallet Manager
//!
//! Handles SUI keypair management, transaction signing, and RPC interactions via HTTP.
//! Provides a secure interface for all SUI DEX connectors (Cetus, DeepBook, etc.)
//!
//! Note: Using HTTP JSON-RPC instead of official sui-sdk (not on crates.io)

use std::sync::Arc;
use crate::core::types::ExecutionError;
use reqwest::Client;
use serde_json::{json, Value};
use ed25519_dalek::{SigningKey, VerifyingKey, Signer, Signature as Ed25519Signature};
use blake2::{Blake2b512, Digest};

/// SUI address (32 bytes, hex encoded with 0x prefix)
pub type SuiAddress = String;

/// SUI wallet with ed25519 keypair and HTTP RPC client
pub struct SuiWallet {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    address: SuiAddress,
    rpc_url: String,
    client: Arc<Client>,
}

impl SuiWallet {
    /// Create new SUI wallet from private key hex string
    pub async fn new(private_key_hex: &str, rpc_url: &str) -> Result<Self, ExecutionError> {
        // Parse ed25519 signing key from hex
        let signing_key = Self::parse_private_key(private_key_hex)?;
        
        // Derive verifying (public) key
        let verifying_key = signing_key.verifying_key();
        
        // Derive SUI address from public key
        let address = Self::derive_address(&verifying_key)?;
        
        // Create HTTP client for RPC calls
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| ExecutionError::Connection(format!("Failed to create HTTP client: {}", e)))?;
        
        Ok(Self {
            signing_key,
            verifying_key,
            address,
            rpc_url: rpc_url.to_string(),
            client: Arc::new(client),
        })
    }
    
    /// Parse ed25519 private key from hex string
    fn parse_private_key(private_key_hex: &str) -> Result<SigningKey, ExecutionError> {
        // Remove 0x prefix if present
        let hex_str = private_key_hex.strip_prefix("0x").unwrap_or(private_key_hex);
        
        // Decode hex to bytes
        let key_bytes = hex::decode(hex_str)
            .map_err(|e| ExecutionError::Validation(format!("Invalid hex private key: {}", e)))?;
        
        // Ed25519 private keys are 32 bytes
        if key_bytes.len() != 32 {
            return Err(ExecutionError::Validation(
                format!("Private key must be 32 bytes, got {}", key_bytes.len())
            ));
        }
        
        // Create signing key
        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(&key_bytes);
        
        Ok(SigningKey::from_bytes(&key_array))
    }
    
    /// Derive SUI address from public key using Blake2b
    fn derive_address(verifying_key: &VerifyingKey) -> Result<SuiAddress, ExecutionError> {
        // SUI address derivation:
        // 1. Take public key bytes (32 bytes)
        // 2. Prepend signature scheme flag (0x00 for ed25519)
        // 3. Hash with Blake2b-256
        // 4. Take first 32 bytes as address
        
        let mut hasher = Blake2b512::new();
        
        // Add scheme flag (0x00 for ed25519)
        hasher.update(&[0x00]);
        
        // Add public key bytes
        hasher.update(verifying_key.as_bytes());
        
        // Get hash result
        let hash = hasher.finalize();
        
        // Take first 32 bytes and convert to hex with 0x prefix
        let address_bytes = &hash[..32];
        let address = format!("0x{}", hex::encode(address_bytes));
        
        Ok(address)
    }
    
    /// Get wallet address
    pub fn address(&self) -> &SuiAddress {
        &self.address
    }
    
    /// Get public key as hex
    pub fn public_key_hex(&self) -> String {
        hex::encode(self.verifying_key.as_bytes())
    }
    
    /// Get RPC URL
    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }
    
    /// Make JSON-RPC call to SUI node
    pub async fn rpc_call(&self, method: &str, params: Vec<Value>) -> Result<Value, ExecutionError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        
        let response = self.client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| ExecutionError::NetworkError(format!("RPC request failed: {}", e)))?;
        
        let json: Value = response
            .json()
            .await
            .map_err(|e| ExecutionError::SerializationError(format!("Failed to parse response: {}", e)))?;
        
        if let Some(error) = json.get("error") {
            return Err(ExecutionError::Exchange(format!("RPC error: {}", error)));
        }
        
        json.get("result")
            .cloned()
            .ok_or_else(|| ExecutionError::Exchange("No result in RPC response".to_string()))
    }
    
    /// Get coin balance for a specific coin type
    pub async fn get_coin_balance(&self, coin_type: &str) -> Result<u64, ExecutionError> {
        let result = self.rpc_call(
            "suix_getBalance",
            vec![
                json!(self.address),
                json!(coin_type),
            ],
        ).await?;
        
        // Parse balance from response
        if let Some(total_balance) = result.get("totalBalance").and_then(|v| v.as_str()) {
            total_balance.parse::<u64>()
                .map_err(|e| ExecutionError::SerializationError(format!("Failed to parse balance: {}", e)))
        } else {
            Ok(0)
        }
    }
    
    /// Get all owned coins of a type
    pub async fn get_coins(&self, coin_type: &str) -> Result<Value, ExecutionError> {
        let result = self.rpc_call(
            "suix_getCoins",
            vec![
                json!(self.address),
                json!(coin_type),
                json!(null), // cursor
                json!(null), // limit
            ],
        ).await?;
        
        Ok(result)
    }
    
    /// Sign transaction bytes with ed25519
    pub fn sign_transaction(&self, tx_bytes: &[u8]) -> Result<Vec<u8>, ExecutionError> {
        // Create intent message for SUI transaction
        // Intent: [intent_scope (3 bytes) || intent_version (1 byte) || intent_app_id (1 byte)]
        // For transactions: [0, 0, 0, 0, 0] (TransactionData)
        let mut intent_message = vec![0u8, 0u8, 0u8, 0u8, 0u8];
        intent_message.extend_from_slice(tx_bytes);
        
        // Sign the intent message
        let signature: Ed25519Signature = self.signing_key.sign(&intent_message);
        
        // Return signature bytes
        Ok(signature.to_bytes().to_vec())
    }
    
    /// Sign raw bytes (for testing)
    pub fn sign_bytes(&self, data: &[u8]) -> Vec<u8> {
        let signature: Ed25519Signature = self.signing_key.sign(data);
        signature.to_bytes().to_vec()
    }
    
    /// Build transaction signature with scheme flag for SUI
    pub fn build_sui_signature(&self, tx_bytes: &[u8]) -> Result<String, ExecutionError> {
        // Sign transaction
        let signature_bytes = self.sign_transaction(tx_bytes)?;
        
        // SUI signature format: [flag (1 byte) || signature (64 bytes) || pubkey (32 bytes)]
        // Flag: 0x00 for ed25519
        let mut sui_signature = vec![0x00];
        sui_signature.extend_from_slice(&signature_bytes);
        sui_signature.extend_from_slice(self.verifying_key.as_bytes());
        
        // Encode as base64 for RPC submission using new base64 API
        use base64::Engine;
        Ok(base64::engine::general_purpose::STANDARD.encode(&sui_signature))
    }
    
    /// Execute a transaction on SUI network
    pub async fn execute_transaction(&self, tx_data: &super::sui_ptb::TransactionData) -> Result<String, ExecutionError> {
        // Serialize transaction data to BCS bytes
        let tx_bytes = bcs::to_bytes(tx_data)
            .map_err(|e| ExecutionError::SerializationError(format!("Failed to serialize transaction: {}", e)))?;
        
        // Sign the transaction
        let signature = self.build_sui_signature(&tx_bytes)?;
        
        // Encode tx_bytes as base64
        use base64::Engine;
        let tx_base64 = base64::engine::general_purpose::STANDARD.encode(&tx_bytes);
        
        // Submit transaction via RPC
        let result = self.rpc_call(
            "sui_executeTransactionBlock",
            vec![
                json!(tx_base64),
                json!([signature]),
                json!({
                    "showInput": false,
                    "showRawInput": false,
                    "showEffects": true,
                    "showEvents": false,
                    "showObjectChanges": false,
                    "showBalanceChanges": false,
                }),
                json!("WaitForLocalExecution"), // Wait for finality
            ],
        ).await?;
        
        // Extract transaction digest
        result.get("digest")
            .and_then(|d| d.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| ExecutionError::Exchange("No digest in transaction result".to_string()))
    }
    
    /// Query transaction status and effects
    pub async fn get_transaction(&self, digest: &str) -> Result<Value, ExecutionError> {
        self.rpc_call(
            "sui_getTransactionBlock",
            vec![
                json!(digest),
                json!({
                    "showInput": true,
                    "showRawInput": false,
                    "showEffects": true,
                    "showEvents": true,
                    "showObjectChanges": true,
                    "showBalanceChanges": true,
                }),
            ],
        ).await
    }
    
    /// Get owned objects (for finding gas coins)
    pub async fn get_owned_objects(&self) -> Result<Value, ExecutionError> {
        self.rpc_call(
            "suix_getOwnedObjects",
            vec![
                json!(self.address),
                json!({
                    "filter": null,
                    "options": {
                        "showType": true,
                        "showOwner": true,
                        "showPreviousTransaction": false,
                        "showDisplay": false,
                        "showContent": true,
                        "showBcs": false,
                        "showStorageRebate": false,
                    }
                }),
            ],
        ).await
    }
}

/// SUI network configuration
#[derive(Debug, Clone)]
pub struct SuiNetworkConfig {
    pub rpc_url: String,
    pub ws_url: Option<String>,
    pub is_mainnet: bool,
}

impl SuiNetworkConfig {
    /// Mainnet configuration
    pub fn mainnet() -> Self {
        Self {
            rpc_url: "https://fullnode.mainnet.sui.io:443".to_string(),
            ws_url: None,
            is_mainnet: true,
        }
    }
    
    /// Testnet configuration
    pub fn testnet() -> Self {
        Self {
            rpc_url: "https://fullnode.testnet.sui.io:443".to_string(),
            ws_url: None,
            is_mainnet: false,
        }
    }
    
    /// Devnet configuration (for testing)
    pub fn devnet() -> Self {
        Self {
            rpc_url: "https://fullnode.devnet.sui.io:443".to_string(),
            ws_url: None,
            is_mainnet: false,
        }
    }
}
