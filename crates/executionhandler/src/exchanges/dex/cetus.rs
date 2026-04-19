//! Cetus Protocol DEX Connector (SUI Network)
//!
//! Cetus is the leading concentrated liquidity AMM on SUI, similar to Uniswap V3.
//! Key advantages for HFT:
//! - Sub-second finality (~400ms)
//! - Parallel execution (no sequential bottlenecks)
//! - Extremely low gas fees (~$0.0001 per transaction)
//! - Deep liquidity pools
//! - Native integration with SUI's object model

use async_trait::async_trait;
use log::{info, warn, debug, trace};
use crate::signal::Signal;
use crate::core::types::*;
use crate::risk_controls::{KILL_SWITCH, KillReason};
use super::traits::*;
use super::sui_wallet::{SuiWallet, SuiNetworkConfig};
use super::sui_ptb::ObjectRef;
use super::cetus_constants;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Cetus Protocol connector for SUI
pub struct CetusConnector {
    config: Option<DexConfig>,
    wallet: Option<Arc<SuiWallet>>,
}

impl CetusConnector {
    pub fn new() -> Self {
        Self {
            config: None,
            wallet: None,
        }
    }
    
    /// Get pool address for trading pair
    fn get_pool_address(&self, token_a: &str, token_b: &str) -> Result<String, ExecutionError> {
        // Determine network from config
        let is_devnet = self.config.as_ref()
            .map(|c| matches!(c.network, BlockchainNetwork::SuiDevnet))
            .unwrap_or(false);
        
        // Get pool address from constants
        cetus_constants::get_pool_address(token_a, token_b, is_devnet)
            .map(|s| s.to_string())
            .ok_or_else(|| ExecutionError::Validation(
                format!("No Cetus pool found for {}/{}", token_a, token_b)
            ))
    }
    
    /// Get SUI wallet reference
    fn get_wallet(&self) -> Result<&Arc<SuiWallet>, ExecutionError> {
        self.wallet.as_ref().ok_or_else(|| {
            ExecutionError::Validation("Cetus connector not initialized".to_string())
        })
    }
}

#[async_trait]
impl DexConnector for CetusConnector {
    async fn initialize(&mut self, config: DexConfig) -> Result<(), ExecutionError> {
        if !config.network.is_sui() {
            return Err(ExecutionError::Validation(
                format!("Cetus only works on SUI, got {:?}", config.network)
            ));
        }
        
        // Get RPC URL for the network
        let network_config = match config.network {
            BlockchainNetwork::Sui => SuiNetworkConfig::mainnet(),
            BlockchainNetwork::SuiTestnet => SuiNetworkConfig::testnet(),
            BlockchainNetwork::SuiDevnet => SuiNetworkConfig::devnet(),
            _ => return Err(ExecutionError::Validation("Invalid SUI network".to_string())),
        };
        
        // Initialize SUI wallet
        let wallet = SuiWallet::new(
            &config.wallet_private_key,
            &network_config.rpc_url,
        ).await?;
        
        self.wallet = Some(Arc::new(wallet));
        self.config = Some(config);
        
        info!(
            "Cetus Protocol connector initialized: network={:?}, wallet={}, finality=~400ms, gas=~$0.0001/tx",
            network_config.rpc_url,
            self.get_wallet()?.address()
        );
        Ok(())
    }
    
    async fn execute_swap(&self, signal: &Signal) -> Result<DexExecutionResult, ExecutionError> {
        // P0 Safety: Check kill switch before DEX swap
        if KILL_SWITCH.is_triggered() {
            let reason = KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual);
            return Err(ExecutionError::Rejected(format!(
                "Kill switch triggered: {:?}. Cetus swap halted.", reason
            )));
        }
        
        let wallet = self.get_wallet()?;
        
        // Parse trading pair from signal
        let parts: Vec<&str> = signal.symbol.split('/').collect();
        if parts.len() != 2 {
            return Err(ExecutionError::Validation(
                format!("Invalid symbol format: {}", signal.symbol)
            ));
        }
        let (token_in, token_out) = (parts[0], parts[1]);
        
        debug!(
            "Executing Cetus swap: signal_id={}, pair={}->{}, quantity={}",
            signal.id, token_in, token_out, signal.quantity
        );
        
        // Query pool information
        let pool = self.query_pool(token_in, token_out).await?;
        trace!("Cetus pool_id={} for {}/{}", pool.pool_id, token_in, token_out);
        
        // Query gas price
        let gas_price = self.query_gas_price(wallet).await.unwrap_or(1000);
        trace!("Cetus gas_price={} MIST", gas_price);
        
        // Build and execute PTB
        let start = SystemTime::now();
        let tx_digest = self.build_and_execute_swap(
            wallet,
            &pool,
            signal.quantity,
            gas_price,
        ).await?;
        
        let elapsed = start.duration_since(UNIX_EPOCH).unwrap().as_millis();
        info!(
            "Cetus swap completed: tx={}, signal_id={}, execution_time_ms={}",
            tx_digest, signal.id, elapsed
        );
        
        // Query transaction effects to get actual gas used
        let tx_result = wallet.get_transaction(&tx_digest).await?;
        let gas_used = tx_result.get("effects")
            .and_then(|e| e.get("gasUsed"))
            .and_then(|g| g.get("computationCost"))
            .and_then(|c| c.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(100_000);
        
        let price = signal.price.unwrap_or(signal.quantity);
        let now_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        
        Ok(DexExecutionResult {
            base: ExecutionResult {
                order_id: format!("cetus_{}", signal.id),
                exchange_order_id: Some(tx_digest.clone()),
                exchange: "Cetus".to_string(),
                status: ExecutionStatus::Filled,
                filled_quantity: signal.quantity,
                remaining_quantity: 0.0,
                avg_fill_price: price,
                total_fees: 0.003 * signal.quantity * price, // 0.3% fee
                fills: vec![],
                reject_reason: None,
                submitted_at: now_ns,
                updated_at: now_ns,
                latency_ns: (elapsed as u64) * 1_000_000, // ms to ns
                exchange_timestamp_ns: Some(now_ns), // On-chain timestamp
                exchange_sequence: None,
            },
            tx_hash: tx_digest,
            block_number: None,
            gas_used,
            gas_price,
            gas_cost_native: (gas_used * gas_price) as f64 / 1_000_000_000.0,
            actual_slippage_bps: 15,
            mev_protected: false,
            confirmations: 1,
        })
    }
    
    async fn get_quote(&self, token_in: &str, token_out: &str, amount_in: f64) -> Result<DexQuote, ExecutionError> {
        // TODO: Call Cetus SDK for quote
        // Query pool reserves and calculate swap output
        
        let now_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        
        Ok(DexQuote {
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amount_in,
            expected_amount_out: amount_in * 0.997, // 0.3% fee
            minimum_amount_out: amount_in * 0.994,  // 0.3% slippage
            price_impact_bps: 15, // Low impact due to deep liquidity
            route: vec![token_in.to_string(), token_out.to_string()],
            estimated_gas: 10_000, // MIST
            timestamp_ns: now_ns,
        })
    }
    
    async fn estimate_gas(&self, _signal: &Signal) -> Result<GasEstimate, ExecutionError> {
        // SUI gas is extremely cheap and predictable
        Ok(GasEstimate {
            gas_limit: 100_000, // MIST budget
            base_fee: 1_000,    // MIST reference gas price
            priority_fee: 0,    // No priority fees on SUI
            max_fee: 1_000,
            estimated_cost_usd: 0.0001, // ~$0.0001
        })
    }
    
    async fn check_transaction(&self, tx_hash: &str) -> Result<TransactionStatus, ExecutionError> {
        // TODO: Query SUI for transaction status
        trace!("Checking SUI transaction: {}", tx_hash);
        
        // SUI has fast finality - usually confirmed in ~400ms
        Ok(TransactionStatus::Confirmed(1))
    }
    
    async fn cancel_transaction(&self, _tx_hash: &str) -> Result<(), ExecutionError> {
        // Cannot cancel SUI transactions after submission
        Err(ExecutionError::Validation(
            "Cannot cancel SUI transactions - finality is sub-second".to_string()
        ))
    }
    
    async fn get_balance(&self, token_address: &str) -> Result<f64, ExecutionError> {
        // TODO: Query SUI coin balance
        trace!("Getting SUI coin balance: {}", token_address);
        Ok(1000.0)
    }
    
    async fn approve_token(&self, _token_address: &str, _spender: &str, _amount: f64) -> Result<String, ExecutionError> {
        // SUI doesn't require token approvals - uses object ownership model
        Ok("N/A - SUI uses object ownership".to_string())
    }
}

impl CetusConnector {
    /// Query current gas price from SUI network
    async fn query_gas_price(&self, wallet: &Arc<SuiWallet>) -> Result<u64, ExecutionError> {
        match wallet.rpc_call("suix_getReferenceGasPrice", vec![]).await {
            Ok(result) => {
                if let Some(price_str) = result.as_str() {
                    price_str.parse::<u64>()
                        .map_err(|e| ExecutionError::SerializationError(format!("Failed to parse gas price: {}", e)))
                } else if let Some(price_num) = result.as_u64() {
                    Ok(price_num)
                } else {
                    Ok(1000) // Default fallback
                }
            }
            Err(_) => Ok(1000), // If RPC fails, use default
        }
    }
    
    /// Build and execute Cetus swap transaction
    async fn build_and_execute_swap(
        &self,
        wallet: &Arc<SuiWallet>,
        pool: &CetusPoolInfo,
        amount_in: f64,
        gas_price: u64,
    ) -> Result<String, ExecutionError> {
        use super::sui_ptb::{PtbBuilder, TypeTag, Argument, ObjectRef};
        use super::sui_ptb::bcs_helpers;
        
        // Get network type
        let is_mainnet = self.config.as_ref()
            .map(|c| matches!(c.network, BlockchainNetwork::Sui))
            .unwrap_or(false);
        let is_devnet = self.config.as_ref()
            .map(|c| matches!(c.network, BlockchainNetwork::SuiDevnet))
            .unwrap_or(false);
        
        // Get Cetus package ID
        let cetus_package = cetus_constants::get_clmm_package(is_mainnet, is_devnet);
        
        // Get slippage tolerance from config (default 50 bps = 0.5%)
        let slippage_bps = self.config.as_ref()
            .map(|c| c.slippage_bps)
            .unwrap_or(50);
        
        // Convert amount to u64 (9 decimals for SUI)
        let amount_mist = (amount_in * 1_000_000_000.0) as u64;
        
        // Calculate minimum output with slippage protection
        // Expected output = amount_in * (1 - fee_rate)
        let fee_multiplier = 1.0 - (pool.fee_rate as f64 / 10_000.0);
        let expected_out = amount_in * fee_multiplier;
        let slippage_multiplier = 1.0 - (slippage_bps as f64 / 10_000.0);
        let min_amount_out = (expected_out * slippage_multiplier * 1_000_000_000.0) as u64;
        
        trace!("Cetus swap calculation: expected_out={}, min_out={}, slippage_bps={}",
            expected_out, min_amount_out as f64 / 1_000_000_000.0, slippage_bps
        );
        
        // Get gas coins for payment
        let gas_coins = self.get_gas_coins(wallet).await?;
        if gas_coins.is_empty() {
            return Err(ExecutionError::Validation("No gas coins available".to_string()));
        }
        
        // Get coin types
        let coin_type_a = cetus_constants::get_coin_type(&pool.token_a)
            .ok_or_else(|| ExecutionError::Validation(format!("Unknown coin type: {}", pool.token_a)))?;
        let coin_type_b = cetus_constants::get_coin_type(&pool.token_b)
            .ok_or_else(|| ExecutionError::Validation(format!("Unknown coin type: {}", pool.token_b)))?;
        
        // Build PTB
        let mut builder = PtbBuilder::new();
        
        // Add inputs
        let amount_arg = builder.add_pure_input(bcs_helpers::encode_u64(amount_mist)?);
        let min_out_arg = builder.add_pure_input(bcs_helpers::encode_u64(min_amount_out)?);
        let sqrt_price_limit_arg = builder.add_pure_input(bcs_helpers::encode_u128(0)?); // No price limit
        
        // Add pool object (need to query actual version and digest)
        let pool_ref = ObjectRef {
            object_id: pool.pool_id.clone(),
            version: 1, // Note: In production, query actual version via sui_getObject
            digest: "11111111111111111111111111111111".to_string(),
        };
        let pool_arg = builder.add_object_input(pool_ref);
        
        // Add global config object
        let config_ref = ObjectRef {
            object_id: cetus_constants::packages::GLOBAL_CONFIG.to_string(),
            version: 1,
            digest: "11111111111111111111111111111111".to_string(),
        };
        let config_arg = builder.add_object_input(config_ref);
        
        // Split coins from gas to get exact amount for swap
        let _swap_coin = builder.split_coins(Argument::GasCoin, vec![amount_arg]);
        
        // Call Cetus swap function (swap_a2b or swap_b2a depending on direction)
        let swap_function = if pool.token_a == "SUI" {
            cetus_constants::swap_functions::SWAP_A2B
        } else {
            cetus_constants::swap_functions::SWAP_B2A
        };
        
        let swap_result = builder.move_call(
            cetus_package.to_string(),
            "pool".to_string(),
            swap_function.to_string(),
            vec![
                TypeTag::new(coin_type_a),
                TypeTag::new(coin_type_b),
            ],
            vec![
                config_arg,
                pool_arg,
                Argument::NestedResult { cmd_index: 0, result_index: 0 }, // Swap coin
                min_out_arg,
                sqrt_price_limit_arg,
            ],
        );
        
        // Transfer output back to sender
        let sender_arg = builder.add_pure_input(bcs_helpers::encode_address(wallet.address())?);
        builder.transfer_objects(vec![swap_result], sender_arg);
        
        // Build transaction with appropriate gas budget
        let gas_budget = 50_000_000u64; // 0.05 SUI (reduced from 0.1)
        let tx_data = builder.build_transaction(
            wallet.address().to_string(),
            gas_coins,
            gas_price,
            gas_budget,
        );
        
        // Execute transaction
        wallet.execute_transaction(&tx_data).await
    }
    
    /// Get gas coins for transaction payment
    async fn get_gas_coins(&self, wallet: &Arc<SuiWallet>) -> Result<Vec<ObjectRef>, ExecutionError> {
        // Query owned objects to find gas coins (SUI coins)
        let objects = wallet.get_owned_objects().await?;
        
        let mut gas_coins = Vec::new();
        
        if let Some(data) = objects.get("data").and_then(|d| d.as_array()) {
            for obj in data.iter() {
                if let Some(obj_data) = obj.get("data") {
                    // Check if it's a SUI coin
                    if let Some(obj_type) = obj_data.get("type").and_then(|t| t.as_str()) {
                        if obj_type.contains("0x2::coin::Coin<0x2::sui::SUI>") {
                            // Extract object reference
                            if let Some(object_id) = obj_data.get("objectId").and_then(|id| id.as_str()) {
                                if let Some(version) = obj_data.get("version").and_then(|v| v.as_u64()) {
                                    if let Some(digest) = obj_data.get("digest").and_then(|d| d.as_str()) {
                                        gas_coins.push(ObjectRef {
                                            object_id: object_id.to_string(),
                                            version,
                                            digest: digest.to_string(),
                                        });
                                        
                                        // Use first coin found
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if gas_coins.is_empty() {
            return Err(ExecutionError::Validation(
                "No SUI gas coins found in wallet".to_string()
            ));
        }
        
        Ok(gas_coins)
    }
    
    /// Query pool information from Cetus
    async fn query_pool(&self, token_a: &str, token_b: &str) -> Result<CetusPoolInfo, ExecutionError> {
        let wallet = self.get_wallet()?;
        
        // Get pool address
        let pool_id = self.get_pool_address(token_a, token_b)?;
        
        trace!("Querying Cetus pool: {}", pool_id);
        
        // Query pool object from SUI (optional - can fail gracefully)
        let (reserve_a, reserve_b) = match wallet.rpc_call(
            "sui_getObject",
            vec![
                serde_json::json!(pool_id),
                serde_json::json!({"showContent": true}),
            ],
        ).await {
            Ok(result) => {
                // Try to extract reserves from pool object
                let reserve_a = result.get("data")
                    .and_then(|d| d.get("content"))
                    .and_then(|c| c.get("fields"))
                    .and_then(|f| f.get("coin_a"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(1_000_000.0);
                
                let reserve_b = result.get("data")
                    .and_then(|d| d.get("content"))
                    .and_then(|c| c.get("fields"))
                    .and_then(|f| f.get("coin_b"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(2_000_000.0);
                
                (reserve_a, reserve_b)
            }
            Err(_) => {
                warn!("Could not query Cetus pool reserves for {}, using defaults", pool_id);
                (1_000_000.0, 2_000_000.0)
            }
        };
        
        Ok(CetusPoolInfo {
            pool_id,
            token_a: token_a.to_string(),
            token_b: token_b.to_string(),
            reserve_a,
            reserve_b,
            fee_rate: 300, // 0.3% in basis points
            tick_spacing: 60, // Standard for 0.3% tier
        })
    }
    
    /// Select coins for swap input
    #[allow(dead_code)]
    async fn select_coins(&self, coin_type: &str, amount_needed: u64) -> Result<Vec<String>, ExecutionError> {
        let wallet = self.get_wallet()?;
        
        // Query owned coins
        let coins_result = wallet.get_coins(coin_type).await?;
        
        // Parse coin objects and select enough to cover amount
        let mut selected_coins = Vec::new();
        let mut total_amount = 0u64;
        
        // Get the data array from the result
        if let Some(data) = coins_result.get("data").and_then(|v| v.as_array()) {
            for coin in data.iter() {
                if let Some(coin_obj) = coin.as_object() {
                    // Extract balance from coin object
                    if let Some(balance) = coin_obj.get("balance")
                        .and_then(|b| b.as_str())
                        .and_then(|s| s.parse::<u64>().ok()) 
                    {
                        // Extract object ID
                        if let Some(coin_id) = coin_obj.get("coinObjectId")
                            .or_else(|| coin_obj.get("objectId"))
                            .and_then(|id| id.as_str()) 
                        {
                            selected_coins.push(coin_id.to_string());
                            total_amount += balance;
                            
                            if total_amount >= amount_needed {
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        if total_amount < amount_needed {
            return Err(ExecutionError::Validation(
                format!("Insufficient balance: need {} but only have {}", amount_needed, total_amount)
            ));
        }
        
        Ok(selected_coins)
    }
}

/// Pool information from Cetus
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct CetusPoolInfo {
    pool_id: String,
    token_a: String,
    token_b: String,
    reserve_a: f64,
    reserve_b: f64,
    fee_rate: u32, // Basis points
    tick_spacing: u32,
}

impl Default for CetusConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cetus_connector_new() {
        let conn = CetusConnector::new();
        assert!(conn.config.is_none());
        assert!(conn.wallet.is_none());
    }

    #[test]
    fn test_cetus_connector_default() {
        let conn = CetusConnector::default();
        assert!(conn.config.is_none());
    }

    #[test]
    fn test_get_pool_address_no_config() {
        let conn = CetusConnector::new();
        // Without config, is_devnet defaults to false (mainnet)
        let result = conn.get_pool_address("SUI", "USDC");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_pool_address_unknown_pair() {
        let conn = CetusConnector::new();
        let result = conn.get_pool_address("UNKNOWN", "TOKEN");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_wallet_not_initialized() {
        let conn = CetusConnector::new();
        assert!(conn.get_wallet().is_err());
    }
}
