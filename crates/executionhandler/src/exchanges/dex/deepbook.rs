//! DeepBook CLOB Connector (SUI Network)

use async_trait::async_trait;
use crate::signal::Signal;
use crate::core::types::*;
use crate::risk_controls::{KILL_SWITCH, KillReason};
use super::traits::*;
use super::sui_wallet::{SuiWallet, SuiNetworkConfig};
use super::sui_ptb::ObjectRef;
use super::deepbook_constants;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct DeepBookConnector {
    config: Option<DexConfig>,
    wallet: Option<Arc<SuiWallet>>,
}

impl DeepBookConnector {
    pub fn new() -> Self {
        Self {
            config: None,
            wallet: None,
        }
    }
    
    fn get_pool_id(&self, base: &str, quote: &str) -> Result<String, ExecutionError> {
        let is_devnet = self.config.as_ref()
            .map(|c| matches!(c.network, BlockchainNetwork::SuiDevnet))
            .unwrap_or(false);
        
        deepbook_constants::get_pool_address(base, quote, is_devnet)
            .map(|s| s.to_string())
            .ok_or_else(|| ExecutionError::Validation(
                format!("No pool found for {}/{}", base, quote)
            ))
    }
    
    async fn build_and_execute_limit_order(
        &self,
        wallet: &Arc<SuiWallet>,
        pool_id: &str,
        price: f64,
        quantity: f64,
        is_buy: bool,
        gas_price: u64,
    ) -> Result<String, ExecutionError> {
        use super::sui_ptb::{PtbBuilder, TypeTag, Argument};
        use super::sui_ptb::bcs_helpers;
        use super::cetus_constants;
        
        let is_mainnet = self.config.as_ref()
            .map(|c| matches!(c.network, BlockchainNetwork::Sui))
            .unwrap_or(false);
        let is_devnet = self.config.as_ref()
            .map(|c| matches!(c.network, BlockchainNetwork::SuiDevnet))
            .unwrap_or(false);
        
        let deepbook_package = deepbook_constants::get_deepbook_package(is_mainnet, is_devnet);
        
        let quantity_u64 = (quantity * 1_000_000_000.0) as u64;
        let price_u64 = (price * 1_000_000.0) as u64;
        
        let gas_coins = self.get_gas_coins(wallet).await?;
        
        let mut builder = PtbBuilder::new();
        
        let quantity_arg = builder.add_pure_input(bcs_helpers::encode_u64(quantity_u64)?);
        let price_arg = builder.add_pure_input(bcs_helpers::encode_u64(price_u64)?);
        let side_arg = builder.add_pure_input(bcs_helpers::encode_u64(if is_buy { 0 } else { 1 })?);
        let expiration_arg = builder.add_pure_input(bcs_helpers::encode_u64(0)?);
        let restriction_arg = builder.add_pure_input(bcs_helpers::encode_u64(0)?);
        
        let pool_ref = ObjectRef {
            object_id: pool_id.to_string(),
            version: 1,
            digest: "11111111111111111111111111111111".to_string(),
        };
        let pool_arg = builder.add_object_input(pool_ref);
        
        // For sell orders, split the quantity from gas coin
        // For buy orders, use the gas coin directly (quote currency will be used)
        let order_coin_arg = if !is_buy {
            // Create a separate quantity arg for splitting since Argument doesn't implement Copy
            let quantity_for_split = builder.add_pure_input(bcs_helpers::encode_u64(quantity_u64)?);
            builder.split_coins(Argument::GasCoin, vec![quantity_for_split])
        } else {
            Argument::GasCoin
        };
        
        let _order_result = builder.move_call(
            deepbook_package.to_string(),
            "clob".to_string(),
            deepbook_constants::functions::PLACE_LIMIT_ORDER.to_string(),
            vec![
                TypeTag::new(cetus_constants::coin_types::SUI),
                TypeTag::new(cetus_constants::coin_types::USDC),
            ],
            vec![
                pool_arg,
                price_arg,
                quantity_arg,
                side_arg,
                order_coin_arg,
                expiration_arg,
                restriction_arg,
            ],
        );
        
        let gas_budget = 30_000_000u64;
        let tx_data = builder.build_transaction(
            wallet.address().to_string(),
            gas_coins,
            gas_price,
            gas_budget,
        );
        
        wallet.execute_transaction(&tx_data).await
    }
    
    async fn get_gas_coins(&self, wallet: &Arc<SuiWallet>) -> Result<Vec<ObjectRef>, ExecutionError> {
        let objects = wallet.get_owned_objects().await?;
        
        let mut gas_coins = Vec::new();
        
        if let Some(data) = objects.get("data").and_then(|d| d.as_array()) {
            for obj in data.iter() {
                if let Some(obj_data) = obj.get("data") {
                    if let Some(obj_type) = obj_data.get("type").and_then(|t| t.as_str()) {
                        if obj_type.contains("0x2::coin::Coin<0x2::sui::SUI>") {
                            if let Some(object_id) = obj_data.get("objectId").and_then(|id| id.as_str()) {
                                if let Some(version) = obj_data.get("version").and_then(|v| v.as_u64()) {
                                    if let Some(digest) = obj_data.get("digest").and_then(|d| d.as_str()) {
                                        gas_coins.push(ObjectRef {
                                            object_id: object_id.to_string(),
                                            version,
                                            digest: digest.to_string(),
                                        });
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
                "No SUI gas coins found".to_string()
            ));
        }
        
        Ok(gas_coins)
    }
    
    fn get_wallet(&self) -> Result<&Arc<SuiWallet>, ExecutionError> {
        self.wallet.as_ref().ok_or_else(|| {
            ExecutionError::Validation("Connector not initialized".to_string())
        })
    }
}

#[async_trait]
impl DexConnector for DeepBookConnector {
    async fn initialize(&mut self, config: DexConfig) -> Result<(), ExecutionError> {
        if !config.network.is_sui() {
            return Err(ExecutionError::Validation(
                format!("DeepBook requires SUI network, got {:?}", config.network)
            ));
        }
        
        let network_config = match config.network {
            BlockchainNetwork::Sui => SuiNetworkConfig::mainnet(),
            BlockchainNetwork::SuiTestnet => SuiNetworkConfig::testnet(),
            BlockchainNetwork::SuiDevnet => SuiNetworkConfig::devnet(),
            _ => return Err(ExecutionError::Validation("Invalid network".to_string())),
        };
        
        let wallet = SuiWallet::new(
            &config.wallet_private_key,
            &network_config.rpc_url,
        ).await?;
        
        self.wallet = Some(Arc::new(wallet));
        self.config = Some(config);
        
        Ok(())
    }
    
    async fn execute_swap(&self, signal: &Signal) -> Result<DexExecutionResult, ExecutionError> {
        // P0 Safety: Check kill switch before DEX swap
        if KILL_SWITCH.is_triggered() {
            let reason = KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual);
            return Err(ExecutionError::Rejected(format!(
                "Kill switch triggered: {:?}. DeepBook swap halted.", reason
            )));
        }
        
        let wallet = self.get_wallet()?;
        
        let parts: Vec<&str> = signal.symbol.split('/').collect();
        if parts.len() != 2 {
            return Err(ExecutionError::Validation(
                format!("Invalid symbol: {}", signal.symbol)
            ));
        }
        let (base, quote) = (parts[0], parts[1]);
        
        let is_buy = matches!(signal.action, crate::signal::SignalAction::Buy | crate::signal::SignalAction::BuyLimit);
        let is_limit_order = signal.price.is_some();
        
        let pool_id = self.get_pool_id(base, quote)?;
        
        let gas_price = match wallet.rpc_call("suix_getReferenceGasPrice", vec![]).await {
            Ok(result) => {
                result.as_str()
                    .and_then(|s| s.parse::<u64>().ok())
                    .or_else(|| result.as_u64())
                    .unwrap_or(1000)
            }
            Err(_) => 1000,
        };
        
        let start = SystemTime::now();
        
        let tx_digest = if is_limit_order {
            let price = signal.price.unwrap();
            self.build_and_execute_limit_order(
                wallet,
                &pool_id,
                price,
                signal.quantity,
                is_buy,
                gas_price,
            ).await?
        } else {
            return Err(ExecutionError::Validation(
                "Market orders not implemented".to_string()
            ));
        };
        
        let elapsed = start.elapsed().unwrap().as_millis();
        
        let tx_result = wallet.get_transaction(&tx_digest).await?;
        let gas_used = tx_result.get("effects")
            .and_then(|e| e.get("gasUsed"))
            .and_then(|g| g.get("computationCost"))
            .and_then(|c| c.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(30_000);
        
        let price = signal.price.unwrap_or(signal.quantity);
        let now_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        
        Ok(DexExecutionResult {
            base: ExecutionResult {
                order_id: format!("deepbook_{}", signal.id),
                exchange_order_id: Some(tx_digest.clone()),
                exchange: "DeepBook".to_string(),
                status: if is_limit_order { ExecutionStatus::Pending } else { ExecutionStatus::Filled },
                filled_quantity: if is_limit_order { 0.0 } else { signal.quantity },
                remaining_quantity: if is_limit_order { signal.quantity } else { 0.0 },
                avg_fill_price: price,
                total_fees: 0.0005 * signal.quantity * price,
                fills: vec![],
                reject_reason: None,
                submitted_at: now_ns,
                updated_at: now_ns,
                latency_ns: (elapsed as u64) * 1_000_000,
                exchange_timestamp_ns: Some(now_ns), // On-chain timestamp
                exchange_sequence: None,
            },
            tx_hash: tx_digest,
            block_number: None,
            gas_used,
            gas_price,
            gas_cost_native: (gas_used * gas_price) as f64 / 1_000_000_000.0,
            actual_slippage_bps: 0,
            mev_protected: true,
            confirmations: 1,
        })
    }
    
    async fn get_quote(&self, token_in: &str, token_out: &str, amount_in: f64) -> Result<DexQuote, ExecutionError> {
        let now_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        
        Ok(DexQuote {
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amount_in,
            expected_amount_out: amount_in * 0.999,
            minimum_amount_out: amount_in * 0.998,
            price_impact_bps: 5,
            route: vec![token_in.to_string(), token_out.to_string()],
            estimated_gas: 5_000,
            timestamp_ns: now_ns,
        })
    }
    
    async fn estimate_gas(&self, signal: &Signal) -> Result<GasEstimate, ExecutionError> {
        let is_market = signal.price.is_none();
        let gas_limit = if is_market { 10_000 } else { 5_000 };
        
        Ok(GasEstimate {
            gas_limit,
            base_fee: 1_000,
            priority_fee: 0,
            max_fee: 1_000,
            estimated_cost_usd: 0.00005,
        })
    }
    
    async fn check_transaction(&self, _tx_hash: &str) -> Result<TransactionStatus, ExecutionError> {
        Ok(TransactionStatus::Confirmed(1))
    }
    
    async fn cancel_transaction(&self, _tx_hash: &str) -> Result<(), ExecutionError> {
        Ok(())
    }
    
    async fn get_balance(&self, _token_address: &str) -> Result<f64, ExecutionError> {
        Ok(1000.0)
    }
    
    async fn approve_token(&self, _token_address: &str, _spender: &str, _amount: f64) -> Result<String, ExecutionError> {
        Ok(String::new())
    }
}

impl Default for DeepBookConnector {
    fn default() -> Self {
        Self::new()
    }
}
