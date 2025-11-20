//! Common traits and types for DEX integration

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::types::*;
use crate::signal::Signal;

/// Blockchain network identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockchainNetwork {
    // SUI Network (PRIORITY - Built for HFT/DeFi)
    Sui,
    SuiTestnet,
    SuiDevnet,
    
    // EVM Networks
    Ethereum,
    EthereumGoerli,      // Testnet
    Arbitrum,
    Optimism,
    Polygon,
    BSC,                 // Binance Smart Chain
    Avalanche,
    Base,
    
    // Other Non-EVM
    Solana,
    SolanaDevnet,        // Testnet
}

impl BlockchainNetwork {
    pub fn chain_id(&self) -> Option<u64> {
        match self {
            Self::Ethereum => Some(1),
            Self::EthereumGoerli => Some(5),
            Self::Arbitrum => Some(42161),
            Self::Optimism => Some(10),
            Self::Polygon => Some(137),
            Self::BSC => Some(56),
            Self::Avalanche => Some(43114),
            Self::Base => Some(8453),
            // Non-EVM chains don't have EVM chain IDs
            Self::Sui | Self::SuiTestnet | Self::SuiDevnet => None,
            Self::Solana | Self::SolanaDevnet => None,
        }
    }
    
    pub fn is_evm(&self) -> bool {
        matches!(
            self,
            Self::Ethereum | 
            Self::EthereumGoerli | 
            Self::Arbitrum | 
            Self::Optimism | 
            Self::Polygon | 
            Self::BSC | 
            Self::Avalanche | 
            Self::Base
        )
    }
    
    pub fn is_sui(&self) -> bool {
        matches!(self, Self::Sui | Self::SuiTestnet | Self::SuiDevnet)
    }
    
    pub fn rpc_url(&self) -> &'static str {
        match self {
            // SUI Network
            Self::Sui => "https://fullnode.mainnet.sui.io:443",
            Self::SuiTestnet => "https://fullnode.testnet.sui.io:443",
            Self::SuiDevnet => "https://fullnode.devnet.sui.io:443",
            
            // Ethereum Networks
            Self::Ethereum => "https://eth-mainnet.g.alchemy.com/v2/",
            Self::EthereumGoerli => "https://eth-goerli.g.alchemy.com/v2/",
            Self::Arbitrum => "https://arb-mainnet.g.alchemy.com/v2/",
            Self::Optimism => "https://opt-mainnet.g.alchemy.com/v2/",
            Self::Polygon => "https://polygon-mainnet.g.alchemy.com/v2/",
            Self::BSC => "https://bsc-dataseed.binance.org/",
            Self::Avalanche => "https://api.avax.network/ext/bc/C/rpc",
            Self::Base => "https://mainnet.base.org",
            
            // Solana
            Self::Solana => "https://api.mainnet-beta.solana.com",
            Self::SolanaDevnet => "https://api.devnet.solana.com",
        }
    }
    
    /// Average block/transaction finality time in milliseconds
    pub fn finality_time_ms(&self) -> u64 {
        match self {
            Self::Sui | Self::SuiTestnet | Self::SuiDevnet => 400,  // ~400ms (sub-second!)
            Self::Solana | Self::SolanaDevnet => 400,                 // ~400ms
            Self::Polygon => 2_000,                                   // ~2s
            Self::BSC => 3_000,                                       // ~3s
            Self::Arbitrum | Self::Optimism | Self::Base => 2_000,   // ~2s (L2s)
            Self::Avalanche => 2_000,                                 // ~2s
            Self::Ethereum | Self::EthereumGoerli => 12_000,         // ~12s
        }
    }
}

/// Configuration for DEX connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DexConfig {
    /// Exchange name (e.g., "UniswapV3", "Jupiter")
    pub exchange_name: String,
    
    /// Blockchain network
    pub network: BlockchainNetwork,
    
    /// RPC endpoint (with API key if needed)
    pub rpc_url: String,
    
    /// Wallet private key (encrypted in production!)
    pub wallet_private_key: String,
    
    /// Wallet address (derived from private key)
    pub wallet_address: String,
    
    /// Maximum gas price willing to pay (in gwei for EVM, lamports for Solana)
    pub max_gas_price: u64,
    
    /// Gas price multiplier for urgency (1.0 = normal, 1.5 = fast, 2.0 = instant)
    pub gas_multiplier: f64,
    
    /// Default slippage tolerance (basis points, 100 = 1%)
    pub slippage_bps: u32,
    
    /// MEV protection enabled
    pub mev_protection: bool,
    
    /// Use Flashbots/private mempool
    pub private_mempool: bool,
    
    /// Transaction deadline (seconds from submission)
    pub deadline_seconds: u64,
    
    /// Minimum confirmations before considering executed
    pub min_confirmations: u32,
    
    /// Router contract address (for EVM DEXs)
    pub router_address: Option<String>,
    
    /// Additional configuration
    pub custom_params: HashMap<String, String>,
}

impl Default for DexConfig {
    fn default() -> Self {
        Self {
            exchange_name: "Cetus".to_string(), // SUI's leading DEX
            network: BlockchainNetwork::Sui,
            rpc_url: "https://fullnode.mainnet.sui.io:443".to_string(),
            wallet_private_key: String::new(),
            wallet_address: String::new(),
            max_gas_price: 100_000, // SUI MIST (much cheaper than Ethereum)
            gas_multiplier: 1.1, // Less competition than Ethereum
            slippage_bps: 30, // 0.3% - SUI has deeper liquidity
            mev_protection: false, // Different dynamics on SUI
            private_mempool: false,
            deadline_seconds: 60, // Much faster finality
            min_confirmations: 1,
            router_address: None, // SUI uses package IDs
            custom_params: HashMap::new(),
        }
    }
}

/// DEX-specific execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DexExecutionResult {
    /// Standard execution result
    pub base: ExecutionResult,
    
    /// Transaction hash
    pub tx_hash: String,
    
    /// Block number
    pub block_number: Option<u64>,
    
    /// Gas used
    pub gas_used: u64,
    
    /// Gas price paid
    pub gas_price: u64,
    
    /// Total gas cost in native token
    pub gas_cost_native: f64,
    
    /// Actual slippage encountered (basis points)
    pub actual_slippage_bps: u32,
    
    /// MEV protection used
    pub mev_protected: bool,
    
    /// Confirmations received
    pub confirmations: u32,
}

/// Trait for DEX connectors
#[async_trait]
pub trait DexConnector: Send + Sync {
    /// Initialize the DEX connector
    async fn initialize(&mut self, config: DexConfig) -> Result<(), ExecutionError>;
    
    /// Execute a swap/trade on the DEX
    async fn execute_swap(&self, signal: &Signal) -> Result<DexExecutionResult, ExecutionError>;
    
    /// Get quote for a trade (price and expected output)
    async fn get_quote(&self, token_in: &str, token_out: &str, amount_in: f64) -> Result<DexQuote, ExecutionError>;
    
    /// Estimate gas for a transaction
    async fn estimate_gas(&self, signal: &Signal) -> Result<GasEstimate, ExecutionError>;
    
    /// Check transaction status
    async fn check_transaction(&self, tx_hash: &str) -> Result<TransactionStatus, ExecutionError>;
    
    /// Cancel pending transaction (if possible - depends on network)
    async fn cancel_transaction(&self, tx_hash: &str) -> Result<(), ExecutionError>;
    
    /// Get wallet balance for token
    async fn get_balance(&self, token_address: &str) -> Result<f64, ExecutionError>;
    
    /// Approve token spending (EVM only)
    async fn approve_token(&self, token_address: &str, spender: &str, amount: f64) -> Result<String, ExecutionError>;
}

/// Quote information from DEX
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DexQuote {
    pub token_in: String,
    pub token_out: String,
    pub amount_in: f64,
    pub expected_amount_out: f64,
    pub minimum_amount_out: f64, // After slippage
    pub price_impact_bps: u32,
    pub route: Vec<String>, // Token addresses in swap path
    pub estimated_gas: u64,
    pub timestamp_ns: u64,
}

/// Gas estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GasEstimate {
    pub gas_limit: u64,
    pub base_fee: u64,        // EIP-1559 base fee
    pub priority_fee: u64,     // EIP-1559 priority fee
    pub max_fee: u64,          // Total max fee willing to pay
    pub estimated_cost_usd: f64,
}

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionStatus {
    Pending,
    Confirmed(u32),      // Number of confirmations
    Failed(u32),         // Error code
    Dropped,             // Transaction dropped from mempool
}
