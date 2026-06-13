//! Decentralized Exchange (DEX) Integration Module
//!
//! Provides unified interface for executing trades on DEXs across multiple chains.
//! Handles blockchain-specific concerns: gas estimation, transaction signing,
//! slippage tolerance, MEV protection, and confirmation monitoring.
//!
//! PRIORITY: SUI Network - Built specifically for high-frequency trading with:
//! - Sub-second finality (~400ms)
//! - Parallel transaction execution
//! - Extremely low gas fees
//! - Native CLOB (DeepBook) support

pub mod traits;
pub mod sui_wallet;
pub mod sui_ptb;
pub mod cetus;
pub mod cetus_constants;
pub mod deepbook;
pub mod deepbook_constants;
pub mod uniswap_v3;
pub mod jupiter;
pub mod adapter;

pub use traits::{DexConnector, DexConfig, BlockchainNetwork};
pub use sui_wallet::{SuiWallet, SuiNetworkConfig};
pub use sui_ptb::{PtbBuilder, TransactionData, ObjectRef, TypeTag, Argument};
pub use cetus::CetusConnector;
pub use cetus_constants as cetus_config;
pub use deepbook::DeepBookConnector;
pub use deepbook_constants as deepbook_config;
pub use uniswap_v3::UniswapV3Connector;
pub use jupiter::JupiterConnector;
pub use adapter::DexToExchangeAdapter;
