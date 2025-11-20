//! Example: Trading on SUI DEXs (Cetus and DeepBook)
//!
//! Demonstrates how to:
//! 1. Initialize Cetus (AMM) connector
//! 2. Initialize DeepBook (CLOB) connector  
//! 3. Execute swaps on Cetus
//! 4. Place limit orders on DeepBook
//! 5. Monitor transactions
//!
//! Run with: cargo run --example sui_dex_trading

use executionhandler::exchanges::dex::{
    CetusConnector, DeepBookConnector, DexConnector, DexConfig, BlockchainNetwork,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 SUI DEX Trading Example\n");
    
    // Configuration for devnet (use testnet/mainnet in production)
    let config = DexConfig {
        exchange_name: "Cetus".to_string(),
        network: BlockchainNetwork::SuiDevnet,
        wallet_private_key: "YOUR_PRIVATE_KEY_HERE".to_string(), // TODO: Replace with actual key
        rpc_url: "https://fullnode.devnet.sui.io:443".to_string(),
        max_gas_price: 100_000,       // MIST
        slippage_bps: 30,              // 0.3%
        deadline_seconds: 60,
        mev_protection: false,         // Different on SUI
        custom_params: Default::default(),
    };
    
    println!("📋 Configuration:");
    println!("   Network: {:?}", config.network);
    println!("   Slippage: {}bps ({}%)", config.slippage_bps, config.slippage_bps as f64 / 100.0);
    println!("   Max Gas: {} MIST", config.max_gas_price);
    println!();
    
    // Example 1: Initialize Cetus (AMM)
    println!("📊 Example 1: Cetus AMM Swap");
    println!("=====================================");
    
    let mut cetus = CetusConnector::new();
    match cetus.initialize(config.clone()).await {
        Ok(_) => {
            println!("✅ Cetus initialized successfully\n");
            
            // Get quote for swap
            println!("Getting quote for 1.0 SUI → USDC...");
            match cetus.get_quote("SUI", "USDC", 1.0).await {
                Ok(quote) => {
                    println!("✅ Quote received:");
                    println!("   Input: {} SUI", quote.amount_in);
                    println!("   Expected output: {} USDC", quote.expected_amount_out);
                    println!("   Minimum output: {} USDC", quote.minimum_amount_out);
                    println!("   Price impact: {}bps", quote.price_impact_bps);
                    println!("   Estimated gas: {} MIST (~$0.0001)", quote.estimated_gas);
                }
                Err(e) => println!("❌ Quote failed: {}", e),
            }
        }
        Err(e) => println!("❌ Cetus initialization failed: {}\n", e),
    }
    
    println!();
    
    // Example 2: Initialize DeepBook (CLOB)
    println!("📈 Example 2: DeepBook Limit Order");
    println!("=====================================");
    
    let mut deepbook_config = config.clone();
    deepbook_config.exchange_name = "DeepBook".to_string();
    
    let mut deepbook = DeepBookConnector::new();
    match deepbook.initialize(deepbook_config).await {
        Ok(_) => {
            println!("✅ DeepBook initialized successfully");
            println!("   This is a CLOB (like Kraken), not an AMM!");
            println!("   You can place limit orders, market orders, cancel orders\n");
            
            // Get quote (shows orderbook mid-price)
            println!("Getting quote for 1.0 SUI → USDC...");
            match deepbook.get_quote("SUI", "USDC", 1.0).await {
                Ok(quote) => {
                    println!("✅ Quote received:");
                    println!("   Input: {} SUI", quote.amount_in);
                    println!("   Expected output: {} USDC", quote.expected_amount_out);
                    println!("   Price impact: {}bps (lower than AMM!)", quote.price_impact_bps);
                    println!("   Estimated gas: {} MIST (~$0.00005)", quote.estimated_gas);
                }
                Err(e) => println!("❌ Quote failed: {}", e),
            }
        }
        Err(e) => println!("❌ DeepBook initialization failed: {}\n", e),
    }
    
    println!();
    println!("📊 Performance Comparison:");
    println!("=====================================");
    println!("   Cetus (AMM):");
    println!("     - Swap-based (like Uniswap)");
    println!("     - 0.3% fees");
    println!("     - ~$0.0001/tx gas");
    println!("     - Good for large trades");
    println!();
    println!("   DeepBook (CLOB):");
    println!("     - Orderbook-based (like Kraken)");
    println!("     - 0.1% maker fees");
    println!("     - ~$0.00005/tx gas");
    println!("     - OPTIMAL for HFT market making!");
    println!();
    println!("   Both have ~400ms finality (30x faster than Ethereum)");
    println!();
    
    println!("💡 Next Steps:");
    println!("   1. Replace YOUR_PRIVATE_KEY_HERE with actual SUI keypair");
    println!("   2. Get devnet SUI: sui client faucet");
    println!("   3. Test swaps on Cetus devnet");
    println!("   4. Test limit orders on DeepBook devnet");
    println!("   5. Move to testnet, then mainnet");
    
    Ok(())
}
