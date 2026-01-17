//! Phase 3: Comprehensive Devnet Testing
//!
//! Tests both Cetus (AMM) and DeepBook (CLOB) with real devnet transactions
//!
//! Usage:
//!   cargo run --example test_dex_devnet -- [cetus|deepbook|both]
//!
//! Before running:
//!   1. Set environment variable: SUI_PRIVATE_KEY="your_key_here"
//!   2. Fund your devnet wallet:
//!      sui client faucet --address YOUR_ADDRESS
//!   3. Verify balance:
//!      sui client gas

use executionhandler::exchanges::dex::{
    CetusConnector, DeepBookConnector, DexConnector, DexConfig, BlockchainNetwork, 
    SuiWallet, SuiNetworkConfig,
};
use executionhandler::signal::{Signal, SignalAction};
use executionhandler::core::types::ExecutionStatus;
use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, Instant};

#[tokio::main]
async fn main() {
    println!("\n{}", "=".repeat(80));
    println!("🧪 PHASE 3: DEX DEVNET TESTING");
    println!("{}", "=".repeat(80));
    
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let test_mode = if args.len() > 1 {
        args[1].as_str()
    } else {
        "both"
    };
    
    // Get private key from environment
    let private_key = match env::var("SUI_PRIVATE_KEY") {
        Ok(key) => {
            println!("✓ Private key loaded from environment");
            key
        }
        Err(_) => {
            eprintln!("\n❌ ERROR: SUI_PRIVATE_KEY environment variable not set!");
            eprintln!("\nTo set it:");
            eprintln!("  PowerShell: $env:SUI_PRIVATE_KEY = \"your_private_key_here\"");
            eprintln!("  Bash: export SUI_PRIVATE_KEY=\"your_private_key_here\"");
            eprintln!("\nTo generate a new key:");
            eprintln!("  sui client new-address ed25519");
            std::process::exit(1);
        }
    };
    
    // Setup wallet
    println!("\n📱 Setting up SUI Wallet");
    let network_config = SuiNetworkConfig::devnet();
    let wallet = match SuiWallet::new(&private_key, &network_config.rpc_url).await {
        Ok(w) => {
            println!("   ✓ Wallet created");
            println!("   Network: Devnet");
            println!("   RPC: {}", network_config.rpc_url);
            println!("   Address: {}", w.address());
            w
        }
        Err(e) => {
            eprintln!("   ✗ Failed to create wallet: {}", e);
            std::process::exit(1);
        }
    };
    
    // Check balance
    println!("\n💰 Checking Wallet Balance");
    let _balance = match wallet.get_coin_balance("0x2::sui::SUI").await {
        Ok(b) => {
            let sui_balance = b as f64 / 1_000_000_000.0;
            println!("   Balance: {} SUI ({} MIST)", sui_balance, b);
            
            if b < 100_000_000 { // Less than 0.1 SUI
                eprintln!("\n   ⚠️  WARNING: Low balance!");
                eprintln!("   Recommended: At least 0.5 SUI for testing");
                eprintln!("   To fund: sui client faucet --address {}", wallet.address());
                
                if b < 10_000_000 { // Less than 0.01 SUI
                    eprintln!("\n   ❌ Insufficient funds for testing. Exiting.");
                    std::process::exit(1);
                }
            }
            b
        }
        Err(e) => {
            eprintln!("   ✗ Failed to check balance: {}", e);
            std::process::exit(1);
        }
    };
    
    // Run tests based on mode
    let mut test_results = TestResults::new();
    
    match test_mode {
        "cetus" => {
            println!("\n{}", "=".repeat(80));
            println!("🔵 Testing CETUS (AMM)");
            println!("{}", "=".repeat(80));
            test_cetus(&wallet, &private_key, &mut test_results).await;
        }
        "deepbook" => {
            println!("\n{}", "=".repeat(80));
            println!("📗 Testing DEEPBOOK (CLOB)");
            println!("{}", "=".repeat(80));
            test_deepbook(&wallet, &private_key, &mut test_results).await;
        }
        "both" | _ => {
            println!("\n{}", "=".repeat(80));
            println!("🔵 Testing CETUS (AMM)");
            println!("{}", "=".repeat(80));
            test_cetus(&wallet, &private_key, &mut test_results).await;
            
            println!("\n{}", "=".repeat(80));
            println!("📗 Testing DEEPBOOK (CLOB)");
            println!("{}", "=".repeat(80));
            test_deepbook(&wallet, &private_key, &mut test_results).await;
        }
    }
    
    // Print final summary
    test_results.print_summary();
}

struct TestResults {
    cetus_quote: Option<bool>,
    cetus_swap: Option<(bool, String)>,
    deepbook_init: Option<bool>,
    deepbook_limit: Option<(bool, String)>,
    total_gas_used: u64,
    total_latency_ms: u64,
}

impl TestResults {
    fn new() -> Self {
        Self {
            cetus_quote: None,
            cetus_swap: None,
            deepbook_init: None,
            deepbook_limit: None,
            total_gas_used: 0,
            total_latency_ms: 0,
        }
    }
    
    fn print_summary(&self) {
        println!("\n{}", "=".repeat(80));
        println!("📊 TEST SUMMARY");
        println!("{}", "=".repeat(80));
        
        let mut passed = 0;
        let mut total = 0;
        
        if let Some(result) = self.cetus_quote {
            total += 1;
            if result {
                passed += 1;
                println!("   ✅ Cetus Quote");
            } else {
                println!("   ❌ Cetus Quote");
            }
        }
        
        if let Some((result, tx)) = &self.cetus_swap {
            total += 1;
            if *result {
                passed += 1;
                println!("   ✅ Cetus Swap - https://suiscan.xyz/devnet/tx/{}", tx);
            } else {
                println!("   ❌ Cetus Swap");
            }
        }
        
        if let Some(result) = self.deepbook_init {
            total += 1;
            if result {
                passed += 1;
                println!("   ✅ DeepBook Init");
            } else {
                println!("   ❌ DeepBook Init");
            }
        }
        
        if let Some((result, tx)) = &self.deepbook_limit {
            total += 1;
            if *result {
                passed += 1;
                println!("   ✅ DeepBook Limit Order - https://suiscan.xyz/devnet/tx/{}", tx);
            } else {
                println!("   ❌ DeepBook Limit Order");
            }
        }
        
        println!("\n   Tests Passed: {}/{}", passed, total);
        println!("   Total Gas Used: {} MIST ({} SUI)", 
            self.total_gas_used, 
            self.total_gas_used as f64 / 1_000_000_000.0
        );
        println!("   Total Latency: {}ms", self.total_latency_ms);
        
        if passed == total {
            println!("\n   🎉 ALL TESTS PASSED! Ready for mainnet.");
        } else {
            println!("\n   ⚠️  Some tests failed. Review errors above.");
        }
        
        println!("{}", "=".repeat(80));
    }
}

async fn test_cetus(wallet: &SuiWallet, private_key: &str, results: &mut TestResults) {
    // Initialize Cetus connector
    println!("\n1️⃣  Initializing Cetus Connector");
    let mut cetus = CetusConnector::new();
    
    let config = DexConfig {
        exchange_name: "Cetus".to_string(),
        network: BlockchainNetwork::SuiDevnet,
        rpc_url: "https://fullnode.devnet.sui.io:443".to_string(),
        wallet_private_key: private_key.to_string(),
        wallet_address: wallet.address().to_string(),
        max_gas_price: 100_000,
        gas_multiplier: 1.1,
        slippage_bps: 50, // 0.5%
        mev_protection: false,
        private_mempool: false,
        deadline_seconds: 60,
        min_confirmations: 1,
        router_address: None,
        custom_params: HashMap::new(),
    };
    
    if let Err(e) = cetus.initialize(config).await {
        eprintln!("   ✗ Failed to initialize: {}", e);
        results.cetus_quote = Some(false);
        results.cetus_swap = Some((false, String::new()));
        return;
    }
    println!("   ✓ Cetus connector initialized");
    
    // Test 1: Get Quote
    println!("\n2️⃣  Testing Quote (SUI → USDC)");
    println!("   Query: 0.1 SUI → USDC");
    
    let quote_result = match cetus.get_quote("SUI", "USDC", 0.1).await {
        Ok(quote) => {
            println!("   ✓ Quote received:");
            println!("     Expected output: {} USDC", quote.expected_amount_out);
            println!("     Minimum output: {} USDC", quote.minimum_amount_out);
            println!("     Price: {} USDC per SUI", quote.expected_amount_out / quote.amount_in);
            println!("     Price impact: {} bps", quote.price_impact_bps);
            println!("     Route: {} → {}", quote.route.join(" → "), quote.token_out);
            println!("     Est. gas: {} MIST", quote.estimated_gas);
            results.cetus_quote = Some(true);
            true
        }
        Err(e) => {
            eprintln!("   ✗ Failed: {}", e);
            results.cetus_quote = Some(false);
            false
        }
    };
    
    if !quote_result {
        results.cetus_swap = Some((false, String::new()));
        return;
    }
    
    // Test 2: Execute Small Swap
    println!("\n3️⃣  Testing Swap Execution");
    println!("   ⚠️  EXECUTING REAL TRANSACTION");
    println!("   Amount: 0.01 SUI → USDC (small test)");
    
    let signal = Signal {
        id: format!("cetus_test_{}", chrono::Utc::now().timestamp_millis()),
        strategy_id: "phase3_testing".to_string(),
        action: SignalAction::Buy,
        symbol: "SUI/USDC".to_string(),
        exchange: "Cetus".to_string(),
        quantity: 0.01, // Very small amount for testing
        price: None, // Market order
        timestamp: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
        confidence: 1.0,
        metadata: HashMap::new(),
    };
    
    let start = Instant::now();
    
    match cetus.execute_swap(&signal).await {
        Ok(result) => {
            let latency = start.elapsed().as_millis() as u64;
            
            println!("\n   ✅ SWAP SUCCESSFUL!");
            println!("   TX Hash: {}", result.tx_hash);
            println!("   Status: {:?}", result.base.status);
            println!("   Filled: {} SUI", result.base.filled_quantity);
            println!("   Avg Price: {} USDC/SUI", result.base.avg_fill_price);
            println!("   Total Fees: ${}", result.base.total_fees);
            println!("   Gas Used: {} MIST ({} SUI)", 
                result.gas_used,
                result.gas_cost_native
            );
            println!("   Slippage: {} bps (limit: 50 bps)", result.actual_slippage_bps);
            println!("   Latency: {}ms", latency);
            println!("   Confirmations: {}", result.confirmations);
            
            if result.base.status != ExecutionStatus::Filled {
                eprintln!("   ⚠️  WARNING: Order not immediately filled!");
            }
            
            results.cetus_swap = Some((true, result.tx_hash.clone()));
            results.total_gas_used += result.gas_used;
            results.total_latency_ms += latency;
        }
        Err(e) => {
            eprintln!("\n   ✗ Swap failed: {}", e);
            eprintln!("   Possible causes:");
            eprintln!("   - Pool address incorrect for devnet");
            eprintln!("   - Package address mismatch");
            eprintln!("   - Insufficient liquidity");
            eprintln!("   - Gas estimation error");
            results.cetus_swap = Some((false, String::new()));
        }
    }
}

async fn test_deepbook(wallet: &SuiWallet, private_key: &str, results: &mut TestResults) {
    // Initialize DeepBook connector
    println!("\n1️⃣  Initializing DeepBook Connector");
    let mut deepbook = DeepBookConnector::new();
    
    let config = DexConfig {
        exchange_name: "DeepBook".to_string(),
        network: BlockchainNetwork::SuiDevnet,
        rpc_url: "https://fullnode.devnet.sui.io:443".to_string(),
        wallet_private_key: private_key.to_string(),
        wallet_address: wallet.address().to_string(),
        max_gas_price: 100_000,
        gas_multiplier: 1.1,
        slippage_bps: 0, // No slippage on limit orders
        mev_protection: false,
        private_mempool: false,
        deadline_seconds: 300, // 5 minutes for limit orders
        min_confirmations: 1,
        router_address: None,
        custom_params: HashMap::new(),
    };
    
    if let Err(e) = deepbook.initialize(config).await {
        eprintln!("   ✗ Failed to initialize: {}", e);
        results.deepbook_init = Some(false);
        results.deepbook_limit = Some((false, String::new()));
        return;
    }
    println!("   ✓ DeepBook connector initialized");
    results.deepbook_init = Some(true);
    
    // Test: Place Limit Order
    println!("\n2️⃣  Testing Limit Order Placement");
    println!("   ⚠️  EXECUTING REAL TRANSACTION");
    
    // Get current price estimate
    println!("   Getting price reference...");
    let ref_price = match deepbook.get_quote("SUI", "USDC", 0.01).await {
        Ok(quote) => {
            let price = quote.expected_amount_out / quote.amount_in;
            println!("   Reference price: {} USDC/SUI", price);
            price
        }
        Err(e) => {
            eprintln!("   ✗ Failed to get quote: {}", e);
            2.0 // Fallback
        }
    };
    
    // Place limit order slightly above market (more likely to fill on buy)
    let limit_price = (ref_price * 1.01).max(0.5); // At least $0.50
    
    println!("\n   Placing BUY limit order:");
    println!("   Quantity: 0.01 SUI");
    println!("   Price: ${} USDC/SUI", limit_price);
    
    let signal = Signal {
        id: format!("deepbook_test_{}", chrono::Utc::now().timestamp_millis()),
        strategy_id: "phase3_testing".to_string(),
        action: SignalAction::BuyLimit,
        symbol: "SUI/USDC".to_string(),
        exchange: "DeepBook".to_string(),
        quantity: 0.01, // Small test order
        price: Some(limit_price),
        timestamp: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
        confidence: 1.0,
        metadata: HashMap::new(),
    };
    
    let start = Instant::now();
    
    match deepbook.execute_swap(&signal).await {
        Ok(result) => {
            let latency = start.elapsed().as_millis() as u64;
            
            println!("\n   ✅ LIMIT ORDER PLACED!");
            println!("   TX Hash: {}", result.tx_hash);
            println!("   Order ID: {}", result.base.order_id);
            println!("   Status: {:?}", result.base.status);
            println!("   Side: BUY");
            println!("   Quantity: {} SUI", signal.quantity);
            println!("   Limit Price: ${} USDC", limit_price);
            println!("   Gas Used: {} MIST ({} SUI)", 
                result.gas_used,
                result.gas_cost_native
            );
            println!("   Latency: {}ms", latency);
            
            if result.base.status == ExecutionStatus::Pending {
                println!("\n   📌 Order Status: PENDING (waiting for fill)");
                println!("   The order is now in the DeepBook order book.");
                println!("   It will fill when market price reaches ${}", limit_price);
                println!("\n   To cancel this order, save this transaction hash:");
                println!("   {}", result.tx_hash);
            } else {
                println!("\n   🎯 Order Status: FILLED");
                println!("   Filled: {} SUI", result.base.filled_quantity);
                println!("   Avg Price: ${}", result.base.avg_fill_price);
            }
            
            results.deepbook_limit = Some((true, result.tx_hash.clone()));
            results.total_gas_used += result.gas_used;
            results.total_latency_ms += latency;
        }
        Err(e) => {
            eprintln!("\n   ✗ Order placement failed: {}", e);
            eprintln!("   Possible causes:");
            eprintln!("   - Pool ID incorrect for devnet");
            eprintln!("   - DeepBook package address mismatch");
            eprintln!("   - Order size below minimum");
            eprintln!("   - Insufficient balance for collateral");
            results.deepbook_limit = Some((false, String::new()));
        }
    }
    
    // Note about order management
    println!("\n3️⃣  Order Management");
    println!("   ℹ️  To check order status:");
    println!("      - View transaction on SuiScan");
    println!("      - Query DeepBook pool for open orders");
    println!("      - Use cancel_transaction() to cancel");
    
    println!("\n   ℹ️  DeepBook Features:");
    println!("      ✓ True limit orders (like CEX)");
    println!("      ✓ Price priority matching");
    println!("      ✓ Time priority for same price");
    println!("      ✓ Order cancellation supported");
    println!("      ✓ Low fees: 5 bps maker, 10 bps taker");
    println!("      ✓ No impermanent loss");
}
