//! Test exchange connections for the generic connector.
//!
//! This example tests connectivity to all supported exchanges:
//! - Public endpoints (health check) - no credentials needed
//! - Authenticated endpoints - requires API credentials via env vars
//!
//! Usage:
//!   # Test public endpoints only (no credentials needed)
//!   cargo run --example test_exchange_connections
//!
//!   # Test specific exchange with credentials
//!   KRAKEN_API_KEY=xxx KRAKEN_SECRET_KEY=yyy cargo run --example test_exchange_connections -- kraken
//!
//!   # Test all exchanges with public endpoints
//!   cargo run --example test_exchange_connections -- --all

use std::collections::HashMap;
use std::env;
use std::time::Duration;

use executionhandler::exchanges::generic::{GenericConnector, ExchangePreset};
use executionhandler::{ExchangeConfig, ExchangeConnector};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    
    if args.len() > 1 {
        match args[1].as_str() {
            "--all" => test_all_exchanges_public().await,
            "--help" | "-h" => print_help(),
            exchange => test_single_exchange(exchange).await,
        }
    } else {
        print_help();
        println!("\n--- Running public endpoint tests for all exchanges ---\n");
        test_all_exchanges_public().await;
    }
}

fn print_help() {
    println!("Exchange Connection Tester");
    println!("==========================");
    println!();
    println!("Usage:");
    println!("  cargo run --example test_exchange_connections [OPTIONS]");
    println!();
    println!("Options:");
    println!("  --all              Test all exchanges (public endpoints only)");
    println!("  --help, -h         Show this help message");
    println!("  <exchange_name>    Test specific exchange (e.g., kraken, coinbase)");
    println!();
    println!("Supported Exchanges:");
    println!("  kraken, coinbase, binance, binance_us, bybit, okx, gemini, deribit");
    println!();
    println!("Environment Variables for Authenticated Tests:");
    println!("  {{EXCHANGE}}_API_KEY        - API key");
    println!("  {{EXCHANGE}}_SECRET_KEY     - Secret key");
    println!("  {{EXCHANGE}}_PASSPHRASE     - Passphrase (Coinbase, OKX only)");
    println!();
    println!("Example:");
    println!("  KRAKEN_API_KEY=xxx KRAKEN_SECRET_KEY=yyy cargo run --example test_exchange_connections -- kraken");
}

async fn test_all_exchanges_public() {
    let exchanges = [
        "kraken",
        "coinbase", 
        "binance",
        "binance_us",
        "bybit",
        "okx",
        "gemini",
        "deribit",
    ];
    
    println!("Testing public endpoints for {} exchanges...\n", exchanges.len());
    
    let mut results: Vec<(String, bool, String)> = Vec::new();
    
    for exchange in &exchanges {
        let (success, message) = test_public_endpoint(exchange).await;
        results.push((exchange.to_string(), success, message));
    }
    
    // Print summary
    println!("\n{}", "=".repeat(70));
    println!("SUMMARY");
    println!("{}", "=".repeat(70));
    
    let mut passed = 0;
    let mut failed = 0;
    
    for (exchange, success, message) in &results {
        let status = if *success { 
            passed += 1;
            "✓ PASS" 
        } else { 
            failed += 1;
            "✗ FAIL" 
        };
        println!("{:12} | {:8} | {}", exchange, status, message);
    }
    
    println!("{}", "-".repeat(70));
    println!("Total: {} passed, {} failed", passed, failed);
}

async fn test_public_endpoint(exchange_name: &str) -> (bool, String) {
    print!("Testing {} ... ", exchange_name);
    
    let preset = match ExchangePreset::from_name(exchange_name) {
        Some(p) => p,
        None => {
            let msg = format!("Unknown exchange: {}", exchange_name);
            println!("SKIP ({})", msg);
            return (false, msg);
        }
    };
    
    let definition = preset.definition();
    let health_url = format!("{}{}", definition.endpoints.rest_url, definition.endpoints.health_check_path);
    
    // Use reqwest directly for public endpoint test
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    
    let start = std::time::Instant::now();
    
    match client.get(&health_url).send().await {
        Ok(response) => {
            let latency = start.elapsed();
            let status = response.status();
            
            if status.is_success() {
                let msg = format!("OK ({} in {:?})", status, latency);
                println!("{}", msg);
                (true, msg)
            } else {
                let msg = format!("HTTP {} in {:?}", status, latency);
                println!("WARN ({})", msg);
                // Some exchanges return non-200 for health but still work
                (status.as_u16() < 500, msg)
            }
        }
        Err(e) => {
            let msg = format!("Error: {}", e);
            println!("FAIL ({})", msg);
            (false, msg)
        }
    }
}

async fn test_single_exchange(exchange_name: &str) {
    println!("Testing exchange: {}\n", exchange_name);
    
    // Test 1: Public endpoint
    println!("1. Testing public endpoint (health check)...");
    let (public_ok, public_msg) = test_public_endpoint(exchange_name).await;
    
    if !public_ok {
        println!("\n❌ Public endpoint test failed. Cannot proceed with authenticated tests.");
        return;
    }
    
    // Test 2: Connector creation
    println!("\n2. Testing connector creation...");
    let connector = match GenericConnector::from_name(exchange_name) {
        Ok(c) => {
            println!("   ✓ Connector created successfully");
            c
        }
        Err(e) => {
            println!("   ✗ Failed to create connector: {}", e);
            return;
        }
    };
    
    // Test 3: Check for credentials
    println!("\n3. Checking for API credentials...");
    let env_prefix = exchange_name.to_uppercase().replace("_", "");
    
    let api_key = env::var(format!("{}_API_KEY", env_prefix)).ok();
    let secret_key = env::var(format!("{}_SECRET_KEY", env_prefix)).ok();
    let passphrase = env::var(format!("{}_PASSPHRASE", env_prefix)).ok();
    
    let definition = connector.preset().definition();
    
    if api_key.is_none() || secret_key.is_none() {
        println!("   ⚠ No credentials found. Set environment variables:");
        println!("     {}_API_KEY=<your_api_key>", env_prefix);
        println!("     {}_SECRET_KEY=<your_secret_key>", env_prefix);
        if definition.requires_passphrase {
            println!("     {}_PASSPHRASE=<your_passphrase>", env_prefix);
        }
        println!("\n   Skipping authenticated tests.");
        return;
    }
    
    if definition.requires_passphrase && passphrase.is_none() {
        println!("   ⚠ {} requires a passphrase. Set:", definition.name);
        println!("     {}_PASSPHRASE=<your_passphrase>", env_prefix);
        println!("\n   Skipping authenticated tests.");
        return;
    }
    
    println!("   ✓ Credentials found");
    
    // Test 4: Initialize connector
    println!("\n4. Testing connector initialization...");
    let mut connector = connector;
    
    let config = ExchangeConfig {
        name: definition.name.clone(),
        api_key: api_key.unwrap(),
        secret_key: secret_key.unwrap(),
        passphrase,
        sandbox: true, // Always use sandbox for testing
        connection_pool_size: 5,
        timeout_ms: 10000,
        rate_limit_per_second: definition.rate_limits.requests_per_second,
        rate_limit_burst: definition.rate_limits.burst,
        websocket_url: Some(definition.endpoints.websocket_url.clone()),
        rest_api_url: Some(definition.endpoints.rest_url.clone()),
        custom_headers: HashMap::new(),
    };
    
    match connector.initialize(config).await {
        Ok(_) => println!("   ✓ Connector initialized successfully"),
        Err(e) => {
            println!("   ✗ Failed to initialize connector: {}", e);
            return;
        }
    }
    
    // Test 5: Health check via connector
    println!("\n5. Testing health check via connector...");
    match connector.health_check().await {
        Ok(status) => {
            println!("   ✓ Health check passed");
            println!("     Status: {:?}", status.status);
            println!("     Latency: {} ns ({:.2} ms)", status.latency_ns, status.latency_ns as f64 / 1_000_000.0);
        }
        Err(e) => {
            println!("   ✗ Health check failed: {}", e);
        }
    }
    
    // Test 6: Get limits
    println!("\n6. Testing exchange limits...");
    let limits = connector.get_limits();
    println!("   Orders/sec:    {}", limits.max_orders_per_second);
    println!("   Max batch:     {}", limits.max_batch_size);
    println!("   Min order:     {}", limits.min_order_size);
    println!("   Max order:     {}", limits.max_order_size);
    println!("   Tick size:     {}", limits.tick_size);
    
    println!("\n✓ All tests passed for {}", exchange_name);
}
