//! Authenticated Exchange API Tests
//!
//! Tests authenticated endpoints (balance, orders) using API keys from environment variables.
//! Designed for CI/CD pipelines where secrets are injected from Azure Key Vault.
//!
//! Usage:
//!   cargo run --example test_exchange_apis_authenticated
//!   cargo run --example test_exchange_apis_authenticated -- kraken
//!   cargo run --example test_exchange_apis_authenticated -- binance
//!   cargo run --example test_exchange_apis_authenticated -- all
//!
//! Required environment variables:
//!   KRAKEN_API_KEY, KRAKEN_SECRET_KEY
//!   BINANCE_API_KEY, BINANCE_SECRET_KEY (optional)

use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type HmacSha512 = Hmac<Sha512>;
type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Deserialize)]
struct KrakenResponse<T> {
    error: Vec<String>,
    result: Option<T>,
}

#[derive(Debug, Deserialize)]
struct KrakenBalance {
    #[serde(flatten)]
    balances: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct BinanceAccountInfo {
    balances: Vec<BinanceBalance>,
}

#[derive(Debug, Deserialize)]
struct BinanceBalance {
    asset: String,
    free: String,
    locked: String,
}

#[tokio::main]
async fn main() {
    println!("\n{}", "=".repeat(80));
    println!("🔐 AUTHENTICATED EXCHANGE API TESTS");
    println!("{}", "=".repeat(80));

    let args: Vec<String> = env::args().collect();
    let exchange = if args.len() > 1 {
        args[1].to_lowercase()
    } else {
        "all".to_string()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client");

    let mut results: Vec<(&str, Result<String, String>)> = Vec::new();
    let mut has_tests = false;

    // Kraken Tests
    if exchange == "kraken" || exchange == "all" {
        match (env::var("KRAKEN_API_KEY"), env::var("KRAKEN_SECRET_KEY")) {
            (Ok(api_key), Ok(secret_key)) => {
                has_tests = true;
                results.push(("Kraken Auth", test_kraken_authenticated(&client, &api_key, &secret_key).await));
            }
            _ => {
                println!("\n⚠️  Skipping Kraken: KRAKEN_API_KEY or KRAKEN_SECRET_KEY not set");
            }
        }
    }

    // Binance Tests
    if exchange == "binance" || exchange == "all" {
        match (env::var("BINANCE_API_KEY"), env::var("BINANCE_SECRET_KEY")) {
            (Ok(api_key), Ok(secret_key)) => {
                has_tests = true;
                results.push(("Binance Auth", test_binance_authenticated(&client, &api_key, &secret_key).await));
            }
            _ => {
                println!("\n⚠️  Skipping Binance: BINANCE_API_KEY or BINANCE_SECRET_KEY not set");
            }
        }
    }

    if !has_tests {
        eprintln!("\n❌ No API credentials found! Set environment variables:");
        eprintln!("   KRAKEN_API_KEY, KRAKEN_SECRET_KEY");
        eprintln!("   BINANCE_API_KEY, BINANCE_SECRET_KEY");
        std::process::exit(1);
    }

    // Summary
    println!("\n{}", "=".repeat(80));
    println!("📊 SUMMARY");
    println!("{}", "=".repeat(80));

    let mut all_passed = true;
    for (name, result) in &results {
        match result {
            Ok(info) => println!("   ✅ {} - {}", name, info),
            Err(e) => {
                println!("   ❌ {} - FAILED: {}", name, e);
                all_passed = false;
            }
        }
    }

    if all_passed {
        println!("\n✅ All authenticated API tests passed!");
    } else {
        println!("\n⚠️  Some authenticated API tests failed");
        std::process::exit(1);
    }
}

async fn test_kraken_authenticated(
    client: &Client,
    api_key: &str,
    secret_key: &str,
) -> Result<String, String> {
    println!("\n🔐 Testing Kraken Authenticated API...");

    // Test 1: Get Account Balance
    println!("   [1/2] Fetching account balance...");
    let start = Instant::now();

    // Use nanoseconds for nonce - must be higher than any previously used nonce for this API key
    // DataEngine uses nanoseconds, so we must too to avoid "Invalid nonce" errors
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let path = "/0/private/Balance";
    let body = format!("nonce={}", nonce);

    // Create signature
    let signature = sign_kraken_request(path, &body, nonce, secret_key)?;

    let response = client
        .post(format!("https://api.kraken.com{}", path))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("API-Key", api_key)
        .header("API-Sign", &signature)
        .body(body)
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let latency = start.elapsed().as_millis();

    let response_text = response
        .text()
        .await
        .map_err(|e| format!("Failed to read response: {}", e))?;

    let balance_response: KrakenResponse<KrakenBalance> = serde_json::from_str(&response_text)
        .map_err(|e| format!("Failed to parse response: {}. Raw: {}", e, response_text))?;

    if !balance_response.error.is_empty() {
        return Err(format!("Kraken API error: {:?}", balance_response.error));
    }

    let balances = balance_response.result.unwrap_or(KrakenBalance {
        balances: HashMap::new(),
    });

    // Count non-zero balances
    let non_zero: Vec<_> = balances
        .balances
        .iter()
        .filter(|(_, v)| v.parse::<f64>().unwrap_or(0.0) > 0.0)
        .collect();

    println!("         ✓ Balance retrieved ({}ms)", latency);
    println!("         Assets with balance: {}", non_zero.len());

    // Show top 5 balances
    for (asset, amount) in non_zero.iter().take(5) {
        println!("           - {}: {}", asset, amount);
    }

    // Test 2: Get open orders (read-only, safe)
    println!("   [2/2] Fetching open orders...");
    let start = Instant::now();

    // Use nanoseconds for nonce - must be higher than any previously used nonce
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let path = "/0/private/OpenOrders";
    let body = format!("nonce={}", nonce);
    let signature = sign_kraken_request(path, &body, nonce, secret_key)?;

    let response = client
        .post(format!("https://api.kraken.com{}", path))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("API-Key", api_key)
        .header("API-Sign", &signature)
        .body(body)
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let latency = start.elapsed().as_millis();

    let response_text = response
        .text()
        .await
        .map_err(|e| format!("Failed to read response: {}", e))?;

    // Just check it's valid JSON with no errors
    let orders_response: KrakenResponse<serde_json::Value> =
        serde_json::from_str(&response_text)
            .map_err(|e| format!("Failed to parse response: {}", e))?;

    if !orders_response.error.is_empty() {
        return Err(format!("Kraken API error: {:?}", orders_response.error));
    }

    println!("         ✓ Open orders retrieved ({}ms)", latency);

    Ok(format!(
        "{} assets with balance, auth OK",
        non_zero.len()
    ))
}

fn sign_kraken_request(
    path: &str,
    body: &str,
    nonce: u64,
    secret_key: &str,
) -> Result<String, String> {
    // Decode base64 secret
    let secret_bytes = base64::engine::general_purpose::STANDARD
        .decode(secret_key)
        .map_err(|e| format!("Invalid secret key: {}", e))?;

    // SHA256(nonce + body)
    let mut sha256 = Sha256::new();
    sha256.update(format!("{}{}", nonce, body).as_bytes());
    let sha256_result = sha256.finalize();

    // Concatenate path + SHA256 result
    let mut message = path.as_bytes().to_vec();
    message.extend_from_slice(&sha256_result);

    // HMAC-SHA512 with decoded secret
    let mut hmac = HmacSha512::new_from_slice(&secret_bytes)
        .map_err(|e| format!("HMAC error: {}", e))?;
    hmac.update(&message);
    let result = hmac.finalize();

    Ok(base64::engine::general_purpose::STANDARD.encode(result.into_bytes()))
}

async fn test_binance_authenticated(
    client: &Client,
    api_key: &str,
    secret_key: &str,
) -> Result<String, String> {
    println!("\n🔐 Testing Binance Authenticated API...");

    // Determine which Binance endpoint to use (US or global)
    let base_url = if env::var("BINANCE_USE_US").is_ok() {
        "https://api.binance.us"
    } else {
        "https://api.binance.com"
    };

    // Test 1: Get Account Info
    println!("   [1/1] Fetching account info...");
    let start = Instant::now();

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let query = format!("timestamp={}", timestamp);

    // Create signature
    let mut hmac = HmacSha256::new_from_slice(secret_key.as_bytes())
        .map_err(|e| format!("HMAC error: {}", e))?;
    hmac.update(query.as_bytes());
    let signature = hex::encode(hmac.finalize().into_bytes());

    let url = format!("{}/api/v3/account?{}&signature={}", base_url, query, signature);

    let response = client
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let latency = start.elapsed().as_millis();

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(format!("HTTP {}: {}", status, text));
    }

    let account_info: BinanceAccountInfo = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    // Count non-zero balances
    let non_zero: Vec<_> = account_info
        .balances
        .iter()
        .filter(|b| {
            b.free.parse::<f64>().unwrap_or(0.0) > 0.0
                || b.locked.parse::<f64>().unwrap_or(0.0) > 0.0
        })
        .collect();

    println!("         ✓ Account info retrieved ({}ms)", latency);
    println!("         Assets with balance: {}", non_zero.len());

    for balance in non_zero.iter().take(5) {
        println!(
            "           - {}: {} (free) + {} (locked)",
            balance.asset, balance.free, balance.locked
        );
    }

    Ok(format!(
        "{} assets with balance, auth OK",
        non_zero.len()
    ))
}
