//! Exchange API Connectivity Test
//!
//! Tests connectivity to exchange APIs (public endpoints, no auth required)
//!
//! Usage:
//!   cargo run --example test_exchange_apis
//!   cargo run --example test_exchange_apis -- kraken
//!   cargo run --example test_exchange_apis -- binance
//!   cargo run --example test_exchange_apis -- all

use reqwest::Client;
use serde::Deserialize;
use std::env;
use std::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
struct KrakenTimeResponse {
    error: Vec<String>,
    result: Option<KrakenTimeResult>,
}

#[derive(Debug, Deserialize)]
struct KrakenTimeResult {
    unixtime: i64,
    rfc1123: String,
}

#[derive(Debug, Deserialize)]
struct KrakenSystemStatusResponse {
    error: Vec<String>,
    result: Option<KrakenSystemStatus>,
}

#[derive(Debug, Deserialize)]
struct KrakenSystemStatus {
    status: String,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct KrakenTickerResponse {
    error: Vec<String>,
    result: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct BinanceTimeResponse {
    serverTime: i64,
}

#[derive(Debug, Deserialize)]
struct BinanceTickerResponse {
    symbol: String,
    price: String,
}

#[tokio::main]
async fn main() {
    println!("\n{}", "=".repeat(80));
    println!("🔌 EXCHANGE API CONNECTIVITY TEST");
    println!("{}", "=".repeat(80));

    let args: Vec<String> = env::args().collect();
    let exchange = if args.len() > 1 {
        args[1].to_lowercase()
    } else {
        "all".to_string()
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");

    let mut results = Vec::new();

    if exchange == "kraken" || exchange == "all" {
        results.push(("Kraken", test_kraken(&client).await));
    }

    if exchange == "binance" || exchange == "all" {
        results.push(("Binance", test_binance(&client).await));
    }

    // Summary
    println!("\n{}", "=".repeat(80));
    println!("📊 SUMMARY");
    println!("{}", "=".repeat(80));

    for (name, result) in &results {
        match result {
            Ok(latency) => println!("   ✅ {} - OK ({}ms)", name, latency),
            Err(e) => println!("   ❌ {} - FAILED: {}", name, e),
        }
    }

    let all_passed = results.iter().all(|(_, r)| r.is_ok());
    if all_passed {
        println!("\n✅ All exchange APIs are reachable!");
    } else {
        println!("\n⚠️  Some exchange APIs failed connectivity test");
        std::process::exit(1);
    }
}

async fn test_kraken(client: &Client) -> Result<u128, String> {
    println!("\n📡 Testing Kraken API...");

    // Test 1: Server Time (basic connectivity)
    println!("   [1/3] Server Time...");
    let start = Instant::now();
    let response = client
        .get("https://api.kraken.com/0/public/Time")
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let time_latency = start.elapsed().as_millis();

    let time_response: KrakenTimeResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    if !time_response.error.is_empty() {
        return Err(format!("API error: {:?}", time_response.error));
    }

    if let Some(result) = time_response.result {
        println!("         ✓ Server time: {} ({}ms)", result.rfc1123, time_latency);
    }

    // Test 2: System Status
    println!("   [2/3] System Status...");
    let start = Instant::now();
    let response = client
        .get("https://api.kraken.com/0/public/SystemStatus")
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let status_latency = start.elapsed().as_millis();

    let status_response: KrakenSystemStatusResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    if !status_response.error.is_empty() {
        return Err(format!("API error: {:?}", status_response.error));
    }

    if let Some(result) = status_response.result {
        let status_emoji = if result.status == "online" { "🟢" } else { "🟡" };
        println!("         {} Status: {} ({}ms)", status_emoji, result.status, status_latency);
    }

    // Test 3: Ticker Data (market data endpoint)
    println!("   [3/3] Ticker Data (BTC/USD)...");
    let start = Instant::now();
    let response = client
        .get("https://api.kraken.com/0/public/Ticker?pair=XBTUSD")
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let ticker_latency = start.elapsed().as_millis();

    let ticker_response: KrakenTickerResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    if !ticker_response.error.is_empty() {
        return Err(format!("API error: {:?}", ticker_response.error));
    }

    if let Some(result) = ticker_response.result {
        if let Some(btc) = result.get("XXBTZUSD") {
            if let Some(price) = btc.get("c").and_then(|c| c.get(0)).and_then(|p| p.as_str()) {
                println!("         ✓ BTC/USD: ${} ({}ms)", price, ticker_latency);
            }
        }
    }

    let avg_latency = (time_latency + status_latency + ticker_latency) / 3;
    println!("   📈 Average latency: {}ms", avg_latency);

    Ok(avg_latency)
}

async fn test_binance(client: &Client) -> Result<u128, String> {
    println!("\n📡 Testing Binance API...");
    
    // Note: Binance.com may be blocked in some regions (e.g., US)
    // We try Binance.US as fallback

    // Test 1: Server Time (basic connectivity)
    println!("   [1/3] Server Time...");
    let start = Instant::now();
    
    // Try binance.com first, then binance.us
    let (time_response, base_url) = match client.get("https://api.binance.com/api/v3/time").send().await {
        Ok(resp) if resp.status().is_success() => {
            match resp.json::<BinanceTimeResponse>().await {
                Ok(parsed) => (parsed, "https://api.binance.com"),
                Err(_) => {
                    // Try Binance.US
                    let resp = client.get("https://api.binance.us/api/v3/time")
                        .send()
                        .await
                        .map_err(|e| format!("Request failed: {}", e))?;
                    let parsed = resp.json::<BinanceTimeResponse>().await
                        .map_err(|e| format!("Failed to parse response: {}", e))?;
                    (parsed, "https://api.binance.us")
                }
            }
        }
        _ => {
            // Try Binance.US
            println!("         ⚠️  Binance.com unavailable, trying Binance.US...");
            let resp = client.get("https://api.binance.us/api/v3/time")
                .send()
                .await
                .map_err(|e| format!("Request failed: {}", e))?;
            let parsed = resp.json::<BinanceTimeResponse>().await
                .map_err(|e| format!("Failed to parse response: {}", e))?;
            (parsed, "https://api.binance.us")
        }
    };

    let time_latency = start.elapsed().as_millis();

    let server_time = chrono::DateTime::from_timestamp_millis(time_response.serverTime)
        .map(|dt| dt.to_rfc2822())
        .unwrap_or_else(|| "Unknown".to_string());
    println!("         ✓ Server time: {} ({}ms)", server_time, time_latency);
    println!("         Using: {}", base_url);

    // Test 2: Exchange Info (connectivity + rate limits)
    println!("   [2/3] Exchange Info...");
    let start = Instant::now();
    let response = client
        .get(format!("{}/api/v3/exchangeInfo?symbol=BTCUSDT", base_url))
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let info_latency = start.elapsed().as_millis();

    if response.status().is_success() {
        println!("         ✓ Exchange info retrieved ({}ms)", info_latency);
    } else {
        return Err(format!("Exchange info failed: {}", response.status()));
    }

    // Test 3: Ticker Price (market data)
    println!("   [3/3] Ticker Data (BTCUSDT)...");
    let start = Instant::now();
    let response = client
        .get(format!("{}/api/v3/ticker/price?symbol=BTCUSDT", base_url))
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let ticker_latency = start.elapsed().as_millis();

    let ticker_response: BinanceTickerResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    println!(
        "         ✓ {}: ${} ({}ms)",
        ticker_response.symbol, ticker_response.price, ticker_latency
    );

    let avg_latency = (time_latency + info_latency + ticker_latency) / 3;
    println!("   📈 Average latency: {}ms", avg_latency);

    Ok(avg_latency)
}
