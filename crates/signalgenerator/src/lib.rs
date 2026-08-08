//! Shared market-data wire type used by `datahandler`.
//!
//! This crate used to also define a set of standalone signal-generation
//! strategies (momentum/spread-arb/volume-spike, moving-average crossover,
//! mean-reversion). They were never wired into any live deployment path --
//! the real strategy path runs through `strategyhandler`'s
//! `PythonBridgeStrategy`/`PairPythonBridgeStrategy`, executing user/AI-
//! authored Python via `pythonbridge-worker` -- and were removed as dead
//! code (nothing outside this crate's own tests ever constructed them).
//! `MarketData` is the one type from this crate other crates still use.

use serde::{Deserialize, Serialize};

/// Market data structure optimized for minimal copying
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    /// The real exchange this tick actually originated from (e.g. "kraken",
    /// "coinbase"), as tagged by DataEngine's per-exchange subscription.
    /// Needed so a multi-venue deployment's tick-matching loop can tell which
    /// of its configured venues a given tick belongs to, instead of every
    /// tick being silently mislabeled with the deployment's single stored
    /// exchange regardless of where it actually came from.
    pub exchange: String,
    pub price: f64,
    pub volume: f64,
    pub timestamp: u64, // RDTSC timestamp for nanosecond precision
    pub bid: f64,
    pub ask: f64,
    pub spread: f64,
    pub last_trade_size: f64,
    pub book_pressure: f64, // Bid/Ask volume ratio
}
