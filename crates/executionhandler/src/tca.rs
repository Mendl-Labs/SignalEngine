//! Transaction Cost Analysis (TCA) Module
//!
//! Institutional-grade post-trade analytics for measuring execution quality.
//! 
//! # Features
//!
//! - **Implementation Shortfall**: Measures cost vs decision price
//! - **VWAP Benchmark**: Compares execution to volume-weighted average
//! - **TWAP Benchmark**: Compares execution to time-weighted average
//! - **Arrival Price Benchmark**: Compares to price at order arrival
//! - **Slippage Analysis**: Quantifies market impact
//! - **Cost Attribution**: Breaks down costs into components
//!
//! # MiFID II Compliance
//!
//! This module supports MiFID II best execution requirements by:
//! - Tracking exchange timestamps for all executions
//! - Providing standardized benchmark comparisons
//! - Generating audit-ready reports
//!
//! # Example
//!
//! ```rust,ignore
//! use executionhandler::tca::{TcaEngine, TcaConfig, ExecutionRecord};
//!
//! let engine = TcaEngine::new(TcaConfig::default());
//!
//! // Record an execution
//! let record = ExecutionRecord {
//!     order_id: "ord-123".to_string(),
//!     symbol: "BTC-USD".to_string(),
//!     side: OrderSide::Buy,
//!     decision_price: 50000.0,
//!     arrival_price: 50010.0,
//!     executed_quantity: 1.0,
//!     executed_value: 50025.0,
//!     executed_price: 50025.0,
//!     fees: 25.0,
//!     ..Default::default()
//! };
//!
//! let analysis = engine.analyze_execution(&record)?;
//! println!("Implementation Shortfall: {} bps", analysis.implementation_shortfall_bps);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use parking_lot::RwLock;

use crate::core::types::OrderSide;

/// Configuration for TCA engine
#[derive(Debug, Clone)]
pub struct TcaConfig {
    /// Default benchmark type
    pub default_benchmark: BenchmarkType,
    /// Whether to track market data for benchmarks
    pub track_market_data: bool,
    /// VWAP calculation window (seconds)
    pub vwap_window_seconds: u64,
    /// TWAP calculation interval (seconds)
    pub twap_interval_seconds: u64,
    /// Maximum records to retain
    pub max_records: usize,
    /// Retention period (seconds)
    pub retention_seconds: u64,
    /// Maximum market data points per symbol (VWAP/TWAP vectors)
    pub max_market_data_per_symbol: usize,
    /// Prune interval - run pruning every N operations
    pub prune_interval: u64,
}

impl Default for TcaConfig {
    fn default() -> Self {
        Self {
            default_benchmark: BenchmarkType::ImplementationShortfall,
            track_market_data: true,
            vwap_window_seconds: 3600, // 1 hour
            twap_interval_seconds: 60,  // 1 minute intervals
            max_records: 100_000,
            retention_seconds: 86400 * 30, // 30 days
            max_market_data_per_symbol: 100_000, // ~1 day at 1 trade/sec
            prune_interval: 1000, // Prune every 1000 operations
        }
    }
}

/// Benchmark types for TCA
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BenchmarkType {
    /// Implementation Shortfall: Cost vs decision price
    ImplementationShortfall,
    /// VWAP: Volume-Weighted Average Price
    Vwap,
    /// TWAP: Time-Weighted Average Price
    Twap,
    /// Arrival Price: Price at order arrival
    ArrivalPrice,
    /// Close Price: End of day/session price
    ClosePrice,
    /// Opening Price: Start of day/session price
    OpenPrice,
    /// Interval VWAP: VWAP over specific interval
    IntervalVwap,
}

/// Record of an execution for TCA analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// Unique order identifier
    pub order_id: String,
    /// Trading symbol
    pub symbol: String,
    /// Exchange where executed
    pub exchange: String,
    /// Order side
    pub side: OrderSide,
    /// Requested quantity
    pub requested_quantity: f64,
    /// Executed quantity
    pub executed_quantity: f64,
    /// Total executed value (quantity * avg_price)
    pub executed_value: f64,
    /// Volume-weighted average execution price
    pub executed_price: f64,
    /// Total fees paid
    pub fees: f64,
    
    // Benchmark prices
    /// Price at decision time (when strategy decided to trade)
    pub decision_price: f64,
    /// Decision timestamp (nanoseconds)
    pub decision_timestamp_ns: u128,
    /// Price at order arrival (when order reached exchange)
    pub arrival_price: f64,
    /// Arrival timestamp (nanoseconds)
    pub arrival_timestamp_ns: u128,
    /// VWAP during execution window
    pub vwap_price: Option<f64>,
    /// TWAP during execution window
    pub twap_price: Option<f64>,
    /// Interval VWAP (e.g., 5-minute)
    pub interval_vwap_price: Option<f64>,
    /// Close price (if available)
    pub close_price: Option<f64>,
    /// Open price (if available)
    pub open_price: Option<f64>,
    
    // Timing
    /// Execution start timestamp (nanoseconds)
    pub execution_start_ns: u128,
    /// Execution end timestamp (nanoseconds)
    pub execution_end_ns: u128,
    /// Exchange timestamps from fills (MiFID II)
    pub exchange_timestamps_ns: Vec<u128>,
    
    // Individual fills
    /// Fill prices and quantities
    pub fills: Vec<(f64, f64, u128)>, // (price, quantity, timestamp)
    
    // Metadata
    /// Order type (market, limit, etc.)
    pub order_type: String,
    /// Execution algorithm used
    pub algorithm: Option<String>,
    /// Urgency level (if applicable)
    pub urgency: Option<ExecutionUrgency>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl Default for ExecutionRecord {
    fn default() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        
        Self {
            order_id: String::new(),
            symbol: String::new(),
            exchange: String::new(),
            side: OrderSide::Buy,
            requested_quantity: 0.0,
            executed_quantity: 0.0,
            executed_value: 0.0,
            executed_price: 0.0,
            fees: 0.0,
            decision_price: 0.0,
            decision_timestamp_ns: now,
            arrival_price: 0.0,
            arrival_timestamp_ns: now,
            vwap_price: None,
            twap_price: None,
            interval_vwap_price: None,
            close_price: None,
            open_price: None,
            execution_start_ns: now,
            execution_end_ns: now,
            exchange_timestamps_ns: Vec::new(),
            fills: Vec::new(),
            order_type: "market".to_string(),
            algorithm: None,
            urgency: None,
            metadata: HashMap::new(),
        }
    }
}

/// Execution urgency level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionUrgency {
    /// Passive: Minimize market impact
    Passive,
    /// Normal: Balance speed and cost
    Normal,
    /// Aggressive: Prioritize speed
    Aggressive,
    /// VeryAggressive: Execute immediately at any cost
    VeryAggressive,
}

/// Result of TCA analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcaAnalysis {
    /// Order ID analyzed
    pub order_id: String,
    /// Symbol
    pub symbol: String,
    /// Analysis timestamp
    pub analyzed_at: u128,
    
    // Implementation Shortfall
    /// Implementation shortfall in basis points
    pub implementation_shortfall_bps: f64,
    /// Implementation shortfall in absolute terms
    pub implementation_shortfall_value: f64,
    
    // Cost components (in basis points)
    /// Delay cost: Decision to arrival
    pub delay_cost_bps: f64,
    /// Market impact: Arrival to execution
    pub market_impact_bps: f64,
    /// Timing cost: Due to execution timing
    pub timing_cost_bps: f64,
    /// Spread cost: Bid-ask spread crossed
    pub spread_cost_bps: f64,
    /// Fee cost: Exchange and broker fees
    pub fee_cost_bps: f64,
    /// Opportunity cost: Unfilled portion
    pub opportunity_cost_bps: f64,
    
    // Benchmark comparisons (in basis points, positive = worse than benchmark)
    /// vs VWAP benchmark
    pub vs_vwap_bps: Option<f64>,
    /// vs TWAP benchmark
    pub vs_twap_bps: Option<f64>,
    /// vs Arrival price
    pub vs_arrival_bps: f64,
    /// vs Close price
    pub vs_close_bps: Option<f64>,
    /// vs Interval VWAP
    pub vs_interval_vwap_bps: Option<f64>,
    
    // Quality metrics
    /// Fill rate (0-1)
    pub fill_rate: f64,
    /// Execution duration (nanoseconds)
    pub execution_duration_ns: u64,
    /// Average fill size
    pub avg_fill_size: f64,
    /// Number of fills
    pub num_fills: u32,
    /// Price improvement (positive = better than expected)
    pub price_improvement_bps: f64,
    
    // Slippage analysis
    /// Total slippage in basis points
    pub total_slippage_bps: f64,
    /// Temporary impact (mean reversion)
    pub temporary_impact_bps: f64,
    /// Permanent impact
    pub permanent_impact_bps: f64,
    
    // Quality score
    /// Overall execution quality score (0-100)
    pub quality_score: f64,
    /// Quality grade (A, B, C, D, F)
    pub quality_grade: ExecutionGrade,
    
    // Breakdown by fill
    /// Per-fill analysis
    pub fill_analysis: Vec<FillAnalysis>,
}

/// Execution quality grade
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionGrade {
    /// Excellent execution (top 10%)
    A,
    /// Good execution (10-30%)
    B,
    /// Average execution (30-70%)
    C,
    /// Below average (70-90%)
    D,
    /// Poor execution (bottom 10%)
    F,
}

impl ExecutionGrade {
    fn from_score(score: f64) -> Self {
        if score >= 90.0 {
            ExecutionGrade::A
        } else if score >= 75.0 {
            ExecutionGrade::B
        } else if score >= 50.0 {
            ExecutionGrade::C
        } else if score >= 25.0 {
            ExecutionGrade::D
        } else {
            ExecutionGrade::F
        }
    }
}

/// Analysis of individual fill
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillAnalysis {
    /// Fill index
    pub index: u32,
    /// Fill price
    pub price: f64,
    /// Fill quantity
    pub quantity: f64,
    /// Fill timestamp
    pub timestamp_ns: u128,
    /// Exchange timestamp (MiFID II)
    pub exchange_timestamp_ns: Option<u128>,
    /// Slippage vs arrival price (bps)
    pub slippage_vs_arrival_bps: f64,
    /// Slippage vs decision price (bps)
    pub slippage_vs_decision_bps: f64,
    /// Price vs VWAP at fill time (bps)
    pub vs_vwap_bps: Option<f64>,
    /// Whether this fill improved the average
    pub improved_average: bool,
}

/// Aggregated TCA statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcaStatistics {
    /// Time period start
    pub period_start_ns: u128,
    /// Time period end
    pub period_end_ns: u128,
    /// Number of executions analyzed
    pub num_executions: u64,
    /// Total value traded
    pub total_value: f64,
    /// Total fees paid
    pub total_fees: f64,
    
    // Average metrics
    /// Average implementation shortfall (bps)
    pub avg_implementation_shortfall_bps: f64,
    /// Average vs VWAP (bps)
    pub avg_vs_vwap_bps: f64,
    /// Average vs TWAP (bps)
    pub avg_vs_twap_bps: f64,
    /// Average fill rate
    pub avg_fill_rate: f64,
    /// Average quality score
    pub avg_quality_score: f64,
    
    // Distribution
    /// Implementation shortfall percentiles
    pub is_percentiles: Percentiles,
    /// Quality score percentiles
    pub quality_percentiles: Percentiles,
    
    // Breakdown by symbol
    pub by_symbol: HashMap<String, SymbolTcaStats>,
    /// Breakdown by exchange
    pub by_exchange: HashMap<String, ExchangeTcaStats>,
    /// Breakdown by algorithm
    pub by_algorithm: HashMap<String, AlgorithmTcaStats>,
    
    // Grade distribution
    pub grade_distribution: HashMap<ExecutionGrade, u64>,
}

/// Percentile values
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Percentiles {
    pub p10: f64,
    pub p25: f64,
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
    pub p99: f64,
}

/// Per-symbol TCA statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SymbolTcaStats {
    pub executions: u64,
    pub total_value: f64,
    pub avg_implementation_shortfall_bps: f64,
    pub avg_quality_score: f64,
}

/// Per-exchange TCA statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExchangeTcaStats {
    pub executions: u64,
    pub total_value: f64,
    pub avg_implementation_shortfall_bps: f64,
    pub avg_latency_ns: u64,
    pub avg_quality_score: f64,
}

/// Per-algorithm TCA statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AlgorithmTcaStats {
    pub executions: u64,
    pub total_value: f64,
    pub avg_implementation_shortfall_bps: f64,
    pub avg_quality_score: f64,
    pub avg_fill_rate: f64,
}

/// TCA analysis engine
pub struct TcaEngine {
    /// Configuration
    config: TcaConfig,
    /// Stored execution records
    records: DashMap<String, ExecutionRecord>,
    /// Stored analyses
    analyses: DashMap<String, TcaAnalysis>,
    /// Market data cache for benchmarks
    market_data: MarketDataCache,
    /// Statistics
    stats: TcaEngineStats,
}

/// Market data cache for benchmark calculations
struct MarketDataCache {
    /// VWAP by symbol: (timestamp, price, volume)
    vwap_data: DashMap<String, Vec<(u128, f64, f64)>>,
    /// TWAP by symbol: (timestamp, price)
    twap_data: DashMap<String, Vec<(u128, f64)>>,
    /// Current bid/ask by symbol
    quotes: DashMap<String, (f64, f64, u128)>, // (bid, ask, timestamp)
}

impl MarketDataCache {
    fn new() -> Self {
        Self {
            vwap_data: DashMap::new(),
            twap_data: DashMap::new(),
            quotes: DashMap::new(),
        }
    }
    
    fn calculate_vwap(&self, symbol: &str, start_ns: u128, end_ns: u128) -> Option<f64> {
        let data = self.vwap_data.get(symbol)?;
        
        let mut sum_pv = 0.0;
        let mut sum_v = 0.0;
        
        for (ts, price, volume) in data.iter() {
            if *ts >= start_ns && *ts <= end_ns {
                sum_pv += price * volume;
                sum_v += volume;
            }
        }
        
        if sum_v > 0.0 {
            Some(sum_pv / sum_v)
        } else {
            None
        }
    }
    
    fn calculate_twap(&self, symbol: &str, start_ns: u128, end_ns: u128) -> Option<f64> {
        let data = self.twap_data.get(symbol)?;
        
        let prices: Vec<f64> = data.iter()
            .filter(|(ts, _)| *ts >= start_ns && *ts <= end_ns)
            .map(|(_, p)| *p)
            .collect();
        
        if prices.is_empty() {
            None
        } else {
            Some(prices.iter().sum::<f64>() / prices.len() as f64)
        }
    }
    
    /// Prune old data from market data cache
    fn prune(&self, retention_ns: u128, max_per_symbol: usize) {
        let cutoff = TcaEngine::now_ns().saturating_sub(retention_ns);
        
        // Prune VWAP data
        for mut entry in self.vwap_data.iter_mut() {
            let data = entry.value_mut();
            // Remove old entries
            data.retain(|(ts, _, _)| *ts > cutoff);
            // Truncate if over limit (keep most recent)
            if data.len() > max_per_symbol {
                let drain_count = data.len() - max_per_symbol;
                data.drain(0..drain_count);
            }
        }
        
        // Prune TWAP data
        for mut entry in self.twap_data.iter_mut() {
            let data = entry.value_mut();
            data.retain(|(ts, _)| *ts > cutoff);
            if data.len() > max_per_symbol {
                let drain_count = data.len() - max_per_symbol;
                data.drain(0..drain_count);
            }
        }
    }
}

/// Engine statistics
#[derive(Debug, Default)]
struct TcaEngineStats {
    records_analyzed: AtomicU64,
    total_value_analyzed: AtomicU64, // Scaled by 100 for precision
    operations_since_prune: AtomicU64,
}

impl TcaEngine {
    /// Create a new TCA engine
    pub fn new(config: TcaConfig) -> Self {
        Self {
            config,
            records: DashMap::new(),
            analyses: DashMap::new(),
            market_data: MarketDataCache::new(),
            stats: TcaEngineStats::default(),
        }
    }
    
    fn now_ns() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    }
    
    /// Record market data for benchmark calculations
    pub fn record_trade(&self, symbol: &str, price: f64, volume: f64) {
        let timestamp = Self::now_ns();
        
        self.market_data.vwap_data
            .entry(symbol.to_string())
            .or_insert_with(Vec::new)
            .push((timestamp, price, volume));
        
        self.market_data.twap_data
            .entry(symbol.to_string())
            .or_insert_with(Vec::new)
            .push((timestamp, price));
        
        // Periodic pruning to bound memory usage
        self.maybe_prune();
    }
    
    /// Prune old data if threshold reached
    fn maybe_prune(&self) {
        let ops = self.stats.operations_since_prune.fetch_add(1, Ordering::Relaxed);
        if ops >= self.config.prune_interval {
            self.stats.operations_since_prune.store(0, Ordering::Relaxed);
            self.prune_old_data();
        }
    }
    
    /// Prune old records and market data
    fn prune_old_data(&self) {
        let retention_ns = (self.config.retention_seconds as u128) * 1_000_000_000;
        let cutoff = Self::now_ns().saturating_sub(retention_ns);
        
        // Prune market data cache
        self.market_data.prune(retention_ns, self.config.max_market_data_per_symbol);
        
        // Prune old records if over limit
        if self.records.len() > self.config.max_records {
            // Find oldest records to remove
            let mut records_by_time: Vec<(String, u128)> = self.records.iter()
                .map(|r| (r.key().clone(), r.value().execution_end_ns))
                .collect();
            records_by_time.sort_by_key(|(_, ts)| *ts);
            
            let to_remove = self.records.len() - self.config.max_records;
            for (order_id, _) in records_by_time.into_iter().take(to_remove) {
                self.records.remove(&order_id);
                self.analyses.remove(&order_id);
            }
        }
        
        // Also prune by retention period
        let records_to_remove: Vec<String> = self.records.iter()
            .filter(|r| r.value().execution_end_ns < cutoff)
            .map(|r| r.key().clone())
            .collect();
        
        for order_id in records_to_remove {
            self.records.remove(&order_id);
            self.analyses.remove(&order_id);
        }
    }
    
    /// Record quote data
    pub fn record_quote(&self, symbol: &str, bid: f64, ask: f64) {
        let timestamp = Self::now_ns();
        self.market_data.quotes.insert(symbol.to_string(), (bid, ask, timestamp));
    }
    
    /// Analyze an execution
    pub fn analyze_execution(&self, record: &ExecutionRecord) -> Result<TcaAnalysis, TcaError> {
        if record.executed_quantity <= 0.0 {
            return Err(TcaError::InvalidRecord("Executed quantity must be positive".into()));
        }
        if record.decision_price <= 0.0 {
            return Err(TcaError::InvalidRecord("Decision price must be positive".into()));
        }
        
        let now = Self::now_ns();
        let side_multiplier = match record.side {
            OrderSide::Buy => 1.0,
            OrderSide::Sell => -1.0,
        };
        
        // Calculate Implementation Shortfall
        // IS = (Executed Price - Decision Price) / Decision Price * side_multiplier * 10000
        let is_bps = (record.executed_price - record.decision_price) 
            / record.decision_price * side_multiplier * 10000.0;
        let is_value = (record.executed_price - record.decision_price) 
            * record.executed_quantity * side_multiplier;
        
        // Calculate cost components
        let delay_cost_bps = (record.arrival_price - record.decision_price) 
            / record.decision_price * side_multiplier * 10000.0;
        
        let market_impact_bps = (record.executed_price - record.arrival_price) 
            / record.arrival_price * side_multiplier * 10000.0;
        
        let fee_cost_bps = record.fees / record.executed_value * 10000.0;
        
        let opportunity_cost_bps = if record.requested_quantity > record.executed_quantity {
            let unfilled = record.requested_quantity - record.executed_quantity;
            let unfilled_ratio = unfilled / record.requested_quantity;
            // Assume unfilled would have been at decision price
            unfilled_ratio * is_bps.abs() * 0.5 // 50% haircut for opportunity cost
        } else {
            0.0
        };
        
        // Benchmark comparisons
        let vs_arrival_bps = (record.executed_price - record.arrival_price) 
            / record.arrival_price * side_multiplier * 10000.0;
        
        let vs_vwap_bps = record.vwap_price.map(|vwap| {
            (record.executed_price - vwap) / vwap * side_multiplier * 10000.0
        });
        
        let vs_twap_bps = record.twap_price.map(|twap| {
            (record.executed_price - twap) / twap * side_multiplier * 10000.0
        });
        
        let vs_close_bps = record.close_price.map(|close| {
            (record.executed_price - close) / close * side_multiplier * 10000.0
        });
        
        let vs_interval_vwap_bps = record.interval_vwap_price.map(|ivwap| {
            (record.executed_price - ivwap) / ivwap * side_multiplier * 10000.0
        });
        
        // Calculate quality metrics
        let fill_rate = record.executed_quantity / record.requested_quantity;
        let execution_duration_ns = (record.execution_end_ns - record.execution_start_ns) as u64;
        let num_fills = record.fills.len() as u32;
        let avg_fill_size = if num_fills > 0 {
            record.executed_quantity / num_fills as f64
        } else {
            record.executed_quantity
        };
        
        // Calculate price improvement (vs worst expected price)
        // For buys, worst is ask; for sells, worst is bid
        let price_improvement_bps = if let Some((bid, ask, _)) = self.market_data.quotes.get(&record.symbol).map(|q| *q) {
            let worst_price = match record.side {
                OrderSide::Buy => ask,
                OrderSide::Sell => bid,
            };
            (worst_price - record.executed_price) / worst_price * side_multiplier * 10000.0
        } else {
            0.0
        };
        
        // Slippage analysis
        let total_slippage_bps = is_bps;
        let temporary_impact_bps = market_impact_bps * 0.6; // Assume 60% is temporary
        let permanent_impact_bps = market_impact_bps * 0.4; // Assume 40% is permanent
        
        // Calculate spread cost
        let spread_cost_bps = if let Some((bid, ask, _)) = self.market_data.quotes.get(&record.symbol).map(|q| *q) {
            let mid = (bid + ask) / 2.0;
            let half_spread = (ask - bid) / 2.0 / mid * 10000.0;
            half_spread // We typically cross half the spread
        } else {
            0.0
        };
        
        // Timing cost (execution VWAP vs interval VWAP)
        let timing_cost_bps = vs_interval_vwap_bps.unwrap_or(0.0);
        
        // Calculate quality score
        let quality_score = self.calculate_quality_score(
            is_bps, fill_rate, execution_duration_ns, price_improvement_bps
        );
        let quality_grade = ExecutionGrade::from_score(quality_score);
        
        // Analyze individual fills
        let fill_analysis: Vec<FillAnalysis> = record.fills.iter().enumerate().map(|(i, (price, qty, ts))| {
            let slippage_vs_arrival = (*price - record.arrival_price) / record.arrival_price * side_multiplier * 10000.0;
            let slippage_vs_decision = (*price - record.decision_price) / record.decision_price * side_multiplier * 10000.0;
            
            // Check if this fill improved our average
            let improved = if i == 0 {
                true
            } else {
                let prev_fills: Vec<_> = record.fills.iter().take(i).collect();
                let prev_avg = prev_fills.iter().map(|(p, q, _)| p * q).sum::<f64>() 
                    / prev_fills.iter().map(|(_, q, _)| q).sum::<f64>();
                match record.side {
                    OrderSide::Buy => *price < prev_avg,
                    OrderSide::Sell => *price > prev_avg,
                }
            };
            
            FillAnalysis {
                index: i as u32,
                price: *price,
                quantity: *qty,
                timestamp_ns: *ts,
                exchange_timestamp_ns: record.exchange_timestamps_ns.get(i).copied(),
                slippage_vs_arrival_bps: slippage_vs_arrival,
                slippage_vs_decision_bps: slippage_vs_decision,
                vs_vwap_bps: None, // Would need real-time VWAP at fill time
                improved_average: improved,
            }
        }).collect();
        
        let analysis = TcaAnalysis {
            order_id: record.order_id.clone(),
            symbol: record.symbol.clone(),
            analyzed_at: now,
            implementation_shortfall_bps: is_bps,
            implementation_shortfall_value: is_value,
            delay_cost_bps,
            market_impact_bps,
            timing_cost_bps,
            spread_cost_bps,
            fee_cost_bps,
            opportunity_cost_bps,
            vs_vwap_bps,
            vs_twap_bps,
            vs_arrival_bps,
            vs_close_bps,
            vs_interval_vwap_bps,
            fill_rate,
            execution_duration_ns,
            avg_fill_size,
            num_fills,
            price_improvement_bps,
            total_slippage_bps,
            temporary_impact_bps,
            permanent_impact_bps,
            quality_score,
            quality_grade,
            fill_analysis,
        };
        
        // Store record and analysis
        self.records.insert(record.order_id.clone(), record.clone());
        self.analyses.insert(record.order_id.clone(), analysis.clone());
        
        self.stats.records_analyzed.fetch_add(1, Ordering::Relaxed);
        self.stats.total_value_analyzed.fetch_add(
            (record.executed_value * 100.0) as u64, 
            Ordering::Relaxed
        );
        
        Ok(analysis)
    }
    
    /// Calculate execution quality score (0-100)
    fn calculate_quality_score(
        &self,
        implementation_shortfall_bps: f64,
        fill_rate: f64,
        execution_duration_ns: u64,
        price_improvement_bps: f64,
    ) -> f64 {
        // Implementation shortfall score (40% weight)
        // 0 bps = 100, 10 bps = 75, 50 bps = 25, 100+ bps = 0
        let is_score = if implementation_shortfall_bps <= 0.0 {
            100.0
        } else if implementation_shortfall_bps < 10.0 {
            100.0 - implementation_shortfall_bps * 2.5
        } else if implementation_shortfall_bps < 50.0 {
            75.0 - (implementation_shortfall_bps - 10.0) * 1.25
        } else {
            (25.0 - (implementation_shortfall_bps - 50.0) * 0.5).max(0.0)
        };
        
        // Fill rate score (30% weight)
        let fill_score = fill_rate * 100.0;
        
        // Speed score (15% weight)
        // Sub-second = 100, 1s = 75, 10s = 50, 60s = 25, 5min+ = 0
        let duration_secs = execution_duration_ns as f64 / 1_000_000_000.0;
        let speed_score = if duration_secs < 1.0 {
            100.0
        } else if duration_secs < 10.0 {
            75.0 + 25.0 * (1.0 - duration_secs / 10.0)
        } else if duration_secs < 60.0 {
            50.0 + 25.0 * (1.0 - duration_secs / 60.0)
        } else if duration_secs < 300.0 {
            25.0 * (1.0 - duration_secs / 300.0)
        } else {
            0.0
        };
        
        // Price improvement score (15% weight)
        let improvement_score = if price_improvement_bps > 0.0 {
            (50.0 + price_improvement_bps * 5.0).min(100.0)
        } else {
            (50.0 + price_improvement_bps).max(0.0)
        };
        
        // Weighted average
        is_score * 0.4 + fill_score * 0.3 + speed_score * 0.15 + improvement_score * 0.15
    }
    
    /// Get aggregated statistics for a time period
    pub fn get_statistics(&self, start_ns: u128, end_ns: u128) -> TcaStatistics {
        let mut stats = TcaStatistics {
            period_start_ns: start_ns,
            period_end_ns: end_ns,
            num_executions: 0,
            total_value: 0.0,
            total_fees: 0.0,
            avg_implementation_shortfall_bps: 0.0,
            avg_vs_vwap_bps: 0.0,
            avg_vs_twap_bps: 0.0,
            avg_fill_rate: 0.0,
            avg_quality_score: 0.0,
            is_percentiles: Percentiles::default(),
            quality_percentiles: Percentiles::default(),
            by_symbol: HashMap::new(),
            by_exchange: HashMap::new(),
            by_algorithm: HashMap::new(),
            grade_distribution: HashMap::new(),
        };
        
        let mut is_values: Vec<f64> = Vec::new();
        let mut quality_scores: Vec<f64> = Vec::new();
        let mut vwap_values: Vec<f64> = Vec::new();
        let mut twap_values: Vec<f64> = Vec::new();
        
        for entry in self.analyses.iter() {
            let analysis = entry.value();
            let record = match self.records.get(&analysis.order_id) {
                Some(r) => r,
                None => continue,
            };
            
            // Check time range
            if record.execution_end_ns < start_ns || record.execution_start_ns > end_ns {
                continue;
            }
            
            stats.num_executions += 1;
            stats.total_value += record.executed_value;
            stats.total_fees += record.fees;
            
            is_values.push(analysis.implementation_shortfall_bps);
            quality_scores.push(analysis.quality_score);
            
            if let Some(vwap) = analysis.vs_vwap_bps {
                vwap_values.push(vwap);
            }
            if let Some(twap) = analysis.vs_twap_bps {
                twap_values.push(twap);
            }
            
            // Update by_symbol
            let symbol_stats = stats.by_symbol
                .entry(record.symbol.clone())
                .or_insert_with(SymbolTcaStats::default);
            symbol_stats.executions += 1;
            symbol_stats.total_value += record.executed_value;
            
            // Update by_exchange
            let exchange_stats = stats.by_exchange
                .entry(record.exchange.clone())
                .or_insert_with(ExchangeTcaStats::default);
            exchange_stats.executions += 1;
            exchange_stats.total_value += record.executed_value;
            
            // Update by_algorithm
            if let Some(ref algo) = record.algorithm {
                let algo_stats = stats.by_algorithm
                    .entry(algo.clone())
                    .or_insert_with(AlgorithmTcaStats::default);
                algo_stats.executions += 1;
                algo_stats.total_value += record.executed_value;
                algo_stats.avg_fill_rate = (algo_stats.avg_fill_rate * (algo_stats.executions - 1) as f64 
                    + analysis.fill_rate) / algo_stats.executions as f64;
            }
            
            // Update grade distribution
            *stats.grade_distribution.entry(analysis.quality_grade).or_insert(0) += 1;
            
            stats.avg_fill_rate += analysis.fill_rate;
        }
        
        if stats.num_executions > 0 {
            let n = stats.num_executions as f64;
            stats.avg_implementation_shortfall_bps = is_values.iter().sum::<f64>() / n;
            stats.avg_quality_score = quality_scores.iter().sum::<f64>() / n;
            stats.avg_fill_rate /= n;
            
            if !vwap_values.is_empty() {
                stats.avg_vs_vwap_bps = vwap_values.iter().sum::<f64>() / vwap_values.len() as f64;
            }
            if !twap_values.is_empty() {
                stats.avg_vs_twap_bps = twap_values.iter().sum::<f64>() / twap_values.len() as f64;
            }
            
            // Calculate percentiles
            stats.is_percentiles = Self::calculate_percentiles(&mut is_values);
            stats.quality_percentiles = Self::calculate_percentiles(&mut quality_scores);
            
            // Update per-symbol averages
            for (_, symbol_stats) in stats.by_symbol.iter_mut() {
                if symbol_stats.executions > 0 {
                    // This would need actual accumulation, simplified here
                }
            }
        }
        
        stats
    }
    
    fn calculate_percentiles(values: &mut Vec<f64>) -> Percentiles {
        if values.is_empty() {
            return Percentiles::default();
        }
        
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = values.len();
        
        Percentiles {
            p10: values[(n as f64 * 0.10) as usize],
            p25: values[(n as f64 * 0.25) as usize],
            p50: values[(n as f64 * 0.50) as usize],
            p75: values[(n as f64 * 0.75) as usize],
            p90: values[(n as f64 * 0.90).min((n - 1) as f64) as usize],
            p99: values[(n as f64 * 0.99).min((n - 1) as f64) as usize],
        }
    }
    
    /// Get analysis by order ID
    pub fn get_analysis(&self, order_id: &str) -> Option<TcaAnalysis> {
        self.analyses.get(order_id).map(|a| a.clone())
    }
    
    /// Get all analyses for a symbol
    pub fn get_analyses_by_symbol(&self, symbol: &str) -> Vec<TcaAnalysis> {
        self.analyses.iter()
            .filter(|a| a.symbol == symbol)
            .map(|a| a.clone())
            .collect()
    }
    
    /// Export analysis to MiFID II format (simplified)
    pub fn export_mifid_report(&self, order_id: &str) -> Option<MifidReport> {
        let analysis = self.analyses.get(order_id)?;
        let record = self.records.get(order_id)?;
        
        Some(MifidReport {
            order_id: order_id.to_string(),
            symbol: analysis.symbol.clone(),
            side: format!("{:?}", record.side),
            requested_quantity: record.requested_quantity,
            executed_quantity: record.executed_quantity,
            execution_price: record.executed_price,
            total_consideration: record.executed_value,
            total_fees: record.fees,
            decision_timestamp_ns: record.decision_timestamp_ns,
            arrival_timestamp_ns: record.arrival_timestamp_ns,
            first_fill_timestamp_ns: record.fills.first().map(|(_, _, t)| *t),
            last_fill_timestamp_ns: record.fills.last().map(|(_, _, t)| *t),
            exchange_timestamps_ns: record.exchange_timestamps_ns.clone(),
            venue: record.exchange.clone(),
            order_type: record.order_type.clone(),
            implementation_shortfall_bps: analysis.implementation_shortfall_bps,
            vs_vwap_bps: analysis.vs_vwap_bps,
            vs_arrival_bps: analysis.vs_arrival_bps,
            quality_grade: format!("{:?}", analysis.quality_grade),
            num_fills: analysis.num_fills,
        })
    }
}

impl Default for TcaEngine {
    fn default() -> Self {
        Self::new(TcaConfig::default())
    }
}

/// MiFID II compliant execution report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MifidReport {
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub requested_quantity: f64,
    pub executed_quantity: f64,
    pub execution_price: f64,
    pub total_consideration: f64,
    pub total_fees: f64,
    pub decision_timestamp_ns: u128,
    pub arrival_timestamp_ns: u128,
    pub first_fill_timestamp_ns: Option<u128>,
    pub last_fill_timestamp_ns: Option<u128>,
    pub exchange_timestamps_ns: Vec<u128>,
    pub venue: String,
    pub order_type: String,
    pub implementation_shortfall_bps: f64,
    pub vs_vwap_bps: Option<f64>,
    pub vs_arrival_bps: f64,
    pub quality_grade: String,
    pub num_fills: u32,
}

/// TCA errors
#[derive(Debug, Clone)]
pub enum TcaError {
    /// Invalid execution record
    InvalidRecord(String),
    /// Record not found
    NotFound(String),
    /// Calculation error
    CalculationError(String),
}

impl std::fmt::Display for TcaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TcaError::InvalidRecord(msg) => write!(f, "Invalid record: {}", msg),
            TcaError::NotFound(id) => write!(f, "Record not found: {}", id),
            TcaError::CalculationError(msg) => write!(f, "Calculation error: {}", msg),
        }
    }
}

impl std::error::Error for TcaError {}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_tca_analysis() {
        let engine = TcaEngine::new(TcaConfig::default());
        
        let record = ExecutionRecord {
            order_id: "ord-1".to_string(),
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            side: OrderSide::Buy,
            requested_quantity: 1.0,
            executed_quantity: 1.0,
            executed_value: 50050.0,
            executed_price: 50050.0,
            fees: 25.0,
            decision_price: 50000.0,
            arrival_price: 50010.0,
            fills: vec![(50050.0, 1.0, 0)],
            ..Default::default()
        };
        
        let analysis = engine.analyze_execution(&record).unwrap();
        
        // Implementation shortfall: (50050 - 50000) / 50000 * 10000 = 10 bps
        assert!((analysis.implementation_shortfall_bps - 10.0).abs() < 0.1);
        assert_eq!(analysis.fill_rate, 1.0);
        assert!(analysis.quality_score > 0.0);
    }
    
    #[test]
    fn test_partial_fill_analysis() {
        let engine = TcaEngine::new(TcaConfig::default());
        
        let record = ExecutionRecord {
            order_id: "ord-2".to_string(),
            symbol: "ETH-USD".to_string(),
            exchange: "kraken".to_string(),
            side: OrderSide::Buy,
            requested_quantity: 10.0,
            executed_quantity: 7.5,
            executed_value: 7500.0,
            executed_price: 1000.0,
            fees: 5.0,
            decision_price: 995.0,
            arrival_price: 997.0,
            fills: vec![
                (998.0, 3.0, 1000),
                (1001.0, 2.5, 2000),
                (1001.5, 2.0, 3000),
            ],
            ..Default::default()
        };
        
        let analysis = engine.analyze_execution(&record).unwrap();
        
        assert_eq!(analysis.fill_rate, 0.75);
        assert!(analysis.opportunity_cost_bps > 0.0);
        assert_eq!(analysis.num_fills, 3);
    }
    
    #[test]
    fn test_sell_side_analysis() {
        let engine = TcaEngine::new(TcaConfig::default());
        
        let record = ExecutionRecord {
            order_id: "ord-3".to_string(),
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            side: OrderSide::Sell,
            requested_quantity: 1.0,
            executed_quantity: 1.0,
            executed_value: 49950.0,
            executed_price: 49950.0,
            fees: 25.0,
            decision_price: 50000.0,
            arrival_price: 49990.0,
            fills: vec![(49950.0, 1.0, 0)],
            ..Default::default()
        };
        
        let analysis = engine.analyze_execution(&record).unwrap();
        
        // For sell, lower price = higher cost
        // IS = (50000 - 49950) / 50000 * 10000 = 10 bps
        assert!((analysis.implementation_shortfall_bps - 10.0).abs() < 0.1);
    }
    
    #[test]
    fn test_quality_grades() {
        assert_eq!(ExecutionGrade::from_score(95.0), ExecutionGrade::A);
        assert_eq!(ExecutionGrade::from_score(80.0), ExecutionGrade::B);
        assert_eq!(ExecutionGrade::from_score(60.0), ExecutionGrade::C);
        assert_eq!(ExecutionGrade::from_score(30.0), ExecutionGrade::D);
        assert_eq!(ExecutionGrade::from_score(10.0), ExecutionGrade::F);
    }
    
    #[test]
    fn test_mifid_report_generation() {
        let engine = TcaEngine::new(TcaConfig::default());
        
        let record = ExecutionRecord {
            order_id: "ord-mifid".to_string(),
            symbol: "ETH-USD".to_string(),
            exchange: "kraken".to_string(),
            side: OrderSide::Buy,
            requested_quantity: 5.0,
            executed_quantity: 5.0,
            executed_value: 5000.0,
            executed_price: 1000.0,
            fees: 2.5,
            decision_price: 998.0,
            arrival_price: 999.0,
            exchange_timestamps_ns: vec![1234567890, 1234567900],
            fills: vec![
                (1000.0, 3.0, 1234567890),
                (1000.0, 2.0, 1234567900),
            ],
            ..Default::default()
        };
        
        engine.analyze_execution(&record).unwrap();
        
        let report = engine.export_mifid_report("ord-mifid").unwrap();
        assert_eq!(report.order_id, "ord-mifid");
        assert_eq!(report.exchange_timestamps_ns.len(), 2);
        assert_eq!(report.num_fills, 2);
    }
    
    #[test]
    fn test_statistics_aggregation() {
        let engine = TcaEngine::new(TcaConfig::default());
        
        // Add multiple executions
        for i in 0..5 {
            let record = ExecutionRecord {
                order_id: format!("ord-{}", i),
                symbol: "BTC-USD".to_string(),
                exchange: "kraken".to_string(),
                side: OrderSide::Buy,
                requested_quantity: 1.0,
                executed_quantity: 1.0,
                executed_value: 50000.0 + i as f64 * 100.0,
                executed_price: 50000.0 + i as f64 * 100.0,
                fees: 25.0,
                decision_price: 50000.0,
                arrival_price: 50000.0,
                fills: vec![(50000.0 + i as f64 * 100.0, 1.0, 0)],
                ..Default::default()
            };
            engine.analyze_execution(&record).unwrap();
        }
        
        let stats = engine.get_statistics(0, u128::MAX);
        
        assert_eq!(stats.num_executions, 5);
        assert!(stats.total_value > 0.0);
        assert!(stats.by_symbol.contains_key("BTC-USD"));
    }
    
    #[test]
    fn test_invalid_record_rejected() {
        let engine = TcaEngine::new(TcaConfig::default());
        
        let record = ExecutionRecord {
            order_id: "ord-invalid".to_string(),
            executed_quantity: 0.0, // Invalid
            ..Default::default()
        };
        
        assert!(engine.analyze_execution(&record).is_err());
    }
}
