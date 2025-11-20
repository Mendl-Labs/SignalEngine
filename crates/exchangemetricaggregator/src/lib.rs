use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

/// Exchange-specific metrics for smart order routing
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExchangeMetrics {
    // Exchange identification
    pub exchange_name: String,
    pub symbol: String,
    
    // Latency metrics
    pub avg_response_time_ms: f64,
    pub min_response_time_ms: f64,
    pub max_response_time_ms: f64,
    pub p95_response_time_ms: f64,
    pub p99_response_time_ms: f64,
    
    // Liquidity metrics
    pub total_bid_liquidity: f64,
    pub total_ask_liquidity: f64,
    pub total_liquidity: f64,
    pub best_bid_size: f64,
    pub best_ask_size: f64,
    pub market_depth_usd: f64,
    pub avg_order_size: f64,
    
    // Price metrics
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid_price: f64,
    pub spread_bps: f64,
    pub price_impact_1pc: f64,  // Price impact for 1% of daily volume
    pub price_impact_5pc: f64,  // Price impact for 5% of daily volume
    
    // Volume and activity metrics
    pub daily_volume_usd: f64,
    pub hourly_volume_usd: f64,
    pub trade_count_1h: u64,
    pub trade_count_24h: u64,
    pub avg_trade_size_usd: f64,
    pub volume_weighted_avg_price: f64,
    
    // Market quality metrics
    pub orderbook_imbalance: f64,
    pub liquidity_ratio: f64,  // bid_liquidity / ask_liquidity
    pub market_efficiency_score: f64,
    pub volatility_score: f64,
    
    // Execution quality metrics
    pub fill_rate: f64,        // Percentage of orders filled
    pub partial_fill_rate: f64,
    pub avg_slippage_bps: f64,
    pub execution_shortfall_bps: f64,
    
    // Exchange reliability metrics
    pub uptime_percentage: f64,
    pub api_error_rate: f64,
    pub order_rejection_rate: f64,
    pub connectivity_score: f64,
    
    // Fee structure
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub effective_fee_bps: f64,  // Weighted by order flow
    
    // Timestamps
    pub last_updated: u64,
    pub measurement_window_ms: u64,
}

/// Aggregated cross-exchange metrics for routing decisions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CrossExchangeMetrics {
    pub symbol: String,
    
    // Best prices across all exchanges
    pub global_best_bid: f64,
    pub global_best_ask: f64,
    pub global_best_bid_exchange: String,
    pub global_best_ask_exchange: String,
    pub global_spread_bps: f64,
    
    // Arbitrage opportunities
    pub max_arbitrage_spread_bps: f64,
    pub arbitrage_opportunity_count: u32,
    pub avg_arbitrage_profit_bps: f64,
    
    // Liquidity aggregation
    pub total_bid_liquidity_all: f64,
    pub total_ask_liquidity_all: f64,
    pub weighted_avg_spread_bps: f64,
    pub effective_spread_for_size: HashMap<String, f64>, // size_key -> effective spread
    
    // Exchange rankings
    pub best_execution_exchange: String,
    pub lowest_latency_exchange: String,
    pub highest_liquidity_exchange: String,
    pub most_reliable_exchange: String,
    
    // Price correlation metrics
    pub price_correlation_matrix: HashMap<(String, String), f64>,
    pub lead_lag_relationships: HashMap<String, i32>, // milliseconds lead/lag
    
    // Market fragmentation
    pub liquidity_fragmentation_index: f64,
    pub price_fragmentation_index: f64,
    pub exchange_concentration_ratio: f64,
    
    pub last_updated: u64,
}

/// Smart order routing metrics and recommendations
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SmartRoutingMetrics {
    pub symbol: String,
    
    // Routing recommendations by order size
    pub routing_recommendations: HashMap<String, f64>, // exchange -> allocation percentage
    pub optimal_split_sizes: Vec<(String, f64)>,       // (exchange, size)
    
    // Expected execution costs
    pub expected_slippage_bps: f64,
    pub expected_fees_bps: f64,
    pub expected_total_cost_bps: f64,
    pub expected_fill_time_ms: f64,
    
    // Risk metrics
    pub execution_risk_score: f64,
    pub liquidity_risk_score: f64,
    pub latency_risk_score: f64,
    pub counterparty_risk_score: f64,
    
    // Historical performance
    pub historical_fill_rate: f64,
    pub historical_avg_slippage_bps: f64,
    pub historical_success_rate: f64,
    
    // Market conditions assessment
    pub market_condition_score: f64,  // 0-100, higher is better for execution
    pub volatility_regime: String,    // "low", "normal", "high", "extreme"
    pub liquidity_regime: String,     // "dry", "normal", "abundant"
    
    pub calculation_timestamp: u64,
    pub recommendation_confidence: f64, // 0.0 to 1.0
}

/// Performance tracking for executed routes
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RoutingPerformance {
    pub route_id: String,
    pub symbol: String,
    pub timestamp: u64,
    
    // Execution details
    pub total_quantity: f64,
    pub filled_quantity: f64,
    pub avg_execution_price: f64,
    pub execution_time_ms: u64,
    
    // Cost analysis
    pub actual_slippage_bps: f64,
    pub actual_fees_bps: f64,
    pub actual_total_cost_bps: f64,
    pub implementation_shortfall_bps: f64,
    
    // vs. Benchmark comparisons
    pub vs_vwap_bps: f64,
    pub vs_twap_bps: f64,
    pub vs_arrival_price_bps: f64,
    
    // Exchange breakdown
    pub exchange_allocations: HashMap<String, f64>, // exchange -> filled quantity
    pub exchange_performance: HashMap<String, f64>, // exchange -> cost in bps
    
    // Quality metrics
    pub fill_rate: f64,
    pub time_to_completion_ms: u64,
    pub price_improvement_bps: f64,
}

/// Main exchange metrics aggregator
pub struct ExchangeMetricsAggregator {
    exchange_metrics: RwLock<HashMap<(String, String), ExchangeMetrics>>, // (exchange, symbol) -> metrics
    cross_exchange_metrics: RwLock<HashMap<String, CrossExchangeMetrics>>, // symbol -> cross-exchange metrics
    routing_metrics: RwLock<HashMap<String, SmartRoutingMetrics>>,         // symbol -> routing metrics
    performance_history: RwLock<Vec<RoutingPerformance>>,
    
    // Configuration
    _update_interval_ms: u64,
    max_history_entries: usize,
    
    // Performance tracking
    last_update: RwLock<HashMap<String, Instant>>,
    calculation_times: RwLock<HashMap<String, Duration>>,
}

impl ExchangeMetricsAggregator {
    /// Create a new metrics aggregator
    pub fn new(update_interval_ms: u64, max_history_entries: usize) -> Self {
        Self {
            exchange_metrics: RwLock::new(HashMap::new()),
            cross_exchange_metrics: RwLock::new(HashMap::new()),
            routing_metrics: RwLock::new(HashMap::new()),
            performance_history: RwLock::new(Vec::new()),
            _update_interval_ms: update_interval_ms,
            max_history_entries,
            last_update: RwLock::new(HashMap::new()),
            calculation_times: RwLock::new(HashMap::new()),
        }
    }
    
    /// Update exchange-specific metrics
    pub fn update_exchange_metrics(
        &self,
        exchange: &str,
        symbol: &str,
        metrics: ExchangeMetrics,
    ) -> Result<(), String> {
        let key = (exchange.to_string(), symbol.to_string());
        
        let mut exchange_metrics = self.exchange_metrics.write()
            .map_err(|_| "Failed to acquire write lock on exchange_metrics")?;
        
        exchange_metrics.insert(key, metrics);
        
        // Update last update timestamp
        let mut last_update = self.last_update.write()
            .map_err(|_| "Failed to acquire write lock on last_update")?;
        last_update.insert(format!("{}:{}", exchange, symbol), Instant::now());
        
        Ok(())
    }
    
    /// Calculate cross-exchange metrics for a symbol
    pub fn calculate_cross_exchange_metrics(&self, symbol: &str) -> Result<CrossExchangeMetrics, String> {
        let start = Instant::now();
        
        let exchange_metrics = self.exchange_metrics.read()
            .map_err(|_| "Failed to acquire read lock on exchange_metrics")?;
        
        // Get all metrics for this symbol across exchanges
        let symbol_metrics: Vec<&ExchangeMetrics> = exchange_metrics
            .iter()
            .filter(|((_, s), _)| s == symbol)
            .map(|(_, metrics)| metrics)
            .collect();
        
        if symbol_metrics.is_empty() {
            return Err(format!("No metrics found for symbol: {}", symbol));
        }
        
        let mut cross_metrics = CrossExchangeMetrics {
            symbol: symbol.to_string(),
            ..Default::default()
        };
        
        // Find global best prices
        let mut best_bid = 0.0;
        let mut best_ask = f64::MAX;
        let mut best_bid_exchange = String::new();
        let mut best_ask_exchange = String::new();
        
        for metrics in &symbol_metrics {
            if metrics.best_bid > best_bid {
                best_bid = metrics.best_bid;
                best_bid_exchange = metrics.exchange_name.clone();
            }
            if metrics.best_ask < best_ask && metrics.best_ask > 0.0 {
                best_ask = metrics.best_ask;
                best_ask_exchange = metrics.exchange_name.clone();
            }
        }
        
        cross_metrics.global_best_bid = best_bid;
        cross_metrics.global_best_ask = best_ask;
        cross_metrics.global_best_bid_exchange = best_bid_exchange;
        cross_metrics.global_best_ask_exchange = best_ask_exchange;
        
        // Calculate global spread
        if best_bid > 0.0 && best_ask > 0.0 && best_ask > best_bid {
            let mid_price = (best_bid + best_ask) / 2.0;
            cross_metrics.global_spread_bps = ((best_ask - best_bid) / mid_price) * 10000.0;
        }
        
        // Calculate arbitrage opportunities
        let mut max_spread: f64 = 0.0;
        let mut arb_count = 0;
        let mut total_arb_profit: f64 = 0.0;
        
        for i in 0..symbol_metrics.len() {
            for j in (i+1)..symbol_metrics.len() {
                let m1 = symbol_metrics[i];
                let m2 = symbol_metrics[j];
                
                // Check for arbitrage: buy on one exchange, sell on another
                let spread1 = m2.best_bid - m1.best_ask; // Buy on m1, sell on m2
                let spread2 = m1.best_bid - m2.best_ask; // Buy on m2, sell on m1
                
                if spread1 > 0.0 {
                    let profit_bps = (spread1 / m1.best_ask) * 10000.0;
                    max_spread = max_spread.max(profit_bps);
                    total_arb_profit += profit_bps;
                    arb_count += 1;
                }
                
                if spread2 > 0.0 {
                    let profit_bps = (spread2 / m2.best_ask) * 10000.0;
                    max_spread = max_spread.max(profit_bps);
                    total_arb_profit += profit_bps;
                    arb_count += 1;
                }
            }
        }
        
        cross_metrics.max_arbitrage_spread_bps = max_spread;
        cross_metrics.arbitrage_opportunity_count = arb_count;
        cross_metrics.avg_arbitrage_profit_bps = if arb_count > 0 {
            total_arb_profit / arb_count as f64
        } else {
            0.0
        };
        
        // Aggregate liquidity
        cross_metrics.total_bid_liquidity_all = symbol_metrics.iter()
            .map(|m| m.total_bid_liquidity)
            .sum();
        cross_metrics.total_ask_liquidity_all = symbol_metrics.iter()
            .map(|m| m.total_ask_liquidity)
            .sum();
        
        // Calculate weighted average spread
        let mut weighted_spread_sum = 0.0;
        let mut total_volume = 0.0;
        
        for metrics in &symbol_metrics {
            if metrics.daily_volume_usd > 0.0 {
                weighted_spread_sum += metrics.spread_bps * metrics.daily_volume_usd;
                total_volume += metrics.daily_volume_usd;
            }
        }
        
        cross_metrics.weighted_avg_spread_bps = if total_volume > 0.0 {
            weighted_spread_sum / total_volume
        } else {
            0.0
        };
        
        // Find best exchanges by different criteria
        cross_metrics.best_execution_exchange = symbol_metrics.iter()
            .min_by(|a, b| a.avg_slippage_bps.partial_cmp(&b.avg_slippage_bps).unwrap())
            .map(|m| m.exchange_name.clone())
            .unwrap_or_default();
        
        cross_metrics.lowest_latency_exchange = symbol_metrics.iter()
            .min_by(|a, b| a.avg_response_time_ms.partial_cmp(&b.avg_response_time_ms).unwrap())
            .map(|m| m.exchange_name.clone())
            .unwrap_or_default();
        
        cross_metrics.highest_liquidity_exchange = symbol_metrics.iter()
            .max_by(|a, b| a.total_liquidity.partial_cmp(&b.total_liquidity).unwrap())
            .map(|m| m.exchange_name.clone())
            .unwrap_or_default();
        
        cross_metrics.most_reliable_exchange = symbol_metrics.iter()
            .max_by(|a, b| a.connectivity_score.partial_cmp(&b.connectivity_score).unwrap())
            .map(|m| m.exchange_name.clone())
            .unwrap_or_default();
        
        // Calculate fragmentation indices
        let total_liquidity = cross_metrics.total_bid_liquidity_all + cross_metrics.total_ask_liquidity_all;
        if total_liquidity > 0.0 {
            // Herfindahl-Hirschman Index for liquidity concentration
            let hhi: f64 = symbol_metrics.iter()
                .map(|m| {
                    let share = m.total_liquidity / total_liquidity;
                    share * share
                })
                .sum();
            
            cross_metrics.liquidity_fragmentation_index = 1.0 - hhi;
            cross_metrics.exchange_concentration_ratio = hhi;
        }
        
        cross_metrics.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        // Record calculation time
        let calc_time = start.elapsed();
        let mut calc_times = self.calculation_times.write()
            .map_err(|_| "Failed to acquire write lock on calculation_times")?;
        calc_times.insert(format!("cross_exchange:{}", symbol), calc_time);
        
        Ok(cross_metrics)
    }
    
    /// Calculate smart routing recommendations
    pub fn calculate_routing_metrics(
        &self,
        symbol: &str,
        order_size: f64,
        side: &str, // "buy" or "sell"
    ) -> Result<SmartRoutingMetrics, String> {
        let start = Instant::now();
        
        let exchange_metrics = self.exchange_metrics.read()
            .map_err(|_| "Failed to acquire read lock on exchange_metrics")?;
        
        // Get all metrics for this symbol
        let symbol_metrics: Vec<&ExchangeMetrics> = exchange_metrics
            .iter()
            .filter(|((_, s), _)| s == symbol)
            .map(|(_, metrics)| metrics)
            .collect();
        
        if symbol_metrics.is_empty() {
            return Err(format!("No metrics found for symbol: {}", symbol));
        }
        
        let mut routing_metrics = SmartRoutingMetrics {
            symbol: symbol.to_string(),
            ..Default::default()
        };
        
        // Calculate routing recommendations based on multiple factors
        let mut exchange_scores: HashMap<String, f64> = HashMap::new();
        let mut total_score = 0.0;
        
        for metrics in &symbol_metrics {
            let mut score = 0.0;
            
            // Liquidity score (40% weight)
            let relevant_liquidity = if side == "buy" {
                metrics.total_ask_liquidity
            } else {
                metrics.total_bid_liquidity
            };
            
            let liquidity_score = (relevant_liquidity / order_size).min(10.0) * 4.0;
            score += liquidity_score;
            
            // Cost score (30% weight) - lower is better
            let cost_score = (100.0 - metrics.avg_slippage_bps.min(100.0)) * 3.0 / 100.0;
            score += cost_score;
            
            // Reliability score (20% weight)
            let reliability_score = metrics.connectivity_score * 2.0 / 100.0;
            score += reliability_score;
            
            // Speed score (10% weight) - lower latency is better
            let speed_score = (1000.0 - metrics.avg_response_time_ms.min(1000.0)) * 1.0 / 1000.0;
            score += speed_score;
            
            exchange_scores.insert(metrics.exchange_name.clone(), score);
            total_score += score;
        }
        
        // Normalize scores to percentages
        for (exchange, score) in &exchange_scores {
            let allocation = if total_score > 0.0 {
                score / total_score
            } else {
                1.0 / symbol_metrics.len() as f64
            };
            routing_metrics.routing_recommendations.insert(exchange.clone(), allocation);
        }
        
        // Calculate optimal split sizes
        for (exchange, allocation) in &routing_metrics.routing_recommendations {
            let split_size = order_size * allocation;
            if split_size > 0.01 { // Only include meaningful splits
                routing_metrics.optimal_split_sizes.push((exchange.clone(), split_size));
            }
        }
        
        // Calculate expected costs
        let mut weighted_slippage = 0.0;
        let mut weighted_fees = 0.0;
        let mut weighted_fill_time = 0.0;
        
        for metrics in &symbol_metrics {
            if let Some(&allocation) = routing_metrics.routing_recommendations.get(&metrics.exchange_name) {
                weighted_slippage += metrics.avg_slippage_bps * allocation;
                weighted_fees += metrics.effective_fee_bps * allocation;
                weighted_fill_time += metrics.avg_response_time_ms * allocation;
            }
        }
        
        routing_metrics.expected_slippage_bps = weighted_slippage;
        routing_metrics.expected_fees_bps = weighted_fees;
        routing_metrics.expected_total_cost_bps = weighted_slippage + weighted_fees;
        routing_metrics.expected_fill_time_ms = weighted_fill_time;
        
        // Calculate risk scores (0-100, lower is better)
        routing_metrics.execution_risk_score = weighted_slippage;
        routing_metrics.liquidity_risk_score = 100.0 - (symbol_metrics.iter()
            .map(|m| m.total_liquidity)
            .sum::<f64>() / order_size).min(100.0);
        routing_metrics.latency_risk_score = weighted_fill_time / 10.0; // Scale to 0-100
        
        // Market condition assessment
        let avg_volatility: f64 = symbol_metrics.iter()
            .map(|m| m.volatility_score)
            .sum::<f64>() / symbol_metrics.len() as f64;
        
        routing_metrics.volatility_regime = match avg_volatility {
            v if v < 20.0 => "low",
            v if v < 50.0 => "normal", 
            v if v < 80.0 => "high",
            _ => "extreme"
        }.to_string();
        
        let avg_liquidity: f64 = symbol_metrics.iter()
            .map(|m| m.total_liquidity)
            .sum::<f64>() / symbol_metrics.len() as f64;
        
        routing_metrics.liquidity_regime = match avg_liquidity / order_size {
            ratio if ratio > 100.0 => "abundant",
            ratio if ratio > 10.0 => "normal",
            _ => "dry"
        }.to_string();
        
        routing_metrics.market_condition_score = (100.0 - avg_volatility).max(0.0);
        routing_metrics.recommendation_confidence = if total_score > 0.0 { 0.8 } else { 0.3 };
        
        routing_metrics.calculation_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        // Record calculation time
        let calc_time = start.elapsed();
        let mut calc_times = self.calculation_times.write()
            .map_err(|_| "Failed to acquire write lock on calculation_times")?;
        calc_times.insert(format!("routing:{}", symbol), calc_time);
        
        Ok(routing_metrics)
    }
    
    /// Record routing performance for analysis
    pub fn record_routing_performance(&self, performance: RoutingPerformance) -> Result<(), String> {
        let mut history = self.performance_history.write()
            .map_err(|_| "Failed to acquire write lock on performance_history")?;
        
        history.push(performance);
        
        // Maintain max history size
        if history.len() > self.max_history_entries {
            history.remove(0);
        }
        
        Ok(())
    }
    
    /// Get exchange metrics for a specific exchange and symbol
    pub fn get_exchange_metrics(&self, exchange: &str, symbol: &str) -> Result<ExchangeMetrics, String> {
        let key = (exchange.to_string(), symbol.to_string());
        let metrics = self.exchange_metrics.read()
            .map_err(|_| "Failed to acquire read lock on exchange_metrics")?;
        
        metrics.get(&key)
            .cloned()
            .ok_or_else(|| format!("No metrics found for {}:{}", exchange, symbol))
    }
    
    /// Get cross-exchange metrics for a symbol
    pub fn get_cross_exchange_metrics(&self, symbol: &str) -> Result<CrossExchangeMetrics, String> {
        let metrics = self.cross_exchange_metrics.read()
            .map_err(|_| "Failed to acquire read lock on cross_exchange_metrics")?;
        
        metrics.get(symbol)
            .cloned()
            .ok_or_else(|| format!("No cross-exchange metrics found for symbol: {}", symbol))
    }
    
    /// Get routing metrics for a symbol
    pub fn get_routing_metrics(&self, symbol: &str) -> Result<SmartRoutingMetrics, String> {
        let metrics = self.routing_metrics.read()
            .map_err(|_| "Failed to acquire read lock on routing_metrics")?;
        
        metrics.get(symbol)
            .cloned()
            .ok_or_else(|| format!("No routing metrics found for symbol: {}", symbol))
    }
    
    /// Get performance history
    pub fn get_performance_history(&self) -> Result<Vec<RoutingPerformance>, String> {
        let history = self.performance_history.read()
            .map_err(|_| "Failed to acquire read lock on performance_history")?;
        
        Ok(history.clone())
    }
    
    /// Get calculation performance statistics
    pub fn get_calculation_stats(&self) -> Result<HashMap<String, Duration>, String> {
        let calc_times = self.calculation_times.read()
            .map_err(|_| "Failed to acquire read lock on calculation_times")?;
        
        Ok(calc_times.clone())
    }
    
    /// Update all metrics for a symbol (convenience method)
    pub fn update_all_metrics(&self, symbol: &str) -> Result<(), String> {
        // Calculate cross-exchange metrics
        let cross_metrics = self.calculate_cross_exchange_metrics(symbol)?;
        {
            let mut cross_exchange_metrics = self.cross_exchange_metrics.write()
                .map_err(|_| "Failed to acquire write lock on cross_exchange_metrics")?;
            cross_exchange_metrics.insert(symbol.to_string(), cross_metrics);
        }
        
        // Calculate routing metrics for different order sizes (example sizes)
        let test_sizes = vec![1000.0, 10000.0, 100000.0];
        for &size in &test_sizes {
            for side in &["buy", "sell"] {
                let routing_metrics = self.calculate_routing_metrics(symbol, size, side)?;
                let key = format!("{}:{}:{}", symbol, size, side);
                
                let mut routing_metrics_map = self.routing_metrics.write()
                    .map_err(|_| "Failed to acquire write lock on routing_metrics")?;
                routing_metrics_map.insert(key, routing_metrics);
            }
        }
        
        Ok(())
    }
    
    /// Clean old data based on timestamps
    pub fn cleanup_old_data(&self, max_age_ms: u64) -> Result<usize, String> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let cutoff_time = current_time.saturating_sub(max_age_ms);
        
        // Clean exchange metrics
        let mut exchange_metrics = self.exchange_metrics.write()
            .map_err(|_| "Failed to acquire write lock on exchange_metrics")?;
        
        let mut removed_count = 0;
        exchange_metrics.retain(|_, metrics| {
            let keep = metrics.last_updated > cutoff_time;
            if !keep {
                removed_count += 1;
            }
            keep
        });
        
        // Clean performance history
        let mut history = self.performance_history.write()
            .map_err(|_| "Failed to acquire write lock on performance_history")?;
        
        let original_len = history.len();
        history.retain(|perf| perf.timestamp > cutoff_time);
        removed_count += original_len - history.len();
        
        Ok(removed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregator_creation() {
        let aggregator = ExchangeMetricsAggregator::new(1000, 10000);
        assert_eq!(aggregator._update_interval_ms, 1000);
        assert_eq!(aggregator.max_history_entries, 10000);
    }

    #[test]
    fn test_exchange_metrics_update() {
        let aggregator = ExchangeMetricsAggregator::new(1000, 10000);
        
        let metrics = ExchangeMetrics {
            exchange_name: "Binance".to_string(),
            symbol: "BTC/USD".to_string(),
            best_bid: 50000.0,
            best_ask: 50001.0,
            total_bid_liquidity: 100.0,
            total_ask_liquidity: 150.0,
            avg_response_time_ms: 50.0,
            spread_bps: 20.0,
            last_updated: 1234567890,
            ..Default::default()
        };
        
        let result = aggregator.update_exchange_metrics("Binance", "BTC/USD", metrics);
        assert!(result.is_ok());
        
        let retrieved = aggregator.get_exchange_metrics("Binance", "BTC/USD");
        assert!(retrieved.is_ok());
        assert_eq!(retrieved.unwrap().best_bid, 50000.0);
    }

    #[test]
    fn test_cross_exchange_metrics_calculation() {
        let aggregator = ExchangeMetricsAggregator::new(1000, 10000);
        
        // Add metrics for multiple exchanges
        let binance_metrics = ExchangeMetrics {
            exchange_name: "Binance".to_string(),
            symbol: "BTC/USD".to_string(),
            best_bid: 50000.0,
            best_ask: 50001.0,
            total_liquidity: 1000.0,
            daily_volume_usd: 1000000.0,
            spread_bps: 20.0,
            ..Default::default()
        };
        
        let coinbase_metrics = ExchangeMetrics {
            exchange_name: "Coinbase".to_string(),
            symbol: "BTC/USD".to_string(),
            best_bid: 49999.0,
            best_ask: 50002.0,
            total_liquidity: 800.0,
            daily_volume_usd: 800000.0,
            spread_bps: 60.0,
            ..Default::default()
        };
        
        aggregator.update_exchange_metrics("Binance", "BTC/USD", binance_metrics).unwrap();
        aggregator.update_exchange_metrics("Coinbase", "BTC/USD", coinbase_metrics).unwrap();
        
        let cross_metrics = aggregator.calculate_cross_exchange_metrics("BTC/USD");
        assert!(cross_metrics.is_ok());
        
        let metrics = cross_metrics.unwrap();
        assert_eq!(metrics.global_best_bid, 50000.0); // Higher bid from Binance
        assert_eq!(metrics.global_best_ask, 50001.0); // Lower ask from Binance
        assert_eq!(metrics.global_best_bid_exchange, "Binance");
    }

    #[test]
    fn test_routing_metrics_calculation() {
        let aggregator = ExchangeMetricsAggregator::new(1000, 10000);
        
        let metrics = ExchangeMetrics {
            exchange_name: "Binance".to_string(),
            symbol: "BTC/USD".to_string(),
            best_bid: 50000.0,
            best_ask: 50001.0,
            total_ask_liquidity: 100.0,
            avg_slippage_bps: 10.0,
            connectivity_score: 95.0,
            avg_response_time_ms: 50.0,
            effective_fee_bps: 5.0,
            ..Default::default()
        };
        
        aggregator.update_exchange_metrics("Binance", "BTC/USD", metrics).unwrap();
        
        let routing_metrics = aggregator.calculate_routing_metrics("BTC/USD", 10.0, "buy");
        assert!(routing_metrics.is_ok());
        
        let metrics = routing_metrics.unwrap();
        assert!(metrics.routing_recommendations.contains_key("Binance"));
        assert!(metrics.expected_total_cost_bps > 0.0);
    }

    #[test]
    fn test_performance_recording() {
        let aggregator = ExchangeMetricsAggregator::new(1000, 10000);
        
        let performance = RoutingPerformance {
            route_id: "test_route_001".to_string(),
            symbol: "BTC/USD".to_string(),
            timestamp: 1234567890,
            total_quantity: 10.0,
            filled_quantity: 10.0,
            avg_execution_price: 50000.0,
            actual_slippage_bps: 5.0,
            fill_rate: 1.0,
            ..Default::default()
        };
        
        let result = aggregator.record_routing_performance(performance);
        assert!(result.is_ok());
        
        let history = aggregator.get_performance_history();
        assert!(history.is_ok());
        assert_eq!(history.unwrap().len(), 1);
    }

    #[test]
    fn test_cleanup_old_data() {
        let aggregator = ExchangeMetricsAggregator::new(1000, 10000);
        
        // Add old metrics
        let old_metrics = ExchangeMetrics {
            exchange_name: "Binance".to_string(),
            symbol: "BTC/USD".to_string(),
            last_updated: 1000, // Very old timestamp
            ..Default::default()
        };
        
        aggregator.update_exchange_metrics("Binance", "BTC/USD", old_metrics).unwrap();
        
        // Clean up data older than 5 seconds (5000ms)
        let max_age_ms = 5000;
        
        let removed_count = aggregator.cleanup_old_data(max_age_ms);
        assert!(removed_count.is_ok());
        assert!(removed_count.unwrap() > 0);
    }

    #[test]
    fn test_arbitrage_detection() {
        let aggregator = ExchangeMetricsAggregator::new(1000, 10000);
        
        // Create arbitrage opportunity: Binance bid > Coinbase ask
        let binance_metrics = ExchangeMetrics {
            exchange_name: "Binance".to_string(),
            symbol: "BTC/USD".to_string(),
            best_bid: 50002.0, // Higher bid
            best_ask: 50003.0,
            ..Default::default()
        };
        
        let coinbase_metrics = ExchangeMetrics {
            exchange_name: "Coinbase".to_string(),
            symbol: "BTC/USD".to_string(),
            best_bid: 49999.0,
            best_ask: 50000.0, // Lower ask
            ..Default::default()
        };
        
        aggregator.update_exchange_metrics("Binance", "BTC/USD", binance_metrics).unwrap();
        aggregator.update_exchange_metrics("Coinbase", "BTC/USD", coinbase_metrics).unwrap();
        
        let cross_metrics = aggregator.calculate_cross_exchange_metrics("BTC/USD").unwrap();
        assert!(cross_metrics.arbitrage_opportunity_count > 0);
        assert!(cross_metrics.max_arbitrage_spread_bps > 0.0);
    }
}
