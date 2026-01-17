//! Multi-Exchange Latency Optimization
//!
//! Provides latency-aware routing and execution optimization across multiple exchanges.
//! Features:
//! - Real-time latency tracking per exchange
//! - Settlement time awareness
//! - Latency-adjusted execution timing
//! - Cross-exchange arbitrage timing
//! - Network condition adaptation

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use dashmap::DashMap;

/// Configuration for latency optimization
#[derive(Debug, Clone)]
pub struct LatencyOptimizerConfig {
    /// Number of samples for latency calculation
    pub sample_window: usize,
    /// Weight for recent samples (exponential moving average)
    pub ema_alpha: f64,
    /// Maximum acceptable latency before exchange is marked degraded (ms)
    pub degraded_threshold_ms: u64,
    /// Maximum acceptable latency before exchange is marked unavailable (ms)
    pub unavailable_threshold_ms: u64,
    /// Settlement buffer time (ms) - how early to send orders
    pub settlement_buffer_ms: u64,
    /// Enable predictive latency adjustment
    pub predictive_mode: bool,
    /// Latency spike detection threshold (multiplier of avg)
    pub spike_threshold_multiplier: f64,
    /// Recovery check interval (ms)
    pub recovery_check_interval_ms: u64,
}

impl Default for LatencyOptimizerConfig {
    fn default() -> Self {
        Self {
            sample_window: 100,
            ema_alpha: 0.2,
            degraded_threshold_ms: 100,
            unavailable_threshold_ms: 500,
            settlement_buffer_ms: 10,
            predictive_mode: true,
            spike_threshold_multiplier: 3.0,
            recovery_check_interval_ms: 1000,
        }
    }
}

/// Exchange health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeHealth {
    /// Normal operation
    Healthy,
    /// Elevated latency but functional
    Degraded,
    /// High latency, should avoid if possible
    Slow,
    /// Unresponsive or erroring
    Unavailable,
    /// Recovering from issues
    Recovering,
}

/// Exchange-specific latency statistics
#[derive(Debug)]
pub struct ExchangeLatencyStats {
    /// Exchange identifier
    pub exchange: String,
    /// Current health status
    pub health: ExchangeHealth,
    /// Average round-trip latency (ns)
    pub avg_latency_ns: u64,
    /// Minimum observed latency (ns)
    pub min_latency_ns: u64,
    /// Maximum observed latency (ns)
    pub max_latency_ns: u64,
    /// P50 latency (ns)
    pub p50_latency_ns: u64,
    /// P95 latency (ns)
    pub p95_latency_ns: u64,
    /// P99 latency (ns)
    pub p99_latency_ns: u64,
    /// Jitter (standard deviation in ns)
    pub jitter_ns: u64,
    /// Current trend (positive = increasing latency)
    pub trend: f64,
    /// Settlement time (ms)
    pub settlement_time_ms: u64,
    /// Sample count
    pub sample_count: u64,
    /// Last update time
    pub last_updated: Instant,
    /// Consecutive errors
    pub consecutive_errors: u32,
}

/// Internal latency sample storage
#[derive(Debug)]
struct LatencySamples {
    samples: Vec<u64>,
    ema_latency: f64,
    min: u64,
    max: u64,
    sum: u64,
    sum_squared: u64,
    count: u64,
    last_sample_time: Instant,
    recent_trend: Vec<u64>, // Last N samples for trend calculation
}

impl Default for LatencySamples {
    fn default() -> Self {
        Self {
            samples: Vec::with_capacity(100),
            ema_latency: 0.0,
            min: u64::MAX,
            max: 0,
            sum: 0,
            sum_squared: 0,
            count: 0,
            last_sample_time: Instant::now(),
            recent_trend: Vec::with_capacity(10),
        }
    }
}

/// Exchange settlement configuration
#[derive(Debug, Clone)]
pub struct SettlementConfig {
    /// Expected settlement time (ms)
    pub settlement_time_ms: u64,
    /// Settlement variance (ms)
    pub settlement_variance_ms: u64,
    /// Is instant settlement (e.g., DEX atomic swaps)
    pub instant_settlement: bool,
    /// Requires confirmation
    pub requires_confirmation: bool,
    /// Number of confirmations if applicable
    pub confirmations_required: u32,
}

impl Default for SettlementConfig {
    fn default() -> Self {
        Self {
            settlement_time_ms: 1000, // 1 second default
            settlement_variance_ms: 200,
            instant_settlement: false,
            requires_confirmation: false,
            confirmations_required: 1,
        }
    }
}

/// Result of routing decision
#[derive(Debug, Clone)]
pub struct RoutingDecision {
    /// Selected exchange
    pub exchange: String,
    /// Reason for selection
    pub reason: RoutingReason,
    /// Estimated latency (ns)
    pub estimated_latency_ns: u64,
    /// Estimated total execution time including settlement (ms)
    pub estimated_total_time_ms: u64,
    /// Confidence in estimate (0.0-1.0)
    pub confidence: f64,
    /// Alternative exchanges (sorted by preference)
    pub alternatives: Vec<String>,
    /// Timing adjustment recommendation (ms to delay/advance)
    pub timing_adjustment_ms: i64,
}

/// Reason for exchange selection
#[derive(Debug, Clone)]
pub enum RoutingReason {
    /// Lowest latency
    LowestLatency,
    /// Best overall score (latency + reliability)
    BestScore,
    /// User/strategy specified
    Specified,
    /// Only healthy option
    OnlyHealthy,
    /// Settlement time optimization
    SettlementOptimized,
}

/// Multi-exchange latency optimizer
pub struct LatencyOptimizer {
    config: LatencyOptimizerConfig,
    /// Per-exchange latency tracking
    exchange_latencies: DashMap<String, LatencySamples>,
    /// Per-exchange health status
    exchange_health: DashMap<String, ExchangeHealth>,
    /// Per-exchange settlement config
    settlement_configs: DashMap<String, SettlementConfig>,
    /// Global system load indicator
    system_load: AtomicU64,
    /// Is optimizer active
    active: AtomicBool,
}

impl LatencyOptimizer {
    /// Create a new latency optimizer
    pub fn new(config: LatencyOptimizerConfig) -> Self {
        Self {
            config,
            exchange_latencies: DashMap::new(),
            exchange_health: DashMap::new(),
            settlement_configs: DashMap::new(),
            system_load: AtomicU64::new(0),
            active: AtomicBool::new(true),
        }
    }

    /// Register an exchange with settlement configuration
    pub fn register_exchange(&self, exchange: &str, settlement: SettlementConfig) {
        self.settlement_configs.insert(exchange.to_string(), settlement);
        self.exchange_health.insert(exchange.to_string(), ExchangeHealth::Healthy);
        self.exchange_latencies.insert(exchange.to_string(), LatencySamples::default());
    }

    /// Record a latency sample
    pub fn record_latency(&self, exchange: &str, latency_ns: u64) {
        let mut entry = self.exchange_latencies
            .entry(exchange.to_string())
            .or_insert_with(LatencySamples::default);

        let samples = entry.value_mut();
        
        // Update statistics
        samples.count += 1;
        samples.sum += latency_ns;
        samples.sum_squared += latency_ns * latency_ns;
        samples.min = samples.min.min(latency_ns);
        samples.max = samples.max.max(latency_ns);

        // EMA update
        if samples.ema_latency == 0.0 {
            samples.ema_latency = latency_ns as f64;
        } else {
            samples.ema_latency = self.config.ema_alpha * latency_ns as f64 
                + (1.0 - self.config.ema_alpha) * samples.ema_latency;
        }

        // Track samples for percentile calculation
        samples.samples.push(latency_ns);
        if samples.samples.len() > self.config.sample_window {
            samples.samples.remove(0);
        }

        // Track recent for trend
        samples.recent_trend.push(latency_ns);
        if samples.recent_trend.len() > 10 {
            samples.recent_trend.remove(0);
        }

        samples.last_sample_time = Instant::now();

        // Update health status
        self.update_health(exchange, latency_ns, false);
    }

    /// Record an error (timeout or failure)
    pub fn record_error(&self, exchange: &str) {
        if let Some(mut entry) = self.exchange_health.get_mut(exchange) {
            *entry = match *entry {
                ExchangeHealth::Healthy => ExchangeHealth::Degraded,
                ExchangeHealth::Degraded => ExchangeHealth::Slow,
                ExchangeHealth::Slow => ExchangeHealth::Unavailable,
                ExchangeHealth::Recovering => ExchangeHealth::Degraded,
                ExchangeHealth::Unavailable => ExchangeHealth::Unavailable,
            };
        }

        // Record a very high latency to affect stats
        self.record_latency(exchange, self.config.unavailable_threshold_ms * 1_000_000);
    }

    /// Get latency statistics for an exchange
    pub fn get_stats(&self, exchange: &str) -> Option<ExchangeLatencyStats> {
        let samples = self.exchange_latencies.get(exchange)?;
        let health = self.exchange_health.get(exchange)
            .map(|h| *h)
            .unwrap_or(ExchangeHealth::Healthy);
        let settlement = self.settlement_configs.get(exchange)
            .map(|s| s.settlement_time_ms)
            .unwrap_or(1000);

        let s = samples.value();
        
        // Calculate percentiles
        let mut sorted = s.samples.clone();
        sorted.sort();
        
        let p50 = sorted.get(sorted.len() / 2).copied().unwrap_or(0);
        let p95 = sorted.get(sorted.len() * 95 / 100).copied().unwrap_or(0);
        let p99 = sorted.get(sorted.len() * 99 / 100).copied().unwrap_or(0);

        // Calculate jitter (std deviation)
        let avg = if s.count > 0 { s.sum / s.count } else { 0 };
        let variance = if s.count > 1 {
            (s.sum_squared / s.count) - (avg * avg)
        } else {
            0
        };
        let jitter = (variance as f64).sqrt() as u64;

        // Calculate trend
        let trend = self.calculate_trend(&s.recent_trend);

        Some(ExchangeLatencyStats {
            exchange: exchange.to_string(),
            health,
            avg_latency_ns: s.ema_latency as u64,
            min_latency_ns: if s.min == u64::MAX { 0 } else { s.min },
            max_latency_ns: s.max,
            p50_latency_ns: p50,
            p95_latency_ns: p95,
            p99_latency_ns: p99,
            jitter_ns: jitter,
            trend,
            settlement_time_ms: settlement,
            sample_count: s.count,
            last_updated: s.last_sample_time,
            consecutive_errors: 0,
        })
    }

    /// Get optimal exchange for execution
    pub fn get_optimal_exchange(&self, candidates: &[&str]) -> Option<RoutingDecision> {
        if candidates.is_empty() {
            return None;
        }

        let mut scored: Vec<(String, f64, u64, ExchangeHealth)> = candidates
            .iter()
            .filter_map(|&ex| {
                let health = self.exchange_health.get(ex)
                    .map(|h| *h)
                    .unwrap_or(ExchangeHealth::Healthy);
                
                // Skip unavailable exchanges
                if health == ExchangeHealth::Unavailable {
                    return None;
                }

                let latency = self.exchange_latencies.get(ex)
                    .map(|s| s.ema_latency as u64)
                    .unwrap_or(100_000_000); // Default 100ms

                let score = self.calculate_score(ex, latency, health);
                
                Some((ex.to_string(), score, latency, health))
            })
            .collect();

        if scored.is_empty() {
            return None;
        }

        // Sort by score (higher is better)
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let best = &scored[0];
        let settlement = self.settlement_configs.get(&best.0)
            .map(|s| s.settlement_time_ms)
            .unwrap_or(1000);

        let alternatives: Vec<String> = scored.iter()
            .skip(1)
            .take(3)
            .map(|(ex, _, _, _)| ex.clone())
            .collect();

        let timing_adj = self.calculate_timing_adjustment(&best.0);

        let reason = if best.3 != ExchangeHealth::Healthy {
            RoutingReason::OnlyHealthy
        } else if scored.len() == 1 {
            RoutingReason::OnlyHealthy
        } else {
            RoutingReason::BestScore
        };

        let confidence = self.calculate_confidence(&best.0);

        Some(RoutingDecision {
            exchange: best.0.clone(),
            reason,
            estimated_latency_ns: best.2,
            estimated_total_time_ms: (best.2 / 1_000_000) + settlement,
            confidence,
            alternatives,
            timing_adjustment_ms: timing_adj,
        })
    }

    /// Get latency-adjusted execution time
    /// Returns when to send order to hit a specific execution time
    pub fn get_adjusted_send_time(
        &self,
        exchange: &str,
        target_execution_time: Instant,
    ) -> Instant {
        let latency_ns = self.exchange_latencies.get(exchange)
            .map(|s| s.ema_latency as u64)
            .unwrap_or(50_000_000); // Default 50ms

        let buffer_ns = self.config.settlement_buffer_ms * 1_000_000;
        let total_advance_ns = latency_ns + buffer_ns;

        target_execution_time - Duration::from_nanos(total_advance_ns)
    }

    /// Calculate optimal timing for cross-exchange arbitrage
    pub fn calculate_arb_timing(
        &self,
        buy_exchange: &str,
        sell_exchange: &str,
    ) -> ArbTimingResult {
        let buy_latency = self.exchange_latencies.get(buy_exchange)
            .map(|s| s.ema_latency as u64)
            .unwrap_or(50_000_000);
        
        let sell_latency = self.exchange_latencies.get(sell_exchange)
            .map(|s| s.ema_latency as u64)
            .unwrap_or(50_000_000);

        let buy_settlement = self.settlement_configs.get(buy_exchange)
            .map(|s| s.settlement_time_ms)
            .unwrap_or(1000);
        
        let sell_settlement = self.settlement_configs.get(sell_exchange)
            .map(|s| s.settlement_time_ms)
            .unwrap_or(1000);

        // Determine which leg to send first
        let (first_leg, second_leg, delay_ns) = if buy_latency > sell_latency {
            // Send buy first
            ("buy", "sell", buy_latency - sell_latency)
        } else {
            // Send sell first
            ("sell", "buy", sell_latency - buy_latency)
        };

        ArbTimingResult {
            first_leg: first_leg.to_string(),
            second_leg: second_leg.to_string(),
            delay_between_legs_ns: delay_ns,
            total_expected_time_ms: (buy_latency.max(sell_latency) / 1_000_000) 
                + buy_settlement.max(sell_settlement),
            buy_latency_ns: buy_latency,
            sell_latency_ns: sell_latency,
            synchronized_execution: delay_ns < 5_000_000, // Within 5ms
        }
    }

    /// Update system load indicator
    pub fn update_system_load(&self, load_percent: u64) {
        self.system_load.store(load_percent, Ordering::Relaxed);
    }

    /// Get all exchange health statuses
    pub fn get_all_health(&self) -> HashMap<String, ExchangeHealth> {
        self.exchange_health
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// Reset statistics for an exchange
    pub fn reset_stats(&self, exchange: &str) {
        if let Some(mut entry) = self.exchange_latencies.get_mut(exchange) {
            *entry = LatencySamples::default();
        }
        if let Some(mut entry) = self.exchange_health.get_mut(exchange) {
            *entry = ExchangeHealth::Healthy;
        }
    }

    // Private helper methods

    fn update_health(&self, exchange: &str, latency_ns: u64, is_error: bool) {
        let latency_ms = latency_ns / 1_000_000;
        
        let new_health = if is_error {
            ExchangeHealth::Unavailable
        } else if latency_ms > self.config.unavailable_threshold_ms {
            ExchangeHealth::Slow
        } else if latency_ms > self.config.degraded_threshold_ms {
            ExchangeHealth::Degraded
        } else {
            // Check for recovery
            let current = self.exchange_health.get(exchange)
                .map(|h| *h)
                .unwrap_or(ExchangeHealth::Healthy);
            
            match current {
                ExchangeHealth::Unavailable | ExchangeHealth::Slow => ExchangeHealth::Recovering,
                ExchangeHealth::Recovering | ExchangeHealth::Degraded => ExchangeHealth::Healthy,
                _ => ExchangeHealth::Healthy,
            }
        };

        self.exchange_health.insert(exchange.to_string(), new_health);
    }

    fn calculate_score(&self, exchange: &str, latency_ns: u64, health: ExchangeHealth) -> f64 {
        // Base score from latency (lower is better, convert to 0-100 scale)
        let latency_ms = latency_ns as f64 / 1_000_000.0;
        let latency_score = (1.0 / (1.0 + latency_ms / 100.0)) * 50.0;

        // Health score
        let health_score = match health {
            ExchangeHealth::Healthy => 50.0,
            ExchangeHealth::Recovering => 35.0,
            ExchangeHealth::Degraded => 25.0,
            ExchangeHealth::Slow => 10.0,
            ExchangeHealth::Unavailable => 0.0,
        };

        // Stability score (based on jitter)
        let stability_score = self.exchange_latencies.get(exchange)
            .map(|s| {
                let jitter_pct = if s.ema_latency > 0.0 {
                    let variance = if s.count > 1 {
                        let avg = s.sum / s.count;
                        ((s.sum_squared / s.count) - (avg * avg)) as f64
                    } else {
                        0.0
                    };
                    variance.sqrt() / s.ema_latency * 100.0
                } else {
                    50.0
                };
                (1.0 / (1.0 + jitter_pct / 100.0)) * 20.0
            })
            .unwrap_or(10.0);

        latency_score + health_score + stability_score
    }

    fn calculate_trend(&self, recent: &[u64]) -> f64 {
        if recent.len() < 3 {
            return 0.0;
        }

        // Simple linear regression slope
        let n = recent.len() as f64;
        let sum_x: f64 = (0..recent.len()).map(|i| i as f64).sum();
        let sum_y: f64 = recent.iter().map(|&v| v as f64).sum();
        let sum_xy: f64 = recent.iter().enumerate()
            .map(|(i, &v)| i as f64 * v as f64)
            .sum();
        let sum_xx: f64 = (0..recent.len()).map(|i| (i * i) as f64).sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);
        
        // Normalize to percentage change per sample
        let avg = sum_y / n;
        if avg > 0.0 {
            slope / avg * 100.0
        } else {
            0.0
        }
    }

    fn calculate_timing_adjustment(&self, exchange: &str) -> i64 {
        // Get current latency trend
        let trend = self.exchange_latencies.get(exchange)
            .map(|s| self.calculate_trend(&s.recent_trend))
            .unwrap_or(0.0);

        // If latency is increasing, send earlier
        // If latency is decreasing, can send slightly later
        if self.config.predictive_mode {
            (-trend * 10.0) as i64 // ms adjustment
        } else {
            0
        }
    }

    fn calculate_confidence(&self, exchange: &str) -> f64 {
        let sample_count = self.exchange_latencies.get(exchange)
            .map(|s| s.count)
            .unwrap_or(0);

        let sample_factor = (sample_count as f64 / self.config.sample_window as f64).min(1.0);
        
        let health_factor = match self.exchange_health.get(exchange).map(|h| *h) {
            Some(ExchangeHealth::Healthy) => 1.0,
            Some(ExchangeHealth::Recovering) => 0.7,
            Some(ExchangeHealth::Degraded) => 0.5,
            Some(ExchangeHealth::Slow) => 0.3,
            Some(ExchangeHealth::Unavailable) | None => 0.1,
        };

        sample_factor * 0.6 + health_factor * 0.4
    }
}

/// Result of arbitrage timing calculation
#[derive(Debug, Clone)]
pub struct ArbTimingResult {
    /// Which leg to execute first ("buy" or "sell")
    pub first_leg: String,
    /// Which leg to execute second
    pub second_leg: String,
    /// Delay between sending orders (ns)
    pub delay_between_legs_ns: u64,
    /// Total expected execution time (ms)
    pub total_expected_time_ms: u64,
    /// Expected buy latency (ns)
    pub buy_latency_ns: u64,
    /// Expected sell latency (ns)
    pub sell_latency_ns: u64,
    /// Are legs synchronized (within 5ms)?
    pub synchronized_execution: bool,
}

/// Global latency optimizer instance
use once_cell::sync::Lazy;

pub static LATENCY_OPTIMIZER: Lazy<LatencyOptimizer> = Lazy::new(|| {
    LatencyOptimizer::new(LatencyOptimizerConfig::default())
});

/// Convenience functions
pub fn record_exchange_latency(exchange: &str, latency_ns: u64) {
    LATENCY_OPTIMIZER.record_latency(exchange, latency_ns);
}

pub fn get_optimal_route(exchanges: &[&str]) -> Option<RoutingDecision> {
    LATENCY_OPTIMIZER.get_optimal_exchange(exchanges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_recording() {
        let optimizer = LatencyOptimizer::new(LatencyOptimizerConfig::default());
        optimizer.register_exchange("test-ex", SettlementConfig::default());

        // Record some samples
        for latency_ms in [10, 12, 11, 13, 10, 11, 12] {
            optimizer.record_latency("test-ex", latency_ms * 1_000_000);
        }

        let stats = optimizer.get_stats("test-ex").unwrap();
        
        assert!(stats.avg_latency_ns > 10_000_000);
        assert!(stats.avg_latency_ns < 15_000_000);
        assert_eq!(stats.health, ExchangeHealth::Healthy);
    }

    #[test]
    fn test_health_degradation() {
        let config = LatencyOptimizerConfig {
            degraded_threshold_ms: 50,
            unavailable_threshold_ms: 200,
            ..Default::default()
        };
        let optimizer = LatencyOptimizer::new(config);
        optimizer.register_exchange("slow-ex", SettlementConfig::default());

        // Record high latency
        optimizer.record_latency("slow-ex", 100_000_000); // 100ms

        let stats = optimizer.get_stats("slow-ex").unwrap();
        assert_eq!(stats.health, ExchangeHealth::Degraded);

        // Record very high latency
        optimizer.record_latency("slow-ex", 300_000_000); // 300ms
        
        let stats = optimizer.get_stats("slow-ex").unwrap();
        assert_eq!(stats.health, ExchangeHealth::Slow);
    }

    #[test]
    fn test_optimal_exchange_selection() {
        let optimizer = LatencyOptimizer::new(LatencyOptimizerConfig::default());
        
        optimizer.register_exchange("fast-ex", SettlementConfig { settlement_time_ms: 100, ..Default::default() });
        optimizer.register_exchange("slow-ex", SettlementConfig { settlement_time_ms: 500, ..Default::default() });

        // Record latencies
        for _ in 0..10 {
            optimizer.record_latency("fast-ex", 10_000_000); // 10ms
            optimizer.record_latency("slow-ex", 50_000_000); // 50ms
        }

        let decision = optimizer.get_optimal_exchange(&["fast-ex", "slow-ex"]).unwrap();
        
        assert_eq!(decision.exchange, "fast-ex");
        assert!(decision.estimated_latency_ns < 20_000_000);
    }

    #[test]
    fn test_skip_unavailable_exchange() {
        let optimizer = LatencyOptimizer::new(LatencyOptimizerConfig {
            unavailable_threshold_ms: 100,
            ..Default::default()
        });
        
        optimizer.register_exchange("good-ex", SettlementConfig::default());
        optimizer.register_exchange("bad-ex", SettlementConfig::default());

        // Make bad-ex unavailable
        for _ in 0..5 {
            optimizer.record_error("bad-ex");
        }

        // Record good latency for good-ex
        for _ in 0..10 {
            optimizer.record_latency("good-ex", 20_000_000);
        }

        let decision = optimizer.get_optimal_exchange(&["good-ex", "bad-ex"]).unwrap();
        
        assert_eq!(decision.exchange, "good-ex");
    }

    #[test]
    fn test_arb_timing() {
        let optimizer = LatencyOptimizer::new(LatencyOptimizerConfig::default());
        
        optimizer.register_exchange("ex-a", SettlementConfig { settlement_time_ms: 100, ..Default::default() });
        optimizer.register_exchange("ex-b", SettlementConfig { settlement_time_ms: 200, ..Default::default() });

        // Different latencies
        for _ in 0..10 {
            optimizer.record_latency("ex-a", 30_000_000); // 30ms
            optimizer.record_latency("ex-b", 10_000_000); // 10ms
        }

        let timing = optimizer.calculate_arb_timing("ex-a", "ex-b");
        
        // ex-a (buy) has higher latency, should send first
        assert_eq!(timing.first_leg, "buy");
        assert!(timing.delay_between_legs_ns > 10_000_000); // ~20ms difference
    }

    #[test]
    fn test_adjusted_send_time() {
        let optimizer = LatencyOptimizer::new(LatencyOptimizerConfig {
            settlement_buffer_ms: 5,
            ..Default::default()
        });
        
        optimizer.register_exchange("test-ex", SettlementConfig::default());

        for _ in 0..10 {
            optimizer.record_latency("test-ex", 20_000_000); // 20ms
        }

        let target = Instant::now() + Duration::from_secs(1);
        let send_time = optimizer.get_adjusted_send_time("test-ex", target);

        // Should be ~25ms before target (20ms latency + 5ms buffer)
        let diff = target.duration_since(send_time);
        assert!(diff.as_millis() >= 20 && diff.as_millis() <= 30);
    }

    #[test]
    fn test_percentile_calculation() {
        let optimizer = LatencyOptimizer::new(LatencyOptimizerConfig::default());
        optimizer.register_exchange("test-ex", SettlementConfig::default());

        // Record varied latencies
        for i in 1..=100 {
            optimizer.record_latency("test-ex", i * 1_000_000);
        }

        let stats = optimizer.get_stats("test-ex").unwrap();
        
        assert!(stats.p50_latency_ns >= 45_000_000 && stats.p50_latency_ns <= 55_000_000);
        assert!(stats.p95_latency_ns >= 90_000_000 && stats.p95_latency_ns <= 100_000_000);
    }
}
