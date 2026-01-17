//! Chaos Engineering & Fault Injection Testing
//!
//! Provides controlled failure injection for testing resilience:
//! - Network failures (timeouts, disconnections)
//! - Message corruption/drops
//! - Latency injection
//! - Circuit breaker testing
//! - Rate limit testing

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use parking_lot::RwLock;
use rand::Rng;

/// Chaos mode configuration
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Enable chaos testing
    pub enabled: bool,
    /// Probability of network failure (0.0 - 1.0)
    pub network_failure_rate: f64,
    /// Probability of message drop (0.0 - 1.0)
    pub message_drop_rate: f64,
    /// Probability of message delay (0.0 - 1.0)
    pub delay_probability: f64,
    /// Minimum delay when triggered
    pub min_delay_ms: u64,
    /// Maximum delay when triggered
    pub max_delay_ms: u64,
    /// Probability of message corruption (0.0 - 1.0)
    pub corruption_rate: f64,
    /// Probability of partial failure (some services fail)
    pub partial_failure_rate: f64,
    /// Target exchanges for chaos (empty = all)
    pub target_exchanges: Vec<String>,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            network_failure_rate: 0.0,
            message_drop_rate: 0.0,
            delay_probability: 0.0,
            min_delay_ms: 10,
            max_delay_ms: 1000,
            corruption_rate: 0.0,
            partial_failure_rate: 0.0,
            target_exchanges: Vec::new(),
        }
    }
}

impl ChaosConfig {
    /// Aggressive chaos configuration for stress testing
    pub fn aggressive() -> Self {
        Self {
            enabled: true,
            network_failure_rate: 0.1,
            message_drop_rate: 0.05,
            delay_probability: 0.2,
            min_delay_ms: 50,
            max_delay_ms: 500,
            corruption_rate: 0.02,
            partial_failure_rate: 0.05,
            target_exchanges: Vec::new(),
        }
    }

    /// Light chaos for integration tests
    pub fn light() -> Self {
        Self {
            enabled: true,
            network_failure_rate: 0.02,
            message_drop_rate: 0.01,
            delay_probability: 0.05,
            min_delay_ms: 10,
            max_delay_ms: 100,
            corruption_rate: 0.0,
            partial_failure_rate: 0.01,
            target_exchanges: Vec::new(),
        }
    }
}

/// Chaos injection result
#[derive(Debug, Clone)]
pub enum ChaosResult {
    /// No chaos applied
    Normal,
    /// Network failure injected
    NetworkFailure(String),
    /// Message dropped
    MessageDropped,
    /// Delay injected
    Delayed(Duration),
    /// Message corrupted
    Corrupted,
    /// Partial failure (some operations succeed)
    PartialFailure(Vec<String>),
}

/// Statistics for chaos testing
#[derive(Debug, Default, Clone)]
pub struct ChaosStats {
    pub total_operations: u64,
    pub network_failures_injected: u64,
    pub messages_dropped: u64,
    pub delays_injected: u64,
    pub corruptions_injected: u64,
    pub partial_failures_injected: u64,
    pub total_delay_ms: u64,
}

/// Chaos Monkey for controlled failure injection
pub struct ChaosMonkey {
    config: RwLock<ChaosConfig>,
    enabled: AtomicBool,
    stats: ChaosStats,
    operation_count: AtomicU64,
    network_failures: AtomicU64,
    messages_dropped: AtomicU64,
    delays_injected: AtomicU64,
    corruptions: AtomicU64,
    partial_failures: AtomicU64,
    total_delay_ms: AtomicU64,
    /// Per-exchange failure tracking
    exchange_failures: RwLock<HashMap<String, u64>>,
}

impl ChaosMonkey {
    pub fn new(config: ChaosConfig) -> Self {
        let enabled = config.enabled;
        Self {
            config: RwLock::new(config),
            enabled: AtomicBool::new(enabled),
            stats: ChaosStats::default(),
            operation_count: AtomicU64::new(0),
            network_failures: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            delays_injected: AtomicU64::new(0),
            corruptions: AtomicU64::new(0),
            partial_failures: AtomicU64::new(0),
            total_delay_ms: AtomicU64::new(0),
            exchange_failures: RwLock::new(HashMap::new()),
        }
    }

    /// Create a disabled chaos monkey (for production)
    pub fn disabled() -> Self {
        Self::new(ChaosConfig::default())
    }

    /// Enable chaos testing
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    /// Disable chaos testing
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    /// Check if chaos is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Update configuration
    pub fn update_config(&self, config: ChaosConfig) {
        let enabled = config.enabled;
        *self.config.write() = config;
        self.enabled.store(enabled, Ordering::Release);
    }

    /// Should this exchange be targeted?
    fn should_target(&self, exchange: Option<&str>) -> bool {
        let config = self.config.read();
        if config.target_exchanges.is_empty() {
            return true;
        }
        exchange.map_or(false, |e| config.target_exchanges.contains(&e.to_string()))
    }

    /// Apply chaos to an operation
    pub fn maybe_inject_chaos(&self, exchange: Option<&str>) -> ChaosResult {
        if !self.is_enabled() {
            return ChaosResult::Normal;
        }

        if !self.should_target(exchange) {
            return ChaosResult::Normal;
        }

        self.operation_count.fetch_add(1, Ordering::Relaxed);
        let config = self.config.read();
        let mut rng = rand::thread_rng();

        // Check for network failure
        if rng.gen::<f64>() < config.network_failure_rate {
            self.network_failures.fetch_add(1, Ordering::Relaxed);
            if let Some(ex) = exchange {
                let mut failures = self.exchange_failures.write();
                *failures.entry(ex.to_string()).or_insert(0) += 1;
            }
            return ChaosResult::NetworkFailure(format!(
                "Chaos: Simulated network failure for {}",
                exchange.unwrap_or("unknown")
            ));
        }

        // Check for message drop
        if rng.gen::<f64>() < config.message_drop_rate {
            self.messages_dropped.fetch_add(1, Ordering::Relaxed);
            return ChaosResult::MessageDropped;
        }

        // Check for delay
        if rng.gen::<f64>() < config.delay_probability {
            let delay_ms = rng.gen_range(config.min_delay_ms..=config.max_delay_ms);
            self.delays_injected.fetch_add(1, Ordering::Relaxed);
            self.total_delay_ms.fetch_add(delay_ms, Ordering::Relaxed);
            return ChaosResult::Delayed(Duration::from_millis(delay_ms));
        }

        // Check for corruption
        if rng.gen::<f64>() < config.corruption_rate {
            self.corruptions.fetch_add(1, Ordering::Relaxed);
            return ChaosResult::Corrupted;
        }

        ChaosResult::Normal
    }

    /// Inject a specific delay (for testing)
    pub async fn inject_delay(&self, min_ms: u64, max_ms: u64) {
        let delay = rand::thread_rng().gen_range(min_ms..=max_ms);
        self.delays_injected.fetch_add(1, Ordering::Relaxed);
        self.total_delay_ms.fetch_add(delay, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }

    /// Get current statistics
    pub fn stats(&self) -> ChaosStats {
        ChaosStats {
            total_operations: self.operation_count.load(Ordering::Relaxed),
            network_failures_injected: self.network_failures.load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            delays_injected: self.delays_injected.load(Ordering::Relaxed),
            corruptions_injected: self.corruptions.load(Ordering::Relaxed),
            partial_failures_injected: self.partial_failures.load(Ordering::Relaxed),
            total_delay_ms: self.total_delay_ms.load(Ordering::Relaxed),
        }
    }

    /// Get per-exchange failure counts
    pub fn exchange_failures(&self) -> HashMap<String, u64> {
        self.exchange_failures.read().clone()
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        self.operation_count.store(0, Ordering::Relaxed);
        self.network_failures.store(0, Ordering::Relaxed);
        self.messages_dropped.store(0, Ordering::Relaxed);
        self.delays_injected.store(0, Ordering::Relaxed);
        self.corruptions.store(0, Ordering::Relaxed);
        self.partial_failures.store(0, Ordering::Relaxed);
        self.total_delay_ms.store(0, Ordering::Relaxed);
        self.exchange_failures.write().clear();
    }
}

/// Global chaos monkey instance
pub static CHAOS_MONKEY: once_cell::sync::Lazy<ChaosMonkey> =
    once_cell::sync::Lazy::new(ChaosMonkey::disabled);

/// Network partition simulator
pub struct NetworkPartition {
    /// Partitioned exchanges (cannot communicate)
    partitioned: RwLock<std::collections::HashSet<String>>,
    /// Partition start time
    partition_start: RwLock<Option<std::time::Instant>>,
    /// Partition duration
    partition_duration: RwLock<Option<Duration>>,
}

impl NetworkPartition {
    pub fn new() -> Self {
        Self {
            partitioned: RwLock::new(std::collections::HashSet::new()),
            partition_start: RwLock::new(None),
            partition_duration: RwLock::new(None),
        }
    }

    /// Create a network partition for specific exchanges
    pub fn partition(&self, exchanges: &[&str], duration: Duration) {
        let mut partitioned = self.partitioned.write();
        for ex in exchanges {
            partitioned.insert(ex.to_string());
        }
        *self.partition_start.write() = Some(std::time::Instant::now());
        *self.partition_duration.write() = Some(duration);
        
        log::warn!("Network partition created for {:?}, duration: {:?}", exchanges, duration);
    }

    /// Check if exchange is partitioned
    pub fn is_partitioned(&self, exchange: &str) -> bool {
        // Check if partition has expired
        if let (Some(start), Some(duration)) = (
            *self.partition_start.read(),
            *self.partition_duration.read(),
        ) {
            if start.elapsed() > duration {
                self.heal();
                return false;
            }
        }
        
        self.partitioned.read().contains(exchange)
    }

    /// Heal the network partition
    pub fn heal(&self) {
        let mut partitioned = self.partitioned.write();
        if !partitioned.is_empty() {
            log::info!("Network partition healed for {:?}", partitioned.iter().collect::<Vec<_>>());
            partitioned.clear();
        }
        *self.partition_start.write() = None;
        *self.partition_duration.write() = None;
    }

    /// Get list of partitioned exchanges
    pub fn partitioned_exchanges(&self) -> Vec<String> {
        self.partitioned.read().iter().cloned().collect()
    }
}

impl Default for NetworkPartition {
    fn default() -> Self {
        Self::new()
    }
}

/// Fault injection decorator for async functions
pub struct FaultInjector {
    /// Chaos monkey
    chaos: Arc<ChaosMonkey>,
    /// Network partition
    partition: Arc<NetworkPartition>,
    /// Exchange name
    exchange: Option<String>,
}

impl FaultInjector {
    pub fn new(chaos: Arc<ChaosMonkey>, partition: Arc<NetworkPartition>) -> Self {
        Self {
            chaos,
            partition,
            exchange: None,
        }
    }

    pub fn for_exchange(mut self, exchange: &str) -> Self {
        self.exchange = Some(exchange.to_string());
        self
    }

    /// Check and apply chaos before an operation
    pub async fn before_operation(&self) -> Result<(), String> {
        // Check network partition
        if let Some(ref ex) = self.exchange {
            if self.partition.is_partitioned(ex) {
                return Err(format!("Network partition: {} is unreachable", ex));
            }
        }

        // Apply chaos
        match self.chaos.maybe_inject_chaos(self.exchange.as_deref()) {
            ChaosResult::Normal => Ok(()),
            ChaosResult::NetworkFailure(msg) => Err(msg),
            ChaosResult::MessageDropped => Err("Chaos: Message dropped".to_string()),
            ChaosResult::Delayed(duration) => {
                tokio::time::sleep(duration).await;
                Ok(())
            }
            ChaosResult::Corrupted => Err("Chaos: Message corrupted".to_string()),
            ChaosResult::PartialFailure(failed) => {
                Err(format!("Chaos: Partial failure for {:?}", failed))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaos_monkey_disabled() {
        let monkey = ChaosMonkey::disabled();
        assert!(!monkey.is_enabled());
        
        for _ in 0..100 {
            assert!(matches!(monkey.maybe_inject_chaos(None), ChaosResult::Normal));
        }
    }

    #[test]
    fn test_chaos_monkey_network_failure() {
        let config = ChaosConfig {
            enabled: true,
            network_failure_rate: 1.0, // Always fail
            ..Default::default()
        };
        let monkey = ChaosMonkey::new(config);
        
        let result = monkey.maybe_inject_chaos(Some("kraken"));
        assert!(matches!(result, ChaosResult::NetworkFailure(_)));
        
        let stats = monkey.stats();
        assert_eq!(stats.network_failures_injected, 1);
    }

    #[test]
    fn test_chaos_monkey_message_drop() {
        let config = ChaosConfig {
            enabled: true,
            message_drop_rate: 1.0, // Always drop
            ..Default::default()
        };
        let monkey = ChaosMonkey::new(config);
        
        let result = monkey.maybe_inject_chaos(None);
        assert!(matches!(result, ChaosResult::MessageDropped));
    }

    #[test]
    fn test_chaos_monkey_delay() {
        let config = ChaosConfig {
            enabled: true,
            delay_probability: 1.0, // Always delay
            min_delay_ms: 50,
            max_delay_ms: 100,
            ..Default::default()
        };
        let monkey = ChaosMonkey::new(config);
        
        let result = monkey.maybe_inject_chaos(None);
        if let ChaosResult::Delayed(duration) = result {
            assert!(duration.as_millis() >= 50);
            assert!(duration.as_millis() <= 100);
        } else {
            panic!("Expected Delayed result");
        }
    }

    #[test]
    fn test_chaos_monkey_target_exchange() {
        let config = ChaosConfig {
            enabled: true,
            network_failure_rate: 1.0,
            target_exchanges: vec!["binance".to_string()],
            ..Default::default()
        };
        let monkey = ChaosMonkey::new(config);
        
        // Should fail for binance
        let result = monkey.maybe_inject_chaos(Some("binance"));
        assert!(matches!(result, ChaosResult::NetworkFailure(_)));
        
        // Should not fail for kraken (not targeted)
        let result = monkey.maybe_inject_chaos(Some("kraken"));
        assert!(matches!(result, ChaosResult::Normal));
    }

    #[test]
    fn test_chaos_monkey_stats() {
        let config = ChaosConfig {
            enabled: true,
            network_failure_rate: 0.5,
            ..Default::default()
        };
        let monkey = ChaosMonkey::new(config);
        
        for _ in 0..100 {
            let _ = monkey.maybe_inject_chaos(None);
        }
        
        let stats = monkey.stats();
        assert_eq!(stats.total_operations, 100);
        assert!(stats.network_failures_injected > 0);
        assert!(stats.network_failures_injected < 100);
    }

    #[test]
    fn test_network_partition() {
        let partition = NetworkPartition::new();
        
        assert!(!partition.is_partitioned("kraken"));
        
        partition.partition(&["kraken", "binance"], Duration::from_secs(60));
        
        assert!(partition.is_partitioned("kraken"));
        assert!(partition.is_partitioned("binance"));
        assert!(!partition.is_partitioned("coinbase"));
        
        partition.heal();
        
        assert!(!partition.is_partitioned("kraken"));
    }

    #[test]
    fn test_network_partition_auto_heal() {
        let partition = NetworkPartition::new();
        
        // Very short partition
        partition.partition(&["kraken"], Duration::from_millis(1));
        
        // Wait for partition to expire
        std::thread::sleep(Duration::from_millis(10));
        
        // Should auto-heal on check
        assert!(!partition.is_partitioned("kraken"));
    }

    #[tokio::test]
    async fn test_fault_injector() {
        let chaos = Arc::new(ChaosMonkey::disabled());
        let partition = Arc::new(NetworkPartition::new());
        
        let injector = FaultInjector::new(chaos.clone(), partition.clone())
            .for_exchange("kraken");
        
        // Should succeed when chaos is disabled
        assert!(injector.before_operation().await.is_ok());
        
        // Create partition
        partition.partition(&["kraken"], Duration::from_secs(60));
        
        // Should fail due to partition
        let result = injector.before_operation().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("partition"));
    }

    #[tokio::test]
    async fn test_fault_injector_with_delay() {
        let config = ChaosConfig {
            enabled: true,
            delay_probability: 1.0,
            min_delay_ms: 10,
            max_delay_ms: 20,
            ..Default::default()
        };
        let chaos = Arc::new(ChaosMonkey::new(config));
        let partition = Arc::new(NetworkPartition::new());
        
        let injector = FaultInjector::new(chaos, partition);
        
        let start = std::time::Instant::now();
        let result = injector.before_operation().await;
        let elapsed = start.elapsed();
        
        // Should succeed but with delay
        assert!(result.is_ok());
        assert!(elapsed.as_millis() >= 10);
    }

    #[test]
    fn test_chaos_config_presets() {
        let light = ChaosConfig::light();
        assert!(light.enabled);
        assert!(light.network_failure_rate > 0.0);
        assert!(light.network_failure_rate < 0.1);
        
        let aggressive = ChaosConfig::aggressive();
        assert!(aggressive.enabled);
        assert!(aggressive.network_failure_rate > light.network_failure_rate);
    }
}
