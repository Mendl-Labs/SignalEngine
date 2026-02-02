//! Backpressure Handling Module
//!
//! Provides queue depth monitoring, adaptive throttling, and overflow protection
//! to ensure system stability under high load conditions.

use std::sync::atomic::{AtomicU64, AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, SemaphorePermit};
use dashmap::DashMap;
use parking_lot::RwLock;

/// Backpressure configuration
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum queue depth before throttling begins
    pub soft_limit: usize,
    /// Maximum queue depth before rejecting new requests
    pub hard_limit: usize,
    /// Initial requests per second limit
    pub initial_rate_limit: u32,
    /// Minimum rate limit (won't go below this)
    pub min_rate_limit: u32,
    /// Maximum rate limit (won't exceed this)
    pub max_rate_limit: u32,
    /// Rate adjustment factor (0.0 - 1.0)
    pub rate_adjustment_factor: f64,
    /// Window size for rate calculations
    pub window_size_ms: u64,
    /// Enable adaptive throttling
    pub adaptive_throttling: bool,
    /// Overflow policy
    pub overflow_policy: OverflowPolicy,
    /// Monitoring interval
    pub monitoring_interval_ms: u64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            soft_limit: 1000,
            hard_limit: 5000,
            initial_rate_limit: 100,
            min_rate_limit: 10,
            max_rate_limit: 1000,
            rate_adjustment_factor: 0.1,
            window_size_ms: 1000,
            adaptive_throttling: true,
            overflow_policy: OverflowPolicy::Reject,
            monitoring_interval_ms: 100,
        }
    }
}

/// Policy for handling queue overflow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Reject new requests when queue is full
    Reject,
    /// Drop oldest requests to make room
    DropOldest,
    /// Drop newest requests (same as reject but different semantics)
    DropNewest,
    /// Block until space is available (with timeout)
    Block { timeout_ms: u64 },
}

/// Current backpressure state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureState {
    /// Normal operation, no throttling
    Normal,
    /// Soft throttling active (queue depth between soft and hard limit)
    Throttled,
    /// Hard limit reached, rejecting requests
    Saturated,
    /// System is recovering from saturation
    Recovering,
}

/// Backpressure statistics
#[derive(Debug, Clone, Default)]
pub struct BackpressureStats {
    pub current_queue_depth: u64,
    pub current_rate_limit: u32,
    pub requests_accepted: u64,
    pub requests_rejected: u64,
    pub requests_dropped: u64,
    pub throttle_events: u64,
    pub saturation_events: u64,
    pub current_state: String,
    pub avg_wait_time_us: u64,
    pub peak_queue_depth: u64,
}

/// Backpressure controller for managing system load
pub struct BackpressureController {
    config: BackpressureConfig,
    // Current queue depth
    queue_depth: AtomicU64,
    // Peak queue depth observed
    peak_depth: AtomicU64,
    // Current rate limit (requests per second)
    current_rate_limit: AtomicU32,
    // Sliding window request counter
    request_count: AtomicU64,
    // Window start time (reserved for future rate window tracking)
    #[allow(dead_code)]
    _window_start: RwLock<Instant>,
    // Current state
    state: RwLock<BackpressureState>,
    // Semaphore for rate limiting
    rate_semaphore: Arc<Semaphore>,
    // Is system under pressure
    under_pressure: AtomicBool,
    // Stats
    accepted_count: AtomicU64,
    rejected_count: AtomicU64,
    dropped_count: AtomicU64,
    throttle_count: AtomicU64,
    saturation_count: AtomicU64,
    total_wait_time_us: AtomicU64,
    wait_count: AtomicU64,
    // Per-exchange backpressure tracking
    exchange_pressure: DashMap<String, ExchangeBackpressure>,
}

/// Per-exchange backpressure tracking
#[derive(Debug)]
#[allow(dead_code)]
struct ExchangeBackpressure {
    queue_depth: AtomicU64,
    rate_limit: AtomicU32,
    last_error_time: RwLock<Option<Instant>>,
    consecutive_errors: AtomicU32,
}

impl BackpressureController {
    /// Create a new backpressure controller
    pub fn new(config: BackpressureConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.initial_rate_limit as usize));
        
        Self {
            current_rate_limit: AtomicU32::new(config.initial_rate_limit),
            rate_semaphore: semaphore,
            config,
            queue_depth: AtomicU64::new(0),
            peak_depth: AtomicU64::new(0),
            request_count: AtomicU64::new(0),
            _window_start: RwLock::new(Instant::now()),
            state: RwLock::new(BackpressureState::Normal),
            under_pressure: AtomicBool::new(false),
            accepted_count: AtomicU64::new(0),
            rejected_count: AtomicU64::new(0),
            dropped_count: AtomicU64::new(0),
            throttle_count: AtomicU64::new(0),
            saturation_count: AtomicU64::new(0),
            total_wait_time_us: AtomicU64::new(0),
            wait_count: AtomicU64::new(0),
            exchange_pressure: DashMap::new(),
        }
    }

    /// Try to acquire permission to process a request
    /// Returns Ok(permit) if allowed, Err if rejected
    pub async fn try_acquire(&self) -> Result<BackpressurePermit<'_>, BackpressureError> {
        let current_depth = self.queue_depth.load(Ordering::Relaxed);
        
        // Check hard limit
        if current_depth >= self.config.hard_limit as u64 {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            self.update_state(BackpressureState::Saturated);
            return Err(BackpressureError::QueueFull {
                current: current_depth as usize,
                limit: self.config.hard_limit,
            });
        }

        // Check soft limit - apply throttling
        if current_depth >= self.config.soft_limit as u64 {
            self.under_pressure.store(true, Ordering::Relaxed);
            self.throttle_count.fetch_add(1, Ordering::Relaxed);
            self.update_state(BackpressureState::Throttled);
            
            // Apply adaptive throttling delay
            if self.config.adaptive_throttling {
                let pressure_ratio = (current_depth - self.config.soft_limit as u64) as f64
                    / (self.config.hard_limit - self.config.soft_limit) as f64;
                let delay_ms = (pressure_ratio * 100.0) as u64; // Up to 100ms delay
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        } else {
            self.under_pressure.store(false, Ordering::Relaxed);
        }

        // Try to acquire rate limit permit
        let start = Instant::now();
        match tokio::time::timeout(
            Duration::from_millis(self.config.window_size_ms),
            self.rate_semaphore.acquire()
        ).await {
            Ok(Ok(permit)) => {
                let wait_us = start.elapsed().as_micros() as u64;
                self.total_wait_time_us.fetch_add(wait_us, Ordering::Relaxed);
                self.wait_count.fetch_add(1, Ordering::Relaxed);
                
                // Increment queue depth
                let new_depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
                self.update_peak(new_depth);
                
                self.accepted_count.fetch_add(1, Ordering::Relaxed);
                self.request_count.fetch_add(1, Ordering::Relaxed);
                
                // Update state if recovering
                if new_depth < self.config.soft_limit as u64 {
                    self.update_state(BackpressureState::Normal);
                }
                
                Ok(BackpressurePermit {
                    controller: self,
                    _permit: permit,
                })
            }
            Ok(Err(_)) => {
                self.rejected_count.fetch_add(1, Ordering::Relaxed);
                Err(BackpressureError::RateLimitExceeded)
            }
            Err(_) => {
                self.rejected_count.fetch_add(1, Ordering::Relaxed);
                Err(BackpressureError::Timeout)
            }
        }
    }

    /// Acquire with exchange-specific backpressure
    pub async fn try_acquire_for_exchange(
        &self,
        exchange: &str
    ) -> Result<BackpressurePermit<'_>, BackpressureError> {
        // Check exchange-specific pressure
        if let Some(ex_pressure) = self.exchange_pressure.get(exchange) {
            let ex_depth = ex_pressure.queue_depth.load(Ordering::Relaxed);
            if ex_depth > 100 {
                // Exchange-specific throttling
                let delay = (ex_depth as f64 / 10.0) as u64;
                tokio::time::sleep(Duration::from_millis(delay.min(50))).await;
            }
            
            // Check for recent errors
            if ex_pressure.consecutive_errors.load(Ordering::Relaxed) > 5 {
                return Err(BackpressureError::ExchangeUnhealthy {
                    exchange: exchange.to_string(),
                });
            }
        }
        
        self.try_acquire().await
    }

    /// Record exchange error (for adaptive backpressure)
    pub fn record_exchange_error(&self, exchange: &str) {
        self.exchange_pressure
            .entry(exchange.to_string())
            .or_insert_with(|| ExchangeBackpressure {
                queue_depth: AtomicU64::new(0),
                rate_limit: AtomicU32::new(100),
                last_error_time: RwLock::new(None),
                consecutive_errors: AtomicU32::new(0),
            })
            .consecutive_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record exchange success (resets error counter)
    pub fn record_exchange_success(&self, exchange: &str) {
        if let Some(ex_pressure) = self.exchange_pressure.get(exchange) {
            ex_pressure.consecutive_errors.store(0, Ordering::Relaxed);
        }
    }

    /// Adjust rate limit based on system conditions
    pub fn adjust_rate_limit(&self, success_rate: f64) {
        if !self.config.adaptive_throttling {
            return;
        }

        let current = self.current_rate_limit.load(Ordering::Relaxed);
        let new_limit = if success_rate > 0.95 {
            // Increase rate if success rate is high
            let increase = (current as f64 * self.config.rate_adjustment_factor) as u32;
            (current + increase).min(self.config.max_rate_limit)
        } else if success_rate < 0.80 {
            // Decrease rate if success rate is low
            let decrease = (current as f64 * self.config.rate_adjustment_factor) as u32;
            (current - decrease).max(self.config.min_rate_limit)
        } else {
            current
        };

        if new_limit != current {
            self.current_rate_limit.store(new_limit, Ordering::Relaxed);
        }
    }

    /// Get current backpressure state
    pub fn get_state(&self) -> BackpressureState {
        *self.state.read()
    }

    /// Get current queue depth
    pub fn get_queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// Get current rate limit
    pub fn get_rate_limit(&self) -> u32 {
        self.current_rate_limit.load(Ordering::Relaxed)
    }

    /// Check if system is under pressure
    pub fn is_under_pressure(&self) -> bool {
        self.under_pressure.load(Ordering::Relaxed)
    }

    /// Get comprehensive statistics
    pub fn get_stats(&self) -> BackpressureStats {
        let wait_count = self.wait_count.load(Ordering::Relaxed);
        let avg_wait = if wait_count > 0 {
            self.total_wait_time_us.load(Ordering::Relaxed) / wait_count
        } else {
            0
        };

        BackpressureStats {
            current_queue_depth: self.queue_depth.load(Ordering::Relaxed),
            current_rate_limit: self.current_rate_limit.load(Ordering::Relaxed),
            requests_accepted: self.accepted_count.load(Ordering::Relaxed),
            requests_rejected: self.rejected_count.load(Ordering::Relaxed),
            requests_dropped: self.dropped_count.load(Ordering::Relaxed),
            throttle_events: self.throttle_count.load(Ordering::Relaxed),
            saturation_events: self.saturation_count.load(Ordering::Relaxed),
            current_state: format!("{:?}", self.get_state()),
            avg_wait_time_us: avg_wait,
            peak_queue_depth: self.peak_depth.load(Ordering::Relaxed),
        }
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        self.accepted_count.store(0, Ordering::Relaxed);
        self.rejected_count.store(0, Ordering::Relaxed);
        self.dropped_count.store(0, Ordering::Relaxed);
        self.throttle_count.store(0, Ordering::Relaxed);
        self.saturation_count.store(0, Ordering::Relaxed);
        self.total_wait_time_us.store(0, Ordering::Relaxed);
        self.wait_count.store(0, Ordering::Relaxed);
        self.peak_depth.store(0, Ordering::Relaxed);
    }

    fn update_peak(&self, depth: u64) {
        let mut peak = self.peak_depth.load(Ordering::Relaxed);
        while depth > peak {
            match self.peak_depth.compare_exchange_weak(
                peak,
                depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => peak = current,
            }
        }
    }

    fn update_state(&self, new_state: BackpressureState) {
        let mut state = self.state.write();
        if *state != new_state {
            if new_state == BackpressureState::Saturated {
                self.saturation_count.fetch_add(1, Ordering::Relaxed);
            }
            *state = new_state;
        }
    }

    fn release(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.rate_semaphore.add_permits(1);
    }
}

/// RAII permit for backpressure
pub struct BackpressurePermit<'a> {
    controller: &'a BackpressureController,
    #[allow(dead_code)]
    _permit: SemaphorePermit<'a>,
}

impl Drop for BackpressurePermit<'_> {
    fn drop(&mut self) {
        self.controller.release();
    }
}

/// Backpressure errors
#[derive(Debug, thiserror::Error)]
pub enum BackpressureError {
    #[error("Queue full: {current}/{limit}")]
    QueueFull { current: usize, limit: usize },
    
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    
    #[error("Request timeout")]
    Timeout,
    
    #[error("Exchange unhealthy: {exchange}")]
    ExchangeUnhealthy { exchange: String },
}

/// Global backpressure controller (for convenience)
use once_cell::sync::Lazy;

pub static BACKPRESSURE: Lazy<BackpressureController> = Lazy::new(|| {
    BackpressureController::new(BackpressureConfig::default())
});

/// Convenience function to acquire backpressure permit
pub async fn acquire_permit() -> Result<BackpressurePermit<'static>, BackpressureError> {
    BACKPRESSURE.try_acquire().await
}

/// Convenience function for exchange-specific permit
pub async fn acquire_permit_for_exchange(
    exchange: &str
) -> Result<BackpressurePermit<'static>, BackpressureError> {
    BACKPRESSURE.try_acquire_for_exchange(exchange).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_acquire_release() {
        let config = BackpressureConfig {
            soft_limit: 10,
            hard_limit: 20,
            initial_rate_limit: 100,
            ..Default::default()
        };
        let controller = BackpressureController::new(config);

        // Should be able to acquire
        let permit = controller.try_acquire().await.expect("Should acquire");
        assert_eq!(controller.get_queue_depth(), 1);
        
        // Release
        drop(permit);
        assert_eq!(controller.get_queue_depth(), 0);
    }

    #[tokio::test]
    async fn test_hard_limit_rejection() {
        let config = BackpressureConfig {
            soft_limit: 2,
            hard_limit: 3,
            initial_rate_limit: 100,
            ..Default::default()
        };
        let controller = BackpressureController::new(config);

        // Fill to hard limit
        let _p1 = controller.try_acquire().await.unwrap();
        let _p2 = controller.try_acquire().await.unwrap();
        let _p3 = controller.try_acquire().await.unwrap();

        // Should reject
        let result = controller.try_acquire().await;
        assert!(matches!(result, Err(BackpressureError::QueueFull { .. })));
    }

    #[tokio::test]
    async fn test_throttle_state() {
        let config = BackpressureConfig {
            soft_limit: 2,
            hard_limit: 10,
            initial_rate_limit: 100,
            adaptive_throttling: false, // Disable delay for faster test
            ..Default::default()
        };
        let controller = BackpressureController::new(config);

        let _p1 = controller.try_acquire().await.unwrap();
        let _p2 = controller.try_acquire().await.unwrap();
        
        // Third should trigger throttled state
        let _p3 = controller.try_acquire().await.unwrap();
        
        assert_eq!(controller.get_state(), BackpressureState::Throttled);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let config = BackpressureConfig {
            soft_limit: 100,
            hard_limit: 200,
            initial_rate_limit: 100,
            ..Default::default()
        };
        let controller = BackpressureController::new(config);

        for _ in 0..5 {
            let permit = controller.try_acquire().await.unwrap();
            drop(permit);
        }

        let stats = controller.get_stats();
        assert_eq!(stats.requests_accepted, 5);
        assert_eq!(stats.requests_rejected, 0);
    }

    #[tokio::test]
    async fn test_exchange_error_tracking() {
        let config = BackpressureConfig::default();
        let controller = BackpressureController::new(config);

        // Record multiple errors
        for _ in 0..6 {
            controller.record_exchange_error("test-exchange");
        }

        // Should reject due to unhealthy exchange
        let result = controller.try_acquire_for_exchange("test-exchange").await;
        assert!(matches!(result, Err(BackpressureError::ExchangeUnhealthy { .. })));

        // Reset with success
        controller.record_exchange_success("test-exchange");
        let result = controller.try_acquire_for_exchange("test-exchange").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rate_limit_adjustment() {
        let config = BackpressureConfig {
            initial_rate_limit: 100,
            min_rate_limit: 50,
            max_rate_limit: 200,
            rate_adjustment_factor: 0.1,
            adaptive_throttling: true,
            ..Default::default()
        };
        let controller = BackpressureController::new(config);

        // High success rate should increase limit
        controller.adjust_rate_limit(0.98);
        assert!(controller.get_rate_limit() > 100);

        // Reset
        controller.current_rate_limit.store(100, Ordering::Relaxed);

        // Low success rate should decrease limit
        controller.adjust_rate_limit(0.70);
        assert!(controller.get_rate_limit() < 100);
    }
}
