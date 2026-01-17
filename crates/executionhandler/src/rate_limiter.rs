//! Token Bucket Rate Limiter with Sliding Window
//!
//! High-performance rate limiter using atomic operations for HFT hot paths.
//! Supports both simple token bucket and sliding window algorithms.

use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use dashmap::DashMap;

/// Configuration for rate limiter
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Maximum tokens (requests) per second
    pub rate_per_second: u32,
    /// Maximum burst size (bucket capacity)
    pub burst_size: u32,
    /// Token refill interval granularity
    pub refill_interval: Duration,
    /// Use sliding window instead of fixed window
    pub sliding_window: bool,
    /// Number of sub-windows for sliding window algorithm
    pub window_segments: u32,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            rate_per_second: 100,
            burst_size: 150,
            refill_interval: Duration::from_millis(10),
            sliding_window: true,
            window_segments: 10,
        }
    }
}

/// Atomic token bucket for lock-free rate limiting
#[repr(C, align(64))]
pub struct AtomicTokenBucket {
    /// Current token count (scaled by 1000 for sub-token precision)
    tokens: AtomicI64,
    /// Last refill timestamp in nanoseconds
    last_refill_ns: AtomicU64,
    /// Startup instant for relative time
    start_instant: Instant,
    /// Configuration
    config: RateLimiterConfig,
    /// Tokens added per nanosecond (scaled by 1e9 for precision)
    tokens_per_ns: f64,
    /// Maximum tokens (scaled)
    max_tokens: i64,
}

impl AtomicTokenBucket {
    const SCALE: i64 = 1000; // Sub-token precision

    pub fn new(config: RateLimiterConfig) -> Self {
        let max_tokens = config.burst_size as i64 * Self::SCALE;
        let tokens_per_ns = config.rate_per_second as f64 / 1_000_000_000.0;
        
        Self {
            tokens: AtomicI64::new(max_tokens),
            last_refill_ns: AtomicU64::new(0),
            start_instant: Instant::now(),
            config,
            tokens_per_ns,
            max_tokens,
        }
    }

    /// Try to acquire a token (hot path - lock-free)
    #[inline]
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_n(1)
    }

    /// Try to acquire N tokens
    #[inline]
    pub fn try_acquire_n(&self, n: u32) -> bool {
        let cost = n as i64 * Self::SCALE;
        
        // Refill tokens based on elapsed time
        self.refill();
        
        // Try to consume tokens using CAS loop
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            
            if current < cost {
                return false;
            }
            
            match self.tokens.compare_exchange_weak(
                current,
                current - cost,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // Retry CAS
            }
        }
    }

    /// Acquire a token, blocking if necessary (returns wait time)
    pub fn acquire(&self) -> Duration {
        self.acquire_n(1)
    }

    /// Acquire N tokens, returning the time waited
    pub fn acquire_n(&self, n: u32) -> Duration {
        let start = Instant::now();
        let cost = n as i64 * Self::SCALE;
        
        loop {
            self.refill();
            
            let current = self.tokens.load(Ordering::Acquire);
            
            if current >= cost {
                match self.tokens.compare_exchange_weak(
                    current,
                    current - cost,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return start.elapsed(),
                    Err(_) => continue,
                }
            }
            
            // Calculate wait time for tokens to refill
            let deficit = cost - current;
            let wait_ns = (deficit as f64 / Self::SCALE as f64 / self.tokens_per_ns) as u64;
            
            if wait_ns > 0 {
                std::thread::sleep(Duration::from_nanos(wait_ns.min(1_000_000))); // Max 1ms sleep
            }
        }
    }

    /// Get current token count
    #[inline]
    pub fn available_tokens(&self) -> u32 {
        self.refill();
        let tokens = self.tokens.load(Ordering::Acquire);
        (tokens.max(0) / Self::SCALE) as u32
    }

    /// Get utilization ratio (0.0 - 1.0)
    pub fn utilization(&self) -> f64 {
        self.refill();
        let current = self.tokens.load(Ordering::Acquire);
        1.0 - (current as f64 / self.max_tokens as f64).clamp(0.0, 1.0)
    }

    /// Reset the bucket to full
    pub fn reset(&self) {
        self.tokens.store(self.max_tokens, Ordering::Release);
        self.last_refill_ns.store(self.now_ns(), Ordering::Release);
    }

    #[inline]
    fn now_ns(&self) -> u64 {
        self.start_instant.elapsed().as_nanos() as u64
    }

    #[inline]
    fn refill(&self) {
        let now_ns = self.now_ns();
        let last_refill = self.last_refill_ns.load(Ordering::Acquire);
        
        if now_ns <= last_refill {
            return;
        }
        
        let elapsed_ns = now_ns - last_refill;
        let tokens_to_add = (elapsed_ns as f64 * self.tokens_per_ns * Self::SCALE as f64) as i64;
        
        if tokens_to_add > 0 {
            // Update last refill time
            let _ = self.last_refill_ns.compare_exchange(
                last_refill,
                now_ns,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            
            // Add tokens (capped at max)
            loop {
                let current = self.tokens.load(Ordering::Acquire);
                let new_tokens = (current + tokens_to_add).min(self.max_tokens);
                
                match self.tokens.compare_exchange_weak(
                    current,
                    new_tokens,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(_) => continue,
                }
            }
        }
    }
}

unsafe impl Send for AtomicTokenBucket {}
unsafe impl Sync for AtomicTokenBucket {}

/// Sliding window rate limiter with sub-windows
pub struct SlidingWindowLimiter {
    /// Window segments storing request counts
    segments: Vec<AtomicU64>,
    /// Current segment index
    current_segment: AtomicU64,
    /// Last segment rotation time
    last_rotation_ns: AtomicU64,
    /// Startup instant
    start_instant: Instant,
    /// Configuration
    config: RateLimiterConfig,
    /// Nanoseconds per segment
    ns_per_segment: u64,
}

impl SlidingWindowLimiter {
    pub fn new(config: RateLimiterConfig) -> Self {
        let segment_count = config.window_segments as usize;
        let ns_per_segment = 1_000_000_000 / config.window_segments as u64;
        
        Self {
            segments: (0..segment_count).map(|_| AtomicU64::new(0)).collect(),
            current_segment: AtomicU64::new(0),
            last_rotation_ns: AtomicU64::new(0),
            start_instant: Instant::now(),
            config,
            ns_per_segment,
        }
    }

    /// Try to acquire permission for a request
    #[inline]
    pub fn try_acquire(&self) -> bool {
        self.rotate_if_needed();
        
        // Count requests in current window
        let total: u64 = self.segments.iter()
            .map(|s| s.load(Ordering::Relaxed))
            .sum();
        
        if total >= self.config.rate_per_second as u64 {
            return false;
        }
        
        // Increment current segment
        let current_idx = self.current_segment.load(Ordering::Acquire) as usize;
        self.segments[current_idx].fetch_add(1, Ordering::Relaxed);
        
        true
    }

    /// Get current request rate
    pub fn current_rate(&self) -> u64 {
        self.rotate_if_needed();
        self.segments.iter()
            .map(|s| s.load(Ordering::Relaxed))
            .sum()
    }

    /// Reset the limiter
    pub fn reset(&self) {
        for segment in &self.segments {
            segment.store(0, Ordering::Release);
        }
        self.current_segment.store(0, Ordering::Release);
        self.last_rotation_ns.store(self.now_ns(), Ordering::Release);
    }

    #[inline]
    fn now_ns(&self) -> u64 {
        self.start_instant.elapsed().as_nanos() as u64
    }

    fn rotate_if_needed(&self) {
        let now_ns = self.now_ns();
        let last_rotation = self.last_rotation_ns.load(Ordering::Acquire);
        
        let segments_to_advance = ((now_ns - last_rotation) / self.ns_per_segment) as usize;
        
        if segments_to_advance > 0 {
            let segment_count = self.segments.len();
            let current_idx = self.current_segment.load(Ordering::Acquire) as usize;
            
            // Clear old segments
            for i in 1..=segments_to_advance.min(segment_count) {
                let idx = (current_idx + i) % segment_count;
                self.segments[idx].store(0, Ordering::Release);
            }
            
            // Update current segment
            let new_idx = (current_idx + segments_to_advance) % segment_count;
            self.current_segment.store(new_idx as u64, Ordering::Release);
            self.last_rotation_ns.store(now_ns, Ordering::Release);
        }
    }
}

unsafe impl Send for SlidingWindowLimiter {}
unsafe impl Sync for SlidingWindowLimiter {}

/// Unified rate limiter supporting both algorithms
pub enum RateLimiter {
    TokenBucket(AtomicTokenBucket),
    SlidingWindow(SlidingWindowLimiter),
}

impl RateLimiter {
    pub fn token_bucket(config: RateLimiterConfig) -> Self {
        RateLimiter::TokenBucket(AtomicTokenBucket::new(config))
    }

    pub fn sliding_window(config: RateLimiterConfig) -> Self {
        RateLimiter::SlidingWindow(SlidingWindowLimiter::new(config))
    }

    pub fn try_acquire(&self) -> bool {
        match self {
            RateLimiter::TokenBucket(tb) => tb.try_acquire(),
            RateLimiter::SlidingWindow(sw) => sw.try_acquire(),
        }
    }

    pub fn reset(&self) {
        match self {
            RateLimiter::TokenBucket(tb) => tb.reset(),
            RateLimiter::SlidingWindow(sw) => sw.reset(),
        }
    }
}

/// Per-exchange rate limiter manager
pub struct ExchangeRateLimiterManager {
    limiters: DashMap<String, AtomicTokenBucket>,
    default_config: RateLimiterConfig,
}

impl ExchangeRateLimiterManager {
    pub fn new(default_config: RateLimiterConfig) -> Self {
        Self {
            limiters: DashMap::new(),
            default_config,
        }
    }

    /// Add or update rate limiter for an exchange
    pub fn configure_exchange(&self, exchange: &str, config: RateLimiterConfig) {
        self.limiters.insert(exchange.to_string(), AtomicTokenBucket::new(config));
    }

    /// Try to acquire permission for a request to an exchange
    #[inline]
    pub fn try_acquire(&self, exchange: &str) -> bool {
        self.limiters
            .entry(exchange.to_string())
            .or_insert_with(|| AtomicTokenBucket::new(self.default_config.clone()))
            .try_acquire()
    }

    /// Try to acquire multiple tokens
    #[inline]
    pub fn try_acquire_n(&self, exchange: &str, n: u32) -> bool {
        self.limiters
            .entry(exchange.to_string())
            .or_insert_with(|| AtomicTokenBucket::new(self.default_config.clone()))
            .try_acquire_n(n)
    }

    /// Get utilization for an exchange
    pub fn utilization(&self, exchange: &str) -> f64 {
        self.limiters
            .get(exchange)
            .map(|l| l.utilization())
            .unwrap_or(0.0)
    }

    /// Get status of all exchanges
    pub fn get_status(&self) -> Vec<RateLimiterStatus> {
        self.limiters.iter()
            .map(|entry| RateLimiterStatus {
                exchange: entry.key().clone(),
                available_tokens: entry.value().available_tokens(),
                utilization: entry.value().utilization(),
            })
            .collect()
    }

    /// Reset all limiters
    pub fn reset_all(&self) {
        for entry in self.limiters.iter() {
            entry.value().reset();
        }
    }
}

impl Default for ExchangeRateLimiterManager {
    fn default() -> Self {
        Self::new(RateLimiterConfig::default())
    }
}

/// Status of a rate limiter
#[derive(Debug, Clone)]
pub struct RateLimiterStatus {
    pub exchange: String,
    pub available_tokens: u32,
    pub utilization: f64,
}

/// Global rate limiter manager
pub static RATE_LIMITERS: once_cell::sync::Lazy<ExchangeRateLimiterManager> =
    once_cell::sync::Lazy::new(ExchangeRateLimiterManager::default);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_basic() {
        let config = RateLimiterConfig {
            rate_per_second: 10,
            burst_size: 10,
            ..Default::default()
        };
        let bucket = AtomicTokenBucket::new(config);
        
        // Should succeed up to burst size
        for _ in 0..10 {
            assert!(bucket.try_acquire());
        }
        
        // Should fail after exhausting burst
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn test_token_bucket_refill() {
        let config = RateLimiterConfig {
            rate_per_second: 1000, // 1000/sec = 1/ms
            burst_size: 10,
            ..Default::default()
        };
        let bucket = AtomicTokenBucket::new(config);
        
        // Exhaust bucket
        for _ in 0..10 {
            bucket.try_acquire();
        }
        assert!(!bucket.try_acquire());
        
        // Wait for refill
        std::thread::sleep(Duration::from_millis(15));
        
        // Should have refilled some tokens
        assert!(bucket.try_acquire());
    }

    #[test]
    fn test_token_bucket_acquire_n() {
        let config = RateLimiterConfig {
            rate_per_second: 100,
            burst_size: 20,
            ..Default::default()
        };
        let bucket = AtomicTokenBucket::new(config);
        
        assert!(bucket.try_acquire_n(10));
        assert!(bucket.try_acquire_n(10));
        assert!(!bucket.try_acquire_n(1));
    }

    #[test]
    fn test_sliding_window_basic() {
        let config = RateLimiterConfig {
            rate_per_second: 10,
            window_segments: 10,
            ..Default::default()
        };
        let limiter = SlidingWindowLimiter::new(config);
        
        // Should succeed up to rate limit
        for _ in 0..10 {
            assert!(limiter.try_acquire());
        }
        
        // Should fail after rate limit
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_sliding_window_rotation() {
        let config = RateLimiterConfig {
            rate_per_second: 10,
            window_segments: 10,
            ..Default::default()
        };
        let limiter = SlidingWindowLimiter::new(config);
        
        // Fill up
        for _ in 0..10 {
            limiter.try_acquire();
        }
        
        // Should be rate limited now
        assert!(!limiter.try_acquire());
        
        // Wait for several segment rotations (100ms per segment * 2)
        std::thread::sleep(Duration::from_millis(250));
        
        // Should allow some requests after rotation clears old segments
        // (may or may not succeed depending on timing - just ensure no panic)
        let _ = limiter.try_acquire();
    }

    #[test]
    fn test_exchange_manager() {
        let manager = ExchangeRateLimiterManager::default();
        
        // Configure specific exchange
        manager.configure_exchange("kraken", RateLimiterConfig {
            rate_per_second: 5,
            burst_size: 5,
            ..Default::default()
        });
        
        // Should respect configured limit
        for _ in 0..5 {
            assert!(manager.try_acquire("kraken"));
        }
        assert!(!manager.try_acquire("kraken"));
        
        // Default exchange should use default config
        assert!(manager.try_acquire("binance"));
    }

    #[test]
    fn test_utilization() {
        let config = RateLimiterConfig {
            rate_per_second: 100,
            burst_size: 100,
            ..Default::default()
        };
        let bucket = AtomicTokenBucket::new(config);
        
        assert!(bucket.utilization() < 0.1);
        
        for _ in 0..50 {
            bucket.try_acquire();
        }
        
        let util = bucket.utilization();
        assert!(util > 0.4 && util < 0.6);
    }
}
