use std::time::{Duration, Instant};
use std::collections::VecDeque;
use crate::core::types::ExecutionError;

/// Token bucket rate limiter for exchange API compliance
pub struct RateLimiter {
    capacity: u32,
    tokens: u32,
    refill_rate: u32, // tokens per second
    last_refill: Instant,
    request_history: VecDeque<Instant>,
}

impl RateLimiter {
    pub fn new(requests_per_second: u32, burst_capacity: u32) -> Self {
        Self {
            capacity: burst_capacity,
            tokens: burst_capacity,
            refill_rate: requests_per_second,
            last_refill: Instant::now(),
            request_history: VecDeque::new(),
        }
    }

    /// Check if a request can be made (non-blocking)
    pub fn try_acquire(&mut self) -> bool {
        self.refill_tokens();
        
        if self.tokens > 0 {
            self.tokens -= 1;
            self.request_history.push_back(Instant::now());
            
            // Keep only last second of requests for rate calculation
            let one_second_ago = Instant::now() - Duration::from_secs(1);
            while let Some(&front) = self.request_history.front() {
                if front < one_second_ago {
                    self.request_history.pop_front();
                } else {
                    break;
                }
            }
            
            true
        } else {
            false
        }
    }

    /// Blocking acquire with timeout
    pub async fn acquire_with_timeout(&mut self, timeout: Duration) -> Result<(), ExecutionError> {
        let start = Instant::now();
        
        while start.elapsed() < timeout {
            if self.try_acquire() {
                return Ok(());
            }
            
            // Calculate wait time until next token
            let wait_time = Duration::from_millis(1000 / self.refill_rate as u64);
            tokio::time::sleep(wait_time.min(Duration::from_millis(10))).await;
        }
        
        Err(ExecutionError::RateLimit("Rate limit timeout exceeded".to_string()))
    }

    /// Get current utilization percentage
    pub fn utilization(&self) -> f64 {
        1.0 - (self.tokens as f64 / self.capacity as f64)
    }

    /// Get current requests per second
    pub fn current_rps(&self) -> f64 {
        self.request_history.len() as f64
    }

    /// Reset the rate limiter
    pub fn reset(&mut self) {
        self.tokens = self.capacity;
        self.last_refill = Instant::now();
        self.request_history.clear();
    }

    fn refill_tokens(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        
        if elapsed >= Duration::from_millis(10) { // Minimum 10ms between refills
            let tokens_to_add = (elapsed.as_millis() as u32 * self.refill_rate) / 1000;
            
            if tokens_to_add > 0 {
                self.tokens = (self.tokens + tokens_to_add).min(self.capacity);
                self.last_refill = now;
            }
        }
    }
}

/// Adaptive rate limiter that adjusts based on exchange feedback
pub struct AdaptiveRateLimiter {
    base_limiter: RateLimiter,
    adaptation_factor: f64,
    consecutive_errors: u32,
    max_consecutive_errors: u32,
}

impl AdaptiveRateLimiter {
    pub fn new(base_rps: u32, burst_capacity: u32) -> Self {
        Self {
            base_limiter: RateLimiter::new(base_rps, burst_capacity),
            adaptation_factor: 1.0,
            consecutive_errors: 0,
            max_consecutive_errors: 3,
        }
    }

    pub fn try_acquire(&mut self) -> bool {
        self.base_limiter.try_acquire()
    }

    pub async fn acquire_with_timeout(&mut self, timeout: Duration) -> Result<(), ExecutionError> {
        self.base_limiter.acquire_with_timeout(timeout).await
    }

    /// Report a rate limit error to adapt the limiter
    pub fn report_rate_limit_error(&mut self) {
        self.consecutive_errors += 1;
        
        if self.consecutive_errors >= self.max_consecutive_errors {
            // Reduce rate by 20%
            self.adaptation_factor *= 0.8;
            self.consecutive_errors = 0;
            
            // Update the base limiter with new rate
            let new_rps = (self.base_limiter.refill_rate as f64 * self.adaptation_factor) as u32;
            self.base_limiter.refill_rate = new_rps.max(1);
        }
    }

    /// Report successful request to potentially increase rate
    pub fn report_success(&mut self) {
        if self.consecutive_errors > 0 {
            self.consecutive_errors -= 1;
        }
        
        // Gradually increase rate back to original
        if self.adaptation_factor < 1.0 {
            self.adaptation_factor = (self.adaptation_factor * 1.01).min(1.0);
            let new_rps = (self.base_limiter.refill_rate as f64 / self.adaptation_factor) as u32;
            self.base_limiter.refill_rate = new_rps;
        }
    }

    pub fn utilization(&self) -> f64 {
        self.base_limiter.utilization()
    }

    pub fn current_rps(&self) -> f64 {
        self.base_limiter.current_rps()
    }

    pub fn adaptation_factor(&self) -> f64 {
        self.adaptation_factor
    }
}
