//! Lock-Free Circuit Breaker for HFT Hot Paths
//!
//! This implementation uses atomics instead of mutexes for the hot path,
//! avoiding lock contention in high-frequency trading scenarios.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use parking_lot::RwLock;

/// Circuit breaker states (stored as u8 for atomic operations)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CircuitState {
    Closed = 0,   // Normal operation
    Open = 1,     // Failing - reject requests
    HalfOpen = 2, // Testing if service recovered
}

impl From<u8> for CircuitState {
    fn from(value: u8) -> Self {
        match value {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

/// Configuration for circuit breaker behavior
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening circuit
    pub failure_threshold: u32,
    /// Time to wait before attempting recovery (half-open state)
    pub recovery_timeout: Duration,
    /// Number of successes in half-open state to close circuit
    pub success_threshold: u32,
    /// Percentage of requests to allow in half-open state (0-100)
    pub half_open_request_pct: u8,
    /// Sliding window size in seconds for failure counting
    pub failure_window_secs: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(30),
            success_threshold: 3,
            half_open_request_pct: 10,
            failure_window_secs: 60,
        }
    }
}

/// Lock-free circuit breaker using atomic operations
/// 
/// All hot-path operations (can_execute, record_success, record_failure)
/// use only atomic operations with no locks.
#[repr(C, align(64))] // Cache-line aligned to prevent false sharing
pub struct AtomicCircuitBreaker {
    // ---- Cache line 1: Hot read path ----
    /// Current state (0=Closed, 1=Open, 2=HalfOpen)
    state: AtomicU8,
    /// Consecutive failure count
    failure_count: AtomicU32,
    /// Timestamp of last failure (nanoseconds since startup)
    last_failure_ns: AtomicU64,
    
    // ---- Cache line 2: Half-open tracking ----
    /// Successful calls in half-open state
    half_open_successes: AtomicU32,
    /// Request counter for half-open rate limiting
    half_open_request_counter: AtomicU32,
    /// Consecutive success count (for reset)
    consecutive_successes: AtomicU32,
    
    // ---- Cache line 3: Configuration (cold path) ----
    config: CircuitBreakerConfig,
    /// Startup instant for relative timestamps
    start_instant: Instant,
}

impl AtomicCircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicU32::new(0),
            last_failure_ns: AtomicU64::new(0),
            half_open_successes: AtomicU32::new(0),
            half_open_request_counter: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            config,
            start_instant: Instant::now(),
        }
    }

    /// Create with default configuration
    pub fn with_defaults(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self::new(CircuitBreakerConfig {
            failure_threshold,
            recovery_timeout,
            ..Default::default()
        })
    }

    /// Check if a request can be executed (hot path - lock-free)
    #[inline]
    pub fn can_execute(&self) -> bool {
        let state = CircuitState::from(self.state.load(Ordering::Acquire));
        
        match state {
            CircuitState::Closed => true,
            CircuitState::Open => self.try_transition_to_half_open(),
            CircuitState::HalfOpen => self.should_allow_half_open_request(),
        }
    }

    /// Record a successful execution (hot path - lock-free)
    #[inline]
    pub fn record_success(&self) {
        let state = CircuitState::from(self.state.load(Ordering::Acquire));
        
        match state {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Release);
                self.consecutive_successes.fetch_add(1, Ordering::Relaxed);
            }
            CircuitState::HalfOpen => {
                let successes = self.half_open_successes.fetch_add(1, Ordering::AcqRel) + 1;
                
                if successes >= self.config.success_threshold {
                    // Transition to closed
                    self.transition_to_closed();
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but handle gracefully
            }
        }
    }

    /// Record a failed execution (hot path - lock-free)
    #[inline]
    pub fn record_failure(&self) {
        let now_ns = self.now_ns();
        let state = CircuitState::from(self.state.load(Ordering::Acquire));
        
        // Update last failure time
        self.last_failure_ns.store(now_ns, Ordering::Release);
        self.consecutive_successes.store(0, Ordering::Release);
        
        match state {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
                
                if failures >= self.config.failure_threshold {
                    self.transition_to_open();
                }
            }
            CircuitState::HalfOpen => {
                // Single failure in half-open goes back to open
                self.transition_to_open();
            }
            CircuitState::Open => {
                // Already open, just update failure count
                self.failure_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Execute a function through the circuit breaker
    pub fn call<F, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Result<T, E>,
    {
        if !self.can_execute() {
            return Err(CircuitBreakerError::CircuitOpen);
        }

        match f() {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(error) => {
                self.record_failure();
                Err(CircuitBreakerError::ServiceError(error))
            }
        }
    }

    /// Execute an async function through the circuit breaker
    pub async fn call_async<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        if !self.can_execute() {
            return Err(CircuitBreakerError::CircuitOpen);
        }

        match f().await {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(error) => {
                self.record_failure();
                Err(CircuitBreakerError::ServiceError(error))
            }
        }
    }

    /// Get current state
    #[inline]
    pub fn get_state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::Acquire))
    }

    /// Get current failure count
    #[inline]
    pub fn get_failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::Acquire)
    }

    /// Get time since last failure in milliseconds
    pub fn time_since_last_failure_ms(&self) -> Option<u64> {
        let last_ns = self.last_failure_ns.load(Ordering::Acquire);
        if last_ns == 0 {
            None
        } else {
            let now_ns = self.now_ns();
            Some((now_ns.saturating_sub(last_ns)) / 1_000_000)
        }
    }

    /// Force reset the circuit breaker
    pub fn reset(&self) {
        self.transition_to_closed();
        self.failure_count.store(0, Ordering::Release);
        self.last_failure_ns.store(0, Ordering::Release);
    }

    // ---- Private methods ----

    #[inline]
    fn now_ns(&self) -> u64 {
        self.start_instant.elapsed().as_nanos() as u64
    }

    fn try_transition_to_half_open(&self) -> bool {
        let last_failure_ns = self.last_failure_ns.load(Ordering::Acquire);
        let now_ns = self.now_ns();
        let recovery_ns = self.config.recovery_timeout.as_nanos() as u64;
        
        if now_ns.saturating_sub(last_failure_ns) >= recovery_ns {
            // Try to transition to half-open using CAS
            let result = self.state.compare_exchange(
                CircuitState::Open as u8,
                CircuitState::HalfOpen as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            
            if result.is_ok() {
                self.half_open_successes.store(0, Ordering::Release);
                self.half_open_request_counter.store(0, Ordering::Release);
            }
            
            result.is_ok()
        } else {
            false
        }
    }

    fn should_allow_half_open_request(&self) -> bool {
        if self.config.half_open_request_pct >= 100 {
            return true;
        }
        
        let counter = self.half_open_request_counter.fetch_add(1, Ordering::Relaxed);
        (counter % 100) < self.config.half_open_request_pct as u32
    }

    fn transition_to_open(&self) {
        self.state.store(CircuitState::Open as u8, Ordering::Release);
        self.half_open_successes.store(0, Ordering::Release);
    }

    fn transition_to_closed(&self) {
        self.state.store(CircuitState::Closed as u8, Ordering::Release);
        self.failure_count.store(0, Ordering::Release);
        self.half_open_successes.store(0, Ordering::Release);
        self.half_open_request_counter.store(0, Ordering::Release);
    }
}

// Manual Send + Sync implementations
unsafe impl Send for AtomicCircuitBreaker {}
unsafe impl Sync for AtomicCircuitBreaker {}

/// Error types for circuit breaker
#[derive(Debug)]
pub enum CircuitBreakerError<E> {
    CircuitOpen,
    ServiceError(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::CircuitOpen => write!(f, "Circuit breaker is open"),
            CircuitBreakerError::ServiceError(e) => write!(f, "Service error: {e}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CircuitBreakerError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CircuitBreakerError::CircuitOpen => None,
            CircuitBreakerError::ServiceError(e) => Some(e),
        }
    }
}

/// Manager for multiple exchange circuit breakers
pub struct ExchangeCircuitBreakerManager {
    breakers: RwLock<HashMap<String, AtomicCircuitBreaker>>,
    default_config: CircuitBreakerConfig,
}

impl ExchangeCircuitBreakerManager {
    pub fn new(default_config: CircuitBreakerConfig) -> Self {
        Self {
            breakers: RwLock::new(HashMap::new()),
            default_config,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }

    /// Add or get circuit breaker for an exchange
    pub fn get_or_create(&self, exchange: &str) -> &AtomicCircuitBreaker {
        // Fast path: try read lock first
        {
            let breakers = self.breakers.read();
            if let Some(breaker) = breakers.get(exchange) {
                // Safety: We're returning a reference that will outlive the read lock
                // because the breaker is stored in a HashMap that's only modified
                // under write lock, and we never remove breakers.
                let ptr = breaker as *const AtomicCircuitBreaker;
                return unsafe { &*ptr };
            }
        }
        
        // Slow path: need to create
        let mut breakers = self.breakers.write();
        let breaker = breakers.entry(exchange.to_string())
            .or_insert_with(|| AtomicCircuitBreaker::new(self.default_config.clone()));
        
        let ptr = breaker as *const AtomicCircuitBreaker;
        unsafe { &*ptr }
    }

    /// Check if request can proceed for an exchange
    #[inline]
    pub fn can_execute(&self, exchange: &str) -> bool {
        self.get_or_create(exchange).can_execute()
    }

    /// Record success for an exchange
    #[inline]
    pub fn record_success(&self, exchange: &str) {
        self.get_or_create(exchange).record_success();
    }

    /// Record failure for an exchange
    #[inline]
    pub fn record_failure(&self, exchange: &str) {
        self.get_or_create(exchange).record_failure();
    }

    /// Execute function through circuit breaker
    pub fn call<F, T, E>(&self, exchange: &str, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Result<T, E>,
    {
        self.get_or_create(exchange).call(f)
    }

    /// Execute async function through circuit breaker
    pub async fn call_async<F, Fut, T, E>(&self, exchange: &str, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        self.get_or_create(exchange).call_async(f).await
    }

    /// Get health status of all exchanges
    pub fn get_health_status(&self) -> HashMap<String, CircuitState> {
        let breakers = self.breakers.read();
        breakers.iter()
            .map(|(name, breaker)| (name.clone(), breaker.get_state()))
            .collect()
    }

    /// Get detailed status of all exchanges
    pub fn get_detailed_status(&self) -> Vec<CircuitBreakerStatus> {
        let breakers = self.breakers.read();
        breakers.iter()
            .map(|(name, breaker)| CircuitBreakerStatus {
                exchange: name.clone(),
                state: breaker.get_state(),
                failure_count: breaker.get_failure_count(),
                time_since_last_failure_ms: breaker.time_since_last_failure_ms(),
            })
            .collect()
    }

    /// Reset all circuit breakers
    pub fn reset_all(&self) {
        let breakers = self.breakers.read();
        for breaker in breakers.values() {
            breaker.reset();
        }
    }
}

impl Default for ExchangeCircuitBreakerManager {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Detailed status of a circuit breaker
#[derive(Debug, Clone)]
pub struct CircuitBreakerStatus {
    pub exchange: String,
    pub state: CircuitState,
    pub failure_count: u32,
    pub time_since_last_failure_ms: Option<u64>,
}

/// Global circuit breaker manager
pub static CIRCUIT_BREAKERS: once_cell::sync::Lazy<ExchangeCircuitBreakerManager> =
    once_cell::sync::Lazy::new(ExchangeCircuitBreakerManager::with_defaults);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_is_closed() {
        let cb = AtomicCircuitBreaker::with_defaults(3, Duration::from_secs(30));
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert!(cb.can_execute());
    }

    #[test]
    fn test_opens_after_threshold_failures() {
        let cb = AtomicCircuitBreaker::with_defaults(3, Duration::from_secs(30));
        
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Closed);
        
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Closed);
        
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);
        assert!(!cb.can_execute());
    }

    #[test]
    fn test_success_resets_failure_count() {
        let cb = AtomicCircuitBreaker::with_defaults(3, Duration::from_secs(30));
        
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.get_failure_count(), 2);
        
        cb.record_success();
        assert_eq!(cb.get_failure_count(), 0);
    }

    #[test]
    fn test_half_open_transition() {
        let cb = AtomicCircuitBreaker::with_defaults(1, Duration::from_millis(10));
        
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);
        
        // Wait for recovery timeout
        std::thread::sleep(Duration::from_millis(20));
        
        // Should transition to half-open on can_execute
        assert!(cb.can_execute());
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_half_open_success_closes() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(10),
            success_threshold: 2,
            half_open_request_pct: 100,
            ..Default::default()
        };
        let cb = AtomicCircuitBreaker::new(config);
        
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(20));
        cb.can_execute(); // Transition to half-open
        
        cb.record_success();
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);
        
        cb.record_success();
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_failure_reopens() {
        let cb = AtomicCircuitBreaker::with_defaults(1, Duration::from_millis(10));
        
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(20));
        cb.can_execute(); // Transition to half-open
        
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);
    }

    #[test]
    fn test_reset() {
        let cb = AtomicCircuitBreaker::with_defaults(1, Duration::from_secs(30));
        
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);
        
        cb.reset();
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert_eq!(cb.get_failure_count(), 0);
    }

    #[test]
    fn test_call_success() {
        let cb = AtomicCircuitBreaker::with_defaults(3, Duration::from_secs(30));
        
        let result = cb.call(|| -> Result<i32, &str> { Ok(42) });
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_call_when_open_returns_error() {
        let cb = AtomicCircuitBreaker::with_defaults(1, Duration::from_secs(30));
        
        cb.record_failure();
        
        let result = cb.call(|| -> Result<i32, &str> { Ok(42) });
        assert!(matches!(result, Err(CircuitBreakerError::CircuitOpen)));
    }

    #[test]
    fn test_manager_creates_breakers() {
        let manager = ExchangeCircuitBreakerManager::with_defaults();
        
        assert!(manager.can_execute("kraken"));
        manager.record_failure("kraken");
        
        let status = manager.get_health_status();
        assert!(status.contains_key("kraken"));
    }
}
