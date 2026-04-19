//! Circuit Breaker v1 (Legacy)
//!
//! # ⚠️ DEPRECATED
//!
//! This module uses `std::sync::Mutex` which can panic if a thread panics while
//! holding the lock. Use `circuit_breaker_v2` instead, which uses atomic operations.
//!
//! ```rust,ignore
//! // Instead of:
//! use executionhandler::circuit_breaker::CircuitBreaker;
//!
//! // Use:
//! use executionhandler::circuit_breaker_v2::CircuitBreakerManager;
//! ```

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::collections::HashMap;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed,   // Normal operation
    Open,     // Failing - reject requests
    HalfOpen, // Testing if service recovered
}

/// Circuit breaker for managing service failures
///
/// # Deprecated
///
/// Use `circuit_breaker_v2::CircuitBreakerManager` instead.
#[deprecated(since = "0.1.0", note = "Use CircuitBreakerManager from circuit_breaker_v2 module - uses atomics instead of Mutex")]
pub struct CircuitBreaker {
    state: Arc<Mutex<CircuitState>>,
    failure_count: Arc<Mutex<u32>>,
    last_failure_time: Arc<Mutex<Option<Instant>>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    success_threshold: u32, // Number of successes needed to close circuit
    half_open_successes: Arc<Mutex<u32>>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(CircuitState::Closed)),
            failure_count: Arc::new(Mutex::new(0)),
            last_failure_time: Arc::new(Mutex::new(None)),
            failure_threshold,
            recovery_timeout,
            success_threshold: 3, // Require 3 successes to fully recover
            half_open_successes: Arc::new(Mutex::new(0)),
        }
    }

    pub fn call<F, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Result<T, E>,
    {
        // Check if circuit is open
        if self.is_open() {
            return Err(CircuitBreakerError::CircuitOpen);
        }

        // Execute the function
        match f() {
            Ok(result) => {
                self.on_success();
                Ok(result)
            }
            Err(error) => {
                self.on_failure();
                Err(CircuitBreakerError::ServiceError(error))
            }
        }
    }

    pub async fn call_async<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        // Check if circuit is open
        if self.is_open() {
            return Err(CircuitBreakerError::CircuitOpen);
        }

        // Execute the async function
        match f().await {
            Ok(result) => {
                self.on_success();
                Ok(result)
            }
            Err(error) => {
                self.on_failure();
                Err(CircuitBreakerError::ServiceError(error))
            }
        }
    }

    fn is_open(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        let _failure_count = *self.failure_count.lock().unwrap();
        let last_failure = *self.last_failure_time.lock().unwrap();

        match *state {
            CircuitState::Closed => false,
            CircuitState::Open => {
                // Check if we should transition to half-open
                if let Some(last_failure) = last_failure {
                    if last_failure.elapsed() > self.recovery_timeout {
                        *state = CircuitState::HalfOpen;
                        *self.half_open_successes.lock().unwrap() = 0;
                        false // Allow the request to proceed
                    } else {
                        true // Still in recovery period
                    }
                } else {
                    true
                }
            }
            CircuitState::HalfOpen => false, // Allow requests to test the service
        }
    }

    fn on_success(&self) {
        let mut state = self.state.lock().unwrap();
        
        match *state {
            CircuitState::Closed => {
                // Reset failure count on success
                *self.failure_count.lock().unwrap() = 0;
            }
            CircuitState::HalfOpen => {
                let mut successes = self.half_open_successes.lock().unwrap();
                *successes += 1;
                
                if *successes >= self.success_threshold {
                    // Service has recovered, close the circuit
                    *state = CircuitState::Closed;
                    *self.failure_count.lock().unwrap() = 0;
                    *self.last_failure_time.lock().unwrap() = None;
                    *successes = 0;
                }
            }
            CircuitState::Open => {
                // Shouldn't reach here, but handle gracefully
                *state = CircuitState::HalfOpen;
                *self.half_open_successes.lock().unwrap() = 1;
            }
        }
    }

    fn on_failure(&self) {
        let mut state = self.state.lock().unwrap();
        let mut failure_count = self.failure_count.lock().unwrap();
        let mut last_failure = self.last_failure_time.lock().unwrap();

        *failure_count += 1;
        *last_failure = Some(Instant::now());

        match *state {
            CircuitState::Closed => {
                if *failure_count >= self.failure_threshold {
                    *state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                // Failed during recovery, go back to open
                *state = CircuitState::Open;
                *self.half_open_successes.lock().unwrap() = 0;
            }
            CircuitState::Open => {
                // Already open, just update failure time
            }
        }
    }

    pub fn get_state(&self) -> CircuitState {
        *self.state.lock().unwrap()
    }

    pub fn get_failure_count(&self) -> u32 {
        *self.failure_count.lock().unwrap()
    }
}

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

/// Exchange circuit breaker manager
pub struct ExchangeCircuitBreakerManager {
    breakers: HashMap<String, CircuitBreaker>,
}

impl ExchangeCircuitBreakerManager {
    pub fn new() -> Self {
        Self {
            breakers: HashMap::new(),
        }
    }

    pub fn add_exchange(&mut self, exchange_name: String, failure_threshold: u32, recovery_timeout: Duration) {
        let breaker = CircuitBreaker::new(failure_threshold, recovery_timeout);
        self.breakers.insert(exchange_name, breaker);
    }

    pub fn call<F, T, E>(&self, exchange_name: &str, f: F) -> Result<T, String>
    where
        F: FnOnce() -> Result<T, E>,
        E: std::fmt::Debug,
    {
        if let Some(breaker) = self.breakers.get(exchange_name) {
            match breaker.call(f) {
                Ok(result) => Ok(result),
                Err(CircuitBreakerError::CircuitOpen) => {
                    Err(format!("Exchange {exchange_name} is unavailable (circuit breaker open)"))
                }
                Err(CircuitBreakerError::ServiceError(e)) => {
                    Err(format!("Exchange {exchange_name} error: {e:?}"))
                }
            }
        } else {
            Err(format!("No circuit breaker configured for exchange {exchange_name}"))
        }
    }

    pub async fn call_async<F, Fut, T, E>(&self, exchange_name: &str, f: F) -> Result<T, String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Debug,
    {
        if let Some(breaker) = self.breakers.get(exchange_name) {
            match breaker.call_async(f).await {
                Ok(result) => Ok(result),
                Err(CircuitBreakerError::CircuitOpen) => {
                    Err(format!("Exchange {exchange_name} is unavailable (circuit breaker open)"))
                }
                Err(CircuitBreakerError::ServiceError(e)) => {
                    Err(format!("Exchange {exchange_name} error: {e:?}"))
                }
            }
        } else {
            Err(format!("No circuit breaker configured for exchange {exchange_name}"))
        }
    }

    pub fn get_health_status(&self) -> HashMap<String, CircuitState> {
        self.breakers.iter()
            .map(|(name, breaker)| (name.clone(), breaker.get_state()))
            .collect()
    }
}

impl Default for ExchangeCircuitBreakerManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(deprecated)]
    fn make_breaker(threshold: u32, timeout_ms: u64) -> CircuitBreaker {
        CircuitBreaker::new(threshold, Duration::from_millis(timeout_ms))
    }

    // ========== CircuitBreaker state transitions ==========

    #[test]
    #[allow(deprecated)]
    fn test_initial_state_is_closed() {
        let cb = make_breaker(3, 1000);
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert_eq!(cb.get_failure_count(), 0);
    }

    #[test]
    #[allow(deprecated)]
    fn test_closed_to_open_after_threshold_failures() {
        let cb = make_breaker(3, 1000);
        for _ in 0..3 {
            let _ = cb.call(|| Err::<(), &str>("fail"));
        }
        assert_eq!(cb.get_state(), CircuitState::Open);
        assert_eq!(cb.get_failure_count(), 3);
    }

    #[test]
    #[allow(deprecated)]
    fn test_open_rejects_calls() {
        let cb = make_breaker(1, 60_000); // long timeout
        let _ = cb.call(|| Err::<(), &str>("fail")); // trip open
        assert_eq!(cb.get_state(), CircuitState::Open);

        let result = cb.call(|| Ok::<&str, &str>("should not run"));
        assert!(matches!(result, Err(CircuitBreakerError::CircuitOpen)));
    }

    #[test]
    #[allow(deprecated)]
    fn test_open_to_half_open_after_timeout() {
        let cb = make_breaker(1, 10); // 10ms timeout
        let _ = cb.call(|| Err::<(), &str>("fail")); // trip open
        assert_eq!(cb.get_state(), CircuitState::Open);

        std::thread::sleep(Duration::from_millis(20));

        // Next call should be allowed (transitions to HalfOpen internally)
        let result = cb.call(|| Ok::<&str, &str>("recovered"));
        assert!(result.is_ok());
        // After success in half-open, state depends on success_threshold (3)
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);
    }

    #[test]
    #[allow(deprecated)]
    fn test_half_open_to_closed_after_success_threshold() {
        let cb = make_breaker(1, 10);
        let _ = cb.call(|| Err::<(), &str>("fail")); // trip open
        std::thread::sleep(Duration::from_millis(20));

        // 3 successes needed (success_threshold = 3)
        for _ in 0..3 {
            let result = cb.call(|| Ok::<&str, &str>("ok"));
            assert!(result.is_ok());
        }
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert_eq!(cb.get_failure_count(), 0);
    }

    #[test]
    #[allow(deprecated)]
    fn test_half_open_to_open_on_failure() {
        let cb = make_breaker(1, 10);
        let _ = cb.call(|| Err::<(), &str>("fail")); // trip open
        std::thread::sleep(Duration::from_millis(20));

        // One success (enters half-open)
        let _ = cb.call(|| Ok::<&str, &str>("ok"));
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);

        // Failure in half-open → back to open
        let _ = cb.call(|| Err::<(), &str>("fail again"));
        assert_eq!(cb.get_state(), CircuitState::Open);
    }

    #[test]
    #[allow(deprecated)]
    fn test_success_resets_failure_count_in_closed() {
        let cb = make_breaker(3, 1000);
        let _ = cb.call(|| Err::<(), &str>("fail"));
        let _ = cb.call(|| Err::<(), &str>("fail"));
        assert_eq!(cb.get_failure_count(), 2);

        let _ = cb.call(|| Ok::<&str, &str>("success"));
        assert_eq!(cb.get_failure_count(), 0);
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    // ========== CircuitBreakerError ==========

    #[test]
    fn test_circuit_breaker_error_display() {
        let open_err: CircuitBreakerError<String> = CircuitBreakerError::CircuitOpen;
        assert_eq!(format!("{}", open_err), "Circuit breaker is open");

        let svc_err = CircuitBreakerError::ServiceError("connection refused".to_string());
        assert!(format!("{}", svc_err).contains("connection refused"));
    }

    // ========== ExchangeCircuitBreakerManager ==========

    #[test]
    #[allow(deprecated)]
    fn test_manager_independent_exchanges() {
        let mut mgr = ExchangeCircuitBreakerManager::new();
        mgr.add_exchange("kraken".into(), 2, Duration::from_secs(60));
        mgr.add_exchange("binance".into(), 2, Duration::from_secs(60));

        // Trip kraken
        let _ = mgr.call("kraken", || Err::<(), &str>("fail"));
        let _ = mgr.call("kraken", || Err::<(), &str>("fail"));

        // Kraken open, binance still closed
        let health = mgr.get_health_status();
        assert_eq!(health["kraken"], CircuitState::Open);
        assert_eq!(health["binance"], CircuitState::Closed);

        // Binance still works
        let result = mgr.call("binance", || Ok::<&str, &str>("ok"));
        assert!(result.is_ok());
    }

    #[test]
    #[allow(deprecated)]
    fn test_manager_unknown_exchange() {
        let mgr = ExchangeCircuitBreakerManager::new();
        let result = mgr.call("unknown", || Ok::<&str, &str>("ok"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No circuit breaker configured"));
    }

    #[test]
    #[allow(deprecated)]
    fn test_manager_default() {
        let mgr = ExchangeCircuitBreakerManager::default();
        assert!(mgr.get_health_status().is_empty());
    }
}
