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
        let failure_count = *self.failure_count.lock().unwrap();
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
