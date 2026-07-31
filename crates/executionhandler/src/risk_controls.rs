//! Risk Controls Module - P0 Safety Features
//!
//! This module provides critical safety controls for live trading:
//! - Global kill switch
//! - Position limits
//! - Circuit breakers (drawdown, rate, daily loss)

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicI64, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

use crate::optimizations::timestamp::nano_timestamp;

// ============================================================================
// GLOBAL KILL SWITCH
// ============================================================================

/// Global kill switch - when triggered, ALL order submission stops immediately.
/// 
/// This is a static atomic that can be accessed from anywhere without passing references.
/// Usage: `if KILL_SWITCH.is_triggered() { return Err(...); }`
pub static KILL_SWITCH: KillSwitch = KillSwitch::new();

/// Thread-safe kill switch for emergency order halt
pub struct KillSwitch {
    triggered: AtomicBool,
    trigger_time_ns: AtomicU64,
    trigger_reason: AtomicU64, // Encoded reason
}

/// Reason codes for kill switch activation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum KillReason {
    Manual = 1,
    MaxDrawdown = 2,
    DailyLossLimit = 3,
    RateLimit = 4,
    PositionLimit = 5,
    SystemError = 6,
    ExchangeError = 7,
    Reconciliation = 8,
}

impl From<u64> for KillReason {
    fn from(v: u64) -> Self {
        match v {
            1 => KillReason::Manual,
            2 => KillReason::MaxDrawdown,
            3 => KillReason::DailyLossLimit,
            4 => KillReason::RateLimit,
            5 => KillReason::PositionLimit,
            6 => KillReason::SystemError,
            7 => KillReason::ExchangeError,
            8 => KillReason::Reconciliation,
            _ => KillReason::Manual,
        }
    }
}

impl KillSwitch {
    /// Create new kill switch (not triggered)
    pub const fn new() -> Self {
        Self {
            triggered: AtomicBool::new(false),
            trigger_time_ns: AtomicU64::new(0),
            trigger_reason: AtomicU64::new(0),
        }
    }

    /// Check if kill switch is triggered (MUST be called before every order)
    #[inline(always)]
    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::Acquire)
    }

    /// Trigger the kill switch - stops ALL trading immediately.
    /// Optionally persists the event to the database (fire-and-forget).
    pub fn trigger(&self, reason: KillReason) {
        let was_triggered = self.triggered.swap(true, Ordering::SeqCst);
        if !was_triggered {
            self.trigger_time_ns.store(nano_timestamp() as u64, Ordering::Release);
            self.trigger_reason.store(reason as u64, Ordering::Release);
            log::error!("🚨 KILL SWITCH TRIGGERED: {:?} - ALL TRADING HALTED", reason);
        }
    }

    /// Trigger the kill switch AND fire-and-forget a DB persistence event.
    #[cfg(feature = "postgres")]
    pub fn trigger_and_persist(
        &self,
        reason: KillReason,
        pool: Arc<smartorderrouter::DbPool>,
        tenant_id: uuid::Uuid,
    ) {
        self.trigger(reason);
        let reason_str = format!("{:?}", reason);
        tokio::spawn(async move {
            match pool.get().await {
                Ok(mut conn) => {
                    if let Err(e) = databaseschema::ops::kill_switch_event_ops::record_trigger(
                        &mut conn,
                        tenant_id,
                        &reason_str,
                        None,
                    ).await {
                        log::error!("[KILL-SWITCH] Failed to persist trigger event: {}", e);
                    }
                }
                Err(e) => {
                    log::error!("[KILL-SWITCH] Failed to get DB connection for trigger persist: {}", e);
                }
            }
        });
    }

    /// Reset the kill switch (requires manual intervention)
    /// Only call this after investigating and resolving the issue!
    pub fn reset(&self) -> bool {
        let was_triggered = self.triggered.swap(false, Ordering::SeqCst);
        if was_triggered {
            log::warn!("⚠️ Kill switch manually reset - trading can resume");
        }
        was_triggered
    }

    /// Reset the kill switch AND persist the reset event to the DB.
    #[cfg(feature = "postgres")]
    pub fn reset_and_persist(
        &self,
        pool: Arc<smartorderrouter::DbPool>,
        tenant_id: uuid::Uuid,
        notes: Option<String>,
    ) -> bool {
        let was_triggered = self.reset();
        if was_triggered {
            tokio::spawn(async move {
                match pool.get().await {
                    Ok(mut conn) => {
                        if let Err(e) = databaseschema::ops::kill_switch_event_ops::record_reset(
                            &mut conn,
                            tenant_id,
                            notes.as_deref(),
                        ).await {
                            log::error!("[KILL-SWITCH] Failed to persist reset event: {}", e);
                        }
                    }
                    Err(e) => {
                        log::error!("[KILL-SWITCH] Failed to get DB connection for reset persist: {}", e);
                    }
                }
            });
        }
        was_triggered
    }

    /// On startup, check if there is an outstanding (un-reset) kill-switch event
    /// in the DB and re-arm the in-memory kill switch accordingly.
    #[cfg(feature = "postgres")]
    pub async fn check_startup_state(
        &self,
        pool: &smartorderrouter::DbPool,
        tenant_id: uuid::Uuid,
    ) -> Result<bool, String> {
        let mut conn = pool.get().await.map_err(|e| format!("DB connection error: {}", e))?;
        match databaseschema::ops::kill_switch_event_ops::has_active_trigger(&mut conn, tenant_id).await {
            Ok(Some(event)) => {
                log::warn!(
                    "🚨 Kill switch was triggered before shutdown (reason: {}, at: {}). Re-arming.",
                    event.reason,
                    event.triggered_at
                );
                self.triggered.store(true, Ordering::SeqCst);
                // Try to map the reason string back
                let reason = match event.reason.as_str() {
                    "Manual" => KillReason::Manual,
                    "MaxDrawdown" => KillReason::MaxDrawdown,
                    "DailyLossLimit" => KillReason::DailyLossLimit,
                    "RateLimit" => KillReason::RateLimit,
                    "PositionLimit" => KillReason::PositionLimit,
                    "SystemError" => KillReason::SystemError,
                    "ExchangeError" => KillReason::ExchangeError,
                    "Reconciliation" => KillReason::Reconciliation,
                    _ => KillReason::Manual,
                };
                self.trigger_reason.store(reason as u64, Ordering::Release);
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(e) => Err(format!("Failed to query kill switch state: {}", e)),
        }
    }

    /// Get the reason for trigger (if triggered)
    pub fn get_trigger_reason(&self) -> Option<KillReason> {
        if self.is_triggered() {
            Some(KillReason::from(self.trigger_reason.load(Ordering::Acquire)))
        } else {
            None
        }
    }

    /// Get timestamp when triggered
    pub fn get_trigger_time(&self) -> Option<u64> {
        if self.is_triggered() {
            Some(self.trigger_time_ns.load(Ordering::Acquire))
        } else {
            None
        }
    }
}

// ============================================================================
// POSITION LIMITS
// ============================================================================

/// Position limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionLimits {
    /// Maximum position size per symbol (in base currency units)
    pub max_position_size: f64,
    /// Maximum position value per symbol (in quote currency, e.g., USD)
    pub max_position_value: f64,
    /// Maximum total portfolio value across all positions
    pub max_portfolio_value: f64,
    /// Maximum number of open positions
    pub max_open_positions: usize,
    /// Maximum order size (single order)
    pub max_order_size: f64,
    /// Maximum order value (single order)
    pub max_order_value: f64,
}

impl Default for PositionLimits {
    fn default() -> Self {
        Self {
            max_position_size: 10.0,        // 10 BTC max per symbol
            max_position_value: 500_000.0,  // $500k max per position
            max_portfolio_value: 2_000_000.0, // $2M total
            max_open_positions: 20,
            max_order_size: 1.0,            // 1 BTC max per order
            max_order_value: 50_000.0,      // $50k max per order
        }
    }
}

/// Position limit checker with current state tracking
pub struct PositionLimitChecker {
    limits: PositionLimits,
    /// Current positions: (symbol, exchange) -> (quantity, value)
    positions: Arc<RwLock<HashMap<(String, String), (f64, f64)>>>,
    /// Pairs-trading support: maps a leg's `(symbol, exchange)` key to its
    /// sibling leg's key when the two are registered as one hedged pair (see
    /// `register_pair_link`). A pair's two legs are opposite-signed by
    /// construction (long one, short the other), so summing their *signed*
    /// notional nets to roughly zero for a well-hedged pair -- treating them
    /// as two independent gross exposures (the default behavior for any
    /// unregistered position) would double-count a hedge as risk instead of
    /// recognizing it reduces risk, which is backwards for a pairs strategy.
    pair_links: Arc<RwLock<HashMap<(String, String), (String, String)>>>,
}

impl PositionLimitChecker {
    pub fn new(limits: PositionLimits) -> Self {
        Self {
            limits,
            positions: Arc::new(RwLock::new(HashMap::new())),
            pair_links: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register `leg_a` and `leg_b` as the two sibling legs of one hedged
    /// pairs-trading position, so portfolio-value exposure nets them instead
    /// of summing their gross values. Idempotent -- safe to call again for
    /// the same pair (e.g. on every entry) with no effect beyond overwriting
    /// the same link.
    pub async fn register_pair_link(&self, leg_a: (String, String), leg_b: (String, String)) {
        let mut links = self.pair_links.write().await;
        links.insert(leg_a.clone(), leg_b.clone());
        links.insert(leg_b, leg_a);
    }

    /// Remove a pair link (e.g. once the pair position is fully closed) --
    /// both legs revert to being treated as independent gross exposures.
    pub async fn unregister_pair_link(&self, leg_a: &(String, String)) {
        let mut links = self.pair_links.write().await;
        if let Some(leg_b) = links.remove(leg_a) {
            links.remove(&leg_b);
        }
    }

    /// Check if an order would violate position limits
    /// Returns Ok(()) if order is allowed, Err with reason if blocked
    pub async fn check_order(
        &self,
        symbol: &str,
        exchange: &str,
        order_quantity: f64,
        order_price: f64,
        is_buy: bool,
    ) -> Result<(), PositionLimitError> {
        // Check kill switch first
        if KILL_SWITCH.is_triggered() {
            return Err(PositionLimitError::KillSwitchActive);
        }

        let order_value = order_quantity.abs() * order_price;

        // Check single order limits
        if order_quantity.abs() > self.limits.max_order_size {
            return Err(PositionLimitError::OrderSizeTooLarge {
                requested: order_quantity.abs(),
                limit: self.limits.max_order_size,
            });
        }

        if order_value > self.limits.max_order_value {
            return Err(PositionLimitError::OrderValueTooLarge {
                requested: order_value,
                limit: self.limits.max_order_value,
            });
        }

        // Check position limits
        let positions = self.positions.read().await;
        let key = (symbol.to_string(), exchange.to_string());
        
        let (current_qty, _current_value) = positions.get(&key).copied().unwrap_or((0.0, 0.0));
        
        // Calculate new position after order
        let new_qty = if is_buy {
            current_qty + order_quantity
        } else {
            current_qty - order_quantity
        };
        let new_value = new_qty.abs() * order_price;

        // Check position size limit
        if new_qty.abs() > self.limits.max_position_size {
            return Err(PositionLimitError::PositionSizeTooLarge {
                symbol: symbol.to_string(),
                current: current_qty,
                requested_change: if is_buy { order_quantity } else { -order_quantity },
                limit: self.limits.max_position_size,
            });
        }

        // Check position value limit
        if new_value > self.limits.max_position_value {
            return Err(PositionLimitError::PositionValueTooLarge {
                symbol: symbol.to_string(),
                projected_value: new_value,
                limit: self.limits.max_position_value,
            });
        }

        // Check portfolio value limit. Candidate positions after this order:
        // every existing position, with `key`'s value replaced by `new_qty`/
        // `new_value` (order_price is this leg's own price, used for both
        // the new gross value and, when paired, the new signed notional).
        let mut candidate: HashMap<(String, String), (f64, f64)> = positions.clone();
        candidate.insert(key.clone(), (new_qty, new_value));
        let pair_links = self.pair_links.read().await;
        let total_value = netted_portfolio_exposure(&candidate, &pair_links);

        if total_value > self.limits.max_portfolio_value {
            return Err(PositionLimitError::PortfolioValueTooLarge {
                projected_value: total_value,
                limit: self.limits.max_portfolio_value,
            });
        }

        // Check max open positions
        let open_count = positions.iter()
            .filter(|(_, (qty, _))| qty.abs() > f64::EPSILON)
            .count();
        
        let would_open_new = current_qty.abs() < f64::EPSILON && new_qty.abs() > f64::EPSILON;
        
        if would_open_new && open_count >= self.limits.max_open_positions {
            return Err(PositionLimitError::TooManyPositions {
                current: open_count,
                limit: self.limits.max_open_positions,
            });
        }

        Ok(())
    }

    /// Update position after a fill
    pub async fn update_position(&self, symbol: &str, exchange: &str, quantity: f64, price: f64) {
        let mut positions = self.positions.write().await;
        let key = (symbol.to_string(), exchange.to_string());
        
        let (current_qty, _) = positions.get(&key).copied().unwrap_or((0.0, 0.0));
        let new_qty = current_qty + quantity;
        let new_value = new_qty.abs() * price;
        
        if new_qty.abs() < f64::EPSILON {
            positions.remove(&key);
        } else {
            positions.insert(key, (new_qty, new_value));
        }
    }

    /// Get current positions snapshot
    pub async fn get_positions(&self) -> HashMap<(String, String), (f64, f64)> {
        self.positions.read().await.clone()
    }

    /// Set positions (for reconciliation)
    pub async fn set_positions(&self, new_positions: HashMap<(String, String), (f64, f64)>) {
        let mut positions = self.positions.write().await;
        *positions = new_positions;
    }
}

/// Signed dollar notional of a `(qty, value)` position tuple. `value` is
/// always non-negative (`qty.abs() * price`), so the sign has to come from
/// `qty` — this recovers "long positions add exposure, short positions
/// subtract it" for the netting calculation below.
fn signed_notional(qty: f64, value: f64) -> f64 {
    if qty > 0.0 {
        value
    } else if qty < 0.0 {
        -value
    } else {
        0.0
    }
}

/// Total portfolio exposure across `positions`, netting any pair registered
/// in `pair_links` (summing the two legs' *signed* notional, not their gross
/// values) instead of summing every position's gross value independently.
/// A pair whose sibling leg isn't currently open (e.g. only one leg has
/// filled so far) falls back to gross for that leg — there's no actual
/// hedge in place yet to net against.
fn netted_portfolio_exposure(
    positions: &HashMap<(String, String), (f64, f64)>,
    pair_links: &HashMap<(String, String), (String, String)>,
) -> f64 {
    let mut visited: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    let mut total = 0.0;

    for (key, &(qty, value)) in positions.iter() {
        if visited.contains(key) {
            continue;
        }
        if let Some(sibling_key) = pair_links.get(key) {
            if let Some(&(sib_qty, sib_value)) = positions.get(sibling_key) {
                let net = (signed_notional(qty, value) + signed_notional(sib_qty, sib_value)).abs();
                total += net;
                visited.insert(key.clone());
                visited.insert(sibling_key.clone());
                continue;
            }
        }
        total += value;
        visited.insert(key.clone());
    }

    total
}

/// Position limit error types
#[derive(Debug, Clone)]
pub enum PositionLimitError {
    KillSwitchActive,
    OrderSizeTooLarge { requested: f64, limit: f64 },
    OrderValueTooLarge { requested: f64, limit: f64 },
    PositionSizeTooLarge { symbol: String, current: f64, requested_change: f64, limit: f64 },
    PositionValueTooLarge { symbol: String, projected_value: f64, limit: f64 },
    PortfolioValueTooLarge { projected_value: f64, limit: f64 },
    TooManyPositions { current: usize, limit: usize },
}

impl std::fmt::Display for PositionLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KillSwitchActive => write!(f, "Kill switch is active - all trading halted"),
            Self::OrderSizeTooLarge { requested, limit } => 
                write!(f, "Order size {} exceeds limit {}", requested, limit),
            Self::OrderValueTooLarge { requested, limit } => 
                write!(f, "Order value ${:.2} exceeds limit ${:.2}", requested, limit),
            Self::PositionSizeTooLarge { symbol, current, requested_change, limit } => 
                write!(f, "Position in {} would be {} (current {} + change {}), exceeds limit {}", 
                       symbol, current + requested_change, current, requested_change, limit),
            Self::PositionValueTooLarge { symbol, projected_value, limit } => 
                write!(f, "Position value in {} would be ${:.2}, exceeds limit ${:.2}", 
                       symbol, projected_value, limit),
            Self::PortfolioValueTooLarge { projected_value, limit } => 
                write!(f, "Portfolio value would be ${:.2}, exceeds limit ${:.2}", 
                       projected_value, limit),
            Self::TooManyPositions { current, limit } => 
                write!(f, "Already have {} open positions, limit is {}", current, limit),
        }
    }
}

impl std::error::Error for PositionLimitError {}

// ============================================================================
// CIRCUIT BREAKERS
// ============================================================================

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Maximum drawdown percentage before kill switch (0.10 = 10%)
    pub max_drawdown_pct: f64,
    /// Maximum daily loss in absolute value (USD)
    pub max_daily_loss: f64,
    /// Maximum orders per second (rate limit)
    pub max_orders_per_second: u32,
    /// Maximum orders per minute
    pub max_orders_per_minute: u32,
    /// Maximum consecutive failures before pause
    pub max_consecutive_failures: u32,
    /// Pause duration after failures (seconds)
    pub failure_pause_seconds: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            max_drawdown_pct: 0.05,       // 5% drawdown triggers kill
            max_daily_loss: 10_000.0,     // $10k daily loss limit
            max_orders_per_second: 10,    // 10 orders/sec max
            max_orders_per_minute: 300,   // 300 orders/min max
            max_consecutive_failures: 5,  // 5 failures in a row
            failure_pause_seconds: 60,    // 1 minute pause
        }
    }
}

/// Circuit breaker state and enforcement
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    
    // PnL tracking
    high_water_mark: AtomicI64,  // Stored as cents
    current_equity: AtomicI64,   // Stored as cents
    daily_pnl: AtomicI64,        // Stored as cents
    day_start_equity: AtomicI64, // Stored as cents
    
    // Rate limiting
    orders_last_second: AtomicU64,
    orders_last_minute: AtomicU64,
    second_window_start: AtomicU64,
    minute_window_start: AtomicU64,
    
    // Failure tracking
    consecutive_failures: AtomicU64,
    paused_until: AtomicU64,
    
    // State
    is_tripped: AtomicBool,
    trip_reason: AtomicU64,
}

/// Circuit breaker trip reasons
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u64)]
pub enum CircuitBreakerTrip {
    None = 0,
    MaxDrawdown = 1,
    DailyLossLimit = 2,
    RateLimitSecond = 3,
    RateLimitMinute = 4,
    ConsecutiveFailures = 5,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        let now = nano_timestamp() as u64;
        Self {
            config,
            high_water_mark: AtomicI64::new(0),
            current_equity: AtomicI64::new(0),
            daily_pnl: AtomicI64::new(0),
            day_start_equity: AtomicI64::new(0),
            orders_last_second: AtomicU64::new(0),
            orders_last_minute: AtomicU64::new(0),
            second_window_start: AtomicU64::new(now),
            minute_window_start: AtomicU64::new(now),
            consecutive_failures: AtomicU64::new(0),
            paused_until: AtomicU64::new(0),
            is_tripped: AtomicBool::new(false),
            trip_reason: AtomicU64::new(0),
        }
    }

    /// Initialize with starting equity
    pub fn initialize(&self, starting_equity: f64) {
        let equity_cents = (starting_equity * 100.0) as i64;
        self.high_water_mark.store(equity_cents, Ordering::Release);
        self.current_equity.store(equity_cents, Ordering::Release);
        self.day_start_equity.store(equity_cents, Ordering::Release);
        self.daily_pnl.store(0, Ordering::Release);
    }

    /// Check if trading is allowed (call before every order)
    #[inline]
    pub fn check_allowed(&self) -> Result<(), CircuitBreakerTrip> {
        // Check kill switch first
        if KILL_SWITCH.is_triggered() {
            return Err(CircuitBreakerTrip::MaxDrawdown); // Generic error
        }

        // Check if circuit breaker is tripped
        if self.is_tripped.load(Ordering::Acquire) {
            let reason = self.trip_reason.load(Ordering::Acquire);
            return Err(unsafe { std::mem::transmute(reason) });
        }

        // Check if in failure pause
        let now = nano_timestamp() as u64;
        let paused_until = self.paused_until.load(Ordering::Acquire);
        if now < paused_until {
            return Err(CircuitBreakerTrip::ConsecutiveFailures);
        }

        Ok(())
    }

    /// Record an order attempt (for rate limiting)
    pub fn record_order_attempt(&self) -> Result<(), CircuitBreakerTrip> {
        let now = nano_timestamp() as u64;
        
        // Update second window
        let second_start = self.second_window_start.load(Ordering::Relaxed);
        if now - second_start >= 1_000_000_000 { // 1 second in ns
            self.orders_last_second.store(1, Ordering::Relaxed);
            self.second_window_start.store(now, Ordering::Relaxed);
        } else {
            let orders = self.orders_last_second.fetch_add(1, Ordering::Relaxed) + 1;
            if orders > self.config.max_orders_per_second as u64 {
                self.trip(CircuitBreakerTrip::RateLimitSecond);
                return Err(CircuitBreakerTrip::RateLimitSecond);
            }
        }

        // Update minute window
        let minute_start = self.minute_window_start.load(Ordering::Relaxed);
        if now - minute_start >= 60_000_000_000 { // 60 seconds in ns
            self.orders_last_minute.store(1, Ordering::Relaxed);
            self.minute_window_start.store(now, Ordering::Relaxed);
        } else {
            let orders = self.orders_last_minute.fetch_add(1, Ordering::Relaxed) + 1;
            if orders > self.config.max_orders_per_minute as u64 {
                self.trip(CircuitBreakerTrip::RateLimitMinute);
                return Err(CircuitBreakerTrip::RateLimitMinute);
            }
        }

        Ok(())
    }

    /// Record a successful order (resets failure counter)
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
    }

    /// Record a failed order
    pub fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= self.config.max_consecutive_failures as u64 {
            let now = nano_timestamp() as u64;
            let pause_until = now + (self.config.failure_pause_seconds * 1_000_000_000);
            self.paused_until.store(pause_until, Ordering::Release);
            log::warn!("Circuit breaker: {} consecutive failures, pausing for {}s", 
                      failures, self.config.failure_pause_seconds);
        }
    }

    /// Update equity and check drawdown/daily loss
    pub fn update_equity(&self, new_equity: f64) {
        let equity_cents = (new_equity * 100.0) as i64;
        let _old_equity = self.current_equity.swap(equity_cents, Ordering::AcqRel);
        
        // Update high water mark
        let hwm = self.high_water_mark.load(Ordering::Acquire);
        if equity_cents > hwm {
            self.high_water_mark.store(equity_cents, Ordering::Release);
        }

        // Check drawdown
        let current_hwm = self.high_water_mark.load(Ordering::Acquire);
        if current_hwm > 0 {
            let drawdown = (current_hwm - equity_cents) as f64 / current_hwm as f64;
            if drawdown >= self.config.max_drawdown_pct {
                self.trip(CircuitBreakerTrip::MaxDrawdown);
                KILL_SWITCH.trigger(KillReason::MaxDrawdown);
                log::error!("🚨 MAX DRAWDOWN BREACHED: {:.2}% (limit: {:.2}%)", 
                           drawdown * 100.0, self.config.max_drawdown_pct * 100.0);
            }
        }

        // Update daily PnL
        let day_start = self.day_start_equity.load(Ordering::Acquire);
        let daily_pnl = equity_cents - day_start;
        self.daily_pnl.store(daily_pnl, Ordering::Release);

        // Check daily loss limit
        let daily_loss_cents = (self.config.max_daily_loss * 100.0) as i64;
        if daily_pnl < -daily_loss_cents {
            self.trip(CircuitBreakerTrip::DailyLossLimit);
            KILL_SWITCH.trigger(KillReason::DailyLossLimit);
            log::error!("🚨 DAILY LOSS LIMIT BREACHED: ${:.2} (limit: ${:.2})", 
                       daily_pnl as f64 / 100.0, self.config.max_daily_loss);
        }
    }

    /// Reset for new trading day
    pub fn reset_daily(&self) {
        let current = self.current_equity.load(Ordering::Acquire);
        self.day_start_equity.store(current, Ordering::Release);
        self.daily_pnl.store(0, Ordering::Release);
        self.orders_last_minute.store(0, Ordering::Relaxed);
        log::info!("Circuit breaker: Daily reset, starting equity: ${:.2}", 
                  current as f64 / 100.0);
    }

    /// Manually reset the circuit breaker
    pub fn reset(&self) {
        self.is_tripped.store(false, Ordering::SeqCst);
        self.trip_reason.store(0, Ordering::Release);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.paused_until.store(0, Ordering::Relaxed);
        log::warn!("Circuit breaker manually reset");
    }

    fn trip(&self, reason: CircuitBreakerTrip) {
        self.is_tripped.store(true, Ordering::SeqCst);
        self.trip_reason.store(reason as u64, Ordering::Release);
    }

    /// Get current status
    pub fn get_status(&self) -> CircuitBreakerStatus {
        CircuitBreakerStatus {
            is_tripped: self.is_tripped.load(Ordering::Acquire),
            trip_reason: if self.is_tripped.load(Ordering::Acquire) {
                Some(unsafe { std::mem::transmute(self.trip_reason.load(Ordering::Acquire)) })
            } else {
                None
            },
            current_equity: self.current_equity.load(Ordering::Acquire) as f64 / 100.0,
            high_water_mark: self.high_water_mark.load(Ordering::Acquire) as f64 / 100.0,
            daily_pnl: self.daily_pnl.load(Ordering::Acquire) as f64 / 100.0,
            drawdown_pct: {
                let hwm = self.high_water_mark.load(Ordering::Acquire);
                let current = self.current_equity.load(Ordering::Acquire);
                if hwm > 0 { (hwm - current) as f64 / hwm as f64 } else { 0.0 }
            },
            orders_last_second: self.orders_last_second.load(Ordering::Relaxed),
            orders_last_minute: self.orders_last_minute.load(Ordering::Relaxed),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
        }
    }
}

/// Circuit breaker status snapshot
#[derive(Debug, Clone, Serialize)]
pub struct CircuitBreakerStatus {
    pub is_tripped: bool,
    pub trip_reason: Option<CircuitBreakerTrip>,
    pub current_equity: f64,
    pub high_water_mark: f64,
    pub daily_pnl: f64,
    pub drawdown_pct: f64,
    pub orders_last_second: u64,
    pub orders_last_minute: u64,
    pub consecutive_failures: u64,
}

// ============================================================================
// UNIFIED RISK MANAGER
// ============================================================================

/// Unified risk manager combining all P0 controls
pub struct RiskManager {
    pub position_limits: PositionLimitChecker,
    pub circuit_breaker: CircuitBreaker,
}

impl RiskManager {
    pub fn new(position_limits: PositionLimits, circuit_breaker_config: CircuitBreakerConfig) -> Self {
        Self {
            position_limits: PositionLimitChecker::new(position_limits),
            circuit_breaker: CircuitBreaker::new(circuit_breaker_config),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(PositionLimits::default(), CircuitBreakerConfig::default())
    }

    /// Pre-trade check - MUST be called before submitting any order
    pub async fn pre_trade_check(
        &self,
        symbol: &str,
        exchange: &str,
        quantity: f64,
        price: f64,
        is_buy: bool,
    ) -> Result<(), RiskCheckError> {
        // 1. Check kill switch
        if KILL_SWITCH.is_triggered() {
            return Err(RiskCheckError::KillSwitch(
                KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual)
            ));
        }

        // 2. Check circuit breaker
        self.circuit_breaker.check_allowed()
            .map_err(RiskCheckError::CircuitBreaker)?;

        // 3. Record order attempt (rate limiting)
        self.circuit_breaker.record_order_attempt()
            .map_err(RiskCheckError::CircuitBreaker)?;

        // 4. Check position limits
        self.position_limits.check_order(symbol, exchange, quantity, price, is_buy).await
            .map_err(RiskCheckError::PositionLimit)?;

        Ok(())
    }

    /// Post-trade update - call after order fill
    pub async fn post_trade_update(
        &self,
        symbol: &str,
        exchange: &str,
        fill_quantity: f64,
        fill_price: f64,
        new_equity: f64,
        success: bool,
    ) {
        // Update position
        self.position_limits.update_position(symbol, exchange, fill_quantity, fill_price).await;

        // Update circuit breaker
        if success {
            self.circuit_breaker.record_success();
        } else {
            self.circuit_breaker.record_failure();
        }

        // Update equity tracking
        self.circuit_breaker.update_equity(new_equity);
    }
}

/// Unified risk check error
#[derive(Debug)]
pub enum RiskCheckError {
    KillSwitch(KillReason),
    CircuitBreaker(CircuitBreakerTrip),
    PositionLimit(PositionLimitError),
}

impl std::fmt::Display for RiskCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KillSwitch(reason) => write!(f, "Kill switch active: {:?}", reason),
            Self::CircuitBreaker(trip) => write!(f, "Circuit breaker tripped: {:?}", trip),
            Self::PositionLimit(err) => write!(f, "Position limit: {}", err),
        }
    }
}

impl std::error::Error for RiskCheckError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kill_switch() {
        // Reset first in case previous test left it triggered
        KILL_SWITCH.reset();
        
        assert!(!KILL_SWITCH.is_triggered());
        
        KILL_SWITCH.trigger(KillReason::Manual);
        assert!(KILL_SWITCH.is_triggered());
        assert_eq!(KILL_SWITCH.get_trigger_reason(), Some(KillReason::Manual));
        
        KILL_SWITCH.reset();
        assert!(!KILL_SWITCH.is_triggered());
    }

    #[tokio::test]
    async fn test_position_limits() {
        let limits = PositionLimits {
            max_order_size: 1.0,
            max_order_value: 50_000.0,
            max_position_size: 5.0,
            ..Default::default()
        };
        
        let checker = PositionLimitChecker::new(limits);
        
        // Reset kill switch for test
        KILL_SWITCH.reset();
        
        // Valid order should pass
        assert!(checker.check_order("BTC", "kraken", 0.5, 50000.0, true).await.is_ok());
        
        // Order too large should fail
        assert!(checker.check_order("BTC", "kraken", 2.0, 50000.0, true).await.is_err());
    }

    #[test]
    fn test_circuit_breaker_rate_limit() {
        let config = CircuitBreakerConfig {
            max_orders_per_second: 3,
            ..Default::default()
        };
        
        let cb = CircuitBreaker::new(config);
        
        // First 3 orders should pass
        assert!(cb.record_order_attempt().is_ok());
        assert!(cb.record_order_attempt().is_ok());
        assert!(cb.record_order_attempt().is_ok());
        
        // 4th order should fail rate limit
        assert!(matches!(
            cb.record_order_attempt(),
            Err(CircuitBreakerTrip::RateLimitSecond)
        ));
    }

    #[test]
    fn signed_notional_uses_qty_sign_not_value_sign() {
        assert_eq!(signed_notional(2.0, 100.0), 100.0);
        assert_eq!(signed_notional(-2.0, 100.0), -100.0);
        assert_eq!(signed_notional(0.0, 0.0), 0.0);
    }

    #[test]
    fn netted_exposure_nets_a_registered_pair_instead_of_summing_gross() {
        let mut positions = HashMap::new();
        // Long $10k of A, short $10k of B -- a well-hedged pair.
        positions.insert(("AAA".to_string(), "kraken".to_string()), (100.0, 10_000.0));
        positions.insert(("BBB".to_string(), "kraken".to_string()), (-50.0, 10_000.0));

        let mut links = HashMap::new();
        links.insert(("AAA".to_string(), "kraken".to_string()), ("BBB".to_string(), "kraken".to_string()));
        links.insert(("BBB".to_string(), "kraken".to_string()), ("AAA".to_string(), "kraken".to_string()));

        let net = netted_portfolio_exposure(&positions, &links);
        assert!(net.abs() < 1e-9, "expected ~0 net exposure for a perfectly offsetting pair, got {}", net);
    }

    #[test]
    fn netted_exposure_falls_back_to_gross_for_unlinked_positions() {
        let mut positions = HashMap::new();
        positions.insert(("AAA".to_string(), "kraken".to_string()), (100.0, 10_000.0));
        positions.insert(("CCC".to_string(), "kraken".to_string()), (50.0, 5_000.0));

        let links = HashMap::new();
        let total = netted_portfolio_exposure(&positions, &links);
        assert!((total - 15_000.0).abs() < 1e-9, "expected gross sum 15000, got {}", total);
    }

    #[test]
    fn netted_exposure_uses_gross_for_a_pair_whose_sibling_leg_is_not_yet_open() {
        let mut positions = HashMap::new();
        positions.insert(("AAA".to_string(), "kraken".to_string()), (100.0, 10_000.0));
        // BBB not in positions -- sibling hasn't filled yet.

        let mut links = HashMap::new();
        links.insert(("AAA".to_string(), "kraken".to_string()), ("BBB".to_string(), "kraken".to_string()));

        let total = netted_portfolio_exposure(&positions, &links);
        assert!((total - 10_000.0).abs() < 1e-9, "expected gross fallback 10000, got {}", total);
    }

    #[tokio::test]
    async fn check_order_allows_a_hedged_pair_that_would_exceed_gross_portfolio_limit() {
        // max_portfolio_value is set BELOW what the two legs would sum to
        // gross ($20k), but well above their netted (~$0) exposure -- an
        // order completing the hedge should be allowed once pair-linked.
        let limits = PositionLimits {
            max_order_size: 1000.0,
            max_order_value: 50_000.0,
            max_position_size: 1000.0,
            max_position_value: 50_000.0,
            max_portfolio_value: 12_000.0,
            max_open_positions: 10,
        };
        let checker = PositionLimitChecker::new(limits);
        KILL_SWITCH.reset();

        let leg_a = ("AAA".to_string(), "kraken".to_string());
        let leg_b = ("BBB".to_string(), "kraken".to_string());
        checker.register_pair_link(leg_a.clone(), leg_b.clone()).await;

        // Open leg A: long $10k.
        checker.check_order("AAA", "kraken", 100.0, 100.0, true).await.unwrap();
        checker.update_position("AAA", "kraken", 100.0, 100.0).await;

        // Opening leg B (short $10k) would push gross exposure to $20k --
        // over the $12k limit -- but nets to ~$0 once linked, so it should
        // still be allowed.
        let result = checker.check_order("BBB", "kraken", 100.0, 100.0, false).await;
        assert!(result.is_ok(), "expected hedged pair leg to be allowed, got {:?}", result);
    }
}
