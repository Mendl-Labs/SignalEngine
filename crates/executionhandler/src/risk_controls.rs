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

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new()
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
        // No tenant concept in the OSS schema; kept in the signature for
        // call-site compatibility with the private codebase this was forked
        // from.
        _tenant_id: uuid::Uuid,
    ) {
        self.trigger(reason);
        let reason_str = format!("{:?}", reason);
        tokio::spawn(async move {
            match pool.get().await {
                Ok(mut conn) => {
                    if let Err(e) = databaseschema::ops::kill_switch_event_ops::record_trigger(
                        &mut conn,
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
        _tenant_id: uuid::Uuid,
        notes: Option<String>,
    ) -> bool {
        let was_triggered = self.reset();
        if was_triggered {
            tokio::spawn(async move {
                match pool.get().await {
                    Ok(mut conn) => {
                        if let Err(e) = databaseschema::ops::kill_switch_event_ops::record_reset(
                            &mut conn,
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
        _tenant_id: uuid::Uuid,
    ) -> Result<bool, String> {
        let mut conn = pool.get().await.map_err(|e| format!("DB connection error: {}", e))?;
        match databaseschema::ops::kill_switch_event_ops::has_active_trigger(&mut conn).await {
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

/// Current positions keyed by (symbol, exchange): (quantity, value).
type PositionMap = HashMap<(String, String), (f64, f64)>;
/// Pair links keyed by (symbol, exchange) to the sibling leg's key.
type PairLinkMap = HashMap<(String, String), (String, String)>;

/// Position limit checker with current state tracking
pub struct PositionLimitChecker {
    limits: PositionLimits,
    /// Current positions: (symbol, exchange) -> (quantity, value)
    positions: Arc<RwLock<PositionMap>>,
    /// Pairs-trading support: maps a leg's `(symbol, exchange)` key to its
    /// sibling leg's key when the two are registered as one hedged pair (see
    /// `register_pair_link`). A pair's two legs are opposite-signed by
    /// construction (long one, short the other), so summing their *signed*
    /// notional nets to roughly zero for a well-hedged pair -- treating them
    /// as two independent gross exposures (the default behavior for any
    /// unregistered position) would double-count a hedge as risk instead of
    /// recognizing it reduces risk, which is backwards for a pairs strategy.
    pair_links: Arc<RwLock<PairLinkMap>>,
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

    /// Check order limits that don't require a reference price: kill switch,
    /// `max_order_size`, `max_position_size`, and `max_open_positions`.
    ///
    /// Used when no reference price can be resolved for an order (no
    /// signal price, no recorded market tick, no existing position to infer
    /// one from) -- deliberately skips the three dollar-denominated checks
    /// (`max_order_value`, `max_position_value`, `max_portfolio_value`)
    /// that `check_order` runs, rather than computing them against a
    /// fabricated price. Mirrors `check_order`'s quantity-based logic
    /// exactly, just without the `order_price`-dependent branches.
    pub async fn check_order_quantity_only(
        &self,
        symbol: &str,
        exchange: &str,
        order_quantity: f64,
        is_buy: bool,
    ) -> Result<(), PositionLimitError> {
        // Check kill switch first
        if KILL_SWITCH.is_triggered() {
            return Err(PositionLimitError::KillSwitchActive);
        }

        // Check single order size limit (quantity only -- no price to value it)
        if order_quantity.abs() > self.limits.max_order_size {
            return Err(PositionLimitError::OrderSizeTooLarge {
                requested: order_quantity.abs(),
                limit: self.limits.max_order_size,
            });
        }

        let positions = self.positions.read().await;
        let key = (symbol.to_string(), exchange.to_string());

        let (current_qty, _current_value) = positions.get(&key).copied().unwrap_or((0.0, 0.0));

        // Calculate new position after order
        let new_qty = if is_buy {
            current_qty + order_quantity
        } else {
            current_qty - order_quantity
        };

        // Check position size limit
        if new_qty.abs() > self.limits.max_position_size {
            return Err(PositionLimitError::PositionSizeTooLarge {
                symbol: symbol.to_string(),
                current: current_qty,
                requested_change: if is_buy { order_quantity } else { -order_quantity },
                limit: self.limits.max_position_size,
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
            return Err(unsafe { std::mem::transmute::<u64, CircuitBreakerTrip>(reason) });
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
                Some(unsafe { std::mem::transmute::<u64, CircuitBreakerTrip>(self.trip_reason.load(Ordering::Acquire)) })
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
// MARGIN USAGE (margin-traded venues only -- OANDA/FX today)
// ============================================================================

/// Margin-usage limit configuration. Only meaningful for margin-traded
/// venues -- mirrors `CircuitBreakerConfig.max_drawdown_pct`'s
/// fraction-of-equity convention, not `PositionLimits`' dollar fields,
/// because margin headroom scales with equity, not a fixed dollar cap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarginLimits {
    /// Fraction of account equity (NAV) that may be committed as margin
    /// before an order is rejected. 0.80 = reject once an order would push
    /// margin usage past 80% of equity.
    pub max_margin_utilization: f64,
}

impl Default for MarginLimits {
    fn default() -> Self {
        Self {
            max_margin_utilization: 0.80,
        }
    }
}

/// A single margin-usage data point fetched from a margin-traded venue's
/// account-summary endpoint (OANDA's `marginUsed`/`NAV`/`marginRate` today
/// -- see `GenericConnector::get_margin_snapshot`). Fetched out-of-band,
/// same as `get_equity()`/`mandate_risk_limits.rs`'s equity -- never on the
/// order hot path.
#[derive(Debug, Clone, Copy)]
pub struct MarginSnapshot {
    /// Account equity (NAV) in the account's margin currency.
    pub equity: f64,
    /// Margin currently committed to open positions, before any new order.
    pub margin_used: f64,
    /// `marginAvailable`, kept only for status/diagnostics -- NOT used in
    /// the utilization ratio (which uses `margin_used / equity` directly;
    /// OANDA defines `marginAvailable == NAV - marginUsed`, so the two are
    /// equivalent, but relying on `equity` directly is one fewer assumption
    /// about a field this codebase hasn't verified against a live account).
    pub margin_available: Option<f64>,
    /// Fraction of notional required as margin for this account (OANDA's
    /// account-level `marginRate`, e.g. 0.02 = 2% = 50:1). `None` if
    /// unavailable -- see `MarginUsageTracker::check_margin_usage` for how
    /// that's handled (never guessed).
    pub margin_rate: Option<f64>,
}

/// Margin-utilization pre-trade check. Atomics-based and lock-free like
/// `CircuitBreaker`, storing dollar fields as cents (`i64`) for the same
/// reason. Inert (`check_margin_usage` always `Ok(())`) until
/// `update_snapshot` is called at least once -- this is what keeps the
/// check a true no-op for venues that never report margin (Kraken/spot),
/// rather than a check with a permissive default.
pub struct MarginUsageTracker {
    limits: MarginLimits,
    equity_cents: AtomicI64,
    margin_used_cents: AtomicI64,
    /// Margin rate scaled to parts-per-million; `-1` is the "unknown" sentinel.
    margin_rate_ppm: AtomicI64,
    has_snapshot: AtomicBool,
}

impl MarginUsageTracker {
    pub fn new(limits: MarginLimits) -> Self {
        Self {
            limits,
            equity_cents: AtomicI64::new(0),
            margin_used_cents: AtomicI64::new(0),
            margin_rate_ppm: AtomicI64::new(-1),
            has_snapshot: AtomicBool::new(false),
        }
    }

    /// Record a fresh snapshot (overwrites any previous one). Call this
    /// out-of-band on a periodic refresh for margin-traded venues only --
    /// never from the order hot path, and never for Kraken/spot.
    pub fn update_snapshot(&self, snapshot: &MarginSnapshot) {
        self.equity_cents.store((snapshot.equity * 100.0).round() as i64, Ordering::Release);
        self.margin_used_cents.store((snapshot.margin_used * 100.0).round() as i64, Ordering::Release);
        self.margin_rate_ppm.store(
            snapshot.margin_rate.map(|r| (r * 1_000_000.0).round() as i64).unwrap_or(-1),
            Ordering::Release,
        );
        self.has_snapshot.store(true, Ordering::Release);
    }

    pub fn has_snapshot(&self) -> bool {
        self.has_snapshot.load(Ordering::Acquire)
    }

    /// Pure, synchronous check against the most recently recorded snapshot
    /// -- never makes a network call. `order_notional` is the proposed
    /// order's absolute dollar value (`quantity.abs() * reference_price`).
    ///
    /// This order's own incremental margin impact is estimated as
    /// `order_notional * margin_rate` when a rate was recorded in the
    /// snapshot; if no rate is available, the check conservatively
    /// evaluates only the CURRENTLY-committed margin (this order's own
    /// impact is NOT added) rather than inventing a rate -- so a breach can
    /// still be caught (current usage already over threshold), but a
    /// previously-safe account won't be rejected on a guessed number.
    ///
    /// Returns `Ok(())` -- a deliberate no-op, not "passed" -- if no
    /// snapshot has ever been recorded (see `has_snapshot`).
    pub fn check_margin_usage(&self, order_notional: f64) -> Result<(), MarginError> {
        if !self.has_snapshot.load(Ordering::Acquire) {
            return Ok(());
        }

        let equity = self.equity_cents.load(Ordering::Acquire) as f64 / 100.0;
        if equity <= 0.0 {
            return Err(MarginError::NoEquity);
        }

        let margin_used = self.margin_used_cents.load(Ordering::Acquire) as f64 / 100.0;
        let margin_rate_ppm = self.margin_rate_ppm.load(Ordering::Acquire);
        let order_margin_required = if margin_rate_ppm >= 0 {
            order_notional.abs() * (margin_rate_ppm as f64 / 1_000_000.0)
        } else {
            0.0
        };

        let projected_margin_used = margin_used + order_margin_required;
        let projected_utilization = projected_margin_used / equity;

        if projected_utilization > self.limits.max_margin_utilization {
            return Err(MarginError::UtilizationTooHigh {
                projected_utilization,
                limit: self.limits.max_margin_utilization,
                projected_margin_used,
                equity,
            });
        }

        Ok(())
    }

    /// Status snapshot for monitoring, mirrors `CircuitBreaker::get_status`.
    /// `None` if no snapshot has ever been recorded.
    pub fn get_status(&self) -> Option<MarginStatus> {
        if !self.has_snapshot.load(Ordering::Acquire) {
            return None;
        }
        let equity = self.equity_cents.load(Ordering::Acquire) as f64 / 100.0;
        let margin_used = self.margin_used_cents.load(Ordering::Acquire) as f64 / 100.0;
        Some(MarginStatus {
            equity,
            margin_used,
            utilization: if equity > 0.0 { margin_used / equity } else { f64::INFINITY },
            limit: self.limits.max_margin_utilization,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MarginStatus {
    pub equity: f64,
    pub margin_used: f64,
    pub utilization: f64,
    pub limit: f64,
}

/// Margin-usage error types.
#[derive(Debug, Clone)]
pub enum MarginError {
    /// Recorded equity is zero or negative -- can't compute a ratio.
    NoEquity,
    UtilizationTooHigh { projected_utilization: f64, limit: f64, projected_margin_used: f64, equity: f64 },
}

impl std::fmt::Display for MarginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoEquity => write!(f, "Cannot evaluate margin utilization: recorded equity is zero or negative"),
            Self::UtilizationTooHigh { projected_utilization, limit, projected_margin_used, equity } => write!(
                f,
                "Margin utilization would be {:.1}% (${:.2} used of ${:.2} equity), exceeds limit {:.1}%",
                projected_utilization * 100.0, projected_margin_used, equity, limit * 100.0
            ),
        }
    }
}

impl std::error::Error for MarginError {}

// ============================================================================
// PRE-TRADE GATE ENFORCE FLAG
// ============================================================================

/// Env var that switches the live pre-trade risk gate (`RiskManager::pre_trade_check`,
/// wired in at `execute_order_on_exchange`) from observing to acting.
pub const PRE_TRADE_GATE_ENFORCE_ENV: &str = "PRE_TRADE_RISK_GATE_ENFORCE";

/// Whether the pre-trade risk gate actually blocks an order.
///
/// Mirrors BacktestingEngine's `deployment_lifecycle_scheduler::LifecycleMode`
/// two-flag dark-by-default convention exactly.
///
/// `DryRun` (the default) runs `pre_trade_check` exactly as `Enforce` would,
/// logs what it WOULD do on a rejection, and increments a metric -- but
/// always lets the order through to the connector unchanged. It exists
/// because this is the first time any live order at this platform has ever
/// been risk-checked before submission; the dry-run window is where
/// `PositionLimits`/`CircuitBreakerConfig`'s placeholder defaults get
/// sanity-checked against real signal volume before they can ever block a
/// real order.
///
/// Only an explicit `true`/`1` (any case) enforces. Unset, empty, or
/// anything unparseable is `DryRun`, so a typo or a missing env var can only
/// ever make the gate MORE permissive, never less -- the safer failure mode
/// for a brand-new safety control with zero production history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreTradeGateMode {
    DryRun,
    Enforce,
}

impl PreTradeGateMode {
    pub fn from_env_value(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("true") | Some("1") => Self::Enforce,
            _ => Self::DryRun,
        }
    }

    pub fn from_env() -> Self {
        Self::from_env_value(std::env::var(PRE_TRADE_GATE_ENFORCE_ENV).ok().as_deref())
    }
}

// ============================================================================
// UNIFIED RISK MANAGER
// ============================================================================

/// Unified risk manager combining all P0 controls
pub struct RiskManager {
    pub position_limits: PositionLimitChecker,
    pub circuit_breaker: CircuitBreaker,
    pub margin: MarginUsageTracker,
}

impl RiskManager {
    pub fn new(position_limits: PositionLimits, circuit_breaker_config: CircuitBreakerConfig) -> Self {
        Self::with_margin_limits(position_limits, circuit_breaker_config, MarginLimits::default())
    }

    /// Same as `new`, but with an explicit margin-utilization limit instead
    /// of the 0.80 default (see `MarginLimits`).
    pub fn with_margin_limits(
        position_limits: PositionLimits,
        circuit_breaker_config: CircuitBreakerConfig,
        margin_limits: MarginLimits,
    ) -> Self {
        Self {
            position_limits: PositionLimitChecker::new(position_limits),
            circuit_breaker: CircuitBreaker::new(circuit_breaker_config),
            margin: MarginUsageTracker::new(margin_limits),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(PositionLimits::default(), CircuitBreakerConfig::default())
    }

    /// Record a fresh margin snapshot (out-of-band periodic refresh for a
    /// margin-traded venue). See `MarginUsageTracker::update_snapshot`.
    pub fn update_margin_snapshot(&self, snapshot: &MarginSnapshot) {
        self.margin.update_snapshot(snapshot);
    }

    /// Margin-utilization pre-trade check. ADDITIVE to `pre_trade_check`/
    /// `pre_trade_check_no_price` -- callers must still run one of those for
    /// kill-switch/circuit-breaker/position-limit coverage; this only adds
    /// the margin check, and is `Ok(())` for any venue with no margin
    /// snapshot ever recorded (Kraken/spot; see `MarginUsageTracker`).
    pub fn check_margin_usage(&self, order_notional: f64) -> Result<(), RiskCheckError> {
        if KILL_SWITCH.is_triggered() {
            return Err(RiskCheckError::KillSwitch(
                KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual)
            ));
        }
        self.margin.check_margin_usage(order_notional).map_err(RiskCheckError::MarginUsage)
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

    /// Pre-trade check for an order with no resolvable reference price
    /// (e.g. a market order with no signal price, no recorded market tick,
    /// and no existing position to infer a price from). Runs the kill
    /// switch, circuit breaker (including rate limiting), and the
    /// quantity-only position checks (`max_order_size`, `max_position_size`,
    /// `max_open_positions`, via `PositionLimitChecker::check_order_quantity_only`)
    /// -- deliberately skips the three dollar-denominated checks
    /// (`max_order_value`, `max_position_value`, `max_portfolio_value`)
    /// rather than inventing a fake price to compute them with. Callers
    /// should log a warning when taking this path instead of
    /// `pre_trade_check`.
    pub async fn pre_trade_check_no_price(
        &self,
        symbol: &str,
        exchange: &str,
        quantity: f64,
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

        // 4. Check quantity-only position limits (no price available)
        self.position_limits.check_order_quantity_only(symbol, exchange, quantity, is_buy).await
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
    MarginUsage(MarginError),
}

impl std::fmt::Display for RiskCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KillSwitch(reason) => write!(f, "Kill switch active: {:?}", reason),
            Self::CircuitBreaker(trip) => write!(f, "Circuit breaker tripped: {:?}", trip),
            Self::PositionLimit(err) => write!(f, "Position limit: {}", err),
            Self::MarginUsage(err) => write!(f, "Margin usage: {}", err),
        }
    }
}

impl RiskCheckError {
    /// Stable, low-cardinality label for the `risk_gate_rejections_total`
    /// metric -- deliberately coarser than `Display` (which embeds the
    /// specific trip/limit detail), so it stays a fixed, queryable set of
    /// values instead of growing a new label per symbol/trip variant.
    pub fn reason_label(&self) -> &'static str {
        match self {
            Self::KillSwitch(_) => "kill_switch",
            Self::CircuitBreaker(_) => "circuit_breaker",
            Self::PositionLimit(_) => "position_limit",
            Self::MarginUsage(_) => "margin_usage",
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

    #[test]
    fn pre_trade_gate_mode_defaults_to_dry_run() {
        assert_eq!(PreTradeGateMode::from_env_value(None), PreTradeGateMode::DryRun);
        assert_eq!(PreTradeGateMode::from_env_value(Some("")), PreTradeGateMode::DryRun);
        assert_eq!(PreTradeGateMode::from_env_value(Some("nonsense")), PreTradeGateMode::DryRun);
        assert_eq!(PreTradeGateMode::from_env_value(Some("false")), PreTradeGateMode::DryRun);
        assert_eq!(PreTradeGateMode::from_env_value(Some("0")), PreTradeGateMode::DryRun);
    }

    #[test]
    fn pre_trade_gate_mode_enforces_only_on_explicit_true_or_one() {
        assert_eq!(PreTradeGateMode::from_env_value(Some("true")), PreTradeGateMode::Enforce);
        assert_eq!(PreTradeGateMode::from_env_value(Some("TRUE")), PreTradeGateMode::Enforce);
        assert_eq!(PreTradeGateMode::from_env_value(Some("True")), PreTradeGateMode::Enforce);
        assert_eq!(PreTradeGateMode::from_env_value(Some("1")), PreTradeGateMode::Enforce);
        assert_eq!(PreTradeGateMode::from_env_value(Some(" true ")), PreTradeGateMode::Enforce);
    }

    #[tokio::test]
    async fn check_order_quantity_only_skips_dollar_checks_but_enforces_quantity_limits() {
        let limits = PositionLimits {
            max_order_size: 1.0,
            // Deliberately tiny -- if the dollar checks were NOT skipped,
            // this order (quantity 1.0 at an implied huge notional) would
            // need a price to even evaluate, let alone pass.
            max_order_value: 1.0,
            max_position_value: 1.0,
            max_portfolio_value: 1.0,
            max_position_size: 5.0,
            max_open_positions: 20,
        };
        let checker = PositionLimitChecker::new(limits);
        KILL_SWITCH.reset();

        // Within max_order_size/max_position_size -- should pass even though
        // the (skipped) dollar limits are absurdly low.
        assert!(checker.check_order_quantity_only("BTC", "kraken", 1.0, true).await.is_ok());

        // Exceeds max_order_size -- still enforced.
        assert!(checker.check_order_quantity_only("BTC", "kraken", 2.0, true).await.is_err());
    }

    #[tokio::test]
    async fn pre_trade_check_no_price_still_enforces_kill_switch() {
        let manager = RiskManager::with_defaults();
        KILL_SWITCH.trigger(KillReason::Manual);

        let result = manager.pre_trade_check_no_price("BTC", "kraken", 0.1, true).await;
        assert!(matches!(result, Err(RiskCheckError::KillSwitch(_))));

        KILL_SWITCH.reset();
    }

    fn oanda_snapshot() -> MarginSnapshot {
        // Matches crates/broker-adapters/tests/fixtures/oanda/account_summary_short_pos.json
        MarginSnapshot { equity: 49_800.0, margin_used: 2_500.0, margin_available: Some(47_300.0), margin_rate: Some(0.02) }
    }

    #[test]
    fn margin_check_allows_order_within_threshold() {
        let tracker = MarginUsageTracker::new(MarginLimits::default()); // 80%
        tracker.update_snapshot(&oanda_snapshot());

        // 1,000,000-unit FX order at 2% margin rate -> +$20,000 margin.
        // Projected: (2500 + 20000) / 49800 = 45.2% < 80%.
        assert!(tracker.check_margin_usage(1_000_000.0).is_ok());
    }

    #[test]
    fn margin_check_rejects_order_that_breaches_threshold() {
        let tracker = MarginUsageTracker::new(MarginLimits::default()); // 80%
        tracker.update_snapshot(&oanda_snapshot());

        // 2,500,000-unit FX order at 2% margin rate -> +$50,000 margin.
        // Projected: (2500 + 50000) / 49800 = 105.4% > 80% -> rejected.
        let result = tracker.check_margin_usage(2_500_000.0);
        assert!(matches!(result, Err(MarginError::UtilizationTooHigh { .. })), "{:?}", result);
    }

    #[test]
    fn margin_check_is_a_no_op_before_any_snapshot_is_recorded() {
        // Mirrors a spot venue (Kraken) that never calls update_snapshot.
        let tracker = MarginUsageTracker::new(MarginLimits::default());
        assert!(tracker.check_margin_usage(10_000_000.0).is_ok());
        assert!(!tracker.has_snapshot());
    }

    #[test]
    fn margin_check_falls_back_to_current_utilization_when_rate_unknown() {
        let tracker = MarginUsageTracker::new(MarginLimits::default()); // 80%
        tracker.update_snapshot(&MarginSnapshot {
            equity: 49_800.0, margin_used: 45_000.0, margin_available: None, margin_rate: None,
        });
        // Current utilization already 90.4% > 80%: rejected even though this
        // specific order's own impact couldn't be added (no rate).
        let result = tracker.check_margin_usage(1_000_000.0);
        assert!(matches!(result, Err(MarginError::UtilizationTooHigh { .. })), "{:?}", result);
    }

    #[test]
    fn margin_check_via_risk_manager_still_enforces_kill_switch() {
        let manager = RiskManager::with_defaults();
        manager.update_margin_snapshot(&oanda_snapshot());
        KILL_SWITCH.trigger(KillReason::Manual);

        let result = manager.check_margin_usage(1_000_000.0);
        assert!(matches!(result, Err(RiskCheckError::KillSwitch(_))));

        KILL_SWITCH.reset();
    }
}
