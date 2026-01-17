//! Fat-Finger Protection Module
//!
//! Prevents catastrophic trading errors by validating order prices
//! against current market conditions before submission.
//!
//! # Features
//!
//! - **Price Deviation Check**: Reject orders > X% from mid/last price
//! - **Notional Limits**: Reject orders exceeding max notional value
//! - **Concentration Limits**: Prevent over-concentration in single asset
//! - **Velocity Checks**: Rate-limit rapid order bursts
//! - **Configurable Thresholds**: Per-symbol and exchange overrides
//!
//! # Example
//!
//! ```rust,ignore
//! use executionhandler::fat_finger::{FatFingerGuard, FatFingerConfig};
//!
//! let guard = FatFingerGuard::new(FatFingerConfig::default());
//!
//! // Update market data
//! guard.update_market_price("BTC-USD", 50000.0, 50001.0);
//!
//! // Validate order before submission
//! match guard.validate_order(&signal) {
//!     Ok(_) => execute_order(signal),
//!     Err(e) => {
//!         log::error!("Fat-finger protection triggered: {}", e);
//!         // Order blocked
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::signal::{Signal, SignalAction};

/// Configuration for fat-finger protection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FatFingerConfig {
    /// Maximum price deviation from mid-price (as percentage, e.g., 5.0 = 5%)
    pub max_price_deviation_pct: f64,
    /// Maximum single order notional value (USD)
    pub max_order_notional_usd: f64,
    /// Maximum position notional per symbol (USD)
    pub max_position_notional_usd: f64,
    /// Maximum concentration in single asset (percentage of portfolio)
    pub max_concentration_pct: f64,
    /// Maximum orders per second per symbol
    pub max_orders_per_second: u32,
    /// Maximum orders per minute per symbol
    pub max_orders_per_minute: u32,
    /// Minimum time between orders for same symbol (milliseconds)
    pub min_order_interval_ms: u64,
    /// Whether to block orders when no market price is available
    pub require_market_price: bool,
    /// Price staleness threshold (milliseconds)
    pub price_staleness_ms: u64,
    /// Symbol-specific overrides
    pub symbol_overrides: HashMap<String, SymbolOverride>,
    /// Whether fat-finger protection is enabled
    pub enabled: bool,
}

impl Default for FatFingerConfig {
    fn default() -> Self {
        Self {
            max_price_deviation_pct: 5.0,     // 5% max deviation from market
            max_order_notional_usd: 100_000.0, // $100k max single order
            max_position_notional_usd: 1_000_000.0, // $1M max position
            max_concentration_pct: 25.0,      // 25% max concentration
            max_orders_per_second: 10,
            max_orders_per_minute: 100,
            min_order_interval_ms: 50,        // 50ms minimum between orders
            require_market_price: true,
            price_staleness_ms: 5000,         // 5 second staleness
            symbol_overrides: HashMap::new(),
            enabled: true,
        }
    }
}

/// Symbol-specific override configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolOverride {
    pub max_price_deviation_pct: Option<f64>,
    pub max_order_notional_usd: Option<f64>,
    pub max_position_notional_usd: Option<f64>,
}

/// Market price data for a symbol
#[derive(Debug, Clone)]
struct MarketPrice {
    /// Best bid price
    bid: f64,
    /// Best ask price
    ask: f64,
    /// Mid-price
    mid: f64,
    /// Last trade price
    last: Option<f64>,
    /// Update timestamp
    updated_at: Instant,
}

/// Order velocity tracking per symbol
struct OrderVelocity {
    /// Orders in current second
    orders_this_second: AtomicU64,
    /// Current second timestamp
    current_second: AtomicU64,
    /// Orders in current minute
    orders_this_minute: AtomicU64,
    /// Current minute timestamp  
    current_minute: AtomicU64,
    /// Last order timestamp (nanoseconds)
    last_order_ns: AtomicU64,
}

impl OrderVelocity {
    fn new() -> Self {
        Self {
            orders_this_second: AtomicU64::new(0),
            current_second: AtomicU64::new(0),
            orders_this_minute: AtomicU64::new(0),
            current_minute: AtomicU64::new(0),
            last_order_ns: AtomicU64::new(0),
        }
    }
}

/// Position tracking for concentration limits
#[derive(Debug, Clone, Default)]
struct PositionInfo {
    /// Current position quantity
    quantity: f64,
    /// Current position notional value (USD)
    notional_usd: f64,
    /// Average entry price
    avg_price: f64,
}

/// Fat-finger protection result
#[derive(Debug, Clone)]
pub enum FatFingerResult {
    /// Order passed all checks
    Allowed,
    /// Order blocked with reason
    Blocked(FatFingerViolation),
    /// Warning issued but order allowed
    Warning(FatFingerViolation),
}

/// Types of fat-finger violations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FatFingerViolation {
    /// Price deviates too much from market
    PriceDeviation {
        order_price: f64,
        market_mid: f64,
        deviation_pct: f64,
        max_allowed_pct: f64,
    },
    /// Order notional exceeds limit
    ExcessiveNotional {
        order_notional: f64,
        max_allowed: f64,
    },
    /// Position would exceed concentration limit
    ConcentrationLimit {
        symbol: String,
        current_pct: f64,
        after_trade_pct: f64,
        max_allowed_pct: f64,
    },
    /// Position notional would exceed limit
    PositionNotionalLimit {
        symbol: String,
        current_notional: f64,
        after_trade_notional: f64,
        max_allowed: f64,
    },
    /// Too many orders per second
    RateLimitSecond {
        orders_this_second: u64,
        max_allowed: u32,
    },
    /// Too many orders per minute
    RateLimitMinute {
        orders_this_minute: u64,
        max_allowed: u32,
    },
    /// Order too soon after previous
    MinimumInterval {
        elapsed_ms: u64,
        min_required_ms: u64,
    },
    /// No market price available
    NoMarketPrice {
        symbol: String,
    },
    /// Market price is stale
    StaleMarketPrice {
        symbol: String,
        age_ms: u64,
        max_age_ms: u64,
    },
}

impl std::fmt::Display for FatFingerViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FatFingerViolation::PriceDeviation { order_price, market_mid, deviation_pct, max_allowed_pct } => {
                write!(f, "Price deviation: order={:.2}, market={:.2}, deviation={:.2}% (max {:.2}%)", 
                    order_price, market_mid, deviation_pct, max_allowed_pct)
            }
            FatFingerViolation::ExcessiveNotional { order_notional, max_allowed } => {
                write!(f, "Excessive notional: ${:.2} exceeds max ${:.2}", order_notional, max_allowed)
            }
            FatFingerViolation::ConcentrationLimit { symbol, current_pct, after_trade_pct, max_allowed_pct } => {
                write!(f, "Concentration limit: {} at {:.1}% -> {:.1}% (max {:.1}%)", 
                    symbol, current_pct, after_trade_pct, max_allowed_pct)
            }
            FatFingerViolation::PositionNotionalLimit { symbol, current_notional, after_trade_notional, max_allowed } => {
                write!(f, "Position notional limit: {} ${:.2} -> ${:.2} (max ${:.2})", 
                    symbol, current_notional, after_trade_notional, max_allowed)
            }
            FatFingerViolation::RateLimitSecond { orders_this_second, max_allowed } => {
                write!(f, "Rate limit (second): {} orders (max {})", orders_this_second, max_allowed)
            }
            FatFingerViolation::RateLimitMinute { orders_this_minute, max_allowed } => {
                write!(f, "Rate limit (minute): {} orders (max {})", orders_this_minute, max_allowed)
            }
            FatFingerViolation::MinimumInterval { elapsed_ms, min_required_ms } => {
                write!(f, "Minimum interval: {}ms since last order (min {}ms)", elapsed_ms, min_required_ms)
            }
            FatFingerViolation::NoMarketPrice { symbol } => {
                write!(f, "No market price available for {}", symbol)
            }
            FatFingerViolation::StaleMarketPrice { symbol, age_ms, max_age_ms } => {
                write!(f, "Stale market price for {}: {}ms old (max {}ms)", symbol, age_ms, max_age_ms)
            }
        }
    }
}

/// Fat-finger protection guard
pub struct FatFingerGuard {
    /// Configuration
    config: RwLock<FatFingerConfig>,
    /// Market prices by symbol
    market_prices: DashMap<String, MarketPrice>,
    /// Order velocity tracking by symbol
    velocity: DashMap<String, OrderVelocity>,
    /// Position tracking by symbol
    positions: DashMap<String, PositionInfo>,
    /// Total portfolio value (USD)
    total_portfolio_value: AtomicU64, // Scaled by 100 for cents
    /// Statistics
    stats: FatFingerStats,
}

/// Statistics for fat-finger protection
#[derive(Debug, Default)]
pub struct FatFingerStats {
    pub orders_checked: AtomicU64,
    pub orders_blocked: AtomicU64,
    pub orders_warned: AtomicU64,
    pub price_deviation_blocks: AtomicU64,
    pub notional_blocks: AtomicU64,
    pub concentration_blocks: AtomicU64,
    pub rate_limit_blocks: AtomicU64,
    pub stale_price_blocks: AtomicU64,
}

impl FatFingerGuard {
    /// Create a new fat-finger protection guard
    pub fn new(config: FatFingerConfig) -> Self {
        Self {
            config: RwLock::new(config),
            market_prices: DashMap::new(),
            velocity: DashMap::new(),
            positions: DashMap::new(),
            total_portfolio_value: AtomicU64::new(100_000_00), // $100k default
            stats: FatFingerStats::default(),
        }
    }
    
    /// Update market price for a symbol
    pub fn update_market_price(&self, symbol: &str, bid: f64, ask: f64) {
        let mid = (bid + ask) / 2.0;
        self.market_prices.insert(symbol.to_string(), MarketPrice {
            bid,
            ask,
            mid,
            last: None,
            updated_at: Instant::now(),
        });
    }
    
    /// Update market price with last trade
    pub fn update_market_price_with_last(&self, symbol: &str, bid: f64, ask: f64, last: f64) {
        let mid = (bid + ask) / 2.0;
        self.market_prices.insert(symbol.to_string(), MarketPrice {
            bid,
            ask,
            mid,
            last: Some(last),
            updated_at: Instant::now(),
        });
    }
    
    /// Update position for a symbol
    pub fn update_position(&self, symbol: &str, quantity: f64, avg_price: f64) {
        let notional = quantity.abs() * avg_price;
        self.positions.insert(symbol.to_string(), PositionInfo {
            quantity,
            notional_usd: notional,
            avg_price,
        });
    }
    
    /// Set total portfolio value
    pub fn set_portfolio_value(&self, value_usd: f64) {
        self.total_portfolio_value.store((value_usd * 100.0) as u64, Ordering::Release);
    }
    
    /// Get portfolio value
    fn get_portfolio_value(&self) -> f64 {
        self.total_portfolio_value.load(Ordering::Acquire) as f64 / 100.0
    }
    
    /// Update configuration
    pub fn update_config(&self, config: FatFingerConfig) {
        *self.config.write() = config;
    }
    
    /// Get configuration (read-only)
    pub fn get_config(&self) -> FatFingerConfig {
        self.config.read().clone()
    }
    
    /// Validate an order before submission
    pub fn validate_order(&self, signal: &Signal) -> Result<FatFingerResult, FatFingerViolation> {
        let config = self.config.read();
        
        if !config.enabled {
            return Ok(FatFingerResult::Allowed);
        }
        
        self.stats.orders_checked.fetch_add(1, Ordering::Relaxed);
        
        // Get symbol-specific overrides
        let max_deviation = config.symbol_overrides.get(&signal.symbol)
            .and_then(|o| o.max_price_deviation_pct)
            .unwrap_or(config.max_price_deviation_pct);
        let max_notional = config.symbol_overrides.get(&signal.symbol)
            .and_then(|o| o.max_order_notional_usd)
            .unwrap_or(config.max_order_notional_usd);
        let max_position_notional = config.symbol_overrides.get(&signal.symbol)
            .and_then(|o| o.max_position_notional_usd)
            .unwrap_or(config.max_position_notional_usd);
        
        // 1. Check market price availability and staleness
        let market_price = self.market_prices.get(&signal.symbol);
        
        if config.require_market_price && market_price.is_none() {
            self.stats.orders_blocked.fetch_add(1, Ordering::Relaxed);
            self.stats.stale_price_blocks.fetch_add(1, Ordering::Relaxed);
            return Err(FatFingerViolation::NoMarketPrice {
                symbol: signal.symbol.clone(),
            });
        }
        
        let (mid_price, reference_price) = if let Some(mp) = &market_price {
            // Check staleness
            let age_ms = mp.updated_at.elapsed().as_millis() as u64;
            if age_ms > config.price_staleness_ms {
                self.stats.orders_blocked.fetch_add(1, Ordering::Relaxed);
                self.stats.stale_price_blocks.fetch_add(1, Ordering::Relaxed);
                return Err(FatFingerViolation::StaleMarketPrice {
                    symbol: signal.symbol.clone(),
                    age_ms,
                    max_age_ms: config.price_staleness_ms,
                });
            }
            (mp.mid, mp.last.unwrap_or(mp.mid))
        } else {
            // No market price but not required - use order price as reference
            let p = signal.price.unwrap_or(0.0);
            (p, p)
        };
        
        // 2. Check price deviation (only for limit orders with price)
        if let Some(order_price) = signal.price {
            if mid_price > 0.0 {
                let deviation_pct = ((order_price - mid_price) / mid_price * 100.0).abs();
                
                // For buys, price shouldn't be much higher than market
                // For sells, price shouldn't be much lower than market
                let is_adverse = match signal.action {
                    SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop => order_price > mid_price,
                    SignalAction::Sell | SignalAction::SellLimit | SignalAction::SellStop => order_price < mid_price,
                };
                
                // Adverse price deviation is the concern
                if is_adverse && deviation_pct > max_deviation {
                    self.stats.orders_blocked.fetch_add(1, Ordering::Relaxed);
                    self.stats.price_deviation_blocks.fetch_add(1, Ordering::Relaxed);
                    return Err(FatFingerViolation::PriceDeviation {
                        order_price,
                        market_mid: mid_price,
                        deviation_pct,
                        max_allowed_pct: max_deviation,
                    });
                }
            }
        }
        
        // 3. Check order notional
        let exec_price = signal.price.unwrap_or(reference_price);
        if exec_price > 0.0 {
            let order_notional = signal.quantity * exec_price;
            if order_notional > max_notional {
                self.stats.orders_blocked.fetch_add(1, Ordering::Relaxed);
                self.stats.notional_blocks.fetch_add(1, Ordering::Relaxed);
                return Err(FatFingerViolation::ExcessiveNotional {
                    order_notional,
                    max_allowed: max_notional,
                });
            }
            
            // 4. Check position notional limit
            let current_position = self.positions.get(&signal.symbol);
            let current_notional = current_position.as_ref()
                .map(|p| p.notional_usd)
                .unwrap_or(0.0);
            
            let after_trade_notional = match signal.action {
                SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop => current_notional + order_notional,
                SignalAction::Sell | SignalAction::SellLimit | SignalAction::SellStop => (current_notional - order_notional).abs(),
            };
            
            if after_trade_notional > max_position_notional {
                self.stats.orders_blocked.fetch_add(1, Ordering::Relaxed);
                self.stats.notional_blocks.fetch_add(1, Ordering::Relaxed);
                return Err(FatFingerViolation::PositionNotionalLimit {
                    symbol: signal.symbol.clone(),
                    current_notional,
                    after_trade_notional,
                    max_allowed: max_position_notional,
                });
            }
            
            // 5. Check concentration limit
            let portfolio_value = self.get_portfolio_value();
            if portfolio_value > 0.0 {
                let current_pct = current_notional / portfolio_value * 100.0;
                let after_trade_pct = after_trade_notional / portfolio_value * 100.0;
                
                if after_trade_pct > config.max_concentration_pct {
                    self.stats.orders_blocked.fetch_add(1, Ordering::Relaxed);
                    self.stats.concentration_blocks.fetch_add(1, Ordering::Relaxed);
                    return Err(FatFingerViolation::ConcentrationLimit {
                        symbol: signal.symbol.clone(),
                        current_pct,
                        after_trade_pct,
                        max_allowed_pct: config.max_concentration_pct,
                    });
                }
            }
        }
        
        // 6. Check rate limits
        if let Some(velocity) = self.velocity.get(&signal.symbol) {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let now_secs = now_ns / 1_000_000_000;
            let now_mins = now_secs / 60;
            
            // Check minimum interval
            let last_order = velocity.last_order_ns.load(Ordering::Acquire);
            if last_order > 0 {
                let elapsed_ns = now_ns.saturating_sub(last_order);
                let elapsed_ms = elapsed_ns / 1_000_000;
                if elapsed_ms < config.min_order_interval_ms {
                    self.stats.orders_blocked.fetch_add(1, Ordering::Relaxed);
                    self.stats.rate_limit_blocks.fetch_add(1, Ordering::Relaxed);
                    return Err(FatFingerViolation::MinimumInterval {
                        elapsed_ms,
                        min_required_ms: config.min_order_interval_ms,
                    });
                }
            }
            
            // Check per-second rate
            let current_second = velocity.current_second.load(Ordering::Acquire);
            if current_second == now_secs {
                let orders = velocity.orders_this_second.load(Ordering::Acquire);
                if orders >= config.max_orders_per_second as u64 {
                    self.stats.orders_blocked.fetch_add(1, Ordering::Relaxed);
                    self.stats.rate_limit_blocks.fetch_add(1, Ordering::Relaxed);
                    return Err(FatFingerViolation::RateLimitSecond {
                        orders_this_second: orders,
                        max_allowed: config.max_orders_per_second,
                    });
                }
            }
            
            // Check per-minute rate
            let current_minute = velocity.current_minute.load(Ordering::Acquire);
            if current_minute == now_mins {
                let orders = velocity.orders_this_minute.load(Ordering::Acquire);
                if orders >= config.max_orders_per_minute as u64 {
                    self.stats.orders_blocked.fetch_add(1, Ordering::Relaxed);
                    self.stats.rate_limit_blocks.fetch_add(1, Ordering::Relaxed);
                    return Err(FatFingerViolation::RateLimitMinute {
                        orders_this_minute: orders,
                        max_allowed: config.max_orders_per_minute,
                    });
                }
            }
        }
        
        Ok(FatFingerResult::Allowed)
    }
    
    /// Record that an order was submitted (call after successful validation)
    pub fn record_order_submitted(&self, symbol: &str) {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let now_secs = now_ns / 1_000_000_000;
        let now_mins = now_secs / 60;
        
        self.velocity.entry(symbol.to_string())
            .or_insert_with(OrderVelocity::new);
        
        if let Some(velocity) = self.velocity.get(symbol) {
            // Update last order time
            velocity.last_order_ns.store(now_ns, Ordering::Release);
            
            // Update per-second counter
            let current_second = velocity.current_second.load(Ordering::Acquire);
            if current_second != now_secs {
                velocity.current_second.store(now_secs, Ordering::Release);
                velocity.orders_this_second.store(1, Ordering::Release);
            } else {
                velocity.orders_this_second.fetch_add(1, Ordering::Relaxed);
            }
            
            // Update per-minute counter
            let current_minute = velocity.current_minute.load(Ordering::Acquire);
            if current_minute != now_mins {
                velocity.current_minute.store(now_mins, Ordering::Release);
                velocity.orders_this_minute.store(1, Ordering::Release);
            } else {
                velocity.orders_this_minute.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    
    /// Get statistics
    pub fn get_stats(&self) -> FatFingerStatistics {
        FatFingerStatistics {
            orders_checked: self.stats.orders_checked.load(Ordering::Relaxed),
            orders_blocked: self.stats.orders_blocked.load(Ordering::Relaxed),
            orders_warned: self.stats.orders_warned.load(Ordering::Relaxed),
            price_deviation_blocks: self.stats.price_deviation_blocks.load(Ordering::Relaxed),
            notional_blocks: self.stats.notional_blocks.load(Ordering::Relaxed),
            concentration_blocks: self.stats.concentration_blocks.load(Ordering::Relaxed),
            rate_limit_blocks: self.stats.rate_limit_blocks.load(Ordering::Relaxed),
            stale_price_blocks: self.stats.stale_price_blocks.load(Ordering::Relaxed),
            block_rate_pct: if self.stats.orders_checked.load(Ordering::Relaxed) > 0 {
                self.stats.orders_blocked.load(Ordering::Relaxed) as f64 
                    / self.stats.orders_checked.load(Ordering::Relaxed) as f64 * 100.0
            } else {
                0.0
            },
        }
    }
    
    /// Clear all state (for testing)
    pub fn clear(&self) {
        self.market_prices.clear();
        self.velocity.clear();
        self.positions.clear();
    }
}

impl Default for FatFingerGuard {
    fn default() -> Self {
        Self::new(FatFingerConfig::default())
    }
}

/// Public statistics structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FatFingerStatistics {
    pub orders_checked: u64,
    pub orders_blocked: u64,
    pub orders_warned: u64,
    pub price_deviation_blocks: u64,
    pub notional_blocks: u64,
    pub concentration_blocks: u64,
    pub rate_limit_blocks: u64,
    pub stale_price_blocks: u64,
    pub block_rate_pct: f64,
}

/// Global fat-finger guard instance
pub static FAT_FINGER_GUARD: std::sync::LazyLock<FatFingerGuard> = 
    std::sync::LazyLock::new(|| FatFingerGuard::new(FatFingerConfig::default()));

/// Convenience function to validate an order
pub fn validate_order(signal: &Signal) -> Result<FatFingerResult, FatFingerViolation> {
    FAT_FINGER_GUARD.validate_order(signal)
}

/// Convenience function to update market price
pub fn update_market_price(symbol: &str, bid: f64, ask: f64) {
    FAT_FINGER_GUARD.update_market_price(symbol, bid, ask);
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn make_signal(symbol: &str, action: SignalAction, quantity: f64, price: Option<f64>) -> Signal {
        Signal {
            id: "test-1".to_string(),
            symbol: symbol.to_string(),
            exchange: "test".to_string(),
            action,
            quantity,
            price,
            timestamp: 0,
            strategy_id: "test".to_string(),
            confidence: 1.0,
            metadata: std::collections::HashMap::new(),
        }
    }
    
    #[test]
    fn test_price_deviation_block() {
        let guard = FatFingerGuard::new(FatFingerConfig {
            max_price_deviation_pct: 5.0,
            require_market_price: true,
            ..Default::default()
        });
        
        // Set market price at $50,000
        guard.update_market_price("BTC-USD", 49990.0, 50010.0);
        
        // Order at market price should pass
        let signal = make_signal("BTC-USD", SignalAction::Buy, 0.1, Some(50000.0));
        assert!(guard.validate_order(&signal).is_ok());
        
        // Buy order 10% above market should be blocked
        let signal = make_signal("BTC-USD", SignalAction::Buy, 0.1, Some(55000.0));
        match guard.validate_order(&signal) {
            Err(FatFingerViolation::PriceDeviation { .. }) => {},
            other => panic!("Expected PriceDeviation, got {:?}", other),
        }
        
        // Sell order 10% below market should be blocked
        let signal = make_signal("BTC-USD", SignalAction::Sell, 0.1, Some(45000.0));
        match guard.validate_order(&signal) {
            Err(FatFingerViolation::PriceDeviation { .. }) => {},
            other => panic!("Expected PriceDeviation, got {:?}", other),
        }
    }
    
    #[test]
    fn test_notional_limit_block() {
        let guard = FatFingerGuard::new(FatFingerConfig {
            max_order_notional_usd: 10_000.0,
            require_market_price: false,
            ..Default::default()
        });
        
        // Order under limit should pass
        let signal = make_signal("BTC-USD", SignalAction::Buy, 0.1, Some(50000.0)); // $5,000
        assert!(guard.validate_order(&signal).is_ok());
        
        // Order over limit should be blocked
        let signal = make_signal("BTC-USD", SignalAction::Buy, 1.0, Some(50000.0)); // $50,000
        match guard.validate_order(&signal) {
            Err(FatFingerViolation::ExcessiveNotional { .. }) => {},
            other => panic!("Expected ExcessiveNotional, got {:?}", other),
        }
    }
    
    #[test]
    fn test_concentration_limit() {
        let guard = FatFingerGuard::new(FatFingerConfig {
            max_concentration_pct: 25.0,
            require_market_price: false,
            max_order_notional_usd: 1_000_000.0,
            max_position_notional_usd: 1_000_000.0,
            ..Default::default()
        });
        
        // Portfolio is $100k by default
        guard.set_portfolio_value(100_000.0);
        
        // $20k order (20%) should pass
        let signal = make_signal("BTC-USD", SignalAction::Buy, 0.4, Some(50000.0)); // $20,000
        assert!(guard.validate_order(&signal).is_ok());
        
        // $30k order (30%) should be blocked
        let signal = make_signal("BTC-USD", SignalAction::Buy, 0.6, Some(50000.0)); // $30,000
        match guard.validate_order(&signal) {
            Err(FatFingerViolation::ConcentrationLimit { .. }) => {},
            other => panic!("Expected ConcentrationLimit, got {:?}", other),
        }
    }
    
    #[test]
    fn test_stale_price_block() {
        let guard = FatFingerGuard::new(FatFingerConfig {
            price_staleness_ms: 100, // 100ms for testing
            require_market_price: true,
            ..Default::default()
        });
        
        guard.update_market_price("BTC-USD", 49990.0, 50010.0);
        
        // Immediate order should pass
        let signal = make_signal("BTC-USD", SignalAction::Buy, 0.1, Some(50000.0));
        assert!(guard.validate_order(&signal).is_ok());
        
        // Wait for price to become stale
        std::thread::sleep(std::time::Duration::from_millis(150));
        
        // Now should be blocked
        match guard.validate_order(&signal) {
            Err(FatFingerViolation::StaleMarketPrice { .. }) => {},
            other => panic!("Expected StaleMarketPrice, got {:?}", other),
        }
    }
    
    #[test]
    fn test_no_market_price_block() {
        let guard = FatFingerGuard::new(FatFingerConfig {
            require_market_price: true,
            ..Default::default()
        });
        
        // No market price set
        let signal = make_signal("ETH-USD", SignalAction::Buy, 1.0, Some(3000.0));
        match guard.validate_order(&signal) {
            Err(FatFingerViolation::NoMarketPrice { .. }) => {},
            other => panic!("Expected NoMarketPrice, got {:?}", other),
        }
    }
    
    #[test]
    fn test_disabled_guard_allows_all() {
        let guard = FatFingerGuard::new(FatFingerConfig {
            enabled: false,
            ..Default::default()
        });
        
        // Crazy order should pass when disabled
        let signal = make_signal("BTC-USD", SignalAction::Buy, 1000.0, Some(999999.0));
        assert!(guard.validate_order(&signal).is_ok());
    }
    
    #[test]
    fn test_statistics_tracking() {
        let guard = FatFingerGuard::new(FatFingerConfig {
            max_order_notional_usd: 1000.0,
            require_market_price: false,
            ..Default::default()
        });
        
        // Pass one order
        let signal = make_signal("BTC-USD", SignalAction::Buy, 0.01, Some(50000.0)); // $500
        assert!(guard.validate_order(&signal).is_ok());
        
        // Block one order
        let signal = make_signal("BTC-USD", SignalAction::Buy, 1.0, Some(50000.0)); // $50,000
        assert!(guard.validate_order(&signal).is_err());
        
        let stats = guard.get_stats();
        assert_eq!(stats.orders_checked, 2);
        assert_eq!(stats.orders_blocked, 1);
        assert_eq!(stats.notional_blocks, 1);
    }
}
