//! Strategy state management
//!
//! Maintains per-strategy and per-asset state for execution tracking,
//! position management, and P&L calculation.

use std::collections::{HashMap, VecDeque};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use ultra_logger::{ultra_warn, ultra_info};

use crate::types::{StrategyInstance, PortfolioRiskLimits};

/// Complete state for a running strategy
#[derive(Debug, Clone)]
pub struct StrategyState {
    /// Strategy identifier
    pub strategy_id: Uuid,
    
    /// Strategy name (for logging)
    pub name: String,
    
    /// Per-asset state
    pub asset_states: HashMap<(String, String), AssetState>,
    
    /// Portfolio-level metrics
    pub portfolio: PortfolioState,
    
    /// Risk limits (from strategy config)
    pub risk_limits: PortfolioRiskLimits,
    
    /// Whether strategy is currently in cooldown
    pub in_cooldown: bool,
    
    /// Cooldown end time (if in cooldown)
    pub cooldown_until: Option<DateTime<Utc>>,
    
    /// Last signal generation time
    pub last_signal_time: Option<DateTime<Utc>>,
}

impl StrategyState {
    /// Create new state from strategy instance
    pub fn new(strategy: &StrategyInstance) -> Self {
        let mut asset_states = HashMap::new();
        
        for asset in &strategy.assets {
            let key = asset.key();
            asset_states.insert(key.clone(), AssetState::new(
                asset.symbol.clone(),
                asset.exchange.clone(),
                asset.weight,
            ));
        }
        
        Self {
            strategy_id: strategy.id,
            name: strategy.name.clone(),
            asset_states,
            portfolio: PortfolioState::default(),
            risk_limits: strategy.portfolio_risk.clone(),
            in_cooldown: false,
            cooldown_until: None,
            last_signal_time: None,
        }
    }
    
    /// Get total exposure across all assets
    pub fn total_exposure(&self) -> f64 {
        self.asset_states.values()
            .map(|a| a.position_value())
            .sum()
    }
    
    /// Check if strategy can take more exposure
    pub fn can_increase_exposure(&self, amount: f64) -> bool {
        if self.in_cooldown {
            return false;
        }
        
        let new_exposure = self.total_exposure() + amount;
        new_exposure <= self.risk_limits.max_total_exposure
    }
    
    /// Check if daily loss limit has been hit
    pub fn is_daily_loss_exceeded(&self) -> bool {
        self.portfolio.daily_pnl < -self.risk_limits.max_daily_loss
    }
    
    /// Check if max drawdown has been hit
    pub fn is_max_drawdown_exceeded(&self) -> bool {
        let current_drawdown = self.portfolio.calculate_drawdown();
        current_drawdown > self.risk_limits.max_drawdown_pct
    }
    
    /// Enter cooldown period
    pub fn enter_cooldown(&mut self, reason: &str) {
        let cooldown_duration = chrono::Duration::minutes(self.risk_limits.cooldown_minutes as i64);
        self.in_cooldown = true;
        self.cooldown_until = Some(Utc::now() + cooldown_duration);
        ultra_warn!(format!(
            "Strategy '{}' entering cooldown for {} minutes: {}",
            self.name,
            self.risk_limits.cooldown_minutes,
            reason
        ));
    }
    
    /// Check and exit cooldown if time has passed
    pub fn check_cooldown(&mut self) {
        if let Some(until) = self.cooldown_until {
            if Utc::now() >= until {
                self.in_cooldown = false;
                self.cooldown_until = None;
                ultra_info!(format!("Strategy '{}' exiting cooldown", self.name));
            }
        }
    }
    
    /// Reset daily metrics (call at start of trading day)
    pub fn reset_daily(&mut self) {
        self.portfolio.daily_pnl = 0.0;
        self.portfolio.daily_trades = 0;
        
        for asset in self.asset_states.values_mut() {
            asset.daily_trades = 0;
            asset.daily_pnl = 0.0;
        }
    }
    
    /// Get asset state by key
    pub fn get_asset(&self, symbol: &str, exchange: &str) -> Option<&AssetState> {
        let key = (symbol.to_string(), exchange.to_string());
        self.asset_states.get(&key)
    }
    
    /// Get mutable asset state by key
    pub fn get_asset_mut(&mut self, symbol: &str, exchange: &str) -> Option<&mut AssetState> {
        let key = (symbol.to_string(), exchange.to_string());
        self.asset_states.get_mut(&key)
    }
}

/// Per-asset state within a strategy
#[derive(Debug, Clone)]
pub struct AssetState {
    /// Trading symbol
    pub symbol: String,
    
    /// Exchange
    pub exchange: String,
    
    /// Weight in portfolio (0.0 to 1.0)
    pub weight: f64,
    
    /// Current position (positive = long, negative = short)
    pub position: f64,
    
    /// Average entry price
    pub avg_entry_price: f64,
    
    /// Current market price
    pub current_price: f64,
    
    /// Unrealized P&L
    pub unrealized_pnl: f64,
    
    /// Realized P&L (lifetime)
    pub realized_pnl: f64,
    
    /// Daily P&L
    pub daily_pnl: f64,
    
    /// Total trades executed
    pub total_trades: u64,
    
    /// Daily trades executed
    pub daily_trades: u32,
    
    /// Price history for calculations (timestamp_ms, price)
    pub price_history: VecDeque<(i64, f64)>,
    
    /// Maximum price history length
    pub max_history_len: usize,
    
    /// Last trade timestamp
    pub last_trade_time: Option<DateTime<Utc>>,
    
    /// Open orders count
    pub open_orders: u32,
}

impl AssetState {
    /// Create new asset state
    pub fn new(symbol: String, exchange: String, weight: f64) -> Self {
        Self {
            symbol,
            exchange,
            weight,
            position: 0.0,
            avg_entry_price: 0.0,
            current_price: 0.0,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
            daily_pnl: 0.0,
            total_trades: 0,
            daily_trades: 0,
            price_history: VecDeque::with_capacity(1000),
            max_history_len: 1000,
            last_trade_time: None,
            open_orders: 0,
        }
    }
    
    /// Update price and recalculate unrealized P&L
    pub fn update_price(&mut self, timestamp_ms: i64, price: f64) {
        self.current_price = price;
        
        // Add to price history
        self.price_history.push_back((timestamp_ms, price));
        while self.price_history.len() > self.max_history_len {
            self.price_history.pop_front();
        }
        
        // Recalculate unrealized P&L
        if self.position != 0.0 && self.avg_entry_price > 0.0 {
            self.unrealized_pnl = self.position * (price - self.avg_entry_price);
        }
    }
    
    /// Record a trade execution
    pub fn record_trade(&mut self, quantity: f64, price: f64, is_buy: bool) {
        let signed_qty = if is_buy { quantity } else { -quantity };
        
        // Calculate realized P&L if reducing position
        if (self.position > 0.0 && !is_buy) || (self.position < 0.0 && is_buy) {
            let closing_qty = signed_qty.abs().min(self.position.abs());
            let pnl = if self.position > 0.0 {
                closing_qty * (price - self.avg_entry_price)
            } else {
                closing_qty * (self.avg_entry_price - price)
            };
            self.realized_pnl += pnl;
            self.daily_pnl += pnl;
        }
        
        // Update position
        let new_position = self.position + signed_qty;
        
        // Update average entry price
        if new_position.abs() > 0.0 {
            if (self.position >= 0.0 && is_buy) || (self.position <= 0.0 && !is_buy) {
                // Adding to position - weighted average
                let old_value = self.position.abs() * self.avg_entry_price;
                let new_value = quantity * price;
                self.avg_entry_price = (old_value + new_value) / new_position.abs();
            }
            // If reducing position, avg entry stays the same
        } else {
            // Position closed
            self.avg_entry_price = 0.0;
        }
        
        self.position = new_position;
        self.total_trades += 1;
        self.daily_trades += 1;
        self.last_trade_time = Some(Utc::now());
        
        // Recalculate unrealized P&L
        if self.position != 0.0 && self.avg_entry_price > 0.0 {
            self.unrealized_pnl = self.position * (self.current_price - self.avg_entry_price);
        } else {
            self.unrealized_pnl = 0.0;
        }
    }
    
    /// Get current position value (in base currency)
    pub fn position_value(&self) -> f64 {
        self.position.abs() * self.current_price
    }
    
    /// Calculate momentum from price history
    pub fn calculate_momentum(&self, lookback_ms: i64) -> Option<f64> {
        let now = self.price_history.back()?.0;
        let cutoff = now - lookback_ms;
        
        // Find oldest price within lookback window
        let old_price = self.price_history.iter()
            .find(|(ts, _)| *ts >= cutoff)
            .map(|(_, p)| *p)?;
        
        let current = self.current_price;
        if old_price > 0.0 {
            Some((current - old_price) / old_price * 100.0) // Return as percentage
        } else {
            None
        }
    }
    
    /// Check if asset is flat (no position)
    pub fn is_flat(&self) -> bool {
        self.position.abs() < 1e-10
    }
    
    /// Check if asset is long
    pub fn is_long(&self) -> bool {
        self.position > 1e-10
    }
    
    /// Check if asset is short
    pub fn is_short(&self) -> bool {
        self.position < -1e-10
    }
}

/// Portfolio-level state
#[derive(Debug, Clone, Default)]
pub struct PortfolioState {
    /// Total equity (positions + cash)
    pub equity: f64,
    
    /// Available cash
    pub cash: f64,
    
    /// Peak equity (for drawdown calculation)
    pub peak_equity: f64,
    
    /// Daily P&L
    pub daily_pnl: f64,
    
    /// Lifetime P&L
    pub lifetime_pnl: f64,
    
    /// Daily trade count
    pub daily_trades: u32,
    
    /// Total trade count
    pub total_trades: u64,
}

impl PortfolioState {
    /// Update equity and track peak
    pub fn update_equity(&mut self, new_equity: f64) {
        self.equity = new_equity;
        if new_equity > self.peak_equity {
            self.peak_equity = new_equity;
        }
    }
    
    /// Calculate current drawdown percentage
    pub fn calculate_drawdown(&self) -> f64 {
        if self.peak_equity > 0.0 {
            (self.peak_equity - self.equity) / self.peak_equity
        } else {
            0.0
        }
    }
}

/// Registry of all strategy states
#[derive(Debug, Default)]
pub struct StrategyStateRegistry {
    states: HashMap<Uuid, StrategyState>,
}

impl StrategyStateRegistry {
    /// Create new registry
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Register a strategy and create its state
    pub fn register(&mut self, strategy: &StrategyInstance) {
        let state = StrategyState::new(strategy);
        self.states.insert(strategy.id, state);
    }
    
    /// Unregister a strategy
    pub fn unregister(&mut self, strategy_id: Uuid) -> Option<StrategyState> {
        self.states.remove(&strategy_id)
    }
    
    /// Get strategy state
    pub fn get(&self, strategy_id: Uuid) -> Option<&StrategyState> {
        self.states.get(&strategy_id)
    }
    
    /// Get mutable strategy state
    pub fn get_mut(&mut self, strategy_id: Uuid) -> Option<&mut StrategyState> {
        self.states.get_mut(&strategy_id)
    }
    
    /// Iterate over all states
    pub fn iter(&self) -> impl Iterator<Item = (&Uuid, &StrategyState)> {
        self.states.iter()
    }
    
    /// Iterate mutably over all states
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&Uuid, &mut StrategyState)> {
        self.states.iter_mut()
    }
    
    /// Count of registered strategies
    pub fn len(&self) -> usize {
        self.states.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
    
    /// Reset daily metrics for all strategies
    pub fn reset_all_daily(&mut self) {
        for state in self.states.values_mut() {
            state.reset_daily();
        }
    }
    
    /// Check and update cooldowns for all strategies
    pub fn check_all_cooldowns(&mut self) {
        for state in self.states.values_mut() {
            state.check_cooldown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_asset_state_price_update() {
        let mut state = AssetState::new("BTC/USD".into(), "kraken".into(), 1.0);
        
        state.update_price(1000, 50000.0);
        assert_eq!(state.current_price, 50000.0);
        assert_eq!(state.price_history.len(), 1);
        
        state.update_price(2000, 51000.0);
        assert_eq!(state.current_price, 51000.0);
        assert_eq!(state.price_history.len(), 2);
    }
    
    #[test]
    fn test_asset_state_trade_recording() {
        let mut state = AssetState::new("BTC/USD".into(), "kraken".into(), 1.0);
        state.current_price = 50000.0;
        
        // Buy 1 BTC at 50000
        state.record_trade(1.0, 50000.0, true);
        assert_eq!(state.position, 1.0);
        assert_eq!(state.avg_entry_price, 50000.0);
        
        // Price goes up
        state.update_price(1000, 51000.0);
        assert_eq!(state.unrealized_pnl, 1000.0);
        
        // Sell 0.5 BTC at 51000 - realize profit
        state.record_trade(0.5, 51000.0, false);
        assert_eq!(state.position, 0.5);
        assert_eq!(state.realized_pnl, 500.0); // 0.5 * (51000 - 50000)
    }
    
    #[test]
    fn test_asset_state_momentum() {
        let mut state = AssetState::new("BTC/USD".into(), "kraken".into(), 1.0);
        
        state.update_price(0, 100.0);
        state.update_price(1000, 105.0);
        
        let momentum = state.calculate_momentum(2000);
        assert!(momentum.is_some());
        assert!((momentum.unwrap() - 5.0).abs() < 0.01); // 5% increase
    }
    
    #[test]
    fn test_portfolio_drawdown() {
        let mut portfolio = PortfolioState::default();
        
        portfolio.update_equity(10000.0);
        assert_eq!(portfolio.peak_equity, 10000.0);
        assert_eq!(portfolio.calculate_drawdown(), 0.0);
        
        portfolio.update_equity(9000.0);
        assert_eq!(portfolio.peak_equity, 10000.0); // Peak unchanged
        assert_eq!(portfolio.calculate_drawdown(), 0.1); // 10% drawdown
    }
}
