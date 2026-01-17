//! Core types for strategy management
//!
//! These types represent the structure of trading strategies as loaded
//! from the database or configuration files.

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::HashMap;

/// Type of trading strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyType {
    Momentum,
    MeanReversion,
    PortfolioMixed,
}

impl std::fmt::Display for StrategyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrategyType::Momentum => write!(f, "momentum"),
            StrategyType::MeanReversion => write!(f, "mean_reversion"),
            StrategyType::PortfolioMixed => write!(f, "portfolio_mixed"),
        }
    }
}

impl std::str::FromStr for StrategyType {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "momentum" => Ok(StrategyType::Momentum),
            "mean_reversion" | "meanreversion" => Ok(StrategyType::MeanReversion),
            "portfolio_mixed" | "portfoliomixed" => Ok(StrategyType::PortfolioMixed),
            _ => Err(format!("Unknown strategy type: {}", s)),
        }
    }
}

/// A complete strategy instance ready for execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInstance {
    /// Unique identifier (from database)
    pub id: Uuid,
    
    /// Human-readable name
    pub name: String,
    
    /// Strategy type determines which executor to use
    pub strategy_type: StrategyType,
    
    /// Version for tracking changes
    pub version: String,
    
    /// Assets this strategy trades
    pub assets: Vec<TradingAsset>,
    
    /// Strategy-specific parameters
    pub parameters: StrategyParameters,
    
    /// Portfolio-level risk limits
    pub portfolio_risk: PortfolioRiskLimits,
    
    /// Whether this strategy is enabled for trading
    pub enabled: bool,
    
    /// Whether to run in paper trading mode
    pub paper_trading: bool,
    
    /// Optional description
    pub description: Option<String>,
    
    /// Additional metadata
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

/// A single tradeable asset within a strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingAsset {
    /// Trading symbol (e.g., "BTC/USD")
    pub symbol: String,
    
    /// Exchange to trade on (e.g., "kraken")
    pub exchange: String,
    
    /// Weight in portfolio (0.0 to 1.0, should sum to 1.0 across assets)
    pub weight: f64,
    
    /// Per-asset risk limits
    pub risk_limits: AssetRiskLimits,
}

impl TradingAsset {
    /// Create asset key for routing (symbol:exchange)
    pub fn key(&self) -> (String, String) {
        (self.symbol.clone(), self.exchange.clone())
    }
}

/// Portfolio-level risk management limits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioRiskLimits {
    /// Maximum total exposure in base currency (e.g., $10,000)
    #[serde(default = "default_max_exposure")]
    pub max_total_exposure: f64,
    
    /// Maximum loss allowed per day
    #[serde(default = "default_max_daily_loss")]
    pub max_daily_loss: f64,
    
    /// Maximum portfolio drawdown before stopping
    #[serde(default = "default_max_drawdown")]
    pub max_drawdown_pct: f64,
    
    /// Maximum correlation exposure (limit concentrated bets)
    #[serde(default = "default_max_correlation")]
    pub max_correlation_exposure: f64,
    
    /// Cooldown period after hitting limits (minutes)
    #[serde(default = "default_cooldown")]
    pub cooldown_minutes: u32,
}

fn default_max_exposure() -> f64 { 10_000.0 }
fn default_max_daily_loss() -> f64 { 500.0 }
fn default_max_drawdown() -> f64 { 0.15 }
fn default_max_correlation() -> f64 { 0.8 }
fn default_cooldown() -> u32 { 30 }

impl Default for PortfolioRiskLimits {
    fn default() -> Self {
        Self {
            max_total_exposure: default_max_exposure(),
            max_daily_loss: default_max_daily_loss(),
            max_drawdown_pct: default_max_drawdown(),
            max_correlation_exposure: default_max_correlation(),
            cooldown_minutes: default_cooldown(),
        }
    }
}

/// Per-asset risk limits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetRiskLimits {
    /// Maximum position size for this asset (in base currency)
    #[serde(default = "default_position_size")]
    pub max_position_size: f64,
    
    /// Maximum position as percentage of portfolio
    #[serde(default = "default_position_pct")]
    pub max_position_pct: f64,
    
    /// Stop loss percentage per trade
    #[serde(default = "default_stop_loss")]
    pub stop_loss_pct: f64,
    
    /// Take profit percentage per trade
    #[serde(default = "default_take_profit")]
    pub take_profit_pct: f64,
    
    /// Maximum orders per minute for this asset
    #[serde(default = "default_max_orders")]
    pub max_orders_per_minute: u32,
}

fn default_position_size() -> f64 { 1_000.0 }
fn default_position_pct() -> f64 { 0.10 }
fn default_stop_loss() -> f64 { 0.02 }
fn default_take_profit() -> f64 { 0.03 }
fn default_max_orders() -> u32 { 10 }

impl Default for AssetRiskLimits {
    fn default() -> Self {
        Self {
            max_position_size: default_position_size(),
            max_position_pct: default_position_pct(),
            stop_loss_pct: default_stop_loss(),
            take_profit_pct: default_take_profit(),
            max_orders_per_minute: default_max_orders(),
        }
    }
}

/// Strategy-specific parameters (type-safe union)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StrategyParameters {
    Momentum(MomentumParams),
    MeanReversion(MeanReversionParams),
    /// Generic parameters for mixed/custom strategies
    Generic(GenericParams),
}

impl StrategyParameters {
    /// Try to get momentum parameters
    pub fn as_momentum(&self) -> Option<&MomentumParams> {
        match self {
            StrategyParameters::Momentum(p) => Some(p),
            _ => None,
        }
    }
    
    /// Try to get mean reversion parameters
    pub fn as_mean_reversion(&self) -> Option<&MeanReversionParams> {
        match self {
            StrategyParameters::MeanReversion(p) => Some(p),
            _ => None,
        }
    }
}

/// Momentum strategy parameters (from BacktestingEngine optimization)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumParams {
    /// Minimum price change % to trigger signal
    pub momentum_threshold_pct: f64,
    
    /// Lookback window for momentum calculation (milliseconds)
    pub momentum_lookback_ms: i64,
    
    /// Order book imbalance threshold
    pub imbalance_threshold: f64,
    
    /// Position size as % of allocated capital
    pub position_size_pct: f64,
    
    /// Take profit % per trade
    pub take_profit_pct: f64,
    
    /// Stop loss % per trade
    pub stop_loss_pct: f64,
    
    /// Minimum time between trades (milliseconds)
    #[serde(default)]
    pub min_trade_interval_ms: i64,
    
    /// Use volume confirmation
    #[serde(default)]
    pub use_volume_confirmation: bool,
    
    /// Volume multiplier threshold
    #[serde(default = "default_volume_multiplier")]
    pub volume_multiplier: f64,
}

fn default_volume_multiplier() -> f64 { 1.5 }

impl Default for MomentumParams {
    fn default() -> Self {
        Self {
            momentum_threshold_pct: 0.5,
            momentum_lookback_ms: 60_000,
            imbalance_threshold: 0.3,
            position_size_pct: 0.10,
            take_profit_pct: 0.8,
            stop_loss_pct: 0.5,
            min_trade_interval_ms: 1000,
            use_volume_confirmation: false,
            volume_multiplier: 1.5,
        }
    }
}

/// Mean reversion strategy parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionParams {
    /// Z-score threshold for entry
    pub zscore_entry_threshold: f64,
    
    /// Z-score threshold for exit
    pub zscore_exit_threshold: f64,
    
    /// Lookback window for mean calculation (milliseconds)
    pub lookback_ms: i64,
    
    /// Position size as % of allocated capital
    pub position_size_pct: f64,
    
    /// Maximum holding time (milliseconds)
    pub max_holding_time_ms: i64,
    
    /// Stop loss % per trade
    pub stop_loss_pct: f64,
    
    /// Take profit % per trade
    pub take_profit_pct: f64,
    
    /// Use Bollinger bands
    #[serde(default)]
    pub use_bollinger_bands: bool,
    
    /// Bollinger band standard deviations
    #[serde(default = "default_bollinger_std")]
    pub bollinger_std: f64,
}

fn default_bollinger_std() -> f64 { 2.0 }

impl Default for MeanReversionParams {
    fn default() -> Self {
        Self {
            zscore_entry_threshold: 2.0,
            zscore_exit_threshold: 0.5,
            lookback_ms: 300_000,
            position_size_pct: 0.10,
            max_holding_time_ms: 3_600_000,
            stop_loss_pct: 1.0,
            take_profit_pct: 0.5,
            use_bollinger_bands: true,
            bollinger_std: 2.0,
        }
    }
}

/// Generic parameters for custom/mixed strategies
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GenericParams {
    /// Arbitrary key-value parameters
    #[serde(flatten)]
    pub params: HashMap<String, serde_json::Value>,
}

impl GenericParams {
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.params.get(key)?.as_f64()
    }
    
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.params.get(key)?.as_i64()
    }
    
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.params.get(key)?.as_str()
    }
    
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.params.get(key)?.as_bool()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_strategy_type_parsing() {
        assert_eq!("momentum".parse::<StrategyType>().unwrap(), StrategyType::Momentum);
        assert_eq!("mean_reversion".parse::<StrategyType>().unwrap(), StrategyType::MeanReversion);
        assert_eq!("portfolio_mixed".parse::<StrategyType>().unwrap(), StrategyType::PortfolioMixed);
    }
    
    #[test]
    fn test_strategy_parameters_serialization() {
        let params = StrategyParameters::Momentum(MomentumParams::default());
        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains("momentum"));
        
        let parsed: StrategyParameters = serde_json::from_str(&json).unwrap();
        assert!(parsed.as_momentum().is_some());
    }
}
