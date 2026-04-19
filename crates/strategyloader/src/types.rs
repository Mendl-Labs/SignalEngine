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
    Custom,
    CustomMarketMaking,
    PortfolioMixed,
}

impl std::fmt::Display for StrategyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrategyType::Custom => write!(f, "custom"),
            StrategyType::CustomMarketMaking => write!(f, "custom_market_making"),
            StrategyType::PortfolioMixed => write!(f, "portfolio_mixed"),
        }
    }
}

impl std::str::FromStr for StrategyType {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "custom" | "momentum" | "mean_reversion" | "meanreversion" | "arbitrage" | "liquidity_sweep" => Ok(StrategyType::Custom),
            "custom_market_making" | "custommarketmaking" | "market_making" | "marketmaking" => Ok(StrategyType::CustomMarketMaking),
            "portfolio_mixed" | "portfoliomixed" => Ok(StrategyType::PortfolioMixed),
            _ => Ok(StrategyType::Custom),
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
    
    /// Optional description
    pub description: Option<String>,
    
    /// Python source code (for custom strategies loaded from backtest)
    pub python_source: Option<String>,
    
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
        // Legacy names map to Custom
        assert_eq!("momentum".parse::<StrategyType>().unwrap(), StrategyType::Custom);
        assert_eq!("mean_reversion".parse::<StrategyType>().unwrap(), StrategyType::Custom);
        // New names
        assert_eq!("custom".parse::<StrategyType>().unwrap(), StrategyType::Custom);
        assert_eq!("custom_market_making".parse::<StrategyType>().unwrap(), StrategyType::CustomMarketMaking);
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

    #[test]
    fn test_strategy_type_display() {
        assert_eq!(StrategyType::Custom.to_string(), "custom");
        assert_eq!(StrategyType::CustomMarketMaking.to_string(), "custom_market_making");
        assert_eq!(StrategyType::PortfolioMixed.to_string(), "portfolio_mixed");
    }

    #[test]
    fn test_strategy_type_serde_roundtrip() {
        for st in [StrategyType::Custom, StrategyType::CustomMarketMaking, StrategyType::PortfolioMixed] {
            let json = serde_json::to_string(&st).unwrap();
            let parsed: StrategyType = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, st);
        }
    }

    #[test]
    fn test_portfolio_risk_limits_default() {
        let limits = PortfolioRiskLimits::default();
        assert_eq!(limits.max_total_exposure, 10_000.0);
        assert_eq!(limits.max_daily_loss, 500.0);
        assert!((limits.max_drawdown_pct - 0.15).abs() < f64::EPSILON);
        assert_eq!(limits.cooldown_minutes, 30);
    }

    #[test]
    fn test_asset_risk_limits_default() {
        let limits = AssetRiskLimits::default();
        assert_eq!(limits.max_position_size, 1_000.0);
        assert!((limits.max_position_pct - 0.10).abs() < f64::EPSILON);
        assert_eq!(limits.max_orders_per_minute, 10);
    }

    #[test]
    fn test_momentum_params_default() {
        let p = MomentumParams::default();
        assert!((p.momentum_threshold_pct - 0.5).abs() < f64::EPSILON);
        assert_eq!(p.momentum_lookback_ms, 60_000);
        assert_eq!(p.volume_multiplier, 1.5);
    }

    #[test]
    fn test_mean_reversion_params_default() {
        let p = MeanReversionParams::default();
        assert_eq!(p.zscore_entry_threshold, 2.0);
        assert_eq!(p.zscore_exit_threshold, 0.5);
        assert!(p.use_bollinger_bands);
    }

    #[test]
    fn test_mean_reversion_serde_roundtrip() {
        let params = StrategyParameters::MeanReversion(MeanReversionParams::default());
        let json = serde_json::to_string(&params).unwrap();
        let parsed: StrategyParameters = serde_json::from_str(&json).unwrap();
        assert!(parsed.as_mean_reversion().is_some());
        assert!(parsed.as_momentum().is_none());
    }

    #[test]
    fn test_generic_params_accessors() {
        let mut map = HashMap::new();
        map.insert("threshold".to_string(), serde_json::json!(0.5));
        map.insert("count".to_string(), serde_json::json!(10));
        map.insert("name".to_string(), serde_json::json!("test"));
        map.insert("flag".to_string(), serde_json::json!(true));
        let gp = GenericParams { params: map };

        assert_eq!(gp.get_f64("threshold"), Some(0.5));
        assert_eq!(gp.get_i64("count"), Some(10));
        assert_eq!(gp.get_str("name"), Some("test"));
        assert_eq!(gp.get_bool("flag"), Some(true));
        assert_eq!(gp.get_f64("missing"), None);
    }

    #[test]
    fn test_trading_asset_key() {
        let asset = TradingAsset {
            symbol: "BTC/USD".to_string(),
            exchange: "kraken".to_string(),
            weight: 0.5,
            risk_limits: AssetRiskLimits::default(),
        };
        assert_eq!(asset.key(), ("BTC/USD".to_string(), "kraken".to_string()));
    }

    #[test]
    fn test_strategy_instance_full_serde() {
        let instance = StrategyInstance {
            id: Uuid::new_v4(),
            name: "test-strat".to_string(),
            strategy_type: StrategyType::Custom,
            version: "1.0".to_string(),
            assets: vec![TradingAsset {
                symbol: "ETH/USD".to_string(),
                exchange: "binance".to_string(),
                weight: 1.0,
                risk_limits: AssetRiskLimits::default(),
            }],
            parameters: StrategyParameters::Generic(GenericParams::default()),
            portfolio_risk: PortfolioRiskLimits::default(),
            enabled: true,
            description: Some("A test strategy".to_string()),
            python_source: None,
            metadata: HashMap::new(),
        };
        let json = serde_json::to_string(&instance).unwrap();
        let parsed: StrategyInstance = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "test-strat");
        assert_eq!(parsed.assets.len(), 1);
        assert!(parsed.enabled);
    }

    #[test]
    fn test_strategy_type_unknown_defaults_to_custom() {
        let st: StrategyType = "something_unknown".parse().unwrap();
        assert_eq!(st, StrategyType::Custom);
    }

    #[test]
    fn test_portfolio_risk_serde_with_defaults() {
        // Missing fields should use defaults
        let json = r#"{}"#;
        let limits: PortfolioRiskLimits = serde_json::from_str(json).unwrap();
        assert_eq!(limits.max_total_exposure, 10_000.0);
        assert_eq!(limits.cooldown_minutes, 30);
    }
}
