//! Database strategy loader - loads strategies from PostgreSQL
//!
//! Connects to the same database as BacktestingEngine to load
//! optimized strategies from the `strategies` and `strategy_instances` tables.

use async_trait::async_trait;
use uuid::Uuid;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use diesel_async::pooled_connection::deadpool::Pool;
use ultra_logger::{ultra_warn, ultra_info};

use databaseschema::models::strategy::{Strategy, StrategyInstance as DbStrategyInstance};
use databaseschema::schema::{strategies, strategy_instances};

use crate::types::*;
use crate::loader::StrategyLoader;
use crate::error::{StrategyLoaderError, Result};

/// Loads strategies from PostgreSQL database
pub struct DatabaseStrategyLoader {
    pool: Pool<AsyncPgConnection>,
}

impl DatabaseStrategyLoader {
    /// Create a new database strategy loader with connection pool
    pub fn new(pool: Pool<AsyncPgConnection>) -> Self {
        Self { pool }
    }
    
    /// Create from DATABASE_URL environment variable
    pub async fn from_env() -> Result<Self> {
        use diesel_async::pooled_connection::AsyncDieselConnectionManager;
        
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| StrategyLoaderError::Config("DATABASE_URL not set".into()))?;
        
        let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&database_url);
        let pool = Pool::builder(config)
            .max_size(5)
            .build()
            .map_err(|e| StrategyLoaderError::Connection(e.to_string()))?;
        
        Ok(Self { pool })
    }
    
    /// Convert database Strategy + StrategyInstance to our StrategyInstance
    fn convert_to_strategy_instance(
        strategy: Strategy,
        instance: DbStrategyInstance,
    ) -> Result<crate::types::StrategyInstance> {
        let strategy_type: StrategyType = strategy.strategy_type.parse()
            .map_err(|e: String| StrategyLoaderError::InvalidParameters(e))?;
        
        // Parse the parameters JSON
        let params_json = instance.parameters;
        
        // Extract assets from parameters
        let assets = Self::parse_assets(&params_json)?;
        
        // Extract strategy-specific parameters
        let strategy_params = Self::parse_strategy_params(strategy_type, &params_json)?;
        
        // Extract portfolio risk limits
        let portfolio_risk = Self::parse_portfolio_risk(&params_json);
        
        // Build metadata from performance summary
        let mut metadata = std::collections::HashMap::new();
        if let Some(perf) = instance.performance_summary {
            metadata.insert("performance_summary".to_string(), perf);
        }
        if let Some(risk) = instance.risk_metrics {
            metadata.insert("risk_metrics".to_string(), risk);
        }
        
        Ok(crate::types::StrategyInstance {
            id: instance.id,
            name: instance.instance_name.unwrap_or_else(|| strategy.strategy_name.clone()),
            strategy_type,
            version: strategy.version,
            assets,
            parameters: strategy_params,
            portfolio_risk,
            enabled: strategy.is_active,
            description: instance.description.or(strategy.description),
            metadata,
        })
    }
    
    /// Parse trading assets from parameters JSON
    fn parse_assets(params: &serde_json::Value) -> Result<Vec<TradingAsset>> {
        // Try to get assets array from parameters
        if let Some(assets_array) = params.get("assets").and_then(|v| v.as_array()) {
            let mut assets = Vec::new();
            for asset_json in assets_array {
                let symbol = asset_json.get("symbol")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| StrategyLoaderError::InvalidParameters("Missing symbol in asset".into()))?;
                
                let exchange = asset_json.get("exchange")
                    .and_then(|v| v.as_str())
                    .unwrap_or("kraken");
                
                let weight = asset_json.get("weight")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(1.0);
                
                let risk_limits = Self::parse_asset_risk_limits(asset_json);
                
                assets.push(TradingAsset {
                    symbol: symbol.to_string(),
                    exchange: exchange.to_string(),
                    weight,
                    risk_limits,
                });
            }
            return Ok(assets);
        }
        
        // Fallback: try to get single symbol/exchange from root
        let symbol = params.get("symbol")
            .and_then(|v| v.as_str())
            .or_else(|| params.get("trading_pair").and_then(|v| v.as_str()));
        
        let exchange = params.get("exchange")
            .and_then(|v| v.as_str())
            .unwrap_or("kraken");
        
        if let Some(sym) = symbol {
            Ok(vec![TradingAsset {
                symbol: sym.to_string(),
                exchange: exchange.to_string(),
                weight: 1.0,
                risk_limits: AssetRiskLimits::default(),
            }])
        } else {
            // Default placeholder
            Ok(vec![TradingAsset {
                symbol: "BTC/USD".to_string(),
                exchange: "kraken".to_string(),
                weight: 1.0,
                risk_limits: AssetRiskLimits::default(),
            }])
        }
    }
    
    /// Parse per-asset risk limits
    fn parse_asset_risk_limits(asset_json: &serde_json::Value) -> AssetRiskLimits {
        let risk = asset_json.get("risk_limits");
        
        AssetRiskLimits {
            max_position_size: risk.and_then(|r| r.get("max_position_size"))
                .and_then(|v| v.as_f64())
                .unwrap_or(1_000.0),
            max_position_pct: risk.and_then(|r| r.get("max_position_pct"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.10),
            stop_loss_pct: risk.and_then(|r| r.get("stop_loss_pct"))
                .and_then(|v| v.as_f64())
                .or_else(|| asset_json.get("stop_loss_pct").and_then(|v| v.as_f64()))
                .unwrap_or(0.02),
            take_profit_pct: risk.and_then(|r| r.get("take_profit_pct"))
                .and_then(|v| v.as_f64())
                .or_else(|| asset_json.get("take_profit_pct").and_then(|v| v.as_f64()))
                .unwrap_or(0.03),
            max_orders_per_minute: risk.and_then(|r| r.get("max_orders_per_minute"))
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as u32,
        }
    }
    
    /// Parse portfolio-level risk limits
    fn parse_portfolio_risk(params: &serde_json::Value) -> PortfolioRiskLimits {
        let risk = params.get("portfolio_risk")
            .or_else(|| params.get("risk_limits"));
        
        PortfolioRiskLimits {
            max_total_exposure: risk.and_then(|r| r.get("max_total_exposure"))
                .and_then(|v| v.as_f64())
                .unwrap_or(10_000.0),
            max_daily_loss: risk.and_then(|r| r.get("max_daily_loss"))
                .and_then(|v| v.as_f64())
                .unwrap_or(500.0),
            max_drawdown_pct: risk.and_then(|r| r.get("max_drawdown_pct"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.15),
            max_correlation_exposure: risk.and_then(|r| r.get("max_correlation_exposure"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.8),
            cooldown_minutes: risk.and_then(|r| r.get("cooldown_minutes"))
                .and_then(|v| v.as_u64())
                .unwrap_or(30) as u32,
        }
    }
    
    /// Parse strategy-specific parameters based on type
    fn parse_strategy_params(
        strategy_type: StrategyType,
        params: &serde_json::Value,
    ) -> Result<StrategyParameters> {
        match strategy_type {
            StrategyType::Momentum => {
                Ok(StrategyParameters::Momentum(MomentumParams {
                    momentum_threshold_pct: params.get("momentum_threshold_pct")
                        .or_else(|| params.get("threshold_pct"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.5),
                    momentum_lookback_ms: params.get("momentum_lookback_ms")
                        .or_else(|| params.get("lookback_ms"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(60_000),
                    imbalance_threshold: params.get("imbalance_threshold")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.3),
                    position_size_pct: params.get("position_size_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.10),
                    take_profit_pct: params.get("take_profit_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.8),
                    stop_loss_pct: params.get("stop_loss_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.5),
                    min_trade_interval_ms: params.get("min_trade_interval_ms")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(1000),
                    use_volume_confirmation: params.get("use_volume_confirmation")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    volume_multiplier: params.get("volume_multiplier")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(1.5),
                }))
            }
            StrategyType::MeanReversion => {
                Ok(StrategyParameters::MeanReversion(MeanReversionParams {
                    zscore_entry_threshold: params.get("zscore_entry_threshold")
                        .or_else(|| params.get("entry_threshold"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(2.0),
                    zscore_exit_threshold: params.get("zscore_exit_threshold")
                        .or_else(|| params.get("exit_threshold"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.5),
                    lookback_ms: params.get("lookback_ms")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(300_000),
                    position_size_pct: params.get("position_size_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.10),
                    max_holding_time_ms: params.get("max_holding_time_ms")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(3_600_000),
                    stop_loss_pct: params.get("stop_loss_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(1.0),
                    take_profit_pct: params.get("take_profit_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.5),
                    use_bollinger_bands: params.get("use_bollinger_bands")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true),
                    bollinger_std: params.get("bollinger_std")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(2.0),
                }))
            }
            StrategyType::MarketMaking => {
                Ok(StrategyParameters::MarketMaking(MarketMakingParams {
                    risk_aversion: params.get("risk_aversion")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.1),
                    inventory_target: params.get("inventory_target")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    order_size: params.get("order_size")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(100.0),
                    window_ms: params.get("window_ms")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(1000),
                    min_quote_lifetime_ms: params.get("min_quote_lifetime_ms")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(100),
                    fee_rate: params.get("fee_rate")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.001),
                    volatility_cap: params.get("volatility_cap")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.2),
                    max_spread_pct: params.get("max_spread_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.02),
                    min_spread_pct: params.get("min_spread_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.001),
                }))
            }
            StrategyType::Arbitrage => {
                Ok(StrategyParameters::Arbitrage(ArbitrageParams {
                    min_spread_pct: params.get("min_spread_pct")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.002),
                    max_execution_time_ms: params.get("max_execution_time_ms")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(500),
                    position_size: params.get("position_size")
                        .or_else(|| params.get("leg_size"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(100.0),
                    include_fees: params.get("include_fees")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true),
                    primary_exchange: params.get("primary_exchange")
                        .and_then(|v| v.as_str())
                        .unwrap_or("kraken")
                        .to_string(),
                    secondary_exchange: params.get("secondary_exchange")
                        .and_then(|v| v.as_str())
                        .unwrap_or("binance")
                        .to_string(),
                }))
            }
            StrategyType::PortfolioMixed => {
                // For mixed strategies, store all params as generic
                Ok(StrategyParameters::Generic(GenericParams {
                    params: params.as_object()
                        .map(|obj| obj.iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect())
                        .unwrap_or_default(),
                }))
            }
        }
    }
}

#[async_trait]
impl StrategyLoader for DatabaseStrategyLoader {
    async fn load_active_strategies(&self) -> Result<Vec<crate::types::StrategyInstance>> {
        let mut conn = self.pool.get().await
            .map_err(|e| StrategyLoaderError::Connection(e.to_string()))?;
        
        // Load all active strategies with their instances
        let results: Vec<(Strategy, DbStrategyInstance)> = strategies::table
            .inner_join(strategy_instances::table.on(
                strategy_instances::strategy_id.eq(strategies::id)
            ))
            .filter(strategies::is_active.eq(true))
            .select((Strategy::as_select(), DbStrategyInstance::as_select()))
            .load(&mut conn)
            .await
            .map_err(|e| StrategyLoaderError::Database(e.to_string()))?;
        
        let mut strategies_list = Vec::new();
        
        for (strategy, instance) in results {
            match Self::convert_to_strategy_instance(strategy, instance) {
                Ok(si) => strategies_list.push(si),
                Err(e) => {
                    ultra_warn!(format!("Failed to parse strategy: {}", e));
                }
            }
        }
        
        ultra_info!(format!("Loaded {} active strategies from database", strategies_list.len()));
        Ok(strategies_list)
    }
    
    async fn load_strategy(&self, id: Uuid) -> Result<Option<crate::types::StrategyInstance>> {
        let mut conn = self.pool.get().await
            .map_err(|e| StrategyLoaderError::Connection(e.to_string()))?;
        
        let result: Option<(Strategy, DbStrategyInstance)> = strategies::table
            .inner_join(strategy_instances::table.on(
                strategy_instances::strategy_id.eq(strategies::id)
            ))
            .filter(strategy_instances::id.eq(id))
            .select((Strategy::as_select(), DbStrategyInstance::as_select()))
            .first(&mut conn)
            .await
            .optional()
            .map_err(|e| StrategyLoaderError::Database(e.to_string()))?;
        
        match result {
            Some((strategy, instance)) => {
                Ok(Some(Self::convert_to_strategy_instance(strategy, instance)?))
            }
            None => Ok(None),
        }
    }
    
    async fn load_strategy_by_name(&self, name: &str, version: &str) -> Result<Option<crate::types::StrategyInstance>> {
        let mut conn = self.pool.get().await
            .map_err(|e| StrategyLoaderError::Connection(e.to_string()))?;
        
        let result: Option<(Strategy, DbStrategyInstance)> = strategies::table
            .inner_join(strategy_instances::table.on(
                strategy_instances::strategy_id.eq(strategies::id)
            ))
            .filter(strategies::strategy_name.eq(name))
            .filter(strategies::version.eq(version))
            .select((Strategy::as_select(), DbStrategyInstance::as_select()))
            .first(&mut conn)
            .await
            .optional()
            .map_err(|e| StrategyLoaderError::Database(e.to_string()))?;
        
        match result {
            Some((strategy, instance)) => {
                Ok(Some(Self::convert_to_strategy_instance(strategy, instance)?))
            }
            None => Ok(None),
        }
    }
}
