//! Database strategy loader - loads strategies from PostgreSQL
//!
//! Connects to the same database as BacktestingEngine to load
//! deployed strategies from the `deployed_strategies` and `backtest_results` tables.
//!
//! ## New Architecture (2026)
//! 
//! Previously loaded from `strategies` + `strategy_instances` tables.
//! Now loads from:
//! - `deployed_strategies` - User-deployed strategies with status tracking
//! - `backtest_results` - Source backtest with strategy_metrics (parameters)

use async_trait::async_trait;
use uuid::Uuid;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use diesel_async::pooled_connection::deadpool::Pool;
use ultra_logger::{ultra_warn, ultra_info};

use databaseschema::models::strategy::{Strategy, StrategyInstance as DbStrategyInstance};
use databaseschema::models::deployed_strategy::DeployedStrategy;
use databaseschema::models::backtest_result::BacktestResult;
use databaseschema::schema::{deployed_strategies, backtest_results};

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
            python_source: None,
            metadata,
        })
    }
    
    /// Convert DeployedStrategy + BacktestResult to our StrategyInstance
    /// This is the NEW conversion for the deployed_strategies architecture
    fn convert_deployment_to_strategy_instance(
        deployment: DeployedStrategy,
        backtest: BacktestResult,
    ) -> Result<crate::types::StrategyInstance> {
        use bigdecimal::ToPrimitive;
        
        // Get strategy params from backtest_results.strategy_metrics JSON
        let strategy_metrics = backtest.strategy_metrics.unwrap_or(serde_json::json!({}));
        
        // Extract optimized_params from strategy_metrics
        let params_json = strategy_metrics.get("optimized_params")
            .cloned()
            .unwrap_or_else(|| strategy_metrics.clone());
        
        // Determine strategy type from backtest.strategy_name
        // Default to PortfolioMixed if we can't parse it
        let strategy_type: StrategyType = backtest.strategy_name.parse()
            .unwrap_or(StrategyType::PortfolioMixed);
        
        // Extract assets from parameters or use backtest symbol
        let assets = Self::parse_assets(&params_json).unwrap_or_else(|_| {
            vec![TradingAsset {
                symbol: backtest.symbol.clone(),
                exchange: deployment.exchange_targets.get(0)
                    .and_then(|e| e.clone())
                    .unwrap_or_else(|| "kraken".to_string()),
                weight: 1.0,
                risk_limits: AssetRiskLimits::default(),
            }]
        });
        
        // Extract strategy-specific parameters
        let strategy_params = Self::parse_strategy_params(strategy_type, &params_json)
            .unwrap_or_else(|_| StrategyParameters::Generic(GenericParams {
                params: Default::default(),
            }));
        
        // Extract portfolio risk limits, override with deployment capital and DB risk columns
        let mut portfolio_risk = Self::parse_portfolio_risk(&params_json);
        portfolio_risk.max_total_exposure = deployment.capital_allocation
            .to_f64()
            .unwrap_or(portfolio_risk.max_total_exposure);
        if let Some(ref v) = deployment.max_daily_loss {
            if let Some(f) = v.to_f64() { portfolio_risk.max_daily_loss = f; }
        }
        if let Some(ref v) = deployment.max_drawdown_pct {
            if let Some(f) = v.to_f64() { portfolio_risk.max_drawdown_pct = f; }
        }
        if let Some(mins) = deployment.cooldown_minutes {
            portfolio_risk.cooldown_minutes = mins as u32;
        }
        
        // Build metadata with deployment and backtest info
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("deployment_id".to_string(), serde_json::json!(deployment.id.to_string()));
        metadata.insert("backtest_result_id".to_string(), serde_json::json!(backtest.id.to_string()));
        metadata.insert("deployed_at".to_string(), serde_json::json!(deployment.deployed_at.to_rfc3339()));
        metadata.insert("backtest_sharpe".to_string(), serde_json::json!(
            backtest.sharpe_ratio.as_ref().and_then(|s| s.to_f64())
        ));
        metadata.insert("backtest_max_drawdown".to_string(), serde_json::json!(
            backtest.max_drawdown.to_f64()
        ));
        
        Ok(crate::types::StrategyInstance {
            id: deployment.id,  // Use deployment ID as the instance ID
            name: deployment.name,
            strategy_type,
            version: "1.0".to_string(),  // Deployments don't have versions
            assets,
            parameters: strategy_params,
            portfolio_risk,
            enabled: deployment.is_active,
            description: deployment.description,
            python_source: backtest.python_source_code,
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
    /// 
    /// Custom Python strategies use Generic params since their parameters
    /// are defined in the Python source code, not built-in structs.
    fn parse_strategy_params(
        strategy_type: StrategyType,
        params: &serde_json::Value,
    ) -> Result<StrategyParameters> {
        match strategy_type {
            StrategyType::Custom | StrategyType::CustomMarketMaking | StrategyType::PortfolioMixed => {
                // All strategy types now use generic params - parameters are defined
                // in the Python strategy source code, not in typed structs
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
        
        // Load all active deployments with their backtest results
        // This is the NEW architecture using deployed_strategies table
        let deployments: Vec<(DeployedStrategy, BacktestResult)> = deployed_strategies::table
            .inner_join(backtest_results::table.on(
                deployed_strategies::backtest_result_id.eq(backtest_results::id)
            ))
            .filter(deployed_strategies::is_active.eq(true))
            .select((DeployedStrategy::as_select(), BacktestResult::as_select()))
            .load(&mut conn)
            .await
            .map_err(|e| StrategyLoaderError::Database(e.to_string()))?;
        
        let mut strategies_list = Vec::new();
        
        for (deployment, backtest) in deployments {
            match Self::convert_deployment_to_strategy_instance(deployment, backtest) {
                Ok(si) => strategies_list.push(si),
                Err(e) => {
                    ultra_warn!(format!("Failed to parse deployed strategy: {}", e));
                }
            }
        }
        
        ultra_info!(format!("Loaded {} active deployments from database", strategies_list.len()));
        Ok(strategies_list)
    }
    
    async fn load_strategy(&self, id: Uuid) -> Result<Option<crate::types::StrategyInstance>> {
        let mut conn = self.pool.get().await
            .map_err(|e| StrategyLoaderError::Connection(e.to_string()))?;
        
        // Load deployment by ID (new architecture)
        let result: Option<(DeployedStrategy, BacktestResult)> = deployed_strategies::table
            .inner_join(backtest_results::table.on(
                deployed_strategies::backtest_result_id.eq(backtest_results::id)
            ))
            .filter(deployed_strategies::id.eq(id))
            .select((DeployedStrategy::as_select(), BacktestResult::as_select()))
            .first(&mut conn)
            .await
            .optional()
            .map_err(|e| StrategyLoaderError::Database(e.to_string()))?;
        
        match result {
            Some((deployment, backtest)) => {
                Ok(Some(Self::convert_deployment_to_strategy_instance(deployment, backtest)?))
            }
            None => Ok(None),
        }
    }
    
    async fn load_strategy_by_name(&self, name: &str, _version: &str) -> Result<Option<crate::types::StrategyInstance>> {
        let mut conn = self.pool.get().await
            .map_err(|e| StrategyLoaderError::Connection(e.to_string()))?;
        
        // Load deployment by name (new architecture)
        let result: Option<(DeployedStrategy, BacktestResult)> = deployed_strategies::table
            .inner_join(backtest_results::table.on(
                deployed_strategies::backtest_result_id.eq(backtest_results::id)
            ))
            .filter(deployed_strategies::name.eq(name))
            .filter(deployed_strategies::is_active.eq(true))
            .select((DeployedStrategy::as_select(), BacktestResult::as_select()))
            .first(&mut conn)
            .await
            .optional()
            .map_err(|e| StrategyLoaderError::Database(e.to_string()))?;
        
        match result {
            Some((deployment, backtest)) => {
                Ok(Some(Self::convert_deployment_to_strategy_instance(deployment, backtest)?))
            }
            None => Ok(None),
        }
    }
}
