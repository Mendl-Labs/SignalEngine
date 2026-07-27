//! Deployment Subscriber for Hot-Loading Strategy Deployments
//!
//! This module subscribes to deployment events from the message broker and
//! hot-loads/unloads strategies without requiring SignalEngine restart.
//!
//! # Topics Subscribed
//! - `strategy.deployment` - New strategy deployments
//! - `strategy.deactivation` - Strategy deactivation requests
//! - `strategy.status.request` - Deployment status requests
//!
//! # Flow
//! 1. BacktestingEngine publishes `StrategyDeployment` after approval
//! 2. DeploymentSubscriber receives event
//! 3. Subscriber loads strategy configuration and hot-loads into StrategyManager
//! 4. Subscriber publishes `StrategyDeploymentAck` with success/failure
//!
//! # Hot-Loading Strategy
//! - Strategies are loaded into a thread-safe `DashMap` in StrategyManager
//! - New strategies can be added while existing strategies continue running
//! - Deactivation gracefully stops strategies (close positions, cancel orders)

use chrono::Utc;
use dashmap::DashMap;
use prost::Message;
use ultra_logger::{ultra_info, ultra_warn};
use protocol::broker::messages::{
    publish_request, DeploymentStatusRequest, DeploymentStatusResponse,
    MarketDataSubscribe, MarketDataUnsubscribe,
    PublishRequest, StrategyDeactivation, StrategyDeployment, StrategyDeploymentAck,
    ActiveStrategyInfo,
};
use publisher::{PublisherConfig, UltraFastPublisher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use subscriber::{UltraFastMessage, UltraFastSubscriber};
use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;

#[cfg(feature = "postgres")]
use diesel::{ExpressionMethods, JoinOnDsl, OptionalExtension, QueryDsl, SelectableHelper};
#[cfg(feature = "postgres")]
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
#[cfg(feature = "postgres")]
use diesel_async::pooled_connection::deadpool::Pool;
#[cfg(feature = "postgres")]
use diesel_async::{AsyncPgConnection, RunQueryDsl};
#[cfg(feature = "postgres")]
use databaseschema::models::backtest_result::BacktestResult;
#[cfg(feature = "postgres")]
use databaseschema::models::deployed_strategy::DeployedStrategy as DbDeployedStrategy;
#[cfg(feature = "postgres")]
use databaseschema::schema::{backtest_jobs, backtest_results, deployed_strategies};
#[cfg(feature = "postgres")]
use bigdecimal::ToPrimitive;

/// Topics for deployment events
pub mod topics {
    pub const STRATEGY_DEPLOYMENT: &str = "strategy.deployment";
    pub const STRATEGY_DEACTIVATION: &str = "strategy.deactivation";
    pub const STRATEGY_DEPLOYMENT_ACK: &str = "strategy.deployment.ack";
    pub const STRATEGY_STATUS_REQUEST: &str = "strategy.status.request";
    pub const STRATEGY_STATUS_RESPONSE: &str = "strategy.status.response";
    /// Market data subscription topic (for DataEngine)
    pub const MARKET_DATA_SUBSCRIBE: &str = "market.subscription.subscribe";
    pub const MARKET_DATA_UNSUBSCRIBE: &str = "market.subscription.unsubscribe";
}

/// Errors that can occur during deployment subscription
#[derive(Debug, Error)]
pub enum DeploymentSubscriberError {
    #[error("Failed to connect to message broker: {0}")]
    ConnectionError(String),
    #[error("Failed to subscribe to topics: {0}")]
    SubscriptionError(String),
    #[error("Failed to parse deployment message: {0}")]
    ParseError(String),
    #[error("Failed to load strategy: {0}")]
    LoadError(String),
    #[error("Failed to publish acknowledgment: {0}")]
    AckError(String),
    #[error("Strategy manager not configured")]
    NoStrategyManager,
}

/// Extra per-deployment config fetched from the DB, not carried on the
/// `StrategyDeployment` wire message -- see
/// `DeploymentSubscriber::fetch_deployment_python_config`.
#[derive(Debug, Clone, Default)]
pub struct DeploymentPythonConfig {
    pub python_source_code: Option<String>,
    /// The strategy's declared bar interval (`backtest_jobs.params_json.candle_interval_minutes`).
    /// `PythonBridgeStrategy` needs this to aggregate ticks into real bars
    /// before calling `compute_signals()` -- without it, a strategy tuned
    /// for e.g. 4-hour bars would have its 25-bar lookback span 25 raw
    /// ticks (seconds) instead of ~100 hours, and its holding-period exit
    /// fire in seconds instead of hours.
    pub candle_interval_minutes: Option<i64>,
    /// The strategy's declared asset class (`backtest_jobs.params_json.asset_class`,
    /// e.g. "forex", "crypto"). Needed to warm-start a strategy's bar history
    /// from Massive/Polygon historical data on init: Polygon's forex ticker
    /// mapping (e.g. "USD-ZAR" -> "C:USDZAR") only fires when asset_class is
    /// exactly "forex" -- it is never inferred from the exchange string, so
    /// without this a forex warm-start fetch would silently use the wrong
    /// (crypto-style) ticker format.
    pub asset_class: Option<String>,
}

/// Resolves `asset_class` from a `backtest_jobs.params_json` value. Checks
/// the top-level key first, but every observed portfolio deployment (2+
/// symbols, e.g. a forex portfolio) leaves that top-level key `null` --
/// asset_class only gets set per-asset inside `portfolio_assets[].asset_class`
/// (a single top-level field can't represent a portfolio that could mix
/// asset classes). Falls back to the FIRST portfolio asset's declared
/// class, matching the existing simplification elsewhere in this codebase
/// of treating `symbols[0]`/`target_exchanges[0]` as the representative
/// symbol/exchange for a multi-symbol deployment (e.g. hostbuilder's
/// warm-start fetch and its live-mode credential loading).
///
/// Deliberately NOT gated behind `#[cfg(feature = "postgres")]` -- despite
/// living alongside DB-querying code in this file, this function is pure
/// (`serde_json::Value` in, `Option<String>` out) and both its call sites
/// are themselves already postgres-gated, so it must stay buildable in a
/// non-postgres build for its own unit tests below.
fn resolve_asset_class(params_json: &serde_json::Value) -> Option<String> {
    if let Some(s) = params_json.get("asset_class").and_then(|v| v.as_str()) {
        return Some(s.to_string());
    }
    params_json
        .get("portfolio_assets")?
        .as_array()?
        .iter()
        .next()?
        .get("asset_class")?
        .as_str()
        .map(String::from)
}

/// Information about a deployed strategy for tracking
#[derive(Debug)]
pub struct DeployedStrategy {
    pub strategy_id: Uuid,
    pub instance_id: Uuid,
    pub tenant_id: Uuid,
    pub strategy_type: String,
    pub strategy_name: String,
    pub parameters: serde_json::Value,
    pub target_exchanges: Vec<String>,
    pub symbols: Vec<String>,
    pub deployed_at: i64,
    pub is_active: AtomicBool,
    pub total_trades: AtomicU64,
    pub unrealized_pnl: std::sync::atomic::AtomicI64, // In basis points for lock-free
    pub realized_pnl: std::sync::atomic::AtomicI64,
    pub open_positions: std::sync::atomic::AtomicI32,
    pub pending_orders: std::sync::atomic::AtomicI32,
    /// Deployment mode: "paper" or "live"
    pub mode: String,
    /// Paper simulation config (only relevant in paper mode)
    pub slippage_bps: Option<f64>,
    pub fill_model: Option<String>,
    pub maker_fee_bps: Option<f64>,
    pub taker_fee_bps: Option<f64>,
    /// Capital allocated to this deployment (from `deployed_strategies.capital_allocation`,
    /// forwarded over the wire as `StrategyDeployment.initial_capital`). Used to size
    /// orders proportionally to account size instead of a fixed unit quantity.
    pub capital_allocation: f64,
    /// Fraction of `capital_allocation` to risk per entry (the strategy's own
    /// declared `position_size_pct`/`risk_per_trade`, when known). `None` until
    /// the deployment's backtest params are fetched (see Phase 2 DB lookup) --
    /// callers should fall back to a conservative default (matching
    /// BacktestingEngine's own `python_simulation.rs` default of 2%).
    ///
    /// KNOWN GAP: this is only ever populated via `risk_metrics` JSON, which
    /// `reconcile_active_deployments_from_db` builds from
    /// `backtest_jobs.params_json.position_size_pct` -- but that top-level
    /// key doesn't actually exist for AI-generated strategies (confirmed):
    /// the real value lives baked into `python_source_code`'s own
    /// `self.params` dict, not as a separate structured DB field, and
    /// BacktestingEngine's publisher never sends it on the wire either. In
    /// practice this is `None` today even for a strategy whose Python
    /// source declares e.g. `position_size_pct: 0.25`, silently falling
    /// back to the 2% default. Correct fix: have the worker report back the
    /// actual `self.params["position_size_pct"]` it initialized with (it's
    /// the single source of truth), not try to duplicate/parse the value
    /// from SQL. Not fixed here -- flagged as a follow-up.
    pub position_size_pct: Option<f64>,
    /// Leverage multiplier for margined order sizing (mirrors
    /// `deployed_strategies.leverage` / BacktestingEngine's
    /// `config::BacktestConfig.leverage`). Defaults to `1.0` (unleveraged)
    /// when absent from `risk_metrics`, matching pre-existing sizing
    /// behavior for deployments that predate this field.
    pub leverage: f64,
    /// The deployment's actual Python strategy source (`backtest_results.python_source_code`),
    /// fetched by strategy_id since it isn't carried on the `StrategyDeployment`
    /// wire message. `None` for genuine (non-AI-authored) strategy types, or
    /// when the lookup fails/postgres isn't enabled.
    pub python_source_code: Option<String>,
    /// The strategy's declared bar interval in minutes
    /// (`backtest_jobs.params_json.candle_interval_minutes`), fetched
    /// alongside `python_source_code`. `PythonBridgeStrategy` aggregates raw
    /// ticks into bars of this size before calling `compute_signals()` --
    /// see `DeploymentPythonConfig`'s doc for why this matters.
    pub candle_interval_minutes: Option<i64>,
    /// The strategy's declared asset class, see `DeploymentPythonConfig::asset_class`.
    pub asset_class: Option<String>,
}

impl DeployedStrategy {
    /// Create a new deployed strategy from a deployment message.
    ///
    /// `python_config` is resolved by the caller (not looked up here) -- see
    /// `DeploymentSubscriber::fetch_deployment_python_config` for the
    /// live/hot path, and `reconcile_active_deployments_from_db` for the
    /// reconcile path, which already has `python_source_code`/
    /// `candle_interval_minutes` in scope from its own DB join.
    pub fn from_deployment(
        msg: &StrategyDeployment,
        python_config: DeploymentPythonConfig,
    ) -> Result<Self, DeploymentSubscriberError> {
        let strategy_id = Uuid::parse_str(&msg.strategy_id)
            .map_err(|e| DeploymentSubscriberError::ParseError(format!("Invalid strategy_id: {}", e)))?;
        let instance_id = Uuid::parse_str(&msg.instance_id)
            .map_err(|e| DeploymentSubscriberError::ParseError(format!("Invalid instance_id: {}", e)))?;
        let tenant_id = Uuid::parse_str(&msg.tenant_id)
            .map_err(|e| DeploymentSubscriberError::ParseError(format!("Invalid tenant_id: {}", e)))?;

        let parameters: serde_json::Value = if msg.parameters.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&msg.parameters)
                .map_err(|e| DeploymentSubscriberError::ParseError(format!("Invalid parameters: {}", e)))?
        };

        // Read mode from the dedicated proto field, falling back to risk_metrics JSON
        let risk_meta: serde_json::Value = if !msg.risk_metrics.is_empty() {
            serde_json::from_slice(&msg.risk_metrics).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        };

        let mode = if !msg.mode.is_empty() {
            msg.mode.clone()
        } else {
            risk_meta.get("mode").and_then(|m| m.as_str()).unwrap_or("paper").to_string()
        };

        // Extract optional sim config from risk_metrics JSON
        let slippage_bps = risk_meta.get("slippage_bps").and_then(|v| v.as_f64());
        let fill_model = risk_meta.get("fill_model").and_then(|v| v.as_str()).map(String::from);
        let maker_fee_bps = risk_meta.get("maker_fee_bps").and_then(|v| v.as_f64());
        let taker_fee_bps = risk_meta.get("taker_fee_bps").and_then(|v| v.as_f64());

        Ok(Self {
            strategy_id,
            instance_id,
            tenant_id,
            strategy_type: msg.strategy_type.clone(),
            strategy_name: msg.strategy_name.clone(),
            parameters,
            target_exchanges: msg.target_exchanges.clone(),
            symbols: msg.symbols.clone(),
            deployed_at: Utc::now().timestamp_millis(),
            is_active: AtomicBool::new(true),
            total_trades: AtomicU64::new(0),
            unrealized_pnl: std::sync::atomic::AtomicI64::new(0),
            realized_pnl: std::sync::atomic::AtomicI64::new(0),
            open_positions: std::sync::atomic::AtomicI32::new(0),
            pending_orders: std::sync::atomic::AtomicI32::new(0),
            mode,
            slippage_bps,
            fill_model,
            maker_fee_bps,
            taker_fee_bps,
            capital_allocation: msg.initial_capital,
            position_size_pct: risk_meta.get("position_size_pct").and_then(|v| v.as_f64()),
            leverage: risk_meta.get("leverage").and_then(|v| v.as_f64()).unwrap_or(1.0),
            python_source_code: python_config.python_source_code,
            candle_interval_minutes: python_config.candle_interval_minutes,
            asset_class: python_config.asset_class,
        })
    }

    /// Convert to ActiveStrategyInfo for status responses
    pub fn to_active_info(&self) -> ActiveStrategyInfo {
        ActiveStrategyInfo {
            strategy_id: self.strategy_id.to_string(),
            instance_id: self.instance_id.to_string(),
            tenant_id: self.tenant_id.to_string(),
            strategy_type: self.strategy_type.clone(),
            strategy_name: self.strategy_name.clone(),
            active_exchanges: self.target_exchanges.clone(),
            symbols: self.symbols.clone(),
            unrealized_pnl: std::sync::atomic::AtomicI64::load(&self.unrealized_pnl, Ordering::Relaxed) as f64 / 10000.0,
            realized_pnl: std::sync::atomic::AtomicI64::load(&self.realized_pnl, Ordering::Relaxed) as f64 / 10000.0,
            open_positions: std::sync::atomic::AtomicI32::load(&self.open_positions, Ordering::Relaxed),
            pending_orders: std::sync::atomic::AtomicI32::load(&self.pending_orders, Ordering::Relaxed),
            deployed_at: self.deployed_at,
            total_trades: std::sync::atomic::AtomicU64::load(&self.total_trades, Ordering::Relaxed) as i64,
        }
    }
}

/// Deployment subscriber that hot-loads strategies
pub struct DeploymentSubscriber {
    /// Broker address for connections
    broker_address: String,
    /// Node identifier for this SignalEngine instance
    node_id: String,
    /// Publisher for acknowledgments
    publisher: Option<Arc<UltraFastPublisher>>,
    /// Active deployed strategies (key: instance_id)
    deployed_strategies: Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
    /// Running flag
    is_running: AtomicBool,
    /// Channel for deployment events to strategy manager
    deployment_tx: Option<mpsc::Sender<DeploymentEvent>>,
}

/// Events sent to the strategy manager
#[derive(Debug, Clone)]
pub enum DeploymentEvent {
    Deploy(Arc<DeployedStrategy>),
    Deactivate {
        instance_id: Uuid,
        reason: String,
        close_positions: bool,
        cancel_orders: bool,
    },
}

impl DeploymentSubscriber {
    fn resolved_database_url() -> Result<String, DeploymentSubscriberError> {
        let raw = match std::env::var("DATABASE_URL") {
            Ok(url) => url,
            Err(_) => return Ok(String::new()),
        };

        // Kubernetes env values are not shell-expanded; resolve placeholders
        // such as $(POSTGRES_USER) from sibling POSTGRES_* env vars.
        let mut resolved = raw.clone();
        for key in [
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
        ] {
            if let Ok(value) = std::env::var(key) {
                let needle = format!("$({})", key);
                resolved = resolved.replace(&needle, &value);
            }
        }

        if resolved.contains("$(") {
            return Err(DeploymentSubscriberError::LoadError(format!(
                "DATABASE_URL contains unresolved placeholders: {}",
                raw
            )));
        }

        Ok(resolved)
    }

    /// Look up the deployment's actual Python strategy source and its
    /// declared bar interval, keyed by the backtest result id (published as
    /// `StrategyDeployment.strategy_id` -- see `deployment_publisher.rs`'s
    /// `strategy_id: backtest_result_id.to_string()`).
    ///
    /// Not carried on the wire message itself: adding fields there would
    /// touch the shared cross-service proto contract
    /// (`MessageBrokerEngine/protos/messages.proto`) for zero benefit until a
    /// consumer exists. A one-off connection per deployment event is
    /// acceptable here since deployments are rare, not a hot path.
    #[cfg(feature = "postgres")]
    async fn fetch_deployment_python_config(strategy_id: &str) -> DeploymentPythonConfig {
        async fn inner(strategy_id: &str) -> Option<DeploymentPythonConfig> {
            let backtest_result_id = Uuid::parse_str(strategy_id).ok()?;
            let database_url = DeploymentSubscriber::resolved_database_url().ok()?;
            if database_url.is_empty() {
                return None;
            }

            let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&database_url);
            let pool: Pool<AsyncPgConnection> = Pool::builder(manager).max_size(1).build().ok()?;
            let mut conn = pool.get().await.ok()?;

            let python_source_code: Option<String> = backtest_results::table
                .filter(backtest_results::id.eq(backtest_result_id))
                .select(backtest_results::python_source_code)
                .first(&mut conn)
                .await
                .ok()?;

            // candle_interval_minutes and asset_class both live on the
            // originating backtest_jobs row's params_json, not on
            // backtest_results itself -- same join reconcile_active_deployments_from_db
            // already does for candle_interval_minutes.
            let params_json: Option<serde_json::Value> = backtest_jobs::table
                .filter(backtest_jobs::result_id.eq(Some(backtest_result_id)))
                .select(backtest_jobs::params_json)
                .first::<serde_json::Value>(&mut conn)
                .await
                .optional()
                .ok()
                .flatten();
            let candle_interval_minutes = params_json.as_ref()
                .and_then(|params| params.get("candle_interval_minutes").and_then(|v| v.as_i64()));
            let asset_class = params_json.as_ref().and_then(resolve_asset_class);

            Some(DeploymentPythonConfig { python_source_code, candle_interval_minutes, asset_class })
        }

        inner(strategy_id).await.unwrap_or_default()
    }

    #[cfg(not(feature = "postgres"))]
    async fn fetch_deployment_python_config(_strategy_id: &str) -> DeploymentPythonConfig {
        DeploymentPythonConfig::default()
    }

    #[cfg(feature = "postgres")]
    fn resolve_reconcile_exchanges(deployment: &DbDeployedStrategy) -> Vec<String> {
        let mut exchanges: Vec<String> = deployment
            .exchange_targets
            .iter()
            .filter_map(|e| e.as_ref().map(|s| s.trim().to_string()))
            .filter(|s| !s.is_empty())
            .collect();

        if exchanges.is_empty() {
            if let Some(metadata) = &deployment.metadata {
                if let Some(arr) = metadata.get("exchange_targets").and_then(|v| v.as_array()) {
                    exchanges.extend(arr.iter().filter_map(|v| v.as_str()).map(|s| s.trim().to_string()));
                }

                if exchanges.is_empty() {
                    if let Some(exchange) = metadata
                        .get("exchange")
                        .or_else(|| metadata.get("target_exchange"))
                        .and_then(|v| v.as_str())
                    {
                        let ex = exchange.trim().to_string();
                        if !ex.is_empty() {
                            exchanges.push(ex);
                        }
                    }
                }
            }
        }

        if exchanges.is_empty() {
            exchanges.push("kraken".to_string());
        }

        exchanges
    }

    /// Resolve the per-asset symbol list to subscribe to, from the backtest
    /// job's `params_json.portfolio_assets`.
    ///
    /// `backtest_results.symbol` is a single display-string column: for a
    /// portfolio backtest it holds every asset's symbol joined with ", "
    /// (e.g. "USD-ZAR, AUD-NZD, CHF-ZAR") for human-readable display. Passing
    /// that joined string straight through as a `MarketDataSubscribe` symbol
    /// looks up a ticker that doesn't exist on any exchange, so the
    /// subscription silently matches nothing and the deployment never
    /// receives data -- with no error anywhere, since publishing the
    /// subscribe request itself succeeds. `portfolio_assets` carries the
    /// real, structured per-asset breakdown and is always present for
    /// portfolio backtests; single-asset backtests have no
    /// `portfolio_assets`, so `single_symbol` is the correct value there.
    #[cfg(feature = "postgres")]
    fn resolve_reconcile_symbols(single_symbol: &str, params_json: &serde_json::Value) -> Vec<String> {
        let portfolio_symbols: Vec<String> = params_json
            .get("portfolio_assets")
            .and_then(|v| v.as_array())
            .map(|assets| {
                assets
                    .iter()
                    .filter_map(|asset| asset.get("symbol").and_then(|s| s.as_str()))
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        if portfolio_symbols.is_empty() {
            vec![single_symbol.to_string()]
        } else {
            portfolio_symbols
        }
    }

    /// Create a new deployment subscriber
    pub fn new(broker_address: &str, node_id: &str) -> Self {
        Self {
            broker_address: broker_address.to_string(),
            node_id: node_id.to_string(),
            publisher: None,
            deployed_strategies: Arc::new(DashMap::new()),
            is_running: AtomicBool::new(false),
            deployment_tx: None,
        }
    }

    /// Set the channel for sending deployment events to strategy manager
    pub fn set_deployment_channel(&mut self, tx: mpsc::Sender<DeploymentEvent>) {
        self.deployment_tx = Some(tx);
    }

    /// Get the deployed strategies map for external access
    pub fn get_deployed_strategies(&self) -> Arc<DashMap<Uuid, Arc<DeployedStrategy>>> {
        self.deployed_strategies.clone()
    }

    /// Start the deployment subscriber
    pub async fn start(&mut self) -> Result<(), DeploymentSubscriberError> {
        // Connect publisher for acknowledgments
        let pub_config = PublisherConfig::new(&self.broker_address);
        let publisher = UltraFastPublisher::new(pub_config);
        publisher.connect().await.map_err(|e| {
            DeploymentSubscriberError::ConnectionError(format!("Publisher: {:?}", e))
        })?;
        self.publisher = Some(Arc::new(publisher));

        // Create subscriber (ID from node_id hash)
        let subscriber_id = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.node_id.hash(&mut hasher);
            hasher.finish()
        };
        let subscriber = Arc::new(UltraFastSubscriber::new(subscriber_id));

        // Subscribe to deployment topics
        subscriber.subscribe_to_topic(topics::STRATEGY_DEPLOYMENT).await.map_err(|e| {
            DeploymentSubscriberError::SubscriptionError(format!("{:?}", e))
        })?;
        subscriber.subscribe_to_topic(topics::STRATEGY_DEACTIVATION).await.map_err(|e| {
            DeploymentSubscriberError::SubscriptionError(format!("{:?}", e))
        })?;
        subscriber.subscribe_to_topic(topics::STRATEGY_STATUS_REQUEST).await.map_err(|e| {
            DeploymentSubscriberError::SubscriptionError(format!("{:?}", e))
        })?;

        // Start the subscriber receive loop after all SUBSCRIBE_ACKs are received.
        subscriber.start();

        self.is_running.store(true, Ordering::SeqCst);

        // Spawn message processing task
        let deployed_strategies = self.deployed_strategies.clone();
        let publisher = self.publisher.clone();
        let node_id = self.node_id.clone();
        let deployment_tx = self.deployment_tx.clone();
        let is_running = Arc::new(AtomicBool::new(true));
        let is_running_clone = is_running.clone();

        tokio::spawn(async move {
            Self::process_messages(
                subscriber,
                deployed_strategies,
                publisher,
                node_id,
                deployment_tx,
                is_running_clone,
            )
            .await;
        });

        Ok(())
    }

    /// Reconcile active deployments from DB after startup.
    ///
    /// This replays deployment events for strategies that were already active
    /// before SignalEngine restarted, ensuring market data subscriptions are
    /// re-established in demand-driven DataEngine mode.
    #[cfg(feature = "postgres")]
    pub async fn reconcile_active_deployments_from_db(
        &self,
    ) -> Result<usize, DeploymentSubscriberError> {
        let database_url = Self::resolved_database_url()?;
        if database_url.is_empty() {
            return Ok(0);
        }

        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&database_url);
        let pool: Pool<AsyncPgConnection> = Pool::builder(manager)
            .max_size(5)
            .build()
            .map_err(|e| DeploymentSubscriberError::LoadError(format!("DB pool: {}", e)))?;
        let mut conn = pool
            .get()
            .await
            .map_err(|e| DeploymentSubscriberError::LoadError(format!("DB connection: {}", e)))?;

        let rows: Vec<(DbDeployedStrategy, BacktestResult)> = deployed_strategies::table
            .inner_join(
                backtest_results::table.on(deployed_strategies::backtest_result_id.eq(backtest_results::id)),
            )
            .filter(deployed_strategies::is_active.eq(true))
            .filter(deployed_strategies::status.eq("active"))
            .select((DbDeployedStrategy::as_select(), BacktestResult::as_select()))
            .load(&mut conn)
            .await
            .map_err(|e| DeploymentSubscriberError::LoadError(format!("Query active deployments: {}", e)))?;

        let mut replayed = 0usize;

        for (deployment, backtest) in rows {
            let (strategy_type, params_json) = backtest_jobs::table
                .filter(backtest_jobs::result_id.eq(Some(deployment.backtest_result_id)))
                .select((backtest_jobs::strategy_type, backtest_jobs::params_json))
                .first::<(String, serde_json::Value)>(&mut conn)
                .await
                .optional()
                .map_err(|e| {
                    DeploymentSubscriberError::LoadError(format!(
                        "Query strategy type for deployment {}: {}",
                        deployment.id, e
                    ))
                })?
                .unwrap_or_else(|| ("custom".to_string(), serde_json::Value::Null));

            let exchanges = Self::resolve_reconcile_exchanges(&deployment);
            let symbols = Self::resolve_reconcile_symbols(&backtest.symbol, &params_json);

            // Carry the strategy's own declared position_size_pct (if present in
            // the backtest's params) and its deployed leverage through
            // risk_metrics -- from_deployment() reads both back out on the
            // other side. capital_allocation goes through the dedicated
            // initial_capital field. leverage comes from the deployment row
            // itself (resolved once at deploy time), not params_json, since
            // that's the actual value the deployment was created with.
            let position_size_pct = params_json
                .get("position_size_pct")
                .or_else(|| params_json.get("risk_per_trade"));
            let leverage = deployment.leverage.to_f64().unwrap_or(1.0);
            let mut risk_metrics_map = serde_json::Map::new();
            if let Some(v) = position_size_pct {
                risk_metrics_map.insert("position_size_pct".to_string(), v.clone());
            }
            risk_metrics_map.insert("leverage".to_string(), serde_json::json!(leverage));
            let risk_metrics = serde_json::to_vec(&serde_json::Value::Object(risk_metrics_map))
                .unwrap_or_default();

            let deployment_msg = StrategyDeployment {
                strategy_id: deployment.backtest_result_id.to_string(),
                instance_id: deployment.id.to_string(),
                tenant_id: deployment.tenant_id.to_string(),
                strategy_type,
                strategy_name: deployment.name.clone(),
                version: "1.0".to_string(),
                parameters: Vec::new(),
                initial_capital: deployment.capital_allocation.to_f64().unwrap_or(0.0),
                target_exchanges: exchanges,
                symbols,
                approved_by: deployment.deployed_by.unwrap_or_else(|| "reconciler".to_string()),
                approved_at: deployment.deployed_at.to_rfc3339(),
                performance_summary: Vec::new(),
                risk_metrics,
                admin_approved: false,
                timestamp: Utc::now().timestamp(),
                mode: deployment.mode,
            };

            let candle_interval_minutes = params_json
                .get("candle_interval_minutes")
                .and_then(|v| v.as_i64());
            let asset_class = resolve_asset_class(&params_json);

            Self::handle_deployment(
                deployment_msg,
                DeploymentPythonConfig {
                    python_source_code: backtest.python_source_code.clone(),
                    candle_interval_minutes,
                    asset_class,
                },
                &self.deployed_strategies,
                self.publisher.as_ref(),
                &self.node_id,
                self.deployment_tx.as_ref(),
            )
            .await;

            replayed += 1;
        }

        Ok(replayed)
    }

    /// Process incoming messages
    async fn process_messages(
        subscriber: Arc<UltraFastSubscriber>,
        deployed_strategies: Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<Arc<UltraFastPublisher>>,
        node_id: String,
        deployment_tx: Option<mpsc::Sender<DeploymentEvent>>,
        is_running: Arc<AtomicBool>,
    ) {
        let topics_to_poll = [
            topics::STRATEGY_DEPLOYMENT,
            topics::STRATEGY_DEACTIVATION,
            topics::STRATEGY_STATUS_REQUEST,
        ];

        while std::sync::atomic::AtomicBool::load(&is_running, Ordering::Relaxed) {
            let mut had_message = false;

            // Poll each topic for messages
            for topic in &topics_to_poll {
                if let Some(msg) = subscriber.get_message_from_topic(topic) {
                    had_message = true;
                    Self::handle_message(
                        msg,
                        &deployed_strategies,
                        publisher.as_ref(),
                        &node_id,
                        deployment_tx.as_ref(),
                    )
                    .await;
                }
            }

            // If no messages, sleep briefly to avoid busy loop
            if !had_message {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }
    }

    /// Handle a single message
    async fn handle_message(
        msg: UltraFastMessage,
        deployed_strategies: &Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        node_id: &str,
        deployment_tx: Option<&mpsc::Sender<DeploymentEvent>>,
    ) {
        // Try to decode as PublishRequest
        if let Ok(request) = PublishRequest::decode(msg.data.as_slice()) {
            match request.payload {
                Some(publish_request::Payload::StrategyDeployment(deployment)) => {
                    let python_config = Self::fetch_deployment_python_config(&deployment.strategy_id).await;
                    Self::handle_deployment(
                        deployment,
                        python_config,
                        deployed_strategies,
                        publisher,
                        node_id,
                        deployment_tx,
                    )
                    .await;
                }
                Some(publish_request::Payload::StrategyDeactivation(deactivation)) => {
                    Self::handle_deactivation(
                        deactivation,
                        deployed_strategies,
                        publisher,
                        node_id,
                        deployment_tx,
                    )
                    .await;
                }
                Some(publish_request::Payload::RawData(data)) => {
                    // Could be a status request
                    if let Ok(status_req) = serde_json::from_slice::<DeploymentStatusRequest>(&data)
                    {
                        Self::handle_status_request(
                            status_req,
                            deployed_strategies,
                            publisher,
                            node_id,
                        )
                        .await;
                    }
                }
                _ => {} // Ignore other message types
            }
        }
    }

    /// Handle a deployment message.
    ///
    /// `python_config` is resolved by the caller -- the reconcile path
    /// already has it from its own DB join; the live/hot path fetches it via
    /// `fetch_deployment_python_config` before calling this.
    async fn handle_deployment(
        deployment: StrategyDeployment,
        python_config: DeploymentPythonConfig,
        deployed_strategies: &Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        node_id: &str,
        deployment_tx: Option<&mpsc::Sender<DeploymentEvent>>,
    ) {
        let (success, error_message, active_exchanges, symbols, instance_id_str, tenant_id_str) =
            match DeployedStrategy::from_deployment(&deployment, python_config) {
            Ok(strategy) => {
                let instance_id = strategy.instance_id;
                let exchanges = strategy.target_exchanges.clone();
                let symbols = strategy.symbols.clone();
                let instance_id_str = strategy.instance_id.to_string();
                let tenant_id_str = strategy.tenant_id.to_string();
                let strategy = Arc::new(strategy);

                // Add to deployed strategies map
                deployed_strategies.insert(instance_id, strategy.clone());

                // Notify strategy manager
                if let Some(tx) = deployment_tx {
                    let _ = tx.send(DeploymentEvent::Deploy(strategy)).await;
                }

                (true, String::new(), exchanges, symbols, instance_id_str, tenant_id_str)
            }
            Err(e) => (false, e.to_string(), Vec::new(), Vec::new(), String::new(), String::new()),
        };

        // Send acknowledgment
        if let Some(pub_arc) = publisher {
            let ack = StrategyDeploymentAck {
                strategy_id: deployment.strategy_id.clone(),
                instance_id: deployment.instance_id.clone(),
                signal_engine_node: node_id.to_string(),
                success,
                error_message: error_message.clone(),
                loaded_at: Utc::now().timestamp_millis(),
                active_exchanges: active_exchanges.clone(),
            };

            let request = PublishRequest {
                topic: topics::STRATEGY_DEPLOYMENT_ACK.to_string(),
                payload: Some(publish_request::Payload::StrategyDeploymentAck(ack)),
            };

            let encoded = request.encode_to_vec();
            let _ = pub_arc.publish_raw(encoded, topics::STRATEGY_DEPLOYMENT_ACK).await;
            let _ = pub_arc.flush().await;

            // If deployment succeeded, publish market data subscriptions for each exchange
            if success && !active_exchanges.is_empty() && !symbols.is_empty() {
                // Market-making strategies require L3 order book data for realistic simulation
                // and quote placement. Directional strategies only need trade ticks.
                // Capability lookup tolerates legacy aliases ("market_making",
                // "MarketMaking", etc.) that the previous literal compare missed.
                let caps = crate::types::data_requirements_for_strategy_type(&deployment.strategy_type);
                // Data-type vocabulary must match DataEngine's tenant tier allowlist
                // (see DataEngine/hostbuilder/src/core/tenant_subscription_limits.rs).
                // Canonical names are plural: "trades", "orderbook".
                let mut data_types: Vec<String> = Vec::with_capacity(2);
                if caps.needs_orderbook {
                    data_types.push("orderbook".to_string());
                }
                if caps.needs_trades {
                    data_types.push("trades".to_string());
                }
                let orderbook_depth: i32 = caps.orderbook_depth as i32;

                for exchange in &active_exchanges {
                    let subscription_id = format!("{}_{}", instance_id_str, exchange);
                    
                    let market_sub = MarketDataSubscribe {
                        subscription_id: subscription_id.clone(),
                        tenant_id: tenant_id_str.clone(),
                        strategy_instance_id: instance_id_str.clone(),
                        exchange: exchange.clone(),
                        symbols: symbols.clone(),
                        data_types: data_types.clone(),
                        orderbook_depth,
                        timestamp: Utc::now().timestamp_millis(),
                    };

                    // Wrap in PublishRequest with RawData payload — DataEngine's SubscriptionManager
                    // decodes PublishRequest and expects MarketDataSubscribe bytes inside RawData.
                    let market_sub_bytes = market_sub.encode_to_vec();
                    let request = PublishRequest {
                        topic: topics::MARKET_DATA_SUBSCRIBE.to_string(),
                        payload: Some(publish_request::Payload::RawData(market_sub_bytes)),
                    };
                    let encoded = request.encode_to_vec();
                    if let Err(_e) = pub_arc.publish_raw(encoded, topics::MARKET_DATA_SUBSCRIBE).await {
                        ultra_warn!(format!(
                            "⚠️ Failed to publish MarketDataSubscribe for exchange {}: {:?}",
                            exchange, _e
                        ));
                    } else if let Err(_e) = pub_arc.flush().await {
                        ultra_warn!(format!(
                            "⚠️ Failed to flush MarketDataSubscribe for exchange {}: {:?}",
                            exchange, _e
                        ));
                    } else {
                        ultra_info!(format!(
                            "📡 Published MarketDataSubscribe for exchange={} symbols={:?}",
                            exchange, symbols
                        ));
                    }
                }
            }
        }
    }

    /// Handle a deactivation message
    async fn handle_deactivation(
        deactivation: StrategyDeactivation,
        deployed_strategies: &Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        _node_id: &str,
        deployment_tx: Option<&mpsc::Sender<DeploymentEvent>>,
    ) {
        if let Ok(instance_id) = Uuid::parse_str(&deactivation.instance_id) {
            // Get strategy info before removing
            let strategy_info = deployed_strategies.get(&instance_id).map(|s| {
                (
                    s.instance_id.to_string(),
                    s.target_exchanges.clone(),
                    s.symbols.clone(),
                )
            });

            // Mark as inactive
            if let Some(strategy) = deployed_strategies.get(&instance_id) {
                strategy.is_active.store(false, Ordering::SeqCst);
            }

            // Remove from map
            deployed_strategies.remove(&instance_id);

            // Publish market data unsubscribe for each exchange
            if let (Some(pub_arc), Some((inst_id, exchanges, symbols))) = 
                (publisher, strategy_info) 
            {
                for exchange in &exchanges {
                    let subscription_id = format!("{}_{}", inst_id, exchange);
                    
                    let market_unsub = MarketDataUnsubscribe {
                        subscription_id: subscription_id.clone(),
                        strategy_instance_id: inst_id.clone(),
                        exchange: exchange.clone(),
                        symbols: symbols.clone(),
                        reason: deactivation.reason.clone(),
                        timestamp: Utc::now().timestamp_millis(),
                    };

                    let encoded = market_unsub.encode_to_vec();
                    let _ = pub_arc.publish_raw(encoded, topics::MARKET_DATA_UNSUBSCRIBE).await;
                    let _ = pub_arc.flush().await;
                }
            }

            // Notify strategy manager
            if let Some(tx) = deployment_tx {
                let _ = tx
                    .send(DeploymentEvent::Deactivate {
                        instance_id,
                        reason: deactivation.reason,
                        close_positions: deactivation.close_positions,
                        cancel_orders: deactivation.cancel_orders,
                    })
                    .await;
            }
        }
    }

    /// Handle a status request
    async fn handle_status_request(
        request: DeploymentStatusRequest,
        deployed_strategies: &Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        node_id: &str,
    ) {
        let mut active_strategies = Vec::new();
        let mut total_memory: i64 = 0;

        for entry in deployed_strategies.iter() {
            let strategy = entry.value();

            // Filter by tenant if specified
            if !request.tenant_id.is_empty() && strategy.tenant_id.to_string() != request.tenant_id
            {
                continue;
            }

            active_strategies.push(strategy.to_active_info());
            total_memory += std::mem::size_of::<DeployedStrategy>() as i64;
        }

        if let Some(pub_arc) = publisher {
            let response = DeploymentStatusResponse {
                request_id: request.request_id,
                signal_engine_node: node_id.to_string(),
                active_strategies,
                total_memory_bytes: total_memory,
                cpu_utilization: 0.0, // TODO: Get actual CPU usage
            };

            let request = PublishRequest {
                topic: topics::STRATEGY_STATUS_RESPONSE.to_string(),
                payload: Some(publish_request::Payload::DeploymentStatusResponse(response)),
            };

            let encoded = request.encode_to_vec();
            let _ = pub_arc
                .publish_raw(encoded, topics::STRATEGY_STATUS_RESPONSE)
                .await;
            let _ = pub_arc.flush().await;
        }
    }

    /// Stop the deployment subscriber
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }

    /// Get count of deployed strategies
    pub fn deployed_count(&self) -> usize {
        self.deployed_strategies.len()
    }

    /// Check if a specific instance is deployed
    pub fn is_deployed(&self, instance_id: &Uuid) -> bool {
        self.deployed_strategies.contains_key(instance_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_constants() {
        assert_eq!(topics::STRATEGY_DEPLOYMENT, "strategy.deployment");
        assert_eq!(topics::STRATEGY_DEACTIVATION, "strategy.deactivation");
    }

    #[test]
    fn resolve_asset_class_prefers_top_level_key() {
        let params = serde_json::json!({
            "asset_class": "crypto",
            "portfolio_assets": [{"asset_class": "forex"}],
        });
        assert_eq!(resolve_asset_class(&params), Some("crypto".to_string()));
    }

    #[test]
    fn resolve_asset_class_falls_back_to_first_portfolio_asset() {
        // Confirmed live in production: every observed oanda portfolio
        // deployment leaves the top-level asset_class null and only sets it
        // per-asset inside portfolio_assets.
        let params = serde_json::json!({
            "asset_class": null,
            "portfolio_assets": [
                {"symbol": "USD-ZAR", "asset_class": "forex"},
                {"symbol": "AUD-NZD", "asset_class": "forex"},
            ],
        });
        assert_eq!(resolve_asset_class(&params), Some("forex".to_string()));
    }

    #[test]
    fn resolve_asset_class_none_when_both_sources_absent() {
        let params = serde_json::json!({"asset_class": null});
        assert_eq!(resolve_asset_class(&params), None);
    }

    #[test]
    fn resolve_asset_class_none_when_portfolio_assets_is_empty() {
        let params = serde_json::json!({"asset_class": null, "portfolio_assets": []});
        assert_eq!(resolve_asset_class(&params), None);
    }

    #[test]
    fn resolve_asset_class_none_when_first_portfolio_asset_lacks_the_field() {
        let params = serde_json::json!({
            "asset_class": null,
            "portfolio_assets": [{"symbol": "USD-ZAR"}],
        });
        assert_eq!(resolve_asset_class(&params), None);
    }

    #[test]
    fn test_deployed_strategy_from_deployment() {
        let deployment = StrategyDeployment {
            strategy_id: Uuid::new_v4().to_string(),
            instance_id: Uuid::new_v4().to_string(),
            tenant_id: Uuid::new_v4().to_string(),
            strategy_type: "custom_market_making".to_string(),
            strategy_name: "BTC Market Maker".to_string(),
            version: "1.0.0".to_string(),
            parameters: serde_json::to_vec(&serde_json::json!({"gamma": 0.1})).unwrap(),
            initial_capital: 10000.0,
            target_exchanges: vec!["kraken".to_string()],
            symbols: vec!["BTCUSD".to_string()],
            approved_by: "admin".to_string(),
            approved_at: "2026-01-25T12:00:00Z".to_string(),
            performance_summary: vec![],
            risk_metrics: vec![],
            admin_approved: true,
            timestamp: 0,
            mode: "paper".to_string(),
        };

        let strategy = DeployedStrategy::from_deployment(&deployment, DeploymentPythonConfig::default()).unwrap();
        assert_eq!(strategy.strategy_type, "custom_market_making");
        assert_eq!(strategy.strategy_name, "BTC Market Maker");
        assert!(strategy.is_active.load(Ordering::Relaxed));
        assert_eq!(strategy.capital_allocation, 10000.0);
        assert_eq!(strategy.position_size_pct, None);
        assert_eq!(strategy.leverage, 1.0);
        assert_eq!(strategy.python_source_code, None);
        assert_eq!(strategy.candle_interval_minutes, None);
    }

    #[test]
    fn test_deployed_strategy_reads_leverage_from_risk_metrics() {
        let deployment = StrategyDeployment {
            strategy_id: Uuid::new_v4().to_string(),
            instance_id: Uuid::new_v4().to_string(),
            tenant_id: Uuid::new_v4().to_string(),
            strategy_type: "custom".to_string(),
            strategy_name: "LeveragedMomentum".to_string(),
            version: "1.0.0".to_string(),
            parameters: Vec::new(),
            initial_capital: 10000.0,
            target_exchanges: vec!["kraken".to_string()],
            symbols: vec!["BTCUSD".to_string()],
            approved_by: "admin".to_string(),
            approved_at: "2026-01-25T12:00:00Z".to_string(),
            performance_summary: vec![],
            risk_metrics: serde_json::to_vec(
                &serde_json::json!({"position_size_pct": 0.1, "leverage": 3.0}),
            )
            .unwrap(),
            admin_approved: true,
            timestamp: 0,
            mode: "paper".to_string(),
        };

        let strategy =
            DeployedStrategy::from_deployment(&deployment, DeploymentPythonConfig::default()).unwrap();
        assert_eq!(strategy.leverage, 3.0);
        assert_eq!(strategy.position_size_pct, Some(0.1));
    }

    #[test]
    fn test_deployed_strategy_reads_position_size_pct_from_risk_metrics() {
        let deployment = StrategyDeployment {
            strategy_id: Uuid::new_v4().to_string(),
            instance_id: Uuid::new_v4().to_string(),
            tenant_id: Uuid::new_v4().to_string(),
            strategy_type: "custom".to_string(),
            strategy_name: "AggressiveMeanReversion".to_string(),
            version: "1.0.0".to_string(),
            parameters: Vec::new(),
            initial_capital: 10000.0,
            target_exchanges: vec!["oanda".to_string()],
            symbols: vec!["USD-ZAR".to_string()],
            approved_by: "admin".to_string(),
            approved_at: "2026-01-25T12:00:00Z".to_string(),
            performance_summary: vec![],
            risk_metrics: serde_json::to_vec(&serde_json::json!({"position_size_pct": 0.25}))
                .unwrap(),
            admin_approved: true,
            timestamp: 0,
            mode: "paper".to_string(),
        };

        let strategy = DeployedStrategy::from_deployment(
            &deployment,
            DeploymentPythonConfig {
                python_source_code: Some("class Strategy:\n    pass".to_string()),
                candle_interval_minutes: Some(240),
                asset_class: Some("forex".to_string()),
            },
        )
        .unwrap();
        assert_eq!(strategy.capital_allocation, 10000.0);
        assert_eq!(strategy.position_size_pct, Some(0.25));
        assert_eq!(
            strategy.python_source_code,
            Some("class Strategy:\n    pass".to_string())
        );
        assert_eq!(strategy.candle_interval_minutes, Some(240));
        assert_eq!(strategy.asset_class, Some("forex".to_string()));
    }
}
