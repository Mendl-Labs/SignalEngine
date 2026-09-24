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
    /// Pairs-trading declaration (`backtest_jobs.params_json.pair_spec`), a
    /// JSON object shaped `{symbol_a, exchange_a, symbol_b, exchange_b,
    /// hedge_ratio_mode, hedge_ratio}` -- the same shape a pairs strategy's
    /// own Python `pair_spec()` method returns for backtest (see
    /// `BacktestingEngine/strategy/src/strategies/python_strategy.rs`).
    /// Whatever constructs a `validation_mode: "pairs"` job is responsible
    /// for setting this at submission time so live deployment can construct
    /// a `PairPythonBridgeStrategy` without executing Python just to learn
    /// which two legs it trades -- it must describe the same relationship
    /// the strategy's own `pair_spec()` declares, the same way
    /// `edge_mechanism`/`asset_class` are submitter-declared metadata
    /// mirrored by (not derived from) the strategy code.
    pub pair_spec: Option<serde_json::Value>,
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

/// Parse a tenant id read from the database: anything that is not a valid UUID
/// (NULL, empty, garbage) becomes the nil UUID, which no provider serves.
pub fn parse_row_tenant(raw: Option<&str>) -> Uuid {
    raw.map(str::trim)
        .and_then(|t| Uuid::parse_str(t).ok())
        .unwrap_or_else(Uuid::nil)
}

/// Real tenant of every active deployment row, keyed by deployment id.
///
/// Production's `deployed_strategies` has a `tenant_id` column (private
/// schema); the public/OSS schema does not. The column's existence is checked
/// through `information_schema` (once per call), and rows are read with a raw
/// query so this compiles against both. NEVER fails the reconcile: when the
/// column is absent, or any query errors, the map is empty (with a WARN for
/// the error case) and every row is treated as tenant-less (nil) -- which a
/// live deployment cannot pass. Rows whose tenant is NULL or not a UUID are
/// simply left out of the map for the same reason.
#[cfg(feature = "postgres")]
pub async fn load_reconcile_tenants(
    conn: &mut AsyncPgConnection,
) -> std::collections::HashMap<Uuid, Uuid> {
    use diesel::sql_types::{Bool, Nullable, Text, Uuid as SqlUuid};

    #[derive(diesel::QueryableByName)]
    struct Present {
        #[diesel(sql_type = Bool)]
        present: bool,
    }
    #[derive(diesel::QueryableByName)]
    struct TenantRow {
        #[diesel(sql_type = SqlUuid)]
        id: Uuid,
        #[diesel(sql_type = Nullable<Text>)]
        tenant: Option<String>,
    }

    let mut out = std::collections::HashMap::new();

    let present = match diesel::sql_query(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns          WHERE table_schema = ANY (current_schemas(false))          AND table_name = 'deployed_strategies' AND column_name = 'tenant_id') AS present",
    )
    .load::<Present>(conn)
    .await
    {
        Ok(rows) => rows.iter().next().map(|r| r.present).unwrap_or(false),
        Err(e) => {
            ultra_warn!(format!(
                "⚠️ reconcile: could not check for deployed_strategies.tenant_id ({}); \
                 replayed deployments keep the nil tenant (live ones are rejected)",
                e
            ));
            return out;
        }
    };
    if !present {
        return out;
    }

    match diesel::sql_query(
        "SELECT id, tenant_id::text AS tenant FROM deployed_strategies \
         WHERE is_active = true AND status = 'active'",
    )
    .load::<TenantRow>(conn)
    .await
    {
        Ok(rows) => {
            for r in rows {
                let t = parse_row_tenant(r.tenant.as_deref());
                if !t.is_nil() {
                    out.insert(r.id, t);
                }
            }
        }
        Err(e) => ultra_warn!(format!(
            "⚠️ reconcile: could not read deployed_strategies.tenant_id ({}); \
             replayed deployments keep the nil tenant (live ones are rejected)",
            e
        )),
    }
    out
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
    /// Pairs-trading declaration, see `DeploymentPythonConfig::pair_spec`.
    pub pair_spec: Option<serde_json::Value>,
    /// Exchanges for which `handle_deployment` published a `MarketDataSubscribe`
    /// (subscription id `{instance_id}_{exchange}`). `reject_live_deployment`
    /// drains this to unsubscribe exactly what was subscribed, once.
    pub subscribed_exchanges: parking_lot::Mutex<Vec<String>>,
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
            pair_spec: python_config.pair_spec,
            subscribed_exchanges: parking_lot::Mutex::new(Vec::new()),
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
            let pair_spec = params_json.as_ref().and_then(|params| params.get("pair_spec")).cloned();

            Some(DeploymentPythonConfig { python_source_code, candle_interval_minutes, asset_class, pair_spec })
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

    /// A cloneable failure-ack sender over the same publisher this subscriber
    /// uses. `None` until [`Self::start`] has connected the publisher.
    pub fn ack_sender(&self) -> Option<crate::live_rejection::AckSender> {
        self.publisher
            .as_ref()
            .map(|p| crate::live_rejection::AckSender::from_publisher(p.clone(), &self.node_id))
    }

    /// The shared-database URL from the environment (with the same
    /// `$(POSTGRES_*)` placeholder resolution the subscriber's own queries
    /// use). `None` when unset, empty or unresolvable.
    pub fn database_url_from_env() -> Option<String> {
        Self::resolved_database_url().ok().filter(|u| !u.is_empty())
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
        self.reconcile_from_database_url(&database_url).await
    }

    /// [`Self::reconcile_active_deployments_from_db`] against an explicit URL.
    ///
    /// Each replayed deployment carries its REAL tenant, read from the shared
    /// database's `deployed_strategies.tenant_id` when that column exists (see
    /// [`load_reconcile_tenants`]). A row with no tenant (public/OSS schema, or
    /// a NULL) keeps the nil tenant, so a live one is rejected (fail closed).
    /// A row is NEVER attributed to the process's configured tenant: the shared
    /// database holds many tenants and that would run another tenant's
    /// deployment with this tenant's credentials.
    #[cfg(feature = "postgres")]
    pub async fn reconcile_from_database_url(
        &self,
        database_url: &str,
    ) -> Result<usize, DeploymentSubscriberError> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
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

        // One column-existence check + one query per reconcile run (never per row).
        let tenants = load_reconcile_tenants(&mut conn).await;

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
                // The row's real tenant when the shared DB has one; nil otherwise
                // (the OSS schema has no tenant column). Nil is never served.
                tenant_id: tenants
                    .get(&deployment.id)
                    .copied()
                    .unwrap_or_else(Uuid::nil)
                    .to_string(),
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
            let pair_spec = params_json.get("pair_spec").cloned();

            Self::handle_deployment(
                deployment_msg,
                DeploymentPythonConfig {
                    python_source_code: backtest.python_source_code.clone(),
                    candle_interval_minutes,
                    asset_class,
                    pair_spec,
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
        let (success, error_message, active_exchanges, symbols, instance_id_str, tenant_id_str, deployed_entry) =
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
                let entry = strategy.clone();

                // Notify strategy manager
                if let Some(tx) = deployment_tx {
                    let _ = tx.send(DeploymentEvent::Deploy(strategy)).await;
                }

                (true, String::new(), exchanges, symbols, instance_id_str, tenant_id_str, Some(entry))
            }
            Err(e) => (false, e.to_string(), Vec::new(), Vec::new(), String::new(), String::new(), None),
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
                    let published = pub_arc.publish_raw(encoded, topics::MARKET_DATA_SUBSCRIBE).await;
                    if published.is_ok() {
                        // Remember it so a later rejection can unsubscribe exactly this.
                        if let Some(entry) = &deployed_entry {
                            entry.subscribed_exchanges.lock().push(exchange.clone());
                        }
                    }
                    if let Err(_e) = published {
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

    // Excluded from compilation (not just #[ignore], which wouldn't help --
    // the body itself fails to type-check): a pre-existing compile error in
    // the private codebase this was forked from (AtomicBool::load
    // type-inference failure at strategy.is_active.load(...) below),
    // confirmed present in the untouched original SignalEngine repo too.
    // Unrelated to the OSS/tenant_id port; not fixed here since it's out of
    // scope for this fork.
    #[cfg(any())]
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
                pair_spec: None,
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

    // ---- end to end: optimistic success, then the host's rejection undoes it --

    use crate::live_rejection::tests::{deployment_msg, RecordingSink};
    use crate::live_rejection::{reason_no_provider, reject_live_deployment, AckSender};

    /// handle_deployment lists the deployment and forwards Deploy BEFORE the
    /// host's credential check; the rejection must then undo the listing.
    #[tokio::test]
    async fn live_deploy_then_host_rejection_leaves_nothing_deployed() {
        let map: Arc<DashMap<Uuid, Arc<DeployedStrategy>>> = Arc::new(DashMap::new());
        let (tx, mut rx) = mpsc::channel::<DeploymentEvent>(8);
        let id = Uuid::new_v4();

        DeploymentSubscriber::handle_deployment(
            deployment_msg(id, "live"),
            DeploymentPythonConfig::default(),
            &map,
            None,
            "node",
            Some(&tx),
        )
        .await;

        // The optimistic state the bug lived in: listed as deployed already.
        assert!(map.contains_key(&id), "handle_deployment lists it before any credential check");
        let DeploymentEvent::Deploy(s) = rx.recv().await.expect("Deploy forwarded") else {
            panic!("expected Deploy")
        };
        assert_eq!(s.mode, "live");

        // The host finds no provider and rejects.
        let sink = Arc::new(RecordingSink::default());
        let sender = AckSender::with_sink(sink.clone(), "node");
        let out = reject_live_deployment(
            &map,
            Some(&sender),
            None,
            &s.strategy_id.to_string(),
            s.instance_id,
            &reason_no_provider(),
        )
        .await;

        assert!(out.removed_from_map);
        assert!(map.is_empty(), "SignalEngine must no longer list the strategy");
        let acks = sink.acks.lock().unwrap();
        assert_eq!(acks.len(), 1);
        assert!(!acks[0].success);
        assert_eq!(acks[0].instance_id, id.to_string());
    }

    /// A paper deployment's event is untouched by the (live-only) rejection path.
    #[tokio::test]
    async fn paper_deploy_stays_listed_when_nothing_rejects_it() {
        let map: Arc<DashMap<Uuid, Arc<DeployedStrategy>>> = Arc::new(DashMap::new());
        let (tx, mut rx) = mpsc::channel::<DeploymentEvent>(8);
        let id = Uuid::new_v4();
        DeploymentSubscriber::handle_deployment(
            deployment_msg(id, "paper"),
            DeploymentPythonConfig::default(),
            &map,
            None,
            "node",
            Some(&tx),
        )
        .await;
        let DeploymentEvent::Deploy(s) = rx.recv().await.unwrap() else { panic!() };
        assert_eq!(s.mode, "paper");
        assert!(map.contains_key(&id));
    }

    /// The reconcile path: an active LIVE row in the shared DB is replayed as a
    /// Deploy event (tenant nil: the OSS schema has no tenant column), the host
    /// rejects it, and the row ends up stopped so the NEXT restart does not
    /// replay (and re-reject) it again. A paper row is replayed and left alone.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn reconcile_replays_live_row_and_rejection_stops_it_for_good() {
        use crate::live_rejection::tests::db::{insert_deployment, row_json, url};
        let Some(db_url) = url() else { return };
        let live_id = insert_deployment(&db_url, "live", None).await;
        let paper_id = insert_deployment(&db_url, "paper", None).await;

        let mut sub = DeploymentSubscriber::new("127.0.0.1:1", "node");
        let (tx, mut rx) = mpsc::channel::<DeploymentEvent>(10_000);
        sub.set_deployment_channel(tx);
        sub.reconcile_from_database_url(&db_url).await.unwrap();

        let mut live_seen = None;
        let mut paper_seen = false;
        while let Ok(ev) = rx.try_recv() {
            if let DeploymentEvent::Deploy(s) = ev {
                if s.instance_id == live_id {
                    live_seen = Some(s);
                } else if s.instance_id == paper_id {
                    paper_seen = true;
                    assert_eq!(s.mode, "paper");
                }
            }
        }
        let s = live_seen.expect("live row replayed as Deploy");
        assert_eq!(s.mode, "live");
        assert!(paper_seen, "paper row replayed");
        assert!(sub.get_deployed_strategies().contains_key(&live_id));

        // Host rejects the live one (it applies the same rule to reconciled events).
        let sink = Arc::new(RecordingSink::default());
        let sender = AckSender::with_sink(sink.clone(), "node");
        let map = sub.get_deployed_strategies();
        reject_live_deployment(
            &map,
            Some(&sender),
            Some(&db_url),
            &s.strategy_id.to_string(),
            s.instance_id,
            &reason_no_provider(),
        )
        .await;
        assert!(!map.contains_key(&live_id));
        assert!(map.contains_key(&paper_id), "paper deployment stays listed");
        assert_eq!(row_json(&db_url, live_id).await["status"], "stopped");
        assert_eq!(row_json(&db_url, paper_id).await["status"], "active");

        // Next restart: the stopped live row is no longer replayed.
        let mut sub2 = DeploymentSubscriber::new("127.0.0.1:1", "node");
        let (tx2, mut rx2) = mpsc::channel::<DeploymentEvent>(10_000);
        sub2.set_deployment_channel(tx2);
        sub2.reconcile_from_database_url(&db_url).await.unwrap();
        while let Ok(ev) = rx2.try_recv() {
            if let DeploymentEvent::Deploy(s2) = ev {
                assert_ne!(s2.instance_id, live_id, "a rejected live row must not be replayed again");
            }
        }
    }

    // ---- FIX 1: tenant on the reconcile path --------------------------------

    #[test]
    fn parse_row_tenant_fails_closed_to_nil() {
        let t = Uuid::new_v4();
        assert_eq!(parse_row_tenant(Some(&t.to_string())), t);
        assert_eq!(parse_row_tenant(Some(&format!(" {} ", t))), t);
        for bad in [None, Some(""), Some("garbage"), Some("1234")] {
            assert!(parse_row_tenant(bad).is_nil(), "{:?}", bad);
        }
    }

    /// handle_deployment with no publisher subscribes nothing, so a rejection
    /// that follows must publish no unsubscribe (nothing was subscribed).
    #[tokio::test]
    async fn deployment_without_publisher_subscribed_nothing_so_rejection_unsubscribes_nothing() {
        let map: Arc<DashMap<Uuid, Arc<DeployedStrategy>>> = Arc::new(DashMap::new());
        let (tx, mut rx) = mpsc::channel::<DeploymentEvent>(4);
        let id = Uuid::new_v4();
        DeploymentSubscriber::handle_deployment(
            deployment_msg(id, "live"),
            DeploymentPythonConfig::default(),
            &map,
            None,
            "node",
            Some(&tx),
        )
        .await;
        let DeploymentEvent::Deploy(s) = rx.recv().await.unwrap() else { panic!() };
        assert!(s.subscribed_exchanges.lock().is_empty());
        let sink = Arc::new(RecordingSink::default());
        let sender = AckSender::with_sink(sink.clone(), "node");
        reject_live_deployment(&map, Some(&sender), None, "s", id, "nope").await;
        assert!(sink.unsubs.lock().unwrap().is_empty());
        assert_eq!(sink.acks.lock().unwrap().len(), 1);
    }

    #[cfg(feature = "postgres")]
    async fn replay(db_url: &str) -> std::collections::HashMap<Uuid, Arc<DeployedStrategy>> {
        let mut sub = DeploymentSubscriber::new("127.0.0.1:1", "node");
        let (tx, mut rx) = mpsc::channel::<DeploymentEvent>(10_000);
        sub.set_deployment_channel(tx);
        sub.reconcile_from_database_url(db_url).await.unwrap();
        let mut out = std::collections::HashMap::new();
        while let Ok(DeploymentEvent::Deploy(s)) = rx.try_recv() {
            out.insert(s.instance_id, s);
        }
        out
    }

    /// Public/OSS schema: no tenant column. Every replayed row keeps the nil
    /// tenant (a live one cannot be served), and the reconcile does not fail.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn reconcile_without_tenant_column_keeps_nil_tenant() {
        use crate::live_rejection::tests::db::{insert_deployment, url};
        use diesel_async::AsyncConnection;
        let Some(db_url) = url() else { return };
        let live = insert_deployment(&db_url, "live", None).await;
        let paper = insert_deployment(&db_url, "paper", None).await;

        let mut conn = AsyncPgConnection::establish(&db_url).await.unwrap();
        assert!(load_reconcile_tenants(&mut conn).await.is_empty(), "no tenant column -> empty map");

        let ev = replay(&db_url).await;
        assert_eq!(ev[&live].mode, "live");
        assert!(ev[&live].tenant_id.is_nil(), "live row without a tenant column stays nil (fail closed)");
        assert_eq!(ev[&paper].mode, "paper");
        assert!(ev[&paper].tenant_id.is_nil());
    }

    /// Private-schema shape: each row's REAL tenant is used; NULL stays nil; the
    /// configured tenant is never substituted for another tenant's row, and the
    /// single-tenant provider serves only the configured tenant's row.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn reconcile_with_tenant_column_uses_each_rows_real_tenant() {
        use crate::live_rejection::tests::db::{insert_deployment_tenant, tenant_url};
        use smartorderrouter::{CredentialError, CredentialProvider, SingleTenantDbProvider};
        let Some(db_url) = tenant_url() else { return };
        let tenant_a = Uuid::new_v4(); // the configured (single) tenant
        let tenant_b = Uuid::new_v4(); // another tenant sharing the database
        let live_a = insert_deployment_tenant(&db_url, "live", None, Some(Some(tenant_a))).await;
        let live_b = insert_deployment_tenant(&db_url, "live", None, Some(Some(tenant_b))).await;
        let live_null = insert_deployment_tenant(&db_url, "live", None, Some(None)).await;
        let paper_b = insert_deployment_tenant(&db_url, "paper", None, Some(Some(tenant_b))).await;

        let ev = replay(&db_url).await;
        assert_eq!(ev[&live_a].tenant_id, tenant_a);
        assert_eq!(ev[&live_b].tenant_id, tenant_b, "another tenant's row keeps ITS tenant");
        assert!(ev[&live_null].tenant_id.is_nil(), "NULL tenant stays nil");
        assert_eq!(ev[&paper_b].tenant_id, tenant_b);
        assert_eq!(ev[&paper_b].mode, "paper", "paper reconcile is unchanged");
        assert_eq!(ev[&live_a].mode, "live");

        // Provider bound to tenant A (unreachable DB: reaching it = passed the tenant check).
        let pool = smartorderrouter::create_pool("postgres://nobody:nothing@127.0.0.1:1/none").await.unwrap();
        let provider = SingleTenantDbProvider::self_hosted_single_tenant_only(Arc::new(pool), tenant_a);

        let served = provider.credentials_for(ev[&live_a].tenant_id, "kraken", true).await.unwrap_err();
        assert!(matches!(served, CredentialError::Backend(_)), "A's row passes the tenant check: {:?}", served);
        for (name, id) in [("other tenant", live_b), ("NULL tenant", live_null)] {
            let err = provider.credentials_for(ev[&id].tenant_id, "kraken", true).await.unwrap_err();
            assert!(
                matches!(err, CredentialError::TenantNotServed { served, .. } if served == tenant_a),
                "{} must be refused: {:?}",
                name,
                err
            );
        }

        // Rejecting them ends the same way as before: stopped rows, gone from the set.
        let sink = Arc::new(RecordingSink::default());
        let sender = AckSender::with_sink(sink.clone(), "node");
        let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
        map.insert(live_b, ev[&live_b].clone());
        reject_live_deployment(&map, Some(&sender), Some(&db_url), "s", live_b, &reason_no_provider()).await;
        assert!(map.is_empty());
        assert_eq!(crate::live_rejection::tests::db::row_json(&db_url, live_b).await["status"], "stopped");
        assert_eq!(crate::live_rejection::tests::db::row_json(&db_url, live_a).await["status"], "active");
    }

    /// The multi-tenant provider (private schema) behind the deployment
    /// pipeline: each live deployment's OWN tenant -- from the reconcile row or
    /// from the broker message -- is what the provider is asked about, so A's
    /// deployment is served A's key, B's is served B's, and a row with NO tenant
    /// (NULL -> nil) is refused even though a credential row stored under the
    /// nil tenant exists. Refused deployments are rejected the usual way.
    ///
    /// Needs TWO scratch databases: FOLLOWUP_TENANT_DATABASE_URL (deployed_strategies
    /// with tenant_id) and TENANTCRED_TEST_DATABASE_URL (a scratch schema for the
    /// private-shaped exchange_credentials is created and dropped in it).
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn multi_tenant_provider_serves_each_deployments_own_tenant_and_refuses_nil() {
        use crate::live_rejection::tests::db::{insert_deployment_tenant, row_json, tenant_url};
        use crate::live_rejection::reason_credential_unavailable;
        use diesel::sql_types::{Nullable, Text, Uuid as SqlUuid};
        use diesel_async::{AsyncConnection, RunQueryDsl};
        use smartorderrouter::{
            resolve_credential, CredentialError, CredentialProvider, MultiTenantDbProvider,
        };

        let Some(dep_url) = tenant_url() else { return };
        let cred_base = match std::env::var("TENANTCRED_TEST_DATABASE_URL") {
            Ok(u) if !u.trim().is_empty() => u,
            _ => {
                eprintln!("SKIPPED: TENANTCRED_TEST_DATABASE_URL not set");
                return;
            }
        };
        // A well-formed (fake) key: the provider refuses to exist without one.
        // Every value below uses the legacy `enc:` (base64) format, which needs no key.
        std::env::set_var("CREDENTIALS_ENCRYPTION_KEY", "11".repeat(32));

        // Scratch schema with the private-shaped table.
        let schema = format!("tcsl_{}", Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&cred_base).await.unwrap();
        diesel::sql_query(format!("CREATE SCHEMA {schema}")).execute(&mut admin).await.unwrap();
        diesel::sql_query(format!(
            "CREATE TABLE {schema}.exchange_credentials (\
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID NOT NULL, \
                exchange VARCHAR(50) NOT NULL, label VARCHAR(255) NOT NULL, \
                api_key_encrypted TEXT NOT NULL, api_secret_encrypted TEXT NOT NULL, \
                passphrase_encrypted TEXT, is_testnet BOOLEAN NOT NULL DEFAULT false, \
                is_enabled BOOLEAN NOT NULL DEFAULT true, \
                CONSTRAINT unique_tenant_exchange_label UNIQUE (tenant_id, exchange, label))"
        ))
        .execute(&mut admin)
        .await
        .unwrap();
        let sep = if cred_base.contains('?') { '&' } else { '?' };
        let cred_url = format!("{cred_base}{sep}options=-c%20search_path%3D{schema}");

        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let mut c = AsyncPgConnection::establish(&cred_url).await.unwrap();
        // (tenant, base64 key, base64 secret): "fake-key-A"/"fake-secret-A" etc.
        for (tenant, key, secret) in [
            (tenant_a, "ZmFrZS1rZXktQQ==", "ZmFrZS1zZWNyZXQtQQ=="),
            (tenant_b, "ZmFrZS1rZXktQg==", "ZmFrZS1zZWNyZXQtQg=="),
            (Uuid::nil(), "ZmFrZS1rZXktTklM", "ZmFrZS1zZWNyZXQtTklM"),
        ] {
            diesel::sql_query(
                "INSERT INTO exchange_credentials (tenant_id, exchange, label, api_key_encrypted, \
                 api_secret_encrypted, passphrase_encrypted) VALUES ($1, 'kraken', 'main', $2, $3, $4)",
            )
            .bind::<SqlUuid, _>(tenant)
            .bind::<Text, _>(format!("enc:{key}"))
            .bind::<Text, _>(format!("enc:{secret}"))
            .bind::<Nullable<Text>, _>(None::<String>)
            .execute(&mut c)
            .await
            .unwrap();
        }
        let pool = smartorderrouter::create_pool(&cred_url).await.unwrap();
        let provider = MultiTenantDbProvider::new(Arc::new(pool)).await.expect("private-shaped table");

        // Reconcile path: real rows, each with ITS tenant (one NULL).
        let live_a = insert_deployment_tenant(&dep_url, "live", None, Some(Some(tenant_a))).await;
        let live_b = insert_deployment_tenant(&dep_url, "live", None, Some(Some(tenant_b))).await;
        let live_null = insert_deployment_tenant(&dep_url, "live", None, Some(None)).await;
        let ev = replay(&dep_url).await;

        // What hostbuilder's Deploy handler does: ask for the DEPLOYMENT's own tenant.
        let ca = resolve_credential(&provider, ev[&live_a].tenant_id, "kraken", true).await.unwrap();
        assert_eq!(ca.api_key, "fake-key-A");
        let cb = resolve_credential(&provider, ev[&live_b].tenant_id, "kraken", true).await.unwrap();
        assert_eq!(cb.api_key, "fake-key-B");
        assert!(ev[&live_null].tenant_id.is_nil());
        let err = resolve_credential(&provider, ev[&live_null].tenant_id, "kraken", true)
            .await
            .unwrap_err();
        assert!(matches!(err, CredentialError::NotFound { .. }), "nil tenant must be refused: {err:?}");
        let err = provider.all_credentials_for(ev[&live_null].tenant_id).await.unwrap_err();
        assert!(matches!(err, CredentialError::NotFound { .. }), "{err:?}");

        // Broker-message path: the message's tenant is the deployment's tenant.
        let map: Arc<DashMap<Uuid, Arc<DeployedStrategy>>> = Arc::new(DashMap::new());
        let (tx, mut rx) = mpsc::channel::<DeploymentEvent>(4);
        let id = Uuid::new_v4();
        let mut msg = deployment_msg(id, "live");
        msg.tenant_id = tenant_b.to_string();
        DeploymentSubscriber::handle_deployment(msg, DeploymentPythonConfig::default(), &map, None, "node", Some(&tx)).await;
        let DeploymentEvent::Deploy(s) = rx.recv().await.unwrap() else { panic!() };
        assert_eq!(s.tenant_id, tenant_b);
        assert_eq!(
            resolve_credential(&provider, s.tenant_id, "kraken", true).await.unwrap().api_key,
            "fake-key-B"
        );

        // The refused deployment is rejected the usual way; the served ones stay active.
        let sink = Arc::new(RecordingSink::default());
        let sender = AckSender::with_sink(sink.clone(), "node");
        let dmap: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
        dmap.insert(live_null, ev[&live_null].clone());
        reject_live_deployment(&dmap, Some(&sender), Some(&dep_url), "s", live_null, &reason_credential_unavailable("kraken")).await;
        assert!(dmap.is_empty());
        assert_eq!(row_json(&dep_url, live_null).await["status"], "stopped");
        assert_eq!(row_json(&dep_url, live_a).await["status"], "active");
        assert_eq!(row_json(&dep_url, live_b).await["status"], "active");

        diesel::sql_query(format!("DROP SCHEMA {schema} CASCADE")).execute(&mut admin).await.unwrap();
    }
}
