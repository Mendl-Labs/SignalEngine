pub mod core;
pub mod exchanges;
pub mod optimizations;
pub mod signal;
pub mod circuit_breaker_v2;
pub mod position_tracker;
pub mod monitoring;
pub mod validation;
pub mod auth;
pub mod risk_controls;
pub mod reconciliation;
pub mod prometheus_metrics;
pub mod dead_letter_queue;
pub mod bounded_dlq;
pub mod metrics_server;
pub mod credential_mode;
pub mod backpressure;
pub mod fill_probability;
pub mod latency_optimizer;
pub mod rate_limiter;
pub mod tracing;
pub mod chaos;
pub mod audit;
pub mod credential_manager;
pub mod database_credentials;
pub mod saas_credential_vault;
pub mod permission_validator;
pub mod multi_leg;
pub mod tca;
pub mod orderbook_reconciliation;
pub mod fat_finger;
pub mod alerts;
pub mod graceful_shutdown;
pub mod order_wal;
pub mod hot_config;
pub mod multi_tenant;
#[cfg(feature = "postgres")]
pub mod portfolio_snapshotter;
pub mod paper_connector;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

use signalengine::SignalEngineLogger;
use std::sync::Arc;

// Re-export main types and traits for easy access
pub use core::{
    ExchangeConnector, ExchangeAuth, ExchangeWebSocket, NanoOptimized,
    ExecutionResult, ExecutionStatus, ExecutionFill, ExecutionError,
    ExecutionMetrics, ExchangeConfig, OrderUpdate, HealthStatus,
    ExchangeLimits, CancelResult, OrderStatus, PooledOrder,
};

pub use exchanges::ExchangeFactory;
pub use signal::{Signal, SignalAction};
pub use paper_connector::{PaperTradingConnector, PaperTradingConfig};
pub use position_tracker::{PositionTracker, Position, PositionSide, PortfolioPnL};
pub use validation::{TradingValidator, VALIDATOR};
pub use auth::{
    TradingAuthenticator, AuthMiddleware, ServiceClaims, AuthError, AUTHENTICATOR,
    AuthRateLimitConfig, FailedAttemptTracker,
};
pub use monitoring::{PerformanceMonitor, PerformanceMetrics, Alert, AlertType, TradingLogger};
pub use optimizations::{
    timestamp::{nano_timestamp, NanoTimer},
    memory_pool::{get_thread_local_order, return_thread_local_order, preallocate_thread_local},
    cpu_affinity::{set_cpu_affinity, get_optimal_trading_core, CoreAssignment, set_high_priority},
    lock_free::{LockFreeRingBuffer, AtomicMetrics, SPSCQueue},
    simd_metrics::{simd_calculate_percentiles, VectorizedMetrics, LatencyHistogram},
};

// Risk management and production safety exports
pub use risk_controls::{
    KILL_SWITCH, KillSwitch, KillReason,
    PositionLimits, PositionLimitChecker,
    CircuitBreaker as RiskCircuitBreaker, CircuitBreakerConfig,
    RiskManager, RiskCheckError,
    PreTradeGateMode, PRE_TRADE_GATE_ENFORCE_ENV,
};
pub use reconciliation::{
    ReconciliationEngine, ExchangeReconciliation, ExchangePosition, 
    ExchangeOpenOrder, ReconciliationResult, PositionMismatch,
    startup_reconciliation,
};
pub use prometheus_metrics::{
    METRICS, MetricsRegistry, get_prometheus_metrics, get_metrics_json,
    inc_orders_submitted, inc_orders_filled, inc_orders_rejected,
    inc_orders_failed, record_order_latency_us, record_fill_latency_us,
    set_kill_switch_active, set_circuit_breaker_active, set_total_pnl_usd,
};
pub use dead_letter_queue::{
    DeadLetterQueue, DeadLetterEntry, DeadLetterStatus, FailureCategory,
    DlqConfig, DlqStats, DlqRetryWorker,
};
pub use metrics_server::{
    start_metrics_server, MetricsServerConfig, MetricsServerHandle,
};
pub use credential_mode::{
    CredentialMode, MultiTenantPrerequisites, ResolvedCredentialMode, compute_credential_mode,
    resolve_credential_mode, resolve_credential_mode_with,
    publish_credential_mode, published_credential_mode,
};
pub use backpressure::{
    BackpressureController, BackpressureConfig, BackpressureError,
    BackpressurePermit, BackpressureState, BackpressureStats,
    OverflowPolicy, BACKPRESSURE, acquire_permit, acquire_permit_for_exchange,
};
pub use fill_probability::{
    FillProbabilityModel, FillProbabilityConfig, FillProbabilityEstimate,
    OrderbookSnapshot, FillFactors, MarketRegime, FILL_MODEL, estimate_fill,
};
pub use latency_optimizer::{
    LatencyOptimizer, LatencyOptimizerConfig, ExchangeLatencyStats,
    ExchangeHealth, SettlementConfig, RoutingDecision, RoutingReason,
    ArbTimingResult, LATENCY_OPTIMIZER, record_exchange_latency, get_optimal_route,
};

// Lock-free circuit breaker (v2 - for hot paths)
pub use circuit_breaker_v2::{
    AtomicCircuitBreaker, CircuitBreakerConfig as AtomicCircuitBreakerConfig,
    CircuitState as AtomicCircuitBreakerState,
    ExchangeCircuitBreakerManager as AtomicExchangeCircuitBreakerManager,
    CIRCUIT_BREAKERS as ATOMIC_CIRCUIT_BREAKERS,
};

// Token bucket rate limiter
pub use rate_limiter::{
    AtomicTokenBucket, RateLimiterConfig, SlidingWindowLimiter,
    ExchangeRateLimiterManager, RATE_LIMITERS,
};

// Bounded DLQ with overflow policies
pub use bounded_dlq::{
    BoundedDeadLetterQueue, BoundedDlqConfig, BoundedDlqStats,
    DlqOverflowPolicy, EnqueueResult, BOUNDED_DLQ,
};

// Distributed tracing
pub use tracing::{
    TraceId, SpanId, TraceContext, Span, SpanKind, SpanStatus,
    AttributeValue, SpanEvent, CompletedSpan, SpanExporter,
    InMemoryExporter, LogExporter, Tracer, ExecutionTraceContext,
    TRACER, init_tracer, tracer,
};

// Chaos engineering / fault injection
pub use chaos::{
    ChaosMonkey, ChaosConfig, ChaosResult, ChaosStats,
    NetworkPartition, FaultInjector, CHAOS_MONKEY,
};

// Audit logging
pub use audit::{
    AuditLogger, AuditEntry, AuditEventType, AuditSeverity,
    AuditBackend, AuditQuery, FileAuditBackend, InMemoryAuditBackend,
};

// Secrets management
pub use credential_manager::{
    SecretsManager, SecretsConfig, SecretBackend, SecretString,
    ApiCredentials, SecurityEventType, SecurityAuditEntry, SECRETS,
};

// Database-backed credentials (from exchange_credentials table)
pub use database_credentials::{
    DatabaseCredentialProvider, DecryptedCredential, decrypt_credential_value,
};

// Multi-leg order management (OCO, bracket, linked pairs)
pub use multi_leg::{
    MultiLegOrderManager, MultiLegType, OrderLeg,
    LegRole, LegStatus, GroupState, MultiLegEvent, MultiLegStats,
    BracketParams, OcoParams, LinkedPairParams, MultiLegError,
};

// Transaction Cost Analysis (TCA)
pub use tca::{
    TcaEngine, TcaConfig, TcaAnalysis, TcaError, TcaStatistics,
    ExecutionRecord, ExecutionUrgency, BenchmarkType, ExecutionGrade,
    FillAnalysis, MifidReport, Percentiles,
};

// Orderbook snapshot reconciliation
pub use orderbook_reconciliation::{
    OrderbookReconciler, ReconciliationConfig, OrderbookUpdate, UpdateType,
    ReconciliationResult as OrderbookReconciliationResult, 
    ReconciliationEvent, SnapshotReason,
    SymbolStatistics, GlobalStatistics,
};

// Multi-tenant SaaS execution (aligned with BacktestingEngine tiers)
pub use multi_tenant::{
    MultiTenantExecutionHandler, SubscriptionTier, TenantContext,
    FairScheduler, TenantStats, GlobalStats, PendingOrder,
    TenantRateLimiter, MultiTenantExecutionResult,
};

// Periodic portfolio PnL snapshotter (off-hot-path persistence)
#[cfg(feature = "postgres")]
pub use portfolio_snapshotter::{PortfolioSnapshotter, PortfolioSnapshotterConfig};

use std::collections::HashMap;
use tokio::sync::RwLock;
use crate::monitoring::MonitoringThresholds;
use chrono::{DateTime, Utc};

// Database integration for execution tracking
pub trait DatabaseExecutionPersistence: Send + Sync {
    fn save_execution(&self, execution_data: &ExecutionData) -> Result<(), String>;
    fn update_execution_status(&self, order_id: &str, status: &str) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct ExecutionData {
    pub order_id: String,
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub price: f64,
    pub fee: f64,
    pub status: String,
    pub executed_at: DateTime<Utc>,
    pub latency_ns: u64,
}

// Simple in-memory implementation for testing
pub struct InMemoryExecutionDatabase {
    executions: std::sync::Mutex<Vec<ExecutionData>>,
}

impl Default for InMemoryExecutionDatabase {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryExecutionDatabase {
    pub fn new() -> Self {
        Self {
            executions: std::sync::Mutex::new(Vec::new()),
        }
    }
    
    pub fn get_all_executions(&self) -> Vec<ExecutionData> {
        self.executions.lock().unwrap().clone()
    }
}

impl DatabaseExecutionPersistence for InMemoryExecutionDatabase {
    fn save_execution(&self, execution_data: &ExecutionData) -> Result<(), String> {
        self.executions.lock().unwrap().push(execution_data.clone());
        
        // Note: This is synchronous logging - in production use log_trading_execution macro for async
        tokio::spawn({
            let data = execution_data.clone();
            async move {
                let logger = signalengine::SignalEngineLogger::new("ExecutionHandler").await;
                logger.log_execution(
                    &data.order_id,
                    &data.symbol,
                    &data.exchange,
                    data.filled_quantity,
                    data.price,
                    data.fee,
                    data.latency_ns
                ).await;
            }
        });
        
        Ok(())
    }
    
    fn update_execution_status(&self, order_id: &str, status: &str) -> Result<(), String> {
        tokio::spawn({
            let order_id = order_id.to_string();
            let status = status.to_string();
            async move {
                let logger = signalengine::SignalEngineLogger::new("ExecutionHandler").await;
                let context = signalengine::TradingContext::new("ExecutionHandler")
                    .with_operation("status_update")
                    .with_order_id(&order_id);
                logger.info_ctx(&format!("Updated execution status: {}", status), context).await;
            }
        });
        Ok(())
    }
}

/// Ultra-low latency multi-exchange execution handler
#[derive(Clone)]
pub struct UltraLowLatencyExecutionHandler {
    connectors: Arc<RwLock<HashMap<String, Box<dyn ExchangeConnector>>>>,
    /// exchange name -> tenant whose credentials the connector was built from
    /// (only for connectors registered via the credential path). Guards against
    /// one tenant replacing or using another tenant's connector.
    connector_owners: Arc<RwLock<HashMap<String, smartorderrouter::TenantId>>>,
    default_exchange: Option<String>,
    global_metrics: Arc<core::MetricsCollector>,
    core_assignment: optimizations::CoreAssignment,
    position_tracker: Arc<PositionTracker>,
    performance_monitor: Arc<PerformanceMonitor>,
    execution_database: Option<Arc<dyn DatabaseExecutionPersistence>>,
    /// Bounded channel sender for off-hot-path execution logging.
    /// Sends are non-blocking (try_send) so the trading loop is never stalled.
    execution_log_sender: Option<tokio::sync::mpsc::Sender<ExecutionData>>,
    /// Tracks how many executions were dropped due to a full channel.
    execution_log_overflow: Arc<std::sync::atomic::AtomicU64>,
    _logger: Arc<SignalEngineLogger>,
    /// Platform-wide pre-trade risk gate (Phase 1: dry-run wiring).
    /// Shared, process-level instance by default -- see `with_risk_manager`
    /// for overriding it (tests, or future mandate-specific injection).
    risk_manager: Arc<risk_controls::RiskManager>,
    /// Whether `risk_manager`'s `pre_trade_check` rejection actually blocks
    /// an order (`Enforce`) or only logs/counts it (`DryRun`, the default).
    /// Read once here at construction, not per-order.
    pre_trade_gate_mode: risk_controls::PreTradeGateMode,
}

/// Configuration for the background execution writer.
pub struct ExecutionWriterConfig {
    /// Max entries buffered before back-pressure drop.  Default: 10 000.
    pub channel_capacity: usize,
    /// Number of records accumulated before a batch flush.  Default: 50.
    pub batch_size: usize,
    /// Max time between flushes (even when batch is not full).  Default: 1 s.
    pub flush_interval: std::time::Duration,
}

impl Default for ExecutionWriterConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 10_000,
            batch_size: 50,
            flush_interval: std::time::Duration::from_secs(1),
        }
    }
}

impl UltraLowLatencyExecutionHandler {
    /// Create a new multi-exchange execution handler
    pub async fn new() -> Self {
        let core_assignment = optimizations::CoreAssignment::optimal_assignment();
        let logger = Arc::new(SignalEngineLogger::new("ExecutionHandler").await);
        
        Self {
            connectors: Arc::new(RwLock::new(HashMap::new())),
            connector_owners: Arc::new(RwLock::new(HashMap::new())),
            default_exchange: None,
            global_metrics: Arc::new(core::MetricsCollector::new()),
            core_assignment,
            position_tracker: Arc::new(PositionTracker::new()),
            performance_monitor: Arc::new(PerformanceMonitor::new(MonitoringThresholds::default())),
            execution_database: None,
            execution_log_sender: None,
            execution_log_overflow: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            _logger: logger,
            risk_manager: Arc::new(risk_controls::RiskManager::with_defaults()),
            pre_trade_gate_mode: risk_controls::PreTradeGateMode::from_env(),
        }
    }

    /// Override the default platform-wide `RiskManager` (e.g. for tests
    /// that need a tighter/looser `PositionLimits`, or future
    /// mandate-specific injection) without touching `new()`'s call sites.
    /// Consumes and returns `Self` so it chains onto any constructor.
    pub fn with_risk_manager(mut self, risk_manager: Arc<risk_controls::RiskManager>) -> Self {
        self.risk_manager = risk_manager;
        self
    }

    /// Create a new execution handler with database integration.
    ///
    /// Spawns a background writer task that batches execution records off the
    /// hot path.  The writer flushes on interval, batch-size threshold, **and**
    /// on graceful shutdown (when the sender half is dropped).
    pub async fn new_with_database(database: Arc<dyn DatabaseExecutionPersistence>) -> Self {
        Self::new_with_database_config(database, ExecutionWriterConfig::default()).await
    }

    /// Create a new execution handler with database integration and custom writer config.
    pub async fn new_with_database_config(
        database: Arc<dyn DatabaseExecutionPersistence>,
        config: ExecutionWriterConfig,
    ) -> Self {
        let mut handler = Self::new().await;
        let overflow_counter = Arc::clone(&handler.execution_log_overflow);
        let (tx, rx) = tokio::sync::mpsc::channel::<ExecutionData>(config.channel_capacity);
        handler.execution_log_sender = Some(tx);
        handler.execution_database = Some(Arc::clone(&database));
        Self::spawn_background_writer(database, rx, config, overflow_counter);
        handler
    }

    /// Returns the number of execution records dropped because the channel was full.
    pub fn execution_log_overflow_count(&self) -> u64 {
        self.execution_log_overflow.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Spawn a background task that drains the execution channel in batches
    /// and persists to the database.  Flushes remaining records when the
    /// channel closes (graceful shutdown).
    fn spawn_background_writer(
        database: Arc<dyn DatabaseExecutionPersistence>,
        mut rx: tokio::sync::mpsc::Receiver<ExecutionData>,
        config: ExecutionWriterConfig,
        overflow_counter: Arc<std::sync::atomic::AtomicU64>,
    ) {
        tokio::spawn(async move {
            let mut batch: Vec<ExecutionData> = Vec::with_capacity(config.batch_size);
            let mut flush_interval = tokio::time::interval(config.flush_interval);
            flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    maybe_item = rx.recv() => {
                        match maybe_item {
                            Some(data) => {
                                batch.push(data);
                                if batch.len() >= config.batch_size {
                                    Self::flush_batch(&database, &mut batch);
                                }
                            }
                            None => {
                                // Channel closed — final flush before exit
                                if !batch.is_empty() {
                                    Self::flush_batch(&database, &mut batch);
                                }
                                let overflow = overflow_counter.load(std::sync::atomic::Ordering::Relaxed);
                                if overflow > 0 {
                                    log::warn!(
                                        "[EXECUTION-WRITER] Shutting down. Total overflows (dropped records): {}",
                                        overflow
                                    );
                                }
                                log::info!("[EXECUTION-WRITER] Background writer shut down cleanly.");
                                return;
                            }
                        }
                    }
                    _ = flush_interval.tick() => {
                        if !batch.is_empty() {
                            Self::flush_batch(&database, &mut batch);
                        }
                        // Periodically report overflow metrics
                        let overflow = overflow_counter.load(std::sync::atomic::Ordering::Relaxed);
                        if overflow > 0 {
                            log::warn!(
                                "[EXECUTION-WRITER] Execution log overflow count: {}",
                                overflow
                            );
                        }
                    }
                }
            }
        });
    }

    /// Persist a batch of execution records to the database.
    fn flush_batch(database: &Arc<dyn DatabaseExecutionPersistence>, batch: &mut Vec<ExecutionData>) {
        for data in batch.drain(..) {
            if let Err(e) = database.save_execution(&data) {
                log::error!(
                    "[EXECUTION-WRITER] Failed to persist execution {}: {}",
                    data.order_id, e
                );
            }
        }
    }

    /// Enqueue execution details for background persistence (non-blocking).
    fn save_execution_to_database(&self, signal: &Signal, execution_result: &ExecutionResult, exchange_name: &str, latency_ns: u64) {
        let sender = match self.execution_log_sender {
            Some(ref s) => s,
            None => return, // No background writer configured
        };

        let build_data = |fill_qty: f64, fill_price: f64, fill_fee: f64, status: &str| -> ExecutionData {
            ExecutionData {
                order_id: execution_result.order_id.clone(),
                exchange: exchange_name.to_string(),
                symbol: signal.symbol.clone(),
                side: match signal.action {
                    SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop => "Buy".to_string(),
                    SignalAction::Sell | SignalAction::SellLimit | SignalAction::SellStop => "Sell".to_string(),
                },
                quantity: signal.quantity,
                filled_quantity: fill_qty,
                price: fill_price,
                fee: fill_fee,
                status: status.to_string(),
                executed_at: Utc::now(),
                latency_ns,
            }
        };

        let status = format!("{:?}", execution_result.status);

        // Send each fill as a separate execution record (non-blocking try_send)
        for fill in &execution_result.fills {
            let data = build_data(fill.quantity, fill.price, fill.fee, &status);
            if sender.try_send(data).is_err() {
                self.execution_log_overflow.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // If no fills, save the order placement record
        if execution_result.fills.is_empty() {
            let data = build_data(0.0, signal.price.unwrap_or(0.0), 0.0, &status);
            if sender.try_send(data).is_err() {
                self.execution_log_overflow.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Add an exchange connector
    pub async fn add_exchange(&mut self, exchange_name: String, config: ExchangeConfig) -> Result<(), ExecutionError> {
        let connector = ExchangeFactory::create_connector(&exchange_name, config).await?;
        
        let mut connectors = self.connectors.write().await;
        connectors.insert(exchange_name.clone(), connector);
        // Config-built connector: process-level credentials, not tenant-owned.
        self.connector_owners.write().await.remove(&exchange_name);

        // Set first exchange as default if none set
        if self.default_exchange.is_none() {
            self.default_exchange = Some(exchange_name);
        }
        
        Ok(())
    }

    /// Add a paper trading connector with explicit simulation configuration.
    ///
    /// Use this instead of `add_exchange` when deploying a strategy in paper mode
    /// so user-specified slippage, fees, and fill model are honoured.
    pub async fn add_paper_exchange(&mut self, exchange_name: String, paper_config: crate::PaperTradingConfig) -> Result<(), ExecutionError> {
        let connector = Box::new(crate::PaperTradingConnector::new(paper_config));
        let mut connectors = self.connectors.write().await;
        connectors.insert(exchange_name.clone(), connector as Box<dyn crate::core::ExchangeConnector>);
        self.connector_owners.write().await.remove(&exchange_name);
        if self.default_exchange.is_none() {
            self.default_exchange = Some(exchange_name);
        }
        Ok(())
    }

    /// Feed a live order book snapshot into a named connector.
    ///
    /// Used by the hostbuilder's book-sync task to push L3 snapshots from the
    /// DataHandler's `ORDERBOOKS` global into paper trading connectors so that
    /// simulated fills use realistic book-walking rather than a flat slippage model.
    pub async fn update_connector_book(
        &self,
        exchange_name: &str,
        symbol: &str,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    ) {
        let connectors = self.connectors.read().await;
        if let Some(connector) = connectors.get(exchange_name) {
            connector.update_book(symbol, bids, asks);
        }
    }

    /// The tenant that owns the connector registered for `exchange_name` via
    /// the credential path, if any. `None` for connectors registered from
    /// static config (`add_exchange`, `add_paper_exchange`) or when no
    /// connector exists.
    pub async fn connector_owner(&self, exchange_name: &str) -> Option<smartorderrouter::TenantId> {
        self.connector_owners.read().await.get(exchange_name).copied()
    }

    /// Add an exchange connector from a credential resolved for `tenant_id`.
    ///
    /// The connector map of this handler is keyed by exchange name only, so a
    /// connector holds exactly one tenant's keys. To stop tenant B from
    /// silently replacing (and so hijacking the order flow of) tenant A's
    /// connector, this FAILS CLOSED if a different tenant already owns the
    /// connector for that exchange. Use one handler per tenant, or
    /// `MultiTenantExecutionHandler`, for multi-tenant processes.
    ///
    /// The caller passes the credential in; this method never loads one.
    pub async fn add_exchange_from_credential(
        &mut self,
        tenant_id: smartorderrouter::TenantId,
        credential: &smartorderrouter::ExchangeCredential
    ) -> Result<(), ExecutionError> {
        self.add_exchange_from_credential_with(
            tenant_id,
            credential,
            &multi_tenant::DefaultConnectorBuilder,
        ).await
    }

    /// [`Self::add_exchange_from_credential`] with an explicit connector builder.
    pub async fn add_exchange_from_credential_with(
        &mut self,
        tenant_id: smartorderrouter::TenantId,
        credential: &smartorderrouter::ExchangeCredential,
        builder: &dyn multi_tenant::ConnectorBuilder,
    ) -> Result<(), ExecutionError> {
        if let Some(owner) = self.connector_owner(&credential.exchange).await {
            if owner != tenant_id {
                return Err(ExecutionError::Rejected(format!(
                    "Refusing to register '{}' for tenant {}: connector is already owned by tenant {}",
                    credential.exchange, tenant_id, owner
                )));
            }
        }
        let connector = builder.build(credential).await?;

        let mut connectors = self.connectors.write().await;
        connectors.insert(credential.exchange.clone(), connector);
        self.connector_owners.write().await.insert(credential.exchange.clone(), tenant_id);

        // Set first exchange as default if none set
        if self.default_exchange.is_none() {
            self.default_exchange = Some(credential.exchange.clone());
        }
        
        log::info!(
            "[EXECUTION] Added exchange '{}' from credential '{}' (testnet={})",
            credential.exchange, credential.label, credential.is_testnet
        );
        
        Ok(())
    }
    
    /// Initialize all exchanges from the credentials `provider` holds for
    /// `tenant_id`.
    ///
    /// Call this at startup to initialize connectors for live trading. The
    /// provider is asked for exactly this tenant's credentials; any provider
    /// error aborts (fail closed) and no connector is registered.
    pub async fn initialize_from_provider(
        &mut self,
        provider: &dyn smartorderrouter::CredentialProvider,
        tenant_id: uuid::Uuid,
    ) -> Result<usize, ExecutionError> {
        let credentials = smartorderrouter::resolve_all_credentials(provider, tenant_id)
            .await
            .map_err(|e| ExecutionError::Unknown(format!(
                "Failed to load credentials for tenant {}: {}", tenant_id, e
            )))?;

        let mut initialized_count = 0;
        for credential in &credentials {
            match self.add_exchange_from_credential(tenant_id, credential).await {
                Ok(_) => {
                    initialized_count += 1;
                }
                Err(e) => {
                    log::error!(
                        "[EXECUTION] Failed to initialize exchange '{}' from credential '{}': {}",
                        credential.exchange, credential.label, e
                    );
                }
            }
        }
        
        log::info!(
            "[EXECUTION] Initialized {}/{} exchanges from database for tenant {}",
            initialized_count, credentials.len(), tenant_id
        );
        
        Ok(initialized_count)
    }

    /// Returns true if a connector for `exchange_name` is currently registered.
    ///
    /// Cheap read-locked lookup. Used by callers that want to check before
    /// triggering a (more expensive) `ensure_exchange_for_tenant` reload.
    pub async fn has_connector(&self, exchange_name: &str) -> bool {
        let connectors = self.connectors.read().await;
        connectors.contains_key(exchange_name)
    }

    /// Start a connector's real-time order-update stream (currently only
    /// Alpaca does real work here -- see `GenericConnector::subscribe_to_updates`).
    /// A no-op, not an error, when no connector is registered for
    /// `exchange_name` yet -- callers that want a live deployment's fills
    /// confirmed should call this right after `ensure_exchange_for_tenant`
    /// succeeds for that same exchange.
    pub async fn subscribe_exchange_updates(
        &self,
        exchange_name: &str,
        callback: Box<dyn Fn(crate::core::types::OrderUpdate) + Send + Sync>,
    ) {
        let connectors = self.connectors.read().await;
        if let Some(connector) = connectors.get(exchange_name) {
            connector.subscribe_to_updates(callback).await;
        }
    }

    /// Resolve a single tenant+exchange credential through `provider` and
    /// register the resulting connector. Idempotent — calling this repeatedly
    /// rebuilds the connector with the latest credentials, so users who edit
    /// their API keys in the UI see the change without a pod restart.
    ///
    /// FAILS CLOSED: returns an error (and registers nothing) if the tenant
    /// has no usable credential for the exchange (unknown tenant/exchange,
    /// disabled, sandbox-only when `live_only`), if the provider errors, if the
    /// connector cannot be initialized, or if a different tenant already owns
    /// the connector for `exchange`. There is no fallback to another tenant's
    /// credential and no "first row".
    ///
    /// `live_only: true` restricts the lookup to non-testnet credentials and
    /// MUST be set when registering a connector for a live deployment — a
    /// tenant holding both sandbox and production keys must never have live
    /// orders signed with the sandbox key.
    pub async fn ensure_exchange_for_tenant(
        &mut self,
        provider: &dyn smartorderrouter::CredentialProvider,
        tenant_id: uuid::Uuid,
        exchange: &str,
        live_only: bool,
    ) -> Result<(), ExecutionError> {
        self.ensure_exchange_for_tenant_with(
            provider,
            tenant_id,
            exchange,
            live_only,
            &multi_tenant::DefaultConnectorBuilder,
        ).await
    }

    /// [`Self::ensure_exchange_for_tenant`] with an explicit connector builder.
    pub async fn ensure_exchange_for_tenant_with(
        &mut self,
        provider: &dyn smartorderrouter::CredentialProvider,
        tenant_id: uuid::Uuid,
        exchange: &str,
        live_only: bool,
        builder: &dyn multi_tenant::ConnectorBuilder,
    ) -> Result<(), ExecutionError> {
        let cred = smartorderrouter::resolve_credential(provider, tenant_id, exchange, live_only)
            .await
            .map_err(|e| ExecutionError::Unknown(format!(
                "No usable credential for tenant={} exchange={} (live_only={}): {}",
                tenant_id, exchange, live_only, e
            )))?;

        self.add_exchange_from_credential_with(tenant_id, &cred, builder).await?;
        log::info!(
            "[EXECUTION] Loaded {} credential for tenant {} (live_only={})",
            exchange, tenant_id, live_only
        );
        Ok(())
    }

    /// Execute an order on behalf of `tenant_id`, refusing unless the
    /// connector for `exchange_name` was registered from THAT tenant's
    /// credentials. Use this (not `execute_order_on_exchange`) whenever the
    /// order belongs to a tenant, so an order can never be signed with another
    /// tenant's keys.
    pub async fn execute_order_on_exchange_for_tenant(
        &self,
        tenant_id: smartorderrouter::TenantId,
        signal: &Signal,
        exchange_name: &str,
    ) -> Result<ExecutionResult, ExecutionError> {
        match self.connector_owner(exchange_name).await {
            Some(owner) if owner == tenant_id => {
                self.execute_order_on_exchange(signal, exchange_name).await
            }
            Some(owner) => Err(ExecutionError::Rejected(format!(
                "Exchange '{}' connector belongs to tenant {}, not tenant {}",
                exchange_name, owner, tenant_id
            ))),
            None => Err(ExecutionError::Rejected(format!(
                "No tenant-owned connector for exchange '{}' (tenant {})",
                exchange_name, tenant_id
            ))),
        }
    }

    /// Remove an exchange connector
    pub async fn remove_exchange(&mut self, exchange_name: &str) -> Result<(), ExecutionError> {
        let mut connectors = self.connectors.write().await;
        connectors.remove(exchange_name);
        self.connector_owners.write().await.remove(exchange_name);

        // Update default if removed
        if self.default_exchange.as_deref() == Some(exchange_name) {
            self.default_exchange = connectors.keys().next().cloned();
        }
        
        Ok(())
    }

    /// List all available exchanges
    pub async fn list_exchanges(&self) -> Vec<String> {
        let connectors = self.connectors.read().await;
        connectors.keys().cloned().collect()
    }

    /// Set the default exchange for orders without explicit exchange
    pub fn set_default_exchange(&mut self, exchange_name: String) {
        self.default_exchange = Some(exchange_name);
    }

    /// Resolve a reference price for the pre-trade risk gate's dollar-
    /// denominated checks, in order: the signal's own price (set for limit
    /// orders, often `None` for market orders) -> the position tracker's
    /// last recorded market tick for the symbol -> the symbol's existing
    /// position's average price on this exchange. Returns `None` if none
    /// of the three resolve -- callers then fall back to the quantity-only
    /// risk checks (`RiskManager::pre_trade_check_no_price`) instead of
    /// guessing a price.
    fn resolve_reference_price(&self, signal: &Signal, exchange_name: &str) -> Option<f64> {
        if let Some(price) = signal.price {
            return Some(price);
        }
        if let Some(price) = self.position_tracker.get_market_price(&signal.symbol) {
            return Some(price);
        }
        self.position_tracker
            .get_position(&signal.symbol, exchange_name)
            .ok()
            .flatten()
            .map(|p| p.average_price)
            .filter(|price| *price > 0.0)
    }

    /// Execute order on specified exchange
    pub async fn execute_order_on_exchange(&self, signal: &Signal, exchange_name: &str) -> Result<ExecutionResult, ExecutionError> {
        // P0 Safety: Check kill switch before any order execution
        if KILL_SWITCH.is_triggered() {
            let reason = KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual);
            return Err(ExecutionError::Rejected(format!(
                "Kill switch triggered: {:?}. All trading halted.", reason
            )));
        }

        // P1 live pre-trade risk gate (platform-wide limits; plan "Priority
        // 1 -- Live pre-trade risk gate", Phase 1). Dark by default
        // (`risk_controls::PreTradeGateMode::DryRun`): a would-be rejection
        // is logged and counted, then the order falls through to the
        // connector exactly as before -- zero behavior change until
        // `PRE_TRADE_RISK_GATE_ENFORCE=true` is set. A single order's
        // rejection here is deliberately NOT escalated to `KILL_SWITCH` --
        // that's reserved for systemic, all-tenant halts.
        let is_buy = matches!(signal.action, SignalAction::Buy | SignalAction::BuyLimit | SignalAction::BuyStop);
        let reference_price = self.resolve_reference_price(signal, exchange_name);

        let risk_check = match reference_price {
            Some(price) => self.risk_manager
                .pre_trade_check(&signal.symbol, exchange_name, signal.quantity, price, is_buy)
                .await,
            None => {
                log::warn!(
                    "[pre-trade-risk-gate] no resolvable reference price for order {} ({} qty {} on {}); skipping the 3 dollar-denominated checks (max_order_value/max_position_value/max_portfolio_value), still enforcing kill-switch/circuit-breaker/rate-limit/max_order_size/max_position_size/max_open_positions",
                    signal.id, signal.symbol, signal.quantity, exchange_name
                );
                self.risk_manager
                    .pre_trade_check_no_price(&signal.symbol, exchange_name, signal.quantity, is_buy)
                    .await
            }
        };

        if let Err(e) = risk_check {
            prometheus_metrics::inc_orders_rejected();
            match self.pre_trade_gate_mode {
                risk_controls::PreTradeGateMode::Enforce => {
                    log::warn!(
                        "[pre-trade-risk-gate] ENFORCE: rejecting order {} ({} on {}): {}",
                        signal.id, signal.symbol, exchange_name, e
                    );
                    return Err(ExecutionError::Rejected(format!("Pre-trade risk check failed: {}", e)));
                }
                risk_controls::PreTradeGateMode::DryRun => {
                    log::warn!(
                        "[pre-trade-risk-gate] DRY-RUN would-reject order {} ({} on {}): {} -- order still placed (dry-run, zero behavior change)",
                        signal.id, signal.symbol, exchange_name, e
                    );
                    // Fall through: proceed exactly as today.
                }
            }
        }

        let connectors = self.connectors.read().await;
        let connector = connectors.get(exchange_name)
            .ok_or_else(|| ExecutionError::Unknown(format!("Exchange not found: {exchange_name}")))?;
        
        let timer = optimizations::NanoTimer::start();
        
        // Log order attempt
        TradingLogger::log_order_execution(
            exchange_name,
            &signal.symbol,
            &format!("{:?}", signal.action),
            signal.quantity,
            signal.price.unwrap_or(0.0),
            &signal.id,
            0, // Will update after execution
            "INITIATED"
        );

        let result = connector.execute_order(signal).await;
        let latency_ns = timer.elapsed_ns();
        
        // Record performance metrics
        self.performance_monitor.record_latency("execution", "order_placement", latency_ns).await;

        match &result {
            Ok(execution_result) => {
                // Calculate volume and fees from the execution
                let volume = execution_result.filled_quantity * execution_result.avg_fill_price;
                let fees = execution_result.total_fees;
                self.global_metrics.record_success(latency_ns, volume, fees);
                
                // Log successful execution
                TradingLogger::log_order_execution(
                    exchange_name,
                    &signal.symbol,
                    &format!("{:?}", signal.action),
                    signal.quantity,
                    signal.price.unwrap_or(0.0),
                    &execution_result.order_id,
                    latency_ns,
                    &format!("{:?}", execution_result.status)
                );
                
                // Save execution to database
                self.save_execution_to_database(signal, execution_result, exchange_name, latency_ns);
                
                // Update position tracking with execution fills
                if !execution_result.fills.is_empty() {
                    if let Some(fill) = execution_result.fills.first() {
                        let old_position = self.position_tracker.get_position(&signal.symbol, exchange_name)
                            .unwrap_or_default()
                            .map(|p| p.quantity)
                            .unwrap_or(0.0);

                        let fill_quantity = if signal.action == SignalAction::Buy { 
                            fill.quantity 
                        } else { 
                            -fill.quantity 
                        };
                        
                        if let Err(e) = self.position_tracker.update_fill(
                            &signal.symbol,
                            exchange_name,
                            fill_quantity,
                            fill.price,
                            fill.fee
                        ) {
                            TradingLogger::log_error("position_tracker", "update_fill", &e, Some(&signal.symbol));
                        } else {
                            // Log position update
                            let new_position = self.position_tracker.get_position(&signal.symbol, exchange_name)
                                .unwrap_or_default()
                                .map(|p| p.quantity)
                                .unwrap_or(0.0);

                            TradingLogger::log_position_update(
                                &signal.symbol,
                                exchange_name,
                                old_position,
                                new_position,
                                fill.price,
                                0.0 // Realized PnL would be calculated in position tracker
                            );
                        }

                        // Pre-trade risk gate (Phase 1) post-fill wiring:
                        // mirror this fill into the risk manager's own
                        // position-limit state so subsequent orders'
                        // `pre_trade_check` sees accumulated exposure, and
                        // record the order as a circuit-breaker success.
                        // Deliberately does NOT call
                        // `circuit_breaker.update_equity(...)` /
                        // `RiskManager::post_trade_update`'s equity path --
                        // live equity/drawdown wiring is explicit future
                        // work (plan Phase 1b) given a bad equity read's
                        // platform-wide kill-switch blast radius.
                        self.risk_manager.position_limits
                            .update_position(&signal.symbol, exchange_name, fill_quantity, fill.price)
                            .await;
                        self.risk_manager.circuit_breaker.record_success();
                    }
                }
            }
            Err(error) => {
                self.global_metrics.record_failure();
                self.risk_manager.circuit_breaker.record_failure();

                // Log error and record monitoring metric
                let error_msg = format!("{error:?}");
                TradingLogger::log_error("execution", "order_placement", &error_msg, Some(&signal.symbol));
                self.performance_monitor.record_error("execution", "order_placement", &error_msg).await;
                
                // Log failed execution
                TradingLogger::log_order_execution(
                    exchange_name,
                    &signal.symbol,
                    &format!("{:?}", signal.action),
                    signal.quantity,
                    signal.price.unwrap_or(0.0),
                    &signal.id,
                    latency_ns,
                    "FAILED"
                );
            }
        }
        
        result
    }

    /// Execute order using signal's exchange or default
    pub async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
        let exchange_name = if !signal.exchange.is_empty() {
            signal.exchange.as_str()
        } else if let Some(ref default) = self.default_exchange {
            default.as_str()
        } else {
            return Err(ExecutionError::Unknown("No exchange specified and no default set".to_string()));
        };
        
        self.execute_order_on_exchange(signal, exchange_name).await
    }

    /// Execute batch orders across multiple exchanges (sequential - safe)
    pub async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        // P0 Safety: Check kill switch before any batch execution
        if KILL_SWITCH.is_triggered() {
            let reason = KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual);
            return Err(ExecutionError::Rejected(format!(
                "Kill switch triggered: {:?}. Batch execution halted ({} orders).", 
                reason, signals.len()
            )));
        }
        self.execute_batch_orders_sequential(signals).await
    }

    /// Execute batch orders sequentially (conservative approach)
    pub async fn execute_batch_orders_sequential(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        // P0 Safety: Check kill switch before sequential batch
        if KILL_SWITCH.is_triggered() {
            let reason = KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual);
            return Err(ExecutionError::Rejected(format!(
                "Kill switch triggered: {:?}. Sequential batch halted.", reason
            )));
        }
        
        // Group signals by exchange
        let mut exchange_groups: HashMap<String, Vec<&Signal>> = HashMap::new();
        
        for signal in signals {
            let exchange = if !signal.exchange.is_empty() {
                signal.exchange.clone()
            } else if let Some(ref default) = self.default_exchange {
                default.clone()
            } else {
                return Err(ExecutionError::Unknown("No exchange specified and no default set".to_string()));
            };
            
            exchange_groups.entry(exchange).or_default().push(signal);
        }
        
        // Execute each group sequentially to avoid lifetime issues
        let connectors = self.connectors.read().await;
        let mut all_results = Vec::new();
        
        for (exchange_name, exchange_signals) in exchange_groups {
            if let Some(connector) = connectors.get(&exchange_name) {
                let signals_vec: Vec<Signal> = exchange_signals.into_iter().cloned().collect();
                
                let mut batch_results = connector.execute_batch_orders(&signals_vec).await?;
                all_results.append(&mut batch_results);
            }
        }
        
        Ok(all_results)
    }

    /// Execute batch orders in parallel across exchanges (high-performance)
    pub async fn execute_batch_orders_parallel(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        use futures::future::try_join_all;
        use std::sync::Arc;
        
        // P0 Safety: Check kill switch before parallel batch
        if KILL_SWITCH.is_triggered() {
            let reason = KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual);
            return Err(ExecutionError::Rejected(format!(
                "Kill switch triggered: {:?}. Parallel batch halted ({} orders).", 
                reason, signals.len()
            )));
        }
        
        // Group signals by exchange
        let mut exchange_groups: HashMap<String, Vec<Signal>> = HashMap::new();
        
        for signal in signals {
            let exchange = if !signal.exchange.is_empty() {
                signal.exchange.clone()
            } else if let Some(ref default) = self.default_exchange {
                default.clone()
            } else {
                return Err(ExecutionError::Unknown("No exchange specified and no default set".to_string()));
            };
            
            exchange_groups.entry(exchange).or_default().push(signal.clone());
        }
        
        // Create futures for parallel execution across exchanges
        let connectors = Arc::new(self.connectors.read().await);
        let mut exchange_futures = Vec::new();
        
        for (exchange_name, exchange_signals) in exchange_groups {
            if connectors.get(&exchange_name).is_some() {
                let connectors_ref = Arc::clone(&connectors);
                let exchange_name_owned = exchange_name.clone();
                
                let future = async move {
                    if let Some(connector) = connectors_ref.get(&exchange_name_owned) {
                        connector.execute_batch_orders_parallel(&exchange_signals).await
                    } else {
                        Err(ExecutionError::Unknown(format!("Exchange not found: {exchange_name_owned}")))
                    }
                };
                
                exchange_futures.push(future);
            }
        }
        
        // Execute all exchange batches in parallel
        let batch_results: Result<Vec<Vec<ExecutionResult>>, ExecutionError> = try_join_all(exchange_futures).await;
        
        // Flatten results from all exchanges
        let all_batches = batch_results?;
        let mut all_results = Vec::new();
        
        for mut batch in all_batches {
            all_results.append(&mut batch);
        }
        
        Ok(all_results)
    }

    /// Execute batch orders with hybrid approach: parallel exchanges, configurable intra-exchange processing
    pub async fn execute_batch_orders_optimized(&self, signals: &[Signal], max_parallel_per_exchange: usize) -> Result<Vec<ExecutionResult>, ExecutionError> {
        use futures::future::try_join_all;
        use std::sync::Arc;
        
        // P0 Safety: Check kill switch before optimized batch
        if KILL_SWITCH.is_triggered() {
            let reason = KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual);
            return Err(ExecutionError::Rejected(format!(
                "Kill switch triggered: {:?}. Optimized batch halted ({} orders).", 
                reason, signals.len()
            )));
        }
        
        // Group signals by exchange
        let mut exchange_groups: HashMap<String, Vec<Signal>> = HashMap::new();
        
        for signal in signals {
            let exchange = if !signal.exchange.is_empty() {
                signal.exchange.clone()
            } else if let Some(ref default) = self.default_exchange {
                default.clone()
            } else {
                return Err(ExecutionError::Unknown("No exchange specified and no default set".to_string()));
            };
            
            exchange_groups.entry(exchange).or_default().push(signal.clone());
        }
        
        // Execute exchanges in parallel
        let connectors = Arc::new(self.connectors.read().await);
        let mut exchange_futures = Vec::new();
        
        for (exchange_name, exchange_signals) in exchange_groups {
            if connectors.get(&exchange_name).is_some() {
                let connectors_ref = Arc::clone(&connectors);
                let exchange_name_owned = exchange_name.clone();
                
                let future = async move {
                    if let Some(connector) = connectors_ref.get(&exchange_name_owned) {
                        connector.execute_batch_orders_optimized(&exchange_signals, max_parallel_per_exchange).await
                    } else {
                        Err(ExecutionError::Unknown(format!("Exchange not found: {exchange_name_owned}")))
                    }
                };
                
                exchange_futures.push(future);
            }
        }
        
        // Execute all exchange batches in parallel
        let batch_results: Result<Vec<Vec<ExecutionResult>>, ExecutionError> = try_join_all(exchange_futures).await;
        
        // Flatten results from all exchanges
        let all_batches = batch_results?;
        let mut all_results = Vec::new();
        
        for mut batch in all_batches {
            all_results.append(&mut batch);
        }
        
        Ok(all_results)
    }

    /// Cancel order on specific exchange
    pub async fn cancel_order_on_exchange(&self, order_id: &str, exchange_name: &str) -> Result<CancelResult, ExecutionError> {
        let connectors = self.connectors.read().await;
        let connector = connectors.get(exchange_name)
            .ok_or_else(|| ExecutionError::Unknown(format!("Exchange not found: {exchange_name}")))?;
        
        connector.cancel_order(order_id).await
    }

    /// Cancel all orders across all exchanges
    pub async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError> {
        let connectors = self.connectors.read().await;
        let mut all_results = Vec::new();
        
        for (_exchange_name, connector) in connectors.iter() {
            match connector.cancel_all_orders().await {
                Ok(mut results) => all_results.append(&mut results),
                Err(_) => continue, // Skip failed exchanges
            }
        }
        
        Ok(all_results)
    }

    /// Get aggregated metrics across all exchanges
    pub async fn get_global_metrics(&self) -> ExecutionMetrics {
        self.global_metrics.get_metrics("Global".to_string())
    }

    /// Get metrics for specific exchange
    pub async fn get_exchange_metrics(&self, exchange_name: &str) -> Option<ExecutionMetrics> {
        let connectors = self.connectors.read().await;
        connectors.get(exchange_name).map(|c| c.get_metrics())
    }

    /// Get health status for all exchanges
    pub async fn health_check_all(&self) -> HashMap<String, HealthStatus> {
        let connectors = self.connectors.read().await;
        let mut results = HashMap::new();
        
        for (exchange_name, connector) in connectors.iter() {
            match connector.health_check().await {
                Ok(status) => { results.insert(exchange_name.clone(), status); }
                Err(_) => continue,
            }
        }
        
        results
    }

    /// Initialize system optimizations
    pub async fn initialize_optimizations(&self) -> Result<(), ExecutionError> {
        // Set CPU affinity for trading threads
        if let Err(e) = optimizations::set_cpu_affinity(self.core_assignment.primary_execution) {
            log::warn!("Failed to set CPU affinity: {e}");
        }
        
        // Set high process priority
        if let Err(e) = optimizations::set_high_priority() {
            log::warn!("Failed to set high priority: {e}");
        }
        
        // Preallocate memory pools
        optimizations::preallocate_thread_local(1000);
        
        // Initialize global memory pool
        core::initialize_order_pool(10000);
        
        Ok(())
    }

    /// Create configuration template for supported exchange
    pub fn create_exchange_config(exchange_name: &str) -> Result<ExchangeConfig, ExecutionError> {
        ExchangeFactory::create_config_template(exchange_name)
    }

    /// Get supported exchanges
    pub fn supported_exchanges() -> Vec<&'static str> {
        ExchangeFactory::supported_exchanges()
    }

    // Position tracking methods

    /// Get current position for a symbol on an exchange
    pub fn get_position(&self, symbol: &str, exchange: &str) -> Result<Option<position_tracker::Position>, ExecutionError> {
        self.position_tracker.get_position(symbol, exchange)
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {e}")))
    }

    /// Get all current positions
    pub fn get_all_positions(&self) -> Result<HashMap<(String, String), position_tracker::Position>, ExecutionError> {
        self.position_tracker.get_all_positions()
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {e}")))
    }

    /// Get net position for a symbol across all exchanges
    pub fn get_net_position(&self, symbol: &str) -> Result<f64, ExecutionError> {
        self.position_tracker.get_net_position(symbol)
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {e}")))
    }

    /// Get total portfolio PnL
    pub fn get_portfolio_pnl(&self) -> Result<position_tracker::PortfolioPnL, ExecutionError> {
        self.position_tracker.get_total_pnl()
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {e}")))
    }

    /// Get PnL by exchange
    pub fn get_pnl_by_exchange(&self) -> Result<HashMap<String, f64>, ExecutionError> {
        self.position_tracker.get_pnl_by_exchange()
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {e}")))
    }

    /// Update market price for PnL calculations
    pub fn update_market_price(&self, symbol: &str, price: f64) -> Result<(), ExecutionError> {
        self.position_tracker.update_market_price(symbol, price)
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {e}")))
    }

    /// Get positions that need market price updates (stale positions)
    pub fn get_stale_positions(&self, max_age_ms: u64) -> Result<Vec<(String, String)>, ExecutionError> {
        self.position_tracker.get_stale_positions(max_age_ms)
            .map_err(|e| ExecutionError::Unknown(format!("Position tracking error: {e}")))
    }

    // Performance monitoring methods

    /// Get current performance metrics
    pub async fn get_performance_metrics(&self) -> monitoring::PerformanceMetrics {
        self.performance_monitor.get_metrics().await
    }

    /// Get recent alerts
    pub async fn get_recent_alerts(&self, max_age_ms: u64) -> Vec<monitoring::Alert> {
        self.performance_monitor.get_recent_alerts(max_age_ms).await
    }

    /// Generate comprehensive performance report
    pub async fn generate_performance_report(&self) -> monitoring::PerformanceReport {
        self.performance_monitor.generate_report().await
    }

    /// Acknowledge alert by index
    pub async fn acknowledge_alert(&self, alert_index: usize) -> Result<(), ExecutionError> {
        self.performance_monitor.acknowledge_alert(alert_index).await
            .map_err(|e| ExecutionError::Unknown(format!("Alert acknowledgment error: {e}")))
    }

    /// Record custom performance metric
    pub async fn record_custom_metric(&self, component: &str, operation: &str, latency_ns: u64) {
        self.performance_monitor.record_latency(component, operation, latency_ns).await;
    }

    /// Update memory usage tracking
    pub async fn update_memory_usage(&self, component: &str, memory_bytes: u64) {
        self.performance_monitor.record_memory_usage(component, memory_bytes).await;
    }

    /// Update throughput tracking
    pub async fn update_throughput(&self, component: &str, ops_per_second: f64) {
        self.performance_monitor.record_throughput(component, ops_per_second).await;
    }
}

impl Default for UltraLowLatencyExecutionHandler {
    fn default() -> Self {
        tokio::runtime::Runtime::new().unwrap().block_on(Self::new())
    }
}

impl UltraLowLatencyExecutionHandler {
    pub async fn cancel_order(&self, order_id: &str) -> Result<bool, ExecutionError> {
        if let Some(ref exchange) = self.default_exchange {
            let result = self.cancel_order_on_exchange(order_id, exchange).await?;
            Ok(matches!(result.status, core::types::CancelStatus::Cancelled))
        } else {
            Err(ExecutionError::Unknown("No default exchange set".to_string()))
        }
    }

    pub async fn get_order_status(&self, order_id: &str) -> Result<Option<OrderStatus>, ExecutionError> {
        if let Some(ref exchange) = self.default_exchange {
            let connectors = self.connectors.read().await;
            if let Some(connector) = connectors.get(exchange) {
                connector.get_order_status(order_id).await
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod credential_isolation_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handler_creation() {
        let handler = UltraLowLatencyExecutionHandler::new().await;
        assert!(handler.list_exchanges().await.is_empty());
    }

    #[tokio::test]
    async fn test_execution_database_integration() {
        // Create execution database
        let execution_db = Arc::new(InMemoryExecutionDatabase::new());
        
        // Create handler with database integration
        let handler = UltraLowLatencyExecutionHandler::new_with_database(execution_db.clone()).await;
        
        // Verify database is integrated
        assert!(handler.execution_database.is_some());
        
        // Check initial state
        let executions = execution_db.get_all_executions();
        assert!(executions.is_empty());
        
        println!("✅ ExecutionHandler created with database integration");
        println!("📊 Initial executions in database: {}", executions.len());
    }

    #[tokio::test]
    async fn test_execution_database_save() {
        let execution_db = InMemoryExecutionDatabase::new();
        
        let execution_data = ExecutionData {
            order_id: "test_order_123".to_string(),
            exchange: "Kraken".to_string(),
            symbol: "BTC/USD".to_string(),
            side: "Buy".to_string(),
            quantity: 1.0,
            filled_quantity: 1.0,
            price: 50000.0,
            fee: 25.0,
            status: "Filled".to_string(),
            executed_at: Utc::now(),
            latency_ns: 1500000, // 1.5ms
        };
        
        // Save execution
        let result = execution_db.save_execution(&execution_data);
        assert!(result.is_ok());
        
        // Verify saved
        let executions = execution_db.get_all_executions();
        assert_eq!(executions.len(), 1);
        
        let saved_execution = &executions[0];
        assert_eq!(saved_execution.order_id, "test_order_123");
        assert_eq!(saved_execution.exchange, "Kraken");
        assert_eq!(saved_execution.symbol, "BTC/USD");
        assert_eq!(saved_execution.side, "Buy");
        assert_eq!(saved_execution.filled_quantity, 1.0);
        assert_eq!(saved_execution.price, 50000.0);
        
        println!("✅ Execution successfully saved to database");
        println!("💾 Order ID: {}, Filled: {} {} @ {}", 
            saved_execution.order_id, 
            saved_execution.filled_quantity, 
            saved_execution.symbol, 
            saved_execution.price
        );
    }

    #[tokio::test]
    #[ignore] // Requires KRAKEN_API_KEY environment variable
    async fn test_exchange_management() {
        let mut handler = UltraLowLatencyExecutionHandler::new().await;
        
        // Test adding exchange
        let config = ExchangeFactory::create_config_template("kraken").unwrap();
        handler.add_exchange("Kraken".to_string(), config).await.unwrap();
        
        let exchanges = handler.list_exchanges().await;
        assert_eq!(exchanges.len(), 1);
        assert_eq!(exchanges[0], "Kraken");
        
        // Test removing exchange
        handler.remove_exchange("Kraken").await.unwrap();
        assert!(handler.list_exchanges().await.is_empty());
    }

    #[tokio::test]
    async fn test_supported_exchanges() {
        let supported = UltraLowLatencyExecutionHandler::supported_exchanges();
        // Currently only kraken is implemented
        assert!(supported.contains(&"kraken"));
        assert!(!supported.is_empty());
    }

    #[test]
    fn test_nano_timestamp() {
        let ts1 = optimizations::nano_timestamp();
        let ts2 = optimizations::nano_timestamp();
        assert!(ts2 >= ts1);
    }

    #[test]
    fn test_memory_pool() {
        let order1 = optimizations::get_thread_local_order();
        let order2 = optimizations::get_thread_local_order();
        
        optimizations::return_thread_local_order(order1);
        optimizations::return_thread_local_order(order2);
    }

    #[test]
    fn test_metrics_calculation() {
        let latencies = vec![100.0, 200.0, 300.0, 400.0, 500.0];
        let metrics = optimizations::simd_calculate_percentiles(&latencies);

        assert_eq!(metrics.count, 5);
        assert_eq!(metrics.min_ns, 100);
        assert_eq!(metrics.max_ns, 500);
    }

    // ========================================================================
    // Pre-trade risk gate (Phase 1 dry-run wiring) tests
    // ========================================================================
    //
    // `PaperTradingConnector` has no public call/fill counter, so these use a
    // small local `CountingConnector` (same shape as `core::traits::tests`'
    // existing `MockConnector`) instead -- it lets every order through
    // unconditionally and exposes an `Arc<AtomicU64>` call counter, so
    // "the connector was never touched" can be asserted directly rather than
    // inferred. `pre_trade_gate_mode` and `risk_manager` are private fields
    // set directly here (this test module is a descendant of the crate root,
    // so it has access) instead of through the `PRE_TRADE_RISK_GATE_ENFORCE`
    // env var, since env vars are process-global and would race against
    // other tests running in parallel in the same process.
    mod pre_trade_risk_gate_tests {
        use super::*;
        use std::sync::atomic::{AtomicU64, Ordering};
        use crate::core::types::*;
        // KILL_SWITCH is a process-wide global static -- these tests must
        // never run concurrently with each other (or they'll race on it),
        // but can still run in parallel with every other, unrelated test.
        use serial_test::serial;

        struct CountingConnector {
            calls: Arc<AtomicU64>,
            fill_price: f64,
        }

        #[async_trait::async_trait]
        impl core::traits::ExchangeConnector for CountingConnector {
            fn exchange_name(&self) -> &str { "counting" }

            async fn initialize(&mut self, _config: ExchangeConfig) -> Result<(), ExecutionError> { Ok(()) }

            async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let side = if signal.action == SignalAction::Buy { OrderSide::Buy } else { OrderSide::Sell };
                Ok(ExecutionResult {
                    order_id: signal.id.clone(),
                    exchange_order_id: Some(format!("exch_{}", signal.id)),
                    exchange: "counting".into(),
                    status: ExecutionStatus::Filled,
                    filled_quantity: signal.quantity,
                    remaining_quantity: 0.0,
                    avg_fill_price: self.fill_price,
                    total_fees: 0.0,
                    fills: vec![ExecutionFill {
                        fill_id: format!("fill_{}", signal.id),
                        order_id: signal.id.clone(),
                        exchange_order_id: format!("exch_{}", signal.id),
                        symbol: signal.symbol.clone(),
                        side,
                        quantity: signal.quantity,
                        price: self.fill_price,
                        fee: 0.0,
                        fee_asset: "USD".into(),
                        timestamp: now,
                        trade_id: format!("trade_{}", signal.id),
                        is_maker: false,
                        exchange_timestamp_ns: None,
                        exchange_sequence: None,
                    }],
                    reject_reason: None,
                    submitted_at: now,
                    updated_at: now,
                    latency_ns: 0,
                    exchange_timestamp_ns: None,
                    exchange_sequence: None,
                })
            }

            async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
                self.execute_batch_orders_sequential(signals).await
            }
            async fn cancel_order(&self, _id: &str) -> Result<CancelResult, ExecutionError> { unimplemented!() }
            async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError> { unimplemented!() }
            async fn edit_order(&self, _p: EditOrderParams) -> Result<EditResult, ExecutionError> { unimplemented!() }
            async fn get_order_status(&self, _id: &str) -> Result<Option<OrderStatus>, ExecutionError> { unimplemented!() }
            fn get_metrics(&self) -> ExecutionMetrics { unimplemented!() }
            async fn subscribe_to_updates(&self, _cb: Box<dyn Fn(OrderUpdate) + Send + Sync>) {}
            async fn health_check(&self) -> Result<HealthStatus, ExecutionError> { unimplemented!() }
            fn get_limits(&self) -> ExchangeLimits { unimplemented!() }
            fn validate_order(&self, _s: &Signal) -> Result<(), ExecutionError> { Ok(()) }
            fn convert_signal(&self, _s: &Signal) -> Result<ExchangeOrder, ExecutionError> { unimplemented!() }
        }

        fn make_signal(id: &str, symbol: &str, exchange: &str, quantity: f64, price: Option<f64>) -> Signal {
            Signal {
                id: id.to_string(),
                strategy_id: "test-strategy".to_string(),
                symbol: symbol.to_string(),
                exchange: exchange.to_string(),
                action: SignalAction::Buy,
                quantity,
                price,
                confidence: 1.0,
                timestamp: 0,
                metadata: std::collections::HashMap::new(),
            }
        }

        async fn handler_with_counting_connector(
            risk_manager: Arc<RiskManager>,
            mode: PreTradeGateMode,
        ) -> (UltraLowLatencyExecutionHandler, Arc<AtomicU64>) {
            let mut handler = UltraLowLatencyExecutionHandler::new().await
                .with_risk_manager(risk_manager);
            handler.pre_trade_gate_mode = mode;

            let call_counter = Arc::new(AtomicU64::new(0));
            let connector = Box::new(CountingConnector {
                calls: call_counter.clone(),
                fill_price: 50_000.0,
            }) as Box<dyn core::traits::ExchangeConnector>;
            handler.connectors.write().await.insert("counting".to_string(), connector);
            (handler, call_counter)
        }

        /// DryRun: an order that violates `PositionLimits::default()`'s
        /// `max_order_size` (1.0) is still placed -- the connector is still
        /// called and a fill is still registered -- while the rejection is
        /// logged and counted. Zero behavior change is the whole point of
        /// Phase 1.
        #[tokio::test]
        #[serial]
        async fn dry_run_lets_over_limit_order_through_and_counts_rejection() {
            KILL_SWITCH.reset();
            let (handler, call_counter) = handler_with_counting_connector(
                Arc::new(RiskManager::with_defaults()),
                PreTradeGateMode::DryRun,
            ).await;

            let rejected_before = prometheus_metrics::METRICS.get_counter("trading_orders_rejected_total");

            // max_order_size is 1.0 BTC -- 2.0 violates it.
            let signal = make_signal("dry-run-1", "BTC/USD", "counting", 2.0, Some(50_000.0));
            let result = handler.execute_order_on_exchange(&signal, "counting").await;

            assert!(result.is_ok(), "dry-run must let the order through, got {:?}", result);
            assert_eq!(call_counter.load(Ordering::SeqCst), 1, "connector should still be called in dry-run");

            let rejected_after = prometheus_metrics::METRICS.get_counter("trading_orders_rejected_total");
            assert!(rejected_after > rejected_before, "a would-be rejection should still be counted in dry-run");

            let position = handler.position_tracker.get_position("BTC/USD", "counting").unwrap();
            assert!(position.is_some(), "the order should have actually filled in dry-run");
        }

        /// Enforce: the same over-limit order is rejected with
        /// `ExecutionError::Rejected`, and the connector is never called --
        /// the gate short-circuits before `execute_order_on_exchange` ever
        /// reaches the connector lookup.
        #[tokio::test]
        #[serial]
        async fn enforce_blocks_over_limit_order_with_zero_connector_calls() {
            KILL_SWITCH.reset();
            let (handler, call_counter) = handler_with_counting_connector(
                Arc::new(RiskManager::with_defaults()),
                PreTradeGateMode::Enforce,
            ).await;

            let signal = make_signal("enforce-1", "BTC/USD", "counting", 2.0, Some(50_000.0));
            let result = handler.execute_order_on_exchange(&signal, "counting").await;

            match result {
                Err(ExecutionError::Rejected(msg)) => {
                    assert!(msg.contains("Pre-trade risk check failed"), "unexpected message: {msg}");
                }
                other => panic!("expected ExecutionError::Rejected, got {:?}", other),
            }
            assert_eq!(call_counter.load(Ordering::SeqCst), 0, "connector must never be called when enforced");

            let position = handler.position_tracker.get_position("BTC/USD", "counting").unwrap();
            assert!(position.is_none(), "a rejected order must not register a fill");
        }

        /// Regression: the pre-existing kill-switch short-circuit still
        /// fires before the new pre-trade gate code is ever reached,
        /// regardless of gate mode, and the connector is never touched.
        #[tokio::test]
        #[serial]
        async fn kill_switch_precedence_unchanged() {
            KILL_SWITCH.reset();
            let (handler, call_counter) = handler_with_counting_connector(
                Arc::new(RiskManager::with_defaults()),
                PreTradeGateMode::DryRun,
            ).await;

            KILL_SWITCH.trigger(KillReason::Manual);

            // A perfectly ordinary, within-limits order.
            let signal = make_signal("kill-switch-1", "BTC/USD", "counting", 0.1, Some(50_000.0));
            let result = handler.execute_order_on_exchange(&signal, "counting").await;

            match result {
                Err(ExecutionError::Rejected(msg)) => {
                    assert!(msg.contains("Kill switch"), "unexpected message: {msg}");
                }
                other => panic!("expected kill-switch Rejected, got {:?}", other),
            }
            assert_eq!(call_counter.load(Ordering::SeqCst), 0, "kill switch must short-circuit before the connector");

            KILL_SWITCH.reset();
        }

        /// Reference-price fallback: with no signal price, no recorded
        /// market tick, and no existing position, the order still gets its
        /// quantity-based checks (and kill-switch/rate-limit) enforced --
        /// only the 3 dollar-denominated checks are skipped. A within-limits
        /// order with no price still succeeds; an over-`max_order_size`
        /// order with no price is still caught.
        #[tokio::test]
        #[serial]
        async fn no_resolvable_price_still_enforces_quantity_checks() {
            KILL_SWITCH.reset();
            let (handler, call_counter) = handler_with_counting_connector(
                Arc::new(RiskManager::with_defaults()),
                PreTradeGateMode::Enforce,
            ).await;

            // Within max_order_size (1.0), no price anywhere -- should pass
            // straight through the quantity-only path.
            let ok_signal = make_signal("no-price-ok", "ETH/USD", "counting", 0.5, None);
            let ok_result = handler.execute_order_on_exchange(&ok_signal, "counting").await;
            assert!(ok_result.is_ok(), "within-limits order with no price should still succeed, got {:?}", ok_result);
            assert_eq!(call_counter.load(Ordering::SeqCst), 1);

            // Exceeds max_order_size (1.0), still no price anywhere --
            // quantity check must still catch it even though no dollar
            // value could be computed.
            let bad_signal = make_signal("no-price-bad", "ETH/USD", "counting", 2.0, None);
            let bad_result = handler.execute_order_on_exchange(&bad_signal, "counting").await;
            match bad_result {
                Err(ExecutionError::Rejected(msg)) => {
                    assert!(msg.contains("Pre-trade risk check failed"), "unexpected message: {msg}");
                }
                other => panic!("expected ExecutionError::Rejected, got {:?}", other),
            }
            // Still 1 -- the second (over-limit) order never reached the connector.
            assert_eq!(call_counter.load(Ordering::SeqCst), 1);
        }

        /// Position-limit state accumulates across sequential orders: two
        /// orders that are each individually within `max_position_size`,
        /// but whose SUM exceeds it, must have the second one caught --
        /// this exercises the post-fill wiring (`position_limits.update_position`)
        /// from step 5, not just stateless per-order logic.
        #[tokio::test]
        #[serial]
        async fn position_limit_accumulates_across_sequential_orders() {
            KILL_SWITCH.reset();
            let limits = PositionLimits {
                max_order_size: 10.0,
                max_order_value: 1_000_000.0,
                max_position_size: 1.5, // two 1.0-unit buys sum to 2.0 > 1.5
                max_position_value: 1_000_000.0,
                max_portfolio_value: 1_000_000.0,
                max_open_positions: 20,
            };
            let risk_manager = Arc::new(RiskManager::new(limits, CircuitBreakerConfig::default()));
            let (handler, call_counter) = handler_with_counting_connector(
                risk_manager,
                PreTradeGateMode::Enforce,
            ).await;

            let signal_1 = make_signal("accum-1", "BTC/USD", "counting", 1.0, Some(50_000.0));
            let result_1 = handler.execute_order_on_exchange(&signal_1, "counting").await;
            assert!(result_1.is_ok(), "first order alone should be within the limit, got {:?}", result_1);
            assert_eq!(call_counter.load(Ordering::SeqCst), 1);

            // Individually identical to the first order, but the SUM (2.0)
            // now exceeds max_position_size (1.5) given the first order's
            // fill already accumulated into risk_manager.position_limits.
            let signal_2 = make_signal("accum-2", "BTC/USD", "counting", 1.0, Some(50_000.0));
            let result_2 = handler.execute_order_on_exchange(&signal_2, "counting").await;
            match result_2 {
                Err(ExecutionError::Rejected(msg)) => {
                    assert!(msg.contains("Pre-trade risk check failed"), "unexpected message: {msg}");
                }
                other => panic!("expected the second, sum-violating order to be rejected, got {:?}", other),
            }
            // Call count stays at 1 -- the second order never reached the connector.
            assert_eq!(call_counter.load(Ordering::SeqCst), 1);
        }

        /// Resolver order: signal.price -> position_tracker's last market
        /// tick -> the symbol's existing position's average_price -> None.
        #[tokio::test]
        async fn resolve_reference_price_follows_documented_fallback_order() {
            let handler = UltraLowLatencyExecutionHandler::new().await;

            // 1. Signal's own price wins outright.
            let s1 = make_signal("r1", "AAA/USD", "ex", 1.0, Some(123.0));
            assert_eq!(handler.resolve_reference_price(&s1, "ex"), Some(123.0));

            // 2. No signal price -> falls back to the position tracker's
            //    last recorded market tick for the symbol.
            handler.position_tracker.update_market_price("BBB/USD", 456.0).unwrap();
            let s2 = make_signal("r2", "BBB/USD", "ex", 1.0, None);
            assert_eq!(handler.resolve_reference_price(&s2, "ex"), Some(456.0));

            // 3. No signal price, no market tick -> falls back to the
            //    symbol's existing position's average_price on this exchange.
            handler.position_tracker.update_fill("CCC/USD", "ex", 1.0, 789.0, 0.0).unwrap();
            let s3 = make_signal("r3", "CCC/USD", "ex", 1.0, None);
            assert_eq!(handler.resolve_reference_price(&s3, "ex"), Some(789.0));

            // 4. None of the three resolve -> None.
            let s4 = make_signal("r4", "DDD/USD", "ex", 1.0, None);
            assert_eq!(handler.resolve_reference_price(&s4, "ex"), None);
        }
    }
}
