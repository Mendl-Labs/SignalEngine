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
    RiskManager,
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
    CredentialMode, ResolvedCredentialMode, compute_credential_mode, resolve_credential_mode,
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
        }
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

    /// Execute order on specified exchange
    pub async fn execute_order_on_exchange(&self, signal: &Signal, exchange_name: &str) -> Result<ExecutionResult, ExecutionError> {
        // P0 Safety: Check kill switch before any order execution
        if KILL_SWITCH.is_triggered() {
            let reason = KILL_SWITCH.get_trigger_reason().unwrap_or(KillReason::Manual);
            return Err(ExecutionError::Rejected(format!(
                "Kill switch triggered: {:?}. All trading halted.", reason
            )));
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
                    }
                }
            }
            Err(error) => {
                self.global_metrics.record_failure();
                
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

}
