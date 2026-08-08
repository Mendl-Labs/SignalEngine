//! Multi-Tenant Execution Handler for SaaS Trading Platform
//!
//! This module provides a multi-tenant execution handler that supports multiple
//! users trading simultaneously with fair scheduling and tier-based rate limiting.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────┐
//! │                  MultiTenantExecutionHandler                        │
//! │                                                                     │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │
//! │  │ TenantCtx A │  │ TenantCtx B │  │ TenantCtx C │  ...          │
//! │  │ (Pro)       │  │ (Free)      │  │ (Live)      │               │
//! │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘               │
//! │         │                │                │                       │
//! │         └────────┬───────┴────────┬───────┘                       │
//! │                  │                │                               │
//! │           ┌──────▼──────┐  ┌──────▼──────┐                       │
//! │           │FairScheduler│  │ RateLimiter │                       │
//! │           └──────┬──────┘  └──────┬──────┘                       │
//! │                  │                │                               │
//! │           ┌──────▼────────────────▼──────┐                       │
//! │           │    Exchange Connectors       │                       │
//! │           │  (Shared connection pool)    │                       │
//! │           └──────────────────────────────┘                       │
//! └────────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::Instant;

use chrono::Utc;
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::core::{ExchangeConnector, ExecutionError};
use crate::signal::Signal;
use crate::exchanges::factory::ExchangeFactory;
use smartorderrouter::{ExchangeCredential, DbPool, load_exchange_credentials};

// ============================================================================
// Subscription Tier (aligned with BacktestingEngine/databaseschema)
// ============================================================================

/// Subscription tier for rate limiting and resource allocation
/// Matches BacktestingEngine's `SubscriptionTier` enum exactly
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionTier {
    Explorer,
    Trader,
    Professional,
    Team,
    Enterprise,
}

impl Default for SubscriptionTier {
    fn default() -> Self {
        Self::Explorer
    }
}

impl SubscriptionTier {
    /// Orders per minute limit for this tier
    pub fn orders_per_minute(&self) -> u64 {
        match self {
            Self::Explorer => 10,           // 10 orders/min - paper trading only
            Self::Trader => 100,            // 100 orders/min - basic live
            Self::Professional => 1_000,    // 1000 orders/min - active trading
            Self::Team => 10_000,           // 10K orders/min - team tier
            Self::Enterprise => 100_000,    // 100K orders/min - HFT
        }
    }

    /// Maximum concurrent strategies
    pub fn max_strategies(&self) -> usize {
        match self {
            Self::Explorer => 3,
            Self::Trader => 25,
            Self::Professional => 100,
            Self::Team => 500,
            Self::Enterprise => usize::MAX, // Unlimited
        }
    }

    /// Maximum exchanges (API keys) allowed
    pub fn max_exchanges(&self) -> usize {
        match self {
            Self::Explorer => 1,
            Self::Trader => 1,
            Self::Professional => 3,
            Self::Team => 10,
            Self::Enterprise => usize::MAX,
        }
    }

    /// Fair scheduler weight (higher = more priority)
    pub fn scheduler_weight(&self) -> u32 {
        match self {
            Self::Explorer => 1,
            Self::Trader => 5,
            Self::Professional => 10,
            Self::Team => 25,
            Self::Enterprise => 50,
        }
    }

    /// Target latency SLA in microseconds (0 = best effort)
    pub fn latency_sla_us(&self) -> u64 {
        match self {
            Self::Explorer => 0,          // Best effort, no SLA
            Self::Trader => 50_000,       // 50ms
            Self::Professional => 10_000, // 10ms
            Self::Team => 1_000,          // 1ms
            Self::Enterprise => 100,      // 100μs
        }
    }

    /// Parse from database tier string
    pub fn from_db_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "trader" => Self::Trader,
            "professional" => Self::Professional,
            "team" | "institution" => Self::Team,
            "enterprise" => Self::Enterprise,
            // Backward compat
            "free" => Self::Explorer,
            "pro" => Self::Professional,
            "live" => Self::Team,
            _ => Self::Explorer,
        }
    }
}

impl std::fmt::Display for SubscriptionTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Explorer => write!(f, "explorer"),
            Self::Trader => write!(f, "trader"),
            Self::Professional => write!(f, "professional"),
            Self::Team => write!(f, "team"),
            Self::Enterprise => write!(f, "enterprise"),
        }
    }
}

// ============================================================================
// Tenant Context
// ============================================================================

/// Pending order in a tenant's queue
#[derive(Debug, Clone)]
pub struct PendingOrder {
    pub signal: Signal,
    pub exchange: String,
    pub queued_at: Instant,
    pub priority: u8, // 0 = highest priority
}

/// Per-tenant rate limiter state
#[derive(Debug)]
pub struct TenantRateLimiter {
    /// Sliding window of order timestamps (unix millis)
    order_timestamps: VecDeque<u64>,
    /// Window size in milliseconds (default: 60_000 = 1 minute)
    window_ms: u64,
    /// Maximum orders per window (based on tier)
    limit: u64,
}

impl TenantRateLimiter {
    pub fn new(tier: SubscriptionTier) -> Self {
        Self {
            order_timestamps: VecDeque::with_capacity(1000),
            window_ms: 60_000,
            limit: tier.orders_per_minute(),
        }
    }

    /// Check if an order is allowed and record it
    pub fn check_and_record(&mut self) -> bool {
        let now = Utc::now().timestamp_millis() as u64;
        let window_start = now.saturating_sub(self.window_ms);

        // Remove expired timestamps
        while let Some(&ts) = self.order_timestamps.front() {
            if ts < window_start {
                self.order_timestamps.pop_front();
            } else {
                break;
            }
        }

        // Check limit
        if self.order_timestamps.len() as u64 >= self.limit {
            return false;
        }

        // Record this order
        self.order_timestamps.push_back(now);
        true
    }

    /// Get current usage (orders in window)
    pub fn current_usage(&self) -> u64 {
        self.order_timestamps.len() as u64
    }

    /// Get remaining capacity
    pub fn remaining(&self) -> u64 {
        self.limit.saturating_sub(self.order_timestamps.len() as u64)
    }

    /// Update limit (e.g., when tier changes)
    pub fn update_limit(&mut self, tier: SubscriptionTier) {
        self.limit = tier.orders_per_minute();
    }
}

/// Context for a single tenant including credentials, connectors, and state
pub struct TenantContext {
    /// Tenant identifier
    pub tenant_id: Uuid,
    /// Subscription tier
    pub tier: SubscriptionTier,
    /// Exchange connectors (exchange name -> connector)
    pub connectors: HashMap<String, Box<dyn ExchangeConnector>>,
    /// Pending order queue
    pub order_queue: VecDeque<PendingOrder>,
    /// Rate limiter
    pub rate_limiter: TenantRateLimiter,
    /// Whether this tenant is active
    pub is_active: AtomicBool,
    /// Total orders executed
    pub total_orders: AtomicU64,
    /// Total orders rejected (rate limit, etc.)
    pub rejected_orders: AtomicU64,
    /// Last activity timestamp
    pub last_activity: AtomicU64,
    /// Deployed strategy count
    pub active_strategies: AtomicU64,
}

impl TenantContext {
    /// Create a new tenant context
    pub fn new(tenant_id: Uuid, tier: SubscriptionTier) -> Self {
        Self {
            tenant_id,
            tier,
            connectors: HashMap::new(),
            order_queue: VecDeque::new(),
            rate_limiter: TenantRateLimiter::new(tier),
            is_active: AtomicBool::new(true),
            total_orders: AtomicU64::new(0),
            rejected_orders: AtomicU64::new(0),
            last_activity: AtomicU64::new(Utc::now().timestamp_millis() as u64),
            active_strategies: AtomicU64::new(0),
        }
    }

    /// Add an exchange connector from a credential
    pub async fn add_exchange(&mut self, credential: &ExchangeCredential) -> Result<(), ExecutionError> {
        if self.connectors.len() >= self.tier.max_exchanges() {
            return Err(ExecutionError::Validation(format!(
                "Exchange limit reached for {} tier: {} max",
                self.tier, self.tier.max_exchanges()
            )));
        }

        let connector = ExchangeFactory::create_connector_from_credential(credential).await?;
        self.connectors.insert(credential.exchange.clone(), connector);
        
        log::info!(
            "[TENANT:{}] Added exchange '{}' (tier: {}, total: {})",
            self.tenant_id, credential.exchange, self.tier, self.connectors.len()
        );
        
        Ok(())
    }

    /// Check rate limit and queue order if allowed
    pub fn queue_order(&mut self, signal: Signal, exchange: String, priority: u8) -> Result<(), ExecutionError> {
        if !self.rate_limiter.check_and_record() {
            self.rejected_orders.fetch_add(1, Ordering::Relaxed);
            return Err(ExecutionError::RateLimit(format!(
                "Rate limit exceeded for tenant {}: {}/min limit",
                self.tenant_id, self.tier.orders_per_minute()
            )));
        }

        self.order_queue.push_back(PendingOrder {
            signal,
            exchange,
            queued_at: Instant::now(),
            priority,
        });

        self.last_activity.store(Utc::now().timestamp_millis() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Get next order from queue
    pub fn dequeue_order(&mut self) -> Option<PendingOrder> {
        self.order_queue.pop_front()
    }

    /// Update subscription tier
    pub fn update_tier(&mut self, new_tier: SubscriptionTier) {
        self.tier = new_tier;
        self.rate_limiter.update_limit(new_tier);
        log::info!("[TENANT:{}] Tier updated to {}", self.tenant_id, new_tier);
    }
}

// ============================================================================
// Fair Scheduler
// ============================================================================

/// Fair scheduler for multi-tenant order execution
/// Uses weighted round-robin based on subscription tier
pub struct FairScheduler {
    /// Tenant execution weights (based on tier)
    weights: DashMap<Uuid, u32>,
    /// Current credits per tenant (replenished based on weight)
    credits: DashMap<Uuid, u32>,
    /// Round-robin index for tie-breaking
    round_robin_index: AtomicU64,
}

impl FairScheduler {
    pub fn new() -> Self {
        Self {
            weights: DashMap::new(),
            credits: DashMap::new(),
            round_robin_index: AtomicU64::new(0),
        }
    }

    /// Register a tenant with their tier weight
    pub fn register_tenant(&self, tenant_id: Uuid, tier: SubscriptionTier) {
        let weight = tier.scheduler_weight();
        self.weights.insert(tenant_id, weight);
        self.credits.insert(tenant_id, weight);
    }

    /// Remove a tenant
    pub fn unregister_tenant(&self, tenant_id: &Uuid) {
        self.weights.remove(tenant_id);
        self.credits.remove(tenant_id);
    }

    /// Update tenant's tier weight
    pub fn update_tenant_tier(&self, tenant_id: Uuid, tier: SubscriptionTier) {
        let weight = tier.scheduler_weight();
        self.weights.insert(tenant_id, weight);
    }

    /// Select next tenant to process (weighted fair scheduling)
    /// Returns tenant_id of the tenant with highest credits, or None if all exhausted
    pub fn select_next(&self, tenants_with_orders: &[Uuid]) -> Option<Uuid> {
        if tenants_with_orders.is_empty() {
            return None;
        }

        // Find tenant with highest credits
        let mut best_tenant: Option<Uuid> = None;
        let mut best_credits: u32 = 0;

        for tenant_id in tenants_with_orders {
            if let Some(credits) = self.credits.get(tenant_id) {
                if *credits > best_credits {
                    best_credits = *credits;
                    best_tenant = Some(*tenant_id);
                }
            }
        }

        // If all credits exhausted, replenish and try again
        if best_tenant.is_none() || best_credits == 0 {
            self.replenish_credits();
            // After replenish, select based on weight (highest tier first)
            for tenant_id in tenants_with_orders {
                if let Some(credits) = self.credits.get(tenant_id) {
                    if *credits > best_credits {
                        best_credits = *credits;
                        best_tenant = Some(*tenant_id);
                    }
                }
            }
        }

        // Consume one credit from selected tenant
        if let Some(tenant_id) = best_tenant {
            if let Some(mut credits) = self.credits.get_mut(&tenant_id) {
                *credits = credits.saturating_sub(1);
            }
        }

        best_tenant
    }

    /// Replenish credits based on weights
    fn replenish_credits(&self) {
        for entry in self.weights.iter() {
            let tenant_id = *entry.key();
            let weight = *entry.value();
            self.credits.insert(tenant_id, weight);
        }
    }
}

impl Default for FairScheduler {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Multi-Tenant Execution Handler
// ============================================================================

/// Execution result with timing info
#[derive(Debug, Clone)]
pub struct MultiTenantExecutionResult {
    pub order_id: String,
    pub tenant_id: Uuid,
    pub exchange: String,
    pub status: String,
    pub filled_quantity: f64,
    pub avg_price: f64,
    pub fees: f64,
    pub latency_us: u64,
    pub queue_time_us: u64,
}

/// Multi-tenant execution handler for SaaS trading platform
pub struct MultiTenantExecutionHandler {
    /// Per-tenant contexts (tenant_id -> context)
    tenants: Arc<RwLock<HashMap<Uuid, TenantContext>>>,
    /// Fair scheduler for order processing
    scheduler: Arc<FairScheduler>,
    /// Database pool for loading credentials
    db_pool: Option<Arc<DbPool>>,
    /// Background task shutdown signal
    shutdown: AtomicBool,
    /// Total orders processed
    total_orders_processed: AtomicU64,
    /// Total orders rejected
    total_orders_rejected: AtomicU64,
}

impl MultiTenantExecutionHandler {
    /// Create a new multi-tenant execution handler
    pub fn new() -> Self {
        Self {
            tenants: Arc::new(RwLock::new(HashMap::new())),
            scheduler: Arc::new(FairScheduler::new()),
            db_pool: None,
            shutdown: AtomicBool::new(false),
            total_orders_processed: AtomicU64::new(0),
            total_orders_rejected: AtomicU64::new(0),
        }
    }

    /// Create with database pool for loading credentials
    pub fn with_db_pool(pool: Arc<DbPool>) -> Self {
        let mut handler = Self::new();
        handler.db_pool = Some(pool);
        handler
    }

    /// Register a new tenant
    pub async fn register_tenant(&self, tenant_id: Uuid, tier: SubscriptionTier) -> Result<(), ExecutionError> {
        let context = TenantContext::new(tenant_id, tier);
        
        // Register with scheduler
        self.scheduler.register_tenant(tenant_id, tier);
        
        // Add to tenants map
        let mut tenants = self.tenants.write().await;
        tenants.insert(tenant_id, context);
        
        log::info!("[MULTI-TENANT] Registered tenant {} with tier {}", tenant_id, tier);
        Ok(())
    }

    /// Load tenant credentials from database and initialize connectors
    pub async fn load_tenant_credentials(&self, tenant_id: Uuid) -> Result<usize, ExecutionError> {
        let pool = self.db_pool.as_ref()
            .ok_or_else(|| ExecutionError::Unknown("Database pool not configured".to_string()))?;

        let credentials = load_exchange_credentials(pool)
            .await
            .map_err(|e| ExecutionError::Unknown(format!("Failed to load credentials: {}", e)))?;

        let mut tenants = self.tenants.write().await;
        let context = tenants.get_mut(&tenant_id)
            .ok_or_else(|| ExecutionError::Unknown(format!("Tenant {} not registered", tenant_id)))?;

        let mut loaded = 0;
        for credential in &credentials {
            match context.add_exchange(credential).await {
                Ok(_) => loaded += 1,
                Err(e) => {
                    log::error!(
                        "[MULTI-TENANT] Failed to load exchange '{}' for tenant {}: {}",
                        credential.exchange, tenant_id, e
                    );
                }
            }
        }

        log::info!(
            "[MULTI-TENANT] Loaded {}/{} credentials for tenant {}",
            loaded, credentials.len(), tenant_id
        );

        Ok(loaded)
    }

    /// Unregister a tenant (cleanup)
    pub async fn unregister_tenant(&self, tenant_id: Uuid) {
        self.scheduler.unregister_tenant(&tenant_id);
        
        let mut tenants = self.tenants.write().await;
        tenants.remove(&tenant_id);
        
        log::info!("[MULTI-TENANT] Unregistered tenant {}", tenant_id);
    }

    /// Update tenant's subscription tier
    pub async fn update_tenant_tier(&self, tenant_id: Uuid, new_tier: SubscriptionTier) -> Result<(), ExecutionError> {
        let mut tenants = self.tenants.write().await;
        let context = tenants.get_mut(&tenant_id)
            .ok_or_else(|| ExecutionError::Unknown(format!("Tenant {} not found", tenant_id)))?;

        context.update_tier(new_tier);
        self.scheduler.update_tenant_tier(tenant_id, new_tier);

        Ok(())
    }

    /// Submit an order for a tenant
    pub async fn submit_order(
        &self,
        tenant_id: Uuid,
        signal: Signal,
        exchange: String,
        priority: u8,
    ) -> Result<(), ExecutionError> {
        let mut tenants = self.tenants.write().await;
        let context = tenants.get_mut(&tenant_id)
            .ok_or_else(|| ExecutionError::Unknown(format!("Tenant {} not registered", tenant_id)))?;

        if !context.is_active.load(Ordering::Relaxed) {
            return Err(ExecutionError::Rejected("Tenant is not active".to_string()));
        }

        context.queue_order(signal, exchange, priority)?;
        Ok(())
    }

    /// Process orders using fair scheduling (call from background task)
    pub async fn process_orders(&self, max_batch: usize) -> Vec<MultiTenantExecutionResult> {
        let mut results = Vec::new();

        for _ in 0..max_batch {
            // Find tenants with pending orders
            let tenants_with_orders: Vec<Uuid> = {
                let tenants = self.tenants.read().await;
                tenants.iter()
                    .filter(|(_, ctx)| !ctx.order_queue.is_empty())
                    .map(|(id, _)| *id)
                    .collect()
            };

            if tenants_with_orders.is_empty() {
                break;
            }

            // Select next tenant using fair scheduler
            let selected_tenant = match self.scheduler.select_next(&tenants_with_orders) {
                Some(t) => t,
                None => break,
            };

            // Dequeue and execute order
            let order = {
                let mut tenants = self.tenants.write().await;
                if let Some(ctx) = tenants.get_mut(&selected_tenant) {
                    ctx.dequeue_order()
                } else {
                    None
                }
            };

            if let Some(pending) = order {
                let queue_time = pending.queued_at.elapsed();
                
                // Execute the order
                match self.execute_order_internal(&selected_tenant, &pending).await {
                    Ok(mut result) => {
                        result.queue_time_us = queue_time.as_micros() as u64;
                        self.total_orders_processed.fetch_add(1, Ordering::Relaxed);
                        results.push(result);
                    }
                    Err(e) => {
                        self.total_orders_rejected.fetch_add(1, Ordering::Relaxed);
                        log::error!(
                            "[MULTI-TENANT] Order execution failed for tenant {}: {}",
                            selected_tenant, e
                        );
                    }
                }
            }
        }

        results
    }

    /// Internal order execution
    async fn execute_order_internal(
        &self,
        tenant_id: &Uuid,
        pending: &PendingOrder,
    ) -> Result<MultiTenantExecutionResult, ExecutionError> {
        let start = Instant::now();

        let tenants = self.tenants.read().await;
        let context = tenants.get(tenant_id)
            .ok_or_else(|| ExecutionError::Unknown(format!("Tenant {} not found", tenant_id)))?;

        let connector = context.connectors.get(&pending.exchange)
            .ok_or_else(|| ExecutionError::Unknown(format!(
                "Exchange '{}' not configured for tenant {}",
                pending.exchange, tenant_id
            )))?;

        // Execute via connector
        let exec_result = connector.execute_order(&pending.signal).await?;

        // Update metrics
        context.total_orders.fetch_add(1, Ordering::Relaxed);

        let latency = start.elapsed();

        Ok(MultiTenantExecutionResult {
            order_id: exec_result.order_id,
            tenant_id: *tenant_id,
            exchange: pending.exchange.clone(),
            status: format!("{:?}", exec_result.status),
            filled_quantity: exec_result.filled_quantity,
            avg_price: exec_result.avg_fill_price,
            fees: exec_result.total_fees,
            latency_us: latency.as_micros() as u64,
            queue_time_us: 0, // Set by caller
        })
    }

    /// Get statistics for a tenant
    pub async fn get_tenant_stats(&self, tenant_id: Uuid) -> Option<TenantStats> {
        let tenants = self.tenants.read().await;
        tenants.get(&tenant_id).map(|ctx| TenantStats {
            tenant_id,
            tier: ctx.tier,
            total_orders: ctx.total_orders.load(Ordering::Relaxed),
            rejected_orders: ctx.rejected_orders.load(Ordering::Relaxed),
            queued_orders: ctx.order_queue.len(),
            connected_exchanges: ctx.connectors.len(),
            rate_limit_remaining: ctx.rate_limiter.remaining(),
            rate_limit_max: ctx.tier.orders_per_minute(),
            is_active: ctx.is_active.load(Ordering::Relaxed),
        })
    }

    /// Get all tenant stats
    pub async fn get_all_tenant_stats(&self) -> Vec<TenantStats> {
        let tenants = self.tenants.read().await;
        tenants.iter().map(|(id, ctx)| TenantStats {
            tenant_id: *id,
            tier: ctx.tier,
            total_orders: ctx.total_orders.load(Ordering::Relaxed),
            rejected_orders: ctx.rejected_orders.load(Ordering::Relaxed),
            queued_orders: ctx.order_queue.len(),
            connected_exchanges: ctx.connectors.len(),
            rate_limit_remaining: ctx.rate_limiter.remaining(),
            rate_limit_max: ctx.tier.orders_per_minute(),
            is_active: ctx.is_active.load(Ordering::Relaxed),
        }).collect()
    }

    /// Get global stats
    pub fn get_global_stats(&self) -> GlobalStats {
        GlobalStats {
            total_orders_processed: self.total_orders_processed.load(Ordering::Relaxed),
            total_orders_rejected: self.total_orders_rejected.load(Ordering::Relaxed),
        }
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Check if shutdown signaled
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}

impl Default for MultiTenantExecutionHandler {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Statistics Types
// ============================================================================

/// Statistics for a single tenant
#[derive(Debug, Clone, Serialize)]
pub struct TenantStats {
    pub tenant_id: Uuid,
    pub tier: SubscriptionTier,
    pub total_orders: u64,
    pub rejected_orders: u64,
    pub queued_orders: usize,
    pub connected_exchanges: usize,
    pub rate_limit_remaining: u64,
    pub rate_limit_max: u64,
    pub is_active: bool,
}

/// Global execution statistics
#[derive(Debug, Clone, Serialize)]
pub struct GlobalStats {
    pub total_orders_processed: u64,
    pub total_orders_rejected: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_tier_limits() {
        assert_eq!(SubscriptionTier::Explorer.orders_per_minute(), 10);
        assert_eq!(SubscriptionTier::Professional.orders_per_minute(), 1000);
        assert_eq!(SubscriptionTier::Enterprise.max_exchanges(), usize::MAX);
    }

    #[test]
    fn test_rate_limiter() {
        let mut limiter = TenantRateLimiter::new(SubscriptionTier::Explorer);
        
        // Should allow up to 10 orders
        for _ in 0..10 {
            assert!(limiter.check_and_record());
        }
        
        // 11th should be rejected
        assert!(!limiter.check_and_record());
    }

    #[test]
    fn test_fair_scheduler_weights() {
        let scheduler = FairScheduler::new();
        let t1 = Uuid::new_v4(); // Explorer
        let t2 = Uuid::new_v4(); // Enterprise
        
        scheduler.register_tenant(t1, SubscriptionTier::Explorer);
        scheduler.register_tenant(t2, SubscriptionTier::Enterprise);
        
        // Enterprise should be selected first (50 credits vs 1)
        let tenants = vec![t1, t2];
        let selected = scheduler.select_next(&tenants);
        assert_eq!(selected, Some(t2));
    }

    #[tokio::test]
    async fn test_tenant_registration() {
        let handler = MultiTenantExecutionHandler::new();
        let tenant_id = Uuid::new_v4();
        
        handler.register_tenant(tenant_id, SubscriptionTier::Professional).await.unwrap();
        
        let stats = handler.get_tenant_stats(tenant_id).await;
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().tier, SubscriptionTier::Professional);
    }
}
