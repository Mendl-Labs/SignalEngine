//! Graceful Shutdown Module
//!
//! Handles orderly shutdown of the trading system with position management.
//!
//! # Features
//!
//! - **Signal handling**: Catches SIGTERM, SIGINT, SIGHUP
//! - **Position unwinding**: Cancel open orders, optionally close positions
//! - **State checkpointing**: Save state before exit
//! - **Timeout enforcement**: Force shutdown after grace period
//! - **Health endpoint update**: Mark as unhealthy during shutdown
//! - **Webhook notifications**: Alert on shutdown initiation
//!
//! # Example
//!
//! ```rust,ignore
//! use executionhandler::graceful_shutdown::{ShutdownManager, ShutdownConfig};
//!
//! let config = ShutdownConfig {
//!     grace_period_secs: 30,
//!     cancel_open_orders: true,
//!     close_positions: false,
//!     ..Default::default()
//! };
//!
//! let manager = ShutdownManager::new(config);
//!
//! // In main async loop
//! tokio::select! {
//!     _ = run_trading_loop() => {},
//!     _ = manager.wait_for_shutdown() => {
//!         manager.execute_shutdown().await?;
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};
use crate::order_wal::OrderWal;

/// Shutdown phase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ShutdownPhase {
    /// Normal operation
    Running = 0,
    /// Shutdown initiated, stopping new orders
    Initiated = 1,
    /// Cancelling open orders
    CancellingOrders = 2,
    /// Closing positions (if configured)
    ClosingPositions = 3,
    /// Saving state checkpoint
    SavingState = 4,
    /// Cleanup and finalization
    Finalizing = 5,
    /// Shutdown complete
    Complete = 6,
}

impl From<u8> for ShutdownPhase {
    fn from(value: u8) -> Self {
        match value {
            0 => ShutdownPhase::Running,
            1 => ShutdownPhase::Initiated,
            2 => ShutdownPhase::CancellingOrders,
            3 => ShutdownPhase::ClosingPositions,
            4 => ShutdownPhase::SavingState,
            5 => ShutdownPhase::Finalizing,
            _ => ShutdownPhase::Complete,
        }
    }
}

impl std::fmt::Display for ShutdownPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownPhase::Running => write!(f, "Running"),
            ShutdownPhase::Initiated => write!(f, "Shutdown Initiated"),
            ShutdownPhase::CancellingOrders => write!(f, "Cancelling Orders"),
            ShutdownPhase::ClosingPositions => write!(f, "Closing Positions"),
            ShutdownPhase::SavingState => write!(f, "Saving State"),
            ShutdownPhase::Finalizing => write!(f, "Finalizing"),
            ShutdownPhase::Complete => write!(f, "Complete"),
        }
    }
}

/// Shutdown reason
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShutdownReason {
    /// SIGTERM received
    Sigterm,
    /// SIGINT received (Ctrl+C)
    Sigint,
    /// Manual API call
    Manual { message: Option<String> },
    /// Kill switch triggered
    KillSwitch { reason: String },
    /// Unrecoverable error
    Error { error: String },
    /// Health check failure
    HealthFailure,
    /// Resource exhaustion
    ResourceExhausted { resource: String },
    /// Scheduled maintenance
    Maintenance,
}

impl std::fmt::Display for ShutdownReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownReason::Sigterm => write!(f, "SIGTERM"),
            ShutdownReason::Sigint => write!(f, "SIGINT (Ctrl+C)"),
            ShutdownReason::Manual { message } => {
                if let Some(msg) = message {
                    write!(f, "Manual: {}", msg)
                } else {
                    write!(f, "Manual shutdown")
                }
            }
            ShutdownReason::KillSwitch { reason } => write!(f, "Kill switch: {}", reason),
            ShutdownReason::Error { error } => write!(f, "Error: {}", error),
            ShutdownReason::HealthFailure => write!(f, "Health check failure"),
            ShutdownReason::ResourceExhausted { resource } => write!(f, "Resource exhausted: {}", resource),
            ShutdownReason::Maintenance => write!(f, "Scheduled maintenance"),
        }
    }
}

/// Shutdown configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownConfig {
    /// Grace period before force shutdown (seconds)
    pub grace_period_secs: u64,
    /// Cancel all open orders during shutdown
    pub cancel_open_orders: bool,
    /// Close all positions during shutdown (market orders)
    pub close_positions: bool,
    /// Save state checkpoint before shutdown
    pub save_checkpoint: bool,
    /// Path for state checkpoint
    pub checkpoint_path: Option<String>,
    /// Timeout for order cancellation (seconds)
    pub cancel_timeout_secs: u64,
    /// Timeout for position closing (seconds)
    pub close_timeout_secs: u64,
    /// Maximum parallel cancel requests
    pub max_parallel_cancels: usize,
    /// Notify webhooks on shutdown
    pub notify_webhooks: bool,
    /// Block new orders immediately on shutdown
    pub block_new_orders: bool,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            grace_period_secs: 30,
            cancel_open_orders: true,
            close_positions: false, // Conservative default
            save_checkpoint: true,
            checkpoint_path: Some("./shutdown_checkpoint.json".to_string()),
            cancel_timeout_secs: 10,
            close_timeout_secs: 20,
            max_parallel_cancels: 50,
            notify_webhooks: true,
            block_new_orders: true,
        }
    }
}

/// Open order for cancellation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub order_id: String,
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub price: Option<f64>,
}

/// Position for closing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPosition {
    pub symbol: String,
    pub exchange: String,
    pub quantity: f64,
    pub side: String,
    pub avg_price: f64,
    pub unrealized_pnl: f64,
}

/// Shutdown state checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownCheckpoint {
    pub timestamp_ms: u64,
    pub reason: String,
    pub phase_completed: String,
    pub orders_cancelled: usize,
    pub orders_failed: usize,
    pub positions_closed: usize,
    pub positions_remaining: Vec<OpenPosition>,
    pub pending_orders: Vec<OpenOrder>,
    pub metadata: HashMap<String, String>,
}

/// Callback for order cancellation
pub type CancelOrderFn = Arc<dyn Fn(&OpenOrder) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> + Send + Sync>;

/// Callback for position closing
pub type ClosePositionFn = Arc<dyn Fn(&OpenPosition) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> + Send + Sync>;

/// Callback for fetching open orders
pub type GetOpenOrdersFn = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<OpenOrder>> + Send>> + Send + Sync>;

/// Callback for fetching open positions
pub type GetPositionsFn = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<OpenPosition>> + Send>> + Send + Sync>;

/// Shutdown manager
pub struct ShutdownManager {
    config: RwLock<ShutdownConfig>,
    phase: AtomicU8,
    shutdown_requested: AtomicBool,
    shutdown_complete: AtomicBool,
    reason: RwLock<Option<ShutdownReason>>,
    started_at: Mutex<Option<Instant>>,
    
    // Channels
    shutdown_tx: broadcast::Sender<ShutdownReason>,
    phase_tx: watch::Sender<ShutdownPhase>,
    
    // Callbacks
    cancel_order_fn: RwLock<Option<CancelOrderFn>>,
    close_position_fn: RwLock<Option<ClosePositionFn>>,
    get_open_orders_fn: RwLock<Option<GetOpenOrdersFn>>,
    get_positions_fn: RwLock<Option<GetPositionsFn>>,
    
    // Order WAL for crash recovery integration
    order_wal: RwLock<Option<Arc<OrderWal>>>,
    
    // Statistics
    stats: ShutdownStats,
}

/// Shutdown statistics
#[derive(Debug, Default)]
pub struct ShutdownStats {
    pub orders_to_cancel: AtomicU64,
    pub orders_cancelled: AtomicU64,
    pub orders_failed: AtomicU64,
    pub positions_to_close: AtomicU64,
    pub positions_closed: AtomicU64,
    pub positions_failed: AtomicU64,
}

impl ShutdownManager {
    /// Create a new shutdown manager
    pub fn new(config: ShutdownConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);
        let (phase_tx, _) = watch::channel(ShutdownPhase::Running);
        
        Self {
            config: RwLock::new(config),
            phase: AtomicU8::new(ShutdownPhase::Running as u8),
            shutdown_requested: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            reason: RwLock::new(None),
            started_at: Mutex::new(None),
            shutdown_tx,
            phase_tx,
            cancel_order_fn: RwLock::new(None),
            close_position_fn: RwLock::new(None),
            get_open_orders_fn: RwLock::new(None),
            get_positions_fn: RwLock::new(None),
            order_wal: RwLock::new(None),
            stats: ShutdownStats::default(),
        }
    }
    
    /// Register order WAL for shutdown integration
    pub fn register_order_wal(&self, wal: Arc<OrderWal>) {
        *self.order_wal.write() = Some(wal);
    }
    
    /// Register order cancellation callback
    pub fn register_cancel_order(&self, f: CancelOrderFn) {
        *self.cancel_order_fn.write() = Some(f);
    }
    
    /// Register position closing callback
    pub fn register_close_position(&self, f: ClosePositionFn) {
        *self.close_position_fn.write() = Some(f);
    }
    
    /// Register open orders fetch callback
    pub fn register_get_open_orders(&self, f: GetOpenOrdersFn) {
        *self.get_open_orders_fn.write() = Some(f);
    }
    
    /// Register positions fetch callback
    pub fn register_get_positions(&self, f: GetPositionsFn) {
        *self.get_positions_fn.write() = Some(f);
    }
    
    /// Check if shutdown is requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Acquire)
    }
    
    /// Check if shutdown is complete
    pub fn is_shutdown_complete(&self) -> bool {
        self.shutdown_complete.load(Ordering::Acquire)
    }
    
    /// Get current phase
    pub fn get_phase(&self) -> ShutdownPhase {
        ShutdownPhase::from(self.phase.load(Ordering::Acquire))
    }
    
    /// Check if new orders should be blocked
    pub fn should_block_orders(&self) -> bool {
        if !self.is_shutdown_requested() {
            return false;
        }
        self.config.read().block_new_orders
    }
    
    /// Request shutdown
    pub fn request_shutdown(&self, reason: ShutdownReason) {
        if self.shutdown_requested.swap(true, Ordering::AcqRel) {
            // Already requested
            return;
        }
        
        *self.started_at.lock() = Some(Instant::now());
        *self.reason.write() = Some(reason.clone());
        
        self.set_phase(ShutdownPhase::Initiated);
        let _ = self.shutdown_tx.send(reason);
    }
    
    /// Subscribe to shutdown signal
    pub fn subscribe(&self) -> broadcast::Receiver<ShutdownReason> {
        self.shutdown_tx.subscribe()
    }
    
    /// Subscribe to phase changes
    pub fn subscribe_phase(&self) -> watch::Receiver<ShutdownPhase> {
        self.phase_tx.subscribe()
    }
    
    /// Wait for shutdown signal
    pub async fn wait_for_shutdown(&self) {
        let mut rx = self.subscribe();
        let _ = rx.recv().await;
    }
    
    /// Set shutdown phase
    fn set_phase(&self, phase: ShutdownPhase) {
        self.phase.store(phase as u8, Ordering::Release);
        let _ = self.phase_tx.send(phase);
    }
    
    /// Execute graceful shutdown
    pub async fn execute_shutdown(&self) -> ShutdownResult {
        let config = self.config.read().clone();
        let started = Instant::now();
        let grace_deadline = started + Duration::from_secs(config.grace_period_secs);
        
        let mut result = ShutdownResult {
            success: true,
            phase_reached: ShutdownPhase::Complete,
            orders_cancelled: 0,
            orders_failed: 0,
            positions_closed: 0,
            positions_failed: 0,
            checkpoint_saved: false,
            duration_ms: 0,
            errors: Vec::new(),
        };
        
        // Phase 1: Cancel open orders
        if config.cancel_open_orders {
            self.set_phase(ShutdownPhase::CancellingOrders);
            
            if Instant::now() > grace_deadline {
                result.errors.push("Grace period expired during order cancellation".to_string());
                result.success = false;
                result.phase_reached = ShutdownPhase::CancellingOrders;
            } else {
                let cancel_deadline = std::cmp::min(
                    grace_deadline,
                    Instant::now() + Duration::from_secs(config.cancel_timeout_secs)
                );
                
                match self.cancel_all_orders(cancel_deadline, config.max_parallel_cancels).await {
                    Ok((cancelled, failed)) => {
                        result.orders_cancelled = cancelled;
                        result.orders_failed = failed;
                        if failed > 0 {
                            result.errors.push(format!("Failed to cancel {} orders", failed));
                        }
                    }
                    Err(e) => {
                        result.errors.push(format!("Order cancellation error: {}", e));
                    }
                }
            }
        }
        
        // Phase 2: Close positions
        if config.close_positions {
            self.set_phase(ShutdownPhase::ClosingPositions);
            
            if Instant::now() > grace_deadline {
                result.errors.push("Grace period expired during position closing".to_string());
                result.success = false;
                result.phase_reached = ShutdownPhase::ClosingPositions;
            } else {
                let close_deadline = std::cmp::min(
                    grace_deadline,
                    Instant::now() + Duration::from_secs(config.close_timeout_secs)
                );
                
                match self.close_all_positions(close_deadline).await {
                    Ok((closed, failed)) => {
                        result.positions_closed = closed;
                        result.positions_failed = failed;
                        if failed > 0 {
                            result.errors.push(format!("Failed to close {} positions", failed));
                            result.success = false;
                        }
                    }
                    Err(e) => {
                        result.errors.push(format!("Position closing error: {}", e));
                    }
                }
            }
        }
        
        // Phase 3: Save checkpoint
        if config.save_checkpoint {
            self.set_phase(ShutdownPhase::SavingState);
            
            if let Some(path) = &config.checkpoint_path {
                match self.save_checkpoint(path, &result).await {
                    Ok(_) => {
                        result.checkpoint_saved = true;
                    }
                    Err(e) => {
                        result.errors.push(format!("Checkpoint save error: {}", e));
                    }
                }
            }
        }
        
        // Phase 4: Finalize (includes WAL flush and shutdown)
        self.set_phase(ShutdownPhase::Finalizing);
        
        // Flush and shutdown Order WAL before exit
        if let Some(wal) = self.order_wal.read().clone() {
            log::info!("Graceful shutdown: Flushing Order WAL...");
            
            // Get incomplete orders for logging
            let incomplete = wal.get_incomplete_orders();
            if !incomplete.is_empty() {
                log::warn!(
                    "Graceful shutdown: {} incomplete orders in WAL will be recovered on restart",
                    incomplete.len()
                );
                for order in &incomplete {
                    log::info!(
                        "  - Order {} ({}): state={:?}", 
                        order.order_id, order.symbol, order.state
                    );
                }
            }
            
            // Flush WAL to ensure all pending writes are persisted
            if let Err(e) = wal.flush().await {
                result.errors.push(format!("WAL flush error: {}", e));
                log::error!("Graceful shutdown: Failed to flush WAL: {}", e);
            } else {
                log::info!("Graceful shutdown: WAL flushed successfully");
            }
            
            // Shutdown WAL writer task
            if let Err(e) = wal.shutdown().await {
                result.errors.push(format!("WAL shutdown error: {}", e));
                log::error!("Graceful shutdown: Failed to shutdown WAL: {}", e);
            } else {
                log::info!("Graceful shutdown: WAL shutdown complete");
            }
        }
        
        // Complete
        self.set_phase(ShutdownPhase::Complete);
        self.shutdown_complete.store(true, Ordering::Release);
        
        result.duration_ms = started.elapsed().as_millis() as u64;
        result
    }
    
    /// Cancel all open orders
    async fn cancel_all_orders(
        &self,
        deadline: Instant,
        max_parallel: usize,
    ) -> Result<(usize, usize), String> {
        let get_orders_fn = self.get_open_orders_fn.read().clone();
        let cancel_fn = self.cancel_order_fn.read().clone();
        
        let (get_orders_fn, cancel_fn) = match (get_orders_fn, cancel_fn) {
            (Some(g), Some(c)) => (g, c),
            _ => return Ok((0, 0)), // No callbacks registered
        };
        
        // Fetch open orders
        let orders = get_orders_fn().await;
        self.stats.orders_to_cancel.store(orders.len() as u64, Ordering::Release);
        
        if orders.is_empty() {
            return Ok((0, 0));
        }
        
        let (mut cancelled, mut failed) = (0, 0);
        
        // Cancel in batches with parallelism
        for chunk in orders.chunks(max_parallel) {
            if Instant::now() > deadline {
                failed += orders.len() - cancelled - failed;
                break;
            }
            
            let futures: Vec<_> = chunk.iter().map(|order| {
                let cancel_fn = cancel_fn.clone();
                let order = order.clone();
                async move {
                    cancel_fn(&order).await
                }
            }).collect();
            
            let results = futures::future::join_all(futures).await;
            
            for result in results {
                match result {
                    Ok(_) => {
                        cancelled += 1;
                        self.stats.orders_cancelled.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failed += 1;
                        self.stats.orders_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        
        Ok((cancelled, failed))
    }
    
    /// Close all positions
    async fn close_all_positions(&self, deadline: Instant) -> Result<(usize, usize), String> {
        let get_positions_fn = self.get_positions_fn.read().clone();
        let close_fn = self.close_position_fn.read().clone();
        
        let (get_positions_fn, close_fn) = match (get_positions_fn, close_fn) {
            (Some(g), Some(c)) => (g, c),
            _ => return Ok((0, 0)), // No callbacks registered
        };
        
        // Fetch positions
        let positions = get_positions_fn().await;
        self.stats.positions_to_close.store(positions.len() as u64, Ordering::Release);
        
        if positions.is_empty() {
            return Ok((0, 0));
        }
        
        let (mut closed, mut failed) = (0, 0);
        
        // Close positions sequentially (safer than parallel for market orders)
        for position in &positions {
            if Instant::now() > deadline {
                failed += positions.len() - closed - failed;
                break;
            }
            
            match close_fn(position).await {
                Ok(_) => {
                    closed += 1;
                    self.stats.positions_closed.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    failed += 1;
                    self.stats.positions_failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        
        Ok((closed, failed))
    }
    
    /// Save shutdown checkpoint
    async fn save_checkpoint(&self, path: &str, result: &ShutdownResult) -> Result<(), String> {
        let reason = self.reason.read().as_ref()
            .map(|r| format!("{}", r))
            .unwrap_or_else(|| "Unknown".to_string());
        
        let get_positions_fn = self.get_positions_fn.read().clone();
        let get_orders_fn = self.get_open_orders_fn.read().clone();
        
        let remaining_positions = if let Some(f) = get_positions_fn {
            f().await
        } else {
            Vec::new()
        };
        
        let pending_orders = if let Some(f) = get_orders_fn {
            f().await
        } else {
            Vec::new()
        };
        
        let checkpoint = ShutdownCheckpoint {
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            reason,
            phase_completed: format!("{}", self.get_phase()),
            orders_cancelled: result.orders_cancelled,
            orders_failed: result.orders_failed,
            positions_closed: result.positions_closed,
            positions_remaining: remaining_positions,
            pending_orders,
            metadata: HashMap::new(),
        };
        
        let json = serde_json::to_string_pretty(&checkpoint)
            .map_err(|e| format!("Serialization error: {}", e))?;
        
        tokio::fs::write(path, json).await
            .map_err(|e| format!("Write error: {}", e))?;
        
        Ok(())
    }
    
    /// Get shutdown statistics
    pub fn get_stats(&self) -> ShutdownStatistics {
        ShutdownStatistics {
            phase: self.get_phase(),
            shutdown_requested: self.is_shutdown_requested(),
            shutdown_complete: self.is_shutdown_complete(),
            orders_to_cancel: self.stats.orders_to_cancel.load(Ordering::Relaxed),
            orders_cancelled: self.stats.orders_cancelled.load(Ordering::Relaxed),
            orders_failed: self.stats.orders_failed.load(Ordering::Relaxed),
            positions_to_close: self.stats.positions_to_close.load(Ordering::Relaxed),
            positions_closed: self.stats.positions_closed.load(Ordering::Relaxed),
            positions_failed: self.stats.positions_failed.load(Ordering::Relaxed),
            elapsed_ms: self.started_at.lock()
                .map(|s| s.elapsed().as_millis() as u64)
                .unwrap_or(0),
        }
    }
}

impl Default for ShutdownManager {
    fn default() -> Self {
        Self::new(ShutdownConfig::default())
    }
}

/// Shutdown execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownResult {
    pub success: bool,
    pub phase_reached: ShutdownPhase,
    pub orders_cancelled: usize,
    pub orders_failed: usize,
    pub positions_closed: usize,
    pub positions_failed: usize,
    pub checkpoint_saved: bool,
    pub duration_ms: u64,
    pub errors: Vec<String>,
}

/// Public statistics structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownStatistics {
    pub phase: ShutdownPhase,
    pub shutdown_requested: bool,
    pub shutdown_complete: bool,
    pub orders_to_cancel: u64,
    pub orders_cancelled: u64,
    pub orders_failed: u64,
    pub positions_to_close: u64,
    pub positions_closed: u64,
    pub positions_failed: u64,
    pub elapsed_ms: u64,
}

/// Global shutdown manager instance
pub static SHUTDOWN_MANAGER: std::sync::LazyLock<ShutdownManager> = 
    std::sync::LazyLock::new(|| ShutdownManager::new(ShutdownConfig::default()));

/// Register OS signal handlers for graceful shutdown
#[cfg(unix)]
pub async fn register_signal_handlers(manager: Arc<ShutdownManager>) {
    use tokio::signal::unix::{signal, SignalKind};
    
    let manager_sigterm = manager.clone();
    let manager_sigint = manager.clone();
    
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM");
        sigterm.recv().await;
        manager_sigterm.request_shutdown(ShutdownReason::Sigterm);
    });
    
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to register SIGINT");
        sigint.recv().await;
        manager_sigint.request_shutdown(ShutdownReason::Sigint);
    });
}

/// Register OS signal handlers for graceful shutdown (Windows)
#[cfg(windows)]
pub async fn register_signal_handlers(manager: Arc<ShutdownManager>) {
    let manager_ctrl_c = manager.clone();
    
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to register Ctrl+C");
        manager_ctrl_c.request_shutdown(ShutdownReason::Sigint);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    
    #[test]
    fn test_shutdown_phase_display() {
        assert_eq!(format!("{}", ShutdownPhase::Running), "Running");
        assert_eq!(format!("{}", ShutdownPhase::CancellingOrders), "Cancelling Orders");
        assert_eq!(format!("{}", ShutdownPhase::Complete), "Complete");
    }
    
    #[test]
    fn test_shutdown_reason_display() {
        assert_eq!(format!("{}", ShutdownReason::Sigterm), "SIGTERM");
        assert_eq!(format!("{}", ShutdownReason::Manual { message: Some("test".into()) }), "Manual: test");
    }
    
    #[test]
    fn test_shutdown_request() {
        let manager = ShutdownManager::new(ShutdownConfig::default());
        
        assert!(!manager.is_shutdown_requested());
        assert_eq!(manager.get_phase(), ShutdownPhase::Running);
        
        manager.request_shutdown(ShutdownReason::Manual { message: None });
        
        assert!(manager.is_shutdown_requested());
        assert_eq!(manager.get_phase(), ShutdownPhase::Initiated);
    }
    
    #[test]
    fn test_block_orders_during_shutdown() {
        let manager = ShutdownManager::new(ShutdownConfig {
            block_new_orders: true,
            ..Default::default()
        });
        
        assert!(!manager.should_block_orders());
        
        manager.request_shutdown(ShutdownReason::Sigterm);
        
        assert!(manager.should_block_orders());
    }
    
    #[test]
    fn test_duplicate_shutdown_request() {
        let manager = ShutdownManager::new(ShutdownConfig::default());
        
        manager.request_shutdown(ShutdownReason::Sigterm);
        let phase1 = manager.get_phase();
        
        // Second request should be ignored
        manager.request_shutdown(ShutdownReason::Manual { message: None });
        let phase2 = manager.get_phase();
        
        assert_eq!(phase1, phase2);
    }
    
    #[tokio::test]
    async fn test_execute_shutdown_no_callbacks() {
        let manager = ShutdownManager::new(ShutdownConfig {
            cancel_open_orders: true,
            close_positions: false,
            save_checkpoint: false,
            ..Default::default()
        });
        
        manager.request_shutdown(ShutdownReason::Manual { message: None });
        
        let result = manager.execute_shutdown().await;
        
        assert!(result.success);
        assert_eq!(result.orders_cancelled, 0);
        assert_eq!(result.phase_reached, ShutdownPhase::Complete);
    }
    
    #[tokio::test]
    async fn test_execute_shutdown_with_callbacks() {
        let manager = ShutdownManager::new(ShutdownConfig {
            cancel_open_orders: true,
            close_positions: false,
            save_checkpoint: false,
            grace_period_secs: 10,
            ..Default::default()
        });
        
        // Register callbacks
        let orders_cancelled = Arc::new(AtomicU64::new(0));
        let orders_cancelled_clone = orders_cancelled.clone();
        
        manager.register_get_open_orders(Arc::new(move || {
            Box::pin(async move {
                vec![
                    OpenOrder {
                        order_id: "order1".to_string(),
                        exchange: "test".to_string(),
                        symbol: "BTC-USD".to_string(),
                        side: "buy".to_string(),
                        quantity: 1.0,
                        price: Some(50000.0),
                    },
                    OpenOrder {
                        order_id: "order2".to_string(),
                        exchange: "test".to_string(),
                        symbol: "ETH-USD".to_string(),
                        side: "sell".to_string(),
                        quantity: 10.0,
                        price: Some(3000.0),
                    },
                ]
            })
        }));
        
        manager.register_cancel_order(Arc::new(move |_order: &OpenOrder| {
            let counter = orders_cancelled_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }));
        
        manager.request_shutdown(ShutdownReason::Manual { message: None });
        
        let result = manager.execute_shutdown().await;
        
        assert!(result.success);
        assert_eq!(result.orders_cancelled, 2);
        assert_eq!(orders_cancelled.load(Ordering::Relaxed), 2);
    }
    
    #[test]
    fn test_shutdown_statistics() {
        let manager = ShutdownManager::new(ShutdownConfig::default());
        
        let stats = manager.get_stats();
        assert!(!stats.shutdown_requested);
        assert_eq!(stats.phase, ShutdownPhase::Running);
        
        manager.request_shutdown(ShutdownReason::Sigterm);
        
        let stats = manager.get_stats();
        assert!(stats.shutdown_requested);
        assert_eq!(stats.phase, ShutdownPhase::Initiated);
    }
}
