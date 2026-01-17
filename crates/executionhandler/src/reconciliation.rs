//! Order and Position Reconciliation Module
//!
//! Provides startup reconciliation to sync local state with exchange state.
//! Critical for crash recovery and ensuring position accuracy.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};

use crate::core::types::ExecutionError;
use crate::risk_controls::{KILL_SWITCH, KillReason, PositionLimitChecker};

/// Exchange position from API query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangePosition {
    pub symbol: String,
    pub exchange: String,
    pub quantity: f64,
    pub avg_entry_price: f64,
    pub unrealized_pnl: f64,
    pub liquidation_price: Option<f64>,
    pub leverage: Option<f64>,
    pub margin_used: Option<f64>,
    pub last_updated: u64,
}

/// Exchange open order from API query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeOpenOrder {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub symbol: String,
    pub exchange: String,
    pub side: String,      // "buy" or "sell"
    pub order_type: String, // "market", "limit", etc.
    pub quantity: f64,
    pub filled_quantity: f64,
    pub price: Option<f64>,
    pub status: String,
    pub created_at: u64,
}

/// Reconciliation result
#[derive(Debug, Clone, Serialize)]
pub struct ReconciliationResult {
    pub exchange: String,
    pub success: bool,
    pub positions_synced: usize,
    pub open_orders_found: usize,
    pub orphaned_orders_cancelled: usize,
    pub position_mismatches: Vec<PositionMismatch>,
    pub errors: Vec<String>,
    pub timestamp: u64,
}

/// Position mismatch between local and exchange
#[derive(Debug, Clone, Serialize)]
pub struct PositionMismatch {
    pub symbol: String,
    pub local_quantity: f64,
    pub exchange_quantity: f64,
    pub difference: f64,
    pub resolution: MismatchResolution,
}

/// How a mismatch was resolved
#[derive(Debug, Clone, Serialize)]
pub enum MismatchResolution {
    /// Used exchange value (trusted source)
    TrustExchange,
    /// Triggered kill switch due to large discrepancy
    KillSwitchTriggered,
    /// Manual review required
    ManualReviewRequired,
}

/// Trait for exchange-specific reconciliation
#[async_trait::async_trait]
pub trait ExchangeReconciliation: Send + Sync {
    /// Get exchange name
    fn exchange_name(&self) -> &str;

    /// Query all open positions from exchange
    async fn get_positions(&self) -> Result<Vec<ExchangePosition>, ExecutionError>;

    /// Query all open orders from exchange
    async fn get_open_orders(&self) -> Result<Vec<ExchangeOpenOrder>, ExecutionError>;

    /// Cancel an order by ID
    async fn cancel_order(&self, order_id: &str) -> Result<(), ExecutionError>;

    /// Get account balance
    async fn get_balance(&self) -> Result<f64, ExecutionError>;
}

/// Reconciliation engine
pub struct ReconciliationEngine {
    /// Local position state to reconcile against
    position_tracker: Arc<PositionLimitChecker>,
    /// Known local orders: order_id -> (symbol, quantity, status)
    local_orders: Arc<RwLock<HashMap<String, LocalOrderState>>>,
    /// Reconciliation history
    history: Arc<RwLock<Vec<ReconciliationResult>>>,
    /// Maximum position mismatch allowed before kill switch (percentage)
    max_mismatch_pct: f64,
}

#[derive(Debug, Clone)]
pub struct LocalOrderState {
    pub symbol: String,
    pub exchange: String,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub side: String,
    pub status: String,
}

impl ReconciliationEngine {
    pub fn new(position_tracker: Arc<PositionLimitChecker>) -> Self {
        Self {
            position_tracker,
            local_orders: Arc::new(RwLock::new(HashMap::new())),
            history: Arc::new(RwLock::new(Vec::new())),
            max_mismatch_pct: 0.01, // 1% mismatch triggers review
        }
    }

    /// Set the maximum position mismatch percentage
    pub fn set_max_mismatch_pct(&mut self, pct: f64) {
        self.max_mismatch_pct = pct;
    }

    /// Register a local order for tracking
    pub async fn register_order(&self, order_id: String, state: LocalOrderState) {
        let mut orders = self.local_orders.write().await;
        orders.insert(order_id, state);
    }

    /// Update local order state
    pub async fn update_order(&self, order_id: &str, filled_qty: f64, status: &str) {
        let mut orders = self.local_orders.write().await;
        if let Some(order) = orders.get_mut(order_id) {
            order.filled_quantity = filled_qty;
            order.status = status.to_string();
        }
    }

    /// Remove completed order from tracking
    pub async fn remove_order(&self, order_id: &str) {
        let mut orders = self.local_orders.write().await;
        orders.remove(order_id);
    }

    /// Perform full reconciliation with an exchange
    pub async fn reconcile<E: ExchangeReconciliation>(
        &self,
        exchange: &E,
    ) -> Result<ReconciliationResult> {
        let exchange_name = exchange.exchange_name().to_string();
        let timestamp = crate::optimizations::timestamp::nano_timestamp() as u64;
        
        log::info!("Starting reconciliation for exchange: {}", exchange_name);
        
        let mut result = ReconciliationResult {
            exchange: exchange_name.clone(),
            success: true,
            positions_synced: 0,
            open_orders_found: 0,
            orphaned_orders_cancelled: 0,
            position_mismatches: Vec::new(),
            errors: Vec::new(),
            timestamp,
        };

        // 1. Get positions from exchange
        let exchange_positions = match exchange.get_positions().await {
            Ok(positions) => positions,
            Err(e) => {
                result.success = false;
                result.errors.push(format!("Failed to get positions: {:?}", e));
                log::error!("Reconciliation failed - cannot get positions: {:?}", e);
                return Ok(result);
            }
        };

        // 2. Get open orders from exchange
        let exchange_orders = match exchange.get_open_orders().await {
            Ok(orders) => orders,
            Err(e) => {
                result.success = false;
                result.errors.push(format!("Failed to get open orders: {:?}", e));
                log::error!("Reconciliation failed - cannot get open orders: {:?}", e);
                return Ok(result);
            }
        };

        result.open_orders_found = exchange_orders.len();

        // 3. Reconcile positions
        let local_positions = self.position_tracker.get_positions().await;
        let mut new_positions: HashMap<(String, String), (f64, f64)> = HashMap::new();

        for ex_pos in &exchange_positions {
            let key = (ex_pos.symbol.clone(), ex_pos.exchange.clone());
            let (local_qty, _) = local_positions.get(&key).copied().unwrap_or((0.0, 0.0));
            
            let diff = (ex_pos.quantity - local_qty).abs();
            let diff_pct = if local_qty.abs() > f64::EPSILON {
                diff / local_qty.abs()
            } else if ex_pos.quantity.abs() > f64::EPSILON {
                1.0 // 100% mismatch if local is 0 but exchange is not
            } else {
                0.0
            };

            // Check for significant mismatch
            if diff > f64::EPSILON {
                let resolution = if diff_pct > 0.10 {
                    // >10% mismatch - trigger kill switch
                    KILL_SWITCH.trigger(KillReason::Reconciliation);
                    result.success = false;
                    log::error!(
                        "🚨 CRITICAL POSITION MISMATCH in {}: local={}, exchange={}, diff={}",
                        ex_pos.symbol, local_qty, ex_pos.quantity, diff
                    );
                    MismatchResolution::KillSwitchTriggered
                } else if diff_pct > self.max_mismatch_pct {
                    log::warn!(
                        "Position mismatch in {}: local={}, exchange={} - manual review required",
                        ex_pos.symbol, local_qty, ex_pos.quantity
                    );
                    MismatchResolution::ManualReviewRequired
                } else {
                    MismatchResolution::TrustExchange
                };

                result.position_mismatches.push(PositionMismatch {
                    symbol: ex_pos.symbol.clone(),
                    local_quantity: local_qty,
                    exchange_quantity: ex_pos.quantity,
                    difference: diff,
                    resolution: resolution.clone(),
                });
            }

            // Always trust exchange position (source of truth)
            let value = ex_pos.quantity.abs() * ex_pos.avg_entry_price;
            new_positions.insert(key, (ex_pos.quantity, value));
            result.positions_synced += 1;
        }

        // Update local positions to match exchange
        self.position_tracker.set_positions(new_positions).await;

        // 4. Find and cancel orphaned orders (in exchange but not in local)
        let local_orders = self.local_orders.read().await;
        for ex_order in &exchange_orders {
            let order_id = ex_order.client_order_id.as_ref()
                .unwrap_or(&ex_order.order_id);
            
            if !local_orders.contains_key(order_id) {
                log::warn!(
                    "Orphaned order found: {} {} {} @ {:?} - cancelling",
                    ex_order.side, ex_order.quantity, ex_order.symbol, ex_order.price
                );
                
                match exchange.cancel_order(&ex_order.order_id).await {
                    Ok(_) => {
                        result.orphaned_orders_cancelled += 1;
                        log::info!("Cancelled orphaned order: {}", ex_order.order_id);
                    }
                    Err(e) => {
                        result.errors.push(format!(
                            "Failed to cancel orphaned order {}: {:?}",
                            ex_order.order_id, e
                        ));
                    }
                }
            }
        }

        // 5. Store result in history
        {
            let mut history = self.history.write().await;
            history.push(result.clone());
            // Keep last 100 reconciliations
            if history.len() > 100 {
                history.remove(0);
            }
        }

        if result.success {
            log::info!(
                "Reconciliation complete for {}: {} positions synced, {} orders found, {} orphans cancelled",
                exchange_name, result.positions_synced, result.open_orders_found, result.orphaned_orders_cancelled
            );
        } else {
            log::error!("Reconciliation FAILED for {}: {:?}", exchange_name, result.errors);
        }

        Ok(result)
    }

    /// Get reconciliation history
    pub async fn get_history(&self) -> Vec<ReconciliationResult> {
        self.history.read().await.clone()
    }

    /// Quick position check (without full reconciliation)
    pub async fn quick_position_check<E: ExchangeReconciliation>(
        &self,
        exchange: &E,
        symbol: &str,
    ) -> Result<Option<PositionMismatch>> {
        let exchange_positions = exchange.get_positions().await
            .context("Failed to get exchange positions")?;
        
        let ex_pos = exchange_positions.iter()
            .find(|p| p.symbol == symbol);
        
        let local_positions = self.position_tracker.get_positions().await;
        let key = (symbol.to_string(), exchange.exchange_name().to_string());
        let (local_qty, _) = local_positions.get(&key).copied().unwrap_or((0.0, 0.0));

        if let Some(ex_pos) = ex_pos {
            let diff = (ex_pos.quantity - local_qty).abs();
            if diff > f64::EPSILON {
                return Ok(Some(PositionMismatch {
                    symbol: symbol.to_string(),
                    local_quantity: local_qty,
                    exchange_quantity: ex_pos.quantity,
                    difference: diff,
                    resolution: MismatchResolution::ManualReviewRequired,
                }));
            }
        } else if local_qty.abs() > f64::EPSILON {
            // Local has position but exchange doesn't
            return Ok(Some(PositionMismatch {
                symbol: symbol.to_string(),
                local_quantity: local_qty,
                exchange_quantity: 0.0,
                difference: local_qty.abs(),
                resolution: MismatchResolution::ManualReviewRequired,
            }));
        }

        Ok(None)
    }
}

/// Startup reconciliation helper
pub async fn startup_reconciliation<E: ExchangeReconciliation>(
    engine: &ReconciliationEngine,
    exchanges: &[E],
) -> Result<Vec<ReconciliationResult>> {
    log::info!("🔄 Starting startup reconciliation for {} exchanges...", exchanges.len());
    
    let mut results = Vec::new();
    
    for exchange in exchanges {
        let result = engine.reconcile(exchange).await?;
        
        if !result.success {
            log::error!(
                "❌ Reconciliation failed for {} - trading may be unsafe!",
                exchange.exchange_name()
            );
        }
        
        results.push(result);
    }

    // Check if any reconciliation triggered kill switch
    if KILL_SWITCH.is_triggered() {
        log::error!("🚨 KILL SWITCH TRIGGERED DURING RECONCILIATION - Manual intervention required!");
    } else {
        log::info!("✅ Startup reconciliation complete - {} exchanges synced", exchanges.len());
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk_controls::PositionLimits;

    struct MockExchange {
        positions: Vec<ExchangePosition>,
        orders: Vec<ExchangeOpenOrder>,
    }

    #[async_trait::async_trait]
    impl ExchangeReconciliation for MockExchange {
        fn exchange_name(&self) -> &str {
            "mock"
        }

        async fn get_positions(&self) -> Result<Vec<ExchangePosition>, ExecutionError> {
            Ok(self.positions.clone())
        }

        async fn get_open_orders(&self) -> Result<Vec<ExchangeOpenOrder>, ExecutionError> {
            Ok(self.orders.clone())
        }

        async fn cancel_order(&self, _order_id: &str) -> Result<(), ExecutionError> {
            Ok(())
        }

        async fn get_balance(&self) -> Result<f64, ExecutionError> {
            Ok(100_000.0)
        }
    }

    #[tokio::test]
    async fn test_reconciliation_no_mismatch() {
        // Reset kill switch
        KILL_SWITCH.reset();
        
        let position_tracker = Arc::new(PositionLimitChecker::new(PositionLimits::default()));
        let engine = ReconciliationEngine::new(position_tracker);

        let exchange = MockExchange {
            positions: vec![],
            orders: vec![],
        };

        let result = engine.reconcile(&exchange).await.unwrap();
        assert!(result.success);
        assert_eq!(result.position_mismatches.len(), 0);
    }
}
