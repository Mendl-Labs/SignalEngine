//! Database integration for SmartOrderRouter
//!
//! This module provides real PostgreSQL persistence for order routing decisions,
//! enabling audit trails, analytics, and crash recovery.

use std::sync::Arc;
use anyhow::{Result, Context};
use bigdecimal::BigDecimal;
use chrono::Utc;
use uuid::Uuid;
use tokio::sync::RwLock;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::deadpool;

use databaseschema::{
    models::strategy_order::{
        NewStrategyOrder, NewStrategyOrderFill, NewStrategyOrderStateChange,
        OrderStatus, OrderSide as DbOrderSide, OrderType as DbOrderType,
        TimeInForce as DbTimeInForce, ExecutionUrgency as DbExecutionUrgency,
    },
    ops::strategy_order_ops::{
        StrategyOrderOps, StrategyOrderFillOps, StrategyOrderStateChangeOps,
        StrategyOrderWorkflow,
    },
};

use crate::{SmartOrderRoute, ChildOrder, OrderSide, OrderType, TimeInForce, ExecutionUrgency, RouteStatus};

/// Type alias for the database pool
pub type DbPool = deadpool::Pool<AsyncPgConnection>;

/// Database persistence layer for SmartOrderRouter
pub struct OrderDatabasePersistence {
    pool: Arc<DbPool>,
    /// Cache of route_id -> database order_id mappings
    route_to_db_id: Arc<RwLock<std::collections::HashMap<String, Uuid>>>,
}

impl OrderDatabasePersistence {
    /// Create a new database persistence layer
    pub fn new(pool: Arc<DbPool>) -> Self {
        Self {
            pool,
            route_to_db_id: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Persist a new SmartOrderRoute to the database
    /// 
    /// This should be called when a new route is created, before execution begins.
    /// Returns the database UUID for the order.
    pub async fn persist_route(&self, route: &SmartOrderRoute, strategy_name: &str) -> Result<Uuid> {
        let mut conn = self.pool.get().await
            .context("Failed to get database connection")?;

        // Convert router types to database types
        let db_order = NewStrategyOrder {
            signal_id: route.created_at as i64, // Use timestamp as signal_id
            strategy_instance_id: None,
            parent_order_id: None,
            unique_id: route.id.clone(),
            symbol: route.symbol.clone(),
            exchange: "smart_router".to_string(), // Parent order is from router
            side: convert_order_side(route.side),
            order_type: DbOrderType::Implementation, // Smart routing
            time_in_force: Some(DbTimeInForce::Gtc),
            original_quantity: BigDecimal::try_from(route.total_quantity)
                .unwrap_or_else(|_| BigDecimal::from(0)),
            remaining_quantity: BigDecimal::try_from(route.total_quantity - route.total_filled_quantity)
                .unwrap_or_else(|_| BigDecimal::from(0)),
            price: route.benchmark_price.map(|p| BigDecimal::try_from(p).ok()).flatten(),
            stop_price: None,
            status: convert_route_status(route.status),
            urgency: Some(convert_urgency(route.urgency)),
            strategy_name: strategy_name.to_string(),
            strategy_version: Some("1.0".to_string()),
            signal_confidence: None,
            signal_flags: None,
            signal_timestamp: Utc::now(),
            created_by: Some("SmartOrderRouter".to_string()),
        };

        let (order, _state_change) = StrategyOrderWorkflow::create_order_with_state(
            &mut conn,
            db_order,
            Some("SmartOrderRouter".to_string()),
        ).await.context("Failed to persist route to database")?;

        // Cache the mapping
        {
            let mut cache = self.route_to_db_id.write().await;
            cache.insert(route.id.clone(), order.id);
        }

        Ok(order.id)
    }

    /// Persist a child order (slice) to the database
    ///
    /// Child orders are linked to their parent route via parent_order_id
    pub async fn persist_child_order(
        &self,
        child: &ChildOrder,
        parent_route_id: &str,
        strategy_name: &str,
    ) -> Result<Uuid> {
        let mut conn = self.pool.get().await
            .context("Failed to get database connection")?;

        // Get parent order DB id
        let parent_db_id = {
            let cache = self.route_to_db_id.read().await;
            cache.get(parent_route_id).copied()
        };

        let db_order = NewStrategyOrder {
            signal_id: child.created_at as i64,
            strategy_instance_id: None,
            parent_order_id: parent_db_id,
            unique_id: child.id.clone(),
            symbol: child.symbol.clone(),
            exchange: child.exchange.clone(),
            side: convert_order_side(child.side),
            order_type: convert_order_type(child.order_type),
            time_in_force: Some(convert_time_in_force(child.time_in_force)),
            original_quantity: BigDecimal::try_from(child.quantity)
                .unwrap_or_else(|_| BigDecimal::from(0)),
            remaining_quantity: BigDecimal::try_from(child.remaining_quantity())
                .unwrap_or_else(|_| BigDecimal::from(0)),
            price: child.price.map(|p| BigDecimal::try_from(p).ok()).flatten(),
            stop_price: None,
            status: convert_route_status(child.status),
            urgency: None,
            strategy_name: strategy_name.to_string(),
            strategy_version: Some("1.0".to_string()),
            signal_confidence: None,
            signal_flags: None,
            signal_timestamp: Utc::now(),
            created_by: Some("SmartOrderRouter".to_string()),
        };

        let (order, _) = StrategyOrderWorkflow::create_order_with_state(
            &mut conn,
            db_order,
            Some("SmartOrderRouter".to_string()),
        ).await.context("Failed to persist child order")?;

        Ok(order.id)
    }

    /// Record a fill for an order
    pub async fn record_fill(
        &self,
        order_unique_id: &str,
        fill_id: &str,
        quantity: f64,
        price: f64,
        fees: f64,
    ) -> Result<()> {
        let mut conn = self.pool.get().await
            .context("Failed to get database connection")?;

        // Look up the order by unique_id
        let order = StrategyOrderOps::get_order_by_unique_id(&mut conn, order_unique_id.to_string())
            .await
            .context("Failed to lookup order")?
            .ok_or_else(|| anyhow::anyhow!("Order not found: {}", order_unique_id))?;

        let fill = NewStrategyOrderFill {
            order_id: order.id,
            fill_id: fill_id.to_string(),
            trade_id: None,
            quantity: BigDecimal::try_from(quantity).unwrap_or_else(|_| BigDecimal::from(0)),
            price: BigDecimal::try_from(price).unwrap_or_else(|_| BigDecimal::from(0)),
            fees: Some(BigDecimal::try_from(fees).unwrap_or_else(|_| BigDecimal::from(0))),
            fee_currency: Some("USD".to_string()),
            bid_price: None,
            ask_price: None,
            mid_price: None,
            spread_bps: None,
            is_maker: None,
            liquidity_flag: None,
            fill_timestamp: Utc::now(),
        };

        StrategyOrderFillOps::create_fill(&mut conn, fill)
            .await
            .context("Failed to record fill")?;

        Ok(())
    }

    /// Update order status with state change tracking
    pub async fn update_order_status(
        &self,
        order_unique_id: &str,
        new_status: RouteStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.pool.get().await
            .context("Failed to get database connection")?;

        // Look up the order
        let order = StrategyOrderOps::get_order_by_unique_id(&mut conn, order_unique_id.to_string())
            .await
            .context("Failed to lookup order")?
            .ok_or_else(|| anyhow::anyhow!("Order not found: {}", order_unique_id))?;

        let db_status = convert_route_status(new_status);

        // Record state change
        let state_change = NewStrategyOrderStateChange {
            order_id: order.id,
            previous_status: Some(order.status.clone()),
            new_status: db_status.clone(),
            previous_quantity: None,
            new_quantity: None,
            change_reason: reason.map(|s| s.to_string()),
            triggered_by: Some("SmartOrderRouter".to_string()),
            exchange_message: None,
            state_data: None,
            changed_by: Some("SmartOrderRouter".to_string()),
        };

        StrategyOrderStateChangeOps::create_state_change(&mut conn, state_change)
            .await
            .context("Failed to record state change")?;

        // Update the order status
        StrategyOrderOps::update_order_status(&mut conn, order.id, db_status)
            .await
            .context("Failed to update order status")?;

        Ok(())
    }

    /// Load pending orders from database (for crash recovery)
    pub async fn load_pending_orders(&self) -> Result<Vec<(String, String, f64, f64)>> {
        let mut conn = self.pool.get().await
            .context("Failed to get database connection")?;

        let pending_orders = StrategyOrderOps::get_orders_by_status(
            &mut conn,
            OrderStatus::Pending,
            Some(100),
        ).await.context("Failed to load pending orders")?;

        let submitted_orders = StrategyOrderOps::get_orders_by_status(
            &mut conn,
            OrderStatus::Submitted,
            Some(100),
        ).await.context("Failed to load submitted orders")?;

        let mut results = Vec::new();
        
        for order in pending_orders.into_iter().chain(submitted_orders) {
            let remaining = order.remaining_quantity.to_string().parse::<f64>().unwrap_or(0.0);
            let filled = order.filled_quantity
                .as_ref()
                .and_then(|f| f.to_string().parse::<f64>().ok())
                .unwrap_or(0.0);
            
            results.push((
                order.unique_id,
                order.symbol,
                remaining,
                filled,
            ));
        }

        Ok(results)
    }
}

// Type conversion helpers
fn convert_order_side(side: OrderSide) -> DbOrderSide {
    match side {
        OrderSide::Buy => DbOrderSide::Buy,
        OrderSide::Sell => DbOrderSide::Sell,
    }
}

fn convert_order_type(order_type: OrderType) -> DbOrderType {
    match order_type {
        OrderType::Market => DbOrderType::Market,
        OrderType::Limit => DbOrderType::Limit,
        OrderType::StopLimit => DbOrderType::StopLimit,
        OrderType::Iceberg => DbOrderType::Iceberg,
        OrderType::TWAP => DbOrderType::Twap,
        OrderType::VWAP => DbOrderType::Vwap,
        OrderType::Implementation => DbOrderType::Implementation,
    }
}

fn convert_time_in_force(tif: TimeInForce) -> DbTimeInForce {
    match tif {
        TimeInForce::IOC => DbTimeInForce::Ioc,
        TimeInForce::FOK => DbTimeInForce::Fok,
        TimeInForce::GTC => DbTimeInForce::Gtc,
        TimeInForce::DAY => DbTimeInForce::Day,
        TimeInForce::GTD => DbTimeInForce::Gtd,
    }
}

fn convert_urgency(urgency: ExecutionUrgency) -> DbExecutionUrgency {
    match urgency {
        ExecutionUrgency::Low => DbExecutionUrgency::Low,
        ExecutionUrgency::Medium => DbExecutionUrgency::Medium,
        ExecutionUrgency::High => DbExecutionUrgency::High,
        ExecutionUrgency::Critical => DbExecutionUrgency::Critical,
    }
}

fn convert_route_status(status: RouteStatus) -> OrderStatus {
    match status {
        RouteStatus::Pending => OrderStatus::Pending,
        RouteStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        RouteStatus::Filled => OrderStatus::Filled,
        RouteStatus::Cancelled => OrderStatus::Cancelled,
        RouteStatus::Rejected => OrderStatus::Rejected,
        RouteStatus::Expired => OrderStatus::Expired,
        RouteStatus::Failed => OrderStatus::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_side_conversion() {
        assert!(matches!(convert_order_side(OrderSide::Buy), DbOrderSide::Buy));
        assert!(matches!(convert_order_side(OrderSide::Sell), DbOrderSide::Sell));
    }

    #[test]
    fn test_status_conversion() {
        assert!(matches!(convert_route_status(RouteStatus::Pending), OrderStatus::Pending));
        assert!(matches!(convert_route_status(RouteStatus::Filled), OrderStatus::Filled));
        assert!(matches!(convert_route_status(RouteStatus::Cancelled), OrderStatus::Cancelled));
    }
}
