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
use diesel::prelude::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};

use databaseschema::{
    models::strategy_order::{
        NewStrategyOrder, NewStrategyOrderFill, NewStrategyOrderStateChange,
        OrderStatus, OrderSide as DbOrderSide, OrderType as DbOrderType,
        TimeInForce as DbTimeInForce, ExecutionUrgency as DbExecutionUrgency,
    },
    models::trade_history::NewTradeRecord,
    ops::strategy_order_ops::{
        StrategyOrderOps, StrategyOrderFillOps, StrategyOrderStateChangeOps,
        StrategyOrderWorkflow,
    },
    ops::trade_history_ops,
};

use crate::{SmartOrderRoute, ChildOrder, OrderSide, OrderType, TimeInForce, ExecutionUrgency, RouteStatus};

/// Type alias for the database pool
pub type DbPool = deadpool::Pool<AsyncPgConnection>;

/// Create a new database connection pool from a DATABASE_URL
pub async fn create_pool(database_url: &str) -> Result<DbPool> {
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    
    let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    let pool = deadpool::Pool::builder(config)
        .max_size(10)
        .build()
        .context("Failed to create database pool")?;
    
    Ok(pool)
}

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

    /// Record a fill for an order AND persist to trade_history table
    /// This is the preferred method for live trading as it updates both tables
    pub async fn record_fill_with_trade_history(
        &self,
        order_unique_id: &str,
        fill_id: &str,
        quantity: f64,
        price: f64,
        fees: f64,
        deployment_id: Uuid,
        exchange: &str,
        symbol: &str,
        side: &str,
        realized_pnl: Option<f64>,
    ) -> Result<()> {
        let mut conn = self.pool.get().await
            .context("Failed to get database connection")?;

        // Look up the order by unique_id
        let order = StrategyOrderOps::get_order_by_unique_id(&mut conn, order_unique_id.to_string())
            .await
            .context("Failed to lookup order")?
            .ok_or_else(|| anyhow::anyhow!("Order not found: {}", order_unique_id))?;

        // Record strategy_order_fill
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

        // Also record to trade_history for dashboard analytics
        let qty_bd = BigDecimal::try_from(quantity).unwrap_or_else(|_| BigDecimal::from(0));
        let price_bd = BigDecimal::try_from(price).unwrap_or_else(|_| BigDecimal::from(0));
        let fees_bd = BigDecimal::try_from(fees).unwrap_or_else(|_| BigDecimal::from(0));
        let realized_pnl_bd = realized_pnl.and_then(|p| BigDecimal::try_from(p).ok());
        let now = Utc::now();

        let trade_record = NewTradeRecord {
            deployment_id,
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            side: side.to_string(),
            quantity: qty_bd.clone(),
            price: price_bd.clone(),
            quote_currency: "USD".to_string(),
            value: &qty_bd * &price_bd,
            commission: fees_bd,
            commission_asset: "USD".to_string(),
            realized_pnl: realized_pnl_bd,
            exchange_trade_id: fill_id.to_string(),
            exchange_order_id: order_unique_id.to_string(),
            executed_at: now,
            signal_price: None,
            signal_at: None,
        };

        trade_history_ops::insert_trade(&mut conn, trade_record)
            .await
            .context("Failed to record trade to trade_history")?;

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

// ============================================================================
// Exchange Credentials Loading
// ============================================================================

/// Decrypt a credential value (matches BacktestingEngine encryption format)
///
/// Supports two formats:
/// - `aes:<base64(nonce+ciphertext)>` — AES-256-GCM with 12-byte nonce, key from
///   `CREDENTIALS_ENCRYPTION_KEY` env (64 hex chars / 32 bytes). This is the
///   format BacktestingEngine writes via the Settings UI.
/// - `enc:<base64>` — legacy plain base64 (kept for backward compat with rows
///   created before AES was introduced).
///
/// Returns `None` (and logs a warning) for any decode/decrypt failure so the
/// engine can keep running rather than crashing on one bad row.
fn decrypt_credential_value(encrypted: &str) -> Option<String> {
    if let Some(encoded) = encrypted.strip_prefix("aes:") {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};
        let hex_key = match std::env::var("CREDENTIALS_ENCRYPTION_KEY") {
            Ok(v) => v,
            Err(_) => {
                log::error!("CREDENTIALS_ENCRYPTION_KEY env var not set; cannot decrypt aes: credential");
                return None;
            }
        };
        let key_bytes = hex::decode(hex_key.trim()).ok()?;
        if key_bytes.len() != 32 {
            log::error!("CREDENTIALS_ENCRYPTION_KEY must be 32 bytes (64 hex chars), got {}", key_bytes.len());
            return None;
        }
        let combined = STANDARD.decode(encoded).ok()?;
        if combined.len() < 12 {
            return None;
        }
        let (nonce_bytes, ciphertext) = combined.split_at(12);
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = cipher.decrypt(nonce, ciphertext).ok()?;
        String::from_utf8(plaintext).ok()
    } else if let Some(encoded) = encrypted.strip_prefix("enc:") {
        STANDARD.decode(encoded).ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
    } else {
        None
    }
}

/// Decrypted credential for use in trading
#[derive(Debug, Clone)]
pub struct ExchangeCredential {
    pub id: Uuid,
    pub exchange: String,
    pub label: String,
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
    pub is_testnet: bool,
    pub is_enabled: bool,
}

/// Load all enabled exchange credentials from the database.
pub async fn load_exchange_credentials(
    pool: &DbPool,
) -> Result<Vec<ExchangeCredential>> {
    use databaseschema::schema::exchange_credentials;
    use diesel_async::RunQueryDsl;

    let mut conn = pool.get().await
        .context("Failed to get database connection")?;

    let query = exchange_credentials::table
        .filter(exchange_credentials::is_enabled.eq(true))
        .select((
            exchange_credentials::id,
            exchange_credentials::exchange,
            exchange_credentials::label,
            exchange_credentials::api_key_encrypted,
            exchange_credentials::api_secret_encrypted,
            exchange_credentials::passphrase_encrypted,
            exchange_credentials::is_testnet,
            exchange_credentials::is_enabled,
        ));

    let rows: Vec<(Uuid, String, String, String, String, Option<String>, bool, bool)> =
        RunQueryDsl::load(query, &mut conn)
            .await
            .context("Failed to load exchange credentials")?;

    // Decrypt and convert
    let credentials: Vec<ExchangeCredential> = rows
        .into_iter()
        .filter_map(|(id, exchange, label, api_key_enc, api_secret_enc, passphrase_enc, is_testnet, is_enabled)| {
            // Decrypt values
            let api_key = decrypt_credential_value(&api_key_enc)?;
            let api_secret = decrypt_credential_value(&api_secret_enc)?;
            let passphrase = passphrase_enc.as_ref().and_then(|p| decrypt_credential_value(p));

            Some(ExchangeCredential {
                id,
                exchange,
                label,
                api_key,
                api_secret,
                passphrase,
                is_testnet,
                is_enabled,
            })
        })
        .collect();

    Ok(credentials)
}

/// Load credentials for a specific exchange.
///
/// `live_only: true` restricts the lookup to non-testnet credentials — REQUIRED
/// for live deployments. Without it the first enabled row wins, and holding
/// both sandbox and production keys for the same exchange could have live
/// orders signed with the sandbox key (orders silently go nowhere real) or,
/// inverted, a "sandbox" flow hit production. Paper/simulation flows may
/// pass `false` to accept either.
pub async fn load_credentials_for_exchange(
    pool: &DbPool,
    exchange: &str,
    live_only: bool,
) -> Result<Option<ExchangeCredential>> {
    use databaseschema::schema::exchange_credentials;
    use diesel_async::RunQueryDsl;

    let mut conn = pool.get().await
        .context("Failed to get database connection")?;

    let mut query = exchange_credentials::table
        .filter(exchange_credentials::exchange.eq(exchange.to_lowercase()))
        .filter(exchange_credentials::is_enabled.eq(true))
        .into_boxed();
    if live_only {
        query = query.filter(exchange_credentials::is_testnet.eq(false));
    }
    let query = query
        .select((
            exchange_credentials::id,
            exchange_credentials::exchange,
            exchange_credentials::label,
            exchange_credentials::api_key_encrypted,
            exchange_credentials::api_secret_encrypted,
            exchange_credentials::passphrase_encrypted,
            exchange_credentials::is_testnet,
            exchange_credentials::is_enabled,
        ));

    let row: Option<(Uuid, String, String, String, String, Option<String>, bool, bool)> =
        RunQueryDsl::first(query, &mut conn)
            .await
            .optional()
            .context("Failed to query exchange credentials")?;

    let credential = row.and_then(|(id, exchange, label, api_key_enc, api_secret_enc, passphrase_enc, is_testnet, is_enabled)| {
        let api_key = decrypt_credential_value(&api_key_enc)?;
        let api_secret = decrypt_credential_value(&api_secret_enc)?;
        let passphrase = passphrase_enc.as_ref().and_then(|p| decrypt_credential_value(p));

        Some(ExchangeCredential {
            id,
            exchange,
            label,
            api_key,
            api_secret,
            passphrase,
            is_testnet,
            is_enabled,
        })
    });

    Ok(credential)
}

// ============================================================================
// Background SOR Database Writer
// ============================================================================

/// Events that can be sent to the background SOR writer.
#[derive(Debug)]
pub enum SorDbEvent {
    PersistRoute {
        route: crate::SmartOrderRoute,
        strategy_name: String,
    },
    RecordFill {
        order_unique_id: String,
        fill_id: String,
        quantity: f64,
        price: f64,
        fees: f64,
    },
    UpdateStatus {
        order_unique_id: String,
        new_status: RouteStatus,
        reason: Option<String>,
    },
}

/// A bounded-channel wrapper around [`OrderDatabasePersistence`] that keeps
/// database I/O off the order execution hot path.
///
/// All sends are `try_send` — if the channel is full, the event is dropped
/// and an overflow counter is incremented so callers can observe back-pressure.
pub struct BackgroundSorWriter {
    sender: tokio::sync::mpsc::Sender<SorDbEvent>,
    overflow: Arc<std::sync::atomic::AtomicU64>,
}

impl BackgroundSorWriter {
    /// Create the writer and spawn its background drain task.
    ///
    /// * `persistence` – the real DB persistence layer.
    /// * `channel_capacity` – bounded channel size (default: 5000).
    pub fn spawn(persistence: Arc<OrderDatabasePersistence>, channel_capacity: usize) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<SorDbEvent>(channel_capacity);
        let overflow = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let overflow_clone = Arc::clone(&overflow);

        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match event {
                    SorDbEvent::PersistRoute { route, strategy_name } => {
                        if let Err(e) = persistence.persist_route(&route, &strategy_name).await {
                            log::error!("[SOR-WRITER] Failed to persist route {}: {}", route.id, e);
                        }
                    }
                    SorDbEvent::RecordFill { order_unique_id, fill_id, quantity, price, fees } => {
                        if let Err(e) = persistence.record_fill(&order_unique_id, &fill_id, quantity, price, fees).await {
                            log::error!("[SOR-WRITER] Failed to record fill {}: {}", fill_id, e);
                        }
                    }
                    SorDbEvent::UpdateStatus { order_unique_id, new_status, reason } => {
                        if let Err(e) = persistence.update_order_status(&order_unique_id, new_status, reason.as_deref()).await {
                            log::error!("[SOR-WRITER] Failed to update status for {}: {}", order_unique_id, e);
                        }
                    }
                }
            }
            // Channel closed — graceful shutdown
            let overflow_count = overflow_clone.load(std::sync::atomic::Ordering::Relaxed);
            if overflow_count > 0 {
                log::warn!("[SOR-WRITER] Shutting down. Total overflow (dropped events): {}", overflow_count);
            }
            log::info!("[SOR-WRITER] Background SOR writer shut down cleanly.");
        });

        Self { sender: tx, overflow }
    }

    /// Non-blocking send. Returns `true` if enqueued, `false` if dropped.
    pub fn try_send(&self, event: SorDbEvent) -> bool {
        match self.sender.try_send(event) {
            Ok(_) => true,
            Err(_) => {
                self.overflow.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                false
            }
        }
    }

    /// Number of events dropped due to channel full.
    pub fn overflow_count(&self) -> u64 {
        self.overflow.load(std::sync::atomic::Ordering::Relaxed)
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
    
    #[test]
    fn test_decrypt_credential_value() {
        let original = "my-api-key-123";
        let encrypted = format!("enc:{}", STANDARD.encode(original));
        
        let decrypted = decrypt_credential_value(&encrypted);
        assert_eq!(decrypted, Some(original.to_string()));
        
        // Invalid format should return None
        assert_eq!(decrypt_credential_value("not-encrypted"), None);
    }
}
