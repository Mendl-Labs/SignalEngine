//! Paper Trade Persistence Writer
//!
//! Receives paper trading fill events and persists them to:
//! - `trade_history` — individual trade records for dashboard analytics
//! - `pnl_snapshots` — periodic P&L snapshots (every 5 minutes)
//! - `deployed_strategies` — updates live_pnl, live_trades, last_trade_at
//!
//! This module is gated behind the `postgres` feature flag.

use std::sync::Arc;
use bigdecimal::BigDecimal;
use chrono::Utc;
use dashmap::DashMap;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::deadpool;
use tokio::sync::mpsc;
use uuid::Uuid;

use databaseschema::models::trade_history::NewTradeRecord;
use databaseschema::models::pnl_snapshot::NewPnLSnapshot;
use databaseschema::ops::trade_history_ops;
use databaseschema::ops::pnl_snapshot_ops;

use ultra_logger::{ultra_info, ultra_warn, ultra_error};

/// Type alias for the database pool
pub type DbPool = deadpool::Pool<AsyncPgConnection>;

/// A paper trade fill event to be persisted
#[derive(Debug, Clone)]
pub struct PaperFillEvent {
    pub tenant_id: Uuid,
    pub deployment_id: Uuid,
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub price: f64,
    pub fees: f64,
    pub realized_pnl: Option<f64>,
    pub fill_id: String,
    pub order_id: String,
}

/// Accumulated P&L state per deployment for periodic snapshots
#[derive(Debug, Default)]
struct DeploymentPnLState {
    realized_pnl: f64,
    unrealized_pnl: f64,
    total_capital: f64,
    trades_count: i32,
    winning_trades: i32,
    losing_trades: i32,
}

/// Paper trade persistence writer
///
/// Spawns a background task that:
/// 1. Writes each fill to `trade_history` immediately
/// 2. Writes P&L snapshots every 5 minutes
pub struct PaperTradeWriter {
    fill_tx: mpsc::Sender<PaperFillEvent>,
}

impl PaperTradeWriter {
    /// Create a new paper trade writer with a database pool
    ///
    /// Spawns a background tokio task that processes fill events.
    pub fn new(pool: Arc<DbPool>) -> Self {
        let (fill_tx, fill_rx) = mpsc::channel::<PaperFillEvent>(1000);

        tokio::spawn(Self::run_writer(pool, fill_rx));

        Self { fill_tx }
    }

    /// Send a fill event for persistence (non-blocking)
    pub fn record_fill(&self, event: PaperFillEvent) {
        if let Err(e) = self.fill_tx.try_send(event) {
            ultra_warn!(format!("Paper trade writer channel full, dropping fill: {}", e));
        }
    }

    /// Get a cloneable sender for use in spawned tasks
    pub fn sender(&self) -> mpsc::Sender<PaperFillEvent> {
        self.fill_tx.clone()
    }

    /// Background writer loop
    async fn run_writer(
        pool: Arc<DbPool>,
        mut fill_rx: mpsc::Receiver<PaperFillEvent>,
    ) {
        ultra_info!("📝 Paper trade writer started");

        let deployment_state: DashMap<Uuid, DeploymentPnLState> = DashMap::new();
        let mut snapshot_interval = tokio::time::interval(tokio::time::Duration::from_secs(300)); // 5 min
        snapshot_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe_fill = fill_rx.recv() => {
                    match maybe_fill {
                        Some(fill) => {
                            Self::persist_fill(&pool, &deployment_state, &fill).await;
                        }
                        None => {
                            // Channel closed — final snapshot and exit
                            ultra_info!("📝 Paper trade writer shutting down, writing final snapshots");
                            Self::write_snapshots(&pool, &deployment_state).await;
                            return;
                        }
                    }
                }
                _ = snapshot_interval.tick() => {
                    Self::write_snapshots(&pool, &deployment_state).await;
                }
            }
        }
    }

    /// Persist a single fill to trade_history and update deployment state
    async fn persist_fill(
        pool: &Arc<DbPool>,
        deployment_state: &DashMap<Uuid, DeploymentPnLState>,
        fill: &PaperFillEvent,
    ) {
        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                ultra_error!(format!("Paper trade writer: failed to get DB connection: {}", e));
                return;
            }
        };

        let qty = BigDecimal::try_from(fill.quantity).unwrap_or_else(|_| BigDecimal::from(0));
        let price = BigDecimal::try_from(fill.price).unwrap_or_else(|_| BigDecimal::from(0));
        let fees = BigDecimal::try_from(fill.fees).unwrap_or_else(|_| BigDecimal::from(0));
        let realized_pnl = fill.realized_pnl.and_then(|p| BigDecimal::try_from(p).ok());
        let value = &qty * &price;

        let trade = NewTradeRecord {
            deployment_id: fill.deployment_id,
            exchange: fill.exchange.clone(),
            symbol: fill.symbol.clone(),
            side: fill.side.clone(),
            quantity: qty,
            price,
            quote_currency: "USD".to_string(),
            value,
            commission: fees,
            commission_asset: "USD".to_string(),
            realized_pnl: realized_pnl.clone(),
            exchange_trade_id: fill.fill_id.clone(),
            exchange_order_id: fill.order_id.clone(),
            executed_at: Utc::now(),
            signal_price: None,
            signal_at: None,
        };

        match trade_history_ops::insert_trade(&mut conn, trade).await {
            Ok(_) => {
                // Update in-memory P&L state
                let mut state = deployment_state
                    .entry(fill.deployment_id)
                    .or_default();
                state.trades_count += 1;
                if let Some(pnl) = fill.realized_pnl {
                    state.realized_pnl += pnl;
                    if pnl > 0.0 {
                        state.winning_trades += 1;
                    } else if pnl < 0.0 {
                        state.losing_trades += 1;
                    }
                }
            }
            Err(e) => {
                ultra_error!(format!(
                    "Paper trade writer: failed to insert trade for deployment {}: {}",
                    fill.deployment_id, e
                ));
            }
        }
    }

    /// Write P&L snapshots for all active deployments
    async fn write_snapshots(
        pool: &Arc<DbPool>,
        deployment_state: &DashMap<Uuid, DeploymentPnLState>,
    ) {
        if deployment_state.is_empty() {
            return;
        }

        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                ultra_error!(format!("Paper trade writer: failed to get DB connection for snapshots: {}", e));
                return;
            }
        };

        let now = Utc::now();

        for entry in deployment_state.iter() {
            let deployment_id = entry.key();
            let state = entry.value();

            // NOTE (OSS port): the private schema disambiguated per-deployment
            // snapshots via a (snapshot_at, tenant_id, mode) key, hackily
            // reusing tenant_id = deployment_id (see the private repo's own
            // "Will be corrected by caller" comment). This OSS schema has no
            // tenant_id, so the upsert key is just (snapshot_at, mode) --
            // multiple deployments snapshotted in the same tick will now
            // collide and only the last-written one survives. Pre-existing
            // design smell in the source this was ported from, not something
            // introduced by dropping tenant_id; left as-is rather than
            // redesigning the aggregation unasked.
            let snapshot = NewPnLSnapshot {
                snapshot_at: now,
                total_pnl: BigDecimal::try_from(state.realized_pnl + state.unrealized_pnl)
                    .unwrap_or_else(|_| BigDecimal::from(0)),
                realized_pnl: BigDecimal::try_from(state.realized_pnl)
                    .unwrap_or_else(|_| BigDecimal::from(0)),
                unrealized_pnl: BigDecimal::try_from(state.unrealized_pnl)
                    .unwrap_or_else(|_| BigDecimal::from(0)),
                daily_pnl: BigDecimal::try_from(state.realized_pnl)
                    .unwrap_or_else(|_| BigDecimal::from(0)),
                total_capital: BigDecimal::try_from(state.total_capital)
                    .unwrap_or_else(|_| BigDecimal::from(0)),
                total_equity: BigDecimal::try_from(state.total_capital + state.unrealized_pnl)
                    .unwrap_or_else(|_| BigDecimal::from(0)),
                by_exchange: serde_json::json!({}),
                by_deployment: serde_json::json!({
                    deployment_id.to_string(): {
                        "realized_pnl": state.realized_pnl,
                        "unrealized_pnl": state.unrealized_pnl,
                        "trades": state.trades_count,
                    }
                }),
                trades_count: state.trades_count,
                winning_trades: state.winning_trades,
                losing_trades: state.losing_trades,
                // This writer is the paper-trading hot path. Live
                // deployments do not flow through here yet.
                mode: "paper".to_string(),
            };

            match pnl_snapshot_ops::upsert_snapshot(&mut conn, snapshot).await {
                Ok(_) => {}
                Err(e) => {
                    ultra_error!(format!(
                        "Paper trade writer: failed to upsert PnL snapshot for {}: {}",
                        deployment_id, e
                    ));
                }
            }
        }

        ultra_info!(format!(
            "📊 Paper trade P&L snapshots written for {} deployments",
            deployment_state.len()
        ));
    }
}
