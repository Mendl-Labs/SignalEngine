//! Paper Trade Persistence Writer
//!
//! Receives paper trading fill events and persists them transactionally to:
//! - `deployment_positions` — open position + cumulative realized P&L (avg-cost engine)
//! - `trade_history` — individual trade record with realized P&L attributed
//! - `deployed_strategies` — incremented `live_trades` and `live_pnl` counters
//!
//! P&L snapshot aggregation happens centrally in
//! `BacktestingEngine::pnl_scheduler`; this writer no longer maintains an
//! in-memory snapshot cache.

use std::sync::Arc;
use chrono::Utc;
use tokio::sync::mpsc;
use uuid::Uuid;

use smartorderrouter::database::DbPool;
use databaseschema::models::trade_history::TradeSide;
use databaseschema::ops::paper_fill_ops;

use ultra_logger::{ultra_info, ultra_error};

/// A paper trade fill event to be persisted.
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
    pub fill_id: String,
    pub order_id: String,
}

/// Paper trade persistence writer.
///
/// Spawns a background task that processes fill events one at a time and
/// applies the avg-cost accounting engine + trade-history insert + counter
/// bump in a single transaction per fill.
pub struct PaperTradeWriter {
    fill_tx: mpsc::Sender<PaperFillEvent>,
}

impl PaperTradeWriter {
    /// Create a new paper trade writer with a database pool.
    /// Spawns a background tokio task that processes fill events.
    pub fn new(pool: Arc<DbPool>) -> Self {
        let (fill_tx, fill_rx) = mpsc::channel::<PaperFillEvent>(1000);
        tokio::spawn(Self::run_writer(pool.clone(), fill_rx));
        tokio::spawn(Self::run_mark_flusher(pool));
        Self { fill_tx }
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

        while let Some(fill) = fill_rx.recv().await {
            Self::persist_fill(&pool, &fill).await;
        }

        ultra_info!("📝 Paper trade writer shutting down");
    }

    /// Background task that flushes the latest mark price for every open
    /// position to `deployment_positions.last_mark_price`.
    ///
    /// Phase 3 will wire this to a shared `MarkPriceCache` populated by the
    /// market-data receiver. For now it is a no-op heartbeat so the rest of
    /// the writer compiles and deploys.
    async fn run_mark_flusher(_pool: Arc<DbPool>) {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        // First tick fires immediately; skip it so we don't log on startup.
        tick.tick().await;
        loop {
            tick.tick().await;
            // TODO(phase-3): pull marks from MarkPriceCache and batch-update
            // deployment_positions.last_mark_price for any open position.
        }
    }

    /// Persist a single fill via the transactional `record_paper_fill` op.
    async fn persist_fill(pool: &Arc<DbPool>, fill: &PaperFillEvent) {
        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                ultra_error!(format!("Paper trade writer: failed to get DB connection: {}", e));
                return;
            }
        };

        let qty = bigdecimal::BigDecimal::try_from(fill.quantity).unwrap_or_default();
        let price = bigdecimal::BigDecimal::try_from(fill.price).unwrap_or_default();
        let fees = bigdecimal::BigDecimal::try_from(fill.fees).unwrap_or_default();

        let side = match fill.side.to_ascii_lowercase().as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            other => {
                ultra_error!(format!(
                    "Paper trade writer: unknown side '{}' for deployment {}",
                    other, fill.deployment_id
                ));
                return;
            }
        };

        match paper_fill_ops::record_paper_fill(
            &mut conn,
            fill.tenant_id,
            fill.deployment_id,
            &fill.exchange,
            &fill.symbol,
            side,
            qty,
            price,
            fees,
            "USD".to_string(),
            fill.fill_id.clone(),
            fill.order_id.clone(),
            Utc::now(),
        )
        .await
        {
            Ok(outcome) => {
                ultra_info!(format!(
                    "💱 Recorded paper fill {} for deployment {}: realized={}",
                    fill.fill_id, fill.deployment_id, outcome.realized_pnl
                ));
            }
            Err(e) => {
                ultra_error!(format!(
                    "Paper trade writer: failed to record fill {} for deployment {}: {}",
                    fill.fill_id, fill.deployment_id, e
                ));
            }
        }
    }
}
