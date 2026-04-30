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
use databaseschema::ops::{paper_fill_ops, deployment_position_ops};

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
    /// Source of truth for mark prices is `datahandler::ORDERBOOKS` populated
    /// by the live market-data feed. Every tick we:
    ///   1. Fetch all open positions across all deployments.
    ///   2. For each, find a matching orderbook (by canonicalized symbol) and
    ///      compute mid = (best_bid + best_ask) / 2.
    ///   3. Bulk-update `deployment_positions.last_mark_price` and
    ///      `last_mark_at`. Failures on individual rows are logged and skipped
    ///      so a single bad row never stalls the whole flush.
    async fn run_mark_flusher(pool: Arc<DbPool>) {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        fn canon(s: &str) -> String { s.replace('/', "-").to_uppercase() }

        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        // First tick fires immediately; skip it so we don't flush on startup
        // before any market data has been received.
        tick.tick().await;

        ultra_info!("📈 Mark-price flusher started (30s cadence)");

        loop {
            tick.tick().await;

            // Snapshot every (canonical_symbol -> mid_price) seen in the live
            // book registry. Doing this once per tick avoids re-locking each
            // book per position.
            let mut mid_by_symbol: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            for entry in datahandler::ORDERBOOKS.iter() {
                let (sym, _exch) = entry.key();
                if let Ok(book) = entry.value().clone().read() {
                    if let Ok((bids, asks)) = book.get_orderbook_levels(1) {
                        if !bids.is_empty() && !asks.is_empty() {
                            let mid = (bids[0].0 + asks[0].0) / 2.0;
                            if mid > 0.0 {
                                mid_by_symbol.insert(canon(sym), mid);
                            }
                        }
                    }
                }
            }

            if mid_by_symbol.is_empty() {
                continue; // No live data yet — skip this tick.
            }

            let mut conn = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    ultra_error!(format!("mark-flusher: DB connect failed: {}", e));
                    continue;
                }
            };

            let positions = match deployment_position_ops::get_all_open_positions(&mut conn).await {
                Ok(p) => p,
                Err(e) => {
                    ultra_error!(format!("mark-flusher: get_all_open_positions failed: {}", e));
                    continue;
                }
            };

            let now = Utc::now();
            let mut updated = 0usize;
            let mut skipped = 0usize;

            for pos in &positions {
                let key = canon(&pos.symbol);
                let mid = match mid_by_symbol.get(&key) {
                    Some(m) => *m,
                    None => { skipped += 1; continue; }
                };
                let mark = match BigDecimal::from_str(&format!("{}", mid)) {
                    Ok(b) => b,
                    Err(_) => { skipped += 1; continue; }
                };
                match deployment_position_ops::update_mark(
                    &mut conn,
                    pos.deployment_id,
                    &pos.exchange,
                    &pos.symbol,
                    &mark,
                    now,
                ).await {
                    Ok(_) => updated += 1,
                    Err(e) => {
                        ultra_error!(format!(
                            "mark-flusher: update_mark failed for {}/{}/{}: {}",
                            pos.deployment_id, pos.exchange, pos.symbol, e
                        ));
                    }
                }
            }

            if updated > 0 || skipped > 0 {
                ultra_info!(format!(
                    "📈 mark-flusher: updated={} skipped={} (no matching book) total_open={}",
                    updated, skipped, positions.len()
                ));
            }
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
