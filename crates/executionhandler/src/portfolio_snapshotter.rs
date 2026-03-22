//! Periodic portfolio state snapshotter.
//!
//! Reads live [`PortfolioPnL`] from the in-memory [`PositionTracker`] on a
//! configurable interval (default 5 s) and writes `pnl_snapshots` rows via a
//! background task so the hot path is never blocked.

use std::sync::Arc;
use std::time::Duration;

use bigdecimal::BigDecimal;
use chrono::Utc;
use uuid::Uuid;

use databaseschema::models::pnl_snapshot::NewPnLSnapshot;
use databaseschema::ops::pnl_snapshot_ops;

use crate::position_tracker::PositionTracker;

/// Configuration for the portfolio snapshotter.
pub struct PortfolioSnapshotterConfig {
    /// How often to capture a snapshot. Default: 5 s.
    pub interval: Duration,
    /// Tenant owning this strategy session.
    pub tenant_id: Uuid,
}

impl Default for PortfolioSnapshotterConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            tenant_id: Uuid::nil(),
        }
    }
}

/// Handle for the background snapshotter.  Dropping this cancels the task.
pub struct PortfolioSnapshotter {
    handle: tokio::task::JoinHandle<()>,
}

impl PortfolioSnapshotter {
    /// Spawn the periodic writer.
    ///
    /// * `tracker`  – shared position tracker (lock-free reads).
    /// * `pool`     – diesel-async DB pool.
    /// * `config`   – interval & tenant id.
    pub fn spawn(
        tracker: Arc<PositionTracker>,
        pool: Arc<smartorderrouter::DbPool>,
        config: PortfolioSnapshotterConfig,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                let pnl = match tracker.get_total_pnl() {
                    Ok(p) => p,
                    Err(e) => {
                        log::warn!("[PORTFOLIO-SNAP] Failed to read PnL: {}", e);
                        continue;
                    }
                };
                let by_exchange = tracker.get_pnl_by_exchange().unwrap_or_default();
                let by_exchange_json = serde_json::to_value(&by_exchange).unwrap_or_default();

                let snapshot = NewPnLSnapshot::new(
                    config.tenant_id,
                    Utc::now(),
                    BigDecimal::try_from(pnl.total_pnl).unwrap_or_default(),
                    BigDecimal::try_from(pnl.realized_pnl).unwrap_or_default(),
                    BigDecimal::try_from(pnl.unrealized_pnl).unwrap_or_default(),
                )
                .with_exchange_breakdown(by_exchange_json);

                match pool.get().await {
                    Ok(mut conn) => {
                        if let Err(e) = pnl_snapshot_ops::upsert_snapshot(&mut conn, snapshot).await {
                            log::error!(
                                "[PORTFOLIO-SNAP] Failed to write PnL snapshot: {}",
                                e
                            );
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "[PORTFOLIO-SNAP] Failed to get DB connection: {}",
                            e
                        );
                    }
                }
            }
        });

        Self { handle }
    }

    /// Cancel the background task.
    pub fn stop(self) {
        self.handle.abort();
    }
}

impl Drop for PortfolioSnapshotter {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
