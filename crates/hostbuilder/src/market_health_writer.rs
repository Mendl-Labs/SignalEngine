//! Market-Data Health Writer
//!
//! Periodically inspects `datahandler`'s in-memory freshness counters and
//! upserts one row per (tenant, exchange, symbol) currently held in
//! `deployment_positions` into `market_data_health`. Consumed by
//! BacktestingEngine's dashboard to drive the per-exchange/per-symbol
//! "Live / Stale / Down" status.
//!
//! Limitation (v1): only symbols with an open position are reported. Idle
//! deployments without positions are not yet enumerated.

use std::sync::Arc;
use std::collections::HashSet;

use smartorderrouter::database::DbPool;
use ultra_logger::{ultra_info, ultra_error};

use databaseschema::models::market_data_health::UpsertMarketDataHealth;
use databaseschema::ops::{deployment_position_ops, market_data_health_ops};

/// Spawn the market-data health writer task.
///
/// Runs every 30s. For each (tenant, exchange, symbol) tuple represented in
/// `deployment_positions` it pulls the freshness snapshot from `datahandler`
/// and upserts a row.
pub fn spawn(pool: Arc<DbPool>) {
    tokio::spawn(run(pool));
}

async fn run(pool: Arc<DbPool>) {
    fn canon(s: &str) -> String { s.replace('/', "-").to_uppercase() }

    let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
    // Skip the first immediate tick — wait a full window before first write.
    tick.tick().await;

    ultra_info!("\u{1f4e1} market-data health writer started (30s cadence)");

    loop {
        tick.tick().await;

        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                ultra_error!(format!("market-health: DB connect failed: {}", e));
                continue;
            }
        };

        let positions = match deployment_position_ops::get_all_open_positions(&mut conn).await {
            Ok(p) => p,
            Err(e) => {
                ultra_error!(format!("market-health: get_all_open_positions failed: {}", e));
                continue;
            }
        };

        if positions.is_empty() {
            continue;
        }

        // Collapse to distinct (tenant, exchange, canonical_symbol).
        let mut tuples: HashSet<(uuid::Uuid, String, String)> = HashSet::new();
        for p in &positions {
            tuples.insert((p.tenant_id, p.exchange.clone(), canon(&p.symbol)));
        }

        let mut upserted = 0usize;
        let mut missing = 0usize;

        for (tenant_id, exchange, symbol_canon) in &tuples {
            // datahandler keys on the raw symbol it received from the feed
            // (e.g. "BTC/USD"). Try the slash form first then the canonical
            // dash form so we don't miss the snapshot.
            let slash_form = symbol_canon.replace('-', "/");
            let snap = datahandler::snapshot_market_data_health(&slash_form, exchange)
                .or_else(|| datahandler::snapshot_market_data_health(symbol_canon, exchange));

            let snap = match snap {
                Some(s) => s,
                None => { missing += 1; continue; }
            };

            let row = UpsertMarketDataHealth {
                tenant_id: *tenant_id,
                exchange: exchange.clone(),
                symbol: symbol_canon.clone(),
                last_tick_at: snap.last_tick_at,
                last_orderbook_at: snap.last_orderbook_at,
                ticks_per_sec: snap.ticks_per_sec,
                gap_count_5m: 0,
            };

            match market_data_health_ops::upsert(&mut conn, &row).await {
                Ok(_) => upserted += 1,
                Err(e) => ultra_error!(format!(
                    "market-health: upsert failed for {}/{}/{}: {}",
                    tenant_id, exchange, symbol_canon, e
                )),
            }
        }

        if upserted > 0 || missing > 0 {
            ultra_info!(format!(
                "\u{1f4e1} market-health: upserted={} missing_snapshot={} tuples={}",
                upserted, missing, tuples.len()
            ));
        }
    }
}
