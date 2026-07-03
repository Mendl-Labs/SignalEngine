//! Market-Data Health Writer
//!
//! Periodically inspects `datahandler`'s in-memory freshness counters and:
//!
//! 1. Upserts one `market_data_health` row per (tenant, exchange, symbol)
//!    from open positions AND the active-deployment registry (dashboard
//!    ExchangeStatus consumes these).
//! 2. Stamps `deployed_strategies.last_data_at` for every ACTIVE deployment
//!    whose symbol received data in the freshness window — the per-deployment
//!    heartbeat behind the dashboard `FeedStatusBadge`. Measured HERE, at the
//!    consumer end, because "DataEngine published" does not imply "strategies
//!    received" (see the 2026-06/07 13-day silent outage: three healthy-looking
//!    layers, zero ticks delivered).
//!
//! The registry enumeration also fixes the old v1 limitation where idle
//! deployments (no open position — i.e. any strategy that is correctly
//! HOLDING) were invisible to health reporting.

use std::sync::Arc;
use std::collections::HashSet;

use chrono::Utc;
use smartorderrouter::database::DbPool;
use ultra_logger::{ultra_info, ultra_error};

use databaseschema::models::market_data_health::UpsertMarketDataHealth;
use databaseschema::ops::{deployed_strategy_ops, deployment_position_ops, market_data_health_ops};

use crate::PaperDeploymentRegistry;

/// A deployment's data is considered fresh (→ stamp last_data_at) when its
/// symbol saw a tick or book update within this window. Matches the 30s task
/// cadence with headroom; the API layer's thresholds sit far above it.
const FRESH_WINDOW_SECS: i64 = 90;

/// Spawn the market-data health writer task.
pub fn spawn(pool: Arc<DbPool>, registry: PaperDeploymentRegistry) {
    tokio::spawn(run(pool, registry));
}

async fn run(pool: Arc<DbPool>, registry: PaperDeploymentRegistry) {
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

        // Collapse to distinct (tenant, exchange, canonical_symbol) —
        // positions PLUS every symbol of every registered deployment, so
        // idle (holding) deployments are reported too.
        let mut tuples: HashSet<(uuid::Uuid, String, String)> = HashSet::new();
        for p in &positions {
            tuples.insert((p.tenant_id, p.exchange.clone(), canon(&p.symbol)));
        }
        for entry in registry.iter() {
            let m = entry.value();
            for sym in &m.symbols {
                tuples.insert((m.tenant_id, m.real_exchange.clone(), canon(sym)));
            }
        }

        if tuples.is_empty() {
            continue;
        }

        // Snapshot each tuple ONCE (snapshot_market_data_health resets the
        // tick-rate window; double-reads would corrupt ticks_per_sec) and
        // reuse for both the health rows and the deployment heartbeat.
        // datahandler keys on the raw (symbol, exchange) it received from the
        // feed: symbol may be slash or dash form, and exchange may be the
        // deployment VENUE ("kraken") or the DATA PROVIDER ("massive" — the
        // consolidated feed publishes payloads with exchange="massive"
        // regardless of requesting venue). Try all four combinations.
        let mut snaps: std::collections::HashMap<(uuid::Uuid, String, String), datahandler::MarketDataHealthSnapshot> =
            std::collections::HashMap::new();
        for (tenant_id, exchange, symbol_canon) in &tuples {
            let slash_form = symbol_canon.replace('-', "/");
            let snap = datahandler::snapshot_market_data_health(&slash_form, exchange)
                .or_else(|| datahandler::snapshot_market_data_health(symbol_canon, exchange))
                .or_else(|| datahandler::snapshot_market_data_health(&slash_form, "massive"))
                .or_else(|| datahandler::snapshot_market_data_health(symbol_canon, "massive"));
            if let Some(s) = snap {
                snaps.insert((*tenant_id, exchange.clone(), symbol_canon.clone()), s);
            }
        }

        // --- Per-deployment heartbeat: stamp deployed_strategies.last_data_at
        // for every registered deployment whose symbol saw data recently.
        let now = Utc::now();
        let fresh_ids: Vec<uuid::Uuid> = registry
            .iter()
            .filter(|entry| {
                let m = entry.value();
                m.symbols.iter().any(|sym| {
                    snaps
                        .get(&(m.tenant_id, m.real_exchange.clone(), canon(sym)))
                        .map(|s| {
                            let newest = s.last_tick_at.max(s.last_orderbook_at);
                            newest.map_or(false, |t| (now - t).num_seconds() < FRESH_WINDOW_SECS)
                        })
                        .unwrap_or(false)
                })
            })
            .map(|entry| *entry.key())
            .collect();

        if !fresh_ids.is_empty() {
            if let Err(e) =
                deployed_strategy_ops::stamp_last_data_at(&mut conn, &fresh_ids, now).await
            {
                ultra_error!(format!("market-health: heartbeat stamp failed: {}", e));
            }
        }

        let mut upserted = 0usize;
        let mut missing = 0usize;

        for (tenant_id, exchange, symbol_canon) in &tuples {
            let snap = match snaps.get(&(*tenant_id, exchange.clone(), symbol_canon.clone())) {
                Some(s) => s.clone(),
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
