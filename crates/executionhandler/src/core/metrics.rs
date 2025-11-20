use crate::core::types::ExecutionMetrics;
use crate::optimizations::timestamp::nano_timestamp;
use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe metrics collector for execution performance
pub struct MetricsCollector {
    total_orders: AtomicU64,
    successful_orders: AtomicU64,
    failed_orders: AtomicU64,
    total_latency_ns: AtomicU64,
    min_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
    last_updated: AtomicU64,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            total_orders: AtomicU64::new(0),
            successful_orders: AtomicU64::new(0),
            failed_orders: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            min_latency_ns: AtomicU64::new(u64::MAX),
            max_latency_ns: AtomicU64::new(0),
            last_updated: AtomicU64::new(0),
        }
    }

    pub fn record_success(&self, latency_ns: u64) {
        self.total_orders.fetch_add(1, Ordering::Relaxed);
        self.successful_orders.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        self.min_latency_ns.fetch_min(latency_ns, Ordering::Relaxed);
        self.max_latency_ns.fetch_max(latency_ns, Ordering::Relaxed);
        self.last_updated.store(nano_timestamp() as u64, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.total_orders.fetch_add(1, Ordering::Relaxed);
        self.failed_orders.fetch_add(1, Ordering::Relaxed);
        self.last_updated.store(nano_timestamp() as u64, Ordering::Relaxed);
    }

    pub fn get_metrics(&self, exchange_name: String) -> ExecutionMetrics {
        let total = self.total_orders.load(Ordering::Relaxed);
        let successful = self.successful_orders.load(Ordering::Relaxed);
        let failed = self.failed_orders.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ns.load(Ordering::Relaxed);
        let min_latency = self.min_latency_ns.load(Ordering::Relaxed);
        let max_latency = self.max_latency_ns.load(Ordering::Relaxed);

        ExecutionMetrics {
            exchange: exchange_name,
            total_orders: total,
            successful_orders: successful,
            failed_orders: failed,
            cancelled_orders: 0, // TODO: Track separately
            avg_latency_ns: if successful > 0 { total_latency / successful } else { 0 },
            min_latency_ns: if min_latency == u64::MAX { 0 } else { min_latency },
            max_latency_ns: max_latency,
            p50_latency_ns: 0, // TODO: Calculate percentiles
            p95_latency_ns: 0,
            p99_latency_ns: 0,
            p999_latency_ns: 0,
            total_volume: 0.0, // TODO: Track volume
            total_fees: 0.0,   // TODO: Track fees
            fill_rate: if total > 0 { successful as f64 / total as f64 } else { 0.0 },
            error_rate: if total > 0 { failed as f64 / total as f64 } else { 0.0 },
            orders_per_second: 0.0, // TODO: Calculate rate
            last_updated: self.last_updated.load(Ordering::Relaxed) as u128,
            websocket_connected: false, // TODO: Track WebSocket status
            connection_pool_utilization: 0.0, // TODO: Track pool utilization
            rate_limit_utilization: 0.0,      // TODO: Track rate limits
        }
    }

    pub fn reset(&self) {
        self.total_orders.store(0, Ordering::Relaxed);
        self.successful_orders.store(0, Ordering::Relaxed);
        self.failed_orders.store(0, Ordering::Relaxed);
        self.total_latency_ns.store(0, Ordering::Relaxed);
        self.min_latency_ns.store(u64::MAX, Ordering::Relaxed);
        self.max_latency_ns.store(0, Ordering::Relaxed);
        self.last_updated.store(nano_timestamp() as u64, Ordering::Relaxed);
    }
}
