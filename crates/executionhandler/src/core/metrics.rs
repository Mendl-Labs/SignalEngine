use crate::core::types::ExecutionMetrics;
use crate::optimizations::timestamp::nano_timestamp;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Mutex;
use std::collections::VecDeque;

/// Maximum latencies to keep for percentile calculation
const LATENCY_BUFFER_SIZE: usize = 10_000;

/// Thread-safe metrics collector for execution performance with full tracking
pub struct MetricsCollector {
    // Order counters
    total_orders: AtomicU64,
    successful_orders: AtomicU64,
    failed_orders: AtomicU64,
    cancelled_orders: AtomicU64,
    
    // Latency tracking
    total_latency_ns: AtomicU64,
    min_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
    latency_buffer: Mutex<VecDeque<u64>>,
    
    // Volume and fees (stored as integer micros for atomic operations)
    total_volume_micros: AtomicU64,
    total_fees_micros: AtomicU64,
    
    // Rate tracking
    orders_in_last_second: AtomicU64,
    rate_window_start_ns: AtomicU64,
    peak_orders_per_second: AtomicU64,
    
    // Connection status
    websocket_connected: AtomicBool,
    connection_pool_size: AtomicU64,
    connection_pool_in_use: AtomicU64,
    rate_limit_remaining: AtomicU64,
    rate_limit_total: AtomicU64,
    
    // Timing
    last_updated: AtomicU64,
    started_at: AtomicU64,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCollector {
    pub fn new() -> Self {
        let now = nano_timestamp() as u64;
        Self {
            total_orders: AtomicU64::new(0),
            successful_orders: AtomicU64::new(0),
            failed_orders: AtomicU64::new(0),
            cancelled_orders: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            min_latency_ns: AtomicU64::new(u64::MAX),
            max_latency_ns: AtomicU64::new(0),
            latency_buffer: Mutex::new(VecDeque::with_capacity(LATENCY_BUFFER_SIZE)),
            total_volume_micros: AtomicU64::new(0),
            total_fees_micros: AtomicU64::new(0),
            orders_in_last_second: AtomicU64::new(0),
            rate_window_start_ns: AtomicU64::new(now),
            peak_orders_per_second: AtomicU64::new(0),
            websocket_connected: AtomicBool::new(false),
            connection_pool_size: AtomicU64::new(0),
            connection_pool_in_use: AtomicU64::new(0),
            rate_limit_remaining: AtomicU64::new(0),
            rate_limit_total: AtomicU64::new(0),
            last_updated: AtomicU64::new(now),
            started_at: AtomicU64::new(now),
        }
    }

    /// Record a successful order with latency, volume, and fees
    pub fn record_success(&self, latency_ns: u64, volume: f64, fees: f64) {
        self.total_orders.fetch_add(1, Ordering::Relaxed);
        self.successful_orders.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        self.min_latency_ns.fetch_min(latency_ns, Ordering::Relaxed);
        self.max_latency_ns.fetch_max(latency_ns, Ordering::Relaxed);
        
        // Track volume and fees (convert to micros for atomic storage)
        self.total_volume_micros.fetch_add((volume * 1_000_000.0) as u64, Ordering::Relaxed);
        self.total_fees_micros.fetch_add((fees * 1_000_000.0) as u64, Ordering::Relaxed);
        
        // Store latency in buffer for percentile calculation
        if let Ok(mut buffer) = self.latency_buffer.lock() {
            if buffer.len() >= LATENCY_BUFFER_SIZE {
                buffer.pop_front();
            }
            buffer.push_back(latency_ns);
        }
        
        // Update rate tracking
        self.update_rate_tracking();
        self.last_updated.store(nano_timestamp() as u64, Ordering::Relaxed);
    }

    /// Record a successful order (backward compatible - no volume/fees)
    pub fn record_success_simple(&self, latency_ns: u64) {
        self.record_success(latency_ns, 0.0, 0.0);
    }

    /// Record a failed order
    pub fn record_failure(&self) {
        self.total_orders.fetch_add(1, Ordering::Relaxed);
        self.failed_orders.fetch_add(1, Ordering::Relaxed);
        self.update_rate_tracking();
        self.last_updated.store(nano_timestamp() as u64, Ordering::Relaxed);
    }

    /// Record a cancelled order
    pub fn record_cancellation(&self) {
        self.total_orders.fetch_add(1, Ordering::Relaxed);
        self.cancelled_orders.fetch_add(1, Ordering::Relaxed);
        self.last_updated.store(nano_timestamp() as u64, Ordering::Relaxed);
    }

    /// Update rate tracking (orders per second)
    fn update_rate_tracking(&self) {
        let now = nano_timestamp() as u64;
        let window_start = self.rate_window_start_ns.load(Ordering::Relaxed);
        
        // If more than 1 second has passed, reset the window
        if now - window_start >= 1_000_000_000 {
            let current_rate = self.orders_in_last_second.swap(1, Ordering::Relaxed);
            // Update peak if this second was higher
            self.peak_orders_per_second.fetch_max(current_rate, Ordering::Relaxed);
            self.rate_window_start_ns.store(now, Ordering::Relaxed);
        } else {
            self.orders_in_last_second.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Set WebSocket connection status
    pub fn set_websocket_connected(&self, connected: bool) {
        self.websocket_connected.store(connected, Ordering::Relaxed);
    }

    /// Set connection pool metrics
    pub fn set_connection_pool(&self, size: u64, in_use: u64) {
        self.connection_pool_size.store(size, Ordering::Relaxed);
        self.connection_pool_in_use.store(in_use, Ordering::Relaxed);
    }

    /// Set rate limit metrics
    pub fn set_rate_limit(&self, remaining: u64, total: u64) {
        self.rate_limit_remaining.store(remaining, Ordering::Relaxed);
        self.rate_limit_total.store(total, Ordering::Relaxed);
    }

    /// Calculate percentiles from the latency buffer
    fn calculate_percentiles(&self) -> (u64, u64, u64, u64) {
        let buffer = match self.latency_buffer.lock() {
            Ok(b) => b,
            Err(poisoned) => poisoned.into_inner(),
        };
        
        if buffer.is_empty() {
            return (0, 0, 0, 0);
        }
        
        let mut sorted: Vec<u64> = buffer.iter().cloned().collect();
        sorted.sort_unstable();
        
        let len = sorted.len();
        let p50 = sorted[(len as f64 * 0.50) as usize];
        let p95 = sorted[((len as f64 * 0.95) as usize).min(len - 1)];
        let p99 = sorted[((len as f64 * 0.99) as usize).min(len - 1)];
        let p999 = sorted[((len as f64 * 0.999) as usize).min(len - 1)];
        
        (p50, p95, p99, p999)
    }

    pub fn get_metrics(&self, exchange_name: String) -> ExecutionMetrics {
        let total = self.total_orders.load(Ordering::Relaxed);
        let successful = self.successful_orders.load(Ordering::Relaxed);
        let failed = self.failed_orders.load(Ordering::Relaxed);
        let cancelled = self.cancelled_orders.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ns.load(Ordering::Relaxed);
        let min_latency = self.min_latency_ns.load(Ordering::Relaxed);
        let max_latency = self.max_latency_ns.load(Ordering::Relaxed);
        
        let (p50, p95, p99, p999) = self.calculate_percentiles();
        
        // Calculate orders per second
        let started_at = self.started_at.load(Ordering::Relaxed);
        let now = nano_timestamp() as u64;
        let elapsed_secs = (now - started_at) as f64 / 1_000_000_000.0;
        let orders_per_second = if elapsed_secs > 0.0 { total as f64 / elapsed_secs } else { 0.0 };
        
        // Connection pool utilization
        let pool_size = self.connection_pool_size.load(Ordering::Relaxed);
        let pool_in_use = self.connection_pool_in_use.load(Ordering::Relaxed);
        let pool_utilization = if pool_size > 0 { pool_in_use as f64 / pool_size as f64 } else { 0.0 };
        
        // Rate limit utilization
        let rate_total = self.rate_limit_total.load(Ordering::Relaxed);
        let rate_remaining = self.rate_limit_remaining.load(Ordering::Relaxed);
        let rate_utilization = if rate_total > 0 { 
            (rate_total - rate_remaining) as f64 / rate_total as f64 
        } else { 
            0.0 
        };

        ExecutionMetrics {
            exchange: exchange_name,
            total_orders: total,
            successful_orders: successful,
            failed_orders: failed,
            cancelled_orders: cancelled,
            avg_latency_ns: if successful > 0 { total_latency / successful } else { 0 },
            min_latency_ns: if min_latency == u64::MAX { 0 } else { min_latency },
            max_latency_ns: max_latency,
            p50_latency_ns: p50,
            p95_latency_ns: p95,
            p99_latency_ns: p99,
            p999_latency_ns: p999,
            total_volume: self.total_volume_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            total_fees: self.total_fees_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            fill_rate: if total > 0 { successful as f64 / total as f64 } else { 0.0 },
            error_rate: if total > 0 { failed as f64 / total as f64 } else { 0.0 },
            orders_per_second,
            last_updated: self.last_updated.load(Ordering::Relaxed) as u128,
            websocket_connected: self.websocket_connected.load(Ordering::Relaxed),
            connection_pool_utilization: pool_utilization,
            rate_limit_utilization: rate_utilization,
        }
    }

    /// Get peak orders per second
    pub fn get_peak_orders_per_second(&self) -> u64 {
        self.peak_orders_per_second.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        let now = nano_timestamp() as u64;
        self.total_orders.store(0, Ordering::Relaxed);
        self.successful_orders.store(0, Ordering::Relaxed);
        self.failed_orders.store(0, Ordering::Relaxed);
        self.cancelled_orders.store(0, Ordering::Relaxed);
        self.total_latency_ns.store(0, Ordering::Relaxed);
        self.min_latency_ns.store(u64::MAX, Ordering::Relaxed);
        self.max_latency_ns.store(0, Ordering::Relaxed);
        self.total_volume_micros.store(0, Ordering::Relaxed);
        self.total_fees_micros.store(0, Ordering::Relaxed);
        self.orders_in_last_second.store(0, Ordering::Relaxed);
        self.rate_window_start_ns.store(now, Ordering::Relaxed);
        self.peak_orders_per_second.store(0, Ordering::Relaxed);
        self.started_at.store(now, Ordering::Relaxed);
        self.last_updated.store(now, Ordering::Relaxed);
        
        if let Ok(mut buffer) = self.latency_buffer.lock() {
            buffer.clear();
        }
    }
}

