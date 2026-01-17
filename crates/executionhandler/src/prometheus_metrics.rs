//! Prometheus Metrics Exporter
//!
//! Exports trading metrics in Prometheus format for monitoring and alerting.
//! Supports latency histograms, order counts, PnL tracking, and circuit breaker status.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;
use once_cell::sync::Lazy;

/// Global metrics registry
pub static METRICS: Lazy<MetricsRegistry> = Lazy::new(MetricsRegistry::new);

/// Prometheus-style metrics registry
pub struct MetricsRegistry {
    /// Counter metrics
    counters: RwLock<HashMap<String, AtomicU64>>,
    /// Gauge metrics (can go up and down)
    gauges: RwLock<HashMap<String, AtomicU64>>,
    /// Histogram buckets for latency tracking
    histograms: RwLock<HashMap<String, HistogramData>>,
    /// Labels for multi-dimensional metrics
    labeled_counters: RwLock<HashMap<String, HashMap<String, AtomicU64>>>,
    labeled_gauges: RwLock<HashMap<String, HashMap<String, AtomicU64>>>,
}

/// Histogram data with buckets
pub struct HistogramData {
    /// Bucket boundaries in microseconds
    buckets: Vec<u64>,
    /// Count in each bucket (cumulative)
    bucket_counts: Vec<AtomicU64>,
    /// Total sum of all observations
    sum: AtomicU64,
    /// Total count of observations
    count: AtomicU64,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let registry = Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            labeled_counters: RwLock::new(HashMap::new()),
            labeled_gauges: RwLock::new(HashMap::new()),
        };

        // Initialize standard trading metrics
        registry.init_standard_metrics();
        registry
    }

    fn init_standard_metrics(&self) {
        // Order counters
        self.register_counter("trading_orders_submitted_total");
        self.register_counter("trading_orders_filled_total");
        self.register_counter("trading_orders_rejected_total");
        self.register_counter("trading_orders_cancelled_total");
        self.register_counter("trading_orders_failed_total");

        // Connection metrics
        self.register_counter("exchange_websocket_messages_total");
        self.register_counter("exchange_websocket_errors_total");
        self.register_counter("exchange_rest_requests_total");
        self.register_counter("exchange_rest_errors_total");

        // Risk metrics
        self.register_gauge("risk_kill_switch_active");
        self.register_gauge("risk_circuit_breaker_active");
        self.register_gauge("risk_daily_loss_usd");
        self.register_gauge("risk_max_drawdown_pct");
        self.register_gauge("risk_open_positions_count");
        self.register_gauge("risk_total_position_value_usd");

        // PnL metrics
        self.register_gauge("trading_realized_pnl_usd");
        self.register_gauge("trading_unrealized_pnl_usd");
        self.register_gauge("trading_total_pnl_usd");
        self.register_gauge("trading_fees_paid_usd");

        // Latency histograms (in microseconds)
        // Buckets: 50us, 100us, 250us, 500us, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s
        let latency_buckets = vec![
            50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 
            25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
        ];
        self.register_histogram("trading_order_latency_us", latency_buckets.clone());
        self.register_histogram("trading_fill_latency_us", latency_buckets.clone());
        self.register_histogram("exchange_api_latency_us", latency_buckets.clone());
        self.register_histogram("signal_processing_latency_us", latency_buckets);

        // Labeled counters for per-exchange metrics
        self.register_labeled_counter("exchange_orders_total");
        self.register_labeled_counter("exchange_fills_total");
        self.register_labeled_counter("exchange_errors_total");

        // Labeled gauges for per-symbol metrics
        self.register_labeled_gauge("symbol_position_quantity");
        self.register_labeled_gauge("symbol_position_value_usd");
        self.register_labeled_gauge("symbol_unrealized_pnl_usd");
    }

    // Counter operations
    pub fn register_counter(&self, name: &str) {
        let mut counters = self.counters.write();
        counters.entry(name.to_string()).or_insert_with(|| AtomicU64::new(0));
    }

    pub fn inc_counter(&self, name: &str) {
        self.add_counter(name, 1);
    }

    pub fn add_counter(&self, name: &str, value: u64) {
        let counters = self.counters.read();
        if let Some(counter) = counters.get(name) {
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }

    pub fn get_counter(&self, name: &str) -> u64 {
        let counters = self.counters.read();
        counters.get(name).map(|c| c.load(Ordering::Relaxed)).unwrap_or(0)
    }

    // Gauge operations
    pub fn register_gauge(&self, name: &str) {
        let mut gauges = self.gauges.write();
        gauges.entry(name.to_string()).or_insert_with(|| AtomicU64::new(0));
    }

    pub fn set_gauge(&self, name: &str, value: u64) {
        let gauges = self.gauges.read();
        if let Some(gauge) = gauges.get(name) {
            gauge.store(value, Ordering::Relaxed);
        }
    }

    pub fn set_gauge_f64(&self, name: &str, value: f64) {
        // Store as fixed-point with 6 decimal places
        let fixed = (value * 1_000_000.0) as u64;
        self.set_gauge(name, fixed);
    }

    pub fn get_gauge(&self, name: &str) -> u64 {
        let gauges = self.gauges.read();
        gauges.get(name).map(|g| g.load(Ordering::Relaxed)).unwrap_or(0)
    }

    pub fn get_gauge_f64(&self, name: &str) -> f64 {
        self.get_gauge(name) as f64 / 1_000_000.0
    }

    pub fn inc_gauge(&self, name: &str) {
        let gauges = self.gauges.read();
        if let Some(gauge) = gauges.get(name) {
            gauge.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dec_gauge(&self, name: &str) {
        let gauges = self.gauges.read();
        if let Some(gauge) = gauges.get(name) {
            // Saturating subtraction
            loop {
                let current = gauge.load(Ordering::Relaxed);
                if current == 0 {
                    break;
                }
                if gauge.compare_exchange(current, current - 1, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                    break;
                }
            }
        }
    }

    // Histogram operations
    pub fn register_histogram(&self, name: &str, buckets: Vec<u64>) {
        let mut histograms = self.histograms.write();
        histograms.entry(name.to_string()).or_insert_with(|| HistogramData {
            bucket_counts: buckets.iter().map(|_| AtomicU64::new(0)).collect(),
            buckets,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        });
    }

    pub fn observe_histogram(&self, name: &str, value: u64) {
        let histograms = self.histograms.read();
        if let Some(hist) = histograms.get(name) {
            // Increment count and sum
            hist.count.fetch_add(1, Ordering::Relaxed);
            hist.sum.fetch_add(value, Ordering::Relaxed);

            // Increment appropriate bucket (cumulative)
            for (i, &bucket) in hist.buckets.iter().enumerate() {
                if value <= bucket {
                    hist.bucket_counts[i].fetch_add(1, Ordering::Relaxed);
                }
            }
            // +Inf bucket (always increment last if we have buckets)
            if let Some(last_bucket) = hist.bucket_counts.last() {
                last_bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // Labeled counter operations
    pub fn register_labeled_counter(&self, name: &str) {
        let mut labeled = self.labeled_counters.write();
        labeled.entry(name.to_string()).or_insert_with(HashMap::new);
    }

    pub fn inc_labeled_counter(&self, name: &str, label: &str) {
        let mut labeled = self.labeled_counters.write();
        if let Some(counters) = labeled.get_mut(name) {
            counters.entry(label.to_string())
                .or_insert_with(|| AtomicU64::new(0))
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    // Labeled gauge operations
    pub fn register_labeled_gauge(&self, name: &str) {
        let mut labeled = self.labeled_gauges.write();
        labeled.entry(name.to_string()).or_insert_with(HashMap::new);
    }

    pub fn set_labeled_gauge(&self, name: &str, label: &str, value: u64) {
        let mut labeled = self.labeled_gauges.write();
        if let Some(gauges) = labeled.get_mut(name) {
            gauges.entry(label.to_string())
                .or_insert_with(|| AtomicU64::new(0))
                .store(value, Ordering::Relaxed);
        }
    }

    pub fn set_labeled_gauge_f64(&self, name: &str, label: &str, value: f64) {
        let fixed = (value * 1_000_000.0) as u64;
        self.set_labeled_gauge(name, label, fixed);
    }

    /// Export all metrics in Prometheus text format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::with_capacity(8192);

        // Export counters
        {
            let counters = self.counters.read();
            for (name, counter) in counters.iter() {
                output.push_str(&format!(
                    "# TYPE {} counter\n{} {}\n",
                    name, name, counter.load(Ordering::Relaxed)
                ));
            }
        }

        // Export gauges
        {
            let gauges = self.gauges.read();
            for (name, gauge) in gauges.iter() {
                let value = gauge.load(Ordering::Relaxed);
                // Check if this is a float gauge (contains "usd" or "pct")
                if name.contains("usd") || name.contains("pct") || name.contains("pnl") {
                    output.push_str(&format!(
                        "# TYPE {} gauge\n{} {:.6}\n",
                        name, name, value as f64 / 1_000_000.0
                    ));
                } else {
                    output.push_str(&format!(
                        "# TYPE {} gauge\n{} {}\n",
                        name, name, value
                    ));
                }
            }
        }

        // Export histograms
        {
            let histograms = self.histograms.read();
            for (name, hist) in histograms.iter() {
                output.push_str(&format!("# TYPE {} histogram\n", name));
                
                let mut cumulative = 0u64;
                for (i, &bucket) in hist.buckets.iter().enumerate() {
                    cumulative += hist.bucket_counts[i].load(Ordering::Relaxed);
                    output.push_str(&format!(
                        "{}{{le=\"{}\"}} {}\n",
                        name, bucket, cumulative
                    ));
                }
                output.push_str(&format!("{}{{le=\"+Inf\"}} {}\n", name, hist.count.load(Ordering::Relaxed)));
                output.push_str(&format!("{}_sum {}\n", name, hist.sum.load(Ordering::Relaxed)));
                output.push_str(&format!("{}_count {}\n", name, hist.count.load(Ordering::Relaxed)));
            }
        }

        // Export labeled counters
        {
            let labeled = self.labeled_counters.read();
            for (name, labels) in labeled.iter() {
                if !labels.is_empty() {
                    output.push_str(&format!("# TYPE {} counter\n", name));
                    for (label, counter) in labels.iter() {
                        output.push_str(&format!(
                            "{}{{label=\"{}\"}} {}\n",
                            name, label, counter.load(Ordering::Relaxed)
                        ));
                    }
                }
            }
        }

        // Export labeled gauges
        {
            let labeled = self.labeled_gauges.read();
            for (name, labels) in labeled.iter() {
                if !labels.is_empty() {
                    output.push_str(&format!("# TYPE {} gauge\n", name));
                    for (label, gauge) in labels.iter() {
                        let value = gauge.load(Ordering::Relaxed);
                        // Float conversion for USD/PnL metrics
                        if name.contains("usd") || name.contains("pnl") || name.contains("value") {
                            output.push_str(&format!(
                                "{}{{label=\"{}\"}} {:.6}\n",
                                name, label, value as f64 / 1_000_000.0
                            ));
                        } else {
                            output.push_str(&format!(
                                "{}{{label=\"{}\"}} {}\n",
                                name, label, value
                            ));
                        }
                    }
                }
            }
        }

        output
    }

    /// Export metrics as JSON for debugging
    pub fn export_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        // Counters
        {
            let counters = self.counters.read();
            let mut counter_map = serde_json::Map::new();
            for (name, counter) in counters.iter() {
                counter_map.insert(name.clone(), serde_json::json!(counter.load(Ordering::Relaxed)));
            }
            result.insert("counters".to_string(), serde_json::Value::Object(counter_map));
        }

        // Gauges
        {
            let gauges = self.gauges.read();
            let mut gauge_map = serde_json::Map::new();
            for (name, gauge) in gauges.iter() {
                let value = gauge.load(Ordering::Relaxed);
                if name.contains("usd") || name.contains("pct") || name.contains("pnl") {
                    gauge_map.insert(name.clone(), serde_json::json!(value as f64 / 1_000_000.0));
                } else {
                    gauge_map.insert(name.clone(), serde_json::json!(value));
                }
            }
            result.insert("gauges".to_string(), serde_json::Value::Object(gauge_map));
        }

        serde_json::Value::Object(result)
    }

    /// Reset all metrics (useful for testing)
    pub fn reset(&self) {
        {
            let counters = self.counters.read();
            for counter in counters.values() {
                counter.store(0, Ordering::Relaxed);
            }
        }
        {
            let gauges = self.gauges.read();
            for gauge in gauges.values() {
                gauge.store(0, Ordering::Relaxed);
            }
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Convenience functions using global registry
pub fn inc_orders_submitted() {
    METRICS.inc_counter("trading_orders_submitted_total");
}

pub fn inc_orders_filled() {
    METRICS.inc_counter("trading_orders_filled_total");
}

pub fn inc_orders_rejected() {
    METRICS.inc_counter("trading_orders_rejected_total");
}

pub fn inc_orders_cancelled() {
    METRICS.inc_counter("trading_orders_cancelled_total");
}

pub fn inc_orders_failed() {
    METRICS.inc_counter("trading_orders_failed_total");
}

pub fn record_order_latency_us(latency_us: u64) {
    METRICS.observe_histogram("trading_order_latency_us", latency_us);
}

pub fn record_fill_latency_us(latency_us: u64) {
    METRICS.observe_histogram("trading_fill_latency_us", latency_us);
}

pub fn record_api_latency_us(latency_us: u64) {
    METRICS.observe_histogram("exchange_api_latency_us", latency_us);
}

pub fn set_kill_switch_active(active: bool) {
    METRICS.set_gauge("risk_kill_switch_active", if active { 1 } else { 0 });
}

pub fn set_circuit_breaker_active(active: bool) {
    METRICS.set_gauge("risk_circuit_breaker_active", if active { 1 } else { 0 });
}

pub fn set_daily_loss_usd(loss: f64) {
    METRICS.set_gauge_f64("risk_daily_loss_usd", loss);
}

pub fn set_total_pnl_usd(pnl: f64) {
    METRICS.set_gauge_f64("trading_total_pnl_usd", pnl);
}

pub fn set_realized_pnl_usd(pnl: f64) {
    METRICS.set_gauge_f64("trading_realized_pnl_usd", pnl);
}

pub fn set_unrealized_pnl_usd(pnl: f64) {
    METRICS.set_gauge_f64("trading_unrealized_pnl_usd", pnl);
}

pub fn set_position_count(count: u64) {
    METRICS.set_gauge("risk_open_positions_count", count);
}

pub fn set_total_position_value_usd(value: f64) {
    METRICS.set_gauge_f64("risk_total_position_value_usd", value);
}

pub fn inc_exchange_orders(exchange: &str) {
    METRICS.inc_labeled_counter("exchange_orders_total", exchange);
}

pub fn inc_exchange_fills(exchange: &str) {
    METRICS.inc_labeled_counter("exchange_fills_total", exchange);
}

pub fn inc_exchange_errors(exchange: &str) {
    METRICS.inc_labeled_counter("exchange_errors_total", exchange);
}

pub fn set_symbol_position(symbol: &str, quantity: f64, value_usd: f64) {
    METRICS.set_labeled_gauge_f64("symbol_position_quantity", symbol, quantity);
    METRICS.set_labeled_gauge_f64("symbol_position_value_usd", symbol, value_usd);
}

/// Get Prometheus formatted metrics string
pub fn get_prometheus_metrics() -> String {
    METRICS.export_prometheus()
}

/// Get metrics as JSON
pub fn get_metrics_json() -> serde_json::Value {
    METRICS.export_json()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_operations() {
        let registry = MetricsRegistry::new();
        
        assert_eq!(registry.get_counter("trading_orders_submitted_total"), 0);
        
        registry.inc_counter("trading_orders_submitted_total");
        assert_eq!(registry.get_counter("trading_orders_submitted_total"), 1);
        
        registry.add_counter("trading_orders_submitted_total", 5);
        assert_eq!(registry.get_counter("trading_orders_submitted_total"), 6);
    }

    #[test]
    fn test_gauge_operations() {
        let registry = MetricsRegistry::new();
        
        registry.set_gauge_f64("trading_total_pnl_usd", 1234.56);
        let value = registry.get_gauge_f64("trading_total_pnl_usd");
        assert!((value - 1234.56).abs() < 0.01);
    }

    #[test]
    fn test_histogram() {
        let registry = MetricsRegistry::new();
        
        // Record some latencies
        registry.observe_histogram("trading_order_latency_us", 100);
        registry.observe_histogram("trading_order_latency_us", 500);
        registry.observe_histogram("trading_order_latency_us", 1000);
        
        let output = registry.export_prometheus();
        assert!(output.contains("trading_order_latency_us"));
        assert!(output.contains("_sum"));
        assert!(output.contains("_count"));
    }

    #[test]
    fn test_prometheus_export() {
        let registry = MetricsRegistry::new();
        
        registry.inc_counter("trading_orders_submitted_total");
        registry.set_gauge("risk_kill_switch_active", 1);
        
        let output = registry.export_prometheus();
        assert!(output.contains("trading_orders_submitted_total 1"));
        assert!(output.contains("risk_kill_switch_active 1"));
    }
}
