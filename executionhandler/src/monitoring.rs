use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Performance monitoring and alerting system
pub struct PerformanceMonitor {
    metrics: Arc<RwLock<PerformanceMetrics>>,
    alerts: Arc<RwLock<Vec<Alert>>>,
    thresholds: MonitoringThresholds,
}

impl PerformanceMonitor {
    pub fn new(thresholds: MonitoringThresholds) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(PerformanceMetrics::new())),
            alerts: Arc::new(RwLock::new(Vec::new())),
            thresholds,
        }
    }

    /// Record latency measurement
    pub async fn record_latency(&self, component: &str, operation: &str, latency_ns: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.record_latency(component, operation, latency_ns);

        // Check for latency threshold violations
        if latency_ns > self.thresholds.max_latency_ns {
            self.create_alert(AlertType::HighLatency, 
                format!("High latency in {}/{}: {}ns", component, operation, latency_ns)).await;
        }
    }

    /// Record error
    pub async fn record_error(&self, component: &str, error_type: &str, message: &str) {
        let mut metrics = self.metrics.write().await;
        metrics.record_error(component, error_type);

        self.create_alert(AlertType::Error, 
            format!("Error in {}: {} - {}", component, error_type, message)).await;
    }

    /// Record throughput measurement
    pub async fn record_throughput(&self, component: &str, operations_per_second: f64) {
        let mut metrics = self.metrics.write().await;
        metrics.record_throughput(component, operations_per_second);

        // Check for throughput threshold violations
        if operations_per_second < self.thresholds.min_throughput {
            self.create_alert(AlertType::LowThroughput, 
                format!("Low throughput in {}: {:.2} ops/sec", component, operations_per_second)).await;
        }
    }

    /// Record memory usage
    pub async fn record_memory_usage(&self, component: &str, memory_bytes: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.record_memory_usage(component, memory_bytes);

        // Check for memory threshold violations
        if memory_bytes > self.thresholds.max_memory_bytes {
            self.create_alert(AlertType::HighMemoryUsage, 
                format!("High memory usage in {}: {} MB", component, memory_bytes / 1024 / 1024)).await;
        }
    }

    /// Create alert
    async fn create_alert(&self, alert_type: AlertType, message: String) {
        let alert = Alert {
            alert_type,
            message: message.clone(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            component: "Monitor".to_string(),
            acknowledged: false,
        };

        let mut alerts = self.alerts.write().await;
        alerts.push(alert.clone());

        // Log alert immediately
        match alert_type {
            AlertType::Error | AlertType::Critical => log::error!("ALERT: {}", message),
            AlertType::HighLatency | AlertType::LowThroughput | AlertType::HighMemoryUsage => {
                log::warn!("ALERT: {}", message);
            }
            AlertType::Info => log::info!("ALERT: {}", message),
        }
    }

    /// Get current metrics
    pub async fn get_metrics(&self) -> PerformanceMetrics {
        let metrics = self.metrics.read().await;
        metrics.clone()
    }

    /// Get recent alerts
    pub async fn get_recent_alerts(&self, max_age_ms: u64) -> Vec<Alert> {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let alerts = self.alerts.read().await;
        
        alerts.iter()
            .filter(|alert| current_time - alert.timestamp < max_age_ms)
            .cloned()
            .collect()
    }

    /// Acknowledge alert
    pub async fn acknowledge_alert(&self, alert_index: usize) -> Result<(), String> {
        let mut alerts = self.alerts.write().await;
        if let Some(alert) = alerts.get_mut(alert_index) {
            alert.acknowledged = true;
            Ok(())
        } else {
            Err("Alert index out of range".to_string())
        }
    }

    /// Generate performance report
    pub async fn generate_report(&self) -> PerformanceReport {
        let metrics = self.get_metrics().await;
        let alerts = self.get_recent_alerts(3600000).await; // Last hour
        
        PerformanceReport {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            metrics,
            recent_alerts: alerts,
            thresholds: self.thresholds.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub latencies: HashMap<String, ComponentLatency>, // component -> latency stats
    pub errors: HashMap<String, ErrorStats>,          // component -> error stats
    pub throughput: HashMap<String, ThroughputStats>, // component -> throughput stats
    pub memory_usage: HashMap<String, MemoryStats>,   // component -> memory stats
    pub last_updated: u64,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self {
            latencies: HashMap::new(),
            errors: HashMap::new(),
            throughput: HashMap::new(),
            memory_usage: HashMap::new(),
            last_updated: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
        }
    }

    pub fn record_latency(&mut self, component: &str, operation: &str, latency_ns: u64) {
        let key = format!("{}::{}", component, operation);
        let entry = self.latencies.entry(key).or_insert_with(ComponentLatency::new);
        entry.record_latency(latency_ns);
        self.last_updated = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    }

    pub fn record_error(&mut self, component: &str, error_type: &str) {
        let entry = self.errors.entry(component.to_string()).or_insert_with(ErrorStats::new);
        entry.record_error(error_type);
        self.last_updated = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    }

    pub fn record_throughput(&mut self, component: &str, ops_per_second: f64) {
        let entry = self.throughput.entry(component.to_string()).or_insert_with(ThroughputStats::new);
        entry.record_throughput(ops_per_second);
        self.last_updated = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    }

    pub fn record_memory_usage(&mut self, component: &str, memory_bytes: u64) {
        let entry = self.memory_usage.entry(component.to_string()).or_insert_with(MemoryStats::new);
        entry.record_memory_usage(memory_bytes);
        self.last_updated = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentLatency {
    pub min_ns: u64,
    pub max_ns: u64,
    pub avg_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub count: usize,
    samples: Vec<u64>, // Keep last 1000 samples for percentile calculation
}

impl ComponentLatency {
    pub fn new() -> Self {
        Self {
            min_ns: u64::MAX,
            max_ns: 0,
            avg_ns: 0,
            p50_ns: 0,
            p95_ns: 0,
            p99_ns: 0,
            count: 0,
            samples: Vec::with_capacity(1000),
        }
    }

    pub fn record_latency(&mut self, latency_ns: u64) {
        self.min_ns = self.min_ns.min(latency_ns);
        self.max_ns = self.max_ns.max(latency_ns);
        self.count += 1;

        // Add sample and maintain sliding window
        if self.samples.len() >= 1000 {
            self.samples.remove(0);
        }
        self.samples.push(latency_ns);

        // Recalculate percentiles
        self.calculate_percentiles();
    }

    fn calculate_percentiles(&mut self) {
        if self.samples.is_empty() {
            return;
        }

        let mut sorted_samples = self.samples.clone();
        sorted_samples.sort_unstable();

        let len = sorted_samples.len();
        self.avg_ns = sorted_samples.iter().sum::<u64>() / len as u64;
        self.p50_ns = sorted_samples[len / 2];
        self.p95_ns = sorted_samples[len * 95 / 100];
        self.p99_ns = sorted_samples[len * 99 / 100];
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorStats {
    pub total_errors: usize,
    pub errors_by_type: HashMap<String, usize>,
    pub error_rate: f64, // errors per minute
    pub last_error_time: u64,
}

impl ErrorStats {
    pub fn new() -> Self {
        Self {
            total_errors: 0,
            errors_by_type: HashMap::new(),
            error_rate: 0.0,
            last_error_time: 0,
        }
    }

    pub fn record_error(&mut self, error_type: &str) {
        self.total_errors += 1;
        *self.errors_by_type.entry(error_type.to_string()).or_insert(0) += 1;
        self.last_error_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        
        // Simple error rate calculation (could be more sophisticated)
        self.error_rate = self.total_errors as f64 / 60.0; // Assuming 1 minute window
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputStats {
    pub current_ops_per_second: f64,
    pub peak_ops_per_second: f64,
    pub avg_ops_per_second: f64,
    pub sample_count: usize,
    samples: Vec<f64>,
}

impl ThroughputStats {
    pub fn new() -> Self {
        Self {
            current_ops_per_second: 0.0,
            peak_ops_per_second: 0.0,
            avg_ops_per_second: 0.0,
            sample_count: 0,
            samples: Vec::with_capacity(100),
        }
    }

    pub fn record_throughput(&mut self, ops_per_second: f64) {
        self.current_ops_per_second = ops_per_second;
        self.peak_ops_per_second = self.peak_ops_per_second.max(ops_per_second);
        self.sample_count += 1;

        // Maintain sliding window
        if self.samples.len() >= 100 {
            self.samples.remove(0);
        }
        self.samples.push(ops_per_second);

        // Calculate average
        self.avg_ops_per_second = self.samples.iter().sum::<f64>() / self.samples.len() as f64;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStats {
    pub current_bytes: u64,
    pub peak_bytes: u64,
    pub avg_bytes: u64,
    pub sample_count: usize,
    samples: Vec<u64>,
}

impl MemoryStats {
    pub fn new() -> Self {
        Self {
            current_bytes: 0,
            peak_bytes: 0,
            avg_bytes: 0,
            sample_count: 0,
            samples: Vec::with_capacity(100),
        }
    }

    pub fn record_memory_usage(&mut self, memory_bytes: u64) {
        self.current_bytes = memory_bytes;
        self.peak_bytes = self.peak_bytes.max(memory_bytes);
        self.sample_count += 1;

        // Maintain sliding window
        if self.samples.len() >= 100 {
            self.samples.remove(0);
        }
        self.samples.push(memory_bytes);

        // Calculate average
        self.avg_bytes = self.samples.iter().sum::<u64>() / self.samples.len() as u64;
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MonitoringThresholds {
    pub max_latency_ns: u64,
    pub min_throughput: f64,
    pub max_memory_bytes: u64,
    pub max_error_rate: f64,
}

impl Default for MonitoringThresholds {
    fn default() -> Self {
        Self {
            max_latency_ns: 1_000_000,    // 1ms
            min_throughput: 100.0,        // 100 ops/sec
            max_memory_bytes: 1024 * 1024 * 512, // 512MB
            max_error_rate: 0.01,         // 1% error rate
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub alert_type: AlertType,
    pub message: String,
    pub timestamp: u64,
    pub component: String,
    pub acknowledged: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum AlertType {
    Error,
    Critical,
    HighLatency,
    LowThroughput,
    HighMemoryUsage,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub timestamp: u64,
    pub metrics: PerformanceMetrics,
    pub recent_alerts: Vec<Alert>,
    pub thresholds: MonitoringThresholds,
}

/// Structured logging utility for trading system
pub struct TradingLogger;

impl TradingLogger {
    /// Log order execution
    pub fn log_order_execution(
        exchange: &str,
        symbol: &str,
        side: &str,
        quantity: f64,
        price: f64,
        order_id: &str,
        latency_ns: u64,
        status: &str,
    ) {
        log::info!(
            target: "trading::execution",
            "ORDER_EXECUTION exchange={} symbol={} side={} quantity={} price={} order_id={} latency_ns={} status={}",
            exchange, symbol, side, quantity, price, order_id, latency_ns, status
        );
    }

    /// Log position update
    pub fn log_position_update(
        symbol: &str,
        exchange: &str,
        old_quantity: f64,
        new_quantity: f64,
        price: f64,
        realized_pnl: f64,
    ) {
        log::info!(
            target: "trading::position",
            "POSITION_UPDATE symbol={} exchange={} old_quantity={} new_quantity={} price={} realized_pnl={}",
            symbol, exchange, old_quantity, new_quantity, price, realized_pnl
        );
    }

    /// Log error with context
    pub fn log_error(component: &str, operation: &str, error: &str, context: Option<&str>) {
        if let Some(ctx) = context {
            log::error!(
                target: "trading::error",
                "ERROR component={} operation={} error=\"{}\" context=\"{}\"",
                component, operation, error, ctx
            );
        } else {
            log::error!(
                target: "trading::error",
                "ERROR component={} operation={} error=\"{}\"",
                component, operation, error
            );
        }
    }

    /// Log performance metrics
    pub fn log_performance_metrics(component: &str, metrics: &str) {
        log::info!(
            target: "trading::performance",
            "PERFORMANCE_METRICS component={} metrics={}",
            component, metrics
        );
    }

    /// Log signal generation
    pub fn log_signal(
        strategy: &str,
        symbol: &str,
        action: &str,
        confidence: f64,
        metadata: Option<&str>,
    ) {
        if let Some(meta) = metadata {
            log::info!(
                target: "trading::signal",
                "SIGNAL_GENERATED strategy={} symbol={} action={} confidence={} metadata=\"{}\"",
                strategy, symbol, action, confidence, meta
            );
        } else {
            log::info!(
                target: "trading::signal",
                "SIGNAL_GENERATED strategy={} symbol={} action={} confidence={}",
                strategy, symbol, action, confidence
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_performance_monitor() {
        let thresholds = MonitoringThresholds::default();
        let monitor = PerformanceMonitor::new(thresholds);

        // Record some metrics
        monitor.record_latency("execution", "order_placement", 500_000).await;
        monitor.record_latency("execution", "order_placement", 2_000_000).await; // Should trigger alert

        let metrics = monitor.get_metrics().await;
        assert!(!metrics.latencies.is_empty());

        let alerts = monitor.get_recent_alerts(60000).await;
        assert_eq!(alerts.len(), 1); // Should have high latency alert
    }

    #[test]
    fn test_component_latency() {
        let mut latency = ComponentLatency::new();
        
        // Add some samples
        for i in 1..=100 {
            latency.record_latency(i * 1000); // 1ms to 100ms
        }

        assert_eq!(latency.count, 100);
        assert!(latency.p99_ns > latency.p95_ns);
        assert!(latency.p95_ns > latency.p50_ns);
    }
}
