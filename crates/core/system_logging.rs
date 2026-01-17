//! Comprehensive Logging System for SignalEngine
//!
//! This module provides production-grade logging across all SignalEngine components.
//! 
//! # Log Categories
//! - **STARTUP**: System initialization and configuration
//! - **MARKET_DATA**: Incoming market data processing
//! - **STRATEGY**: Strategy loading, execution, and lifecycle
//! - **SIGNAL**: Signal generation and routing
//! - **EXECUTION**: Order execution and fills
//! - **RISK**: Risk checks and position limits
//! - **PORTFOLIO**: Portfolio updates and P&L
//! - **PERFORMANCE**: Latency and throughput metrics
//! - **ERROR**: Errors and failures
//!
//! # Log Levels
//! - DEBUG: Detailed diagnostic information
//! - INFO: General operational events
//! - WARN: Potential issues and degraded performance
//! - ERROR: Failures requiring attention
//!
//! # Usage
//! ```rust
//! use signalengine_core::system_logging::{SystemLogger, LogCategory};
//! 
//! let logger = SystemLogger::new("MyComponent");
//! logger.info(LogCategory::Strategy, "Strategy initialized", &[("id", "123")]);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Global metrics counters
pub static LOG_METRICS: LazyLock<LogMetrics> = LazyLock::new(LogMetrics::new);

/// Log categories for structured logging
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogCategory {
    Startup,
    Shutdown,
    Config,
    MarketData,
    Strategy,
    Signal,
    Execution,
    Risk,
    Portfolio,
    OrderBook,
    Exchange,
    Network,
    Database,
    Performance,
    Error,
    Audit,
}

impl LogCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogCategory::Startup => "STARTUP",
            LogCategory::Shutdown => "SHUTDOWN",
            LogCategory::Config => "CONFIG",
            LogCategory::MarketData => "MARKET_DATA",
            LogCategory::Strategy => "STRATEGY",
            LogCategory::Signal => "SIGNAL",
            LogCategory::Execution => "EXECUTION",
            LogCategory::Risk => "RISK",
            LogCategory::Portfolio => "PORTFOLIO",
            LogCategory::OrderBook => "ORDERBOOK",
            LogCategory::Exchange => "EXCHANGE",
            LogCategory::Network => "NETWORK",
            LogCategory::Database => "DATABASE",
            LogCategory::Performance => "PERFORMANCE",
            LogCategory::Error => "ERROR",
            LogCategory::Audit => "AUDIT",
        }
    }
}

/// Log level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
}

impl Level {
    pub fn as_str(&self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }
}

/// Structured log entry
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp_ns: u64,
    pub level: Level,
    pub category: LogCategory,
    pub component: String,
    pub message: String,
    pub fields: HashMap<String, String>,
}

impl LogEntry {
    pub fn to_json(&self) -> String {
        let mut json = format!(
            r#"{{"ts":{},"level":"{}","cat":"{}","comp":"{}","msg":"{}""#,
            self.timestamp_ns,
            self.level.as_str(),
            self.category.as_str(),
            self.component,
            self.message.replace('"', "\\\"")
        );
        
        for (key, value) in &self.fields {
            json.push_str(&format!(r#","{}":"{}""#, key, value.replace('"', "\\\"")));
        }
        
        json.push('}');
        json
    }
    
    pub fn to_human(&self) -> String {
        let ts = self.timestamp_ns / 1_000_000; // Convert to ms
        let fields_str: String = self.fields.iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" ");
        
        if fields_str.is_empty() {
            format!(
                "[{}] {} [{}] {} | {}",
                ts,
                self.level.as_str(),
                self.category.as_str(),
                self.component,
                self.message
            )
        } else {
            format!(
                "[{}] {} [{}] {} | {} | {}",
                ts,
                self.level.as_str(),
                self.category.as_str(),
                self.component,
                self.message,
                fields_str
            )
        }
    }
}

/// Log metrics for monitoring
pub struct LogMetrics {
    pub total_logs: AtomicU64,
    pub debug_count: AtomicU64,
    pub info_count: AtomicU64,
    pub warn_count: AtomicU64,
    pub error_count: AtomicU64,
    pub logs_per_category: RwLock<HashMap<LogCategory, u64>>,
}

impl LogMetrics {
    pub fn new() -> Self {
        Self {
            total_logs: AtomicU64::new(0),
            debug_count: AtomicU64::new(0),
            info_count: AtomicU64::new(0),
            warn_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            logs_per_category: RwLock::new(HashMap::new()),
        }
    }
    
    pub fn record(&self, level: Level, category: LogCategory) {
        self.total_logs.fetch_add(1, Ordering::Relaxed);
        
        match level {
            Level::Debug => self.debug_count.fetch_add(1, Ordering::Relaxed),
            Level::Info => self.info_count.fetch_add(1, Ordering::Relaxed),
            Level::Warn => self.warn_count.fetch_add(1, Ordering::Relaxed),
            Level::Error => self.error_count.fetch_add(1, Ordering::Relaxed),
        };
        
        if let Ok(mut cats) = self.logs_per_category.write() {
            *cats.entry(category).or_insert(0) += 1;
        }
    }
    
    pub fn get_stats(&self) -> LogStats {
        LogStats {
            total: self.total_logs.load(Ordering::Relaxed),
            debug: self.debug_count.load(Ordering::Relaxed),
            info: self.info_count.load(Ordering::Relaxed),
            warn: self.warn_count.load(Ordering::Relaxed),
            error: self.error_count.load(Ordering::Relaxed),
        }
    }
}

impl Default for LogMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct LogStats {
    pub total: u64,
    pub debug: u64,
    pub info: u64,
    pub warn: u64,
    pub error: u64,
}

/// System-wide logger with component context
pub struct SystemLogger {
    component: String,
    min_level: Level,
    output_json: bool,
}

impl SystemLogger {
    pub fn new(component: &str) -> Self {
        Self {
            component: component.to_string(),
            min_level: Level::Debug,
            output_json: std::env::var("LOG_FORMAT").map(|v| v == "json").unwrap_or(false),
        }
    }
    
    pub fn with_level(mut self, level: Level) -> Self {
        self.min_level = level;
        self
    }
    
    fn log(&self, level: Level, category: LogCategory, message: &str, fields: &[(&str, &str)]) {
        if level < self.min_level {
            return;
        }
        
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        let entry = LogEntry {
            timestamp_ns,
            level,
            category,
            component: self.component.clone(),
            message: message.to_string(),
            fields: fields.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        };
        
        LOG_METRICS.record(level, category);
        
        if self.output_json {
            println!("{}", entry.to_json());
        } else {
            println!("{}", entry.to_human());
        }
    }
    
    // ==================== DEBUG ====================
    
    pub fn debug(&self, category: LogCategory, message: &str, fields: &[(&str, &str)]) {
        self.log(Level::Debug, category, message, fields);
    }
    
    // ==================== INFO ====================
    
    pub fn info(&self, category: LogCategory, message: &str, fields: &[(&str, &str)]) {
        self.log(Level::Info, category, message, fields);
    }
    
    // ==================== WARN ====================
    
    pub fn warn(&self, category: LogCategory, message: &str, fields: &[(&str, &str)]) {
        self.log(Level::Warn, category, message, fields);
    }
    
    // ==================== ERROR ====================
    
    pub fn error(&self, category: LogCategory, message: &str, fields: &[(&str, &str)]) {
        self.log(Level::Error, category, message, fields);
    }
    
    // ==================== SPECIALIZED LOGGING ====================
    
    /// Log system startup
    pub fn startup(&self, message: &str) {
        self.info(LogCategory::Startup, message, &[]);
    }
    
    /// Log system shutdown
    pub fn shutdown(&self, message: &str) {
        self.info(LogCategory::Shutdown, message, &[]);
    }
    
    /// Log market data received
    pub fn market_data(&self, symbol: &str, exchange: &str, price: f64, latency_us: u64) {
        self.debug(LogCategory::MarketData, "Market data received", &[
            ("symbol", symbol),
            ("exchange", exchange),
            ("price", &format!("{:.8}", price)),
            ("latency_us", &latency_us.to_string()),
        ]);
    }
    
    /// Log trade execution
    pub fn trade_execution(
        &self,
        order_id: &str,
        symbol: &str,
        exchange: &str,
        side: &str,
        quantity: f64,
        price: f64,
        fees: f64,
        latency_ns: u64,
    ) {
        self.info(LogCategory::Execution, "Trade executed", &[
            ("order_id", order_id),
            ("symbol", symbol),
            ("exchange", exchange),
            ("side", side),
            ("quantity", &format!("{:.8}", quantity)),
            ("price", &format!("{:.8}", price)),
            ("fees", &format!("{:.8}", fees)),
            ("latency_ns", &latency_ns.to_string()),
        ]);
    }
    
    /// Log signal generation
    pub fn signal_generated(
        &self,
        signal_id: &str,
        strategy_id: &str,
        symbol: &str,
        action: &str,
        urgency: &str,
        quantity: f64,
    ) {
        self.info(LogCategory::Signal, "Signal generated", &[
            ("signal_id", signal_id),
            ("strategy_id", strategy_id),
            ("symbol", symbol),
            ("action", action),
            ("urgency", urgency),
            ("quantity", &format!("{:.8}", quantity)),
        ]);
    }
    
    /// Log signal routed
    pub fn signal_routed(&self, signal_id: &str, handler: &str) {
        self.debug(LogCategory::Signal, "Signal routed to handler", &[
            ("signal_id", signal_id),
            ("handler", handler),
        ]);
    }
    
    /// Log strategy loaded
    pub fn strategy_loaded(&self, strategy_id: &str, strategy_type: &str, symbols: &[String]) {
        self.info(LogCategory::Strategy, "Strategy loaded", &[
            ("strategy_id", strategy_id),
            ("strategy_type", strategy_type),
            ("symbols", &symbols.join(",")),
        ]);
    }
    
    /// Log strategy error
    pub fn strategy_error(&self, strategy_id: &str, error: &str) {
        self.error(LogCategory::Strategy, "Strategy error", &[
            ("strategy_id", strategy_id),
            ("error", error),
        ]);
    }
    
    /// Log risk check
    pub fn risk_check(
        &self,
        symbol: &str,
        check_type: &str,
        current_value: f64,
        limit: f64,
        passed: bool,
    ) {
        let level = if passed { Level::Debug } else { Level::Warn };
        self.log(level, LogCategory::Risk, "Risk check performed", &[
            ("symbol", symbol),
            ("check_type", check_type),
            ("current_value", &format!("{:.4}", current_value)),
            ("limit", &format!("{:.4}", limit)),
            ("passed", &passed.to_string()),
        ]);
    }
    
    /// Log risk breach
    pub fn risk_breach(&self, symbol: &str, breach_type: &str, current_value: f64, limit: f64) {
        self.error(LogCategory::Risk, "RISK BREACH DETECTED", &[
            ("symbol", symbol),
            ("breach_type", breach_type),
            ("current_value", &format!("{:.4}", current_value)),
            ("limit", &format!("{:.4}", limit)),
        ]);
    }
    
    /// Log portfolio update
    pub fn portfolio_update(
        &self,
        exchange: &str,
        total_value: f64,
        cash: f64,
        positions: usize,
        pnl: f64,
    ) {
        self.info(LogCategory::Portfolio, "Portfolio updated", &[
            ("exchange", exchange),
            ("total_value", &format!("{:.2}", total_value)),
            ("cash", &format!("{:.2}", cash)),
            ("positions", &positions.to_string()),
            ("pnl", &format!("{:.2}", pnl)),
        ]);
    }
    
    /// Log orderbook update
    pub fn orderbook_update(&self, symbol: &str, exchange: &str, bids: usize, asks: usize, spread_bps: f64) {
        self.debug(LogCategory::OrderBook, "Orderbook updated", &[
            ("symbol", symbol),
            ("exchange", exchange),
            ("bids", &bids.to_string()),
            ("asks", &asks.to_string()),
            ("spread_bps", &format!("{:.2}", spread_bps)),
        ]);
    }
    
    /// Log exchange connection
    pub fn exchange_connected(&self, exchange: &str, latency_ms: u64) {
        self.info(LogCategory::Exchange, "Exchange connected", &[
            ("exchange", exchange),
            ("latency_ms", &latency_ms.to_string()),
        ]);
    }
    
    /// Log exchange disconnection
    pub fn exchange_disconnected(&self, exchange: &str, reason: &str) {
        self.warn(LogCategory::Exchange, "Exchange disconnected", &[
            ("exchange", exchange),
            ("reason", reason),
        ]);
    }
    
    /// Log exchange error
    pub fn exchange_error(&self, exchange: &str, error: &str) {
        self.error(LogCategory::Exchange, "Exchange error", &[
            ("exchange", exchange),
            ("error", error),
        ]);
    }
    
    /// Log network latency
    pub fn network_latency(&self, endpoint: &str, latency_ms: u64, success: bool) {
        self.debug(LogCategory::Network, "Network request completed", &[
            ("endpoint", endpoint),
            ("latency_ms", &latency_ms.to_string()),
            ("success", &success.to_string()),
        ]);
    }
    
    /// Log database operation
    pub fn database_op(&self, operation: &str, table: &str, duration_ms: u64, success: bool) {
        self.debug(LogCategory::Database, "Database operation", &[
            ("operation", operation),
            ("table", table),
            ("duration_ms", &duration_ms.to_string()),
            ("success", &success.to_string()),
        ]);
    }
    
    /// Log performance metrics
    pub fn performance_metrics(
        &self,
        metric_type: &str,
        value: f64,
        unit: &str,
    ) {
        self.info(LogCategory::Performance, "Performance metric", &[
            ("metric_type", metric_type),
            ("value", &format!("{:.4}", value)),
            ("unit", unit),
        ]);
    }
    
    /// Log latency percentiles
    pub fn latency_percentiles(
        &self,
        component: &str,
        p50_ns: u64,
        p95_ns: u64,
        p99_ns: u64,
        p999_ns: u64,
    ) {
        self.info(LogCategory::Performance, "Latency percentiles", &[
            ("component", component),
            ("p50_ns", &p50_ns.to_string()),
            ("p95_ns", &p95_ns.to_string()),
            ("p99_ns", &p99_ns.to_string()),
            ("p999_ns", &p999_ns.to_string()),
        ]);
    }
    
    /// Log throughput
    pub fn throughput(&self, component: &str, ops_per_sec: f64, period_sec: u64) {
        self.info(LogCategory::Performance, "Throughput measurement", &[
            ("component", component),
            ("ops_per_sec", &format!("{:.2}", ops_per_sec)),
            ("period_sec", &period_sec.to_string()),
        ]);
    }
    
    /// Log audit event (always logged regardless of level)
    pub fn audit(&self, event_type: &str, user: &str, details: &str) {
        self.log(Level::Info, LogCategory::Audit, "Audit event", &[
            ("event_type", event_type),
            ("user", user),
            ("details", details),
        ]);
    }
    
    /// Log configuration loaded
    pub fn config_loaded(&self, config_path: &str, keys_loaded: usize) {
        self.info(LogCategory::Config, "Configuration loaded", &[
            ("config_path", config_path),
            ("keys_loaded", &keys_loaded.to_string()),
        ]);
    }
    
    /// Log configuration error
    pub fn config_error(&self, config_path: &str, error: &str) {
        self.error(LogCategory::Config, "Configuration error", &[
            ("config_path", config_path),
            ("error", error),
        ]);
    }
}

/// Convenience macros for logging without creating logger instance
#[macro_export]
macro_rules! sys_log {
    ($level:ident, $category:expr, $component:expr, $message:expr $(, $key:expr => $value:expr)*) => {{
        let logger = $crate::system_logging::SystemLogger::new($component);
        logger.$level($category, $message, &[$(($key, $value)),*]);
    }};
}

#[macro_export]
macro_rules! sys_info {
    ($category:expr, $component:expr, $message:expr $(, $key:expr => $value:expr)*) => {{
        sys_log!(info, $category, $component, $message $(, $key => $value)*);
    }};
}

#[macro_export]
macro_rules! sys_debug {
    ($category:expr, $component:expr, $message:expr $(, $key:expr => $value:expr)*) => {{
        sys_log!(debug, $category, $component, $message $(, $key => $value)*);
    }};
}

#[macro_export]
macro_rules! sys_warn {
    ($category:expr, $component:expr, $message:expr $(, $key:expr => $value:expr)*) => {{
        sys_log!(warn, $category, $component, $message $(, $key => $value)*);
    }};
}

#[macro_export]
macro_rules! sys_error {
    ($category:expr, $component:expr, $message:expr $(, $key:expr => $value:expr)*) => {{
        sys_log!(error, $category, $component, $message $(, $key => $value)*);
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_log_entry_json() {
        let entry = LogEntry {
            timestamp_ns: 1705000000000000000,
            level: Level::Info,
            category: LogCategory::Execution,
            component: "ExecutionHandler".to_string(),
            message: "Order filled".to_string(),
            fields: [
                ("order_id".to_string(), "12345".to_string()),
                ("price".to_string(), "50000.00".to_string()),
            ].into(),
        };
        
        let json = entry.to_json();
        assert!(json.contains("\"level\":\"INFO\""));
        assert!(json.contains("\"cat\":\"EXECUTION\""));
        assert!(json.contains("\"order_id\":\"12345\""));
    }
    
    #[test]
    fn test_log_entry_human() {
        let entry = LogEntry {
            timestamp_ns: 1705000000000000000,
            level: Level::Info,
            category: LogCategory::Signal,
            component: "SignalGenerator".to_string(),
            message: "Signal generated".to_string(),
            fields: HashMap::new(),
        };
        
        let human = entry.to_human();
        assert!(human.contains("INFO"));
        assert!(human.contains("SIGNAL"));
        assert!(human.contains("SignalGenerator"));
    }
    
    #[test]
    fn test_logger_methods() {
        let logger = SystemLogger::new("TestComponent");
        
        // These should not panic
        logger.startup("System starting");
        logger.market_data("BTC/USD", "kraken", 50000.0, 100);
        logger.signal_generated("sig-1", "strat-1", "BTC/USD", "BUY", "HIGH", 1.0);
        logger.trade_execution("ord-1", "BTC/USD", "kraken", "BUY", 1.0, 50000.0, 25.0, 1500);
        logger.risk_check("BTC/USD", "position_limit", 5.0, 10.0, true);
        logger.portfolio_update("kraken", 100000.0, 50000.0, 5, 5000.0);
        logger.shutdown("System stopped");
    }
    
    #[test]
    fn test_log_metrics() {
        let metrics = LogMetrics::new();
        
        metrics.record(Level::Info, LogCategory::Execution);
        metrics.record(Level::Error, LogCategory::Risk);
        metrics.record(Level::Debug, LogCategory::MarketData);
        
        let stats = metrics.get_stats();
        assert_eq!(stats.total, 3);
        assert_eq!(stats.info, 1);
        assert_eq!(stats.error, 1);
        assert_eq!(stats.debug, 1);
    }
}
