//! Audit Log Persistence
//!
//! Provides persistent audit logging for compliance and debugging:
//! - Order lifecycle events
//! - Execution results
//! - DLQ entries
//! - System events

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};

/// Audit event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditEventType {
    // Order lifecycle
    OrderCreated,
    OrderSubmitted,
    OrderAccepted,
    OrderRejected,
    OrderFilled,
    OrderPartiallyFilled,
    OrderCancelled,
    OrderExpired,
    
    // Execution events
    ExecutionStarted,
    ExecutionCompleted,
    ExecutionFailed,
    ExecutionRetried,
    
    // DLQ events
    DlqEntryCreated,
    DlqEntryRetried,
    DlqEntryResolved,
    DlqEntryDropped,
    
    // System events
    CircuitBreakerOpened,
    CircuitBreakerClosed,
    RateLimitExceeded,
    ConnectionEstablished,
    ConnectionLost,
    
    // Risk events
    RiskLimitBreached,
    PositionLiquidated,
    MarginCall,
}

impl AuditEventType {
    pub fn severity(&self) -> AuditSeverity {
        match self {
            // Critical
            Self::ExecutionFailed | Self::RiskLimitBreached | 
            Self::PositionLiquidated | Self::MarginCall |
            Self::DlqEntryDropped => AuditSeverity::Critical,
            
            // Warning
            Self::OrderRejected | Self::CircuitBreakerOpened |
            Self::RateLimitExceeded | Self::ConnectionLost |
            Self::DlqEntryCreated | Self::ExecutionRetried => AuditSeverity::Warning,
            
            // Info
            _ => AuditSeverity::Info,
        }
    }
}

/// Audit severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AuditSeverity {
    Debug = 0,
    Info = 1,
    Warning = 2,
    Critical = 3,
}

/// An audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Unique entry ID
    pub id: u64,
    /// Timestamp (nanoseconds since epoch)
    pub timestamp_ns: u64,
    /// Event type
    pub event_type: AuditEventType,
    /// Severity level
    pub severity: AuditSeverity,
    /// Related order ID (if applicable)
    pub order_id: Option<String>,
    /// Related trade ID (if applicable)
    pub trade_id: Option<String>,
    /// Symbol
    pub symbol: Option<String>,
    /// Exchange
    pub exchange: Option<String>,
    /// Side (buy/sell)
    pub side: Option<String>,
    /// Quantity
    pub quantity: Option<f64>,
    /// Price
    pub price: Option<f64>,
    /// Message/description
    pub message: String,
    /// Additional metadata as JSON
    pub metadata: serde_json::Value,
    /// Trace ID for correlation
    pub trace_id: Option<String>,
    /// User/API key that initiated
    pub initiator: Option<String>,
}

/// Entry counter for unique IDs
static ENTRY_COUNTER: AtomicU64 = AtomicU64::new(1);

impl AuditEntry {
    pub fn new(event_type: AuditEventType, message: impl Into<String>) -> Self {
        let id = ENTRY_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp_ns = crate::optimizations::timestamp::nano_timestamp() as u64;
        
        Self {
            id,
            timestamp_ns,
            event_type,
            severity: event_type.severity(),
            order_id: None,
            trade_id: None,
            symbol: None,
            exchange: None,
            side: None,
            quantity: None,
            price: None,
            message: message.into(),
            metadata: serde_json::json!({}),
            trace_id: None,
            initiator: None,
        }
    }

    pub fn with_order(mut self, order_id: &str) -> Self {
        self.order_id = Some(order_id.to_string());
        self
    }

    pub fn with_trade(mut self, trade_id: &str) -> Self {
        self.trade_id = Some(trade_id.to_string());
        self
    }

    pub fn with_symbol(mut self, symbol: &str) -> Self {
        self.symbol = Some(symbol.to_string());
        self
    }

    pub fn with_exchange(mut self, exchange: &str) -> Self {
        self.exchange = Some(exchange.to_string());
        self
    }

    pub fn with_side(mut self, side: &str) -> Self {
        self.side = Some(side.to_string());
        self
    }

    pub fn with_quantity(mut self, quantity: f64) -> Self {
        self.quantity = Some(quantity);
        self
    }

    pub fn with_price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_trace_id(mut self, trace_id: &str) -> Self {
        self.trace_id = Some(trace_id.to_string());
        self
    }

    pub fn with_initiator(mut self, initiator: &str) -> Self {
        self.initiator = Some(initiator.to_string());
        self
    }
}

/// Audit log backend trait
#[async_trait::async_trait]
pub trait AuditBackend: Send + Sync {
    /// Write entries to the backend
    async fn write(&self, entries: Vec<AuditEntry>) -> Result<()>;
    
    /// Query entries by criteria
    async fn query(&self, query: AuditQuery) -> Result<Vec<AuditEntry>>;
    
    /// Get entry count
    async fn count(&self, query: AuditQuery) -> Result<u64>;
}

/// Query criteria for audit logs
#[derive(Debug, Clone, Default)]
pub struct AuditQuery {
    /// Filter by event types
    pub event_types: Option<Vec<AuditEventType>>,
    /// Filter by minimum severity
    pub min_severity: Option<AuditSeverity>,
    /// Filter by order ID
    pub order_id: Option<String>,
    /// Filter by symbol
    pub symbol: Option<String>,
    /// Filter by exchange
    pub exchange: Option<String>,
    /// Start time (nanoseconds)
    pub start_time_ns: Option<u64>,
    /// End time (nanoseconds)
    pub end_time_ns: Option<u64>,
    /// Maximum results
    pub limit: Option<usize>,
    /// Offset for pagination
    pub offset: Option<usize>,
}

impl AuditQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_event_types(mut self, types: Vec<AuditEventType>) -> Self {
        self.event_types = Some(types);
        self
    }

    pub fn with_min_severity(mut self, severity: AuditSeverity) -> Self {
        self.min_severity = Some(severity);
        self
    }

    pub fn with_order_id(mut self, order_id: &str) -> Self {
        self.order_id = Some(order_id.to_string());
        self
    }

    pub fn with_time_range(mut self, start_ns: u64, end_ns: u64) -> Self {
        self.start_time_ns = Some(start_ns);
        self.end_time_ns = Some(end_ns);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// File-based audit backend (JSON lines format)
pub struct FileAuditBackend {
    path: std::path::PathBuf,
    max_file_size: u64,
    rotate_count: usize,
}

impl FileAuditBackend {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            path: path.into(),
            max_file_size: 100 * 1024 * 1024, // 100MB
            rotate_count: 5,
        }
    }

    pub fn with_rotation(mut self, max_size: u64, rotate_count: usize) -> Self {
        self.max_file_size = max_size;
        self.rotate_count = rotate_count;
        self
    }

    async fn maybe_rotate(&self) -> Result<()> {
        if let Ok(metadata) = tokio::fs::metadata(&self.path).await {
            if metadata.len() > self.max_file_size {
                // Rotate files
                for i in (0..self.rotate_count - 1).rev() {
                    let old_path = if i == 0 {
                        self.path.clone()
                    } else {
                        self.path.with_extension(format!("json.{}", i))
                    };
                    let new_path = self.path.with_extension(format!("json.{}", i + 1));
                    
                    if old_path.exists() {
                        let _ = tokio::fs::rename(&old_path, &new_path).await;
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AuditBackend for FileAuditBackend {
    async fn write(&self, entries: Vec<AuditEntry>) -> Result<()> {
        self.maybe_rotate().await?;
        
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .context("Failed to open audit log file")?;
        
        use tokio::io::AsyncWriteExt;
        
        for entry in entries {
            let line = serde_json::to_string(&entry)
                .context("Failed to serialize audit entry")?;
            file.write_all(line.as_bytes()).await?;
            file.write_all(b"\n").await?;
        }
        
        file.flush().await?;
        Ok(())
    }

    async fn query(&self, query: AuditQuery) -> Result<Vec<AuditEntry>> {
        let content = tokio::fs::read_to_string(&self.path)
            .await
            .unwrap_or_default();
        
        let mut results: Vec<AuditEntry> = Vec::new();
        
        for line in content.lines() {
            if line.is_empty() {
                continue;
            }
            
            let entry: AuditEntry = match serde_json::from_str(line) {
                Ok(e) => e,
                Err(_) => continue,
            };
            
            // Apply filters
            if let Some(ref types) = query.event_types {
                if !types.contains(&entry.event_type) {
                    continue;
                }
            }
            
            if let Some(min_sev) = query.min_severity {
                if entry.severity < min_sev {
                    continue;
                }
            }
            
            if let Some(ref order_id) = query.order_id {
                if entry.order_id.as_ref() != Some(order_id) {
                    continue;
                }
            }
            
            if let Some(start) = query.start_time_ns {
                if entry.timestamp_ns < start {
                    continue;
                }
            }
            
            if let Some(end) = query.end_time_ns {
                if entry.timestamp_ns > end {
                    continue;
                }
            }
            
            results.push(entry);
        }
        
        // Apply offset and limit
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(usize::MAX);
        
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }

    async fn count(&self, query: AuditQuery) -> Result<u64> {
        let results = self.query(query).await?;
        Ok(results.len() as u64)
    }
}

/// In-memory audit backend (for testing)
pub struct InMemoryAuditBackend {
    entries: tokio::sync::RwLock<Vec<AuditEntry>>,
    max_entries: usize,
}

impl InMemoryAuditBackend {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: tokio::sync::RwLock::new(Vec::with_capacity(max_entries)),
            max_entries,
        }
    }

    pub async fn clear(&self) {
        self.entries.write().await.clear();
    }

    pub async fn all_entries(&self) -> Vec<AuditEntry> {
        self.entries.read().await.clone()
    }
}

#[async_trait::async_trait]
impl AuditBackend for InMemoryAuditBackend {
    async fn write(&self, entries: Vec<AuditEntry>) -> Result<()> {
        let mut buffer = self.entries.write().await;
        for entry in entries {
            if buffer.len() >= self.max_entries {
                buffer.remove(0);
            }
            buffer.push(entry);
        }
        Ok(())
    }

    async fn query(&self, query: AuditQuery) -> Result<Vec<AuditEntry>> {
        let entries = self.entries.read().await;
        let mut results: Vec<AuditEntry> = entries
            .iter()
            .filter(|e| {
                if let Some(ref types) = query.event_types {
                    if !types.contains(&e.event_type) {
                        return false;
                    }
                }
                if let Some(min_sev) = query.min_severity {
                    if e.severity < min_sev {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(usize::MAX);
        
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }

    async fn count(&self, query: AuditQuery) -> Result<u64> {
        let results = self.query(query).await?;
        Ok(results.len() as u64)
    }
}

/// Audit logger with background flushing
pub struct AuditLogger {
    sender: mpsc::Sender<AuditEntry>,
    min_severity: AuditSeverity,
}

impl AuditLogger {
    /// Create a new audit logger with the specified backend
    pub fn new<B: AuditBackend + 'static>(
        backend: B,
        buffer_size: usize,
        flush_interval: Duration,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(buffer_size);
        
        // Spawn background flusher
        let backend = std::sync::Arc::new(backend);
        tokio::spawn(Self::flush_task(backend, receiver, flush_interval));
        
        Self {
            sender,
            min_severity: AuditSeverity::Info,
        }
    }

    /// Set minimum severity to log
    pub fn with_min_severity(mut self, severity: AuditSeverity) -> Self {
        self.min_severity = severity;
        self
    }

    async fn flush_task<B: AuditBackend>(
        backend: std::sync::Arc<B>,
        mut receiver: mpsc::Receiver<AuditEntry>,
        flush_interval: Duration,
    ) {
        let mut buffer: Vec<AuditEntry> = Vec::with_capacity(100);
        let mut interval = tokio::time::interval(flush_interval);
        
        loop {
            tokio::select! {
                entry = receiver.recv() => {
                    match entry {
                        Some(e) => {
                            buffer.push(e);
                            if buffer.len() >= 100 {
                                let entries = std::mem::take(&mut buffer);
                                if let Err(e) = backend.write(entries).await {
                                    log::error!("Failed to write audit log: {}", e);
                                }
                            }
                        }
                        None => break, // Channel closed
                    }
                }
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        let entries = std::mem::take(&mut buffer);
                        if let Err(e) = backend.write(entries).await {
                            log::error!("Failed to write audit log: {}", e);
                        }
                    }
                }
            }
        }
        
        // Final flush
        if !buffer.is_empty() {
            let _ = backend.write(buffer).await;
        }
    }

    /// Log an audit entry
    pub async fn log(&self, entry: AuditEntry) {
        if entry.severity >= self.min_severity {
            let _ = self.sender.send(entry).await;
        }
    }

    /// Log an order event
    pub async fn log_order_event(
        &self,
        event_type: AuditEventType,
        order_id: &str,
        symbol: &str,
        exchange: &str,
        message: &str,
    ) {
        let entry = AuditEntry::new(event_type, message)
            .with_order(order_id)
            .with_symbol(symbol)
            .with_exchange(exchange);
        self.log(entry).await;
    }

    /// Log an execution event
    pub async fn log_execution(
        &self,
        event_type: AuditEventType,
        order_id: &str,
        symbol: &str,
        exchange: &str,
        side: &str,
        quantity: f64,
        price: Option<f64>,
        message: &str,
    ) {
        let mut entry = AuditEntry::new(event_type, message)
            .with_order(order_id)
            .with_symbol(symbol)
            .with_exchange(exchange)
            .with_side(side)
            .with_quantity(quantity);
        
        if let Some(p) = price {
            entry = entry.with_price(p);
        }
        
        self.log(entry).await;
    }

    /// Log a system event
    pub async fn log_system_event(&self, event_type: AuditEventType, message: &str) {
        let entry = AuditEntry::new(event_type, message);
        self.log(entry).await;
    }
}

/// Convenience macros for audit logging
#[macro_export]
macro_rules! audit_order {
    ($logger:expr, $event:expr, $order_id:expr, $symbol:expr, $exchange:expr, $msg:expr) => {
        $logger.log_order_event($event, $order_id, $symbol, $exchange, $msg).await
    };
}

#[macro_export]
macro_rules! audit_execution {
    ($logger:expr, $event:expr, $order_id:expr, $symbol:expr, $exchange:expr, 
     $side:expr, $qty:expr, $price:expr, $msg:expr) => {
        $logger.log_execution($event, $order_id, $symbol, $exchange, $side, $qty, $price, $msg).await
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry_creation() {
        let entry = AuditEntry::new(AuditEventType::OrderCreated, "New order")
            .with_order("order123")
            .with_symbol("BTC/USD")
            .with_exchange("kraken")
            .with_side("buy")
            .with_quantity(1.5)
            .with_price(50000.0);
        
        assert_eq!(entry.order_id, Some("order123".to_string()));
        assert_eq!(entry.event_type, AuditEventType::OrderCreated);
        assert_eq!(entry.severity, AuditSeverity::Info);
    }

    #[test]
    fn test_audit_severity() {
        assert_eq!(AuditEventType::OrderCreated.severity(), AuditSeverity::Info);
        assert_eq!(AuditEventType::ExecutionFailed.severity(), AuditSeverity::Critical);
        assert_eq!(AuditEventType::CircuitBreakerOpened.severity(), AuditSeverity::Warning);
    }

    #[tokio::test]
    async fn test_in_memory_backend() {
        let backend = InMemoryAuditBackend::new(10);
        
        let entry = AuditEntry::new(AuditEventType::OrderCreated, "Test");
        backend.write(vec![entry]).await.unwrap();
        
        let results = backend.query(AuditQuery::new()).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_in_memory_backend_overflow() {
        let backend = InMemoryAuditBackend::new(3);
        
        for i in 0..5 {
            let entry = AuditEntry::new(AuditEventType::OrderCreated, format!("Order {}", i));
            backend.write(vec![entry]).await.unwrap();
        }
        
        let results = backend.query(AuditQuery::new()).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].message.contains("2")); // First entry should be "Order 2"
    }

    #[tokio::test]
    async fn test_audit_query_filter() {
        let backend = InMemoryAuditBackend::new(100);
        
        // Add mixed events
        backend.write(vec![
            AuditEntry::new(AuditEventType::OrderCreated, "Created"),
            AuditEntry::new(AuditEventType::ExecutionFailed, "Failed"),
            AuditEntry::new(AuditEventType::OrderFilled, "Filled"),
        ]).await.unwrap();
        
        // Query only critical events
        let query = AuditQuery::new().with_min_severity(AuditSeverity::Critical);
        let results = backend.query(query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].event_type, AuditEventType::ExecutionFailed);
    }

    #[tokio::test]
    async fn test_audit_logger() {
        let backend = InMemoryAuditBackend::new(100);
        let backend = std::sync::Arc::new(backend);
        let backend_ref = backend.clone();
        
        let logger = AuditLogger::new(
            InMemoryAuditBackend::new(100), // Use fresh backend for logger
            100,
            Duration::from_millis(10),
        );
        
        logger.log_order_event(
            AuditEventType::OrderCreated,
            "order123",
            "BTC/USD",
            "kraken",
            "New order created",
        ).await;
        
        // Wait for flush
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[test]
    fn test_audit_entry_serialization() {
        let entry = AuditEntry::new(AuditEventType::ExecutionCompleted, "Done")
            .with_order("order123")
            .with_metadata(serde_json::json!({"filled": true}));
        
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: AuditEntry = serde_json::from_str(&json).unwrap();
        
        assert_eq!(parsed.order_id, entry.order_id);
        assert_eq!(parsed.event_type, entry.event_type);
    }
}
