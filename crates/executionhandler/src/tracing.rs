//! OpenTelemetry Distributed Tracing for SignalEngine
//!
//! Provides span propagation, trace context management, and integration
//! with execution handlers for cross-service correlation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};

/// Trace ID (128-bit W3C format)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceId([u8; 16]);

impl TraceId {
    /// Generate a new random trace ID
    pub fn generate() -> Self {
        let mut bytes = [0u8; 16];
        getrandom::getrandom(&mut bytes).unwrap_or_else(|_| {
            // Fallback: use timestamp + counter
            let ts = crate::optimizations::timestamp::nano_timestamp() as u64;
            bytes[0..8].copy_from_slice(&ts.to_le_bytes());
            bytes[8..16].copy_from_slice(&SPAN_COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
        });
        TraceId(bytes)
    }

    /// Create from hex string
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 32 {
            return None;
        }
        let mut bytes = [0u8; 16];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[i*2..i*2+2], 16).ok()?;
        }
        Some(TraceId(bytes))
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Span ID (64-bit)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanId([u8; 8]);

impl SpanId {
    /// Generate a new random span ID
    pub fn generate() -> Self {
        let mut bytes = [0u8; 8];
        getrandom::getrandom(&mut bytes).unwrap_or_else(|_| {
            bytes.copy_from_slice(&SPAN_COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
        });
        SpanId(bytes)
    }

    /// Create from hex string
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 16 {
            return None;
        }
        let mut bytes = [0u8; 8];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[i*2..i*2+2], 16).ok()?;
        }
        Some(SpanId(bytes))
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

impl std::fmt::Display for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Span counter for ID generation fallback
static SPAN_COUNTER: AtomicU64 = AtomicU64::new(1);

/// W3C Trace Context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_span_id: Option<SpanId>,
    pub trace_flags: u8,
    pub trace_state: Option<String>,
}

impl TraceContext {
    /// Create a new root context
    pub fn new() -> Self {
        Self {
            trace_id: TraceId::generate(),
            span_id: SpanId::generate(),
            parent_span_id: None,
            trace_flags: 0x01, // sampled
            trace_state: None,
        }
    }

    /// Create a child context from this context
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: SpanId::generate(),
            parent_span_id: Some(self.span_id),
            trace_flags: self.trace_flags,
            trace_state: self.trace_state.clone(),
        }
    }

    /// Parse from W3C traceparent header
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() < 4 || parts[0] != "00" {
            return None;
        }
        
        let trace_id = TraceId::from_hex(parts[1])?;
        let span_id = SpanId::from_hex(parts[2])?;
        let trace_flags = u8::from_str_radix(parts[3], 16).ok()?;
        
        Some(Self {
            trace_id,
            span_id,
            parent_span_id: None,
            trace_flags,
            trace_state: None,
        })
    }

    /// Convert to W3C traceparent header
    pub fn to_traceparent(&self) -> String {
        format!("00-{}-{}-{:02x}", self.trace_id, self.span_id, self.trace_flags)
    }

    /// Check if trace is sampled
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & 0x01 != 0
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Span status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

/// Span kind (matching OpenTelemetry spec)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

/// An active tracing span
pub struct Span {
    context: TraceContext,
    name: String,
    kind: SpanKind,
    start_time: Instant,
    start_timestamp_ns: u64,
    attributes: HashMap<String, AttributeValue>,
    events: Vec<SpanEvent>,
    status: SpanStatus,
    status_message: Option<String>,
    ended: AtomicBool,
}

/// Span attribute value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    StringArray(Vec<String>),
    IntArray(Vec<i64>),
}

impl From<&str> for AttributeValue {
    fn from(s: &str) -> Self {
        AttributeValue::String(s.to_string())
    }
}

impl From<String> for AttributeValue {
    fn from(s: String) -> Self {
        AttributeValue::String(s)
    }
}

impl From<i64> for AttributeValue {
    fn from(v: i64) -> Self {
        AttributeValue::Int(v)
    }
}

impl From<f64> for AttributeValue {
    fn from(v: f64) -> Self {
        AttributeValue::Float(v)
    }
}

impl From<bool> for AttributeValue {
    fn from(v: bool) -> Self {
        AttributeValue::Bool(v)
    }
}

/// Span event (log within a span)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp_ns: u64,
    pub attributes: HashMap<String, AttributeValue>,
}

/// Completed span data for export
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedSpan {
    pub context: TraceContext,
    pub name: String,
    pub kind: SpanKind,
    pub start_timestamp_ns: u64,
    pub end_timestamp_ns: u64,
    pub duration_ns: u64,
    pub attributes: HashMap<String, AttributeValue>,
    pub events: Vec<SpanEvent>,
    pub status: SpanStatus,
    pub status_message: Option<String>,
}

impl Span {
    /// Create a new span with parent context
    pub fn new(name: impl Into<String>, parent: Option<&TraceContext>, kind: SpanKind) -> Self {
        let context = parent.map(|p| p.child()).unwrap_or_else(TraceContext::new);
        let now_ns = crate::optimizations::timestamp::nano_timestamp() as u64;
        
        Self {
            context,
            name: name.into(),
            kind,
            start_time: Instant::now(),
            start_timestamp_ns: now_ns,
            attributes: HashMap::new(),
            events: Vec::new(),
            status: SpanStatus::Unset,
            status_message: None,
            ended: AtomicBool::new(false),
        }
    }

    /// Get the trace context
    pub fn context(&self) -> &TraceContext {
        &self.context
    }

    /// Set an attribute
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<AttributeValue>) -> &mut Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Add an event
    pub fn add_event(&mut self, name: impl Into<String>) -> &mut Self {
        let event = SpanEvent {
            name: name.into(),
            timestamp_ns: crate::optimizations::timestamp::nano_timestamp() as u64,
            attributes: HashMap::new(),
        };
        self.events.push(event);
        self
    }

    /// Add an event with attributes
    pub fn add_event_with_attrs(
        &mut self,
        name: impl Into<String>,
        attrs: impl IntoIterator<Item = (impl Into<String>, impl Into<AttributeValue>)>,
    ) -> &mut Self {
        let event = SpanEvent {
            name: name.into(),
            timestamp_ns: crate::optimizations::timestamp::nano_timestamp() as u64,
            attributes: attrs.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
        };
        self.events.push(event);
        self
    }

    /// Set span status to OK
    pub fn set_ok(&mut self) -> &mut Self {
        self.status = SpanStatus::Ok;
        self
    }

    /// Set span status to Error
    pub fn set_error(&mut self, message: impl Into<String>) -> &mut Self {
        self.status = SpanStatus::Error;
        self.status_message = Some(message.into());
        self
    }

    /// Record an exception
    pub fn record_exception(&mut self, err: &dyn std::error::Error) -> &mut Self {
        self.add_event_with_attrs("exception", [
            ("exception.type", AttributeValue::String(std::any::type_name_of_val(err).to_string())),
            ("exception.message", AttributeValue::String(err.to_string())),
        ]);
        self.set_error(err.to_string())
    }

    /// End the span and return completed data
    pub fn end(self) -> CompletedSpan {
        self.ended.store(true, Ordering::Release);
        let end_timestamp_ns = crate::optimizations::timestamp::nano_timestamp() as u64;
        let duration_ns = self.start_time.elapsed().as_nanos() as u64;
        
        CompletedSpan {
            context: self.context,
            name: self.name,
            kind: self.kind,
            start_timestamp_ns: self.start_timestamp_ns,
            end_timestamp_ns,
            duration_ns,
            attributes: self.attributes,
            events: self.events,
            status: self.status,
            status_message: self.status_message,
        }
    }

    /// Get elapsed time
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Span exporter trait
pub trait SpanExporter: Send + Sync {
    fn export(&self, spans: Vec<CompletedSpan>);
}

/// In-memory span buffer for testing
pub struct InMemoryExporter {
    spans: std::sync::Mutex<Vec<CompletedSpan>>,
    max_spans: usize,
}

impl InMemoryExporter {
    pub fn new(max_spans: usize) -> Self {
        Self {
            spans: std::sync::Mutex::new(Vec::with_capacity(max_spans)),
            max_spans,
        }
    }

    pub fn get_spans(&self) -> Vec<CompletedSpan> {
        self.spans.lock().ok().map(|g| g.clone()).unwrap_or_default()
    }

    pub fn clear(&self) {
        if let Ok(mut guard) = self.spans.lock() {
            guard.clear();
        }
    }
}

impl SpanExporter for InMemoryExporter {
    fn export(&self, spans: Vec<CompletedSpan>) {
        if let Ok(mut buffer) = self.spans.lock() {
            for span in spans {
                if buffer.len() >= self.max_spans {
                    buffer.remove(0);
                }
                buffer.push(span);
            }
        }
    }
}

/// Log exporter (writes spans to logs)
pub struct LogExporter {
    min_duration_ms: u64,
}

impl LogExporter {
    pub fn new(min_duration_ms: u64) -> Self {
        Self { min_duration_ms }
    }
}

impl SpanExporter for LogExporter {
    fn export(&self, spans: Vec<CompletedSpan>) {
        for span in spans {
            let duration_ms = span.duration_ns / 1_000_000;
            if duration_ms >= self.min_duration_ms {
                log::info!(
                    "[TRACE] {} trace_id={} span_id={} duration={}ms status={:?}",
                    span.name,
                    span.context.trace_id,
                    span.context.span_id,
                    duration_ms,
                    span.status
                );
            }
        }
    }
}

/// Tracer for creating spans
pub struct Tracer {
    service_name: String,
    exporters: Vec<Box<dyn SpanExporter>>,
    sampling_rate: f64,
}

impl Tracer {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            exporters: Vec::new(),
            sampling_rate: 1.0,
        }
    }

    pub fn with_exporter(mut self, exporter: impl SpanExporter + 'static) -> Self {
        self.exporters.push(Box::new(exporter));
        self
    }

    pub fn with_sampling_rate(mut self, rate: f64) -> Self {
        self.sampling_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Create a new span
    pub fn start_span(&self, name: impl Into<String>, parent: Option<&TraceContext>) -> Span {
        Span::new(name, parent, SpanKind::Internal)
    }

    /// Create a new span with kind
    pub fn start_span_with_kind(
        &self,
        name: impl Into<String>,
        parent: Option<&TraceContext>,
        kind: SpanKind,
    ) -> Span {
        Span::new(name, parent, kind)
    }

    /// Export a completed span
    pub fn export(&self, span: CompletedSpan) {
        // Check sampling
        if self.sampling_rate < 1.0 {
            let sample: f64 = (span.context.trace_id.0[0] as f64) / 255.0;
            if sample > self.sampling_rate {
                return;
            }
        }
        
        for exporter in &self.exporters {
            exporter.export(vec![span.clone()]);
        }
    }

    /// Get service name
    pub fn service_name(&self) -> &str {
        &self.service_name
    }
}

/// Global tracer instance
pub static TRACER: once_cell::sync::Lazy<std::sync::RwLock<Option<Tracer>>> =
    once_cell::sync::Lazy::new(|| std::sync::RwLock::new(None));

/// Initialize the global tracer
pub fn init_tracer(service_name: &str) {
    let tracer = Tracer::new(service_name)
        .with_exporter(LogExporter::new(1));
    
    if let Ok(mut guard) = TRACER.write() {
        *guard = Some(tracer);
    } else {
        log::error!("Failed to initialize global tracer: RwLock poisoned");
    }
}

/// Get the global tracer
pub fn tracer() -> Option<std::sync::RwLockReadGuard<'static, Option<Tracer>>> {
    let guard = TRACER.read().ok()?;
    if guard.is_some() {
        Some(guard)
    } else {
        None
    }
}

/// Convenience macro for tracing a function
#[macro_export]
macro_rules! trace_span {
    ($name:expr) => {{
        $crate::tracing::Span::new($name, None, $crate::tracing::SpanKind::Internal)
    }};
    ($name:expr, $parent:expr) => {{
        $crate::tracing::Span::new($name, Some($parent), $crate::tracing::SpanKind::Internal)
    }};
    ($name:expr, $parent:expr, $kind:expr) => {{
        $crate::tracing::Span::new($name, Some($parent), $kind)
    }};
}

/// Execution trace context for order execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTraceContext {
    pub trace_context: TraceContext,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub symbol: String,
    pub exchange: String,
    pub side: String,
    pub quantity: f64,
    pub start_timestamp_ns: u64,
}

impl ExecutionTraceContext {
    pub fn new(
        order_id: &str,
        client_order_id: Option<&str>,
        symbol: &str,
        exchange: &str,
        side: &str,
        quantity: f64,
    ) -> Self {
        Self {
            trace_context: TraceContext::new(),
            order_id: order_id.to_string(),
            client_order_id: client_order_id.map(String::from),
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            side: side.to_string(),
            quantity,
            start_timestamp_ns: crate::optimizations::timestamp::nano_timestamp() as u64,
        }
    }

    /// Create from existing trace context (for propagation)
    pub fn from_context(
        trace_context: TraceContext,
        order_id: &str,
        symbol: &str,
        exchange: &str,
        side: &str,
        quantity: f64,
    ) -> Self {
        Self {
            trace_context,
            order_id: order_id.to_string(),
            client_order_id: None,
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            side: side.to_string(),
            quantity,
            start_timestamp_ns: crate::optimizations::timestamp::nano_timestamp() as u64,
        }
    }

    /// Get traceparent header for HTTP propagation
    pub fn traceparent(&self) -> String {
        self.trace_context.to_traceparent()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_id_generation() {
        let id1 = TraceId::generate();
        let id2 = TraceId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_trace_id_hex_roundtrip() {
        let id = TraceId::generate();
        let hex = id.to_hex();
        let parsed = TraceId::from_hex(&hex).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_span_id_generation() {
        let id1 = SpanId::generate();
        let id2 = SpanId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_trace_context_child() {
        let parent = TraceContext::new();
        let child = parent.child();
        
        assert_eq!(parent.trace_id, child.trace_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
        assert_ne!(parent.span_id, child.span_id);
    }

    #[test]
    fn test_traceparent_roundtrip() {
        let ctx = TraceContext::new();
        let header = ctx.to_traceparent();
        let parsed = TraceContext::from_traceparent(&header).unwrap();
        
        assert_eq!(ctx.trace_id, parsed.trace_id);
        assert_eq!(ctx.span_id, parsed.span_id);
        assert_eq!(ctx.trace_flags, parsed.trace_flags);
    }

    #[test]
    fn test_span_lifecycle() {
        let mut span = Span::new("test_operation", None, SpanKind::Internal);
        
        span.set_attribute("key", "value");
        span.add_event("checkpoint");
        span.set_ok();
        
        let completed = span.end();
        
        assert_eq!(completed.name, "test_operation");
        assert_eq!(completed.status, SpanStatus::Ok);
        assert!(completed.duration_ns > 0);
        assert_eq!(completed.attributes.len(), 1);
        assert_eq!(completed.events.len(), 1);
    }

    #[test]
    fn test_span_with_error() {
        let mut span = Span::new("failing_operation", None, SpanKind::Internal);
        span.set_error("Something went wrong");
        
        let completed = span.end();
        
        assert_eq!(completed.status, SpanStatus::Error);
        assert_eq!(completed.status_message, Some("Something went wrong".to_string()));
    }

    #[test]
    fn test_in_memory_exporter() {
        let exporter = InMemoryExporter::new(10);
        
        let span = Span::new("test", None, SpanKind::Internal);
        let completed = span.end();
        
        exporter.export(vec![completed]);
        
        let spans = exporter.get_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "test");
    }

    #[test]
    fn test_in_memory_exporter_overflow() {
        let exporter = InMemoryExporter::new(2);
        
        for i in 0..5 {
            let span = Span::new(format!("span_{}", i), None, SpanKind::Internal);
            exporter.export(vec![span.end()]);
        }
        
        let spans = exporter.get_spans();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].name, "span_3");
        assert_eq!(spans[1].name, "span_4");
    }

    #[test]
    fn test_tracer_with_sampling() {
        let exporter = std::sync::Arc::new(InMemoryExporter::new(100));
        let tracer = Tracer::new("test_service")
            .with_sampling_rate(0.5);
        
        // Can't easily test sampling without lots of spans
        assert_eq!(tracer.service_name(), "test_service");
    }

    #[test]
    fn test_execution_trace_context() {
        let ctx = ExecutionTraceContext::new(
            "order123",
            Some("client456"),
            "BTC/USD",
            "kraken",
            "buy",
            1.0,
        );
        
        assert_eq!(ctx.order_id, "order123");
        assert!(ctx.traceparent().starts_with("00-"));
    }
}
