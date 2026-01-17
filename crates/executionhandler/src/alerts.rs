//! Alert Webhooks Module
//!
//! Production-grade alerting system for critical trading events.
//! Supports multiple providers: PagerDuty, Slack, Discord, generic webhooks.
//!
//! # Features
//!
//! - **Multi-provider support**: PagerDuty, Slack, Discord, OpsGenie, generic HTTP
//! - **Alert severity levels**: Critical, Warning, Info
//! - **Rate limiting**: Prevent alert storms
//! - **Deduplication**: Suppress duplicate alerts within time window
//! - **Async dispatch**: Non-blocking alert delivery
//! - **Fallback routing**: Try secondary providers on failure
//!
//! # Example
//!
//! ```rust,ignore
//! use executionhandler::alerts::{AlertManager, AlertConfig, AlertSeverity};
//!
//! let manager = AlertManager::new(AlertConfig::default());
//!
//! // Send critical alert
//! manager.alert(
//!     AlertSeverity::Critical,
//!     "Kill Switch Triggered",
//!     "Trading halted due to max drawdown",
//!     Some(json!({"drawdown": "15.2%", "threshold": "10%"})),
//! ).await?;
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    /// Critical: Immediate attention required (pages on-call)
    Critical,
    /// Warning: Requires attention but not immediate
    Warning,
    /// Info: Informational, logged but typically not paged
    Info,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Critical => write!(f, "CRITICAL"),
            AlertSeverity::Warning => write!(f, "WARNING"),
            AlertSeverity::Info => write!(f, "INFO"),
        }
    }
}

/// Alert categories for routing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertCategory {
    /// Trading halted (kill switch, circuit breaker)
    TradingHalted,
    /// Risk limit breached
    RiskBreach,
    /// Position reconciliation mismatch
    ReconciliationError,
    /// Exchange connectivity issues
    ConnectivityError,
    /// Fat-finger protection triggered
    FatFingerBlock,
    /// Order execution failure
    ExecutionError,
    /// System health issue
    SystemHealth,
    /// Performance degradation
    PerformanceDegraded,
    /// Custom/other
    Custom,
}

/// Alert payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Unique alert ID
    pub id: String,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert category
    pub category: AlertCategory,
    /// Short title
    pub title: String,
    /// Detailed message
    pub message: String,
    /// Additional context/metadata
    pub context: Option<JsonValue>,
    /// Source component
    pub source: String,
    /// Timestamp (Unix millis)
    pub timestamp_ms: u64,
    /// Deduplication key (alerts with same key within window are suppressed)
    pub dedup_key: Option<String>,
}

impl Alert {
    /// Create a new alert
    pub fn new(
        severity: AlertSeverity,
        category: AlertCategory,
        title: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            severity,
            category,
            title: title.into(),
            message: message.into(),
            context: None,
            source: "SignalEngine".to_string(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            dedup_key: None,
        }
    }
    
    /// Add context metadata
    pub fn with_context(mut self, context: JsonValue) -> Self {
        self.context = Some(context);
        self
    }
    
    /// Set deduplication key
    pub fn with_dedup_key(mut self, key: impl Into<String>) -> Self {
        self.dedup_key = Some(key.into());
        self
    }
    
    /// Set source component
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = source.into();
        self
    }
}

/// Alert provider trait
#[async_trait]
pub trait AlertProvider: Send + Sync {
    /// Provider name
    fn name(&self) -> &str;
    
    /// Send an alert
    async fn send(&self, alert: &Alert) -> Result<(), AlertError>;
    
    /// Check if provider is healthy
    async fn health_check(&self) -> bool;
}

/// Alert delivery errors
#[derive(Debug, thiserror::Error)]
pub enum AlertError {
    #[error("HTTP request failed: {0}")]
    HttpError(String),
    
    #[error("Provider configuration error: {0}")]
    ConfigError(String),
    
    #[error("Rate limited")]
    RateLimited,
    
    #[error("Timeout")]
    Timeout,
    
    #[error("Provider unavailable: {0}")]
    Unavailable(String),
}

/// PagerDuty provider
pub struct PagerDutyProvider {
    routing_key: String,
    http_client: reqwest::Client,
}

impl PagerDutyProvider {
    pub fn new(routing_key: impl Into<String>) -> Result<Self, AlertError> {
        Ok(Self {
            routing_key: routing_key.into(),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .map_err(|e| AlertError::ConfigError(format!("Failed to create HTTP client: {}", e)))?,
        })
    }
}

#[async_trait]
impl AlertProvider for PagerDutyProvider {
    fn name(&self) -> &str {
        "pagerduty"
    }
    
    async fn send(&self, alert: &Alert) -> Result<(), AlertError> {
        let severity = match alert.severity {
            AlertSeverity::Critical => "critical",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Info => "info",
        };
        
        let payload = serde_json::json!({
            "routing_key": self.routing_key,
            "event_action": "trigger",
            "dedup_key": alert.dedup_key.clone().unwrap_or_else(|| alert.id.clone()),
            "payload": {
                "summary": format!("[{}] {}: {}", alert.source, alert.severity, alert.title),
                "severity": severity,
                "source": alert.source,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "custom_details": {
                    "title": alert.title,
                    "message": alert.message,
                    "category": format!("{:?}", alert.category),
                    "context": alert.context,
                }
            }
        });
        
        let response = self.http_client
            .post("https://events.pagerduty.com/v2/enqueue")
            .json(&payload)
            .send()
            .await
            .map_err(|e| AlertError::HttpError(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(AlertError::HttpError(format!(
                "PagerDuty returned status {}",
                response.status()
            )))
        }
    }
    
    async fn health_check(&self) -> bool {
        // PagerDuty doesn't have a dedicated health endpoint
        true
    }
}

/// Slack provider
pub struct SlackProvider {
    webhook_url: String,
    channel: Option<String>,
    http_client: reqwest::Client,
}

impl SlackProvider {
    pub fn new(webhook_url: impl Into<String>) -> Result<Self, AlertError> {
        Ok(Self {
            webhook_url: webhook_url.into(),
            channel: None,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .map_err(|e| AlertError::ConfigError(format!("Failed to create HTTP client: {}", e)))?,
        })
    }
    
    pub fn with_channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = Some(channel.into());
        self
    }
}

#[async_trait]
impl AlertProvider for SlackProvider {
    fn name(&self) -> &str {
        "slack"
    }
    
    async fn send(&self, alert: &Alert) -> Result<(), AlertError> {
        let color = match alert.severity {
            AlertSeverity::Critical => "#FF0000", // Red
            AlertSeverity::Warning => "#FFA500",  // Orange
            AlertSeverity::Info => "#36A64F",     // Green
        };
        
        let emoji = match alert.severity {
            AlertSeverity::Critical => "🚨",
            AlertSeverity::Warning => "⚠️",
            AlertSeverity::Info => "ℹ️",
        };
        
        let mut fields = vec![
            serde_json::json!({
                "title": "Category",
                "value": format!("{:?}", alert.category),
                "short": true
            }),
            serde_json::json!({
                "title": "Source",
                "value": &alert.source,
                "short": true
            }),
        ];
        
        if let Some(ctx) = &alert.context {
            fields.push(serde_json::json!({
                "title": "Context",
                "value": format!("```{}```", serde_json::to_string_pretty(ctx).unwrap_or_default()),
                "short": false
            }));
        }
        
        let mut payload = serde_json::json!({
            "attachments": [{
                "color": color,
                "title": format!("{} {} - {}", emoji, alert.severity, alert.title),
                "text": &alert.message,
                "fields": fields,
                "footer": "SignalEngine Alerts",
                "ts": alert.timestamp_ms / 1000
            }]
        });
        
        if let Some(channel) = &self.channel {
            payload["channel"] = serde_json::json!(channel);
        }
        
        let response = self.http_client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| AlertError::HttpError(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(AlertError::HttpError(format!(
                "Slack returned status {}",
                response.status()
            )))
        }
    }
    
    async fn health_check(&self) -> bool {
        true
    }
}

/// Discord provider
pub struct DiscordProvider {
    webhook_url: String,
    http_client: reqwest::Client,
}

impl DiscordProvider {
    pub fn new(webhook_url: impl Into<String>) -> Result<Self, AlertError> {
        Ok(Self {
            webhook_url: webhook_url.into(),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .map_err(|e| AlertError::ConfigError(format!("Failed to create HTTP client: {}", e)))?,
        })
    }
}

#[async_trait]
impl AlertProvider for DiscordProvider {
    fn name(&self) -> &str {
        "discord"
    }
    
    async fn send(&self, alert: &Alert) -> Result<(), AlertError> {
        let color = match alert.severity {
            AlertSeverity::Critical => 0xFF0000, // Red
            AlertSeverity::Warning => 0xFFA500,  // Orange
            AlertSeverity::Info => 0x36A64F,     // Green
        };
        
        let emoji = match alert.severity {
            AlertSeverity::Critical => "🚨",
            AlertSeverity::Warning => "⚠️",
            AlertSeverity::Info => "ℹ️",
        };
        
        let mut fields = vec![
            serde_json::json!({
                "name": "Category",
                "value": format!("{:?}", alert.category),
                "inline": true
            }),
            serde_json::json!({
                "name": "Source",
                "value": &alert.source,
                "inline": true
            }),
        ];
        
        if let Some(ctx) = &alert.context {
            fields.push(serde_json::json!({
                "name": "Context",
                "value": format!("```json\n{}\n```", 
                    serde_json::to_string_pretty(ctx)
                        .unwrap_or_default()
                        .chars()
                        .take(1000)
                        .collect::<String>()
                ),
                "inline": false
            }));
        }
        
        let payload = serde_json::json!({
            "embeds": [{
                "title": format!("{} {} - {}", emoji, alert.severity, alert.title),
                "description": &alert.message,
                "color": color,
                "fields": fields,
                "footer": {
                    "text": "SignalEngine Alerts"
                },
                "timestamp": chrono::Utc::now().to_rfc3339()
            }]
        });
        
        let response = self.http_client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| AlertError::HttpError(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(AlertError::HttpError(format!(
                "Discord returned status {}",
                response.status()
            )))
        }
    }
    
    async fn health_check(&self) -> bool {
        true
    }
}

/// Generic HTTP webhook provider
pub struct GenericWebhookProvider {
    url: String,
    headers: HashMap<String, String>,
    http_client: reqwest::Client,
}

impl GenericWebhookProvider {
    pub fn new(url: impl Into<String>) -> Result<Self, AlertError> {
        Ok(Self {
            url: url.into(),
            headers: HashMap::new(),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .map_err(|e| AlertError::ConfigError(format!("Failed to create HTTP client: {}", e)))?,
        })
    }
    
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

#[async_trait]
impl AlertProvider for GenericWebhookProvider {
    fn name(&self) -> &str {
        "generic_webhook"
    }
    
    async fn send(&self, alert: &Alert) -> Result<(), AlertError> {
        let mut request = self.http_client.post(&self.url);
        
        for (key, value) in &self.headers {
            request = request.header(key, value);
        }
        
        let response = request
            .json(alert)
            .send()
            .await
            .map_err(|e| AlertError::HttpError(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(AlertError::HttpError(format!(
                "Webhook returned status {}",
                response.status()
            )))
        }
    }
    
    async fn health_check(&self) -> bool {
        true
    }
}

/// Deduplication entry
struct DedupEntry {
    first_seen: Instant,
    count: u64,
}

/// Alert manager configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertManagerConfig {
    /// Deduplication window (seconds)
    pub dedup_window_secs: u64,
    /// Rate limit per severity (alerts per minute)
    pub rate_limits: HashMap<String, u32>,
    /// Whether to send test alert on startup
    pub send_test_on_startup: bool,
    /// Buffer size for async queue
    pub queue_size: usize,
    /// Retry attempts for failed alerts
    pub max_retries: u32,
    /// Retry delay (milliseconds)
    pub retry_delay_ms: u64,
}

impl Default for AlertManagerConfig {
    fn default() -> Self {
        let mut rate_limits = HashMap::new();
        rate_limits.insert("critical".to_string(), 60);  // 60/min for critical
        rate_limits.insert("warning".to_string(), 30);   // 30/min for warning
        rate_limits.insert("info".to_string(), 10);      // 10/min for info
        
        Self {
            dedup_window_secs: 300, // 5 minutes
            rate_limits,
            send_test_on_startup: false,
            queue_size: 1000,
            max_retries: 3,
            retry_delay_ms: 1000,
        }
    }
}

/// Alert manager for routing alerts to providers
pub struct AlertManager {
    config: RwLock<AlertManagerConfig>,
    providers: RwLock<Vec<Box<dyn AlertProvider>>>,
    dedup_cache: DashMap<String, DedupEntry>,
    rate_counters: DashMap<String, (Instant, AtomicU64)>,
    stats: AlertStats,
    sender: mpsc::Sender<Alert>,
}

/// Alert statistics
#[derive(Debug, Default)]
pub struct AlertStats {
    pub total_alerts: AtomicU64,
    pub alerts_sent: AtomicU64,
    pub alerts_deduplicated: AtomicU64,
    pub alerts_rate_limited: AtomicU64,
    pub alerts_failed: AtomicU64,
    pub critical_count: AtomicU64,
    pub warning_count: AtomicU64,
    pub info_count: AtomicU64,
}

impl AlertManager {
    /// Create a new alert manager
    pub fn new(config: AlertManagerConfig) -> (Self, mpsc::Receiver<Alert>) {
        let (sender, receiver) = mpsc::channel(config.queue_size);
        
        (Self {
            config: RwLock::new(config),
            providers: RwLock::new(Vec::new()),
            dedup_cache: DashMap::new(),
            rate_counters: DashMap::new(),
            stats: AlertStats::default(),
            sender,
        }, receiver)
    }
    
    /// Add an alert provider
    pub fn add_provider(&self, provider: Box<dyn AlertProvider>) {
        self.providers.write().push(provider);
    }
    
    /// Send an alert (non-blocking, queues for async delivery)
    pub async fn alert(
        &self,
        severity: AlertSeverity,
        category: AlertCategory,
        title: impl Into<String>,
        message: impl Into<String>,
        context: Option<JsonValue>,
    ) -> Result<(), AlertError> {
        let mut alert = Alert::new(severity, category, title, message);
        if let Some(ctx) = context {
            alert = alert.with_context(ctx);
        }
        
        self.send_alert(alert).await
    }
    
    /// Send a pre-built alert
    pub async fn send_alert(&self, alert: Alert) -> Result<(), AlertError> {
        self.stats.total_alerts.fetch_add(1, Ordering::Relaxed);
        
        // Track by severity
        match alert.severity {
            AlertSeverity::Critical => self.stats.critical_count.fetch_add(1, Ordering::Relaxed),
            AlertSeverity::Warning => self.stats.warning_count.fetch_add(1, Ordering::Relaxed),
            AlertSeverity::Info => self.stats.info_count.fetch_add(1, Ordering::Relaxed),
        };
        
        // Check deduplication
        if let Some(dedup_key) = &alert.dedup_key {
            let config = self.config.read();
            let window = Duration::from_secs(config.dedup_window_secs);
            drop(config);
            
            if let Some(mut entry) = self.dedup_cache.get_mut(dedup_key) {
                if entry.first_seen.elapsed() < window {
                    entry.count += 1;
                    self.stats.alerts_deduplicated.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                } else {
                    // Window expired, reset
                    entry.first_seen = Instant::now();
                    entry.count = 1;
                }
            } else {
                self.dedup_cache.insert(dedup_key.clone(), DedupEntry {
                    first_seen: Instant::now(),
                    count: 1,
                });
            }
        }
        
        // Check rate limit
        let severity_key = format!("{:?}", alert.severity).to_lowercase();
        let config = self.config.read();
        let limit = config.rate_limits.get(&severity_key).copied().unwrap_or(60);
        drop(config);
        
        let now = Instant::now();
        let mut counter = self.rate_counters.entry(severity_key.clone())
            .or_insert_with(|| (now, AtomicU64::new(0)));
        
        // Reset counter if minute has passed
        if counter.0.elapsed() > Duration::from_secs(60) {
            *counter = (now, AtomicU64::new(0));
        }
        
        let count = counter.1.fetch_add(1, Ordering::Relaxed);
        if count >= limit as u64 {
            self.stats.alerts_rate_limited.fetch_add(1, Ordering::Relaxed);
            return Err(AlertError::RateLimited);
        }
        
        // Queue for async delivery
        self.sender.send(alert).await
            .map_err(|e| AlertError::HttpError(format!("Failed to queue alert: {}", e)))?;
        
        self.stats.alerts_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    
    /// Process alerts from queue (run in background task)
    pub async fn process_alerts(&self, mut receiver: mpsc::Receiver<Alert>) {
        let config = self.config.read();
        let max_retries = config.max_retries;
        let retry_delay = Duration::from_millis(config.retry_delay_ms);
        drop(config);
        
        while let Some(alert) = receiver.recv().await {
            let providers = self.providers.read();
            
            for provider in providers.iter() {
                let mut attempts = 0;
                loop {
                    match provider.send(&alert).await {
                        Ok(_) => break,
                        Err(e) => {
                            attempts += 1;
                            if attempts >= max_retries {
                                self.stats.alerts_failed.fetch_add(1, Ordering::Relaxed);
                                eprintln!("Alert delivery failed after {} attempts to {}: {}", 
                                    attempts, provider.name(), e);
                                break;
                            }
                            tokio::time::sleep(retry_delay).await;
                        }
                    }
                }
            }
        }
    }
    
    /// Get statistics
    pub fn get_stats(&self) -> AlertStatistics {
        AlertStatistics {
            total_alerts: self.stats.total_alerts.load(Ordering::Relaxed),
            alerts_sent: self.stats.alerts_sent.load(Ordering::Relaxed),
            alerts_deduplicated: self.stats.alerts_deduplicated.load(Ordering::Relaxed),
            alerts_rate_limited: self.stats.alerts_rate_limited.load(Ordering::Relaxed),
            alerts_failed: self.stats.alerts_failed.load(Ordering::Relaxed),
            critical_count: self.stats.critical_count.load(Ordering::Relaxed),
            warning_count: self.stats.warning_count.load(Ordering::Relaxed),
            info_count: self.stats.info_count.load(Ordering::Relaxed),
        }
    }
    
    /// Clean up stale dedup entries
    pub fn cleanup_dedup_cache(&self) {
        let config = self.config.read();
        let window = Duration::from_secs(config.dedup_window_secs);
        drop(config);
        
        self.dedup_cache.retain(|_, entry| entry.first_seen.elapsed() < window);
    }
}

/// Public statistics structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertStatistics {
    pub total_alerts: u64,
    pub alerts_sent: u64,
    pub alerts_deduplicated: u64,
    pub alerts_rate_limited: u64,
    pub alerts_failed: u64,
    pub critical_count: u64,
    pub warning_count: u64,
    pub info_count: u64,
}

// ============================================================================
// Convenience functions for common alert scenarios
// ============================================================================

/// Send kill switch alert
pub async fn alert_kill_switch(
    manager: &AlertManager,
    reason: &str,
    context: Option<JsonValue>,
) -> Result<(), AlertError> {
    manager.alert(
        AlertSeverity::Critical,
        AlertCategory::TradingHalted,
        "Kill Switch Triggered",
        format!("All trading halted: {}", reason),
        context,
    ).await
}

/// Send circuit breaker alert
pub async fn alert_circuit_breaker(
    manager: &AlertManager,
    exchange: &str,
    reason: &str,
    context: Option<JsonValue>,
) -> Result<(), AlertError> {
    manager.alert(
        AlertSeverity::Critical,
        AlertCategory::TradingHalted,
        format!("Circuit Breaker: {}", exchange),
        reason.to_string(),
        context,
    ).await
}

/// Send fat-finger block alert
pub async fn alert_fat_finger_block(
    manager: &AlertManager,
    violation: &str,
    context: Option<JsonValue>,
) -> Result<(), AlertError> {
    manager.alert(
        AlertSeverity::Warning,
        AlertCategory::FatFingerBlock,
        "Fat-Finger Protection Triggered",
        violation.to_string(),
        context,
    ).await
}

/// Send reconciliation error alert
pub async fn alert_reconciliation_error(
    manager: &AlertManager,
    message: &str,
    context: Option<JsonValue>,
) -> Result<(), AlertError> {
    manager.alert(
        AlertSeverity::Critical,
        AlertCategory::ReconciliationError,
        "Position Reconciliation Mismatch",
        message.to_string(),
        context,
    ).await
}

/// Send connectivity error alert
pub async fn alert_connectivity_error(
    manager: &AlertManager,
    exchange: &str,
    error: &str,
    context: Option<JsonValue>,
) -> Result<(), AlertError> {
    manager.alert(
        AlertSeverity::Warning,
        AlertCategory::ConnectivityError,
        format!("Exchange Connectivity: {}", exchange),
        error.to_string(),
        context,
    ).await
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_alert_creation() {
        let alert = Alert::new(
            AlertSeverity::Critical,
            AlertCategory::TradingHalted,
            "Test Alert",
            "This is a test",
        );
        
        assert_eq!(alert.severity, AlertSeverity::Critical);
        assert_eq!(alert.title, "Test Alert");
        assert!(!alert.id.is_empty());
    }
    
    #[test]
    fn test_alert_with_context() {
        let alert = Alert::new(
            AlertSeverity::Warning,
            AlertCategory::RiskBreach,
            "Risk Alert",
            "Position limit exceeded",
        ).with_context(serde_json::json!({
            "symbol": "BTC-USD",
            "position": 100,
            "limit": 50
        }));
        
        assert!(alert.context.is_some());
    }
    
    #[tokio::test]
    async fn test_alert_manager_deduplication() {
        let config = AlertManagerConfig {
            dedup_window_secs: 1,
            ..Default::default()
        };
        let (manager, _receiver) = AlertManager::new(config);
        
        // First alert should go through
        let alert1 = Alert::new(
            AlertSeverity::Warning,
            AlertCategory::Custom,
            "Test",
            "Message",
        ).with_dedup_key("test-key");
        
        assert!(manager.send_alert(alert1).await.is_ok());
        
        // Duplicate should be suppressed
        let alert2 = Alert::new(
            AlertSeverity::Warning,
            AlertCategory::Custom,
            "Test",
            "Message",
        ).with_dedup_key("test-key");
        
        assert!(manager.send_alert(alert2).await.is_ok());
        
        let stats = manager.get_stats();
        assert_eq!(stats.total_alerts, 2);
        assert_eq!(stats.alerts_deduplicated, 1);
    }
    
    #[tokio::test]
    async fn test_alert_manager_rate_limiting() {
        let mut rate_limits = HashMap::new();
        rate_limits.insert("warning".to_string(), 2); // Only 2 per minute
        
        let config = AlertManagerConfig {
            rate_limits,
            ..Default::default()
        };
        let (manager, _receiver) = AlertManager::new(config);
        
        // First two should succeed
        for _ in 0..2 {
            let alert = Alert::new(
                AlertSeverity::Warning,
                AlertCategory::Custom,
                "Test",
                "Message",
            );
            assert!(manager.send_alert(alert).await.is_ok());
        }
        
        // Third should be rate limited
        let alert = Alert::new(
            AlertSeverity::Warning,
            AlertCategory::Custom,
            "Test",
            "Message",
        );
        match manager.send_alert(alert).await {
            Err(AlertError::RateLimited) => {},
            other => panic!("Expected RateLimited, got {:?}", other),
        }
        
        let stats = manager.get_stats();
        assert_eq!(stats.alerts_rate_limited, 1);
    }
    
    #[test]
    fn test_severity_display() {
        assert_eq!(format!("{}", AlertSeverity::Critical), "CRITICAL");
        assert_eq!(format!("{}", AlertSeverity::Warning), "WARNING");
        assert_eq!(format!("{}", AlertSeverity::Info), "INFO");
    }
}
