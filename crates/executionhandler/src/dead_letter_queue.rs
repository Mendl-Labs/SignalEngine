//! Dead Letter Queue for Failed Orders
//!
//! Provides persistent storage for orders that failed to execute,
//! enabling manual review, retry logic, and audit trails.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};

/// Dead letter queue entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    /// Unique entry ID
    pub id: String,
    /// Original order ID
    pub order_id: String,
    /// Client order ID (if any)
    pub client_order_id: Option<String>,
    /// Trading symbol
    pub symbol: String,
    /// Exchange name
    pub exchange: String,
    /// Order side
    pub side: String,
    /// Order type
    pub order_type: String,
    /// Requested quantity
    pub quantity: f64,
    /// Price (for limit orders)
    pub price: Option<f64>,
    /// Error message
    pub error_message: String,
    /// Error code (if available)
    pub error_code: Option<String>,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Maximum allowed retries
    pub max_retries: u32,
    /// Timestamp when order was originally submitted
    pub original_timestamp: u64,
    /// Timestamp when added to DLQ
    pub dlq_timestamp: u64,
    /// Last retry timestamp
    pub last_retry_timestamp: Option<u64>,
    /// Status of the DLQ entry
    pub status: DeadLetterStatus,
    /// Additional metadata
    pub metadata: serde_json::Value,
}

/// Status of a dead letter entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeadLetterStatus {
    /// Awaiting retry
    Pending,
    /// Currently being retried
    Retrying,
    /// Retry succeeded
    Resolved,
    /// Max retries exceeded, needs manual review
    RequiresManualReview,
    /// Manually resolved/dismissed
    Dismissed,
    /// Permanently failed (e.g., invalid order params)
    PermanentFailure,
}

/// Failure category for retry logic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureCategory {
    /// Network error - should retry
    Network,
    /// Rate limited - retry with backoff
    RateLimit,
    /// Insufficient funds - don't retry, needs attention
    InsufficientFunds,
    /// Invalid order - permanent failure
    InvalidOrder,
    /// Exchange error - may retry
    ExchangeError,
    /// Internal error - may retry
    InternalError,
    /// Unknown error
    Unknown,
}

impl FailureCategory {
    /// Determine if this failure type should be retried
    pub fn should_retry(&self) -> bool {
        matches!(self, 
            FailureCategory::Network | 
            FailureCategory::RateLimit | 
            FailureCategory::ExchangeError |
            FailureCategory::InternalError
        )
    }

    /// Get retry delay in milliseconds
    pub fn retry_delay_ms(&self, attempt: u32) -> u64 {
        let base_delay = match self {
            FailureCategory::RateLimit => 5000,   // 5 second base for rate limits
            FailureCategory::Network => 1000,     // 1 second for network issues
            FailureCategory::ExchangeError => 2000, // 2 seconds for exchange errors
            _ => 1000,
        };
        
        // Exponential backoff with jitter
        let delay = base_delay * (2_u64.pow(attempt.min(5)));
        let jitter = (delay as f64 * 0.1 * rand::random::<f64>()) as u64;
        delay + jitter
    }
}

/// Dead letter queue configuration
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// Maximum entries to keep in memory
    pub max_memory_entries: usize,
    /// Maximum retry attempts per order
    pub max_retries: u32,
    /// Path to persistence file
    pub persistence_path: Option<PathBuf>,
    /// Auto-persist interval in seconds
    pub persist_interval_secs: u64,
    /// Enable automatic retries
    pub auto_retry_enabled: bool,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            max_memory_entries: 10_000,
            max_retries: 3,
            persistence_path: Some(PathBuf::from("./dead_letter_queue.json")),
            persist_interval_secs: 60,
            auto_retry_enabled: true,
        }
    }
}

/// Dead Letter Queue
pub struct DeadLetterQueue {
    /// In-memory queue
    entries: Arc<RwLock<VecDeque<DeadLetterEntry>>>,
    /// Configuration
    config: DlqConfig,
    /// Entry counter for unique IDs
    entry_counter: std::sync::atomic::AtomicU64,
}

impl DeadLetterQueue {
    pub fn new(config: DlqConfig) -> Self {
        Self {
            entries: Arc::new(RwLock::new(VecDeque::with_capacity(config.max_memory_entries))),
            config,
            entry_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Create with default configuration
    pub fn default_instance() -> Self {
        Self::new(DlqConfig::default())
    }

    /// Generate unique entry ID
    fn generate_entry_id(&self) -> String {
        let counter = self.entry_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let timestamp = crate::optimizations::timestamp::nano_timestamp();
        format!("dlq_{}_{}", timestamp, counter)
    }

    /// Add a failed order to the queue
    pub async fn enqueue(&self, entry: DeadLetterEntry) -> Result<String> {
        let mut entries = self.entries.write().await;
        
        // Evict oldest if at capacity
        while entries.len() >= self.config.max_memory_entries {
            if let Some(evicted) = entries.pop_front() {
                log::warn!(
                    "DLQ at capacity, evicting entry: {} (order: {})",
                    evicted.id, evicted.order_id
                );
            }
        }

        let id = entry.id.clone();
        entries.push_back(entry);
        
        log::info!("Order added to dead letter queue: {}", id);
        
        Ok(id)
    }

    /// Create and enqueue a failed order
    pub async fn add_failed_order(
        &self,
        order_id: &str,
        client_order_id: Option<&str>,
        symbol: &str,
        exchange: &str,
        side: &str,
        order_type: &str,
        quantity: f64,
        price: Option<f64>,
        error: &str,
        error_code: Option<&str>,
        category: FailureCategory,
    ) -> Result<String> {
        let now = crate::optimizations::timestamp::nano_timestamp() as u64;
        
        let status = if category.should_retry() {
            DeadLetterStatus::Pending
        } else {
            DeadLetterStatus::PermanentFailure
        };

        let entry = DeadLetterEntry {
            id: self.generate_entry_id(),
            order_id: order_id.to_string(),
            client_order_id: client_order_id.map(String::from),
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            side: side.to_string(),
            order_type: order_type.to_string(),
            quantity,
            price,
            error_message: error.to_string(),
            error_code: error_code.map(String::from),
            retry_count: 0,
            max_retries: self.config.max_retries,
            original_timestamp: now,
            dlq_timestamp: now,
            last_retry_timestamp: None,
            status,
            metadata: serde_json::json!({
                "failure_category": format!("{:?}", category),
            }),
        };

        self.enqueue(entry).await
    }

    /// Get all pending entries for retry
    pub async fn get_pending(&self) -> Vec<DeadLetterEntry> {
        let entries = self.entries.read().await;
        entries.iter()
            .filter(|e| e.status == DeadLetterStatus::Pending)
            .cloned()
            .collect()
    }

    /// Get entries requiring manual review
    pub async fn get_manual_review(&self) -> Vec<DeadLetterEntry> {
        let entries = self.entries.read().await;
        entries.iter()
            .filter(|e| e.status == DeadLetterStatus::RequiresManualReview)
            .cloned()
            .collect()
    }

    /// Get entry by ID
    pub async fn get(&self, id: &str) -> Option<DeadLetterEntry> {
        let entries = self.entries.read().await;
        entries.iter().find(|e| e.id == id).cloned()
    }

    /// Mark entry as retrying
    pub async fn mark_retrying(&self, id: &str) -> Result<()> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
            entry.status = DeadLetterStatus::Retrying;
            entry.last_retry_timestamp = Some(crate::optimizations::timestamp::nano_timestamp() as u64);
            entry.retry_count += 1;
        }
        Ok(())
    }

    /// Mark entry as resolved (retry succeeded)
    pub async fn mark_resolved(&self, id: &str) -> Result<()> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
            entry.status = DeadLetterStatus::Resolved;
            log::info!("DLQ entry resolved: {} (order: {})", id, entry.order_id);
        }
        Ok(())
    }

    /// Mark retry as failed
    pub async fn mark_retry_failed(&self, id: &str, new_error: &str) -> Result<()> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
            entry.error_message = new_error.to_string();
            
            if entry.retry_count >= entry.max_retries {
                entry.status = DeadLetterStatus::RequiresManualReview;
                log::warn!(
                    "DLQ entry exceeded max retries, requires manual review: {} (order: {})",
                    id, entry.order_id
                );
            } else {
                entry.status = DeadLetterStatus::Pending;
            }
        }
        Ok(())
    }

    /// Dismiss an entry (manual resolution)
    pub async fn dismiss(&self, id: &str, reason: &str) -> Result<()> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
            entry.status = DeadLetterStatus::Dismissed;
            entry.metadata["dismiss_reason"] = serde_json::json!(reason);
            log::info!("DLQ entry dismissed: {} - {}", id, reason);
        }
        Ok(())
    }

    /// Get queue statistics
    pub async fn stats(&self) -> DlqStats {
        let entries = self.entries.read().await;
        
        let mut stats = DlqStats::default();
        stats.total_entries = entries.len();
        
        for entry in entries.iter() {
            match entry.status {
                DeadLetterStatus::Pending => stats.pending += 1,
                DeadLetterStatus::Retrying => stats.retrying += 1,
                DeadLetterStatus::Resolved => stats.resolved += 1,
                DeadLetterStatus::RequiresManualReview => stats.requires_review += 1,
                DeadLetterStatus::Dismissed => stats.dismissed += 1,
                DeadLetterStatus::PermanentFailure => stats.permanent_failures += 1,
            }
        }
        
        stats
    }

    /// Persist queue to disk
    pub async fn persist(&self) -> Result<()> {
        if let Some(ref path) = self.config.persistence_path {
            let entries = self.entries.read().await;
            let json = serde_json::to_string_pretty(&*entries)
                .context("Failed to serialize DLQ")?;
            
            tokio::fs::write(path, json).await
                .context("Failed to write DLQ file")?;
            
            log::debug!("DLQ persisted to {:?} ({} entries)", path, entries.len());
        }
        Ok(())
    }

    /// Load queue from disk
    pub async fn load(&self) -> Result<()> {
        if let Some(ref path) = self.config.persistence_path {
            if path.exists() {
                let json = tokio::fs::read_to_string(path).await
                    .context("Failed to read DLQ file")?;
                
                let loaded: VecDeque<DeadLetterEntry> = serde_json::from_str(&json)
                    .context("Failed to deserialize DLQ")?;
                
                let mut entries = self.entries.write().await;
                *entries = loaded;
                
                log::info!("DLQ loaded from {:?} ({} entries)", path, entries.len());
            }
        }
        Ok(())
    }

    /// Clean up old resolved/dismissed entries
    pub async fn cleanup(&self, max_age_hours: u64) -> usize {
        let cutoff = crate::optimizations::timestamp::nano_timestamp() as u64 
            - (max_age_hours * 3600 * 1_000_000_000);
        
        let mut entries = self.entries.write().await;
        let before = entries.len();
        
        entries.retain(|e| {
            // Keep non-terminal states
            if !matches!(e.status, 
                DeadLetterStatus::Resolved | 
                DeadLetterStatus::Dismissed | 
                DeadLetterStatus::PermanentFailure
            ) {
                return true;
            }
            // Keep terminal states newer than cutoff
            e.dlq_timestamp > cutoff
        });
        
        let removed = before - entries.len();
        if removed > 0 {
            log::info!("DLQ cleanup: removed {} old entries", removed);
        }
        removed
    }

    /// Export entries as JSON for API/debugging
    pub async fn export_json(&self) -> serde_json::Value {
        let entries = self.entries.read().await;
        serde_json::json!({
            "entries": &*entries,
            "stats": self.stats().await,
        })
    }
}

/// Dead letter queue statistics
#[derive(Debug, Default, Clone, Serialize)]
pub struct DlqStats {
    pub total_entries: usize,
    pub pending: usize,
    pub retrying: usize,
    pub resolved: usize,
    pub requires_review: usize,
    pub dismissed: usize,
    pub permanent_failures: usize,
}

/// Retry worker for automatic order retries
pub struct DlqRetryWorker {
    dlq: Arc<DeadLetterQueue>,
    running: std::sync::atomic::AtomicBool,
}

impl DlqRetryWorker {
    pub fn new(dlq: Arc<DeadLetterQueue>) -> Self {
        Self {
            dlq,
            running: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Start the retry worker
    pub async fn start<F, Fut>(&self, retry_fn: F) 
    where
        F: Fn(DeadLetterEntry) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        if self.running.swap(true, std::sync::atomic::Ordering::SeqCst) {
            log::warn!("DLQ retry worker already running");
            return;
        }

        log::info!("Starting DLQ retry worker");
        
        while self.running.load(std::sync::atomic::Ordering::Relaxed) {
            let pending = self.dlq.get_pending().await;
            
            for entry in pending {
                if !self.running.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                // Check if enough time has passed since last retry
                if let Some(last_retry) = entry.last_retry_timestamp {
                    let now = crate::optimizations::timestamp::nano_timestamp() as u64;
                    let min_delay_ns = 1_000_000 * FailureCategory::Unknown.retry_delay_ms(entry.retry_count);
                    if now - last_retry < min_delay_ns {
                        continue;
                    }
                }

                log::info!("Retrying DLQ entry: {} (attempt {})", entry.id, entry.retry_count + 1);
                
                if let Err(e) = self.dlq.mark_retrying(&entry.id).await {
                    log::error!("Failed to mark entry as retrying: {:?}", e);
                    continue;
                }

                match retry_fn(entry.clone()).await {
                    Ok(_) => {
                        if let Err(e) = self.dlq.mark_resolved(&entry.id).await {
                            log::error!("Failed to mark entry as resolved: {:?}", e);
                        }
                    }
                    Err(e) => {
                        let error_msg = format!("{:?}", e);
                        if let Err(e) = self.dlq.mark_retry_failed(&entry.id, &error_msg).await {
                            log::error!("Failed to mark retry as failed: {:?}", e);
                        }
                    }
                }
            }

            // Sleep between retry cycles
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }

        log::info!("DLQ retry worker stopped");
    }

    /// Stop the retry worker
    pub fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dlq_add_and_retrieve() {
        let dlq = DeadLetterQueue::new(DlqConfig {
            persistence_path: None,
            ..Default::default()
        });

        let id = dlq.add_failed_order(
            "order123",
            Some("client456"),
            "BTC/USD",
            "kraken",
            "buy",
            "limit",
            0.1,
            Some(50000.0),
            "Network timeout",
            None,
            FailureCategory::Network,
        ).await.unwrap();

        let entry = dlq.get(&id).await.unwrap();
        assert_eq!(entry.order_id, "order123");
        assert_eq!(entry.status, DeadLetterStatus::Pending);
        assert_eq!(entry.retry_count, 0);
    }

    #[tokio::test]
    async fn test_dlq_retry_flow() {
        let dlq = DeadLetterQueue::new(DlqConfig {
            persistence_path: None,
            max_retries: 2,
            ..Default::default()
        });

        let id = dlq.add_failed_order(
            "order123",
            None,
            "BTC/USD",
            "kraken",
            "buy",
            "market",
            0.1,
            None,
            "Rate limited",
            Some("429"),
            FailureCategory::RateLimit,
        ).await.unwrap();

        // First retry
        dlq.mark_retrying(&id).await.unwrap();
        dlq.mark_retry_failed(&id, "Still rate limited").await.unwrap();
        
        let entry = dlq.get(&id).await.unwrap();
        assert_eq!(entry.retry_count, 1);
        assert_eq!(entry.status, DeadLetterStatus::Pending);

        // Second retry
        dlq.mark_retrying(&id).await.unwrap();
        dlq.mark_retry_failed(&id, "Rate limited again").await.unwrap();
        
        let entry = dlq.get(&id).await.unwrap();
        assert_eq!(entry.retry_count, 2);
        assert_eq!(entry.status, DeadLetterStatus::RequiresManualReview);
    }

    #[tokio::test]
    async fn test_dlq_stats() {
        let dlq = DeadLetterQueue::new(DlqConfig {
            persistence_path: None,
            ..Default::default()
        });

        dlq.add_failed_order("o1", None, "BTC", "ex", "buy", "limit", 1.0, None, "err", None, FailureCategory::Network).await.unwrap();
        dlq.add_failed_order("o2", None, "BTC", "ex", "buy", "limit", 1.0, None, "err", None, FailureCategory::InvalidOrder).await.unwrap();

        let stats = dlq.stats().await;
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.pending, 1); // Network error is retryable
        assert_eq!(stats.permanent_failures, 1); // InvalidOrder is permanent
    }
}
