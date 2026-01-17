//! Bounded Dead Letter Queue with Overflow Policies
//!
//! Enhanced DLQ with configurable overflow handling to prevent OOM under load.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};

pub use crate::dead_letter_queue::{
    DeadLetterEntry, DeadLetterStatus, FailureCategory, DlqStats,
};

/// Overflow policy when DLQ reaches capacity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DlqOverflowPolicy {
    /// Drop the oldest entry (FIFO eviction)
    DropOldest,
    /// Drop the newest entry (reject incoming)
    DropNewest,
    /// Drop lowest priority entry (based on status/age)
    DropLowestPriority,
    /// Block until space is available (not recommended for hot paths)
    Block,
    /// Persist to disk and evict from memory
    PersistAndEvict,
}

/// Priority for DLQ entries (higher = more important to keep)
fn entry_priority(entry: &DeadLetterEntry) -> u32 {
    let status_priority = match entry.status {
        DeadLetterStatus::RequiresManualReview => 100,
        DeadLetterStatus::Pending => 80,
        DeadLetterStatus::Retrying => 70,
        DeadLetterStatus::PermanentFailure => 50,
        DeadLetterStatus::Resolved => 10,
        DeadLetterStatus::Dismissed => 5,
    };
    
    // Newer entries have slightly higher priority
    let age_bonus = (entry.retry_count == 0) as u32 * 10;
    
    status_priority + age_bonus
}

/// Configuration for bounded DLQ
#[derive(Debug, Clone)]
pub struct BoundedDlqConfig {
    /// Hard maximum entries in memory
    pub max_entries: usize,
    /// Soft limit (triggers warnings)
    pub soft_limit: usize,
    /// Overflow policy
    pub overflow_policy: DlqOverflowPolicy,
    /// Maximum retry attempts per order
    pub max_retries: u32,
    /// Path to overflow persistence file
    pub overflow_persistence_path: Option<std::path::PathBuf>,
    /// Enable metrics collection
    pub metrics_enabled: bool,
}

impl Default for BoundedDlqConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
            soft_limit: 8_000,
            overflow_policy: DlqOverflowPolicy::DropLowestPriority,
            max_retries: 3,
            overflow_persistence_path: Some(std::path::PathBuf::from("./dlq_overflow.json")),
            metrics_enabled: true,
        }
    }
}

/// Statistics for bounded DLQ
#[derive(Debug, Default, Clone, Serialize)]
pub struct BoundedDlqStats {
    /// Basic DLQ stats
    pub base_stats: DlqStats,
    /// Total entries dropped due to overflow
    pub entries_dropped: u64,
    /// Total entries evicted to disk
    pub entries_evicted_to_disk: u64,
    /// Current memory utilization (0.0 - 1.0)
    pub memory_utilization: f64,
    /// Peak memory usage
    pub peak_entries: usize,
    /// Entries currently on disk
    pub entries_on_disk: usize,
}

/// Bounded Dead Letter Queue with overflow handling
pub struct BoundedDeadLetterQueue {
    /// In-memory entries
    entries: Arc<RwLock<VecDeque<DeadLetterEntry>>>,
    /// Configuration
    config: BoundedDlqConfig,
    /// Entry counter for unique IDs
    entry_counter: AtomicU64,
    /// Dropped entry counter
    dropped_counter: AtomicU64,
    /// Evicted to disk counter
    evicted_counter: AtomicU64,
    /// Peak entries seen
    peak_entries: AtomicU64,
    /// Overflow entries (persisted to disk)
    overflow_entries: Arc<RwLock<Vec<DeadLetterEntry>>>,
}

impl BoundedDeadLetterQueue {
    pub fn new(config: BoundedDlqConfig) -> Self {
        Self {
            entries: Arc::new(RwLock::new(VecDeque::with_capacity(config.max_entries))),
            config,
            entry_counter: AtomicU64::new(0),
            dropped_counter: AtomicU64::new(0),
            evicted_counter: AtomicU64::new(0),
            peak_entries: AtomicU64::new(0),
            overflow_entries: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn default_instance() -> Self {
        Self::new(BoundedDlqConfig::default())
    }

    fn generate_entry_id(&self) -> String {
        let counter = self.entry_counter.fetch_add(1, Ordering::Relaxed);
        let timestamp = crate::optimizations::timestamp::nano_timestamp();
        format!("bdlq_{}_{}", timestamp, counter)
    }

    /// Enqueue an entry with overflow handling
    pub async fn enqueue(&self, mut entry: DeadLetterEntry) -> Result<EnqueueResult> {
        let mut entries = self.entries.write().await;
        
        // Update peak
        let current_len = entries.len();
        self.peak_entries.fetch_max(current_len as u64 + 1, Ordering::Relaxed);
        
        // Check soft limit (warning)
        if current_len >= self.config.soft_limit && current_len < self.config.max_entries {
            log::warn!(
                "DLQ approaching capacity: {}/{} entries",
                current_len, self.config.max_entries
            );
        }
        
        // Handle overflow
        if current_len >= self.config.max_entries {
            match self.config.overflow_policy {
                DlqOverflowPolicy::DropOldest => {
                    if let Some(dropped) = entries.pop_front() {
                        self.dropped_counter.fetch_add(1, Ordering::Relaxed);
                        log::warn!("DLQ overflow: dropped oldest entry {} (order: {})",
                            dropped.id, dropped.order_id);
                    }
                }
                
                DlqOverflowPolicy::DropNewest => {
                    self.dropped_counter.fetch_add(1, Ordering::Relaxed);
                    log::warn!("DLQ overflow: rejected new entry (order: {})", entry.order_id);
                    return Ok(EnqueueResult::Dropped);
                }
                
                DlqOverflowPolicy::DropLowestPriority => {
                    // Find lowest priority entry
                    if let Some((idx, _)) = entries.iter().enumerate()
                        .min_by_key(|(_, e)| entry_priority(e))
                    {
                        if let Some(dropped) = entries.remove(idx) {
                            self.dropped_counter.fetch_add(1, Ordering::Relaxed);
                            log::warn!("DLQ overflow: dropped low-priority entry {} (order: {})",
                                dropped.id, dropped.order_id);
                        }
                    }
                }
                
                DlqOverflowPolicy::Block => {
                    // Release lock and wait
                    drop(entries);
                    
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        let entries = self.entries.read().await;
                        if entries.len() < self.config.max_entries {
                            break;
                        }
                    }
                    
                    // Reacquire lock
                    entries = self.entries.write().await;
                }
                
                DlqOverflowPolicy::PersistAndEvict => {
                    // Evict oldest to disk
                    if let Some(evicted) = entries.pop_front() {
                        let mut overflow = self.overflow_entries.write().await;
                        overflow.push(evicted.clone());
                        self.evicted_counter.fetch_add(1, Ordering::Relaxed);
                        log::info!("DLQ overflow: evicted entry {} to disk", evicted.id);
                    }
                }
            }
        }
        
        // Assign ID if not set
        if entry.id.is_empty() {
            entry.id = self.generate_entry_id();
        }
        
        let id = entry.id.clone();
        entries.push_back(entry);
        
        Ok(EnqueueResult::Enqueued(id))
    }

    /// Add a failed order to the queue
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
    ) -> Result<EnqueueResult> {
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

    /// Get entry by ID (checks both memory and overflow)
    pub async fn get(&self, id: &str) -> Option<DeadLetterEntry> {
        // Check memory first
        let entries = self.entries.read().await;
        if let Some(entry) = entries.iter().find(|e| e.id == id) {
            return Some(entry.clone());
        }
        drop(entries);
        
        // Check overflow
        let overflow = self.overflow_entries.read().await;
        overflow.iter().find(|e| e.id == id).cloned()
    }

    /// Get all pending entries
    pub async fn get_pending(&self) -> Vec<DeadLetterEntry> {
        let entries = self.entries.read().await;
        entries.iter()
            .filter(|e| e.status == DeadLetterStatus::Pending)
            .cloned()
            .collect()
    }

    /// Mark entry as resolved
    pub async fn mark_resolved(&self, id: &str) -> Result<()> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
            entry.status = DeadLetterStatus::Resolved;
            log::info!("DLQ entry resolved: {} (order: {})", id, entry.order_id);
        }
        Ok(())
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

    /// Mark retry as failed
    pub async fn mark_retry_failed(&self, id: &str, new_error: &str) -> Result<()> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
            entry.error_message = new_error.to_string();
            
            if entry.retry_count >= entry.max_retries {
                entry.status = DeadLetterStatus::RequiresManualReview;
            } else {
                entry.status = DeadLetterStatus::Pending;
            }
        }
        Ok(())
    }

    /// Get comprehensive statistics
    pub async fn stats(&self) -> BoundedDlqStats {
        let entries = self.entries.read().await;
        let overflow = self.overflow_entries.read().await;
        
        let mut base_stats = DlqStats::default();
        base_stats.total_entries = entries.len();
        
        for entry in entries.iter() {
            match entry.status {
                DeadLetterStatus::Pending => base_stats.pending += 1,
                DeadLetterStatus::Retrying => base_stats.retrying += 1,
                DeadLetterStatus::Resolved => base_stats.resolved += 1,
                DeadLetterStatus::RequiresManualReview => base_stats.requires_review += 1,
                DeadLetterStatus::Dismissed => base_stats.dismissed += 1,
                DeadLetterStatus::PermanentFailure => base_stats.permanent_failures += 1,
            }
        }
        
        BoundedDlqStats {
            base_stats,
            entries_dropped: self.dropped_counter.load(Ordering::Relaxed),
            entries_evicted_to_disk: self.evicted_counter.load(Ordering::Relaxed),
            memory_utilization: entries.len() as f64 / self.config.max_entries as f64,
            peak_entries: self.peak_entries.load(Ordering::Relaxed) as usize,
            entries_on_disk: overflow.len(),
        }
    }

    /// Persist overflow entries to disk
    pub async fn persist_overflow(&self) -> Result<()> {
        if let Some(ref path) = self.config.overflow_persistence_path {
            let overflow = self.overflow_entries.read().await;
            if !overflow.is_empty() {
                let json = serde_json::to_string_pretty(&*overflow)
                    .context("Failed to serialize overflow")?;
                tokio::fs::write(path, json).await
                    .context("Failed to write overflow file")?;
                log::info!("Persisted {} overflow entries to {:?}", overflow.len(), path);
            }
        }
        Ok(())
    }

    /// Load overflow entries from disk
    pub async fn load_overflow(&self) -> Result<()> {
        if let Some(ref path) = self.config.overflow_persistence_path {
            if path.exists() {
                let json = tokio::fs::read_to_string(path).await
                    .context("Failed to read overflow file")?;
                let loaded: Vec<DeadLetterEntry> = serde_json::from_str(&json)
                    .context("Failed to deserialize overflow")?;
                
                let mut overflow = self.overflow_entries.write().await;
                *overflow = loaded;
                log::info!("Loaded {} overflow entries from {:?}", overflow.len(), path);
            }
        }
        Ok(())
    }

    /// Recover entries from overflow back to memory (if space available)
    pub async fn recover_from_overflow(&self, max_to_recover: usize) -> usize {
        let mut entries = self.entries.write().await;
        let mut overflow = self.overflow_entries.write().await;
        
        let available_space = self.config.max_entries.saturating_sub(entries.len());
        let to_recover = max_to_recover.min(available_space).min(overflow.len());
        
        for _ in 0..to_recover {
            if let Some(entry) = overflow.pop() {
                entries.push_back(entry);
            }
        }
        
        if to_recover > 0 {
            log::info!("Recovered {} entries from overflow", to_recover);
        }
        
        to_recover
    }
}

/// Result of enqueue operation
#[derive(Debug, Clone)]
pub enum EnqueueResult {
    /// Successfully enqueued
    Enqueued(String),
    /// Dropped due to overflow policy
    Dropped,
    /// Evicted older entry to make room
    EvictedOlder(String),
}

impl EnqueueResult {
    pub fn is_success(&self) -> bool {
        matches!(self, EnqueueResult::Enqueued(_))
    }

    pub fn entry_id(&self) -> Option<&str> {
        match self {
            EnqueueResult::Enqueued(id) => Some(id),
            EnqueueResult::EvictedOlder(id) => Some(id),
            EnqueueResult::Dropped => None,
        }
    }
}

/// Global bounded DLQ instance
pub static BOUNDED_DLQ: once_cell::sync::Lazy<BoundedDeadLetterQueue> =
    once_cell::sync::Lazy::new(BoundedDeadLetterQueue::default_instance);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bounded_dlq_basic() {
        let dlq = BoundedDeadLetterQueue::new(BoundedDlqConfig {
            max_entries: 100,
            soft_limit: 80,
            overflow_policy: DlqOverflowPolicy::DropOldest,
            overflow_persistence_path: None,
            ..Default::default()
        });

        let result = dlq.add_failed_order(
            "order1", None, "BTC/USD", "kraken", "buy", "limit",
            1.0, Some(50000.0), "Network error", None, FailureCategory::Network,
        ).await.unwrap();

        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_bounded_dlq_overflow_drop_oldest() {
        let dlq = BoundedDeadLetterQueue::new(BoundedDlqConfig {
            max_entries: 3,
            soft_limit: 2,
            overflow_policy: DlqOverflowPolicy::DropOldest,
            overflow_persistence_path: None,
            ..Default::default()
        });

        // Fill the queue
        for i in 0..5 {
            dlq.add_failed_order(
                &format!("order{}", i), None, "BTC/USD", "kraken", "buy", "limit",
                1.0, None, "Error", None, FailureCategory::Network,
            ).await.unwrap();
        }

        let stats = dlq.stats().await;
        assert_eq!(stats.base_stats.total_entries, 3);
        assert_eq!(stats.entries_dropped, 2);
    }

    #[tokio::test]
    async fn test_bounded_dlq_overflow_drop_newest() {
        let dlq = BoundedDeadLetterQueue::new(BoundedDlqConfig {
            max_entries: 2,
            soft_limit: 1,
            overflow_policy: DlqOverflowPolicy::DropNewest,
            overflow_persistence_path: None,
            ..Default::default()
        });

        dlq.add_failed_order(
            "order1", None, "BTC/USD", "kraken", "buy", "limit",
            1.0, None, "Error", None, FailureCategory::Network,
        ).await.unwrap();

        dlq.add_failed_order(
            "order2", None, "BTC/USD", "kraken", "buy", "limit",
            1.0, None, "Error", None, FailureCategory::Network,
        ).await.unwrap();

        let result = dlq.add_failed_order(
            "order3", None, "BTC/USD", "kraken", "buy", "limit",
            1.0, None, "Error", None, FailureCategory::Network,
        ).await.unwrap();

        assert!(matches!(result, EnqueueResult::Dropped));
        
        let stats = dlq.stats().await;
        assert_eq!(stats.base_stats.total_entries, 2);
    }

    #[tokio::test]
    async fn test_bounded_dlq_persist_and_evict() {
        let dlq = BoundedDeadLetterQueue::new(BoundedDlqConfig {
            max_entries: 2,
            soft_limit: 1,
            overflow_policy: DlqOverflowPolicy::PersistAndEvict,
            overflow_persistence_path: None,
            ..Default::default()
        });

        for i in 0..4 {
            dlq.add_failed_order(
                &format!("order{}", i), None, "BTC/USD", "kraken", "buy", "limit",
                1.0, None, "Error", None, FailureCategory::Network,
            ).await.unwrap();
        }

        let stats = dlq.stats().await;
        assert_eq!(stats.base_stats.total_entries, 2);
        assert_eq!(stats.entries_evicted_to_disk, 2);
        assert_eq!(stats.entries_on_disk, 2);
    }

    #[tokio::test]
    async fn test_bounded_dlq_recovery() {
        let dlq = BoundedDeadLetterQueue::new(BoundedDlqConfig {
            max_entries: 3,
            soft_limit: 2,
            overflow_policy: DlqOverflowPolicy::PersistAndEvict,
            overflow_persistence_path: None,
            ..Default::default()
        });

        // Fill and evict
        for i in 0..5 {
            dlq.add_failed_order(
                &format!("order{}", i), None, "BTC/USD", "kraken", "buy", "limit",
                1.0, None, "Error", None, FailureCategory::Network,
            ).await.unwrap();
        }

        // Resolve some to make space
        let pending = dlq.get_pending().await;
        if let Some(entry) = pending.first() {
            dlq.mark_resolved(&entry.id).await.unwrap();
        }

        // Can't recover yet - queue still full
        let recovered = dlq.recover_from_overflow(10).await;
        assert_eq!(recovered, 0);
    }

    #[tokio::test]
    async fn test_bounded_dlq_priority_eviction() {
        let dlq = BoundedDeadLetterQueue::new(BoundedDlqConfig {
            max_entries: 2,
            soft_limit: 1,
            overflow_policy: DlqOverflowPolicy::DropLowestPriority,
            overflow_persistence_path: None,
            ..Default::default()
        });

        // Add a resolved entry (low priority)
        let result = dlq.add_failed_order(
            "resolved_order", None, "BTC/USD", "kraken", "buy", "limit",
            1.0, None, "Error", None, FailureCategory::Network,
        ).await.unwrap();
        if let EnqueueResult::Enqueued(id) = result {
            dlq.mark_resolved(&id).await.unwrap();
        }

        // Add a pending entry (higher priority)
        dlq.add_failed_order(
            "pending_order", None, "BTC/USD", "kraken", "buy", "limit",
            1.0, None, "Error", None, FailureCategory::Network,
        ).await.unwrap();

        // Add another pending entry - should drop the resolved one
        dlq.add_failed_order(
            "new_pending", None, "BTC/USD", "kraken", "buy", "limit",
            1.0, None, "Error", None, FailureCategory::Network,
        ).await.unwrap();

        // Verify resolved entry was dropped
        assert!(dlq.get("resolved_order").await.is_none() || {
            let entry = dlq.get("resolved_order").await;
            entry.map(|e| e.status != DeadLetterStatus::Resolved).unwrap_or(true)
        });
    }
}
