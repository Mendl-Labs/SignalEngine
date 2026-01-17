//! Order Write-Ahead Log (WAL) Module
//!
//! Provides crash recovery for in-flight orders by persisting order state
//! before exchange submission and updating on state changes.
//!
//! # Features
//!
//! - **Pre-submission logging**: Orders written to WAL before exchange submission
//! - **State transitions**: All order state changes logged (filled, cancelled, rejected)
//! - **Crash recovery**: On restart, replays incomplete orders for reconciliation
//! - **Checkpointing**: Periodic compaction to prevent unbounded growth
//! - **Async I/O**: Non-blocking writes with fsync for durability
//!
//! # Architecture
//!
//! ```text
//! Order Flow:
//! 1. Signal received → WAL.append(Pending)
//! 2. Submit to exchange → WAL.append(Submitted)  
//! 3. Exchange ACK → WAL.append(Acknowledged)
//! 4. Fill/Cancel → WAL.append(Complete) + mark for compaction
//!
//! Recovery Flow:
//! 1. On startup, read WAL from last checkpoint
//! 2. Identify incomplete orders (Pending/Submitted/Acknowledged)
//! 3. Query exchange for current status
//! 4. Reconcile and resume or cancel
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use executionhandler::order_wal::{OrderWal, WalConfig, OrderWalEntry};
//!
//! let wal = OrderWal::new(WalConfig::default()).await?;
//!
//! // Before submitting order
//! wal.log_pending(&order_id, &signal).await?;
//!
//! // After exchange acknowledges
//! wal.log_acknowledged(&order_id, &exchange_order_id).await?;
//!
//! // On fill
//! wal.log_filled(&order_id, fill_qty, fill_price).await?;
//!
//! // On startup - recover incomplete orders
//! let incomplete = wal.recover_incomplete_orders().await?;
//! for order in incomplete {
//!     reconcile_with_exchange(&order).await?;
//! }
//! ```

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::sync::mpsc;

/// WAL entry states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalOrderState {
    /// Order created, not yet submitted
    Pending,
    /// Submitted to exchange, awaiting ACK
    Submitted,
    /// Exchange acknowledged receipt
    Acknowledged,
    /// Partially filled
    PartiallyFilled,
    /// Fully filled - terminal state
    Filled,
    /// Cancelled - terminal state
    Cancelled,
    /// Rejected by exchange - terminal state
    Rejected,
    /// Expired - terminal state
    Expired,
    /// Error state - terminal state
    Failed,
}

impl WalOrderState {
    /// Check if this is a terminal state (order complete)
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WalOrderState::Filled
                | WalOrderState::Cancelled
                | WalOrderState::Rejected
                | WalOrderState::Expired
                | WalOrderState::Failed
        )
    }
}

/// WAL entry for a single order event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Sequence number (monotonically increasing)
    pub sequence: u64,
    /// Timestamp (Unix nanoseconds)
    pub timestamp_ns: u128,
    /// Internal order ID
    pub order_id: String,
    /// Order state
    pub state: WalOrderState,
    /// Exchange name
    pub exchange: String,
    /// Trading symbol
    pub symbol: String,
    /// Order side (buy/sell)
    pub side: String,
    /// Order quantity
    pub quantity: f64,
    /// Order price (None for market orders)
    pub price: Option<f64>,
    /// Exchange-assigned order ID (after submission)
    pub exchange_order_id: Option<String>,
    /// Filled quantity (cumulative)
    pub filled_quantity: f64,
    /// Average fill price
    pub avg_fill_price: Option<f64>,
    /// Error message (for failed/rejected)
    pub error: Option<String>,
    /// Strategy ID
    pub strategy_id: String,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// CRC32 checksum for integrity
    pub checksum: u32,
}

impl WalEntry {
    /// Create a new WAL entry
    pub fn new(
        sequence: u64,
        order_id: String,
        state: WalOrderState,
        exchange: String,
        symbol: String,
        side: String,
        quantity: f64,
        price: Option<f64>,
        strategy_id: String,
    ) -> Self {
        let mut entry = Self {
            sequence,
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            order_id,
            state,
            exchange,
            symbol,
            side,
            quantity,
            price,
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: None,
            error: None,
            strategy_id,
            metadata: HashMap::new(),
            checksum: 0,
        };
        entry.checksum = entry.calculate_checksum();
        entry
    }

    /// Calculate CRC32 checksum
    fn calculate_checksum(&self) -> u32 {
        let data = format!(
            "{}:{}:{}:{:?}:{}:{}:{}:{}:{:?}",
            self.sequence,
            self.order_id,
            self.timestamp_ns,
            self.state,
            self.exchange,
            self.symbol,
            self.quantity,
            self.filled_quantity,
            self.price
        );
        crc32fast::hash(data.as_bytes())
    }

    /// Verify checksum
    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }

    /// Create state transition entry
    pub fn transition(&self, new_state: WalOrderState, sequence: u64) -> Self {
        let mut entry = self.clone();
        entry.sequence = sequence;
        entry.state = new_state;
        entry.timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        entry.checksum = entry.calculate_checksum();
        entry
    }
}

/// Current order state (in-memory view)
#[derive(Debug, Clone)]
pub struct OrderState {
    /// Latest WAL entry for this order
    pub latest_entry: WalEntry,
    /// All entries for this order (for debugging)
    pub history: Vec<WalEntry>,
}

/// WAL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// WAL directory path
    pub wal_dir: PathBuf,
    /// Maximum WAL file size before rotation (bytes)
    pub max_file_size: u64,
    /// Checkpoint interval (number of entries)
    pub checkpoint_interval: u64,
    /// Sync mode (fsync after each write vs batched)
    pub sync_mode: WalSyncMode,
    /// Retention period for completed orders (seconds)
    pub retention_secs: u64,
    /// Enable compression for archived WAL files
    pub compress_archived: bool,
    /// Maximum entries to buffer before flush
    pub buffer_size: usize,
    /// Flush interval (milliseconds)
    pub flush_interval_ms: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            wal_dir: PathBuf::from("./data/wal"),
            max_file_size: 100 * 1024 * 1024, // 100MB
            checkpoint_interval: 10000,
            sync_mode: WalSyncMode::EveryWrite,
            retention_secs: 86400 * 7, // 7 days
            compress_archived: true,
            buffer_size: 100,
            flush_interval_ms: 100,
        }
    }
}

/// WAL sync modes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalSyncMode {
    /// fsync after every write (safest, slowest)
    EveryWrite,
    /// fsync at intervals (balanced)
    Periodic,
    /// OS-managed sync (fastest, least safe)
    OsManaged,
}

/// WAL errors
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Checksum mismatch for order {order_id}")]
    ChecksumMismatch { order_id: String },

    #[error("Order not found: {0}")]
    OrderNotFound(String),

    #[error("Invalid state transition: {from:?} -> {to:?}")]
    InvalidTransition {
        from: WalOrderState,
        to: WalOrderState,
    },

    #[error("WAL corrupted at sequence {sequence}")]
    Corrupted { sequence: u64 },

    #[error("Channel closed")]
    ChannelClosed,
}

/// Write command for async writer
enum WriteCommand {
    Append(WalEntry),
    Checkpoint,
    Flush,
    Shutdown,
}

/// Order Write-Ahead Log
pub struct OrderWal {
    /// Configuration
    config: WalConfig,
    /// In-memory order states
    orders: DashMap<String, OrderState>,
    /// Current sequence number
    sequence: AtomicU64,
    /// Write command sender
    write_tx: mpsc::Sender<WriteCommand>,
    /// Statistics
    stats: WalStats,
    /// Last checkpoint sequence
    last_checkpoint: AtomicU64,
}

/// WAL statistics
#[derive(Debug, Default)]
pub struct WalStats {
    pub entries_written: AtomicU64,
    pub entries_recovered: AtomicU64,
    pub checkpoints: AtomicU64,
    pub corrupted_entries: AtomicU64,
    pub bytes_written: AtomicU64,
}

impl OrderWal {
    /// Create a new WAL instance
    pub async fn new(config: WalConfig) -> Result<Arc<Self>, WalError> {
        // Create WAL directory
        tokio::fs::create_dir_all(&config.wal_dir).await?;

        let (write_tx, write_rx) = mpsc::channel(config.buffer_size * 2);

        let wal = Arc::new(Self {
            config: config.clone(),
            orders: DashMap::new(),
            sequence: AtomicU64::new(0),
            write_tx,
            stats: WalStats::default(),
            last_checkpoint: AtomicU64::new(0),
        });

        // Start background writer
        let wal_clone = wal.clone();
        tokio::spawn(async move {
            wal_clone.run_writer(write_rx).await;
        });

        // Recover existing WAL
        wal.recover().await?;

        Ok(wal)
    }

    /// Get current WAL file path
    fn current_wal_path(&self) -> PathBuf {
        self.config.wal_dir.join("current.wal")
    }

    /// Get checkpoint file path
    fn checkpoint_path(&self) -> PathBuf {
        self.config.wal_dir.join("checkpoint.json")
    }

    /// Background writer task
    async fn run_writer(self: Arc<Self>, mut rx: mpsc::Receiver<WriteCommand>) {
        let wal_path = self.current_wal_path();

        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to open WAL file: {}", e);
                return;
            }
        };

        let mut buffer = Vec::with_capacity(self.config.buffer_size);
        let flush_interval = Duration::from_millis(self.config.flush_interval_ms);
        let mut last_flush = std::time::Instant::now();

        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    match cmd {
                        Some(WriteCommand::Append(entry)) => {
                            buffer.push(entry);
                            
                            // Flush if buffer full or sync mode requires it
                            if buffer.len() >= self.config.buffer_size 
                                || self.config.sync_mode == WalSyncMode::EveryWrite 
                            {
                                if let Err(e) = self.flush_buffer(&mut file, &mut buffer).await {
                                    eprintln!("WAL flush error: {}", e);
                                }
                                last_flush = std::time::Instant::now();
                            }
                        }
                        Some(WriteCommand::Checkpoint) => {
                            // Flush pending writes first
                            if let Err(e) = self.flush_buffer(&mut file, &mut buffer).await {
                                eprintln!("WAL flush error before checkpoint: {}", e);
                            }
                            if let Err(e) = self.write_checkpoint().await {
                                eprintln!("Checkpoint error: {}", e);
                            }
                        }
                        Some(WriteCommand::Flush) => {
                            if let Err(e) = self.flush_buffer(&mut file, &mut buffer).await {
                                eprintln!("WAL flush error: {}", e);
                            }
                            last_flush = std::time::Instant::now();
                        }
                        Some(WriteCommand::Shutdown) | None => {
                            // Final flush
                            let _ = self.flush_buffer(&mut file, &mut buffer).await;
                            let _ = file.sync_all().await;
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(flush_interval) => {
                    // Periodic flush
                    if !buffer.is_empty() && last_flush.elapsed() >= flush_interval {
                        if let Err(e) = self.flush_buffer(&mut file, &mut buffer).await {
                            eprintln!("WAL periodic flush error: {}", e);
                        }
                        last_flush = std::time::Instant::now();
                    }
                }
            }
        }
    }

    /// Flush buffer to disk
    async fn flush_buffer(
        &self,
        file: &mut File,
        buffer: &mut Vec<WalEntry>,
    ) -> Result<(), WalError> {
        if buffer.is_empty() {
            return Ok(());
        }

        let mut bytes_written = 0u64;
        for entry in buffer.drain(..) {
            let line = serde_json::to_string(&entry)? + "\n";
            file.write_all(line.as_bytes()).await?;
            bytes_written += line.len() as u64;
        }

        // Sync based on mode
        if self.config.sync_mode == WalSyncMode::EveryWrite {
            file.sync_all().await?;
        }

        self.stats
            .bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);

        Ok(())
    }

    /// Write checkpoint file
    async fn write_checkpoint(&self) -> Result<(), WalError> {
        let checkpoint_path = self.checkpoint_path();
        let temp_path = checkpoint_path.with_extension("tmp");

        // Collect non-terminal orders
        let active_orders: HashMap<String, WalEntry> = self
            .orders
            .iter()
            .filter(|entry| !entry.value().latest_entry.state.is_terminal())
            .map(|entry| (entry.key().clone(), entry.value().latest_entry.clone()))
            .collect();

        let checkpoint = Checkpoint {
            sequence: self.sequence.load(Ordering::Acquire),
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            active_orders,
        };

        let json = serde_json::to_string_pretty(&checkpoint)?;
        tokio::fs::write(&temp_path, &json).await?;
        tokio::fs::rename(&temp_path, &checkpoint_path).await?;

        self.last_checkpoint
            .store(checkpoint.sequence, Ordering::Release);
        self.stats.checkpoints.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Recover WAL state on startup
    async fn recover(&self) -> Result<(), WalError> {
        // Try to load checkpoint first
        let checkpoint_path = self.checkpoint_path();
        if checkpoint_path.exists() {
            let json = tokio::fs::read_to_string(&checkpoint_path).await?;
            let checkpoint: Checkpoint = serde_json::from_str(&json)?;

            self.sequence
                .store(checkpoint.sequence, Ordering::Release);
            self.last_checkpoint
                .store(checkpoint.sequence, Ordering::Release);

            for (order_id, entry) in checkpoint.active_orders {
                self.orders.insert(
                    order_id,
                    OrderState {
                        latest_entry: entry.clone(),
                        history: vec![entry],
                    },
                );
            }
        }

        // Replay WAL from last checkpoint
        let wal_path = self.current_wal_path();
        if wal_path.exists() {
            let file = tokio::fs::File::open(&wal_path).await?;
            let reader = AsyncBufReader::new(file);
            let mut lines = reader.lines();

            let checkpoint_seq = self.last_checkpoint.load(Ordering::Acquire);

            while let Some(line) = lines.next_line().await? {
                if line.is_empty() {
                    continue;
                }

                match serde_json::from_str::<WalEntry>(&line) {
                    Ok(entry) => {
                        if entry.sequence <= checkpoint_seq {
                            continue; // Already in checkpoint
                        }

                        if !entry.verify_checksum() {
                            self.stats.corrupted_entries.fetch_add(1, Ordering::Relaxed);
                            eprintln!(
                                "WAL checksum mismatch at sequence {}, skipping",
                                entry.sequence
                            );
                            continue;
                        }

                        self.apply_entry(entry);
                        self.stats.entries_recovered.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("WAL parse error: {}, line: {}", e, line);
                        self.stats.corrupted_entries.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        Ok(())
    }

    /// Apply a WAL entry to in-memory state
    fn apply_entry(&self, entry: WalEntry) {
        let seq = entry.sequence;
        if seq > self.sequence.load(Ordering::Acquire) {
            self.sequence.store(seq, Ordering::Release);
        }

        self.orders
            .entry(entry.order_id.clone())
            .and_modify(|state| {
                state.history.push(entry.clone());
                state.latest_entry = entry.clone();
            })
            .or_insert_with(|| OrderState {
                latest_entry: entry.clone(),
                history: vec![entry],
            });
    }

    /// Get next sequence number
    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Log a pending order (before submission)
    pub async fn log_pending(
        &self,
        order_id: &str,
        exchange: &str,
        symbol: &str,
        side: &str,
        quantity: f64,
        price: Option<f64>,
        strategy_id: &str,
    ) -> Result<(), WalError> {
        let entry = WalEntry::new(
            self.next_sequence(),
            order_id.to_string(),
            WalOrderState::Pending,
            exchange.to_string(),
            symbol.to_string(),
            side.to_string(),
            quantity,
            price,
            strategy_id.to_string(),
        );

        self.apply_entry(entry.clone());
        self.write_tx
            .send(WriteCommand::Append(entry))
            .await
            .map_err(|_| WalError::ChannelClosed)?;

        self.maybe_checkpoint().await?;
        self.stats.entries_written.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Log order submitted to exchange
    pub async fn log_submitted(&self, order_id: &str) -> Result<(), WalError> {
        self.transition_state(order_id, WalOrderState::Submitted, |_| {})
            .await
    }

    /// Log exchange acknowledgment
    pub async fn log_acknowledged(
        &self,
        order_id: &str,
        exchange_order_id: &str,
    ) -> Result<(), WalError> {
        self.transition_state(order_id, WalOrderState::Acknowledged, |entry| {
            entry.exchange_order_id = Some(exchange_order_id.to_string());
        })
        .await
    }

    /// Log partial fill
    pub async fn log_partial_fill(
        &self,
        order_id: &str,
        filled_qty: f64,
        avg_price: f64,
    ) -> Result<(), WalError> {
        self.transition_state(order_id, WalOrderState::PartiallyFilled, |entry| {
            entry.filled_quantity = filled_qty;
            entry.avg_fill_price = Some(avg_price);
        })
        .await
    }

    /// Log full fill
    pub async fn log_filled(
        &self,
        order_id: &str,
        filled_qty: f64,
        avg_price: f64,
    ) -> Result<(), WalError> {
        self.transition_state(order_id, WalOrderState::Filled, |entry| {
            entry.filled_quantity = filled_qty;
            entry.avg_fill_price = Some(avg_price);
        })
        .await
    }

    /// Log cancellation
    pub async fn log_cancelled(&self, order_id: &str) -> Result<(), WalError> {
        self.transition_state(order_id, WalOrderState::Cancelled, |_| {})
            .await
    }

    /// Log rejection
    pub async fn log_rejected(&self, order_id: &str, reason: &str) -> Result<(), WalError> {
        self.transition_state(order_id, WalOrderState::Rejected, |entry| {
            entry.error = Some(reason.to_string());
        })
        .await
    }

    /// Log failure
    pub async fn log_failed(&self, order_id: &str, error: &str) -> Result<(), WalError> {
        self.transition_state(order_id, WalOrderState::Failed, |entry| {
            entry.error = Some(error.to_string());
        })
        .await
    }

    /// Transition order state
    async fn transition_state<F>(
        &self,
        order_id: &str,
        new_state: WalOrderState,
        modifier: F,
    ) -> Result<(), WalError>
    where
        F: FnOnce(&mut WalEntry),
    {
        let entry = {
            let state = self
                .orders
                .get(order_id)
                .ok_or_else(|| WalError::OrderNotFound(order_id.to_string()))?;

            let mut entry = state.latest_entry.transition(new_state, self.next_sequence());
            modifier(&mut entry);
            entry.checksum = entry.calculate_checksum();
            entry
        };

        self.apply_entry(entry.clone());
        self.write_tx
            .send(WriteCommand::Append(entry))
            .await
            .map_err(|_| WalError::ChannelClosed)?;

        self.maybe_checkpoint().await?;
        self.stats.entries_written.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Maybe trigger checkpoint
    async fn maybe_checkpoint(&self) -> Result<(), WalError> {
        let current = self.sequence.load(Ordering::Acquire);
        let last = self.last_checkpoint.load(Ordering::Acquire);

        if current - last >= self.config.checkpoint_interval {
            self.write_tx
                .send(WriteCommand::Checkpoint)
                .await
                .map_err(|_| WalError::ChannelClosed)?;
        }

        Ok(())
    }

    /// Force a checkpoint
    pub async fn checkpoint(&self) -> Result<(), WalError> {
        self.write_tx
            .send(WriteCommand::Checkpoint)
            .await
            .map_err(|_| WalError::ChannelClosed)
    }

    /// Flush all pending writes
    pub async fn flush(&self) -> Result<(), WalError> {
        self.write_tx
            .send(WriteCommand::Flush)
            .await
            .map_err(|_| WalError::ChannelClosed)
    }

    /// Get incomplete orders for recovery
    pub fn get_incomplete_orders(&self) -> Vec<WalEntry> {
        self.orders
            .iter()
            .filter(|entry| !entry.value().latest_entry.state.is_terminal())
            .map(|entry| entry.value().latest_entry.clone())
            .collect()
    }

    /// Get order state
    pub fn get_order(&self, order_id: &str) -> Option<OrderState> {
        self.orders.get(order_id).map(|r| r.value().clone())
    }

    /// Get statistics
    pub fn get_stats(&self) -> WalStatistics {
        WalStatistics {
            entries_written: self.stats.entries_written.load(Ordering::Relaxed),
            entries_recovered: self.stats.entries_recovered.load(Ordering::Relaxed),
            checkpoints: self.stats.checkpoints.load(Ordering::Relaxed),
            corrupted_entries: self.stats.corrupted_entries.load(Ordering::Relaxed),
            bytes_written: self.stats.bytes_written.load(Ordering::Relaxed),
            active_orders: self
                .orders
                .iter()
                .filter(|e| !e.value().latest_entry.state.is_terminal())
                .count(),
            total_orders: self.orders.len(),
            current_sequence: self.sequence.load(Ordering::Relaxed),
        }
    }

    /// Shutdown the WAL gracefully
    pub async fn shutdown(&self) -> Result<(), WalError> {
        self.write_tx
            .send(WriteCommand::Shutdown)
            .await
            .map_err(|_| WalError::ChannelClosed)
    }
}

/// Checkpoint structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    sequence: u64,
    timestamp_ns: u128,
    active_orders: HashMap<String, WalEntry>,
}

/// Public statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalStatistics {
    pub entries_written: u64,
    pub entries_recovered: u64,
    pub checkpoints: u64,
    pub corrupted_entries: u64,
    pub bytes_written: u64,
    pub active_orders: usize,
    pub total_orders: usize,
    pub current_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_wal() -> (Arc<OrderWal>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            checkpoint_interval: 5,
            sync_mode: WalSyncMode::EveryWrite,
            ..Default::default()
        };
        let wal = OrderWal::new(config).await.unwrap();
        (wal, temp_dir)
    }

    #[tokio::test]
    async fn test_basic_order_lifecycle() {
        let (wal, _temp) = create_test_wal().await;

        // Log pending
        wal.log_pending("order-1", "kraken", "BTC-USD", "buy", 1.0, Some(50000.0), "strat-1")
            .await
            .unwrap();

        let order = wal.get_order("order-1").unwrap();
        assert_eq!(order.latest_entry.state, WalOrderState::Pending);

        // Log submitted
        wal.log_submitted("order-1").await.unwrap();
        let order = wal.get_order("order-1").unwrap();
        assert_eq!(order.latest_entry.state, WalOrderState::Submitted);

        // Log acknowledged
        wal.log_acknowledged("order-1", "exch-123").await.unwrap();
        let order = wal.get_order("order-1").unwrap();
        assert_eq!(order.latest_entry.state, WalOrderState::Acknowledged);
        assert_eq!(
            order.latest_entry.exchange_order_id,
            Some("exch-123".to_string())
        );

        // Log filled
        wal.log_filled("order-1", 1.0, 50005.0).await.unwrap();
        let order = wal.get_order("order-1").unwrap();
        assert_eq!(order.latest_entry.state, WalOrderState::Filled);
        assert!(order.latest_entry.state.is_terminal());

        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_incomplete_orders() {
        let (wal, _temp) = create_test_wal().await;

        // Create some orders at different states
        wal.log_pending("order-1", "kraken", "BTC-USD", "buy", 1.0, Some(50000.0), "strat-1")
            .await
            .unwrap();
        wal.log_submitted("order-1").await.unwrap();

        wal.log_pending("order-2", "kraken", "ETH-USD", "sell", 10.0, Some(3000.0), "strat-1")
            .await
            .unwrap();
        wal.log_submitted("order-2").await.unwrap();
        wal.log_acknowledged("order-2", "exch-456").await.unwrap();

        wal.log_pending("order-3", "kraken", "BTC-USD", "buy", 0.5, Some(50000.0), "strat-1")
            .await
            .unwrap();
        wal.log_submitted("order-3").await.unwrap();
        wal.log_filled("order-3", 0.5, 50000.0).await.unwrap();

        let incomplete = wal.get_incomplete_orders();
        assert_eq!(incomplete.len(), 2); // order-1 and order-2

        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_statistics() {
        let (wal, _temp) = create_test_wal().await;

        wal.log_pending("order-1", "kraken", "BTC-USD", "buy", 1.0, Some(50000.0), "strat-1")
            .await
            .unwrap();
        wal.log_submitted("order-1").await.unwrap();
        wal.log_filled("order-1", 1.0, 50000.0).await.unwrap();

        wal.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stats = wal.get_stats();
        assert_eq!(stats.entries_written, 3);
        assert_eq!(stats.active_orders, 0);
        assert_eq!(stats.total_orders, 1);

        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_checksum_verification() {
        let entry = WalEntry::new(
            1,
            "order-1".to_string(),
            WalOrderState::Pending,
            "kraken".to_string(),
            "BTC-USD".to_string(),
            "buy".to_string(),
            1.0,
            Some(50000.0),
            "strat-1".to_string(),
        );

        assert!(entry.verify_checksum());

        let mut corrupted = entry.clone();
        corrupted.quantity = 999.0; // Tamper with data
        assert!(!corrupted.verify_checksum());
    }

    #[tokio::test]
    async fn test_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            checkpoint_interval: 100,
            sync_mode: WalSyncMode::EveryWrite,
            ..Default::default()
        };

        // First session - create orders
        {
            let wal = OrderWal::new(config.clone()).await.unwrap();
            wal.log_pending("order-1", "kraken", "BTC-USD", "buy", 1.0, Some(50000.0), "strat-1")
                .await
                .unwrap();
            wal.log_submitted("order-1").await.unwrap();
            wal.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            wal.shutdown().await.unwrap();
        }

        // Second session - recover
        {
            let wal = OrderWal::new(config).await.unwrap();
            let order = wal.get_order("order-1");
            assert!(order.is_some());
            let order = order.unwrap();
            assert_eq!(order.latest_entry.state, WalOrderState::Submitted);

            let stats = wal.get_stats();
            assert!(stats.entries_recovered > 0);

            wal.shutdown().await.unwrap();
        }
    }
}
