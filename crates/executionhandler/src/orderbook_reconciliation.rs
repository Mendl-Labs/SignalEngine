//! Order Book Snapshot Reconciliation Module
//!
//! Ensures orderbook integrity by:
//! - Periodic snapshot requests for full state reset
//! - Sequence number validation for delta updates
//! - Gap detection and automatic recovery
//! - Checksum verification (exchange-specific)
//!
//! # Critical for Production
//!
//! Without reconciliation, orderbook drift can occur from:
//! - Missed WebSocket messages
//! - Out-of-order updates
//! - Network partitions
//! - Exchange-side resets
//!
//! # Example
//!
//! ```rust,ignore
//! use executionhandler::orderbook_reconciliation::{
//!     OrderbookReconciler, ReconciliationConfig, ReconciliationEvent
//! };
//!
//! let config = ReconciliationConfig {
//!     snapshot_interval_ms: 60_000, // Full snapshot every minute
//!     max_sequence_gap: 10,          // Trigger recovery if gap > 10
//!     checksum_enabled: true,        // Enable checksum validation
//!     ..Default::default()
//! };
//!
//! let reconciler = OrderbookReconciler::new(config);
//!
//! // Process an update
//! match reconciler.process_update(&update) {
//!     ReconciliationResult::Applied => { /* Normal update */ }
//!     ReconciliationResult::SnapshotRequired(reason) => {
//!         // Request full snapshot from exchange
//!     }
//!     ReconciliationResult::Recovered(events) => {
//!         // Gap recovered via buffered updates
//!     }
//! }
//! ```

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

/// Configuration for orderbook reconciliation
#[derive(Debug, Clone)]
pub struct ReconciliationConfig {
    /// Interval for requesting full snapshots (milliseconds)
    pub snapshot_interval_ms: u64,
    /// Maximum allowed sequence gap before triggering recovery
    pub max_sequence_gap: u64,
    /// Enable checksum validation
    pub checksum_enabled: bool,
    /// Checksum validation depth (number of price levels)
    pub checksum_depth: usize,
    /// Maximum updates to buffer for gap recovery
    pub max_buffered_updates: usize,
    /// Time to wait for missing sequence before snapshot (milliseconds)
    pub gap_timeout_ms: u64,
    /// Auto-request snapshot on checksum mismatch
    pub auto_snapshot_on_mismatch: bool,
    /// Enabled exchanges for reconciliation
    pub enabled_exchanges: Vec<String>,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            snapshot_interval_ms: 60_000, // 1 minute
            max_sequence_gap: 10,
            checksum_enabled: true,
            checksum_depth: 25, // Top 25 levels
            max_buffered_updates: 1000,
            gap_timeout_ms: 5000, // 5 seconds
            auto_snapshot_on_mismatch: true,
            enabled_exchanges: vec![
                "kraken".to_string(),
                "binance".to_string(),
                "deribit".to_string(),
            ],
        }
    }
}

/// Orderbook update from exchange
#[derive(Debug, Clone)]
pub struct OrderbookUpdate {
    /// Symbol
    pub symbol: String,
    /// Exchange
    pub exchange: String,
    /// Update type
    pub update_type: UpdateType,
    /// Sequence number (exchange-assigned)
    pub sequence: u64,
    /// Previous sequence (for validation)
    pub prev_sequence: Option<u64>,
    /// Update timestamp (nanoseconds)
    pub timestamp_ns: u128,
    /// Exchange timestamp (nanoseconds)
    pub exchange_timestamp_ns: Option<u128>,
    /// Bid updates: (price, quantity)
    pub bids: Vec<(f64, f64)>,
    /// Ask updates: (price, quantity)
    pub asks: Vec<(f64, f64)>,
    /// Checksum from exchange
    pub checksum: Option<u32>,
}

/// Type of orderbook update
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateType {
    /// Full snapshot
    Snapshot,
    /// Incremental delta
    Delta,
    /// Trade occurred (may affect book)
    Trade,
}

/// Result of processing an update
#[derive(Debug, Clone)]
pub enum ReconciliationResult {
    /// Update applied successfully
    Applied {
        /// Current sequence
        sequence: u64,
        /// Processing latency (nanoseconds)
        latency_ns: u64,
    },
    /// Snapshot required - local book may be stale
    SnapshotRequired {
        /// Reason for requiring snapshot
        reason: SnapshotReason,
        /// Current local sequence
        local_sequence: u64,
        /// Update's sequence
        update_sequence: u64,
    },
    /// Gap recovered from buffered updates
    Recovered {
        /// Number of updates applied
        updates_applied: u32,
        /// Gap that was recovered
        gap_start: u64,
        /// End of recovered gap
        gap_end: u64,
    },
    /// Update skipped (duplicate or old)
    Skipped {
        /// Reason for skip
        reason: String,
    },
    /// Checksum mismatch detected
    ChecksumMismatch {
        /// Expected checksum
        expected: u32,
        /// Actual calculated checksum
        actual: u32,
        /// Current sequence
        sequence: u64,
    },
}

/// Reasons for requiring a snapshot
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotReason {
    /// Initial state - no local book yet
    Initial,
    /// Sequence gap too large
    LargeGap,
    /// Gap timeout expired
    GapTimeout,
    /// Checksum mismatch
    ChecksumMismatch,
    /// Periodic refresh
    PeriodicRefresh,
    /// Exchange requested reset
    ExchangeReset,
    /// Manual request
    Manual,
    /// Internal state error (rare race condition)
    StateError,
}

/// Events emitted during reconciliation
#[derive(Debug, Clone)]
pub enum ReconciliationEvent {
    /// Sequence gap detected
    GapDetected {
        symbol: String,
        expected: u64,
        received: u64,
        timestamp_ns: u128,
    },
    /// Gap recovered
    GapRecovered {
        symbol: String,
        gap_start: u64,
        gap_end: u64,
        updates_applied: u32,
    },
    /// Snapshot applied
    SnapshotApplied {
        symbol: String,
        sequence: u64,
        bid_levels: usize,
        ask_levels: usize,
    },
    /// Checksum validated
    ChecksumValidated {
        symbol: String,
        sequence: u64,
        checksum: u32,
    },
    /// Checksum failed
    ChecksumFailed {
        symbol: String,
        sequence: u64,
        expected: u32,
        actual: u32,
    },
    /// Book state possibly stale
    StaleWarning {
        symbol: String,
        last_sequence: u64,
        age_ms: u64,
    },
}

/// Per-symbol reconciliation state
struct SymbolState {
    /// Current sequence number
    current_sequence: AtomicU64,
    /// Whether we have a valid book
    is_valid: AtomicBool,
    /// Last snapshot time
    last_snapshot: RwLock<Instant>,
    /// Last update time
    last_update: RwLock<Instant>,
    /// Buffered out-of-order updates
    buffered_updates: Mutex<VecDeque<OrderbookUpdate>>,
    /// Gap detection state
    gap_detected_at: Mutex<Option<(u64, Instant)>>, // (missing_seq, detected_time)
    /// Local orderbook copy for checksum
    local_book: RwLock<LocalBook>,
    /// Statistics
    stats: SymbolStats,
}

/// Local orderbook copy for checksum validation
#[derive(Debug, Default)]
struct LocalBook {
    /// Bid side: price -> quantity (sorted descending)
    bids: BTreeMap<OrderedFloat, f64>,
    /// Ask side: price -> quantity (sorted ascending)
    asks: BTreeMap<OrderedFloat, f64>,
    /// Sequence this book represents
    sequence: u64,
}

/// Wrapper for ordered float comparison
#[derive(Debug, Clone, Copy)]
struct OrderedFloat(f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Per-symbol statistics
#[derive(Debug, Default)]
struct SymbolStats {
    updates_processed: AtomicU64,
    snapshots_applied: AtomicU64,
    gaps_detected: AtomicU64,
    gaps_recovered: AtomicU64,
    checksum_validations: AtomicU64,
    checksum_failures: AtomicU64,
}

impl SymbolState {
    fn new() -> Self {
        Self {
            current_sequence: AtomicU64::new(0),
            is_valid: AtomicBool::new(false),
            last_snapshot: RwLock::new(Instant::now()),
            last_update: RwLock::new(Instant::now()),
            buffered_updates: Mutex::new(VecDeque::new()),
            gap_detected_at: Mutex::new(None),
            local_book: RwLock::new(LocalBook::default()),
            stats: SymbolStats::default(),
        }
    }
}

/// Orderbook reconciler
pub struct OrderbookReconciler {
    /// Configuration
    config: ReconciliationConfig,
    /// Per-symbol state
    states: DashMap<String, SymbolState>,
    /// Event callback
    event_handler: RwLock<Option<Box<dyn Fn(ReconciliationEvent) + Send + Sync>>>,
    /// Global statistics
    global_stats: GlobalStats,
}

/// Global reconciliation statistics
#[derive(Debug, Default)]
struct GlobalStats {
    total_updates: AtomicU64,
    total_snapshots: AtomicU64,
    total_gaps: AtomicU64,
    total_recoveries: AtomicU64,
}

impl OrderbookReconciler {
    /// Create a new reconciler
    pub fn new(config: ReconciliationConfig) -> Self {
        Self {
            config,
            states: DashMap::new(),
            event_handler: RwLock::new(None),
            global_stats: GlobalStats::default(),
        }
    }
    
    fn now_ns() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    }
    
    /// Set event handler for reconciliation events
    pub fn set_event_handler<F>(&self, handler: F)
    where
        F: Fn(ReconciliationEvent) + Send + Sync + 'static,
    {
        *self.event_handler.write() = Some(Box::new(handler));
    }
    
    fn emit_event(&self, event: ReconciliationEvent) {
        if let Some(handler) = self.event_handler.read().as_ref() {
            handler(event);
        }
    }
    
    /// Create unique key for symbol+exchange
    fn make_key(symbol: &str, exchange: &str) -> String {
        format!("{}:{}", exchange, symbol)
    }
    
    /// Get or create state for a symbol
    fn get_or_create_state(&self, symbol: &str, exchange: &str) -> dashmap::mapref::one::Ref<'_, String, SymbolState> {
        let key = Self::make_key(symbol, exchange);
        // Use entry API - this returns a reference that keeps the entry alive
        self.states.entry(key.clone()).or_insert_with(SymbolState::new);
        // Under normal DashMap operation this cannot fail after entry() above.
        // Loop to handle extremely rare race conditions under high contention.
        loop {
            if let Some(state) = self.states.get(&key) {
                return state;
            }
            // Re-insert if somehow missing (should never happen)
            self.states.entry(key.clone()).or_insert_with(SymbolState::new);
        }
    }
    
    /// Process an orderbook update
    pub fn process_update(&self, update: &OrderbookUpdate) -> ReconciliationResult {
        let start = Instant::now();
        let key = Self::make_key(&update.symbol, &update.exchange);
        
        // Get or create state using entry API to avoid race condition
        self.states.entry(key.clone()).or_insert_with(SymbolState::new);
        // Safe fallback if get fails under extreme contention
        let state = match self.states.get(&key) {
            Some(s) => s,
            None => {
                // Retry once - this handles rare race conditions
                self.states.entry(key.clone()).or_insert_with(SymbolState::new);
                match self.states.get(&key) {
                    Some(s) => s,
                    None => {
                        // Cannot get state - return error instead of panic
                        return ReconciliationResult::SnapshotRequired {
                            reason: SnapshotReason::StateError,
                            local_sequence: 0,
                            update_sequence: update.sequence,
                        };
                    }
                }
            }
        };
        
        self.global_stats.total_updates.fetch_add(1, Ordering::Relaxed);
        
        // Handle based on update type
        match update.update_type {
            UpdateType::Snapshot => {
                return self.apply_snapshot(&state, update, start);
            }
            UpdateType::Delta | UpdateType::Trade => {
                // Check if we have a valid book
                if !state.is_valid.load(Ordering::Acquire) {
                    return ReconciliationResult::SnapshotRequired {
                        reason: SnapshotReason::Initial,
                        local_sequence: state.current_sequence.load(Ordering::Relaxed),
                        update_sequence: update.sequence,
                    };
                }
                
                // Check for periodic refresh
                let last_snapshot = *state.last_snapshot.read();
                if last_snapshot.elapsed().as_millis() as u64 > self.config.snapshot_interval_ms {
                    return ReconciliationResult::SnapshotRequired {
                        reason: SnapshotReason::PeriodicRefresh,
                        local_sequence: state.current_sequence.load(Ordering::Relaxed),
                        update_sequence: update.sequence,
                    };
                }
                
                // Process delta update
                return self.process_delta(&state, update, start);
            }
        }
    }
    
    /// Apply a snapshot
    fn apply_snapshot(
        &self,
        state: &SymbolState,
        update: &OrderbookUpdate,
        start: Instant,
    ) -> ReconciliationResult {
        // Build local book from snapshot
        {
            let mut book = state.local_book.write();
            book.bids.clear();
            book.asks.clear();
            
            for (price, qty) in &update.bids {
                if *qty > 0.0 {
                    book.bids.insert(OrderedFloat(*price), *qty);
                }
            }
            for (price, qty) in &update.asks {
                if *qty > 0.0 {
                    book.asks.insert(OrderedFloat(*price), *qty);
                }
            }
            book.sequence = update.sequence;
        }
        
        // Update state
        state.current_sequence.store(update.sequence, Ordering::Release);
        state.is_valid.store(true, Ordering::Release);
        *state.last_snapshot.write() = Instant::now();
        *state.last_update.write() = Instant::now();
        
        // Clear buffered updates older than this snapshot
        {
            let mut buffer = state.buffered_updates.lock();
            buffer.retain(|u| u.sequence > update.sequence);
        }
        
        // Clear gap detection
        *state.gap_detected_at.lock() = None;
        
        state.stats.snapshots_applied.fetch_add(1, Ordering::Relaxed);
        self.global_stats.total_snapshots.fetch_add(1, Ordering::Relaxed);
        
        self.emit_event(ReconciliationEvent::SnapshotApplied {
            symbol: update.symbol.clone(),
            sequence: update.sequence,
            bid_levels: update.bids.len(),
            ask_levels: update.asks.len(),
        });
        
        // Validate checksum if provided
        if self.config.checksum_enabled {
            if let Some(expected_checksum) = update.checksum {
                let actual = self.calculate_checksum(state, &update.exchange);
                if actual != expected_checksum {
                    state.stats.checksum_failures.fetch_add(1, Ordering::Relaxed);
                    self.emit_event(ReconciliationEvent::ChecksumFailed {
                        symbol: update.symbol.clone(),
                        sequence: update.sequence,
                        expected: expected_checksum,
                        actual,
                    });
                    
                    // Even snapshot has bad checksum - critical issue
                    return ReconciliationResult::ChecksumMismatch {
                        expected: expected_checksum,
                        actual,
                        sequence: update.sequence,
                    };
                }
                state.stats.checksum_validations.fetch_add(1, Ordering::Relaxed);
                self.emit_event(ReconciliationEvent::ChecksumValidated {
                    symbol: update.symbol.clone(),
                    sequence: update.sequence,
                    checksum: actual,
                });
            }
        }
        
        ReconciliationResult::Applied {
            sequence: update.sequence,
            latency_ns: start.elapsed().as_nanos() as u64,
        }
    }
    
    /// Process a delta update
    fn process_delta(
        &self,
        state: &SymbolState,
        update: &OrderbookUpdate,
        start: Instant,
    ) -> ReconciliationResult {
        let current_seq = state.current_sequence.load(Ordering::Acquire);
        let expected_seq = current_seq + 1;
        
        // Check sequence
        if update.sequence < expected_seq {
            // Old/duplicate update - skip
            return ReconciliationResult::Skipped {
                reason: format!("Old sequence: {} < expected {}", update.sequence, expected_seq),
            };
        }
        
        if update.sequence > expected_seq {
            // Gap detected
            let gap_size = update.sequence - expected_seq;
            
            if gap_size > self.config.max_sequence_gap {
                // Gap too large - need snapshot
                state.stats.gaps_detected.fetch_add(1, Ordering::Relaxed);
                self.global_stats.total_gaps.fetch_add(1, Ordering::Relaxed);
                
                self.emit_event(ReconciliationEvent::GapDetected {
                    symbol: update.symbol.clone(),
                    expected: expected_seq,
                    received: update.sequence,
                    timestamp_ns: Self::now_ns(),
                });
                
                return ReconciliationResult::SnapshotRequired {
                    reason: SnapshotReason::LargeGap,
                    local_sequence: current_seq,
                    update_sequence: update.sequence,
                };
            }
            
            // Buffer update and check timeout
            {
                let mut buffer = state.buffered_updates.lock();
                
                // Don't buffer duplicates
                if !buffer.iter().any(|u| u.sequence == update.sequence) {
                    buffer.push_back(update.clone());
                    
                    // Trim buffer if too large
                    while buffer.len() > self.config.max_buffered_updates {
                        buffer.pop_front();
                    }
                }
            }
            
            // Track gap detection time
            {
                let mut gap_state = state.gap_detected_at.lock();
                if gap_state.is_none() {
                    *gap_state = Some((expected_seq, Instant::now()));
                    state.stats.gaps_detected.fetch_add(1, Ordering::Relaxed);
                    self.global_stats.total_gaps.fetch_add(1, Ordering::Relaxed);
                    
                    self.emit_event(ReconciliationEvent::GapDetected {
                        symbol: update.symbol.clone(),
                        expected: expected_seq,
                        received: update.sequence,
                        timestamp_ns: Self::now_ns(),
                    });
                }
                
                // Check timeout
                if let Some((_, detected_at)) = *gap_state {
                    if detected_at.elapsed().as_millis() as u64 > self.config.gap_timeout_ms {
                        return ReconciliationResult::SnapshotRequired {
                            reason: SnapshotReason::GapTimeout,
                            local_sequence: current_seq,
                            update_sequence: update.sequence,
                        };
                    }
                }
            }
            
            // Try to recover from buffer
            if let Some(result) = self.try_recover_from_buffer(state, &update.symbol) {
                return result;
            }
            
            return ReconciliationResult::Skipped {
                reason: format!("Buffered: waiting for sequence {}", expected_seq),
            };
        }
        
        // Sequence matches - apply update
        self.apply_delta(state, update);
        
        // Clear gap detection
        *state.gap_detected_at.lock() = None;
        
        // Validate checksum
        if self.config.checksum_enabled {
            if let Some(expected_checksum) = update.checksum {
                let actual = self.calculate_checksum(state, &update.exchange);
                if actual != expected_checksum {
                    state.stats.checksum_failures.fetch_add(1, Ordering::Relaxed);
                    self.emit_event(ReconciliationEvent::ChecksumFailed {
                        symbol: update.symbol.clone(),
                        sequence: update.sequence,
                        expected: expected_checksum,
                        actual,
                    });
                    
                    if self.config.auto_snapshot_on_mismatch {
                        return ReconciliationResult::SnapshotRequired {
                            reason: SnapshotReason::ChecksumMismatch,
                            local_sequence: update.sequence,
                            update_sequence: update.sequence,
                        };
                    }
                    
                    return ReconciliationResult::ChecksumMismatch {
                        expected: expected_checksum,
                        actual,
                        sequence: update.sequence,
                    };
                }
                state.stats.checksum_validations.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        // Process any buffered updates that are now applicable
        self.process_buffered_updates(state, &update.symbol);
        
        ReconciliationResult::Applied {
            sequence: update.sequence,
            latency_ns: start.elapsed().as_nanos() as u64,
        }
    }
    
    /// Apply a delta update to local book
    fn apply_delta(&self, state: &SymbolState, update: &OrderbookUpdate) {
        let mut book = state.local_book.write();
        
        // Apply bid updates
        for (price, qty) in &update.bids {
            let key = OrderedFloat(*price);
            if *qty <= 0.0 {
                book.bids.remove(&key);
            } else {
                book.bids.insert(key, *qty);
            }
        }
        
        // Apply ask updates
        for (price, qty) in &update.asks {
            let key = OrderedFloat(*price);
            if *qty <= 0.0 {
                book.asks.remove(&key);
            } else {
                book.asks.insert(key, *qty);
            }
        }
        
        book.sequence = update.sequence;
        state.current_sequence.store(update.sequence, Ordering::Release);
        *state.last_update.write() = Instant::now();
        
        state.stats.updates_processed.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Try to recover gap from buffered updates
    fn try_recover_from_buffer(
        &self,
        state: &SymbolState,
        symbol: &str,
    ) -> Option<ReconciliationResult> {
        let current_seq = state.current_sequence.load(Ordering::Acquire);
        let expected_seq = current_seq + 1;
        
        let mut buffer = state.buffered_updates.lock();
        
        // Sort buffer by sequence
        let mut sorted: Vec<_> = buffer.drain(..).collect();
        sorted.sort_by_key(|u| u.sequence);
        
        // Check if we have a contiguous sequence
        let mut next = expected_seq;
        let mut to_apply: Vec<OrderbookUpdate> = Vec::new();
        let mut remaining: Vec<OrderbookUpdate> = Vec::new();
        
        for update in sorted {
            if update.sequence == next {
                to_apply.push(update);
                next += 1;
            } else if update.sequence > next {
                remaining.push(update);
            }
            // Skip old updates
        }
        
        // Put remaining back in buffer
        for u in remaining {
            buffer.push_back(u);
        }
        
        if to_apply.is_empty() {
            return None;
        }
        
        drop(buffer); // Release lock before applying
        
        // Apply recovered updates
        let gap_start = expected_seq;
        let gap_end = next - 1;
        let count = to_apply.len() as u32;
        
        for update in to_apply {
            self.apply_delta(state, &update);
        }
        
        state.stats.gaps_recovered.fetch_add(1, Ordering::Relaxed);
        self.global_stats.total_recoveries.fetch_add(1, Ordering::Relaxed);
        
        self.emit_event(ReconciliationEvent::GapRecovered {
            symbol: symbol.to_string(),
            gap_start,
            gap_end,
            updates_applied: count,
        });
        
        Some(ReconciliationResult::Recovered {
            updates_applied: count,
            gap_start,
            gap_end,
        })
    }
    
    /// Process any buffered updates that are now applicable
    fn process_buffered_updates(&self, state: &SymbolState, _symbol: &str) {
        loop {
            let current_seq = state.current_sequence.load(Ordering::Acquire);
            let expected_seq = current_seq + 1;
            
            let update = {
                let mut buffer = state.buffered_updates.lock();
                
                // Find and remove the next expected update
                if let Some(pos) = buffer.iter().position(|u| u.sequence == expected_seq) {
                    Some(buffer.remove(pos).unwrap())
                } else {
                    None
                }
            };
            
            if let Some(update) = update {
                self.apply_delta(state, &update);
            } else {
                break;
            }
        }
    }
    
    /// Calculate checksum for current book state
    fn calculate_checksum(&self, state: &SymbolState, exchange: &str) -> u32 {
        let book = state.local_book.read();
        
        match exchange.to_lowercase().as_str() {
            "kraken" => self.calculate_kraken_checksum(&book),
            "binance" => self.calculate_binance_checksum(&book),
            "deribit" => self.calculate_deribit_checksum(&book),
            _ => self.calculate_generic_checksum(&book),
        }
    }
    
    /// Kraken-style checksum: concatenate top N prices and quantities as strings
    fn calculate_kraken_checksum(&self, book: &LocalBook) -> u32 {
        let mut data = String::new();
        
        // Top N asks (ascending price)
        for (price, qty) in book.asks.iter().take(self.config.checksum_depth) {
            data.push_str(&self.format_kraken_value(price.0));
            data.push_str(&self.format_kraken_value(*qty));
        }
        
        // Top N bids (descending price)
        for (price, qty) in book.bids.iter().rev().take(self.config.checksum_depth) {
            data.push_str(&self.format_kraken_value(price.0));
            data.push_str(&self.format_kraken_value(*qty));
        }
        
        crc32fast::hash(data.as_bytes())
    }
    
    /// Format value for Kraken checksum (remove decimal point and leading zeros)
    fn format_kraken_value(&self, value: f64) -> String {
        let s = format!("{:.10}", value);
        let s = s.replace('.', "");
        s.trim_start_matches('0').to_string()
    }
    
    /// Binance-style checksum: similar to Kraken
    fn calculate_binance_checksum(&self, book: &LocalBook) -> u32 {
        self.calculate_generic_checksum(book)
    }
    
    /// Deribit-style checksum
    fn calculate_deribit_checksum(&self, book: &LocalBook) -> u32 {
        self.calculate_generic_checksum(book)
    }
    
    /// Generic checksum using price:qty pairs
    fn calculate_generic_checksum(&self, book: &LocalBook) -> u32 {
        let mut data = Vec::new();
        
        // Bids (descending)
        for (price, qty) in book.bids.iter().rev().take(self.config.checksum_depth) {
            data.extend_from_slice(&price.0.to_le_bytes());
            data.extend_from_slice(&qty.to_le_bytes());
        }
        
        // Asks (ascending)
        for (price, qty) in book.asks.iter().take(self.config.checksum_depth) {
            data.extend_from_slice(&price.0.to_le_bytes());
            data.extend_from_slice(&qty.to_le_bytes());
        }
        
        crc32fast::hash(&data)
    }
    
    /// Check if a symbol needs a snapshot
    pub fn needs_snapshot(&self, symbol: &str, exchange: &str) -> Option<SnapshotReason> {
        let key = Self::make_key(symbol, exchange);
        let state = self.states.get(&key)?;
        
        if !state.is_valid.load(Ordering::Acquire) {
            return Some(SnapshotReason::Initial);
        }
        
        let last_snapshot = *state.last_snapshot.read();
        if last_snapshot.elapsed().as_millis() as u64 > self.config.snapshot_interval_ms {
            return Some(SnapshotReason::PeriodicRefresh);
        }
        
        if let Some((_, detected_at)) = *state.gap_detected_at.lock() {
            if detected_at.elapsed().as_millis() as u64 > self.config.gap_timeout_ms {
                return Some(SnapshotReason::GapTimeout);
            }
        }
        
        None
    }
    
    /// Get current sequence for a symbol
    pub fn get_sequence(&self, symbol: &str, exchange: &str) -> Option<u64> {
        let key = Self::make_key(symbol, exchange);
        self.states.get(&key).map(|s| s.current_sequence.load(Ordering::Acquire))
    }
    
    /// Get statistics for a symbol
    pub fn get_symbol_stats(&self, symbol: &str, exchange: &str) -> Option<SymbolStatistics> {
        let key = Self::make_key(symbol, exchange);
        let state = self.states.get(&key)?;
        
        // Extract all values while we hold the reference
        let current_sequence = state.current_sequence.load(Ordering::Relaxed);
        let is_valid = state.is_valid.load(Ordering::Relaxed);
        let updates_processed = state.stats.updates_processed.load(Ordering::Relaxed);
        let snapshots_applied = state.stats.snapshots_applied.load(Ordering::Relaxed);
        let gaps_detected = state.stats.gaps_detected.load(Ordering::Relaxed);
        let gaps_recovered = state.stats.gaps_recovered.load(Ordering::Relaxed);
        let checksum_validations = state.stats.checksum_validations.load(Ordering::Relaxed);
        let checksum_failures = state.stats.checksum_failures.load(Ordering::Relaxed);
        let buffered_updates = state.buffered_updates.lock().len();
        let last_update_age_ms = state.last_update.read().elapsed().as_millis() as u64;
        
        // Now drop the reference and return
        drop(state);
        
        Some(SymbolStatistics {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            current_sequence,
            is_valid,
            updates_processed,
            snapshots_applied,
            gaps_detected,
            gaps_recovered,
            checksum_validations,
            checksum_failures,
            buffered_updates,
            last_update_age_ms,
        })
    }
    
    /// Get global statistics
    pub fn get_global_stats(&self) -> GlobalStatistics {
        GlobalStatistics {
            total_updates: self.global_stats.total_updates.load(Ordering::Relaxed),
            total_snapshots: self.global_stats.total_snapshots.load(Ordering::Relaxed),
            total_gaps: self.global_stats.total_gaps.load(Ordering::Relaxed),
            total_recoveries: self.global_stats.total_recoveries.load(Ordering::Relaxed),
            active_symbols: self.states.len(),
        }
    }
    
    /// Force a snapshot request for a symbol
    pub fn request_snapshot(&self, symbol: &str, exchange: &str) {
        let key = Self::make_key(symbol, exchange);
        if let Some(state) = self.states.get(&key) {
            state.is_valid.store(false, Ordering::Release);
        }
    }
    
    /// Clear all state (for testing)
    pub fn clear(&self) {
        self.states.clear();
    }
}

impl Default for OrderbookReconciler {
    fn default() -> Self {
        Self::new(ReconciliationConfig::default())
    }
}

/// Public statistics for a symbol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolStatistics {
    pub symbol: String,
    pub exchange: String,
    pub current_sequence: u64,
    pub is_valid: bool,
    pub updates_processed: u64,
    pub snapshots_applied: u64,
    pub gaps_detected: u64,
    pub gaps_recovered: u64,
    pub checksum_validations: u64,
    pub checksum_failures: u64,
    pub buffered_updates: usize,
    pub last_update_age_ms: u64,
}

/// Global statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalStatistics {
    pub total_updates: u64,
    pub total_snapshots: u64,
    pub total_gaps: u64,
    pub total_recoveries: u64,
    pub active_symbols: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_initial_snapshot_required() {
        let reconciler = OrderbookReconciler::new(ReconciliationConfig::default());
        
        let update = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Delta,
            sequence: 100,
            prev_sequence: Some(99),
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(50000.0, 1.0)],
            asks: vec![(50001.0, 1.0)],
            checksum: None,
        };
        
        match reconciler.process_update(&update) {
            ReconciliationResult::SnapshotRequired { reason, .. } => {
                assert_eq!(reason, SnapshotReason::Initial);
            }
            _ => panic!("Expected SnapshotRequired"),
        }
    }
    
    #[test]
    fn test_snapshot_application() {
        let reconciler = OrderbookReconciler::new(ReconciliationConfig::default());
        
        let snapshot = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Snapshot,
            sequence: 100,
            prev_sequence: None,
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(50000.0, 1.0), (49999.0, 2.0)],
            asks: vec![(50001.0, 1.0), (50002.0, 2.0)],
            checksum: None,
        };
        
        match reconciler.process_update(&snapshot) {
            ReconciliationResult::Applied { sequence, .. } => {
                assert_eq!(sequence, 100);
            }
            _ => panic!("Expected Applied"),
        }
        
        assert_eq!(reconciler.get_sequence("BTC-USD", "kraken"), Some(100));
    }
    
    #[test]
    fn test_sequential_deltas() {
        let reconciler = OrderbookReconciler::new(ReconciliationConfig::default());
        
        // Apply snapshot first
        let snapshot = OrderbookUpdate {
            symbol: "ETH-USD".to_string(),
            exchange: "binance".to_string(),
            update_type: UpdateType::Snapshot,
            sequence: 1,
            prev_sequence: None,
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(3000.0, 10.0)],
            asks: vec![(3001.0, 10.0)],
            checksum: None,
        };
        reconciler.process_update(&snapshot);
        
        // Apply sequential deltas
        for seq in 2..=5 {
            let delta = OrderbookUpdate {
                symbol: "ETH-USD".to_string(),
                exchange: "binance".to_string(),
                update_type: UpdateType::Delta,
                sequence: seq,
                prev_sequence: Some(seq - 1),
                timestamp_ns: 0,
                exchange_timestamp_ns: None,
                bids: vec![(3000.0 - seq as f64, 1.0)],
                asks: vec![],
                checksum: None,
            };
            
            match reconciler.process_update(&delta) {
                ReconciliationResult::Applied { sequence, .. } => {
                    assert_eq!(sequence, seq);
                }
                _ => panic!("Expected Applied for sequence {}", seq),
            }
        }
        
        assert_eq!(reconciler.get_sequence("ETH-USD", "binance"), Some(5));
    }
    
    #[test]
    fn test_gap_detection() {
        let reconciler = OrderbookReconciler::new(ReconciliationConfig {
            max_sequence_gap: 5,
            ..Default::default()
        });
        
        // Apply snapshot
        let snapshot = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Snapshot,
            sequence: 1,
            prev_sequence: None,
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(50000.0, 1.0)],
            asks: vec![(50001.0, 1.0)],
            checksum: None,
        };
        reconciler.process_update(&snapshot);
        
        // Send update with large gap
        let delta = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Delta,
            sequence: 100, // Gap of 99
            prev_sequence: Some(99),
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(50000.0, 2.0)],
            asks: vec![],
            checksum: None,
        };
        
        match reconciler.process_update(&delta) {
            ReconciliationResult::SnapshotRequired { reason, .. } => {
                assert_eq!(reason, SnapshotReason::LargeGap);
            }
            _ => panic!("Expected SnapshotRequired for large gap"),
        }
    }
    
    #[test]
    fn test_gap_recovery_from_buffer() {
        let reconciler = OrderbookReconciler::new(ReconciliationConfig {
            max_sequence_gap: 10,
            ..Default::default()
        });
        
        // Apply snapshot
        let snapshot = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Snapshot,
            sequence: 1,
            prev_sequence: None,
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(50000.0, 1.0)],
            asks: vec![(50001.0, 1.0)],
            checksum: None,
        };
        reconciler.process_update(&snapshot);
        
        // Send updates out of order (3 before 2)
        let delta3 = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Delta,
            sequence: 3,
            prev_sequence: Some(2),
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(49998.0, 1.0)],
            asks: vec![],
            checksum: None,
        };
        
        // Should be buffered
        match reconciler.process_update(&delta3) {
            ReconciliationResult::Skipped { reason } => {
                assert!(reason.contains("Buffered"));
            }
            _ => panic!("Expected Skipped (buffered)"),
        }
        
        // Send missing update
        let delta2 = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Delta,
            sequence: 2,
            prev_sequence: Some(1),
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(49999.0, 1.0)],
            asks: vec![],
            checksum: None,
        };
        
        // Should apply and process buffered
        match reconciler.process_update(&delta2) {
            ReconciliationResult::Applied { sequence, .. } => {
                // After applying 2 and processing buffer, should be at 3
                assert!(sequence >= 2);
            }
            _ => panic!("Expected Applied"),
        }
        
        // Sequence should now be 3
        assert_eq!(reconciler.get_sequence("BTC-USD", "kraken"), Some(3));
    }
    
    #[test]
    fn test_duplicate_update_skipped() {
        let reconciler = OrderbookReconciler::new(ReconciliationConfig::default());
        
        // Apply snapshot
        let snapshot = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Snapshot,
            sequence: 10,
            prev_sequence: None,
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(50000.0, 1.0)],
            asks: vec![(50001.0, 1.0)],
            checksum: None,
        };
        reconciler.process_update(&snapshot);
        
        // Send old update
        let old_delta = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Delta,
            sequence: 5, // Old
            prev_sequence: Some(4),
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(50000.0, 2.0)],
            asks: vec![],
            checksum: None,
        };
        
        match reconciler.process_update(&old_delta) {
            ReconciliationResult::Skipped { reason } => {
                assert!(reason.contains("Old sequence"));
            }
            _ => panic!("Expected Skipped for old update"),
        }
    }
    
    #[test]
    fn test_statistics() {
        let reconciler = OrderbookReconciler::new(ReconciliationConfig::default());
        
        // Apply snapshot
        let snapshot = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Snapshot,
            sequence: 1,
            prev_sequence: None,
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(50000.0, 1.0)],
            asks: vec![(50001.0, 1.0)],
            checksum: None,
        };
        reconciler.process_update(&snapshot);
        
        // Apply some deltas
        for seq in 2..=5 {
            let delta = OrderbookUpdate {
                symbol: "BTC-USD".to_string(),
                exchange: "kraken".to_string(),
                update_type: UpdateType::Delta,
                sequence: seq,
                prev_sequence: Some(seq - 1),
                timestamp_ns: 0,
                exchange_timestamp_ns: None,
                bids: vec![],
                asks: vec![],
                checksum: None,
            };
            reconciler.process_update(&delta);
        }
        
        let stats = reconciler.get_symbol_stats("BTC-USD", "kraken").unwrap();
        assert_eq!(stats.snapshots_applied, 1);
        assert_eq!(stats.updates_processed, 4);
        assert!(stats.is_valid);
        
        let global = reconciler.get_global_stats();
        assert_eq!(global.total_snapshots, 1);
        assert_eq!(global.active_symbols, 1);
    }
    
    #[test]
    fn test_force_snapshot_request() {
        let reconciler = OrderbookReconciler::new(ReconciliationConfig::default());
        
        // Apply snapshot
        let snapshot = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Snapshot,
            sequence: 1,
            prev_sequence: None,
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![(50000.0, 1.0)],
            asks: vec![(50001.0, 1.0)],
            checksum: None,
        };
        reconciler.process_update(&snapshot);
        
        // Force snapshot
        reconciler.request_snapshot("BTC-USD", "kraken");
        
        // Next delta should require snapshot
        let delta = OrderbookUpdate {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            update_type: UpdateType::Delta,
            sequence: 2,
            prev_sequence: Some(1),
            timestamp_ns: 0,
            exchange_timestamp_ns: None,
            bids: vec![],
            asks: vec![],
            checksum: None,
        };
        
        match reconciler.process_update(&delta) {
            ReconciliationResult::SnapshotRequired { reason, .. } => {
                assert_eq!(reason, SnapshotReason::Initial);
            }
            _ => panic!("Expected SnapshotRequired after force request"),
        }
    }
}
