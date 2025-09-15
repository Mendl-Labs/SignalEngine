use std::collections::VecDeque;
use crossbeam_skiplist::SkipMap;
use ordered_float::NotNan;
use statrs::statistics::Statistics;
use std::sync::{RwLock, Arc, atomic::{AtomicU64, Ordering}};
use std::time::{Instant, Duration};
use crossbeam::queue::SegQueue;
use std::fmt;
use std::ops::Deref;

/// Fixed-point decimal representation for price to avoid floating-point precision issues
/// Uses 8 decimal places precision
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FixedPrice(i64);

impl FixedPrice {
    const SCALE: i64 = 100_000_000; // 8 decimal places
    
    #[inline]
    pub fn from_f64(value: f64) -> Result<Self, &'static str> {
        if !value.is_finite() {
            return Err("Price must be a finite number");
        }
        Ok(Self((value * Self::SCALE as f64) as i64))
    }
    
    #[inline]
    pub fn to_f64(&self) -> f64 {
        self.0 as f64 / Self::SCALE as f64
    }
    
    #[inline]
    pub fn raw_value(&self) -> i64 {
        self.0
    }
}

impl fmt::Display for FixedPrice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_f64())
    }
}

/// A memory pool for OrderbookEntry objects to reduce allocation overhead
// Replace UnsafeCell with proper thread-safe alternatives

pub struct OrderEntryPool {
    // Pre-allocated entries
    entries: RwLock<Vec<Option<OrderbookEntry>>>,
    
    // Queue of freed indices that can be reused
    free_indices: SegQueue<u64>,
    
    // Next never-used slot index
    next_new: AtomicU64,
    
    // Maximum capacity
    capacity: usize,
}

impl OrderEntryPool {
    pub fn with_capacity(capacity: usize) -> Self {
        // Pre-allocate the entire vector with None values
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            entries.push(None);
        }
        
        Self {
            entries: RwLock::new(entries),
            free_indices: SegQueue::new(),  // Empty free list initially
            next_new: AtomicU64::new(0),    // Start allocating from index 0
            capacity,
        }
    }
    
    #[inline]
    pub fn allocate(&self, order_id: u64, quantity: f64, timestamp: u64) -> Option<u64> {
        // First try to reuse a freed slot
        if let Some(free_idx) = self.free_indices.pop() {
            match self.entries.write() {
                Ok(mut entries) => {
                    entries[free_idx as usize] = Some(OrderbookEntry {
                        order_id, quantity, timestamp
                    });
                    return Some(free_idx);
                },
                Err(_) => {
                    // Push the index back to the queue if we fail to acquire the lock
                    self.free_indices.push(free_idx);
                    return None;
                }
            }
        }
        
        // Fall back to new allocation
        let idx = self.next_new.fetch_add(1, Ordering::SeqCst);
        if idx as usize >= self.capacity {
            self.next_new.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        
        match self.entries.write() {
            Ok(mut entries) => {
                entries[idx as usize] = Some(OrderbookEntry {
                    order_id, quantity, timestamp
                });
                Some(idx)
            },
            Err(_) => {
                self.next_new.fetch_sub(1, Ordering::SeqCst);
                None
            }
        }
    }
    
    #[inline]
    pub fn get(&self, idx: u64) -> Option<OrderbookEntry> {
        if idx as usize >= self.capacity {
            return None;
        }
        
        // Use a read lock to access entries
        if let Ok(entries) = self.entries.read() {
            entries[idx as usize].clone()
        } else {
            None
        }
    }
    
    #[inline]
    pub fn get_mut(&self, idx: u64, f: impl FnOnce(&mut OrderbookEntry)) -> bool {
        if idx as usize >= self.capacity {
            return false;
        }
        
        // Use a write lock to modify entries
        if let Ok(mut entries) = self.entries.write() {
            if let Some(entry) = entries[idx as usize].as_mut() {
                f(entry);
                true
            } else {
                false
            }
        } else {
            false
        }
    }
    
    #[inline]
    pub fn free(&self, idx: u64) {
        if idx as usize >= self.capacity {
            return;
        }
        
        // Acquire the write lock first
        if let Ok(mut entries) = self.entries.write() {
            entries[idx as usize] = None;
            // Push to the free indices queue only after successfully updating the entry
            self.free_indices.push(idx);
        }
    }
}

// Thread-safe wrapper for the memory pool
pub struct SharedOrderEntryPool(Arc<OrderEntryPool>);

impl SharedOrderEntryPool {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(OrderEntryPool::with_capacity(capacity)))
    }
}

impl Deref for SharedOrderEntryPool {
    type Target = OrderEntryPool;
    
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for SharedOrderEntryPool {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// Core orderbook entry struct
#[derive(Debug, Clone)]
pub struct OrderbookEntry {
    pub order_id: u64,
    pub quantity: f64,
    pub timestamp: u64,
}

// Enum to represent either order type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Bid,
    Ask,
}

// Enum for market order execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStatus {
    Complete,
    Partial,
    Unfilled,
}

/// Order execution result
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub executed_quantity: f64,
    pub remaining_quantity: f64,
    pub average_price: f64,
    pub execution_time: Duration,
    pub status: ExecutionStatus,
}

// Statistical metrics that are tracked for the orderbook
#[derive(Debug, Default, Clone)]
pub struct OrderbookMetrics {
    // Price metrics
    pub best_ask: f64,
    pub mid_price: f64,
    pub best_bid: f64,
    pub previous_mid_price: f64,
    pub spread: f64,
    pub spread_bps: f64, // Spread in basis points
    
    // Depth metrics
    pub total_depth: f64,
    pub total_bid_depth: f64,
    pub previous_total_bid_depth: f64,
    pub total_ask_depth: f64,
    pub previous_total_ask_depth: f64,
    pub best_bid_depth: f64,
    pub best_ask_depth: f64,
    
    // Orderbook structure metrics
    pub bid_depth_by_tick: Vec<(f64, f64)>,  // (price_level, depth)
    pub ask_depth_by_tick: Vec<(f64, f64)>,
    
    // Statistical metrics
    pub drift: f64,
    pub variance: f64,
    pub previous_variance: f64,
    
    // Liquidity metrics
    pub liquidity_weighted_orderbook_imbalance: f64,
    pub orderbook_imbalance: f64,
    pub smoothed_orderbook_imbalance: f64,
    
    // Performance metrics
    pub update_duration_ns: u64,
    pub last_match_duration_ns: u64,
    pub avg_match_duration_ns: u64,
    pub match_count: u64,
}

/// Main orderbook implementation with thread-safety
pub struct Orderbook {
    // Core orderbook state - using NotNan for more efficient price point lookups
    bids: SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>, // Price -> vector of indices into the memory pool
    asks: SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>,
    
    // Memory pool for orderbook entries
    entry_pool: SharedOrderEntryPool,
    
    // Metrics tracking
    metrics: RwLock<OrderbookMetrics>,
    
    // Instrument details
    instrument: String,
    exchange: String,
    
    // Performance metric tracking
    update_count: AtomicU64,
    total_update_time_ns: AtomicU64,
}

/// Implementation of the orderbook
impl Orderbook {
    /// Create a new orderbook with specified capacity
    #[inline]
    pub fn new(instrument: String, exchange: String, capacity: usize) -> Self {
        Self {
            bids: SkipMap::new(),
            asks: SkipMap::new(),
            entry_pool: SharedOrderEntryPool::with_capacity(capacity),
            metrics: RwLock::new(OrderbookMetrics::default()),
            instrument,
            exchange,
            update_count: AtomicU64::new(0),
            total_update_time_ns: AtomicU64::new(0),
        }
    }

    /// Update all orderbook metrics - this acquires read locks on the orderbook
    #[inline]
    pub fn update(&self) -> Result<Duration, &'static str> {
        let start = Instant::now();
        
        // Acquire write lock for metrics
        let mut metrics = match self.metrics.write() {
            Ok(metrics) => metrics,
            Err(_) => return Err("Failed to acquire metrics write lock"),
        };
        
        // Prepare for update by storing previous state
        self.prepare_for_update(&mut metrics);
        
        // Calculate all metrics
        self.calculate_depths(&self.bids, &self.asks, &mut metrics);
        self.calculate_mid_price(&mut metrics);
        self.calculate_spread(&mut metrics);
        self.calculate_drift(&mut metrics);
        self.calculate_variance(&self.bids, &self.asks, &mut metrics);
        self.calculate_depth_by_tick(&self.bids, &self.asks, &mut metrics);
        self.calculate_orderbook_imbalance(&mut metrics);
        self.calculate_smoothed_orderbook_imbalance(&mut metrics);
        self.calculate_liquidity_weighted_orderbook_imbalance(&mut metrics);
        
        // Record update time
        let duration = start.elapsed();
        metrics.update_duration_ns = duration.as_nanos() as u64;
        
        // Update performance metrics
        self.update_count.fetch_add(1, Ordering::Relaxed);
        self.total_update_time_ns.fetch_add(metrics.update_duration_ns, Ordering::Relaxed);
        
        Ok(duration)
    }
    
    // Prepare previous state metrics before update
    #[inline]
    fn prepare_for_update(&self, metrics: &mut OrderbookMetrics) {
        metrics.previous_total_bid_depth = metrics.total_bid_depth;
        metrics.previous_total_ask_depth = metrics.total_ask_depth;
        metrics.previous_mid_price = metrics.mid_price;
        metrics.previous_variance = metrics.variance;
    }

    // Helper methods for accessing best prices
    #[inline]
    fn best_bid(&self, bids: &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>) -> Option<f64> {
        bids.back().map(|entry| entry.key().into_inner())
    }

    #[inline]
    fn best_ask(&self, asks: &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>) -> Option<f64> {
        asks.front().map(|entry| entry.key().into_inner())
    }
    
    // Calculate mid price with robust error handling
    #[inline]
    fn calculate_mid_price(&self, metrics: &mut OrderbookMetrics) {
        match (metrics.best_bid, metrics.best_ask) {
            (best_bid, best_ask) if best_bid > 0.0 && best_ask > 0.0 => {
                metrics.mid_price = (best_bid + best_ask) / 2.0;
            },
            _ => {
                // If unable to calculate mid price, reset to previous or zero
                metrics.mid_price = metrics.previous_mid_price;
            }
        }
    }
    
    // Calculate spread and basis points spread
    #[inline]
    fn calculate_spread(&self, metrics: &mut OrderbookMetrics) {
        match (metrics.best_bid, metrics.best_ask) {
            (best_bid, best_ask) if best_bid > 0.0 && best_ask > 0.0 => {
                metrics.spread = best_ask - best_bid;
                
                // Calculate spread in basis points (bps) = (spread / mid_price) * 10000
                if metrics.mid_price > 0.0 {
                    metrics.spread_bps = (metrics.spread / metrics.mid_price) * 10000.0;
                }
            },
            _ => {
                // Reset spread if unable to calculate
                metrics.spread = 0.0;
                metrics.spread_bps = 0.0;
            }
        }
    }
    
    // Calculate all depth metrics
    #[inline]
    fn calculate_depths(
        &self, 
        bids: &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>, 
        asks: &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>,
        metrics: &mut OrderbookMetrics
    ) {
        // Get best prices
        metrics.best_bid = self.best_bid(bids).unwrap_or(0.0);
        metrics.best_ask = self.best_ask(asks).unwrap_or(0.0);
        
        // Calculate total depths - use fold for better performance
        metrics.total_bid_depth = bids.iter()
            .fold(0.0, |acc, entry| {
                if let Ok(orders) = entry.value().read() {
                    acc + orders.iter().fold(0.0, |sum, &idx| {
                        sum + self.entry_pool.get(idx).map_or(0.0, |entry| entry.quantity)
                    })
                } else {
                    acc
                }
            });
        
        metrics.total_ask_depth = asks.iter()
            .fold(0.0, |acc, entry| {
                if let Ok(orders) = entry.value().read() {
                    acc + orders.iter().fold(0.0, |sum, &idx| {
                        sum + self.entry_pool.get(idx).map_or(0.0, |entry| entry.quantity)
                    })
                } else {
                    acc
                }
            });
        
        // Calculate total depth
        metrics.total_depth = metrics.total_bid_depth + metrics.total_ask_depth;
        
        // Best level depths
        metrics.best_bid_depth = if let Some(best_bid) = NotNan::new(metrics.best_bid).ok() {
            if let Some(indices) = bids.get(&best_bid) {
                if let Ok(orders) = indices.value().read() {
                    orders.iter().fold(0.0, |sum, &idx| {
                        sum + self.entry_pool.get(idx).map_or(0.0, |entry| entry.quantity)
                    })
                } else {
                    0.0
                }
            } else {
                0.0
            }
        } else {
            0.0
        };
        
        metrics.best_ask_depth = if let Some(best_ask) = NotNan::new(metrics.best_ask).ok() {
            if let Some(indices) = asks.get(&best_ask) {
                if let Ok(orders) = indices.value().read() {
                    orders.iter().fold(0.0, |sum, &idx| {
                        sum + self.entry_pool.get(idx).map_or(0.0, |entry| entry.quantity)
                    })
                } else {
                    0.0
                }
            } else {
                0.0
            }
        } else {
            0.0
        };
    }
    
    // Calculate price drift
    #[inline]
    fn calculate_drift(&self, metrics: &mut OrderbookMetrics) {
        // Calculate log return instead of simple price difference
        metrics.drift = if metrics.previous_mid_price > 0.0 && metrics.mid_price > 0.0 {
            (metrics.mid_price / metrics.previous_mid_price).ln()
        } else {
            0.0
        };
    }
    
    // Calculate price variance
    #[inline]
    fn calculate_variance(
        &self,
        bids: &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>, 
        asks: &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>,
        metrics: &mut OrderbookMetrics
    ) {
        // Pre-allocate with capacity for better performance
        let mut prices = Vec::with_capacity(bids.len() + asks.len());
        
        // Extract prices
        for entry in bids.iter() {
            prices.push(entry.key().into_inner());
        }
        
        for entry in asks.iter() {
            prices.push(entry.key().into_inner());
        }
        
        // Handle variance calculation with error handling
        metrics.variance = if prices.len() > 1 {
            prices.variance()
        } else {
            metrics.previous_variance // Use previous variance instead of 0.0 for stability
        };
    }
    
    // Calculate depth by price tick
    #[inline]
    fn calculate_depth_by_tick(
        &self,
        bids: &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>, 
        asks: &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>>,
        metrics: &mut OrderbookMetrics
    ) {
        // Pre-allocate vectors with capacity for better performance
        let bid_count = bids.len();
        let ask_count = asks.len();
        
        // Bid depth by tick
        metrics.bid_depth_by_tick = Vec::with_capacity(bid_count);
        for entry in bids.iter() {
            let price = entry.key();
            let indices = entry.value();
            if let Ok(orders) = indices.read() {
                let total_depth = orders.iter().fold(0.0, |sum, &idx| {
                    sum + self.entry_pool.get(idx).map_or(0.0, |entry| entry.quantity)
                });
                metrics.bid_depth_by_tick.push((price.into_inner(), total_depth));
            }
        }
        
        // Ask depth by tick
        metrics.ask_depth_by_tick = Vec::with_capacity(ask_count);
        for entry in asks.iter() {
            let price = entry.key();
            let indices = entry.value();
            if let Ok(orders) = indices.read() {
                let total_depth = orders.iter().fold(0.0, |sum, &idx| {
                    sum + self.entry_pool.get(idx).map_or(0.0, |entry| entry.quantity)
                });
                metrics.ask_depth_by_tick.push((price.into_inner(), total_depth));
            }
        }
    }
    
    // Calculate orderbook imbalance
    #[inline]
    fn calculate_orderbook_imbalance(&self, metrics: &mut OrderbookMetrics) {
        let total_bid_depth = metrics.total_bid_depth;
        let total_ask_depth = metrics.total_ask_depth;
        let total_depth = total_bid_depth + total_ask_depth;
        
        // Avoid division by zero
        metrics.orderbook_imbalance = if total_depth > 0.0 {
            (total_bid_depth - total_ask_depth) / total_depth
        } else {
            0.0
        };
    }
    
    // Calculate smoothed orderbook imbalance
    #[inline]
    fn calculate_smoothed_orderbook_imbalance(&self, metrics: &mut OrderbookMetrics) {
        const ALPHA: f64 = 0.2;  // Smoothing factor
        
        // Exponential moving average for smoothed imbalance
        metrics.smoothed_orderbook_imbalance = 
            ALPHA * metrics.orderbook_imbalance + 
            (1.0 - ALPHA) * metrics.smoothed_orderbook_imbalance;
    }
    
    // Calculate liquidity-weighted orderbook imbalance
    #[inline]
    fn calculate_liquidity_weighted_orderbook_imbalance(&self, metrics: &mut OrderbookMetrics) {
        let previous_bid_depth = metrics.previous_total_bid_depth;
        let previous_ask_depth = metrics.previous_total_ask_depth;
        let total_bid_depth = metrics.total_bid_depth;
        let total_ask_depth = metrics.total_ask_depth;

        // Calculate depth changes
        let delta_bid_plus = f64::max(total_bid_depth, previous_bid_depth) - f64::min(total_bid_depth, previous_bid_depth);
        let delta_ask_plus = f64::max(total_ask_depth, previous_ask_depth) - f64::min(total_ask_depth, previous_ask_depth);
        
        // Liquidity-weighted imbalance calculation
        metrics.liquidity_weighted_orderbook_imbalance = 
            delta_bid_plus - delta_ask_plus;
    }

    // Getter for metrics
    #[inline]
    pub fn metrics(&self) -> Result<OrderbookMetrics, &'static str> {
        match self.metrics.read() {
            Ok(metrics) => Ok(metrics.clone()),
            Err(_) => Err("Failed to acquire metrics read lock"),
        }
    }
    
    // Get average update time
    #[inline]
    pub fn average_update_time(&self) -> Duration {
        let update_count = self.update_count.load(Ordering::Relaxed);
        let total_time = self.total_update_time_ns.load(Ordering::Relaxed);
        
        if update_count > 0 {
            Duration::from_nanos(total_time / update_count)
        } else {
            Duration::from_nanos(0)
        }
    }

    // Get bids
    #[inline]
    pub fn get_bids(&self) -> &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>> {
        &self.bids
    }

    // Get asks
    #[inline]
    pub fn get_asks(&self) -> &SkipMap<NotNan<f64>, RwLock<VecDeque<u64>>> {
        &self.asks
    }
}

// Order management methods
impl Orderbook {
    // Method to add a limit bid order - returns order index in the pool
    #[inline]
    pub fn add_limit_bid(&mut self, price: f64, order_id: u64, quantity: f64, timestamp: u64) -> Result<u64, &'static str> {
        // Enhanced validation for trading safety
        if !quantity.is_finite() || quantity <= 0.0 {
            return Err("Quantity must be finite and positive");
        }
        if !price.is_finite() || price <= 0.0 {
            return Err("Price must be finite and positive");
        }
        if quantity > 1_000_000.0 {
            return Err("Quantity exceeds maximum allowed");
        }
        if price > 10_000_000.0 {
            return Err("Price exceeds maximum allowed");
        }
        if quantity < 1e-8 {
            return Err("Quantity below minimum precision");
        }
        if price < 1e-6 {
            return Err("Price below minimum precision");
        }
        if order_id == 0 {
            return Err("Order ID cannot be zero");
        }
        
        // Convert price to NotNan for the BTreeMap key
        let ordered_price = match NotNan::new(price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid price (NaN)"),
        };
        
        // Allocate order in the memory pool
        let order_idx = match self.entry_pool.allocate(order_id, quantity, timestamp) {
            Some(idx) => idx,
            None => return Err("Order pool capacity exceeded"),
        };
        
        // Get or create price level and add the order
        self.bids.get_or_insert_with(ordered_price, || RwLock::new(VecDeque::new()));
        
        // Now get the entry and write to it
        if let Some(entry) = self.bids.get(&ordered_price) {
            if let Ok(mut orders) = entry.value().write() {
                orders.push_back(order_idx);
            } else {
                self.entry_pool.free(order_idx);
                return Err("Failed to acquire write lock on price level");
            }
        }
        
        Ok(order_idx)
    }

    // Method to remove a bid order
    #[inline]
    pub fn remove_limit_bid(&mut self, price: f64, order_id: u64) -> Result<bool, &'static str> {
        // Convert price to NotNan for the BTreeMap key
        let ordered_price = match NotNan::new(price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid price (NaN)"),
        };

        // Try to find and remove the order
        let mut order_found = false;
        let mut order_idx_to_free = None;
        
        // First, try to find and remove the order
        if let Some(price_level_entry) = self.bids.get(&ordered_price) {
            if let Ok(mut orders) = price_level_entry.value().write() {
                // Find the order position
                let mut position = None;
                for (i, &idx) in orders.iter().enumerate() {
                    if let Some(entry) = self.entry_pool.get(idx) {
                        if entry.order_id == order_id {
                            position = Some(i);
                            order_idx_to_free = Some(idx);
                            break;
                        }
                    }
                }
                
                // Remove the order if found
                if let Some(pos) = position {
                    orders.remove(pos);
                    order_found = true;
                }
            }
        }
        
        // Free the order from the pool if we removed it
        if let Some(idx) = order_idx_to_free {
            self.entry_pool.free(idx);
        }
        
        // Check if we need to remove the price level (it's empty)
        if order_found {
            let should_remove = if let Some(price_level_entry) = self.bids.get(&ordered_price) {
                if let Ok(orders) = price_level_entry.value().read() {
                    orders.is_empty()
                } else {
                    false
                }
            } else {
                false
            };
            
            if should_remove {
                self.bids.remove(&ordered_price);
            }
        }
        
        Ok(order_found)
    }

    // Method to edit a bid order quantity
    #[inline]
    pub fn edit_limit_bid(&mut self, price: f64, order_id: u64, new_quantity: f64) -> Result<bool, &'static str> {
        if new_quantity <= 0.0 {
            return Err("Quantity must be positive");
        }
        
        // Convert price to NotNan for the BTreeMap key
        let ordered_price = match NotNan::new(price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid price (NaN)"),
        };

        if let Some(price_level) = self.bids.get(&ordered_price) {
            if let Ok(orders) = price_level.value().read() {
                // Find the order and update quantity
                for &idx in orders.iter() {
                    // Check if this is the order we're looking for first
                    if let Some(entry) = self.entry_pool.get(idx) {
                        if entry.order_id == order_id {
                            // Using the new get_mut method with a closure
                            let updated = self.entry_pool.get_mut(idx, |entry| {
                                entry.quantity = new_quantity;
                            });
                            return Ok(updated);
                        }
                    }
                }
            }
        }
        
        // Order not found
        Ok(false)
    }

    // Method to modify a limit bid order by changing its price
    #[inline]
    pub fn modify_limit_bid(&mut self, old_price: f64, order_id: u64, new_price: f64, new_quantity: f64, timestamp: u64) 
    -> Result<bool, &'static str> 
    {
        // First, find and remove the existing order
        let old_ordered_price = match NotNan::new(old_price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid old price (NaN)"),
        };
        
        let new_ordered_price = match NotNan::new(new_price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid new price (NaN)"),
        };

        // Find and remove the order from the old price level
        let mut order_idx_to_move = None;
        
        if let Some(old_price_level) = self.bids.get(&old_ordered_price) {
            if let Ok(mut orders) = old_price_level.value().write() {
                // Find the order
                let mut position = None;
                for (i, &idx) in orders.iter().enumerate() {
                    if let Some(entry) = self.entry_pool.get(idx) {
                        if entry.order_id == order_id {
                            position = Some(i);
                            order_idx_to_move = Some(idx);
                            break;
                        }
                    }
                }
                
                // Remove the order if found
                if let Some(pos) = position {
                    orders.remove(pos);
                }
            }
        }
        
        // Check if old price level is empty and remove it
        if let Some(old_price_level) = self.bids.get(&old_ordered_price) {
            let should_remove = if let Ok(orders) = old_price_level.value().read() {
                orders.is_empty()
            } else {
                false
            };
            
            if should_remove {
                self.bids.remove(&old_ordered_price);
            }
        }
        
        // If order was found, update it and move to new price
        if let Some(idx) = order_idx_to_move {
            // Update the order entry
            let updated = self.entry_pool.get_mut(idx, |entry| {
                entry.quantity = new_quantity;
            });
            
            if updated {
                // Add to the new price level
                self.bids.get_or_insert_with(new_ordered_price, || RwLock::new(VecDeque::new()));
                
                if let Some(entry) = self.bids.get(&new_ordered_price) {
                    if let Ok(mut orders) = entry.value().write() {
                        orders.push_back(idx);
                        Ok(true)
                    } else {
                        self.entry_pool.free(idx);
                        Err("Failed to acquire write lock on new price level")
                    }
                } else {
                    self.entry_pool.free(idx);
                    Ok(false)
                }
            } else {
                self.entry_pool.free(idx); // Free the entry if we can't update it
                Ok(false)
            }
        } else {
            // Order not found, create a new one
            match self.add_limit_bid(new_price, order_id, new_quantity, timestamp) {
                Ok(_) => Ok(true),
                Err(e) => Err(e),
            }
        }
    }
    
    // Method to add an ask order
    #[inline]
    pub fn add_limit_ask(&mut self, price: f64, order_id: u64, quantity: f64, timestamp: u64) -> Result<u64, &'static str> {
        // Validate inputs
        if quantity <= 0.0 {
            return Err("Quantity must be positive");
        }
        
        if price <= 0.0 {
            return Err("Price must be positive");
        }
        
        // Convert price to NotNan for the BTreeMap key
        let ordered_price = match NotNan::new(price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid price (NaN)"),
        };
        
        // Allocate order in the memory pool
        let order_idx = match self.entry_pool.allocate(order_id, quantity, timestamp) {
            Some(idx) => idx,
            None => return Err("Order pool capacity exceeded"),
        };
        
        // Get or create price level and add the order
        self.asks.get_or_insert_with(ordered_price, || RwLock::new(VecDeque::new()));
        
        // Now get the entry and write to it
        if let Some(entry) = self.asks.get(&ordered_price) {
            if let Ok(mut orders) = entry.value().write() {
                orders.push_back(order_idx);
            } else {
                self.entry_pool.free(order_idx);
                return Err("Failed to acquire write lock on price level");
            }
        }
        
        Ok(order_idx)
    }
    
    #[inline]
    pub fn match_market_bid(&mut self, price_limit: f64, quantity: f64) 
    -> Result<ExecutionResult, &'static str> 
    {
        let start = Instant::now();
        
        // Validate inputs
        if quantity <= 0.0 {
            return Err("Quantity must be positive");
        }
        
        if price_limit <= 0.0 {
            return Err("Price limit must be positive");
        }
        
        // Convert price limit to NotNan
        let price_limit_nonnan = match NotNan::new(price_limit) {
            Ok(p) => p,
            Err(_) => return Err("Invalid price limit (NaN)"),
        };
        
        // Initialize execution tracking variables
        let mut remaining_quantity = quantity;
        let mut total_executed_quantity = 0.0;
        let mut total_execution_price = 0.0;
        let mut price_levels_to_remove = Vec::new();

        // Get all eligible ask price levels (price <= price_limit)
        let eligible_prices: Vec<NotNan<f64>> = self.asks.iter()
            .map(|entry| *entry.key())
            .take_while(|&p| p <= price_limit_nonnan)
            .collect();
        
        // Process each price level in order (lowest to highest)
        for ask_price in eligible_prices {
            if remaining_quantity <= 0.0 {
                break;
            }
            
            let ask_price_value = ask_price.into_inner();
            
            if let Some(ask_orders_entry) = self.asks.get(&ask_price) {
                if let Ok(mut ask_orders) = ask_orders_entry.value().write() {
                    let mut orders_to_remove = Vec::new();
                    let mut i = 0;
                    
                    // Match orders at this price level
                    while i < ask_orders.len() && remaining_quantity > 0.0 {
                        let idx = ask_orders[i];
                        
                        // Get the order details first
                        let order_opt = self.entry_pool.get(idx);
                        
                        if let Some(order) = order_opt {
                            // Determine executable quantity
                            let executable_quantity = f64::min(order.quantity, remaining_quantity);
                            
                            // Update execution metrics
                            total_execution_price += ask_price_value * executable_quantity;
                            total_executed_quantity += executable_quantity;
                            remaining_quantity -= executable_quantity;
                            
                            // Update or mark for removal
                            if executable_quantity >= order.quantity {
                                // Mark for removal after iteration
                                orders_to_remove.push(i);
                            } else {
                                // Decrease the quantity of the ask order using get_mut with closure
                                let final_qty = order.quantity - executable_quantity;
                                self.entry_pool.get_mut(idx, |entry| {
                                    entry.quantity = final_qty;
                                });
                            }
                        }
                        
                        i += 1;
                    }
                    
                    // Remove filled orders from back to front to maintain indices
                    for &idx in orders_to_remove.iter().rev() {
                        if let Some(order_idx) = ask_orders.remove(idx) {
                            self.entry_pool.free(order_idx);
                        }
                    }
                    
                    // Mark empty price levels for removal
                    if ask_orders.is_empty() {
                        price_levels_to_remove.push(ask_price);
                    }
                }
            }
        }
        
        // Remove empty price levels
        for price_level in price_levels_to_remove {
            self.asks.remove(&price_level);
        }
        
        // Calculate execution metrics
        let execution_time = start.elapsed();
        let average_price = if total_executed_quantity > 0.0 {
            total_execution_price / total_executed_quantity
        } else {
            0.0
        };
        
        // Update performance metrics
        let mut metrics = match self.metrics.write() {
            Ok(metrics) => metrics,
            Err(_) => return Err("Failed to acquire metrics write lock"),
        };
        
        metrics.last_match_duration_ns = execution_time.as_nanos() as u64;
        metrics.match_count += 1;
        
        // Update the running average of match durations
        metrics.avg_match_duration_ns = 
            ((metrics.avg_match_duration_ns * (metrics.match_count - 1)) + metrics.last_match_duration_ns) / 
            metrics.match_count;
        
        // Create and return execution result
        let status = if remaining_quantity <= 0.0 {
            ExecutionStatus::Complete
        } else if total_executed_quantity > 0.0 {
            ExecutionStatus::Partial
        } else {
            ExecutionStatus::Unfilled
        };
        
        Ok(ExecutionResult {
            executed_quantity: total_executed_quantity,
            remaining_quantity,
            average_price,
            execution_time,
            status,
        })
    }

    // Method to remove an ask order
    #[inline]
    pub fn remove_limit_ask(&mut self, price: f64, order_id: u64) -> Result<bool, &'static str> {
        // Convert price to NotNan for the BTreeMap key
        let ordered_price = match NotNan::new(price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid price (NaN)"),
        };

        // Try to find and remove the order
        let mut order_found = false;
        let mut order_idx_to_free = None;
        
        // First, try to find and remove the order
        if let Some(price_level_entry) = self.asks.get(&ordered_price) {
            if let Ok(mut orders) = price_level_entry.value().write() {
                // Find the order position
                let mut position = None;
                for (i, &idx) in orders.iter().enumerate() {
                    if let Some(entry) = self.entry_pool.get(idx) {
                        if entry.order_id == order_id {
                            position = Some(i);
                            order_idx_to_free = Some(idx);
                            break;
                        }
                    }
                }
                
                // Remove the order if found
                if let Some(pos) = position {
                    orders.remove(pos);
                    order_found = true;
                }
            }
        }
        
        // Free the order from the pool if we removed it
        if let Some(idx) = order_idx_to_free {
            self.entry_pool.free(idx);
        }
        
        // Check if we need to remove the price level (it's empty)
        if order_found {
            let should_remove = if let Some(price_level_entry) = self.asks.get(&ordered_price) {
                if let Ok(orders) = price_level_entry.value().read() {
                    orders.is_empty()
                } else {
                    false
                }
            } else {
                false
            };
            
            if should_remove {
                self.asks.remove(&ordered_price);
            }
        }
        
        Ok(order_found)
    }

    // Method to edit an ask order quantity
    #[inline]
    pub fn edit_limit_ask(&mut self, price: f64, order_id: u64, new_quantity: f64) -> Result<bool, &'static str> {
        if new_quantity <= 0.0 {
            return Err("Quantity must be positive");
        }
        
        // Convert price to NotNan for the BTreeMap key
        let ordered_price = match NotNan::new(price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid price (NaN)"),
        };

        if let Some(price_level) = self.asks.get(&ordered_price) {
            if let Ok(orders) = price_level.value().read() {
                // Find the order and update quantity
                for &idx in orders.iter() {
                    // Check if this is the order we're looking for first
                    if let Some(entry) = self.entry_pool.get(idx) {
                        if entry.order_id == order_id {
                            // Using the new get_mut method with a closure
                            let updated = self.entry_pool.get_mut(idx, |entry| {
                                entry.quantity = new_quantity;
                            });
                            return Ok(updated);
                        }
                    }
                }
            }
        }
        
        // Order not found
        Ok(false)
    }

    // Method to modify a limit ask order by changing its price
    #[inline]
    pub fn modify_limit_ask(&mut self, old_price: f64, order_id: u64, new_price: f64, new_quantity: f64, timestamp: u64) 
    -> Result<bool, &'static str> 
    {
        // First, find and remove the existing order
        let old_ordered_price = match NotNan::new(old_price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid old price (NaN)"),
        };
        
        let new_ordered_price = match NotNan::new(new_price) {
            Ok(p) => p,
            Err(_) => return Err("Invalid new price (NaN)"),
        };

        // Find and remove the order from the old price level
        let mut order_idx_to_move = None;
        
        if let Some(old_price_level) = self.asks.get(&old_ordered_price) {
            if let Ok(mut orders) = old_price_level.value().write() {
                // Find the order
                let mut position = None;
                for (i, &idx) in orders.iter().enumerate() {
                    if let Some(entry) = self.entry_pool.get(idx) {
                        if entry.order_id == order_id {
                            position = Some(i);
                            order_idx_to_move = Some(idx);
                            break;
                        }
                    }
                }
                
                // Remove the order if found
                if let Some(pos) = position {
                    orders.remove(pos);
                }
            }
        }
        
        // Check if old price level is empty and remove it
        if let Some(old_price_level) = self.asks.get(&old_ordered_price) {
            let should_remove = if let Ok(orders) = old_price_level.value().read() {
                orders.is_empty()
            } else {
                false
            };
            
            if should_remove {
                self.asks.remove(&old_ordered_price);
            }
        }
        
        // If order was found, update it and move to new price
        if let Some(idx) = order_idx_to_move {
            // Update the order entry
            let updated = self.entry_pool.get_mut(idx, |entry| {
                entry.quantity = new_quantity;
            });
            
            if updated {
                // Add to the new price level
                self.asks.get_or_insert_with(new_ordered_price, || RwLock::new(VecDeque::new()));
                
                if let Some(entry) = self.asks.get(&new_ordered_price) {
                    if let Ok(mut orders) = entry.value().write() {
                        orders.push_back(idx);
                        Ok(true)
                    } else {
                        self.entry_pool.free(idx);
                        Err("Failed to acquire write lock on new price level")
                    }
                } else {
                    self.entry_pool.free(idx);
                    Ok(false)
                }
            } else {
                self.entry_pool.free(idx); // Free the entry if we can't update it
                Ok(false)
            }
        } else {
            // Order not found, create a new one
            match self.add_limit_ask(new_price, order_id, new_quantity, timestamp) {
                Ok(_) => Ok(true),
                Err(e) => Err(e),
            }
        }
    }
    
    // Method to match a market ask (sell) order
    #[inline]
    pub fn match_market_ask(&mut self, price_limit: f64, quantity: f64) 
    -> Result<ExecutionResult, &'static str> 
    {
        let start = Instant::now();
        
        // Validate inputs
        if quantity <= 0.0 {
            return Err("Quantity must be positive");
        }
        
        if price_limit <= 0.0 {
            return Err("Price limit must be positive");
        }
        
        // Convert price limit to NotNan
        let price_limit_nonnan = match NotNan::new(price_limit) {
            Ok(p) => p,
            Err(_) => return Err("Invalid price limit (NaN)"),
        };
        
        // Initialize execution tracking variables
        let mut remaining_quantity = quantity;
        let mut total_executed_quantity = 0.0;
        let mut total_execution_price = 0.0;
        let mut price_levels_to_remove = Vec::new();

        // Get all eligible bid price levels (price >= price_limit)
        let eligible_prices: Vec<NotNan<f64>> = self.bids.iter()
            .rev() // Get highest to lowest
            .map(|entry| *entry.key())
            .take_while(|&p| p >= price_limit_nonnan)
            .collect();
        
        // Process each price level in order (highest to lowest)
        for bid_price in eligible_prices {
            if remaining_quantity <= 0.0 {
                break;
            }
            
            let bid_price_value = bid_price.into_inner();
            
            if let Some(bid_orders_entry) = self.bids.get(&bid_price) {
                if let Ok(mut bid_orders) = bid_orders_entry.value().write() {
                    let mut orders_to_remove = Vec::new();
                    let mut i = 0;
                    
                    // Match orders at this price level
                    while i < bid_orders.len() && remaining_quantity > 0.0 {
                        let idx = bid_orders[i];
                        
                        // Get the order details first
                        let order_opt = self.entry_pool.get(idx);
                        
                        if let Some(order) = order_opt {
                            // Determine executable quantity
                            let executable_quantity = f64::min(order.quantity, remaining_quantity);
                            
                            // Update execution metrics
                            total_execution_price += bid_price_value * executable_quantity;
                            total_executed_quantity += executable_quantity;
                            remaining_quantity -= executable_quantity;
                            
                            // Update or mark for removal
                            if executable_quantity >= order.quantity {
                                // Mark for removal after iteration
                                orders_to_remove.push(i);
                            } else {
                                // Decrease the quantity of the bid order using get_mut with closure
                                let final_qty = order.quantity - executable_quantity;
                                self.entry_pool.get_mut(idx, |entry| {
                                    entry.quantity = final_qty;
                                });
                            }
                        }
                        
                        i += 1;
                    }
                    
                    // Remove filled orders from back to front to maintain indices
                    for &idx in orders_to_remove.iter().rev() {
                        if let Some(order_idx) = bid_orders.remove(idx) {
                            self.entry_pool.free(order_idx);
                        }
                    }
                    
                    // Mark empty price levels for removal
                    if bid_orders.is_empty() {
                        price_levels_to_remove.push(bid_price);
                    }
                }
            }
        }
        
        // Remove empty price levels
        for price_level in price_levels_to_remove {
            self.bids.remove(&price_level);
        }
        
        // Calculate execution metrics
        let execution_time = start.elapsed();
        let average_price = if total_executed_quantity > 0.0 {
            total_execution_price / total_executed_quantity
        } else {
            0.0
        };
        
        // Update performance metrics
        let mut metrics = match self.metrics.write() {
            Ok(metrics) => metrics,
            Err(_) => return Err("Failed to acquire metrics write lock"),
        };
        
        metrics.last_match_duration_ns = execution_time.as_nanos() as u64;
        metrics.match_count += 1;
        
        // Update the running average of match durations
        metrics.avg_match_duration_ns = 
            ((metrics.avg_match_duration_ns * (metrics.match_count - 1)) + metrics.last_match_duration_ns) / 
            metrics.match_count;
        
        // Create and return execution result
        let status = if remaining_quantity <= 0.0 {
            ExecutionStatus::Complete
        } else if total_executed_quantity > 0.0 {
            ExecutionStatus::Partial
        } else {
            ExecutionStatus::Unfilled
        };
        
        Ok(ExecutionResult {
            executed_quantity: total_executed_quantity,
            remaining_quantity,
            average_price,
            execution_time,
            status,
        })
    }

    // Method to get orderbook levels up to a specified depth
    #[inline]
    pub fn get_orderbook_levels(&self, depth: usize) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), &'static str> {
        // Collect bid levels (highest to lowest)
        let mut bid_levels = Vec::with_capacity(depth);
        for entry in self.bids.iter().rev().take(depth) {
            let price = entry.key();
            let indices = entry.value();
            if let Ok(orders) = indices.read() {
                let total_quantity = orders.iter().fold(0.0, |sum, &idx| {
                    sum + self.entry_pool.get(idx).map_or(0.0, |entry| entry.quantity)
                });
                bid_levels.push((price.into_inner(), total_quantity));
            }
        }
        
        // Collect ask levels (lowest to highest)
        let mut ask_levels = Vec::with_capacity(depth);
        for entry in self.asks.iter().take(depth) {
            let price = entry.key();
            let indices = entry.value();
            if let Ok(orders) = indices.read() {
                let total_quantity = orders.iter().fold(0.0, |sum, &idx| {
                    sum + self.entry_pool.get(idx).map_or(0.0, |entry| entry.quantity)
                });
                ask_levels.push((price.into_inner(), total_quantity));
            }
        }
        
        Ok((bid_levels, ask_levels))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_orderbook_creation() {
        let orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        let metrics = orderbook.metrics().unwrap();
        
        // Basic assertions
        assert_eq!(orderbook.instrument, "BTC/USD");
        assert_eq!(orderbook.exchange, "Kraken");
        assert_eq!(metrics.total_bid_depth, 0.0);
        assert_eq!(metrics.total_ask_depth, 0.0);
        assert_eq!(metrics.mid_price, 0.0);
        assert_eq!(metrics.spread, 0.0);
        assert_eq!(metrics.best_bid, 0.0);
        assert_eq!(metrics.best_ask, 0.0);
        assert_eq!(metrics.total_depth, 0.0);
    }

    #[test]
    fn test_add_limit_bid_and_ask() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add bids
        orderbook.add_limit_bid(100.0, 1, 10.0, 1000).unwrap();
        orderbook.add_limit_bid(100.0, 2, 20.0, 1001).unwrap();
        
        // Add asks
        orderbook.add_limit_ask(101.0, 3, 15.0, 1002).unwrap();
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Basic assertions
        assert_eq!(metrics.total_bid_depth, 30.0);
        assert_eq!(metrics.best_bid, 100.0);
        assert_eq!(metrics.best_ask, 101.0);
    }

    #[test]
    fn test_remove_limit_bid_and_ask() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add bid
        orderbook.add_limit_bid(100.0, 1, 10.0, 1000).unwrap();
        
        // Remove bid
        let result = orderbook.remove_limit_bid(100.0, 1).unwrap();
        assert!(result);
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Assertions
        assert_eq!(metrics.total_bid_depth, 0.0);
    }

    #[test]
    fn test_edit_limit_bid_and_ask() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add bid
        orderbook.add_limit_bid(100.0, 1, 10.0, 1000).unwrap();
        
        // Edit bid
        let result = orderbook.edit_limit_bid(100.0, 1, 20.0).unwrap();
        assert!(result);
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Assertions
        assert_eq!(metrics.total_bid_depth, 20.0);
    }

    #[test]
    fn test_modify_limit_bid_changes_price() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add initial bid
        orderbook.add_limit_bid(100.0, 1, 10.0, 1000).unwrap();
        
        // Modify bid to a different price
        let result = orderbook.modify_limit_bid(100.0, 1, 101.0, 10.0, 1001).unwrap();
        assert!(result);
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Verify best bid is now at the new price
        assert_eq!(metrics.best_bid, 101.0);
    }

    #[test]
    fn test_market_bid_with_slippage() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add asks at different price levels
        orderbook.add_limit_ask(101.0, 1, 10.0, 1000).unwrap();
        orderbook.add_limit_ask(102.0, 2, 15.0, 1001).unwrap();
        
        // Update metrics before market order
        orderbook.update().unwrap();
        
        // Place a market buy order
        let result = orderbook.match_market_bid(103.0, 20.0).unwrap();
        
        // Assert execution status and remaining orderbook state
        assert_eq!(result.status, ExecutionStatus::Complete);
        assert!(result.average_price > 101.0);  // Should be between 101 and 102 due to slippage
        
        // Update metrics after market order
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Should have consumed some asks
        assert_eq!(metrics.total_ask_depth, 5.0);  // 25 original - 20 executed = 5 remaining
    }

    #[test]
    fn test_market_ask_with_slippage() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add bids at different price levels
        orderbook.add_limit_bid(99.0, 1, 10.0, 1000).unwrap();
        orderbook.add_limit_bid(98.0, 2, 15.0, 1001).unwrap();
        
        // Update metrics before market order
        orderbook.update().unwrap();
        
        // Place a market sell order
        let result = orderbook.match_market_ask(97.0, 20.0).unwrap();
        
        // Assert execution status and remaining orderbook state
        assert_eq!(result.status, ExecutionStatus::Complete);
        assert!(result.average_price > 98.0);  // Should be between 98 and 99 due to slippage
        
        // Update metrics after market order
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Should have consumed some bids
        assert_eq!(metrics.total_bid_depth, 5.0);  // 25 original - 20 executed = 5 remaining
    }
    
    #[test]
    fn test_market_bid_partial_fill() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add a small ask
        orderbook.add_limit_ask(101.0, 1, 5.0, 1000).unwrap();
        
        // Update metrics
        orderbook.update().unwrap();
        
        // Place a market buy order larger than available liquidity
        let result = orderbook.match_market_bid(101.0, 10.0).unwrap();
        
        // Assert partial execution
        assert_eq!(result.status, ExecutionStatus::Partial);
        assert_eq!(result.executed_quantity, 5.0);
        assert_eq!(result.remaining_quantity, 5.0);
        assert_eq!(result.average_price, 101.0);
        
        // Orderbook should be empty
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        assert_eq!(metrics.total_ask_depth, 0.0);
    }
    
    #[test]
    fn test_market_order_no_matching_orders() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Empty orderbook, place a market buy
        let result = orderbook.match_market_bid(100.0, 10.0).unwrap();
        
        // Assert no execution
        assert_eq!(result.status, ExecutionStatus::Unfilled);
        assert_eq!(result.executed_quantity, 0.0);
        assert_eq!(result.remaining_quantity, 10.0);
        assert_eq!(result.average_price, 0.0);
    }
    
    #[test]
    fn test_price_time_priority() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add several asks at the same price level but different times
        orderbook.add_limit_ask(100.0, 1, 5.0, 1000).unwrap(); // Earlier timestamp
        orderbook.add_limit_ask(100.0, 2, 5.0, 1001).unwrap(); // Later timestamp
        
        // Add an ask at a higher price
        orderbook.add_limit_ask(101.0, 3, 5.0, 1002).unwrap();
        
        // Execute a market order that will only fill the first level
        let result = orderbook.match_market_bid(100.0, 7.0).unwrap();
        
        // Should have executed 7 units, with 3 remaining at price level 100.0
        assert_eq!(result.executed_quantity, 7.0);
        assert_eq!(result.status, ExecutionStatus::Complete);
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Should have 3 + 5 = 8 left (3 at best price, 5 at higher price)
        assert_eq!(metrics.total_ask_depth, 8.0);
    }
    
    #[test]
    fn test_concurrent_access() {
        let orderbook = Arc::new(Mutex::new(Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 100000)));
        
        // Spawn multiple threads to add orders
        let mut handles = vec![];
        
        // Add bids from multiple threads
        for i in 0..5 {
            let ob = Arc::clone(&orderbook);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let order_id = i * 1000 + j;
                    let price = 100.0 - (j as f64 / 100.0);
                    ob.lock().unwrap().add_limit_bid(price, order_id, 1.0, order_id).unwrap();
                }
            });
            handles.push(handle);
        }
        
        // Add asks from multiple threads
        for i in 5..10 {
            let ob = Arc::clone(&orderbook);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let order_id = i * 1000 + j;
                    let price = 101.0 + (j as f64 / 100.0);
                    ob.lock().unwrap().add_limit_ask(price, order_id, 1.0, order_id).unwrap();
                }
            });
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Update metrics
        orderbook.lock().unwrap().update().unwrap();
        let metrics = orderbook.lock().unwrap().metrics().unwrap();
        
        // Should have 500 bids and 500 asks
        assert_eq!(metrics.total_bid_depth, 500.0);
        assert_eq!(metrics.total_ask_depth, 500.0);
    }
    
    #[test]
    fn test_performance_benchmarks() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 100000);
        
        // Add a significant number of orders
        for i in 0..1000 {
            let price = 100.0 - (i as f64 / 100.0);
            orderbook.add_limit_bid(price, i, 1.0, i).unwrap();
            
            let ask_price = 101.0 + (i as f64 / 100.0);
            orderbook.add_limit_ask(ask_price, i + 1000, 1.0, i).unwrap();
        }
        
        // Measure update time
        let start = Instant::now();
        orderbook.update().unwrap();
        let update_time = start.elapsed();
        
        // Measure market order execution time
        let start = Instant::now();
        orderbook.match_market_bid(101.0, 10.0).unwrap();
        let execution_time = start.elapsed();
        
        // Print performance metrics (not assertions since they depend on hardware)
        println!("Update time: {:?}", update_time);
        println!("Market order execution time: {:?}", execution_time);
        
        // Add some basic thresholds for CI environments
        assert!(update_time < Duration::from_millis(50), "Update too slow");
        assert!(execution_time < Duration::from_millis(10), "Market order execution too slow");
    }
    
    #[test]
    fn test_memory_pool_capacity() {
        // Create a small orderbook to test capacity limits
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 5);
        
        // Should be able to add up to capacity
        for i in 0..5 {
            let result = orderbook.add_limit_bid(100.0, i, 1.0, i);
            assert!(result.is_ok());
        }
        
        // Adding one more should fail with capacity error
        let result = orderbook.add_limit_bid(100.0, 5, 1.0, 5);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Order pool capacity exceeded");
        
        // Free up some space
        orderbook.remove_limit_bid(100.0, 0).unwrap();
        
        // Now should be able to add again
        let result = orderbook.add_limit_bid(100.0, 5, 1.0, 5);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_invalid_inputs() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Test negative quantity
        let result = orderbook.add_limit_bid(100.0, 1, -1.0, 1000);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Quantity must be positive");
        
        // Test zero quantity
        let result = orderbook.add_limit_bid(100.0, 1, 0.0, 1000);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Quantity must be positive");
        
        // Test negative price
        let result = orderbook.add_limit_bid(-100.0, 1, 1.0, 1000);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Price must be positive");
        
        // Test nan price
        let result = orderbook.add_limit_bid(f64::NAN, 1, 1.0, 1000);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid price (NaN)");
    }
    
    #[test]
    fn test_modify_nonexistent_order() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Try to edit a nonexistent order
        let result = orderbook.edit_limit_bid(100.0, 1, 20.0).unwrap();
        assert!(!result);
        
        // Try to remove a nonexistent order
        let result = orderbook.remove_limit_bid(100.0, 1).unwrap();
        assert!(!result);
    }
    
    #[test]
    fn test_cross_market_orders() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Set up a cross (bids higher than asks)
        orderbook.add_limit_bid(101.0, 1, 10.0, 1000).unwrap();
        orderbook.add_limit_ask(100.0, 2, 5.0, 1001).unwrap();
        
        // Execute market order at this cross
        let result = orderbook.match_market_bid(102.0, 3.0).unwrap();
        
        // Should execute at the ask price (100.0)
        assert_eq!(result.average_price, 100.0);
        assert_eq!(result.executed_quantity, 3.0);
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Should have 2 units left at ask price 100.0
        assert_eq!(metrics.total_ask_depth, 2.0);
    }
    
    #[test]
    fn test_fixedprice_conversion() {
        // Test conversion from f64 to FixedPrice and back
        let original = 123.45678;
        let fixed = FixedPrice::from_f64(original).unwrap();
        let roundtrip = fixed.to_f64();
        
        // Should be very close (within floating-point precision)
        assert!((original - roundtrip).abs() < 1e-10);
        
        // Test error handling
        let nan_result = FixedPrice::from_f64(f64::NAN);
        assert!(nan_result.is_err());
        
        let inf_result = FixedPrice::from_f64(f64::INFINITY);
        assert!(inf_result.is_err());
    }
    
    #[test]
    fn test_ask_price_ordering() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add asks in reverse order (high to low)
        orderbook.add_limit_ask(103.0, 1, 5.0, 1000).unwrap();
        orderbook.add_limit_ask(102.0, 2, 5.0, 1001).unwrap();
        orderbook.add_limit_ask(101.0, 3, 5.0, 1002).unwrap();
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Best ask should be the lowest price
        assert_eq!(metrics.best_ask, 101.0);
        
        // Match a market order
        let result = orderbook.match_market_bid(103.0, 10.0).unwrap();
        
        // Should execute at the lowest prices first
        assert!(result.average_price < 102.0); // Should be weighted toward 101.0
    }
    
    #[test]
    fn test_bid_price_ordering() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add bids in reverse order (low to high)
        orderbook.add_limit_bid(97.0, 1, 5.0, 1000).unwrap();
        orderbook.add_limit_bid(98.0, 2, 5.0, 1001).unwrap();
        orderbook.add_limit_bid(99.0, 3, 5.0, 1002).unwrap();
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Best bid should be the highest price
        assert_eq!(metrics.best_bid, 99.0);
        
        // Match a market order
        let result = orderbook.match_market_ask(97.0, 10.0).unwrap();
        
        // Should execute at the highest prices first
        assert!(result.average_price > 98.0); // Should be weighted toward 99.0
    }
    
    #[test]
    fn test_liquidity_metrics() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add equal bids and asks
        orderbook.add_limit_bid(99.0, 1, 10.0, 1000).unwrap();
        orderbook.add_limit_ask(101.0, 2, 10.0, 1001).unwrap();
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Orderbook imbalance should be 0 for equal bid/ask depth
        assert_eq!(metrics.orderbook_imbalance, 0.0);
        
        // Add more bids to create imbalance
        orderbook.add_limit_bid(98.0, 3, 10.0, 1002).unwrap();
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Orderbook imbalance should be positive for more bid depth
        assert!(metrics.orderbook_imbalance > 0.0);
    }
    
    #[test]
    fn test_market_order_slippage_calculation() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add asks at different price levels
        orderbook.add_limit_ask(100.0, 1, 5.0, 1000).unwrap();
        orderbook.add_limit_ask(101.0, 2, 5.0, 1001).unwrap();
        orderbook.add_limit_ask(102.0, 3, 5.0, 1002).unwrap();
        
        // Execute market order that consumes multiple levels
        let result = orderbook.match_market_bid(103.0, 12.0).unwrap();
        
        // Calculate expected average price:
        // (5 * 100 + 5 * 101 + 2 * 102) / 12 = 100.75
        let expected_avg_price = (5.0 * 100.0 + 5.0 * 101.0 + 2.0 * 102.0) / 12.0;
        
        // Should match our calculation within floating-point precision
        assert!((result.average_price - expected_avg_price).abs() < 1e-10);
    }
    
    #[test]
    fn test_get_orderbook_levels() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add bids and asks at different price levels
        orderbook.add_limit_bid(99.0, 1, 10.0, 1000).unwrap();
        orderbook.add_limit_bid(98.0, 2, 15.0, 1001).unwrap();
        orderbook.add_limit_bid(97.0, 3, 20.0, 1002).unwrap();
        
        orderbook.add_limit_ask(101.0, 4, 5.0, 1003).unwrap();
        orderbook.add_limit_ask(102.0, 5, 8.0, 1004).unwrap();
        orderbook.add_limit_ask(103.0, 6, 12.0, 1005).unwrap();
        
        // Get top 2 levels
        let (bids, asks) = orderbook.get_orderbook_levels(2).unwrap();
        
        // Should have 2 bid levels (highest first)
        assert_eq!(bids.len(), 2);
        assert_eq!(bids[0].0, 99.0);
        assert_eq!(bids[0].1, 10.0);
        assert_eq!(bids[1].0, 98.0);
        assert_eq!(bids[1].1, 15.0);
        
        // Should have 2 ask levels (lowest first)
        assert_eq!(asks.len(), 2);
        assert_eq!(asks[0].0, 101.0);
        assert_eq!(asks[0].1, 5.0);
        assert_eq!(asks[1].0, 102.0);
        assert_eq!(asks[1].1, 8.0);
    }
    
    #[test]
    fn test_memory_cleanup() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add and then remove orders
        for i in 0..100 {
            orderbook.add_limit_bid(100.0, i, 1.0, i).unwrap();
            orderbook.add_limit_ask(101.0, i + 100, 1.0, i).unwrap();
        }
        
        // Remove all orders
        for i in 0..100 {
            orderbook.remove_limit_bid(100.0, i).unwrap();
            orderbook.remove_limit_ask(101.0, i + 100).unwrap();
        }
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Orderbook should be empty
        assert_eq!(metrics.total_bid_depth, 0.0);
        assert_eq!(metrics.total_ask_depth, 0.0);
        
        // The memory pool properly reuses freed entries
        let result = orderbook.add_limit_bid(100.0, 200, 1.0, 2000);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_stress_orderbook_state() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 100000);
        
        // Add a large number of orders at many price levels
        for i in 0..1000 {
            // Add bids at 100 different price levels
            let bid_price = 90.0 + (i % 100) as f64 * 0.1;
            orderbook.add_limit_bid(bid_price, i, 1.0, i).unwrap();
            
            // Add asks at 100 different price levels
            let ask_price = 110.0 + (i % 100) as f64 * 0.1;
            orderbook.add_limit_ask(ask_price, i + 1000, 1.0, i).unwrap();
        }
        
        // Measure update time under load
        let start = Instant::now();
        orderbook.update().unwrap();
        let update_time = start.elapsed();
        
        // Execute large market orders
        let start = Instant::now();
        let bid_result = orderbook.match_market_bid(115.0, 200.0).unwrap();
        let bid_time = start.elapsed();
        
        let start = Instant::now();
        let ask_result = orderbook.match_market_ask(85.0, 200.0).unwrap();
        let ask_time = start.elapsed();
        
        // Print performance metrics
        println!("Stress test update time: {:?}", update_time);
        println!("Large market bid execution time: {:?}", bid_time);
        println!("Large market ask execution time: {:?}", ask_time);
        
        // Basic assertions
        assert_eq!(bid_result.status, ExecutionStatus::Complete);
        assert_eq!(ask_result.status, ExecutionStatus::Complete);
        
        // Performance thresholds for stress conditions
        assert!(update_time < Duration::from_millis(100), "Stress update too slow");
        assert!(bid_time < Duration::from_millis(20), "Large market bid too slow");
        assert!(ask_time < Duration::from_millis(20), "Large market ask too slow");
    }
    
    #[test]
    fn test_error_propagation() {
        // Test with very small capacity to force errors
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 1);
        
        // Add one order (fills capacity)
        orderbook.add_limit_bid(100.0, 1, 1.0, 1).unwrap();
        
        // Try to add another (should error)
        let result = orderbook.add_limit_bid(100.0, 2, 1.0, 2);
        assert!(result.is_err());
        
        // Try to add an ask (should error)
        let result = orderbook.add_limit_ask(101.0, 3, 1.0, 3);
        assert!(result.is_err());
        
        // Remove the bid to free capacity
        orderbook.remove_limit_bid(100.0, 1).unwrap();
        
        // Should now be able to add again
        let result = orderbook.add_limit_ask(101.0, 3, 1.0, 3);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_empty_order_cleanup() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add orders at multiple price levels
        orderbook.add_limit_bid(99.0, 1, 10.0, 1000).unwrap();
        orderbook.add_limit_bid(98.0, 2, 10.0, 1001).unwrap();
        orderbook.add_limit_ask(101.0, 3, 10.0, 1002).unwrap();
        orderbook.add_limit_ask(102.0, 4, 10.0, 1003).unwrap();
        
        // Execute market orders that completely consume one price level
        orderbook.match_market_bid(101.0, 10.0).unwrap();
        orderbook.match_market_ask(99.0, 10.0).unwrap();
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Should have one price level left on each side
        assert_eq!(metrics.best_bid, 98.0);
        assert_eq!(metrics.best_ask, 102.0);
        
        // Check price levels are properly removed
        let (bids, asks) = orderbook.get_orderbook_levels(10).unwrap();
        assert_eq!(bids.len(), 1); // Only one bid level left
        assert_eq!(asks.len(), 1); // Only one ask level left
    }
    
    #[test]
    fn test_large_number_of_price_levels() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 100000);
        
        // Add a large number of unique price levels
        for i in 0..1000 {
            let bid_price = 100.0 - (i as f64 * 0.01);
            let ask_price = 100.0 + (i as f64 * 0.01);
            
            orderbook.add_limit_bid(bid_price, i, 1.0, i).unwrap();
            orderbook.add_limit_ask(ask_price, i + 1000, 1.0, i).unwrap();
        }
        
        // Update metrics
        let start = Instant::now();
        orderbook.update().unwrap();
        let update_time = start.elapsed();
        
        // Verify metrics
        let metrics = orderbook.metrics().unwrap();
        assert_eq!(metrics.best_bid, 100.0); 
        assert_eq!(metrics.best_ask, 100.0);
        assert_eq!(metrics.total_bid_depth, 1000.0);
        assert_eq!(metrics.total_ask_depth, 1000.0);
        
        // Performance should be reasonably fast even with 2000 price levels
        println!("Large price levels update time: {:?}", update_time);
        assert!(update_time < Duration::from_millis(100), "Update too slow with many price levels");
    }
    
    #[test]
    fn test_spread_calculation() {
        let mut orderbook = Orderbook::new("BTC/USD".to_string(), "Kraken".to_string(), 10000);
        
        // Add orders to create a specific spread
        orderbook.add_limit_bid(100.0, 1, 10.0, 1000).unwrap();
        orderbook.add_limit_ask(101.0, 2, 10.0, 1001).unwrap();
        
        // Update metrics
        orderbook.update().unwrap();
        let metrics = orderbook.metrics().unwrap();
        
        // Spread should be 1.0
        assert_eq!(metrics.spread, 1.0);
        
        // Spread in basis points should be (1.0 / 100.5) * 10000 = ~99.5 bps
        let expected_bps = (1.0 / 100.5) * 10000.0;
        assert!((metrics.spread_bps - expected_bps).abs() < 0.01);
    }
}