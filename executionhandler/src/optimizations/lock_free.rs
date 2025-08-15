use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use crossbeam_channel::{unbounded, Receiver, Sender};
use crate::core::types::{ExecutionResult, OrderUpdate};

/// Lock-free circular buffer for high-frequency data
pub struct LockFreeRingBuffer<T> {
    buffer: Vec<std::sync::atomic::AtomicPtr<T>>,
    capacity: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl<T> LockFreeRingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()));
        }
        
        Self {
            buffer,
            capacity,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    /// Try to push an item (non-blocking)
    pub fn try_push(&self, item: Box<T>) -> Result<(), Box<T>> {
        let current_tail = self.tail.load(Ordering::Relaxed);
        let next_tail = (current_tail + 1) % self.capacity;
        
        // Check if buffer is full
        if next_tail == self.head.load(Ordering::Acquire) {
            return Err(item);
        }

        // Store the item
        let raw_item = Box::into_raw(item);
        self.buffer[current_tail].store(raw_item, Ordering::Release);
        
        // Update tail
        self.tail.store(next_tail, Ordering::Release);
        
        Ok(())
    }

    /// Try to pop an item (non-blocking)
    pub fn try_pop(&self) -> Option<Box<T>> {
        let current_head = self.head.load(Ordering::Relaxed);
        
        // Check if buffer is empty
        if current_head == self.tail.load(Ordering::Acquire) {
            return None;
        }

        // Load the item
        let raw_item = self.buffer[current_head].load(Ordering::Acquire);
        if raw_item.is_null() {
            return None;
        }

        // Clear the slot
        self.buffer[current_head].store(std::ptr::null_mut(), Ordering::Release);
        
        // Update head
        let next_head = (current_head + 1) % self.capacity;
        self.head.store(next_head, Ordering::Release);
        
        Some(unsafe { Box::from_raw(raw_item) })
    }

    /// Get current size (approximate)
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        
        if tail >= head {
            tail - head
        } else {
            self.capacity - head + tail
        }
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Relaxed) == self.tail.load(Ordering::Relaxed)
    }
}

/// Lock-free atomic counter for metrics
pub struct AtomicMetrics {
    pub total_orders: AtomicU64,
    pub successful_orders: AtomicU64,
    pub failed_orders: AtomicU64,
    pub total_latency_ns: AtomicU64,
    pub min_latency_ns: AtomicU64,
    pub max_latency_ns: AtomicU64,
}

impl AtomicMetrics {
    pub fn new() -> Self {
        Self {
            total_orders: AtomicU64::new(0),
            successful_orders: AtomicU64::new(0),
            failed_orders: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            min_latency_ns: AtomicU64::new(u64::MAX),
            max_latency_ns: AtomicU64::new(0),
        }
    }

    pub fn record_order_success(&self, latency_ns: u64) {
        self.total_orders.fetch_add(1, Ordering::Relaxed);
        self.successful_orders.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        
        // Update min latency
        self.min_latency_ns.fetch_min(latency_ns, Ordering::Relaxed);
        
        // Update max latency
        self.max_latency_ns.fetch_max(latency_ns, Ordering::Relaxed);
    }

    pub fn record_order_failure(&self) {
        self.total_orders.fetch_add(1, Ordering::Relaxed);
        self.failed_orders.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_snapshot(&self) -> MetricsSnapshot {
        let total = self.total_orders.load(Ordering::Relaxed);
        let successful = self.successful_orders.load(Ordering::Relaxed);
        let failed = self.failed_orders.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ns.load(Ordering::Relaxed);
        let min_latency = self.min_latency_ns.load(Ordering::Relaxed);
        let max_latency = self.max_latency_ns.load(Ordering::Relaxed);

        MetricsSnapshot {
            total_orders: total,
            successful_orders: successful,
            failed_orders: failed,
            avg_latency_ns: if successful > 0 { total_latency / successful } else { 0 },
            min_latency_ns: if min_latency == u64::MAX { 0 } else { min_latency },
            max_latency_ns: max_latency,
            success_rate: if total > 0 { successful as f64 / total as f64 } else { 0.0 },
        }
    }

    pub fn reset(&self) {
        self.total_orders.store(0, Ordering::Relaxed);
        self.successful_orders.store(0, Ordering::Relaxed);
        self.failed_orders.store(0, Ordering::Relaxed);
        self.total_latency_ns.store(0, Ordering::Relaxed);
        self.min_latency_ns.store(u64::MAX, Ordering::Relaxed);
        self.max_latency_ns.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub total_orders: u64,
    pub successful_orders: u64,
    pub failed_orders: u64,
    pub avg_latency_ns: u64,
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
    pub success_rate: f64,
}

/// Lock-free SPSC (Single Producer Single Consumer) queue for order updates
pub struct SPSCQueue<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T> SPSCQueue<T> {
    pub fn new() -> Self {
        let (sender, receiver) = unbounded();
        Self { sender, receiver }
    }

    /// Send item (non-blocking for unbounded)
    pub fn send(&self, item: T) -> Result<(), T> {
        self.sender.send(item).map_err(|e| e.0)
    }

    /// Try to receive item (non-blocking)
    pub fn try_recv(&self) -> Option<T> {
        self.receiver.try_recv().ok()
    }

    /// Get receiver for async processing
    pub fn receiver(&self) -> &Receiver<T> {
        &self.receiver
    }
}

/// Lock-free timestamp cache for order correlation
pub struct TimestampCache {
    cache: Vec<AtomicU64>,
    index_mask: usize,
}

impl TimestampCache {
    pub fn new(size_power_of_2: usize) -> Self {
        let size = 1 << size_power_of_2;
        let mut cache = Vec::with_capacity(size);
        for _ in 0..size {
            cache.push(AtomicU64::new(0));
        }
        
        Self {
            cache,
            index_mask: size - 1,
        }
    }

    /// Store timestamp for order ID hash
    pub fn store(&self, order_id_hash: u64, timestamp: u64) {
        let index = (order_id_hash as usize) & self.index_mask;
        self.cache[index].store(timestamp, Ordering::Release);
    }

    /// Load timestamp for order ID hash
    pub fn load(&self, order_id_hash: u64) -> Option<u64> {
        let index = (order_id_hash as usize) & self.index_mask;
        let timestamp = self.cache[index].load(Ordering::Acquire);
        if timestamp > 0 {
            Some(timestamp)
        } else {
            None
        }
    }
}

/// Simple hash function for order ID strings
pub fn fast_hash(s: &str) -> u64 {
    let mut hash = 14695981039346656037u64;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash
}

/// Lock-free batch processor for order results
pub struct BatchProcessor {
    buffer: Arc<LockFreeRingBuffer<ExecutionResult>>,
    processor_handle: Option<std::thread::JoinHandle<()>>,
}

impl BatchProcessor {
    pub fn new(capacity: usize, batch_size: usize) -> Self {
        let buffer = Arc::new(LockFreeRingBuffer::new(capacity));
        let buffer_clone = Arc::clone(&buffer);
        
        let processor_handle = std::thread::spawn(move || {
            let mut batch = Vec::with_capacity(batch_size);
            
            loop {
                // Collect batch
                while batch.len() < batch_size {
                    if let Some(result) = buffer_clone.try_pop() {
                        batch.push(*result);
                    } else {
                        break;
                    }
                }
                
                if !batch.is_empty() {
                    // Process batch (placeholder for actual processing)
                    Self::process_batch(&batch);
                    batch.clear();
                }
                
                // Small yield to prevent busy spinning
                std::thread::yield_now();
            }
        });
        
        Self {
            buffer,
            processor_handle: Some(processor_handle),
        }
    }

    pub fn submit_result(&self, result: ExecutionResult) -> Result<(), ExecutionResult> {
        self.buffer.try_push(Box::new(result)).map_err(|boxed| *boxed)
    }

    fn process_batch(results: &[ExecutionResult]) {
        // Placeholder for batch processing logic
        // This could be metrics aggregation, persistence, notifications, etc.
        for result in results {
            // Process each result
            log::debug!("Processed order: {} in {}ns", result.order_id, result.latency_ns);
        }
    }
}
