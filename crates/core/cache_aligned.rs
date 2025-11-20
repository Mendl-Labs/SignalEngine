// Cache-Aligned Atomic Structures for False Sharing Prevention
// 
// On modern CPUs, cache lines are 64 bytes. When multiple threads access
// different atomic variables on the same cache line, false sharing occurs,
// causing significant performance degradation (up to 40x slower).
//
// This module provides cache-aligned atomic types that guarantee each
// atomic variable occupies its own cache line.

use std::sync::atomic::{AtomicU64, AtomicU32, AtomicBool, Ordering};

/// Cache line size on x86_64 and ARM64
const CACHE_LINE_SIZE: usize = 64;

/// Cache-aligned AtomicU64 (prevents false sharing)
#[repr(align(64))]
#[derive(Debug)]
pub struct CacheAlignedAtomicU64 {
    value: AtomicU64,
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<AtomicU64>()],
}

impl CacheAlignedAtomicU64 {
    /// Create new cache-aligned atomic with initial value
    #[inline(always)]
    pub const fn new(val: u64) -> Self {
        Self {
            value: AtomicU64::new(val),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<AtomicU64>()],
        }
    }
    
    /// Load value
    #[inline(always)]
    pub fn load(&self, order: Ordering) -> u64 {
        self.value.load(order)
    }
    
    /// Store value
    #[inline(always)]
    pub fn store(&self, val: u64, order: Ordering) {
        self.value.store(val, order)
    }
    
    /// Fetch and add
    #[inline(always)]
    pub fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
        self.value.fetch_add(val, order)
    }
    
    /// Fetch and sub
    #[inline(always)]
    pub fn fetch_sub(&self, val: u64, order: Ordering) -> u64 {
        self.value.fetch_sub(val, order)
    }
    
    /// Compare and swap
    #[inline(always)]
    pub fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.value.compare_exchange(current, new, success, failure)
    }
    
    /// Swap
    #[inline(always)]
    pub fn swap(&self, val: u64, order: Ordering) -> u64 {
        self.value.swap(val, order)
    }
}

impl Default for CacheAlignedAtomicU64 {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Cache-aligned AtomicU32
#[repr(align(64))]
#[derive(Debug)]
pub struct CacheAlignedAtomicU32 {
    value: AtomicU32,
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<AtomicU32>()],
}

impl CacheAlignedAtomicU32 {
    #[inline(always)]
    pub const fn new(val: u32) -> Self {
        Self {
            value: AtomicU32::new(val),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<AtomicU32>()],
        }
    }
    
    #[inline(always)]
    pub fn load(&self, order: Ordering) -> u32 {
        self.value.load(order)
    }
    
    #[inline(always)]
    pub fn store(&self, val: u32, order: Ordering) {
        self.value.store(val, order)
    }
    
    #[inline(always)]
    pub fn fetch_add(&self, val: u32, order: Ordering) -> u32 {
        self.value.fetch_add(val, order)
    }
    
    #[inline(always)]
    pub fn fetch_sub(&self, val: u32, order: Ordering) -> u32 {
        self.value.fetch_sub(val, order)
    }
}

impl Default for CacheAlignedAtomicU32 {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Cache-aligned AtomicBool
#[repr(align(64))]
#[derive(Debug)]
pub struct CacheAlignedAtomicBool {
    value: AtomicBool,
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<AtomicBool>()],
}

impl CacheAlignedAtomicBool {
    #[inline(always)]
    pub const fn new(val: bool) -> Self {
        Self {
            value: AtomicBool::new(val),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<AtomicBool>()],
        }
    }
    
    #[inline(always)]
    pub fn load(&self, order: Ordering) -> bool {
        self.value.load(order)
    }
    
    #[inline(always)]
    pub fn store(&self, val: bool, order: Ordering) {
        self.value.store(val, order)
    }
    
    #[inline(always)]
    pub fn swap(&self, val: bool, order: Ordering) -> bool {
        self.value.swap(val, order)
    }
    
    #[inline(always)]
    pub fn compare_exchange(
        &self,
        current: bool,
        new: bool,
        success: Ordering,
        failure: Ordering,
    ) -> Result<bool, bool> {
        self.value.compare_exchange(current, new, success, failure)
    }
}

impl Default for CacheAlignedAtomicBool {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Atomic metrics structure with cache-aligned fields
#[repr(C)]
pub struct AtomicMetrics {
    pub signal_count: CacheAlignedAtomicU64,
    pub error_count: CacheAlignedAtomicU64,
    pub total_latency_ns: CacheAlignedAtomicU64,
    pub min_latency_ns: CacheAlignedAtomicU64,
    pub max_latency_ns: CacheAlignedAtomicU64,
}

impl AtomicMetrics {
    pub fn new() -> Self {
        Self {
            signal_count: CacheAlignedAtomicU64::new(0),
            error_count: CacheAlignedAtomicU64::new(0),
            total_latency_ns: CacheAlignedAtomicU64::new(0),
            min_latency_ns: CacheAlignedAtomicU64::new(u64::MAX),
            max_latency_ns: CacheAlignedAtomicU64::new(0),
        }
    }
    
    /// Record a signal with latency
    #[inline(always)]
    pub fn record_signal(&self, latency_ns: u64) {
        self.signal_count.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        
        // Update min latency
        let mut current_min = self.min_latency_ns.load(Ordering::Relaxed);
        while latency_ns < current_min {
            match self.min_latency_ns.compare_exchange(
                current_min,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }
        
        // Update max latency
        let mut current_max = self.max_latency_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.max_latency_ns.compare_exchange(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }
    
    /// Get average latency
    #[inline(always)]
    pub fn avg_latency_ns(&self) -> u64 {
        let count = self.signal_count.load(Ordering::Relaxed);
        if count == 0 {
            0
        } else {
            self.total_latency_ns.load(Ordering::Relaxed) / count
        }
    }
}

impl Default for AtomicMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;
    
    #[test]
    fn test_cache_alignment() {
        // Verify all types are cache-aligned (64 bytes)
        assert_eq!(mem::size_of::<CacheAlignedAtomicU64>(), 64);
        assert_eq!(mem::size_of::<CacheAlignedAtomicU32>(), 64);
        assert_eq!(mem::size_of::<CacheAlignedAtomicBool>(), 64);
        
        // Verify alignment
        assert_eq!(mem::align_of::<CacheAlignedAtomicU64>(), 64);
        assert_eq!(mem::align_of::<CacheAlignedAtomicU32>(), 64);
        assert_eq!(mem::align_of::<CacheAlignedAtomicBool>(), 64);
    }
    
    #[test]
    fn test_atomic_operations() {
        let counter = CacheAlignedAtomicU64::new(0);
        
        counter.store(100, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 100);
        
        counter.fetch_add(50, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 150);
        
        let old = counter.swap(200, Ordering::Relaxed);
        assert_eq!(old, 150);
        assert_eq!(counter.load(Ordering::Relaxed), 200);
    }
    
    #[test]
    fn test_metrics() {
        let metrics = AtomicMetrics::new();
        
        metrics.record_signal(100);
        metrics.record_signal(200);
        metrics.record_signal(150);
        
        assert_eq!(metrics.signal_count.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.min_latency_ns.load(Ordering::Relaxed), 100);
        assert_eq!(metrics.max_latency_ns.load(Ordering::Relaxed), 200);
        assert_eq!(metrics.avg_latency_ns(), 150);
    }
    
    #[test]
    fn test_false_sharing_prevention() {
        use std::thread;
        use std::sync::Arc;
        
        let metrics = Arc::new(AtomicMetrics::new());
        
        // Spawn multiple threads that update different counters
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let metrics_clone = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..10000 {
                        if i % 2 == 0 {
                            metrics_clone.signal_count.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics_clone.error_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Each counter should have been incremented 20000 times
        assert_eq!(metrics.signal_count.load(Ordering::Relaxed), 20000);
        assert_eq!(metrics.error_count.load(Ordering::Relaxed), 20000);
    }
}
