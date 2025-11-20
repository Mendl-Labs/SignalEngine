// Memory Ordering and Barrier Optimizations
//
// Provides fine-grained control over memory ordering for
// maximum performance in lock-free algorithms.

use std::sync::atomic::{compiler_fence, fence, Ordering};
use std::arch::asm;

/// Memory barrier utilities for precise ordering control
pub struct MemoryBarrier;

impl MemoryBarrier {
    /// Full memory barrier (MFENCE on x86)
    /// 
    /// Ensures all loads/stores before are visible before any after.
    /// Use sparingly - expensive operation (~20-30 cycles).
    #[inline(always)]
    pub fn full() {
        fence(Ordering::SeqCst);
    }
    
    /// Store barrier (SFENCE on x86)
    /// 
    /// Ensures all stores before are visible before any after.
    /// Cheaper than full fence (~5-10 cycles).
    #[inline(always)]
    pub fn store() {
        fence(Ordering::Release);
    }
    
    /// Load barrier (LFENCE on x86)
    /// 
    /// Ensures all loads before complete before any after.
    /// Cheapest fence (~3-5 cycles).
    #[inline(always)]
    pub fn load() {
        fence(Ordering::Acquire);
    }
    
    /// Compiler barrier (no CPU instruction)
    /// 
    /// Prevents compiler reordering, no runtime cost.
    /// Use when CPU ordering is sufficient (x86 strong model).
    #[inline(always)]
    pub fn compiler_only() {
        compiler_fence(Ordering::SeqCst);
    }
    
    /// Store-load barrier
    /// 
    /// Prevents store-load reordering (most expensive on x86).
    #[inline(always)]
    pub fn store_load() {
        fence(Ordering::SeqCst);
    }
}

/// CPU pause instruction for spin loops
/// 
/// Improves performance and power efficiency in spin-wait scenarios.
#[inline(always)]
pub fn cpu_pause() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        asm!("pause", options(nomem, nostack, preserves_flags));
    }
    
    #[cfg(target_arch = "aarch64")]
    unsafe {
        asm!("yield", options(nomem, nostack, preserves_flags));
    }
    
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    std::hint::spin_loop();
}

/// Optimized spin-wait with exponential backoff
pub struct SpinWait {
    count: u32,
}

impl SpinWait {
    /// Create new spin-wait
    pub const fn new() -> Self {
        Self { count: 0 }
    }
    
    /// Spin once with adaptive backoff
    #[inline]
    pub fn spin(&mut self) {
        for _ in 0..(1 << self.count.min(10)) {
            cpu_pause();
        }
        self.count = self.count.saturating_add(1);
    }
    
    /// Reset backoff counter
    #[inline]
    pub fn reset(&mut self) {
        self.count = 0;
    }
    
    /// Check if should yield to OS
    #[inline]
    pub fn should_yield(&self) -> bool {
        self.count > 20
    }
}

impl Default for SpinWait {
    fn default() -> Self {
        Self::new()
    }
}

/// Ordering strategy for different use cases
#[derive(Debug, Clone, Copy)]
pub enum OrderingStrategy {
    /// Relaxed - no synchronization (fastest)
    /// Use for: Counters, statistics, non-critical metrics
    Relaxed,
    
    /// Acquire-Release - synchronizes with matching Release/Acquire
    /// Use for: Lock-free data structures, signal passing
    AcquireRelease,
    
    /// Sequential Consistency - strongest guarantee (slowest)
    /// Use for: Critical correctness, rare operations
    SeqCst,
}

impl OrderingStrategy {
    /// Get load ordering
    #[inline]
    pub fn load(&self) -> Ordering {
        match self {
            Self::Relaxed => Ordering::Relaxed,
            Self::AcquireRelease => Ordering::Acquire,
            Self::SeqCst => Ordering::SeqCst,
        }
    }
    
    /// Get store ordering
    #[inline]
    pub fn store(&self) -> Ordering {
        match self {
            Self::Relaxed => Ordering::Relaxed,
            Self::AcquireRelease => Ordering::Release,
            Self::SeqCst => Ordering::SeqCst,
        }
    }
    
    /// Get compare-exchange ordering (success, failure)
    #[inline]
    pub fn compare_exchange(&self) -> (Ordering, Ordering) {
        match self {
            Self::Relaxed => (Ordering::Relaxed, Ordering::Relaxed),
            Self::AcquireRelease => (Ordering::AcqRel, Ordering::Acquire),
            Self::SeqCst => (Ordering::SeqCst, Ordering::SeqCst),
        }
    }
}

/// Cache line prefetch hints
pub struct Prefetch;

impl Prefetch {
    /// Prefetch for read (temporal locality)
    #[inline(always)]
    pub fn read<T>(ptr: *const T) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            asm!(
                "prefetcht0 [{0}]",
                in(reg) ptr,
                options(nostack, preserves_flags)
            );
        }
        
        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = ptr;
        }
    }
    
    /// Prefetch for write (exclusive)
    #[inline(always)]
    pub fn write<T>(ptr: *const T) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            asm!(
                "prefetchw [{0}]",
                in(reg) ptr,
                options(nostack, preserves_flags)
            );
        }
        
        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = ptr;
        }
    }
    
    /// Prefetch non-temporal (no cache pollution)
    #[inline(always)]
    pub fn non_temporal<T>(ptr: *const T) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            asm!(
                "prefetchnta [{0}]",
                in(reg) ptr,
                options(nostack, preserves_flags)
            );
        }
        
        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = ptr;
        }
    }
}

/// False sharing prevention utilities
pub struct CacheLinePadding<T> {
    _pad1: [u8; 64],
    value: T,
    _pad2: [u8; 64],
}

impl<T> CacheLinePadding<T> {
    /// Create new cache-line padded value
    pub fn new(value: T) -> Self {
        Self {
            _pad1: [0; 64],
            value,
            _pad2: [0; 64],
        }
    }
    
    /// Get reference to value
    #[inline]
    pub fn get(&self) -> &T {
        &self.value
    }
    
    /// Get mutable reference to value
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> std::ops::Deref for CacheLinePadding<T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CacheLinePadding<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::sync::Arc;
    
    #[test]
    fn test_memory_barriers() {
        // Just ensure they compile and don't panic
        MemoryBarrier::full();
        MemoryBarrier::store();
        MemoryBarrier::load();
        MemoryBarrier::compiler_only();
        MemoryBarrier::store_load();
    }
    
    #[test]
    fn test_cpu_pause() {
        for _ in 0..100 {
            cpu_pause();
        }
    }
    
    #[test]
    fn test_spin_wait() {
        let mut spin = SpinWait::new();
        
        for i in 0..30 {
            spin.spin();
            if i < 20 {
                assert!(!spin.should_yield());
            } else {
                assert!(spin.should_yield());
            }
        }
        
        spin.reset();
        assert!(!spin.should_yield());
    }
    
    #[test]
    fn test_ordering_strategy() {
        let relaxed = OrderingStrategy::Relaxed;
        assert!(matches!(relaxed.load(), Ordering::Relaxed));
        assert!(matches!(relaxed.store(), Ordering::Relaxed));
        
        let acq_rel = OrderingStrategy::AcquireRelease;
        assert!(matches!(acq_rel.load(), Ordering::Acquire));
        assert!(matches!(acq_rel.store(), Ordering::Release));
        
        let seq_cst = OrderingStrategy::SeqCst;
        assert!(matches!(seq_cst.load(), Ordering::SeqCst));
        assert!(matches!(seq_cst.store(), Ordering::SeqCst));
    }
    
    #[test]
    fn test_prefetch() {
        let data = vec![1u64, 2, 3, 4, 5];
        
        for val in &data {
            Prefetch::read(val as *const u64);
            Prefetch::write(val as *const u64);
            Prefetch::non_temporal(val as *const u64);
        }
    }
    
    #[test]
    fn test_cache_line_padding() {
        let padded = CacheLinePadding::new(42u64);
        assert_eq!(*padded.get(), 42);
        
        let mut padded_mut = CacheLinePadding::new(0u64);
        *padded_mut.get_mut() = 100;
        assert_eq!(*padded_mut, 100);
    }
    
    #[test]
    fn test_concurrent_with_ordering() {
        let counter = Arc::new(AtomicU64::new(0));
        let strategy = OrderingStrategy::AcquireRelease;
        
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let counter_clone = Arc::clone(&counter);
                let strategy_clone = strategy;
                
                thread::spawn(move || {
                    for _ in 0..1000 {
                        counter_clone.fetch_add(1, strategy_clone.store());
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(counter.load(strategy.load()), 10000);
    }
}
