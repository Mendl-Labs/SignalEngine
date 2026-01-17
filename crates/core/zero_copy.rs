// Zero-Copy Message Passing for Ultra-Low Latency
//
// Eliminates memory copies by using Arc-based shared ownership
// and custom arena allocators for signal processing.

use std::sync::Arc;
use std::alloc::{alloc, dealloc, Layout};
use std::ptr;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Zero-copy signal wrapper using Arc
/// 
/// Signals are shared across threads without copying data.
/// Only the Arc pointer is cloned (8-byte atomic increment).
#[derive(Clone)]
pub struct ZeroCopySignal<T> {
    inner: Arc<T>,
}

impl<T> ZeroCopySignal<T> {
    /// Create new zero-copy signal
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            inner: Arc::new(data),
        }
    }
    
    /// Get reference to inner data (zero-copy)
    #[inline]
    pub fn get(&self) -> &T {
        &self.inner
    }
    
    /// Get Arc clone (only increments ref count, no data copy)
    #[inline]
    pub fn clone_arc(&self) -> Arc<T> {
        Arc::clone(&self.inner)
    }
    
    /// Check if this is the only reference (for optimization)
    #[inline]
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }
    
    /// Get strong reference count
    #[inline]
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }
}

impl<T> AsRef<T> for ZeroCopySignal<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T> std::ops::Deref for ZeroCopySignal<T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Arena allocator for batch signal processing
/// 
/// Allocates signals in contiguous memory blocks, improving
/// cache locality and reducing allocation overhead.
pub struct SignalArena {
    buffer: *mut u8,
    capacity: usize,
    offset: AtomicUsize,
    layout: Layout,
}

/// Error type for arena allocation failures
#[derive(Debug, Clone)]
pub struct ArenaAllocationError {
    pub capacity: usize,
    pub message: String,
}

impl std::fmt::Display for ArenaAllocationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Arena allocation failed for {} bytes: {}", self.capacity, self.message)
    }
}

impl std::error::Error for ArenaAllocationError {}

impl SignalArena {
    /// Create new signal arena with specified capacity (bytes)
    /// Returns error if allocation fails instead of panicking
    pub fn new(capacity: usize) -> Result<Self, ArenaAllocationError> {
        let layout = Layout::from_size_align(capacity, 64)
            .map_err(|e| ArenaAllocationError {
                capacity,
                message: format!("Invalid layout: {}", e),
            })?;
        
        let buffer = unsafe { alloc(layout) };
        if buffer.is_null() {
            return Err(ArenaAllocationError {
                capacity,
                message: "Memory allocation returned null".to_string(),
            });
        }
        
        Ok(Self {
            buffer,
            capacity,
            offset: AtomicUsize::new(0),
            layout,
        })
    }
    
    /// Allocate space for type T in arena (returns None if full)
    #[inline]
    pub fn allocate<T>(&self) -> Option<*mut T> {
        let size = mem::size_of::<T>();
        let align = mem::align_of::<T>();
        
        let mut offset = self.offset.load(Ordering::Relaxed);
        
        loop {
            // Align offset
            let aligned = (offset + align - 1) & !(align - 1);
            let new_offset = aligned + size;
            
            if new_offset > self.capacity {
                return None; // Arena full
            }
            
            match self.offset.compare_exchange_weak(
                offset,
                new_offset,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let ptr = unsafe { self.buffer.add(aligned) as *mut T };
                    return Some(ptr);
                }
                Err(current) => offset = current,
            }
        }
    }
    
    /// Reset arena (invalidates all allocations)
    #[inline]
    pub fn reset(&self) {
        self.offset.store(0, Ordering::Relaxed);
    }
    
    /// Get bytes used
    #[inline]
    pub fn used(&self) -> usize {
        self.offset.load(Ordering::Relaxed)
    }
    
    /// Get bytes available
    #[inline]
    pub fn available(&self) -> usize {
        self.capacity.saturating_sub(self.used())
    }
    
    /// Get capacity utilization (0.0 - 1.0)
    #[inline]
    pub fn utilization(&self) -> f64 {
        self.used() as f64 / self.capacity as f64
    }
}

impl Drop for SignalArena {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.buffer, self.layout);
        }
    }
}

unsafe impl Send for SignalArena {}
unsafe impl Sync for SignalArena {}

/// Zero-copy message channel using ring buffer
/// 
/// Fixed-size ring buffer for passing messages between threads
/// without allocations or copies (uses references).
pub struct ZeroCopyChannel<T> {
    buffer: Vec<Option<Arc<T>>>,
    capacity: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl<T> ZeroCopyChannel<T> {
    /// Create new zero-copy channel with capacity
    pub fn new(capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(None);
        }
        
        Self {
            buffer,
            capacity,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }
    
    /// Send message (returns false if full)
    #[inline]
    pub fn send(&self, value: Arc<T>) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);
        let next_tail = (tail + 1) % self.capacity;
        
        if next_tail == self.head.load(Ordering::Acquire) {
            return false; // Full
        }
        
        // Safe because we checked capacity
        unsafe {
            let slot = self.buffer.as_ptr().add(tail) as *mut Option<Arc<T>>;
            ptr::write(slot, Some(value));
        }
        
        self.tail.store(next_tail, Ordering::Release);
        true
    }
    
    /// Receive message (returns None if empty)
    #[inline]
    pub fn recv(&self) -> Option<Arc<T>> {
        let head = self.head.load(Ordering::Relaxed);
        
        if head == self.tail.load(Ordering::Acquire) {
            return None; // Empty
        }
        
        let value = unsafe {
            let slot = self.buffer.as_ptr().add(head) as *mut Option<Arc<T>>;
            ptr::replace(slot, None)
        };
        
        let next_head = (head + 1) % self.capacity;
        self.head.store(next_head, Ordering::Release);
        
        value
    }
    
    /// Check if channel is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }
    
    /// Check if channel is full
    #[inline]
    pub fn is_full(&self) -> bool {
        let tail = self.tail.load(Ordering::Acquire);
        let next_tail = (tail + 1) % self.capacity;
        next_tail == self.head.load(Ordering::Acquire)
    }
    
    /// Get current size
    #[inline]
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        
        if tail >= head {
            tail - head
        } else {
            self.capacity - head + tail
        }
    }
}

unsafe impl<T: Send> Send for ZeroCopyChannel<T> {}
unsafe impl<T: Send> Sync for ZeroCopyChannel<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    
    #[derive(Debug, Clone, PartialEq)]
    struct TestSignal {
        symbol: String,
        price: f64,
        volume: u64,
    }
    
    #[test]
    fn test_zero_copy_signal() {
        let signal = ZeroCopySignal::new(TestSignal {
            symbol: "BTC".to_string(),
            price: 50000.0,
            volume: 100,
        });
        
        assert!(signal.is_unique());
        assert_eq!(signal.ref_count(), 1);
        
        let clone = signal.clone();
        assert!(!signal.is_unique());
        assert_eq!(signal.ref_count(), 2);
        
        assert_eq!(clone.symbol, "BTC");
        assert_eq!(clone.price, 50000.0);
    }
    
    #[test]
    fn test_signal_arena() {
        let arena = SignalArena::new(1024).expect("Failed to create arena");
        
        // Allocate some u64s
        let ptr1 = arena.allocate::<u64>().expect("Allocation failed");
        let ptr2 = arena.allocate::<u64>().expect("Allocation failed");
        
        unsafe {
            *ptr1 = 42;
            *ptr2 = 84;
            assert_eq!(*ptr1, 42);
            assert_eq!(*ptr2, 84);
        }
        
        assert!(arena.used() >= 16); // At least 2 * size_of::<u64>()
        assert!(arena.utilization() < 0.5);
        
        arena.reset();
        assert_eq!(arena.used(), 0);
    }
    
    #[test]
    fn test_signal_arena_full() {
        let arena = SignalArena::new(64).expect("Failed to create arena");
        
        // Fill arena
        let mut ptrs = Vec::new();
        while let Some(ptr) = arena.allocate::<u64>() {
            ptrs.push(ptr);
        }
        
        assert!(ptrs.len() >= 7); // At least 64/8 = 8, but alignment may reduce
        assert!(arena.allocate::<u64>().is_none());
    }
    
    #[test]
    fn test_zero_copy_channel() {
        let channel = ZeroCopyChannel::new(10);
        
        let signal = Arc::new(TestSignal {
            symbol: "ETH".to_string(),
            price: 3000.0,
            volume: 50,
        });
        
        assert!(channel.send(signal.clone()));
        assert_eq!(channel.len(), 1);
        
        let received = channel.recv().expect("Should receive");
        assert_eq!(received.symbol, "ETH");
        assert_eq!(channel.len(), 0);
    }
    
    #[test]
    fn test_zero_copy_channel_full() {
        let channel = ZeroCopyChannel::new(3);
        
        let s1 = Arc::new(TestSignal {
            symbol: "BTC".to_string(),
            price: 50000.0,
            volume: 100,
        });
        
        assert!(channel.send(s1.clone()));
        assert!(channel.send(s1.clone()));
        assert!(!channel.send(s1.clone())); // Full (capacity - 1)
        
        assert_eq!(channel.len(), 2);
        assert!(channel.is_full());
    }
    
    #[test]
    fn test_zero_copy_channel_concurrent() {
        let channel = Arc::new(ZeroCopyChannel::new(1000));
        let channel_clone = Arc::clone(&channel);
        
        // Producer thread
        let producer = thread::spawn(move || {
            for i in 0..500 {
                let signal = Arc::new(TestSignal {
                    symbol: format!("SYM{}", i),
                    price: 1000.0 + i as f64,
                    volume: i as u64,
                });
                
                while !channel_clone.send(signal.clone()) {
                    thread::yield_now();
                }
            }
        });
        
        // Consumer thread
        let consumer = thread::spawn(move || {
            let mut count = 0;
            while count < 500 {
                if let Some(_signal) = channel.recv() {
                    count += 1;
                }
            }
            count
        });
        
        producer.join().unwrap();
        let received = consumer.join().unwrap();
        assert_eq!(received, 500);
    }
}
