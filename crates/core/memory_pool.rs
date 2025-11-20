// Memory Pool for Zero-Allocation Signal Processing
//
// Provides object pooling for Signal structures to eliminate allocation
// overhead in hot paths. Uses lock-free stack for thread-safe access.

use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::ptr;

/// Lock-free memory pool for reusable objects
pub struct MemoryPool<T> {
    head: AtomicPtr<Node<T>>,
    capacity: usize,
    allocated: AtomicUsize,
}

struct Node<T> {
    value: T,
    next: *mut Node<T>,
}

impl<T: Default> MemoryPool<T> {
    /// Create a new memory pool with specified capacity
    pub fn new(capacity: usize) -> Self {
        let pool = Self {
            head: AtomicPtr::new(ptr::null_mut()),
            capacity,
            allocated: AtomicUsize::new(0),
        };
        
        // Pre-allocate objects to avoid allocation in hot path
        for _ in 0..capacity {
            if let Some(obj) = pool.create_object() {
                pool.return_object(obj);
            }
        }
        
        pool
    }
    
    /// Acquire an object from the pool (lock-free)
    #[inline]
    pub fn acquire(&self) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            
            if head.is_null() {
                // Pool is empty, try to create new object if under capacity
                return self.create_object();
            }
            
            // SAFETY: We've checked head is not null
            let next = unsafe { (*head).next };
            
            // Try to atomically update head to next
            if self.head.compare_exchange(
                head,
                next,
                Ordering::Release,
                Ordering::Acquire,
            ).is_ok() {
                // Successfully removed from pool
                // SAFETY: We own this node now
                let value = unsafe {
                    let node = Box::from_raw(head);
                    node.value
                };
                return Some(value);
            }
            // CAS failed, retry
        }
    }
    
    /// Return an object to the pool (lock-free)
    #[inline]
    pub fn release(&self, value: T) {
        self.return_object(value);
    }
    
    /// Create a new object if under capacity
    fn create_object(&self) -> Option<T> {
        let current = self.allocated.load(Ordering::Relaxed);
        if current >= self.capacity {
            return None;
        }
        
        // Try to increment allocated count
        if self.allocated.compare_exchange(
            current,
            current + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ).is_ok() {
            Some(T::default())
        } else {
            None
        }
    }
    
    /// Return object to pool
    fn return_object(&self, value: T) {
        let node = Box::into_raw(Box::new(Node {
            value,
            next: ptr::null_mut(),
        }));
        
        loop {
            let head = self.head.load(Ordering::Acquire);
            // SAFETY: We own node
            unsafe { (*node).next = head };
            
            if self.head.compare_exchange(
                head,
                node,
                Ordering::Release,
                Ordering::Acquire,
            ).is_ok() {
                break;
            }
        }
    }
    
    /// Get current pool statistics
    pub fn stats(&self) -> PoolStats {
        let allocated = self.allocated.load(Ordering::Relaxed);
        let mut available = 0;
        
        let mut current = self.head.load(Ordering::Acquire);
        while !current.is_null() {
            available += 1;
            // SAFETY: We're just reading, not modifying
            current = unsafe { (*current).next };
        }
        
        PoolStats {
            capacity: self.capacity,
            allocated,
            available,
            in_use: allocated.saturating_sub(available),
        }
    }
}

impl<T> Drop for MemoryPool<T> {
    fn drop(&mut self) {
        let mut current = self.head.load(Ordering::Acquire);
        while !current.is_null() {
            // SAFETY: We're dropping the pool, we own all nodes
            let node = unsafe { Box::from_raw(current) };
            current = node.next;
        }
    }
}

unsafe impl<T: Send> Send for MemoryPool<T> {}
unsafe impl<T: Send> Sync for MemoryPool<T> {}

/// Memory pool statistics
#[derive(Debug, Clone, Copy)]
pub struct PoolStats {
    pub capacity: usize,
    pub allocated: usize,
    pub available: usize,
    pub in_use: usize,
}

/// RAII guard for automatic object return to pool
pub struct PoolGuard<'a, T: Default> {
    value: Option<T>,
    pool: &'a MemoryPool<T>,
}

impl<'a, T: Default> PoolGuard<'a, T> {
    pub fn new(value: T, pool: &'a MemoryPool<T>) -> Self {
        Self {
            value: Some(value),
            pool,
        }
    }
    
    /// Get reference to the value
    #[inline]
    pub fn get(&self) -> &T {
        self.value.as_ref().unwrap()
    }
    
    /// Get mutable reference to the value
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        self.value.as_mut().unwrap()
    }
}

impl<'a, T: Default> Drop for PoolGuard<'a, T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            self.pool.return_object(value);
        }
    }
}

impl<'a, T: Default> std::ops::Deref for PoolGuard<'a, T> {
    type Target = T;
    
    fn deref(&self) -> &T {
        self.get()
    }
}

impl<'a, T: Default> std::ops::DerefMut for PoolGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.get_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_pool_acquire_release() {
        let pool = MemoryPool::<Vec<u8>>::new(10);
        
        let mut obj = pool.acquire().expect("Should acquire from pool");
        obj.push(42);
        
        pool.release(obj);
        
        let stats = pool.stats();
        assert_eq!(stats.capacity, 10);
        assert!(stats.available > 0);
    }
    
    #[test]
    fn test_pool_capacity_limit() {
        let pool = MemoryPool::<Vec<u8>>::new(5);
        
        let mut objects = Vec::new();
        for _ in 0..5 {
            objects.push(pool.acquire().expect("Should acquire"));
        }
        
        // Should fail - pool at capacity
        assert!(pool.acquire().is_none());
        
        // Release one
        pool.release(objects.pop().unwrap());
        
        // Should succeed now
        assert!(pool.acquire().is_some());
    }
    
    #[test]
    fn test_pool_guard() {
        let pool = MemoryPool::<Vec<u8>>::new(10);
        
        {
            let mut guard = PoolGuard::new(Vec::new(), &pool);
            guard.push(1);
            guard.push(2);
            assert_eq!(guard.len(), 2);
        } // guard dropped, object returned to pool
        
        let stats = pool.stats();
        assert!(stats.available > 0);
    }
    
    #[test]
    fn test_concurrent_access() {
        use std::thread;
        use std::sync::Arc;
        
        let pool = Arc::new(MemoryPool::<Vec<u8>>::new(100));
        
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let pool_clone = Arc::clone(&pool);
                thread::spawn(move || {
                    for _ in 0..100 {
                        if let Some(mut obj) = pool_clone.acquire() {
                            obj.push(1);
                            pool_clone.release(obj);
                        }
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let stats = pool.stats();
        assert_eq!(stats.allocated, stats.available);
    }
}
