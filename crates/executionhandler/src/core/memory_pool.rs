use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::{Mutex, OnceLock};
use std::collections::VecDeque;

use crate::core::types::{ExchangeOrder, PooledOrder};

/// High-performance memory pool for order objects
pub struct MemoryPool<T> {
    pool: Mutex<VecDeque<NonNull<T>>>,
    layout: Layout,
    capacity: usize,
    allocated: usize,
}

impl<T> MemoryPool<T> {
    pub fn new(capacity: usize) -> Self {
        let layout = Layout::new::<T>();
        let pool = Mutex::new(VecDeque::with_capacity(capacity));
        
        Self {
            pool,
            layout,
            capacity,
            allocated: 0,
        }
    }

    /// Pre-allocate objects for the hot path
    pub fn preallocate(&mut self, count: usize) {
        let mut pool = self.pool.lock().unwrap();
        
        for _ in 0..count.min(self.capacity - self.allocated) {
            unsafe {
                let ptr = alloc(self.layout) as *mut T;
                if !ptr.is_null() {
                    pool.push_back(NonNull::new_unchecked(ptr));
                    self.allocated += 1;
                }
            }
        }
    }

    /// Get an object from the pool (zero allocation in hot path)
    pub fn get(&self) -> Option<NonNull<T>> {
        let mut pool = self.pool.lock().unwrap();
        pool.pop_front()
    }

    /// Return an object to the pool
    pub fn return_object(&self, ptr: NonNull<T>) {
        let mut pool = self.pool.lock().unwrap();
        if pool.len() < self.capacity {
            pool.push_back(ptr);
        } else {
            // Pool is full, deallocate
            unsafe {
                dealloc(ptr.as_ptr() as *mut u8, self.layout);
            }
        }
    }

    /// Get pool statistics
    pub fn stats(&self) -> MemoryPoolStats {
        let pool = self.pool.lock().unwrap();
        MemoryPoolStats {
            capacity: self.capacity,
            allocated: self.allocated,
            available: pool.len(),
            utilization: (self.allocated - pool.len()) as f64 / self.allocated as f64,
        }
    }
}

impl<T> Drop for MemoryPool<T> {
    fn drop(&mut self) {
        let mut pool = self.pool.lock().unwrap();
        while let Some(ptr) = pool.pop_front() {
            unsafe {
                dealloc(ptr.as_ptr() as *mut u8, self.layout);
            }
        }
    }
}

// SAFETY: MemoryPool is thread-safe because:
// 1. All access to the internal pool is protected by a Mutex
// 2. The raw pointers are only accessed while holding the lock
// 3. Each pointer is either in the pool (available) or loaned out (tracked by pool_id)
unsafe impl<T: Send> Send for MemoryPool<T> {}
unsafe impl<T: Send> Sync for MemoryPool<T> {}

/// Memory pool statistics
#[derive(Debug, Clone)]
pub struct MemoryPoolStats {
    pub capacity: usize,
    pub allocated: usize,
    pub available: usize,
    pub utilization: f64,
}

/// Thread-safe global memory pool using OnceLock (safe alternative to static mut)
static ORDER_POOL: OnceLock<Mutex<MemoryPool<ExchangeOrder>>> = OnceLock::new();

/// Initialize the global order pool (thread-safe via OnceLock)
pub fn initialize_order_pool(capacity: usize) {
    let _ = ORDER_POOL.get_or_init(|| Mutex::new(MemoryPool::new(capacity)));
}

/// Get an order from the global pool
pub fn get_pooled_order() -> PooledOrder {
    if let Some(pool_mutex) = ORDER_POOL.get() {
        if let Ok(pool) = pool_mutex.lock() {
            if let Some(mut ptr) = pool.get() {
                unsafe {
                    // Reset the order to default state
                    let order_ref = ptr.as_mut();
                    std::ptr::write(order_ref, ExchangeOrder::default());
                    
                    return PooledOrder {
                        inner: std::ptr::read(order_ref),
                        pool_id: ptr.as_ptr() as usize,
                    };
                }
            }
        }
    }
    
    // Fallback to heap allocation
    PooledOrder {
        inner: ExchangeOrder::default(),
        pool_id: 0, // Indicates heap allocation
    }
}

/// Return order to the global pool
pub fn return_pooled_order(order: PooledOrder) {
    if order.pool_id != 0 {
        if let Some(pool_mutex) = ORDER_POOL.get() {
            if let Ok(pool) = pool_mutex.lock() {
                unsafe {
                    let mut ptr = NonNull::new_unchecked(order.pool_id as *mut ExchangeOrder);
                    std::ptr::write(ptr.as_mut(), order.inner);
                    pool.return_object(ptr);
                }
            }
        }
    }
    // If pool_id is 0, object was heap allocated and will be dropped normally
}

// Thread-local memory pools for maximum performance
thread_local! {
    static THREAD_ORDER_POOL: std::cell::RefCell<MemoryPool<ExchangeOrder>> = 
        std::cell::RefCell::new(MemoryPool::new(100));
}

/// Get order from thread-local pool
pub fn get_thread_local_order() -> PooledOrder {
    THREAD_ORDER_POOL.with(|pool| {
        let pool_ref = pool.borrow_mut();
        if let Some(mut ptr) = pool_ref.get() {
            unsafe {
                let order_ref = ptr.as_mut();
                std::ptr::write(order_ref, ExchangeOrder::default());
                
                PooledOrder {
                    inner: std::ptr::read(order_ref),
                    pool_id: ptr.as_ptr() as usize,
                }
            }
        } else {
            PooledOrder {
                inner: ExchangeOrder::default(),
                pool_id: 0,
            }
        }
    })
}

/// Return order to thread-local pool
pub fn return_thread_local_order(order: PooledOrder) {
    if order.pool_id != 0 {
        THREAD_ORDER_POOL.with(|pool| {
            let pool_ref = pool.borrow();
            unsafe {
                let mut ptr = NonNull::new_unchecked(order.pool_id as *mut ExchangeOrder);
                std::ptr::write(ptr.as_mut(), order.inner);
                pool_ref.return_object(ptr);
            }
        });
    }
}

/// Preallocate thread-local pool
pub fn preallocate_thread_local(count: usize) {
    THREAD_ORDER_POOL.with(|pool| {
        pool.borrow_mut().preallocate(count);
    });
}
