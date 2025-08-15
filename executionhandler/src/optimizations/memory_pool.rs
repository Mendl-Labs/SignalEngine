use std::alloc::{alloc, dealloc, Layout};
use std::collections::VecDeque;
use std::ptr::NonNull;
use std::sync::Mutex;
use crate::core::types::{ExchangeOrder, PooledOrder};

/// High-performance memory pool for order objects to eliminate allocations
pub struct OrderPool {
    pool: Mutex<VecDeque<NonNull<ExchangeOrder>>>,
    layout: Layout,
    capacity: usize,
    allocated: usize,
}

impl OrderPool {
    pub fn new(capacity: usize) -> Self {
        let layout = Layout::new::<ExchangeOrder>();
        let pool = Mutex::new(VecDeque::with_capacity(capacity));
        
        Self {
            pool,
            layout,
            capacity,
            allocated: 0,
        }
    }

    /// Pre-allocate orders for the hot path
    pub fn preallocate(&mut self, count: usize) {
        let mut pool = self.pool.lock().unwrap();
        
        for _ in 0..count.min(self.capacity - self.allocated) {
            unsafe {
                let ptr = alloc(self.layout) as *mut ExchangeOrder;
                if !ptr.is_null() {
                    pool.push_back(NonNull::new_unchecked(ptr));
                    self.allocated += 1;
                }
            }
        }
    }

    /// Get a pooled order object (zero allocation in hot path)
    pub fn get(&self) -> PooledOrder {
        let mut pool = self.pool.lock().unwrap();
        
        if let Some(mut ptr) = pool.pop_front() {
            // Use pre-allocated object
            unsafe {
                let order_ref = ptr.as_mut();
                // Reset the order to default state
                std::ptr::write(order_ref, ExchangeOrder::default());
                
                PooledOrder {
                    inner: std::ptr::read(order_ref),
                    pool_id: ptr.as_ptr() as usize,
                }
            }
        } else {
            // Fallback to heap allocation if pool is empty
            PooledOrder {
                inner: ExchangeOrder::default(),
                pool_id: 0, // Indicates heap allocation
            }
        }
    }

    /// Return order object to pool for reuse
    pub fn return_order(&self, order: PooledOrder) {
        if order.pool_id != 0 {
            // Return to pool
            let mut pool = self.pool.lock().unwrap();
            unsafe {
                let mut ptr = NonNull::new_unchecked(order.pool_id as *mut ExchangeOrder);
                std::ptr::write(ptr.as_mut(), order.inner);
                pool.push_back(ptr);
            }
        }
        // If pool_id is 0, object was heap allocated and will be dropped normally
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let pool = self.pool.lock().unwrap();
        PoolStats {
            capacity: self.capacity,
            allocated: self.allocated,
            available: pool.len(),
            utilization: (self.allocated - pool.len()) as f64 / self.allocated as f64,
        }
    }
}

impl Drop for OrderPool {
    fn drop(&mut self) {
        let mut pool = self.pool.lock().unwrap();
        while let Some(ptr) = pool.pop_front() {
            unsafe {
                dealloc(ptr.as_ptr() as *mut u8, self.layout);
            }
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub capacity: usize,
    pub allocated: usize,
    pub available: usize,
    pub utilization: f64,
}

/// Default implementation for ExchangeOrder to support pooling
impl Default for ExchangeOrder {
    fn default() -> Self {
        use crate::core::types::{OrderSide, OrderType, TimeInForce};
        use std::collections::HashMap;
        
        Self {
            symbol: String::new(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            quantity: 0.0,
            price: None,
            time_in_force: TimeInForce::GoodTillCancelled,
            client_order_id: String::new(),
            metadata: HashMap::new(),
        }
    }
}

/// Thread-local memory pool for ultimate performance
thread_local! {
    static THREAD_LOCAL_POOL: std::cell::RefCell<OrderPool> = std::cell::RefCell::new(OrderPool::new(100));
}

/// Get order from thread-local pool (fastest path)
pub fn get_thread_local_order() -> PooledOrder {
    THREAD_LOCAL_POOL.with(|pool| pool.borrow().get())
}

/// Return order to thread-local pool
pub fn return_thread_local_order(order: PooledOrder) {
    THREAD_LOCAL_POOL.with(|pool| pool.borrow().return_order(order));
}

/// Preallocate thread-local pool
pub fn preallocate_thread_local(count: usize) {
    THREAD_LOCAL_POOL.with(|pool| pool.borrow_mut().preallocate(count));
}

/// Arena allocator for batch operations
pub struct ArenaAllocator {
    buffer: Vec<u8>,
    offset: usize,
}

impl ArenaAllocator {
    pub fn new(size: usize) -> Self {
        Self {
            buffer: vec![0; size],
            offset: 0,
        }
    }

    pub fn allocate<T>(&mut self) -> Option<&mut T> {
        let layout = Layout::new::<T>();
        let aligned_offset = (self.offset + layout.align() - 1) & !(layout.align() - 1);
        
        if aligned_offset + layout.size() <= self.buffer.len() {
            let ptr = unsafe { self.buffer.as_mut_ptr().add(aligned_offset) as *mut T };
            self.offset = aligned_offset + layout.size();
            Some(unsafe { &mut *ptr })
        } else {
            None
        }
    }

    pub fn reset(&mut self) {
        self.offset = 0;
    }

    pub fn utilization(&self) -> f64 {
        self.offset as f64 / self.buffer.len() as f64
    }
}
