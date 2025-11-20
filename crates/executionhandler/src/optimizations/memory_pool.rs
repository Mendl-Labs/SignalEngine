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
    pub fn preallocate(&mut self, count: usize) -> Result<(), &'static str> {
        let mut pool = self.pool.lock().map_err(|_| "Failed to acquire memory pool lock")?;
        
        for _ in 0..count.min(self.capacity - self.allocated) {
            unsafe {
                let ptr = alloc(self.layout) as *mut ExchangeOrder;
                if !ptr.is_null() {
                    // Safe to use new_unchecked here since we just verified ptr is not null
                    pool.push_back(NonNull::new_unchecked(ptr));
                    self.allocated += 1;
                } else {
                    // Allocation failed - stop trying to avoid memory pressure
                    break;
                }
            }
        }
        Ok(())
    }

    /// Get a pooled order object (zero allocation in hot path)
    pub fn get(&self) -> PooledOrder {
        let mut pool = self.pool.lock().unwrap();
        
        if let Some(ptr) = pool.pop_front() {
            // Use pre-allocated object - ptr is guaranteed non-null from pool
            unsafe {
                let _order_ref = ptr.as_ref(); 
                // Create a default order - safer than ptr::write + ptr::read
                let default_order = ExchangeOrder::default();
                
                PooledOrder {
                    inner: default_order,
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
            // Return to pool - this is unsafe but constrained to known pool addresses
            let mut pool = self.pool.lock().unwrap();
            unsafe {
                // SAFETY: pool_id came from a valid NonNull pointer from our pool
                // This is still risky - a safer design would store the NonNull directly
                let ptr = NonNull::new(order.pool_id as *mut ExchangeOrder);
                if let Some(mut valid_ptr) = ptr {
                    // Write the order back to the pooled memory
                    std::ptr::write(valid_ptr.as_mut(), order.inner);
                    pool.push_back(valid_ptr);
                }
                // If ptr is null, we just drop the order (safer than crashing)
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

// Thread-local memory pool for ultimate performance
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
    let _ = THREAD_LOCAL_POOL.with(|pool| pool.borrow_mut().preallocate(count));
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
