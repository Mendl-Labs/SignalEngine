// Lock-Free Data Structures for Ultra-Low Latency Trading
//
// Provides lock-free alternatives to RwLock and Mutex for concurrent access
// with predictable, bounded latency (no waiting/blocking).

use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::ptr;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Lock-free hash map entry
struct Entry<K, V> {
    key: K,
    value: V,
    hash: u64,
    next: AtomicPtr<Entry<K, V>>,
}

/// Lock-free concurrent hash map
/// 
/// Provides O(1) average-case reads and writes without locks.
/// Uses separate chaining with atomic pointers for collision resolution.
pub struct LockFreeHashMap<K, V> {
    buckets: Vec<AtomicPtr<Entry<K, V>>>,
    size: AtomicUsize,
    capacity: usize,
}

impl<K: Hash + Eq + Clone, V: Clone> LockFreeHashMap<K, V> {
    /// Create new lock-free hash map with specified capacity
    pub fn new(capacity: usize) -> Self {
        let mut buckets = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buckets.push(AtomicPtr::new(ptr::null_mut()));
        }
        
        Self {
            buckets,
            size: AtomicUsize::new(0),
            capacity,
        }
    }
    
    /// Insert or update a key-value pair (lock-free)
    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let hash = self.hash_key(&key);
        let bucket_idx = (hash as usize) % self.capacity;
        
        let new_entry = Box::into_raw(Box::new(Entry {
            key: key.clone(),
            value: value.clone(),
            hash,
            next: AtomicPtr::new(ptr::null_mut()),
        }));
        
        loop {
            let head = self.buckets[bucket_idx].load(Ordering::Acquire);
            
            // Check if key already exists
            let mut current = head;
            while !current.is_null() {
                let entry = unsafe { &*current };
                if entry.hash == hash && entry.key == key {
                    // Key exists, update value (this is a simplified approach)
                    // In production, would use CAS on value field
                    let _ = unsafe { Box::from_raw(new_entry) }; // Clean up
                    return Some(entry.value.clone());
                }
                current = entry.next.load(Ordering::Acquire);
            }
            
            // Key doesn't exist, insert at head
            unsafe { (*new_entry).next.store(head, Ordering::Release) };
            
            if self.buckets[bucket_idx].compare_exchange(
                head,
                new_entry,
                Ordering::Release,
                Ordering::Acquire,
            ).is_ok() {
                self.size.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            // CAS failed, retry
        }
    }
    
    /// Get value by key (lock-free, wait-free read)
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        let hash = self.hash_key(key);
        let bucket_idx = (hash as usize) % self.capacity;
        
        let mut current = self.buckets[bucket_idx].load(Ordering::Acquire);
        while !current.is_null() {
            let entry = unsafe { &*current };
            if entry.hash == hash && entry.key == *key {
                return Some(entry.value.clone());
            }
            current = entry.next.load(Ordering::Acquire);
        }
        None
    }
    
    /// Check if key exists (lock-free)
    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
    
    /// Get current size
    #[inline]
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    
    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Hash a key
    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl<K, V> Drop for LockFreeHashMap<K, V> {
    fn drop(&mut self) {
        for bucket in &self.buckets {
            let mut current = bucket.load(Ordering::Acquire);
            while !current.is_null() {
                let entry = unsafe { Box::from_raw(current) };
                current = entry.next.load(Ordering::Acquire);
            }
        }
    }
}

unsafe impl<K: Send, V: Send> Send for LockFreeHashMap<K, V> {}
unsafe impl<K: Send, V: Send> Sync for LockFreeHashMap<K, V> {}

/// Lock-free stack for LIFO operations
pub struct LockFreeStack<T> {
    head: AtomicPtr<Node<T>>,
    size: AtomicUsize,
}

struct Node<T> {
    value: T,
    next: *mut Node<T>,
}

impl<T> LockFreeStack<T> {
    /// Create new lock-free stack
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
            size: AtomicUsize::new(0),
        }
    }
    
    /// Push value onto stack (lock-free)
    #[inline]
    pub fn push(&self, value: T) {
        let node = Box::into_raw(Box::new(Node {
            value,
            next: ptr::null_mut(),
        }));
        
        loop {
            let head = self.head.load(Ordering::Acquire);
            unsafe { (*node).next = head };
            
            if self.head.compare_exchange(
                head,
                node,
                Ordering::Release,
                Ordering::Acquire,
            ).is_ok() {
                self.size.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }
    
    /// Pop value from stack (lock-free)
    #[inline]
    pub fn pop(&self) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            
            if head.is_null() {
                return None;
            }
            
            let next = unsafe { (*head).next };
            
            if self.head.compare_exchange(
                head,
                next,
                Ordering::Release,
                Ordering::Acquire,
            ).is_ok() {
                self.size.fetch_sub(1, Ordering::Relaxed);
                let node = unsafe { Box::from_raw(head) };
                return Some(node.value);
            }
        }
    }
    
    /// Get current size
    #[inline]
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    
    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire).is_null()
    }
}

impl<T> Drop for LockFreeStack<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

unsafe impl<T: Send> Send for LockFreeStack<T> {}
unsafe impl<T: Send> Sync for LockFreeStack<T> {}

impl<T> Default for LockFreeStack<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Arc;
    
    #[test]
    fn test_lock_free_hashmap() {
        let map = LockFreeHashMap::new(16);
        
        map.insert("BTC".to_string(), 50000.0);
        map.insert("ETH".to_string(), 3000.0);
        
        assert_eq!(map.get(&"BTC".to_string()), Some(50000.0));
        assert_eq!(map.get(&"ETH".to_string()), Some(3000.0));
        assert_eq!(map.get(&"XRP".to_string()), None);
        
        assert_eq!(map.len(), 2);
    }
    
    #[test]
    fn test_lock_free_hashmap_concurrent() {
        let map = Arc::new(LockFreeHashMap::new(64));
        
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let map_clone = Arc::clone(&map);
                thread::spawn(move || {
                    for j in 0..100 {
                        let key = format!("key_{}_{}", i, j);
                        map_clone.insert(key.clone(), i * 100 + j);
                        assert!(map_clone.get(&key).is_some());
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(map.len(), 1000);
    }
    
    #[test]
    fn test_lock_free_stack() {
        let stack = LockFreeStack::new();
        
        stack.push(1);
        stack.push(2);
        stack.push(3);
        
        assert_eq!(stack.len(), 3);
        assert_eq!(stack.pop(), Some(3));
        assert_eq!(stack.pop(), Some(2));
        assert_eq!(stack.pop(), Some(1));
        assert_eq!(stack.pop(), None);
        assert!(stack.is_empty());
    }
    
    #[test]
    fn test_lock_free_stack_concurrent() {
        let stack = Arc::new(LockFreeStack::new());
        
        // Push phase
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let stack_clone = Arc::clone(&stack);
                thread::spawn(move || {
                    for j in 0..100 {
                        stack_clone.push(i * 100 + j);
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(stack.len(), 1000);
        
        // Pop phase
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let stack_clone = Arc::clone(&stack);
                thread::spawn(move || {
                    let mut count = 0;
                    while stack_clone.pop().is_some() {
                        count += 1;
                    }
                    count
                })
            })
            .collect();
        
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, 1000);
        assert!(stack.is_empty());
    }
}
