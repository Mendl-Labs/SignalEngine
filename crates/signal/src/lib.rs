// Ultra-low latency unified signal system
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// High-precision nanosecond timer for ultra-low latency
#[inline]
pub fn high_precision_timestamp_ns() -> u64 {
    // Use Instant for high-precision timing (monotonic, no system call overhead)
    static START_TIME: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START_TIME.get_or_init(|| Instant::now());
    start.elapsed().as_nanos() as u64
}

/// Unified signal type for ultra-low latency trading
/// Optimized for minimal memory footprint and zero-copy operations
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Signal {
    // Fixed-size fields to avoid dynamic allocation
    pub id: u64,                    // Atomic counter instead of UUID
    pub strategy_id: u16,           // Compact strategy identifier  
    pub timestamp_ns: u64,          // Nanosecond precision timestamp
    pub symbol_hash: u64,           // Pre-computed hash of symbol
    pub exchange_id: u8,            // Exchange enum (0-255 exchanges)
    pub action: SignalAction,       // 1 byte enum
    pub side: OrderSide,            // Buy/Sell (1 byte)
    pub quantity: f64,              // 8 bytes
    pub price: f64,                 // 8 bytes (NaN = market order)
    pub confidence: f32,            // 4 bytes (0.0-1.0)
    pub flags: u32,                 // Bit flags for urgency, IOC, etc.
}

// Total size: 64 bytes (fits in single cache line)

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalAction {
    Buy = 0,
    Sell = 1,
    BuyLimit = 2,
    SellLimit = 3,
    Cancel = 4,
    Hold = 5,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy = 0,
    Sell = 1,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeId {
    // Centralized Exchanges (CEX)
    Binance = 0,
    Coinbase = 1,
    Kraken = 2,
    FTX = 3,
    
    // SUI Network DEXs (PRIORITY - Built for HFT)
    Cetus = 50,              // Leading AMM on SUI with concentrated liquidity
    Turbos = 51,             // High-performance DEX aggregator
    Aftermath = 52,          // DeFi hub with trading
    DeepBook = 53,           // Native SUI CLOB (Central Limit Order Book)
    
    // Decentralized Exchanges (DEX) - Ethereum
    UniswapV3 = 100,
    SushiSwap = 101,
    Curve = 102,
    Balancer = 103,
    
    // DEX - Solana
    JupiterAggregator = 120,
    Orca = 121,
    Raydium = 122,
    
    // DEX - BSC
    PancakeSwap = 130,
    
    // DEX - Arbitrum/Optimism
    UniswapV3Arbitrum = 140,
    UniswapV3Optimism = 141,
    
    // ... up to 255 exchanges
}

// Signal flags for ultra-fast processing
pub mod signal_flags {
    pub const IMMEDIATE_OR_CANCEL: u32 = 1 << 0;
    pub const FILL_OR_KILL: u32 = 1 << 1;
    pub const POST_ONLY: u32 = 1 << 2;
    pub const REDUCE_ONLY: u32 = 1 << 3;
    pub const URGENT: u32 = 1 << 4;
    pub const BYPASS_RISK_CHECKS: u32 = 1 << 5;
}

impl Default for Signal {
    fn default() -> Self {
        Self {
            id: 0,
            strategy_id: 0,
            timestamp_ns: 0,
            symbol_hash: 0,
            exchange_id: 0,
            action: SignalAction::Hold,
            side: OrderSide::Buy,
            quantity: 0.0,
            price: 0.0,
            confidence: 0.0,
            flags: 0,
        }
    }
}

impl Signal {
    /// Create new signal with atomic ID generation (zero allocation)
    #[inline]
    pub fn new(
        strategy_id: u16,
        symbol_hash: u64,
        exchange_id: ExchangeId,
        action: SignalAction,
        quantity: f64,
        price: f64,
    ) -> Self {
        static SIGNAL_COUNTER: AtomicU64 = AtomicU64::new(1);
        
        Self {
            id: SIGNAL_COUNTER.fetch_add(1, Ordering::Relaxed),
            strategy_id,
            timestamp_ns: high_precision_timestamp_ns(),
            symbol_hash,
            exchange_id: exchange_id as u8,
            action,
            side: if matches!(action, SignalAction::Buy | SignalAction::BuyLimit) {
                OrderSide::Buy 
            } else { 
                OrderSide::Sell 
            },
            quantity,
            price,
            confidence: 1.0,
            flags: 0,
        }
    }

    /// Create new signal with custom timestamp (zero allocation)
    #[inline]
    pub fn new_with_timestamp(
        strategy_id: u16,
        symbol_hash: u64,
        exchange_id: ExchangeId,
        action: SignalAction,
        quantity: f64,
        price: f64,
        timestamp_ns: u64,
    ) -> Self {
        static SIGNAL_COUNTER: AtomicU64 = AtomicU64::new(1);
        
        Self {
            id: SIGNAL_COUNTER.fetch_add(1, Ordering::Relaxed),
            strategy_id,
            timestamp_ns,
            symbol_hash,
            exchange_id: exchange_id as u8,
            action,
            side: if matches!(action, SignalAction::Buy | SignalAction::BuyLimit) {
                OrderSide::Buy 
            } else { 
                OrderSide::Sell 
            },
            quantity,
            price,
            confidence: 1.0,
            flags: 0,
        }
    }

    /// Create urgent market order (bypasses most checks)
    #[inline]
    pub fn urgent_market_order(
        strategy_id: u16,
        symbol_hash: u64,
        exchange_id: ExchangeId,
        side: OrderSide,
        quantity: f64,
    ) -> Self {
        let mut signal = Self::new(
            strategy_id,
            symbol_hash,
            exchange_id,
            if side == OrderSide::Buy { SignalAction::Buy } else { SignalAction::Sell },
            quantity,
            f64::NAN, // Market order
        );
        signal.flags |= signal_flags::URGENT | signal_flags::BYPASS_RISK_CHECKS;
        signal
    }

    #[inline]
    pub fn is_market_order(&self) -> bool {
        self.price.is_nan()
    }

    #[inline] 
    pub fn is_urgent(&self) -> bool {
        (self.flags & signal_flags::URGENT) != 0
    }

    #[inline]
    pub fn should_bypass_risk_checks(&self) -> bool {
        (self.flags & signal_flags::BYPASS_RISK_CHECKS) != 0
    }
}

/// Pre-computed symbol hash table for ultra-fast lookups
pub struct SymbolHashTable {
    // Static symbol mappings for fastest possible lookups
    pub btc_usd: u64,
    pub eth_usd: u64,
    pub bnb_usd: u64,
    // ... other major pairs
}

impl SymbolHashTable {
    pub const fn new() -> Self {
        Self {
            btc_usd: hash_symbol("BTC/USD"),
            eth_usd: hash_symbol("ETH/USD"), 
            bnb_usd: hash_symbol("BNB/USD"),
        }
    }
}

/// Compile-time hash function for symbols
pub const fn hash_symbol(symbol: &str) -> u64 {
    // FNV-1a hash - very fast for short strings
    const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    
    let bytes = symbol.as_bytes();
    let mut hash = FNV_OFFSET_BASIS;
    let mut i = 0;
    
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    
    hash
}

// Global symbol table
pub static SYMBOLS: SymbolHashTable = SymbolHashTable::new();

/// Lock-free signal queue for ultra-low latency
pub struct UltraFastSignalQueue {
    buffer: *mut Signal,
    capacity: usize,
    head: AtomicU64,
    tail: AtomicU64,
    mask: u64,
}

unsafe impl Send for UltraFastSignalQueue {}
unsafe impl Sync for UltraFastSignalQueue {}

impl UltraFastSignalQueue {
    /// Create new queue with power-of-2 capacity for bit masking
    pub fn new(capacity_pow2: usize) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        assert!(capacity_pow2.is_power_of_two());
        
        let layout = std::alloc::Layout::array::<Signal>(capacity_pow2)
            .map_err(|e| format!("Failed to create memory layout for signal queue: {}", e))?;
        let buffer = unsafe { std::alloc::alloc(layout) as *mut Signal };
        
        // Check for allocation failure
        if buffer.is_null() {
            return Err("Failed to allocate memory for signal queue buffer".into());
        }
        
        // Initialize buffer with zeroed signals - this is safe now that we've checked for null
        unsafe {
            std::ptr::write_bytes(buffer, 0, capacity_pow2);
        }
        
        Ok(Self {
            buffer,
            capacity: capacity_pow2,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            mask: (capacity_pow2 - 1) as u64,
        })
    }

    /// Try to push signal (non-blocking, ultra-fast)
    #[inline]
    pub fn try_push(&self, signal: Signal) -> Result<(), Signal> {
        let tail = self.tail.load(Ordering::Relaxed);
        let next_tail = tail.wrapping_add(1);
        
        // Check if full using bit mask (faster than modulo)
        if (next_tail & self.mask) == (self.head.load(Ordering::Acquire) & self.mask) {
            return Err(signal); // Queue full
        }
        
        // Store signal and update tail atomically
        // Safety: We've verified the queue isn't full and the index is within bounds due to masking
        let index = (tail & self.mask) as usize;
        debug_assert!(index < self.capacity, "Signal queue index out of bounds");
        unsafe {
            std::ptr::write(self.buffer.add(index), signal);
        }
        
        self.tail.store(next_tail, Ordering::Release);
        Ok(())
    }

    /// Try to pop signal (non-blocking, ultra-fast) 
    #[inline]
    pub fn try_pop(&self) -> Option<Signal> {
        let head = self.head.load(Ordering::Relaxed);
        
        // Check if empty
        if head == self.tail.load(Ordering::Acquire) {
            return None;
        }
        
        // Load signal and update head
        // Safety: We've verified the queue isn't empty and the index is within bounds due to masking
        let index = (head & self.mask) as usize;
        debug_assert!(index < self.capacity, "Signal queue index out of bounds");
        let signal = unsafe { 
            std::ptr::read(self.buffer.add(index))
        };
        
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Some(signal)
    }

    /// Get current queue length (approximate)
    #[inline]
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        (tail.wrapping_sub(head) & (self.capacity as u64 - 1)) as usize
    }
}

impl Drop for UltraFastSignalQueue {
    fn drop(&mut self) {
        if let Ok(layout) = std::alloc::Layout::array::<Signal>(self.capacity) {
            unsafe {
                std::alloc::dealloc(self.buffer as *mut u8, layout);
            }
        }
        // If layout creation fails, we can't safely deallocate
        // This is a memory leak but prevents a crash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_signal_size() {
        assert_eq!(std::mem::size_of::<Signal>(), 64);
        assert_eq!(std::mem::align_of::<Signal>(), 8);
    }
    
    #[test]
    fn test_signal_creation() {
        let signal = Signal::new(
            1, 
            SYMBOLS.btc_usd,
            ExchangeId::Binance,
            SignalAction::Buy,
            1.0,
            50000.0
        );
        
        assert_eq!(signal.strategy_id, 1);
        assert_eq!(signal.symbol_hash, SYMBOLS.btc_usd);
        assert_eq!(signal.action, SignalAction::Buy);
    }
    
    #[test] 
    fn test_queue_operations() {
        let queue = UltraFastSignalQueue::new(1024).expect("Failed to create signal queue");
        
        let signal = Signal::urgent_market_order(
            1,
            SYMBOLS.btc_usd,
            ExchangeId::Binance, 
            OrderSide::Buy,
            1.0
        );
        
        assert!(queue.try_push(signal).is_ok());
        assert_eq!(queue.len(), 1);
        
        let popped = queue.try_pop().expect("Failed to pop signal from queue");
        assert_eq!(popped.id, signal.id);
        assert_eq!(queue.len(), 0);
    }
}
