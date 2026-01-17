//! SignalEngine - Ultra-Fast Trading Signal Processing System
//! 
//! Core library providing shared logging, utilities, and common types
//! for all SignalEngine components.

pub mod logging;
pub mod logging_guide;
pub mod system_logging;
pub mod rdtsc;
pub mod cache_aligned;
pub mod simd;
pub mod memory_pool;
pub mod system_optimize;
pub mod lock_free;
pub mod zero_copy;
pub mod memory_ordering;

// Re-export logging components for easy access
pub use logging::{
    SignalEngineLogger, 
    TradingContext, 
    initialize_signal_engine_logging,
};

// Re-export system logging
pub use system_logging::{
    SystemLogger,
    LogCategory,
    Level as LogLevel,
    LogEntry,
    LogMetrics,
    LOG_METRICS,
};

// Re-export RDTSC timing utilities
pub use rdtsc::{
    get_rdtsc,
    get_timestamp_ns,
    rdtsc_duration_ns,
    rdtsc_to_ns,
    init_rdtsc,
};

// Re-export cache-aligned atomic types
pub use cache_aligned::{
    CacheAlignedAtomicU64,
    CacheAlignedAtomicU32,
    CacheAlignedAtomicBool,
    AtomicMetrics,
};

// Re-export SIMD operations
pub use simd::{
    price_diff,
    returns,
    sma,
    min,
    max,
    sum,
};

// Re-export memory pool
pub use memory_pool::{MemoryPool, PoolGuard, PoolStats};

// Re-export system optimization utilities
pub use system_optimize::{
    ThreadPriority,
    SystemOptimization,
    set_thread_affinity,
    set_thread_priority,
    pin_to_core,
    optimize_thread_for_latency,
    get_cpu_count,
    enable_huge_pages,
};

// Re-export lock-free data structures
pub use lock_free::{
    LockFreeHashMap,
    LockFreeStack,
};

// Re-export zero-copy utilities
pub use zero_copy::{
    ZeroCopySignal,
    SignalArena,
    ZeroCopyChannel,
};

// Re-export memory ordering utilities
pub use memory_ordering::{
    MemoryBarrier,
    cpu_pause,
    SpinWait,
    OrderingStrategy,
    Prefetch,
    CacheLinePadding,
};

/// SignalEngine version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Initialize the entire SignalEngine system
pub async fn initialize_signal_engine() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging first
    logging::initialize_signal_engine_logging().await?;
    
    let logger = SignalEngineLogger::new("SignalEngine").await;
    logger.info(&format!("SignalEngine v{} initializing...", VERSION)).await;
    
    Ok(())
}