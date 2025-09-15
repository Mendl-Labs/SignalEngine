/// SignalEngine - Ultra-Fast Trading Signal Processing System
/// 
/// Core library providing shared logging, utilities, and common types
/// for all SignalEngine components.

pub mod logging;
pub mod logging_guide;

// Re-export logging components for easy access
pub use logging::{
    SignalEngineLogger, 
    TradingContext, 
    initialize_signal_engine_logging,
    log_trading_execution,
    log_trading_signal, 
    log_info_async,
    log_warn_async,
    log_error_async,
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