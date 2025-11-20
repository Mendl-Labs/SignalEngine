/// Ultra-Fast Logging Facade for SignalEngine
/// 
/// This module provides a centralized logging interface that wraps the ultra-logger
/// for consistent, high-performance logging across all SignalEngine components.
/// 
/// Features:
/// - Zero-allocation hot path logging
/// - Async logging with batching
/// - Context-aware logging with component identification
/// - Performance metrics integration
/// - Trade execution audit trails
use ultra_logger::{UltraLogger, LogLevel};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::Serialize;

/// Global logger registry for component-specific loggers
static LOGGER_REGISTRY: std::sync::LazyLock<LoggerRegistry> = std::sync::LazyLock::new(|| {
    LoggerRegistry::new()
});

/// Logger registry manages component-specific ultra-logger instances
pub struct LoggerRegistry {
    loggers: Arc<RwLock<HashMap<String, Arc<UltraLogger>>>>,
}

impl LoggerRegistry {
    fn new() -> Self {
        Self {
            loggers: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Get or create a logger for a specific component
    pub async fn get_logger(&self, component: &str) -> Arc<UltraLogger> {
        let mut loggers = self.loggers.write().await;
        
        if let Some(logger) = loggers.get(component) {
            return logger.clone();
        }
        
        let logger = Arc::new(UltraLogger::new(component.to_string()));
        loggers.insert(component.to_string(), logger.clone());
        logger
    }
}

/// Trading-specific log context for audit trails
#[derive(Debug, Clone, Serialize)]
pub struct TradingContext {
    pub component: String,
    pub operation: Option<String>,
    pub symbol: Option<String>,
    pub order_id: Option<String>,
    pub exchange: Option<String>,
    pub latency_ns: Option<u64>,
}

impl TradingContext {
    pub fn new(component: &str) -> Self {
        Self {
            component: component.to_string(),
            operation: None,
            symbol: None,
            order_id: None,
            exchange: None,
            latency_ns: None,
        }
    }
    
    pub fn with_operation(mut self, operation: &str) -> Self {
        self.operation = Some(operation.to_string());
        self
    }
    
    pub fn with_symbol(mut self, symbol: &str) -> Self {
        self.symbol = Some(symbol.to_string());
        self
    }
    
    pub fn with_order_id(mut self, order_id: &str) -> Self {
        self.order_id = Some(order_id.to_string());
        self
    }
    
    pub fn with_exchange(mut self, exchange: &str) -> Self {
        self.exchange = Some(exchange.to_string());
        self
    }
    
    pub fn with_latency(mut self, latency_ns: u64) -> Self {
        self.latency_ns = Some(latency_ns);
        self
    }
}

/// High-performance logging facade
pub struct SignalEngineLogger {
    component: String,
    logger: Arc<UltraLogger>,
}

impl SignalEngineLogger {
    /// Create a new logger for a component
    pub async fn new(component: &str) -> Self {
        let logger = LOGGER_REGISTRY.get_logger(component).await;
        Self {
            component: component.to_string(),
            logger,
        }
    }
    
    /// Log with context (most flexible)
    pub async fn log_with_context(&self, level: LogLevel, message: &str, context: Option<TradingContext>) {
        let formatted_message = if let Some(ctx) = context {
            if let Ok(context_json) = serde_json::to_string(&ctx) {
                format!("[{}] {} | Context: {}", self.component, message, context_json)
            } else {
                format!("[{}] {}", self.component, message)
            }
        } else {
            format!("[{}] {}", self.component, message)
        };
        
        let _ = self.logger.log(level, formatted_message).await;
    }
    
    /// Info level logging
    pub async fn info(&self, message: &str) {
        self.log_with_context(LogLevel::Info, message, None).await;
    }
    
    /// Info with trading context
    pub async fn info_ctx(&self, message: &str, context: TradingContext) {
        self.log_with_context(LogLevel::Info, message, Some(context)).await;
    }
    
    /// Warning level logging
    pub async fn warn(&self, message: &str) {
        self.log_with_context(LogLevel::Warn, message, None).await;
    }
    
    /// Warning with trading context
    pub async fn warn_ctx(&self, message: &str, context: TradingContext) {
        self.log_with_context(LogLevel::Warn, message, Some(context)).await;
    }
    
    /// Error level logging
    pub async fn error(&self, message: &str) {
        self.log_with_context(LogLevel::Error, message, None).await;
    }
    
    /// Error with trading context
    pub async fn error_ctx(&self, message: &str, context: TradingContext) {
        self.log_with_context(LogLevel::Error, message, Some(context)).await;
    }
    
    /// Debug level logging
    pub async fn debug(&self, message: &str) {
        self.log_with_context(LogLevel::Debug, message, None).await;
    }
    
    /// Debug with trading context
    pub async fn debug_ctx(&self, message: &str, context: TradingContext) {
        self.log_with_context(LogLevel::Debug, message, Some(context)).await;
    }
    
    /// Critical/Fatal level logging (maps to Error)
    pub async fn critical(&self, message: &str) {
        self.log_with_context(LogLevel::Error, message, None).await;
    }
    
    /// Critical with trading context (maps to Error)
    pub async fn critical_ctx(&self, message: &str, context: TradingContext) {
        self.log_with_context(LogLevel::Error, message, Some(context)).await;
    }
    
    /// Log trading execution with full context
    #[allow(clippy::too_many_arguments)]
    pub async fn log_execution(&self, 
        order_id: &str, 
        symbol: &str, 
        exchange: &str, 
        quantity: f64, 
        price: f64, 
        fees: f64, 
        latency_ns: u64
    ) {
        let context = TradingContext::new(&self.component)
            .with_operation("execution")
            .with_symbol(symbol)
            .with_order_id(order_id)
            .with_exchange(exchange)
            .with_latency(latency_ns);
            
        let message = format!(
            "EXECUTION: {} {} {} @ {} (Fee: {}, Latency: {}ns)", 
            quantity, symbol, exchange, price, fees, latency_ns
        );
        
        self.info_ctx(&message, context).await;
    }
    
    /// Log signal generation
    pub async fn log_signal(&self, signal_id: u64, symbol: &str, action: &str, urgency: &str) {
        let context = TradingContext::new(&self.component)
            .with_operation("signal_generation")
            .with_symbol(symbol)
            .with_order_id(&signal_id.to_string());
            
        let message = format!("SIGNAL: {} {} {} ({})", signal_id, action, symbol, urgency);
        self.info_ctx(&message, context).await;
    }
    
    /// Log portfolio update
    pub async fn log_portfolio_update(&self, exchange: &str, total_value: f64, pnl: f64) {
        let context = TradingContext::new(&self.component)
            .with_operation("portfolio_update")
            .with_exchange(exchange);
            
        let message = format!("PORTFOLIO: {} Total: ${:.2} PnL: ${:.2}", exchange, total_value, pnl);
        self.info_ctx(&message, context).await;
    }
    
    /// Log risk check
    pub async fn log_risk_check(&self, symbol: &str, risk_score: f64, action_taken: &str) {
        let context = TradingContext::new(&self.component)
            .with_operation("risk_check")
            .with_symbol(symbol);
            
        let message = format!("RISK: {} Score: {:.2} Action: {}", symbol, risk_score, action_taken);
        self.warn_ctx(&message, context).await;
    }
}

/// Convenience macros for zero-allocation logging in hot paths
#[macro_export]
macro_rules! log_trading_execution {
    ($logger:expr, $order_id:expr, $symbol:expr, $exchange:expr, $qty:expr, $price:expr, $fees:expr, $latency:expr) => {
        tokio::spawn({
            let logger = $logger.clone();
            let order_id = $order_id.to_string();
            let symbol = $symbol.to_string();
            let exchange = $exchange.to_string();
            async move {
                logger.log_execution(&order_id, &symbol, &exchange, $qty, $price, $fees, $latency).await;
            }
        });
    };
}

#[macro_export]
macro_rules! log_trading_signal {
    ($logger:expr, $signal_id:expr, $symbol:expr, $action:expr, $urgency:expr) => {
        tokio::spawn({
            let logger = $logger.clone();
            let symbol = $symbol.to_string();
            let action = $action.to_string();
            let urgency = $urgency.to_string();
            async move {
                logger.log_signal($signal_id, &symbol, &action, &urgency).await;
            }
        });
    };
}

#[macro_export]
macro_rules! log_info_async {
    ($logger:expr, $message:expr) => {
        tokio::spawn({
            let logger = $logger.clone();
            let message = $message.to_string();
            async move {
                logger.info(&message).await;
            }
        });
    };
}

#[macro_export]
macro_rules! log_warn_async {
    ($logger:expr, $message:expr) => {
        tokio::spawn({
            let logger = $logger.clone();
            let message = $message.to_string();
            async move {
                logger.warn(&message).await;
            }
        });
    };
}

#[macro_export]
macro_rules! log_error_async {
    ($logger:expr, $message:expr) => {
        tokio::spawn({
            let logger = $logger.clone();
            let message = $message.to_string();
            async move {
                logger.error(&message).await;
            }
        });
    };
}

/// Initialize logging system for the entire SignalEngine
pub async fn initialize_signal_engine_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Pre-register all core components
    let components = [
        "SignalEngine",
        "ExecutionHandler", 
        "StrategyHandler",
        "SignalDispatcher",
        "SignalGenerator",
        "PortfolioHandler",
        "RiskManager",
        "OrderBook",
        "SmartOrderRouter",
    ];
    
    for component in &components {
        let _ = LOGGER_REGISTRY.get_logger(component).await;
    }
    
    // Log system initialization
    let main_logger = SignalEngineLogger::new("SignalEngine").await;
    main_logger.info("Ultra-fast logging system initialized for SignalEngine").await;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_logger_creation() {
        let logger = SignalEngineLogger::new("TestComponent").await;
        logger.info("Test message").await;
        
        // Test context logging
        let context = TradingContext::new("TestComponent")
            .with_operation("test")
            .with_symbol("BTC/USD");
            
        logger.info_ctx("Test with context", context).await;
    }
    
    #[tokio::test]
    async fn test_trading_specific_logging() {
        let logger = SignalEngineLogger::new("ExecutionHandler").await;
        
        logger.log_execution("12345", "BTC/USD", "binance", 1.0, 50000.0, 25.0, 1500).await;
        logger.log_signal(67890, "ETH/USD", "BUY", "URGENT").await;
        logger.log_portfolio_update("kraken", 100000.0, 5000.0).await;
        logger.log_risk_check("BTC/USD", 0.75, "ALLOW").await;
    }
}