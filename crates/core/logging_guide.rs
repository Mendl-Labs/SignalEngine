// Ultra-Fast SignalEngine Logging Usage Guide
//
// This document demonstrates how to use the SignalEngine logging facade
// consistently across all components for maximum performance and clarity.

/*## 1. INITIALIZATION

In your main program or component initialization:

```rust
use signalengine::{initialize_signal_engine, SignalEngineLogger};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the entire system with logging
    initialize_signal_engine().await?;
    
    let logger = SignalEngineLogger::new("YourComponent").await;
    logger.info("Component initialized").await;
    
    Ok(())
}
```

## 2. COMPONENT LOGGING

For each major component, create a logger instance:

```rust
use signalengine::{SignalEngineLogger, TradingContext};

pub struct YourTradingComponent {
    logger: Arc<SignalEngineLogger>,
    // ... other fields
}

impl YourTradingComponent {
    pub async fn new() -> Self {
        let logger = Arc::new(SignalEngineLogger::new("YourComponent").await);
        
        // Log initialization
        logger.info("YourComponent initialized").await;
        
        Self {
            logger,
            // ... initialize other fields
        }
    }
}
```

## 3. TRADING-SPECIFIC LOGGING

Use the specialized trading log methods for audit trails:

```rust
// Log execution with full context
self.logger.log_execution(
    "order_12345",          // order_id
    "BTC/USD",              // symbol  
    "binance",              // exchange
    1.5,                    // quantity
    50000.0,                // price
    25.0,                   // fees
    1200                    // latency_ns
).await;

// Log signal generation
self.logger.log_signal(
    67890,                  // signal_id
    "ETH/USD",              // symbol
    "BUY",                  // action
    "URGENT"                // urgency
).await;

// Log portfolio updates
self.logger.log_portfolio_update(
    "kraken",               // exchange
    150000.0,               // total_value
    7500.0                  // pnl
).await;

// Log risk checks
self.logger.log_risk_check(
    "BTC/USD",              // symbol
    0.85,                   // risk_score
    "ALLOW"                 // action_taken
).await;
```

## 4. CONTEXT-AWARE LOGGING

For complex operations, use context-aware logging:

```rust
let context = TradingContext::new("ExecutionHandler")
    .with_operation("submit_order")
    .with_symbol("BTC/USD")
    .with_order_id("order_12345")
    .with_exchange("binance")
    .with_latency(1500);

self.logger.info_ctx("Order submitted successfully", context).await;

// For errors
self.logger.error_ctx("Order submission failed", context).await;
```

## 5. HIGH-PERFORMANCE ASYNC LOGGING

For ultra-hot paths, use the async macros to avoid blocking:

```rust
use signalengine::{log_trading_execution, log_trading_signal, log_info_async};

// In hot trading path - these spawn async tasks and return immediately
log_trading_execution!(self.logger, order_id, symbol, exchange, qty, price, fees, latency);
log_trading_signal!(self.logger, signal_id, symbol, action, urgency);
log_info_async!(self.logger, "Hot path operation completed");
```

## 6. STANDARD LOG LEVELS

Use appropriate log levels:

```rust
// Info - normal operations, successful executions
self.logger.info("Trade executed successfully").await;

// Warn - potential issues, circuit breaker triggers
self.logger.warn("High latency detected").await;

// Error - failures that don't crash the system
self.logger.error("Failed to connect to exchange").await;

// Debug - detailed information for troubleshooting
self.logger.debug("Order book updated").await;

// Critical - system failures, immediate attention required
self.logger.critical("Database connection lost").await;
```

## 7. MIGRATION FROM OLD LOGGING

Replace existing logging patterns:

```rust
// OLD: Direct println!
println!("Order executed: {} at {}", symbol, price);

// NEW: Structured logging
self.logger.log_execution(order_id, symbol, exchange, qty, price, fees, latency).await;

// OLD: log crate
log::info!("Operation completed");

// NEW: SignalEngine facade
self.logger.info("Operation completed").await;

// OLD: Ultra-logger direct usage
let logger = UltraLogger::new("Component".to_string());
logger.log(LogLevel::Info, message).await;

// NEW: SignalEngine facade
let logger = SignalEngineLogger::new("Component").await;
logger.info(&message).await;
```

## 8. ERROR HANDLING WITH LOGGING

Combine error handling with proper logging:

```rust
match self.submit_order(order).await {
    Ok(result) => {
        self.logger.log_execution(
            &result.order_id,
            &result.symbol,
            &result.exchange,
            result.quantity,
            result.price,
            result.fees,
            result.latency_ns
        ).await;
    }
    Err(e) => {
        let context = TradingContext::new("ExecutionHandler")
            .with_operation("submit_order")
            .with_symbol(&order.symbol);
        self.logger.error_ctx(&format!("Order submission failed: {}", e), context).await;
        return Err(e);
    }
}
```

## 9. PERFORMANCE CONSIDERATIONS

- Use async macros (`log_info_async!`, etc.) in hot trading paths
- Batch related log entries when possible
- Use appropriate log levels to control verbosity
- The logging system is designed for zero-allocation in critical paths

## 10. MONITORING AND OBSERVABILITY

The logging system integrates with monitoring:

```rust
// Performance metrics are automatically included
self.logger.info_ctx("Operation completed", context.with_latency(elapsed_ns)).await;

// This creates searchable audit trails
self.logger.log_execution(order_id, symbol, exchange, qty, price, fees, latency).await;
```

*/

#[cfg(test)]
mod examples {
    use crate::{SignalEngineLogger, TradingContext, initialize_signal_engine_logging};
    use std::sync::Arc;
    
    /// Example trading component showing proper logging usage
    pub struct ExampleTradingComponent {
        logger: Arc<SignalEngineLogger>,
    }
    
    impl ExampleTradingComponent {
        pub async fn new() -> Self {
            let logger: Arc<SignalEngineLogger> = Arc::new(SignalEngineLogger::new("ExampleComponent").await);
            logger.info("ExampleTradingComponent initialized").await;
            
            Self { logger }
        }
        
        pub async fn execute_trade(&self, symbol: &str, quantity: f64, price: f64) -> Result<String, String> {
            let start_time = std::time::Instant::now();
            let order_id = format!("order_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros());
            
            // Log trade attempt with context
            let context = TradingContext::new("ExampleComponent")
                .with_operation("execute_trade")
                .with_symbol(symbol)
                .with_order_id(&order_id);
            
            self.logger.info_ctx("Executing trade", context.clone()).await;
            
            // Simulate trade execution
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
            
            let latency_ns = start_time.elapsed().as_nanos() as u64;
            
            // Log successful execution
            self.logger.log_execution(
                &order_id,
                symbol,
                "binance",
                quantity,
                price,
                price * quantity * 0.001, // 0.1% fee
                latency_ns
            ).await;
            
            Ok(order_id)
        }
        
        pub async fn generate_signal(&self, symbol: &str, action: &str) -> u64 {
            let signal_id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
            
            // Log signal with context
            let context = TradingContext::new("ExampleComponent")
                .with_operation("generate_signal")
                .with_symbol(symbol);
            self.logger.info_ctx(&format!("Signal {} generated: {}", signal_id, action), context).await;
            
            signal_id
        }
    }
    
    #[tokio::test]
    async fn test_comprehensive_logging() {
        initialize_signal_engine_logging().await.unwrap();
        
        let component = ExampleTradingComponent::new().await;
        
        // Test trade execution logging
        let result = component.execute_trade("BTC/USD", 1.5, 50000.0).await;
        assert!(result.is_ok());
        
        // Test signal logging
        let signal_id = component.generate_signal("ETH/USD", "BUY").await;
        assert!(signal_id > 0);
    }
}