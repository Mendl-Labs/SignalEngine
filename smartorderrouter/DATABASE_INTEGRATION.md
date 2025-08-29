# SmartOrderRouter Database Integration Guide

## Overview

The SmartOrderRouter has been enhanced with comprehensive database integration capabilities to automatically save strategy orders to a PostgreSQL database using the strategy_order_ops.rs workflow. This integration includes both **order creation** and **order execution tracking** for complete order lifecycle management.

## Architecture

### Components

1. **DatabaseOrderPersistence Trait**: Interface for database operations
   - `save_strategy_order()`: Save new orders to database  
   - `update_order_execution()`: Update order execution details
2. **StrategyOrderData Struct**: Data structure for new orders
3. **OrderExecutionData Struct**: Data structure for execution updates
4. **InMemoryOrderDatabase**: Test implementation for development
5. **PostgresOrderDatabase**: Production implementation (example provided)
6. **SmartOrderRouter**: Enhanced with complete order lifecycle tracking

### Complete Order Lifecycle with Database Integration

```
1. ORDER CREATION:
Signal → SmartOrderRouter.route_order()
    ↓
Child Orders Created (route_abc123_child_0, route_abc123_child_1)  
    ↓
log_orders_for_database() called
    ↓
✓ Orders saved to database with initial status "Pending"

2. ORDER EXECUTION:
Exchange Fill → update_child_order_execution()
    ↓
log_execution_to_database() called
    ↓  
Database execution update with:
  - Filled quantity
  - Fill price
  - Fees paid
  - Updated status (PartiallyFilled/Filled/Cancelled/etc.)
    ↓
✓ Order execution tracked in database
```

## Database Schema Integration

### Order Creation Fields
- **trade_id**: Unique child order ID
- **symbol**: Trading pair (e.g., "BTC/USD") 
- **order_type**: Market, Limit, StopLimit, Iceberg, TWAP, VWAP, Implementation
- **side**: Buy or Sell
- **quantity**: Order size as string
- **price**: Optional limit price
- **time_in_force**: IOC/GTC/FOK/DAY/GTD
- **execution_urgency**: Low/Medium/High/Critical
- **created_at**: UTC timestamp

### Order Execution Fields  
- **trade_id**: Matching order ID for updates
- **filled_quantity**: Amount filled in this execution
- **fill_price**: Execution price
- **fees_paid**: Transaction fees
- **status**: Pending/PartiallyFilled/Filled/Cancelled/Rejected/Expired/Failed
- **updated_at**: Execution timestamp

## Usage Examples

### 1. Basic Usage with Order Creation and Execution Tracking

```rust
use std::sync::Arc;
use smartorderrouter::{SmartOrderRouter, InMemoryOrderDatabase, OrderSide, ExecutionUrgency, RoutingAlgorithm, RouteStatus};
use exchangemetricaggregator::ExchangeMetricsAggregator;

// Create test database
let database = Arc::new(InMemoryOrderDatabase::new());

// Create metrics aggregator
let metrics = Arc::new(ExchangeMetricsAggregator::new(1000, 100));

// Create router with database integration
let router = SmartOrderRouter::new(
    metrics,
    1000,  // max_route_history
    1000,  // monitoring_interval_ms
    Some(database.clone())  // database interface
);

// Create an order - automatically saves to database
let route_id = router.route_order(
    "BTC/USD",
    OrderSide::Buy,
    100.0,
    ExecutionUrgency::High,
    RoutingAlgorithm::SmartRouting,
)?;

// Simulate order execution - automatically tracked in database
router.update_child_order_execution(
    &route_id,
    "route_abc123_child_0", // child order ID
    50.0,       // filled quantity
    50000.0,    // fill price
    25.0,       // fees
    RouteStatus::PartiallyFilled,
)?;

// Complete the order
router.update_child_order_execution(
    &route_id,
    "route_abc123_child_0",
    50.0,       // remaining quantity  
    50100.0,    // fill price
    25.0,       // fees
    RouteStatus::Filled,
)?;
```

### 2. Production Usage with PostgreSQL

```rust
use smartorderrouter::database_integration_example::{setup_smart_router_with_database, example_order_with_database_save};

// Set up router with PostgreSQL integration
let router = setup_smart_router_with_database().await?;

// Route orders with automatic database persistence
example_order_with_database_save().await?;
```

### 3. Custom Database Implementation with Execution Tracking

```rust
use smartorderrouter::{DatabaseOrderPersistence, StrategyOrderData, OrderExecutionData};

pub struct MyCustomDatabase {
    // Your database connection/config
}

impl DatabaseOrderPersistence for MyCustomDatabase {
    fn save_strategy_order(&self, order_data: &StrategyOrderData) -> Result<(), String> {
        // Your custom order creation logic
        // Convert to NewStrategyOrder and use strategy_order_ops.rs
        Ok(())
    }
    
    fn update_order_execution(&self, execution_data: &OrderExecutionData) -> Result<(), String> {
        // Your custom execution tracking logic
        // Update filled_quantity, avg_fill_price, fees_paid, status in database
        Ok(())
    }
}
```

## Testing

### Run Database Integration Tests

```bash
cd SignalEngine/smartorderrouter

# Test order creation tracking
cargo test test_database_integration -- --nocapture

# Test order execution tracking  
cargo test test_execution_tracking -- --nocapture

# Run all tests
cargo test -- --nocapture
```

### Expected Output for Order Creation

```
=== Saving orders to database ===
Route: route_abc123
Symbol: BTC/USD, Side: Buy, Total Quantity: 10
✓ Saved child order route_abc123_child_0 to database
✓ Saved child order route_abc123_child_1 to database
=== End of database integration ===

Successfully saved 2 orders to database
Order: Buy 5.015 BTC/USD at 2025-08-28 21:19:56 UTC
Order: Buy 4.985 BTC/USD at 2025-08-28 21:19:56 UTC
```

### Expected Output for Order Execution Tracking

```
=== Simulating execution of child order route_abc123_child_0 ===
✓ Updated execution for order route_abc123_child_0 - Filled: 2.5, Price: 50000, Status: PartiallyFilled
✓ Updated execution for order route_abc123_child_0 - Filled: 2.5, Price: 50100, Status: Filled
✓ Successfully tracked order execution lifecycle
```

## Integration with strategy_order_ops.rs

To use the real PostgreSQL database through strategy_order_ops.rs:

1. **Include the dependency** in your Cargo.toml:
```toml
[dependencies]
databaseschema = { path = "../../databaseschema" }
```

2. **Implement PostgresOrderDatabase**:
```rust
impl DatabaseOrderPersistence for PostgresOrderDatabase {
    fn save_strategy_order(&self, order_data: &StrategyOrderData) -> Result<(), String> {
        // Convert StrategyOrderData to NewStrategyOrder
        let new_order = NewStrategyOrder {
            trade_id: order_data.trade_id.clone(),
            symbol: order_data.symbol.clone(),
            order_type: order_data.order_type.clone(),
            side: order_data.side.clone(),
            quantity: order_data.quantity.parse().map_err(|_| "Invalid quantity")?,
            price: order_data.price.as_ref().and_then(|p| p.parse().ok()),
            time_in_force: order_data.time_in_force.clone(),
            execution_urgency: order_data.execution_urgency.clone(),
        };
        
        // Use the strategy_order_ops workflow
        let result = StrategyOrderWorkflow::create_order_with_state(
            conn,
            new_order,
            Some("SmartOrderRouter".to_string())
        ).await;
        
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Database error: {}", e)),
        }
    }
}
```

## Configuration Options

### Router Constructor Parameters

- **metrics_aggregator**: Exchange metrics for routing decisions
- **max_route_history**: Maximum number of completed routes to keep in memory
- **monitoring_interval_ms**: Interval for route monitoring (milliseconds)
- **database**: Optional database interface for order persistence

### Database Interface Options

- **None**: No database integration, orders are logged only
- **Some(Arc<dyn DatabaseOrderPersistence>)**: Database integration enabled

## Error Handling

The database integration includes comprehensive error handling:

- **Connection errors**: Logged and operation continues
- **Serialization errors**: Invalid data format handling
- **Transaction errors**: Database transaction failure handling
- **Timeout errors**: Database operation timeout handling

## Performance Considerations

- **Async Operations**: Database saves are synchronous in current implementation
- **Connection Pooling**: Use connection pooling for production deployments
- **Batch Operations**: Consider batching multiple orders for better performance
- **Error Recovery**: Implement retry logic for transient database errors

## Monitoring and Debugging

### Debug Output

Set `RUST_LOG=debug` to see detailed database integration logs:

```bash
RUST_LOG=debug cargo test test_database_integration -- --nocapture
```

### Production Monitoring

Monitor database integration health:

- Order save success/failure rates
- Database connection status
- Order persistence latency
- Database transaction counts

## Best Practices

1. **Always use connection pooling** in production
2. **Implement proper error handling** and retry logic
3. **Monitor database performance** and optimize queries
4. **Use transactions** for consistency when saving multiple orders
5. **Implement backup strategies** for database failures
6. **Test thoroughly** with realistic order volumes

## Troubleshooting

### Common Issues

1. **Database connection failures**:
   - Check connection string
   - Verify database is running
   - Check network connectivity

2. **Order data validation errors**:
   - Verify enum variant names match
   - Check data type conversions
   - Validate required fields

3. **Performance issues**:
   - Check database indexes
   - Monitor connection pool usage
   - Consider async database operations

### Debug Commands

```bash
# Test database integration
cargo test test_database_integration -- --nocapture

# Check compilation
cargo check

# Run all tests
cargo test -- --nocapture
```

## Next Steps

1. **Implement async database operations** for better performance
2. **Add connection pooling** for production use
3. **Implement retry logic** for transient failures  
4. **Add metrics collection** for database operations
5. **Create dashboard** for order persistence monitoring
