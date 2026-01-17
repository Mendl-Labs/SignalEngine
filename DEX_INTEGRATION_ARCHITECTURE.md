# SignalEngine + DEX Integration Architecture

## Overview

The DEX connectors (Cetus & DeepBook) integrate seamlessly into your existing ultra-low-latency HFT SignalEngine architecture. Here's how the components work together:

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                     TRADING STRATEGY LAYER                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │   Strategy   │  │   Strategy   │  │   Strategy   │             │
│  │   Handler    │  │   Handler    │  │   Handler    │             │
│  │   (Arb)      │  │   (MM)       │  │   (Trend)    │             │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘             │
│         └─────────────────┴─────────────────┘                       │
│                            │                                         │
│                            ▼                                         │
│                   ┌────────────────┐                                │
│                   │  Signal Queue  │                                │
│                   │  (64-byte      │                                │
│                   │   cache-line)  │                                │
│                   └────────┬───────┘                                │
└──────────────────────────────┼─────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   SIGNAL DISPATCHER LAYER                           │
│  ┌──────────────────────────────────────────────────────┐          │
│  │  UltraFastSignalDispatcher                           │          │
│  │  • Lock-free queues (urgent + normal)                │          │
│  │  • SIMD-optimized batch routing                      │          │
│  │  • RDTSC hardware timestamping                       │          │
│  │  • 2.33μs latency (313K orders/sec on Kraken)       │          │
│  └──────────────────┬──────────────────┬────────────────┘          │
│                     │                  │                            │
│         ┌───────────┴────────┐  ┌─────┴───────────┐               │
│         │ Execution Channel  │  │ Portfolio Ch.   │               │
│         │  (crossbeam)       │  │  Risk Ch.       │               │
│         └───────────┬────────┘  └─────────────────┘               │
└─────────────────────┼───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│               EXECUTION HANDLER LAYER (You are here!)               │
│  ┌──────────────────────────────────────────────────────┐          │
│  │  UltraLowLatencyExecutionHandler                     │          │
│  │  • Multi-exchange routing                            │          │
│  │  • Nano-optimized (CPU pinning, memory pools)       │          │
│  │  • Circuit breakers & position tracking             │          │
│  │  • Performance monitoring                            │          │
│  └──────────────────┬──────────────────────────────────┘          │
│                     │                                               │
│         ┌───────────┴────────────────────────┐                     │
│         │    ExchangeFactory::create_connector()                   │
│         │    (Routes to appropriate connector)                     │
│         └───────────┬────────────────────────┘                     │
│                     │                                               │
│      ┌──────────────┼──────────────┬─────────────────┐            │
│      │              │              │                 │            │
│      ▼              ▼              ▼                 ▼            │
│ ┌─────────┐   ┌─────────┐   ┌──────────┐    ┌──────────┐        │
│ │ Kraken  │   │ Binance │   │  Cetus   │    │ DeepBook │        │
│ │Connector│   │Connector│   │Connector │    │Connector │        │
│ │ (CEX)   │   │ (CEX)   │   │ (DEX)    │    │ (DEX)    │        │
│ │         │   │         │   │          │    │          │        │
│ │ WS/REST │   │ WS/REST │   │ RPC/PTB  │    │ RPC/PTB  │        │
│ └────┬────┘   └────┬────┘   └────┬─────┘    └────┬─────┘        │
└──────┼─────────────┼─────────────┼───────────────┼───────────────┘
       │             │             │               │
       ▼             ▼             ▼               ▼
┌────────────┐ ┌────────────┐ ┌─────────────┐ ┌─────────────┐
│  Kraken    │ │  Binance   │ │  SUI RPC    │ │  SUI RPC    │
│  Order API │ │  Order API │ │  (Cetus)    │ │  (DeepBook) │
│            │ │            │ │             │ │             │
│  • 2.33μs  │ │  • ~500μs  │ │  • ~400ms   │ │  • ~400ms   │
│  • 313K/s  │ │  • ~5K/s   │ │  • ~100/s   │ │  • ~100/s   │
└────────────┘ └────────────┘ └─────────────┘ └─────────────┘
```

## Signal Flow (End-to-End)

### 1. Signal Generation
```rust
// Strategy creates a signal
let signal = Signal {
    id: "arb_001",
    strategy_id: "cross_venue_arbitrage",
    action: SignalAction::BuyLimit,
    symbol: "SUI/USDC",
    exchange: "DeepBook",  // ← Specifies DEX
    quantity: 0.1,
    price: Some(2.50),
    timestamp: nano_timestamp(),
    confidence: 0.95,
    metadata: HashMap::new(),
};
```

### 2. Signal Dispatch
```rust
// UltraFastSignalDispatcher routes to execution handler
dispatcher.dispatch_signal(signal);
// → Routed via lock-free queue
// → SIMD batch processing
// → Sent to execution_sender channel
```

### 3. Execution Handler Processing
```rust
// UltraLowLatencyExecutionHandler receives signal
execution_handler.execute_order(&signal).await;

// Internal flow:
// 1. Determine exchange: signal.exchange = "DeepBook"
// 2. Get connector: connectors.get("DeepBook")
// 3. Execute order: connector.execute_order(signal)
// 4. Record metrics, update positions, log
```

### 4. DEX Connector Execution
```rust
// DeepBookConnector processes the order
impl DexConnector for DeepBookConnector {
    async fn execute_swap(&self, signal: &Signal) -> Result<DexExecutionResult> {
        // 1. Parse signal → Extract price, quantity, side
        let is_buy = matches!(signal.action, SignalAction::BuyLimit);
        let price = signal.price.unwrap();
        
        // 2. Get pool address
        let pool_id = self.get_pool_id("SUI", "USDC")?;
        
        // 3. Build PTB (Programmable Transaction Block)
        let tx_digest = self.build_and_execute_limit_order(
            wallet, &pool_id, price, quantity, is_buy, gas_price
        ).await?;
        
        // 4. Return result
        Ok(DexExecutionResult {
            status: ExecutionStatus::Pending,  // Limit order
            tx_hash: tx_digest,
            gas_used: 30_000_000,  // ~0.03 SUI
            latency_ns: 420_000_000,  // ~420ms
            ...
        })
    }
}
```

## Key Integration Points

### 1. Signal Structure (Unified Across CEX/DEX)
```rust
// From ultra_signal crate
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Signal {
    pub id: u64,
    pub strategy_id: u16,
    pub timestamp_ns: u64,
    pub symbol_hash: u64,
    pub exchange_id: u8,       // Kraken=2, Cetus=50, DeepBook=53
    pub action: SignalAction,  // Buy, Sell, BuyLimit, SellLimit
    pub side: OrderSide,       // Buy/Sell
    pub quantity: f64,
    pub price: f64,            // NaN = market order
    pub confidence: f32,
    pub flags: u32,            // IOC, FOK, PostOnly, etc.
}
// Size: 64 bytes (fits in single cache line!)
```

**DEX-specific considerations**:
- `exchange_id`: Cetus=50, DeepBook=53 (already defined in enum)
- `price`: For DEX limit orders, price is mandatory
- `flags`: Can use IMMEDIATE_OR_CANCEL, POST_ONLY, etc.

### 2. ExchangeFactory (Auto-routing)
```rust
// From executionhandler/exchanges/mod.rs
impl ExchangeFactory {
    pub async fn create_connector(
        exchange_name: &str,
        config: ExchangeConfig
    ) -> Result<Box<dyn ExchangeConnector>> {
        match exchange_name {
            // CEX connectors
            "kraken" => Ok(Box::new(KrakenConnector::new(config).await?)),
            "binance" => Ok(Box::new(BinanceConnector::new(config).await?)),
            
            // DEX connectors (NEW!)
            "cetus" => {
                let dex_config = config.to_dex_config()?;
                let mut connector = CetusConnector::new();
                connector.initialize(dex_config).await?;
                Ok(Box::new(connector))
            }
            "deepbook" => {
                let dex_config = config.to_dex_config()?;
                let mut connector = DeepBookConnector::new();
                connector.initialize(dex_config).await?;
                Ok(Box::new(connector))
            }
            
            _ => Err(ExecutionError::Unknown("Unsupported exchange".into()))
        }
    }
}
```

### 3. DexConnector Trait (Unified Interface)
```rust
// All DEX connectors implement this trait
#[async_trait]
pub trait DexConnector: Send + Sync {
    async fn initialize(&mut self, config: DexConfig) -> Result<()>;
    async fn execute_swap(&self, signal: &Signal) -> Result<DexExecutionResult>;
    async fn get_quote(&self, token_in: &str, token_out: &str, amount: f64) -> Result<DexQuote>;
    async fn cancel_transaction(&self, tx_hash: &str) -> Result<()>;
    // ... other methods
}
```

**Seamless integration**: DEX connectors follow the same pattern as CEX connectors, so the execution handler doesn't need DEX-specific code!

## Configuration Example

### Setup DEX Connectors
```rust
// In your application startup (hostbuilder or main)
let mut execution_handler = UltraLowLatencyExecutionHandler::new().await;

// Add Kraken (existing CEX)
execution_handler.add_exchange(
    "kraken".to_string(),
    ExchangeConfig {
        api_key: env::var("KRAKEN_API_KEY")?,
        api_secret: env::var("KRAKEN_API_SECRET")?,
        endpoint: "wss://ws.kraken.com".to_string(),
        // ... other CEX config
    }
).await?;

// Add Cetus DEX (NEW!)
execution_handler.add_exchange(
    "cetus".to_string(),
    ExchangeConfig {
        exchange_type: "dex".to_string(),
        blockchain_network: "sui_devnet".to_string(),
        rpc_url: "https://fullnode.devnet.sui.io:443".to_string(),
        wallet_private_key: env::var("SUI_PRIVATE_KEY")?,
        slippage_bps: 50,  // 0.5%
        max_gas_price: 100_000,
        // ... other DEX config
    }
).await?;

// Add DeepBook DEX (NEW!)
execution_handler.add_exchange(
    "deepbook".to_string(),
    ExchangeConfig {
        exchange_type: "dex".to_string(),
        blockchain_network: "sui_devnet".to_string(),
        rpc_url: "https://fullnode.devnet.sui.io:443".to_string(),
        wallet_private_key: env::var("SUI_PRIVATE_KEY")?,
        deadline_seconds: 300,  // 5 minutes for limit orders
        // ... other DEX config
    }
).await?;
```

## Cross-Venue Arbitrage Example

### Strategy: Kraken ↔ DeepBook Arbitrage
```rust
// Strategy detects price difference
let kraken_price = 2.45;  // SUI/USDC on Kraken
let deepbook_price = 2.50;  // SUI/USDC on DeepBook
let spread_bps = ((deepbook_price - kraken_price) / kraken_price) * 10000.0;

if spread_bps > 50.0 {  // 0.5% spread
    // Generate simultaneous signals
    let signals = vec![
        Signal {
            exchange: "kraken".to_string(),
            action: SignalAction::Buy,  // Buy on Kraken (cheaper)
            symbol: "SUI/USDC".to_string(),
            quantity: 10.0,
            price: None,  // Market order for speed
            ...
        },
        Signal {
            exchange: "deepbook".to_string(),
            action: SignalAction::SellLimit,  // Sell on DeepBook (higher)
            symbol: "SUI/USDC".to_string(),
            quantity: 10.0,
            price: Some(2.49),  // Slightly below ask for fast fill
            ...
        },
    ];
    
    // Execute both legs simultaneously
    let results = execution_handler.execute_batch_orders(&signals).await?;
    
    // Profit calculation:
    // Buy 10 SUI @ $2.45 on Kraken = $24.50
    // Sell 10 SUI @ $2.49 on DeepBook = $24.90
    // Gross profit: $0.40
    // Fees: Kraken (0.16%) + DeepBook (0.05%) + Gas (~$0.03) ≈ $0.08
    // Net profit: $0.32 (1.3% ROI on $24.50 capital)
}
```

## Performance Characteristics

### Latency Breakdown (End-to-End)

**CEX Path (Kraken)**:
```
Strategy → Dispatcher → ExecutionHandler → Kraken
  100ns       2.3μs          500ns          2.33μs
                                           
Total: ~5μs (microseconds)
```

**DEX Path (DeepBook)**:
```
Strategy → Dispatcher → ExecutionHandler → DeepBook → SUI Network
  100ns       2.3μs          500ns          420ms        ~400ms

Total: ~420ms (milliseconds)
```

**Why the difference?**
- **CEX (Kraken)**: In-memory order matching, direct connection
- **DEX (DeepBook)**: Blockchain consensus, transaction finality, network propagation

### Throughput

| Exchange | Latency | Throughput | Use Case |
|----------|---------|------------|----------|
| Kraken (CEX) | 2.33μs | 313K orders/sec | Market making, scalping |
| Cetus (DEX AMM) | ~450ms | ~100 tx/sec | Instant swaps, large orders |
| DeepBook (DEX CLOB) | ~420ms | ~100 tx/sec | Limit orders, patient fills |

### When to Use Each Exchange

**Kraken (CEX)** - Ultra-fast execution:
- ✅ High-frequency market making
- ✅ Scalping strategies (millisecond edges)
- ✅ Large volume (deep liquidity)
- ❌ Custodial risk
- ❌ KYC/compliance required

**Cetus (DEX AMM)** - Instant swaps:
- ✅ Market orders (instant fill)
- ✅ Large swaps (concentrated liquidity)
- ✅ No custody, self-custodial
- ❌ Slippage on large orders
- ❌ 30 bps fees (vs 5 bps on CLOB)

**DeepBook (DEX CLOB)** - Patient orders:
- ✅ Limit orders (exact price)
- ✅ Low fees (5 bps maker)
- ✅ No impermanent loss
- ✅ Self-custodial
- ❌ May not fill immediately
- ❌ Requires blockchain confirmation

## Database Integration

All executions (CEX + DEX) are automatically persisted:

```rust
// Automatic logging by ExecutionHandler
pub struct ExecutionData {
    pub order_id: String,
    pub exchange: String,         // "kraken", "cetus", "deepbook"
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub price: f64,
    pub fee: f64,
    pub status: String,
    pub executed_at: DateTime<Utc>,
    pub latency_ns: u64,
}

// DEX-specific fields in metadata
pub struct DexExecutionResult {
    pub base: ExecutionResult,  // Standard fields
    pub tx_hash: String,        // Blockchain transaction
    pub block_number: Option<u64>,
    pub gas_used: u64,          // In MIST
    pub gas_price: u64,
    pub gas_cost_native: f64,   // In SUI
    pub actual_slippage_bps: u16,
    pub mev_protected: bool,
    pub confirmations: u8,
}
```

## Monitoring & Metrics

The system tracks DEX performance alongside CEX:

```rust
// Performance Monitor (existing)
performance_monitor.record_latency("execution", "order_placement", latency_ns).await;
performance_monitor.record_error("execution", "order_placement", error_msg).await;

// DEX-specific metrics
performance_monitor.record_gas_cost("cetus", "swap", gas_used).await;
performance_monitor.record_slippage("cetus", "SUI-USDC", actual_slippage_bps).await;
performance_monitor.record_blockchain_latency("sui", finality_ms).await;
```

## Position Tracking

Unified position tracking across CEX and DEX:

```rust
// PositionTracker (existing)
position_tracker.update_fill(
    &signal.symbol,
    exchange_name,    // "kraken" or "deepbook"
    fill_quantity,
    fill_price,
    fill.fee
);

// Get consolidated position across all venues
let total_sui_position = 
    position_tracker.get_position("SUI/USDC", "kraken").quantity +
    position_tracker.get_position("SUI/USDC", "cetus").quantity +
    position_tracker.get_position("SUI/USDC", "deepbook").quantity;
```

## Circuit Breakers

DEX connectors inherit the same circuit breaker protection:

```rust
// Circuit Breaker Manager (existing)
// - Automatic halt on error thresholds
// - Exchange-specific limits
// - Rate limiting
// - Health checks

// Works identically for DEX:
circuit_breaker.check_can_execute("deepbook").await?;
```

## Testing Integration

Your existing test infrastructure works with DEX:

```rust
// Unit tests
#[tokio::test]
async fn test_cross_venue_execution() {
    let mut handler = UltraLowLatencyExecutionHandler::new().await;
    handler.add_exchange("kraken", kraken_config).await?;
    handler.add_exchange("cetus", cetus_config).await?;
    
    let signal = Signal {
        exchange: "cetus".to_string(),
        action: SignalAction::Buy,
        ...
    };
    
    let result = handler.execute_order(&signal).await?;
    assert_eq!(result.status, ExecutionStatus::Filled);
}
```

## Summary: Why This Works Seamlessly

1. **Unified Signal Interface**: DEX uses the same `Signal` struct as CEX
2. **Trait-Based Architecture**: `DexConnector` trait mirrors `ExchangeConnector`
3. **Factory Pattern**: `ExchangeFactory` handles routing automatically
4. **Async All the Way**: Both CEX and DEX use `async fn` for execution
5. **Performance Monitoring**: Same metrics collection for all venues
6. **Position Tracking**: Unified across CEX and DEX
7. **Database Persistence**: All executions logged identically
8. **Circuit Breakers**: Same protection mechanisms

## Next Steps

To start using DEX in production:

1. **Test on devnet** (current phase):
   ```bash
   cargo run --example test_dex_devnet -- both
   ```

2. **Add to your strategy**:
   ```rust
   // In your arbitrage strategy
   if price_diff > threshold {
       dispatcher.dispatch_signal(cex_signal);
       dispatcher.dispatch_signal(dex_signal);
   }
   ```

3. **Monitor performance**:
   ```rust
   let metrics = execution_handler.get_metrics("deepbook").await;
   println!("DEX avg latency: {}ms", metrics.avg_latency_ms);
   ```

4. **Scale gradually**:
   - Start with small amounts (0.01 SUI)
   - Monitor gas costs
   - Tune slippage parameters
   - Increase size as confidence grows

The architecture is **ready** - DEX connectors plug into your existing ultra-fast HFT infrastructure with zero changes to upstream components! 🚀
