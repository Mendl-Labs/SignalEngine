# SignalEngine

**Institutional-Grade High-Frequency Trading Engine**

A production-ready trading engine designed for sub-millisecond execution with lock-free architecture, comprehensive risk controls, and support for both centralized (CEX) and decentralized (DEX) exchanges.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Performance](#performance)
- [Crate Reference](#crate-reference)
- [Risk Controls](#risk-controls)
- [Exchange Connectors](#exchange-connectors)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Deployment](#deployment)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

---

## Overview

SignalEngine is the real-time trading execution layer of the TradingPlatform. It:

1. **Subscribes** to normalized market data from MessageBroker (published by DataEngine)
2. **Generates signals** using loaded strategies with SIMD-optimized calculations
3. **Validates orders** through multiple risk control layers
4. **Executes trades** on CEX (Kraken) and DEX (Cetus, DeepBook on Sui)
5. **Persists state** via Write-Ahead Log (WAL) for crash recovery
6. **Reports metrics** to Prometheus/Grafana for monitoring

### Key Characteristics

| Attribute | Value |
|-----------|-------|
| **End-to-End Latency** | &lt;1ms (signal to order) |
| **Throughput** | 50K+ operations/second |
| **Architecture** | Lock-free, zero-copy, SIMD-optimized |
| **Risk Controls** | Kill switch, fat-finger, circuit breakers |
| **Persistence** | WAL with graceful shutdown integration |
| **Exchanges** | Kraken (CEX), Cetus/DeepBook (Sui DEX) |

---

## Architecture

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              SIGNAL ENGINE                                       │
│                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                          MESSAGE BROKER                                   │  │
│  │              (market.data.{exchange}.trades / .level3)                   │  │
│  └────────────────────────────────┬─────────────────────────────────────────┘  │
│                                   │                                             │
│                                   ▼                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │                       DATA HANDLER                                      │    │
│  │     Subscribes to MessageBroker, maintains local orderbook replicas     │    │
│  │                  ORDERBOOKS: DashMap<(exchange, symbol), Orderbook>     │    │
│  └────────────────────────────────┬───────────────────────────────────────┘    │
│                                   │                                             │
│                                   ▼                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │                    STRATEGY MANAGER                                     │    │
│  │    Loads strategies from DB, routes market data, generates signals      │    │
│  │    Strategies: Momentum, MeanReversion, PortfolioMixed                  │    │
│  └────────────────────────────────┬───────────────────────────────────────┘    │
│                                   │ Signal                                      │
│                                   ▼                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │                    EXECUTION HANDLER                                    │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │    │
│  │  │              PRE-EXECUTION RISK CONTROLS                         │   │    │
│  │  │                                                                  │   │    │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │   │    │
│  │  │  │ KILL SWITCH  │─▶│ FAT FINGER   │─▶│ CIRCUIT BREAKER (v2)   │ │   │    │
│  │  │  │   (atomic)   │  │  (limits)    │  │ (lock-free, 5 = trip)  │ │   │    │
│  │  │  └──────────────┘  └──────────────┘  └────────────────────────┘ │   │    │
│  │  │         │                 │                      │               │   │    │
│  │  │         ▼                 ▼                      ▼               │   │    │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │   │    │
│  │  │  │ RATE LIMITER │  │ BACKPRESSURE │  │ POSITION LIMITS        │ │   │    │
│  │  │  └──────────────┘  └──────────────┘  └────────────────────────┘ │   │    │
│  │  └─────────────────────────────────────────────────────────────────┘   │    │
│  │                                   │                                     │    │
│  │                                   ▼                                     │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │    │
│  │  │                 EXCHANGE CONNECTORS                              │   │    │
│  │  │                                                                  │   │    │
│  │  │   ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐   │   │    │
│  │  │   │    KRAKEN    │  │    CETUS     │  │     DEEPBOOK       │   │   │    │
│  │  │   │    (CEX)     │  │  (Sui DEX)   │  │    (Sui DEX)       │   │   │    │
│  │  │   │  REST + WS   │  │    PTB       │  │      PTB           │   │   │    │
│  │  │   └──────────────┘  └──────────────┘  └────────────────────┘   │   │    │
│  │  └─────────────────────────────────────────────────────────────────┘   │    │
│  │                                   │                                     │    │
│  │                                   ▼                                     │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │    │
│  │  │                 PERSISTENCE & AUDIT                              │   │    │
│  │  │                                                                  │   │    │
│  │  │   ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐   │   │    │
│  │  │   │  ORDER WAL   │  │ POSITION     │  │       TCA          │   │   │    │
│  │  │   │ (crash safe) │  │ TRACKER      │  │ (cost analysis)    │   │   │    │
│  │  │   └──────────────┘  └──────────────┘  └────────────────────┘   │   │    │
│  │  │                                                                  │   │    │
│  │  │   ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐   │   │    │
│  │  │   │    AUDIT     │  │   ALERTS     │  │   PROMETHEUS       │   │   │    │
│  │  │   │   (trail)    │  │  (webhooks)  │  │    (metrics)       │   │   │    │
│  │  │   └──────────────┘  └──────────────┘  └────────────────────┘   │   │    │
│  │  └─────────────────────────────────────────────────────────────────┘   │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │                    GRACEFUL SHUTDOWN                                    │    │
│  │         Coordinates WAL flush, position sync, and clean exit           │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Component Interaction

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ DataEngine  │────▶│MessageBroker│────▶│SignalEngine │
│ (normalizes)│     │  (pub/sub)  │     │ (executes)  │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                    ┌──────────────────────────┼──────────────────────────┐
                    │                          │                          │
                    ▼                          ▼                          ▼
             ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
             │   Kraken    │           │   Cetus     │           │  DeepBook   │
             │    API      │           │  (Sui PTB)  │           │  (Sui PTB)  │
             └─────────────┘           └─────────────┘           └─────────────┘
```

---

## Performance

### Latency Benchmarks

| Component | P50 | P99 | P99.9 |
|-----------|-----|-----|-------|
| Signal Generation | 45μs | 75μs | 100μs |
| Risk Validation | 5μs | 10μs | 15μs |
| Order Routing | 85μs | 120μs | 150μs |
| Exchange Submit | 400μs | 600μs | 800μs |
| **End-to-End** | **535μs** | **805μs** | **1065μs** |

### Optimization Techniques

| Technique | Impact | Implementation |
|-----------|--------|----------------|
| **Lock-Free Structures** | -30% contention | `DashMap`, atomic operations, `crossbeam` |
| **Zero-Copy Messaging** | -80% memory overhead | `Arc<Signal>`, ring buffers |
| **SIMD Processing** | +40% throughput | AVX2 batch price calculations |
| **Memory Pools** | -25% allocation time | Thread-local pre-allocated buffers |
| **Cache Alignment** | -25% false sharing | 64-byte aligned atomics |
| **RDTSC Timestamps** | ~10ns overhead | Hardware timestamp counter |

### Memory Usage

| State | Memory |
|-------|--------|
| Baseline | ~500MB |
| Per 1K Strategies | +50MB |
| Per 1M Signals/day | +200MB |
| Peak Load | ~2GB |

---

## Crate Reference

### Workspace Structure

```
SignalEngine/
├── program/                    # Main binary entrypoint
├── crates/
│   ├── core/                   # Shared low-level primitives
│   ├── config/                 # Configuration management
│   ├── signal/                 # Signal type definitions
│   ├── orderbook/              # Orderbook data structures
│   ├── portfolio/              # Portfolio state management
│   │
│   ├── datahandler/            # MessageBroker subscriber, local orderbook cache
│   ├── signalgenerator/        # SIMD-optimized signal generation
│   ├── signaldispatcher/       # Signal routing and batching
│   ├── strategyhandler/        # Strategy execution orchestration
│   ├── strategyloader/         # Database strategy loading
│   ├── smartorderrouter/       # Intelligent order routing
│   ├── portfoliohandler/       # Portfolio management
│   ├── exchangemetricaggregator/ # Exchange metrics collection
│   │
│   ├── executionhandler/       # Order execution (35+ modules)
│   │   ├── core/               # ExchangeConnector trait, types
│   │   ├── exchanges/          # Kraken, Cetus, DeepBook connectors
│   │   │   └── dex/            # DEX-specific implementations
│   │   ├── risk_controls.rs    # Kill switch, position limits
│   │   ├── fat_finger.rs       # Fat-finger protection
│   │   ├── circuit_breaker_v2.rs # Lock-free circuit breaker
│   │   ├── order_wal.rs        # Write-ahead log
│   │   ├── graceful_shutdown.rs # Coordinated shutdown
│   │   ├── hot_config.rs       # Runtime config reload
│   │   ├── alerts.rs           # Webhook notifications
│   │   ├── tca.rs              # Transaction cost analysis
│   │   ├── fill_probability.rs # ML fill prediction
│   │   ├── multi_leg.rs        # OCO, bracket orders
│   │   └── ...
│   │
│   └── hostbuilder/            # Service orchestration
│
├── k8s/                        # Kubernetes Helm charts
├── scripts/                    # Build and deployment scripts
└── tests/                      # Integration tests
```

### Core Crate (`signalengine-core`)

Low-level primitives shared across all crates:

| Module | Purpose |
|--------|---------|
| `lock_free.rs` | `LockFreeHashMap`, `LockFreeStack` |
| `cache_aligned.rs` | 64-byte aligned atomics |
| `memory_pool.rs` | Thread-local memory pools |
| `zero_copy.rs` | `ZeroCopyChannel`, `SignalArena` |
| `simd.rs` | AVX2 batch operations |
| `rdtsc.rs` | Hardware timestamps |
| `memory_ordering.rs` | Precise memory barriers |

### ExecutionHandler Crate (35+ modules)

The most critical crate for production trading:

#### Risk Control Layer
| Module | Purpose |
|--------|---------|
| `risk_controls.rs` | Global `KILL_SWITCH`, position limits |
| `fat_finger.rs` | Max order size/notional limits (default: $10,000) |
| `circuit_breaker_v2.rs` | Lock-free failure detection (5 failures = trip) |
| `backpressure.rs` | Adaptive flow control |
| `rate_limiter.rs` | Exchange rate limit compliance |
| `validation.rs` | Order validation rules |

#### Exchange Connectors
| Module | Exchange | Type | Features |
|--------|----------|------|----------|
| `kraken.rs` | Kraken | CEX | HMAC-SHA512, edit orders, WebSocket |
| `cetus.rs` | Cetus | Sui DEX | PTB execution, pool discovery |
| `deepbook.rs` | DeepBook | Sui DEX | PTB execution, order book |
| `sui_wallet.rs` | - | Utility | Sui wallet management |
| `sui_ptb.rs` | - | Utility | Programmable Transaction Blocks |

#### Persistence & Audit
| Module | Purpose |
|--------|---------|
| `order_wal.rs` | Write-ahead log for crash recovery |
| `position_tracker.rs` | Real-time position state |
| `tca.rs` | Transaction Cost Analysis (slippage, VWAP, TWAP) |
| `audit.rs` | Kill switch audit trail |
| `reconciliation.rs` | Exchange state synchronization |

#### Operations
| Module | Purpose |
|--------|---------|
| `graceful_shutdown.rs` | Coordinated shutdown with WAL flush |
| `hot_config.rs` | Runtime config reload without restart |
| `alerts.rs` | Webhook notifications (Slack, PagerDuty) |
| `prometheus_metrics.rs` | Metrics export |
| `secrets.rs` | Credential management |

#### Advanced Features
| Module | Purpose |
|--------|---------|
| `multi_leg.rs` | OCO, bracket orders, spreads |
| `fill_probability.rs` | ML-based fill prediction |
| `latency_optimizer.rs` | Adaptive routing optimization |
| `bounded_dlq.rs` | Dead letter queue with bounds |
| `chaos.rs` | Chaos engineering for testing |

### StrategyLoader Crate

Strategy management and execution:

| Type | Purpose |
|------|---------|
| `StrategyInstance` | Loaded strategy with parameters |
| `StrategyType` | `Momentum`, `MeanReversion`, `PortfolioMixed` |
| `StrategyManager` | Orchestrates loading, routing, execution |
| `SignalStore` | Tracks generated signals with lifecycle |
| `PortfolioStrategy` | Trait for strategy implementations |

---

## Risk Controls

### Kill Switch

The global kill switch is the **first check** in every order path:

```rust
use signalengine::risk_controls::KILL_SWITCH;

// In every execute_order():
if KILL_SWITCH.is_triggered() {
    return Err(ExecutionError::KillSwitchActive);
}

// Trigger manually or automatically:
KILL_SWITCH.trigger(KillReason::MaxDrawdown);
KILL_SWITCH.trigger(KillReason::DailyLossLimit);

// Reset (requires manual intervention):
KILL_SWITCH.reset();
```

**Kill Reasons:**
- `Manual` - Operator triggered
- `MaxDrawdown` - Portfolio drawdown exceeded
- `DailyLossLimit` - Daily loss limit hit
- `RateLimit` - Exchange rate limit exceeded
- `PositionLimit` - Position size violated
- `SystemError` - Internal system error
- `ExchangeError` - Exchange connectivity issue
- `Reconciliation` - Position mismatch detected

### Fat-Finger Protection

Prevents accidental large orders:

```rust
let config = FatFingerConfig {
    max_order_size: 1.0,           // Max 1 BTC per order
    max_order_notional: 10_000.0,  // Max $10,000 per order
    max_daily_notional: 100_000.0, // Max $100,000 per day
    max_position_notional: 50_000.0, // Max $50,000 position
};
```

### Circuit Breaker (v2)

Lock-free circuit breaker using atomics:

```rust
// Automatic tripping after 5 consecutive failures
let breaker = CircuitBreakerV2::new(5, Duration::from_secs(60));

// State machine: Closed -> Open -> HalfOpen -> Closed
match breaker.state() {
    CircuitState::Closed => { /* Normal operation */ }
    CircuitState::Open => { /* Reject all requests */ }
    CircuitState::HalfOpen => { /* Allow probe request */ }
}
```

### Position Limits

```rust
let limits = PositionLimits {
    max_position_size: 10.0,        // 10 BTC max per symbol
    max_position_value: 500_000.0,  // $500k max per position
    max_portfolio_value: 2_000_000.0, // $2M total
    max_open_positions: 20,
    max_order_size: 1.0,            // 1 BTC max per order
    max_order_value: 50_000.0,      // $50k max per order
};
```

---

## Exchange Connectors

### Kraken (CEX)

Full-featured centralized exchange connector:

```rust
use signalengine::exchanges::KrakenConnector;

let connector = KrakenConnector::new(
    api_key,
    api_secret,
    Some(10), // Connection pool size
).await?;

// Execute order (kill switch checked internally)
let result = connector.execute_order(&signal).await?;

// Edit order (Kraken's atomic cancel+replace)
let edited = connector.edit_order(
    &order_id,
    EditOrderParams {
        new_price: Some(50000.0),
        new_quantity: Some(0.5),
    }
).await?;

// Cancel order
connector.cancel_order(&order_id, "BTC/USD").await?;
```

**Features:**
- HMAC-SHA512 request signing
- Connection pooling
- WebSocket for real-time updates
- Edit order support (atomic cancel+replace)
- Memory pools for zero-allocation hot paths
- SIMD metrics calculation

### Cetus (Sui DEX)

Sui blockchain DEX using Programmable Transaction Blocks:

```rust
use signalengine::exchanges::dex::CetusConnector;

let connector = CetusConnector::new(
    sui_client,
    wallet,
    CetusConfig::mainnet(),
).await?;

// Swap execution
let result = connector.execute_swap(
    pool_id,
    amount_in,
    min_amount_out,
    a_to_b, // Direction
).await?;

// Pool discovery
let pools = connector.discover_pools("SUI", "USDC").await?;
```

**Features:**
- PTB (Programmable Transaction Block) construction
- Pool discovery and routing
- Slippage protection
- Gas estimation

### DeepBook (Sui DEX)

Sui's native order book DEX:

```rust
use signalengine::exchanges::dex::DeepBookConnector;

let connector = DeepBookConnector::new(
    sui_client,
    wallet,
    DeepBookConfig::mainnet(),
).await?;

// Place limit order
let result = connector.place_limit_order(
    pool_id,
    price,
    quantity,
    is_bid,
).await?;

// Cancel order
connector.cancel_order(pool_id, order_id).await?;
```

### Deprecated Connectors

The following are **stub implementations** with runtime deprecation warnings:

- `UniswapV3Connector` - EVM DEX (not maintained)
- `JupiterConnector` - Solana DEX (not maintained)

---

## Quick Start

### Prerequisites

- Rust 1.75+ (stable)
- 8GB+ RAM
- Linux recommended (for best performance)
- PostgreSQL (for strategy storage)
- MessageBroker running (for market data)

### Build

```bash
cd SignalEngine

# Development build
cargo build --workspace

# Release build (optimized)
cargo build --workspace --release

# Ultra-optimized with native CPU features
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### Run

```bash
# Development
cargo run --bin program

# Production
cargo run --bin program --release

# With environment variables
KRAKEN_API_KEY="..." \
KRAKEN_SECRET_KEY="..." \
DATABASE_URL="postgresql://..." \
cargo run --bin program --release
```

### Test

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p executionhandler

# Integration tests
cargo test --test integration_test

# With output
cargo test -- --nocapture
```

---

## Configuration

### Environment Variables

```bash
# Required
export KRAKEN_API_KEY="your_api_key"
export KRAKEN_SECRET_KEY="your_secret_key"
export DATABASE_URL="postgresql://user:pass@localhost/trading"
export MESSAGE_BROKER_URL="tcp://localhost:9000"

# Optional
export LOG_LEVEL="info"
export CPU_AFFINITY="true"
export ENABLE_SIMD="true"
export WAL_PATH="/var/lib/signalengine/wal"
export METRICS_PORT="9090"

# Sui DEX (if using)
export SUI_WALLET_PATH="/path/to/wallet.keystore"
export SUI_RPC_URL="https://fullnode.mainnet.sui.io"
```

### Production Configuration

**config/production.yaml**:
```yaml
engine:
  max_threads: 16
  worker_threads: 8
  cpu_affinity: true
  high_priority: true

performance:
  enable_simd: true
  use_rdtsc: true
  lock_free_mode: true
  batch_size: 1000

risk:
  kill_switch_enabled: true
  fat_finger:
    max_order_notional: 10000.0
    max_daily_notional: 100000.0
  circuit_breaker:
    failure_threshold: 5
    reset_timeout_secs: 60
  position_limits:
    max_position_value: 500000.0
    max_portfolio_value: 2000000.0

persistence:
  wal_enabled: true
  wal_path: /var/lib/signalengine/wal
  wal_sync_interval_ms: 100
  wal_max_size_mb: 1024

alerts:
  slack_webhook: "https://hooks.slack.com/..."
  pagerduty_key: "..."
  alert_on_kill_switch: true
  alert_on_circuit_breaker: true

exchanges:
  kraken:
    enabled: true
    api_key: "${KRAKEN_API_KEY}"
    secret_key: "${KRAKEN_SECRET_KEY}"
    rate_limit_per_second: 10
    
  cetus:
    enabled: true
    rpc_url: "https://fullnode.mainnet.sui.io"
    
  deepbook:
    enabled: true
    rpc_url: "https://fullnode.mainnet.sui.io"
```

### Hot Configuration Reload

Runtime config changes without restart:

```bash
# Send SIGHUP to reload config
kill -HUP $(pidof signalengine)

# Or via API
curl -X POST http://localhost:8080/admin/reload-config
```

Supported hot-reload fields:
- Risk limits (fat-finger, position limits)
- Alert thresholds
- Rate limits
- Log levels

---

## API Reference

### ExchangeConnector Trait

All exchange connectors implement this trait:

```rust
#[async_trait]
pub trait ExchangeConnector: Send + Sync {
    /// Execute a trading signal
    async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError>;
    
    /// Execute multiple signals in batch
    async fn execute_batch_orders(&self, signals: &[Signal]) -> Vec<Result<ExecutionResult, ExecutionError>>;
    
    /// Cancel an existing order
    async fn cancel_order(&self, order_id: &str, symbol: &str) -> Result<bool, ExecutionError>;
    
    /// Edit an existing order (if supported)
    async fn edit_order(&self, order_id: &str, params: EditOrderParams) -> Result<ExecutionResult, ExecutionError>;
    
    /// Health check
    async fn health_check(&self) -> Result<bool, ExecutionError>;
    
    /// Exchange name
    fn exchange_name(&self) -> &str;
    
    /// Supported trading pairs
    fn supported_symbols(&self) -> &[String];
    
    /// Get exchange rate limits
    fn get_limits(&self) -> ExchangeLimits;
    
    /// Validate order before submission
    fn validate_order(&self, signal: &Signal) -> Result<(), ExecutionError>;
}
```

### ExecutionResult

```rust
pub struct ExecutionResult {
    pub order_id: String,
    pub exchange_order_id: Option<String>,
    pub exchange: String,
    pub status: ExecutionStatus,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub avg_fill_price: f64,
    pub total_fees: f64,
    pub fills: Vec<ExecutionFill>,
    pub reject_reason: Option<String>,
    pub submitted_at: u128,        // Nanosecond timestamp
    pub updated_at: u128,
    pub latency_ns: u64,
    pub exchange_timestamp_ns: Option<u64>, // MiFID II compliance
    pub exchange_sequence: Option<u64>,
}

pub enum ExecutionStatus {
    Pending,
    Submitted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
}
```

### Signal Type

```rust
pub struct Signal {
    pub id: String,
    pub strategy_id: Uuid,
    pub symbol: String,
    pub exchange: String,
    pub action: SignalAction,
    pub quantity: f64,
    pub price: Option<f64>,
    pub confidence: f64,
    pub timestamp_ms: i64,
    pub metadata: HashMap<String, Value>,
}

pub enum SignalAction {
    Buy,
    Sell,
    BuyLimit,
    SellLimit,
    BuyStop,
    SellStop,
}
```

---

## Deployment

### Docker

```dockerfile
FROM rust:1.75-slim as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/program /usr/local/bin/signalengine
EXPOSE 8080 9090
CMD ["signalengine"]
```

```bash
# Build and run
docker build -t signalengine:latest .
docker run -d \
  -e KRAKEN_API_KEY="..." \
  -e KRAKEN_SECRET_KEY="..." \
  -e DATABASE_URL="..." \
  -p 8080:8080 \
  -p 9090:9090 \
  signalengine:latest
```

### Kubernetes

```bash
cd k8s/signal-engine-helm

# Development
helm upgrade --install signalengine . \
  -f values-dev.yaml \
  --namespace signalengine-dev \
  --create-namespace

# Production
helm upgrade --install signalengine . \
  -f values-prod.yaml \
  --namespace signalengine \
  --set-string secrets.kraken.apiKey="$KRAKEN_API_KEY" \
  --set-string secrets.kraken.secretKey="$KRAKEN_SECRET_KEY"
```

Key Helm values:
- `replicaCount` - Number of pods
- `resources.limits.memory` - Memory limit (default: 4Gi)
- `config.risk.*` - Risk control settings
- `secrets.*` - Exchange credentials

### Monitoring

SignalEngine exports Prometheus metrics on port 9090:

```
# HELP signalengine_orders_submitted_total Total orders submitted
# TYPE signalengine_orders_submitted_total counter
signalengine_orders_submitted_total{exchange="kraken"} 1234

# HELP signalengine_order_latency_ns Order execution latency
# TYPE signalengine_order_latency_ns histogram
signalengine_order_latency_ns_bucket{le="100000"} 500
signalengine_order_latency_ns_bucket{le="500000"} 950

# HELP signalengine_kill_switch_triggered Kill switch status
# TYPE signalengine_kill_switch_triggered gauge
signalengine_kill_switch_triggered 0

# HELP signalengine_circuit_breaker_state Circuit breaker state
# TYPE signalengine_circuit_breaker_state gauge
signalengine_circuit_breaker_state{exchange="kraken"} 0
```

---

## Development

### Adding a New Exchange Connector

1. Create connector in `crates/executionhandler/src/exchanges/`:

```rust
// my_exchange.rs
use crate::core::{ExchangeConnector, ExecutionResult, ExecutionError};

pub struct MyExchangeConnector {
    // ...
}

#[async_trait]
impl ExchangeConnector for MyExchangeConnector {
    async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
        // 1. Check kill switch FIRST
        if KILL_SWITCH.is_triggered() {
            return Err(ExecutionError::KillSwitchActive);
        }
        
        // 2. Validate order
        self.validate_order(signal)?;
        
        // 3. Execute
        // ...
    }
    
    // Implement other methods...
}
```

2. Register in factory (`exchanges/factory.rs`)
3. Add tests
4. Update documentation

### Running Benchmarks

```bash
# All benchmarks
cargo bench

# Specific benchmark
cargo bench --bench signal_latency

# With flamegraph
cargo flamegraph --bench signal_latency
```

### Code Style

- Zero warnings (`cargo clippy --all-targets`)
- Format with `cargo fmt`
- Document all public APIs
- Test all critical paths
- Benchmark latency-sensitive code

---

## Troubleshooting

### Kill Switch Triggered

```bash
# Check kill switch status
curl http://localhost:8080/admin/kill-switch

# View audit log
tail -f /var/log/signalengine/audit.log

# Reset (after investigation!)
curl -X POST http://localhost:8080/admin/kill-switch/reset
```

### High Latency

1. Check CPU affinity: `taskset -p $(pidof signalengine)`
2. Verify huge pages: `cat /proc/meminfo | grep Huge`
3. Check network latency: `ping api.kraken.com`
4. Review metrics: `curl localhost:9090/metrics | grep latency`

### Memory Issues

```bash
# Check memory usage
ps aux | grep signalengine

# Monitor allocations
MALLOC_CONF="prof:true" ./signalengine

# Analyze with valgrind
valgrind --tool=massif ./signalengine
```

### WAL Recovery

```bash
# List WAL files
ls -la /var/lib/signalengine/wal/

# Replay WAL (on startup, automatic)
# Manual inspection:
./signalengine --wal-inspect /var/lib/signalengine/wal/
```

---

## Related Projects

| Project | Purpose |
|---------|---------|
| **DataEngine** | Market data ingestion, normalization, TimescaleDB storage |
| **MessageBrokerEngine** | Low-latency pub/sub (176ns, 900K msg/s) |
| **BacktestingEngine** | Strategy backtesting with genetic optimization |
| **SimulationEngine** | Risk-free strategy testing |
| **LoggingEngine** | Ultra-low latency structured logging |

---

## License

Proprietary - Nwagbara Group LLC

---

## Support

For support, contact: **support@nwabaragroup.com**

---

**Built by Nwagbara Group LLC** • *Institutional-grade trading performance*

**Version**: 0.2.0  
**Rust Version**: 1.83+ (stable)  
**Last Updated**: 2026-01-17
