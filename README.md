# SignalEngine

**Institutional-Grade High-Frequency Trading Engine**

A production-ready trading engine designed for sub-millisecond execution with lock-free architecture, comprehensive risk controls, and support for both centralized (CEX) and decentralized (DEX) exchanges.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Workspace Crates](#workspace-crates)
- [Data Flow](#data-flow)
- [Risk Controls](#risk-controls)
- [Exchange Connectors](#exchange-connectors)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Performance](#performance)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Environment Variables](#environment-variables)

---

## Overview

SignalEngine is the real-time trading execution layer of the TradingPlatform. It:

1. **Subscribes** to market data from MessageBroker (published by DataEngine)
2. **Loads strategies** from database with optimized parameters from BacktestingEngine
3. **Generates signals** using SIMD-optimized calculations
4. **Validates orders** through multiple risk control layers
5. **Executes trades** on 10 CEX/broker presets spanning crypto, equities (Alpaca), and forex (OANDA), plus DEX (Cetus, DeepBook on Sui)
6. **Tracks positions** with real-time P&L and audit trail

### Key Characteristics

| Attribute | Value |
|-----------|-------|
| **End-to-End Latency** | <1ms (signal to order) |
| **Throughput** | 50K+ operations/second |
| **Architecture** | Lock-free, zero-copy, SIMD-optimized |
| **Risk Controls** | Kill switch, fat-finger, circuit breakers |
| **Persistence** | WAL with graceful shutdown |
| **Exchanges** | 10 CEX/broker presets (crypto, equities, forex) + Cetus/DeepBook (Sui DEX) |

---

## Architecture

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SIGNAL ENGINE                                   │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                       MESSAGE BROKER                                    │ │
│  │              (market_data.{exchange}.{symbol})                         │ │
│  └────────────────────────────────┬───────────────────────────────────────┘ │
│                                   │                                          │
│                                   ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                       DATA HANDLER                                      │ │
│  │     Subscribes to MessageBroker, maintains local orderbook replicas     │ │
│  │              ORDERBOOKS: DashMap<(exchange, symbol), Orderbook>         │ │
│  └────────────────────────────────┬───────────────────────────────────────┘ │
│                                   │                                          │
│                                   ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                    STRATEGY MANAGER                                     │ │
│  │    Loads strategies from DB, routes market data, generates signals      │ │
│  └────────────────────────────────┬───────────────────────────────────────┘ │
│                                   │ Signal                                   │
│                                   ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                    EXECUTION HANDLER                                    │ │
│  │                                                                         │ │
│  │  ┌───────────────────────────────────────────────────────────────────┐ │ │
│  │  │              PRE-EXECUTION RISK CONTROLS                           │ │ │
│  │  │                                                                    │ │ │
│  │  │  KILL SWITCH ─▶ FAT FINGER ─▶ CIRCUIT BREAKER ─▶ RATE LIMITER    │ │ │
│  │  │    (atomic)      (limits)      (lock-free)        (token bucket)  │ │ │
│  │  └───────────────────────────────────────────────────────────────────┘ │ │
│  │                              │                                          │ │
│  │                              ▼                                          │ │
│  │  ┌───────────────────────────────────────────────────────────────────┐ │ │
│  │  │                 EXCHANGE CONNECTORS                                │ │ │
│  │  │                                                                    │ │ │
│  │  │   ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │ │ │
│  │  │   │    KRAKEN    │  │    CETUS     │  │     DEEPBOOK       │     │ │ │
│  │  │   │    (CEX)     │  │  (Sui DEX)   │  │    (Sui DEX)       │     │ │ │
│  │  │   │  REST + WS   │  │    PTB       │  │      PTB           │     │ │ │
│  │  │   └──────────────┘  └──────────────┘  └────────────────────┘     │ │ │
│  │  └───────────────────────────────────────────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                    PERSISTENCE & AUDIT                                  │ │
│  │                                                                         │ │
│  │   ORDER WAL        POSITION TRACKER      TCA           PROMETHEUS      │ │
│  │   (crash safe)     (real-time P&L)       (cost)        (metrics)       │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Integration with TradingPlatform

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ DataEngine  │────▶│MessageBroker│────▶│SignalEngine │
│ (WebSocket) │     │  (pub/sub)  │     │ (executes)  │
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

## Workspace Crates

Located in `crates/`:

| Crate | Purpose |
|-------|---------|
| `core/` | Lock-free primitives, SIMD operations, cache-aligned structures |
| `datahandler/` | MessageBroker subscription, orderbook replication (DashMap) |
| `strategyloader/` | Database strategy loading from PostgreSQL |
| `strategyhandler/` | Strategy execution orchestration, signal routing |
| `signalgenerator/` | Shared `MarketData` wire type (used by `datahandler`) |
| `signaldispatcher/` | Signal routing and batching |
| `executionhandler/` | Pre-execution risk, CEX/DEX connectors |
| `portfoliohandler/` | Position tracking, P&L calculation |
| `portfolio/` | Wallet/balance state shared across handlers |
| `smartorderrouter/` | Multi-venue execution optimization |
| `exchangemetricaggregator/` | Exchange performance metrics collection |
| `orderbook/` | Lock-free orderbook implementation |
| `config/` | Configuration management |
| `hostbuilder/` | Dependency injection and service orchestration |
| `pythonbridge-worker/` | Live/paper strategy execution via an embedded Python interpreter (PyO3), run as an isolated child process per deployment so one strategy's crash/hang can't take down others |

---

## Data Flow

### Strategy Deployment Flow

```
1. BacktestingEngine optimizes strategy parameters
         │
         ▼
2. Optimized strategy saved to PostgreSQL
         │
         ▼
3. SignalEngine StrategyLoader loads strategy from DB
         │
         ▼
4. SignalEngine publishes MarketDataSubscribe to MessageBroker
   {
     "exchange": "kraken",
     "symbol": "XBTUSD",
     "data_types": ["trades", "orderbook"]
   }
         │
         ▼
5. DataEngine receives subscription, connects to exchange WebSocket
         │
         ▼
6. Real-time data flows:
   Exchange → DataEngine → MessageBroker → SignalEngine
         │
         ▼
7. SignalEngine DataHandler updates local orderbook
         │
         ▼
8. StrategyManager runs strategy logic
         │
         ▼
9. SignalGenerator produces trading signal
         │
         ▼
10. ExecutionHandler validates through risk controls
         │
         ▼
11. Exchange connector submits order
```

### Signal Processing Pipeline

```
Market Data ──▶ DataHandler ──▶ StrategyManager ──▶ SignalGenerator
                    │                │                    │
                    ▼                ▼                    ▼
              Orderbook         Strategy            SIMD-optimized
              (DashMap)         Instance            calculations
                                                         │
                                                         ▼
                                                  SignalDispatcher
                                                         │
                                                         ▼
                                                  ExecutionHandler
                                                         │
                    ┌────────────────────────────────────┼────────────────────────────────────┐
                    │                                    │                                    │
                    ▼                                    ▼                                    ▼
              Kill Switch                          Fat Finger                         Circuit Breaker
              (if triggered,                       (size/price                        (error rate
               reject all)                          limits)                            threshold)
                    │                                    │                                    │
                    └────────────────────────────────────┼────────────────────────────────────┘
                                                         │
                                                         ▼
                                                  Exchange Connector
```

---

## Risk Controls

### Pre-Execution Risk Stack

| Control | Type | Description |
|---------|------|-------------|
| **Kill Switch** | Atomic boolean | Emergency halt all trading |
| **Fat Finger** | Limits | Max order size, price deviation |
| **Circuit Breaker** | Lock-free counter | Trips after N consecutive errors |
| **Rate Limiter** | Token bucket | Exchange API rate limiting |
| **Position Limits** | Per-symbol | Maximum position per instrument |
| **Daily Loss Limit** | Aggregate | Stop trading if daily P&L exceeds limit |

### Configuration

```rust
RiskConfig {
    kill_switch_enabled: bool,
    max_order_size: f64,
    max_price_deviation_pct: f64,
    circuit_breaker_threshold: u32,  // Trip after N errors
    circuit_breaker_reset_secs: u64,
    daily_loss_limit: f64,
    position_limit_per_symbol: f64,
}
```

---

## Exchange Connectors

### CEX/Broker: 10 presets via one config-driven connector

Handled by the config-driven `GenericConnector` in `crates/executionhandler/src/exchanges/generic/`
(preset selected via `ExchangePreset::from_name`):

| Preset | Asset class |
|--------|-------------|
| Kraken, Coinbase, Binance, Binance US, Bybit, OKX, Gemini, Deribit | Crypto spot/derivatives |
| Alpaca (paper) | Equities |
| OANDA (practice) | Forex |

- **REST API** for order submission/cancellation
- **WebSocket** for execution reports
- Per-exchange auth method (Kraken/Gemini use HMAC-SHA512; see `generic/auth.rs`)
- Rate limiting compliance

### DEX: Sui Network

Located in `crates/executionhandler/src/exchanges/dex/`:

**Cetus (AMM):**
- Programmable Transaction Blocks (PTB)
- Atomic swap execution
- Liquidity pool interaction

**DeepBook (CLOB):**
- Programmable Transaction Blocks (PTB)
- Limit order placement
- Order matching on-chain

### Adding New Exchanges

1. Create connector in `crates/executionhandler/src/`
2. Implement `ExchangeConnector` trait
3. Register in SmartOrderRouter

---

## Quick Start

### Prerequisites

- Rust 1.82+
- MessageBrokerEngine running
- PostgreSQL (for strategy storage)
- Exchange API credentials

### Build

```powershell
cd SignalEngine

# Development build
cargo build --workspace

# Release with ultra optimizations
cargo build --workspace --profile release-ultra
```

### Run

```powershell
# Set environment variables
$env:MESSAGE_BROKER_URL = "tcp://localhost:9000"
$env:DATABASE_URL = "postgresql://user:pass@localhost/trading"
$env:KRAKEN_API_KEY = "your-api-key"
$env:KRAKEN_API_SECRET = "your-api-secret"

# Run SignalEngine
cargo run --bin program
```

---

## Configuration

### Main Configuration (`config/`)

```yaml
message_broker:
  url: "tcp://localhost:9000"
  market_data_prefix: "market_data"
  subscription_topic: "market_data.subscriptions"

database:
  url: "postgresql://localhost/trading"
  max_connections: 10

risk:
  kill_switch_enabled: false
  max_order_size: 10.0
  max_price_deviation_pct: 5.0
  circuit_breaker_threshold: 5
  daily_loss_limit: 10000.0

exchanges:
  kraken:
    enabled: true
    api_key_env: "KRAKEN_API_KEY"
    api_secret_env: "KRAKEN_API_SECRET"
  
  sui:
    enabled: true
    network: "mainnet"  # or "testnet", "devnet"
    wallet_path: "~/.sui/sui_config/sui.keystore"
```

---

## Performance

### Latency Benchmarks

| Operation | p50 | p99 | p99.9 |
|-----------|-----|-----|-------|
| Signal Generation | 50μs | 150μs | 500μs |
| Risk Validation | 10μs | 50μs | 100μs |
| Order Submission (CEX) | 5ms | 20ms | 50ms |
| Order Submission (DEX) | 500ms | 1s | 2s |
| End-to-End | <1ms | 5ms | 20ms |

### Key Optimizations

**Lock-Free Patterns:**
```rust
// ✅ Use DashMap for concurrent access
use dashmap::DashMap;
let orderbooks: DashMap<(String, String), Orderbook> = DashMap::new();

// ✅ Use crossbeam for queues
use crossbeam::queue::ArrayQueue;

// ❌ Avoid Mutex in hot paths
use std::sync::Mutex;  // DON'T use in latency-critical code
```

**SIMD Operations:**
```rust
use signalengine_core::simd::{price_diff, returns, sma};

// Vectorized calculations
let diffs = price_diff(&prices);
let ret = returns(&prices);
let moving_avg = sma(&prices, 20);
```

**Cache-Line Alignment:**
```rust
#[repr(C, align(64))]
pub struct CacheAlignedAtomicU64 {
    value: AtomicU64,
    _padding: [u8; 56],
}
```

---

## Kubernetes Deployment

### Helm Install

```powershell
# Development
helm upgrade --install signal-engine ./k8s/signal-engine-helm `
  -f values-dev.yaml --namespace signalengine-dev --create-namespace

# Production
helm upgrade --install signal-engine ./k8s/signal-engine-helm `
  -f values-prod.yaml --namespace signalengine `
  --set-string secrets.krakenApiKey="$KRAKEN_API_KEY" `
  --set-string secrets.krakenApiSecret="$KRAKEN_API_SECRET"
```

### Key Helm Values

| Value | Description | Default |
|-------|-------------|---------|
| `replicaCount` | Number of replicas | 2 |
| `resources.limits.memory` | Memory limit | 4Gi |
| `resources.limits.cpu` | CPU limit | 2 |
| `messageBroker.url` | MessageBroker connection | tcp://message-broker:9000 |

---

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `MESSAGE_BROKER_URL` | MessageBroker connection URL | Yes |
| `DATABASE_URL` | PostgreSQL connection URL | Yes |
| `KRAKEN_API_KEY` | Kraken API key | For CEX trading |
| `KRAKEN_API_SECRET` | Kraken API secret | For CEX trading |
| `SUI_WALLET_PATH` | Path to Sui wallet keystore | For DEX trading |
| `RUST_LOG` | Log level (info, debug, trace) | No |
| `CREDENTIAL_MODE` | How live trading gets exchange credentials: `none` (default; live disabled, paper only), `single_tenant` (public schema, one `TENANT_ID`), `multi_tenant` (opt-in, private schema, see below) | No |
| `TENANT_ID` | The one tenant served by `single_tenant`; must be unset/empty for `multi_tenant` | With `single_tenant` |
| `CREDENTIALS_ENCRYPTION_KEY` | 64 hex chars (AES-256-GCM); must match the key BacktestingEngine encrypts `exchange_credentials` with | With `single_tenant` / `multi_tenant` |

### Live-trading credentials: `CREDENTIAL_MODE`

Live deployments only run when a tenant-scoped credential provider exists; otherwise they are
rejected loudly (paper trading is unaffected). `multi_tenant` selects `MultiTenantDbProvider`
(`crates/smartorderrouter/src/tenant_provider.rs`): each live deployment signs with **its own
tenant's** `exchange_credentials` row (`tenant_id` = the deployment's tenant), never another
tenant's and never a default/first row. It exists, but:

- it is **opt-in**: never selected implicitly, and `credentialMode` stays `none` in `values.yaml` and
  `values-prod.yaml` (the chart refuses `multi_tenant` together with a `tenantId`);
- it needs the **private** schema (`exchange_credentials.tenant_id`); against the public schema, or
  without a valid `CREDENTIALS_ENCRYPTION_KEY`, SignalEngine logs an ERROR and runs as `none`;
- several enabled credentials for one tenant + exchange are an error (ambiguous), not a pick;
- the execution handler keeps one connector per exchange name, so a second tenant's live deployment
  on an exchange another tenant already holds is rejected (fail closed).

---

## Testing

```powershell
# Unit tests
cargo test --workspace

# Integration tests (requires MessageBroker + DB)
cargo test --workspace -- --ignored

# Run DEX connector tests (Sui devnet)
cargo run --example test_dex_devnet -- both

# Benchmarks
cargo bench --package signalengine-core
```

---

## Key Files

| File | Purpose |
|------|---------|
| `program/src/main.rs` | Entry point |
| `crates/core/src/lib.rs` | Lock-free primitives, SIMD |
| `crates/datahandler/src/lib.rs` | MessageBroker subscription |
| `crates/strategyhandler/src/lib.rs` | Strategy orchestration |
| `crates/executionhandler/src/lib.rs` | Risk controls, connectors |
| `crates/executionhandler/src/exchanges/generic/` | Config-driven CEX connector (Kraken and 7 others) |
| `crates/executionhandler/src/exchanges/dex/` | Sui DEX connectors |

---

## Troubleshooting

### High Latency

1. Check MessageBroker connection latency
2. Verify orderbook updates aren't backed up
3. Review risk control processing time
4. Check exchange API response times

### Order Rejections

1. Check risk control logs for rejection reason
2. Verify position limits aren't exceeded
3. Confirm exchange credentials are valid
4. Check circuit breaker status

### DEX Transaction Failures

1. Verify Sui wallet has sufficient balance
2. Check gas budget configuration
3. Review transaction simulation results
4. Confirm network (mainnet/testnet/devnet)

---

## License

Functional Source License, Version 1.1, ALv2 Future License (FSL-1.1-ALv2) — see [LICENSE](LICENSE). Free for internal use, non-commercial research/education, and professional services; converts to Apache License 2.0 two years after each version's release.
