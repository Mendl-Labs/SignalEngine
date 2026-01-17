//! Integration Tests for ExecutionHandler
//!
//! End-to-end tests using MockExchange to validate the complete execution pipeline.

use executionhandler::core::types::ExecutionStatus;

#[cfg(feature = "testing")]
use executionhandler::testing::{
    MockExchange, MockExchangeConfig, MockFillBehavior,
    TestSignal, TestScenario, TestFixtures,
    ExecutionResultExt,
};

/// Test basic buy order execution
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_basic_buy_order() {
    let config = MockExchangeConfig::default();
    let exchange = MockExchange::new(config);
    
    let signal = TestSignal::new("BTC/USD")
        .with_id("buy-1")
        .buy()
        .with_quantity(1.0)
        .with_price(50000.0)
        .build();
    
    let result = exchange.execute_signal(&signal).await.expect("Execution should succeed");
    
    result.assert()
        .is_success()
        .has_order_id()
        .has_fill_quantity(1.0, 0.01);
}

/// Test basic sell order execution
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_basic_sell_order() {
    let config = MockExchangeConfig::default();
    let exchange = MockExchange::new(config);
    
    let signal = TestSignal::new("BTC/USD")
        .with_id("sell-1")
        .sell()
        .with_quantity(0.5)
        .with_price(51000.0)
        .build();
    
    let result = exchange.execute_signal(&signal).await.expect("Execution should succeed");
    
    result.assert()
        .is_success()
        .has_order_id();
}

/// Test round-trip position (buy then sell)
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_round_trip_position() {
    let config = MockExchangeConfig {
        fill_behavior: MockFillBehavior {
            full_fill_rate: 1.0,
            partial_fill_rate: 0.0,
            reject_rate: 0.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    let scenario = TestScenario::round_trip();
    
    // Execute all signals in scenario
    for signal in &scenario.signals {
        let result = exchange.execute_signal(signal).await.expect("Should execute");
        assert!(
            matches!(result.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled),
            "Expected fill, got {:?}", result.status
        );
    }
    
    // Verify position tracking
    let history = exchange.get_execution_history().await;
    assert_eq!(history.len(), 2, "Should have 2 executions");
}

/// Test burst order handling
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_burst_order_handling() {
    let config = MockExchangeConfig {
        rate_limit_per_sec: Some(100), // Allow burst
        fill_behavior: MockFillBehavior {
            full_fill_rate: 1.0,
            partial_fill_rate: 0.0,
            reject_rate: 0.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    let scenario = TestScenario::burst_orders(20);
    let mut results = Vec::new();
    
    for signal in &scenario.signals {
        match exchange.execute_signal(signal).await {
            Ok(result) => results.push(result),
            Err(_) => {} // Rate limit rejections are expected
        }
    }
    
    // Should have executed most orders
    assert!(results.len() >= 10, "Should execute at least 10 orders, got {}", results.len());
}

/// Test latency measurement
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_latency_measurement() {
    let config = MockExchangeConfig {
        latency_us: 1000, // 1ms base latency
        latency_jitter: 0.1, // Low jitter for predictable tests
        fill_behavior: MockFillBehavior {
            full_fill_rate: 1.0,
            partial_fill_rate: 0.0,
            reject_rate: 0.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    let signal = TestSignal::new("BTC/USD")
        .buy()
        .with_quantity(1.0)
        .with_price(50000.0)
        .build();
    
    let result = exchange.execute_signal(&signal).await.expect("Should execute");
    
    // Latency should be around 1ms (1,000,000 ns) with some jitter
    // Allow generous bounds due to OS scheduling variability
    assert!(result.latency_ns > 100_000, "Latency too low: {} ns", result.latency_ns);
    assert!(result.latency_ns < 50_000_000, "Latency too high: {} ns", result.latency_ns);
}

/// Test fill behavior with guaranteed partial fills
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_partial_fills() {
    let config = MockExchangeConfig {
        fill_behavior: MockFillBehavior {
            full_fill_rate: 0.0,
            partial_fill_rate: 1.0,
            min_partial_pct: 0.5,
            max_partial_pct: 0.5,
            reject_rate: 0.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    let signal = TestSignal::new("BTC/USD")
        .buy()
        .with_quantity(2.0)
        .with_price(50000.0)
        .build();
    
    let result = exchange.execute_signal(&signal).await.expect("Should execute");
    
    assert_eq!(result.status, ExecutionStatus::PartiallyFilled);
    assert!((result.filled_quantity - 1.0).abs() < 0.01, "Expected 50% fill");
    assert!(result.remaining_quantity > 0.0);
}

/// Test fill behavior with guaranteed rejections
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_rejections() {
    let config = MockExchangeConfig {
        fill_behavior: MockFillBehavior {
            full_fill_rate: 0.0,
            partial_fill_rate: 0.0,
            reject_rate: 1.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    let signal = TestSignal::new("BTC/USD")
        .buy()
        .with_quantity(1.0)
        .with_price(50000.0)
        .build();
    
    let result = exchange.execute_signal(&signal).await.expect("Should return rejection");
    
    assert_eq!(result.status, ExecutionStatus::Rejected);
    assert!(result.reject_reason.is_some());
    assert_eq!(result.filled_quantity, 0.0);
}

/// Test disconnect/reconnect behavior
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_disconnect_reconnect() {
    let exchange = MockExchange::default_instance();
    
    let signal = TestSignal::new("BTC/USD")
        .buy()
        .with_quantity(1.0)
        .with_price(50000.0)
        .build();
    
    // Should work when connected
    assert!(exchange.execute_signal(&signal).await.is_ok());
    
    // Disconnect
    exchange.disconnect();
    assert!(exchange.execute_signal(&signal).await.is_err());
    
    // Reconnect
    exchange.reconnect();
    assert!(exchange.execute_signal(&signal).await.is_ok());
}

/// Test position tracking
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_position_tracking() {
    let config = MockExchangeConfig {
        fill_behavior: MockFillBehavior {
            full_fill_rate: 1.0,
            partial_fill_rate: 0.0,
            reject_rate: 0.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    // Buy
    let buy_signal = TestSignal::new("BTC/USD")
        .buy()
        .with_quantity(1.0)
        .with_price(50000.0)
        .build();
    exchange.execute_signal(&buy_signal).await.unwrap();
    
    let position = exchange.get_position("BTC/USD").await;
    assert!((position - 1.0).abs() < 0.01, "Expected position 1.0, got {}", position);
    
    // Sell half
    let sell_signal = TestSignal::new("BTC/USD")
        .sell()
        .with_quantity(0.5)
        .with_price(51000.0)
        .build();
    exchange.execute_signal(&sell_signal).await.unwrap();
    
    let position = exchange.get_position("BTC/USD").await;
    assert!((position - 0.5).abs() < 0.01, "Expected position 0.5, got {}", position);
}

/// Test exchange metrics
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_exchange_metrics() {
    let config = MockExchangeConfig {
        fill_behavior: MockFillBehavior {
            full_fill_rate: 0.8,
            partial_fill_rate: 0.1,
            reject_rate: 0.1,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    // Execute many orders
    for i in 0..50 {
        let signal = TestSignal::new("BTC/USD")
            .with_id(&format!("order-{}", i))
            .buy()
            .with_quantity(0.1)
            .with_price(50000.0)
            .build();
        let _ = exchange.execute_signal(&signal).await;
    }
    
    let metrics = exchange.get_metrics();
    assert_eq!(metrics.total_orders, 50, "Should have 50 total orders");
    assert!(metrics.total_fills > 0, "Should have some fills");
}

/// Test multiple symbols
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_multi_symbol() {
    let config = MockExchangeConfig {
        fill_behavior: MockFillBehavior {
            full_fill_rate: 1.0,
            partial_fill_rate: 0.0,
            reject_rate: 0.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    let scenario = TestScenario::multi_symbol();
    
    for signal in &scenario.signals {
        let result = exchange.execute_signal(signal).await.expect("Should execute");
        assert_eq!(result.status, ExecutionStatus::Filled);
    }
    
    // Verify positions for each symbol
    let btc_pos = exchange.get_position("BTC/USD").await;
    let eth_pos = exchange.get_position("ETH/USD").await;
    let sol_pos = exchange.get_position("SOL/USD").await;
    
    assert!(btc_pos > 0.0, "Should have BTC position");
    assert!(eth_pos > 0.0, "Should have ETH position");
    assert!(sol_pos > 0.0, "Should have SOL position");
}

/// Test limit order ladder
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_limit_order_ladder() {
    let config = MockExchangeConfig {
        fill_behavior: MockFillBehavior {
            full_fill_rate: 1.0,
            partial_fill_rate: 0.0,
            reject_rate: 0.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    let scenario = TestScenario::limit_ladder(5, 50000.0, 10.0);
    
    let mut fill_count = 0;
    for signal in &scenario.signals {
        let result = exchange.execute_signal(signal).await.expect("Should execute");
        if matches!(result.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled) {
            fill_count += 1;
        }
    }
    
    assert_eq!(fill_count, scenario.expected_fills, "All ladder orders should fill");
}

/// Test exchange reset
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_exchange_reset() {
    let exchange = MockExchange::default_instance();
    
    // Execute some orders
    let signal = TestSignal::new("BTC/USD")
        .buy()
        .with_quantity(1.0)
        .with_price(50000.0)
        .build();
    exchange.execute_signal(&signal).await.ok();
    exchange.execute_signal(&signal).await.ok();
    
    // Verify state exists
    let metrics = exchange.get_metrics();
    assert!(metrics.total_orders > 0);
    
    // Reset
    exchange.reset().await;
    
    // Verify state is cleared
    let metrics = exchange.get_metrics();
    assert_eq!(metrics.total_orders, 0);
    
    let history = exchange.get_execution_history().await;
    assert!(history.is_empty());
}

/// Test all standard scenarios
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_all_standard_scenarios() {
    let config = MockExchangeConfig {
        fill_behavior: MockFillBehavior {
            full_fill_rate: 1.0,
            partial_fill_rate: 0.0,
            reject_rate: 0.0,
            ..Default::default()
        },
        ..Default::default()
    };
    
    for scenario in TestFixtures::all_scenarios() {
        let exchange = MockExchange::new(config.clone());
        
        let mut fills = 0;
        for signal in &scenario.signals {
            match exchange.execute_signal(signal).await {
                Ok(result) if matches!(result.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled) => {
                    fills += 1;
                }
                _ => {}
            }
        }
        
        assert_eq!(
            fills, scenario.expected_fills,
            "Scenario '{}' expected {} fills, got {}",
            scenario.name, scenario.expected_fills, fills
        );
    }
}

/// Test stress with random signals
#[tokio::test]
#[cfg(feature = "testing")]
async fn test_stress_random_signals() {
    let config = MockExchangeConfig {
        fill_behavior: MockFillBehavior {
            full_fill_rate: 0.9,
            partial_fill_rate: 0.08,
            reject_rate: 0.02,
            ..Default::default()
        },
        ..Default::default()
    };
    let exchange = MockExchange::new(config);
    
    let signals = TestFixtures::random_signals(100);
    
    let mut success_count = 0;
    for signal in signals {
        match exchange.execute_signal(&signal).await {
            Ok(result) if matches!(result.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled) => {
                success_count += 1;
            }
            _ => {}
        }
    }
    
    // With 90% fill rate + 8% partial = 98% success, we should have > 80% success
    assert!(success_count > 80, "Expected > 80% success rate, got {}/100", success_count);
}
