//! Test Fixtures for Integration Testing
//!
//! Provides pre-built test scenarios and signal generators for common test cases.

use std::collections::HashMap;
use crate::signal::{Signal, SignalAction};
use crate::optimizations::timestamp::nano_timestamp;

/// Test signal builder for easy signal creation
#[derive(Debug, Clone)]
pub struct TestSignal {
    signal: Signal,
}

impl TestSignal {
    pub fn new(symbol: &str) -> Self {
        Self {
            signal: Signal {
                id: format!("test-{}", nano_timestamp()),
                strategy_id: "test-strategy".to_string(),
                symbol: symbol.to_string(),
                exchange: "MockExchange".to_string(),
                action: SignalAction::Buy,
                quantity: 1.0,
                price: Some(100.0),
                confidence: 0.9,
                timestamp: nano_timestamp() as u64,
                metadata: HashMap::new(),
            },
        }
    }

    pub fn with_id(mut self, id: &str) -> Self {
        self.signal.id = id.to_string();
        self
    }

    pub fn with_strategy(mut self, strategy_id: &str) -> Self {
        self.signal.strategy_id = strategy_id.to_string();
        self
    }

    pub fn with_exchange(mut self, exchange: &str) -> Self {
        self.signal.exchange = exchange.to_string();
        self
    }

    pub fn buy(mut self) -> Self {
        self.signal.action = SignalAction::Buy;
        self
    }

    pub fn sell(mut self) -> Self {
        self.signal.action = SignalAction::Sell;
        self
    }

    pub fn buy_limit(mut self) -> Self {
        self.signal.action = SignalAction::BuyLimit;
        self
    }

    pub fn sell_limit(mut self) -> Self {
        self.signal.action = SignalAction::SellLimit;
        self
    }

    pub fn with_quantity(mut self, qty: f64) -> Self {
        self.signal.quantity = qty;
        self
    }

    pub fn with_price(mut self, price: f64) -> Self {
        self.signal.price = Some(price);
        self
    }

    pub fn with_confidence(mut self, confidence: f64) -> Self {
        self.signal.confidence = confidence;
        self
    }

    pub fn build(self) -> Signal {
        self.signal
    }
}

/// Pre-built test scenarios
#[derive(Debug, Clone)]
pub struct TestScenario {
    pub name: String,
    pub description: String,
    pub signals: Vec<Signal>,
    pub expected_fills: usize,
    pub expected_rejects: usize,
}

impl TestScenario {
    /// Simple buy order scenario
    pub fn single_buy() -> Self {
        Self {
            name: "single_buy".to_string(),
            description: "A single market buy order".to_string(),
            signals: vec![
                TestSignal::new("BTC/USD")
                    .buy()
                    .with_quantity(0.1)
                    .with_price(50000.0)
                    .build(),
            ],
            expected_fills: 1,
            expected_rejects: 0,
        }
    }

    /// Simple sell order scenario
    pub fn single_sell() -> Self {
        Self {
            name: "single_sell".to_string(),
            description: "A single market sell order".to_string(),
            signals: vec![
                TestSignal::new("BTC/USD")
                    .sell()
                    .with_quantity(0.1)
                    .with_price(50000.0)
                    .build(),
            ],
            expected_fills: 1,
            expected_rejects: 0,
        }
    }

    /// Round trip: buy then sell
    pub fn round_trip() -> Self {
        Self {
            name: "round_trip".to_string(),
            description: "Buy then sell the same quantity".to_string(),
            signals: vec![
                TestSignal::new("BTC/USD")
                    .with_id("buy-1")
                    .buy()
                    .with_quantity(1.0)
                    .with_price(50000.0)
                    .build(),
                TestSignal::new("BTC/USD")
                    .with_id("sell-1")
                    .sell()
                    .with_quantity(1.0)
                    .with_price(50100.0)
                    .build(),
            ],
            expected_fills: 2,
            expected_rejects: 0,
        }
    }

    /// Multiple orders across different symbols
    pub fn multi_symbol() -> Self {
        Self {
            name: "multi_symbol".to_string(),
            description: "Orders across multiple trading pairs".to_string(),
            signals: vec![
                TestSignal::new("BTC/USD")
                    .with_id("btc-buy")
                    .buy()
                    .with_quantity(0.1)
                    .with_price(50000.0)
                    .build(),
                TestSignal::new("ETH/USD")
                    .with_id("eth-buy")
                    .buy()
                    .with_quantity(1.0)
                    .with_price(3000.0)
                    .build(),
                TestSignal::new("SOL/USD")
                    .with_id("sol-buy")
                    .buy()
                    .with_quantity(10.0)
                    .with_price(100.0)
                    .build(),
            ],
            expected_fills: 3,
            expected_rejects: 0,
        }
    }

    /// High frequency burst of orders
    pub fn burst_orders(count: usize) -> Self {
        let signals: Vec<Signal> = (0..count)
            .map(|i| {
                TestSignal::new("BTC/USD")
                    .with_id(&format!("burst-{}", i))
                    .buy()
                    .with_quantity(0.01)
                    .with_price(50000.0)
                    .build()
            })
            .collect();

        Self {
            name: format!("burst_{}_orders", count),
            description: format!("Rapid burst of {} orders", count),
            signals,
            expected_fills: count,
            expected_rejects: 0,
        }
    }

    /// Limit order ladder (multiple limit orders at different prices)
    pub fn limit_ladder(levels: usize, base_price: f64, spread_bps: f64) -> Self {
        let mut signals = Vec::with_capacity(levels * 2);
        let spread = base_price * spread_bps / 10000.0;

        for i in 0..levels {
            let offset = spread * (i + 1) as f64;
            
            // Buy side
            signals.push(
                TestSignal::new("BTC/USD")
                    .with_id(&format!("bid-{}", i))
                    .buy_limit()
                    .with_quantity(0.1)
                    .with_price(base_price - offset)
                    .build(),
            );
            
            // Sell side
            signals.push(
                TestSignal::new("BTC/USD")
                    .with_id(&format!("ask-{}", i))
                    .sell_limit()
                    .with_quantity(0.1)
                    .with_price(base_price + offset)
                    .build(),
            );
        }

        Self {
            name: format!("limit_ladder_{}_levels", levels),
            description: format!("{}-level limit order ladder around {}", levels, base_price),
            signals,
            expected_fills: levels * 2,
            expected_rejects: 0,
        }
    }
}

/// Collection of test fixtures
pub struct TestFixtures;

impl TestFixtures {
    /// Get all standard test scenarios
    pub fn all_scenarios() -> Vec<TestScenario> {
        vec![
            TestScenario::single_buy(),
            TestScenario::single_sell(),
            TestScenario::round_trip(),
            TestScenario::multi_symbol(),
            TestScenario::burst_orders(10),
            TestScenario::burst_orders(100),
            TestScenario::limit_ladder(5, 50000.0, 10.0),
        ]
    }

    /// Generate random signals for stress testing
    pub fn random_signals(count: usize) -> Vec<Signal> {
        let symbols = ["BTC/USD", "ETH/USD", "SOL/USD", "AVAX/USD"];
        let actions = [SignalAction::Buy, SignalAction::Sell, SignalAction::BuyLimit, SignalAction::SellLimit];
        
        (0..count)
            .map(|i| {
                let symbol = symbols[i % symbols.len()];
                let action = actions[i % actions.len()];
                let price = 100.0 + (i as f64 * 0.1);
                
                Signal {
                    id: format!("random-{}", i),
                    strategy_id: "stress-test".to_string(),
                    symbol: symbol.to_string(),
                    exchange: "MockExchange".to_string(),
                    action,
                    quantity: 0.1 + (i as f64 * 0.01),
                    price: Some(price),
                    confidence: 0.5 + (rand::random::<f64>() * 0.5),
                    timestamp: nano_timestamp() as u64,
                    metadata: HashMap::new(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_builder() {
        let signal = TestSignal::new("BTC/USD")
            .with_id("test-123")
            .buy()
            .with_quantity(1.5)
            .with_price(45000.0)
            .build();

        assert_eq!(signal.id, "test-123");
        assert_eq!(signal.symbol, "BTC/USD");
        assert_eq!(signal.quantity, 1.5);
        assert_eq!(signal.price, Some(45000.0));
        assert!(matches!(signal.action, SignalAction::Buy));
    }

    #[test]
    fn test_scenarios() {
        let scenarios = TestFixtures::all_scenarios();
        assert!(!scenarios.is_empty());
        
        for scenario in scenarios {
            assert!(!scenario.signals.is_empty());
        }
    }

    #[test]
    fn test_random_signals() {
        let signals = TestFixtures::random_signals(100);
        assert_eq!(signals.len(), 100);
        
        for signal in &signals {
            assert!(signal.quantity > 0.0);
            assert!(signal.price.is_some());
        }
    }
}
