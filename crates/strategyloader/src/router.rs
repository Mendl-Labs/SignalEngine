//! Market data routing to strategies
//!
//! Routes incoming market data to strategies that are subscribed
//! to specific symbol/exchange pairs.

use std::collections::HashMap;
use uuid::Uuid;
use ultra_logger::ultra_debug;
use crate::types::StrategyInstance;

/// Routes market data to subscribed strategies
#[derive(Debug, Default)]
pub struct MarketDataRouter {
    /// Maps (symbol, exchange) -> list of strategy IDs
    subscriptions: HashMap<(String, String), Vec<Uuid>>,
    
    /// Reverse mapping: strategy ID -> list of (symbol, exchange) pairs
    strategy_assets: HashMap<Uuid, Vec<(String, String)>>,
}

impl MarketDataRouter {
    /// Create a new empty router
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Register a strategy's asset subscriptions
    pub fn register_strategy(&mut self, strategy: &StrategyInstance) {
        let mut asset_keys = Vec::new();
        
        for asset in &strategy.assets {
            let key = asset.key();
            asset_keys.push(key.clone());
            
            self.subscriptions
                .entry(key)
                .or_default()
                .push(strategy.id);
        }
        
        self.strategy_assets.insert(strategy.id, asset_keys);
        
        ultra_debug!(format!(
            "Registered strategy '{}' ({}) for {} assets",
            strategy.name,
            strategy.id,
            strategy.assets.len()
        ));
    }
    
    /// Unregister a strategy
    pub fn unregister_strategy(&mut self, strategy_id: Uuid) {
        if let Some(assets) = self.strategy_assets.remove(&strategy_id) {
            for key in assets {
                if let Some(strategies) = self.subscriptions.get_mut(&key) {
                    strategies.retain(|id| *id != strategy_id);
                    if strategies.is_empty() {
                        self.subscriptions.remove(&key);
                    }
                }
            }
        }
    }
    
    /// Get all strategy IDs that need this market data
    pub fn route(&self, symbol: &str, exchange: &str) -> Vec<Uuid> {
        let key = (symbol.to_string(), exchange.to_string());
        self.subscriptions
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }
    
    /// Get all unique (symbol, exchange) pairs that need subscriptions
    pub fn get_required_subscriptions(&self) -> Vec<(String, String)> {
        self.subscriptions.keys().cloned().collect()
    }
    
    /// Check if any strategy needs this symbol/exchange
    pub fn has_subscribers(&self, symbol: &str, exchange: &str) -> bool {
        let key = (symbol.to_string(), exchange.to_string());
        self.subscriptions.contains_key(&key)
    }
    
    /// Get count of strategies subscribed to a pair
    pub fn subscriber_count(&self, symbol: &str, exchange: &str) -> usize {
        let key = (symbol.to_string(), exchange.to_string());
        self.subscriptions
            .get(&key)
            .map(|v| v.len())
            .unwrap_or(0)
    }
    
    /// Get all assets for a strategy
    pub fn get_strategy_assets(&self, strategy_id: Uuid) -> Vec<(String, String)> {
        self.strategy_assets
            .get(&strategy_id)
            .cloned()
            .unwrap_or_default()
    }
    
    /// Total number of registered strategies
    pub fn strategy_count(&self) -> usize {
        self.strategy_assets.len()
    }
    
    /// Total number of unique symbol/exchange subscriptions
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    
    fn make_test_strategy(name: &str, assets: Vec<(&str, &str)>) -> StrategyInstance {
        StrategyInstance {
            id: Uuid::new_v4(),
            name: name.to_string(),
            strategy_type: StrategyType::Momentum,
            version: "1.0".to_string(),
            assets: assets.into_iter().map(|(s, e)| TradingAsset {
                symbol: s.to_string(),
                exchange: e.to_string(),
                weight: 1.0,
                risk_limits: AssetRiskLimits::default(),
            }).collect(),
            parameters: StrategyParameters::Momentum(MomentumParams::default()),
            portfolio_risk: PortfolioRiskLimits::default(),
            enabled: true,
            paper_trading: true,
            description: None,
            metadata: HashMap::new(),
        }
    }
    
    #[test]
    fn test_router_registration() {
        let mut router = MarketDataRouter::new();
        
        let strategy1 = make_test_strategy("Strategy1", vec![
            ("BTC/USD", "kraken"),
            ("ETH/USD", "kraken"),
        ]);
        
        let strategy2 = make_test_strategy("Strategy2", vec![
            ("BTC/USD", "kraken"),
            ("BTC/USD", "binance"),
        ]);
        
        router.register_strategy(&strategy1);
        router.register_strategy(&strategy2);
        
        // BTC/USD on kraken should have 2 subscribers
        assert_eq!(router.subscriber_count("BTC/USD", "kraken"), 2);
        
        // ETH/USD on kraken should have 1 subscriber
        assert_eq!(router.subscriber_count("ETH/USD", "kraken"), 1);
        
        // BTC/USD on binance should have 1 subscriber
        assert_eq!(router.subscriber_count("BTC/USD", "binance"), 1);
        
        // Total strategies
        assert_eq!(router.strategy_count(), 2);
        
        // Total subscriptions
        assert_eq!(router.subscription_count(), 3);
    }
    
    #[test]
    fn test_router_routing() {
        let mut router = MarketDataRouter::new();
        
        let strategy = make_test_strategy("MyStrategy", vec![
            ("BTC/USD", "kraken"),
        ]);
        let strategy_id = strategy.id;
        
        router.register_strategy(&strategy);
        
        let routed = router.route("BTC/USD", "kraken");
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0], strategy_id);
        
        let not_routed = router.route("ETH/USD", "kraken");
        assert!(not_routed.is_empty());
    }
    
    #[test]
    fn test_router_unregister() {
        let mut router = MarketDataRouter::new();
        
        let strategy = make_test_strategy("MyStrategy", vec![
            ("BTC/USD", "kraken"),
        ]);
        let strategy_id = strategy.id;
        
        router.register_strategy(&strategy);
        assert_eq!(router.strategy_count(), 1);
        
        router.unregister_strategy(strategy_id);
        assert_eq!(router.strategy_count(), 0);
        assert!(!router.has_subscribers("BTC/USD", "kraken"));
    }
}
