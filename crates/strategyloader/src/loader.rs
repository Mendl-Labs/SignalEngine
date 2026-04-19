//! Strategy loader trait definition

use async_trait::async_trait;
use uuid::Uuid;
use ultra_logger::ultra_warn;
use crate::types::StrategyInstance;
use crate::error::Result;

/// Trait for loading strategies from various sources
#[async_trait]
pub trait StrategyLoader: Send + Sync {
    /// Load all active strategies
    async fn load_active_strategies(&self) -> Result<Vec<StrategyInstance>>;
    
    /// Load a specific strategy by ID
    async fn load_strategy(&self, id: Uuid) -> Result<Option<StrategyInstance>>;
    
    /// Load a strategy by name and version
    async fn load_strategy_by_name(&self, name: &str, version: &str) -> Result<Option<StrategyInstance>>;
    
    /// Reload a strategy (for hot-reloading)
    async fn reload_strategy(&self, id: Uuid) -> Result<Option<StrategyInstance>> {
        self.load_strategy(id).await
    }
    
    /// Check if a strategy exists and is active
    async fn is_strategy_active(&self, id: Uuid) -> Result<bool> {
        Ok(self.load_strategy(id).await?.map(|s| s.enabled).unwrap_or(false))
    }
}

/// Combinator that tries multiple loaders in order
pub struct ChainedStrategyLoader {
    loaders: Vec<Box<dyn StrategyLoader>>,
}

impl ChainedStrategyLoader {
    pub fn new() -> Self {
        Self { loaders: Vec::new() }
    }
    
    pub fn add_loader(mut self, loader: impl StrategyLoader + 'static) -> Self {
        self.loaders.push(Box::new(loader));
        self
    }
}

impl Default for ChainedStrategyLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyLoader for ChainedStrategyLoader {
    async fn load_active_strategies(&self) -> Result<Vec<StrategyInstance>> {
        let mut all_strategies = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();
        
        for loader in &self.loaders {
            match loader.load_active_strategies().await {
                Ok(strategies) => {
                    for strategy in strategies {
                        if seen_ids.insert(strategy.id) {
                            all_strategies.push(strategy);
                        }
                    }
                }
                Err(e) => {
                    ultra_warn!(format!("Loader failed: {}, trying next", e));
                }
            }
        }
        
        Ok(all_strategies)
    }
    
    async fn load_strategy(&self, id: Uuid) -> Result<Option<StrategyInstance>> {
        for loader in &self.loaders {
            if let Ok(Some(strategy)) = loader.load_strategy(id).await {
                return Ok(Some(strategy));
            }
        }
        Ok(None)
    }
    
    async fn load_strategy_by_name(&self, name: &str, version: &str) -> Result<Option<StrategyInstance>> {
        for loader in &self.loaders {
            if let Ok(Some(strategy)) = loader.load_strategy_by_name(name, version).await {
                return Ok(Some(strategy));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use std::collections::HashMap;

    /// A mock in-memory loader for testing
    struct MockLoader {
        strategies: Vec<StrategyInstance>,
    }

    impl MockLoader {
        fn new(strategies: Vec<StrategyInstance>) -> Self {
            Self { strategies }
        }

        fn make_strategy(name: &str, enabled: bool) -> StrategyInstance {
            StrategyInstance {
                id: Uuid::new_v4(),
                name: name.to_string(),
                strategy_type: StrategyType::Custom,
                version: "1.0".to_string(),
                assets: vec![],
                parameters: StrategyParameters::Generic(GenericParams::default()),
                portfolio_risk: PortfolioRiskLimits::default(),
                enabled,
                description: None,
                python_source: None,
                metadata: HashMap::new(),
            }
        }
    }

    #[async_trait]
    impl StrategyLoader for MockLoader {
        async fn load_active_strategies(&self) -> Result<Vec<StrategyInstance>> {
            Ok(self.strategies.iter().filter(|s| s.enabled).cloned().collect())
        }
        async fn load_strategy(&self, id: Uuid) -> Result<Option<StrategyInstance>> {
            Ok(self.strategies.iter().find(|s| s.id == id).cloned())
        }
        async fn load_strategy_by_name(&self, name: &str, version: &str) -> Result<Option<StrategyInstance>> {
            Ok(self.strategies.iter().find(|s| s.name == name && s.version == version).cloned())
        }
    }

    #[tokio::test]
    async fn test_chained_loader_empty() {
        let loader = ChainedStrategyLoader::new();
        let results = loader.load_active_strategies().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_chained_loader_single_source() {
        let s1 = MockLoader::make_strategy("alpha", true);
        let loader = ChainedStrategyLoader::new()
            .add_loader(MockLoader::new(vec![s1.clone()]));
        let results = loader.load_active_strategies().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "alpha");
    }

    #[tokio::test]
    async fn test_chained_loader_dedup_by_id() {
        let mut s1 = MockLoader::make_strategy("dup", true);
        let id = s1.id;
        let s2 = StrategyInstance { id, ..s1.clone() };

        let loader = ChainedStrategyLoader::new()
            .add_loader(MockLoader::new(vec![s1]))
            .add_loader(MockLoader::new(vec![s2]));
        let results = loader.load_active_strategies().await.unwrap();
        // Dedup: same id appears only once
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_chained_loader_lookup_by_id() {
        let s1 = MockLoader::make_strategy("find-me", true);
        let id = s1.id;
        let loader = ChainedStrategyLoader::new()
            .add_loader(MockLoader::new(vec![s1]));
        let found = loader.load_strategy(id).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "find-me");
    }

    #[tokio::test]
    async fn test_chained_loader_missing_strategy() {
        let loader = ChainedStrategyLoader::new()
            .add_loader(MockLoader::new(vec![]));
        let result = loader.load_strategy(Uuid::new_v4()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_is_strategy_active_default_impl() {
        let s = MockLoader::make_strategy("active", true);
        let id = s.id;
        let loader = MockLoader::new(vec![s]);
        assert!(loader.is_strategy_active(id).await.unwrap());

        let disabled = MockLoader::make_strategy("disabled", false);
        let id2 = disabled.id;
        let loader2 = MockLoader::new(vec![disabled]);
        assert!(!loader2.is_strategy_active(id2).await.unwrap());
    }

    #[tokio::test]
    async fn test_reload_strategy_default_impl() {
        let s = MockLoader::make_strategy("reload-me", true);
        let id = s.id;
        let loader = MockLoader::new(vec![s]);
        let reloaded = loader.reload_strategy(id).await.unwrap();
        assert!(reloaded.is_some());
    }

    #[test]
    fn test_chained_loader_default() {
        let loader = ChainedStrategyLoader::default();
        assert!(loader.loaders.is_empty());
    }
}
