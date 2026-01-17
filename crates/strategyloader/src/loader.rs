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
