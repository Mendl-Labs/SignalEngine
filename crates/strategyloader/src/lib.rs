//! Strategy Loading and Management for SignalEngine
//! 
//! This crate provides the infrastructure for loading trading strategies from
//! the database and managing their lifecycle.
//!
//! # Architecture
//! 
//! - `StrategyInstance`: A configured strategy ready to execute
//! - `TradingAsset`: A single tradeable asset within a strategy  
//! - `StrategyLoader`: Trait for loading strategies from various sources
//! - `DatabaseStrategyLoader`: Loads strategies from PostgreSQL
//! - `DeploymentSubscriber`: Hot-loads strategies from message broker events
//! - `MarketDataRouter`: Routes market data to subscribed strategies
//! - `StrategyStateRegistry`: Manages per-strategy execution state
//! - `PortfolioStrategy`: Trait for parameterized strategy execution
//! - `StrategyManager`: Orchestrates strategy loading and execution

pub mod types;
pub mod loader;
pub mod error;
pub mod router;
pub mod state;
pub mod strategy;
pub mod manager;
pub mod deployment_subscriber;

#[cfg(feature = "postgres")]
pub mod database_loader;

#[cfg(feature = "postgres")]
pub mod paper_trade_writer;

// Re-exports
pub use types::*;
pub use loader::{StrategyLoader, ChainedStrategyLoader};
pub use error::StrategyLoaderError;
pub use router::MarketDataRouter;
pub use state::{StrategyState, AssetState, PortfolioState, StrategyStateRegistry};
pub use strategy::{
    PortfolioStrategy, Signal, MarketDataEvent, BarEvent,
    MomentumStrategy, MeanReversionStrategy, StrategyFactory
};
pub use manager::{StrategyManager, SignalStore, SignalInfo, SignalStatus, ManagerMetrics};
pub use deployment_subscriber::{
    DeploymentSubscriber, DeployedStrategy, DeploymentEvent, DeploymentSubscriberError,
    topics as deployment_topics,
};

#[cfg(feature = "postgres")]
pub use database_loader::DatabaseStrategyLoader;

#[cfg(feature = "postgres")]
pub use paper_trade_writer::{PaperTradeWriter, PaperFillEvent};
