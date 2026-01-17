//! Error types for strategy loading

use thiserror::Error;

#[derive(Error, Debug)]
pub enum StrategyLoaderError {
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Strategy not found: {0}")]
    NotFound(String),
    
    #[error("Invalid strategy parameters: {0}")]
    InvalidParameters(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("Connection error: {0}")]
    Connection(String),
}

pub type Result<T> = std::result::Result<T, StrategyLoaderError>;
