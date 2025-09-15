use hostbuilder::{
    HostedObjectTrait
};
use anyhow::Result;
use std::env;
use signalengine::{initialize_signal_engine, SignalEngineLogger, TradingContext};

#[cfg(test)]
mod performance_tests;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the entire SignalEngine system with logging
    initialize_signal_engine().await?;
    
    let logger = SignalEngineLogger::new("SignalEngine").await;

    // Get configuration path from environment or use default
    let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| {
        tokio::spawn({
            let logger = logger.clone();
            async move {
                logger.warn("CONFIG_PATH not set, using default config path").await;
            }
        });
        "./config/default.toml".to_string()
    });

    logger.log(LogLevel::Info, format!("Using configuration from: {config_path}")).await.ok();

    // Create hosted object using builder pattern
    let engine = hostbuilder::HostedObjectBuilder::new()
        .with_config_path(config_path)
        .build().expect("Failed to build hosted object");

    // Run the hosted object and handle any errors
    match engine.run().await {
        Ok(_) => {
            logger.log(LogLevel::Info, "Signal Engine completed successfully".to_string()).await.ok();
            Ok(())
        },
        Err(e) => {
            logger.log(LogLevel::Error, format!("Signal Engine error: {e}")).await.ok();
            Err(anyhow::anyhow!("Signal Engine failed: {}", e))
        }
    }
}