use hostbuilder::{
    HostedObjectTrait
};
use anyhow::Result;
use std::env;
use signalengine::{SignalEngineLogger};

#[cfg(test)]
mod performance_tests;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the entire SignalEngine system with logging
    signalengine::initialize_signal_engine().await.map_err(|e| anyhow::anyhow!("{}", e))?;
    
    let logger = SignalEngineLogger::new("SignalEngine").await;

    // Get configuration path from environment or use default
    let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| {
        tokio::spawn(async move {
            let warn_logger = SignalEngineLogger::new("SignalEngine").await;
            warn_logger.warn("CONFIG_PATH not set, using default config path").await;
        });
        "./config/default.toml".to_string()
    });

    logger.info(&format!("Using configuration from: {config_path}")).await;

    // Create hosted object using builder pattern
    let engine = hostbuilder::HostedObjectBuilder::new()
        .with_config_path(config_path)
        .build().expect("Failed to build hosted object");

    // Run the hosted object and handle any errors
    match engine.run().await {
        Ok(_) => {
            logger.info("Signal Engine completed successfully").await;
            Ok(())
        },
        Err(e) => {
            logger.error(&format!("Signal Engine error: {e}")).await;
            Err(anyhow::anyhow!("Signal Engine failed: {}", e))
        }
    }
}