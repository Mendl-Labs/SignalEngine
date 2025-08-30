use hostbuilder::{
    HostedObjectTrait
};
use anyhow::Result;
use std::env;
use ultra_logger::{UltraLogger, LogLevel};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize ultra-low latency logging
    let logger = UltraLogger::new("SignalEngine".to_string());
    logger.log(LogLevel::Info, "Starting Signal Engine...".to_string()).await.ok();

    // Get configuration path from environment or use default
    let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(logger.log(LogLevel::Warn, "CONFIG_PATH not set, using default config path".to_string())).ok();
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