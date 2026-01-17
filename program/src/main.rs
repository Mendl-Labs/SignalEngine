use hostbuilder::{
    HostedObjectTrait
};
use anyhow::Result;
use std::env;
use signalengine::{SignalEngineLogger, TradingContext};
use ultra_logger::init_global_logger_from_env;
use std::time::Instant;

#[cfg(test)]
mod performance_tests;

#[tokio::main]
async fn main() -> Result<()> {
    let startup_time = Instant::now();
    
    // Initialize the global logger from environment variables
    // Set ULTRA_LOGGER_TRANSPORT=elasticsearch to send logs to Kibana
    init_global_logger_from_env("SignalEngine".to_string());
    
    // Initialize the entire SignalEngine system with logging
    signalengine::initialize_signal_engine().await.map_err(|e| anyhow::anyhow!("{}", e))?;
    
    let logger = SignalEngineLogger::new("SignalEngine").await;
    
    // Log system startup
    let startup_ctx = TradingContext::new("SignalEngine")
        .with_operation("startup");
    
    logger.info_ctx(
        &format!("SignalEngine v{} initializing on {} ({})", 
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH
        ),
        startup_ctx.clone()
    ).await;

    // Get configuration path from environment or use default
    let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| {
        tokio::spawn(async move {
            let warn_logger = SignalEngineLogger::new("SignalEngine").await;
            warn_logger.warn("CONFIG_PATH not set, using default config path").await;
        });
        "./config/default.toml".to_string()
    });

    logger.info(&format!("Using configuration from: {}", config_path)).await;

    // Create hosted object using builder pattern
    logger.info("Building HostedObject...").await;
    
    let engine = hostbuilder::HostedObjectBuilder::new()
        .with_config_path(config_path)
        .build().expect("Failed to build hosted object");
    
    let init_duration = startup_time.elapsed();
    logger.info(&format!("SignalEngine initialized in {:?}", init_duration)).await;

    // Run the hosted object and handle any errors
    logger.info("Starting main execution loop...").await;
    
    match engine.run().await {
        Ok(_) => {
            let total_runtime = startup_time.elapsed();
            let shutdown_ctx = TradingContext::new("SignalEngine")
                .with_operation("shutdown");
            logger.info_ctx(
                &format!("Signal Engine completed successfully (runtime: {}s)", total_runtime.as_secs()),
                shutdown_ctx
            ).await;
            Ok(())
        },
        Err(e) => {
            logger.error(&format!("Signal Engine error: {}", e)).await;
            Err(anyhow::anyhow!("Signal Engine failed: {}", e))
        }
    }
}