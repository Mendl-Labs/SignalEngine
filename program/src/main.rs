use hostbuilder::{
    HostedObject,
    HostedObjectTrait
};
use anyhow::Result;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    println!("Starting Signal Engine...");

    // Get configuration path from environment or use default
    let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| {
        println!("CONFIG_PATH not set, using default config path");
        "./config/default.toml".to_string()
    });

    println!("Using configuration from: {}", config_path);

    // Create hosted object using builder pattern
    let engine = hostbuilder::HostedObjectBuilder::new()
        .with_config_path(config_path)
        .build().expect("Failed to build hosted object");

    // Run the hosted object and handle any errors
    match engine.run().await {
        Ok(_) => {
            println!("Signal Engine completed successfully");
            Ok(())
        },
        Err(e) => {
            eprintln!("Signal Engine error: {}", e);
            Err(anyhow::anyhow!("Signal Engine failed: {}", e))
        }
    }
}