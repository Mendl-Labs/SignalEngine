use executionhandler::UltraLowLatencyExecutionHandler;

#[tokio::test]
async fn test_new_architecture_basic() {
    let handler = UltraLowLatencyExecutionHandler::new().await;
    assert!(handler.list_exchanges().await.is_empty());
}

#[test]
fn test_exchange_factory() {
    let supported = UltraLowLatencyExecutionHandler::supported_exchanges();
    // Currently only kraken is implemented
    assert!(supported.contains(&"kraken"));
    assert!(!supported.is_empty());
}

#[test]
#[ignore] // Requires KRAKEN_API_KEY environment variable
fn test_exchange_config_templates() {
    let kraken_config = UltraLowLatencyExecutionHandler::create_exchange_config("kraken");
    assert!(kraken_config.is_ok());
    
    let invalid_config = UltraLowLatencyExecutionHandler::create_exchange_config("invalid");
    assert!(invalid_config.is_err());
}
