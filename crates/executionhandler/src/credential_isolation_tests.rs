//! Tenant credential isolation tests.
//!
//! Two tenants hold DIFFERENT keys for the SAME exchange. These tests prove
//! that every order is signed with its own tenant's key, and that a tenant
//! with no credentials gets an error -- never another tenant's key.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use smartorderrouter::{ExchangeCredential, StaticCredentialProvider, TenantId};
use uuid::Uuid;

use crate::core::*;
use crate::multi_tenant::{ConnectorBuilder, MultiTenantExecutionHandler, SubscriptionTier};
use crate::signal::{Signal, SignalAction};
use crate::UltraLowLatencyExecutionHandler;

/// (signal id, api key the connector was built with) for every executed order.
type Log = Arc<Mutex<Vec<(String, String)>>>;

/// Connector that records which API key "signed" each order.
struct KeyRecordingConnector {
    api_key: String,
    log: Log,
}

#[async_trait]
impl ExchangeConnector for KeyRecordingConnector {
    fn exchange_name(&self) -> &str { "kraken" }
    async fn initialize(&mut self, _config: ExchangeConfig) -> Result<(), ExecutionError> { Ok(()) }
    async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
        self.log.lock().unwrap().push((signal.id.clone(), self.api_key.clone()));
        Ok(ExecutionResult {
            order_id: signal.id.clone(),
            exchange_order_id: Some(format!("exch_{}", signal.id)),
            exchange: "kraken".into(),
            status: ExecutionStatus::Filled,
            filled_quantity: signal.quantity,
            remaining_quantity: 0.0,
            avg_fill_price: 100.0,
            total_fees: 0.0,
            fills: vec![],
            reject_reason: None,
            submitted_at: 0,
            updated_at: 0,
            latency_ns: 1,
            exchange_timestamp_ns: None,
            exchange_sequence: None,
        })
    }
    async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        self.execute_batch_orders_sequential(signals).await
    }
    async fn cancel_order(&self, _id: &str) -> Result<CancelResult, ExecutionError> { unimplemented!() }
    async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError> { unimplemented!() }
    async fn edit_order(&self, _p: EditOrderParams) -> Result<EditResult, ExecutionError> { unimplemented!() }
    async fn get_order_status(&self, _id: &str) -> Result<Option<OrderStatus>, ExecutionError> { unimplemented!() }
    fn get_metrics(&self) -> ExecutionMetrics {
        ExecutionMetrics {
            exchange: "kraken".into(),
            total_orders: 0, successful_orders: 0, failed_orders: 0, cancelled_orders: 0,
            avg_latency_ns: 0, min_latency_ns: 0, max_latency_ns: 0,
            p50_latency_ns: 0, p95_latency_ns: 0, p99_latency_ns: 0, p999_latency_ns: 0,
            total_volume: 0.0, total_fees: 0.0, fill_rate: 0.0, error_rate: 0.0,
            orders_per_second: 0.0, last_updated: 0,
            websocket_connected: false, connection_pool_utilization: 0.0, rate_limit_utilization: 0.0,
        }
    }
    async fn subscribe_to_updates(&self, _cb: Box<dyn Fn(OrderUpdate) + Send + Sync>) {}
    async fn health_check(&self) -> Result<HealthStatus, ExecutionError> { unimplemented!() }
    fn get_limits(&self) -> ExchangeLimits { unimplemented!() }
    fn validate_order(&self, _s: &Signal) -> Result<(), ExecutionError> { Ok(()) }
    fn convert_signal(&self, _s: &Signal) -> Result<ExchangeOrder, ExecutionError> { unimplemented!() }
}

struct RecordingBuilder {
    log: Log,
}

#[async_trait]
impl ConnectorBuilder for RecordingBuilder {
    async fn build(&self, credential: &ExchangeCredential) -> Result<Box<dyn ExchangeConnector>, ExecutionError> {
        Ok(Box::new(KeyRecordingConnector {
            api_key: credential.api_key.clone(),
            log: Arc::clone(&self.log),
        }))
    }
}

fn cred(key: &str) -> ExchangeCredential {
    ExchangeCredential {
        id: Uuid::new_v4(),
        exchange: "kraken".into(),
        label: "test".into(),
        api_key: key.into(),
        api_secret: format!("{}-secret", key),
        passphrase: None,
        is_testnet: false,
        is_enabled: true,
    }
}

fn signal(id: &str) -> Signal {
    Signal {
        id: id.to_string(),
        strategy_id: String::new(),
        symbol: "BTC/USD".into(),
        exchange: "kraken".into(),
        action: SignalAction::Buy,
        quantity: 1.0,
        price: Some(100.0),
        confidence: 1.0,
        timestamp: 0,
        metadata: std::collections::HashMap::new(),
    }
}

fn two_tenant_provider() -> (TenantId, TenantId, TenantId, Arc<StaticCredentialProvider>) {
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let stranger = Uuid::new_v4();
    let provider = StaticCredentialProvider::new()
        .with_credential(a, cred("KEY-A"))
        .with_credential(b, cred("KEY-B"));
    (a, b, stranger, Arc::new(provider))
}

/// MultiTenantExecutionHandler: each tenant's order is executed by a connector
/// built from that tenant's own credential.
#[tokio::test]
async fn multi_tenant_orders_are_signed_with_own_tenants_key() {
    let (a, b, stranger, provider) = two_tenant_provider();
    let log: Log = Arc::new(Mutex::new(vec![]));
    let handler = MultiTenantExecutionHandler::with_credential_provider(provider)
        .with_connector_builder(Arc::new(RecordingBuilder { log: Arc::clone(&log) }));

    for t in [a, b, stranger] {
        handler.register_tenant(t, SubscriptionTier::Professional).await.unwrap();
    }
    assert_eq!(handler.load_tenant_credentials(a).await.unwrap(), 1);
    assert_eq!(handler.load_tenant_credentials(b).await.unwrap(), 1);
    // The stranger holds nothing: nothing is loaded for them.
    assert_eq!(handler.load_tenant_credentials(stranger).await.unwrap(), 0);

    handler.submit_order(a, signal("order-A"), "kraken".into(), 5).await.unwrap();
    handler.submit_order(b, signal("order-B"), "kraken".into(), 5).await.unwrap();
    handler.submit_order(stranger, signal("order-S"), "kraken".into(), 5).await.unwrap();

    let results = handler.process_orders(10).await;
    assert_eq!(results.len(), 2, "only A and B may execute");

    let log = log.lock().unwrap();
    assert!(log.contains(&("order-A".to_string(), "KEY-A".to_string())), "{:?}", *log);
    assert!(log.contains(&("order-B".to_string(), "KEY-B".to_string())), "{:?}", *log);
    assert!(!log.iter().any(|(id, _)| id == "order-S"), "stranger's order must not execute: {:?}", *log);
    assert_eq!(log.len(), 2);
    assert_eq!(handler.get_global_stats().total_orders_rejected, 1);
}

/// A handler with no provider, or a provider error, loads nothing.
#[tokio::test]
async fn multi_tenant_without_provider_fails_closed() {
    let handler = MultiTenantExecutionHandler::new();
    let t = Uuid::new_v4();
    handler.register_tenant(t, SubscriptionTier::Professional).await.unwrap();
    assert!(handler.load_tenant_credentials(t).await.is_err());
}

/// UltraLowLatencyExecutionHandler (shared connector map keyed by exchange):
/// a tenant with no credentials errors, the second tenant cannot take over the
/// first tenant's connector, and each tenant can only execute through a
/// connector built from its own key.
#[tokio::test]
async fn shared_handler_never_serves_another_tenants_connector() {
    let (a, b, stranger, provider) = two_tenant_provider();
    let log: Log = Arc::new(Mutex::new(vec![]));
    let builder = RecordingBuilder { log: Arc::clone(&log) };
    let mut handler = UltraLowLatencyExecutionHandler::new().await;

    // Tenant with no credentials: error, nothing registered.
    let err = handler
        .ensure_exchange_for_tenant_with(provider.as_ref(), stranger, "kraken", true, &builder)
        .await
        .unwrap_err();
    assert!(format!("{}", err).contains("No usable credential"), "{}", err);
    assert!(!handler.has_connector("kraken").await);

    // Tenant A registers and trades with KEY-A.
    handler.ensure_exchange_for_tenant_with(provider.as_ref(), a, "kraken", true, &builder).await.unwrap();
    assert_eq!(handler.connector_owner("kraken").await, Some(a));
    handler.execute_order_on_exchange_for_tenant(a, &signal("order-A"), "kraken").await.unwrap();

    // Tenant B must NOT be able to replace A's connector...
    let err = handler
        .ensure_exchange_for_tenant_with(provider.as_ref(), b, "kraken", true, &builder)
        .await
        .unwrap_err();
    assert!(format!("{}", err).contains("already owned"), "{}", err);
    assert_eq!(handler.connector_owner("kraken").await, Some(a));

    // ...nor execute through A's connector, nor may the stranger.
    assert!(handler.execute_order_on_exchange_for_tenant(b, &signal("order-B"), "kraken").await.is_err());
    assert!(handler.execute_order_on_exchange_for_tenant(stranger, &signal("order-S"), "kraken").await.is_err());

    let log = log.lock().unwrap();
    assert_eq!(*log, vec![("order-A".to_string(), "KEY-A".to_string())]);
}

/// initialize_from_provider loads only the requested tenant's credentials.
#[tokio::test]
async fn initialize_from_provider_loads_only_requested_tenant() {
    let (a, _b, stranger, provider) = two_tenant_provider();
    let mut handler = UltraLowLatencyExecutionHandler::new().await;
    // Stranger: zero credentials, zero connectors.
    assert_eq!(handler.initialize_from_provider(provider.as_ref(), stranger).await.unwrap(), 0);
    assert!(handler.list_exchanges().await.is_empty());
    // (Real ExchangeFactory build for A is exercised elsewhere; here we only
    // assert the stranger case and that A's lookup returns exactly one credential.)
    let creds = smartorderrouter::resolve_all_credentials(provider.as_ref(), a).await.unwrap();
    assert_eq!(creds.len(), 1);
    assert_eq!(creds[0].api_key, "KEY-A");
}
