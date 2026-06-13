//! Adapter that wraps a [`DexConnector`] as an [`ExchangeConnector`].
//!
//! This allows DEX connectors to be used seamlessly in the existing deployment
//! pipeline and factory without modifying the core `ExchangeConnector` trait.

use async_trait::async_trait;
use std::collections::HashMap;

use crate::core::traits::ExchangeConnector;
use crate::core::types::*;
use crate::signal::{Signal, SignalAction};
use super::traits::{DexConnector, DexConfig, BlockchainNetwork};

/// Wraps any `DexConnector` implementation as an `ExchangeConnector`.
pub struct DexToExchangeAdapter {
    inner: Box<dyn DexConnector>,
    exchange_name: String,
    network: BlockchainNetwork,
    initialized: bool,
}

impl DexToExchangeAdapter {
    pub fn new(connector: Box<dyn DexConnector>, exchange_name: impl Into<String>, network: BlockchainNetwork) -> Self {
        Self {
            inner: connector,
            exchange_name: exchange_name.into(),
            network,
            initialized: false,
        }
    }

    pub fn cetus(network: BlockchainNetwork) -> Self {
        Self::new(
            Box::new(super::CetusConnector::new()),
            "cetus",
            network,
        )
    }

    pub fn deepbook(network: BlockchainNetwork) -> Self {
        Self::new(
            Box::new(super::DeepBookConnector::new()),
            "deepbook",
            network,
        )
    }
}

#[async_trait]
impl ExchangeConnector for DexToExchangeAdapter {
    fn exchange_name(&self) -> &str {
        &self.exchange_name
    }

    async fn initialize(&mut self, config: ExchangeConfig) -> Result<(), ExecutionError> {
        let dex_config = DexConfig {
            exchange_name: config.name.clone(),
            network: self.network,
            rpc_url: config.rest_api_url.unwrap_or_else(|| self.network.rpc_url().to_string()),
            wallet_private_key: config.api_key.clone(),
            wallet_address: config.passphrase.unwrap_or_default(),
            max_gas_price: 10_000,
            gas_multiplier: 1.2,
            slippage_bps: 50,
            mev_protection: false,
            private_mempool: false,
            deadline_seconds: 60,
            min_confirmations: 1,
            router_address: config.custom_headers.get("router_address").cloned(),
            custom_params: config.custom_headers.clone(),
        };

        self.inner.initialize(dex_config).await?;
        self.initialized = true;
        Ok(())
    }

    async fn execute_order(&self, signal: &Signal) -> Result<ExecutionResult, ExecutionError> {
        let dex_result = self.inner.execute_swap(signal).await?;
        Ok(dex_result.base)
    }

    async fn execute_batch_orders(&self, signals: &[Signal]) -> Result<Vec<ExecutionResult>, ExecutionError> {
        self.execute_batch_orders_sequential(signals).await
    }

    async fn cancel_order(&self, tx_hash: &str) -> Result<CancelResult, ExecutionError> {
        self.inner.cancel_transaction(tx_hash).await?;
        Ok(CancelResult {
            order_id: tx_hash.to_string(),
            exchange_order_id: Some(tx_hash.to_string()),
            status: CancelStatus::Cancelled,
            cancelled_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        })
    }

    async fn cancel_all_orders(&self) -> Result<Vec<CancelResult>, ExecutionError> {
        Ok(vec![])
    }

    async fn edit_order(&self, _params: EditOrderParams) -> Result<EditResult, ExecutionError> {
        Err(ExecutionError::Validation(
            "DEX orders cannot be edited — cancel and resubmit instead".into(),
        ))
    }

    async fn get_order_status(&self, tx_hash: &str) -> Result<Option<OrderStatus>, ExecutionError> {
        let status = self.inner.check_transaction(tx_hash).await?;
        let order_status = match status {
            super::traits::TransactionStatus::Pending => OrderStatus {
                order_id: tx_hash.to_string(),
                exchange_order_id: Some(tx_hash.to_string()),
                status: ExecutionStatus::Pending,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                last_updated: 0,
            },
            super::traits::TransactionStatus::Confirmed(_) => OrderStatus {
                order_id: tx_hash.to_string(),
                exchange_order_id: Some(tx_hash.to_string()),
                status: ExecutionStatus::Filled,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                last_updated: 0,
            },
            super::traits::TransactionStatus::Failed(_) | super::traits::TransactionStatus::Dropped => OrderStatus {
                order_id: tx_hash.to_string(),
                exchange_order_id: Some(tx_hash.to_string()),
                status: ExecutionStatus::Rejected,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                last_updated: 0,
            },
        };
        Ok(Some(order_status))
    }

    fn get_metrics(&self) -> ExecutionMetrics {
        ExecutionMetrics {
            exchange: self.exchange_name.clone(),
            total_orders: 0,
            successful_orders: 0,
            failed_orders: 0,
            cancelled_orders: 0,
            avg_latency_ns: self.network.finality_time_ms() as u64 * 1_000_000,
            min_latency_ns: 200_000_000,
            max_latency_ns: 2_000_000_000,
            p50_latency_ns: self.network.finality_time_ms() as u64 * 1_000_000,
            p95_latency_ns: self.network.finality_time_ms() as u64 * 1_500_000,
            p99_latency_ns: self.network.finality_time_ms() as u64 * 2_000_000,
            p999_latency_ns: self.network.finality_time_ms() as u64 * 3_000_000,
            total_volume: 0.0,
            total_fees: 0.0,
            fill_rate: 0.0,
            error_rate: 0.0,
            orders_per_second: 0.0,
            last_updated: 0,
            websocket_connected: false,
            connection_pool_utilization: 0.0,
            rate_limit_utilization: 0.0,
        }
    }

    async fn subscribe_to_updates(&self, _callback: Box<dyn Fn(OrderUpdate) + Send + Sync>) {
        // DEX connectors don't have WebSocket order update feeds
    }

    async fn health_check(&self) -> Result<HealthStatus, ExecutionError> {
        Ok(HealthStatus {
            exchange: self.exchange_name.clone(),
            status: if self.initialized { HealthState::Healthy } else { HealthState::Unhealthy },
            latency_ns: self.network.finality_time_ms() as u64 * 1_000_000,
            last_check: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            error_message: if self.initialized { None } else { Some("Not initialized".into()) },
        })
    }

    fn get_limits(&self) -> ExchangeLimits {
        ExchangeLimits {
            max_orders_per_second: 10,
            max_batch_size: 1,
            min_order_size: 0.01,
            max_order_size: 1_000_000.0,
            tick_size: 0.000001,
            supported_order_types: vec![OrderType::Market],
            supported_time_in_force: vec![TimeInForce::ImmediateOrCancel],
        }
    }

    fn validate_order(&self, signal: &Signal) -> Result<(), ExecutionError> {
        if signal.quantity <= 0.0 {
            return Err(ExecutionError::Validation("Quantity must be positive".into()));
        }
        Ok(())
    }

    fn convert_signal(&self, signal: &Signal) -> Result<ExchangeOrder, ExecutionError> {
        let side = match signal.action {
            SignalAction::Buy => OrderSide::Buy,
            SignalAction::Sell => OrderSide::Sell,
            _ => OrderSide::Buy,
        };

        Ok(ExchangeOrder {
            symbol: signal.symbol.clone(),
            side,
            order_type: OrderType::Market,
            quantity: signal.quantity,
            price: signal.price,
            time_in_force: TimeInForce::ImmediateOrCancel,
            client_order_id: format!("dex_{}_{}", self.exchange_name, signal.id),
            metadata: HashMap::new(),
        })
    }
}
