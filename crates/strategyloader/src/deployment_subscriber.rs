//! Deployment Subscriber for Hot-Loading Strategy Deployments
//!
//! This module subscribes to deployment events from the message broker and
//! hot-loads/unloads strategies without requiring SignalEngine restart.
//!
//! # Topics Subscribed
//! - `strategy.deployment` - New strategy deployments
//! - `strategy.deactivation` - Strategy deactivation requests
//! - `strategy.status.request` - Deployment status requests
//!
//! # Flow
//! 1. BacktestingEngine publishes `StrategyDeployment` after approval
//! 2. DeploymentSubscriber receives event
//! 3. Subscriber loads strategy configuration and hot-loads into StrategyManager
//! 4. Subscriber publishes `StrategyDeploymentAck` with success/failure
//!
//! # Hot-Loading Strategy
//! - Strategies are loaded into a thread-safe `DashMap` in StrategyManager
//! - New strategies can be added while existing strategies continue running
//! - Deactivation gracefully stops strategies (close positions, cancel orders)

use chrono::Utc;
use dashmap::DashMap;
use prost::Message;
use protocol::broker::messages::{
    publish_request, DeploymentStatusRequest, DeploymentStatusResponse,
    MarketDataSubscribe, MarketDataUnsubscribe,
    PublishRequest, StrategyDeactivation, StrategyDeployment, StrategyDeploymentAck,
    ActiveStrategyInfo,
};
use publisher::{PublisherConfig, UltraFastPublisher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use subscriber::{UltraFastMessage, UltraFastSubscriber};
use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;

/// Topics for deployment events
pub mod topics {
    pub const STRATEGY_DEPLOYMENT: &str = "strategy.deployment";
    pub const STRATEGY_DEACTIVATION: &str = "strategy.deactivation";
    pub const STRATEGY_DEPLOYMENT_ACK: &str = "strategy.deployment.ack";
    pub const STRATEGY_STATUS_REQUEST: &str = "strategy.status.request";
    pub const STRATEGY_STATUS_RESPONSE: &str = "strategy.status.response";
    /// Market data subscription topic (for DataEngine)
    pub const MARKET_DATA_SUBSCRIBE: &str = "market.subscription.subscribe";
    pub const MARKET_DATA_UNSUBSCRIBE: &str = "market.subscription.unsubscribe";
}

/// Errors that can occur during deployment subscription
#[derive(Debug, Error)]
pub enum DeploymentSubscriberError {
    #[error("Failed to connect to message broker: {0}")]
    ConnectionError(String),
    #[error("Failed to subscribe to topics: {0}")]
    SubscriptionError(String),
    #[error("Failed to parse deployment message: {0}")]
    ParseError(String),
    #[error("Failed to load strategy: {0}")]
    LoadError(String),
    #[error("Failed to publish acknowledgment: {0}")]
    AckError(String),
    #[error("Strategy manager not configured")]
    NoStrategyManager,
}

/// Information about a deployed strategy for tracking
#[derive(Debug)]
pub struct DeployedStrategy {
    pub strategy_id: Uuid,
    pub instance_id: Uuid,
    pub tenant_id: Uuid,
    pub strategy_type: String,
    pub strategy_name: String,
    pub parameters: serde_json::Value,
    pub target_exchanges: Vec<String>,
    pub symbols: Vec<String>,
    pub deployed_at: i64,
    pub is_active: AtomicBool,
    pub total_trades: AtomicU64,
    pub unrealized_pnl: std::sync::atomic::AtomicI64, // In basis points for lock-free
    pub realized_pnl: std::sync::atomic::AtomicI64,
    pub open_positions: std::sync::atomic::AtomicI32,
    pub pending_orders: std::sync::atomic::AtomicI32,
}

impl DeployedStrategy {
    /// Create a new deployed strategy from a deployment message
    pub fn from_deployment(msg: &StrategyDeployment) -> Result<Self, DeploymentSubscriberError> {
        let strategy_id = Uuid::parse_str(&msg.strategy_id)
            .map_err(|e| DeploymentSubscriberError::ParseError(format!("Invalid strategy_id: {}", e)))?;
        let instance_id = Uuid::parse_str(&msg.instance_id)
            .map_err(|e| DeploymentSubscriberError::ParseError(format!("Invalid instance_id: {}", e)))?;
        let tenant_id = Uuid::parse_str(&msg.tenant_id)
            .map_err(|e| DeploymentSubscriberError::ParseError(format!("Invalid tenant_id: {}", e)))?;

        let parameters: serde_json::Value = if msg.parameters.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&msg.parameters)
                .map_err(|e| DeploymentSubscriberError::ParseError(format!("Invalid parameters: {}", e)))?
        };

        Ok(Self {
            strategy_id,
            instance_id,
            tenant_id,
            strategy_type: msg.strategy_type.clone(),
            strategy_name: msg.strategy_name.clone(),
            parameters,
            target_exchanges: msg.target_exchanges.clone(),
            symbols: msg.symbols.clone(),
            deployed_at: Utc::now().timestamp_millis(),
            is_active: AtomicBool::new(true),
            total_trades: AtomicU64::new(0),
            unrealized_pnl: std::sync::atomic::AtomicI64::new(0),
            realized_pnl: std::sync::atomic::AtomicI64::new(0),
            open_positions: std::sync::atomic::AtomicI32::new(0),
            pending_orders: std::sync::atomic::AtomicI32::new(0),
        })
    }

    /// Convert to ActiveStrategyInfo for status responses
    pub fn to_active_info(&self) -> ActiveStrategyInfo {
        ActiveStrategyInfo {
            strategy_id: self.strategy_id.to_string(),
            instance_id: self.instance_id.to_string(),
            tenant_id: self.tenant_id.to_string(),
            strategy_type: self.strategy_type.clone(),
            strategy_name: self.strategy_name.clone(),
            active_exchanges: self.target_exchanges.clone(),
            symbols: self.symbols.clone(),
            unrealized_pnl: self.unrealized_pnl.load(Ordering::Relaxed) as f64 / 10000.0,
            realized_pnl: self.realized_pnl.load(Ordering::Relaxed) as f64 / 10000.0,
            open_positions: self.open_positions.load(Ordering::Relaxed),
            pending_orders: self.pending_orders.load(Ordering::Relaxed),
            deployed_at: self.deployed_at,
            total_trades: self.total_trades.load(Ordering::Relaxed) as i64,
        }
    }
}

/// Deployment subscriber that hot-loads strategies
pub struct DeploymentSubscriber {
    /// Broker address for connections
    broker_address: String,
    /// Node identifier for this SignalEngine instance
    node_id: String,
    /// Publisher for acknowledgments
    publisher: Option<Arc<UltraFastPublisher>>,
    /// Active deployed strategies (key: instance_id)
    deployed_strategies: Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
    /// Running flag
    is_running: AtomicBool,
    /// Channel for deployment events to strategy manager
    deployment_tx: Option<mpsc::Sender<DeploymentEvent>>,
}

/// Events sent to the strategy manager
#[derive(Debug, Clone)]
pub enum DeploymentEvent {
    Deploy(Arc<DeployedStrategy>),
    Deactivate {
        instance_id: Uuid,
        reason: String,
        close_positions: bool,
        cancel_orders: bool,
    },
}

impl DeploymentSubscriber {
    /// Create a new deployment subscriber
    pub fn new(broker_address: &str, node_id: &str) -> Self {
        Self {
            broker_address: broker_address.to_string(),
            node_id: node_id.to_string(),
            publisher: None,
            deployed_strategies: Arc::new(DashMap::new()),
            is_running: AtomicBool::new(false),
            deployment_tx: None,
        }
    }

    /// Set the channel for sending deployment events to strategy manager
    pub fn set_deployment_channel(&mut self, tx: mpsc::Sender<DeploymentEvent>) {
        self.deployment_tx = Some(tx);
    }

    /// Get the deployed strategies map for external access
    pub fn get_deployed_strategies(&self) -> Arc<DashMap<Uuid, Arc<DeployedStrategy>>> {
        self.deployed_strategies.clone()
    }

    /// Start the deployment subscriber
    pub async fn start(&mut self) -> Result<(), DeploymentSubscriberError> {
        // Connect publisher for acknowledgments
        let pub_config = PublisherConfig::new(&self.broker_address);
        let publisher = UltraFastPublisher::new(pub_config);
        publisher.connect().await.map_err(|e| {
            DeploymentSubscriberError::ConnectionError(format!("Publisher: {:?}", e))
        })?;
        self.publisher = Some(Arc::new(publisher));

        // Create subscriber (ID from node_id hash)
        let subscriber_id = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.node_id.hash(&mut hasher);
            hasher.finish()
        };
        let subscriber = Arc::new(UltraFastSubscriber::new(subscriber_id));

        // Subscribe to deployment topics
        subscriber.subscribe_to_topic(topics::STRATEGY_DEPLOYMENT).await.map_err(|e| {
            DeploymentSubscriberError::SubscriptionError(format!("{:?}", e))
        })?;
        subscriber.subscribe_to_topic(topics::STRATEGY_DEACTIVATION).await.map_err(|e| {
            DeploymentSubscriberError::SubscriptionError(format!("{:?}", e))
        })?;
        subscriber.subscribe_to_topic(topics::STRATEGY_STATUS_REQUEST).await.map_err(|e| {
            DeploymentSubscriberError::SubscriptionError(format!("{:?}", e))
        })?;

        self.is_running.store(true, Ordering::SeqCst);

        // Spawn message processing task
        let deployed_strategies = self.deployed_strategies.clone();
        let publisher = self.publisher.clone();
        let node_id = self.node_id.clone();
        let deployment_tx = self.deployment_tx.clone();
        let is_running = Arc::new(AtomicBool::new(true));
        let is_running_clone = is_running.clone();

        tokio::spawn(async move {
            Self::process_messages(
                subscriber,
                deployed_strategies,
                publisher,
                node_id,
                deployment_tx,
                is_running_clone,
            )
            .await;
        });

        Ok(())
    }

    /// Process incoming messages
    async fn process_messages(
        subscriber: Arc<UltraFastSubscriber>,
        deployed_strategies: Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<Arc<UltraFastPublisher>>,
        node_id: String,
        deployment_tx: Option<mpsc::Sender<DeploymentEvent>>,
        is_running: Arc<AtomicBool>,
    ) {
        let topics_to_poll = [
            topics::STRATEGY_DEPLOYMENT,
            topics::STRATEGY_DEACTIVATION,
            topics::STRATEGY_STATUS_REQUEST,
        ];

        while is_running.load(Ordering::Relaxed) {
            let mut had_message = false;

            // Poll each topic for messages
            for topic in &topics_to_poll {
                if let Some(msg) = subscriber.get_message_from_topic(topic) {
                    had_message = true;
                    Self::handle_message(
                        msg,
                        &deployed_strategies,
                        publisher.as_ref(),
                        &node_id,
                        deployment_tx.as_ref(),
                    )
                    .await;
                }
            }

            // If no messages, sleep briefly to avoid busy loop
            if !had_message {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }
    }

    /// Handle a single message
    async fn handle_message(
        msg: UltraFastMessage,
        deployed_strategies: &Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        node_id: &str,
        deployment_tx: Option<&mpsc::Sender<DeploymentEvent>>,
    ) {
        // Try to decode as PublishRequest
        if let Ok(request) = PublishRequest::decode(msg.data.as_slice()) {
            match request.payload {
                Some(publish_request::Payload::StrategyDeployment(deployment)) => {
                    Self::handle_deployment(
                        deployment,
                        deployed_strategies,
                        publisher,
                        node_id,
                        deployment_tx,
                    )
                    .await;
                }
                Some(publish_request::Payload::StrategyDeactivation(deactivation)) => {
                    Self::handle_deactivation(
                        deactivation,
                        deployed_strategies,
                        publisher,
                        node_id,
                        deployment_tx,
                    )
                    .await;
                }
                Some(publish_request::Payload::RawData(data)) => {
                    // Could be a status request
                    if let Ok(status_req) = serde_json::from_slice::<DeploymentStatusRequest>(&data)
                    {
                        Self::handle_status_request(
                            status_req,
                            deployed_strategies,
                            publisher,
                            node_id,
                        )
                        .await;
                    }
                }
                _ => {} // Ignore other message types
            }
        }
    }

    /// Handle a deployment message
    async fn handle_deployment(
        deployment: StrategyDeployment,
        deployed_strategies: &Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        node_id: &str,
        deployment_tx: Option<&mpsc::Sender<DeploymentEvent>>,
    ) {
        let (success, error_message, active_exchanges, symbols, instance_id_str, tenant_id_str) = 
            match DeployedStrategy::from_deployment(&deployment) {
            Ok(strategy) => {
                let instance_id = strategy.instance_id;
                let exchanges = strategy.target_exchanges.clone();
                let symbols = strategy.symbols.clone();
                let instance_id_str = strategy.instance_id.to_string();
                let tenant_id_str = strategy.tenant_id.to_string();
                let strategy = Arc::new(strategy);

                // Add to deployed strategies map
                deployed_strategies.insert(instance_id, strategy.clone());

                // Notify strategy manager
                if let Some(tx) = deployment_tx {
                    let _ = tx.send(DeploymentEvent::Deploy(strategy)).await;
                }

                (true, String::new(), exchanges, symbols, instance_id_str, tenant_id_str)
            }
            Err(e) => (false, e.to_string(), Vec::new(), Vec::new(), String::new(), String::new()),
        };

        // Send acknowledgment
        if let Some(pub_arc) = publisher {
            let ack = StrategyDeploymentAck {
                strategy_id: deployment.strategy_id.clone(),
                instance_id: deployment.instance_id.clone(),
                signal_engine_node: node_id.to_string(),
                success,
                error_message: error_message.clone(),
                loaded_at: Utc::now().timestamp_millis(),
                active_exchanges: active_exchanges.clone(),
            };

            let request = PublishRequest {
                topic: topics::STRATEGY_DEPLOYMENT_ACK.to_string(),
                payload: Some(publish_request::Payload::StrategyDeploymentAck(ack)),
            };

            let encoded = request.encode_to_vec();
            let _ = pub_arc.publish_raw(encoded, topics::STRATEGY_DEPLOYMENT_ACK).await;

            // If deployment succeeded, publish market data subscriptions for each exchange
            if success && !active_exchanges.is_empty() && !symbols.is_empty() {
                for exchange in &active_exchanges {
                    let subscription_id = format!("{}_{}", instance_id_str, exchange);
                    
                    let market_sub = MarketDataSubscribe {
                        subscription_id: subscription_id.clone(),
                        tenant_id: tenant_id_str.clone(),
                        strategy_instance_id: instance_id_str.clone(),
                        exchange: exchange.clone(),
                        symbols: symbols.clone(),
                        data_types: vec!["level3".to_string(), "trade".to_string()],
                        orderbook_depth: 100, // Default depth for HFT
                        timestamp: Utc::now().timestamp_millis(),
                    };

                    let encoded = market_sub.encode_to_vec();
                    if let Err(_e) = pub_arc.publish_raw(encoded, topics::MARKET_DATA_SUBSCRIBE).await {
                        // Log error but don't fail deployment
                        // Strategy can still work if DataEngine is already streaming
                    }
                }
            }
        }
    }

    /// Handle a deactivation message
    async fn handle_deactivation(
        deactivation: StrategyDeactivation,
        deployed_strategies: &Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        _node_id: &str,
        deployment_tx: Option<&mpsc::Sender<DeploymentEvent>>,
    ) {
        if let Ok(instance_id) = Uuid::parse_str(&deactivation.instance_id) {
            // Get strategy info before removing
            let strategy_info = deployed_strategies.get(&instance_id).map(|s| {
                (
                    s.instance_id.to_string(),
                    s.target_exchanges.clone(),
                    s.symbols.clone(),
                )
            });

            // Mark as inactive
            if let Some(strategy) = deployed_strategies.get(&instance_id) {
                strategy.is_active.store(false, Ordering::SeqCst);
            }

            // Remove from map
            deployed_strategies.remove(&instance_id);

            // Publish market data unsubscribe for each exchange
            if let (Some(pub_arc), Some((inst_id, exchanges, symbols))) = 
                (publisher, strategy_info) 
            {
                for exchange in &exchanges {
                    let subscription_id = format!("{}_{}", inst_id, exchange);
                    
                    let market_unsub = MarketDataUnsubscribe {
                        subscription_id: subscription_id.clone(),
                        strategy_instance_id: inst_id.clone(),
                        exchange: exchange.clone(),
                        symbols: symbols.clone(),
                        reason: deactivation.reason.clone(),
                        timestamp: Utc::now().timestamp_millis(),
                    };

                    let encoded = market_unsub.encode_to_vec();
                    let _ = pub_arc.publish_raw(encoded, topics::MARKET_DATA_UNSUBSCRIBE).await;
                }
            }

            // Notify strategy manager
            if let Some(tx) = deployment_tx {
                let _ = tx
                    .send(DeploymentEvent::Deactivate {
                        instance_id,
                        reason: deactivation.reason,
                        close_positions: deactivation.close_positions,
                        cancel_orders: deactivation.cancel_orders,
                    })
                    .await;
            }
        }
    }

    /// Handle a status request
    async fn handle_status_request(
        request: DeploymentStatusRequest,
        deployed_strategies: &Arc<DashMap<Uuid, Arc<DeployedStrategy>>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        node_id: &str,
    ) {
        let mut active_strategies = Vec::new();
        let mut total_memory: i64 = 0;

        for entry in deployed_strategies.iter() {
            let strategy = entry.value();

            // Filter by tenant if specified
            if !request.tenant_id.is_empty() && strategy.tenant_id.to_string() != request.tenant_id
            {
                continue;
            }

            active_strategies.push(strategy.to_active_info());
            total_memory += std::mem::size_of::<DeployedStrategy>() as i64;
        }

        if let Some(pub_arc) = publisher {
            let response = DeploymentStatusResponse {
                request_id: request.request_id,
                signal_engine_node: node_id.to_string(),
                active_strategies,
                total_memory_bytes: total_memory,
                cpu_utilization: 0.0, // TODO: Get actual CPU usage
            };

            let request = PublishRequest {
                topic: topics::STRATEGY_STATUS_RESPONSE.to_string(),
                payload: Some(publish_request::Payload::DeploymentStatusResponse(response)),
            };

            let encoded = request.encode_to_vec();
            let _ = pub_arc
                .publish_raw(encoded, topics::STRATEGY_STATUS_RESPONSE)
                .await;
        }
    }

    /// Stop the deployment subscriber
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }

    /// Get count of deployed strategies
    pub fn deployed_count(&self) -> usize {
        self.deployed_strategies.len()
    }

    /// Check if a specific instance is deployed
    pub fn is_deployed(&self, instance_id: &Uuid) -> bool {
        self.deployed_strategies.contains_key(instance_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_constants() {
        assert_eq!(topics::STRATEGY_DEPLOYMENT, "strategy.deployment");
        assert_eq!(topics::STRATEGY_DEACTIVATION, "strategy.deactivation");
    }

    #[test]
    fn test_deployed_strategy_from_deployment() {
        let deployment = StrategyDeployment {
            strategy_id: Uuid::new_v4().to_string(),
            instance_id: Uuid::new_v4().to_string(),
            tenant_id: Uuid::new_v4().to_string(),
            strategy_type: "AvellanedaStoikov".to_string(),
            strategy_name: "BTC Market Maker".to_string(),
            version: "1.0.0".to_string(),
            parameters: serde_json::to_vec(&serde_json::json!({"gamma": 0.1})).unwrap(),
            initial_capital: 10000.0,
            target_exchanges: vec!["kraken".to_string()],
            symbols: vec!["BTCUSD".to_string()],
            approved_by: "admin".to_string(),
            approved_at: "2026-01-25T12:00:00Z".to_string(),
            performance_summary: vec![],
            risk_metrics: vec![],
            admin_approved: true,
            timestamp: 0,
        };

        let strategy = DeployedStrategy::from_deployment(&deployment).unwrap();
        assert_eq!(strategy.strategy_type, "AvellanedaStoikov");
        assert_eq!(strategy.strategy_name, "BTC Market Maker");
        assert!(strategy.is_active.load(Ordering::Relaxed));
    }
}
