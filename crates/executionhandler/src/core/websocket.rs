use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use crate::core::types::{ExecutionError, OrderUpdate, WebSocketSubscription};

/// Generic WebSocket manager for exchange connections
pub struct WebSocketManager {
    url: String,
    connected: Arc<RwLock<bool>>,
    subscriptions: Arc<RwLock<HashMap<String, WebSocketSubscription>>>,
    #[allow(clippy::type_complexity)]
    message_handlers: Arc<RwLock<Vec<Box<dyn Fn(&str) -> Result<Vec<OrderUpdate>, ExecutionError> + Send + Sync>>>>,
}

impl WebSocketManager {
    pub fn new(url: String) -> Self {
        Self {
            url,
            connected: Arc::new(RwLock::new(false)),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            message_handlers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Connect to WebSocket and start message processing
    pub async fn connect(&self) -> Result<(), ExecutionError> {
        let (ws_stream, _) = connect_async(&self.url).await
            .map_err(|e| ExecutionError::Connection(format!("WebSocket connection failed: {}", e)))?;

        *self.connected.write().await = true;

        // Start message processing task
        let connected_clone = Arc::clone(&self.connected);
        let handlers_clone = Arc::clone(&self.message_handlers);
        
        tokio::spawn(async move {
            let (mut write, mut read) = ws_stream.split();
            
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        let handlers = handlers_clone.read().await;
                        for handler in handlers.iter() {
                            if let Ok(updates) = handler(&text) {
                                for update in updates {
                                    // Process order updates
                                    log::debug!("Order update: {:?}", update);
                                }
                            }
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        // Handle binary messages if needed
                    }
                    Ok(Message::Ping(data)) => {
                        // Respond to ping
                        if let Err(e) = write.send(Message::Pong(data)).await {
                            log::error!("Failed to send pong: {}", e);
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        log::info!("WebSocket connection closed");
                        break;
                    }
                    Err(e) => {
                        log::error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
            
            *connected_clone.write().await = false;
        });

        Ok(())
    }

    /// Disconnect from WebSocket
    pub async fn disconnect(&self) -> Result<(), ExecutionError> {
        *self.connected.write().await = false;
        Ok(())
    }

    /// Add a message handler
    pub async fn add_message_handler<F>(&self, handler: F) 
    where
        F: Fn(&str) -> Result<Vec<OrderUpdate>, ExecutionError> + Send + Sync + 'static,
    {
        let mut handlers = self.message_handlers.write().await;
        handlers.push(Box::new(handler));
    }

    /// Subscribe to a channel
    pub async fn subscribe(&self, subscription: WebSocketSubscription) -> Result<(), ExecutionError> {
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.insert(subscription.channel.clone(), subscription);
        
        // Send subscription message (exchange-specific implementation needed)
        Ok(())
    }

    /// Unsubscribe from a channel
    pub async fn unsubscribe(&self, channel: &str) -> Result<(), ExecutionError> {
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.remove(channel);
        
        // Send unsubscription message (exchange-specific implementation needed)
        Ok(())
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }

    /// Get current subscriptions
    pub async fn get_subscriptions(&self) -> HashMap<String, WebSocketSubscription> {
        self.subscriptions.read().await.clone()
    }
}

/// WebSocket connection pool for multiple concurrent connections
pub struct WebSocketPool {
    connections: Vec<WebSocketManager>,
    round_robin_index: std::sync::atomic::AtomicUsize,
}

impl WebSocketPool {
    pub fn new(base_url: String, pool_size: usize) -> Self {
        let mut connections = Vec::with_capacity(pool_size);
        
        for i in 0..pool_size {
            let url = if i == 0 {
                base_url.clone()
            } else {
                format!("{}?conn_id={}", base_url, i)
            };
            connections.push(WebSocketManager::new(url));
        }

        Self {
            connections,
            round_robin_index: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Get the next available connection using round-robin
    pub fn get_connection(&self) -> &WebSocketManager {
        let index = self.round_robin_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        &self.connections[index % self.connections.len()]
    }

    /// Connect all connections in the pool
    pub async fn connect_all(&self) -> Result<(), ExecutionError> {
        for connection in &self.connections {
            connection.connect().await?;
        }
        Ok(())
    }

    /// Disconnect all connections
    pub async fn disconnect_all(&self) -> Result<(), ExecutionError> {
        for connection in &self.connections {
            connection.disconnect().await?;
        }
        Ok(())
    }

    /// Check if any connection is active
    pub async fn has_active_connections(&self) -> bool {
        for connection in &self.connections {
            if connection.is_connected().await {
                return true;
            }
        }
        false
    }
}
