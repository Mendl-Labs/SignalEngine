//! Metrics HTTP Server
//!
//! Provides a simple HTTP server for Prometheus metrics scraping.
//! Also provides JSON endpoints for debugging and health checks.
//!
//! Uses raw TCP with hand-written HTTP responses for minimal dependencies
//! and maximum control over latency.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::prometheus_metrics::{get_prometheus_metrics, get_metrics_json};
use crate::risk_controls::KILL_SWITCH;
use crate::dead_letter_queue::DeadLetterQueue;

/// Metrics server configuration
#[derive(Debug, Clone)]
pub struct MetricsServerConfig {
    /// Host to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
}

impl Default for MetricsServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 9090,
        }
    }
}

/// Handle for controlling the metrics server
pub struct MetricsServerHandle {
    running: Arc<AtomicBool>,
}

impl MetricsServerHandle {
    /// Signal the server to shutdown
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
    
    /// Check if the server is still running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Start the metrics HTTP server
pub async fn start_metrics_server(
    config: MetricsServerConfig,
    dlq: Option<Arc<DeadLetterQueue>>,
) -> Result<MetricsServerHandle, Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    
    let listener = TcpListener::bind(&addr).await?;
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = Arc::clone(&running);
    let dlq_clone = dlq.clone();
    
    log::info!("Metrics server listening on http://{}", addr);
    
    // Spawn the server
    tokio::spawn(async move {
        while running_clone.load(Ordering::Relaxed) {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((mut socket, _peer_addr)) => {
                            let dlq = dlq_clone.clone();
                            tokio::spawn(async move {
                                let mut buffer = [0u8; 4096];
                                
                                // Read request
                                match socket.read(&mut buffer).await {
                                    Ok(n) if n > 0 => {
                                        let request = String::from_utf8_lossy(&buffer[..n]);
                                        let response = handle_request(&request, &dlq).await;
                                        
                                        // Write response
                                        if let Err(e) = socket.write_all(response.as_bytes()).await {
                                            log::debug!("Failed to write response: {}", e);
                                        }
                                    }
                                    _ => {}
                                }
                            });
                        }
                        Err(e) => {
                            log::error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Check running flag periodically
                }
            }
        }
        log::info!("Metrics server stopped");
    });

    Ok(MetricsServerHandle { running })
}

/// Parse HTTP request path from raw request
fn parse_request_path(request: &str) -> (&str, &str) {
    let first_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    
    let method = parts.first().copied().unwrap_or("GET");
    let path = parts.get(1).copied().unwrap_or("/");
    
    (method, path)
}

/// Format HTTP response
fn http_response(status: u16, content_type: &str, body: &str) -> String {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    };
    
    format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         Access-Control-Allow-Origin: *\r\n\
         \r\n\
         {}",
        status, status_text, content_type, body.len(), body
    )
}

/// Handle HTTP requests
async fn handle_request(request: &str, dlq: &Option<Arc<DeadLetterQueue>>) -> String {
    let (method, path) = parse_request_path(request);
    
    match path {
        "/metrics" => {
            // Prometheus format metrics
            let mut metrics = get_prometheus_metrics();
            metrics.push_str(&crate::credential_mode::credential_mode_prometheus());
            http_response(200, "text/plain; version=0.0.4; charset=utf-8", &metrics)
        }
        
        "/health" => {
            // Health check endpoint
            let kill_switch_active = KILL_SWITCH.is_triggered();
            let status = if kill_switch_active { "unhealthy" } else { "healthy" };
            let status_code = if kill_switch_active { 503 } else { 200 };
            
            let body = serde_json::json!({
                "status": status,
                "kill_switch_active": kill_switch_active,
                "kill_switch_reason": KILL_SWITCH.get_trigger_reason().map(|r| format!("{:?}", r)),
                "timestamp": crate::optimizations::timestamp::nano_timestamp(),
            }).to_string();
            
            http_response(status_code, "application/json", &body)
        }
        
        "/metrics/json" => {
            // JSON format metrics (for debugging)
            let metrics = get_metrics_json().to_string();
            http_response(200, "application/json", &metrics)
        }
        
        "/risk/status" => {
            // Risk management status
            let kill_switch_triggered = KILL_SWITCH.is_triggered();
            let kill_reason = KILL_SWITCH.get_trigger_reason();
            
            let body = serde_json::json!({
                "kill_switch": {
                    "triggered": kill_switch_triggered,
                    "reason": kill_reason.map(|r| format!("{:?}", r)),
                    "trigger_time": KILL_SWITCH.get_trigger_time(),
                },
                "timestamp": crate::optimizations::timestamp::nano_timestamp(),
            }).to_string();
            
            http_response(200, "application/json", &body)
        }
        
        "/risk/kill" => {
            // Manual kill switch trigger (POST only)
            if method == "POST" {
                use crate::risk_controls::KillReason;
                KILL_SWITCH.trigger(KillReason::Manual);
                
                let body = serde_json::json!({
                    "success": true,
                    "message": "Kill switch triggered",
                    "timestamp": crate::optimizations::timestamp::nano_timestamp(),
                }).to_string();
                
                http_response(200, "application/json", &body)
            } else {
                http_response(405, "application/json", r#"{"error": "Method not allowed, use POST"}"#)
            }
        }
        
        "/risk/reset" => {
            // Reset kill switch (POST only)
            if method == "POST" {
                KILL_SWITCH.reset();
                
                let body = serde_json::json!({
                    "success": true,
                    "message": "Kill switch reset - trading enabled",
                    "timestamp": crate::optimizations::timestamp::nano_timestamp(),
                }).to_string();
                
                http_response(200, "application/json", &body)
            } else {
                http_response(405, "application/json", r#"{"error": "Method not allowed, use POST"}"#)
            }
        }
        
        "/dlq" => {
            // Dead letter queue status
            if let Some(ref dlq) = dlq {
                let stats = dlq.stats().await;
                let body = serde_json::json!({
                    "stats": stats,
                    "timestamp": crate::optimizations::timestamp::nano_timestamp(),
                }).to_string();
                
                http_response(200, "application/json", &body)
            } else {
                http_response(501, "application/json", r#"{"error": "DLQ not configured"}"#)
            }
        }
        
        "/dlq/pending" => {
            // Dead letter queue pending entries
            if let Some(ref dlq) = dlq {
                let pending = dlq.get_pending().await;
                let body = serde_json::json!({
                    "count": pending.len(),
                    "entries": pending,
                    "timestamp": crate::optimizations::timestamp::nano_timestamp(),
                }).to_string();
                
                http_response(200, "application/json", &body)
            } else {
                http_response(501, "application/json", r#"{"error": "DLQ not configured"}"#)
            }
        }
        
        "/dlq/review" => {
            // Dead letter queue entries needing manual review
            if let Some(ref dlq) = dlq {
                let review = dlq.get_manual_review().await;
                let body = serde_json::json!({
                    "count": review.len(),
                    "entries": review,
                    "timestamp": crate::optimizations::timestamp::nano_timestamp(),
                }).to_string();
                
                http_response(200, "application/json", &body)
            } else {
                http_response(501, "application/json", r#"{"error": "DLQ not configured"}"#)
            }
        }
        
        _ => {
            // 404 for unknown paths
            let body = serde_json::json!({
                "error": "Not found",
                "available_endpoints": [
                    "/metrics",
                    "/metrics/json",
                    "/health",
                    "/risk/status",
                    "/risk/kill (POST)",
                    "/risk/reset (POST)",
                    "/dlq",
                    "/dlq/pending",
                    "/dlq/review"
                ]
            }).to_string();
            
            http_response(404, "application/json", &body)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_server_config_default() {
        let config = MetricsServerConfig::default();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 9090);
    }

    #[test]
    fn test_parse_request_path() {
        let request = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let (method, path) = parse_request_path(request);
        assert_eq!(method, "GET");
        assert_eq!(path, "/metrics");
    }

    #[test]
    fn test_http_response_format() {
        let response = http_response(200, "text/plain", "hello");
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("Content-Type: text/plain"));
        assert!(response.contains("Content-Length: 5"));
        assert!(response.ends_with("hello"));
    }
}
