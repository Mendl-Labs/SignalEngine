use serde::{Serialize, Deserialize};
use serde_yaml;
use serde_json;
use std::fs;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use anyhow::{
    Context,
    Result
};

/// Environment types for configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Environment {
    Development,
    Testing,
    Staging,
    Production,
}

impl Default for Environment {
    fn default() -> Self {
        Environment::Development
    }
}

/// Enhanced configuration with validation and environment support
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Config {
    pub environment: Environment,
    pub message_broker: MessageBroker,
    pub publish_topics: Vec<String>,
    pub subscribe_topics: Vec<String>,
    pub topic_routing: HashMap<String, String>,
    pub strategies: Vec<StrategyConfig>,
    pub exchanges: Vec<ExchangeConfig>,
    pub performance: PerformanceConfig,
    pub security: SecurityConfig,
    pub monitoring: MonitoringConfig,
    pub logging: LoggingConfig,
    
    #[serde(skip)]
    pub last_modified: Option<Instant>,
    #[serde(skip)]
    pub config_path: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MessageBroker {
    pub address: String,
    pub port: u16,
    pub max_connections: u32,
    pub buffer_size: usize,
    pub compression_enabled: bool,
    pub batch_size: usize,
    pub flush_interval: Duration,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StrategyConfig {
    pub id: String,
    pub name: String,
    pub strategy_type: String,
    pub enabled: bool,
    pub symbols: Vec<String>,
    pub exchanges: Vec<String>,
    pub parameters: HashMap<String, serde_json::Value>,
    pub risk_limits: RiskLimits,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RiskLimits {
    pub max_position_size: f64,
    pub max_order_size: f64,
    pub max_daily_loss: f64,
    pub max_position_value: f64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ExchangeConfig {
    pub name: String,
    pub enabled: bool,
    pub api_key: Option<String>,
    pub secret_key: Option<String>,
    pub sandbox: bool,
    pub rate_limits: RateLimitConfig,
    pub timeout: Duration,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RateLimitConfig {
    pub orders_per_second: u32,
    pub requests_per_second: u32,
    pub window_size: Duration,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PerformanceConfig {
    pub worker_threads: u32,
    pub cpu_affinity_enabled: bool,
    pub cpu_cores: Vec<u32>,
    pub memory_limit_mb: u64,
    pub latency_threshold_us: u64,
    pub metrics_enabled: bool,
    pub metrics_interval: Duration,
    pub simd_enabled: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SecurityConfig {
    pub api_key_required: bool,
    pub request_signing_required: bool,
    pub rate_limiting_enabled: bool,
    pub max_requests_per_minute: u32,
    pub blocked_ips: Vec<String>,
    pub allowed_ips: Vec<String>,
    pub encryption_enabled: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MonitoringConfig {
    pub health_check_enabled: bool,
    pub health_check_interval: Duration,
    pub metrics_retention_days: u32,
    pub alerting_enabled: bool,
    pub alert_thresholds: AlertThresholds,
    pub prometheus_enabled: bool,
    pub prometheus_port: u16,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AlertThresholds {
    pub latency_us: f64,
    pub error_rate_percent: f64,
    pub cpu_usage_percent: f64,
    pub memory_usage_percent: f64,
    pub connection_failures: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String, // "json" or "text"
    pub output: Vec<String>, // ["stdout", "file", "syslog"]
    pub file_path: Option<String>,
    pub max_file_size_mb: u64,
    pub max_files: u32,
    pub structured_logging: bool,
}

impl Config {
    pub fn new(file_path: &str) -> Result<Self> {
        let config_data = fs::read_to_string(file_path)
            .with_context(|| format!("Unable to read file: {}", file_path))?;
        let mut config: Config = serde_yaml::from_str(&config_data)
            .context("YAML was not well-formatted")?;
        
        config.config_path = Some(file_path.to_string());
        config.last_modified = Some(Instant::now());
        
        // Apply environment variable overrides
        config.apply_environment_overrides();
        
        // Validate configuration
        config.validate()?;
        
        Ok(config)
    }

    /// Apply environment variable overrides
    pub fn apply_environment_overrides(&mut self) {
        // Database-like environment variables
        if let Ok(mb_address) = std::env::var("MESSAGE_BROKER_HOST").or_else(|_| std::env::var("MESSAGE_BROKER_ADDRESS")) {
            self.message_broker.address = mb_address;
        }
        if let Ok(mb_port) = std::env::var("MESSAGE_BROKER_PORT") {
            if let Ok(port) = mb_port.parse::<u16>() {
                self.message_broker.port = port;
            }
        }
        
        // Environment detection
        if let Ok(env) = std::env::var("ENVIRONMENT") {
            match env.to_lowercase().as_str() {
                "development" | "dev" => self.environment = Environment::Development,
                "testing" | "test" => self.environment = Environment::Testing,
                "staging" | "stage" => self.environment = Environment::Staging,
                "production" | "prod" => self.environment = Environment::Production,
                _ => {}
            }
        }
        
        // Performance
        if let Ok(threads) = std::env::var("WORKER_THREADS") {
            if let Ok(thread_count) = threads.parse::<u32>() {
                self.performance.worker_threads = thread_count;
            }
        }
        
        // Logging
        if let Ok(log_level) = std::env::var("LOG_LEVEL") {
            self.logging.level = log_level;
        }
    }

    /// Validate configuration settings
    pub fn validate(&self) -> Result<()> {
        // Validate message broker
        if self.message_broker.address.is_empty() {
            return Err(anyhow::anyhow!("Message broker address cannot be empty"));
        }
        if self.message_broker.port == 0 {
            return Err(anyhow::anyhow!("Message broker port must be greater than 0"));
        }

        // Validate strategies
        for strategy in &self.strategies {
            if strategy.id.is_empty() {
                return Err(anyhow::anyhow!("Strategy ID cannot be empty"));
            }
            if strategy.symbols.is_empty() {
                return Err(anyhow::anyhow!("Strategy '{}' must have at least one symbol", strategy.id));
            }
        }

        // Validate exchanges
        for exchange in &self.exchanges {
            if exchange.name.is_empty() {
                return Err(anyhow::anyhow!("Exchange name cannot be empty"));
            }
        }

        // Validate performance settings
        if self.performance.worker_threads == 0 {
            return Err(anyhow::anyhow!("Worker threads must be greater than 0"));
        }

        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        let mut topic_routing = HashMap::new();
        topic_routing.insert("signals".to_string(), "signals.trading".to_string());
        topic_routing.insert("orders".to_string(), "orders.execution".to_string());
        topic_routing.insert("fills".to_string(), "executions.fills".to_string());
        topic_routing.insert("positions".to_string(), "portfolio.positions".to_string());
        
        Self {
            environment: Environment::Development,
            message_broker: MessageBroker {
                address: "localhost".to_string(),
                port: 8080,
                max_connections: 10,
                buffer_size: 8192,
                compression_enabled: true,
                batch_size: 1000,
                flush_interval: Duration::from_millis(100),
            },
            publish_topics: vec![
                "orders.btc".to_string(),
                "orders.eth".to_string(),
                "signals.trading".to_string(),
            ],
            subscribe_topics: vec![
                "market_data.orderbook".to_string(),
                "portfolio.balances".to_string(),
                "executions.fills".to_string(),
            ],
            topic_routing,
            strategies: vec![
                StrategyConfig {
                    id: "market_maker_btc".to_string(),
                    name: "BTC Market Maker".to_string(),
                    strategy_type: "MarketMaking".to_string(),
                    enabled: true,
                    symbols: vec!["BTC/USD".to_string()],
                    exchanges: vec!["kraken".to_string()],
                    parameters: HashMap::new(),
                    risk_limits: RiskLimits {
                        max_position_size: 10.0,
                        max_order_size: 1.0,
                        max_daily_loss: 1000.0,
                        max_position_value: 50000.0,
                    },
                }
            ],
            exchanges: vec![
                ExchangeConfig {
                    name: "kraken".to_string(),
                    enabled: true,
                    api_key: None,
                    secret_key: None,
                    sandbox: false,
                    rate_limits: RateLimitConfig {
                        orders_per_second: 10,
                        requests_per_second: 50,
                        window_size: Duration::from_secs(60),
                    },
                    timeout: Duration::from_secs(30),
                }
            ],
            performance: PerformanceConfig {
                worker_threads: 4,
                cpu_affinity_enabled: false,
                cpu_cores: vec![],
                memory_limit_mb: 1024,
                latency_threshold_us: 100,
                metrics_enabled: true,
                metrics_interval: Duration::from_secs(60),
                simd_enabled: true,
            },
            security: SecurityConfig {
                api_key_required: false,
                request_signing_required: false,
                rate_limiting_enabled: true,
                max_requests_per_minute: 1000,
                blocked_ips: vec![],
                allowed_ips: vec![],
                encryption_enabled: true,
            },
            monitoring: MonitoringConfig {
                health_check_enabled: true,
                health_check_interval: Duration::from_secs(30),
                metrics_retention_days: 7,
                alerting_enabled: false,
                alert_thresholds: AlertThresholds {
                    latency_us: 1000.0,
                    error_rate_percent: 5.0,
                    cpu_usage_percent: 80.0,
                    memory_usage_percent: 85.0,
                    connection_failures: 10,
                },
                prometheus_enabled: true,
                prometheus_port: 9090,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "json".to_string(),
                output: vec!["stdout".to_string()],
                file_path: None,
                max_file_size_mb: 100,
                max_files: 5,
                structured_logging: true,
            },
            last_modified: None,
            config_path: None,
        }
    }
}