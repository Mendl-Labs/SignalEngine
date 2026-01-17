//! Hot Configuration Reload Module
//!
//! Provides runtime configuration updates without service restart.
//! Watches config files and safely applies changes to trading parameters.
//!
//! # Features
//!
//! - **File watching**: Detects config file changes via filesystem events
//! - **Safe updates**: Validates new config before applying
//! - **Atomic swap**: Uses ArcSwap for lock-free config access
//! - **Rollback**: Reverts to previous config on validation failure
//! - **Audit trail**: Logs all config changes with timestamps
//!
//! # Supported Runtime Updates
//!
//! - Risk limits (position size, drawdown thresholds)
//! - Circuit breaker thresholds
//! - Fat-finger protection limits
//! - Rate limits
//! - Alert thresholds
//!
//! # NOT Hot-Reloadable (Requires Restart)
//!
//! - Exchange credentials
//! - WAL directory paths
//! - Network bindings
//!
//! # Example
//!
//! ```rust,ignore
//! use executionhandler::hot_config::{HotConfigManager, RuntimeConfig};
//!
//! let manager = HotConfigManager::new("./config/trading.yaml").await?;
//!
//! // Get current config (lock-free read)
//! let config = manager.get_config();
//! println!("Max position: {}", config.risk.max_position_usd);
//!
//! // Subscribe to config changes
//! let mut rx = manager.subscribe();
//! tokio::spawn(async move {
//!     while let Ok(new_config) = rx.recv().await {
//!         println!("Config updated: {:?}", new_config);
//!     }
//! });
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use notify::{Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};

/// Runtime configuration that can be hot-reloaded
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Risk management settings
    pub risk: RiskConfig,
    /// Circuit breaker settings
    pub circuit_breaker: CircuitBreakerConfig,
    /// Fat-finger protection settings
    pub fat_finger: FatFingerConfig,
    /// Rate limiting settings
    pub rate_limits: RateLimitConfig,
    /// Alert thresholds
    pub alerts: AlertConfig,
    /// Logging settings
    pub logging: LoggingConfig,
    /// Config version (for tracking)
    #[serde(default)]
    pub version: u64,
    /// Last modified timestamp
    #[serde(default)]
    pub last_modified_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            risk: RiskConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            fat_finger: FatFingerConfig::default(),
            rate_limits: RateLimitConfig::default(),
            alerts: AlertConfig::default(),
            logging: LoggingConfig::default(),
            version: 1,
            last_modified_ms: 0,
        }
    }
}

/// Risk management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Maximum position value per symbol (USD)
    pub max_position_usd: f64,
    /// Maximum total portfolio exposure (USD)
    pub max_portfolio_usd: f64,
    /// Maximum drawdown before kill switch (percentage)
    pub max_drawdown_pct: f64,
    /// Daily loss limit (USD)
    pub daily_loss_limit_usd: f64,
    /// Maximum concentration per symbol (percentage)
    pub max_concentration_pct: f64,
    /// VaR limit (percentage)
    pub var_limit_pct: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_position_usd: 100_000.0,
            max_portfolio_usd: 1_000_000.0,
            max_drawdown_pct: 10.0,
            daily_loss_limit_usd: 10_000.0,
            max_concentration_pct: 25.0,
            var_limit_pct: 5.0,
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Error threshold before opening circuit
    pub error_threshold: u32,
    /// Success threshold to close circuit
    pub success_threshold: u32,
    /// Half-open timeout (seconds)
    pub half_open_timeout_secs: u64,
    /// Open state timeout (seconds)
    pub open_timeout_secs: u64,
    /// Consecutive failures for per-exchange breaker
    pub exchange_failure_threshold: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            error_threshold: 5,
            success_threshold: 3,
            half_open_timeout_secs: 30,
            open_timeout_secs: 60,
            exchange_failure_threshold: 3,
        }
    }
}

/// Fat-finger protection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FatFingerConfig {
    /// Maximum price deviation from market (percentage)
    pub max_price_deviation_pct: f64,
    /// Maximum single order notional (USD)
    pub max_order_notional_usd: f64,
    /// Maximum orders per second
    pub max_orders_per_second: u32,
    /// Maximum orders per minute
    pub max_orders_per_minute: u32,
    /// Price staleness threshold (milliseconds)
    pub price_staleness_ms: u64,
    /// Require market price for order validation
    pub require_market_price: bool,
}

impl Default for FatFingerConfig {
    fn default() -> Self {
        Self {
            max_price_deviation_pct: 5.0,
            max_order_notional_usd: 100_000.0,
            max_orders_per_second: 10,
            max_orders_per_minute: 100,
            price_staleness_ms: 5000,
            require_market_price: true,
        }
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Global requests per second
    pub global_rps: u32,
    /// Per-exchange rate limits
    pub exchange_limits: HashMap<String, u32>,
    /// Burst allowance multiplier
    pub burst_multiplier: f64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        let mut exchange_limits = HashMap::new();
        exchange_limits.insert("kraken".to_string(), 15);
        exchange_limits.insert("binance".to_string(), 1200);
        
        Self {
            global_rps: 100,
            exchange_limits,
            burst_multiplier: 1.5,
        }
    }
}

/// Alert configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Critical alert rate limit per minute
    pub critical_rate_limit: u32,
    /// Warning alert rate limit per minute
    pub warning_rate_limit: u32,
    /// Deduplication window (seconds)
    pub dedup_window_secs: u64,
    /// Enable PagerDuty alerts
    pub pagerduty_enabled: bool,
    /// Enable Slack alerts
    pub slack_enabled: bool,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            critical_rate_limit: 60,
            warning_rate_limit: 30,
            dedup_window_secs: 300,
            pagerduty_enabled: true,
            slack_enabled: true,
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    pub level: String,
    /// Enable JSON structured logging
    pub json_format: bool,
    /// Include timestamps
    pub include_timestamps: bool,
    /// Max log file size (MB)
    pub max_file_size_mb: u64,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            json_format: true,
            include_timestamps: true,
            max_file_size_mb: 100,
        }
    }
}

/// Configuration change event
#[derive(Debug, Clone)]
pub struct ConfigChangeEvent {
    /// Previous config version
    pub previous_version: u64,
    /// New config version
    pub new_version: u64,
    /// Timestamp of change
    pub timestamp_ms: u64,
    /// Changed sections
    pub changed_sections: Vec<String>,
    /// Source of change (file, api, etc.)
    pub source: String,
}

/// Validation error
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Validation error: {0}")]
    Validation(String),
    
    #[error("Watch error: {0}")]
    Watch(String),
}

/// Hot config manager
pub struct HotConfigManager {
    /// Current config (lock-free access)
    config: Arc<ArcSwap<RuntimeConfig>>,
    /// Config file path
    config_path: PathBuf,
    /// Version counter
    version: AtomicU64,
    /// Change broadcast channel
    change_tx: broadcast::Sender<ConfigChangeEvent>,
    /// Config history for rollback
    history: RwLock<Vec<RuntimeConfig>>,
    /// Maximum history entries
    max_history: usize,
    /// File watcher handle
    _watcher: Option<RecommendedWatcher>,
}

impl HotConfigManager {
    /// Create a new hot config manager
    pub async fn new(config_path: impl AsRef<Path>) -> Result<Arc<Self>, ConfigError> {
        let config_path = config_path.as_ref().to_path_buf();
        
        // Load initial config
        let initial_config = if config_path.exists() {
            Self::load_config_file(&config_path).await?
        } else {
            RuntimeConfig::default()
        };
        
        let (change_tx, _) = broadcast::channel(100);
        
        let manager = Arc::new(Self {
            config: Arc::new(ArcSwap::from_pointee(initial_config.clone())),
            config_path: config_path.clone(),
            version: AtomicU64::new(initial_config.version),
            change_tx,
            history: RwLock::new(vec![initial_config]),
            max_history: 10,
            _watcher: None,
        });
        
        // Start file watcher
        manager.clone().start_watcher()?;
        
        Ok(manager)
    }
    
    /// Load config from file
    async fn load_config_file(path: &Path) -> Result<RuntimeConfig, ConfigError> {
        let content = tokio::fs::read_to_string(path).await?;
        
        // Support both YAML and JSON
        let config: RuntimeConfig = if path.extension().map(|e| e == "yaml" || e == "yml").unwrap_or(false) {
            serde_yaml::from_str(&content)
                .map_err(|e| ConfigError::Parse(e.to_string()))?
        } else {
            serde_json::from_str(&content)
                .map_err(|e| ConfigError::Parse(e.to_string()))?
        };
        
        Ok(config)
    }
    
    /// Start file watcher
    fn start_watcher(self: Arc<Self>) -> Result<(), ConfigError> {
        let config_path = self.config_path.clone();
        let manager = self.clone();
        
        let (tx, mut rx) = mpsc::channel(10);
        
        // Create watcher
        let mut watcher = RecommendedWatcher::new(
            move |res: Result<Event, notify::Error>| {
                if let Ok(event) = res {
                    if event.kind.is_modify() {
                        let _ = tx.blocking_send(());
                    }
                }
            },
            NotifyConfig::default().with_poll_interval(Duration::from_secs(1)),
        ).map_err(|e| ConfigError::Watch(e.to_string()))?;
        
        // Watch the config file's parent directory
        if let Some(parent) = config_path.parent() {
            watcher.watch(parent, RecursiveMode::NonRecursive)
                .map_err(|e| ConfigError::Watch(e.to_string()))?;
        }
        
        // Spawn reload handler
        tokio::spawn(async move {
            // Debounce - wait for file to stabilize
            let mut pending = false;
            
            loop {
                tokio::select! {
                    Some(_) = rx.recv() => {
                        pending = true;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(500)), if pending => {
                        pending = false;
                        if let Err(e) = manager.reload().await {
                            eprintln!("Config reload failed: {}", e);
                        }
                    }
                    else => break,
                }
            }
        });
        
        Ok(())
    }
    
    /// Get current config (lock-free)
    pub fn get_config(&self) -> Arc<RuntimeConfig> {
        self.config.load().clone()
    }
    
    /// Subscribe to config changes
    pub fn subscribe(&self) -> broadcast::Receiver<ConfigChangeEvent> {
        self.change_tx.subscribe()
    }
    
    /// Reload config from file
    pub async fn reload(&self) -> Result<(), ConfigError> {
        let new_config = Self::load_config_file(&self.config_path).await?;
        self.apply_config(new_config, "file").await
    }
    
    /// Apply new config with validation
    pub async fn apply_config(&self, mut new_config: RuntimeConfig, source: &str) -> Result<(), ConfigError> {
        // Validate
        self.validate_config(&new_config)?;
        
        let old_config = self.get_config();
        let previous_version = old_config.version;
        
        // Update version and timestamp
        let new_version = self.version.fetch_add(1, Ordering::AcqRel) + 1;
        new_config.version = new_version;
        new_config.last_modified_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        // Detect changed sections
        let changed_sections = self.detect_changes(&old_config, &new_config);
        
        // Store in history
        {
            let mut history = self.history.write();
            history.push(new_config.clone());
            if history.len() > self.max_history {
                history.remove(0);
            }
        }
        
        // Atomic swap
        self.config.store(Arc::new(new_config));
        
        // Broadcast change event
        let event = ConfigChangeEvent {
            previous_version,
            new_version,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            changed_sections,
            source: source.to_string(),
        };
        
        let _ = self.change_tx.send(event);
        
        println!("Config reloaded: version {} -> {}", previous_version, new_version);
        
        Ok(())
    }
    
    /// Validate config
    fn validate_config(&self, config: &RuntimeConfig) -> Result<(), ConfigError> {
        // Risk limits validation
        if config.risk.max_position_usd <= 0.0 {
            return Err(ConfigError::Validation("max_position_usd must be positive".into()));
        }
        if config.risk.max_drawdown_pct <= 0.0 || config.risk.max_drawdown_pct > 100.0 {
            return Err(ConfigError::Validation("max_drawdown_pct must be between 0 and 100".into()));
        }
        if config.risk.max_concentration_pct <= 0.0 || config.risk.max_concentration_pct > 100.0 {
            return Err(ConfigError::Validation("max_concentration_pct must be between 0 and 100".into()));
        }
        
        // Circuit breaker validation
        if config.circuit_breaker.error_threshold == 0 {
            return Err(ConfigError::Validation("error_threshold must be positive".into()));
        }
        
        // Fat-finger validation
        if config.fat_finger.max_price_deviation_pct <= 0.0 {
            return Err(ConfigError::Validation("max_price_deviation_pct must be positive".into()));
        }
        if config.fat_finger.max_order_notional_usd <= 0.0 {
            return Err(ConfigError::Validation("max_order_notional_usd must be positive".into()));
        }
        
        // Rate limit validation
        if config.rate_limits.global_rps == 0 {
            return Err(ConfigError::Validation("global_rps must be positive".into()));
        }
        
        Ok(())
    }
    
    /// Detect which sections changed
    fn detect_changes(&self, old: &RuntimeConfig, new: &RuntimeConfig) -> Vec<String> {
        let mut changes = Vec::new();
        
        // Compare each section (simplified - in production use serde_diff or similar)
        if serde_json::to_string(&old.risk).ok() != serde_json::to_string(&new.risk).ok() {
            changes.push("risk".to_string());
        }
        if serde_json::to_string(&old.circuit_breaker).ok() != serde_json::to_string(&new.circuit_breaker).ok() {
            changes.push("circuit_breaker".to_string());
        }
        if serde_json::to_string(&old.fat_finger).ok() != serde_json::to_string(&new.fat_finger).ok() {
            changes.push("fat_finger".to_string());
        }
        if serde_json::to_string(&old.rate_limits).ok() != serde_json::to_string(&new.rate_limits).ok() {
            changes.push("rate_limits".to_string());
        }
        if serde_json::to_string(&old.alerts).ok() != serde_json::to_string(&new.alerts).ok() {
            changes.push("alerts".to_string());
        }
        if serde_json::to_string(&old.logging).ok() != serde_json::to_string(&new.logging).ok() {
            changes.push("logging".to_string());
        }
        
        changes
    }
    
    /// Rollback to previous version
    pub fn rollback(&self) -> Result<(), ConfigError> {
        let mut history = self.history.write();
        
        if history.len() < 2 {
            return Err(ConfigError::Validation("No previous config to rollback to".into()));
        }
        
        // Remove current
        history.pop();
        
        // Get previous
        let previous = history.last().cloned()
            .ok_or_else(|| ConfigError::Validation("History is empty".into()))?;
        
        // Apply
        self.config.store(Arc::new(previous));
        
        Ok(())
    }
    
    /// Update specific risk config
    pub async fn update_risk(&self, risk: RiskConfig) -> Result<(), ConfigError> {
        let mut config = (*self.get_config()).clone();
        config.risk = risk;
        self.apply_config(config, "api").await
    }
    
    /// Update fat-finger config
    pub async fn update_fat_finger(&self, fat_finger: FatFingerConfig) -> Result<(), ConfigError> {
        let mut config = (*self.get_config()).clone();
        config.fat_finger = fat_finger;
        self.apply_config(config, "api").await
    }
    
    /// Update circuit breaker config
    pub async fn update_circuit_breaker(&self, cb: CircuitBreakerConfig) -> Result<(), ConfigError> {
        let mut config = (*self.get_config()).clone();
        config.circuit_breaker = cb;
        self.apply_config(config, "api").await
    }
    
    /// Get config history
    pub fn get_history(&self) -> Vec<RuntimeConfig> {
        self.history.read().clone()
    }
    
    /// Save current config to file
    pub async fn save(&self) -> Result<(), ConfigError> {
        let config = self.get_config();
        
        let content = if self.config_path.extension().map(|e| e == "yaml" || e == "yml").unwrap_or(false) {
            serde_yaml::to_string(&*config)
                .map_err(|e| ConfigError::Parse(e.to_string()))?
        } else {
            serde_json::to_string_pretty(&*config)
                .map_err(|e| ConfigError::Parse(e.to_string()))?
        };
        
        tokio::fs::write(&self.config_path, content).await?;
        
        Ok(())
    }
}

/// Global config manager instance
pub static CONFIG_MANAGER: std::sync::OnceLock<Arc<HotConfigManager>> = std::sync::OnceLock::new();

/// Initialize global config manager
pub async fn init_config_manager(config_path: impl AsRef<Path>) -> Result<Arc<HotConfigManager>, ConfigError> {
    let manager = HotConfigManager::new(config_path).await?;
    CONFIG_MANAGER.set(manager.clone()).ok();
    Ok(manager)
}

/// Get global config (convenience function)
pub fn get_config() -> Option<Arc<RuntimeConfig>> {
    CONFIG_MANAGER.get().map(|m| m.get_config())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_default_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.json");
        
        let manager = HotConfigManager::new(&config_path).await.unwrap();
        let config = manager.get_config();
        
        assert_eq!(config.risk.max_position_usd, 100_000.0);
        assert_eq!(config.fat_finger.max_price_deviation_pct, 5.0);
    }
    
    #[tokio::test]
    async fn test_load_config_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.json");
        
        let custom_config = RuntimeConfig {
            risk: RiskConfig {
                max_position_usd: 50_000.0,
                ..Default::default()
            },
            ..Default::default()
        };
        
        let json = serde_json::to_string_pretty(&custom_config).unwrap();
        tokio::fs::write(&config_path, &json).await.unwrap();
        
        let manager = HotConfigManager::new(&config_path).await.unwrap();
        let config = manager.get_config();
        
        assert_eq!(config.risk.max_position_usd, 50_000.0);
    }
    
    #[tokio::test]
    async fn test_validation() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.json");
        
        let manager = HotConfigManager::new(&config_path).await.unwrap();
        
        // Invalid config should fail
        let invalid_config = RuntimeConfig {
            risk: RiskConfig {
                max_position_usd: -100.0, // Invalid
                ..Default::default()
            },
            ..Default::default()
        };
        
        let result = manager.apply_config(invalid_config, "test").await;
        assert!(result.is_err());
    }
    
    #[tokio::test]
    async fn test_update_risk() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.json");
        
        let manager = HotConfigManager::new(&config_path).await.unwrap();
        
        let new_risk = RiskConfig {
            max_position_usd: 200_000.0,
            ..Default::default()
        };
        
        manager.update_risk(new_risk).await.unwrap();
        
        let config = manager.get_config();
        assert_eq!(config.risk.max_position_usd, 200_000.0);
        assert_eq!(config.version, 2);
    }
    
    #[tokio::test]
    async fn test_rollback() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.json");
        
        let manager = HotConfigManager::new(&config_path).await.unwrap();
        
        // Make a change
        let new_risk = RiskConfig {
            max_position_usd: 200_000.0,
            ..Default::default()
        };
        manager.update_risk(new_risk).await.unwrap();
        assert_eq!(manager.get_config().risk.max_position_usd, 200_000.0);
        
        // Rollback
        manager.rollback().unwrap();
        assert_eq!(manager.get_config().risk.max_position_usd, 100_000.0);
    }
    
    #[tokio::test]
    async fn test_change_detection() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.json");
        
        let manager = HotConfigManager::new(&config_path).await.unwrap();
        let mut rx = manager.subscribe();
        
        // Make a change
        let new_risk = RiskConfig {
            max_position_usd: 200_000.0,
            ..Default::default()
        };
        manager.update_risk(new_risk).await.unwrap();
        
        // Check event
        let event = rx.try_recv().unwrap();
        assert!(event.changed_sections.contains(&"risk".to_string()));
        assert_eq!(event.source, "api");
    }
}
