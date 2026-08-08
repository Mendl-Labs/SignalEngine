use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use std::thread;
use serde::{Serialize, Deserialize};
use exchangemetricaggregator::ExchangeMetricsAggregator;
use dashmap::DashMap;
use crossbeam::utils::CachePadded;

#[cfg(feature = "postgres")]
pub mod database;

#[cfg(feature = "postgres")]
pub use database::{
    OrderDatabasePersistence, 
    DbPool,
    ExchangeCredential,
    load_exchange_credentials,
    load_credentials_for_exchange,
    create_pool,
    BackgroundSorWriter,
    SorDbEvent,
};

// Stub types when postgres feature is not enabled, so downstream crates compile.
#[cfg(not(feature = "postgres"))]
pub type DbPool = ();

#[cfg(not(feature = "postgres"))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeCredential {
    pub id: uuid::Uuid,
    pub exchange: String,
    pub label: String,
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
    pub is_testnet: bool,
    pub is_enabled: bool,
}

#[cfg(not(feature = "postgres"))]
pub async fn load_exchange_credentials(
    _pool: &DbPool,
) -> anyhow::Result<Vec<ExchangeCredential>> {
    Ok(vec![])
}

#[cfg(not(feature = "postgres"))]
pub async fn load_credentials_for_exchange(
    _pool: &DbPool,
    _exchange: &str,
    _live_only: bool,
) -> anyhow::Result<Option<ExchangeCredential>> {
    Ok(None)
}

#[cfg(not(feature = "postgres"))]
pub async fn create_pool(_database_url: &str) -> anyhow::Result<DbPool> {
    Ok(())
}

/// Order side enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl std::fmt::Display for OrderSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderSide::Buy => write!(f, "BUY"),
            OrderSide::Sell => write!(f, "SELL"),
        }
    }
}

/// Order type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    StopLimit,
    Iceberg,
    TWAP,
    VWAP,
    Implementation,  // Implementation Shortfall
}

/// Order time in force
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    IOC,  // Immediate or Cancel
    FOK,  // Fill or Kill
    GTC,  // Good Till Cancel
    DAY,  // Good for Day
    GTD,  // Good Till Date
}

/// Execution urgency level (exported for compatibility)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionUrgency {
    Low,     // Optimize for cost, can take time
    Medium,  // Balance cost and time
    High,    // Optimize for speed, cost secondary
    Critical, // Execute immediately regardless of cost
}

/// Route execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RouteStatus {
    Pending,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
    Failed,
}

/// Routing algorithm type (for compatibility)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingAlgorithm {
    TWAP,
    VWAP,
    ImplementationShortfall,
    SmartRouting,
    MinimizeMarketImpact,
}

/// Individual child order within a route
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildOrder {
    pub id: String,
    pub exchange: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,  // None for market orders
    pub filled_quantity: f64,
    pub avg_fill_price: f64,
    pub status: RouteStatus,
    pub created_at: u64,
    pub updated_at: u64,
    pub fees_paid: f64,
    pub exchange_order_id: Option<String>,
    pub rejection_reason: Option<String>,
    pub time_in_force: TimeInForce,
}

impl ChildOrder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        exchange: String,
        symbol: String,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        time_in_force: TimeInForce,
    ) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        
        Self {
            id,
            exchange,
            symbol,
            side,
            order_type,
            quantity,
            price,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            status: RouteStatus::Pending,
            created_at: now,
            updated_at: now,
            fees_paid: 0.0,
            exchange_order_id: None,
            rejection_reason: None,
            time_in_force,
        }
    }
    
    pub fn is_complete(&self) -> bool {
        matches!(self.status, RouteStatus::Filled | RouteStatus::Cancelled | RouteStatus::Rejected | RouteStatus::Expired | RouteStatus::Failed)
    }
    
    pub fn remaining_quantity(&self) -> f64 {
        (self.quantity - self.filled_quantity).max(0.0)
    }
    
    pub fn fill_rate(&self) -> f64 {
        if self.quantity > 0.0 {
            self.filled_quantity / self.quantity
        } else {
            0.0
        }
    }
}

/// Smart order route containing multiple child orders
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmartOrderRoute {
    pub id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub total_quantity: f64,
    pub urgency: ExecutionUrgency,
    pub child_orders: Vec<ChildOrder>,
    pub created_at: u64,
    pub updated_at: u64,
    pub status: RouteStatus,
    pub target_completion_time: Option<u64>,
    pub max_slippage: f64,
    pub benchmark_price: Option<f64>,
    pub total_filled_quantity: f64,
    pub weighted_avg_fill_price: f64,
    pub total_fees_paid: f64,
}

impl SmartOrderRoute {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        symbol: String,
        side: OrderSide,
        total_quantity: f64,
        urgency: ExecutionUrgency,
        target_completion_time: Option<u64>,
        max_slippage: f64,
        benchmark_price: Option<f64>,
    ) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        
        Self {
            id,
            symbol,
            side,
            total_quantity,
            urgency,
            child_orders: Vec::new(),
            created_at: now,
            updated_at: now,
            status: RouteStatus::Pending,
            target_completion_time,
            max_slippage,
            benchmark_price,
            total_filled_quantity: 0.0,
            weighted_avg_fill_price: 0.0,
            total_fees_paid: 0.0,
        }
    }
    
    pub fn add_child_order(&mut self, child_order: ChildOrder) {
        self.child_orders.push(child_order);
        self.updated_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    }
    
    pub fn update_from_fill(&mut self, child_order_id: &str, filled_qty: f64, fill_price: f64, fees: f64) {
        if let Some(child) = self.child_orders.iter_mut().find(|c| c.id == child_order_id) {
            child.filled_quantity += filled_qty;
            child.avg_fill_price = ((child.avg_fill_price * (child.filled_quantity - filled_qty)) + (fill_price * filled_qty)) / child.filled_quantity;
            child.fees_paid += fees;
            child.updated_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            
            if child.filled_quantity >= child.quantity {
                child.status = RouteStatus::Filled;
            } else {
                child.status = RouteStatus::PartiallyFilled;
            }
            
            // Update route-level statistics
            self.recalculate_totals();
        }
    }
    
    pub fn recalculate_totals(&mut self) {
        let mut total_filled = 0.0;
        let mut weighted_price_sum = 0.0;
        let mut total_fees = 0.0;
        
        for child in &self.child_orders {
            total_filled += child.filled_quantity;
            weighted_price_sum += child.avg_fill_price * child.filled_quantity;
            total_fees += child.fees_paid;
        }
        
        self.total_filled_quantity = total_filled;
        self.weighted_avg_fill_price = if total_filled > 0.0 { weighted_price_sum / total_filled } else { 0.0 };
        self.total_fees_paid = total_fees;
        
        // Update status based on fill progress
        if total_filled >= self.total_quantity {
            self.status = RouteStatus::Filled;
        } else if total_filled > 0.0 {
            self.status = RouteStatus::PartiallyFilled;
        }
        
        self.updated_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    }
    
    pub fn remaining_quantity(&self) -> f64 {
        (self.total_quantity - self.total_filled_quantity).max(0.0)
    }
    
    pub fn fill_rate(&self) -> f64 {
        if self.total_quantity > 0.0 {
            self.total_filled_quantity / self.total_quantity
        } else {
            0.0
        }
    }
    
    pub fn is_complete(&self) -> bool {
        matches!(self.status, RouteStatus::Filled | RouteStatus::Cancelled | RouteStatus::Rejected | RouteStatus::Expired | RouteStatus::Failed)
    }
    
    pub fn slippage(&self) -> Option<f64> {
        self.benchmark_price.map(|benchmark| {
            if benchmark > 0.0 {
                (self.weighted_avg_fill_price - benchmark) / benchmark
            } else {
                0.0
            }
        })
    }
}

/// Ultra-Fast Smart Order Router - main export for compatibility
pub type SmartOrderRouter = UltraFastSmartOrderRouter;

/// Ultra-Fast Smart Order Router with lock-free concurrent access
pub struct UltraFastSmartOrderRouter {
    // Lock-free concurrent data structures
    active_routes: Arc<DashMap<String, SmartOrderRoute>>,
    exchange_configs: Arc<DashMap<String, ExchangeConfig>>,
    routing_rules: Arc<DashMap<String, RoutingRule>>,
    
    // Atomic performance statistics
    stats: Arc<AtomicRoutingStats>,
    
    // Lock-free metrics aggregator
    _metrics_aggregator: Arc<ExchangeMetricsAggregator>,
    
    // Background processing state
    is_running: Arc<AtomicBool>,
}

/// Ultra-high performance atomic routing statistics
#[derive(Debug)]
pub struct AtomicRoutingStats {
    pub routes_created: CachePadded<AtomicU64>,
    pub routes_completed: CachePadded<AtomicU64>,
    pub routes_failed: CachePadded<AtomicU64>,
    pub total_volume_routed: CachePadded<AtomicU64>, // In cents to avoid floating point
    pub total_fees_paid: CachePadded<AtomicU64>,     // In cents
    pub avg_routing_latency_ns: CachePadded<AtomicU64>,
    pub successful_fills: CachePadded<AtomicU64>,
    pub partial_fills: CachePadded<AtomicU64>,
    pub rejections: CachePadded<AtomicU64>,
    pub timeouts: CachePadded<AtomicU64>,
}

impl Default for AtomicRoutingStats {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicRoutingStats {
    pub fn new() -> Self {
        Self {
            routes_created: CachePadded::new(AtomicU64::new(0)),
            routes_completed: CachePadded::new(AtomicU64::new(0)),
            routes_failed: CachePadded::new(AtomicU64::new(0)),
            total_volume_routed: CachePadded::new(AtomicU64::new(0)),
            total_fees_paid: CachePadded::new(AtomicU64::new(0)),
            avg_routing_latency_ns: CachePadded::new(AtomicU64::new(0)),
            successful_fills: CachePadded::new(AtomicU64::new(0)),
            partial_fills: CachePadded::new(AtomicU64::new(0)),
            rejections: CachePadded::new(AtomicU64::new(0)),
            timeouts: CachePadded::new(AtomicU64::new(0)),
        }
    }
    
    #[inline(always)]
    pub fn increment_routes_created(&self) {
        self.routes_created.fetch_add(1, Ordering::Relaxed);
    }
    
    #[inline(always)]
    pub fn increment_routes_completed(&self) {
        self.routes_completed.fetch_add(1, Ordering::Relaxed);
    }
    
    #[inline(always)]
    pub fn increment_routes_failed(&self) {
        self.routes_failed.fetch_add(1, Ordering::Relaxed);
    }
    
    #[inline(always)]
    pub fn add_volume_routed(&self, volume_cents: u64) {
        self.total_volume_routed.fetch_add(volume_cents, Ordering::Relaxed);
    }
    
    #[inline(always)]
    pub fn add_fees_paid(&self, fees_cents: u64) {
        self.total_fees_paid.fetch_add(fees_cents, Ordering::Relaxed);
    }
    
    #[inline(always)]
    pub fn update_routing_latency(&self, latency_ns: u64) {
        // Simple exponential moving average for latency
        let current = self.avg_routing_latency_ns.load(Ordering::Relaxed);
        let new_avg = if current == 0 { 
            latency_ns 
        } else { 
            (current * 9 + latency_ns) / 10 // 90% old, 10% new
        };
        self.avg_routing_latency_ns.store(new_avg, Ordering::Relaxed);
    }
    
    pub fn success_rate(&self) -> f64 {
        let completed = self.routes_completed.load(Ordering::Relaxed);
        let failed = self.routes_failed.load(Ordering::Relaxed);
        let total = completed + failed;
        
        if total > 0 {
            completed as f64 / total as f64
        } else {
            0.0
        }
    }
}

/// Exchange configuration for routing decisions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub name: String,
    pub enabled: bool,
    pub max_order_size: f64,
    pub min_order_size: f64,
    pub fee_rate: f64,
    pub latency_penalty: f64,
    pub reliability_score: f64,
    pub supported_order_types: Vec<OrderType>,
    pub supported_time_in_force: Vec<TimeInForce>,
}

/// Routing rule for symbol/exchange combinations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    pub symbol: String,
    pub exchange_priority: Vec<String>,
    pub max_allocation_pct: HashMap<String, f64>,
    pub min_liquidity_threshold: f64,
    pub urgency_routing: HashMap<ExecutionUrgency, Vec<String>>,
}

impl UltraFastSmartOrderRouter {
    pub fn new(metrics_aggregator: Arc<ExchangeMetricsAggregator>) -> Self {
        Self {
            active_routes: Arc::new(DashMap::new()),
            exchange_configs: Arc::new(DashMap::new()),
            routing_rules: Arc::new(DashMap::new()),
            stats: Arc::new(AtomicRoutingStats::new()),
            _metrics_aggregator: metrics_aggregator,
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }
    
    /// Start the background processing threads
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed).is_err() {
            return Err("Router is already running".into());
        }
        
        // Start monitoring thread
        let routes = Arc::clone(&self.active_routes);
        let stats = Arc::clone(&self.stats);
        let running = Arc::clone(&self.is_running);
        
        thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                // Monitor and clean up completed routes
                let mut completed_routes = Vec::new();
                
                for entry in routes.iter() {
                    if entry.value().is_complete() {
                        completed_routes.push(entry.key().clone());
                    }
                }
                
                // Remove completed routes (keep recent ones for performance analysis)
                for route_id in completed_routes {
                    if let Some((_, route)) = routes.remove(&route_id) {
                        if route.status == RouteStatus::Filled {
                            stats.increment_routes_completed();
                        } else {
                            stats.increment_routes_failed();
                        }
                    }
                }
                
                thread::sleep(Duration::from_millis(100));
            }
        });
        
        Ok(())
    }
    
    /// Stop the background processing
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }
    
    /// Add or update an exchange configuration
    pub fn configure_exchange(&self, config: ExchangeConfig) {
        self.exchange_configs.insert(config.name.clone(), config);
    }
    
    /// Add or update a routing rule
    pub fn set_routing_rule(&self, rule: RoutingRule) {
        self.routing_rules.insert(rule.symbol.clone(), rule);
    }
    
    /// Create and route a smart order with ultra-low latency
    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    pub fn create_smart_route(
        &self,
        route_id: String,
        symbol: String,
        side: OrderSide,
        quantity: f64,
        urgency: ExecutionUrgency,
        target_completion_time: Option<u64>,
        max_slippage: f64,
        benchmark_price: Option<f64>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        
        // Create the route
        let mut route = SmartOrderRoute::new(
            route_id.clone(),
            symbol.clone(),
            side,
            quantity,
            urgency,
            target_completion_time,
            max_slippage,
            benchmark_price,
        );
        
        // Generate child orders using optimized routing algorithm
        let child_orders = self.generate_optimal_child_orders(&symbol, side, quantity, urgency)?;
        
        for child_order in child_orders {
            route.add_child_order(child_order);
        }
        
        // Store route in concurrent map
        self.active_routes.insert(route_id.clone(), route);
        
        // Update statistics atomically
        self.stats.increment_routes_created();
        self.stats.add_volume_routed((quantity * benchmark_price.unwrap_or(100.0) * 100.0) as u64);
        
        let routing_latency = start_time.elapsed().as_nanos() as u64;
        self.stats.update_routing_latency(routing_latency);
        
        Ok(route_id)
    }
    
    /// Get route status with zero-copy access
    #[inline(always)]
    pub fn get_route_status(&self, route_id: &str) -> Option<RouteStatus> {
        self.active_routes.get(route_id).map(|route| route.status)
    }
    
    /// Update route from fill with lock-free atomic operations
    #[inline(always)]
    pub fn update_route_from_fill(
        &self,
        route_id: &str,
        child_order_id: &str,
        filled_qty: f64,
        fill_price: f64,
        fees: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut route) = self.active_routes.get_mut(route_id) {
            route.update_from_fill(child_order_id, filled_qty, fill_price, fees);
            
            // Update atomic statistics
            self.stats.add_fees_paid((fees * 100.0) as u64); // Convert to cents
            
            if filled_qty > 0.0 {
                self.stats.successful_fills.fetch_add(1, Ordering::Relaxed);
            }
            
            Ok(())
        } else {
            Err(format!("Route {} not found", route_id).into())
        }
    }
    
    /// Generate optimal child orders using advanced routing algorithms
    fn generate_optimal_child_orders(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        urgency: ExecutionUrgency,
    ) -> Result<Vec<ChildOrder>, Box<dyn std::error::Error>> {
        let mut child_orders = Vec::new();
        
        // Get routing rule for symbol
        let routing_rule = self.routing_rules.get(symbol);
        
        match urgency {
            ExecutionUrgency::Critical => {
                // Single large order to most liquid exchange
                if let Some(rule) = routing_rule {
                    if let Some(best_exchange) = rule.exchange_priority.first() {
                        let child_order = ChildOrder::new(
                            format!("child_{}_{}", uuid::Uuid::new_v4(), 1),
                            best_exchange.clone(),
                            symbol.to_string(),
                            side,
                            OrderType::Market,
                            quantity,
                            None, // Market order
                            TimeInForce::IOC,
                        );
                        child_orders.push(child_order);
                    }
                }
            },
            ExecutionUrgency::High => {
                // Split across top 2-3 exchanges
                if let Some(rule) = routing_rule {
                    let exchanges = &rule.exchange_priority[..3.min(rule.exchange_priority.len())];
                    let qty_per_exchange = quantity / exchanges.len() as f64;
                    
                    for (i, exchange) in exchanges.iter().enumerate() {
                        let child_order = ChildOrder::new(
                            format!("child_{}_{}", uuid::Uuid::new_v4(), i + 1),
                            exchange.clone(),
                            symbol.to_string(),
                            side,
                            OrderType::Limit,
                            qty_per_exchange,
                            None, // Will be set based on current market
                            TimeInForce::IOC,
                        );
                        child_orders.push(child_order);
                    }
                }
            },
            ExecutionUrgency::Medium => {
                // TWAP-style execution across multiple exchanges
                if let Some(rule) = routing_rule {
                    let exchanges = &rule.exchange_priority;
                    let qty_per_exchange = quantity / exchanges.len() as f64;
                    
                    for (i, exchange) in exchanges.iter().enumerate() {
                        let child_order = ChildOrder::new(
                            format!("child_{}_{}", uuid::Uuid::new_v4(), i + 1),
                            exchange.clone(),
                            symbol.to_string(),
                            side,
                            OrderType::TWAP,
                            qty_per_exchange,
                            None,
                            TimeInForce::GTC,
                        );
                        child_orders.push(child_order);
                    }
                }
            },
            ExecutionUrgency::Low => {
                // Cost-optimized execution with limit orders
                if let Some(rule) = routing_rule {
                    if let Some(cheapest_exchange) = rule.exchange_priority.last() {
                        let child_order = ChildOrder::new(
                            format!("child_{}_{}", uuid::Uuid::new_v4(), 1),
                            cheapest_exchange.clone(),
                            symbol.to_string(),
                            side,
                            OrderType::Limit,
                            quantity,
                            None, // Will be set based on best bid/ask
                            TimeInForce::GTC,
                        );
                        child_orders.push(child_order);
                    }
                }
            },
        }
        
        if child_orders.is_empty() {
            // Fallback: single market order
            child_orders.push(ChildOrder::new(
                format!("child_fallback_{}", uuid::Uuid::new_v4()),
                "default_exchange".to_string(),
                symbol.to_string(),
                side,
                OrderType::Market,
                quantity,
                None,
                TimeInForce::IOC,
            ));
        }
        
        Ok(child_orders)
    }
    
    /// Get comprehensive routing performance statistics
    pub fn get_performance_stats(&self) -> RoutingPerformanceStats {
        RoutingPerformanceStats {
            routes_created: self.stats.routes_created.load(Ordering::Relaxed),
            routes_completed: self.stats.routes_completed.load(Ordering::Relaxed),
            routes_failed: self.stats.routes_failed.load(Ordering::Relaxed),
            total_volume_routed_dollars: self.stats.total_volume_routed.load(Ordering::Relaxed) as f64 / 100.0,
            total_fees_paid_dollars: self.stats.total_fees_paid.load(Ordering::Relaxed) as f64 / 100.0,
            avg_routing_latency_ns: self.stats.avg_routing_latency_ns.load(Ordering::Relaxed),
            successful_fills: self.stats.successful_fills.load(Ordering::Relaxed),
            partial_fills: self.stats.partial_fills.load(Ordering::Relaxed),
            rejections: self.stats.rejections.load(Ordering::Relaxed),
            timeouts: self.stats.timeouts.load(Ordering::Relaxed),
            success_rate: self.stats.success_rate(),
            active_routes_count: self.active_routes.len() as u64,
        }
    }
    
    /// Get all active routes (for monitoring)
    pub fn get_active_routes(&self) -> Vec<SmartOrderRoute> {
        self.active_routes.iter().map(|entry| entry.value().clone()).collect()
    }
    
    /// Cancel a route and all its child orders
    pub fn cancel_route(&self, route_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut route) = self.active_routes.get_mut(route_id) {
            route.status = RouteStatus::Cancelled;
            
            for child_order in &mut route.child_orders {
                if !child_order.is_complete() {
                    child_order.status = RouteStatus::Cancelled;
                }
            }
            
            route.updated_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            
            Ok(())
        } else {
            Err(format!("Route {} not found", route_id).into())
        }
    }
}

/// Performance statistics for routing operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingPerformanceStats {
    pub routes_created: u64,
    pub routes_completed: u64,
    pub routes_failed: u64,
    pub total_volume_routed_dollars: f64,
    pub total_fees_paid_dollars: f64,
    pub avg_routing_latency_ns: u64,
    pub successful_fills: u64,
    pub partial_fills: u64,
    pub rejections: u64,
    pub timeouts: u64,
    pub success_rate: f64,
    pub active_routes_count: u64,
}

/// Create default exchange configurations for testing
pub fn create_default_exchange_configs() -> Vec<ExchangeConfig> {
    vec![
        ExchangeConfig {
            name: "binance".to_string(),
            enabled: true,
            max_order_size: 1000000.0,
            min_order_size: 0.001,
            fee_rate: 0.001,
            latency_penalty: 1.0,
            reliability_score: 0.99,
            supported_order_types: vec![OrderType::Market, OrderType::Limit, OrderType::StopLimit],
            supported_time_in_force: vec![TimeInForce::IOC, TimeInForce::FOK, TimeInForce::GTC],
        },
        ExchangeConfig {
            name: "coinbase".to_string(),
            enabled: true,
            max_order_size: 500000.0,
            min_order_size: 0.01,
            fee_rate: 0.005,
            latency_penalty: 1.2,
            reliability_score: 0.98,
            supported_order_types: vec![OrderType::Market, OrderType::Limit],
            supported_time_in_force: vec![TimeInForce::IOC, TimeInForce::GTC],
        },
        ExchangeConfig {
            name: "kraken".to_string(),
            enabled: true,
            max_order_size: 100000.0,
            min_order_size: 0.1,
            fee_rate: 0.0025,
            latency_penalty: 1.5,
            reliability_score: 0.97,
            supported_order_types: vec![OrderType::Market, OrderType::Limit, OrderType::StopLimit],
            supported_time_in_force: vec![TimeInForce::IOC, TimeInForce::GTC, TimeInForce::DAY],
        },
    ]
}

/// Create default routing rules for common symbols
pub fn create_default_routing_rules() -> Vec<RoutingRule> {
    vec![
        RoutingRule {
            symbol: "BTC/USD".to_string(),
            exchange_priority: vec!["binance".to_string(), "coinbase".to_string(), "kraken".to_string()],
            max_allocation_pct: [
                ("binance".to_string(), 0.5),
                ("coinbase".to_string(), 0.3),
                ("kraken".to_string(), 0.2),
            ].into_iter().collect(),
            min_liquidity_threshold: 10000.0,
            urgency_routing: [
                (ExecutionUrgency::Critical, vec!["binance".to_string()]),
                (ExecutionUrgency::High, vec!["binance".to_string(), "coinbase".to_string()]),
                (ExecutionUrgency::Medium, vec!["binance".to_string(), "coinbase".to_string(), "kraken".to_string()]),
                (ExecutionUrgency::Low, vec!["kraken".to_string()]),
            ].into_iter().collect(),
        },
        RoutingRule {
            symbol: "ETH/USD".to_string(),
            exchange_priority: vec!["binance".to_string(), "coinbase".to_string(), "kraken".to_string()],
            max_allocation_pct: [
                ("binance".to_string(), 0.4),
                ("coinbase".to_string(), 0.4),
                ("kraken".to_string(), 0.2),
            ].into_iter().collect(),
            min_liquidity_threshold: 5000.0,
            urgency_routing: [
                (ExecutionUrgency::Critical, vec!["binance".to_string()]),
                (ExecutionUrgency::High, vec!["binance".to_string(), "coinbase".to_string()]),
                (ExecutionUrgency::Medium, vec!["binance".to_string(), "coinbase".to_string(), "kraken".to_string()]),
                (ExecutionUrgency::Low, vec!["kraken".to_string()]),
            ].into_iter().collect(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_atomic_routing_stats() {
        let stats = AtomicRoutingStats::new();
        
        stats.increment_routes_created();
        stats.increment_routes_completed();
        stats.add_volume_routed(100000); // $1000.00
        stats.add_fees_paid(100); // $1.00
        stats.update_routing_latency(1000);
        
        assert_eq!(stats.routes_created.load(Ordering::Relaxed), 1);
        assert_eq!(stats.routes_completed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_volume_routed.load(Ordering::Relaxed), 100000);
        assert_eq!(stats.total_fees_paid.load(Ordering::Relaxed), 100);
        assert_eq!(stats.avg_routing_latency_ns.load(Ordering::Relaxed), 1000);
        assert_eq!(stats.success_rate(), 1.0);
    }
    
    #[test]
    fn test_smart_order_route_creation() {
        let route = SmartOrderRoute::new(
            "test_route_1".to_string(),
            "BTC/USD".to_string(),
            OrderSide::Buy,
            1.0,
            ExecutionUrgency::High,
            None,
            0.01,
            Some(50000.0),
        );
        
        assert_eq!(route.id, "test_route_1");
        assert_eq!(route.symbol, "BTC/USD");
        assert_eq!(route.side, OrderSide::Buy);
        assert_eq!(route.total_quantity, 1.0);
        assert_eq!(route.urgency, ExecutionUrgency::High);
        assert_eq!(route.status, RouteStatus::Pending);
        assert_eq!(route.remaining_quantity(), 1.0);
        assert_eq!(route.fill_rate(), 0.0);
    }
    
    #[test]
    fn test_child_order_creation() {
        let child = ChildOrder::new(
            "child_1".to_string(),
            "binance".to_string(),
            "BTC/USD".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            0.5,
            Some(49500.0),
            TimeInForce::GTC,
        );
        
        assert_eq!(child.id, "child_1");
        assert_eq!(child.exchange, "binance");
        assert_eq!(child.quantity, 0.5);
        assert_eq!(child.price, Some(49500.0));
        assert_eq!(child.status, RouteStatus::Pending);
        assert!(!child.is_complete());
        assert_eq!(child.remaining_quantity(), 0.5);
    }
    
    #[tokio::test]
    async fn test_ultra_fast_router_basic_operations() {
        let metrics_aggregator = Arc::new(ExchangeMetricsAggregator::new(1000, 100));
        let router = UltraFastSmartOrderRouter::new(metrics_aggregator);
        
        // Configure exchanges
        let configs = create_default_exchange_configs();
        for config in configs {
            router.configure_exchange(config);
        }
        
        // Set routing rules
        let rules = create_default_routing_rules();
        for rule in rules {
            router.set_routing_rule(rule);
        }
        
        // Create a route
        let route_id = router.create_smart_route(
            "test_route_1".to_string(),
            "BTC/USD".to_string(),
            OrderSide::Buy,
            1.0,
            ExecutionUrgency::High,
            None,
            0.01,
            Some(50000.0),
        ).unwrap();
        
        // Check status
        let status = router.get_route_status(&route_id);
        assert_eq!(status, Some(RouteStatus::Pending));
        
        // Get stats
        let stats = router.get_performance_stats();
        assert_eq!(stats.routes_created, 1);
        assert_eq!(stats.active_routes_count, 1);
    }
}
