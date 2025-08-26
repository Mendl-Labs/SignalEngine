use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;
use serde::{Serialize, Deserialize};
use exchangemetricaggregator::{ExchangeMetricsAggregator, ExchangeMetrics, SmartRoutingMetrics, RoutingPerformance};

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

/// Execution urgency level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    pub status: RouteStatus,
    pub created_at: u64,
    pub updated_at: u64,
    pub completion_time: Option<u64>,
    
    // Execution metrics
    pub total_filled: f64,
    pub volume_weighted_avg_price: f64,
    pub total_fees: f64,
    pub actual_slippage_bps: f64,
    pub execution_time_ms: u64,
    
    // Performance tracking
    pub expected_cost_bps: f64,
    pub actual_cost_bps: f64,
    pub vs_arrival_price_bps: f64,
    pub vs_vwap_bps: f64,
    pub benchmark_performance: HashMap<String, f64>,
    
    // Configuration
    pub max_participation_rate: f64,  // Max % of volume
    pub slice_size: f64,             // Size of each slice
    pub min_fill_size: f64,          // Minimum acceptable fill
    pub timeout_ms: u64,             // Route timeout
}

impl SmartOrderRoute {
    pub fn new(
        id: String,
        symbol: String,
        side: OrderSide,
        total_quantity: f64,
        urgency: ExecutionUrgency,
    ) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        
        Self {
            id,
            symbol,
            side,
            total_quantity,
            urgency,
            child_orders: Vec::new(),
            status: RouteStatus::Pending,
            created_at: now,
            updated_at: now,
            completion_time: None,
            total_filled: 0.0,
            volume_weighted_avg_price: 0.0,
            total_fees: 0.0,
            actual_slippage_bps: 0.0,
            execution_time_ms: 0,
            expected_cost_bps: 0.0,
            actual_cost_bps: 0.0,
            vs_arrival_price_bps: 0.0,
            vs_vwap_bps: 0.0,
            benchmark_performance: HashMap::new(),
            max_participation_rate: 0.1, // Default 10%
            slice_size: 0.0,
            min_fill_size: 0.0,
            timeout_ms: 300000, // Default 5 minutes
        }
    }
    
    pub fn add_child_order(&mut self, child_order: ChildOrder) {
        self.child_orders.push(child_order);
        self.update_metrics();
    }
    
    pub fn update_child_order(&mut self, order_id: &str, filled_qty: f64, fill_price: f64, fees: f64, status: RouteStatus) {
        if let Some(child) = self.child_orders.iter_mut().find(|o| o.id == order_id) {
            child.filled_quantity += filled_qty;
            child.fees_paid += fees;
            child.status = status;
            child.updated_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            
            // Update average fill price
            if child.filled_quantity > 0.0 {
                child.avg_fill_price = ((child.avg_fill_price * (child.filled_quantity - filled_qty)) + (fill_price * filled_qty)) / child.filled_quantity;
            }
        }
        
        self.update_metrics();
        self.check_completion();
    }
    
    fn update_metrics(&mut self) {
        // Update total filled quantity
        self.total_filled = self.child_orders.iter().map(|o| o.filled_quantity).sum();
        
        // Update total fees
        self.total_fees = self.child_orders.iter().map(|o| o.fees_paid).sum();
        
        // Calculate VWAP
        let mut total_value = 0.0;
        let mut total_volume = 0.0;
        
        for child in &self.child_orders {
            if child.filled_quantity > 0.0 {
                total_value += child.avg_fill_price * child.filled_quantity;
                total_volume += child.filled_quantity;
            }
        }
        
        if total_volume > 0.0 {
            self.volume_weighted_avg_price = total_value / total_volume;
        }
        
        // Update execution time
        self.execution_time_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 - self.created_at;
        
        self.updated_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    }
    
    fn check_completion(&mut self) {
        let all_complete = self.child_orders.iter().all(|o| o.is_complete());
        let fill_rate = if self.total_quantity > 0.0 { self.total_filled / self.total_quantity } else { 0.0 };
        
        if all_complete || fill_rate >= 0.99 { // 99% filled considered complete
            if self.total_filled > 0.0 {
                self.status = RouteStatus::Filled;
            } else {
                self.status = RouteStatus::Cancelled;
            }
            self.completion_time = Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64);
        } else if self.total_filled > 0.0 {
            self.status = RouteStatus::PartiallyFilled;
        }
    }
    
    pub fn fill_rate(&self) -> f64 {
        if self.total_quantity > 0.0 {
            self.total_filled / self.total_quantity
        } else {
            0.0
        }
    }
    
    pub fn is_complete(&self) -> bool {
        matches!(self.status, RouteStatus::Filled | RouteStatus::Cancelled | RouteStatus::Rejected | RouteStatus::Expired | RouteStatus::Failed)
    }
    
    pub fn remaining_quantity(&self) -> f64 {
        (self.total_quantity - self.total_filled).max(0.0)
    }
}

/// Routing strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    pub max_exchanges: usize,
    pub min_exchange_allocation: f64,
    pub max_single_exchange_allocation: f64,
    pub rebalance_threshold: f64,
    pub market_impact_threshold_bps: f64,
    pub latency_weight: f64,
    pub cost_weight: f64,
    pub liquidity_weight: f64,
    pub reliability_weight: f64,
    pub enable_dark_pools: bool,
    pub enable_iceberg_orders: bool,
    pub max_order_size_per_exchange: f64,
    pub participation_rate_limit: f64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            max_exchanges: 5,
            min_exchange_allocation: 0.05, // 5%
            max_single_exchange_allocation: 0.6, // 60%
            rebalance_threshold: 0.1, // 10%
            market_impact_threshold_bps: 50.0,
            latency_weight: 0.2,
            cost_weight: 0.4,
            liquidity_weight: 0.3,
            reliability_weight: 0.1,
            enable_dark_pools: true,
            enable_iceberg_orders: true,
            max_order_size_per_exchange: 1000000.0, // $1M
            participation_rate_limit: 0.15, // 15%
        }
    }
}

/// Algorithm for different routing strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingAlgorithm {
    SmartRouting,    // Multi-factor optimization
    TWAP,           // Time-Weighted Average Price
    VWAP,           // Volume-Weighted Average Price
    Implementation, // Implementation Shortfall
    Iceberg,        // Large order slicing
    Stealth,        // Minimal market impact
    Aggressive,     // Fast execution, higher cost
    Conservative,   // Low cost, slower execution
}

/// Smart Order Router - the main routing engine
pub struct SmartOrderRouter {
    metrics_aggregator: Arc<ExchangeMetricsAggregator>,
    active_routes: Arc<RwLock<HashMap<String, SmartOrderRoute>>>,
    routing_config: Arc<RwLock<RoutingConfig>>,
    
    // Performance tracking
    completed_routes: Arc<RwLock<VecDeque<SmartOrderRoute>>>,
    performance_stats: Arc<RwLock<RoutingStats>>,
    
    // Real-time monitoring
    route_monitor: Arc<Mutex<RouteMonitor>>,
    
    // Configuration
    max_route_history: usize,
    monitoring_interval_ms: u64,
    
    // Worker thread handles
    worker_handles: Mutex<Vec<thread::JoinHandle<()>>>,
    is_running: Arc<Mutex<bool>>,
}

/// Performance statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoutingStats {
    pub total_routes: u64,
    pub successful_routes: u64,
    pub failed_routes: u64,
    pub avg_fill_rate: f64,
    pub avg_execution_time_ms: f64,
    pub avg_slippage_bps: f64,
    pub avg_cost_savings_bps: f64,
    pub total_volume_routed: f64,
    pub total_fees_saved: f64,
    pub best_vs_worst_exchange_bps: f64,
    pub arbitrage_opportunities_captured: u64,
}

/// Route monitoring system
#[derive(Debug)]
pub struct RouteMonitor {
    monitored_routes: HashMap<String, Instant>,
    timeout_alerts: VecDeque<String>,
    performance_alerts: VecDeque<String>,
}

impl RouteMonitor {
    pub fn new() -> Self {
        Self {
            monitored_routes: HashMap::new(),
            timeout_alerts: VecDeque::new(),
            performance_alerts: VecDeque::new(),
        }
    }
    
    pub fn add_route(&mut self, route_id: String) {
        self.monitored_routes.insert(route_id, Instant::now());
    }
    
    pub fn remove_route(&mut self, route_id: &str) {
        self.monitored_routes.remove(route_id);
    }
    
    pub fn check_timeouts(&mut self, timeout_ms: u64) -> Vec<String> {
        let timeout_duration = Duration::from_millis(timeout_ms);
        let now = Instant::now();
        let mut timed_out = Vec::new();
        
        self.monitored_routes.retain(|route_id, start_time| {
            if now.duration_since(*start_time) > timeout_duration {
                timed_out.push(route_id.clone());
                self.timeout_alerts.push_back(format!("Route {} timed out after {}ms", route_id, timeout_ms));
                false
            } else {
                true
            }
        });
        
        timed_out
    }
    
    pub fn get_alerts(&mut self) -> (Vec<String>, Vec<String>) {
        let timeouts: Vec<String> = self.timeout_alerts.drain(..).collect();
        let performance: Vec<String> = self.performance_alerts.drain(..).collect();
        (timeouts, performance)
    }
}

impl SmartOrderRouter {
    /// Create a new smart order router
    pub fn new(
        metrics_aggregator: Arc<ExchangeMetricsAggregator>,
        max_route_history: usize,
        monitoring_interval_ms: u64,
    ) -> Self {
        Self {
            metrics_aggregator,
            active_routes: Arc::new(RwLock::new(HashMap::new())),
            routing_config: Arc::new(RwLock::new(RoutingConfig::default())),
            completed_routes: Arc::new(RwLock::new(VecDeque::new())),
            performance_stats: Arc::new(RwLock::new(RoutingStats::default())),
            route_monitor: Arc::new(Mutex::new(RouteMonitor::new())),
            max_route_history,
            monitoring_interval_ms,
            worker_handles: Mutex::new(Vec::new()),
            is_running: Arc::new(Mutex::new(false)),
        }
    }
    
    /// Start the smart order router monitoring
    pub fn start(&self) -> Result<(), String> {
        *self.is_running.lock().map_err(|_| "Failed to acquire running lock")? = true;
        
        // Start route monitoring thread
        let monitor_handle = {
            let active_routes = Arc::clone(&self.active_routes);
            let route_monitor = Arc::clone(&self.route_monitor);
            let is_running = Arc::clone(&self.is_running);
            let interval = self.monitoring_interval_ms;
            
            thread::spawn(move || {
                Self::route_monitoring_worker(active_routes, route_monitor, is_running, interval);
            })
        };
        
        self.worker_handles.lock().map_err(|_| "Failed to acquire worker handles lock")?.push(monitor_handle);
        
        println!("Smart Order Router started");
        Ok(())
    }
    
    /// Stop the smart order router
    pub fn stop(&self) -> Result<(), String> {
        *self.is_running.lock().map_err(|_| "Failed to acquire running lock")? = false;
        
        // Wait for worker threads to complete
        let mut handles = self.worker_handles.lock().map_err(|_| "Failed to acquire worker handles lock")?;
        for handle in handles.drain(..) {
            handle.join().map_err(|_| "Failed to join worker thread")?;
        }
        
        println!("Smart Order Router stopped");
        Ok(())
    }
    
    /// Create and execute a smart order route
    pub fn route_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        urgency: ExecutionUrgency,
        algorithm: RoutingAlgorithm,
    ) -> Result<String, String> {
        let route_id = format!("route_{}", uuid::Uuid::new_v4().to_string().replace("-", "")[..12].to_string());
        
        // Get routing metrics
        let routing_metrics = self.metrics_aggregator
            .calculate_routing_metrics(symbol, quantity, match side { OrderSide::Buy => "buy", OrderSide::Sell => "sell" })?;
        
        // Create route
        let mut route = SmartOrderRoute::new(route_id.clone(), symbol.to_string(), side, quantity, urgency);
        
        // Set expected costs
        route.expected_cost_bps = routing_metrics.expected_total_cost_bps;
        
        // Generate child orders based on algorithm
        self.generate_child_orders(&mut route, &routing_metrics, algorithm)?;
        
        // Add to active routes
        {
            let mut active_routes = self.active_routes.write().map_err(|_| "Failed to acquire active routes lock")?;
            active_routes.insert(route_id.clone(), route);
        }
        
        // Add to monitoring
        {
            let mut monitor = self.route_monitor.lock().map_err(|_| "Failed to acquire monitor lock")?;
            monitor.add_route(route_id.clone());
        }
        
        println!("Created route {} for {} {} {} with {} child orders", route_id, quantity, side, symbol, routing_metrics.routing_recommendations.len());
        
        Ok(route_id)
    }
    
    /// Generate child orders based on routing strategy
    fn generate_child_orders(
        &self,
        route: &mut SmartOrderRoute,
        routing_metrics: &SmartRoutingMetrics,
        algorithm: RoutingAlgorithm,
    ) -> Result<(), String> {
        let config = self.routing_config.read().map_err(|_| "Failed to acquire config lock")?;
        
        match algorithm {
            RoutingAlgorithm::SmartRouting => {
                self.generate_smart_routing_orders(route, routing_metrics, &config)?;
            },
            RoutingAlgorithm::TWAP => {
                self.generate_twap_orders(route, routing_metrics, &config)?;
            },
            RoutingAlgorithm::VWAP => {
                self.generate_vwap_orders(route, routing_metrics, &config)?;
            },
            RoutingAlgorithm::Iceberg => {
                self.generate_iceberg_orders(route, routing_metrics, &config)?;
            },
            RoutingAlgorithm::Aggressive => {
                self.generate_aggressive_orders(route, routing_metrics, &config)?;
            },
            RoutingAlgorithm::Conservative => {
                self.generate_conservative_orders(route, routing_metrics, &config)?;
            },
            _ => {
                return Err(format!("Algorithm {:?} not implemented yet", algorithm));
            }
        }
        
        Ok(())
    }
    
    /// Generate orders using smart routing algorithm
    fn generate_smart_routing_orders(
        &self,
        route: &mut SmartOrderRoute,
        routing_metrics: &SmartRoutingMetrics,
        config: &RoutingConfig,
    ) -> Result<(), String> {
        let mut remaining_qty = route.total_quantity;
        let mut order_counter = 0;
        
        // Sort exchanges by allocation percentage
        let mut allocations: Vec<(&String, &f64)> = routing_metrics.routing_recommendations.iter().collect();
        allocations.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
        
        for (exchange, &allocation_pct) in allocations {
            if allocation_pct < config.min_exchange_allocation {
                continue;
            }
            
            let allocation_qty = (route.total_quantity * allocation_pct).min(remaining_qty);
            if allocation_qty < route.min_fill_size {
                continue;
            }
            
            // Create child order
            let child_id = format!("{}_child_{}", route.id, order_counter);
            let order_type = match route.urgency {
                ExecutionUrgency::Critical => OrderType::Market,
                ExecutionUrgency::High => OrderType::Limit,
                _ => OrderType::Limit,
            };
            
            let child_order = ChildOrder::new(
                child_id,
                exchange.clone(),
                route.symbol.clone(),
                route.side,
                order_type,
                allocation_qty,
                None, // Price will be set by execution engine
                TimeInForce::IOC,
            );
            
            route.add_child_order(child_order);
            remaining_qty -= allocation_qty;
            order_counter += 1;
            
            if remaining_qty <= route.min_fill_size {
                break;
            }
        }
        
        Ok(())
    }
    
    /// Generate TWAP (Time-Weighted Average Price) orders
    fn generate_twap_orders(
        &self,
        route: &mut SmartOrderRoute,
        routing_metrics: &SmartRoutingMetrics,
        _config: &RoutingConfig,
    ) -> Result<(), String> {
        // TWAP splits the order over time
        let time_slices = match route.urgency {
            ExecutionUrgency::Critical => 1,
            ExecutionUrgency::High => 2,
            ExecutionUrgency::Medium => 5,
            ExecutionUrgency::Low => 10,
        };
        
        let slice_qty = route.total_quantity / time_slices as f64;
        
        // Use best exchange for TWAP
        let best_exchange = &routing_metrics.routing_recommendations
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(exchange, _)| exchange)
            .ok_or("No suitable exchange found")?;
        
        for i in 0..time_slices {
            let child_id = format!("{}_twap_{}", route.id, i);
            let child_order = ChildOrder::new(
                child_id,
                best_exchange.to_string(),
                route.symbol.clone(),
                route.side,
                OrderType::TWAP,
                slice_qty,
                None,
                TimeInForce::GTC,
            );
            
            route.add_child_order(child_order);
        }
        
        Ok(())
    }
    
    /// Generate VWAP (Volume-Weighted Average Price) orders
    fn generate_vwap_orders(
        &self,
        route: &mut SmartOrderRoute,
        routing_metrics: &SmartRoutingMetrics,
        config: &RoutingConfig,
    ) -> Result<(), String> {
        // VWAP adjusts order size based on historical volume patterns
        let volume_profile = vec![0.1, 0.15, 0.2, 0.25, 0.2, 0.1]; // Simplified U-shaped profile
        let mut remaining_qty = route.total_quantity;
        
        let best_exchanges: Vec<&String> = routing_metrics.routing_recommendations
            .iter()
            .filter(|(_, &pct)| pct >= config.min_exchange_allocation)
            .take(3) // Top 3 exchanges
            .map(|(exchange, _)| exchange)
            .collect();
        
        for (i, &volume_pct) in volume_profile.iter().enumerate() {
            let slice_qty = (route.total_quantity * volume_pct).min(remaining_qty);
            if slice_qty < route.min_fill_size {
                continue;
            }
            
            let exchange = best_exchanges[i % best_exchanges.len()];
            let child_id = format!("{}_vwap_{}", route.id, i);
            let child_order = ChildOrder::new(
                child_id,
                exchange.clone(),
                route.symbol.clone(),
                route.side,
                OrderType::VWAP,
                slice_qty,
                None,
                TimeInForce::GTC,
            );
            
            route.add_child_order(child_order);
            remaining_qty -= slice_qty;
            
            if remaining_qty <= route.min_fill_size {
                break;
            }
        }
        
        Ok(())
    }
    
    /// Generate iceberg orders (large order slicing)
    fn generate_iceberg_orders(
        &self,
        route: &mut SmartOrderRoute,
        routing_metrics: &SmartRoutingMetrics,
        _config: &RoutingConfig,
    ) -> Result<(), String> {
        let iceberg_size = route.slice_size.max(route.total_quantity * 0.1); // Default 10% slice
        let num_slices = (route.total_quantity / iceberg_size).ceil() as usize;
        
        let best_exchange = routing_metrics.routing_recommendations
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(exchange, _)| exchange)
            .ok_or("No suitable exchange found")?;
        
        let mut remaining_qty = route.total_quantity;
        
        for i in 0..num_slices {
            let slice_qty = iceberg_size.min(remaining_qty);
            if slice_qty < route.min_fill_size {
                break;
            }
            
            let child_id = format!("{}_iceberg_{}", route.id, i);
            let child_order = ChildOrder::new(
                child_id,
                best_exchange.clone(),
                route.symbol.clone(),
                route.side,
                OrderType::Iceberg,
                slice_qty,
                None,
                TimeInForce::GTC,
            );
            
            route.add_child_order(child_order);
            remaining_qty -= slice_qty;
        }
        
        Ok(())
    }
    
    /// Generate aggressive orders (fast execution)
    fn generate_aggressive_orders(
        &self,
        route: &mut SmartOrderRoute,
        routing_metrics: &SmartRoutingMetrics,
        _config: &RoutingConfig,
    ) -> Result<(), String> {
        // Use top 2 exchanges for aggressive execution
        let mut allocations: Vec<(&String, &f64)> = routing_metrics.routing_recommendations.iter().collect();
        allocations.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
        
        let mut remaining_qty = route.total_quantity;
        
        for (i, (exchange, &allocation_pct)) in allocations.iter().take(2).enumerate() {
            let allocation_qty = (route.total_quantity * allocation_pct).min(remaining_qty);
            if allocation_qty < route.min_fill_size {
                continue;
            }
            
            let child_id = format!("{}_aggressive_{}", route.id, i);
            let child_order = ChildOrder::new(
                child_id,
                exchange.to_string(),
                route.symbol.clone(),
                route.side,
                OrderType::Market, // Market orders for aggressive execution
                allocation_qty,
                None,
                TimeInForce::IOC,
            );
            
            route.add_child_order(child_order);
            remaining_qty -= allocation_qty;
            
            if remaining_qty <= route.min_fill_size {
                break;
            }
        }
        
        Ok(())
    }
    
    /// Generate conservative orders (cost optimization)
    fn generate_conservative_orders(
        &self,
        route: &mut SmartOrderRoute,
        routing_metrics: &SmartRoutingMetrics,
        config: &RoutingConfig,
    ) -> Result<(), String> {
        // Use all available exchanges with limit orders
        let mut remaining_qty = route.total_quantity;
        let mut order_counter = 0;
        
        for (exchange, &allocation_pct) in &routing_metrics.routing_recommendations {
            if allocation_pct < config.min_exchange_allocation {
                continue;
            }
            
            let allocation_qty = (route.total_quantity * allocation_pct).min(remaining_qty);
            if allocation_qty < route.min_fill_size {
                continue;
            }
            
            let child_id = format!("{}_conservative_{}", route.id, order_counter);
            let child_order = ChildOrder::new(
                child_id,
                exchange.clone(),
                route.symbol.clone(),
                route.side,
                OrderType::Limit, // Always use limit orders for conservative routing
                allocation_qty,
                None,
                TimeInForce::GTC,
            );
            
            route.add_child_order(child_order);
            remaining_qty -= allocation_qty;
            order_counter += 1;
            
            if remaining_qty <= route.min_fill_size {
                break;
            }
        }
        
        Ok(())
    }
    
    /// Update a child order execution
    pub fn update_child_order_execution(
        &self,
        route_id: &str,
        child_order_id: &str,
        filled_qty: f64,
        fill_price: f64,
        fees: f64,
        status: RouteStatus,
    ) -> Result<(), String> {
        let mut active_routes = self.active_routes.write().map_err(|_| "Failed to acquire active routes lock")?;
        
        if let Some(route) = active_routes.get_mut(route_id) {
            route.update_child_order(child_order_id, filled_qty, fill_price, fees, status);
            
            // Check if route is complete
            if route.is_complete() {
                self.complete_route(route_id, route.clone())?;
            }
        } else {
            return Err(format!("Route {} not found", route_id));
        }
        
        Ok(())
    }
    
    /// Complete a route and move to history
    fn complete_route(&self, route_id: &str, route: SmartOrderRoute) -> Result<(), String> {
        // Record performance
        let performance = RoutingPerformance {
            route_id: route_id.to_string(),
            symbol: route.symbol.clone(),
            timestamp: route.completion_time.unwrap_or(route.updated_at),
            total_quantity: route.total_quantity,
            filled_quantity: route.total_filled,
            avg_execution_price: route.volume_weighted_avg_price,
            execution_time_ms: route.execution_time_ms,
            actual_slippage_bps: route.actual_slippage_bps,
            actual_fees_bps: (route.total_fees / (route.volume_weighted_avg_price * route.total_filled)) * 10000.0,
            actual_total_cost_bps: route.actual_cost_bps,
            implementation_shortfall_bps: route.vs_arrival_price_bps,
            vs_vwap_bps: route.vs_vwap_bps,
            vs_twap_bps: 0.0, // Would need TWAP calculation
            vs_arrival_price_bps: route.vs_arrival_price_bps,
            exchange_allocations: route.child_orders.iter()
                .map(|o| (o.exchange.clone(), o.filled_quantity))
                .collect(),
            exchange_performance: HashMap::new(), // Would be calculated based on child performance
            fill_rate: route.fill_rate(),
            time_to_completion_ms: route.execution_time_ms,
            price_improvement_bps: 0.0, // Would be calculated vs benchmark
        };
        
        self.metrics_aggregator.record_routing_performance(performance)?;
        
        // Remove from active routes
        {
            let mut active_routes = self.active_routes.write().map_err(|_| "Failed to acquire active routes lock")?;
            active_routes.remove(route_id);
        }
        
        // Move to completed routes
        {
            let mut completed = self.completed_routes.write().map_err(|_| "Failed to acquire completed routes lock")?;
            completed.push_back(route);
            
            // Maintain history size
            if completed.len() > self.max_route_history {
                completed.pop_front();
            }
        }
        
        // Update performance stats
        self.update_performance_stats()?;
        
        // Remove from monitoring
        {
            let mut monitor = self.route_monitor.lock().map_err(|_| "Failed to acquire monitor lock")?;
            monitor.remove_route(route_id);
        }
        
        Ok(())
    }
    
    /// Update performance statistics
    fn update_performance_stats(&self) -> Result<(), String> {
        let completed = self.completed_routes.read().map_err(|_| "Failed to acquire completed routes lock")?;
        let mut stats = self.performance_stats.write().map_err(|_| "Failed to acquire performance stats lock")?;
        
        if completed.is_empty() {
            return Ok(());
        }
        
        stats.total_routes = completed.len() as u64;
        stats.successful_routes = completed.iter().filter(|r| matches!(r.status, RouteStatus::Filled)).count() as u64;
        stats.failed_routes = stats.total_routes - stats.successful_routes;
        
        let filled_routes: Vec<&SmartOrderRoute> = completed.iter().filter(|r| r.total_filled > 0.0).collect();
        
        if !filled_routes.is_empty() {
            stats.avg_fill_rate = filled_routes.iter().map(|r| r.fill_rate()).sum::<f64>() / filled_routes.len() as f64;
            stats.avg_execution_time_ms = filled_routes.iter().map(|r| r.execution_time_ms as f64).sum::<f64>() / filled_routes.len() as f64;
            stats.avg_slippage_bps = filled_routes.iter().map(|r| r.actual_slippage_bps).sum::<f64>() / filled_routes.len() as f64;
            stats.total_volume_routed = filled_routes.iter().map(|r| r.total_filled).sum::<f64>();
            stats.total_fees_saved = filled_routes.iter().map(|r| {
                // Calculate fees saved vs worst case (simplified)
                let worst_case_fees = r.total_filled * r.volume_weighted_avg_price * 0.001; // 0.1% worst case
                worst_case_fees - r.total_fees
            }).sum::<f64>();
        }
        
        Ok(())
    }
    
    /// Route monitoring worker thread
    fn route_monitoring_worker(
        active_routes: Arc<RwLock<HashMap<String, SmartOrderRoute>>>,
        route_monitor: Arc<Mutex<RouteMonitor>>,
        is_running: Arc<Mutex<bool>>,
        interval_ms: u64,
    ) {
        println!("Route monitoring worker started");
        
        let sleep_duration = Duration::from_millis(interval_ms);
        
        while *is_running.lock().unwrap() {
            thread::sleep(sleep_duration);
            
            // Check for timeouts
            let timed_out_routes = {
                let mut monitor = route_monitor.lock().unwrap();
                monitor.check_timeouts(300000) // 5 minute timeout
            };
            
            // Handle timed out routes
            if !timed_out_routes.is_empty() {
                let mut active = active_routes.write().unwrap();
                for route_id in timed_out_routes {
                    if let Some(mut route) = active.remove(&route_id) {
                        route.status = RouteStatus::Expired;
                        println!("Route {} timed out", route_id);
                    }
                }
            }
            
            // Check route performance and generate alerts
            {
                let mut monitor = route_monitor.lock().unwrap();
                let (timeout_alerts, performance_alerts) = monitor.get_alerts();
                
                for alert in timeout_alerts {
                    println!("TIMEOUT ALERT: {}", alert);
                }
                
                for alert in performance_alerts {
                    println!("PERFORMANCE ALERT: {}", alert);
                }
            }
        }
        
        println!("Route monitoring worker stopped");
    }
    
    /// Get active route
    pub fn get_active_route(&self, route_id: &str) -> Result<SmartOrderRoute, String> {
        let active_routes = self.active_routes.read().map_err(|_| "Failed to acquire active routes lock")?;
        active_routes.get(route_id).cloned().ok_or_else(|| format!("Route {} not found", route_id))
    }
    
    /// Get all active routes
    pub fn get_active_routes(&self) -> Result<Vec<SmartOrderRoute>, String> {
        let active_routes = self.active_routes.read().map_err(|_| "Failed to acquire active routes lock")?;
        Ok(active_routes.values().cloned().collect())
    }
    
    /// Get performance statistics
    pub fn get_performance_stats(&self) -> Result<RoutingStats, String> {
        let stats = self.performance_stats.read().map_err(|_| "Failed to acquire performance stats lock")?;
        Ok(stats.clone())
    }
    
    /// Update routing configuration
    pub fn update_config(&self, new_config: RoutingConfig) -> Result<(), String> {
        let mut config = self.routing_config.write().map_err(|_| "Failed to acquire config lock")?;
        *config = new_config;
        Ok(())
    }
    
    /// Cancel a route
    pub fn cancel_route(&self, route_id: &str) -> Result<(), String> {
        let mut active_routes = self.active_routes.write().map_err(|_| "Failed to acquire active routes lock")?;
        
        if let Some(route) = active_routes.get_mut(route_id) {
            route.status = RouteStatus::Cancelled;
            // In practice, would also cancel all child orders on exchanges
            
            // Move to completed
            let completed_route = route.clone();
            drop(active_routes); // Release lock before calling complete_route
            self.complete_route(route_id, completed_route)?;
        } else {
            return Err(format!("Route {} not found", route_id));
        }
        
        Ok(())
    }
    
    /// Get routing recommendations for a potential order
    pub fn get_routing_recommendation(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        _urgency: ExecutionUrgency,
    ) -> Result<SmartRoutingMetrics, String> {
        let side_str = match side { OrderSide::Buy => "buy", OrderSide::Sell => "sell" };
        self.metrics_aggregator.calculate_routing_metrics(symbol, quantity, side_str)
    }
}

// Add a simple UUID implementation since we're using it
mod uuid {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;
    
    pub struct Uuid;
    
    impl Uuid {
        pub fn new_v4() -> UuidStruct {
            let mut hasher = DefaultHasher::new();
            SystemTime::now().hash(&mut hasher);
            std::thread::current().id().hash(&mut hasher);
            
            UuidStruct {
                value: hasher.finish(),
            }
        }
    }
    
    pub struct UuidStruct {
        value: u64,
    }
    
    impl UuidStruct {
        pub fn to_string(&self) -> String {
            format!("{:016x}", self.value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn create_test_metrics_aggregator() -> Arc<ExchangeMetricsAggregator> {
        Arc::new(ExchangeMetricsAggregator::new(1000, 10000))
    }
    
    fn setup_test_metrics(aggregator: &ExchangeMetricsAggregator) {
        let binance_metrics = ExchangeMetrics {
            exchange_name: "Binance".to_string(),
            symbol: "BTC/USD".to_string(),
            best_bid: 50000.0,
            best_ask: 50001.0,
            total_bid_liquidity: 100.0,
            total_ask_liquidity: 150.0,
            avg_response_time_ms: 50.0,
            avg_slippage_bps: 10.0,
            connectivity_score: 95.0,
            effective_fee_bps: 5.0,
            ..Default::default()
        };
        
        let coinbase_metrics = ExchangeMetrics {
            exchange_name: "Coinbase".to_string(),
            symbol: "BTC/USD".to_string(),
            best_bid: 49999.0,
            best_ask: 50002.0,
            total_bid_liquidity: 80.0,
            total_ask_liquidity: 120.0,
            avg_response_time_ms: 75.0,
            avg_slippage_bps: 15.0,
            connectivity_score: 90.0,
            effective_fee_bps: 7.0,
            ..Default::default()
        };
        
        aggregator.update_exchange_metrics("Binance", "BTC/USD", binance_metrics).unwrap();
        aggregator.update_exchange_metrics("Coinbase", "BTC/USD", coinbase_metrics).unwrap();
    }

    #[test]
    fn test_router_creation() {
        let aggregator = create_test_metrics_aggregator();
        let router = SmartOrderRouter::new(aggregator, 1000, 1000);
        
        assert_eq!(router.max_route_history, 1000);
        assert_eq!(router.monitoring_interval_ms, 1000);
    }

    #[test]
    fn test_smart_order_route_creation() {
        let route = SmartOrderRoute::new(
            "test_route".to_string(),
            "BTC/USD".to_string(),
            OrderSide::Buy,
            10.0,
            ExecutionUrgency::Medium,
        );
        
        assert_eq!(route.symbol, "BTC/USD");
        assert_eq!(route.side, OrderSide::Buy);
        assert_eq!(route.total_quantity, 10.0);
        assert_eq!(route.urgency, ExecutionUrgency::Medium);
        assert_eq!(route.status, RouteStatus::Pending);
    }

    #[test]
    fn test_child_order_creation() {
        let child = ChildOrder::new(
            "child_1".to_string(),
            "Binance".to_string(),
            "BTC/USD".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            5.0,
            Some(50000.0),
            TimeInForce::IOC,
        );
        
        assert_eq!(child.exchange, "Binance");
        assert_eq!(child.quantity, 5.0);
        assert_eq!(child.price, Some(50000.0));
        assert_eq!(child.status, RouteStatus::Pending);
        assert_eq!(child.remaining_quantity(), 5.0);
        assert_eq!(child.fill_rate(), 0.0);
    }

    #[test]
    fn test_route_order_creation() {
        let aggregator = create_test_metrics_aggregator();
        setup_test_metrics(&aggregator);
        
        let router = SmartOrderRouter::new(aggregator, 1000, 1000);
        
        let route_id = router.route_order(
            "BTC/USD",
            OrderSide::Buy,
            10.0,
            ExecutionUrgency::Medium,
            RoutingAlgorithm::SmartRouting,
        );
        
        assert!(route_id.is_ok());
        let route_id = route_id.unwrap();
        
        let route = router.get_active_route(&route_id);
        assert!(route.is_ok());
        
        let route = route.unwrap();
        assert_eq!(route.symbol, "BTC/USD");
        assert_eq!(route.side, OrderSide::Buy);
        assert!(route.child_orders.len() > 0);
    }

    #[test]
    fn test_child_order_update() {
        let mut route = SmartOrderRoute::new(
            "test_route".to_string(),
            "BTC/USD".to_string(),
            OrderSide::Buy,
            10.0,
            ExecutionUrgency::Medium,
        );
        
        let child = ChildOrder::new(
            "child_1".to_string(),
            "Binance".to_string(),
            "BTC/USD".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            5.0,
            Some(50000.0),
            TimeInForce::IOC,
        );
        
        route.add_child_order(child);
        
        // Update with partial fill
        route.update_child_order("child_1", 3.0, 50000.0, 1.5, RouteStatus::PartiallyFilled);
        
        assert_eq!(route.total_filled, 3.0);
        assert_eq!(route.status, RouteStatus::PartiallyFilled);
        assert_eq!(route.child_orders[0].filled_quantity, 3.0);
        assert_eq!(route.child_orders[0].remaining_quantity(), 2.0);
    }

    #[test]
    fn test_routing_config() {
        let config = RoutingConfig::default();
        
        assert_eq!(config.max_exchanges, 5);
        assert_eq!(config.min_exchange_allocation, 0.05);
        assert_eq!(config.cost_weight, 0.4);
        assert_eq!(config.liquidity_weight, 0.3);
    }

    #[test]
    fn test_performance_stats_update() {
        let aggregator = create_test_metrics_aggregator();
        let router = SmartOrderRouter::new(aggregator, 1000, 1000);
        
        let stats = router.get_performance_stats();
        assert!(stats.is_ok());
        
        let stats = stats.unwrap();
        assert_eq!(stats.total_routes, 0);
        assert_eq!(stats.successful_routes, 0);
    }

    #[test]
    fn test_different_routing_algorithms() {
        let aggregator = create_test_metrics_aggregator();
        setup_test_metrics(&aggregator);
        
        let router = SmartOrderRouter::new(aggregator, 1000, 1000);
        
        // Test TWAP algorithm
        let route_id = router.route_order(
            "BTC/USD",
            OrderSide::Buy,
            100.0,
            ExecutionUrgency::Low,
            RoutingAlgorithm::TWAP,
        );
        assert!(route_id.is_ok());
        
        // Test Aggressive algorithm
        let route_id = router.route_order(
            "BTC/USD",
            OrderSide::Sell,
            50.0,
            ExecutionUrgency::High,
            RoutingAlgorithm::Aggressive,
        );
        assert!(route_id.is_ok());
        
        // Test Conservative algorithm
        let route_id = router.route_order(
            "BTC/USD",
            OrderSide::Buy,
            25.0,
            ExecutionUrgency::Low,
            RoutingAlgorithm::Conservative,
        );
        assert!(route_id.is_ok());
    }

    #[test]
    fn test_route_cancellation() {
        let aggregator = create_test_metrics_aggregator();
        setup_test_metrics(&aggregator);
        
        let router = SmartOrderRouter::new(aggregator, 1000, 1000);
        
        let route_id = router.route_order(
            "BTC/USD",
            OrderSide::Buy,
            10.0,
            ExecutionUrgency::Medium,
            RoutingAlgorithm::SmartRouting,
        ).unwrap();
        
        // Cancel the route
        let result = router.cancel_route(&route_id);
        assert!(result.is_ok());
        
        // Route should no longer be active
        let route = router.get_active_route(&route_id);
        assert!(route.is_err());
    }
}
