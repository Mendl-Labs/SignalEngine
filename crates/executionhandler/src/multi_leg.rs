//! Multi-Leg Order Support for Complex Trading Strategies
//!
//! This module provides institutional-grade multi-leg order types:
//! - **OCO (One-Cancels-Other)**: Two orders where filling one cancels the other
//! - **Bracket Orders**: Entry + Stop-Loss + Take-Profit as atomic unit
//! - **Linked Orders**: Pairs trading with synchronized execution
//! - **Conditional Orders**: Orders triggered by market conditions
//!
//! # Architecture
//!
//! Multi-leg orders are managed through a central `MultiLegOrderManager` that:
//! - Tracks order group state across all legs
//! - Handles partial fills with proportional adjustments
//! - Ensures atomic cancellation of related orders
//! - Provides audit trail for regulatory compliance
//!
//! # Example
//!
//! ```rust,ignore
//! use executionhandler::multi_leg::{MultiLegOrderManager, BracketOrder, BracketParams};
//!
//! let manager = MultiLegOrderManager::new();
//!
//! // Create bracket order: Buy entry, stop-loss, take-profit
//! let bracket = BracketOrder::new(BracketParams {
//!     symbol: "BTC-USD".to_string(),
//!     entry_side: OrderSide::Buy,
//!     entry_price: 50000.0,
//!     entry_quantity: 1.0,
//!     stop_loss_price: 48000.0,
//!     take_profit_price: 55000.0,
//!     ..Default::default()
//! });
//!
//! let group_id = manager.submit_bracket(bracket).await?;
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use parking_lot::RwLock;

use crate::core::types::{OrderSide, OrderType, TimeInForce, ExecutionStatus, ExecutionFill};

/// Unique identifier for a multi-leg order group
pub type GroupId = String;

/// Unique identifier for an individual leg
pub type LegId = String;

/// Multi-leg order group state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum GroupState {
    /// Group created but not yet submitted
    Pending = 0,
    /// All legs submitted to exchange(s)
    Active = 1,
    /// Entry leg filled, contingent legs active
    EntryFilled = 2,
    /// Some legs partially filled
    PartiallyFilled = 3,
    /// All legs completed (filled or cancelled)
    Completed = 4,
    /// Group cancelled by user or system
    Cancelled = 5,
    /// Group failed due to error
    Failed = 6,
}

impl From<u8> for GroupState {
    fn from(v: u8) -> Self {
        match v {
            0 => GroupState::Pending,
            1 => GroupState::Active,
            2 => GroupState::EntryFilled,
            3 => GroupState::PartiallyFilled,
            4 => GroupState::Completed,
            5 => GroupState::Cancelled,
            _ => GroupState::Failed,
        }
    }
}

/// Type of multi-leg order
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultiLegType {
    /// One-Cancels-Other: Two orders, filling one cancels the other
    OCO,
    /// Bracket: Entry + Stop-Loss + Take-Profit
    Bracket,
    /// Linked pair: Two orders that should fill together (pairs trading)
    LinkedPair,
    /// Conditional: Order triggered by another order's fill
    Conditional,
    /// Spread: Multi-leg options/futures spread
    Spread,
}

/// Individual leg within a multi-leg order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderLeg {
    /// Unique leg identifier
    pub leg_id: LegId,
    /// Parent group identifier
    pub group_id: GroupId,
    /// Leg sequence number (0 = primary/entry)
    pub sequence: u32,
    /// Trading symbol
    pub symbol: String,
    /// Order side
    pub side: OrderSide,
    /// Order type
    pub order_type: OrderType,
    /// Order quantity
    pub quantity: f64,
    /// Limit price (if applicable)
    pub price: Option<f64>,
    /// Stop/trigger price (if applicable)
    pub trigger_price: Option<f64>,
    /// Time in force
    pub time_in_force: TimeInForce,
    /// Exchange to route to
    pub exchange: String,
    /// Leg role in the group
    pub role: LegRole,
    /// Current leg status
    pub status: LegStatus,
    /// Exchange order ID once submitted
    pub exchange_order_id: Option<String>,
    /// Filled quantity
    pub filled_quantity: f64,
    /// Average fill price
    pub avg_fill_price: f64,
    /// Associated fills
    pub fills: Vec<ExecutionFill>,
    /// Created timestamp (nanoseconds)
    pub created_at: u128,
    /// Last updated timestamp (nanoseconds)
    pub updated_at: u128,
    /// Exchange timestamp for MiFID II (nanoseconds)
    pub exchange_timestamp_ns: Option<u128>,
}

/// Role of a leg within the multi-leg order
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LegRole {
    /// Primary entry order
    Entry,
    /// Stop-loss protection
    StopLoss,
    /// Take-profit target
    TakeProfit,
    /// OCO leg A
    OcoLegA,
    /// OCO leg B
    OcoLegB,
    /// Long leg of a pair
    PairLong,
    /// Short leg of a pair
    PairShort,
    /// Conditional trigger order
    Trigger,
    /// Conditional target order
    Target,
}

/// Status of an individual leg
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LegStatus {
    /// Leg created but not submitted
    Pending,
    /// Waiting for trigger condition
    Waiting,
    /// Submitted to exchange
    Submitted,
    /// Partially filled
    PartiallyFilled,
    /// Completely filled
    Filled,
    /// Cancelled (by user or due to OCO)
    Cancelled,
    /// Rejected by exchange
    Rejected,
}

/// OCO (One-Cancels-Other) order parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OcoParams {
    /// Trading symbol
    pub symbol: String,
    /// Exchange to use
    pub exchange: String,
    
    // Leg A parameters (typically limit order)
    /// Leg A side
    pub leg_a_side: OrderSide,
    /// Leg A order type
    pub leg_a_type: OrderType,
    /// Leg A quantity
    pub leg_a_quantity: f64,
    /// Leg A price
    pub leg_a_price: f64,
    
    // Leg B parameters (typically stop order)
    /// Leg B side
    pub leg_b_side: OrderSide,
    /// Leg B order type
    pub leg_b_type: OrderType,
    /// Leg B quantity
    pub leg_b_quantity: f64,
    /// Leg B price
    pub leg_b_price: f64,
    /// Leg B trigger price (for stop orders)
    pub leg_b_trigger: Option<f64>,
    
    /// Time in force for both legs
    pub time_in_force: TimeInForce,
    /// Client-provided group ID (optional)
    pub client_group_id: Option<String>,
}

/// Bracket order parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BracketParams {
    /// Trading symbol
    pub symbol: String,
    /// Exchange to use
    pub exchange: String,
    
    // Entry parameters
    /// Entry order side (Buy to go long, Sell to go short)
    pub entry_side: OrderSide,
    /// Entry order type (Market or Limit)
    pub entry_type: OrderType,
    /// Entry quantity
    pub entry_quantity: f64,
    /// Entry limit price (for limit orders)
    pub entry_price: Option<f64>,
    
    // Stop-loss parameters
    /// Stop-loss trigger price
    pub stop_loss_price: f64,
    /// Stop-loss limit price (for stop-limit orders)
    pub stop_loss_limit: Option<f64>,
    
    // Take-profit parameters
    /// Take-profit price
    pub take_profit_price: f64,
    /// Take-profit limit price (optional, defaults to take_profit_price)
    pub take_profit_limit: Option<f64>,
    
    /// Time in force for contingent orders
    pub time_in_force: TimeInForce,
    /// Whether to submit stop-loss and take-profit immediately or wait for entry fill
    pub submit_contingent_immediately: bool,
    /// Client-provided group ID (optional)
    pub client_group_id: Option<String>,
}

impl Default for BracketParams {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            exchange: String::new(),
            entry_side: OrderSide::Buy,
            entry_type: OrderType::Limit,
            entry_quantity: 0.0,
            entry_price: None,
            stop_loss_price: 0.0,
            stop_loss_limit: None,
            take_profit_price: 0.0,
            take_profit_limit: None,
            time_in_force: TimeInForce::GoodTillCancelled,
            submit_contingent_immediately: false,
            client_group_id: None,
        }
    }
}

/// Linked pair order parameters (for pairs trading)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkedPairParams {
    // Long leg
    /// Long leg symbol
    pub long_symbol: String,
    /// Long leg exchange
    pub long_exchange: String,
    /// Long leg quantity
    pub long_quantity: f64,
    /// Long leg limit price
    pub long_price: Option<f64>,
    
    // Short leg
    /// Short leg symbol
    pub short_symbol: String,
    /// Short leg exchange
    pub short_exchange: String,
    /// Short leg quantity
    pub short_quantity: f64,
    /// Short leg limit price
    pub short_price: Option<f64>,
    
    /// Order type for both legs
    pub order_type: OrderType,
    /// Time in force
    pub time_in_force: TimeInForce,
    /// Execution strategy
    pub execution_strategy: PairExecutionStrategy,
    /// Maximum allowable spread deviation (bps)
    pub max_spread_deviation_bps: Option<f64>,
    /// Client-provided group ID
    pub client_group_id: Option<String>,
}

/// Execution strategy for linked pairs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PairExecutionStrategy {
    /// Execute both legs simultaneously
    Simultaneous,
    /// Execute long leg first, then short
    LongFirst,
    /// Execute short leg first, then long
    ShortFirst,
    /// Execute based on liquidity/fill probability
    Adaptive,
}

/// Multi-leg order group containing all legs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiLegOrderGroup {
    /// Unique group identifier
    pub group_id: GroupId,
    /// Type of multi-leg order
    pub order_type: MultiLegType,
    /// Current group state
    pub state: GroupState,
    /// All legs in the group
    pub legs: Vec<OrderLeg>,
    /// Creation timestamp (nanoseconds)
    pub created_at: u128,
    /// Last update timestamp (nanoseconds)
    pub updated_at: u128,
    /// Completion timestamp (nanoseconds)
    pub completed_at: Option<u128>,
    /// Error message if failed
    pub error: Option<String>,
    /// Client-provided reference
    pub client_reference: Option<String>,
    /// Metadata for audit/compliance
    pub metadata: HashMap<String, String>,
}

impl MultiLegOrderGroup {
    /// Get the entry leg (if exists)
    pub fn entry_leg(&self) -> Option<&OrderLeg> {
        self.legs.iter().find(|l| l.role == LegRole::Entry)
    }
    
    /// Get the stop-loss leg (if exists)
    pub fn stop_loss_leg(&self) -> Option<&OrderLeg> {
        self.legs.iter().find(|l| l.role == LegRole::StopLoss)
    }
    
    /// Get the take-profit leg (if exists)
    pub fn take_profit_leg(&self) -> Option<&OrderLeg> {
        self.legs.iter().find(|l| l.role == LegRole::TakeProfit)
    }
    
    /// Check if group is terminal (completed, cancelled, or failed)
    pub fn is_terminal(&self) -> bool {
        matches!(self.state, GroupState::Completed | GroupState::Cancelled | GroupState::Failed)
    }
    
    /// Get total filled value across all legs
    pub fn total_filled_value(&self) -> f64 {
        self.legs.iter()
            .map(|l| l.filled_quantity * l.avg_fill_price)
            .sum()
    }
    
    /// Get total fees across all legs
    pub fn total_fees(&self) -> f64 {
        self.legs.iter()
            .flat_map(|l| l.fills.iter())
            .map(|f| f.fee)
            .sum()
    }
}

/// Event emitted when multi-leg order state changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiLegEvent {
    /// Group ID
    pub group_id: GroupId,
    /// Event type
    pub event_type: MultiLegEventType,
    /// Affected leg ID (if applicable)
    pub leg_id: Option<LegId>,
    /// Previous state
    pub previous_state: GroupState,
    /// New state
    pub new_state: GroupState,
    /// Event timestamp (nanoseconds)
    pub timestamp: u128,
    /// Additional details
    pub details: Option<String>,
}

/// Types of multi-leg events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultiLegEventType {
    /// Group created
    Created,
    /// Group submitted to exchange(s)
    Submitted,
    /// Leg submitted
    LegSubmitted,
    /// Leg partially filled
    LegPartialFill,
    /// Leg completely filled
    LegFilled,
    /// Leg cancelled (OCO trigger or user cancel)
    LegCancelled,
    /// Leg rejected
    LegRejected,
    /// Group completed
    GroupCompleted,
    /// Group cancelled
    GroupCancelled,
    /// Group failed
    GroupFailed,
    /// Contingent orders activated
    ContingentActivated,
}

/// Configuration for the multi-leg order manager
#[derive(Debug, Clone)]
pub struct MultiLegConfig {
    /// Maximum number of active groups per user
    pub max_active_groups: usize,
    /// Maximum legs per group
    pub max_legs_per_group: usize,
    /// Timeout for group completion (ms)
    pub group_timeout_ms: u64,
    /// Whether to auto-cancel contingent legs on entry timeout
    pub auto_cancel_on_timeout: bool,
    /// Maximum retries for leg submission
    pub max_leg_retries: u32,
}

impl Default for MultiLegConfig {
    fn default() -> Self {
        Self {
            max_active_groups: 1000,
            max_legs_per_group: 10,
            group_timeout_ms: 86400000, // 24 hours
            auto_cancel_on_timeout: true,
            max_leg_retries: 3,
        }
    }
}

/// Multi-leg order manager
/// 
/// Thread-safe manager for creating, tracking, and managing multi-leg orders.
pub struct MultiLegOrderManager {
    /// Active order groups
    groups: DashMap<GroupId, MultiLegOrderGroup>,
    /// Leg to group mapping for fast lookups
    leg_to_group: DashMap<LegId, GroupId>,
    /// Exchange order ID to leg mapping
    exchange_order_to_leg: DashMap<String, LegId>,
    /// Event listeners
    event_handlers: RwLock<Vec<Box<dyn Fn(MultiLegEvent) + Send + Sync>>>,
    /// Configuration
    config: MultiLegConfig,
    /// Group ID counter
    group_counter: AtomicU64,
    /// Statistics
    stats: MultiLegStats,
}

/// Statistics for multi-leg order management
#[derive(Debug, Default)]
pub struct MultiLegStats {
    /// Total groups created
    pub groups_created: AtomicU64,
    /// Total groups completed
    pub groups_completed: AtomicU64,
    /// Total groups cancelled
    pub groups_cancelled: AtomicU64,
    /// Total groups failed
    pub groups_failed: AtomicU64,
    /// OCO triggers executed
    pub oco_triggers: AtomicU64,
    /// Bracket entries filled
    pub bracket_entries_filled: AtomicU64,
}

impl MultiLegOrderManager {
    /// Create a new multi-leg order manager
    pub fn new() -> Self {
        Self::with_config(MultiLegConfig::default())
    }
    
    /// Create with custom configuration
    pub fn with_config(config: MultiLegConfig) -> Self {
        Self {
            groups: DashMap::new(),
            leg_to_group: DashMap::new(),
            exchange_order_to_leg: DashMap::new(),
            event_handlers: RwLock::new(Vec::new()),
            config,
            group_counter: AtomicU64::new(0),
            stats: MultiLegStats::default(),
        }
    }
    
    /// Generate a unique group ID
    fn generate_group_id(&self) -> GroupId {
        let counter = self.group_counter.fetch_add(1, Ordering::Relaxed);
        let timestamp = Self::now_ns();
        format!("MLG-{:016x}-{:08x}", timestamp, counter)
    }
    
    /// Generate a unique leg ID
    fn generate_leg_id(group_id: &str, sequence: u32) -> LegId {
        format!("{}-L{:02}", group_id, sequence)
    }
    
    fn now_ns() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    }
    
    /// Create an OCO (One-Cancels-Other) order
    pub fn create_oco(&self, params: OcoParams) -> Result<MultiLegOrderGroup, MultiLegError> {
        // Enforce max_active_groups limit
        if self.groups.len() >= self.config.max_active_groups {
            return Err(MultiLegError::MaxGroupsExceeded);
        }
        
        let group_id = params.client_group_id
            .unwrap_or_else(|| self.generate_group_id());
        let now = Self::now_ns();
        
        let leg_a = OrderLeg {
            leg_id: Self::generate_leg_id(&group_id, 0),
            group_id: group_id.clone(),
            sequence: 0,
            symbol: params.symbol.clone(),
            side: params.leg_a_side,
            order_type: params.leg_a_type,
            quantity: params.leg_a_quantity,
            price: Some(params.leg_a_price),
            trigger_price: None,
            time_in_force: params.time_in_force.clone(),
            exchange: params.exchange.clone(),
            role: LegRole::OcoLegA,
            status: LegStatus::Pending,
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            fills: Vec::new(),
            created_at: now,
            updated_at: now,
            exchange_timestamp_ns: None,
        };
        
        let leg_b = OrderLeg {
            leg_id: Self::generate_leg_id(&group_id, 1),
            group_id: group_id.clone(),
            sequence: 1,
            symbol: params.symbol,
            side: params.leg_b_side,
            order_type: params.leg_b_type,
            quantity: params.leg_b_quantity,
            price: Some(params.leg_b_price),
            trigger_price: params.leg_b_trigger,
            time_in_force: params.time_in_force,
            exchange: params.exchange,
            role: LegRole::OcoLegB,
            status: LegStatus::Pending,
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            fills: Vec::new(),
            created_at: now,
            updated_at: now,
            exchange_timestamp_ns: None,
        };
        
        let group = MultiLegOrderGroup {
            group_id: group_id.clone(),
            order_type: MultiLegType::OCO,
            state: GroupState::Pending,
            legs: vec![leg_a.clone(), leg_b.clone()],
            created_at: now,
            updated_at: now,
            completed_at: None,
            error: None,
            client_reference: None,
            metadata: HashMap::new(),
        };
        
        // Store mappings
        self.leg_to_group.insert(leg_a.leg_id.clone(), group_id.clone());
        self.leg_to_group.insert(leg_b.leg_id, group_id.clone());
        self.groups.insert(group_id, group.clone());
        
        self.stats.groups_created.fetch_add(1, Ordering::Relaxed);
        
        Ok(group)
    }
    
    /// Create a bracket order (entry + stop-loss + take-profit)
    pub fn create_bracket(&self, params: BracketParams) -> Result<MultiLegOrderGroup, MultiLegError> {
        // Validate parameters
        if params.entry_quantity <= 0.0 {
            return Err(MultiLegError::InvalidParameter("Entry quantity must be positive".into()));
        }
        
        let is_long = params.entry_side == OrderSide::Buy;
        
        // Validate stop-loss and take-profit relative to entry
        if let Some(entry_price) = params.entry_price {
            if is_long {
                if params.stop_loss_price >= entry_price {
                    return Err(MultiLegError::InvalidParameter(
                        "Stop-loss must be below entry for long positions".into()
                    ));
                }
                if params.take_profit_price <= entry_price {
                    return Err(MultiLegError::InvalidParameter(
                        "Take-profit must be above entry for long positions".into()
                    ));
                }
            } else {
                if params.stop_loss_price <= entry_price {
                    return Err(MultiLegError::InvalidParameter(
                        "Stop-loss must be above entry for short positions".into()
                    ));
                }
                if params.take_profit_price >= entry_price {
                    return Err(MultiLegError::InvalidParameter(
                        "Take-profit must be below entry for short positions".into()
                    ));
                }
            }
        }
        
        let group_id = params.client_group_id
            .unwrap_or_else(|| self.generate_group_id());
        let now = Self::now_ns();
        
        // Entry leg
        let entry_leg = OrderLeg {
            leg_id: Self::generate_leg_id(&group_id, 0),
            group_id: group_id.clone(),
            sequence: 0,
            symbol: params.symbol.clone(),
            side: params.entry_side.clone(),
            order_type: params.entry_type,
            quantity: params.entry_quantity,
            price: params.entry_price,
            trigger_price: None,
            time_in_force: params.time_in_force.clone(),
            exchange: params.exchange.clone(),
            role: LegRole::Entry,
            status: LegStatus::Pending,
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            fills: Vec::new(),
            created_at: now,
            updated_at: now,
            exchange_timestamp_ns: None,
        };
        
        // Stop-loss leg (opposite side of entry)
        let sl_side = if is_long { OrderSide::Sell } else { OrderSide::Buy };
        let stop_loss_leg = OrderLeg {
            leg_id: Self::generate_leg_id(&group_id, 1),
            group_id: group_id.clone(),
            sequence: 1,
            symbol: params.symbol.clone(),
            side: sl_side.clone(),
            order_type: if params.stop_loss_limit.is_some() { OrderType::StopLimit } else { OrderType::Stop },
            quantity: params.entry_quantity,
            price: params.stop_loss_limit,
            trigger_price: Some(params.stop_loss_price),
            time_in_force: params.time_in_force.clone(),
            exchange: params.exchange.clone(),
            role: LegRole::StopLoss,
            status: if params.submit_contingent_immediately { LegStatus::Pending } else { LegStatus::Waiting },
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            fills: Vec::new(),
            created_at: now,
            updated_at: now,
            exchange_timestamp_ns: None,
        };
        
        // Take-profit leg (opposite side of entry)
        let take_profit_leg = OrderLeg {
            leg_id: Self::generate_leg_id(&group_id, 2),
            group_id: group_id.clone(),
            sequence: 2,
            symbol: params.symbol,
            side: sl_side.clone(),
            order_type: OrderType::Limit,
            quantity: params.entry_quantity,
            price: Some(params.take_profit_limit.unwrap_or(params.take_profit_price)),
            trigger_price: None,
            time_in_force: params.time_in_force,
            exchange: params.exchange,
            role: LegRole::TakeProfit,
            status: if params.submit_contingent_immediately { LegStatus::Pending } else { LegStatus::Waiting },
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            fills: Vec::new(),
            created_at: now,
            updated_at: now,
            exchange_timestamp_ns: None,
        };
        
        let group = MultiLegOrderGroup {
            group_id: group_id.clone(),
            order_type: MultiLegType::Bracket,
            state: GroupState::Pending,
            legs: vec![entry_leg.clone(), stop_loss_leg.clone(), take_profit_leg.clone()],
            created_at: now,
            updated_at: now,
            completed_at: None,
            error: None,
            client_reference: None,
            metadata: HashMap::new(),
        };
        
        // Store mappings
        self.leg_to_group.insert(entry_leg.leg_id, group_id.clone());
        self.leg_to_group.insert(stop_loss_leg.leg_id, group_id.clone());
        self.leg_to_group.insert(take_profit_leg.leg_id, group_id.clone());
        self.groups.insert(group_id, group.clone());
        
        self.stats.groups_created.fetch_add(1, Ordering::Relaxed);
        
        Ok(group)
    }
    
    /// Create a linked pair order (for pairs trading)
    pub fn create_linked_pair(&self, params: LinkedPairParams) -> Result<MultiLegOrderGroup, MultiLegError> {
        let group_id = params.client_group_id
            .unwrap_or_else(|| self.generate_group_id());
        let now = Self::now_ns();
        
        let long_leg = OrderLeg {
            leg_id: Self::generate_leg_id(&group_id, 0),
            group_id: group_id.clone(),
            sequence: 0,
            symbol: params.long_symbol,
            side: OrderSide::Buy,
            order_type: params.order_type.clone(),
            quantity: params.long_quantity,
            price: params.long_price,
            trigger_price: None,
            time_in_force: params.time_in_force.clone(),
            exchange: params.long_exchange,
            role: LegRole::PairLong,
            status: LegStatus::Pending,
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            fills: Vec::new(),
            created_at: now,
            updated_at: now,
            exchange_timestamp_ns: None,
        };
        
        let short_leg = OrderLeg {
            leg_id: Self::generate_leg_id(&group_id, 1),
            group_id: group_id.clone(),
            sequence: 1,
            symbol: params.short_symbol,
            side: OrderSide::Sell,
            order_type: params.order_type,
            quantity: params.short_quantity,
            price: params.short_price,
            trigger_price: None,
            time_in_force: params.time_in_force,
            exchange: params.short_exchange,
            role: LegRole::PairShort,
            status: LegStatus::Pending,
            exchange_order_id: None,
            filled_quantity: 0.0,
            avg_fill_price: 0.0,
            fills: Vec::new(),
            created_at: now,
            updated_at: now,
            exchange_timestamp_ns: None,
        };
        
        let mut metadata = HashMap::new();
        metadata.insert("execution_strategy".to_string(), format!("{:?}", params.execution_strategy));
        if let Some(deviation) = params.max_spread_deviation_bps {
            metadata.insert("max_spread_deviation_bps".to_string(), deviation.to_string());
        }
        
        let group = MultiLegOrderGroup {
            group_id: group_id.clone(),
            order_type: MultiLegType::LinkedPair,
            state: GroupState::Pending,
            legs: vec![long_leg.clone(), short_leg.clone()],
            created_at: now,
            updated_at: now,
            completed_at: None,
            error: None,
            client_reference: None,
            metadata,
        };
        
        self.leg_to_group.insert(long_leg.leg_id, group_id.clone());
        self.leg_to_group.insert(short_leg.leg_id, group_id.clone());
        self.groups.insert(group_id, group.clone());
        
        self.stats.groups_created.fetch_add(1, Ordering::Relaxed);
        
        Ok(group)
    }
    
    /// Process a fill for a leg, handling OCO/bracket logic
    pub fn process_leg_fill(
        &self,
        leg_id: &str,
        fill: ExecutionFill,
        exchange_timestamp_ns: Option<u128>,
    ) -> Result<Vec<MultiLegEvent>, MultiLegError> {
        let group_id = self.leg_to_group.get(leg_id)
            .ok_or_else(|| MultiLegError::LegNotFound(leg_id.to_string()))?
            .clone();
        
        let mut events = Vec::new();
        let now = Self::now_ns();
        
        let mut group = self.groups.get_mut(&group_id)
            .ok_or_else(|| MultiLegError::GroupNotFound(group_id.clone()))?;
        
        let previous_state = group.state;
        let order_type = group.order_type.clone();
        let group_state = group.state;
        
        // Find the leg index and update it
        let leg_idx = group.legs.iter().position(|l| l.leg_id == leg_id);
        
        if let Some(idx) = leg_idx {
            // Update leg with fill info
            let leg = &mut group.legs[idx];
            let new_filled = leg.filled_quantity + fill.quantity;
            leg.avg_fill_price = if leg.filled_quantity == 0.0 {
                fill.price
            } else {
                (leg.avg_fill_price * leg.filled_quantity + fill.price * fill.quantity) / new_filled
            };
            leg.filled_quantity = new_filled;
            leg.fills.push(fill.clone());
            leg.updated_at = now;
            leg.exchange_timestamp_ns = exchange_timestamp_ns;
            
            // Update leg status
            let new_leg_status = if new_filled >= leg.quantity {
                LegStatus::Filled
            } else {
                LegStatus::PartiallyFilled
            };
            leg.status = new_leg_status;
            let leg_role = leg.role;
            
            // Emit leg fill event
            events.push(MultiLegEvent {
                group_id: group_id.clone(),
                event_type: if new_leg_status == LegStatus::Filled {
                    MultiLegEventType::LegFilled
                } else {
                    MultiLegEventType::LegPartialFill
                },
                leg_id: Some(leg_id.to_string()),
                previous_state,
                new_state: group_state,
                timestamp: now,
                details: Some(format!("Filled {} @ {}", fill.quantity, fill.price)),
            });
            
            // Handle multi-leg logic based on order type
            match order_type {
                MultiLegType::OCO => {
                    if new_leg_status == LegStatus::Filled {
                        // Cancel the other leg
                        let other_role = if leg_role == LegRole::OcoLegA {
                            LegRole::OcoLegB
                        } else {
                            LegRole::OcoLegA
                        };
                        
                        if let Some(other_leg) = group.legs.iter_mut().find(|l| l.role == other_role) {
                            if other_leg.status != LegStatus::Cancelled && other_leg.status != LegStatus::Filled {
                                other_leg.status = LegStatus::Cancelled;
                                other_leg.updated_at = now;
                                
                                events.push(MultiLegEvent {
                                    group_id: group_id.clone(),
                                    event_type: MultiLegEventType::LegCancelled,
                                    leg_id: Some(other_leg.leg_id.clone()),
                                    previous_state,
                                    new_state: group_state,
                                    timestamp: now,
                                    details: Some("OCO: Other leg cancelled".to_string()),
                                });
                                
                                self.stats.oco_triggers.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        
                        group.state = GroupState::Completed;
                        group.completed_at = Some(now);
                    }
                }
                
                MultiLegType::Bracket => {
                    if leg_role == LegRole::Entry && new_leg_status == LegStatus::Filled {
                        // Activate contingent orders
                        for contingent_leg in group.legs.iter_mut() {
                            if contingent_leg.status == LegStatus::Waiting {
                                contingent_leg.status = LegStatus::Pending;
                                contingent_leg.updated_at = now;
                            }
                        }
                        
                        group.state = GroupState::EntryFilled;
                        self.stats.bracket_entries_filled.fetch_add(1, Ordering::Relaxed);
                        
                        events.push(MultiLegEvent {
                            group_id: group_id.clone(),
                            event_type: MultiLegEventType::ContingentActivated,
                            leg_id: None,
                            previous_state,
                            new_state: group.state,
                            timestamp: now,
                            details: Some("Entry filled, contingent orders activated".to_string()),
                        });
                    } else if (leg_role == LegRole::StopLoss || leg_role == LegRole::TakeProfit) 
                              && new_leg_status == LegStatus::Filled {
                        // Cancel the other contingent leg
                        let other_role = if leg_role == LegRole::StopLoss {
                            LegRole::TakeProfit
                        } else {
                            LegRole::StopLoss
                        };
                        
                        if let Some(other_leg) = group.legs.iter_mut().find(|l| l.role == other_role) {
                            if other_leg.status != LegStatus::Cancelled && other_leg.status != LegStatus::Filled {
                                other_leg.status = LegStatus::Cancelled;
                                other_leg.updated_at = now;
                                
                                events.push(MultiLegEvent {
                                    group_id: group_id.clone(),
                                    event_type: MultiLegEventType::LegCancelled,
                                    leg_id: Some(other_leg.leg_id.clone()),
                                    previous_state,
                                    new_state: group.state,
                                    timestamp: now,
                                    details: Some("Bracket: Other contingent cancelled".to_string()),
                                });
                            }
                        }
                        
                        group.state = GroupState::Completed;
                        group.completed_at = Some(now);
                    }
                }
                
                MultiLegType::LinkedPair => {
                    // Check if both legs are filled
                    let all_filled = group.legs.iter().all(|l| l.status == LegStatus::Filled);
                    if all_filled {
                        group.state = GroupState::Completed;
                        group.completed_at = Some(now);
                    } else if group.legs.iter().any(|l| l.status == LegStatus::PartiallyFilled) {
                        group.state = GroupState::PartiallyFilled;
                    }
                }
                
                _ => {}
            }
            
            group.updated_at = now;
            
            // Check for completion
            if group.state == GroupState::Completed {
                self.stats.groups_completed.fetch_add(1, Ordering::Relaxed);
                
                events.push(MultiLegEvent {
                    group_id: group_id.clone(),
                    event_type: MultiLegEventType::GroupCompleted,
                    leg_id: None,
                    previous_state,
                    new_state: group.state,
                    timestamp: now,
                    details: None,
                });
            }
        }
        
        Ok(events)
    }
    
    /// Cancel an entire multi-leg order group
    pub fn cancel_group(&self, group_id: &str) -> Result<Vec<String>, MultiLegError> {
        let mut group = self.groups.get_mut(group_id)
            .ok_or_else(|| MultiLegError::GroupNotFound(group_id.to_string()))?;
        
        if group.is_terminal() {
            return Err(MultiLegError::GroupAlreadyTerminal(group_id.to_string()));
        }
        
        let now = Self::now_ns();
        let mut orders_to_cancel = Vec::new();
        
        for leg in group.legs.iter_mut() {
            if leg.status == LegStatus::Submitted || leg.status == LegStatus::PartiallyFilled {
                if let Some(ref exchange_order_id) = leg.exchange_order_id {
                    orders_to_cancel.push(exchange_order_id.clone());
                }
                leg.status = LegStatus::Cancelled;
                leg.updated_at = now;
            }
        }
        
        group.state = GroupState::Cancelled;
        group.updated_at = now;
        group.completed_at = Some(now);
        
        self.stats.groups_cancelled.fetch_add(1, Ordering::Relaxed);
        
        Ok(orders_to_cancel)
    }
    
    /// Get a group by ID
    pub fn get_group(&self, group_id: &str) -> Option<MultiLegOrderGroup> {
        self.groups.get(group_id).map(|g| g.clone())
    }
    
    /// Get all active groups
    pub fn get_active_groups(&self) -> Vec<MultiLegOrderGroup> {
        self.groups.iter()
            .filter(|g| !g.is_terminal())
            .map(|g| g.clone())
            .collect()
    }
    
    /// Get statistics
    pub fn get_stats(&self) -> MultiLegStatsSnapshot {
        MultiLegStatsSnapshot {
            groups_created: self.stats.groups_created.load(Ordering::Relaxed),
            groups_completed: self.stats.groups_completed.load(Ordering::Relaxed),
            groups_cancelled: self.stats.groups_cancelled.load(Ordering::Relaxed),
            groups_failed: self.stats.groups_failed.load(Ordering::Relaxed),
            oco_triggers: self.stats.oco_triggers.load(Ordering::Relaxed),
            bracket_entries_filled: self.stats.bracket_entries_filled.load(Ordering::Relaxed),
            active_groups: self.groups.iter().filter(|g| !g.is_terminal()).count() as u64,
        }
    }
    
    /// Register an event handler
    pub fn on_event<F>(&self, handler: F)
    where
        F: Fn(MultiLegEvent) + Send + Sync + 'static,
    {
        self.event_handlers.write().push(Box::new(handler));
    }
    
    /// Link exchange order ID to leg
    pub fn link_exchange_order(&self, leg_id: &str, exchange_order_id: &str) -> Result<(), MultiLegError> {
        let group_id = self.leg_to_group.get(leg_id)
            .ok_or_else(|| MultiLegError::LegNotFound(leg_id.to_string()))?
            .clone();
        
        let mut group = self.groups.get_mut(&group_id)
            .ok_or_else(|| MultiLegError::GroupNotFound(group_id.clone()))?;
        
        if let Some(leg) = group.legs.iter_mut().find(|l| l.leg_id == leg_id) {
            leg.exchange_order_id = Some(exchange_order_id.to_string());
            leg.status = LegStatus::Submitted;
            leg.updated_at = Self::now_ns();
        }
        
        self.exchange_order_to_leg.insert(exchange_order_id.to_string(), leg_id.to_string());
        
        Ok(())
    }
    
    /// Get leg by exchange order ID
    pub fn get_leg_by_exchange_order(&self, exchange_order_id: &str) -> Option<(GroupId, OrderLeg)> {
        let leg_id = self.exchange_order_to_leg.get(exchange_order_id)?.clone();
        let group_id = self.leg_to_group.get(&leg_id)?.clone();
        let group = self.groups.get(&group_id)?;
        let leg = group.legs.iter().find(|l| l.leg_id == leg_id)?.clone();
        Some((group_id, leg))
    }
}

impl Default for MultiLegOrderManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiLegStatsSnapshot {
    pub groups_created: u64,
    pub groups_completed: u64,
    pub groups_cancelled: u64,
    pub groups_failed: u64,
    pub oco_triggers: u64,
    pub bracket_entries_filled: u64,
    pub active_groups: u64,
}

/// Multi-leg order errors
#[derive(Debug, Clone)]
pub enum MultiLegError {
    /// Invalid parameter
    InvalidParameter(String),
    /// Group not found
    GroupNotFound(String),
    /// Leg not found
    LegNotFound(String),
    /// Group already in terminal state
    GroupAlreadyTerminal(String),
    /// Maximum groups exceeded
    MaxGroupsExceeded,
    /// Execution error
    ExecutionError(String),
}

impl std::fmt::Display for MultiLegError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MultiLegError::InvalidParameter(msg) => write!(f, "Invalid parameter: {}", msg),
            MultiLegError::GroupNotFound(id) => write!(f, "Group not found: {}", id),
            MultiLegError::LegNotFound(id) => write!(f, "Leg not found: {}", id),
            MultiLegError::GroupAlreadyTerminal(id) => write!(f, "Group already terminal: {}", id),
            MultiLegError::MaxGroupsExceeded => write!(f, "Maximum number of groups exceeded"),
            MultiLegError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
        }
    }
}

impl std::error::Error for MultiLegError {}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_create_oco_order() {
        let manager = MultiLegOrderManager::new();
        
        let params = OcoParams {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            leg_a_side: OrderSide::Sell,
            leg_a_type: OrderType::Limit,
            leg_a_quantity: 1.0,
            leg_a_price: 55000.0,  // Take profit
            leg_b_side: OrderSide::Sell,
            leg_b_type: OrderType::Stop,
            leg_b_quantity: 1.0,
            leg_b_price: 48000.0,  // Stop loss
            leg_b_trigger: Some(48000.0),
            time_in_force: TimeInForce::GoodTillCancelled,
            client_group_id: None,
        };
        
        let group = manager.create_oco(params).unwrap();
        
        assert_eq!(group.order_type, MultiLegType::OCO);
        assert_eq!(group.state, GroupState::Pending);
        assert_eq!(group.legs.len(), 2);
        assert_eq!(group.legs[0].role, LegRole::OcoLegA);
        assert_eq!(group.legs[1].role, LegRole::OcoLegB);
    }
    
    #[test]
    fn test_create_bracket_order() {
        let manager = MultiLegOrderManager::new();
        
        let params = BracketParams {
            symbol: "ETH-USD".to_string(),
            exchange: "kraken".to_string(),
            entry_side: OrderSide::Buy,
            entry_type: OrderType::Limit,
            entry_quantity: 10.0,
            entry_price: Some(2000.0),
            stop_loss_price: 1900.0,
            stop_loss_limit: None,
            take_profit_price: 2200.0,
            take_profit_limit: None,
            time_in_force: TimeInForce::GoodTillCancelled,
            submit_contingent_immediately: false,
            client_group_id: None,
        };
        
        let group = manager.create_bracket(params).unwrap();
        
        assert_eq!(group.order_type, MultiLegType::Bracket);
        assert_eq!(group.legs.len(), 3);
        
        let entry = group.entry_leg().unwrap();
        assert_eq!(entry.role, LegRole::Entry);
        assert_eq!(entry.status, LegStatus::Pending);
        
        let sl = group.stop_loss_leg().unwrap();
        assert_eq!(sl.role, LegRole::StopLoss);
        assert_eq!(sl.status, LegStatus::Waiting);
        
        let tp = group.take_profit_leg().unwrap();
        assert_eq!(tp.role, LegRole::TakeProfit);
        assert_eq!(tp.status, LegStatus::Waiting);
    }
    
    #[test]
    fn test_bracket_entry_fill_activates_contingent() {
        let manager = MultiLegOrderManager::new();
        
        let params = BracketParams {
            symbol: "ETH-USD".to_string(),
            exchange: "kraken".to_string(),
            entry_side: OrderSide::Buy,
            entry_type: OrderType::Limit,
            entry_quantity: 10.0,
            entry_price: Some(2000.0),
            stop_loss_price: 1900.0,
            stop_loss_limit: None,
            take_profit_price: 2200.0,
            take_profit_limit: None,
            time_in_force: TimeInForce::GoodTillCancelled,
            submit_contingent_immediately: false,
            client_group_id: None,
        };
        
        let group = manager.create_bracket(params).unwrap();
        let entry_leg_id = group.legs[0].leg_id.clone();
        
        // Simulate entry fill
        let fill = ExecutionFill {
            fill_id: "fill-1".to_string(),
            order_id: "order-1".to_string(),
            exchange_order_id: "exch-1".to_string(),
            symbol: "ETH-USD".to_string(),
            side: OrderSide::Buy,
            quantity: 10.0,
            price: 2000.0,
            fee: 1.0,
            fee_asset: "USD".to_string(),
            timestamp: 0,
            trade_id: "trade-1".to_string(),
            is_maker: true,
            exchange_timestamp_ns: None,
            exchange_sequence: None,
        };
        
        let events = manager.process_leg_fill(&entry_leg_id, fill, Some(123456789)).unwrap();
        
        // Should have events for fill and contingent activation
        assert!(events.iter().any(|e| e.event_type == MultiLegEventType::LegFilled));
        assert!(events.iter().any(|e| e.event_type == MultiLegEventType::ContingentActivated));
        
        // Check group state updated
        let updated_group = manager.get_group(&group.group_id).unwrap();
        assert_eq!(updated_group.state, GroupState::EntryFilled);
        
        // Check contingent legs activated
        assert_eq!(updated_group.stop_loss_leg().unwrap().status, LegStatus::Pending);
        assert_eq!(updated_group.take_profit_leg().unwrap().status, LegStatus::Pending);
    }
    
    #[test]
    fn test_oco_cancel_other_leg_on_fill() {
        let manager = MultiLegOrderManager::new();
        
        let params = OcoParams {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            leg_a_side: OrderSide::Sell,
            leg_a_type: OrderType::Limit,
            leg_a_quantity: 1.0,
            leg_a_price: 55000.0,
            leg_b_side: OrderSide::Sell,
            leg_b_type: OrderType::Stop,
            leg_b_quantity: 1.0,
            leg_b_price: 48000.0,
            leg_b_trigger: Some(48000.0),
            time_in_force: TimeInForce::GoodTillCancelled,
            client_group_id: None,
        };
        
        let group = manager.create_oco(params).unwrap();
        let leg_a_id = group.legs[0].leg_id.clone();
        
        // Fill leg A
        let fill = ExecutionFill {
            fill_id: "fill-1".to_string(),
            order_id: "order-1".to_string(),
            exchange_order_id: "exch-1".to_string(),
            symbol: "BTC-USD".to_string(),
            side: OrderSide::Sell,
            quantity: 1.0,
            price: 55000.0,
            fee: 5.0,
            fee_asset: "USD".to_string(),
            timestamp: 0,
            trade_id: "trade-1".to_string(),
            is_maker: true,
            exchange_timestamp_ns: None,
            exchange_sequence: None,
        };
        
        let events = manager.process_leg_fill(&leg_a_id, fill, None).unwrap();
        
        // Should cancel leg B
        assert!(events.iter().any(|e| e.event_type == MultiLegEventType::LegCancelled));
        assert!(events.iter().any(|e| e.event_type == MultiLegEventType::GroupCompleted));
        
        let updated_group = manager.get_group(&group.group_id).unwrap();
        assert_eq!(updated_group.state, GroupState::Completed);
        assert_eq!(updated_group.legs[1].status, LegStatus::Cancelled);
    }
    
    #[test]
    fn test_linked_pair_creation() {
        let manager = MultiLegOrderManager::new();
        
        let params = LinkedPairParams {
            long_symbol: "BTC-USD".to_string(),
            long_exchange: "kraken".to_string(),
            long_quantity: 1.0,
            long_price: Some(50000.0),
            short_symbol: "ETH-USD".to_string(),
            short_exchange: "kraken".to_string(),
            short_quantity: 15.0,
            short_price: Some(3300.0),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GoodTillCancelled,
            execution_strategy: PairExecutionStrategy::Simultaneous,
            max_spread_deviation_bps: Some(50.0),
            client_group_id: None,
        };
        
        let group = manager.create_linked_pair(params).unwrap();
        
        assert_eq!(group.order_type, MultiLegType::LinkedPair);
        assert_eq!(group.legs.len(), 2);
        assert_eq!(group.legs[0].role, LegRole::PairLong);
        assert_eq!(group.legs[1].role, LegRole::PairShort);
        assert!(group.metadata.contains_key("execution_strategy"));
    }
    
    #[test]
    fn test_cancel_group() {
        let manager = MultiLegOrderManager::new();
        
        let params = OcoParams {
            symbol: "BTC-USD".to_string(),
            exchange: "kraken".to_string(),
            leg_a_side: OrderSide::Sell,
            leg_a_type: OrderType::Limit,
            leg_a_quantity: 1.0,
            leg_a_price: 55000.0,
            leg_b_side: OrderSide::Sell,
            leg_b_type: OrderType::Stop,
            leg_b_quantity: 1.0,
            leg_b_price: 48000.0,
            leg_b_trigger: Some(48000.0),
            time_in_force: TimeInForce::GoodTillCancelled,
            client_group_id: None,
        };
        
        let group = manager.create_oco(params).unwrap();
        
        // Link exchange orders
        manager.link_exchange_order(&group.legs[0].leg_id, "exch-order-1").unwrap();
        manager.link_exchange_order(&group.legs[1].leg_id, "exch-order-2").unwrap();
        
        // Cancel group
        let orders_to_cancel = manager.cancel_group(&group.group_id).unwrap();
        
        assert_eq!(orders_to_cancel.len(), 2);
        
        let updated_group = manager.get_group(&group.group_id).unwrap();
        assert_eq!(updated_group.state, GroupState::Cancelled);
    }
    
    #[test]
    fn test_statistics() {
        let manager = MultiLegOrderManager::new();
        
        // Create some groups
        for _ in 0..3 {
            let params = OcoParams {
                symbol: "BTC-USD".to_string(),
                exchange: "kraken".to_string(),
                leg_a_side: OrderSide::Sell,
                leg_a_type: OrderType::Limit,
                leg_a_quantity: 1.0,
                leg_a_price: 55000.0,
                leg_b_side: OrderSide::Sell,
                leg_b_type: OrderType::Stop,
                leg_b_quantity: 1.0,
                leg_b_price: 48000.0,
                leg_b_trigger: Some(48000.0),
                time_in_force: TimeInForce::GoodTillCancelled,
                client_group_id: None,
            };
            manager.create_oco(params).unwrap();
        }
        
        let stats = manager.get_stats();
        assert_eq!(stats.groups_created, 3);
        assert_eq!(stats.active_groups, 3);
    }
    
    #[test]
    fn test_bracket_validation() {
        let manager = MultiLegOrderManager::new();
        
        // Invalid: stop-loss above entry for long position
        let params = BracketParams {
            symbol: "ETH-USD".to_string(),
            exchange: "kraken".to_string(),
            entry_side: OrderSide::Buy,
            entry_type: OrderType::Limit,
            entry_quantity: 10.0,
            entry_price: Some(2000.0),
            stop_loss_price: 2100.0, // Invalid: above entry
            stop_loss_limit: None,
            take_profit_price: 2200.0,
            take_profit_limit: None,
            time_in_force: TimeInForce::GoodTillCancelled,
            submit_contingent_immediately: false,
            client_group_id: None,
        };
        
        let result = manager.create_bracket(params);
        assert!(result.is_err());
    }
}
