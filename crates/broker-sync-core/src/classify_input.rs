//! Input types for the difference classifier (ruling R54-B2, built in PR 5). Types only: there is no classifier
//! logic in this crate yet, so nothing here decides whether two snapshots differ.

use crate::snapshot::{AccountRef, AccountSnapshot, Broker, Environment};
use broker_adapters::Dec;
use chrono::{DateTime, Utc};

/// What the platform expects the account to hold: the rebalancer's last post-run snapshot plus the effect of its own
/// recorded fills. Absent for unmanaged accounts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedBaseline {
    pub snapshot: AccountSnapshot,
    /// Quantity change per symbol that the platform's own recorded fills account for since `snapshot`.
    pub own_fill_deltas: Vec<(String, Dec)>,
}

/// The platform's target for one instrument: weight of account value, with the plan's own rebalance band.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanTarget {
    pub symbol: String,
    pub target_weight: Dec,
    /// Half-width of the band in the same units as the weight; `None` means use the platform default.
    pub band: Option<Dec>,
}

/// Which orders are the platform's own: by client tag prefix and by recorded broker order id.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OwnOrders {
    pub tag_prefixes: Vec<String>,
    pub broker_order_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CashFlowKind {
    Deposit,
    Withdrawal,
}

/// A cash movement the user confirmed ("Yes, I moved this money").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedCashFlow {
    pub kind: CashFlowKind,
    pub amount: Dec,
    pub confirmed_at: DateTime<Utc>,
}

/// Everything the classifier will receive for one account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifyInput {
    pub broker: Broker,
    pub account: AccountRef,
    pub environment: Environment,
    pub current: AccountSnapshot,
    /// The previous snapshot, a minute or more earlier (a difference must persist across two reads).
    pub previous: Option<AccountSnapshot>,
    pub baseline: Option<ExpectedBaseline>,
    pub plan: Vec<PlanTarget>,
    pub own_orders: OwnOrders,
    pub cash_flows: Vec<ConfirmedCashFlow>,
}
