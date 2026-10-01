//! Broker-neutral request/response data and the [`BrokerAdapter`] trait.

use crate::decimal::Dec;
use crate::error::{BrokerError, ExchangeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }
}

/// Order kind. The limit price lives inside the variant so an incoherent request
/// (limit without a price, market with one) cannot be expressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderKind {
    Market,
    Limit { price: Dec },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    Gtc,
    Ioc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRequest {
    /// Our deterministic idempotency tag (for example `"<run-key>:BTC/USD:buy"`). Mapped to the
    /// broker's client-order-id field by the adapter.
    pub tag: String,
    /// Canonical symbol, `BASE/QUOTE` with ticker names (`BTC/USD`, `ETH/USD`).
    pub symbol: String,
    pub side: Side,
    pub kind: OrderKind,
    /// Quantity in base units, before the adapter rounds it to the pair's precision.
    pub quantity: Dec,
    pub time_in_force: Option<TimeInForce>,
    pub post_only: bool,
    /// Ask the broker to only reduce an existing position. Adapters that cannot honour this
    /// return `Unsupported` rather than silently dropping the safety flag.
    pub reduce_only: bool,
    /// Ask the broker to validate the order without placing it.
    pub validate_only: bool,
    /// Price used only for the minimum-cost check of market orders.
    pub reference_price: Option<Dec>,
}

impl OrderRequest {
    pub fn market(tag: &str, symbol: &str, side: Side, quantity: Dec) -> Self {
        Self::base(tag, symbol, side, OrderKind::Market, quantity)
    }
    pub fn limit(tag: &str, symbol: &str, side: Side, quantity: Dec, price: Dec) -> Self {
        Self::base(tag, symbol, side, OrderKind::Limit { price }, quantity)
    }
    fn base(tag: &str, symbol: &str, side: Side, kind: OrderKind, quantity: Dec) -> Self {
        Self {
            tag: tag.to_string(),
            symbol: symbol.to_string(),
            side,
            kind,
            quantity,
            time_in_force: None,
            post_only: false,
            reduce_only: false,
            validate_only: false,
            reference_price: None,
        }
    }
}

/// What was actually sent to the broker after rounding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentOrder {
    pub broker_pair: String,
    pub side: Side,
    pub quantity: Dec,
    pub price: Option<Dec>,
    pub userref: i32,
    pub validate_only: bool,
}

/// Result of an order placement attempt. `UnknownOutcome` means the order MAY exist on the
/// broker: the caller must reconcile by tag (`find_orders_by_tag`) before any retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaceOutcome {
    Accepted { broker_order_id: String, description: Option<String>, sent: SentOrder, warnings: Vec<String> },
    ValidatedOnly { description: Option<String>, sent: SentOrder, warnings: Vec<String> },
    Rejected { errors: Vec<ExchangeError>, sent: SentOrder },
    UnknownOutcome { reason: String, sent: SentOrder },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    /// Accepted but not yet on the book.
    Pending,
    /// On the book, nothing executed.
    Open,
    /// On the book, partly executed.
    PartiallyFilled,
    /// Completely executed.
    Filled,
    /// Ended after executing part of the quantity by cancellation. The executed quantity is real.
    PartiallyFilledThenCanceled,
    /// Ended after executing part of the quantity by expiry. The executed quantity is real.
    PartiallyFilledThenExpired,
    /// Ended with nothing executed (canceled, or closed with zero fill).
    Canceled,
    /// Expired with nothing executed.
    Expired,
    /// Refused by the broker (Alpaca `rejected`). Nothing executed.
    Rejected,
}

impl OrderStatus {
    pub fn is_terminal(self) -> bool {
        !matches!(self, OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled)
    }
    /// True when any quantity executed. Position and P&L accounting must use the executed
    /// quantity of such orders even when the order was later canceled.
    pub fn has_fills(self) -> bool {
        matches!(
            self,
            OrderStatus::PartiallyFilled
                | OrderStatus::Filled
                | OrderStatus::PartiallyFilledThenCanceled
                | OrderStatus::PartiallyFilledThenExpired
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderReport {
    pub broker_order_id: String,
    /// Broker client-order-id (Kraken `userref`), if the order carried one.
    pub userref: Option<i32>,
    /// Our tag, when the userref is in our mapping table. `None` for foreign orders.
    pub tag: Option<String>,
    /// Canonical symbol when known, otherwise the broker's raw pair string.
    pub symbol: String,
    pub side: Option<Side>,
    pub kind: Option<OrderKind>,
    pub status: OrderStatus,
    /// Broker's own status string, kept verbatim.
    pub raw_status: String,
    pub reason: Option<String>,
    pub quantity: Dec,
    pub executed_quantity: Dec,
    /// Average execution price; `None` when nothing executed.
    pub avg_price: Option<Dec>,
    /// Total executed cost in the quote currency, as reported.
    pub cost: Option<Dec>,
    /// Total fee as reported (Kraken: quote currency). `None` = the field was absent.
    pub fee: Option<Dec>,
    pub open_time: Option<f64>,
    pub close_time: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalanceKind {
    /// Free spot balance.
    Spot,
    /// Staked / earn / opt-in-reward balance (Kraken suffixes `.S .F .M .P .B`); not tradable.
    Earn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceEntry {
    pub raw_asset: String,
    /// Canonical ticker (`BTC`, `ETH`, `USD`).
    pub asset: String,
    pub amount: Dec,
    pub kind: BalanceKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Balances {
    pub entries: Vec<BalanceEntry>,
}

impl Balances {
    /// Sum of `Spot` entries for a canonical asset; zero when absent.
    pub fn spot(&self, asset: &str) -> Dec {
        self.entries
            .iter()
            .filter(|e| e.kind == BalanceKind::Spot && e.asset.eq_ignore_ascii_case(asset))
            .fold(Dec::ZERO, |acc, e| acc.checked_add(e.amount).unwrap_or(acc))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quote {
    pub symbol: String,
    pub bid: Dec,
    pub ask: Dec,
    pub last: Dec,
}

impl Quote {
    pub fn mid(&self) -> f64 {
        (self.bid.to_f64() + self.ask.to_f64()) / 2.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelOutcome {
    pub canceled_count: u32,
    /// The broker accepted the cancel but has not finished processing it.
    pub pending: bool,
}

/// The broker-neutral surface the rebalancer codes against.
///
/// All calls are synchronous. Implementations must not log secrets.
pub trait BrokerAdapter {
    fn broker_name(&self) -> &'static str;
    fn get_balances(&self) -> Result<Balances, BrokerError>;
    fn get_quote(&self, symbol: &str) -> Result<Quote, BrokerError>;
    /// Place (or validate) an order. See [`PlaceOutcome`] for the unknown-outcome contract.
    fn place_order(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError>;
    fn get_order(&self, broker_order_id: &str) -> Result<OrderReport, BrokerError>;
    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError>;
    /// All orders (open or finished) carrying the userref assigned to `tag`.
    fn find_orders_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError>;
    /// Cancel one order. Always follow with `get_order`: the order may have executed part of its
    /// quantity before the cancel took effect.
    fn cancel_order(&self, broker_order_id: &str) -> Result<CancelOutcome, BrokerError>;
}
