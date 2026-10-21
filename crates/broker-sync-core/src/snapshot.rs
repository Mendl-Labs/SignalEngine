//! The normalised account snapshot: one shape for every venue.

use broker_adapters::Dec;
use chrono::{DateTime, Utc};

/// Brokers the sync worker knows by name. Venues without an adapter (Binance, Coinbase, ...) are absent on purpose:
/// they are shown as "not supported yet" by the dashboard and never get a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Broker {
    Alpaca,
    Kraken,
    Oanda,
    Bitstamp,
}

impl Broker {
    pub fn as_str(self) -> &'static str {
        match self {
            Broker::Alpaca => "alpaca",
            Broker::Kraken => "kraken",
            Broker::Oanda => "oanda",
            Broker::Bitstamp => "bitstamp",
        }
    }
}

/// Paper and live are never added together (R54 decision 7); the environment is part of every snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Environment {
    Paper,
    Live,
}

impl Environment {
    pub fn as_str(self) -> &'static str {
        match self {
            Environment::Paper => "paper",
            Environment::Live => "live",
        }
    }
}

/// A stable, non-secret reference to one broker account (for example an Alpaca account number). Never a key id,
/// never a secret.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountRef(pub String);

impl AccountRef {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One holding. `qty` is signed (negative for a short). `price` and `value` are `None` when the broker did not
/// report them; they are never invented and never defaulted to zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub symbol: String,
    pub qty: Dec,
    pub price: Option<Dec>,
    pub value: Option<Dec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// An order that is still working at the broker. The `client_tag` is what lets the classifier (PR 5) tell the
/// platform's own orders from foreign ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOrder {
    pub broker_order_id: String,
    pub client_tag: Option<String>,
    pub symbol: String,
    pub side: Option<OrderSide>,
    pub qty: Dec,
    pub filled_qty: Dec,
    /// The broker's own status word, kept verbatim for display and debugging.
    pub status: String,
}

/// Whether the snapshot is a complete read. Anything less is carried to the dashboard as a note, never hidden.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceStatus {
    Complete,
    /// The read succeeded but some figures are missing (for example a position the broker did not price).
    Partial { notes: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSnapshot {
    pub broker: Broker,
    pub account: AccountRef,
    pub environment: Environment,
    /// Upper-case ISO currency code of `equity` and `cash`.
    pub currency: String,
    pub equity: Dec,
    pub cash: Dec,
    pub positions: Vec<Position>,
    pub open_orders: Vec<OpenOrder>,
    pub taken_at: DateTime<Utc>,
    pub source_status: SourceStatus,
}

impl AccountSnapshot {
    pub fn position(&self, symbol: &str) -> Option<&Position> {
        self.positions.iter().find(|p| p.symbol == symbol)
    }
}
