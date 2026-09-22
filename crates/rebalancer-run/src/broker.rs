//! `Broker`: the thin, object-safe wrapper over the adapters that the pipeline codes against, so tests can put a
//! scripted or crashing broker in front of the real adapter without changing the pipeline.
//!
//! Contract notes the callers rely on (they come from `broker-adapters` and are repeated because getting them wrong
//! loses money):
//! * `place` returning `Ok(PlaceOutcome::UnknownOutcome)` means the order MAY exist: look it up with
//!   [`Broker::find_by_tag`] before doing anything else. `Err(_)` from `place` means the order was definitely NOT
//!   sent.
//! * `cancel_and_settle` returns the order's report AFTER the cancel: callers must check `report.status`. An order
//!   that filled before the cancel landed comes back `Filled` with a zero cancel count, not as an error.
//! * `find_by_tag` on Kraken needs the tag's userref in the adapter's table; the Kraken wrapper reserves it first
//!   (deterministic for an unseen tag), so a restarted process can still find its own orders.

use std::collections::BTreeMap;

use broker_adapters::alpaca::AlpacaAdapter;
use broker_adapters::kraken::KrakenAdapter;
use broker_adapters::{
    BalanceKind, BrokerAdapter, BrokerError, CancelOutcome, Dec, OrderReport, OrderRequest, PlaceOutcome, Quote,
};
use chrono::{DateTime, Utc};

use crate::view::{alpaca_snapshot, kraken_snapshot, BrokerSnapshot, KrakenViewInput, ViewError};

/// Every order tag this rebalancer creates starts with this (planner tags, flatten tags). An open order without it
/// (and whose id we never recorded) is FOREIGN.
pub const OWN_TAG_PREFIX: &str = "rb1:";

/// Errors of the pipeline-level broker calls: an adapter error, or the reading could not be turned into a snapshot.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("BROKER_UNREACHABLE: {0}")]
    Broker(#[from] BrokerError),
    #[error("{0}")]
    View(#[from] ViewError),
}

impl SnapshotError {
    pub fn code(&self) -> &'static str {
        match self {
            SnapshotError::Broker(_) => "BROKER_READ_FAILED",
            SnapshotError::View(v) => v.code(),
        }
    }
}

pub trait Broker {
    fn venue(&self) -> &'static str;
    /// Read the whole account (balances, equity, holdings, open orders) as of `now`.
    fn snapshot(&self, now: DateTime<Utc>) -> Result<BrokerSnapshot, SnapshotError>;
    fn place(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError>;
    fn get_order(&self, broker_order_id: &str) -> Result<OrderReport, BrokerError>;
    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError>;
    /// Orders (open or finished) carrying `tag`. May be empty. See the module docs.
    fn find_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError>;
    fn cancel_and_settle(&self, broker_order_id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError>;
    fn quote(&self, symbol: &str) -> Result<Quote, BrokerError>;
}

/// Is this order one of ours? Its tag carries our prefix, or its broker id is one we recorded when placing it.
pub fn is_own_order(order: &OrderReport, known_order_ids: &std::collections::BTreeSet<String>) -> bool {
    order.tag.as_deref().is_some_and(|t| t.starts_with(OWN_TAG_PREFIX)) || known_order_ids.contains(&order.broker_order_id)
}

// ------------------------------------------------------------------------------------------------------------
// Kraken
// ------------------------------------------------------------------------------------------------------------

/// The Kraken spot account in USD (or another quote asset the adapter's pair table knows).
pub struct KrakenBroker<'a> {
    pub adapter: &'a KrakenAdapter,
    /// Canonical quote asset and account currency, `USD`.
    pub quote_asset: String,
    /// Kraken's asset code to ask `TradeBalance` in (`ZUSD`).
    pub trade_balance_asset: String,
    pub asset_class: String,
}

impl<'a> KrakenBroker<'a> {
    pub fn usd(adapter: &'a KrakenAdapter) -> Self {
        Self {
            adapter,
            quote_asset: "USD".to_string(),
            trade_balance_asset: "ZUSD".to_string(),
            asset_class: "crypto_spot".to_string(),
        }
    }
}

impl Broker for KrakenBroker<'_> {
    fn venue(&self) -> &'static str {
        "kraken"
    }

    fn snapshot(&self, now: DateTime<Utc>) -> Result<BrokerSnapshot, SnapshotError> {
        let balances = self.adapter.get_balances()?;
        let trade_balance = self.adapter.trade_balance(Some(&self.trade_balance_asset))?;
        let open_orders = self.adapter.open_orders()?;
        let mut marks: BTreeMap<String, Dec> = BTreeMap::new();
        for e in &balances.entries {
            if e.kind != BalanceKind::Spot || e.amount.is_zero() || e.asset.eq_ignore_ascii_case(&self.quote_asset) {
                continue;
            }
            let symbol = format!("{}/{}", e.asset.to_uppercase(), self.quote_asset);
            if self.adapter.pair_table().lookup(&symbol).is_some() && !marks.contains_key(&symbol) {
                let q = self.adapter.get_quote(&symbol)?;
                marks.insert(symbol, q.last);
            }
        }
        Ok(kraken_snapshot(
            &KrakenViewInput {
                balances: &balances,
                trade_balance: &trade_balance,
                open_orders,
                marks: &marks,
                quote_asset: &self.quote_asset,
                asset_class: &self.asset_class,
            },
            now,
        )?)
    }

    fn place(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        self.adapter.place_order(req)
    }

    fn get_order(&self, id: &str) -> Result<OrderReport, BrokerError> {
        self.adapter.get_order(id)
    }

    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.adapter.open_orders()
    }

    fn find_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        self.adapter.reserve_userref(tag)?;
        self.adapter.find_orders_by_tag(tag)
    }

    fn cancel_and_settle(&self, id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        self.adapter.cancel_and_settle(id)
    }

    fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.adapter.get_quote(symbol)
    }
}

// ------------------------------------------------------------------------------------------------------------
// Alpaca
// ------------------------------------------------------------------------------------------------------------

/// The Alpaca equities account. Configure the adapter with `own_tag_prefix: Some(OWN_TAG_PREFIX)` so foreign orders
/// come back with `tag: None`.
pub struct AlpacaBroker<'a> {
    pub adapter: &'a AlpacaAdapter,
    pub asset_class: String,
}

impl<'a> AlpacaBroker<'a> {
    pub fn us_etf(adapter: &'a AlpacaAdapter) -> Self {
        Self { adapter, asset_class: "us_etf".to_string() }
    }
}

impl Broker for AlpacaBroker<'_> {
    fn venue(&self) -> &'static str {
        "alpaca"
    }

    fn snapshot(&self, now: DateTime<Utc>) -> Result<BrokerSnapshot, SnapshotError> {
        let account = self.adapter.get_account()?;
        let positions = self.adapter.get_positions()?;
        let open_orders = self.adapter.open_orders()?;
        let class = self.asset_class.clone();
        Ok(alpaca_snapshot(&account, &positions, open_orders, &move |_| class.clone(), now)?)
    }

    fn place(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        self.adapter.place_order(req)
    }

    fn get_order(&self, id: &str) -> Result<OrderReport, BrokerError> {
        self.adapter.get_order(id)
    }

    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.adapter.open_orders()
    }

    fn find_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        self.adapter.find_orders_by_tag(tag)
    }

    fn cancel_and_settle(&self, id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        self.adapter.cancel_and_settle(id)
    }

    fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.adapter.get_quote(symbol)
    }
}
