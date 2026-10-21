//! Alpaca reader: wraps the existing tested `broker-adapters` Alpaca adapter and calls its READ methods only
//! (account, positions, open orders). It is handed an already-built adapter; it never builds credentials and never
//! sees a key.

use crate::reader::{AccountSummary, BrokerReader, ReadError};
use crate::snapshot::{AccountRef, Broker, Environment, OpenOrder, OrderSide, Position};
use broker_adapters::alpaca::{AccountInfo, AlpacaAdapter, PositionInfo};
use broker_adapters::{BrokerAdapter, OrderReport, Side};

pub struct AlpacaReader {
    adapter: AlpacaAdapter,
}

impl AlpacaReader {
    pub fn new(adapter: AlpacaAdapter) -> Self {
        Self { adapter }
    }

    fn env(&self) -> Environment {
        match self.adapter.environment() {
            broker_adapters::alpaca::Environment::Paper => Environment::Paper,
            broker_adapters::alpaca::Environment::Live => Environment::Live,
        }
    }
}

impl std::fmt::Debug for AlpacaReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaReader").field("adapter", &self.adapter).finish()
    }
}

/// Normalise an Alpaca account object. Pure.
pub fn normalise_account(acct: &AccountInfo, environment: Environment) -> Result<AccountSummary, ReadError> {
    let number = acct
        .account_number
        .clone()
        .filter(|n| !n.is_empty())
        .ok_or_else(|| ReadError::Malformed("account has no account_number".to_string()))?;
    Ok(AccountSummary {
        account: AccountRef(number),
        environment,
        currency: acct.currency.clone().unwrap_or_else(|| "USD".to_string()).to_ascii_uppercase(),
        equity: acct.equity,
        cash: acct.cash,
    })
}

/// Normalise Alpaca positions (signed quantity). Pure.
pub fn normalise_positions(positions: &[PositionInfo]) -> Vec<Position> {
    positions
        .iter()
        .map(|p| Position {
            symbol: p.symbol.clone(),
            qty: p.signed_qty(),
            price: p.current_price,
            value: Some(p.market_value),
        })
        .collect()
}

/// Normalise open orders. Pure.
pub fn normalise_orders(orders: &[OrderReport]) -> Vec<OpenOrder> {
    orders
        .iter()
        .map(|o| OpenOrder {
            broker_order_id: o.broker_order_id.clone(),
            client_tag: o.tag.clone(),
            symbol: o.symbol.clone(),
            side: o.side.map(|s| match s {
                Side::Buy => OrderSide::Buy,
                Side::Sell => OrderSide::Sell,
            }),
            qty: o.quantity,
            filled_qty: o.executed_quantity,
            status: o.raw_status.clone(),
        })
        .collect()
}

impl BrokerReader for AlpacaReader {
    fn broker(&self) -> Broker {
        Broker::Alpaca
    }

    fn read_account_summary(&self) -> Result<AccountSummary, ReadError> {
        let acct = self.adapter.get_account().map_err(ReadError::from_broker_error)?;
        normalise_account(&acct, self.env())
    }

    fn read_positions(&self) -> Result<Vec<Position>, ReadError> {
        let p = self.adapter.get_positions().map_err(ReadError::from_broker_error)?;
        Ok(normalise_positions(&p))
    }

    fn read_open_orders(&self) -> Result<Vec<OpenOrder>, ReadError> {
        let o = self.adapter.open_orders().map_err(ReadError::from_broker_error)?;
        Ok(normalise_orders(&o))
    }
}
