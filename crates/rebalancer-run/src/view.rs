//! `BrokerSnapshot`: one venue-neutral reading of a broker account, plus the per-venue builders that turn the
//! adapters' responses into it. Everything here is pure: a builder takes already-parsed adapter responses.
//!
//! # Equity definitions (a reviewer must confirm these; only the Kraken one was exercised against the fake)
//! * **Kraken** (`kraken_snapshot`): `equity` is the BROKER-reported `TradeBalance` equity `e` (falling back to the
//!   equivalent balance `eb`, and refusing when neither is present), requested in the account currency (USD).
//!   `cash` is the free SPOT balance of the quote currency (`Balance`, kind `Spot`). Kraken has no "positions" for
//!   spot: every non-zero spot balance of another asset is a holding, valued at the pair's LAST price (`mark`). Earn /
//!   staked balances (`.S`, `.F`, ...) are not tradable and are excluded from cash and holdings; if the broker's
//!   equity includes them, the equity cross-check ([`BrokerSnapshot::derived_equity`]) will differ and reconciliation
//!   halts (a dedicated trading account should hold none). A non-zero balance of an asset with no pair in the table
//!   cannot be valued: it is listed in `unvalued` and reconciliation halts on it.
//! * **Alpaca** (`alpaca_snapshot`): `equity` is `account.equity`, `cash` is `account.cash` (negative cash means
//!   margin is in use and reconciliation halts), holdings are `GET /v2/positions` with the broker's own signed
//!   `market_value`. A blocked or inactive account is refused outright.
//!
//! In both cases `derived_equity` = `cash + sum(holdings market value)` is OUR arithmetic over the broker's own
//! numbers, kept only as a cross-check of the broker's equity figure (reads happen at slightly different instants,
//! so reconciliation allows a percentage tolerance). The risk overlay never uses it: risk uses the broker's `equity`.

use std::collections::BTreeMap;

use broker_adapters::alpaca::{AccountInfo, PositionInfo};
use broker_adapters::kraken::parse::TradeBalance;
use broker_adapters::{Balances, BalanceKind, Dec, OrderReport};
use chrono::{DateTime, Utc};
use rebalancer_core::dec_math::{add, mul};
use rebalancer_core::guard::{AccountView, Position};
use rebalancer_risk::overlay::EquitySnapshot;

/// One held instrument as the broker reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holding {
    /// Tradable symbol on the venue (`BTC/USD`, `SPY`).
    pub symbol: String,
    pub asset_class: String,
    /// Signed quantity (negative = short).
    pub quantity: Dec,
    /// The price the value was computed at (`None` when the broker supplied the value directly).
    pub mark: Option<Dec>,
    /// Signed value in the account currency.
    pub market_value: Dec,
}

/// A non-zero balance that could not be priced (no pair, no quote).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnvaluedHolding {
    pub asset: String,
    pub quantity: Dec,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrokerSnapshot {
    pub venue: String,
    pub ccy: String,
    /// Broker-reported equity (see the module docs for the per-venue definition).
    pub equity: Dec,
    /// Broker-reported free cash.
    pub cash: Dec,
    pub holdings: Vec<Holding>,
    pub unvalued: Vec<UnvaluedHolding>,
    pub open_orders: Vec<OrderReport>,
    /// Earn / staked balances that were excluded (Kraken).
    pub excluded_balances: Vec<UnvaluedHolding>,
    /// When WE read the account (the pipeline clock), for staleness checks.
    pub taken_at: DateTime<Utc>,
    /// `cash + sum(holding value)`: our cross-check of `equity`, never used for risk.
    pub derived_equity: Dec,
}

impl BrokerSnapshot {
    pub fn holding(&self, symbol: &str) -> Option<&Holding> {
        self.holdings.iter().find(|h| h.symbol.eq_ignore_ascii_case(symbol))
    }

    pub fn quantity_of(&self, symbol: &str) -> Dec {
        self.holding(symbol).map_or(Dec::ZERO, |h| h.quantity)
    }

    /// The broker-reported equity as the ONLY input the risk overlay accepts.
    pub fn equity_snapshot(&self) -> EquitySnapshot {
        EquitySnapshot::broker_reported(&self.venue, &self.ccy, self.equity, self.taken_at)
    }

    /// The account as the pre-trade guard and planner see it.
    pub fn account_view(&self, account_id: &str, halted: bool, now: DateTime<Utc>) -> AccountView {
        AccountView {
            account_id: account_id.to_string(),
            ccy: self.ccy.clone(),
            equity: self.equity,
            cash: self.cash,
            positions: self
                .holdings
                .iter()
                .map(|h| Position {
                    symbol: h.symbol.clone(),
                    venue: self.venue.clone(),
                    asset_class: h.asset_class.clone(),
                    quantity: h.quantity,
                    market_value: h.market_value,
                })
                .collect(),
            halted,
            now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ViewError {
    #[error("VIEW_EQUITY_MISSING: the broker's TradeBalance carries neither equity (e) nor equivalent balance (eb)")]
    EquityMissing,
    #[error("VIEW_ACCOUNT_BLOCKED: {0}")]
    AccountBlocked(String),
    #[error("VIEW_ARITHMETIC_OVERFLOW: {0}")]
    Overflow(String),
}

impl ViewError {
    pub fn code(&self) -> &'static str {
        match self {
            ViewError::EquityMissing => "VIEW_EQUITY_MISSING",
            ViewError::AccountBlocked(_) => "VIEW_ACCOUNT_BLOCKED",
            ViewError::Overflow(_) => "VIEW_ARITHMETIC_OVERFLOW",
        }
    }
}

fn overflow(what: &str) -> ViewError {
    ViewError::Overflow(what.to_string())
}

/// Inputs of the Kraken builder: the three raw reads plus the marks of the assets held.
pub struct KrakenViewInput<'a> {
    pub balances: &'a Balances,
    pub trade_balance: &'a TradeBalance,
    pub open_orders: Vec<OrderReport>,
    /// Last price by canonical pair (`BTC/USD`), for every held asset that has a pair.
    pub marks: &'a BTreeMap<String, Dec>,
    /// Account / quote currency, canonical ticker (`USD`).
    pub quote_asset: &'a str,
    /// Asset class stamped on every holding (`crypto_spot`).
    pub asset_class: &'a str,
}

/// Build a snapshot from Kraken's `Balance`, `TradeBalance` and `OpenOrders` (see the module docs for the equity
/// definition).
pub fn kraken_snapshot(input: &KrakenViewInput<'_>, now: DateTime<Utc>) -> Result<BrokerSnapshot, ViewError> {
    let quote = input.quote_asset.trim().to_uppercase();
    let equity = input
        .trade_balance
        .equity
        .or(input.trade_balance.equivalent_balance)
        .ok_or(ViewError::EquityMissing)?;
    let cash = input.balances.spot(&quote);
    let mut holdings = Vec::new();
    let mut unvalued = Vec::new();
    let mut excluded = Vec::new();
    // Sum duplicate spot rows of one asset (e.g. `XBT` and `XBT.M` are distinct kinds, but two Spot rows would add).
    let mut spot: BTreeMap<String, Dec> = BTreeMap::new();
    for e in &input.balances.entries {
        if e.amount.is_zero() {
            continue;
        }
        match e.kind {
            BalanceKind::Earn => excluded.push(UnvaluedHolding {
                asset: e.asset.clone(),
                quantity: e.amount,
                reason: format!("earn/staked balance {} is not tradable", e.raw_asset),
            }),
            BalanceKind::Spot if e.asset.eq_ignore_ascii_case(&quote) => {}
            BalanceKind::Spot => {
                let entry = spot.entry(e.asset.to_uppercase()).or_insert(Dec::ZERO);
                *entry = add(*entry, e.amount).map_err(|_| overflow("summing spot balances"))?;
            }
        }
    }
    let mut derived = cash;
    for (asset, qty) in spot {
        let symbol = format!("{asset}/{quote}");
        match input.marks.get(&symbol) {
            Some(mark) => {
                let value = mul(qty, *mark).map_err(|_| overflow("valuing a holding"))?;
                derived = add(derived, value).map_err(|_| overflow("summing holdings"))?;
                holdings.push(Holding {
                    symbol,
                    asset_class: input.asset_class.to_string(),
                    quantity: qty,
                    mark: Some(*mark),
                    market_value: value,
                });
            }
            None => unvalued.push(UnvaluedHolding { asset, quantity: qty, reason: format!("no price for {symbol}") }),
        }
    }
    Ok(BrokerSnapshot {
        venue: "kraken".to_string(),
        ccy: quote,
        equity,
        cash,
        holdings,
        unvalued,
        open_orders: input.open_orders.clone(),
        excluded_balances: excluded,
        taken_at: now,
        derived_equity: derived,
    })
}

/// Build a snapshot from Alpaca's account, positions and open orders. `asset_class_of` maps a symbol to the
/// mandate's asset-class name (`us_etf`).
pub fn alpaca_snapshot(
    account: &AccountInfo,
    positions: &[PositionInfo],
    open_orders: Vec<OrderReport>,
    asset_class_of: &dyn Fn(&str) -> String,
    now: DateTime<Utc>,
) -> Result<BrokerSnapshot, ViewError> {
    if let Some(why) = account.blocked_reason() {
        return Err(ViewError::AccountBlocked(why));
    }
    let ccy = account.currency.clone().unwrap_or_else(|| "USD".to_string()).to_uppercase();
    let mut holdings = Vec::new();
    let mut derived = account.cash;
    for p in positions {
        derived = add(derived, p.market_value).map_err(|_| overflow("summing positions"))?;
        holdings.push(Holding {
            symbol: p.symbol.to_uppercase(),
            asset_class: asset_class_of(&p.symbol),
            quantity: p.signed_qty(),
            mark: p.current_price,
            market_value: p.market_value,
        });
    }
    holdings.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    Ok(BrokerSnapshot {
        venue: "alpaca".to_string(),
        ccy,
        equity: account.equity,
        cash: account.cash,
        holdings,
        unvalued: Vec::new(),
        open_orders,
        excluded_balances: Vec::new(),
        taken_at: now,
        derived_equity: derived,
    })
}
