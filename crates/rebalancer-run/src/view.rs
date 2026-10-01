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
//! * **OANDA** (`oanda_snapshot`, NEVER exercised against a real OANDA account): a MARGIN account, so the spot
//!   equation `equity = cash + sum(holdings)` does not hold and is not faked. `equity` is the broker-reported `NAV`;
//!   `cash` is the broker-reported realised `balance` (NOT free cash: margin is a separate figure, carried in
//!   [`MarginInfo`] next to the snapshot in [`OandaSnapshot`], never invented); holdings are the open positions
//!   with SIGNED NET units (long positive, short negative, `long.units + short.units`). A holding's `market_value`
//!   is the signed NOTIONAL in the account currency, our arithmetic over broker numbers: `units * mid * factor`,
//!   where `mid` is the mid of the instrument's `pricing` bid/ask (a closed market has no liquidity: then the mid of
//!   the broker's own `closeoutBid`/`closeoutAsk`, so positions stay visible over a weekend) and `factor` converts the instrument's quote
//!   currency into the account currency (1 when they are the same, otherwise the `positionValue` factor of the
//!   pricing response's `homeConversions`). No price or no conversion means the position is listed in `unvalued`
//!   (reconciliation halts on it), never valued at a guess. `derived_equity` is `balance + sum(position
//!   unrealizedPL)`, all broker numbers (OANDA documents `NAV = balance + unrealizedPL`), so it cross-checks the NAV
//!   and does NOT include notional. A hedging account, or a position with both long and short units, is refused.
//!   Consumers built for spot (reconciliation's expected-cash model, the guard's cash test, the planner) still
//!   assume that a trade moves cash by its cost; on a margin account it does not, so they are NOT yet correct for
//!   FX: that is the leverage/signed-weight work (B3), not done here.
//!
//! In every case `derived_equity` is OUR arithmetic over the broker's own numbers, kept only as a cross-check of the
//! broker's equity figure (reads happen at slightly different instants, so reconciliation allows a percentage
//! tolerance). The risk overlay never uses it: risk uses the broker's `equity`.

use std::collections::BTreeMap;

use broker_adapters::alpaca::{AccountInfo, PositionInfo};
use broker_adapters::kraken::parse::TradeBalance;
use broker_adapters::oanda::instrument::{canonical_symbol, quote_currency};
use broker_adapters::oanda::{AccountSummary, OandaPosition, Pricing};
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

/// The margin figures of an OANDA account exactly as the broker reported them (nothing derived). Kept beside the
/// [`BrokerSnapshot`] because the snapshot has no place for margin and the venue-neutral consumers do not use it yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarginInfo {
    /// Account (home) currency, upper-case.
    pub currency: String,
    /// Net asset value.
    pub nav: Dec,
    /// Realised balance.
    pub balance: Dec,
    /// Unrealised P&L of the open positions, account currency.
    pub unrealized_pl: Dec,
    pub margin_used: Dec,
    pub margin_available: Dec,
    /// Total position value the broker reports (`positionValue`), when present.
    pub position_value: Option<Dec>,
    /// `marginCloseoutPercent` (a fraction: 0.5 means closeout at 50 percent margin used), when present.
    pub margin_closeout_percent: Option<Dec>,
    pub margin_rate: Option<Dec>,
    pub open_position_count: Option<u32>,
    pub pending_order_count: Option<u32>,
}

/// An OANDA reading: the venue-neutral snapshot plus the broker's margin figures.
#[derive(Debug, Clone, PartialEq)]
pub struct OandaSnapshot {
    pub snapshot: BrokerSnapshot,
    pub margin: MarginInfo,
}

/// Inputs of the OANDA builder: three raw reads plus pricing for the instruments held.
pub struct OandaViewInput<'a> {
    pub account: &'a AccountSummary,
    pub positions: &'a [OandaPosition],
    pub open_orders: Vec<OrderReport>,
    /// Prices (and home conversions) for every held instrument. Missing entries make that holding `unvalued`.
    pub pricing: &'a Pricing,
    /// Asset class stamped on every holding (`fx_spot`).
    pub asset_class: &'a str,
}

/// Build a snapshot from OANDA's account summary, open positions, pending orders and pricing (see the module docs for
/// the definitions). Refuses a hedging account and any hedged position; signed net units keep the direction.
pub fn oanda_snapshot(input: &OandaViewInput<'_>, now: DateTime<Utc>) -> Result<OandaSnapshot, ViewError> {
    let a = input.account;
    if a.hedging_enabled {
        return Err(ViewError::AccountBlocked("hedgingEnabled=true: only netting accounts are supported".into()));
    }
    let ccy = a.currency.trim().to_uppercase();
    let mut holdings = Vec::new();
    let mut unvalued = Vec::new();
    let mut derived = a.balance;
    for p in input.positions {
        if p.is_hedged() {
            return Err(ViewError::AccountBlocked(format!("{}: long and short units are both open (hedged position)", p.instrument)));
        }
        let net = p.net_units();
        if net.is_zero() {
            continue;
        }
        derived = add(derived, p.unrealized_pl).map_err(|_| overflow("summing position P&L"))?;
        let symbol = canonical_symbol(&p.instrument).map_err(|e| ViewError::AccountBlocked(format!("{}: {e}", p.instrument)))?;
        let mark = input.pricing.price(&p.instrument).and_then(|q| q.valuation_mid());
        let factor = match quote_currency(&p.instrument) {
            Some(q) if q.eq_ignore_ascii_case(&ccy) => Some(Dec::from_i64(1)),
            Some(q) => input.pricing.conversion(q).map(|c| c.position_value),
            None => None,
        };
        match (mark, factor) {
            (Some(mark), Some(factor)) => {
                let notional = mul(net, mark).and_then(|v| mul(v, factor)).map_err(|_| overflow("valuing a position"))?;
                holdings.push(Holding { symbol, asset_class: input.asset_class.to_string(), quantity: net, mark: Some(mark), market_value: notional });
            }
            (None, _) => unvalued.push(UnvaluedHolding {
                asset: symbol.clone(),
                quantity: net,
                reason: format!("no usable price for {symbol} (missing, one-sided, crossed)"),
            }),
            (_, None) => unvalued.push(UnvaluedHolding {
                asset: symbol.clone(),
                quantity: net,
                reason: format!("no conversion from the quote currency of {symbol} to {ccy}"),
            }),
        }
    }
    holdings.sort_by(|x, y| x.symbol.cmp(&y.symbol));
    unvalued.sort_by(|x, y| x.asset.cmp(&y.asset));
    let snapshot = BrokerSnapshot {
        venue: "oanda".to_string(),
        ccy: ccy.clone(),
        equity: a.nav,
        cash: a.balance,
        holdings,
        unvalued,
        open_orders: input.open_orders.clone(),
        excluded_balances: Vec::new(),
        taken_at: now,
        derived_equity: derived,
    };
    let margin = MarginInfo {
        currency: ccy,
        nav: a.nav,
        balance: a.balance,
        unrealized_pl: a.unrealized_pl,
        margin_used: a.margin_used,
        margin_available: a.margin_available,
        position_value: a.position_value,
        margin_closeout_percent: a.margin_closeout_percent,
        margin_rate: a.margin_rate,
        open_position_count: a.open_position_count,
        pending_order_count: a.pending_order_count,
    };
    Ok(OandaSnapshot { snapshot, margin })
}
