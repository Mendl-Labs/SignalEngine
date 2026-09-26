//! `SimBroker`: an in-memory, stateful `Broker` for the whole-pipeline replay. Every market order is filled COMPLETELY
//! and IMMEDIATELY at the broker's current price (set by the test before each run, the same number the run sizes on),
//! with ZERO fees: the "fake broker filling at the close" of design 5.4 test 4. An optional adverse fill offset in bps
//! (buys pay more, sells receive less) lets a test measure what a departure from same-close fills does.
//!
//! The broker reports its own equity as `cash + SUM(quantity * price)` (spot-account definition), so the pipeline's
//! risk overlay, reconciliation and planner all see a consistent account.

use std::collections::BTreeMap;
use std::sync::Mutex;

use broker_adapters::{
    BrokerError, CancelOutcome, Dec, OrderKind, OrderReport, OrderRequest, OrderStatus, PlaceOutcome, Quote, SentOrder, Side,
};
use chrono::{DateTime, Utc};
use rebalancer_core::dec_math::{add, mul, sub};
use rebalancer_run::broker::{Broker, SnapshotError};
use rebalancer_run::view::{BrokerSnapshot, Holding};

use super::d;

struct St {
    cash: Dec,
    hold: BTreeMap<String, Dec>,
    price: BTreeMap<String, Dec>,
    class_of: Box<dyn Fn(&str) -> String + Send>,
    orders: Vec<OrderReport>,
    /// Every fill in the order it happened: `(symbol, side, quantity)`.
    fills: Vec<(String, Side, Dec)>,
    seq: u32,
    /// Adverse fill offset in bps (0 = fill at the reference price).
    slippage_bps: i64,
}

pub struct SimBroker {
    st: Mutex<St>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl SimBroker {
    pub fn new(cash: Dec, class_of: impl Fn(&str) -> String + Send + 'static) -> Self {
        Self {
            st: Mutex::new(St {
                cash,
                hold: BTreeMap::new(),
                price: BTreeMap::new(),
                class_of: Box::new(class_of),
                orders: Vec::new(),
                fills: Vec::new(),
                seq: 0,
                slippage_bps: 0,
            }),
        }
    }

    pub fn set_prices(&self, prices: impl IntoIterator<Item = (String, Dec)>) {
        let mut s = lock(&self.st);
        for (k, v) in prices {
            s.price.insert(k.to_uppercase(), v);
        }
    }

    pub fn set_slippage_bps(&self, bps: i64) {
        lock(&self.st).slippage_bps = bps;
    }

    pub fn cash(&self) -> Dec {
        lock(&self.st).cash
    }

    pub fn holdings(&self) -> BTreeMap<String, Dec> {
        lock(&self.st).hold.iter().filter(|(_, q)| !q.is_zero()).map(|(k, v)| (k.clone(), *v)).collect()
    }

    pub fn quantity(&self, symbol: &str) -> Dec {
        lock(&self.st).hold.get(&symbol.to_uppercase()).copied().unwrap_or(Dec::ZERO)
    }

    /// `cash + SUM(quantity * price)` at the current prices.
    pub fn equity(&self) -> Dec {
        let s = lock(&self.st);
        equity_of(&s)
    }

    pub fn fill_count(&self) -> usize {
        lock(&self.st).fills.len()
    }

    /// Fills since fill number `from` (an index taken from `fill_count`).
    pub fn fills_since(&self, from: usize) -> Vec<(String, Side, Dec)> {
        lock(&self.st).fills[from..].to_vec()
    }
}

fn equity_of(s: &St) -> Dec {
    let mut e = s.cash;
    for (sym, q) in &s.hold {
        if let Some(p) = s.price.get(sym) {
            e = add(e, mul(*q, *p).expect("no overflow")).expect("no overflow");
        }
    }
    e
}

impl Broker for SimBroker {
    fn venue(&self) -> &'static str {
        "alpaca"
    }

    fn snapshot(&self, now: DateTime<Utc>) -> Result<BrokerSnapshot, SnapshotError> {
        let s = lock(&self.st);
        let mut holdings = Vec::new();
        for (sym, q) in &s.hold {
            if q.is_zero() {
                continue;
            }
            let p = *s.price.get(sym).ok_or_else(|| BrokerError::UnknownSymbol(sym.clone()))?;
            holdings.push(Holding {
                symbol: sym.clone(),
                asset_class: (s.class_of)(sym),
                quantity: *q,
                mark: Some(p),
                market_value: mul(*q, p).map_err(|_| BrokerError::Malformed("overflow".into()))?,
            });
        }
        let equity = equity_of(&s);
        Ok(BrokerSnapshot {
            venue: "alpaca".into(),
            ccy: "USD".into(),
            equity,
            cash: s.cash,
            holdings,
            unvalued: Vec::new(),
            open_orders: Vec::new(),
            excluded_balances: Vec::new(),
            taken_at: now,
            derived_equity: equity,
        })
    }

    fn place(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        let mut s = lock(&self.st);
        let sent = SentOrder {
            broker_pair: req.symbol.clone(),
            side: req.side,
            quantity: req.quantity,
            price: req.reference_price,
            userref: 0,
            validate_only: req.validate_only,
        };
        if req.validate_only {
            return Ok(PlaceOutcome::ValidatedOnly { description: None, sent, warnings: Vec::new() });
        }
        if !matches!(req.kind, OrderKind::Market) {
            return Err(BrokerError::Unsupported("SimBroker fills market orders only".into()));
        }
        let key = req.symbol.to_uppercase();
        let base = *s.price.get(&key).ok_or_else(|| BrokerError::UnknownSymbol(key.clone()))?;
        // adverse offset: buys pay more, sells receive less
        let bps = if req.side == Side::Buy { s.slippage_bps } else { -s.slippage_bps };
        let px = if bps == 0 {
            base
        } else {
            let f = add(d("1"), mul(d("0.0001"), Dec::from_i64(bps)).expect("no overflow")).expect("no overflow");
            mul(base, f).expect("no overflow")
        };
        let notional = mul(req.quantity, px).map_err(|_| BrokerError::Malformed("overflow".into()))?;
        let held = s.hold.get(&key).copied().unwrap_or(Dec::ZERO);
        match req.side {
            Side::Buy => {
                s.cash = sub(s.cash, notional).map_err(|_| BrokerError::Malformed("overflow".into()))?;
                s.hold.insert(key.clone(), add(held, req.quantity).expect("no overflow"));
            }
            Side::Sell => {
                s.cash = add(s.cash, notional).map_err(|_| BrokerError::Malformed("overflow".into()))?;
                s.hold.insert(key.clone(), sub(held, req.quantity).expect("no overflow"));
            }
        }
        s.seq += 1;
        let id = format!("SIM-{:06}", s.seq);
        s.fills.push((key.clone(), req.side, req.quantity));
        s.orders.push(OrderReport {
            broker_order_id: id.clone(),
            userref: None,
            tag: Some(req.tag.clone()),
            symbol: key,
            side: Some(req.side),
            kind: Some(OrderKind::Market),
            status: OrderStatus::Filled,
            raw_status: "filled".into(),
            reason: None,
            quantity: req.quantity,
            executed_quantity: req.quantity,
            avg_price: Some(px),
            cost: Some(notional),
            fee: Some(Dec::ZERO),
            open_time: None,
            close_time: None,
        });
        Ok(PlaceOutcome::Accepted { broker_order_id: id, description: None, sent, warnings: Vec::new() })
    }

    fn get_order(&self, id: &str) -> Result<OrderReport, BrokerError> {
        lock(&self.st)
            .orders
            .iter()
            .find(|o| o.broker_order_id == id)
            .cloned()
            .ok_or_else(|| BrokerError::UnknownSymbol(format!("no order {id}")))
    }

    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        Ok(Vec::new())
    }

    fn find_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        Ok(lock(&self.st).orders.iter().filter(|o| o.tag.as_deref() == Some(tag)).cloned().collect())
    }

    fn cancel_and_settle(&self, id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        Ok((CancelOutcome { canceled_count: 0, pending: false }, self.get_order(id)?))
    }

    fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        let s = lock(&self.st);
        let p = *s.price.get(&symbol.to_uppercase()).ok_or_else(|| BrokerError::UnknownSymbol(symbol.to_string()))?;
        Ok(Quote { symbol: symbol.to_string(), bid: p, ask: p, last: p })
    }
}
