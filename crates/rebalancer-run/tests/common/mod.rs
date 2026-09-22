//! Shared fixtures for the rebalancer-run integration tests: a fake Kraken exchange with a real adapter, broker
//! wrappers that crash or lie on demand, and small builders.
#![allow(dead_code)]

pub mod harness;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};

use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::{
    BrokerError, CancelOutcome, Dec, OrderReport, OrderRequest, PlaceOutcome, Quote, Side,
};
use chrono::{DateTime, Utc};
use fake_broker::testkit::KrakenRig;
use rebalancer_core::venue::{KrakenRules, VenueRuleBook};
use rebalancer_run::broker::{Broker, KrakenBroker, SnapshotError};
use rebalancer_run::clock::ManualClock;
use rebalancer_run::view::BrokerSnapshot;

pub fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

pub fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

pub fn t0() -> DateTime<Utc> {
    at("2026-09-21T15:00:00Z")
}

pub const ACCOUNT: &str = "main";

/// The fake exchange, a real `KrakenAdapter` talking to it, a manual pipeline clock and the adapter's own pair table.
pub struct Env {
    pub rig: KrakenRig,
    pub clock: ManualClock,
    pub pairs: PairTable,
}

impl Env {
    /// Standard exchange: BTC/USD 60000, ETH/USD 3000, account `main` funded with `usd` USD and nothing else.
    pub fn with_usd(usd: &str) -> Env {
        let rig = KrakenRig::new();
        rig.handle.set_balance(ACCOUNT, "USD", usd);
        Env { rig, clock: ManualClock::new(t0()), pairs: PairTable::builtin() }
    }

    pub fn new() -> Env {
        Env::with_usd("5000")
    }

    pub fn broker(&self) -> KrakenBroker<'_> {
        KrakenBroker::usd(&self.rig.adapter)
    }

    /// Give the account `qty` of `asset` without any trade (an external deposit).
    pub fn hold(&self, asset: &str, qty: &str) {
        self.rig.handle.adjust_balance(ACCOUNT, asset, qty);
    }

    pub fn bal(&self, asset: &str) -> Dec {
        self.rig.handle.balance(ACCOUNT, asset)
    }

    pub fn snapshot(&self) -> BrokerSnapshot {
        self.broker().snapshot(self.clock_now()).expect("snapshot")
    }

    pub fn clock_now(&self) -> DateTime<Utc> {
        use rebalancer_run::clock::Clock;
        self.clock.now()
    }

    /// Requests that reached the exchange and could change orders (validate-only AddOrders excluded).
    pub fn order_requests(&self) -> usize {
        fake_broker::scenarios::order_affecting_requests(&self.rig.handle)
    }

    /// Number of AddOrder requests that were APPLIED (created an order), by fill count of sells.
    pub fn sell_fills(&self, pair: &str) -> Vec<broker_adapters::Dec> {
        let mut out = Vec::new();
        for o in self.rig.handle.orders(ACCOUNT) {
            if o.pair == pair && o.side == Side::Sell && !o.foreign {
                out.push(o.vol_exec());
            }
        }
        out
    }
}

impl Default for Env {
    fn default() -> Self {
        Self::new()
    }
}

/// A resting limit order of ours (placed through the real adapter) that stays open. Returns its broker id.
pub fn rest_limit_order(env: &Env, tag: &str, side: Side, pair: &str, qty: &str, price: &str) -> String {
    use broker_adapters::BrokerAdapter;
    match env.rig.adapter.place_order(&OrderRequest::limit(tag, pair, side, d(qty), d(price))).unwrap() {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    }
}

/// A caller as the authenticated-human endpoint would see it.
pub struct Person(pub &'static str, pub rebalancer_risk::approval::PrincipalKind);

impl rebalancer_risk::approval::AuthenticatedPrincipal for Person {
    fn subject(&self) -> &str {
        self.0
    }
    fn kind(&self) -> rebalancer_risk::approval::PrincipalKind {
        self.1
    }
}

pub fn universe(symbols: &[&str]) -> BTreeSet<String> {
    symbols.iter().map(|s| s.to_uppercase()).collect()
}

pub fn no_ids() -> BTreeSet<String> {
    BTreeSet::new()
}

/// Rule book with the adapter's own Kraken pair table (the same rows the planner uses).
pub fn kraken_rules(pairs: &PairTable) -> KrakenRules<'_> {
    KrakenRules { pairs }
}

pub fn book<'a>(rules: &'a KrakenRules<'a>) -> VenueRuleBook<'a> {
    VenueRuleBook::new().with("kraken", rules)
}

// ---------------------------------------------------------------------------------------------------------------
// Crashing broker: dies (panics) before or after the N-th call, to model a process killed at any point.
// ---------------------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashAt {
    /// Panic when call number `n` (1-based) starts: the call never reaches the exchange.
    Before(u32),
    /// Panic when call number `n` has finished: the exchange applied it, the caller never saw the answer.
    After(u32),
}

pub struct CrashingBroker<'a> {
    pub inner: &'a dyn Broker,
    pub crash: Option<CrashAt>,
    pub calls: AtomicU32,
}

impl<'a> CrashingBroker<'a> {
    pub fn new(inner: &'a dyn Broker, crash: Option<CrashAt>) -> Self {
        Self { inner, crash, calls: AtomicU32::new(0) }
    }

    pub fn call_count(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn guard<T>(&self, f: impl FnOnce() -> T) -> T {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.crash == Some(CrashAt::Before(n)) {
            panic!("simulated crash before call {n}");
        }
        let out = f();
        if self.crash == Some(CrashAt::After(n)) {
            panic!("simulated crash after call {n}");
        }
        out
    }
}

impl Broker for CrashingBroker<'_> {
    fn venue(&self) -> &'static str {
        self.inner.venue()
    }
    fn snapshot(&self, now: DateTime<Utc>) -> Result<BrokerSnapshot, SnapshotError> {
        self.guard(|| self.inner.snapshot(now))
    }
    fn place(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        self.guard(|| self.inner.place(req))
    }
    fn get_order(&self, id: &str) -> Result<OrderReport, BrokerError> {
        self.guard(|| self.inner.get_order(id))
    }
    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.guard(|| self.inner.open_orders())
    }
    fn find_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        self.guard(|| self.inner.find_by_tag(tag))
    }
    fn cancel_and_settle(&self, id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        self.guard(|| self.inner.cancel_and_settle(id))
    }
    fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.guard(|| self.inner.quote(symbol))
    }
}

/// Wraps a broker and rewrites every snapshot (to inject a short position, or to lie about equity).
pub struct DistortingBroker<'a> {
    pub inner: &'a dyn Broker,
    pub distort: Box<dyn Fn(&mut BrokerSnapshot) + 'a>,
}

impl Broker for DistortingBroker<'_> {
    fn venue(&self) -> &'static str {
        self.inner.venue()
    }
    fn snapshot(&self, now: DateTime<Utc>) -> Result<BrokerSnapshot, SnapshotError> {
        let mut s = self.inner.snapshot(now)?;
        (self.distort)(&mut s);
        Ok(s)
    }
    fn place(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        self.inner.place(req)
    }
    fn get_order(&self, id: &str) -> Result<OrderReport, BrokerError> {
        self.inner.get_order(id)
    }
    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.inner.open_orders()
    }
    fn find_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        self.inner.find_by_tag(tag)
    }
    fn cancel_and_settle(&self, id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        self.inner.cancel_and_settle(id)
    }
    fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.inner.quote(symbol)
    }
}
