//! The wire-neutral exchange core: accounts, balances, a simplified book (one controllable price
//! per pair), orders, fills, fees and cancel semantics. Nothing here knows about HTTP, nonces or
//! signatures; a wire front end (see [`crate::kraken`]) translates requests into these calls and
//! renders the results. An Alpaca front end would sit beside it and reuse everything here.
//!
//! Invariants (checked by [`Core::check_invariants`] and by the property tests):
//! * a balance never goes negative;
//! * balance = external ledger (deposits, withdrawals, drift) + the effect of every fill;
//! * an order's executed volume never exceeds its volume, and a `Closed` order is fully filled;
//! * order cost = the sum of `qty * price` over its fills.

pub mod policy;

use crate::money::{add, div_floor, mul, sub, sum};
use crate::rng::kraken_style_id;
use broker_adapters::{Dec, OrderKind, Side};
use policy::{FillPolicy, FillStep, OrderRule, StepAmount};
use std::collections::{BTreeMap, VecDeque};

// ---------------------------------------------------------------- pairs and market

/// Static description of a tradable pair. Rows are independent of the adapter's own pair table
/// on purpose: the tests then prove the two agree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairSpec {
    /// Canonical `BASE/QUOTE` with ticker names (`BTC/USD`).
    pub canonical: String,
    /// Kraken altname (`XBTUSD`), the name orders are sent with.
    pub altname: String,
    /// Kraken REST result key (`XXBTZUSD`).
    pub rest_name: String,
    /// Kraken websocket name (`XBT/USD`).
    pub ws_name: String,
    pub pair_decimals: u32,
    pub lot_decimals: u32,
    pub order_min: Dec,
    pub cost_min: Dec,
    pub tick_size: Dec,
    /// `online`, `cancel_only`, `post_only`, `limit_only`, `reduce_only`.
    pub status: String,
}

impl PairSpec {
    pub fn new(canonical: &str, altname: &str, rest_name: &str, ws_name: &str) -> Self {
        Self {
            canonical: canonical.to_string(),
            altname: altname.to_string(),
            rest_name: rest_name.to_string(),
            ws_name: ws_name.to_string(),
            pair_decimals: 2,
            lot_decimals: 8,
            order_min: crate::money::dec("0.0001"),
            cost_min: crate::money::dec("0.5"),
            tick_size: crate::money::dec("0.01"),
            status: "online".to_string(),
        }
    }

    pub fn btc_usd() -> Self {
        let mut p = Self::new("BTC/USD", "XBTUSD", "XXBTZUSD", "XBT/USD");
        p.pair_decimals = 1;
        p.tick_size = crate::money::dec("0.1");
        p
    }

    pub fn eth_usd() -> Self {
        let mut p = Self::new("ETH/USD", "ETHUSD", "XETHZUSD", "ETH/USD");
        p.order_min = crate::money::dec("0.002");
        p
    }

    pub fn base(&self) -> &str {
        self.canonical.split_once('/').map(|(b, _)| b).unwrap_or(&self.canonical)
    }

    pub fn quote(&self) -> &str {
        self.canonical.split_once('/').map(|(_, q)| q).unwrap_or("")
    }

    /// Kraken accepts the altname, the REST name and the websocket name (not the canonical
    /// ticker form, whose `BTC` differs from Kraken's `XBT`).
    pub(crate) fn matches_wire_name(&self, name: &str) -> bool {
        let n = name.trim();
        self.altname.eq_ignore_ascii_case(n) || self.rest_name.eq_ignore_ascii_case(n) || self.ws_name.eq_ignore_ascii_case(n)
    }
}

/// The simplified book of one pair: a last price and a touch (best bid / best ask).
#[derive(Debug, Clone)]
pub struct PairMarket {
    pub spec: PairSpec,
    pub last: Dec,
    pub bid: Dec,
    pub ask: Dec,
    /// Distance between bid and ask that `set_price` applies (`bid = last`, `ask = last + spread`).
    pub spread: Dec,
}

// ---------------------------------------------------------------- fees

/// Fee rates as fractions of cost (0.0026 = 0.26 percent). Charged in the quote currency
/// (Kraken's default `fciq` flag).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeSchedule {
    pub maker: Dec,
    pub taker: Dec,
}

impl Default for FeeSchedule {
    /// Kraken's entry-level spot tier.
    fn default() -> Self {
        Self { maker: crate::money::dec("0.0016"), taker: crate::money::dec("0.0026") }
    }
}

// ---------------------------------------------------------------- orders and fills

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liquidity {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    Open,
    /// Fully filled.
    Closed,
    Canceled,
    Expired,
}

impl OrderStatus {
    /// Kraken's status string.
    pub fn as_kraken(self) -> &'static str {
        match self {
            OrderStatus::Pending => "pending",
            OrderStatus::Open => "open",
            OrderStatus::Closed => "closed",
            OrderStatus::Canceled => "canceled",
            OrderStatus::Expired => "expired",
        }
    }
    /// Still able to execute.
    pub fn is_live(self) -> bool {
        matches!(self, OrderStatus::Pending | OrderStatus::Open)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tif {
    Gtc,
    Ioc,
}

/// Distorts how a fill shows up in order reports while leaving balances and the true fill
/// history untouched. This is how "drop or duplicate a fill report" is modelled on a REST-only
/// venue: the report (`vol_exec`, `cost`, `fee` of the order) disagrees with the balances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportGlitch {
    Normal,
    /// The fill is missing from order reports.
    Dropped,
    /// The fill is counted twice in order reports.
    Duplicated,
}

impl ReportGlitch {
    fn multiplicity(self) -> i64 {
        match self {
            ReportGlitch::Normal => 1,
            ReportGlitch::Dropped => 0,
            ReportGlitch::Duplicated => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fill {
    pub id: String,
    pub order: String,
    pub qty: Dec,
    pub price: Dec,
    pub cost: Dec,
    pub fee: Dec,
    pub time_nanos: u64,
    pub liquidity: Liquidity,
    pub report: ReportGlitch,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub txid: String,
    pub account: String,
    /// Creation sequence, used for stable ordering.
    pub seq: u64,
    /// Canonical pair, `BTC/USD`.
    pub pair: String,
    pub side: Side,
    pub kind: OrderKind,
    pub volume: Dec,
    pub userref: Option<i32>,
    pub tif: Tif,
    pub post_only: bool,
    pub status: OrderStatus,
    pub reason: Option<String>,
    pub open_time_nanos: u64,
    pub close_time_nanos: Option<u64>,
    pub fills: Vec<Fill>,
    /// Placed through `add_foreign_order` (someone else's order on this account).
    pub foreign: bool,
    /// A cancel was accepted but has not been processed yet (deferred-cancel mode).
    pub cancel_requested: bool,
    pub(crate) policy: FillPolicy,
    pub(crate) queue: VecDeque<FillStep>,
    /// Price used to size the funds reservation of this order.
    pub(crate) reserve_price: Dec,
}

impl Order {
    /// True executed volume (the sum of all fills).
    pub fn vol_exec(&self) -> Dec {
        sum(self.fills.iter().map(|f| f.qty))
    }
    pub fn remaining(&self) -> Dec {
        sub(self.volume, self.vol_exec())
    }
    /// True executed cost in the quote currency.
    pub fn cost(&self) -> Dec {
        sum(self.fills.iter().map(|f| f.cost))
    }
    pub fn fee(&self) -> Dec {
        sum(self.fills.iter().map(|f| f.fee))
    }
    pub fn avg_price(&self) -> Option<Dec> {
        let v = self.vol_exec();
        v.is_positive().then(|| div_floor(self.cost(), v, 12).normalized())
    }

    fn reported<F: Fn(&Fill) -> Dec>(&self, pick: F) -> Dec {
        sum(self.fills.iter().map(|f| mul(pick(f), Dec::from_i64(f.report.multiplicity()))))
    }
    /// Executed volume as order reports show it (after any [`ReportGlitch`]).
    pub fn reported_vol_exec(&self) -> Dec {
        self.reported(|f| f.qty)
    }
    pub fn reported_cost(&self) -> Dec {
        self.reported(|f| f.cost)
    }
    pub fn reported_fee(&self) -> Dec {
        self.reported(|f| f.fee)
    }
    pub fn reported_avg_price(&self) -> Option<Dec> {
        let v = self.reported_vol_exec();
        v.is_positive().then(|| div_floor(self.reported_cost(), v, 12).normalized())
    }
}

/// A request to place an order, in wire-neutral terms. `pair` is the canonical name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOrder {
    pub account: String,
    pub pair: String,
    pub side: Side,
    pub kind: OrderKind,
    pub volume: Dec,
    pub userref: Option<i32>,
    pub tif: Tif,
    pub post_only: bool,
    pub validate: bool,
}

/// Someone else's order on the account: it carries no userref of ours (or a userref we never
/// assigned) and may already be partly filled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignOrder {
    pub pair: String,
    pub side: Side,
    pub kind: OrderKind,
    pub volume: Dec,
    pub userref: Option<i32>,
    /// Executed at creation.
    pub filled: Dec,
}

impl ForeignOrder {
    pub fn limit(pair: &str, side: Side, volume: &str, price: &str) -> Self {
        Self {
            pair: pair.to_string(),
            side,
            kind: OrderKind::Limit { price: crate::money::dec(price) },
            volume: crate::money::dec(volume),
            userref: None,
            filled: Dec::ZERO,
        }
    }
    pub fn market(pair: &str, side: Side, volume: &str) -> Self {
        Self {
            pair: pair.to_string(),
            side,
            kind: OrderKind::Market,
            volume: crate::money::dec(volume),
            userref: None,
            filled: Dec::ZERO,
        }
    }
    pub fn userref(mut self, userref: i32) -> Self {
        self.userref = Some(userref);
        self
    }
    pub fn filled(mut self, qty: &str) -> Self {
        self.filled = crate::money::dec(qty);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placed {
    /// Validate-only: nothing was created.
    Validated,
    Created { txid: String },
}

/// Why the exchange refused a request. Front ends translate these into their own error strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reject {
    UnknownPair(String),
    InvalidArguments(String),
    OrderMinimumNotMet,
    CostMinimumNotMet,
    InsufficientFunds,
    PostOnlyWouldCross,
    /// The pair's trading status forbids this order (payload = the status).
    MarketMode(String),
    UnknownOrder,
    /// Verbatim wire-level error strings scripted by the test.
    Scripted(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelTarget {
    Txid(String),
    Userref(i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelResult {
    pub count: u32,
    /// The cancel was accepted but not yet processed (deferred-cancel mode).
    pub pending: bool,
    pub txids: Vec<String>,
}

// ---------------------------------------------------------------- accounts

#[derive(Debug, Clone, Default)]
struct AccountState {
    balances: BTreeMap<String, Dec>,
    /// Net external movements per asset: deposits, withdrawals, unexplained drift. Together with
    /// the fill history this must reproduce the balances exactly.
    external: BTreeMap<String, Dec>,
    fees: Option<FeeSchedule>,
}

/// The exchange core.
pub struct Core {
    seed: u64,
    pairs: BTreeMap<String, PairMarket>,
    accounts: BTreeMap<String, AccountState>,
    orders: Vec<Order>,
    index: BTreeMap<String, usize>,
    fee_default: FeeSchedule,
    default_policy: FillPolicy,
    rules: Vec<OrderRule>,
    armed_glitches: VecDeque<ReportGlitch>,
    deferred_cancels: u32,
    next_order: u64,
    next_fill: u64,
}

impl Core {
    pub fn new(seed: u64, fees: FeeSchedule) -> Self {
        Self {
            seed,
            pairs: BTreeMap::new(),
            accounts: BTreeMap::new(),
            orders: Vec::new(),
            index: BTreeMap::new(),
            fee_default: fees,
            default_policy: FillPolicy::Auto,
            rules: Vec::new(),
            armed_glitches: VecDeque::new(),
            deferred_cancels: 0,
            next_order: 1,
            next_fill: 1,
        }
    }

    // ------------------------------------------------------------ setup

    pub fn add_pair(&mut self, spec: PairSpec, price: Dec) {
        let spread = spec.tick_size;
        let m = PairMarket { last: price, bid: price, ask: add(price, spread), spread, spec };
        self.pairs.insert(m.spec.canonical.clone(), m);
    }

    pub fn add_account(&mut self, name: &str) {
        self.accounts.entry(name.to_string()).or_default();
    }

    pub fn has_account(&self, name: &str) -> bool {
        self.accounts.contains_key(name)
    }

    pub fn pair_specs(&self) -> Vec<&PairSpec> {
        self.pairs.values().map(|m| &m.spec).collect()
    }

    pub fn resolve_wire_pair(&self, name: &str) -> Option<&PairSpec> {
        self.pairs.values().map(|m| &m.spec).find(|s| s.matches_wire_name(name))
    }

    pub fn market(&self, canonical: &str) -> Option<&PairMarket> {
        self.pairs.get(canonical)
    }

    pub fn set_pair_status(&mut self, canonical: &str, status: &str) {
        self.pairs.get_mut(canonical).unwrap_or_else(|| panic!("unknown pair {canonical}")).spec.status = status.to_string();
    }

    pub fn set_fee_default(&mut self, fees: FeeSchedule) {
        self.fee_default = fees;
    }

    pub fn set_account_fees(&mut self, account: &str, fees: Option<FeeSchedule>) {
        self.acct_mut(account).fees = fees;
    }

    pub fn set_default_policy(&mut self, policy: FillPolicy) {
        self.default_policy = policy;
    }

    pub fn push_rule(&mut self, rule: OrderRule) {
        self.rules.push(rule);
    }

    pub fn clear_rules(&mut self) {
        self.rules.clear();
    }

    pub fn arm_glitch(&mut self, glitch: ReportGlitch) {
        self.armed_glitches.push_back(glitch);
    }

    /// Defer the next `n` cancel requests: the exchange answers `pending` and processes them
    /// only when `settle_pending_cancels` runs. Until then the order can still fill.
    pub fn defer_next_cancels(&mut self, n: u32) {
        self.deferred_cancels = n;
    }

    fn acct(&self, name: &str) -> &AccountState {
        self.accounts.get(name).unwrap_or_else(|| panic!("fake-broker: unknown account {name:?}"))
    }

    fn acct_mut(&mut self, name: &str) -> &mut AccountState {
        self.accounts.get_mut(name).unwrap_or_else(|| panic!("fake-broker: unknown account {name:?}"))
    }

    fn fee_rate(&self, account: &str, liquidity: Liquidity) -> Dec {
        let f = self.acct(account).fees.unwrap_or(self.fee_default);
        match liquidity {
            Liquidity::Maker => f.maker,
            Liquidity::Taker => f.taker,
        }
    }

    // ------------------------------------------------------------ balances

    pub fn balance(&self, account: &str, asset: &str) -> Dec {
        self.acct(account).balances.get(asset).copied().unwrap_or(Dec::ZERO)
    }

    pub fn balances(&self, account: &str) -> BTreeMap<String, Dec> {
        self.acct(account).balances.clone()
    }

    /// Funds committed to live orders: quote for buys (at the reserve price plus taker fee),
    /// base for sells.
    pub fn reserved(&self, account: &str, asset: &str) -> Dec {
        let taker = self.fee_rate(account, Liquidity::Taker);
        let mut total = Dec::ZERO;
        for o in self.orders.iter().filter(|o| o.account == account && o.status.is_live()) {
            let m = &self.pairs[&o.pair];
            let rem = o.remaining();
            match o.side {
                Side::Buy if m.spec.quote() == asset => {
                    total = add(total, mul(mul(rem, o.reserve_price), add(Dec::from_i64(1), taker)));
                }
                Side::Sell if m.spec.base() == asset => total = add(total, rem),
                _ => {}
            }
        }
        total
    }

    /// Balance not committed to live orders.
    pub fn available(&self, account: &str, asset: &str) -> Dec {
        sub(self.balance(account, asset), self.reserved(account, asset))
    }

    /// An external movement of `delta` (positive = deposit, negative = withdrawal or drift).
    /// Balances may not go negative.
    pub fn external_adjust(&mut self, account: &str, asset: &str, delta: Dec) {
        let a = self.acct_mut(account);
        let new_bal = add(a.balances.get(asset).copied().unwrap_or(Dec::ZERO), delta);
        assert!(!new_bal.is_negative(), "fake-broker: external adjustment would make {asset} balance negative");
        a.balances.insert(asset.to_string(), new_bal);
        let ext = add(a.external.get(asset).copied().unwrap_or(Dec::ZERO), delta);
        a.external.insert(asset.to_string(), ext);
    }

    pub fn set_balance(&mut self, account: &str, asset: &str, amount: Dec) {
        let delta = sub(amount, self.balance(account, asset));
        self.external_adjust(account, asset, delta);
    }

    /// Mark-to-market value of the account in `quote`, using each holding's `X/quote` last
    /// price. Holdings with no such pair are ignored.
    pub fn equity(&self, account: &str, quote: &str) -> Dec {
        let mut total = Dec::ZERO;
        for (asset, bal) in &self.acct(account).balances {
            if asset == quote {
                total = add(total, *bal);
            } else if let Some(m) = self.pairs.get(&format!("{asset}/{quote}")) {
                total = add(total, mul(*bal, m.last));
            }
        }
        total
    }

    // ------------------------------------------------------------ market moves

    pub fn set_price(&mut self, now: u64, canonical: &str, price: Dec) {
        let m = self.pairs.get_mut(canonical).unwrap_or_else(|| panic!("fake-broker: unknown pair {canonical}"));
        m.last = price;
        m.bid = price;
        m.ask = add(price, m.spread);
        self.match_resting(now, canonical);
    }

    pub fn set_spread(&mut self, canonical: &str, spread: Dec) {
        let m = self.pairs.get_mut(canonical).unwrap_or_else(|| panic!("fake-broker: unknown pair {canonical}"));
        m.spread = spread;
        m.ask = add(m.bid, spread);
    }

    pub fn set_quote(&mut self, now: u64, canonical: &str, bid: Dec, ask: Dec, last: Dec) {
        let m = self.pairs.get_mut(canonical).unwrap_or_else(|| panic!("fake-broker: unknown pair {canonical}"));
        m.bid = bid;
        m.ask = ask;
        m.last = last;
        self.match_resting(now, canonical);
    }

    fn touch(&self, pair: &str, side: Side) -> Dec {
        let m = &self.pairs[pair];
        match side {
            Side::Buy => m.ask,
            Side::Sell => m.bid,
        }
    }

    fn crosses(&self, pair: &str, side: Side, limit: Dec) -> bool {
        let t = self.touch(pair, side);
        match side {
            Side::Buy => t <= limit,
            Side::Sell => t >= limit,
        }
    }

    // ------------------------------------------------------------ placing

    fn take_policy(&mut self, new: &NewOrder, consume: bool) -> FillPolicy {
        let pos = self.rules.iter().position(|r| {
            r.remaining > 0 && r.matcher.matches(&new.pair, new.side, new.kind, new.userref)
        });
        match pos {
            Some(i) => {
                let policy = self.rules[i].policy.clone();
                if consume {
                    self.rules[i].remaining -= 1;
                    if self.rules[i].remaining == 0 {
                        self.rules.remove(i);
                    }
                }
                policy
            }
            None => self.default_policy.clone(),
        }
    }

    /// Validate an order against the exchange rules and the account's funds.
    fn check_order(&self, new: &NewOrder) -> Result<(), Reject> {
        let m = self.pairs.get(&new.pair).ok_or_else(|| Reject::UnknownPair(new.pair.clone()))?;
        let spec = &m.spec;
        let is_limit = matches!(new.kind, OrderKind::Limit { .. });
        match spec.status.as_str() {
            "online" => {}
            "post_only" if is_limit && new.post_only => {}
            "limit_only" if is_limit => {}
            other => return Err(Reject::MarketMode(other.to_string())),
        }
        if !new.volume.is_positive() {
            return Err(Reject::InvalidArguments("volume".into()));
        }
        if new.volume.decimals() > spec.lot_decimals {
            return Err(Reject::InvalidArguments("volume".into()));
        }
        if new.post_only && !is_limit {
            return Err(Reject::InvalidArguments("oflags".into()));
        }
        if new.tif == Tif::Ioc && !is_limit {
            return Err(Reject::InvalidArguments("timeinforce".into()));
        }
        let ref_price = match new.kind {
            OrderKind::Market => self.touch(&new.pair, new.side),
            OrderKind::Limit { price } => {
                if !price.is_positive() || price.decimals() > spec.pair_decimals {
                    return Err(Reject::InvalidArguments("price".into()));
                }
                match price.round_to_multiple(spec.tick_size, broker_adapters::decimal::Rounding::Floor) {
                    Ok(p) if p == price => {}
                    _ => return Err(Reject::InvalidArguments("price".into())),
                }
                price
            }
        };
        if new.volume < spec.order_min {
            return Err(Reject::OrderMinimumNotMet);
        }
        if mul(new.volume, ref_price) < spec.cost_min {
            return Err(Reject::CostMinimumNotMet);
        }
        if new.post_only {
            if let OrderKind::Limit { price } = new.kind {
                if self.crosses(&new.pair, new.side, price) {
                    return Err(Reject::PostOnlyWouldCross);
                }
            }
        }
        // Funds.
        let taker = self.fee_rate(&new.account, Liquidity::Taker);
        match new.side {
            Side::Buy => {
                let need = mul(mul(new.volume, ref_price), add(Dec::from_i64(1), taker));
                if need > self.available(&new.account, spec.quote()) {
                    return Err(Reject::InsufficientFunds);
                }
            }
            Side::Sell => {
                if new.volume > self.available(&new.account, spec.base()) {
                    return Err(Reject::InsufficientFunds);
                }
            }
        }
        Ok(())
    }

    /// Place (or validate) an order, consuming a scripted rule if one matches.
    pub fn place(&mut self, now: u64, new: &NewOrder) -> Result<Placed, Reject> {
        self.place_inner(now, new, None, false)
    }

    fn place_inner(&mut self, now: u64, new: &NewOrder, forced: Option<FillPolicy>, foreign: bool) -> Result<Placed, Reject> {
        if !self.accounts.contains_key(&new.account) {
            panic!("fake-broker: unknown account {:?}", new.account);
        }
        let policy = match forced {
            Some(p) => p,
            None => self.take_policy(new, !new.validate),
        };
        if let FillPolicy::Reject(codes) = &policy {
            return Err(Reject::Scripted(codes.clone()));
        }
        self.check_order(new)?;
        if new.validate {
            return Ok(Placed::Validated);
        }
        let txid = kraken_style_id('O', self.seed, self.next_order);
        let reserve_price = match new.kind {
            OrderKind::Limit { price } => price,
            OrderKind::Market => self.touch(&new.pair, new.side),
        };
        let mut order = Order {
            txid: txid.clone(),
            account: new.account.clone(),
            seq: self.next_order,
            pair: new.pair.clone(),
            side: new.side,
            kind: new.kind,
            volume: new.volume,
            userref: new.userref,
            tif: new.tif,
            post_only: new.post_only,
            status: OrderStatus::Open,
            reason: None,
            open_time_nanos: now,
            close_time_nanos: None,
            fills: Vec::new(),
            foreign,
            cancel_requested: false,
            policy: policy.clone(),
            queue: VecDeque::new(),
            reserve_price,
        };
        self.next_order += 1;
        let idx = self.orders.len();

        match &policy {
            FillPolicy::Auto | FillPolicy::NoFill | FillPolicy::FullFill { .. } => {}
            FillPolicy::Pending => {
                order.status = OrderStatus::Pending;
                order.policy = FillPolicy::Auto;
            }
            FillPolicy::Partial(steps) => order.queue = steps.iter().cloned().collect(),
            FillPolicy::Reject(_) => unreachable!("handled above"),
        }
        self.index.insert(txid.clone(), idx);
        self.orders.push(order);

        match policy {
            FillPolicy::Auto => self.try_auto_fill(now, idx, true),
            FillPolicy::FullFill { price } => {
                let o = &self.orders[idx];
                let qty = o.volume;
                let (px, liq) = match (price, o.kind) {
                    (Some(p), OrderKind::Market) => (p, Liquidity::Taker),
                    (Some(p), OrderKind::Limit { .. }) => (p, Liquidity::Taker),
                    (None, OrderKind::Market) => (self.touch(&o.pair, o.side), Liquidity::Taker),
                    (None, OrderKind::Limit { price }) => (price, Liquidity::Maker),
                };
                self.fill_internal(now, idx, qty, px, liq);
            }
            FillPolicy::Partial(_) => {
                // The first scripted step happens at placement.
                self.apply_next_step(now, idx, true);
            }
            FillPolicy::NoFill | FillPolicy::Pending => {}
            FillPolicy::Reject(_) => unreachable!(),
        }
        self.finish_ioc(now, idx);
        Ok(Placed::Created { txid })
    }

    /// Add someone else's order to the account. Goes through the normal validation and funds
    /// check (an order that could not exist is a test-authoring error), never consumes a
    /// scripted rule, and then behaves like any other order under `Auto` matching: a limit that
    /// does not cross rests, and fills later if the market moves through it.
    pub fn add_foreign_order(&mut self, now: u64, account: &str, f: &ForeignOrder) -> Result<String, Reject> {
        let new = NewOrder {
            account: account.to_string(),
            pair: f.pair.clone(),
            side: f.side,
            kind: f.kind,
            volume: f.volume,
            userref: f.userref,
            tif: Tif::Gtc,
            post_only: false,
            validate: false,
        };
        match self.place_inner(now, &new, Some(FillPolicy::Auto), true)? {
            Placed::Created { txid } => {
                if f.filled.is_positive() {
                    self.script_fill(now, &txid, f.filled, None).map_err(Reject::InvalidArguments)?;
                }
                Ok(txid)
            }
            Placed::Validated => unreachable!("validate is false"),
        }
    }

    fn finish_ioc(&mut self, now: u64, idx: usize) {
        let o = &self.orders[idx];
        if o.tif == Tif::Ioc && o.status.is_live() {
            self.close_order(now, idx, OrderStatus::Canceled, "Immediate-or-cancel");
        }
    }

    fn close_order(&mut self, now: u64, idx: usize, status: OrderStatus, reason: &str) {
        let o = &mut self.orders[idx];
        o.status = status;
        o.reason = Some(reason.to_string());
        o.close_time_nanos = Some(now);
        o.cancel_requested = false;
    }

    // ------------------------------------------------------------ matching and filling

    fn try_auto_fill(&mut self, now: u64, idx: usize, at_placement: bool) {
        let o = &self.orders[idx];
        if o.status != OrderStatus::Open || o.policy != FillPolicy::Auto {
            return;
        }
        let qty = o.remaining();
        if !qty.is_positive() {
            return;
        }
        let (px, liq) = match o.kind {
            OrderKind::Market => (self.touch(&o.pair, o.side), Liquidity::Taker),
            OrderKind::Limit { price } => {
                if !self.crosses(&o.pair, o.side, price) {
                    return;
                }
                if at_placement {
                    (self.touch(&o.pair, o.side), Liquidity::Taker)
                } else {
                    (price, Liquidity::Maker)
                }
            }
        };
        self.fill_internal(now, idx, qty, px, liq);
    }

    fn match_resting(&mut self, now: u64, pair: &str) {
        for idx in 0..self.orders.len() {
            if self.orders[idx].pair == pair {
                self.try_auto_fill(now, idx, false);
            }
        }
    }

    /// Execute `qty` at `price`. Applies balances, records the fill, closes the order when it is
    /// complete. If the account cannot pay (an external withdrawal or an adverse price move after
    /// placement) nothing executes and the order is closed as cancelled with reason
    /// `Insufficient funds`. Returns whether the fill happened.
    fn fill_internal(&mut self, now: u64, idx: usize, qty: Dec, price: Dec, liquidity: Liquidity) -> bool {
        let (account, pair, side, txid) = {
            let o = &self.orders[idx];
            (o.account.clone(), o.pair.clone(), o.side, o.txid.clone())
        };
        assert!(qty.is_positive(), "fake-broker: fill quantity must be positive");
        assert!(qty <= self.orders[idx].remaining(), "fake-broker: fill of {qty} exceeds the remaining volume of {txid}");
        let spec = self.pairs[&pair].spec.clone();
        let cost = mul(qty, price);
        let fee = mul(cost, self.fee_rate(&account, liquidity));

        let affordable = match side {
            Side::Buy => add(cost, fee) <= self.balance(&account, spec.quote()),
            Side::Sell => qty <= self.balance(&account, spec.base()),
        };
        if !affordable {
            self.close_order(now, idx, OrderStatus::Canceled, "Insufficient funds");
            return false;
        }
        {
            let a = self.acct_mut(&account);
            let bal = |a: &AccountState, k: &str| a.balances.get(k).copied().unwrap_or(Dec::ZERO);
            match side {
                Side::Buy => {
                    let nb = add(bal(a, spec.base()), qty);
                    let nq = sub(bal(a, spec.quote()), add(cost, fee));
                    a.balances.insert(spec.base().to_string(), nb);
                    a.balances.insert(spec.quote().to_string(), nq);
                }
                Side::Sell => {
                    let nb = sub(bal(a, spec.base()), qty);
                    let nq = add(bal(a, spec.quote()), sub(cost, fee));
                    a.balances.insert(spec.base().to_string(), nb);
                    a.balances.insert(spec.quote().to_string(), nq);
                }
            }
        }
        let report = self.armed_glitches.pop_front().unwrap_or(ReportGlitch::Normal);
        let fill = Fill {
            id: kraken_style_id('T', self.seed, self.next_fill),
            order: txid,
            qty,
            price,
            cost,
            fee,
            time_nanos: now,
            liquidity,
            report,
        };
        self.next_fill += 1;
        self.orders[idx].fills.push(fill);
        if !self.orders[idx].remaining().is_positive() {
            let o = &mut self.orders[idx];
            o.status = OrderStatus::Closed;
            o.close_time_nanos = Some(now);
            o.cancel_requested = false;
        }
        true
    }

    /// Apply the next queued step of a `Partial` script. Returns false when the queue is empty.
    fn apply_next_step(&mut self, now: u64, idx: usize, at_placement: bool) -> bool {
        let Some(step) = self.orders[idx].queue.pop_front() else { return false };
        let (kind, volume, remaining, pair, side, txid) = {
            let o = &self.orders[idx];
            (o.kind, o.volume, o.remaining(), o.pair.clone(), o.side, o.txid.clone())
        };
        let lot = self.pairs[&pair].spec.lot_decimals;
        let qty = match &step.amount {
            StepAmount::Qty(q) => *q,
            StepAmount::Fraction(f) => mul(volume, *f)
                .round_dp(lot, broker_adapters::decimal::Rounding::Floor)
                .expect("lot decimals within scale"),
            StepAmount::Remainder => remaining,
        };
        assert!(
            qty.is_positive() && qty <= remaining,
            "fake-broker script error: step {step:?} fills {qty} but order {txid} has {remaining} remaining"
        );
        let (px, liq) = match (step.price, kind) {
            (Some(p), OrderKind::Market) => (p, Liquidity::Taker),
            (Some(p), OrderKind::Limit { .. }) => (p, Liquidity::Maker),
            (None, OrderKind::Market) => (self.touch(&pair, side), Liquidity::Taker),
            (None, OrderKind::Limit { price }) => {
                if at_placement && self.crosses(&pair, side, price) {
                    (self.touch(&pair, side), Liquidity::Taker)
                } else {
                    (price, Liquidity::Maker)
                }
            }
        };
        self.fill_internal(now, idx, qty, px, liq);
        true
    }

    // ------------------------------------------------------------ scripted order control

    fn idx_of(&self, txid: &str) -> Result<usize, String> {
        self.index.get(txid).copied().ok_or_else(|| format!("unknown order {txid}"))
    }

    /// Apply the next queued step of an order's `Partial` script.
    pub fn apply_next_fill(&mut self, now: u64, txid: &str) -> Result<(), String> {
        let idx = self.idx_of(txid)?;
        if !self.orders[idx].status.is_live() {
            return Err(format!("order {txid} is {}", self.orders[idx].status.as_kraken()));
        }
        if self.apply_next_step(now, idx, false) {
            Ok(())
        } else {
            Err(format!("order {txid} has no scripted fill left"))
        }
    }

    /// Execute `qty` of a live order now. `price: None` = limit price / touch.
    pub fn script_fill(&mut self, now: u64, txid: &str, qty: Dec, price: Option<Dec>) -> Result<(), String> {
        let idx = self.idx_of(txid)?;
        let o = &self.orders[idx];
        if !o.status.is_live() {
            return Err(format!("order {txid} is {}", o.status.as_kraken()));
        }
        if !qty.is_positive() || qty > o.remaining() {
            return Err(format!("fill {qty} does not fit the remaining {} of {txid}", o.remaining()));
        }
        let (px, liq) = match (price, o.kind) {
            (Some(p), OrderKind::Market) => (p, Liquidity::Taker),
            (Some(p), OrderKind::Limit { .. }) => (p, Liquidity::Maker),
            (None, OrderKind::Market) => (self.touch(&o.pair, o.side), Liquidity::Taker),
            (None, OrderKind::Limit { price }) => (price, Liquidity::Maker),
        };
        if self.fill_internal(now, idx, qty, px, liq) {
            Ok(())
        } else {
            Err(format!("insufficient funds to execute the fill on {txid}; the order was closed"))
        }
    }

    /// Move a `Pending` order to `Open` and let it match as `Auto`.
    pub fn activate(&mut self, now: u64, txid: &str) -> Result<(), String> {
        let idx = self.idx_of(txid)?;
        if self.orders[idx].status != OrderStatus::Pending {
            return Err(format!("order {txid} is not pending"));
        }
        self.orders[idx].status = OrderStatus::Open;
        self.try_auto_fill(now, idx, true);
        self.finish_ioc(now, idx);
        Ok(())
    }

    /// Expire a live order (partial fills are kept).
    pub fn expire(&mut self, now: u64, txid: &str, reason: &str) -> Result<(), String> {
        let idx = self.idx_of(txid)?;
        if !self.orders[idx].status.is_live() {
            return Err(format!("order {txid} is not live"));
        }
        self.close_order(now, idx, OrderStatus::Expired, reason);
        Ok(())
    }

    /// Cancel from the exchange side (not a user request).
    pub fn cancel_from_exchange(&mut self, now: u64, txid: &str, reason: &str) -> Result<(), String> {
        let idx = self.idx_of(txid)?;
        if !self.orders[idx].status.is_live() {
            return Err(format!("order {txid} is not live"));
        }
        self.close_order(now, idx, OrderStatus::Canceled, reason);
        Ok(())
    }

    pub fn set_fill_glitch(&mut self, txid: &str, fill_index: usize, glitch: ReportGlitch) -> Result<(), String> {
        let idx = self.idx_of(txid)?;
        let fills = &mut self.orders[idx].fills;
        let f = fills.get_mut(fill_index).ok_or_else(|| format!("order {txid} has no fill #{fill_index}"))?;
        f.report = glitch;
        Ok(())
    }

    // ------------------------------------------------------------ cancel

    /// User-requested cancel of one order (by txid) or of every live order with a userref.
    /// Only live orders of `account` qualify; anything else is `UnknownOrder`, like Kraken.
    pub fn cancel(&mut self, now: u64, account: &str, target: &CancelTarget) -> Result<CancelResult, Reject> {
        let idxs: Vec<usize> = self
            .orders
            .iter()
            .enumerate()
            .filter(|(_, o)| o.account == account && o.status.is_live())
            .filter(|(_, o)| match target {
                CancelTarget::Txid(t) => &o.txid == t,
                CancelTarget::Userref(u) => o.userref == Some(*u),
            })
            .map(|(i, _)| i)
            .collect();
        if idxs.is_empty() {
            return Err(Reject::UnknownOrder);
        }
        let deferred = if self.deferred_cancels > 0 {
            self.deferred_cancels -= 1;
            true
        } else {
            false
        };
        let mut txids = Vec::new();
        for idx in idxs {
            txids.push(self.orders[idx].txid.clone());
            if deferred {
                self.orders[idx].cancel_requested = true;
            } else {
                self.close_order(now, idx, OrderStatus::Canceled, "User requested");
            }
        }
        Ok(CancelResult { count: u32::try_from(txids.len()).unwrap_or(u32::MAX), pending: deferred, txids })
    }

    /// Process cancels that were accepted in deferred mode. Returns how many completed.
    pub fn settle_pending_cancels(&mut self, now: u64) -> usize {
        let idxs: Vec<usize> =
            self.orders.iter().enumerate().filter(|(_, o)| o.cancel_requested && o.status.is_live()).map(|(i, _)| i).collect();
        for &idx in &idxs {
            self.close_order(now, idx, OrderStatus::Canceled, "User requested");
        }
        idxs.len()
    }

    // ------------------------------------------------------------ queries

    pub fn order(&self, txid: &str) -> Option<&Order> {
        self.index.get(txid).map(|&i| &self.orders[i])
    }

    pub fn orders_of(&self, account: &str) -> Vec<&Order> {
        self.orders.iter().filter(|o| o.account == account).collect()
    }

    pub fn all_orders(&self) -> &[Order] {
        &self.orders
    }

    // ------------------------------------------------------------ invariants

    /// Verify the core invariants. Returns a description of the first violation.
    pub fn check_invariants(&self) -> Result<(), String> {
        for (name, acct) in &self.accounts {
            // Rebuild balances from the external ledger plus every fill of this account.
            let mut expected: BTreeMap<String, Dec> = acct.external.clone();
            for o in self.orders.iter().filter(|o| &o.account == name) {
                let spec = &self.pairs[&o.pair].spec;
                for f in &o.fills {
                    let (b, q) = (spec.base().to_string(), spec.quote().to_string());
                    let get = |m: &BTreeMap<String, Dec>, k: &str| m.get(k).copied().unwrap_or(Dec::ZERO);
                    match o.side {
                        Side::Buy => {
                            let nb = add(get(&expected, &b), f.qty);
                            let nq = sub(get(&expected, &q), add(f.cost, f.fee));
                            expected.insert(b, nb);
                            expected.insert(q, nq);
                        }
                        Side::Sell => {
                            let nb = sub(get(&expected, &b), f.qty);
                            let nq = add(get(&expected, &q), sub(f.cost, f.fee));
                            expected.insert(b, nb);
                            expected.insert(q, nq);
                        }
                    }
                }
            }
            let assets: std::collections::BTreeSet<&String> = acct.balances.keys().chain(expected.keys()).collect();
            for asset in assets {
                let have = acct.balances.get(asset).copied().unwrap_or(Dec::ZERO);
                let want = expected.get(asset).copied().unwrap_or(Dec::ZERO);
                if have.is_negative() {
                    return Err(format!("{name}: {asset} balance is negative ({have})"));
                }
                if have != want {
                    return Err(format!("{name}: {asset} balance {have} != ledger {want}"));
                }
            }
        }
        for o in &self.orders {
            let exec = o.vol_exec();
            if exec > o.volume {
                return Err(format!("{}: executed {exec} exceeds volume {}", o.txid, o.volume));
            }
            if o.status == OrderStatus::Closed && exec != o.volume {
                return Err(format!("{}: closed but executed {exec} of {}", o.txid, o.volume));
            }
            if o.status.is_live() && exec >= o.volume {
                return Err(format!("{}: live but fully executed", o.txid));
            }
            let cost_from_fills = sum(o.fills.iter().map(|f| mul(f.qty, f.price)));
            if cost_from_fills != o.cost() {
                return Err(format!("{}: cost {} != sum(qty*price) {cost_from_fills}", o.txid, o.cost()));
            }
            if o.fills.iter().any(|f| !f.qty.is_positive() || f.fee.is_negative()) {
                return Err(format!("{}: a fill has a non-positive quantity or a negative fee", o.txid));
            }
        }
        Ok(())
    }
}
