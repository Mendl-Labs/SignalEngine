//! Scripting types: how a fake order should behave once it is placed.

use crate::money::dec;
use broker_adapters::{Dec, OrderKind, Side};

/// How much one scripted fill executes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepAmount {
    /// An absolute base quantity.
    Qty(Dec),
    /// A fraction of the order volume, floored to the pair's lot decimals.
    Fraction(Dec),
    /// Whatever is still unfilled.
    Remainder,
}

/// One scripted execution. `price: None` means "the limit price for a limit order, the touch
/// (ask for buys, bid for sells) for a market order".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillStep {
    pub amount: StepAmount,
    pub price: Option<Dec>,
}

impl FillStep {
    pub fn qty(q: &str) -> Self {
        Self { amount: StepAmount::Qty(dec(q)), price: None }
    }
    pub fn fraction(f: &str) -> Self {
        Self { amount: StepAmount::Fraction(dec(f)), price: None }
    }
    pub fn remainder() -> Self {
        Self { amount: StepAmount::Remainder, price: None }
    }
    /// Execute this step at an explicit price.
    pub fn at(mut self, price: &str) -> Self {
        self.price = Some(dec(price));
        self
    }
}

/// What the exchange does with an order after accepting it. The default is [`FillPolicy::Auto`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillPolicy {
    /// Realistic matching against the simplified book. Market orders, and limit orders that
    /// cross the touch, fill fully and immediately at the touch (taker fee). Other limit orders
    /// rest and fill fully at their limit price (maker fee) when the market moves through them.
    Auto,
    /// Fill the whole order at once, at `price` (limit price / touch when `None`), even if a
    /// limit order does not cross the book.
    FullFill { price: Option<Dec> },
    /// Fill in scripted pieces. The first step executes at placement; each later step executes
    /// when the test calls `apply_next_fill`. If the steps do not add up to the full volume the
    /// order stays open (partially filled) until cancelled.
    Partial(Vec<FillStep>),
    /// Accept the order and let it rest, never filling on its own. Tests can still script fills.
    NoFill,
    /// Accept the order in Kraken's `pending` state; it becomes `open` (and matches as `Auto`)
    /// when the test calls `activate_order`.
    Pending,
    /// Refuse the order with these Kraken error strings, for example
    /// `"EOrder:Insufficient funds"`. Nothing is created.
    Reject(Vec<String>),
}

impl FillPolicy {
    pub fn reject(code: &str) -> Self {
        FillPolicy::Reject(vec![code.to_string()])
    }
    pub fn full_fill_at(price: &str) -> Self {
        FillPolicy::FullFill { price: Some(dec(price)) }
    }
    pub fn partial(steps: Vec<FillStep>) -> Self {
        FillPolicy::Partial(steps)
    }
}

/// Which orders a rule applies to. Every unset field matches anything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderMatcher {
    pub pair: Option<String>,
    pub side: Option<Side>,
    pub userref: Option<i32>,
    /// `Some(true)` = market orders only, `Some(false)` = limit orders only.
    pub market: Option<bool>,
}

impl OrderMatcher {
    pub(crate) fn matches(&self, pair: &str, side: Side, kind: OrderKind, userref: Option<i32>) -> bool {
        self.pair.as_deref().is_none_or(|p| p == pair)
            && self.side.is_none_or(|s| s == side)
            && self.userref.is_none_or(|u| Some(u) == userref)
            && self.market.is_none_or(|m| m == matches!(kind, OrderKind::Market))
    }
}

/// "The next `remaining` matching orders get `policy`." Rules are consulted first-in first-out;
/// the first rule that still has uses left and matches the order wins. Validate-only requests
/// never consume a rule (but a matching `Reject` rule is applied to them, as Kraken's validation
/// reports the same errors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRule {
    pub matcher: OrderMatcher,
    pub policy: FillPolicy,
    pub remaining: u32,
}

impl OrderRule {
    /// A rule for the next single matching order.
    pub fn next(policy: FillPolicy) -> Self {
        Self { matcher: OrderMatcher::default(), policy, remaining: 1 }
    }
    pub fn times(mut self, n: u32) -> Self {
        self.remaining = n;
        self
    }
    pub fn pair(mut self, canonical: &str) -> Self {
        self.matcher.pair = Some(canonical.to_string());
        self
    }
    pub fn side(mut self, side: Side) -> Self {
        self.matcher.side = Some(side);
        self
    }
    pub fn userref(mut self, userref: i32) -> Self {
        self.matcher.userref = Some(userref);
        self
    }
    pub fn market_only(mut self) -> Self {
        self.matcher.market = Some(true);
        self
    }
    pub fn limit_only(mut self) -> Self {
        self.matcher.market = Some(false);
        self
    }
}
