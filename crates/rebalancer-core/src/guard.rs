//! Pre-trade guard: a pure function from (policy, account, proposed order, day counters) to a verdict.
//!
//! Semantics a reviewer must know:
//! * **All applicable denials are returned**, in a fixed order, each with a stable machine code
//!   ([`DenialCode::as_str`]). `allow` is true only when there are none.
//! * **Reducing orders.** A SELL that does not exceed a long position (or a BUY that does not exceed a short one)
//!   can only shrink exposure. It is exempt from the exposure caps (position, asset class, gross, net), the cash
//!   reserve and the per-order notional cap, because otherwise a position that has drifted above a cap could never
//!   be trimmed. It is still subject to the universe checks, the price checks, the daily order count and the daily
//!   turnover, EXCEPT when the account is halted or the mandate is expired: then reducing orders skip the count and
//!   turnover limits too, and every non-reducing order is denied (`ACCOUNT_HALTED`, `MANDATE_EXPIRED`; spec B3/B4).
//! * **No usable mandate denies everything, including reductions:** invalid mandate, no envelope, status other than
//!   Active, or not yet effective (`MANDATE_INVALID`, `MANDATE_NOT_ACTIVE`). Flattening is a separate path (WP4.5).
//! * **Fail closed on data:** missing/non-positive/stale/future-dated price, non-positive equity, currency
//!   mismatch, arithmetic overflow all deny.
//! * Limits are checked AFTER the order (`exposure after = exposure now + this order`), exact decimals throughout;
//!   a value exactly at a limit is allowed, one unit over is denied.
//! * **Capital base.** Every percentage-of-equity limit (`max_position`, `max_asset_class`, `max_gross`,
//!   `max_net`, `max_turnover_per_day`) and the cash-reserve fraction is measured against
//!   `Policy::capital_base(equity)` = `min(broker equity, mandate.capital.allocated)`, the same number the planner
//!   sizes on. A 25% position cap on a $5,000 allocation is $1,250 even when the broker account holds $20,000.
//!   The AVAILABLE CASH test still uses the broker's actual cash. Exposure (gross, net, class) still sums every
//!   position on the account, including holdings the mandate does not cover, so unrelated holdings in a shared
//!   account consume the allocation's headroom (fail closed).
//!
//! * **Signed plans (opt-in).** `PreTradeGuard::check` is unchanged and knows only the caller's `uses_margin` flag.
//!   `PreTradeGuard::check_margin` adds what a short or levered book needs: it derives margin use from the account
//!   itself (`margin_use`: an order that is not a pure reduction and leaves the instrument short, or gross above the
//!   broker's equity, or negative cash), denies it `LEVERAGE_FORBIDDEN` when the mandate's `leverage_max_gross` is 1,
//!   and tests funding against the broker's buying power (`MarginContext`) instead of cash. Shorting stays governed
//!   by `universe.shorting` (`SHORTING_FORBIDDEN`), and every cap above still applies.
//!
//! The guard does not mutate anything. The planner simulates its own sequence of orders (see `planner`).

use broker_adapters::{Dec, Side};
use chrono::{DateTime, Utc};

use crate::dec_math::{abs, add, mul, neg, sub, MathError};
use crate::policy::{Limits, Policy, Standing, MAX_FUTURE_SKEW_SECS};

/// Stable machine codes. Never rename a variant's string: alerts, dashboards and tests key on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DenialCode {
    MandateInvalid,
    MandateNotActive,
    MandateExpired,
    AccountHalted,
    OrderInvalid,
    EquityInvalid,
    CurrencyMismatch,
    PriceMissing,
    PriceStale,
    InstrumentDenied,
    InstrumentNotAllowed,
    VenueNotAllowed,
    AssetClassNotAllowed,
    ShortingForbidden,
    DerivativesForbidden,
    LeverageForbidden,
    MaxOrderNotional,
    MaxPosition,
    MaxAssetClass,
    MaxGross,
    MaxNet,
    MaxOrdersPerDay,
    MaxTurnoverPerDay,
    CashReserve,
    ArithmeticOverflow,
}

impl DenialCode {
    /// Every code, in the order the guard evaluates them.
    pub const ALL: [DenialCode; 25] = [
        DenialCode::MandateInvalid,
        DenialCode::MandateNotActive,
        DenialCode::MandateExpired,
        DenialCode::AccountHalted,
        DenialCode::OrderInvalid,
        DenialCode::EquityInvalid,
        DenialCode::CurrencyMismatch,
        DenialCode::PriceMissing,
        DenialCode::PriceStale,
        DenialCode::InstrumentDenied,
        DenialCode::InstrumentNotAllowed,
        DenialCode::VenueNotAllowed,
        DenialCode::AssetClassNotAllowed,
        DenialCode::ShortingForbidden,
        DenialCode::DerivativesForbidden,
        DenialCode::LeverageForbidden,
        DenialCode::MaxOrderNotional,
        DenialCode::MaxPosition,
        DenialCode::MaxAssetClass,
        DenialCode::MaxGross,
        DenialCode::MaxNet,
        DenialCode::MaxOrdersPerDay,
        DenialCode::MaxTurnoverPerDay,
        DenialCode::CashReserve,
        DenialCode::ArithmeticOverflow,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            DenialCode::MandateInvalid => "MANDATE_INVALID",
            DenialCode::MandateNotActive => "MANDATE_NOT_ACTIVE",
            DenialCode::MandateExpired => "MANDATE_EXPIRED",
            DenialCode::AccountHalted => "ACCOUNT_HALTED",
            DenialCode::OrderInvalid => "ORDER_INVALID",
            DenialCode::EquityInvalid => "EQUITY_INVALID",
            DenialCode::CurrencyMismatch => "CURRENCY_MISMATCH",
            DenialCode::PriceMissing => "PRICE_MISSING",
            DenialCode::PriceStale => "PRICE_STALE",
            DenialCode::InstrumentDenied => "INSTRUMENT_DENIED",
            DenialCode::InstrumentNotAllowed => "INSTRUMENT_NOT_ALLOWED",
            DenialCode::VenueNotAllowed => "VENUE_NOT_ALLOWED",
            DenialCode::AssetClassNotAllowed => "ASSET_CLASS_NOT_ALLOWED",
            DenialCode::ShortingForbidden => "SHORTING_FORBIDDEN",
            DenialCode::DerivativesForbidden => "DERIVATIVES_FORBIDDEN",
            DenialCode::LeverageForbidden => "LEVERAGE_FORBIDDEN",
            DenialCode::MaxOrderNotional => "MAX_ORDER_NOTIONAL",
            DenialCode::MaxPosition => "MAX_POSITION",
            DenialCode::MaxAssetClass => "MAX_ASSET_CLASS",
            DenialCode::MaxGross => "MAX_GROSS",
            DenialCode::MaxNet => "MAX_NET",
            DenialCode::MaxOrdersPerDay => "MAX_ORDERS_PER_DAY",
            DenialCode::MaxTurnoverPerDay => "MAX_TURNOVER_PER_DAY",
            DenialCode::CashReserve => "CASH_RESERVE",
            DenialCode::ArithmeticOverflow => "ARITHMETIC_OVERFLOW",
        }
    }
}

impl std::fmt::Display for DenialCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denial {
    pub code: DenialCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub allow: bool,
    pub reasons: Vec<Denial>,
}

impl Verdict {
    pub fn codes(&self) -> Vec<DenialCode> {
        self.reasons.iter().map(|r| r.code).collect()
    }
    pub fn has(&self, code: DenialCode) -> bool {
        self.reasons.iter().any(|r| r.code == code)
    }
}

/// One held position as the broker reports it. `quantity` is signed (negative = short).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub symbol: String,
    pub venue: String,
    pub asset_class: String,
    pub quantity: Dec,
    /// Signed marked value in the account currency.
    pub market_value: Dec,
}

/// The broker-reported account at one instant. Equity, cash and positions come from the BROKER (spec principle:
/// broker is the source of truth).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountView {
    pub account_id: String,
    pub ccy: String,
    pub equity: Dec,
    pub cash: Dec,
    pub positions: Vec<Position>,
    pub halted: bool,
    /// "Now" for expiry and price-staleness decisions.
    pub now: DateTime<Utc>,
}

impl AccountView {
    pub fn position(&self, symbol: &str) -> Option<&Position> {
        self.positions.iter().find(|p| p.symbol.eq_ignore_ascii_case(symbol))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PricePoint {
    pub price: Dec,
    pub as_of: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposedOrder {
    pub venue: String,
    pub asset_class: String,
    pub symbol: String,
    pub side: Side,
    pub quantity: Dec,
    /// The price used to value the order (`None` = unknown, which denies).
    pub price: Option<PricePoint>,
    /// Estimated fee in the account currency, charged on top of a buy and deducted from a sell's proceeds.
    pub est_fee: Dec,
    pub uses_margin: bool,
    pub is_derivative: bool,
}

/// What has already been done today (the account-local day is chosen by the caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DayCounters {
    pub orders_today: u32,
    /// Sum of order notionals already traded today, in the account currency.
    pub turnover_today: Dec,
}

impl DayCounters {
    pub const ZERO: DayCounters = DayCounters { orders_today: 0, turnover_today: Dec::ZERO };
}

/// Margin awareness for a SIGNED (short and/or levered) plan. `PreTradeGuard::check` knows nothing of it and is
/// unchanged; only [`PreTradeGuard::check_margin`] takes one. See that function for what it changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarginContext {
    /// What is left, before THIS order, of the broker-reported buying power: the total notional of
    /// exposure-INCREASING orders (buys that add long, sells that add short) the broker would still accept. `None`:
    /// the caller has no such figure, so any order that would use margin is denied.
    pub buying_power_left: Option<Dec>,
}

pub struct PreTradeGuard;

fn deny(reasons: &mut Vec<Denial>, code: DenialCode, message: impl Into<String>) {
    reasons.push(Denial { code, message: message.into() });
}

fn one() -> Dec {
    Dec::from_i64(1)
}

/// The account's exposure after the order, all exact.
struct After {
    position_value: Dec,
    gross: Dec,
    net: Dec,
    class_gross: Dec,
    cash: Dec,
}

fn after_state(account: &AccountView, order: &ProposedOrder, notional: Dec, class: &str) -> Result<After, MathError> {
    let signed = match order.side {
        Side::Buy => notional,
        Side::Sell => neg(notional)?,
    };
    let mut position_value = signed;
    let mut others_gross = Dec::ZERO;
    let mut others_net = Dec::ZERO;
    let mut others_class_gross = Dec::ZERO;
    for p in &account.positions {
        if p.symbol.eq_ignore_ascii_case(order.symbol.trim()) {
            position_value = add(position_value, p.market_value)?;
        } else {
            let a = abs(p.market_value)?;
            others_gross = add(others_gross, a)?;
            others_net = add(others_net, p.market_value)?;
            if p.asset_class.trim().eq_ignore_ascii_case(class) {
                others_class_gross = add(others_class_gross, a)?;
            }
        }
    }
    let pv_abs = abs(position_value)?;
    Ok(After {
        position_value: pv_abs,
        gross: add(others_gross, pv_abs)?,
        net: add(others_net, position_value)?,
        class_gross: add(others_class_gross, pv_abs)?,
        cash: sub(sub(account.cash, signed)?, order.est_fee)?,
    })
}

/// Would this order put the account on margin? True only for an order that is not a pure reduction and that leaves
/// the account (a) short in this instrument, or (b) with gross exposure above the broker's equity (levered), or
/// (c) with negative cash (borrowing). A reduction (a sell up to the long held, a buy up to the short held) is never
/// margin use: it can only shrink what margin is used. `price` must be the order's usable price.
///
/// Pure and exact. The planner sets `ProposedOrder::uses_margin` from this same function and the guard derives it
/// again itself in [`PreTradeGuard::check_margin`], so a caller that forgets the flag cannot lever an account.
pub fn margin_use(account: &AccountView, order: &ProposedOrder, price: Dec) -> Result<bool, MathError> {
    let pos_qty = account.position(&order.symbol).map_or(Dec::ZERO, |p| p.quantity);
    let (signed_qty, reducing) = match order.side {
        Side::Sell => (neg(order.quantity)?, pos_qty.is_positive() && order.quantity <= pos_qty),
        Side::Buy => (order.quantity, pos_qty.is_negative() && order.quantity <= neg(pos_qty)?),
    };
    if reducing {
        return Ok(false);
    }
    let notional = mul(order.quantity, price)?;
    let class = order.asset_class.trim().to_lowercase();
    let after = after_state(account, order, notional, &class)?;
    let short_after = add(pos_qty, signed_qty)?.is_negative();
    Ok(short_after || after.gross > account.equity || after.cash.is_negative())
}

/// The part of a non-reducing order that ADDS exposure (an order that flips through zero closes first, and only the
/// remainder adds). Exact.
fn increasing_notional(account: &AccountView, order: &ProposedOrder, price: Dec) -> Result<Dec, MathError> {
    let pos_qty = account.position(&order.symbol).map_or(Dec::ZERO, |p| p.quantity);
    let closing = match order.side {
        Side::Sell if pos_qty.is_positive() => std::cmp::min(order.quantity, pos_qty),
        Side::Buy if pos_qty.is_negative() => std::cmp::min(order.quantity, neg(pos_qty)?),
        _ => Dec::ZERO,
    };
    mul(sub(order.quantity, closing)?, price)
}

impl PreTradeGuard {
    /// Check one proposed order. Pure: no clock (the account view carries `now`), no I/O, no mutation.
    pub fn check(policy: &Policy, account: &AccountView, order: &ProposedOrder, day: &DayCounters) -> Verdict {
        Self::run(policy, account, order, day, None)
    }

    /// [`check`](Self::check) for an order of a SIGNED plan (shorts and/or gross above 1x). Every check of `check`
    /// still applies unchanged (universe, shorting permission, per-position / class / gross / net caps, order and
    /// turnover limits), plus:
    /// * the guard derives margin use itself ([`margin_use`]); an order that uses margin (flagged by the caller OR
    ///   derived) is denied `LEVERAGE_FORBIDDEN` unless the mandate's `leverage_max_gross` is above 1;
    /// * the funding test of a non-reducing order changes from "cash after the order stays above the reserve" to
    ///   "buying power left after the order stays above the reserve" when `buying_power_left` is given, because on a
    ///   margin book cash is not what limits an order (short proceeds are collateral, not spendable) and the
    ///   broker's own buying-power figure is. The shortfall is reported as `CASH_RESERVE`;
    /// * with no buying power supplied, an order that uses margin is denied `CASH_RESERVE` (nothing to verify it
    ///   against), and every other order keeps the plain cash test.
    pub fn check_margin(policy: &Policy, account: &AccountView, order: &ProposedOrder, day: &DayCounters, margin: &MarginContext) -> Verdict {
        Self::run(policy, account, order, day, Some(margin))
    }

    fn run(policy: &Policy, account: &AccountView, order: &ProposedOrder, day: &DayCounters, margin: Option<&MarginContext>) -> Verdict {
        let mut reasons = Vec::new();
        if let Err(e) = check_inner(policy, account, order, day, margin, &mut reasons) {
            deny(&mut reasons, DenialCode::ArithmeticOverflow, e.to_string());
        }
        Verdict { allow: reasons.is_empty(), reasons }
    }
}

fn check_inner(
    policy: &Policy,
    account: &AccountView,
    order: &ProposedOrder,
    day: &DayCounters,
    margin: Option<&MarginContext>,
    reasons: &mut Vec<Denial>,
) -> Result<(), MathError> {
    // --- 1. Is there a usable mandate at all?
    let standing = policy.standing(account.now);
    match &standing {
        Standing::Invalid => {
            let why = match &policy.body {
                crate::policy::PolicyBody::Invalid(r) => r.join("; "),
                crate::policy::PolicyBody::Valid(_) => String::new(),
            };
            deny(reasons, DenialCode::MandateInvalid, format!("the mandate could not be compiled: {why}"));
            return Ok(());
        }
        Standing::NotActive(why) => {
            deny(reasons, DenialCode::MandateNotActive, why.clone());
            return Ok(());
        }
        Standing::Active | Standing::Expired => {}
    }
    let Some(limits) = policy.limits() else {
        deny(reasons, DenialCode::MandateInvalid, "no compiled limits");
        return Ok(());
    };

    // --- 2. Is the order itself well formed, and is the account readable?
    if !order.quantity.is_positive() || order.est_fee.is_negative() || order.symbol.trim().is_empty() {
        deny(reasons, DenialCode::OrderInvalid, "quantity must be positive, fee non-negative and symbol non-empty");
        return Ok(());
    }
    if !account.equity.is_positive() {
        deny(reasons, DenialCode::EquityInvalid, format!("account equity {} is not positive", account.equity));
        return Ok(());
    }
    if !account.ccy.trim().eq_ignore_ascii_case(&limits.ccy) {
        deny(
            reasons,
            DenialCode::CurrencyMismatch,
            format!("account currency {} differs from the mandate currency {}", account.ccy, limits.ccy),
        );
        return Ok(());
    }

    // --- 3. Reducing or not?
    let pos_qty = account.position(&order.symbol).map_or(Dec::ZERO, |p| p.quantity);
    let held_long = if pos_qty.is_positive() { pos_qty } else { Dec::ZERO };
    let reducing = match order.side {
        Side::Sell => pos_qty.is_positive() && order.quantity <= pos_qty,
        Side::Buy => pos_qty.is_negative() && order.quantity <= neg(pos_qty)?,
    };
    let expired = standing == Standing::Expired;

    // --- 4. Account / mandate state.
    if account.halted && !reducing {
        deny(reasons, DenialCode::AccountHalted, "the account is halted: only reducing orders are allowed");
    }
    if expired && !reducing {
        deny(reasons, DenialCode::MandateExpired, "the mandate is past its review date: only reducing orders are allowed");
    }

    // --- 5. Price.
    let price = check_price(policy, account, order, reasons);

    // --- 6. Universe.
    check_universe(limits, order, reasons);

    // --- 7. Shorting, derivatives, leverage.
    if order.side == Side::Sell && !limits.shorting && order.quantity > held_long {
        deny(
            reasons,
            DenialCode::ShortingForbidden,
            format!("selling {} {} would exceed the {} held and shorting is off", order.quantity, order.symbol, held_long),
        );
    }
    if order.is_derivative && !limits.derivatives {
        deny(reasons, DenialCode::DerivativesForbidden, "derivatives are not allowed by the mandate");
    }
    // With a margin context the guard derives margin use from the account itself; without one, only the caller's flag.
    let derived_margin = match (margin, price) {
        (Some(_), Some(p)) => margin_use(account, order, p)?,
        _ => false,
    };
    if (order.uses_margin || derived_margin) && limits.leverage_max_gross <= one() {
        deny(reasons, DenialCode::LeverageForbidden, "the order uses margin and the mandate allows no leverage");
    }

    // --- 8. Limits that need a price.
    let Some(price) = price else { return Ok(()) };
    let notional = mul(order.quantity, price)?;
    let class = order.asset_class.trim().to_lowercase();
    let after = after_state(account, order, notional, &class)?;
    // The ONE capital base (min of broker equity and the mandate's allocation): the denominator of every
    // percentage-of-equity limit below and of the cash-reserve fraction. Only `after.cash` (the cash test) uses
    // the broker's actual cash.
    let equity = policy.capital_base(account.equity, &account.ccy);

    if !reducing {
        if notional > limits.max_order_notional {
            deny(
                reasons,
                DenialCode::MaxOrderNotional,
                format!("order notional {notional} is above the limit {}", limits.max_order_notional),
            );
        }
        let cap = mul(limits.max_position, equity)?;
        if after.position_value > cap {
            deny(
                reasons,
                DenialCode::MaxPosition,
                format!("{} would be {} after the order; the cap is {cap}", order.symbol, after.position_value),
            );
        }
        if let Some(class_cap) = limits.max_asset_class.get(&class) {
            let cap = mul(*class_cap, equity)?;
            if after.class_gross > cap {
                deny(
                    reasons,
                    DenialCode::MaxAssetClass,
                    format!("asset class {class} would be {} after the order; the cap is {cap}", after.class_gross),
                );
            }
        }
        let cap = mul(limits.max_gross, equity)?;
        if after.gross > cap {
            deny(reasons, DenialCode::MaxGross, format!("gross exposure would be {}; the cap is {cap}", after.gross));
        }
        let cap = mul(limits.max_net, equity)?;
        if abs(after.net)? > cap {
            deny(reasons, DenialCode::MaxNet, format!("net exposure would be {}; the cap is {cap}", after.net));
        }
    }

    // Churn limits: skipped only for reducing orders in a state that forces de-risking.
    if !((account.halted || expired) && reducing) {
        if u64::from(day.orders_today) + 1 > u64::from(limits.max_orders_per_day) {
            deny(
                reasons,
                DenialCode::MaxOrdersPerDay,
                format!("{} orders already placed today; the limit is {}", day.orders_today, limits.max_orders_per_day),
            );
        }
        let turnover_after = add(day.turnover_today, notional)?;
        let cap = mul(limits.max_turnover_per_day, equity)?;
        if turnover_after > cap {
            deny(
                reasons,
                DenialCode::MaxTurnoverPerDay,
                format!("today's turnover would be {turnover_after}; the limit is {cap}"),
            );
        }
    }

    if !reducing {
        let reserve = policy.reserve_amount(equity)?;
        match margin.map(|m| m.buying_power_left) {
            Some(Some(bp)) => {
                let increase = increasing_notional(account, order, price)?;
                let left = sub(sub(bp, increase)?, order.est_fee)?;
                if left < reserve {
                    deny(
                        reasons,
                        DenialCode::CashReserve,
                        format!("buying power would be {left} after the order (increasing {increase}, fee {}); the required reserve is {reserve}", order.est_fee),
                    );
                }
            }
            Some(None) if order.uses_margin || derived_margin => {
                deny(reasons, DenialCode::CashReserve, "the order needs margin and no buying power figure was supplied to check it against");
            }
            _ => {
                if after.cash < reserve {
                    deny(
                        reasons,
                        DenialCode::CashReserve,
                        format!("cash would be {} after the order; the required reserve is {reserve}", after.cash),
                    );
                }
            }
        }
    }
    Ok(())
}

/// Returns the usable price, or records why there is none.
fn check_price(policy: &Policy, account: &AccountView, order: &ProposedOrder, reasons: &mut Vec<Denial>) -> Option<Dec> {
    let Some(p) = order.price else {
        deny(reasons, DenialCode::PriceMissing, format!("no price for {}", order.symbol));
        return None;
    };
    if !p.price.is_positive() {
        deny(reasons, DenialCode::PriceMissing, format!("price {} for {} is not positive", p.price, order.symbol));
        return None;
    }
    let age_ms = account.now.signed_duration_since(p.as_of).num_milliseconds();
    if age_ms < -MAX_FUTURE_SKEW_SECS.saturating_mul(1000) {
        deny(reasons, DenialCode::PriceStale, format!("price for {} is stamped in the future ({})", order.symbol, p.as_of));
        return None;
    }
    if age_ms > policy.max_price_age_secs.saturating_mul(1000) {
        deny(
            reasons,
            DenialCode::PriceStale,
            format!("price for {} is {} s old; the limit is {} s", order.symbol, age_ms / 1000, policy.max_price_age_secs),
        );
        return None;
    }
    Some(p.price)
}

fn check_universe(limits: &Limits, order: &ProposedOrder, reasons: &mut Vec<Denial>) {
    let sym = order.symbol.trim().to_uppercase();
    if limits.instrument_deny.contains(&sym) {
        deny(reasons, DenialCode::InstrumentDenied, format!("{sym} is on the deny list"));
    } else if !limits.instrument_allow.contains(&sym) {
        deny(reasons, DenialCode::InstrumentNotAllowed, format!("{sym} is not on the allow list"));
    }
    let venue = order.venue.trim().to_lowercase();
    if !limits.venues.contains(&venue) {
        deny(reasons, DenialCode::VenueNotAllowed, format!("venue {venue} is not allowed"));
    }
    let class = order.asset_class.trim().to_lowercase();
    if !limits.asset_classes.contains(&class) {
        deny(reasons, DenialCode::AssetClassNotAllowed, format!("asset class {class} is not allowed"));
    }
}

