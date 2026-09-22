//! Order planner: sleeve targets in, a sells-first list of guarded, idempotently tagged orders out.
//!
//! Pure and deterministic: the same inputs give an identical plan (same orders, same tags, same digest), whatever
//! the order of the input slices. Every quantity is an exact decimal; the only `f64` that ever enters is the
//! reference-rules weights, converted once in [`SleeveTarget::from_etf`] / [`SleeveTarget::from_crypto`] (rounded
//! DOWN, so a weight can never be inflated) and the mandate ratios converted once in `Policy::compile`.
//!
//! # How the pieces compose
//! For each instrument named by any sleeve:
//! `target_notional = capital_base * ( sum over sleeves of share_s * weight_s,i ) * risk_scale`
//! * `share_s`: the sleeve's fraction of capital, in (0, 1], all shares summing to at most 1.
//! * `weight_s,i`: the reference rule's fraction OF THE SLEEVE (0.20 / 0.50 or 0).
//! * `risk_scale` in (0, 1]: one caller-supplied multiplier on every target (the drawdown ladder's shrink), applied
//!   AFTER the sleeve share. It multiplies the target, so a lower scale can only lower the targets.
//! * `capital_base = Policy::capital_base(equity)` = `min(equity, mandate allocated capital)` when the mandate
//!   currency matches the account's, else `equity`. Equity is the BROKER's number. The guard uses the very same
//!   value as the denominator of its percentage limits, so plan and guard agree.
//!
//! Instruments the sleeves do not name are left alone (unmanaged), though they still count in the guard's exposure
//! sums. A pre-existing short is never touched (skipped with a reason).
//!
//! # Trade rules
//! * `delta = target - held * price`. Trades with `|delta| < min_trade_abs`, or `|delta| < min_trade_pct * target`
//!   (`* current` when the target is zero), are dropped (recorded in `skipped`).
//! * Sizes are `floor(|delta| / price)` then rounded DOWN by the venue rules. A full exit (target zero) sells the
//!   whole held quantity. A sell never exceeds the held quantity. Venue refusals (minimums, unknown instrument) are
//!   recorded, never bumped up.
//! * Buys are limited by cash: `available = cash - reserve` where `reserve = min_cash_reserve * capital_base`
//!   (rounded up; `cash` is the broker's actual cash)
//!   and, when `credit_sell_proceeds` (default), cash includes the net proceeds of the sells the guard accepted.
//!   Each buy costs `notional + fee`, with `fee = ceil8(notional * fee_rate)`. If the buys do not fit, ALL buys are
//!   scaled by one common factor (so the outcome does not depend on order) and re-rounded down.
//! * The plan is ordered sells first, then buys; within a side by (venue, symbol).
//! * Every order goes through [`PreTradeGuard::check`] against a running simulation of the account (positions,
//!   cash, day counters updated by each accepted order). Denied orders are dropped and recorded with their reasons.
//!   The planner never clamps an order to make it pass.

use std::collections::{BTreeMap, BTreeSet};

use broker_adapters::decimal::Rounding;
use broker_adapters::{Dec, Side};
use chrono::{DateTime, Utc};
use reference_rules::{CryptoDecision, EtfDecision};
use sha2::{Digest, Sha256};

use crate::dec_math::{abs, add, div_floor, mul, neg, ratio_to_dec, sub, sum, MathError, RatioRound};
use crate::guard::{
    AccountView, DayCounters, Denial, Position, PreTradeGuard, PricePoint, ProposedOrder,
};
use crate::policy::Policy;
use crate::venue::{SizeRefusal, VenueRuleBook};

/// One sleeve's target: which instruments, at what fraction of the sleeve, on which venue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleeveTarget {
    pub sleeve: String,
    /// Fraction of capital allocated to this sleeve, in (0, 1].
    pub share: Dec,
    pub venue: String,
    pub asset_class: String,
    pub weights: Vec<TargetWeight>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetWeight {
    /// Tradable symbol on the venue (`SPY`, `BTC/USD`).
    pub symbol: String,
    /// Fraction of the SLEEVE, in [0, 1].
    pub weight: Dec,
}

impl SleeveTarget {
    /// From the ETF trend decision. Weights are converted from `f64` once, rounded down.
    pub fn from_etf(
        sleeve: &str,
        share: Dec,
        venue: &str,
        asset_class: &str,
        d: &EtfDecision,
    ) -> Result<Self, MathError> {
        let mut weights = Vec::new();
        for i in &d.instruments {
            weights.push(TargetWeight { symbol: i.symbol.clone(), weight: ratio_to_dec(i.weight, RatioRound::Down)? });
        }
        Ok(Self { sleeve: sleeve.to_string(), share, venue: venue.to_string(), asset_class: asset_class.to_string(), weights })
    }

    /// From the crypto trend decision; `BTC` becomes `BTC/<quote>`.
    pub fn from_crypto(
        sleeve: &str,
        share: Dec,
        venue: &str,
        asset_class: &str,
        quote: &str,
        d: &CryptoDecision,
    ) -> Result<Self, MathError> {
        let mut weights = Vec::new();
        for i in &d.instruments {
            weights.push(TargetWeight {
                symbol: format!("{}/{}", i.symbol, quote),
                weight: ratio_to_dec(i.weight, RatioRound::Down)?,
            });
        }
        Ok(Self { sleeve: sleeve.to_string(), share, venue: venue.to_string(), asset_class: asset_class.to_string(), weights })
    }
}

/// Parameters of one planning run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanConfig {
    /// Idempotency anchor: part of every order tag.
    pub scheduled_for: DateTime<Utc>,
    /// Multiplier on every target, in (0, 1].
    pub risk_scale: Dec,
    /// Drop trades smaller than this many account-currency units.
    pub min_trade_abs: Dec,
    /// Drop trades smaller than this fraction of the target (of the current value when the target is zero), in [0, 1).
    pub min_trade_pct: Dec,
    /// Fee as a fraction of notional, charged per order, in [0, 1). No default: it is a venue fact the caller owns.
    pub fee_rate: Dec,
    /// Count the net proceeds of accepted sells as cash available for the buys of the same plan.
    pub credit_sell_proceeds: bool,
    /// Orders already placed / turnover already traded today.
    pub day: DayCounters,
}

impl PlanConfig {
    pub fn new(scheduled_for: DateTime<Utc>, risk_scale: Dec, min_trade_abs: Dec, min_trade_pct: Dec, fee_rate: Dec) -> Self {
        Self {
            scheduled_for,
            risk_scale,
            min_trade_abs,
            min_trade_pct,
            fee_rate,
            credit_sell_proceeds: true,
            day: DayCounters::ZERO,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedOrder {
    /// Deterministic idempotency tag, see [`client_tag`].
    pub tag: String,
    /// Sleeve id, or the sorted ids joined by `+` when several sleeves want the instrument.
    pub sleeve: String,
    pub venue: String,
    pub asset_class: String,
    pub symbol: String,
    pub side: Side,
    /// Already rounded DOWN by the venue rules.
    pub quantity: Dec,
    /// Reference price the order was sized and checked at.
    pub price: Dec,
    pub notional: Dec,
    pub est_fee: Dec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeniedOrder {
    pub order: PlannedOrder,
    pub reasons: Vec<Denial>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    NoPrice,
    NoVenueRules,
    ShortPositionHeld,
    BelowMinTradeAbs { delta: Dec, min: Dec },
    BelowMinTradePct { delta: Dec, min: Dec },
    VenueRefused(SizeRefusal),
    /// The buy did not fit the cash left after the reserve and fees; scaled down, it fell below the venue minimum.
    CutBelowVenueMinimum,
    /// No cash above the reserve.
    NoCashAvailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedTrade {
    pub symbol: String,
    pub sleeve: String,
    pub reason: SkipReason,
}

/// What the planner saw for one managed instrument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentLine {
    pub symbol: String,
    pub sleeve: String,
    pub held: Dec,
    pub current_notional: Dec,
    pub target_notional: Dec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderPlan {
    /// SHA-256 (hex) of the canonical text of every input; same inputs, same digest, any input ordering.
    pub inputs_digest: String,
    pub account_id: String,
    pub scheduled_for: DateTime<Utc>,
    pub equity: Dec,
    pub capital_base: Dec,
    pub risk_scale: Dec,
    /// Sells first, then buys; within a side by (venue, symbol).
    pub orders: Vec<PlannedOrder>,
    pub denied: Vec<DeniedOrder>,
    pub skipped: Vec<SkippedTrade>,
    pub lines: Vec<InstrumentLine>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("risk_scale must be in (0, 1], got {0}")]
    BadRiskScale(Dec),
    #[error("bad parameter: {0}")]
    BadParam(&'static str),
    #[error("sleeve {0:?}: share must be in (0, 1]")]
    BadShare(String),
    #[error("sleeve shares sum to {0}, more than 1")]
    SharesExceedOne(Dec),
    #[error("sleeve id {0:?} appears twice")]
    DuplicateSleeve(String),
    #[error("sleeve {sleeve:?}: weight of {symbol} must be in [0, 1]")]
    BadWeight { sleeve: String, symbol: String },
    #[error("sleeve {0:?}: weights sum to more than 1")]
    WeightsExceedOne(String),
    #[error("sleeve {sleeve:?} lists {symbol} twice")]
    DuplicateWeight { sleeve: String, symbol: String },
    #[error("{0} is given different venues or asset classes by different sleeves")]
    InstrumentConflict(String),
    #[error("account equity {0} is not positive")]
    EquityInvalid(Dec),
    #[error("venue rules rounded {symbol} UP ({wished} -> {returned}); rules must only round down")]
    VenueRoundedUp { symbol: String, wished: Dec, returned: Dec },
    #[error("two orders got the same tag {0}")]
    DuplicateTag(String),
    #[error(transparent)]
    Math(#[from] MathError),
}

pub struct OrderPlanner;

/// Deterministic client tag: `rb1:<scheduled UTC>:<symbol>:<side>:<20 hex of SHA-256>`, where the hash covers the
/// full identity `rebalance:{account}:{scheduled_for}:{sleeve}:{symbol}:{side}`. The hash makes it unique even when
/// the readable parts are shortened, and it stays well under Alpaca's 128 character limit.
pub fn client_tag(account: &str, scheduled_for: DateTime<Utc>, sleeve: &str, symbol: &str, side: Side) -> String {
    let sched = scheduled_for.format("%Y%m%dT%H%M%SZ").to_string();
    let identity = format!("rebalance:{account}:{sched}:{sleeve}:{symbol}:{}", side.as_str());
    let hash = hex::encode(Sha256::digest(identity.as_bytes()));
    let readable: String = symbol.chars().filter(char::is_ascii_alphanumeric).take(10).collect::<String>().to_uppercase();
    format!("rb1:{sched}:{readable}:{}:{}", side.as_str(), &hash[..20])
}

struct Managed {
    symbol: String,
    venue: String,
    asset_class: String,
    sleeves: BTreeSet<String>,
    /// Sum over sleeves of share * weight, exact.
    weight: Dec,
}

impl Managed {
    fn sleeve_label(&self) -> String {
        self.sleeves.iter().cloned().collect::<Vec<_>>().join("+")
    }
}

struct Candidate<'a> {
    m: &'a Managed,
    side: Side,
    qty: Dec,
    price: PricePoint,
}

fn one() -> Dec {
    Dec::from_i64(1)
}

fn key(s: &str) -> String {
    s.trim().to_uppercase()
}

fn fee_for(notional: Dec, rate: Dec) -> Result<Dec, MathError> {
    mul(notional, rate)?.round_dp(8, Rounding::Ceil).map_err(|_| MathError::Overflow)
}

fn dtxt(d: Dec) -> String {
    d.normalized().to_string()
}

fn validate(targets: &[SleeveTarget], account: &AccountView, cfg: &PlanConfig) -> Result<(), PlanError> {
    if cfg.risk_scale <= Dec::ZERO || cfg.risk_scale > one() {
        return Err(PlanError::BadRiskScale(cfg.risk_scale));
    }
    if cfg.min_trade_abs.is_negative() {
        return Err(PlanError::BadParam("min_trade_abs must not be negative"));
    }
    if cfg.min_trade_pct.is_negative() || cfg.min_trade_pct >= one() {
        return Err(PlanError::BadParam("min_trade_pct must be in [0, 1)"));
    }
    if cfg.fee_rate.is_negative() || cfg.fee_rate >= one() {
        return Err(PlanError::BadParam("fee_rate must be in [0, 1)"));
    }
    if !account.equity.is_positive() {
        return Err(PlanError::EquityInvalid(account.equity));
    }
    let mut seen = BTreeSet::new();
    let mut total_share = Dec::ZERO;
    for t in targets {
        if !seen.insert(t.sleeve.trim().to_string()) {
            return Err(PlanError::DuplicateSleeve(t.sleeve.clone()));
        }
        if t.share <= Dec::ZERO || t.share > one() {
            return Err(PlanError::BadShare(t.sleeve.clone()));
        }
        total_share = add(total_share, t.share)?;
        let mut symbols = BTreeSet::new();
        let mut total_weight = Dec::ZERO;
        for w in &t.weights {
            if w.weight.is_negative() || w.weight > one() {
                return Err(PlanError::BadWeight { sleeve: t.sleeve.clone(), symbol: w.symbol.clone() });
            }
            if !symbols.insert(key(&w.symbol)) {
                return Err(PlanError::DuplicateWeight { sleeve: t.sleeve.clone(), symbol: w.symbol.clone() });
            }
            total_weight = add(total_weight, w.weight)?;
        }
        if total_weight > one() {
            return Err(PlanError::WeightsExceedOne(t.sleeve.clone()));
        }
    }
    if total_share > one() {
        return Err(PlanError::SharesExceedOne(total_share));
    }
    Ok(())
}

fn aggregate(targets: &[SleeveTarget]) -> Result<BTreeMap<String, Managed>, PlanError> {
    let mut map: BTreeMap<String, Managed> = BTreeMap::new();
    for t in targets {
        for w in &t.weights {
            let k = key(&w.symbol);
            let contribution = mul(t.share, w.weight)?;
            let venue = t.venue.trim().to_lowercase();
            let class = t.asset_class.trim().to_lowercase();
            match map.get_mut(&k) {
                Some(m) => {
                    if m.venue != venue || m.asset_class != class {
                        return Err(PlanError::InstrumentConflict(k));
                    }
                    m.weight = add(m.weight, contribution)?;
                    m.sleeves.insert(t.sleeve.trim().to_string());
                }
                None => {
                    map.insert(
                        k.clone(),
                        Managed {
                            symbol: k,
                            venue,
                            asset_class: class,
                            sleeves: BTreeSet::from([t.sleeve.trim().to_string()]),
                            weight: contribution,
                        },
                    );
                }
            }
        }
    }
    Ok(map)
}

/// Apply an accepted order to the simulated account and the day counters.
fn apply(sim: &mut AccountView, day: &mut DayCounters, m: &Managed, side: Side, qty: Dec, price: Dec, fee: Dec) -> Result<(), MathError> {
    let notional = mul(qty, price)?;
    let (signed_qty, signed_value) = match side {
        Side::Buy => (qty, notional),
        Side::Sell => (neg(qty)?, neg(notional)?),
    };
    match sim.positions.iter_mut().find(|p| p.symbol.eq_ignore_ascii_case(&m.symbol)) {
        Some(p) => {
            p.quantity = add(p.quantity, signed_qty)?;
            p.market_value = add(p.market_value, signed_value)?;
        }
        None => sim.positions.push(Position {
            symbol: m.symbol.clone(),
            venue: m.venue.clone(),
            asset_class: m.asset_class.clone(),
            quantity: signed_qty,
            market_value: signed_value,
        }),
    }
    sim.cash = sub(sub(sim.cash, signed_value)?, fee)?;
    day.orders_today = day.orders_today.saturating_add(1);
    day.turnover_today = add(day.turnover_today, notional)?;
    Ok(())
}

impl OrderPlanner {
    pub fn plan(
        targets: &[SleeveTarget],
        account: &AccountView,
        prices: &BTreeMap<String, PricePoint>,
        venue_rules: &VenueRuleBook<'_>,
        policy: &Policy,
        cfg: &PlanConfig,
    ) -> Result<OrderPlan, PlanError> {
        validate(targets, account, cfg)?;
        let managed = aggregate(targets)?;
        let prices: BTreeMap<String, PricePoint> = prices.iter().map(|(k, v)| (key(k), *v)).collect();

        let equity = account.equity;
        // The same capital base the guard uses as the denominator of its percentage limits.
        let capital_base = policy.capital_base(equity, &account.ccy);

        // The simulated account: positions valued at the plan's own prices where it has one.
        let mut sim = account.clone();
        for p in &mut sim.positions {
            if let Some(px) = prices.get(&key(&p.symbol)) {
                p.market_value = mul(p.quantity, px.price)?;
            }
        }
        let mut day = cfg.day;

        let mut lines = Vec::new();
        let mut skipped = Vec::new();
        let mut sells: Vec<Candidate<'_>> = Vec::new();
        let mut buys: Vec<Candidate<'_>> = Vec::new();

        for m in managed.values() {
            let skip = |reason: SkipReason| SkippedTrade { symbol: m.symbol.clone(), sleeve: m.sleeve_label(), reason };
            let Some(price) = prices.get(&m.symbol).copied().filter(|p| p.price.is_positive()) else {
                skipped.push(skip(SkipReason::NoPrice));
                continue;
            };
            let Some(rules) = venue_rules.get(&m.venue) else {
                skipped.push(skip(SkipReason::NoVenueRules));
                continue;
            };
            let held = account.position(&m.symbol).map_or(Dec::ZERO, |p| p.quantity);
            if held.is_negative() {
                skipped.push(skip(SkipReason::ShortPositionHeld));
                continue;
            }
            let current = mul(held, price.price)?;
            let target = mul(mul(capital_base, m.weight)?, cfg.risk_scale)?
                .round_dp(8, Rounding::Floor)
                .map_err(|_| MathError::Overflow)?;
            lines.push(InstrumentLine {
                symbol: m.symbol.clone(),
                sleeve: m.sleeve_label(),
                held,
                current_notional: current,
                target_notional: target,
            });
            let delta = sub(target, current)?;
            if delta.is_zero() {
                continue;
            }
            let abs_delta = abs(delta)?;
            if abs_delta < cfg.min_trade_abs {
                skipped.push(skip(SkipReason::BelowMinTradeAbs { delta: abs_delta, min: cfg.min_trade_abs }));
                continue;
            }
            let reference = if target.is_positive() { target } else { current };
            let pct_min = mul(cfg.min_trade_pct, reference)?;
            if abs_delta < pct_min {
                skipped.push(skip(SkipReason::BelowMinTradePct { delta: abs_delta, min: pct_min }));
                continue;
            }
            let (side, wished) = if delta.is_positive() {
                (Side::Buy, div_floor(abs_delta, price.price, 18)?)
            } else if target.is_zero() {
                (Side::Sell, held)
            } else {
                (Side::Sell, std::cmp::min(div_floor(abs_delta, price.price, 18)?, held))
            };
            let qty = match rules.round_quantity(&m.symbol, side, wished, price.price) {
                Ok(q) => q,
                Err(refusal) => {
                    skipped.push(skip(SkipReason::VenueRefused(refusal)));
                    continue;
                }
            };
            if qty > wished {
                return Err(PlanError::VenueRoundedUp { symbol: m.symbol.clone(), wished, returned: qty });
            }
            let cand = Candidate { m, side, qty, price };
            match side {
                Side::Sell => sells.push(cand),
                Side::Buy => buys.push(cand),
            }
        }

        let by_venue_symbol = |a: &Candidate<'_>, b: &Candidate<'_>| (&a.m.venue, &a.m.symbol).cmp(&(&b.m.venue, &b.m.symbol));
        sells.sort_by(by_venue_symbol);
        buys.sort_by(by_venue_symbol);

        let mut orders: Vec<PlannedOrder> = Vec::new();
        let mut denied: Vec<DeniedOrder> = Vec::new();
        let mut try_order = |c: &Candidate<'_>, qty: Dec, sim: &mut AccountView, day: &mut DayCounters| -> Result<(), PlanError> {
            let notional = mul(qty, c.price.price)?;
            let fee = fee_for(notional, cfg.fee_rate)?;
            let proposed = ProposedOrder {
                venue: c.m.venue.clone(),
                asset_class: c.m.asset_class.clone(),
                symbol: c.m.symbol.clone(),
                side: c.side,
                quantity: qty,
                price: Some(c.price),
                est_fee: fee,
                uses_margin: false,
                is_derivative: false,
            };
            let verdict = PreTradeGuard::check(policy, sim, &proposed, day);
            let planned = PlannedOrder {
                tag: client_tag(&account.account_id, cfg.scheduled_for, &c.m.sleeve_label(), &c.m.symbol, c.side),
                sleeve: c.m.sleeve_label(),
                venue: c.m.venue.clone(),
                asset_class: c.m.asset_class.clone(),
                symbol: c.m.symbol.clone(),
                side: c.side,
                quantity: qty,
                price: c.price.price,
                notional,
                est_fee: fee,
            };
            if verdict.allow {
                apply(sim, day, c.m, c.side, qty, c.price.price, fee)?;
                orders.push(planned);
            } else {
                denied.push(DeniedOrder { order: planned, reasons: verdict.reasons });
            }
            Ok(())
        };

        // Sells first. Never more than held: `qty <= wished <= held` by construction.
        for c in &sells {
            try_order(c, c.qty, &mut sim, &mut day)?;
        }

        // Buys: limited by cash after the reserve and fees.
        let reserve = policy.reserve_amount(capital_base)?;
        let budget = if cfg.credit_sell_proceeds { sim.cash } else { std::cmp::min(sim.cash, account.cash) };
        let available = sub(budget, reserve)?;
        let mut buy_qtys: Vec<Dec> = buys.iter().map(|c| c.qty).collect();
        if !buys.is_empty() {
            let mut costs = Vec::new();
            for (c, q) in buys.iter().zip(&buy_qtys) {
                let n = mul(*q, c.price.price)?;
                costs.push(add(n, fee_for(n, cfg.fee_rate)?)?);
            }
            let total = sum(costs.iter().copied())?;
            if total > available {
                // One common factor for every buy, computed against a budget shrunk by the per-order fee rounding
                // slack (each fee is rounded UP by at most 1e-8), so the re-rounded total provably fits.
                let slack = mul(Dec::new(1, 8).map_err(|_| MathError::Overflow)?, Dec::from_i64(i64::try_from(buys.len()).unwrap_or(i64::MAX)))?;
                let usable = sub(available, slack)?;
                let factor = if usable.is_positive() { div_floor(usable, total, 18)? } else { Dec::ZERO };
                for (c, q) in buys.iter().zip(buy_qtys.iter_mut()) {
                    let scaled = mul(*q, factor)?.round_dp(18, Rounding::Floor).map_err(|_| MathError::Overflow)?;
                    if scaled.is_zero() {
                        *q = Dec::ZERO;
                        continue;
                    }
                    match venue_rules.get(&c.m.venue).map(|r| r.round_quantity(&c.m.symbol, Side::Buy, scaled, c.price.price)) {
                        Some(Ok(rounded)) if rounded <= scaled => *q = rounded,
                        Some(Ok(rounded)) => {
                            return Err(PlanError::VenueRoundedUp { symbol: c.m.symbol.clone(), wished: scaled, returned: rounded })
                        }
                        _ => *q = Dec::ZERO,
                    }
                }
            }
        }
        for (c, q) in buys.iter().zip(&buy_qtys) {
            if q.is_zero() {
                let reason = if available.is_positive() { SkipReason::CutBelowVenueMinimum } else { SkipReason::NoCashAvailable };
                skipped.push(SkippedTrade { symbol: c.m.symbol.clone(), sleeve: c.m.sleeve_label(), reason });
                continue;
            }
            try_order(c, *q, &mut sim, &mut day)?;
        }

        let mut tags = BTreeSet::new();
        for o in orders.iter().chain(denied.iter().map(|d| &d.order)) {
            if !tags.insert(o.tag.clone()) {
                return Err(PlanError::DuplicateTag(o.tag.clone()));
            }
        }

        let inputs_digest = digest_inputs(targets, account, &prices, venue_rules, policy, cfg, &managed);
        Ok(OrderPlan {
            inputs_digest,
            account_id: account.account_id.clone(),
            scheduled_for: cfg.scheduled_for,
            equity,
            capital_base,
            risk_scale: cfg.risk_scale,
            orders,
            denied,
            skipped,
            lines,
        })
    }
}

/// Canonical text of every input, order-insensitive (everything is sorted, decimals normalised), hashed.
fn digest_inputs(
    targets: &[SleeveTarget],
    account: &AccountView,
    prices: &BTreeMap<String, PricePoint>,
    venue_rules: &VenueRuleBook<'_>,
    policy: &Policy,
    cfg: &PlanConfig,
    managed: &BTreeMap<String, Managed>,
) -> String {
    let mut lines: Vec<String> = vec!["rb-plan-digest-v1".to_string()];
    lines.push(format!(
        "account|{}|{}|{}|{}|{}|{}",
        account.account_id,
        account.ccy,
        dtxt(account.equity),
        dtxt(account.cash),
        account.halted,
        account.now.to_rfc3339()
    ));
    let mut positions: Vec<String> = account
        .positions
        .iter()
        .map(|p| {
            format!("pos|{}|{}|{}|{}|{}", key(&p.symbol), p.venue.to_lowercase(), p.asset_class.to_lowercase(), dtxt(p.quantity), dtxt(p.market_value))
        })
        .collect();
    positions.sort();
    lines.extend(positions);
    for (sym, p) in prices {
        lines.push(format!("price|{sym}|{}|{}", dtxt(p.price), p.as_of.to_rfc3339()));
    }
    let mut ts: Vec<String> = targets
        .iter()
        .map(|t| {
            let mut ws: Vec<String> = t.weights.iter().map(|w| format!("{}={}", key(&w.symbol), dtxt(w.weight))).collect();
            ws.sort();
            format!(
                "target|{}|{}|{}|{}|{}",
                t.sleeve.trim(),
                dtxt(t.share),
                t.venue.trim().to_lowercase(),
                t.asset_class.trim().to_lowercase(),
                ws.join(",")
            )
        })
        .collect();
    ts.sort();
    lines.extend(ts);
    lines.push(format!(
        "policy|{}|{:?}|{}",
        policy.body_hash,
        policy.envelope.as_ref().map(|e| (e.version, e.status, e.effective_from.to_rfc3339(), e.review_by.to_rfc3339())),
        policy.max_price_age_secs
    ));
    lines.push(format!(
        "cfg|{}|{}|{}|{}|{}|{}|{}|{}",
        cfg.scheduled_for.to_rfc3339(),
        dtxt(cfg.risk_scale),
        dtxt(cfg.min_trade_abs),
        dtxt(cfg.min_trade_pct),
        dtxt(cfg.fee_rate),
        cfg.credit_sell_proceeds,
        cfg.day.orders_today,
        dtxt(cfg.day.turnover_today)
    ));
    for m in managed.values() {
        if let Some(r) = venue_rules.get(&m.venue) {
            lines.push(format!("rules|{}", r.fingerprint(&m.symbol)));
        }
    }
    hex::encode(Sha256::digest(lines.join("\n").as_bytes()))
}
