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
//! sums (so the guard can deny an order for exposure the sleeves do not manage, even when the planner's own gross
//! check on the targets passed). A pre-existing short is never touched (skipped with a reason), unless a signed
//! sleeve manages that instrument (see the last section).
//!
//! # Trade rules (long-only, the default)
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
//!
//! # Signed sleeves (shorts and gross above 1x): OPT-IN, default OFF
//! Everything above describes the default, long-only unit-weight behaviour, which is unchanged byte for byte (see
//! `tests/long_only_golden.rs`). A sleeve is SIGNED only when the caller says so explicitly, per sleeve, with
//! [`PlanConfig::with_signed_sleeve`]; nothing is inferred from a weight's sign. For a signed sleeve:
//! * the `[0, 1]` weight and `sum <= 1` rules are replaced by `|weight| <= max_abs_weight`, with `max_abs_weight`
//!   in `(0, MAX_ABS_WEIGHT_CAP]` chosen by the caller ([`MAX_ABS_WEIGHT_CAP`] documents why 3). Weights are signed
//!   fractions of the sleeve's capital; a negative target is a short. Share rules (each in (0, 1], sum <= 1) apply
//!   to every sleeve. Targets are rounded toward ZERO (a short is never made bigger by rounding).
//! * the planner refuses the whole plan ([`PlanError::GrossAboveCap`]) when the sum of `|target|` exceeds
//!   `max_gross * capital_base`, and ([`PlanError::BuyingPowerRequired`]) when the targets need margin (any short,
//!   or gross above the broker's equity) and the caller gave no `buying_power`. Whether shorting and leverage are
//!   PERMITTED is the guard's call, not the planner's, and it is decided from these facts (see
//!   [`PreTradeGuard::check_signed`]): a short is denied `SHORTING_FORBIDDEN` when the mandate has `shorting: false`
//!   OR the venue facts for that instrument ([`crate::venue::InstrumentRules`], carried by the
//!   [`VenueRuleBook`](crate::venue::VenueRuleBook)) forbid it, are absent (unknown: fail closed, nothing is assumed
//!   allowed) or need a locate that was not supplied. Leverage is denied `LEVERAGE_FORBIDDEN` only when the projected
//!   GROSS exceeds `min(mandate leverage_max_gross, mandate max_gross, the instrument's venue max_leverage)` times
//!   the capital base; a short by itself is not leverage, so a long/short book within 1x gross needs no leverage
//!   allowance. An order that would take a position above the venue's `max_position_units` is denied `MAX_POSITION`
//!   (never clipped). Denied orders are recorded in `denied`. The planner's own gross check on the targets
//!   ([`PlanError::GrossAboveCap`]) stays the mandate's cap; the venue's per-instrument limits bind in the guard.
//! * `delta = target - held * price` is signed. A trade that moves toward zero is a REDUCTION (a sell of a long, a
//!   buy that covers a short); one that moves away is an INCREASE. Reductions are ordered first, increases second,
//!   each by (venue, symbol): risk is taken off before it is added, on any book.
//! * A trade that CROSSES zero is always two orders, never one: a close leg (reduction, sized to exactly the held
//!   quantity) and an open leg (increase, sized to `|target|` plus whatever the venue's rounding left of the old
//!   position, so the final position never exceeds the target). See [`client_tag`] for their tags. The open leg
//!   is only attempted when the close leg was accepted; if the venue refuses the open leg the account is left flat
//!   (on a whole-share venue: holding only the dust the close leg could not sell).
//! * A held short is no longer skipped (`ShortPositionHeld` remains for long-only instruments).
//! * Increases are limited by the caller's `buying_power` (the broker's own number), not by cash: see
//!   [`PlanConfig::buying_power`]. Without it (a plan that needs no margin) the plain cash rule above applies.
//! * Each order carries `uses_margin`, derived from the account state by [`crate::guard::margin_use`] as
//!   informational data (it denies nothing by itself), and the guard is called through
//!   [`PreTradeGuard::check_signed`] with the rule book's instrument facts.
//! * A signed plan's inputs digest also covers the venue facts of every managed instrument (a long-only plan's
//!   digest is unchanged, and never consults them).

use std::collections::{BTreeMap, BTreeSet};

use broker_adapters::decimal::Rounding;
use broker_adapters::{Dec, Side};
use chrono::{DateTime, Utc};
use reference_rules::{CryptoDecision, EtfDecision};
use sha2::{Digest, Sha256};

use crate::dec_math::{abs, add, div_floor, mul, neg, ratio_to_dec, sub, sum, MathError, RatioRound};
use crate::guard::{
    margin_use, AccountView, DayCounters, Denial, MarginContext, Position, PreTradeGuard, PricePoint, ProposedOrder,
};
use crate::policy::Policy;
use crate::venue::{SizeRefusal, VenueRuleBook};

/// Hard ceiling on a signed sleeve's `max_abs_weight`. The reference FX rule clips every weight to +-3 AFTER its
/// volatility scaling (and its observed maximum was 1.87), so a per-instrument weight above 3 is not a
/// reproduction of any documented rule; it would only be a typo or a runaway scale. The mandate's own gross and
/// leverage caps still bind on top of this, so this constant is a sanity ceiling on the INPUT, never a permission.
pub const MAX_ABS_WEIGHT_CAP: i64 = 3;

/// How a sleeve's weights are bounded. The default for every sleeve is [`WeightBounds::LongOnlyUnit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightBounds {
    /// Today's rules: each weight in `[0, 1]`, weights summing to at most 1. Shorts and leverage are impossible.
    LongOnlyUnit,
    /// Signed: each weight `|w| <= max_abs_weight` (finite, positive, at most [`MAX_ABS_WEIGHT_CAP`]); no sum rule.
    Signed { max_abs_weight: Dec },
}

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
    /// Fraction of the SLEEVE: in [0, 1] for a long-only sleeve; signed, `|w| <= max_abs_weight`, for a signed one
    /// (see [`PlanConfig::with_signed_sleeve`]).
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
    /// Sleeves that have opted in to signed weights, by (trimmed) sleeve id, with their `max_abs_weight`. EMPTY by
    /// default: every sleeve is long-only. Set with [`PlanConfig::with_signed_sleeve`].
    pub signed_sleeves: BTreeMap<String, Dec>,
    /// The broker's own buying power, in the account currency, as of the account snapshot: the total notional of
    /// exposure-INCREASING orders (buys that add long, sells that add short) the broker would accept now, i.e. what
    /// is left after the margin the current positions already use (Alpaca `buying_power`; OANDA
    /// margin-available / margin-rate). It is NOT `cash` and NOT `equity - gross`: only the broker can compute it,
    /// which is why the planner takes it as an input instead of inventing a margin model.
    ///
    /// Used only by a plan with a signed sleeve. Required when that plan needs margin (a short target, or target
    /// gross above the account's equity): its absence is [`PlanError::BuyingPowerRequired`]. When given, it
    /// REPLACES cash as the budget of the increasing orders (`credit_sell_proceeds` is then ignored): the budget
    /// is `buying_power - reserve`, the same reserve fraction of the capital base, and reductions are NOT credited
    /// back (the figure is a snapshot; crediting a reduction would need the broker's margin numbers). A book that
    /// is fully levered therefore rotates over two runs, never by borrowing against sells not yet filled.
    pub buying_power: Option<Dec>,
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
            signed_sleeves: BTreeMap::new(),
            buying_power: None,
        }
    }

    /// Opt ONE sleeve in to signed weights, `|weight| <= max_abs_weight`. Explicit and per sleeve; a sleeve never
    /// named here keeps the long-only unit rules. Validated by the planner (unknown sleeve id, or a bound that is
    /// not in `(0, MAX_ABS_WEIGHT_CAP]`, is an error).
    pub fn with_signed_sleeve(mut self, sleeve: &str, max_abs_weight: Dec) -> Self {
        self.signed_sleeves.insert(sleeve.trim().to_string(), max_abs_weight);
        self
    }

    /// The bounds that apply to a sleeve.
    pub fn bounds_for(&self, sleeve: &str) -> WeightBounds {
        match self.signed_sleeves.get(sleeve.trim()) {
            Some(m) => WeightBounds::Signed { max_abs_weight: *m },
            None => WeightBounds::LongOnlyUnit,
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
    /// No cash above the reserve (in a margin plan: no buying power above the reserve).
    NoCashAvailable,
    /// The open leg of a trade that crosses zero was not attempted because its close leg was not accepted.
    FlipCloseLegNotPlaced,
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
    /// Margin facts of the plan; all zero/false for a plan with no signed sleeve.
    pub margin: MarginReport,
}

/// What the planner derived about margin. `uses_margin` is DATA here (and on every guarded order), not a constant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarginReport {
    /// True when the plan had at least one signed sleeve (so the margin-aware paths ran).
    pub signed: bool,
    /// The TARGETS need margin: a short target, or projected gross above the broker's equity.
    pub needs_margin: bool,
    pub buying_power: Option<Dec>,
    /// Buying power left after the accepted increasing orders (`None` when none was supplied).
    pub buying_power_left: Option<Dec>,
    /// Sum of `|target|` over the managed instruments.
    pub target_gross: Dec,
    /// `target_gross` plus the current value of every position the sleeves do not manage.
    pub projected_gross: Dec,
    /// Tags of the ACCEPTED orders that use margin.
    pub margin_order_tags: Vec<String>,
}

impl MarginReport {
    fn long_only() -> Self {
        Self {
            signed: false,
            needs_margin: false,
            buying_power: None,
            buying_power_left: None,
            target_gross: Dec::ZERO,
            projected_gross: Dec::ZERO,
            margin_order_tags: Vec::new(),
        }
    }
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
    #[error("signed sleeve {0:?} does not name any target sleeve")]
    UnknownSignedSleeve(String),
    #[error("signed sleeve {sleeve:?}: max_abs_weight {max} must be in (0, {cap}]", cap = MAX_ABS_WEIGHT_CAP)]
    BadMaxAbsWeight { sleeve: String, max: Dec },
    #[error("sleeve {sleeve:?}: |weight| of {symbol} exceeds the sleeve's max_abs_weight {max}")]
    BadSignedWeight { sleeve: String, symbol: String, max: Dec },
    #[error("the targets need margin (a short, or gross {gross} above equity) and no buying_power was supplied")]
    BuyingPowerRequired { gross: Dec },
    #[error("target gross {gross} exceeds the mandate's gross cap {cap}")]
    GrossAboveCap { gross: Dec, cap: Dec },
    #[error(transparent)]
    Math(#[from] MathError),
}

pub struct OrderPlanner;

/// Deterministic client tag: `rb1:<scheduled UTC>:<symbol>:<side>:<20 hex of SHA-256>`, where the hash covers the
/// full identity `rebalance:{account}:{scheduled_for}:{sleeve}:{symbol}:{side}`. The hash makes it unique even when
/// the readable parts are shortened, and it stays well under Alpaca's 128 character limit.
///
/// A trade that crosses zero has two legs on the SAME side (a long-to-short flip is a sell to close then a sell to
/// open). The close leg's identity gets a `:close` suffix (see [`client_tag_close_leg`]); the opening leg keeps the
/// plain identity, so re-planning after only the close leg filled produces the very tag the open leg already had
/// (same intent, same tag, never a second order).
pub fn client_tag(account: &str, scheduled_for: DateTime<Utc>, sleeve: &str, symbol: &str, side: Side) -> String {
    tag_with(account, scheduled_for, sleeve, symbol, side, "")
}

/// The tag of the close leg of a trade that crosses zero: [`client_tag`] with a `:close` identity suffix.
pub fn client_tag_close_leg(account: &str, scheduled_for: DateTime<Utc>, sleeve: &str, symbol: &str, side: Side) -> String {
    tag_with(account, scheduled_for, sleeve, symbol, side, ":close")
}

fn tag_with(account: &str, scheduled_for: DateTime<Utc>, sleeve: &str, symbol: &str, side: Side, suffix: &str) -> String {
    let sched = scheduled_for.format("%Y%m%dT%H%M%SZ").to_string();
    let identity = format!("rebalance:{account}:{sched}:{sleeve}:{symbol}:{}{suffix}", side.as_str());
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
    /// True when at least one sleeve that names this instrument is signed.
    signed: bool,
}

impl Managed {
    fn sleeve_label(&self) -> String {
        self.sleeves.iter().cloned().collect::<Vec<_>>().join("+")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    /// The whole trade, in one order.
    Whole,
    /// First order of a trade that crosses zero: closes the held position exactly.
    Close,
    /// Second order of a trade that crosses zero: opens the position on the other side.
    Open,
}

struct Candidate<'a> {
    m: &'a Managed,
    side: Side,
    qty: Dec,
    price: PricePoint,
    /// Moves the position toward zero (a sell of a long, a buy that covers a short). Everything else adds exposure.
    reducing: bool,
    leg: Leg,
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
    if cfg.buying_power.is_some_and(|bp| bp.is_negative()) {
        return Err(PlanError::BadParam("buying_power must not be negative"));
    }
    let hard_cap = Dec::from_i64(MAX_ABS_WEIGHT_CAP);
    for (id, max) in &cfg.signed_sleeves {
        if !targets.iter().any(|t| t.sleeve.trim() == id) {
            return Err(PlanError::UnknownSignedSleeve(id.clone()));
        }
        if *max <= Dec::ZERO || *max > hard_cap {
            return Err(PlanError::BadMaxAbsWeight { sleeve: id.clone(), max: *max });
        }
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
        let bounds = cfg.bounds_for(&t.sleeve);
        for w in &t.weights {
            match bounds {
                WeightBounds::LongOnlyUnit => {
                    if w.weight.is_negative() || w.weight > one() {
                        return Err(PlanError::BadWeight { sleeve: t.sleeve.clone(), symbol: w.symbol.clone() });
                    }
                }
                WeightBounds::Signed { max_abs_weight } => {
                    if abs(w.weight)? > max_abs_weight {
                        return Err(PlanError::BadSignedWeight { sleeve: t.sleeve.clone(), symbol: w.symbol.clone(), max: max_abs_weight });
                    }
                }
            }
            if !symbols.insert(key(&w.symbol)) {
                return Err(PlanError::DuplicateWeight { sleeve: t.sleeve.clone(), symbol: w.symbol.clone() });
            }
            total_weight = add(total_weight, w.weight)?;
        }
        if bounds == WeightBounds::LongOnlyUnit && total_weight > one() {
            return Err(PlanError::WeightsExceedOne(t.sleeve.clone()));
        }
    }
    if total_share > one() {
        return Err(PlanError::SharesExceedOne(total_share));
    }
    Ok(())
}

fn aggregate(targets: &[SleeveTarget], cfg: &PlanConfig) -> Result<BTreeMap<String, Managed>, PlanError> {
    let mut map: BTreeMap<String, Managed> = BTreeMap::new();
    for t in targets {
        let signed = cfg.bounds_for(&t.sleeve) != WeightBounds::LongOnlyUnit;
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
                    m.signed |= signed;
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
                            signed,
                        },
                    );
                }
            }
        }
    }
    Ok(map)
}

/// Round a target notional to 8 decimals TOWARD ZERO: for a long that is the crate's usual floor, for a short it
/// keeps the short from being made bigger by rounding (a plain floor would round it away from zero).
fn round_toward_zero(v: Dec) -> Result<Dec, MathError> {
    let down = |x: Dec| x.round_dp(8, Rounding::Floor).map_err(|_| MathError::Overflow);
    if v.is_negative() {
        neg(down(abs(v)?)?)
    } else {
        down(v)
    }
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
        let managed = aggregate(targets, cfg)?;
        let prices: BTreeMap<String, PricePoint> = prices.iter().map(|(k, v)| (key(k), *v)).collect();
        // Any signed sleeve at all switches the margin-aware paths on for the whole plan (account-level facts).
        let signed_plan = !cfg.signed_sleeves.is_empty();

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
        // Reductions (toward zero) go first, increases second. For a long-only plan these are exactly the sells and
        // the buys.
        let mut reducers: Vec<Candidate<'_>> = Vec::new();
        let mut increasers: Vec<Candidate<'_>> = Vec::new();

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
            if held.is_negative() && !m.signed {
                skipped.push(skip(SkipReason::ShortPositionHeld));
                continue;
            }
            let current = mul(held, price.price)?;
            let target = round_toward_zero(mul(mul(capital_base, m.weight)?, cfg.risk_scale)?)?;
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
            let reference = if target.is_zero() { abs(current)? } else { abs(target)? };
            let pct_min = mul(cfg.min_trade_pct, reference)?;
            if abs_delta < pct_min {
                skipped.push(skip(SkipReason::BelowMinTradePct { delta: abs_delta, min: pct_min }));
                continue;
            }

            // The legs of this trade: (leg, side, wished quantity, reducing). Only a signed instrument can cross zero.
            let held_abs = abs(held)?;
            let crossing = (current.is_positive() && target.is_negative()) || (current.is_negative() && target.is_positive());
            let legs: Vec<(Leg, Side, Dec, bool)> = if crossing {
                let close_side = if held.is_positive() { Side::Sell } else { Side::Buy };
                let open_side = if target.is_positive() { Side::Buy } else { Side::Sell };
                vec![(Leg::Close, close_side, held_abs, true), (Leg::Open, open_side, div_floor(abs(target)?, price.price, 18)?, false)]
            } else {
                let side = if delta.is_positive() { Side::Buy } else { Side::Sell };
                let reduction = (side == Side::Sell && held.is_positive()) || (side == Side::Buy && held.is_negative());
                let wished = if !reduction {
                    div_floor(abs_delta, price.price, 18)?
                } else if target.is_zero() {
                    held_abs
                } else {
                    std::cmp::min(div_floor(abs_delta, price.price, 18)?, held_abs)
                };
                vec![(Leg::Whole, side, wished, reduction)]
            };
            let mut pending: Vec<Candidate<'_>> = Vec::new();
            // What the close leg leaves behind when the venue rounds it down (dust on a whole-share venue).
            let mut residual = Dec::ZERO;
            for (leg, side, wished, reducing) in legs {
                // The open leg has to cover that residual as well, or the book would end short of the target by it:
                // it is sized `|target| / price + residual`, still rounded DOWN, so the final position can never be
                // larger than the target.
                let wished = if leg == Leg::Open { add(wished, residual)? } else { wished };
                let qty = match rules.round_quantity(&m.symbol, side, wished, price.price) {
                    Ok(q) => q,
                    Err(refusal) => {
                        skipped.push(skip(SkipReason::VenueRefused(refusal)));
                        if leg == Leg::Close {
                            // Cannot close: do not open the other side either.
                            pending.clear();
                            break;
                        }
                        continue;
                    }
                };
                if qty > wished {
                    return Err(PlanError::VenueRoundedUp { symbol: m.symbol.clone(), wished, returned: qty });
                }
                if leg == Leg::Close {
                    residual = sub(wished, qty)?;
                }
                pending.push(Candidate { m, side, qty, price, reducing, leg });
            }
            for cand in pending {
                if cand.reducing {
                    reducers.push(cand);
                } else {
                    increasers.push(cand);
                }
            }
        }

        // Signed plans: gross cap on the targets, and margin need vs the buying power the caller supplied.
        let mut margin = MarginReport::long_only();
        if signed_plan {
            let mut target_gross = Dec::ZERO;
            let mut named = BTreeSet::new();
            for l in &lines {
                target_gross = add(target_gross, abs(l.target_notional)?)?;
                named.insert(key(&l.symbol));
            }
            let mut projected = target_gross;
            for p in &sim.positions {
                if !named.contains(&key(&p.symbol)) {
                    projected = add(projected, abs(p.market_value)?)?;
                }
            }
            if let Some(limits) = policy.limits() {
                let cap = mul(limits.max_gross, capital_base)?;
                if target_gross > cap {
                    return Err(PlanError::GrossAboveCap { gross: target_gross, cap });
                }
            }
            let needs_margin = lines.iter().any(|l| l.target_notional.is_negative()) || projected > equity;
            if needs_margin && cfg.buying_power.is_none() {
                return Err(PlanError::BuyingPowerRequired { gross: projected });
            }
            margin = MarginReport {
                signed: true,
                needs_margin,
                buying_power: cfg.buying_power,
                buying_power_left: cfg.buying_power,
                target_gross,
                projected_gross: projected,
                margin_order_tags: Vec::new(),
            };
        }

        let by_venue_symbol = |a: &Candidate<'_>, b: &Candidate<'_>| (&a.m.venue, &a.m.symbol).cmp(&(&b.m.venue, &b.m.symbol));
        reducers.sort_by(by_venue_symbol);
        increasers.sort_by(by_venue_symbol);

        let mut orders: Vec<PlannedOrder> = Vec::new();
        let mut denied: Vec<DeniedOrder> = Vec::new();
        let mut bp_left = margin.buying_power_left;
        let mut margin_tags: Vec<String> = Vec::new();
        let mut closed: BTreeSet<String> = BTreeSet::new();
        let mut try_order = |c: &Candidate<'_>, qty: Dec, sim: &mut AccountView, day: &mut DayCounters| -> Result<bool, PlanError> {
            let notional = mul(qty, c.price.price)?;
            let fee = fee_for(notional, cfg.fee_rate)?;
            let mut proposed = ProposedOrder {
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
            let verdict = if signed_plan {
                // Margin use is DATA derived from the simulated account, not a constant.
                proposed.uses_margin = margin_use(sim, &proposed, c.price.price)?;
                PreTradeGuard::check_signed(
                    policy,
                    sim,
                    &proposed,
                    day,
                    &MarginContext { buying_power_left: bp_left },
                    venue_rules.instrument_rules(),
                )
            } else {
                PreTradeGuard::check(policy, sim, &proposed, day)
            };
            let sleeve = c.m.sleeve_label();
            let tag = if c.leg == Leg::Close {
                client_tag_close_leg(&account.account_id, cfg.scheduled_for, &sleeve, &c.m.symbol, c.side)
            } else {
                client_tag(&account.account_id, cfg.scheduled_for, &sleeve, &c.m.symbol, c.side)
            };
            let planned = PlannedOrder {
                tag,
                sleeve,
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
                if let Some(bp) = bp_left {
                    if !c.reducing {
                        bp_left = Some(sub(sub(bp, notional)?, fee)?);
                    }
                }
                if proposed.uses_margin {
                    margin_tags.push(planned.tag.clone());
                }
                orders.push(planned);
                Ok(true)
            } else {
                denied.push(DeniedOrder { order: planned, reasons: verdict.reasons });
                Ok(false)
            }
        };

        // Reductions first. Never more than held: `qty <= wished <= |held|` by construction.
        for c in &reducers {
            let placed = try_order(c, c.qty, &mut sim, &mut day)?;
            if placed && c.leg == Leg::Close {
                closed.insert(c.m.symbol.clone());
            }
        }

        // The open leg of a trade that crosses zero only follows an accepted close leg.
        let mut buys: Vec<Candidate<'_>> = Vec::new();
        for c in increasers {
            if c.leg == Leg::Open && !closed.contains(&c.m.symbol) {
                skipped.push(SkippedTrade { symbol: c.m.symbol.clone(), sleeve: c.m.sleeve_label(), reason: SkipReason::FlipCloseLegNotPlaced });
            } else {
                buys.push(c);
            }
        }

        // Increases: limited by cash after the reserve and fees, or by the broker's buying power when the caller
        // supplied it for a signed plan.
        let reserve = policy.reserve_amount(capital_base)?;
        let budget = match (signed_plan, cfg.buying_power) {
            (true, Some(bp)) => bp,
            _ if cfg.credit_sell_proceeds => sim.cash,
            _ => std::cmp::min(sim.cash, account.cash),
        };
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
                // One common factor for every increasing order, computed against a budget shrunk by the per-order fee
                // rounding slack (each fee is rounded UP by at most 1e-8), so the re-rounded total provably fits.
                let slack = mul(Dec::new(1, 8).map_err(|_| MathError::Overflow)?, Dec::from_i64(i64::try_from(buys.len()).unwrap_or(i64::MAX)))?;
                let usable = sub(available, slack)?;
                let factor = if usable.is_positive() { div_floor(usable, total, 18)? } else { Dec::ZERO };
                for (c, q) in buys.iter().zip(buy_qtys.iter_mut()) {
                    let scaled = mul(*q, factor)?.round_dp(18, Rounding::Floor).map_err(|_| MathError::Overflow)?;
                    if scaled.is_zero() {
                        *q = Dec::ZERO;
                        continue;
                    }
                    match venue_rules.get(&c.m.venue).map(|r| r.round_quantity(&c.m.symbol, c.side, scaled, c.price.price)) {
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

        margin.buying_power_left = bp_left;
        margin.margin_order_tags = margin_tags;
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
            margin,
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
    // Only a plan with a signed sleeve carries these lines, so a long-only plan's digest is unchanged.
    if !cfg.signed_sleeves.is_empty() {
        for (id, max) in &cfg.signed_sleeves {
            lines.push(format!("signed|{id}|{}", dtxt(*max)));
        }
        lines.push(format!("buying_power|{}", cfg.buying_power.map_or("-".to_string(), dtxt)));
        for m in managed.values() {
            lines.push(format!("instrument_rule|{}|{}|{}", m.venue, m.symbol, venue_rules.instrument_rules().fingerprint(&m.venue, &m.symbol)));
        }
    }
    for m in managed.values() {
        if let Some(r) = venue_rules.get(&m.venue) {
            lines.push(format!("rules|{}", r.fingerprint(&m.symbol)));
        }
    }
    hex::encode(Sha256::digest(lines.join("\n").as_bytes()))
}
