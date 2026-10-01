//! The risk overlay: the drawdown ladder and the daily-loss limit, evaluated on BROKER-reported equity.
//!
//! [`evaluate_risk`] is a pure function `(state, equity snapshot, risk policy) -> RiskDecision`; [`apply_decision`]
//! turns a decision into the next [`AccountState`]. Nothing here reads a clock, a fill or a database.
//!
//! # Rules (SPEC section 2 and 3, tests B4, B5, B7)
//! * **Equity source.** The only input is an [`EquitySnapshot`], whose only constructor is
//!   [`EquitySnapshot::broker_reported`]: equity is what the BROKER says, never something derived from our own fills,
//!   so a bug in our bookkeeping cannot hide a loss (B7).
//! * **Drawdown** is measured from the high-water mark: `loss = hwm - equity`, and rung `at` (a fraction) is hit when
//!   `loss >= at * hwm`. The comparison is exact decimal arithmetic and is INCLUSIVE: equity exactly at the trigger
//!   triggers; one unit above does not. The product `at * hwm` is floored at 18 decimals, which can only make a
//!   trigger fire a hair EARLIER (stricter), never later.
//! * **Ladder.** Rungs are ascending. The highest rung hit decides: a `shrink` rung scales every target by its
//!   `scale`; the last rung (`halt_flatten`) halts and flattens.
//! * **Daily loss.** `day_start - equity >= limit * day_start` (inclusive) halts and flattens, whatever the
//!   drawdown. Day-start is the equity at the first observation of the account-local trading day the caller supplies.
//! * **Recovery from a shrink (hysteresis, a documented parameter).** A shrink rung is released only when the
//!   drawdown has recovered to within `recovery_fraction` of that rung's trigger: `loss <= at * recovery_fraction *
//!   hwm`. The default is 0.5 ("recover to within half of the rung"): with a 10% shrink rung the account returns to
//!   full size only when the drawdown is back to 5% or less. This stops the account flapping between full and half
//!   size around the rung. With several shrink rungs the account steps down one rung at a time, each release
//!   needing its own recovery. A halt is never released by this function.
//! * **Halted is sticky.** For a `Flattening` or `Halted` account the decision is "already halted" with a risk scale
//!   of zero; no equity path changes that (property-tested). Only `AccountState::resume` with a `HumanApproval`
//!   leaves a halt.
//! * **Monotone.** For a fixed prior state, lower equity never yields a larger `risk_scale` (property-tested).
//! * **Fail closed.** Non-positive equity or arithmetic overflow yields a halt, never "no action".

use chrono::{DateTime, NaiveDate, Utc};
use mandate_core::mandate::{self, LadderAction, MandateBody};
use rebalancer_core::dec_math::{div_floor, mul, ratio_to_dec, sub, MathError, RatioRound};
use rebalancer_core::Dec;

use crate::state::{AccountState, HaltReason, Transition};

/// A broker-reported equity reading. The constructor name is the policy: there is no other way to make one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EquitySnapshot {
    equity: Dec,
    ccy: String,
    at: DateTime<Utc>,
    venue: String,
}

impl EquitySnapshot {
    /// `equity` as reported BY THE BROKER for `venue`, in `ccy`, read at `at`.
    pub fn broker_reported(venue: &str, ccy: &str, equity: Dec, at: DateTime<Utc>) -> Self {
        Self { equity, ccy: ccy.trim().to_uppercase(), at, venue: venue.trim().to_lowercase() }
    }
    pub fn equity(&self) -> Dec {
        self.equity
    }
    pub fn ccy(&self) -> &str {
        &self.ccy
    }
    pub fn at(&self) -> DateTime<Utc> {
        self.at
    }
    pub fn venue(&self) -> &str {
        &self.venue
    }
}

// --------------------------------------------------------------------------------------------------------------
// Policy
// --------------------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RungAction {
    Shrink { scale: Dec },
    HaltFlatten,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rung {
    /// Fraction below the high-water mark, in (0, 1].
    pub at: Dec,
    pub action: RungAction,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("RISK_POLICY_INVALID: {0}")]
pub struct RiskPolicyError(pub String);

/// Default recovery fraction: a shrink is released when the drawdown is back within half of the rung's trigger.
pub const DEFAULT_RECOVERY_FRACTION_TEXT: &str = "0.5";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskPolicy {
    daily_loss_limit: Dec,
    rungs: Vec<Rung>,
    recovery_fraction: Dec,
}

fn one() -> Dec {
    Dec::from_i64(1)
}

impl RiskPolicy {
    /// Build directly (tests, and callers that already hold exact decimals). Checks the same invariants as the
    /// mandate validator: fractions in (0, 1]; ladder strictly ascending, ends in `HaltFlatten`, only the last rung
    /// halts, shrink scales in (0, 1) and never scaling back up.
    pub fn new(daily_loss_limit: Dec, rungs: Vec<Rung>, recovery_fraction: Dec) -> Result<Self, RiskPolicyError> {
        let bad = |m: &str| Err(RiskPolicyError(m.to_string()));
        if daily_loss_limit <= Dec::ZERO || daily_loss_limit > one() {
            return bad("daily_loss_limit must be in (0, 1]");
        }
        if recovery_fraction <= Dec::ZERO || recovery_fraction > one() {
            return bad("recovery_fraction must be in (0, 1]");
        }
        if rungs.is_empty() {
            return bad("the ladder needs at least one rung");
        }
        let last = rungs.len() - 1;
        let mut prev_at = Dec::ZERO;
        let mut prev_scale = one();
        for (i, r) in rungs.iter().enumerate() {
            if r.at <= prev_at || r.at > one() {
                return bad("ladder rungs must be strictly ascending fractions in (0, 1]");
            }
            prev_at = r.at;
            match &r.action {
                RungAction::Shrink { scale } => {
                    if i == last {
                        return bad("the last rung must be halt_flatten");
                    }
                    if *scale <= Dec::ZERO || *scale >= one() {
                        return bad("a shrink scale must be in (0, 1)");
                    }
                    if *scale > prev_scale {
                        return bad("a later shrink rung cannot scale back up");
                    }
                    prev_scale = *scale;
                }
                RungAction::HaltFlatten => {
                    if i != last {
                        return bad("only the last rung may be halt_flatten");
                    }
                }
            }
        }
        Ok(Self { daily_loss_limit, rungs, recovery_fraction })
    }

    /// From a mandate body. The body is validated first (an invalid mandate yields an error, never a permissive
    /// policy). Ratios are converted with the rounding direction that makes each limit STRICTER: `at`, the daily
    /// loss limit and `scale` all round down.
    pub fn from_mandate(body: &MandateBody) -> Result<Self, RiskPolicyError> {
        Self::from_mandate_with_recovery(body, Dec::parse(DEFAULT_RECOVERY_FRACTION_TEXT).unwrap_or_else(|_| one()))
    }

    pub fn from_mandate_with_recovery(body: &MandateBody, recovery_fraction: Dec) -> Result<Self, RiskPolicyError> {
        let violations = mandate::validate(body);
        if !violations.is_empty() {
            let why: Vec<String> = violations.iter().map(|v| format!("{}: {}", v.field, v.message)).collect();
            return Err(RiskPolicyError(format!("the mandate is invalid: {}", why.join("; "))));
        }
        let conv = |v: f64| ratio_to_dec(v, RatioRound::Down).map_err(|e| RiskPolicyError(e.to_string()));
        let mut rungs = Vec::new();
        for r in &body.loss.drawdown_ladder {
            let action = match r.action {
                LadderAction::Shrink => RungAction::Shrink {
                    scale: conv(r.scale.ok_or_else(|| RiskPolicyError("a shrink rung has no scale".to_string()))?)?,
                },
                LadderAction::HaltFlatten => RungAction::HaltFlatten,
            };
            rungs.push(Rung { at: conv(r.at)?, action });
        }
        Self::new(conv(body.loss.daily_loss_limit)?, rungs, recovery_fraction)
    }

    pub fn with_recovery_fraction(mut self, f: Dec) -> Result<Self, RiskPolicyError> {
        if f <= Dec::ZERO || f > one() {
            return Err(RiskPolicyError("recovery_fraction must be in (0, 1]".to_string()));
        }
        self.recovery_fraction = f;
        Ok(self)
    }

    pub fn daily_loss_limit(&self) -> Dec {
        self.daily_loss_limit
    }
    pub fn rungs(&self) -> &[Rung] {
        &self.rungs
    }
    pub fn recovery_fraction(&self) -> Dec {
        self.recovery_fraction
    }
}

// --------------------------------------------------------------------------------------------------------------
// Decision
// --------------------------------------------------------------------------------------------------------------

/// Stable machine codes of the risk overlay. Alerts, dashboards and tests key on them; never rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RiskCode {
    NoAction,
    DrawdownShrink,
    ShrinkHeld,
    PartialRecovery,
    Recovered,
    DrawdownHalt,
    DailyLossHalt,
    AlreadyHalted,
    EquityInvalid,
    ArithmeticOverflow,
}

impl RiskCode {
    pub const ALL: [RiskCode; 10] = [
        RiskCode::NoAction,
        RiskCode::DrawdownShrink,
        RiskCode::ShrinkHeld,
        RiskCode::PartialRecovery,
        RiskCode::Recovered,
        RiskCode::DrawdownHalt,
        RiskCode::DailyLossHalt,
        RiskCode::AlreadyHalted,
        RiskCode::EquityInvalid,
        RiskCode::ArithmeticOverflow,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RiskCode::NoAction => "RISK_NO_ACTION",
            RiskCode::DrawdownShrink => "RISK_DRAWDOWN_SHRINK",
            RiskCode::ShrinkHeld => "RISK_SHRINK_HELD",
            RiskCode::PartialRecovery => "RISK_PARTIAL_RECOVERY",
            RiskCode::Recovered => "RISK_RECOVERED",
            RiskCode::DrawdownHalt => "RISK_DRAWDOWN_HALT",
            RiskCode::DailyLossHalt => "RISK_DAILY_LOSS_HALT",
            RiskCode::AlreadyHalted => "RISK_ALREADY_HALTED",
            RiskCode::EquityInvalid => "RISK_EQUITY_INVALID",
            RiskCode::ArithmeticOverflow => "RISK_ARITHMETIC_OVERFLOW",
        }
    }
}

impl std::fmt::Display for RiskCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskReason {
    pub code: RiskCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskAction {
    /// Full size (or, for a halted account, nothing to do: see `risk_scale` and the state's status).
    None,
    /// Targets are scaled by `scale`.
    Shrink { scale: Dec },
    /// Cancel open orders, flatten, verify flat, halt.
    HaltFlatten,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskDecision {
    /// The multiplier the planner applies to every target: 1, a rung's scale, or 0 (halted / halting: plan nothing).
    pub risk_scale: Dec,
    pub action: RiskAction,
    pub reasons: Vec<RiskReason>,
    /// The shrink rung in force after this decision.
    pub next_rung: Option<usize>,
    /// Set exactly when `action` is `HaltFlatten`.
    pub halt_reason: Option<HaltReason>,
    /// Drawdown from the high-water mark as a fraction (6 dp, floored), for records only.
    pub drawdown: Option<Dec>,
    /// Loss since day-start as a fraction (6 dp, floored; negative = a gain), for records only.
    pub daily_loss: Option<Dec>,
}

impl RiskDecision {
    pub fn has(&self, code: RiskCode) -> bool {
        self.reasons.iter().any(|r| r.code == code)
    }
    pub fn codes(&self) -> Vec<&'static str> {
        self.reasons.iter().map(|r| r.code.as_str()).collect()
    }
    fn reason(code: RiskCode, message: impl Into<String>) -> RiskReason {
        RiskReason { code, message: message.into() }
    }
    fn halted(halt: HaltReason, reasons: Vec<RiskReason>, drawdown: Option<Dec>, daily_loss: Option<Dec>) -> Self {
        Self {
            risk_scale: Dec::ZERO,
            action: RiskAction::HaltFlatten,
            reasons,
            next_rung: None,
            halt_reason: Some(halt),
            drawdown,
            daily_loss,
        }
    }
}

/// `reference - equity >= fraction * reference` (inclusive), exact.
fn breached(reference: Dec, equity: Dec, fraction: Dec) -> Result<bool, MathError> {
    let loss = sub(reference, equity)?;
    let threshold = mul(fraction, reference)?;
    Ok(loss >= threshold)
}

/// `reference - equity <= at * recovery * reference` (inclusive), exact: the drawdown is back within
/// `recovery` of the rung.
fn recovered(reference: Dec, equity: Dec, at: Dec, recovery: Dec) -> Result<bool, MathError> {
    let loss = sub(reference, equity)?;
    let threshold = mul(mul(at, recovery)?, reference)?;
    Ok(loss <= threshold)
}

fn fraction_of(reference: Dec, equity: Dec) -> Option<Dec> {
    let loss = sub(reference, equity).ok()?;
    if loss.is_negative() {
        // A gain: report the (negative) fraction using the magnitude.
        let gain = sub(equity, reference).ok()?;
        let f = div_floor(gain, reference, 6).ok()?;
        return Dec::ZERO.checked_add(f).and_then(|f| sub(Dec::ZERO, f).ok());
    }
    div_floor(loss, reference, 6).ok()
}

/// Evaluate the account. Pure. `state` should already have folded in `snapshot` via [`AccountState::observe`], but
/// the high-water mark is recomputed defensively as `max(state.hwm, equity)` for an account that may add risk, so a
/// caller that forgot cannot get a lenient answer.
pub fn evaluate_risk(state: &AccountState, snapshot: &EquitySnapshot, policy: &RiskPolicy) -> RiskDecision {
    match evaluate_inner(state, snapshot, policy) {
        Ok(d) => d,
        Err(e) => RiskDecision::halted(
            HaltReason::EquityInvalid,
            vec![RiskDecision::reason(RiskCode::ArithmeticOverflow, format!("risk arithmetic failed ({e}); failing closed"))],
            None,
            None,
        ),
    }
}

fn evaluate_inner(state: &AccountState, snapshot: &EquitySnapshot, policy: &RiskPolicy) -> Result<RiskDecision, MathError> {
    if state.status().is_halt() {
        return Ok(RiskDecision {
            risk_scale: Dec::ZERO,
            action: RiskAction::None,
            reasons: vec![RiskDecision::reason(
                RiskCode::AlreadyHalted,
                format!("the account is {}: no equity path can change that, only a human resume can", state.status().as_str()),
            )],
            next_rung: None,
            halt_reason: None,
            drawdown: None,
            daily_loss: None,
        });
    }
    let equity = snapshot.equity();
    if !equity.is_positive() {
        return Ok(RiskDecision::halted(
            HaltReason::EquityInvalid,
            vec![RiskDecision::reason(RiskCode::EquityInvalid, format!("broker-reported equity {equity} is not positive"))],
            None,
            None,
        ));
    }
    let hwm = match state.hwm() {
        Some(h) if h >= equity => h,
        _ => equity,
    };
    let drawdown = fraction_of(hwm, equity);
    let mut daily_loss = None;
    let mut reasons: Vec<RiskReason> = Vec::new();
    let mut halt: Option<HaltReason> = None;

    // Daily loss.
    if let Some(day_start) = state.day_start_equity() {
        if day_start.is_positive() {
            daily_loss = fraction_of(day_start, equity);
            if breached(day_start, equity, policy.daily_loss_limit)? {
                halt = Some(HaltReason::DailyLoss);
                reasons.push(RiskDecision::reason(
                    RiskCode::DailyLossHalt,
                    format!(
                        "equity {equity} is {} below the day-start {day_start}: the daily loss limit is {}",
                        sub(day_start, equity)?,
                        policy.daily_loss_limit
                    ),
                ));
            }
        }
    }

    // Drawdown ladder: the highest rung hit.
    let mut raw: Option<usize> = None;
    for (i, r) in policy.rungs.iter().enumerate() {
        if breached(hwm, equity, r.at)? {
            raw = Some(i);
        }
    }
    if let Some(i) = raw {
        if policy.rungs[i].action == RungAction::HaltFlatten {
            halt.get_or_insert(HaltReason::DrawdownLadder);
            reasons.push(RiskDecision::reason(
                RiskCode::DrawdownHalt,
                format!("equity {equity} is {} below the high-water mark {hwm}: the halt rung is {}", sub(hwm, equity)?, policy.rungs[i].at),
            ));
        }
    }
    if let Some(h) = halt {
        return Ok(RiskDecision::halted(h, reasons, drawdown, daily_loss));
    }

    // Shrink posture, with hysteresis on the way back.
    let last_shrink = policy.rungs.len().checked_sub(2); // the last rung is the halt rung
    let entered = state.shrink_rung();
    let mut held = if state.status() == crate::state::AccountStatus::Shrunk {
        match (entered, last_shrink) {
            (Some(c), Some(l)) => Some(c.min(l)), // a stale index (mandate changed) clamps to the most severe shrink
            (Some(_), None) => None,
            (None, Some(l)) => Some(l), // Shrunk without a recorded rung: assume the most severe, fail safe
            (None, None) => None,
        }
    } else {
        None
    };
    while let Some(c) = held {
        if recovered(hwm, equity, policy.rungs[c].at, policy.recovery_fraction)? {
            held = c.checked_sub(1);
        } else {
            break;
        }
    }
    let target = match (raw, held) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    Ok(match target {
        None => {
            let code = if state.status() == crate::state::AccountStatus::Shrunk { RiskCode::Recovered } else { RiskCode::NoAction };
            let message = if code == RiskCode::Recovered {
                format!("drawdown recovered to within {} of the shrink trigger: full size again", policy.recovery_fraction)
            } else {
                "no risk limit is hit".to_string()
            };
            RiskDecision {
                risk_scale: one(),
                action: RiskAction::None,
                reasons: vec![RiskDecision::reason(code, message)],
                next_rung: None,
                halt_reason: None,
                drawdown,
                daily_loss,
            }
        }
        Some(i) => {
            let RungAction::Shrink { scale } = policy.rungs[i].action else {
                // Unreachable: a halt rung returned above. Fail closed anyway.
                return Ok(RiskDecision::halted(
                    HaltReason::DrawdownLadder,
                    vec![RiskDecision::reason(RiskCode::DrawdownHalt, "internal: halt rung reached the shrink branch")],
                    drawdown,
                    daily_loss,
                ));
            };
            let newly = raw == Some(i) && state.shrink_rung() != Some(i);
            let stepped_down = state.status() == crate::state::AccountStatus::Shrunk && state.shrink_rung().is_some_and(|c| i < c);
            let code = if newly {
                RiskCode::DrawdownShrink
            } else if stepped_down {
                RiskCode::PartialRecovery
            } else {
                RiskCode::ShrinkHeld
            };
            RiskDecision {
                risk_scale: scale,
                action: RiskAction::Shrink { scale },
                reasons: vec![RiskDecision::reason(
                    code,
                    format!("drawdown {} from the high-water mark {hwm}: shrink rung {} in force, targets scaled by {scale}", drawdown.map_or("?".to_string(), |d| d.to_string()), policy.rungs[i].at),
                )],
                next_rung: Some(i),
                halt_reason: None,
                drawdown,
                daily_loss,
            }
        }
    })
}

/// Turn a decision into the next state. A no-op on a `Flattening` or `Halted` account (halts are sticky).
pub fn apply_decision(state: &AccountState, decision: &RiskDecision, at: DateTime<Utc>) -> (AccountState, Option<Transition>) {
    if state.status().is_halt() {
        return (state.clone(), None);
    }
    match &decision.action {
        RiskAction::None => state.with_full_size(),
        RiskAction::Shrink { scale } => match decision.next_rung {
            Some(rung) => state.with_shrink(*scale, rung),
            None => state.begin_flatten(HaltReason::EquityInvalid, "internal: a shrink decision without a rung; failing closed", at),
        },
        RiskAction::HaltFlatten => {
            let detail = decision.reasons.iter().map(|r| format!("{}: {}", r.code, r.message)).collect::<Vec<_>>().join(" | ");
            state.begin_flatten(decision.halt_reason.unwrap_or(HaltReason::EquityInvalid), &detail, at)
        }
    }
}

/// `observe` + `evaluate_risk` + `apply_decision` in one call: the step the run pipeline performs.
pub fn step(
    state: &AccountState,
    snapshot: &EquitySnapshot,
    day: NaiveDate,
    policy: &RiskPolicy,
) -> (AccountState, RiskDecision, Option<Transition>) {
    let observed = state.observe(snapshot, day);
    let decision = evaluate_risk(&observed, snapshot, policy);
    let (next, transition) = apply_decision(&observed, &decision, snapshot.at());
    (next, decision, transition)
}
