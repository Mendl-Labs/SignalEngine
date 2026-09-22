//! `Policy::compile`: the mandate body turned once into exact-decimal limits the guard and planner use.
//!
//! The mandate carries ratios as `f64` fractions (`0.25` = 25% of equity). This is the ONLY place they are
//! converted, with the rule documented on [`crate::dec_math::ratio_to_dec`] (shortest round-trip decimal) and a
//! direction chosen per field so that an inexact conversion can only make a limit STRICTER:
//!
//! | field | direction | why |
//! |---|---|---|
//! | `max_position`, `max_asset_class.*`, `max_gross`, `max_net`, `max_turnover_per_day`, `leverage_max_gross` | down | caps |
//! | `min_cash_reserve` | up | a floor on cash |
//! | money (`allocated`, `max_order_notional`) | exact | decimal strings already |
//!
//! A mandate that fails `mandate_core::mandate::validate`, or whose numbers cannot be converted, compiles to a
//! policy that DENIES EVERYTHING (`MANDATE_INVALID`). `compile` never fails open and never panics.
//!
//! The mandate BODY carries no status, grant or expiry (those live in the storage envelope), so a compiled policy
//! is not usable until [`Policy::with_envelope`] supplies them; a policy without an envelope denies everything with
//! `MANDATE_NOT_ACTIVE`.

use std::collections::{BTreeMap, BTreeSet};

use broker_adapters::Dec;
use chrono::{DateTime, Utc};
use mandate_core::mandate::{self, MandateBody, Money};

use crate::dec_math::{mul, ratio_to_dec, MathError, RatioRound};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MandateStatus {
    Draft,
    Active,
    Superseded,
    Revoked,
    Expired,
}

/// The storage-envelope facts about one mandate version that the guard needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MandateEnvelope {
    pub version: u32,
    pub status: MandateStatus,
    pub effective_from: DateTime<Utc>,
    /// The mandate stops being valid AT this instant (`now >= review_by` is expired).
    pub review_by: DateTime<Utc>,
}

/// All limits, exact. Sets are normalised: instruments upper case, venues and asset classes lower case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub ccy: String,
    pub allocated: Dec,
    pub min_cash_reserve: Dec,
    pub instrument_allow: BTreeSet<String>,
    pub instrument_deny: BTreeSet<String>,
    pub venues: BTreeSet<String>,
    pub asset_classes: BTreeSet<String>,
    pub shorting: bool,
    pub derivatives: bool,
    pub leverage_max_gross: Dec,
    pub max_position: Dec,
    pub max_asset_class: BTreeMap<String, Dec>,
    /// Already the minimum of `exposure.max_gross` and `universe.leverage_max_gross`.
    pub max_gross: Dec,
    pub max_net: Dec,
    pub max_order_notional: Dec,
    pub max_orders_per_day: u32,
    pub max_turnover_per_day: Dec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyBody {
    Valid(Box<Limits>),
    /// Human-readable reasons the mandate could not be compiled.
    Invalid(Vec<String>),
}

/// Default maximum age of a price used for an order, seconds. NOT part of the mandate (it has no such field): a
/// deployment setting, changed with [`Policy::with_max_price_age_secs`].
pub const DEFAULT_MAX_PRICE_AGE_SECS: i64 = 300;
/// Prices stamped further in the future than this (clock skew allowance) are treated as stale.
pub const MAX_FUTURE_SKEW_SECS: i64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    /// `mandate::canonical_hash` of the compiled body (recorded on every decision).
    pub body_hash: String,
    pub body: PolicyBody,
    pub envelope: Option<MandateEnvelope>,
    pub max_price_age_secs: i64,
}

/// What the policy allows at a given instant, before looking at any order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Standing {
    Active,
    /// Past `review_by`: only reducing orders may pass (spec B3).
    Expired,
    /// Not usable at all (no envelope, not active, not yet effective).
    NotActive(String),
    /// The mandate did not validate or convert.
    Invalid,
}

fn money_dec(m: &Money) -> Result<Dec, MathError> {
    Dec::parse(m.amount.trim()).map_err(|_| MathError::BadRatio(m.amount.clone(), "not a decimal amount"))
}

fn normalise(items: &[String], upper: bool) -> BTreeSet<String> {
    items
        .iter()
        .map(|s| if upper { s.trim().to_uppercase() } else { s.trim().to_lowercase() })
        .collect()
}

fn convert(b: &MandateBody) -> Result<Limits, MathError> {
    let down = |v: f64| ratio_to_dec(v, RatioRound::Down);
    let leverage = down(b.universe.leverage_max_gross)?;
    let max_gross = std::cmp::min(down(b.exposure.max_gross)?, leverage);
    let mut classes = BTreeMap::new();
    for (class, cap) in &b.exposure.max_asset_class {
        classes.insert(class.trim().to_lowercase(), down(*cap)?);
    }
    Ok(Limits {
        ccy: b.capital.allocated.ccy.clone(),
        allocated: money_dec(&b.capital.allocated)?,
        min_cash_reserve: ratio_to_dec(b.capital.min_cash_reserve, RatioRound::Up)?,
        instrument_allow: normalise(&b.universe.instrument_allow, true),
        instrument_deny: normalise(&b.universe.instrument_deny, true),
        venues: normalise(&b.universe.venues, false),
        asset_classes: normalise(&b.universe.asset_classes, false),
        shorting: b.universe.shorting,
        derivatives: b.universe.derivatives,
        leverage_max_gross: leverage,
        max_position: down(b.exposure.max_position)?,
        max_asset_class: classes,
        max_gross,
        max_net: down(b.exposure.max_net)?,
        max_order_notional: money_dec(&b.exposure.max_order_notional)?,
        max_orders_per_day: b.exposure.max_orders_per_day,
        max_turnover_per_day: down(b.exposure.max_turnover_per_day)?,
    })
}

impl Policy {
    /// Compile a mandate body. See the module docs: an unusable body yields a deny-everything policy.
    pub fn compile(body: &MandateBody) -> Policy {
        let body_hash = mandate::canonical_hash(body);
        let violations = mandate::validate(body);
        let compiled = if violations.is_empty() {
            match convert(body) {
                Ok(l) => PolicyBody::Valid(Box::new(l)),
                Err(e) => PolicyBody::Invalid(vec![format!("could not convert the mandate's numbers: {e}")]),
            }
        } else {
            PolicyBody::Invalid(violations.into_iter().map(|v| format!("{}: {}", v.field, v.message)).collect())
        };
        Policy { body_hash, body: compiled, envelope: None, max_price_age_secs: DEFAULT_MAX_PRICE_AGE_SECS }
    }

    pub fn with_envelope(mut self, envelope: MandateEnvelope) -> Policy {
        self.envelope = Some(envelope);
        self
    }

    pub fn with_max_price_age_secs(mut self, secs: i64) -> Policy {
        self.max_price_age_secs = secs;
        self
    }

    pub fn limits(&self) -> Option<&Limits> {
        match &self.body {
            PolicyBody::Valid(l) => Some(l),
            PolicyBody::Invalid(_) => None,
        }
    }

    pub fn standing(&self, now: DateTime<Utc>) -> Standing {
        if self.limits().is_none() {
            return Standing::Invalid;
        }
        let Some(env) = &self.envelope else {
            return Standing::NotActive("no mandate envelope (status, effective and review dates) was supplied".into());
        };
        if env.status != MandateStatus::Active {
            return Standing::NotActive(format!("mandate status is {:?}, not Active", env.status));
        }
        if now < env.effective_from {
            return Standing::NotActive(format!("mandate is effective from {}, not yet", env.effective_from));
        }
        if now >= env.review_by {
            return Standing::Expired;
        }
        Standing::Active
    }

    /// THE capital base: the one number every percentage-of-equity rule is measured against. It is
    /// `min(broker equity, mandate.capital.allocated)` when the account currency matches the mandate currency,
    /// otherwise the raw equity (a currency mismatch is denied by the guard before any limit is evaluated, and
    /// `allocated` cannot be compared across currencies).
    ///
    /// The planner sizes targets on it AND the guard uses it as the denominator of `max_position`,
    /// `max_asset_class`, `max_gross`, `max_net`, `max_turnover_per_day` and of the cash-reserve fraction, so a
    /// plan and the guard that checks it can never disagree about what "25% of equity" means. Only the AVAILABLE
    /// CASH test (does the account actually hold the cash) uses the broker's own cash figure, not this base.
    ///
    /// A policy with no compiled limits returns the raw equity (the guard denies everything for it anyway).
    pub fn capital_base(&self, equity: Dec, account_ccy: &str) -> Dec {
        match self.limits() {
            Some(l) if l.ccy.eq_ignore_ascii_case(account_ccy.trim()) => std::cmp::min(equity, l.allocated),
            _ => equity,
        }
    }

    /// The smallest cash balance the account may be left with: `min_cash_reserve` of the CAPITAL BASE (see
    /// [`Policy::capital_base`]), rounded UP to 8 decimals so the reserve is never understated. Shared by the guard
    /// and the planner. Callers pass the capital base, not the raw equity.
    pub fn reserve_amount(&self, capital_base: Dec) -> Result<Dec, MathError> {
        let Some(l) = self.limits() else { return Ok(capital_base) };
        mul(l.min_cash_reserve, capital_base)?.round_dp(8, broker_adapters::decimal::Rounding::Ceil).map_err(|_| MathError::Overflow)
    }

    /// Apply a deployment's own limits: SPEC section 3, "effective limit = min(mandate, deployment)". A deployment
    /// can be TIGHTER than the mandate, never looser: every cap becomes the smaller of the two, the cash reserve
    /// the larger, the order count the smaller. A deployment value that is not stricter is simply ignored (it
    /// cannot loosen anything). `body_hash` and the envelope are unchanged (they identify the MANDATE); the
    /// deployment is identified separately by [`DeploymentLimits::digest`]. An invalid policy stays invalid.
    pub fn with_deployment(mut self, d: &DeploymentLimits) -> Policy {
        let PolicyBody::Valid(limits) = &mut self.body else { return self };
        let l: &mut Limits = limits;
        let tighten = |current: &mut Dec, cap: Option<Dec>| {
            if let Some(c) = cap {
                if c < *current {
                    *current = c;
                }
            }
        };
        tighten(&mut l.allocated, d.capital_allocation);
        tighten(&mut l.max_position, d.max_position);
        tighten(&mut l.max_gross, d.max_gross);
        tighten(&mut l.max_net, d.max_net);
        tighten(&mut l.max_order_notional, d.max_order_notional);
        tighten(&mut l.max_turnover_per_day, d.max_turnover_per_day);
        if let Some(n) = d.max_orders_per_day {
            l.max_orders_per_day = l.max_orders_per_day.min(n);
        }
        if let Some(r) = d.min_cash_reserve {
            if r > l.min_cash_reserve {
                l.min_cash_reserve = r;
            }
        }
        for (class, cap) in &d.max_asset_class {
            let class = class.trim().to_lowercase();
            match l.max_asset_class.get_mut(&class) {
                Some(cur) => tighten(cur, Some(*cap)),
                // A class the mandate does not cap is now capped by the deployment (stricter, never looser).
                None => {
                    l.max_asset_class.insert(class, *cap);
                }
            }
        }
        self
    }
}

/// A deployment's own, optional limits. Every field is `None` = "no deployment limit"; see
/// [`Policy::with_deployment`] for how they combine with the mandate (always the stricter of the two).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeploymentLimits {
    /// Capital the deployment may use; clamped to the mandate's allocation.
    pub capital_allocation: Option<Dec>,
    pub max_position: Option<Dec>,
    pub max_gross: Option<Dec>,
    pub max_net: Option<Dec>,
    pub max_order_notional: Option<Dec>,
    pub max_orders_per_day: Option<u32>,
    pub max_turnover_per_day: Option<Dec>,
    /// A deployment may ask for MORE cash held back, never less.
    pub min_cash_reserve: Option<Dec>,
    pub max_asset_class: BTreeMap<String, Dec>,
}

impl DeploymentLimits {
    /// Deterministic text of the limits, for the run record. `None` fields print as `-`.
    pub fn digest(&self) -> String {
        let f = |d: Option<Dec>| d.map_or("-".to_string(), |v| v.normalized().to_string());
        let classes: Vec<String> = self.max_asset_class.iter().map(|(k, v)| format!("{}={}", k.to_lowercase(), v.normalized())).collect();
        format!(
            "cap={}|pos={}|gross={}|net={}|notional={}|orders={}|turnover={}|reserve={}|classes={}",
            f(self.capital_allocation),
            f(self.max_position),
            f(self.max_gross),
            f(self.max_net),
            f(self.max_order_notional),
            self.max_orders_per_day.map_or("-".to_string(), |n| n.to_string()),
            f(self.max_turnover_per_day),
            f(self.min_cash_reserve),
            classes.join(",")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASELINE: &str = include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json");

    fn body() -> MandateBody {
        serde_json::from_str(BASELINE).unwrap()
    }

    fn d(s: &str) -> Dec {
        Dec::parse(s).unwrap()
    }

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn baseline_compiles_to_exact_limits() {
        let p = Policy::compile(&body());
        let l = p.limits().expect("valid");
        assert_eq!(l.max_position, d("0.25"));
        assert_eq!(l.min_cash_reserve, d("0.05"));
        assert_eq!(l.max_asset_class["crypto_spot"], d("0.6"));
        assert_eq!(l.max_gross, d("1"));
        assert_eq!(l.max_order_notional, d("1500"));
        assert_eq!(l.allocated, d("5000"));
        assert!(l.instrument_allow.contains("BTC/USD"));
        assert_eq!(p.body_hash.len(), 64);
    }

    #[test]
    fn max_gross_is_capped_by_the_leverage_limit() {
        let mut b = body();
        b.universe.leverage_max_gross = 1.0;
        b.exposure.max_gross = 1.0;
        assert_eq!(Policy::compile(&b).limits().unwrap().max_gross, d("1"));
    }

    #[test]
    fn invalid_mandate_compiles_to_an_invalid_policy_with_reasons() {
        let mut b = body();
        b.exposure.max_position = 25.0; // "25%" typed as 25
        let p = Policy::compile(&b);
        match &p.body {
            PolicyBody::Invalid(r) => assert!(r.iter().any(|m| m.contains("exposure.max_position")), "{r:?}"),
            PolicyBody::Valid(_) => panic!("must be invalid"),
        }
        assert_eq!(p.standing(at("2026-09-21T15:00:00Z")), Standing::Invalid);
    }

    #[test]
    fn nan_in_the_body_is_invalid_not_a_panic() {
        let mut b = body();
        b.exposure.max_gross = f64::NAN;
        assert_eq!(Policy::compile(&b).standing(at("2026-09-21T15:00:00Z")), Standing::Invalid);
    }

    #[test]
    fn no_envelope_is_not_active() {
        assert!(matches!(Policy::compile(&body()).standing(at("2026-09-21T15:00:00Z")), Standing::NotActive(_)));
    }

    #[test]
    fn standing_boundaries() {
        let env = |status| MandateEnvelope {
            version: 3,
            status,
            effective_from: at("2026-09-21T00:00:00Z"),
            review_by: at("2026-12-21T00:00:00Z"),
        };
        let p = Policy::compile(&body()).with_envelope(env(MandateStatus::Active));
        assert!(matches!(p.standing(at("2026-09-20T23:59:59Z")), Standing::NotActive(_)));
        assert_eq!(p.standing(at("2026-09-21T00:00:00Z")), Standing::Active, "active from the effective instant");
        assert_eq!(p.standing(at("2026-12-20T23:59:59Z")), Standing::Active);
        assert_eq!(p.standing(at("2026-12-21T00:00:00Z")), Standing::Expired, "expired AT review_by");
        for s in [MandateStatus::Draft, MandateStatus::Superseded, MandateStatus::Revoked, MandateStatus::Expired] {
            let q = Policy::compile(&body()).with_envelope(env(s));
            assert!(matches!(q.standing(at("2026-10-01T00:00:00Z")), Standing::NotActive(_)), "{s:?}");
        }
    }

    #[test]
    fn reserve_rounds_up() {
        let p = Policy::compile(&body());
        assert_eq!(p.reserve_amount(d("5000")).unwrap(), d("250"));
        // 0.05 * 3333.333333335 = 166.66666666675 -> rounded UP at 8 dp
        assert_eq!(p.reserve_amount(d("3333.333333335")).unwrap(), d("166.66666667"));
    }
}
