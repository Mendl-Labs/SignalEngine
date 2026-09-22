//! Trading mandate: the versioned record of what an account's agent may do and
//! the limits it must stay inside (spec: `product-mandate/SPEC.md`, section 2).
//!
//! This module is PURE: types, validation and a canonical hash. It reads no
//! database, calls no broker and is consumed by nothing yet, so it has no
//! runtime effect. Later work wires it to storage, the onboarding flow and the
//! order-path guard; keeping the rules in one dependency-free place is what
//! lets the guard, the API and the agent's tools all apply the *same* checks.
//!
//! Conventions that the rest of the system must keep:
//! - Ratios are FRACTIONS (0.10 = 10%), never percents. The old code carried
//!   drawdown as a percent on the wire and a fraction in the database; here a
//!   value like `10` is rejected with a hint instead of being guessed at.
//! - Money is a decimal string plus a currency code, compared as `Decimal`.
//! - Unknown fields are rejected (`deny_unknown_fields`), so nothing can be
//!   smuggled into a mandate that a validator does not know about.
//! - Anything not listed in `autonomy.may` is not permitted (deny by default).
//! - Only `capital_source = "own"` is accepted for now (owner decision,
//!   2026-09-21); `third_party` and `mixed` parse but never validate.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Version of this body schema; stored beside the body so a reader can tell
/// which rules a stored mandate was validated under.
pub const MANDATE_SCHEMA_VERSION: u32 = 1;

/// Acknowledgement documents every mandate must carry.
pub const REQUIRED_ACK_DOCS: [&str; 2] = ["own_capital_attestation", "risk_disclosure"];

/// Actions that can never be granted to the agent; always present in `never`.
pub const REQUIRED_NEVER: [AgentAction; 4] = [
    AgentAction::ChangeLimits,
    AgentAction::ResumeAfterHalt,
    AgentAction::ChangeCredentials,
    AgentAction::Withdraw,
];

/// Actions that move or size positions; these need autonomy level L2 or above.
/// `Halt` is deliberately absent: stopping is the fail-safe direction and is
/// allowed at every level.
const ORDER_ACTIONS: [AgentAction; 5] = [
    AgentAction::PlaceOrders,
    AgentAction::CancelOrders,
    AgentAction::ResizeWithinLimits,
    AgentAction::Rebalance,
    AgentAction::Flatten,
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Violation {
    /// Dotted path of the offending field, e.g. `loss.drawdown_ladder[1].at`.
    pub field: String,
    /// Plain-language reason, safe to show to the user.
    pub message: String,
}

impl Violation {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self { field: field.into(), message: message.into() }
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Money {
    /// Decimal string, e.g. "5000.00" (a string so no float rounding).
    pub amount: String,
    /// ISO-4217-style code, three uppercase letters.
    pub ccy: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapitalSource {
    Own,
    ThirdParty,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acknowledgement {
    pub doc: String,
    pub doc_version: String,
    pub at: DateTime<Utc>,
    pub by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Basis {
    pub capital_source: CapitalSource,
    /// e.g. "US-GA": country, optionally a region, for tax and regulatory routing.
    pub jurisdiction: String,
    pub acknowledgements: Vec<Acknowledgement>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capital {
    pub allocated: Money,
    /// Fraction of equity kept in cash, in [0, 1).
    pub min_cash_reserve: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Universe {
    pub venues: Vec<String>,
    pub asset_classes: Vec<String>,
    pub instrument_allow: Vec<String>,
    #[serde(default)]
    pub instrument_deny: Vec<String>,
    pub shorting: bool,
    pub derivatives: bool,
    /// Maximum gross leverage as a multiple of equity (1.0 = unleveraged).
    pub leverage_max_gross: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exposure {
    /// Per-instrument cap as a multiple of equity (a fraction when unleveraged).
    pub max_position: f64,
    /// Per-asset-class cap, same units, keyed by a class in `universe.asset_classes`.
    #[serde(default)]
    pub max_asset_class: BTreeMap<String, f64>,
    pub max_gross: f64,
    pub max_net: f64,
    pub max_order_notional: Money,
    pub max_orders_per_day: u32,
    /// Daily traded notional as a multiple of equity.
    pub max_turnover_per_day: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LadderAction {
    /// Scale all target positions by `scale` using reduce-only orders.
    Shrink,
    /// Cancel open orders, flatten, and halt until a human resumes.
    HaltFlatten,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LadderRung {
    /// Fraction below the high-water mark of broker-reported equity.
    pub at: f64,
    pub action: LadderAction,
    /// Required for `shrink` (0 < scale < 1); must be absent for `halt_flatten`.
    #[serde(default)]
    pub scale: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumePolicy {
    HumanOnly,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Loss {
    /// Fraction of start-of-day equity; a breach halts the account.
    pub daily_loss_limit: f64,
    pub drawdown_ladder: Vec<LadderRung>,
    pub resume_after_halt: ResumePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentAction {
    PlaceOrders,
    CancelOrders,
    ResizeWithinLimits,
    Rebalance,
    Halt,
    Flatten,
    AddStrategy,
    RemoveStrategy,
    RaiseTargetRisk,
    ChangeLimits,
    ResumeAfterHalt,
    ChangeCredentials,
    Withdraw,
}

/// Mirrors the certification ladder (CERTIFICATION.md): L0 suggest, L1
/// assisted, L2 paper-autonomous, L3 live-autonomous within limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AutonomyLevel {
    L0,
    L1,
    L2,
    L3,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Autonomy {
    pub level: AutonomyLevel,
    pub may: Vec<AgentAction>,
    pub needs_confirmation: Vec<AgentAction>,
    pub never: Vec<AgentAction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DigestCadence {
    Daily,
    Weekly,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reporting {
    pub digest: DigestCadence,
    pub alert_channels: Vec<String>,
}

/// The immutable content of one mandate version. Identity, status, grant
/// details and expiry belong to the storage envelope, not to this body, so the
/// body hash covers only what the risk layers act on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MandateBody {
    pub basis: Basis,
    pub capital: Capital,
    pub universe: Universe,
    pub exposure: Exposure,
    pub loss: Loss,
    pub autonomy: Autonomy,
    pub reporting: Reporting,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn fraction(field: &str, v: f64, out: &mut Vec<Violation>) {
    if !v.is_finite() || v <= 0.0 {
        out.push(Violation::new(field, "must be greater than 0 (a fraction such as 0.10 for 10%)"));
    } else if v > 1.0 {
        out.push(Violation::new(
            field,
            format!("{v} is above 1: ratios are fractions, so 10% is written 0.10, not 10"),
        ));
    }
}

fn multiple(field: &str, v: f64, out: &mut Vec<Violation>) -> bool {
    if !v.is_finite() || v <= 0.0 {
        out.push(Violation::new(field, "must be greater than 0"));
        return false;
    }
    true
}

fn money(field: &str, m: &Money, out: &mut Vec<Violation>) -> Option<Decimal> {
    let mut ok = true;
    if m.ccy.len() != 3 || !m.ccy.chars().all(|c| c.is_ascii_uppercase()) {
        out.push(Violation::new(format!("{field}.ccy"), "must be a 3-letter uppercase currency code such as USD"));
        ok = false;
    }
    match Decimal::from_str(m.amount.trim()) {
        Ok(d) if d > Decimal::ZERO => ok.then_some(d),
        Ok(_) => {
            out.push(Violation::new(format!("{field}.amount"), "must be greater than 0"));
            None
        }
        Err(_) => {
            out.push(Violation::new(format!("{field}.amount"), "must be a decimal number written as a string, e.g. \"5000.00\""));
            None
        }
    }
}

fn nonempty_list(field: &str, items: &[String], out: &mut Vec<Violation>) {
    if items.is_empty() {
        out.push(Violation::new(field, "must list at least one entry"));
    }
    if items.iter().any(|s| s.trim().is_empty()) {
        out.push(Violation::new(field, "entries must not be blank"));
    }
}

/// Check every invariant and return ALL violations (empty = valid), so a user
/// fixing a form sees everything at once rather than one error at a time.
pub fn validate(m: &MandateBody) -> Vec<Violation> {
    let mut v = Vec::new();

    // --- basis
    if m.basis.capital_source != CapitalSource::Own {
        v.push(Violation::new(
            "basis.capital_source",
            "only your own capital is supported right now; third-party or pooled money is not available",
        ));
    }
    let j = m.basis.jurisdiction.trim();
    if j.is_empty() || j.len() > 16 || !j.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        v.push(Violation::new("basis.jurisdiction", "must be a country or country-region code such as US-GA"));
    }
    for doc in REQUIRED_ACK_DOCS {
        if !m.basis.acknowledgements.iter().any(|a| a.doc == doc) {
            v.push(Violation::new("basis.acknowledgements", format!("missing required acknowledgement: {doc}")));
        }
    }
    for (i, a) in m.basis.acknowledgements.iter().enumerate() {
        if a.by.trim().is_empty() || a.doc_version.trim().is_empty() {
            v.push(Violation::new(format!("basis.acknowledgements[{i}]"), "needs a signer and a document version"));
        }
    }

    // --- capital
    let allocated = money("capital.allocated", &m.capital.allocated, &mut v);
    if !m.capital.min_cash_reserve.is_finite() || !(0.0..1.0).contains(&m.capital.min_cash_reserve) {
        v.push(Violation::new(
            "capital.min_cash_reserve",
            "must be a fraction from 0 up to (not including) 1, e.g. 0.05 for 5%",
        ));
    }

    // --- universe
    nonempty_list("universe.venues", &m.universe.venues, &mut v);
    nonempty_list("universe.asset_classes", &m.universe.asset_classes, &mut v);
    nonempty_list("universe.instrument_allow", &m.universe.instrument_allow, &mut v);
    let allow: BTreeSet<String> = m.universe.instrument_allow.iter().map(|s| s.trim().to_uppercase()).collect();
    let overlap: Vec<&String> = m.universe.instrument_deny.iter().filter(|s| allow.contains(&s.trim().to_uppercase())).collect();
    if !overlap.is_empty() {
        v.push(Violation::new("universe.instrument_deny", format!("also listed in instrument_allow: {overlap:?}")));
    }
    let leverage_ok = if !m.universe.leverage_max_gross.is_finite() || m.universe.leverage_max_gross < 1.0 {
        v.push(Violation::new("universe.leverage_max_gross", "must be at least 1 (1 means no leverage)"));
        false
    } else {
        true
    };

    // --- exposure (multiples of equity, bounded by the leverage cap)
    let e = &m.exposure;
    let gross_ok = multiple("exposure.max_gross", e.max_gross, &mut v);
    let net_ok = multiple("exposure.max_net", e.max_net, &mut v);
    let pos_ok = multiple("exposure.max_position", e.max_position, &mut v);
    if gross_ok && leverage_ok && e.max_gross > m.universe.leverage_max_gross {
        v.push(Violation::new("exposure.max_gross", "cannot exceed universe.leverage_max_gross"));
    }
    if gross_ok && net_ok && e.max_net > e.max_gross {
        v.push(Violation::new("exposure.max_net", "cannot exceed exposure.max_gross"));
    }
    if gross_ok && pos_ok && e.max_position > e.max_gross {
        v.push(Violation::new(
            "exposure.max_position",
            format!("{} is above the gross limit {}: if you meant a percent, use a fraction (0.25 for 25%)", e.max_position, e.max_gross),
        ));
    }
    for (class, cap) in &e.max_asset_class {
        let f = format!("exposure.max_asset_class.{class}");
        if !m.universe.asset_classes.iter().any(|c| c == class) {
            v.push(Violation::new(&f, "is not one of universe.asset_classes"));
        }
        if multiple(&f, *cap, &mut v) && gross_ok && *cap > e.max_gross {
            v.push(Violation::new(&f, "cannot exceed exposure.max_gross"));
        }
    }
    if let (Some(alloc), Some(notional)) = (allocated, money("exposure.max_order_notional", &e.max_order_notional, &mut v)) {
        if e.max_order_notional.ccy != m.capital.allocated.ccy {
            v.push(Violation::new("exposure.max_order_notional.ccy", "must match capital.allocated.ccy"));
        } else if notional > alloc {
            v.push(Violation::new("exposure.max_order_notional", "cannot exceed the allocated capital"));
        }
    }
    if e.max_orders_per_day == 0 {
        v.push(Violation::new("exposure.max_orders_per_day", "must be at least 1"));
    }
    multiple("exposure.max_turnover_per_day", e.max_turnover_per_day, &mut v);

    // --- loss limits
    fraction("loss.daily_loss_limit", m.loss.daily_loss_limit, &mut v);
    let ladder = &m.loss.drawdown_ladder;
    if ladder.is_empty() {
        v.push(Violation::new("loss.drawdown_ladder", "needs at least one rung, ending in halt_flatten"));
    }
    let mut prev_at = 0.0_f64;
    let mut prev_scale = 1.0_f64;
    for (i, rung) in ladder.iter().enumerate() {
        let f = format!("loss.drawdown_ladder[{i}]");
        fraction(&format!("{f}.at"), rung.at, &mut v);
        if rung.at.is_finite() && rung.at <= prev_at {
            v.push(Violation::new(format!("{f}.at"), "rungs must be in strictly increasing order"));
        }
        prev_at = rung.at;
        let last = i + 1 == ladder.len();
        match (rung.action, rung.scale) {
            (LadderAction::Shrink, Some(s)) if s.is_finite() && s > 0.0 && s < 1.0 => {
                if s > prev_scale {
                    v.push(Violation::new(format!("{f}.scale"), "a later rung cannot scale positions back up"));
                }
                prev_scale = s;
            }
            (LadderAction::Shrink, _) => {
                v.push(Violation::new(format!("{f}.scale"), "a shrink rung needs a scale strictly between 0 and 1"));
            }
            (LadderAction::HaltFlatten, Some(_)) => {
                v.push(Violation::new(format!("{f}.scale"), "a halt_flatten rung takes no scale"));
            }
            (LadderAction::HaltFlatten, None) => {}
        }
        if last && rung.action != LadderAction::HaltFlatten {
            v.push(Violation::new(&f, "the last rung must be halt_flatten"));
        }
        if !last && rung.action == LadderAction::HaltFlatten {
            v.push(Violation::new(&f, "halt_flatten must be the last rung; later rungs could never be reached"));
        }
    }
    if let Some(last) = ladder.last() {
        if m.loss.daily_loss_limit.is_finite() && last.at.is_finite() && m.loss.daily_loss_limit > last.at {
            v.push(Violation::new("loss.daily_loss_limit", "cannot be larger than the final drawdown rung"));
        }
    }

    // --- autonomy
    let a = &m.autonomy;
    for required in REQUIRED_NEVER {
        if !a.never.contains(&required) {
            v.push(Violation::new("autonomy.never", format!("must always include {required:?}")));
        }
    }
    let mut seen: BTreeMap<AgentAction, &str> = BTreeMap::new();
    for (list, name) in [(&a.may, "may"), (&a.needs_confirmation, "needs_confirmation"), (&a.never, "never")] {
        for action in list {
            if let Some(prev) = seen.insert(*action, name) {
                v.push(Violation::new(
                    format!("autonomy.{name}"),
                    format!("{action:?} is already listed under {prev}; an action can be in only one list"),
                ));
            }
        }
    }
    if a.level < AutonomyLevel::L2 {
        if let Some(act) = a.may.iter().find(|x| ORDER_ACTIONS.contains(x)) {
            v.push(Violation::new(
                "autonomy.may",
                format!("{act:?} needs autonomy level L2 or higher; at {:?} the agent may only suggest", a.level),
            ));
        }
    }

    // --- reporting
    if m.reporting.alert_channels.is_empty() {
        v.push(Violation::new("reporting.alert_channels", "needs at least one channel so a halt reaches a person"));
    }
    v
}

/// Parse untrusted JSON and validate it. A parse failure (missing or unknown
/// field, wrong type) is returned as a single violation naming the problem.
pub fn validate_json(value: &serde_json::Value) -> Result<MandateBody, Vec<Violation>> {
    let body: MandateBody = serde_json::from_value(value.clone())
        .map_err(|e| vec![Violation::new("mandate", format!("could not be read: {e}"))])?;
    let violations = validate(&body);
    if violations.is_empty() { Ok(body) } else { Err(violations) }
}

/// SHA-256 (hex) of the canonical serialization. Always hash a body that was
/// parsed into `MandateBody` and re-serialized here, never the client's raw
/// JSON, so key order and whitespace cannot change the hash.
pub fn canonical_hash(m: &MandateBody) -> String {
    let bytes = serde_json::to_vec(m).expect("MandateBody always serializes");
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> serde_json::Value {
        json!({
            "basis": {
                "capital_source": "own",
                "jurisdiction": "US-GA",
                "acknowledgements": [
                    {"doc": "own_capital_attestation", "doc_version": "2026-10", "at": "2026-09-21T12:00:00Z", "by": "user_1"},
                    {"doc": "risk_disclosure", "doc_version": "2026-10", "at": "2026-09-21T12:00:00Z", "by": "user_1"}
                ]
            },
            "capital": {"allocated": {"amount": "5000.00", "ccy": "USD"}, "min_cash_reserve": 0.05},
            "universe": {
                "venues": ["alpaca", "kraken"], "asset_classes": ["us_etf", "crypto_spot"],
                "instrument_allow": ["SPY", "EFA", "BTC/USD", "ETH/USD"], "instrument_deny": [],
                "shorting": false, "derivatives": false, "leverage_max_gross": 1.0
            },
            "exposure": {
                "max_position": 0.25, "max_asset_class": {"crypto_spot": 0.6},
                "max_gross": 1.0, "max_net": 1.0,
                "max_order_notional": {"amount": "1500.00", "ccy": "USD"},
                "max_orders_per_day": 20, "max_turnover_per_day": 0.5
            },
            "loss": {
                "daily_loss_limit": 0.03,
                "drawdown_ladder": [
                    {"at": 0.10, "action": "shrink", "scale": 0.5},
                    {"at": 0.20, "action": "halt_flatten"}
                ],
                "resume_after_halt": "human_only"
            },
            "autonomy": {
                "level": "L3",
                "may": ["place_orders", "cancel_orders", "resize_within_limits", "rebalance", "halt", "flatten"],
                "needs_confirmation": ["add_strategy", "remove_strategy", "raise_target_risk"],
                "never": ["change_limits", "resume_after_halt", "change_credentials", "withdraw"]
            },
            "reporting": {"digest": "daily", "alert_channels": ["email"]}
        })
    }

    fn body(v: serde_json::Value) -> MandateBody {
        serde_json::from_value(v).expect("fixture parses")
    }

    /// Apply `edit` to the baseline and return its violations.
    fn with(edit: impl FnOnce(&mut serde_json::Value)) -> Vec<Violation> {
        let mut v = base();
        edit(&mut v);
        validate(&body(v))
    }

    fn fields(v: &[Violation]) -> Vec<&str> {
        v.iter().map(|x| x.field.as_str()).collect()
    }

    #[test]
    fn baseline_is_valid() {
        assert_eq!(validate(&body(base())), vec![]);
        assert!(validate_json(&base()).is_ok());
    }

    #[test]
    fn hash_is_stable_and_changes_with_any_limit() {
        let a = canonical_hash(&body(base()));
        assert_eq!(a, canonical_hash(&body(base())));
        assert_eq!(a.len(), 64);
        let mut v = base();
        v["loss"]["daily_loss_limit"] = json!(0.04);
        assert_ne!(a, canonical_hash(&body(v)));
    }

    #[test]
    fn serde_round_trip_is_lossless() {
        let m = body(base());
        let back: MandateBody = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(m, back);
    }

    // A1: a percent typed where a fraction belongs is rejected, with a hint.
    #[test]
    fn whole_number_percent_is_rejected_for_every_ratio() {
        let v = with(|m| {
            m["loss"]["daily_loss_limit"] = json!(3);
            m["loss"]["drawdown_ladder"][0]["at"] = json!(10);
            m["capital"]["min_cash_reserve"] = json!(5);
            m["exposure"]["max_position"] = json!(25);
        });
        for f in ["loss.daily_loss_limit", "loss.drawdown_ladder[0].at", "capital.min_cash_reserve", "exposure.max_position"] {
            assert!(fields(&v).contains(&f), "{f} should be rejected: {v:?}");
        }
        assert!(v.iter().any(|x| x.message.contains("fractions")), "hint should explain fractions: {v:?}");
    }

    #[test]
    fn boundary_ratios() {
        assert!(with(|m| m["capital"]["min_cash_reserve"] = json!(0.0)).is_empty(), "0 reserve is allowed");
        assert!(fields(&with(|m| m["capital"]["min_cash_reserve"] = json!(1.0))).contains(&"capital.min_cash_reserve"));
        assert!(fields(&with(|m| m["loss"]["daily_loss_limit"] = json!(0.0))).contains(&"loss.daily_loss_limit"));
        assert!(fields(&with(|m| m["loss"]["daily_loss_limit"] = json!(-0.1))).contains(&"loss.daily_loss_limit"));
    }

    // Unknown fields cannot be smuggled in.
    #[test]
    fn unknown_field_is_rejected() {
        let mut v = base();
        v["loss"]["max_loss_override"] = json!(0.9);
        let err = validate_json(&v).unwrap_err();
        assert!(err[0].message.contains("max_loss_override"), "{err:?}");
    }

    #[test]
    fn missing_field_names_the_field() {
        let mut v = base();
        v["capital"].as_object_mut().unwrap().remove("allocated");
        let err = validate_json(&v).unwrap_err();
        assert!(err[0].message.contains("allocated"), "{err:?}");
    }

    // D1: only own capital.
    #[test]
    fn third_party_and_mixed_capital_are_rejected() {
        for src in ["third_party", "mixed"] {
            let v = with(|m| m["basis"]["capital_source"] = json!(src));
            assert!(fields(&v).contains(&"basis.capital_source"), "{src}: {v:?}");
        }
    }

    #[test]
    fn required_acknowledgements_and_jurisdiction() {
        let v = with(|m| {
            m["basis"]["acknowledgements"].as_array_mut().unwrap().remove(0);
        });
        assert!(v.iter().any(|x| x.message.contains("own_capital_attestation")), "{v:?}");
        let v = with(|m| {
            m["basis"]["acknowledgements"].as_array_mut().unwrap().remove(1);
        });
        assert!(v.iter().any(|x| x.message.contains("risk_disclosure")), "{v:?}");
        assert!(fields(&with(|m| m["basis"]["jurisdiction"] = json!(" "))).contains(&"basis.jurisdiction"));
        assert!(fields(&with(|m| m["basis"]["jurisdiction"] = json!("US GA!"))).contains(&"basis.jurisdiction"));
    }

    #[test]
    fn ladder_rules() {
        // not strictly increasing
        let v = with(|m| m["loss"]["drawdown_ladder"][1]["at"] = json!(0.10));
        assert!(fields(&v).contains(&"loss.drawdown_ladder[1].at"), "{v:?}");
        // last rung must halt
        let v = with(|m| {
            m["loss"]["drawdown_ladder"][1] = json!({"at": 0.20, "action": "shrink", "scale": 0.25});
        });
        assert!(fields(&v).contains(&"loss.drawdown_ladder[1]"), "{v:?}");
        // halt before the end
        let v = with(|m| {
            m["loss"]["drawdown_ladder"] = json!([
                {"at": 0.10, "action": "halt_flatten"},
                {"at": 0.20, "action": "halt_flatten"}
            ]);
        });
        assert!(fields(&v).contains(&"loss.drawdown_ladder[0]"), "{v:?}");
        // shrink scale out of range, missing, or scaling back up
        for bad in [json!(0.0), json!(1.0), json!(1.5)] {
            let v = with(|m| m["loss"]["drawdown_ladder"][0]["scale"] = bad.clone());
            assert!(fields(&v).contains(&"loss.drawdown_ladder[0].scale"), "{bad}: {v:?}");
        }
        let v = with(|m| {
            m["loss"]["drawdown_ladder"][0].as_object_mut().unwrap().remove("scale");
        });
        assert!(fields(&v).contains(&"loss.drawdown_ladder[0].scale"), "{v:?}");
        let v = with(|m| {
            m["loss"]["drawdown_ladder"] = json!([
                {"at": 0.05, "action": "shrink", "scale": 0.5},
                {"at": 0.10, "action": "shrink", "scale": 0.8},
                {"at": 0.20, "action": "halt_flatten"}
            ]);
        });
        assert!(fields(&v).contains(&"loss.drawdown_ladder[1].scale"), "{v:?}");
        // halt rung with a scale
        let v = with(|m| m["loss"]["drawdown_ladder"][1]["scale"] = json!(0.5));
        assert!(fields(&v).contains(&"loss.drawdown_ladder[1].scale"), "{v:?}");
        // empty ladder
        let v = with(|m| m["loss"]["drawdown_ladder"] = json!([]));
        assert!(fields(&v).contains(&"loss.drawdown_ladder"), "{v:?}");
    }

    #[test]
    fn daily_loss_cannot_exceed_final_rung_but_may_equal_it() {
        let v = with(|m| m["loss"]["daily_loss_limit"] = json!(0.25));
        assert!(fields(&v).contains(&"loss.daily_loss_limit"), "{v:?}");
        assert!(with(|m| m["loss"]["daily_loss_limit"] = json!(0.20)).is_empty());
    }

    #[test]
    fn exposure_ordering() {
        // position above the gross limit (the "typed 25 for 25%" mistake)
        let v = with(|m| m["exposure"]["max_position"] = json!(25));
        assert!(fields(&v).contains(&"exposure.max_position"), "{v:?}");
        // net above gross
        let mut v = base();
        v["exposure"]["max_gross"] = json!(0.8);
        v["exposure"]["max_net"] = json!(0.9);
        v["exposure"]["max_position"] = json!(0.2);
        v["exposure"]["max_asset_class"] = json!({});
        assert!(fields(&validate(&body(v))).contains(&"exposure.max_net"));
        // gross above the leverage cap; allowed once the cap is raised
        let v = with(|m| m["exposure"]["max_gross"] = json!(1.5));
        assert!(fields(&v).contains(&"exposure.max_gross"), "{v:?}");
        let v = with(|m| {
            m["exposure"]["max_gross"] = json!(1.5);
            m["universe"]["leverage_max_gross"] = json!(2.0);
        });
        assert!(v.is_empty(), "{v:?}");
        // leverage below 1
        assert!(fields(&with(|m| m["universe"]["leverage_max_gross"] = json!(0.5))).contains(&"universe.leverage_max_gross"));
        // asset-class cap for a class that is not in the universe
        let v = with(|m| m["exposure"]["max_asset_class"] = json!({"fx": 0.5}));
        assert!(fields(&v).contains(&"exposure.max_asset_class.fx"), "{v:?}");
        // zero orders per day
        assert!(fields(&with(|m| m["exposure"]["max_orders_per_day"] = json!(0))).contains(&"exposure.max_orders_per_day"));
    }

    #[test]
    fn money_rules() {
        let v = with(|m| m["exposure"]["max_order_notional"]["amount"] = json!("6000.00"));
        assert!(fields(&v).contains(&"exposure.max_order_notional"), "{v:?}");
        let v = with(|m| m["exposure"]["max_order_notional"]["ccy"] = json!("EUR"));
        assert!(fields(&v).contains(&"exposure.max_order_notional.ccy"), "{v:?}");
        let v = with(|m| m["capital"]["allocated"]["amount"] = json!("five thousand"));
        assert!(fields(&v).contains(&"capital.allocated.amount"), "{v:?}");
        let v = with(|m| m["capital"]["allocated"]["amount"] = json!("0"));
        assert!(fields(&v).contains(&"capital.allocated.amount"), "{v:?}");
        let v = with(|m| m["capital"]["allocated"]["ccy"] = json!("usd"));
        assert!(fields(&v).contains(&"capital.allocated.ccy"), "{v:?}");
        // equal to the allocation is allowed
        assert!(with(|m| m["exposure"]["max_order_notional"]["amount"] = json!("5000")).is_empty());
    }

    #[test]
    fn universe_rules() {
        assert!(fields(&with(|m| m["universe"]["instrument_allow"] = json!([]))).contains(&"universe.instrument_allow"));
        assert!(fields(&with(|m| m["universe"]["venues"] = json!(["kraken", " "]))).contains(&"universe.venues"));
        let v = with(|m| m["universe"]["instrument_deny"] = json!(["spy"]));
        assert!(fields(&v).contains(&"universe.instrument_deny"), "case-insensitive overlap: {v:?}");
    }

    // A5: the `never` list is not editable.
    #[test]
    fn never_list_must_keep_all_four() {
        for gone in ["change_limits", "resume_after_halt", "change_credentials", "withdraw"] {
            let v = with(|m| {
                let never = m["autonomy"]["never"].as_array_mut().unwrap();
                never.retain(|x| x != gone);
            });
            assert!(fields(&v).contains(&"autonomy.never"), "{gone}: {v:?}");
        }
    }

    #[test]
    fn an_action_cannot_sit_in_two_lists() {
        let v = with(|m| m["autonomy"]["may"].as_array_mut().unwrap().push(json!("withdraw")));
        assert!(!v.is_empty(), "withdraw in may and never must be rejected: {v:?}");
        let v = with(|m| m["autonomy"]["needs_confirmation"].as_array_mut().unwrap().push(json!("rebalance")));
        assert!(fields(&v).contains(&"autonomy.needs_confirmation") || fields(&v).contains(&"autonomy.may"), "{v:?}");
    }

    #[test]
    fn order_actions_need_level_two_but_halt_does_not() {
        for level in ["L0", "L1"] {
            let v = with(|m| m["autonomy"]["level"] = json!(level));
            assert!(fields(&v).contains(&"autonomy.may"), "{level}: {v:?}");
        }
        assert!(with(|m| m["autonomy"]["level"] = json!("L2")).is_empty());
        // suggest-only mandate that may still halt
        let v = with(|m| {
            m["autonomy"]["level"] = json!("L0");
            m["autonomy"]["may"] = json!(["halt"]);
        });
        assert!(v.is_empty(), "{v:?}");
    }

    #[test]
    fn alerts_are_required() {
        assert!(fields(&with(|m| m["reporting"]["alert_channels"] = json!([]))).contains(&"reporting.alert_channels"));
    }

    #[test]
    fn all_violations_are_reported_together() {
        let v = with(|m| {
            m["loss"]["daily_loss_limit"] = json!(3);
            m["capital"]["allocated"]["amount"] = json!("abc");
            m["basis"]["capital_source"] = json!("third_party");
        });
        assert!(v.len() >= 3, "expected several violations at once, got {v:?}");
    }

    #[test]
    fn non_finite_numbers_are_rejected() {
        // serde_json cannot carry NaN, so build the struct directly.
        let mut m = body(base());
        m.loss.daily_loss_limit = f64::NAN;
        m.exposure.max_gross = f64::INFINITY;
        let v = validate(&m);
        assert!(fields(&v).contains(&"loss.daily_loss_limit"));
        assert!(fields(&v).contains(&"exposure.max_gross"));
    }
}
