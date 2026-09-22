//! Shared helpers for the rebalancer-risk integration tests.
#![allow(dead_code)]

use chrono::{DateTime, NaiveDate, Utc};
use mandate_core::mandate::MandateBody;
use rebalancer_risk::approval::{issue_human_approval, AuthenticatedPrincipal, HumanApproval, PrincipalKind};
use rebalancer_risk::overlay::{EquitySnapshot, RiskPolicy, Rung, RungAction};
use rebalancer_risk::state::AccountState;
use rebalancer_risk::Dec;

pub const BASELINE: &str = include_str!("../../../mandate-core/tests/fixtures/baseline_mandate.json");

pub fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

pub fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

pub fn t0() -> DateTime<Utc> {
    at("2026-09-21T15:00:00Z")
}

pub fn day(n: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, n).unwrap()
}

pub fn baseline_body() -> MandateBody {
    serde_json::from_str(BASELINE).unwrap()
}

/// The baseline mandate's policy: daily loss 3%, shrink 0.5 at 10%, halt_flatten at 20%, recovery fraction 0.5.
pub fn policy() -> RiskPolicy {
    RiskPolicy::from_mandate(&baseline_body()).unwrap()
}

/// A three-rung policy: shrink 0.75 at 5%, shrink 0.5 at 10%, halt at 20%; daily loss 3%.
pub fn three_rung_policy() -> RiskPolicy {
    RiskPolicy::new(
        d("0.03"),
        vec![
            Rung { at: d("0.05"), action: RungAction::Shrink { scale: d("0.75") } },
            Rung { at: d("0.10"), action: RungAction::Shrink { scale: d("0.5") } },
            Rung { at: d("0.20"), action: RungAction::HaltFlatten },
        ],
        d("0.5"),
    )
    .unwrap()
}

pub fn snap(equity: &str) -> EquitySnapshot {
    EquitySnapshot::broker_reported("kraken", "USD", d(equity), t0())
}

/// An Active state whose high-water mark is `hwm` and whose day-start equity equals `equity` on day 2 (so the
/// daily-loss limit does not interfere with a pure drawdown test), already holding `equity`.
pub fn state_with_hwm(hwm: &str, equity: &str) -> AccountState {
    AccountState::new("acct").observe(&snap(hwm), day(1)).observe(&snap(equity), day(2))
}

/// An Active state whose day started at `day_start` and whose hwm equals it (a pure daily-loss test), on day 1.
pub fn state_day_start(day_start: &str) -> AccountState {
    AccountState::new("acct").observe(&snap(day_start), day(1))
}

pub struct Person(pub &'static str, pub PrincipalKind);

impl AuthenticatedPrincipal for Person {
    fn subject(&self) -> &str {
        self.0
    }
    fn kind(&self) -> PrincipalKind {
        self.1
    }
}

/// What the authenticated-human endpoint does, in a test.
pub fn human_approval(note: &str) -> HumanApproval {
    issue_human_approval(&Person("user_owner", PrincipalKind::Human), note, t0()).expect("a human may approve")
}
