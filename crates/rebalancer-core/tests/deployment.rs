//! Effective limit = min(mandate, deployment) (SPEC B9): a deployment can be tighter than the mandate, never looser.

mod common;

use common::*;
use rebalancer_core::guard::{DayCounters, DenialCode, PreTradeGuard};
use rebalancer_core::policy::DeploymentLimits;
use rebalancer_core::Dec;

fn limits(p: &rebalancer_core::policy::Policy) -> rebalancer_core::policy::Limits {
    p.limits().expect("valid").clone()
}

#[test]
fn a_tighter_deployment_is_honoured_for_every_cap() {
    let base = policy(); // allocated 5000, position 0.25, gross 1, net 1, notional 1500, 20 orders, turnover 0.5, reserve 0.05
    let dep = DeploymentLimits {
        capital_allocation: Some(d("2000")),
        max_position: Some(d("0.1")),
        max_gross: Some(d("0.6")),
        max_net: Some(d("0.5")),
        max_order_notional: Some(d("400")),
        max_orders_per_day: Some(3),
        max_turnover_per_day: Some(d("0.2")),
        min_cash_reserve: Some(d("0.1")),
        max_asset_class: [("crypto_spot".to_string(), d("0.3")), ("US_ETF".to_string(), d("0.5"))].into(),
    };
    let l = limits(&base.clone().with_deployment(&dep));
    assert_eq!(l.allocated, d("2000"));
    assert_eq!(l.max_position, d("0.1"));
    assert_eq!(l.max_gross, d("0.6"));
    assert_eq!(l.max_net, d("0.5"));
    assert_eq!(l.max_order_notional, d("400"));
    assert_eq!(l.max_orders_per_day, 3);
    assert_eq!(l.max_turnover_per_day, d("0.2"));
    assert_eq!(l.min_cash_reserve, d("0.1"));
    assert_eq!(l.max_asset_class["crypto_spot"], d("0.3"), "the tighter of 0.6 and 0.3");
    assert_eq!(l.max_asset_class["us_etf"], d("0.5"), "a class the mandate does not cap is capped by the deployment");
}

#[test]
fn a_looser_deployment_cannot_loosen_anything() {
    let base = policy();
    let dep = DeploymentLimits {
        capital_allocation: Some(d("50000")),
        max_position: Some(d("0.9")),
        max_gross: Some(d("3")),
        max_net: Some(d("3")),
        max_order_notional: Some(d("99999")),
        max_orders_per_day: Some(1000),
        max_turnover_per_day: Some(d("10")),
        min_cash_reserve: Some(d("0.01")),
        max_asset_class: [("crypto_spot".to_string(), d("0.95"))].into(),
    };
    assert_eq!(limits(&base.clone().with_deployment(&dep)), limits(&base), "every looser value is ignored");
    assert_eq!(base.clone().with_deployment(&DeploymentLimits::default()), base, "no deployment limits: unchanged");
}

#[test]
fn the_effective_limit_is_never_looser_than_the_mandate_for_random_deployments() {
    let mut rng = SplitMix64(0xDE91_0141);
    let base = policy();
    let bl = limits(&base);
    let pick = |rng: &mut SplitMix64| -> Option<Dec> {
        if rng.chance(30) {
            None
        } else {
            Some(Dec::new(i128::from(rng.range(1, 400)), 2).unwrap())
        }
    };
    let mut tightened = 0;
    for _ in 0..500 {
        let dep = DeploymentLimits {
            capital_allocation: pick(&mut rng).map(|v| v.checked_mul(Dec::from_i64(3000)).unwrap()),
            max_position: pick(&mut rng),
            max_gross: pick(&mut rng),
            max_net: pick(&mut rng),
            max_order_notional: pick(&mut rng).map(|v| v.checked_mul(Dec::from_i64(1000)).unwrap()),
            max_orders_per_day: if rng.chance(50) { Some(rng.range(0, 60) as u32) } else { None },
            max_turnover_per_day: pick(&mut rng),
            min_cash_reserve: pick(&mut rng).map(|v| Dec::new(v.units() / 100, 2).unwrap()),
            max_asset_class: if rng.chance(50) { [("crypto_spot".to_string(), pick(&mut rng).unwrap_or(d("1")))].into() } else { Default::default() },
        };
        let e = limits(&base.clone().with_deployment(&dep));
        assert!(e.allocated <= bl.allocated && e.max_position <= bl.max_position && e.max_gross <= bl.max_gross);
        assert!(e.max_net <= bl.max_net && e.max_order_notional <= bl.max_order_notional && e.max_turnover_per_day <= bl.max_turnover_per_day);
        assert!(e.max_orders_per_day <= bl.max_orders_per_day);
        assert!(e.min_cash_reserve >= bl.min_cash_reserve, "the reserve only ever goes UP");
        assert!(e.max_asset_class["crypto_spot"] <= bl.max_asset_class["crypto_spot"]);
        tightened += u32::from(e != bl);
    }
    assert!(tightened > 300, "the generator must actually tighten things, saw {tightened}");
}

#[test]
fn the_guard_enforces_the_tighter_deployment_limit() {
    // Mandate: max_position 0.25 of 5000 = 1250. Deployment: 0.1 = 500.
    let dep = DeploymentLimits { max_position: Some(d("0.1")), ..DeploymentLimits::default() };
    let tight = policy().with_deployment(&dep);
    let acct = flat();
    let v = PreTradeGuard::check(&tight, &acct, &buy("SPY", "5", "100"), &DayCounters::ZERO); // 500.00
    assert!(v.allow, "{:?}", v.reasons);
    let v = PreTradeGuard::check(&tight, &acct, &buy("SPY", "5.01", "100"), &DayCounters::ZERO);
    assert_eq!(v.codes(), vec![DenialCode::MaxPosition]);
    // The mandate alone would have allowed it.
    assert!(PreTradeGuard::check(&policy(), &acct, &buy("SPY", "5.01", "100"), &DayCounters::ZERO).allow);
    // And a looser deployment leaves the mandate's 1250 in force.
    let loose = policy().with_deployment(&DeploymentLimits { max_position: Some(d("0.9")), ..DeploymentLimits::default() });
    assert!(!PreTradeGuard::check(&loose, &acct, &buy("SPY", "12.51", "100"), &DayCounters::ZERO).allow);
    assert!(PreTradeGuard::check(&loose, &acct, &buy("SPY", "12.5", "100"), &DayCounters::ZERO).allow);
}

#[test]
fn the_deployment_capital_allocation_shrinks_the_capital_base() {
    let p = policy().with_deployment(&DeploymentLimits { capital_allocation: Some(d("2000")), ..DeploymentLimits::default() });
    assert_eq!(p.capital_base(d("20000"), "USD"), d("2000"));
    // 0.25 * 2000 = 500 per position even though the account holds 20000.
    let acct = account("20000", "20000", vec![]);
    assert!(PreTradeGuard::check(&p, &acct, &buy("SPY", "5", "100"), &DayCounters::ZERO).allow);
    assert!(!PreTradeGuard::check(&p, &acct, &buy("SPY", "5.01", "100"), &DayCounters::ZERO).allow);
}

#[test]
fn an_invalid_policy_stays_invalid_and_the_mandate_identity_is_unchanged() {
    let bad = policy_with(|m| m["exposure"]["max_position"] = serde_json::json!(25));
    let after = bad.clone().with_deployment(&DeploymentLimits { max_position: Some(d("0.1")), ..DeploymentLimits::default() });
    assert_eq!(after, bad);
    let base = policy();
    let dep = DeploymentLimits { max_position: Some(d("0.1")), ..DeploymentLimits::default() };
    assert_eq!(base.clone().with_deployment(&dep).body_hash, base.body_hash);
    assert_eq!(DeploymentLimits::default().digest(), "cap=-|pos=-|gross=-|net=-|notional=-|orders=-|turnover=-|reserve=-|classes=");
    assert!(dep.digest().contains("pos=0.1"), "{}", dep.digest());
}
