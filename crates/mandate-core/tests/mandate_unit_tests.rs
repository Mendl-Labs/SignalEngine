//! Copy of the unit tests in `src/mandate.rs` (the `tests` module), run as an integration test against the PUBLIC
//! API of `mandate_core::mandate`. Only change from the Engine text: `use super::*;` became
//! `use mandate_core::mandate::*;`. Kept so a change to the validator that breaks the Engine tests is caught here
//! even if the inline module is edited. Do not edit by hand: regenerate from `src/mandate.rs`.

use mandate_core::mandate::*;
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
