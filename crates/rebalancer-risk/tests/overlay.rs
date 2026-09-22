//! The risk overlay: exact ladder and daily-loss boundaries, hysteresis, sticky halts, fail-closed inputs, and the
//! pinned machine codes.

mod common;

use common::*;
use mandate_core::mandate::LadderAction;
use rebalancer_risk::overlay::{
    apply_decision, evaluate_risk, step, EquitySnapshot, RiskAction, RiskCode, RiskDecision, RiskPolicy, Rung, RungAction,
};
use rebalancer_risk::state::{AccountState, AccountStatus, HaltReason};
use rebalancer_risk::Dec;

fn eval(state: &AccountState, equity: &str, policy: &RiskPolicy) -> RiskDecision {
    evaluate_risk(state, &snap(equity), policy)
}

fn kind(dec: &RiskDecision) -> &'static str {
    match dec.action {
        RiskAction::None => "none",
        RiskAction::Shrink { .. } => "shrink",
        RiskAction::HaltFlatten => "halt",
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Codes
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn risk_code_strings_are_pinned_and_unique() {
    let expected = [
        "RISK_NO_ACTION",
        "RISK_DRAWDOWN_SHRINK",
        "RISK_SHRINK_HELD",
        "RISK_PARTIAL_RECOVERY",
        "RISK_RECOVERED",
        "RISK_DRAWDOWN_HALT",
        "RISK_DAILY_LOSS_HALT",
        "RISK_ALREADY_HALTED",
        "RISK_EQUITY_INVALID",
        "RISK_ARITHMETIC_OVERFLOW",
    ];
    let actual: Vec<&str> = RiskCode::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(actual, expected);
    assert_eq!(actual.iter().collect::<std::collections::BTreeSet<_>>().len(), actual.len());
}

#[test]
fn halt_reason_and_state_codes_are_pinned() {
    let codes: Vec<&str> = HaltReason::ALL.iter().map(|r| r.code()).collect();
    assert_eq!(codes, ["HALT_DAILY_LOSS", "HALT_DRAWDOWN_LADDER", "HALT_RECONCILIATION", "HALT_MANUAL", "HALT_EQUITY_INVALID"]);
    let all: Vec<&str> = rebalancer_risk::state::all_state_codes().into_iter().collect();
    assert!(all.contains(&"RESUMED") && all.contains(&"RESUME_NOT_HALTED"));
    assert_eq!(rebalancer_risk::store::StoreError::VersionConflict { expected: 1, actual: 2 }.code(), "STORE_VERSION_CONFLICT");
    assert_eq!(rebalancer_risk::store::StoreError::IllegalTransition(String::new()).code(), "STORE_ILLEGAL_TRANSITION");
    assert_eq!(rebalancer_risk::store::StoreError::Unavailable(String::new()).code(), "STORE_UNAVAILABLE");
}

// ---------------------------------------------------------------------------------------------------------------
// Drawdown ladder boundaries (hwm 10000; rung 1 shrink 0.5 at 10% = 9000; halt at 20% = 8000)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn ladder_rung_one_triggers_exactly_at_the_boundary() {
    let p = policy();
    let cases = [
        ("10000", "none"),
        ("9000.01", "none"),   // just below the trigger (drawdown 9.9999%)
        ("9000.00", "shrink"), // exactly at
        ("8999.99", "shrink"), // just past
        ("8000.01", "shrink"),
    ];
    for (equity, want) in cases {
        let dec = eval(&state_with_hwm("10000", equity), equity, &p);
        assert_eq!(kind(&dec), want, "equity {equity}: {dec:?}");
    }
    let at_rung = eval(&state_with_hwm("10000", "9000"), "9000", &p);
    assert_eq!(at_rung.risk_scale, d("0.5"));
    assert_eq!(at_rung.action, RiskAction::Shrink { scale: d("0.5") });
    assert_eq!(at_rung.codes(), ["RISK_DRAWDOWN_SHRINK"]);
    assert_eq!(at_rung.next_rung, Some(0));
    assert_eq!(eval(&state_with_hwm("10000", "9000.01"), "9000.01", &p).risk_scale, d("1"));
}

#[test]
fn ladder_halt_rung_triggers_exactly_at_the_boundary() {
    let p = policy();
    let below = eval(&state_with_hwm("10000", "8000.01"), "8000.01", &p);
    assert_eq!(kind(&below), "shrink", "just above the halt rung is still only a shrink");
    let at = eval(&state_with_hwm("10000", "8000"), "8000", &p);
    assert_eq!(at.action, RiskAction::HaltFlatten);
    assert_eq!(at.risk_scale, Dec::ZERO);
    assert_eq!(at.halt_reason, Some(HaltReason::DrawdownLadder));
    assert!(at.has(RiskCode::DrawdownHalt));
    assert_eq!(kind(&eval(&state_with_hwm("10000", "7999.99"), "7999.99", &p)), "halt");
    assert_eq!(kind(&eval(&state_with_hwm("10000", "1"), "1", &p)), "halt");
}

#[test]
fn boundaries_scale_with_the_high_water_mark_not_with_a_constant() {
    let p = policy();
    // hwm 12345.67: 10% = 1234.567, so the trigger equity is 11111.103.
    assert_eq!(kind(&eval(&state_with_hwm("12345.67", "11111.104"), "11111.104", &p)), "none");
    assert_eq!(kind(&eval(&state_with_hwm("12345.67", "11111.103"), "11111.103", &p)), "shrink");
    // 20% = 2469.134 -> 9876.536
    assert_eq!(kind(&eval(&state_with_hwm("12345.67", "9876.537"), "9876.537", &p)), "shrink");
    assert_eq!(kind(&eval(&state_with_hwm("12345.67", "9876.536"), "9876.536", &p)), "halt");
}

#[test]
fn a_new_high_is_not_a_drawdown_and_moves_the_mark() {
    let p = policy();
    let s = state_with_hwm("10000", "10000");
    let (next, dec, _) = step(&s, &snap("11000"), day(2), &p);
    assert_eq!(dec.action, RiskAction::None);
    assert_eq!(next.hwm(), Some(d("11000")));
    // 10% below the NEW mark is 9900.
    assert_eq!(kind(&eval(&next, "9900", &p)), "shrink");
    assert_eq!(kind(&eval(&next, "9900.01", &p)), "none");
}

// ---------------------------------------------------------------------------------------------------------------
// Daily loss (3% of day-start equity)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn daily_loss_halts_exactly_at_the_limit() {
    let p = policy();
    let s = state_day_start("10000");
    assert_eq!(kind(&eval(&s, "9700.01", &p)), "none"); // 2.9999% (and drawdown 3% < 10%)
    let at = eval(&s, "9700", &p);
    assert_eq!(at.action, RiskAction::HaltFlatten, "{at:?}");
    assert_eq!(at.halt_reason, Some(HaltReason::DailyLoss));
    assert_eq!(at.codes(), ["RISK_DAILY_LOSS_HALT"]);
    assert_eq!(kind(&eval(&s, "9699.99", &p)), "halt");
    // Gains never trip it.
    assert_eq!(kind(&eval(&s, "10500", &p)), "none");
}

#[test]
fn daily_loss_is_measured_from_the_days_start_not_from_the_high_water_mark() {
    let p = policy();
    // hwm 12000 but the day started at 10000: a fall to 9700 is a 3% daily loss.
    let s = AccountState::new("acct").observe(&snap("12000"), day(1)).observe(&snap("10000"), day(2));
    let dec = eval(&s, "9700", &p);
    assert_eq!(dec.action, RiskAction::HaltFlatten);
    assert_eq!(dec.halt_reason, Some(HaltReason::DailyLoss));
    // A new trading day re-bases: the same 9700 the next morning is a fresh day-start, not a daily loss.
    let next_day = s.observe(&snap("9700"), day(3));
    assert_eq!(next_day.day_start_equity(), Some(d("9700")));
    assert_ne!(eval(&next_day, "9700", &p).halt_reason, Some(HaltReason::DailyLoss));
}

#[test]
fn both_limits_hit_reports_both_and_prefers_the_daily_loss_reason() {
    let p = policy();
    let s = state_day_start("10000");
    let dec = eval(&s, "7900", &p); // -21%: daily loss AND the halt rung
    assert_eq!(dec.halt_reason, Some(HaltReason::DailyLoss));
    assert!(dec.has(RiskCode::DailyLossHalt) && dec.has(RiskCode::DrawdownHalt), "{:?}", dec.codes());
}

// ---------------------------------------------------------------------------------------------------------------
// Equity comes from the snapshot; fail closed on bad input
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_decision_uses_the_broker_snapshot_not_any_remembered_number() {
    let p = policy();
    let s = state_with_hwm("10000", "10000"); // last_equity says 10000 (a "fine" bookkeeping figure)
    assert_eq!(s.last_equity(), Some(d("10000")));
    let dec = eval(&s, "8000", &p); // the broker says 8000
    assert_eq!(dec.action, RiskAction::HaltFlatten);
}

#[test]
fn non_positive_equity_halts_fail_closed() {
    let p = policy();
    for e in ["0", "-1", "-0.01"] {
        let dec = eval(&state_with_hwm("10000", "10000"), e, &p);
        assert_eq!(dec.action, RiskAction::HaltFlatten, "{e}");
        assert_eq!(dec.halt_reason, Some(HaltReason::EquityInvalid));
        assert!(dec.has(RiskCode::EquityInvalid));
    }
}

#[test]
fn arithmetic_overflow_halts_fail_closed() {
    let p = policy();
    let huge = Dec::new(i128::MAX / 2, 0).unwrap();
    let s = AccountState::new("acct").observe(&EquitySnapshot::broker_reported("kraken", "USD", huge, t0()), day(1));
    let dec = evaluate_risk(&s, &EquitySnapshot::broker_reported("kraken", "USD", d("1"), t0()), &p);
    assert_eq!(dec.action, RiskAction::HaltFlatten);
    assert!(dec.has(RiskCode::ArithmeticOverflow), "{:?}", dec.codes());
}

#[test]
fn evaluate_is_not_lenient_when_the_caller_forgot_to_observe() {
    // A policy whose daily-loss limit is wide, so only the drawdown ladder speaks.
    let p = RiskPolicy::new(
        d("0.5"),
        vec![Rung { at: d("0.1"), action: RungAction::Shrink { scale: d("0.5") } }, Rung { at: d("0.2"), action: RungAction::HaltFlatten }],
        d("0.5"),
    )
    .unwrap();
    // State knows hwm 10000; evaluating a 9000 snapshot WITHOUT observe still sees the drawdown.
    let s = AccountState::new("acct").observe(&snap("10000"), day(1));
    assert_eq!(kind(&eval(&s, "9000", &p)), "shrink");
    // And with no mark at all, the snapshot itself is the mark: no drawdown.
    assert_eq!(kind(&eval(&AccountState::new("a"), "500", &p)), "none");
}

// ---------------------------------------------------------------------------------------------------------------
// Shrink recovery and hysteresis
// ---------------------------------------------------------------------------------------------------------------

fn shrunk_state(p: &RiskPolicy) -> AccountState {
    let s = state_with_hwm("10000", "9000");
    let (next, dec, tr) = step(&s, &snap("9000"), day(2), p);
    assert_eq!(dec.action, RiskAction::Shrink { scale: d("0.5") });
    assert_eq!(tr.unwrap().to, AccountStatus::Shrunk);
    next
}

#[test]
fn a_shrink_is_released_only_when_the_drawdown_is_back_within_half_of_the_rung() {
    let p = policy(); // recovery fraction 0.5: release at drawdown <= 5% (equity >= 9500)
    let s = shrunk_state(&p);
    assert_eq!(s.status(), AccountStatus::Shrunk);
    assert_eq!(s.risk_scale(), d("0.5"));
    for equity in ["9000", "9100", "9400", "9499.99"] {
        let dec = eval(&s, equity, &p);
        assert_eq!(dec.action, RiskAction::Shrink { scale: d("0.5") }, "equity {equity}");
        assert!(dec.has(RiskCode::ShrinkHeld) || dec.has(RiskCode::DrawdownShrink), "{:?}", dec.codes());
    }
    let dec = eval(&s, "9500", &p); // exactly at half the rung: released (inclusive)
    assert_eq!(dec.action, RiskAction::None, "{dec:?}");
    assert_eq!(dec.codes(), ["RISK_RECOVERED"]);
    assert_eq!(dec.risk_scale, d("1"));
    // The same equity from an ACTIVE state is simply "no action": the hysteresis only holds an existing shrink.
    assert_eq!(kind(&eval(&state_with_hwm("10000", "9400"), "9400", &p)), "none");
}

#[test]
fn shrink_holds_while_recovering_then_apply_returns_to_active() {
    let p = policy();
    let s = shrunk_state(&p);
    let (held, dec, tr) = step(&s, &snap("9300"), day(2), &p);
    assert_eq!(held.status(), AccountStatus::Shrunk);
    assert!(tr.is_none(), "still shrunk: no transition");
    assert_eq!(dec.codes(), ["RISK_SHRINK_HELD"]);
    let (back, dec, tr) = step(&held, &snap("9600"), day(2), &p);
    assert_eq!(back.status(), AccountStatus::Active);
    assert_eq!(back.risk_scale(), d("1"));
    assert_eq!(dec.codes(), ["RISK_RECOVERED"]);
    let tr = tr.unwrap();
    assert_eq!((tr.from, tr.to, tr.code), (AccountStatus::Shrunk, AccountStatus::Active, "RISK_RECOVERED"));
}

#[test]
fn the_recovery_fraction_is_a_documented_parameter() {
    let strict = policy().with_recovery_fraction(d("0.2")).unwrap(); // release at drawdown <= 2% (equity >= 9800)
    let s = shrunk_state(&strict);
    assert_eq!(kind(&eval(&s, "9799.99", &strict)), "shrink");
    assert_eq!(kind(&eval(&s, "9800", &strict)), "none");
    let none = policy().with_recovery_fraction(d("1")).unwrap(); // no hysteresis: release as soon as it is below the rung
    let s = shrunk_state(&none);
    assert_eq!(kind(&eval(&s, "9000", &none)), "shrink", "at the trigger the raw rung hits again");
    assert_eq!(kind(&eval(&s, "9000.01", &none)), "none");
    assert!(policy().with_recovery_fraction(d("0")).is_err());
    assert!(policy().with_recovery_fraction(d("1.01")).is_err());
}

#[test]
fn several_shrink_rungs_step_down_one_at_a_time() {
    let p = three_rung_policy(); // 5% -> 0.75, 10% -> 0.5, 20% halt; recovery 0.5
    let s = state_with_hwm("10000", "8900"); // -11%
    let (s, dec, _) = step(&s, &snap("8900"), day(2), &p);
    assert_eq!(dec.next_rung, Some(1));
    assert_eq!(s.risk_scale(), d("0.5"));
    // Drawdown 4%: rung 1 (trigger 10%, release <= 5%) is released, rung 0 (trigger 5%, release <= 2.5%) is not.
    let (s, dec, _) = step(&s, &snap("9600"), day(2), &p);
    assert_eq!(dec.codes(), ["RISK_PARTIAL_RECOVERY"], "{dec:?}");
    assert_eq!(dec.next_rung, Some(0));
    assert_eq!(s.risk_scale(), d("0.75"));
    assert_eq!(s.status(), AccountStatus::Shrunk);
    // Drawdown 2.5%: rung 0 released too.
    let (s, dec, _) = step(&s, &snap("9750"), day(2), &p);
    assert_eq!(dec.codes(), ["RISK_RECOVERED"]);
    assert_eq!(s.status(), AccountStatus::Active);
    // Jumping straight from Active to the second rung is one decision.
    let (s, dec, _) = step(&s, &snap("9000"), day(2), &p);
    assert_eq!(dec.next_rung, Some(1));
    assert_eq!(s.risk_scale(), d("0.5"));
}

// ---------------------------------------------------------------------------------------------------------------
// Halt is sticky
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_halted_or_flattening_account_gets_scale_zero_whatever_the_equity() {
    let p = policy();
    let s = state_with_hwm("10000", "7000");
    let (flattening, dec, tr) = step(&s, &snap("7000"), day(2), &p);
    assert_eq!(dec.action, RiskAction::HaltFlatten);
    assert_eq!(flattening.status(), AccountStatus::Flattening);
    assert_eq!(tr.unwrap().to, AccountStatus::Flattening);
    let (halted, _) = flattening.complete_flatten(true);
    assert_eq!(halted.status(), AccountStatus::Halted);
    for st in [&flattening, &halted] {
        for equity in ["1", "7000", "10000", "50000", "1000000"] {
            let dec = eval(st, equity, &p);
            assert_eq!(dec.action, RiskAction::None, "{equity}");
            assert_eq!(dec.risk_scale, Dec::ZERO);
            assert_eq!(dec.codes(), ["RISK_ALREADY_HALTED"]);
            let (again, tr) = apply_decision(st, &dec, t0());
            assert_eq!(&again, st);
            assert!(tr.is_none());
        }
    }
}

#[test]
fn halt_details_are_recorded() {
    let p = policy();
    let (s, _, _) = step(&state_with_hwm("10000", "7000"), &snap("7000"), day(2), &p);
    let h = s.halt_record().unwrap();
    assert_eq!(h.reason, HaltReason::DrawdownLadder);
    assert_eq!(h.equity_at_halt, Some(d("7000")));
    assert_eq!(h.hwm_at_halt, Some(d("10000")));
    assert!(h.detail.contains("RISK_DRAWDOWN_HALT"), "{}", h.detail);
    assert_eq!(s.risk_scale(), Dec::ZERO, "a flattening account has no risk budget");
}

// ---------------------------------------------------------------------------------------------------------------
// Policy construction
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn from_mandate_reads_the_baseline_exactly() {
    let p = policy();
    assert_eq!(p.daily_loss_limit(), d("0.03"));
    assert_eq!(p.recovery_fraction(), d("0.5"));
    assert_eq!(p.rungs().len(), 2);
    assert_eq!(p.rungs()[0], Rung { at: d("0.1"), action: RungAction::Shrink { scale: d("0.5") } });
    assert_eq!(p.rungs()[1], Rung { at: d("0.2"), action: RungAction::HaltFlatten });
}

#[test]
fn an_invalid_mandate_never_yields_a_policy() {
    let mut b = baseline_body();
    b.loss.daily_loss_limit = 3.0; // "3%" typed as 3
    let e = RiskPolicy::from_mandate(&b).unwrap_err();
    assert!(e.to_string().starts_with("RISK_POLICY_INVALID"), "{e}");
    assert!(e.to_string().contains("loss.daily_loss_limit"), "{e}");
    let mut b = baseline_body();
    b.loss.drawdown_ladder[1].action = LadderAction::Shrink; // the last rung must halt
    assert!(RiskPolicy::from_mandate(&b).is_err());
}

#[test]
fn policy_new_enforces_the_ladder_invariants() {
    let shrink = |at: &str, s: &str| Rung { at: d(at), action: RungAction::Shrink { scale: d(s) } };
    let halt = |at: &str| Rung { at: d(at), action: RungAction::HaltFlatten };
    let ok = |r: Vec<Rung>| RiskPolicy::new(d("0.03"), r, d("0.5"));
    assert!(ok(vec![shrink("0.1", "0.5"), halt("0.2")]).is_ok());
    assert!(ok(vec![halt("0.2")]).is_ok(), "a ladder may be a single halt rung");
    assert!(ok(vec![]).is_err(), "empty ladder");
    assert!(ok(vec![shrink("0.1", "0.5")]).is_err(), "the last rung must halt");
    assert!(ok(vec![halt("0.1"), halt("0.2")]).is_err(), "only the last rung may halt");
    assert!(ok(vec![shrink("0.2", "0.5"), halt("0.2")]).is_err(), "strictly ascending");
    assert!(ok(vec![shrink("0.3", "0.5"), shrink("0.2", "0.4"), halt("0.4")]).is_err(), "ascending order");
    assert!(ok(vec![shrink("0.1", "0.5"), shrink("0.15", "0.6"), halt("0.2")]).is_err(), "no scaling back up");
    assert!(ok(vec![shrink("0.1", "1"), halt("0.2")]).is_err(), "scale must be below 1");
    assert!(ok(vec![shrink("0.1", "0"), halt("0.2")]).is_err(), "scale must be above 0");
    assert!(ok(vec![shrink("0.1", "0.5"), halt("1.5")]).is_err(), "rung above 1");
    assert!(RiskPolicy::new(d("0"), vec![halt("0.2")], d("0.5")).is_err());
    assert!(RiskPolicy::new(d("1.5"), vec![halt("0.2")], d("0.5")).is_err());
    assert!(RiskPolicy::new(d("0.03"), vec![halt("0.2")], d("0")).is_err());
}

// ---------------------------------------------------------------------------------------------------------------
// State observations
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_high_water_mark_only_ratchets_up_and_freezes_when_halted() {
    let s = AccountState::new("a");
    assert_eq!(s.hwm(), None);
    let s = s.observe(&snap("10000"), day(1));
    assert_eq!(s.hwm(), Some(d("10000")));
    let s = s.observe(&snap("9000"), day(1));
    assert_eq!(s.hwm(), Some(d("10000")), "a fall never lowers it");
    let s = s.observe(&snap("10500"), day(1));
    assert_eq!(s.hwm(), Some(d("10500")));
    let (halted, _) = s.halt(HaltReason::Manual, "test", t0());
    let after = halted.observe(&snap("20000"), day(1));
    assert_eq!(after.hwm(), Some(d("10500")), "frozen while halted");
    assert_eq!(after.last_equity(), Some(d("20000")), "but the observation is still recorded");
    let (flattening, _) = s.begin_flatten(HaltReason::Manual, "test", t0());
    assert_eq!(flattening.observe(&snap("30000"), day(1)).hwm(), Some(d("10500")), "frozen while flattening");
}

#[test]
fn the_trading_day_rolls_forward_only() {
    let s = AccountState::new("a").observe(&snap("10000"), day(5));
    assert_eq!((s.trading_day(), s.day_start_equity()), (Some(day(5)), Some(d("10000"))));
    let same = s.observe(&snap("9800"), day(5));
    assert_eq!(same.day_start_equity(), Some(d("10000")), "same day keeps the reference");
    let earlier = s.observe(&snap("9800"), day(4));
    assert_eq!((earlier.trading_day(), earlier.day_start_equity()), (Some(day(5)), Some(d("10000"))), "a backwards clock is ignored");
    let later = s.observe(&snap("9800"), day(6));
    assert_eq!((later.trading_day(), later.day_start_equity()), (Some(day(6)), Some(d("9800"))));
}
