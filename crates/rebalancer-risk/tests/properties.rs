//! Seeded property tests (SplitMix64, no `rand`): halted stays halted for any equity path, the ladder is monotone,
//! the high-water mark is a running maximum, and ladder boundaries are exact for random marks and rungs.

mod common;

use common::*;
use rebalancer_risk::overlay::{apply_decision, evaluate_risk, step, EquitySnapshot, RiskAction, RiskPolicy, Rung, RungAction};
use rebalancer_risk::rng::SplitMix64;
use rebalancer_risk::state::{AccountState, AccountStatus, HaltReason};
use rebalancer_risk::Dec;

const CASES: u64 = 400;

fn dec_from_cents(cents: u64) -> Dec {
    Dec::new(i128::from(cents), 2).unwrap()
}

fn snap_at(equity: Dec) -> EquitySnapshot {
    EquitySnapshot::broker_reported("kraken", "USD", equity, t0())
}

/// A random valid policy: 0-2 shrink rungs then a halt rung, fractions with 2 decimals.
fn random_policy(rng: &mut SplitMix64) -> RiskPolicy {
    let n_shrink = rng.range(0, 2);
    let mut at = 0u64;
    let mut scale = 95u64;
    let mut rungs = Vec::new();
    for _ in 0..n_shrink {
        at += rng.range(2, 12);
        scale = rng.range(20, scale.saturating_sub(5).max(21));
        rungs.push(Rung { at: Dec::new(i128::from(at), 2).unwrap(), action: RungAction::Shrink { scale: Dec::new(i128::from(scale), 2).unwrap() } });
    }
    at += rng.range(2, 15);
    rungs.push(Rung { at: Dec::new(i128::from(at.min(100)), 2).unwrap(), action: RungAction::HaltFlatten });
    let daily = Dec::new(i128::from(rng.range(1, 10)), 2).unwrap();
    let recovery = *rng.pick(&["0.25", "0.5", "0.75", "1"]);
    RiskPolicy::new(daily, rungs, d(recovery)).expect("generated policy is valid")
}

/// A random state reached through the public transitions: some observations, maybe a shrink.
fn random_state(rng: &mut SplitMix64, policy: &RiskPolicy) -> AccountState {
    let mut s = AccountState::new("acct");
    let mut equity = rng.range(500_000, 2_000_000); // cents
    for i in 0..rng.range(1, 8) {
        let change = rng.range(0, 200) as i64 - 110; // -110..+90 (per mille)
        equity = ((equity as i64) + (equity as i64) * change / 1000).max(100) as u64;
        let (next, _, _) = step(&s, &snap_at(dec_from_cents(equity)), day(1 + (i as u32 / 3)), policy);
        s = next;
        if s.status().is_halt() {
            break;
        }
    }
    s
}

// ---------------------------------------------------------------------------------------------------------------
// Halted stays halted
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn halted_stays_halted_for_any_equity_path_without_a_resume_token() {
    let mut halted_seen = 0u32;
    let mut flattening_seen = 0u32;
    for seed in 0..CASES {
        let mut rng = SplitMix64::new(seed ^ 0xA11CE);
        let p = random_policy(&mut rng);
        let mut s = random_state(&mut rng, &p);
        // Drive it into a halt if the random start did not, sometimes through Flattening, sometimes directly.
        if !s.status().is_halt() {
            s = match rng.range(0, 2) {
                0 => s.begin_flatten(HaltReason::DrawdownLadder, "test", t0()).0,
                1 => s.halt(HaltReason::Reconciliation, "test", t0()).0,
                _ => step(&s, &snap_at(Dec::new(1, 2).unwrap()), day(9), &p).0, // a crash to a cent trips a halt rung
            };
        }
        assert!(s.status().is_halt(), "seed {seed}: could not reach a halt");
        // Now hammer it: random equities (incl. huge gains and zero), random transition calls, forged decisions.
        for op in 0..80 {
            let before = s.status();
            let equity = match rng.range(0, 5) {
                0 => Dec::ZERO,
                1 => dec_from_cents(rng.range(1, 100)),
                2 => dec_from_cents(rng.range(1_000_000, 100_000_000)),
                3 => Dec::new(i128::from(rng.range(1, 1_000_000_000)), 0).unwrap(),
                _ => dec_from_cents(rng.range(500_000, 2_000_000)),
            };
            let snapshot = snap_at(equity);
            let next = match rng.range(0, 6) {
                0 => s.observe(&snapshot, day(1 + (op as u32 % 20))),
                1 => step(&s, &snapshot, day(1 + (op as u32 % 20)), &p).0,
                2 => s.complete_flatten(rng.chance(2)).0,
                3 => s.halt(HaltReason::Manual, "again", t0()).0,
                4 => s.begin_flatten(HaltReason::DailyLoss, "again", t0()).0,
                5 => {
                    // A decision that says "full size" or "shrink", applied by hand: must still be refused.
                    let mut forged = evaluate_risk(&AccountState::new("other"), &snapshot, &p);
                    forged.action = if rng.chance(50) { RiskAction::None } else { RiskAction::Shrink { scale: d("0.5") } };
                    forged.next_rung = Some(0);
                    apply_decision(&s, &forged, t0()).0
                }
                _ => evaluate_and_apply_many(&s, &p, &mut rng),
            };
            assert!(next.status().is_halt(), "seed {seed} op {op}: a halted account became {}", next.status().as_str());
            assert!(!(before == AccountStatus::Halted && next.status() == AccountStatus::Flattening), "seed {seed}: Halted went back to Flattening");
            assert_eq!(next.resumes().len(), s.resumes().len(), "seed {seed}: history changed without a resume");
            assert_eq!(next.halt_record().map(|h| h.reason), s.halt_record().map(|h| h.reason), "seed {seed}: the halt reason changed");
            assert_eq!(next.hwm(), s.hwm(), "seed {seed} op {op}: the mark moved while halted");
            s = next;
        }
        halted_seen += u32::from(s.status() == AccountStatus::Halted);
        flattening_seen += u32::from(s.status() == AccountStatus::Flattening);
    }
    assert!(halted_seen > 50 && flattening_seen > 50, "coverage: halted {halted_seen}, flattening {flattening_seen}");
}

fn evaluate_and_apply_many(s: &AccountState, p: &RiskPolicy, rng: &mut SplitMix64) -> AccountState {
    let mut cur = s.clone();
    for i in 0..5 {
        cur = step(&cur, &snap_at(dec_from_cents(rng.range(100, 9_000_000))), day(1 + i), p).0;
    }
    cur
}

#[test]
fn the_only_way_out_of_a_halt_is_resume_and_it_always_lands_active() {
    for seed in 0..CASES {
        let mut rng = SplitMix64::new(seed ^ 0xBEEF);
        let p = random_policy(&mut rng);
        let s = random_state(&mut rng, &p);
        let (halted, _) = s.halt(HaltReason::Manual, "t", t0());
        let halted = halted.complete_flatten(true).0; // a flatten in progress must finish before a resume is possible
        let equity = dec_from_cents(rng.range(100, 5_000_000));
        let (back, _) = halted.resume(human_approval("checked"), &snap_at(equity), day(5)).unwrap();
        assert_eq!(back.status(), AccountStatus::Active, "seed {seed}");
        assert_eq!(back.hwm(), Some(equity));
        assert_eq!(back.day_start_equity(), Some(equity));
        assert_eq!(back.resumes().len(), halted.resumes().len() + 1);
        assert!(AccountState::transition_allowed(&halted, &back));
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Monotone ladder
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn lower_equity_never_yields_a_larger_risk_scale() {
    let mut strict_cases = 0u32;
    for seed in 0..CASES {
        let mut rng = SplitMix64::new(seed ^ 0x40707);
        let p = random_policy(&mut rng);
        let s = random_state(&mut rng, &p);
        if s.status().is_halt() {
            continue;
        }
        // 24 equities from low to high around the state's mark.
        let mark = s.hwm().unwrap_or(d("10000"));
        let base = mark.units().max(1) as u128;
        let mut equities: Vec<Dec> = (0..24)
            .map(|_| {
                let permille = rng.range(1, 1_400) as u128; // 0.1% .. 140% of the mark
                Dec::new((base * permille / 1000).max(1) as i128, mark.scale()).unwrap()
            })
            .collect();
        equities.sort();
        let mut prev: Option<(Dec, Dec)> = None;
        for e in equities {
            let dec = evaluate_risk(&s, &snap_at(e), &p);
            if let Some((pe, ps)) = prev {
                assert!(dec.risk_scale >= ps, "seed {seed}: equity {pe} -> {e} but the scale fell from {ps} to {} ({:?})", dec.risk_scale, dec.codes());
                strict_cases += u32::from(dec.risk_scale > ps);
            }
            prev = Some((e, dec.risk_scale));
        }
    }
    assert!(strict_cases > 200, "the generator must exercise scale changes, saw {strict_cases}");
}

#[test]
fn the_action_kind_follows_the_scale_none_above_shrink_above_halt() {
    for seed in 0..CASES {
        let mut rng = SplitMix64::new(seed ^ 0xFACE);
        let p = random_policy(&mut rng);
        let s = random_state(&mut rng, &p);
        if s.status().is_halt() {
            continue;
        }
        let e = dec_from_cents(rng.range(100, 5_000_000));
        let dec = evaluate_risk(&s, &snap_at(e), &p);
        match &dec.action {
            RiskAction::None => assert_eq!(dec.risk_scale, d("1")),
            RiskAction::Shrink { scale } => {
                assert_eq!(dec.risk_scale, *scale);
                assert!(*scale > Dec::ZERO && *scale < d("1"));
                assert!(dec.halt_reason.is_none() && dec.next_rung.is_some());
            }
            RiskAction::HaltFlatten => {
                assert_eq!(dec.risk_scale, Dec::ZERO);
                assert!(dec.halt_reason.is_some());
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// High-water mark
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_mark_is_the_running_maximum_while_trading_and_frozen_after_a_halt() {
    for seed in 0..CASES {
        let mut rng = SplitMix64::new(seed ^ 0xC0DE);
        let mut s = AccountState::new("acct");
        let mut running_max: Option<Dec> = None;
        let mut halted_mark: Option<Option<Dec>> = None;
        for i in 0..60 {
            let e = dec_from_cents(rng.range(100, 3_000_000));
            s = s.observe(&snap_at(e), day(1 + (i / 10)));
            match halted_mark {
                None => {
                    running_max = Some(running_max.map_or(e, |m| m.max(e)));
                    assert_eq!(s.hwm(), running_max, "seed {seed} step {i}");
                }
                Some(m) => assert_eq!(s.hwm(), m, "seed {seed} step {i}: moved while halted"),
            }
            if halted_mark.is_none() && rng.chance(6) {
                s = s.halt(HaltReason::Manual, "t", t0()).0;
                halted_mark = Some(s.hwm());
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Exact boundaries for random marks and rungs
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_trigger_equity_is_exact_for_random_marks_and_rungs() {
    // mark has 2 decimals and the rung 2 decimals, so mark * (1 - at) has 4 decimals: exactly representable, and
    // one 1e-4 step above it must NOT trigger while the value itself must.
    let unit = Dec::new(1, 4).unwrap();
    let mut halts = 0u32;
    let mut shrinks = 0u32;
    for seed in 0..CASES {
        let mut rng = SplitMix64::new(seed ^ 0xB0B);
        let mark = dec_from_cents(rng.range(10_000, 5_000_000));
        let s10 = rng.range(4, 12);
        let h20 = rng.range(s10 + 3, 40);
        let p = RiskPolicy::new(
            d("0.5"), // wide daily limit: only the ladder speaks
            vec![
                Rung { at: Dec::new(i128::from(s10), 2).unwrap(), action: RungAction::Shrink { scale: d("0.5") } },
                Rung { at: Dec::new(i128::from(h20), 2).unwrap(), action: RungAction::HaltFlatten },
            ],
            d("0.5"),
        )
        .unwrap();
        let state = AccountState::new("acct").observe(&snap_at(mark), day(1));
        for (at_pct, is_halt) in [(s10, false), (h20, true)] {
            let one_minus_at = Dec::new(100 - i128::from(at_pct), 2).unwrap();
            let trigger = mark.checked_mul(one_minus_at).unwrap();
            let above = trigger.checked_add(unit).unwrap();
            let at_dec = evaluate_risk(&state, &snap_at(trigger), &p);
            let above_dec = evaluate_risk(&state, &snap_at(above), &p);
            if is_halt {
                assert_eq!(at_dec.action, RiskAction::HaltFlatten, "seed {seed}: exactly at the halt rung {h20}% of {mark}");
                assert!(matches!(above_dec.action, RiskAction::Shrink { .. }), "seed {seed}: one step above the halt rung must only shrink: {above_dec:?}");
                halts += 1;
            } else {
                assert!(matches!(at_dec.action, RiskAction::Shrink { .. }), "seed {seed}: exactly at the shrink rung {s10}% of {mark}: {at_dec:?}");
                assert_eq!(above_dec.action, RiskAction::None, "seed {seed}: one step above the shrink rung: {above_dec:?}");
                shrinks += 1;
            }
        }
    }
    assert_eq!((halts, shrinks), (CASES as u32, CASES as u32));
}

#[test]
fn the_daily_loss_trigger_is_exact_for_random_day_starts() {
    let unit = Dec::new(1, 4).unwrap();
    for seed in 0..CASES {
        let mut rng = SplitMix64::new(seed ^ 0xDA11);
        let start = dec_from_cents(rng.range(10_000, 5_000_000));
        let pct = rng.range(1, 9);
        let p = RiskPolicy::new(
            Dec::new(i128::from(pct), 2).unwrap(),
            vec![Rung { at: d("0.5"), action: RungAction::HaltFlatten }],
            d("0.5"),
        )
        .unwrap();
        let state = AccountState::new("acct").observe(&snap_at(start), day(1));
        let trigger = start.checked_mul(Dec::new(100 - i128::from(pct), 2).unwrap()).unwrap();
        let at = evaluate_risk(&state, &snap_at(trigger), &p);
        assert_eq!((at.action.clone(), at.halt_reason), (RiskAction::HaltFlatten, Some(HaltReason::DailyLoss)), "seed {seed}: {start} at {pct}%");
        let above = evaluate_risk(&state, &snap_at(trigger.checked_add(unit).unwrap()), &p);
        assert_eq!(above.action, RiskAction::None, "seed {seed}");
    }
}
