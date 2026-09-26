//! PARITY TEST 2 (design 5.4 #2): the drawdown ladder and daily-loss limit.
//!
//! `rebalancer-risk::overlay::step` (exact decimals, the live implementation) against `portfolio_construct::Ladder`
//! (f64, the backtester's specification), over 10,000 generated equity paths.
//!
//! What is compared at every observation: the risk scale, the primary code (and the second halt reason when both halts
//! hit), the shrink rung in force and whether the account is halted. A difference is a FAILURE unless the observation is
//! within 1e-9 (relative) of some trigger or release level, where the two are allowed to differ (the documented band:
//! the f64 ladder compares with a 1e-12 relative tolerance, the decimal one floors `at * hwm` at 18 decimals); such a
//! divergence is COUNTED, printed, and ends that path (the states have diverged from then on). The exact boundary cases
//! are enumerated separately, and one case inside the band is pinned as an EXPECTED-DIVERGENCE.

mod common;

use chrono::{Duration, NaiveDate};
use common::replay::at;
use common::*;
use portfolio_construct as pc;
use rebalancer_risk::overlay::{step, EquitySnapshot, RiskAction, RiskCode, RiskPolicy, Rung, RungAction};
use rebalancer_risk::state::AccountState;

/// A ladder described in exact decimals (scale 4 for fractions), built for both implementations from the SAME numbers.
#[derive(Clone, Debug)]
struct Spec {
    daily_loss: i128,
    /// `(at, shrink scale)`; the last entry has no scale and is the halt rung.
    rungs: Vec<(i128, Option<i128>)>,
    /// Recovery fraction, scale 2.
    recovery: i128,
}

impl Spec {
    fn se(&self) -> RiskPolicy {
        let rungs = self
            .rungs
            .iter()
            .map(|(at, sc)| Rung { at: dec(*at, 4), action: sc.map_or(RungAction::HaltFlatten, |s| RungAction::Shrink { scale: dec(s, 4) }) })
            .collect();
        RiskPolicy::new(dec(self.daily_loss, 4), rungs, dec(self.recovery, 2)).expect("valid policy")
    }

    fn f64(&self) -> pc::Ladder {
        let rungs = self
            .rungs
            .iter()
            .map(|(at, sc)| pc::Rung {
                at: f64_of(*at, 4),
                action: sc.map_or(pc::RungAction::HaltFlatten, |s| pc::RungAction::Shrink { scale: f64_of(s, 4) }),
            })
            .collect();
        pc::Ladder::new(f64_of(self.daily_loss, 4), rungs, f64_of(self.recovery, 2)).expect("valid ladder")
    }

    /// Every level (as a fraction of a reference) at which a decision can change: triggers, releases, daily limit.
    fn levels(&self) -> Vec<(f64, LevelKind)> {
        let rec = f64_of(self.recovery, 2);
        let mut v = vec![(f64_of(self.daily_loss, 4), LevelKind::Daily)];
        for (at, sc) in &self.rungs {
            v.push((f64_of(*at, 4), LevelKind::Trigger));
            if sc.is_some() {
                v.push((f64_of(*at, 4) * rec, LevelKind::Release));
            }
        }
        v
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum LevelKind {
    Daily,
    Trigger,
    Release,
}

fn map_code(c: RiskCode) -> pc::LadderCode {
    match c {
        RiskCode::NoAction => pc::LadderCode::NoAction,
        RiskCode::DrawdownShrink => pc::LadderCode::DrawdownShrink,
        RiskCode::ShrinkHeld => pc::LadderCode::ShrinkHeld,
        RiskCode::PartialRecovery => pc::LadderCode::PartialRecovery,
        RiskCode::Recovered => pc::LadderCode::Recovered,
        RiskCode::DrawdownHalt => pc::LadderCode::DrawdownHalt,
        RiskCode::DailyLossHalt => pc::LadderCode::DailyLossHalt,
        RiskCode::AlreadyHalted => pc::LadderCode::AlreadyHalted,
        RiskCode::EquityInvalid => pc::LadderCode::EquityInvalid,
        RiskCode::ArithmeticOverflow => panic!("the decimal ladder overflowed on a generated path"),
    }
}

/// What both implementations answered at one observation, in comparable terms.
#[derive(Clone, Debug, PartialEq)]
struct Answer {
    scale: f64,
    code: pc::LadderCode,
    also_drawdown_halt: bool,
    rung: Option<usize>,
    halted: bool,
}

/// One synchronized pair of state machines.
struct Pair {
    se_policy: RiskPolicy,
    se_state: AccountState,
    f_ladder: pc::Ladder,
    f_state: pc::LadderState,
    day: Option<NaiveDate>,
    day_start: f64,
    when: chrono::DateTime<chrono::Utc>,
}

impl Pair {
    fn new(spec: &Spec) -> Pair {
        Pair {
            se_policy: spec.se(),
            se_state: AccountState::new("a"),
            f_ladder: spec.f64(),
            f_state: pc::LadderState::new(),
            day: None,
            day_start: 0.0,
            when: at("2030-01-01T00:00:00Z"),
        }
    }

    /// One observation of `equity_cents` on `day`; returns `(decimal, f64)` answers.
    fn observe(&mut self, day: NaiveDate, equity_cents: i128) -> (Answer, Answer) {
        let e = dec(equity_cents, 2);
        let ef = f64_of(equity_cents, 2);
        // ---- live (exact decimals)
        let snap = EquitySnapshot::broker_reported("alpaca", "USD", e, self.when);
        let (next, decision, _t) = step(&self.se_state, &snap, day, &self.se_policy);
        self.se_state = next;
        let codes: Vec<RiskCode> = decision.reasons.iter().map(|r| r.code).collect();
        let a = Answer {
            scale: decision.risk_scale.to_f64(),
            code: map_code(codes[0]),
            also_drawdown_halt: codes.len() > 1 && codes.contains(&RiskCode::DrawdownHalt) && codes[0] == RiskCode::DailyLossHalt,
            rung: decision.next_rung,
            halted: decision.action == RiskAction::HaltFlatten || decision.risk_scale.is_zero(),
        };
        // ---- specification (f64); day-start = the first observation of the account-local day, as `observe` does
        if self.day.is_none_or(|d0| day > d0) {
            self.day = Some(day);
            self.day_start = ef;
        }
        let ld = self.f_ladder.step(&mut self.f_state, ef, self.day_start);
        let b = Answer {
            scale: ld.scale,
            code: ld.code,
            also_drawdown_halt: ld.both_halts,
            rung: ld.rung,
            halted: ld.scale == 0.0,
        };
        (a, b)
    }

    /// Is the observation within 1e-9 (relative) of a level at which either implementation's answer can change?
    fn near_boundary(&self, spec: &Spec, equity_cents: i128) -> bool {
        let e = f64_of(equity_cents, 2);
        let hwm = self.f_state.hwm.unwrap_or(e);
        let close = |loss: f64, level: f64, reference: f64| ((loss - level * reference) / reference).abs() < 1e-9;
        for (level, kind) in spec.levels() {
            match kind {
                LevelKind::Daily => {
                    if self.day_start > 0.0 && close(self.day_start - e, level, self.day_start) {
                        return true;
                    }
                }
                LevelKind::Trigger | LevelKind::Release => {
                    if close(hwm - e, level, hwm) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

fn base_day() -> NaiveDate {
    NaiveDate::from_ymd_opt(2030, 1, 1).unwrap()
}

fn gen_spec(r: &mut SplitMix64) -> Spec {
    let n_shrink = [0usize, 1, 1, 2, 2, 2][ri(r, 0, 5) as usize];
    let halt_at = ri(r, 20, 80) * 50; // 0.10 .. 0.40 in steps of 0.005 (scale 4)
    let mut ats: Vec<i128> = Vec::new();
    let mut lo = 100;
    for k in 0..n_shrink {
        let room = halt_at - lo - 50 * ((n_shrink - k) as i64);
        if room < 50 {
            break;
        }
        let a = lo + ri(r, 1, (room / 50).max(1)) * 50;
        ats.push(i128::from(a));
        lo = a + 50;
    }
    let mut scale = 9000i64;
    let mut rungs: Vec<(i128, Option<i128>)> = Vec::new();
    for a in ats {
        scale = (scale - ri(r, 500, 3500)).max(500);
        rungs.push((a, Some(i128::from(scale))));
    }
    rungs.push((i128::from(halt_at), None));
    let daily = i128::from(ri(r, 2, halt_at / 50) * 50).min(i128::from(halt_at));
    let recovery = [25i128, 50, 75, 100][ri(r, 0, 3) as usize];
    Spec { daily_loss: daily, rungs, recovery }
}

/// A random equity path in cents: a per-path drift, a mixture of small moves and shocks, sometimes several
/// observations on the same day.
fn gen_path(r: &mut SplitMix64) -> Vec<(NaiveDate, i128)> {
    let steps = ri(r, 30, 90);
    let drift = ri(r, -25, 25); // bp per step
    let mut e: i128 = i128::from(ri(r, 5_000_000, 50_000_000)); // 50k .. 500k dollars in cents
    let mut day = base_day();
    let mut out = Vec::new();
    for _ in 0..steps {
        let bp = match ri(r, 0, 99) {
            0..=79 => ri(r, -50, 50) + drift,
            80..=91 => ri(r, -300, -60) + drift / 2,
            _ => ri(r, 60, 350),
        };
        e = (e * i128::from(10_000 + bp)) / 10_000;
        if e < 100 {
            e = 100;
        }
        if r.chance(70) {
            day += Duration::days(1);
        }
        out.push((day, e));
    }
    out
}

#[derive(Default, Debug)]
struct Stats {
    paths: usize,
    observations: usize,
    boundary_divergences: usize,
    halts: usize,
    shrink_steps: usize,
    partial_recoveries: usize,
    recovered: usize,
    daily_halts: usize,
    drawdown_halts: usize,
}

#[test]
fn ten_thousand_generated_paths_agree_rung_for_rung_away_from_boundaries() {
    let mut r = SplitMix64::new(0x00C0_FFEE_2026_0925);
    let mut st = Stats::default();
    let mut digest: u64 = 0xcbf2_9ce4_8422_2325;
    for p in 0..10_000 {
        let spec = gen_spec(&mut r);
        let path = gen_path(&mut r);
        let mut pair = Pair::new(&spec);
        st.paths += 1;
        for (i, (day, cents)) in path.iter().enumerate() {
            // the boundary test needs the state BEFORE folding this observation in, but the high-water mark ratchets
            // with it: fold first on a clone of the f64 state is unnecessary because `hwm = max(hwm, e)` is what
            // `near_boundary` recomputes below
            let (a, b) = pair.observe(*day, *cents);
            st.observations += 1;
            match a.code {
                pc::LadderCode::DrawdownShrink | pc::LadderCode::ShrinkHeld => st.shrink_steps += 1,
                pc::LadderCode::PartialRecovery => st.partial_recoveries += 1,
                pc::LadderCode::Recovered => st.recovered += 1,
                pc::LadderCode::DailyLossHalt => st.daily_halts += 1,
                pc::LadderCode::DrawdownHalt => st.drawdown_halts += 1,
                _ => {}
            }
            if a.halted {
                st.halts += 1;
            }
            digest = digest.wrapping_mul(0x100_0000_01b3) ^ (a.scale.to_bits() ^ (a.rung.map_or(99, |r| r as u64) << 3) ^ (a.code as u64) << 8);
            let same = (a.scale - b.scale).abs() < 1e-15 && a.code == b.code && a.also_drawdown_halt == b.also_drawdown_halt && a.rung == b.rung && a.halted == b.halted;
            if !same {
                if pair.near_boundary(&spec, *cents) {
                    st.boundary_divergences += 1;
                    println!("BOUNDARY-DIVERGENCE path {p} step {i}: decimal {a:?} f64 {b:?} equity {cents} cents spec {spec:?}");
                    break;
                }
                panic!("path {p} step {i} (equity {cents} cents): decimal {a:?} != f64 {b:?}\nspec {spec:?}\npath prefix {:?}", &path[..=i]);
            }
        }
    }
    println!("LADDER-PARITY paths={} observations={} halts={} shrink_steps={} partial_recoveries={} recovered={} daily_halts={} drawdown_halts={} boundary_divergences={} digest={digest:016x}", st.paths, st.observations, st.halts, st.shrink_steps, st.partial_recoveries, st.recovered, st.daily_halts, st.drawdown_halts, st.boundary_divergences);
    // non-vacuity: the generator exercises every branch of the ladder
    assert_eq!(st.paths, 10_000);
    assert!(st.shrink_steps > 1_000, "shrink rungs must be exercised ({})", st.shrink_steps);
    assert!(st.partial_recoveries > 10, "hysteresis releases must be exercised ({})", st.partial_recoveries);
    assert!(st.recovered > 50, "full recoveries must be exercised ({})", st.recovered);
    assert!(st.daily_halts > 100 && st.drawdown_halts > 100, "both halt kinds must be exercised ({} / {})", st.daily_halts, st.drawdown_halts);
    // away from boundaries there is NO divergence, and boundaries are rare in cent-valued random paths
    assert!(st.boundary_divergences <= 5, "boundary divergences must stay negligible ({})", st.boundary_divergences);
}

// -------------------------------------------------------------------------------------------------------------
// Enumerated boundary cases: exactly on a level, one cent either side
// -------------------------------------------------------------------------------------------------------------

fn spec_single() -> Spec {
    // shrink at 0.10 x 0.5, halt at 0.20, daily loss 0.03, recovery 0.5: the mandate baseline
    Spec { daily_loss: 300, rungs: vec![(1000, Some(5000)), (2000, None)], recovery: 50 }
}

/// Run `equities` (cents, one per day) through both and return the pair of answers of the LAST observation.
fn last_answers(spec: &Spec, equities: &[(u32, i128)]) -> (Answer, Answer) {
    let mut pair = Pair::new(spec);
    let mut last = None;
    for (d, e) in equities {
        last = Some(pair.observe(base_day() + Duration::days(i64::from(*d)), *e));
    }
    last.expect("at least one observation")
}

#[test]
fn shrink_trigger_boundary_exact_and_one_cent_either_side() {
    let spec = spec_single();
    // hwm 100_000.00; trigger at loss = 0.10 * hwm = 10_000.00, i.e. equity 90_000.00
    let hwm = 10_000_000i128;
    for (equity, expect_shrink) in [(9_000_000i128, true), (9_000_001, false), (8_999_999, true), (9_000_100, false)] {
        let (a, b) = last_answers(&spec, &[(0, hwm), (1, equity)]);
        assert_eq!(a.rung.is_some(), expect_shrink, "decimal at equity {equity}: {a:?}");
        assert_eq!(a, b, "decimal and f64 agree at equity {equity}");
    }
}

#[test]
fn halt_trigger_boundary_exact_and_one_cent_either_side() {
    let spec = spec_single();
    let hwm = 10_000_000i128;
    for (equity, expect_halt) in [(8_000_000i128, true), (8_000_001, false), (7_999_999, true)] {
        let (a, b) = last_answers(&spec, &[(0, hwm), (1, equity)]);
        assert_eq!(a.halted, expect_halt, "decimal at equity {equity}: {a:?}");
        assert_eq!(a, b, "decimal and f64 agree at equity {equity}");
    }
}

#[test]
fn daily_loss_boundary_exact_and_one_cent_either_side() {
    let spec = spec_single();
    // day-start 100_000.00 (the first observation of the day), then 97_000.00 on the same day: loss = 3_000 = 0.03 x
    let day0 = 10_000_000i128;
    for (equity, expect_halt) in [(9_700_000i128, true), (9_700_001, false), (9_699_999, true)] {
        let (a, b) = last_answers(&spec, &[(0, day0), (0, equity)]);
        assert_eq!(a.halted, expect_halt, "decimal at equity {equity}: {a:?}");
        assert_eq!(a.code == pc::LadderCode::DailyLossHalt, expect_halt);
        assert_eq!(a, b, "decimal and f64 agree at equity {equity}");
    }
}

#[test]
fn release_boundary_is_inclusive_and_matches() {
    let spec = spec_single();
    // shrunk at 88_000 (12% below 100_000); release when loss <= 0.10 * 0.5 * hwm = 5_000.00, i.e. equity >= 95_000.00
    for (equity, expect_released) in [(9_500_000i128, true), (9_499_999, false), (9_500_001, true)] {
        let (a, b) = last_answers(&spec, &[(0, 10_000_000), (1, 8_800_000), (2, equity)]);
        assert_eq!(a.rung.is_none(), expect_released, "decimal at equity {equity}: {a:?}");
        assert_eq!(a, b, "decimal and f64 agree at equity {equity}");
    }
}

#[test]
fn every_rung_boundary_of_a_three_rung_ladder_agrees() {
    let spec = Spec { daily_loss: 500, rungs: vec![(500, Some(8000)), (1000, Some(5000)), (1500, Some(2500)), (3000, None)], recovery: 75 };
    let hwm = 20_000_000i128; // 200_000.00
    let mut checked = 0;
    for (at, _) in &spec.rungs {
        let trigger = hwm - hwm * at / 10_000; // exact in cents for these numbers
        for delta in [-1i128, 0, 1] {
            let (a, b) = last_answers(&spec, &[(0, hwm), (1, trigger + delta)]);
            assert_eq!(a, b, "rung at {at}, equity {} cents", trigger + delta);
            checked += 1;
        }
    }
    assert_eq!(checked, 12);
}

// -------------------------------------------------------------------------------------------------------------
// EXPECTED-DIVERGENCE: inside the 1e-12 relative band the two ladders may differ (design 5.2 level 2)
// -------------------------------------------------------------------------------------------------------------

/// Pinned by the ledger as `LADDER_EDGE_BAND`: with a high-water mark of 1e14 dollars a one-cent difference is 1e-16 of
/// the level. The decimal ladder does NOT trigger at one cent above the trigger equity; the f64 ladder (tolerance 1e-12
/// relative, and f64 cannot even hold the cent at that magnitude) DOES. The design bounds the disagreement to exactly
/// this band; if either side moves, this test fails and the ledger must be re-stated.
#[test]
fn ladder_edge_band_case_is_a_pinned_expected_divergence() {
    let spec = spec_single();
    let hwm: i128 = 1_000_000_000_000_000; // 1e14 dollars in cents
    let trigger: i128 = hwm - hwm / 10; // 9e13 dollars
    let (dec_at_trigger_plus_cent, f64_at_trigger_plus_cent) = last_answers(&spec, &[(0, hwm), (1, trigger + 1)]);
    assert!(dec_at_trigger_plus_cent.rung.is_none(), "exact decimals: one cent above the trigger does not shrink");
    assert!(f64_at_trigger_plus_cent.rung.is_some(), "f64 within its 1e-12 relative band shrinks: {f64_at_trigger_plus_cent:?}");
    // one cent below the trigger: both trigger
    let (a, b) = last_answers(&spec, &[(0, hwm), (1, trigger - 1)]);
    assert!(a.rung.is_some() && b.rung.is_some());
}
