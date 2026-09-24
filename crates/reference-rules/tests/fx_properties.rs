//! Property tests for the FX momentum rule with seeded randomness (SplitMix64; fixed seeds, so failures reproduce).
//!
//! Which invariances actually hold (and which do not) is decided from the formula, not assumed:
//! * Multiplying a pair's prices by a positive constant leaves every return and every month-end ratio unchanged:
//!   exactly (bit for bit) for a power of two, to rounding for any other constant. Only the reported `close` moves.
//! * Returns are NOT scale-free in the other direction: negating a pair's daily returns does not negate its
//!   12-month momentum (compounding), and the arithmetic returns of the reciprocal price 1/p are -r/(1+r), not
//!   -r. What is exact: the price INVERSION (p -> 1/p, the USDJPY/JPYUSD flip) flips the momentum sign exactly,
//!   so that pair's weight changes sign, while sigma and the joint scale move only at second order.
//! * After the joint scaling, the sleeve's annualised volatility over the 60-day window is exactly the 10% target
//!   provided nothing was clipped (the cap breaks it by construction).

mod common;
mod fx_common;

use chrono::{Datelike, Duration, NaiveDate};
use common::*;
use fx_common::*;
use reference_rules::*;

const CASES: u64 = 120;

fn sample_std(xs: &[f64]) -> f64 {
    let n = xs.len() as f64;
    let mean = xs.iter().sum::<f64>() / n;
    (xs.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / (n - 1.0)).sqrt()
}

/// Last 60 daily returns of every pair on a panel whose pairs share all dates, up to and including `date`.
fn last_60_returns(panel: &Panel, date: NaiveDate) -> Vec<Vec<f64>> {
    FX_SYMBOLS
        .iter()
        .map(|s| {
            let series = panel.get(s).unwrap();
            let pos = series.position_of(date).unwrap();
            let c = series.closes();
            (pos + 1 - 60..=pos)
                .map(|i| c[i] / c[i - 1] - 1.0)
                .collect()
        })
        .collect()
}

// ------------------------------------------------------------------ basic invariants

#[test]
fn weights_are_bounded_signed_and_consistent_with_signs() {
    let (mut clipped_cases, mut zero_sign, mut shorts, mut longs) = (0, 0, 0, 0);
    for seed in 0..CASES {
        let (panel, date, start) = random_case(10_000 + seed);
        let dec = decide(&panel, date, start).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert_eq!(dec.instruments.len(), 7);
        assert_eq!(dec.month_end_dates.len(), 13);
        assert_eq!(*dec.month_end_dates.last().unwrap(), date);
        assert!(
            dec.ppy.is_finite() && dec.ppy > 200.0 && dec.ppy < 300.0,
            "{}",
            dec.ppy
        );
        assert!(dec.vol_scale.is_finite() && dec.vol_scale > 0.0);
        assert_eq!(dec.dropped_bars, 0);
        let mut any_clipped = false;
        for (i, sym) in dec.instruments.iter().zip(FX_SYMBOLS) {
            assert_eq!(i.symbol, sym);
            assert!(i.sigma.is_finite() && i.sigma > 0.0);
            assert!(i.weight.abs() <= FX_WEIGHT_CAP, "seed {seed}: {}", i.weight);
            assert_eq!(i.weight == 0.0, i.sign == 0, "seed {seed}");
            assert_eq!(i.weight > 0.0, i.sign > 0, "seed {seed}");
            assert_eq!(i.weight < 0.0, i.sign < 0, "seed {seed}");
            if i.clipped {
                any_clipped = true;
                assert_eq!(i.weight.abs(), FX_WEIGHT_CAP);
            } else {
                // Unclipped weight is exactly k * sign / sigma, computed in the reference's operation order.
                assert_eq!(i.weight, dec.vol_scale * (f64::from(i.sign) / i.sigma));
            }
            zero_sign += usize::from(i.sign == 0);
            shorts += usize::from(i.weight < 0.0);
            longs += usize::from(i.weight > 0.0);
        }
        clipped_cases += usize::from(any_clipped);
        assert!(dec.gross_weight() > 0.0);
    }
    println!("weights property: {CASES} cases, {clipped_cases} with a clipped weight, {longs} long / {shorts} short weights, {zero_sign} zero signs");
    assert!(longs > 100 && shorts > 100, "both directions must occur");
}

#[test]
fn sign_field_is_the_direction_of_the_13_month_end_return() {
    for seed in 0..CASES {
        let (panel, date, start) = random_case(11_000 + seed);
        let dec = decide(&panel, date, start).unwrap();
        for i in &dec.instruments {
            let s = panel.get(&i.symbol).unwrap();
            let now = s.closes()[s.position_of(dec.month_end_dates[12]).unwrap()];
            let then = s.closes()[s.position_of(dec.month_end_dates[0]).unwrap()];
            let want = if now > then {
                1
            } else if now < then {
                -1
            } else {
                0
            };
            assert_eq!(i.sign, want, "seed {seed} {}", i.symbol);
            assert_eq!(i.close, s.closes()[s.position_of(date).unwrap()]);
        }
        // month-end dates ascend, are in distinct months, and are bars of the panel
        for w in dec.month_end_dates.windows(2) {
            assert!(w[0] < w[1]);
            assert!((w[0].year(), w[0].month()) < (w[1].year(), w[1].month()));
        }
    }
}

#[test]
fn realised_sleeve_volatility_is_the_target_when_nothing_is_clipped() {
    let (mut checked, mut skipped_clipped) = (0, 0);
    for seed in 0..CASES {
        let (panel, date, start) = random_case(12_000 + seed);
        let dec = decide(&panel, date, start).unwrap();
        if dec.instruments.iter().any(|i| i.clipped) {
            skipped_clipped += 1;
            continue;
        }
        let r = last_60_returns(&panel, date);
        let sleeve: Vec<f64> = (0..60)
            .map(|t| {
                dec.instruments
                    .iter()
                    .enumerate()
                    .map(|(k, i)| r[k][t] * i.weight)
                    .sum::<f64>()
            })
            .collect();
        let realised = sample_std(&sleeve) * dec.ppy.sqrt();
        assert!(
            (realised - FX_SLEEVE_VOL_TARGET).abs() < 1e-10,
            "seed {seed}: realised {realised}"
        );
        checked += 1;
    }
    println!("sleeve vol property: {checked} unclipped cases hit 10% (skipped {skipped_clipped} clipped)");
    assert!(checked >= 60, "only {checked} unclipped cases");
}

// ------------------------------------------------------------------ invariances

#[test]
fn scaling_a_pairs_prices_by_a_power_of_two_changes_nothing_but_the_close() {
    for seed in 0..CASES {
        let (panel, date, start) = random_case(13_000 + seed);
        let base = decide(&panel, date, start).unwrap();
        let factors = [2.0, 0.5, 1024.0, 1.0 / 4096.0, 8.0, 0.125, 65536.0];
        let scaled = map_closes(&panel, |k, _, c| c * factors[k]);
        let got = decide(&scaled, date, start).unwrap();
        assert_eq!(got.ppy, base.ppy);
        assert_eq!(got.vol_scale, base.vol_scale, "seed {seed}");
        for ((a, b), f) in base.instruments.iter().zip(&got.instruments).zip(factors) {
            assert_eq!(
                (a.sign, a.sigma, a.weight, a.clipped),
                (b.sign, b.sigma, b.weight, b.clipped),
                "seed {seed}"
            );
            assert_eq!(b.close, a.close * f);
        }
    }
}

#[test]
fn scaling_prices_by_an_arbitrary_constant_changes_weights_only_at_rounding_level() {
    for seed in 0..CASES {
        let (panel, date, start) = random_case(14_000 + seed);
        let base = decide(&panel, date, start).unwrap();
        let factors = [3.7, 0.013, 101.0, 7.0e-3, 12.5, 0.3, 999.0];
        let scaled = map_closes(&panel, |k, _, c| c * factors[k]);
        let got = decide(&scaled, date, start).unwrap();
        for (a, b) in base.instruments.iter().zip(&got.instruments) {
            assert_eq!(a.sign, b.sign, "seed {seed}");
            assert!(
                (a.weight - b.weight).abs() < 1e-9,
                "seed {seed}: {} vs {}",
                a.weight,
                b.weight
            );
            assert!(rel_diff(a.sigma, b.sigma) < 1e-9);
        }
    }
}

fn rel_diff(a: f64, b: f64) -> f64 {
    (a - b).abs() / a.abs().max(b.abs())
}

#[test]
fn inverting_a_pairs_price_flips_its_sign_and_weight_and_barely_moves_the_rest() {
    let (mut worst_ratio_lo, mut worst_ratio_hi) = (f64::MAX, 0.0f64);
    for seed in 0..CASES {
        let (panel, date, start) = random_case(15_000 + seed);
        let base = decide(&panel, date, start).unwrap();
        let j = (seed % 7) as usize;
        let inverted = map_closes(&panel, |k, _, c| if k == j { 1.0 / c } else { c });
        let got = decide(&inverted, date, start).unwrap();
        for (k, (a, b)) in base.instruments.iter().zip(&got.instruments).enumerate() {
            if k == j {
                assert_eq!(b.sign, -a.sign, "seed {seed}");
                assert_eq!(
                    b.weight.signum() == a.weight.signum(),
                    a.sign == 0,
                    "seed {seed}"
                );
            } else {
                assert_eq!(b.sign, a.sign, "seed {seed}");
                assert_eq!(b.sigma, a.sigma, "other pairs' sigma is untouched");
            }
            if a.weight != 0.0 {
                let ratio = (b.weight / a.weight).abs();
                worst_ratio_lo = worst_ratio_lo.min(ratio);
                worst_ratio_hi = worst_ratio_hi.max(ratio);
                assert!(
                    ratio > 0.98 && ratio < 1.02,
                    "seed {seed}: |w'|/|w| = {ratio}"
                );
            }
        }
    }
    println!("inversion: |w'|/|w| ranged over [{worst_ratio_lo:.4}, {worst_ratio_hi:.4}]");
}

// ------------------------------------------------------------------ causality

#[test]
fn decision_never_depends_on_bars_after_the_decision_date() {
    let mut future_tested = 0;
    for seed in 0..CASES {
        let (panel, date, start) = random_case(16_000 + seed);
        let mut rng = Rng(16_500 + seed);
        let base = decide(&panel, date, start).unwrap();
        // 1. physically truncated at the decision date (what a live run sees)
        assert_eq!(
            decide(&panel.truncated_to(date), date, start).unwrap(),
            base,
            "seed {seed}"
        );
        // 2. every later bar replaced by garbage of the same dates
        let junk = map_closes(&panel, |_, dt, c| {
            if dt > date {
                c * (0.1 + 10.0 * rng.unit())
            } else {
                c
            }
        });
        assert_eq!(decide(&junk, date, start).unwrap(), base, "seed {seed}");
        if panel.get("EURUSD").unwrap().last_date() > date {
            future_tested += 1;
            let nmb = Options::fx_replay(MonthEndMode::NextMonthBar);
            assert_eq!(
                decide_fx_tsmom(&junk, date, start, &nmb).unwrap(),
                base,
                "seed {seed}"
            );
            assert_eq!(
                decide_fx_tsmom(&panel, date, start, &nmb).unwrap(),
                base,
                "seed {seed}"
            );
        }
    }
    assert!(future_tested > CASES / 2);
}

/// Extending a panel with new bars (as time passes) leaves the decision at an earlier month-end unchanged: build
/// one long walk and decide the same date on prefixes of increasing length.
#[test]
fn appending_future_bars_never_changes_an_earlier_decision() {
    let end = d(2021, 12, 31);
    let dates = weekdays(d(2018, 1, 1), end);
    let mut rng = Rng(424242);
    let closes = seven_walks(dates.len(), &mut rng);
    let decision = d(2019, 6, 28);
    let cut = |until: NaiveDate| {
        let n = dates.partition_point(|x| *x <= until);
        panel_same_dates(
            &dates[..n],
            &closes.iter().map(|c| c[..n].to_vec()).collect::<Vec<_>>(),
        )
    };
    let base = decide(&cut(d(2019, 7, 5)), decision, d(2018, 1, 1)).unwrap();
    for until in [d(2019, 7, 31), d(2019, 12, 31), d(2020, 6, 30), end] {
        assert_eq!(
            decide(&cut(until), decision, d(2018, 1, 1)).unwrap(),
            base,
            "until {until}"
        );
    }
}

// ------------------------------------------------------------------ the history window

#[test]
fn bars_before_history_start_are_ignored_and_the_start_itself_matters() {
    let (mut compared, mut ppy_changed) = (0, 0);
    for seed in 0..CASES {
        let (panel, date, _) = random_case(17_000 + seed);
        let dates = panel.get("EURUSD").unwrap().dates().to_vec();
        let start = dates[40];
        let Ok(base) = decide(&panel, date, start) else {
            continue; // fewer than 13 month-ends after `start`
        };
        compared += 1;
        let scrambled = map_closes(
            &panel,
            |_, dt, c| if dt < start { c * 3.7 + 1.0 } else { c },
        );
        assert_eq!(
            decide(&scrambled, date, start).unwrap(),
            base,
            "seed {seed}"
        );
        // the same date decided with a start 17 days earlier is a different window: ppy (hence every weight) moves
        let earlier = decide(&panel, date, start - Duration::days(17)).unwrap();
        assert!(earlier.joint_bars > base.joint_bars);
        if rel_diff(earlier.ppy, base.ppy) > 1e-6 {
            ppy_changed += 1;
            assert_ne!(earlier.instruments, base.instruments);
        }
    }
    println!("history window property: {compared} compared, ppy changed in {ppy_changed}");
    assert!(compared > 60 && ppy_changed * 10 >= compared * 9);
}

// ------------------------------------------------------------------ joint calendar

#[test]
fn a_bar_only_one_pair_has_is_dropped_and_counted() {
    for seed in 0..CASES {
        let (panel, date, start) = random_case(18_000 + seed);
        let base = decide(&panel, date, start).unwrap();
        // Add a Sunday bar to one pair, inside the window and well before the decision date.
        let s = panel.get("USDCHF").unwrap();
        let mut extra = base.first_joint_date + Duration::days(20);
        while extra.weekday().number_from_monday() != 7 {
            extra += Duration::days(1);
        }
        assert!(extra < date - Duration::days(60));
        let mut dates = s.dates().to_vec();
        let mut closes = s.closes().to_vec();
        let at = dates.partition_point(|x| *x < extra);
        dates.insert(at, extra);
        closes.insert(at, 1234.5);
        let with_extra = with_series(&panel, "USDCHF", dates, closes);
        let got = decide(&with_extra, date, start).unwrap();
        assert_eq!(got.dropped_bars, base.dropped_bars + 1, "seed {seed}");
        assert_eq!(
            (&got.instruments, got.ppy, got.vol_scale, got.joint_bars),
            (&base.instruments, base.ppy, base.vol_scale, base.joint_bars),
            "seed {seed}"
        );
    }
}

#[test]
fn decisions_are_deterministic_and_independent_of_series_order() {
    for seed in 0..40 {
        let (panel, date, start) = random_case(19_000 + seed);
        let mut rev: Vec<PriceSeries> = panel.iter().cloned().collect();
        rev.reverse();
        let reversed = Panel::new(rev).unwrap();
        let a = decide(&panel, date, start).unwrap();
        assert_eq!(a, decide(&panel, date, start).unwrap());
        assert_eq!(a, decide(&reversed, date, start).unwrap());
    }
}

/// With one pair made 20 times quieter the cap must bind on it (and only it) in every case; the cap is applied to
/// `k * sign / sigma`, never fed back into `k`, so every other weight is exactly what it would be uncapped.
#[test]
fn a_quiet_pair_is_capped_and_the_cap_does_not_feed_back_into_the_scale() {
    let mut capped = 0;
    for seed in 0..60u64 {
        let (panel, date, start) = random_case(20_000 + seed);
        let s = panel.get("EURUSD").unwrap();
        let mut px = s.closes()[0];
        let quiet: Vec<f64> = s
            .closes()
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if i > 0 {
                    px *= 1.0 + 0.05 * (c / s.closes()[i - 1] - 1.0);
                }
                px
            })
            .collect();
        let panel = with_series(&panel, "EURUSD", s.dates().to_vec(), quiet);
        let dec = decide(&panel, date, start).unwrap();
        let eur = dec.get("EURUSD").unwrap();
        if eur.sign != 0 {
            capped += 1;
            assert!(eur.clipped, "seed {seed}: weight {}", eur.weight);
            assert_eq!(eur.weight, FX_WEIGHT_CAP * f64::from(eur.sign));
            assert!((dec.vol_scale * f64::from(eur.sign) / eur.sigma).abs() > FX_WEIGHT_CAP);
        }
        for i in dec.instruments.iter().filter(|i| !i.clipped) {
            assert_eq!(i.weight, dec.vol_scale * (f64::from(i.sign) / i.sigma));
        }
    }
    assert!(capped >= 50, "{capped}");
}
