//! Boundary cases of the FX momentum rule: exactly 13 month-ends, the 70-bar floor, the 60-return window, the
//! exact 12-month-end lookback, sign ties, one-ulp differences.

mod common;
mod fx_common;

use chrono::NaiveDate;
use common::*;
use fx_common::*;
use reference_rules::*;

fn pos_of(case: &Case, date: NaiveDate) -> usize {
    case.dates.iter().position(|x| *x == date).unwrap()
}

/// Weekday panel Jan 2018 .. Jan 2019 (13 calendar months, 13 month-ends); decision = last weekday of Jan 2019.
fn thirteen_months(seed: u64) -> (Panel, Vec<NaiveDate>, Vec<Vec<f64>>, NaiveDate) {
    let mut rng = Rng(seed);
    let decision = last_weekday(2019, 1);
    let dates = weekdays(d(2018, 1, 1), decision);
    let closes = seven_walks(dates.len(), &mut rng);
    (panel_same_dates(&dates, &closes), dates, closes, decision)
}

// ------------------------------------------------------------------ 13 month-ends

#[test]
fn exactly_13_month_ends_is_enough_and_12_is_not() {
    let (panel, _, _, decision) = thirteen_months(1);
    let dec = decide(&panel, decision, d(2018, 1, 1)).unwrap();
    assert_eq!(dec.month_end_dates.len(), 13);
    assert_eq!(dec.month_end_dates[0], last_weekday(2018, 1));
    assert_eq!(dec.month_end_dates[12], decision);

    // Twelve months of the same walk: the same decision date, one month-end short.
    let short = decide(&panel, decision, d(2018, 2, 1));
    assert_eq!(
        short.unwrap_err(),
        RuleError::InsufficientHistory {
            symbol: FX_JOINT_CALENDAR.to_string(),
            needed: 13,
            have: 12
        }
    );
    // A window that starts on the LAST bar of January counts that lone bar as a month-end: 13 again.
    let lone = decide(&panel, decision, last_weekday(2018, 1)).unwrap();
    assert_eq!(lone.month_end_dates, dec.month_end_dates);
    assert_eq!(
        lone.instruments.iter().map(|i| i.sign).collect::<Vec<_>>(),
        dec.instruments.iter().map(|i| i.sign).collect::<Vec<_>>()
    );
    // ... but its ppy (and so the weights) differ from the whole-year window: the quirk, at the boundary.
    assert_ne!(lone.ppy, dec.ppy);
    assert!(lone.joint_bars < dec.joint_bars);
}

#[test]
fn month_end_lookback_is_exactly_twelve_month_ends() {
    let (_, dates, closes, decision) = thirteen_months(2);
    let me0 = last_weekday(2018, 1);
    let me1 = last_weekday(2018, 2);
    let (i0, i1, id) = (
        dates.iter().position(|x| *x == me0).unwrap(),
        dates.iter().position(|x| *x == me1).unwrap(),
        dates.iter().position(|x| *x == decision).unwrap(),
    );
    let mut closes = closes;
    let (now_eur, now_gbp) = (closes[0][id], closes[1][id]);
    // EURUSD: 13th-last month-end is far BELOW now (sign +1); the 12th-last is far ABOVE (an off-by-one gives -1).
    closes[0][i0] = now_eur * 0.5;
    closes[0][i1] = now_eur * 2.0;
    // GBPUSD: the opposite arrangement.
    closes[1][i0] = now_gbp * 2.0;
    closes[1][i1] = now_gbp * 0.5;
    let panel = panel_same_dates(&dates, &closes);
    let dec = decide(&panel, decision, d(2018, 1, 1)).unwrap();
    assert_eq!(dec.get("EURUSD").unwrap().sign, 1);
    assert_eq!(dec.get("GBPUSD").unwrap().sign, -1);

    // With one more month of history (14 month-ends) the lookback still reaches exactly 12 month-ends back:
    // the oldest month-end (Jan 2018) is no longer used, Feb 2018 is.
    let mut rng = Rng(22);
    let more_dates = weekdays(d(2018, 1, 1), last_weekday(2019, 2));
    let mut more_closes = seven_walks(more_dates.len(), &mut rng);
    let new_decision = last_weekday(2019, 2);
    let (j0, j1, jd) = (
        more_dates.iter().position(|x| *x == me0).unwrap(),
        more_dates.iter().position(|x| *x == me1).unwrap(),
        more_dates.iter().position(|x| *x == new_decision).unwrap(),
    );
    let now = more_closes[0][jd];
    more_closes[0][j0] = now * 2.0; // 14th-last: would give -1 if (wrongly) used
    more_closes[0][j1] = now * 0.5; // 13th-last: the one that must be used -> +1
    let panel14 = panel_same_dates(&more_dates, &more_closes);
    let dec14 = decide(&panel14, new_decision, d(2018, 1, 1)).unwrap();
    assert_eq!(dec14.month_end_dates.len(), 13);
    assert_eq!(dec14.month_end_dates[0], me1);
    assert_eq!(dec14.get("EURUSD").unwrap().sign, 1);
}

// ------------------------------------------------------------------ 70 bars, 60 returns

/// 13 month-ends with exactly `n` bars: four bars (5th, 12th, 19th, 26th) in each of the 12 months of 2018, then
/// `n - 48` consecutive days of January 2019 ending on the decision date. Calendar-free (dates need not be weekdays).
fn sparse_panel(n: usize, seed: u64) -> (Panel, NaiveDate) {
    let mut dates = Vec::new();
    for m in 1..=12u32 {
        for day in [5, 12, 19, 26] {
            dates.push(d(2018, m, day));
        }
    }
    let jan = n - dates.len();
    for day in 1..=jan as u32 {
        dates.push(d(2019, 1, day));
    }
    assert_eq!(dates.len(), n);
    let mut rng = Rng(seed);
    let closes = seven_walks(n, &mut rng);
    (panel_same_dates(&dates, &closes), *dates.last().unwrap())
}

fn unchecked() -> Options {
    let mut o = explicit();
    o.gap_policy = GapPolicy::Unchecked;
    o
}

#[test]
fn seventy_joint_bars_is_the_floor_and_69_is_refused() {
    let (ok, date) = sparse_panel(70, 3);
    let dec = decide_fx_tsmom(&ok, date, d(2018, 1, 1), &unchecked()).unwrap();
    assert_eq!(dec.joint_bars, 70);
    assert_eq!(dec.month_end_dates.len(), 13);

    let (short, date) = sparse_panel(69, 3);
    assert_eq!(
        decide_fx_tsmom(&short, date, d(2018, 1, 1), &unchecked()).unwrap_err(),
        RuleError::InsufficientHistory {
            symbol: FX_JOINT_CALENDAR.to_string(),
            needed: FX_MIN_JOINT_BARS,
            have: 69
        }
    );
    // 60 returns need only 61 bars; the reference's floor of 70 bars (69 returns) is the binding boundary, so a
    // 60-return / 59-return panel never reaches the volatility step: it is refused as above.
    assert!(FX_MIN_JOINT_BARS > FX_VOL_WINDOW + 1);
    for n in [61usize, 62, 65] {
        let (p, date) = sparse_panel(n, 4);
        assert!(matches!(
            decide_fx_tsmom(&p, date, d(2018, 1, 1), &unchecked()),
            Err(RuleError::InsufficientHistory { needed: 70, have, .. }) if have == n
        ));
    }
}

#[test]
fn the_volatility_window_is_exactly_the_last_60_returns() {
    let case = standard_case(5);
    let pos = pos_of(&case, case.decision);
    let base = decide(&case.panel, case.decision, case.history_start).unwrap();
    let used: Vec<NaiveDate> = base.month_end_dates.clone();
    let perturb = |i: usize| {
        assert!(
            !used.contains(&case.dates[i]),
            "test needs a non-month-end bar"
        );
        let mut closes = case.closes.clone();
        for c in closes.iter_mut() {
            c[i] *= 1.05;
        }
        decide(
            &panel_same_dates(&case.dates, &closes),
            case.decision,
            case.history_start,
        )
        .unwrap()
    };
    // Prices pos-60 .. pos define the last 60 returns (return t uses prices t-1 and t, t = pos-59 ..= pos).
    // The price at pos-61 enters only the 61st-last return: outside the window, nothing changes (bit for bit).
    let outside = perturb(pos - 61);
    assert_eq!(outside, base);
    // The price at pos-60 is the base of the oldest return in the window: sigma moves.
    let inside = perturb(pos - 60);
    assert_ne!(inside.instruments[0].sigma, base.instruments[0].sigma);
    assert_ne!(inside.instruments, base.instruments);
    // The price at pos-1 and at pos are in the window too.
    assert_ne!(perturb(pos - 1).instruments, base.instruments);
}

// ------------------------------------------------------------------ signs

#[test]
fn equal_month_end_closes_give_sign_zero_and_weight_zero() {
    let case = standard_case(6);
    let base = decide(&case.panel, case.decision, case.history_start).unwrap();
    let id = pos_of(&case, case.decision);
    let i0 = pos_of(&case, base.month_end_dates[0]);
    let mut closes = case.closes.clone();
    closes[2][i0] = closes[2][id]; // USDJPY: 13th-last month-end close == today's close
    let dec = decide(
        &panel_same_dates(&case.dates, &closes),
        case.decision,
        case.history_start,
    )
    .unwrap();
    let jpy = dec.get("USDJPY").unwrap();
    assert_eq!(jpy.sign, 0);
    assert_eq!(jpy.weight, 0.0);
    assert!(!jpy.clipped);
    // the other pairs keep a non-zero position, and the sleeve is still scaled (the zero pair adds no variance)
    assert!(dec
        .instruments
        .iter()
        .filter(|i| i.sign != 0)
        .all(|i| i.weight != 0.0));
    assert!(dec.vol_scale > 0.0);
}

#[test]
fn one_ulp_difference_between_month_end_closes_is_a_signal() {
    let case = standard_case(7);
    let base = decide(&case.panel, case.decision, case.history_start).unwrap();
    let id = pos_of(&case, case.decision);
    let i0 = pos_of(&case, base.month_end_dates[0]);
    let today = case.closes[3][id];
    for (then, want) in [
        (f64::from_bits(today.to_bits() + 1), -1),
        (f64::from_bits(today.to_bits() - 1), 1),
        (today, 0),
    ] {
        let mut closes = case.closes.clone();
        closes[3][i0] = then;
        let dec = decide(
            &panel_same_dates(&case.dates, &closes),
            case.decision,
            case.history_start,
        )
        .unwrap();
        assert_eq!(
            dec.get("AUDUSD").unwrap().sign,
            want,
            "then = {then:e}, today = {today:e}"
        );
    }
    // Adjacent doubles at other magnitudes (a power of two, just below one, tiny) never collapse to sign 0.
    for x in [1.0f64, 0.999_999_999_999_999_9, 2.0, 1.5, 1e-3, 7.3e5] {
        let up = f64::from_bits(x.to_bits() + 1);
        assert!(up / x - 1.0 > 0.0, "{x}");
        assert!(x / up - 1.0 < 0.0, "{x}");
    }
}

#[test]
fn a_single_zero_sign_pair_does_not_stop_the_sleeve_but_all_zero_does() {
    let case = standard_case(8);
    let base = decide(&case.panel, case.decision, case.history_start).unwrap();
    let id = pos_of(&case, case.decision);
    let i0 = pos_of(&case, base.month_end_dates[0]);
    let mut closes = case.closes.clone();
    for k in 0..7 {
        closes[k][i0] = closes[k][id];
    }
    let err = decide(
        &panel_same_dates(&case.dates, &closes),
        case.decision,
        case.history_start,
    )
    .unwrap_err();
    assert!(
        matches!(err, RuleError::DegenerateSleeveVolatility { .. }),
        "{err:?}"
    );
}

// ------------------------------------------------------------------ the cap

#[test]
fn the_cap_is_applied_after_scaling_and_only_when_exceeded() {
    // A panel where EURUSD is by far the quietest pair: its weight is capped, the others are not, and the scale
    // factor `vol_scale` is unchanged by the capping (the cap is applied to k * raw, not fed back into k).
    let case = standard_case(9);
    let mut closes = case.closes.clone();
    // Damp EURUSD's daily moves to 5% of their size (keeps its month-end direction roughly, cuts sigma 20x).
    let mut px = closes[0][0];
    let mut damped = vec![px];
    for i in 1..closes[0].len() {
        let r = closes[0][i] / closes[0][i - 1] - 1.0;
        px *= 1.0 + 0.05 * r;
        damped.push(px);
    }
    closes[0] = damped;
    let dec = decide(
        &panel_same_dates(&case.dates, &closes),
        case.decision,
        case.history_start,
    )
    .unwrap();
    let eur = dec.get("EURUSD").unwrap();
    if eur.sign != 0 {
        assert!(eur.clipped, "weight {}", eur.weight);
        assert_eq!(eur.weight, FX_WEIGHT_CAP * f64::from(eur.sign));
        let raw_scaled = dec.vol_scale * f64::from(eur.sign) / eur.sigma;
        assert!(raw_scaled.abs() > FX_WEIGHT_CAP);
    }
    for i in dec.instruments.iter().skip(1) {
        assert!(!i.clipped);
    }
}
