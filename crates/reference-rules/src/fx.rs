//! FX time-series momentum sleeve (reference: `decide_s2` in stage1-record/tool/ticket.py).
//!
//! At a month-end decision date `d`, using only bars dated on or before `d`, for each of seven pairs:
//!
//! * `sign` = sign of (month-end close now / month-end close 12 month-ends earlier - 1); needs 13 month-ends;
//!   sign 0 gives weight 0;
//! * `ret` = daily close-to-close change `p[i] / p[i-1] - 1` on the JOINT calendar (dates on which all seven pairs
//!   have a bar), `ppy` = `len(ret) / ((last date - first date).days / 365.25)` over the whole joint window;
//! * `r60` = the last 60 daily returns, `sigma = std(r60, ddof = 1) * sqrt(ppy)` per pair, `raw = sign / sigma`;
//! * sleeve series = `sum over pairs of r60 * raw` (per day); `k = 0.10 / (std(sleeve, ddof = 1) * sqrt(ppy))`
//!   (see [`fx_joint_vol_scale`]); `weight = clip(k * raw, -3, 3)`, the clip applied AFTER scaling.
//!
//! Weights are SIGNED fractions of the sleeve's equity: negative = short, and the sum of absolute weights is
//! several times 1 (leverage; the reference's own config note says about 6x gross notional).
//!
//! # The `ppy` window (a reference quirk, reproduced deliberately)
//!
//! The reference computes `ppy` over EVERY row it is handed up to `d` (after the joint `dropna`), so the result
//! depends on how much history the caller supplied: the backtest saw 11 years, the live ticket fetches
//! `asof - 520 days`. The number changes with the window (on the ladder data at 2015-06-30 it is 310.5
//! with the whole file and 311.9 with the 520-day window) and, because `ppy` enters both `sigma` and the
//! joint scale, every weight moves with it. It also counts weekend-dated bars in the data as observations, so it
//! is not "trading days per year" at all. This port reproduces the formula exactly and refuses to hide the
//! dependence: `history_start` is a REQUIRED argument of [`decide_fx_tsmom`]. Bars dated before it are ignored;
//! the result reports `first_joint_date`, `joint_bars`, `dropped_bars` and `ppy`. To reproduce the live reference
//! ticket pass `asof - FX_REFERENCE_LOOKBACK_DAYS` days (see [`fx_history_start`]); to reproduce the backtest pass
//! the first date of the data set. There is no default because either choice is a decision, not a fact.
//!
//! # Numerics
//!
//! All `f64`. Standard deviations are the two-pass sample form (mean first, then the sum of squared deviations,
//! both summed left to right in date order; the sleeve's per-day sum runs left to right over `FX_SYMBOLS`
//! order). numpy/pandas sum with pairwise blocks and (when `bottleneck` is installed) may use a different std
//! kernel, so the last bits differ. Measured against the reference on the ladder history, the maximum absolute
//! difference over all golden weights is stated in `tests/golden_fx.rs`; two different numpy/pandas versions of the
//! reference itself differ from each other by about 9e-16 on the same input.
//!
//! # Refusals
//!
//! Instead of guessing, this rule refuses with a specific `RuleError`: a pair missing, the decision date not a bar
//! of some pair or not the last bar of its month (`NotMonthEnd` / `MonthNotComplete`), fewer than 13 joint
//! month-ends or fewer than 70 joint bars (`InsufficientHistory`, symbol `FX_JOINT_CALENDAR`), a data hole on the
//! joint calendar over the decision window (`DataGap`, symbol `FX_JOINT_CALENDAR`), a pair with zero or non-finite
//! 60-day volatility (`ZeroVolatility`; "zero" includes a series that varies only by rounding noise, at most 1e-12 of its largest value), a degenerate sleeve (`DegenerateSleeveVolatility`), or any non-finite
//! intermediate (`NonFiniteValue`).

use chrono::NaiveDate;

use crate::checks::{check_as_of, check_month_end, locate};
use crate::decision::{FxInstrumentDecision, FxTsmomDecision};
use crate::error::RuleError;
use crate::months::{check_gaps, month_end_indices};
use crate::options::Options;
use crate::series::{Panel, PriceSeries};

pub const FX_SYMBOLS: [&str; 7] = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD",
];
/// Month-end closes needed: now and 12 month-ends earlier.
pub const FX_MOMENTUM_MONTH_ENDS: usize = 13;
/// Trailing window of daily returns for `sigma` and for the sleeve series.
pub const FX_VOL_WINDOW: usize = 60;
/// The reference refuses when the joint calendar has fewer than this many bars (`len(c) < 70`), which is stricter
/// than the 61 bars that 60 returns need.
pub const FX_MIN_JOINT_BARS: usize = 70;
/// Annualised volatility the whole sleeve is scaled to.
pub const FX_SLEEVE_VOL_TARGET: f64 = 0.10;
/// Absolute cap on each weight, applied after the joint scaling.
pub const FX_WEIGHT_CAP: f64 = 3.0;
/// `config.json` `data.lookback_days` of the reference ticket: the live ticket fetches `asof - 520 days`.
pub const FX_REFERENCE_LOOKBACK_DAYS: i64 = 520;
/// Label used as `symbol` in errors about the joint calendar of the seven pairs (no single pair is at fault).
pub const FX_JOINT_CALENDAR: &str = "FX_JOINT_CALENDAR";

const N: usize = FX_SYMBOLS.len();

/// The history window start of the live reference ticket: `as_of - lookback_days` (its `load_api` asks the
/// vendor for bars from that date on, inclusive). Provided so a caller cannot silently pick a different window
/// by arithmetic slip; passing any other date is legitimate but changes the weights.
pub fn fx_history_start(as_of: NaiveDate, lookback_days: i64) -> NaiveDate {
    as_of - chrono::Duration::days(lookback_days)
}

/// Observations per year exactly as the reference forms it: `returns / (days / 365.25)` with `days` the calendar
/// days from the first to the last bar of the joint window and `returns` the number of daily returns (bars - 1).
/// `None` when `returns == 0` or `last <= first`.
pub fn fx_periods_per_year(returns: usize, first: NaiveDate, last: NaiveDate) -> Option<f64> {
    let days = (last - first).num_days();
    if returns == 0 || days <= 0 {
        return None;
    }
    let years = days as f64 / 365.25;
    Some(returns as f64 / years)
}

/// Sample (ddof = 1) standard deviation, two-pass, sequential summation. `None` for fewer than 2 values. The
/// result may be NaN or infinite when a value is; callers check.
pub(crate) fn sample_std(xs: &[f64]) -> Option<f64> {
    if xs.len() < 2 {
        return None;
    }
    let n = xs.len() as f64;
    let mut sum = 0.0;
    for &x in xs {
        sum += x;
    }
    let mean = sum / n;
    let mut ss = 0.0;
    for &x in xs {
        let dev = x - mean;
        ss += dev * dev;
    }
    Some((ss / (n - 1.0)).sqrt())
}

/// A standard deviation that is only rounding noise around a constant series: at most `1e-12` of the largest
/// magnitude in it. Such a series has no volatility to scale by; dividing by the noise would produce astronomically
/// large weights (the reference does exactly that). Refused, never guessed.
pub(crate) fn is_flat(std: f64, xs: &[f64]) -> bool {
    let max_abs = xs.iter().fold(0.0f64, |m, x| m.max(x.abs()));
    !(std > 1e-12 * max_abs)
}

/// The joint volatility scale of the sleeve: `k = target_vol / (std(sleeve) * sqrt(ppy))` where
/// `sleeve[t] = sum over pairs p of returns[p][t] * raw[p]` (summed in pair order) and `std` is the two-pass
/// sample standard deviation. `returns[p]` is the window of daily returns of pair `p` (all the same length, at
/// least 2); `raw[p]` is the volatility-scaled signed size `sign / sigma` of the pair.
///
/// Pure. Returns `DegenerateSleeveVolatility` for malformed or non-finite input, for a sleeve series with zero or
/// non-finite volatility (for example all `raw` are 0), or for a non-positive `ppy` / `target_vol`. Properties
/// (tested): `k` is unchanged by negating all of `raw`, is divided by `c` when `raw` is multiplied by `c > 0`,
/// and `k * raw` reproduces a sleeve whose annualised volatility is exactly `target_vol`.
pub fn fx_joint_vol_scale(
    returns: &[&[f64]],
    raw: &[f64],
    ppy: f64,
    target_vol: f64,
) -> Result<f64, RuleError> {
    let bad = |reason| Err(RuleError::DegenerateSleeveVolatility { reason });
    if returns.is_empty() || returns.len() != raw.len() {
        return bad("malformed input: pairs and raw sizes differ or are empty");
    }
    let window = returns[0].len();
    if window < 2 || returns.iter().any(|r| r.len() != window) {
        return bad("malformed input: return windows must have equal length of at least 2");
    }
    if !(ppy.is_finite() && ppy > 0.0 && target_vol.is_finite() && target_vol > 0.0) {
        return bad("ppy and target volatility must be finite and positive");
    }
    if raw.iter().any(|x| !x.is_finite())
        || returns.iter().any(|r| r.iter().any(|x| !x.is_finite()))
    {
        return bad("non-finite input");
    }
    let mut sleeve = Vec::with_capacity(window);
    for t in 0..window {
        let mut s = 0.0;
        for p in 0..returns.len() {
            s += returns[p][t] * raw[p];
        }
        sleeve.push(s);
    }
    let std = match sample_std(&sleeve) {
        Some(x) if x.is_finite() => x,
        _ => return bad("sleeve volatility is not finite"),
    };
    let vol = std * ppy.sqrt();
    if is_flat(std, &sleeve) || !(vol.is_finite() && vol > 0.0) {
        return bad("sleeve volatility is zero (no positions, or no variation beyond rounding)");
    }
    Ok(target_vol / vol)
}

/// Joint calendar of the seven series inside `[start, bar at pos]`: dates on which every pair has a bar (the
/// reference's `dropna`), the rows of closes in `FX_SYMBOLS` order, and how many dates were dropped.
fn joint_calendar(
    series: &[&PriceSeries; N],
    pos: &[usize; N],
    start: NaiveDate,
) -> (Vec<NaiveDate>, Vec<[f64; N]>, usize) {
    let mut cur = [0usize; N];
    let mut end = [0usize; N];
    let mut union: Vec<NaiveDate> = Vec::new();
    for k in 0..N {
        let dates = series[k].dates();
        let hi = pos[k] + 1;
        let lo = dates.partition_point(|x| *x < start).min(hi);
        cur[k] = lo;
        end[k] = hi;
        union.extend_from_slice(&dates[lo..hi]);
    }
    union.sort_unstable();
    union.dedup();
    let mut dates_out = Vec::new();
    let mut rows = Vec::new();
    'walk: loop {
        if (0..N).any(|k| cur[k] >= end[k]) {
            break;
        }
        let target = (0..N)
            .map(|k| series[k].dates()[cur[k]])
            .max()
            .expect("N > 0");
        let mut all_equal = true;
        for k in 0..N {
            while cur[k] < end[k] && series[k].dates()[cur[k]] < target {
                cur[k] += 1;
            }
            if cur[k] >= end[k] {
                break 'walk;
            }
            if series[k].dates()[cur[k]] != target {
                all_equal = false;
            }
        }
        if all_equal {
            let mut row = [0.0; N];
            for k in 0..N {
                row[k] = series[k].closes()[cur[k]];
                cur[k] += 1;
            }
            dates_out.push(target);
            rows.push(row);
        }
    }
    let dropped = union.len() - dates_out.len();
    (dates_out, rows, dropped)
}

fn sign_of(x: f64) -> i8 {
    if x > 0.0 {
        1
    } else if x < 0.0 {
        -1
    } else {
        0
    }
}

/// Decide the FX time-series-momentum sleeve at `decision_date` (a month-end; see `Options::month_end_mode`).
///
/// `history_start` is the first date whose bars are used (bars before it are ignored). It is an explicit,
/// required input because `ppy` (and so every weight) depends on it: see the module docs. Only bars dated on or
/// before `decision_date` enter the computation.
///
/// Per-pair checks (as for the ETF rule): every pair present, `as_of` staleness/forming checks, the decision date
/// is a bar of every pair and the last bar of its month in each pair's own series. Then the seven series are
/// inner-joined on date inside the window (the reference's `dropna`; the number of dropped dates is reported).
pub fn decide_fx_tsmom(
    panel: &Panel,
    decision_date: NaiveDate,
    history_start: NaiveDate,
    opts: &Options,
) -> Result<FxTsmomDecision, RuleError> {
    let mut series: Vec<&PriceSeries> = Vec::with_capacity(N);
    let mut pos = [0usize; N];
    for (k, symbol) in FX_SYMBOLS.iter().enumerate() {
        let s = panel.get(symbol)?;
        check_as_of(s, opts)?;
        pos[k] = locate(s, decision_date)?;
        check_month_end(s, pos[k], opts)?;
        series.push(s);
    }
    let series: [&PriceSeries; N] = series.try_into().expect("seven series");

    let (dates, rows, dropped_bars) = joint_calendar(&series, &pos, history_start);
    let n = dates.len();
    let me = month_end_indices(&dates);
    if me.len() < FX_MOMENTUM_MONTH_ENDS {
        return Err(RuleError::InsufficientHistory {
            symbol: FX_JOINT_CALENDAR.to_string(),
            needed: FX_MOMENTUM_MONTH_ENDS,
            have: me.len(),
        });
    }
    if n < FX_MIN_JOINT_BARS {
        return Err(RuleError::InsufficientHistory {
            symbol: FX_JOINT_CALENDAR.to_string(),
            needed: FX_MIN_JOINT_BARS,
            have: n,
        });
    }
    let me = &me[me.len() - FX_MOMENTUM_MONTH_ENDS..];
    // Everything that determines sign and sigma: from the oldest month-end used to the decision date, and the
    // 60 returns (61 bars). The older history only enters through `ppy`.
    let gap_from = me[0].min(n - FX_VOL_WINDOW - 1);
    check_gaps(FX_JOINT_CALENDAR, &dates[gap_from..], opts.gap_policy)?;

    let ppy = fx_periods_per_year(n - 1, dates[0], dates[n - 1]).ok_or_else(|| {
        RuleError::NonFiniteValue {
            symbol: FX_JOINT_CALENDAR.to_string(),
            what: "observations per year",
        }
    })?;
    if !(ppy.is_finite() && ppy > 0.0) {
        return Err(RuleError::NonFiniteValue {
            symbol: FX_JOINT_CALENDAR.to_string(),
            what: "observations per year",
        });
    }
    let sqrt_ppy = ppy.sqrt();

    // r60[k][t]: the last 60 daily returns of pair k.
    let mut r60: Vec<Vec<f64>> = vec![Vec::with_capacity(FX_VOL_WINDOW); N];
    for i in (n - FX_VOL_WINDOW)..n {
        for k in 0..N {
            let r = rows[i][k] / rows[i - 1][k] - 1.0;
            if !r.is_finite() {
                return Err(RuleError::NonFiniteValue {
                    symbol: FX_SYMBOLS[k].to_string(),
                    what: "daily return",
                });
            }
            r60[k].push(r);
        }
    }

    let mut sign = [0i8; N];
    let mut sigma = [0.0f64; N];
    let mut raw = [0.0f64; N];
    for k in 0..N {
        let now = rows[me[FX_MOMENTUM_MONTH_ENDS - 1]][k];
        let then = rows[me[0]][k];
        // Same operations as the reference (`m.iloc[-1] / m.iloc[-13] - 1`, then np.sign); for positive finite
        // prices this equals comparing `now` with `then` (crate docs, choice 12).
        sign[k] = sign_of(now / then - 1.0);
        let std = sample_std(&r60[k]).expect("60 returns");
        sigma[k] = std * sqrt_ppy;
        if is_flat(std, &r60[k]) || !(sigma[k].is_finite() && sigma[k] > 0.0) {
            return Err(RuleError::ZeroVolatility {
                symbol: FX_SYMBOLS[k].to_string(),
            });
        }
        raw[k] = f64::from(sign[k]) / sigma[k];
    }
    let cols: Vec<&[f64]> = r60.iter().map(Vec::as_slice).collect();
    let k_scale = fx_joint_vol_scale(&cols, &raw, ppy, FX_SLEEVE_VOL_TARGET)?;

    let mut instruments = Vec::with_capacity(N);
    for k in 0..N {
        let scaled = k_scale * raw[k];
        if !scaled.is_finite() {
            return Err(RuleError::NonFiniteValue {
                symbol: FX_SYMBOLS[k].to_string(),
                what: "scaled weight",
            });
        }
        instruments.push(FxInstrumentDecision {
            symbol: FX_SYMBOLS[k].to_string(),
            close: rows[n - 1][k],
            sign: sign[k],
            sigma: sigma[k],
            weight: scaled.clamp(-FX_WEIGHT_CAP, FX_WEIGHT_CAP),
            clipped: scaled.abs() > FX_WEIGHT_CAP,
        });
    }
    Ok(FxTsmomDecision {
        decision_date,
        history_start,
        first_joint_date: dates[0],
        joint_bars: n,
        dropped_bars,
        ppy,
        vol_scale: k_scale,
        month_end_dates: me.iter().map(|&i| dates[i]).collect(),
        instruments,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn close(a: f64, b: f64, rel: f64) -> bool {
        (a - b).abs() <= rel * a.abs().max(b.abs())
    }

    // -------------------------------------------------------------- sample_std

    #[test]
    fn sample_std_matches_a_hand_computation() {
        // mean 5, deviations -3 -1 1 3 -> ss 20 -> var 20/3
        let got = sample_std(&[2.0, 4.0, 6.0, 8.0]).unwrap();
        assert!(close(got, (20.0f64 / 3.0).sqrt(), 1e-15), "{got}");
        assert_eq!(sample_std(&[1.0, 1.0, 1.0]).unwrap(), 0.0);
        assert!(sample_std(&[1.0]).is_none());
        assert!(sample_std(&[]).is_none());
        assert!(sample_std(&[1.0, f64::NAN]).unwrap().is_nan());
    }

    #[test]
    fn sample_std_two_pass_survives_a_large_offset() {
        // Values 1e9 + {0,1,2,3,4}: the one-pass sum-of-squares formula loses everything at this offset.
        let xs: Vec<f64> = (0..5).map(|i| 1e9 + f64::from(i)).collect();
        let got = sample_std(&xs).unwrap();
        assert!(close(got, 2.5f64.sqrt(), 1e-12), "{got}");
    }

    // -------------------------------------------------------------- fx_periods_per_year

    #[test]
    fn periods_per_year_follows_the_reference_formula() {
        // 365.25 calendar days, 260 returns -> exactly 260 per year
        let first = d(2020, 1, 1);
        assert!(fx_periods_per_year(10, first, first).is_none());
        let last = d(2021, 1, 1); // 366 days
        let got = fx_periods_per_year(260, first, last).unwrap();
        assert_eq!(got, 260.0 / (366.0 / 365.25));
        assert!(fx_periods_per_year(0, first, last).is_none());
        assert!(fx_periods_per_year(5, last, first).is_none());
    }

    // -------------------------------------------------------------- fx_history_start

    #[test]
    fn history_start_is_asof_minus_lookback() {
        assert_eq!(
            fx_history_start(d(2020, 9, 30), FX_REFERENCE_LOOKBACK_DAYS),
            d(2019, 4, 29)
        );
        assert_eq!(fx_history_start(d(2020, 3, 1), 0), d(2020, 3, 1));
    }

    // -------------------------------------------------------------- fx_joint_vol_scale

    /// Deterministic pseudo-random windows (LCG; test-local so the unit tests need no dev-dependency).
    fn windows(pairs: usize, len: usize, seed: u64) -> Vec<Vec<f64>> {
        let mut s = seed;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 11) as f64 / (1u64 << 53) as f64) - 0.5
        };
        (0..pairs)
            .map(|p| (0..len).map(|_| next() * 0.01 * (1.0 + p as f64)).collect())
            .collect()
    }

    fn as_slices(v: &[Vec<f64>]) -> Vec<&[f64]> {
        v.iter().map(Vec::as_slice).collect()
    }

    #[test]
    fn joint_scale_hand_example() {
        // Two pairs with identical alternating returns +-1%; raw = [1, 1] -> sleeve = +-2%.
        // sleeve std (ddof 1, n = 4) = 0.02 * sqrt(4/3); ppy = 1 -> k = 0.10 / that.
        let a = [0.01, -0.01, 0.01, -0.01];
        let k = fx_joint_vol_scale(&[&a, &a], &[1.0, 1.0], 1.0, 0.10).unwrap();
        let expect = 0.10 / (0.02 * (4.0f64 / 3.0).sqrt());
        assert!(close(k, expect, 1e-14), "{k} vs {expect}");
        // ppy enters as sqrt: quadrupling it halves k.
        let k4 = fx_joint_vol_scale(&[&a, &a], &[1.0, 1.0], 4.0, 0.10).unwrap();
        assert!(close(k4, k / 2.0, 1e-14));
    }

    #[test]
    fn joint_scale_hits_the_target_volatility() {
        for seed in 0..25u64 {
            let w = windows(7, 60, 100 + seed);
            let raw: Vec<f64> = (0..7)
                .map(|p| if (p + seed as usize) % 3 == 0 { -1.0 } else { 1.0 } * (3.0 + p as f64))
                .collect();
            let ppy = 250.0 + seed as f64;
            let k = fx_joint_vol_scale(&as_slices(&w), &raw, ppy, 0.10).unwrap();
            let sleeve: Vec<f64> = (0..60)
                .map(|t| (0..7).map(|p| w[p][t] * raw[p] * k).sum::<f64>())
                .collect();
            let realised = sample_std(&sleeve).unwrap() * ppy.sqrt();
            assert!(close(realised, 0.10, 1e-12), "seed {seed}: {realised}");
        }
    }

    #[test]
    fn joint_scale_invariances() {
        let w = windows(7, 60, 7);
        let raw = [3.0, -2.0, 5.0, 1.5, -4.0, 2.5, 6.0];
        let base = fx_joint_vol_scale(&as_slices(&w), &raw, 260.0, 0.10).unwrap();
        // Negating every raw size flips every sleeve value's sign: the std, hence k, is exactly unchanged.
        let neg: Vec<f64> = raw.iter().map(|x| -x).collect();
        assert_eq!(
            fx_joint_vol_scale(&as_slices(&w), &neg, 260.0, 0.10).unwrap(),
            base
        );
        // Multiplying raw by a power of two divides k by it exactly (no rounding).
        let dbl: Vec<f64> = raw.iter().map(|x| x * 2.0).collect();
        assert_eq!(
            fx_joint_vol_scale(&as_slices(&w), &dbl, 260.0, 0.10).unwrap(),
            base / 2.0
        );
        // Multiplying raw by 3 divides k by 3 to rounding.
        let tri: Vec<f64> = raw.iter().map(|x| x * 3.0).collect();
        let k3 = fx_joint_vol_scale(&as_slices(&w), &tri, 260.0, 0.10).unwrap();
        assert!(close(k3, base / 3.0, 1e-14));
        // Reordering the pairs changes only the summation order: equal to rounding.
        let mut order: Vec<usize> = (0..7).collect();
        order.reverse();
        let w2: Vec<Vec<f64>> = order.iter().map(|&i| w[i].clone()).collect();
        let raw2: Vec<f64> = order.iter().map(|&i| raw[i]).collect();
        let k2 = fx_joint_vol_scale(&as_slices(&w2), &raw2, 260.0, 0.10).unwrap();
        assert!(close(k2, base, 1e-14), "{k2} vs {base}");
        // A pair with raw 0 contributes nothing: dropping it changes nothing.
        let mut raw0 = raw;
        raw0[3] = 0.0;
        let with_zero = fx_joint_vol_scale(&as_slices(&w), &raw0, 260.0, 0.10).unwrap();
        let idx = [0usize, 1, 2, 4, 5, 6];
        let wz: Vec<Vec<f64>> = idx.iter().map(|&i| w[i].clone()).collect();
        let rz: Vec<f64> = idx.iter().map(|&i| raw0[i]).collect();
        assert_eq!(
            fx_joint_vol_scale(&as_slices(&wz), &rz, 260.0, 0.10).unwrap(),
            with_zero
        );
    }

    #[test]
    fn joint_scale_refuses_degenerate_and_malformed_input() {
        let w = windows(3, 60, 3);
        let s = as_slices(&w);
        let is_degenerate = |r: Result<f64, RuleError>| {
            matches!(r, Err(RuleError::DegenerateSleeveVolatility { .. }))
        };
        // all raw zero -> sleeve is identically 0
        assert!(is_degenerate(fx_joint_vol_scale(
            &s, &[0.0; 3], 260.0, 0.10
        )));
        // constant sleeve (each pair's return is constant) -> zero std
        let flat = vec![vec![0.001; 60]; 3];
        assert!(is_degenerate(fx_joint_vol_scale(
            &as_slices(&flat),
            &[1.0, 2.0, 3.0],
            260.0,
            0.10
        )));
        // non-finite raw / return / ppy / target
        assert!(is_degenerate(fx_joint_vol_scale(
            &s,
            &[1.0, f64::NAN, 1.0],
            260.0,
            0.10
        )));
        assert!(is_degenerate(fx_joint_vol_scale(
            &s,
            &[1.0, f64::INFINITY, 1.0],
            260.0,
            0.10
        )));
        let mut bad = w.clone();
        bad[1][10] = f64::NAN;
        assert!(is_degenerate(fx_joint_vol_scale(
            &as_slices(&bad),
            &[1.0; 3],
            260.0,
            0.10
        )));
        for ppy in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(
                is_degenerate(fx_joint_vol_scale(&s, &[1.0; 3], ppy, 0.10)),
                "{ppy}"
            );
        }
        for target in [0.0, -0.1, f64::NAN] {
            assert!(
                is_degenerate(fx_joint_vol_scale(&s, &[1.0; 3], 260.0, target)),
                "{target}"
            );
        }
        // shape problems
        assert!(is_degenerate(fx_joint_vol_scale(
            &s, &[1.0; 2], 260.0, 0.10
        )));
        assert!(is_degenerate(fx_joint_vol_scale(&[], &[], 260.0, 0.10)));
        let one = [0.01];
        assert!(is_degenerate(fx_joint_vol_scale(
            &[&one],
            &[1.0],
            260.0,
            0.10
        )));
        let ragged: Vec<&[f64]> = vec![&w[0][..], &w[1][..59]];
        assert!(is_degenerate(fx_joint_vol_scale(
            &ragged,
            &[1.0, 1.0],
            260.0,
            0.10
        )));
    }

    #[test]
    fn sign_helper() {
        assert_eq!(sign_of(0.3), 1);
        assert_eq!(sign_of(-0.3), -1);
        assert_eq!(sign_of(0.0), 0);
        assert_eq!(sign_of(-0.0), 0);
    }
}
