//! Exact comparison of a close with the arithmetic mean of a window of closes.
//!
//! The signal is `close > mean(window)`. Evaluating that in binary floating point can turn a real tie into
//! "above" (ten closes of 0.1 sum to 0.9999999999999999, so the naive mean is below 0.1). Here every f64 is
//! decomposed into `mantissa * 2^exponent` and the comparison `close * n  vs  sum` is done in integers on a
//! common binary scale, so a tie is exactly a tie. The reported mean is the exact mean rounded once to f64.

use std::cmp::Ordering;

/// Largest binary-exponent spread accepted between the smallest and largest value of a window.
const MAX_SHIFT: i32 = 64;

/// Decompose a positive finite f64 into (mantissa, exponent) with value = mantissa * 2^exponent.
fn decompose(x: f64) -> Option<(u64, i32)> {
    if !(x.is_finite() && x > 0.0) {
        return None;
    }
    let bits = x.to_bits();
    let exp_bits = ((bits >> 52) & 0x7ff) as i32;
    let frac = bits & ((1u64 << 52) - 1);
    if exp_bits == 0 {
        Some((frac, -1074))
    } else {
        Some((frac | (1u64 << 52), exp_bits - 1075))
    }
}

/// `x * 2^e` without intermediate overflow/underflow for the exponents that occur here.
fn ldexp(x: f64, e: i32) -> f64 {
    let h = e / 2;
    x * 2f64.powi(h) * 2f64.powi(e - h)
}

/// Result of comparing a close with the mean of a window.
pub(crate) struct MeanComparison {
    /// `close` compared with the exact mean of `window`.
    pub ordering: Ordering,
    /// The exact mean rounded once to f64 (informational).
    pub mean: f64,
}

/// Compare `close` with the exact mean of `window`. `None` when a value is not a positive finite number, the
/// window is empty, or the values span too wide a scale for the integer arithmetic (caller refuses).
pub(crate) fn compare_to_mean(close: f64, window: &[f64]) -> Option<MeanComparison> {
    if window.is_empty() || window.len() > 4096 {
        return None;
    }
    let (cm, ce) = decompose(close)?;
    let mut parts = Vec::with_capacity(window.len());
    let mut emin = ce;
    for &x in window {
        let (m, e) = decompose(x)?;
        emin = emin.min(e);
        parts.push((m, e));
    }
    let mut sum: i128 = 0;
    for (m, e) in parts {
        let shift = e - emin;
        if shift > MAX_SHIFT {
            return None;
        }
        sum = sum.checked_add((m as i128) << shift)?;
    }
    let cshift = ce - emin;
    if cshift > MAX_SHIFT {
        return None;
    }
    let n = window.len() as i128;
    let close_n = ((cm as i128) << cshift).checked_mul(n)?;
    let mean = ldexp(sum as f64, emin) / window.len() as f64;
    Some(MeanComparison {
        ordering: close_n.cmp(&sum),
        mean,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tie_is_a_tie_even_where_naive_float_mean_is_not() {
        let w = [0.1f64; 10];
        let naive: f64 = w.iter().sum::<f64>() / 10.0;
        assert!(
            0.1 > naive,
            "the naive mean is below 0.1, so the naive rule would say 'above'"
        );
        let c = compare_to_mean(0.1, &w).unwrap();
        assert_eq!(c.ordering, Ordering::Equal);
    }

    #[test]
    fn strictly_above_and_below() {
        let w = [100.0, 100.0, 100.0, 100.0];
        assert_eq!(
            compare_to_mean(100.000001, &w).unwrap().ordering,
            Ordering::Greater
        );
        assert_eq!(
            compare_to_mean(99.999999, &w).unwrap().ordering,
            Ordering::Less
        );
    }

    #[test]
    fn rejects_non_positive_and_wide_scale() {
        assert!(compare_to_mean(0.0, &[1.0]).is_none());
        assert!(compare_to_mean(1.0, &[f64::NAN]).is_none());
        assert!(compare_to_mean(1.0, &[]).is_none());
        assert!(compare_to_mean(1e300, &[1e-300]).is_none());
    }

    #[test]
    fn mean_is_the_true_mean() {
        let w = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(compare_to_mean(2.5, &w).unwrap().mean, 2.5);
    }
}
