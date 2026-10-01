//! Exact decimal helpers on top of `broker_adapters::Dec` (which offers add, multiply and rounding but no
//! subtraction, negation or division), plus the one place where an `f64` ratio from the mandate becomes a `Dec`.
//!
//! Every function is total: overflow is an `Err`, never a wrap or a panic, so a caller can fail closed.

use broker_adapters::Dec;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MathError {
    #[error("decimal arithmetic overflow")]
    Overflow,
    #[error("division by a non-positive number")]
    BadDivisor,
    #[error("ratio {0} cannot be converted: {1}")]
    BadRatio(String, &'static str),
}

pub fn neg(a: Dec) -> Result<Dec, MathError> {
    Dec::new(a.units().checked_neg().ok_or(MathError::Overflow)?, a.scale()).map_err(|_| MathError::Overflow)
}

pub fn add(a: Dec, b: Dec) -> Result<Dec, MathError> {
    a.checked_add(b).ok_or(MathError::Overflow)
}

pub fn sub(a: Dec, b: Dec) -> Result<Dec, MathError> {
    add(a, neg(b)?)
}

pub fn mul(a: Dec, b: Dec) -> Result<Dec, MathError> {
    a.checked_mul(b).ok_or(MathError::Overflow)
}

pub fn abs(a: Dec) -> Result<Dec, MathError> {
    if a.is_negative() {
        neg(a)
    } else {
        Ok(a)
    }
}

pub fn sum<I: IntoIterator<Item = Dec>>(items: I) -> Result<Dec, MathError> {
    items.into_iter().try_fold(Dec::ZERO, add)
}

fn pow10(n: u32) -> Result<i128, MathError> {
    10i128.checked_pow(n).ok_or(MathError::Overflow)
}

/// `floor(n / d)` at `dp` fractional digits (rounding toward negative infinity), exact. `d` must be positive.
pub fn div_floor(n: Dec, d: Dec, dp: u32) -> Result<Dec, MathError> {
    if !d.is_positive() {
        return Err(MathError::BadDivisor);
    }
    // n/d = (nu / 10^ns) / (du / 10^ds); scaled to dp digits: nu * 10^(dp + ds - ns) / du.
    let e = i64::from(dp) + i64::from(d.scale()) - i64::from(n.scale());
    let (num, den) = if e >= 0 {
        let m = pow10(u32::try_from(e).map_err(|_| MathError::Overflow)?)?;
        (n.units().checked_mul(m).ok_or(MathError::Overflow)?, d.units())
    } else {
        let m = pow10(u32::try_from(-e).map_err(|_| MathError::Overflow)?)?;
        (n.units(), d.units().checked_mul(m).ok_or(MathError::Overflow)?)
    };
    Dec::new(num.div_euclid(den), dp).map_err(|_| MathError::Overflow)
}

/// Direction in which a ratio that does not fit 18 decimals is rounded. Callers choose the SAFE direction for the
/// quantity: caps round `Down`, floors (a minimum cash reserve) round `Up`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RatioRound {
    Down,
    Up,
}

/// Convert an `f64` ratio from the mandate to an exact decimal, ONCE.
///
/// Rule: the decimal is the SHORTEST decimal string that round-trips to the same `f64` (Rust's `Display`, which is
/// what the mandate author wrote for any value typed with up to 15 significant digits), NOT the exact binary
/// expansion. `0.1` therefore becomes exactly `0.1`, not `0.1000000000000000055...`; reading the binary expansion
/// would make a limit "slightly above what the user typed" for some values and "slightly below" for others.
/// If the shortest string has more than 18 fractional digits it is cut at 18, in the direction `round` says.
/// Negative, NaN and infinite values are errors (the mandate validator rejects them too; this is the second line).
pub fn ratio_to_dec(v: f64, round: RatioRound) -> Result<Dec, MathError> {
    let bad = |why: &'static str| MathError::BadRatio(format!("{v:?}"), why);
    if !v.is_finite() {
        return Err(bad("not finite"));
    }
    if v < 0.0 {
        return Err(bad("negative"));
    }
    let text = format!("{v}");
    let (int_part, frac_part) = text.split_once('.').unwrap_or((text.as_str(), ""));
    if frac_part.len() <= 18 {
        return Dec::parse(&text).map_err(|_| bad("not a decimal"));
    }
    let (kept, dropped) = frac_part.split_at(18);
    let base = Dec::parse(&format!("{int_part}.{kept}")).map_err(|_| bad("not a decimal"))?;
    if round == RatioRound::Up && dropped.bytes().any(|b| b != b'0') {
        let unit = Dec::new(1, 18).map_err(|_| bad("scale"))?;
        return add(base, unit);
    }
    Ok(base)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Dec {
        Dec::parse(s).unwrap()
    }

    #[test]
    fn sub_neg_abs() {
        assert_eq!(sub(d("1.5"), d("2.25")).unwrap(), d("-0.75"));
        assert_eq!(neg(d("0")).unwrap(), d("0"));
        assert_eq!(abs(d("-3.10")).unwrap(), d("3.1"));
        assert_eq!(sum([d("0.1"), d("0.2"), d("0.3")]).unwrap(), d("0.6"));
    }

    #[test]
    fn division_floors_exactly() {
        assert_eq!(div_floor(d("10"), d("3"), 4).unwrap(), d("3.3333"));
        assert_eq!(div_floor(d("1"), d("8"), 3).unwrap(), d("0.125"));
        assert_eq!(div_floor(d("0.0001"), d("61234.5"), 18).unwrap().to_string(), "0.000000001633066326");
        assert_eq!(div_floor(d("-10"), d("3"), 2).unwrap(), d("-3.34"), "floor is toward negative infinity");
        assert_eq!(div_floor(d("100"), d("0.5"), 0).unwrap(), d("200"));
        assert_eq!(div_floor(d("5"), d("2"), 0).unwrap(), d("2"));
        assert_eq!(div_floor(d("1"), d("0"), 2), Err(MathError::BadDivisor));
        assert_eq!(div_floor(d("1"), d("-2"), 2), Err(MathError::BadDivisor));
    }

    #[test]
    fn division_overflow_is_an_error_not_a_wrap() {
        let huge = Dec::new(i128::MAX / 2, 0).unwrap();
        assert_eq!(div_floor(huge, d("0.000001"), 18), Err(MathError::Overflow));
    }

    // Documented rounding rule for f64 ratios: shortest round-trip decimal, not the binary expansion.
    #[test]
    fn ratios_become_the_decimal_the_author_typed() {
        for (f, s) in [
            (0.1, "0.1"),
            (0.05, "0.05"),
            (0.07, "0.07"),
            (0.3, "0.3"),
            (0.25, "0.25"),
            (1.0, "1"),
            (0.6, "0.6"),
            (0.0, "0"),
            (1e-7, "0.0000001"),
            (2.5, "2.5"),
        ] {
            assert_eq!(ratio_to_dec(f, RatioRound::Down).unwrap(), d(s), "{f}");
            assert_eq!(ratio_to_dec(f, RatioRound::Up).unwrap(), d(s), "{f}");
        }
    }

    #[test]
    fn ratios_beyond_18_decimals_round_in_the_requested_direction() {
        let f = 0.123_456_789_012_345_67_f64; // 17 digits: fits in 18 decimals, so no rounding is needed
        assert_eq!(ratio_to_dec(f, RatioRound::Down).unwrap(), ratio_to_dec(f, RatioRound::Up).unwrap());
        // 1.2345678901234567e-5 prints as 0.000012345678901234567 (21 fractional digits): cut at 18.
        let g = 1.2345678901234567e-5_f64;
        let down = ratio_to_dec(g, RatioRound::Down).unwrap();
        let up = ratio_to_dec(g, RatioRound::Up).unwrap();
        assert_eq!(down, d("0.000012345678901234"));
        assert_eq!(up, d("0.000012345678901235"));
        assert!(down < up);
    }

    #[test]
    fn bad_ratios_are_errors() {
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.1] {
            assert!(matches!(ratio_to_dec(v, RatioRound::Down), Err(MathError::BadRatio(..))), "{v}");
        }
        // Astronomically large values cannot be represented and must not wrap.
        assert!(ratio_to_dec(1e300, RatioRound::Down).is_err());
    }
}
