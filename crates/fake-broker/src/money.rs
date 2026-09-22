//! Exact decimal helpers on top of the adapter's [`Dec`]. Money never touches `f64`.
//!
//! `Dec` deliberately offers only a few operations; the exchange needs subtraction, negation and
//! a truncating division (average price = cost / volume). Overflow panics: a fake exchange that
//! silently wraps would hide bugs, and the magnitudes involved (i128 units) never reach it.

use broker_adapters::Dec;

/// Parse a decimal literal, panicking with the literal in the message. For tests and builders.
pub fn dec(s: &str) -> Dec {
    Dec::parse(s).unwrap_or_else(|e| panic!("fake-broker: bad decimal literal {s:?}: {e}"))
}

pub fn add(a: Dec, b: Dec) -> Dec {
    a.checked_add(b).expect("fake-broker: decimal overflow in add")
}

pub fn neg(a: Dec) -> Dec {
    Dec::new(-a.units(), a.scale()).expect("scale of an existing Dec is valid")
}

pub fn sub(a: Dec, b: Dec) -> Dec {
    add(a, neg(b))
}

pub fn mul(a: Dec, b: Dec) -> Dec {
    a.checked_mul(b).expect("fake-broker: decimal overflow in mul")
}

fn pow10(n: u32) -> i128 {
    10i128.checked_pow(n).expect("fake-broker: power of ten overflow")
}

/// `a / b` truncated toward negative infinity to `dp` fractional digits. `b` must be non-zero.
pub fn div_floor(a: Dec, b: Dec, dp: u32) -> Dec {
    assert!(!b.is_zero(), "fake-broker: division by zero");
    // (ua / 10^sa) / (ub / 10^sb) = ua * 10^sb / (ub * 10^sa); scale the numerator by 10^dp.
    let mut num = a
        .units()
        .checked_mul(pow10(b.scale() + dp))
        .expect("fake-broker: decimal overflow in div");
    let mut den = b.units().checked_mul(pow10(a.scale())).expect("fake-broker: decimal overflow in div");
    if den < 0 {
        num = -num;
        den = -den;
    }
    Dec::new(num.div_euclid(den), dp).expect("dp within the decimal scale cap")
}

/// Render with exactly `dp` decimals when that is lossless, otherwise in the value's own scale.
pub fn fixed(d: Dec, dp: u32) -> String {
    d.to_fixed(dp).unwrap_or_else(|_| d.to_string())
}

/// Sum of an iterator of decimals.
pub fn sum<I: IntoIterator<Item = Dec>>(items: I) -> Dec {
    items.into_iter().fold(Dec::ZERO, add)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_is_exact() {
        assert_eq!(add(dec("0.1"), dec("0.2")), dec("0.3"));
        assert_eq!(sub(dec("1"), dec("0.9")), dec("0.1"));
        assert_eq!(neg(dec("1.5")), dec("-1.5"));
        assert_eq!(mul(dec("0.0025"), dec("61234.5")), dec("153.08625"));
    }

    #[test]
    fn division_floors_and_handles_signs() {
        assert_eq!(div_floor(dec("153.08625"), dec("0.0025"), 8), dec("61234.5"));
        assert_eq!(div_floor(dec("1"), dec("3"), 4), dec("0.3333"));
        assert_eq!(div_floor(dec("-1"), dec("3"), 4), dec("-0.3334"));
        assert_eq!(div_floor(dec("1"), dec("-3"), 4), dec("-0.3334"));
        assert_eq!(div_floor(dec("10"), dec("4"), 0), dec("2"));
    }

    #[test]
    fn fixed_falls_back_when_lossy() {
        assert_eq!(fixed(dec("1.5"), 3), "1.500");
        assert_eq!(fixed(dec("1.2345"), 2), "1.2345");
    }
}
