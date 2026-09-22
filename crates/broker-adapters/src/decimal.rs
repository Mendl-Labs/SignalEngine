//! A tiny exact decimal type.
//!
//! Money-moving arithmetic (rounding a volume to a pair's lot decimals, comparing against a
//! minimum order size) must not go through `f64`: `0.1 + 0.2 != 0.3` style errors turn into
//! rejected or oversized orders. `Dec` is `units / 10^scale` with `scale <= 18`, `units: i128`.
//!
//! Only the operations the adapters need are provided. All comparisons are exact.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt;

/// Maximum number of fractional digits a `Dec` carries.
pub const MAX_SCALE: u32 = 18;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecError {
    #[error("invalid decimal literal: {0:?}")]
    Invalid(String),
    #[error("decimal has more than {MAX_SCALE} fractional digits: {0:?}")]
    TooPrecise(String),
    #[error("decimal arithmetic overflow")]
    Overflow,
    #[error("scale {0} exceeds the maximum of {MAX_SCALE}")]
    ScaleTooLarge(u32),
    #[error("rounding step must be positive")]
    BadStep,
}

/// Rounding direction for [`Dec::round_dp`] and the multiple-rounding helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rounding {
    /// Toward negative infinity (for positive numbers: truncate).
    Floor,
    /// Toward positive infinity.
    Ceil,
    /// Nearest; ties go up.
    HalfUp,
}

#[derive(Clone, Copy)]
pub struct Dec {
    units: i128,
    scale: u32,
}

fn pow10(n: u32) -> i128 {
    // n <= 38 is guaranteed by callers (scale <= 18, differences <= 18).
    10i128.pow(n)
}

impl Dec {
    pub const ZERO: Dec = Dec { units: 0, scale: 0 };

    pub fn new(units: i128, scale: u32) -> Result<Dec, DecError> {
        if scale > MAX_SCALE {
            return Err(DecError::ScaleTooLarge(scale));
        }
        Ok(Dec { units, scale })
    }

    /// 10^-dp, e.g. `Dec::unit(2)` is 0.01.
    pub fn unit(dp: u32) -> Result<Dec, DecError> {
        Dec::new(1, dp)
    }

    pub fn from_i64(v: i64) -> Dec {
        Dec { units: i128::from(v), scale: 0 }
    }

    /// Parse `"123"`, `"-0.5"`, `"1.25000000"`, and scientific forms like `"1e-7"`.
    pub fn parse(s: &str) -> Result<Dec, DecError> {
        let bad = || DecError::Invalid(s.to_string());
        let t = s.trim();
        if t.is_empty() {
            return Err(bad());
        }
        let (mant, exp) = match t.find(['e', 'E']) {
            Some(i) => {
                let e: i32 = t[i + 1..].parse().map_err(|_| bad())?;
                (&t[..i], e)
            }
            None => (t, 0),
        };
        let (neg, mant) = match mant.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, mant.strip_prefix('+').unwrap_or(mant)),
        };
        let (int_part, frac_part) = match mant.split_once('.') {
            Some((i, f)) => (i, f),
            None => (mant, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return Err(bad());
        }
        if !int_part.chars().all(|c| c.is_ascii_digit()) || !frac_part.chars().all(|c| c.is_ascii_digit()) {
            return Err(bad());
        }
        let digits = format!("{int_part}{frac_part}");
        let mut units: i128 = 0;
        for c in digits.chars() {
            let d = i128::from(c.to_digit(10).ok_or_else(bad)?);
            units = units.checked_mul(10).and_then(|u| u.checked_add(d)).ok_or(DecError::Overflow)?;
        }
        // value = units * 10^(exp - frac_len)
        let net_scale = i64::from(frac_part.len() as u32) - i64::from(exp);
        let (units, scale) = if net_scale < 0 {
            let up = u32::try_from(-net_scale).map_err(|_| DecError::Overflow)?;
            if up > 38 {
                return Err(DecError::Overflow);
            }
            (units.checked_mul(pow10(up)).ok_or(DecError::Overflow)?, 0u32)
        } else {
            (units, u32::try_from(net_scale).map_err(|_| DecError::Overflow)?)
        };
        let d = Dec { units: if neg { -units } else { units }, scale };
        if scale > MAX_SCALE {
            // Allow trailing zeros beyond the cap (e.g. "1.0000000000000000000"); reject real precision.
            let n = d.normalized();
            if n.scale > MAX_SCALE {
                return Err(DecError::TooPrecise(s.to_string()));
            }
            return Ok(n);
        }
        Ok(d)
    }

    pub fn units(&self) -> i128 {
        self.units
    }
    pub fn scale(&self) -> u32 {
        self.scale
    }
    pub fn is_zero(&self) -> bool {
        self.units == 0
    }
    pub fn is_negative(&self) -> bool {
        self.units < 0
    }
    pub fn is_positive(&self) -> bool {
        self.units > 0
    }

    /// Strip trailing fractional zeros (`1.2500` -> `1.25`).
    pub fn normalized(self) -> Dec {
        let mut d = self;
        while d.scale > 0 && d.units % 10 == 0 {
            d.units /= 10;
            d.scale -= 1;
        }
        if d.units == 0 {
            d.scale = 0;
        }
        d
    }

    /// Number of significant fractional digits (after stripping trailing zeros).
    pub fn decimals(&self) -> u32 {
        self.normalized().scale
    }

    fn int_frac(&self) -> (i128, i128) {
        let p = pow10(self.scale);
        (self.units.div_euclid(p), self.units.rem_euclid(p))
    }

    /// Round to `dp` fractional digits in the given direction.
    pub fn round_dp(self, dp: u32, mode: Rounding) -> Result<Dec, DecError> {
        if dp > MAX_SCALE {
            return Err(DecError::ScaleTooLarge(dp));
        }
        if self.scale <= dp {
            return Ok(Dec { units: self.units * pow10(dp - self.scale), scale: dp });
        }
        let div = pow10(self.scale - dp);
        let q = self.units.div_euclid(div);
        let r = self.units.rem_euclid(div);
        let units = match mode {
            Rounding::Floor => q,
            Rounding::Ceil => q + i128::from(r != 0),
            Rounding::HalfUp => q + i128::from(r * 2 >= div),
        };
        Ok(Dec { units, scale: dp })
    }

    /// Round to a multiple of `step` (which must be positive) in the given direction.
    pub fn round_to_multiple(self, step: Dec, mode: Rounding) -> Result<Dec, DecError> {
        if !step.is_positive() {
            return Err(DecError::BadStep);
        }
        let s = self.scale.max(step.scale);
        let a = self.units.checked_mul(pow10(s - self.scale)).ok_or(DecError::Overflow)?;
        let b = step.units.checked_mul(pow10(s - step.scale)).ok_or(DecError::Overflow)?;
        let q = a.div_euclid(b);
        let r = a.rem_euclid(b);
        let n = match mode {
            Rounding::Floor => q,
            Rounding::Ceil => q + i128::from(r != 0),
            Rounding::HalfUp => q + i128::from(r * 2 >= b),
        };
        let units = n.checked_mul(b).ok_or(DecError::Overflow)?;
        Ok(Dec { units, scale: s })
    }

    pub fn checked_mul(self, other: Dec) -> Option<Dec> {
        let units = self.units.checked_mul(other.units)?;
        let scale = self.scale + other.scale;
        if scale <= MAX_SCALE {
            return Some(Dec { units, scale });
        }
        Dec { units, scale: MAX_SCALE }.into_scale_floor(scale)
    }

    // Helper for checked_mul when the natural scale exceeds the cap: floor down to MAX_SCALE.
    fn into_scale_floor(self, real_scale: u32) -> Option<Dec> {
        let drop = real_scale - MAX_SCALE;
        if drop > 38 {
            return None;
        }
        Some(Dec { units: self.units.div_euclid(pow10(drop)), scale: MAX_SCALE })
    }

    pub fn checked_add(self, other: Dec) -> Option<Dec> {
        let s = self.scale.max(other.scale);
        let a = self.units.checked_mul(pow10(s - self.scale))?;
        let b = other.units.checked_mul(pow10(s - other.scale))?;
        Some(Dec { units: a.checked_add(b)?, scale: s })
    }

    /// Render with exactly `dp` fractional digits. Errors if that would lose non-zero digits.
    pub fn to_fixed(&self, dp: u32) -> Result<String, DecError> {
        let r = self.round_dp(dp, Rounding::Floor)?;
        if r != *self {
            return Err(DecError::TooPrecise(self.to_string()));
        }
        Ok(r.to_string())
    }

    /// Lossy conversion, for display and coarse ratios only. Never use for order sizing.
    pub fn to_f64(&self) -> f64 {
        self.units as f64 / 10f64.powi(self.scale as i32)
    }
}

impl PartialEq for Dec {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Dec {}

impl PartialOrd for Dec {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Dec {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare integer and fractional parts separately so no rescaling can overflow.
        let (ai, af) = self.int_frac();
        let (bi, bf) = other.int_frac();
        ai.cmp(&bi).then_with(|| {
            let s = self.scale.max(other.scale);
            let af = af * pow10(s - self.scale);
            let bf = bf * pow10(s - other.scale);
            af.cmp(&bf)
        })
    }
}

impl fmt::Display for Dec {
    /// Plain decimal notation keeping the value's own scale (`1.25000000` stays as is).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let neg = self.units < 0;
        let abs = self.units.unsigned_abs();
        let p = pow10(self.scale).unsigned_abs();
        let (i, fr) = (abs / p, abs % p);
        let sign = if neg { "-" } else { "" };
        if self.scale == 0 {
            write!(f, "{sign}{i}")
        } else {
            write!(f, "{sign}{i}.{fr:0width$}", width = self.scale as usize)
        }
    }
}

impl fmt::Debug for Dec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Dec({self})")
    }
}

impl std::str::FromStr for Dec {
    type Err = DecError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Dec::parse(s)
    }
}

impl Serialize for Dec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawDec {
    Str(String),
    Num(serde_json::Number),
}

impl<'de> Deserialize<'de> for Dec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = RawDec::deserialize(d)?;
        let text = match raw {
            RawDec::Str(s) => s,
            RawDec::Num(n) => n.to_string(),
        };
        Dec::parse(&text).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Dec {
        Dec::parse(s).unwrap()
    }

    #[test]
    fn parse_and_display_roundtrip() {
        assert_eq!(d("1.25000000").to_string(), "1.25000000");
        assert_eq!(d("-0.5").to_string(), "-0.5");
        assert_eq!(d("37500").to_string(), "37500");
        assert_eq!(d("1e-7").to_string(), "0.0000001");
        assert_eq!(d("1.5E2").to_string(), "150");
        assert_eq!(d(".5").to_string(), "0.5");
    }

    #[test]
    fn parse_rejects_garbage() {
        for s in ["", "abc", "1.2.3", "--1", "1e", "1,5", "0x10", " "] {
            assert!(Dec::parse(s).is_err(), "{s:?} should not parse");
        }
    }

    #[test]
    fn too_precise_is_rejected_but_trailing_zeros_are_fine() {
        assert!(Dec::parse("0.1234567890123456789").is_err());
        assert_eq!(d("1.0000000000000000000"), d("1"));
    }

    #[test]
    fn equality_ignores_scale() {
        assert_eq!(d("1.50"), d("1.5"));
        assert!(d("0.1") < d("0.10000000000000001"));
        assert!(d("-1") < d("-0.5"));
        assert!(d("2") > d("1.999999999999999999"));
    }

    #[test]
    fn rounding_directions() {
        assert_eq!(d("0.123456789").round_dp(8, Rounding::Floor).unwrap().to_string(), "0.12345678");
        assert_eq!(d("0.123456781").round_dp(8, Rounding::Ceil).unwrap().to_string(), "0.12345679");
        assert_eq!(d("0.125").round_dp(2, Rounding::HalfUp).unwrap().to_string(), "0.13");
        assert_eq!(d("0.124").round_dp(2, Rounding::HalfUp).unwrap().to_string(), "0.12");
        assert_eq!(d("-0.121").round_dp(2, Rounding::Floor).unwrap().to_string(), "-0.13");
        assert_eq!(d("5").round_dp(3, Rounding::Floor).unwrap().to_string(), "5.000");
    }

    #[test]
    fn rounding_to_multiples() {
        assert_eq!(d("61234.57").round_to_multiple(d("0.5"), Rounding::Floor).unwrap(), d("61234.5"));
        assert_eq!(d("61234.57").round_to_multiple(d("0.5"), Rounding::Ceil).unwrap(), d("61235"));
        assert!(d("1").round_to_multiple(d("0"), Rounding::Floor).is_err());
    }

    #[test]
    fn to_fixed_is_exact_or_errors() {
        assert_eq!(d("1.5").to_fixed(3).unwrap(), "1.500");
        assert!(d("1.2345").to_fixed(2).is_err());
    }

    #[test]
    fn mul_and_add() {
        assert_eq!(d("0.0025").checked_mul(d("61234.5")).unwrap(), d("153.08625"));
        assert_eq!(d("0.1").checked_add(d("0.2")).unwrap(), d("0.3"));
    }

    #[test]
    fn serde_accepts_strings_and_numbers() {
        let v: Dec = serde_json::from_str("\"0.0001\"").unwrap();
        assert_eq!(v, d("0.0001"));
        let n: Dec = serde_json::from_str("3").unwrap();
        assert_eq!(n, d("3"));
        let f: Dec = serde_json::from_str("0.5").unwrap();
        assert_eq!(f, d("0.5"));
        assert_eq!(serde_json::to_string(&d("1.25")).unwrap(), "\"1.25\"");
    }
}
