//! Asset table (fractionable / tradable / minimum size) and the quantity rounding policy.
//!
//! Policy (deliberate, all tested):
//! * A FRACTIONABLE symbol's quantity is rounded DOWN to 9 decimal places (Alpaca supports
//!   fractional shares to 9 dp, FROM-MEMORY-OF-DOCS), and further down to a multiple of
//!   `min_trade_increment` when the API supplied a coarser one.
//! * A NON-fractionable symbol's quantity is rounded DOWN to whole shares.
//! * A quantity that rounds to zero, or below `min_order_size`, is REFUSED, never bumped up.
//! * An unknown symbol is REFUSED (we do not guess fractionability).
//!
//! Rows come from `GET /v2/assets/{symbol}` JSON ([`AssetInfo::from_json`]); a built-in fallback
//! for the five documented ETF-sleeve symbols exists, marked [`AssetSource::Builtin`] because it
//! was written from memory and is NOT verified against the live API.

use crate::decimal::{Dec, Rounding};
use crate::error::BrokerError;
use serde_json::Value;
use std::collections::BTreeMap;

/// Alpaca supports fractional shares to 9 decimal places (FROM-MEMORY-OF-DOCS).
pub const FRACTIONAL_DP: u32 = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetSource {
    /// Hard-coded in this crate. UNVERIFIED (from memory).
    Builtin,
    /// Parsed from an Alpaca `/v2/assets` response.
    Api,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetInfo {
    pub symbol: String,
    pub tradable: bool,
    pub fractionable: bool,
    /// Explicit minimum order size in shares; zero when the API gave none.
    pub min_order_size: Dec,
    pub min_trade_increment: Option<Dec>,
    pub price_increment: Option<Dec>,
    /// Alpaca `status` (`active` / `inactive`) when known.
    pub status: Option<String>,
    pub source: AssetSource,
}

fn opt_dec(obj: &Value, field: &str) -> Result<Option<Dec>, BrokerError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.trim().is_empty() => Ok(None),
        Some(v) => {
            let text = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                _ => return Err(BrokerError::Malformed(format!("asset field `{field}` is not a number"))),
            };
            Dec::parse(&text)
                .map(Some)
                .map_err(|_| BrokerError::Malformed(format!("asset field `{field}` is not a decimal")))
        }
    }
}

impl AssetInfo {
    /// Parse one `GET /v2/assets/{symbol}` object. `symbol` and `tradable` are required (a row we
    /// cannot classify is an error, not a default); a missing `fractionable` means NOT fractionable
    /// (whole shares, the conservative reading).
    pub fn from_json(v: &Value) -> Result<AssetInfo, BrokerError> {
        let symbol = v
            .get("symbol")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| BrokerError::Malformed("asset has no `symbol`".into()))?
            .to_ascii_uppercase();
        let tradable = v
            .get("tradable")
            .and_then(Value::as_bool)
            .ok_or_else(|| BrokerError::Malformed(format!("asset {symbol}: missing boolean `tradable`")))?;
        let fractionable = v.get("fractionable").and_then(Value::as_bool).unwrap_or(false);
        let min_order_size = opt_dec(v, "min_order_size")?.unwrap_or(Dec::ZERO);
        if min_order_size.is_negative() {
            return Err(BrokerError::Malformed(format!("asset {symbol}: negative min_order_size")));
        }
        Ok(AssetInfo {
            symbol,
            tradable,
            fractionable,
            min_order_size,
            min_trade_increment: opt_dec(v, "min_trade_increment")?.filter(|d| d.is_positive()),
            price_increment: opt_dec(v, "price_increment")?.filter(|d| d.is_positive()),
            status: v.get("status").and_then(Value::as_str).map(str::to_string),
            source: AssetSource::Api,
        })
    }

    pub fn parse_json(json: &str) -> Result<AssetInfo, BrokerError> {
        let v: Value = serde_json::from_str(json).map_err(|_| BrokerError::Malformed("asset body is not JSON".into()))?;
        Self::from_json(&v)
    }

    /// Why this asset cannot be traded, if it cannot.
    pub fn not_tradable_reason(&self) -> Option<String> {
        if !self.tradable {
            return Some("tradable=false".to_string());
        }
        match self.status.as_deref() {
            Some(s) if !s.eq_ignore_ascii_case("active") => Some(format!("status {s}")),
            _ => None,
        }
    }

    /// Round `qty` DOWN per the policy above. Zero and sub-minimum results are errors.
    pub fn round_quantity(&self, qty: Dec) -> Result<Dec, BrokerError> {
        let map = |e: crate::decimal::DecError| BrokerError::InvalidRequest(e.to_string());
        let mut rounded = if self.fractionable {
            let mut q = qty.round_dp(FRACTIONAL_DP, Rounding::Floor).map_err(map)?;
            if let Some(step) = self.min_trade_increment {
                q = q.round_to_multiple(step, Rounding::Floor).map_err(map)?;
            }
            q
        } else {
            qty.round_dp(0, Rounding::Floor).map_err(map)?
        };
        // Canonical scale so `Display` never carries a long run of trailing zeros.
        rounded = rounded.normalized();
        if rounded.is_zero() {
            return Err(BrokerError::QuantityRoundsToZero { symbol: self.symbol.clone(), requested: qty, rounded });
        }
        if rounded < self.min_order_size {
            return Err(BrokerError::BelowMinQuantity { symbol: self.symbol.clone(), min: self.min_order_size, rounded });
        }
        Ok(rounded)
    }
}

#[derive(Debug, Clone, Default)]
pub struct AssetTable {
    assets: BTreeMap<String, AssetInfo>,
}

impl AssetTable {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Built-in fallback rows for SPY, EFA, IEF, DBC and VNQ: tradable and fractionable, no
    /// explicit minimum. UNVERIFIED (from memory); replace with live rows via
    /// [`AssetTable::upsert`] / `AlpacaAdapter::refresh_asset`.
    pub fn builtin() -> Self {
        let mut t = Self::empty();
        for sym in ["SPY", "EFA", "IEF", "DBC", "VNQ"] {
            t.upsert(AssetInfo {
                symbol: sym.to_string(),
                tradable: true,
                fractionable: true,
                min_order_size: Dec::ZERO,
                min_trade_increment: None,
                price_increment: None,
                status: None,
                source: AssetSource::Builtin,
            });
        }
        t
    }

    /// Parse either a single asset object or a `GET /v2/assets` array.
    pub fn from_assets_json(json: &str) -> Result<Self, BrokerError> {
        let v: Value = serde_json::from_str(json).map_err(|_| BrokerError::Malformed("assets body is not JSON".into()))?;
        let mut t = Self::empty();
        match &v {
            Value::Array(items) => {
                for it in items {
                    t.upsert(AssetInfo::from_json(it)?);
                }
            }
            Value::Object(_) => t.upsert(AssetInfo::from_json(&v)?),
            _ => return Err(BrokerError::Malformed("assets body is neither an object nor an array".into())),
        }
        Ok(t)
    }

    pub fn upsert(&mut self, info: AssetInfo) {
        self.assets.insert(info.symbol.to_ascii_uppercase(), info);
    }

    pub fn lookup(&self, symbol: &str) -> Option<&AssetInfo> {
        self.assets.get(&symbol.trim().to_ascii_uppercase())
    }

    pub fn len(&self) -> usize {
        self.assets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.assets.is_empty()
    }
}
