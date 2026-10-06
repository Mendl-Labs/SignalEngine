//! The public ticker (`GET /api/v2/ticker/{market_symbol}/`, no key). Documented fields: `last`, `bid`, `ask`,
//! `timestamp` (a string of Unix seconds in the documented example), plus high/low/vwap/volume.
//!
//! UNVERIFIED: the documentation page does not say explicitly that `bid`/`ask` are the best bid and best ask, and it
//! does not state the production base URL in a quoted line. The field names and the timestamp unit come from the
//! documented example only.

use serde_json::Value;

use crate::decimal::Dec;
use crate::error::BrokerError;

use super::parse::{dec_field, malformed, text_field};

pub fn ticker_path(market_symbol: &str) -> String {
    format!("/api/v2/ticker/{market_symbol}/")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickerQuote {
    /// Canonical symbol (`BTC/USD`).
    pub symbol: String,
    pub bid: Dec,
    pub ask: Dec,
    pub last: Dec,
    /// Unix seconds, as the ticker reports it.
    pub timestamp_secs: i64,
}

impl TickerQuote {
    /// `(bid + ask) / 2`, exact: the sum is halved by moving the decimal point one place. `None` on overflow.
    pub fn mid(&self) -> Option<Dec> {
        let sum = self.bid.checked_add(self.ask)?;
        let units = sum.units().checked_mul(5)?;
        Dec::new(units, sum.scale().checked_add(1)?).ok().map(Dec::normalized)
    }
}

pub fn parse_ticker(symbol: &str, v: &Value) -> Result<TickerQuote, BrokerError> {
    let bid = dec_field(v, "bid")?;
    let ask = dec_field(v, "ask")?;
    let last = dec_field(v, "last")?;
    if !bid.is_positive() || !ask.is_positive() {
        return Err(malformed("ticker bid or ask is not positive"));
    }
    if bid > ask {
        return Err(malformed("ticker bid is above ask (crossed book)"));
    }
    let timestamp_secs = text_field(v, "timestamp")?
        .parse::<i64>()
        .ok()
        .filter(|t| *t > 0)
        .ok_or_else(|| malformed("ticker timestamp is not a positive integer"))?;
    Ok(TickerQuote { symbol: symbol.to_string(), bid, ask, last, timestamp_secs })
}
