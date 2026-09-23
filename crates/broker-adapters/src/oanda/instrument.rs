//! Instrument naming (ONE normalisation function) and the instrument metadata table.
//!
//! # Naming
//! OANDA names instruments `BASE_QUOTE` (`EUR_USD`; VERIFIED-FROM-REPO-CODE: the legacy
//! connector's symbol format uses `_`). [`normalize_instrument`] is the only place any other shape
//! is converted, and it refuses what it does not recognise:
//!
//! * `EUR/USD`, `EUR-USD`, `EUR_USD`, `eur_usd` -> `EUR_USD`;
//! * a bare six-letter `EURUSD` -> `EUR_USD`;
//! * anything else (empty, more than one separator, a quote that is not three letters, a bare string
//!   that is not six letters, digits in the quote, whitespace inside, a trailing separator, ...) is
//!   `BrokerError::UnknownSymbol`. A bare `XAUUSD`-style concatenation is accepted only when it is
//!   exactly six letters; index/CFD names (`SPX500_USD`) need their separator.
//!
//! Passing a shape-valid name says nothing about whether the account can trade it: that is decided
//! by the [`InstrumentTable`], which is filled from the broker's own instrument list.
//!
//! # Metadata (FROM-MEMORY-OF-DOCS)
//! `GET /v3/accounts/{id}/instruments` returns `{"instruments": [...]}`; each row has `name`,
//! `type`, `displayName`, `pipLocation` (integer), `displayPrecision` (price decimals),
//! `tradeUnitsPrecision` (unit decimals; 0 for FX), `minimumTradeSize`, `maximumOrderUnits` and
//! `marginRate`, all numbers as decimal strings. There are NO built-in rows: an instrument that
//! was not loaded from the broker cannot be traded (fail closed), and no precision constant lives
//! in this crate.

use crate::decimal::Dec;
use crate::error::BrokerError;
use crate::oanda::parse::{json_of, opt_str, req_dec, req_str};
use serde_json::Value;
use std::collections::BTreeMap;

/// Normalise any accepted spelling to OANDA's `BASE_QUOTE`. See the module docs.
pub fn normalize_instrument(input: &str) -> Result<String, BrokerError> {
    let bad = || BrokerError::UnknownSymbol(format!("{input:?} is not a recognised OANDA instrument name"));
    let s = input.trim().to_ascii_uppercase();
    if s.is_empty() || !s.is_ascii() {
        return Err(bad());
    }
    let seps: Vec<usize> = s.char_indices().filter(|(_, c)| matches!(c, '_' | '/' | '-')).map(|(i, _)| i).collect();
    let (base, quote) = match seps.as_slice() {
        [] => {
            if s.len() == 6 && s.bytes().all(|b| b.is_ascii_uppercase()) {
                (s[..3].to_string(), s[3..].to_string())
            } else {
                return Err(bad());
            }
        }
        [i] => (s[..*i].to_string(), s[*i + 1..].to_string()),
        _ => return Err(bad()),
    };
    let base_ok = base.len() >= 2
        && base.len() <= 8
        && base.starts_with(|c: char| c.is_ascii_uppercase())
        && base.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit());
    let quote_ok = quote.len() == 3 && quote.bytes().all(|b| b.is_ascii_uppercase());
    if !(base_ok && quote_ok) {
        return Err(bad());
    }
    Ok(format!("{base}_{quote}"))
}

/// `EUR_USD` -> `EUR/USD` (the platform's canonical `BASE/QUOTE` spelling). The input is
/// normalised first, so any accepted spelling works.
pub fn canonical_symbol(input: &str) -> Result<String, BrokerError> {
    Ok(normalize_instrument(input)?.replacen('_', "/", 1))
}

/// Quote currency of a normalised instrument name (`EUR_USD` -> `USD`).
pub fn quote_currency(instrument: &str) -> Option<&str> {
    instrument.split_once('_').map(|(_, q)| q)
}

/// One row of the broker's instrument list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentInfo {
    /// `EUR_USD`.
    pub name: String,
    /// `CURRENCY`, `CFD` or `METAL`, verbatim.
    pub kind: String,
    /// Decimals of a price (`displayPrecision`).
    pub display_precision: u32,
    /// Decimals of a unit count (`tradeUnitsPrecision`); 0 means whole units.
    pub trade_units_precision: u32,
    pub minimum_trade_size: Dec,
    pub maximum_order_units: Dec,
    /// `marginRate` (0.02 = 2 percent = 50:1).
    pub margin_rate: Dec,
}

impl InstrumentInfo {
    fn parse_value(v: &Value) -> Result<Self, BrokerError> {
        let name = normalize_instrument(req_str(v, "name")?)?;
        let precision = |field: &str| -> Result<u32, BrokerError> {
            let n = v
                .get(field)
                .and_then(Value::as_u64)
                .ok_or_else(|| BrokerError::Malformed(format!("instrument {name}: missing or non-integer `{field}`")))?;
            u32::try_from(n)
                .ok()
                .filter(|p| *p <= 12)
                .ok_or_else(|| BrokerError::Malformed(format!("instrument {name}: `{field}` {n} is out of range")))
        };
        let info = InstrumentInfo {
            display_precision: precision("displayPrecision")?,
            trade_units_precision: precision("tradeUnitsPrecision")?,
            minimum_trade_size: req_dec(v, "minimumTradeSize")?,
            maximum_order_units: req_dec(v, "maximumOrderUnits")?,
            margin_rate: req_dec(v, "marginRate")?,
            kind: opt_str(v, "type").unwrap_or_default(),
            name,
        };
        if !info.minimum_trade_size.is_positive() {
            return Err(BrokerError::Malformed(format!("instrument {}: minimumTradeSize is not positive", info.name)));
        }
        if !info.maximum_order_units.is_positive() {
            return Err(BrokerError::Malformed(format!("instrument {}: maximumOrderUnits is not positive", info.name)));
        }
        Ok(info)
    }
}

/// Instruments the account can trade, keyed by normalised name.
#[derive(Debug, Clone, Default)]
pub struct InstrumentTable {
    rows: BTreeMap<String, InstrumentInfo>,
}

impl InstrumentTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_rows(rows: impl IntoIterator<Item = InstrumentInfo>) -> Self {
        Self { rows: rows.into_iter().map(|r| (r.name.clone(), r)).collect() }
    }

    /// Parse the body of `GET /v3/accounts/{id}/instruments`. One malformed row fails the whole
    /// table: a half-loaded table would make some instruments silently untradable or, worse, mis-rounded.
    pub fn from_instruments_json(body: &str) -> Result<Self, BrokerError> {
        let v = json_of(body, "instruments")?;
        let arr = v
            .get("instruments")
            .and_then(Value::as_array)
            .ok_or_else(|| BrokerError::Malformed("instruments response has no `instruments` array".into()))?;
        let mut rows = BTreeMap::new();
        for row in arr {
            let info = InstrumentInfo::parse_value(row)?;
            rows.insert(info.name.clone(), info);
        }
        Ok(Self { rows })
    }

    /// Look up by any accepted spelling. `None` for a name that is unknown or not shape-valid.
    pub fn lookup(&self, name: &str) -> Option<&InstrumentInfo> {
        normalize_instrument(name).ok().and_then(|n| self.rows.get(&n))
    }

    pub fn upsert(&mut self, info: InstrumentInfo) {
        self.rows.insert(info.name.clone(), info);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.rows.keys().map(String::as_str)
    }
}
