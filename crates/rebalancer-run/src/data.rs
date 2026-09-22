//! What the pipeline needs from the data layer, as traits, plus the sleeve description.
//!
//! A [`DataSource`] returns VALIDATED price panels (`reference_rules::Panel`: strictly ascending dates, positive
//! finite closes) and current prices for sizing. It is the seam where the real data gate (WP2.3: last complete bar,
//! no forming bar, retry on 429, bypass caches) plugs in. The reference rules apply their own checks on top
//! (staleness, gaps, month-end), and ANY refusal from either layer means the run trades nothing.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use rebalancer_core::guard::PricePoint;
use rebalancer_core::Dec;
use reference_rules::Panel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleeveKind {
    /// Faber-style ETF trend at month-ends (SPY, EFA, IEF, DBC, VNQ; 20% each of the sleeve).
    EtfTrend,
    /// 100-day crypto trend (BTC, ETH; 50% each of the sleeve).
    CryptoTrend,
}

/// One strategy sleeve of an account's plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleeveSpec {
    /// Stable id (part of the run key and of every order tag).
    pub id: String,
    pub kind: SleeveKind,
    /// Fraction of capital, in (0, 1]; all sleeves of a run sum to at most 1.
    pub share: Dec,
    pub venue: String,
    pub asset_class: String,
    /// Quote currency for crypto pairs (`BTC` becomes `BTC/USD`); ignored for ETFs.
    pub quote: String,
}

/// The validated data one sleeve's rule needs.
#[derive(Debug, Clone)]
pub struct SleeveData {
    pub panel: Panel,
}

/// A data problem. `code` is stable (`DATA_UNAVAILABLE`, `DATA_STALE`, ...); the pipeline maps any error to
/// `RUN_DATA_ERROR` and records both.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct DataError {
    pub code: String,
    pub message: String,
}

impl DataError {
    pub fn new(code: &str, message: &str) -> Self {
        Self { code: code.to_string(), message: message.to_string() }
    }
}

pub trait DataSource {
    /// The panel for a sleeve as of the run date `as_of` (the UTC date of the scheduled time).
    fn sleeve_data(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError>;

    /// Current prices for sizing and for valuing held positions, by canonical symbol. A symbol that cannot be
    /// priced is simply absent (the planner then skips it and records `NoPrice`); an outage is an `Err`.
    fn prices(&self, symbols: &[String], now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError>;
}
