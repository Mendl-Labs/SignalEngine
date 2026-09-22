//! Validated daily price series and a panel of them.

use std::collections::BTreeMap;

use chrono::NaiveDate;

use crate::error::RuleError;

/// Daily closes of one instrument. Invariants (enforced by `new`): at least one bar, dates strictly ascending,
/// every close finite and strictly positive, symbol non-empty without whitespace or control characters.
#[derive(Debug, Clone, PartialEq)]
pub struct PriceSeries {
    symbol: String,
    dates: Vec<NaiveDate>,
    closes: Vec<f64>,
}

impl PriceSeries {
    pub fn new(
        symbol: impl Into<String>,
        dates: Vec<NaiveDate>,
        closes: Vec<f64>,
    ) -> Result<Self, RuleError> {
        let symbol = symbol.into();
        if symbol.is_empty() || symbol.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(RuleError::InvalidSymbol { symbol });
        }
        if dates.len() != closes.len() {
            return Err(RuleError::LengthMismatch {
                symbol,
                dates: dates.len(),
                closes: closes.len(),
            });
        }
        if dates.is_empty() {
            return Err(RuleError::EmptySeries { symbol });
        }
        for i in 0..dates.len() {
            let value = closes[i];
            if !(value.is_finite() && value > 0.0) {
                return Err(RuleError::InvalidPrice {
                    symbol,
                    date: dates[i],
                    value,
                });
            }
            if i > 0 && dates[i] <= dates[i - 1] {
                return Err(RuleError::NonMonotonic {
                    symbol,
                    index: i,
                    previous: dates[i - 1],
                    current: dates[i],
                });
            }
        }
        Ok(Self {
            symbol,
            dates,
            closes,
        })
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    pub fn dates(&self) -> &[NaiveDate] {
        &self.dates
    }
    pub fn closes(&self) -> &[f64] {
        &self.closes
    }
    pub fn len(&self) -> usize {
        self.dates.len()
    }
    pub fn is_empty(&self) -> bool {
        self.dates.is_empty()
    }
    pub fn first_date(&self) -> NaiveDate {
        self.dates[0]
    }
    pub fn last_date(&self) -> NaiveDate {
        self.dates[self.dates.len() - 1]
    }

    /// Index of the bar dated exactly `date`.
    pub fn position_of(&self, date: NaiveDate) -> Option<usize> {
        self.dates.binary_search(&date).ok()
    }

    /// The bars dated on or before `date` (None if there are none).
    pub fn truncated_to(&self, date: NaiveDate) -> Option<PriceSeries> {
        let n = self.dates.partition_point(|d| *d <= date);
        if n == 0 {
            return None;
        }
        Some(PriceSeries {
            symbol: self.symbol.clone(),
            dates: self.dates[..n].to_vec(),
            closes: self.closes[..n].to_vec(),
        })
    }
}

/// A set of series keyed by symbol. Iteration order is by symbol, so results never depend on insertion order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Panel {
    series: BTreeMap<String, PriceSeries>,
}

impl Panel {
    pub fn new(series: Vec<PriceSeries>) -> Result<Self, RuleError> {
        let mut map = BTreeMap::new();
        for s in series {
            let key = s.symbol().to_string();
            if map.insert(key.clone(), s).is_some() {
                return Err(RuleError::DuplicateSymbol { symbol: key });
            }
        }
        Ok(Self { series: map })
    }

    pub fn get(&self, symbol: &str) -> Result<&PriceSeries, RuleError> {
        self.series
            .get(symbol)
            .ok_or_else(|| RuleError::MissingInstrument {
                symbol: symbol.to_string(),
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = &PriceSeries> {
        self.series.values()
    }

    pub fn symbols(&self) -> Vec<&str> {
        self.series.keys().map(String::as_str).collect()
    }

    /// Every series cut to bars on or before `date` (instruments with no such bar are dropped).
    pub fn truncated_to(&self, date: NaiveDate) -> Panel {
        Panel {
            series: self
                .series
                .iter()
                .filter_map(|(k, s)| s.truncated_to(date).map(|t| (k.clone(), t)))
                .collect(),
        }
    }
}
