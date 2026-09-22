//! Specific refusal reasons. Every rule refuses (returns one of these) instead of guessing.

use std::fmt;

use chrono::NaiveDate;

/// Why a rule or a data structure refused to produce a result.
#[derive(Debug, Clone, PartialEq)]
pub enum RuleError {
    /// `dates` and `closes` passed to `PriceSeries::new` differ in length.
    LengthMismatch {
        symbol: String,
        dates: usize,
        closes: usize,
    },
    /// A series with no bars.
    EmptySeries { symbol: String },
    /// Symbol is empty or contains whitespace/control characters (would make the fingerprint ambiguous).
    InvalidSymbol { symbol: String },
    /// Two series with the same symbol in one panel.
    DuplicateSymbol { symbol: String },
    /// Dates are not strictly ascending (a duplicate date counts as non-monotonic).
    NonMonotonic {
        symbol: String,
        index: usize,
        previous: NaiveDate,
        current: NaiveDate,
    },
    /// Close is NaN, infinite, zero or negative.
    InvalidPrice {
        symbol: String,
        date: NaiveDate,
        value: f64,
    },
    /// Prices in one window span so many binary orders of magnitude that exact arithmetic is refused.
    PriceScaleTooWide { symbol: String },
    /// The panel has no series for a required instrument.
    MissingInstrument { symbol: String },
    /// The decision date is not a bar of this instrument (missing bar).
    DateNotInPanel { symbol: String, date: NaiveDate },
    /// The decision date is not the last bar of its calendar month: a later bar of the same month exists.
    NotMonthEnd {
        symbol: String,
        decision_date: NaiveDate,
        later_bar_in_month: NaiveDate,
    },
    /// `MonthEndMode::NextMonthBar` and no bar from a later month exists yet, so the month is not known to be complete.
    MonthNotComplete {
        symbol: String,
        decision_date: NaiveDate,
    },
    /// `require_panel_end_on_decision_date` was set and bars after the decision date exist.
    PanelDoesNotEndOnDecisionDate {
        symbol: String,
        decision_date: NaiveDate,
        last_bar: NaiveDate,
    },
    /// Not enough history. `needed`/`have` are month-ends (ETF trend) or daily bars (crypto trend).
    InsufficientHistory {
        symbol: String,
        needed: usize,
        have: usize,
    },
    /// The newest bar is older than the allowed staleness relative to `as_of`.
    StaleData {
        symbol: String,
        last_bar: NaiveDate,
        as_of: NaiveDate,
        max_stale_days: i64,
    },
    /// A bar dated on or after `as_of` (the day is not over: a forming bar) is in the panel.
    FormingBar {
        symbol: String,
        bar_date: NaiveDate,
        as_of: NaiveDate,
    },
    /// Two consecutive bars in the required window are further apart than the gap tolerance allows.
    /// `weekdays` says whether `missing`/`tolerance` count weekdays (stocks) or calendar days (crypto).
    DataGap {
        symbol: String,
        from: NaiveDate,
        to: NaiveDate,
        missing: u32,
        tolerance: u32,
        weekdays: bool,
    },
    /// Two instruments that must trade the same sessions disagree on the month-end date of a month.
    MonthEndMismatch {
        symbol_a: String,
        date_a: NaiveDate,
        symbol_b: String,
        date_b: NaiveDate,
    },
    /// The panel has no completed month (no bar from a later month than its first month).
    NoCompletedMonth { symbol: String },
}

impl fmt::Display for RuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for RuleError {}
