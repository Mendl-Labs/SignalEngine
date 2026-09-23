//! Data-quality options that accompany a decision. They are explicit arguments (never globals) so that a run
//! record can state exactly which tolerances applied.

use chrono::NaiveDate;

/// Stocks/ETFs: the newest bar may be at most this many calendar days older than `as_of`
/// (same as `max_stale_days` = 5 in the reference tool's config.json; covers a Friday close read on Tuesday
/// after a Monday holiday).
pub const ETF_MAX_STALE_DAYS: i64 = 5;
/// Stocks/ETFs: at most this many WEEKDAYS may be missing between two consecutive bars inside the required
/// window. One missing weekday is an ordinary market holiday; 3 leaves room for a holiday next to another
/// closure, while a hole of a week or more is refused. A single missing ordinary session is NOT detectable
/// without an exchange calendar (see crate docs, "Residual risk").
pub const ETF_MAX_MISSING_WEEKDAYS: u32 = 3;
/// Crypto: the newest bar must be yesterday's (UTC) bar: `as_of - last_bar` may be at most 1 day.
pub const CRYPTO_MAX_STALE_DAYS: i64 = 1;
/// Crypto trades every day: no calendar day may be missing inside the 100-bar window.
pub const CRYPTO_MAX_MISSING_DAYS: u32 = 0;

/// How much of the calendar may be missing between two consecutive bars of the required window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapPolicy {
    /// No gap check (only for replaying a reference data set that itself has holes; never for live decisions).
    Unchecked,
    /// At most `n` weekdays (Mon-Fri) strictly between two consecutive bars (stocks, ETFs).
    MaxMissingWeekdays(u32),
    /// At most `n` calendar days strictly between two consecutive bars (24/7 markets).
    MaxMissingDays(u32),
}

/// When is a date accepted as the month-end decision date (ETF trend and FX momentum; crypto trend ignores it)?
///
/// In both modes the decision date must be a bar of every instrument and the LAST bar of its calendar month in
/// the panel (a later bar of the same month refuses with `NotMonthEnd`). Bars after the decision date, if any,
/// are ignored by the computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonthEndMode {
    /// Calendar-free and the default for live use: a bar from a later month must exist in the data, which proves
    /// the month is over without a holiday table. Costs one session of delay (decide once the first bar of the
    /// next month has printed).
    NextMonthBar,
    /// The caller asserts (from an exchange calendar) that the decision date is the month's final session; the
    /// panel may end exactly on it. The crate cannot verify the assertion: if the panel ends before the month's
    /// true final bar, the rule would treat an earlier day as the month-end.
    Explicit,
}

/// Options for one decision.
#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    /// The run date (UTC). `Some`: bars dated on or after it are refused as forming (`FormingBar`) and the
    /// newest bar may be at most `max_stale_days` older (`StaleData`). `None`: historical replay, neither check.
    pub as_of: Option<NaiveDate>,
    pub max_stale_days: i64,
    pub gap_policy: GapPolicy,
    /// ETF trend and FX momentum; ignored by the crypto rule.
    pub month_end_mode: MonthEndMode,
    /// Refuse (`PanelDoesNotEndOnDecisionDate`) when any instrument has a bar after the decision date.
    pub require_panel_end_on_decision_date: bool,
}

impl Options {
    /// ETF trend on live data: calendar-free month-end (a next-month bar must exist).
    pub fn etf_live(as_of: NaiveDate) -> Self {
        Self {
            as_of: Some(as_of),
            max_stale_days: ETF_MAX_STALE_DAYS,
            gap_policy: GapPolicy::MaxMissingWeekdays(ETF_MAX_MISSING_WEEKDAYS),
            month_end_mode: MonthEndMode::NextMonthBar,
            require_panel_end_on_decision_date: false,
        }
    }

    /// ETF trend replayed over history (no as_of checks); strict gap policy.
    pub fn etf_replay(mode: MonthEndMode) -> Self {
        Self {
            as_of: None,
            month_end_mode: mode,
            ..Self::etf_live(NaiveDate::MIN)
        }
    }

    /// FX momentum on live data: same staleness and month-end tolerances as the ETF rule (weekday FX bars;
    /// a next-month bar must exist). The gap policy is applied to the JOINT calendar of the seven pairs.
    pub fn fx_live(as_of: NaiveDate) -> Self {
        Self::etf_live(as_of)
    }

    /// FX momentum replayed over history (no as_of checks); strict gap policy on the joint calendar. Pass
    /// `gap_policy = GapPolicy::Unchecked` on the result to replay a data set that itself has holes.
    pub fn fx_replay(mode: MonthEndMode) -> Self {
        Self::etf_replay(mode)
    }

    /// Crypto trend on live data: newest bar = yesterday, panel must end on the decision date.
    pub fn crypto_live(as_of: NaiveDate) -> Self {
        Self {
            as_of: Some(as_of),
            max_stale_days: CRYPTO_MAX_STALE_DAYS,
            gap_policy: GapPolicy::MaxMissingDays(CRYPTO_MAX_MISSING_DAYS),
            month_end_mode: MonthEndMode::Explicit,
            require_panel_end_on_decision_date: true,
        }
    }

    /// Crypto trend replayed over history: bars after the decision date are allowed (and ignored).
    pub fn crypto_replay() -> Self {
        Self {
            as_of: None,
            require_panel_end_on_decision_date: false,
            ..Self::crypto_live(NaiveDate::MIN)
        }
    }
}
