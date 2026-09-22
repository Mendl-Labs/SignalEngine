//! Calendar-free month-end and gap helpers. Nothing here knows about holidays: a month-end is "the last bar
//! of a calendar month that the data contains", and a month is known to be over only when a later month's bar
//! exists.

use chrono::{Datelike, NaiveDate};

use crate::error::RuleError;
use crate::options::GapPolicy;
use crate::series::{Panel, PriceSeries};

fn same_month(a: NaiveDate, b: NaiveDate) -> bool {
    a.year() == b.year() && a.month() == b.month()
}

/// Indices of the last bar of every calendar month present in `dates` (ascending). The last index is always
/// included, so the final month is treated as complete: use `completed_month_end_dates` when the data may end
/// inside a month.
pub fn month_end_indices(dates: &[NaiveDate]) -> Vec<usize> {
    (0..dates.len())
        .filter(|&i| i + 1 == dates.len() || !same_month(dates[i], dates[i + 1]))
        .collect()
}

/// The last bar of every calendar month present, including the month of the newest bar (which may still be
/// forming).
pub fn month_end_dates(series: &PriceSeries) -> Vec<NaiveDate> {
    month_end_indices(series.dates())
        .into_iter()
        .map(|i| series.dates()[i])
        .collect()
}

/// Month-ends of months proven complete by the data itself: every month strictly before the month of the newest
/// bar.
pub fn completed_month_end_dates(series: &PriceSeries) -> Vec<NaiveDate> {
    let mut all = month_end_dates(series);
    all.pop();
    all
}

/// The month-end decision date in force, calendar-free: the newest completed month-end, which every listed
/// instrument must agree on. Refuses when an instrument has no completed month or when instruments disagree.
pub fn latest_decision_date(panel: &Panel, symbols: &[&str]) -> Result<NaiveDate, RuleError> {
    let mut first: Option<(String, NaiveDate)> = None;
    for sym in symbols {
        let s = panel.get(sym)?;
        let last = completed_month_end_dates(s)
            .last()
            .copied()
            .ok_or_else(|| RuleError::NoCompletedMonth {
                symbol: sym.to_string(),
            })?;
        match &first {
            None => first = Some((sym.to_string(), last)),
            Some((sa, da)) if *da != last => {
                return Err(RuleError::MonthEndMismatch {
                    symbol_a: sa.clone(),
                    date_a: *da,
                    symbol_b: sym.to_string(),
                    date_b: last,
                })
            }
            Some(_) => {}
        }
    }
    first.map(|(_, d)| d).ok_or(RuleError::NoCompletedMonth {
        symbol: String::new(),
    })
}

/// Number of weekdays (Mon-Fri) strictly between `a` and `b` (a < b).
pub fn missing_weekdays_between(a: NaiveDate, b: NaiveDate) -> u32 {
    let n = (b - a).num_days() - 1;
    if n <= 0 {
        return 0;
    }
    // Any 7 consecutive days contain exactly 5 weekdays; only the remainder depends on the start weekday.
    let mut count = (n / 7) * 5;
    let mut d = a;
    for _ in 0..(n % 7) {
        d = match d.succ_opt() {
            Some(x) => x,
            None => break,
        };
        if d.weekday().number_from_monday() <= 5 {
            count += 1;
        }
    }
    count as u32
}

/// Number of calendar days strictly between `a` and `b` (a < b).
pub fn missing_days_between(a: NaiveDate, b: NaiveDate) -> u32 {
    ((b - a).num_days() - 1).max(0) as u32
}

/// Refuse when any two consecutive `dates` are further apart than `policy` allows.
pub(crate) fn check_gaps(
    symbol: &str,
    dates: &[NaiveDate],
    policy: GapPolicy,
) -> Result<(), RuleError> {
    for w in dates.windows(2) {
        let (missing, tolerance, weekdays) = match policy {
            GapPolicy::Unchecked => return Ok(()),
            GapPolicy::MaxMissingWeekdays(t) => (missing_weekdays_between(w[0], w[1]), t, true),
            GapPolicy::MaxMissingDays(t) => (missing_days_between(w[0], w[1]), t, false),
        };
        if missing > tolerance {
            return Err(RuleError::DataGap {
                symbol: symbol.to_string(),
                from: w[0],
                to: w[1],
                missing,
                tolerance,
                weekdays,
            });
        }
    }
    Ok(())
}
