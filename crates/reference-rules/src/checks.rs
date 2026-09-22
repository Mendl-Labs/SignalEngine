//! Data-quality checks shared by both rules.

use chrono::{Datelike, NaiveDate};

use crate::error::RuleError;
use crate::options::{MonthEndMode, Options};
use crate::series::PriceSeries;

/// Forming-bar and staleness checks against the run date (skipped when `opts.as_of` is `None`).
pub(crate) fn check_as_of(series: &PriceSeries, opts: &Options) -> Result<(), RuleError> {
    let Some(as_of) = opts.as_of else {
        return Ok(());
    };
    let last = series.last_date();
    if last >= as_of {
        return Err(RuleError::FormingBar {
            symbol: series.symbol().to_string(),
            bar_date: last,
            as_of,
        });
    }
    if (as_of - last).num_days() > opts.max_stale_days {
        return Err(RuleError::StaleData {
            symbol: series.symbol().to_string(),
            last_bar: last,
            as_of,
            max_stale_days: opts.max_stale_days,
        });
    }
    Ok(())
}

/// Index of the decision date, or `DateNotInPanel`.
pub(crate) fn locate(series: &PriceSeries, date: NaiveDate) -> Result<usize, RuleError> {
    series
        .position_of(date)
        .ok_or_else(|| RuleError::DateNotInPanel {
            symbol: series.symbol().to_string(),
            date,
        })
}

/// The bars after `pos` (if any) must not extend into the decision date's month; in `NextMonthBar` mode a bar
/// from a later month must exist; with `require_panel_end_on_decision_date` no bar may follow at all.
pub(crate) fn check_month_end(
    series: &PriceSeries,
    pos: usize,
    opts: &Options,
) -> Result<(), RuleError> {
    let d = series.dates()[pos];
    let symbol = series.symbol().to_string();
    if let Some(&next) = series.dates().get(pos + 1) {
        if next.year() == d.year() && next.month() == d.month() {
            return Err(RuleError::NotMonthEnd {
                symbol,
                decision_date: d,
                later_bar_in_month: next,
            });
        }
    } else if opts.month_end_mode == MonthEndMode::NextMonthBar {
        return Err(RuleError::MonthNotComplete {
            symbol,
            decision_date: d,
        });
    }
    check_panel_end(series, pos, opts)
}

/// `require_panel_end_on_decision_date`: no bar after the decision date.
pub(crate) fn check_panel_end(
    series: &PriceSeries,
    pos: usize,
    opts: &Options,
) -> Result<(), RuleError> {
    if opts.require_panel_end_on_decision_date && pos + 1 < series.len() {
        return Err(RuleError::PanelDoesNotEndOnDecisionDate {
            symbol: series.symbol().to_string(),
            decision_date: series.dates()[pos],
            last_bar: series.last_date(),
        });
    }
    Ok(())
}
