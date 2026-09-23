#![allow(dead_code)]
//! FX test helpers (in addition to `common`): synthetic weekday panels of the seven pairs, with prices that look
//! like FX (around 1, daily moves well under 1%), unrounded so that returns are not quantised.

use chrono::{Datelike, Duration, NaiveDate};
use reference_rules::{
    decide_fx_tsmom, FxTsmomDecision, MonthEndMode, Options, Panel, PriceSeries, RuleError,
    FX_SYMBOLS,
};

use crate::common::{d, Rng};

/// Every weekday from `start` to `end` inclusive.
pub fn weekdays(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut out = Vec::new();
    let mut day = start;
    while day <= end {
        if day.weekday().number_from_monday() <= 5 {
            out.push(day);
        }
        day += Duration::days(1);
    }
    out
}

/// Random-walk closes: `px *= 1 + vol * u`, `u` uniform in (-1, 1). No rounding.
pub fn walk(n: usize, start_px: f64, vol: f64, rng: &mut Rng) -> Vec<f64> {
    let mut px = start_px;
    (0..n)
        .map(|_| {
            px *= 1.0 + vol * (rng.unit() - 0.5) * 2.0;
            px
        })
        .collect()
}

/// Seven independent random walks with pair-specific start level and volatility (0.2% .. 0.9% per day).
pub fn seven_walks(n: usize, rng: &mut Rng) -> Vec<Vec<f64>> {
    (0..FX_SYMBOLS.len())
        .map(|_| {
            let start = 0.5 + 100.0 * rng.unit().powi(3);
            let vol = 0.002 + 0.007 * rng.unit();
            walk(n, start, vol, rng)
        })
        .collect()
}

/// A panel with the same dates for every pair (`closes[k]` belongs to `FX_SYMBOLS[k]`).
pub fn panel_same_dates(dates: &[NaiveDate], closes: &[Vec<f64>]) -> Panel {
    Panel::new(
        FX_SYMBOLS
            .iter()
            .zip(closes)
            .map(|(s, c)| PriceSeries::new(*s, dates.to_vec(), c.clone()).unwrap())
            .collect(),
    )
    .unwrap()
}

/// Rebuild one pair's series from (date, close) pairs.
pub fn with_series(panel: &Panel, symbol: &str, dates: Vec<NaiveDate>, closes: Vec<f64>) -> Panel {
    Panel::new(
        panel
            .iter()
            .map(|s| {
                if s.symbol() == symbol {
                    PriceSeries::new(symbol, dates.clone(), closes.clone()).unwrap()
                } else {
                    s.clone()
                }
            })
            .collect(),
    )
    .unwrap()
}

/// Apply `f(pair index, date, close) -> close` to every bar of every pair.
pub fn map_closes(panel: &Panel, mut f: impl FnMut(usize, NaiveDate, f64) -> f64) -> Panel {
    Panel::new(
        FX_SYMBOLS
            .iter()
            .enumerate()
            .map(|(k, sym)| {
                let s = panel.get(sym).unwrap();
                PriceSeries::new(
                    *sym,
                    s.dates().to_vec(),
                    s.dates()
                        .iter()
                        .zip(s.closes())
                        .map(|(dt, c)| f(k, *dt, *c))
                        .collect(),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

/// The standard case: weekday bars for every pair from 2018-01-01 to 2019-07-05 (bars in July exist, so June is a
/// completed month), decision at the last weekday of June 2019, history window starting at the first bar. 18
/// month-ends up to the decision date.
pub struct Case {
    pub panel: Panel,
    pub dates: Vec<NaiveDate>,
    pub closes: Vec<Vec<f64>>,
    pub decision: NaiveDate,
    pub history_start: NaiveDate,
}

pub fn standard_case(seed: u64) -> Case {
    let mut rng = Rng(seed);
    let dates = weekdays(d(2018, 1, 1), d(2019, 7, 5));
    let closes = seven_walks(dates.len(), &mut rng);
    Case {
        panel: panel_same_dates(&dates, &closes),
        dates,
        closes,
        decision: d(2019, 6, 28),
        history_start: d(2018, 1, 1),
    }
}

/// Replay options for tests: month-end asserted by the caller (panel may end on the decision date), no as_of.
pub fn explicit() -> Options {
    Options::fx_replay(MonthEndMode::Explicit)
}

pub fn decide(
    case_panel: &Panel,
    date: NaiveDate,
    start: NaiveDate,
) -> Result<FxTsmomDecision, RuleError> {
    decide_fx_tsmom(case_panel, date, start, &explicit())
}

/// A random case: random start, length and pairs; the decision is a random month-end with at least 13 month-ends
/// behind it (counting a partial first month) and a later-month bar after it. Returns (panel, decision date,
/// history_start = first bar).
pub fn random_case(seed: u64) -> (Panel, NaiveDate, NaiveDate) {
    let mut rng = Rng(seed);
    let start = d(2005, 1, 1) + Duration::days(rng.below(4000) as i64);
    let end = start + Duration::days(430 + rng.below(700) as i64);
    let dates = weekdays(start, end);
    let closes = seven_walks(dates.len(), &mut rng);
    let panel = panel_same_dates(&dates, &closes);
    let me = reference_rules::completed_month_end_dates(panel.get("EURUSD").unwrap());
    assert!(me.len() >= 14, "generator must give enough history");
    let pick = 12 + rng.below((me.len() - 12) as u64) as usize;
    (panel, me[pick], dates[0])
}
