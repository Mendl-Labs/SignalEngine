//! Every refusal path returns its specific RuleError.

mod common;

use chrono::Duration;
use common::*;
use reference_rules::*;

fn etf_panel_same(n_months: usize) -> Panel {
    let closes = vec![100.0; n_months];
    Panel::new(
        ETF_SYMBOLS
            .iter()
            .map(|s| etf_series(s, 2019, 8, &closes, 100.0))
            .collect(),
    )
    .unwrap()
}

/// Series spanning Aug 2019 .. Jun 2020 (11 months). 10th month-end = 2020-05-29; last bar 2020-06-30.
fn etf_eleven() -> Panel {
    etf_panel_same(11)
}
const ETF_D: (i32, u32, u32) = (2020, 5, 29);
fn etf_d() -> chrono::NaiveDate {
    d(ETF_D.0, ETF_D.1, ETF_D.2)
}

fn replace_series(panel: &Panel, symbol: &str, f: impl Fn(&PriceSeries) -> PriceSeries) -> Panel {
    Panel::new(
        panel
            .iter()
            .map(|s| {
                if s.symbol() == symbol {
                    f(s)
                } else {
                    s.clone()
                }
            })
            .collect(),
    )
    .unwrap()
}

fn without_dates(s: &PriceSeries, drop: impl Fn(chrono::NaiveDate) -> bool) -> PriceSeries {
    let (dates, closes): (Vec<_>, Vec<_>) = s
        .dates()
        .iter()
        .zip(s.closes())
        .filter(|(dt, _)| !drop(**dt))
        .map(|(a, b)| (*a, *b))
        .unzip();
    PriceSeries::new(s.symbol(), dates, closes).unwrap()
}

// ------------------------------------------------------------------ construction

#[test]
fn series_construction_refusals() {
    let ok_dates = vec![d(2020, 1, 2), d(2020, 1, 3)];
    assert!(matches!(
        PriceSeries::new("A", ok_dates.clone(), vec![1.0]),
        Err(RuleError::LengthMismatch {
            dates: 2,
            closes: 1,
            ..
        })
    ));
    assert!(matches!(
        PriceSeries::new("A", vec![], vec![]),
        Err(RuleError::EmptySeries { .. })
    ));
    assert!(matches!(
        PriceSeries::new("", ok_dates.clone(), vec![1.0, 2.0]),
        Err(RuleError::InvalidSymbol { .. })
    ));
    assert!(matches!(
        PriceSeries::new("A B", ok_dates.clone(), vec![1.0, 2.0]),
        Err(RuleError::InvalidSymbol { .. })
    ));
    for bad in [f64::NAN, 0.0, -1.0, f64::INFINITY, f64::NEG_INFINITY, -0.0] {
        let r = PriceSeries::new("A", ok_dates.clone(), vec![1.0, bad]);
        assert!(
            matches!(r, Err(RuleError::InvalidPrice { date, .. }) if date == d(2020, 1, 3)),
            "{bad}: {r:?}"
        );
    }
    let desc = PriceSeries::new("A", vec![d(2020, 1, 3), d(2020, 1, 2)], vec![1.0, 2.0]);
    assert!(
        matches!(desc, Err(RuleError::NonMonotonic { index: 1, .. })),
        "{desc:?}"
    );
    let dup = PriceSeries::new("A", vec![d(2020, 1, 2), d(2020, 1, 2)], vec![1.0, 2.0]);
    assert!(
        matches!(dup, Err(RuleError::NonMonotonic { .. })),
        "{dup:?}"
    );
    let a = PriceSeries::new("A", vec![d(2020, 1, 2)], vec![1.0]).unwrap();
    assert!(matches!(
        Panel::new(vec![a.clone(), a]),
        Err(RuleError::DuplicateSymbol { .. })
    ));
}

#[test]
fn missing_instrument_is_refused() {
    let panel = etf_eleven();
    let missing_vnq = Panel::new(
        panel
            .iter()
            .filter(|s| s.symbol() != "VNQ")
            .cloned()
            .collect(),
    )
    .unwrap();
    let err = decide_etf_trend(
        &missing_vnq,
        etf_d(),
        &Options::etf_replay(MonthEndMode::Explicit),
    )
    .unwrap_err();
    assert_eq!(
        err,
        RuleError::MissingInstrument {
            symbol: "VNQ".into()
        }
    );
    let crypto = Panel::new(vec![crypto_series("BTC", d(2020, 6, 30), &[100.0; 100])]).unwrap();
    let err = decide_crypto_trend(&crypto, d(2020, 6, 30), &Options::crypto_replay()).unwrap_err();
    assert_eq!(
        err,
        RuleError::MissingInstrument {
            symbol: "ETH".into()
        }
    );
}

// ------------------------------------------------------------------ missing / forming / stale (ETF)

#[test]
fn etf_missing_bar_on_the_decision_date_is_refused() {
    let panel = replace_series(&etf_eleven(), "IEF", |s| without_dates(s, |x| x == etf_d()));
    let err = decide_etf_trend(
        &panel,
        etf_d(),
        &Options::etf_replay(MonthEndMode::Explicit),
    )
    .unwrap_err();
    assert_eq!(
        err,
        RuleError::DateNotInPanel {
            symbol: "IEF".into(),
            date: etf_d()
        }
    );
}

#[test]
fn etf_instruments_that_disagree_on_a_month_end_are_refused() {
    // DBC lacks the session on which everyone else closed the month of Jan 2020 (Fri 2020-01-31).
    let panel = replace_series(&etf_eleven(), "DBC", |s| {
        without_dates(s, |x| x == d(2020, 1, 31))
    });
    let err = decide_etf_trend(
        &panel,
        etf_d(),
        &Options::etf_replay(MonthEndMode::Explicit),
    )
    .unwrap_err();
    assert!(
        matches!(&err, RuleError::MonthEndMismatch { date_a, date_b, .. } if (*date_a, *date_b) == (d(2020, 1, 31), d(2020, 1, 30)) || (*date_a, *date_b) == (d(2020, 1, 30), d(2020, 1, 31))),
        "{err:?}"
    );
}

#[test]
fn etf_live_options_forming_and_stale() {
    let panel = etf_eleven(); // newest bar 2020-06-30
    let last = d(2020, 6, 30);
    let live = |as_of| Options::etf_live(as_of);
    assert!(decide_etf_trend(&panel, etf_d(), &live(last + Duration::days(1))).is_ok());
    assert!(
        decide_etf_trend(&panel, etf_d(), &live(last + Duration::days(5))).is_ok(),
        "5 days old is the boundary and allowed"
    );
    let err = decide_etf_trend(&panel, etf_d(), &live(last + Duration::days(6))).unwrap_err();
    assert!(
        matches!(err, RuleError::StaleData { last_bar, max_stale_days: 5, .. } if last_bar == last),
        "{err:?}"
    );
    // A bar dated as_of (the day is not over) or later is a forming bar.
    let err = decide_etf_trend(&panel, etf_d(), &live(last)).unwrap_err();
    assert!(
        matches!(err, RuleError::FormingBar { bar_date, .. } if bar_date == last),
        "{err:?}"
    );
    let err = decide_etf_trend(&panel, etf_d(), &live(last - Duration::days(3))).unwrap_err();
    assert!(matches!(err, RuleError::FormingBar { .. }), "{err:?}");
    // One stale instrument refuses the sleeve.
    let one_stale = replace_series(&panel, "SPY", |s| without_dates(s, |x| x > d(2020, 6, 22)));
    let err = decide_etf_trend(&one_stale, etf_d(), &live(last + Duration::days(1))).unwrap_err();
    assert!(
        matches!(&err, RuleError::StaleData { symbol, .. } if symbol == "SPY"),
        "{err:?}"
    );
}

#[test]
fn etf_panel_must_end_on_the_decision_date_when_required() {
    let mut opts = Options::etf_replay(MonthEndMode::Explicit);
    opts.require_panel_end_on_decision_date = true;
    let err = decide_etf_trend(&etf_eleven(), etf_d(), &opts).unwrap_err();
    assert!(
        matches!(err, RuleError::PanelDoesNotEndOnDecisionDate { last_bar, .. } if last_bar == d(2020, 6, 30)),
        "{err:?}"
    );
    let ten = etf_eleven().truncated_to(etf_d());
    assert!(decide_etf_trend(&ten, etf_d(), &opts).is_ok());
}

// ------------------------------------------------------------------ gaps (ETF)

fn drop_weekdays_from(
    panel: &Panel,
    symbol: &str,
    first: chrono::NaiveDate,
    n_weekdays: usize,
) -> Panel {
    let mut dropped = Vec::new();
    let mut day = first;
    while dropped.len() < n_weekdays {
        if s_is_weekday(day) {
            dropped.push(day);
        }
        day += Duration::days(1);
    }
    replace_series(panel, symbol, |s| {
        without_dates(s, |x| dropped.contains(&x))
    })
}

fn s_is_weekday(x: chrono::NaiveDate) -> bool {
    use chrono::Datelike;
    x.weekday().number_from_monday() <= 5
}

#[test]
fn etf_gap_tolerance_is_three_missing_weekdays() {
    let opts = Options::etf_replay(MonthEndMode::Explicit);
    // Mon 2020-03-09 .. : 3 missing weekdays is the tolerance, 4 is refused.
    let three = drop_weekdays_from(&etf_eleven(), "EFA", d(2020, 3, 9), 3);
    assert!(decide_etf_trend(&three, etf_d(), &opts).is_ok());
    let four = drop_weekdays_from(&etf_eleven(), "EFA", d(2020, 3, 9), 4);
    let err = decide_etf_trend(&four, etf_d(), &opts).unwrap_err();
    assert!(
        matches!(&err, RuleError::DataGap { symbol, missing: 4, tolerance: 3, weekdays: true, .. } if symbol == "EFA"),
        "{err:?}"
    );
    // A hole older than the first month-end used is outside the window and not checked.
    let old_hole = drop_weekdays_from(&etf_eleven(), "EFA", d(2019, 8, 5), 10);
    assert!(decide_etf_trend(&old_hole, etf_d(), &opts).is_ok());
    // ... and the gap policy can be switched off for replaying data with known holes.
    let mut lenient = opts.clone();
    lenient.gap_policy = GapPolicy::Unchecked;
    assert!(decide_etf_trend(&four, etf_d(), &lenient).is_ok());
}

// ------------------------------------------------------------------ crypto refusals

fn crypto_panel(n: usize) -> Panel {
    let end = d(2020, 6, 30);
    Panel::new(
        CRYPTO_SYMBOLS
            .iter()
            .map(|s| crypto_series(s, end, &vec![100.0; n]))
            .collect(),
    )
    .unwrap()
}

#[test]
fn crypto_missing_bar_on_the_decision_date_is_refused() {
    let panel = replace_series(&crypto_panel(150), "ETH", |s| {
        without_dates(s, |x| x == d(2020, 6, 30))
    });
    let err = decide_crypto_trend(&panel, d(2020, 6, 30), &Options::crypto_replay()).unwrap_err();
    assert_eq!(
        err,
        RuleError::DateNotInPanel {
            symbol: "ETH".into(),
            date: d(2020, 6, 30)
        }
    );
}

#[test]
fn crypto_gap_inside_the_window_is_refused_and_outside_is_not() {
    let opts = Options::crypto_replay();
    // 150 bars ending 2020-06-30: the window starts 2020-03-23.
    let inside = replace_series(&crypto_panel(150), "BTC", |s| {
        without_dates(s, |x| x == d(2020, 4, 10))
    });
    let err = decide_crypto_trend(&inside, d(2020, 6, 30), &opts).unwrap_err();
    assert!(
        matches!(&err, RuleError::DataGap { symbol, from, to, missing: 1, tolerance: 0, weekdays: false }
            if symbol == "BTC" && *from == d(2020, 4, 9) && *to == d(2020, 4, 11)),
        "{err:?}"
    );
    let before_window = replace_series(&crypto_panel(150), "BTC", |s| {
        without_dates(s, |x| x == d(2020, 3, 22))
    });
    assert!(decide_crypto_trend(&before_window, d(2020, 6, 30), &opts).is_ok());
    let first_in_window = replace_series(&crypto_panel(150), "BTC", |s| {
        without_dates(s, |x| x == d(2020, 3, 23))
    });
    assert!(
        decide_crypto_trend(&first_in_window, d(2020, 6, 30), &opts).is_err(),
        "removing a bar shifts the window back over older bars"
    );
}

#[test]
fn crypto_live_options_forming_stale_and_panel_end() {
    let panel = crypto_panel(150); // newest bar 2020-06-30
    let last = d(2020, 6, 30);
    assert!(decide_crypto_trend(&panel, last, &Options::crypto_live(d(2020, 7, 1))).is_ok());
    let err = decide_crypto_trend(&panel, last, &Options::crypto_live(d(2020, 7, 2))).unwrap_err();
    assert!(
        matches!(
            err,
            RuleError::StaleData {
                max_stale_days: 1,
                ..
            }
        ),
        "{err:?}"
    );
    let err = decide_crypto_trend(&panel, last, &Options::crypto_live(last)).unwrap_err();
    assert!(matches!(err, RuleError::FormingBar { .. }), "{err:?}");
    // Deciding an older day on a longer panel: refused live, allowed in replay.
    let err = decide_crypto_trend(
        &panel,
        last - Duration::days(1),
        &Options::crypto_live(d(2020, 7, 1)),
    )
    .unwrap_err();
    assert!(
        matches!(err, RuleError::PanelDoesNotEndOnDecisionDate { last_bar, .. } if last_bar == last),
        "{err:?}"
    );
    assert!(
        decide_crypto_trend(&panel, last - Duration::days(1), &Options::crypto_replay()).is_ok()
    );
}

#[test]
fn crypto_decision_date_before_enough_history_is_refused() {
    let panel = crypto_panel(150);
    let day_99 = d(2020, 6, 30) - Duration::days(150 - 99);
    let err = decide_crypto_trend(&panel, day_99, &Options::crypto_replay()).unwrap_err();
    assert!(
        matches!(
            err,
            RuleError::InsufficientHistory {
                needed: 100,
                have: 99,
                ..
            }
        ),
        "{err:?}"
    );
    let day_100 = day_99 + Duration::days(1);
    assert!(decide_crypto_trend(&panel, day_100, &Options::crypto_replay()).is_ok());
    let err = decide_crypto_trend(&panel, d(2019, 1, 1), &Options::crypto_replay()).unwrap_err();
    assert!(matches!(err, RuleError::DateNotInPanel { .. }), "{err:?}");
}

#[test]
fn month_end_helpers_refuse_empty_and_disagreeing_panels() {
    // Data inside one single month: no completed month.
    let s = |sym: &str| {
        PriceSeries::new(sym, vec![d(2020, 1, 2), d(2020, 1, 3)], vec![1.0, 2.0]).unwrap()
    };
    let panel = Panel::new(vec![s("SPY")]).unwrap();
    assert!(matches!(
        latest_decision_date(&panel, &["SPY"]),
        Err(RuleError::NoCompletedMonth { .. })
    ));
    // Instruments disagree on the latest completed month-end.
    let a = PriceSeries::new("A", vec![d(2020, 1, 30), d(2020, 2, 3)], vec![1.0, 2.0]).unwrap();
    let b = PriceSeries::new("B", vec![d(2020, 1, 31), d(2020, 2, 3)], vec![1.0, 2.0]).unwrap();
    let panel = Panel::new(vec![a, b]).unwrap();
    assert!(matches!(
        latest_decision_date(&panel, &["A", "B"]),
        Err(RuleError::MonthEndMismatch { .. })
    ));
}

#[test]
fn weekday_and_day_gap_counting() {
    use reference_rules::months::{missing_days_between, missing_weekdays_between};
    // Fri -> Mon: nothing missing on weekdays, two calendar days.
    assert_eq!(missing_weekdays_between(d(2020, 3, 6), d(2020, 3, 9)), 0);
    assert_eq!(missing_days_between(d(2020, 3, 6), d(2020, 3, 9)), 2);
    // Fri -> Tue: Monday missing.
    assert_eq!(missing_weekdays_between(d(2020, 3, 6), d(2020, 3, 10)), 1);
    // Fri -> next Fri: Mon-Thu missing.
    assert_eq!(missing_weekdays_between(d(2020, 3, 6), d(2020, 3, 13)), 4);
    // Long gaps: 5 weekdays per full week.
    assert_eq!(missing_weekdays_between(d(2020, 3, 6), d(2020, 4, 3)), 19);
    assert_eq!(missing_weekdays_between(d(2020, 3, 6), d(2020, 3, 7)), 0);
    assert_eq!(missing_weekdays_between(d(2020, 3, 6), d(2020, 3, 6)), 0);
}
