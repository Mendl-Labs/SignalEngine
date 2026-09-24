//! Every refusal path of the FX momentum rule returns its specific RuleError (see also fx_boundaries.rs for the
//! 13-month-end and 70-bar boundaries and the all-zero-sign sleeve).

mod common;
mod fx_common;

use chrono::{Duration, NaiveDate};
use common::*;
use fx_common::*;
use reference_rules::*;

const SEED: u64 = 77;

fn pos_of(case: &Case, date: NaiveDate) -> usize {
    case.dates.iter().position(|x| *x == date).unwrap()
}

fn decide_case(case: &Case, panel: &Panel, opts: &Options) -> Result<FxTsmomDecision, RuleError> {
    decide_fx_tsmom(panel, case.decision, case.history_start, opts)
}

/// Remove the bars of `dates` from one pair.
fn without(panel: &Panel, symbol: &str, drop: &[NaiveDate]) -> Panel {
    let s = panel.get(symbol).unwrap();
    let (dates, closes): (Vec<_>, Vec<_>) = s
        .dates()
        .iter()
        .zip(s.closes())
        .filter(|(dt, _)| !drop.contains(dt))
        .map(|(a, b)| (*a, *b))
        .unzip();
    with_series(panel, symbol, dates, closes)
}

/// Replace some closes of one pair.
fn with_closes(case: &Case, symbol: &str, edit: impl Fn(&mut Vec<f64>)) -> Panel {
    let k = FX_SYMBOLS.iter().position(|s| *s == symbol).unwrap();
    let mut closes = case.closes.clone();
    edit(&mut closes[k]);
    panel_same_dates(&case.dates, &closes)
}

// ------------------------------------------------------------------ instruments and dates

#[test]
fn missing_pair_is_refused() {
    let case = standard_case(SEED);
    let panel = Panel::new(
        case.panel
            .iter()
            .filter(|s| s.symbol() != "NZDUSD")
            .cloned()
            .collect(),
    )
    .unwrap();
    assert_eq!(
        decide_case(&case, &panel, &explicit()).unwrap_err(),
        RuleError::MissingInstrument {
            symbol: "NZDUSD".into()
        }
    );
}

#[test]
fn a_pair_without_a_bar_on_the_decision_date_is_refused() {
    let case = standard_case(SEED);
    let panel = without(&case.panel, "USDCAD", &[case.decision]);
    assert_eq!(
        decide_case(&case, &panel, &explicit()).unwrap_err(),
        RuleError::DateNotInPanel {
            symbol: "USDCAD".into(),
            date: case.decision
        }
    );
}

#[test]
fn a_date_that_is_not_the_last_bar_of_its_month_is_refused() {
    let case = standard_case(SEED);
    let mid = d(2019, 6, 27); // 06-28 is a later bar of the same month
    for opts in [explicit(), Options::fx_replay(MonthEndMode::NextMonthBar)] {
        let err = decide_fx_tsmom(&case.panel, mid, case.history_start, &opts).unwrap_err();
        assert_eq!(
            err,
            RuleError::NotMonthEnd {
                symbol: "EURUSD".into(),
                decision_date: mid,
                later_bar_in_month: case.decision
            }
        );
    }
    // Only ONE pair has a later bar in the month (a Saturday-dated bar): that pair is named.
    let s = case.panel.get("GBPUSD").unwrap();
    let mut dates = s.dates().to_vec();
    let mut closes = s.closes().to_vec();
    let sat = d(2019, 6, 29);
    let at = dates.partition_point(|x| *x < sat);
    dates.insert(at, sat);
    closes.insert(at, 1.3);
    let panel = with_series(&case.panel, "GBPUSD", dates, closes);
    assert_eq!(
        decide_case(&case, &panel, &explicit()).unwrap_err(),
        RuleError::NotMonthEnd {
            symbol: "GBPUSD".into(),
            decision_date: case.decision,
            later_bar_in_month: sat
        }
    );
}

#[test]
fn month_not_complete_forming_stale_and_panel_end_are_refused() {
    let case = standard_case(SEED);
    let cut = case.panel.truncated_to(case.decision);
    // next-month-bar mode with no later-month bar
    assert_eq!(
        decide_fx_tsmom(
            &cut,
            case.decision,
            case.history_start,
            &Options::fx_replay(MonthEndMode::NextMonthBar)
        )
        .unwrap_err(),
        RuleError::MonthNotComplete {
            symbol: "EURUSD".into(),
            decision_date: case.decision
        }
    );
    // the panel must end on the decision date when asked
    let mut strict = explicit();
    strict.require_panel_end_on_decision_date = true;
    assert!(matches!(
        decide_case(&case, &case.panel, &strict),
        Err(RuleError::PanelDoesNotEndOnDecisionDate { .. })
    ));
    assert!(decide_case(&case, &cut, &strict).is_ok());

    let last = case.panel.get("EURUSD").unwrap().last_date(); // 2019-07-05
                                                              // live: a fresh panel (as_of a weekend + a Monday after the last bar) is accepted
    assert!(decide_case(
        &case,
        &case.panel,
        &Options::fx_live(last + Duration::days(3))
    )
    .is_ok());
    // a bar dated on or after as_of is a forming bar
    assert!(matches!(
        decide_case(&case, &case.panel, &Options::fx_live(last)),
        Err(RuleError::FormingBar { .. })
    ));
    // the newest bar is more than 5 days older than as_of
    assert!(matches!(
        decide_case(
            &case,
            &case.panel,
            &Options::fx_live(last + Duration::days(6))
        ),
        Err(RuleError::StaleData {
            max_stale_days: 5,
            ..
        })
    ));
}

#[test]
fn history_window_that_leaves_too_little_is_refused() {
    let case = standard_case(SEED);
    // start after the decision date: no bars at all
    assert_eq!(
        decide_fx_tsmom(
            &case.panel,
            case.decision,
            case.decision + Duration::days(1),
            &explicit()
        )
        .unwrap_err(),
        RuleError::InsufficientHistory {
            symbol: FX_JOINT_CALENDAR.into(),
            needed: 13,
            have: 0
        }
    );
    // start on the decision date: one bar, one month-end
    assert_eq!(
        decide_fx_tsmom(&case.panel, case.decision, case.decision, &explicit()).unwrap_err(),
        RuleError::InsufficientHistory {
            symbol: FX_JOINT_CALENDAR.into(),
            needed: 13,
            have: 1
        }
    );
}

// ------------------------------------------------------------------ data gaps on the joint calendar

#[test]
fn a_hole_in_the_decision_window_is_refused_and_three_missing_weekdays_are_tolerated() {
    let case = standard_case(SEED);
    // Remove k consecutive weekdays from ONE pair inside the last 60 bars: the joint calendar loses them all.
    let hole = |k: usize| -> Vec<NaiveDate> {
        let pos = pos_of(&case, case.decision);
        case.dates[pos - 30..pos - 30 + k].to_vec()
    };
    for k in 1..=3 {
        let panel = without(&case.panel, "AUDUSD", &hole(k));
        let dec = decide_case(&case, &panel, &explicit()).unwrap_or_else(|e| panic!("k={k}: {e}"));
        assert_eq!(dec.dropped_bars, k);
    }
    let panel = without(&case.panel, "AUDUSD", &hole(4));
    let err = decide_case(&case, &panel, &explicit()).unwrap_err();
    let h = hole(4);
    match err {
        RuleError::DataGap {
            symbol,
            from,
            to,
            missing,
            tolerance,
            weekdays,
        } => {
            assert_eq!(symbol, FX_JOINT_CALENDAR);
            assert_eq!(missing, 4);
            assert_eq!(tolerance, 3);
            assert!(weekdays);
            assert_eq!(from, case.dates[pos_of(&case, h[0]) - 1]);
            assert_eq!(to, case.dates[pos_of(&case, h[3]) + 1]);
        }
        other => panic!("{other:?}"),
    }
    // reference semantics (gap policy off) computes across the hole, like the reference does
    let mut off = explicit();
    off.gap_policy = GapPolicy::Unchecked;
    let dec = decide_case(&case, &panel, &off).unwrap();
    assert_eq!(dec.dropped_bars, 4);
}

#[test]
fn a_hole_older_than_the_decision_window_does_not_refuse_but_moves_ppy() {
    let case = standard_case(SEED);
    let base = decide_case(&case, &case.panel, &explicit()).unwrap();
    // Ten weekdays of February 2018 are missing from one pair: older than the oldest month-end used (Jun 2018)
    // and than the 61 bars behind the 60 returns. Only ppy sees it.
    let drop: Vec<NaiveDate> = weekdays(d(2018, 2, 5), d(2018, 2, 16));
    assert_eq!(drop.len(), 10);
    let panel = without(&case.panel, "EURUSD", &drop);
    let dec = decide_case(&case, &panel, &explicit()).unwrap();
    assert_eq!(dec.dropped_bars, 10);
    assert_ne!(dec.ppy, base.ppy, "fewer joint bars over the same span");
    assert_eq!(dec.joint_bars, base.joint_bars - 10);
    assert_eq!(dec.month_end_dates, base.month_end_dates);
}

// ------------------------------------------------------------------ degenerate volatility and non-finite values

#[test]
fn a_pair_with_no_volatility_in_the_window_is_refused() {
    let case = standard_case(SEED);
    let pos = pos_of(&case, case.decision);
    // constant price over the last 61 bars: every return is exactly 0
    let flat = with_closes(&case, "USDCHF", |c| {
        let v = c[pos - 61];
        for x in &mut c[pos - 61..=pos] {
            *x = v;
        }
    });
    assert_eq!(
        decide_case(&case, &flat, &explicit()).unwrap_err(),
        RuleError::ZeroVolatility {
            symbol: "USDCHF".into()
        }
    );
    // geometric growth at a constant rate: returns are equal up to rounding noise, which is not volatility
    let geometric = with_closes(&case, "USDCAD", |c| {
        let mut v = c[pos - 61];
        for x in &mut c[pos - 61..=pos] {
            *x = v;
            v *= 1.0005;
        }
    });
    assert_eq!(
        decide_case(&case, &geometric, &explicit()).unwrap_err(),
        RuleError::ZeroVolatility {
            symbol: "USDCAD".into()
        }
    );
    // a single non-flat return is enough volatility to proceed
    let almost = with_closes(&case, "USDCHF", |c| {
        let v = c[pos - 61];
        for x in &mut c[pos - 61..=pos] {
            *x = v;
        }
        c[pos] = v * 1.001;
    });
    assert!(decide_case(&case, &almost, &explicit()).is_ok());
}

#[test]
fn an_overflowing_daily_return_is_refused_as_non_finite() {
    let case = standard_case(SEED);
    let pos = pos_of(&case, case.decision);
    let panel = with_closes(&case, "EURUSD", |c| {
        c[pos - 1] = 1e-10;
        c[pos] = 1e300; // ratio 1e310 overflows f64
    });
    assert_eq!(
        decide_case(&case, &panel, &explicit()).unwrap_err(),
        RuleError::NonFiniteValue {
            symbol: "EURUSD".into(),
            what: "daily return"
        }
    );
}

#[test]
fn non_finite_and_non_positive_closes_never_reach_the_rule() {
    let case = standard_case(SEED);
    let s = case.panel.get("EURUSD").unwrap();
    for bad in [f64::NAN, f64::INFINITY, 0.0, -1.0] {
        let mut closes = s.closes().to_vec();
        closes[100] = bad;
        assert!(matches!(
            PriceSeries::new("EURUSD", s.dates().to_vec(), closes),
            Err(RuleError::InvalidPrice { .. })
        ));
    }
}
