//! Boundary cases of the two rules: ties, SMA inclusion of the current bar, window lengths, weights,
//! month-end rule, fingerprint format.

mod common;

use common::*;
use reference_rules::*;
use sha2::{Digest, Sha256};

const START_Y: i32 = 2019;
const START_M: u32 = 1;

/// Five ETFs with month-end closes given per symbol (same number of months, same dates).
fn etf_panel_from(closes: [&[f64]; 5]) -> Panel {
    Panel::new(
        ETF_SYMBOLS
            .iter()
            .zip(closes)
            .map(|(s, c)| etf_series(s, START_Y, START_M, c, 1000.0))
            .collect(),
    )
    .unwrap()
}

/// Same month-end closes for all five ETFs.
fn etf_panel_same(closes: &[f64]) -> Panel {
    etf_panel_from([closes; 5])
}

/// Last weekday of the n-th month (1-based) counted from the start month.
fn month_end_of(n: u32) -> chrono::NaiveDate {
    let idx = START_M - 1 + n - 1;
    last_weekday(START_Y + (idx / 12) as i32, idx % 12 + 1)
}

fn explicit() -> Options {
    Options::etf_replay(MonthEndMode::Explicit)
}

fn crypto_panel_same(closes: &[f64]) -> Panel {
    let end = d(2020, 6, 30);
    Panel::new(
        CRYPTO_SYMBOLS
            .iter()
            .map(|s| crypto_series(s, end, closes))
            .collect(),
    )
    .unwrap()
}

fn with_cash_stub(mut v: Vec<f64>, last: f64) -> Vec<f64> {
    v.push(last);
    v
}

// ------------------------------------------------------------------ ETF: ties, inclusion, window length

#[test]
fn etf_close_equal_to_sma_is_not_above() {
    let panel = etf_panel_same(&[100.0; 10]);
    let dec = decide_etf_trend(&panel, month_end_of(10), &explicit()).unwrap();
    for i in &dec.instruments {
        assert_eq!(i.signal, Signal::Cash, "{}", i.symbol);
        assert_eq!(i.weight, 0.0);
        assert_eq!(i.sma, 100.0);
    }
    assert_eq!(dec.cash_weight(), 1.0);
}

#[test]
fn etf_tie_survives_binary_rounding() {
    // Ten closes of 0.1: the naive f64 mean is 0.09999999999999999 < 0.1, which would wrongly read "above".
    let panel = etf_panel_same(&[0.1; 10]);
    let dec = decide_etf_trend(&panel, month_end_of(10), &explicit()).unwrap();
    assert!(dec.instruments.iter().all(|i| i.signal == Signal::Cash));
}

#[test]
fn etf_smallest_step_above_the_average_is_long() {
    let panel = etf_panel_same(&with_cash_stub(vec![100.0; 9], 100.01));
    let dec = decide_etf_trend(&panel, month_end_of(10), &explicit()).unwrap();
    assert!(dec
        .instruments
        .iter()
        .all(|i| i.signal == Signal::Long && i.weight == 0.2));
    assert!((dec.instruments[0].sma - 100.001).abs() < 1e-9);
    let below = etf_panel_same(&with_cash_stub(vec![100.0; 9], 99.99));
    let dec = decide_etf_trend(&below, month_end_of(10), &explicit()).unwrap();
    assert!(dec.instruments.iter().all(|i| i.signal == Signal::Cash));
}

#[test]
fn etf_sma_includes_the_current_month_end() {
    // 11 month-ends: 200, 100 x9, 105. Including the current bar the window is the last 10 (100 x9, 105):
    // sma 100.5, 105 > 100.5 -> Long. Excluding it (the previous 10 = 200 + 100 x9) the sma would be 110 -> Cash.
    let mut v = vec![200.0];
    v.extend([100.0; 9]);
    v.push(105.0);
    let dec = decide_etf_trend(&etf_panel_same(&v), month_end_of(11), &explicit()).unwrap();
    for i in &dec.instruments {
        assert_eq!(i.signal, Signal::Long);
        assert_eq!(i.sma, 100.5);
        assert_eq!(i.close, 105.0);
    }
}

#[test]
fn etf_window_is_exactly_ten_month_ends() {
    // 12 month-ends: m0=m1=1000 (out of every reading), m2=10, m3..m10=100 (8 values), m11=99.5.
    //   window 10 (m2..m11): mean 90.95      -> 99.5 above -> Long   (the only correct reading)
    //   window  9 (m3..m11): mean ~99.94     -> Cash
    //   window 11 (m1..m11): mean ~190       -> Cash
    //   previous 10, current excluded (m1..m10): mean 181 -> Cash
    let mut v = vec![1000.0, 1000.0, 10.0];
    v.extend([100.0; 8]);
    v.push(99.5);
    assert_eq!(v.len(), 12);
    let dec = decide_etf_trend(&etf_panel_same(&v), month_end_of(12), &explicit()).unwrap();
    assert!(dec.instruments.iter().all(|i| i.signal == Signal::Long));
    assert!((dec.instruments[0].sma - 90.95).abs() < 1e-9);
    assert_eq!(dec.month_end_dates.len(), 10);
    assert_eq!(dec.month_end_dates[0], month_end_of(3));
    assert_eq!(*dec.month_end_dates.last().unwrap(), month_end_of(12));
}

#[test]
fn etf_exactly_ten_month_ends_is_enough_and_nine_is_not() {
    let ok = decide_etf_trend(&etf_panel_same(&[100.0; 10]), month_end_of(10), &explicit());
    assert!(ok.is_ok());
    let err =
        decide_etf_trend(&etf_panel_same(&[100.0; 9]), month_end_of(9), &explicit()).unwrap_err();
    assert!(
        matches!(
            err,
            RuleError::InsufficientHistory {
                needed: 10,
                have: 9,
                ..
            }
        ),
        "{err:?}"
    );
}

#[test]
fn etf_weight_is_twenty_percent_of_the_sleeve_per_long_instrument() {
    // SPY, IEF long; EFA, DBC, VNQ cash.
    let up = with_cash_stub(vec![100.0; 9], 110.0);
    let down = with_cash_stub(vec![100.0; 9], 90.0);
    let panel = etf_panel_from([&up, &down, &up, &down, &down]);
    let dec = decide_etf_trend(&panel, month_end_of(10), &explicit()).unwrap();
    let sig: Vec<_> = dec
        .instruments
        .iter()
        .map(|i| (i.symbol.as_str(), i.signal.as_int(), i.weight))
        .collect();
    assert_eq!(
        sig,
        vec![
            ("SPY", 1, 0.2),
            ("EFA", 0, 0.0),
            ("IEF", 1, 0.2),
            ("DBC", 0, 0.0),
            ("VNQ", 0, 0.0)
        ]
    );
    assert!((dec.invested_weight() - 0.4).abs() < 1e-12);
    assert!((dec.cash_weight() - 0.6).abs() < 1e-12);
    let all_up = decide_etf_trend(&etf_panel_same(&up), month_end_of(10), &explicit()).unwrap();
    assert!((all_up.invested_weight() - 1.0).abs() < 1e-12);
}

// ------------------------------------------------------------------ ETF: month-end rule

#[test]
fn etf_mid_month_decision_date_is_refused() {
    // Reference decide_s1 would accept this (its month-end check is vacuous); this port does not.
    let panel = etf_panel_same(&[100.0; 11]);
    let mid = d(2019, 11, 15);
    for mode in [MonthEndMode::Explicit, MonthEndMode::NextMonthBar] {
        let err = decide_etf_trend(&panel, mid, &Options::etf_replay(mode)).unwrap_err();
        assert!(
            matches!(err, RuleError::NotMonthEnd { decision_date, .. } if decision_date == mid),
            "{err:?}"
        );
    }
}

#[test]
fn etf_explicit_mode_accepts_a_panel_ending_on_the_decision_date() {
    let panel = etf_panel_same(&[100.0; 10]);
    assert!(decide_etf_trend(&panel, month_end_of(10), &explicit()).is_ok());
}

#[test]
fn etf_next_month_bar_mode_needs_a_later_month_bar() {
    let ten = etf_panel_same(&[100.0; 10]);
    let err = decide_etf_trend(
        &ten,
        month_end_of(10),
        &Options::etf_replay(MonthEndMode::NextMonthBar),
    )
    .unwrap_err();
    assert!(matches!(err, RuleError::MonthNotComplete { .. }), "{err:?}");
    // With an 11th month present the 10th month-end is decidable, and equals the explicit-mode decision.
    let eleven = etf_panel_same(&[100.0; 11]);
    let a = decide_etf_trend(
        &eleven,
        month_end_of(10),
        &Options::etf_replay(MonthEndMode::NextMonthBar),
    )
    .unwrap();
    let b = decide_etf_trend(&eleven, month_end_of(10), &explicit()).unwrap();
    assert_eq!(a, b);
    // A single bar of the next month is enough.
    let one_bar = eleven.truncated_to(d(2019, 11, 1));
    assert!(decide_etf_trend(
        &one_bar,
        month_end_of(10),
        &Options::etf_replay(MonthEndMode::NextMonthBar)
    )
    .is_ok());
    assert_eq!(
        latest_decision_date(&one_bar, &ETF_SYMBOLS).unwrap(),
        month_end_of(10)
    );
    assert_eq!(
        latest_decision_date(&ten, &ETF_SYMBOLS).unwrap(),
        month_end_of(9)
    );
}

#[test]
fn etf_date_that_is_not_the_last_bar_of_its_month_is_refused_even_if_it_looks_like_a_month_end() {
    // Friday the 27th of Sep 2019 has later bars in the month (Mon 30th).
    let panel = etf_panel_same(&[100.0; 10]);
    let err = decide_etf_trend(&panel, d(2019, 9, 27), &explicit()).unwrap_err();
    assert!(
        matches!(err, RuleError::NotMonthEnd { later_bar_in_month, .. } if later_bar_in_month == d(2019, 9, 30)),
        "{err:?}"
    );
}

// ------------------------------------------------------------------ crypto: ties, inclusion, window length

#[test]
fn crypto_hundred_bars_is_enough_and_ninety_nine_is_not() {
    let end = d(2020, 6, 30);
    let ok = crypto_panel_same(&[100.0; 100]);
    assert!(decide_crypto_trend(&ok, end, &Options::crypto_replay()).is_ok());
    let short = crypto_panel_same(&[100.0; 99]);
    let err = decide_crypto_trend(&short, end, &Options::crypto_replay()).unwrap_err();
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
}

#[test]
fn crypto_close_equal_to_sma_is_not_above() {
    let end = d(2020, 6, 30);
    let dec = decide_crypto_trend(
        &crypto_panel_same(&[100.0; 100]),
        end,
        &Options::crypto_replay(),
    )
    .unwrap();
    assert!(dec
        .instruments
        .iter()
        .all(|i| i.signal == Signal::Cash && i.weight == 0.0 && i.sma == 100.0));
    let tie01 = decide_crypto_trend(
        &crypto_panel_same(&[0.1; 100]),
        end,
        &Options::crypto_replay(),
    )
    .unwrap();
    assert!(tie01.instruments.iter().all(|i| i.signal == Signal::Cash));
    let up = decide_crypto_trend(
        &crypto_panel_same(&with_cash_stub(vec![100.0; 99], 100.5)),
        end,
        &Options::crypto_replay(),
    )
    .unwrap();
    assert!(up
        .instruments
        .iter()
        .all(|i| i.signal == Signal::Long && i.weight == 0.5));
    assert!((up.instruments[0].sma - 100.005).abs() < 1e-9);
}

#[test]
fn crypto_window_is_exactly_100_bars_including_today() {
    // 101 bars: b0=1000 (outside), b1=10, b2..b99=100 (98 bars), b100=99.5.
    //   window 100 incl. today (b1..b100): mean 99.095 -> Long (only correct reading)
    //   window  99 (b2..b100): mean ~99.995 -> Cash;  window 101: Cash;  previous 100 excl. today (b0..b99): Cash
    let mut v = vec![1000.0, 10.0];
    v.extend([100.0; 98]);
    v.push(99.5);
    assert_eq!(v.len(), 101);
    let dec = decide_crypto_trend(
        &crypto_panel_same(&v),
        d(2020, 6, 30),
        &Options::crypto_replay(),
    )
    .unwrap();
    assert!(dec.instruments.iter().all(|i| i.signal == Signal::Long));
    assert!((dec.instruments[0].sma - 99.095).abs() < 1e-9);
    assert_eq!(
        dec.window_start[0],
        d(2020, 6, 30) - chrono::Duration::days(99)
    );
}

#[test]
fn crypto_weight_is_fifty_percent_per_coin() {
    let end = d(2020, 6, 30);
    let up = with_cash_stub(vec![100.0; 99], 120.0);
    let down = with_cash_stub(vec![100.0; 99], 80.0);
    let panel = Panel::new(vec![
        crypto_series("BTC", end, &up),
        crypto_series("ETH", end, &down),
    ])
    .unwrap();
    let dec = decide_crypto_trend(&panel, end, &Options::crypto_replay()).unwrap();
    assert_eq!(
        (
            dec.get("BTC").unwrap().weight,
            dec.get("ETH").unwrap().weight
        ),
        (0.5, 0.0)
    );
    assert_eq!(dec.cash_weight(), 0.5);
    let both =
        decide_crypto_trend(&crypto_panel_same(&up), end, &Options::crypto_replay()).unwrap();
    assert_eq!(both.invested_weight(), 1.0);
}

// ------------------------------------------------------------------ fingerprint

#[test]
fn fingerprint_matches_the_documented_canonical_form() {
    let s = PriceSeries::new("ABC", vec![d(2020, 1, 2)], vec![1.5]).unwrap();
    let panel = Panel::new(vec![s]).unwrap();
    let canonical = format!(
        "mendl-reference-rules-panel-v1\nS 3:ABC 1\n2020-01-02 {:016x}\n",
        1.5f64.to_bits()
    );
    assert_eq!(
        data_fingerprint(&panel),
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    assert_eq!(data_fingerprint(&panel).len(), 64);
}

#[test]
fn fingerprint_changes_with_any_bar_and_ignores_insertion_order() {
    let a = PriceSeries::new("A", vec![d(2020, 1, 2), d(2020, 1, 3)], vec![1.0, 2.0]).unwrap();
    let b = PriceSeries::new("B", vec![d(2020, 1, 2)], vec![3.0]).unwrap();
    let base = data_fingerprint(&Panel::new(vec![a.clone(), b.clone()]).unwrap());
    assert_eq!(
        base,
        data_fingerprint(&Panel::new(vec![b.clone(), a.clone()]).unwrap())
    );
    let ulp = PriceSeries::new(
        "A",
        a.dates().to_vec(),
        vec![1.0, f64::from_bits(2.0f64.to_bits() + 1)],
    )
    .unwrap();
    assert_ne!(
        base,
        data_fingerprint(&Panel::new(vec![ulp, b.clone()]).unwrap())
    );
    let dropped = PriceSeries::new("A", vec![d(2020, 1, 2)], vec![1.0]).unwrap();
    assert_ne!(
        base,
        data_fingerprint(&Panel::new(vec![dropped, b.clone()]).unwrap())
    );
    let shifted =
        PriceSeries::new("A", vec![d(2020, 1, 2), d(2020, 1, 6)], vec![1.0, 2.0]).unwrap();
    assert_ne!(
        base,
        data_fingerprint(&Panel::new(vec![shifted, b]).unwrap())
    );
}

// ---------------------------------------------------------------------------------------------------------------
// is_calendar_month_end: the clock-based scheduling helper, distinct from the data-driven month-end functions above
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn is_calendar_month_end_matches_the_wall_clock_not_any_data() {
    assert!(is_calendar_month_end(d(2026, 1, 31)));
    assert!(is_calendar_month_end(d(2026, 2, 28)), "2026 is not a leap year");
    assert!(is_calendar_month_end(d(2024, 2, 29)), "2024 is a leap year");
    assert!(is_calendar_month_end(d(2026, 4, 30)));
    assert!(is_calendar_month_end(d(2026, 12, 31)));
    assert!(!is_calendar_month_end(d(2026, 1, 30)));
    assert!(!is_calendar_month_end(d(2026, 3, 1)));
    assert!(!is_calendar_month_end(d(2026, 6, 15)));
}
