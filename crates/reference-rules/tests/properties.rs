//! Property tests with seeded randomness (SplitMix64; the seeds are fixed, so failures reproduce).

mod common;

use chrono::{Datelike, NaiveDate};
use common::*;
use reference_rules::*;

const CASES: u64 = 120;

/// Panel with the same dates for every symbol and independent random-walk closes.
fn random_panel(symbols: &[&str], dates: &[NaiveDate], rng: &mut Rng) -> Panel {
    Panel::new(
        symbols
            .iter()
            .map(|s| {
                PriceSeries::new(
                    *s,
                    dates.to_vec(),
                    random_walk(dates.len(), 50.0 + 100.0 * rng.unit(), 0.03, rng),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

fn map_closes(panel: &Panel, mut f: impl FnMut(&str, NaiveDate, f64) -> f64) -> Panel {
    Panel::new(
        panel
            .iter()
            .map(|s| {
                PriceSeries::new(
                    s.symbol(),
                    s.dates().to_vec(),
                    s.dates()
                        .iter()
                        .zip(s.closes())
                        .map(|(dt, c)| f(s.symbol(), *dt, *c))
                        .collect(),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

/// Drop every bar dated before `from` from every series.
fn cut_before(panel: &Panel, from: NaiveDate) -> Panel {
    Panel::new(
        panel
            .iter()
            .map(|s| {
                let start = s.dates().partition_point(|x| *x < from);
                PriceSeries::new(
                    s.symbol(),
                    s.dates()[start..].to_vec(),
                    s.closes()[start..].to_vec(),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

fn etf_case(seed: u64) -> (Panel, NaiveDate, Rng) {
    let mut rng = Rng(seed);
    let start = d(2010, 1, 1) + chrono::Duration::days(rng.below(3000) as i64);
    let n_days = 330 + rng.below(1200) as i64;
    let dates = weekday_dates(start, n_days, 0.03, &mut rng);
    let panel = random_panel(&ETF_SYMBOLS, &dates, &mut rng);
    let me = completed_month_end_dates(panel.get("SPY").unwrap());
    assert!(me.len() >= 10, "generator must give enough history");
    let pick = 9 + rng.below((me.len() - 9) as u64) as usize;
    (panel, me[pick], rng)
}

fn crypto_case(seed: u64) -> (Panel, NaiveDate, Rng) {
    let mut rng = Rng(seed);
    let start = d(2015, 1, 1) + chrono::Duration::days(rng.below(2000) as i64);
    let n_days = 100 + rng.below(500) as i64;
    let dates = calendar_dates(start, n_days);
    let panel = random_panel(&CRYPTO_SYMBOLS, &dates, &mut rng);
    let pick = 99 + rng.below((n_days - 99) as u64) as usize;
    (panel, dates[pick], rng)
}

// ------------------------------------------------------------------ older history never changes the decision

#[test]
fn etf_older_history_beyond_the_window_never_changes_the_decision() {
    for seed in 0..CASES {
        let (panel, date, _) = etf_case(1000 + seed);
        let opts = Options::etf_replay(MonthEndMode::Explicit);
        let full = decide_etf_trend(&panel, date, &opts).unwrap();
        // Keep everything from the first day of the month of the oldest month-end in the window.
        let oldest = full.month_end_dates[0];
        let cut = cut_before(
            &panel,
            NaiveDate::from_ymd_opt(oldest.year(), oldest.month(), 1).unwrap(),
        );
        assert_eq!(
            decide_etf_trend(&cut, date, &opts).unwrap(),
            full,
            "seed {seed}"
        );
        // Scrambling the pre-window prices (all older than the window's first month) changes nothing either.
        let scrambled = map_closes(&panel, |_, dt, c| {
            if dt < NaiveDate::from_ymd_opt(oldest.year(), oldest.month(), 1).unwrap() {
                c * 3.7 + 1.0
            } else {
                c
            }
        });
        assert_eq!(
            decide_etf_trend(&scrambled, date, &opts).unwrap(),
            full,
            "seed {seed}"
        );
    }
}

#[test]
fn crypto_older_history_beyond_the_window_never_changes_the_decision() {
    for seed in 0..CASES {
        let (panel, date, _) = crypto_case(2000 + seed);
        let opts = Options::crypto_replay();
        let full = decide_crypto_trend(&panel, date, &opts).unwrap();
        let cut = cut_before(&panel, full.window_start[0]);
        assert_eq!(
            decide_crypto_trend(&cut, date, &opts).unwrap(),
            full,
            "seed {seed}"
        );
        let scrambled = map_closes(&panel, |_, dt, c| {
            if dt < full.window_start[0] {
                c * 5.0 + 3.0
            } else {
                c
            }
        });
        assert_eq!(
            decide_crypto_trend(&scrambled, date, &opts).unwrap(),
            full,
            "seed {seed}"
        );
    }
}

// ------------------------------------------------------------------ truncation: a decision at t never depends on bars after t

#[test]
fn etf_decision_never_depends_on_bars_after_the_decision_date() {
    let mut future_tested = 0;
    for seed in 0..CASES {
        let (panel, date, mut rng) = etf_case(3000 + seed);
        let explicit = Options::etf_replay(MonthEndMode::Explicit);
        let base = decide_etf_trend(&panel, date, &explicit).unwrap();
        // 1. the panel physically truncated at t (this is also what a live run sees)
        assert_eq!(
            decide_etf_trend(&panel.truncated_to(date), date, &explicit).unwrap(),
            base,
            "seed {seed}"
        );
        // 2. every later bar replaced by garbage (same dates, so a next-month bar still exists)
        let junk = map_closes(&panel, |_, dt, c| {
            if dt > date {
                c * (0.1 + 10.0 * rng.unit())
            } else {
                c
            }
        });
        assert_eq!(
            decide_etf_trend(&junk, date, &explicit).unwrap(),
            base,
            "seed {seed}"
        );
        if panel.get("SPY").unwrap().last_date() > date {
            future_tested += 1;
            let nmb = Options::etf_replay(MonthEndMode::NextMonthBar);
            assert_eq!(
                decide_etf_trend(&junk, date, &nmb).unwrap(),
                base,
                "seed {seed}"
            );
            assert_eq!(
                decide_etf_trend(&panel, date, &nmb).unwrap(),
                base,
                "seed {seed}"
            );
        }
    }
    assert!(
        future_tested > CASES / 2,
        "the generator must usually leave later bars after the decision date"
    );
}

#[test]
fn crypto_decision_never_depends_on_bars_after_the_decision_date() {
    let mut future_tested = 0;
    for seed in 0..CASES {
        let (panel, date, mut rng) = crypto_case(4000 + seed);
        let opts = Options::crypto_replay();
        let base = decide_crypto_trend(&panel, date, &opts).unwrap();
        assert_eq!(
            decide_crypto_trend(&panel.truncated_to(date), date, &opts).unwrap(),
            base,
            "seed {seed}"
        );
        let junk = map_closes(&panel, |_, dt, c| {
            if dt > date {
                c * (0.1 + 10.0 * rng.unit())
            } else {
                c
            }
        });
        assert_eq!(
            decide_crypto_trend(&junk, date, &opts).unwrap(),
            base,
            "seed {seed}"
        );
        if panel.get("BTC").unwrap().last_date() > date {
            future_tested += 1;
        }
        // Live semantics on the truncated panel: newest bar = decision date, run date = the day after.
        let live = Options::crypto_live(date + chrono::Duration::days(1));
        assert_eq!(
            decide_crypto_trend(&panel.truncated_to(date), date, &live).unwrap(),
            base,
            "seed {seed}"
        );
    }
    assert!(future_tested > CASES / 2);
}

// ------------------------------------------------------------------ other invariants

#[test]
fn decisions_are_deterministic_and_independent_of_series_order() {
    for seed in 0..40 {
        let (panel, date, _) = crypto_case(5000 + seed);
        let mut rev: Vec<PriceSeries> = panel.iter().cloned().collect();
        rev.reverse();
        let reversed = Panel::new(rev).unwrap();
        let opts = Options::crypto_replay();
        let a = decide_crypto_trend(&panel, date, &opts).unwrap();
        assert_eq!(a, decide_crypto_trend(&panel, date, &opts).unwrap());
        assert_eq!(a, decide_crypto_trend(&reversed, date, &opts).unwrap());
        assert_eq!(data_fingerprint(&panel), data_fingerprint(&reversed));
        assert_eq!(data_fingerprint(&panel), data_fingerprint(&panel.clone()));

        let (epanel, edate, _) = etf_case(5500 + seed);
        let eopts = Options::etf_replay(MonthEndMode::Explicit);
        assert_eq!(
            decide_etf_trend(&epanel, edate, &eopts).unwrap(),
            decide_etf_trend(&epanel, edate, &eopts).unwrap()
        );
    }
}

#[test]
fn scaling_all_prices_by_a_power_of_two_changes_no_signal() {
    for seed in 0..CASES {
        let (panel, date, _) = crypto_case(6000 + seed);
        let scaled = map_closes(&panel, |_, _, c| c * 4.0);
        let a = decide_crypto_trend(&panel, date, &Options::crypto_replay()).unwrap();
        let b = decide_crypto_trend(&scaled, date, &Options::crypto_replay()).unwrap();
        for (x, y) in a.instruments.iter().zip(&b.instruments) {
            assert_eq!((x.signal, x.weight), (y.signal, y.weight), "seed {seed}");
            assert_eq!(x.sma * 4.0, y.sma);
        }
        let (panel, date, _) = etf_case(6500 + seed);
        let scaled = map_closes(&panel, |_, _, c| c * 0.25);
        let opts = Options::etf_replay(MonthEndMode::Explicit);
        let a = decide_etf_trend(&panel, date, &opts).unwrap();
        let b = decide_etf_trend(&scaled, date, &opts).unwrap();
        for (x, y) in a.instruments.iter().zip(&b.instruments) {
            assert_eq!((x.signal, x.weight), (y.signal, y.weight), "seed {seed}");
        }
    }
}

#[test]
fn raising_the_decision_close_never_turns_long_into_cash_and_weights_follow_signals() {
    let mut saw_long = 0;
    let mut saw_cash = 0;
    for seed in 0..CASES {
        let (panel, date, _) = crypto_case(7000 + seed);
        let base = decide_crypto_trend(&panel, date, &Options::crypto_replay()).unwrap();
        let raised = map_closes(&panel, |_, dt, c| if dt == date { c * 1.5 } else { c });
        let up = decide_crypto_trend(&raised, date, &Options::crypto_replay()).unwrap();
        for (x, y) in base.instruments.iter().zip(&up.instruments) {
            if x.signal == Signal::Long {
                assert_eq!(y.signal, Signal::Long, "seed {seed}");
            }
            assert_eq!(
                x.weight,
                if x.signal == Signal::Long {
                    CRYPTO_WEIGHT_PER_INSTRUMENT
                } else {
                    0.0
                }
            );
            saw_long += usize::from(x.signal == Signal::Long);
            saw_cash += usize::from(x.signal == Signal::Cash);
        }
        let (epanel, edate, _) = etf_case(7500 + seed);
        let dec =
            decide_etf_trend(&epanel, edate, &Options::etf_replay(MonthEndMode::Explicit)).unwrap();
        for i in &dec.instruments {
            assert_eq!(
                i.weight,
                if i.signal == Signal::Long {
                    ETF_WEIGHT_PER_INSTRUMENT
                } else {
                    0.0
                }
            );
            // The reported sma is the exact mean rounded once, so Long implies close >= sma and Cash implies close <= sma.
            match i.signal {
                Signal::Long => assert!(i.close >= i.sma),
                Signal::Cash => assert!(i.close <= i.sma),
            }
        }
    }
    assert!(
        saw_long > 20 && saw_cash > 20,
        "the random cases must exercise both outcomes ({saw_long} long, {saw_cash} cash)"
    );
}
