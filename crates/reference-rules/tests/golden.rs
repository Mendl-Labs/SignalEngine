//! Golden tests: replay the Rust rules over the ladder candles and require 100% agreement with the shadow
//! signal files, the way stage1-record/tool/validate.py does. The shadow files are used ONLY as expected output.
//! Run with `-- --nocapture` to see the agreement counts.

mod common;

use common::*;
use reference_rules::*;

const LADDER_SHA: &str = "d5cea5f25a4d0d4ecdf4f3c48ba0bd8d50f72fe208a7a6b5ace1399bb644a365";
const S1_SHA: &str = "81717083558de340c200e9fd55b10ce4b6b14e214ca5269530f24317f98476ab";
const S3_SHA: &str = "ced88d667dfe71347a41dc30f9a8a52400c34965c37fe7b353fa036525397035";

#[test]
fn golden_inputs_are_the_recorded_files() {
    assert_eq!(sha256_of("ladder_candles.csv"), LADDER_SHA);
    assert_eq!(sha256_of("shadow_S1_monthend_signals.csv"), S1_SHA);
    assert_eq!(sha256_of("shadow_S3_daily_signals.csv"), S3_SHA);
}

fn etf_panel() -> Panel {
    panel_of(&load_ladder(), &ETF_SYMBOLS)
}

fn crypto_panel() -> Panel {
    panel_of(&load_ladder(), &CRYPTO_SYMBOLS)
}

/// Mirror of validate.py's S1 check, explicit month-end mode. validate.py: 555/555 over 111 month-ends, none skipped.
#[test]
fn golden_etf_every_month_end_explicit_mode() {
    let panel = etf_panel();
    let (header, rows) = load_shadow("shadow_S1_monthend_signals.csv");
    assert_eq!(header, ETF_SYMBOLS.to_vec());
    let opts = Options::etf_replay(MonthEndMode::Explicit);
    let (mut hit, mut tot) = (0, 0);
    let mut skipped = Vec::new();
    for (date, expected) in &rows {
        match decide_etf_trend(&panel, *date, &opts) {
            Ok(dec) => {
                for (sym, exp) in ETF_SYMBOLS.iter().zip(expected) {
                    let got = dec.get(sym).unwrap();
                    hit += usize::from(got.signal.as_int() == *exp);
                    tot += 1;
                    assert_eq!(got.weight, 0.2 * f64::from(*exp), "{date} {sym}");
                }
            }
            Err(e) => skipped.push((*date, e)),
        }
    }
    println!("S1 ETF trend (explicit mode): {hit}/{tot} signals agree over {} month-ends; skipped: {skipped:?}", rows.len() - skipped.len());
    assert!(skipped.is_empty(), "skipped: {skipped:?}");
    assert_eq!((hit, tot, rows.len()), (555, 555, 111));
}

/// Same, calendar-free mode: every month-end that has a later-month bar agrees; the final month-end (no
/// September bar in the data) is refused with MonthNotComplete rather than decided.
#[test]
fn golden_etf_next_month_bar_mode() {
    let panel = etf_panel();
    let (_, rows) = load_shadow("shadow_S1_monthend_signals.csv");
    let opts = Options::etf_replay(MonthEndMode::NextMonthBar);
    let (mut hit, mut tot, mut refused) = (0, 0, Vec::new());
    for (date, expected) in &rows {
        match decide_etf_trend(&panel, *date, &opts) {
            Ok(dec) => {
                for (sym, exp) in ETF_SYMBOLS.iter().zip(expected) {
                    hit += usize::from(dec.get(sym).unwrap().signal.as_int() == *exp);
                    tot += 1;
                }
            }
            Err(e) => refused.push((*date, e)),
        }
    }
    println!("S1 ETF trend (next-month-bar mode): {hit}/{tot} agree; refused: {refused:?}");
    assert_eq!((hit, tot), (550, 550));
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].0, d(2026, 8, 31));
    assert!(matches!(refused[0].1, RuleError::MonthNotComplete { .. }));
}

/// The month-end helper reproduces the shadow's list of month-end dates, and history before the first shadow
/// date is refused (fewer than 10 month-ends).
#[test]
fn golden_month_end_helper_and_warmup() {
    let panel = etf_panel();
    let (_, rows) = load_shadow("shadow_S1_monthend_signals.csv");
    for sym in ETF_SYMBOLS {
        let all = month_end_dates(panel.get(sym).unwrap());
        assert_eq!(all.len(), 120, "{sym}");
        let shadow_dates: Vec<_> = rows.iter().map(|r| r.0).collect();
        assert_eq!(&all[9..], &shadow_dates[..], "{sym}");
        assert_eq!(
            completed_month_end_dates(panel.get(sym).unwrap()),
            all[..119].to_vec()
        );
    }
    assert_eq!(
        latest_decision_date(&panel, &ETF_SYMBOLS).unwrap(),
        d(2026, 7, 31)
    );
    let opts = Options::etf_replay(MonthEndMode::Explicit);
    let all = month_end_dates(panel.get("SPY").unwrap());
    for (k, date) in all[..9].iter().enumerate() {
        match decide_etf_trend(&panel, *date, &opts) {
            Err(RuleError::InsufficientHistory {
                needed: 10, have, ..
            }) => assert_eq!(have, k + 1),
            other => panic!("{date}: {other:?}"),
        }
    }
}

/// Mirror of validate.py's S3 check with the production gap policy (no missing calendar day in the 100-bar
/// window). Dates refused for a data gap are listed, and must be a leading block of the shadow's history.
#[test]
fn golden_crypto_every_day_strict_gap_policy() {
    let panel = crypto_panel();
    let (header, rows) = load_shadow("shadow_S3_daily_signals.csv");
    assert_eq!(header, CRYPTO_SYMBOLS.to_vec());
    let opts = Options::crypto_replay();
    let (mut hit, mut tot) = (0usize, 0usize);
    let mut skipped = Vec::new();
    for (date, expected) in &rows {
        match decide_crypto_trend(&panel, *date, &opts) {
            Ok(dec) => {
                for (sym, exp) in CRYPTO_SYMBOLS.iter().zip(expected) {
                    let got = dec.get(sym).unwrap();
                    hit += usize::from(got.signal.as_int() == *exp);
                    tot += 1;
                    assert_eq!(got.weight, 0.5 * f64::from(*exp));
                }
            }
            Err(e) => skipped.push((*date, e)),
        }
    }
    let skipped_dates: Vec<_> = skipped.iter().map(|s| s.0).collect();
    println!(
        "S3 crypto trend (strict gaps): {hit}/{tot} agree over {} days; skipped {} days ({} .. {}): reason DataGap (missing calendar day inside the 100-bar window)",
        rows.len() - skipped.len(),
        skipped.len(),
        skipped_dates.first().map(|x| x.to_string()).unwrap_or_default(),
        skipped_dates.last().map(|x| x.to_string()).unwrap_or_default(),
    );
    assert_eq!(hit, tot);
    assert!(tot > 0);
    assert!(
        skipped
            .iter()
            .all(|s| matches!(s.1, RuleError::DataGap { .. })),
        "{skipped:?}"
    );
    // The skipped dates are exactly the leading days whose window reaches the 2015 holes in the source data.
    assert!(
        skipped_dates.iter().all(|x| *x < d(2016, 2, 1)),
        "{skipped_dates:?}"
    );
    assert!(skipped_dates
        .windows(2)
        .all(|w| (w[1] - w[0]).num_days() == 1));
    assert_eq!(skipped_dates[0], rows[0].0);
    assert_eq!(hit + 2 * skipped.len(), 2 * rows.len());
}

/// Pure-rule agreement exactly as validate.py evaluates it: joint (dropna) panel, no gap check. Every one of the
/// shadow's days is evaluated (validate.py skips none) and all agree.
#[test]
fn golden_crypto_every_day_reference_semantics() {
    let panel = inner_join(&crypto_panel());
    let (_, rows) = load_shadow("shadow_S3_daily_signals.csv");
    let mut opts = Options::crypto_replay();
    opts.gap_policy = GapPolicy::Unchecked;
    let (mut hit, mut tot) = (0, 0);
    for (date, expected) in &rows {
        let dec =
            decide_crypto_trend(&panel, *date, &opts).unwrap_or_else(|e| panic!("{date}: {e}"));
        for (sym, exp) in CRYPTO_SYMBOLS.iter().zip(expected) {
            hit += usize::from(dec.get(sym).unwrap().signal.as_int() == *exp);
            tot += 1;
        }
    }
    println!("S3 crypto trend (reference semantics: joint panel, gaps unchecked): {hit}/{tot} agree over {} days, none skipped", rows.len());
    assert_eq!((hit, tot, rows.len()), (3654, 3654, 1827));
}
