//! Golden tests for the FX momentum rule: replay `decide_fx_tsmom` and require agreement with what the REFERENCE
//! (`decide_s2` in stage1-record/tool/ticket.py) returned, per weight, sign and sigma. The golden files were
//! produced by running the reference (tests/data/gen_fx_golden.py), never by re-deriving the rule.
//! Run with `-- --nocapture` to see the observed maximum differences.
//!
//! Tolerances (stated, and far above the observed noise but far below any formula error):
//! * `sign`: exact.
//! * `weight`: absolute 1e-12 (weights are at most 3, so at most ~3e-13 relative). The port sums and takes the
//!   standard deviation in a different order than numpy/pandas (sequential two-pass vs pairwise/bottleneck), which
//!   moves the last one or two bits: the observed maximum is printed by the test and is about 1e-15, and two
//!   different numpy/pandas versions of the reference itself differ from each other by 8.9e-16 on the same data.
//!   A real formula difference (ddof 0 instead of 1 on 60 returns, ppy taken over another window, the cap applied
//!   before scaling) moves weights by 1e-4 or more, so 1e-12 cannot hide one.
//! * `sigma`: relative 1e-12, for the same reason.

mod common;
mod fx_common;

use std::collections::BTreeMap;

use chrono::NaiveDate;
use common::*;
use fx_common::*;
use reference_rules::*;

const LADDER_SHA: &str = "d5cea5f25a4d0d4ecdf4f3c48ba0bd8d50f72fe208a7a6b5ace1399bb644a365";
/// Pinned after generation with `gen_fx_golden.py` (see its header for the reference sha256 and versions).
const GOLDEN_SHA: &str = "7e44995640c18bcc2942ebd695ae2f23030cc825ee4da987440070d20f911d94";
const CLIP_CANDLES_SHA: &str = "fcf46910cf7d11c93d40e870563e5be6f9f32f53b264d10e722f41bdf43e03da";
const CLIP_GOLDEN_SHA: &str = "7799ab74b464bf588f0ccc18a8e9c35c1cf4aeea6508449f5aa6808ecbd1dc65";

const WEIGHT_ABS_TOL: f64 = 1e-12;
const SIGMA_REL_TOL: f64 = 1e-12;
const PPY_REL_TOL: f64 = 1e-13;

struct Row {
    variant: String,
    date: NaiveDate,
    history_start: NaiveDate,
    symbol: String,
    sign: i8,
    sigma: f64,
    weight: f64,
    ppy: f64,
}

/// (variant, date, history_start) -> the seven rows in `FX_SYMBOLS` order.
type Groups = BTreeMap<(String, NaiveDate, NaiveDate), Vec<Row>>;

fn parse_date(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

fn load_golden(name: &str) -> Groups {
    let text = read_data(name);
    let mut groups: Groups = BTreeMap::new();
    let mut header_seen = false;
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        if !header_seen {
            assert_eq!(
                line.trim(),
                "variant,date,history_start,symbol,sign,sigma,weight,ppy_aux"
            );
            header_seen = true;
            continue;
        }
        let f: Vec<&str> = line.trim().split(',').collect();
        let row = Row {
            variant: f[0].to_string(),
            date: parse_date(f[1]),
            history_start: parse_date(f[2]),
            symbol: f[3].to_string(),
            sign: f[4].parse().unwrap(),
            sigma: f[5].parse().unwrap(),
            weight: f[6].parse().unwrap(),
            ppy: f[7].parse().unwrap(),
        };
        groups
            .entry((row.variant.clone(), row.date, row.history_start))
            .or_default()
            .push(row);
    }
    for g in groups.values() {
        assert_eq!(
            g.iter().map(|r| r.symbol.as_str()).collect::<Vec<_>>(),
            FX_SYMBOLS.to_vec()
        );
    }
    groups
}

/// The clip case has no history_start / variant columns (whole file): reuse the group shape.
fn load_clip_golden() -> Groups {
    let text = read_data("golden_fx_clip.csv");
    let mut groups: Groups = BTreeMap::new();
    let mut header_seen = false;
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        if !header_seen {
            assert_eq!(line.trim(), "date,symbol,sign,sigma,weight,ppy_aux");
            header_seen = true;
            continue;
        }
        let f: Vec<&str> = line.trim().split(',').collect();
        let row = Row {
            variant: "clip".to_string(),
            date: parse_date(f[0]),
            history_start: d(2018, 1, 1),
            symbol: f[1].to_string(),
            sign: f[2].parse().unwrap(),
            sigma: f[3].parse().unwrap(),
            weight: f[4].parse().unwrap(),
            ppy: f[5].parse().unwrap(),
        };
        groups
            .entry((row.variant.clone(), row.date, row.history_start))
            .or_default()
            .push(row);
    }
    groups
}

fn load_candles(name: &str) -> Panel {
    let text = read_data(name);
    let mut by: BTreeMap<String, Vec<(NaiveDate, f64)>> = BTreeMap::new();
    for (i, line) in text.lines().enumerate() {
        if i == 0 {
            assert_eq!(line.trim(), "symbol,date_utc,close");
            continue;
        }
        let f: Vec<&str> = line.trim().split(',').collect();
        by.entry(f[0].to_string())
            .or_default()
            .push((parse_date(f[1]), f[2].parse().unwrap()));
    }
    panel_of(&by, &FX_SYMBOLS)
}

fn ladder_fx_panel() -> Panel {
    panel_of(&load_ladder(), &FX_SYMBOLS)
}

fn unchecked() -> Options {
    let mut o = Options::fx_replay(MonthEndMode::Explicit);
    o.gap_policy = GapPolicy::Unchecked;
    o
}

fn rel(a: f64, b: f64) -> f64 {
    (a - b).abs() / a.abs().max(b.abs()).max(f64::MIN_POSITIVE)
}

#[derive(Default)]
struct Stats {
    decisions: usize,
    weights: usize,
    max_w_abs: f64,
    max_sigma_rel: f64,
    max_ppy_rel: f64,
    clipped: usize,
}

fn compare_group(
    panel: &Panel,
    key: &(String, NaiveDate, NaiveDate),
    rows: &[Row],
    opts: &Options,
    st: &mut Stats,
) {
    let dec = decide_fx_tsmom(panel, key.1, key.2, opts)
        .unwrap_or_else(|e| panic!("{} {}: {e}", key.0, key.1));
    st.decisions += 1;
    st.max_ppy_rel = st.max_ppy_rel.max(rel(dec.ppy, rows[0].ppy));
    for (got, want) in dec.instruments.iter().zip(rows) {
        assert_eq!(got.symbol, want.symbol);
        assert_eq!(
            got.sign, want.sign,
            "{} {} {} sign",
            key.0, key.1, got.symbol
        );
        let wd = (got.weight - want.weight).abs();
        let sd = rel(got.sigma, want.sigma);
        assert!(
            wd <= WEIGHT_ABS_TOL,
            "{} {} {}: weight {} vs reference {} (diff {wd:e})",
            key.0,
            key.1,
            got.symbol,
            got.weight,
            want.weight
        );
        assert!(
            sd <= SIGMA_REL_TOL,
            "{} {} {}: sigma {} vs reference {} (rel {sd:e})",
            key.0,
            key.1,
            got.symbol,
            got.sigma,
            want.sigma
        );
        st.weights += 1;
        st.max_w_abs = st.max_w_abs.max(wd);
        st.max_sigma_rel = st.max_sigma_rel.max(sd);
        st.clipped += usize::from(got.clipped);
    }
    assert!(st.max_ppy_rel <= PPY_REL_TOL, "{} {} ppy", key.0, key.1);
}

#[test]
fn golden_fx_inputs_are_the_recorded_files() {
    assert_eq!(sha256_of("ladder_candles.csv"), LADDER_SHA);
    assert_eq!(sha256_of("golden_fx_tsmom.csv"), GOLDEN_SHA);
    assert_eq!(sha256_of("fx_clip_candles.csv"), CLIP_CANDLES_SHA);
    assert_eq!(sha256_of("golden_fx_clip.csv"), CLIP_GOLDEN_SHA);
}

/// Every eligible month-end of the ladder history (120 dates, 2010-09-30 .. 2020-09-30), in both history windows
/// (the whole file, and the live ticket's 520-day lookback): 1680 weights, sign, sigma and ppy against the
/// reference. Uses reference semantics for gaps (`Unchecked`), because the ladder FX data has a 23-day joint hole
/// in Sept-Oct 2019 that the reference silently computes across.
#[test]
fn golden_fx_every_month_end_both_history_windows() {
    let panel = ladder_fx_panel();
    let golden = load_golden("golden_fx_tsmom.csv");
    let mut st = Stats::default();
    let opts = unchecked();
    for (key, rows) in &golden {
        compare_group(&panel, key, rows, &opts, &mut st);
    }
    println!(
        "FX momentum golden: {} decisions, {} weights; max |weight diff| {:e}, max sigma rel diff {:e}, max ppy rel diff {:e}; weights hitting the +-3 cap: {}",
        st.decisions, st.weights, st.max_w_abs, st.max_sigma_rel, st.max_ppy_rel, st.clipped
    );
    assert_eq!((st.decisions, st.weights), (240, 1680));
}

/// What the ladder golden covers: early, middle and late dates, months where signs flip, and NO weight at the
/// +-3 cap (the real history never reaches it; the cap is covered by the synthetic golden below).
#[test]
fn golden_fx_coverage() {
    let golden = load_golden("golden_fx_tsmom.csv");
    let full: Vec<_> = golden.iter().filter(|(k, _)| k.0 == "full").collect();
    let dates: Vec<NaiveDate> = full.iter().map(|(k, _)| k.1).collect();
    assert_eq!(dates.len(), 120);
    assert_eq!(dates[0], d(2010, 9, 30));
    assert_eq!(*dates.last().unwrap(), d(2020, 9, 30));
    for probe in [d(2012, 6, 29), d(2015, 6, 30), d(2018, 3, 30)] {
        assert!(dates.contains(&probe) || dates.iter().any(|x| (*x - probe).num_days().abs() <= 3));
    }
    let mut flips = 0;
    let mut sign_zero = 0;
    let mut max_abs_w = 0.0f64;
    for pair in full.windows(2) {
        let (a, b) = (pair[0].1, pair[1].1);
        if a.iter().zip(b.iter()).any(|(x, y)| x.sign != y.sign) {
            flips += 1;
        }
    }
    for (_, rows) in &full {
        for r in rows.iter() {
            sign_zero += usize::from(r.sign == 0);
            max_abs_w = max_abs_w.max(r.weight.abs());
        }
    }
    println!("ladder golden: {} dates, {flips} month-ends with at least one sign flip vs the previous month-end, {sign_zero} zero signs, max |weight| {max_abs_w}", dates.len());
    assert!(flips >= 20, "flips {flips}");
    assert!(
        max_abs_w < FX_WEIGHT_CAP,
        "the ladder history never reaches the cap"
    );
}

/// The cap: a synthetic panel (candles in tests/data, answers from the reference) where EURUSD and GBPUSD are so
/// quiet that `k * sign / sigma` exceeds 3 in absolute value. Weights must be exactly +-3 and match the reference
/// for every weight, including the unclipped ones the cap must not touch.
#[test]
fn golden_fx_weight_cap_binds_on_synthetic_panel() {
    let panel = load_candles("fx_clip_candles.csv");
    let golden = load_clip_golden();
    assert!(golden.len() >= 6);
    let mut st = Stats::default();
    let opts = explicit();
    for (key, rows) in &golden {
        compare_group(&panel, key, rows, &opts, &mut st);
        let dec = decide_fx_tsmom(&panel, key.1, key.2, &opts).unwrap();
        assert_eq!(dec.get("EURUSD").unwrap().weight, 3.0);
        assert_eq!(dec.get("GBPUSD").unwrap().weight, -3.0);
        assert!(dec.get("EURUSD").unwrap().clipped && dec.get("GBPUSD").unwrap().clipped);
        assert!(dec
            .instruments
            .iter()
            .skip(2)
            .all(|i| !i.clipped && i.weight.abs() < 3.0));
    }
    println!(
        "FX momentum cap golden: {} decisions, {} weights, {} at +-3; max |weight diff| {:e}",
        st.decisions, st.weights, st.clipped, st.max_w_abs
    );
    assert_eq!(st.clipped, 2 * golden.len());
}

/// The strict (production) gap policy on the joint calendar: a decision either agrees with the reference exactly
/// as under `Unchecked` (the gap policy only refuses, it never changes numbers) or is refused with a `DataGap`
/// on the joint calendar. The refused ones are exactly those whose decision window contains the 23-day hole
/// 2019-09-13 .. 2019-10-06.
#[test]
fn golden_fx_strict_gap_policy_refuses_only_the_2019_hole() {
    let panel = ladder_fx_panel();
    let golden = load_golden("golden_fx_tsmom.csv");
    let strict = Options::fx_replay(MonthEndMode::Explicit);
    let (mut agree, mut refused) = (0, Vec::new());
    for (key, _) in &golden {
        let loose = decide_fx_tsmom(&panel, key.1, key.2, &unchecked()).unwrap();
        match decide_fx_tsmom(&panel, key.1, key.2, &strict) {
            Ok(dec) => {
                assert_eq!(dec, loose, "{} {}", key.0, key.1);
                agree += 1;
            }
            Err(RuleError::DataGap {
                symbol,
                from,
                to,
                weekdays: true,
                ..
            }) => {
                assert_eq!(symbol, FX_JOINT_CALENDAR);
                assert_eq!(
                    (from, to),
                    (d(2019, 9, 13), d(2019, 10, 6)),
                    "{} {}",
                    key.0,
                    key.1
                );
                refused.push((key.0.clone(), key.1));
            }
            Err(e) => panic!("{} {}: {e}", key.0, key.1),
        }
    }
    println!(
        "strict gap policy: {agree} of {} decisions identical to the reference-semantics run; {} refused for the 2019 joint hole ({} .. {})",
        golden.len(),
        refused.len(),
        refused.iter().map(|r| r.1).min().unwrap(),
        refused.iter().map(|r| r.1).max().unwrap()
    );
    assert!(!refused.is_empty());
    assert!(refused
        .iter()
        .all(|r| r.1 >= d(2019, 10, 31) && r.1 <= d(2020, 9, 30)));
    assert_eq!(agree + refused.len(), golden.len());
}

/// Calendar-free month-end mode gives the same decisions as the explicit mode wherever a later-month bar exists
/// (it does for every eligible date here).
#[test]
fn golden_fx_next_month_bar_mode_agrees() {
    let panel = ladder_fx_panel();
    let golden = load_golden("golden_fx_tsmom.csv");
    let mut nmb = Options::fx_replay(MonthEndMode::NextMonthBar);
    nmb.gap_policy = GapPolicy::Unchecked;
    for (key, _) in &golden {
        assert_eq!(
            decide_fx_tsmom(&panel, key.1, key.2, &nmb).unwrap(),
            decide_fx_tsmom(&panel, key.1, key.2, &unchecked()).unwrap(),
            "{} {}",
            key.0,
            key.1
        );
    }
}

/// The reference's history-window quirk, made visible: the same decision date gives different `ppy`, sigma and
/// weights under the two windows, and both windows match the reference (they are separate goldens). The port
/// reproduces the quirk; it is the caller's explicit `history_start` that selects the window.
#[test]
fn golden_fx_ppy_window_quirk_is_real_and_reproduced() {
    let panel = ladder_fx_panel();
    let golden = load_golden("golden_fx_tsmom.csv");
    let mut max_gap = 0.0f64;
    let (mut checked, mut same_window, mut coincident) = (0, 0, 0);
    for ((_, date, start), rows) in golden.iter().filter(|(k, _)| k.0 == "full") {
        let lb_start = fx_history_start(*date, FX_REFERENCE_LOOKBACK_DAYS);
        let lb_rows = golden
            .get(&("lb520".to_string(), *date, lb_start))
            .unwrap_or_else(|| panic!("lb520 golden for {date} starts at {lb_start}"));
        let full = decide_fx_tsmom(&panel, *date, *start, &unchecked()).unwrap();
        let lb = decide_fx_tsmom(&panel, *date, lb_start, &unchecked()).unwrap();
        if lb_start <= full.first_joint_date {
            // The 520-day window reaches back before the file starts: both windows are the whole file.
            assert_eq!(full.instruments, lb.instruments, "{date}");
            same_window += 1;
            continue;
        }
        assert!(full.joint_bars > lb.joint_bars);
        if full.ppy == lb.ppy {
            // Different windows can still give the very same rows/days ratio (the reference agrees: its
            // ppy_aux, sigma and weights are identical for these dates too); then nothing differs.
            assert_eq!(full.instruments, lb.instruments, "{date}");
            coincident += 1;
            continue;
        }
        let gap = full
            .instruments
            .iter()
            .zip(&lb.instruments)
            .map(|(a, b)| (a.weight - b.weight).abs())
            .fold(0.0, f64::max);
        max_gap = max_gap.max(gap);
        assert!(gap > 1e-9, "{date}: windows must differ");
        // and the reference itself differs by the same amount
        let ref_gap = rows
            .iter()
            .zip(lb_rows)
            .map(|(a, b)| (a.weight - b.weight).abs())
            .fold(0.0, f64::max);
        assert!((gap - ref_gap).abs() <= 1e-11, "{date}: {gap} vs {ref_gap}");
        checked += 1;
    }
    println!("ppy-window quirk: {checked} dates where the windows differ (plus {same_window} early dates where the 520-day window covers the whole file, and {coincident} where different windows happen to give the identical ppy); the 520-day window moves some weight by up to {max_gap:.6} vs the whole-file window");
    assert_eq!(checked + same_window + coincident, 120);
    assert!(checked > 90);
    assert!(max_gap > 1e-3);
}

/// `history_start` earlier than every bar of the file is the same as starting at the first bar: only bars in the
/// window count, so the argument is a pure window, not a hidden dependence on the file's extent.
#[test]
fn golden_fx_history_start_before_the_file_changes_nothing() {
    let panel = ladder_fx_panel();
    let date = d(2015, 6, 30);
    let a = decide_fx_tsmom(&panel, date, d(2009, 9, 25), &unchecked()).unwrap();
    let b = decide_fx_tsmom(&panel, date, d(1990, 1, 1), &unchecked()).unwrap();
    assert_eq!(a.instruments, b.instruments);
    assert_eq!(a.first_joint_date, b.first_joint_date);
    assert_eq!(a.ppy, b.ppy);
    // The first joint bar of the file is 2009-09-27 (a Sunday-dated bar): starting there is again identical.
    let c = decide_fx_tsmom(&panel, date, a.first_joint_date, &unchecked()).unwrap();
    assert_eq!(a.instruments, c.instruments);
    // Whereas starting one bar later changes ppy and the weights: the quirk.
    let e = decide_fx_tsmom(
        &panel,
        date,
        a.first_joint_date + chrono::Duration::days(1),
        &unchecked(),
    )
    .unwrap();
    assert_ne!(a.ppy, e.ppy);
    assert!(
        a.dropped_bars > 0,
        "the ladder FX pairs do not share every date"
    );
}
