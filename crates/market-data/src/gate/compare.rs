//! The pure two-source comparison (COUNCIL_DATA_GATE R18.2-R18.4, R19): two panels in, a verdict with every reason
//! and every compared close out. No I/O, no clock. Symmetric in its two arguments by construction: every check runs
//! on both sides and every comparison date set is the UNION of what the two sources say (never "the primary's
//! dates"), so swapping the roles changes no verdict (R18 test 3, R21 test 2).
//!
//! What is compared (R18.2): ETF, the completed month-end closes in the rule's window (the last
//! `ETF_SMA_MONTH_ENDS` of each source, united), the decision date and the newest session; crypto, the last
//! `CRYPTO_SMA_DAYS` completed UTC closes, with yesterday's at its own (tighter) tolerance.
//!
//! Refusals (R18.3; the letters are the ruling's): (a) `REFUSE_MISSING_BAR`, (b) `REFUSE_DATE_MISMATCH`, (c)
//! `REFUSE_L1_OVER_TOLERANCE`, (d) `REFUSE_GAP`, (e) `REFUSE_SPLIT_ONE_SOURCE`, (h) `REFUSE_BAR_AT_OR_AFTER_AS_OF`;
//! (g) `REFUSE_SECONDARY_UNAVAILABLE` is raised by the decorator, not here. Flags: `FLAG_L1_OVER_FLAG`,
//! `FLAG_NEWEST_SESSION_DIFFERS`. NOT in this slice: (f) L2 decision-state agreement / KNIFE_EDGE_HOLD (needs the rule
//! run on the secondary's panel; shadow mode records L1 only) and the correction ledger (R18.6).

use std::collections::BTreeSet;

use chrono::{Duration, NaiveDate};
use rebalancer_run::data::{BarPosition, GateComparison, GateInstrument, GateReason, GateVerdict, SleeveKind};
use reference_rules::months::missing_weekdays_between;
use reference_rules::{completed_month_end_dates, latest_decision_date, Panel, PriceSeries, CRYPTO_SMA_DAYS, CRYPTO_SYMBOLS, ETF_SMA_MONTH_ENDS, ETF_SYMBOLS};

use super::policy::{diff_bps, Policy};

#[derive(Debug, Clone, PartialEq)]
pub struct Comparison {
    pub verdict: GateVerdict,
    pub reasons: Vec<GateReason>,
    pub instruments: Vec<GateInstrument>,
    pub primary_decision_date: Option<NaiveDate>,
    pub secondary_decision_date: Option<NaiveDate>,
}

#[derive(Clone, Copy)]
enum Side {
    Primary,
    Secondary,
}

impl Side {
    fn name(self) -> &'static str {
        match self {
            Side::Primary => "primary",
            Side::Secondary => "secondary",
        }
    }
}

fn reason(code: &str, verdict: GateVerdict, symbol: Option<&str>, date: Option<NaiveDate>, detail: String) -> GateReason {
    GateReason { code: code.to_string(), verdict, symbol: symbol.map(str::to_string), date, detail }
}

fn close_at(s: Option<&PriceSeries>, date: NaiveDate) -> Option<f64> {
    let s = s?;
    s.position_of(date).map(|i| s.closes()[i])
}

/// `r` is within `tol` (fraction) of an integer `n >= 2` or of `1/n`.
fn looks_like_split(r: f64, tol: f64) -> bool {
    if !r.is_finite() || r <= 0.0 {
        return false;
    }
    (2..=50).any(|n| {
        let n = n as f64;
        (r / n - 1.0).abs() <= tol || (r * n - 1.0).abs() <= tol
    })
}

/// Consecutive `(d1, d2)` pairs of a series inside `[from, to]`.
fn consecutive_in(s: &PriceSeries, from: NaiveDate, to: NaiveDate) -> Vec<(NaiveDate, NaiveDate, f64, f64)> {
    let dates = s.dates();
    let closes = s.closes();
    let mut out = Vec::new();
    for i in 1..dates.len() {
        if dates[i - 1] >= from && dates[i] <= to {
            out.push((dates[i - 1], dates[i], closes[i - 1], closes[i]));
        }
    }
    out
}

fn symbols(kind: SleeveKind) -> &'static [&'static str] {
    match kind {
        SleeveKind::EtfTrend => &ETF_SYMBOLS,
        SleeveKind::CryptoTrend => &CRYPTO_SYMBOLS,
    }
}

fn worst(reasons: &[GateReason]) -> GateVerdict {
    reasons.iter().map(|r| r.verdict).max().unwrap_or(GateVerdict::Pass)
}

/// The comparison dates of one instrument and the position of each.
fn comparison_dates(kind: SleeveKind, as_of: NaiveDate, p: Option<&PriceSeries>, s: Option<&PriceSeries>, p_dd: Option<NaiveDate>, s_dd: Option<NaiveDate>) -> Vec<(NaiveDate, BarPosition)> {
    match kind {
        SleeveKind::EtfTrend => {
            let mut dates: BTreeSet<NaiveDate> = BTreeSet::new();
            for (series, dd) in [(p, p_dd), (s, s_dd)] {
                let Some(series) = series else { continue };
                let mut ends = completed_month_end_dates(series);
                if let Some(dd) = dd {
                    ends.retain(|d| *d <= dd);
                }
                let keep = ends.len().saturating_sub(ETF_SMA_MONTH_ENDS);
                dates.extend(ends.into_iter().skip(keep));
            }
            dates.into_iter().map(|d| (d, BarPosition::MonthEnd)).collect()
        }
        SleeveKind::CryptoTrend => (1..=CRYPTO_SMA_DAYS as i64)
            .rev()
            .filter_map(|k| as_of.checked_sub_signed(Duration::days(k)))
            .map(|d| (d, if d == as_of - Duration::days(1) { BarPosition::DecisionDay } else { BarPosition::Window }))
            .collect(),
    }
}

/// Compare `primary` and `secondary` for `kind` as of `as_of` under `policy`. See the module docs.
pub fn compare(kind: SleeveKind, as_of: NaiveDate, primary: &Panel, secondary: &Panel, policy: &Policy) -> Comparison {
    let mut reasons: Vec<GateReason> = Vec::new();

    // (h) a bar dated on or after the run date, on either side
    for (side, panel) in [(Side::Primary, primary), (Side::Secondary, secondary)] {
        for s in panel.iter() {
            if s.last_date() >= as_of {
                reasons.push(reason(
                    "REFUSE_BAR_AT_OR_AFTER_AS_OF",
                    GateVerdict::Refuse,
                    Some(s.symbol()),
                    Some(s.last_date()),
                    format!("{} has a bar dated {} on or after the run date {as_of}", side.name(), s.last_date()),
                ));
            }
        }
    }

    // (b) decision dates
    let (p_dd, s_dd) = match kind {
        SleeveKind::EtfTrend => {
            let mut dd = [None, None];
            for (i, (side, panel)) in [(Side::Primary, primary), (Side::Secondary, secondary)].into_iter().enumerate() {
                match latest_decision_date(panel, &ETF_SYMBOLS) {
                    Ok(d) => dd[i] = Some(d),
                    Err(e) => reasons.push(reason("REFUSE_DATE_MISMATCH", GateVerdict::Refuse, None, None, format!("{}: no decision date can be computed: {e}", side.name()))),
                }
            }
            if let (Some(p), Some(s)) = (dd[0], dd[1]) {
                if p != s {
                    reasons.push(reason("REFUSE_DATE_MISMATCH", GateVerdict::Refuse, None, Some(p.max(s)), format!("the month-end decision dates differ: primary {p}, secondary {s}")));
                }
            }
            (dd[0], dd[1])
        }
        SleeveKind::CryptoTrend => {
            let d = as_of.pred_opt();
            (d, d)
        }
    };

    let mut instruments = Vec::new();
    for sym in symbols(kind) {
        let p = primary.get(sym).ok();
        let s = secondary.get(sym).ok();
        let mut mine: Vec<GateReason> = Vec::new();
        let mut comparisons = Vec::new();

        let dates = comparison_dates(kind, as_of, p, s, p_dd, s_dd);
        // (a) + (c): every comparison date on both sides, within tolerance
        for (date, position) in &dates {
            let pc = close_at(p, *date);
            let sc = close_at(s, *date);
            let mut diff = None;
            let verdict = match (pc, sc) {
                (None, None) => {
                    mine.push(reason("REFUSE_MISSING_BAR", GateVerdict::Refuse, Some(sym), Some(*date), format!("neither source has a bar dated {date}")));
                    GateVerdict::Refuse
                }
                (None, Some(_)) => {
                    mine.push(reason("REFUSE_MISSING_BAR", GateVerdict::Refuse, Some(sym), Some(*date), format!("primary has no bar dated {date} (secondary has one)")));
                    GateVerdict::Refuse
                }
                (Some(_), None) => {
                    mine.push(reason("REFUSE_MISSING_BAR", GateVerdict::Refuse, Some(sym), Some(*date), format!("secondary has no bar dated {date} (primary has one)")));
                    GateVerdict::Refuse
                }
                (Some(a), Some(b)) => {
                    let d = diff_bps(a, b);
                    diff = Some(d);
                    let tol = policy.tolerance(*position);
                    if d > tol.refuse_bps {
                        mine.push(reason(
                            "REFUSE_L1_OVER_TOLERANCE",
                            GateVerdict::Refuse,
                            Some(sym),
                            Some(*date),
                            format!("{} close differs by {d} bp (primary {a}, secondary {b}); refuse level {} bp", position.as_str(), tol.refuse_bps),
                        ));
                        GateVerdict::Refuse
                    } else if d > tol.flag_bps {
                        mine.push(reason(
                            "FLAG_L1_OVER_FLAG",
                            GateVerdict::Flag,
                            Some(sym),
                            Some(*date),
                            format!("{} close differs by {d} bp (primary {a}, secondary {b}); flag level {} bp", position.as_str(), tol.flag_bps),
                        ));
                        GateVerdict::Flag
                    } else {
                        GateVerdict::Pass
                    }
                }
            };
            comparisons.push(GateComparison { symbol: sym.to_string(), date: *date, position: *position, primary_close: pc, secondary_close: sc, diff_bps: diff, verdict });
        }

        // The window the gap and split checks run over: from the oldest comparison date to the newest bar either
        // side has (bounded by as_of - 1).
        let window_from = dates.first().map(|(d, _)| *d);
        let window_to = [p, s].into_iter().flatten().map(|x| x.last_date()).max().map(|d| d.min(as_of - Duration::days(1)));
        if let (Some(from), Some(to)) = (window_from, window_to) {
            // (d) gap guard (ETF; crypto's missing-bar check already covers every calendar day)
            if kind == SleeveKind::EtfTrend {
                for (side, series) in [(Side::Primary, p), (Side::Secondary, s)] {
                    let Some(series) = series else { continue };
                    for (d1, d2, _, _) in consecutive_in(series, from, to) {
                        let missing = missing_weekdays_between(d1, d2);
                        if missing > policy.etf_max_missing_weekdays {
                            mine.push(reason(
                                "REFUSE_GAP",
                                GateVerdict::Refuse,
                                Some(sym),
                                Some(d2),
                                format!("{}: {missing} weekdays missing between {d1} and {d2} (guard {})", side.name(), policy.etf_max_missing_weekdays),
                            ));
                        }
                    }
                }
            }
            // (e) a split on one source only
            for (side, series, other) in [(Side::Primary, p, s), (Side::Secondary, s, p)] {
                let Some(series) = series else { continue };
                for (d1, d2, c1, c2) in consecutive_in(series, from, to) {
                    let r = c2 / c1;
                    if !looks_like_split(r, policy.split_ratio_tolerance) {
                        continue;
                    }
                    let (Some(o1), Some(o2)) = (close_at(other, d1), close_at(other, d2)) else { continue };
                    let ro = o2 / o1;
                    if (ro / r - 1.0).abs() > policy.split_ratio_tolerance {
                        mine.push(reason(
                            "REFUSE_SPLIT_ONE_SOURCE",
                            GateVerdict::Refuse,
                            Some(sym),
                            Some(d2),
                            format!("{} moves by a factor {r:.4} from {d1} to {d2} (a split-like ratio); the other source moves by {ro:.4}", side.name()),
                        ));
                    }
                }
            }
        }

        // newest session (informational)
        if let (Some(p), Some(s)) = (p, s) {
            if p.last_date() != s.last_date() {
                mine.push(reason(
                    "FLAG_NEWEST_SESSION_DIFFERS",
                    GateVerdict::Flag,
                    Some(sym),
                    Some(p.last_date().max(s.last_date())),
                    format!("newest bar: primary {}, secondary {}", p.last_date(), s.last_date()),
                ));
            }
        }

        let max_diff = comparisons.iter().filter_map(|c| c.diff_bps).fold(None, |m: Option<f64>, d| Some(m.map_or(d, |x| x.max(d))));
        instruments.push(GateInstrument { symbol: sym.to_string(), verdict: worst(&mine), max_diff_bps: max_diff, comparisons });
        reasons.extend(mine);
    }

    Comparison { verdict: worst(&reasons), reasons, instruments, primary_decision_date: p_dd, secondary_decision_date: s_dd }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_like_ratios_are_integers_and_reciprocals_within_tolerance() {
        assert!(looks_like_split(2.0, 0.01));
        assert!(looks_like_split(0.5, 0.01));
        assert!(looks_like_split(3.02, 0.01));
        assert!(looks_like_split(0.2495, 0.01));
        assert!(!looks_like_split(1.0, 0.01));
        assert!(!looks_like_split(1.5, 0.01));
        assert!(!looks_like_split(2.05, 0.01));
        assert!(!looks_like_split(f64::NAN, 0.01));
        assert!(!looks_like_split(0.0, 0.01));
    }
}
