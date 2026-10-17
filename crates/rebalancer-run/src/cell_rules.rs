//! Rule-family seam for R41 CELLS (rule x universe), long-only ETFs.
//!
//! A cell is the existing certified trend rule run over a pre-registered universe of N instruments, each held at
//! exactly `1 / N` (R41-A(1): `WeightPolicy::EqualOverN`, max gross 100% by construction). The published rule and
//! the Phase 4 implementation points hold `ETF_WEIGHT_PER_INSTRUMENT` = 0.20 per name, which IS `1.0 / 5.0` bit for
//! bit, so on a five-instrument universe the two policies are the same number.
//!
//! # The seam
//! The rule is reached through [`TrendRuleBackend`]; [`CoreTrendBackend`] calls BacktestingCore's
//! `decide_etf_trend_with_weight` (PR #73, pinned at its merge commit) with the weight the policy derives for the
//! universe (`1 / N` under [`WeightPolicy::EqualOverN`], the Core `weightsim-rules` policy of the same name; the
//! constant under [`WeightPolicy::FixedConstant`]). Signals, month-end and gap checks are Core's, one
//! implementation; a non-`Long` instrument carries weight 0.0.
//!
//! Momentum cells (linear rank weights) are a different kind, `SleeveKind::EtfMomentum`: their decision is
//! `decision::decide_momentum` over Core's pure `momentum_rank_weights`, with no weight policy to apply (the rank
//! weights sum to 1 at any K); only the gross guard, [`sleeve_max_gross`], is shared with this module.

use chrono::NaiveDate;
use reference_rules::{decide_etf_trend_with_weight, EtfDecision, Options, Panel, RuleError, CRYPTO_SYMBOLS, CRYPTO_WEIGHT_PER_INSTRUMENT, ETF_SYMBOLS, ETF_WEIGHT_PER_INSTRUMENT};

use crate::data::{SleeveKind, SleeveSpec};

/// How a trend rule sizes each `Long` instrument. Mirrors the cell file's `weight_policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightPolicy {
    /// The rule's own constant (`ETF_WEIGHT_PER_INSTRUMENT` = 0.20): the published rule.
    FixedConstant,
    /// Exactly `1.0 / N` of the sleeve per `Long` instrument, N = the number of instruments of the universe.
    EqualOverN,
}

/// The policy a sleeve runs under. A sleeve at the PUBLISHED instruments (`instruments` empty) is the certified rule:
/// [`WeightPolicy::FixedConstant`], the code path before cells existed. A sleeve that names its instruments (a Phase
/// 4 implementation point, which must have exactly the entry's five slots, or a cell) runs [`WeightPolicy::EqualOverN`];
/// at N = 5 that is bit-identical to the constant, so no existing sleeve changes, and for a cell it is what the cell
/// registered (the account source refuses a cell registered under any other policy).
pub fn trend_weight_policy(sleeve: &SleeveSpec) -> WeightPolicy {
    if sleeve.instruments.is_empty() {
        WeightPolicy::FixedConstant
    } else {
        WeightPolicy::EqualOverN
    }
}

/// The per-instrument weight of a `Long` name under `policy` on an `n`-instrument universe.
pub fn long_weight(policy: WeightPolicy, n: usize, fixed_constant: f64) -> f64 {
    match policy {
        WeightPolicy::FixedConstant => fixed_constant,
        WeightPolicy::EqualOverN => 1.0 / (n as f64),
    }
}

/// The trend rule over an explicit instrument list and month-end lookback, with a weight policy.
pub trait TrendRuleBackend: Send + Sync {
    fn decide_trend(&self, panel: &Panel, decision_date: NaiveDate, opts: &Options, symbols: &[&str], sma_month_ends: usize, policy: WeightPolicy) -> Result<EtfDecision, RuleError>;
}

/// The backend over the pinned Core: `decide_etf_trend_with_weight` with the policy's per-instrument weight.
#[derive(Debug, Default, Clone, Copy)]
pub struct CoreTrendBackend;

impl TrendRuleBackend for CoreTrendBackend {
    fn decide_trend(&self, panel: &Panel, decision_date: NaiveDate, opts: &Options, symbols: &[&str], sma_month_ends: usize, policy: WeightPolicy) -> Result<EtfDecision, RuleError> {
        decide_etf_trend_with_weight(panel, decision_date, opts, symbols, sma_month_ends, long_weight(policy, symbols.len(), ETF_WEIGHT_PER_INSTRUMENT))
    }
}

/// The backend the pipeline uses.
pub static TREND_BACKEND: CoreTrendBackend = CoreTrendBackend;

/// The maximum gross exposure of one sleeve as a fraction of the ACCOUNT's capital: its share times the gross its rule
/// can reach (every instrument `Long`). Trend ETF: `trend_max_gross` over the sleeve's instruments (the published five
/// when none are named); crypto: the published per-instrument weight times its instruments.
pub fn sleeve_max_gross(sleeve: &SleeveSpec) -> f64 {
    let per_unit = match sleeve.kind {
        SleeveKind::EtfTrend => {
            let n = if sleeve.instruments.is_empty() { ETF_SYMBOLS.len() } else { sleeve.instruments.len() };
            trend_max_gross(trend_weight_policy(sleeve), n, ETF_WEIGHT_PER_INSTRUMENT)
        }
        SleeveKind::CryptoTrend => {
            let n = if sleeve.instruments.is_empty() { CRYPTO_SYMBOLS.len() } else { sleeve.instruments.len() };
            CRYPTO_WEIGHT_PER_INSTRUMENT * (n as f64)
        }
        // R41-A(2): the linear-rank weights of the K ranked names sum to exactly 1.0 for every K, so the maximum gross
        // is 100% of the sleeve at any N, by construction (no parameter to derive, none to tune).
        SleeveKind::EtfMomentum => 1.0,
    };
    per_unit * sleeve.share.to_f64()
}

/// The largest gross exposure, as a fraction of the sleeve, the trend rule can ask for on `n` instruments under
/// `policy`: every instrument `Long`. `EqualOverN` is 1.0 for every N (up to f64 rounding of `n * (1/n)`);
/// `FixedConstant` is `n * 0.20`, which is why a fixed-constant cell on 11 names (220%) cannot be allowed.
pub fn trend_max_gross(policy: WeightPolicy, n: usize, fixed_constant: f64) -> f64 {
    long_weight(policy, n, fixed_constant) * (n as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Weekday};
    use reference_rules::{decide_etf_trend_with, PriceSeries, Signal, ETF_SMA_MONTH_ENDS};

    fn spec(instruments: &[&str]) -> SleeveSpec {
        SleeveSpec {
            id: "s".into(),
            kind: crate::data::SleeveKind::EtfTrend,
            share: rebalancer_core::Dec::from_i64(1),
            venue: "alpaca".into(),
            asset_class: "us_etf".into(),
            quote: String::new(),
            instruments: instruments.iter().map(|s| s.to_string()).collect(),
            tranche_sessions: 0,
            lookback: None,
        }
    }

    #[test]
    fn the_published_sleeve_keeps_the_constant_and_a_named_universe_runs_one_over_n() {
        assert_eq!(trend_weight_policy(&spec(&[])), WeightPolicy::FixedConstant);
        assert_eq!(trend_weight_policy(&spec(&["SPY", "EFA", "IEF", "DBC", "VNQ"])), WeightPolicy::EqualOverN);
    }

    #[test]
    fn one_over_five_is_bit_identical_to_the_published_constant() {
        assert_eq!(long_weight(WeightPolicy::EqualOverN, 5, ETF_WEIGHT_PER_INSTRUMENT).to_bits(), ETF_WEIGHT_PER_INSTRUMENT.to_bits());
        assert_eq!(long_weight(WeightPolicy::FixedConstant, 11, ETF_WEIGHT_PER_INSTRUMENT).to_bits(), ETF_WEIGHT_PER_INSTRUMENT.to_bits());
        assert_eq!(long_weight(WeightPolicy::EqualOverN, 9, ETF_WEIGHT_PER_INSTRUMENT).to_bits(), (1.0_f64 / 9.0).to_bits());
    }

    #[test]
    fn max_gross_is_100_percent_under_one_over_n_and_not_under_the_fixed_constant() {
        for n in [5, 9, 11, 20] {
            assert!((trend_max_gross(WeightPolicy::EqualOverN, n, ETF_WEIGHT_PER_INSTRUMENT) - 1.0).abs() < 1e-12, "N = {n}");
        }
        assert!((trend_max_gross(WeightPolicy::FixedConstant, 11, ETF_WEIGHT_PER_INSTRUMENT) - 2.2).abs() < 1e-12, "the 220% R41-A(1) rules out");
        assert!((trend_max_gross(WeightPolicy::FixedConstant, 5, ETF_WEIGHT_PER_INSTRUMENT) - 1.0).abs() < 1e-12);
    }

    /// Daily weekday bars from 2018-01-01 to 2019-03-01: the first `up` symbols rise, the rest fall. The decision date
    /// is the last bar of February 2019 (a month-end; March 1 exists, so it is known to be over).
    fn panel(symbols: &[String], up: usize) -> (Panel, NaiveDate) {
        let mut dates = Vec::new();
        let mut d = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2019, 3, 1).unwrap();
        while d <= end {
            if !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) {
                dates.push(d);
            }
            d = d.succ_opt().unwrap();
        }
        let series = symbols
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let closes: Vec<f64> = (0..dates.len()).map(|k| if i < up { 100.0 + k as f64 } else { 1000.0 - k as f64 }).collect();
                PriceSeries::new(s.as_str(), dates.clone(), closes).unwrap()
            })
            .collect();
        (Panel::new(series).unwrap(), NaiveDate::from_ymd_opt(2019, 2, 28).unwrap())
    }

    #[test]
    fn equal_over_n_leaves_the_signals_alone_and_sizes_each_long_name_at_one_over_n() {
        for n in [9usize, 11] {
            let symbols: Vec<String> = (0..n).map(|i| format!("E{i:02}")).collect();
            let refs: Vec<&str> = symbols.iter().map(String::as_str).collect();
            let (p, date) = panel(&symbols, n - 3);
            let opts = Options::etf_replay(reference_rules::MonthEndMode::Explicit);
            let core = decide_etf_trend_with(&p, date, &opts, &refs, ETF_SMA_MONTH_ENDS).unwrap();
            let cell = TREND_BACKEND.decide_trend(&p, date, &opts, &refs, ETF_SMA_MONTH_ENDS, WeightPolicy::EqualOverN).unwrap();
            assert_eq!(core.instruments.len(), n);
            for (a, b) in core.instruments.iter().zip(&cell.instruments) {
                assert_eq!((&a.symbol, a.close.to_bits(), a.sma.to_bits(), a.signal), (&b.symbol, b.close.to_bits(), b.sma.to_bits(), b.signal), "signals and evidence are the unweighted rule's, untouched");
                let want = if a.signal == Signal::Long { (1.0_f64 / n as f64).to_bits() } else { 0.0_f64.to_bits() };
                assert_eq!(b.weight.to_bits(), want, "{} at N = {n}", b.symbol);
            }
            assert_eq!(cell.instruments.iter().filter(|i| i.signal == Signal::Long).count(), n - 3);
            let gross: f64 = cell.instruments.iter().map(|i| i.weight).sum();
            assert!(gross <= 1.0 + 1e-12);
        }
    }

    #[test]
    fn at_the_five_published_instruments_the_two_policies_are_the_same_decision() {
        let symbols: Vec<String> = ETF_SYMBOLS.iter().map(|s| s.to_string()).collect();
        let (p, date) = panel(&symbols, 3);
        let opts = Options::etf_replay(reference_rules::MonthEndMode::Explicit);
        let fixed = TREND_BACKEND.decide_trend(&p, date, &opts, &ETF_SYMBOLS, ETF_SMA_MONTH_ENDS, WeightPolicy::FixedConstant).unwrap();
        let equal = TREND_BACKEND.decide_trend(&p, date, &opts, &ETF_SYMBOLS, ETF_SMA_MONTH_ENDS, WeightPolicy::EqualOverN).unwrap();
        assert_eq!(fixed, equal);
    }

    #[test]
    fn a_sleeves_max_gross_is_its_share_times_the_rules_gross() {
        let mut published = spec(&[]);
        published.share = rebalancer_core::Dec::parse("0.6").unwrap();
        assert!((sleeve_max_gross(&published) - 0.6).abs() < 1e-12, "five names at 0.20");
        let mut cell = spec(&["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"]);
        cell.share = rebalancer_core::Dec::parse("0.5").unwrap();
        assert!((sleeve_max_gross(&cell) - 0.5).abs() < 1e-12, "eleven names at 1/11");
        let mut crypto = spec(&[]);
        crypto.kind = SleeveKind::CryptoTrend;
        assert!((sleeve_max_gross(&crypto) - 1.0).abs() < 1e-12, "two names at 0.50");
    }
}
