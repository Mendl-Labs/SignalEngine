//! What the pipeline needs from the data layer, as traits, plus the sleeve description.
//!
//! A [`DataSource`] returns VALIDATED price panels (`reference_rules::Panel`: strictly ascending dates, positive
//! finite closes) and current prices for sizing. It is the seam where the real data gate (WP2.3: last complete bar,
//! no forming bar, retry on 429, bypass caches) plugs in. The reference rules apply their own checks on top
//! (staleness, gaps, month-end), and ANY refusal from either layer means the run trades nothing.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use rebalancer_core::guard::PricePoint;
use rebalancer_core::Dec;
use reference_rules::{Panel, CRYPTO_SMA_DAYS, CRYPTO_SYMBOLS, ETF_SMA_MONTH_ENDS, ETF_SYMBOLS, MOMENTUM_RANK_WEIGHTED_DEFAULT_LOOKBACK};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SleeveKind {
    /// Faber-style ETF trend (SPY, EFA, IEF, DBC, VNQ; 20% each of the sleeve). The RULE decides at a month-end
    /// close; the platform ACTS on that decision on the first run after the first bar of the next month exists
    /// (`MonthEndMode::NextMonthBar`: one session after the month's last close), see [`Cadence::OnDecision`]. It does
    /// NOT act on the calendar month-end: a run on that date cannot yet see the month complete.
    EtfTrend,
    /// 100-day crypto trend (BTC, ETH; 50% each of the sleeve).
    CryptoTrend,
    /// R41 CELLS ONLY: cross-sectional ETF momentum (`etf_cross_sectional_momentum`, linear rank weights over the
    /// cell's universe, trailing 126-bar return). Never a published-point sleeve: the account source builds it only
    /// from a registered cell, so `instruments` is always set. The rule re-decides on every completed daily bar
    /// (`DecisionSchedule::Daily`, `RebalancePolicy::OnDecision`, Amendment 14): the decision date is the newest
    /// completed bar, and the platform acts when that date is newer than the last one it acted on (so a weekend or
    /// holiday run, which sees no new bar, plans nothing). Closes are daily, like [`SleeveKind::EtfTrend`].
    EtfMomentum,
}

/// When a sleeve's target is ACTED on, as opposed to when its rule is evaluated (every driver run evaluates every
/// sleeve). This is a property of the sleeve KIND (the rule's own rebalance policy), never of the mandate: a
/// customer cannot configure a certified rule into a different cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cadence {
    /// Planned on every run (the rule re-decides every day: crypto trend).
    Daily,
    /// Planned only when the rule's decision is newer than the last decision this account ACTED on
    /// (`D_computable > D_acted`; a missing `D_acted` is an entry, planned once on the decision in force).
    OnDecision,
}

impl SleeveKind {
    pub fn cadence(self) -> Cadence {
        match self {
            SleeveKind::CryptoTrend => Cadence::Daily,
            SleeveKind::EtfTrend | SleeveKind::EtfMomentum => Cadence::OnDecision,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SleeveKind::EtfTrend => "etf_trend",
            SleeveKind::CryptoTrend => "crypto_trend",
            SleeveKind::EtfMomentum => "etf_momentum",
        }
    }
}

/// One strategy sleeve of an account's plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleeveSpec {
    /// Stable id (part of the run key and of every order tag).
    pub id: String,
    pub kind: SleeveKind,
    /// Fraction of capital, in (0, 1]; all sleeves of a run sum to at most 1.
    pub share: Dec,
    pub venue: String,
    pub asset_class: String,
    /// Quote currency for crypto pairs (`BTC` becomes `BTC/USD`); ignored for ETFs.
    pub quote: String,
    /// The tenant's IMPLEMENTATION POINT inside the library entry's pre-registered envelope (certified-envelopes
    /// plan, Phase 4). **Empty = the published instruments** (`ETF_SYMBOLS` / `CRYPTO_SYMBOLS`); otherwise one per
    /// slot, in the entry's slot order (ETF: tickers; crypto: `BASE/QUOTE` as the entry spells them). The
    /// account source fills it from the plan's point only after the point passed the envelope check.
    pub instruments: Vec<String>,
    /// Execution lag in trading sessions (ETF: sessions AFTER the published action session; crypto: days of
    /// signal lag). `0` = the published timing.
    pub tranche_sessions: u8,
    /// Lookback override (ETF: month-ends; crypto: days). `None` = the published lookback.
    pub lookback: Option<u32>,
}

impl SleeveSpec {
    /// The published point: nothing overridden. A sleeve at the published point behaves exactly as before the
    /// envelope fields existed.
    pub fn is_published_point(&self) -> bool {
        self.instruments.is_empty() && self.tranche_sessions == 0 && self.lookback.is_none()
    }

    /// The symbols the reference rule and the market-data fetch use (the panel's series names): the ETF tickers, or
    /// the crypto BASE of each `BASE/QUOTE` instrument. Published constants when `instruments` is empty.
    pub fn rule_symbols(&self) -> Vec<String> {
        if self.instruments.is_empty() {
            return match self.kind {
                SleeveKind::EtfTrend | SleeveKind::EtfMomentum => ETF_SYMBOLS.iter().map(|s| s.to_string()).collect(),
                SleeveKind::CryptoTrend => CRYPTO_SYMBOLS.iter().map(|s| s.to_string()).collect(),
            };
        }
        match self.kind {
            SleeveKind::EtfTrend | SleeveKind::EtfMomentum => self.instruments.clone(),
            SleeveKind::CryptoTrend => self.instruments.iter().map(|i| i.split('/').next().unwrap_or(i).to_string()).collect(),
        }
    }

    /// The rule's lookback: the override, else the published constant (month-ends for ETF trend, days for crypto,
    /// trading-day bars for momentum: 126).
    pub fn sma_lookback(&self) -> usize {
        match (self.kind, self.lookback) {
            (_, Some(n)) => n as usize,
            (SleeveKind::EtfTrend, None) => ETF_SMA_MONTH_ENDS,
            (SleeveKind::CryptoTrend, None) => CRYPTO_SMA_DAYS,
            (SleeveKind::EtfMomentum, None) => MOMENTUM_RANK_WEIGHTED_DEFAULT_LOOKBACK,
        }
    }

    /// `""` at the published point, else a stable text of everything about the point that can change what a fetch
    /// or a decision returns. Part of the per-tick `EvalCache` keys, so two accounts at different points never share
    /// a fetch or a decision, and two at the same point still do.
    pub fn variant_key(&self) -> String {
        if self.is_published_point() {
            return String::new();
        }
        format!("{}|t{}|l{}", self.instruments.join(","), self.tranche_sessions, self.lookback.map(|l| l.to_string()).unwrap_or_default())
    }
}

/// The validated data one sleeve's rule needs.
#[derive(Debug, Clone)]
pub struct SleeveData {
    pub panel: Panel,
    /// What the two-source data gate (W9.2, COUNCIL_DATA_GATE R18-R20) found about this panel, when a gate was in
    /// the path. `None` = no gate (mode `off`, or a source that is not a gate). In SHADOW mode the report is
    /// recorded and `panel` is the primary's, unchanged, whatever the verdict.
    pub gate: Option<DataGateReport>,
}

impl SleeveData {
    pub fn new(panel: Panel) -> Self {
        Self { panel, gate: None }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// The two-source gate's report (R20 `data_provenance`, the comparison half). Pure data: the gate itself lives in
// `market-data`; the pipeline only records this on the sleeve decision and alerts on a shadow refusal.
// ---------------------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateMode {
    /// The verdict is computed and recorded; the primary panel is always returned (nothing refuses yet).
    Shadow,
}

impl GateMode {
    pub fn as_str(self) -> &'static str {
        match self {
            GateMode::Shadow => "shadow",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GateVerdict {
    Pass,
    /// Recorded and (in enforce, later) continued: an input above the FLAG tolerance, or an informational finding.
    Flag,
    /// Would have refused in enforce mode: no orders, alert.
    Refuse,
}

impl GateVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            GateVerdict::Pass => "PASS",
            GateVerdict::Flag => "FLAG",
            GateVerdict::Refuse => "REFUSE",
        }
    }
}

/// Which R19 tolerance row a compared bar falls under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarPosition {
    /// ETF: one of the month-end closes the rule reads.
    MonthEnd,
    /// Crypto: yesterday's close (weight 1 in the rule).
    DecisionDay,
    /// Crypto: one of the other SMA inputs (weight 1/100).
    Window,
    /// ETF momentum: the decision-day close or the close one lookback earlier (the two closes the rank reads); the
    /// equity-ETF tolerance row, the same one a month-end close is held to.
    DailyClose,
}

impl BarPosition {
    pub fn as_str(self) -> &'static str {
        match self {
            BarPosition::MonthEnd => "month_end",
            BarPosition::DecisionDay => "decision_day",
            BarPosition::Window => "window",
            BarPosition::DailyClose => "daily_close",
        }
    }
}

/// One finding of the gate. `code` is stable (`REFUSE_*` / `FLAG_*`); `symbol` and `date` are set when the finding is
/// about one bar, which is what the shadow alert de-duplicates on.
#[derive(Debug, Clone, PartialEq)]
pub struct GateReason {
    pub code: String,
    pub verdict: GateVerdict,
    pub symbol: Option<String>,
    pub date: Option<NaiveDate>,
    pub detail: String,
}

/// Both closes of one compared bar and their difference. `None` closes mean the bar is missing on that side.
#[derive(Debug, Clone, PartialEq)]
pub struct GateComparison {
    pub symbol: String,
    pub date: NaiveDate,
    pub position: BarPosition,
    pub primary_close: Option<f64>,
    pub secondary_close: Option<f64>,
    /// `|primary - secondary| / min(primary, secondary) * 10_000` (symmetric under a source swap), rounded to 1e-6.
    pub diff_bps: Option<f64>,
    pub verdict: GateVerdict,
}

/// Per instrument: the verdict and the comparisons behind it.
#[derive(Debug, Clone, PartialEq)]
pub struct GateInstrument {
    pub symbol: String,
    pub verdict: GateVerdict,
    pub max_diff_bps: Option<f64>,
    pub comparisons: Vec<GateComparison>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataGateReport {
    pub mode: GateMode,
    pub kind: SleeveKind,
    pub as_of: NaiveDate,
    pub primary_source: String,
    pub secondary_source: String,
    pub policy_version: String,
    pub policy_hash: String,
    pub primary_fingerprint: String,
    /// `None` when the secondary could not be fetched (the report then carries the refusal reason).
    pub secondary_fingerprint: Option<String>,
    pub primary_decision_date: Option<NaiveDate>,
    pub secondary_decision_date: Option<NaiveDate>,
    pub verdict: GateVerdict,
    pub reasons: Vec<GateReason>,
    pub instruments: Vec<GateInstrument>,
}

impl DataGateReport {
    /// The refusals, one per `(symbol, date)` at most (sleeve-level reasons have no symbol), in order: what the shadow
    /// alert is raised for.
    pub fn refusals(&self) -> Vec<&GateReason> {
        let mut seen: std::collections::BTreeSet<(Option<String>, Option<NaiveDate>)> = std::collections::BTreeSet::new();
        self.reasons.iter().filter(|r| r.verdict == GateVerdict::Refuse).filter(|r| seen.insert((r.symbol.clone(), r.date))).collect()
    }

    pub fn summary(&self) -> String {
        let reasons: Vec<String> = self.reasons.iter().take(8).map(|r| match (&r.symbol, r.date) {
            (Some(s), Some(d)) => format!("{}({s}@{d})", r.code),
            (Some(s), None) => format!("{}({s})", r.code),
            (None, Some(d)) => format!("{}(@{d})", r.code),
            (None, None) => r.code.clone(),
        }).collect();
        format!(
            "{} gate {} vs {}: {}{}",
            self.mode.as_str(),
            self.primary_source,
            self.secondary_source,
            self.verdict.as_str(),
            if reasons.is_empty() { String::new() } else { format!(" [{}{}]", reasons.join(", "), if self.reasons.len() > 8 { ", ..." } else { "" }) }
        )
    }
}

/// A data problem. `code` is stable (`DATA_UNAVAILABLE`, `DATA_STALE`, ...); the pipeline maps any error to
/// `RUN_DATA_ERROR` and records both.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct DataError {
    pub code: String,
    pub message: String,
}

impl DataError {
    pub fn new(code: &str, message: &str) -> Self {
        Self { code: code.to_string(), message: message.to_string() }
    }
}

pub trait DataSource {
    /// The panel for a sleeve as of the run date `as_of` (the UTC date of the scheduled time).
    fn sleeve_data(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<SleeveData, DataError>;

    /// Current prices for sizing and for valuing held positions, by canonical symbol. A symbol that cannot be
    /// priced is simply absent (the planner then skips it and records `NoPrice`); an outage is an `Err`.
    fn prices(&self, symbols: &[String], now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sleeve(kind: SleeveKind) -> SleeveSpec {
        SleeveSpec {
            id: "s".into(),
            kind,
            share: Dec::from_i64(1),
            venue: "alpaca".into(),
            asset_class: "us_etf".into(),
            quote: "USD".into(),
            instruments: Vec::new(),
            tranche_sessions: 0,
            lookback: None,
        }
    }

    #[test]
    fn a_sleeve_without_a_point_is_the_published_constants_and_has_an_empty_variant_key() {
        let etf = sleeve(SleeveKind::EtfTrend);
        assert!(etf.is_published_point());
        assert_eq!(etf.rule_symbols(), ETF_SYMBOLS.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        assert_eq!(etf.sma_lookback(), ETF_SMA_MONTH_ENDS);
        assert_eq!(etf.variant_key(), "");
        let crypto = sleeve(SleeveKind::CryptoTrend);
        assert_eq!(crypto.rule_symbols(), CRYPTO_SYMBOLS.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        assert_eq!(crypto.sma_lookback(), CRYPTO_SMA_DAYS);
        assert_eq!(crypto.variant_key(), "");
    }

    #[test]
    fn every_axis_of_the_point_changes_the_variant_key_and_unsets_the_published_flag() {
        let base = sleeve(SleeveKind::EtfTrend);
        let proxy = SleeveSpec { instruments: vec!["IVV".into(), "EFA".into(), "IEF".into(), "DBC".into(), "VNQ".into()], ..base.clone() };
        let lagged = SleeveSpec { tranche_sessions: 2, ..base.clone() };
        let longer = SleeveSpec { lookback: Some(12), ..base.clone() };
        let keys = [base.variant_key(), proxy.variant_key(), lagged.variant_key(), longer.variant_key()];
        for (i, a) in keys.iter().enumerate() {
            for b in &keys[i + 1..] {
                assert_ne!(a, b);
            }
        }
        assert!(!proxy.is_published_point() && !lagged.is_published_point() && !longer.is_published_point());
        assert_eq!(proxy.rule_symbols()[0], "IVV");
        assert_eq!(longer.sma_lookback(), 12);
    }

    #[test]
    fn a_crypto_pairs_rule_symbol_is_its_base() {
        let s = SleeveSpec { instruments: vec!["BTC/USD".into(), "ETH/USD".into()], ..sleeve(SleeveKind::CryptoTrend) };
        assert_eq!(s.rule_symbols(), vec!["BTC", "ETH"]);
    }
}
