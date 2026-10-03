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
use reference_rules::Panel;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SleeveKind {
    /// Faber-style ETF trend (SPY, EFA, IEF, DBC, VNQ; 20% each of the sleeve). The RULE decides at a month-end
    /// close; the platform ACTS on that decision on the first run after the first bar of the next month exists
    /// (`MonthEndMode::NextMonthBar`: one session after the month's last close), see [`Cadence::OnDecision`]. It does
    /// NOT act on the calendar month-end: a run on that date cannot yet see the month complete.
    EtfTrend,
    /// 100-day crypto trend (BTC, ETH; 50% each of the sleeve).
    CryptoTrend,
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
            SleeveKind::EtfTrend => Cadence::OnDecision,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SleeveKind::EtfTrend => "etf_trend",
            SleeveKind::CryptoTrend => "crypto_trend",
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
}

impl BarPosition {
    pub fn as_str(self) -> &'static str {
        match self {
            BarPosition::MonthEnd => "month_end",
            BarPosition::DecisionDay => "decision_day",
            BarPosition::Window => "window",
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
