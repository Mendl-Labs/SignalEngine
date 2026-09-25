//! Sleeve EVALUATION: the pure, tenant-independent half of "what does this sleeve want today?", kept apart from the
//! per-account half ("is that decision pending for THIS account, and do we act on it?", `pipeline`).
//!
//! `evaluate(kind, as_of)` fetches the sleeve's validated panel, fingerprints it and runs the reference rule. Nothing
//! in it depends on the account, the tenant or the broker, which is what lets many accounts share one fetch and one
//! decision within a tick ([`EvalCache`]).
//!
//! # The decision date of each kind (unchanged from before this module existed)
//! * ETF trend: `latest_decision_date` on the panel (the newest COMPLETED month-end; `MonthEndMode::NextMonthBar`, so
//!   a bar of a later month must exist: one session of delay by design) and `decide_etf_trend` with
//!   `Options::etf_live(as_of)`.
//! * Crypto trend: yesterday's UTC bar (`as_of - 1`) and `decide_crypto_trend` with `Options::crypto_live(as_of)`.
//!
//! # The in-tick cache
//! [`EvalCache`] lives for ONE driver tick (`run_all_due` makes one per call) and is keyed so that a stale answer can
//! never be served for a different question:
//! * fetches by `(kind, venue, asset_class, quote, as_of)`: everything about a sleeve that can change what a
//!   `DataSource` returns, plus the run date. (A fetch cannot be keyed by the data fingerprint: it is only known once
//!   the data has been fetched.) `DataSource` implementations must not vary the panel by sleeve id or tenant.
//! * decisions by `(kind, as_of, data fingerprint)`: two panels that differ by a single bar never share a decision.
//! Errors are cached for the tick as well (a vendor that returned 429 is not asked again by every account); the next
//! tick starts with an empty cache, so a corrected bar is always re-fetched.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::NaiveDate;
use rebalancer_core::dec_math::MathError;
use rebalancer_core::planner::SleeveTarget;
use reference_rules::{
    data_fingerprint, decide_crypto_trend, decide_etf_trend, latest_decision_date, CryptoDecision, EtfDecision, InstrumentDecision, Options, Panel,
    RuleError, ETF_SYMBOLS,
};

use crate::data::{DataError, DataSource, SleeveData, SleeveKind, SleeveSpec};
use crate::record::InstrumentEvidence;

/// The rule's own output, kept so a per-sleeve target can be built from it later (the target carries the sleeve's
/// id, share, venue and asset class, which are the account's, not the rule's).
#[derive(Debug, Clone)]
pub enum RuleDecision {
    Etf(EtfDecision),
    Crypto(CryptoDecision),
}

/// The tenant-independent result of evaluating one sleeve kind on one run date.
#[derive(Debug, Clone)]
pub struct Evaluation {
    pub kind: SleeveKind,
    pub as_of: NaiveDate,
    pub fingerprint: String,
    /// `D_computable` for `OnDecision` kinds; yesterday for `Daily` kinds.
    pub decision_date: NaiveDate,
    /// The newest bar of any instrument in the panel.
    pub newest_bar_date: NaiveDate,
    /// Bars of the first instrument dated after `decision_date`.
    pub lag_sessions: u32,
    pub instruments: Vec<InstrumentEvidence>,
    pub rule: RuleDecision,
}

impl Evaluation {
    /// The planner target of `sleeve` for this decision.
    pub fn target(&self, sleeve: &SleeveSpec) -> Result<SleeveTarget, MathError> {
        match &self.rule {
            RuleDecision::Etf(d) => SleeveTarget::from_etf(&sleeve.id, sleeve.share, &sleeve.venue, &sleeve.asset_class, d),
            RuleDecision::Crypto(d) => SleeveTarget::from_crypto(&sleeve.id, sleeve.share, &sleeve.venue, &sleeve.asset_class, &sleeve.quote, d),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EvalError {
    /// The data layer refused (unavailable, stale, ...).
    Data(DataError),
    /// The reference rule refused (`MonthEndMismatch`, `StaleData`, a gap, ...).
    Rule(RuleError),
}

/// A validated panel and its fingerprint.
#[derive(Debug, Clone)]
pub struct Fetched {
    pub data: SleeveData,
    pub fingerprint: String,
}

type FetchKey = (SleeveKind, String, String, String, NaiveDate);
type DecisionKey = (SleeveKind, NaiveDate, String);

/// Per-tick memo of fetches and decisions. See the module docs for the keys.
#[derive(Default)]
pub struct EvalCache {
    fetched: Mutex<BTreeMap<FetchKey, Result<Arc<Fetched>, DataError>>>,
    decided: Mutex<BTreeMap<DecisionKey, Result<Arc<Evaluation>, RuleError>>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl EvalCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// (fetches remembered, decisions remembered): for tests and logs.
    pub fn sizes(&self) -> (usize, usize) {
        (lock(&self.fetched).len(), lock(&self.decided).len())
    }
}

fn evidence(i: &InstrumentDecision) -> InstrumentEvidence {
    InstrumentEvidence { symbol: i.symbol.clone(), close: i.close, sma: i.sma, margin_bps: (i.close / i.sma - 1.0) * 10_000.0, weight: i.weight }
}

fn fetch(data: &dyn DataSource, cache: Option<&EvalCache>, s: &SleeveSpec, as_of: NaiveDate) -> Result<Arc<Fetched>, DataError> {
    let key: FetchKey = (s.kind, s.venue.clone(), s.asset_class.clone(), s.quote.clone(), as_of);
    if let Some(c) = cache {
        if let Some(hit) = lock(&c.fetched).get(&key) {
            return hit.clone();
        }
    }
    let result = data.sleeve_data(s, as_of).map(|d| {
        let fingerprint = data_fingerprint(&d.panel);
        Arc::new(Fetched { data: d, fingerprint })
    });
    if let Some(c) = cache {
        lock(&c.fetched).insert(key, result.clone());
    }
    result
}

fn newest_bar(panel: &Panel) -> Option<NaiveDate> {
    panel.iter().map(|s| s.last_date()).max()
}

fn decide(kind: SleeveKind, panel: &Panel, as_of: NaiveDate) -> Result<(NaiveDate, RuleDecision), RuleError> {
    match kind {
        SleeveKind::EtfTrend => {
            let date = latest_decision_date(panel, &ETF_SYMBOLS)?;
            let d = decide_etf_trend(panel, date, &Options::etf_live(as_of))?;
            Ok((date, RuleDecision::Etf(d)))
        }
        SleeveKind::CryptoTrend => {
            let date = as_of.pred_opt().unwrap_or(as_of);
            let d = decide_crypto_trend(panel, date, &Options::crypto_live(as_of))?;
            Ok((date, RuleDecision::Crypto(d)))
        }
    }
}

fn build(kind: SleeveKind, as_of: NaiveDate, fetched: &Fetched) -> Result<Evaluation, RuleError> {
    let panel = &fetched.data.panel;
    let (decision_date, rule) = decide(kind, panel, as_of)?;
    let instruments: Vec<InstrumentEvidence> = match &rule {
        RuleDecision::Etf(d) => d.instruments.iter().map(evidence).collect(),
        RuleDecision::Crypto(d) => d.instruments.iter().map(evidence).collect(),
    };
    let lag_sessions = instruments
        .first()
        .and_then(|i| panel.get(&i.symbol).ok())
        .map(|s| {
            let up_to = s.dates().partition_point(|d| *d <= decision_date);
            (s.len() - up_to) as u32
        })
        .unwrap_or(0);
    // The rule succeeded, so the panel has every instrument it needs and therefore at least one bar.
    let newest_bar_date = newest_bar(panel).unwrap_or(decision_date);
    Ok(Evaluation { kind, as_of, fingerprint: fetched.fingerprint.clone(), decision_date, newest_bar_date, lag_sessions, instruments, rule })
}

/// Fetch the sleeve's panel and run its rule as of `as_of`, through `cache` when one is given.
pub fn evaluate(data: &dyn DataSource, cache: Option<&EvalCache>, s: &SleeveSpec, as_of: NaiveDate) -> Result<Arc<Evaluation>, EvalError> {
    let fetched = fetch(data, cache, s, as_of).map_err(EvalError::Data)?;
    let key: DecisionKey = (s.kind, as_of, fetched.fingerprint.clone());
    if let Some(c) = cache {
        if let Some(hit) = lock(&c.decided).get(&key) {
            return hit.clone().map_err(EvalError::Rule);
        }
    }
    let result = build(s.kind, as_of, &fetched).map(Arc::new);
    if let Some(c) = cache {
        lock(&c.decided).insert(key, result.clone());
    }
    result.map_err(EvalError::Rule)
}
