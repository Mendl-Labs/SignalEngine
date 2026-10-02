//! The two-source data gate (W9.2; COUNCIL_DATA_GATE R15/R18/R19/R20), SHADOW mode only.
//!
//! [`TwoSourceGate`] is a decorator over [`SleeveFetcher`]: it fetches the PRIMARY (the source whose definition the
//! certified rule uses: Massive), then the SECONDARY of the sleeve's kind (ETF: Alpaca daily bars, crypto: Kraken
//! public OHLC; both platform credentials, never a tenant's), runs the pure [`compare`] and attaches the
//! [`DataGateReport`] to the fetched sleeve. In shadow mode it ALWAYS returns the primary's panel unchanged, whatever
//! the verdict: nothing refuses yet. The pipeline records the report on the run (`SleeveDecision::data_gate`) and
//! raises `ALERT_DATA_GATE_SHADOW_REFUSE` when the verdict is `REFUSE`, so the owner sees what enforce would do.
//!
//! Sleeve-scoped by construction: each `fetch_sleeve` call compares one kind against that kind's secondary; an ETF
//! comparison failure is in the ETF report and nowhere else (R18 test 7). Per-tick memoisation is the pipeline's
//! `EvalCache` (the report travels inside `SleeveData`), so cost is independent of tenant count (R18.1).
//!
//! [`DataGateMode`] is read from `DATA_GATE_MODE`: `off` (default) or `shadow`; `enforce` is REJECTED at startup in
//! this slice so it cannot be turned on by accident.

pub mod compare;
pub mod policy;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use chrono::NaiveDate;
use rebalancer_run::data::{DataGateReport, GateMode, GateReason, GateVerdict, SleeveKind, SleeveSpec};
use reference_rules::data_fingerprint;

use crate::error::SleeveError;
use crate::source::{FetchedSleeve, SleeveFetcher};

pub use compare::{compare, Comparison};
pub use policy::{diff_bps, Policy, Tolerance, POLICY_VERSION};

pub const ENV_DATA_GATE_MODE: &str = "DATA_GATE_MODE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataGateMode {
    Off,
    Shadow,
}

impl DataGateMode {
    /// `off` (unset or empty = off), `shadow`; `enforce` and anything else is an error (fail closed at startup).
    pub fn parse(raw: Option<&str>) -> Result<DataGateMode, String> {
        match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("off") => Ok(DataGateMode::Off),
            Some("shadow") => Ok(DataGateMode::Shadow),
            Some("enforce") => Err(format!(
                "{ENV_DATA_GATE_MODE}=enforce is not available in this build: W9.2 ships SHADOW mode only (the verdict is recorded and alerted, the primary panel is always used); set off or shadow"
            )),
            Some(other) => Err(format!("{ENV_DATA_GATE_MODE}={other:?} is not one of off, shadow")),
        }
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<DataGateMode, String> {
        Self::parse(lookup(ENV_DATA_GATE_MODE).as_deref())
    }

    pub fn as_str(self) -> &'static str {
        match self {
            DataGateMode::Off => "off",
            DataGateMode::Shadow => "shadow",
        }
    }
}

/// The shadow-mode decorator. See the module docs.
pub struct TwoSourceGate<P: SleeveFetcher> {
    primary: P,
    secondaries: BTreeMap<SleeveKind, Arc<dyn SleeveFetcher>>,
    policy: Policy,
}

impl<P: SleeveFetcher> fmt::Debug for TwoSourceGate<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: Vec<String> = self.secondaries.iter().map(|(k, v)| format!("{}={}", k.as_str(), v.source_id())).collect();
        f.debug_struct("TwoSourceGate").field("mode", &GateMode::Shadow.as_str()).field("primary", &self.primary.source_id()).field("secondaries", &s).finish()
    }
}

impl<P: SleeveFetcher> TwoSourceGate<P> {
    pub fn shadow(primary: P) -> Self {
        Self { primary, secondaries: BTreeMap::new(), policy: Policy::r19() }
    }

    pub fn with_secondary(mut self, kind: SleeveKind, secondary: Arc<dyn SleeveFetcher>) -> Self {
        self.secondaries.insert(kind, secondary);
        self
    }

    pub fn with_policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    pub fn secondary_for(&self, kind: SleeveKind) -> Option<&Arc<dyn SleeveFetcher>> {
        self.secondaries.get(&kind)
    }

    fn report(&self, sleeve: &SleeveSpec, as_of: NaiveDate, primary: &FetchedSleeve) -> DataGateReport {
        let base = |secondary_source: String| DataGateReport {
            mode: GateMode::Shadow,
            kind: sleeve.kind,
            as_of,
            primary_source: self.primary.source_id().to_string(),
            secondary_source,
            policy_version: POLICY_VERSION.to_string(),
            policy_hash: self.policy.hash(),
            primary_fingerprint: data_fingerprint(&primary.panel),
            secondary_fingerprint: None,
            primary_decision_date: None,
            secondary_decision_date: None,
            verdict: GateVerdict::Refuse,
            reasons: Vec::new(),
            instruments: Vec::new(),
        };
        let Some(secondary) = self.secondaries.get(&sleeve.kind) else {
            let mut r = base("none".to_string());
            r.reasons.push(GateReason {
                code: "REFUSE_SECONDARY_NOT_CONFIGURED".to_string(),
                verdict: GateVerdict::Refuse,
                symbol: None,
                date: None,
                detail: format!("no secondary source is configured for {} sleeves; the gate never falls back to single-source", sleeve.kind.as_str()),
            });
            return r;
        };
        match secondary.fetch_sleeve(sleeve, as_of) {
            Err(e) => {
                let mut r = base(secondary.source_id().to_string());
                r.reasons.push(GateReason {
                    code: "REFUSE_SECONDARY_UNAVAILABLE".to_string(),
                    verdict: GateVerdict::Refuse,
                    symbol: e.error.instrument().map(str::to_string),
                    date: None,
                    detail: format!("{:?}/{}: {}", e.class(), e.kind().code(), e.error),
                });
                r
            }
            Ok(f) => {
                let c = compare(sleeve.kind, as_of, &primary.panel, &f.panel, &self.policy);
                let mut r = base(secondary.source_id().to_string());
                r.secondary_fingerprint = Some(data_fingerprint(&f.panel));
                r.primary_decision_date = c.primary_decision_date;
                r.secondary_decision_date = c.secondary_decision_date;
                r.verdict = c.verdict;
                r.reasons = c.reasons;
                r.instruments = c.instruments;
                r
            }
        }
    }
}

impl<P: SleeveFetcher> SleeveFetcher for TwoSourceGate<P> {
    fn source_id(&self) -> &'static str {
        self.primary.source_id()
    }

    /// The primary's failure is the caller's failure (unchanged from an ungated source). On success the secondary is
    /// fetched and compared; the report is attached and the PRIMARY panel is returned as is.
    fn fetch_sleeve(&self, sleeve: &SleeveSpec, as_of: NaiveDate) -> Result<FetchedSleeve, SleeveError> {
        let primary = self.primary.fetch_sleeve(sleeve, as_of)?;
        let report = self.report(sleeve, as_of, &primary);
        Ok(FetchedSleeve { panel: primary.panel, provenance: primary.provenance, gate: Some(report) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mode_defaults_to_off_accepts_shadow_and_rejects_enforce() {
        assert_eq!(DataGateMode::parse(None), Ok(DataGateMode::Off));
        assert_eq!(DataGateMode::parse(Some("")), Ok(DataGateMode::Off));
        assert_eq!(DataGateMode::parse(Some(" OFF ")), Ok(DataGateMode::Off));
        assert_eq!(DataGateMode::parse(Some("shadow")), Ok(DataGateMode::Shadow));
        let e = DataGateMode::parse(Some("enforce")).unwrap_err();
        assert!(e.contains("not available") && e.contains("SHADOW"), "{e}");
        assert!(DataGateMode::parse(Some("on")).is_err());
        assert_eq!(DataGateMode::from_lookup(|_| Some("shadow".into())), Ok(DataGateMode::Shadow));
    }
}
