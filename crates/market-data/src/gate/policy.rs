//! The R19 tolerances as a versioned, hashed policy. Changing a constant without bumping [`POLICY_VERSION`] fails a
//! test (R19 acceptance 7): the hash of the canonical text is pinned.
//!
//! Semantics: a difference is "beyond" a tolerance when it is STRICTLY greater (a bar exactly at 25 bp passes the
//! 25 bp flag level). Differences are `|a - b| / min(a, b) * 10_000`, rounded to 1e-6 bp, which is symmetric under a
//! source swap and never smaller than the primary-relative figure.

use rebalancer_run::data::BarPosition;
use sha2::{Digest, Sha256};

pub const POLICY_VERSION: &str = "R19-2026-09-26";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tolerance {
    pub flag_bps: f64,
    pub refuse_bps: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Policy {
    /// ETF: the month-end closes the rule reads.
    pub etf_month_end: Tolerance,
    /// Crypto: yesterday's close.
    pub crypto_decision_day: Tolerance,
    /// Crypto: the other SMA inputs.
    pub crypto_window: Tolerance,
    /// Core's ETF gap guard: more missing weekdays than this between consecutive bars is a gap (R15).
    pub etf_max_missing_weekdays: u32,
    /// A day-over-day ratio within this fraction of an integer (or its reciprocal) looks like a split; the other
    /// source's ratio for the same two dates must be within the same fraction of it, else the split is on one source
    /// only (R18.3e).
    pub split_ratio_tolerance: f64,
}

impl Policy {
    /// The R19 table.
    pub const fn r19() -> Policy {
        Policy {
            etf_month_end: Tolerance { flag_bps: 25.0, refuse_bps: 50.0 },
            crypto_decision_day: Tolerance { flag_bps: 50.0, refuse_bps: 150.0 },
            crypto_window: Tolerance { flag_bps: 100.0, refuse_bps: 500.0 },
            etf_max_missing_weekdays: 3,
            split_ratio_tolerance: 0.01,
        }
    }

    pub fn tolerance(&self, position: BarPosition) -> Tolerance {
        match position {
            BarPosition::MonthEnd => self.etf_month_end,
            BarPosition::DecisionDay => self.crypto_decision_day,
            BarPosition::Window => self.crypto_window,
        }
    }

    /// The canonical text the hash is taken over.
    pub fn canonical(&self) -> String {
        format!(
            "{POLICY_VERSION}|diff=abs/min*1e4,round1e-6,strict-greater|etf_month_end={}/{}|crypto_decision_day={}/{}|crypto_window={}/{}|etf_max_missing_weekdays={}|split_ratio_tolerance={}",
            self.etf_month_end.flag_bps,
            self.etf_month_end.refuse_bps,
            self.crypto_decision_day.flag_bps,
            self.crypto_decision_day.refuse_bps,
            self.crypto_window.flag_bps,
            self.crypto_window.refuse_bps,
            self.etf_max_missing_weekdays,
            self.split_ratio_tolerance
        )
    }

    /// SHA-256 (hex) of [`Policy::canonical`].
    pub fn hash(&self) -> String {
        hex::encode(Sha256::digest(self.canonical().as_bytes()))
    }
}

impl Default for Policy {
    fn default() -> Self {
        Policy::r19()
    }
}

/// `|a - b| / min(a, b) * 10_000`, rounded to 1e-6 bp.
pub fn diff_bps(a: f64, b: f64) -> f64 {
    let d = (a - b).abs() / a.min(b) * 10_000.0;
    (d * 1e6).round() / 1e6
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R19 acceptance 7: a tolerance constant cannot change without a version bump AND a new pinned hash.
    #[test]
    fn the_r19_policy_hash_is_pinned() {
        let p = Policy::r19();
        assert_eq!(
            p.canonical(),
            "R19-2026-09-26|diff=abs/min*1e4,round1e-6,strict-greater|etf_month_end=25/50|crypto_decision_day=50/150|crypto_window=100/500|etf_max_missing_weekdays=3|split_ratio_tolerance=0.01"
        );
        assert_eq!(p.hash(), "2dab1c304208267aadc69a41ed167a5d2a8e7ae6cfceb2b5ceedf82484818711");
    }

    #[test]
    fn diff_is_symmetric_and_exact_at_round_basis_points() {
        assert_eq!(diff_bps(100.0, 100.25), 25.0);
        assert_eq!(diff_bps(100.25, 100.0), 25.0);
        assert_eq!(diff_bps(100.0, 100.5), 50.0);
        assert_eq!(diff_bps(100.0, 100.0), 0.0);
        assert_eq!(diff_bps(50.0, 100.0), 10_000.0);
    }
}
