//! The three delivery severities (`alert_deliveries.severity IN ('info','warning','critical')`) and the mapping from
//! what `rebalancer_alerts` stores.
//!
//! The pipeline already grades every alert (`AlertSeverity::as_str()` is exactly `info|warning|critical`), so the
//! normal path is a straight parse of the stored text. The fallback by code exists because the stored text is a
//! VARCHAR a future writer could get wrong, and a wrong severity must fail TOWARDS the louder channel set: an
//! unparseable severity is graded by what the code means (a halt is critical whatever the row says), never dropped.

use std::fmt;

use rebalancer_run::record::AlertSeverity;

use crate::ledger::ChannelScope;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Critical => "critical",
        }
    }

    /// The stored text, exactly as `AlertSeverity::as_str()` writes it (case-insensitive, trimmed).
    pub fn parse(s: &str) -> Option<Severity> {
        match s.trim().to_ascii_lowercase().as_str() {
            "info" => Some(Severity::Info),
            "warning" => Some(Severity::Warning),
            "critical" => Some(Severity::Critical),
            _ => None,
        }
    }

    /// The severity the pipeline assigns to a code when the stored severity is unusable. Mirrors the pipeline's own
    /// `alert(..)` calls (`rebalancer-run/src/pipeline.rs`): halts, failed-closed runs and incomplete flattens are
    /// critical; reminders, unusable mandates, not-acted decisions and observer failures are warnings. An unknown
    /// code is graded `Warning`, the level that reaches the platform channels, so a new code is never silently
    /// confined to tenant channels because nobody added it here.
    pub fn fallback_for_code(code: &str) -> Severity {
        match code.trim() {
            "ALERT_HALT" | "ALERT_FLATTEN_INCOMPLETE" | "ALERT_RUN_FAILED" => Severity::Critical,
            "ALERT_STILL_HALTED" | "ALERT_MANDATE_UNUSABLE" | "ALERT_DECISION_NOT_ACTED" | "ALERT_HEARTBEAT_FAILED" => Severity::Warning,
            _ => Severity::Warning,
        }
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<AlertSeverity> for Severity {
    fn from(s: AlertSeverity) -> Self {
        match s {
            AlertSeverity::Info => Severity::Info,
            AlertSeverity::Warning => Severity::Warning,
            AlertSeverity::Critical => Severity::Critical,
        }
    }
}

/// The delivery severity of a stored alert: the stored text when it parses, else the code's own grade.
pub fn delivery_severity(code: &str, stored_severity: &str) -> Severity {
    Severity::parse(stored_severity).unwrap_or_else(|| Severity::fallback_for_code(code))
}

/// Does a channel of `scope` receive an alert of `severity`? Tenant channels receive everything addressed to their
/// tenant (the tenant match itself is the caller's job); platform channels receive `warning` and `critical` only.
pub fn channel_applies(scope: ChannelScope, severity: Severity) -> bool {
    match scope {
        ChannelScope::Tenant => true,
        ChannelScope::Platform => severity >= Severity::Warning,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rebalancer_run::record::AlertCode;

    #[test]
    fn the_stored_text_round_trips_and_is_forgiving_of_case() {
        for s in [Severity::Info, Severity::Warning, Severity::Critical] {
            assert_eq!(Severity::parse(s.as_str()), Some(s));
            assert_eq!(Severity::parse(&s.as_str().to_uppercase()), Some(s));
            assert_eq!(Severity::parse(&format!("  {s}  ")), Some(s));
        }
        assert_eq!(Severity::parse("fatal"), None);
        assert_eq!(Severity::parse(""), None);
    }

    #[test]
    fn the_pipeline_severities_map_one_to_one() {
        assert_eq!(Severity::from(AlertSeverity::Info), Severity::Info);
        assert_eq!(Severity::from(AlertSeverity::Warning), Severity::Warning);
        assert_eq!(Severity::from(AlertSeverity::Critical), Severity::Critical);
        for s in [AlertSeverity::Info, AlertSeverity::Warning, AlertSeverity::Critical] {
            assert_eq!(Severity::from(s).as_str(), s.as_str(), "the stored text and the delivery text are the same vocabulary");
        }
    }

    #[test]
    fn every_pipeline_code_has_a_fallback_and_unknown_codes_fail_towards_warning() {
        let critical = ["ALERT_HALT", "ALERT_FLATTEN_INCOMPLETE", "ALERT_RUN_FAILED"];
        for c in AlertCode::ALL {
            let want = if critical.contains(&c.as_str()) { Severity::Critical } else { Severity::Warning };
            assert_eq!(Severity::fallback_for_code(c.as_str()), want, "{}", c.as_str());
        }
        assert_eq!(Severity::fallback_for_code("ALERT_SOMETHING_NEW"), Severity::Warning);
        // stored text wins when it parses; the code grades only when it does not
        assert_eq!(delivery_severity("ALERT_HALT", "info"), Severity::Info);
        assert_eq!(delivery_severity("ALERT_HALT", "garbage"), Severity::Critical);
        assert_eq!(delivery_severity("ALERT_MANDATE_UNUSABLE", ""), Severity::Warning);
    }

    #[test]
    fn platform_channels_receive_warning_and_critical_only() {
        assert!(channel_applies(ChannelScope::Tenant, Severity::Info));
        assert!(channel_applies(ChannelScope::Tenant, Severity::Critical));
        assert!(!channel_applies(ChannelScope::Platform, Severity::Info));
        assert!(channel_applies(ChannelScope::Platform, Severity::Warning));
        assert!(channel_applies(ChannelScope::Platform, Severity::Critical));
    }
}
