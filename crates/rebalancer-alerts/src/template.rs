//! Plain-text e-mail templates. Subject `[Mendl Labs] <severity> <code> <account>`; body = the alert's own message,
//! the run key and time, and for the codes that need a human to ACT (kill, halt, flatten, missed run) one line of
//! "what to do". No HTML, no links the reader has to trust, nothing secret (the message text comes from the pipeline,
//! which never puts a credential in an alert).

use crate::ledger::PendingAlert;
use crate::severity::Severity;

pub const SUBJECT_PREFIX: &str = "[Mendl Labs]";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    pub subject: String,
    pub body: String,
}

/// The one-line "what to do" footer of the codes that need a person: the halt / kill family, an incomplete flatten,
/// a run that failed closed, a decision that was not acted on, and a missed heartbeat. Informational codes get none.
pub fn what_to_do(code: &str) -> Option<&'static str> {
    match code.trim() {
        "ALERT_HALT" => Some("What to do: the account is halted and will not trade again until a person resumes it. Check the broker account and the run record, then resume through the human endpoint only once the cause is understood."),
        "ALERT_STILL_HALTED" => Some("What to do: the account is still halted; nothing runs until a person resumes it. If the halt was already investigated, resume it; otherwise investigate before resuming."),
        "ALERT_FLATTEN_INCOMPLETE" => Some("What to do: the flatten did not verify flat. Open the broker account NOW, close any remaining positions by hand, and do not resume until the account verifies flat."),
        "ALERT_RUN_FAILED" => Some("What to do: the run failed closed and traded nothing. Read the run record's outcome code; if it persists past the retry window, the decision is being missed and needs a person."),
        "ALERT_DECISION_NOT_ACTED" => Some("What to do: a planned order was not carried out, so the decision stays pending and is planned again next run. Check the placed orders in the run record for a venue refusal or guard denial."),
        "ALERT_HEARTBEAT_FAILED" => Some("What to do: the service could not reach its external monitor; the monitor will page as if the service were down. Check network egress from the service and the monitor URL."),
        _ => None,
    }
}

pub fn subject(severity: Severity, code: &str, account_id: &str) -> String {
    format!("{SUBJECT_PREFIX} {} {} {}", severity.as_str(), code.trim(), account_id.trim())
}

/// Render one alert as a plain-text e-mail.
pub fn render(alert: &PendingAlert, severity: Severity) -> Rendered {
    let mut body = String::new();
    body.push_str(&format!("Severity: {}\n", severity.as_str()));
    body.push_str(&format!("Code:     {}\n", alert.code.trim()));
    body.push_str(&format!("Account:  {}\n", alert.account_id.trim()));
    body.push_str(&format!("Run:      {}\n", alert.run_key.trim()));
    body.push_str(&format!("At:       {}\n", alert.at.to_rfc3339()));
    body.push('\n');
    body.push_str(alert.message.trim());
    body.push('\n');
    if let Some(action) = what_to_do(&alert.code) {
        body.push('\n');
        body.push_str(action);
        body.push('\n');
    }
    body.push_str("\n-- \nMendl Labs rebalancer. This alert is also recorded in the run record; de-duplication key: ");
    body.push_str(alert.dedupe_key.trim());
    body.push('\n');
    Rendered { subject: subject(severity, &alert.code, &alert.account_id), body }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use rebalancer_run::record::AlertCode;

    fn alert(code: &str) -> PendingAlert {
        PendingAlert {
            id: "a1".into(),
            tenant_id: Some("11111111-1111-1111-1111-111111111111".into()),
            account_id: "acct-1".into(),
            code: code.into(),
            severity: "critical".into(),
            message: "HALT_RISK_LIMIT: drawdown breached; nothing was traded".into(),
            run_key: "acct-1|2026-10-02T15:00:00+00:00|etf".into(),
            dedupe_key: "acct-1:halt".into(),
            at: Utc.with_ymd_and_hms(2026, 10, 2, 15, 0, 7).unwrap(),
            created_at: Utc.with_ymd_and_hms(2026, 10, 2, 15, 0, 8).unwrap(),
        }
    }

    #[test]
    fn the_subject_is_prefix_severity_code_account() {
        let r = render(&alert("ALERT_HALT"), Severity::Critical);
        assert_eq!(r.subject, "[Mendl Labs] critical ALERT_HALT acct-1");
    }

    #[test]
    fn the_body_carries_the_message_the_run_key_and_the_time() {
        let r = render(&alert("ALERT_HALT"), Severity::Critical);
        assert!(r.body.contains("HALT_RISK_LIMIT: drawdown breached"));
        assert!(r.body.contains("acct-1|2026-10-02T15:00:00+00:00|etf"));
        assert!(r.body.contains("2026-10-02T15:00:07+00:00"));
        assert!(r.body.contains("acct-1:halt"));
        assert!(!r.body.contains('<'), "plain text, no markup: {}", r.body);
    }

    #[test]
    fn action_codes_get_a_what_to_do_footer_and_informational_codes_do_not() {
        for code in ["ALERT_HALT", "ALERT_STILL_HALTED", "ALERT_FLATTEN_INCOMPLETE", "ALERT_RUN_FAILED", "ALERT_DECISION_NOT_ACTED", "ALERT_HEARTBEAT_FAILED"] {
            let r = render(&alert(code), Severity::Critical);
            assert!(r.body.contains("What to do:"), "{code} needs a footer:\n{}", r.body);
        }
        for code in ["ALERT_MANDATE_UNUSABLE", "ALERT_SOMETHING_NEW"] {
            let r = render(&alert(code), Severity::Warning);
            assert!(!r.body.contains("What to do:"), "{code} must not carry a footer:\n{}", r.body);
        }
        // every pipeline code either has a footer or is deliberately informational (listed here, so adding a code
        // forces a decision). ALERT_LATENCY_RECORDER_FAILED (W9.1) is a read-only observer failure that never
        // affects a run -- there is no action for a person to take beyond checking the migration/logs, so it is
        // informational like ALERT_MANDATE_UNUSABLE, not actionable.
        let informational = ["ALERT_MANDATE_UNUSABLE", "ALERT_LATENCY_RECORDER_FAILED"];
        for c in AlertCode::ALL {
            assert!(what_to_do(c.as_str()).is_some() || informational.contains(&c.as_str()), "{} is neither actionable nor listed as informational", c.as_str());
        }
    }
}
