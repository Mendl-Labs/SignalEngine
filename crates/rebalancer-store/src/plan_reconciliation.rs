//! The reconciliation write-back for proposal-derived plans (`strategy_plans.reconciliation_status` /
//! `reconciliation_detail` / `reconciliation_checked_at`): the three columns the proposals migration reserved for
//! "the rebalancer / a reconciliation job, never the API" (databaseschema-internal
//! `2026-09-23-000000_create_proposals_and_plans/up.sql`), written here for the first time.
//!
//! After every run of an account whose plan origin is [`crate::PlanOrigin::Proposal`], the service maps the run's
//! outcome to a status ([`reconciliation_for`]) and records it ([`PgPlanReconciliation::record`]). The mapping is
//! pure and pinned by tests; the write is one `UPDATE` bound to (plan id, tenant id), so a plan of another tenant
//! can never be touched. `strategy_plans_enforce_immutability` allows exactly these columns (plus `state` and
//! `updated_at`) to change; the service's database role needs column-level `UPDATE` on them (see the PR / the
//! scratch-role grants in `tests/common/mod.rs`).
//!
//! The pilot plan (`rebalancer_pilot_plans`) is append-only and has no such columns: nothing is written for it.

use chrono::{DateTime, Utc};
use diesel::sql_types::{Text, Timestamptz, Uuid as SqlUuid};
use diesel_async::RunQueryDsl;
use rebalancer_run::record::{OutcomeKind, RunRecord};
use uuid::Uuid;

use crate::pg::{Bridge, Pool};

/// The values `strategy_plans.reconciliation_status` accepts after a run (`awaiting_first_run` is the DB default and
/// is never written back).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationStatus {
    InSync,
    Mismatch,
}

impl ReconciliationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ReconciliationStatus::InSync => "in_sync",
            ReconciliationStatus::Mismatch => "mismatch",
        }
    }
}

/// Longest `reconciliation_detail` written (the column is TEXT; this keeps a run's message from dumping a page).
pub const MAX_DETAIL_CHARS: usize = 600;

/// The status and detail a finished run implies for its plan. `Completed` (the pipeline ran to its end: the
/// account's reconciliation step passed and whatever orders were needed were handled) is `in_sync`; a refusal, a
/// fail-closed run or a halt is `mismatch`, with the run's own code and message as the detail so a person reading
/// the plan sees the same reason the run record carries.
pub fn reconciliation_for(record: &RunRecord) -> (ReconciliationStatus, String) {
    let status = match record.outcome.kind {
        OutcomeKind::Completed => ReconciliationStatus::InSync,
        OutcomeKind::Refused | OutcomeKind::FailedClosed | OutcomeKind::Halted => ReconciliationStatus::Mismatch,
    };
    let mut detail = format!(
        "run {} ({} mode): {} {}: {}",
        record.key.canonical(),
        record.mode.as_str(),
        record.outcome.kind.as_str(),
        record.outcome.code,
        record.outcome.message.trim()
    );
    if detail.chars().count() > MAX_DETAIL_CHARS {
        detail = detail.chars().take(MAX_DETAIL_CHARS - 3).collect::<String>() + "...";
    }
    (status, detail)
}

pub struct PgPlanReconciliation {
    bridge: Bridge,
}

impl PgPlanReconciliation {
    pub fn new(pool: Pool) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)? })
    }

    /// Write the status of ONE plan, bound to its tenant. Exactly one non-closed row must match; anything else
    /// (no row, a closed plan, a row of another tenant, a database error) is an `Err` the caller logs. Never
    /// panics, never touches a second row.
    pub fn record(&self, tenant_id: Uuid, plan_id: Uuid, status: ReconciliationStatus, detail: &str, checked_at: DateTime<Utc>) -> Result<(), String> {
        let detail = detail.to_string();
        let status_text = status.as_str().to_string();
        let updated = self
            .bridge
            .block_on(move |mut conn| async move {
                diesel::sql_query(
                    "UPDATE strategy_plans SET reconciliation_status = $1, reconciliation_detail = $2, reconciliation_checked_at = $3 \
                     WHERE id = $4 AND tenant_id = $5 AND state <> 'closed'",
                )
                .bind::<Text, _>(&status_text)
                .bind::<Text, _>(&detail)
                .bind::<Timestamptz, _>(checked_at)
                .bind::<SqlUuid, _>(plan_id)
                .bind::<SqlUuid, _>(tenant_id)
                .execute(&mut conn)
                .await
                .map_err(|e| format!("PLAN_RECONCILIATION_UNAVAILABLE: {e}"))
            })
            .map_err(|e| if e.starts_with("PLAN_RECONCILIATION_") { e } else { format!("PLAN_RECONCILIATION_UNAVAILABLE: {e}") })?;
        match updated {
            1 => Ok(()),
            0 => Err(format!("PLAN_RECONCILIATION_NOT_WRITTEN: no non-closed strategy_plans row {plan_id} under tenant {tenant_id}")),
            n => Err(format!("PLAN_RECONCILIATION_NOT_WRITTEN: {n} rows matched plan {plan_id} under tenant {tenant_id}; expected exactly one")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rebalancer_run::record::{ExecutionMode, RunKey, RunOutcome};

    fn record(kind: OutcomeKind, code: &str, message: &str) -> RunRecord {
        let at: chrono::DateTime<Utc> = "2026-10-15T15:00:00Z".parse().unwrap();
        RunRecord {
            key: RunKey::new("acct", at, &["etf_trend_faber"]),
            mode: ExecutionMode::Live,
            attempt: 1,
            trading_day: at.date_naive(),
            scheduled_for: at,
            started_at: at,
            finished_at: at,
            outcome: RunOutcome { kind, code: code.to_string(), message: message.to_string() },
            mandate_hash: "a".repeat(64),
            mandate_version: Some(1),
            mandate_standing: "active".to_string(),
            deployment_digest: None,
            data_fingerprints: vec![],
            pre_snapshot: None,
            post_snapshot: None,
            recon: vec![],
            state_before: None,
            state_after: None,
            transitions: vec![],
            risk: None,
            decisions: vec![],
            targets: vec![],
            plan: None,
            replan: None,
            tickets: vec![],
            placed: vec![],
            cleanup: vec![],
            flatten: None,
            alerts: vec![],
            alert_delivery_failures: vec![],
            steps: vec![],
        }
    }

    #[test]
    fn completed_is_in_sync_and_everything_else_is_a_mismatch_carrying_the_runs_own_reason() {
        let (s, d) = reconciliation_for(&record(OutcomeKind::Completed, "RUN_COMPLETED", "2 orders filled"));
        assert_eq!(s, ReconciliationStatus::InSync);
        assert!(d.contains("acct|2026-10-15T15:00:00+00:00|etf_trend_faber") && d.contains("COMPLETED") && d.contains("RUN_COMPLETED") && d.contains("2 orders filled"), "{d}");
        for (kind, code) in [(OutcomeKind::Refused, "RUN_KILL_FLAG"), (OutcomeKind::FailedClosed, "RUN_DATA_ERROR"), (OutcomeKind::Halted, "RUN_RECON_HALT")] {
            let (s, d) = reconciliation_for(&record(kind, code, "why"));
            assert_eq!(s, ReconciliationStatus::Mismatch, "{code}");
            assert!(d.contains(code) && d.contains("why"), "{d}");
        }
    }

    #[test]
    fn the_detail_is_bounded() {
        let (_, d) = reconciliation_for(&record(OutcomeKind::FailedClosed, "RUN_DATA_ERROR", &"x".repeat(5000)));
        assert_eq!(d.chars().count(), MAX_DETAIL_CHARS);
        assert!(d.ends_with("..."));
    }

    #[test]
    fn status_strings_are_the_check_constraints_values() {
        assert_eq!(ReconciliationStatus::InSync.as_str(), "in_sync");
        assert_eq!(ReconciliationStatus::Mismatch.as_str(), "mismatch");
    }
}
