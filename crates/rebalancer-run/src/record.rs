//! The immutable run record and the small vocabulary around it (keys, outcomes, alerts).
//!
//! A [`RunRecord`] is written exactly once per run key (`RunStore::finish`, append-only) and is the audit trail of
//! the run: which mandate (hash and version), which data (fingerprints), which decisions (risk, targets, plan, every
//! guard denial), which orders were planned, ticketed or placed and what came back, both reconciliation reports, the
//! state transitions and the alerts raised. Nothing in it is edited afterwards.

use std::collections::BTreeMap;

use broker_adapters::{Dec, OrderReport, OrderStatus, Side};
use chrono::{DateTime, NaiveDate, Utc};
use rebalancer_core::guard::Denial;
use rebalancer_core::planner::{InstrumentLine, PlannedOrder, SkippedTrade};
use rebalancer_risk::overlay::RiskDecision;
use rebalancer_risk::state::{AccountStatus, Transition};

use crate::data::{Cadence, SleeveKind};
use crate::flatten::{CancelRecord, FlattenReport};
use crate::recon::{ReconBaseline, ReconReport};

/// How orders leave the rebalancer. Chosen by the caller (the account's execution mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionMode {
    /// Write the planned orders as tickets for a person to execute; place nothing, cancel nothing.
    Assisted,
    /// Send every order validate-only: the exchange checks it and creates nothing.
    Paper,
    /// Send real orders.
    Live,
}

impl ExecutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ExecutionMode::Assisted => "assisted",
            ExecutionMode::Paper => "paper",
            ExecutionMode::Live => "live",
        }
    }
}

/// The unique identity of a run: (account, scheduled time, sleeve set). A second run with the same key is a no-op.
///
/// The sleeve set is ALWAYS the account's CONFIGURED sleeves, never the subset that happened to be pending or due
/// on that day: a slot must map to exactly one run whatever the data looked like at the moment each attempt read it
/// (two attempts of one slot with different pending sets would otherwise be two keys and could both plan).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunKey {
    pub account_id: String,
    pub scheduled_for: DateTime<Utc>,
    /// Sleeve ids, sorted and joined with `+`.
    pub sleeve_set: String,
}

impl RunKey {
    pub fn new(account_id: &str, scheduled_for: DateTime<Utc>, sleeve_ids: &[&str]) -> Self {
        let mut ids: Vec<&str> = sleeve_ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        Self { account_id: account_id.to_string(), scheduled_for, sleeve_set: ids.join("+") }
    }

    /// `account|2026-09-21T15:00:00Z|etf+crypto`
    pub fn canonical(&self) -> String {
        format!("{}|{}|{}", self.account_id, self.scheduled_for.to_rfc3339(), self.sleeve_set)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeKind {
    /// The pipeline ran to its end (there may be zero orders).
    Completed,
    /// A deliberate refusal: kill flag, no usable mandate, account halted, another run in progress.
    Refused,
    /// Any doubt (broker unreachable, bad data, a rule refused, a store error): NO TRADING and an alert.
    FailedClosed,
    /// This run halted the account (risk limit or reconciliation).
    Halted,
}

impl OutcomeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            OutcomeKind::Completed => "COMPLETED",
            OutcomeKind::Refused => "REFUSED",
            OutcomeKind::FailedClosed => "FAILED_CLOSED",
            OutcomeKind::Halted => "HALTED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutcome {
    pub kind: OutcomeKind,
    /// A stable machine code, see [`crate::pipeline::RunCode`] and `HaltReason::code`.
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl AlertSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            AlertSeverity::Info => "info",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Critical => "critical",
        }
    }
}

/// Stable alert codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AlertCode {
    /// The account was halted (risk limit or reconciliation).
    Halt,
    /// A flatten did not verify flat, or hit a blocking failure.
    FlattenIncomplete,
    /// The run failed closed: nothing was traded because something was in doubt.
    RunFailed,
    /// The account is still halted (one reminder per run; the sink de-duplicates by `dedupe_key`).
    StillHalted,
    /// The mandate is unusable, so the run refused.
    MandateUnusable,
}

impl AlertCode {
    pub const ALL: [AlertCode; 5] = [AlertCode::Halt, AlertCode::FlattenIncomplete, AlertCode::RunFailed, AlertCode::StillHalted, AlertCode::MandateUnusable];

    pub fn as_str(self) -> &'static str {
        match self {
            AlertCode::Halt => "ALERT_HALT",
            AlertCode::FlattenIncomplete => "ALERT_FLATTEN_INCOMPLETE",
            AlertCode::RunFailed => "ALERT_RUN_FAILED",
            AlertCode::StillHalted => "ALERT_STILL_HALTED",
            AlertCode::MandateUnusable => "ALERT_MANDATE_UNUSABLE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alert {
    pub code: AlertCode,
    pub severity: AlertSeverity,
    pub account_id: String,
    pub run_key: String,
    pub message: String,
    /// Alerts with the same key describe the same condition; a sink may collapse them.
    pub dedupe_key: String,
    pub at: DateTime<Utc>,
}

/// What a broker snapshot contributes to the record (and, for the last one, to the next run's baseline).
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotSummary {
    pub taken_at: DateTime<Utc>,
    pub equity: Dec,
    pub cash: Dec,
    pub derived_equity: Dec,
    /// Upper-case symbol to quantity.
    pub holdings: BTreeMap<String, Dec>,
    pub marks: BTreeMap<String, Dec>,
    pub open_order_ids: Vec<String>,
}

impl SnapshotSummary {
    pub fn baseline(&self) -> ReconBaseline {
        ReconBaseline { cash: self.cash, holdings: self.holdings.clone(), marks: self.marks.clone() }
    }
}

/// One planned sleeve's target as the reference rule decided it. Present only for sleeves that were PLANNED in this
/// run (a sleeve that was evaluated but not pending has a [`SleeveDecision`] and no `TargetSummary`).
#[derive(Debug, Clone, PartialEq)]
pub struct TargetSummary {
    pub sleeve: String,
    pub decision_date: NaiveDate,
    pub data_fingerprint: String,
    /// (symbol, weight of the sleeve) as the reference rule decided.
    pub weights: Vec<(String, Dec)>,
}

/// The evidence behind one instrument's signal: what the rule compared. `margin_bps` = `(close / sma - 1) * 10_000`
/// (positive = above the average). Informational (fragility monitoring), never a trading input.
#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentEvidence {
    pub symbol: String,
    pub close: f64,
    pub sma: f64,
    pub margin_bps: f64,
    /// The rule's weight, as a fraction of the sleeve.
    pub weight: f64,
}

/// What one run learned about one CONFIGURED sleeve, whether or not it was planned: the structured per-decision
/// fields the council asked for (Ruling 7), kept on the record (and therefore in the Postgres `record_debug` text)
/// until a structured ledger exists (work item W6).
#[derive(Debug, Clone, PartialEq)]
pub struct SleeveDecision {
    pub sleeve: String,
    pub kind: SleeveKind,
    pub cadence: Cadence,
    /// The decision date this run's plan (if any) is built on.
    pub decision_date: NaiveDate,
    /// The newest decision the rule can compute from this run's panel (`D_computable`; for the ETF sleeve the newest
    /// COMPLETED month-end, `NextMonthBar`). Equal to `decision_date` in this version; kept apart because a
    /// catch-up policy may later act on something else.
    pub computable_decision_date: NaiveDate,
    /// The newest bar of any instrument in the panel the rule saw.
    pub newest_bar_date: NaiveDate,
    /// Sessions elapsed since the decision: bars of the sleeve's first instrument dated AFTER `decision_date`.
    /// 1 = acted on the first run after the first bar of the next period (the pre-registered one-session delay);
    /// 0 = decided on the newest bar (crypto).
    pub lag_sessions: u32,
    /// `D_acted`: the newest decision this account had acted on for this sleeve before this run. `None` for a
    /// `Daily` sleeve (not consulted) and for an `OnDecision` sleeve with no previous acted decision.
    pub last_acted_decision: Option<NaiveDate>,
    /// `Daily`: always. `OnDecision`: `decision_date > last_acted_decision`, or no `last_acted_decision`.
    pub pending: bool,
    /// An `OnDecision` sleeve planned only because nothing had ever been acted on (plan on the decision in force).
    pub entry: bool,
    /// The sleeve's target was handed to the planner in this run.
    pub planned: bool,
    /// The run completed with this sleeve planned: this decision counts as ACTED (`D_acted` advances). A run that
    /// failed closed, was refused or halted acts on nothing.
    pub acted: bool,
    pub instruments: Vec<InstrumentEvidence>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeniedSummary {
    pub order: PlannedOrder,
    /// The guard's stable denial codes, in the guard's order.
    pub codes: Vec<&'static str>,
    pub reasons: Vec<Denial>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanSummary {
    pub inputs_digest: String,
    pub equity: Dec,
    pub capital_base: Dec,
    pub risk_scale: Dec,
    pub orders: Vec<PlannedOrder>,
    pub denied: Vec<DeniedSummary>,
    pub skipped: Vec<SkippedTrade>,
    /// Held, current and target value per managed instrument.
    pub lines: Vec<InstrumentLine>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Sells,
    Buys,
    /// Paper mode (validate-only) submissions.
    Rehearsal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacedOutcome {
    /// The order ended fully executed.
    Filled,
    PartiallyFilled,
    /// Accepted and ended with nothing executed (cancelled by us after polling, or expired).
    NothingExecuted,
    /// Paper mode: the exchange validated the order and created nothing.
    Validated,
    /// The exchange refused it.
    Rejected,
    /// Not sent (local refusal, connection failure before sending, or the tag look-up failed so sending was unsafe).
    NotSent,
    /// The tag already existed at the exchange (an earlier attempt of this run): adopted, not re-sent.
    AdoptedExisting,
    /// The outcome was unknown and a look-up by tag found nothing: treated as not placed, NOT retried in this run.
    UnknownNotFound,
    /// Still live after polling and a cancel.
    Unsettled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlacedOrder {
    pub phase: Phase,
    pub tag: String,
    pub symbol: String,
    pub side: Side,
    pub planned_quantity: Dec,
    pub price: Dec,
    pub outcome: PlacedOutcome,
    pub broker_order_id: Option<String>,
    pub status: Option<OrderStatus>,
    pub executed_quantity: Dec,
    pub reports: Vec<OrderReport>,
    /// Errors met reading it, e.g. an adapter "overfill anomaly".
    pub anomalies: Vec<String>,
    pub detail: String,
}

/// One entry of the ordered step log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepNote {
    pub step: &'static str,
    pub at: DateTime<Utc>,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StageRecon {
    /// `pre` (before trading) or `post` (after the run's orders).
    pub stage: &'static str,
    pub report: ReconReport,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunRecord {
    pub key: RunKey,
    pub mode: ExecutionMode,
    /// Which attempt of this key wrote this record (a crashed attempt that is resumed counts up).
    pub attempt: u32,
    /// The account-local trading day the caller supplied.
    pub trading_day: NaiveDate,
    /// When the run was EXPECTED to start, and when it actually did / finished: the raw material of a missed-run
    /// (dead-man's switch) check.
    pub scheduled_for: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub outcome: RunOutcome,
    pub mandate_hash: String,
    pub mandate_version: Option<u32>,
    /// `active`, `expired`, `draft (rehearsal)`, `not active: ...`, `invalid`, `none`.
    pub mandate_standing: String,
    pub deployment_digest: Option<String>,
    pub data_fingerprints: Vec<(String, String)>,
    pub pre_snapshot: Option<SnapshotSummary>,
    pub post_snapshot: Option<SnapshotSummary>,
    pub recon: Vec<StageRecon>,
    pub state_before: Option<AccountStatus>,
    pub state_after: Option<AccountStatus>,
    pub transitions: Vec<Transition>,
    pub risk: Option<RiskDecision>,
    /// One entry per CONFIGURED sleeve that was evaluated (empty when the run stopped before the `decisions` step).
    pub decisions: Vec<SleeveDecision>,
    /// The targets of the PLANNED sleeves only (the pending ones).
    pub targets: Vec<TargetSummary>,
    pub plan: Option<PlanSummary>,
    /// The second plan of a live run, made after the sells settled and the account was re-read.
    pub replan: Option<PlanSummary>,
    /// Assisted mode: the orders a person is asked to place (the plan's orders).
    pub tickets: Vec<PlannedOrder>,
    pub placed: Vec<PlacedOrder>,
    /// Our own stale open orders cancelled before the run (live mode).
    pub cleanup: Vec<CancelRecord>,
    pub flatten: Option<FlattenReport>,
    pub alerts: Vec<Alert>,
    /// Alerts the notifier failed to deliver (the record still has them).
    pub alert_delivery_failures: Vec<String>,
    pub steps: Vec<StepNote>,
}

impl RunRecord {
    pub fn step_names(&self) -> Vec<&'static str> {
        self.steps.iter().map(|s| s.step).collect()
    }

    pub fn placed_live(&self) -> impl Iterator<Item = &PlacedOrder> {
        self.placed.iter().filter(|p| matches!(p.phase, Phase::Sells | Phase::Buys))
    }

    /// Denial codes across the plan and re-plan, in order.
    pub fn denial_codes(&self) -> Vec<&'static str> {
        self.plan.iter().chain(self.replan.iter()).flat_map(|p| p.denied.iter().flat_map(|d| d.codes.iter().copied())).collect()
    }
}
