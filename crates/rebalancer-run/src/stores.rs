//! The persistence and side-effect seams of the pipeline, as traits with in-memory implementations:
//! [`RunStore`] (append-only run records with a UNIQUE run key), [`Notifier`] (halt / failure alerts) and
//! [`KillFlag`] (the database kill switch read at the start of every run). A Postgres implementation of
//! `RunStore` maps `begin` to `INSERT ... ON CONFLICT (account, scheduled_for, sleeve_set) DO NOTHING` plus a read,
//! and `finish` to one immutable-row insert (a trigger rejects UPDATE/DELETE).
//!
//! # Run-key semantics
//! * First `begin` for a key: `Started { attempt: 1 }`; the key is now IN PROGRESS with a lease.
//! * `begin` for a FINISHED key: `AlreadyDone(record)`: the caller returns that record and does nothing else (a
//!   duplicate scheduled run is a no-op).
//! * `begin` for an in-progress key whose lease has not expired: `Busy` (someone is running it).
//! * `begin` for an in-progress key whose lease HAS expired (the earlier attempt died): `Started { attempt + 1 }`.
//!   The pipeline then resumes: it never sends an order whose tag already exists at the broker.
//! * `finish` writes the record once; a second `finish` is an error (records are immutable).
//!
//! # Missed runs
//! Every record carries `scheduled_for` (expected) and `started_at` / `finished_at` (actual). [`find_missed`] compares
//! a list of expected slots with the store's summaries, which is all a heartbeat / dead-man's switch needs.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rebalancer_core::guard::DayCounters;
use rebalancer_core::Dec;

use crate::record::{Alert, ExecutionMode, OutcomeKind, Phase, PlacedOutcome, RunKey, RunRecord, SnapshotSummary};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RunStoreError {
    #[error("RUNSTORE_UNAVAILABLE: {0}")]
    Unavailable(String),
    #[error("RUNSTORE_ALREADY_FINISHED: the record for {0} was already written and is immutable")]
    AlreadyFinished(String),
    #[error("RUNSTORE_NOT_STARTED: no run in progress for {0}")]
    NotStarted(String),
}

impl RunStoreError {
    pub fn code(&self) -> &'static str {
        match self {
            RunStoreError::Unavailable(_) => "RUNSTORE_UNAVAILABLE",
            RunStoreError::AlreadyFinished(_) => "RUNSTORE_ALREADY_FINISHED",
            RunStoreError::NotStarted(_) => "RUNSTORE_NOT_STARTED",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Begin {
    Started { attempt: u32 },
    AlreadyDone(Box<RunRecord>),
    Busy { started_at: DateTime<Utc> },
}

/// An order recorded the moment it was accepted, before the run finishes, so a crashed run's orders are still known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    pub tag: String,
    /// `None` while only the INTENT to place the order is recorded (written before sending); set once the broker
    /// accepted it.
    pub broker_order_id: Option<String>,
    pub notional: Dec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub key: RunKey,
    pub attempt: u32,
    pub scheduled_for: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub outcome: Option<(OutcomeKind, String)>,
}

impl RunSummary {
    /// How late the run started relative to its schedule, in seconds.
    pub fn lateness_secs(&self) -> i64 {
        self.started_at.signed_duration_since(self.scheduled_for).num_seconds()
    }
}

pub trait RunStore {
    fn begin(&self, key: &RunKey, trading_day: NaiveDate, started_at: DateTime<Utc>, lease_secs: i64) -> Result<Begin, RunStoreError>;
    /// Record an order of the running attempt: first as an intent (`broker_order_id: None`, BEFORE it is sent), then
    /// again with the broker id. An entry with the same tag replaces the earlier one. A crashed attempt's journal
    /// is what the next attempt reconciles against.
    fn journal_order(&self, key: &RunKey, entry: JournalEntry) -> Result<(), RunStoreError>;
    /// Journal entries of runs that are still in progress (or died) for this account.
    fn in_flight(&self, account_id: &str) -> Result<Vec<JournalEntry>, RunStoreError>;
    fn finish(&self, record: RunRecord) -> Result<(), RunStoreError>;
    /// The broker-side snapshot the next run reconciles against: the post-run snapshot (or, failing that, the
    /// pre-run one) of the most recently finished record that has one.
    fn last_snapshot(&self, account_id: &str) -> Result<Option<SnapshotSummary>, RunStoreError>;
    /// Broker ids of every order this account's runs placed (finished or journaled).
    fn known_order_ids(&self, account_id: &str) -> Result<BTreeSet<String>, RunStoreError>;
    /// Live orders already placed on `day` (count and notional), from finished records and in-progress journals.
    fn day_counters(&self, account_id: &str, day: NaiveDate) -> Result<DayCounters, RunStoreError>;
    fn summaries(&self, account_id: &str) -> Result<Vec<RunSummary>, RunStoreError>;
    fn get(&self, key: &RunKey) -> Result<Option<RunRecord>, RunStoreError>;
}

// ------------------------------------------------------------------------------------------------------------
// In-memory RunStore
// ------------------------------------------------------------------------------------------------------------

enum Entry {
    InProgress { attempt: u32, trading_day: NaiveDate, started_at: DateTime<Utc>, lease_until: DateTime<Utc>, journal: Vec<JournalEntry> },
    Done(Box<RunRecord>),
}

#[derive(Default)]
struct Inner {
    entries: BTreeMap<RunKey, Entry>,
    order: Vec<RunKey>,
    fail_next: u32,
}

#[derive(Default)]
pub struct InMemoryRunStore {
    inner: Mutex<Inner>,
}

impl InMemoryRunStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make the next `n` calls fail with `Unavailable`.
    pub fn fail_next_calls(&self, n: u32) {
        self.lock().fail_next = n;
    }

    /// Every finished record, oldest first.
    pub fn records(&self) -> Vec<RunRecord> {
        let g = self.lock();
        g.order
            .iter()
            .filter_map(|k| match g.entries.get(k) {
                Some(Entry::Done(r)) => Some((**r).clone()),
                _ => None,
            })
            .collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn check(g: &mut Inner) -> Result<(), RunStoreError> {
        if g.fail_next > 0 {
            g.fail_next -= 1;
            return Err(RunStoreError::Unavailable("injected failure".into()));
        }
        Ok(())
    }
}

impl RunStore for InMemoryRunStore {
    fn begin(&self, key: &RunKey, trading_day: NaiveDate, started_at: DateTime<Utc>, lease_secs: i64) -> Result<Begin, RunStoreError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let lease_until = started_at + Duration::seconds(lease_secs);
        match g.entries.get_mut(key) {
            None => {
                g.entries.insert(key.clone(), Entry::InProgress { attempt: 1, trading_day, started_at, lease_until, journal: Vec::new() });
                g.order.push(key.clone());
                Ok(Begin::Started { attempt: 1 })
            }
            Some(Entry::Done(r)) => Ok(Begin::AlreadyDone(r.clone())),
            Some(Entry::InProgress { attempt, started_at: prev_start, lease_until: prev_lease, trading_day: td, .. }) => {
                if started_at < *prev_lease {
                    return Ok(Begin::Busy { started_at: *prev_start });
                }
                *attempt += 1;
                *prev_start = started_at;
                *prev_lease = lease_until;
                *td = trading_day;
                Ok(Begin::Started { attempt: *attempt })
            }
        }
    }

    fn journal_order(&self, key: &RunKey, entry: JournalEntry) -> Result<(), RunStoreError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        match g.entries.get_mut(key) {
            Some(Entry::InProgress { journal, .. }) => {
                match journal.iter_mut().find(|j| j.tag == entry.tag) {
                    Some(existing) => *existing = entry,
                    None => journal.push(entry),
                }
                Ok(())
            }
            _ => Err(RunStoreError::NotStarted(key.canonical())),
        }
    }

    fn in_flight(&self, account_id: &str) -> Result<Vec<JournalEntry>, RunStoreError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let mut out = Vec::new();
        for k in g.order.iter().filter(|k| k.account_id == account_id) {
            if let Some(Entry::InProgress { journal, .. }) = g.entries.get(k) {
                out.extend(journal.iter().cloned());
            }
        }
        Ok(out)
    }

    fn finish(&self, record: RunRecord) -> Result<(), RunStoreError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let key = record.key.clone();
        match g.entries.get(&key) {
            Some(Entry::InProgress { .. }) => {
                g.entries.insert(key, Entry::Done(Box::new(record)));
                Ok(())
            }
            Some(Entry::Done(_)) => Err(RunStoreError::AlreadyFinished(key.canonical())),
            None => Err(RunStoreError::NotStarted(key.canonical())),
        }
    }

    fn last_snapshot(&self, account_id: &str) -> Result<Option<SnapshotSummary>, RunStoreError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let mut best: Option<(DateTime<Utc>, SnapshotSummary)> = None;
        for k in g.order.iter().filter(|k| k.account_id == account_id) {
            if let Some(Entry::Done(r)) = g.entries.get(k) {
                if let Some(s) = r.post_snapshot.clone().or_else(|| r.pre_snapshot.clone()) {
                    if best.as_ref().is_none_or(|(t, _)| r.finished_at >= *t) {
                        best = Some((r.finished_at, s));
                    }
                }
            }
        }
        Ok(best.map(|(_, s)| s))
    }

    fn known_order_ids(&self, account_id: &str) -> Result<BTreeSet<String>, RunStoreError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let mut ids = BTreeSet::new();
        for k in g.order.iter().filter(|k| k.account_id == account_id) {
            match g.entries.get(k) {
                Some(Entry::Done(r)) => ids.extend(r.placed.iter().filter_map(|p| p.broker_order_id.clone())),
                Some(Entry::InProgress { journal, .. }) => ids.extend(journal.iter().filter_map(|j| j.broker_order_id.clone())),
                None => {}
            }
        }
        Ok(ids)
    }

    fn day_counters(&self, account_id: &str, day: NaiveDate) -> Result<DayCounters, RunStoreError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let mut orders = 0u32;
        let mut turnover = Dec::ZERO;
        for k in g.order.iter().filter(|k| k.account_id == account_id) {
            match g.entries.get(k) {
                Some(Entry::Done(r)) if r.trading_day == day && r.mode == ExecutionMode::Live => {
                    for p in r.placed.iter().filter(|p| matches!(p.phase, Phase::Sells | Phase::Buys)) {
                        if p.broker_order_id.is_some() && p.outcome != PlacedOutcome::AdoptedExisting {
                            orders = orders.saturating_add(1);
                            let value = p.executed_quantity.checked_mul(p.price).unwrap_or(Dec::ZERO);
                            turnover = turnover.checked_add(value).unwrap_or(turnover);
                        }
                    }
                }
                Some(Entry::InProgress { trading_day, journal, .. }) if *trading_day == day => {
                    for j in journal {
                        orders = orders.saturating_add(1);
                        turnover = turnover.checked_add(j.notional).unwrap_or(turnover);
                    }
                }
                _ => {}
            }
        }
        Ok(DayCounters { orders_today: orders, turnover_today: turnover })
    }

    fn summaries(&self, account_id: &str) -> Result<Vec<RunSummary>, RunStoreError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let mut out = Vec::new();
        for k in g.order.iter().filter(|k| k.account_id == account_id) {
            match g.entries.get(k) {
                Some(Entry::Done(r)) => out.push(RunSummary {
                    key: k.clone(),
                    attempt: r.attempt,
                    scheduled_for: r.scheduled_for,
                    started_at: r.started_at,
                    finished_at: Some(r.finished_at),
                    outcome: Some((r.outcome.kind, r.outcome.code.clone())),
                }),
                Some(Entry::InProgress { attempt, started_at, .. }) => out.push(RunSummary {
                    key: k.clone(),
                    attempt: *attempt,
                    scheduled_for: k.scheduled_for,
                    started_at: *started_at,
                    finished_at: None,
                    outcome: None,
                }),
                None => {}
            }
        }
        Ok(out)
    }

    fn get(&self, key: &RunKey) -> Result<Option<RunRecord>, RunStoreError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        Ok(match g.entries.get(key) {
            Some(Entry::Done(r)) => Some((**r).clone()),
            _ => None,
        })
    }
}

// ------------------------------------------------------------------------------------------------------------
// Missed-run detection
// ------------------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissedKind {
    /// No run with this scheduled time exists at all.
    NeverStarted,
    /// A run started but never finished and its lease has expired: the process died.
    StuckInProgress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissedRun {
    pub scheduled_for: DateTime<Utc>,
    pub kind: MissedKind,
    /// Seconds since the slot's grace period ended.
    pub overdue_secs: i64,
}

/// Which expected slots were missed as of `now`? A slot is due `grace_secs` after its scheduled time. It is missed
/// when no run carries that scheduled time (`NeverStarted`), or when the run that does never finished within
/// `lease_secs` of starting (`StuckInProgress`). ANY finished run satisfies its slot, including one that refused or
/// failed closed: the dead-man's switch watches that the process runs; failures alert separately.
pub fn find_missed(expected: &[DateTime<Utc>], summaries: &[RunSummary], now: DateTime<Utc>, grace_secs: i64, lease_secs: i64) -> Vec<MissedRun> {
    let mut out = Vec::new();
    for slot in expected {
        let due = *slot + Duration::seconds(grace_secs);
        if now < due {
            continue;
        }
        let overdue = now.signed_duration_since(due).num_seconds();
        let mine: Vec<&RunSummary> = summaries.iter().filter(|s| s.scheduled_for == *slot).collect();
        if mine.is_empty() {
            out.push(MissedRun { scheduled_for: *slot, kind: MissedKind::NeverStarted, overdue_secs: overdue });
        } else if mine.iter().all(|s| s.finished_at.is_none() && now >= s.started_at + Duration::seconds(lease_secs)) {
            out.push(MissedRun { scheduled_for: *slot, kind: MissedKind::StuckInProgress, overdue_secs: overdue });
        }
    }
    out
}

// ------------------------------------------------------------------------------------------------------------
// Notifier and kill flag
// ------------------------------------------------------------------------------------------------------------

/// The alert sink. Real implementations send email / page; a failure to deliver is recorded in the run record and
/// must never stop the pipeline from failing closed.
pub trait Notifier {
    fn notify(&self, alert: &Alert) -> Result<(), String>;
}

/// The kill flag (a database row in production). `Err` means "could not read it": the run fails closed.
pub trait KillFlag {
    fn is_set(&self) -> Result<bool, String>;
}
