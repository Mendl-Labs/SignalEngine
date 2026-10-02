//! The delivery ledger seam: what the worker reads from `rebalancer_alerts` / `alert_channels` and writes to
//! `alert_deliveries`, as a trait, plus an in-memory implementation for tests.
//!
//! The table shapes are a contract with `databaseschema-internal` (`feat/w6-1-alert-channels`):
//! `alert_channels(id, scope 'tenant'|'platform', tenant_id (null when platform), kind, address,
//! verification_token_hash, verified_at, disabled_at, created_at)` and `alert_deliveries(id, scope, tenant_id,
//! channel_id, source, severity, dedupe_key, subject, body, sent_at, provider_message_id, error, created_at;
//! UNIQUE (channel_id, dedupe_key))`. Nothing here renames them.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

use chrono::{DateTime, Utc};

/// `alert_deliveries.source` for everything this crate writes.
pub const SOURCE_REBALANCER: &str = "rebalancer";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChannelScope {
    Tenant,
    Platform,
}

impl ChannelScope {
    pub fn as_str(self) -> &'static str {
        match self {
            ChannelScope::Tenant => "tenant",
            ChannelScope::Platform => "platform",
        }
    }

    pub fn parse(s: &str) -> Option<ChannelScope> {
        match s.trim().to_ascii_lowercase().as_str() {
            "tenant" => Some(ChannelScope::Tenant),
            "platform" => Some(ChannelScope::Platform),
            _ => None,
        }
    }
}

/// One row of `rebalancer_alerts` as the worker reads it. Ids and tenant ids are text (the hyphenated UUID) so this
/// crate needs no uuid dependency; the Postgres ledger casts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAlert {
    pub id: String,
    /// `None` when the row carries the nil tenant (an alert about the platform itself, e.g. the heartbeat), which
    /// matches no tenant channel.
    pub tenant_id: Option<String>,
    pub account_id: String,
    pub code: String,
    pub severity: String,
    pub message: String,
    pub run_key: String,
    pub dedupe_key: String,
    pub at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// A VERIFIED, ENABLED channel (the ledger filters `verified_at IS NOT NULL AND disabled_at IS NULL` itself).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Channel {
    pub id: String,
    pub scope: ChannelScope,
    pub tenant_id: Option<String>,
    /// `email` today.
    pub kind: String,
    pub address: String,
}

/// What the worker asks the ledger to insert (if absent) for one (alert, channel) pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDelivery {
    pub channel_id: String,
    pub scope: ChannelScope,
    pub tenant_id: Option<String>,
    pub severity: String,
    pub dedupe_key: String,
    pub subject: String,
    pub body: String,
}

/// A delivery row with neither `sent_at` nor `error`: the sender has not been asked yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDelivery {
    pub id: String,
    pub channel_id: String,
    pub kind: String,
    pub address: String,
    pub subject: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerError(pub String);

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALERT_STORE_UNAVAILABLE: {}", self.0)
    }
}

impl std::error::Error for LedgerError {}

/// The ledger seam. Every method fails CLOSED (`Err`, never a silently-empty success) when the store cannot answer,
/// including when the tables do not exist yet.
pub trait DeliveryLedger {
    /// Alerts created strictly after `since`, oldest first, at most `limit`.
    fn alerts_since(&self, since: DateTime<Utc>, limit: usize) -> Result<Vec<PendingAlert>, LedgerError>;
    /// Every verified, enabled channel.
    fn channels(&self) -> Result<Vec<Channel>, LedgerError>;
    /// `INSERT ... ON CONFLICT (channel_id, dedupe_key) DO NOTHING`: `Ok(true)` when a row was created, `Ok(false)`
    /// when one already existed (the de-duplication).
    fn insert_if_absent(&self, delivery: &NewDelivery, now: DateTime<Utc>) -> Result<bool, LedgerError>;
    /// Delivery rows of this crate's source with neither `sent_at` nor `error`, oldest first, at most `limit`.
    fn undelivered(&self, limit: usize) -> Result<Vec<PendingDelivery>, LedgerError>;
    /// Mark a delivery sent. Only a row with neither `sent_at` nor `error` is touched (a row is marked once).
    fn mark_sent(&self, id: &str, provider_message_id: &str, at: DateTime<Utc>) -> Result<(), LedgerError>;
    /// Mark a delivery failed. Same once-only rule.
    fn mark_error(&self, id: &str, error: &str) -> Result<(), LedgerError>;
}

// ---------------------------------------------------------------------------------------------------------------
// In-memory ledger (tests, and the honest "what the Postgres ledger must do" reference)
// ---------------------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDelivery {
    pub id: String,
    pub delivery: NewDelivery,
    pub created_at: DateTime<Utc>,
    pub sent_at: Option<DateTime<Utc>>,
    pub provider_message_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Default)]
struct Inner {
    alerts: Vec<PendingAlert>,
    channels: Vec<Channel>,
    deliveries: Vec<StoredDelivery>,
    by_key: BTreeMap<(String, String), usize>,
    next_id: u64,
    fail_next: u32,
    skip_before_fail: u32,
}

/// An in-memory [`DeliveryLedger`] with the same semantics the Postgres one must have (unique key, once-only marks,
/// injectable failure).
#[derive(Default)]
pub struct InMemoryLedger {
    inner: Mutex<Inner>,
}

impl InMemoryLedger {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn check(g: &mut Inner) -> Result<(), LedgerError> {
        if g.skip_before_fail > 0 {
            g.skip_before_fail -= 1;
            return Ok(());
        }
        if g.fail_next > 0 {
            g.fail_next -= 1;
            return Err(LedgerError("injected failure".into()));
        }
        Ok(())
    }

    pub fn fail_next_calls(&self, n: u32) {
        self.lock().fail_next = n;
    }

    /// Let the next `after` calls succeed, then fail the following `n`.
    pub fn fail_after(&self, after: u32, n: u32) {
        let mut g = self.lock();
        g.skip_before_fail = after;
        g.fail_next = n;
    }

    pub fn add_alert(&self, a: PendingAlert) {
        self.lock().alerts.push(a);
    }

    pub fn add_channel(&self, c: Channel) {
        self.lock().channels.push(c);
    }

    pub fn deliveries(&self) -> Vec<StoredDelivery> {
        self.lock().deliveries.clone()
    }
}

impl DeliveryLedger for InMemoryLedger {
    fn alerts_since(&self, since: DateTime<Utc>, limit: usize) -> Result<Vec<PendingAlert>, LedgerError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let mut out: Vec<PendingAlert> = g.alerts.iter().filter(|a| a.created_at > since).cloned().collect();
        out.sort_by(|a, b| a.created_at.cmp(&b.created_at).then_with(|| a.id.cmp(&b.id)));
        out.truncate(limit);
        Ok(out)
    }

    fn channels(&self) -> Result<Vec<Channel>, LedgerError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        Ok(g.channels.clone())
    }

    fn insert_if_absent(&self, delivery: &NewDelivery, now: DateTime<Utc>) -> Result<bool, LedgerError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let key = (delivery.channel_id.clone(), delivery.dedupe_key.clone());
        if g.by_key.contains_key(&key) {
            return Ok(false);
        }
        g.next_id += 1;
        let id = format!("d{}", g.next_id);
        let idx = g.deliveries.len();
        g.deliveries.push(StoredDelivery { id, delivery: delivery.clone(), created_at: now, sent_at: None, provider_message_id: None, error: None });
        g.by_key.insert(key, idx);
        Ok(true)
    }

    fn undelivered(&self, limit: usize) -> Result<Vec<PendingDelivery>, LedgerError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        let channels = g.channels.clone();
        let out = g
            .deliveries
            .iter()
            .filter(|d| d.sent_at.is_none() && d.error.is_none())
            .filter_map(|d| {
                let c = channels.iter().find(|c| c.id == d.delivery.channel_id)?;
                Some(PendingDelivery {
                    id: d.id.clone(),
                    channel_id: c.id.clone(),
                    kind: c.kind.clone(),
                    address: c.address.clone(),
                    subject: d.delivery.subject.clone(),
                    body: d.delivery.body.clone(),
                })
            })
            .take(limit)
            .collect();
        Ok(out)
    }

    fn mark_sent(&self, id: &str, provider_message_id: &str, at: DateTime<Utc>) -> Result<(), LedgerError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        if let Some(d) = g.deliveries.iter_mut().find(|d| d.id == id && d.sent_at.is_none() && d.error.is_none()) {
            d.sent_at = Some(at);
            d.provider_message_id = Some(provider_message_id.to_string());
        }
        Ok(())
    }

    fn mark_error(&self, id: &str, error: &str) -> Result<(), LedgerError> {
        let mut g = self.lock();
        Self::check(&mut g)?;
        if let Some(d) = g.deliveries.iter_mut().find(|d| d.id == id && d.sent_at.is_none() && d.error.is_none()) {
            d.error = Some(error.to_string());
        }
        Ok(())
    }
}
