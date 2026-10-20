//! The read-only reader trait and its error type.

use crate::snapshot::{AccountRef, AccountSnapshot, Broker, Environment, OpenOrder, Position, SourceStatus};
use broker_adapters::{BrokerError, Dec, ErrorClass};
use chrono::{DateTime, Utc};

/// Account-level figures from one read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSummary {
    pub account: AccountRef,
    pub environment: Environment,
    pub currency: String,
    pub equity: Dec,
    pub cash: Dec,
}

/// Why a read failed. The variants are the states the dashboard must tell apart (R54-A4): a rejected key is not a
/// network blip, and neither is ever shown as a zero balance.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    /// No adapter for this venue yet. `pr` names the build-order step that adds it.
    #[error("{broker} reads are not supported yet ({pr})")]
    Unsupported { broker: &'static str, pr: &'static str },
    /// 401/403 or an auth-class exchange error. Never retried.
    #[error("the broker rejected the key: {0}")]
    KeyRejected(String),
    /// 429. Honour `retry_after_secs` when present.
    #[error("rate limited (retry after {retry_after_secs:?}s)")]
    RateLimited { retry_after_secs: Option<u64> },
    /// Network failure, timeout or a 5xx.
    #[error("broker unreachable: {0}")]
    Unreachable(String),
    /// The broker answered with something we could not parse.
    #[error("malformed broker response: {0}")]
    Malformed(String),
    #[error("read failed: {0}")]
    Other(String),
}

impl ReadError {
    /// Map an adapter error. Error text from the adapters is already redacted by them; nothing is added here.
    pub fn from_broker_error(e: BrokerError) -> Self {
        match e {
            BrokerError::RateLimited { retry_after_secs, .. } => ReadError::RateLimited { retry_after_secs },
            BrokerError::Http(s) if s == 401 || s == 403 => ReadError::KeyRejected(format!("HTTP {s}")),
            BrokerError::Http(s) => ReadError::Unreachable(format!("HTTP {s}")),
            BrokerError::Transport(t) => ReadError::Unreachable(t.to_string()),
            BrokerError::Malformed(m) => ReadError::Malformed(m),
            BrokerError::Exchange(errs) => {
                let codes = errs.iter().map(|x| x.code.as_str()).collect::<Vec<_>>().join(", ");
                if errs.iter().any(|x| x.class == ErrorClass::Auth) {
                    ReadError::KeyRejected(codes)
                } else if errs.iter().any(|x| x.class == ErrorClass::RateLimited) {
                    ReadError::RateLimited { retry_after_secs: None }
                } else if errs.iter().any(|x| x.class == ErrorClass::ServiceUnavailable) {
                    ReadError::Unreachable(codes)
                } else {
                    ReadError::Other(codes)
                }
            }
            other => ReadError::Other(other.to_string()),
        }
    }
}

/// Read access to one broker account. THE TRAIT HAS NO WRITE METHODS, by design (R54-A1): an implementation cannot
/// be asked to change an order through it.
pub trait BrokerReader: Send + Sync {
    fn broker(&self) -> Broker;

    fn read_account_summary(&self) -> Result<AccountSummary, ReadError>;

    fn read_positions(&self) -> Result<Vec<Position>, ReadError>;

    fn read_open_orders(&self) -> Result<Vec<OpenOrder>, ReadError>;

    /// One full read cycle (three reads). `now` is injected so tests and the scheduler own the clock.
    fn read_snapshot(&self, now: DateTime<Utc>) -> Result<AccountSnapshot, ReadError> {
        let summary = self.read_account_summary()?;
        let positions = self.read_positions()?;
        let open_orders = self.read_open_orders()?;
        Ok(build_snapshot(self.broker(), summary, positions, open_orders, now))
    }
}

/// Assemble a snapshot from the three reads. Positions are sorted by symbol so equal accounts compare equal.
/// A position without a price or a value makes the snapshot `Partial` with a note; it is never defaulted to zero.
pub fn build_snapshot(
    broker: Broker,
    summary: AccountSummary,
    mut positions: Vec<Position>,
    open_orders: Vec<OpenOrder>,
    taken_at: DateTime<Utc>,
) -> AccountSnapshot {
    positions.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    let notes: Vec<String> = positions
        .iter()
        .filter(|p| p.price.is_none() || p.value.is_none())
        .map(|p| format!("{}: price or value not reported by the broker", p.symbol))
        .collect();
    let source_status = if notes.is_empty() { SourceStatus::Complete } else { SourceStatus::Partial { notes } };
    AccountSnapshot {
        broker,
        account: summary.account,
        environment: summary.environment,
        currency: summary.currency.to_ascii_uppercase(),
        equity: summary.equity,
        cash: summary.cash,
        positions,
        open_orders,
        taken_at,
        source_status,
    }
}
