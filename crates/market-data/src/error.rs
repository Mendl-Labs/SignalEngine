//! The failure taxonomy. Every way a Massive fetch can fail is one [`MassiveError`] variant, so a decorator (the future
//! two-source gate, COUNCIL_DATA_GATE R18-R19) can branch on the CLASS of failure instead of parsing text:
//!
//! | kind                  | code                       | class         | meaning |
//! |-----------------------|----------------------------|---------------|---------|
//! | `Unavailable`         | `DATA_UNAVAILABLE`         | Transient     | transport failure / timeout / 5xx, after the bounded retries |
//! | `RateLimited`         | `DATA_RATE_LIMITED`        | Transient     | HTTP 429 after the retries, or the local per-tick request budget is spent |
//! | `NotAuthorized`       | `DATA_NOT_AUTHORIZED`      | Deterministic | HTTP 403 (either body shape) or 401: an entitlement/key problem, never retried |
//! | `Malformed`           | `DATA_MALFORMED`           | Deterministic | the response is not what the API documents (bad JSON, wrong symbol, bad dates or closes, ...), never retried |
//! | `MissingBar`          | `DATA_MISSING_BAR`         | Deterministic | a bar that must exist inside the decision window is absent |
//! | `StaleData`           | `DATA_STALE`               | Settling      | the newest complete bar is older than the run date allows (the vendor has not published it yet) |
//! | `InsufficientHistory` | `DATA_INSUFFICIENT_HISTORY`| Deterministic | fewer bars (or month-ends) than the rule needs |
//! | `Unsupported`         | `DATA_UNSUPPORTED`         | Deterministic | the request is outside what this source does (quote currency, prices, a date it cannot compute with) |
//!
//! Classes follow COUNCIL_DATA_GATE R19: TRANSIENT retries on later ticks inside the retry window, SETTLING is a
//! disagreement/absence confined to the newest bar and retries too, DETERMINISTIC does not retry against the vendor.
//! Deciding WHEN to retry (the window across ticks) is not this crate's job; it only says which class an error is.

use chrono::NaiveDate;
use rebalancer_run::data::{DataError, SleeveKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    Unavailable,
    RateLimited,
    NotAuthorized,
    Malformed,
    MissingBar,
    StaleData,
    InsufficientHistory,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailureClass {
    Transient,
    Settling,
    Deterministic,
}

impl ErrorKind {
    /// The stable `DataError::code` string.
    pub fn code(self) -> &'static str {
        match self {
            ErrorKind::Unavailable => "DATA_UNAVAILABLE",
            ErrorKind::RateLimited => "DATA_RATE_LIMITED",
            ErrorKind::NotAuthorized => "DATA_NOT_AUTHORIZED",
            ErrorKind::Malformed => "DATA_MALFORMED",
            ErrorKind::MissingBar => "DATA_MISSING_BAR",
            ErrorKind::StaleData => "DATA_STALE",
            ErrorKind::InsufficientHistory => "DATA_INSUFFICIENT_HISTORY",
            ErrorKind::Unsupported => "DATA_UNSUPPORTED",
        }
    }

    pub fn class(self) -> FailureClass {
        match self {
            ErrorKind::Unavailable | ErrorKind::RateLimited => FailureClass::Transient,
            ErrorKind::StaleData => FailureClass::Settling,
            ErrorKind::NotAuthorized
            | ErrorKind::Malformed
            | ErrorKind::MissingBar
            | ErrorKind::InsufficientHistory
            | ErrorKind::Unsupported => FailureClass::Deterministic,
        }
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum MassiveError {
    #[error("vendor unavailable after {attempts} attempt(s): {detail}")]
    Unavailable { detail: String, attempts: u32 },
    /// `local_budget`: the source's own per-tick request budget ran out (no request was sent for the refused call);
    /// otherwise the vendor answered 429 on every attempt.
    #[error("rate limited after {attempts} attempt(s) (local per-tick budget exhausted: {local_budget})")]
    RateLimited { attempts: u32, local_budget: bool },
    #[error("not authorized (HTTP {status}): {detail}")]
    NotAuthorized { status: u16, detail: String },
    #[error("malformed response for {instrument}: {detail}")]
    Malformed { instrument: String, detail: String },
    #[error("missing bar: {symbol} has no bar dated {date}")]
    MissingBar { symbol: String, date: NaiveDate },
    #[error("stale data for {symbol}: newest complete bar {newest:?}, run date {as_of}: {detail}")]
    StaleData { symbol: String, newest: Option<NaiveDate>, as_of: NaiveDate, detail: String },
    #[error("insufficient history for {symbol}: need {needed}, have {have}")]
    InsufficientHistory { symbol: String, needed: usize, have: usize },
    #[error("unsupported: {detail}")]
    Unsupported { detail: String },
}

impl MassiveError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            MassiveError::Unavailable { .. } => ErrorKind::Unavailable,
            MassiveError::RateLimited { .. } => ErrorKind::RateLimited,
            MassiveError::NotAuthorized { .. } => ErrorKind::NotAuthorized,
            MassiveError::Malformed { .. } => ErrorKind::Malformed,
            MassiveError::MissingBar { .. } => ErrorKind::MissingBar,
            MassiveError::StaleData { .. } => ErrorKind::StaleData,
            MassiveError::InsufficientHistory { .. } => ErrorKind::InsufficientHistory,
            MassiveError::Unsupported { .. } => ErrorKind::Unsupported,
        }
    }

    pub fn class(&self) -> FailureClass {
        self.kind().class()
    }

    /// The instrument the failure is about, when it is about one.
    pub fn instrument(&self) -> Option<&str> {
        match self {
            MassiveError::Malformed { instrument, .. } => Some(instrument),
            MassiveError::MissingBar { symbol, .. }
            | MassiveError::StaleData { symbol, .. }
            | MassiveError::InsufficientHistory { symbol, .. } => Some(symbol),
            _ => None,
        }
    }
}

impl From<MassiveError> for DataError {
    fn from(e: MassiveError) -> Self {
        DataError::new(e.kind().code(), &e.to_string())
    }
}

/// A failure scoped to ONE sleeve fetch: which kind of sleeve, which run date, what went wrong. This is the typed
/// error of [`crate::SleeveFetcher`]; a decorator keeps it, and the pipeline's `DataError` is derived from it.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{} sleeve as of {as_of}: {error}", .sleeve.as_str())]
pub struct SleeveError {
    pub sleeve: SleeveKind,
    pub as_of: NaiveDate,
    pub error: MassiveError,
}

impl SleeveError {
    pub fn kind(&self) -> ErrorKind {
        self.error.kind()
    }
    pub fn class(&self) -> FailureClass {
        self.error.class()
    }
}

impl From<SleeveError> for DataError {
    fn from(e: SleeveError) -> Self {
        DataError::new(e.error.kind().code(), &e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_and_classes_are_stable() {
        let all = [
            (ErrorKind::Unavailable, "DATA_UNAVAILABLE", FailureClass::Transient),
            (ErrorKind::RateLimited, "DATA_RATE_LIMITED", FailureClass::Transient),
            (ErrorKind::NotAuthorized, "DATA_NOT_AUTHORIZED", FailureClass::Deterministic),
            (ErrorKind::Malformed, "DATA_MALFORMED", FailureClass::Deterministic),
            (ErrorKind::MissingBar, "DATA_MISSING_BAR", FailureClass::Deterministic),
            (ErrorKind::StaleData, "DATA_STALE", FailureClass::Settling),
            (ErrorKind::InsufficientHistory, "DATA_INSUFFICIENT_HISTORY", FailureClass::Deterministic),
            (ErrorKind::Unsupported, "DATA_UNSUPPORTED", FailureClass::Deterministic),
        ];
        for (k, code, class) in all {
            assert_eq!(k.code(), code);
            assert_eq!(k.class(), class);
        }
    }

    #[test]
    fn data_error_carries_the_code() {
        let e: DataError = MassiveError::MissingBar { symbol: "SPY".into(), date: NaiveDate::from_ymd_opt(2026, 1, 2).unwrap() }.into();
        assert_eq!(e.code, "DATA_MISSING_BAR");
        assert!(e.message.contains("SPY") && e.message.contains("2026-01-02"));
    }
}
