//! Exchange-key permission check and test-before-store gate.
//!
//! A key is accepted for storage only when the venue's own read-only endpoint PROVES two things:
//! trading is enabled and withdrawal is disabled. Every missing or unknown value is a refusal
//! (fail closed). A venue with no read-only permission endpoint refuses, and says so; nothing is
//! guessed from unrelated fields.
//!
//! The gate never sees key material in its errors: refusal text is fixed, and venue error text
//! comes from adapters that already redact credentials.

use crate::error::BrokerError;
use crate::types::BrokerAdapter;
use std::fmt;

/// What a venue reported about one key. `None` means the venue did not report the field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyPermissions {
    /// `Some(true)` only when the venue says trading is enabled for this key or account.
    pub trading_enabled: Option<bool>,
    /// `Some(false)` only when the venue says withdrawal is disabled for this key.
    pub withdrawal_enabled: Option<bool>,
}

/// Why a key was refused. Carries no key material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRefusal {
    /// The venue says withdrawal is enabled.
    WithdrawalEnabled,
    /// The venue did not report whether withdrawal is enabled.
    WithdrawalNotReported,
    /// The venue says trading is disabled.
    TradingDisabled,
    /// The venue did not report whether trading is enabled.
    TradingNotReported,
}

impl fmt::Display for KeyRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            KeyRefusal::WithdrawalEnabled => "withdrawal is enabled on this key",
            KeyRefusal::WithdrawalNotReported => "the venue did not report withdrawal permission (fail closed)",
            KeyRefusal::TradingDisabled => "trading is not enabled on this key",
            KeyRefusal::TradingNotReported => "the venue did not report trading permission (fail closed)",
        })
    }
}

/// Pure check. Withdrawal is checked first: a key that can withdraw is the most dangerous refusal.
pub fn check_key_permissions(p: &KeyPermissions) -> Result<(), KeyRefusal> {
    match p.withdrawal_enabled {
        Some(false) => {}
        Some(true) => return Err(KeyRefusal::WithdrawalEnabled),
        None => return Err(KeyRefusal::WithdrawalNotReported),
    }
    match p.trading_enabled {
        Some(true) => Ok(()),
        Some(false) => Err(KeyRefusal::TradingDisabled),
        None => Err(KeyRefusal::TradingNotReported),
    }
}

/// Why the gate did not store the key.
#[derive(Debug)]
pub enum KeyGateError {
    /// The venue's permissions failed the check. Nothing was stored.
    Refused { venue: &'static str, reason: KeyRefusal },
    /// The venue's read-only permission call failed (or the venue has none). Nothing was stored.
    ProbeFailed { venue: &'static str, detail: String },
    /// The check passed but the caller's store step failed.
    StoreFailed { detail: String },
}

impl fmt::Display for KeyGateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyGateError::Refused { venue, reason } => {
                write!(f, "exchange key refused by {venue}: {reason}; nothing was stored")
            }
            KeyGateError::ProbeFailed { venue, detail } => {
                write!(f, "exchange key not stored: {venue} permission check failed ({detail}); nothing was stored")
            }
            KeyGateError::StoreFailed { detail } => write!(f, "exchange key check passed but storing failed: {detail}"),
        }
    }
}

impl std::error::Error for KeyGateError {}

/// Test-before-store. Reads the venue's permissions through `broker`, runs the check, and calls
/// `store` ONLY if the check passes. A refused or failed check never calls `store`.
pub fn test_before_store<B, T, E, F>(broker: &B, store: F) -> Result<T, KeyGateError>
where
    B: BrokerAdapter + ?Sized,
    E: fmt::Display,
    F: FnOnce() -> Result<T, E>,
{
    let venue = broker.broker_name();
    let perms = broker
        .read_key_permissions()
        .map_err(|e: BrokerError| KeyGateError::ProbeFailed { venue, detail: e.to_string() })?;
    check_key_permissions(&perms).map_err(|reason| KeyGateError::Refused { venue, reason })?;
    store().map_err(|e| KeyGateError::StoreFailed { detail: e.to_string() })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(trading: Option<bool>, withdrawal: Option<bool>) -> KeyPermissions {
        KeyPermissions { trading_enabled: trading, withdrawal_enabled: withdrawal }
    }

    #[test]
    fn all_clear_passes() {
        assert_eq!(check_key_permissions(&p(Some(true), Some(false))), Ok(()));
    }

    #[test]
    fn withdrawal_enabled_is_refused() {
        assert_eq!(check_key_permissions(&p(Some(true), Some(true))), Err(KeyRefusal::WithdrawalEnabled));
    }

    #[test]
    fn trading_disabled_is_refused() {
        assert_eq!(check_key_permissions(&p(Some(false), Some(false))), Err(KeyRefusal::TradingDisabled));
    }

    #[test]
    fn missing_withdrawal_field_is_refused() {
        assert_eq!(check_key_permissions(&p(Some(true), None)), Err(KeyRefusal::WithdrawalNotReported));
    }

    #[test]
    fn missing_trading_field_is_refused() {
        assert_eq!(check_key_permissions(&p(None, Some(false))), Err(KeyRefusal::TradingNotReported));
    }

    #[test]
    fn both_missing_refuses_on_withdrawal_first() {
        assert_eq!(check_key_permissions(&p(None, None)), Err(KeyRefusal::WithdrawalNotReported));
    }
}
