//! OANDA credentials: a bearer token plus the account id, each marked with the environment the
//! operator says they belong to.
//!
//! Auth is `Authorization: Bearer <token>` (VERIFIED-FROM-REPO-CODE: the legacy connector's
//! `AuthMethod::BearerToken` for OANDA; the account id rides in the URL path). There is no
//! request signing.
//!
//! FROM-MEMORY-OF-DOCS: v20 account ids look like `101-001-1234567-001`; fxTrade PRACTICE accounts
//! start `101-` and fxTrade live accounts start `001-`. Only a CONTRADICTION is refused (a live
//! mark with a `101-` id, a practice mark with a `001-` id); other prefixes are not judged.
//!
//! The token is a secret: [`OandaToken`] has no `Clone`, no `Display`, no `Serialize`, a redacted
//! `Debug`, and is zeroed best-effort on drop. Error texts in this module never contain it.

use crate::error::BrokerError;
use crate::oanda::config::Environment;
use sha2::{Digest, Sha256};
use std::fmt;

pub const AUTH_HEADER: &str = "Authorization";

/// Secret bearer token.
pub struct OandaToken(String);

impl fmt::Debug for OandaToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl Drop for OandaToken {
    fn drop(&mut self) {
        let mut bytes = std::mem::take(&mut self.0).into_bytes();
        bytes.fill(0);
        std::hint::black_box(&mut bytes);
    }
}

impl OandaToken {
    /// Refuses empty or non-header-safe (whitespace, control, non-ASCII) tokens. The error never
    /// contains the input.
    pub fn new(token: &str) -> Result<Self, BrokerError> {
        let t = token.trim();
        if t.is_empty() {
            return Err(BrokerError::Credentials("oanda token is empty".into()));
        }
        if !t.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(BrokerError::Credentials(
                "oanda token contains characters that are not valid in an HTTP header".into(),
            ));
        }
        Ok(Self(t.to_string()))
    }

    /// Stable non-secret identifier for correlating logs: first 8 bytes of SHA-256, hex.
    pub fn fingerprint(&self) -> String {
        let d = Sha256::digest(self.0.as_bytes());
        d[..8].iter().map(|b| format!("{b:02x}")).collect()
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

pub struct OandaCredentials {
    environment: Environment,
    token: OandaToken,
    account_id: String,
}

impl OandaCredentials {
    /// `environment` is the environment the operator says these credentials belong to.
    pub fn new(environment: Environment, token: &str, account_id: &str) -> Result<Self, BrokerError> {
        let token = OandaToken::new(token)?;
        let account_id = account_id.trim();
        if account_id.is_empty() {
            return Err(BrokerError::Credentials("oanda account id is empty".into()));
        }
        if account_id.len() > 32 || !account_id.bytes().all(|b| b.is_ascii_digit() || b == b'-') {
            return Err(BrokerError::Credentials(
                "oanda account id must be digits and hyphens only (for example 101-001-1234567-001)".into(),
            ));
        }
        match environment {
            Environment::Live if account_id.starts_with("101-") => {
                return Err(BrokerError::Credentials(
                    "credentials are marked live but the account id has the practice prefix (101-)".into(),
                ));
            }
            Environment::Practice if account_id.starts_with("001-") => {
                return Err(BrokerError::Credentials(
                    "credentials are marked practice but the account id has the live prefix (001-)".into(),
                ));
            }
            _ => {}
        }
        Ok(Self { environment, token, account_id: account_id.to_string() })
    }

    pub fn environment(&self) -> Environment {
        self.environment
    }

    /// The account id is an identifier, not a secret (it appears in every request path).
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn token_fingerprint(&self) -> String {
        self.token.fingerprint()
    }

    pub(crate) fn token(&self) -> &OandaToken {
        &self.token
    }

    pub(crate) fn headers(&self) -> Vec<(String, String)> {
        vec![(AUTH_HEADER.to_string(), format!("Bearer {}", self.token.expose()))]
    }
}

impl fmt::Debug for OandaCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OandaCredentials")
            .field("environment", &self.environment)
            .field("account_id", &self.account_id)
            .field("token", &self.token)
            .field("token_fingerprint", &self.token_fingerprint())
            .finish()
    }
}
