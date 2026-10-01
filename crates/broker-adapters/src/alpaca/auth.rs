//! Alpaca credentials: header auth with `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY`
//! (VERIFIED-FROM-REPO-CODE: SignalEngine `alpaca_paper_definition` and `ApiKeyHeaderAuth`).
//! There is no request signing.

use crate::alpaca::config::Environment;
use crate::error::BrokerError;
use sha2::{Digest, Sha256};
use std::fmt;

pub const KEY_HEADER: &str = "APCA-API-KEY-ID";
pub const SECRET_HEADER: &str = "APCA-API-SECRET-KEY";

/// Secret text. No `Clone`/`Display`/`Serialize`; `Debug` is redacted; best-effort wipe on drop.
struct SecretText(String);

impl fmt::Debug for SecretText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl Drop for SecretText {
    fn drop(&mut self) {
        let mut bytes = std::mem::take(&mut self.0).into_bytes();
        bytes.fill(0);
        std::hint::black_box(&mut bytes);
    }
}

/// Key id + secret, each marked with the environment the operator says they belong to.
pub struct AlpacaCredentials {
    environment: Environment,
    key_id: String,
    secret: SecretText,
}

fn header_safe(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_graphic())
}

impl AlpacaCredentials {
    /// Refuses empty or non-header-safe values, and a key id whose prefix contradicts the mark:
    /// paper keys start with `PK` and live keys with `AK` (FROM-MEMORY-OF-DOCS; unknown prefixes
    /// are allowed, only the contradiction is refused). The error text never contains the input.
    pub fn new(environment: Environment, key_id: &str, secret: &str) -> Result<Self, BrokerError> {
        let key_id = key_id.trim();
        let secret = secret.trim();
        if key_id.is_empty() {
            return Err(BrokerError::Credentials("alpaca key id is empty".into()));
        }
        if secret.is_empty() {
            return Err(BrokerError::Credentials("alpaca secret is empty".into()));
        }
        if !header_safe(key_id) || !header_safe(secret) {
            return Err(BrokerError::Credentials(
                "alpaca key id / secret contain characters that are not valid in an HTTP header".into(),
            ));
        }
        match environment {
            Environment::Live if key_id.starts_with("PK") => {
                return Err(BrokerError::Credentials(
                    "credentials are marked live but the key id has the paper prefix (PK)".into(),
                ));
            }
            Environment::Paper if key_id.starts_with("AK") => {
                return Err(BrokerError::Credentials(
                    "credentials are marked paper but the key id has the live prefix (AK)".into(),
                ));
            }
            _ => {}
        }
        Ok(Self { environment, key_id: key_id.to_string(), secret: SecretText(secret.to_string()) })
    }

    pub fn environment(&self) -> Environment {
        self.environment
    }

    /// Stable, non-secret identifier for correlating logs (first 8 bytes of SHA-256(key id), hex).
    pub fn key_id_fingerprint(&self) -> String {
        let d = Sha256::digest(self.key_id.as_bytes());
        d[..8].iter().map(|b| format!("{b:02x}")).collect()
    }

    pub(crate) fn headers(&self) -> Vec<(String, String)> {
        vec![
            (KEY_HEADER.to_string(), self.key_id.clone()),
            (SECRET_HEADER.to_string(), self.secret.0.clone()),
        ]
    }
}

impl fmt::Debug for AlpacaCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlpacaCredentials")
            .field("environment", &self.environment)
            .field("key_id", &"<redacted>")
            .field("key_id_fingerprint", &self.key_id_fingerprint())
            .field("secret", &self.secret)
            .finish()
    }
}
