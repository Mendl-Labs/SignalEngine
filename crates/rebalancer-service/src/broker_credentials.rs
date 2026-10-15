//! The HTTP credential-broker client (certified-envelopes Phase 0): the [`PaperCredentialProvider`] for every
//! non-pilot account when `REBALANCER_SERVE_PROPOSAL_PLANS=true`.
//!
//! This process must never hold the tenant-credential master key (`crate::pilot::FORBIDDEN_ENV`, unchanged). The
//! BacktestingEngine API pod does hold it, and exposes `GET /internal/credentials/paper/{tenant}/{account}`,
//! gated by a shared bearer token, returning ONLY a paper Alpaca key pair. This client calls that endpoint and
//! hands the pair to the existing seam; the key then still goes through `connect_account_alpaca`, which builds the
//! paper-only adapter and runs the `PA` account check, so the broker is trusted for nothing the adapter does not
//! re-verify.
//!
//! Nothing here logs a key, the token or a response body. Every secret-holding type has a MANUAL redacting
//! `Debug` (never derive it on a type that holds a secret).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use broker_adapters::alpaca::PAPER_BASE_URL;
use broker_adapters::transport::{HttpMethod, HttpRequest, HttpTransport, TransportError};
use uuid::Uuid;

use crate::credentials::{CredentialError, PaperAlpacaKeys, PaperCredentialProvider};

pub const ENV_BROKER_URL: &str = "REBALANCER_CREDENTIAL_BROKER_URL";
pub const ENV_BROKER_TOKEN: &str = "CREDENTIAL_BROKER_SERVICE_TOKEN";

/// Stable `NotProvided` reason codes.
pub const BROKER_UNAUTHORIZED: &str = "BROKER_UNAUTHORIZED";
pub const BROKER_REFUSED: &str = "BROKER_REFUSED";
pub const BROKER_NOT_FOUND: &str = "BROKER_NOT_FOUND";
pub const BROKER_UNAVAILABLE: &str = "BROKER_UNAVAILABLE";
pub const BROKER_BAD_RESPONSE: &str = "BROKER_BAD_RESPONSE";
pub const BROKER_NOT_PAPER_URL: &str = "BROKER_NOT_PAPER_URL";
pub const BROKER_BAD_ID: &str = "BROKER_BAD_ID";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerConfigError {
    /// `REBALANCER_SERVE_PROPOSAL_PLANS=true` without the broker URL and/or token.
    MissingWithProposalPlans(&'static str),
    /// The URL is not an `http(s)://` base URL.
    BadUrl,
}

impl fmt::Display for BrokerConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrokerConfigError::MissingWithProposalPlans(name) => write!(
                f,
                "BROKER_CONFIG_MISSING: {name} must be set when REBALANCER_SERVE_PROPOSAL_PLANS=true (this process cannot decrypt tenant credentials itself)"
            ),
            BrokerConfigError::BadUrl => write!(f, "BROKER_CONFIG_BAD_URL: {ENV_BROKER_URL} must be an http(s) base URL"),
        }
    }
}

impl std::error::Error for BrokerConfigError {}

/// Broker location and bearer token. Manual redacting `Debug`: the token is a secret.
#[derive(Clone, PartialEq, Eq)]
pub struct BrokerConfig {
    url: String,
    token: String,
}

impl fmt::Debug for BrokerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerConfig").field("url", &self.url).field("token", &"<redacted>").finish()
    }
}

impl BrokerConfig {
    /// `Ok(None)` when proposal plans are not served (the broker is never used, so it is not required); an error
    /// when they are and the URL or token is missing or blank.
    pub fn from_lookup(serve_proposal_plans: bool, lookup: impl Fn(&str) -> Option<String>) -> Result<Option<Self>, BrokerConfigError> {
        if !serve_proposal_plans {
            return Ok(None);
        }
        let read = |name: &'static str| lookup(name).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()).ok_or(BrokerConfigError::MissingWithProposalPlans(name));
        let url = read(ENV_BROKER_URL)?;
        let token = read(ENV_BROKER_TOKEN)?;
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(BrokerConfigError::BadUrl);
        }
        Ok(Some(Self { url: url.trim_end_matches('/').to_string(), token }))
    }
}

/// Fetches each account's paper key pair from the credential broker.
pub struct HttpBrokerCredentials {
    config: BrokerConfig,
    transport: Arc<dyn HttpTransport>,
}

impl fmt::Debug for HttpBrokerCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpBrokerCredentials").field("config", &self.config).finish_non_exhaustive()
    }
}

impl HttpBrokerCredentials {
    pub fn new(config: BrokerConfig, transport: Arc<dyn HttpTransport>) -> Self {
        Self { config, transport }
    }
}

fn not_provided(tenant_id: &str, account_id: &str, reason: &'static str) -> CredentialError {
    CredentialError::NotProvided { tenant_id: tenant_id.to_string(), account_id: account_id.to_string(), reason }
}

impl PaperCredentialProvider for HttpBrokerCredentials {
    fn paper_alpaca_keys(&self, tenant_id: &str, account_id: &str) -> Result<PaperAlpacaKeys, CredentialError> {
        // Both ids go into a URL path: only canonical UUIDs are ever interpolated.
        let (Ok(t), Ok(a)) = (Uuid::parse_str(tenant_id.trim()), Uuid::parse_str(account_id.trim())) else {
            return Err(not_provided(tenant_id, account_id, BROKER_BAD_ID));
        };
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: format!("{}/internal/credentials/paper/{}/{}", self.config.url, t.hyphenated(), a.hyphenated()),
            headers: vec![
                ("Authorization".to_string(), format!("Bearer {}", self.config.token)),
                ("Accept".to_string(), "application/json".to_string()),
            ],
            body: None,
        };
        let resp = match self.transport.execute(&req) {
            Ok(r) => r,
            Err(TransportError::ConnectFailed(_) | TransportError::Timeout | TransportError::Io(_)) => {
                return Err(not_provided(tenant_id, account_id, BROKER_UNAVAILABLE));
            }
        };
        match resp.status {
            200 => {}
            401 => return Err(not_provided(tenant_id, account_id, BROKER_UNAUTHORIZED)),
            403 => return Err(not_provided(tenant_id, account_id, BROKER_REFUSED)),
            404 => return Err(not_provided(tenant_id, account_id, BROKER_NOT_FOUND)),
            _ => return Err(not_provided(tenant_id, account_id, BROKER_UNAVAILABLE)),
        }
        let parsed: BTreeMap<String, serde_json::Value> = match serde_json::from_str(&resp.body) {
            Ok(v) => v,
            Err(_) => return Err(not_provided(tenant_id, account_id, BROKER_BAD_RESPONSE)),
        };
        let text = |name: &str| parsed.get(name).and_then(|v| v.as_str()).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        // The broker is trusted for the key pair, not for the venue: anything but the paper host is refused here,
        // before the key can reach an adapter.
        match text("base_url") {
            Some(u) if u.trim_end_matches('/') == PAPER_BASE_URL.trim_end_matches('/') => {}
            _ => return Err(not_provided(tenant_id, account_id, BROKER_NOT_PAPER_URL)),
        }
        match (text("key_id"), text("secret")) {
            (Some(key_id), Some(secret)) => Ok(PaperAlpacaKeys { key_id, secret }),
            _ => Err(not_provided(tenant_id, account_id, BROKER_BAD_RESPONSE)),
        }
    }
}
