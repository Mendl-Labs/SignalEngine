//! The paper-credential seam (certified-envelopes Phase 0): WHERE the service gets an account's paper Alpaca key
//! from, typed, so the per-account runtime construction in `main.rs` never reads a key itself.
//!
//! # Today's one implementation, and the design question the flag is blocked on
//! The pilot's key comes from the process environment (`PILOT_ALPACA_KEY_ID` / `PILOT_ALPACA_KEY_SECRET`, a
//! Kubernetes secret; `PAPER_PILOT_DRAGONSTONE_PLAN.md` "paper key from a Kubernetes secret instead of the tenant
//! credential store"). [`EnvPilotCredentials`] serves exactly the pilot pair from it.
//!
//! A PROPOSAL-served account's key lives in `exchange_credentials.api_key_encrypted` / `api_secret_encrypted`,
//! decryptable only with the tenant-credential master key. The pilot design forbids this process to hold that key
//! (`crate::pilot::FORBIDDEN_ENV`: the service refuses to START if it is present; `tests/pilot_source_scan.rs`
//! fails if the source ever reads it; the service's database role has no SELECT on the `*_encrypted` columns).
//! Decrypting tenant credentials inside the rebalancer would therefore reverse three deliberate layers of the
//! paper-only interlock, and this PR does not do that silently: [`TenantCredentialsNotBuilt`] is the typed
//! placeholder behind the seam, so with `REBALANCER_SERVE_PROPOSAL_PLANS=true` every non-pilot account is
//! enumerated, logged with `CREDENTIALS_NOT_PROVIDED`, and gets NO runtime (the driver records `DRIVER_NO_RUNTIME`
//! each tick; nothing is sized, nothing is placed). The owner decision needed before the flag can serve a proposal
//! account is recorded in the PR body (a sidecar/secret-per-tenant, a credential-broker service, or a scoped role
//! plus the master key in a separate pod).

use std::fmt;

use crate::pilot::PilotPair;

/// Read by both the service and `print-fingerprint`, never logged and never read from a secrets file.
pub const ENV_ALPACA_KEY_ID: &str = "PILOT_ALPACA_KEY_ID";
pub const ENV_ALPACA_KEY_SECRET: &str = "PILOT_ALPACA_KEY_SECRET";

/// One account's paper Alpaca key pair. `Debug` is redacted on purpose: this type can be logged by accident, the
/// key must not be.
#[derive(Clone, PartialEq, Eq)]
pub struct PaperAlpacaKeys {
    pub key_id: String,
    pub secret: String,
}

impl fmt::Debug for PaperAlpacaKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PaperAlpacaKeys {{ key_id: <redacted {} chars>, secret: <redacted> }}", self.key_id.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// A required environment variable is unset or blank (the pilot's own key).
    Missing(&'static str),
    /// No provider can supply this account's key. `reason` is a stable code.
    NotProvided { tenant_id: String, account_id: String, reason: &'static str },
}

impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CredentialError::Missing(name) => write!(f, "CREDENTIALS_MISSING: {name} is not set"),
            CredentialError::NotProvided { tenant_id, account_id, reason } => {
                write!(f, "CREDENTIALS_NOT_PROVIDED: no paper credential provider serves tenant {tenant_id} account {account_id} ({reason})")
            }
        }
    }
}

impl std::error::Error for CredentialError {}

/// Where an account's paper Alpaca key comes from. Never the master key, never a live key (every key still goes
/// through `crate::pilot::build_paper_alpaca`, which refuses anything but a `PK` key on the paper host).
pub trait PaperCredentialProvider {
    fn paper_alpaca_keys(&self, tenant_id: &str, account_id: &str) -> Result<PaperAlpacaKeys, CredentialError>;
}

/// The pilot pair's key from the process environment. Serves ONLY that pair.
pub struct EnvPilotCredentials {
    pair: PilotPair,
    keys: PaperAlpacaKeys,
}

impl EnvPilotCredentials {
    pub fn from_lookup(pair: PilotPair, lookup: impl Fn(&str) -> Option<String>) -> Result<Self, CredentialError> {
        let read = |name: &'static str| lookup(name).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()).ok_or(CredentialError::Missing(name));
        Ok(Self { pair, keys: PaperAlpacaKeys { key_id: read(ENV_ALPACA_KEY_ID)?, secret: read(ENV_ALPACA_KEY_SECRET)? } })
    }

    pub fn pair(&self) -> &PilotPair {
        &self.pair
    }
}

/// Manual, redacting: the pair is not secret, the keys are. Never derive `Debug` on a type holding a secret.
impl fmt::Debug for EnvPilotCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvPilotCredentials").field("pair", &self.pair).field("keys", &self.keys).finish()
    }
}

fn same_id(a: &str, b: &str) -> bool {
    a.trim().eq_ignore_ascii_case(b.trim())
}

impl PaperCredentialProvider for EnvPilotCredentials {
    fn paper_alpaca_keys(&self, tenant_id: &str, account_id: &str) -> Result<PaperAlpacaKeys, CredentialError> {
        if same_id(tenant_id, &self.pair.tenant_id) && same_id(account_id, &self.pair.account_id) {
            Ok(self.keys.clone())
        } else {
            Err(CredentialError::NotProvided { tenant_id: tenant_id.to_string(), account_id: account_id.to_string(), reason: NOT_THE_PILOT_PAIR })
        }
    }
}

pub const NOT_THE_PILOT_PAIR: &str = "NOT_THE_PILOT_PAIR";
pub const TENANT_CREDENTIALS_NOT_BUILT: &str = "TENANT_CREDENTIALS_NOT_BUILT";

/// The typed "not built" provider for every non-pilot account: refuses with [`TENANT_CREDENTIALS_NOT_BUILT`]. See
/// the module docs for why decrypting `exchange_credentials` here is a design decision, not a code change.
#[derive(Default)]
pub struct TenantCredentialsNotBuilt;

impl PaperCredentialProvider for TenantCredentialsNotBuilt {
    fn paper_alpaca_keys(&self, tenant_id: &str, account_id: &str) -> Result<PaperAlpacaKeys, CredentialError> {
        Err(CredentialError::NotProvided { tenant_id: tenant_id.to_string(), account_id: account_id.to_string(), reason: TENANT_CREDENTIALS_NOT_BUILT })
    }
}

/// The service's provider: the pilot pair from the environment (when configured), every other account from the
/// tenant provider.
pub struct ServiceCredentials {
    pilot: Option<EnvPilotCredentials>,
    tenants: Box<dyn PaperCredentialProvider>,
}

impl ServiceCredentials {
    pub fn new(pilot: Option<EnvPilotCredentials>, tenants: Box<dyn PaperCredentialProvider>) -> Self {
        Self { pilot, tenants }
    }

    pub fn pilot(&self) -> Option<&EnvPilotCredentials> {
        self.pilot.as_ref()
    }
}

/// Manual, redacting (through `EnvPilotCredentials`' and `PaperAlpacaKeys`' own redacting `Debug`); the tenant
/// provider is named, not printed.
impl fmt::Debug for ServiceCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceCredentials").field("pilot", &self.pilot).field("tenants", &"<dyn PaperCredentialProvider>").finish()
    }
}

impl PaperCredentialProvider for ServiceCredentials {
    fn paper_alpaca_keys(&self, tenant_id: &str, account_id: &str) -> Result<PaperAlpacaKeys, CredentialError> {
        if let Some(p) = &self.pilot {
            if same_id(tenant_id, &p.pair.tenant_id) && same_id(account_id, &p.pair.account_id) {
                return p.paper_alpaca_keys(tenant_id, account_id);
            }
        }
        self.tenants.paper_alpaca_keys(tenant_id, account_id)
    }
}
