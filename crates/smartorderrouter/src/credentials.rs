//! Tenant-scoped exchange credential resolution.
//!
//! # Design: "the caller passes credentials in"
//!
//! The public SignalEngine crates never decide *whose* API keys sign an order
//! by reading an unscoped table. Every credential lookup goes through the
//! [`CredentialProvider`] trait and carries an explicit [`TenantId`]. There is
//! deliberately **no** credential-loading API in this crate that omits the
//! tenant: a lookup without a tenant cannot be expressed.
//!
//! Providers:
//!
//! * [`StaticCredentialProvider`] -- in-memory, keyed by `(tenant, exchange)`.
//!   Used by tests and by callers that already hold decrypted credentials.
//! * `SingleTenantDbProvider` (feature `postgres`) -- reads the PUBLIC
//!   `exchange_credentials` table, which has **no tenant column**. It is
//!   therefore bound to exactly one tenant at construction and refuses every
//!   other tenant. Self-hosted, single-tenant deployments only.
//! * The multi-tenant (SaaS) provider is implemented OUTSIDE this public
//!   repository, against the private schema that carries `tenant_id`. It
//!   implements [`CredentialProvider`] and is handed to the execution handlers
//!   by the (private) host binary.
//!
//! # Fail closed
//!
//! Unknown tenant, unknown exchange, disabled credential, testnet-only
//! credential for a live request, wrong-tenant request against a
//! single-tenant provider, or any backend error all yield a
//! [`CredentialError`]. Callers must propagate it and place no order. Nothing
//! in this module ever falls back to "some other tenant's credential" or to
//! "the first row".
//!
//! Every call site should use [`resolve_credential`] / [`resolve_all_credentials`]
//! rather than calling a provider directly: they re-validate whatever the
//! provider returned (enabled, right exchange, live-only) so a buggy or
//! third-party provider cannot smuggle a disabled or sandbox key into a live
//! order path.

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;

/// Identifier of the tenant on whose behalf an order is placed.
pub type TenantId = uuid::Uuid;

/// Decrypted exchange credential for use in trading.
///
/// `Debug` is implemented by hand and never prints the key, secret or
/// passphrase.
#[derive(Clone)]
pub struct ExchangeCredential {
    pub id: uuid::Uuid,
    pub exchange: String,
    pub label: String,
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
    pub is_testnet: bool,
    pub is_enabled: bool,
}

impl fmt::Debug for ExchangeCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeCredential")
            .field("id", &self.id)
            .field("exchange", &self.exchange)
            .field("label", &self.label)
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("passphrase", &self.passphrase.as_ref().map(|_| "<redacted>"))
            .field("is_testnet", &self.is_testnet)
            .field("is_enabled", &self.is_enabled)
            .finish()
    }
}

impl ExchangeCredential {
    /// Defence-in-depth check applied to whatever a provider returned.
    pub fn check_usable(&self, exchange: &str, live_only: bool) -> Result<(), CredentialError> {
        if !self.exchange.eq_ignore_ascii_case(exchange) {
            return Err(CredentialError::Invalid(format!(
                "provider returned a credential for exchange '{}' when '{}' was requested",
                self.exchange, exchange
            )));
        }
        if !self.is_enabled {
            return Err(CredentialError::Disabled { exchange: exchange.to_string() });
        }
        if live_only && self.is_testnet {
            return Err(CredentialError::NoLiveCredential { exchange: exchange.to_string() });
        }
        Ok(())
    }
}

/// Why a credential could not be provided. Every variant means "do not trade".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// No credential stored for this tenant + exchange (includes unknown tenant).
    NotFound { tenant: TenantId, exchange: String },
    /// A credential exists but is disabled.
    Disabled { exchange: String },
    /// Only sandbox/testnet credentials exist but a live one was required.
    NoLiveCredential { exchange: String },
    /// A single-tenant provider was asked for a tenant other than the one it serves.
    TenantNotServed { requested: TenantId, served: TenantId },
    /// The provider returned something that violates the provider contract.
    Invalid(String),
    /// The backing store failed (database down, decryption failure, ...).
    Backend(String),
}

impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CredentialError::NotFound { tenant, exchange } => {
                write!(f, "no credential for tenant {} on exchange '{}'", tenant, exchange)
            }
            CredentialError::Disabled { exchange } => {
                write!(f, "credential for exchange '{}' is disabled", exchange)
            }
            CredentialError::NoLiveCredential { exchange } => {
                write!(f, "only testnet credentials exist for exchange '{}' but a live one is required", exchange)
            }
            CredentialError::TenantNotServed { requested, served } => write!(
                f,
                "credential provider serves only tenant {} and refuses tenant {}",
                served, requested
            ),
            CredentialError::Invalid(m) => write!(f, "invalid credential provider response: {}", m),
            CredentialError::Backend(m) => write!(f, "credential backend error: {}", m),
        }
    }
}

impl std::error::Error for CredentialError {}

/// Source of exchange credentials, always scoped by tenant.
///
/// Implementations MUST return only credentials owned by `tenant`, and MUST
/// return an error -- never another tenant's credential -- when they cannot.
#[async_trait]
pub trait CredentialProvider: Send + Sync {
    /// The enabled credential `tenant` holds for `exchange`.
    ///
    /// `live_only: true` restricts the result to non-testnet credentials; it
    /// is REQUIRED for live deployments.
    async fn credentials_for(
        &self,
        tenant: TenantId,
        exchange: &str,
        live_only: bool,
    ) -> Result<ExchangeCredential, CredentialError>;

    /// Every enabled credential `tenant` holds (all exchanges, testnet included).
    async fn all_credentials_for(
        &self,
        tenant: TenantId,
    ) -> Result<Vec<ExchangeCredential>, CredentialError>;
}

/// Resolve one credential through `provider` and re-validate the answer.
/// This is the single choke point every order-path call site uses.
pub async fn resolve_credential(
    provider: &dyn CredentialProvider,
    tenant: TenantId,
    exchange: &str,
    live_only: bool,
) -> Result<ExchangeCredential, CredentialError> {
    let credential = provider.credentials_for(tenant, exchange, live_only).await?;
    credential.check_usable(exchange, live_only)?;
    Ok(credential)
}

/// Resolve all of a tenant's credentials through `provider`, re-validating each.
pub async fn resolve_all_credentials(
    provider: &dyn CredentialProvider,
    tenant: TenantId,
) -> Result<Vec<ExchangeCredential>, CredentialError> {
    let credentials = provider.all_credentials_for(tenant).await?;
    for c in &credentials {
        if !c.is_enabled {
            return Err(CredentialError::Disabled { exchange: c.exchange.clone() });
        }
    }
    Ok(credentials)
}

/// Pick the credential for `exchange` from one tenant's credentials.
/// Shared by the in-memory and database providers.
pub fn select_credential(
    tenant_credentials: &[ExchangeCredential],
    tenant: TenantId,
    exchange: &str,
    live_only: bool,
) -> Result<ExchangeCredential, CredentialError> {
    let mut seen = false;
    let mut seen_enabled = false;
    for c in tenant_credentials
        .iter()
        .filter(|c| c.exchange.eq_ignore_ascii_case(exchange))
    {
        seen = true;
        if !c.is_enabled {
            continue;
        }
        seen_enabled = true;
        if live_only && c.is_testnet {
            continue;
        }
        return Ok(c.clone());
    }
    if !seen {
        Err(CredentialError::NotFound { tenant, exchange: exchange.to_string() })
    } else if !seen_enabled {
        Err(CredentialError::Disabled { exchange: exchange.to_string() })
    } else {
        Err(CredentialError::NoLiveCredential { exchange: exchange.to_string() })
    }
}

/// In-memory provider keyed by tenant. Intended for tests and for callers that
/// already hold decrypted credentials and want to hand them in.
#[derive(Debug, Default, Clone)]
pub struct StaticCredentialProvider {
    by_tenant: HashMap<TenantId, Vec<ExchangeCredential>>,
}

impl StaticCredentialProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a credential owned by `tenant`.
    pub fn with_credential(mut self, tenant: TenantId, credential: ExchangeCredential) -> Self {
        self.by_tenant.entry(tenant).or_default().push(credential);
        self
    }
}

#[async_trait]
impl CredentialProvider for StaticCredentialProvider {
    async fn credentials_for(
        &self,
        tenant: TenantId,
        exchange: &str,
        live_only: bool,
    ) -> Result<ExchangeCredential, CredentialError> {
        match self.by_tenant.get(&tenant) {
            Some(creds) => select_credential(creds, tenant, exchange, live_only),
            None => Err(CredentialError::NotFound { tenant, exchange: exchange.to_string() }),
        }
    }

    async fn all_credentials_for(
        &self,
        tenant: TenantId,
    ) -> Result<Vec<ExchangeCredential>, CredentialError> {
        Ok(self
            .by_tenant
            .get(&tenant)
            .map(|v| v.iter().filter(|c| c.is_enabled).cloned().collect())
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cred(exchange: &str, key: &str, testnet: bool, enabled: bool) -> ExchangeCredential {
        ExchangeCredential {
            id: uuid::Uuid::new_v4(),
            exchange: exchange.to_string(),
            label: "label".to_string(),
            api_key: key.to_string(),
            api_secret: format!("{}-secret", key),
            passphrase: None,
            is_testnet: testnet,
            is_enabled: enabled,
        }
    }

    #[tokio::test]
    async fn two_tenants_same_exchange_get_their_own_keys() {
        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();
        let provider = StaticCredentialProvider::new()
            .with_credential(a, cred("kraken", "KEY-A", false, true))
            .with_credential(b, cred("kraken", "KEY-B", false, true));

        let ca = resolve_credential(&provider, a, "kraken", true).await.unwrap();
        let cb = resolve_credential(&provider, b, "Kraken", true).await.unwrap();
        assert_eq!(ca.api_key, "KEY-A");
        assert_eq!(cb.api_key, "KEY-B");
    }

    #[tokio::test]
    async fn tenant_without_credentials_gets_error_not_another_tenants_key() {
        let a = uuid::Uuid::new_v4();
        let stranger = uuid::Uuid::new_v4();
        let provider = StaticCredentialProvider::new()
            .with_credential(a, cred("kraken", "KEY-A", false, true));

        let err = resolve_credential(&provider, stranger, "kraken", true).await.unwrap_err();
        assert!(matches!(err, CredentialError::NotFound { tenant, .. } if tenant == stranger));
        assert!(resolve_all_credentials(&provider, stranger).await.unwrap().is_empty());

        // Known tenant, exchange it does not hold.
        let err = resolve_credential(&provider, a, "coinbase", true).await.unwrap_err();
        assert!(matches!(err, CredentialError::NotFound { .. }));
    }

    #[tokio::test]
    async fn disabled_and_testnet_credentials_fail_closed() {
        let t = uuid::Uuid::new_v4();
        let provider = StaticCredentialProvider::new()
            .with_credential(t, cred("kraken", "OFF", false, false))
            .with_credential(t, cred("coinbase", "SANDBOX", true, true));

        let err = resolve_credential(&provider, t, "kraken", false).await.unwrap_err();
        assert!(matches!(err, CredentialError::Disabled { .. }));
        // Sandbox key is fine for paper, refused for live.
        assert!(resolve_credential(&provider, t, "coinbase", false).await.is_ok());
        let err = resolve_credential(&provider, t, "coinbase", true).await.unwrap_err();
        assert!(matches!(err, CredentialError::NoLiveCredential { .. }));
    }

    /// A provider that ignores the request and returns a disabled / wrong
    /// exchange / sandbox credential must still be rejected by the choke point.
    struct BadProvider(ExchangeCredential);

    #[async_trait]
    impl CredentialProvider for BadProvider {
        async fn credentials_for(&self, _: TenantId, _: &str, _: bool)
            -> Result<ExchangeCredential, CredentialError> { Ok(self.0.clone()) }
        async fn all_credentials_for(&self, _: TenantId)
            -> Result<Vec<ExchangeCredential>, CredentialError> { Ok(vec![self.0.clone()]) }
    }

    #[tokio::test]
    async fn resolve_revalidates_provider_output() {
        let t = uuid::Uuid::new_v4();
        let wrong_exchange = BadProvider(cred("binance", "K", false, true));
        assert!(matches!(
            resolve_credential(&wrong_exchange, t, "kraken", true).await,
            Err(CredentialError::Invalid(_))
        ));
        let disabled = BadProvider(cred("kraken", "K", false, false));
        assert!(matches!(
            resolve_credential(&disabled, t, "kraken", true).await,
            Err(CredentialError::Disabled { .. })
        ));
        assert!(matches!(
            resolve_all_credentials(&disabled, t).await,
            Err(CredentialError::Disabled { .. })
        ));
        let sandbox = BadProvider(cred("kraken", "K", true, true));
        assert!(matches!(
            resolve_credential(&sandbox, t, "kraken", true).await,
            Err(CredentialError::NoLiveCredential { .. })
        ));
    }

    #[test]
    fn debug_never_prints_secrets() {
        let c = cred("kraken", "SUPERSECRETKEY", false, true);
        let s = format!("{:?}", c);
        assert!(!s.contains("SUPERSECRETKEY"));
        assert!(!s.contains("-secret"));
    }
}
