//! The paper-only Alpaca adapter: the ONE way the paper pilot builds an Alpaca connection.
//!
//! The general [`AlpacaAdapter`] can be built for either environment and is deliberately permissive about unknown
//! key-id prefixes. The paper pilot places REAL orders (the pipeline's `Live` mode, because Alpaca has no
//! validate-only) on Alpaca's PAPER endpoint, so "cannot possibly be a live account" has to be a property of the
//! type, not a convention. [`PaperOnlyAlpaca`] therefore:
//!
//! * has no environment parameter: it always builds the paper environment (this file never names the live
//!   environment, host or URL; a source-scan test in the crate's tests fails if it ever does);
//! * requires the key id to start with `PK` (the documented paper prefix, FROM-MEMORY-OF-DOCS): a live key (`AK`),
//!   an unknown prefix and an empty value are all refused, where the general constructor only refuses the
//!   contradiction;
//! * requires the base URL to be the paper host (or a loopback host for the fake broker), through the same
//!   normalising guard as every Alpaca config;
//! * offers [`PaperOnlyAlpaca::verify_paper_account`], which additionally requires the account number to be present
//!   and to start with `PA` (the general check only refuses a paper-looking number on a live adapter);
//! * keeps the adapter private and hands out only `&AlpacaAdapter`, so nothing can swap its environment.
//!
//! This is a guard against mix-ups (a live key pasted into a paper secret, a wrong URL), not against a hostile
//! process: anything that can construct an [`AlpacaAdapter`] itself is outside this type's reach, which is why the
//! pilot also runs the pipeline under `VenuePolicy::PaperOnly` and scans its own source (see `rebalancer-run`).

use std::sync::Arc;

use super::config::{AlpacaConfig, Environment};
use super::parse::AccountInfo;
use super::{AlpacaAdapter, AlpacaCredentials};
use crate::error::BrokerError;
use crate::transport::HttpTransport;

/// Alpaca paper key ids start with this (FROM-MEMORY-OF-DOCS; UNVERIFIED against the real key: if the real paper key
/// has another prefix the pilot refuses to start, which is the safe direction).
pub const PAPER_KEY_PREFIX: &str = "PK";
/// Alpaca paper account numbers start with this (FROM-MEMORY-OF-DOCS).
pub const PAPER_ACCOUNT_PREFIX: &str = "PA";

/// Strict paper key-id check: must start with [`PAPER_KEY_PREFIX`]. The error never contains the input.
pub fn require_paper_key_id(key_id: &str) -> Result<(), BrokerError> {
    let key_id = key_id.trim();
    if key_id.starts_with(PAPER_KEY_PREFIX) {
        return Ok(());
    }
    let why = if key_id.starts_with("AK") {
        "the key id has the live prefix (AK)"
    } else if key_id.is_empty() {
        "the key id is empty"
    } else {
        "the key id does not have the paper prefix (PK)"
    };
    Err(BrokerError::Credentials(format!("paper-only Alpaca connection refused: {why}")))
}

/// An Alpaca adapter that can only ever be the paper environment. See the module docs.
pub struct PaperOnlyAlpaca {
    adapter: AlpacaAdapter,
}

impl std::fmt::Debug for PaperOnlyAlpaca {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaperOnlyAlpaca").field("adapter", &self.adapter).finish()
    }
}

impl PaperOnlyAlpaca {
    /// Build the paper adapter. `own_tag_prefix` and `refuse_builtin_assets` are the two pilot settings of
    /// [`AlpacaConfig`]; extended hours stay off (an ETF market order outside the session must be refused, not
    /// queued). Refuses a key id without the `PK` prefix, a base URL that is not the paper host or a loopback host,
    /// and empty or header-unsafe credentials.
    pub fn new(
        key_id: &str,
        secret: &str,
        base_url: &str,
        transport: Arc<dyn HttpTransport>,
        own_tag_prefix: Option<&str>,
        refuse_builtin_assets: bool,
    ) -> Result<Self, BrokerError> {
        require_paper_key_id(key_id)?;
        let mut config = AlpacaConfig::new(Environment::Paper, base_url)?;
        config.own_tag_prefix = own_tag_prefix.map(str::to_string);
        config.refuse_builtin_assets = refuse_builtin_assets;
        config.allow_extended_hours = false;
        let creds = AlpacaCredentials::new(Environment::Paper, key_id, secret)?;
        let adapter = AlpacaAdapter::new(config, creds, transport)?;
        // Belt and braces: the pieces above already agree; this states the invariant of the type.
        if adapter.environment() != Environment::Paper {
            return Err(BrokerError::Config("paper-only Alpaca adapter came out with a different environment".into()));
        }
        Ok(Self { adapter })
    }

    /// The adapter, for `rebalancer_run::broker::AlpacaBroker`. Borrowed only: its environment cannot be changed.
    pub fn adapter(&self) -> &AlpacaAdapter {
        &self.adapter
    }

    /// Non-secret identifier of the key (first 8 bytes of SHA-256 of the key id, hex), for the pilot plan's
    /// `credential_fingerprint`.
    pub fn key_id_fingerprint(&self) -> String {
        self.adapter.key_id_fingerprint()
    }

    /// `GET /v2/account`, the general checks (not blocked), plus the paper account-number check: the account number
    /// must be present and start with `PA`. A missing number is refused (the pilot needs the positive evidence, not
    /// the absence of a contradiction).
    pub fn verify_paper_account(&self) -> Result<AccountInfo, BrokerError> {
        let acct = self.adapter.verify_account()?;
        require_paper_account_number(&acct)?;
        Ok(acct)
    }
}

/// The account number must be present and start with [`PAPER_ACCOUNT_PREFIX`].
pub fn require_paper_account_number(acct: &AccountInfo) -> Result<(), BrokerError> {
    match acct.account_number.as_deref() {
        Some(n) if n.starts_with(PAPER_ACCOUNT_PREFIX) => Ok(()),
        Some(_) => Err(BrokerError::Credentials(
            "paper-only Alpaca connection refused: the account number does not have the paper prefix (PA)".into(),
        )),
        None => Err(BrokerError::Credentials("paper-only Alpaca connection refused: the account has no account number to check".into())),
    }
}
