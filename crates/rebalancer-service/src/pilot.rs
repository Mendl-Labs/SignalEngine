//! The paper-pilot interlock of the rebalancer service (slice S-8 of product-mandate/PAPER_PILOT_DRAGONSTONE_PLAN.md).
//!
//! The pilot places REAL orders on an Alpaca PAPER account through the pipeline's `Live` execution mode (Alpaca has no
//! validate-only, so the pipeline's `Paper` mode cannot place anything there). That is only acceptable if "this can
//! never be a live account" is enforced in layers, any one of which stops a mix-up. The layers that live in THIS
//! crate:
//!
//! 1. [`PilotConfig::from_lookup`]: the service refuses to start unless `REBALANCER_PAPER_ONLY=true` and both
//!    `PILOT_TENANT_ID` and `PILOT_ACCOUNT_ID` are set (UUIDs), and refuses when `CREDENTIALS_ENCRYPTION_KEY` (the
//!    tenant-credential master key) is present in the process environment: the pilot pod must not be able to decrypt
//!    a live credential even by accident.
//! 2. [`PilotAccountSource`]: wraps any `AccountSource` and serves exactly ONE (tenant, account) pair. An account
//!    with the pilot's account id under another tenant, an ambiguous match, an execution mode other than
//!    Assisted/Live, a sleeve or mandate venue other than `alpaca`: the whole enumeration fails (loudly, nothing runs).
//! 3. [`pilot_run_config`]: a `RunConfig` with `VenuePolicy::PaperOnly`, so the pipeline itself refuses (before
//!    reading the broker) any broker that does not report a paper connection, in every execution mode. This is what
//!    lets `ExecutionMode::Live` be paired ONLY with a paper environment in the pilot build.
//! 4. [`build_paper_alpaca`]: the one function that builds an Alpaca connection here. It goes through
//!    `broker_adapters::alpaca::PaperOnlyAlpaca` (no environment parameter; `PK` key ids only; paper host or
//!    loopback only; `PA` account numbers only). This crate never names the live environment, the live host, a Kraken
//!    adapter or the master key: `tests/pilot_source_scan.rs` fails if it ever does.
//! 5. [`allow_venue_environment`]: the allow-list check the account source / runtime builder (later slices) must call
//!    on a credential row: only (`alpaca`, `paper`) passes.
//!
//! NOT here (later slices, listed in the S-8 hand-off): the database CHECK / trigger on the pilot plan table,
//! the restricted database role, the `is_testnet` account filter and the L2-to-mode mapping in the Postgres account
//! source (S-5), the startup `GET /v2/account` call and fingerprint comparison against the plan row (S-6).

use std::sync::Arc;

use broker_adapters::alpaca::PaperOnlyAlpaca;
use broker_adapters::transport::HttpTransport;
use broker_adapters::Dec;
use rebalancer_run::broker::OWN_TAG_PREFIX;
use rebalancer_run::driver::{AccountSource, ActiveAccount};
use rebalancer_run::pipeline::{RunConfig, VenuePolicy};
use rebalancer_run::recon::ReconTolerances;
use rebalancer_run::record::ExecutionMode;
use uuid::Uuid;

/// Must be exactly `true` for the service to start.
pub const ENV_PAPER_ONLY: &str = "REBALANCER_PAPER_ONLY";
pub const ENV_TENANT: &str = "PILOT_TENANT_ID";
pub const ENV_ACCOUNT: &str = "PILOT_ACCOUNT_ID";
/// Environment variables the pilot process must NOT hold.
pub const FORBIDDEN_ENV: [&str; 1] = ["CREDENTIALS_ENCRYPTION_KEY"];
/// The only venue the pilot may serve.
pub const PILOT_VENUE: &str = "alpaca";

/// Why the pilot interlock refused. Every variant has a stable code (`PILOT_...`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PilotRefusal {
    PaperOnlyNotSet,
    PaperOnlyBadValue(String),
    MissingId(&'static str),
    BadId { name: &'static str },
    ForbiddenEnvPresent(&'static str),
    AccountUnderOtherTenant { account_id: String },
    AmbiguousAccount { count: usize },
    ModeNotAllowed { account_id: String, mode: &'static str },
    VenueNotAllowed { what: &'static str, venue: String },
    NoSleeves { account_id: String },
    NotPaperEnvironment { venue: String, environment: String },
    Adapter(String),
}

impl PilotRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            PilotRefusal::PaperOnlyNotSet => "PILOT_PAPER_ONLY_NOT_SET",
            PilotRefusal::PaperOnlyBadValue(_) => "PILOT_PAPER_ONLY_BAD_VALUE",
            PilotRefusal::MissingId(_) => "PILOT_ID_MISSING",
            PilotRefusal::BadId { .. } => "PILOT_ID_INVALID",
            PilotRefusal::ForbiddenEnvPresent(_) => "PILOT_FORBIDDEN_ENV_PRESENT",
            PilotRefusal::AccountUnderOtherTenant { .. } => "PILOT_ACCOUNT_UNDER_OTHER_TENANT",
            PilotRefusal::AmbiguousAccount { .. } => "PILOT_ACCOUNT_AMBIGUOUS",
            PilotRefusal::ModeNotAllowed { .. } => "PILOT_MODE_NOT_ALLOWED",
            PilotRefusal::VenueNotAllowed { .. } => "PILOT_VENUE_NOT_ALLOWED",
            PilotRefusal::NoSleeves { .. } => "PILOT_NO_SLEEVES",
            PilotRefusal::NotPaperEnvironment { .. } => "PILOT_NOT_PAPER_ENVIRONMENT",
            PilotRefusal::Adapter(_) => "PILOT_ADAPTER_REFUSED",
        }
    }
}

impl std::fmt::Display for PilotRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: ", self.code())?;
        match self {
            PilotRefusal::PaperOnlyNotSet => write!(f, "{ENV_PAPER_ONLY} is not set; the pilot service starts only with {ENV_PAPER_ONLY}=true"),
            PilotRefusal::PaperOnlyBadValue(v) => write!(f, "{ENV_PAPER_ONLY} is {v:?}; it must be exactly \"true\""),
            PilotRefusal::MissingId(n) => write!(f, "{n} is not set"),
            PilotRefusal::BadId { name } => write!(f, "{name} is not a UUID"),
            PilotRefusal::ForbiddenEnvPresent(n) => write!(f, "{n} is present in the environment; the pilot process must not hold it"),
            PilotRefusal::AccountUnderOtherTenant { account_id } => {
                write!(f, "account {account_id} is the pilot account id but belongs to a different tenant than {ENV_TENANT}")
            }
            PilotRefusal::AmbiguousAccount { count } => write!(f, "{count} accounts match the pilot (tenant, account) pair; exactly one may"),
            PilotRefusal::ModeNotAllowed { account_id, mode } => {
                write!(f, "account {account_id} has execution mode {mode}; the pilot serves only assisted and live-on-paper")
            }
            PilotRefusal::VenueNotAllowed { what, venue } => write!(f, "{what} names venue {venue:?}; the pilot serves only {PILOT_VENUE:?}"),
            PilotRefusal::NoSleeves { account_id } => write!(f, "account {account_id} has no sleeves"),
            PilotRefusal::NotPaperEnvironment { venue, environment } => {
                write!(f, "venue {venue:?} in environment {environment:?} is not on the pilot allow-list (only \"alpaca\" in \"paper\")")
            }
            PilotRefusal::Adapter(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for PilotRefusal {}

/// The one (tenant, account) pair the pilot serves, canonical (lowercase hyphenated UUIDs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PilotConfig {
    pub tenant_id: String,
    pub account_id: String,
}

fn canonical_uuid(name: &'static str, raw: Option<String>) -> Result<String, PilotRefusal> {
    let raw = raw.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).ok_or(PilotRefusal::MissingId(name))?;
    let id = Uuid::parse_str(&raw).map_err(|_| PilotRefusal::BadId { name })?;
    if id.is_nil() {
        return Err(PilotRefusal::BadId { name });
    }
    Ok(id.hyphenated().to_string())
}

impl PilotConfig {
    /// Read the interlock settings through `lookup` (the process environment in `main`, a map in tests).
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, PilotRefusal> {
        for name in FORBIDDEN_ENV {
            if lookup(name).is_some() {
                return Err(PilotRefusal::ForbiddenEnvPresent(name));
            }
        }
        match lookup(ENV_PAPER_ONLY) {
            None => return Err(PilotRefusal::PaperOnlyNotSet),
            Some(v) if v.trim() == "true" => {}
            Some(v) => return Err(PilotRefusal::PaperOnlyBadValue(v)),
        }
        Ok(Self { tenant_id: canonical_uuid(ENV_TENANT, lookup(ENV_TENANT))?, account_id: canonical_uuid(ENV_ACCOUNT, lookup(ENV_ACCOUNT))? })
    }

    pub fn from_env() -> Result<Self, PilotRefusal> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }
}

/// A sizing/valuation price is stale after this many seconds (slice S-6): the pilot's `DataSource` supplies the last
/// COMPLETE daily close (`crate::runtime::LastClosePrices`), which can legitimately be a Friday close carried
/// through a weekend, or a close from before a market holiday -- up to a few calendar days old, never "a few minutes
/// old" the way a live quote feed would be. Four days covers a Friday close read on the following Monday or Tuesday
/// (a weekend plus one holiday) without stretching so far that a genuinely missed run goes unnoticed.
pub const PILOT_MAX_PRICE_AGE_SECS: i64 = 4 * 24 * 60 * 60;

/// The reconciliation drift floor is raised for the pilot (slice S-6, plan section "Corporate actions and
/// dividends"): a paper dividend can move cash between monthly runs by more than the platform's normal $1 floor
/// (IEF pays monthly, VNQ quarterly), and `RunConfig::tolerances`'s default `BalanceDrift` would halt the account
/// without flattening on the very first distribution. $15 is comfortably above a plausible single distribution on a
/// $5,000 pilot account; any halt above that floor is a genuine finding to investigate, not a false alarm to raise
/// past.
pub const PILOT_RECON_VALUE_ABS: &str = "15";

/// The pipeline configuration of the pilot: the defaults plus `VenuePolicy::PaperOnly`, a sizing-price staleness
/// tolerance that matches a daily-close data source, and a reconciliation drift floor that tolerates a paper
/// dividend (slice S-6; see the constants above for why each value is what it is).
pub fn pilot_run_config() -> RunConfig {
    let tolerances = ReconTolerances { value_abs: Dec::parse(PILOT_RECON_VALUE_ABS).unwrap_or_else(|_| Dec::from_i64(15)), ..ReconTolerances::default() };
    RunConfig { venue_policy: VenuePolicy::PaperOnly, max_price_age_secs: PILOT_MAX_PRICE_AGE_SECS, tolerances, ..RunConfig::default() }
}

fn same_id(a: &str, b: &str) -> bool {
    a.trim().eq_ignore_ascii_case(b.trim())
}

/// `Ok` only for (`alpaca`, `paper`). Called with the venue and environment a credential row / runtime request names.
pub fn allow_venue_environment(venue: &str, environment: &str) -> Result<(), PilotRefusal> {
    if venue.trim().eq_ignore_ascii_case(PILOT_VENUE) && environment.trim().eq_ignore_ascii_case("paper") {
        Ok(())
    } else {
        Err(PilotRefusal::NotPaperEnvironment { venue: venue.to_string(), environment: environment.to_string() })
    }
}

/// Serves exactly one (tenant, account) pair from an inner source and refuses anything about it that the pilot does
/// not allow. See the module docs, layer 2.
pub struct PilotAccountSource<S> {
    inner: S,
    config: PilotConfig,
}

impl<S: AccountSource> PilotAccountSource<S> {
    pub fn new(inner: S, config: PilotConfig) -> Self {
        Self { inner, config }
    }

    fn check(&self, a: &ActiveAccount) -> Result<(), PilotRefusal> {
        match a.mode {
            ExecutionMode::Assisted | ExecutionMode::Live => {}
            ExecutionMode::Paper => return Err(PilotRefusal::ModeNotAllowed { account_id: a.account_id.clone(), mode: a.mode.as_str() }),
        }
        if a.sleeves.is_empty() {
            return Err(PilotRefusal::NoSleeves { account_id: a.account_id.clone() });
        }
        for s in &a.sleeves {
            if !s.venue.trim().eq_ignore_ascii_case(PILOT_VENUE) {
                return Err(PilotRefusal::VenueNotAllowed { what: "a sleeve", venue: s.venue.clone() });
            }
        }
        for v in &a.mandate.universe.venues {
            if !v.trim().eq_ignore_ascii_case(PILOT_VENUE) {
                return Err(PilotRefusal::VenueNotAllowed { what: "the mandate universe", venue: v.clone() });
            }
        }
        Ok(())
    }

    fn select(&self) -> Result<Vec<ActiveAccount>, PilotRefusal> {
        let all = self.inner.active_accounts().map_err(PilotRefusal::Adapter)?;
        let mut mine = Vec::new();
        for a in all {
            let account_matches = same_id(&a.account_id, &self.config.account_id);
            let tenant_matches = same_id(&a.tenant_id, &self.config.tenant_id);
            match (account_matches, tenant_matches) {
                (true, true) => mine.push(a),
                (true, false) => return Err(PilotRefusal::AccountUnderOtherTenant { account_id: a.account_id }),
                _ => {} // every other account (any tenant) is invisible to the pilot
            }
        }
        if mine.len() > 1 {
            return Err(PilotRefusal::AmbiguousAccount { count: mine.len() });
        }
        for a in &mine {
            self.check(a)?;
        }
        Ok(mine)
    }
}

impl<S: AccountSource> AccountSource for PilotAccountSource<S> {
    fn active_accounts(&self) -> Result<Vec<ActiveAccount>, String> {
        self.select().map_err(|r| r.to_string())
    }
}

/// The ONE way the pilot builds an Alpaca connection: a [`PaperOnlyAlpaca`] with the rebalancer's own tag prefix and
/// built-in (unverified) assets refused. `key_id` must start with `PK`; `base_url` must be the paper host (or a
/// loopback host in tests). There is no environment parameter.
pub fn build_paper_alpaca(key_id: &str, secret: &str, base_url: &str, transport: Arc<dyn HttpTransport>) -> Result<PaperOnlyAlpaca, PilotRefusal> {
    PaperOnlyAlpaca::new(key_id, secret, base_url, transport, Some(OWN_TAG_PREFIX), true).map_err(|e| PilotRefusal::Adapter(format!("{e}")))
}
