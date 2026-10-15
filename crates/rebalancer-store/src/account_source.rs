//! `PgAccountSource`: the Postgres-backed [`AccountSource`] for the paper pilot (plan
//! `product-mandate/PAPER_PILOT_DRAGONSTONE_PLAN.md`, slice S-5) and, behind a flag, for every account whose plan is
//! an APPROVED PROPOSAL (`strategy_plans`; certified-envelopes plan, Phase 0).
//!
//! It answers ONE question: which accounts may the driver run, in which execution mode, with which sleeves? It
//! builds the [`ActiveAccount`] descriptors the driver needs and constructs NO broker, reads NO secret and calls no
//! vendor: the `AccountRuntime` builder (`rebalancer-service::runtime`) is the other half.
//!
//! # Two plan sources, exactly one per account
//! * The owner-authored pilot plan (`rebalancer_pilot_plans`, newest row wins, `origin = 'owner_pilot'`).
//! * The proposal-derived plan (`strategy_plans`, written by the Engine's approve path; `mode = 'paper'` and
//!   `state = 'paper'` are the only servable values today, `live`/`halted` are typed exclusions, `closed` is
//!   ignored). Served ONLY when the scope says so ([`AccountScope::PilotAndProposals`], the service's
//!   `REBALANCER_SERVE_PROPOSAL_PLANS=true`); under [`AccountScope::Pilot`] the behaviour is exactly the pilot's.
//! * An account that has BOTH a pilot plan and a non-closed proposal plan is refused
//!   ([`ExclusionReason::StrategyPlanPresent`]): a plan cannot come from two sources. The pilot-plan guard trigger
//!   refuses the insert in the same situation; this is the second, independent line.
//!
//! # Strict scoping
//! * Under [`AccountScope::Pilot`] a PILOT ALLOW-LIST ([`PilotAllowList`]: one tenant id and explicit account ids) is
//!   the only input. Every query is bound to (tenant, account) pairs; nothing else is ever selected. An empty
//!   allow-list is a construction error, never "all accounts".
//! * Under [`AccountScope::PilotAndProposals`] the allow-list comes from the DATABASE: every (tenant, account) that
//!   has a non-closed `strategy_plans` row, plus the pilot pair(s) if configured. The per-pair binding of every
//!   other query is unchanged, so tenant A's rows can never be read while evaluating tenant B's account.
//! * Only the non-secret `exchange_credentials` columns are selected (no `*_encrypted` column), so the service's
//!   database role needs no access to them.
//!
//! # What makes an account eligible (each failure is a typed [`ExclusionReason`], never a silent skip)
//! * its credential exists under the targeted tenant, is not deleted, is enabled, is a TESTNET (paper) credential
//!   and an Alpaca exchange (paper only: anything else is refused);
//! * it has an ACTIVE, SIGNED mandate (status `active`, granted, effective and review dates present) whose body
//!   validates and whose stored hash matches; a mandate at autonomy L3 (live) is refused;
//! * it has exactly one plan source (above), bound to exactly that mandate version, whose sleeves parse, run on
//!   Alpaca only and pin library entries whose hashes equal the platform's own ([`check_entry`]).
//!
//! # The execution mode (plan section 3.5) the source derives, never more than the mandate grants
//! | mandate level | plan `execution` | mode handed to the driver |
//! |---|---|---|
//! | L0, L1 | any | `Assisted` |
//! | L2 | `assisted` | `Assisted` |
//! | L2 | `paper_orders` and `place_orders` granted | `Live` (the pipeline's order-placing mode, against the PAPER venue) |
//! | L2 | `paper_orders` without `place_orders` | refused |
//! | L3 | any | refused |
//!
//! A proposal plan has no `execution` column: it asks for `paper_orders` when the mandate is L2 and grants
//! `place_orders`, and for `assisted` otherwise, so the table above applies unchanged and such a plan is never
//! refused for asking more than its mandate grants.
//!
//! A mandate whose `review_by` has passed is handed over as it is (status `Active`, its real dates): the pipeline
//! computes the standing and only lets reducing orders through, exactly as it does for any account.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use broker_adapters::Dec;
use chrono::{DateTime, Utc};
use diesel::sql_types::{Array, BigInt, Bool, Integer, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel_async::RunQueryDsl;
use mandate_core::mandate::{canonical_hash, validate_json, AgentAction, AutonomyLevel, MandateBody};
use mandate_core::strategy_library::{entry_hash, seed_library};
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::driver::{AccountSource, ActiveAccount};
use rebalancer_run::record::ExecutionMode;
use serde_json::Value;
use uuid::Uuid;

use crate::pg::{Bridge, Pool};
use crate::provenance::{PlanOrigin, PlanProvenance, PlanProvenanceRegistry, VenueEnvironment};

/// Exchange strings a paper Alpaca credential may carry (which one production uses is unverified, plan
/// section 3.1). Lower case.
pub const ALPACA_EXCHANGES: [&str; 2] = ["alpaca_paper", "alpaca"];

/// The only venue a plan sleeve may run on in this phase (both sources). Proposal plans name no venue in their body;
/// this is what they get.
pub const PLAN_VENUE: &str = "alpaca";

/// `strategy_plans.mode` / `.state` values the source serves. `live` (either column) and `halted` are typed
/// exclusions; `closed` rows are not read at all.
pub const SERVABLE_PLAN_MODE: &str = "paper";
pub const SERVABLE_PLAN_STATE: &str = "paper";

/// The library entry -> sleeve kind table for proposal plans (`PlanSleeve.entry_id`). An entry id that is not here
/// is a typed exclusion ([`ExclusionReason::UnsupportedSleeve`]), never a guess. Phase 4 of the certified-envelopes
/// plan replaces this with per-point instruments; until then the kind pins the compile-time universe.
pub const ENTRY_SLEEVE_KINDS: [(&str, SleeveKind); 2] = [("etf_trend_faber", SleeveKind::EtfTrend), ("crypto_trend_100d", SleeveKind::CryptoTrend)];

pub fn sleeve_kind_for_entry(entry_id: &str) -> Option<SleeveKind> {
    ENTRY_SLEEVE_KINDS.iter().find(|(id, _)| *id == entry_id).map(|(_, k)| *k)
}

/// The sleeve shape of a kind on [`PLAN_VENUE`]: (asset class, quote). The mandate names the same strings
/// (`us_etf`; `crypto_spot` with `USD` quote, the Alpaca crypto path's own names).
fn sleeve_shape(kind: SleeveKind) -> (&'static str, &'static str) {
    match kind {
        SleeveKind::EtfTrend => ("us_etf", ""),
        SleeveKind::CryptoTrend => ("crypto_spot", "USD"),
    }
}

// ---------------------------------------------------------------------------------------------------
// Inputs and results
// ---------------------------------------------------------------------------------------------------

/// The explicit pilot allow-list: ONE tenant and the account ids (credential ids) that may be enumerated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PilotAllowList {
    tenant_id: Uuid,
    account_ids: BTreeSet<Uuid>,
}

impl PilotAllowList {
    /// Refuses the nil tenant and an empty account list.
    pub fn new(tenant_id: Uuid, account_ids: impl IntoIterator<Item = Uuid>) -> Result<Self, AccountSourceError> {
        let account_ids: BTreeSet<Uuid> = account_ids.into_iter().collect();
        if tenant_id.is_nil() {
            return Err(AccountSourceError::BadAllowList("the pilot tenant id must not be the nil UUID".into()));
        }
        if account_ids.is_empty() {
            return Err(AccountSourceError::BadAllowList("the pilot allow-list names no account; an empty list never means \"all accounts\"".into()));
        }
        if account_ids.iter().any(|a| a.is_nil()) {
            return Err(AccountSourceError::BadAllowList("the pilot account list contains the nil UUID".into()));
        }
        Ok(Self { tenant_id, account_ids })
    }

    /// From the `PILOT_TENANT_ID` / `PILOT_ACCOUNT_ID` style strings (the account value may be a comma-separated list).
    pub fn parse(tenant_id: &str, account_ids: &str) -> Result<Self, AccountSourceError> {
        let tenant = tenant_id.trim().parse::<Uuid>().map_err(|e| AccountSourceError::BadAllowList(format!("tenant id {tenant_id:?} is not a UUID: {e}")))?;
        let mut ids = Vec::new();
        for part in account_ids.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            ids.push(part.parse::<Uuid>().map_err(|e| AccountSourceError::BadAllowList(format!("account id {part:?} is not a UUID: {e}")))?);
        }
        Self::new(tenant, ids)
    }

    pub fn tenant_id(&self) -> Uuid {
        self.tenant_id
    }

    pub fn account_ids(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.account_ids.iter().copied()
    }

    pub fn contains(&self, tenant_id: Uuid, account_id: Uuid) -> bool {
        self.tenant_id == tenant_id && self.account_ids.contains(&account_id)
    }

    fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        self.account_ids.iter().map(move |a| Target { tenant_id: self.tenant_id, account_id: *a })
    }
}

/// Which plan sources the source serves, and for whom.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountScope {
    /// The pilot only: exactly the allow-listed pair(s), pilot plans only. This is the service's behaviour with
    /// `REBALANCER_SERVE_PROPOSAL_PLANS` unset or `false`.
    Pilot(PilotAllowList),
    /// The pilot pair(s), if any, PLUS every account of every tenant that has a non-closed `strategy_plans` row
    /// (the allow-list comes from the database). `REBALANCER_SERVE_PROPOSAL_PLANS=true`.
    PilotAndProposals { pilot: Option<PilotAllowList> },
}

impl AccountScope {
    pub fn serves_proposal_plans(&self) -> bool {
        matches!(self, AccountScope::PilotAndProposals { .. })
    }

    pub fn pilot(&self) -> Option<&PilotAllowList> {
        match self {
            AccountScope::Pilot(p) => Some(p),
            AccountScope::PilotAndProposals { pilot } => pilot.as_ref(),
        }
    }
}

/// One (tenant, account) the source evaluates. Every row query is bound to these pairs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Target {
    pub tenant_id: Uuid,
    pub account_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccountSourceError {
    #[error("ACCOUNT_SOURCE_BAD_ALLOW_LIST: {0}")]
    BadAllowList(String),
    #[error("ACCOUNT_SOURCE_DB_ERROR: {0}")]
    Db(String),
}

/// What the plan's `execution` column says (the DB CHECK allows exactly these two). A proposal plan derives it from
/// the mandate (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanExecution {
    Assisted,
    PaperOrders,
}

impl PlanExecution {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "assisted" => Some(PlanExecution::Assisted),
            "paper_orders" => Some(PlanExecution::PaperOrders),
            _ => None,
        }
    }
}

/// Why a targeted account was NOT handed to the driver. The account is excluded, never partially run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExclusionReason {
    /// No credential row for this id under the targeted tenant (missing, or it belongs to another tenant).
    CredentialNotFound,
    CredentialDeleted,
    CredentialDisabled,
    /// `is_testnet = false`: not a paper credential.
    NotPaperCredential,
    /// The exchange is not one of [`ALPACA_EXCHANGES`].
    NotAlpaca(String),
    /// No mandate with status `active` (draft, superseded, revoked or expired ones do not count).
    NoActiveMandate,
    /// The mandate is active in name but lacks its grant metadata.
    MandateNotSigned,
    MandateBodyInvalid(String),
    MandateHashMismatch,
    /// Autonomy level L3 (live): never for this service.
    AutonomyL3,
    /// The plan asks for orders but the mandate is not at L2 with `place_orders` granted.
    OrdersNotGranted,
    /// No pilot plan (and, under a proposal-serving scope, no proposal plan either).
    NoPilotPlan,
    /// The plan is bound to another mandate id or version than the active one.
    PlanBoundToOtherMandate { plan_version: i32, active_version: i32 },
    /// The plan row is not `venue_environment = 'paper'` (the DB CHECK forbids it; this is the second line).
    PlanNotPaper(String),
    /// The plan row is not `origin = 'owner_pilot'`.
    PlanNotPilotOrigin(String),
    /// The account has BOTH a pilot plan and a non-closed proposal-derived plan (`strategy_plans`): a plan cannot
    /// come from two sources.
    StrategyPlanPresent,
    PlanBodyInvalid(String),
    /// A sleeve this service cannot run (any venue but Alpaca, a kind it has no rule for, an entry id not in
    /// [`ENTRY_SLEEVE_KINDS`]).
    UnsupportedSleeve(String),
    /// The plan pins a library entry the platform does not have, or one whose hash differs from the platform's.
    EntryMismatch(String),
    /// More than one non-closed proposal plan for the account: the source cannot pick, so it refuses loudly.
    AmbiguousProposalPlans { count: usize },
    /// The proposal plan is not `mode = 'paper'` and `state = 'paper'` (live, or halted by a person).
    ProposalPlanNotServable { mode: String, state: String },
}

impl ExclusionReason {
    pub fn code(&self) -> &'static str {
        match self {
            ExclusionReason::CredentialNotFound => "PILOT_CREDENTIAL_NOT_FOUND",
            ExclusionReason::CredentialDeleted => "PILOT_CREDENTIAL_DELETED",
            ExclusionReason::CredentialDisabled => "PILOT_CREDENTIAL_DISABLED",
            ExclusionReason::NotPaperCredential => "PILOT_NOT_PAPER_CREDENTIAL",
            ExclusionReason::NotAlpaca(_) => "PILOT_NOT_ALPACA",
            ExclusionReason::NoActiveMandate => "PILOT_NO_ACTIVE_MANDATE",
            ExclusionReason::MandateNotSigned => "PILOT_MANDATE_NOT_SIGNED",
            ExclusionReason::MandateBodyInvalid(_) => "PILOT_MANDATE_BODY_INVALID",
            ExclusionReason::MandateHashMismatch => "PILOT_MANDATE_HASH_MISMATCH",
            ExclusionReason::AutonomyL3 => "PILOT_AUTONOMY_L3",
            ExclusionReason::OrdersNotGranted => "PILOT_ORDERS_NOT_GRANTED",
            ExclusionReason::NoPilotPlan => "PILOT_NO_PLAN",
            ExclusionReason::PlanBoundToOtherMandate { .. } => "PILOT_PLAN_BOUND_TO_OTHER_MANDATE",
            ExclusionReason::PlanNotPaper(_) => "PILOT_PLAN_NOT_PAPER",
            ExclusionReason::PlanNotPilotOrigin(_) => "PILOT_PLAN_NOT_PILOT_ORIGIN",
            ExclusionReason::StrategyPlanPresent => "PILOT_STRATEGY_PLAN_PRESENT",
            ExclusionReason::PlanBodyInvalid(_) => "PILOT_PLAN_BODY_INVALID",
            ExclusionReason::UnsupportedSleeve(_) => "PILOT_UNSUPPORTED_SLEEVE",
            ExclusionReason::EntryMismatch(_) => "PILOT_ENTRY_MISMATCH",
            ExclusionReason::AmbiguousProposalPlans { .. } => "PILOT_PROPOSAL_PLANS_AMBIGUOUS",
            ExclusionReason::ProposalPlanNotServable { .. } => "PILOT_PROPOSAL_PLAN_NOT_SERVABLE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exclusion {
    pub account_id: Uuid,
    pub reason: ExclusionReason,
}

/// A library entry a plan pins (the pilot pins exactly one; a proposal plan pins one per sleeve).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedEntry {
    pub entry_id: String,
    pub entry_version: u32,
    pub entry_hash: String,
}

/// One account the driver may run, with the facts the runtime builder and the report need. (Named for the pilot it
/// was built for; a proposal-served account is the same shape with `origin = Proposal`.)
#[derive(Debug, Clone, PartialEq)]
pub struct PilotAccount {
    pub active: ActiveAccount,
    pub origin: PlanOrigin,
    pub plan_id: Uuid,
    pub execution: PlanExecution,
    pub credential_label: String,
    /// The fingerprint the PILOT plan pinned (first 8 bytes of SHA-256 of the paper key id, hex): the runtime builder
    /// compares it with the key it actually holds. `None` for a proposal plan, which records no fingerprint.
    pub credential_fingerprint: Option<String>,
    /// The entries the plan pins, in sleeve order.
    pub entries: Vec<PinnedEntry>,
}

/// A full enumeration: who runs and who was refused (and why).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Enumeration {
    pub accounts: Vec<PilotAccount>,
    pub exclusions: Vec<Exclusion>,
}

// ---------------------------------------------------------------------------------------------------
// Rows (exactly the columns read; no secret column)
// ---------------------------------------------------------------------------------------------------

#[derive(diesel::QueryableByName, Debug, Clone)]
pub(crate) struct CredentialRow {
    #[diesel(sql_type = SqlUuid)]
    pub id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub tenant_id: Uuid,
    #[diesel(sql_type = Text)]
    pub exchange: String,
    #[diesel(sql_type = Text)]
    pub label: String,
    #[diesel(sql_type = Bool)]
    pub is_testnet: bool,
    #[diesel(sql_type = Bool)]
    pub is_enabled: bool,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(diesel::QueryableByName, Debug, Clone)]
pub(crate) struct MandateRow {
    #[diesel(sql_type = SqlUuid)]
    pub id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub tenant_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub account_id: Uuid,
    #[diesel(sql_type = Integer)]
    pub version: i32,
    #[diesel(sql_type = Text)]
    pub status: String,
    #[diesel(sql_type = Jsonb)]
    pub body: Value,
    #[diesel(sql_type = Text)]
    pub body_hash: String,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub granted_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub effective_from: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub review_by: Option<DateTime<Utc>>,
}

/// A `rebalancer_pilot_plans` row.
#[derive(diesel::QueryableByName, Debug, Clone)]
pub(crate) struct PlanRow {
    #[diesel(sql_type = SqlUuid)]
    pub id: Uuid,
    #[diesel(sql_type = BigInt)]
    pub seq: i64,
    #[diesel(sql_type = SqlUuid)]
    pub tenant_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub account_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub mandate_id: Uuid,
    #[diesel(sql_type = Integer)]
    pub mandate_version: i32,
    #[diesel(sql_type = Text)]
    pub origin: String,
    #[diesel(sql_type = Text)]
    pub venue_environment: String,
    #[diesel(sql_type = Text)]
    pub execution: String,
    #[diesel(sql_type = Jsonb)]
    pub body: Value,
    #[diesel(sql_type = Text)]
    pub entry_id: String,
    #[diesel(sql_type = Integer)]
    pub entry_version: i32,
    #[diesel(sql_type = Text)]
    pub entry_hash: String,
    #[diesel(sql_type = Text)]
    pub credential_fingerprint: String,
}

/// A non-closed `strategy_plans` row (the proposal-derived plan). `body` is the Engine's `PlanBody`:
/// `{proposal_id, capital: {amount, ccy}, risk_scale, sleeves: [{entry_id, entry_version, entry_hash, weight}]}`.
#[derive(diesel::QueryableByName, Debug, Clone)]
pub(crate) struct ProposalPlanRow {
    #[diesel(sql_type = SqlUuid)]
    pub id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub tenant_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub proposal_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub account_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub mandate_id: Uuid,
    #[diesel(sql_type = Integer)]
    pub mandate_version: i32,
    #[diesel(sql_type = Text)]
    pub mode: String,
    #[diesel(sql_type = Text)]
    pub state: String,
    #[diesel(sql_type = Jsonb)]
    pub body: Value,
}

/// Everything the SQL side hands to the pure evaluation.
#[derive(Debug, Clone, Default)]
pub(crate) struct Rows {
    pub credentials: Vec<CredentialRow>,
    pub mandates: Vec<MandateRow>,
    /// Newest first is NOT assumed: [`evaluate`] picks the highest `seq` per account itself.
    pub plans: Vec<PlanRow>,
    /// Non-closed proposal-derived plans of the targeted accounts (every non-closed state, so a halted plan is a
    /// typed exclusion rather than invisible).
    pub proposal_plans: Vec<ProposalPlanRow>,
}

// ---------------------------------------------------------------------------------------------------
// The pure decision: rows in, descriptors and typed exclusions out
// ---------------------------------------------------------------------------------------------------

fn parse_sleeves(plan: &PlanRow) -> Result<Vec<SleeveSpec>, ExclusionReason> {
    let bad = |m: String| ExclusionReason::PlanBodyInvalid(m);
    let arr = plan.body.get("sleeves").and_then(Value::as_array).ok_or_else(|| bad("body.sleeves is not an array".into()))?;
    if arr.is_empty() {
        return Err(bad("body.sleeves is empty".into()));
    }
    let mut out: Vec<SleeveSpec> = Vec::new();
    let one = Dec::from_i64(1);
    let mut total = Dec::ZERO;
    for (i, s) in arr.iter().enumerate() {
        let text = |k: &str| -> Result<String, ExclusionReason> {
            s.get(k).and_then(Value::as_str).map(str::to_string).ok_or_else(|| bad(format!("sleeve {i}: {k} missing or not a string")))
        };
        let id = text("sleeve_id")?;
        if id.trim().is_empty() || id.contains('+') {
            return Err(bad(format!("sleeve {i}: sleeve_id must be non-empty and contain no '+'")));
        }
        if out.iter().any(|o| o.id == id) {
            return Err(bad(format!("sleeve id {id} appears twice")));
        }
        let kind = match text("kind")?.as_str() {
            "etf_trend" => SleeveKind::EtfTrend,
            other => return Err(ExclusionReason::UnsupportedSleeve(format!("sleeve {id}: kind {other:?} cannot run in the paper pilot (ETF sleeve on Alpaca only)"))),
        };
        let venue = text("venue")?;
        if !venue.eq_ignore_ascii_case(PLAN_VENUE) {
            return Err(ExclusionReason::UnsupportedSleeve(format!("sleeve {id}: venue {venue:?} is not alpaca")));
        }
        let asset_class = text("asset_class")?;
        let quote = s.get("quote").and_then(Value::as_str).unwrap_or("").to_string();
        let share = Dec::parse(text("share")?.trim()).map_err(|e| bad(format!("sleeve {id}: share is not a decimal: {e}")))?;
        if share <= Dec::ZERO || share > one {
            return Err(bad(format!("sleeve {id}: share {share} is not in (0, 1]")));
        }
        total = total.checked_add(share).ok_or_else(|| bad("the sleeve shares overflow".into()))?;
        out.push(SleeveSpec { id, kind, share, venue, asset_class, quote });
    }
    if total > one {
        return Err(bad(format!("the sleeve shares sum to {total}, above 1")));
    }
    // The pilot pins ONE library entry: the columns mirror body.sleeves[0].
    let first = &arr[0];
    let same = first.get("entry_id").and_then(Value::as_str) == Some(plan.entry_id.as_str())
        && first.get("entry_version").and_then(Value::as_i64) == Some(i64::from(plan.entry_version))
        && first.get("entry_hash").and_then(Value::as_str) == Some(plan.entry_hash.as_str());
    if !same {
        return Err(bad("the entry_id/entry_version/entry_hash columns do not match body.sleeves[0]".into()));
    }
    Ok(out)
}

/// The platform's own library must hold the pinned entry at the pinned hash. Shared by both plan sources: a plan
/// (of either origin) whose entry drifted from the platform's library is refused, never run on a different rule.
fn check_entry(entry_id: &str, entry_version: i32, pinned_hash: &str) -> Result<(), ExclusionReason> {
    let lib = seed_library().map_err(|e| ExclusionReason::EntryMismatch(format!("the platform library did not load: {e}")))?;
    let version = u32::try_from(entry_version).map_err(|_| ExclusionReason::EntryMismatch("negative entry version".into()))?;
    let entry = lib
        .iter()
        .find(|e| e.id == entry_id && e.version == version)
        .ok_or_else(|| ExclusionReason::EntryMismatch(format!("the platform has no library entry {entry_id} version {version}")))?;
    let ours = entry_hash(entry);
    if ours != pinned_hash {
        return Err(ExclusionReason::EntryMismatch(format!("entry {entry_id} v{version}: the plan pins hash {pinned_hash} but the platform's is {ours}")));
    }
    Ok(())
}

/// An f64 from the Engine's JSON (`weight`, `risk_scale`) as an exact decimal: via its shortest round-trip text, so
/// `0.25` stays `0.25` and no binary artefact is introduced by a float multiplication here.
fn dec_from_f64(name: &str, v: f64) -> Result<Dec, ExclusionReason> {
    if !v.is_finite() {
        return Err(ExclusionReason::PlanBodyInvalid(format!("{name} is not a finite number")));
    }
    Dec::parse(&format!("{v}")).map_err(|e| ExclusionReason::PlanBodyInvalid(format!("{name} {v} is not representable as a decimal: {e}")))
}

/// The sleeves of a proposal plan (`PlanBody`): `share = weight * risk_scale`, kind from [`ENTRY_SLEEVE_KINDS`],
/// venue/asset class/quote from the kind. Returns the sleeves and the entries they pin, in order.
fn parse_proposal_sleeves(plan: &ProposalPlanRow) -> Result<(Vec<SleeveSpec>, Vec<PinnedEntry>), ExclusionReason> {
    let bad = |m: String| ExclusionReason::PlanBodyInvalid(m);
    let body = &plan.body;
    let body_proposal = body.get("proposal_id").and_then(Value::as_str).ok_or_else(|| bad("body.proposal_id missing or not a string".into()))?;
    if body_proposal.parse::<Uuid>().ok() != Some(plan.proposal_id) {
        return Err(bad(format!("body.proposal_id {body_proposal:?} does not match the row's proposal_id {}", plan.proposal_id)));
    }
    let risk_scale_f = body.get("risk_scale").and_then(Value::as_f64).ok_or_else(|| bad("body.risk_scale missing or not a number".into()))?;
    let one = Dec::from_i64(1);
    let risk_scale = dec_from_f64("risk_scale", risk_scale_f)?;
    if risk_scale <= Dec::ZERO || risk_scale > one {
        return Err(bad(format!("risk_scale {risk_scale} is not in (0, 1]")));
    }
    let arr = body.get("sleeves").and_then(Value::as_array).ok_or_else(|| bad("body.sleeves is not an array".into()))?;
    if arr.is_empty() {
        return Err(bad("body.sleeves is empty".into()));
    }
    // Inverse-vol weights are f64 sums to 1 up to the last bit; tolerate that, and only that.
    let cap = Dec::parse("1.000000001").expect("literal");
    let mut sleeves: Vec<SleeveSpec> = Vec::new();
    let mut entries: Vec<PinnedEntry> = Vec::new();
    let mut total = Dec::ZERO;
    for (i, s) in arr.iter().enumerate() {
        let entry_id = s.get("entry_id").and_then(Value::as_str).ok_or_else(|| bad(format!("sleeve {i}: entry_id missing or not a string")))?.to_string();
        let entry_version = s.get("entry_version").and_then(Value::as_i64).ok_or_else(|| bad(format!("sleeve {i}: entry_version missing or not an integer")))?;
        let entry_version = i32::try_from(entry_version).map_err(|_| bad(format!("sleeve {i}: entry_version {entry_version} out of range")))?;
        let entry_hash = s.get("entry_hash").and_then(Value::as_str).ok_or_else(|| bad(format!("sleeve {i}: entry_hash missing or not a string")))?.to_string();
        let weight_f = s.get("weight").and_then(Value::as_f64).ok_or_else(|| bad(format!("sleeve {i}: weight missing or not a number")))?;
        if entry_id.trim().is_empty() || entry_id.contains('+') {
            return Err(bad(format!("sleeve {i}: entry_id must be non-empty and contain no '+'")));
        }
        if sleeves.iter().any(|o| o.id == entry_id) {
            return Err(bad(format!("entry {entry_id} appears twice")));
        }
        let kind = sleeve_kind_for_entry(&entry_id)
            .ok_or_else(|| ExclusionReason::UnsupportedSleeve(format!("sleeve {entry_id}: no sleeve kind is registered for this library entry (ENTRY_SLEEVE_KINDS)")))?;
        let weight = dec_from_f64(&format!("sleeve {entry_id}: weight"), weight_f)?;
        if weight <= Dec::ZERO || weight > one {
            return Err(bad(format!("sleeve {entry_id}: weight {weight} is not in (0, 1]")));
        }
        let share = weight.checked_mul(risk_scale).ok_or_else(|| bad(format!("sleeve {entry_id}: share overflows")))?;
        if share <= Dec::ZERO || share > one {
            return Err(bad(format!("sleeve {entry_id}: share {share} is not in (0, 1]")));
        }
        total = total.checked_add(weight).ok_or_else(|| bad("the sleeve weights overflow".into()))?;
        let (asset_class, quote) = sleeve_shape(kind);
        // The sleeve id is the entry id: stable, part of the run key and every order tag, unique within the plan.
        sleeves.push(SleeveSpec { id: entry_id.clone(), kind, share, venue: PLAN_VENUE.to_string(), asset_class: asset_class.to_string(), quote: quote.to_string() });
        let version = u32::try_from(entry_version).map_err(|_| ExclusionReason::EntryMismatch("negative entry version".into()))?;
        entries.push(PinnedEntry { entry_id, entry_version: version, entry_hash });
    }
    if total > cap {
        return Err(bad(format!("the sleeve weights sum to {total}, above 1")));
    }
    Ok((sleeves, entries))
}

fn derive_mode(body: &MandateBody, execution: PlanExecution) -> Result<ExecutionMode, ExclusionReason> {
    match body.autonomy.level {
        AutonomyLevel::L3 => Err(ExclusionReason::AutonomyL3),
        AutonomyLevel::L0 | AutonomyLevel::L1 => Ok(ExecutionMode::Assisted),
        AutonomyLevel::L2 => match execution {
            PlanExecution::Assisted => Ok(ExecutionMode::Assisted),
            PlanExecution::PaperOrders => {
                if body.autonomy.may.contains(&AgentAction::PlaceOrders) {
                    Ok(ExecutionMode::Live)
                } else {
                    Err(ExclusionReason::OrdersNotGranted)
                }
            }
        },
    }
}

/// The checked mandate of an account: what both plan paths build the descriptor from.
struct CheckedMandate<'a> {
    row: &'a MandateRow,
    body: MandateBody,
    version: u32,
    effective_from: DateTime<Utc>,
    review_by: DateTime<Utc>,
}

fn evaluate_pilot_plan(target: Target, cred: &CredentialRow, mandate: &CheckedMandate<'_>, plan: &PlanRow, proposal_plans_present: bool) -> Result<PilotAccount, ExclusionReason> {
    if plan.venue_environment != "paper" {
        return Err(ExclusionReason::PlanNotPaper(plan.venue_environment.clone()));
    }
    if plan.origin != "owner_pilot" {
        return Err(ExclusionReason::PlanNotPilotOrigin(plan.origin.clone()));
    }
    if plan.mandate_id != mandate.row.id || plan.mandate_version != mandate.row.version {
        return Err(ExclusionReason::PlanBoundToOtherMandate { plan_version: plan.mandate_version, active_version: mandate.row.version });
    }
    if proposal_plans_present {
        return Err(ExclusionReason::StrategyPlanPresent);
    }
    let execution = PlanExecution::parse(&plan.execution).ok_or_else(|| ExclusionReason::PlanBodyInvalid(format!("unknown execution {:?}", plan.execution)))?;
    let sleeves = parse_sleeves(plan)?;
    check_entry(&plan.entry_id, plan.entry_version, &plan.entry_hash)?;

    // --- the mode, never more than the mandate grants.
    let mode = derive_mode(&mandate.body, execution)?;

    let active = ActiveAccount {
        account_id: target.account_id.to_string(),
        tenant_id: target.tenant_id.to_string(),
        mandate: mandate.body.clone(),
        envelope: MandateEnvelope { version: mandate.version, status: MandateStatus::Active, effective_from: mandate.effective_from, review_by: mandate.review_by },
        plan_approved: true,
        sleeves,
        mode,
    };
    Ok(PilotAccount {
        active,
        origin: PlanOrigin::OwnerPilot,
        plan_id: plan.id,
        execution,
        credential_label: cred.label.clone(),
        credential_fingerprint: Some(plan.credential_fingerprint.clone()),
        entries: vec![PinnedEntry { entry_id: plan.entry_id.clone(), entry_version: u32::try_from(plan.entry_version).unwrap_or_default(), entry_hash: plan.entry_hash.clone() }],
    })
}

fn evaluate_proposal_plan(target: Target, cred: &CredentialRow, mandate: &CheckedMandate<'_>, candidates: &[&ProposalPlanRow]) -> Result<PilotAccount, ExclusionReason> {
    if candidates.len() > 1 {
        return Err(ExclusionReason::AmbiguousProposalPlans { count: candidates.len() });
    }
    let plan = candidates[0];
    if plan.mode != SERVABLE_PLAN_MODE || plan.state != SERVABLE_PLAN_STATE {
        return Err(ExclusionReason::ProposalPlanNotServable { mode: plan.mode.clone(), state: plan.state.clone() });
    }
    if plan.mandate_id != mandate.row.id || plan.mandate_version != mandate.row.version {
        return Err(ExclusionReason::PlanBoundToOtherMandate { plan_version: plan.mandate_version, active_version: mandate.row.version });
    }
    let (sleeves, entries) = parse_proposal_sleeves(plan)?;
    for e in &entries {
        check_entry(&e.entry_id, i32::try_from(e.entry_version).unwrap_or(i32::MAX), &e.entry_hash)?;
    }
    // A proposal plan has no `execution` column: it asks for orders exactly when the mandate grants them.
    let execution = if mandate.body.autonomy.level == AutonomyLevel::L2 && mandate.body.autonomy.may.contains(&AgentAction::PlaceOrders) {
        PlanExecution::PaperOrders
    } else {
        PlanExecution::Assisted
    };
    let mode = derive_mode(&mandate.body, execution)?;

    let active = ActiveAccount {
        account_id: target.account_id.to_string(),
        tenant_id: target.tenant_id.to_string(),
        mandate: mandate.body.clone(),
        envelope: MandateEnvelope { version: mandate.version, status: MandateStatus::Active, effective_from: mandate.effective_from, review_by: mandate.review_by },
        plan_approved: true,
        sleeves,
        mode,
    };
    Ok(PilotAccount { active, origin: PlanOrigin::Proposal, plan_id: plan.id, execution, credential_label: cred.label.clone(), credential_fingerprint: None, entries })
}

fn evaluate_one(target: Target, rows: &Rows, serve_proposal_plans: bool) -> Result<PilotAccount, ExclusionReason> {
    let Target { tenant_id, account_id } = target;
    // --- credential: paper Alpaca, enabled, not deleted, of THIS tenant.
    let cred = rows.credentials.iter().find(|c| c.id == account_id && c.tenant_id == tenant_id).ok_or(ExclusionReason::CredentialNotFound)?;
    if cred.deleted_at.is_some() {
        return Err(ExclusionReason::CredentialDeleted);
    }
    if !cred.is_enabled {
        return Err(ExclusionReason::CredentialDisabled);
    }
    if !cred.is_testnet {
        return Err(ExclusionReason::NotPaperCredential);
    }
    if !ALPACA_EXCHANGES.contains(&cred.exchange.to_ascii_lowercase().as_str()) {
        return Err(ExclusionReason::NotAlpaca(cred.exchange.clone()));
    }

    // --- mandate: the active, signed one of this tenant and account.
    let row = rows
        .mandates
        .iter()
        .find(|m| m.account_id == account_id && m.tenant_id == tenant_id && m.status == "active")
        .ok_or(ExclusionReason::NoActiveMandate)?;
    let (Some(_granted), Some(effective_from), Some(review_by)) = (row.granted_at, row.effective_from, row.review_by) else {
        return Err(ExclusionReason::MandateNotSigned);
    };
    let body = validate_json(&row.body).map_err(|v| ExclusionReason::MandateBodyInvalid(v.iter().map(|x| format!("{}: {}", x.field, x.message)).collect::<Vec<_>>().join("; ")))?;
    if canonical_hash(&body) != row.body_hash {
        return Err(ExclusionReason::MandateHashMismatch);
    }
    let version = u32::try_from(row.version).map_err(|_| ExclusionReason::MandateBodyInvalid("negative mandate version".into()))?;
    let mandate = CheckedMandate { row, body, version, effective_from, review_by };

    // --- the plan: exactly one source. The newest pilot plan row of this tenant and account, or its non-closed
    // proposal plan(s).
    let pilot_plan = rows.plans.iter().filter(|p| p.account_id == account_id && p.tenant_id == tenant_id).max_by_key(|p| p.seq);
    let proposal_plans: Vec<&ProposalPlanRow> = rows.proposal_plans.iter().filter(|p| p.account_id == account_id && p.tenant_id == tenant_id && p.state != "closed").collect();
    match pilot_plan {
        Some(plan) => evaluate_pilot_plan(target, cred, &mandate, plan, !proposal_plans.is_empty()),
        None if serve_proposal_plans && !proposal_plans.is_empty() => evaluate_proposal_plan(target, cred, &mandate, &proposal_plans),
        None => Err(ExclusionReason::NoPilotPlan),
    }
}

/// Rows in, descriptors and typed exclusions out. Pure: no I/O, so every refusal has a test that needs no database.
/// `targets` is evaluated in order; a target is never silently dropped.
pub(crate) fn evaluate(targets: &[Target], rows: &Rows, serve_proposal_plans: bool) -> Enumeration {
    let mut out = Enumeration::default();
    for target in targets {
        match evaluate_one(*target, rows, serve_proposal_plans) {
            Ok(a) => out.accounts.push(a),
            Err(reason) => out.exclusions.push(Exclusion { account_id: target.account_id, reason }),
        }
    }
    out
}

/// The targets of a scope given the proposal plans the database holds: the pilot pair(s), plus (when serving
/// proposals) every (tenant, account) with a non-closed proposal plan. Sorted and de-duplicated, so an account that
/// is both pilot-listed and proposal-planned is evaluated once (and refused once, as two sources).
pub(crate) fn targets_for(scope: &AccountScope, proposal_plans: &[ProposalPlanRow]) -> Vec<Target> {
    let mut set: BTreeSet<Target> = scope.pilot().map(|p| p.targets().collect()).unwrap_or_default();
    if scope.serves_proposal_plans() {
        set.extend(proposal_plans.iter().filter(|p| p.state != "closed").map(|p| Target { tenant_id: p.tenant_id, account_id: p.account_id }));
    }
    set.into_iter().collect()
}

// ---------------------------------------------------------------------------------------------------
// PgAccountSource
// ---------------------------------------------------------------------------------------------------

pub struct PgAccountSource {
    bridge: Bridge,
    scope: AccountScope,
    provenance: Option<Arc<PlanProvenanceRegistry>>,
    last_exclusions: Mutex<Vec<Exclusion>>,
}

const PROPOSAL_PLAN_COLUMNS: &str = "id, tenant_id, proposal_id, account_id, mandate_id, mandate_version, mode::text AS mode, state::text AS state, body";

impl PgAccountSource {
    /// The pilot-only source (today's behaviour): exactly the allow-listed pair(s).
    pub fn new(pool: Pool, allow: PilotAllowList) -> Result<Self, String> {
        Self::with_scope(pool, AccountScope::Pilot(allow))
    }

    pub fn with_scope(pool: Pool, scope: AccountScope) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)?, scope, provenance: None, last_exclusions: Mutex::new(Vec::new()) })
    }

    /// Publish each enumerated account's plan provenance (origin, plan id, `paper`) for `PgRunStore::finish` and
    /// the reconciliation write-back.
    pub fn with_plan_provenance(mut self, registry: Arc<PlanProvenanceRegistry>) -> Self {
        self.provenance = Some(registry);
        self
    }

    pub fn scope(&self) -> &AccountScope {
        &self.scope
    }

    /// The pilot allow-list, if the scope has one.
    pub fn allow_list(&self) -> Option<&PilotAllowList> {
        self.scope.pilot()
    }

    /// The exclusions of the most recent enumeration (the service logs and alerts on them).
    pub fn last_exclusions(&self) -> Vec<Exclusion> {
        self.last_exclusions.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Read the rows of the scope's targets. Every per-account query is bound to (tenant, account) PAIRS
    /// (`unnest` of two parallel arrays), never to a bare account id, so a row of another tenant is never read.
    fn fetch(&self) -> Result<(Vec<Target>, Rows), AccountSourceError> {
        let scope = self.scope.clone();
        self.bridge
            .block_on(move |mut conn| async move {
                // 1. The proposal plans: of the pilot pair(s) only (to refuse two sources), or of every tenant (the
                //    database allow-list) when serving proposals.
                let proposal_plans: Vec<ProposalPlanRow> = if scope.serves_proposal_plans() {
                    diesel::sql_query(format!("SELECT {PROPOSAL_PLAN_COLUMNS} FROM strategy_plans WHERE state <> 'closed' ORDER BY tenant_id, account_id, created_at"))
                        .get_results(&mut conn)
                        .await
                        .map_err(|e| format!("strategy plans: {e}"))?
                } else {
                    let (tenants, accounts) = pair_arrays(scope.pilot().map(|p| p.targets().collect::<Vec<_>>()).unwrap_or_default());
                    diesel::sql_query(format!(
                        "SELECT {PROPOSAL_PLAN_COLUMNS} FROM strategy_plans sp \
                         JOIN unnest($1::uuid[], $2::uuid[]) AS t(tenant_id, account_id) ON sp.tenant_id = t.tenant_id AND sp.account_id = t.account_id \
                         WHERE sp.state <> 'closed' ORDER BY sp.tenant_id, sp.account_id, sp.created_at"
                    ))
                    .bind::<Array<SqlUuid>, _>(&tenants)
                    .bind::<Array<SqlUuid>, _>(&accounts)
                    .get_results(&mut conn)
                    .await
                    .map_err(|e| format!("strategy plans: {e}"))?
                };
                let targets = targets_for(&scope, &proposal_plans);
                let (tenants, accounts) = pair_arrays(targets.clone());

                // 2. Credentials, mandates and pilot plans of exactly those pairs.
                let credentials: Vec<CredentialRow> = diesel::sql_query(
                    "SELECT c.id, c.tenant_id, c.exchange::text AS exchange, c.label::text AS label, c.is_testnet, c.is_enabled, c.deleted_at \
                     FROM exchange_credentials c JOIN unnest($1::uuid[], $2::uuid[]) AS t(tenant_id, account_id) ON c.tenant_id = t.tenant_id AND c.id = t.account_id",
                )
                .bind::<Array<SqlUuid>, _>(&tenants)
                .bind::<Array<SqlUuid>, _>(&accounts)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("credentials: {e}"))?;
                let mandates: Vec<MandateRow> = diesel::sql_query(
                    "SELECT m.id, m.tenant_id, m.account_id, m.version, m.status::text AS status, m.body, m.body_hash::text AS body_hash, m.granted_at, m.effective_from, m.review_by \
                     FROM mandates m JOIN unnest($1::uuid[], $2::uuid[]) AS t(tenant_id, account_id) ON m.tenant_id = t.tenant_id AND m.account_id = t.account_id \
                     WHERE m.status = 'active'",
                )
                .bind::<Array<SqlUuid>, _>(&tenants)
                .bind::<Array<SqlUuid>, _>(&accounts)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("mandates: {e}"))?;
                let plans: Vec<PlanRow> = diesel::sql_query(
                    "SELECT p.id, p.seq, p.tenant_id, p.account_id, p.mandate_id, p.mandate_version, p.origin::text AS origin, p.venue_environment::text AS venue_environment, \
                            p.execution::text AS execution, p.body, p.entry_id::text AS entry_id, p.entry_version, p.entry_hash::text AS entry_hash, \
                            p.credential_fingerprint::text AS credential_fingerprint \
                     FROM rebalancer_pilot_plans p JOIN unnest($1::uuid[], $2::uuid[]) AS t(tenant_id, account_id) ON p.tenant_id = t.tenant_id AND p.account_id = t.account_id \
                     ORDER BY p.seq DESC",
                )
                .bind::<Array<SqlUuid>, _>(&tenants)
                .bind::<Array<SqlUuid>, _>(&accounts)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("pilot plans: {e}"))?;
                Ok((targets, Rows { credentials, mandates, plans, proposal_plans }))
            })
            .map_err(AccountSourceError::Db)
    }

    /// Read the database and decide, keeping the typed exclusions. A database failure is an error (the driver then
    /// runs nothing this tick), never an empty list dressed up as success.
    pub fn enumerate(&self) -> Result<Enumeration, AccountSourceError> {
        let (targets, rows) = self.fetch()?;
        let result = evaluate(&targets, &rows, self.scope.serves_proposal_plans());
        *self.last_exclusions.lock().unwrap_or_else(|e| e.into_inner()) = result.exclusions.clone();
        if let Some(reg) = &self.provenance {
            reg.set_all(result.accounts.iter().map(|a| (a.active.account_id.clone(), PlanProvenance { origin: a.origin, plan_id: a.plan_id, venue_environment: VenueEnvironment::Paper })));
        }
        Ok(result)
    }
}

/// Two parallel arrays for `unnest($1::uuid[], $2::uuid[])`.
fn pair_arrays(targets: Vec<Target>) -> (Vec<Uuid>, Vec<Uuid>) {
    targets.into_iter().map(|t| (t.tenant_id, t.account_id)).unzip()
}

impl AccountSource for PgAccountSource {
    fn active_accounts(&self) -> Result<Vec<ActiveAccount>, String> {
        self.enumerate().map(|e| e.accounts.into_iter().map(|a| a.active).collect()).map_err(|e| e.to_string())
    }
}

/// Map of exclusions by account, for callers that want to look one up.
pub fn exclusions_by_account(exclusions: &[Exclusion]) -> BTreeMap<Uuid, &ExclusionReason> {
    exclusions.iter().map(|e| (e.account_id, &e.reason)).collect()
}

// ---------------------------------------------------------------------------------------------------
// Unit tests of the pure decision (no database): every refusal, the mode table, the allow-list, both sources.
// ---------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const TENANT: Uuid = Uuid::from_u128(0xA);
    const OTHER_TENANT: Uuid = Uuid::from_u128(0xB);
    const ACCOUNT: Uuid = Uuid::from_u128(0x100);
    const OTHER_ACCOUNT: Uuid = Uuid::from_u128(0x101);
    const MANDATE: Uuid = Uuid::from_u128(0x200);
    const PLAN: Uuid = Uuid::from_u128(0x300);
    const PROPOSAL: Uuid = Uuid::from_u128(0x400);
    const STRATEGY_PLAN: Uuid = Uuid::from_u128(0x500);

    fn mandate_body(level: &str, may: &[&str]) -> Value {
        let mut v: Value = serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap();
        v["autonomy"]["level"] = json!(level);
        v["autonomy"]["may"] = json!(may);
        v
    }

    fn library_hash(id: &str) -> String {
        let lib = seed_library().unwrap();
        entry_hash(lib.iter().find(|e| e.id == id && e.version == 1).unwrap())
    }

    fn entry_hash_v1() -> String {
        library_hash("etf_trend_faber")
    }

    fn plan_body(hash: &str) -> Value {
        json!({"sleeves": [{"sleeve_id": "etf", "entry_id": "etf_trend_faber", "entry_version": 1, "entry_hash": hash,
                            "kind": "etf_trend", "share": "1.0", "venue": "alpaca", "asset_class": "us_etf", "quote": ""}]})
    }

    /// The Engine's `PlanBody` shape, exactly.
    fn proposal_body(proposal_id: Uuid, risk_scale: f64, sleeves: &[(&str, f64)]) -> Value {
        json!({
            "proposal_id": proposal_id.to_string(),
            "capital": {"amount": "5000.00", "ccy": "USD"},
            "risk_scale": risk_scale,
            "sleeves": sleeves.iter().map(|(id, w)| json!({"entry_id": id, "entry_version": 1, "entry_hash": library_hash(id), "weight": w})).collect::<Vec<_>>(),
        })
    }

    fn proposal_plan(tenant: Uuid, account: Uuid, mandate: Uuid, body: Value) -> ProposalPlanRow {
        ProposalPlanRow { id: STRATEGY_PLAN, tenant_id: tenant, proposal_id: PROPOSAL, account_id: account, mandate_id: mandate, mandate_version: 1, mode: "paper".into(), state: "paper".into(), body }
    }

    fn set_mandate(rows: &mut Rows, level: &str, may: &[&str]) {
        let body = mandate_body(level, may);
        let parsed: MandateBody = serde_json::from_value(body.clone()).unwrap();
        rows.mandates[0].body = body;
        rows.mandates[0].body_hash = canonical_hash(&parsed);
    }

    fn happy_rows(level: &str, execution: &str) -> Rows {
        let body = mandate_body(level, &["place_orders", "cancel_orders", "halt", "flatten"]);
        let parsed: MandateBody = serde_json::from_value(body.clone()).unwrap();
        let hash = entry_hash_v1();
        Rows {
            credentials: vec![CredentialRow { id: ACCOUNT, tenant_id: TENANT, exchange: "alpaca_paper".into(), label: "pilot".into(), is_testnet: true, is_enabled: true, deleted_at: None }],
            mandates: vec![MandateRow {
                id: MANDATE,
                tenant_id: TENANT,
                account_id: ACCOUNT,
                version: 1,
                status: "active".into(),
                body,
                body_hash: canonical_hash(&parsed),
                granted_at: Some("2026-09-27T00:00:00Z".parse().unwrap()),
                effective_from: Some("2026-09-27T00:00:00Z".parse().unwrap()),
                review_by: Some("2026-12-26T00:00:00Z".parse().unwrap()),
            }],
            plans: vec![PlanRow {
                id: PLAN,
                seq: 1,
                tenant_id: TENANT,
                account_id: ACCOUNT,
                mandate_id: MANDATE,
                mandate_version: 1,
                origin: "owner_pilot".into(),
                venue_environment: "paper".into(),
                execution: execution.into(),
                body: plan_body(&hash),
                entry_id: "etf_trend_faber".into(),
                entry_version: 1,
                entry_hash: hash,
                credential_fingerprint: "0123456789abcdef".into(),
            }],
            proposal_plans: Vec::new(),
        }
    }

    /// The same account with a proposal plan INSTEAD of the pilot plan.
    fn proposal_rows(level: &str, may: &[&str], risk_scale: f64, sleeves: &[(&str, f64)]) -> Rows {
        let mut rows = happy_rows(level, "assisted");
        set_mandate(&mut rows, level, may);
        rows.plans.clear();
        rows.proposal_plans.push(proposal_plan(TENANT, ACCOUNT, MANDATE, proposal_body(PROPOSAL, risk_scale, sleeves)));
        rows
    }

    fn allow() -> PilotAllowList {
        PilotAllowList::new(TENANT, [ACCOUNT]).unwrap()
    }

    fn pilot_targets() -> Vec<Target> {
        allow().targets().collect()
    }

    fn evaluate_pilot(rows: &Rows) -> Enumeration {
        evaluate(&pilot_targets(), rows, false)
    }

    fn evaluate_serving(rows: &Rows) -> Enumeration {
        let targets = targets_for(&AccountScope::PilotAndProposals { pilot: Some(allow()) }, &rows.proposal_plans);
        evaluate(&targets, rows, true)
    }

    fn only_reason(rows: &Rows) -> ExclusionReason {
        let e = evaluate_pilot(rows);
        assert!(e.accounts.is_empty(), "must be excluded: {:?}", e.accounts);
        assert_eq!(e.exclusions.len(), 1);
        e.exclusions.into_iter().next().unwrap().reason
    }

    fn only_reason_serving(rows: &Rows) -> ExclusionReason {
        let e = evaluate_serving(rows);
        assert!(e.accounts.is_empty(), "must be excluded: {:?}", e.accounts);
        assert_eq!(e.exclusions.len(), 1, "{:?}", e.exclusions);
        e.exclusions.into_iter().next().unwrap().reason
    }

    #[test]
    fn the_happy_path_builds_the_descriptor_the_driver_needs() {
        let e = evaluate_pilot(&happy_rows("L2", "assisted"));
        assert!(e.exclusions.is_empty(), "{:?}", e.exclusions);
        let a = &e.accounts[0];
        assert_eq!(a.active.account_id, ACCOUNT.to_string());
        assert_eq!(a.active.tenant_id, TENANT.to_string());
        assert_eq!(a.active.mode, ExecutionMode::Assisted);
        assert!(a.active.plan_approved);
        assert_eq!(a.active.envelope.version, 1);
        assert_eq!(a.active.envelope.status, MandateStatus::Active);
        assert_eq!(a.active.sleeves.len(), 1);
        assert_eq!(a.active.sleeves[0].id, "etf");
        assert_eq!(a.active.sleeves[0].kind, SleeveKind::EtfTrend);
        assert_eq!(a.active.sleeves[0].venue, "alpaca");
        assert_eq!(a.origin, PlanOrigin::OwnerPilot);
        assert_eq!((a.plan_id, a.credential_fingerprint.as_deref(), a.entries[0].entry_version), (PLAN, Some("0123456789abcdef"), 1));
        assert_eq!(a.entries.len(), 1);
    }

    #[test]
    fn the_mode_table_never_grants_more_than_the_mandate() {
        let mode = |level: &str, exec: &str, may: &[&str]| -> Result<ExecutionMode, ExclusionReason> {
            let mut rows = happy_rows(level, exec);
            set_mandate(&mut rows, level, may);
            let e = evaluate_pilot(&rows);
            match e.accounts.into_iter().next() {
                Some(a) => Ok(a.active.mode),
                None => Err(e.exclusions.into_iter().next().unwrap().reason),
            }
        };
        let full = ["place_orders", "cancel_orders", "halt", "flatten"];
        assert_eq!(mode("L2", "assisted", &full), Ok(ExecutionMode::Assisted));
        assert_eq!(mode("L2", "paper_orders", &full), Ok(ExecutionMode::Live), "L2 + paper_orders = the order-placing pipeline mode against the PAPER venue");
        assert_eq!(mode("L2", "paper_orders", &["cancel_orders", "halt", "flatten"]), Err(ExclusionReason::OrdersNotGranted));
        // L0 / L1 never place orders, whatever the plan says.
        assert_eq!(mode("L1", "paper_orders", &["halt"]), Ok(ExecutionMode::Assisted));
        assert_eq!(mode("L0", "paper_orders", &["halt"]), Ok(ExecutionMode::Assisted));
        assert_eq!(mode("L1", "assisted", &["halt"]), Ok(ExecutionMode::Assisted));
        // L3 (live autonomy) is refused outright, for either execution.
        assert_eq!(mode("L3", "assisted", &full), Err(ExclusionReason::AutonomyL3));
        assert_eq!(mode("L3", "paper_orders", &full), Err(ExclusionReason::AutonomyL3));
    }

    #[test]
    fn a_non_paper_credential_is_refused() {
        let mut rows = happy_rows("L2", "assisted");
        rows.credentials[0].is_testnet = false;
        assert_eq!(only_reason(&rows), ExclusionReason::NotPaperCredential);
    }

    #[test]
    fn only_alpaca_exchanges_are_accepted() {
        let mut rows = happy_rows("L2", "assisted");
        rows.credentials[0].exchange = "kraken".into();
        assert_eq!(only_reason(&rows), ExclusionReason::NotAlpaca("kraken".into()));
        rows.credentials[0].exchange = "ALPACA".into();
        assert!(evaluate_pilot(&rows).exclusions.is_empty(), "the exchange match is case-insensitive");
        rows.credentials[0].exchange = "alpaca_live".into();
        assert!(matches!(only_reason(&rows), ExclusionReason::NotAlpaca(_)));
    }

    #[test]
    fn a_deleted_or_disabled_credential_is_refused() {
        let mut rows = happy_rows("L2", "assisted");
        rows.credentials[0].deleted_at = Some("2026-09-01T00:00:00Z".parse().unwrap());
        assert_eq!(only_reason(&rows), ExclusionReason::CredentialDeleted);
        let mut rows = happy_rows("L2", "assisted");
        rows.credentials[0].is_enabled = false;
        assert_eq!(only_reason(&rows), ExclusionReason::CredentialDisabled);
    }

    #[test]
    fn a_credential_of_another_tenant_or_missing_is_not_found() {
        let mut rows = happy_rows("L2", "assisted");
        rows.credentials[0].tenant_id = OTHER_TENANT;
        assert_eq!(only_reason(&rows), ExclusionReason::CredentialNotFound);
        let mut rows = happy_rows("L2", "assisted");
        rows.credentials.clear();
        assert_eq!(only_reason(&rows), ExclusionReason::CredentialNotFound);
    }

    #[test]
    fn a_mandate_that_is_not_active_and_signed_is_refused() {
        for status in ["draft", "superseded", "revoked", "expired"] {
            let mut rows = happy_rows("L2", "assisted");
            rows.mandates[0].status = status.into();
            assert_eq!(only_reason(&rows), ExclusionReason::NoActiveMandate, "{status}");
        }
        let mut rows = happy_rows("L2", "assisted");
        rows.mandates.clear();
        assert_eq!(only_reason(&rows), ExclusionReason::NoActiveMandate);
        // Active in name but unsigned: every grant field matters.
        for field in 0..3 {
            let mut rows = happy_rows("L2", "assisted");
            match field {
                0 => rows.mandates[0].granted_at = None,
                1 => rows.mandates[0].effective_from = None,
                _ => rows.mandates[0].review_by = None,
            }
            assert_eq!(only_reason(&rows), ExclusionReason::MandateNotSigned, "field {field}");
        }
    }

    #[test]
    fn a_mandate_of_another_tenant_or_account_is_not_active_for_this_account() {
        let mut rows = happy_rows("L2", "assisted");
        rows.mandates[0].tenant_id = OTHER_TENANT;
        assert_eq!(only_reason(&rows), ExclusionReason::NoActiveMandate);
        let mut rows = happy_rows("L2", "assisted");
        rows.mandates[0].account_id = OTHER_ACCOUNT;
        assert_eq!(only_reason(&rows), ExclusionReason::NoActiveMandate);
    }

    #[test]
    fn an_invalid_body_or_a_tampered_hash_is_refused() {
        let mut rows = happy_rows("L2", "assisted");
        rows.mandates[0].body["capital"]["min_cash_reserve"] = json!(7);
        assert!(matches!(only_reason(&rows), ExclusionReason::MandateBodyInvalid(_)));
        let mut rows = happy_rows("L2", "assisted");
        rows.mandates[0].body_hash = "f".repeat(64);
        assert_eq!(only_reason(&rows), ExclusionReason::MandateHashMismatch);
        let mut rows = happy_rows("L2", "assisted");
        rows.mandates[0].body = json!({"not": "a mandate"});
        assert!(matches!(only_reason(&rows), ExclusionReason::MandateBodyInvalid(_)));
    }

    #[test]
    fn a_missing_plan_or_a_plan_of_another_tenant_is_refused() {
        let mut rows = happy_rows("L2", "assisted");
        rows.plans.clear();
        assert_eq!(only_reason(&rows), ExclusionReason::NoPilotPlan);
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].tenant_id = OTHER_TENANT;
        assert_eq!(only_reason(&rows), ExclusionReason::NoPilotPlan);
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].account_id = OTHER_ACCOUNT;
        assert_eq!(only_reason(&rows), ExclusionReason::NoPilotPlan);
    }

    #[test]
    fn the_newest_plan_row_wins_so_inserting_an_assisted_row_tightens() {
        // paper_orders first, then a NEWER assisted row: tightened, whatever the order the rows arrive in.
        let mut rows = happy_rows("L2", "paper_orders");
        assert_eq!(evaluate_pilot(&rows).accounts[0].active.mode, ExecutionMode::Live);
        let mut newer = rows.plans[0].clone();
        newer.id = Uuid::from_u128(0x301);
        newer.seq = 2;
        newer.execution = "assisted".into();
        rows.plans.push(newer.clone());
        let e = evaluate_pilot(&rows);
        assert_eq!(e.accounts[0].active.mode, ExecutionMode::Assisted);
        assert_eq!(e.accounts[0].plan_id, newer.id);
        rows.plans.reverse();
        assert_eq!(evaluate_pilot(&rows).accounts[0].active.mode, ExecutionMode::Assisted);
        // And the other way round: a newer paper_orders row loosens.
        let mut rows = happy_rows("L2", "assisted");
        let mut newer = rows.plans[0].clone();
        newer.seq = 5;
        newer.execution = "paper_orders".into();
        rows.plans.push(newer);
        assert_eq!(evaluate_pilot(&rows).accounts[0].active.mode, ExecutionMode::Live);
        rows.plans.reverse();
        assert_eq!(evaluate_pilot(&rows).accounts[0].active.mode, ExecutionMode::Live);
    }

    #[test]
    fn a_plan_bound_to_another_mandate_version_is_refused() {
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].mandate_version = 0;
        assert_eq!(only_reason(&rows), ExclusionReason::PlanBoundToOtherMandate { plan_version: 0, active_version: 1 });
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].mandate_id = Uuid::from_u128(0x999);
        assert!(matches!(only_reason(&rows), ExclusionReason::PlanBoundToOtherMandate { .. }));
    }

    #[test]
    fn a_plan_that_is_not_paper_or_not_pilot_origin_is_refused() {
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].venue_environment = "live".into();
        assert_eq!(only_reason(&rows), ExclusionReason::PlanNotPaper("live".into()));
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].origin = "proposal".into();
        assert_eq!(only_reason(&rows), ExclusionReason::PlanNotPilotOrigin("proposal".into()));
    }

    #[test]
    fn an_account_with_a_proposal_derived_plan_too_is_refused_under_either_scope() {
        let mut rows = happy_rows("L2", "assisted");
        rows.proposal_plans.push(proposal_plan(TENANT, ACCOUNT, MANDATE, json!({})));
        assert_eq!(only_reason(&rows), ExclusionReason::StrategyPlanPresent, "pilot scope");
        assert_eq!(only_reason_serving(&rows), ExclusionReason::StrategyPlanPresent, "serving scope: two sources is two sources");
        // Even a halted proposal plan counts: only `closed` frees the account for the pilot.
        rows.proposal_plans[0].state = "halted".into();
        assert_eq!(only_reason(&rows), ExclusionReason::StrategyPlanPresent);
        rows.proposal_plans[0].state = "closed".into();
        assert!(evaluate_pilot(&rows).exclusions.is_empty(), "a closed proposal plan is no plan");
        let mut rows = happy_rows("L2", "assisted");
        rows.proposal_plans.push(proposal_plan(TENANT, OTHER_ACCOUNT, MANDATE, json!({})));
        assert!(evaluate_pilot(&rows).exclusions.is_empty(), "another account's strategy plan does not matter");
    }

    #[test]
    fn sleeves_the_paper_pilot_cannot_run_are_refused() {
        let with = |mutate: &dyn Fn(&mut Value)| -> ExclusionReason {
            let mut rows = happy_rows("L2", "assisted");
            mutate(&mut rows.plans[0].body["sleeves"][0]);
            only_reason(&rows)
        };
        assert!(matches!(with(&|s| s["venue"] = json!("kraken")), ExclusionReason::UnsupportedSleeve(_)));
        assert!(matches!(with(&|s| s["kind"] = json!("crypto_trend")), ExclusionReason::UnsupportedSleeve(_)));
        assert!(matches!(with(&|s| s["share"] = json!("1.5")), ExclusionReason::PlanBodyInvalid(_)));
        assert!(matches!(with(&|s| s["share"] = json!("0")), ExclusionReason::PlanBodyInvalid(_)));
        assert!(matches!(with(&|s| s["share"] = json!("abc")), ExclusionReason::PlanBodyInvalid(_)));
        assert!(matches!(with(&|s| s["sleeve_id"] = json!("a+b")), ExclusionReason::PlanBodyInvalid(_)));
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].body = json!({"sleeves": []});
        assert!(matches!(only_reason(&rows), ExclusionReason::PlanBodyInvalid(_)));
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].body = json!({"nope": 1});
        assert!(matches!(only_reason(&rows), ExclusionReason::PlanBodyInvalid(_)));
        // Shares that sum above 1.
        let mut rows = happy_rows("L2", "assisted");
        let second = json!({"sleeve_id": "etf2", "entry_id": "etf_trend_faber", "entry_version": 1, "entry_hash": entry_hash_v1(), "kind": "etf_trend", "share": "0.5", "venue": "alpaca", "asset_class": "us_etf", "quote": ""});
        rows.plans[0].body["sleeves"].as_array_mut().unwrap().push(second);
        assert!(matches!(only_reason(&rows), ExclusionReason::PlanBodyInvalid(m) if m.contains("above 1")));
    }

    #[test]
    fn the_pinned_library_entry_must_exist_and_hash_the_same() {
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].entry_hash = "0".repeat(64);
        rows.plans[0].body["sleeves"][0]["entry_hash"] = json!("0".repeat(64));
        assert!(matches!(only_reason(&rows), ExclusionReason::EntryMismatch(_)));
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].entry_version = 99;
        rows.plans[0].body["sleeves"][0]["entry_version"] = json!(99);
        assert!(matches!(only_reason(&rows), ExclusionReason::EntryMismatch(_)));
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].entry_id = "no_such_entry".into();
        rows.plans[0].body["sleeves"][0]["entry_id"] = json!("no_such_entry");
        assert!(matches!(only_reason(&rows), ExclusionReason::EntryMismatch(_)));
        // The columns must mirror the body.
        let mut rows = happy_rows("L2", "assisted");
        rows.plans[0].entry_hash = "0".repeat(64);
        assert!(matches!(only_reason(&rows), ExclusionReason::PlanBodyInvalid(_)));
    }

    #[test]
    fn only_allow_listed_accounts_are_ever_considered_and_every_one_is_accounted_for() {
        let mut rows = happy_rows("L2", "assisted");
        // A second, fully valid account of the same tenant that is NOT on the allow-list.
        let mut cred = rows.credentials[0].clone();
        cred.id = OTHER_ACCOUNT;
        rows.credentials.push(cred);
        let mut m = rows.mandates[0].clone();
        m.id = Uuid::from_u128(0x201);
        m.account_id = OTHER_ACCOUNT;
        rows.mandates.push(m);
        let mut p = rows.plans[0].clone();
        p.id = Uuid::from_u128(0x301);
        p.account_id = OTHER_ACCOUNT;
        p.mandate_id = Uuid::from_u128(0x201);
        rows.plans.push(p);
        let e = evaluate_pilot(&rows);
        assert_eq!(e.accounts.iter().map(|a| a.active.account_id.clone()).collect::<Vec<_>>(), vec![ACCOUNT.to_string()], "the other account is never returned");
        assert!(e.exclusions.is_empty(), "and is not even reported: it is outside the pilot");
        // With both on the list, both run; an allow-listed account that does not exist is reported, not dropped.
        let both = PilotAllowList::new(TENANT, [ACCOUNT, OTHER_ACCOUNT, Uuid::from_u128(0x102)]).unwrap();
        let e = evaluate(&both.targets().collect::<Vec<_>>(), &rows, false);
        assert_eq!(e.accounts.len(), 2);
        assert_eq!(e.exclusions, vec![Exclusion { account_id: Uuid::from_u128(0x102), reason: ExclusionReason::CredentialNotFound }]);
        // The allow-list's tenant is the only tenant: a different list tenant finds nothing.
        let wrong = PilotAllowList::new(OTHER_TENANT, [ACCOUNT]).unwrap();
        assert_eq!(evaluate(&wrong.targets().collect::<Vec<_>>(), &rows, false).exclusions[0].reason, ExclusionReason::CredentialNotFound);
    }

    #[test]
    fn the_allow_list_refuses_to_be_empty_or_nil() {
        assert!(PilotAllowList::new(TENANT, []).is_err(), "an empty list never means all accounts");
        assert!(PilotAllowList::new(Uuid::nil(), [ACCOUNT]).is_err());
        assert!(PilotAllowList::new(TENANT, [Uuid::nil()]).is_err());
        assert!(PilotAllowList::parse("not-a-uuid", &ACCOUNT.to_string()).is_err());
        assert!(PilotAllowList::parse(&TENANT.to_string(), "").is_err());
        assert!(PilotAllowList::parse(&TENANT.to_string(), "zzz").is_err());
        let l = PilotAllowList::parse(&format!(" {TENANT} "), &format!("{ACCOUNT}, {OTHER_ACCOUNT}")).unwrap();
        assert_eq!(l.account_ids().count(), 2);
        assert!(l.contains(TENANT, ACCOUNT) && !l.contains(OTHER_TENANT, ACCOUNT) && !l.contains(TENANT, Uuid::from_u128(1)));
    }

    #[test]
    fn exclusion_codes_are_stable_and_unique() {
        let all = [
            ExclusionReason::CredentialNotFound,
            ExclusionReason::CredentialDeleted,
            ExclusionReason::CredentialDisabled,
            ExclusionReason::NotPaperCredential,
            ExclusionReason::NotAlpaca(String::new()),
            ExclusionReason::NoActiveMandate,
            ExclusionReason::MandateNotSigned,
            ExclusionReason::MandateBodyInvalid(String::new()),
            ExclusionReason::MandateHashMismatch,
            ExclusionReason::AutonomyL3,
            ExclusionReason::OrdersNotGranted,
            ExclusionReason::NoPilotPlan,
            ExclusionReason::PlanBoundToOtherMandate { plan_version: 0, active_version: 0 },
            ExclusionReason::PlanNotPaper(String::new()),
            ExclusionReason::PlanNotPilotOrigin(String::new()),
            ExclusionReason::StrategyPlanPresent,
            ExclusionReason::PlanBodyInvalid(String::new()),
            ExclusionReason::UnsupportedSleeve(String::new()),
            ExclusionReason::EntryMismatch(String::new()),
            ExclusionReason::AmbiguousProposalPlans { count: 2 },
            ExclusionReason::ProposalPlanNotServable { mode: String::new(), state: String::new() },
        ];
        let codes: BTreeSet<&str> = all.iter().map(ExclusionReason::code).collect();
        assert_eq!(codes.len(), all.len());
        assert!(codes.iter().all(|c| c.starts_with("PILOT_")));
    }

    // -----------------------------------------------------------------------------------------------
    // The proposal-derived plan source
    // -----------------------------------------------------------------------------------------------

    #[test]
    fn a_proposal_plan_is_served_only_under_the_serving_scope_and_the_pilot_scope_is_unchanged() {
        let rows = proposal_rows("L2", &["place_orders", "cancel_orders", "halt", "flatten"], 0.5, &[("etf_trend_faber", 0.6), ("crypto_trend_100d", 0.4)]);
        // Pilot scope (flag off): exactly today's answer -- no pilot plan, nothing else is looked at.
        assert_eq!(only_reason(&rows), ExclusionReason::NoPilotPlan);
        // Serving scope: the plan runs.
        let e = evaluate_serving(&rows);
        assert!(e.exclusions.is_empty(), "{:?}", e.exclusions);
        let a = &e.accounts[0];
        assert_eq!(a.origin, PlanOrigin::Proposal);
        assert_eq!(a.plan_id, STRATEGY_PLAN);
        assert_eq!(a.credential_fingerprint, None, "a proposal plan pins no key fingerprint");
        assert_eq!(a.active.tenant_id, TENANT.to_string());
        assert_eq!(a.active.mode, ExecutionMode::Live, "L2 + place_orders: orders on the paper venue");
        assert_eq!(a.execution, PlanExecution::PaperOrders);
        assert_eq!(a.active.sleeves.len(), 2);
        let etf = &a.active.sleeves[0];
        assert_eq!((etf.id.as_str(), etf.kind, etf.venue.as_str(), etf.asset_class.as_str(), etf.quote.as_str()), ("etf_trend_faber", SleeveKind::EtfTrend, "alpaca", "us_etf", ""));
        assert_eq!(etf.share, Dec::parse("0.30").unwrap(), "share = weight 0.6 * risk_scale 0.5");
        let crypto = &a.active.sleeves[1];
        assert_eq!((crypto.id.as_str(), crypto.kind, crypto.asset_class.as_str(), crypto.quote.as_str()), ("crypto_trend_100d", SleeveKind::CryptoTrend, "crypto_spot", "USD"));
        assert_eq!(crypto.share, Dec::parse("0.20").unwrap());
        assert_eq!(a.entries.iter().map(|e| e.entry_id.as_str()).collect::<Vec<_>>(), ["etf_trend_faber", "crypto_trend_100d"]);
        assert_eq!(a.entries[1].entry_hash, library_hash("crypto_trend_100d"));
    }

    #[test]
    fn a_proposal_plan_asks_for_orders_exactly_when_the_mandate_grants_them() {
        let mode = |level: &str, may: &[&str]| -> Result<ExecutionMode, ExclusionReason> {
            let rows = proposal_rows(level, may, 1.0, &[("etf_trend_faber", 1.0)]);
            let e = evaluate_serving(&rows);
            match e.accounts.into_iter().next() {
                Some(a) => Ok(a.active.mode),
                None => Err(e.exclusions.into_iter().next().unwrap().reason),
            }
        };
        assert_eq!(mode("L2", &["place_orders", "halt"]), Ok(ExecutionMode::Live));
        assert_eq!(mode("L2", &["cancel_orders", "halt", "flatten"]), Ok(ExecutionMode::Assisted), "no place_orders: assisted, never refused (the plan did not ask)");
        assert_eq!(mode("L1", &["place_orders", "halt"]), Ok(ExecutionMode::Assisted));
        assert_eq!(mode("L0", &["halt"]), Ok(ExecutionMode::Assisted));
        assert_eq!(mode("L3", &["place_orders", "halt"]), Err(ExclusionReason::AutonomyL3));
    }

    #[test]
    fn a_proposal_plan_that_is_live_halted_or_ambiguous_is_a_typed_exclusion() {
        let full = ["place_orders", "cancel_orders", "halt", "flatten"];
        let mut rows = proposal_rows("L2", &full, 1.0, &[("etf_trend_faber", 1.0)]);
        rows.proposal_plans[0].state = "halted".into();
        assert_eq!(only_reason_serving(&rows), ExclusionReason::ProposalPlanNotServable { mode: "paper".into(), state: "halted".into() });
        rows.proposal_plans[0].state = "live".into();
        rows.proposal_plans[0].mode = "live".into();
        assert_eq!(only_reason_serving(&rows), ExclusionReason::ProposalPlanNotServable { mode: "live".into(), state: "live".into() });
        rows.proposal_plans[0].state = "paper".into();
        assert_eq!(only_reason_serving(&rows), ExclusionReason::ProposalPlanNotServable { mode: "live".into(), state: "paper".into() }, "mode live with state paper is still not servable");
        // Two non-closed rows: refused loudly, never "the newest wins".
        let mut rows = proposal_rows("L2", &full, 1.0, &[("etf_trend_faber", 1.0)]);
        let mut second = rows.proposal_plans[0].clone();
        second.id = Uuid::from_u128(0x501);
        second.proposal_id = Uuid::from_u128(0x401);
        rows.proposal_plans.push(second);
        assert_eq!(only_reason_serving(&rows), ExclusionReason::AmbiguousProposalPlans { count: 2 });
        // A closed second row does not count.
        rows.proposal_plans[1].state = "closed".into();
        assert!(evaluate_serving(&rows).exclusions.is_empty());
    }

    #[test]
    fn a_proposal_plan_bound_to_another_mandate_version_is_refused() {
        let full = ["place_orders", "cancel_orders", "halt", "flatten"];
        let mut rows = proposal_rows("L2", &full, 1.0, &[("etf_trend_faber", 1.0)]);
        rows.proposal_plans[0].mandate_version = 2;
        assert_eq!(only_reason_serving(&rows), ExclusionReason::PlanBoundToOtherMandate { plan_version: 2, active_version: 1 });
        rows.proposal_plans[0].mandate_version = 1;
        rows.proposal_plans[0].mandate_id = Uuid::from_u128(0x999);
        assert!(matches!(only_reason_serving(&rows), ExclusionReason::PlanBoundToOtherMandate { .. }));
    }

    #[test]
    fn a_proposal_plan_body_is_validated_and_an_unknown_entry_or_drifted_hash_is_refused() {
        let full = ["place_orders", "cancel_orders", "halt", "flatten"];
        let with = |mutate: &dyn Fn(&mut Value)| -> ExclusionReason {
            let mut rows = proposal_rows("L2", &full, 1.0, &[("etf_trend_faber", 1.0)]);
            mutate(&mut rows.proposal_plans[0].body);
            only_reason_serving(&rows)
        };
        // An entry the table does not know: typed, never guessed.
        assert!(matches!(with(&|b| b["sleeves"][0]["entry_id"] = json!("fx_tsmom_12m")), ExclusionReason::UnsupportedSleeve(m) if m.contains("fx_tsmom_12m")));
        // A known entry whose hash is not the platform's.
        assert!(matches!(with(&|b| b["sleeves"][0]["entry_hash"] = json!("0".repeat(64))), ExclusionReason::EntryMismatch(_)));
        assert!(matches!(with(&|b| b["sleeves"][0]["entry_version"] = json!(99)), ExclusionReason::EntryMismatch(_)));
        // Body shape.
        assert!(matches!(with(&|b| b["proposal_id"] = json!(Uuid::from_u128(0x401).to_string())), ExclusionReason::PlanBodyInvalid(m) if m.contains("proposal_id")));
        assert!(matches!(with(&|b| *b = json!({})), ExclusionReason::PlanBodyInvalid(_)));
        assert!(matches!(with(&|b| b["sleeves"] = json!([])), ExclusionReason::PlanBodyInvalid(_)));
        assert!(matches!(with(&|b| b["risk_scale"] = json!(0.0)), ExclusionReason::PlanBodyInvalid(_)));
        assert!(matches!(with(&|b| b["risk_scale"] = json!(1.5)), ExclusionReason::PlanBodyInvalid(_)));
        assert!(matches!(with(&|b| b["risk_scale"] = json!("0.5")), ExclusionReason::PlanBodyInvalid(_)));
        assert!(matches!(with(&|b| b["sleeves"][0]["weight"] = json!(0.0)), ExclusionReason::PlanBodyInvalid(_)));
        assert!(matches!(with(&|b| b["sleeves"][0]["weight"] = json!(1.2)), ExclusionReason::PlanBodyInvalid(_)));
        // Weights above 1 in total; a last-bit float overshoot is tolerated.
        let mut rows = proposal_rows("L2", &full, 1.0, &[("etf_trend_faber", 0.6), ("crypto_trend_100d", 0.6)]);
        assert!(matches!(only_reason_serving(&rows), ExclusionReason::PlanBodyInvalid(m) if m.contains("above 1")));
        rows = proposal_rows("L2", &full, 1.0, &[("etf_trend_faber", 0.7), ("crypto_trend_100d", 0.30000000000000004)]);
        assert!(evaluate_serving(&rows).exclusions.is_empty(), "f64 weights that sum to 1 up to the last bit are fine");
        // The same entry twice.
        rows = proposal_rows("L2", &full, 1.0, &[("etf_trend_faber", 0.5), ("etf_trend_faber", 0.5)]);
        assert!(matches!(only_reason_serving(&rows), ExclusionReason::PlanBodyInvalid(m) if m.contains("twice")));
    }

    #[test]
    fn tenants_are_isolated_the_targets_come_from_the_plans_and_each_is_evaluated_under_its_own_tenant() {
        let full = ["place_orders", "cancel_orders", "halt", "flatten"];
        let mut rows = proposal_rows("L2", &full, 1.0, &[("etf_trend_faber", 1.0)]);
        // Tenant B: its own credential, mandate and proposal plan for a different account.
        let other_mandate = Uuid::from_u128(0x201);
        let mut cred = rows.credentials[0].clone();
        cred.id = OTHER_ACCOUNT;
        cred.tenant_id = OTHER_TENANT;
        rows.credentials.push(cred);
        let mut m = rows.mandates[0].clone();
        m.id = other_mandate;
        m.tenant_id = OTHER_TENANT;
        m.account_id = OTHER_ACCOUNT;
        rows.mandates.push(m);
        let mut p = proposal_plan(OTHER_TENANT, OTHER_ACCOUNT, other_mandate, proposal_body(Uuid::from_u128(0x401), 1.0, &[("etf_trend_faber", 1.0)]));
        p.id = Uuid::from_u128(0x501);
        p.proposal_id = Uuid::from_u128(0x401);
        rows.proposal_plans.push(p);

        // No pilot pair at all: the targets are exactly the two plans' (tenant, account) pairs.
        let scope = AccountScope::PilotAndProposals { pilot: None };
        let targets = targets_for(&scope, &rows.proposal_plans);
        assert_eq!(targets, vec![Target { tenant_id: TENANT, account_id: ACCOUNT }, Target { tenant_id: OTHER_TENANT, account_id: OTHER_ACCOUNT }]);
        let e = evaluate(&targets, &rows, true);
        assert!(e.exclusions.is_empty(), "{:?}", e.exclusions);
        assert_eq!(e.accounts.iter().map(|a| (a.active.tenant_id.clone(), a.active.account_id.clone())).collect::<Vec<_>>(), vec![(TENANT.to_string(), ACCOUNT.to_string()), (OTHER_TENANT.to_string(), OTHER_ACCOUNT.to_string())]);
        assert_ne!(e.accounts[0].plan_id, e.accounts[1].plan_id);

        // A plan whose account id exists only under ANOTHER tenant: the target carries the plan's tenant, so the
        // credential is not found -- tenant B's credential is never used for tenant A's plan.
        rows.proposal_plans[1].tenant_id = TENANT;
        let targets = targets_for(&scope, &rows.proposal_plans);
        let e = evaluate(&targets, &rows, true);
        assert_eq!(e.accounts.len(), 1);
        assert_eq!(e.exclusions, vec![Exclusion { account_id: OTHER_ACCOUNT, reason: ExclusionReason::CredentialNotFound }]);

        // The pilot scope never targets a proposal account, whatever the database holds.
        assert_eq!(targets_for(&AccountScope::Pilot(allow()), &rows.proposal_plans), vec![Target { tenant_id: TENANT, account_id: ACCOUNT }]);
        // Closed plans produce no target.
        let mut closed = rows.proposal_plans.clone();
        closed.iter_mut().for_each(|p| p.state = "closed".into());
        assert!(targets_for(&scope, &closed).is_empty());
    }

    #[test]
    fn a_pilot_pair_and_a_proposal_account_are_served_side_by_side_each_from_its_own_source() {
        let mut rows = happy_rows("L2", "paper_orders");
        // Tenant B's proposal account.
        let other_mandate = Uuid::from_u128(0x201);
        let mut cred = rows.credentials[0].clone();
        cred.id = OTHER_ACCOUNT;
        cred.tenant_id = OTHER_TENANT;
        rows.credentials.push(cred);
        let mut m = rows.mandates[0].clone();
        m.id = other_mandate;
        m.tenant_id = OTHER_TENANT;
        m.account_id = OTHER_ACCOUNT;
        rows.mandates.push(m);
        rows.proposal_plans.push(proposal_plan(OTHER_TENANT, OTHER_ACCOUNT, other_mandate, proposal_body(PROPOSAL, 0.8, &[("etf_trend_faber", 1.0)])));
        let e = evaluate_serving(&rows);
        assert!(e.exclusions.is_empty(), "{:?}", e.exclusions);
        assert_eq!(e.accounts.len(), 2);
        let pilot = e.accounts.iter().find(|a| a.active.account_id == ACCOUNT.to_string()).unwrap();
        let proposal = e.accounts.iter().find(|a| a.active.account_id == OTHER_ACCOUNT.to_string()).unwrap();
        assert_eq!((pilot.origin, pilot.plan_id), (PlanOrigin::OwnerPilot, PLAN));
        assert_eq!((proposal.origin, proposal.plan_id), (PlanOrigin::Proposal, STRATEGY_PLAN));
        assert_eq!(proposal.active.sleeves[0].share, Dec::parse("0.8").unwrap());
    }

    #[test]
    fn the_entry_to_sleeve_kind_table_is_exactly_the_two_runnable_entries() {
        assert_eq!(sleeve_kind_for_entry("etf_trend_faber"), Some(SleeveKind::EtfTrend));
        assert_eq!(sleeve_kind_for_entry("crypto_trend_100d"), Some(SleeveKind::CryptoTrend));
        assert_eq!(sleeve_kind_for_entry("fx_tsmom_12m"), None, "FX has no runnable sleeve kind in this service");
        assert_eq!(sleeve_kind_for_entry(""), None);
    }
}
