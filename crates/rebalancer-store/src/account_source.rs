//! `PgAccountSource`: the Postgres-backed [`AccountSource`] for the paper pilot (plan
//! `product-mandate/PAPER_PILOT_DRAGONSTONE_PLAN.md`, slice S-5).
//!
//! It answers ONE question: which accounts may the driver run, in which execution mode, with which sleeves? It
//! builds the [`ActiveAccount`] descriptors the driver needs and constructs NO broker, reads NO secret and calls no
//! vendor: the `AccountRuntime` builder (S-6) comes later.
//!
//! # Strict scoping
//! * A PILOT ALLOW-LIST ([`PilotAllowList`]: one tenant id and explicit account ids) is a required input. Every
//!   query is bound to that tenant AND to those account ids; nothing else is ever selected. An empty allow-list is
//!   a construction error, never "all accounts".
//! * Only the non-secret `exchange_credentials` columns are selected (no `*_encrypted` column), so the service's
//!   database role needs no access to them.
//!
//! # What makes an account eligible (each failure is a typed [`ExclusionReason`], never a silent skip)
//! * its credential exists under the allow-listed tenant, is not deleted, is enabled, is a TESTNET (paper) credential
//!   and an Alpaca exchange (paper only: anything else is refused);
//! * it has an ACTIVE, SIGNED mandate (status `active`, granted, effective and review dates present) whose body
//!   validates and whose stored hash matches; a mandate at autonomy L3 (live) is refused;
//! * it has an owner-authored pilot plan (`rebalancer_pilot_plans`, newest row wins), bound to exactly that mandate
//!   version, paper environment and pilot origin, whose sleeves parse, are ETF-on-Alpaca only and pin a library
//!   entry whose hash equals the platform's own;
//! * it has NO proposal-derived plan (`strategy_plans`): a plan cannot come from two sources.
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
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccountSourceError {
    #[error("ACCOUNT_SOURCE_BAD_ALLOW_LIST: {0}")]
    BadAllowList(String),
    #[error("ACCOUNT_SOURCE_DB_ERROR: {0}")]
    Db(String),
}

/// What the plan's `execution` column says (the DB CHECK allows exactly these two).
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

/// Why an allow-listed account was NOT handed to the driver. The account is excluded, never partially run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExclusionReason {
    /// No credential row for this id under the allow-listed tenant (missing, or it belongs to another tenant).
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
    NoPilotPlan,
    /// The plan is bound to another mandate id or version than the active one.
    PlanBoundToOtherMandate { plan_version: i32, active_version: i32 },
    /// The plan row is not `venue_environment = 'paper'` (the DB CHECK forbids it; this is the second line).
    PlanNotPaper(String),
    /// The plan row is not `origin = 'owner_pilot'`.
    PlanNotPilotOrigin(String),
    /// The account ALSO has a proposal-derived plan (`strategy_plans`): a plan cannot come from two sources.
    StrategyPlanPresent,
    PlanBodyInvalid(String),
    /// A sleeve the paper pilot cannot run (any venue but Alpaca, any kind but the ETF sleeve).
    UnsupportedSleeve(String),
    /// The plan pins a library entry the platform does not have, or one whose hash differs from the platform's.
    EntryMismatch(String),
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exclusion {
    pub account_id: Uuid,
    pub reason: ExclusionReason,
}

/// One account the driver may run, with the pilot facts the runtime builder (S-6) and the report need.
#[derive(Debug, Clone, PartialEq)]
pub struct PilotAccount {
    pub active: ActiveAccount,
    pub plan_id: Uuid,
    pub execution: PlanExecution,
    pub credential_label: String,
    /// The fingerprint the plan pinned (first 8 bytes of SHA-256 of the paper key id, hex): S-6 compares it with the
    /// key it actually holds.
    pub credential_fingerprint: String,
    pub entry_id: String,
    pub entry_version: u32,
    pub entry_hash: String,
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

#[derive(diesel::QueryableByName)]
struct AccountIdRow {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
}

/// Everything the SQL side hands to the pure evaluation.
#[derive(Debug, Clone, Default)]
pub(crate) struct Rows {
    pub credentials: Vec<CredentialRow>,
    pub mandates: Vec<MandateRow>,
    /// Newest first is NOT assumed: [`evaluate`] picks the highest `seq` per account itself.
    pub plans: Vec<PlanRow>,
    /// Accounts that also have a proposal-derived (non-closed) plan.
    pub strategy_plan_accounts: BTreeSet<Uuid>,
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
        if !venue.eq_ignore_ascii_case("alpaca") {
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

/// The platform's own library must hold the pinned entry at the pinned hash.
fn check_entry(plan: &PlanRow) -> Result<(), ExclusionReason> {
    let lib = seed_library().map_err(|e| ExclusionReason::EntryMismatch(format!("the platform library did not load: {e}")))?;
    let version = u32::try_from(plan.entry_version).map_err(|_| ExclusionReason::EntryMismatch("negative entry version".into()))?;
    let entry = lib
        .iter()
        .find(|e| e.id == plan.entry_id && e.version == version)
        .ok_or_else(|| ExclusionReason::EntryMismatch(format!("the platform has no library entry {} version {version}", plan.entry_id)))?;
    let ours = entry_hash(entry);
    if ours != plan.entry_hash {
        return Err(ExclusionReason::EntryMismatch(format!("entry {} v{version}: the plan pins hash {} but the platform's is {ours}", plan.entry_id, plan.entry_hash)));
    }
    Ok(())
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

fn evaluate_one(allow: &PilotAllowList, account: Uuid, rows: &Rows) -> Result<PilotAccount, ExclusionReason> {
    // --- credential: paper Alpaca, enabled, not deleted, of THIS tenant.
    let cred = rows
        .credentials
        .iter()
        .find(|c| c.id == account && c.tenant_id == allow.tenant_id)
        .ok_or(ExclusionReason::CredentialNotFound)?;
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
    let mandate = rows
        .mandates
        .iter()
        .find(|m| m.account_id == account && m.tenant_id == allow.tenant_id && m.status == "active")
        .ok_or(ExclusionReason::NoActiveMandate)?;
    let (Some(_granted), Some(effective_from), Some(review_by)) = (mandate.granted_at, mandate.effective_from, mandate.review_by) else {
        return Err(ExclusionReason::MandateNotSigned);
    };
    let body = validate_json(&mandate.body).map_err(|v| ExclusionReason::MandateBodyInvalid(v.iter().map(|x| format!("{}: {}", x.field, x.message)).collect::<Vec<_>>().join("; ")))?;
    if canonical_hash(&body) != mandate.body_hash {
        return Err(ExclusionReason::MandateHashMismatch);
    }
    let version = u32::try_from(mandate.version).map_err(|_| ExclusionReason::MandateBodyInvalid("negative mandate version".into()))?;

    // --- plan: the newest pilot plan row of this tenant and account.
    let plan = rows
        .plans
        .iter()
        .filter(|p| p.account_id == account && p.tenant_id == allow.tenant_id)
        .max_by_key(|p| p.seq)
        .ok_or(ExclusionReason::NoPilotPlan)?;
    if plan.venue_environment != "paper" {
        return Err(ExclusionReason::PlanNotPaper(plan.venue_environment.clone()));
    }
    if plan.origin != "owner_pilot" {
        return Err(ExclusionReason::PlanNotPilotOrigin(plan.origin.clone()));
    }
    if plan.mandate_id != mandate.id || plan.mandate_version != mandate.version {
        return Err(ExclusionReason::PlanBoundToOtherMandate { plan_version: plan.mandate_version, active_version: mandate.version });
    }
    if rows.strategy_plan_accounts.contains(&account) {
        return Err(ExclusionReason::StrategyPlanPresent);
    }
    let execution = PlanExecution::parse(&plan.execution).ok_or_else(|| ExclusionReason::PlanBodyInvalid(format!("unknown execution {:?}", plan.execution)))?;
    let sleeves = parse_sleeves(plan)?;
    check_entry(plan)?;

    // --- the mode, never more than the mandate grants.
    let mode = derive_mode(&body, execution)?;

    let active = ActiveAccount {
        account_id: account.to_string(),
        tenant_id: allow.tenant_id.to_string(),
        mandate: body,
        envelope: MandateEnvelope { version, status: MandateStatus::Active, effective_from, review_by },
        plan_approved: true,
        sleeves,
        mode,
    };
    Ok(PilotAccount {
        active,
        plan_id: plan.id,
        execution,
        credential_label: cred.label.clone(),
        credential_fingerprint: plan.credential_fingerprint.clone(),
        entry_id: plan.entry_id.clone(),
        entry_version: u32::try_from(plan.entry_version).unwrap_or_default(),
        entry_hash: plan.entry_hash.clone(),
    })
}

/// Rows in, descriptors and typed exclusions out. Pure: no I/O, so every refusal has a test that needs no database.
pub(crate) fn evaluate(allow: &PilotAllowList, rows: &Rows) -> Enumeration {
    let mut out = Enumeration::default();
    for account in allow.account_ids() {
        match evaluate_one(allow, account, rows) {
            Ok(a) => out.accounts.push(a),
            Err(reason) => out.exclusions.push(Exclusion { account_id: account, reason }),
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------------
// PgAccountSource
// ---------------------------------------------------------------------------------------------------

pub struct PgAccountSource {
    bridge: Bridge,
    allow: PilotAllowList,
    provenance: Option<Arc<PlanProvenanceRegistry>>,
    last_exclusions: Mutex<Vec<Exclusion>>,
}

impl PgAccountSource {
    pub fn new(pool: Pool, allow: PilotAllowList) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)?, allow, provenance: None, last_exclusions: Mutex::new(Vec::new()) })
    }

    /// Publish each enumerated account's plan provenance (`owner_pilot`, plan id, `paper`) for `PgRunStore::finish`.
    pub fn with_plan_provenance(mut self, registry: Arc<PlanProvenanceRegistry>) -> Self {
        self.provenance = Some(registry);
        self
    }

    pub fn allow_list(&self) -> &PilotAllowList {
        &self.allow
    }

    /// The exclusions of the most recent enumeration (the service logs and alerts on them).
    pub fn last_exclusions(&self) -> Vec<Exclusion> {
        self.last_exclusions.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn fetch(&self) -> Result<Rows, AccountSourceError> {
        let tenant = self.allow.tenant_id;
        let ids: Vec<Uuid> = self.allow.account_ids().collect();
        self.bridge
            .block_on(move |mut conn| async move {
                let credentials: Vec<CredentialRow> = diesel::sql_query(
                    "SELECT id, tenant_id, exchange::text AS exchange, label::text AS label, is_testnet, is_enabled, deleted_at \
                     FROM exchange_credentials WHERE tenant_id = $1 AND id = ANY($2)",
                )
                .bind::<SqlUuid, _>(tenant)
                .bind::<Array<SqlUuid>, _>(&ids)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("credentials: {e}"))?;
                let mandates: Vec<MandateRow> = diesel::sql_query(
                    "SELECT id, tenant_id, account_id, version, status::text AS status, body, body_hash::text AS body_hash, granted_at, effective_from, review_by \
                     FROM mandates WHERE tenant_id = $1 AND account_id = ANY($2) AND status = 'active'",
                )
                .bind::<SqlUuid, _>(tenant)
                .bind::<Array<SqlUuid>, _>(&ids)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("mandates: {e}"))?;
                let plans: Vec<PlanRow> = diesel::sql_query(
                    "SELECT id, seq, tenant_id, account_id, mandate_id, mandate_version, origin::text AS origin, venue_environment::text AS venue_environment, \
                            execution::text AS execution, body, entry_id::text AS entry_id, entry_version, entry_hash::text AS entry_hash, \
                            credential_fingerprint::text AS credential_fingerprint \
                     FROM rebalancer_pilot_plans WHERE tenant_id = $1 AND account_id = ANY($2) ORDER BY seq DESC",
                )
                .bind::<SqlUuid, _>(tenant)
                .bind::<Array<SqlUuid>, _>(&ids)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("pilot plans: {e}"))?;
                let strategy_plans: Vec<AccountIdRow> = diesel::sql_query(
                    "SELECT DISTINCT account_id AS id FROM strategy_plans WHERE tenant_id = $1 AND account_id = ANY($2) AND state <> 'closed'",
                )
                .bind::<SqlUuid, _>(tenant)
                .bind::<Array<SqlUuid>, _>(&ids)
                .get_results(&mut conn)
                .await
                .map_err(|e| format!("strategy plans: {e}"))?;
                Ok(Rows { credentials, mandates, plans, strategy_plan_accounts: strategy_plans.into_iter().map(|r| r.id).collect() })
            })
            .map_err(AccountSourceError::Db)
    }

    /// Read the database and decide, keeping the typed exclusions. A database failure is an error (the driver then
    /// runs nothing this tick), never an empty list dressed up as success.
    pub fn enumerate(&self) -> Result<Enumeration, AccountSourceError> {
        let rows = self.fetch()?;
        let result = evaluate(&self.allow, &rows);
        *self.last_exclusions.lock().unwrap_or_else(|e| e.into_inner()) = result.exclusions.clone();
        if let Some(reg) = &self.provenance {
            reg.set_all(result.accounts.iter().map(|a| {
                (a.active.account_id.clone(), PlanProvenance { origin: PlanOrigin::OwnerPilot, plan_id: a.plan_id, venue_environment: VenueEnvironment::Paper })
            }));
        }
        Ok(result)
    }
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
// Unit tests of the pure decision (no database): every refusal, the mode table, the allow-list.
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

    fn mandate_body(level: &str, may: &[&str]) -> Value {
        let mut v: Value = serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap();
        v["autonomy"]["level"] = json!(level);
        v["autonomy"]["may"] = json!(may);
        v
    }

    fn entry_hash_v1() -> String {
        let lib = seed_library().unwrap();
        entry_hash(lib.iter().find(|e| e.id == "etf_trend_faber" && e.version == 1).unwrap())
    }

    fn plan_body(hash: &str) -> Value {
        json!({"sleeves": [{"sleeve_id": "etf", "entry_id": "etf_trend_faber", "entry_version": 1, "entry_hash": hash,
                            "kind": "etf_trend", "share": "1.0", "venue": "alpaca", "asset_class": "us_etf", "quote": ""}]})
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
            strategy_plan_accounts: BTreeSet::new(),
        }
    }

    fn allow() -> PilotAllowList {
        PilotAllowList::new(TENANT, [ACCOUNT]).unwrap()
    }

    fn only_reason(rows: &Rows) -> ExclusionReason {
        let e = evaluate(&allow(), rows);
        assert!(e.accounts.is_empty(), "must be excluded: {:?}", e.accounts);
        assert_eq!(e.exclusions.len(), 1);
        e.exclusions.into_iter().next().unwrap().reason
    }

    #[test]
    fn the_happy_path_builds_the_descriptor_the_driver_needs() {
        let e = evaluate(&allow(), &happy_rows("L2", "assisted"));
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
        assert_eq!((a.plan_id, a.credential_fingerprint.as_str(), a.entry_version), (PLAN, "0123456789abcdef", 1));
    }

    #[test]
    fn the_mode_table_never_grants_more_than_the_mandate() {
        let mode = |level: &str, exec: &str, may: &[&str]| -> Result<ExecutionMode, ExclusionReason> {
            let mut rows = happy_rows(level, exec);
            set_mandate(&mut rows, level, may);
            let e = evaluate(&allow(), &rows);
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
        assert!(evaluate(&allow(), &rows).exclusions.is_empty(), "the exchange match is case-insensitive");
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
        assert_eq!(evaluate(&allow(), &rows).accounts[0].active.mode, ExecutionMode::Live);
        let mut newer = rows.plans[0].clone();
        newer.id = Uuid::from_u128(0x301);
        newer.seq = 2;
        newer.execution = "assisted".into();
        rows.plans.push(newer.clone());
        let e = evaluate(&allow(), &rows);
        assert_eq!(e.accounts[0].active.mode, ExecutionMode::Assisted);
        assert_eq!(e.accounts[0].plan_id, newer.id);
        rows.plans.reverse();
        assert_eq!(evaluate(&allow(), &rows).accounts[0].active.mode, ExecutionMode::Assisted);
        // And the other way round: a newer paper_orders row loosens.
        let mut rows = happy_rows("L2", "assisted");
        let mut newer = rows.plans[0].clone();
        newer.seq = 5;
        newer.execution = "paper_orders".into();
        rows.plans.push(newer);
        assert_eq!(evaluate(&allow(), &rows).accounts[0].active.mode, ExecutionMode::Live);
        rows.plans.reverse();
        assert_eq!(evaluate(&allow(), &rows).accounts[0].active.mode, ExecutionMode::Live);
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
    fn an_account_with_a_proposal_derived_plan_too_is_refused() {
        let mut rows = happy_rows("L2", "assisted");
        rows.strategy_plan_accounts.insert(ACCOUNT);
        assert_eq!(only_reason(&rows), ExclusionReason::StrategyPlanPresent);
        let mut rows = happy_rows("L2", "assisted");
        rows.strategy_plan_accounts.insert(OTHER_ACCOUNT);
        assert!(evaluate(&allow(), &rows).exclusions.is_empty(), "another account's strategy plan does not matter");
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
        let e = evaluate(&allow(), &rows);
        assert_eq!(e.accounts.iter().map(|a| a.active.account_id.clone()).collect::<Vec<_>>(), vec![ACCOUNT.to_string()], "the other account is never returned");
        assert!(e.exclusions.is_empty(), "and is not even reported: it is outside the pilot");
        // With both on the list, both run; an allow-listed account that does not exist is reported, not dropped.
        let both = PilotAllowList::new(TENANT, [ACCOUNT, OTHER_ACCOUNT, Uuid::from_u128(0x102)]).unwrap();
        let e = evaluate(&both, &rows);
        assert_eq!(e.accounts.len(), 2);
        assert_eq!(e.exclusions, vec![Exclusion { account_id: Uuid::from_u128(0x102), reason: ExclusionReason::CredentialNotFound }]);
        // The allow-list's tenant is the only tenant: a different list tenant finds nothing.
        let wrong = PilotAllowList::new(OTHER_TENANT, [ACCOUNT]).unwrap();
        assert_eq!(evaluate(&wrong, &rows).exclusions[0].reason, ExclusionReason::CredentialNotFound);
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
        ];
        let codes: BTreeSet<&str> = all.iter().map(ExclusionReason::code).collect();
        assert_eq!(codes.len(), all.len());
        assert!(codes.iter().all(|c| c.starts_with("PILOT_")));
    }
}
