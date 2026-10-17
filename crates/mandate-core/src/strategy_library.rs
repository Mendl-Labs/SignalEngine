//! Curated library of documented strategies, the deterministic eligibility
//! check against a mandate, sizing to the drawdown ladder, and the selection
//! log (plan: `product-mandate/PLAN.md`, "The documented-strategy flow").
//!
//! This module is PURE: types, embedded seed data, and functions of their
//! arguments. It reads no database, calls no broker, has no async and is
//! consumed by nothing yet, so it has no runtime effect.
//!
//! What the pieces are for:
//! - [`LibraryEntry`]: one strategy, its exact rule text, what it needs
//!   (shorting, leverage, venues), the parameters fixed at published values,
//!   honest expected ranges with their sources, and an evidence label.
//! - [`eligibility`]: entry requirements against a [`MandateBody`] (what the
//!   USER forbids) and against [`PlatformCapabilities`] (what the PLATFORM
//!   cannot do today). Returns every reason tagged with its origin (mandate,
//!   platform or advisory), and separates "blocks everything" from "blocks live
//!   only" (a venue that is paper-only today).
//! - [`fit_scale_to_ladder`]: the largest scale at which a historical return
//!   series replays inside the mandate's first drawdown rung.
//! - [`inverse_vol_weights`]: weights by the pre-declared rule.
//! - [`SelectionRecord`]: the log an agent's selection must produce, and its
//!   validator. There is NO selection logic here.
//!
//! Conventions: ratios are fractions (0.10 = 10%); drawdowns are positive
//! magnitudes; a "gross multiple" is a multiple of sleeve (or account) equity.
//! Only `super::mandate` (T4b: was `crate::mandate`, changed so this file resolves identically whether it is a
//! top-level child of the real Engine crate or path-included one level deeper by
//! `program/tests/repl_include/mod.rs` -- see `program/tests/replication_tamper.rs`) and plain dependencies are
//! used, so the module compiles inside the Engine and in a small isolated test crate alike.

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// `super::mandate`, not `crate::mandate`: this module is always exactly one level below whatever its crate root is
// (`program/src/lib.rs`'s top-level `pub mod strategy_library;` in the real Engine binary; `program/tests/repl_include/mod.rs`'s
// top-level `pub mod strategy_library;` when this file is path-included by `program/tests/replication_tamper.rs`,
// T4b) -- `super::` reaches the same sibling `mandate` module either way, so this file needs no change depending on
// which tree includes it. `super::mandate` == `crate::mandate` in the real crate.
use super::mandate::{LadderRung, MandateBody};

/// Version of the library entry schema.
pub const LIBRARY_SCHEMA_VERSION: u32 = 1;

/// Absolute tolerance when comparing a required leverage to a cap.
const LEVERAGE_EPS: f64 = 1e-9;

/// Absolute tolerance when deciding whether a replayed drawdown reached a rung:
/// a drawdown within this of a rung counts as reaching it (the safe direction),
/// so float noise such as 0.09999999999999998 cannot hide a boundary hit.
const RUNG_EPS: f64 = 1e-12;

/// Smallest scale a fit may return. A sleeve that would have to be scaled below
/// this is not usable under the ladder at all; that is reported as an error
/// rather than as a scale of essentially zero.
pub const MIN_FIT_SCALE: f64 = 1e-6;

// ---------------------------------------------------------------------------
// Library types
// ---------------------------------------------------------------------------

/// How far the platform has verified an entry. Nothing is `ReplicatedOnPlatform`
/// until the platform's own engine reproduces the reference implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Evidence {
    /// The platform engine reproduced the reference within tolerance.
    ReplicatedOnPlatform,
    /// AUTONOMOUS_LIBRARY_CURATION_PLAN.md section 2.2: a genuinely new, fifth label -- never a field alongside the
    /// other four, and never a reuse of `ReplicatedOnPlatform`'s meaning. Reachable ONLY via
    /// `TransitionCause::AgentVerifiedReplication`, itself reachable only from `derive_evidence` when a stored
    /// review's `reviewer_kind: Agent` comes from an identity in `agent_review::certified::CERTIFIED_AGENT_REVIEWER_IDENTITIES`.
    /// A customer must always be able to tell an agent-verified entry from a human-verified one: this label
    /// serializes as `"agent_verified"`, never `"replicated_on_platform"`, and the two are never directly
    /// interchangeable (no transition edge in either direction between them -- see
    /// [`evidence_transition_allowed`]'s doc comment).
    AgentVerified,
    /// A reference implementation exists elsewhere; the platform has not
    /// replicated it.
    ReferenceOnly,
    /// No independent implementation has been checked.
    Unverified,
    /// WP9.3: the rule needs something today's platform cannot do (shorting,
    /// joint vol-targeting, cross-asset rebalance, a live-ready venue) --
    /// parked, not silently downgraded to an approximation. A separate
    /// terminal state from the other three: it says "the platform can't run
    /// this," not "how much evidence exists that the rule works."
    PlatformCapabilityBlocked,
}

/// WP9.4 (IMPLEMENTATION_PLAN.md): is moving an entry from `from` to `to` a
/// valid state transition. Enforces `unverified -> reference_only ->
/// replicated_on_platform`, in order -- no entry may jump straight from
/// `unverified` to `replicated_on_platform`, and nothing may move backward
/// except an explicit demotion (a decayed entry dropping back to
/// `reference_only`, caught by WP9.6's monitor -- allowed, since a human/the
/// monitor demoting something is different from an unearned promotion).
/// `PlatformCapabilityBlocked` is reachable from either of the other two
/// non-replicated states and is terminal until the platform gap closes, at
/// which point it re-enters at `unverified` (a capability existing again
/// doesn't un-verify anything that was already checked before the block).
pub fn evidence_transition_allowed(from: Evidence, to: Evidence) -> bool {
    use Evidence::*;
    match (from, to) {
        // No-op: staying put is always fine.
        (a, b) if a == b => true,
        // Forward progress, one step at a time.
        (Unverified, ReferenceOnly) => true,
        // T4, stage T4b (`product-mandate/T4_FIXED_RULE_ENDPOINT_PLAN.md` section 2.4): `reference_only ->
        // replicated_on_platform` is no longer a valid transition WITHOUT a cause -- see
        // [`evidence_transition_allowed_with`], the only way this promotion is ever allowed, and only when the cause
        // is a genuine [`TransitionCause::VerifiedReplication`]. This function stays the CAUSE-LESS form used
        // everywhere a promotion is never in question (curation edits, decay, capability changes); it now correctly
        // refuses the one transition that always needed independent evidence behind it.
        // (ReferenceOnly, ReplicatedOnPlatform) was `true` here before T4b; see the updated test below.
        // The one explicitly-allowed backward move: decay demotes, never
        // silently deletes.
        (ReplicatedOnPlatform, ReferenceOnly) => true,
        // AUTONOMOUS_LIBRARY_CURATION_PLAN.md section 2.2: the same demotion, for the agent-verified label. There is
        // deliberately NO (AgentVerified, ReplicatedOnPlatform) edge or its reverse in either direction here or in
        // `evidence_transition_allowed_with`: a human independently re-reviewing an agent-verified entry produces a
        // NEW promotion (ReferenceOnly -> ReplicatedOnPlatform, cause VerifiedReplication, from the same underlying
        // run), never a same-label mutation of AgentVerified -- this keeps the historical record honest ("this entry
        // was once agent-verified, then a human independently verified it too" is two distinct facts, not one edited
        // field).
        (AgentVerified, ReferenceOnly) => true,
        // Platform-blocked is reachable any time the platform turns out not
        // to support what the rule needs, and clears back to Unverified (not
        // straight to ReferenceOnly/ReplicatedOnPlatform) once the gap closes.
        (Unverified, PlatformCapabilityBlocked) => true,
        (ReferenceOnly, PlatformCapabilityBlocked) => true,
        (PlatformCapabilityBlocked, Unverified) => true,
        // Everything else -- straight to ReplicatedOnPlatform from
        // Unverified, straight from PlatformCapabilityBlocked to a verified
        // state, ReplicatedOnPlatform demoted more than one step, etc. -- is
        // not a valid single transition.
        _ => false,
    }
}

/// Why a call site believes an evidence transition is legitimate (T4b, design section 2.4: "`evidence_transition_allowed`
/// gains a sibling `evidence_transition_allowed_with(from, to, cause: TransitionCause)`"). A plain marker, not a
/// value carrying the verification itself -- the caller (`replication::evidence::derive_evidence`, the only real
/// caller) must already have PERFORMED the verification before it can even construct a
/// `replication::evidence::VerifiedReplication` to promote from; this enum exists so the promotion rule itself
/// (`evidence_transition_allowed_with`) can require that a cause was given, without `strategy_library.rs` needing to
/// name `replication`'s types to do it (keeping this module's own stated invariant: "Only `crate::mandate` and plain
/// dependencies are used").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionCause {
    /// A person edited the embedded JSON in a reviewed PR (the `unverified -> reference_only` path, and any other
    /// hand-curated move).
    Curation,
    /// `replication::evidence::derive_evidence` is promoting `reference_only -> replicated_on_platform` on the
    /// strength of an already-verified `replication::evidence::VerifiedReplication`. The ONLY cause that unlocks
    /// that one transition.
    VerifiedReplication,
    /// AUTONOMOUS_LIBRARY_CURATION_PLAN.md section 2.2: `replication::evidence::derive_evidence` is promoting
    /// `reference_only -> agent_verified` on the strength of a `StoredReview{reviewer_kind: Agent}` from a
    /// certified agent-reviewer identity. The ONLY cause that unlocks that one transition -- mirrors
    /// `VerifiedReplication` exactly, for the sibling label.
    AgentVerifiedReplication,
    /// A `decay_check` run (or a revoked review) is demoting `replicated_on_platform -> reference_only`.
    Decay,
    /// The platform gained or lost a capability (`platform_capability_blocked` in or out).
    CapabilityChange,
}

/// [`evidence_transition_allowed`], but able to allow `reference_only -> replicated_on_platform` when (and only
/// when) `cause` proves that transition was backed by real, independent verification, not a curator's or an agent's
/// say-so. Every other pair is identical to the cause-less form.
pub fn evidence_transition_allowed_with(from: Evidence, to: Evidence, cause: TransitionCause) -> bool {
    if from == Evidence::ReferenceOnly && to == Evidence::ReplicatedOnPlatform {
        return matches!(cause, TransitionCause::VerifiedReplication);
    }
    // AUTONOMOUS_LIBRARY_CURATION_PLAN.md section 2.2: the sibling promotion, gated on its own distinct cause. Note
    // this is `ReferenceOnly -> AgentVerified` only -- there is no `ReplicatedOnPlatform -> AgentVerified` or
    // `AgentVerified -> ReplicatedOnPlatform` edge anywhere in this function or the cause-less form.
    if from == Evidence::ReferenceOnly && to == Evidence::AgentVerified {
        return matches!(cause, TransitionCause::AgentVerifiedReplication);
    }
    evidence_transition_allowed(from, to)
}

/// A tiny, plain provenance marker `intake_gate` accepts in place of a full
/// `replication::evidence::VerifiedReplication` (T4b, design section 2.4 item 5: "`intake_gate` gains
/// `Option<&VerifiedReplication>`"). `intake_gate`'s OWN verdict never changes shape because of this parameter --
/// WP9.5 still requires a human sign-off before `replicated_on_platform`, so `intake_gate` never returns that label
/// regardless of what it is given here; the parameter is informational provenance only (surfaced on
/// [`IntakeVerdict::verified_replication`]), and keeping it a plain struct (rather than the concrete
/// `replication::evidence::VerifiedReplication`) keeps this module free of any dependency on `replication`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedReplicationProvenance {
    pub run_id: String,
    pub reviewed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceKind {
    Daily,
    Monthly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rebalance {
    pub kind: RebalanceKind,
    /// Exact timing and calendar caveats, in words.
    pub detail: String,
}

/// Whether the published rule rebalances ACROSS instruments jointly (e.g. back
/// to fixed weights, or to a joint volatility target).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossAssetRebalance {
    /// The rule cannot be run faithfully without it.
    Needed,
    /// The rule rebalances to fixed weights; independent per-instrument
    /// sub-accounts drift between rebalances, which is an approximation and not
    /// a block.
    Preferred,
    NotNeeded,
}

/// Measured gross exposure of a reference implementation, kept with its source
/// so the figure can be regenerated. `None` on an entry means not measured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrossExposureStats {
    pub n_month_ends: u32,
    pub first_month_end: String,
    pub last_month_end: String,
    pub mean: f64,
    pub median: f64,
    pub p90: f64,
    pub max: f64,
    pub min: f64,
    /// Path of the saved script and its output.
    pub source: String,
    pub note: String,
}

/// What an entry needs. Two different questions are checked from it by
/// [`eligibility`]: what the user's MANDATE forbids, and what the PLATFORM
/// cannot do today ([`PlatformCapabilities`]). `needs_shorting` and the gross
/// leverage are properties of the STRATEGY, not blockers by themselves.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirements {
    pub needs_shorting: bool,
    /// Gross notional, as a multiple of sleeve equity, the rule needs to run
    /// (1.0 = fully invested, unleveraged). Compared with the mandate's
    /// leverage cap and gross limit. For a rule whose gross varies over time
    /// this is a stated judgment, explained in `notes`.
    pub min_leverage_gross: f64,
    /// Measured distribution of the rule's gross exposure, when available.
    #[serde(default)]
    pub gross_exposure_reference: Option<GrossExposureStats>,
    /// The rule sizes the whole sleeve jointly to a portfolio volatility
    /// target (not per instrument).
    pub needs_portfolio_vol_target: bool,
    /// Largest absolute weight, as a multiple of sleeve equity, the published
    /// rule can assign to one instrument; `None` when the rule does not bound it.
    #[serde(default)]
    pub max_abs_weight_per_instrument: Option<f64>,
    pub cross_asset_rebalance: CrossAssetRebalance,
    /// Asset classes, using the mandate's vocabulary (e.g. `us_etf`, `fx`,
    /// `crypto_spot`). ALL must be permitted by a mandate.
    pub asset_classes: Vec<String>,
    /// Venues where the entry can trade at all (paper or live). Which venues
    /// are live-ready is a PLATFORM fact: see [`PlatformCapabilities`].
    pub venues_supported: Vec<String>,
    /// History needed to compute the first signal (not to judge the strategy).
    pub min_history_years: f64,
    #[serde(default)]
    pub notes: Vec<String>,
}

// ---------------------------------------------------------------------------
// What the platform can do today
// ---------------------------------------------------------------------------

/// What the platform can and cannot do TODAY, kept apart from what a mandate
/// permits. Change a flag only after verifying it, and update its date.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformCapabilities {
    /// Short positions (sell-to-open) exist in the backtest/portfolio engine.
    /// TRUE: verified 2026-09-21 in `BacktestingCore/portfoliomanager/src/lib.rs`
    /// (`PositionSide::Short`, sell-to-open-short handling).
    pub shorting_in_backtest: bool,
    /// Account-level leverage exists (deployments accept a leverage multiplier).
    /// TRUE: verified 2026-09-21 in `BacktestingCore/portfoliomanager/src/lib.rs`
    /// (`leverage.max(1.0)`, margin model). The maximum multiplier is NOT
    /// asserted here.
    pub account_leverage: bool,
    /// The whole sleeve can be sized jointly to a portfolio volatility target.
    /// FALSE: stage-1 record `SCORECARD_PREREGISTRATION.md`, Amendment 8 and the
    /// raw-engine results (2026-09-21): each asset is sized alone, the agent
    /// stated a joint 10% volatility target cannot be expressed.
    pub portfolio_vol_target: bool,
    /// Positions can be rebalanced jointly across instruments (back to target
    /// weights, or to a joint target). FALSE: same source: "no cross-asset
    /// rebalancing", independent per-asset sub-accounts (2026-09-21).
    pub cross_asset_rebalance: bool,
    /// A per-instrument position size above 1.0 (a multiple of equity) can be
    /// expressed. FALSE: same source: `compute_position_sizes` is capped at
    /// 1.0 and each asset is sized alone (2026-09-21).
    pub per_instrument_weight_above_1: bool,
    /// Venues where LIVE trading is wired today. Only kraken (crypto): Alpaca
    /// and OANDA are paper/practice only (product-mandate/WORKFLOWS.md,
    /// 2026-09-21). Do not add a venue that has not been verified live.
    pub venues_live_ready: Vec<String>,
}

/// The platform's capabilities as verified on 2026-09-21.
pub fn current_platform_capabilities() -> PlatformCapabilities {
    PlatformCapabilities {
        shorting_in_backtest: true,
        account_leverage: true,
        portfolio_vol_target: false,
        cross_asset_rebalance: false,
        per_instrument_weight_above_1: false,
        venues_live_ready: vec!["kraken".to_string()],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeRole {
    /// A band the results are expected to fall in.
    PerformanceBand,
    /// A sanity bound: outside it, suspect the test (look-ahead, bug); inside
    /// it says nothing about quality.
    IntegrityBound,
}

/// A wide, honest band with the place the number came from. `None` on a side
/// means no bound is known there (never a guess).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedRange {
    pub metric: String,
    pub low: Option<f64>,
    pub high: Option<f64>,
    pub role: RangeRole,
    pub source: String,
    pub note: String,
}

impl ExpectedRange {
    /// True when `value` is inside the known bounds (unknown bounds never
    /// exclude), false for a non-finite value.
    pub fn contains(&self, value: f64) -> bool {
        let above_low = match self.low {
            Some(l) => value >= l,
            None => true,
        };
        let below_high = match self.high {
            Some(h) => value <= h,
            None => true,
        };
        value.is_finite() && above_low && below_high
    }

    /// True when at least one bound is known, i.e. the check can fail.
    pub fn is_informative(&self) -> bool {
        self.low.is_some() || self.high.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CostProfile {
    /// Annual turnover as a multiple of sleeve equity; `None` when not
    /// measured (the note says so).
    pub turnover_per_year_estimate: Option<f64>,
    /// Signal flips per year in the reference run: a trade-count proxy, NOT
    /// turnover.
    pub signal_flips_per_year_shadow: Option<f64>,
    pub note: String,
}

/// How a decision made by the rule becomes an action, as DATA rather than prose (stage T4, design section 2.3;
/// `COUNCIL_ETF_TIMING_AND_CADENCE.md` Ruling 10: "Operating procedure as data; single source of truth" -- "Library
/// entry version 2 (new `rule_hash`) carries `operating_procedure`... The pipeline and weightsim read the same
/// fields... Deliberately not a mandate field. Two enum variants and integers only, no scheduling DSL."). Present
/// only on a version-2-or-later entry (`#[serde(default)]`, `Option`) so every existing entry's serialisation,
/// `entry_hash` and `rule_hash` are byte-identical to before this field existed; when present it participates in
/// [`crate::replication::binding::rule_hash`] (design section 2.3: "operating_procedure (only when present)").
/// Two-valued/integer fields only, deliberately no scheduling DSL, per the ruling's own last sentence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatingProcedure {
    /// When a NEW decision is made: `"monthly"` or `"daily"` (mirrors `Rebalance.kind`, but as the compiled rule's
    /// own vocabulary rather than the mandate-facing one).
    pub decision_schedule: String,
    /// How often the standing decision is checked/replayed even when it does not change: `"daily"` for both compiled
    /// library rules today.
    pub evaluation: String,
    /// When a decision made at a bar's close may first be acted on: `"first_bar_of_next_month"` (ETF, monthly) or
    /// `"next_bar"` (crypto, daily).
    pub actionable_after: String,
    /// `"on_decision"` (trade only when the target changes, units drift between decisions) or `"every_bar"` (trade
    /// back to the standing target every bar) -- the compiled rule's `rebalance_policy()`.
    pub rebalance_policy: String,
    /// The pre-registered live delay, in bars, between a decision and the bar it is acted on (Ruling 4: "ETF
    /// `execution_delay_bars = 1`... crypto `0` plus slippage").
    pub expected_delay_bars: u32,
    /// How a delayed decision is actually filled once it IS acted on: `"next_open"` (unassisted) or `"assisted"`
    /// (a human acts within the window, adding latency).
    pub execution_assumption: String,
    /// How a missed decision (an outage, a paused account) is made up: `"cap"` (act on the newest decision only,
    /// never queue and replay every missed one) is the only value either compiled rule uses today.
    pub catch_up: String,
}

/// One human-stated numeric fact a cited source ("`sources[source_index]`") is claimed to say -- item 2 of
/// `product-mandate/LIBRARY_ENTRY_ROBUSTNESS_PLAN.md` (section 2.2). Field-for-field identical to
/// `citation_verification::verify::NumericClaim`'s own shape (`metric`, `value`, `period`), DUPLICATED here rather
/// than imported: `strategy_library.rs`'s own module doc comment requires this file to compile using only
/// `super::mandate`/plain dependencies (so `program/tests/repl_include/mod.rs` can path-include it without also
/// mirroring the much larger `citation_verification` tree), and `citation_verification` is not mirrored there.
/// `library_citation::check::library_citation_check` converts this to the real
/// `citation_verification::verify::NumericClaim` field-by-field before calling `verify_claim` -- see that module's
/// own doc comment for why this is safe (identical JSON shape, proven by a round-trip test).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibraryNumericClaim {
    /// Free-text metric name, e.g. "Sharpe Ratio improvement over buy-and-hold".
    pub metric: String,
    pub value: f64,
    /// Optional period the number applies to, e.g. "1973-2008".
    #[serde(default)]
    pub period: Option<String>,
}

/// What a `LibrarySourceClaim` claims its cited source says -- field-for-field identical to
/// `citation_verification::verify::CitationClaim` (`quotes`, `numbers`, `sample_period`); see
/// [`LibraryNumericClaim`]'s doc comment for exactly why this is a duplicate type rather than an import.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibraryCitationClaim {
    #[serde(default)]
    pub quotes: Vec<String>,
    #[serde(default)]
    pub numbers: Vec<LibraryNumericClaim>,
    #[serde(default)]
    pub sample_period: Option<String>,
}

/// A human-authored, checkable claim about one of this entry's own `sources` -- item 2 of
/// `product-mandate/LIBRARY_ENTRY_ROBUSTNESS_PLAN.md` (section 2.2), the plan's own recommended shape and its
/// RECOMMENDED option (b) for who authors it: "Human-authored at entry-curation time... the same human who writes
/// `rule_text` and merges the entry (the existing trust root T4 already relies on for every other library fact)".
/// Deliberately NOT LLM-extracted (WP8.3's `ClaimExtractor`/`extract.rs` is not used here, and never will be by this
/// field: an extraction step that could itself hallucinate a quote would undermine the point of verifying it).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibrarySourceClaim {
    /// Index into this entry's own `sources` -- which cited source this claim is about.
    pub source_index: usize,
    pub claim: LibraryCitationClaim,
}

/// One curated strategy. Parameters are fixed at the published values: the
/// agent chooses among entries, it never tunes one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibraryEntry {
    pub id: String,
    /// Entry version; bump on any change to the rule, parameters or ranges.
    pub version: u32,
    pub name: String,
    /// Source citations as plain strings.
    pub sources: Vec<String>,
    /// Item 2 (`product-mandate/LIBRARY_ENTRY_ROBUSTNESS_PLAN.md` section 2.2): human-authored, structured, checkable
    /// claims about what specific entries of `sources` (by index) say. `#[serde(default)]` so every existing entry
    /// (including `etf_trend_faber` before this field existed) parses unchanged, and `skip_serializing_if` so an
    /// entry that never sets it re-serialises byte-identically -- `entry_hash` of such an entry is therefore
    /// unaffected by this field's addition (mirrors `operating_procedure`'s own precedent exactly, see that field's
    /// doc comment below). Disclosure-only, like `operating_procedure`'s binding into `rule_hash`, but this field
    /// participates in NEITHER `rule_hash` nor `claim_hash` nor `entry_hash`'s label-relevant meaning: it is read
    /// only by `library_citation::check::library_citation_check`, never by `replication::evidence`/`admission`/
    /// `binding`, and never gates anything (see `program/tests/library_citation.rs`'s own source-scan test).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub citation_claims: Vec<LibrarySourceClaim>,
    /// The exact, unambiguous rule.
    pub rule_text: String,
    /// Where the reference implementation lives and its pin.
    pub reference_implementation: String,
    /// Instruments in the mandate's naming (e.g. `SPY`, `BTC/USD`). ALL are
    /// required by the published rule.
    pub instruments: Vec<String>,
    pub requirements: Requirements,
    pub rebalance: Rebalance,
    /// COUNCIL Ruling 10 (`operating_procedure`, [`OperatingProcedure`]): present only on a version-2-or-later entry.
    /// `#[serde(default)]` so every existing (version-1) entry still parses with no field at all, and
    /// `skip_serializing_if` so its own re-serialisation stays byte-identical -- `entry_hash` of a version-1 entry is
    /// therefore unaffected by this field's addition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operating_procedure: Option<OperatingProcedure>,
    pub parameters: BTreeMap<String, serde_json::Value>,
    /// The first entry is the primary one.
    pub expected_ranges: Vec<ExpectedRange>,
    /// WP9.8 (IMPLEMENTATION_PLAN.md): the window the independent-replication
    /// backtest actually ran over, as structured data a check can read.
    pub backtest_window: BacktestWindow,
    /// WP9.9 (IMPLEMENTATION_PLAN.md): every deviation between what the
    /// platform will actually run and what the rule as published specifies
    /// (an approximated `cross_asset_rebalance`, a capped weight/leverage,
    /// anything `eligibility()` would flag as an advisory rather than a hard
    /// block). Explicitly empty (`[]`), never omitted, when there is truly
    /// nothing to disclose -- matches this file's existing "never a guess,
    /// never a silent default" convention (see `ExpectedRange`'s doc comment).
    pub known_deviations: Vec<String>,
    pub failure_regimes: Vec<String>,
    pub cost_profile: CostProfile,
    pub evidence: Evidence,
    /// `YYYY-MM-DD`: when the rule text was last checked against the reference.
    pub last_verified: String,
    /// What exactly was and was not verified on that date.
    pub verification_note: String,
    pub retirement_rule: String,
    /// Whether this entry is shown to the free, paper-only "Observer" tier -- a small, deliberately curated
    /// subset (the most-vetted, longest-track-record entries), never the full library every paying tier sees.
    /// `#[serde(default)]` so every existing entry parses unchanged (defaults to `false`), and
    /// `skip_serializing_if = "is_false"` so a non-flagship entry (the common case) re-serialises byte-identically
    /// to before this field existed -- `entry_hash`/`rule_hash` of such an entry are therefore unaffected, mirroring
    /// `operating_procedure`'s and `citation_claims`'s own precedent exactly (see their doc comments above). Only a
    /// human/curator decision may set this `true`; `library_curation::draft::draft_from_candidate` always sets it
    /// `false` -- an autonomous draft never earns flagship status on its own.
    #[serde(default, skip_serializing_if = "is_false")]
    pub flagship: bool,
}

/// `skip_serializing_if` helper for [`LibraryEntry::flagship`]: `true` (skip) exactly when the value is the
/// default `false`, so the common case re-serialises with no `flagship` key at all.
fn is_false(b: &bool) -> bool {
    !b
}

/// The independent-replication backtest's own window, as structured data --
/// added for WP9.8 (IMPLEMENTATION_PLAN.md). Before this, a window's dates and
/// whether it covered a real down-period lived only as prose inside an
/// `ExpectedRange.note`, which a function can't check. One window per entry:
/// in practice every existing entry's ranges came from the same single
/// independent-reimplementation run, so a per-range window would be spurious
/// precision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestWindow {
    /// `YYYY-MM-DD`.
    pub start: String,
    /// `YYYY-MM-DD`.
    pub end: String,
    /// The largest peak-to-trough decline of the underlying BUY-AND-HOLD
    /// benchmark (not the strategy's own drawdown) actually observed in this
    /// window, as a positive fraction (0.15 = 15%). Deliberately the
    /// benchmark's number, not the strategy's: a working trend/timing rule is
    /// SUPPOSED to have a smaller drawdown than buy-and-hold by ducking out of
    /// downturns, so using the strategy's own number here would perversely
    /// penalise the best-performing strategies for successfully avoiding
    /// losses (etf_trend_faber's own drawdown was only 8.97% in a window that
    /// included COVID and the 2022 bear market -- its point was avoiding that
    /// fall, not that the window lacked one; the benchmark's 26.68% is what
    /// shows the window had real stress to be tested against). `None` when
    /// genuinely not measured -- never a guess, same convention as
    /// `ExpectedRange`.
    pub max_observed_drawdown: Option<f64>,
}

/// SHA-256 (hex) of the canonical serialization, so a proposal can record which
/// exact entry version it used.
pub fn entry_hash(e: &LibraryEntry) -> String {
    let bytes = serde_json::to_vec(e).expect("LibraryEntry always serializes");
    hex::encode(Sha256::digest(bytes))
}

/// Sanity-check a curated entry; empty result = well-formed. This checks the
/// SHAPE of the record (curation slips), not the truth of its numbers.
///
/// Stage T4, slice T4a (`product-mandate/T4_FIXED_RULE_ENDPOINT_PLAN.md` section 2.4, "How `validate_entry`, the
/// loader and the intake gate reject a typed label"): `replicated_on_platform` is a DERIVED label (design section
/// 2.4/4.1-4.2) -- as of T4a there is no verified-replication pipeline to derive it from at all (that is T4b/T4d), so
/// no entry may declare it by hand, unconditionally. This is the safe, honest interim named in the BUILD instructions:
/// it makes hand-typing the label an explicit, loud validation failure starting now, rather than waiting for the full
/// pipeline to exist. `seed_library()` calls this on every embedded entry (also T4a), so a seed file that declares the
/// label can never ship.
pub fn validate_entry(e: &LibraryEntry) -> Vec<String> {
    let mut v = Vec::new();
    if e.evidence == Evidence::ReplicatedOnPlatform {
        v.push(
            "evidence: \"replicated_on_platform\" cannot be declared by hand; it is derived from a verified \
             replication run, and the replication pipeline (stage T4b/T4d) does not exist yet -- no entry may \
             declare this label until then"
                .to_string(),
        );
    }
    // AUTONOMOUS_LIBRARY_CURATION_PLAN.md section 2.2: mirrors the `ReplicatedOnPlatform` rejection exactly, for the
    // sibling label. This applies uniformly to embedded seed entries and drafted entries alike -- the draft path
    // never declares anything above `ReferenceOnly`, so this is defense-in-depth, not a live gap it closes.
    if e.evidence == Evidence::AgentVerified {
        v.push(
            "evidence: \"agent_verified\" cannot be declared by hand; it is derived from a certified agent \
             reviewer's signed review, and no entry may declare this label until a real replication run and review \
             have earned it"
                .to_string(),
        );
    }
    let id_ok = !e.id.is_empty() && e.id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !id_ok {
        v.push(format!("id {:?} must be lowercase letters, digits and underscores", e.id));
    }
    if e.version == 0 {
        v.push("version must be at least 1".to_string());
    }
    for (name, blank) in [
        ("name", e.name.trim().is_empty()),
        ("rule_text", e.rule_text.trim().is_empty()),
        ("reference_implementation", e.reference_implementation.trim().is_empty()),
        ("verification_note", e.verification_note.trim().is_empty()),
        ("retirement_rule", e.retirement_rule.trim().is_empty()),
        ("cost_profile.note", e.cost_profile.note.trim().is_empty()),
    ] {
        if blank {
            v.push(format!("{name} must not be blank"));
        }
    }
    if e.sources.is_empty() || e.sources.iter().any(|s| s.trim().is_empty()) {
        v.push("sources needs at least one non-blank citation".to_string());
    }
    if e.instruments.is_empty() || e.instruments.iter().any(|s| s.trim().is_empty()) {
        v.push("instruments needs at least one non-blank symbol".to_string());
    }
    let r = &e.requirements;
    if r.asset_classes.is_empty() || r.asset_classes.iter().any(|s| s.trim().is_empty()) {
        v.push("requirements.asset_classes needs at least one non-blank class".to_string());
    }
    if r.venues_supported.is_empty() || r.venues_supported.iter().any(|s| s.trim().is_empty()) {
        v.push("requirements.venues_supported needs at least one non-blank venue".to_string());
    }
    if r.max_abs_weight_per_instrument.is_some_and(|w| !w.is_finite() || w <= 0.0) {
        v.push("requirements.max_abs_weight_per_instrument must be a positive number when present".to_string());
    }
    if let Some(g) = &r.gross_exposure_reference {
        let ordered = g.min <= g.median && g.median <= g.max && g.min <= g.mean && g.mean <= g.max && g.median <= g.p90 && g.p90 <= g.max;
        if !ordered || !g.min.is_finite() || !g.max.is_finite() || g.min <= 0.0 || g.n_month_ends == 0 || g.source.trim().is_empty() || g.note.trim().is_empty() {
            v.push("requirements.gross_exposure_reference must be internally consistent (min <= median <= p90 <= max), positive, and carry a source and a note".to_string());
        }
    }
    if !r.min_leverage_gross.is_finite() || r.min_leverage_gross <= 0.0 {
        v.push("requirements.min_leverage_gross must be a positive number".to_string());
    }
    if !r.min_history_years.is_finite() || r.min_history_years <= 0.0 {
        v.push("requirements.min_history_years must be a positive number".to_string());
    }
    if e.expected_ranges.is_empty() {
        v.push("expected_ranges needs at least one entry (a bound-less one says 'no band' explicitly)".to_string());
    }
    for (i, x) in e.expected_ranges.iter().enumerate() {
        if x.metric.trim().is_empty() || x.source.trim().is_empty() || x.note.trim().is_empty() {
            v.push(format!("expected_ranges[{i}] needs a metric, a source and a note"));
        }
        if x.low.is_some_and(|l| !l.is_finite()) || x.high.is_some_and(|h| !h.is_finite()) {
            v.push(format!("expected_ranges[{i}] bounds must be finite when present"));
        }
        if let (Some(l), Some(h)) = (x.low, x.high) {
            if l > h {
                v.push(format!("expected_ranges[{i}] low {l} is above high {h}"));
            }
        }
    }
    if e.failure_regimes.is_empty() {
        v.push("failure_regimes needs at least one entry".to_string());
    }
    if NaiveDate::parse_from_str(&e.last_verified, "%Y-%m-%d").is_err() {
        v.push(format!("last_verified {:?} must be a YYYY-MM-DD date", e.last_verified));
    }
    match (
        NaiveDate::parse_from_str(&e.backtest_window.start, "%Y-%m-%d"),
        NaiveDate::parse_from_str(&e.backtest_window.end, "%Y-%m-%d"),
    ) {
        (Ok(start), Ok(end)) if start > end => {
            v.push(format!("backtest_window.start {:?} is after backtest_window.end {:?}", e.backtest_window.start, e.backtest_window.end));
        }
        (Ok(_), Ok(_)) => {}
        _ => v.push("backtest_window.start and .end must both be YYYY-MM-DD dates".to_string()),
    }
    if e.backtest_window.max_observed_drawdown.is_some_and(|d| !d.is_finite() || d < 0.0) {
        v.push("backtest_window.max_observed_drawdown must be a non-negative number when present".to_string());
    }
    if e.known_deviations.iter().any(|s| s.trim().is_empty()) {
        v.push("known_deviations must not contain a blank entry (omit it from the list instead)".to_string());
    }
    v
}

// ---------------------------------------------------------------------------
// WP9.7-9.9 intake gate (IMPLEMENTATION_PLAN.md): explicit, numeric checks
// that replace "a human judges if it's good enough" with policy an agent can
// run mechanically. Pure functions, same as `validate_entry` above -- these
// check the QUALITY of what a candidate claims, not its shape. Run once at
// candidate intake (see the "First-content cut line" in the plan); WP9.5's
// staged autonomy determines how much of the result is trusted unattended.
// ---------------------------------------------------------------------------

/// The floor an `ExpectedRange`'s `low` must clear, as a `PerformanceBand`
/// (not merely an `IntegrityBound`), to auto-clear WP9.7. Calibrated to
/// `etf_trend_faber`'s own existing band (0.2), not an arbitrary number.
pub const MIN_PERFORMANCE_BAND_LOW: f64 = 0.2;

/// Minimum backtest window length, in years, for WP9.8.
pub const MIN_BACKTEST_WINDOW_YEARS: f64 = 5.0;

/// Minimum real peak-to-trough decline a backtest window must contain for
/// WP9.8 -- catches a candidate tested only in a rising market (the exact
/// failure mode `crypto_trend_100d`'s own 2016-2020-only window has).
pub const MIN_REQUIRED_DRAWDOWN_COVERAGE: f64 = 0.15;

#[derive(Debug, Clone, PartialEq)]
pub struct GateResult {
    pub passes: bool,
    pub reason: String,
}

impl GateResult {
    fn pass(reason: impl Into<String>) -> Self {
        Self { passes: true, reason: reason.into() }
    }
    fn fail(reason: impl Into<String>) -> Self {
        Self { passes: false, reason: reason.into() }
    }
}

/// WP9.7: does the primary expected range represent a real, positive
/// performance expectation, strong enough to auto-clear -- not merely an
/// integrity bound (a sanity check that a result isn't a look-ahead artifact,
/// which says nothing about whether the strategy is actually any good).
pub fn evidence_strength_check(entry: &LibraryEntry) -> GateResult {
    let Some(primary) = entry.expected_ranges.first() else {
        return GateResult::fail("no expected_ranges at all -- nothing to judge evidence strength from");
    };
    match primary.role {
        RangeRole::IntegrityBound => GateResult::fail(format!(
            "primary range ({}) is only an integrity bound, not a performance expectation -- \
             does not auto-clear without an explicit, logged owner override",
            primary.metric
        )),
        RangeRole::PerformanceBand => match primary.low {
            Some(low) if low >= MIN_PERFORMANCE_BAND_LOW => {
                GateResult::pass(format!("performance band low={low} >= floor {MIN_PERFORMANCE_BAND_LOW}"))
            }
            Some(low) => GateResult::fail(format!("performance band low={low} is below the {MIN_PERFORMANCE_BAND_LOW} floor")),
            None => GateResult::fail("performance band has no low bound to check at all"),
        },
    }
}

/// WP9.8: is the backtest window long enough, AND does it actually contain a
/// real down-period for the asset class -- not just a long span that happens
/// to avoid every drawdown. Length alone is not sufficient (see this
/// function's second check); a long window that is all one regime still fails.
pub fn representativeness_check(entry: &LibraryEntry) -> GateResult {
    let w = &entry.backtest_window;
    let (start, end) = match (
        NaiveDate::parse_from_str(&w.start, "%Y-%m-%d"),
        NaiveDate::parse_from_str(&w.end, "%Y-%m-%d"),
    ) {
        (Ok(s), Ok(e)) => (s, e),
        _ => return GateResult::fail("backtest_window.start/end are not both valid YYYY-MM-DD dates"),
    };
    let years = (end - start).num_days() as f64 / 365.25;
    if years < MIN_BACKTEST_WINDOW_YEARS {
        return GateResult::fail(format!(
            "backtest window is {years:.1} years, below the {MIN_BACKTEST_WINDOW_YEARS}-year minimum"
        ));
    }
    let dd_pct = |x: f64| x * 100.0;
    match w.max_observed_drawdown {
        Some(dd) if dd >= MIN_REQUIRED_DRAWDOWN_COVERAGE => GateResult::pass(format!(
            "{years:.1}-year window, includes a {:.1}% drawdown >= the {:.0}% minimum",
            dd_pct(dd),
            dd_pct(MIN_REQUIRED_DRAWDOWN_COVERAGE)
        )),
        Some(dd) => GateResult::fail(format!(
            "{years:.1}-year window is long enough, but its largest observed drawdown ({:.1}%) is below the \
             {:.0}% minimum -- likely tested only in one favourable regime",
            dd_pct(dd),
            dd_pct(MIN_REQUIRED_DRAWDOWN_COVERAGE)
        )),
        None => GateResult::fail("backtest_window.max_observed_drawdown is not recorded -- cannot confirm regime coverage"),
    }
}

/// WP9.9: any approximation between what the platform will actually run and
/// what the rule as published specifies must be disclosed structurally, not
/// left to a reader to find in free text. This checks presence, not writing
/// quality -- `eligibility()`'s advisories are the mechanical signal that a
/// deviation exists; the human sign-off step (not this function) judges
/// whether the disclosure text itself is honest and clear.
pub fn disclosure_check(entry: &LibraryEntry, platform: &PlatformCapabilities) -> GateResult {
    let advisories_exist = {
        // A neutral, always-permitted mandate, matching WP9.3's own intake-time
        // use of eligibility() -- this checks the RULE's fit against the
        // platform in general, not against any one user's mandate. The
        // allow-list is built from the entry's OWN instruments: an empty
        // instrument_allow means "nothing is allowed" (fail-closed, see
        // eligibility()'s own InstrumentsNotAllowed check), not "everything."
        let permissive = mandate_allowing_everything(&entry.instruments);
        let elig = eligibility(entry, &permissive, platform);
        elig.reasons.iter().any(|r| r.origin == Origin::Advisory)
    };
    match (advisories_exist, entry.known_deviations.is_empty()) {
        (true, true) => GateResult::fail(
            "eligibility() found at least one advisory-level approximation, but known_deviations is empty -- \
             the deviation must be disclosed structurally, not left implicit",
        ),
        (true, false) => GateResult::pass(format!("{} known_deviations entries disclosed", entry.known_deviations.len())),
        (false, _) => GateResult::pass("no advisory-level approximation found; nothing required to disclose"),
    }
}

/// A mandate permitting everything a rule could need, purely so
/// `disclosure_check` can ask "does the PLATFORM approximate this rule,"
/// independent of any one user's actual limits. Values are deliberately
/// maximal/unconstrained, not a real account -- do not reuse this as a real
/// mandate anywhere outside this intake-time check.
fn mandate_allowing_everything(instruments: &[String]) -> MandateBody {
    serde_json::from_value(serde_json::json!({
        "basis": {
            "capital_source": "own",
            "jurisdiction": "US-GA",
            "acknowledgements": []
        },
        "capital": { "allocated": { "amount": "1000000.00", "ccy": "USD" }, "min_cash_reserve": 0.0 },
        "universe": {
            "venues": ["alpaca", "kraken", "oanda"],
            "asset_classes": ["us_etf", "crypto_spot", "fx"],
            "instrument_allow": instruments,
            "instrument_deny": [],
            "shorting": true, "derivatives": true, "leverage_max_gross": 100.0
        },
        "exposure": {
            "max_position": 100.0, "max_asset_class": {},
            "max_gross": 100.0, "max_net": 100.0,
            "max_order_notional": { "amount": "1000000.00", "ccy": "USD" },
            "max_orders_per_day": 100000, "max_turnover_per_day": 100.0
        },
        "loss": {
            "daily_loss_limit": 1.0,
            "drawdown_ladder": [{ "at": 0.99, "action": "halt_flatten" }],
            "resume_after_halt": "human_only"
        },
        "autonomy": {
            "level": "L3",
            "may": ["place_orders", "cancel_orders", "resize_within_limits", "rebalance", "halt", "flatten"],
            "needs_confirmation": [],
            "never": ["change_limits", "resume_after_halt", "change_credentials", "withdraw"]
        },
        "reporting": { "digest": "daily", "alert_channels": ["email"] }
    }))
    .expect("hand-built permissive mandate fixture parses")
}

// ---------------------------------------------------------------------------
// WP9.2 automated replication checks (IMPLEMENTATION_PLAN.md): the checks
// that were already specced before this session's WP9.7-9.9 additions, but
// never built. Pure functions, same discipline as everything else in this
// file -- these take already-computed signals/results as input; producing
// those (running a candidate's reference implementation against real data)
// is a separate, not-yet-built concern (see IMPLEMENTATION_PLAN.md's note on
// where the real backtesting infra needs to be invoked).
// ---------------------------------------------------------------------------

pub const SIGNAL_AGREEMENT_FLOOR: f64 = 0.98;

/// Absolute tolerance comparing two implementations' per-bar signals. Two
/// independent implementations of the SAME unambiguous rule text should
/// compute near-identical numbers -- this only absorbs float rounding, not
/// real implementation disagreement.
const SIGNAL_AGREEMENT_TOLERANCE: f64 = 1e-6;

/// WP9.2: what fraction of bars do two independent implementations of the
/// same rule agree on. `None` at a bar means "flat" (no position); `Some(w)`
/// is a signed weight. Both series must be the same length and bar-aligned
/// by the caller -- this function has no notion of time itself.
pub fn signal_agreement(a: &[Option<f64>], b: &[Option<f64>]) -> GateResult {
    if a.len() != b.len() {
        return GateResult::fail(format!("signal series have different lengths ({} vs {})", a.len(), b.len()));
    }
    if a.is_empty() {
        return GateResult::fail("signal series are empty -- nothing to compare");
    }
    let agree = a
        .iter()
        .zip(b.iter())
        .filter(|(x, y)| match (x, y) {
            (None, None) => true,
            (Some(p), Some(q)) => (p - q).abs() <= SIGNAL_AGREEMENT_TOLERANCE,
            _ => false,
        })
        .count();
    let fraction = agree as f64 / a.len() as f64;
    let pct = |f: f64| f * 100.0;
    if fraction >= SIGNAL_AGREEMENT_FLOOR {
        GateResult::pass(format!(
            "{agree}/{} bars agree ({:.1}%) >= the {:.0}% floor",
            a.len(),
            pct(fraction),
            pct(SIGNAL_AGREEMENT_FLOOR)
        ))
    } else {
        GateResult::fail(format!(
            "only {agree}/{} bars agree ({:.1}%), below the {:.0}% floor",
            a.len(),
            pct(fraction),
            pct(SIGNAL_AGREEMENT_FLOOR)
        ))
    }
}

/// WP9.2: a signal function must not change its decision at bar `t` when
/// bars after `t` are removed -- the exact defect that failed 7 of 24 trials
/// in the raw-engine baseline (CERTIFICATION.md). Takes the signal FUNCTION
/// itself, not just its output over one fixed series: only re-running it
/// against a genuinely shorter series can catch an implementation that reads
/// `all_prices[t+1]`, a centered window, or `all_prices.len()` to sneak in
/// future information a plain output-comparison would never expose.
pub fn causality_by_truncation_check<F>(prices: &[f64], signal_at: F) -> GateResult
where
    F: Fn(&[f64], usize) -> Option<f64>,
{
    if prices.len() < 2 {
        return GateResult::fail("need at least 2 bars to check causality by truncation");
    }
    let violations: Vec<usize> = (0..prices.len())
        .filter(|&t| {
            let full = signal_at(prices, t);
            let truncated = signal_at(&prices[..=t], t);
            match (full, truncated) {
                (None, None) => false,
                (Some(a), Some(b)) => (a - b).abs() > SIGNAL_AGREEMENT_TOLERANCE,
                _ => true,
            }
        })
        .collect();
    if violations.is_empty() {
        GateResult::pass(format!("signal at every one of {} bars is unchanged when later bars are removed", prices.len()))
    } else {
        GateResult::fail(format!(
            "signal changed when later bars were removed at {} of {} bars (first: bar {}) -- look-ahead",
            violations.len(),
            prices.len(),
            violations[0]
        ))
    }
}

/// WP9.2: sizing must be set explicitly, never silently defaulted by a
/// reference implementation. Checks `parameters` for at least one
/// sizing-shaped key (naming convention: contains "weight", "sizing",
/// "target" or "leverage") whose value is a concrete number, not absent or
/// null.
pub fn explicit_sizing_check(entry: &LibraryEntry) -> GateResult {
    let sizing_keys: Vec<&String> = entry
        .parameters
        .keys()
        .filter(|k| {
            let k = k.as_str();
            k.contains("weight") || k.contains("sizing") || k.contains("target") || k.contains("leverage")
        })
        .collect();
    if sizing_keys.is_empty() {
        return GateResult::fail("parameters has no sizing-shaped key (weight/sizing/target/leverage) at all");
    }
    // At least one matching key must be a concrete number -- NOT every one:
    // a descriptive key can legitimately share the naming pattern without
    // being a number itself (fx_tsmom_12m's own "weighting":
    // "inverse_volatility (direction / sigma)" describes the sizing METHOD
    // in prose, while "sleeve_vol_target"/"max_abs_weight" carry the actual
    // numbers -- requiring every match to be numeric penalised the entry for
    // documenting itself clearly).
    let numeric: Vec<String> =
        sizing_keys.iter().filter(|k| entry.parameters[k.as_str()].is_number()).map(|k| k.to_string()).collect();
    if numeric.is_empty() {
        return GateResult::fail(format!(
            "sizing-shaped key(s) {sizing_keys:?} exist but none is a concrete number"
        ));
    }
    GateResult::pass(format!("{} numeric sizing parameter(s) explicitly set: {:?}", numeric.len(), numeric))
}

/// WP9.2: both gross and net results must be reported -- net-only hides how
/// much of the edge is cost; gross-only overstates what's actually
/// achievable. Checked by metric naming (a `_gross`/`_net` suffix).
pub fn gross_and_net_reported_check(entry: &LibraryEntry) -> GateResult {
    let has_gross = entry.expected_ranges.iter().any(|r| r.metric.ends_with("_gross"));
    let has_net = entry.expected_ranges.iter().any(|r| r.metric.ends_with("_net"));
    match (has_gross, has_net) {
        (true, true) => GateResult::pass("both a _gross and a _net metric are reported"),
        (true, false) => GateResult::fail("only a gross metric is reported -- costs are not shown"),
        (false, true) => {
            GateResult::fail("only a net metric is reported -- the pre-cost edge is not shown, can't tell how much is cost")
        }
        (false, false) => GateResult::fail("neither metric is explicitly labeled _gross or _net -- ambiguous which this is"),
    }
}

/// WP9.2: does a freshly-measured result fall inside the entry's own stated
/// range for that metric. `None` when the entry has no range for this metric
/// at all -- distinct from failing, since there is nothing to check against.
pub fn falls_inside_expected_range(entry: &LibraryEntry, metric: &str, measured_value: f64) -> Option<GateResult> {
    let range = entry.expected_ranges.iter().find(|r| r.metric == metric)?;
    if !range.is_informative() {
        return Some(GateResult::pass(format!("{metric} range has no bounds set -- nothing to fail against")));
    }
    Some(if range.contains(measured_value) {
        GateResult::pass(format!("{metric}={measured_value} is inside the stated range"))
    } else {
        GateResult::fail(format!(
            "{metric}={measured_value} is OUTSIDE the stated range [{:?}, {:?}] -- tagged outside_expected_range, \
             blocked pending an explicit owner override",
            range.low, range.high
        ))
    })
}

// ---------------------------------------------------------------------------
// Intake gate: ties every WP9.2/9.7-9.9 check into one verdict. This is the
// review packet described in the "how does the human sign-off actually
// work" discussion -- every field here is exactly what a reviewer needs to
// see, structured, not a transcript they have to comb through.
// ---------------------------------------------------------------------------

/// Replication evidence a caller has already measured (running a candidate's
/// two independent implementations against real data is a separate, not-yet-
/// built concern -- this struct is the boundary: everything on the other
/// side of it is "given these measurements," not "go measure them").
///
/// UPDATE 2026-09-23: for `etf_trend_faber` and `crypto_trend_100d`
/// specifically, "go measure them" is no longer entirely separate --
/// `SignalEngine/crates/reference-rules` already has a real Rust
/// implementation of both rules (`decide_etf_trend`, `decide_crypto_trend`)
/// and a golden test (`tests/golden.rs`) that replays them against real
/// historical prices (`tests/data/ladder_candles.csv`, SHA-pinned) and
/// compares to the original stage1-record shadow signals -- the actual
/// independent second implementation these entries were built from. Verified
/// running for real today, in this session, not assumed: `cargo test --test
/// golden -- --nocapture`, 6/6 passed --
/// `S1 ETF trend (explicit mode): 555/555 signals agree over 111 month-ends`,
/// `S3 crypto trend (reference semantics): 3654/3654 agree over 1827 days`.
/// That crate lives in a different repo with no existing dependency edge to
/// this one, so this module doesn't import it -- a real cross-repo
/// dependency is a bigger decision than one function needs. Test fixtures
/// below transcribe that REAL, already-measured result (100% agreement, not
/// assumed) rather than a synthetic stand-in, with this comment as the
/// paper trail for where the number actually came from.
pub struct ReplicationEvidence {
    pub signal_a: Vec<Option<f64>>,
    pub signal_b: Vec<Option<f64>>,
    /// The metric name and measured value to check against `expected_ranges`
    /// -- typically the entry's own primary metric, freshly measured.
    pub measured_metric: String,
    pub measured_value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntakeVerdict {
    /// The evidence label this candidate has EARNED, purely from the checks
    /// -- what a human still has to decide (per the "why not let the agent
    /// decide" discussion) is whether to actually apply it; this is not a
    /// bypass of that step.
    pub earned_evidence: Evidence,
    pub checks: Vec<(&'static str, GateResult)>,
    /// T4b, design section 2.4 item 5: informational provenance only, passed straight through from `intake_gate`'s
    /// own new parameter. Never affects `earned_evidence` (see [`VerifiedReplicationProvenance`]'s doc comment).
    pub verified_replication: Option<VerifiedReplicationProvenance>,
}

impl IntakeVerdict {
    /// Every check that failed, for a reviewer who only wants the problems.
    pub fn failures(&self) -> impl Iterator<Item = (&'static str, &GateResult)> {
        self.checks.iter().filter(|(_, r)| !r.passes).map(|(name, r)| (*name, r))
    }
}

/// Run every WP9.2/9.7-9.9 check and produce one verdict. Does NOT write
/// anything or move an entry's real `evidence` field -- that write, and the
/// one remaining human sign-off before `ReplicatedOnPlatform`, both happen
/// outside this pure function (see WP9.5's staged autonomy).
pub fn intake_gate(
    entry: &LibraryEntry,
    platform: &PlatformCapabilities,
    replication: &ReplicationEvidence,
    verified_replication: Option<VerifiedReplicationProvenance>,
) -> IntakeVerdict {
    let mut checks: Vec<(&'static str, GateResult)> = Vec::new();

    // WP9.3: does the platform even support what the rule needs -- for
    // INTAKE purposes, "can it run at all" means "can it run in PAPER,"
    // not "can it go live." `blocked_by_platform()` is deliberately
    // broader (it also fires on a live-only block, e.g. a venue like
    // Alpaca not being live-ready yet -- correct for a per-user eligibility
    // check, wrong here: a paper-only rule should still be able to earn
    // reference_only, since paper trading is how a strategy gets proven out
    // before going live in the first place). `eligible_paper` is the field
    // purpose-built for exactly this distinction.
    let permissive = mandate_allowing_everything(&entry.instruments);
    let elig = eligibility(entry, &permissive, platform);
    let platform_ok = elig.eligible_paper;
    checks.push((
        "9.3_platform_capability",
        if platform_ok {
            GateResult::pass("platform supports everything this rule needs in paper mode (or only advisory-level approximations)")
        } else {
            GateResult::fail(format!(
                "platform cannot run this rule even in paper mode: {}",
                elig.reasons.iter().filter(|r| r.origin == Origin::Platform).map(|r| r.message.clone()).collect::<Vec<_>>().join("; ")
            ))
        },
    ));

    // WP9.2: the four mechanical replication checks.
    checks.push(("9.2_signal_agreement", signal_agreement(&replication.signal_a, &replication.signal_b)));
    checks.push(("9.2_explicit_sizing", explicit_sizing_check(entry)));
    checks.push(("9.2_gross_and_net_reported", gross_and_net_reported_check(entry)));
    checks.push((
        "9.2_falls_inside_expected_range",
        falls_inside_expected_range(entry, &replication.measured_metric, replication.measured_value)
            .unwrap_or_else(|| GateResult::fail(format!("entry has no expected_ranges entry for metric {:?} at all", replication.measured_metric))),
    ));

    // WP9.7-9.9.
    checks.push(("9.7_evidence_strength", evidence_strength_check(entry)));
    checks.push(("9.8_representativeness", representativeness_check(entry)));
    checks.push(("9.9_disclosure", disclosure_check(entry, platform)));

    let earned_evidence = if !platform_ok {
        Evidence::PlatformCapabilityBlocked
    } else if checks.iter().filter(|(name, _)| *name != "9.3_platform_capability").all(|(_, r)| r.passes) {
        // All of WP9.2/9.7-9.9 clean -- earns reference_only. Note this
        // function never returns ReplicatedOnPlatform: per WP9.5, that label
        // is never auto-cleared, only reference_only is (partial autonomy).
        Evidence::ReferenceOnly
    } else {
        Evidence::Unverified
    };

    IntakeVerdict { earned_evidence, checks, verified_replication }
}

// ---------------------------------------------------------------------------
// WP8.5/9.5 calibration harness (IMPLEMENTATION_PLAN.md): a checker earns no
// autonomy until its OWN accuracy is measured against cases whose correct
// answer is already known -- this is that measurement, not the trust
// decision itself (that's still WP9.5's staged autonomy unlock, elsewhere).
// ---------------------------------------------------------------------------

/// One case in a known-answer set: a candidate whose CORRECT `intake_gate`
/// outcome is already known, either by construction (a deliberately
/// corrupted copy of a real entry) or because a human already checked it by
/// hand (a real entry). The known-answer SET itself (which real/synthetic
/// cases to use) is a caller concern, not this module's -- this only
/// provides the case shape and the measurement.
pub struct CalibrationCase {
    pub name: String,
    pub entry: LibraryEntry,
    pub replication: ReplicationEvidence,
    pub expected_evidence: Evidence,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CalibrationReport {
    pub total: usize,
    pub correct: usize,
    /// The gate was MORE generous than the known-correct answer -- a real
    /// safety concern (something that should have stayed back earned more
    /// trust than it should have).
    pub false_accepts: Vec<String>,
    /// The gate was MORE strict than the known-correct answer -- a cost
    /// concern, not a safety one (a genuinely good candidate held back).
    pub false_rejects: Vec<String>,
}

impl CalibrationReport {
    pub fn pass_rate(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        self.correct as f64 / self.total as f64
    }

    /// Wilson 95% confidence interval on the pass rate, same style as
    /// `CERTIFICATION.md`'s existing ladder for reporting a checker's own
    /// measured accuracy (not a promise the checker is trustworthy at that
    /// rate -- WP9.5's autonomy unlock is a separate decision on top of this
    /// number, not automatic once it looks good).
    pub fn confidence_interval(&self) -> (f64, f64) {
        wilson_95(self.correct, self.total)
    }
}

/// Wilson 95% interval for a binomial proportion. Same formula as
/// `meta_portfolio_service::placebo_suite`'s own -- that module isn't
/// crate-visible from here, so re-derived rather than imported; both are
/// small, standard, and independently testable.
fn wilson_95(k: usize, n: usize) -> (f64, f64) {
    if n == 0 {
        return (0.0, 1.0);
    }
    let (k, n, z) = (k as f64, n as f64, 1.96_f64);
    let p = k / n;
    let d = 1.0 + z * z / n;
    let c = p + z * z / (2.0 * n);
    let h = z * ((p * (1.0 - p) + z * z / (4.0 * n)) / n).sqrt();
    (((c - h) / d).max(0.0), ((c + h) / d).min(1.0))
}

/// How "generous" an `Evidence` label is, purely for calibration's
/// over-vs-under classification. `PlatformCapabilityBlocked` is not on this
/// ladder -- it's a different DIMENSION (can the platform even run it),
/// not more or less evidence -- so any mismatch involving it is classified
/// separately, in `run_calibration`, not ranked here.
fn evidence_rank(e: Evidence) -> u8 {
    match e {
        Evidence::Unverified => 0,
        Evidence::ReferenceOnly => 1,
        // `ReplicatedOnPlatform` and `AgentVerified` share a rank here on purpose: this ladder measures HOW MUCH
        // evidence a label represents, and the two are not more/less evidence than each other -- only WHO verified
        // differs (L1, AUTONOMOUS_LIBRARY_CURATION_PLAN.md section 2.2), which this calibration ladder does not
        // (and must not) collapse into "more generous." `intake_gate` itself never produces either as `got` (its
        // own doc comment), so this arm is exercised only via a calibration case's aspirational `want`.
        Evidence::ReplicatedOnPlatform | Evidence::AgentVerified => 2,
        Evidence::PlatformCapabilityBlocked => u8::MAX, // unreachable: handled before this is called
    }
}

/// Run `intake_gate` against a known-answer set and measure its accuracy.
pub fn run_calibration(cases: &[CalibrationCase], platform: &PlatformCapabilities) -> CalibrationReport {
    let mut correct = 0;
    let mut false_accepts = Vec::new();
    let mut false_rejects = Vec::new();
    for case in cases {
        let got = intake_gate(&case.entry, platform, &case.replication, None).earned_evidence;
        let want = case.expected_evidence;
        if got == want {
            correct += 1;
        } else if got == Evidence::PlatformCapabilityBlocked || want == Evidence::PlatformCapabilityBlocked {
            false_accepts.push(case.name.clone());
        } else if evidence_rank(got) > evidence_rank(want) {
            false_accepts.push(case.name.clone());
        } else {
            false_rejects.push(case.name.clone());
        }
    }
    CalibrationReport { total: cases.len(), correct, false_accepts, false_rejects }
}

// ---------------------------------------------------------------------------
// Seed entries (embedded JSON; facts only from the stage-1 record files)
// ---------------------------------------------------------------------------

/// The real, shipped library content. Empty from 2026-09-23 until stage T4, slice T4e
/// (`product-mandate/T4_FIXED_RULE_ENDPOINT_PLAN.md` section 3.3 "T4e: the first entry"), which adds ONE entry:
/// `etf_trend_faber` at version 2, re-issued in the compiled rule's own parameter vocabulary (so it passes
/// [`crate::replication::binding::check_declared_parameters`] with ZERO drift, unlike the version-1 fixture that
/// still lives under `test_fixtures/` purely as realistic test data -- see `tests::TEST_FIXTURE_JSON` below) and
/// carrying `operating_procedure` (COUNCIL Ruling 10). Its declared `evidence` stays `reference_only`: T4e ships the
/// entry and a path-A run GENERATOR (`program/src/bin/generate_replication_run.rs`) that can produce a genuine,
/// self-verifying run against the embedded answer key, but deliberately ships NO human review (see that binary's own
/// doc comment and `program/data/replication_runs/README.md`) -- so `GET /api/library` serves this entry at
/// `reference_only` until the repo owner supplies one. `fx_tsmom_12m` is not re-issued here (FX has no compiled rule
/// at all today -- `platform_capability_blocked`) and stays out of `SEED_JSON`.
///
/// `crypto_trend_100d` (added 2026-10-09): the v1 fixture's 2016-2020 window failed WP9.8 representativeness by a
/// hair (4.9993 years, just under the 5.0-year floor) -- the fix this doc comment used to point at ("extend the
/// window by a few weeks of real data") was already done, separately, on 2026-10-04 (Amendment 13,
/// `program/tests/fixtures/replication_ladder_crypto_evidence/`, PR #213): a real, pre-registered, PASS-verdict
/// data-quality-certified fixture over 2020-03-16..2026-10-03 (6.549 years). That fixture's own final step --
/// drafting this entry from it -- was never done until now; nothing in this crate referenced it before this entry.
/// No performance band is published (council rulings R22/R23, Amendment 13 section 7: a deliberate policy choice
/// given the window is still only one crypto cycle, not a failed check) -- see the entry's own `expected_ranges[0]`
/// and `verification_note` for the exact reasoning and the real certified numbers.
const SEED_JSON: [(&str, &str); 3] = [
    ("etf_trend_faber", include_str!("../data/strategy_library/etf_trend_faber.json")),
    ("crypto_trend_100d", include_str!("../data/strategy_library/crypto_trend_100d.json")),
    ("etf_cross_sectional_momentum", include_str!("../data/strategy_library/etf_cross_sectional_momentum.json")),
];

/// Parse the embedded seed entries. Fails (rather than panics) if an embedded
/// file is malformed, its id disagrees with its file name, or it fails
/// [`validate_entry`] (stage T4a: this was previously never checked here at
/// all -- `product-mandate/T4_FIXED_RULE_ENDPOINT_PLAN.md` section 2.4 names
/// this exact gap: "`seed_library()` ... does not call `validate_entry`" --
/// so a malformed or label-declaring embedded entry now fails loudly instead
/// of shipping silently.)
pub fn seed_library() -> Result<Vec<LibraryEntry>, String> {
    SEED_JSON.iter().map(|(name, json)| parse_seed_entry(name, json)).collect()
}

/// One `(name, json)` pair of [`seed_library`]'s loop body, factored out so a test can exercise it directly against a
/// hand-built entry (proving the loud rejection of a malformed or label-declaring entry) without `SEED_JSON` itself
/// needing to be non-empty.
fn parse_seed_entry(name: &str, json: &str) -> Result<LibraryEntry, String> {
    let e: LibraryEntry = serde_json::from_str(json).map_err(|err| format!("{name}: {err}"))?;
    if e.id != name {
        return Err(format!("{name}: id {:?} does not match its file name", e.id));
    }
    let violations = validate_entry(&e);
    if !violations.is_empty() {
        return Err(format!("{name}: fails validate_entry: {}", violations.join("; ")));
    }
    Ok(e)
}

// ---------------------------------------------------------------------------
// Eligibility
// ---------------------------------------------------------------------------

/// Who is responsible for a reason. Never mixed: a reason has exactly one
/// origin, fixed by its code ([`ReasonCode::origin`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    /// The user's mandate forbids it. Fix: change the mandate (a human act) or
    /// pick another strategy.
    Mandate,
    /// The platform cannot do it today. Fix: platform work; no mandate change
    /// helps.
    Platform,
    /// A non-blocking warning; the entry can still be used.
    Advisory,
}

/// Machine code for why an entry does not fit, or a warning about how it will run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonCode {
    // ---- Origin::Mandate
    /// The rule shorts but the mandate forbids shorting.
    ShortingNotPermitted,
    /// Required gross exceeds `universe.leverage_max_gross`.
    LeverageExceedsCap,
    /// Required gross exceeds `exposure.max_gross`.
    GrossExceedsExposureLimit,
    /// An asset class the rule trades is not in `universe.asset_classes`.
    AssetClassNotPermitted,
    /// An instrument the rule needs is not in `universe.instrument_allow`.
    InstrumentsNotAllowed,
    /// An instrument the rule needs is in `universe.instrument_deny`.
    InstrumentDenied,
    /// None of the venues the rule can trade on is in `universe.venues`.
    VenueNotPermitted,
    // ---- Origin::Platform
    /// The rule shorts and the platform has no shorting.
    ShortingUnsupported,
    /// The rule needs gross above 1x and the platform has no account leverage.
    LeverageUnsupported,
    /// The rule needs a joint portfolio volatility target the platform cannot run.
    PortfolioVolTargetUnsupported,
    /// The rule's per-instrument weights exceed what the platform can express.
    WeightAboveOneUnsupported,
    /// The rule NEEDS cross-asset rebalancing and the platform has none.
    CrossAssetRebalanceUnsupported,
    /// A permitted venue exists but live trading is not wired there yet:
    /// blocks live, not paper.
    NotLiveReady,
    // ---- Origin::Advisory
    /// The rule PREFERS cross-asset rebalancing; independent sub-accounts drift.
    CrossAssetRebalanceApproximated,
    /// The mandate's gross room is below the rule's observed peak gross, so the
    /// target will be clipped in the peak months.
    PeakGrossMayClip,
}

impl ReasonCode {
    /// The single origin of this code.
    pub fn origin(self) -> Origin {
        use ReasonCode::*;
        match self {
            ShortingNotPermitted | LeverageExceedsCap | GrossExceedsExposureLimit | AssetClassNotPermitted | InstrumentsNotAllowed
            | InstrumentDenied | VenueNotPermitted => Origin::Mandate,
            ShortingUnsupported | LeverageUnsupported | PortfolioVolTargetUnsupported | WeightAboveOneUnsupported
            | CrossAssetRebalanceUnsupported | NotLiveReady => Origin::Platform,
            CrossAssetRebalanceApproximated | PeakGrossMayClip => Origin::Advisory,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Blocks {
    /// The entry cannot be proposed or paper-traded.
    PaperAndLive,
    /// Paper is fine; going live is blocked until the cause is removed.
    LiveOnly,
    /// Advisory: nothing is blocked.
    Nothing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reason {
    pub code: ReasonCode,
    /// Always `code.origin()`.
    pub origin: Origin,
    pub blocks: Blocks,
    /// Plain language, safe to show to the user.
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Eligibility {
    /// May be proposed and paper-traded.
    pub eligible_paper: bool,
    /// May additionally go live (paper-eligible and nothing blocks live).
    pub eligible_live: bool,
    /// EVERY reason, blocking and advisory, ordered by code.
    pub reasons: Vec<Reason>,
}

impl Eligibility {
    /// Alias for `eligible_paper`: the entry can be selected at all.
    pub fn is_eligible(&self) -> bool {
        self.eligible_paper
    }

    fn blocked_by(&self, origin: Origin) -> bool {
        self.reasons.iter().any(|r| r.origin == origin && r.blocks != Blocks::Nothing)
    }

    /// True when at least one BLOCKING reason comes from the user's mandate.
    pub fn blocked_by_mandate(&self) -> bool {
        self.blocked_by(Origin::Mandate)
    }

    /// True when at least one BLOCKING reason is a platform limitation.
    pub fn blocked_by_platform(&self) -> bool {
        self.blocked_by(Origin::Platform)
    }

    /// Non-blocking warnings.
    pub fn advisories(&self) -> impl Iterator<Item = &Reason> {
        self.reasons.iter().filter(|r| r.origin == Origin::Advisory)
    }
}

/// Instrument names compare case-insensitively and ignore `/`, `-`, `_` and
/// spaces, so `BTC/USD`, `btc-usd` and `BTCUSD` are the same instrument.
///
/// `pub(crate)` (not `pub`) since `replication::binding`'s entry-versus-compiled-rule drift check (T4a) reuses this
/// EXACT normalization to compare a `LibraryEntry`'s `instruments` against a compiled rule's universe, rather than
/// inventing a second, possibly-diverging one.
pub(crate) fn norm_symbol(s: &str) -> String {
    s.chars().filter(|c| !matches!(c, '/' | '-' | '_' | ' ')).flat_map(char::to_uppercase).collect()
}

fn norm_venue(s: &str) -> String {
    s.trim().to_lowercase()
}

fn norm_class(s: &str) -> String {
    s.trim().to_lowercase()
}

/// True when `need` cannot be satisfied by `cap`. Non-finite values fail closed.
fn exceeds(need: f64, cap: f64) -> bool {
    !need.is_finite() || !cap.is_finite() || need > cap + LEVERAGE_EPS
}

fn quoted(items: &[String]) -> String {
    items.iter().map(|s| format!("\"{s}\"")).collect::<Vec<_>>().join(", ")
}

/// Check an entry against a mandate (what the USER forbids) and against the
/// platform's capabilities (what the PLATFORM cannot do today). Deterministic;
/// returns every reason, each tagged with its [`Origin`].
///
/// Mandate rules (the entry needs ALL of its instruments and asset classes,
/// because dropping one would change the published rule):
/// - shorting needed but `universe.shorting` is false
/// - required gross above `universe.leverage_max_gross` or `exposure.max_gross`
/// - an asset class not in `universe.asset_classes`
/// - an instrument not in `instrument_allow`, or one in `instrument_deny`
/// - no overlap between the entry's venues and `universe.venues`
///
/// Platform rules (blocking rules apply only to a `Needed` requirement, and block
/// paper AND live because the platform cannot run the rule faithfully):
/// - shorting needed but `!shorting_in_backtest`; gross above 1x but `!account_leverage`
/// - `needs_portfolio_vol_target` but `!portfolio_vol_target`
/// - a per-instrument weight above 1.0 but `!per_instrument_weight_above_1`
/// - `cross_asset_rebalance == Needed` but `!cross_asset_rebalance`
/// - permitted venues exist but none is in `venues_live_ready`: `NotLiveReady`,
///   which blocks LIVE only
///
/// Advisory (never blocks): a `Preferred` cross-asset rebalance the platform
/// lacks; mandate gross room below the rule's observed peak gross.
///
/// Capital-vs-minimum-position checks are NOT done: no per-entry minimum
/// position size is defined in the source material.
pub fn eligibility(entry: &LibraryEntry, mandate: &MandateBody, platform: &PlatformCapabilities) -> Eligibility {
    let u = &mandate.universe;
    let req = &entry.requirements;
    let mut reasons = Vec::new();
    let mut push = |code: ReasonCode, blocks: Blocks, message: String| {
        reasons.push(Reason { code, origin: code.origin(), blocks, message });
    };
    let name = &entry.name;

    // ---------------------------------------------------------------- mandate
    if req.needs_shorting && !u.shorting {
        push(
            ReasonCode::ShortingNotPermitted,
            Blocks::PaperAndLive,
            format!("{name} takes short positions, but the mandate does not allow shorting."),
        );
    }

    if exceeds(req.min_leverage_gross, u.leverage_max_gross) {
        push(
            ReasonCode::LeverageExceedsCap,
            Blocks::PaperAndLive,
            format!(
                "{name} needs about {:.1}x gross notional, but the mandate's leverage cap is {:.1}x.",
                req.min_leverage_gross, u.leverage_max_gross
            ),
        );
    }
    if exceeds(req.min_leverage_gross, mandate.exposure.max_gross) {
        push(
            ReasonCode::GrossExceedsExposureLimit,
            Blocks::PaperAndLive,
            format!(
                "{name} needs about {:.1}x gross exposure, but the mandate's gross exposure limit is {:.1}x.",
                req.min_leverage_gross, mandate.exposure.max_gross
            ),
        );
    }

    let classes: BTreeSet<String> = u.asset_classes.iter().map(|c| norm_class(c)).collect();
    let missing_classes: Vec<String> = req.asset_classes.iter().filter(|c| !classes.contains(&norm_class(c))).cloned().collect();
    if !missing_classes.is_empty() {
        push(
            ReasonCode::AssetClassNotPermitted,
            Blocks::PaperAndLive,
            format!("{name} trades {}, which the mandate's asset classes do not include.", quoted(&missing_classes)),
        );
    }

    let allow: BTreeSet<String> = u.instrument_allow.iter().map(|s| norm_symbol(s)).collect();
    let deny: BTreeSet<String> = u.instrument_deny.iter().map(|s| norm_symbol(s)).collect();
    let not_allowed: Vec<String> = entry.instruments.iter().filter(|s| !allow.contains(&norm_symbol(s))).cloned().collect();
    if !not_allowed.is_empty() {
        let which = if not_allowed.len() == entry.instruments.len() {
            "none of them are".to_string()
        } else {
            format!("{} of {} are missing", not_allowed.len(), entry.instruments.len())
        };
        push(
            ReasonCode::InstrumentsNotAllowed,
            Blocks::PaperAndLive,
            format!("{name} needs all of its instruments, but {which} in the mandate's allow-list; missing: {}.", quoted(&not_allowed)),
        );
    }
    let denied: Vec<String> = entry.instruments.iter().filter(|s| deny.contains(&norm_symbol(s))).cloned().collect();
    if !denied.is_empty() {
        push(
            ReasonCode::InstrumentDenied,
            Blocks::PaperAndLive,
            format!("{name} needs {}, which the mandate explicitly denies.", quoted(&denied)),
        );
    }

    let mandate_venues: BTreeSet<String> = u.venues.iter().map(|v| norm_venue(v)).collect();
    let permitted_venues: BTreeSet<String> =
        req.venues_supported.iter().map(|v| norm_venue(v)).filter(|v| mandate_venues.contains(v)).collect();
    if permitted_venues.is_empty() {
        push(
            ReasonCode::VenueNotPermitted,
            Blocks::PaperAndLive,
            format!("{name} trades on {}, but the mandate permits only {}.", quoted(&req.venues_supported), quoted(&u.venues)),
        );
    }

    // --------------------------------------------------------------- platform
    if req.needs_shorting && !platform.shorting_in_backtest {
        push(
            ReasonCode::ShortingUnsupported,
            Blocks::PaperAndLive,
            format!("{name} takes short positions, and the platform cannot short today."),
        );
    }
    if req.min_leverage_gross > 1.0 + LEVERAGE_EPS && !platform.account_leverage {
        push(
            ReasonCode::LeverageUnsupported,
            Blocks::PaperAndLive,
            format!("{name} needs about {:.1}x gross notional, and the platform has no account leverage today.", req.min_leverage_gross),
        );
    }
    if req.needs_portfolio_vol_target && !platform.portfolio_vol_target {
        push(
            ReasonCode::PortfolioVolTargetUnsupported,
            Blocks::PaperAndLive,
            format!(
                "{name} sizes the whole sleeve to a joint portfolio volatility target, which the platform cannot express today (each asset is sized alone)."
            ),
        );
    }
    if let Some(w) = req.max_abs_weight_per_instrument {
        if w > 1.0 + LEVERAGE_EPS && !platform.per_instrument_weight_above_1 {
            push(
                ReasonCode::WeightAboveOneUnsupported,
                Blocks::PaperAndLive,
                format!(
                    "{name} can give a single instrument a weight of up to {w:.1}x sleeve equity, but the platform caps a per-instrument position size at 1.0 today."
                ),
            );
        }
    }
    match req.cross_asset_rebalance {
        CrossAssetRebalance::Needed if !platform.cross_asset_rebalance => push(
            ReasonCode::CrossAssetRebalanceUnsupported,
            Blocks::PaperAndLive,
            format!("{name} needs positions rebalanced jointly across instruments, which the platform cannot do today."),
        ),
        CrossAssetRebalance::Preferred if !platform.cross_asset_rebalance => push(
            ReasonCode::CrossAssetRebalanceApproximated,
            Blocks::Nothing,
            format!(
                "{name} rebalances to fixed weights across instruments; the platform runs each instrument as an independent sub-account, so weights drift between rebalances. Results approximate the published rule."
            ),
        ),
        _ => {}
    }
    if !permitted_venues.is_empty() {
        let live_ready: BTreeSet<String> = platform.venues_live_ready.iter().map(|v| norm_venue(v)).collect();
        if !permitted_venues.iter().any(|v| live_ready.contains(v)) {
            push(
                ReasonCode::NotLiveReady,
                Blocks::LiveOnly,
                format!("Live trading is not available yet on the venue(s) {name} can use under this mandate; it can run on paper only."),
            );
        }
    }

    // --------------------------------------------------------------- advisory
    if let Some(g) = &req.gross_exposure_reference {
        let room = u.leverage_max_gross.min(mandate.exposure.max_gross);
        // Only meaningful once the entry's own gate is met; otherwise the mandate reasons already explain.
        if !exceeds(req.min_leverage_gross, room) && room.is_finite() && g.max > room + LEVERAGE_EPS {
            push(
                ReasonCode::PeakGrossMayClip,
                Blocks::Nothing,
                format!(
                    "The mandate allows up to {room:.1}x gross, but the reference run of {name} needed up to {:.1}x at its peak (typical {:.1}x); in peak months the target would be clipped.",
                    g.max, g.median
                ),
            );
        }
    }

    reasons.sort_by_key(|r| r.code);
    let eligible_paper = !reasons.iter().any(|r| r.blocks == Blocks::PaperAndLive);
    let eligible_live = eligible_paper && !reasons.iter().any(|r| r.blocks == Blocks::LiveOnly);
    Eligibility { eligible_paper, eligible_live, reasons }
}

// ---------------------------------------------------------------------------
// Sizing to the drawdown ladder
// ---------------------------------------------------------------------------

/// Why a ladder fit could not be computed. Sizing fails closed: no fit is ever
/// reported for a series or ladder it cannot honestly replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LadderFitError {
    EmptyLadder,
    /// The first rung's `at` is not a finite number in (0, 1].
    BadFirstRung,
    /// `headroom` is not a finite number in (0, 1].
    BadHeadroom,
    /// No return history to replay.
    EmptySeries,
    NonFiniteReturn { index: usize },
    /// A daily return below -100% cannot happen without leverage beyond ruin.
    ReturnBelowMinusOne { index: usize },
    /// Meeting the target would need a scale below [`MIN_FIT_SCALE`]: the
    /// series cannot be run under this ladder in any useful size.
    TargetUnreachable,
}

impl std::fmt::Display for LadderFitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyLadder => write!(f, "the mandate has no drawdown ladder to size against"),
            Self::BadFirstRung => write!(f, "the first ladder rung must be a fraction in (0, 1]"),
            Self::BadHeadroom => write!(f, "headroom must be a fraction in (0, 1]"),
            Self::EmptySeries => write!(f, "no return history to replay"),
            Self::NonFiniteReturn { index } => write!(f, "return at index {index} is not a finite number"),
            Self::ReturnBelowMinusOne { index } => write!(f, "return at index {index} is below -100%"),
            Self::TargetUnreachable => write!(f, "staying inside the first drawdown rung would need a scale below {MIN_FIT_SCALE}, i.e. essentially no position"),
        }
    }
}

impl std::error::Error for LadderFitError {}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LadderFit {
    /// Largest scale in (0, 1] meeting the target; 1.0 when already inside.
    pub scale: f64,
    /// Max drawdown (positive fraction) of the SCALED series.
    pub replay_max_drawdown: f64,
    /// `at` of the ladder's first rung.
    pub first_rung_at: f64,
    /// Indices of ladder rungs the scaled replay reaches (`drawdown >= at`,
    /// within a 1e-12 tolerance in the safe direction).
    pub rungs_hit: Vec<usize>,
    /// Max drawdown of the UNSCALED series, so the proposal can show the gap.
    pub unscaled_max_drawdown: f64,
    /// Rungs the unscaled replay would have reached.
    pub unscaled_rungs_hit: Vec<usize>,
    /// Number of daily returns replayed.
    pub observations: usize,
}

/// Maximum drawdown (positive fraction) of the compounded equity curve of
/// `scale * daily_returns`, starting from equity 1.0 (the start counts as a
/// high-water mark). Equity that reaches zero or below counts as a 100%
/// drawdown. Does not validate the inputs; see [`fit_scale_to_ladder`].
pub fn replay_max_drawdown(daily_returns: &[f64], scale: f64) -> f64 {
    let mut equity = 1.0_f64;
    let mut peak = 1.0_f64;
    let mut worst = 0.0_f64;
    for r in daily_returns {
        equity *= 1.0 + scale * r;
        if equity <= 0.0 {
            return 1.0;
        }
        peak = peak.max(equity);
        worst = worst.max(1.0 - equity / peak);
    }
    worst
}

fn rungs_reached(ladder: &[LadderRung], drawdown: f64) -> Vec<usize> {
    ladder.iter().enumerate().filter(|(_, r)| drawdown + RUNG_EPS >= r.at).map(|(i, _)| i).collect()
}

/// Find, by bisection, the largest scale `s` in (0, 1] such that the
/// compounded max drawdown of `s * daily_returns` is at most
/// `headroom * ladder[0].at`. Returns scale 1.0 when the series is already
/// inside. Never scales up.
///
/// Why bisection is sound: for a fixed path the max drawdown is non-decreasing
/// in `s` (each drawdown segment's log-growth is concave in `s` and zero at
/// `s = 0`), so the set of acceptable scales is an interval starting at 0.
///
/// This is a replay of one historical path at daily close-to-close
/// resolution; it says nothing about paths that did not happen.
pub fn fit_scale_to_ladder(daily_returns: &[f64], ladder: &[LadderRung], headroom: f64) -> Result<LadderFit, LadderFitError> {
    let first = ladder.first().ok_or(LadderFitError::EmptyLadder)?;
    if !first.at.is_finite() || first.at <= 0.0 || first.at > 1.0 {
        return Err(LadderFitError::BadFirstRung);
    }
    if !headroom.is_finite() || headroom <= 0.0 || headroom > 1.0 {
        return Err(LadderFitError::BadHeadroom);
    }
    if daily_returns.is_empty() {
        return Err(LadderFitError::EmptySeries);
    }
    for (index, r) in daily_returns.iter().enumerate() {
        if !r.is_finite() {
            return Err(LadderFitError::NonFiniteReturn { index });
        }
        if *r < -1.0 {
            return Err(LadderFitError::ReturnBelowMinusOne { index });
        }
    }

    let target = headroom * first.at;
    let unscaled = replay_max_drawdown(daily_returns, 1.0);
    let build = |scale: f64, drawdown: f64| LadderFit {
        scale,
        replay_max_drawdown: drawdown,
        first_rung_at: first.at,
        rungs_hit: rungs_reached(ladder, drawdown),
        unscaled_max_drawdown: unscaled,
        unscaled_rungs_hit: rungs_reached(ladder, unscaled),
        observations: daily_returns.len(),
    };
    if unscaled <= target {
        return Ok(build(1.0, unscaled));
    }

    // Invariant: drawdown(lo) <= target < drawdown(hi).
    let (mut lo, mut hi) = (0.0_f64, 1.0_f64);
    for _ in 0..200 {
        let mid = 0.5 * (lo + hi);
        if replay_max_drawdown(daily_returns, mid) <= target {
            lo = mid;
        } else {
            hi = mid;
        }
        if hi - lo <= 1e-12 * hi {
            break;
        }
    }
    if lo < MIN_FIT_SCALE {
        return Err(LadderFitError::TargetUnreachable);
    }
    Ok(build(lo, replay_max_drawdown(daily_returns, lo)))
}

// ---------------------------------------------------------------------------
// Inverse-volatility weights
// ---------------------------------------------------------------------------

/// Weights proportional to 1 / vol, summing to 1. `None` for an empty input or
/// any vol that is not a positive finite number (a silent fallback would hide
/// a data problem), or when the reciprocals overflow.
pub fn inverse_vol_weights(vols: &[f64]) -> Option<Vec<f64>> {
    if vols.is_empty() || vols.iter().any(|v| !v.is_finite() || *v <= 0.0) {
        return None;
    }
    let inv: Vec<f64> = vols.iter().map(|v| 1.0 / v).collect();
    let total: f64 = inv.iter().sum();
    if !total.is_finite() || total <= 0.0 {
        return None;
    }
    Some(inv.iter().map(|x| x / total).collect())
}

// ---------------------------------------------------------------------------
// Selection log (types and validation only; no selection logic)
// ---------------------------------------------------------------------------

/// What an agent's pre-declared selection produced. The count of
/// `candidates_considered` is the trial count for later statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionRecord {
    pub candidates_considered: Vec<String>,
    pub chosen: Vec<String>,
    /// (entry id, reason) for every considered entry that was not chosen.
    pub rejected: Vec<(String, String)>,
    /// The pre-declared selection rule, in words.
    pub rule: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionIssueCode {
    MissingRule,
    DuplicateCandidate,
    DuplicateChosen,
    ChosenNotConsidered,
    /// A chosen entry has no eligibility result, so nothing vouches for it.
    ChosenUnknown,
    ChosenIneligible,
    TooManyChosen,
    ChosenAndRejected,
    RejectedNotConsidered,
    /// A considered, unchosen entry has no (or a blank) rejection reason.
    MissingRejectionReason,
    /// An entry that IS eligible was left out of the considered list, which
    /// would hide a trial from the count.
    EligibleNotLogged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SelectionIssue {
    pub code: SelectionIssueCode,
    pub entry_id: Option<String>,
    pub message: String,
}

impl SelectionRecord {
    /// Check the record against the eligibility results of the library it was
    /// drawn from and the maximum number of sleeves. Returns ALL issues
    /// (empty = valid). A chosen entry must be eligible for paper; live
    /// eligibility is a later, separate gate.
    pub fn validate(&self, eligibility: &BTreeMap<String, Eligibility>, max_chosen: usize) -> Vec<SelectionIssue> {
        let mut out = Vec::new();
        let mut issue = |code: SelectionIssueCode, id: Option<&str>, message: String| {
            out.push(SelectionIssue { code, entry_id: id.map(str::to_string), message });
        };

        if self.rule.trim().is_empty() {
            issue(SelectionIssueCode::MissingRule, None, "the selection rule must be stated before choosing".to_string());
        }

        let mut considered: BTreeSet<&str> = BTreeSet::new();
        for id in &self.candidates_considered {
            if !considered.insert(id.as_str()) {
                issue(SelectionIssueCode::DuplicateCandidate, Some(id.as_str()), format!("{id} is listed twice among the candidates considered"));
            }
        }

        if self.chosen.len() > max_chosen {
            issue(
                SelectionIssueCode::TooManyChosen,
                None,
                format!("{} entries chosen, but at most {max_chosen} are allowed", self.chosen.len()),
            );
        }

        let mut chosen: BTreeSet<&str> = BTreeSet::new();
        for id in &self.chosen {
            if !chosen.insert(id.as_str()) {
                issue(SelectionIssueCode::DuplicateChosen, Some(id.as_str()), format!("{id} is chosen twice"));
                continue;
            }
            if !considered.contains(id.as_str()) {
                issue(SelectionIssueCode::ChosenNotConsidered, Some(id.as_str()), format!("{id} is chosen but was not logged as considered"));
            }
            match eligibility.get(id) {
                None => issue(SelectionIssueCode::ChosenUnknown, Some(id.as_str()), format!("{id} has no eligibility result")),
                Some(e) if !e.eligible_paper => {
                    let why = e.reasons.iter().filter(|r| r.blocks != Blocks::Nothing).map(|r| r.message.as_str()).collect::<Vec<_>>().join(" ");
                    issue(SelectionIssueCode::ChosenIneligible, Some(id.as_str()), format!("{id} is not eligible under the mandate: {why}"));
                }
                Some(_) => {}
            }
        }

        let mut rejected: BTreeMap<&str, &str> = BTreeMap::new();
        for (id, reason) in &self.rejected {
            if !considered.contains(id.as_str()) {
                issue(SelectionIssueCode::RejectedNotConsidered, Some(id.as_str()), format!("{id} is rejected but was not logged as considered"));
            }
            if chosen.contains(id.as_str()) {
                issue(SelectionIssueCode::ChosenAndRejected, Some(id.as_str()), format!("{id} is both chosen and rejected"));
            }
            if reason.trim().is_empty() {
                issue(SelectionIssueCode::MissingRejectionReason, Some(id.as_str()), format!("{id} is rejected without a reason"));
            }
            rejected.insert(id.as_str(), reason.as_str());
        }
        for id in &considered {
            if !chosen.contains(id) && !rejected.contains_key(id) {
                issue(
                    SelectionIssueCode::MissingRejectionReason,
                    Some(*id),
                    format!("{id} was considered but neither chosen nor rejected with a reason"),
                );
            }
        }

        for (id, e) in eligibility {
            if e.eligible_paper && !considered.contains(id.as_str()) {
                issue(
                    SelectionIssueCode::EligibleNotLogged,
                    Some(id.as_str()),
                    format!("{id} is eligible but was not logged as considered; every eligible candidate counts as a trial"),
                );
            }
        }

        out.sort_by(|a, b| (a.code, &a.entry_id).cmp(&(b.code, &b.entry_id)));
        out
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::mandate::{self, LadderAction}; // T4b: was `crate::mandate`; see the module-level comment above.
    use serde_json::json;

    // Copied from mandate.rs tests; ONLY change: instrument_allow lists all
    // instruments the seed entries trade (the original lists just four).
    fn mandate_json() -> serde_json::Value {
        json!({
            "basis": {
                "capital_source": "own",
                "jurisdiction": "US-GA",
                "acknowledgements": [
                    {"doc": "own_capital_attestation", "doc_version": "2026-10", "at": "2026-09-21T12:00:00Z", "by": "user_1"},
                    {"doc": "risk_disclosure", "doc_version": "2026-10", "at": "2026-09-21T12:00:00Z", "by": "user_1"}
                ]
            },
            "capital": {"allocated": {"amount": "5000.00", "ccy": "USD"}, "min_cash_reserve": 0.05},
            "universe": {
                "venues": ["alpaca", "kraken"], "asset_classes": ["us_etf", "crypto_spot"],
                "instrument_allow": ["SPY", "EFA", "IEF", "DBC", "VNQ", "BTC/USD", "ETH/USD"], "instrument_deny": [],
                "shorting": false, "derivatives": false, "leverage_max_gross": 1.0
            },
            "exposure": {
                "max_position": 0.25, "max_asset_class": {"crypto_spot": 0.6},
                "max_gross": 1.0, "max_net": 1.0,
                "max_order_notional": {"amount": "1500.00", "ccy": "USD"},
                "max_orders_per_day": 20, "max_turnover_per_day": 0.5
            },
            "loss": {
                "daily_loss_limit": 0.03,
                "drawdown_ladder": [
                    {"at": 0.10, "action": "shrink", "scale": 0.5},
                    {"at": 0.20, "action": "halt_flatten"}
                ],
                "resume_after_halt": "human_only"
            },
            "autonomy": {
                "level": "L3",
                "may": ["place_orders", "cancel_orders", "resize_within_limits", "rebalance", "halt", "flatten"],
                "needs_confirmation": ["add_strategy", "remove_strategy", "raise_target_risk"],
                "never": ["change_limits", "resume_after_halt", "change_credentials", "withdraw"]
            },
            "reporting": {"digest": "daily", "alert_channels": ["email"]}
        })
    }

    fn mandate_with(edit: impl FnOnce(&mut serde_json::Value)) -> MandateBody {
        let mut v = mandate_json();
        edit(&mut v);
        serde_json::from_value(v).expect("fixture parses")
    }

    fn base_mandate() -> MandateBody {
        mandate_with(|_| {})
    }

    /// Realistic fixture data for exercising `eligibility()`/`fit_scale_to_ladder()`
    /// against genuine strategy shapes (real shorting/leverage/asset-class
    /// requirements) -- NOT the shipped library, which is deliberately empty
    /// (see `SEED_JSON`'s doc comment above). Same 3 files as before this
    /// module's content was cleared, just no longer wired to `seed_library()`.
    const TEST_FIXTURE_JSON: [(&str, &str); 3] = [
        ("etf_trend_faber", include_str!("../data/strategy_library/test_fixtures/etf_trend_faber.json")),
        ("fx_tsmom_12m", include_str!("../data/strategy_library/test_fixtures/fx_tsmom_12m.json")),
        ("crypto_trend_100d", include_str!("../data/strategy_library/test_fixtures/crypto_trend_100d.json")),
    ];

    fn lib() -> Vec<LibraryEntry> {
        TEST_FIXTURE_JSON
            .iter()
            .map(|(name, json)| {
                let e: LibraryEntry = serde_json::from_str(json).expect("fixture parses");
                assert_eq!(&e.id, name, "fixture id matches its file name");
                e
            })
            .collect()
    }

    fn entry(id: &str) -> LibraryEntry {
        lib().into_iter().find(|e| e.id == id).unwrap_or_else(|| panic!("no seed entry {id}"))
    }

    /// Codes of the BLOCKING reasons only (advisories excluded), in code order.
    fn codes(e: &Eligibility) -> Vec<ReasonCode> {
        e.reasons.iter().filter(|r| r.blocks != Blocks::Nothing).map(|r| r.code).collect()
    }

    fn advisory_codes(e: &Eligibility) -> Vec<ReasonCode> {
        e.advisories().map(|r| r.code).collect()
    }

    fn caps() -> PlatformCapabilities {
        current_platform_capabilities()
    }

    fn elig(entry: &LibraryEntry, mandate: &MandateBody) -> Eligibility {
        eligibility(entry, mandate, &caps())
    }

    fn elig_with(entry: &LibraryEntry, mandate: &MandateBody, platform: &PlatformCapabilities) -> Eligibility {
        eligibility(entry, mandate, platform)
    }

    /// A mandate that permits everything the FX entry needs, so that only
    /// platform limits can stand in its way.
    fn fx_friendly_mandate() -> MandateBody {
        let m = mandate_with(|v| {
            v["universe"]["venues"] = json!(["alpaca", "kraken", "oanda"]);
            v["universe"]["asset_classes"] = json!(["us_etf", "crypto_spot", "fx"]);
            v["universe"]["instrument_allow"] = json!([
                "SPY", "EFA", "IEF", "DBC", "VNQ", "BTC/USD", "ETH/USD",
                "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF", "NZD/USD"
            ]);
            v["universe"]["shorting"] = json!(true);
            v["universe"]["leverage_max_gross"] = json!(6.0);
            v["exposure"]["max_gross"] = json!(6.0);
            v["exposure"]["max_net"] = json!(6.0);
        });
        assert_eq!(mandate::validate(&m), vec![], "test mandate must itself be valid");
        m
    }

    fn ladder() -> Vec<LadderRung> {
        base_mandate().loss.drawdown_ladder
    }

    // ------------------------------------------------------------------ fixture sanity

    #[test]
    fn mandate_fixture_is_valid() {
        assert_eq!(mandate::validate(&base_mandate()), vec![]);
    }

    // ------------------------------------------------------------------ seed library

    #[test]
    fn seed_library_has_the_three_entries_and_all_validate() {
        let l = lib();
        let ids: Vec<&str> = l.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["etf_trend_faber", "fx_tsmom_12m", "crypto_trend_100d"]);
        for e in &l {
            assert_eq!(validate_entry(e), Vec::<String>::new(), "{}", e.id);
        }
    }

    // ------------------------------------------------------------------ T4a: a hand-typed label is rejected

    /// Mutant target: "`validate_entry` stops rejecting the declared label".
    #[test]
    fn validate_entry_rejects_a_hand_typed_replicated_on_platform_label_even_when_otherwise_valid() {
        for e in lib() {
            assert_eq!(validate_entry(&e), Vec::<String>::new(), "{} starts valid", e.id);
            let mut declared = e.clone();
            declared.evidence = Evidence::ReplicatedOnPlatform;
            let violations = validate_entry(&declared);
            assert_eq!(violations.len(), 1, "{}: {violations:?}", e.id);
            assert!(violations[0].contains("replicated_on_platform"), "{}: {violations:?}", e.id);
        }
    }

    /// The rejection fires independently of every other violation: an otherwise-garbage entry that ALSO declares the
    /// label still names the label violation (it is not swallowed or short-circuited by earlier checks).
    #[test]
    fn validate_entry_rejects_the_label_alongside_other_violations() {
        let mut e = entry("etf_trend_faber");
        e.evidence = Evidence::ReplicatedOnPlatform;
        e.name = "   ".to_string(); // also blank -- a second, unrelated violation
        e.version = 0; // a third
        let violations = validate_entry(&e);
        assert!(violations.iter().any(|v| v.contains("replicated_on_platform")), "{violations:?}");
        assert!(violations.len() >= 3, "{violations:?}");
    }

    // ---------------------------------------------------- L1: a hand-typed agent_verified label is rejected too

    /// AUTONOMOUS_LIBRARY_CURATION_PLAN.md section 2.2, mirrors
    /// `validate_entry_rejects_a_hand_typed_replicated_on_platform_label_even_when_otherwise_valid` exactly for the
    /// sibling label. Mutant target: "`validate_entry` stops rejecting a declared `agent_verified` label".
    #[test]
    fn validate_entry_rejects_a_hand_typed_agent_verified_label_even_when_otherwise_valid() {
        for e in lib() {
            assert_eq!(validate_entry(&e), Vec::<String>::new(), "{} starts valid", e.id);
            let mut declared = e.clone();
            declared.evidence = Evidence::AgentVerified;
            let violations = validate_entry(&declared);
            assert_eq!(violations.len(), 1, "{}: {violations:?}", e.id);
            assert!(violations[0].contains("agent_verified"), "{}: {violations:?}", e.id);
        }
    }

    /// The two rejection clauses are independent: declaring BOTH `ReplicatedOnPlatform` and (impossible on a single
    /// field, but exercised via two separate entries) each label alone produces exactly its own violation string,
    /// never the other's -- proving the two clauses never get conflated with each other.
    #[test]
    fn validate_entry_agent_verified_and_replicated_on_platform_rejections_are_distinct_messages() {
        let mut replicated = entry("etf_trend_faber");
        replicated.evidence = Evidence::ReplicatedOnPlatform;
        let replicated_violations = validate_entry(&replicated);
        assert!(replicated_violations.iter().any(|v| v.contains("replicated_on_platform")));
        assert!(!replicated_violations.iter().any(|v| v.contains("agent_verified")));

        let mut agent = entry("etf_trend_faber");
        agent.evidence = Evidence::AgentVerified;
        let agent_violations = validate_entry(&agent);
        assert!(agent_violations.iter().any(|v| v.contains("agent_verified")));
        assert!(!agent_violations.iter().any(|v| v.contains("replicated_on_platform")));
    }

    /// Mutant target: "`seed_library` stops calling `validate_entry`". `SEED_JSON` itself is empty (nothing to load
    /// in production today), so this exercises `parse_seed_entry` -- `seed_library`'s own per-entry loop body --
    /// directly against a hand-built JSON string, exactly as `seed_library` would if `SEED_JSON` were non-empty.
    #[test]
    fn parse_seed_entry_rejects_a_declared_replicated_on_platform_label() {
        let mut v: serde_json::Value = serde_json::from_str(TEST_FIXTURE_JSON[0].1).expect("fixture parses");
        v["evidence"] = json!("replicated_on_platform");
        let json = serde_json::to_string(&v).expect("re-serializes");
        let err = parse_seed_entry("etf_trend_faber", &json).expect_err("must fail loudly, not load silently");
        assert!(err.contains("fails validate_entry"), "{err}");
        assert!(err.contains("replicated_on_platform"), "{err}");
    }

    /// The id-mismatch and JSON-parse checks `seed_library` already had keep working through the refactor into
    /// `parse_seed_entry`.
    #[test]
    fn parse_seed_entry_still_rejects_id_mismatch_and_bad_json() {
        assert!(parse_seed_entry("wrong_name", TEST_FIXTURE_JSON[0].1).is_err());
        assert!(parse_seed_entry("etf_trend_faber", "{ not json").is_err());
    }

    #[test]
    fn every_seed_is_reference_only_with_a_caveat_about_the_literature() {
        for e in lib() {
            assert_eq!(e.evidence, Evidence::ReferenceOnly, "{}", e.id);
            assert_eq!(e.version, 1);
            // All 3 got a real, dated citation check on 2026-09-23 (see
            // sources[0]); rule-text transcription itself is still dated
            // 2026-09-21 -- last_verified reflects the more recent of the two.
            assert_eq!(e.last_verified, "2026-09-23", "{}", e.id);
            assert!(e.verification_note.contains("NOT replicated"), "{}", e.id);
            // fx and crypto's citations remain genuinely unverified against
            // the original claim; etf's was actually checked and found
            // wrong (a real, checked "no," stronger than lingering
            // uncertainty) -- both are honest states, not the same one.
            let honestly_uncertain_or_checked =
                e.verification_note.contains("UNVERIFIED") || e.verification_note.contains("CHECKED");
            assert!(honestly_uncertain_or_checked, "{}: {}", e.id, e.verification_note);
        }
    }

    #[test]
    fn seed_facts_match_the_reference_rules() {
        let etf = entry("etf_trend_faber");
        assert_eq!(etf.instruments, ["SPY", "EFA", "IEF", "DBC", "VNQ"]);
        assert_eq!(etf.parameters["sma_month_ends"], json!(10));
        assert_eq!(etf.parameters["weight_per_etf"], json!(0.2));
        assert_eq!(etf.rebalance.kind, RebalanceKind::Monthly);
        assert!(!etf.requirements.needs_shorting);
        assert_eq!(etf.requirements.min_leverage_gross, 1.0);
        assert_eq!(etf.requirements.venues_supported, ["alpaca"]);
        assert!(!etf.requirements.needs_portfolio_vol_target);
        assert_eq!(etf.requirements.max_abs_weight_per_instrument, Some(0.2));
        assert_eq!(etf.requirements.cross_asset_rebalance, CrossAssetRebalance::Preferred);
        assert!(etf.requirements.gross_exposure_reference.is_none());

        let fx = entry("fx_tsmom_12m");
        assert_eq!(fx.instruments.len(), 7);
        assert_eq!(fx.parameters["lookback_month_ends"], json!(12));
        assert_eq!(fx.parameters["vol_window_days"], json!(60));
        assert_eq!(fx.parameters["sleeve_vol_target"], json!(0.10));
        assert_eq!(fx.parameters["max_abs_weight"], json!(3.0));
        assert!(fx.requirements.needs_shorting);
        assert_eq!(fx.requirements.venues_supported, ["oanda"]);
        assert!(fx.requirements.needs_portfolio_vol_target);
        assert_eq!(fx.requirements.max_abs_weight_per_instrument, Some(3.0));
        assert_eq!(fx.requirements.cross_asset_rebalance, CrossAssetRebalance::Needed);
        assert_eq!(fx.rebalance.kind, RebalanceKind::Monthly);

        let cr = entry("crypto_trend_100d");
        assert_eq!(cr.instruments, ["BTC/USD", "ETH/USD"]);
        assert_eq!(cr.parameters["sma_days"], json!(100));
        assert_eq!(cr.parameters["weight_per_coin"], json!(0.5));
        assert_eq!(cr.rebalance.kind, RebalanceKind::Daily);
        assert!(!cr.requirements.needs_shorting);
        assert!(!cr.requirements.needs_portfolio_vol_target);
        assert_eq!(cr.requirements.max_abs_weight_per_instrument, Some(0.5));
        assert_eq!(cr.requirements.cross_asset_rebalance, CrossAssetRebalance::Preferred);
    }

    #[test]
    fn fx_gross_exposure_is_the_saved_rederivation_not_the_old_six_x() {
        let fx = entry("fx_tsmom_12m");
        let g = fx.requirements.gross_exposure_reference.as_ref().expect("FX carries its measured gross exposure");
        // Numbers as printed in product-mandate/evidence/s2_gross_exposure_output.txt (PRIMARY block).
        assert_eq!((g.mean, g.median, g.p90, g.max, g.min), (2.69, 2.32, 4.41, 10.39, 0.92));
        assert_eq!((g.n_month_ends, g.first_month_end.as_str(), g.last_month_end.as_str()), (121, "2010-09-30", "2020-09-30"));
        assert!(g.source.contains("product-mandate/evidence/s2_gross_exposure.py"), "{}", g.source);
        assert!(g.note.contains("2020-10-23") && g.note.contains("RE-DERIVATION"), "data gaps must be stated: {}", g.note);
        assert_eq!(fx.requirements.min_leverage_gross, g.p90, "the gate is the stated p90 judgment");
        assert_ne!(fx.requirements.min_leverage_gross, 6.0);
        assert!(fx.requirements.notes.iter().any(|n| n.contains("JUDGMENT")));
    }

    #[test]
    fn fx_text_says_shorting_and_leverage_exist_lists_the_real_gaps_and_calls_the_evidence_weak() {
        let fx = entry("fx_tsmom_12m");
        let notes = fx.requirements.notes.join("\n");
        assert!(notes.contains("SHORTING AND LEVERAGE EXIST"), "{notes}");
        assert!(notes.contains("REAL PLATFORM GAPS") && notes.contains("capped at 1.0") && notes.contains("no cross-asset rebalancing"), "{notes}");
        assert!(notes.contains("OANDA live is not wired"), "{notes}");
        assert!(notes.contains("WEAK") && notes.contains("-0.06"), "{notes}");
        assert!(fx.verification_note.contains("shorting and leverage themselves DO exist"), "{}", fx.verification_note);
        assert!(fx.verification_note.contains("-0.06") && fx.verification_note.contains("known gaps"), "{}", fx.verification_note);
        assert!(!fx.rule_text.contains("far more than 1x"), "old wording removed");
    }

    #[test]
    fn platform_capabilities_today_are_exactly_the_verified_ones() {
        let c = current_platform_capabilities();
        assert!(c.shorting_in_backtest && c.account_leverage);
        assert!(!c.portfolio_vol_target && !c.cross_asset_rebalance && !c.per_instrument_weight_above_1);
        assert_eq!(c.venues_live_ready, ["kraken"], "no venue that has not been verified live may be added");
        let back: PlatformCapabilities = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(c, back);
    }
    #[test]
    fn seed_ranges_come_from_the_scorecard_and_admit_when_there_is_none() {
        let etf = entry("etf_trend_faber");
        assert_eq!((etf.expected_ranges[0].low, etf.expected_ranges[0].high), (Some(0.2), Some(1.0)));
        assert_eq!(etf.expected_ranges[0].role, RangeRole::PerformanceBand);
        assert_eq!(etf.expected_ranges[1].high, Some(0.25));

        let fx = entry("fx_tsmom_12m");
        assert_eq!(fx.expected_ranges[0].role, RangeRole::IntegrityBound, "no performance band exists for FX");
        assert_eq!((fx.expected_ranges[0].low, fx.expected_ranges[0].high), (Some(-2.0), Some(2.0)));
        assert_eq!((fx.expected_ranges[1].low, fx.expected_ranges[1].high), (Some(0.07), Some(0.13)));

        let cr = entry("crypto_trend_100d");
        assert!(!cr.expected_ranges[0].is_informative(), "crypto has no band in the source material");
        assert!(cr.expected_ranges[0].contains(123.0), "an unbounded range excludes nothing");
        for e in lib() {
            for r in &e.expected_ranges {
                assert!(r.source.len() > 20 && r.note.len() > 20, "{} range must say where it came from", e.id);
            }
        }
    }

    #[test]
    fn expected_range_contains_handles_edges() {
        let r = |low, high| ExpectedRange {
            metric: "m".into(),
            low,
            high,
            role: RangeRole::PerformanceBand,
            source: "s".into(),
            note: "n".into(),
        };
        let band = r(Some(0.2), Some(1.0));
        assert!(band.contains(0.2) && band.contains(1.0) && band.contains(0.5));
        assert!(!band.contains(0.19) && !band.contains(1.01));
        assert!(!band.contains(f64::NAN) && !band.contains(f64::INFINITY));
        assert!(r(Some(0.2), None).contains(99.0) && !r(Some(0.2), None).contains(0.1));
        assert!(r(None, Some(1.0)).contains(-99.0) && !r(None, Some(1.0)).contains(1.1));
    }

    #[test]
    fn entry_hash_is_stable_and_changes_with_any_field() {
        let a = entry("etf_trend_faber");
        assert_eq!(entry_hash(&a), entry_hash(&entry("etf_trend_faber")));
        assert_eq!(entry_hash(&a).len(), 64);
        let mut b = a.clone();
        b.parameters.insert("sma_month_ends".into(), json!(12));
        assert_ne!(entry_hash(&a), entry_hash(&b));
        let mut c = a.clone();
        c.version = 2;
        assert_ne!(entry_hash(&a), entry_hash(&c));
    }

    #[test]
    fn entry_json_round_trips_and_rejects_unknown_fields() {
        for e in lib() {
            let back: LibraryEntry = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
            assert_eq!(e, back);
        }
        let mut v = serde_json::to_value(entry("crypto_trend_100d")).unwrap();
        v["auto_tune"] = json!(true);
        assert!(serde_json::from_value::<LibraryEntry>(v).is_err());
    }

    #[test]
    fn validate_entry_flags_curation_slips() {
        let good = entry("crypto_trend_100d");
        let bad = |edit: &dyn Fn(&mut LibraryEntry)| {
            let mut e = good.clone();
            edit(&mut e);
            validate_entry(&e)
        };
        assert!(!bad(&|e| e.id = "Has Space".into()).is_empty());
        assert!(!bad(&|e| e.version = 0).is_empty());
        assert!(!bad(&|e| e.rule_text = "  ".into()).is_empty());
        assert!(!bad(&|e| e.sources.clear()).is_empty());
        assert!(!bad(&|e| e.instruments.clear()).is_empty());
        assert!(!bad(&|e| e.requirements.max_abs_weight_per_instrument = Some(0.0)).is_empty());
        assert!(!bad(&|e| e.requirements.max_abs_weight_per_instrument = Some(f64::NAN)).is_empty());
        let fx = entry("fx_tsmom_12m");
        let bad_fx = |edit: &dyn Fn(&mut GrossExposureStats)| {
            let mut e = fx.clone();
            edit(e.requirements.gross_exposure_reference.as_mut().unwrap());
            validate_entry(&e)
        };
        assert!(bad_fx(&|_| {}).is_empty());
        assert!(!bad_fx(&|g| g.median = 99.0).is_empty(), "median above max");
        assert!(!bad_fx(&|g| g.source = " ".into()).is_empty());
        assert!(!bad_fx(&|g| g.n_month_ends = 0).is_empty());
        assert!(!bad(&|e| e.requirements.min_leverage_gross = f64::NAN).is_empty());
        assert!(!bad(&|e| e.requirements.min_leverage_gross = 0.0).is_empty());
        assert!(!bad(&|e| e.expected_ranges.clear()).is_empty());
        assert!(!bad(&|e| {
            e.expected_ranges[0].low = Some(2.0);
            e.expected_ranges[0].high = Some(1.0);
        })
        .is_empty());
        assert!(!bad(&|e| e.last_verified = "21/09/2026".into()).is_empty());
        assert!(!bad(&|e| e.retirement_rule.clear()).is_empty());
        assert!(!bad(&|e| e.failure_regimes.clear()).is_empty());
    }

    #[test]
    fn seed_ids_match_files_and_are_unique() {
        let l = lib();
        let ids: BTreeSet<&str> = l.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids.len(), l.len());
    }

    // ------------------------------------------------------------------ eligibility: the expected scenario

    #[test]
    fn long_only_unleveraged_alpaca_kraken_mandate() {
        let m = base_mandate();

        let etf = elig(&entry("etf_trend_faber"), &m);
        assert!(etf.eligible_paper, "{etf:?}");
        assert!(!etf.eligible_live, "alpaca is not live-ready");
        assert_eq!(codes(&etf), [ReasonCode::NotLiveReady]);
        let r = etf.reasons.iter().find(|r| r.code == ReasonCode::NotLiveReady).unwrap();
        assert_eq!((r.blocks, r.origin), (Blocks::LiveOnly, Origin::Platform));
        assert_eq!(advisory_codes(&etf), [ReasonCode::CrossAssetRebalanceApproximated]);
        assert!(!etf.blocked_by_mandate() && etf.blocked_by_platform());

        let cr = elig(&entry("crypto_trend_100d"), &m);
        assert!(cr.eligible_paper && cr.eligible_live, "{cr:?}");
        assert!(codes(&cr).is_empty());
        assert_eq!(advisory_codes(&cr), [ReasonCode::CrossAssetRebalanceApproximated], "an approximation warning, not a block");
        assert!(!cr.blocked_by_mandate() && !cr.blocked_by_platform());

        let fx = elig(&entry("fx_tsmom_12m"), &m);
        assert!(!fx.eligible_paper && !fx.eligible_live);
        // this mandate forbids shorting, leverage, the class, the instruments and the venue ...
        for c in [
            ReasonCode::ShortingNotPermitted,
            ReasonCode::LeverageExceedsCap,
            ReasonCode::GrossExceedsExposureLimit,
            ReasonCode::VenueNotPermitted,
            ReasonCode::AssetClassNotPermitted,
            ReasonCode::InstrumentsNotAllowed,
        ] {
            assert!(codes(&fx).contains(&c), "{c:?} missing from {:?}", codes(&fx));
        }
        // ... and the platform ALSO cannot run it; both are reported, each with its own origin
        for c in [ReasonCode::PortfolioVolTargetUnsupported, ReasonCode::WeightAboveOneUnsupported, ReasonCode::CrossAssetRebalanceUnsupported] {
            assert!(codes(&fx).contains(&c), "{c:?} missing from {:?}", codes(&fx));
        }
        assert!(fx.blocked_by_mandate() && fx.blocked_by_platform());
        assert!(!codes(&fx).contains(&ReasonCode::NotLiveReady), "no permitted venue, so live readiness is moot");
        assert!(!codes(&fx).contains(&ReasonCode::ShortingUnsupported), "the platform CAN short");
        assert!(!codes(&fx).contains(&ReasonCode::LeverageUnsupported), "the platform HAS account leverage");
        assert!(fx.reasons.iter().all(|r| !r.message.is_empty()));
    }

    // The owner's exact question: the mandate permits everything FX needs; is FX still blocked, and by whom?
    #[test]
    fn a_mandate_that_permits_everything_still_cannot_run_fx_because_of_the_platform() {
        let m = fx_friendly_mandate();
        let fx = elig(&entry("fx_tsmom_12m"), &m);
        assert!(!fx.eligible_paper && !fx.eligible_live);
        assert!(!fx.blocked_by_mandate(), "nothing in the mandate stands in the way: {fx:?}");
        assert!(fx.blocked_by_platform());
        let blocking: Vec<_> = fx.reasons.iter().filter(|r| r.blocks != Blocks::Nothing).collect();
        assert!(blocking.iter().all(|r| r.origin == Origin::Platform), "only platform-origin blockers: {blocking:?}");
        assert_eq!(
            codes(&fx),
            [
                ReasonCode::PortfolioVolTargetUnsupported,
                ReasonCode::WeightAboveOneUnsupported,
                ReasonCode::CrossAssetRebalanceUnsupported,
                ReasonCode::NotLiveReady
            ]
        );
        // the three faithfulness gaps block paper AND live; OANDA not being live only blocks live
        for r in &blocking {
            let expect = if r.code == ReasonCode::NotLiveReady { Blocks::LiveOnly } else { Blocks::PaperAndLive };
            assert_eq!(r.blocks, expect, "{:?}", r.code);
        }
        // the gross-exposure gate is met (4.41 <= 6), but the reference run peaked at 10.39: advisory only
        assert_eq!(advisory_codes(&fx), [ReasonCode::PeakGrossMayClip]);
        // shorting and leverage are not reported as problems at all
        assert!(!fx.reasons.iter().any(|r| matches!(r.code, ReasonCode::ShortingNotPermitted | ReasonCode::ShortingUnsupported | ReasonCode::LeverageExceedsCap | ReasonCode::LeverageUnsupported)));
    }

    // The gate is driven by capability flags: a future platform makes FX eligible.
    #[test]
    fn flipping_platform_capabilities_makes_fx_eligible_so_the_gate_is_capability_driven() {
        let m = fx_friendly_mandate();
        let fx = entry("fx_tsmom_12m");
        let mut c = caps();
        assert!(!elig_with(&fx, &m, &c).eligible_paper);
        // each faithfulness gap alone still blocks
        for flip in [0, 1, 2] {
            let mut one = caps();
            match flip {
                0 => one.portfolio_vol_target = true,
                1 => one.cross_asset_rebalance = true,
                _ => one.per_instrument_weight_above_1 = true,
            }
            assert!(!elig_with(&fx, &m, &one).eligible_paper, "flag {flip} alone must not be enough");
        }
        c.portfolio_vol_target = true;
        c.cross_asset_rebalance = true;
        c.per_instrument_weight_above_1 = true;
        let e = elig_with(&fx, &m, &c);
        assert!(e.eligible_paper, "{e:?}");
        assert!(!e.eligible_live, "OANDA is still not live-ready");
        assert_eq!(codes(&e), [ReasonCode::NotLiveReady]);
        assert!(!e.blocked_by_mandate());
        // ... and once OANDA is live-ready, live too
        c.venues_live_ready.push("oanda".into());
        let e = elig_with(&fx, &m, &c);
        assert!(e.eligible_paper && e.eligible_live, "{e:?}");
        // and the ETF entry's approximation advisory goes away with cross-asset rebalancing
        assert!(advisory_codes(&elig_with(&entry("etf_trend_faber"), &m, &c)).is_empty());
    }

    // Each of the three faithfulness gaps blocks FX by itself: leave exactly one missing.
    #[test]
    fn each_platform_gap_alone_blocks_fx_on_paper_and_live() {
        let m = fx_friendly_mandate();
        let fx = entry("fx_tsmom_12m");
        for (missing, code) in [
            (0, ReasonCode::PortfolioVolTargetUnsupported),
            (1, ReasonCode::WeightAboveOneUnsupported),
            (2, ReasonCode::CrossAssetRebalanceUnsupported),
        ] {
            let mut c = caps();
            c.portfolio_vol_target = missing != 0;
            c.per_instrument_weight_above_1 = missing != 1;
            c.cross_asset_rebalance = missing != 2;
            c.venues_live_ready.push("oanda".into());
            let e = elig_with(&fx, &m, &c);
            assert!(!e.eligible_paper && !e.eligible_live, "{code:?}: {e:?}");
            assert_eq!(codes(&e), [code]);
            assert_eq!(e.reasons.iter().find(|r| r.code == code).unwrap().blocks, Blocks::PaperAndLive);
        }
    }

    // Approximation is a warning, never a block.
    #[test]
    fn etf_and_crypto_get_an_advisory_approximation_note_but_stay_eligible_for_paper() {
        let m = base_mandate();
        for id in ["etf_trend_faber", "crypto_trend_100d"] {
            let e = elig(&entry(id), &m);
            assert!(e.eligible_paper, "{id}: {e:?}");
            let adv: Vec<&Reason> = e.advisories().collect();
            assert_eq!(adv.len(), 1, "{id}");
            assert_eq!(adv[0].code, ReasonCode::CrossAssetRebalanceApproximated);
            assert_eq!((adv[0].origin, adv[0].blocks), (Origin::Advisory, Blocks::Nothing));
            assert!(adv[0].message.contains("independent sub-account") && adv[0].message.contains("approximate"), "{}", adv[0].message);
        }
        // a Needed entry with the same missing capability is a block, a Preferred one is not
        let mut e = entry("crypto_trend_100d");
        e.requirements.cross_asset_rebalance = CrossAssetRebalance::Needed;
        let r = elig(&e, &m);
        assert!(!r.eligible_paper && codes(&r) == [ReasonCode::CrossAssetRebalanceUnsupported], "{r:?}");
        e.requirements.cross_asset_rebalance = CrossAssetRebalance::NotNeeded;
        let r = elig(&e, &m);
        assert!(r.eligible_live && r.reasons.is_empty(), "{r:?}");
    }

    // Mandate-origin and platform-origin reasons are never mixed up.
    #[test]
    fn mandate_and_platform_reasons_are_never_mixed_up() {
        use ReasonCode::*;
        let expected = [
            (ShortingNotPermitted, Origin::Mandate),
            (LeverageExceedsCap, Origin::Mandate),
            (GrossExceedsExposureLimit, Origin::Mandate),
            (AssetClassNotPermitted, Origin::Mandate),
            (InstrumentsNotAllowed, Origin::Mandate),
            (InstrumentDenied, Origin::Mandate),
            (VenueNotPermitted, Origin::Mandate),
            (ShortingUnsupported, Origin::Platform),
            (LeverageUnsupported, Origin::Platform),
            (PortfolioVolTargetUnsupported, Origin::Platform),
            (WeightAboveOneUnsupported, Origin::Platform),
            (CrossAssetRebalanceUnsupported, Origin::Platform),
            (NotLiveReady, Origin::Platform),
            (CrossAssetRebalanceApproximated, Origin::Advisory),
            (PeakGrossMayClip, Origin::Advisory),
        ];
        for (code, origin) in expected {
            assert_eq!(code.origin(), origin, "{code:?}");
        }
        // every reason actually emitted carries its code's origin, in every scenario
        let mut scenarios = Vec::new();
        for id in ["etf_trend_faber", "fx_tsmom_12m", "crypto_trend_100d"] {
            for m in [base_mandate(), fx_friendly_mandate()] {
                scenarios.push(elig(&entry(id), &m));
            }
        }
        for e in &scenarios {
            for r in &e.reasons {
                assert_eq!(r.origin, r.code.origin(), "{r:?}");
                assert_eq!(r.blocks == Blocks::Nothing, r.origin == Origin::Advisory, "advisory iff non-blocking: {r:?}");
            }
        }
        // a mandate-only problem yields no platform blocker ...
        let mut e = entry("crypto_trend_100d");
        e.requirements.needs_shorting = true;
        let r = elig(&e, &base_mandate());
        assert_eq!(codes(&r), [ShortingNotPermitted]);
        assert!(r.blocked_by_mandate() && !r.blocked_by_platform());
        // ... and a platform-only problem yields no mandate blocker
        let mut none_can_short = caps();
        none_can_short.shorting_in_backtest = false;
        let r = elig_with(&e, &mandate_with(|v| v["universe"]["shorting"] = json!(true)), &none_can_short);
        assert_eq!(codes(&r), [ShortingUnsupported]);
        assert!(!r.blocked_by_mandate() && r.blocked_by_platform());
        // both at once: both reported, each under its own origin
        let r = elig_with(&e, &base_mandate(), &none_can_short);
        assert_eq!(codes(&r), [ShortingNotPermitted, ShortingUnsupported]);
        assert!(r.blocked_by_mandate() && r.blocked_by_platform());
    }

    #[test]
    fn platform_leverage_and_weight_rules_are_capability_driven() {
        let mut e = entry("crypto_trend_100d");
        e.requirements.min_leverage_gross = 2.0;
        let m = mandate_with(|v| {
            v["universe"]["leverage_max_gross"] = json!(2.0);
            v["exposure"]["max_gross"] = json!(2.0);
        });
        assert!(elig(&e, &m).eligible_live, "the platform has account leverage today");
        let mut no_lev = caps();
        no_lev.account_leverage = false;
        let r = elig_with(&e, &m, &no_lev);
        assert_eq!(codes(&r), [ReasonCode::LeverageUnsupported]);
        assert!(!r.eligible_paper);
        // exactly 1x needs no leverage capability
        e.requirements.min_leverage_gross = 1.0;
        assert!(elig_with(&e, &base_mandate(), &no_lev).eligible_live);
        // a weight of exactly 1.0 is expressible; above 1 is not (today)
        e.requirements.max_abs_weight_per_instrument = Some(1.0);
        assert!(elig(&e, &base_mandate()).eligible_live);
        e.requirements.max_abs_weight_per_instrument = Some(1.5);
        assert_eq!(codes(&elig(&e, &base_mandate())), [ReasonCode::WeightAboveOneUnsupported]);
        e.requirements.max_abs_weight_per_instrument = None;
        assert!(elig(&e, &base_mandate()).eligible_live);
        // a vol-target requirement blocks until the platform has it
        e.requirements.needs_portfolio_vol_target = true;
        assert_eq!(codes(&elig(&e, &base_mandate())), [ReasonCode::PortfolioVolTargetUnsupported]);
    }

    #[test]
    fn live_readiness_is_a_platform_fact_and_needs_a_venue_the_entry_can_use_under_the_mandate() {
        // kraken is live-ready but the ETF entry only trades on alpaca: no help
        let etf = entry("etf_trend_faber");
        let r = elig(&etf, &base_mandate());
        assert_eq!(codes(&r), [ReasonCode::NotLiveReady]);
        // if the platform verifies alpaca live, the block disappears
        let mut c = caps();
        c.venues_live_ready.push(" Alpaca ".into());
        assert!(elig_with(&etf, &base_mandate(), &c).eligible_live);
        // a live-ready venue the MANDATE does not permit does not help either
        let mandate_alpaca_only = mandate_with(|v| v["universe"]["venues"] = json!(["alpaca"]));
        let mut c = caps();
        c.venues_live_ready = vec!["ibkr".into()];
        assert_eq!(codes(&elig_with(&etf, &mandate_alpaca_only, &c)), [ReasonCode::NotLiveReady]);
        // no live-ready venues at all: crypto is paper-only
        c.venues_live_ready.clear();
        let r = elig_with(&entry("crypto_trend_100d"), &base_mandate(), &c);
        assert!(r.eligible_paper && !r.eligible_live);
    }

    #[test]
    fn peak_gross_advisory_appears_only_when_the_gate_is_met_but_the_peak_is_not_covered() {
        let fx = entry("fx_tsmom_12m");
        let m6 = fx_friendly_mandate(); // room 6 < peak 10.39
        assert_eq!(advisory_codes(&elig(&fx, &m6)), [ReasonCode::PeakGrossMayClip]);
        let m12 = mandate_with(|v| {
            v["universe"]["leverage_max_gross"] = json!(12.0);
            v["exposure"]["max_gross"] = json!(12.0);
            v["exposure"]["max_net"] = json!(12.0);
        });
        assert!(advisory_codes(&elig(&fx, &m12)).is_empty(), "room above the observed peak");
        // gate not met (room 1 < 4.41): the mandate reasons explain it, no advisory on top
        let r = elig(&fx, &base_mandate());
        assert!(advisory_codes(&r).is_empty());
    }

    #[test]
    fn original_mandate_fixture_allow_list_leaves_the_etf_sleeve_ineligible() {
        // mandate.rs's own fixture allows only SPY, EFA, BTC/USD, ETH/USD.
        let m = mandate_with(|v| v["universe"]["instrument_allow"] = json!(["SPY", "EFA", "BTC/USD", "ETH/USD"]));
        let e = elig(&entry("etf_trend_faber"), &m);
        assert!(!e.eligible_paper);
        let r = e.reasons.iter().find(|r| r.code == ReasonCode::InstrumentsNotAllowed).expect("reason present");
        assert!(r.message.contains("IEF") && r.message.contains("DBC") && r.message.contains("VNQ"), "{}", r.message);
        assert!(!r.message.contains("\"SPY\""), "SPY is allowed and must not be listed: {}", r.message);
        assert!(r.message.contains("3 of 5"), "{}", r.message);
        // crypto is unaffected
        assert!(elig(&entry("crypto_trend_100d"), &m).eligible_live);
    }


    // ------------------------------------------------------------------ eligibility: each rule alone

    #[test]
    fn shorting_rule() {
        let mut e = entry("crypto_trend_100d");
        e.requirements.needs_shorting = true;
        let r = elig(&e, &base_mandate());
        assert_eq!(codes(&r), [ReasonCode::ShortingNotPermitted]);
        assert!(!r.eligible_paper);
        let allowed = mandate_with(|v| v["universe"]["shorting"] = json!(true));
        assert!(elig(&e, &allowed).eligible_live);
    }

    #[test]
    fn leverage_rule_checks_both_the_cap_and_the_exposure_limit() {
        let mut e = entry("crypto_trend_100d");
        e.requirements.min_leverage_gross = 1.5;
        let r = elig(&e, &base_mandate());
        assert_eq!(codes(&r), [ReasonCode::LeverageExceedsCap, ReasonCode::GrossExceedsExposureLimit]);
        // cap raised but exposure limit not: only the exposure reason remains
        let m = mandate_with(|v| v["universe"]["leverage_max_gross"] = json!(2.0));
        assert_eq!(codes(&elig(&e, &m)), [ReasonCode::GrossExceedsExposureLimit]);
        // both raised: fine
        let m = mandate_with(|v| {
            v["universe"]["leverage_max_gross"] = json!(2.0);
            v["exposure"]["max_gross"] = json!(2.0);
        });
        assert!(elig(&e, &m).eligible_live);
        // exactly equal is allowed
        e.requirements.min_leverage_gross = 1.0;
        assert!(elig(&e, &base_mandate()).eligible_live);
        // just above is not
        e.requirements.min_leverage_gross = 1.0001;
        assert!(!elig(&e, &base_mandate()).eligible_paper);
    }

    #[test]
    fn non_finite_leverage_fails_closed() {
        let mut e = entry("crypto_trend_100d");
        e.requirements.min_leverage_gross = f64::NAN;
        assert!(!elig(&e, &base_mandate()).eligible_paper);
        let mut m = base_mandate();
        m.universe.leverage_max_gross = f64::NAN;
        assert!(!elig(&entry("crypto_trend_100d"), &m).eligible_paper);
    }

    #[test]
    fn asset_class_rule_requires_all_classes_and_ignores_case() {
        let m = mandate_with(|v| {
            v["universe"]["asset_classes"] = json!(["US_ETF"]);
            v["exposure"]["max_asset_class"] = json!({});
        });
        assert!(elig(&entry("etf_trend_faber"), &m).eligible_paper, "case-insensitive");
        let r = elig(&entry("crypto_trend_100d"), &m);
        assert_eq!(codes(&r), [ReasonCode::AssetClassNotPermitted]);
        assert!(r.reasons[0].message.contains("crypto_spot"));
        // a two-class entry needs both
        let mut e = entry("crypto_trend_100d");
        e.requirements.asset_classes = vec!["crypto_spot".into(), "us_etf".into()];
        let r = elig(&e, &base_mandate());
        assert!(r.eligible_live);
        let only_crypto = mandate_with(|v| {
            v["universe"]["asset_classes"] = json!(["crypto_spot"]);
            v["exposure"]["max_asset_class"] = json!({});
        });
        assert_eq!(codes(&elig(&e, &only_crypto)), [ReasonCode::AssetClassNotPermitted]);
    }

    #[test]
    fn instrument_allow_list_rule_requires_every_instrument_and_normalises_names() {
        // "btc-usd" and "ETHUSD" spell the same instruments
        let m = mandate_with(|v| v["universe"]["instrument_allow"] = json!(["btc-usd", "ETHUSD", "SPY"]));
        assert!(elig(&entry("crypto_trend_100d"), &m).eligible_live);
        // only one of two coins allowed
        let m = mandate_with(|v| v["universe"]["instrument_allow"] = json!(["BTC/USD"]));
        let r = elig(&entry("crypto_trend_100d"), &m);
        assert_eq!(codes(&r), [ReasonCode::InstrumentsNotAllowed]);
        assert!(r.reasons[0].message.contains("ETH/USD") && r.reasons[0].message.contains("1 of 2"), "{}", r.reasons[0].message);
        // none allowed
        let m = mandate_with(|v| v["universe"]["instrument_allow"] = json!(["SPY"]));
        let r = elig(&entry("crypto_trend_100d"), &m);
        assert!(r.reasons[0].message.contains("none of them"), "{}", r.reasons[0].message);
        // empty allow-list (invalid mandate) must not panic and must fail
        let mut m = base_mandate();
        m.universe.instrument_allow.clear();
        assert!(!elig(&entry("crypto_trend_100d"), &m).eligible_paper);
    }

    #[test]
    fn instrument_deny_rule() {
        let m = mandate_with(|v| v["universe"]["instrument_deny"] = json!(["eth/usd"]));
        let r = elig(&entry("crypto_trend_100d"), &m);
        assert_eq!(codes(&r), [ReasonCode::InstrumentDenied]);
        assert!(!r.eligible_paper);
        assert!(r.reasons[0].message.contains("ETH/USD"));
    }

    #[test]
    fn a_denied_instrument_missing_from_allow_gives_both_reasons() {
        let m = mandate_with(|v| {
            v["universe"]["instrument_allow"] = json!(["BTC/USD"]);
            v["universe"]["instrument_deny"] = json!(["ETH/USD"]);
        });
        assert_eq!(codes(&elig(&entry("crypto_trend_100d"), &m)), [ReasonCode::InstrumentsNotAllowed, ReasonCode::InstrumentDenied]);
    }

    #[test]
    fn venue_rule_blocks_everything_when_no_venue_overlaps() {
        let m = mandate_with(|v| v["universe"]["venues"] = json!(["alpaca"]));
        let r = elig(&entry("crypto_trend_100d"), &m);
        assert_eq!(codes(&r), [ReasonCode::VenueNotPermitted]);
        assert!(!r.eligible_paper && !r.eligible_live);
        assert_eq!(r.reasons[0].blocks, Blocks::PaperAndLive);
        // case and whitespace do not matter
        let m = mandate_with(|v| v["universe"]["venues"] = json!([" Kraken "]));
        assert!(elig(&entry("crypto_trend_100d"), &m).eligible_live);
    }

    #[test]
    fn not_live_ready_blocks_live_but_allows_paper() {
        let r = elig(&entry("etf_trend_faber"), &base_mandate());
        assert!(r.eligible_paper && !r.eligible_live);
        let reason = r.reasons.iter().find(|r| r.code == ReasonCode::NotLiveReady).expect("present");
        assert_eq!((reason.blocks, reason.origin), (Blocks::LiveOnly, Origin::Platform));
        assert_eq!(codes(&r), [ReasonCode::NotLiveReady]);
    }

    #[test]
    fn every_failing_reason_is_reported_together_and_sorted() {
        let mut e = entry("fx_tsmom_12m");
        e.instruments = vec!["EUR/USD".into()];
        let r = elig(&e, &base_mandate());
        let c = codes(&r);
        let mut sorted = c.clone();
        sorted.sort();
        assert_eq!(c, sorted);
        // mandate: shorting, leverage, gross, class, instruments, venue; platform: vol target, weight, cross-asset
        assert_eq!(c.len(), 9, "{c:?}");
        assert_eq!(c.iter().filter(|x| x.origin() == Origin::Mandate).count(), 6);
        assert_eq!(c.iter().filter(|x| x.origin() == Origin::Platform).count(), 3);
    }

    #[test]
    fn eligibility_is_deterministic_and_serialises() {
        let a = elig(&entry("fx_tsmom_12m"), &base_mandate());
        let b = elig(&entry("fx_tsmom_12m"), &base_mandate());
        assert_eq!(a, b);
        let s = serde_json::to_string(&a).unwrap();
        assert!(s.contains("shorting_not_permitted") && s.contains("venue_not_permitted"), "{s}");
    }

    // ------------------------------------------------------------------ inverse-vol

    #[test]
    fn inverse_vol_weights_sum_to_one_and_favour_low_vol() {
        let w = inverse_vol_weights(&[0.10, 0.20, 0.40]).unwrap();
        assert!((w.iter().sum::<f64>() - 1.0).abs() < 1e-12);
        assert!(w[0] > w[1] && w[1] > w[2]);
        assert!((w[0] / w[1] - 2.0).abs() < 1e-12, "half the vol, twice the weight");
        assert!((w[1] / w[2] - 2.0).abs() < 1e-12);
        let one = inverse_vol_weights(&[0.3]).unwrap();
        assert_eq!(one, vec![1.0]);
        let eq = inverse_vol_weights(&[0.2, 0.2, 0.2, 0.2]).unwrap();
        assert!(eq.iter().all(|x| (x - 0.25).abs() < 1e-12));
    }

    #[test]
    fn inverse_vol_weights_reject_bad_input() {
        assert_eq!(inverse_vol_weights(&[]), None);
        assert_eq!(inverse_vol_weights(&[0.1, 0.0]), None);
        assert_eq!(inverse_vol_weights(&[0.1, -0.2]), None);
        assert_eq!(inverse_vol_weights(&[0.1, f64::NAN]), None);
        assert_eq!(inverse_vol_weights(&[0.1, f64::INFINITY]), None);
        assert_eq!(inverse_vol_weights(&[f64::NEG_INFINITY]), None);
        assert_eq!(inverse_vol_weights(&[1e-320, 0.1]), None, "reciprocal overflow");
    }

    #[test]
    fn inverse_vol_weights_are_scale_invariant() {
        let a = inverse_vol_weights(&[0.1, 0.25, 0.5]).unwrap();
        let b = inverse_vol_weights(&[1.0, 2.5, 5.0]).unwrap();
        for (x, y) in a.iter().zip(&b) {
            assert!((x - y).abs() < 1e-12);
        }
    }

    // ------------------------------------------------------------------ ladder fit: hand-computed cases

    #[test]
    fn replay_drawdown_hand_cases() {
        assert_eq!(replay_max_drawdown(&[], 1.0), 0.0);
        assert_eq!(replay_max_drawdown(&[0.01, 0.02, 0.03], 1.0), 0.0, "all gains: no drawdown");
        // +10% then -20%: peak 1.1, end 0.88 => 20%
        assert!((replay_max_drawdown(&[0.10, -0.20], 1.0) - 0.20).abs() < 1e-12);
        // starting equity is a high-water mark: -10% at once is a 10% drawdown
        assert!((replay_max_drawdown(&[-0.10, 0.05], 1.0) - 0.10).abs() < 1e-12);
        // compounding: two -10% days => 19%, not 20%
        assert!((replay_max_drawdown(&[-0.10, -0.10], 1.0) - 0.19).abs() < 1e-12);
        // scale halves the moves before compounding: -5% -5% => 9.75%
        assert!((replay_max_drawdown(&[-0.10, -0.10], 0.5) - 0.0975).abs() < 1e-12);
        // ruin
        assert_eq!(replay_max_drawdown(&[-1.0], 1.0), 1.0);
        assert_eq!(replay_max_drawdown(&[-0.6], 2.0), 1.0, "a scaled loss beyond -100% is ruin");
        // recovers to a new peak, then a smaller dip: worst is the first
        assert!((replay_max_drawdown(&[-0.10, 0.20, -0.05], 1.0) - 0.10).abs() < 1e-12);
    }

    #[test]
    fn fit_returns_one_when_already_inside() {
        let r = [0.01, -0.02, 0.015, -0.01, 0.02]; // worst dd ~2%
        let fit = fit_scale_to_ladder(&r, &ladder(), 0.8).unwrap();
        assert_eq!(fit.scale, 1.0);
        assert_eq!(fit.first_rung_at, 0.10);
        assert!(fit.rungs_hit.is_empty() && fit.unscaled_rungs_hit.is_empty());
        assert_eq!(fit.replay_max_drawdown, fit.unscaled_max_drawdown);
        assert_eq!(fit.observations, 5);
    }

    #[test]
    fn fit_boundary_exactly_at_the_target_is_inside() {
        // a single -8% day: drawdown exactly 0.08 = 0.8 * 0.10
        let fit = fit_scale_to_ladder(&[-0.08], &ladder(), 0.8).unwrap();
        assert_eq!(fit.scale, 1.0);
        // drawdown exactly equal to the target in floating point (0.5 = 1.0 * 0.5) is inside: scale exactly 1.0
        let half = [LadderRung { at: 0.5, action: LadderAction::HaltFlatten, scale: None }];
        let fit = fit_scale_to_ladder(&[-0.5], &half, 1.0).unwrap();
        assert_eq!(fit.scale, 1.0);
        assert_eq!(fit.replay_max_drawdown, 0.5);
        // headroom 1.0 with dd exactly at the rung is inside the target but touches the rung
        let fit = fit_scale_to_ladder(&[-0.10], &ladder(), 1.0).unwrap();
        assert_eq!(fit.scale, 1.0);
        assert_eq!(fit.rungs_hit, vec![0]);
    }

    #[test]
    fn fit_single_crash_scales_linearly() {
        // -40% in one day; target 0.8 * 0.10 = 0.08 => scale 0.2 (1 - 0.2*0.4 = 0.92)
        let fit = fit_scale_to_ladder(&[-0.40, 0.10], &ladder(), 0.8).unwrap();
        assert!((fit.scale - 0.2).abs() < 1e-9, "{fit:?}");
        assert!(fit.replay_max_drawdown <= 0.08 + 1e-12 && fit.replay_max_drawdown > 0.08 - 1e-6, "{fit:?}");
        assert!((fit.unscaled_max_drawdown - 0.40).abs() < 1e-12);
        assert_eq!(fit.unscaled_rungs_hit, vec![0, 1], "unscaled replay would have shrunk AND halted");
        assert!(fit.rungs_hit.is_empty(), "the fitted replay stays above the first rung");
    }

    #[test]
    fn fit_all_positive_and_zero_series_need_no_scaling() {
        let gains = vec![0.001; 500];
        let fit = fit_scale_to_ladder(&gains, &ladder(), 0.8).unwrap();
        assert_eq!((fit.scale, fit.replay_max_drawdown), (1.0, 0.0));
        let flat = vec![0.0; 10];
        assert_eq!(fit_scale_to_ladder(&flat, &ladder(), 0.8).unwrap().scale, 1.0);
    }

    #[test]
    fn fit_rejects_bad_inputs() {
        let l = ladder();
        assert_eq!(fit_scale_to_ladder(&[], &l, 0.8), Err(LadderFitError::EmptySeries));
        assert_eq!(fit_scale_to_ladder(&[0.01], &[], 0.8), Err(LadderFitError::EmptyLadder));
        assert_eq!(fit_scale_to_ladder(&[0.01, f64::NAN], &l, 0.8), Err(LadderFitError::NonFiniteReturn { index: 1 }));
        assert_eq!(fit_scale_to_ladder(&[f64::INFINITY], &l, 0.8), Err(LadderFitError::NonFiniteReturn { index: 0 }));
        assert_eq!(fit_scale_to_ladder(&[0.0, -1.5], &l, 0.8), Err(LadderFitError::ReturnBelowMinusOne { index: 1 }));
        for h in [0.0, -0.1, 1.01, f64::NAN, f64::INFINITY] {
            assert_eq!(fit_scale_to_ladder(&[0.01], &l, h), Err(LadderFitError::BadHeadroom), "{h}");
        }
        for at in [0.0, -0.1, 1.5, f64::NAN] {
            let bad = [LadderRung { at, action: LadderAction::HaltFlatten, scale: None }];
            assert_eq!(fit_scale_to_ladder(&[0.01], &bad, 0.8), Err(LadderFitError::BadFirstRung), "{at}");
        }
        assert!(!LadderFitError::EmptySeries.to_string().is_empty());
    }

    #[test]
    fn fit_a_return_of_exactly_minus_one_still_fits() {
        // -100% day: at scale s the loss is s. target 0.08 => s = 0.08
        let fit = fit_scale_to_ladder(&[-1.0], &ladder(), 0.8).unwrap();
        assert!((fit.scale - 0.08).abs() < 1e-9, "{fit:?}");
        assert_eq!(fit.unscaled_max_drawdown, 1.0);
    }

    #[test]
    fn fit_uses_only_the_first_rung() {
        let l = vec![
            LadderRung { at: 0.05, action: LadderAction::Shrink, scale: Some(0.5) },
            LadderRung { at: 0.5, action: LadderAction::HaltFlatten, scale: None },
        ];
        let fit = fit_scale_to_ladder(&[-0.20], &l, 1.0).unwrap();
        assert_eq!(fit.first_rung_at, 0.05);
        assert!((fit.scale - 0.25).abs() < 1e-9, "{fit:?}");
    }

    #[test]
    fn fit_unreachable_target_is_an_error_not_a_zero_scale() {
        // a first rung at 1e-9 with a -50% day needs a scale near 1.6e-9, below MIN_FIT_SCALE
        let l = [LadderRung { at: 1e-9, action: LadderAction::HaltFlatten, scale: None }];
        assert_eq!(fit_scale_to_ladder(&[-0.5], &l, 0.8), Err(LadderFitError::TargetUnreachable));
        // 1e-300 must not return a float-noise "scale" either (equity rounds to 1.0 at scale 1e-16)
        let l = [LadderRung { at: 1e-300, action: LadderAction::HaltFlatten, scale: None }];
        assert_eq!(fit_scale_to_ladder(&[-0.5], &l, 0.8), Err(LadderFitError::TargetUnreachable));
        // just above the floor still fits: -50% day, target 8e-6 => scale 1.6e-5
        let l = [LadderRung { at: 1e-5, action: LadderAction::HaltFlatten, scale: None }];
        let fit = fit_scale_to_ladder(&[-0.5], &l, 0.8).unwrap();
        assert!((fit.scale - 1.6e-5).abs() < 1e-9, "{fit:?}");
    }

    // ------------------------------------------------------------------ ladder fit: property-style

    /// Small deterministic generator (SplitMix64) so the property tests need no extra crates.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        /// Uniform in [0, 1).
        fn unit(&mut self) -> f64 {
            (self.next() >> 11) as f64 / (1u64 << 53) as f64
        }
        fn range(&mut self, lo: f64, hi: f64) -> f64 {
            lo + (hi - lo) * self.unit()
        }
    }

    /// A random daily-return path: drift, volatility and fat-ish tails vary per path.
    fn random_path(rng: &mut Rng, len: usize) -> Vec<f64> {
        let drift = rng.range(-0.0005, 0.0015);
        let vol = rng.range(0.002, 0.04);
        (0..len)
            .map(|_| {
                let u = rng.unit() + rng.unit() + rng.unit() - 1.5; // ~ bell shaped, |u| <= 1.5
                let shock = if rng.unit() < 0.01 { rng.range(-0.25, 0.20) } else { 0.0 };
                (drift + vol * u * 1.6 + shock).clamp(-0.6, 0.6)
            })
            .collect()
    }

    #[test]
    fn property_larger_scale_never_lowers_drawdown() {
        let mut rng = Rng(7);
        for _ in 0..300 {
            let path = random_path(&mut rng, 250);
            let mut prev = 0.0;
            for k in 0..=40 {
                let s = 0.05 * k as f64; // 0 ..= 2, beyond 1 on purpose
                let dd = replay_max_drawdown(&path, s);
                assert!(dd >= prev - 1e-12, "drawdown fell from {prev} to {dd} at scale {s}");
                assert!((0.0..=1.0).contains(&dd));
                prev = dd;
            }
        }
    }

    #[test]
    fn property_fit_is_feasible_maximal_and_never_above_one() {
        let mut rng = Rng(11);
        let l = ladder();
        let mut scaled_down = 0;
        for i in 0..300 {
            let path = random_path(&mut rng, 300);
            let headroom = [0.5, 0.8, 1.0][i % 3];
            let fit = fit_scale_to_ladder(&path, &l, headroom).unwrap();
            let target = headroom * 0.10;
            assert!(fit.scale > 0.0 && fit.scale <= 1.0, "{fit:?}");
            assert!(replay_max_drawdown(&path, fit.scale) <= target + 1e-12, "infeasible: {fit:?}");
            assert_eq!(fit.replay_max_drawdown, replay_max_drawdown(&path, fit.scale));
            if fit.scale < 1.0 {
                scaled_down += 1;
                // maximal: nudging the scale up breaks the target
                assert!(replay_max_drawdown(&path, (fit.scale * (1.0 + 1e-6)).min(1.0)) > target, "not maximal: {fit:?}");
                assert!(fit.unscaled_max_drawdown > target);
            } else {
                assert!(fit.unscaled_max_drawdown <= target);
            }
        }
        assert!(scaled_down > 50, "the generator should exercise the scaling branch, got {scaled_down}");
    }

    #[test]
    fn property_inside_ladder_input_returns_exactly_one() {
        let mut rng = Rng(23);
        let l = ladder();
        for _ in 0..200 {
            // tiny volatility keeps the path well inside 0.8 * 10%
            let path: Vec<f64> = (0..200).map(|_| rng.range(-0.0015, 0.002)).collect();
            assert!(replay_max_drawdown(&path, 1.0) <= 0.08);
            let fit = fit_scale_to_ladder(&path, &l, 0.8).unwrap();
            assert_eq!(fit.scale, 1.0);
        }
    }

    #[test]
    fn property_fit_is_monotone_in_headroom_and_in_first_rung() {
        let mut rng = Rng(31);
        for _ in 0..150 {
            let path = random_path(&mut rng, 300);
            let mut prev = 0.0;
            for h in [0.2, 0.4, 0.6, 0.8, 1.0] {
                let s = fit_scale_to_ladder(&path, &ladder(), h).unwrap().scale;
                assert!(s >= prev - 1e-12, "more headroom must never shrink the scale: {prev} -> {s} at {h}");
                prev = s;
            }
            let mut prev = 0.0;
            for at in [0.05, 0.10, 0.15, 0.25] {
                let l = [LadderRung { at, action: LadderAction::HaltFlatten, scale: None }];
                let s = fit_scale_to_ladder(&path, &l, 0.8).unwrap().scale;
                assert!(s >= prev - 1e-12, "a looser first rung must never shrink the scale: {prev} -> {s} at {at}");
                prev = s;
            }
        }
    }

    #[test]
    fn property_refitting_a_fitted_series_is_a_no_op() {
        let mut rng = Rng(41);
        let l = ladder();
        for _ in 0..100 {
            let path = random_path(&mut rng, 250);
            let fit = fit_scale_to_ladder(&path, &l, 0.8).unwrap();
            let scaled: Vec<f64> = path.iter().map(|r| r * fit.scale).collect();
            let again = fit_scale_to_ladder(&scaled, &l, 0.8).unwrap();
            assert_eq!(again.scale, 1.0, "{fit:?} then {again:?}");
        }
    }

    #[test]
    fn property_inverse_vol_weights_sum_to_one() {
        let mut rng = Rng(53);
        for _ in 0..200 {
            let n = 1 + (rng.next() % 9) as usize;
            let vols: Vec<f64> = (0..n).map(|_| rng.range(0.01, 2.0)).collect();
            let w = inverse_vol_weights(&vols).unwrap();
            assert_eq!(w.len(), n);
            assert!((w.iter().sum::<f64>() - 1.0).abs() < 1e-12);
            assert!(w.iter().all(|x| *x > 0.0 && *x <= 1.0));
            // ordering: lower vol never gets a smaller weight
            for i in 0..n {
                for j in 0..n {
                    if vols[i] < vols[j] {
                        assert!(w[i] > w[j]);
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ selection log

    fn elig_map(m: &MandateBody) -> BTreeMap<String, Eligibility> {
        lib().iter().map(|e| (e.id.clone(), elig(e, m))).collect()
    }

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn good_record() -> SelectionRecord {
        SelectionRecord {
            candidates_considered: strs(&["etf_trend_faber", "fx_tsmom_12m", "crypto_trend_100d"]),
            chosen: strs(&["etf_trend_faber", "crypto_trend_100d"]),
            rejected: vec![("fx_tsmom_12m".into(), "mandate forbids shorting and leverage".into())],
            rule: "Choose every eligible entry, up to the maximum, in library order.".into(),
        }
    }

    fn issue_codes(v: &[SelectionIssue]) -> Vec<SelectionIssueCode> {
        v.iter().map(|i| i.code).collect()
    }

    #[test]
    fn a_complete_record_validates() {
        let map = elig_map(&base_mandate());
        assert_eq!(good_record().validate(&map, 4), vec![]);
    }

    #[test]
    fn choosing_an_ineligible_entry_is_rejected() {
        let map = elig_map(&base_mandate());
        let mut r = good_record();
        r.chosen.push("fx_tsmom_12m".into());
        r.rejected.clear();
        let v = r.validate(&map, 4);
        assert_eq!(issue_codes(&v), [SelectionIssueCode::ChosenIneligible]);
        assert!(v[0].message.contains("shorting"), "{}", v[0].message);
        assert_eq!(v[0].entry_id.as_deref(), Some("fx_tsmom_12m"));
    }

    #[test]
    fn choosing_a_live_blocked_but_paper_eligible_entry_is_allowed() {
        let map = elig_map(&base_mandate());
        assert!(map["etf_trend_faber"].eligible_paper && !map["etf_trend_faber"].eligible_live);
        assert!(good_record().validate(&map, 4).is_empty());
    }

    #[test]
    fn choosing_more_than_the_maximum_is_rejected() {
        let map = elig_map(&base_mandate());
        let v = good_record().validate(&map, 1);
        assert_eq!(issue_codes(&v), [SelectionIssueCode::TooManyChosen]);
        assert!(good_record().validate(&map, 2).is_empty(), "exactly the maximum is fine");
        let v = good_record().validate(&map, 0);
        assert_eq!(issue_codes(&v), [SelectionIssueCode::TooManyChosen]);
    }

    #[test]
    fn omitting_a_rejection_reason_is_rejected() {
        let map = elig_map(&base_mandate());
        // no rejected entry at all for the unchosen candidate
        let mut r = good_record();
        r.rejected.clear();
        let v = r.validate(&map, 4);
        assert_eq!(issue_codes(&v), [SelectionIssueCode::MissingRejectionReason]);
        assert_eq!(v[0].entry_id.as_deref(), Some("fx_tsmom_12m"));
        // present but blank
        let mut r = good_record();
        r.rejected = vec![("fx_tsmom_12m".into(), "   ".into())];
        assert_eq!(issue_codes(&r.validate(&map, 4)), [SelectionIssueCode::MissingRejectionReason]);
    }

    #[test]
    fn a_missing_rule_is_rejected() {
        let map = elig_map(&base_mandate());
        let mut r = good_record();
        r.rule = " ".into();
        assert_eq!(issue_codes(&r.validate(&map, 4)), [SelectionIssueCode::MissingRule]);
    }

    #[test]
    fn hidden_trials_and_inconsistent_lists_are_rejected() {
        let map = elig_map(&base_mandate());
        // an eligible entry left out of the considered list
        let mut r = good_record();
        r.candidates_considered = strs(&["etf_trend_faber", "fx_tsmom_12m"]);
        let v = r.validate(&map, 4);
        assert!(issue_codes(&v).contains(&SelectionIssueCode::EligibleNotLogged));
        assert!(issue_codes(&v).contains(&SelectionIssueCode::ChosenNotConsidered));
        // duplicates
        let mut r = good_record();
        r.candidates_considered.push("etf_trend_faber".into());
        r.chosen.push("etf_trend_faber".into());
        let c = issue_codes(&r.validate(&map, 4));
        assert!(c.contains(&SelectionIssueCode::DuplicateCandidate) && c.contains(&SelectionIssueCode::DuplicateChosen), "{c:?}");
        // chosen and rejected at once; rejected but never considered; chosen with no eligibility result
        let mut r = good_record();
        r.rejected.push(("etf_trend_faber".into(), "changed my mind".into()));
        r.rejected.push(("ghost".into(), "n/a".into()));
        let c = issue_codes(&r.validate(&map, 4));
        assert!(c.contains(&SelectionIssueCode::ChosenAndRejected) && c.contains(&SelectionIssueCode::RejectedNotConsidered), "{c:?}");
        let mut r = good_record();
        r.candidates_considered.push("mystery".into());
        r.chosen.push("mystery".into());
        assert!(issue_codes(&r.validate(&map, 4)).contains(&SelectionIssueCode::ChosenUnknown));
    }

    #[test]
    fn selection_record_round_trips_and_rejects_unknown_fields() {
        let r = good_record();
        let back: SelectionRecord = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(r, back);
        let mut v = serde_json::to_value(&r).unwrap();
        v["notes"] = json!("x");
        assert!(serde_json::from_value::<SelectionRecord>(v).is_err());
    }

    #[test]
    fn an_empty_selection_with_reasons_is_valid_when_nothing_is_eligible() {
        // a mandate that allows nothing the library trades
        let m = mandate_with(|v| {
            v["universe"]["asset_classes"] = json!(["options"]);
            v["exposure"]["max_asset_class"] = json!({});
            v["universe"]["instrument_allow"] = json!(["AAPL"]);
        });
        let map = elig_map(&m);
        assert!(map.values().all(|e| !e.eligible_paper));
        let r = SelectionRecord {
            candidates_considered: strs(&["etf_trend_faber", "fx_tsmom_12m", "crypto_trend_100d"]),
            chosen: vec![],
            rejected: strs(&["etf_trend_faber", "fx_tsmom_12m", "crypto_trend_100d"]).into_iter().map(|id| (id, "ineligible under the mandate".to_string())).collect(),
            rule: "Choose every eligible entry.".into(),
        };
        assert_eq!(r.validate(&map, 4), vec![]);
    }

    // -----------------------------------------------------------------
    // WP9.7-9.9 intake gate (IMPLEMENTATION_PLAN.md)
    // -----------------------------------------------------------------

    #[test]
    fn evidence_strength_etf_passes_at_its_own_published_floor() {
        let r = evidence_strength_check(&entry("etf_trend_faber"));
        assert!(r.passes, "{}", r.reason);
    }

    #[test]
    fn evidence_strength_fx_fails_integrity_bound_only() {
        let r = evidence_strength_check(&entry("fx_tsmom_12m"));
        assert!(!r.passes);
        assert!(r.reason.contains("integrity bound"), "{}", r.reason);
    }

    #[test]
    fn evidence_strength_crypto_fails_no_low_bound() {
        // crypto's own performance_band was explicitly withdrawn (low/high
        // both null) -- a different failure reason than fx's, but the same
        // "does not auto-clear" outcome.
        let r = evidence_strength_check(&entry("crypto_trend_100d"));
        assert!(!r.passes);
        assert!(r.reason.contains("no low bound"), "{}", r.reason);
    }

    #[test]
    fn representativeness_etf_passes_on_the_benchmarks_real_drawdown() {
        // etf_trend_faber's OWN drawdown was only 8.97% -- that's the rule
        // successfully ducking out of COVID/2022, not evidence the window
        // lacked stress. The benchmark's 26.68% is what proves it.
        let r = representativeness_check(&entry("etf_trend_faber"));
        assert!(r.passes, "{}", r.reason);
    }

    #[test]
    fn representativeness_fx_passes_length_and_drawdown() {
        let r = representativeness_check(&entry("fx_tsmom_12m"));
        assert!(r.passes, "{}", r.reason);
    }

    #[test]
    fn representativeness_crypto_fails_by_a_hair_on_window_length() {
        // A real, precise finding (2026-09-23): crypto_trend_100d's real
        // window (2016-01-01 to 2020-12-31) is 4.9993 years by exact day
        // count -- fractionally under the 5-year minimum. Not the
        // "bull-market, no real drawdown" failure mode this check mainly
        // exists to catch (its 50.82% drawdown clears that easily) -- a
        // separate, narrower gap. Worth fixing on the real entry (extend
        // the window by a few weeks of real data) rather than lowering the
        // threshold to fit this one case.
        let r = representativeness_check(&entry("crypto_trend_100d"));
        assert!(!r.passes, "expected a real 4.9993-year window to fail the 5.0-year minimum");
        assert!(r.reason.contains("below the"), "{}", r.reason);
    }

    #[test]
    fn representativeness_check_rejects_a_short_window_regardless_of_drawdown() {
        let mut e = entry("etf_trend_faber");
        e.backtest_window = BacktestWindow {
            start: "2024-01-01".into(),
            end: "2024-06-01".into(),
            max_observed_drawdown: Some(0.5), // severe, but the window itself is too short
        };
        let r = representativeness_check(&e);
        assert!(!r.passes);
        assert!(r.reason.contains("below the"), "{}", r.reason);
    }

    #[test]
    fn representativeness_check_rejects_a_long_window_with_no_real_drawdown() {
        let mut e = entry("etf_trend_faber");
        e.backtest_window = BacktestWindow {
            start: "2010-01-01".into(),
            end: "2025-01-01".into(),
            max_observed_drawdown: Some(0.03), // long window, but a suspiciously calm one
        };
        let r = representativeness_check(&e);
        assert!(!r.passes);
        assert!(r.reason.contains("likely tested only"), "{}", r.reason);
    }

    #[test]
    fn disclosure_etf_passes_with_its_real_known_deviation() {
        let caps = current_platform_capabilities();
        let r = disclosure_check(&entry("etf_trend_faber"), &caps);
        assert!(r.passes, "{}", r.reason);
    }

    #[test]
    fn disclosure_check_rejects_an_approximation_with_no_disclosure() {
        let caps = current_platform_capabilities();
        let mut e = entry("etf_trend_faber");
        e.known_deviations = vec![]; // strip the real disclosure this entry actually has
        let r = disclosure_check(&e, &caps);
        assert!(!r.passes, "an undisclosed approximation must not pass");
        assert!(r.reason.contains("must be disclosed"), "{}", r.reason);
    }

    #[test]
    fn disclosure_check_passes_a_rule_with_no_approximation_to_disclose() {
        // crypto_trend_100d's ONLY platform gap is the same sub-account drift
        // as etf's; give it a mandate/platform combo where that's already
        // disclosed, confirming the pass path isn't vacuous.
        let caps = current_platform_capabilities();
        let r = disclosure_check(&entry("crypto_trend_100d"), &caps);
        assert!(r.passes, "{}", r.reason);
    }

    // -----------------------------------------------------------------
    // WP9.2 automated replication checks (IMPLEMENTATION_PLAN.md)
    // -----------------------------------------------------------------

    #[test]
    fn signal_agreement_identical_series_is_100_percent() {
        let a = vec![None, Some(0.2), Some(0.2), None];
        let r = signal_agreement(&a, &a);
        assert!(r.passes, "{}", r.reason);
        assert!(r.reason.contains("4/4"), "{}", r.reason);
    }

    #[test]
    fn signal_agreement_below_floor_fails() {
        // 1 of 4 bars differs -- 75%, below the 98% floor.
        let a = vec![None, Some(0.2), Some(0.2), None];
        let b = vec![None, Some(0.2), Some(0.5), None];
        let r = signal_agreement(&a, &b);
        assert!(!r.passes);
        assert!(r.reason.contains("3/4"), "{}", r.reason);
    }

    #[test]
    fn signal_agreement_rejects_mismatched_lengths() {
        let r = signal_agreement(&[None, Some(0.1)], &[None]);
        assert!(!r.passes);
        assert!(r.reason.contains("different lengths"), "{}", r.reason);
    }

    #[test]
    fn causality_by_truncation_passes_a_genuinely_causal_signal() {
        // A real trailing-average rule: only ever looks at prices[..=t].
        let prices = vec![1.0, 2.0, 3.0, 2.5, 4.0, 3.5];
        let causal = |series: &[f64], t: usize| -> Option<f64> {
            let window = &series[..=t.min(series.len() - 1)];
            let avg = window.iter().sum::<f64>() / window.len() as f64;
            Some(avg)
        };
        let r = causality_by_truncation_check(&prices, causal);
        assert!(r.passes, "{}", r.reason);
    }

    #[test]
    fn causality_by_truncation_catches_a_look_ahead_bug() {
        // A buggy rule that peeks one bar ahead when it's available -- exactly
        // the class of defect this check exists to catch.
        let prices = vec![1.0, 2.0, 3.0, 2.5, 4.0, 3.5];
        let peeks_ahead = |series: &[f64], t: usize| -> Option<f64> {
            if t + 1 < series.len() {
                Some(series[t + 1]) // uses tomorrow's price -- a real look-ahead
            } else {
                Some(series[t])
            }
        };
        let r = causality_by_truncation_check(&prices, peeks_ahead);
        assert!(!r.passes, "a look-ahead bug must be caught, not passed");
        assert!(r.reason.contains("look-ahead"), "{}", r.reason);
    }

    #[test]
    fn explicit_sizing_all_three_real_entries_pass() {
        for id in ["etf_trend_faber", "fx_tsmom_12m", "crypto_trend_100d"] {
            let r = explicit_sizing_check(&entry(id));
            assert!(r.passes, "{id}: {}", r.reason);
        }
    }

    #[test]
    fn explicit_sizing_check_rejects_no_sizing_parameter_at_all() {
        let mut e = entry("etf_trend_faber");
        e.parameters = BTreeMap::from([("sma_month_ends".to_string(), json!(10))]);
        let r = explicit_sizing_check(&e);
        assert!(!r.passes);
        assert!(r.reason.contains("no sizing-shaped key"), "{}", r.reason);
    }

    #[test]
    fn gross_and_net_etf_fails_net_only() {
        // etf_trend_faber's real metric is "sharpe_net" -- no gross figure at all.
        let r = gross_and_net_reported_check(&entry("etf_trend_faber"));
        assert!(!r.passes);
        assert!(r.reason.contains("only a net metric"), "{}", r.reason);
    }

    #[test]
    fn gross_and_net_crypto_fails_gross_only() {
        // crypto_trend_100d's real metric is "sharpe_gross" -- no net figure.
        let r = gross_and_net_reported_check(&entry("crypto_trend_100d"));
        assert!(!r.passes);
        assert!(r.reason.contains("only a gross metric"), "{}", r.reason);
    }

    #[test]
    fn gross_and_net_fx_fails_unlabeled() {
        // fx_tsmom_12m's real metrics are "sharpe" and "realised_annual_volatility"
        // -- neither is labeled gross or net at all.
        let r = gross_and_net_reported_check(&entry("fx_tsmom_12m"));
        assert!(!r.passes);
        assert!(r.reason.contains("ambiguous"), "{}", r.reason);
    }

    #[test]
    fn gross_and_net_reported_check_passes_when_both_present() {
        let mut e = entry("etf_trend_faber");
        e.expected_ranges.push(ExpectedRange {
            metric: "sharpe_gross".to_string(),
            low: Some(0.3),
            high: Some(1.1),
            role: RangeRole::PerformanceBand,
            source: "test fixture".to_string(),
            note: "test fixture".to_string(),
        });
        let r = gross_and_net_reported_check(&e);
        assert!(r.passes, "{}", r.reason);
    }

    #[test]
    fn falls_inside_expected_range_etf_real_number_passes() {
        // etf_trend_faber's own real reimplementation Sharpe was 0.645, inside its [0.2, 1.0] band.
        let r = falls_inside_expected_range(&entry("etf_trend_faber"), "sharpe_net", 0.645);
        assert!(r.is_some());
        assert!(r.unwrap().passes);
    }

    #[test]
    fn falls_inside_expected_range_flags_an_out_of_band_result() {
        let r = falls_inside_expected_range(&entry("etf_trend_faber"), "sharpe_net", 5.0);
        let r = r.expect("range exists");
        assert!(!r.passes);
        assert!(r.reason.contains("OUTSIDE"), "{}", r.reason);
    }

    #[test]
    fn falls_inside_expected_range_is_none_for_an_unlisted_metric() {
        assert!(falls_inside_expected_range(&entry("etf_trend_faber"), "not_a_real_metric", 1.0).is_none());
    }

    // -----------------------------------------------------------------
    // WP9.4 evidence-label state machine
    // -----------------------------------------------------------------

    #[test]
    fn evidence_transition_allows_the_forward_path_one_step_at_a_time() {
        use Evidence::*;
        assert!(evidence_transition_allowed(Unverified, ReferenceOnly));
    }

    /// T4, stage T4b (design section 2.4): UPDATED from T4a's behaviour, per the plan's explicit instruction --
    /// "the cause-less form now returns false for `(ReferenceOnly, ReplicatedOnPlatform)` and the one existing test
    /// that asserts the old behaviour ... is updated in the same PR." Promotion is now reachable ONLY through
    /// [`evidence_transition_allowed_with`] with a genuine [`TransitionCause::VerifiedReplication`] -- see the tests
    /// below. Mutant target: "the cause-less form still allows `(ReferenceOnly, ReplicatedOnPlatform)`" (this WOULD
    /// let a curator promote an entry by editing declared JSON alone, exactly what T4 exists to prevent).
    #[test]
    fn evidence_transition_no_longer_allows_reference_only_to_replicated_without_a_cause() {
        assert!(!evidence_transition_allowed(Evidence::ReferenceOnly, Evidence::ReplicatedOnPlatform));
    }

    #[test]
    fn evidence_transition_rejects_skipping_straight_to_replicated() {
        assert!(!evidence_transition_allowed(Evidence::Unverified, Evidence::ReplicatedOnPlatform));
    }

    /// T20: `evidence_transition_allowed_with` allows `reference_only -> replicated_on_platform` ONLY under
    /// `TransitionCause::VerifiedReplication`, and is identical to the cause-less form for every other pair.
    #[test]
    fn evidence_transition_allowed_with_gates_promotion_on_a_genuine_cause() {
        use Evidence::*;
        assert!(evidence_transition_allowed_with(ReferenceOnly, ReplicatedOnPlatform, TransitionCause::VerifiedReplication));
        for cause in [TransitionCause::Curation, TransitionCause::Decay, TransitionCause::CapabilityChange] {
            assert!(
                !evidence_transition_allowed_with(ReferenceOnly, ReplicatedOnPlatform, cause),
                "{cause:?} must not unlock promotion"
            );
        }
        // Every other pair matches the cause-less form regardless of cause.
        for (from, to) in [
            (Unverified, ReferenceOnly),
            (ReplicatedOnPlatform, ReferenceOnly),
            (Unverified, PlatformCapabilityBlocked),
            (PlatformCapabilityBlocked, ReferenceOnly),
        ] {
            for cause in [TransitionCause::Curation, TransitionCause::VerifiedReplication, TransitionCause::Decay, TransitionCause::CapabilityChange] {
                assert_eq!(evidence_transition_allowed_with(from, to, cause), evidence_transition_allowed(from, to), "{from:?}->{to:?} under {cause:?}");
            }
        }
    }

    #[test]
    fn evidence_transition_allows_decay_demotion_but_not_a_bigger_backward_jump() {
        assert!(evidence_transition_allowed(Evidence::ReplicatedOnPlatform, Evidence::ReferenceOnly));
        assert!(!evidence_transition_allowed(Evidence::ReplicatedOnPlatform, Evidence::Unverified));
    }

    // ---------------------------------------------------- L1: agent_verified state-machine tests

    /// AUTONOMOUS_LIBRARY_CURATION_PLAN.md section 2.2 / section 4's test plan: exhaustively mirrors
    /// `evidence_transition_allowed_with_gates_promotion_on_a_genuine_cause` for the sibling label --
    /// `(ReferenceOnly, AgentVerified, AgentVerifiedReplication)` is the ONLY triple that succeeds for that pair,
    /// including the "wrong cause" case `(ReferenceOnly, AgentVerified, VerifiedReplication)`, the mirror image of
    /// the existing T4b test for the human path.
    #[test]
    fn evidence_transition_allowed_with_gates_agent_verified_promotion_on_a_genuine_cause() {
        use Evidence::*;
        assert!(evidence_transition_allowed_with(ReferenceOnly, AgentVerified, TransitionCause::AgentVerifiedReplication));
        for cause in [
            TransitionCause::Curation,
            TransitionCause::VerifiedReplication, // the wrong cause -- must not unlock this promotion either
            TransitionCause::Decay,
            TransitionCause::CapabilityChange,
        ] {
            assert!(
                !evidence_transition_allowed_with(ReferenceOnly, AgentVerified, cause),
                "{cause:?} must not unlock agent_verified promotion"
            );
        }
        // And the reverse: AgentVerifiedReplication must not unlock the HUMAN promotion either -- the two causes
        // are never interchangeable.
        assert!(!evidence_transition_allowed_with(ReferenceOnly, ReplicatedOnPlatform, TransitionCause::AgentVerifiedReplication));
    }

    /// Every `(from, to, cause)` triple involving `AgentVerified` that ISN'T the one promotion above matches the
    /// cause-less form regardless of cause -- the same exhaustive-equivalence style as
    /// `evidence_transition_allowed_with_gates_promotion_on_a_genuine_cause`.
    #[test]
    fn evidence_transition_allowed_with_agent_verified_other_pairs_match_causeless_form() {
        use Evidence::*;
        for (from, to) in [
            (AgentVerified, ReferenceOnly),
            (AgentVerified, Unverified),
            (AgentVerified, PlatformCapabilityBlocked),
            (AgentVerified, ReplicatedOnPlatform),
            (ReplicatedOnPlatform, AgentVerified),
            (Unverified, AgentVerified),
            (PlatformCapabilityBlocked, AgentVerified),
        ] {
            for cause in [
                TransitionCause::Curation,
                TransitionCause::VerifiedReplication,
                TransitionCause::AgentVerifiedReplication,
                TransitionCause::Decay,
                TransitionCause::CapabilityChange,
            ] {
                assert_eq!(
                    evidence_transition_allowed_with(from, to, cause),
                    evidence_transition_allowed(from, to),
                    "{from:?}->{to:?} under {cause:?}"
                );
            }
        }
    }

    /// The demotion mirrors `ReplicatedOnPlatform`'s exactly, and -- the load-bearing negative assertion --
    /// `AgentVerified` and `ReplicatedOnPlatform` are NEVER directly reachable from each other in either direction,
    /// under the cause-less form.
    #[test]
    fn evidence_transition_allows_agent_verified_demotion_but_never_a_direct_edge_to_or_from_replicated_on_platform() {
        assert!(evidence_transition_allowed(Evidence::AgentVerified, Evidence::ReferenceOnly));
        assert!(!evidence_transition_allowed(Evidence::AgentVerified, Evidence::Unverified));
        assert!(!evidence_transition_allowed(Evidence::AgentVerified, Evidence::ReplicatedOnPlatform));
        assert!(!evidence_transition_allowed(Evidence::ReplicatedOnPlatform, Evidence::AgentVerified));
    }

    #[test]
    fn evidence_transition_platform_blocked_reenters_at_unverified_not_a_verified_state() {
        assert!(evidence_transition_allowed(Evidence::Unverified, Evidence::PlatformCapabilityBlocked));
        assert!(evidence_transition_allowed(Evidence::PlatformCapabilityBlocked, Evidence::Unverified));
        assert!(!evidence_transition_allowed(Evidence::PlatformCapabilityBlocked, Evidence::ReferenceOnly));
        assert!(!evidence_transition_allowed(Evidence::PlatformCapabilityBlocked, Evidence::ReplicatedOnPlatform));
    }

    #[test]
    fn evidence_transition_staying_put_is_always_allowed() {
        for e in [
            Evidence::Unverified,
            Evidence::ReferenceOnly,
            Evidence::ReplicatedOnPlatform,
            Evidence::AgentVerified,
            Evidence::PlatformCapabilityBlocked,
        ] {
            assert!(evidence_transition_allowed(e, e));
        }
    }

    // -----------------------------------------------------------------
    // Intake gate (aggregate)
    // -----------------------------------------------------------------

    /// Two identical (fully-agreeing) synthetic signal series, paired with a
    /// measured metric/value -- a stand-in for "two independent
    /// implementations that agree," which this test module has no real
    /// engine to produce.
    fn agreeing_replication(metric: &str, value: f64) -> ReplicationEvidence {
        let signal = vec![Some(0.2); 20];
        ReplicationEvidence { signal_a: signal.clone(), signal_b: signal, measured_metric: metric.to_string(), measured_value: value }
    }

    #[test]
    fn intake_gate_etf_earns_reference_only_except_for_the_known_gross_net_gap() {
        let caps = current_platform_capabilities();
        let e = entry("etf_trend_faber");
        let evidence = agreeing_replication("sharpe_net", 0.645);
        let verdict = intake_gate(&e, &caps, &evidence, None);
        // etf_trend_faber passes everything EXCEPT gross_and_net (it only
        // reports sharpe_net) -- so it does NOT earn reference_only yet, and
        // that failure is exactly, only, the gross/net one.
        assert_eq!(verdict.earned_evidence, Evidence::Unverified);
        let failures: Vec<&str> = verdict.failures().map(|(name, _)| name).collect();
        assert_eq!(failures, vec!["9.2_gross_and_net_reported"], "{:?}", verdict.checks);
    }

    #[test]
    fn intake_gate_fx_is_platform_capability_blocked() {
        // fx_tsmom_12m needs joint vol-targeting and cross-asset rebalance the
        // platform doesn't have -- this must dominate the verdict regardless
        // of how its other checks come out.
        let caps = current_platform_capabilities();
        let e = entry("fx_tsmom_12m");
        let evidence = agreeing_replication("sharpe", -0.064);
        let verdict = intake_gate(&e, &caps, &evidence, None);
        assert_eq!(verdict.earned_evidence, Evidence::PlatformCapabilityBlocked);
    }

    #[test]
    fn intake_gate_a_fully_clean_synthetic_candidate_earns_reference_only() {
        // Take etf_trend_faber and fix the one real gap (add a matching
        // _gross metric) to confirm the pass path genuinely reaches
        // reference_only, not just that failures are detected.
        let caps = current_platform_capabilities();
        let mut e = entry("etf_trend_faber");
        e.expected_ranges.push(ExpectedRange {
            metric: "sharpe_gross".to_string(),
            low: Some(0.3),
            high: Some(1.1),
            role: RangeRole::PerformanceBand,
            source: "test fixture".to_string(),
            note: "test fixture".to_string(),
        });
        let evidence = agreeing_replication("sharpe_net", 0.645);
        let verdict = intake_gate(&e, &caps, &evidence, None);
        assert_eq!(verdict.earned_evidence, Evidence::ReferenceOnly, "{:?}", verdict.checks);
        assert_eq!(verdict.failures().count(), 0);
    }

    #[test]
    fn intake_gate_etf_with_real_cross_repo_replication_evidence_earns_reference_only() {
        // Unlike every other test in this file, the 100%-agreement signal
        // here is NOT synthetic -- it transcribes a real, verified result
        // (see ReplicationEvidence's doc comment): SignalEngine's
        // reference-rules crate's golden test, run for real on 2026-09-23,
        // `S1 ETF trend (explicit mode): 555/555 signals agree over 111
        // month-ends`. 555 identical values stands in for "555 matched
        // decisions, all agreeing" -- signal_agreement() only needs to know
        // they matched, not each decision's actual value, so this is an
        // honest, minimal encoding of that real result, not a shortcut
        // around measuring it.
        let caps = current_platform_capabilities();
        let mut e = entry("etf_trend_faber");
        e.expected_ranges.push(ExpectedRange {
            metric: "sharpe_gross".to_string(),
            low: Some(0.3),
            high: Some(1.1),
            role: RangeRole::PerformanceBand,
            source: "test fixture (the real gap this entry still needs closed, separate from replication)".to_string(),
            note: "test fixture".to_string(),
        });
        let real_signal = vec![Some(1.0); 555];
        let evidence = ReplicationEvidence {
            signal_a: real_signal.clone(),
            signal_b: real_signal,
            measured_metric: "sharpe_net".to_string(),
            measured_value: 0.645, // the entry's own real independent-reimplementation figure
        };
        let verdict = intake_gate(&e, &caps, &evidence, None);
        assert_eq!(verdict.earned_evidence, Evidence::ReferenceOnly, "{:?}", verdict.checks);
        let signal_check = verdict.checks.iter().find(|(name, _)| *name == "9.2_signal_agreement").unwrap();
        assert!(signal_check.1.reason.contains("555/555"), "{}", signal_check.1.reason);
    }

    #[test]
    fn intake_gate_disagreeing_implementations_block_earning_reference_only() {
        let caps = current_platform_capabilities();
        let mut e = entry("etf_trend_faber");
        e.expected_ranges.push(ExpectedRange {
            metric: "sharpe_gross".to_string(),
            low: Some(0.3),
            high: Some(1.1),
            role: RangeRole::PerformanceBand,
            source: "test fixture".to_string(),
            note: "test fixture".to_string(),
        });
        let evidence = ReplicationEvidence {
            signal_a: vec![Some(0.2); 20],
            signal_b: vec![Some(0.9); 20], // wildly different -- a real disagreement
            measured_metric: "sharpe_net".to_string(),
            measured_value: 0.645,
        };
        let verdict = intake_gate(&e, &caps, &evidence, None);
        assert_eq!(verdict.earned_evidence, Evidence::Unverified);
        assert!(verdict.failures().any(|(name, _)| name == "9.2_signal_agreement"));
    }

    // -----------------------------------------------------------------
    // WP8.5/9.5 calibration harness
    // -----------------------------------------------------------------

    /// The known-answer set: the 3 real entries (their correct answer
    /// established by hand in the tests above) plus deliberately corrupted
    /// copies of each, one corruption per case, so a failure here points at
    /// exactly which check's accuracy is in question. This is a SAMPLE known-
    /// answer set for measuring the checker itself -- not the production
    /// candidate list (which is empty; see `SEED_JSON`'s doc comment).
    fn known_answer_set() -> Vec<CalibrationCase> {
        let mut cases = Vec::new();

        // --- etf_trend_faber: real entry, one real gap (gross/net) away from ReferenceOnly.
        cases.push(CalibrationCase {
            name: "etf_real_as_is".to_string(),
            entry: entry("etf_trend_faber"),
            replication: agreeing_replication("sharpe_net", 0.645),
            expected_evidence: Evidence::Unverified, // fails 9.2_gross_and_net_reported today
        });
        {
            let mut e = entry("etf_trend_faber");
            e.expected_ranges.push(ExpectedRange {
                metric: "sharpe_gross".to_string(),
                low: Some(0.3),
                high: Some(1.1),
                role: RangeRole::PerformanceBand,
                source: "test fixture".to_string(),
                note: "test fixture".to_string(),
            });
            cases.push(CalibrationCase {
                name: "etf_with_gross_metric_added".to_string(),
                entry: e,
                replication: agreeing_replication("sharpe_net", 0.645),
                expected_evidence: Evidence::ReferenceOnly, // now genuinely clean
            });
        }
        {
            let mut e = entry("etf_trend_faber");
            e.expected_ranges.push(ExpectedRange {
                metric: "sharpe_gross".to_string(),
                low: Some(0.3),
                high: Some(1.1),
                role: RangeRole::PerformanceBand,
                source: "test fixture".to_string(),
                note: "test fixture".to_string(),
            });
            let bad_replication = ReplicationEvidence {
                signal_a: vec![Some(0.2); 20],
                signal_b: vec![Some(0.9); 20], // disagreeing implementations
                measured_metric: "sharpe_net".to_string(),
                measured_value: 0.645,
            };
            cases.push(CalibrationCase {
                name: "etf_with_disagreeing_implementations".to_string(),
                entry: e,
                replication: bad_replication,
                expected_evidence: Evidence::Unverified, // must be caught, not passed
            });
        }
        {
            let mut e = entry("etf_trend_faber");
            e.known_deviations = vec![]; // strip its real, disclosed deviation
            cases.push(CalibrationCase {
                name: "etf_with_undisclosed_approximation".to_string(),
                entry: e,
                replication: agreeing_replication("sharpe_net", 0.645),
                expected_evidence: Evidence::Unverified, // must be caught, not passed
            });
        }

        // --- fx_tsmom_12m: real entry, platform-blocked.
        cases.push(CalibrationCase {
            name: "fx_real_as_is".to_string(),
            entry: entry("fx_tsmom_12m"),
            replication: agreeing_replication("sharpe", -0.064),
            expected_evidence: Evidence::PlatformCapabilityBlocked,
        });

        // --- crypto_trend_100d: real entry, no low bound set.
        cases.push(CalibrationCase {
            name: "crypto_real_as_is".to_string(),
            entry: entry("crypto_trend_100d"),
            replication: agreeing_replication("sharpe_gross", 2.10),
            expected_evidence: Evidence::Unverified,
        });
        {
            let mut e = entry("crypto_trend_100d");
            e.expected_ranges[0].low = Some(0.5); // give it a real floor
            e.expected_ranges.push(ExpectedRange {
                metric: "sharpe_net".to_string(),
                low: Some(0.4),
                high: Some(2.0),
                role: RangeRole::PerformanceBand,
                source: "test fixture".to_string(),
                note: "test fixture".to_string(),
            });
            // Also extend the window: crypto's REAL window (2016-01-01 to
            // 2020-12-31) is 4.9993 years by exact day count -- a hair
            // under the 5-year minimum, a separate real finding (see the
            // representativeness tests). Extending it here isolates what
            // THIS case is actually testing (the evidence-strength fix)
            // from that unrelated, already-covered gap.
            e.backtest_window.end = "2021-06-01".to_string();
            cases.push(CalibrationCase {
                name: "crypto_with_a_real_band_and_net_metric".to_string(),
                entry: e,
                replication: agreeing_replication("sharpe_gross", 2.10),
                expected_evidence: Evidence::ReferenceOnly,
            });
        }

        // --- A representativeness-specific failure, isolated from every other check
        // (not yet covered by calibration -- only unit-tested on its own until now).
        {
            let mut e = entry("etf_trend_faber");
            e.expected_ranges.push(ExpectedRange {
                metric: "sharpe_gross".to_string(),
                low: Some(0.3),
                high: Some(1.1),
                role: RangeRole::PerformanceBand,
                source: "test fixture".to_string(),
                note: "test fixture".to_string(),
            });
            e.backtest_window = BacktestWindow {
                start: "2024-01-01".to_string(),
                end: "2024-06-01".to_string(),
                max_observed_drawdown: Some(0.5), // severe, but the window itself is too short
            };
            cases.push(CalibrationCase {
                name: "etf_with_too_short_a_window".to_string(),
                entry: e,
                replication: agreeing_replication("sharpe_net", 0.645),
                expected_evidence: Evidence::Unverified,
            });
        }

        // --- etf_trend_faber with REAL cross-repo replication evidence (not synthetic
        // -- see ReplicationEvidence's doc comment and the golden test it cites), folded
        // into the formal known-answer set rather than left as a one-off test.
        {
            let mut e = entry("etf_trend_faber");
            e.expected_ranges.push(ExpectedRange {
                metric: "sharpe_gross".to_string(),
                low: Some(0.3),
                high: Some(1.1),
                role: RangeRole::PerformanceBand,
                source: "test fixture (the real gap this entry still needs closed, separate from replication)".to_string(),
                note: "test fixture".to_string(),
            });
            let real_signal = vec![Some(1.0); 555];
            cases.push(CalibrationCase {
                name: "etf_with_real_golden_test_replication_evidence".to_string(),
                entry: e,
                replication: ReplicationEvidence {
                    signal_a: real_signal.clone(),
                    signal_b: real_signal,
                    measured_metric: "sharpe_net".to_string(),
                    measured_value: 0.645,
                },
                expected_evidence: Evidence::ReferenceOnly,
            });
        }

        cases
    }

    #[test]
    fn calibration_known_answer_set_measures_a_real_pass_rate() {
        let caps = current_platform_capabilities();
        let cases = known_answer_set();
        let report = run_calibration(&cases, &caps);
        assert_eq!(report.total, 9);
        // The whole point: every corrupted case must be CAUGHT (a false
        // accept would mean the gate can be fooled), so this must be 100%
        // on this known-answer set before the gate earns any trust at all --
        // WP9.5's autonomy unlock still needs a MUCH larger set than 9
        // before that trust is real, this just proves the measurement
        // itself works and the gate isn't trivially foolable.
        assert_eq!(report.false_accepts, Vec::<String>::new(), "{:?}", report);
        assert_eq!(report.correct, 9, "{:?}", report);
        assert_eq!(report.pass_rate(), 1.0);
        let (lo, _hi) = report.confidence_interval();
        // 9/9 with n=9 still has a wide interval -- exactly why WP9.5 needs
        // a much bigger known-answer set before trusting this for real.
        assert!(lo < 1.0, "a 7-case sample must not claim certainty");
    }

    #[test]
    fn wilson_95_matches_known_reference_values() {
        // Same reference values as meta_portfolio_service::placebo_suite's
        // own test for the same formula.
        let (lo, hi) = wilson_95(0, 400);
        assert!(lo < 0.001 && hi < 0.02, "{lo} {hi}");
        let (lo, hi) = wilson_95(20, 400);
        assert!((0.03..0.08).contains(&lo), "{lo}");
        assert!((0.07..0.12).contains(&hi), "{hi}");
    }
}
