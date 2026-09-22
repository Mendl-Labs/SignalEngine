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
//! Only `crate::mandate` and plain dependencies are used, so the module
//! compiles inside the Engine and in a small isolated test crate alike.

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::mandate::{LadderRung, MandateBody};

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
    /// A reference implementation exists elsewhere; the platform has not
    /// replicated it.
    ReferenceOnly,
    /// No independent implementation has been checked.
    Unverified,
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
    /// The exact, unambiguous rule.
    pub rule_text: String,
    /// Where the reference implementation lives and its pin.
    pub reference_implementation: String,
    /// Instruments in the mandate's naming (e.g. `SPY`, `BTC/USD`). ALL are
    /// required by the published rule.
    pub instruments: Vec<String>,
    pub requirements: Requirements,
    pub rebalance: Rebalance,
    pub parameters: BTreeMap<String, serde_json::Value>,
    /// The first entry is the primary one.
    pub expected_ranges: Vec<ExpectedRange>,
    pub failure_regimes: Vec<String>,
    pub cost_profile: CostProfile,
    pub evidence: Evidence,
    /// `YYYY-MM-DD`: when the rule text was last checked against the reference.
    pub last_verified: String,
    /// What exactly was and was not verified on that date.
    pub verification_note: String,
    pub retirement_rule: String,
}

/// SHA-256 (hex) of the canonical serialization, so a proposal can record which
/// exact entry version it used.
pub fn entry_hash(e: &LibraryEntry) -> String {
    let bytes = serde_json::to_vec(e).expect("LibraryEntry always serializes");
    hex::encode(Sha256::digest(bytes))
}

/// Sanity-check a curated entry; empty result = well-formed. This checks the
/// SHAPE of the record (curation slips), not the truth of its numbers.
pub fn validate_entry(e: &LibraryEntry) -> Vec<String> {
    let mut v = Vec::new();
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
    v
}

// ---------------------------------------------------------------------------
// Seed entries (embedded JSON; facts only from the stage-1 record files)
// ---------------------------------------------------------------------------

const SEED_JSON: [(&str, &str); 3] = [
    ("etf_trend_faber", include_str!("../data/strategy_library/etf_trend_faber.json")),
    ("fx_tsmom_12m", include_str!("../data/strategy_library/fx_tsmom_12m.json")),
    ("crypto_trend_100d", include_str!("../data/strategy_library/crypto_trend_100d.json")),
];

/// Parse the embedded seed entries. Fails (rather than panics) if an embedded
/// file is malformed or its id disagrees with its file name.
pub fn seed_library() -> Result<Vec<LibraryEntry>, String> {
    SEED_JSON
        .iter()
        .map(|(name, json)| {
            let e: LibraryEntry = serde_json::from_str(json).map_err(|err| format!("{name}: {err}"))?;
            if e.id != *name {
                return Err(format!("{name}: id {:?} does not match its file name", e.id));
            }
            Ok(e)
        })
        .collect()
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
fn norm_symbol(s: &str) -> String {
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
    use crate::mandate::{self, LadderAction};
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

    fn lib() -> Vec<LibraryEntry> {
        seed_library().expect("seed library parses")
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

    #[test]
    fn every_seed_is_reference_only_with_a_caveat_about_the_literature() {
        for e in lib() {
            assert_eq!(e.evidence, Evidence::ReferenceOnly, "{}", e.id);
            assert_eq!(e.version, 1);
            assert_eq!(e.last_verified, "2026-09-21");
            assert!(e.verification_note.contains("NOT replicated"), "{}", e.id);
            assert!(e.verification_note.contains("UNVERIFIED"), "{}", e.id);
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
}
