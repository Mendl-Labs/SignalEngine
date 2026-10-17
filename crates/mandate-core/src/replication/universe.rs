//! R41 slice 1 (quant council ruling R41 / R41-A): a hashed, pre-registered UNIVERSE a certified rule may be
//! evaluated on. A rule is certified once; a [`Universe`] says where else that rule is asked to prove itself.
//!
//! The record is its own git sidecar (`program/data/strategy_library/universes/<id>_v<n>.universe.json`), never a
//! field on `LibraryEntry` (so `entry_hash` is untouched). [`universe_hash`] is the SHA-256 of the canonical JSON
//! ([`crate::replication::canon`]) of the whole file, `ruling` and `preregistered_at` included, exactly like
//! `envelope::envelope_hash`: the hash pins the file as registered, and git history is the pre-registration
//! timestamp.
//!
//! Ships INERT: [`EMBEDDED_UNIVERSE_JSON`] is empty and no `*.universe.json` is committed, so [`load_universes`]
//! returns an empty map unless [`UNIVERSE_DIR_ENV`] names a directory. Nothing here is consulted by
//! `replication::admission`/`evidence`/`binding` (the label path), by the proposal builder, or by any route.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::citation_verification::robust_premia::AssetClass;
use crate::replication::canon::canonical_hash;

/// The ruling every universe must cite.
pub const UNIVERSE_RULING: &str = "R41";
/// Sidecar suffix: `universes/<id>_v<n>.universe.json`.
pub const UNIVERSE_FILE_SUFFIX: &str = ".universe.json";
/// Directory (of `*.universe.json`) loaded IN ADDITION to the embedded list; embedded wins a collision.
pub const UNIVERSE_DIR_ENV: &str = "STRATEGY_LIBRARY_UNIVERSE_DIR";
/// R41-A(4): the mechanical slot-fill rule. A slot filled by it must carry AUM evidence and the universe must
/// carry the sha256 of the rule's hashed mapping table.
pub const SLOT_FILL_RULE_ID: &str = "ETF-SLOT-LARGEST-AUM-v1";
/// R41 floors: 20-day average dollar volume must be at least this (USD) ...
pub const MIN_ADV_20D_USD: u64 = 10_000_000;
/// ... and the spread ceiling in basis points is at most this.
pub const MAX_SPREAD_BP: u32 = 5;
/// R41-A(1): a universe a trend cell (EqualOverN) runs on needs at least this many instruments, so 1/N <= 0.20.
pub const TREND_MIN_N: usize = 5;
/// The only reconstitution value slice 1 accepts: a fixed ETF set that never changes membership.
pub const RECONSTITUTION_NONE: &str = "none";

/// Embedded sidecars (`include_str!`, same pattern as `envelope::EMBEDDED_ENVELOPE_JSON`). EMPTY by design.
const EMBEDDED_UNIVERSE_JSON: &[(&str, &str)] = &[];

/// AUM evidence for one slot filled by [`SLOT_FILL_RULE_ID`] (R41-A(4)): issuer net assets cross-checked against
/// the vendor figure, as of the last NYSE trading day of the month before the universe commit. Whole USD, so the
/// canonical hash never depends on float printing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AumEvidence {
    /// `YYYY-MM-DD`.
    pub as_of: String,
    pub issuer_net_assets_usd: u64,
    pub vendor_net_assets_usd: u64,
    /// `YYYY-MM-DD`; the rule requires inception on or before the data window start (checked by the data
    /// certificate, not here).
    pub inception: String,
}

/// One slot of a universe: an asset-class slot, the rule that filled it, the resolved ticker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Slot {
    /// The asset-class slot label (e.g. `"us_equity_large"`).
    pub asset_class_slot: String,
    /// The rule id that resolved the ticker ([`SLOT_FILL_RULE_ID`], or a fixed-set label).
    pub filled_by: String,
    pub ticker: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aum: Option<AumEvidence>,
}

/// R41 liquidity floors, recorded in the file so a loosening is a visible diff and a different hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Floors {
    pub min_adv_20d_usd: u64,
    pub max_spread_bp: u32,
}

/// The pre-registered universe record. See the module doc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Universe {
    pub id: String,
    pub version: u32,
    /// Must be `"R41"`. Optional in the type so a draft can be parsed and refused with a readable message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ruling: Option<String>,
    /// `YYYY-MM-DD` the file was fixed, BEFORE any evaluation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preregistered_at: Option<String>,
    /// The asset class of every instrument in the universe; checked against the premium's `documented_universes`.
    pub asset_class: AssetClass,
    pub construction_rule: String,
    pub slots: Vec<Slot>,
    pub floors: Floors,
    pub membership_source: String,
    /// `"none"` for a fixed ETF set (the only value slice 1 accepts).
    pub reconstitution: String,
    /// Required when any slot is filled by [`SLOT_FILL_RULE_ID`]: sha256 of that rule's hashed mapping table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_fill_table_hash: Option<String>,
    /// Reference (id or path) of the R22 two-source data-quality certificate for this universe's candles.
    pub data_certificate_ref: String,
    pub cost_model_id: String,
}

impl Universe {
    /// `"{id}@{version}"`, the key universes are stored and referenced under.
    pub fn key(&self) -> String {
        universe_key(&self.id, self.version)
    }

    /// N, the number of instruments.
    pub fn n(&self) -> usize {
        self.slots.len()
    }
}

pub fn universe_key(id: &str, version: u32) -> String {
    format!("{id}@{version}")
}

/// SHA-256 hex of the canonical JSON of the whole record (ruling and preregistration date included).
pub fn universe_hash(u: &Universe) -> String {
    canonical_hash(&serde_json::to_value(u).expect("Universe always serializes"))
}

/// Parse one sidecar. Unknown fields are refused at every level (`deny_unknown_fields`), so a typo cannot
/// silently change a pre-registered universe.
pub fn parse_universe(json: &str) -> Result<Universe, String> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Strict {
        id: String,
        version: u32,
        #[serde(default)]
        ruling: Option<String>,
        #[serde(default)]
        preregistered_at: Option<String>,
        asset_class: AssetClass,
        construction_rule: String,
        slots: Vec<Slot>,
        floors: Floors,
        membership_source: String,
        reconstitution: String,
        #[serde(default)]
        slot_fill_table_hash: Option<String>,
        data_certificate_ref: String,
        cost_model_id: String,
    }
    let s: Strict = serde_json::from_str(json).map_err(|e| format!("universe JSON does not parse: {e}"))?;
    Ok(Universe {
        id: s.id,
        version: s.version,
        ruling: s.ruling,
        preregistered_at: s.preregistered_at,
        asset_class: s.asset_class,
        construction_rule: s.construction_rule,
        slots: s.slots,
        floors: s.floors,
        membership_source: s.membership_source,
        reconstitution: s.reconstitution,
        slot_fill_table_hash: s.slot_fill_table_hash,
        data_certificate_ref: s.data_certificate_ref,
        cost_model_id: s.cost_model_id,
    })
}

pub(crate) fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Refuse a universe that is not a well-formed, pre-registered R41 record. All problems are reported together,
/// `; `-joined. Does NOT apply the trend minimum N (see [`validate_for_trend`]).
pub fn validate(u: &Universe) -> Result<(), String> {
    let mut errors: Vec<String> = Vec::new();

    if u.id.is_empty() || !u.id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        errors.push(format!("id '{}' must be non-empty ascii alphanumeric, '-' or '_'", u.id));
    }
    if u.version == 0 {
        errors.push("version must be >= 1".to_string());
    }
    match u.ruling.as_deref() {
        Some(UNIVERSE_RULING) => {}
        Some(other) => errors.push(format!("ruling must be \"{UNIVERSE_RULING}\", found \"{other}\"")),
        None => errors.push(format!("ruling is missing (must be \"{UNIVERSE_RULING}\")")),
    }
    if u.preregistered_at.as_deref().map(str::trim).unwrap_or("").is_empty() {
        errors.push("preregistered_at is missing".to_string());
    }
    for (name, v) in [
        ("construction_rule", &u.construction_rule),
        ("membership_source", &u.membership_source),
        ("data_certificate_ref", &u.data_certificate_ref),
        ("cost_model_id", &u.cost_model_id),
    ] {
        if v.trim().is_empty() {
            errors.push(format!("{name} is empty"));
        }
    }
    if u.reconstitution != RECONSTITUTION_NONE {
        errors.push(format!("reconstitution must be \"{RECONSTITUTION_NONE}\" (fixed ETF set), found \"{}\"", u.reconstitution));
    }
    if u.floors.min_adv_20d_usd < MIN_ADV_20D_USD {
        errors.push(format!("floors.min_adv_20d_usd {} is below the R41 floor of {MIN_ADV_20D_USD}", u.floors.min_adv_20d_usd));
    }
    if u.floors.max_spread_bp == 0 || u.floors.max_spread_bp > MAX_SPREAD_BP {
        errors.push(format!("floors.max_spread_bp {} must be in 1..={MAX_SPREAD_BP}", u.floors.max_spread_bp));
    }

    if u.slots.is_empty() {
        errors.push("slots is empty".to_string());
    }
    let mut seen = BTreeSet::new();
    let mut uses_slot_fill_rule = false;
    for (i, s) in u.slots.iter().enumerate() {
        if s.ticker.trim().is_empty() {
            errors.push(format!("slots[{i}].ticker is empty"));
        } else if !seen.insert(s.ticker.trim().to_ascii_uppercase()) {
            errors.push(format!("duplicate ticker '{}' (slots[{i}])", s.ticker));
        }
        if s.asset_class_slot.trim().is_empty() {
            errors.push(format!("slots[{i}].asset_class_slot is empty"));
        }
        if s.filled_by.trim().is_empty() {
            errors.push(format!("slots[{i}].filled_by is empty"));
        }
        if s.filled_by == SLOT_FILL_RULE_ID {
            uses_slot_fill_rule = true;
            match &s.aum {
                None => errors.push(format!("slots[{i}] is filled by {SLOT_FILL_RULE_ID} but carries no AUM evidence")),
                Some(a) => {
                    if a.as_of.trim().is_empty() || a.inception.trim().is_empty() {
                        errors.push(format!("slots[{i}].aum needs as_of and inception"));
                    }
                    if a.issuer_net_assets_usd == 0 || a.vendor_net_assets_usd == 0 {
                        errors.push(format!("slots[{i}].aum needs both issuer and vendor net assets (cross-check)"));
                    }
                }
            }
        }
    }
    match (&u.slot_fill_table_hash, uses_slot_fill_rule) {
        (None, true) => errors.push(format!("slot_fill_table_hash is required when a slot is filled by {SLOT_FILL_RULE_ID}")),
        (Some(h), _) if !is_sha256_hex(h) => errors.push("slot_fill_table_hash must be 64 lowercase hex characters".to_string()),
        _ => {}
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

/// [`validate`] plus the R41-A(1) trend rule: N >= [`TREND_MIN_N`].
pub fn validate_for_trend(u: &Universe) -> Result<(), String> {
    validate(u)?;
    if u.n() < TREND_MIN_N {
        return Err(format!("a trend universe needs N >= {TREND_MIN_N} instruments (so 1/N <= 0.20), found N = {}", u.n()));
    }
    Ok(())
}

/// Parse + validate one sidecar; `where_from` prefixes the error.
fn bind_universe(json: &str, where_from: &str) -> Result<Universe, String> {
    let u = parse_universe(json).map_err(|e| format!("{where_from}: {e}"))?;
    validate(&u).map_err(|e| format!("{where_from}: {e}"))?;
    Ok(u)
}

/// The embedded universes, keyed by [`Universe::key`]. Empty today.
pub fn embedded_universes() -> Result<BTreeMap<String, Universe>, String> {
    let mut out = BTreeMap::new();
    for (name, json) in EMBEDDED_UNIVERSE_JSON {
        let u = bind_universe(json, &format!("embedded universe '{name}'"))?;
        if out.insert(u.key(), u).is_some() {
            return Err(format!("embedded universe '{name}' duplicates an earlier universe key"));
        }
    }
    Ok(out)
}

/// Sorted `(path, contents)` of every `*{suffix}` file directly in `dir`. A missing directory is an empty list,
/// never an error; an unreadable file is. Shared by the cell and ledger loaders.
pub(crate) fn read_sidecars(dir: &Path, suffix: &str) -> Result<Vec<(String, String)>, String> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut paths: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?
        .filter_map(|r| r.ok().map(|d| d.path()))
        .filter(|p| p.is_file() && p.file_name().and_then(|n| n.to_str()).map(|n| n.ends_with(suffix)).unwrap_or(false))
        .collect();
    paths.sort();
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        let json = std::fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
        out.push((p.display().to_string(), json));
    }
    Ok(out)
}

/// Every `*.universe.json` in `dir`, parsed and validated, keyed by [`Universe::key`]. A missing or empty
/// directory yields an empty map.
pub fn load_universes_from_dir(dir: &Path) -> Result<BTreeMap<String, Universe>, String> {
    let mut out = BTreeMap::new();
    for (path, json) in read_sidecars(dir, UNIVERSE_FILE_SUFFIX)? {
        let u = bind_universe(&json, &path)?;
        if out.insert(u.key(), u).is_some() {
            return Err(format!("{path}: duplicate universe key"));
        }
    }
    Ok(out)
}

/// Embedded universes plus, when [`UNIVERSE_DIR_ENV`] is set, the files in that directory (embedded wins a
/// collision). Empty today.
pub fn load_universes() -> Result<BTreeMap<String, Universe>, String> {
    let mut out = embedded_universes()?;
    if let Ok(dir) = std::env::var(UNIVERSE_DIR_ENV) {
        let dir = dir.trim().to_string();
        if !dir.is_empty() {
            for (k, u) in load_universes_from_dir(Path::new(&dir))? {
                out.entry(k).or_insert(u);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub const AC5_JSON: &str = include_str!("../../tests/fixtures/r41/test_ac5_v1.universe.json");
    pub const SEC9_JSON: &str = include_str!("../../tests/fixtures/r41/test_sec9_v1.universe.json");
    /// Pinned values: python canonical JSON (sorted keys, no whitespace) of the fixture files.
    pub const AC5_HASH: &str = "6a6f5b38cfbda82827ea1fafc8b036cca90675b1562636cc54c3db41ce4efe21";
    pub const SEC9_HASH: &str = "5640a303ff3bf03078aa7ef14af5a5e8a8b984906bfcbe189c5fc7d19dabd99a";

    pub fn ac5() -> Universe {
        parse_universe(AC5_JSON).unwrap()
    }
    pub fn sec9() -> Universe {
        parse_universe(SEC9_JSON).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[test]
    fn fixture_hashes_are_pinned() {
        assert_eq!(universe_hash(&ac5()), AC5_HASH);
        assert_eq!(universe_hash(&sec9()), SEC9_HASH);
    }

    #[test]
    fn fixtures_validate_and_round_trip() {
        for (json, n) in [(AC5_JSON, 5), (SEC9_JSON, 9)] {
            let u = parse_universe(json).unwrap();
            validate(&u).unwrap();
            assert_eq!(u.n(), n);
            let again = parse_universe(&serde_json::to_string(&u).unwrap()).unwrap();
            assert_eq!(u, again);
            assert_eq!(universe_hash(&u), universe_hash(&again));
        }
    }

    #[test]
    fn hash_changes_with_any_field_including_ruling() {
        let base = ac5();
        let mut changed = base.clone();
        changed.slots[0].ticker = "VOO".to_string();
        assert_ne!(universe_hash(&base), universe_hash(&changed));
        let mut unruled = base.clone();
        unruled.ruling = None;
        assert_ne!(universe_hash(&base), universe_hash(&unruled));
    }

    #[test]
    fn unknown_fields_are_refused_at_every_level() {
        let top = AC5_JSON.replacen("\"id\"", "\"extra\":1,\"id\"", 1);
        assert!(parse_universe(&top).unwrap_err().contains("does not parse"));
        let nested = AC5_JSON.replacen("\"ticker\"", "\"weight\":0.5,\"ticker\"", 1);
        assert!(parse_universe(&nested).unwrap_err().contains("does not parse"));
        let floors = AC5_JSON.replacen("\"min_adv_20d_usd\"", "\"min_price\":1,\"min_adv_20d_usd\"", 1);
        assert!(parse_universe(&floors).unwrap_err().contains("does not parse"));
    }

    #[test]
    fn ruling_is_required_and_must_be_r41() {
        let mut u = ac5();
        u.ruling = None;
        assert!(validate(&u).unwrap_err().contains("ruling is missing"));
        u.ruling = Some("R38".to_string());
        assert!(validate(&u).unwrap_err().contains("must be \"R41\""));
    }

    #[test]
    fn duplicate_tickers_are_refused_case_insensitively() {
        let mut u = sec9();
        u.slots[1].ticker = "xlb".to_string();
        assert!(validate(&u).unwrap_err().contains("duplicate ticker"));
    }

    #[test]
    fn trend_universe_needs_n_of_at_least_five() {
        let mut u = ac5();
        validate_for_trend(&u).unwrap();
        u.slots.pop();
        assert_eq!(u.n(), 4);
        validate(&u).unwrap();
        assert!(validate_for_trend(&u).unwrap_err().contains("N >= 5"));
    }

    #[test]
    fn floors_cannot_be_loosened_and_reconstitution_must_be_none() {
        let mut u = sec9();
        u.floors.min_adv_20d_usd = 9_999_999;
        assert!(validate(&u).unwrap_err().contains("below the R41 floor"));
        let mut u = sec9();
        u.floors.max_spread_bp = 6;
        assert!(validate(&u).unwrap_err().contains("max_spread_bp"));
        let mut u = sec9();
        u.reconstitution = "annual".to_string();
        assert!(validate(&u).unwrap_err().contains("reconstitution"));
    }

    #[test]
    fn slot_fill_rule_requires_aum_evidence_and_a_table_hash() {
        let mut u = ac5();
        u.slots[0].aum = None;
        assert!(validate(&u).unwrap_err().contains("no AUM evidence"));
        let mut u = ac5();
        u.slot_fill_table_hash = None;
        assert!(validate(&u).unwrap_err().contains("slot_fill_table_hash is required"));
        let mut u = ac5();
        u.slot_fill_table_hash = Some("nothex".to_string());
        assert!(validate(&u).unwrap_err().contains("64 lowercase hex"));
    }

    #[test]
    fn loaders_are_empty_when_nothing_is_committed_and_read_a_directory_when_given_one() {
        assert!(embedded_universes().unwrap().is_empty());
        let committed = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/strategy_library/universes");
        assert!(load_universes_from_dir(&committed).unwrap().is_empty(), "inert: no universe files committed");

        let dir = std::env::temp_dir().join(format!("universe-test-{}", uuid::Uuid::new_v4()));
        assert!(load_universes_from_dir(&dir).unwrap().is_empty(), "a missing directory is empty, not an error");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("test_ac5_v1.universe.json"), AC5_JSON).unwrap();
        std::fs::write(dir.join("ignored.txt"), "not a universe").unwrap();
        let loaded = load_universes_from_dir(&dir).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded.get("TEST-AC5@1"), Some(&ac5()));
        std::fs::write(dir.join("bad.universe.json"), "{}").unwrap();
        assert!(load_universes_from_dir(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
