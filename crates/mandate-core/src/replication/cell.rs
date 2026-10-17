//! R41 slice 1 (R41 / R41-A): a CELL is one pre-registered (rule x universe) pair. The rule is the existing,
//! already-certified library entry; the universe is a hashed [`crate::replication::universe::Universe`]; the cell
//! file records everything that is fixed BEFORE the sweep runs (rule@version, universe@version + universe hash,
//! family id, premium reference, weight policy and its derived weight, the per-cell envelope reference, the
//! cost-model id) and, after evaluation, the status and the printed admission reason.
//!
//! The cell is a git sidecar (`program/data/strategy_library/cells/<rule>__<universe>.cell.json`), never a field
//! on `LibraryEntry`: the rule entry, its `entry_hash` and every certified digest are untouched.
//!
//! [`cell_hash`] pins the PRE-REGISTERED part only: `status`, `evaluation_refs` and `admission` are excluded, so
//! recording an outcome never changes the hash that was fixed before evaluation (R41-A(3)(3): "cell file written
//! and hashed BEFORE evaluation").
//!
//! Caps (R41, year one): at most [`MAX_CELLS_YEAR_ONE`] cells, [`MAX_RULES`] rules, and
//! [`MAX_UNIVERSES_PER_RULE`] universes per rule ([`validate_cell_set`]).
//!
//! Ships INERT, same as `universe` and `envelope`: the embedded list is empty and no `*.cell.json` is committed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::replication::canon::canonical_hash;
use crate::replication::universe::{is_sha256_hex, read_sidecars, universe_hash, universe_key, validate_for_trend, Universe};
use crate::strategy_library::LibraryEntry;

pub const CELL_RULING: &str = "R41";
pub const CELL_FILE_SUFFIX: &str = ".cell.json";
/// Directory (of `*.cell.json`) loaded IN ADDITION to the embedded list; embedded wins a collision.
pub const CELL_DIR_ENV: &str = "STRATEGY_LIBRARY_CELL_DIR";
/// R41 year-one caps.
pub const MAX_CELLS_YEAR_ONE: usize = 12;
pub const MAX_RULES: usize = 3;
pub const MAX_UNIVERSES_PER_RULE: usize = 6;
/// The family-wise false-admission rate each printed expected-spurious figure is `family_n` times.
pub const FAMILY_ALPHA: f64 = 0.05;

/// Embedded sidecars. EMPTY by design.
const EMBEDDED_CELL_JSON: &[(&str, &str)] = &[];

/// R41-A(1)/(2): how the cell's per-instrument weight is fixed. Serialised as a string in the cell file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WeightPolicy {
    /// The rule's own constant (the certified default path), or no weight parameter at all (momentum: rank
    /// weights sum to 1 by construction). No `n` or `derived_weight` is recorded.
    #[serde(rename = "fixed_constant")]
    FixedConstant,
    /// R41-A(1): exactly `1.0 / N` per instrument, N from the hashed universe, written into the file.
    #[serde(rename = "equal_1_over_N")]
    EqualOverN,
}

impl WeightPolicy {
    /// The weight this policy derives on an `n`-instrument universe: `1.0 / n as f64` for [`Self::EqualOverN`]
    /// (so N = 5 is bit-identical to the literal `0.20`), `None` for [`Self::FixedConstant`].
    pub fn derive(self, n: usize) -> Option<f64> {
        match self {
            WeightPolicy::FixedConstant => None,
            WeightPolicy::EqualOverN => Some(1.0 / (n as f64)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CellStatus {
    Preregistered,
    Evaluated,
    Admitted,
    PaperOnly,
    Held,
}

/// The per-cell envelope (R38) the sweep will use: its id and sha256, fixed before the sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvelopeRef {
    pub envelope_id: String,
    pub envelope_hash: String,
}

/// The printed admission reason (R41 family disclosure): window T, power at SR 0.5, family n, expected spurious
/// admits (`0.05 x n`), correlation to the canonical cell.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionReason {
    pub t_months: u32,
    pub power_at_sr_0_5: f64,
    pub family_n: u32,
    pub expected_spurious: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rho_to_canonical: Option<f64>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cell {
    pub rule_entry_id: String,
    pub rule_entry_version: u32,
    pub universe_id: String,
    pub universe_version: u32,
    /// [`universe_hash`] of the universe file this cell was registered against.
    pub universe_hash: String,
    pub family_id: String,
    /// A [`ROBUST_PREMIA`] id; the universe's asset class must be in that premium's `documented_universes`.
    pub premium_ref: String,
    pub weight_policy: WeightPolicy,
    /// Required (== the universe's N) for [`WeightPolicy::EqualOverN`], absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub n: Option<u32>,
    /// Required (== `1.0 / n`, bit-for-bit) for [`WeightPolicy::EqualOverN`], absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_weight: Option<f64>,
    pub envelope_ref: EnvelopeRef,
    pub cost_model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ruling: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preregistered_at: Option<String>,
    pub status: CellStatus,
    /// Run / digest references recorded after evaluation. Empty while `Preregistered`.
    #[serde(default)]
    pub evaluation_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<AdmissionReason>,
}

impl Cell {
    /// `"{rule}@{v}__{universe}@{v}"`, the cell's identity (and its file stem).
    pub fn cell_id(&self) -> String {
        format!(
            "{}@{}__{}",
            self.rule_entry_id,
            self.rule_entry_version,
            universe_key(&self.universe_id, self.universe_version)
        )
    }

    pub fn universe_key(&self) -> String {
        universe_key(&self.universe_id, self.universe_version)
    }
}

/// SHA-256 hex of the canonical JSON of the PRE-REGISTERED part of the cell: everything except `status`,
/// `evaluation_refs` and `admission`.
pub fn cell_hash(c: &Cell) -> String {
    let mut v = serde_json::to_value(c).expect("Cell always serializes");
    if let Some(o) = v.as_object_mut() {
        for k in ["status", "evaluation_refs", "admission"] {
            o.remove(k);
        }
    }
    canonical_hash(&v)
}

/// Parse one sidecar; unknown fields are refused at every level.
pub fn parse_cell(json: &str) -> Result<Cell, String> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Strict {
        rule_entry_id: String,
        rule_entry_version: u32,
        universe_id: String,
        universe_version: u32,
        universe_hash: String,
        family_id: String,
        premium_ref: String,
        weight_policy: WeightPolicy,
        #[serde(default)]
        n: Option<u32>,
        #[serde(default)]
        derived_weight: Option<f64>,
        envelope_ref: EnvelopeRef,
        cost_model_id: String,
        #[serde(default)]
        ruling: Option<String>,
        #[serde(default)]
        preregistered_at: Option<String>,
        status: CellStatus,
        #[serde(default)]
        evaluation_refs: Vec<String>,
        #[serde(default)]
        admission: Option<AdmissionReason>,
    }
    let s: Strict = serde_json::from_str(json).map_err(|e| format!("cell JSON does not parse: {e}"))?;
    Ok(Cell {
        rule_entry_id: s.rule_entry_id,
        rule_entry_version: s.rule_entry_version,
        universe_id: s.universe_id,
        universe_version: s.universe_version,
        universe_hash: s.universe_hash,
        family_id: s.family_id,
        premium_ref: s.premium_ref,
        weight_policy: s.weight_policy,
        n: s.n,
        derived_weight: s.derived_weight,
        envelope_ref: s.envelope_ref,
        cost_model_id: s.cost_model_id,
        ruling: s.ruling,
        preregistered_at: s.preregistered_at,
        status: s.status,
        evaluation_refs: s.evaluation_refs,
        admission: s.admission,
    })
}

/// Refuse a cell that does not reference an existing rule entry and an existing, matching universe, whose
/// universe asset class its premium does not document, whose weight policy is not exactly derived from the
/// universe, or whose status and admission fields disagree. All problems are reported together, `; `-joined.
pub fn validate(cell: &Cell, universe: &Universe, library: &[LibraryEntry]) -> Result<(), String> {
    let mut errors: Vec<String> = Vec::new();

    match cell.ruling.as_deref() {
        Some(CELL_RULING) => {}
        Some(other) => errors.push(format!("ruling must be \"{CELL_RULING}\", found \"{other}\"")),
        None => errors.push(format!("ruling is missing (must be \"{CELL_RULING}\")")),
    }
    if cell.preregistered_at.as_deref().map(str::trim).unwrap_or("").is_empty() {
        errors.push("preregistered_at is missing".to_string());
    }
    if cell.family_id.trim().is_empty() {
        errors.push("family_id is empty".to_string());
    }

    // ---- rule entry
    if !library.iter().any(|e| e.id == cell.rule_entry_id && e.version == cell.rule_entry_version) {
        errors.push(format!("no library entry {}/{} to bind to", cell.rule_entry_id, cell.rule_entry_version));
    }

    // ---- universe
    if universe.id != cell.universe_id || universe.version != cell.universe_version {
        errors.push(format!(
            "universe {} does not match the cell's reference {}",
            universe.key(),
            cell.universe_key()
        ));
    }
    let actual = universe_hash(universe);
    if cell.universe_hash != actual {
        errors.push(format!(
            "universe_hash {} does not match the universe file's hash {}",
            short(&cell.universe_hash),
            short(&actual)
        ));
    }
    if cell.cost_model_id.trim().is_empty() {
        errors.push("cost_model_id is empty".to_string());
    } else if cell.cost_model_id != universe.cost_model_id {
        errors.push(format!(
            "cost_model_id '{}' differs from the universe's '{}'",
            cell.cost_model_id, universe.cost_model_id
        ));
    }

    // ---- premium and documented_universes (R31 amendment): checked by the Engine when it loads the cell. This copy
    // omits the check (it needs the Engine's ROBUST_PREMIA table); the rebalancer binds the cell by HASH instead, see
    // `crate::cell_registry`.

    // ---- envelope reference
    if cell.envelope_ref.envelope_id.trim().is_empty() {
        errors.push("envelope_ref.envelope_id is empty".to_string());
    }
    if !is_sha256_hex(&cell.envelope_ref.envelope_hash) {
        errors.push("envelope_ref.envelope_hash must be 64 lowercase hex characters".to_string());
    }

    // ---- weight policy
    match cell.weight_policy {
        WeightPolicy::FixedConstant => {
            if cell.n.is_some() || cell.derived_weight.is_some() {
                errors.push("fixed_constant takes no n or derived_weight".to_string());
            }
        }
        WeightPolicy::EqualOverN => {
            if let Err(e) = validate_for_trend(universe) {
                errors.push(e);
            }
            match cell.n {
                None => errors.push("equal_1_over_N requires n".to_string()),
                Some(n) if n as usize != universe.n() => {
                    errors.push(format!("n {n} does not equal the universe's N {}", universe.n()))
                }
                Some(_) => {}
            }
            let expected = cell.weight_policy.derive(universe.n());
            match (cell.derived_weight, expected) {
                (None, _) => errors.push("equal_1_over_N requires derived_weight".to_string()),
                (Some(w), Some(e)) if w.to_bits() != e.to_bits() => {
                    errors.push(format!("derived_weight {w} is not exactly 1.0/N = {e}"))
                }
                _ => {}
            }
        }
    }

    // ---- status and admission consistency
    match cell.status {
        CellStatus::Preregistered => {
            if !cell.evaluation_refs.is_empty() || cell.admission.is_some() {
                errors.push("a preregistered cell carries no evaluation_refs or admission".to_string());
            }
        }
        CellStatus::Evaluated => {
            if cell.evaluation_refs.is_empty() {
                errors.push("an evaluated cell needs evaluation_refs".to_string());
            }
        }
        CellStatus::Admitted | CellStatus::PaperOnly | CellStatus::Held => {
            if cell.evaluation_refs.is_empty() {
                errors.push("a decided cell needs evaluation_refs".to_string());
            }
            if cell.admission.is_none() {
                errors.push("a decided cell needs its printed admission reason".to_string());
            }
        }
    }
    if let Some(a) = &cell.admission {
        if a.reason.trim().is_empty() {
            errors.push("admission.reason is empty".to_string());
        }
        if a.t_months == 0 || a.family_n == 0 {
            errors.push("admission.t_months and admission.family_n must be >= 1".to_string());
        }
        if !(0.0..=1.0).contains(&a.power_at_sr_0_5) {
            errors.push("admission.power_at_sr_0_5 must be in [0, 1]".to_string());
        }
        if (a.expected_spurious - FAMILY_ALPHA * f64::from(a.family_n)).abs() > 1e-9 {
            errors.push(format!("admission.expected_spurious must be {FAMILY_ALPHA} x family_n"));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn short(h: &str) -> &str {
    &h[..h.len().min(12)]
}

/// R41 year-one caps over a whole set of cells: at most [`MAX_CELLS_YEAR_ONE`] cells, [`MAX_RULES`] distinct
/// rules, [`MAX_UNIVERSES_PER_RULE`] distinct universes per rule, and no cell id twice.
pub fn validate_cell_set(cells: &[Cell]) -> Result<(), String> {
    let mut errors: Vec<String> = Vec::new();
    if cells.len() > MAX_CELLS_YEAR_ONE {
        errors.push(format!("{} cells exceeds the year-one cap of {MAX_CELLS_YEAR_ONE}", cells.len()));
    }
    let mut ids = BTreeSet::new();
    let mut by_rule: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for c in cells {
        if !ids.insert(c.cell_id()) {
            errors.push(format!("cell {} appears twice", c.cell_id()));
        }
        by_rule.entry(c.rule_entry_id.as_str()).or_default().insert(c.universe_key());
    }
    if by_rule.len() > MAX_RULES {
        errors.push(format!("{} rules exceeds the cap of {MAX_RULES}", by_rule.len()));
    }
    for (rule, universes) in &by_rule {
        if universes.len() > MAX_UNIVERSES_PER_RULE {
            errors.push(format!("rule '{rule}' has {} universes, exceeding the cap of {MAX_UNIVERSES_PER_RULE}", universes.len()));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn bind_cell(
    json: &str,
    where_from: &str,
    universes: &BTreeMap<String, Universe>,
    library: &[LibraryEntry],
) -> Result<Cell, String> {
    let cell = parse_cell(json).map_err(|e| format!("{where_from}: {e}"))?;
    let universe = universes
        .get(&cell.universe_key())
        .ok_or_else(|| format!("{where_from}: no universe {} to bind to", cell.universe_key()))?;
    validate(&cell, universe, library).map_err(|e| format!("{where_from}: {e}"))?;
    Ok(cell)
}

fn finish(cells: BTreeMap<String, Cell>) -> Result<BTreeMap<String, Cell>, String> {
    let list: Vec<Cell> = cells.values().cloned().collect();
    validate_cell_set(&list)?;
    Ok(cells)
}

/// The embedded cells, keyed by [`Cell::cell_id`], each bound to its universe and rule entry. Empty today.
pub fn embedded_cells(universes: &BTreeMap<String, Universe>, library: &[LibraryEntry]) -> Result<BTreeMap<String, Cell>, String> {
    let mut out = BTreeMap::new();
    for (name, json) in EMBEDDED_CELL_JSON {
        let c = bind_cell(json, &format!("embedded cell '{name}'"), universes, library)?;
        if out.insert(c.cell_id(), c).is_some() {
            return Err(format!("embedded cell '{name}' duplicates an earlier cell id"));
        }
    }
    finish(out)
}

/// Every `*.cell.json` in `dir`, parsed, bound and validated, then checked against the caps. A missing or empty
/// directory yields an empty map.
pub fn load_cells_from_dir(
    dir: &Path,
    universes: &BTreeMap<String, Universe>,
    library: &[LibraryEntry],
) -> Result<BTreeMap<String, Cell>, String> {
    let mut out = BTreeMap::new();
    for (path, json) in read_sidecars(dir, CELL_FILE_SUFFIX)? {
        let c = bind_cell(&json, &path, universes, library)?;
        if out.insert(c.cell_id(), c).is_some() {
            return Err(format!("{path}: duplicate cell id"));
        }
    }
    finish(out)
}

/// Embedded cells plus, when [`CELL_DIR_ENV`] is set, the files in that directory (embedded wins a collision);
/// the cap check runs over the union. Empty today.
pub fn load_cells(universes: &BTreeMap<String, Universe>, library: &[LibraryEntry]) -> Result<BTreeMap<String, Cell>, String> {
    let mut out = embedded_cells(universes, library)?;
    if let Ok(dir) = std::env::var(CELL_DIR_ENV) {
        let dir = dir.trim().to_string();
        if !dir.is_empty() {
            for (k, c) in load_cells_from_dir(Path::new(&dir), universes, library)? {
                out.entry(k).or_insert(c);
            }
            return finish(out);
        }
    }
    Ok(out)
}
