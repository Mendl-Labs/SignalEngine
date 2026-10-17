//! The platform's REGISTERED R41 cells, and the check that a plan sleeve's cell reference equals one of them.
//!
//! A plan sleeve that is a cell names it by hashes only: the rule entry's hash (already on every sleeve), the
//! universe hash, the cell hash and the weight policy ([`CellClaim`]). The plan is data written by another service;
//! it is never trusted for WHAT a cell is. The registry holds the platform's own sidecars (the embedded universe
//! and cell lists, loaded the way `seed_library` loads the strategy library: compiled in, EMPTY today), recomputes
//! every hash itself, and answers with the universe's instruments only when every component of the claim equals a
//! registered cell. Anything else is a typed refusal, never a guess and never the published point instead.
//!
//! Pure: no I/O. The environment-directory loaders of the copied modules are NOT consulted here, so an operator
//! cannot register a cell without a code change.

use std::collections::BTreeMap;

use crate::replication::cell::{cell_hash, embedded_cells, validate, validate_cell_set, Cell, CellStatus, WeightPolicy};
use crate::replication::universe::{embedded_universes, universe_hash, Universe};
use crate::strategy_library::{entry_hash, platform_library, LibraryEntry};

/// Refusal code: the claimed cell is not one the platform registered (unknown hash, or any component differing).
pub const PILOT_CELL_NOT_REGISTERED: &str = "PILOT_CELL_NOT_REGISTERED";
/// Refusal code: the cell is registered but its status does not allow execution (only `admitted` and `paper_only`).
pub const PILOT_CELL_NOT_RUNNABLE: &str = "PILOT_CELL_NOT_RUNNABLE";

/// What a plan sleeve says about its cell. Everything here is checked against the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellClaim {
    pub rule_entry_id: String,
    pub rule_entry_version: u32,
    /// `strategy_library::entry_hash` of the pinned rule entry.
    pub rule_entry_hash: String,
    /// `replication::universe::universe_hash` of the universe file.
    pub universe_hash: String,
    /// `replication::cell::cell_hash` of the cell's pre-registered part.
    pub cell_hash: String,
    pub weight_policy: WeightPolicy,
}

/// A claim that equals a registered, runnable cell, with the facts the runtime needs. The instruments come from the
/// platform's universe file, in slot order.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedCell {
    pub cell_id: String,
    pub rule_entry_id: String,
    pub rule_entry_version: u32,
    pub weight_policy: WeightPolicy,
    pub instruments: Vec<String>,
    /// `Some(1.0 / N)` for [`WeightPolicy::EqualOverN`], `None` otherwise.
    pub derived_weight: Option<f64>,
    pub status: CellStatus,
}

impl VerifiedCell {
    pub fn n(&self) -> usize {
        self.instruments.len()
    }
}

/// Why a claim was not accepted. [`CellRefusal::code`] is the stable typed code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellRefusal {
    NotRegistered(String),
    NotRunnable(String),
}

impl CellRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            CellRefusal::NotRegistered(_) => PILOT_CELL_NOT_REGISTERED,
            CellRefusal::NotRunnable(_) => PILOT_CELL_NOT_RUNNABLE,
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            CellRefusal::NotRegistered(m) | CellRefusal::NotRunnable(m) => m,
        }
    }
}

impl std::fmt::Display for CellRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.detail())
    }
}

/// The platform's registered cells, their universes and the library entry hashes they bind to.
#[derive(Debug, Clone, Default)]
pub struct CellRegistry {
    /// Keyed by the cell hash RECOMPUTED here ([`cell_hash`]), never read from a file.
    cells: BTreeMap<String, (Cell, Universe)>,
    entry_hashes: BTreeMap<(String, u32), String>,
}

impl CellRegistry {
    /// A registry holding nothing: every claim is refused.
    pub fn empty() -> Self {
        Self::default()
    }

    /// The platform's registry: the embedded universes and cells bound to the embedded strategy library. EMPTY
    /// today. `Err` is a defect in the compiled-in sidecars; the caller must then fall back to [`Self::empty`]
    /// (fail closed), not run cells.
    pub fn platform() -> Result<Self, String> {
        let library = platform_library()?;
        let universes = embedded_universes()?;
        let cells = embedded_cells(&universes, &library)?;
        Self::from_parts(&universes, cells.into_values().collect(), &library)
    }

    /// Build a registry from loaded parts, re-validating every cell against its universe and the library and the
    /// year-one caps over the whole set. Used by [`Self::platform`] and by tests with fixture sidecars.
    pub fn from_parts(universes: &BTreeMap<String, Universe>, cells: Vec<Cell>, library: &[LibraryEntry]) -> Result<Self, String> {
        validate_cell_set(&cells)?;
        let mut reg = Self::empty();
        for e in library {
            reg.entry_hashes.insert((e.id.clone(), e.version), entry_hash(e));
        }
        for c in cells {
            let u = universes.get(&c.universe_key()).ok_or_else(|| format!("cell {}: no universe {} to bind to", c.cell_id(), c.universe_key()))?;
            validate(&c, u, library).map_err(|e| format!("cell {}: {e}", c.cell_id()))?;
            let h = cell_hash(&c);
            if reg.cells.insert(h, (c, u.clone())).is_some() {
                return Err("two cells have the same cell hash".to_string());
            }
        }
        Ok(reg)
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Accept `claim` only when EVERY component equals a registered cell: the cell hash, the rule entry (id, version
    /// and the platform's own hash of it), the universe hash and the weight policy. Only `admitted` and `paper_only`
    /// cells are runnable.
    pub fn verify(&self, claim: &CellClaim) -> Result<VerifiedCell, CellRefusal> {
        let not_registered = CellRefusal::NotRegistered;
        let (cell, universe) = self
            .cells
            .get(&claim.cell_hash)
            .ok_or_else(|| not_registered(format!("no registered cell has hash {}", short(&claim.cell_hash))))?;
        if cell.rule_entry_id != claim.rule_entry_id || cell.rule_entry_version != claim.rule_entry_version {
            return Err(not_registered(format!(
                "cell {} is a cell of {} v{}, not of {} v{}",
                cell.cell_id(),
                cell.rule_entry_id,
                cell.rule_entry_version,
                claim.rule_entry_id,
                claim.rule_entry_version
            )));
        }
        let ours = self.entry_hashes.get(&(cell.rule_entry_id.clone(), cell.rule_entry_version));
        if ours != Some(&claim.rule_entry_hash) {
            return Err(not_registered(format!("cell {}: the claimed rule entry hash is not the platform's", cell.cell_id())));
        }
        let actual_universe = universe_hash(universe);
        if claim.universe_hash != actual_universe || cell.universe_hash != actual_universe {
            return Err(not_registered(format!("cell {}: the claimed universe hash is not the registered universe's", cell.cell_id())));
        }
        if claim.weight_policy != cell.weight_policy {
            return Err(not_registered(format!("cell {}: the claimed weight policy differs from the registered one", cell.cell_id())));
        }
        match cell.status {
            CellStatus::Admitted | CellStatus::PaperOnly => {}
            other => return Err(CellRefusal::NotRunnable(format!("cell {} has status {other:?}; only admitted and paper_only cells execute", cell.cell_id()))),
        }
        let instruments: Vec<String> = universe.slots.iter().map(|s| s.ticker.trim().to_ascii_uppercase()).collect();
        Ok(VerifiedCell {
            cell_id: cell.cell_id(),
            rule_entry_id: cell.rule_entry_id.clone(),
            rule_entry_version: cell.rule_entry_version,
            weight_policy: cell.weight_policy,
            derived_weight: cell.weight_policy.derive(instruments.len()),
            instruments,
            status: cell.status,
        })
    }
}

fn short(h: &str) -> &str {
    &h[..h.len().min(12)]
}
