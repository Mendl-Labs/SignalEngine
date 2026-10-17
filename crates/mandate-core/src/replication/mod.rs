//! R41 types copied from the Engine's `program/src/replication/` (see `SOURCE.md`): the canonical-JSON hash, the
//! pre-registered universe and the (rule x universe) cell. The rebalancer uses them to verify a plan sleeve's cell
//! against the platform's own hashed sidecars (`crate::cell_registry`). The Engine's `family_ledger`, `envelope`
//! and evaluation modules are deliberately NOT copied: executing a cell needs only the cell, its universe and the
//! hashes that bind them.

pub mod canon;
pub mod cell;
pub mod universe;
