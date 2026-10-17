//! Standalone copy of the Engine's pure mandate module and strategy library.
//!
//! `mandate.rs` and `strategy_library.rs` are VERBATIM copies (see `SOURCE.md` for the source commit and blob
//! hashes). They are declared here as the modules `mandate` and `strategy_library`, so the Engine's
//! `use crate::mandate::...` paths and the `include_str!("../data/strategy_library/...")` paths resolve unchanged.
//! Do not edit those two files here: change them in the Engine and re-copy, then update `SOURCE.md` and the
//! pinned hash in `tests/golden.rs`.

pub mod mandate;
pub mod strategy_library;
// R41 (rule x universe cells): the canonical hash, universe and cell types copied from the Engine (see SOURCE.md),
// a stand-in for the one Engine type they need, and the registry the rebalancer verifies a plan sleeve against.
// All inert: the embedded universe and cell lists are empty.
pub mod cell_registry;
pub mod citation_verification;
pub mod replication;
