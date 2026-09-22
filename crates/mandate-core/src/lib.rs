//! Standalone copy of the Engine's pure mandate module and strategy library.
//!
//! `mandate.rs` and `strategy_library.rs` are VERBATIM copies (see `SOURCE.md` for the source commit and blob
//! hashes). They are declared here as the modules `mandate` and `strategy_library`, so the Engine's
//! `use crate::mandate::...` paths and the `include_str!("../data/strategy_library/...")` paths resolve unchanged.
//! Do not edit those two files here: change them in the Engine and re-copy, then update `SOURCE.md` and the
//! pinned hash in `tests/golden.rs`.

pub mod mandate;
pub mod strategy_library;
