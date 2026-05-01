//! Lightweight typedefs shared by `UltraStrategyEngine`.
//!
//! The legacy `StrategyRegistry` / `StrategyType` (Avellaneda-Stoikov,
//! RSI-mean-reversion) registry that lived here was never queried by any
//! runtime caller — strategy dispatch happens via `strategyloader::types`
//! and capability flags on `Strategy`. The dead types were removed
//! to eliminate two parallel `StrategyType` enums in the workspace.

/// Strategy ID type — using u16 for cache efficiency and atomic operations.
pub type StrategyId = u16;

/// Pre-hashed symbol identifier used by the hot-path strategy maps.
pub type SymbolHash = u64;
