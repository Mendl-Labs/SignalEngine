//! Pure rebalancer logic (first slice of WP4): the mandate policy compiler, the pre-trade guard and the order
//! planner. No network, no database, no async, no clock (every instant is an explicit input).
//!
//! * [`policy`]: `Policy::compile(&MandateBody)`, the ONLY place mandate `f64` ratios become exact decimals.
//! * [`guard`]: `PreTradeGuard::check`, a pure allow/deny function with stable machine codes.
//! * [`planner`]: `OrderPlanner::plan`, targets to a sells-first list of guarded, idempotently tagged orders.
//! * [`venue`]: venue size rules, filled from the broker adapters' own tables and code (no duplicated constants).
//! * [`dec_math`]: exact decimal helpers.

#![forbid(unsafe_code)]

pub mod dec_math;
pub mod guard;
pub mod planner;
pub mod policy;
pub mod venue;

pub use broker_adapters::Dec;
