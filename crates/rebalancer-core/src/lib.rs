//! Pure rebalancer logic (first slice of WP4): the mandate policy compiler, the pre-trade guard and the order
//! planner. No network, no database, no async, no clock (every instant is an explicit input).
//!
//! * [`policy`]: `Policy::compile(&MandateBody)`, the ONLY place mandate `f64` ratios become exact decimals.
//! * [`guard`]: `PreTradeGuard::check`, a pure allow/deny function with stable machine codes.
//! * [`planner`]: `OrderPlanner::plan`, targets to a sells-first list of guarded, idempotently tagged orders.
//!   Signed sleeves (shorts, gross above 1x) are an explicit per-sleeve opt-in, default off; see the planner docs.
//!   For a signed plan the guard (`PreTradeGuard::check_signed`) denies a short when the mandate forbids shorting or
//!   the venue facts for that instrument forbid, lack or need a locate for it, and denies leverage only when GROSS
//!   exceeds `min(mandate leverage, mandate max gross, venue max leverage)`; a short alone is not leverage.
//! * [`venue`]: venue size rules, filled from the broker adapters' own tables and code (no duplicated constants),
//!   and the per-instrument short / leverage / position-size facts a signed plan needs (`InstrumentRules`, data
//!   supplied by the caller from the broker; absent means unknown, which fails closed for a short).
//! * [`dec_math`]: exact decimal helpers.

#![forbid(unsafe_code)]

pub mod dec_math;
pub mod guard;
pub mod planner;
pub mod policy;
pub mod venue;

pub use broker_adapters::Dec;
