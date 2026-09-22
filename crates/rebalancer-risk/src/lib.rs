//! Risk overlay and account state for the Mendl Labs rebalancer (slice 2A of WP4.3).
//!
//! Pure and offline: no network, no database, no async runtime, no clock (every instant is an argument; `chrono` is
//! built without its `clock` feature so `Utc::now()` does not even exist here). Money is the exact decimal
//! [`Dec`]; randomness in tests is [`rng::SplitMix64`].
//!
//! * [`state`]: `AccountState`, the Active / Shrunk / Flattening / Halted machine, high-water mark, day-start
//!   equity, version, halt and resume records. Halted is left only by `resume` with a human approval.
//! * [`overlay`]: `evaluate_risk` (drawdown ladder + daily loss on BROKER equity), `apply_decision`, `step`.
//! * [`approval`]: the unforgeable `HumanApproval` token and its gated issuer.
//! * [`store`]: the `StateStore` compare-and-swap trait and an in-memory implementation.
//!
//! Stable machine codes: [`overlay::RiskCode`], [`state::HaltReason`], [`state::ResumeError`],
//! [`store::StoreError`], [`approval::ApprovalDenied`]. Each is pinned by a test.

#![forbid(unsafe_code)]

pub mod approval;
pub mod overlay;
pub mod rng;
pub mod state;
pub mod store;

pub use rebalancer_core::Dec;
