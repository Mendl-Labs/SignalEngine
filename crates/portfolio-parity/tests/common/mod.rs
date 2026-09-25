//! Shared helpers of the PF3 parity tests. Every integration test file includes this module with `mod common;`, so
//! everything here is `dead_code` in some binary.
//!
//! * [`world`]: a SYNTHETIC world (deterministic formulas, closes rounded to cents; no vendor data), a synthetic
//!   trading calendar, and the assumed 00:10Z data provider (`Vendor`: only bars dated STRICTLY BEFORE the run date).
//! * [`broker`]: `SimBroker`, an in-memory stateful `Broker` that fills every market order at its current price with
//!   ZERO fees (the "fake broker filling at the close" of design 5.4 test 4).
//! * [`adapters`]: the weightsim side: the library rules as `weightsim::WeightRule`s (over the very same
//!   `reference-rules` the pipeline uses), the per-sleeve delay EMULATION, and `PcConstruct`, the adapter that plugs
//!   `portfolio-construct` into `simulate_book` through the `weightsim::Construct` boundary with the live pipeline's
//!   two-phase (sells, re-read cash, buys) order of operations.
//! * [`replay`]: the driver-level replay of a synthetic history through `find_due_runs` / `run_all_due` / `run_once`.

#![allow(dead_code, unused_imports)]

pub mod adapters;
pub mod broker;
pub mod replay;
pub mod world;

use broker_adapters::Dec;
pub use rebalancer_risk::rng::SplitMix64;

const POW10: [f64; 19] =
    [1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18];

/// Exact decimal `units / 10^scale`.
pub fn dec(units: i128, scale: u32) -> Dec {
    Dec::new(units, scale).expect("decimal in range")
}

/// The f64 nearest to `units / 10^scale` (one correctly rounded division: the f64 spelling of the same decimal, so the
/// two sides of a parity test are handed the SAME number).
pub fn f64_of(units: i128, scale: u32) -> f64 {
    units as f64 / POW10[scale as usize]
}

pub fn d(s: &str) -> Dec {
    Dec::parse(s).expect("decimal literal")
}

/// Uniform integer in `lo..=hi` (SplitMix64, no OS randomness).
pub fn ri(r: &mut SplitMix64, lo: i64, hi: i64) -> i64 {
    r.range(0, (hi - lo) as u64) as i64 + lo
}

/// FNV-1a 64 over bytes: a dependency-free digest for pinning tables and sequences.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
