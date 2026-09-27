//! Library half of the rebalancer service: the parts that are pure and testable without a database or a network.
//! The tick loop itself is `main.rs`.

pub mod pilot;
