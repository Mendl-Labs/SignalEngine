//! The rebalancer's run machinery (slice 2B and 2C of WP4): broker views, reconciliation, flatten and the `run_once`
//! pipeline, all over traits. Pure and offline: no network, no database, no async runtime; real brokers, a Postgres
//! store and real clocks plug in behind the traits.
//!
//! * [`clock`]: the injectable `Clock` and a `ManualClock` for tests.
//! * [`broker`]: the object-safe `Broker` wrapper over the adapters (Kraken, Alpaca), and what "ours" means.
//! * [`view`]: `BrokerSnapshot` and the per-venue builders with their documented equity definitions.
//! * [`recon`]: `reconcile`, the coded findings and the halt verdict.
//! * [`flatten`]: cancel our orders, sell exactly what is held, verify flat.
//! * [`data`], [`stores`], [`record`]: the data / run-store / notifier / kill-flag traits and the immutable record.
//! * [`pipeline`] holds `run_once`.
//! * [`testkit`]: in-memory doubles for the traits (a recording notifier, a kill-flag switch, fixture data).

#![forbid(unsafe_code)]

pub mod broker;
pub mod clock;
pub mod data;
pub mod flatten;
pub mod pipeline;
pub mod recon;
pub mod record;
pub mod stores;
pub mod testkit;
pub mod view;

pub use rebalancer_core::Dec;
