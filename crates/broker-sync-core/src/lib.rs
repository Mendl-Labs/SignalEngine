//! Read-only broker account reading for the dashboard sync (ruling R54, build-order PR 1).
//!
//! What lives here:
//! - [`BrokerReader`]: the narrow trait. It exposes reads only (account summary, positions, open orders). It has no
//!   method that changes an order, and the source-scan test (`tests/source_scan.rs`) fails the build if this crate
//!   or any sibling `broker-sync*` crate ever mentions an order-changing API.
//! - [`AccountSnapshot`] and friends: the one normalised shape every venue is mapped into.
//! - [`alpaca::AlpacaReader`]: wraps the existing, tested `broker-adapters` Alpaca adapter (reads only).
//! - [`stubs`]: Kraken, OANDA and Bitstamp readers that answer `Unsupported` until their PRs land.
//! - [`fake::FakeReader`]: an in-memory reader for tests.
//! - [`classify_input`]: the input types of the difference classifier (PR 5). No classifier logic here.
//!
//! Deliberately NOT here: any database, pod, scheduler, alert, credential handling or master-key code. A reader is
//! handed an already-built adapter; this crate never sees how credentials were obtained.

#![forbid(unsafe_code)]

pub mod alpaca;
pub mod classify_input;
pub mod fake;
pub mod reader;
pub mod snapshot;
pub mod stubs;

pub use reader::{build_snapshot, AccountSummary, BrokerReader, ReadError};
pub use snapshot::{AccountRef, AccountSnapshot, Broker, Environment, OpenOrder, OrderSide, Position, SourceStatus};
