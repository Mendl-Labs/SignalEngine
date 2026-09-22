//! Broker adapters for the Mendl Labs rebalancer.
//!
//! Design: everything is synchronous and pure except the [`transport::HttpTransport`] boundary.
//! Request building, signing, rounding, response parsing and nonce handling are plain functions
//! and structs, so they are testable offline with [`testing::FakeTransport`]. A real
//! `reqwest`-based transport lives behind the optional `reqwest-transport` feature.
//!
//! Currently implemented: Kraken (see [`kraken`]) and Alpaca equities (see [`alpaca`]).

#![forbid(unsafe_code)]

pub mod alpaca;
pub mod decimal;
pub mod error;
pub mod kraken;
pub mod nonce;
pub mod testing;
pub mod transport;
pub mod types;

pub use decimal::Dec;
pub use error::{BrokerError, ErrorClass, ExchangeError};
pub use types::*;
