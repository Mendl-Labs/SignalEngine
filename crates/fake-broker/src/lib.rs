//! A stateful, scriptable, in-process fake exchange for the Mendl Labs rebalancer's kill drills
//! and journey tests. No network, no async runtime, no real keys; everything is deterministic
//! (manual clock, seeded ids, exact decimal money).
//!
//! # Layout
//!
//! * [`exchange`] is wire-neutral: accounts, balances, a book that is one controllable price per
//!   pair, market and limit orders, scripted fill policies, fees, cancel semantics, invariants.
//! * [`kraken`] is a wire front end: [`kraken::KrakenTransport`] implements the adapter's
//!   `HttpTransport` and speaks Kraken's REST format, including its strict per-key nonce rule and
//!   `API-Sign` verification. An Alpaca front end can be added as a sibling module
//!   (`alpaca/`) that owns its own auth state and rendering and calls the same `Core`; the
//!   shared `World` in [`broker`] would gain one more state field. Nothing in `exchange`, `fault`,
//!   `log` or the control API is Kraken specific except the error strings a test scripts.
//! * [`FakeBrokerHandle`] is the control API (plain methods, no HTTP): move the market, script
//!   fills, inject faults, drift balances, add foreign orders, read the event log.
//! * [`testkit`] wires a real `KrakenAdapter` to the fake; [`scenarios`] are one-call setups for
//!   the broker-only drills of SPEC B1-B10.
//!
//! # Intended usage
//!
//! ```
//! use fake_broker::testkit::KrakenRig;
//! use broker_adapters::{BrokerAdapter, OrderRequest, PlaceOutcome, Side, Dec};
//!
//! let rig = KrakenRig::new();                       // fake exchange + real KrakenAdapter
//! let req = OrderRequest::market("run1:BTC/USD:buy", "BTC/USD", Side::Buy, Dec::parse("0.01").unwrap());
//! match rig.adapter.place_order(&req).unwrap() {
//!     PlaceOutcome::Accepted { broker_order_id, .. } => {
//!         let report = rig.adapter.get_order(&broker_order_id).unwrap();
//!         assert!(report.status.is_terminal());   // the fake filled it at the touch
//!     }
//!     other => panic!("{other:?}"),
//! }
//! assert_eq!(rig.handle.balance("main", "BTC"), Dec::parse("0.01").unwrap());
//! ```

#![forbid(unsafe_code)]

pub mod broker;
pub mod clock;
pub mod exchange;
pub mod fault;
pub mod kraken;
pub mod log;
pub mod money;
pub mod rng;
pub mod scenarios;
pub mod testkit;

pub use broker::{
    default_secret_b64, AccountSpec, FakeBroker, FakeBrokerBuilder, FakeBrokerHandle, IntoDec, DEFAULT_ACCOUNT, DEFAULT_API_KEY,
};
pub use clock::FakeClock;
pub use exchange::policy::{FillPolicy, FillStep, OrderMatcher, OrderRule, StepAmount};
pub use exchange::{FeeSchedule, ForeignOrder, Liquidity, Order, OrderStatus, PairSpec, ReportGlitch};
pub use fault::{Fault, FaultKind, RequestMatcher, Timing};
pub use kraken::{KeyPermissions, KrakenTransport};
pub use log::{Delivered, LogEntry, Origin, RequestRecord};
