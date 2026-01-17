//! Testing utilities for ExecutionHandler
//!
//! Provides mock exchanges, test fixtures, and integration test helpers
//! for end-to-end testing of the execution pipeline.

pub mod mock_exchange;
pub mod fixtures;
pub mod assertions;

pub use mock_exchange::{MockExchange, MockExchangeConfig, MockFillBehavior};
pub use fixtures::{TestFixtures, TestSignal, TestScenario};
pub use assertions::{
    ExecutionAssertions, assert_fill_within_tolerance, 
    ResultAssertion, ExecutionResultExt, BatchAssertions
};
