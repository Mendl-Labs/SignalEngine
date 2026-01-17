//! Test Assertions for Execution Results
//!
//! Provides fluent assertion helpers for validating execution outcomes.

use crate::core::types::{ExecutionResult, ExecutionStatus, ExecutionFill, OrderSide};

/// Assertion helpers for execution results
pub struct ExecutionAssertions;

impl ExecutionAssertions {
    /// Assert that the execution was successful (filled or partially filled)
    pub fn assert_success(result: &ExecutionResult) {
        assert!(
            matches!(result.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled),
            "Expected successful execution, got {:?}", result.status
        );
    }

    /// Assert that the execution was fully filled
    pub fn assert_filled(result: &ExecutionResult) {
        assert!(
            matches!(result.status, ExecutionStatus::Filled),
            "Expected filled status, got {:?}", result.status
        );
        assert_eq!(
            result.remaining_quantity, 0.0,
            "Expected no remaining quantity for filled order"
        );
    }

    /// Assert that the execution was rejected
    pub fn assert_rejected(result: &ExecutionResult) {
        assert!(
            matches!(result.status, ExecutionStatus::Rejected),
            "Expected rejected status, got {:?}", result.status
        );
    }

    /// Assert fill price is within tolerance of expected price
    pub fn assert_price_within_tolerance(result: &ExecutionResult, expected_price: f64, tolerance_bps: f64) {
        if result.filled_quantity > 0.0 {
            let diff_bps = ((result.avg_fill_price - expected_price) / expected_price).abs() * 10000.0;
            assert!(
                diff_bps <= tolerance_bps,
                "Fill price {} differs from expected {} by {} bps (tolerance: {} bps)",
                result.avg_fill_price, expected_price, diff_bps, tolerance_bps
            );
        }
    }

    /// Assert latency is below threshold
    pub fn assert_latency_under(result: &ExecutionResult, max_latency_ns: u64) {
        assert!(
            result.latency_ns <= max_latency_ns,
            "Latency {} ns exceeds maximum {} ns",
            result.latency_ns, max_latency_ns
        );
    }

    /// Assert fill quantity matches expected within tolerance
    pub fn assert_fill_quantity(result: &ExecutionResult, expected_qty: f64, tolerance: f64) {
        let diff = (result.filled_quantity - expected_qty).abs();
        assert!(
            diff <= tolerance,
            "Fill quantity {} differs from expected {} by {} (tolerance: {})",
            result.filled_quantity, expected_qty, diff, tolerance
        );
    }
}

/// Convenience function for price tolerance assertion
pub fn assert_fill_within_tolerance(result: &ExecutionResult, expected_price: f64, tolerance_bps: f64) {
    ExecutionAssertions::assert_price_within_tolerance(result, expected_price, tolerance_bps);
}

/// Builder for fluent assertions
pub struct ResultAssertion<'a> {
    result: &'a ExecutionResult,
}

impl<'a> ResultAssertion<'a> {
    pub fn new(result: &'a ExecutionResult) -> Self {
        Self { result }
    }

    pub fn is_filled(self) -> Self {
        ExecutionAssertions::assert_filled(self.result);
        self
    }

    pub fn is_success(self) -> Self {
        ExecutionAssertions::assert_success(self.result);
        self
    }

    pub fn is_rejected(self) -> Self {
        ExecutionAssertions::assert_rejected(self.result);
        self
    }

    pub fn has_latency_under(self, max_ns: u64) -> Self {
        ExecutionAssertions::assert_latency_under(self.result, max_ns);
        self
    }

    pub fn has_price_within(self, expected: f64, tolerance_bps: f64) -> Self {
        ExecutionAssertions::assert_price_within_tolerance(self.result, expected, tolerance_bps);
        self
    }

    pub fn has_fill_quantity(self, expected: f64, tolerance: f64) -> Self {
        ExecutionAssertions::assert_fill_quantity(self.result, expected, tolerance);
        self
    }

    pub fn has_fees(self) -> Self {
        assert!(
            self.result.total_fees > 0.0,
            "Expected non-zero fees for filled order"
        );
        self
    }

    pub fn has_order_id(self) -> Self {
        assert!(
            !self.result.order_id.is_empty(),
            "Expected non-empty order ID"
        );
        self
    }
}

/// Extension trait for ExecutionResult
pub trait ExecutionResultExt {
    fn assert(&self) -> ResultAssertion;
}

impl ExecutionResultExt for ExecutionResult {
    fn assert(&self) -> ResultAssertion {
        ResultAssertion::new(self)
    }
}

/// Batch assertion helpers
pub struct BatchAssertions;

impl BatchAssertions {
    /// Assert all results are successful
    pub fn all_successful(results: &[ExecutionResult]) {
        for (i, result) in results.iter().enumerate() {
            assert!(
                matches!(result.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled),
                "Result {} failed: {:?}", i, result.status
            );
        }
    }

    /// Assert success rate meets threshold
    pub fn success_rate_at_least(results: &[ExecutionResult], min_rate: f64) {
        let successful = results.iter()
            .filter(|r| matches!(r.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled))
            .count();
        let rate = successful as f64 / results.len() as f64;
        assert!(
            rate >= min_rate,
            "Success rate {:.2}% is below minimum {:.2}%",
            rate * 100.0, min_rate * 100.0
        );
    }

    /// Assert average latency is below threshold
    pub fn avg_latency_under(results: &[ExecutionResult], max_avg_ns: u64) {
        if results.is_empty() {
            return;
        }
        let avg: u64 = results.iter().map(|r| r.latency_ns).sum::<u64>() / results.len() as u64;
        assert!(
            avg <= max_avg_ns,
            "Average latency {} ns exceeds maximum {} ns",
            avg, max_avg_ns
        );
    }

    /// Assert p99 latency is below threshold
    pub fn p99_latency_under(results: &[ExecutionResult], max_p99_ns: u64) {
        if results.is_empty() {
            return;
        }
        let mut latencies: Vec<u64> = results.iter().map(|r| r.latency_ns).collect();
        latencies.sort();
        let p99_idx = (latencies.len() as f64 * 0.99) as usize;
        let p99 = latencies.get(p99_idx.min(latencies.len() - 1)).copied().unwrap_or(0);
        assert!(
            p99 <= max_p99_ns,
            "P99 latency {} ns exceeds maximum {} ns",
            p99, max_p99_ns
        );
    }

    /// Assert no duplicate order IDs
    pub fn no_duplicate_order_ids(results: &[ExecutionResult]) {
        let mut seen = std::collections::HashSet::new();
        for result in results {
            assert!(
                seen.insert(&result.order_id),
                "Duplicate order ID: {}", result.order_id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::ExecutionFill;

    fn make_filled_result(id: &str, price: f64, qty: f64, latency_ns: u64) -> ExecutionResult {
        ExecutionResult {
            order_id: id.to_string(),
            exchange_order_id: Some(format!("ex-{}", id)),
            exchange: "test".to_string(),
            status: ExecutionStatus::Filled,
            filled_quantity: qty,
            remaining_quantity: 0.0,
            avg_fill_price: price,
            total_fees: 0.1,
            fills: vec![ExecutionFill {
                fill_id: "f-1".to_string(),
                order_id: "ord-1".to_string(),
                exchange_order_id: "ex-1".to_string(),
                symbol: "BTC/USD".to_string(),
                side: OrderSide::Buy,
                quantity: qty,
                price,
                fee: 0.1,
                fee_asset: "USD".to_string(),
                timestamp: 0,
                trade_id: "t-1".to_string(),
                is_maker: false,
                exchange_timestamp_ns: None,
                exchange_sequence: None,
            }],
            reject_reason: None,
            submitted_at: 0,
            updated_at: 0,
            latency_ns,
            exchange_timestamp_ns: None,
            exchange_sequence: None,
        }
    }

    #[test]
    fn test_fluent_assertions() {
        let result = make_filled_result("test-1", 100.0, 1.0, 1000);
        
        result.assert()
            .is_filled()
            .has_order_id()
            .has_latency_under(10000)
            .has_price_within(100.0, 10.0)
            .has_fill_quantity(1.0, 0.001);
    }

    #[test]
    fn test_batch_assertions() {
        let results = vec![
            make_filled_result("order-1", 100.0, 1.0, 1000),
            make_filled_result("order-2", 101.0, 1.0, 1200),
            make_filled_result("order-3", 99.5, 1.0, 800),
        ];

        BatchAssertions::all_successful(&results);
        BatchAssertions::avg_latency_under(&results, 5000);
        BatchAssertions::no_duplicate_order_ids(&results);
    }
}
