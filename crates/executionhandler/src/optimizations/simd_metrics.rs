use crate::core::types::MetricsSnapshot;

/// SIMD-accelerated metrics calculations for high-performance monitoring
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
pub fn simd_calculate_percentiles(latencies: &[f64]) -> MetricsSnapshot {
    use std::arch::x86_64::*;
    
    if latencies.is_empty() {
        return MetricsSnapshot {
            count: 0,
            min_ns: 0,
            max_ns: 0,
            mean_ns: 0.0,
            p50_ns: 0,
            p95_ns: 0,
            p99_ns: 0,
            p999_ns: 0,
            calculated_at: crate::optimizations::timestamp::nano_timestamp(),
        };
    }

    let mut sorted_latencies = latencies.to_vec();
    sorted_latencies.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    
    let count = sorted_latencies.len();
    let min_ns = sorted_latencies[0] as u64;
    let max_ns = sorted_latencies[count - 1] as u64;
    
    // SIMD-accelerated sum calculation
    let mean_ns = unsafe { simd_sum(&sorted_latencies) / count as f64 };
    
    // Calculate percentiles
    let p50_ns = sorted_latencies[(count as f64 * 0.50) as usize] as u64;
    let p95_ns = sorted_latencies[(count as f64 * 0.95) as usize] as u64;
    let p99_ns = sorted_latencies[(count as f64 * 0.99) as usize] as u64;
    let p999_ns = sorted_latencies[(count as f64 * 0.999) as usize] as u64;
    
    MetricsSnapshot {
        count: count as u64,
        min_ns,
        max_ns,
        mean_ns,
        p50_ns,
        p95_ns,
        p99_ns,
        p999_ns,
        calculated_at: crate::optimizations::timestamp::nano_timestamp(),
    }
}

/// SIMD-accelerated sum calculation using AVX2
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
unsafe fn simd_sum(values: &[f64]) -> f64 {
    use std::arch::x86_64::*;
    
    let mut sum = _mm256_setzero_pd();
    let len = values.len();
    let chunks = len / 4;
    
    // Process 4 doubles at a time using AVX2
    for i in 0..chunks {
        let chunk_ptr = values.as_ptr().add(i * 4);
        let chunk = _mm256_loadu_pd(chunk_ptr);
        sum = _mm256_add_pd(sum, chunk);
    }
    
    // Extract sum from SIMD register
    let mut result = [0.0; 4];
    _mm256_storeu_pd(result.as_mut_ptr(), sum);
    let simd_sum = result[0] + result[1] + result[2] + result[3];
    
    // Add remaining elements
    let remainder_sum: f64 = values[chunks * 4..].iter().sum();
    
    simd_sum + remainder_sum
}

/// Fallback implementation for non-AVX2 systems or non-x86_64 architectures
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
pub fn simd_calculate_percentiles(latencies: &[f64]) -> MetricsSnapshot {
    if latencies.is_empty() {
        return MetricsSnapshot {
            count: 0,
            min_ns: 0,
            max_ns: 0,
            mean_ns: 0.0,
            p50_ns: 0,
            p95_ns: 0,
            p99_ns: 0,
            p999_ns: 0,
            calculated_at: crate::optimizations::timestamp::nano_timestamp(),
        };
    }

    let mut sorted_latencies = latencies.to_vec();
    sorted_latencies.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    
    let count = sorted_latencies.len();
    let min_ns = sorted_latencies[0] as u64;
    let max_ns = sorted_latencies[count - 1] as u64;
    let mean_ns = sorted_latencies.iter().sum::<f64>() / count as f64;
    
    // Calculate percentiles
    let p50_ns = sorted_latencies[(count as f64 * 0.50) as usize] as u64;
    let p95_ns = sorted_latencies[(count as f64 * 0.95) as usize] as u64;
    let p99_ns = sorted_latencies[(count as f64 * 0.99) as usize] as u64;
    let p999_ns = sorted_latencies[(count as f64 * 0.999) as usize] as u64;
    
    MetricsSnapshot {
        count: count as u64,
        min_ns,
        max_ns,
        mean_ns,
        p50_ns,
        p95_ns,
        p99_ns,
        p999_ns,
        calculated_at: crate::optimizations::timestamp::nano_timestamp(),
    }
}

/// Vectorized operations for bulk processing
pub struct VectorizedMetrics {
    latencies: Vec<f64>,
    capacity: usize,
}

impl VectorizedMetrics {
    pub fn new(capacity: usize) -> Self {
        Self {
            latencies: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn add_latency(&mut self, latency_ns: u64) {
        if self.latencies.len() >= self.capacity {
            // Circular buffer - overwrite oldest
            let index = self.latencies.len() % self.capacity;
            self.latencies[index] = latency_ns as f64;
        } else {
            self.latencies.push(latency_ns as f64);
        }
    }

    pub fn calculate_snapshot(&self) -> MetricsSnapshot {
        simd_calculate_percentiles(&self.latencies)
    }

    pub fn reset(&mut self) {
        self.latencies.clear();
    }
}

/// Optimized histogram for latency distribution
pub struct LatencyHistogram {
    buckets: Vec<u64>,
    bucket_bounds: Vec<u64>, // in nanoseconds
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        // Logarithmic buckets for latency distribution
        let bucket_bounds = vec![
            1_000,      // 1 μs
            5_000,      // 5 μs
            10_000,     // 10 μs
            50_000,     // 50 μs
            100_000,    // 100 μs
            500_000,    // 500 μs
            1_000_000,  // 1 ms
            5_000_000,  // 5 ms
            10_000_000, // 10 ms
            50_000_000, // 50 ms
            100_000_000,// 100 ms
            u64::MAX,   // > 100 ms
        ];
        
        let buckets = vec![0; bucket_bounds.len()];
        
        Self {
            buckets,
            bucket_bounds,
        }
    }

    pub fn record(&mut self, latency_ns: u64) {
        for (i, &bound) in self.bucket_bounds.iter().enumerate() {
            if latency_ns <= bound {
                self.buckets[i] += 1;
                break;
            }
        }
    }

    pub fn get_distribution(&self) -> Vec<(u64, u64)> {
        self.bucket_bounds.iter().zip(self.buckets.iter())
            .map(|(&bound, &count)| (bound, count))
            .collect()
    }

    pub fn reset(&mut self) {
        self.buckets.iter_mut().for_each(|bucket| *bucket = 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── simd_calculate_percentiles ──────────────────────────────

    #[test]
    fn test_percentiles_empty() {
        let snap = simd_calculate_percentiles(&[]);
        assert_eq!(snap.count, 0);
        assert_eq!(snap.min_ns, 0);
        assert_eq!(snap.max_ns, 0);
    }

    #[test]
    fn test_percentiles_single_element() {
        let snap = simd_calculate_percentiles(&[5000.0]);
        assert_eq!(snap.count, 1);
        assert_eq!(snap.min_ns, 5000);
        assert_eq!(snap.max_ns, 5000);
    }

    #[test]
    fn test_percentiles_known_distribution() {
        let latencies: Vec<f64> = (1..=100).map(|i| i as f64 * 1000.0).collect();
        let snap = simd_calculate_percentiles(&latencies);
        assert_eq!(snap.count, 100);
        assert_eq!(snap.min_ns, 1000);
        assert_eq!(snap.max_ns, 100_000);
        // p50 of 1..=100 (sorted) at index 50 → 51_000
        assert_eq!(snap.p50_ns, 51_000);
        assert_eq!(snap.p95_ns, 96_000);
        assert_eq!(snap.p99_ns, 100_000);
    }

    #[test]
    fn test_percentiles_mean_accuracy() {
        let latencies = vec![100.0, 200.0, 300.0, 400.0];
        let snap = simd_calculate_percentiles(&latencies);
        assert!((snap.mean_ns - 250.0).abs() < 1.0);
    }

    // ── VectorizedMetrics ───────────────────────────────────────

    #[test]
    fn test_vectorized_metrics_new_empty() {
        let vm = VectorizedMetrics::new(100);
        let snap = vm.calculate_snapshot();
        assert_eq!(snap.count, 0);
    }

    #[test]
    fn test_vectorized_metrics_add_and_snapshot() {
        let mut vm = VectorizedMetrics::new(100);
        vm.add_latency(1000);
        vm.add_latency(2000);
        vm.add_latency(3000);
        let snap = vm.calculate_snapshot();
        assert_eq!(snap.count, 3);
        assert_eq!(snap.min_ns, 1000);
        assert_eq!(snap.max_ns, 3000);
    }

    #[test]
    fn test_vectorized_metrics_reset() {
        let mut vm = VectorizedMetrics::new(100);
        vm.add_latency(1000);
        vm.reset();
        let snap = vm.calculate_snapshot();
        assert_eq!(snap.count, 0);
    }

    // ── LatencyHistogram ────────────────────────────────────────

    #[test]
    fn test_histogram_new_empty() {
        let h = LatencyHistogram::new();
        let dist = h.get_distribution();
        assert_eq!(dist.len(), 12); // 12 buckets
        assert!(dist.iter().all(|&(_, count)| count == 0));
    }

    #[test]
    fn test_histogram_record_buckets() {
        let mut h = LatencyHistogram::new();
        h.record(500);    // ≤ 1_000 → bucket 0
        h.record(8_000);  // ≤ 10_000 → bucket 2
        h.record(200_000_000); // > 100ms → bucket 11 (u64::MAX)

        let dist = h.get_distribution();
        assert_eq!(dist[0].1, 1); // ≤ 1μs
        assert_eq!(dist[2].1, 1); // ≤ 10μs
        assert_eq!(dist[11].1, 1); // overflow
    }

    #[test]
    fn test_histogram_reset() {
        let mut h = LatencyHistogram::new();
        h.record(500);
        h.reset();
        let dist = h.get_distribution();
        assert!(dist.iter().all(|&(_, count)| count == 0));
    }

    #[test]
    fn test_histogram_default() {
        let h = LatencyHistogram::default();
        assert_eq!(h.get_distribution().len(), 12);
    }
}
