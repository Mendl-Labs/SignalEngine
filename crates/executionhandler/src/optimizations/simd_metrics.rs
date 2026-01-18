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
