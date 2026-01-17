//! Latency Benchmarks for SignalEngine Execution Handler
//!
//! Criterion benchmarks measuring hot path latency for:
//! - Circuit breaker state checks
//! - Rate limiter token acquisition
//! - Order execution pipeline
//! - DLQ operations

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;

// Import from the executionhandler crate
use executionhandler::circuit_breaker_v2::{AtomicCircuitBreaker, CircuitBreakerConfig};
use executionhandler::rate_limiter::{AtomicTokenBucket, RateLimiterConfig, SlidingWindowLimiter};

fn bench_circuit_breaker_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("circuit_breaker");
    
    // Closed state (hot path - should be fastest)
    let cb_closed = AtomicCircuitBreaker::new(CircuitBreakerConfig::default());
    group.bench_function("get_state_closed", |b| {
        b.iter(|| {
            black_box(cb_closed.get_state())
        });
    });
    
    // Can execute check (includes state transition logic)
    group.bench_function("can_execute_closed", |b| {
        b.iter(|| {
            black_box(cb_closed.can_execute())
        });
    });
    
    // Open state check
    let cb_open = AtomicCircuitBreaker::new(CircuitBreakerConfig::default());
    for _ in 0..10 {
        cb_open.record_failure();
    }
    group.bench_function("get_state_open", |b| {
        b.iter(|| {
            black_box(cb_open.get_state())
        });
    });
    
    // Record success (CAS operation)
    let cb_success = AtomicCircuitBreaker::new(CircuitBreakerConfig::default());
    group.bench_function("record_success", |b| {
        b.iter(|| {
            cb_success.record_success();
        });
    });
    
    // Record failure (CAS operation + potential state transition)
    let cb_failure = AtomicCircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 1000, // High threshold to avoid opening
        ..Default::default()
    });
    group.bench_function("record_failure", |b| {
        b.iter(|| {
            cb_failure.record_failure();
        });
    });
    
    group.finish();
}

fn bench_token_bucket_rate_limiter(c: &mut Criterion) {
    let mut group = c.benchmark_group("token_bucket");
    
    // Full bucket - should always succeed
    let limiter_full = AtomicTokenBucket::new(RateLimiterConfig {
        rate_per_second: 1000,
        burst_size: 1000,
        ..Default::default()
    });
    group.bench_function("try_acquire_1_full", |b| {
        b.iter(|| {
            black_box(limiter_full.try_acquire())
        });
    });
    
    // Empty bucket - should fail fast
    let limiter_empty = AtomicTokenBucket::new(RateLimiterConfig {
        rate_per_second: 1,
        burst_size: 1,
        ..Default::default()
    });
    // Drain the bucket
    let _ = limiter_empty.try_acquire();
    group.bench_function("try_acquire_1_empty", |b| {
        b.iter(|| {
            black_box(limiter_empty.try_acquire())
        });
    });
    
    // Multiple token acquisition
    let limiter_multi = AtomicTokenBucket::new(RateLimiterConfig {
        rate_per_second: 10000,
        burst_size: 10000,
        ..Default::default()
    });
    for tokens in [1u32, 5, 10, 50] {
        group.bench_with_input(BenchmarkId::new("try_acquire_n", tokens), &tokens, |b, &n| {
            b.iter(|| {
                black_box(limiter_multi.try_acquire_n(n))
            });
        });
    }
    
    // Utilization check
    group.bench_function("utilization", |b| {
        b.iter(|| {
            black_box(limiter_multi.utilization())
        });
    });
    
    group.finish();
}

fn bench_sliding_window_limiter(c: &mut Criterion) {
    let mut group = c.benchmark_group("sliding_window");
    
    let limiter = SlidingWindowLimiter::new(RateLimiterConfig {
        rate_per_second: 10000,
        burst_size: 10000,
        window_segments: 60,
        ..Default::default()
    });
    
    group.bench_function("try_acquire", |b| {
        b.iter(|| {
            black_box(limiter.try_acquire())
        });
    });
    
    group.bench_function("current_rate", |b| {
        b.iter(|| {
            black_box(limiter.current_rate())
        });
    });
    
    group.finish();
}

fn bench_concurrent_circuit_breaker(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_circuit_breaker");
    
    let cb = Arc::new(AtomicCircuitBreaker::new(CircuitBreakerConfig::default()));
    
    // Concurrent reads from multiple threads
    for num_threads in [2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("concurrent_check", num_threads),
            &num_threads,
            |b, &n| {
                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|_| {
                            let cb = cb.clone();
                            std::thread::spawn(move || {
                                for _ in 0..100 {
                                    black_box(cb.can_execute());
                                }
                            })
                        })
                        .collect();
                    
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    
    // Concurrent reads and writes
    let cb_mixed = Arc::new(AtomicCircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 10000, // High to prevent opening
        ..Default::default()
    }));
    
    group.bench_function("concurrent_read_write", |b| {
        b.iter(|| {
            let cb = cb_mixed.clone();
            let handles: Vec<_> = (0..4)
                .map(|i| {
                    let cb = cb.clone();
                    std::thread::spawn(move || {
                        for j in 0..100 {
                            if i % 2 == 0 {
                                black_box(cb.can_execute());
                            } else if j % 3 == 0 {
                                cb.record_failure();
                            } else {
                                cb.record_success();
                            }
                        }
                    })
                })
                .collect();
            
            for h in handles {
                h.join().unwrap();
            }
        });
    });
    
    group.finish();
}

fn bench_concurrent_rate_limiter(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_rate_limiter");
    
    let limiter = Arc::new(AtomicTokenBucket::new(RateLimiterConfig {
        rate_per_second: 100000,
        burst_size: 100000,
        ..Default::default()
    }));
    
    for num_threads in [2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("concurrent_acquire", num_threads),
            &num_threads,
            |b, &n| {
                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|_| {
                            let limiter = limiter.clone();
                            std::thread::spawn(move || {
                                for _ in 0..100 {
                                    black_box(limiter.try_acquire());
                                }
                            })
                        })
                        .collect();
                    
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_circuit_breaker_check,
    bench_token_bucket_rate_limiter,
    bench_sliding_window_limiter,
    bench_concurrent_circuit_breaker,
    bench_concurrent_rate_limiter,
);

criterion_main!(benches);
