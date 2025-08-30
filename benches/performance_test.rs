use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

// Import SignalEngine components
use smartorderrouter::{UltraFastSmartOrderRouter, ExecutionUrgency, RoutingAlgorithm};
use signalgenerator::UltraFastSignalGenerator;
use signaldispatcher::UltraFastSignalDispatcher;
use executionhandler::UltraLowLatencyExecutionHandler;
use strategyhandler::UltraStrategyEngine;
use ultra_signal::{Signal, SignalAction};

// Performance benchmark for SmartOrderRouter
fn bench_smart_order_router(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("smart_order_router_50x", |b| {
        b.iter(|| {
            rt.block_on(async {
                let router = UltraFastSmartOrderRouter::new();
                
                // Create test signal
                let signal = Signal {
                    id: "test-signal-1".to_string(),
                    symbol: "BTC/USD".to_string(),
                    action: SignalAction::Buy,
                    quantity: 1.0,
                    price: Some(50000.0),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    exchange: "Kraken".to_string(),
                    strategy: "momentum".to_string(),
                    confidence: 0.85,
                    metadata: std::collections::HashMap::new(),
                };
                
                // Benchmark ultra-fast routing
                let start = Instant::now();
                let _result = router.route_order_ultra_fast(
                    black_box(&signal), 
                    black_box(ExecutionUrgency::Immediate)
                ).await;
                let duration = start.elapsed();
                
                // Ensure sub-100μs performance
                assert!(duration.as_micros() < 100);
                duration
            })
        })
    });
}

// Performance benchmark for SignalGenerator
fn bench_signal_generator(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("signal_generator_45x", |b| {
        b.iter(|| {
            rt.block_on(async {
                let generator = UltraFastSignalGenerator::new();
                
                // Create test market data
                let market_data = vec![
                    ("BTC/USD".to_string(), 50000.0, 1000.0),
                    ("ETH/USD".to_string(), 3000.0, 500.0),
                    ("ADA/USD".to_string(), 1.50, 10000.0),
                ];
                
                // Benchmark SIMD signal generation
                let start = Instant::now();
                let _signals = generator.generate_momentum_signals_simd(
                    black_box(&market_data)
                ).await;
                let duration = start.elapsed();
                
                // Ensure sub-50μs performance
                assert!(duration.as_micros() < 50);
                duration
            })
        })
    });
}

// Performance benchmark for SignalDispatcher  
fn bench_signal_dispatcher(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("signal_dispatcher_4x", |b| {
        b.iter(|| {
            rt.block_on(async {
                let dispatcher = UltraFastSignalDispatcher::new();
                
                // Create batch of test signals
                let signals = vec![
                    Signal {
                        id: "signal-1".to_string(),
                        symbol: "BTC/USD".to_string(),
                        action: SignalAction::Buy,
                        quantity: 1.0,
                        price: Some(50000.0),
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64,
                        exchange: "Kraken".to_string(),
                        strategy: "momentum".to_string(),
                        confidence: 0.85,
                        metadata: std::collections::HashMap::new(),
                    };
                    100 // Batch of 100 signals
                ];
                
                // Benchmark SIMD batch dispatch
                let start = Instant::now();
                let _result = dispatcher.dispatch_batch_simd(
                    black_box(&signals)
                ).await;
                let duration = start.elapsed();
                
                // Ensure sub-200μs performance for batch
                assert!(duration.as_micros() < 200);
                duration
            })
        })
    });
}

// Performance benchmark for ExecutionHandler
fn bench_execution_handler(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("execution_handler_10x", |b| {
        b.iter(|| {
            rt.block_on(async {
                let handler = UltraLowLatencyExecutionHandler::new();
                
                // Create test signal
                let signal = Signal {
                    id: "exec-test-1".to_string(),
                    symbol: "BTC/USD".to_string(),
                    action: SignalAction::Buy,
                    quantity: 1.0,
                    price: Some(50000.0),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    exchange: "Kraken".to_string(),
                    strategy: "momentum".to_string(),
                    confidence: 0.85,
                    metadata: std::collections::HashMap::new(),
                };
                
                // Benchmark ultra-low latency execution
                let start = Instant::now();
                let _result = handler.execute_order_ultra_fast(
                    black_box(&signal)
                ).await;
                let duration = start.elapsed();
                
                // Ensure sub-500μs performance
                assert!(duration.as_micros() < 500);
                duration
            })
        })
    });
}

// Performance benchmark for StrategyHandler
fn bench_strategy_handler(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("strategy_handler_25x", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = UltraStrategyEngine::new();
                
                // Create test market data
                let market_data = vec![
                    ("BTC/USD".to_string(), 50000.0, 1000.0),
                    ("ETH/USD".to_string(), 3000.0, 500.0),
                ];
                
                // Benchmark ultra-fast strategy execution
                let start = Instant::now();
                let _signals = engine.execute_strategies_parallel(
                    black_box(&market_data)
                ).await;
                let duration = start.elapsed();
                
                // Ensure sub-300μs performance
                assert!(duration.as_micros() < 300);
                duration
            })
        })
    });
}

// End-to-end performance benchmark
fn bench_end_to_end_pipeline(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("end_to_end_sub_millisecond", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Initialize all components
                let strategy_engine = UltraStrategyEngine::new();
                let signal_generator = UltraFastSignalGenerator::new();
                let signal_dispatcher = UltraFastSignalDispatcher::new();
                let smart_router = UltraFastSmartOrderRouter::new();
                let execution_handler = UltraLowLatencyExecutionHandler::new();
                
                // Create test market data
                let market_data = vec![
                    ("BTC/USD".to_string(), 50000.0, 1000.0)
                ];
                
                // Measure complete end-to-end pipeline
                let start = Instant::now();
                
                // Step 1: Strategy execution (300μs target)
                let _strategy_signals = strategy_engine.execute_strategies_parallel(
                    black_box(&market_data)
                ).await.unwrap_or_default();
                
                // Step 2: Signal generation (50μs target)
                let signals = signal_generator.generate_momentum_signals_simd(
                    black_box(&market_data)
                ).await.unwrap_or_default();
                
                // Step 3: Signal dispatch (200μs target)
                let _dispatch_result = signal_dispatcher.dispatch_batch_simd(
                    black_box(&signals)
                ).await;
                
                // Step 4: Smart routing (100μs target)
                if let Some(signal) = signals.first() {
                    let _route_result = smart_router.route_order_ultra_fast(
                        black_box(signal), 
                        black_box(ExecutionUrgency::Immediate)
                    ).await;
                    
                    // Step 5: Execution (500μs target)
                    let _exec_result = execution_handler.execute_order_ultra_fast(
                        black_box(signal)
                    ).await;
                }
                
                let total_duration = start.elapsed();
                
                // Ensure sub-millisecond end-to-end performance
                assert!(total_duration.as_micros() < 1000);
                total_duration
            })
        })
    });
}

criterion_group!(
    benches,
    bench_smart_order_router,
    bench_signal_generator,
    bench_signal_dispatcher,
    bench_execution_handler,
    bench_strategy_handler,
    bench_end_to_end_pipeline
);

criterion_main!(benches);
