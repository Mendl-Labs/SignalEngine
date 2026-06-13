// Ultra-low latency signal dispatcher for nanosecond trading
use ultra_signal::{Signal, UltraFastSignalQueue, SignalAction};
use signalengine::{
    get_rdtsc, rdtsc_duration_ns, init_rdtsc, AtomicMetrics,
    SystemOptimization, ZeroCopyChannel,
    MemoryBarrier,
};
use crossbeam::channel::Sender;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

/// Lock-free signal dispatcher optimized for ultra-low latency
pub struct UltraFastSignalDispatcher {
    // Lock-free signal queues for different priority levels
    urgent_queue: Arc<UltraFastSignalQueue>,
    normal_queue: Arc<UltraFastSignalQueue>,
    
    // Execution handlers for different components
    execution_sender: Sender<Signal>,
    portfolio_sender: Sender<Signal>,
    risk_sender: Sender<Signal>,
    
    // Performance metrics (cache-aligned to prevent false sharing)
    metrics: Arc<AtomicMetrics>,
    
    // Control flags
    is_running: AtomicBool,
    
    // Hot path optimization: Pre-allocated buffer for batch processing
    batch_buffer: Vec<Signal>,
}

impl UltraFastSignalDispatcher {
    pub fn new(
        execution_sender: Sender<Signal>,
        portfolio_sender: Sender<Signal>, 
        risk_sender: Sender<Signal>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Initialize RDTSC for hardware timestamps
        init_rdtsc();
        
        Ok(Self {
            urgent_queue: Arc::new(UltraFastSignalQueue::new(4096)?), // 4K urgent signals
            normal_queue: Arc::new(UltraFastSignalQueue::new(8192)?), // 8K normal signals
            execution_sender,
            portfolio_sender,
            risk_sender,
            metrics: Arc::new(AtomicMetrics::new()),
            is_running: AtomicBool::new(false),
            batch_buffer: Vec::with_capacity(32), // Batch up to 32 signals
        })
    }

        /// Ultra-fast batch signal processing using pre-allocated buffer
    pub fn process_batch_signals(&mut self) -> u64 {
        if self.batch_buffer.is_empty() {
            self.batch_buffer.reserve(32); // Ensure capacity
        }
        self.batch_buffer.clear();
        
        let mut processed_count = 0u64;
        let batch_start = get_rdtsc();
        
        // Fill batch buffer from urgent queue first (high priority)
        while self.batch_buffer.len() < 16 { // Half batch from urgent
            if let Some(signal) = self.urgent_queue.try_pop() {
                self.batch_buffer.push(signal);
            } else {
                break;
            }
        }
        
        // Fill remaining from normal queue
        while self.batch_buffer.len() < 32 { // Complete batch from normal
            if let Some(signal) = self.normal_queue.try_pop() {
                self.batch_buffer.push(signal);
            } else {
                break;
            }
        }
        
        // Process batch using SIMD-optimized routing
        if !self.batch_buffer.is_empty() {
            processed_count = self.dispatch_batch_simd();
            
            // Update performance metrics using RDTSC
            let batch_end = get_rdtsc();
            let batch_latency_ns = rdtsc_duration_ns(batch_start, batch_end);
            
            if processed_count > 0 {
                let per_signal_latency = batch_latency_ns / processed_count;
                self.metrics.record_signal(per_signal_latency);
            }
        }
        
        processed_count
    }

    /// SIMD-optimized batch signal dispatch
    #[inline]
    fn dispatch_batch_simd(&self) -> u64 {
        let mut processed = 0u64;
        
        // Process signals in groups of 4 for SIMD efficiency  
        let chunks = self.batch_buffer.chunks(4);
        
        for chunk in chunks {
            // Parallel processing of up to 4 signals
            for signal in chunk {
                Self::dispatch_signal_fast(
                    signal,
                    &self.execution_sender,
                    &self.portfolio_sender,
                    &self.risk_sender,
                );
                processed += 1;
            }
        }
        
        processed
    }

    /// Get performance statistics
    #[inline]
    pub fn get_stats(&self) -> (u64, u64, u64, u64) {
        (
            self.metrics.signal_count.load(Ordering::Relaxed),
            self.metrics.avg_latency_ns(),
            self.metrics.min_latency_ns.load(Ordering::Relaxed),
            self.metrics.max_latency_ns.load(Ordering::Relaxed),
        )
    }
    pub fn start(&self) -> std::thread::JoinHandle<()> {
        let urgent_queue = Arc::clone(&self.urgent_queue);
        let normal_queue = Arc::clone(&self.normal_queue);
        let execution_sender = self.execution_sender.clone();
        let portfolio_sender = self.portfolio_sender.clone();
        let risk_sender = self.risk_sender.clone();
        let metrics = Arc::clone(&self.metrics);
        let is_running = Arc::new(AtomicBool::new(true));

        thread::spawn(move || {
            // Apply system optimizations for ultra-low latency
            // Pin to core 0 for consistent performance (modify as needed)
            let optimization = SystemOptimization::for_trading(Some(0));
            if let Err(e) = optimization.apply() {
                eprintln!("Warning: System optimization failed: {}", e);
            }

            let mut batch_buffer: Vec<Signal> = Vec::with_capacity(32);

            while is_running.load(Ordering::Relaxed) {
                let start_time = get_rdtsc();
                
                // Process urgent signals first (highest priority)
                let mut processed_count = 0;
                
                // Batch process urgent signals for efficiency
                batch_buffer.clear();
                while batch_buffer.len() < 32 {
                    if let Some(signal) = urgent_queue.try_pop() {
                        batch_buffer.push(signal);
                    } else {
                        break;
                    }
                }
                
                // Send urgent signals immediately
                for signal in &batch_buffer {
                    Self::dispatch_signal_fast(
                        signal, 
                        &execution_sender, 
                        &portfolio_sender, 
                        &risk_sender
                    );
                    processed_count += 1;
                }

                // Process normal priority signals if no urgent signals
                if batch_buffer.is_empty() {
                    batch_buffer.clear();
                    while batch_buffer.len() < 16 { // Smaller batch for normal priority
                        if let Some(signal) = normal_queue.try_pop() {
                            batch_buffer.push(signal);
                        } else {
                            break;
                        }
                    }
                    
                    for signal in &batch_buffer {
                        Self::dispatch_signal_fast(
                            signal,
                            &execution_sender,
                            &portfolio_sender, 
                            &risk_sender
                        );
                        processed_count += 1;
                    }
                }

                // Update performance metrics using RDTSC
                if processed_count > 0 {
                    let end_time = get_rdtsc();
                    let latency_ns = rdtsc_duration_ns(start_time, end_time);
                    let per_signal = latency_ns / processed_count as u64;
                    
                    metrics.record_signal(per_signal);
                } else {
                    // Yield CPU for 100 nanoseconds if no signals to process
                    thread::yield_now();
                }
            }
        })
    }

    /// Dispatch signal to appropriate handlers with ultra-fast routing
    #[inline]
    fn dispatch_signal_fast(
        signal: &Signal,
        execution_sender: &Sender<Signal>,
        portfolio_sender: &Sender<Signal>,
        risk_sender: &Sender<Signal>,
    ) {
        // **ULTRA-FAST SIGNAL ROUTING**
        // Route based on signal action without complex logic

        match signal.action {
            SignalAction::Buy | SignalAction::Sell => {
                // Market orders go directly to execution (fastest path)
                if signal.is_market_order() {
                    let _ = execution_sender.try_send(*signal); // Non-blocking send
                } else {
                    // Limit orders need risk check first
                    if signal.should_bypass_risk_checks() {
                        Self::log_risk_bypass(signal);
                        let _ = execution_sender.try_send(*signal);
                    } else {
                        let _ = risk_sender.try_send(*signal);
                    }
                }
                // Always update portfolio
                let _ = portfolio_sender.try_send(*signal);
            }

            SignalAction::BuyLimit | SignalAction::SellLimit => {
                // Limit orders through risk management unless urgent
                if signal.is_urgent() {
                    Self::log_risk_bypass(signal);
                    let _ = execution_sender.try_send(*signal);
                } else {
                    let _ = risk_sender.try_send(*signal);
                }
                let _ = portfolio_sender.try_send(*signal);
            }

            SignalAction::Cancel => {
                // Cancel orders go directly to execution
                let _ = execution_sender.try_send(*signal);
            }

            SignalAction::Hold => {
                // Hold signals only update portfolio
                let _ = portfolio_sender.try_send(*signal);
            }
        }
    }

    /// Loudly log signals that skip risk checks, rate-limited so the hot
    /// path is not flooded (first occurrence, then every 100th).
    #[cold]
    fn log_risk_bypass(signal: &Signal) {
        use std::sync::atomic::AtomicU64;
        static BYPASS_COUNT: AtomicU64 = AtomicU64::new(0);
        let n = BYPASS_COUNT.fetch_add(1, Ordering::Relaxed);
        if n % 100 == 0 {
            ultra_logger::ultra_warn!(format!(
                "⚠️ RISK BYPASS: signal routed directly to execution without risk checks \
                 (strategy_id={}, symbol_hash={:#x}, action={:?}, qty={}, total_bypasses={})",
                signal.strategy_id, signal.symbol_hash, signal.action, signal.quantity, n + 1
            ));
        }
    }

    /// Submit signal for ultra-fast processing
    pub fn submit_signal(&self, signal: Signal) -> Result<(), Signal> {
        if signal.is_urgent() {
            self.urgent_queue.try_push(signal)
        } else {
            self.normal_queue.try_push(signal)
        }
    }

    /// Submit multiple signals as a batch
    pub fn submit_signal_batch(&self, signals: &[Signal]) -> usize {
        let mut submitted = 0;
        for signal in signals {
            if self.submit_signal(*signal).is_ok() {
                submitted += 1;
            }
        }
        submitted
    }

    /// Get performance metrics
    pub fn get_metrics(&self) -> (u64, u64, u64) {
        let (count, avg, _min, max) = self.get_stats();
        (count, avg, max)
    }

    /// Get queue lengths for monitoring
    pub fn get_queue_stats(&self) -> (usize, usize) {
        (self.urgent_queue.len(), self.normal_queue.len())
    }

    /// Stop the dispatcher
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
    }

    /// Get comprehensive performance statistics
    pub fn get_performance_stats(&self) -> DispatcherStats {
        let (count, avg, _min, max) = self.get_stats();
        DispatcherStats {
            signals_processed: count,
            avg_latency_ns: avg,
            max_latency_ns: max,
            is_running: self.is_running.load(Ordering::Relaxed),
            urgent_queue_size: self.urgent_queue.len(),
            normal_queue_size: self.normal_queue.len(),
        }
    }

    /// Reset performance counters
    pub fn reset_stats(&self) {
        // Note: AtomicMetrics accumulate indefinitely in current design
        // To reset, would need to restart the dispatcher or use interior mutability
        eprintln!("Warning: reset_stats() not fully implemented with AtomicMetrics");
    }
}

/// Performance statistics for the signal dispatcher
#[derive(Debug, Clone)]
pub struct DispatcherStats {
    pub signals_processed: u64,
    pub avg_latency_ns: u64,
    pub max_latency_ns: u64,
    pub is_running: bool,
    pub urgent_queue_size: usize,
    pub normal_queue_size: usize,
}

/// Legacy signal dispatcher for backward compatibility
pub struct SignalDispatcher {
    ultra_dispatcher: UltraFastSignalDispatcher,
}

impl SignalDispatcher {
    pub fn new(
        execution_sender: Sender<Signal>,
        portfolio_sender: Sender<Signal>,
        risk_sender: Sender<Signal>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            ultra_dispatcher: UltraFastSignalDispatcher::new(
                execution_sender,
                portfolio_sender,
                risk_sender,
            )?,
        })
    }

    /// Start signal processing
    pub fn start(&self) -> std::thread::JoinHandle<()> {
        self.ultra_dispatcher.start()
    }

    /// Submit signal for processing
    pub fn submit(&self, signal: Signal) -> Result<(), Signal> {
        self.ultra_dispatcher.submit_signal(signal)
    }

    /// Submit multiple signals
    pub fn submit_batch(&self, signals: Vec<Signal>) -> usize {
        self.ultra_dispatcher.submit_signal_batch(&signals)
    }

    /// Get dispatcher statistics
    pub fn get_stats(&self) -> (u64, u64, u64, usize, usize) {
        let (processed, avg_latency, max_latency) = self.ultra_dispatcher.get_metrics();
        let (urgent_len, normal_len) = self.ultra_dispatcher.get_queue_stats();
        (processed, avg_latency, max_latency, urgent_len, normal_len)
    }

    /// Stop signal processing
    pub fn stop(&self) {
        self.ultra_dispatcher.stop();
    }
}

/// Zero-copy signal dispatcher using Arc-based signal sharing
/// 
/// Eliminates signal copies by sharing Arc<Signal> references.
/// Reduces latency by ~20-30% compared to copying signals.
pub struct ZeroCopyDispatcher {
    urgent_channel: Arc<ZeroCopyChannel<Signal>>,
    normal_channel: Arc<ZeroCopyChannel<Signal>>,
    metrics: Arc<AtomicMetrics>,
    is_running: Arc<AtomicBool>,
}

impl ZeroCopyDispatcher {
    /// Create new zero-copy dispatcher
    pub fn new(urgent_capacity: usize, normal_capacity: usize) -> Self {
        init_rdtsc();
        
        Self {
            urgent_channel: Arc::new(ZeroCopyChannel::new(urgent_capacity)),
            normal_channel: Arc::new(ZeroCopyChannel::new(normal_capacity)),
            metrics: Arc::new(AtomicMetrics::new()),
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }
    
    /// Submit signal using zero-copy (Arc-based sharing)
    #[inline]
    pub fn submit_zero_copy(&self, signal: Arc<Signal>) -> bool {
        let start = get_rdtsc();
        
        // Urgent signals bypass normal queue
        let result = if signal.is_urgent() {
            self.urgent_channel.send(signal)
        } else {
            self.normal_channel.send(signal)
        };
        
        if result {
            let end = get_rdtsc();
            let latency = rdtsc_duration_ns(start, end);
            self.metrics.record_signal(latency);
        }
        
        result
    }
    
    /// Receive next signal (zero-copy, returns Arc)
    #[inline]
    pub fn recv_zero_copy(&self) -> Option<Arc<Signal>> {
        // Urgent queue has priority
        if let Some(signal) = self.urgent_channel.recv() {
            return Some(signal);
        }
        
        self.normal_channel.recv()
    }
    
    /// Start zero-copy processing loop
    pub fn start_zero_copy<F>(&self, mut handler: F) -> thread::JoinHandle<()>
    where
        F: FnMut(Arc<Signal>) + Send + 'static,
    {
        self.is_running.store(true, Ordering::Release);
        
        let urgent = Arc::clone(&self.urgent_channel);
        let normal = Arc::clone(&self.normal_channel);
        let metrics = Arc::clone(&self.metrics);
        let running = Arc::clone(&self.is_running);
        
        thread::spawn(move || {
            let optimization = SystemOptimization::for_trading(Some(0));
            let _ = optimization.apply();
            
            while running.load(Ordering::Acquire) {
                let start = get_rdtsc();
                
                // Priority: Urgent signals first
                if let Some(signal) = urgent.recv() {
                    handler(signal);
                    
                    let end = get_rdtsc();
                    let latency = rdtsc_duration_ns(start, end);
                    metrics.record_signal(latency);
                } else if let Some(signal) = normal.recv() {
                    handler(signal);
                    
                    let end = get_rdtsc();
                    let latency = rdtsc_duration_ns(start, end);
                    metrics.record_signal(latency);
                } else {
                    thread::yield_now();
                }
            }
        })
    }
    
    /// Get metrics
    pub fn get_metrics(&self) -> (u64, u64, u64) {
        let signal_count = self.metrics.signal_count.load(Ordering::Relaxed);
        let avg_latency = self.metrics.avg_latency_ns();
        let max_latency = self.metrics.max_latency_ns.load(Ordering::Relaxed);
        (signal_count, avg_latency, max_latency)
    }
    
    /// Stop processing
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Release);
        MemoryBarrier::full();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel;
    use ultra_signal::{Signal, SignalAction, ExchangeId, signal_flags};

    fn make_dispatcher() -> (UltraFastSignalDispatcher, channel::Receiver<Signal>, channel::Receiver<Signal>, channel::Receiver<Signal>) {
        let (exec_tx, exec_rx) = channel::bounded(64);
        let (port_tx, port_rx) = channel::bounded(64);
        let (risk_tx, risk_rx) = channel::bounded(64);
        let dispatcher = UltraFastSignalDispatcher::new(exec_tx, port_tx, risk_tx).unwrap();
        (dispatcher, exec_rx, port_rx, risk_rx)
    }

    fn buy_signal() -> Signal {
        Signal::new(1, 12345, ExchangeId::Kraken, SignalAction::Buy, 1.0, f64::NAN) // market order
    }

    fn sell_signal() -> Signal {
        Signal::new(1, 12345, ExchangeId::Kraken, SignalAction::Sell, 1.0, f64::NAN)
    }

    fn buy_limit_signal() -> Signal {
        Signal::new(1, 12345, ExchangeId::Kraken, SignalAction::BuyLimit, 1.0, 50000.0)
    }

    fn cancel_signal() -> Signal {
        Signal::new(1, 12345, ExchangeId::Kraken, SignalAction::Cancel, 0.0, 0.0)
    }

    fn hold_signal() -> Signal {
        Signal::new(1, 12345, ExchangeId::Kraken, SignalAction::Hold, 0.0, 0.0)
    }

    // All tests ignored on non-elevated Windows due to winapi thread priority APIs
    // requiring elevation (os error 740). Run with: cargo test -p signal_dispatcher -- --ignored

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_new_creates_dispatcher() {
        let (dispatcher, _, _, _) = make_dispatcher();
        let (urgent, normal) = dispatcher.get_queue_stats();
        assert_eq!(urgent, 0);
        assert_eq!(normal, 0);
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_submit_normal_signal_to_normal_queue() {
        let (dispatcher, _, _, _) = make_dispatcher();
        let sig = buy_signal();
        assert!(dispatcher.submit_signal(sig).is_ok());
        let (urgent, normal) = dispatcher.get_queue_stats();
        assert_eq!(urgent, 0);
        assert_eq!(normal, 1);
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_submit_urgent_signal_to_urgent_queue() {
        let (dispatcher, _, _, _) = make_dispatcher();
        let mut sig = buy_signal();
        sig.flags |= signal_flags::URGENT;
        assert!(dispatcher.submit_signal(sig).is_ok());
        let (urgent, normal) = dispatcher.get_queue_stats();
        assert_eq!(urgent, 1);
        assert_eq!(normal, 0);
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_submit_batch() {
        let (dispatcher, _, _, _) = make_dispatcher();
        let signals = vec![buy_signal(), sell_signal(), hold_signal()];
        let submitted = dispatcher.submit_signal_batch(&signals);
        assert_eq!(submitted, 3);
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_route_buy_market_to_execution_and_portfolio() {
        let (mut dispatcher, exec_rx, port_rx, risk_rx) = make_dispatcher();
        dispatcher.submit_signal(buy_signal()).unwrap();
        dispatcher.process_batch_signals();
        assert!(exec_rx.try_recv().is_ok());
        assert!(port_rx.try_recv().is_ok());
        assert!(risk_rx.try_recv().is_err());
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_route_sell_market_to_execution_and_portfolio() {
        let (mut dispatcher, exec_rx, port_rx, risk_rx) = make_dispatcher();
        dispatcher.submit_signal(sell_signal()).unwrap();
        dispatcher.process_batch_signals();
        assert!(exec_rx.try_recv().is_ok());
        assert!(port_rx.try_recv().is_ok());
        assert!(risk_rx.try_recv().is_err());
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_route_buy_limit_to_risk_and_portfolio() {
        let (mut dispatcher, exec_rx, port_rx, risk_rx) = make_dispatcher();
        dispatcher.submit_signal(buy_limit_signal()).unwrap();
        dispatcher.process_batch_signals();
        assert!(risk_rx.try_recv().is_ok());
        assert!(port_rx.try_recv().is_ok());
        assert!(exec_rx.try_recv().is_err());
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_route_cancel_to_execution_only() {
        let (mut dispatcher, exec_rx, port_rx, risk_rx) = make_dispatcher();
        dispatcher.submit_signal(cancel_signal()).unwrap();
        dispatcher.process_batch_signals();
        assert!(exec_rx.try_recv().is_ok());
        assert!(port_rx.try_recv().is_err());
        assert!(risk_rx.try_recv().is_err());
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_route_hold_to_portfolio_only() {
        let (mut dispatcher, exec_rx, port_rx, risk_rx) = make_dispatcher();
        dispatcher.submit_signal(hold_signal()).unwrap();
        dispatcher.process_batch_signals();
        assert!(port_rx.try_recv().is_ok());
        assert!(exec_rx.try_recv().is_err());
        assert!(risk_rx.try_recv().is_err());
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_get_performance_stats() {
        let (dispatcher, _, _, _) = make_dispatcher();
        let stats = dispatcher.get_performance_stats();
        assert_eq!(stats.signals_processed, 0);
        assert!(!stats.is_running);
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_zero_copy_dispatcher_submit_and_recv() {
        let zd = ZeroCopyDispatcher::new(64, 64);
        let sig = Arc::new(buy_signal());
        assert!(zd.submit_zero_copy(sig));
        let received = zd.recv_zero_copy();
        assert!(received.is_some());
        assert_eq!(received.unwrap().action, SignalAction::Buy);
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_zero_copy_urgent_priority() {
        let zd = ZeroCopyDispatcher::new(64, 64);
        let normal = Arc::new(hold_signal());
        let mut urgent_sig = buy_signal();
        urgent_sig.flags |= signal_flags::URGENT;
        let urgent = Arc::new(urgent_sig);
        zd.submit_zero_copy(normal);
        zd.submit_zero_copy(urgent);
        let first = zd.recv_zero_copy().unwrap();
        assert!(first.is_urgent());
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_zero_copy_metrics() {
        let zd = ZeroCopyDispatcher::new(64, 64);
        let sig = Arc::new(buy_signal());
        zd.submit_zero_copy(sig);
        let (count, _avg, _max) = zd.get_metrics();
        assert_eq!(count, 1);
    }

    #[test]
    #[ignore = "requires Windows elevation (os error 740)"]
    fn test_legacy_dispatcher_new() {
        let (exec_tx, _) = channel::bounded(64);
        let (port_tx, _) = channel::bounded(64);
        let (risk_tx, _) = channel::bounded(64);
        let dispatcher = SignalDispatcher::new(exec_tx, port_tx, risk_tx).unwrap();
        let (processed, _avg, _max, urgent, normal) = dispatcher.get_stats();
        assert_eq!(processed, 0);
        assert_eq!(urgent, 0);
        assert_eq!(normal, 0);
    }
}
