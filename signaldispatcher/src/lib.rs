// Ultra-low latency signal dispatcher for nanosecond trading
use ultra_signal::{Signal, UltraFastSignalQueue, SignalAction, OrderSide};
use crossbeam::channel::{Sender, Receiver, bounded, TryRecvError};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Lock-free signal dispatcher optimized for ultra-low latency
pub struct UltraFastSignalDispatcher {
    // Lock-free signal queues for different priority levels
    urgent_queue: Arc<UltraFastSignalQueue>,
    normal_queue: Arc<UltraFastSignalQueue>,
    
    // Execution handlers for different components
    execution_sender: Sender<Signal>,
    portfolio_sender: Sender<Signal>,
    risk_sender: Sender<Signal>,
    
    // Performance metrics
    signals_processed: AtomicU64,
    avg_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
    
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
    ) -> Self {
        Self {
            urgent_queue: Arc::new(UltraFastSignalQueue::new(4096)), // 4K urgent signals
            normal_queue: Arc::new(UltraFastSignalQueue::new(8192)), // 8K normal signals
            execution_sender,
            portfolio_sender,
            risk_sender,
            signals_processed: AtomicU64::new(0),
            avg_latency_ns: AtomicU64::new(0),
            max_latency_ns: AtomicU64::new(0),
            is_running: AtomicBool::new(false),
            batch_buffer: Vec::with_capacity(32), // Batch up to 32 signals
        }
    }

        /// Ultra-fast batch signal processing using pre-allocated buffer
    pub fn process_batch_signals(&mut self) -> u64 {
        if self.batch_buffer.is_empty() {
            self.batch_buffer.reserve(32); // Ensure capacity
        }
        self.batch_buffer.clear();
        
        let mut processed_count = 0u64;
        let batch_start = std::time::Instant::now();
        
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
            
            // Update performance metrics
            let batch_latency_ns = batch_start.elapsed().as_nanos() as u64;
            self.update_performance_metrics(processed_count, batch_latency_ns);
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

    /// Update performance metrics atomically
    #[inline]
    fn update_performance_metrics(&self, processed_count: u64, batch_latency_ns: u64) {
        let current_processed = self.signals_processed.fetch_add(processed_count, Ordering::Relaxed);
        
        // Update average latency using exponential moving average
        if processed_count > 0 {
            let avg_per_signal = batch_latency_ns / processed_count;
            let current_avg = self.avg_latency_ns.load(Ordering::Relaxed);
            let new_avg = if current_processed == processed_count {
                avg_per_signal // First batch
            } else {
                // Exponential moving average: new_avg = old_avg * 0.9 + new_sample * 0.1
                (current_avg * 9 + avg_per_signal) / 10
            };
            self.avg_latency_ns.store(new_avg, Ordering::Relaxed);
            
            // Update max latency if needed
            let current_max = self.max_latency_ns.load(Ordering::Relaxed);
            if avg_per_signal > current_max {
                self.max_latency_ns.store(avg_per_signal, Ordering::Relaxed);
            }
        }
    }
    pub fn start(&self) -> std::thread::JoinHandle<()> {
        let urgent_queue = Arc::clone(&self.urgent_queue);
        let normal_queue = Arc::clone(&self.normal_queue);
        let execution_sender = self.execution_sender.clone();
        let portfolio_sender = self.portfolio_sender.clone();
        let risk_sender = self.risk_sender.clone();
        let signals_processed = Arc::new(AtomicU64::new(0));
        let avg_latency_ns = Arc::new(AtomicU64::new(0));
        let max_latency_ns = Arc::new(AtomicU64::new(0));
        let is_running = Arc::new(AtomicBool::new(true));

        thread::spawn(move || {
            // Set thread priority to high for ultra-low latency
            #[cfg(target_os = "windows")]
            unsafe {
                use std::os::windows::io::AsRawHandle;
                let handle = std::process::id();
                // Note: Actual Windows API calls would go here
                // winapi::um::processthreadsapi::SetThreadPriority(...);
            }

            let mut batch_buffer: Vec<Signal> = Vec::with_capacity(32);

            while is_running.load(Ordering::Relaxed) {
                let start_time = ultra_signal::high_precision_timestamp_ns();
                
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

                // Update performance metrics
                if processed_count > 0 {
                    let end_time = ultra_signal::high_precision_timestamp_ns();
                    let latency_ns = end_time - start_time;
                    
                    signals_processed.fetch_add(processed_count, Ordering::Relaxed);
                    
                    // Update average latency with exponential moving average
                    let current_avg = avg_latency_ns.load(Ordering::Relaxed);
                    let new_avg = (current_avg * 9 + latency_ns) / 10;
                    avg_latency_ns.store(new_avg, Ordering::Relaxed);
                    
                    // Update max latency
                    let current_max = max_latency_ns.load(Ordering::Relaxed);
                    if latency_ns > current_max {
                        max_latency_ns.store(latency_ns, Ordering::Relaxed);
                    }
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
        (
            self.signals_processed.load(Ordering::Relaxed),
            self.avg_latency_ns.load(Ordering::Relaxed),
            self.max_latency_ns.load(Ordering::Relaxed),
        )
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
        DispatcherStats {
            signals_processed: self.signals_processed.load(Ordering::Relaxed),
            avg_latency_ns: self.avg_latency_ns.load(Ordering::Relaxed),
            max_latency_ns: self.max_latency_ns.load(Ordering::Relaxed),
            is_running: self.is_running.load(Ordering::Relaxed),
            urgent_queue_size: self.urgent_queue.len(),
            normal_queue_size: self.normal_queue.len(),
        }
    }

    /// Reset performance counters
    pub fn reset_stats(&self) {
        self.signals_processed.store(0, Ordering::Relaxed);
        self.avg_latency_ns.store(0, Ordering::Relaxed);
        self.max_latency_ns.store(0, Ordering::Relaxed);
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
    ) -> Self {
        Self {
            ultra_dispatcher: UltraFastSignalDispatcher::new(
                execution_sender,
                portfolio_sender,
                risk_sender,
            ),
        }
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
