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

    /// Start ultra-fast signal processing loop
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
