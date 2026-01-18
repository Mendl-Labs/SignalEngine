use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::OnceLock;

/// Calibrated TSC frequency (cycles per nanosecond)
/// Lazily initialized on first use
static TSC_FREQ_GHZ: OnceLock<f64> = OnceLock::new();

/// Get calibrated TSC frequency, or default to 3.0 GHz
fn get_tsc_freq_ghz() -> f64 {
    *TSC_FREQ_GHZ.get_or_init(|| {
        // Try to calibrate by measuring TSC over a known time period
        #[cfg(target_arch = "x86_64")]
        {
            let start_tsc = unsafe { std::arch::x86_64::_rdtsc() };
            let start_time = std::time::Instant::now();
            
            // Spin for ~10ms to get a reasonable sample
            std::thread::sleep(std::time::Duration::from_millis(10));
            
            let end_tsc = unsafe { std::arch::x86_64::_rdtsc() };
            let elapsed_ns = start_time.elapsed().as_nanos() as f64;
            
            if elapsed_ns > 0.0 {
                let tsc_diff = (end_tsc - start_tsc) as f64;
                let freq = tsc_diff / elapsed_ns;
                // Sanity check: should be between 1-6 GHz
                if freq > 0.5 && freq < 8.0 {
                    eprintln!("[timestamp] Calibrated TSC frequency: {:.2} GHz", freq);
                    return freq;
                }
            }
        }
        // Fallback to typical frequency
        eprintln!("[timestamp] Using default TSC frequency: 3.0 GHz");
        3.0
    })
}

/// Get nanosecond precision timestamp using the fastest available method
#[inline(always)]
pub fn nano_timestamp() -> u128 {
    // Use hardware timestamp counter for maximum precision on x86_64
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { ultra_signal::high_precision_timestamp_ns() as u128 }
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    }
}

/// High-resolution timer for latency measurements
pub struct NanoTimer {
    start: u128,
}

impl NanoTimer {
    #[inline(always)]
    pub fn start() -> Self {
        Self {
            start: nano_timestamp(),
        }
    }

    #[inline(always)]
    pub fn elapsed_ns(&self) -> u64 {
        (nano_timestamp() - self.start) as u64
    }

    #[inline(always)]
    pub fn elapsed_us(&self) -> f64 {
        (nano_timestamp() - self.start) as f64 / 1_000.0
    }

    #[inline(always)]
    pub fn elapsed_ms(&self) -> f64 {
        (nano_timestamp() - self.start) as f64 / 1_000_000.0
    }
}

/// Convert hardware timestamp to nanoseconds (calibrated)
#[cfg(target_arch = "x86_64")]
pub fn rdtsc_to_ns(tsc: u64) -> u128 {
    // Use runtime-calibrated TSC frequency
    let freq = get_tsc_freq_ghz();
    (tsc as f64 / freq) as u128
}

/// Busy-wait for precise timing (use sparingly)
#[inline(always)]
pub fn nano_sleep(nanos: u64) {
    let start = nano_timestamp();
    while (nano_timestamp() - start) < nanos as u128 {
        std::hint::spin_loop();
    }
}

/// Timing utilities for performance measurements
pub struct PerformanceTimer {
    measurements: Vec<u64>,
    capacity: usize,
}

impl PerformanceTimer {
    pub fn new(capacity: usize) -> Self {
        Self {
            measurements: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn measure<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = nano_timestamp();
        let result = f();
        let elapsed = (nano_timestamp() - start) as u64;
        
        if self.measurements.len() < self.capacity {
            self.measurements.push(elapsed);
        } else {
            // Circular buffer for continuous monitoring
            let index = self.measurements.len() % self.capacity;
            self.measurements[index] = elapsed;
        }
        
        result
    }

    pub fn get_statistics(&self) -> Option<(u64, u64, f64)> {
        if self.measurements.is_empty() {
            return None;
        }

        let min = *self.measurements.iter().min()?;
        let max = *self.measurements.iter().max()?;
        let avg = self.measurements.iter().sum::<u64>() as f64 / self.measurements.len() as f64;

        Some((min, max, avg))
    }

    pub fn percentile(&self, p: f64) -> Option<u64> {
        if self.measurements.is_empty() {
            return None;
        }

        let mut sorted = self.measurements.clone();
        sorted.sort_unstable();
        
        let index = ((sorted.len() as f64 - 1.0) * p / 100.0).round() as usize;
        sorted.get(index).copied()
    }
}
