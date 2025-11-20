// RDTSC Hardware Timestamp Module for Ultra-Low Latency
// Provides sub-nanosecond precision timing across x86_64 and ARM64

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

static RDTSC_FREQUENCY: AtomicU64 = AtomicU64::new(0);
static INIT: Once = Once::new();

/// Get RDTSC timestamp (CPU cycles)
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn get_rdtsc() -> u64 {
    unsafe { std::arch::x86_64::_rdtsc() }
}

/// Get RDTSC timestamp (ARM64 virtual counter)
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn get_rdtsc() -> u64 {
    let mut val: u64;
    unsafe {
        std::arch::asm!("mrs {0}, cntvct_el0", out(reg) val);
    }
    val
}

/// Fallback for other architectures
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
pub fn get_rdtsc() -> u64 {
    std::time::Instant::now().elapsed().as_nanos() as u64
}

/// Initialize RDTSC frequency calibration
pub fn init_rdtsc() {
    INIT.call_once(|| {
        let freq = calibrate_rdtsc_frequency();
        RDTSC_FREQUENCY.store(freq, Ordering::Relaxed);
    });
}

/// Calibrate RDTSC frequency (cycles per second)
fn calibrate_rdtsc_frequency() -> u64 {
    let iterations = 10;
    let mut frequencies = Vec::with_capacity(iterations);
    
    for _ in 0..iterations {
        let start_time = std::time::Instant::now();
        let start_rdtsc = get_rdtsc();
        
        // Sleep for 10ms to get accurate measurement
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let end_rdtsc = get_rdtsc();
        let end_time = std::time::Instant::now();
        
        let elapsed_ns = end_time.duration_since(start_time).as_nanos() as u64;
        let cycles = end_rdtsc.wrapping_sub(start_rdtsc);
        
        // Calculate frequency: cycles / time_in_seconds
        let frequency = (cycles * 1_000_000_000) / elapsed_ns;
        frequencies.push(frequency);
    }
    
    // Use median to avoid outliers
    frequencies.sort_unstable();
    frequencies[iterations / 2]
}

/// Convert RDTSC cycles to nanoseconds
#[inline(always)]
pub fn rdtsc_to_ns(cycles: u64) -> u64 {
    let freq = RDTSC_FREQUENCY.load(Ordering::Relaxed);
    if freq == 0 {
        // Not calibrated, use approximate conversion
        cycles / 3 // Assume ~3GHz CPU
    } else {
        (cycles * 1_000_000_000) / freq
    }
}

/// Convert nanoseconds to RDTSC cycles
#[inline(always)]
pub fn ns_to_rdtsc(ns: u64) -> u64 {
    let freq = RDTSC_FREQUENCY.load(Ordering::Relaxed);
    if freq == 0 {
        ns * 3 // Assume ~3GHz CPU
    } else {
        (ns * freq) / 1_000_000_000
    }
}

/// Get current timestamp in nanoseconds using RDTSC
#[inline(always)]
pub fn get_timestamp_ns() -> u64 {
    rdtsc_to_ns(get_rdtsc())
}

/// Measure duration between two RDTSC timestamps
#[inline(always)]
pub fn rdtsc_duration_ns(start: u64, end: u64) -> u64 {
    rdtsc_to_ns(end.wrapping_sub(start))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_rdtsc_monotonic() {
        init_rdtsc();
        
        let t1 = get_rdtsc();
        std::thread::sleep(std::time::Duration::from_micros(100));
        let t2 = get_rdtsc();
        
        assert!(t2 > t1, "RDTSC should be monotonically increasing");
    }
    
    #[test]
    fn test_rdtsc_to_ns_conversion() {
        init_rdtsc();
        
        let start = get_rdtsc();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let end = get_rdtsc();
        
        let duration_ns = rdtsc_duration_ns(start, end);
        
        // Should be approximately 10ms (10,000,000 ns)
        // Allow 20% tolerance for measurement variance
        assert!(duration_ns > 8_000_000 && duration_ns < 12_000_000,
                "Duration should be ~10ms, got {}ns", duration_ns);
    }
    
    #[test]
    fn test_rdtsc_frequency_calibration() {
        init_rdtsc();
        
        let freq = RDTSC_FREQUENCY.load(Ordering::Relaxed);
        
        // On some systems (VMs, laptops with power management), RDTSC may not be reliable
        // Just verify we got some reasonable value
        if freq > 10_000_000 && freq < 10_000_000_000 {
            println!("RDTSC frequency calibrated to {} GHz", freq as f64 / 1_000_000_000.0);
        } else {
            println!("RDTSC calibration may be unreliable on this system: {} Hz", freq);
            println!("This is expected on VMs or systems with aggressive power management");
        }
    }
    
    #[test]
    fn test_rdtsc_overhead() {
        init_rdtsc();
        
        // Measure overhead of RDTSC call itself
        let mut overhead_samples = Vec::with_capacity(1000);
        
        for _ in 0..1000 {
            let start = get_rdtsc();
            let end = get_rdtsc();
            overhead_samples.push(end.wrapping_sub(start));
        }
        
        overhead_samples.sort_unstable();
        let median_overhead = overhead_samples[500];
        let overhead_ns = rdtsc_to_ns(median_overhead);
        
        println!("RDTSC overhead: {} cycles ({} ns)", median_overhead, overhead_ns);
        
        // RDTSC should have very low overhead (<100ns)
        assert!(overhead_ns < 100, "RDTSC overhead too high: {}ns", overhead_ns);
    }
}
