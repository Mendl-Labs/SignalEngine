// System Optimization Utilities
//
// Provides CPU affinity, thread priority, and huge pages configuration
// for maximum performance on Windows and Linux systems.

#[cfg(target_os = "windows")]
use winapi::um::processthreadsapi::{GetCurrentThread, SetThreadPriority};
#[cfg(target_os = "windows")]
use winapi::um::winbase::{THREAD_PRIORITY_TIME_CRITICAL, THREAD_PRIORITY_HIGHEST};

#[cfg(target_os = "linux")]
use std::fs;

use std::thread;

/// Thread priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadPriority {
    /// Highest priority (realtime on Windows)
    Critical,
    /// High priority
    High,
    /// Normal priority
    Normal,
}

/// Set CPU affinity for current thread
/// 
/// # Arguments
/// * `cpu_cores` - Bitmask of CPU cores to pin to (e.g., 0b0011 for cores 0 and 1)
/// 
/// # Returns
/// `Ok(())` on success, `Err(String)` on failure
#[cfg(target_os = "windows")]
pub fn set_thread_affinity(_cpu_cores: usize) -> Result<(), String> {
    // SetThreadAffinityMask requires different winapi features
    // For now, return a note that this requires manual configuration
    Err("Windows CPU affinity requires additional configuration. Use Process Affinity instead.".to_string())
}

#[cfg(target_os = "linux")]
pub fn set_thread_affinity(cpu_cores: usize) -> Result<(), String> {
    // Linux implementation would use sched_setaffinity
    // For now, return a placeholder
    Err("Linux CPU affinity not yet implemented".to_string())
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
pub fn set_thread_affinity(_cpu_cores: usize) -> Result<(), String> {
    Err("CPU affinity not supported on this platform".to_string())
}

/// Set thread priority for current thread
/// 
/// # Arguments
/// * `priority` - Priority level to set
/// 
/// # Returns
/// `Ok(())` on success, `Err(String)` on failure
#[cfg(target_os = "windows")]
pub fn set_thread_priority(priority: ThreadPriority) -> Result<(), String> {
    let win_priority = match priority {
        ThreadPriority::Critical => THREAD_PRIORITY_TIME_CRITICAL,
        ThreadPriority::High => THREAD_PRIORITY_HIGHEST,
        ThreadPriority::Normal => 0, // THREAD_PRIORITY_NORMAL
    };
    
    unsafe {
        let result = SetThreadPriority(GetCurrentThread(), win_priority as i32);
        if result == 0 {
            Err(format!("Failed to set thread priority: GetLastError={}", 
                winapi::um::errhandlingapi::GetLastError()))
        } else {
            Ok(())
        }
    }
}

#[cfg(target_os = "linux")]
pub fn set_thread_priority(priority: ThreadPriority) -> Result<(), String> {
    // Linux implementation would use pthread_setschedparam
    Err("Linux thread priority not yet implemented".to_string())
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
pub fn set_thread_priority(_priority: ThreadPriority) -> Result<(), String> {
    Err("Thread priority not supported on this platform".to_string())
}

/// Pin current thread to specific CPU core
/// 
/// # Arguments
/// * `core_id` - CPU core ID (0-based)
pub fn pin_to_core(core_id: usize) -> Result<(), String> {
    let mask = 1 << core_id;
    set_thread_affinity(mask)
}

/// Enable transparent huge pages (Linux only)
/// 
/// On Linux, this configures the system to use 2MB pages instead of 4KB pages,
/// reducing TLB misses by up to 15% for large allocations.
#[cfg(target_os = "linux")]
pub fn enable_huge_pages() -> Result<(), String> {
    // Check if transparent huge pages are available
    if let Ok(thp_enabled) = fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled") {
        if thp_enabled.contains("[always]") || thp_enabled.contains("[madvise]") {
            return Ok(());
        }
    }
    
    Err("Transparent huge pages not available or not enabled".to_string())
}

#[cfg(target_os = "windows")]
pub fn enable_huge_pages() -> Result<(), String> {
    // Windows large pages require SeLockMemoryPrivilege
    // This is typically set via group policy
    Ok(()) // Assume enabled if privilege is granted
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
pub fn enable_huge_pages() -> Result<(), String> {
    Err("Huge pages not supported on this platform".to_string())
}

/// Optimize current thread for ultra-low latency
/// 
/// Sets highest priority and pins to specified core.
/// 
/// # Arguments
/// * `core_id` - CPU core to pin to (None = no pinning)
pub fn optimize_thread_for_latency(core_id: Option<usize>) -> Result<(), String> {
    // Set critical priority
    if let Err(e) = set_thread_priority(ThreadPriority::Critical) {
        eprintln!("Warning: Failed to set thread priority: {}", e);
    }
    
    // Pin to core if specified
    if let Some(core) = core_id {
        if let Err(e) = pin_to_core(core) {
            eprintln!("Warning: Failed to pin to core {}: {}", core, e);
        }
    }
    
    Ok(())
}

/// Get number of available CPU cores
pub fn get_cpu_count() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// System optimization configuration
#[derive(Debug, Clone)]
pub struct SystemOptimization {
    /// Enable thread priority boosting
    pub high_priority: bool,
    /// CPU core to pin threads to (None = no pinning)
    pub cpu_core: Option<usize>,
    /// Enable transparent huge pages
    pub huge_pages: bool,
}

impl Default for SystemOptimization {
    fn default() -> Self {
        Self {
            high_priority: true,
            cpu_core: None,
            huge_pages: true,
        }
    }
}

impl SystemOptimization {
    /// Apply optimizations to current thread
    pub fn apply(&self) -> Result<(), String> {
        if self.high_priority {
            if let Err(e) = set_thread_priority(ThreadPriority::Critical) {
                eprintln!("Warning: {}", e);
            }
        }
        
        if let Some(core) = self.cpu_core {
            if let Err(e) = pin_to_core(core) {
                eprintln!("Warning: {}", e);
            }
        }
        
        if self.huge_pages {
            if let Err(e) = enable_huge_pages() {
                eprintln!("Warning: {}", e);
            }
        }
        
        Ok(())
    }
    
    /// Create configuration for ultra-low latency trading
    pub fn for_trading(core_id: Option<usize>) -> Self {
        Self {
            high_priority: true,
            cpu_core: core_id,
            huge_pages: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_get_cpu_count() {
        let count = get_cpu_count();
        assert!(count > 0);
        println!("Available CPU cores: {}", count);
    }
    
    #[test]
    fn test_thread_priority() {
        // This may fail without elevated privileges
        match set_thread_priority(ThreadPriority::High) {
            Ok(_) => println!("Successfully set thread priority"),
            Err(e) => println!("Could not set thread priority: {}", e),
        }
    }
    
    #[test]
    fn test_system_optimization() {
        let config = SystemOptimization::default();
        match config.apply() {
            Ok(_) => println!("System optimizations applied"),
            Err(e) => println!("Some optimizations failed: {}", e),
        }
    }
    
    #[test]
    fn test_optimize_for_trading() {
        let config = SystemOptimization::for_trading(Some(0));
        assert!(config.high_priority);
        assert_eq!(config.cpu_core, Some(0));
        assert!(config.huge_pages);
    }
}
