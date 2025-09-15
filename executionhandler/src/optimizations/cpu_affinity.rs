use crate::core::types::ExecutionError;

/// Set CPU affinity for critical trading threads
#[cfg(target_os = "linux")]
pub fn set_cpu_affinity(core_id: usize) -> Result<(), ExecutionError> {
    use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
    use std::mem;
    use std::os::unix::thread::JoinHandleExt;

    // Validate core_id is within reasonable bounds (0-255 cores should be sufficient)
    if core_id >= 256 {
        return Err(ExecutionError::InvalidParameter(format!(
            "Core ID {} exceeds maximum supported cores (255)", core_id
        )));
    }

    unsafe {
        let mut cpu_set: cpu_set_t = mem::zeroed();
        CPU_ZERO(&mut cpu_set);
        CPU_SET(core_id, &mut cpu_set);

        let result = sched_setaffinity(
            0, // current thread
            mem::size_of::<cpu_set_t>(),
            &cpu_set,
        );

        if result == 0 {
            Ok(())
        } else {
            Err(ExecutionError::Unknown(format!(
                "Failed to set CPU affinity to core {}", core_id
            )))
        }
    }
}

/// Windows CPU affinity implementation
#[cfg(target_os = "windows")]
pub fn set_cpu_affinity(core_id: usize) -> Result<(), ExecutionError> {
    use winapi::um::processthreadsapi::{GetCurrentThread, SetThreadPriority};
    use winapi::um::winbase::THREAD_PRIORITY_HIGHEST;
    
    // Validate core_id is within reasonable bounds
    if core_id >= 256 {
        return Err(ExecutionError::InvalidParameter(format!(
            "Core ID {} exceeds maximum supported cores (255)", core_id
        )));
    }
    
    unsafe {
        // Windows: Set thread priority instead of affinity 
        // SAFETY: GetCurrentThread() always returns a valid pseudo-handle
        let result = SetThreadPriority(
            GetCurrentThread(), 
            THREAD_PRIORITY_HIGHEST as i32 // Safe conversion instead of try_into().unwrap()
        );
        
        if result != 0 {
            Ok(())
        } else {
            Err(ExecutionError::Unknown(format!(
                "Failed to set CPU affinity to core {}", core_id
            )))
        }
    }
}

/// macOS CPU affinity (limited support)
#[cfg(target_os = "macos")]
pub fn set_cpu_affinity(core_id: usize) -> Result<(), ExecutionError> {
    // macOS doesn't support strict CPU affinity, but we can use thread affinity policy
    use mach::mach_types::{thread_act_t, thread_port_t};
    use mach::thread_act::thread_policy_set;
    use mach::thread_policy::{thread_affinity_policy_data_t, THREAD_AFFINITY_POLICY};
    use mach::traps::mach_thread_self;
    
    unsafe {
        let mut policy = thread_affinity_policy_data_t {
            affinity_tag: core_id as u32,
        };
        
        let result = thread_policy_set(
            mach_thread_self(),
            THREAD_AFFINITY_POLICY,
            &mut policy as *mut _ as *mut i32,
            1,
        );
        
        if result == 0 {
            Ok(())
        } else {
            Err(ExecutionError::Unknown(format!(
                "Failed to set thread affinity to core {}", core_id
            )))
        }
    }
}

/// Get optimal CPU core for trading threads
pub fn get_optimal_trading_core() -> usize {
    let num_cores = num_cpus::get();
    
    // Reserve core 0 for OS, use core 1 for primary trading thread
    if num_cores > 1 {
        1
    } else {
        0
    }
}

/// Get dedicated cores for different trading components
#[derive(Debug, Clone)]
pub struct CoreAssignment {
    pub primary_execution: usize,
    pub websocket_processing: usize,
    pub metrics_collection: usize,
    pub order_management: usize,
}

impl CoreAssignment {
    pub fn optimal_assignment() -> Self {
        let num_cores = num_cpus::get();
        
        match num_cores {
            1 => Self {
                primary_execution: 0,
                websocket_processing: 0,
                metrics_collection: 0,
                order_management: 0,
            },
            2 => Self {
                primary_execution: 1,
                websocket_processing: 0,
                metrics_collection: 0,
                order_management: 1,
            },
            3 => Self {
                primary_execution: 1,
                websocket_processing: 2,
                metrics_collection: 0,
                order_management: 1,
            },
            4..=7 => Self {
                primary_execution: 1,
                websocket_processing: 2,
                metrics_collection: 3,
                order_management: 1,
            },
            _ => Self {
                primary_execution: 1,
                websocket_processing: 2,
                metrics_collection: 3,
                order_management: 4,
            },
        }
    }
}

/// Set process priority for low-latency trading
#[cfg(target_os = "linux")]
pub fn set_high_priority() -> Result<(), ExecutionError> {
    use libc::{setpriority, PRIO_PROCESS};
    
    unsafe {
        let result = setpriority(PRIO_PROCESS, 0, -10); // High priority
        if result == 0 {
            Ok(())
        } else {
            Err(ExecutionError::Unknown(
                "Failed to set high process priority".to_string()
            ))
        }
    }
}

#[cfg(target_os = "windows")]
pub fn set_high_priority() -> Result<(), ExecutionError> {
    use winapi::um::processthreadsapi::{GetCurrentProcess, SetPriorityClass};
    use winapi::um::winbase::HIGH_PRIORITY_CLASS;
    
    unsafe {
        let result = SetPriorityClass(GetCurrentProcess(), HIGH_PRIORITY_CLASS);
        if result != 0 {
            Ok(())
        } else {
            Err(ExecutionError::Unknown(
                "Failed to set high process priority".to_string()
            ))
        }
    }
}

#[cfg(target_os = "macos")]
pub fn set_high_priority() -> Result<(), ExecutionError> {
    use libc::{setpriority, PRIO_PROCESS};
    
    unsafe {
        let result = setpriority(PRIO_PROCESS, 0, -10);
        if result == 0 {
            Ok(())
        } else {
            Err(ExecutionError::Unknown(
                "Failed to set high process priority".to_string()
            ))
        }
    }
}

/// Disable CPU frequency scaling for consistent performance
#[cfg(target_os = "linux")]
pub fn disable_cpu_scaling() -> Result<(), ExecutionError> {
    // This typically requires root permissions
    // In practice, this would be done at the system level
    println!("Note: Disable CPU frequency scaling with: echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor");
    Ok(())
}

/// Enable real-time scheduling (requires elevated privileges)
#[cfg(target_os = "linux")]
pub fn enable_realtime_scheduling() -> Result<(), ExecutionError> {
    use libc::{sched_param, sched_setscheduler, SCHED_FIFO};
    use std::mem;
    
    unsafe {
        let param = sched_param {
            sched_priority: 50, // Real-time priority
            ..mem::zeroed()
        };
        
        let result = sched_setscheduler(0, SCHED_FIFO, &param);
        if result == 0 {
            Ok(())
        } else {
            Err(ExecutionError::Unknown(
                "Failed to enable real-time scheduling (requires root)".to_string()
            ))
        }
    }
}
