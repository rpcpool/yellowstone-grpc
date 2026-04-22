#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn set_thread_affinity(cpus: &[usize]) -> Result<(), String> {
    affinity::set_thread_affinity(cpus).map_err(|e| e.to_string())
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub fn set_thread_affinity(_cpus: &[usize]) -> Result<(), String> {
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn get_thread_affinity() -> Result<Vec<usize>, String> {
    affinity::get_thread_affinity().map_err(|e| e.to_string())
}

/// Return sensible values though these IDs are not really useful as the set_thread_affinity() method is a no-op.
///
/// Returns all CPU indices as available cores. Uses `std::thread::available_parallelism`
/// which accounts for cgroup/sched_affinity restrictions on Linux but on macOS simply
/// returns the number of logical CPUs (hw.logicalcpu) without actual affinity support.
/// See https://doc.rust-lang.org/std/thread/fn.available_parallelism.html
#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub fn get_thread_affinity() -> Result<Vec<usize>, String> {
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    Ok((0..num_cpus).collect())
}
