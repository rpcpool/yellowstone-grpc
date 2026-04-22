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

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub fn get_thread_affinity() -> Result<Vec<usize>, String> {
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    Ok((0..num_cpus).collect())
}
