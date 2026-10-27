//! Thin wrappers over the OS calls the harness needs: monotonic clock, absolute sleeps, pinning.
//! Pinning, timer slack and SCHED_FIFO exist only on Linux; elsewhere they do nothing or fail softly.

use std::io;

pub fn mono_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid out-pointer for clock_gettime.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

pub fn unix_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid out-pointer for clock_gettime.
    unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Sleeps until an absolute CLOCK_MONOTONIC deadline in nanoseconds.
#[cfg(target_os = "linux")]
pub fn sleep_until_mono(deadline_ns: u64) {
    let ts = libc::timespec {
        tv_sec: (deadline_ns / 1_000_000_000) as libc::time_t,
        tv_nsec: (deadline_ns % 1_000_000_000) as libc::c_long,
    };
    loop {
        // SAFETY: `ts` is a valid timespec and the remainder pointer may be null with TIMER_ABSTIME.
        let rc = unsafe {
            libc::clock_nanosleep(
                libc::CLOCK_MONOTONIC,
                libc::TIMER_ABSTIME,
                &ts,
                std::ptr::null_mut(),
            )
        };
        if rc != libc::EINTR {
            return;
        }
    }
}

/// Sleeps until an absolute CLOCK_MONOTONIC deadline in nanoseconds without clock_nanosleep.
/// macOS lets a sleep end late by a share of its length, so long waits go in halves.
#[cfg(not(target_os = "linux"))]
pub fn sleep_until_mono(deadline_ns: u64) {
    loop {
        let left = deadline_ns.saturating_sub(mono_ns());
        if left == 0 {
            return;
        }
        let step = if left > 400_000 { left / 2 } else { left };
        std::thread::sleep(std::time::Duration::from_nanos(step));
    }
}

/// Shrinks the calling thread's timer slack so absolute sleeps wake on time.
pub fn set_min_timer_slack() {
    // SAFETY: PR_SET_TIMERSLACK takes a plain integer argument.
    #[cfg(target_os = "linux")]
    unsafe {
        libc::prctl(libc::PR_SET_TIMERSLACK, 1 as libc::c_ulong, 0, 0, 0)
    };
}

pub fn pin_current_thread(cpus: &[usize]) -> io::Result<()> {
    pin_thread(0, cpus)
}

/// Pins a thread (0 for the calling thread) to `cpus`; an empty list leaves it as it is.
#[cfg(target_os = "linux")]
pub fn pin_thread(tid: i32, cpus: &[usize]) -> io::Result<()> {
    if cpus.is_empty() {
        return Ok(());
    }
    // SAFETY: cpu_set_t is plain data; CPU_SET writes inside it and sched_setaffinity reads it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for cpu in cpus {
            libc::CPU_SET(*cpu, &mut set);
        }
        if libc::sched_setaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Other systems have no CPU affinity to set; threads run where the OS puts them.
#[cfg(not(target_os = "linux"))]
pub fn pin_thread(_tid: i32, _cpus: &[usize]) -> io::Result<()> {
    Ok(())
}

/// Thread ids and names of this process whose names start with one of `prefixes`; Linux only.
fn threads_named(prefixes: &[&str]) -> Vec<(i32, String)> {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    tasks
        .flatten()
        .filter_map(|task| {
            let tid = task.file_name().to_string_lossy().parse::<i32>().ok()?;
            let name = std::fs::read_to_string(task.path().join("comm")).ok()?;
            let name = name.trim_end().to_owned();
            prefixes
                .iter()
                .any(|prefix| name.starts_with(prefix))
                .then_some((tid, name))
        })
        .collect()
}

/// Pins this process's threads whose names start with one of `prefixes`; returns how many moved.
pub fn pin_by_name(prefixes: &[&str], cpus: &[usize]) -> usize {
    let mut changed = 0;
    for (tid, name) in threads_named(prefixes) {
        match pin_thread(tid, cpus) {
            Ok(()) => changed += 1,
            Err(error) => log::warn!("pinning {name} failed: {error}"),
        }
    }
    changed
}

/// Sets SCHED_FIFO on a thread (0 for the calling thread); needs CAP_SYS_NICE.
#[cfg(target_os = "linux")]
pub fn set_fifo(tid: i32, priority: i32) -> io::Result<()> {
    let param = libc::sched_param {
        sched_priority: priority,
    };
    // SAFETY: param is a valid sched_param for the duration of the call.
    if unsafe { libc::sched_setscheduler(tid, libc::SCHED_FIFO, &param) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_fifo(_tid: i32, _priority: i32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SCHED_FIFO is set only on Linux",
    ))
}

/// Sets SCHED_FIFO on this process's threads whose names start with one of `prefixes`.
pub fn set_fifo_by_name(prefixes: &[&str], priority: i32) -> usize {
    let mut changed = 0;
    for (tid, name) in threads_named(prefixes) {
        match set_fifo(tid, priority) {
            Ok(()) => changed += 1,
            Err(error) => log::warn!("SCHED_FIFO on {name} failed: {error}"),
        }
    }
    changed
}
/// Raises the soft open-file limit toward the hard one. macOS terminals start at 256, and a run
/// with 450 streams holds about 500 sockets in each of the plugin and fleet processes.
pub fn raise_fd_limit() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit and setrlimit read and write a plain struct.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return;
        }
        // macOS refuses values above kern.maxfilesperproc even when the hard limit is unlimited.
        for want in [65_536 as libc::rlim_t, 10_240] {
            let wanted = want.min(limit.rlim_max);
            if wanted <= limit.rlim_cur {
                return;
            }
            let raised = libc::rlimit {
                rlim_cur: wanted,
                rlim_max: limit.rlim_max,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &raised) == 0 {
                return;
            }
        }
    }
}

/// Parses a CPU list such as `8-13,24-29`.
pub fn parse_cpu_list(list: &str) -> anyhow::Result<Vec<usize>> {
    let mut cpus = Vec::new();
    for part in list
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        match part.split_once('-') {
            Some((start, end)) => {
                let (start, end): (usize, usize) = (start.parse()?, end.parse()?);
                anyhow::ensure!(start <= end, "bad cpu range {part}");
                cpus.extend(start..=end);
            }
            None => cpus.push(part.parse()?),
        }
    }
    Ok(cpus)
}

#[cfg(test)]
mod tests {
    use super::parse_cpu_list;

    #[test]
    fn cpu_lists_expand() {
        assert_eq!(parse_cpu_list("8-10,24").unwrap(), vec![8, 9, 10, 24]);
        assert!(parse_cpu_list("3-1").is_err());
    }
}
