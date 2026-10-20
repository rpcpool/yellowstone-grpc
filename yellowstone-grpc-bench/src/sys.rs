//! Thin wrappers over the Linux calls the harness needs: monotonic clock, absolute sleeps, pinning.

use std::{io, mem};

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

/// Shrinks the calling thread's timer slack so absolute sleeps wake on time.
pub fn set_min_timer_slack() {
    // SAFETY: PR_SET_TIMERSLACK takes a plain integer argument.
    unsafe { libc::prctl(libc::PR_SET_TIMERSLACK, 1 as libc::c_ulong, 0, 0, 0) };
}

pub fn pin_current_thread(cpus: &[usize]) -> io::Result<()> {
    if cpus.is_empty() {
        return Ok(());
    }
    // SAFETY: cpu_set_t is plain data; CPU_SET writes inside it and sched_setaffinity reads it.
    unsafe {
        let mut set: libc::cpu_set_t = mem::zeroed();
        for cpu in cpus {
            libc::CPU_SET(*cpu, &mut set);
        }
        if libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Parses a CPU list such as `8-13,24-29`.
pub fn parse_cpu_list(list: &str) -> anyhow::Result<Vec<usize>> {
    let mut cpus = Vec::new();
    for part in list.split(',').map(str::trim).filter(|part| !part.is_empty()) {
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
