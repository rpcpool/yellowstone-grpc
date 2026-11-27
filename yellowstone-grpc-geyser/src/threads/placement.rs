//! The placement from config, and the system calls a new thread makes to apply its plan.

use crate::{
    config::Config,
    extension::seams::{SchedulingPolicy, ThreadPlacement, ThreadPlan, ThreadRole},
};

/// The placement from config: `tokio.affinity` for the delivery workers, `threads.control_cpus`
/// for the filter compile lane and the metrics thread, `threads.compressor_cpus` for both pools.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfiguredThreadPlacement {
    pub delivery_worker_cpus: Option<Vec<usize>>,
    pub control_cpus: Option<Vec<usize>>,
    pub compressor_cpus: Option<Vec<usize>>,
}

impl ConfiguredThreadPlacement {
    pub fn from_config(plugin_config: &Config) -> Self {
        let data_plane_threads = &plugin_config.grpc.data_plane.threads;
        Self {
            delivery_worker_cpus: plugin_config.tokio.affinity.clone(),
            control_cpus: data_plane_threads.control_cpus.clone(),
            compressor_cpus: data_plane_threads.compressor_cpus.clone(),
        }
    }
}

impl ThreadPlacement for ConfiguredThreadPlacement {
    fn place_thread(&self, thread_role: ThreadRole, _thread_ordinal: u16) -> ThreadPlan {
        let allowed_cpus = match thread_role {
            ThreadRole::DeliveryWorker => self.delivery_worker_cpus.clone(),
            ThreadRole::FilterCompileWorker | ThreadRole::MetricsRender => {
                self.control_cpus.clone()
            }
            ThreadRole::ProcessedCompressionWorker | ThreadRole::ConfirmedCompressionWorker => {
                self.compressor_cpus.clone()
            }
            ThreadRole::BankRelease | ThreadRole::BlockMetaStorage => None,
        };
        ThreadPlan {
            allowed_cpus,
            scheduling_policy: SchedulingPolicy::Other,
        }
    }
}

pub use os_thread_calls::{
    current_os_thread_id, os_thread_carries_name, os_thread_exists, read_os_thread_cpus,
    read_process_cpu_mask, set_current_thread_cpus, set_current_thread_policy,
};

#[cfg(target_os = "linux")]
mod os_thread_calls {
    use {
        crate::extension::seams::SchedulingPolicy,
        std::{fs, io, mem},
    };

    /// The calling thread's id, as `/proc/self/task` lists it.
    pub fn current_os_thread_id() -> Option<i32> {
        // SAFETY: gettid has no precondition and cannot fail.
        Some(unsafe { libc::gettid() })
    }

    /// Whether the kernel still lists `os_thread_id` among this process's threads.
    pub fn os_thread_exists(os_thread_id: i32) -> bool {
        // SAFETY: signal 0 sends nothing; the call only checks that the thread is in this group.
        let tgkill_result =
            unsafe { libc::syscall(libc::SYS_tgkill, libc::getpid(), os_thread_id, 0) };
        tgkill_result == 0
    }

    /// Whether `os_thread_id` is listed and its name starts with `name_prefix`. An unreadable
    /// name counts as a match, so a listed thread is never taken for gone.
    pub fn os_thread_carries_name(os_thread_id: i32, name_prefix: &str) -> bool {
        os_thread_exists(os_thread_id)
            && fs::read_to_string(format!("/proc/self/task/{os_thread_id}/comm"))
                .map_or(true, |thread_comm| {
                    thread_comm.trim_end().starts_with(name_prefix)
                })
    }

    pub fn set_current_thread_policy(scheduling_policy: SchedulingPolicy) -> io::Result<()> {
        let (policy_number, policy_priority) = match scheduling_policy {
            SchedulingPolicy::Other => (libc::SCHED_OTHER, 0),
            SchedulingPolicy::Fifo { fifo_priority } => {
                (libc::SCHED_FIFO, libc::c_int::from(fifo_priority))
            }
        };
        let scheduling_parameters = libc::sched_param {
            sched_priority: policy_priority,
        };
        // SAFETY: pid 0 names the calling thread, and the parameters outlive the call.
        if unsafe { libc::sched_setscheduler(0, policy_number, &scheduling_parameters) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if policy_number == libc::SCHED_OTHER {
            // SAFETY: gettid has no precondition and cannot fail.
            let current_thread_id = unsafe { libc::gettid() };
            // SAFETY: with PRIO_PROCESS, a thread id sets that one thread's nice value on Linux.
            if unsafe { libc::setpriority(libc::PRIO_PROCESS, current_thread_id as libc::id_t, 0) }
                != 0
            {
                let nice_error = io::Error::last_os_error();
                return Err(io::Error::new(
                    nice_error.kind(),
                    format!("nice 0: {nice_error}"),
                ));
            }
        }
        Ok(())
    }

    pub fn set_current_thread_cpus(allowed_cpus: &[usize]) -> io::Result<()> {
        // SAFETY: an all-zero cpu_set_t is the empty set.
        let mut cpu_set: libc::cpu_set_t = unsafe { mem::zeroed() };
        let cpu_set_capacity = mem::size_of::<libc::cpu_set_t>() * 8;
        for allowed_cpu in allowed_cpus.iter().copied() {
            if allowed_cpu >= cpu_set_capacity {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("CPU {allowed_cpu} is past the {cpu_set_capacity} CPUs of a CPU set"),
                ));
            }
            // SAFETY: the index is below the set's capacity.
            unsafe { libc::CPU_SET(allowed_cpu, &mut cpu_set) };
        }
        // SAFETY: pid 0 names the calling thread, and the set is initialized.
        if unsafe { libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &cpu_set) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// The CPUs `os_thread_id` may run on; 0 names the calling thread.
    pub fn read_os_thread_cpus(os_thread_id: i32) -> io::Result<Vec<usize>> {
        // SAFETY: an all-zero cpu_set_t is the empty set.
        let mut cpu_set: libc::cpu_set_t = unsafe { mem::zeroed() };
        // SAFETY: the set is writable and as large as the size passed.
        if unsafe {
            libc::sched_getaffinity(
                os_thread_id,
                mem::size_of::<libc::cpu_set_t>(),
                &mut cpu_set,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        let cpu_set_capacity = mem::size_of::<libc::cpu_set_t>() * 8;
        Ok((0..cpu_set_capacity)
            // SAFETY: every index is below the set's capacity.
            .filter(|cpu_index| unsafe { libc::CPU_ISSET(*cpu_index, &cpu_set) })
            .collect())
    }

    /// The main thread's CPUs: the process mask, whatever thread loads the plugin.
    pub fn read_process_cpu_mask() -> io::Result<Vec<usize>> {
        // SAFETY: getpid has no precondition and cannot fail.
        read_os_thread_cpus(unsafe { libc::getpid() })
    }
}

#[cfg(not(target_os = "linux"))]
mod os_thread_calls {
    use {crate::extension::seams::SchedulingPolicy, std::io};

    pub const fn current_os_thread_id() -> Option<i32> {
        None
    }

    pub const fn os_thread_exists(_os_thread_id: i32) -> bool {
        false
    }

    pub const fn os_thread_carries_name(_os_thread_id: i32, _name_prefix: &str) -> bool {
        false
    }

    pub const fn set_current_thread_policy(_scheduling_policy: SchedulingPolicy) -> io::Result<()> {
        Ok(())
    }

    pub const fn set_current_thread_cpus(_allowed_cpus: &[usize]) -> io::Result<()> {
        Ok(())
    }

    pub const fn read_os_thread_cpus(_os_thread_id: i32) -> io::Result<Vec<usize>> {
        Ok(Vec::new())
    }

    pub const fn read_process_cpu_mask() -> io::Result<Vec<usize>> {
        Ok(Vec::new())
    }
}
