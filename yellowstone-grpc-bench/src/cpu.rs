//! CPU accounting during a run: cores used by thread group, and busy time on watched CPUs.
//! Busy watched CPU minus interrupt time and the threads pinned there estimates load from other tenants.
//! This assumes IRQ time accounting, the default on Ubuntu kernels: interrupt time is charged to no thread.

use {
    serde::Serialize,
    std::{
        collections::BTreeMap,
        fs,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::JoinHandle,
        time::{Duration, Instant},
    },
};

const INTERVAL: Duration = Duration::from_millis(500);

/// Thread groups of the `ygb run` process: name prefix and reported group; the first match wins.
pub const RUN_GROUPS: [(&str, &str); 7] = [
    ("solGeyserGrpc", "plugin_workers"),
    ("solGrpcBlockMet", "plugin_block_meta"),
    ("solGrpcTrace", "plugin_trace"),
    ("ygbEntryNotif", "notifier_lanes"),
    ("ygbTxStatus", "notifier_lanes"),
    ("ygbSchedule", "decoder"),
    ("ygb", "driver_lanes"),
];

/// Groups whose threads run on the plugin CPUs in `ygb run`.
pub const RUN_OWN: [&str; 3] = ["plugin_workers", "plugin_block_meta", "plugin_trace"];
/// The same, when the notifier lanes share the plugin CPUs.
pub const RUN_OWN_WITH_NOTIFIERS: [&str; 4] = [
    "plugin_workers",
    "plugin_block_meta",
    "plugin_trace",
    "notifier_lanes",
];

#[derive(Debug, Default, Serialize, serde::Deserialize)]
pub struct CpuReport {
    pub seconds: f64,
    /// Mean and peak cores used per thread group, over 500 ms intervals.
    pub groups: BTreeMap<String, (f64, f64)>,
    /// Busiest single thread per group: its peak share of one core over 500 ms intervals.
    #[serde(default)]
    pub thread_peak: BTreeMap<String, f64>,
    /// Mean and peak busy cores over the watched CPUs, from /proc/stat.
    pub plugin_cpus_busy: (f64, f64),
    /// Mean and peak busy cores on the watched CPUs not explained by the pinned groups or interrupts.
    pub plugin_cpus_foreign: (f64, f64),
    /// Mean and peak cores of irq and softirq time on the watched CPUs, mostly the network stack.
    #[serde(default)]
    pub plugin_cpus_irq: (f64, f64),
}

pub struct CpuSampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<CpuReport>,
}

impl CpuSampler {
    /// Samples `groups` (thread name prefix, group) and the busy time of `cpus`; `None` off Linux,
    /// where /proc is missing. `own` lists the groups pinned to `cpus`; the rest counts as foreign.
    pub fn start(
        cpus: Vec<usize>,
        groups: &'static [(&'static str, &'static str)],
        own: &'static [&'static str],
    ) -> Option<Self> {
        if !cfg!(target_os = "linux") {
            return None;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name("ygbCpuSampler".to_owned())
                .spawn(move || sample(&stop, &cpus, groups, own))
                .expect("spawn cpu sampler")
        };
        Some(Self { stop, handle })
    }

    pub fn finish(self) -> CpuReport {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap_or_default()
    }
}

fn sample(
    stop: &AtomicBool,
    plugin_cpus: &[usize],
    groups_spec: &[(&'static str, &'static str)],
    own: &[&str],
) -> CpuReport {
    // SAFETY: sysconf has no preconditions.
    let ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    let started = Instant::now();
    let mut previous_threads = thread_ticks();
    let mut previous_cpus = cpu_ticks();
    let mut previous_at = Instant::now();
    let mut groups: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut thread_peak: BTreeMap<String, f64> = BTreeMap::new();
    let (mut busy, mut foreign, mut irq) = (Vec::new(), Vec::new(), Vec::new());
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(INTERVAL);
        let threads = thread_ticks();
        let cpus = cpu_ticks();
        let secs = previous_at.elapsed().as_secs_f64();
        previous_at = Instant::now();
        let mut used: BTreeMap<&str, f64> =
            groups_spec.iter().map(|(_, group)| (*group, 0.0)).collect();
        for (tid, (name, ticks)) in &threads {
            let before = previous_threads.get(tid).map_or(0, |(_, ticks)| *ticks);
            if let Some((_, group)) = groups_spec
                .iter()
                .find(|(prefix, _)| name.starts_with(prefix))
            {
                let cores = ticks.saturating_sub(before) as f64 / ticks_per_sec / secs;
                *used.get_mut(group).expect("known group") += cores;
                let peak = thread_peak.entry((*group).to_owned()).or_insert(0.0);
                *peak = peak.max(cores);
            }
        }
        let (mut busy_now, mut irq_now) = (0.0, 0.0);
        for cpu in plugin_cpus {
            let now = cpus.get(cpu).copied().unwrap_or_default();
            let then = previous_cpus.get(cpu).copied().unwrap_or_default();
            let total = now.total.saturating_sub(then.total).max(1) as f64;
            busy_now += now.busy.saturating_sub(then.busy) as f64 / total;
            irq_now += now.irq.saturating_sub(then.irq) as f64 / total;
        }
        let own: f64 = own
            .iter()
            .map(|group| used.get(group).copied().unwrap_or(0.0))
            .sum();
        busy.push(busy_now);
        irq.push(irq_now);
        foreign.push((busy_now - irq_now - own).max(0.0));
        for (group, cores) in used {
            groups.entry(group.to_owned()).or_default().push(cores);
        }
        previous_threads = threads;
        previous_cpus = cpus;
    }
    CpuReport {
        seconds: started.elapsed().as_secs_f64(),
        groups: groups
            .into_iter()
            .map(|(group, values)| (group, mean_max(&values)))
            .collect(),
        thread_peak,
        plugin_cpus_busy: mean_max(&busy),
        plugin_cpus_foreign: mean_max(&foreign),
        plugin_cpus_irq: mean_max(&irq),
    }
}

fn mean_max(values: &[f64]) -> (f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0);
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    (mean, values.iter().copied().fold(0.0, f64::max))
}

/// utime + stime per thread of this process, by tid, with the thread name.
fn thread_ticks() -> BTreeMap<u32, (String, u64)> {
    let mut out = BTreeMap::new();
    let Ok(tasks) = fs::read_dir("/proc/self/task") else {
        return out;
    };
    for task in tasks.flatten() {
        let Ok(tid) = task.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = fs::read_to_string(task.path().join("stat")) else {
            continue;
        };
        // The name sits in parentheses and may contain spaces; fields follow the last ')'.
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let name = stat[open + 1..close].to_owned();
        let fields: Vec<&str> = stat[close + 2..].split_whitespace().collect();
        let ticks = fields
            .get(11)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
            + fields
                .get(12)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
        out.insert(tid, (name, ticks));
    }
    out
}

/// Jiffies of one CPU from /proc/stat; busy includes irq, softirq and steal.
#[derive(Debug, Clone, Copy, Default)]
struct CpuTicks {
    busy: u64,
    irq: u64,
    total: u64,
}

fn cpu_ticks() -> BTreeMap<usize, CpuTicks> {
    let mut out = BTreeMap::new();
    let Ok(stat) = fs::read_to_string("/proc/stat") else {
        return out;
    };
    for line in stat.lines() {
        let mut parts = line.split_whitespace();
        let Some(cpu) = parts.next().and_then(|name| name.strip_prefix("cpu")) else {
            continue;
        };
        let Ok(cpu) = cpu.parse::<usize>() else {
            continue;
        };
        // user nice system idle iowait irq softirq steal
        let values: Vec<u64> = parts.filter_map(|v| v.parse().ok()).collect();
        let field = |index: usize| values.get(index).copied().unwrap_or(0);
        let total: u64 = values.iter().take(8).sum();
        out.insert(
            cpu,
            CpuTicks {
                busy: total.saturating_sub(field(3) + field(4)),
                irq: field(5) + field(6),
                total,
            },
        );
    }
    out
}
