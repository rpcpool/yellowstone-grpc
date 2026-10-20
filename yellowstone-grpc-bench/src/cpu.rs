//! CPU accounting during a run: cores used by thread group, and busy time on the plugin CPUs.
//! Busy plugin CPU minus the plugin threads' own CPU estimates load from other tenants.

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

/// Thread name prefixes and the group they are reported under.
const GROUPS: [(&str, &str); 5] = [
    ("solGeyserGrpc", "plugin_workers"),
    ("ygbLane", "driver_lanes"),
    ("ygbSchedule", "decoder"),
    ("solGrpcBlockMet", "plugin_block_meta"),
    ("solGrpcTrace", "plugin_trace"),
];

#[derive(Debug, Default, Serialize)]
pub struct CpuReport {
    pub seconds: f64,
    /// Mean and peak cores used per thread group, over 500 ms intervals.
    pub groups: BTreeMap<String, (f64, f64)>,
    /// Mean and peak busy cores over the plugin CPUs, from /proc/stat.
    pub plugin_cpus_busy: (f64, f64),
    /// Mean and peak busy cores on the plugin CPUs not explained by the plugin's own threads.
    pub plugin_cpus_foreign: (f64, f64),
}

pub struct CpuSampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<CpuReport>,
}

impl CpuSampler {
    pub fn start(plugin_cpus: Vec<usize>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name("ygbCpuSampler".to_owned())
                .spawn(move || sample(&stop, &plugin_cpus))
                .expect("spawn cpu sampler")
        };
        Self { stop, handle }
    }

    pub fn finish(self) -> CpuReport {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap_or_default()
    }
}

fn sample(stop: &AtomicBool, plugin_cpus: &[usize]) -> CpuReport {
    // SAFETY: sysconf has no preconditions.
    let ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    let started = Instant::now();
    let mut previous_threads = thread_ticks();
    let mut previous_cpus = cpu_ticks();
    let mut previous_at = Instant::now();
    let mut groups: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let (mut busy, mut foreign) = (Vec::new(), Vec::new());
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(INTERVAL);
        let threads = thread_ticks();
        let cpus = cpu_ticks();
        let secs = previous_at.elapsed().as_secs_f64();
        previous_at = Instant::now();
        let mut used: BTreeMap<&str, f64> = GROUPS.iter().map(|(_, group)| (*group, 0.0)).collect();
        for (tid, (name, ticks)) in &threads {
            let before = previous_threads.get(tid).map_or(0, |(_, ticks)| *ticks);
            if let Some((_, group)) = GROUPS.iter().find(|(prefix, _)| name.starts_with(prefix)) {
                *used.get_mut(group).expect("known group") +=
                    ticks.saturating_sub(before) as f64 / ticks_per_sec / secs;
            }
        }
        let busy_now: f64 = plugin_cpus
            .iter()
            .map(|cpu| {
                let (now_busy, now_total) = cpus.get(cpu).copied().unwrap_or_default();
                let (then_busy, then_total) = previous_cpus.get(cpu).copied().unwrap_or_default();
                let total = now_total.saturating_sub(then_total).max(1) as f64;
                now_busy.saturating_sub(then_busy) as f64 / total
            })
            .sum();
        let own = used["plugin_workers"] + used["plugin_block_meta"] + used["plugin_trace"];
        busy.push(busy_now);
        foreign.push((busy_now - own).max(0.0));
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
        plugin_cpus_busy: mean_max(&busy),
        plugin_cpus_foreign: mean_max(&foreign),
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
        let ticks = fields.get(11).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)
            + fields.get(12).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        out.insert(tid, (name, ticks));
    }
    out
}

/// (busy, total) jiffies per CPU from /proc/stat.
fn cpu_ticks() -> BTreeMap<usize, (u64, u64)> {
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
        let values: Vec<u64> = parts.filter_map(|v| v.parse().ok()).collect();
        let total: u64 = values.iter().take(8).sum();
        let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
        out.insert(cpu, (total.saturating_sub(idle), total));
    }
    out
}
