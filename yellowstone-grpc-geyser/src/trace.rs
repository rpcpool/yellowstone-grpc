//! Latency checkpoints for the `latency-trace` feature; without it every call compiles to nothing.
//! Checkpoints record a message's age (now - created_at) or a duration into per-thread histograms,
//! flushed every second to `$YELLOWSTONE_LATENCY_TRACE_DIR/trace-<pid>.json`.

use {crate::plugin::message::Message, prost_types::Timestamp};

pub const ENABLED: bool = cfg!(feature = "latency-trace");

#[derive(Debug, Clone, Copy)]
pub enum Stage {
    /// geyser_loop took the message off the plugin channel.
    DequeueAge,
    /// Messages per processed broadcast batch (a count, not ns).
    BatchSize,
    /// Duration of the processed broadcast send, which wakes every processed subscriber.
    BroadcastSend,
    /// A client_loop received a batch; age of its first message. Sampled.
    ClientRecvAge,
    /// Duration of one client_loop pass over a batch. Sampled with ClientRecvAge.
    ClientBatchLoop,
    /// Time an update waited in a subscriber queue before the response stream took it.
    QueueResidence,
    /// Age of an update when the response stream took it from the subscriber queue.
    StreamDequeueAge,
    /// Duration of one block reconstruction loop pass.
    ReconstructionPass,
    /// Busy permille of each runtime worker over 100 ms.
    WorkerBusyPermille,
    /// Runtime inject queue depth, sampled every 100 ms.
    GlobalQueueDepth,
}

pub const STAGE_NAMES: [&str; 10] = [
    "dequeue_age_ns",
    "batch_size",
    "broadcast_send_ns",
    "client_recv_age_ns",
    "client_batch_loop_ns",
    "queue_residence_ns",
    "stream_dequeue_age_ns",
    "reconstruction_pass_ns",
    "worker_busy_permille",
    "global_queue_depth",
];

/// One in 16 batches is sampled for per-subscriber checkpoints, chosen by the first message's stamp.
pub fn sampled(message: &Message) -> bool {
    created_at(message).nanos % 16 == 0
}

pub fn age_ns(now_unix_ns: u64, message: &Message) -> u64 {
    now_unix_ns.saturating_sub(timestamp_ns(created_at(message)))
}

pub const fn timestamp_ns(ts: &Timestamp) -> u64 {
    (ts.seconds as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.nanos as u64)
}

fn created_at(message: &Message) -> &Timestamp {
    match message {
        Message::DeshredUpdateParent(msg) => &msg.created_at,
        Message::EntryUpdateParent(msg) => &msg.created_at,
        Message::Slot(msg) => &msg.created_at,
        Message::Account(msg) => &msg.created_at,
        Message::Transaction(msg) => &msg.created_at,
        Message::DeshredTransaction(msg) => &msg.created_at,
        Message::Entry(msg) => &msg.created_at,
        Message::BlockFooter(msg) => &msg.created_at,
        Message::BlockMeta(msg) => &msg.created_at,
        Message::Block(msg) => &msg.created_at,
    }
}

pub use imp::*;

#[cfg(not(feature = "latency-trace"))]
mod imp {
    use super::Stage;

    #[inline(always)]
    pub const fn record(_stage: Stage, _value: u64) {}

    #[inline(always)]
    pub const fn unix_ns() -> u64 {
        0
    }

    #[inline(always)]
    pub const fn mono_ns() -> u64 {
        0
    }

    pub const fn start(_runtime: &tokio::runtime::Handle) {}
}

#[cfg(feature = "latency-trace")]
mod imp {
    use {
        super::{Stage, STAGE_NAMES},
        log::{info, warn},
        std::{
            cell::RefCell,
            path::PathBuf,
            sync::{Arc, Mutex, OnceLock},
            time::{Duration, Instant, SystemTime, UNIX_EPOCH},
        },
    };

    const SUB: u64 = 16;
    const BUCKETS: usize = 40 * SUB as usize;

    /// Log-linear histogram with 16 sub-buckets per power of two.
    #[derive(Clone)]
    struct Hist {
        buckets: Vec<u64>,
        count: u64,
        sum: u128,
        max: u64,
    }

    impl Default for Hist {
        fn default() -> Self {
            Self {
                buckets: vec![0; BUCKETS],
                count: 0,
                sum: 0,
                max: 0,
            }
        }
    }

    impl Hist {
        fn record(&mut self, value: u64) {
            let index = if value < SUB {
                value as usize
            } else {
                let exp = 63 - u64::from(value.leading_zeros());
                ((exp - 3) * SUB + ((value >> (exp - 4)) - SUB)) as usize
            };
            self.buckets[index.min(BUCKETS - 1)] += 1;
            self.count += 1;
            self.sum += u128::from(value);
            self.max = self.max.max(value);
        }

        fn merge(&mut self, other: &Self) {
            for (bucket, add) in self.buckets.iter_mut().zip(&other.buckets) {
                *bucket += add;
            }
            self.count += other.count;
            self.sum += other.sum;
            self.max = self.max.max(other.max);
        }

        fn quantile(&self, q: f64) -> u64 {
            let target = ((self.count as f64) * q).ceil().max(1.0) as u64;
            let mut seen = 0;
            for (index, bucket) in self.buckets.iter().enumerate() {
                seen += bucket;
                if seen >= target {
                    let index = index as u64;
                    if index < SUB {
                        return index;
                    }
                    let exp = index / SUB + 3;
                    return (((SUB + index % SUB + 1) << (exp - 4)) - 1).min(self.max);
                }
            }
            self.max
        }

        fn summary(&self) -> serde_json::Value {
            serde_json::json!({
                "count": self.count,
                "mean": if self.count == 0 { 0.0 } else { self.sum as f64 / self.count as f64 },
                "p50": self.quantile(0.5),
                "p90": self.quantile(0.9),
                "p99": self.quantile(0.99),
                "p999": self.quantile(0.999),
                "max": self.max,
            })
        }
    }

    type Shard = Arc<Mutex<Vec<Hist>>>;

    static SHARDS: OnceLock<Mutex<Vec<Shard>>> = OnceLock::new();

    thread_local! {
        static LOCAL: RefCell<Option<Shard>> = const { RefCell::new(None) };
    }

    pub fn record(stage: Stage, value: u64) {
        LOCAL.with(|local| {
            let mut local = local.borrow_mut();
            let shard = local.get_or_insert_with(|| {
                let shard: Shard = Arc::new(Mutex::new(vec![Hist::default(); STAGE_NAMES.len()]));
                SHARDS
                    .get_or_init(Default::default)
                    .lock()
                    .expect("trace registry")
                    .push(Arc::clone(&shard));
                shard
            });
            shard.lock().expect("trace shard")[stage as usize].record(value);
        });
    }

    pub fn unix_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or_default()
    }

    pub fn mono_ns() -> u64 {
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        ORIGIN.get_or_init(Instant::now).elapsed().as_nanos() as u64
    }

    /// Starts the runtime sampler and the flusher, when YELLOWSTONE_LATENCY_TRACE_DIR is set.
    pub fn start(runtime: &tokio::runtime::Handle) {
        let Some(dir) = std::env::var_os("YELLOWSTONE_LATENCY_TRACE_DIR").map(PathBuf::from) else {
            return;
        };
        if let Err(error) = std::fs::create_dir_all(&dir) {
            warn!("latency-trace: cannot create {}: {error}", dir.display());
            return;
        }
        let runtime = runtime.clone();
        let path = dir.join(format!("trace-{}.json", std::process::id()));
        info!("latency-trace: writing {}", path.display());
        let _ = std::thread::Builder::new()
            .name("solGrpcTrace".to_owned())
            .spawn(move || run(&runtime, &path));
    }

    fn run(runtime: &tokio::runtime::Handle, path: &std::path::Path) {
        let metrics = runtime.metrics();
        let workers = metrics.num_workers();
        let mut busy: Vec<Duration> = (0..workers).map(|i| metrics.worker_total_busy_duration(i)).collect();
        let mut last_flush = Instant::now();
        loop {
            std::thread::sleep(Duration::from_millis(100));
            record(Stage::GlobalQueueDepth, metrics.global_queue_depth() as u64);
            for (worker, previous) in busy.iter_mut().enumerate() {
                let total = metrics.worker_total_busy_duration(worker);
                let delta = total.saturating_sub(*previous);
                *previous = total;
                record(Stage::WorkerBusyPermille, (delta.as_micros() / 100) as u64);
            }
            if last_flush.elapsed() >= Duration::from_secs(1) {
                last_flush = Instant::now();
                if let Err(error) = flush(path) {
                    warn!("latency-trace: flush failed: {error}");
                }
            }
        }
    }

    fn flush(path: &std::path::Path) -> std::io::Result<()> {
        let mut merged = vec![Hist::default(); STAGE_NAMES.len()];
        for shard in SHARDS.get_or_init(Default::default).lock().expect("trace registry").iter() {
            for (total, hist) in merged.iter_mut().zip(shard.lock().expect("trace shard").iter()) {
                total.merge(hist);
            }
        }
        let stages: serde_json::Map<String, serde_json::Value> = STAGE_NAMES
            .iter()
            .zip(&merged)
            .map(|(name, hist)| ((*name).to_owned(), hist.summary()))
            .collect();
        let report = serde_json::json!({ "unix_ns": unix_ns(), "stages": stages });
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&report)?)?;
        std::fs::rename(tmp, path)
    }
}
