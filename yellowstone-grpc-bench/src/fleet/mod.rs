//! `ygb fleet`: many concurrent subscribers on a raw HTTP/2 gRPC client.
//! Probes timestamp each message when its last DATA frame arrives, before decompression.

pub mod link;
pub mod profile;
pub mod scan;
pub mod unary;

use {
    crate::{cpu, hist::LogHist, sys},
    anyhow::Context,
    bytes::{Bytes, BytesMut},
    log::{info, warn},
    profile::{BuiltRequest, Compression, Profile, Role, Subscriber},
    prost::Message,
    scan::{Kind, Rpc},
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        fs,
        io::{BufWriter, Write},
        path::PathBuf,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
    },
    tokio::sync::watch,
};

#[derive(Debug, Clone, clap::Args)]
pub struct FleetArgs {
    /// Plaintext gRPC address of the plugin
    #[clap(long, default_value = "127.0.0.1:10000")]
    pub target: String,
    #[clap(long)]
    pub profile: PathBuf,
    /// Output directory for fleet.json and samples.bin.zst
    #[clap(long)]
    pub out: PathBuf,
    /// Stop after this many seconds; otherwise run until SIGINT or SIGTERM
    #[clap(long)]
    pub duration_secs: Option<u64>,
    /// Load subscribers sharing one HTTP/2 connection
    #[clap(long, default_value_t = 1)]
    pub streams_per_connection: usize,
    #[clap(long)]
    pub probe_cpus: Option<String>,
    #[clap(long)]
    pub load_cpus: Option<String>,
    #[clap(long, default_value_t = 2)]
    pub probe_threads: usize,
    #[clap(long, default_value_t = 6)]
    pub load_threads: usize,
    #[clap(long, default_value_t = 8)]
    pub stream_window_mib: u32,
    #[clap(long, default_value_t = 32)]
    pub connection_window_mib: u32,
    /// Use only the first N subscribers of `load-*` classes; probes are always kept
    #[clap(long)]
    pub max_load: Option<usize>,
    /// Use only the first N `blocks` subscribers
    #[clap(long)]
    pub max_blocks: Option<usize>,
    /// Use only the first N `deshred` subscribers
    #[clap(long)]
    pub max_deshred: Option<usize>,
    /// Seed for link jitter and churn timing
    #[clap(long, default_value_t = 1)]
    pub seed: u64,
    /// Plugin Prometheus address to scrape, e.g. 127.0.0.1:9123
    #[clap(long)]
    pub scrape: Option<String>,
    /// Seconds between scrapes; production scrapes every 10 s
    #[clap(long, default_value_t = 10.0)]
    pub scrape_secs: f64,
}

impl FleetArgs {
    /// Keeps probes and the first N subscribers of each limited group, in profile order.
    fn select(&self, expanded: Vec<Expanded>) -> Vec<Expanded> {
        let (mut load, mut blocks, mut deshred) = (0, 0, 0);
        expanded
            .into_iter()
            .filter(|sub| {
                if sub.spec.role == Role::Probe {
                    return true;
                }
                let (seen, limit) = if sub.spec.class.starts_with("load-") {
                    (&mut load, self.max_load)
                } else if sub.spec.class == "blocks" {
                    (&mut blocks, self.max_blocks)
                } else if sub.spec.class == "deshred" {
                    (&mut deshred, self.max_deshred)
                } else {
                    return true;
                };
                *seen += 1;
                limit.is_none_or(|limit| *seen <= limit)
            })
            .enumerate()
            .map(|(index, sub)| Expanded { index, ..sub })
            .collect()
    }
}

/// One received message of a probe, stored as a 32-byte little-endian record.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub subscriber: u16,
    pub kind: u8,
    pub status: u8,
    pub slot: u64,
    pub created_ns: u64,
    pub recv_ns: u64,
}

pub const SAMPLE_LEN: usize = 32;
/// Probe samples of a run, a zstd stream of `SAMPLE_LEN` records.
pub const SAMPLES_FILE: &str = "samples.bin.zst";

/// Samples per chunk of a `SampleLog`, 2 MiB.
const SAMPLE_CHUNK: usize = 1 << 16;

/// A probe's samples in fixed chunks. A growing Vec would copy the whole log on the probe thread
/// at each doubling and stamp the messages that arrive meanwhile late.
#[derive(Default)]
pub struct SampleLog(Vec<Vec<Sample>>);

impl SampleLog {
    fn push(&mut self, sample: Sample) {
        if self
            .0
            .last()
            .is_none_or(|chunk| chunk.len() == SAMPLE_CHUNK)
        {
            self.0.push(Vec::with_capacity(SAMPLE_CHUNK));
        }
        self.0.last_mut().expect("a chunk").push(sample);
    }

    fn len(&self) -> usize {
        self.0.iter().map(Vec::len).sum()
    }

    fn iter(&self) -> impl Iterator<Item = &Sample> {
        self.0.iter().flatten()
    }
}

impl Sample {
    fn write(&self, out: &mut impl Write) -> std::io::Result<()> {
        out.write_all(&self.subscriber.to_le_bytes())?;
        out.write_all(&[self.kind, self.status])?;
        out.write_all(&[0u8; 4])?;
        out.write_all(&self.slot.to_le_bytes())?;
        out.write_all(&self.created_ns.to_le_bytes())?;
        out.write_all(&self.recv_ns.to_le_bytes())
    }

    pub fn read(record: &[u8; SAMPLE_LEN]) -> Self {
        let u64_at =
            |at: usize| u64::from_le_bytes(record[at..at + 8].try_into().expect("8 bytes"));
        Self {
            subscriber: u16::from_le_bytes([record[0], record[1]]),
            kind: record[2],
            status: record[3],
            slot: u64_at(8),
            created_ns: u64_at(16),
            recv_ns: u64_at(24),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SubscriberReport {
    pub index: usize,
    pub name: String,
    pub class: String,
    pub role: String,
    pub compression: String,
    #[serde(default)]
    pub commitment: String,
    #[serde(default)]
    pub subscription_id: Option<String>,
    #[serde(default)]
    pub link: Option<link::LinkSpec>,
    /// Planned reconnects (churn).
    #[serde(default)]
    pub reconnects: u32,
    /// Streams the server or the network ended; load subscribers reconnect after each.
    #[serde(default)]
    pub errors: u32,
    pub messages: u64,
    pub bytes: u64,
    pub kinds: BTreeMap<String, u64>,
    pub disconnect: Option<String>,
    /// Time spent handling one received chunk, probes only.
    pub handle_ns: Option<crate::hist::Summary>,
    /// Reconnects and stream ends with their times, for lining stalls up with fleet activity.
    #[serde(default)]
    pub events: Vec<FleetEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetEvent {
    pub unix_ns: u64,
    pub what: String,
}

/// Events kept per subscriber; churn every few seconds over a long run stays bounded.
const MAX_EVENTS: usize = 256;

fn push_event(report: &mut SubscriberReport, what: String) {
    if report.events.len() < MAX_EVENTS {
        report.events.push(FleetEvent {
            unix_ns: sys::unix_ns(),
            what,
        });
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FleetReport {
    pub target: String,
    pub started_unix_ns: u64,
    pub stopped_unix_ns: u64,
    pub subscribers: Vec<SubscriberReport>,
    /// Subscribers with at least one ended stream.
    pub disconnects: usize,
    /// CPU used by the fleet's own threads, and foreign load on the probe CPUs.
    #[serde(default)]
    pub cpu: Option<crate::cpu::CpuReport>,
    /// Scrapes of the plugin's Prometheus endpoint; it renders the registry on a runtime worker.
    #[serde(default)]
    pub scrapes: Vec<Scrape>,
    /// Unary calls by method.
    #[serde(default)]
    pub unary: BTreeMap<String, unary::UnaryReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scrape {
    pub unix_ns: u64,
    pub millis: f64,
    pub bytes: u64,
    #[serde(default)]
    pub error: Option<String>,
}

/// Scrapes like a Prometheus agent until stopped: one GET per interval on a fresh connection.
async fn scrape_loop(
    address: String,
    every: Duration,
    mut stop: watch::Receiver<bool>,
) -> Vec<Scrape> {
    let mut scrapes = Vec::new();
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = stop.changed() => break,
        }
        let unix_ns = sys::unix_ns();
        let started = std::time::Instant::now();
        let result = scrape_once(&address).await;
        scrapes.push(Scrape {
            unix_ns,
            millis: started.elapsed().as_secs_f64() * 1e3,
            bytes: *result.as_ref().unwrap_or(&0),
            error: result.err().map(|error| format!("{error:#}")),
        });
    }
    scrapes
}

async fn scrape_once(address: &str) -> anyhow::Result<u64> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    anyhow::ensure!(
        response.starts_with(b"HTTP/1.1 200"),
        "scrape answered {:?}",
        String::from_utf8_lossy(&response[..response.len().min(40)])
    );
    Ok(response.len() as u64)
}

struct Expanded {
    index: usize,
    name: String,
    spec: Subscriber,
}

pub fn run(args: FleetArgs) -> anyhow::Result<()> {
    // The control thread, the CPU sampler and the scraper start from here; keep them off the probe CPUs.
    if let Some(cpus) = &args.load_cpus {
        sys::pin_current_thread(&sys::parse_cpu_list(cpus)?)?;
    }
    let profile: Profile = serde_json::from_slice(
        &fs::read(&args.profile)
            .with_context(|| format!("failed to read {}", args.profile.display()))?,
    )?;
    fs::create_dir_all(&args.out)?;
    let expanded: Vec<Expanded> = profile
        .subscribers
        .iter()
        .flat_map(|spec| {
            (0..spec.count).map(move |i| {
                (
                    spec,
                    if spec.count > 1 {
                        format!("{}-{i}", spec.name)
                    } else {
                        spec.name.clone()
                    },
                )
            })
        })
        .enumerate()
        .map(|(index, (spec, name))| Expanded {
            index,
            name,
            spec: spec.clone(),
        })
        .collect();
    let expanded = args.select(expanded);
    anyhow::ensure!(
        expanded.len() <= usize::from(u16::MAX),
        "too many subscribers"
    );
    info!(
        "fleet: {} subscribers ({} probes, {} with an emulated link, {} churning) against {}",
        expanded.len(),
        expanded
            .iter()
            .filter(|s| s.spec.role == Role::Probe)
            .count(),
        expanded
            .iter()
            .filter(|s| s.spec.link.as_ref().is_some_and(|link| !link.is_local()))
            .count(),
        expanded.iter().filter(|s| s.spec.churn.is_some()).count(),
        args.target
    );

    let probe_rt = build_runtime("ygbProbe", args.probe_threads, args.probe_cpus.as_deref())?;
    let load_rt = build_runtime("ygbLoad", args.load_threads, args.load_cpus.as_deref())?;
    let (stop_tx, stop_rx) = watch::channel(false);
    let started_unix_ns = sys::unix_ns();
    let latest_slot = Arc::new(AtomicU64::new(0));
    let cpu = cpu::CpuSampler::start(
        args.probe_cpus
            .as_deref()
            .map(sys::parse_cpu_list)
            .transpose()?
            .unwrap_or_default(),
        &FLEET_GROUPS,
        &["fleet_probes"],
    );

    let mut handles = Vec::new();
    let (probes, loads): (Vec<_>, Vec<_>) = expanded
        .into_iter()
        .partition(|s| s.spec.role == Role::Probe);
    for (subscribers, runtime, per_connection) in [
        (probes, &probe_rt, 1),
        (loads, &load_rt, args.streams_per_connection.max(1)),
    ] {
        for group in connection_groups(subscribers, per_connection) {
            let context = StreamContext {
                target: args.target.clone(),
                stop_rx: stop_rx.clone(),
                latest_slot: Arc::clone(&latest_slot),
                seed: args.seed,
                windows: (args.stream_window_mib, args.connection_window_mib),
            };
            handles.push(runtime.spawn(async move { run_connection(context, group).await }));
        }
    }

    let scraper = args
        .scrape
        .clone()
        .filter(|_| args.scrape_secs > 0.0)
        .map(|address| {
            load_rt.spawn(scrape_loop(
                address,
                Duration::from_secs_f64(args.scrape_secs),
                stop_rx.clone(),
            ))
        });
    let unary_handles: Vec<_> = profile
        .unary
        .iter()
        .filter(|load| load.rate_per_sec > 0.0)
        .map(|load| {
            let context = StreamContext {
                target: args.target.clone(),
                stop_rx: stop_rx.clone(),
                latest_slot: Arc::clone(&latest_slot),
                seed: args.seed,
                windows: (args.stream_window_mib, args.connection_window_mib),
            };
            load_rt.spawn(unary::run(context, load.clone()))
        })
        .collect();

    let control = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    control.block_on(async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let duration = args
            .duration_secs
            .map_or(Duration::MAX, Duration::from_secs);
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!("fleet: interrupted"),
            _ = term.recv() => info!("fleet: terminated"),
            _ = tokio::time::sleep(duration) => info!("fleet: duration reached"),
        }
        anyhow::Ok(())
    })?;
    let _ = stop_tx.send(true);

    let mut reports = Vec::new();
    let mut samples = Vec::new();
    for handle in handles {
        let results = probe_rt
            .block_on(handle)
            .context("connection task panicked")?;
        for (report, probe_samples) in results {
            reports.push(report);
            samples.push(probe_samples);
        }
    }
    reports.sort_by_key(|report| report.index);
    let disconnects = reports.iter().filter(|report| report.errors > 0).count();
    let scrapes = match scraper {
        Some(handle) => load_rt.block_on(handle).unwrap_or_default(),
        None => Vec::new(),
    };
    let unary = unary_handles
        .into_iter()
        .filter_map(|handle| load_rt.block_on(handle).ok())
        .collect();
    let report = FleetReport {
        target: args.target.clone(),
        started_unix_ns,
        stopped_unix_ns: sys::unix_ns(),
        subscribers: reports,
        disconnects,
        cpu: Some(cpu.finish()),
        scrapes,
        unary,
    };
    fs::write(
        args.out.join("fleet.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    let mut writer = zstd::stream::Encoder::new(
        BufWriter::new(fs::File::create(args.out.join(SAMPLES_FILE))?),
        1,
    )?;
    for sample in samples.iter().flat_map(SampleLog::iter) {
        sample.write(&mut writer)?;
    }
    writer.finish()?.flush()?;
    info!(
        "fleet: wrote {} probe samples, {} disconnects",
        samples.iter().map(SampleLog::len).sum::<usize>(),
        disconnects
    );
    probe_rt.shutdown_background();
    load_rt.shutdown_background();
    Ok(())
}

/// Thread groups of the fleet process, for its own CPU accounting.
const FLEET_GROUPS: [(&str, &str); 2] = [("ygbProbe", "fleet_probes"), ("ygbLoad", "fleet_load")];

/// Consecutive subscribers share a connection when role, link and subscription id match.
fn connection_groups(subscribers: Vec<Expanded>, per_connection: usize) -> Vec<Vec<Expanded>> {
    let mut groups: Vec<Vec<Expanded>> = Vec::new();
    for sub in subscribers {
        if let Some(group) = groups.last_mut() {
            let first = &group[0].spec;
            if group.len() < per_connection
                && first.link == sub.spec.link
                && first.windows_kib == sub.spec.windows_kib
                && first.subscription_id == sub.spec.subscription_id
            {
                group.push(sub);
                continue;
            }
        }
        groups.push(vec![sub]);
    }
    groups
}

fn build_runtime(
    name: &'static str,
    threads: usize,
    cpus: Option<&str>,
) -> anyhow::Result<tokio::runtime::Runtime> {
    let cpus = cpus
        .map(sys::parse_cpu_list)
        .transpose()?
        .unwrap_or_default();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.max(1))
        .thread_name(name)
        .on_thread_start(move || {
            if let Err(error) = sys::pin_current_thread(&cpus) {
                warn!("failed to pin fleet thread: {error}");
            }
        })
        .enable_all()
        .build()
        .map_err(Into::into)
}

type SubscriberResult = (SubscriberReport, SampleLog);

#[derive(Clone)]
pub struct StreamContext {
    target: String,
    stop_rx: watch::Receiver<bool>,
    /// Newest slot any probe has seen; churning subscribers resume behind it.
    latest_slot: Arc<AtomicU64>,
    seed: u64,
    /// HTTP/2 stream and connection windows, in MiB.
    windows: (u32, u32),
}

/// Opens one HTTP/2 connection through the subscriber's emulated link, with its windows in KiB.
async fn open_connection(
    context: &StreamContext,
    link: &link::LinkSpec,
    windows_kib: Option<[u32; 2]>,
    seed: u64,
) -> anyhow::Result<h2::client::SendRequest<Bytes>> {
    let stream = link::connect(&context.target, link, seed).await?;
    let [stream_window, connection_window] = windows_kib.map_or(
        [context.windows.0 << 20, context.windows.1 << 20],
        |[stream, connection]| [stream << 10, connection << 10],
    );
    let (send_request, connection) = h2::client::Builder::new()
        .initial_window_size(stream_window)
        .initial_connection_window_size(connection_window)
        .max_frame_size(1 << 20)
        .handshake::<_, Bytes>(stream)
        .await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            warn!("h2 connection ended: {error}");
        }
    });
    Ok(send_request)
}

async fn run_connection(context: StreamContext, group: Vec<Expanded>) -> Vec<SubscriberResult> {
    let link = group[0].spec.link.clone().unwrap_or_default();
    let seed = context.seed ^ (group[0].index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let connected = open_connection(&context, &link, group[0].spec.windows_kib, seed)
        .await
        .map_err(|error| format!("connect: {error:#}"));
    let streams: Vec<_> = group
        .into_iter()
        .map(|sub| {
            let connected = connected.clone();
            let context = context.clone();
            tokio::spawn(async move { run_stream(context, connected, sub).await })
        })
        .collect();
    let mut results = Vec::new();
    for stream in streams {
        if let Ok(result) = stream.await {
            results.push(result);
        }
    }
    results
}

fn failed_report(sub: &Expanded, reason: String) -> SubscriberReport {
    SubscriberReport {
        index: sub.index,
        name: sub.name.clone(),
        class: sub.spec.class.clone(),
        role: format!("{:?}", sub.spec.role).to_lowercase(),
        compression: format!("{:?}", sub.spec.compression).to_lowercase(),
        commitment: format!("{:?}", sub.spec.request.commitment).to_lowercase(),
        subscription_id: sub.spec.subscription_id.clone(),
        link: sub
            .spec
            .link
            .as_ref()
            .filter(|link| !link.is_local())
            .cloned(),
        disconnect: Some(reason),
        ..Default::default()
    }
}

/// How one stream attempt ended.
enum Attempt {
    Stopped,
    ChurnDue,
    Ended(String),
}

/// Waits out the backoff and opens a fresh connection; `None` when the fleet stops first.
async fn reconnect(
    context: &StreamContext,
    sub: &Expanded,
    report: &mut SubscriberReport,
    backoff: &mut Duration,
    stop_rx: &mut watch::Receiver<bool>,
) -> Option<h2::client::SendRequest<Bytes>> {
    let link = sub.spec.link.clone().unwrap_or_default();
    loop {
        tokio::select! {
            () = tokio::time::sleep(*backoff) => {}
            _ = stop_rx.changed() => return None,
        }
        *backoff = (*backoff * 2).min(Duration::from_secs(16));
        let seed = context.seed ^ (u64::from(report.errors) << 32 | sub.index as u64);
        match open_connection(context, &link, sub.spec.windows_kib, seed).await {
            Ok(fresh) => return Some(fresh),
            Err(error) => report.disconnect = Some(format!("reconnect: {error:#}")),
        }
    }
}

async fn run_stream(
    context: StreamContext,
    connected: Result<h2::client::SendRequest<Bytes>, String>,
    sub: Expanded,
) -> SubscriberResult {
    let mut report = failed_report(&sub, String::new());
    report.disconnect = None;
    let mut samples = SampleLog::default();
    let mut handle_hist = LogHist::default();
    let mut rng = context.seed ^ (sub.index as u64 + 1).wrapping_mul(0xA24B_AED4_963E_E407) | 1;
    let mut from_slot = None;
    let mut backoff = Duration::from_secs(1);
    let mut stop_rx = context.stop_rx.clone();
    let resume_slot = |back: u64| {
        let latest = context.latest_slot.load(Ordering::Relaxed);
        (back > 0 && latest > back).then(|| latest - back)
    };
    // A failed first connect counts as an ended stream; load subscribers then retry like the rest.
    let mut send_request = match connected {
        Ok(send_request) => Some(send_request),
        Err(reason) => {
            report.errors += 1;
            push_event(&mut report, reason.clone());
            report.disconnect = Some(reason);
            if sub.spec.role == Role::Probe {
                None
            } else {
                reconnect(&context, &sub, &mut report, &mut backoff, &mut stop_rx).await
            }
        }
    };
    'streams: while let Some(sender) = send_request.clone() {
        let attempt_started = tokio::time::Instant::now();
        let deadline = sub.spec.churn.as_ref().map(|churn| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let factor = 0.5 + (rng >> 11) as f64 / (1u64 << 53) as f64;
            tokio::time::Instant::now() + Duration::from_secs_f64(churn.every_secs * factor)
        });
        let attempt = stream_once(
            &context,
            &sender,
            &sub,
            from_slot,
            deadline,
            &mut report,
            &mut samples,
            &mut handle_hist,
        )
        .await;
        let reason = match attempt {
            Ok(Attempt::Stopped) => break 'streams,
            Ok(Attempt::ChurnDue) => {
                report.reconnects += 1;
                from_slot = resume_slot(
                    sub.spec
                        .churn
                        .as_ref()
                        .map_or(0, |churn| churn.from_slot_back),
                );
                push_event(
                    &mut report,
                    format!("churn reconnect, from_slot {from_slot:?}"),
                );
                continue;
            }
            Ok(Attempt::Ended(reason)) => reason,
            Err(error) => format!("{error:#}"),
        };
        report.errors += 1;
        push_event(&mut report, format!("stream ended: {reason}"));
        report.disconnect = Some(reason);
        if sub.spec.role == Role::Probe || *stop_rx.borrow() {
            break;
        }
        if attempt_started.elapsed() > Duration::from_secs(30) {
            backoff = Duration::from_secs(1);
        }
        // Load subscribers come back like production clients: after a backoff, on a new connection.
        send_request = reconnect(&context, &sub, &mut report, &mut backoff, &mut stop_rx).await;
        from_slot = resume_slot(
            sub.spec
                .churn
                .as_ref()
                .map_or(0, |churn| churn.from_slot_back),
        );
        if send_request.is_some() {
            push_event(&mut report, format!("reconnected, from_slot {from_slot:?}"));
        }
    }
    if sub.spec.role == Role::Probe {
        report.handle_ns = Some(handle_hist.summary());
    }
    (report, samples)
}

#[allow(clippy::too_many_arguments)]
async fn stream_once(
    context: &StreamContext,
    send_request: &h2::client::SendRequest<Bytes>,
    sub: &Expanded,
    from_slot: Option<u64>,
    deadline: Option<tokio::time::Instant>,
    report: &mut SubscriberReport,
    samples: &mut SampleLog,
    handle_hist: &mut LogHist,
) -> anyhow::Result<Attempt> {
    let (rpc, path, payload) = match sub.spec.request.build() {
        BuiltRequest::Subscribe(mut request) => {
            request.from_slot = from_slot;
            (Rpc::Subscribe, "Subscribe", request.encode_to_vec())
        }
        BuiltRequest::Deshred(request) => {
            (Rpc::Deshred, "SubscribeDeshred", request.encode_to_vec())
        }
    };
    let subscription_id = sub
        .spec
        .subscription_id
        .as_deref()
        .unwrap_or(sub.name.as_str());
    let mut builder = http::Request::builder()
        .method("POST")
        .uri(format!("http://{}/geyser.Geyser/{path}", context.target))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("user-agent", "ygb-fleet")
        .header("x-subscription-id", subscription_id);
    builder = match sub.spec.compression {
        Compression::None => builder,
        Compression::Zstd => builder.header("grpc-accept-encoding", "zstd"),
        Compression::Gzip => builder.header("grpc-accept-encoding", "gzip"),
    };
    let mut send_request = send_request.clone().ready().await?;
    let (response, mut send_stream) = send_request.send_request(builder.body(())?, false)?;
    let mut frame = BytesMut::with_capacity(5 + payload.len());
    frame.extend_from_slice(&[0]);
    frame.extend_from_slice(&u32::try_from(payload.len())?.to_be_bytes());
    frame.extend_from_slice(&payload);
    send_stream.send_data(frame.freeze(), false)?;
    let response = response.await?;
    anyhow::ensure!(
        response.status() == 200,
        "HTTP status {}",
        response.status()
    );
    let encoding = response
        .headers()
        .get("grpc-encoding")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("identity")
        .to_owned();
    let mut body = response.into_body();
    let mut framer = Framer::default();
    let mut decoder = Decompressor::new(&encoding)?;
    let probe = sub.spec.role == Role::Probe;
    let mut stop_rx = context.stop_rx.clone();
    let churn_at =
        deadline.unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(365 * 86_400));
    loop {
        let chunk = tokio::select! {
            chunk = body.data() => chunk,
            _ = stop_rx.changed() => return Ok(Attempt::Stopped),
            _ = tokio::time::sleep_until(churn_at) => {
                // Dropping both halves resets the stream, as a client reconnect does.
                drop(send_stream);
                return Ok(Attempt::ChurnDue);
            }
        };
        let Some(chunk) = chunk else {
            let trailers = body.trailers().await?;
            let header = |name: &str| {
                trailers
                    .as_ref()
                    .and_then(|trailers| trailers.get(name))
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            return Ok(Attempt::Ended(format!(
                "stream ended, grpc-status {}: {}",
                header("grpc-status"),
                header("grpc-message")
            )));
        };
        let chunk = chunk?;
        let recv_ns = sys::unix_ns();
        let begin = if probe { sys::mono_ns() } else { 0 };
        let _ = body.flow_control().release_capacity(chunk.len());
        report.bytes += chunk.len() as u64;
        if probe {
            framer.push(&chunk, |compressed, message| {
                report.messages += 1;
                let decoded;
                let message = if compressed {
                    match decoder.decompress(message) {
                        Ok(data) => {
                            decoded = data;
                            decoded.as_slice()
                        }
                        Err(error) => {
                            warn!("{}: decompress failed: {error}", sub.name);
                            return;
                        }
                    }
                } else {
                    message
                };
                let scanned = scan::scan(rpc, message);
                let kind = scanned.kind.unwrap_or(Kind::Unknown);
                match report.kinds.get_mut(kind.name()) {
                    Some(count) => *count += 1,
                    None => {
                        report.kinds.insert(kind.name().to_owned(), 1);
                    }
                }
                if let Some(slot) = scanned.slot {
                    context.latest_slot.fetch_max(slot, Ordering::Relaxed);
                }
                if let Some(created_ns) = scanned.created_ns {
                    samples.push(Sample {
                        subscriber: sub.index as u16,
                        kind: kind as u8,
                        status: scanned.status.unwrap_or(u8::MAX),
                        slot: scanned.slot.unwrap_or_default(),
                        created_ns,
                        recv_ns,
                    });
                }
            });
            handle_hist.record(sys::mono_ns() - begin);
        } else {
            report.messages += framer.count(&chunk);
        }
    }
}

/// Splits a gRPC response body into length-prefixed messages across chunk boundaries.
#[derive(Default)]
struct Framer {
    header: [u8; 5],
    header_len: usize,
    remaining: usize,
    compressed: bool,
    message: Vec<u8>,
}

impl Framer {
    fn push(&mut self, mut chunk: &[u8], mut on_message: impl FnMut(bool, &[u8])) {
        while !chunk.is_empty() {
            if self.header_len < 5 {
                let take = (5 - self.header_len).min(chunk.len());
                self.header[self.header_len..self.header_len + take]
                    .copy_from_slice(&chunk[..take]);
                self.header_len += take;
                chunk = &chunk[take..];
                if self.header_len == 5 {
                    self.compressed = self.header[0] == 1;
                    self.remaining =
                        u32::from_be_bytes(self.header[1..5].try_into().expect("4 bytes")) as usize;
                    self.message.clear();
                    if self.remaining == 0 {
                        on_message(self.compressed, &[]);
                        self.header_len = 0;
                    }
                }
                continue;
            }
            let take = self.remaining.min(chunk.len());
            self.message.extend_from_slice(&chunk[..take]);
            self.remaining -= take;
            chunk = &chunk[take..];
            if self.remaining == 0 {
                on_message(self.compressed, &self.message);
                self.header_len = 0;
            }
        }
    }

    /// Counts completed messages without copying their bytes.
    fn count(&mut self, mut chunk: &[u8]) -> u64 {
        let mut completed = 0;
        while !chunk.is_empty() {
            if self.header_len < 5 {
                let take = (5 - self.header_len).min(chunk.len());
                self.header[self.header_len..self.header_len + take]
                    .copy_from_slice(&chunk[..take]);
                self.header_len += take;
                chunk = &chunk[take..];
                if self.header_len == 5 {
                    self.remaining =
                        u32::from_be_bytes(self.header[1..5].try_into().expect("4 bytes")) as usize;
                    if self.remaining == 0 {
                        completed += 1;
                        self.header_len = 0;
                    }
                }
                continue;
            }
            let take = self.remaining.min(chunk.len());
            self.remaining -= take;
            chunk = &chunk[take..];
            if self.remaining == 0 {
                completed += 1;
                self.header_len = 0;
            }
        }
        completed
    }
}

enum Decompressor {
    Identity,
    Zstd(zstd::bulk::Decompressor<'static>),
    Gzip,
}

impl Decompressor {
    fn new(encoding: &str) -> anyhow::Result<Self> {
        Ok(match encoding {
            "identity" => Self::Identity,
            "zstd" => Self::Zstd(zstd::bulk::Decompressor::new()?),
            "gzip" => Self::Gzip,
            other => anyhow::bail!("unsupported grpc-encoding {other}"),
        })
    }

    fn decompress(&mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Identity => Ok(data.to_vec()),
            Self::Zstd(decompressor) => {
                let capacity = zstd::zstd_safe::get_frame_content_size(data)
                    .ok()
                    .flatten()
                    .map_or(data.len() * 8 + 1024, |size| size as usize);
                Ok(decompressor.decompress(data, capacity)?)
            }
            Self::Gzip => {
                let mut out = Vec::new();
                std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(data), &mut out)?;
                Ok(out)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Framer;

    #[test]
    fn sample_log_keeps_order_across_chunks() {
        let mut log = super::SampleLog::default();
        for slot in 0..(super::SAMPLE_CHUNK as u64 * 2 + 5) {
            log.push(super::Sample {
                subscriber: 0,
                kind: 0,
                status: 0,
                slot,
                created_ns: 0,
                recv_ns: 0,
            });
        }
        assert_eq!(log.0.len(), 3);
        assert_eq!(log.len(), super::SAMPLE_CHUNK * 2 + 5);
        assert!(log
            .iter()
            .enumerate()
            .all(|(index, sample)| sample.slot == index as u64));
    }

    #[test]
    fn framer_splits_across_chunks() {
        let mut body = Vec::new();
        for payload in [&b"abc"[..], b"", b"hello world"] {
            body.push(0);
            body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            body.extend_from_slice(payload);
        }
        for split in 1..body.len() {
            let mut framer = Framer::default();
            let mut got = Vec::new();
            for chunk in body.chunks(split) {
                framer.push(chunk, |_, message| got.push(message.to_vec()));
            }
            assert_eq!(got, vec![b"abc".to_vec(), vec![], b"hello world".to_vec()]);
            let mut counter = Framer::default();
            let counted: u64 = body.chunks(split).map(|chunk| counter.count(chunk)).sum();
            assert_eq!(counted, 3);
        }
    }
}
