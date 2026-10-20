//! `ygb fleet`: many concurrent subscribers on a raw HTTP/2 gRPC client.
//! Probes timestamp each message when its last DATA frame arrives, before decompression.

pub mod profile;
pub mod scan;

use {
    crate::{hist::LogHist, sys},
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
        time::Duration,
    },
    tokio::{net::TcpStream, sync::watch},
};

#[derive(Debug, Clone, clap::Args)]
pub struct FleetArgs {
    /// Plaintext gRPC address of the plugin
    #[clap(long, default_value = "127.0.0.1:10000")]
    pub target: String,
    #[clap(long)]
    pub profile: PathBuf,
    /// Output directory for fleet.json and samples.bin
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

/// One received message of a probe. Written to samples.bin as 32-byte little-endian records.
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
        let u64_at = |at: usize| u64::from_le_bytes(record[at..at + 8].try_into().expect("8 bytes"));
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
    pub messages: u64,
    pub bytes: u64,
    pub kinds: BTreeMap<String, u64>,
    pub disconnect: Option<String>,
    /// Time spent handling one received chunk, probes only.
    pub handle_ns: Option<crate::hist::Summary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FleetReport {
    pub target: String,
    pub started_unix_ns: u64,
    pub stopped_unix_ns: u64,
    pub subscribers: Vec<SubscriberReport>,
    pub disconnects: usize,
}

struct Expanded {
    index: usize,
    name: String,
    spec: Subscriber,
}

pub fn run(args: FleetArgs) -> anyhow::Result<()> {
    let profile: Profile = serde_json::from_slice(
        &fs::read(&args.profile).with_context(|| format!("failed to read {}", args.profile.display()))?,
    )?;
    fs::create_dir_all(&args.out)?;
    let expanded: Vec<Expanded> = profile
        .subscribers
        .iter()
        .flat_map(|spec| {
            (0..spec.count).map(move |i| (spec, if spec.count > 1 { format!("{}-{i}", spec.name) } else { spec.name.clone() }))
        })
        .enumerate()
        .map(|(index, (spec, name))| Expanded {
            index,
            name,
            spec: spec.clone(),
        })
        .collect();
    let expanded = args.select(expanded);
    anyhow::ensure!(expanded.len() <= usize::from(u16::MAX), "too many subscribers");
    info!(
        "fleet: {} subscribers ({} probes) against {}",
        expanded.len(),
        expanded.iter().filter(|s| s.spec.role == Role::Probe).count(),
        args.target
    );

    let probe_rt = build_runtime("ygbProbe", args.probe_threads, args.probe_cpus.as_deref())?;
    let load_rt = build_runtime("ygbLoad", args.load_threads, args.load_cpus.as_deref())?;
    let (stop_tx, stop_rx) = watch::channel(false);
    let started_unix_ns = sys::unix_ns();

    let mut handles = Vec::new();
    let (probes, loads): (Vec<_>, Vec<_>) = expanded.into_iter().partition(|s| s.spec.role == Role::Probe);
    for (subscribers, runtime, per_connection) in [
        (probes, &probe_rt, 1),
        (loads, &load_rt, args.streams_per_connection.max(1)),
    ] {
        let mut subscribers = subscribers.into_iter().peekable();
        while subscribers.peek().is_some() {
            let group: Vec<Expanded> = subscribers.by_ref().take(per_connection).collect();
            let args = args.clone();
            let stop_rx = stop_rx.clone();
            handles.push(runtime.spawn(async move { run_connection(&args, group, stop_rx).await }));
        }
    }

    let control = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    control.block_on(async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let duration = args.duration_secs.map_or(Duration::MAX, Duration::from_secs);
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
        let results = probe_rt.block_on(handle).context("connection task panicked")?;
        for (report, mut probe_samples) in results {
            reports.push(report);
            samples.append(&mut probe_samples);
        }
    }
    reports.sort_by_key(|report| report.index);
    let disconnects = reports.iter().filter(|report| report.disconnect.is_some()).count();
    let report = FleetReport {
        target: args.target.clone(),
        started_unix_ns,
        stopped_unix_ns: sys::unix_ns(),
        subscribers: reports,
        disconnects,
    };
    fs::write(args.out.join("fleet.json"), serde_json::to_vec_pretty(&report)?)?;
    let mut writer = BufWriter::new(fs::File::create(args.out.join("samples.bin"))?);
    for sample in &samples {
        sample.write(&mut writer)?;
    }
    writer.flush()?;
    info!(
        "fleet: wrote {} probe samples, {} disconnects",
        samples.len(),
        disconnects
    );
    probe_rt.shutdown_background();
    load_rt.shutdown_background();
    Ok(())
}

fn build_runtime(name: &'static str, threads: usize, cpus: Option<&str>) -> anyhow::Result<tokio::runtime::Runtime> {
    let cpus = cpus.map(sys::parse_cpu_list).transpose()?.unwrap_or_default();
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

type SubscriberResult = (SubscriberReport, Vec<Sample>);

async fn run_connection(
    args: &FleetArgs,
    group: Vec<Expanded>,
    stop_rx: watch::Receiver<bool>,
) -> Vec<SubscriberResult> {
    let connected = async {
        let tcp = TcpStream::connect(&args.target).await?;
        tcp.set_nodelay(true)?;
        let (send_request, connection) = h2::client::Builder::new()
            .initial_window_size(args.stream_window_mib << 20)
            .initial_connection_window_size(args.connection_window_mib << 20)
            .max_frame_size(1 << 20)
            .handshake::<_, Bytes>(tcp)
            .await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                warn!("h2 connection ended: {error}");
            }
        });
        anyhow::Ok(send_request)
    }
    .await;
    let send_request = match connected {
        Ok(send_request) => send_request,
        Err(error) => {
            return group
                .into_iter()
                .map(|sub| (failed_report(&sub, format!("connect: {error:#}")), Vec::new()))
                .collect();
        }
    };
    let streams = group.into_iter().map(|sub| {
        let send_request = send_request.clone();
        let stop_rx = stop_rx.clone();
        let target = args.target.clone();
        tokio::spawn(async move { run_stream(&target, send_request, sub, stop_rx).await })
    });
    let mut results = Vec::new();
    for stream in streams.collect::<Vec<_>>() {
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
        disconnect: Some(reason),
        ..Default::default()
    }
}

async fn run_stream(
    target: &str,
    send_request: h2::client::SendRequest<Bytes>,
    sub: Expanded,
    mut stop_rx: watch::Receiver<bool>,
) -> SubscriberResult {
    let mut report = failed_report(&sub, String::new());
    report.disconnect = None;
    let mut samples = Vec::new();
    let mut handle_hist = LogHist::default();
    let result = async {
        let (rpc, path, payload) = match sub.spec.request.build() {
            BuiltRequest::Subscribe(request) => (Rpc::Subscribe, "Subscribe", request.encode_to_vec()),
            BuiltRequest::Deshred(request) => (Rpc::Deshred, "SubscribeDeshred", request.encode_to_vec()),
        };
        let mut builder = http::Request::builder()
            .method("POST")
            .uri(format!("http://{target}/geyser.Geyser/{path}"))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("user-agent", "ygb-fleet")
            .header("x-subscription-id", sub.name.as_str());
        builder = match sub.spec.compression {
            Compression::None => builder,
            Compression::Zstd => builder.header("grpc-accept-encoding", "zstd"),
            Compression::Gzip => builder.header("grpc-accept-encoding", "gzip"),
        };
        let mut send_request = send_request.ready().await?;
        let (response, mut send_stream) = send_request.send_request(builder.body(())?, false)?;
        let mut frame = BytesMut::with_capacity(5 + payload.len());
        frame.extend_from_slice(&[0]);
        frame.extend_from_slice(&u32::try_from(payload.len())?.to_be_bytes());
        frame.extend_from_slice(&payload);
        send_stream.send_data(frame.freeze(), false)?;
        let response = response.await?;
        anyhow::ensure!(response.status() == 200, "HTTP status {}", response.status());
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
        loop {
            let chunk = tokio::select! {
                chunk = body.data() => chunk,
                _ = stop_rx.changed() => return anyhow::Ok(None),
            };
            let Some(chunk) = chunk else {
                let trailers = body.trailers().await?;
                let status = trailers
                    .as_ref()
                    .and_then(|trailers| trailers.get("grpc-status"))
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("?")
                    .to_owned();
                let message = trailers
                    .as_ref()
                    .and_then(|trailers| trailers.get("grpc-message"))
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                return Ok(Some(format!("stream ended, grpc-status {status}: {message}")));
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
                    *report.kinds.entry(kind.name().to_owned()).or_default() += 1;
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
    .await;
    match result {
        Ok(None) => {}
        Ok(Some(reason)) => report.disconnect = Some(reason),
        Err(error) => report.disconnect = Some(format!("{error:#}")),
    }
    if sub.spec.role == Role::Probe {
        report.handle_ns = Some(handle_hist.summary());
    }
    (report, samples)
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
                self.header[self.header_len..self.header_len + take].copy_from_slice(&chunk[..take]);
                self.header_len += take;
                chunk = &chunk[take..];
                if self.header_len == 5 {
                    self.compressed = self.header[0] == 1;
                    self.remaining = u32::from_be_bytes(self.header[1..5].try_into().expect("4 bytes")) as usize;
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
                self.header[self.header_len..self.header_len + take].copy_from_slice(&chunk[..take]);
                self.header_len += take;
                chunk = &chunk[take..];
                if self.header_len == 5 {
                    self.remaining = u32::from_be_bytes(self.header[1..5].try_into().expect("4 bytes")) as usize;
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
