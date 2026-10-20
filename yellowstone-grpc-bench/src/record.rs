//! `ygb record`: subscribe to a node's processed firehose on two streams and store raw frames.
//! Statuses are taken later from the deshred stream, which carries the callback-time `created_at`.

use {
    crate::{
        codec::RawCodec,
        fixture::{FixtureWriter, Manifest, StreamId, FORMAT_VERSION},
        frames,
    },
    anyhow::Context,
    bytes::Bytes,
    futures::StreamExt,
    log::{info, warn},
    std::{
        collections::{BTreeMap, HashMap},
        path::PathBuf,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    tokio::sync::oneshot,
    tonic::{
        codec::CompressionEncoding,
        transport::{Channel, ClientTlsConfig, Endpoint},
    },
    yellowstone_grpc_proto::geyser::{
        geyser_client::GeyserClient, CommitmentLevel, GetVersionRequest, SubscribeDeshredRequest,
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlockFooter,
        SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterDeshredTransactions,
        SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions,
    },
};

const DEFAULT_ENDPOINT: &str = "https://nyc226.nodes.rpcpool.com:10000";

#[derive(Debug, clap::Args)]
pub struct RecordArgs {
    /// gRPC endpoint of the node to record
    #[clap(long, default_value = DEFAULT_ENDPOINT)]
    endpoint: String,
    /// TLS server name, when the endpoint host is not covered by the node's certificate
    #[clap(long)]
    sni: Option<String>,
    #[clap(long)]
    x_token: Option<String>,
    /// Fixture directory to create
    #[clap(long)]
    out: PathBuf,
    /// Stop after this many BlockMeta updates
    #[clap(long, default_value_t = 3000)]
    slots: u64,
    /// Keep recording this long after the target so the last banks get their commitment statuses
    #[clap(long, default_value_t = 30)]
    tail_secs: u64,
    /// Hard limit on the recording time
    #[clap(long)]
    max_secs: Option<u64>,
    /// Uncompressed size at which a new chunk starts
    #[clap(long, default_value_t = 256)]
    chunk_mib: u64,
    #[clap(long, default_value_t = 3)]
    zstd_level: i32,
    /// Record only the main Subscribe stream
    #[clap(long)]
    no_deshred: bool,
    /// Subscribe to accounts on a separate connection; one connection's receive window caps WAN throughput
    #[clap(long)]
    split_accounts: bool,
    /// Free-form `key=value` notes stored in the manifest
    #[clap(long = "note")]
    notes: Vec<String>,
}

type FrameMsg = (StreamId, u64, Bytes);

pub fn run(args: RecordArgs) -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("ygbRecord")
        .enable_all()
        .build()?
        .block_on(record(args))
}

async fn record(args: RecordArgs) -> anyhow::Result<()> {
    let channel = connect(&args).await?;
    let mut version_client = GeyserClient::new(channel.clone());
    let server_version = version_client
        .get_version(with_token(GetVersionRequest {}, args.x_token.as_deref())?)
        .await
        .context("GetVersion failed")?
        .into_inner()
        .version;
    info!("server version: {server_version}");

    let mut main_request = main_request();
    let accounts_request = args.split_accounts.then(|| SubscribeRequest {
        accounts: std::mem::take(&mut main_request.accounts),
        commitment: main_request.commitment,
        ..Default::default()
    });
    let deshred_request = deshred_request();
    let mut manifest = Manifest {
        format: FORMAT_VERSION,
        endpoint: args.endpoint.clone(),
        sni: args.sni.clone(),
        server_version: Some(server_version),
        started_unix_ns: unix_ns(),
        ..Default::default()
    };
    manifest
        .requests
        .insert("main".to_owned(), format!("{main_request:?}"));
    if let Some(request) = &accounts_request {
        manifest
            .requests
            .insert("main-accounts".to_owned(), format!("{request:?}"));
    }
    if !args.no_deshred {
        manifest
            .requests
            .insert("deshred".to_owned(), format!("{deshred_request:?}"));
    }
    for note in &args.notes {
        let (key, value) = note
            .split_once('=')
            .with_context(|| format!("note must be key=value: {note}"))?;
        manifest.notes.insert(key.to_owned(), value.to_owned());
    }

    let writer = FixtureWriter::create(&args.out, args.chunk_mib << 20, args.zstd_level)?;
    let (frames_tx, frames_rx) = crossbeam_channel::unbounded::<FrameMsg>();
    let (target_tx, target_rx) = oneshot::channel::<()>();
    let writer_thread = {
        let out = args.out.clone();
        let manifest = manifest.clone();
        let target_slots = args.slots;
        std::thread::Builder::new()
            .name("ygbRecWriter".to_owned())
            .spawn(move || writer_loop(frames_rx, writer, target_slots, target_tx, manifest, out))?
    };

    let mut tasks = vec![tokio::spawn(stream_loop(
        channel.clone(),
        StreamId::Main,
        "/geyser.Geyser/Subscribe",
        main_request,
        args.x_token.clone(),
        frames_tx.clone(),
    ))];
    if let Some(request) = accounts_request {
        tasks.push(tokio::spawn(stream_loop(
            connect(&args).await?,
            StreamId::MainAccounts,
            "/geyser.Geyser/Subscribe",
            request,
            args.x_token.clone(),
            frames_tx.clone(),
        )));
    }
    if !args.no_deshred {
        // A second connection keeps the streams from sharing one TCP flow and its windows.
        let deshred_channel = connect(&args).await?;
        tasks.push(tokio::spawn(stream_loop(
            deshred_channel,
            StreamId::Deshred,
            "/geyser.Geyser/SubscribeDeshred",
            deshred_request,
            args.x_token.clone(),
            frames_tx.clone(),
        )));
    }
    drop(frames_tx);

    let max_wait = args.max_secs.map_or(Duration::MAX, Duration::from_secs);
    let mut streams = futures::future::select_all(tasks.iter_mut());
    let end_reason = tokio::select! {
        _ = target_rx => {
            info!("target of {} BlockMeta reached, recording {} s more", args.slots, args.tail_secs);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(args.tail_secs)) => "target reached".to_owned(),
                (result, _, _) = &mut streams => format!("stream ended during tail: {}", task_error(result)),
            }
        }
        (result, _, _) = &mut streams => format!("stream ended: {}", task_error(result)),
        _ = tokio::time::sleep(max_wait) => "max_secs reached".to_owned(),
        _ = tokio::signal::ctrl_c() => "interrupted".to_owned(),
    };
    drop(streams);
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
    info!("stopping: {end_reason}");

    let (chunks, stats) = writer_thread
        .join()
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
    manifest.end_reason = end_reason;
    manifest.finished_unix_ns = unix_ns();
    manifest.chunks = chunks;
    stats.apply(&mut manifest);
    manifest.save(&args.out)?;
    info!(
        "fixture written to {}: {} chunks, {} BlockMeta, slots {:?}..{:?}",
        args.out.display(),
        manifest.chunks.len(),
        manifest.block_meta_count,
        manifest.first_slot,
        manifest.last_slot
    );
    Ok(())
}

fn task_error(result: Result<anyhow::Result<()>, tokio::task::JoinError>) -> String {
    match result {
        Ok(Ok(())) => "closed".to_owned(),
        Ok(Err(error)) => format!("{error:#}"),
        Err(error) => format!("task failed: {error}"),
    }
}

async fn connect(args: &RecordArgs) -> anyhow::Result<Channel> {
    let mut endpoint = Endpoint::from_shared(args.endpoint.clone())?
        .connect_timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .initial_stream_window_size(32 << 20)
        .initial_connection_window_size(64 << 20)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20));
    if args.endpoint.starts_with("https://") {
        let mut tls = ClientTlsConfig::new().with_native_roots();
        if let Some(sni) = &args.sni {
            tls = tls.domain_name(sni.clone());
        }
        endpoint = endpoint.tls_config(tls)?;
    }
    endpoint
        .connect()
        .await
        .with_context(|| format!("failed to connect to {}", args.endpoint))
}

fn with_token<T>(message: T, x_token: Option<&str>) -> anyhow::Result<tonic::Request<T>> {
    let mut request = tonic::Request::new(message);
    if let Some(token) = x_token {
        request.metadata_mut().insert("x-token", token.parse()?);
    }
    Ok(request)
}

fn main_request() -> SubscribeRequest {
    let all_slots = SubscribeRequestFilterSlots {
        filter_by_commitment: Some(false),
        interslot_updates: Some(true),
    };
    SubscribeRequest {
        accounts: HashMap::from([("all".to_owned(), SubscribeRequestFilterAccounts::default())]),
        slots: HashMap::from([("all".to_owned(), all_slots)]),
        transactions: HashMap::from([(
            "all".to_owned(),
            SubscribeRequestFilterTransactions::default(),
        )]),
        blocks_meta: HashMap::from([("all".to_owned(), SubscribeRequestFilterBlocksMeta {})]),
        entry: HashMap::from([(
            "all".to_owned(),
            SubscribeRequestFilterEntry {
                include_update_parent: Some(true),
            },
        )]),
        block_footer: HashMap::from([(
            "all".to_owned(),
            SubscribeRequestFilterBlockFooter {
                include_certificates: Some(true),
            },
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    }
}

fn deshred_request() -> SubscribeDeshredRequest {
    SubscribeDeshredRequest {
        deshred_transactions: HashMap::from([(
            "all".to_owned(),
            SubscribeRequestFilterDeshredTransactions {
                include_update_parent: Some(true),
                ..Default::default()
            },
        )]),
        ping: None,
        slots: HashMap::from([(
            "all".to_owned(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            },
        )]),
    }
}

async fn stream_loop<R>(
    channel: Channel,
    stream: StreamId,
    path: &'static str,
    request: R,
    x_token: Option<String>,
    frames_tx: crossbeam_channel::Sender<FrameMsg>,
) -> anyhow::Result<()>
where
    R: prost::Message + Send + Sync + 'static,
{
    let mut grpc = tonic::client::Grpc::new(channel)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(1 << 30);
    grpc.ready().await.context("channel not ready")?;
    // The server ends a subscription on half-close, so the request stream stays open.
    let requests = futures::stream::once(async move { request }).chain(futures::stream::pending());
    let response = grpc
        .streaming(
            with_token(requests, x_token.as_deref())?,
            http::uri::PathAndQuery::from_static(path),
            RawCodec::<R>::default(),
        )
        .await
        .with_context(|| format!("{} subscribe failed", stream.name()))?;
    let mut messages = response.into_inner();
    while let Some(message) = messages
        .message()
        .await
        .with_context(|| format!("{} stream error", stream.name()))?
    {
        frames_tx
            .send((stream, unix_ns(), message))
            .context("writer stopped")?;
    }
    anyhow::bail!("{} stream closed by the server", stream.name())
}

#[derive(Default)]
struct WriterStats {
    counts: BTreeMap<String, u64>,
    bytes: BTreeMap<String, u64>,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    block_meta_count: u64,
}

impl WriterStats {
    fn add(&mut self, stream: StreamId, info: &frames::FrameInfo, len: usize) {
        let key = format!("{}.{}", stream.name(), info.kind);
        *self.bytes.entry(key.clone()).or_default() += len as u64;
        *self.counts.entry(key).or_default() += 1;
        if let Some(slot) = info.slot {
            self.first_slot = Some(self.first_slot.map_or(slot, |first| first.min(slot)));
            self.last_slot = Some(self.last_slot.map_or(slot, |last| last.max(slot)));
        }
        if stream != StreamId::Deshred && info.kind == "block_meta" {
            self.block_meta_count += 1;
        }
    }

    fn apply(&self, manifest: &mut Manifest) {
        manifest.counts = self.counts.clone();
        manifest.bytes = self.bytes.clone();
        manifest.first_slot = self.first_slot;
        manifest.last_slot = self.last_slot;
        manifest.block_meta_count = self.block_meta_count;
    }
}

/// Lag of received frames behind their server `created_at`, per stream, over one log window.
#[derive(Default)]
struct LagWindow {
    frames: u64,
    bytes: u64,
    lag_sum_ms: f64,
    lag_max_ms: f64,
}

fn writer_loop(
    frames_rx: crossbeam_channel::Receiver<FrameMsg>,
    mut writer: FixtureWriter,
    target_slots: u64,
    target_tx: oneshot::Sender<()>,
    mut manifest: Manifest,
    out: PathBuf,
) -> anyhow::Result<(Vec<crate::fixture::ChunkInfo>, WriterStats)> {
    let mut target_tx = Some(target_tx);
    let mut stats = WriterStats::default();
    let mut windows: BTreeMap<StreamId, LagWindow> = BTreeMap::new();
    let mut last_log = Instant::now();
    for (stream, recv_ns, data) in frames_rx {
        let info = frames::decode(stream, &data)
            .map(|decoded| frames::info(&decoded))
            .unwrap_or_else(|error| {
                warn!("undecodable {} frame: {error}", stream.name());
                frames::FrameInfo {
                    kind: "undecodable",
                    ..Default::default()
                }
            });
        if writer.write(stream, recv_ns, &data, info.slot)? {
            manifest.chunks = writer.chunks().to_vec();
            stats.apply(&mut manifest);
            manifest.end_reason = "recording".to_owned();
            manifest.save(&out)?;
        }
        stats.add(stream, &info, data.len());
        if stats.block_meta_count >= target_slots {
            if let Some(tx) = target_tx.take() {
                let _ = tx.send(());
            }
        }

        let window = windows.entry(stream).or_default();
        window.frames += 1;
        window.bytes += data.len() as u64;
        if let Some(created_at_ns) = info.created_at_ns {
            let lag_ms = (recv_ns as f64 - created_at_ns as f64) / 1e6;
            window.lag_sum_ms += lag_ms;
            window.lag_max_ms = window.lag_max_ms.max(lag_ms);
        }
        if last_log.elapsed() >= Duration::from_secs(10) {
            let secs = last_log.elapsed().as_secs_f64();
            for (stream, window) in &windows {
                info!(
                    "{}: {:.0} msg/s, {:.1} MiB/s, lag mean {:.1} ms max {:.1} ms | BlockMeta {}/{} slot {:?}",
                    stream.name(),
                    window.frames as f64 / secs,
                    window.bytes as f64 / secs / (1 << 20) as f64,
                    window.lag_sum_ms / window.frames.max(1) as f64,
                    window.lag_max_ms,
                    stats.block_meta_count,
                    target_slots,
                    stats.last_slot,
                );
            }
            windows.clear();
            last_log = Instant::now();
        }
    }
    Ok((writer.finish()?, stats))
}

fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or_default()
}
