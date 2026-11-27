use {
    crate::{
        config::ConfigPrometheus,
        extension::seams::ThreadRole,
        plugin::{
            filter::{Filter, FilterStats},
            message::SlotStatus,
        },
        telemetry::{
            data_plane_counts::{DataPlaneCount, DataPlaneCounts, OwnerQueue, DATA_PLANE_COUNTS},
            safety_summary::SafetySummary,
            slot_status_gauges::{
                agave_slot_status_index, agave_slot_status_labels, plugin_slot_status_index,
                plugin_slot_status_labels, SlotStatusGauges, AGAVE_SLOT_STATUS_GAUGES,
                PLUGIN_SLOT_STATUS_GAUGES, SLOT_STATUS_KINDS,
            },
            stream_counters::{
                empty_family, label_pair, SubscriberSeriesCollector, SUBSCRIBER_COUNTS,
            },
        },
        threads::PluginThreads,
        version::Version,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus as GeyserSlosStatus,
    http_body_util::{combinators::BoxBody, BodyExt, Empty as BodyEmpty, Full as BodyFull},
    hyper::{
        body::{Bytes, Incoming as BodyIncoming},
        service::service_fn,
        Request, Response, StatusCode,
    },
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    log::{debug, error, info, warn},
    prometheus::{
        core::{Collector, Desc},
        proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType},
        Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
        Opts, Registry, TextEncoder,
    },
    std::{collections::HashMap, convert::Infallible, net::SocketAddr, sync::Once, time::Duration},
    tokio::{
        net::TcpListener,
        time::{Instant, Interval, MissedTickBehavior},
    },
    tokio_util::sync::CancellationToken,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("yellowstone_grpc_geyser_version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    static ref INVALID_FULL_BLOCKS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("yellowstone_grpc_geyser_invalid_full_blocks_total", "Total number of fails on constructin full blocks"),
        &["reason"]
    ).unwrap();

    static ref SUBSCRIPTIONS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("yellowstone_grpc_geyser_subscriptions_total", "Total number of subscriptions to gRPC service"),
        &["endpoint", "subscription"]
    ).unwrap();

    static ref MISSED_STATUS_MESSAGE: IntCounterVec = IntCounterVec::new(
        Opts::new("yellowstone_grpc_geyser_missed_status_message_total", "Number of missed messages by commitment"),
        &["status"]
    ).unwrap();

    static ref GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "yellowstone_grpc_geyser_subscriber_send_bandwidth_load",
            "Current Send load we send to subscriber channel (in bytes per second)"
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref GRPC_CLIENT_DISCONNECTS: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_geyser_client_disconnects_total",
            "Total client disconnections by reason"
        ),
        &["subscriber_id", "reason"]
    ).unwrap();

    static ref GEYSER_ACCOUNT_UPDATE_RECEIVED: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "yellowstone_grpc_geyser_account_update_data_size_kib",
            "Histogram of all account update data (kib) received from Geyser plugin"
        )
        .buckets(vec![5.0, 10.0, 20.0, 30.0, 50.0, 100.0, 200.0, 300.0, 500.0, 1000.0, 2000.0, 3000.0, 5000.0, 10000.0])
    ).unwrap();

    static ref PRE_ENCODED_CACHE: PreEncodedCacheCollector =
        PreEncodedCacheCollector::new(&DATA_PLANE_COUNTS);

    static ref GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "yellowstone_grpc_geyser_concurrent_subscribe_per_subscriber_id",
            "Current concurrent subscriptions per subscriber ID"
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref GRPC_METHOD_CALL_COUNT: IntCounterVec = IntCounterVec::new(
        Opts::new("yellowstone_grpc_geyser_method_call_count", "Total number of calls to GetVersion gRPC method"),
        &["method"]
    ).unwrap();

    static ref GRPC_SUBSCRIPTION_LIMIT_EXCEEDED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_geyser_subscription_limit_exceeded_total",
            "Number of subscribe attempts that exceeded the per-subscriber limit"
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref GRPC_SERVICE_OUTBOUND_BYTES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("yellowstone_grpc_geyser_service_outbound_bytes", "Current emitted bytes by tonic service response bodies per active subscriber stream"),
        &["subscriber_id", "uri_path"]
    ).unwrap();

    static ref GEYSER_BLOCK_MISMATCH_TRANSACTION: IntCounter = IntCounter::new(
        "yellowstone_grpc_geyser_block_mismatch_transaction", "Number of block mismatch transactions encountered in the plugin"
    ).unwrap();

    static ref GEYSER_UNTRACK_SLOT_EVENT_DROPPED: IntCounter = IntCounter::new(
        "yellowstone_grpc_geyser_untrack_slot_event_dropped_total", "Number of geyser event drop due to untrack slot"
    ).unwrap();

    static ref IP_CONNCUR_RATE_LIMIT_EXCEEDED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_geyser_ip_concurrent_rate_limit_exceeded_total",
            "Number of incoming connections that exceeded the per-IP connection limit at the transport layer"
        ),
        &["remote_peer_ip"]
    ).unwrap();

    static ref UNAUTHORIZED_COUNT: IntCounter = IntCounter::new(
        "yellowstone_grpc_geyser_unauthorized_count_total",
        "Number of unauthorized requests to the gRPC service"
    ).unwrap();

    static ref AUTHORIZED_COUNT: IntCounter = IntCounter::new(
        "yellowstone_grpc_geyser_authorized_count_total",
        "Number of authorized requests to the gRPC service"
    ).unwrap();

    static ref METHOD_RATELIMITED_COUNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_geyser_method_ratelimited_count_total",
            "Number of requests that were rate limited by the method ratelimiter"
        ),
        &["subscriber_id", "method"]
    ).unwrap();

    static ref FILTER_STATS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_geyser_filter_stats",
            "Distribution of filter stats dimensions"
        ).buckets(vec![
            0.0,
            1.0,
            10.0,
            50.0,
            100.0,
            1000.0,
            2000.0,
            5000.0,
            10000.0,
            20000.0,
            50000.0,
            100000.0,
            175000.0,
            250000.0,
            375000.0,
            500000.0,
            800000.0,
            1_000_000.0,
            2_500_000.0,
        ]),
        &["subscriber_id", "type", "prop"]
    ).unwrap();
}

/// The collector of every baseline family, the families each load registers, in order.
fn baseline_collectors() -> Vec<Box<dyn Collector>> {
    vec![
        Box::new(VERSION.clone()),
        Box::new(INVALID_FULL_BLOCKS.clone()),
        Box::new(SUBSCRIPTIONS_TOTAL.clone()),
        Box::new(MISSED_STATUS_MESSAGE.clone()),
        Box::new(SubscriberSeriesCollector::new(&SUBSCRIBER_COUNTS)),
        Box::new(GEYSER_ACCOUNT_UPDATE_RECEIVED.clone()),
        Box::new(GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD.clone()),
        Box::new(GRPC_CLIENT_DISCONNECTS.clone()),
        Box::new(PRE_ENCODED_CACHE.clone()),
        Box::new(GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID.clone()),
        Box::new(GRPC_METHOD_CALL_COUNT.clone()),
        Box::new(GRPC_SUBSCRIPTION_LIMIT_EXCEEDED.clone()),
        Box::new(GRPC_SERVICE_OUTBOUND_BYTES.clone()),
        Box::new(DataPathGaugesCollector::new(
            &DATA_PLANE_COUNTS,
            &AGAVE_SLOT_STATUS_GAUGES,
            &PLUGIN_SLOT_STATUS_GAUGES,
        )),
        Box::new(GEYSER_BLOCK_MISMATCH_TRANSACTION.clone()),
        Box::new(GEYSER_UNTRACK_SLOT_EVENT_DROPPED.clone()),
        Box::new(IP_CONNCUR_RATE_LIMIT_EXCEEDED.clone()),
        Box::new(UNAUTHORIZED_COUNT.clone()),
        Box::new(AUTHORIZED_COUNT.clone()),
        Box::new(METHOD_RATELIMITED_COUNT.clone()),
        Box::new(FILTER_STATS.clone()),
    ]
}

pub struct PrometheusService;

/// How long the metrics server waits after a failed accept, such as out of file descriptors.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(100);

impl PrometheusService {
    /// Registers the families once and starts `solGrpcMetrics`, which serves `/metrics` on its
    /// own runtime when a `prometheus` section binds a listener, and writes the safety summary
    /// every `summary_interval` that is not zero. Returns the bound address, if any.
    pub fn spawn(
        config: Option<ConfigPrometheus>,
        summary_interval: Duration,
        cancellation_token: CancellationToken,
        version: &Version,
        plugin_threads: &PluginThreads,
    ) -> std::io::Result<Option<SocketAddr>> {
        static REGISTER: Once = Once::new();
        REGISTER.call_once(|| {
            for baseline_collector in baseline_collectors() {
                REGISTRY
                    .register(baseline_collector)
                    .expect("collector can't be registered");
            }

            VERSION
                .with_label_values(&[
                    version.buildts,
                    version.git,
                    version.package,
                    version.proto,
                    version.rustc,
                    version.solana,
                    version.version,
                ])
                .inc();
        });

        // Taken on the loading thread, after the load's reset of the counts.
        let safety_summary = (!summary_interval.is_zero()).then(|| {
            (
                SafetySummary::new(&DATA_PLANE_COUNTS, Instant::now()),
                summary_interval,
            )
        });
        let std_listener = match config {
            Some(ConfigPrometheus { address }) => {
                let std_listener = std::net::TcpListener::bind(address)?;
                std_listener.set_nonblocking(true)?;
                Some(std_listener)
            }
            None => None,
        };
        let bound_address = std_listener
            .as_ref()
            .map(std::net::TcpListener::local_addr)
            .transpose()?;
        if bound_address.is_none() && safety_summary.is_none() {
            return Ok(None);
        }
        // A listener's start error fails the load as a bind error does; the summary's alone logs.
        let summary_only = bound_address.is_none();
        let metrics_runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(metrics_runtime) => metrics_runtime,
            Err(build_error) if summary_only => {
                error!("the safety summary has no runtime: {build_error}");
                return Ok(None);
            }
            Err(build_error) => return Err(build_error),
        };
        if let Some(bound_address) = bound_address {
            info!("start prometheus server: {bound_address}");
        }
        let thread_start =
            plugin_threads.spawn_plugin_thread(ThreadRole::MetricsRender, 0, move || {
                serve_metrics_on_this_thread(
                    metrics_runtime,
                    std_listener,
                    safety_summary,
                    cancellation_token,
                )
            });
        match thread_start {
            // The plugin token's cancellation ends the thread; `stop_and_join_all` joins it.
            Ok(metrics_thread) => drop(metrics_thread),
            Err(spawn_error) if summary_only => {
                error!("the safety summary has no thread: {spawn_error}");
                return Ok(None);
            }
            Err(spawn_error) => return Err(spawn_error),
        }
        Ok(bound_address)
    }
}

/// Runs `solGrpcMetrics`'s loop on `metrics_runtime`, a current-thread runtime, so a render, its
/// response write and the summary hold only this thread.
fn serve_metrics_on_this_thread(
    metrics_runtime: tokio::runtime::Runtime,
    std_listener: Option<std::net::TcpListener>,
    safety_summary: Option<(SafetySummary, Duration)>,
    cancellation_token: CancellationToken,
) {
    metrics_runtime.block_on(async move {
        let scrape_listener = std_listener.and_then(|std_listener| {
            TcpListener::from_std(std_listener)
                .inspect_err(|listener_error| {
                    error!("failed to register the metrics listener: {listener_error}");
                })
                .ok()
        });
        run_metrics_loop(
            scrape_listener,
            safety_summary,
            cancellation_token,
            &mut |summary_line| warn!("{summary_line}"),
        )
        .await;
        info!("metrics thread exiting");
    });
}

/// Serves `/metrics` on `scrape_listener` and writes each summary line through
/// `write_summary_line`, one interval after another, until the cancellation writes a last one.
pub async fn run_metrics_loop(
    scrape_listener: Option<TcpListener>,
    mut safety_summary: Option<(SafetySummary, Duration)>,
    cancellation_token: CancellationToken,
    write_summary_line: &mut dyn FnMut(&str),
) {
    // A period too long to add to now never ticks; the last summary still follows the cancellation.
    let mut summary_interval = safety_summary.as_ref().and_then(|(_, summary_period)| {
        let first_tick_at = Instant::now().checked_add(*summary_period)?;
        let mut summary_interval = tokio::time::interval_at(first_tick_at, *summary_period);
        summary_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Some(summary_interval)
    });
    loop {
        tokio::select! {
            () = cancellation_token.cancelled() => break,
            accept_result = accept_next_scrape(scrape_listener.as_ref()) => match accept_result {
                Ok((scrape_stream, _)) => {
                    tokio::spawn(serve_metrics_connection(scrape_stream));
                }
                Err(accept_error) => {
                    // This is the instance's only metrics server, so it keeps accepting.
                    error!("failed to accept new connection: {accept_error}");
                    tokio::time::sleep(ACCEPT_ERROR_PAUSE).await;
                }
            },
            () = next_summary_tick(summary_interval.as_mut()) => {
                if let Some((safety_summary, _)) = &mut safety_summary {
                    if let Some(summary_line) = safety_summary.next_line(Instant::now()) {
                        write_summary_line(&summary_line);
                    }
                }
            }
        }
    }
    if let Some((safety_summary, _)) = &mut safety_summary {
        if let Some(summary_line) = safety_summary.next_line(Instant::now()) {
            write_summary_line(&summary_line);
        }
    }
}

async fn accept_next_scrape(
    scrape_listener: Option<&TcpListener>,
) -> std::io::Result<(tokio::net::TcpStream, SocketAddr)> {
    match scrape_listener {
        Some(scrape_listener) => scrape_listener.accept().await,
        None => std::future::pending().await,
    }
}

async fn next_summary_tick(summary_interval: Option<&mut Interval>) {
    match summary_interval {
        Some(summary_interval) => {
            summary_interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// One scraper's connection; the runtime's drop ends it at unload.
async fn serve_metrics_connection(stream: tokio::net::TcpStream) {
    let peer_addr = stream.peer_addr().ok();
    debug!(
        "Prometheus server accepted new connection from {:?}",
        peer_addr
    );
    if let Err(error) = ServerBuilder::new(TokioExecutor::new())
        .serve_connection(
            TokioIo::new(stream),
            service_fn(move |req: Request<BodyIncoming>| async move {
                match req.uri().path() {
                    "/metrics" => metrics_handler(),
                    _ => not_found_handler(),
                }
            }),
        )
        .await
    {
        error!("failed to handle request: {error}");
    }
    debug!("Prometheus server finished connection from {:?}", peer_addr);
}

fn metrics_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    Response::builder()
        .status(StatusCode::OK)
        .body(BodyFull::new(Bytes::from(metrics)).boxed())
}

fn not_found_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(BodyEmpty::new().boxed())
}

pub fn observe_filter_complexity(subscriber_id: &str, observation: &FilterStats) {
    FILTER_STATS
        .with_label_values(&[subscriber_id, "accounts", "len"])
        .observe(observation.accounts.len() as f64);
    for score in &observation.accounts {
        FILTER_STATS
            .with_label_values(&[subscriber_id, "accounts", "accounts_len"])
            .observe(score.accounts_len as f64);
        FILTER_STATS
            .with_label_values(&[subscriber_id, "accounts", "owners_len"])
            .observe(score.owners_len as f64);
    }

    FILTER_STATS
        .with_label_values(&[subscriber_id, "transactions", "len"])
        .observe(observation.transactions.len() as f64);
    for score in &observation.transactions {
        FILTER_STATS
            .with_label_values(&[subscriber_id, "transactions", "accounts_include_len"])
            .observe(score.accounts_include_len as f64);
        FILTER_STATS
            .with_label_values(&[subscriber_id, "transactions", "accounts_exclude_len"])
            .observe(score.accounts_exclude_len as f64);
        FILTER_STATS
            .with_label_values(&[subscriber_id, "transactions", "accounts_required_len"])
            .observe(score.accounts_required_len as f64);
        FILTER_STATS
            .with_label_values(&[subscriber_id, "transactions", "token_accounts_enabled"])
            .observe(if score.token_accounts_enabled {
                1.0
            } else {
                0.0
            });
    }

    FILTER_STATS
        .with_label_values(&[subscriber_id, "transaction_status", "len"])
        .observe(observation.transaction_status.len() as f64);
    for score in &observation.transaction_status {
        FILTER_STATS
            .with_label_values(&[subscriber_id, "transaction_status", "accounts_include_len"])
            .observe(score.accounts_include_len as f64);
        FILTER_STATS
            .with_label_values(&[subscriber_id, "transaction_status", "accounts_exclude_len"])
            .observe(score.accounts_exclude_len as f64);
        FILTER_STATS
            .with_label_values(&[subscriber_id, "transaction_status", "accounts_required_len"])
            .observe(score.accounts_required_len as f64);
        FILTER_STATS
            .with_label_values(&[
                subscriber_id,
                "transaction_status",
                "token_accounts_enabled",
            ])
            .observe(if score.token_accounts_enabled {
                1.0
            } else {
                0.0
            });
    }

    FILTER_STATS
        .with_label_values(&[subscriber_id, "blocks", "len"])
        .observe(observation.blocks.len() as f64);
    for score in &observation.blocks {
        FILTER_STATS
            .with_label_values(&[subscriber_id, "blocks", "include_len"])
            .observe(score.include_len as f64);
        FILTER_STATS
            .with_label_values(&[subscriber_id, "blocks", "exclude_len"])
            .observe(score.exclude_len as f64);
    }
}

pub fn incr_ip_conncur_rate_limit_exceeded(remote_peer_ip: Option<String>) {
    IP_CONNCUR_RATE_LIMIT_EXCEEDED
        .with_label_values(&[remote_peer_ip.as_deref().unwrap_or("unknown")])
        .inc();
}

pub fn incr_unauthorized_count() {
    UNAUTHORIZED_COUNT.inc();
}

pub fn incr_authorized_count() {
    AUTHORIZED_COUNT.inc();
}

pub fn incr_method_ratelimited_count<S1: AsRef<str>, S2: AsRef<str>>(
    subscriber_id: S1,
    method: S2,
) {
    METHOD_RATELIMITED_COUNT
        .with_label_values(&[subscriber_id.as_ref(), method.as_ref()])
        .inc();
}

/// Counts an account update that `accounts.owner_reject` dropped.
pub fn incr_account_event_dropped() {
    DATA_PLANE_COUNTS.add(DataPlaneCount::AccountEventDropped, 1);
}

pub fn incr_geyser_block_mismatch_transaction() {
    GEYSER_BLOCK_MISMATCH_TRANSACTION.inc();
}

pub fn incr_geyser_untrack_slot_event_dropped() {
    GEYSER_UNTRACK_SLOT_EVENT_DROPPED.inc();
}

pub fn update_slot_status(status: &GeyserSlosStatus, slot: u64) {
    AGAVE_SLOT_STATUS_GAUGES.set(agave_slot_status_index(status), slot);
}

pub fn update_slot_plugin_status(status: SlotStatus, slot: u64) {
    PLUGIN_SLOT_STATUS_GAUGES.set(plugin_slot_status_index(status), slot);
}

pub fn update_invalid_blocks(reason: impl AsRef<str>) {
    INVALID_FULL_BLOCKS
        .with_label_values(&[reason.as_ref()])
        .inc();
    INVALID_FULL_BLOCKS.with_label_values(&["all"]).inc();
}

pub fn message_queue_size_inc() {
    DATA_PLANE_COUNTS.add(DataPlaneCount::OwnerQueueEntered(OwnerQueue::GeyserLoop), 1);
}

pub fn block_reconstruction_queue_size_inc() {
    let block_queue = OwnerQueue::BlockReconstruction;
    DATA_PLANE_COUNTS.add(DataPlaneCount::OwnerQueueEntered(block_queue), 1);
}

pub fn message_queue_size_dec() {
    message_queue_size_dec_by(1);
}

pub fn message_queue_size_dec_by(amount: u64) {
    DATA_PLANE_COUNTS.add(
        DataPlaneCount::OwnerQueueLeft(OwnerQueue::GeyserLoop),
        amount,
    );
}

pub fn block_reconstruction_queue_size_dec_by(amount: u64) {
    let block_queue = OwnerQueue::BlockReconstruction;
    DATA_PLANE_COUNTS.add(DataPlaneCount::OwnerQueueLeft(block_queue), amount);
}

pub fn subscription_limit_exceeded_inc<S: AsRef<str>>(subscriber_id: S) {
    GRPC_SUBSCRIPTION_LIMIT_EXCEEDED
        .with_label_values(&[subscriber_id.as_ref()])
        .inc();
}

pub fn update_subscriptions(endpoint: &str, old: Option<&Filter>, new: Option<&Filter>) {
    for (multiplier, filter) in [(-1, old), (1, new)] {
        if let Some(filter) = filter {
            SUBSCRIPTIONS_TOTAL
                .with_label_values(&[endpoint, "grpc_total"])
                .add(multiplier);

            for (name, value) in filter.get_metrics() {
                SUBSCRIPTIONS_TOTAL
                    .with_label_values(&[endpoint, name])
                    .add((value as i64) * multiplier);
            }
        }
    }
}

pub fn missed_status_message_inc(status: SlotStatus) {
    MISSED_STATUS_MESSAGE
        .with_label_values(&[status.as_str()])
        .inc()
}

pub fn observe_geyser_account_update_received(data_bytesize: usize) {
    GEYSER_ACCOUNT_UPDATE_RECEIVED.observe(data_bytesize as f64 / 1024.0);
}

pub fn set_subscriber_send_bandwidth_load<S: AsRef<str>>(subscriber_id: S, load: i64) {
    GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD
        .with_label_values(&[subscriber_id.as_ref()])
        .set(load);
}

/// Reads the `yellowstone_grpc_geyser_client_disconnects_total` counter for one subscriber
/// and reason.
///
/// # Arguments
///
/// * `subscriber_id` - The `subscriber_id` label.
/// * `reason` - The `reason` label, for example `request_too_large`.
///
/// # Returns
///
/// The counter value, `0` if that label pair was never counted.
#[cfg(test)]
pub fn client_disconnect_count(subscriber_id: &str, reason: &str) -> u64 {
    GRPC_CLIENT_DISCONNECTS
        .with_label_values(&[subscriber_id, reason])
        .get()
}

pub fn incr_client_disconnect<S: AsRef<str>>(subscriber_id: S, reason: &str) {
    GRPC_CLIENT_DISCONNECTS
        .with_label_values(&[subscriber_id.as_ref(), reason])
        .inc();
}

/// `pre_encoded_cache_hit` and `_miss{type="txn"}` from a set of striped counts, each series once
/// it counts. Accounts have no cache to count: capture builds their bodies.
#[derive(Clone)]
struct PreEncodedCacheCollector {
    hit_desc: Desc,
    miss_desc: Desc,
    data_plane_counts: &'static DataPlaneCounts,
}

impl PreEncodedCacheCollector {
    fn new(data_plane_counts: &'static DataPlaneCounts) -> Self {
        let counter_desc = |fq_name: &str, help: &str| {
            Desc::new(
                fq_name.to_owned(),
                help.to_owned(),
                vec!["type".to_owned()],
                HashMap::new(),
            )
            .expect("a valid family name and label")
        };
        Self {
            hit_desc: counter_desc(
                "yellowstone_grpc_geyser_pre_encoded_cache_hit",
                "Pre-encoded cache hits by message type",
            ),
            miss_desc: counter_desc(
                "yellowstone_grpc_geyser_pre_encoded_cache_miss",
                "Pre-encoded cache misses by message type",
            ),
            data_plane_counts,
        }
    }
}

impl Collector for PreEncodedCacheCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.hit_desc, &self.miss_desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        [
            (&self.hit_desc, DataPlaneCount::PreEncodedTransactionHit),
            (&self.miss_desc, DataPlaneCount::PreEncodedTransactionMiss),
        ]
        .into_iter()
        .map(|(counter_desc, data_plane_count)| {
            let mut metric_family = MetricFamily::default();
            metric_family.set_name(counter_desc.fq_name.clone());
            metric_family.set_help(counter_desc.help.clone());
            metric_family.set_field_type(MetricType::COUNTER);
            let count_sum = self.data_plane_counts.sum(data_plane_count);
            if count_sum > 0 {
                let mut type_label = LabelPair::default();
                type_label.set_name("type".to_owned());
                type_label.set_value("txn".to_owned());
                let mut series_counter = Counter::default();
                series_counter.set_value(count_sum as f64);
                let mut series_metric = Metric::default();
                series_metric.set_label(vec![type_label]);
                series_metric.set_counter(series_counter);
                metric_family.mut_metric().push(series_metric);
            }
            metric_family
        })
        .collect()
    }
}

/// The families the data path keeps in striped words and slot status words: the two owner queue
/// gauges, the slot status gauges, dropped events and sent traffic, each as 8666a6c renders it.
struct DataPathGaugesCollector {
    message_queue_desc: Desc,
    block_queue_desc: Desc,
    slot_status_desc: Desc,
    slot_status_plugin_desc: Desc,
    event_dropped_desc: Desc,
    traffic_sent_desc: Desc,
    data_plane_counts: &'static DataPlaneCounts,
    agave_slot_status_gauges: &'static SlotStatusGauges,
    plugin_slot_status_gauges: &'static SlotStatusGauges,
}

impl DataPathGaugesCollector {
    fn new(
        data_plane_counts: &'static DataPlaneCounts,
        agave_slot_status_gauges: &'static SlotStatusGauges,
        plugin_slot_status_gauges: &'static SlotStatusGauges,
    ) -> Self {
        let family_desc = |fq_name: &str, help: &str, label_names: &[&str]| {
            let variable_labels = label_names
                .iter()
                .map(|label_name| (*label_name).to_owned());
            Desc::new(
                fq_name.to_owned(),
                help.to_owned(),
                variable_labels.collect(),
                HashMap::new(),
            )
            .expect("a valid family name and labels")
        };
        Self {
            message_queue_desc: family_desc(
                "yellowstone_grpc_geyser_message_queue_size",
                "Size of geyser message queue",
                &[],
            ),
            block_queue_desc: family_desc(
                "yellowstone_grpc_geyser_block_reconstruction_queue_size",
                "Size of block reconstruction message queue",
                &[],
            ),
            slot_status_desc: family_desc(
                "yellowstone_grpc_geyser_slot_status",
                "Lastest received slot from Geyser",
                &["status"],
            ),
            slot_status_plugin_desc: family_desc(
                "yellowstone_grpc_geyser_slot_status_plugin",
                "Latest processed slot in the plugin to client queues",
                &["status"],
            ),
            event_dropped_desc: family_desc(
                "yellowstone_grpc_geyser_event_dropped_total",
                "Total number of events dropped in the plugin due to drop_list",
                &["event"],
            ),
            traffic_sent_desc: family_desc(
                "yellowstone_grpc_geyser_total_traffic_sent_bytes",
                "Total traffic sent to subscriber by type (account_update, block_meta, etc)",
                &[],
            ),
            data_plane_counts,
            agave_slot_status_gauges,
            plugin_slot_status_gauges,
        }
    }

    /// A slot status family's series: each status set since the last reset, at its last slot.
    fn slot_status_family(
        family_desc: &Desc,
        slot_status_gauges: &SlotStatusGauges,
        status_labels: [&str; SLOT_STATUS_KINDS],
    ) -> MetricFamily {
        let status_metrics = (0..SLOT_STATUS_KINDS).filter_map(|status_index| {
            let last_slot = slot_status_gauges.last_slot(status_index)?;
            let status_label = label_pair("status", status_labels[status_index]);
            Some(gauge_metric(vec![status_label], last_slot as i64 as f64))
        });
        rendered_family(family_desc, MetricType::GAUGE, status_metrics.collect())
    }
}

fn gauge_metric(label_pairs: Vec<LabelPair>, gauge_value: f64) -> Metric {
    let mut series_gauge = Gauge::default();
    series_gauge.set_value(gauge_value);
    let mut series_metric = Metric::default();
    series_metric.set_label(label_pairs);
    series_metric.set_gauge(series_gauge);
    series_metric
}

fn counter_metric(label_pairs: Vec<LabelPair>, counter_value: f64) -> Metric {
    let mut series_counter = Counter::default();
    series_counter.set_value(counter_value);
    let mut series_metric = Metric::default();
    series_metric.set_label(label_pairs);
    series_metric.set_counter(series_counter);
    series_metric
}

fn rendered_family(
    family_desc: &Desc,
    family_type: MetricType,
    series_metrics: Vec<Metric>,
) -> MetricFamily {
    let mut metric_family = empty_family(family_desc, family_type);
    metric_family.set_metric(series_metrics);
    metric_family
}

impl Collector for DataPathGaugesCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![
            &self.message_queue_desc,
            &self.block_queue_desc,
            &self.slot_status_desc,
            &self.slot_status_plugin_desc,
            &self.event_dropped_desc,
            &self.traffic_sent_desc,
        ]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let owner_queue_family = |family_desc: &Desc, owner_queue: OwnerQueue| {
            let queue_size = self.data_plane_counts.owner_queue_size(owner_queue);
            let queue_metric = gauge_metric(Vec::new(), queue_size as f64);
            rendered_family(family_desc, MetricType::GAUGE, vec![queue_metric])
        };
        let dropped_accounts = (self.data_plane_counts).sum(DataPlaneCount::AccountEventDropped);
        let dropped_metrics = (dropped_accounts > 0)
            .then(|| {
                let event_label = label_pair("event", "account");
                counter_metric(vec![event_label], dropped_accounts as f64)
            })
            .into_iter()
            .collect();
        let traffic_sent_bytes = (self.data_plane_counts).sum(DataPlaneCount::TrafficSentBytes);
        let traffic_metric = counter_metric(Vec::new(), traffic_sent_bytes as f64);
        vec![
            owner_queue_family(&self.message_queue_desc, OwnerQueue::GeyserLoop),
            owner_queue_family(&self.block_queue_desc, OwnerQueue::BlockReconstruction),
            Self::slot_status_family(
                &self.slot_status_desc,
                self.agave_slot_status_gauges,
                agave_slot_status_labels(),
            ),
            Self::slot_status_family(
                &self.slot_status_plugin_desc,
                self.plugin_slot_status_gauges,
                plugin_slot_status_labels(),
            ),
            rendered_family(
                &self.event_dropped_desc,
                MetricType::COUNTER,
                dropped_metrics,
            ),
            rendered_family(
                &self.traffic_sent_desc,
                MetricType::COUNTER,
                vec![traffic_metric],
            ),
        ]
    }
}

pub fn incr_grpc_method_call_count<S: AsRef<str>>(method: S) {
    GRPC_METHOD_CALL_COUNT
        .with_label_values(&[method.as_ref()])
        .inc();
}

/// The `service_outbound_bytes` series of one subscriber id and path, which a metered body keeps
/// from its start, so a report adds to it without a label lookup.
pub fn grpc_service_outbound_bytes_gauge(subscriber_id: &str, uri_path: &str) -> IntGauge {
    GRPC_SERVICE_OUTBOUND_BYTES.with_label_values(&[subscriber_id, uri_path])
}

pub fn reset_grpc_service_outbound_bytes<S: AsRef<str>, P: AsRef<str>>(
    subscriber_id: S,
    uri_path: P,
) {
    GRPC_SERVICE_OUTBOUND_BYTES
        .with_label_values(&[subscriber_id.as_ref(), uri_path.as_ref()])
        .set(0);
}

pub fn add_total_traffic_sent(bytes: u64) {
    DATA_PLANE_COUNTS.add(DataPlaneCount::TrafficSentBytes, bytes);
}

pub fn set_grpc_concurrent_subscribe_per_subscriber_id<S: AsRef<str>>(subscriber_id: S, size: u64) {
    GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID
        .with_label_values(&[subscriber_id.as_ref()])
        .set(size as i64);
}

pub fn remove_grpc_concurrent_subscribe_per_subscriber_id<S: AsRef<str>>(subscriber_id: S) {
    // Prometheus keys children by label hash, so a colliding id or a reset can remove it first.
    GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID
        .remove_label_values(&[subscriber_id.as_ref()])
        .ok();
}

/// Reset all metrics on plugin unload to prevent metric accumulation across plugin lifecycle
pub fn reset_metrics() {
    // Reset gauge vectors (clears all label combinations)
    SUBSCRIPTIONS_TOTAL.reset();
    AGAVE_SLOT_STATUS_GAUGES.reset();
    PLUGIN_SLOT_STATUS_GAUGES.reset();
    INVALID_FULL_BLOCKS.reset();
    GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD.reset();

    // Reset counter vectors (clears all label combinations)
    MISSED_STATUS_MESSAGE.reset();
    SUBSCRIBER_COUNTS.reset();
    GRPC_CLIENT_DISCONNECTS.reset();
    GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID.reset();
    GRPC_SERVICE_OUTBOUND_BYTES.reset();
    GRPC_SUBSCRIPTION_LIMIT_EXCEEDED.reset();
    GRPC_METHOD_CALL_COUNT.reset();

    // Every data-path count: pre-encoding, the owner queue gauges, dropped events, traffic
    DATA_PLANE_COUNTS.reset();

    GEYSER_BLOCK_MISMATCH_TRANSACTION.reset();
    GEYSER_UNTRACK_SLOT_EVENT_DROPPED.reset();

    IP_CONNCUR_RATE_LIMIT_EXCEEDED.reset();
    UNAUTHORIZED_COUNT.reset();
    AUTHORIZED_COUNT.reset();
    METHOD_RATELIMITED_COUNT.reset();
    FILTER_STATS.reset();
    // Note: VERSION and GEYSER_ACCOUNT_UPDATE_RECEIVED are intentionally not reset
    // - VERSION contains build info set once on startup
    // - GEYSER_ACCOUNT_UPDATE_RECEIVED is a Histogram which doesn't support reset()
}
