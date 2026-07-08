use {
    crate::{
        config::ConfigPrometheus,
        plugin::{
            filter::{Filter, FilterStats},
            message::SlotStatus,
        },
        version::VERSION as VERSION_INFO,
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
    log::{debug, error, info},
    prometheus::{
        Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
        Opts, Registry, TextEncoder,
    },
    std::{convert::Infallible, sync::Once},
    tokio::net::TcpListener,
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("yellowstone_grpc_version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    static ref SLOT_STATUS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("yellowstone_grpc_slot_status", "Lastest received slot from Geyser"),
        &["status"]
    ).unwrap();

    static ref SLOT_STATUS_PLUGIN: IntGaugeVec = IntGaugeVec::new(
        Opts::new("yellowstone_grpc_slot_status_plugin", "Latest processed slot in the plugin to client queues"),
        &["status"]
    ).unwrap();

    static ref INVALID_FULL_BLOCKS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("yellowstone_grpc_invalid_full_blocks_total", "Total number of fails on constructin full blocks"),
        &["reason"]
    ).unwrap();

    static ref MESSAGE_QUEUE_SIZE: IntGauge = IntGauge::new(
        "yellowstone_grpc_message_queue_size", "Size of geyser message queue"
    ).unwrap();

    static ref DESHRED_QUEUE_SIZE: IntGauge = IntGauge::new(
        "yellowstone_grpc_deshred_queue_size", "Size of deshred message queue"
    ).unwrap();

    static ref SUBSCRIPTIONS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("yellowstone_grpc_subscriptions_total", "Total number of subscriptions to gRPC service"),
        &["endpoint", "subscription"]
    ).unwrap();

    static ref MISSED_STATUS_MESSAGE: IntCounterVec = IntCounterVec::new(
        Opts::new("yellowstone_grpc_missed_status_message_total", "Number of missed messages by commitment"),
        &["status"]
    ).unwrap();

    static ref GRPC_MESSAGE_SENT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_message_sent_count",
            "Number of message sent over grpc to downstream client",
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "yellowstone_grpc_subscriber_send_bandwidth_load",
            "Current Send load we send to subscriber channel (in bytes per second)"
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref GRPC_SUBSCRIBER_QUEUE_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_subscriber_queue_size",
            "Current size of subscriber channel queue"
        )
        .buckets(vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 20000.0, 50000.0, 100_000.0, 175_000.0, 250_000.0, 500_000.0, 1_000_000.0]),
        &["subscriber_id", "kind"]
    ).unwrap();

    static ref GRPC_CLIENT_DISCONNECTS: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_client_disconnects_total",
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

    pub static ref GEYSER_BATCH_SIZE: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "yellowstone_grpc_geyser_batch_size",
            "Size of processed message batches"
        )
        .buckets(vec![1.0, 4.0, 8.0, 16.0, 24.0, 31.0])
    ).unwrap();

    static ref PRE_ENCODED_CACHE_HIT: IntCounterVec = IntCounterVec::new(
        Opts::new("yellowstone_grpc_pre_encoded_cache_hit", "Pre-encoded cache hits by message type"),
        &["type"]
    ).unwrap();

    static ref PRE_ENCODED_CACHE_MISS: IntCounterVec = IntCounterVec::new(
        Opts::new("yellowstone_grpc_pre_encoded_cache_miss", "Pre-encoded cache misses by message type"),
        &["type"]
    ).unwrap();

    static ref GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "yellowstone_grpc_concurrent_subscribe_per_subscriber_id",
            "Current concurrent subscriptions per subscriber ID"
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref TOTAL_TRAFFIC_SENT: IntCounter = IntCounter::new(
        "yellowstone_grpc_total_traffic_sent_bytes",
        "Total traffic sent to subscriber by type (account_update, block_meta, etc)"
    ).unwrap();

    static ref GRPC_METHOD_CALL_COUNT: IntCounterVec = IntCounterVec::new(
        Opts::new("yellowstone_grpc_method_call_count", "Total number of calls to GetVersion gRPC method"),
        &["method"]
    ).unwrap();

    static ref GRPC_SUBSCRIPTION_LIMIT_EXCEEDED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_subscription_limit_exceeded_total",
            "Number of subscribe attempts that exceeded the per-subscriber limit"
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref GRPC_SERVICE_OUTBOUND_BYTES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("yellowstone_grpc_service_outbound_bytes", "Current emitted bytes by tonic service response bodies per active subscriber stream"),
        &["subscriber_id", "uri_path"]
    ).unwrap();

    static ref GEYSER_EVENT_DROPPED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_geyser_event_dropped_total",
            "Total number of events dropped in the plugin due to drop_list"
        ),
        &["event"]
    ).unwrap();

    static ref GEYSER_BLOCK_MISMATCH_TRANSACTION: IntCounter = IntCounter::new(
        "yellowstone_grpc_geyser_block_mismatch_transaction", "Number of block mismatch transactions encountered in the plugin"
    ).unwrap();

    static ref GEYSER_UNTRACK_SLOT_EVENT_DROPPED: IntCounter = IntCounter::new(
        "yellowstone_grpc_geyser_untrack_slot_event_dropped_total", "Number of geyser event drop due to untrack slot"
    ).unwrap();


    static ref IP_CONNCUR_RATE_LIMIT_EXCEEDED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_ip_concurrent_rate_limit_exceeded_total",
            "Number of incoming connections that exceeded the per-IP connection limit at the transport layer"
        ),
        &["remote_peer_ip"]
    ).unwrap();

    static ref UNAUTHORIZED_COUNT: IntCounter = IntCounter::new(
        "yellowstone_grpc_unauthorized_count_total",
        "Number of unauthorized requests to the gRPC service"
    ).unwrap();

    static ref AUTHORIZED_COUNT: IntCounter = IntCounter::new(
        "yellowstone_grpc_authorized_count_total",
        "Number of authorized requests to the gRPC service"
    ).unwrap();

    static ref METHOD_RATELIMITED_COUNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_method_ratelimited_count_total",
            "Number of requests that were rate limited by the method ratelimiter"
        ),
        &["subscriber_id", "method"]
    ).unwrap();

    static ref FILTER_STATS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_stats",
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

pub struct PrometheusService;

impl PrometheusService {
    pub async fn spawn(
        config: Option<ConfigPrometheus>,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> std::io::Result<()> {
        static REGISTER: Once = Once::new();
        REGISTER.call_once(|| {
            macro_rules! register {
                ($collector:ident) => {
                    REGISTRY
                        .register(Box::new($collector.clone()))
                        .expect("collector can't be registered");
                };
            }
            register!(VERSION);
            register!(SLOT_STATUS);
            register!(SLOT_STATUS_PLUGIN);
            register!(INVALID_FULL_BLOCKS);
            register!(MESSAGE_QUEUE_SIZE);
            register!(SUBSCRIPTIONS_TOTAL);
            register!(MISSED_STATUS_MESSAGE);
            register!(GRPC_MESSAGE_SENT);
            register!(GEYSER_ACCOUNT_UPDATE_RECEIVED);
            register!(GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD);
            register!(GRPC_SUBSCRIBER_QUEUE_SIZE);
            register!(GEYSER_BATCH_SIZE);
            register!(GRPC_CLIENT_DISCONNECTS);
            register!(PRE_ENCODED_CACHE_HIT);
            register!(PRE_ENCODED_CACHE_MISS);
            register!(GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID);
            register!(TOTAL_TRAFFIC_SENT);
            register!(GRPC_METHOD_CALL_COUNT);
            register!(GRPC_SUBSCRIPTION_LIMIT_EXCEEDED);
            register!(GRPC_SERVICE_OUTBOUND_BYTES);
            register!(GEYSER_EVENT_DROPPED);
            register!(GEYSER_BLOCK_MISMATCH_TRANSACTION);
            register!(GEYSER_UNTRACK_SLOT_EVENT_DROPPED);
            register!(IP_CONNCUR_RATE_LIMIT_EXCEEDED);
            register!(UNAUTHORIZED_COUNT);
            register!(AUTHORIZED_COUNT);
            register!(METHOD_RATELIMITED_COUNT);

            register!(FILTER_STATS);

            VERSION
                .with_label_values(&[
                    VERSION_INFO.buildts,
                    VERSION_INFO.git,
                    VERSION_INFO.package,
                    VERSION_INFO.proto,
                    VERSION_INFO.rustc,
                    VERSION_INFO.solana,
                    VERSION_INFO.version,
                ])
                .inc();
        });

        if let Some(ConfigPrometheus { address }) = config {
            let listener = TcpListener::bind(&address).await?;
            info!("start prometheus server: {address}");
            let task_tracker_clone = task_tracker.clone();
            task_tracker.spawn(async move {
                debug!("Prometheus server listening on {}", address);
                loop {
                    let stream = tokio::select! {
                        () = cancellation_token.cancelled() => {
                            info!("Prometheus server received cancellation");
                            break
                        },
                        maybe_conn = listener.accept() => {
                            match maybe_conn {
                                Ok((stream, _addr)) => stream,
                                Err(error) => {
                                    error!("failed to accept new connection: {error}");
                                    break;
                                }
                            }
                        }
                    };
                    task_tracker_clone.spawn(async move {
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
                    });
                }
                info!("Prometheus server exiting");
            });
        }
        Ok(())
    }
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

pub fn incr_geyser_event_dropped<S: AsRef<str>>(event: S) {
    GEYSER_EVENT_DROPPED
        .with_label_values(&[event.as_ref()])
        .inc();
}

pub fn incr_geyser_block_mismatch_transaction() {
    GEYSER_BLOCK_MISMATCH_TRANSACTION.inc();
}

pub fn incr_geyser_untrack_slot_event_dropped() {
    GEYSER_UNTRACK_SLOT_EVENT_DROPPED.inc();
}

pub fn incr_grpc_message_sent_counter<S: AsRef<str>>(remote_id: S) {
    GRPC_MESSAGE_SENT
        .with_label_values(&[remote_id.as_ref()])
        .inc();
}

pub fn update_slot_status(status: &GeyserSlosStatus, slot: u64) {
    SLOT_STATUS
        .with_label_values(&[status.as_str()])
        .set(slot as i64);
}

pub fn update_slot_plugin_status(status: SlotStatus, slot: u64) {
    SLOT_STATUS_PLUGIN
        .with_label_values(&[status.as_str()])
        .set(slot as i64);
}

pub fn update_invalid_blocks(reason: impl AsRef<str>) {
    INVALID_FULL_BLOCKS
        .with_label_values(&[reason.as_ref()])
        .inc();
    INVALID_FULL_BLOCKS.with_label_values(&["all"]).inc();
}

pub fn message_queue_size_inc() {
    MESSAGE_QUEUE_SIZE.inc()
}

pub fn deshred_queue_size_inc(amount: i64) {
    DESHRED_QUEUE_SIZE.add(amount);
}

pub fn message_queue_size_dec() {
    MESSAGE_QUEUE_SIZE.dec()
}

pub fn deshred_queue_size_dec() {
    DESHRED_QUEUE_SIZE.dec()
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

pub fn observe_subscriber_queue_size<S: AsRef<str>>(
    subscriber_id: S,
    size: u64,
    kind: &'static str,
) {
    GRPC_SUBSCRIBER_QUEUE_SIZE
        .with_label_values(&[subscriber_id.as_ref(), kind])
        .observe(size as f64);
}

pub fn incr_client_disconnect<S: AsRef<str>>(subscriber_id: S, reason: &str) {
    GRPC_CLIENT_DISCONNECTS
        .with_label_values(&[subscriber_id.as_ref(), reason])
        .inc();
}

pub fn pre_encoded_cache_hit(msg_type: &str) {
    PRE_ENCODED_CACHE_HIT.with_label_values(&[msg_type]).inc();
}

pub fn pre_encoded_cache_miss(msg_type: &str) {
    PRE_ENCODED_CACHE_MISS.with_label_values(&[msg_type]).inc();
}

pub fn incr_grpc_method_call_count<S: AsRef<str>>(method: S) {
    GRPC_METHOD_CALL_COUNT
        .with_label_values(&[method.as_ref()])
        .inc();
}

pub fn add_grpc_service_outbound_bytes<S: AsRef<str>, P: AsRef<str>>(
    subscriber_id: S,
    uri_path: P,
    bytes: u64,
) {
    GRPC_SERVICE_OUTBOUND_BYTES
        .with_label_values(&[subscriber_id.as_ref(), uri_path.as_ref()])
        .add(bytes as i64);
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
    TOTAL_TRAFFIC_SENT.inc_by(bytes);
}

pub fn set_grpc_concurrent_subscribe_per_subscriber_id<S: AsRef<str>>(subscriber_id: S, size: u64) {
    GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID
        .with_label_values(&[subscriber_id.as_ref()])
        .set(size as i64);
}

pub fn remove_grpc_concurrent_subscribe_per_subscriber_id<S: AsRef<str>>(subscriber_id: S) {
    GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID
        .remove_label_values(&[subscriber_id.as_ref()])
        .expect("remove_label_values");
}

/// Reset all metrics on plugin unload to prevent metric accumulation across plugin lifecycle
pub fn reset_metrics() {
    // Reset gauge metrics to 0
    MESSAGE_QUEUE_SIZE.set(0);

    // Reset gauge vectors (clears all label combinations)
    SUBSCRIPTIONS_TOTAL.reset();
    SLOT_STATUS.reset();
    SLOT_STATUS_PLUGIN.reset();
    INVALID_FULL_BLOCKS.reset();
    GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD.reset();
    GRPC_SUBSCRIBER_QUEUE_SIZE.reset();

    // Reset counter vectors (clears all label combinations)
    MISSED_STATUS_MESSAGE.reset();
    GRPC_MESSAGE_SENT.reset();
    GRPC_CLIENT_DISCONNECTS.reset();
    GRPC_CONCURRENT_SUBSCRIBE_PER_SUBSCRIBER_ID.reset();
    TOTAL_TRAFFIC_SENT.reset();
    GRPC_SERVICE_OUTBOUND_BYTES.reset();
    GRPC_SUBSCRIPTION_LIMIT_EXCEEDED.reset();
    GRPC_METHOD_CALL_COUNT.reset();

    // Pre-encoding
    PRE_ENCODED_CACHE_HIT.reset();
    PRE_ENCODED_CACHE_MISS.reset();

    GEYSER_EVENT_DROPPED.reset();
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
