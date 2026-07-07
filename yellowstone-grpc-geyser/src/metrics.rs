use {
    crate::{
        config::ConfigPrometheus,
        plugin::{
            filter::{Filter, FilterComplexityProfile},
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
    solana_clock::Slot,
    std::{
        collections::{hash_map::Entry as HashMapEntry, HashMap},
        convert::Infallible,
        sync::{Arc, Once},
    },
    tokio::{
        net::TcpListener,
        sync::{mpsc, oneshot},
        task::JoinHandle,
    },
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    static ref SLOT_STATUS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("slot_status", "Lastest received slot from Geyser"),
        &["status"]
    ).unwrap();

    static ref SLOT_STATUS_PLUGIN: IntGaugeVec = IntGaugeVec::new(
        Opts::new("slot_status_plugin", "Latest processed slot in the plugin to client queues"),
        &["status"]
    ).unwrap();

    static ref INVALID_FULL_BLOCKS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("invalid_full_blocks_total", "Total number of fails on constructin full blocks"),
        &["reason"]
    ).unwrap();

    static ref MESSAGE_QUEUE_SIZE: IntGauge = IntGauge::new(
        "message_queue_size", "Size of geyser message queue"
    ).unwrap();

    static ref DESHRED_QUEUE_SIZE: IntGauge = IntGauge::new(
        "deshred_queue_size", "Size of deshred message queue"
    ).unwrap();

    static ref CONNECTIONS_TOTAL: IntGauge = IntGauge::new(
        "connections_total", "Total number of connections to gRPC service"
    ).unwrap();

    static ref SUBSCRIPTIONS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("subscriptions_total", "Total number of subscriptions to gRPC service"),
        &["endpoint", "subscription"]
    ).unwrap();

    static ref MISSED_STATUS_MESSAGE: IntCounterVec = IntCounterVec::new(
        Opts::new("missed_status_message_total", "Number of missed messages by commitment"),
        &["status"]
    ).unwrap();

    static ref GRPC_MESSAGE_SENT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "grpc_message_sent_count",
            "Number of message sent over grpc to downstream client",
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "grpc_subscriber_send_bandwidth_load",
            "Current Send load we send to subscriber channel (in bytes per second)"
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref GRPC_SUBSCRIBER_QUEUE_SIZE: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "grpc_subscriber_queue_size",
            "Current size of subscriber channel queue"
        ),
        &["subscriber_id"]
    ).unwrap();

    static ref GRPC_CLIENT_DISCONNECTS: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "grpc_client_disconnects_total",
            "Total client disconnections by reason"
        ),
        &["subscriber_id", "reason"]
    ).unwrap();

    static ref GEYSER_ACCOUNT_UPDATE_RECEIVED: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "geyser_account_update_data_size_kib",
            "Histogram of all account update data (kib) received from Geyser plugin"
        )
        .buckets(vec![5.0, 10.0, 20.0, 30.0, 50.0, 100.0, 200.0, 300.0, 500.0, 1000.0, 2000.0, 3000.0, 5000.0, 10000.0])
    ).unwrap();

    pub static ref GEYSER_BATCH_SIZE: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "yellowstone_geyser_batch_size",
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

    static ref GRPC_CONCURRENT_SUBSCRIBE_PER_TCP_CONNECTION: IntGaugeVec = IntGaugeVec::new(
            Opts::new(
                "grpc_concurrent_subscribe_per_tcp_connection",
                "Current concurrent subscriptions per remote TCP peer socket address"
            ),
            &["remote_peer_sk_addr"]
    ).unwrap();

    static ref TOTAL_TRAFFIC_SENT: IntCounter = IntCounter::new(
        "total_traffic_sent_bytes",
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
            "geyser_event_dropped_total",
            "Total number of events dropped in the plugin due to drop_list"
        ),
        &["event"]
    ).unwrap();

    static ref GEYSER_BLOCK_MISMATCH_TRANSACTION: IntCounter = IntCounter::new(
        "geyser_block_mismatch_transaction", "Number of block mismatch transactions encountered in the plugin"
    ).unwrap();

    static ref GEYSER_UNTRACK_SLOT_EVENT_DROPPED: IntCounter = IntCounter::new(
        "geyser_untrack_slot_event_dropped_total", "Number of geyser event drop due to untrack slot"
    ).unwrap();


    static ref IP_CONNCUR_RATE_LIMIT_EXCEEDED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "yellowstone_grpc_ip_conncur_rate_limit_exceeded_total",
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

    static ref FILTER_COMPLEXITY_ACCOUNTS_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_accounts_size",
            "Distribution of account_include size per account filter"
        ).buckets(vec![0.0, 1.0, 10.0, 100.0, 1000.0, 5000.0, 10000.0, 20000.0, 50000.0, 100000.0, 2500000.0, 5000000.0, 1000000.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_OWNERS_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_owners_size",
            "Distribution of owner_include size per account filter"
        ).buckets(vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_TX_ACCOUNTS_INCLUDE_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_tx_accounts_include_size",
            "Distribution of account_include size per transaction filter"
        ).buckets(vec![0.0, 1.0, 10.0, 100.0, 1000.0, 5000.0, 10000.0, 20000.0, 50000.0, 100000.0, 2500000.0, 5000000.0, 1000000.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_TX_ACCOUNTS_EXCLUDE_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_tx_accounts_exclude_size",
            "Distribution of account_exclude size per transaction filter"
        ).buckets(vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_TX_ACCOUNTS_REQUIRED_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_tx_accounts_required_size",
            "Distribution of account_required size per transaction filter"
        ).buckets(vec![0.0, 1.0, 10.0, 100.0, 1000.0, 5000.0, 10000.0, 20000.0, 50000.0, 100000.0, 2500000.0, 5000000.0, 1000000.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_TX_TOKEN_MODE_ENABLED: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_tx_token_mode_enabled",
            "Whether token-account expansion is enabled (0/1) per transaction filter"
        ).buckets(vec![0.0, 1.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_INCLUDE_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_tx_status_accounts_include_size",
            "Distribution of account_include size per transaction-status filter"
        ).buckets(vec![0.0, 1.0, 10.0, 100.0, 1000.0, 5000.0, 10000.0, 20000.0, 50000.0, 100000.0, 2500000.0, 5000000.0, 1000000.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_EXCLUDE_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_tx_status_accounts_exclude_size",
            "Distribution of account_exclude size per transaction-status filter"
        ).buckets(vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_REQUIRED_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_tx_status_accounts_required_size",
            "Distribution of account_required size per transaction-status filter"
        ).buckets(vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_TX_STATUS_TOKEN_MODE_ENABLED: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_tx_status_token_mode_enabled",
            "Whether token-account expansion is enabled (0/1) per transaction-status filter"
        ).buckets(vec![0.0, 1.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_BLOCKS_INCLUDE_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_blocks_include_size",
            "Distribution of include complexity per block filter"
        ).buckets(vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0]),
        &["subscriber_id"]
    ).unwrap();

    static ref FILTER_COMPLEXITY_BLOCKS_EXCLUDE_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "yellowstone_grpc_filter_complexity_blocks_exclude_size",
            "Distribution of exclude complexity per block filter"
        ).buckets(vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0]),
        &["subscriber_id"]
    ).unwrap();
}

#[derive(Debug)]
pub enum DebugClientMessage {
    UpdateFilter { id: usize, filter: Box<Filter> },
    UpdateSlot { id: usize, slot: Slot },
    Removed { id: usize },
}

impl DebugClientMessage {
    pub fn maybe_send(tx: &Option<mpsc::UnboundedSender<Self>>, get_msg: impl FnOnce() -> Self) {
        if let Some(tx) = tx {
            let _ = tx.send(get_msg());
        }
    }
}

#[derive(Debug)]
struct DebugClientStatus {
    filter: Box<Filter>,
    processed_slot: Slot,
}

#[derive(Debug)]
struct DebugClientStatuses {
    requests_tx: mpsc::UnboundedSender<oneshot::Sender<String>>,
    jh: JoinHandle<()>,
}

impl Drop for DebugClientStatuses {
    fn drop(&mut self) {
        self.jh.abort();
    }
}

impl DebugClientStatuses {
    fn new(
        clients_rx: mpsc::UnboundedReceiver<DebugClientMessage>,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Arc<Self> {
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        let jh = task_tracker.spawn(Self::run(clients_rx, requests_rx, cancellation_token));
        Arc::new(Self { requests_tx, jh })
    }

    async fn run(
        mut clients_rx: mpsc::UnboundedReceiver<DebugClientMessage>,
        mut requests_rx: mpsc::UnboundedReceiver<oneshot::Sender<String>>,
        cancellation_token: CancellationToken,
    ) {
        let mut clients = HashMap::<usize, DebugClientStatus>::new();
        loop {
            tokio::select! {
                () = cancellation_token.cancelled() => {
                    info!("DebugClientStatuses received cancellation");
                    break
                },
                maybe = clients_rx.recv() => {
                    let Some(message) = maybe else {
                        break;
                    };
                    match message {
                        DebugClientMessage::UpdateFilter { id, filter } => {
                            match clients.entry(id) {
                                HashMapEntry::Occupied(mut entry) => {
                                    entry.get_mut().filter = filter;
                                }
                                HashMapEntry::Vacant(entry) => {
                                    entry.insert(DebugClientStatus {
                                        filter,
                                        processed_slot: 0,
                                    });
                                }
                            }
                        }
                        DebugClientMessage::UpdateSlot { id, slot } => {
                            if let Some(status) = clients.get_mut(&id) {
                                status.processed_slot = slot;
                            }
                        }
                        DebugClientMessage::Removed { id } => {
                            clients.remove(&id);
                        }
                    }
                },
                Some(tx) = requests_rx.recv() => {
                    let mut statuses: Vec<(usize, String)> = clients.iter().map(|(id, status)| {
                        (*id, format!("client#{id:06}, {}, {:?}", status.processed_slot, status.filter))
                    }).collect();
                    statuses.sort();

                    let mut status = statuses.into_iter().fold(String::new(), |mut acc: String, (_id, status)| {
                        if !acc.is_empty() {
                            acc += "\n";
                        }
                        acc + &status
                    });
                    if !status.is_empty() {
                        status += "\n";
                    }

                    let _ = tx.send(status);
                },
            }
        }
        info!("DebugClientStatuses exiting");
    }

    async fn get_statuses(&self) -> anyhow::Result<String> {
        let (tx, rx) = oneshot::channel();
        self.requests_tx
            .send(tx)
            .map_err(|_error| anyhow::anyhow!("failed to send request"))?;
        rx.await
            .map_err(|_error| anyhow::anyhow!("failed to wait response"))
    }
}

pub struct PrometheusService;

impl PrometheusService {
    pub async fn spawn(
        config: Option<ConfigPrometheus>,
        debug_clients_rx: Option<mpsc::UnboundedReceiver<DebugClientMessage>>,
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
            register!(CONNECTIONS_TOTAL);
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
            register!(GRPC_CONCURRENT_SUBSCRIBE_PER_TCP_CONNECTION);
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

            register!(FILTER_COMPLEXITY_ACCOUNTS_SIZE);
            register!(FILTER_COMPLEXITY_OWNERS_SIZE);
            register!(FILTER_COMPLEXITY_TX_ACCOUNTS_INCLUDE_SIZE);
            register!(FILTER_COMPLEXITY_TX_ACCOUNTS_EXCLUDE_SIZE);
            register!(FILTER_COMPLEXITY_TX_ACCOUNTS_REQUIRED_SIZE);
            register!(FILTER_COMPLEXITY_TX_TOKEN_MODE_ENABLED);
            register!(FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_INCLUDE_SIZE);
            register!(FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_EXCLUDE_SIZE);
            register!(FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_REQUIRED_SIZE);
            register!(FILTER_COMPLEXITY_TX_STATUS_TOKEN_MODE_ENABLED);
            register!(FILTER_COMPLEXITY_BLOCKS_INCLUDE_SIZE);
            register!(FILTER_COMPLEXITY_BLOCKS_EXCLUDE_SIZE);

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

        let mut debug_clients_statuses = None;
        if let Some(ConfigPrometheus { address }) = config {
            if let Some(debug_clients_rx) = debug_clients_rx {
                info!("starting DebugClientStatuses");
                debug_clients_statuses = Some(DebugClientStatuses::new(
                    debug_clients_rx,
                    cancellation_token.child_token(),
                    task_tracker.clone(),
                ));
            }
            let debug_clients_statuses2 = debug_clients_statuses.clone();

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
                    let debug_clients_statuses = debug_clients_statuses2.clone();
                    task_tracker_clone.spawn(async move {
                        let peer_addr = stream.peer_addr().ok();
                        debug!(
                            "Prometheus server accepted new connection from {:?}",
                            peer_addr
                        );
                        if let Err(error) = ServerBuilder::new(TokioExecutor::new())
                            .serve_connection(
                                TokioIo::new(stream),
                                service_fn(move |req: Request<BodyIncoming>| {
                                    let debug_clients_statuses = debug_clients_statuses.clone();
                                    async move {
                                        match req.uri().path() {
                                            "/metrics" => metrics_handler(),
                                            "/debug_clients" => {
                                                if let Some(debug_clients_statuses) =
                                                    &debug_clients_statuses
                                                {
                                                    let (status, body) =
                                                        match debug_clients_statuses
                                                            .get_statuses()
                                                            .await
                                                        {
                                                            Ok(body) => (StatusCode::OK, body),
                                                            Err(error) => (
                                                                StatusCode::INTERNAL_SERVER_ERROR,
                                                                error.to_string(),
                                                            ),
                                                        };
                                                    Response::builder().status(status).body(
                                                        BodyFull::new(Bytes::from(body)).boxed(),
                                                    )
                                                } else {
                                                    not_found_handler()
                                                }
                                            }
                                            _ => not_found_handler(),
                                        }
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

pub fn observe_filter_complexity(subscriber_id: &str, observation: &FilterComplexityProfile) {
    for score in &observation.accounts {
        FILTER_COMPLEXITY_ACCOUNTS_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.accounts_size as f64);
        FILTER_COMPLEXITY_OWNERS_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.owners_size as f64);
    }

    for score in &observation.transactions {
        FILTER_COMPLEXITY_TX_ACCOUNTS_INCLUDE_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.accounts_include_size as f64);
        FILTER_COMPLEXITY_TX_ACCOUNTS_EXCLUDE_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.accounts_exclude_size as f64);
        FILTER_COMPLEXITY_TX_ACCOUNTS_REQUIRED_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.accounts_required_size as f64);
        FILTER_COMPLEXITY_TX_TOKEN_MODE_ENABLED
            .with_label_values(&[subscriber_id])
            .observe(if score.token_accounts_enabled {
                1.0
            } else {
                0.0
            });
    }

    for score in &observation.transaction_status {
        FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_INCLUDE_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.accounts_include_size as f64);
        FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_EXCLUDE_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.accounts_exclude_size as f64);
        FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_REQUIRED_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.accounts_required_size as f64);
        FILTER_COMPLEXITY_TX_STATUS_TOKEN_MODE_ENABLED
            .with_label_values(&[subscriber_id])
            .observe(if score.token_accounts_enabled {
                1.0
            } else {
                0.0
            });
    }

    for score in &observation.blocks {
        FILTER_COMPLEXITY_BLOCKS_INCLUDE_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.include_size as f64);
        FILTER_COMPLEXITY_BLOCKS_EXCLUDE_SIZE
            .with_label_values(&[subscriber_id])
            .observe(score.exclude_size as f64);
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

pub fn connections_total_inc() {
    CONNECTIONS_TOTAL.inc()
}

pub fn connections_total_dec() {
    CONNECTIONS_TOTAL.dec()
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

pub fn set_subscriber_queue_size<S: AsRef<str>>(subscriber_id: S, size: u64) {
    GRPC_SUBSCRIBER_QUEUE_SIZE
        .with_label_values(&[subscriber_id.as_ref()])
        .set(size as i64);
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

pub fn set_grpc_concurrent_subscribe_per_tcp_connection<S: AsRef<str>>(
    remote_peer_sk_addr: S,
    size: u64,
) {
    GRPC_CONCURRENT_SUBSCRIBE_PER_TCP_CONNECTION
        .with_label_values(&[remote_peer_sk_addr.as_ref()])
        .set(size as i64);
}

pub fn remove_grpc_concurrent_subscribe_per_tcp_connection<S: AsRef<str>>(remote_peer_sk_addr: S) {
    GRPC_CONCURRENT_SUBSCRIBE_PER_TCP_CONNECTION
        .remove_label_values(&[remote_peer_sk_addr.as_ref()])
        .expect("remove_label_values");
}

/// Reset all metrics on plugin unload to prevent metric accumulation across plugin lifecycle
pub fn reset_metrics() {
    // Reset gauge metrics to 0
    CONNECTIONS_TOTAL.set(0);
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
    GRPC_CONCURRENT_SUBSCRIBE_PER_TCP_CONNECTION.reset();
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
    FILTER_COMPLEXITY_ACCOUNTS_SIZE.reset();
    FILTER_COMPLEXITY_OWNERS_SIZE.reset();
    FILTER_COMPLEXITY_TX_ACCOUNTS_INCLUDE_SIZE.reset();
    FILTER_COMPLEXITY_TX_ACCOUNTS_EXCLUDE_SIZE.reset();
    FILTER_COMPLEXITY_TX_ACCOUNTS_REQUIRED_SIZE.reset();
    FILTER_COMPLEXITY_TX_TOKEN_MODE_ENABLED.reset();
    FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_INCLUDE_SIZE.reset();
    FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_EXCLUDE_SIZE.reset();
    FILTER_COMPLEXITY_TX_STATUS_ACCOUNTS_REQUIRED_SIZE.reset();
    FILTER_COMPLEXITY_TX_STATUS_TOKEN_MODE_ENABLED.reset();
    FILTER_COMPLEXITY_BLOCKS_INCLUDE_SIZE.reset();
    FILTER_COMPLEXITY_BLOCKS_EXCLUDE_SIZE.reset();
    // Note: VERSION and GEYSER_ACCOUNT_UPDATE_RECEIVED are intentionally not reset
    // - VERSION contains build info set once on startup
    // - GEYSER_ACCOUNT_UPDATE_RECEIVED is a Histogram which doesn't support reset()
}
