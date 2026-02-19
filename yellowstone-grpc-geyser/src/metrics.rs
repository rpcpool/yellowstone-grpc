use {
    crate::{
        config::ConfigPrometheus,
        plugin::{filter::Filter, message::SlotStatus},
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
        Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
        TextEncoder,
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

    static ref GRPC_BYTES_SENT: IntCounterVec = IntCounterVec::new(
        Opts::new("grpc_bytes_sent", "Number of bytes sent over grpc to downstream client"),
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

    static ref GRPC_CONCURRENT_SUBSCRIBE_PER_TCP_CONNECTION: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "grpc_concurrent_subscribe_per_tcp_connection",
            "Current concurrent subscriptions per remote TCP peer socket address"
        ),
        &["remote_peer_sk_addr"]
    ).unwrap();

    static ref GEYSER_ACCOUNT_UPDATE_RECEIVED: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "geyser_account_update_data_size_kib",
            "Histogram of all account update data (kib) received from Geyser plugin"
        )
        .buckets(vec![5.0, 10.0, 20.0, 30.0, 50.0, 100.0, 200.0, 300.0, 500.0, 1000.0, 2000.0, 3000.0, 5000.0, 10000.0])
    ).unwrap();

    static ref TOTAL_TRAFFIC_SENT: IntCounter = IntCounter::new(
        "total_traffic_sent_bytes",
        "Total traffic sent to subscriber by type (account_update, block_meta, etc)"
    ).unwrap();

    static ref TRAFFIC_SENT_PER_REMOTE_IP: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "traffic_sent_per_remote_ip_bytes",
            "Total traffic sent to subscriber by remote IP"
        ),
        &["remote_ip"]
    ).unwrap();
}

pub fn add_traffic_sent_per_remote_ip<S: AsRef<str>>(remote_ip: S, bytes: u64) {
    TRAFFIC_SENT_PER_REMOTE_IP
        .with_label_values(&[remote_ip.as_ref()])
        .inc_by(bytes);
}

pub fn reset_traffic_sent_per_remote_ip<S: AsRef<str>>(remote_ip: S) {
    TRAFFIC_SENT_PER_REMOTE_IP
        .with_label_values(&[remote_ip.as_ref()])
        .reset();
    TRAFFIC_SENT_PER_REMOTE_IP
        .remove_label_values(&[remote_ip.as_ref()])
        .expect("remove_label_values")
}

pub fn add_total_traffic_sent(bytes: u64) {
    TOTAL_TRAFFIC_SENT.inc_by(bytes);
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
            register!(GRPC_BYTES_SENT);
            register!(GEYSER_ACCOUNT_UPDATE_RECEIVED);
            register!(GRPC_SUBSCRIBER_QUEUE_SIZE);
            register!(GRPC_CLIENT_DISCONNECTS);
            register!(GRPC_CONCURRENT_SUBSCRIBE_PER_TCP_CONNECTION);
            register!(TOTAL_TRAFFIC_SENT);
            register!(TRAFFIC_SENT_PER_REMOTE_IP);

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

pub fn incr_grpc_bytes_sent<S: AsRef<str>>(remote_id: S, byte_sent: u32) {
    GRPC_BYTES_SENT
        .with_label_values(&[remote_id.as_ref()])
        .inc_by(byte_sent as u64);
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

pub fn message_queue_size_dec() {
    MESSAGE_QUEUE_SIZE.dec()
}

pub fn connections_total_inc() {
    CONNECTIONS_TOTAL.inc()
}

pub fn connections_total_dec() {
    CONNECTIONS_TOTAL.dec()
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
    GRPC_SUBSCRIBER_QUEUE_SIZE.reset();
    GRPC_CONCURRENT_SUBSCRIBE_PER_TCP_CONNECTION.reset();

    // Reset counter vectors (clears all label combinations)
    MISSED_STATUS_MESSAGE.reset();
    GRPC_MESSAGE_SENT.reset();
    GRPC_BYTES_SENT.reset();
    GRPC_CLIENT_DISCONNECTS.reset();
    TOTAL_TRAFFIC_SENT.reset();
    TRAFFIC_SENT_PER_REMOTE_IP.reset();

    // Note: VERSION and GEYSER_ACCOUNT_UPDATE_RECEIVED are intentionally not reset
    // - VERSION contains build info set once on startup
    // - GEYSER_ACCOUNT_UPDATE_RECEIVED is a Histogram which doesn't support reset()
}
