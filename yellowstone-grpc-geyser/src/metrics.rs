use {
    crate::{config::ConfigPrometheus, version::VERSION as VERSION_INFO},
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
    log::{error, info},
    prometheus::{
        HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
        TextEncoder, TEXT_FORMAT,
    },
    solana_clock::Slot,
    std::{
        collections::{hash_map::Entry as HashMapEntry, HashMap},
        convert::Infallible,
        sync::{Arc, Once},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        net::TcpListener,
        sync::{mpsc, oneshot, Notify},
        task::JoinHandle,
    },
    yellowstone_grpc_proto::plugin::{
        filter::Filter,
        message::{CommitmentLevel, SlotStatus},
    },
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
    // This metric measures the transmission delay from block production to the RPC node, in milliseconds.
    // Possible values for mark_point:
    // - created: Marked when the block created
    // - ready_to_send: Marked when the block is ready to be sent to the client
    static ref BLOCK_RECEIVING_DELAY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("block_receiving_delay", "Block propagation time: from block generation to plugin reception,unit seconds").buckets(vec![
            0.005,
            0.01,
            0.025,
            0.05,
            0.1,
            0.25,
            0.5,
            1.0,
            1.5,
            2.0,
            2.5,
            3.0,
            3.5,
            4.0,
            4.5,
            5.0,
            10.0,
            20.0,
            100.0,
            500.0,
            1000.0
            ]),
        &["commitment","mark_point"]
    ).unwrap();
    //  Total number of messages sent to the client
    static ref MESSAGES_SENT_TOTAL: IntGauge = IntGauge::new(
        "geyser_messages_sent_total","Total number of messages sent to clients"
    ).unwrap();

    // Total number of messages sent to the client
    static ref CONNECTIONS_CLOSED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("geyser_connections_closed_total","Total number of closed connections by reason"),
        &["reason"]
    ).unwrap();

    // Client queue size
    static ref CLIENT_QUEUE_SIZE: IntGauge = IntGauge::new(
        "geyser_client_queue_size","Size of client message queue",
    ).unwrap();

    // The message size sent to the client
    static ref MESSAGE_SIZE_BYTES: IntGauge = IntGauge::new(
        "geyser_message_size_bytes","Size of messages sent to clients in bytes",
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
    fn new(clients_rx: mpsc::UnboundedReceiver<DebugClientMessage>) -> Arc<Self> {
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        let jh = tokio::spawn(Self::run(clients_rx, requests_rx));
        Arc::new(Self { requests_tx, jh })
    }

    async fn run(
        mut clients_rx: mpsc::UnboundedReceiver<DebugClientMessage>,
        mut requests_rx: mpsc::UnboundedReceiver<oneshot::Sender<String>>,
    ) {
        let mut clients = HashMap::<usize, DebugClientStatus>::new();
        loop {
            tokio::select! {
                Some(message) = clients_rx.recv() => match message {
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

#[derive(Debug)]
pub struct PrometheusService {
    debug_clients_statuses: Option<Arc<DebugClientStatuses>>,
    shutdown: Arc<Notify>,
}

impl PrometheusService {
    pub async fn new(
        config: Option<ConfigPrometheus>,
        debug_clients_rx: Option<mpsc::UnboundedReceiver<DebugClientMessage>>,
    ) -> std::io::Result<Self> {
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
            register!(BLOCK_RECEIVING_DELAY);
            register!(MESSAGES_SENT_TOTAL);
            register!(CLIENT_QUEUE_SIZE);
            register!(MESSAGE_SIZE_BYTES);

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

        let shutdown = Arc::new(Notify::new());
        let mut debug_clients_statuses = None;
        if let Some(ConfigPrometheus { address }) = config {
            if let Some(debug_clients_rx) = debug_clients_rx {
                debug_clients_statuses = Some(DebugClientStatuses::new(debug_clients_rx));
            }
            let debug_clients_statuses2 = debug_clients_statuses.clone();

            let shutdown = Arc::clone(&shutdown);
            let listener = TcpListener::bind(&address).await?;
            info!("start prometheus server: {address}");
            tokio::spawn(async move {
                loop {
                    let stream = tokio::select! {
                        () = shutdown.notified() => break,
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
                    tokio::spawn(async move {
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
                    });
                }
            });
        }

        Ok(PrometheusService {
            debug_clients_statuses,
            shutdown,
        })
    }

    pub fn shutdown(self) {
        drop(self.debug_clients_statuses);
        self.shutdown.notify_one();
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
        .header("Content-Type", TEXT_FORMAT)
        .body(BodyFull::new(Bytes::from(metrics)).boxed())
}

fn not_found_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(BodyEmpty::new().boxed())
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

pub fn update_block_receiving_delay(commitment: CommitmentLevel, point: &str, timestamp_secs: i64) {
    let now = SystemTime::now();
    let target_time = if timestamp_secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(timestamp_secs as u64)
    } else {
        let abs_secs = (-timestamp_secs) as u64;
        UNIX_EPOCH - Duration::from_secs(abs_secs)
    };
    let d = match now.duration_since(target_time) {
        Ok(duration) => duration.as_millis() as i64,
        Err(e) => -(e.duration().as_millis() as i64),
    };
    BLOCK_RECEIVING_DELAY
        .with_label_values(&[commitment.as_str(), point])
        .observe(d as f64 / 1000.0);
}

pub fn messages_sent_inc() {
    MESSAGES_SENT_TOTAL.inc();
}

pub fn connection_closed_inc(reason: &str) {
    CONNECTIONS_CLOSED_TOTAL.with_label_values(&[reason]).inc();
}

pub fn update_client_queue_size(size: usize) {
    CLIENT_QUEUE_SIZE.set(size as i64);
}

pub fn update_message_size(size: usize) {
    MESSAGE_SIZE_BYTES.set(size as i64);
}
