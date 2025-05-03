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
        TextEncoder,
    },
    solana_sdk::clock::Slot,
    std::{
        collections::{hash_map::Entry as HashMapEntry, HashMap},
        convert::Infallible,
        sync::{
            atomic::{AtomicI64, AtomicUsize, Ordering},
            Arc, Once,
        },
    },
    tokio::{
        net::TcpListener,
        sync::{mpsc, oneshot, Notify},
        task::JoinHandle,
    },
    yellowstone_grpc_proto::plugin::{filter::Filter, message::SlotStatus},
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    static ref SLOT_STATUS: Arc<AtomicI64> = Arc::new(AtomicI64::new(0));

    static ref SLOT_STATUS_PLUGIN: Arc<AtomicI64> = Arc::new(AtomicI64::new(0));

    static ref INVALID_FULL_BLOCKS: Arc<AtomicI64> = Arc::new(AtomicI64::new(0));

    static ref MESSAGE_QUEUE_SIZE: Arc<AtomicI64> = Arc::new(AtomicI64::new(0));

    static ref CONNECTIONS_TOTAL: Arc<AtomicI64> = Arc::new(AtomicI64::new(0));

    static ref SUBSCRIPTIONS_TOTAL: Arc<AtomicI64> = Arc::new(AtomicI64::new(0));

    static ref MISSED_STATUS_MESSAGE: IntCounterVec = IntCounterVec::new(
        Opts::new("missed_status_message_total", "Number of missed messages by commitment"),
        &["status"]
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
        .body(BodyFull::new(Bytes::from(metrics)).boxed())
}

fn not_found_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(BodyEmpty::new().boxed())
}

pub fn update_slot_status(status: &GeyserSlosStatus, slot: u64) {
    ::metrics::gauge!("slot_status", "status" => status.as_str()).set(slot as f64);
}

pub fn update_slot_plugin_status(status: SlotStatus, slot: u64) {
    ::metrics::gauge!("slot_status_plugin", "status" => status.as_str()).set(slot as f64);
}

pub fn update_invalid_blocks(reason: String) {
    INVALID_FULL_BLOCKS.fetch_add(1, Ordering::Relaxed);
    ::metrics::gauge!("invalid_full_blocks", "reason" => reason)
        .set(INVALID_FULL_BLOCKS.load(Ordering::Relaxed) as f64);
}

pub fn message_queue_size_inc() {
    MESSAGE_QUEUE_SIZE.fetch_add(1, Ordering::Relaxed);
    ::metrics::gauge!("message_queue_size").set(MESSAGE_QUEUE_SIZE.load(Ordering::Relaxed) as f64);
}

pub fn message_queue_size_dec() {
    MESSAGE_QUEUE_SIZE.fetch_sub(1, Ordering::Relaxed);
    ::metrics::gauge!("message_queue_size").set(MESSAGE_QUEUE_SIZE.load(Ordering::Relaxed) as f64);
}

pub fn connections_total_inc() {
    CONNECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
    ::metrics::gauge!("connections_total").set(CONNECTIONS_TOTAL.load(Ordering::Relaxed) as f64);
}

pub fn connections_total_dec() {
    CONNECTIONS_TOTAL.fetch_sub(1, Ordering::Relaxed);
    ::metrics::gauge!("connections_total").set(CONNECTIONS_TOTAL.load(Ordering::Relaxed) as f64);
}

pub fn update_subscriptions(endpoint: &str, old: Option<&Filter>, new: Option<&Filter>) {
    for (multiplier, filter) in [(-1, old), (1, new)] {
        if let Some(filter) = filter {
            #[cfg(feature = "statsd")]
            {
                SUBSCRIPTIONS_TOTAL.fetch_add(multiplier, Ordering::Relaxed);
                ::metrics::gauge!("subscriptions_total", "endpoint" => endpoint.to_string(), "filter" => "grpc_total").set(SUBSCRIPTIONS_TOTAL.load(Ordering::Relaxed) as f64);
            }
            // NOTE: These below do not work with statsd
            let endpoint = endpoint.to_string();
            ::metrics::gauge!("subscriptions_total", "endpoint" => endpoint.clone(), "filter" => "grpc_total").increment(multiplier as f64);

            for (name, value) in filter.get_metrics() {
                ::metrics::gauge!("subscriptions_total", "endpoint" => endpoint.clone(), "filter" => name)
                    .increment(value as f64 * multiplier as f64);
            }
        }
    }
}

pub fn missed_status_message_inc(status: SlotStatus) {
    MISSED_STATUS_MESSAGE
        .with_label_values(&[status.as_str()])
        .inc()
}
