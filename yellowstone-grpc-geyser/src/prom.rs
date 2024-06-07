use {
    crate::{config::ConfigPrometheus, filters::Filter, version::VERSION as VERSION_INFO},
    futures::future::FutureExt,
    hyper::{
        server::conn::AddrStream,
        service::{make_service_fn, service_fn},
        Body, Request, Response, Server, StatusCode,
    },
    log::error,
    prometheus::{IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder},
    solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    solana_sdk::clock::Slot,
    std::{
        collections::{hash_map::Entry as HashMapEntry, HashMap},
        sync::{Arc, Once},
    },
    tokio::{
        sync::{mpsc, oneshot},
        task::JoinHandle,
    },
    yellowstone_grpc_proto::prelude::CommitmentLevel,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    pub static ref SLOT_STATUS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("slot_status", "Lastest received slot from Geyser"),
        &["status"]
    ).unwrap();

    pub static ref SLOT_STATUS_PLUGIN: IntGaugeVec = IntGaugeVec::new(
        Opts::new("slot_status_plugin", "Latest processed slot in the plugin to client queues"),
        &["status"]
    ).unwrap();

    pub static ref INVALID_FULL_BLOCKS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("invalid_full_blocks_total", "Total number of fails on constructin full blocks"),
        &["reason"]
    ).unwrap();

    pub static ref MESSAGE_QUEUE_SIZE: IntGauge = IntGauge::new(
        "message_queue_size", "Size of geyser message queue"
    ).unwrap();

    pub static ref CONNECTIONS_TOTAL: IntGauge = IntGauge::new(
        "connections_total", "Total number of connections to gRPC service"
    ).unwrap();

    static ref SUBSCRIPTIONS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("subscriptions_total", "Total number of subscriptions to gRPC service"),
        &["endpoint", "subscription"]
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
    shutdown_signal: oneshot::Sender<()>,
}

impl PrometheusService {
    pub fn new(
        config: Option<ConfigPrometheus>,
        debug_clients_rx: Option<mpsc::UnboundedReceiver<DebugClientMessage>>,
    ) -> hyper::Result<Self> {
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

        let (shutdown_signal, shutdown) = oneshot::channel();
        let mut debug_clients_statuses = None;
        if let Some(ConfigPrometheus { address }) = config {
            if let Some(debug_clients_rx) = debug_clients_rx {
                debug_clients_statuses = Some(DebugClientStatuses::new(debug_clients_rx));
            }
            let debug_clients_statuses2 = debug_clients_statuses.clone();
            let make_service = make_service_fn(move |_: &AddrStream| {
                let debug_clients_statuses = debug_clients_statuses2.clone();
                async move {
                    let debug_clients_statuses = debug_clients_statuses.clone();
                    Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                        let debug_clients_statuses = debug_clients_statuses.clone();
                        async move {
                            let response = match req.uri().path() {
                                "/metrics" => metrics_handler(),
                                "/debug_clients" => {
                                    if let Some(debug_clients_statuses) = &debug_clients_statuses {
                                        let (status, body) =
                                            match debug_clients_statuses.get_statuses().await {
                                                Ok(body) => (StatusCode::OK, body),
                                                Err(error) => (
                                                    StatusCode::INTERNAL_SERVER_ERROR,
                                                    error.to_string(),
                                                ),
                                            };
                                        build_http_response(status, Body::from(body))
                                    } else {
                                        not_found_handler()
                                    }
                                }
                                _ => not_found_handler(),
                            };
                            Ok::<_, hyper::Error>(response)
                        }
                    }))
                }
            });
            let server = Server::try_bind(&address)?.serve(make_service);
            let shutdown = shutdown.map(|_| Ok(()));
            tokio::spawn(async move {
                if let Err(error) = tokio::try_join!(server, shutdown) {
                    error!("prometheus service failed: {}", error);
                }
            });
        }

        Ok(PrometheusService {
            debug_clients_statuses,
            shutdown_signal,
        })
    }

    pub fn shutdown(self) {
        drop(self.debug_clients_statuses);
        let _ = self.shutdown_signal.send(());
    }
}

fn build_http_response(status: StatusCode, body: Body) -> Response<Body> {
    Response::builder().status(status).body(body).unwrap()
}

fn metrics_handler() -> Response<Body> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    build_http_response(StatusCode::OK, Body::from(metrics))
}

fn not_found_handler() -> Response<Body> {
    build_http_response(StatusCode::NOT_FOUND, Body::empty())
}

pub fn update_slot_status(status: SlotStatus, slot: u64) {
    SLOT_STATUS
        .with_label_values(&[match status {
            SlotStatus::Processed => "processed",
            SlotStatus::Confirmed => "confirmed",
            SlotStatus::Rooted => "finalized",
        }])
        .set(slot as i64);
}

pub fn update_slot_plugin_status(status: CommitmentLevel, slot: u64) {
    SLOT_STATUS_PLUGIN
        .with_label_values(&[match status {
            CommitmentLevel::Processed => "processed",
            CommitmentLevel::Confirmed => "confirmed",
            CommitmentLevel::Finalized => "finalized",
        }])
        .set(slot as i64);
}

pub fn update_invalid_blocks(reason: impl AsRef<str>) {
    INVALID_FULL_BLOCKS
        .with_label_values(&[reason.as_ref()])
        .inc();
    INVALID_FULL_BLOCKS.with_label_values(&["all"]).inc();
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
