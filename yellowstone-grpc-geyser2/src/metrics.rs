use {
    crate::{
        config::ConfigPrometheus, proto::geyser::SlotStatus, version::VERSION as VERSION_INFO,
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
    log::{error, info},
    prometheus::{IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder},
    std::{
        convert::Infallible,
        sync::Once,
    },
    tokio::net::TcpListener, tokio_util::sync::CancellationToken,
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
}

#[derive(Debug)]
pub struct PrometheusService;

impl PrometheusService {
    pub async fn spawn(
        config: ConfigPrometheus, 
        cancellation_token: CancellationToken
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

            crate::grpc::metrics::register_metrics(&REGISTRY).expect("metrics can't be registered");
        });
        let ConfigPrometheus { address } = config;
        let listener = TcpListener::bind(&address).await?;
        info!("start prometheus server: {address}");
        tokio::spawn(async move {
            loop {
                let stream = tokio::select! {
                    () = cancellation_token.cancelled() => break,
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
                tokio::spawn(async move {
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
                });
            }
        });

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

pub fn update_slot_status(status: &GeyserSlosStatus, slot: u64) {
    SLOT_STATUS
        .with_label_values(&[status.as_str()])
        .set(slot as i64);
}

pub fn update_slot_plugin_status(status: SlotStatus, slot: u64) {
    SLOT_STATUS_PLUGIN
        .with_label_values(&[status.as_str_name()])
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

pub fn missed_status_message_inc(status: SlotStatus) {
    MISSED_STATUS_MESSAGE
        .with_label_values(&[status.as_str_name()])
        .inc()
}
