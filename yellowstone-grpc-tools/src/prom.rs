#[cfg(feature = "google-pubsub")]
use crate::google_pubsub::prom::{
    GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS, GOOGLE_PUBSUB_DROP_OVERSIZED_TOTAL,
    GOOGLE_PUBSUB_RECV_TOTAL, GOOGLE_PUBSUB_SEND_BATCHES_IN_PROGRESS, GOOGLE_PUBSUB_SENT_TOTAL,
    GOOGLE_PUBSUB_SLOT_TIP,
};
#[cfg(feature = "kafka")]
use crate::kafka::prom::{KAFKA_DEDUP_TOTAL, KAFKA_RECV_TOTAL, KAFKA_SENT_TOTAL, KAFKA_STATS};
#[cfg(feature = "scylla")]
use crate::scylladb::prom::{
    SCYLLADB_BATCHITEM_DELIVERED, SCYLLADB_BATCH_DELIVERED, SCYLLADB_BATCH_QUEUE,
    SCYLLADB_BATCH_REQUEST_LAG, SCYLLADB_BATCH_SIZE, SCYLLADB_PEAK_BATCH_LINGER_SECONDS,
};
use {
    crate::version::VERSION as VERSION_INFO,
    hyper::{
        server::conn::AddrStream,
        service::{make_service_fn, service_fn},
        Body, Request, Response, Server, StatusCode,
    },
    prometheus::{IntCounterVec, Opts, Registry, TextEncoder},
    std::{net::SocketAddr, sync::Once},
    tracing::{error, info},
    yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof,
};

lazy_static::lazy_static! {
    static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();
}

pub fn run_server(address: SocketAddr) -> anyhow::Result<()> {
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
        #[cfg(feature = "google-pubsub")]
        {
            register!(GOOGLE_PUBSUB_RECV_TOTAL);
            register!(GOOGLE_PUBSUB_SENT_TOTAL);
            register!(GOOGLE_PUBSUB_SEND_BATCHES_IN_PROGRESS);
            register!(GOOGLE_PUBSUB_AWAITERS_IN_PROGRESS);
            register!(GOOGLE_PUBSUB_DROP_OVERSIZED_TOTAL);
            register!(GOOGLE_PUBSUB_SLOT_TIP);
        }
        #[cfg(feature = "kafka")]
        {
            register!(KAFKA_STATS);
            register!(KAFKA_DEDUP_TOTAL);
            register!(KAFKA_RECV_TOTAL);
            register!(KAFKA_SENT_TOTAL);
        }

        #[cfg(feature = "scylla")]
        {
            register!(SCYLLADB_PEAK_BATCH_LINGER_SECONDS);
            register!(SCYLLADB_BATCH_DELIVERED);
            register!(SCYLLADB_BATCHITEM_DELIVERED);
            register!(SCYLLADB_BATCH_SIZE);
            register!(SCYLLADB_BATCH_QUEUE);
            register!(SCYLLADB_BATCH_REQUEST_LAG);
        }

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

    let make_service = make_service_fn(move |_: &AddrStream| async move {
        Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| async move {
            let response = match req.uri().path() {
                "/metrics" => metrics_handler(),
                _ => not_found_handler(),
            };
            Ok::<_, hyper::Error>(response)
        }))
    });
    let server = Server::try_bind(&address)?.serve(make_service);
    info!("prometheus server started: {address:?}");
    tokio::spawn(async move {
        if let Err(error) = server.await {
            error!("prometheus server failed: {error:?}");
        }
    });

    Ok(())
}

fn metrics_handler() -> Response<Body> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    Response::builder().body(Body::from(metrics)).unwrap()
}

fn not_found_handler() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}

#[derive(Debug, Clone, Copy)]
pub enum GprcMessageKind {
    Account,
    Slot,
    Transaction,
    TransactionStatus,
    Block,
    Ping,
    Pong,
    BlockMeta,
    Entry,
    Unknown,
}

impl From<&UpdateOneof> for GprcMessageKind {
    fn from(msg: &UpdateOneof) -> Self {
        match msg {
            UpdateOneof::Account(_) => Self::Account,
            UpdateOneof::Slot(_) => Self::Slot,
            UpdateOneof::Transaction(_) => Self::Transaction,
            UpdateOneof::TransactionStatus(_) => Self::TransactionStatus,
            UpdateOneof::Block(_) => Self::Block,
            UpdateOneof::Ping(_) => Self::Ping,
            UpdateOneof::Pong(_) => Self::Pong,
            UpdateOneof::BlockMeta(_) => Self::BlockMeta,
            UpdateOneof::Entry(_) => Self::Entry,
        }
    }
}

impl GprcMessageKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            GprcMessageKind::Account => "account",
            GprcMessageKind::Slot => "slot",
            GprcMessageKind::Transaction => "transaction",
            GprcMessageKind::TransactionStatus => "transactionstatus",
            GprcMessageKind::Block => "block",
            GprcMessageKind::Ping => "ping",
            GprcMessageKind::Pong => "pong",
            GprcMessageKind::BlockMeta => "blockmeta",
            GprcMessageKind::Entry => "entry",
            GprcMessageKind::Unknown => "unknown",
        }
    }
}
