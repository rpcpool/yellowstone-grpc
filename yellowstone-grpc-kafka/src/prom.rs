use {
    crate::version::VERSION as VERSION_INFO,
    hyper::{
        server::conn::AddrStream,
        service::{make_service_fn, service_fn},
        Body, Request, Response, Server, StatusCode,
    },
    prometheus::{GaugeVec, IntCounter, IntCounterVec, Opts, Registry, TextEncoder},
    std::{net::SocketAddr, sync::Once},
    tracing::{error, info},
};

lazy_static::lazy_static! {
    static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    static ref KAFKA_STATS: GaugeVec = GaugeVec::new(
        Opts::new("kafka_stats", "librdkafka metrics"),
        &["broker", "metric"]
    ).unwrap();

    static ref KAFKA_DEDUP_TOTAL: IntCounter = IntCounter::new(
        "kafka_dedup_total", "Total number of deduplicated messages"
    ).unwrap();

    static ref KAFKA_RECV_TOTAL: IntCounter = IntCounter::new(
        "kafka_recv_total", "Total number of received messages"
    ).unwrap();

    static ref KAFKA_SENT_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("kafka_sent_total", "Total number of uploaded messages by type"),
        &["kind"]
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
        register!(KAFKA_STATS);
        register!(KAFKA_DEDUP_TOTAL);
        register!(KAFKA_RECV_TOTAL);
        register!(KAFKA_SENT_TOTAL);

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

pub mod kafka {
    use {
        super::{KAFKA_DEDUP_TOTAL, KAFKA_RECV_TOTAL, KAFKA_SENT_TOTAL, KAFKA_STATS},
        rdkafka::{
            client::ClientContext,
            config::{ClientConfig, FromClientConfigAndContext},
            consumer::{ConsumerContext, StreamConsumer},
            error::KafkaResult,
            producer::FutureProducer,
            statistics::Statistics,
        },
        yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof,
    };

    #[derive(Debug, Default, Clone, Copy)]
    pub struct StatsContext;

    impl ClientContext for StatsContext {
        fn stats(&self, statistics: Statistics) {
            for (name, broker) in statistics.brokers {
                macro_rules! set_value {
                    ($name:expr, $value:expr) => {
                        KAFKA_STATS
                            .with_label_values(&[&name, $name])
                            .set($value as f64);
                    };
                }

                set_value!("outbuf_cnt", broker.outbuf_cnt);
                set_value!("outbuf_msg_cnt", broker.outbuf_msg_cnt);
                set_value!("waitresp_cnt", broker.waitresp_cnt);
                set_value!("waitresp_msg_cnt", broker.waitresp_msg_cnt);
                set_value!("tx", broker.tx);
                set_value!("txerrs", broker.txerrs);
                set_value!("txretries", broker.txretries);
                set_value!("req_timeouts", broker.req_timeouts);

                if let Some(window) = broker.int_latency {
                    set_value!("int_latency.min", window.min);
                    set_value!("int_latency.max", window.max);
                    set_value!("int_latency.avg", window.avg);
                    set_value!("int_latency.sum", window.sum);
                    set_value!("int_latency.cnt", window.cnt);
                    set_value!("int_latency.stddev", window.stddev);
                    set_value!("int_latency.hdrsize", window.hdrsize);
                    set_value!("int_latency.p50", window.p50);
                    set_value!("int_latency.p75", window.p75);
                    set_value!("int_latency.p90", window.p90);
                    set_value!("int_latency.p95", window.p95);
                    set_value!("int_latency.p99", window.p99);
                    set_value!("int_latency.p99_99", window.p99_99);
                    set_value!("int_latency.outofrange", window.outofrange);
                }

                if let Some(window) = broker.outbuf_latency {
                    set_value!("outbuf_latency.min", window.min);
                    set_value!("outbuf_latency.max", window.max);
                    set_value!("outbuf_latency.avg", window.avg);
                    set_value!("outbuf_latency.sum", window.sum);
                    set_value!("outbuf_latency.cnt", window.cnt);
                    set_value!("outbuf_latency.stddev", window.stddev);
                    set_value!("outbuf_latency.hdrsize", window.hdrsize);
                    set_value!("outbuf_latency.p50", window.p50);
                    set_value!("outbuf_latency.p75", window.p75);
                    set_value!("outbuf_latency.p90", window.p90);
                    set_value!("outbuf_latency.p95", window.p95);
                    set_value!("outbuf_latency.p99", window.p99);
                    set_value!("outbuf_latency.p99_99", window.p99_99);
                    set_value!("outbuf_latency.outofrange", window.outofrange);
                }
            }
        }
    }

    impl ConsumerContext for StatsContext {}

    impl StatsContext {
        pub fn create_future_producer(config: &ClientConfig) -> KafkaResult<FutureProducer<Self>> {
            FutureProducer::from_config_and_context(config, Self)
        }

        pub fn create_stream_consumer(config: &ClientConfig) -> KafkaResult<StreamConsumer<Self>> {
            StreamConsumer::from_config_and_context(config, Self)
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub enum GprcMessageKind {
        Account,
        Slot,
        Transaction,
        Block,
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
                UpdateOneof::Block(_) => Self::Block,
                UpdateOneof::Ping(_) => unreachable!(),
                UpdateOneof::BlockMeta(_) => Self::BlockMeta,
                UpdateOneof::Entry(_) => Self::Entry,
            }
        }
    }

    impl GprcMessageKind {
        const fn as_str(self) -> &'static str {
            match self {
                GprcMessageKind::Account => "account",
                GprcMessageKind::Slot => "slot",
                GprcMessageKind::Transaction => "transaction",
                GprcMessageKind::Block => "block",
                GprcMessageKind::BlockMeta => "blockmeta",
                GprcMessageKind::Entry => "entry",
                GprcMessageKind::Unknown => "unknown",
            }
        }
    }

    pub fn sent_inc(kind: GprcMessageKind) {
        KAFKA_SENT_TOTAL.with_label_values(&[kind.as_str()]).inc()
    }

    pub fn recv_inc() {
        KAFKA_RECV_TOTAL.inc();
    }

    pub fn dedup_inc() {
        KAFKA_DEDUP_TOTAL.inc();
    }
}
