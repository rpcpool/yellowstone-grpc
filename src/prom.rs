use prometheus::IntGauge;

use {
    crate::{config::ConfigPrometheus, version::VERSION as VERSION_INFO},
    futures::future::FutureExt,
    hyper::{
        server::conn::AddrStream,
        service::{make_service_fn, service_fn},
        Body, Request, Response, Server, StatusCode,
    },
    log::*,
    prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder},
    std::sync::Once,
    tokio::sync::oneshot,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["key", "value"]
    ).unwrap();

    pub static ref SLOT_STATUS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("slot_status", "Last processed slot by plugin"),
        &["status"]
    ).unwrap();

    pub static ref CONNECTIONS_TOTAL: IntGauge = IntGauge::new(
        "connections_total", "Total number of connections to GRPC service"
    ).unwrap();
}

#[derive(Debug)]
pub struct PrometheusService {
    shutdown_signal: oneshot::Sender<()>,
}

impl PrometheusService {
    pub fn new(config: Option<ConfigPrometheus>) -> hyper::Result<Self> {
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
            register!(CONNECTIONS_TOTAL);

            for (key, value) in &[
                ("version", VERSION_INFO.version),
                ("solana", VERSION_INFO.solana),
                ("git", VERSION_INFO.git),
                ("rustc", VERSION_INFO.rustc),
                ("buildts", VERSION_INFO.buildts),
            ] {
                VERSION.with_label_values(&[key, value]).inc()
            }
        });

        let (shutdown_signal, shutdown) = oneshot::channel();
        if let Some(ConfigPrometheus { address }) = config {
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
            let shutdown = shutdown.map(|_| Ok(()));
            tokio::spawn(async move {
                if let Err(error) = tokio::try_join!(server, shutdown) {
                    error!("prometheus service failed: {}", error);
                }
            });
        }

        Ok(PrometheusService { shutdown_signal })
    }

    pub fn shutdown(self) {
        let _ = self.shutdown_signal.send(());
    }
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
