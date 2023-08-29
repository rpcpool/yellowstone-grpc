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
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();
}

pub fn run_server(address: Option<SocketAddr>) -> anyhow::Result<()> {
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

    if let Some(address) = address {
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
    }

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
