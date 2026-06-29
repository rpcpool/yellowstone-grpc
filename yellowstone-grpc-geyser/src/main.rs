use std::path::Path;
use std::sync::Once;

use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio_rustls::rustls;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use yellowstone_grpc_geyser::{
    config::Config,
    grpc::GrpcService,
    metrics::PrometheusService,
};
use yellowstone_shmem_client::ShmemClient;
use yellowstone_grpc_geyser::plugin::shmem::decoder::ProstShmemDecoder;

fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: yellowstone-grpc <config.json>"))?;

    let config = Config::load_from_file(&config_path)?;

    solana_logger::setup_with_default(&config.log.level);
    log::info!("starting yellowstone-grpc server");

    // Open conduit ring
    let shmem_path = config.grpc.shmem_path.as_ref()
        .ok_or_else(|| anyhow::anyhow!("config: grpc.shmem_path is required"))?;
    let client = ShmemClient::open(Path::new(shmem_path), ProstShmemDecoder)?;

    let mut builder = Builder::new_multi_thread();
    if let Some(worker_threads) = config.tokio.worker_threads {
        builder.worker_threads(worker_threads);
    }

    let runtime = builder
        .thread_name("yellowstone-grpc")
        .enable_all()
        .build()?;

    static CRYPTO_PROVIDER_INIT: Once = Once::new();
    CRYPTO_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });

    runtime.block_on(async move {
        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        let (debug_client_tx, debug_client_rx) = mpsc::unbounded_channel();

        PrometheusService::spawn(
            config.prometheus,
            config.debug_clients_http.then_some(debug_client_rx),
            cancellation_token.child_token(),
            task_tracker.clone(),
        )
        .await?;

        let _snapshot_tx = GrpcService::create(
            config.grpc,
            client,
            config.debug_clients_http.then_some(debug_client_tx),
            false,
            cancellation_token.child_token(),
            task_tracker.clone(),
        )
        .await?;

        let cancel = cancellation_token.clone();
        task_tracker.spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        log::info!("received SIGINT, shutting down");
                    }
                    _ = sigterm.recv() => {
                        log::info!("received SIGTERM, shutting down");
                    }
                }
            }
            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c().await.ok();
                log::info!("received SIGINT, shutting down");
            }
            cancel.cancel();
        });

        task_tracker.close();
        task_tracker.wait().await;

        log::info!("yellowstone-grpc server shutdown complete");
        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}