use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use yellowstone_grpc_geyser::{
    config::Config,
    grpc::GrpcService,
    metrics::PrometheusService,
    parallel::ParallelEncoder,
};


fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: yellowstone-grpc-geyser <config.json>"))?;

    let config = Config::load_from_file(&config_path)?;

    solana_logger::setup_with_default(&config.log.level);

    log::info!("starting yellowstone-grpc-geyser");

    let mut builder = Builder::new_multi_thread();
    if let Some(worker_threads) = config.tokio.worker_threads {
        builder.worker_threads(worker_threads);
    }

    let runtime = builder
        .thread_name("yellowstone-grpc")
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        let encoder_threads = config.grpc.encoder_threads;
        let (encoder, encoder_handle) = ParallelEncoder::new(encoder_threads);

        let (debug_client_tx, debug_client_rx) = mpsc::unbounded_channel();

        PrometheusService::spawn(
            config.prometheus,
            config.debug_clients_http.then_some(debug_client_rx),
            cancellation_token.child_token(),
            task_tracker.clone(),
        )
        .await?;

        let (_snapshot_tx, messages_tx) = GrpcService::create(
            config.grpc,
            config.debug_clients_http.then_some(debug_client_tx),
            false,
            cancellation_token.child_token(),
            task_tracker.clone(),
            encoder,
        )
        .await?;

        // Keep messages_tx alive — dropping it closes the channel and exits geyser_loop.
        let _keep_alive = messages_tx;

        // Wait for SIGTERM or SIGINT.
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

        if let Err(e) = encoder_handle.join() {
            log::error!("encoder thread panicked: {:?}", e);
        }

        log::info!("yellowstone-grpc-geyser shutdown complete");
        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}