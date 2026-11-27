//! The standard engine of this release: tonic's server with today's HTTP/2 settings.

use {
    crate::{
        extension::seams::{ExtensionError, ListenerHttp2Settings, SubscribeEngine},
        grpc::GrpcService,
        threads::STUCK_THREAD_REPORT_INTERVAL,
    },
    futures::Stream,
    std::{
        future::Future,
        io,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::{Duration, Instant},
    },
    tokio::io::{AsyncRead, AsyncWrite},
    tokio_util::sync::CancellationToken,
    tonic::{service::Routes, transport::server::Connected},
};

/// The engine slot's standard adapter in this release.
pub type StandardSubscribeEngine = TonicServerEngine;

/// Serves each listener with tonic's server, as the plugin's own listeners are served.
#[derive(Debug, Default)]
pub struct TonicServerEngine {
    /// Cancelled by `stop_and_join`; each listener's shutdown is a child of it.
    engine_shutdown: CancellationToken,
    /// Serve futures handed out that have not ended or dropped.
    serving_listener_count: Arc<AtomicUsize>,
}

/// Counts one serve future as serving until it ends or drops.
struct ServingListenerGuard {
    serving_listener_count: Arc<AtomicUsize>,
}

impl Drop for ServingListenerGuard {
    fn drop(&mut self) {
        self.serving_listener_count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl TonicServerEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn serving_listener_count(&self) -> usize {
        self.serving_listener_count.load(Ordering::Acquire)
    }
}

impl SubscribeEngine for TonicServerEngine {
    fn serve_listener<IncomingConnections, ConnectionIo>(
        &self,
        incoming_connections: IncomingConnections,
        listener_http2_settings: ListenerHttp2Settings,
        listener_routes: Routes,
        serve_shutdown: CancellationToken,
    ) -> impl Future<Output = Result<(), ExtensionError>> + Send + 'static
    where
        IncomingConnections: Stream<Item = io::Result<ConnectionIo>> + Send + 'static,
        ConnectionIo: Connected + AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.serving_listener_count.fetch_add(1, Ordering::AcqRel);
        let serving_listener_guard = ServingListenerGuard {
            serving_listener_count: Arc::clone(&self.serving_listener_count),
        };
        let listener_shutdown = self.engine_shutdown.child_token();
        async move {
            let _serving_listener_guard = serving_listener_guard;
            let serve_future = GrpcService::serve_listener(
                incoming_connections,
                listener_http2_settings.adaptive_window,
                listener_http2_settings.keepalive_interval,
                listener_http2_settings.keepalive_timeout,
                listener_http2_settings.initial_connection_window_size,
                listener_http2_settings.initial_stream_window_size,
                listener_routes,
                listener_shutdown.clone(),
            );
            tokio::pin!(serve_future);
            let serve_result = tokio::select! {
                serve_result = &mut serve_future => serve_result,
                () = serve_shutdown.cancelled() => {
                    listener_shutdown.cancel();
                    serve_future.await
                }
            };
            serve_result.map_err(|serve_error| {
                ExtensionError::with_source("the tonic server failed", serve_error)
            })
        }
    }

    /// Shuts every listener down gracefully (GOAWAY), then waits, with no bound, until every
    /// serve future has ended or dropped. Call it off the runtime that polls them.
    fn stop_and_join(&self) {
        self.engine_shutdown.cancel();
        let join_started_at = Instant::now();
        let mut next_report_at = join_started_at + STUCK_THREAD_REPORT_INTERVAL;
        while self.serving_listener_count() > 0 {
            if Instant::now() >= next_report_at {
                log::warn!(
                    "the tonic engine still waits after {:?} for {} serving listeners",
                    join_started_at.elapsed(),
                    self.serving_listener_count()
                );
                next_report_at += STUCK_THREAD_REPORT_INTERVAL;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
}
