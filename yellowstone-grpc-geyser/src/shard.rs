//! gRPC shards: one pinned thread per core, each with its own runtime and its own TCP
//! listeners on the shared ports, so the kernel spreads clients across the cores and each
//! client's connection, HTTP/2 task and response stream run on one core.

use {
    crate::{grpc::GrpcService, ratelimit::PrometheusRatelimitCallbacks},
    log::{error, warn},
    std::{net::SocketAddr, thread::JoinHandle, time::Duration},
    tokio::sync::oneshot,
    tokio_rustls::TlsAcceptor,
    tokio_util::sync::CancellationToken,
    yellowstone_grpc_tools::server::{
        tcp::{TcpConfiguration, TcpIncoming},
        tls::TlsIncoming,
        tonic::ratelimit::transport::{RateLimitedIncoming, SharedRateLimitTable},
    },
};

/// Longest wait for a shard's tasks to stop after shutdown.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// TCP keepalive on accepted sockets, as on the unsharded listeners.
const TCP_KEEPALIVE: Duration = Duration::from_secs(20);

/// One TCP listener that every shard binds: its address, optional TLS and its service.
#[derive(Clone)]
pub(crate) struct ShardListener {
    /// The address every shard listens on.
    pub(crate) addr: SocketAddr,
    /// The TLS acceptor, or [`None`] for plaintext.
    pub(crate) tls: Option<TlsAcceptor>,
    /// The service with its auth, rate limit and metering layers.
    pub(crate) routes: tonic::service::Routes,
}

/// The HTTP/2 settings each shard's server uses, as on the unsharded listeners.
#[derive(Clone, Copy)]
pub(crate) struct Http2Settings {
    pub(crate) adaptive_window: Option<bool>,
    pub(crate) keepalive_interval: Option<Duration>,
    pub(crate) keepalive_timeout: Option<Duration>,
    pub(crate) initial_connection_window_size: Option<u32>,
    pub(crate) initial_stream_window_size: Option<u32>,
}

/// The running shard threads.
///
/// # Examples
///
/// ```ignore
/// let shards = spawn_shards(&[12, 13], listeners, settings, 100, table, shutdown).await?;
/// shutdown.cancel();
/// shards.join();
/// ```
#[derive(Debug, Default)]
pub struct ShardThreads(Vec<JoinHandle<()>>);

impl ShardThreads {
    /// Waits for every shard thread to exit. Call it after the shutdown token is cancelled.
    pub fn join(self) {
        for thread in self.0 {
            if thread.join().is_err() {
                error!("gRPC shard thread panicked");
            }
        }
    }
}

/// Starts one shard per core and waits until each has bound its listeners.
///
/// # Arguments
///
/// * `cores` - The cores to pin the shards to, one shard each.
/// * `listeners` - The listeners every shard binds.
/// * `http2` - The HTTP/2 settings of each shard's server.
/// * `ip_conncur` - The most concurrent connections per client IP, per listener.
/// * `rate_limit_table` - The connection counts shared by every listener.
/// * `shutdown` - Stops the shards when cancelled.
///
/// # Returns
///
/// The shard threads.
///
/// # Errors
///
/// Returns an error if a thread cannot start, or a shard cannot build its runtime or bind a
/// listener. The shards already started then stop.
pub(crate) async fn spawn_shards(
    cores: &[usize],
    listeners: Vec<ShardListener>,
    http2: Http2Settings,
    ip_conncur: u64,
    rate_limit_table: SharedRateLimitTable,
    shutdown: CancellationToken,
) -> anyhow::Result<ShardThreads> {
    let mut threads = Vec::with_capacity(cores.len());
    let mut ready = Vec::with_capacity(cores.len());
    for (index, &core) in cores.iter().enumerate() {
        let (ready_tx, ready_rx) = oneshot::channel();
        let listeners = listeners.clone();
        let rate_limit_table = rate_limit_table.clone();
        let shard_shutdown = shutdown.clone();
        let thread = std::thread::Builder::new()
            .name(format!("solGrpcShard{index:02}"))
            .spawn(move || {
                run_shard(
                    core,
                    listeners,
                    http2,
                    ip_conncur,
                    rate_limit_table,
                    shard_shutdown,
                    ready_tx,
                )
            });
        match thread {
            Ok(thread) => threads.push(thread),
            Err(error) => {
                shutdown.cancel();
                ShardThreads(threads).join();
                return Err(error.into());
            }
        }
        ready.push(ready_rx);
    }
    for ready_rx in ready {
        let started = match ready_rx.await {
            Ok(started) => started,
            Err(_) => Err(anyhow::anyhow!("gRPC shard exited before it was ready")),
        };
        if let Err(error) = started {
            shutdown.cancel();
            ShardThreads(threads).join();
            return Err(error);
        }
    }
    Ok(ShardThreads(threads))
}

/// Runs one shard on the calling thread until `shutdown` is cancelled.
///
/// # Arguments
///
/// * `core` - The core to pin the thread to.
/// * `listeners` - The listeners to bind.
/// * `http2` - The HTTP/2 settings of the server.
/// * `ip_conncur` - The most concurrent connections per client IP, per listener.
/// * `rate_limit_table` - The connection counts shared by every listener.
/// * `shutdown` - Stops the shard when cancelled.
/// * `ready_tx` - Told once the listeners are bound, or why they could not be.
fn run_shard(
    core: usize,
    listeners: Vec<ShardListener>,
    http2: Http2Settings,
    ip_conncur: u64,
    rate_limit_table: SharedRateLimitTable,
    shutdown: CancellationToken,
    ready_tx: oneshot::Sender<anyhow::Result<()>>,
) {
    if let Err(error) = crate::util::cpu_core_affinity::set_thread_affinity(&[core]) {
        warn!("failed to pin gRPC shard to core {core}: {error}");
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready_tx.send(Err(error.into()));
            return;
        }
    };
    runtime.block_on(async {
        let mut bound = Vec::with_capacity(listeners.len());
        for listener in listeners {
            let config = TcpConfiguration::default()
                .with_nodelay(Some(true))
                .with_keepalive(Some(TCP_KEEPALIVE));
            match TcpIncoming::bind_reuse_port(listener.addr, config) {
                Ok(incoming) => bound.push((incoming, listener)),
                Err(error) => {
                    let addr = listener.addr;
                    let _ = ready_tx.send(Err(anyhow::anyhow!(
                        "gRPC shard failed to bind {addr}: {error}"
                    )));
                    return;
                }
            }
        }
        let _ = ready_tx.send(Ok(()));
        for (incoming, listener) in bound {
            tokio::spawn(serve(
                incoming,
                listener,
                http2,
                ip_conncur,
                rate_limit_table.clone(),
                shutdown.clone(),
            ));
        }
        shutdown.cancelled().await;
    });
    runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);
}

/// Serves one shard listener until `shutdown` is cancelled, and cancels it if serving fails.
///
/// # Arguments
///
/// * `incoming` - The bound listener.
/// * `listener` - Its TLS acceptor and service.
/// * `http2` - The HTTP/2 settings of the server.
/// * `ip_conncur` - The most concurrent connections per client IP.
/// * `rate_limit_table` - The connection counts shared by every listener.
/// * `shutdown` - Stops serving when cancelled.
async fn serve(
    incoming: TcpIncoming,
    listener: ShardListener,
    http2: Http2Settings,
    ip_conncur: u64,
    rate_limit_table: SharedRateLimitTable,
    shutdown: CancellationToken,
) {
    let callbacks = PrometheusRatelimitCallbacks;
    let result = match listener.tls {
        Some(acceptor) => {
            let incoming = TlsIncoming::new(incoming, acceptor);
            let incoming =
                RateLimitedIncoming::new(incoming, ip_conncur, rate_limit_table, callbacks);
            GrpcService::serve_listener(incoming, http2, listener.routes, shutdown.clone()).await
        }
        None => {
            let incoming =
                RateLimitedIncoming::new(incoming, ip_conncur, rate_limit_table, callbacks);
            GrpcService::serve_listener(incoming, http2, listener.routes, shutdown.clone()).await
        }
    };
    if let Err(error) = result {
        error!("gRPC shard listener failed: {error}");
        shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_shard_that_cannot_bind_fails_the_start_and_stops_the_others() {
        let cores = crate::util::cpu_core_affinity::get_thread_affinity().expect("affinity");
        let shutdown = CancellationToken::new();
        // 192.0.2.1 is a documentation address, so no local interface has it.
        let listener = ShardListener {
            addr: "192.0.2.1:1".parse().expect("address"),
            tls: None,
            routes: tonic::service::Routes::default(),
        };
        let http2 = Http2Settings {
            adaptive_window: None,
            keepalive_interval: None,
            keepalive_timeout: None,
            initial_connection_window_size: None,
            initial_stream_window_size: None,
        };

        let started = spawn_shards(
            &cores[..1],
            vec![listener],
            http2,
            1,
            SharedRateLimitTable::default(),
            shutdown.clone(),
        )
        .await;

        assert!(started.is_err());
        assert!(shutdown.is_cancelled());
    }
}
