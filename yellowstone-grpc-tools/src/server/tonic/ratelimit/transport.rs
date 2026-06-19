use {
    futures::{Stream, StreamExt},
    std::{
        any::Any,
        collections::HashMap,
        marker::PhantomData,
        net::{IpAddr, SocketAddr},
        pin::Pin,
        sync::{Arc, Mutex},
    },
    tokio::io::{AsyncRead, AsyncWrite},
    tonic::transport::server::{Connected, TcpConnectInfo, TlsConnectInfo},
};

pub trait RatelimitedCallbacks: Send + Sync + 'static {
    /// Callback invoked when an incoming connection is rejected due to exceeding the per-IP connection limit.
    /// The remote peer IP is provided if it could be extracted from the connection metadata.
    fn on_rate_limit_exceeded(&self, _remote_peer_ip: Option<IpAddr>) {}
}

#[cfg(test)]
#[derive(Clone)]
struct TestTlsConnectInfo<T> {
    inner: T,
}

#[cfg(test)]
impl<T> TestTlsConnectInfo<T> {
    fn get_ref(&self) -> &T {
        &self.inner
    }
}

/// IP Rate-limiting wrapper around an incoming stream of transport connections.
///
/// Wraps an incoming stream of transport connections and converts each item into [`RateLimitedIO`].
pub struct RateLimitedIncoming<I, IO, CB> {
    incoming: I,
    max_ip_conncur: u64,
    callbacks: CB,
    active_conn_map: Arc<Mutex<HashMap<IpAddr, u64>>>,
    _io: PhantomData<IO>,
}

///
/// Shared state for tracking active connections per peer IP across all incoming streams and connections. The key is the peer IP and the value is the number of active connections from that IP.
///
#[derive(Debug, Clone, Default)]
pub struct SharedRateLimitTable {
    inner: Arc<Mutex<HashMap<IpAddr, u64>>>,
}

pub const DEFAULT_MAX_IP_CONNCUR: u64 = 250;

impl<I, IO, CB> RateLimitedIncoming<I, IO, CB>
where
    I: Stream<Item = std::io::Result<IO>> + Unpin,
    IO: AsyncRead + AsyncWrite + Unpin + Connected + Send + 'static,
    CB: RatelimitedCallbacks,
{
    /// Creates a new [`RateLimitedIncoming`] wrapper.
    pub fn new(
        incoming: I,
        max_ip_conncur: u64,
        table: SharedRateLimitTable,
        callbacks: CB,
    ) -> Self {
        Self {
            incoming,
            max_ip_conncur: max_ip_conncur,
            _io: Default::default(),
            active_conn_map: Arc::clone(&table.inner),
            callbacks,
        }
    }

    pub fn with_default_rate_limit(
        incoming: I,
        table: SharedRateLimitTable,
        callbacks: CB,
    ) -> Self {
        Self::new(incoming, DEFAULT_MAX_IP_CONNCUR, table, callbacks)
    }
}

/// IO wrapper that keeps tracks of active connections per peer IP and enforces a maximum concurrent connection limit.
pub struct RateLimitedIO<IO> {
    wrappee: IO,
    remote_peer: Option<SocketAddr>,
    active_conn_map: Arc<Mutex<HashMap<IpAddr, u64>>>,
}

impl<IO> Connected for RateLimitedIO<IO>
where
    IO: Connected,
{
    type ConnectInfo = IO::ConnectInfo;

    /// Delegates to the wrapped IO connection info.
    fn connect_info(&self) -> Self::ConnectInfo {
        self.wrappee.connect_info()
    }
}

impl<IO> AsyncRead for RateLimitedIO<IO>
where
    IO: AsyncRead + Unpin,
{
    /// Reads from the wrapped IO.
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().wrappee).poll_read(cx, buf)
    }
}

impl<IO> AsyncWrite for RateLimitedIO<IO>
where
    IO: AsyncWrite + Unpin,
{
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.wrappee).poll_write(cx, buf)
    }

    /// Flushes the wrapped IO.
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().wrappee).poll_flush(cx)
    }

    /// Shuts down the wrapped IO.
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().wrappee).poll_shutdown(cx)
    }
}

impl<IO> Drop for RateLimitedIO<IO> {
    fn drop(&mut self) {
        if let Some(remote_peer_ip) = &self.remote_peer {
            let mut guard = self.active_conn_map.lock().unwrap();
            let new_val = guard
                .get_mut(&remote_peer_ip.ip())
                .expect("should have an entry for the remote peer IP on drop");
            let new_val = new_val.checked_sub(1).expect("should not underflow");

            if new_val == 0 {
                guard.remove(&remote_peer_ip.ip());
            }
        }
    }
}

/// Extracts the peer socket address from tonic connection metadata.
///
/// Supports plain TCP and TLS-wrapped TCP connect metadata.
fn extract_remote_peer_addr(info: &dyn Any) -> Option<std::net::SocketAddr> {
    let maybe_tcp_info = info.downcast_ref::<TcpConnectInfo>().or_else(|| {
        info.downcast_ref::<TlsConnectInfo<TcpConnectInfo>>()
            .map(|tls_info| tls_info.get_ref())
    });
    #[cfg(test)]
    let maybe_tcp_info = maybe_tcp_info.or_else(|| {
        info.downcast_ref::<TestTlsConnectInfo<TcpConnectInfo>>()
            .map(|tls_info| tls_info.get_ref())
    });
    maybe_tcp_info.and_then(TcpConnectInfo::remote_addr)
}

impl<IO> RateLimitedIO<IO>
where
    IO: Connected,
{
    /// Creates a new [`RateLimitedIO`] and initializes per-peer connection accounting.
    fn try_new<CB>(
        io: IO,
        max_ip_conncur: u64,
        active_conn_map: Arc<Mutex<HashMap<IpAddr, u64>>>,
        callbacks: &CB,
    ) -> Result<Self, std::io::Error>
    where
        CB: RatelimitedCallbacks,
    {
        let info = &io.connect_info() as &dyn Any;
        let maybe_remote_peer_addr = extract_remote_peer_addr(info);

        if let Some(remote_peer_addr) = &maybe_remote_peer_addr {
            let mut guard = active_conn_map.lock().unwrap();
            let curr_val = guard.get(&remote_peer_addr.ip()).cloned().unwrap_or(0);
            if curr_val >= max_ip_conncur {
                callbacks.on_rate_limit_exceeded(Some(remote_peer_addr.ip()));
                return Err(std::io::Error::new(
                    std::io::ErrorKind::QuotaExceeded,
                    "connection limit exceeded for remote peer ip".to_string(),
                ));
            }
            guard
                .entry(remote_peer_addr.ip())
                .and_modify(|count| *count = count.checked_add(1).expect("should not overflow"))
                .or_insert(1);
        }

        // We create the string once and all its derivatives (e.g. for metrics labels) can clone it cheaply, instead of converting the SocketAddr to String every time.
        Ok(Self {
            wrappee: io,
            remote_peer: maybe_remote_peer_addr,
            active_conn_map,
        })
    }
}

impl<I, IO, CB> Stream for RateLimitedIncoming<I, IO, CB>
where
    I: Stream<Item = std::io::Result<IO>> + Unpin,
    IO: AsyncRead + AsyncWrite + Unpin + Connected + Send + 'static,
    CB: RatelimitedCallbacks + Unpin + Send + Sync + 'static,
{
    type Item = std::io::Result<RateLimitedIO<IO>>;
    /// Polls for the next accepted connection and wraps it as [`RateLimitedIO`].
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let max_ip_conncur = self.max_ip_conncur;
        self.incoming.poll_next_unpin(cx).map(|maybe| {
            maybe.map(|result| {
                result.and_then(|io| {
                    RateLimitedIO::try_new(
                        io,
                        max_ip_conncur,
                        Arc::clone(&self.active_conn_map),
                        &self.callbacks,
                    )
                })
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{io, task::Poll},
    };

    #[derive(Default)]
    struct NoopCallbacks;

    impl RatelimitedCallbacks for NoopCallbacks {
        fn on_rate_limit_exceeded(&self, _remote_peer_ip: Option<IpAddr>) {}
    }

    #[derive(Clone)]
    struct MockIo {
        remote_addr: Option<SocketAddr>,
    }

    impl Connected for MockIo {
        type ConnectInfo = TcpConnectInfo;

        fn connect_info(&self) -> Self::ConnectInfo {
            TcpConnectInfo {
                local_addr: None,
                remote_addr: self.remote_addr,
            }
        }
    }

    impl AsyncRead for MockIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for MockIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn rate_limited_io_enforces_per_ip_quota_and_releases_slot_on_drop() {
        let remote_addr: SocketAddr = "192.168.1.42:50051".parse().expect("valid socket addr");
        let active_conn_map = Arc::new(Mutex::new(HashMap::new()));
        let callbacks = NoopCallbacks;

        let conn1 = RateLimitedIO::try_new(
            MockIo {
                remote_addr: Some(remote_addr),
            },
            1,
            Arc::clone(&active_conn_map),
            &callbacks,
        )
        .expect("first connection should be accepted");

        let err = RateLimitedIO::try_new(
            MockIo {
                remote_addr: Some(remote_addr),
            },
            1,
            Arc::clone(&active_conn_map),
            &callbacks,
        )
        .err()
        .expect("second connection should be rejected at max=1");
        assert_eq!(err.kind(), std::io::ErrorKind::QuotaExceeded);

        {
            let guard = active_conn_map.lock().expect("poisoned mutex");
            assert_eq!(guard.get(&remote_addr.ip()), Some(&1));
        }

        drop(conn1);

        {
            let guard = active_conn_map.lock().expect("poisoned mutex");
            assert!(!guard.contains_key(&remote_addr.ip()));
        }

        RateLimitedIO::try_new(
            MockIo {
                remote_addr: Some(remote_addr),
            },
            1,
            Arc::clone(&active_conn_map),
            &callbacks,
        )
        .expect("new connection should be accepted after drop");
    }
}
