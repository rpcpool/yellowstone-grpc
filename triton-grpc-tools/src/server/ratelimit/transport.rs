use {
    futures::{Stream, StreamExt},
    std::{
        any::Any,
        collections::HashMap,
        marker::PhantomData,
        net::{IpAddr, SocketAddr},
        pin::Pin,
        sync::{Arc, LazyLock, Mutex},
    },
    tokio::io::{AsyncRead, AsyncWrite},
    tonic::transport::server::{Connected, TcpConnectInfo, TlsConnectInfo},
};

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
pub struct RateLimitedIncoming<I, IO> {
    incoming: I,
    max_ip_conncur: u64,
    active_conn_map: Arc<Mutex<HashMap<IpAddr, u64>>>,
    _io: PhantomData<IO>,
}

pub const DEFAULT_MAX_IP_CONNCUR: u64 = 400;

/// Runtime options for [`RateLimitedIncoming`].
pub struct RateLimitedIncomingConfig {
    /// maximum concurrent connections per ip
    pub max_ip_conncur: u64,
}

impl Default for RateLimitedIncomingConfig {
    fn default() -> Self {
        Self {
            max_ip_conncur: DEFAULT_MAX_IP_CONNCUR,
        }
    }
}

impl<I, IO> RateLimitedIncoming<I, IO>
where
    I: Stream<Item = std::io::Result<IO>> + Unpin,
    IO: AsyncRead + AsyncWrite + Unpin + Connected + Send + 'static,
{
    /// Creates a new [`RateLimitedIncoming`] wrapper.
    pub fn new(incoming: I, max_ip_conncur: u64) -> Self {
        Self {
            incoming,
            max_ip_conncur: max_ip_conncur,
            _io: Default::default(),
            active_conn_map: Default::default(),
        }
    }
}

/// IO wrapper that tracks peer metadata and reports traffic/connection metrics.
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
    /// Writes to the wrapped IO and updates buffered traffic counters.
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
                .entry(remote_peer_ip.ip())
                .and_modify(|count| {
                    if *count > 0 {
                        *count -= 1;
                    }
                })
                .or_insert(0);

            if new_val == &0 {
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
    fn try_new(
        io: IO,
        max_ip_conncur: u64,
        active_conn_map: Arc<Mutex<HashMap<IpAddr, u64>>>,
    ) -> Result<Self, std::io::Error> {
        let info = &io.connect_info() as &dyn Any;
        let maybe_remote_peer_addr = extract_remote_peer_addr(info);

        if let Some(remote_peer_addr) = &maybe_remote_peer_addr {
            let mut guard = active_conn_map.lock().unwrap();
            let curr_val = guard.get(&remote_peer_addr.ip()).cloned().unwrap_or(0);
            if curr_val >= max_ip_conncur {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::QuotaExceeded,
                    "connection limit exceeded for remote peer ip".to_string(),
                ));
            }
            guard
                .entry(remote_peer_addr.ip())
                .and_modify(|count| *count += 1)
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

impl<I, IO> Stream for RateLimitedIncoming<I, IO>
where
    I: Stream<Item = std::io::Result<IO>> + Unpin,
    IO: AsyncRead + AsyncWrite + Unpin + Connected + Send + 'static,
{
    type Item = std::io::Result<RateLimitedIO<IO>>;
    /// Polls for the next accepted connection and wraps it as [`RateLimitedIO`].
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let max_ip_conncur = self.max_ip_conncur;
        let active_conn_map = Arc::clone(&self.active_conn_map);
        self.incoming.poll_next_unpin(cx).map(|maybe| {
            maybe.map(|result| {
                result.and_then(|io| {
                    RateLimitedIO::try_new(io, max_ip_conncur, Arc::clone(&active_conn_map))
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

        let conn1 = RateLimitedIO::try_new(
            MockIo {
                remote_addr: Some(remote_addr),
            },
            1,
            Arc::clone(&active_conn_map),
        )
        .expect("first connection should be accepted");

        let err = RateLimitedIO::try_new(
            MockIo {
                remote_addr: Some(remote_addr),
            },
            1,
            Arc::clone(&active_conn_map),
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
        )
        .expect("new connection should be accepted after drop");
    }
}
