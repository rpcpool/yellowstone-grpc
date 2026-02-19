use {
    crate::metrics,
    bytesize::ByteSize,
    futures::{Stream, StreamExt},
    std::{
        any::Any,
        collections::HashMap,
        marker::PhantomData,
        pin::Pin,
        sync::{LazyLock, Mutex},
        task::ready,
    },
    tokio::io::{AsyncRead, AsyncWrite},
    tonic::transport::server::{Connected, TcpConnectInfo, TlsConnectInfo},
};

///
/// This map is a global to track distinct TCP connection PER ip address.
/// Because we are using HTTP2, different HTTP/2 calls/streams re-use the same TCP connection, therefore the number of new
/// TCP connections open/close is far less frequent than the number of HTTP/2 calls/streams, and thus the contention on this map should be low.
/// Moreover, the map lock/unlock does not happen on the critical path of processing an HTTP/2 call/stream, but only when a TCP connection is established or closed, so it should not cause significant performance issues.
///
/// We are using this map to properly reset prometheus metrics.
/// Since we track the traffic per remote ip, when a connection from a remote ip is closed, we want to reset the traffic for that remote ip,
/// but only if there are no more active connections from that remote ip,
/// otherwise we would lose the traffic information for the still active connections from that remote ip.
///
///
static ACTIVE_CONNECTIONS_PER_REMOTE_IP: LazyLock<Mutex<HashMap<String /* Ip address */, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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

/// Increments the number of active TCP connections for the given remote IP.
fn increment_active_connections_for_remote_ip(remote_ip: &str) {
    let mut active_by_ip = ACTIVE_CONNECTIONS_PER_REMOTE_IP
        .lock()
        .expect("ACTIVE_CONNECTIONS_PER_REMOTE_IP mutex poisoned");
    active_by_ip
        .entry(remote_ip.to_owned())
        .and_modify(|active_count| *active_count += 1)
        .or_insert(1);
}

/// Decrements the number of active TCP connections for the given remote IP.
///
/// Returns `true` when this was the last active connection for that IP.
fn decrement_active_connections_for_remote_ip(remote_ip: &str) -> bool {
    let mut active_by_ip = ACTIVE_CONNECTIONS_PER_REMOTE_IP
        .lock()
        .expect("ACTIVE_CONNECTIONS_PER_REMOTE_IP mutex poisoned");
    if let Some(active_count) = active_by_ip.get_mut(remote_ip) {
        if *active_count > 1 {
            *active_count -= 1;
            false
        } else {
            active_by_ip.remove(remote_ip);
            metrics::reset_traffic_sent_per_remote_ip(remote_ip);
            true
        }
    } else {
        true
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

/// Wraps an incoming stream of transport connections and converts each item into [`SpyIO`].
pub struct SpyIncoming<I, IO> {
    incoming: I,
    config: SpyIncomingConfig,
    _io: PhantomData<IO>,
}

pub const DEFAULT_TRAFFIC_REPORTING_THRESHOLD: u64 = 64 * 1024; // 64 KiB

/// Runtime options for [`SpyIncoming`].
pub struct SpyIncomingConfig {
    /// Minimum number of bytes to accumulate before updating traffic metrics.
    pub traffic_reporting_threshold: ByteSize,
}

impl Default for SpyIncomingConfig {
    fn default() -> Self {
        Self {
            traffic_reporting_threshold: ByteSize::b(DEFAULT_TRAFFIC_REPORTING_THRESHOLD),
        }
    }
}

impl<I, IO> SpyIncoming<I, IO>
where
    I: Stream<Item = std::io::Result<IO>> + Unpin,
    IO: AsyncRead + AsyncWrite + Unpin + Connected + Send + 'static,
{
    /// Creates a new [`SpyIncoming`] wrapper.
    pub fn new(incoming: I, config: SpyIncomingConfig) -> Self {
        Self {
            incoming,
            config,
            _io: Default::default(),
        }
    }
}

/// IO wrapper that tracks peer metadata and reports traffic/connection metrics.
pub struct SpyIO<IO> {
    wrappee: IO,
    remote_peer_ip_str: Option<String>,
    unreported_traffic_sent: u64,
    traffic_reporting_threshold: u64,
}

impl<IO> Drop for SpyIO<IO> {
    fn drop(&mut self) {
        self.flush_metrics();
        if let Some(remote_peer_ip_str) = &self.remote_peer_ip_str {
            if decrement_active_connections_for_remote_ip(remote_peer_ip_str) {
                log::debug!("Last connection from remote ip {remote_peer_ip_str} closed, resetting its traffic metrics");
            }
        }
        metrics::connections_total_dec();
    }
}

impl<IO> SpyIO<IO>
where
    IO: Connected,
{
    /// Creates a new [`SpyIO`] and initializes per-peer connection accounting.
    fn new(io: IO, traffic_reporting_threshold: u64) -> Self {
        metrics::connections_total_inc();
        let info = &io.connect_info() as &dyn Any;
        let maybe_remote_peer_addr = extract_remote_peer_addr(info);
        // We create the string once and all its derivatives (e.g. for metrics labels) can clone it cheaply, instead of converting the SocketAddr to String every time.
        let maybe_remote_peer_ip_string = maybe_remote_peer_addr.map(|addr| addr.ip().to_string());
        if let Some(remote_peer_ip_str) = maybe_remote_peer_ip_string.as_deref() {
            increment_active_connections_for_remote_ip(remote_peer_ip_str);
        }
        Self {
            wrappee: io,
            remote_peer_ip_str: maybe_remote_peer_ip_string,
            unreported_traffic_sent: 0,
            traffic_reporting_threshold,
        }
    }
}

impl<IO> Connected for SpyIO<IO>
where
    IO: Connected,
{
    type ConnectInfo = IO::ConnectInfo;

    /// Delegates to the wrapped IO connection info.
    fn connect_info(&self) -> Self::ConnectInfo {
        self.wrappee.connect_info()
    }
}

impl<IO> AsyncRead for SpyIO<IO>
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

impl<IO> SpyIO<IO> {
    /// Flushes accumulated traffic counters into metrics.
    fn flush_metrics(&mut self) {
        metrics::add_total_traffic_sent(self.unreported_traffic_sent);
        if let Some(remote_peer_ip_str) = &self.remote_peer_ip_str {
            metrics::add_traffic_sent_per_remote_ip(
                remote_peer_ip_str,
                self.unreported_traffic_sent,
            );
        }
        self.unreported_traffic_sent = 0;
    }
}

impl<IO> AsyncWrite for SpyIO<IO>
where
    IO: AsyncWrite + Unpin + Connected,
{
    /// Writes to the wrapped IO and updates buffered traffic counters.
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let result = ready!(Pin::new(&mut this.wrappee).poll_write(cx, buf));
        if let Ok(bytes_written) = &result {
            this.unreported_traffic_sent += *bytes_written as u64;
            if this.unreported_traffic_sent >= this.traffic_reporting_threshold {
                this.flush_metrics();
            }
        }
        result.into()
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

impl<I, IO> Stream for SpyIncoming<I, IO>
where
    I: Stream<Item = std::io::Result<IO>> + Unpin,
    IO: AsyncRead + AsyncWrite + Unpin + Connected + Send + 'static,
{
    type Item = std::io::Result<SpyIO<IO>>;
    /// Polls for the next accepted connection and wraps it as [`SpyIO`].
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let reporting_threshold = self.config.traffic_reporting_threshold;
        self.incoming.poll_next_unpin(cx).map(|maybe| {
            maybe.map(|result| result.map(|io| SpyIO::new(io, reporting_threshold.as_u64())))
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{
            io,
            net::SocketAddr,
            task::{Context, Poll, Waker},
        },
    };

    static TEST_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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

    struct MockIoRw {
        read_data: Vec<u8>,
        read_pos: usize,
        written_data: Vec<u8>,
        flush_calls: usize,
        shutdown_calls: usize,
    }

    impl Connected for MockIoRw {
        type ConnectInfo = TcpConnectInfo;

        fn connect_info(&self) -> Self::ConnectInfo {
            TcpConnectInfo {
                local_addr: None,
                remote_addr: None,
            }
        }
    }

    impl AsyncRead for MockIoRw {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let remaining = &self.read_data[self.read_pos..];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            self.read_pos += to_copy;
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for MockIoRw {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.written_data.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.flush_calls += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.shutdown_calls += 1;
            Poll::Ready(Ok(()))
        }
    }

    fn noop_context() -> Context<'static> {
        let waker: &'static Waker = Box::leak(Box::new(Waker::noop().clone()));
        Context::from_waker(waker)
    }

    fn clear_active_connections_map() {
        ACTIVE_CONNECTIONS_PER_REMOTE_IP
            .lock()
            .expect("ACTIVE_CONNECTIONS_PER_REMOTE_IP mutex poisoned")
            .clear();
    }

    #[test]
    fn extract_remote_peer_addr_uses_tcp_connect_info_branch() {
        let remote_addr: SocketAddr = "10.1.2.3:50051".parse().expect("valid socket addr");
        let connect_info = TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(remote_addr),
        };
        let info = &connect_info as &dyn Any;

        let extracted = extract_remote_peer_addr(info);
        assert_eq!(extracted, Some(remote_addr));
    }

    #[test]
    fn extract_remote_peer_addr_uses_tls_connect_info_branch() {
        let remote_addr: SocketAddr = "10.9.8.7:443".parse().expect("valid socket addr");
        let connect_info = TestTlsConnectInfo {
            inner: TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(remote_addr),
            },
        };
        let info = &connect_info as &dyn Any;

        let extracted = extract_remote_peer_addr(info);
        assert_eq!(extracted, Some(remote_addr));
    }

    #[test]
    fn extract_remote_peer_addr_returns_none_for_unknown_connect_info() {
        let connect_info = ();
        let info = &connect_info as &dyn Any;

        let extracted = extract_remote_peer_addr(info);
        assert_eq!(extracted, None);
    }

    #[test]
    fn active_connections_helpers_handle_increment_and_last_drop() {
        let _guard = TEST_MUTEX.lock().expect("test mutex poisoned");
        clear_active_connections_map();

        increment_active_connections_for_remote_ip("10.0.0.1");
        increment_active_connections_for_remote_ip("10.0.0.1");

        let active = ACTIVE_CONNECTIONS_PER_REMOTE_IP
            .lock()
            .expect("ACTIVE_CONNECTIONS_PER_REMOTE_IP mutex poisoned");
        assert_eq!(active.get("10.0.0.1"), Some(&2));
        drop(active);

        assert!(!decrement_active_connections_for_remote_ip("10.0.0.1"));
        let active = ACTIVE_CONNECTIONS_PER_REMOTE_IP
            .lock()
            .expect("ACTIVE_CONNECTIONS_PER_REMOTE_IP mutex poisoned");
        assert_eq!(active.get("10.0.0.1"), Some(&1));
        drop(active);

        assert!(decrement_active_connections_for_remote_ip("10.0.0.1"));
        let active = ACTIVE_CONNECTIONS_PER_REMOTE_IP
            .lock()
            .expect("ACTIVE_CONNECTIONS_PER_REMOTE_IP mutex poisoned");
        assert!(!active.contains_key("10.0.0.1"));
    }

    #[test]
    fn spyio_lifecycle_tracks_connections_per_ip() {
        let _guard = TEST_MUTEX.lock().expect("test mutex poisoned");
        clear_active_connections_map();

        let remote_addr: SocketAddr = "192.168.1.10:5000".parse().expect("valid socket addr");

        let io1 = MockIo {
            remote_addr: Some(remote_addr),
        };
        let io2 = MockIo {
            remote_addr: Some(remote_addr),
        };

        let spy1 = SpyIO::new(io1, 1024);
        let spy2 = SpyIO::new(io2, 1024);

        let active = ACTIVE_CONNECTIONS_PER_REMOTE_IP
            .lock()
            .expect("ACTIVE_CONNECTIONS_PER_REMOTE_IP mutex poisoned");
        assert_eq!(active.get("192.168.1.10"), Some(&2));
        drop(active);

        drop(spy1);
        let active = ACTIVE_CONNECTIONS_PER_REMOTE_IP
            .lock()
            .expect("ACTIVE_CONNECTIONS_PER_REMOTE_IP mutex poisoned");
        assert_eq!(active.get("192.168.1.10"), Some(&1));
        drop(active);

        drop(spy2);
        let active = ACTIVE_CONNECTIONS_PER_REMOTE_IP
            .lock()
            .expect("ACTIVE_CONNECTIONS_PER_REMOTE_IP mutex poisoned");
        assert!(!active.contains_key("192.168.1.10"));
    }

    #[test]
    fn spyio_async_read_delegates_to_wrappee() {
        let mut spy = SpyIO {
            wrappee: MockIoRw {
                read_data: b"hello".to_vec(),
                read_pos: 0,
                written_data: Vec::new(),
                flush_calls: 0,
                shutdown_calls: 0,
            },
            remote_peer_ip_str: None,
            unreported_traffic_sent: 0,
            traffic_reporting_threshold: u64::MAX,
        };

        let mut dst = [0u8; 5];
        let mut read_buf = tokio::io::ReadBuf::new(&mut dst);
        let mut cx = noop_context();
        let poll = Pin::new(&mut spy).poll_read(&mut cx, &mut read_buf);

        assert!(poll.is_ready());
        assert_eq!(read_buf.filled(), b"hello");
    }

    #[test]
    fn spyio_async_write_flush_shutdown_delegate_to_wrappee() {
        let mut spy = SpyIO {
            wrappee: MockIoRw {
                read_data: Vec::new(),
                read_pos: 0,
                written_data: Vec::new(),
                flush_calls: 0,
                shutdown_calls: 0,
            },
            remote_peer_ip_str: None,
            unreported_traffic_sent: 0,
            traffic_reporting_threshold: u64::MAX,
        };

        let mut cx = noop_context();
        let write_poll = Pin::new(&mut spy).poll_write(&mut cx, b"abc");
        assert!(matches!(write_poll, Poll::Ready(Ok(3))));

        let flush_poll = Pin::new(&mut spy).poll_flush(&mut cx);
        assert!(matches!(flush_poll, Poll::Ready(Ok(()))));

        let shutdown_poll = Pin::new(&mut spy).poll_shutdown(&mut cx);
        assert!(matches!(shutdown_poll, Poll::Ready(Ok(()))));

        assert_eq!(spy.wrappee.written_data, b"abc");
        assert_eq!(spy.wrappee.flush_calls, 1);
        assert_eq!(spy.wrappee.shutdown_calls, 1);
    }
}
