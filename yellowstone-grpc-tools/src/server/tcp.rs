use {
    futures::Stream,
    socket2::TcpKeepalive,
    std::{
        io,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    },
    tokio::net::{TcpListener, TcpStream},
    tokio_stream::wrappers::TcpListenerStream,
};

/// Configuration applied to accepted TCP sockets.
#[derive(Default, Debug)]
pub struct TcpConfiguration {
    /// Optional TCP_NODELAY setting.
    pub nodelay: Option<bool>,
    /// Optional TCP keepalive parameters.
    pub keepalive: Option<TcpKeepalive>,
    /// Optional TCP send buffer size.
    pub so_sndbuf: Option<usize>,
    /// Optional limit of unsent bytes (TCP_NOTSENT_LOWAT).
    pub tcp_notsent_lowat: Option<u32>,
    /// Optional TCP congestion control algorithm, such as "cubic".
    pub tcp_congestion: Option<String>,
}

impl TcpConfiguration {
    /// Sets optional TCP_NODELAY for accepted sockets.
    pub fn with_nodelay(mut self, nodelay: Option<bool>) -> Self {
        self.nodelay = nodelay;
        self
    }

    /// Sets optional TCP keepalive time for accepted sockets.
    pub fn with_keepalive(mut self, keepalive: Option<Duration>) -> Self {
        self.keepalive = keepalive.and_then(|k| make_keepalive(Some(k), None, None));
        self
    }

    /// Sets optional TCP send buffer size for accepted sockets.
    pub fn with_so_sndbuf(mut self, so_sndbuf: Option<usize>) -> Self {
        self.so_sndbuf = so_sndbuf;
        self
    }

    /// Sets optional TCP_NOTSENT_LOWAT for accepted sockets.
    pub fn with_tcp_notsent_lowat(mut self, tcp_notsent_lowat: Option<u32>) -> Self {
        self.tcp_notsent_lowat = tcp_notsent_lowat;
        self
    }

    /// Sets optional TCP congestion control algorithm for accepted sockets.
    pub fn with_tcp_congestion(mut self, tcp_congestion: Option<String>) -> Self {
        self.tcp_congestion = tcp_congestion;
        self
    }
}

/// Stream of accepted TCP connections.
#[derive(Debug)]
pub struct TcpIncoming {
    listener: TcpListenerStream,
    nodelay: Option<bool>,
    keepalive: Option<TcpKeepalive>,
    so_sndbuf: Option<usize>,
    tcp_notsent_lowat: Option<u32>,
    tcp_congestion: Option<String>,
}

impl TcpIncoming {
    /// Binds a new listener with default socket options.
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener: TcpListenerStream::new(listener),
            nodelay: None,
            keepalive: None,
            so_sndbuf: None,
            tcp_notsent_lowat: None,
            tcp_congestion: None,
        })
    }

    /// Binds a new listener with custom accepted-socket options.
    pub async fn bind_with_config(
        addr: impl tokio::net::ToSocketAddrs,
        config: TcpConfiguration,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener: TcpListenerStream::new(listener),
            nodelay: config.nodelay,
            keepalive: config.keepalive,
            so_sndbuf: config.so_sndbuf,
            tcp_notsent_lowat: config.tcp_notsent_lowat,
            tcp_congestion: config.tcp_congestion,
        })
    }

    /// Returns listener local address.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.as_ref().local_addr()
    }
}

impl Stream for TcpIncoming {
    type Item = io::Result<TcpStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = Pin::new(&mut self.listener).poll_next(cx);

        if let Poll::Ready(Some(Ok(stream))) = &polled {
            set_accepted_socket_options(stream, self.as_ref());
        }

        polled
    }
}

// Consistent with hyper-0.14, this function does not return an error.
fn set_accepted_socket_options(stream: &TcpStream, config: Pin<&TcpIncoming>) {
    if let Some(nodelay) = config.nodelay {
        if let Err(e) = stream.set_nodelay(nodelay) {
            log::warn!("error trying to set TCP_NODELAY: {e}");
        }
    }

    if let Some(keepalive) = config.keepalive.as_ref() {
        let sock_ref = socket2::SockRef::from(&stream);
        if let Err(e) = sock_ref.set_tcp_keepalive(keepalive) {
            log::warn!("error trying to set TCP_KEEPALIVE: {e}");
        }
    }

    if let Some(so_sndbuf) = config.so_sndbuf {
        let sock_ref = socket2::SockRef::from(&stream);
        if let Err(e) = sock_ref.set_send_buffer_size(so_sndbuf) {
            log::warn!("error trying to set TCP's SO_SNDBUF: {e}");
        }
    }

    #[cfg(any(target_os = "android", target_os = "linux"))]
    if let Some(lowat) = config.tcp_notsent_lowat {
        let sock_ref = socket2::SockRef::from(&stream);
        if let Err(e) = sock_ref.set_tcp_notsent_lowat(lowat) {
            log::warn!("error trying to set TCP_NOTSENT_LOWAT: {e}");
        }
    }

    #[cfg(any(target_os = "freebsd", target_os = "linux"))]
    if let Some(congestion) = config.tcp_congestion.as_deref() {
        let sock_ref = socket2::SockRef::from(&stream);
        if let Err(e) = sock_ref.set_tcp_congestion(congestion.as_bytes()) {
            log::warn!("error trying to set TCP_CONGESTION to {congestion}: {e}");
        }
    }

    // avoid clippy errors for targets that do not use these fields.
    let _ = &config.tcp_notsent_lowat;
    let _ = &config.tcp_congestion;
}

fn make_keepalive(
    keepalive_time: Option<Duration>,
    keepalive_interval: Option<Duration>,
    keepalive_retries: Option<u32>,
) -> Option<TcpKeepalive> {
    let mut dirty = false;
    let mut keepalive = TcpKeepalive::new();
    if let Some(t) = keepalive_time {
        keepalive = keepalive.with_time(t);
        dirty = true;
    }

    #[cfg(
        // See https://docs.rs/socket2/0.5.8/src/socket2/lib.rs.html#511-525
        any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "visionos",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "windows",
        )
    )]
    if let Some(t) = keepalive_interval {
        keepalive = keepalive.with_interval(t);
        dirty = true;
    }

    #[cfg(
        // See https://docs.rs/socket2/0.5.8/src/socket2/lib.rs.html#557-570
        any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "visionos",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
        )
    )]
    if let Some(r) = keepalive_retries {
        keepalive = keepalive.with_retries(r);
        dirty = true;
    }

    // avoid clippy errors for targets that do not use these fields.
    let _ = keepalive_retries;
    let _ = keepalive_interval;

    dirty.then_some(keepalive)
}

#[cfg(test)]
mod tests {
    use {super::*, tokio::net::TcpStream, tokio_stream::StreamExt};

    #[test]
    fn tcp_configuration_builder_sets_fields() {
        let cfg = TcpConfiguration::default()
            .with_nodelay(Some(true))
            .with_keepalive(Some(Duration::from_secs(5)))
            .with_tcp_notsent_lowat(Some(131_072))
            .with_tcp_congestion(Some("cubic".to_owned()));

        assert_eq!(cfg.nodelay, Some(true));
        assert!(cfg.keepalive.is_some());
        assert_eq!(cfg.tcp_notsent_lowat, Some(131_072));
        assert_eq!(cfg.tcp_congestion.as_deref(), Some("cubic"));
    }

    #[test]
    fn make_keepalive_returns_none_without_inputs() {
        let keepalive = make_keepalive(None, None, None);
        assert!(keepalive.is_none());
    }

    #[tokio::test]
    async fn bind_with_config_applies_nodelay_to_accepted_stream() {
        let mut incoming = TcpIncoming::bind_with_config(
            "127.0.0.1:0",
            TcpConfiguration::default().with_nodelay(Some(true)),
        )
        .await
        .expect("listener should bind");

        let addr = incoming
            .local_addr()
            .expect("listener should expose address");
        let client = tokio::spawn(async move { TcpStream::connect(addr).await });

        let accepted = incoming
            .next()
            .await
            .expect("stream should yield connection")
            .expect("accept should succeed");

        let _client = client
            .await
            .expect("client task should complete")
            .expect("client should connect");
        assert!(accepted.nodelay().expect("reading nodelay should succeed"));
    }

    #[cfg(target_os = "linux")]
    async fn accept_with_config(config: TcpConfiguration) -> (TcpStream, TcpStream) {
        let mut incoming = TcpIncoming::bind_with_config("127.0.0.1:0", config)
            .await
            .expect("listener should bind");
        let addr = incoming
            .local_addr()
            .expect("listener should expose address");
        let client = tokio::spawn(async move { TcpStream::connect(addr).await });

        let accepted = incoming
            .next()
            .await
            .expect("stream should yield connection")
            .expect("accept should succeed");
        let client = client
            .await
            .expect("client task should complete")
            .expect("client should connect");
        (accepted, client)
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn bind_with_config_applies_tcp_options_to_accepted_stream() {
        // reno is built into every kernel and differs from the usual default, cubic.
        let (accepted, _client) = accept_with_config(
            TcpConfiguration::default()
                .with_tcp_notsent_lowat(Some(131_072))
                .with_tcp_congestion(Some("reno".to_owned())),
        )
        .await;

        let sock_ref = socket2::SockRef::from(&accepted);
        let lowat = sock_ref
            .tcp_notsent_lowat()
            .expect("reading TCP_NOTSENT_LOWAT should succeed");
        assert_eq!(lowat, 131_072);
        let congestion = sock_ref
            .tcp_congestion()
            .expect("reading TCP_CONGESTION should succeed");
        assert_eq!(congestion.split(|&b| b == 0).next(), Some(&b"reno"[..]));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn unknown_congestion_algorithm_still_accepts_the_connection() {
        let (accepted, _client) = accept_with_config(
            TcpConfiguration::default().with_tcp_congestion(Some("no-such-algorithm".to_owned())),
        )
        .await;

        assert!(accepted.peer_addr().is_ok());
    }
}
