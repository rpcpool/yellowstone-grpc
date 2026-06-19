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
}

/// Stream of accepted TCP connections.
#[derive(Debug)]
pub struct TcpIncoming {
    listener: TcpListenerStream,
    nodelay: Option<bool>,
    keepalive: Option<TcpKeepalive>,
}

impl TcpIncoming {
    /// Binds a new listener with default socket options.
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener: TcpListenerStream::new(listener),
            nodelay: None,
            keepalive: None,
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
            set_accepted_socket_options(stream, self.nodelay, &self.keepalive);
        }

        polled
    }
}

// Consistent with hyper-0.14, this function does not return an error.
fn set_accepted_socket_options(
    stream: &TcpStream,
    nodelay: Option<bool>,
    keepalive: &Option<TcpKeepalive>,
) {
    if let Some(nodelay) = nodelay {
        if let Err(e) = stream.set_nodelay(nodelay) {
            log::warn!("error trying to set TCP_NODELAY: {e}");
        }
    }

    if let Some(keepalive) = keepalive {
        let sock_ref = socket2::SockRef::from(&stream);
        if let Err(e) = sock_ref.set_tcp_keepalive(keepalive) {
            log::warn!("error trying to set TCP_KEEPALIVE: {e}");
        }
    }
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
            .with_keepalive(Some(Duration::from_secs(5)));

        assert_eq!(cfg.nodelay, Some(true));
        assert!(cfg.keepalive.is_some());
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
}
