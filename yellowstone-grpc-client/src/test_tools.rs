//! Stand-ins for a live server, to test code built on the subscription streams.

use {
    crate::{GeyserGrpcClientError, GrpcConnector, TonicGrpcConnector},
    futures::{
        channel::mpsc,
        stream::{Stream, StreamExt},
    },
    std::{
        collections::VecDeque,
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
        task::Poll,
        time::{Duration, Instant},
    },
    tonic::{codec::Streaming, Status},
    yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeUpdate},
};

pub struct Unstable<S> {
    inner: Option<S>,
    drop_interval: Duration,
    started_at: Instant,
}

impl<S> Stream for Unstable<S>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.started_at.elapsed() >= this.drop_interval {
            log::warn!(
                "unstable: simulated disconnect after {:?}",
                this.drop_interval
            );
            if let Some(stream) = this.inner.take() {
                drop(stream);
            }

            return Poll::Ready(Some(Err(Status::aborted("unstable: simulated disconnect"))));
        }
        if let Some(stream) = &mut this.inner {
            return stream.poll_next_unpin(cx);
        }
        Poll::Ready(None)
    }
}

impl<S> Unstable<S> {
    pub fn new(inner: S, drop_interval: Duration) -> Self {
        Self {
            inner: Some(inner),
            drop_interval,
            started_at: Instant::now(),
        }
    }
}

#[derive(Clone)]
pub struct UnstableConnector {
    inner: TonicGrpcConnector,
    drop_interval: Duration,
}

impl UnstableConnector {
    pub const fn new(inner: TonicGrpcConnector, drop_interval: Duration) -> Self {
        Self {
            inner,
            drop_interval,
        }
    }
}

impl GrpcConnector for UnstableConnector {
    type Stream = Unstable<Streaming<SubscribeUpdate>>;
    type ConnectError = GeyserGrpcClientError;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Self::Stream, Self::ConnectError>> + Send>>;

    fn connect(
        &self,
        request: std::sync::Arc<SubscribeRequest>,
        from_slot: Option<u64>,
    ) -> Self::ConnectFuture {
        let inner_fut = self.inner.connect(request, from_slot);
        let drop_interval = self.drop_interval;

        Box::pin(async move {
            let stream = inner_fut.await?;
            Ok(Unstable::new(stream, drop_interval))
        })
    }
}

/// One call to [`ScriptedConnector::connect`](GrpcConnector::connect).
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectCall {
    /// The request the connection subscribed with.
    pub request: SubscribeRequest,
    /// The `from_slot` the connection replays from.
    pub from_slot: Option<u64>,
}

/// The outcome of the next scripted connect.
enum ScriptedConnection {
    Accept(mpsc::UnboundedReceiver<Result<SubscribeUpdate, Status>>),
    Refuse(Status),
}

/// The script shared by every clone of one [`ScriptedConnector`].
#[derive(Default)]
struct Script {
    connections: VecDeque<ScriptedConnection>,
    calls: Vec<ConnectCall>,
}

/// A [`GrpcConnector`] whose connections the test scripts: each connect takes the next
/// outcome queued with [`ScriptedConnector::accept`] or [`ScriptedConnector::refuse`], and is
/// recorded for [`ScriptedConnector::calls`]. A connect with nothing queued fails with
/// [`Status::unavailable`].
///
/// Clones share the script, so a test keeps one clone and hands the other to the stream.
///
/// # Examples
///
/// ```ignore
/// let connector = ScriptedConnector::default();
/// let server = connector.accept();
/// let mut stream = ReconnectStream::connect(connector.clone(), request, config).await?;
/// server.unbounded_send(Ok(update))?;
/// drop(server); // ends the connection
/// assert_eq!(connector.calls()[0].from_slot, None);
/// ```
#[derive(Clone, Default)]
pub struct ScriptedConnector {
    script: Arc<Mutex<Script>>,
}

impl ScriptedConnector {
    /// Queues a connect that succeeds.
    ///
    /// # Returns
    ///
    /// The server side of the connection: send updates or an error through it, and drop it
    /// to end the connection.
    pub fn accept(&self) -> mpsc::UnboundedSender<Result<SubscribeUpdate, Status>> {
        let (server, client) = mpsc::unbounded();
        self.lock()
            .connections
            .push_back(ScriptedConnection::Accept(client));
        server
    }

    /// Queues a connect that fails.
    ///
    /// # Arguments
    ///
    /// * `status` - The [`Status`] the connect fails with.
    pub fn refuse(&self, status: Status) {
        self.lock()
            .connections
            .push_back(ScriptedConnection::Refuse(status));
    }

    /// Lists every connect so far.
    ///
    /// # Returns
    ///
    /// One [`ConnectCall`] per connect, in call order.
    pub fn calls(&self) -> Vec<ConnectCall> {
        self.lock().calls.clone()
    }

    /// Locks the shared script.
    ///
    /// # Returns
    ///
    /// The guard over the [`Script`].
    ///
    /// # Panics
    ///
    /// Panics if a test thread panicked while holding the lock.
    fn lock(&self) -> std::sync::MutexGuard<'_, Script> {
        self.script
            .lock()
            .expect("scripted connector mutex poisoned")
    }
}

impl GrpcConnector for ScriptedConnector {
    type Stream = mpsc::UnboundedReceiver<Result<SubscribeUpdate, Status>>;
    type ConnectError = GeyserGrpcClientError;
    type ConnectFuture = std::future::Ready<Result<Self::Stream, Self::ConnectError>>;

    fn connect(
        &self,
        request: Arc<SubscribeRequest>,
        from_slot: Option<u64>,
    ) -> Self::ConnectFuture {
        let mut script = self.lock();
        script.calls.push(ConnectCall {
            request: SubscribeRequest::clone(&request),
            from_slot,
        });
        let result = match script.connections.pop_front() {
            Some(ScriptedConnection::Accept(stream)) => Ok(stream),
            Some(ScriptedConnection::Refuse(status)) => Err(status.into()),
            None => Err(Status::unavailable("no scripted connection left").into()),
        };
        std::future::ready(result)
    }
}
