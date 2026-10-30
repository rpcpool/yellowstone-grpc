mod reconnect;
mod reconnect_stream;
mod tracked_stream;

pub use {
    crate::{
        reconnect::{
            Backoff, GrpcConnector, ReconnectPolicy, ReconnectRequest, TonicGeyserClientOptions,
            TonicGrpcConnector, RECONNECT_FILTER_KEY,
        },
        reconnect_stream::{Disconnected, ReconnectEvent, ReconnectStream},
        tracked_stream::BankRef,
    },
    tonic::{service::Interceptor, transport::ClientTlsConfig},
};
use {
    bytes::Bytes,
    futures::{
        channel::mpsc,
        sink::{Sink, SinkExt},
        stream::Stream,
    },
    std::{
        path::PathBuf,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::Duration,
    },
    tokio::net::UnixStream,
    tonic::{
        codec::{CompressionEncoding, Streaming},
        metadata::{errors::InvalidMetadataValue, AsciiMetadataValue, MetadataValue},
        service::interceptor::InterceptedService,
        transport::{
            channel::{Channel, Endpoint},
            Uri,
        },
        Request, Response, Status,
    },
    tonic_health::pb::{health_client::HealthClient, HealthCheckRequest, HealthCheckResponse},
    yellowstone_grpc_proto::prelude::{
        geyser_client::GeyserClient, CommitmentLevel, GetBlockHeightRequest,
        GetBlockHeightResponse, GetLatestBlockhashRequest, GetLatestBlockhashResponse,
        GetSlotRequest, GetSlotResponse, GetVersionRequest, GetVersionResponse,
        IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest, PongResponse,
        SubscribeDeshredRequest, SubscribeGossipRequest, SubscribeReplayInfoRequest,
        SubscribeReplayInfoResponse, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
        SubscribeUpdateGossip,
    },
};
#[cfg(feature = "test-tools")]
use {hyper::rt, tower::Service};

#[cfg(any(test, feature = "test-tools"))]
pub mod test_tools;

#[derive(Debug, Clone)]
pub struct InterceptorXToken {
    pub x_token: Option<AsciiMetadataValue>,
    pub x_request_snapshot: bool,
}

impl Interceptor for InterceptorXToken {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(x_token) = self.x_token.clone() {
            request.metadata_mut().insert("x-token", x_token);
        }
        if self.x_request_snapshot {
            request
                .metadata_mut()
                .insert("x-request-snapshot", MetadataValue::from_static("true"));
        }
        Ok(request)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserGrpcClientError {
    #[error("gRPC status: {0}")]
    TonicStatus(#[from] Status),
    #[error("gRPC transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
}

pub type GeyserGrpcClientResult<T> = Result<T, GeyserGrpcClientError>;

/// Configuration of a [`ReconnectStream`]. Plain subscriptions never reconnect.
#[derive(Clone, Debug, Default)]
pub struct ReconnectConfig {
    /// The retry budget: up to [`Backoff::max_retries`] reconnects in a row, each after a
    /// delay that starts at [`Backoff::initial_interval`] and grows by
    /// [`Backoff::multiplier`]. A connection that delivers a bank's Block, footer or block meta
    /// refills it. See [`ReconnectStream`]'s "Retry budget" section.
    pub backoff: Backoff,

    /// Where a reconnect resumes: at the live tip, or replaying from the last confirmed slot.
    pub policy: ReconnectPolicy,
}

impl ReconnectConfig {
    /// Sets the retry schedule.
    ///
    /// # Arguments
    ///
    /// * `backoff` - The [`Backoff`] for reconnects.
    ///
    /// # Returns
    ///
    /// This [`ReconnectConfig`] with `backoff`.
    pub const fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Sets where a reconnect resumes.
    ///
    /// # Arguments
    ///
    /// * `policy` - The [`ReconnectPolicy`] for reconnects.
    ///
    /// # Returns
    ///
    /// This [`ReconnectConfig`] with `policy`.
    pub const fn with_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.policy = policy;
        self
    }
}

///
/// See [`GeyserGrpcBuilder`] for constructing a client with custom options.
///
#[derive(Clone)]
pub struct GeyserGrpcClient {
    pub health: HealthClient<InterceptedService<Channel, InterceptorXToken>>,
    pub geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>,
    reconnect_config: Option<ReconnectConfig>,
    geyser_client_opts: TonicGeyserClientOptions,
    reconnect_endpoint: Option<Endpoint>,
    reconnect_x_token: Option<AsciiMetadataValue>,
}

impl GeyserGrpcClient {
    pub fn build_from_shared(
        endpoint: impl Into<Bytes>,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcBuilder> {
        Ok(GeyserGrpcBuilder::new(Endpoint::from_shared(endpoint)?))
    }

    pub fn build_from_static(endpoint: &'static str) -> GeyserGrpcBuilder {
        GeyserGrpcBuilder::new(Endpoint::from_static(endpoint))
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
///
/// Errors returns by the [`SubscribeDeshredRequestSink`] when sending subscription updates to the server.
///
pub struct SubscribeDeshredRequestSinkError(#[from] mpsc::SendError);

///
/// Sinks returned by the [`GeyserGrpcClient::subscribe_deshred`].
///
/// The sink is used to send [`SubscribeDeshredRequest`] updates to the server.
///
#[derive(Clone)]
pub struct SubscribeDeshredRequestSink {
    inner: mpsc::UnboundedSender<SubscribeDeshredRequest>,
}

#[cfg(feature = "test-tools")]
impl SubscribeDeshredRequestSink {
    /// Build a `SubscribeDeshredRequestSink` backed by an in-memory channel
    /// instead of a live gRPC connection, for tests that need a real
    /// `SubscribeDeshredRequestSink` value.
    pub const fn mock(sender: mpsc::UnboundedSender<SubscribeDeshredRequest>) -> Self {
        Self { inner: sender }
    }
}

impl Sink<SubscribeDeshredRequest> for SubscribeDeshredRequestSink {
    type Error = SubscribeDeshredRequestSinkError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: SubscribeDeshredRequest,
    ) -> Result<(), Self::Error> {
        self.inner.start_send_unpin(item).map_err(Into::into)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_flush_unpin(cx).map_err(Into::into)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_close_unpin(cx).map_err(Into::into)
    }
}

///
/// Streams returned by the [`GeyserGrpcClient::subscribe`].
///
/// The stream yields [`SubscribeUpdate`] from the server.
///
pub struct GeyserStream {
    inner: InnerStream,
}

#[allow(
    clippy::large_enum_variant,
    reason = "Keep the live transport inline; the smaller variant is only used by test tools"
)]
enum InnerStream {
    Live(Streaming<SubscribeUpdate>),
    #[cfg(feature = "test-tools")]
    MockSource(tokio::sync::mpsc::Receiver<Result<SubscribeUpdate, Status>>),
}

#[cfg(feature = "test-tools")]
impl GeyserStream {
    /// Build a `GeyserStream` backed by an in-memory channel instead of a live
    /// gRPC connection, for tests that need a real `GeyserStream` value.
    pub const fn mock(
        receiver: tokio::sync::mpsc::Receiver<Result<SubscribeUpdate, Status>>,
    ) -> Self {
        Self {
            inner: InnerStream::MockSource(receiver),
        }
    }
}

/// Streams returned by the [`GeyserGrpcClient::subscribe_gossip`].
///
/// The stream yields [`SubscribeUpdateGossip`] from the server.
///
/// Wrapping the transport keeps it out of the public signature, so it can change without a
/// breaking release.
pub struct GeyserGossipStream {
    inner: Streaming<SubscribeUpdateGossip>,
}

impl GeyserGossipStream {
    pub const fn new(inner: Streaming<SubscribeUpdateGossip>) -> Self {
        Self { inner }
    }
}

impl Stream for GeyserGossipStream {
    type Item = Result<SubscribeUpdateGossip, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

///
/// Streams returned by the [`GeyserGrpcClient::subscribe_deshred`].
///
/// The stream yields [`SubscribeUpdateDeshred`] from the server.
///
pub struct SubscribeDeshredStream {
    inner: DeshredInnerStream,
}

#[allow(clippy::large_enum_variant)]
enum DeshredInnerStream {
    Live(Streaming<SubscribeUpdateDeshred>),
    #[cfg(feature = "test-tools")]
    MockSource(tokio::sync::mpsc::Receiver<Result<SubscribeUpdateDeshred, Status>>),
}

impl Stream for SubscribeDeshredStream {
    type Item = Result<SubscribeUpdateDeshred, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match &mut self.inner {
            DeshredInnerStream::Live(stream) => std::pin::Pin::new(stream).poll_next(cx),
            #[cfg(feature = "test-tools")]
            DeshredInnerStream::MockSource(rx) => rx.poll_recv(cx),
        }
    }
}

#[cfg(feature = "test-tools")]
impl SubscribeDeshredStream {
    /// Build a `SubscribeDeshredStream` backed by an in-memory channel instead
    /// of a live gRPC connection, for tests that need a real
    /// `SubscribeDeshredStream` value.
    pub const fn mock(
        receiver: tokio::sync::mpsc::Receiver<Result<SubscribeUpdateDeshred, Status>>,
    ) -> Self {
        Self {
            inner: DeshredInnerStream::MockSource(receiver),
        }
    }
}

impl Stream for GeyserStream {
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.inner {
            InnerStream::Live(stream) => Pin::new(stream).poll_next(cx),
            #[cfg(feature = "test-tools")]
            InnerStream::MockSource(rx) => rx.poll_recv(cx),
        }
    }
}

///
/// A sink returned by the [`GeyserGrpcClient::subscribe`].
///
/// The sink is used to send [`SubscribeRequest`] updates to the server.
///
#[derive(Clone)]
pub struct SubscribeRequestSink {
    target: SinkTarget,
}

/// Where a [`SubscribeRequestSink`] sends.
#[derive(Clone)]
enum SinkTarget {
    /// A plain subscription's request channel.
    Plain(Arc<Mutex<mpsc::Sender<SubscribeRequest>>>),
    /// A [`ReconnectStream`]'s request: each request gets the reconnect filters, goes to the
    /// live connection, and is what the next reconnect subscribes with.
    Reconnect(ReconnectRequest),
}

impl SinkTarget {
    /// Locks a plain subscription's request channel.
    ///
    /// # Arguments
    ///
    /// * `sender` - The shared channel.
    ///
    /// # Returns
    ///
    /// The guard over the channel.
    ///
    /// # Panics
    ///
    /// Panics if a thread panicked while holding the lock.
    fn lock(
        sender: &Mutex<mpsc::Sender<SubscribeRequest>>,
    ) -> std::sync::MutexGuard<'_, mpsc::Sender<SubscribeRequest>> {
        sender
            .lock()
            .expect("subscribe request sink mutex poisoned")
    }
}

#[cfg(feature = "test-tools")]
impl SubscribeRequestSink {
    /// Build a `SubscribeRequestSink` backed by an in-memory channel instead
    /// of a live gRPC connection, for tests that need a real
    /// `SubscribeRequestSink` value.
    pub fn mock(sender: mpsc::Sender<SubscribeRequest>) -> Self {
        Self {
            target: SinkTarget::Plain(Arc::new(Mutex::new(sender))),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{inner}")]
pub struct SubscribeRequestSinkError {
    inner: SubscribeRequestSinkErrorKind,
}

#[derive(Debug, thiserror::Error)]
enum SubscribeRequestSinkErrorKind {
    #[error(transparent)]
    Send(mpsc::SendError),
}

impl From<mpsc::SendError> for SubscribeRequestSinkError {
    fn from(err: mpsc::SendError) -> Self {
        Self {
            inner: SubscribeRequestSinkErrorKind::Send(err),
        }
    }
}

impl Sink<SubscribeRequest> for SubscribeRequestSink {
    type Error = SubscribeRequestSinkError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match &self.target {
            SinkTarget::Plain(sender) => SinkTarget::lock(sender).poll_ready(cx),
            SinkTarget::Reconnect(request) => request.poll_ready(cx),
        }
        .map_err(Into::into)
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: SubscribeRequest,
    ) -> Result<(), Self::Error> {
        match &self.target {
            SinkTarget::Plain(sender) => SinkTarget::lock(sender).start_send_unpin(item),
            SinkTarget::Reconnect(request) => request.send(item),
        }
        .map_err(Into::into)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match &self.target {
            SinkTarget::Plain(sender) => SinkTarget::lock(sender).poll_flush_unpin(cx),
            SinkTarget::Reconnect(request) => request.poll_flush_or_close(cx, false),
        }
        .map_err(Into::into)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match &self.target {
            SinkTarget::Plain(sender) => SinkTarget::lock(sender).poll_close_unpin(cx),
            SinkTarget::Reconnect(request) => request.poll_flush_or_close(cx, true),
        }
        .map_err(Into::into)
    }
}

impl GeyserGrpcClient {
    pub const fn new(
        health: HealthClient<InterceptedService<Channel, InterceptorXToken>>,
        geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>,
    ) -> Self {
        Self {
            health,
            geyser,
            reconnect_config: None,
            reconnect_endpoint: None,
            reconnect_x_token: None,
            geyser_client_opts: TonicGeyserClientOptions {
                x_request_snapshot: false,
                send_compressed: None,
                accept_compressed: None,
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            },
        }
    }

    // Health
    pub async fn health_check(&mut self) -> GeyserGrpcClientResult<HealthCheckResponse> {
        let request = HealthCheckRequest {
            service: "geyser.Geyser".to_owned(),
        };
        let response = self.health.check(request).await?;
        Ok(response.into_inner())
    }

    pub async fn health_watch(
        &mut self,
    ) -> GeyserGrpcClientResult<impl Stream<Item = Result<HealthCheckResponse, Status>>> {
        let request = HealthCheckRequest {
            service: "geyser.Geyser".to_owned(),
        };
        let response = self.health.watch(request).await?;
        Ok(response.into_inner())
    }

    // Subscribe
    pub async fn subscribe(
        &mut self,
    ) -> GeyserGrpcClientResult<(SubscribeRequestSink, GeyserStream)> {
        self.subscribe_with_request(None).await
    }

    /// Opens a subscription on this client's channel.
    ///
    /// # Arguments
    ///
    /// * `request` - The first [`SubscribeRequest`].
    ///
    /// # Returns
    ///
    /// The subscription's request channel and its updates.
    ///
    /// # Errors
    ///
    /// Returns the server's error if the subscribe fails.
    async fn open_subscription(
        &mut self,
        request: SubscribeRequest,
    ) -> GeyserGrpcClientResult<(mpsc::Sender<SubscribeRequest>, Streaming<SubscribeUpdate>)> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::channel(1000);
        subscribe_tx
            .send(request)
            .await
            .expect("channel cannot be disconnected or full at this point");
        let response: Response<Streaming<SubscribeUpdate>> =
            self.geyser.subscribe(subscribe_rx).await?;
        Ok((subscribe_tx, response.into_inner()))
    }

    pub(crate) async fn subscribe_raw(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(SubscribeRequestSink, Streaming<SubscribeUpdate>)> {
        let (subscribe_tx, stream) = self.open_subscription(request.unwrap_or_default()).await?;
        let sink = SubscribeRequestSink {
            target: SinkTarget::Plain(Arc::new(Mutex::new(subscribe_tx))),
        };
        Ok((sink, stream))
    }

    pub async fn subscribe_with_request(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(SubscribeRequestSink, GeyserStream)> {
        let (sink, stream) = self.subscribe_raw(request).await?;
        Ok((
            sink,
            GeyserStream {
                inner: InnerStream::Live(stream),
            },
        ))
    }

    /// Subscribes with a stream that reconnects after a recoverable failure.
    ///
    /// The first connection uses this client's channel; reconnects open a new one to the
    /// builder's endpoint, configured by the builder's [`ReconnectConfig`] (or its default).
    /// See [`ReconnectStream`] for what the stream yields across a reconnect.
    ///
    /// # Arguments
    ///
    /// * `request` - The first [`SubscribeRequest`]. Its `from_slot`, if any, is where the
    ///   first connection replays from: a `from_slot` sent later on the sink is ignored by the
    ///   server. No filter may be named [`RECONNECT_FILTER_KEY`].
    ///
    /// # Returns
    ///
    /// The [`SubscribeRequestSink`] to change filters with, and the [`ReconnectStream`]. A
    /// request sent on the sink applies to the current connection and to every reconnect.
    ///
    /// # Errors
    ///
    /// Returns an error if the client requests a startup snapshot, which the server never
    /// replays from, or if the first subscribe fails.
    pub async fn subscribe_with_reconnect(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(SubscribeRequestSink, ReconnectStream)> {
        if self.geyser_client_opts.x_request_snapshot {
            return Err(Status::failed_precondition(
                "a reconnecting subscription cannot request a startup snapshot",
            )
            .into());
        }
        let config = self.reconnect_config.clone().unwrap_or_default();
        let request = ReconnectRequest::new(request.unwrap_or_default());
        let first = request.load();
        let (subscribe_tx, stream) = self
            .open_subscription(SubscribeRequest::clone(&first))
            .await?;
        request.install_sender(subscribe_tx, &first);
        let connector = TonicGrpcConnector::new(
            self.reconnect_endpoint
                .clone()
                .unwrap_or_else(|| Endpoint::from_static("http://127.0.0.1:0")),
            config.clone(),
            self.reconnect_x_token.clone(),
            self.geyser_client_opts.clone(),
            request.clone(),
        );
        let sink = SubscribeRequestSink {
            target: SinkTarget::Reconnect(request.clone()),
        };
        Ok((
            sink,
            ReconnectStream::open(stream, connector, request, config),
        ))
    }

    pub async fn subscribe_once(
        &mut self,
        request: SubscribeRequest,
    ) -> GeyserGrpcClientResult<GeyserStream> {
        let (_sink, stream) = self.subscribe_with_request(Some(request)).await?;
        Ok(stream)
    }

    // Subscribe Deshred
    pub async fn subscribe_deshred(
        &mut self,
    ) -> GeyserGrpcClientResult<(SubscribeDeshredRequestSink, SubscribeDeshredStream)> {
        self.subscribe_deshred_with_request(None).await
    }

    pub async fn subscribe_deshred_with_request(
        &mut self,
        request: Option<SubscribeDeshredRequest>,
    ) -> GeyserGrpcClientResult<(SubscribeDeshredRequestSink, SubscribeDeshredStream)> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        if let Some(request) = request {
            subscribe_tx
                .send(request)
                .await
                .expect("channel cannot be disconnected or full at this point");
        }
        let response: Response<Streaming<SubscribeUpdateDeshred>> =
            self.geyser.subscribe_deshred(subscribe_rx).await?;
        Ok((
            SubscribeDeshredRequestSink {
                inner: subscribe_tx,
            },
            SubscribeDeshredStream {
                inner: DeshredInnerStream::Live(response.into_inner()),
            },
        ))
    }

    pub async fn subscribe_deshred_once(
        &mut self,
        request: SubscribeDeshredRequest,
    ) -> GeyserGrpcClientResult<SubscribeDeshredStream> {
        self.subscribe_deshred_with_request(Some(request))
            .await
            .map(|(_sink, stream)| stream)
    }

    pub async fn subscribe_gossip(&mut self) -> GeyserGrpcClientResult<GeyserGossipStream> {
        let request = tonic::Request::new(SubscribeGossipRequest {});
        let response = self.geyser.subscribe_gossip(request).await?;
        Ok(GeyserGossipStream::new(response.into_inner()))
    }

    // RPC calls
    pub async fn subscribe_replay_info(
        &mut self,
    ) -> GeyserGrpcClientResult<SubscribeReplayInfoResponse> {
        let message = SubscribeReplayInfoRequest {};
        let request = tonic::Request::new(message);
        let response = self.geyser.subscribe_replay_info(request).await?;
        Ok(response.into_inner())
    }

    pub async fn ping(&mut self, count: i32) -> GeyserGrpcClientResult<PongResponse> {
        let message = PingRequest { count };
        let request = tonic::Request::new(message);
        let response = self.geyser.ping(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_latest_blockhash(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> GeyserGrpcClientResult<GetLatestBlockhashResponse> {
        let request = tonic::Request::new(GetLatestBlockhashRequest {
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.get_latest_blockhash(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_block_height(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> GeyserGrpcClientResult<GetBlockHeightResponse> {
        let request = tonic::Request::new(GetBlockHeightRequest {
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.get_block_height(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_slot(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> GeyserGrpcClientResult<GetSlotResponse> {
        let request = tonic::Request::new(GetSlotRequest {
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.get_slot(request).await?;
        Ok(response.into_inner())
    }

    pub async fn is_blockhash_valid(
        &mut self,
        blockhash: String,
        commitment: Option<CommitmentLevel>,
    ) -> GeyserGrpcClientResult<IsBlockhashValidResponse> {
        let request = tonic::Request::new(IsBlockhashValidRequest {
            blockhash,
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.is_blockhash_valid(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_version(&mut self) -> GeyserGrpcClientResult<GetVersionResponse> {
        let request = tonic::Request::new(GetVersionRequest {});
        let response = self.geyser.get_version(request).await?;
        Ok(response.into_inner())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserGrpcBuilderError {
    #[error("Failed to parse x-token: {0}")]
    MetadataValueError(#[from] InvalidMetadataValue),
    #[error("gRPC transport error: {0}")]
    TonicError(#[from] tonic::transport::Error),
}

pub type GeyserGrpcBuilderResult<T> = Result<T, GeyserGrpcBuilderError>;

///
/// The builder for constructing a [`GeyserGrpcClient`] with custom options.
///
/// The builder provides a fluent API to configure both the gRPC transport options and the Geyser client options.
/// For transport options, it exposes the similar configuration as [`Endpoint`] builder since it is used to construct.
///
/// Use [`GeyserGrpcBuilder::connect`] or [`GeyserGrpcBuilder::connect_lazy`] to create a [`GeyserGrpcClient`] from configured builder.
///
#[derive(Debug)]
pub struct GeyserGrpcBuilder {
    pub endpoint: Endpoint,
    pub x_token: Option<AsciiMetadataValue>,
    pub x_request_snapshot: bool,
    pub send_compressed: Option<CompressionEncoding>,
    pub accept_compressed: Option<CompressionEncoding>,
    pub max_decoding_message_size: Option<usize>,
    pub max_encoding_message_size: Option<usize>,
    pub reconnect_config: Option<ReconnectConfig>,
}

impl GeyserGrpcBuilder {
    // Create new builder
    const fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            x_token: None,
            x_request_snapshot: false,
            send_compressed: None,
            accept_compressed: None,
            max_decoding_message_size: None,
            max_encoding_message_size: None,
            reconnect_config: None,
        }
    }

    pub fn from_shared(endpoint: impl Into<Bytes>) -> GeyserGrpcBuilderResult<Self> {
        Ok(Self::new(
            Endpoint::from_shared(endpoint)?.http2_adaptive_window(true),
        ))
    }

    pub fn from_static(endpoint: &'static str) -> Self {
        Self::new(Endpoint::from_static(endpoint).http2_adaptive_window(true))
    }

    // Create client
    fn build(self, channel: Channel) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let reconnect_x_token = self.x_token.clone();
        let geyser_client_opts = TonicGeyserClientOptions {
            x_request_snapshot: self.x_request_snapshot,
            send_compressed: self.send_compressed,
            accept_compressed: self.accept_compressed,
            max_decoding_message_size: self.max_decoding_message_size,
            max_encoding_message_size: self.max_encoding_message_size,
        };
        let interceptor = InterceptorXToken {
            x_token: self.x_token,
            x_request_snapshot: self.x_request_snapshot,
        };

        let mut geyser = GeyserClient::with_interceptor(channel.clone(), interceptor.clone());
        if let Some(encoding) = self.send_compressed {
            geyser = geyser.send_compressed(encoding);
        }
        if let Some(encoding) = self.accept_compressed {
            geyser = geyser.accept_compressed(encoding);
        }
        if let Some(limit) = self.max_decoding_message_size {
            geyser = geyser.max_decoding_message_size(limit);
        }
        if let Some(limit) = self.max_encoding_message_size {
            geyser = geyser.max_encoding_message_size(limit);
        }

        Ok(GeyserGrpcClient {
            health: HealthClient::with_interceptor(channel, interceptor),
            geyser,
            reconnect_config: self.reconnect_config,
            reconnect_endpoint: Some(self.endpoint),
            reconnect_x_token,
            geyser_client_opts,
        })
    }

    pub async fn connect(self) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let channel = self.endpoint.connect().await?;
        self.build(channel)
    }

    pub fn connect_lazy(self) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let channel = self.endpoint.connect_lazy();
        self.build(channel)
    }

    #[cfg(feature = "test-tools")]
    pub async fn connect_with_connector<C>(
        self,
        connector: C,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcClient>
    where
        C: Service<Uri> + Send + 'static,
        C::Response: rt::Read + rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        let channel = self.endpoint.connect_with_connector(connector).await?;
        self.build(channel)
    }

    #[cfg(feature = "test-tools")]
    pub fn connect_with_connector_lazy<C>(
        self,
        connector: C,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcClient>
    where
        C: Service<Uri> + Send + 'static,
        C::Response: rt::Read + rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        let channel = self.endpoint.connect_with_connector_lazy(connector);
        self.build(channel)
    }

    /// Connect to a gRPC server over a Unix Domain Socket.
    ///
    /// The `path` is the filesystem path to the socket (e.g. "/tmp/yellowstone.sock").
    /// tonic requires a dummy HTTP URI for the channel, but the actual transport
    /// goes through the UDS connector.
    pub async fn connect_uds(
        self,
        path: impl Into<PathBuf>,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let path = path.into();

        // tonic needs an Endpoint to hang config off of, but the URI is ignored
        // by the connector — all traffic goes through the UnixStream.
        let channel = Endpoint::from_static("http://[::]:0")
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    let stream = UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await?;

        self.build(channel)
    }

    pub fn x_token<T>(self, x_token: Option<T>) -> GeyserGrpcBuilderResult<Self>
    where
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        Ok(Self {
            x_token: x_token.map(|x_token| x_token.try_into()).transpose()?,
            ..self
        })
    }

    pub fn set_x_request_snapshot(self, value: bool) -> Self {
        Self {
            x_request_snapshot: value,
            ..self
        }
    }

    pub fn connect_timeout(self, dur: Duration) -> Self {
        Self {
            endpoint: self.endpoint.connect_timeout(dur),
            ..self
        }
    }

    pub fn buffer_size(self, sz: impl Into<Option<usize>>) -> Self {
        Self {
            endpoint: self.endpoint.buffer_size(sz),
            ..self
        }
    }

    pub fn http2_adaptive_window(self, enabled: bool) -> Self {
        Self {
            endpoint: self.endpoint.http2_adaptive_window(enabled),
            ..self
        }
    }

    pub fn http2_keep_alive_interval(self, interval: Duration) -> Self {
        Self {
            endpoint: self.endpoint.http2_keep_alive_interval(interval),
            ..self
        }
    }

    pub fn initial_connection_window_size(self, sz: impl Into<Option<u32>>) -> Self {
        Self {
            endpoint: self.endpoint.initial_connection_window_size(sz),
            ..self
        }
    }

    pub fn initial_stream_window_size(self, sz: impl Into<Option<u32>>) -> Self {
        Self {
            endpoint: self.endpoint.initial_stream_window_size(sz),
            ..self
        }
    }

    pub fn keep_alive_timeout(self, duration: Duration) -> Self {
        Self {
            endpoint: self.endpoint.keep_alive_timeout(duration),
            ..self
        }
    }

    pub fn keep_alive_while_idle(self, enabled: bool) -> Self {
        Self {
            endpoint: self.endpoint.keep_alive_while_idle(enabled),
            ..self
        }
    }

    pub fn tcp_keepalive(self, tcp_keepalive: Option<Duration>) -> Self {
        Self {
            endpoint: self.endpoint.tcp_keepalive(tcp_keepalive),
            ..self
        }
    }

    pub fn tcp_nodelay(self, enabled: bool) -> Self {
        Self {
            endpoint: self.endpoint.tcp_nodelay(enabled),
            ..self
        }
    }

    pub fn timeout(self, dur: Duration) -> Self {
        Self {
            endpoint: self.endpoint.timeout(dur),
            ..self
        }
    }

    pub fn tls_config(self, tls_config: ClientTlsConfig) -> GeyserGrpcBuilderResult<Self> {
        Ok(Self {
            endpoint: self.endpoint.tls_config(tls_config)?,
            ..self
        })
    }

    // Geyser options
    pub fn send_compressed(self, encoding: CompressionEncoding) -> Self {
        Self {
            send_compressed: Some(encoding),
            ..self
        }
    }

    pub fn accept_compressed(self, encoding: CompressionEncoding) -> Self {
        Self {
            accept_compressed: Some(encoding),
            ..self
        }
    }

    pub fn max_decoding_message_size(self, limit: usize) -> Self {
        Self {
            max_decoding_message_size: Some(limit),
            ..self
        }
    }

    pub fn max_encoding_message_size(self, limit: usize) -> Self {
        Self {
            max_encoding_message_size: Some(limit),
            ..self
        }
    }

    /// Configure retries for `subscribe_with_reconnect`; ordinary subscriptions are unaffected.
    pub fn set_reconnect_config(self, config: ReconnectConfig) -> Self {
        Self {
            reconnect_config: Some(config),
            ..self
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            GeyserGrpcClient, GeyserGrpcClientError, ReconnectRequest, SinkTarget,
            SubscribeRequest, SubscribeRequestSink, RECONNECT_FILTER_KEY,
        },
        futures::{channel::mpsc, FutureExt, SinkExt, StreamExt},
        std::sync::{Arc, Mutex},
        yellowstone_grpc_proto::prelude::SubscribeRequestPing,
    };

    #[tokio::test]
    async fn test_channel_https_success() {
        let endpoint = "https://ams17.rpcpool.com:443";
        let x_token = "1000000000000000000000000007";

        let res = GeyserGrpcClient::build_from_shared(endpoint);
        assert!(res.is_ok());

        let res = res.unwrap().x_token(Some(x_token));
        assert!(res.is_ok());

        let res = res.unwrap().connect_lazy();
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_channel_http_success() {
        let endpoint = "http://127.0.0.1:10000";
        let x_token = "1234567891012141618202224268";

        let res = GeyserGrpcClient::build_from_shared(endpoint);
        assert!(res.is_ok());

        let res = res.unwrap().x_token(Some(x_token));
        assert!(res.is_ok());

        let res = res.unwrap().connect_lazy();
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_channel_empty_token_some() {
        let endpoint = "http://127.0.0.1:10000";
        let x_token = "";

        let res = GeyserGrpcClient::build_from_shared(endpoint);
        assert!(res.is_ok());

        let res = res.unwrap().x_token(Some(x_token));
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_channel_invalid_token_none() {
        let endpoint = "http://127.0.0.1:10000";

        let res = GeyserGrpcClient::build_from_shared(endpoint);
        assert!(res.is_ok());

        let res = res.unwrap().x_token::<String>(None);
        assert!(res.is_ok());

        let res = res.unwrap().connect_lazy();
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_channel_invalid_uri() {
        let endpoint = "sites/files/images/picture.png";

        let res = GeyserGrpcClient::build_from_shared(endpoint);
        assert_eq!(
            format!("{:?}", res),
            "Err(TonicError(tonic::transport::Error(InvalidUri, InvalidUri(InvalidFormat))))"
                .to_owned()
        );
    }

    fn plain_sink(sender: mpsc::Sender<SubscribeRequest>) -> SubscribeRequestSink {
        SubscribeRequestSink {
            target: SinkTarget::Plain(Arc::new(Mutex::new(sender))),
        }
    }

    /// A reconnecting subscription's sink, with `sender` as its live connection.
    fn reconnect_sink(
        sender: mpsc::Sender<SubscribeRequest>,
    ) -> (SubscribeRequestSink, ReconnectRequest) {
        let request = ReconnectRequest::new(SubscribeRequest::default());
        request.install_sender(sender, &request.load());
        let sink = SubscribeRequestSink {
            target: SinkTarget::Reconnect(request.clone()),
        };
        (sink, request)
    }

    #[tokio::test]
    async fn test_subscribe_request_sink_uses_swapped_sender() {
        let (tx1, mut rx1) = mpsc::channel(8);
        let (tx2, mut rx2) = mpsc::channel(8);
        let (mut sink, request) = reconnect_sink(tx1);

        let req1 = SubscribeRequest {
            from_slot: Some(11),
            ..Default::default()
        };
        sink.send(req1).await.expect("first send must succeed");
        let first = rx1.next().await.expect("first receiver gets the request");
        assert_eq!(first.from_slot, Some(11));

        // A reconnect installs the new connection; the request did not change meanwhile.
        request.install_sender(tx2, &request.load());
        assert!(rx2.next().now_or_never().is_none(), "nothing to resend");

        let req2 = SubscribeRequest {
            from_slot: Some(22),
            ..Default::default()
        };
        sink.send(req2).await.expect("second send must succeed");
        let second = rx2.next().await.expect("second receiver gets the request");
        assert_eq!(second.from_slot, Some(22));

        match rx1.next().now_or_never() {
            None | Some(None) => {}
            Some(Some(_)) => panic!("old receiver must not get requests after sender swap"),
        }
    }

    #[tokio::test]
    async fn plain_sink_sends_requests_unchanged() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut sink = plain_sink(tx);

        sink.send(SubscribeRequest::default())
            .await
            .expect("send must succeed");
        let sent = rx.next().await.expect("receiver gets the request");
        assert_eq!(sent, SubscribeRequest::default());
    }

    #[tokio::test]
    async fn reconnect_sink_adds_the_reconnect_filters_and_keeps_the_request() {
        let (tx, mut rx) = mpsc::channel(8);
        let (mut sink, request) = reconnect_sink(tx);

        let mut update = SubscribeRequest::default();
        update.slots.insert("user".to_owned(), Default::default());
        sink.send(update).await.expect("send must succeed");

        let sent = rx.next().await.expect("receiver gets the request");
        assert!(sent.slots.contains_key("user"));
        assert!(sent.slots.contains_key(RECONNECT_FILTER_KEY));
        assert!(sent.blocks_meta.contains_key(RECONNECT_FILTER_KEY));
        assert_eq!(*request.load(), sent);
    }

    #[tokio::test]
    async fn reconnect_sink_does_not_keep_a_ping() {
        let (tx, mut rx) = mpsc::channel(8);
        let (mut sink, request) = reconnect_sink(tx);
        let before = request.load();

        let ping = SubscribeRequest {
            ping: Some(SubscribeRequestPing { id: 1 }),
            ..Default::default()
        };
        sink.send(ping.clone()).await.expect("send must succeed");

        assert_eq!(rx.next().await, Some(ping));
        assert!(Arc::ptr_eq(&before, &request.load()));
    }

    #[tokio::test]
    async fn subscribe_with_reconnect_rejects_startup_snapshots() {
        let mut client = GeyserGrpcClient::build_from_static("http://127.0.0.1:1")
            .set_x_request_snapshot(true)
            .connect_lazy()
            .expect("lazy client");
        let Err(GeyserGrpcClientError::TonicStatus(status)) =
            client.subscribe_with_reconnect(None).await
        else {
            panic!("a snapshot subscription must not reconnect");
        };
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }
}

/// Tests against an in-process Geyser server, adapted from `refactor/auto-reconnect`: they
/// cover `TonicGrpcConnector` and the sink's sender swap, which the scripted tests skip.
#[cfg(test)]
mod server_tests {
    use {
        super::*,
        futures::StreamExt,
        yellowstone_grpc_proto::prelude::{
            subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdateAccount,
            SubscribeUpdateBlockMeta, SubscribeUpdateSlot,
        },
    };

    struct SubscribeService {
        updates: Mutex<Option<mpsc::UnboundedReceiver<Result<SubscribeUpdate, Status>>>>,
        replay_requests: Arc<Mutex<Vec<u64>>>,
        /// Every request of the subscription: the first, then each one sent later.
        requests: Arc<Mutex<Vec<SubscribeRequest>>>,
        accept_replay: bool,
        expected_request: Option<SubscribeRequest>,
        hold: Mutex<Option<Hold>>,
    }

    /// Holds a subscribe response until the test releases it.
    struct Hold {
        /// Fires once the subscribe request arrived.
        reached: futures::channel::oneshot::Sender<()>,
        /// The response waits for it.
        release: futures::channel::oneshot::Receiver<()>,
    }

    /// The test's side of a [`Hold`].
    struct HoldHandle {
        reached: futures::channel::oneshot::Receiver<()>,
        release: futures::channel::oneshot::Sender<()>,
    }

    #[tonic::async_trait]
    impl yellowstone_grpc_proto::geyser::geyser_server::Geyser for SubscribeService {
        type SubscribeStream = futures::stream::BoxStream<'static, Result<SubscribeUpdate, Status>>;

        async fn subscribe(
            &self,
            request: Request<tonic::Streaming<SubscribeRequest>>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            let mut incoming = request.into_inner();
            let request = incoming.message().await?.unwrap();
            self.requests.lock().unwrap().push(request.clone());
            if let Some(expected) = &self.expected_request {
                assert_eq!(&request, expected);
            } else {
                assert!(request.blocks_meta.contains_key(RECONNECT_FILTER_KEY));
                assert_eq!(
                    request
                        .slots
                        .get(RECONNECT_FILTER_KEY)
                        .unwrap()
                        .filter_by_commitment,
                    Some(false)
                );
            }
            if let Some(slot) = request.from_slot {
                self.replay_requests.lock().unwrap().push(slot);
                if !self.accept_replay {
                    return Err(Status::failed_precondition(
                        "test server has no replay coverage",
                    ));
                }
            }
            let hold = self.hold.lock().unwrap().take();
            if let Some(Hold { reached, release }) = hold {
                let _ = reached.send(());
                let _ = release.await;
            }
            let requests = Arc::clone(&self.requests);
            tokio::spawn(async move {
                while let Ok(Some(request)) = incoming.message().await {
                    requests.lock().unwrap().push(request);
                }
            });
            Ok(Response::new(
                self.updates.lock().unwrap().take().unwrap().boxed(),
            ))
        }
        type SubscribeDeshredStream = futures::stream::BoxStream<
            'static,
            Result<yellowstone_grpc_proto::geyser::SubscribeUpdateDeshred, Status>,
        >;
        async fn subscribe_deshred(
            &self,
            _: Request<tonic::Streaming<yellowstone_grpc_proto::geyser::SubscribeDeshredRequest>>,
        ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
            Err(Status::unimplemented("unused"))
        }
        type SubscribeGossipStream = futures::stream::BoxStream<
            'static,
            Result<yellowstone_grpc_proto::geyser::SubscribeUpdateGossip, Status>,
        >;
        async fn subscribe_gossip(
            &self,
            _: Request<yellowstone_grpc_proto::geyser::SubscribeGossipRequest>,
        ) -> Result<Response<Self::SubscribeGossipStream>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn subscribe_replay_info(
            &self,
            _: Request<yellowstone_grpc_proto::geyser::SubscribeReplayInfoRequest>,
        ) -> Result<Response<yellowstone_grpc_proto::geyser::SubscribeReplayInfoResponse>, Status>
        {
            Err(Status::unimplemented("unused"))
        }
        async fn ping(
            &self,
            _: Request<yellowstone_grpc_proto::geyser::PingRequest>,
        ) -> Result<Response<yellowstone_grpc_proto::geyser::PongResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn get_latest_blockhash(
            &self,
            _: Request<yellowstone_grpc_proto::geyser::GetLatestBlockhashRequest>,
        ) -> Result<Response<yellowstone_grpc_proto::geyser::GetLatestBlockhashResponse>, Status>
        {
            Err(Status::unimplemented("unused"))
        }
        async fn get_block_height(
            &self,
            _: Request<yellowstone_grpc_proto::geyser::GetBlockHeightRequest>,
        ) -> Result<Response<yellowstone_grpc_proto::geyser::GetBlockHeightResponse>, Status>
        {
            Err(Status::unimplemented("unused"))
        }
        async fn get_slot(
            &self,
            _: Request<yellowstone_grpc_proto::geyser::GetSlotRequest>,
        ) -> Result<Response<yellowstone_grpc_proto::geyser::GetSlotResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn is_blockhash_valid(
            &self,
            _: Request<yellowstone_grpc_proto::geyser::IsBlockhashValidRequest>,
        ) -> Result<Response<yellowstone_grpc_proto::geyser::IsBlockhashValidResponse>, Status>
        {
            Err(Status::unimplemented("unused"))
        }
        async fn get_version(
            &self,
            _: Request<yellowstone_grpc_proto::geyser::GetVersionRequest>,
        ) -> Result<Response<yellowstone_grpc_proto::geyser::GetVersionResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }

    fn account(slot: u64, bank_id: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["accounts".into()],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                bank_id: Some(bank_id),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn hidden_status(slot: u64, status: SlotStatus) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![RECONNECT_FILTER_KEY.into()],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                bank_id: Some(slot),
                status: status as i32,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn hidden_meta(slot: u64, bank_id: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![RECONNECT_FILTER_KEY.into()],
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                bank_id,
                blockhash: "unchanged".into(),
                parent_slot: slot - 1,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    /// A Geyser server on a local port that serves one scripted subscription.
    struct TestServer {
        endpoint: String,
        /// Updates the subscription sends; dropping it ends the subscription.
        updates: mpsc::UnboundedSender<Result<SubscribeUpdate, Status>>,
        /// The `from_slot` of every subscription that asked for a replay.
        replay_requests: Arc<Mutex<Vec<u64>>>,
        /// Every request the subscription received.
        requests: Arc<Mutex<Vec<SubscribeRequest>>>,
        handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    }

    impl TestServer {
        /// Starts a server.
        ///
        /// # Arguments
        ///
        /// * `accept_replay` - Whether a subscription with `from_slot` is served or refused.
        /// * `expected_request` - The exact request to expect, or [`None`] to only check
        ///   that it carries the reconnect filters.
        async fn spawn(accept_replay: bool, expected_request: Option<SubscribeRequest>) -> Self {
            Self::spawn_with(accept_replay, expected_request, None).await
        }

        /// Starts a server that holds its subscribe response until the test releases it.
        ///
        /// # Returns
        ///
        /// The server, and the [`HoldHandle`] to wait for the request and release the response.
        async fn spawn_held(accept_replay: bool) -> (Self, HoldHandle) {
            let (reached_tx, reached) = futures::channel::oneshot::channel();
            let (release, release_rx) = futures::channel::oneshot::channel();
            let hold = Hold {
                reached: reached_tx,
                release: release_rx,
            };
            let server = Self::spawn_with(accept_replay, None, Some(hold)).await;
            (server, HoldHandle { reached, release })
        }

        async fn spawn_with(
            accept_replay: bool,
            expected_request: Option<SubscribeRequest>,
            hold: Option<Hold>,
        ) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let incoming = futures::stream::unfold(listener, |listener| async move {
                Some((listener.accept().await.map(|(stream, _)| stream), listener))
            });
            let (updates, rx) = mpsc::unbounded();
            let replay_requests = Arc::new(Mutex::new(Vec::new()));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let handle = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(
                        yellowstone_grpc_proto::geyser::geyser_server::GeyserServer::new(
                            SubscribeService {
                                updates: Mutex::new(Some(rx)),
                                replay_requests: Arc::clone(&replay_requests),
                                requests: Arc::clone(&requests),
                                accept_replay,
                                expected_request,
                                hold: Mutex::new(hold),
                            },
                        ),
                    )
                    .serve_with_incoming(incoming),
            );
            Self {
                endpoint,
                updates,
                replay_requests,
                requests,
                handle,
            }
        }

        async fn stop(self) {
            self.handle.abort();
            let _ = self.handle.await;
        }
    }

    async fn next<S, T>(stream: &mut S) -> T
    where
        S: Stream<Item = T> + Unpin,
    {
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended")
    }

    /// Plain subscriptions must not reconnect, add filters or hide updates, with or without
    /// a reconnect config: the reconnect machinery is only for `subscribe_with_reconnect`.
    #[tokio::test]
    async fn ordinary_subscriptions_never_reconnect_or_rewrite_updates() {
        for config in [None, Some(ReconnectConfig::default())] {
            for method in 0..3 {
                for disconnect_error in [false, true] {
                    let request = if method == 0 {
                        SubscribeRequest::default()
                    } else {
                        SubscribeRequest {
                            accounts: [("accounts".into(), Default::default())].into(),
                            commitment: Some(1),
                            ..Default::default()
                        }
                    };
                    let server = TestServer::spawn(false, Some(request.clone())).await;
                    let mut builder =
                        GeyserGrpcClient::build_from_shared(server.endpoint.clone()).unwrap();
                    if let Some(config) = config.clone() {
                        builder = builder.set_reconnect_config(config);
                    }
                    let mut client = builder.connect().await.unwrap();
                    let (_sink, mut stream) = match method {
                        0 => {
                            let (sink, stream) = client.subscribe().await.unwrap();
                            (Some(sink), stream)
                        }
                        1 => {
                            let (sink, stream) =
                                client.subscribe_with_request(Some(request)).await.unwrap();
                            (Some(sink), stream)
                        }
                        _ => (None, client.subscribe_once(request).await.unwrap()),
                    };
                    // An update named after the reconnect filter goes through untouched.
                    for update in [hidden_meta(42, 7), account(42, 7), account(42, 7)] {
                        server.updates.unbounded_send(Ok(update.clone())).unwrap();
                        assert_eq!(next(&mut stream).await.unwrap(), update);
                    }
                    if disconnect_error {
                        server
                            .updates
                            .unbounded_send(Err(Status::unavailable("ordinary disconnect")))
                            .unwrap();
                        let error = next(&mut stream).await.unwrap_err();
                        assert_eq!(error.code(), tonic::Code::Unavailable);
                        assert_eq!(error.message(), "ordinary disconnect");
                    }
                    server.updates.close_channel();
                    assert!(tokio::time::timeout(Duration::from_secs(2), stream.next())
                        .await
                        .unwrap()
                        .is_none());
                    assert!(server.replay_requests.lock().unwrap().is_empty());
                    server.stop().await;
                }
            }
        }
    }

    /// The public API end to end: the first connection on the client's channel, a reconnect
    /// to another server through `TonicGrpcConnector`, with the reconnect filters on both
    /// subscriptions and a replay from the last confirmed slot.
    #[tokio::test]
    async fn public_api_reports_partial_banks_and_replays_on_another_server() {
        let first = TestServer::spawn(true, None).await;
        let second = TestServer::spawn(true, None).await;
        let mut client = GeyserGrpcClient::build_from_shared(first.endpoint.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        // Route reconnects to the second server while the first channel stays on the first.
        client.reconnect_endpoint = Some(Endpoint::from_shared(second.endpoint.clone()).unwrap());
        let (_sink, mut stream) = client.subscribe_with_reconnect(None).await.unwrap();

        first
            .updates
            .unbounded_send(Ok(hidden_status(41, SlotStatus::SlotConfirmed)))
            .unwrap();
        first.updates.unbounded_send(Ok(account(42, 7))).unwrap();
        assert_eq!(
            next(&mut stream).await.unwrap(),
            ReconnectEvent::Update(account(42, 7))
        );

        second.updates.unbounded_send(Ok(account(42, 99))).unwrap();
        first
            .updates
            .unbounded_send(Err(Status::unavailable("mid-bank disconnect")))
            .unwrap();
        assert_eq!(
            next(&mut stream).await.unwrap(),
            ReconnectEvent::Disconnected(Disconnected {
                partial_banks: vec![BankRef {
                    slot: 42,
                    bank_id: 7
                }],
                replay_from_slot: Some(41),
            })
        );
        assert_eq!(
            next(&mut stream).await.unwrap(),
            ReconnectEvent::Update(account(42, 99))
        );
        assert_eq!(*second.replay_requests.lock().unwrap(), [41]);
        assert!(first.replay_requests.lock().unwrap().is_empty());
        first.stop().await;
        second.stop().await;
    }

    /// A filter change sent while a reconnect is under way must reach the new connection.
    /// The second server holds its subscribe response, so the change is sent after the
    /// connector read the stored request and before it swapped the sink's sender.
    ///
    /// ```text
    /// time ──────────────────────────────────────────────────────────────────────────►
    /// server 1:   subscribe(v1) … ✂ drop
    /// stream:                         Disconnected  load() = v1, connect to server 2
    /// server 2:                                     subscribe(v1)  ⏸ held ──── ▶ released
    /// user:                                                    send(v2)
    /// server 2 gets:                                                               v2
    /// ```
    ///
    /// Before the fix, `send(v2)` went to server 1's dead connection and returned `Ok`, and
    /// server 2 never got `v2`.
    #[tokio::test]
    async fn a_filter_change_during_a_reconnect_reaches_the_new_connection() {
        let first = TestServer::spawn(true, None).await;
        let (second, held) = TestServer::spawn_held(true).await;
        let mut client = GeyserGrpcClient::build_from_shared(first.endpoint.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        client.reconnect_endpoint = Some(Endpoint::from_shared(second.endpoint.clone()).unwrap());
        let v1 = SubscribeRequest {
            accounts: [("v1".into(), Default::default())].into(),
            ..Default::default()
        };
        let (mut sink, stream) = client
            .subscribe_with_reconnect(Some(v1.clone()))
            .await
            .unwrap();
        // Poll the stream in the background: the reconnect runs only while it is polled.
        let (events_tx, mut events) = mpsc::unbounded();
        let reader = tokio::spawn(async move {
            let mut stream = stream;
            while let Some(event) = stream.next().await {
                if events_tx.unbounded_send(event).is_err() {
                    break;
                }
            }
        });

        first
            .updates
            .unbounded_send(Err(Status::unavailable("drop")))
            .unwrap();
        assert!(matches!(
            next(&mut events).await.unwrap(),
            ReconnectEvent::Disconnected(_)
        ));
        // The reconnect read the stored request and waits for the second server's answer.
        tokio::time::timeout(Duration::from_secs(2), held.reached)
            .await
            .expect("the reconnect never reached the second server")
            .unwrap();

        let mut v2 = v1.clone();
        v2.accounts.insert("v2".into(), Default::default());
        let sent = sink.send(v2).await;

        held.release.send(()).unwrap();
        second.updates.unbounded_send(Ok(account(1, 1))).unwrap();
        assert!(matches!(
            next(&mut events).await.unwrap(),
            ReconnectEvent::Update(_)
        ));
        // Time for a request sent on the new connection to arrive.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let received: Vec<Vec<String>> = second
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| {
                let mut names: Vec<_> = request.accounts.keys().cloned().collect();
                names.sort();
                names
            })
            .collect();
        assert!(
            received
                .iter()
                .any(|names| names.contains(&"v2".to_owned())),
            "the new connection never got the filter change: it received account filters \
             {received:?}; sink.send returned {sent:?}"
        );
        reader.abort();
        first.stop().await;
        second.stop().await;
    }
}
