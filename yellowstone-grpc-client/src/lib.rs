pub use tonic::{service::Interceptor, transport::ClientTlsConfig};
use {
    bytes::Bytes,
    futures::{
        channel::mpsc,
        sink::{Sink, SinkExt},
        stream::Stream,
        StreamExt,
    },
    std::{path::PathBuf, time::Duration},
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
        SubscribeDeshredRequest, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse,
        SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
    },
};

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
}

pub type GeyserGrpcClientResult<T> = Result<T, GeyserGrpcClientError>;

///
/// See [`GeyserGrpcBuilder`] for constructing a client with custom options.
///
#[derive(Clone)]
pub struct GeyserGrpcClient {
    pub health: HealthClient<InterceptedService<Channel, InterceptorXToken>>,
    pub geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>,
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
pub struct SubscribeRequestSinkError(#[from] mpsc::SendError);

///
/// A sink returned by the [`GeyserGrpcClient::subscribe`].
///
/// The sink is used to send [`SubscribeRequest`] updates to the server.
///
#[derive(Clone)]
pub struct SubscribeRequestSink {
    inner: mpsc::UnboundedSender<SubscribeRequest>,
}

impl Sink<SubscribeRequest> for SubscribeRequestSink {
    type Error = SubscribeRequestSinkError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: SubscribeRequest,
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
    inner: Streaming<SubscribeUpdate>,
}

impl Stream for GeyserStream {
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

///
/// Streams returned by the [`GeyserGrpcClient::subscribe_deshred`].
///
/// The stream yields [`SubscribeUpdateDeshred`] from the server.
///
pub struct DeshredStream {
    inner: Streaming<SubscribeUpdateDeshred>,
}

impl Stream for DeshredStream {
    type Item = Result<SubscribeUpdateDeshred, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

///
/// Errors returns by the [`SubscribeDeshredRequestSink`] when sending subscription updates to the server.
///
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct SubscribeDeshredRequestSinkError(#[from] mpsc::SendError);

///
/// Sinks returned by the [`GeyserGrpcClient::subscribe_deshred`].
///
/// The sink is used to send [`SubscribeDeshredRequest`] updates to the server.
///
pub struct SubscribeDeshredRequestSink {
    inner: mpsc::UnboundedSender<SubscribeDeshredRequest>,
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

impl GeyserGrpcClient {
    // TODO: check if we need to make this function `pub(crate` instead as users of the lib should use the builder to construct a client anyway.
    pub const fn new(
        health: HealthClient<InterceptedService<Channel, InterceptorXToken>>,
        geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>,
    ) -> Self {
        Self { health, geyser }
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

    ///
    /// Establish a subscription to the gRPC server without an initial request.
    ///
    /// If you don't plan to change the [`SubscribeRequest`] after the initial subscription, consider using [`GeyserGrpcClient::subscribe_once`] instead for a simpler API.
    ///
    /// # Returns
    ///
    /// A tuple of ([`SubscribeRequestSink`], [`GeyserStream`]):
    ///
    /// - [`SubscribeRequestSink`]: a sink to send `SubscribeRequest` updates to the server.
    ///   The server will update the subscription based on the latest request received from the sink.
    ///   request received from the sink.
    /// - [`GeyserStream`]: a stream of `SubscribeUpdate` from the server.
    ///   The stream will yield updates based on the latest request received from the sink.
    ///
    /// # Lifecyle and dropping rules
    ///
    /// The subscription will remain active until the stream is dropped or the server closes the connection.
    ///
    /// # Initial [`SubscribeRequest`]
    ///
    /// You have to provide a [`SubscribeRequest`] to the server to start receiving updates.
    ///
    pub async fn subscribe(
        &mut self,
    ) -> GeyserGrpcClientResult<(SubscribeRequestSink, GeyserStream)> {
        self.subscribe_with_request(None).await
    }

    ///
    /// Similar to [`GeyserGrpcClient::subscribe`] but allows you to provide an initial [`SubscribeRequest`] to the server.
    ///
    pub async fn subscribe_with_request(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(SubscribeRequestSink, GeyserStream)> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        if let Some(request) = request {
            match subscribe_tx.send(request).await {
                Ok(_) => (),
                Err(e) => unreachable!(
                    "channel cannot be disconnect or full at this point, got error: {e}"
                ),
            }
        }
        let response: Response<Streaming<SubscribeUpdate>> =
            self.geyser.subscribe(subscribe_rx).await?;
        Ok((
            SubscribeRequestSink {
                inner: subscribe_tx,
            },
            GeyserStream {
                inner: response.into_inner(),
            },
        ))
    }

    ///
    /// Subscribe to updates with an initial request.
    ///
    /// Unlike [`GeyserGrpcClient::subscribe`], it does not return the sink to send subsequent requests,
    /// so it is only useful for one-off subscription that does not need to update the request after
    /// the initial subscription.
    ///
    pub async fn subscribe_once(
        &mut self,
        request: SubscribeRequest,
    ) -> GeyserGrpcClientResult<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
        self.subscribe_with_request(Some(request))
            .await
            .map(|(_sink, stream)| stream)
    }

    ///
    /// Subscribe to deshred (only transaction right now).
    ///
    /// Deshred updates only supports deshredded transaction updates.
    /// Deshredded update are event happening before any replay, they are not guarantee to be valid or even included in the ledger,
    /// but they are emitted at the earliest possible time with the most information available.
    ///
    /// # Deshred vs Processed
    ///
    /// Deshred transactions have not been replayed yet, so they do not contains any replayed metadata, such as status, log messages, or compute units used.
    ///
    pub async fn subscribe_deshred(
        &mut self,
    ) -> GeyserGrpcClientResult<(SubscribeDeshredRequestSink, DeshredStream)> {
        self.subscribe_deshred_with_request(None).await
    }

    ///
    /// See [`GeyserGrpcClient::subscribe_deshred`] for more details.
    pub async fn subscribe_deshred_with_request(
        &mut self,
        request: Option<SubscribeDeshredRequest>,
    ) -> GeyserGrpcClientResult<(SubscribeDeshredRequestSink, DeshredStream)> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        if let Some(request) = request {
            match subscribe_tx.send(request).await {
                Ok(_) => (),
                Err(e) => unreachable!(
                    "channel cannot be disconnect or full at this point, got error: {e}"
                ),
            }
        }
        let response: Response<Streaming<SubscribeUpdateDeshred>> =
            self.geyser.subscribe_deshred(subscribe_rx).await?;
        Ok((
            SubscribeDeshredRequestSink {
                inner: subscribe_tx,
            },
            DeshredStream {
                inner: response.into_inner(),
            },
        ))
    }

    ///
    /// Subscribe to deshred updates with an initial request.
    ///
    /// Unlike [`GeyserGrpcClient::subscribe_deshred`], it does not return the sink to send subsequent requests,
    /// so it is only useful for one-off subscription that does not need to update the request after
    /// the initial subscription.
    ///
    pub async fn subscribe_deshred_once(
        &mut self,
        request: SubscribeDeshredRequest,
    ) -> GeyserGrpcClientResult<impl Stream<Item = Result<SubscribeUpdateDeshred, Status>>> {
        self.subscribe_deshred_with_request(Some(request))
            .await
            .map(|(_sink, stream)| stream)
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
    ///
    /// Raised when invalid x-token is provided, such as empty string or string with non-ASCII characters.
    #[error("Failed to parse x-token: {0}")]
    MetadataValueError(#[from] InvalidMetadataValue),
    ///
    /// Raised when there is an error in the underlying gRPC transport, such as invalid URI, connection failure, TLS configuration error, etc.
    ///
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
#[derive(Debug)]
pub struct GeyserGrpcBuilder {
    pub endpoint: Endpoint,
    pub x_token: Option<AsciiMetadataValue>,
    pub x_request_snapshot: bool,
    pub send_compressed: Option<CompressionEncoding>,
    pub accept_compressed: Option<CompressionEncoding>,
    pub max_decoding_message_size: Option<usize>,
    pub max_encoding_message_size: Option<usize>,
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
        }
    }

    pub fn from_shared(endpoint: impl Into<Bytes>) -> GeyserGrpcBuilderResult<Self> {
        Ok(Self::new(Endpoint::from_shared(endpoint)?))
    }

    pub fn from_static(endpoint: &'static str) -> Self {
        Self::new(Endpoint::from_static(endpoint))
    }

    // Create client
    fn build(self, channel: Channel) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
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

        Ok(GeyserGrpcClient::new(
            HealthClient::with_interceptor(channel, interceptor),
            geyser,
        ))
    }

    ///
    /// Builds an instance of [`GeyserGrpcClient`] by connecting to the gRPC server.
    ///
    pub async fn connect(self) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let channel = self.endpoint.connect().await?;
        self.build(channel)
    }

    ///
    /// Builds an instance of [`GeyserGrpcClient`] without actually connecting to the gRPC server.
    /// This will wait for the first gRPC call to trigger the connection to the server, and it will use the configured options in the builder for that connection.
    ///
    pub fn connect_lazy(self) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let channel = self.endpoint.connect_lazy();
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

    ///
    /// Sets `x-token` credentials for the client. The token will be included in the metadata of every gRPC request sent by the client.
    pub fn x_token<T>(self, x_token: Option<T>) -> GeyserGrpcBuilderResult<Self>
    where
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        Ok(Self {
            x_token: x_token.map(|x_token| x_token.try_into()).transpose()?,
            ..self
        })
    }

    ///
    /// Sets the `x-request-snapshot` flag for the client. This flag will be included in the metadata of every gRPC request sent by the client.
    pub fn set_x_request_snapshot(self, value: bool) -> Self {
        Self {
            x_request_snapshot: value,
            ..self
        }
    }

    ///
    /// Sets endpoint options
    ///
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
}

#[cfg(test)]
mod tests {
    use super::GeyserGrpcClient;

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
}
