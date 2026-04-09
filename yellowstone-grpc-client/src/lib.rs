mod dedup;
mod reconnect;

use {
    crate::{dedup::DEFAULT_SLOT_RETENTION, reconnect::{Backoff, TonicGeyserClientOptions}},
    arc_swap::ArcSwap,
    bytes::Bytes,
    futures::{
        channel::mpsc,
        sink::{Sink, SinkExt},
        stream::Stream,
    },
    std::{
        path::PathBuf,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::net::UnixStream,
    tonic::{
        Request, Response, Status, codec::{CompressionEncoding, Streaming}, metadata::{AsciiMetadataValue, MetadataValue, errors::InvalidMetadataValue}, service::interceptor::InterceptedService, transport::{
            Uri, channel::{Channel, Endpoint}
        }
    },
    tonic_health::pb::{HealthCheckRequest, HealthCheckResponse, health_client::HealthClient},
    yellowstone_grpc_proto::prelude::{
        CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeDeshredRequest, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred, geyser_client::GeyserClient
    },
};
pub use {
    crate::{
        dedup::{DedupState, DedupStream},
        reconnect::{AutoReconnect, GrpcConnector, TonicGrpcConnector},
    },
    tonic::{service::Interceptor, transport::ClientTlsConfig},
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
    #[error("gRPC transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
}

pub type GeyserGrpcClientResult<T> = Result<T, GeyserGrpcClientError>;

#[derive(Clone, Debug)]
/// Configuration for automatic subscribe reconnect behavior.
pub struct ReconnectConfig {
    pub backoff: Backoff,
    pub slot_retention: usize,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            backoff: Backoff::default(),
            slot_retention: DEFAULT_SLOT_RETENTION,
        }
    }
}

impl ReconnectConfig {
    pub const fn no_reconnect() -> Self {
        Self {
            backoff: Backoff::new(Duration::from_millis(0), Duration::from_millis(0), 1.0, 0),
            slot_retention: 0,
        }
    }

    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    pub fn with_slot_retention(mut self, slot_retention: usize) -> Self {
        self.slot_retention = slot_retention;
        self
    }
}

#[derive(Clone)]
pub struct GeyserGrpcClient {
    pub health: HealthClient<InterceptedService<Channel, InterceptorXToken>>,
    pub geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>,
    reconnect_config: ReconnectConfig,
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

pub struct GeyserStream {
    inner: AutoReconnect<Streaming<SubscribeUpdate>, TonicGrpcConnector>,
}

pub struct SubscribeDeshredStream {
    inner: Streaming<SubscribeUpdateDeshred>,
}

impl Stream for SubscribeDeshredStream {
    type Item = Result<SubscribeUpdateDeshred, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl Stream for GeyserStream {
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// User-facing sink for subscribe requests on a bidirectional stream.
///
/// The inner sender is swappable so reconnect logic can replace the active
/// request channel while keeping this sink instance alive.
#[derive(Clone)]
pub struct SubscribeRequestSink {
    inner: Arc<Mutex<mpsc::Sender<SubscribeRequest>>>,
    shared: Arc<ArcSwap<SubscribeRequest>>,
}

#[derive(Debug, thiserror::Error)]
#[error("{inner}")]
pub struct SubscribeRequestSinkError {
    inner: mpsc::SendError,
}

impl From<mpsc::SendError> for SubscribeRequestSinkError {
    fn from(err: mpsc::SendError) -> Self {
        Self { inner: err }
    }
}

impl Sink<SubscribeRequest> for SubscribeRequestSink {
    type Error = SubscribeRequestSinkError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let mut inner = self
            .inner
            .lock()
            .expect("subscribe request sink mutex poisoned");
        std::pin::Pin::new(&mut *inner)
            .poll_ready(cx)
            .map_err(Into::into)
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: SubscribeRequest,
    ) -> Result<(), Self::Error> {
        let mut inner = self
            .inner
            .lock()
            .expect("subscribe request sink mutex poisoned");
        inner
            .start_send_unpin(item.clone())
            .map_err(SubscribeRequestSinkError::from)?;
        self.shared.store(Arc::new(item));
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let mut inner = self
            .inner
            .lock()
            .expect("subscribe request sink mutex poisoned");
        inner.poll_flush_unpin(cx).map_err(Into::into)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let mut inner = self
            .inner
            .lock()
            .expect("subscribe request sink mutex poisoned");
        inner.poll_close_unpin(cx).map_err(Into::into)
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
            reconnect_config: ReconnectConfig::no_reconnect(),
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

    pub(crate) async fn subscribe_raw(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(SubscribeRequestSink, Streaming<SubscribeUpdate>)> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::channel(1000);
        if let Some(request) = request.clone() {
            subscribe_tx
                .send(request)
                .await
                .expect("channel cannot be disconnected or full at this point");
        }
        let response: Response<Streaming<SubscribeUpdate>> =
            self.geyser.subscribe(subscribe_rx).await?;
        let sink = SubscribeRequestSink {
            inner: Arc::new(Mutex::new(subscribe_tx)),
            shared: Arc::new(ArcSwap::new(Arc::new(request.unwrap_or_default()))),
        };
        Ok((sink, response.into_inner()))
    }

    pub async fn subscribe_with_request(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(SubscribeRequestSink, GeyserStream)> {
        let reconnect_config = self.reconnect_config.clone();
        let endpoint = self
            .reconnect_endpoint
            .clone()
            .unwrap_or_else(|| Endpoint::from_static("http://127.0.0.1:0"));
        let reconnect_x_token = self.reconnect_x_token.clone();

        self.subscribe_raw(request.clone())
            .await
            .map(|(sink, stream)| {
                let connector = TonicGrpcConnector::new(
                    endpoint,
                    reconnect_config.clone(),
                    reconnect_x_token,
                    self.geyser_client_opts.clone(),
                    Arc::clone(&sink.inner),
                );
                let inner = AutoReconnect::new(
                    DedupStream::new(
                        stream,
                        DedupState::with_slot_retention(reconnect_config.slot_retention),
                    ),
                    connector,
                    Arc::clone(&sink.shared),
                );
                (sink, GeyserStream { inner })
            })
    }

    pub async fn subscribe_once(
        &mut self,
        request: SubscribeRequest,
    ) -> GeyserGrpcClientResult<GeyserStream> {
        let (_sink, stream) = self.subscribe_with_request(Some(request.clone())).await?;
        Ok(stream)
    }

    // Subscribe Deshred
    pub async fn subscribe_deshred(
        &mut self,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeDeshredRequest, Error = mpsc::SendError>,
        SubscribeDeshredStream,
    )> {
        self.subscribe_deshred_with_request(None).await
    }

    pub async fn subscribe_deshred_with_request(
        &mut self,
        request: Option<SubscribeDeshredRequest>,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeDeshredRequest, Error = mpsc::SendError>,
        SubscribeDeshredStream,
    )> {
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
            subscribe_tx,
            SubscribeDeshredStream {
                inner: response.into_inner(),
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

#[derive(Debug)]
pub struct GeyserGrpcBuilder {
    pub endpoint: Endpoint,
    pub x_token: Option<AsciiMetadataValue>,
    pub x_request_snapshot: bool,
    pub send_compressed: Option<CompressionEncoding>,
    pub accept_compressed: Option<CompressionEncoding>,
    pub max_decoding_message_size: Option<usize>,
    pub max_encoding_message_size: Option<usize>,
    pub reconnect_config: ReconnectConfig,
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
            reconnect_config: ReconnectConfig::no_reconnect(),
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

    pub fn set_reconnect_config(self, config: ReconnectConfig) -> Self {
        Self {
            reconnect_config: config,
            ..self
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{GeyserGrpcClient, SubscribeRequest, SubscribeRequestSink},
        arc_swap::ArcSwap,
        futures::{channel::mpsc, FutureExt, SinkExt, StreamExt},
        std::sync::{Arc, Mutex},
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

    #[tokio::test]
    async fn test_subscribe_request_sink_uses_swapped_sender() {
        let (tx1, mut rx1) = mpsc::channel(8);
        let (tx2, mut rx2) = mpsc::channel(8);

        let shared = Arc::new(ArcSwap::new(Arc::new(SubscribeRequest::default())));
        let mut sink = SubscribeRequestSink {
            inner: Arc::new(Mutex::new(tx1)),
            shared: Arc::clone(&shared),
        };

        let req1 = SubscribeRequest {
            from_slot: Some(11),
            ..Default::default()
        };
        sink.send(req1).await.expect("first send must succeed");

        let first = rx1
            .next()
            .await
            .expect("first receiver should get first request");
        assert_eq!(first.from_slot, Some(11));

        *sink
            .inner
            .lock()
            .expect("subscribe request sink mutex poisoned") = tx2;

        let req2 = SubscribeRequest {
            from_slot: Some(22),
            ..Default::default()
        };
        sink.send(req2).await.expect("second send must succeed");

        let second = rx2
            .next()
            .await
            .expect("second receiver should get second request");
        assert_eq!(second.from_slot, Some(22));

        match rx1.next().now_or_never() {
            None | Some(None) => {}
            Some(Some(_)) => panic!("old receiver must not get requests after sender swap"),
        }
        assert_eq!(shared.load_full().from_slot, Some(22));
    }
}
