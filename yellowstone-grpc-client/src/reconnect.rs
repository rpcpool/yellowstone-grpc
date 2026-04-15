use {
    crate::{
        dedup::{DedupState, DedupStream},
        GeyserGrpcClientError, InterceptorXToken, ReconnectConfig,
    },
    arc_swap::ArcSwap,
    futures::{channel::mpsc, sink::SinkExt, stream::Stream},
    std::{
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::Duration,
    },
    tonic::{
        codec::CompressionEncoding, metadata::AsciiMetadataValue, transport::Endpoint, Code,
        Status, Streaming,
    },
    yellowstone_grpc_proto::{
        geyser::geyser_client::GeyserClient,
        prelude::{subscribe_update::UpdateOneof, SubscribeRequest, SubscribeUpdate},
    },
};

/// Number of slots behind the last block_meta to checkpoint.
/// Conservative buffer to account for late-arriving events
/// and out-of-order delivery within a slot.
const CHECKPOINT_SLOT_BUFFER: u64 = 2;

pub const AUTORECONNECT_FILTER_KEY: &str = "__autoreconnect";

type ConnectFuture<S> =
    Pin<Box<dyn Future<Output = Result<DedupStream<S>, GeyserGrpcClientError>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct Backoff {
    pub initial_interval: Duration,
    pub multiplier: f64,
    pub max_retries: u32,
}

pub(crate) enum ErrorCategory<E> {
    Retryable(E),
    Unrecoverable(E),
}

impl<E> ErrorCategory<E> {
    const fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }

    fn into_inner(self) -> E {
        match self {
            Self::Retryable(e) | Self::Unrecoverable(e) => e,
        }
    }
}

#[derive(Debug, Clone)]
/// Mutable runtime state for a single backoff execution.
struct BackoffInstance {
    current_interval: Duration,
    attempts: u32,
    multiplier: f64,
    max_retries: u32,
}

impl BackoffInstance {
    const fn new(config: &Backoff) -> Self {
        Self {
            current_interval: config.initial_interval,
            attempts: 0,
            multiplier: config.multiplier,
            max_retries: config.max_retries,
        }
    }

    /// Returns true once the configured retry budget has been consumed.
    const fn exhausted(&self) -> bool {
        self.attempts >= self.max_retries
    }

    /// Advances retry counters and computes the next interval.
    fn advance(&mut self) {
        self.attempts += 1;
        self.current_interval = self.current_interval.mul_f64(self.multiplier);
    }
}

impl Iterator for BackoffInstance {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted() {
            None
        } else {
            let interval = self.current_interval;
            self.advance();
            Some(interval)
        }
    }
}

impl Backoff {
    const fn default_initial_interval() -> Duration {
        Duration::from_millis(10)
    }

    const fn default_multiplier() -> f64 {
        2.0
    }

    const fn default_max_retries() -> u32 {
        3
    }

    /// Creates a new backoff policy.
    pub const fn new(initial_interval: Duration, multiplier: f64, max_retries: u32) -> Self {
        Self {
            initial_interval,
            multiplier,
            max_retries,
        }
    }

    const fn instance(&self) -> BackoffInstance {
        BackoffInstance::new(self)
    }

    /// Retries an async operation with exponential backoff
    ///
    /// The operation returns `ErrorCategory::Retryable` for transient failures,
    /// and `ErrorCategory::Unrecoverable` for terminal failures.
    pub(crate) fn retry<F, Fut, T, E>(&self, mut operation: F) -> impl Future<Output = Result<T, E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ErrorCategory<E>>>,
    {
        let mut state = self.instance();
        async move {
            loop {
                match operation().await {
                    Ok(value) => return Ok(value),
                    Err(error) => {
                        if !error.is_retryable() {
                            return Err(error.into_inner());
                        }

                        let Some(sleep_for) = state.next() else {
                            return Err(error.into_inner());
                        };

                        tokio::time::sleep(sleep_for).await;
                    }
                }
            }
        }
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(
            Self::default_initial_interval(),
            Self::default_multiplier(),
            Self::default_max_retries(),
        )
    }
}

/// Connector trait used by AutoReconnect to create new subscribe streams.
pub trait GrpcConnector: Clone + Send + Sync + 'static {
    type Stream: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static;

    type ConnectError: std::error::Error + Send + Sync + 'static;

    type ConnectFuture: Future<Output = Result<Self::Stream, Self::ConnectError>> + Send + 'static;

    /// `connect()` takes the latest subscribe request and an optional checkpoint slot
    /// for replay. returns a future that resolves to a fresh stream or an error.
    fn connect(
        &self,
        request: Arc<SubscribeRequest>,
        from_slot: Option<u64>,
    ) -> Self::ConnectFuture;
}

/// Tonic connector implementation for AutoReconnect. on reconnect, creates a new channel,
/// subscribes, and swaps the sender in the user's SubscribeRequestSink
/// so both sides of the bidi stream point to the new connection.
#[derive(Clone)]
pub struct TonicGrpcConnector {
    backoff: Backoff,
    request_sink: Arc<Mutex<mpsc::Sender<SubscribeRequest>>>,
    endpoint: Endpoint,
    x_token: Option<AsciiMetadataValue>,
    x_request_snapshot: bool,
    send_compressed: Option<CompressionEncoding>,
    accept_compressed: Option<CompressionEncoding>,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct TonicGeyserClientOptions {
    pub x_request_snapshot: bool,
    pub send_compressed: Option<CompressionEncoding>,
    pub accept_compressed: Option<CompressionEncoding>,
    pub max_decoding_message_size: Option<usize>,
    pub max_encoding_message_size: Option<usize>,
}

impl Default for TonicGeyserClientOptions {
    fn default() -> Self {
        Self {
            x_request_snapshot: false,
            send_compressed: None,
            accept_compressed: None,
            max_decoding_message_size: Some(50_000_000), // 50mb default max message size for tonic
            max_encoding_message_size: Some(50_000_000),
        }
    }
}

impl TonicGrpcConnector {
    pub const fn new(
        endpoint: Endpoint,
        config: ReconnectConfig,
        x_token: Option<AsciiMetadataValue>,
        options: TonicGeyserClientOptions,
        request_sink: Arc<Mutex<mpsc::Sender<SubscribeRequest>>>,
    ) -> Self {
        Self {
            backoff: config.backoff,
            request_sink,
            endpoint,
            x_token,
            x_request_snapshot: options.x_request_snapshot,
            send_compressed: options.send_compressed,
            accept_compressed: options.accept_compressed,
            max_decoding_message_size: options.max_decoding_message_size,
            max_encoding_message_size: options.max_encoding_message_size,
        }
    }
}

impl GrpcConnector for TonicGrpcConnector {
    type Stream = Streaming<SubscribeUpdate>;
    type ConnectError = GeyserGrpcClientError;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Self::Stream, Self::ConnectError>> + Send + 'static>>;

    fn connect(
        &self,
        request: Arc<SubscribeRequest>,
        from_slot: Option<u64>,
    ) -> Self::ConnectFuture {
        let backoff = self.backoff.clone();
        let endpoint = self.endpoint.clone();
        let request_sink = Arc::clone(&self.request_sink);
        let x_token = self.x_token.clone();
        let x_request_snapshot = self.x_request_snapshot;
        let send_compressed = self.send_compressed;
        let accept_compressed = self.accept_compressed;
        let max_decoding_message_size = self.max_decoding_message_size;
        let max_encoding_message_size = self.max_encoding_message_size;
        let base_request = (*request).clone();

        let fut = backoff.retry(move || {
            let endpoint = endpoint.clone();
            let request_sink = Arc::clone(&request_sink);
            let x_token = x_token.clone();
            let mut request = base_request.clone();
            async move {
                request.from_slot = from_slot;

                let channel = endpoint
                    .connect()
                    .await
                    .map_err(GeyserGrpcClientError::TransportError)
                    .map_err(ErrorCategory::Retryable)?;

                let interceptor = InterceptorXToken {
                    x_token,
                    x_request_snapshot,
                };

                let mut geyser =
                    GeyserClient::with_interceptor(channel.clone(), interceptor.clone());
                if let Some(encoding) = send_compressed {
                    geyser = geyser.send_compressed(encoding);
                }
                if let Some(encoding) = accept_compressed {
                    geyser = geyser.accept_compressed(encoding);
                }
                if let Some(limit) = max_decoding_message_size {
                    geyser = geyser.max_decoding_message_size(limit);
                }
                if let Some(limit) = max_encoding_message_size {
                    geyser = geyser.max_encoding_message_size(limit);
                }

                let (mut subscribe_tx, subscribe_rx) = mpsc::channel(1000);
                subscribe_tx
                    .send(request)
                    .await
                    .expect("channel cannot be disconnected or full at this point");

                let mut tonic_request = tonic::Request::new(subscribe_rx);
                if let Some(slot) = from_slot {
                    tonic_request.metadata_mut().insert(
                        "x-min-context-slot",
                        slot.to_string().parse().expect("slot is valid metadata"),
                    );
                }

                let response = geyser
                    .subscribe(tonic_request)
                    .await
                    .map_err(GeyserGrpcClientError::from)
                    .map_err(|e| {
                        if is_recoverable_client_error(&e) {
                            ErrorCategory::Retryable(e)
                        } else {
                            ErrorCategory::Unrecoverable(e)
                        }
                    })?;

                // Reconnect creates a new bidi request channel; swap sender so user-facing
                // SubscribeRequestSink continues writing into the active stream.
                *request_sink.lock().expect("request sink mutex poisoned") = subscribe_tx;

                Ok(response.into_inner())
            }
        });

        Box::pin(fut)
    }
}

/// Stream wrapper that transparently reconnects on recoverable failures.
/// delegates connection logic to a `GrpcConnector` and preserves dedup state across reconnects.
///
/// `request` is shared with `SubscribeRequestSink` via `ArcSwap`, when the user
/// sends a new subscribe request mid-stream, ArcSwap stores it atomically.
/// on reconnect, we load the latest request so the new connection gets
/// the user's current filters, not the stale initial ones.
pub struct AutoReconnect<GrpcStream, Connector> {
    request: Arc<ArcSwap<SubscribeRequest>>,
    last_checkpoint: Option<u64>,
    stop: bool,
    connector: Connector,
    backoff: Backoff,
    inner_stream: Option<DedupStream<GrpcStream>>,
    pending_connecting_task: Option<ConnectFuture<GrpcStream>>,
}

impl<S, Connector> AutoReconnect<S, Connector>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
    Connector: GrpcConnector<Stream = S, ConnectError = GeyserGrpcClientError>,
{
    pub fn new(
        stream: DedupStream<S>,
        connector: Connector,
        request: Arc<ArcSwap<SubscribeRequest>>,
        backoff: Backoff,
    ) -> Self {
        Self {
            request,
            last_checkpoint: None,
            inner_stream: Some(stream),
            pending_connecting_task: None,
            stop: false,
            connector,
            backoff,
        }
    }

    fn make_connection_future(&self, dedup_state: DedupState) -> ConnectFuture<S> {
        let connector = self.connector.clone();
        let request = self.request.load_full();
        let from_slot = self.last_checkpoint;
        let fut = async move {
            let stream = connector.connect(request, from_slot).await?;
            Ok(DedupStream::new(stream, dedup_state))
        };
        Box::pin(fut)
    }
}

impl<S, Connector> Stream for AutoReconnect<S, Connector>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
    Connector: GrpcConnector<Stream = S, ConnectError = GeyserGrpcClientError> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    /// State machine with two states:
    /// - inner_stream is Some -> poll it for messages, checkpoint on BlockMeta
    /// - pending_connecting_task is Some -> poll the reconnect future
    ///
    /// on recoverable error: extract dedup state, start reconnect, loop back.
    /// on unrecoverable error or stream end: stop permanently.
    /// the while loop avoids wake_by_ref(), state transitions fall through immediately.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stop {
            return Poll::Ready(None);
        }
        let me = self.get_mut();
        while !me.stop {
            if let Some(mut stream) = me.inner_stream.take() {
                match Pin::new(&mut stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
                        if let Some(UpdateOneof::BlockMeta(_)) = msg.update_oneof.as_ref() {
                            if let Some(slot) = extract_slot(&msg) {
                                // checkpoint a few slots behind the last fully built slot.
                                // block_meta can arrive late and events within a slot
                                // arrive in random order, replaying from a couple slots
                                // back guarantees we don't miss data. dedup handles
                                // the duplicates this creates.
                                me.last_checkpoint =
                                    Some(slot.saturating_sub(CHECKPOINT_SLOT_BUFFER));
                            }
                        }

                        if msg.filters.len() == 1 && msg.filters[0] == AUTORECONNECT_FILTER_KEY {
                            me.inner_stream = Some(stream);
                            continue;
                        }

                        me.inner_stream = Some(stream);
                        return Poll::Ready(Some(Ok(msg)));
                    }
                    Poll::Ready(Some(Err(status))) if is_recoverable_status_code(status.code()) => {
                        // If the server's replay buffer has moved past our checkpoint,
                        // clear it so the next reconnect starts from "now" instead of
                        // looping forever on an unavailable slot.
                        if status.code() == Code::OutOfRange {
                            me.last_checkpoint = None;
                        }
                        log::warn!(
                            "stream error: {status}. starting reconnect from slot {:?}",
                            me.last_checkpoint
                        );
                        if me.backoff.max_retries == 0 {
                            me.stop = true;
                            return Poll::Ready(Some(Err(status)));
                        }

                        let dedup_state = stream.state;
                        me.pending_connecting_task = Some(me.make_connection_future(dedup_state));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        me.stop = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        me.stop = true;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => {
                        me.inner_stream = Some(stream);
                        return Poll::Pending;
                    }
                }
            }

            if let Some(mut fut) = me.pending_connecting_task.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(result) => match result {
                        Ok(stream) => {
                            me.inner_stream = Some(stream);
                        }
                        Err(error) => {
                            me.stop = true;
                            return Poll::Ready(Some(Err(Status::internal(format!(
                                "reconnect failed: {error}",
                            )))));
                        }
                    },
                    Poll::Pending => {
                        me.pending_connecting_task = Some(fut);
                        return Poll::Pending;
                    }
                }
            }
            assert!(
                me.inner_stream.is_some() || me.pending_connecting_task.is_some() || me.stop,
                "must have either an active stream, a pending connecting task, or be stopped"
            );
        }

        Poll::Ready(None)
    }
}

/// Returns true if a client error is considered transient and reconnectable.
fn is_recoverable_client_error(err: &GeyserGrpcClientError) -> bool {
    match err {
        GeyserGrpcClientError::TonicStatus(status) => is_recoverable_status_code(status.code()),
        GeyserGrpcClientError::TransportError(_) => true,
    }
}

/// Returns true for gRPC status codes that should trigger reconnect.
const fn is_recoverable_status_code(code: Code) -> bool {
    matches!(
        code,
        Code::Cancelled
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Internal
            | Code::Unavailable
            | Code::DataLoss
            | Code::OutOfRange
    )
}

/// Extracts the slot number from a subscribe update when available.
pub(crate) fn extract_slot(msg: &SubscribeUpdate) -> Option<u64> {
    match msg.update_oneof.as_ref()? {
        UpdateOneof::Account(m) => Some(m.slot),
        UpdateOneof::Slot(m) => Some(m.slot),
        UpdateOneof::Transaction(m) => Some(m.slot),
        UpdateOneof::Block(m) => Some(m.slot),
        UpdateOneof::BlockMeta(m) => Some(m.slot),
        UpdateOneof::Entry(m) => Some(m.slot),
        UpdateOneof::TransactionStatus(m) => Some(m.slot),
        UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        futures::{stream, StreamExt},
        std::{
            collections::VecDeque,
            sync::{Arc, Mutex},
        },
        tonic::Code,
        yellowstone_grpc_proto::prelude::{
            subscribe_update::UpdateOneof, SubscribeUpdatePing, SubscribeUpdateSlot,
        },
    };

    #[derive(Clone)]
    struct MockGrpcConnector {
        plans: Arc<Mutex<VecDeque<ConnectPlan>>>,
        from_slot_calls: Arc<Mutex<Vec<Option<u64>>>>,
    }

    struct ConnectPlan {
        expected_from_slot: Option<Option<u64>>,
        result: Result<Vec<Result<SubscribeUpdate, Status>>, GeyserGrpcClientError>,
    }

    impl MockGrpcConnector {
        fn new(plans: Vec<ConnectPlan>) -> Self {
            Self {
                plans: Arc::new(Mutex::new(plans.into())),
                from_slot_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<Option<u64>> {
            self.from_slot_calls
                .lock()
                .expect("calls mutex poisoned")
                .clone()
        }
    }

    impl GrpcConnector for MockGrpcConnector {
        type Stream = futures::stream::BoxStream<'static, Result<SubscribeUpdate, Status>>;
        type ConnectError = GeyserGrpcClientError;
        type ConnectFuture = Pin<
            Box<dyn Future<Output = Result<Self::Stream, Self::ConnectError>> + Send + 'static>,
        >;

        fn connect(
            &self,
            _request: Arc<SubscribeRequest>,
            from_slot: Option<u64>,
        ) -> Self::ConnectFuture {
            let mut plans = self.plans.lock().expect("plans mutex poisoned");
            let plan = plans
                .pop_front()
                .expect("unexpected connect call without a test plan");
            drop(plans);

            self.from_slot_calls
                .lock()
                .expect("calls mutex poisoned")
                .push(from_slot);

            if let Some(expected_from_slot) = plan.expected_from_slot {
                assert_eq!(
                    from_slot, expected_from_slot,
                    "unexpected reconnect from_slot"
                );
            }

            Box::pin(async move {
                match plan.result {
                    Ok(items) => Ok(stream::iter(items).boxed()),
                    Err(err) => Err(err),
                }
            })
        }
    }

    fn backoff_with_retries(max_retries: u32) -> Backoff {
        Backoff::new(Duration::from_millis(0), 1.0, max_retries)
    }

    fn request_state(request: SubscribeRequest) -> Arc<ArcSwap<SubscribeRequest>> {
        Arc::new(ArcSwap::new(Arc::new(request)))
    }

    #[test]
    fn test_backoff_default() {
        let backoff = Backoff::default();
        assert_eq!(backoff.initial_interval, Duration::from_millis(10));
        assert_eq!(backoff.multiplier, 2.0);
        assert_eq!(backoff.max_retries, 3);
    }

    #[test]
    fn test_backoff_instance_exhaustion() {
        let backoff = Backoff::new(Duration::from_millis(100), 2.0, 3);
        let mut instance = backoff.instance();

        assert!(!instance.exhausted());
        instance.attempts = 3;
        assert!(instance.exhausted());
    }

    #[test]
    fn test_backoff_instance_starts_from_initial() {
        let backoff = Backoff::new(Duration::from_millis(100), 2.0, 3);
        let instance = backoff.instance();
        assert_eq!(instance.attempts, 0);
        assert_eq!(instance.current_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_backoff_advance() {
        let backoff = Backoff::new(Duration::from_millis(100), 2.0, 5);
        let mut instance = backoff.instance();
        assert_eq!(instance.current_interval, Duration::from_millis(100));

        instance.advance();
        assert_eq!(instance.attempts, 1);
        assert_eq!(instance.current_interval, Duration::from_millis(200));

        instance.advance();
        assert_eq!(instance.attempts, 2);
        assert_eq!(instance.current_interval, Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_advance_unbounded_growth() {
        let backoff = Backoff::new(Duration::from_millis(500), 2.0, 10);
        let mut instance = backoff.instance();
        instance.advance(); // 1s
        instance.advance(); // 2s
        assert_eq!(instance.current_interval, Duration::from_secs(2));
    }

    #[test]
    fn test_backoff_iterator_yields_expected_intervals() {
        let backoff = Backoff::new(Duration::from_millis(100), 2.0, 3);
        let mut instance = backoff.instance();

        assert_eq!(instance.next(), Some(Duration::from_millis(100)));
        assert_eq!(instance.next(), Some(Duration::from_millis(200)));
        assert_eq!(instance.next(), Some(Duration::from_millis(400)));
        assert_eq!(instance.next(), None);
    }

    #[test]
    fn test_backoff_iterator_stops_after_max_retries() {
        let backoff = Backoff::new(Duration::from_millis(100), 2.0, 2);
        let mut instance = backoff.instance();

        assert_eq!(instance.next(), Some(Duration::from_millis(100)));
        assert_eq!(instance.next(), Some(Duration::from_millis(200)));
        assert_eq!(instance.next(), None);
        assert_eq!(instance.next(), None);
    }

    #[tokio::test]
    async fn test_backoff_retry_eventually_succeeds() {
        let backoff = Backoff::new(Duration::from_millis(0), 2.0, 5);
        let mut calls = 0;

        let result = backoff
            .retry(|| {
                let current = calls;
                calls += 1;
                async move {
                    if current < 2 {
                        Err(ErrorCategory::Retryable("transient"))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result, Ok(42));
        assert_eq!(calls, 3);
    }

    #[tokio::test]
    async fn test_backoff_retry_exhausted_returns_last_error() {
        let backoff = Backoff::new(Duration::from_millis(0), 2.0, 2);
        let mut calls = 0;

        let result = backoff
            .retry(|| {
                calls += 1;
                async { Err::<(), _>(ErrorCategory::Retryable("still failing")) }
            })
            .await;

        assert_eq!(result, Err("still failing"));
        assert_eq!(calls, 3);
    }

    #[tokio::test]
    async fn test_backoff_retry_with_zero_retries_does_not_retry() {
        let backoff = Backoff::new(Duration::from_millis(0), 2.0, 0);
        let mut calls = 0;

        let result = backoff
            .retry(|| {
                calls += 1;
                async { Err::<(), _>(ErrorCategory::Retryable("still failing")) }
            })
            .await;

        assert_eq!(result, Err("still failing"));
        assert_eq!(calls, 1);
    }

    #[test]
    fn test_should_reconnect() {
        assert!(is_recoverable_status_code(Code::Unavailable));
        assert!(is_recoverable_status_code(Code::Internal));
        assert!(is_recoverable_status_code(Code::Unknown));
        assert!(is_recoverable_status_code(Code::Cancelled));
        assert!(is_recoverable_status_code(Code::DeadlineExceeded));
        assert!(is_recoverable_status_code(Code::ResourceExhausted));
        assert!(is_recoverable_status_code(Code::Aborted));
        assert!(is_recoverable_status_code(Code::DataLoss));
        assert!(is_recoverable_status_code(Code::OutOfRange));

        assert!(!is_recoverable_status_code(Code::InvalidArgument));
        assert!(!is_recoverable_status_code(Code::NotFound));
        assert!(!is_recoverable_status_code(Code::PermissionDenied));
        assert!(!is_recoverable_status_code(Code::Unauthenticated));
        assert!(!is_recoverable_status_code(Code::Unimplemented));
        assert!(!is_recoverable_status_code(Code::FailedPrecondition));
    }

    #[test]
    fn test_extract_slot_from_slot_update() {
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 42,
                parent: None,
                status: 0,
                dead_error: None,
            })),
            created_at: None,
        };
        assert_eq!(extract_slot(&msg), Some(42));
    }

    #[test]
    fn test_extract_slot_from_ping() {
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        };
        assert_eq!(extract_slot(&msg), None);
    }

    #[test]
    fn test_extract_slot_none() {
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: None,
            created_at: None,
        };
        assert_eq!(extract_slot(&msg), None);
    }

    fn make_slot_msg(slot: u64, status: i32) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent: None,
                status,
                dead_error: None,
            })),
            created_at: None,
        }
    }

    fn make_block_meta_msg(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::BlockMeta(
                yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta {
                    slot,
                    blockhash: String::new(),
                    rewards: None,
                    block_time: None,
                    block_height: None,
                    parent_slot: slot.saturating_sub(1),
                    parent_blockhash: String::new(),
                    executed_transaction_count: 0,
                    entries_count: 0,
                },
            )),
            created_at: None,
        }
    }

    #[test]
    fn test_dedup_record_and_detect() {
        let mut dedup = DedupState::default();
        let msg = make_slot_msg(100, 0);

        assert!(!dedup.is_duplicate(&msg));
        dedup.record(&msg);
        assert!(dedup.is_duplicate(&msg));
    }

    #[test]
    fn test_dedup_different_slots_not_duplicate() {
        let mut dedup = DedupState::default();
        let msg1 = make_slot_msg(100, 0);
        let msg2 = make_slot_msg(101, 0);

        dedup.record(&msg1);
        assert!(!dedup.is_duplicate(&msg2));
    }

    #[test]
    fn test_dedup_ping_ignored() {
        let mut dedup = DedupState::default();
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        };

        dedup.record(&msg);
        assert!(!dedup.is_duplicate(&msg));
    }

    #[test]
    fn test_dedup_clear() {
        let mut dedup = DedupState::default();
        let msg = make_slot_msg(100, 0);

        dedup.record(&msg);
        assert!(dedup.is_duplicate(&msg));
        dedup.clear();
        assert!(!dedup.is_duplicate(&msg));
    }

    #[test]
    fn test_dedup_same_slot_different_status() {
        let mut dedup = DedupState::default();
        let msg1 = make_slot_msg(100, 0);
        let msg2 = make_slot_msg(100, 1);

        dedup.record(&msg1);
        assert!(!dedup.is_duplicate(&msg2));
    }

    #[test]
    fn test_dedup_prune() {
        let mut dedup = DedupState::with_slot_retention(3);

        dedup.record(&make_slot_msg(100, 0));
        dedup.record(&make_slot_msg(101, 0));
        dedup.record(&make_slot_msg(102, 0));
        assert!(dedup.is_duplicate(&make_slot_msg(100, 0)));

        dedup.record(&make_slot_msg(103, 0));
        assert!(!dedup.is_duplicate(&make_slot_msg(100, 0)));
        assert!(dedup.is_duplicate(&make_slot_msg(101, 0)));
    }

    #[test]
    fn test_dedup_slot_status_moves_to_processed_on_blockmeta() {
        let mut dedup = DedupState::default();

        dedup.record(&make_slot_msg(200, 0));
        dedup.record(&make_block_meta_msg(200));

        assert!(dedup.is_duplicate(&make_slot_msg(200, 0)));
        assert!(!dedup.is_duplicate(&make_slot_msg(200, 1)));
    }

    #[tokio::test]
    async fn test_autoreconnect_recovers_from_recoverable_stream_error() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(None),
            result: Ok(vec![Ok(make_slot_msg(42, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            DedupStream::new(initial, DedupState::default()),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        );

        let next = auto.next().await;
        let slot = next
            .expect("expected one item")
            .expect("expected successful reconnect message");
        assert_eq!(extract_slot(&slot), Some(42));
        assert_eq!(connector.calls(), vec![None]);
    }

    #[tokio::test]
    async fn test_autoreconnect_no_reconnect_when_max_retries_zero() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();
        let connector = MockGrpcConnector::new(vec![]);

        let mut auto = AutoReconnect::new(
            DedupStream::new(initial, DedupState::default()),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(0),
        );

        let first = auto.next().await.expect("expected one item");
        assert!(first.is_err());
        assert_eq!(
            first.expect_err("expected recoverable error").code(),
            Code::Unavailable
        );
        assert_eq!(connector.calls().len(), 0);
    }

    #[tokio::test]
    async fn test_autoreconnect_passes_checkpoint_as_from_slot() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(Some(77)),
            result: Ok(vec![Ok(make_slot_msg(90, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            DedupStream::new(initial, DedupState::default()),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        );
        auto.last_checkpoint = Some(77);

        let msg = auto
            .next()
            .await
            .expect("expected one item")
            .expect("expected message after reconnect");
        assert_eq!(extract_slot(&msg), Some(90));
        assert_eq!(connector.calls(), vec![Some(77)]);
    }

    #[tokio::test]
    async fn test_autoreconnect_dedup_survives_reconnect() {
        let initial = stream::iter(vec![
            Ok(make_slot_msg(100, 0)),
            Err(Status::unavailable("disconnect")),
        ])
        .boxed();

        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: None,
            result: Ok(vec![
                Ok(make_slot_msg(100, 0)), // duplicate — should be filtered
                Ok(make_slot_msg(101, 0)), // new — should pass through
            ]),
        }]);

        let mut auto = AutoReconnect::new(
            DedupStream::new(initial, DedupState::default()),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        );

        let msg1 = auto
            .next()
            .await
            .expect("expected item")
            .expect("expected ok");
        assert_eq!(extract_slot(&msg1), Some(100));

        let msg2 = auto
            .next()
            .await
            .expect("expected item")
            .expect("expected ok");
        assert_eq!(extract_slot(&msg2), Some(101));
    }

    #[tokio::test]
    async fn test_autoreconnect_checkpoint_buffer() {
        let block_meta_msg = make_block_meta_msg(100);

        let initial = stream::iter(vec![
            Ok(block_meta_msg),
            Err(Status::unavailable("disconnect")),
        ])
        .boxed();

        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(Some(100 - CHECKPOINT_SLOT_BUFFER)),
            result: Ok(vec![Ok(make_slot_msg(105, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            DedupStream::new(initial, DedupState::default()),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        );

        let msg1 = auto
            .next()
            .await
            .expect("expected item")
            .expect("expected ok");
        assert_eq!(extract_slot(&msg1), Some(100));

        let msg2 = auto
            .next()
            .await
            .expect("expected item")
            .expect("expected ok");
        assert_eq!(extract_slot(&msg2), Some(105));
        assert_eq!(connector.calls(), vec![Some(100 - CHECKPOINT_SLOT_BUFFER)]);
    }

    #[tokio::test]
    async fn test_autoreconnect_unrecoverable_error_stops_stream() {
        let initial = stream::iter(vec![Err(Status::invalid_argument("bad filter"))]).boxed();

        let connector = MockGrpcConnector::new(vec![]);

        let mut auto = AutoReconnect::new(
            DedupStream::new(initial, DedupState::default()),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        );

        let result = auto.next().await.expect("expected item");
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").code(),
            Code::InvalidArgument
        );

        assert!(auto.next().await.is_none());
        assert_eq!(connector.calls().len(), 0);
    }
}
