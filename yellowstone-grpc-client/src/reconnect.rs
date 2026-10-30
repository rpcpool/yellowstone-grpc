use {
    crate::{GeyserGrpcClientError, InterceptorXToken, ReconnectConfig, ReconnectPolicy},
    arc_swap::ArcSwap,
    futures::{channel::mpsc, sink::SinkExt, stream::Stream},
    std::{
        error::Error,
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
        prelude::{subscribe_update::UpdateOneof, SlotStatus, SubscribeRequest, SubscribeUpdate},
    },
};

pub const AUTORECONNECT_FILTER_KEY: &str = "__autoreconnect";

type ConnectFuture<S> =
    Pin<Box<dyn Future<Output = Result<S, GeyserGrpcClientError>> + Send + 'static>>;

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
    // 300 ms doubling over 6 retries waits about 19 s in total before giving up.
    const fn default_initial_interval() -> Duration {
        Duration::from_millis(300)
    }

    const fn default_multiplier() -> f64 {
        2.0
    }

    const fn default_max_retries() -> u32 {
        6
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

/// Defines how a Stream opens a new subscription after a disconnect.
///
/// Implementors provide [`connect`](Self::connect), which takes the latest request and returns
/// a future that resolves to a fresh update stream. [`TonicGrpcConnector`] implements it over gRPC;
/// tests implement it with scripted streams.
pub trait GrpcConnector: Clone + Send + Sync + 'static {
    type Stream: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static;

    type ConnectError: std::error::Error + Send + Sync + 'static;

    type ConnectFuture: Future<Output = Result<Self::Stream, Self::ConnectError>> + Send + 'static;

    /// Subscribes with the latest request, replaying from `from_slot` when it is set;
    /// resolves to a fresh stream or an error.
    fn connect(
        &self,
        request: Arc<SubscribeRequest>,
        from_slot: Option<u64>,
    ) -> Self::ConnectFuture;
}

/// The real [`GrpcConnector`]: it connects to a gRPC server with tonic.
///
/// Each call to `connect`:
/// 1. opens a new channel to the endpoint, retrying with [`Backoff`] if that fails;
/// 2. subscribes with the latest request, replaying from the given `from_slot`;
/// 3. points the user's `SubscribeRequestSink` at the new connection, so filter changes the user
///    sends after a reconnect reach the new server instead of the dead one.
#[derive(Clone)]
pub struct TonicGrpcConnector {
    backoff: Backoff,
    request_sink: Arc<Mutex<mpsc::Sender<SubscribeRequest>>>,
    endpoint: Endpoint,
    x_token: Option<AsciiMetadataValue>,
    options: TonicGeyserClientOptions,
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
            options,
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
        let options = self.options.clone();
        let base_request = (*request).clone();

        let fut = backoff.retry(move || {
            let endpoint = endpoint.clone();
            let request_sink = Arc::clone(&request_sink);
            let x_token = x_token.clone();
            let mut request = base_request.clone();
            let options = options.clone();
            async move {
                request.from_slot = from_slot;

                let channel = endpoint
                    .connect()
                    .await
                    .map_err(GeyserGrpcClientError::TransportError)
                    .map_err(ErrorCategory::Retryable)?;

                let interceptor = InterceptorXToken {
                    x_token,
                    x_request_snapshot: options.x_request_snapshot,
                };

                let mut geyser =
                    GeyserClient::with_interceptor(channel.clone(), interceptor.clone());
                if let Some(encoding) = options.send_compressed {
                    geyser = geyser.send_compressed(encoding);
                }
                if let Some(encoding) = options.accept_compressed {
                    geyser = geyser.accept_compressed(encoding);
                }
                if let Some(limit) = options.max_decoding_message_size {
                    geyser = geyser.max_decoding_message_size(limit);
                }
                if let Some(limit) = options.max_encoding_message_size {
                    geyser = geyser.max_encoding_message_size(limit);
                }

                let (mut subscribe_tx, subscribe_rx) = mpsc::channel(1000);
                subscribe_tx
                    .send(request)
                    .await
                    .expect("channel cannot be disconnected or full at this point");

                let response = geyser
                    .subscribe(tonic::Request::new(subscribe_rx))
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

/// An item yielded by [`ReconnectGeyserStream`].
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "Keep subscription updates inline to avoid an allocation per message"
)]
pub enum ReconnectEvent {
    Update(SubscribeUpdate),

    /// The connection dropped. Slots after `from_slot` up to and including `to_slot` may be
    /// incomplete. The new connection replays them whole when the server allows it; dedup
    /// any complete bank you already hold. If it reconnects live instead, the slots produced
    /// during the outage are not in the range either.
    Missing {
        /// Last finalized slot before the disconnect: every bank on the finalized chain at or
        /// below it is complete. `None` if nothing was finalized yet.
        from_slot: Option<u64>,
        /// Highest slot seen before the disconnect.
        to_slot: u64,
    },
}

/// A subscription stream that reconnects by itself.
///
/// # Behaviour
///
/// - **Reconnects** on a recoverable error or when the stream ends.
/// - **Replays** from the slot after the last finalized one, until it joins the live stream,
///   under [`ReconnectPolicy::Replay`] (the default). If the server refuses the replay, or the
///   policy is [`ReconnectPolicy::NoReplay`], it reconnects at the latest slot.
/// - **Reports the gap** with [`ReconnectEvent::Missing`] as soon as the new connection is up,
///   before any of its updates.
/// - **Stops** on any other error, or when a reconnect fails.
///
/// # Example
///
/// ```no_run
/// use {
///     futures::StreamExt,
///     tonic::Status,
///     yellowstone_grpc_client::{GeyserGrpcClient, ReconnectEvent, ReconnectGeyserStream},
///     yellowstone_grpc_proto::prelude::SubscribeRequest,
/// };
///
/// async fn consume(mut stream: ReconnectGeyserStream) -> Result<(), Status> {
///     while let Some(event) = stream.next().await {
///         match event? {
///             ReconnectEvent::Update(update) => println!("{update:?}"),
///
///             // Slots after `from_slot`, up to and including `to_slot`, may be incomplete.
///             ReconnectEvent::Missing { from_slot, to_slot } => {
///                 println!("incomplete: after {from_slot:?} through {to_slot}")
///             }
///         }
///     }
///     Ok(())
/// }
///
/// // Connect, subscribe, and hand the stream to `consume`.
/// async fn run(request: SubscribeRequest) -> Result<(), Box<dyn std::error::Error>> {
///     let mut client = GeyserGrpcClient::build_from_static("http://127.0.0.1:10000")
///         .connect()
///         .await?;
///     let (_sink, stream) = client.subscribe_with_reconnect(Some(request)).await?;
///     consume(stream).await?;
///     Ok(())
/// }
/// ```
pub struct ReconnectGeyserStream<
    GrpcStream = Streaming<SubscribeUpdate>,
    Connector = TonicGrpcConnector,
> {
    inner_stream: Option<GrpcStream>,
    request: Arc<ArcSwap<SubscribeRequest>>,
    // Last finalized slot; persists across connections for `Missing` and the replay start.
    last_checkpoint: Option<u64>,
    // Highest slot seen in any update; the end of the `Missing` range.
    highest_slot: Option<u64>,
    // The current connection asked for a replay and has delivered nothing yet.
    replaying: bool,
    // The last replay was refused, so the next reconnect goes live.
    replay_refused: bool,
    stop: bool,
    connector: Connector,
    backoff: Backoff,
    policy: ReconnectPolicy,
    pending_connecting_task: Option<ConnectFuture<GrpcStream>>,
    // Delay and budget for the next reconnect; reset when a full block (BlockMeta) arrives.
    retries: BackoffInstance,
}

impl<S, Connector> ReconnectGeyserStream<S, Connector>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
    Connector: GrpcConnector<Stream = S, ConnectError = GeyserGrpcClientError>,
{
    pub fn new(
        stream: S,
        connector: Connector,
        request: Arc<ArcSwap<SubscribeRequest>>,
        config: ReconnectConfig,
    ) -> Self {
        let ReconnectConfig { backoff, policy } = config;
        Self {
            inner_stream: Some(stream),
            request,
            last_checkpoint: None,
            highest_slot: None,
            replaying: false,
            replay_refused: false,
            stop: false,
            connector,
            retries: backoff.instance(),
            backoff,
            policy,
            pending_connecting_task: None,
        }
    }

    /// Waits for the next backoff delay, then reconnects; `None` when the budget is spent.
    fn reconnect(&mut self) -> Option<ConnectFuture<S>> {
        let delay = self.retries.next()?;
        let from_slot = self
            .last_checkpoint
            .filter(|_| self.policy == ReconnectPolicy::Replay && !self.replay_refused)
            .map(|slot| slot + 1);
        self.replaying = from_slot.is_some();
        let connector = self.connector.clone();
        let request = self.request.load_full();
        Some(Box::pin(async move {
            tokio::time::sleep(delay).await;
            connector.connect(request, from_slot).await
        }))
    }
}

impl<S, Connector> ReconnectGeyserStream<S, Connector> {
    fn deliver(&mut self, mut update: SubscribeUpdate) -> Option<ReconnectEvent> {
        self.highest_slot = self.highest_slot.max(extract_slot(&update));
        match update.update_oneof.as_ref() {
            // A finalized slot never gets another bank, and every fork below it is decided.
            Some(UpdateOneof::Slot(slot)) if slot.status == SlotStatus::SlotFinalized as i32 => {
                self.last_checkpoint = self.last_checkpoint.max(Some(slot.slot));
            }
            // A full block proves the connection makes progress.
            Some(UpdateOneof::BlockMeta(_)) => self.retries = self.backoff.instance(),
            _ => {}
        }
        visible_update(&mut update).then_some(ReconnectEvent::Update(update))
    }

    /// Ends the current connection: schedules a reconnect, or stops with `error`.
    fn on_drop(&mut self, error: Status) -> Option<Status>
    where
        S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
        Connector: GrpcConnector<Stream = S, ConnectError = GeyserGrpcClientError>,
    {
        // A replay that fails before any update was refused; retry live.
        self.replay_refused = self.replaying;
        match self.reconnect() {
            Some(reconnect) => {
                log::warn!("stream error: {error}. reconnecting");
                self.pending_connecting_task = Some(reconnect);
                None
            }
            None => {
                self.stop = true;
                Some(error)
            }
        }
    }
}

impl<S, Connector> Stream for ReconnectGeyserStream<S, Connector>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
    Connector: GrpcConnector<Stream = S, ConnectError = GeyserGrpcClientError> + Unpin,
{
    type Item = Result<ReconnectEvent, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        while !me.stop {
            if let Some(mut stream) = me.inner_stream.take() {
                match Pin::new(&mut stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
                        me.replaying = false;
                        me.inner_stream = Some(stream);
                        if let Some(event) = me.deliver(msg) {
                            return Poll::Ready(Some(Ok(event)));
                        }
                        continue;
                    }
                    Poll::Ready(Some(Err(status)))
                        if me.replaying || is_recoverable_status_code(status.code()) =>
                    {
                        if let Some(error) = me.on_drop(status) {
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                    Poll::Ready(Some(Err(e))) => {
                        me.stop = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        let closed = Status::unavailable(
                            "subscription closed and reconnect retries are exhausted or disabled",
                        );
                        if let Some(error) = me.on_drop(closed) {
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                    Poll::Pending => {
                        me.inner_stream = Some(stream);
                        return Poll::Pending;
                    }
                }
            }

            if let Some(mut fut) = me.pending_connecting_task.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        me.inner_stream = Some(stream);
                        // Built from what the old connection saw, before any new update.
                        if let Some(to_slot) = me.highest_slot {
                            return Poll::Ready(Some(Ok(ReconnectEvent::Missing {
                                from_slot: me.last_checkpoint,
                                to_slot,
                            })));
                        }
                    }
                    Poll::Ready(Err(error)) => {
                        me.stop = true;
                        let status = match error {
                            GeyserGrpcClientError::TonicStatus(status) => status,
                            other => Status::unavailable(format!("reconnect failed: {other}")),
                        };
                        return Poll::Ready(Some(Err(status)));
                    }
                    Poll::Pending => {
                        me.pending_connecting_task = Some(fut);
                        return Poll::Pending;
                    }
                }
            }
        }
        Poll::Ready(None)
    }
}

/// Returns true if a client error is considered transient and reconnectable.
fn is_recoverable_client_error(err: &GeyserGrpcClientError) -> bool {
    match err {
        GeyserGrpcClientError::TonicStatus(status) => {
            // Transport errors can be wrapped inside a Status
            if let Some(source) = status.source() {
                if source.downcast_ref::<tonic::transport::Error>().is_some() {
                    return true;
                }
            }
            is_recoverable_status_code(status.code())
        }
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
        UpdateOneof::EntryUpdateParent(m) => Some(m.slot),
        UpdateOneof::BlockFooter(m) => Some(m.slot),
        UpdateOneof::TransactionStatus(m) => Some(m.slot),
        UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => None,
    }
}

// Hide control-only messages only after the stream has tracked their slots.
pub(crate) fn visible_update(update: &mut SubscribeUpdate) -> bool {
    let internal_only = !update.filters.is_empty()
        && update
            .filters
            .iter()
            .all(|filter| filter == AUTORECONNECT_FILTER_KEY);
    update
        .filters
        .retain(|filter| filter != AUTORECONNECT_FILTER_KEY);
    !internal_only
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
        yellowstone_grpc_proto::{
            geyser::{SubscribeUpdateAccount, SubscribeUpdateAccountInfo},
            prelude::{subscribe_update::UpdateOneof, SubscribeUpdatePing, SubscribeUpdateSlot},
        },
    };

    #[derive(Clone)]
    struct MockGrpcConnector {
        plans: Arc<Mutex<VecDeque<ConnectPlan>>>,
        from_slots: Arc<Mutex<Vec<Option<u64>>>>,
    }

    struct ConnectPlan {
        result: Result<Vec<Result<SubscribeUpdate, Status>>, GeyserGrpcClientError>,
    }

    impl MockGrpcConnector {
        fn new(plans: Vec<ConnectPlan>) -> Self {
            Self {
                plans: Arc::new(Mutex::new(plans.into())),
                from_slots: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> usize {
            self.requested_from_slots().len()
        }

        /// The `from_slot` each connect asked for, in order.
        fn requested_from_slots(&self) -> Vec<Option<u64>> {
            self.from_slots
                .lock()
                .expect("from_slots mutex poisoned")
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

            self.from_slots
                .lock()
                .expect("from_slots mutex poisoned")
                .push(from_slot);

            Box::pin(async move {
                match plan.result {
                    Ok(items) => Ok(stream::iter(items).boxed()),
                    Err(err) => Err(err),
                }
            })
        }
    }

    fn config_with_retries(max_retries: u32) -> ReconnectConfig {
        ReconnectConfig::default().with_backoff(backoff_with_retries(max_retries))
    }

    fn backoff_with_retries(max_retries: u32) -> Backoff {
        Backoff::new(Duration::from_millis(0), 1.0, max_retries)
    }

    fn request_state(request: SubscribeRequest) -> Arc<ArcSwap<SubscribeRequest>> {
        Arc::new(ArcSwap::new(Arc::new(request)))
    }

    fn make_account_msg(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: vec![1; 32],
                    lamports: 100,
                    owner: vec![0; 32],
                    executable: false,
                    rent_epoch: 0,
                    data: vec![].into(),
                    write_version: 1,
                    txn_signature: Some(vec![0; 64]),
                }),
                slot,
                is_startup: false,
                bank_id: Some(slot),
            })),
            created_at: None,
        }
    }

    #[test]
    fn test_backoff_default() {
        let backoff = Backoff::default();
        assert_eq!(backoff.initial_interval, Duration::from_millis(300));
        assert_eq!(backoff.multiplier, 2.0);
        assert_eq!(backoff.max_retries, 6);
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
        assert!(!is_recoverable_status_code(Code::OutOfRange));

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
                bank_id: Some(42),
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
                bank_id: Some(slot),
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
                    bank_id: slot,
                },
            )),
            created_at: None,
        }
    }

    #[tokio::test]
    async fn test_reconnect_stream_no_reconnect_when_max_retries_zero() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();
        let connector = MockGrpcConnector::new(vec![]);

        let mut auto = ReconnectGeyserStream::new(
            initial,
            connector.clone(),
            request_state(SubscribeRequest::default()),
            config_with_retries(0),
        );

        let first = auto.next().await.expect("expected one item");
        assert!(first.is_err());
        assert_eq!(
            first.expect_err("expected recoverable error").code(),
            Code::Unavailable
        );
        assert_eq!(connector.calls(), 0);
    }

    #[test]
    fn test_status_codes_recoverable() {
        for code in [
            Code::Unavailable,
            Code::Aborted,
            Code::Internal,
            Code::Unknown,
        ] {
            let status = Status::new(code, "test");
            let err = GeyserGrpcClientError::TonicStatus(status);
            assert!(
                is_recoverable_client_error(&err),
                "expected {:?} to be recoverable",
                code
            );
        }
    }

    #[test]
    fn test_status_codes_unrecoverable() {
        for code in [
            Code::InvalidArgument,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::NotFound,
        ] {
            let status = Status::new(code, "test");
            let err = GeyserGrpcClientError::TonicStatus(status);
            assert!(
                !is_recoverable_client_error(&err),
                "expected {:?} to be unrecoverable",
                code
            );
        }
    }

    #[tokio::test]
    async fn test_reconnect_stream_unrecoverable_error_stops_stream() {
        let initial = stream::iter(vec![Err(Status::invalid_argument("bad filter"))]).boxed();

        let connector = MockGrpcConnector::new(vec![]);

        let mut auto = ReconnectGeyserStream::new(
            initial,
            connector.clone(),
            request_state(SubscribeRequest::default()),
            config_with_retries(1),
        );

        let result = auto.next().await.expect("expected item");
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").code(),
            Code::InvalidArgument
        );

        assert!(auto.next().await.is_none());
        assert_eq!(connector.calls(), 0);
    }

    #[tokio::test]
    async fn test_reconnect_exhausts_retries_then_stops() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();

        // Connector fails every reconnect attempt
        let connector = MockGrpcConnector::new(vec![
            ConnectPlan {
                result: Err(GeyserGrpcClientError::TonicStatus(Status::unavailable(
                    "still down",
                ))),
            },
            ConnectPlan {
                result: Err(GeyserGrpcClientError::TonicStatus(Status::unavailable(
                    "still down",
                ))),
            },
        ]);

        let mut auto = ReconnectGeyserStream::new(
            initial,
            connector.clone(),
            request_state(SubscribeRequest::default()),
            config_with_retries(2), // 2 retries allowed
        );

        // Should get error after retries exhausted
        let result = auto.next().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_err());

        // Stream should end
        assert!(auto.next().await.is_none());
    }

    #[tokio::test]
    async fn live_reconnect_respects_disabled_retries() {
        for disconnect in [Some(Status::unavailable("partial bank")), None] {
            let mut initial = vec![Ok(make_account_msg(100))];
            if let Some(error) = disconnect {
                initial.push(Err(error));
            }
            let connector = MockGrpcConnector::new(vec![]);
            let mut stream = ReconnectGeyserStream::new(
                stream::iter(initial).boxed(),
                connector.clone(),
                request_state(SubscribeRequest::default()),
                config_with_retries(0),
            );
            assert!(stream.next().await.unwrap().is_ok());
            assert_eq!(
                stream.next().await.unwrap().unwrap_err().code(),
                Code::Unavailable
            );
            assert!(stream.next().await.is_none());
            assert!(connector.calls() == 0);
        }
    }

    fn finalized(slot: u64) -> SubscribeUpdate {
        make_slot_msg(
            slot,
            yellowstone_grpc_proto::prelude::SlotStatus::SlotFinalized as i32,
        )
    }

    fn hidden(mut update: SubscribeUpdate) -> SubscribeUpdate {
        update.filters = vec![AUTORECONNECT_FILTER_KEY.into()];
        update
    }

    fn reconnect_stream(
        first: Vec<Result<SubscribeUpdate, Status>>,
        plans: Vec<ConnectPlan>,
    ) -> ReconnectGeyserStream<
        futures::stream::BoxStream<'static, Result<SubscribeUpdate, Status>>,
        MockGrpcConnector,
    > {
        ReconnectGeyserStream::new(
            stream::iter(first).boxed(),
            MockGrpcConnector::new(plans),
            request_state(SubscribeRequest::default()),
            config_with_retries(2),
        )
    }

    async fn next_update<S, C>(stream: &mut ReconnectGeyserStream<S, C>) -> SubscribeUpdate
    where
        ReconnectGeyserStream<S, C>: Stream<Item = Result<ReconnectEvent, Status>> + Unpin,
    {
        match stream.next().await.unwrap().unwrap() {
            ReconnectEvent::Update(update) => update,
            event => panic!("expected an update, got {event:?}"),
        }
    }

    #[tokio::test]
    async fn internal_controls_are_hidden() {
        let mut stream = reconnect_stream(
            vec![
                Ok(hidden(make_block_meta_msg(41))),
                Ok(make_account_msg(42)),
            ],
            vec![],
        );
        assert_eq!(next_update(&mut stream).await, make_account_msg(42));
    }

    fn missing(from_slot: Option<u64>, to_slot: u64) -> ReconnectEvent {
        ReconnectEvent::Missing { from_slot, to_slot }
    }

    fn assert_missing(event: ReconnectEvent, from: Option<u64>, to: u64) {
        assert!(
            matches!(event, ReconnectEvent::Missing { from_slot, to_slot } if from_slot == from && to_slot == to),
            "expected {:?}, got {event:?}",
            missing(from, to)
        );
    }

    #[tokio::test]
    async fn missing_comes_first_and_the_replay_starts_after_the_last_finalized_slot() {
        for drop in [Some(Status::unavailable("disconnect")), None] {
            let mut first = vec![Ok(finalized(41)), Ok(make_account_msg(42))];
            first.extend(drop.map(Err));
            let mut stream = reconnect_stream(
                first,
                vec![ConnectPlan {
                    result: Ok(vec![Ok(make_account_msg(42)), Ok(make_account_msg(45))]),
                }],
            );
            for _ in 0..2 {
                next_update(&mut stream).await;
            }
            // Built from the old connection, before any replayed update.
            assert_missing(stream.next().await.unwrap().unwrap(), Some(41), 42);
            assert_eq!(next_update(&mut stream).await, make_account_msg(42));
            assert_eq!(next_update(&mut stream).await, make_account_msg(45));
            assert_eq!(stream.connector.requested_from_slots(), [Some(42)]);
        }
    }

    #[tokio::test]
    async fn without_a_finalized_slot_the_reconnect_is_live() {
        let mut stream = reconnect_stream(
            vec![
                Ok(make_account_msg(42)),
                Err(Status::unavailable("disconnect")),
            ],
            vec![ConnectPlan {
                result: Ok(vec![Ok(make_account_msg(45))]),
            }],
        );
        next_update(&mut stream).await;
        assert_missing(stream.next().await.unwrap().unwrap(), None, 42);
        assert_eq!(next_update(&mut stream).await, make_account_msg(45));
        assert_eq!(stream.connector.requested_from_slots(), [None]);
    }

    #[tokio::test]
    async fn no_replay_policy_reconnects_live_and_still_reports_the_gap() {
        let mut stream = ReconnectGeyserStream::new(
            stream::iter(vec![
                Ok(finalized(41)),
                Ok(make_account_msg(42)),
                Err(Status::unavailable("disconnect")),
            ])
            .boxed(),
            MockGrpcConnector::new(vec![ConnectPlan {
                result: Ok(vec![Ok(make_account_msg(50))]),
            }]),
            request_state(SubscribeRequest::default()),
            config_with_retries(2).with_policy(ReconnectPolicy::NoReplay),
        );
        for _ in 0..2 {
            next_update(&mut stream).await;
        }
        assert_missing(stream.next().await.unwrap().unwrap(), Some(41), 42);
        assert_eq!(next_update(&mut stream).await, make_account_msg(50));
        assert_eq!(stream.connector.requested_from_slots(), [None]);
    }

    #[tokio::test]
    async fn a_refused_replay_is_retried_live() {
        for refusal in [
            Status::out_of_range("broadcast from 42 is not available"),
            Status::internal("from_slot is not supported"),
        ] {
            let mut stream = reconnect_stream(
                vec![
                    Ok(finalized(41)),
                    Ok(make_account_msg(42)),
                    Err(Status::unavailable("disconnect")),
                ],
                vec![
                    ConnectPlan {
                        result: Ok(vec![Err(refusal)]),
                    },
                    ConnectPlan {
                        result: Ok(vec![Ok(make_account_msg(50))]),
                    },
                ],
            );
            for _ in 0..2 {
                next_update(&mut stream).await;
            }
            // One `Missing` per new connection: the refused replay and the live one.
            assert_missing(stream.next().await.unwrap().unwrap(), Some(41), 42);
            assert_missing(stream.next().await.unwrap().unwrap(), Some(41), 42);
            assert_eq!(next_update(&mut stream).await, make_account_msg(50));
            assert_eq!(stream.connector.requested_from_slots(), [Some(42), None]);
        }
    }

    #[tokio::test]
    async fn the_checkpoint_keeps_the_highest_finalized_slot() {
        let mut stream = reconnect_stream(
            vec![
                Ok(finalized(100)),
                Ok(finalized(99)),
                Ok(make_account_msg(101)),
                Err(Status::unavailable("disconnect")),
            ],
            vec![ConnectPlan { result: Ok(vec![]) }],
        );
        for _ in 0..3 {
            next_update(&mut stream).await;
        }
        assert_missing(stream.next().await.unwrap().unwrap(), Some(100), 101);
        assert_eq!(stream.connector.requested_from_slots(), [Some(101)]);
    }

    #[tokio::test]
    async fn a_second_bank_in_a_completed_slot_is_in_the_range() {
        let mut stream = reconnect_stream(
            vec![
                Ok(finalized(100)),
                Ok(make_block_meta_msg(101)),
                Ok(make_account_msg(101)),
                Err(Status::unavailable("disconnect")),
            ],
            vec![ConnectPlan { result: Ok(vec![]) }],
        );
        for _ in 0..3 {
            next_update(&mut stream).await;
        }
        assert_missing(stream.next().await.unwrap().unwrap(), Some(100), 101);
    }

    #[tokio::test]
    async fn no_missing_when_nothing_was_received() {
        let mut stream = reconnect_stream(
            vec![Err(Status::unavailable("disconnect"))],
            vec![ConnectPlan {
                result: Ok(vec![Ok(make_account_msg(42))]),
            }],
        );
        assert_eq!(next_update(&mut stream).await, make_account_msg(42));
    }

    #[tokio::test]
    async fn an_unrecoverable_error_on_a_live_connection_ends_the_stream() {
        let mut stream = reconnect_stream(
            vec![
                Ok(make_account_msg(42)),
                Err(Status::unavailable("disconnect")),
            ],
            vec![ConnectPlan {
                result: Ok(vec![Err(Status::invalid_argument("bad filter"))]),
            }],
        );
        next_update(&mut stream).await;
        assert_missing(stream.next().await.unwrap().unwrap(), None, 42);
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            Code::InvalidArgument
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn connections_that_deliver_nothing_exhaust_the_retry_budget() {
        let mut stream = ReconnectGeyserStream::new(
            stream::iter(vec![
                Ok(make_account_msg(42)),
                Err(Status::unavailable("drop")),
            ])
            .boxed(),
            MockGrpcConnector::new(vec![ConnectPlan {
                result: Ok(vec![Err(Status::unavailable("drop again"))]),
            }]),
            request_state(SubscribeRequest::default()),
            config_with_retries(1),
        );
        next_update(&mut stream).await;
        assert_missing(stream.next().await.unwrap().unwrap(), None, 42);
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            Code::Unavailable
        );
        assert!(stream.next().await.is_none());
        assert_eq!(stream.connector.calls(), 1);
    }

    #[tokio::test]
    async fn flapping_server_without_full_blocks_exhausts_the_retry_budget() {
        // Each connection sends one account and drops; no BlockMeta, so no progress.
        let flap = || ConnectPlan {
            result: Ok(vec![
                Ok(make_account_msg(50)),
                Err(Status::unavailable("flap")),
            ]),
        };
        let mut stream = reconnect_stream(
            vec![Ok(make_account_msg(50)), Err(Status::unavailable("flap"))],
            vec![flap(), flap(), flap()],
        );
        let mut errors = 0;
        while let Some(item) = stream.next().await {
            errors += usize::from(item.is_err());
        }
        assert_eq!(errors, 1, "stops with one error after the budget is spent");
        assert_eq!(stream.connector.calls(), 2, "max_retries is 2");
    }

    #[tokio::test]
    async fn full_blocks_reset_the_retry_budget() {
        // Every connection completes a block before dropping, so reconnects never run out.
        let healthy = |slot| ConnectPlan {
            result: Ok(vec![
                Ok(make_block_meta_msg(slot)),
                Err(Status::unavailable("drop")),
            ]),
        };
        let mut stream = reconnect_stream(
            vec![
                Ok(make_block_meta_msg(10)),
                Err(Status::unavailable("drop")),
            ],
            vec![
                healthy(11),
                healthy(12),
                healthy(13),
                ConnectPlan {
                    result: Ok(vec![
                        Ok(make_block_meta_msg(14)),
                        Err(Status::invalid_argument("end of test")),
                    ]),
                },
            ],
        );
        let mut blocks = 0;
        while let Some(Ok(event)) = stream.next().await {
            blocks += usize::from(matches!(event, ReconnectEvent::Update(_)));
        }
        // Four reconnects with a budget of two: each full block refilled it.
        assert_eq!(blocks, 5);
        assert_eq!(stream.connector.calls(), 4);
    }
}

#[cfg(test)]
mod reconnect_stream_tests {
    use {
        crate::*,
        futures::{FutureExt, StreamExt},
        yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdateAccount},
    };

    fn account(slot: u64, bank_id: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                bank_id: Some(bank_id),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn finalized_status(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![AUTORECONNECT_FILTER_KEY.into()],
            update_oneof: Some(UpdateOneof::Slot(
                yellowstone_grpc_proto::prelude::SubscribeUpdateSlot {
                    slot,
                    status: yellowstone_grpc_proto::prelude::SlotStatus::SlotFinalized as i32,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn metadata(slot: u64, bank_id: u64, blockhash: &str, parent_slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![AUTORECONNECT_FILTER_KEY.into()],
            update_oneof: Some(UpdateOneof::BlockMeta(
                yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta {
                    slot,
                    bank_id,
                    blockhash: blockhash.into(),
                    parent_slot,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn api_rejects_non_processed_commitment_before_connecting() {
        let mut client = GeyserGrpcClient::build_from_static("http://127.0.0.1:1")
            .connect_lazy()
            .unwrap();
        let result = client
            .subscribe_with_reconnect(Some(SubscribeRequest {
                commitment: Some(1),
                ..Default::default()
            }))
            .await;
        assert!(
            matches!(result, Err(GeyserGrpcClientError::TonicStatus(status)) if status.code() == tonic::Code::FailedPrecondition)
        );
    }

    #[tokio::test]
    async fn strict_sink_rejects_replay_before_changing_request_or_sending() {
        let (tx, mut rx) = mpsc::channel(2);
        let shared = Arc::new(ArcSwap::new(Arc::new(SubscribeRequest::default())));
        let mut sink = SubscribeRequestSink {
            verified_recovery: true,
            inner: Arc::new(Mutex::new(tx)),
            shared: Arc::clone(&shared),
        };
        assert!(sink
            .send(SubscribeRequest {
                from_slot: Some(42),
                ..Default::default()
            })
            .await
            .is_err());
        assert!(shared.load().from_slot.is_none());
        assert!(rx.next().now_or_never().is_none());
        sink.send(SubscribeRequest {
            ping: Some(yellowstone_grpc_proto::prelude::SubscribeRequestPing { id: 1 }),
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(rx.next().await.is_some());
    }

    struct SubscribeService {
        updates: Mutex<Option<mpsc::UnboundedReceiver<Result<SubscribeUpdate, Status>>>>,
        replay_requests: Arc<Mutex<Vec<u64>>>,
        accept_replay: bool,
        expected_request: Option<SubscribeRequest>,
    }

    #[tonic::async_trait]
    impl yellowstone_grpc_proto::geyser::geyser_server::Geyser for SubscribeService {
        type SubscribeStream = futures::stream::BoxStream<'static, Result<SubscribeUpdate, Status>>;

        async fn subscribe(
            &self,
            mut request: Request<tonic::Streaming<SubscribeRequest>>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            let request = request.get_mut().message().await?.unwrap();
            if let Some(expected) = &self.expected_request {
                assert_eq!(&request, expected);
            } else {
                assert!(request.blocks_meta.contains_key(AUTORECONNECT_FILTER_KEY));
                assert_eq!(
                    request
                        .slots
                        .get(AUTORECONNECT_FILTER_KEY)
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
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let endpoint = format!("http://{}", listener.local_addr().unwrap());
                    let incoming = futures::stream::unfold(listener, |listener| async move {
                        Some((listener.accept().await.map(|(stream, _)| stream), listener))
                    });
                    let (tx, rx) = mpsc::unbounded();
                    let replay_requests = Arc::new(Mutex::new(Vec::new()));
                    let server = tokio::spawn(
                        tonic::transport::Server::builder()
                            .add_service(
                                yellowstone_grpc_proto::geyser::geyser_server::GeyserServer::new(
                                    SubscribeService {
                                        updates: Mutex::new(Some(rx)),
                                        replay_requests: Arc::clone(&replay_requests),
                                        accept_replay: false,
                                        expected_request: Some(request.clone()),
                                    },
                                ),
                            )
                            .serve_with_incoming(incoming),
                    );
                    let mut builder = GeyserGrpcClient::build_from_shared(endpoint).unwrap();
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
                    for update in [
                        metadata(42, 7, "unchanged", 41),
                        account(42, 7),
                        account(42, 7),
                    ] {
                        tx.unbounded_send(Ok(update.clone())).unwrap();
                        let delivered =
                            tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                                .await
                                .unwrap()
                                .unwrap()
                                .unwrap();
                        assert_eq!(delivered, update);
                    }
                    if disconnect_error {
                        tx.unbounded_send(Err(Status::unavailable("ordinary disconnect")))
                            .unwrap();
                        let error =
                            tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                                .await
                                .unwrap()
                                .unwrap()
                                .unwrap_err();
                        assert_eq!(error.code(), tonic::Code::Unavailable);
                        assert_eq!(error.message(), "ordinary disconnect");
                    }
                    drop(tx);
                    assert!(
                        tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                            .await
                            .unwrap()
                            .is_none()
                    );
                    assert!(replay_requests.lock().unwrap().is_empty());
                    server.abort();
                    let _ = server.await;
                }
            }
        }
    }

    type TestServer = (
        String,
        mpsc::UnboundedSender<Result<SubscribeUpdate, Status>>,
        Arc<Mutex<Vec<u64>>>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    );

    fn spawn_server(accept_replay: bool) -> TestServer {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let (tx, rx) = mpsc::unbounded();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handle = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    yellowstone_grpc_proto::geyser::geyser_server::GeyserServer::new(
                        SubscribeService {
                            updates: Mutex::new(Some(rx)),
                            replay_requests: Arc::clone(&requests),
                            accept_replay,
                            expected_request: None,
                        },
                    ),
                )
                .serve_with_incoming(incoming),
        );
        (endpoint, tx, requests, handle)
    }

    async fn next_event(stream: &mut ReconnectGeyserStream) -> Result<ReconnectEvent, Status> {
        tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn public_api_reports_the_gap_and_replays_from_the_last_finalized_slot() {
        let servers = [spawn_server(true), spawn_server(true)];
        let mut client = GeyserGrpcClient::build_from_shared(servers[0].0.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        // Route reconnects to a different server while the initial channel remains on the first.
        client.reconnect_endpoint =
            Some(tonic::transport::Endpoint::from_shared(servers[1].0.clone()).unwrap());
        let (_sink, mut stream) = client.subscribe_with_reconnect(None).await.unwrap();
        servers[0]
            .1
            .unbounded_send(Ok(finalized_status(41)))
            .unwrap();
        servers[0].1.unbounded_send(Ok(account(42, 7))).unwrap();
        assert!(matches!(
            next_event(&mut stream).await.unwrap(),
            ReconnectEvent::Update(_)
        ));
        servers[1].1.unbounded_send(Ok(account(45, 99))).unwrap();
        servers[0]
            .1
            .unbounded_send(Err(Status::unavailable("mid-slot disconnect")))
            .unwrap();
        assert!(matches!(
            next_event(&mut stream).await.unwrap(),
            ReconnectEvent::Missing {
                from_slot: Some(41),
                to_slot: 42
            }
        ));
        assert!(matches!(next_event(&mut stream).await.unwrap(),
            ReconnectEvent::Update(update) if update == account(45, 99)));
        assert_eq!(
            *servers[1].2.lock().unwrap(),
            [42],
            "replays after the finalized slot"
        );
        for (_, _, _, handle) in servers {
            handle.abort();
            let _ = handle.await;
        }
    }
}
