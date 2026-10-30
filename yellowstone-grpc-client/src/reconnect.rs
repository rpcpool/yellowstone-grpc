//! Re-establishing a subscription: the retry schedule, the connector that opens a new
//! subscribe stream, the replay policy, and the internal filters a reconnecting subscription
//! adds to every request.

use {
    crate::{GeyserGrpcClientError, InterceptorXToken, ReconnectConfig},
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
        prelude::{
            SubscribeRequest, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
            SubscribeUpdate,
        },
    },
};

/// Name of the block meta and slot filters a reconnecting subscription adds to every request.
///
/// The [`ReconnectStream`](crate::ReconnectStream) reads block metas to know which banks were
/// delivered whole, and confirmed slots to know where to replay from. It removes this name from
/// [`SubscribeUpdate::filters`], and hides an update matched by this filter alone. A user
/// filter must not use this name.
pub const RECONNECT_FILTER_KEY: &str = "__reconn";

/// What a [`ReconnectStream`](crate::ReconnectStream) asks the server for when it reconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectPolicy {
    /// Resume at the live tip. Whatever the server sent during the outage is lost.
    NoReplay,
    /// Replay from `slots_behind` slots before the last confirmed slot the stream saw.
    ///
    /// The replay resends banks the client already has: the client must handle duplicates.
    /// A replayed bank carries only the latest write of each account, so it can have fewer
    /// account updates than the same bank delivered live; each account's final state is the
    /// same. Before the first confirmed slot, a reconnect replays from the first request's own
    /// `from_slot`, so the history it asked for is not skipped; without one, it resumes at the
    /// live tip. The server keeps a bounded replay window (150 slots by default); a
    /// `from_slot` older than that fails the reconnect.
    Replay { slots_behind: u64 },
}

impl Default for ReconnectPolicy {
    /// Replays from the last confirmed slot. Every bank still in flight at a disconnect is
    /// above it, so the replay resends each of them whole.
    fn default() -> Self {
        Self::Replay { slots_behind: 0 }
    }
}

impl ReconnectPolicy {
    /// Picks the slot a reconnect replays from.
    ///
    /// # Arguments
    ///
    /// * `last_confirmed` - The highest confirmed slot the stream saw, if any.
    /// * `requested_from_slot` - The first request's `from_slot`, if it set one.
    ///
    /// # Returns
    ///
    /// The `from_slot` for the next subscribe request, or [`None`] to resume at the live tip.
    /// [`ReconnectPolicy::NoReplay`] always resumes at the tip. [`ReconnectPolicy::Replay`]
    /// replays from `slots_behind` before `last_confirmed`, or from `requested_from_slot`
    /// before any confirmed slot was seen.
    pub(crate) fn replay_from_slot(
        self,
        last_confirmed: Option<u64>,
        requested_from_slot: Option<u64>,
    ) -> Option<u64> {
        match self {
            Self::NoReplay => None,
            Self::Replay { slots_behind } => last_confirmed
                .map(|slot| slot.saturating_sub(slots_behind))
                .or(requested_from_slot),
        }
    }
}

/// The latest request of a reconnecting subscription, always with the [`RECONNECT_FILTER_KEY`]
/// filters, and the request channel of its live connection.
///
/// The [`ReconnectStream`](crate::ReconnectStream), its
/// [`SubscribeRequestSink`](crate::SubscribeRequestSink) and its [`TonicGrpcConnector`] share
/// it, and clones share its state:
///
/// - the sink stores each request and sends it on the live connection, in one step;
/// - a reconnect subscribes with the latest request;
/// - the connector installs each new connection's channel, and resends the latest request on
///   it if the request changed after the reconnect read it, in one step.
///
/// One lock covers both steps. Without it, a request sent while a reconnect is connecting went
/// to the old, dead connection, and the new one ran the older filters.
///
/// # Examples
///
/// ```ignore
/// let request = ReconnectRequest::new(SubscribeRequest::default());
/// let stream = ReconnectStream::connect(connector, request.clone(), config).await?;
/// let stored = request.store(updated_request);
/// ```
#[derive(Clone)]
pub struct ReconnectRequest {
    state: Arc<Mutex<RequestState>>,
}

/// What a [`ReconnectRequest`] guards with one lock.
struct RequestState {
    /// The latest request, with the [`RECONNECT_FILTER_KEY`] filters.
    latest: Arc<SubscribeRequest>,
    /// The live connection's request channel, once one is installed.
    sender: Option<mpsc::Sender<SubscribeRequest>>,
}

impl ReconnectRequest {
    /// Starts from the subscription's first request.
    ///
    /// # Arguments
    ///
    /// * `request` - The first [`SubscribeRequest`]. Its `from_slot`, if any, is where the
    ///   first connection replays from.
    ///
    /// # Returns
    ///
    /// A [`ReconnectRequest`] holding `request` with the [`RECONNECT_FILTER_KEY`] filters, and
    /// no connection yet.
    pub fn new(mut request: SubscribeRequest) -> Self {
        add_reconnect_filters(&mut request);
        Self {
            state: Arc::new(Mutex::new(RequestState {
                latest: Arc::new(request),
                sender: None,
            })),
        }
    }

    /// Replaces the latest request, without sending it.
    ///
    /// # Arguments
    ///
    /// * `request` - The new [`SubscribeRequest`].
    ///
    /// # Returns
    ///
    /// The stored request: `request` with the [`RECONNECT_FILTER_KEY`] filters.
    pub fn store(&self, mut request: SubscribeRequest) -> Arc<SubscribeRequest> {
        add_reconnect_filters(&mut request);
        let request = Arc::new(request);
        self.lock().latest = Arc::clone(&request);
        request
    }

    /// Reads the latest request.
    ///
    /// # Returns
    ///
    /// The last request passed to [`ReconnectRequest::new`], [`ReconnectRequest::store`] or
    /// sent on the sink, with the [`RECONNECT_FILTER_KEY`] filters.
    pub fn load(&self) -> Arc<SubscribeRequest> {
        Arc::clone(&self.lock().latest)
    }

    /// Waits until the live connection can take a request.
    ///
    /// # Arguments
    ///
    /// * `cx` - The task context to wake.
    ///
    /// # Returns
    ///
    /// Ready once a request can be sent; ready at once without a connection.
    ///
    /// # Errors
    ///
    /// Returns the channel's [`mpsc::SendError`] if the connection's channel is closed.
    pub(crate) fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), mpsc::SendError>> {
        match self.lock().sender.as_mut() {
            Some(sender) => sender.poll_ready(cx),
            None => Poll::Ready(Ok(())),
        }
    }

    /// Stores a request and sends it on the live connection, under one lock.
    ///
    /// A ping is sent but not stored: it is not a subscription, and a reconnect must not
    /// subscribe with it.
    ///
    /// # Arguments
    ///
    /// * `request` - The [`SubscribeRequest`] to send.
    ///
    /// # Errors
    ///
    /// Returns the channel's [`mpsc::SendError`] if the connection's channel is full or closed.
    /// The request is stored anyway, so the next reconnect subscribes with it.
    pub(crate) fn send(&self, mut request: SubscribeRequest) -> Result<(), mpsc::SendError> {
        let mut state = self.lock();
        if request.ping.is_none() {
            add_reconnect_filters(&mut request);
            state.latest = Arc::new(request.clone());
        }
        match state.sender.as_mut() {
            Some(sender) => sender.start_send(request),
            None => Ok(()),
        }
    }

    /// Flushes or closes the live connection's channel.
    ///
    /// # Arguments
    ///
    /// * `cx` - The task context to wake.
    /// * `close` - Whether to close the channel after flushing it.
    ///
    /// # Returns
    ///
    /// Ready once done; ready at once without a connection.
    ///
    /// # Errors
    ///
    /// Returns the channel's [`mpsc::SendError`] if it fails.
    pub(crate) fn poll_flush_or_close(
        &self,
        cx: &mut Context<'_>,
        close: bool,
    ) -> Poll<Result<(), mpsc::SendError>> {
        match self.lock().sender.as_mut() {
            Some(sender) if close => sender.poll_close_unpin(cx),
            Some(sender) => sender.poll_flush_unpin(cx),
            None => Poll::Ready(Ok(())),
        }
    }

    /// Makes a new connection's channel the live one.
    ///
    /// If the latest request is not the one the connection subscribed with, a request was
    /// sent while the connection was being made, on the old channel: it is resent on the new
    /// one before any later request.
    ///
    /// # Arguments
    ///
    /// * `sender` - The new connection's request channel.
    /// * `subscribed_with` - The request the connection subscribed with, as returned by
    ///   [`ReconnectRequest::load`].
    pub(crate) fn install_sender(
        &self,
        mut sender: mpsc::Sender<SubscribeRequest>,
        subscribed_with: &Arc<SubscribeRequest>,
    ) {
        let mut state = self.lock();
        if !Arc::ptr_eq(&state.latest, subscribed_with) {
            // A fresh channel holds only the first request: it cannot be full.
            if let Err(error) = sender.try_send(SubscribeRequest::clone(&state.latest)) {
                log::warn!("failed to resend the latest request on the new connection: {error}");
            }
        }
        state.sender = Some(sender);
    }

    /// Locks the shared state.
    ///
    /// # Returns
    ///
    /// The guard over the [`RequestState`].
    ///
    /// # Panics
    ///
    /// Panics if a thread panicked while holding the lock.
    fn lock(&self) -> std::sync::MutexGuard<'_, RequestState> {
        self.state.lock().expect("reconnect request mutex poisoned")
    }
}

/// Adds the [`RECONNECT_FILTER_KEY`] filters to a subscribe request.
///
/// The block meta filter reports every bank that completes. The slot filter reports every
/// Processed, Confirmed and Finalized status, whatever the request's commitment.
///
/// # Arguments
///
/// * `request` - The [`SubscribeRequest`] to extend. A user filter already named
///   [`RECONNECT_FILTER_KEY`] is replaced.
fn add_reconnect_filters(request: &mut SubscribeRequest) {
    request.blocks_meta.insert(
        RECONNECT_FILTER_KEY.to_owned(),
        SubscribeRequestFilterBlocksMeta::default(),
    );
    request.slots.insert(
        RECONNECT_FILTER_KEY.to_owned(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(false),
        },
    );
}

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
pub(crate) struct BackoffInstance {
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

    pub(crate) const fn instance(&self) -> BackoffInstance {
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

/// The [`GrpcConnector`] of [`ReconnectStream`](crate::ReconnectStream): each connect opens a
/// new channel to the endpoint, subscribes, then installs the new connection's request channel
/// in the subscription's [`ReconnectRequest`], so the user's
/// [`SubscribeRequestSink`](crate::SubscribeRequestSink) writes to the new connection.
#[derive(Clone)]
pub struct TonicGrpcConnector {
    backoff: Backoff,
    request: ReconnectRequest,
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
        request: ReconnectRequest,
    ) -> Self {
        Self {
            backoff: config.backoff,
            request,
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
        let shared = self.request.clone();
        let x_token = self.x_token.clone();
        let options = self.options.clone();
        let base_request = (*request).clone();

        let fut = backoff.retry(move || {
            let endpoint = endpoint.clone();
            let shared = shared.clone();
            let subscribed_with = Arc::clone(&request);
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

                // The sink writes to the new connection from now on, and a request it sent
                // while this one was connecting is resent here.
                shared.install_sender(subscribe_tx, &subscribed_with);

                Ok(response.into_inner())
            }
        });

        Box::pin(fut)
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
pub(crate) const fn is_recoverable_status_code(code: Code) -> bool {
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

#[cfg(test)]
mod tests {
    use {super::*, tonic::Code};

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
    fn test_backoff_iterator_yields_expected_intervals() {
        let backoff = Backoff::new(Duration::from_millis(100), 2.0, 3);
        let mut instance = backoff.instance();

        assert_eq!(instance.next(), Some(Duration::from_millis(100)));
        assert_eq!(instance.next(), Some(Duration::from_millis(200)));
        assert_eq!(instance.next(), Some(Duration::from_millis(400)));
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
    async fn test_backoff_retry_does_not_retry_unrecoverable() {
        let backoff = Backoff::new(Duration::from_millis(0), 2.0, 5);
        let mut calls = 0;

        let result = backoff
            .retry(|| {
                calls += 1;
                async { Err::<(), _>(ErrorCategory::Unrecoverable("fatal")) }
            })
            .await;

        assert_eq!(result, Err("fatal"));
        assert_eq!(calls, 1);
    }

    #[test]
    fn test_should_reconnect() {
        for code in [
            Code::Cancelled,
            Code::Unknown,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
            Code::Aborted,
            Code::Internal,
            Code::Unavailable,
            Code::DataLoss,
        ] {
            assert!(is_recoverable_status_code(code), "{code:?}");
        }
        for code in [
            Code::OutOfRange,
            Code::InvalidArgument,
            Code::NotFound,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::Unimplemented,
            Code::FailedPrecondition,
        ] {
            assert!(!is_recoverable_status_code(code), "{code:?}");
        }
    }

    #[test]
    fn test_client_error_recoverability_follows_status_code() {
        let recoverable = GeyserGrpcClientError::TonicStatus(Status::unavailable("down"));
        assert!(is_recoverable_client_error(&recoverable));
        let fatal = GeyserGrpcClientError::TonicStatus(Status::permission_denied("token"));
        assert!(!is_recoverable_client_error(&fatal));
    }

    #[test]
    fn no_replay_never_replays() {
        assert_eq!(ReconnectPolicy::NoReplay.replay_from_slot(None, None), None);
        assert_eq!(
            ReconnectPolicy::NoReplay.replay_from_slot(Some(100), Some(42)),
            None
        );
    }

    #[test]
    fn replay_starts_behind_the_last_confirmed_slot() {
        let policy = ReconnectPolicy::Replay { slots_behind: 5 };
        assert_eq!(policy.replay_from_slot(Some(100), None), Some(95));
        assert_eq!(policy.replay_from_slot(Some(3), None), Some(0));
        assert_eq!(policy.replay_from_slot(None, None), None);
        // Before any confirmed slot, the first request's own from_slot.
        assert_eq!(policy.replay_from_slot(None, Some(42)), Some(42));
        // Once one is known, the confirmed slot wins.
        assert_eq!(policy.replay_from_slot(Some(100), Some(42)), Some(95));
        assert_eq!(
            ReconnectPolicy::default().replay_from_slot(Some(100), None),
            Some(100)
        );
    }

    #[test]
    fn reconnect_filters_ask_for_block_metas_and_every_commitment_status() {
        let mut request = SubscribeRequest::default();
        request
            .slots
            .insert("user".to_owned(), SubscribeRequestFilterSlots::default());
        add_reconnect_filters(&mut request);

        assert!(request.blocks_meta.contains_key(RECONNECT_FILTER_KEY));
        assert_eq!(
            request.slots[RECONNECT_FILTER_KEY],
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(false),
            }
        );
        assert!(request.slots.contains_key("user"));
    }

    fn drain(receiver: &mut mpsc::Receiver<SubscribeRequest>) -> Vec<SubscribeRequest> {
        std::iter::from_fn(|| receiver.try_recv().ok()).collect()
    }

    /// The race: a filter change sent while a reconnect is connecting.
    ///
    /// ```text
    /// time ─────────────────────────────────────────────────────────────────►
    /// reconnect:  load() = v1  ──── connecting ────  install_sender(new, v1)
    /// user:                    send(v2)
    /// old conn:                v2   (dead: nobody reads it)
    /// new conn:                                      v2   ← resent: latest v2 ≠ v1
    /// ```
    #[test]
    fn a_request_sent_while_connecting_is_resent_on_the_new_connection() {
        let request = ReconnectRequest::new(SubscribeRequest::default());
        let (old, mut old_rx) = mpsc::channel(8);
        request.install_sender(old, &request.load());

        // A reconnect reads the request, then a filter change is sent before it connects.
        let subscribed_with = request.load();
        let mut change = SubscribeRequest::default();
        change.slots.insert("user".to_owned(), Default::default());
        request.send(change).unwrap();
        assert_eq!(
            drain(&mut old_rx).len(),
            1,
            "the change went to the old connection"
        );

        let (new, mut new_rx) = mpsc::channel(8);
        request.install_sender(new, &subscribed_with);
        let resent = drain(&mut new_rx);
        assert_eq!(resent.len(), 1);
        assert!(resent[0].slots.contains_key("user"));
        assert!(resent[0].slots.contains_key(RECONNECT_FILTER_KEY));
    }

    /// No change while connecting: nothing to catch up.
    ///
    /// ```text
    /// time ─────────────────────────────────────────────────────────────────►
    /// reconnect:  load() = v1  ──── connecting ────  install_sender(new, v1)
    /// user:                    (no send)
    /// new conn:                                      (nothing: latest is v1)
    /// ```
    #[test]
    fn an_unchanged_request_is_not_resent() {
        let request = ReconnectRequest::new(SubscribeRequest::default());
        let (new, mut new_rx) = mpsc::channel(8);
        request.install_sender(new, &request.load());
        assert!(drain(&mut new_rx).is_empty());
    }

    /// A change sent after the install goes straight to the new connection, and is not sent
    /// twice: the install already happened, so it has nothing to resend.
    ///
    /// ```text
    /// time ─────────────────────────────────────────────────────────────────►
    /// reconnect:  load() = v1  install_sender(new, v1)
    /// user:                                            send(v2)
    /// old conn:   (nothing)
    /// new conn:                                        v2   (once)
    /// ```
    #[test]
    fn a_request_sent_after_the_install_goes_to_the_new_connection_once() {
        let request = ReconnectRequest::new(SubscribeRequest::default());
        let (old, mut old_rx) = mpsc::channel(8);
        request.install_sender(old, &request.load());
        let (new, mut new_rx) = mpsc::channel(8);
        request.install_sender(new, &request.load());

        request.send(SubscribeRequest::default()).unwrap();
        assert!(drain(&mut old_rx).is_empty());
        assert_eq!(drain(&mut new_rx).len(), 1);
    }

    /// A ping goes to the live connection, but is not the request a reconnect subscribes with.
    ///
    /// ```text
    /// time ─────────────────────────────────────────────────────────────────►
    /// user:       send(ping)
    /// conn:       ping
    /// stored:     v1 ──────────── v1   (unchanged: a ping is not a subscription)
    /// ```
    #[test]
    fn a_ping_is_sent_but_not_stored() {
        let request = ReconnectRequest::new(SubscribeRequest::default());
        let before = request.load();
        let (sender, mut receiver) = mpsc::channel(8);
        request.install_sender(sender, &before);

        let ping = SubscribeRequest {
            ping: Some(yellowstone_grpc_proto::prelude::SubscribeRequestPing { id: 1 }),
            ..Default::default()
        };
        request.send(ping.clone()).unwrap();
        assert_eq!(drain(&mut receiver), [ping]);
        assert!(Arc::ptr_eq(&before, &request.load()));
    }

    #[test]
    fn a_reconnect_request_always_carries_the_reconnect_filters() {
        let request = ReconnectRequest::new(SubscribeRequest {
            from_slot: Some(7),
            ..Default::default()
        });
        let first = request.load();
        assert!(first.blocks_meta.contains_key(RECONNECT_FILTER_KEY));
        assert_eq!(first.from_slot, Some(7));

        let stored = request.store(SubscribeRequest::default());
        assert!(stored.slots.contains_key(RECONNECT_FILTER_KEY));
        assert!(Arc::ptr_eq(&stored, &request.load()));
        assert!(Arc::ptr_eq(&request.clone().load(), &stored));
    }
}
