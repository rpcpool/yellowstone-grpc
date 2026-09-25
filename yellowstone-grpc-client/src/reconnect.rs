use {
    crate::{dedup::ReconnectCounter, GeyserGrpcClientError, InterceptorXToken, ReconnectConfig},
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

/// Number of slots behind the last block_meta to checkpoint.
/// Conservative buffer to account for late-arriving events
/// and out-of-order delivery within a slot.
const CHECKPOINT_SLOT_BUFFER: u64 = 2;

pub const AUTORECONNECT_FILTER_KEY: &str = "__autoreconnect";

/// Why previously delivered bank state must be removed by the consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardReason {
    /// Delivery was interrupted; the partial state cannot be joined across connections.
    /// This does not establish that any of the discarded banks lost consensus.
    IncompleteDelivery,
}

/// The replacement stream from which the consumer must rebuild discarded state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplacementReplay {
    /// Inclusive replay boundary. Remove the listed banks before applying replacement updates.
    pub from_slot: u64,
    /// Connection generation supplying replacement updates; it does not identify a winner.
    pub generation: u64,
}

/// Consensus outcome for one affected slot, independent of the replay source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotWinner {
    /// The finalized chain skips this slot, so there is no winning blockhash to report.
    Unknown { slot: u64 },
    /// Finality establishes this block as the winner, using its cross-connection block identity.
    /// A matching parent, bank ID, or replacement update alone cannot establish this outcome.
    Finalized { slot: u64, blockhash: String },
}

pub(crate) struct RecoveryDecision {
    pub(crate) banks: Vec<crate::BankRef>,
    pub(crate) replacement: ReplacementReplay,
    outcomes: std::collections::BTreeMap<u64, Option<SlotWinner>>,
    metadata: std::collections::HashMap<(u64, u64), (String, u64)>,
    finalized: std::collections::HashSet<(u64, u64)>,
    evidence_limit: usize,
}

impl RecoveryDecision {
    pub(crate) fn new(
        banks: Vec<crate::BankRef>,
        replacement: ReplacementReplay,
        evidence_limit: usize,
    ) -> Self {
        Self {
            outcomes: banks.iter().map(|bank| (bank.slot, None)).collect(),
            banks,
            replacement,
            metadata: Default::default(),
            finalized: Default::default(),
            evidence_limit,
        }
    }

    pub(crate) fn observe(&mut self, update: &SubscribeUpdate) -> Result<(), Status> {
        let identity = match update.update_oneof.as_ref() {
            Some(UpdateOneof::BlockMeta(m)) => {
                Some((m.slot, m.bank_id, &m.blockhash, m.parent_slot))
            }
            Some(UpdateOneof::Block(m)) => Some((m.slot, m.bank_id, &m.blockhash, m.parent_slot)),
            _ => None,
        };
        let key = if let Some((slot, bank_id, blockhash, parent_slot)) = identity {
            if blockhash.is_empty() || blockhash.len() > 128 || (slot != 0 && parent_slot >= slot) {
                return Err(Status::failed_precondition(
                    "invalid block identity while resolving replay winners",
                ));
            }
            let key = (slot, bank_id);
            if let Some(previous) = self.metadata.get(&key) {
                if previous.0 != *blockhash || previous.1 != parent_slot {
                    return Err(Status::failed_precondition(
                        "conflicting block identity for one connection-local bank",
                    ));
                }
            } else {
                if self.metadata.len() >= self.evidence_limit {
                    return Err(Status::resource_exhausted(
                        "reconnect winner evidence limit reached",
                    ));
                }
                self.metadata.insert(key, (blockhash.clone(), parent_slot));
            }
            key
        } else if let Some(UpdateOneof::Slot(m)) = update.update_oneof.as_ref() {
            if m.status != SlotStatus::SlotFinalized as i32 {
                return Ok(());
            }
            let bank_id = m.bank_id.ok_or_else(|| {
                Status::failed_precondition(
                    "finalized slot has no bank_id to associate with a blockhash",
                )
            })?;
            let key = (m.slot, bank_id);
            if !self.finalized.contains(&key) && self.finalized.len() >= self.evidence_limit {
                return Err(Status::resource_exhausted(
                    "reconnect finality evidence limit reached",
                ));
            }
            self.finalized.insert(key);
            key
        } else {
            return Ok(());
        };

        if !self.finalized.contains(&key) {
            return Ok(());
        }
        let Some((blockhash, parent_slot)) = self.metadata.get(&key) else {
            return Ok(());
        };
        for (&slot, outcome) in &mut self.outcomes {
            let decision = if slot == key.0 {
                SlotWinner::Finalized {
                    slot,
                    blockhash: blockhash.clone(),
                }
            } else if *parent_slot < slot && slot < key.0 {
                // A finalized block's direct parent link excludes every intervening slot.
                SlotWinner::Unknown { slot }
            } else {
                continue;
            };
            if outcome
                .as_ref()
                .is_some_and(|previous| previous != &decision)
            {
                return Err(Status::failed_precondition(
                    "conflicting finalized outcomes during reconnect",
                ));
            }
            *outcome = Some(decision);
        }
        Ok(())
    }

    pub(crate) fn winners(&self) -> Option<Vec<SlotWinner>> {
        self.outcomes.values().cloned().collect()
    }
}

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
    inner_stream: Option<GrpcStream>,
    pending_connecting_task: Option<ConnectFuture<GrpcStream>>,
    reconnect_count: u32,
    resume_from_checkpoint: bool,
    bank_replay: bool,
    // Slots below this are settled. A late message for one of them must not pull the bank
    // replay checkpoint back, because dedup state below it has already been dropped.
    settled_before: u64,
    stream_retries: u32,
    replay_from_slot: Option<u64>,
}

impl<S, Connector> AutoReconnect<S, Connector>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
    Connector: GrpcConnector<Stream = S, ConnectError = GeyserGrpcClientError>,
{
    pub fn new(
        stream: S,
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
            reconnect_count: 0,
            resume_from_checkpoint: true,
            bank_replay: false,
            settled_before: 0,
            stream_retries: 0,
            replay_from_slot: None,
        }
    }

    fn make_connection_future(&self) -> ConnectFuture<S> {
        let connector = self.connector.clone();
        let request = self.request.load_full();
        let from_slot = self.checkpoint();
        Box::pin(async move { connector.connect(request, from_slot).await })
    }
}

impl<S, Connector> ReconnectCounter for AutoReconnect<S, Connector> {
    fn reconnect_count(&self) -> u32 {
        self.reconnect_count
    }

    fn replay_from_slot(&self) -> Option<u64> {
        self.replay_from_slot
    }

    fn settle_before(&mut self, slot: u64) {
        // Only bank replay keeps the lowest slot seen; the legacy heuristic sets its own.
        if self.bank_replay {
            self.settled_before = self.settled_before.max(slot);
            self.last_checkpoint = Some(self.last_checkpoint.map_or(slot, |c| c.max(slot)));
        }
    }
}

impl<S, Connector> AutoReconnect<S, Connector> {
    fn checkpoint(&self) -> Option<u64> {
        self.last_checkpoint.filter(|_| self.resume_from_checkpoint)
    }

    /// Retain a replay checkpoint without assuming BlockMeta proves complete delivery.
    pub(crate) const fn with_bank_replay(mut self) -> Self {
        self.bank_replay = true;
        self
    }

    /// Never resume from a checkpoint. Reconnects start from the live head.
    pub const fn without_checkpoint(mut self) -> Self {
        self.resume_from_checkpoint = false;
        self
    }
}

#[cfg(feature = "test-tools")]
impl<S, Connector> AutoReconnect<S, Connector> {
    pub const fn with_bank_replay_for_test(self) -> Self {
        self.with_bank_replay()
    }
}

impl<S, Connector> Stream for AutoReconnect<S, Connector>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
    Connector: GrpcConnector<Stream = S, ConnectError = GeyserGrpcClientError> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    /// Reconnects recoverable failures without clearing the replay checkpoint.
    /// Bank replacement decisions belong to ReconnectStream.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stop {
            return Poll::Ready(None);
        }
        let me = self.get_mut();
        while !me.stop {
            if let Some(mut stream) = me.inner_stream.take() {
                match Pin::new(&mut stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
                        me.stream_retries = 0;
                        if me.bank_replay {
                            if let Some(slot) = extract_slot(&msg) {
                                me.last_checkpoint = Some(
                                    me.last_checkpoint
                                        .map_or(slot, |checkpoint| checkpoint.min(slot))
                                        .max(me.settled_before),
                                );
                            }
                        } else if let Some(UpdateOneof::BlockMeta(_)) = msg.update_oneof.as_ref() {
                            if let Some(slot) = extract_slot(&msg) {
                                // BlockMeta supplies a legacy heuristic, not proof of full delivery.
                                me.last_checkpoint =
                                    Some(slot.saturating_sub(CHECKPOINT_SLOT_BUFFER));
                            }
                        }

                        me.inner_stream = Some(stream);
                        return Poll::Ready(Some(Ok(msg)));
                    }
                    Poll::Ready(Some(Err(status))) if is_recoverable_status_code(status.code()) => {
                        if me.backoff.max_retries == 0
                            || (me.bank_replay && me.stream_retries >= me.backoff.max_retries)
                        {
                            me.stop = true;
                            return Poll::Ready(Some(Err(status)));
                        }

                        log::warn!(
                            "stream error: {status}. reconnecting from slot {:?}",
                            me.checkpoint()
                        );

                        if me.bank_replay {
                            me.stream_retries += 1;
                        }
                        me.pending_connecting_task = Some(me.make_connection_future());
                    }
                    Poll::Ready(Some(Err(e))) => {
                        me.stop = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None)
                        if me.bank_replay && me.stream_retries < me.backoff.max_retries =>
                    {
                        me.stream_retries += 1;
                        me.pending_connecting_task = Some(me.make_connection_future());
                    }
                    Poll::Ready(None) => {
                        me.stop = true;
                        return if me.bank_replay {
                            Poll::Ready(Some(Err(Status::unavailable(
                                "subscription closed and reconnect retries are exhausted or disabled",
                            ))))
                        } else {
                            Poll::Ready(None)
                        };
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
                            me.reconnect_count += 1;
                            me.replay_from_slot = me.checkpoint();
                            me.inner_stream = Some(stream);
                        }
                        Err(error) => {
                            me.stop = true;
                            let status = match error {
                                GeyserGrpcClientError::TonicStatus(status) => status,
                                other => Status::unavailable(format!("reconnect failed: {other}")),
                            };
                            return Poll::Ready(Some(Err(status)));
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

pub(crate) fn unverified_replay() -> Status {
    Status::failed_precondition(
        "recovery unavailable: the existing Subscribe protocol cannot certify complete replacement coverage",
    )
}

pub(crate) fn finalized_slot(update: &SubscribeUpdate) -> Option<u64> {
    match update.update_oneof.as_ref()? {
        UpdateOneof::Slot(m) if m.status == SlotStatus::SlotFinalized as i32 => Some(m.slot),
        _ => None,
    }
}

// Hide control-only messages only after checkpoint and dedup machinery has seen them.
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
        crate::{DedupState, DedupStream},
        futures::{stream, StreamExt},
        std::{
            collections::VecDeque,
            sync::{Arc, Mutex},
        },
        tonic::Code,
        yellowstone_grpc_proto::{
            geyser::{
                SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta,
            },
            prelude::{subscribe_update::UpdateOneof, SubscribeUpdatePing, SubscribeUpdateSlot},
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
    async fn test_autoreconnect_recovers_from_recoverable_stream_error() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(None),
            result: Ok(vec![Ok(make_slot_msg(42, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            initial,
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
            initial,
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
    async fn test_autoreconnect_passes_checkpoint_as_from_slot() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(Some(77)),
            result: Ok(vec![Ok(make_slot_msg(90, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            initial,
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

        let mut auto = DedupStream::new(
            AutoReconnect::new(
                initial,
                connector.clone(),
                request_state(SubscribeRequest::default()),
                backoff_with_retries(1),
            ),
            DedupState::default(),
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
    async fn test_interrupted_slot_payload_reconciled_without_duplicates() {
        // slot 100 is inflight (account seen, no BlockMeta) when the connection dies
        let initial = stream::iter(vec![
            Ok(make_account_msg(100)),
            Err(Status::unavailable("disconnect")),
        ])
        .boxed();

        // replay re-sends the same account, then the BlockMeta that settles slot 100
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: None,
            result: Ok(vec![
                Ok(make_account_msg(100)),
                Ok(make_block_meta_msg(100)),
                Ok(make_slot_msg(101, 0)),
            ]),
        }]);

        let mut stream = DedupStream::new(
            AutoReconnect::new(
                initial,
                connector.clone(),
                request_state(SubscribeRequest::default()),
                backoff_with_retries(1),
            ),
            DedupState::default(),
        );

        let m1 = stream.next().await.expect("item").expect("ok");
        assert!(matches!(m1.update_oneof, Some(UpdateOneof::Account(_))));

        // Matching replay delivers BlockMeta without repeating the account payload.
        let m2 = stream.next().await.expect("item").expect("ok");
        assert!(matches!(m2.update_oneof, Some(UpdateOneof::BlockMeta(_))));

        let m3 = stream.next().await.expect("item").expect("ok");
        assert_eq!(extract_slot(&m3), Some(101));
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
            initial,
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
            initial,
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

    #[tokio::test]
    async fn test_no_checkpoint_when_no_block_meta() {
        // Stream emits only account updates, no block_meta
        let account_msg = SubscribeUpdate {
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
                slot: 100,
                is_startup: false,
                bank_id: Some(100),
            })),
            created_at: None,
        };

        let initial = stream::iter(vec![
            Ok(account_msg),
            Err(Status::unavailable("disconnect")),
        ])
        .boxed();

        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(None), // <-- checkpoint should be None
            result: Ok(vec![Ok(make_slot_msg(101, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            initial,
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        );

        // Consume account message
        let _ = auto.next().await;

        // Reconnect happens, verify from_slot is None (not Some(100))
        let _ = auto.next().await;

        assert_eq!(
            connector.calls(),
            vec![None],
            "from_slot should be None when no block_meta received"
        );
    }

    #[tokio::test]
    async fn test_checkpoint_updates_on_block_meta() {
        let block_meta_msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: 100,
                ..Default::default()
            })),
            created_at: None,
        };

        let initial = stream::iter(vec![
            Ok(block_meta_msg),
            Err(Status::unavailable("disconnect")),
        ])
        .boxed();

        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(Some(98)), // 100 - CHECKPOINT_SLOT_BUFFER (2)
            result: Ok(vec![Ok(make_slot_msg(101, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            initial,
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        );

        // Consume block_meta — should set checkpoint
        let _ = auto.next().await;

        // Reconnect happens
        let _ = auto.next().await;

        assert_eq!(
            connector.calls(),
            vec![Some(98)],
            "from_slot should be checkpoint - buffer"
        );
    }

    #[tokio::test]
    async fn test_reconnect_exhausts_retries_then_stops() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();

        // Connector fails every reconnect attempt
        let connector = MockGrpcConnector::new(vec![
            ConnectPlan {
                expected_from_slot: Some(None),
                result: Err(GeyserGrpcClientError::TonicStatus(Status::unavailable(
                    "still down",
                ))),
            },
            ConnectPlan {
                expected_from_slot: Some(None),
                result: Err(GeyserGrpcClientError::TonicStatus(Status::unavailable(
                    "still down",
                ))),
            },
        ]);

        let mut auto = AutoReconnect::new(
            initial,
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(2), // 2 retries allowed
        );

        // Should get error after retries exhausted
        let result = auto.next().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_err());

        // Stream should end
        assert!(auto.next().await.is_none());
    }

    #[tokio::test]
    async fn test_skip_missed_data_never_sends_from_slot() {
        let initial = stream::iter(vec![
            Ok(make_block_meta_msg(100)),
            Err(Status::unavailable("disconnect")),
        ])
        .boxed();

        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(None),
            result: Ok(vec![Ok(make_slot_msg(105, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            initial,
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        )
        .without_checkpoint();

        let _ = auto.next().await;
        let _ = auto.next().await;

        assert_eq!(
            connector.calls(),
            vec![None],
            "SkipMissedData must not replay even after a BlockMeta set a checkpoint"
        );
    }
    #[tokio::test]
    async fn bank_recovery_reconnects_stream_error_and_eof_from_partial_slot() {
        for disconnect in [Some(Status::unavailable("partial bank")), None] {
            let mut initial = vec![Ok(make_account_msg(100)), Ok(make_block_meta_msg(105))];
            if let Some(error) = disconnect {
                initial.push(Err(error));
            }
            let replacement = make_account_msg(100);
            let connector = MockGrpcConnector::new(vec![ConnectPlan {
                expected_from_slot: Some(Some(100)),
                result: Ok(vec![Ok(replacement.clone())]),
            }]);
            let mut stream = AutoReconnect::new(
                stream::iter(initial).boxed(),
                connector.clone(),
                request_state(SubscribeRequest::default()),
                backoff_with_retries(1),
            )
            .with_bank_replay();
            assert!(stream.next().await.unwrap().is_ok());
            assert!(stream.next().await.unwrap().is_ok());
            assert_eq!(stream.next().await.unwrap().unwrap(), replacement);
            assert_eq!(connector.calls(), vec![Some(100)]);
            assert_eq!(stream.reconnect_count(), 1);
            assert_eq!(stream.last_checkpoint, Some(100));
        }
    }

    // Before settle_before, bank replay kept the lowest slot ever seen, so any reconnect
    // after the server's replay window asked for an expired slot and failed with OutOfRange.
    #[tokio::test]
    async fn bank_recovery_resumes_after_the_settled_boundary() {
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(Some(201)),
            result: Ok(vec![Ok(make_account_msg(201))]),
        }]);
        let mut stream = AutoReconnect::new(
            stream::iter(vec![
                Ok(make_account_msg(100)),
                Ok(make_account_msg(205)),
                Err(Status::unavailable("disconnected")),
            ])
            .boxed(),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        )
        .with_bank_replay();
        assert!(stream.next().await.unwrap().is_ok());
        stream.settle_before(201);
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(stream.last_checkpoint, Some(201));
        assert_eq!(stream.next().await.unwrap().unwrap(), make_account_msg(201));
        assert_eq!(connector.calls(), vec![Some(201)]);
    }

    // ReconnectStream reports finality to AutoReconnect through settle_before; without that
    // wiring a reconnect asks for the first slot of the subscription.
    #[tokio::test]
    async fn reconnect_stream_resumes_after_finalized_slot() {
        use yellowstone_grpc_proto::prelude::{
            SlotStatus, SubscribeUpdateBlockMeta, SubscribeUpdateSlot,
        };
        let control = |update_oneof| SubscribeUpdate {
            filters: vec![AUTORECONNECT_FILTER_KEY.into()],
            update_oneof: Some(update_oneof),
            ..Default::default()
        };
        let mut initial = Vec::new();
        for slot in 100..=110 {
            initial.push(Ok(make_account_msg(slot)));
            initial.push(Ok(control(UpdateOneof::BlockMeta(
                SubscribeUpdateBlockMeta {
                    slot,
                    bank_id: slot,
                    blockhash: format!("hash-{slot}"),
                    parent_slot: slot - 1,
                    ..Default::default()
                },
            ))));
        }
        initial.push(Ok(control(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 108,
            bank_id: Some(108),
            status: SlotStatus::SlotFinalized as i32,
            ..Default::default()
        }))));
        initial.push(Err(Status::unavailable("disconnected")));
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(Some(109)),
            result: Ok(vec![Ok(make_account_msg(111))]),
        }]);
        let mut stream = crate::ReconnectStream::new(
            AutoReconnect::new(
                stream::iter(initial).boxed(),
                connector.clone(),
                request_state(SubscribeRequest::default()),
                backoff_with_retries(1),
            )
            .with_bank_replay(),
        );
        for slot in 100..=110 {
            let crate::ReconnectEvent::Update { update, .. } =
                stream.next().await.unwrap().unwrap()
            else {
                panic!("unexpected discard")
            };
            assert_eq!(update, make_account_msg(slot));
        }
        let crate::ReconnectEvent::Update { generation, update } =
            stream.next().await.unwrap().unwrap()
        else {
            panic!("complete banks need no discard")
        };
        assert_eq!((generation, update), (1, make_account_msg(111)));
        assert_eq!(connector.calls(), vec![Some(109)]);
    }

    #[tokio::test]
    async fn late_update_below_the_settled_boundary_keeps_the_checkpoint() {
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(Some(201)),
            result: Ok(vec![Ok(make_account_msg(201))]),
        }]);
        let mut stream = AutoReconnect::new(
            stream::iter(vec![
                Ok(make_account_msg(205)),
                Ok(make_account_msg(150)),
                Err(Status::unavailable("disconnected")),
            ])
            .boxed(),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        )
        .with_bank_replay();
        stream.settle_before(201);
        assert!(stream.next().await.unwrap().is_ok());
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(stream.last_checkpoint, Some(201));
        assert_eq!(stream.next().await.unwrap().unwrap(), make_account_msg(201));
        assert_eq!(connector.calls(), vec![Some(201)]);
    }

    #[tokio::test]
    async fn settle_before_is_ignored_without_bank_replay() {
        let mut stream = AutoReconnect::new(
            stream::iter(vec![Ok(make_account_msg(100))]).boxed(),
            MockGrpcConnector::new(vec![]),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        );
        stream.settle_before(201);
        assert_eq!(stream.last_checkpoint, None);
    }

    #[tokio::test]
    async fn bank_recovery_keeps_checkpoint_on_another_replay_disconnect() {
        let connector = MockGrpcConnector::new(vec![
            ConnectPlan {
                expected_from_slot: Some(Some(100)),
                result: Ok(vec![
                    Ok(make_account_msg(100)),
                    Ok(make_block_meta_msg(110)),
                    Err(Status::unavailable("disconnected during replay")),
                ]),
            },
            ConnectPlan {
                expected_from_slot: Some(Some(100)),
                result: Ok(vec![Ok(make_account_msg(100))]),
            },
        ]);
        let mut stream = AutoReconnect::new(
            stream::iter(vec![
                Ok(make_account_msg(100)),
                Err(Status::unavailable("partial bank")),
            ])
            .boxed(),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        )
        .with_bank_replay();
        for _ in 0..4 {
            assert!(stream.next().await.unwrap().is_ok());
        }
        assert_eq!(connector.calls(), vec![Some(100), Some(100)]);
        assert_eq!(stream.reconnect_count(), 2);
        assert_eq!(stream.last_checkpoint, Some(100));
    }

    #[tokio::test]
    async fn bank_recovery_reports_unavailable_replay_after_attempting_reconnect() {
        for in_stream in [false, true] {
            let error = Status::out_of_range("replay expired");
            let connector = MockGrpcConnector::new(vec![ConnectPlan {
                expected_from_slot: Some(Some(100)),
                result: if in_stream {
                    Ok(vec![Err(error)])
                } else {
                    Err(GeyserGrpcClientError::TonicStatus(error))
                },
            }]);
            let mut stream = AutoReconnect::new(
                stream::iter(vec![
                    Ok(make_account_msg(100)),
                    Err(Status::unavailable("partial bank")),
                ])
                .boxed(),
                connector.clone(),
                request_state(SubscribeRequest::default()),
                backoff_with_retries(1),
            )
            .with_bank_replay();
            assert!(stream.next().await.unwrap().is_ok());
            assert_eq!(
                stream.next().await.unwrap().unwrap_err().code(),
                Code::OutOfRange
            );
            assert!(stream.next().await.is_none());
            assert_eq!(connector.calls(), vec![Some(100)]);
            assert_eq!(stream.last_checkpoint, Some(100));
        }
    }

    #[tokio::test]
    async fn bank_recovery_bounds_retries_when_replay_streams_make_no_progress() {
        for disconnect in [Some(Status::internal("from_slot is not supported")), None] {
            let failed_stream = || disconnect.clone().map(Err).into_iter().collect();
            let connector = MockGrpcConnector::new(vec![ConnectPlan {
                expected_from_slot: Some(Some(100)),
                result: Ok(failed_stream()),
            }]);
            let mut initial = vec![Ok(make_account_msg(100))];
            initial.extend(failed_stream());
            let mut stream = AutoReconnect::new(
                stream::iter(initial).boxed(),
                connector.clone(),
                request_state(SubscribeRequest::default()),
                backoff_with_retries(1),
            )
            .with_bank_replay();
            assert!(stream.next().await.unwrap().is_ok());
            let error = stream.next().await.unwrap().unwrap_err();
            assert_eq!(
                error.code(),
                disconnect.as_ref().map_or(Code::Unavailable, Status::code)
            );
            assert!(stream.next().await.is_none());
            assert_eq!(connector.calls(), vec![Some(100)]);
            assert_eq!(stream.last_checkpoint, Some(100));
        }
    }

    #[tokio::test]
    async fn bank_recovery_respects_disabled_retries() {
        for disconnect in [Some(Status::unavailable("partial bank")), None] {
            let mut initial = vec![Ok(make_account_msg(100))];
            if let Some(error) = disconnect {
                initial.push(Err(error));
            }
            let connector = MockGrpcConnector::new(vec![]);
            let mut stream = AutoReconnect::new(
                stream::iter(initial).boxed(),
                connector.clone(),
                request_state(SubscribeRequest::default()),
                backoff_with_retries(0),
            )
            .with_bank_replay();
            assert!(stream.next().await.unwrap().is_ok());
            assert_eq!(
                stream.next().await.unwrap().unwrap_err().code(),
                Code::Unavailable
            );
            assert!(stream.next().await.is_none());
            assert!(connector.calls().is_empty());
        }
    }

    #[tokio::test]
    async fn expired_replay_is_terminal_without_clearing_checkpoint() {
        let connector = MockGrpcConnector::new(vec![]);
        let mut stream = AutoReconnect::new(
            stream::iter(vec![Err(Status::out_of_range("replay expired"))]).boxed(),
            connector.clone(),
            request_state(SubscribeRequest::default()),
            backoff_with_retries(1),
        );
        stream.last_checkpoint = Some(42);
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            Code::OutOfRange
        );
        assert_eq!(stream.last_checkpoint, Some(42));
        assert!(connector.calls().is_empty());
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn another_disconnect_during_replay_drops_previous_connection_buffer() {
        let mut stale = make_account_msg(100);
        if let Some(UpdateOneof::Account(account)) = &mut stale.update_oneof {
            account.account.as_mut().unwrap().lamports = 200;
        }
        let connector = MockGrpcConnector::new(vec![
            ConnectPlan {
                expected_from_slot: None,
                result: Ok(vec![
                    Ok(stale),
                    Err(Status::unavailable("interrupted replay")),
                ]),
            },
            ConnectPlan {
                expected_from_slot: None,
                result: Ok(vec![
                    Ok(make_account_msg(100)),
                    Ok(make_block_meta_msg(100)),
                ]),
            },
        ]);
        let mut stream = DedupStream::new(
            AutoReconnect::new(
                stream::iter(vec![
                    Ok(make_account_msg(100)),
                    Err(Status::unavailable("disconnect")),
                ])
                .boxed(),
                connector.clone(),
                request_state(SubscribeRequest::default()),
                backoff_with_retries(1),
            ),
            DedupState::default(),
        );
        assert!(stream.next().await.unwrap().is_ok());
        // The account was delivered before the first disconnect, so the second replay only
        // adds BlockMeta; the stale account from the interrupted replay never surfaces.
        assert!(matches!(
            stream.next().await.unwrap().unwrap().update_oneof,
            Some(UpdateOneof::BlockMeta(_))
        ));
        assert!(stream.next().await.is_none());
        assert_eq!(connector.calls().len(), 2);
    }

    #[tokio::test]
    async fn internal_metadata_reaches_dedup_before_being_hidden() {
        let mut metadata = make_block_meta_msg(100);
        metadata.filters = vec![AUTORECONNECT_FILTER_KEY.into()];
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: None,
            result: Ok(vec![Ok(make_account_msg(100)), Ok(metadata)]),
        }]);
        let mut stream = DedupStream::new(
            AutoReconnect::new(
                stream::iter(vec![
                    Ok(make_account_msg(100)),
                    Err(Status::unavailable("disconnect")),
                ])
                .boxed(),
                connector,
                request_state(SubscribeRequest::default()),
                backoff_with_retries(1),
            ),
            DedupState::default(),
        );
        assert!(stream.next().await.unwrap().is_ok());
        // The replayed account was already delivered; only the metadata comes through.
        let mut metadata = stream.next().await.unwrap().unwrap();
        assert!(!visible_update(&mut metadata));
        assert!(stream.next().await.is_none());
    }
}

#[cfg(test)]
mod reconnect_stream_tests {
    use {
        crate::{dedup::ReconnectCounter, *},
        futures::{FutureExt, StreamExt},
        std::collections::VecDeque,
        yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdateAccount},
    };

    type StreamPoll = std::task::Poll<Option<Result<SubscribeUpdate, Status>>>;

    struct Source {
        generation: u32,
        replay_from_slot: Option<u64>,
        steps: VecDeque<(u32, StreamPoll)>,
        settled: Vec<u64>,
    }

    impl ReconnectCounter for Source {
        fn reconnect_count(&self) -> u32 {
            self.generation
        }

        fn replay_from_slot(&self) -> Option<u64> {
            self.replay_from_slot
        }

        fn settle_before(&mut self, slot: u64) {
            self.settled.push(slot);
        }
    }

    impl Stream for Source {
        type Item = Result<SubscribeUpdate, Status>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = self.get_mut();
            match this.steps.pop_front() {
                Some((generation, polled)) => {
                    this.generation = generation;
                    polled
                }
                None => std::task::Poll::Ready(None),
            }
        }
    }

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

    fn finalized(slot: u64, bank_id: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![AUTORECONNECT_FILTER_KEY.into()],
            update_oneof: Some(UpdateOneof::Slot(
                yellowstone_grpc_proto::prelude::SubscribeUpdateSlot {
                    slot,
                    bank_id: Some(bank_id),
                    status: yellowstone_grpc_proto::prelude::SlotStatus::SlotFinalized as i32,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn source(updates: Vec<(u32, SubscribeUpdate)>) -> Source {
        Source {
            generation: 0,
            replay_from_slot: Some(42),
            steps: updates
                .into_iter()
                .map(|(generation, update)| (generation, std::task::Poll::Ready(Some(Ok(update)))))
                .collect(),
            settled: Vec::new(),
        }
    }

    // Before finality pruning, every bank ever delivered stayed tracked and a long lived
    // subscription hit the bank limit and failed after 65,536 banks.
    #[tokio::test]
    async fn finality_prunes_settled_banks_so_the_bank_limit_is_not_reached() {
        let mut updates = Vec::new();
        for slot in 1..=100 {
            updates.push((0, account(slot, slot)));
            updates.push((0, metadata(slot, slot, &format!("hash-{slot}"), slot - 1)));
            // A dead fork bank at the same slot never completes.
            updates.push((0, account(slot, slot + 1_000)));
            updates.push((0, finalized(slot, slot)));
        }
        let mut stream = ReconnectStream::with_bank_limit(source(updates), 4);
        for _ in 1..=100 {
            let ReconnectEvent::Update { .. } = stream.next().await.unwrap().unwrap() else {
                panic!("unexpected discard")
            };
            let ReconnectEvent::Update { .. } = stream.next().await.unwrap().unwrap() else {
                panic!("unexpected discard")
            };
        }
        assert!(stream.next().await.unwrap().is_err(), "source ended");
        assert!(stream.delivered_banks.is_empty());
        assert!(!stream.dedup.is_complete(&BankRef {
            generation: 0,
            slot: 100,
            bank_id: 100
        }));
        assert_eq!(stream.inner.settled, (2..=101).collect::<Vec<_>>());
    }

    // Pruning up to the finalized slot dropped the hash of complete bank 41 while recovery
    // held the boundary at 40, so the second replay from 40 delivered bank 41 again.
    #[tokio::test]
    async fn finality_during_recovery_keeps_dedup_state_above_the_boundary() {
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(40, 1)),
            (0, account(41, 2)),
            (0, metadata(41, 2, "h41", 40)),
            (1, finalized(45, 9)),
            (2, account(40, 1)),
            (2, account(41, 2)),
            (2, metadata(41, 2, "h41", 40)),
            (2, metadata(40, 1, "h40", 39)),
            (2, finalized(40, 1)),
        ]));
        stream.inner.replay_from_slot = Some(40);
        stream.inner.steps.push_back((2, std::task::Poll::Pending));
        for slot in [40, 41] {
            let ReconnectEvent::Update { update, .. } = stream.next().await.unwrap().unwrap()
            else {
                panic!("unexpected discard")
            };
            assert_eq!(update, account(slot, slot - 39));
        }
        let ReconnectEvent::DiscardBanks { banks, .. } = stream.next().await.unwrap().unwrap()
        else {
            panic!("expected discard of the partial bank")
        };
        assert_eq!(
            banks,
            vec![BankRef {
                generation: 0,
                slot: 40,
                bank_id: 1
            }]
        );
        let ReconnectEvent::Update { generation, update } = stream.next().await.unwrap().unwrap()
        else {
            panic!("expected the replacement bank")
        };
        assert_eq!((generation, update), (2, account(40, 1)));
        // Bank 41 was complete before the first disconnect; its replay is a duplicate.
        assert!(stream.next().now_or_never().is_none());
        assert_eq!(stream.inner.settled, vec![40]);
    }

    #[tokio::test]
    async fn finality_keeps_banks_awaiting_recovery_and_holds_the_boundary() {
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(42, 7)),
            (1, finalized(40, 3)),
            (1, account(43, 9)),
            (1, finalized(50, 11)),
        ]));
        stream.inner.steps.push_back((1, std::task::Poll::Pending));
        assert!(stream.next().await.unwrap().is_ok());
        assert!(stream.next().now_or_never().is_none());
        let partial = BankRef {
            generation: 0,
            slot: 42,
            bank_id: 7,
        };
        assert!(stream.recovery.is_some());
        assert!(stream.delivered_banks.contains(&partial));
        // The partial bank at 42 has no finalized identity yet, so a further reconnect
        // must still replay from 42 even though slot 50 is finalized.
        assert_eq!(stream.inner.settled, vec![41, 42]);
    }

    #[tokio::test]
    async fn finality_never_moves_the_boundary_backwards() {
        let mut stream = ReconnectStream::new(source(vec![
            (0, finalized(50, 1)),
            (0, finalized(49, 2)),
            (0, account(51, 3)),
        ]));
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(stream.inner.settled, vec![51]);
    }

    #[tokio::test]
    async fn partial_bank_reconnect_discards_before_each_replacement() {
        for replacement_id in [7, 99] {
            let mut stream = ReconnectStream::new(source(vec![
                (0, account(41, 6)),
                (0, account(42, 7)),
                (0, account(42, 8)),
                (1, account(42, replacement_id)),
                (1, metadata(42, replacement_id, "winner", 41)),
                (1, finalized(42, replacement_id)),
                (1, account(42, replacement_id)),
                (2, account(42, 7)),
                (2, finalized(42, 7)),
                (2, metadata(42, 7, "winner", 41)),
            ]));
            for _ in 0..3 {
                assert!(stream.next().await.unwrap().is_ok());
            }
            for generation in 1..=2 {
                if generation == 2 {
                    assert!(matches!(
                        stream.next().await.unwrap().unwrap(),
                        ReconnectEvent::Update { generation: 1, .. }
                    ));
                }
                let ReconnectEvent::DiscardBanks {
                    banks,
                    reason,
                    replacement,
                    winners,
                } = stream.next().await.unwrap().unwrap()
                else {
                    panic!("expected discard before replacement")
                };
                let ids = if generation == 1 {
                    vec![7, 8]
                } else {
                    vec![replacement_id]
                };
                assert_eq!(
                    banks,
                    ids.into_iter()
                        .map(|bank_id| BankRef {
                            generation: generation - 1,
                            slot: 42,
                            bank_id,
                        })
                        .collect::<Vec<_>>()
                );
                assert_eq!(reason, DiscardReason::IncompleteDelivery);
                assert_eq!(
                    replacement,
                    ReplacementReplay {
                        from_slot: 42,
                        generation
                    }
                );
                assert_eq!(
                    winners,
                    vec![SlotWinner::Finalized {
                        slot: 42,
                        blockhash: "winner".into()
                    }]
                );
                assert!(matches!(stream.next().await.unwrap().unwrap(),
                    ReconnectEvent::Update { generation: actual, update }
                    if actual == generation && update == account(42, if generation == 1 { replacement_id } else { 7 })));
            }
            // The discards above never listed slot 41. Finality at 42 then settles it, so
            // its bank is no longer tracked.
            assert!(!stream.delivered_banks.iter().any(|bank| bank.slot == 41));
        }
    }

    #[tokio::test]
    async fn complete_banks_are_deduplicated_while_partial_banks_are_replaced() {
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(42, 7)),
            (0, metadata(42, 7, "complete", 41)),
            (0, account(43, 8)),
            (1, account(42, 99)),
            (1, metadata(42, 99, "complete", 41)),
            (1, account(43, 100)),
            (1, account(43, 100)),
            (1, metadata(43, 100, "replacement", 42)),
            (1, finalized(43, 100)),
            (1, account(44, 101)),
        ]));
        for _ in 0..2 {
            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                ReconnectEvent::Update { generation: 0, .. }
            ));
        }
        let ReconnectEvent::DiscardBanks { banks, winners, .. } =
            stream.next().await.unwrap().unwrap()
        else {
            panic!("expected partial discard")
        };
        assert_eq!(
            banks,
            vec![BankRef {
                generation: 0,
                slot: 43,
                bank_id: 8
            }]
        );
        assert_eq!(
            winners,
            vec![SlotWinner::Finalized {
                slot: 43,
                blockhash: "replacement".into()
            }]
        );
        for expected in [account(43, 100), account(43, 100), account(44, 101)] {
            assert!(
                matches!(stream.next().await.unwrap().unwrap(), ReconnectEvent::Update { generation: 1, update } if update == expected)
            );
        }
    }

    #[tokio::test]
    async fn complete_only_reconnect_needs_no_discard_or_finality_wait() {
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(42, 7)),
            (0, metadata(42, 7, "complete", 41)),
            (1, account(42, 99)),
            (1, metadata(42, 99, "complete", 41)),
            (1, account(43, 100)),
        ]));
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ReconnectEvent::Update { generation: 0, .. }
        ));
        assert!(
            matches!(stream.next().await.unwrap().unwrap(), ReconnectEvent::Update { generation: 1, update } if update == account(43, 100))
        );
    }

    #[tokio::test]
    async fn complete_bank_dedup_keeps_new_finality_and_drops_interrupted_candidates() {
        let mut status = finalized(42, 99);
        status.filters = vec!["user".into()];
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(42, 7)),
            (0, metadata(42, 7, "complete", 41)),
            (1, account(42, 8)),
            (2, account(42, 99)),
            (2, metadata(42, 99, "complete", 41)),
            (2, status.clone()),
            (2, status.clone()),
            (2, account(43, 100)),
        ]));
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ReconnectEvent::Update { generation: 0, .. }
        ));
        assert!(
            matches!(stream.next().await.unwrap().unwrap(), ReconnectEvent::Update { generation: 2, update } if update == status)
        );
        assert!(
            matches!(stream.next().await.unwrap().unwrap(), ReconnectEvent::Update { generation: 2, update } if update == account(43, 100))
        );
    }

    #[tokio::test]
    async fn different_hash_is_not_a_duplicate_even_when_bank_id_is_reused() {
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(42, 7)),
            (0, metadata(42, 7, "first", 41)),
            (1, account(42, 7)),
            (1, metadata(42, 7, "different", 41)),
        ]));
        for generation in [0, 1] {
            assert!(
                matches!(stream.next().await.unwrap().unwrap(), ReconnectEvent::Update { generation: actual, update } if actual == generation && update == account(42, 7))
            );
        }
    }

    #[tokio::test]
    async fn completed_bank_in_same_slot_cannot_hide_partial_replacement() {
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(42, 7)),
            (0, metadata(42, 7, "winner", 41)),
            (0, account(42, 8)),
            (1, account(42, 99)),
            (1, metadata(42, 99, "winner", 41)),
            (1, finalized(42, 99)),
        ]));
        for _ in 0..2 {
            assert!(stream.next().await.unwrap().is_ok());
        }
        let ReconnectEvent::DiscardBanks { banks, .. } = stream.next().await.unwrap().unwrap()
        else {
            panic!("expected discard")
        };
        assert_eq!(
            banks,
            vec![BankRef {
                generation: 0,
                slot: 42,
                bank_id: 8
            }]
        );
        assert!(
            matches!(stream.next().await.unwrap().unwrap(), ReconnectEvent::Update { generation: 1, update } if update == account(42, 99))
        );
    }

    #[tokio::test]
    async fn reconnect_without_replay_boundary_is_an_error() {
        let mut inner = source(vec![(0, account(42, 7)), (1, account(42, 99))]);
        inner.replay_from_slot = None;
        let mut stream = ReconnectStream::new(inner);
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(stream.delivered_banks.len(), 1);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn replay_error_is_preserved_when_connection_generation_changes() {
        let mut inner = source(vec![(0, account(42, 7))]);
        inner.steps.push_back((
            1,
            std::task::Poll::Ready(Some(Err(Status::out_of_range("replay expired")))),
        ));
        let mut stream = ReconnectStream::new(inner);
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::OutOfRange
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn multiple_banks_and_repeated_writes_are_delivered_immediately() {
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(42, 7)),
            (0, account(42, 8)),
            (0, account(42, 7)),
        ]));
        for expected in [7, 8, 7] {
            let event = stream.next().now_or_never().unwrap().unwrap().unwrap();
            let ReconnectEvent::Update { update, .. } = event else {
                panic!("unexpected discard")
            };
            let Some(UpdateOneof::Account(update)) = update.update_oneof else {
                panic!("expected account")
            };
            assert_eq!(update.bank_id, Some(expected));
        }
        assert_eq!(stream.delivered_banks.len(), 2);
    }

    #[tokio::test]
    async fn generation_change_waits_for_finality_before_discard_and_replacement() {
        let mut inner = source(vec![(0, account(42, 7))]);
        inner.steps.push_back((1, std::task::Poll::Pending));
        for update in [account(42, 99), metadata(42, 99, "winner", 41)] {
            inner
                .steps
                .push_back((1, std::task::Poll::Ready(Some(Ok(update)))));
        }
        inner.steps.push_back((1, std::task::Poll::Pending));
        inner
            .steps
            .push_back((1, std::task::Poll::Ready(Some(Ok(finalized(42, 99))))));
        let mut stream = ReconnectStream::new(inner);
        assert!(stream.next().await.unwrap().is_ok());
        assert!(stream.next().now_or_never().is_none());
        assert!(stream.next().now_or_never().is_none());
        assert_eq!(stream.delivered_banks.len(), 1);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ReconnectEvent::DiscardBanks { .. }
        ));
        assert!(matches!(stream.next().await.unwrap().unwrap(),
            ReconnectEvent::Update { generation: 1, update } if update == account(42, 99)));
    }

    #[tokio::test]
    async fn finalized_parent_gap_reports_skipped_slot_as_unknown() {
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(42, 7)),
            (1, account(44, 8)),
            (1, metadata(44, 8, "child", 41)),
            (1, finalized(44, 8)),
        ]));
        assert!(stream.next().await.unwrap().is_ok());
        let ReconnectEvent::DiscardBanks { winners, .. } = stream.next().await.unwrap().unwrap()
        else {
            panic!("expected discard")
        };
        assert_eq!(winners, vec![SlotWinner::Unknown { slot: 42 }]);
        assert!(matches!(stream.next().await.unwrap().unwrap(),
            ReconnectEvent::Update { generation: 1, update } if update == account(44, 8)));
    }

    #[tokio::test]
    async fn finality_requires_matching_bank_and_does_not_infer_skip_from_slot_height() {
        let mut inner = source(vec![
            (0, account(42, 7)),
            (1, account(42, 8)),
            (1, metadata(42, 8, "loser", 41)),
            (1, finalized(42, 9)),
            (1, metadata(44, 10, "child", 42)),
            (1, finalized(44, 10)),
        ]);
        inner.steps.push_back((1, std::task::Poll::Pending));
        inner.steps.push_back((
            1,
            std::task::Poll::Ready(Some(Ok(metadata(42, 9, "winner", 41)))),
        ));
        let mut stream = ReconnectStream::new(inner);
        assert!(stream.next().await.unwrap().is_ok());
        assert!(stream.next().now_or_never().is_none());
        let ReconnectEvent::DiscardBanks { winners, .. } = stream.next().await.unwrap().unwrap()
        else {
            panic!("expected discard")
        };
        assert_eq!(
            winners,
            vec![SlotWinner::Finalized {
                slot: 42,
                blockhash: "winner".into()
            }]
        );
    }

    #[tokio::test]
    async fn disconnect_while_waiting_drops_buffer_and_does_not_reuse_finality() {
        let mut inner = source(vec![
            (0, account(42, 7)),
            (1, account(42, 8)),
            (1, finalized(42, 8)),
            (2, account(42, 8)),
            (2, metadata(42, 8, "new-server", 41)),
        ]);
        inner.steps.push_back((2, std::task::Poll::Pending));
        inner
            .steps
            .push_back((2, std::task::Poll::Ready(Some(Ok(finalized(42, 8))))));
        let mut stream = ReconnectStream::new(inner);
        assert!(stream.next().await.unwrap().is_ok());
        assert!(stream.next().now_or_never().is_none());
        let ReconnectEvent::DiscardBanks {
            banks,
            winners,
            replacement,
            ..
        } = stream.next().await.unwrap().unwrap()
        else {
            panic!("expected discard")
        };
        assert_eq!(
            banks,
            vec![BankRef {
                generation: 0,
                slot: 42,
                bank_id: 7
            }]
        );
        assert_eq!(replacement.generation, 2);
        assert_eq!(
            winners,
            vec![SlotWinner::Finalized {
                slot: 42,
                blockhash: "new-server".into()
            }]
        );
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ReconnectEvent::Update { generation: 2, .. }
        ));
        assert!(stream.pending.iter().all(|update| {
            let mut update = update.clone();
            !super::visible_update(&mut update)
        }));
    }

    #[tokio::test]
    async fn missing_finalized_bank_identity_errors_without_discard() {
        let mut status = finalized(42, 7);
        if let Some(UpdateOneof::Slot(slot)) = status.update_oneof.as_mut() {
            slot.bank_id = None;
        }
        let mut stream = ReconnectStream::new(source(vec![
            (0, account(42, 7)),
            (1, account(42, 8)),
            (1, status),
        ]));
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(stream.delivered_banks.len(), 1);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn all_affected_slots_must_resolve_before_discard() {
        let mut inner = source(vec![
            (0, account(42, 7)),
            (0, account(43, 8)),
            (1, account(42, 9)),
            (1, metadata(42, 9, "winner", 41)),
            (1, finalized(42, 9)),
        ]);
        inner.steps.push_back((1, std::task::Poll::Pending));
        for update in [metadata(44, 10, "child", 42), finalized(44, 10)] {
            inner
                .steps
                .push_back((1, std::task::Poll::Ready(Some(Ok(update)))));
        }
        let mut stream = ReconnectStream::new(inner);
        assert!(stream.next().await.unwrap().is_ok());
        assert!(stream.next().await.unwrap().is_ok());
        assert!(stream.next().now_or_never().is_none());
        assert_eq!(stream.delivered_banks.len(), 2);
        let ReconnectEvent::DiscardBanks { winners, banks, .. } =
            stream.next().await.unwrap().unwrap()
        else {
            panic!("expected discard")
        };
        assert_eq!(banks.len(), 2);
        assert_eq!(
            winners,
            vec![
                SlotWinner::Finalized {
                    slot: 42,
                    blockhash: "winner".into()
                },
                SlotWinner::Unknown { slot: 43 },
            ]
        );
    }

    #[tokio::test]
    async fn conflicting_identity_and_evidence_limits_preserve_old_banks() {
        for (limit, second_id, expected) in [
            (10, 8, tonic::Code::FailedPrecondition),
            (1, 9, tonic::Code::ResourceExhausted),
        ] {
            let mut stream = ReconnectStream::with_bank_limit(
                source(vec![
                    (0, account(42, 7)),
                    (1, account(42, 8)),
                    (1, metadata(42, 8, "first", 41)),
                    (1, metadata(42, second_id, "different", 41)),
                ]),
                limit,
            );
            assert!(stream.next().await.unwrap().is_ok());
            assert_eq!(stream.next().await.unwrap().unwrap_err().code(), expected);
            assert!(stream.delivered_banks.contains(&BankRef {
                generation: 0,
                slot: 42,
                bank_id: 7
            }));
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn recovery_buffer_retains_large_backlogs_in_order() {
        let payload_len = 64 * 1024 * 1024 + 1;
        let mut updates = vec![(0, account(42, 7))];
        updates.extend((0..65_537).map(|index| {
            (
                1,
                SubscribeUpdate {
                    filters: vec![index.to_string()],
                    ..account(42, 8)
                },
            )
        }));
        updates.extend([
            (
                1,
                SubscribeUpdate {
                    filters: vec!["x".repeat(payload_len)],
                    ..account(42, 8)
                },
            ),
            (1, metadata(42, 8, "winner", 41)),
            (1, finalized(42, 8)),
        ]);
        let mut stream = ReconnectStream::new(source(updates));
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ReconnectEvent::Update { generation: 0, .. }
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ReconnectEvent::DiscardBanks { .. }
        ));
        for index in 0..65_537 {
            let ReconnectEvent::Update { generation, update } =
                stream.next().await.unwrap().unwrap()
            else {
                panic!("expected buffered replacement update")
            };
            assert_eq!(generation, 1);
            assert_eq!(update.filters, vec![index.to_string()]);
        }
        let ReconnectEvent::Update { update, .. } = stream.next().await.unwrap().unwrap() else {
            panic!("expected large buffered update")
        };
        assert_eq!(update.filters[0].len(), payload_len);
    }

    #[tokio::test]
    async fn tracking_limit_errors_without_evicting_banks_or_delivering_untracked_data() {
        let mut stream = ReconnectStream::with_bank_limit(
            source(vec![
                (0, account(42, 7)),
                (0, account(42, 7)),
                (0, account(42, 8)),
            ]),
            1,
        );
        assert!(stream.next().await.unwrap().is_ok());
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(stream.delivered_banks.len(), 1);
        assert!(stream.delivered_banks.contains(&BankRef {
            generation: 0,
            slot: 42,
            bank_id: 7
        }));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn internal_controls_are_hidden_without_tracking_undelivered_banks() {
        let mut internal = account(1, 1);
        internal.filters = vec![AUTORECONNECT_FILTER_KEY.into()];
        let mut visible = account(42, 7);
        visible.filters = vec![AUTORECONNECT_FILTER_KEY.into(), "user".into()];
        let mut stream = ReconnectStream::new(source(vec![(0, internal), (0, visible)]));
        let ReconnectEvent::Update { update, .. } = stream.next().await.unwrap().unwrap() else {
            panic!("unexpected discard")
        };
        assert_eq!(update.filters, vec!["user"]);
        assert_eq!(stream.delivered_banks.len(), 1);
    }

    #[tokio::test]
    async fn eof_is_a_recovery_error_and_fused() {
        let mut stream = ReconnectStream::new(source(vec![(0, account(42, 7))]));
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn account_without_bank_identity_is_rejected() {
        let mut update = account(42, 7);
        if let Some(UpdateOneof::Account(account)) = &mut update.update_oneof {
            account.bank_id = None;
        }
        let mut stream = ReconnectStream::new(source(vec![(0, update)]));
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert!(stream.delivered_banks.is_empty());
    }

    #[test]
    fn bank_identity_includes_generation() {
        let first = BankRef {
            generation: 0,
            slot: 42,
            bank_id: 7,
        };
        let second = BankRef {
            generation: 1,
            ..first
        };
        assert_ne!(first, second);
        let mut inner = source(vec![(1, account(42, 7))]);
        inner.generation = 1;
        assert_eq!(ReconnectStream::new(inner).generation, 1);
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
        for config in [
            None,
            Some(ReconnectConfig::default()),
            Some(ReconnectConfig {
                policy: ReconnectionPolicy::SkipMissedData,
                ..Default::default()
            }),
        ] {
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

    #[tokio::test]
    async fn public_api_preserves_live_writes_for_every_builder_policy() {
        for config in [
            None,
            Some(ReconnectConfig::default()),
            Some(ReconnectConfig {
                policy: ReconnectionPolicy::SkipMissedData,
                ..Default::default()
            }),
        ] {
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
                                expected_request: None,
                            },
                        ),
                    )
                    .serve_with_incoming(incoming),
            );
            let mut builder = GeyserGrpcClient::build_from_shared(endpoint).unwrap();
            if let Some(config) = config {
                builder = builder.set_reconnect_config(config);
            }
            let mut client = builder.connect().await.unwrap();
            let (_sink, mut stream) = client.subscribe_with_reconnect(None).await.unwrap();
            assert!(stream.inner.bank_replay);
            let metadata = SubscribeUpdate {
                filters: vec!["user".into()],
                update_oneof: Some(UpdateOneof::BlockMeta(Default::default())),
                ..Default::default()
            };
            // Metadata cannot quarantine subsequent writes or another bank in that slot.
            for update in [metadata, account(0, 7), account(0, 8), account(0, 7)] {
                tx.unbounded_send(Ok(update.clone())).unwrap();
                let item = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                let ReconnectEvent::Update {
                    generation,
                    update: delivered,
                } = item
                else {
                    panic!("unexpected discard")
                };
                assert_eq!(generation, 0);
                assert_eq!(delivered, update);
            }
            tx.unbounded_send(Err(Status::unavailable("disconnect mid-bank")))
                .unwrap();
            let error = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(stream.next().await.is_none());
            assert_eq!(*replay_requests.lock().unwrap(), vec![0]);
            server.abort();
            let _ = server.await;
        }
    }
    #[tokio::test]
    async fn public_api_reconnects_to_another_server_and_discards_before_replacement() {
        for config in [
            None,
            Some(ReconnectConfig::default()),
            Some(ReconnectConfig {
                policy: ReconnectionPolicy::SkipMissedData,
                ..Default::default()
            }),
        ] {
            let mut servers = Vec::new();
            for _ in 0..2 {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
                                    accept_replay: true,
                                    expected_request: None,
                                },
                            ),
                        )
                        .serve_with_incoming(incoming),
                );
                servers.push((endpoint, tx, requests, handle));
            }
            let mut builder = GeyserGrpcClient::build_from_shared(servers[0].0.clone()).unwrap();
            if let Some(config) = config {
                builder = builder.set_reconnect_config(config);
            }
            let mut client = builder.connect().await.unwrap();
            // Route reconnects to a different server while the initial channel remains on the first.
            client.reconnect_endpoint =
                Some(tonic::transport::Endpoint::from_shared(servers[1].0.clone()).unwrap());
            let (_sink, mut stream) = client.subscribe_with_reconnect(None).await.unwrap();
            for bank_id in [7, 8] {
                servers[0]
                    .1
                    .unbounded_send(Ok(account(42, bank_id)))
                    .unwrap();
                let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert!(matches!(
                    event,
                    ReconnectEvent::Update { generation: 0, .. }
                ));
            }
            // Queue identical data on the replacement server before the first server disconnects.
            for bank_id in [7, 99, 7] {
                servers[1]
                    .1
                    .unbounded_send(Ok(account(42, bank_id)))
                    .unwrap();
            }
            servers[0]
                .1
                .unbounded_send(Err(Status::unavailable("mid-slot disconnect")))
                .unwrap();
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(30), stream.next())
                    .await
                    .is_err()
            );
            servers[1].1.unbounded_send(Ok(finalized(42, 99))).unwrap();
            servers[1]
                .1
                .unbounded_send(Ok(metadata(42, 99, "winner", 41)))
                .unwrap();
            let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let ReconnectEvent::DiscardBanks {
                banks,
                reason,
                replacement,
                winners,
            } = event
            else {
                panic!("replacement arrived before discard")
            };
            assert_eq!(
                banks,
                vec![
                    BankRef {
                        generation: 0,
                        slot: 42,
                        bank_id: 7
                    },
                    BankRef {
                        generation: 0,
                        slot: 42,
                        bank_id: 8
                    },
                ]
            );
            assert_eq!(reason, DiscardReason::IncompleteDelivery);
            assert_eq!(
                replacement,
                ReplacementReplay {
                    from_slot: 42,
                    generation: 1
                }
            );
            assert_eq!(
                winners,
                vec![SlotWinner::Finalized {
                    slot: 42,
                    blockhash: "winner".into()
                }]
            );
            for bank_id in [7, 99, 7] {
                let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert!(
                    matches!(event, ReconnectEvent::Update { generation: 1, update }
                    if update == account(42, bank_id))
                );
            }
            assert_eq!(*servers[1].2.lock().unwrap(), vec![42]);
            for (_, _, _, handle) in servers {
                handle.abort();
                let _ = handle.await;
            }
        }
    }
    struct LiveInterruptStream {
        inner: tonic::Streaming<SubscribeUpdate>,
        interrupt: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Stream for LiveInterruptStream {
        type Item = Result<SubscribeUpdate, Status>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if this
                .interrupt
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return std::task::Poll::Ready(Some(Err(Status::unavailable(
                    "live e2e mid-slot disconnect",
                ))));
            }
            std::pin::Pin::new(&mut this.inner).poll_next(cx)
        }
    }

    #[derive(Clone)]
    struct LiveReconnectConnector {
        inner: TonicGrpcConnector,
        interrupt: Arc<std::sync::atomic::AtomicBool>,
        requests: Arc<Mutex<Vec<Option<u64>>>>,
    }

    impl GrpcConnector for LiveReconnectConnector {
        type Stream = LiveInterruptStream;
        type ConnectError = GeyserGrpcClientError;
        type ConnectFuture = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Self::Stream, Self::ConnectError>> + Send>,
        >;

        fn connect(
            &self,
            request: Arc<SubscribeRequest>,
            from_slot: Option<u64>,
        ) -> Self::ConnectFuture {
            self.requests.lock().unwrap().push(from_slot);
            let future = self.inner.connect(request, from_slot);
            let interrupt = Arc::clone(&self.interrupt);
            Box::pin(async move {
                Ok(LiveInterruptStream {
                    inner: future.await?,
                    interrupt,
                })
            })
        }
    }

    #[tokio::test]
    #[ignore = "requires YELLOWSTONE_RECONNECT_ENDPOINT pointing to a live bank-aware replay server"]
    async fn live_reconnect_discards_partial_bank_and_replays_entries() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async {
            use std::collections::{HashMap, HashSet};
            let endpoint = std::env::var("YELLOWSTONE_RECONNECT_ENDPOINT").expect("set live endpoint");
            let mut client = GeyserGrpcClient::build_from_shared(endpoint.clone()).unwrap().connect().await.unwrap();
            eprintln!("LIVE_RECONNECT endpoint={endpoint} version={:?}", client.get_version().await.unwrap());
            let request = SubscribeRequest {
                entry: [("entries".into(), Default::default())].into(),
                blocks_meta: [("metadata".into(), Default::default())].into(),
                ..Default::default()
            };
            let (_sink, public_stream) = client.subscribe_with_reconnect(Some(request)).await.unwrap();
            let auto = public_stream.inner;
            let interrupt = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let mut stream = ReconnectStream::new(AutoReconnect::new(
                LiveInterruptStream { inner: auto.inner_stream.unwrap(), interrupt: Arc::clone(&interrupt) },
                LiveReconnectConnector { inner: auto.connector, interrupt: Arc::clone(&interrupt), requests: Arc::clone(&requests) },
                auto.request, auto.backoff,
            ).with_bank_replay());
            let mut completed = HashSet::new();
            let partial = loop {
                let ReconnectEvent::Update { generation, update } = stream.next().await.unwrap().unwrap()
                else { panic!("unexpected discard before interruption") };
                match update.update_oneof {
                    Some(UpdateOneof::BlockMeta(m)) => { completed.insert((m.slot, m.bank_id)); }
                    Some(UpdateOneof::Entry(e)) if e.index == 0 && !completed.contains(&(e.slot, e.bank_id)) => {
                        break BankRef { generation, slot: e.slot, bank_id: e.bank_id };
                    }
                    _ => {}
                }
            };
            eprintln!("LIVE_RECONNECT interrupt={partial:?} delivered_entries=1");
            interrupt.store(true, std::sync::atomic::Ordering::SeqCst);
            let event = stream.next().await.unwrap().unwrap();
            let ReconnectEvent::DiscardBanks { banks, replacement, winners, .. } = event
            else { panic!("replacement arrived before discard: {event:?}") };
            assert!(banks.contains(&partial));
            assert!(replacement.generation > partial.generation);
            assert!(replacement.from_slot <= partial.slot);
            assert_eq!(*requests.lock().unwrap(), vec![Some(replacement.from_slot)]);
            let winner = winners.iter().find_map(|winner| match winner {
                SlotWinner::Finalized { slot, blockhash } if *slot == partial.slot => Some(blockhash.clone()),
                _ => None,
            }).expect("live partial slot must finalize to verify replacement entries");
            eprintln!("LIVE_RECONNECT discard_banks={} replacement={replacement:?} winner_slot={} winner_blockhash={winner}", banks.len(), partial.slot);
            let mut entries: HashMap<(u64, u64), HashSet<u64>> = HashMap::new();
            loop {
                let event = stream.next().await.unwrap().unwrap();
                let ReconnectEvent::Update { generation, update } = event
                else { panic!("unexpected additional discard") };
                assert_eq!(generation, replacement.generation);
                match update.update_oneof {
                    Some(UpdateOneof::Entry(e)) => { entries.entry((e.slot, e.bank_id)).or_default().insert(e.index); }
                    Some(UpdateOneof::BlockMeta(m)) if m.slot == partial.slot && m.blockhash == winner => {
                        let delivered = entries.get(&(m.slot, m.bank_id)).expect("replacement entries precede metadata");
                        assert!(m.entries_count > 1, "interruption did not split bank entry delivery");
                        assert_eq!(delivered.len() as u64, m.entries_count);
                        assert!((0..m.entries_count).all(|index| delivered.contains(&index)));
                        eprintln!("LIVE_RECONNECT PASS slot={} generation={} bank_id={} replayed_entries={} winner_blockhash={winner}", m.slot, generation, m.bank_id, delivered.len());
                        break;
                    }
                    _ => {}
                }
            }
        }).await.expect("live reconnect e2e timed out after 90 seconds");
    }
}
