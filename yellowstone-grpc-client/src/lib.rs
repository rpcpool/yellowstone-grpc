mod dedup;
mod reconnect;

use {
    crate::reconnect::{TonicGeyserClientOptions, AUTORECONNECT_FILTER_KEY},
    arc_swap::ArcSwap,
    bytes::Bytes,
    futures::{
        channel::mpsc,
        ready,
        sink::{Sink, SinkExt},
        stream::Stream,
    },
    std::{
        collections::{HashSet, VecDeque},
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
        geyser_client::GeyserClient, subscribe_update::UpdateOneof, CommitmentLevel,
        GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
        GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
        GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
        PongResponse, SubscribeDeshredRequest, SubscribeGossipRequest, SubscribeReplayInfoRequest,
        SubscribeReplayInfoResponse, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
        SubscribeUpdateGossip,
    },
};
pub use {
    crate::{
        dedup::{DedupState, DedupStream, DEFAULT_SLOT_RETENTION},
        reconnect::{
            AutoReconnect, Backoff, DiscardReason, GrpcConnector, ReplacementReplay, SlotWinner,
            TonicGrpcConnector,
        },
    },
    tonic::{service::Interceptor, transport::ClientTlsConfig},
};
#[cfg(feature = "test-tools")]
use {hyper::rt, tower::Service};

#[cfg(feature = "test-tools")]
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

#[derive(Clone, Debug)]
pub enum ReconnectionPolicy {
    /// Resumes at the latest slot after a disconnect.
    ///
    /// # Warning
    ///
    /// Data produced during the outage is not delivered and cannot be recovered
    /// later. Only use this when your application acts on the current tip and has
    /// no use for history.
    SkipMissedData,
    /// Best-effort replay of retained frozen banks with slot-based dedup.
    /// Does not guarantee complete processed history. See the client README.
    RecoverMissedData { slot_retention: usize },
}

/// Configuration for reconnecting subscriptions and manually composed reconnect streams.
/// Ordinary subscriptions do not reconnect; use `subscribe_with_reconnect` for bank recovery.
#[derive(Clone, Debug)]
pub struct ReconnectConfig {
    /// Retry schedule for reconnect attempts. Resets after each successful
    /// connection, so the budget is per outage, not per stream.
    pub backoff: Backoff,

    /// Policy for manually composed streams; `subscribe_with_reconnect` always uses bank recovery.
    pub policy: ReconnectionPolicy,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            backoff: Backoff::default(),
            policy: ReconnectionPolicy::RecoverMissedData {
                slot_retention: DEFAULT_SLOT_RETENTION,
            },
        }
    }
}

impl ReconnectConfig {
    pub const fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    pub const fn with_slot_retention(mut self, slot_retention: usize) -> Self {
        self.policy = ReconnectionPolicy::RecoverMissedData { slot_retention };
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

/// Identifies a bank within this subscription's connection history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BankRef {
    pub generation: u64,
    pub slot: u64,
    pub bank_id: u64,
}

impl BankRef {
    fn from_update(generation: u64, update: &SubscribeUpdate) -> Option<Self> {
        let (slot, bank_id) = match update.update_oneof.as_ref()? {
            UpdateOneof::Account(m) => (m.slot, m.bank_id?),
            UpdateOneof::Slot(m) => (m.slot, m.bank_id?),
            UpdateOneof::Transaction(m) => (m.slot, m.bank_id),
            UpdateOneof::TransactionStatus(m) => (m.slot, m.bank_id),
            UpdateOneof::Entry(m) => (m.slot, m.bank_id),
            UpdateOneof::Block(m) => (m.slot, m.bank_id),
            UpdateOneof::BlockMeta(m) => (m.slot, m.bank_id),
            UpdateOneof::BlockFooter(m) => (m.slot, m.bank_id),
            _ => return None,
        };
        Some(Self {
            generation,
            slot,
            bank_id,
        })
    }
}

#[derive(Debug)]
pub enum ReconnectEvent {
    Update {
        generation: u64,
        update: SubscribeUpdate,
    },

    /// Remove the listed bank state, then rebuild it from the specified replacement stream.
    /// Winner identities come from finality evidence on the replacement connection.
    DiscardBanks {
        /// Exact connection-scoped banks to remove; do not discard other banks in their slots.
        banks: Vec<BankRef>,
        reason: DiscardReason,
        /// Replacement updates must follow this event. Replay failure terminates with an error.
        replacement: ReplacementReplay,
        /// One outcome per distinct slot in `banks`; skipped slots have an unknown winner.
        winners: Vec<SlotWinner>,
    },
}

/// Immediate subscription updates with connection-scoped bank identities.
///
/// Reconnects invalidate retained banks in the requested replay range before replacement updates.
/// Recovery waits for finalized block identities or proven skipped slots before emitting a discard.
pub struct ReconnectStream<S> {
    inner: S,
    generation: u64,
    delivered_banks: HashSet<BankRef>,
    bank_limit: usize,
    dedup: crate::dedup::CompleteBankDedup,
    pending: VecDeque<SubscribeUpdate>,
    recovery: Option<reconnect::RecoveryDecision>,
    stopped: bool,
}

impl<S> ReconnectStream<S> {
    fn deliver(&mut self, mut update: SubscribeUpdate) -> Result<Option<ReconnectEvent>, Status> {
        let bank = BankRef::from_update(self.generation, &update);
        let visible = reconnect::visible_update(&mut update);
        if visible {
            if matches!(update.update_oneof.as_ref(), Some(UpdateOneof::Account(account)) if account.bank_id.is_none())
            {
                return Err(Status::failed_precondition(
                    "cannot track an account update without bank_id",
                ));
            }
            if let Some(bank) = bank {
                if !self.delivered_banks.contains(&bank)
                    && self.delivered_banks.len() >= self.bank_limit
                {
                    return Err(Status::resource_exhausted(
                        "bank tracking limit reached; cannot forget retained banks before recovery",
                    ));
                }
                self.delivered_banks.insert(bank);
            }
        }
        if let Some(bank) = bank.filter(|bank| self.delivered_banks.contains(bank)) {
            self.dedup.delivered(bank, &update);
        }
        Ok(visible.then_some(ReconnectEvent::Update {
            generation: self.generation,
            update,
        }))
    }

    fn begin_replay(&mut self, generation: u64, from_slot: Option<u64>) -> Result<(), Status> {
        if !self.delivered_banks.is_empty() && from_slot.is_none() {
            return Err(Status::failed_precondition(
                "reconnected without a replay boundary for delivered banks",
            ));
        }
        self.generation = generation;
        self.recovery = None;
        self.pending.clear();
        if let Some(from_slot) = from_slot {
            let mut banks: Vec<_> = self
                .delivered_banks
                .iter()
                .copied()
                .filter(|bank| bank.slot >= from_slot && !self.dedup.is_complete(bank))
                .collect();
            banks.sort_unstable_by_key(|bank| (bank.slot, bank.generation, bank.bank_id));
            self.dedup.begin_replay(&banks);
            if !banks.is_empty() {
                self.recovery = Some(reconnect::RecoveryDecision::new(
                    banks,
                    ReplacementReplay {
                        from_slot,
                        generation,
                    },
                    self.bank_limit,
                ));
            }
        }
        Ok(())
    }
}

impl<S: crate::dedup::ReconnectCounter> ReconnectStream<S> {
    pub fn new(inner: S) -> Self {
        Self::with_bank_limit(inner, 65_536)
    }

    /// Stop before delivering an untrackable bank rather than evict recovery state.
    pub fn with_bank_limit(inner: S, bank_limit: usize) -> Self {
        let generation = u64::from(inner.reconnect_count());
        Self {
            inner,
            generation,
            delivered_banks: HashSet::new(),
            bank_limit,
            dedup: Default::default(),
            pending: Default::default(),
            recovery: None,
            stopped: false,
        }
    }
}

impl<S> ReconnectStream<S>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + crate::dedup::ReconnectCounter + Unpin,
{
    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<ReconnectEvent, Status>> {
        for _ in 0..64 {
            if self.recovery.is_none() {
                if let Some(update) = self.pending.pop_front() {
                    if let Some(event) = self.deliver(update)? {
                        return Poll::Ready(Ok(event));
                    }
                    continue;
                }
            }
            let polled = Pin::new(&mut self.inner).poll_next(cx);
            let generation = u64::from(self.inner.reconnect_count());
            if generation != self.generation && !matches!(polled, Poll::Ready(Some(Err(_)))) {
                self.begin_replay(generation, self.inner.replay_from_slot())?;
            }
            let update = ready!(polled).ok_or_else(|| {
                Status::failed_precondition(
                    "subscription ended before reconnect recovery completed",
                )
            })??;
            if let Some(recovery) = &mut self.recovery {
                recovery.observe(&update)?;
            }
            self.dedup
                .filter(self.generation, update, &mut self.pending);
            if let Some(winners) = self
                .recovery
                .as_ref()
                .and_then(reconnect::RecoveryDecision::winners)
            {
                let recovery = self.recovery.take().unwrap();
                for bank in &recovery.banks {
                    self.delivered_banks.remove(bank);
                }
                return Poll::Ready(Ok(ReconnectEvent::DiscardBanks {
                    banks: recovery.banks,
                    reason: DiscardReason::IncompleteDelivery,
                    replacement: recovery.replacement,
                    winners,
                }));
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl<S> Stream for ReconnectStream<S>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + crate::dedup::ReconnectCounter + Unpin,
{
    type Item = Result<ReconnectEvent, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.stopped {
            return Poll::Ready(None);
        }
        let result = ready!(this.poll_event(cx));
        if result.is_err() {
            this.stopped = true;
            this.recovery = None;
            this.pending.clear();
        }
        Poll::Ready(Some(result))
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
    verified_recovery: bool,
    inner: Arc<Mutex<mpsc::Sender<SubscribeRequest>>>,
    shared: Arc<ArcSwap<SubscribeRequest>>,
}

#[cfg(feature = "test-tools")]
impl SubscribeRequestSink {
    /// Build a `SubscribeRequestSink` backed by an in-memory channel instead
    /// of a live gRPC connection, for tests that need a real
    /// `SubscribeRequestSink` value.
    pub fn mock(sender: mpsc::Sender<SubscribeRequest>) -> Self {
        Self {
            verified_recovery: false,
            inner: Arc::new(Mutex::new(sender)),
            shared: Arc::new(ArcSwap::new(Arc::new(SubscribeRequest::default()))),
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
    #[error(
        "bank replay requires fixed filters; only ping requests may be sent after subscribing"
    )]
    UnverifiedReplay,
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
        if self.verified_recovery && item.ping.is_none() {
            return Err(SubscribeRequestSinkError {
                inner: SubscribeRequestSinkErrorKind::UnverifiedReplay,
            });
        }
        let mut inner = self
            .inner
            .lock()
            .expect("subscribe request sink mutex poisoned");

        inner
            .start_send_unpin(item.clone())
            .map_err(SubscribeRequestSinkError::from)?;
        if !self.verified_recovery {
            self.shared.store(Arc::new(item));
        }
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

    pub(crate) async fn subscribe_raw(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(SubscribeRequestSink, Streaming<SubscribeUpdate>)> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::channel(1000);

        let request = request.unwrap_or_default();

        subscribe_tx
            .send(request.clone())
            .await
            .expect("channel cannot be disconnected or full at this point");

        let response: Response<Streaming<SubscribeUpdate>> =
            self.geyser.subscribe(subscribe_rx).await?;

        let sink = SubscribeRequestSink {
            verified_recovery: false,
            inner: Arc::new(Mutex::new(subscribe_tx)),
            shared: Arc::new(ArcSwap::new(Arc::new(request))),
        };
        Ok((sink, response.into_inner()))
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

    /// Subscribe with immediate bank-aware updates and explicit recovery failures.
    ///
    /// Processed updates are immediate during normal delivery; recovery waits for finalized winners.
    /// Skipped slots report Unknown. Discards precede buffered replacement updates.
    /// Recovery buffers updates without a size cap until finalized winners are known.
    pub async fn subscribe_with_reconnect(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(
        SubscribeRequestSink,
        ReconnectStream<AutoReconnect<Streaming<SubscribeUpdate>, TonicGrpcConnector>>,
    )> {
        let mut request = request.unwrap_or_default();
        if request.commitment.unwrap_or_default() != 0 || self.geyser_client_opts.x_request_snapshot
        {
            return Err(Status::failed_precondition(
                "bank replay requires processed commitment without startup snapshots",
            )
            .into());
        }
        if request.from_slot.is_some() {
            return Err(reconnect::unverified_replay().into());
        }
        request
            .blocks_meta
            .insert(AUTORECONNECT_FILTER_KEY.into(), Default::default());
        request.slots.insert(
            AUTORECONNECT_FILTER_KEY.into(),
            yellowstone_grpc_proto::prelude::SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                ..Default::default()
            },
        );
        let config = self.reconnect_config.clone().unwrap_or_default();
        let (mut sink, stream) = self.subscribe_raw(Some(request)).await?;
        sink.verified_recovery = true;
        let connector = TonicGrpcConnector::new(
            self.reconnect_endpoint
                .clone()
                .unwrap_or_else(|| Endpoint::from_static("http://127.0.0.1:0")),
            config.clone(),
            self.reconnect_x_token.clone(),
            self.geyser_client_opts.clone(),
            Arc::clone(&sink.inner),
        );
        let stream =
            AutoReconnect::new(stream, connector, Arc::clone(&sink.shared), config.backoff)
                .with_bank_replay();
        Ok((sink, ReconnectStream::new(stream)))
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
        super::{GeyserGrpcClient, SubscribeRequest, SubscribeRequestSink},
        crate::reconnect::AUTORECONNECT_FILTER_KEY,
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
            verified_recovery: false,
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

    #[tokio::test]
    async fn ordinary_sink_sends_filter_changes_without_internal_injection() {
        let (tx, mut rx) = mpsc::channel(8);

        let mut initial = SubscribeRequest::default();
        initial
            .blocks_meta
            .insert(AUTORECONNECT_FILTER_KEY.into(), Default::default());

        let shared = Arc::new(ArcSwap::new(Arc::new(initial)));
        let mut sink = SubscribeRequestSink {
            verified_recovery: false,
            inner: Arc::new(Mutex::new(tx)),
            shared: Arc::clone(&shared),
        };

        // user sends a request that does not carry the key
        sink.send(SubscribeRequest::default())
            .await
            .expect("send must succeed");

        let sent = rx.next().await.expect("receiver should get the request");
        assert_eq!(sent, SubscribeRequest::default());
        assert_eq!(**shared.load(), SubscribeRequest::default());
    }
}

#[cfg(test)]
mod bank_recovery_flow_test {
    use super::*;
    use futures::{channel::mpsc, StreamExt};
    use yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdateAccount,
        SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta, SubscribeUpdateSlot,
    };

    type TestStream =
        mpsc::UnboundedReceiver<Result<SubscribeUpdate, tonic::Status>>;

    #[derive(Clone)]
    struct TestConnector {
        replacement: Arc<Mutex<Option<TestStream>>>,
        requests: Arc<Mutex<Vec<Option<u64>>>>,
    }

    impl GrpcConnector for TestConnector {
        type Stream = TestStream;
        type ConnectError = GeyserGrpcClientError;
        type ConnectFuture = std::future::Ready<
            Result<Self::Stream, Self::ConnectError>,
        >;

        fn connect(
            &self,
            _request: Arc<SubscribeRequest>,
            from_slot: Option<u64>,
        ) -> Self::ConnectFuture {
            self.requests.lock().unwrap().push(from_slot);

            std::future::ready(
                self.replacement.lock().unwrap().take().ok_or_else(|| {
                    GeyserGrpcClientError::TonicStatus(
                        Status::unavailable("unexpected extra reconnect"),
                    )
                }),
            )
        }
    }

    fn account(lamports: u64, write_version: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["accounts".into()],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot: 100,
                bank_id: Some(7),
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: vec![1; 32],
                    lamports,
                    write_version,
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn block_meta() -> SubscribeUpdate {
        SubscribeUpdate {
            // Control-only metadata must still reach recovery machinery.
            filters: vec![AUTORECONNECT_FILTER_KEY.into()],
            update_oneof: Some(UpdateOneof::BlockMeta(
                SubscribeUpdateBlockMeta {
                    slot: 100,
                    bank_id: 7,
                    blockhash: "11111111111111111111111111111111".into(),
                    parent_slot: 99,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn finalized() -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![AUTORECONNECT_FILTER_KEY.into()],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 100,
                bank_id: Some(7),
                status: SlotStatus::SlotFinalized as i32,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    async fn next_event<S>(stream: &mut S) -> ReconnectEvent
    where
        S: futures::Stream<Item = Result<ReconnectEvent, Status>> + Unpin,
    {
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended")
            .expect("stream returned an error")
    }

    fn assert_account(
        event: ReconnectEvent,
        expected_generation: u64,
        expected_slot: u64,
        expected_lamports: u64,
        expected_write_version: u64,
    ) {
        let ReconnectEvent::Update { generation, update } = event else {
            panic!("expected an account update, got {event:?}");
        };
        assert_eq!(generation, expected_generation);
        assert_eq!(update.filters, vec!["accounts".to_owned()]);

        let Some(UpdateOneof::Account(account)) = update.update_oneof else {
            panic!("expected account payload");
        };
        assert_eq!(account.slot, expected_slot);
        assert_eq!(account.bank_id, Some(7));

        let info = account.account.unwrap();
        assert_eq!(info.lamports, expected_lamports);
        assert_eq!(info.write_version, expected_write_version);
    }

    #[tokio::test]
    async fn partial_bank_reconnect_waits_for_finality() {
        let (a_tx, a_rx) = mpsc::unbounded();
        let (b_tx, b_rx) = mpsc::unbounded();
        let requests = Arc::new(Mutex::new(Vec::new()));

        let connector = TestConnector {
            replacement: Arc::new(Mutex::new(Some(b_rx))),
            requests: Arc::clone(&requests),
        };

        let inner = AutoReconnect::new(
            a_rx,
            connector,
            Arc::new(ArcSwap::new(Arc::new(SubscribeRequest::default()))),
            Backoff::default(),
        )
        .with_bank_replay();

        let mut stream = ReconnectStream::new(inner);

        // A: processed data arrives immediately, before BlockMeta.
        a_tx.unbounded_send(Ok(account(10, 1))).unwrap();
        assert_account(next_event(&mut stream).await, 0, 100, 10, 1);

        // A disconnects while bank 7 is partial.
        a_tx.unbounded_send(Err(Status::unavailable("disconnect A")))
            .unwrap();

        // Poll through reconnect. B is open but has sent nothing.
        assert!(futures::poll!(stream.next()).is_pending());
        assert_eq!(*requests.lock().unwrap(), vec![Some(100)]);

        // B reuses bank ID 7 and sends repeated writes to one account.
        b_tx.unbounded_send(Ok(account(20, 2))).unwrap();
        b_tx.unbounded_send(Ok(account(30, 3))).unwrap();

        // Replacement data must remain buffered.
        assert!(futures::poll!(stream.next()).is_pending());

        // BlockMeta completes the bank, but finality is still missing.
        b_tx.unbounded_send(Ok(block_meta())).unwrap();
        assert!(futures::poll!(stream.next()).is_pending());

        // Finality permits the discard decision.
        b_tx.unbounded_send(Ok(finalized())).unwrap();

        let event = next_event(&mut stream).await;
        let ReconnectEvent::DiscardBanks {
            banks,
            reason,
            replacement,
            winners,
        } = event else {
            panic!("expected discard before replacement, got {event:?}");
        };

        assert_eq!(
            banks,
            vec![BankRef {
                generation: 0,
                slot: 100,
                bank_id: 7,
            }]
        );
        assert_eq!(reason, DiscardReason::IncompleteDelivery);
        assert_eq!(
            replacement,
            ReplacementReplay {
                from_slot: 100,
                generation: 1,
            }
        );
        assert_eq!(
            winners,
            vec![SlotWinner::Finalized {
                slot: 100,
                blockhash: "11111111111111111111111111111111".into(),
            }]
        );

        // Both replacement writes follow the discard, in order.
        assert_account(next_event(&mut stream).await, 1, 100, 20, 2);
        assert_account(next_event(&mut stream).await, 1, 100, 30, 3);

        // Internal metadata/status messages and duplicates do not leak.
        assert!(futures::poll!(stream.next()).is_pending());

        // Normal processed delivery resumes without waiting for finality.
        let mut live = account(40, 4);
        if let Some(UpdateOneof::Account(account)) = &mut live.update_oneof {
            account.slot = 101;
        }
        b_tx.unbounded_send(Ok(live)).unwrap();

        assert_account(next_event(&mut stream).await, 1, 101, 40, 4);
        assert!(futures::poll!(stream.next()).is_pending());
        assert_eq!(requests.lock().unwrap().len(), 1);
    }
}
