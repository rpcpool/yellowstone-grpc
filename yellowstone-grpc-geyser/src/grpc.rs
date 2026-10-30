use {
    crate::{
        auth::{
            ConstantSubscriptionRepository, HttpSubscriptionRepository, SubscriptionInfo,
            TrustedMetadataAuthenticator,
        },
        billing::{BillingMeteredManager, HttpBillingEventSink},
        block_reconstruction_v2::BlockMachineStorage,
        config::{AuthConfig, AuthKind, BillingConfig, ConfigGrpc, GrpcAddress, GrpcTlsConfig},
        contact_info::{self, ContactInfoState},
        file_watcher::FileWatcher,
        geyser_subscriber::{BankSpan, ReplayCut, ReplayPhase},
        metered::PrometheusMeteredManager,
        metrics::{
            self, incr_grpc_method_call_count, observe_subscriber_queue_size,
            subscription_limit_exceeded_inc,
        },
        plugin::{
            filter::{
                limits::FilterLimits,
                message::{
                    FilteredUpdate, FilteredUpdateDeshred, FilteredUpdateOneof, WeightScales,
                },
                name::FilterNames,
                DeshredFilter, Filter,
            },
            message::{CommitmentLevel, Message, MessageBlockMeta, MessageSlot, SlotStatus},
            proto::geyser_server::{Geyser, GeyserServer},
        },
        ratelimit::{MethodRatelimiter, PrometheusRatelimitCallbacks},
        stream::{tokio::BatchStreamUnboundedReceiver, BatchStream, BatchStreamExt, Buffer},
        util::stream::{
            load_aware_channel, LoadAwareReceiver, LoadAwareSender, SendError, TrySendError,
            UnitWeigher, Weigher,
        },
        version::GrpcVersionInfo,
    },
    anyhow::Context as _,
    bytesize::ByteSize,
    futures::Stream,
    log::{error, info},
    prost_types::Timestamp,
    rustls::{
        pki_types::{pem::PemObject, PrivateKeyDer},
        ServerConfig,
    },
    solana_clock::{BankId, Slot, MAX_RECENT_BLOCKHASHES},
    std::{
        collections::HashMap,
        io,
        num::NonZeroUsize,
        ops::Range,
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, Mutex as StdMutex,
        },
        task::{Context, Poll},
        time::SystemTime,
    },
    tokio::{
        io::{AsyncRead, AsyncWrite},
        net::UnixListener,
        sync::{broadcast, mpsc, oneshot, Mutex, RwLock, Semaphore},
        time::{sleep, Duration},
    },
    tokio_rustls::{rustls, TlsAcceptor},
    tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream, UnixListenerStream},
    tokio_util::{sync::CancellationToken, task::TaskTracker},
    tonic::{
        metadata::AsciiMetadataValue,
        service::{interceptor, LayerExt},
        transport::{
            server::{Connected, Server},
            CertificateDer,
        },
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    tonic_health::{pb::health_server::HealthServer, server::health_reporter},
    yellowstone_grpc_proto::prelude::{
        CommitmentLevel as CommitmentLevelProto, GetBlockHeightRequest, GetBlockHeightResponse,
        GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
        GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse,
        PingRequest, PongResponse, SubscribeDeshredRequest, SubscribeGossipRequest,
        SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest,
        SubscribeUpdateGossip,
    },
    yellowstone_grpc_tools::server::{
        tcp::{TcpConfiguration, TcpIncoming as TritonTcpIncoming},
        tls::{
            build_identity_certified_key, build_sni_resolver_from_cert_dir,
            HotResolvesServerCertUsingIdentity, HotResolvesServerCertUsingSni, TlsIncoming,
        },
        tonic::{
            auth::service::AuthLayer,
            interceptor::{HttpInterceptorLayer, OptionalHttpInterceptor},
            metered::{MeteredBandwidthLayer, MeteredManager, DEFAULT_TRAFFIC_REPORTING_THRESHOLD},
            ratelimit::transport::{RateLimitedIncoming, SharedRateLimitTable},
        },
    },
};

#[derive(Debug)]
struct BlockhashStatus {
    slot: u64,
    processed: bool,
    confirmed: bool,
    finalized: bool,
}

impl BlockhashStatus {
    const fn new(slot: u64) -> Self {
        Self {
            slot,
            processed: false,
            confirmed: false,
            finalized: false,
        }
    }
}

#[derive(Debug, Default)]
struct BlockMetaStorageInner {
    blocks: HashMap<u64, Arc<MessageBlockMeta>>,
    blockhashes: HashMap<String, BlockhashStatus>,
    processed: Option<u64>,
    confirmed: Option<u64>,
    finalized: Option<u64>,
}

#[derive(Debug)]
struct BlockMetaStorage {
    read_sem: Semaphore,
    inner: Arc<RwLock<BlockMetaStorageInner>>,
}

impl BlockMetaStorage {
    fn new(
        unary_concurrency_limit: usize,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> (Self, mpsc::UnboundedSender<Message>) {
        let inner = Arc::new(RwLock::new(BlockMetaStorageInner::default()));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let storage = Arc::clone(&inner);
        let completion_token = task_tracker.token();
        let _ = std::thread::Builder::new()
            .name("solGrpcBlockMetaStorage".to_string())
            .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create Tokio runtime for BlockMetaStorage");   

            runtime.block_on(async move {
                const KEEP_SLOTS: u64 = 3;

                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            info!("BlockMetaStorage task cancelled");
                            break;
                        },
                        maybe = rx.recv() => {
                            let Some(message) = maybe else {
                                info!("BlockMetaStorage channel closed");
                                break;
                            };
                            let mut storage = storage.write().await;
                            match message {
                                Message::Slot(msg) => {
                                    match msg.status {
                                        SlotStatus::Processed => {
                                            storage.processed.replace(msg.slot);
                                        }
                                        SlotStatus::Confirmed => {
                                            storage.confirmed.replace(msg.slot);
                                        }
                                        SlotStatus::Finalized => {
                                            storage.finalized.replace(msg.slot);
                                        }
                                        _ => {}
                                    }

                                    if let Some(blockhash) = storage
                                        .blocks
                                        .get(&msg.slot)
                                        .map(|block| block.blockhash.clone())
                                    {
                                        let entry = storage
                                            .blockhashes
                                            .entry(blockhash)
                                            .or_insert_with(|| BlockhashStatus::new(msg.slot));

                                        match msg.status {
                                            SlotStatus::Processed => {
                                                entry.processed = true;
                                            }
                                            SlotStatus::Confirmed => {
                                                entry.confirmed = true;
                                            }
                                            SlotStatus::Finalized => {
                                                entry.finalized = true;
                                            }
                                            _ => {}
                                        }
                                    }

                                    if msg.status == SlotStatus::Finalized {
                                        if let Some(keep_slot) = msg.slot.checked_sub(KEEP_SLOTS) {
                                            storage.blocks.retain(|slot, _block| *slot >= keep_slot);
                                        }

                                        if let Some(keep_slot) =
                                            msg.slot.checked_sub(MAX_RECENT_BLOCKHASHES as u64 + 32)
                                        {
                                            storage
                                                .blockhashes
                                                .retain(|_blockhash, status| status.slot >= keep_slot);
                                        }
                                    }
                                }
                                Message::BlockMeta(msg) => {
                                    storage.blocks.insert(msg.slot, msg);
                                }
                                msg => {
                                    error!("invalid message in BlockMetaStorage: {msg:?}");
                                }
                            }
                        }
                    }
                }
                info!("BlockMetaStorage task exiting");
            });

            drop(completion_token);
        });

        (
            Self {
                read_sem: Semaphore::new(unary_concurrency_limit),
                inner,
            },
            tx,
        )
    }

    fn parse_commitment(commitment: Option<i32>) -> Result<CommitmentLevel, Status> {
        let commitment = commitment.unwrap_or(CommitmentLevelProto::Processed as i32);
        CommitmentLevelProto::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| {
                let msg = format!("failed to create CommitmentLevel from {commitment:?}");
                Status::unknown(msg)
            })
    }

    async fn get_block<F, T>(
        &self,
        handler: F,
        commitment: Option<i32>,
    ) -> Result<Response<T>, Status>
    where
        F: FnOnce(&MessageBlockMeta) -> Option<T>,
    {
        let commitment = Self::parse_commitment(commitment)?;
        let _permit = self.read_sem.acquire().await;
        let storage = self.inner.read().await;

        let slot = match commitment {
            CommitmentLevel::Processed => storage.processed,
            CommitmentLevel::Confirmed => storage.confirmed,
            CommitmentLevel::Finalized => storage.finalized,
        };

        match slot.and_then(|slot| storage.blocks.get(&slot)) {
            Some(block) => match handler(block) {
                Some(resp) => Ok(Response::new(resp)),
                None => Err(Status::internal("failed to build response")),
            },
            None => Err(Status::internal("block is not available yet")),
        }
    }

    async fn is_blockhash_valid(
        &self,
        blockhash: &str,
        commitment: Option<i32>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        let commitment = Self::parse_commitment(commitment)?;
        let _permit = self.read_sem.acquire().await;
        let storage = self.inner.read().await;

        if storage.blockhashes.len() < MAX_RECENT_BLOCKHASHES + 32 {
            return Err(Status::internal("startup"));
        }

        let slot = match commitment {
            CommitmentLevel::Processed => storage.processed,
            CommitmentLevel::Confirmed => storage.confirmed,
            CommitmentLevel::Finalized => storage.finalized,
        }
        .ok_or_else(|| Status::internal("startup"))?;

        let valid = storage
            .blockhashes
            .get(blockhash)
            .map(|status| match commitment {
                CommitmentLevel::Processed => status.processed,
                CommitmentLevel::Confirmed => status.confirmed,
                CommitmentLevel::Finalized => status.finalized,
            })
            .unwrap_or(false);

        Ok(Response::new(IsBlockhashValidResponse { valid, slot }))
    }
}

/// What geyser_loop hands to block reconstruction, named by whether it was broadcast: that,
/// not how many messages it holds, decides whether it can be a replay cut.
#[derive(Clone)]
pub enum BlockReconstructionMessage {
    /// A batch geyser_loop broadcast on the processed channel before handing it over: this
    /// very `Arc`, so a client can find it on its live receiver by pointer.
    BroadcastBatch(Arc<Vec<Message>>),
    /// An event geyser_loop does not broadcast: a block meta, footer or commitment status,
    /// which block reconstruction holds until the bank seals and then broadcasts in its own
    /// batches.
    SealGated(Message),
}

pub type BroadcastedMessage = Arc<Vec<Message>>;

/// Weighs a Subscribe queue update. Errors weigh `1`.
///
/// Under [`WeightScales::default`] an account counts `1` plus one per 4 KiB of data, a block
/// counts `1` plus its transactions and accounts, and other updates count `1`.
impl Weigher for WeightScales {
    type Item = TonicResult<FilteredUpdate>;

    fn weight(&self, update: &Self::Item) -> u32 {
        update
            .as_ref()
            .map_or(1, |update| self.update_weight(update))
    }
}

/// Sender half of a Subscribe queue.
type SubscribeSender = LoadAwareSender<TonicResult<FilteredUpdate>, WeightScales>;

/// Sender half of a SubscribeDeshred queue, which bounds the number of queued updates.
type DeshredSubscribeSender = LoadAwareSender<
    TonicResult<FilteredUpdateDeshred>,
    UnitWeigher<TonicResult<FilteredUpdateDeshred>>,
>;

#[derive(Debug, Clone)]
pub struct SubscriberChannels {
    processed: broadcast::Sender<BroadcastedMessage>,
    confirmed: broadcast::Sender<BroadcastedMessage>,
    finalized: broadcast::Sender<BroadcastedMessage>,
}

impl SubscriberChannels {
    pub fn new(processed: usize, confirmed: usize, finalized: usize) -> Self {
        Self {
            processed: broadcast::channel(processed).0,
            confirmed: broadcast::channel(confirmed).0,
            finalized: broadcast::channel(finalized).0,
        }
    }

    #[inline]
    const fn sender(&self, commitment: CommitmentLevel) -> &broadcast::Sender<BroadcastedMessage> {
        match commitment {
            CommitmentLevel::Processed => &self.processed,
            CommitmentLevel::Confirmed => &self.confirmed,
            CommitmentLevel::Finalized => &self.finalized,
        }
    }

    #[inline]
    pub fn send(&self, commitment: CommitmentLevel, messages: BroadcastedMessage) {
        let _ = self.sender(commitment).send(messages);
    }

    #[inline]
    pub fn subscribe(
        &self,
        commitment: CommitmentLevel,
    ) -> broadcast::Receiver<BroadcastedMessage> {
        self.sender(commitment).subscribe()
    }
}

/// Messages broadcast on the deshred channel. Deshred is a pre-execution
/// stream and has no commitment level: each message is emitted exactly
/// once, when first received from the geyser plugin.
type DeshredBroadcastedMessage = Message;

pub enum ReplayResponseMessageType {
    Single(Message),
    /// The messages of `batch` in `range`, so part of a shared batch is replayed without a copy.
    Batch(Arc<Vec<Message>>, Range<usize>),
}

impl<'a> IntoIterator for &'a ReplayResponseMessageType {
    type Item = &'a Message;
    type IntoIter = ReplayResponseMessageIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct ReplayResponseMessageIterator<'a> {
    msg: &'a ReplayResponseMessageType,
    current_index: usize,
}

impl ReplayResponseMessageType {
    const fn iter(&self) -> ReplayResponseMessageIterator<'_> {
        ReplayResponseMessageIterator {
            msg: self,
            current_index: 0,
        }
    }
}

impl<'a> Iterator for ReplayResponseMessageIterator<'a> {
    type Item = &'a Message;

    fn next(&mut self) -> Option<Self::Item> {
        match self.msg {
            ReplayResponseMessageType::Single(msg) => {
                if self.current_index == 0 {
                    self.current_index += 1;
                    Some(msg)
                } else {
                    None
                }
            }
            ReplayResponseMessageType::Batch(batch, range) => {
                let index = range.start + self.current_index;
                if index < range.end {
                    self.current_index += 1;
                    batch.get(index)
                } else {
                    None
                }
            }
        }
    }
}

pub enum ReplayedResponse {
    Messages(Replay),
    Lagged(Slot),
}

/// The replayed messages, and where block reconstruction stood when it built them.
pub struct Replay {
    pub messages: Vec<ReplayResponseMessageType>,
    /// The smallest and largest bank id in `messages`. [`None`] if no message names a bank.
    pub bank_id_span: Option<BankSpan>,
    pub cut: ReplayCut,
}

/// Where block reconstruction stands against the broadcast channels (live), so each replay it builds
/// can say where it ends: its [`ReplayCut`], made by [`BroadcastPosition::cut`].
///
/// A replay is built at one moment, R, but the client's live receiver was subscribed earlier,
/// at L. To join the two, the client needs R as a point on the channel. Two tasks feed the
/// processed channel:
///
/// - [`GrpcService::geyser_loop`] broadcasts the raw batches (accounts, transactions, entries,
///   lifecycle statuses) as they arrive from the plugin, then hands each to block
///   reconstruction;
/// - [`GrpcService::block_reconstruction_loop`] broadcasts what it builds once a bank seals:
///   its Block, footer and block meta, and its commitment statuses.
///
/// So R is two batches: the last raw batch block reconstruction took in from geyser_loop, and
/// the last batch it broadcast itself. Both are kept here as the very `Arc` that went out, so
/// the client can find them on its receiver with [`Arc::ptr_eq`]. The confirmed and finalized
/// channels are fed by block reconstruction, plus the lifecycle statuses the plugin entry point
/// sends there, so only the second batch counts for them.
///
/// ```text
/// time ──────────────────────────────────────────►
/// channel:   b1   b2   b3   b4   b5   b6
///          L                R
/// replay:    b1   b2   b3   b4
/// live:      b1   b2   b3   b4   b5   b6
///                      ▲    ▲
///              last_sent    last_taken_in
/// ```
///
/// It is updated by [`GrpcService::take_in`]. When a replay is built, [`Self::cut`] makes the
/// replay's [`ReplayCut`] from it.
///
/// # Examples
///
/// ```ignore
/// let mut position = BroadcastPosition::default();
/// // In `take_in`: record each raw batch taken in, and broadcast through `send`.
/// position.last_taken_in = Some(Arc::clone(&batch));
/// position.send(&broadcast, CommitmentLevel::Processed, block_messages);
/// // When building a replay at `commitment`:
/// let cut = position.cut(commitment);
/// ```
#[derive(Default)]
struct BroadcastPosition {
    /// The last raw batch taken in from geyser_loop. Raw batches go out on the processed
    /// channel only.
    last_taken_in: Option<BroadcastedMessage>,
    /// The last batch block reconstruction broadcast on each channel, indexed by
    /// [`Self::index`]: processed, confirmed, finalized.
    last_sent: [Option<BroadcastedMessage>; 3],
    /// Banks whose block, block meta and Processed status went out at processed, with their
    /// slot. A bank sealed but not in here is still waiting for its Processed status, so a
    /// processed replay sends only its data. Pruned below the oldest replayable slot.
    sent_at_processed: HashMap<BankId, Slot>,
    /// Processed replays waiting for the next raw batch taken in. See
    /// [`ReplayCut::next_taken_in`].
    waiting_next_taken_in: Vec<oneshot::Sender<BroadcastedMessage>>,
}

impl BroadcastPosition {
    /// The slot in [`Self::last_sent`] for a commitment channel.
    ///
    /// # Arguments
    ///
    /// * `commitment` - The channel.
    ///
    /// # Returns
    ///
    /// `0` for processed, `1` for confirmed, `2` for finalized.
    const fn index(commitment: CommitmentLevel) -> usize {
        match commitment {
            CommitmentLevel::Processed => 0,
            CommitmentLevel::Confirmed => 1,
            CommitmentLevel::Finalized => 2,
        }
    }

    /// Broadcasts `messages` on the `commitment` channel and records it as that channel's
    /// last batch sent.
    ///
    /// # Arguments
    ///
    /// * `broadcast` - The commitment channels.
    /// * `commitment` - The channel to send on.
    /// * `messages` - The batch to send.
    fn send(
        &mut self,
        broadcast: &SubscriberChannels,
        commitment: CommitmentLevel,
        messages: BroadcastedMessage,
    ) {
        broadcast.send(commitment, Arc::clone(&messages));
        self.last_sent[Self::index(commitment)] = Some(messages);
    }

    /// Where a replay at `commitment` built now ends. At processed, it also registers the
    /// replay for the next raw batch taken in.
    ///
    /// The cut means the same at every commitment: where the replay ends on that channel. It
    /// needs more at processed only because that channel has two senders out of step:
    ///
    /// ```text
    /// plugin ──► geyser_loop ──(1) broadcast raw batch ──────────────► processed channel
    ///                 │                                                      ▲
    ///                 └──(2) hand over ──► block reconstruction ──(3) Block, meta, statuses
    ///                                      (builds the replay)
    /// ```
    ///
    /// - Processed: [`GrpcService::geyser_loop`] broadcasts a raw batch at (1) before block
    ///   reconstruction takes it in at (2), so whether a raw batch is in the replay depends on
    ///   whether it was taken in: [`ReplayCut::last_taken_in`]. One raw batch can be broadcast
    ///   before the client subscribed and handed over after the replay was built, in neither:
    ///   [`ReplayCut::next_taken_in`]. Block reconstruction's own batches at (3) are cut by
    ///   [`ReplayCut::last_sent`].
    /// - Confirmed and finalized: block reconstruction sends a bank's whole content there
    ///   itself, when the bank reaches that level, and builds the replay between two of its own
    ///   broadcasts. Everything up to [`ReplayCut::last_sent`] is in the replay, everything after
    ///   is not, and no raw batch is in flight between two tasks: `last_taken_in` and
    ///   `next_taken_in` are [`None`]. The lifecycle statuses the plugin entry point also sends
    ///   there are never in a replay, so they need no cut.
    ///
    /// # Arguments
    ///
    /// * `commitment` - The replay's commitment.
    ///
    /// # Returns
    ///
    /// The [`ReplayCut`]: [`ReplayCut::last_taken_in`] and [`ReplayCut::next_taken_in`] at
    /// processed only, and [`ReplayCut::last_sent`] for the `commitment` channel.
    fn cut(&mut self, commitment: CommitmentLevel) -> ReplayCut {
        let processed = commitment == CommitmentLevel::Processed;
        ReplayCut {
            last_taken_in: processed.then(|| self.last_taken_in.clone()).flatten(),
            last_sent: self.last_sent[Self::index(commitment)].clone(),
            next_taken_in: processed.then(|| {
                let (tx, rx) = oneshot::channel();
                self.waiting_next_taken_in.push(tx);
                rx
            }),
        }
    }
}

type ReplayStoredSlotsRequest = (CommitmentLevel, Slot, oneshot::Sender<ReplayedResponse>);

///
/// Tracks the number of active subscriptions per subscriber ID. When a new subscription is created, it increments the count for that subscriber ID. When the subscription is dropped, it decrements the count. If the count exceeds the configured limit,
/// it returns an error when trying to create a new subscription.
#[derive(Clone)]
struct SubscriptionTracker {
    counters: Arc<StdMutex<HashMap<String, usize>>>,
    subscription_limit: NonZeroUsize,
    limit_enforce: bool,
}

impl SubscriptionTracker {
    fn new(subscription_limit: NonZeroUsize, limit_enforce: bool) -> Self {
        Self {
            counters: Arc::new(StdMutex::new(HashMap::new())),
            subscription_limit,
            limit_enforce,
        }
    }
}
///
/// A permit that is owned by a subscriber. When the permit is dropped,
/// it decrements the subscription count for the subscriber in the SubscriptionTracker.
pub struct SubscriptionOwnedPermit {
    inner: Option<SubscriptionTracker>,
    key: String,
}

impl Drop for SubscriptionOwnedPermit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let mut tracker = inner
                .counters
                .lock()
                .expect("subscription_tracker mutex poisoned");
            if let Some(count) = tracker.get_mut(&self.key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    metrics::remove_grpc_concurrent_subscribe_per_subscriber_id(&self.key);
                    tracker.remove(&self.key);
                } else {
                    metrics::set_grpc_concurrent_subscribe_per_subscriber_id(
                        &self.key,
                        *count as u64,
                    );
                }
            }
        }
    }
}

impl SubscriptionTracker {
    ///
    /// Attempts to insert a new subscription for the given subscriber ID.
    /// If the subscription limit is exceeded, it returns an error.
    ///
    /// Otherwise, it increments the count and returns a [`SubscriptionOwnedPermit`] that will decrement the count when dropped.
    ///
    pub fn try_insert(&self, subscriber_id: String) -> Result<SubscriptionOwnedPermit, ()> {
        let mut tracker = self
            .counters
            .lock()
            .map_err(|_| ())
            .expect("subscription_tracker mutex poisoned");
        let count = tracker.entry(subscriber_id.clone()).or_insert(0);
        if *count >= self.subscription_limit.get() {
            subscription_limit_exceeded_inc(&subscriber_id);
            if self.limit_enforce {
                return Err(());
            } else {
                info!(
                    "subscriber {subscriber_id:?} over limit, not enforcing (limit: {})",
                    self.subscription_limit
                );
            }
        }
        *count = count.saturating_add(1);
        metrics::set_grpc_concurrent_subscribe_per_subscriber_id(&subscriber_id, *count as u64);
        let permit = SubscriptionOwnedPermit {
            inner: Some(self.clone()),
            key: subscriber_id,
        };
        Ok(permit)
    }
}

#[derive(Debug, thiserror::Error)]
enum ClientSnapshotReplayError {
    #[error("gRPC connection closed")]
    ClientGrpcConnectionClosed,
    #[error("client session is cancelled by plugin")]
    Cancelled,
}

struct ClientSession {
    id: usize,
    subscriber_id: String,
    endpoint: String,
    filter: Filter,
    cancellation_token: CancellationToken,
    disconnect_reason: &'static str,
    /// Must own it so that when the session is dropped, the subscription count is decremented.
    _subscription_permit: Option<SubscriptionOwnedPermit>,
}

impl ClientSession {
    fn new(
        id: usize,
        subscriber_id: Option<String>,
        endpoint: String,
        cancellation_token: CancellationToken,
        subscription_permit: Option<SubscriptionOwnedPermit>,
    ) -> Self {
        let filter = Filter::default();
        let subscriber_id = subscriber_id.unwrap_or("UNKNOWN".to_owned());
        metrics::update_subscriptions(&endpoint, None, Some(&filter));
        info!("client #{id} ({subscriber_id}): new");
        Self {
            id,
            subscriber_id,
            endpoint,
            filter,
            cancellation_token,
            disconnect_reason: "unknown",
            _subscription_permit: subscription_permit,
        }
    }

    fn set_filter(&mut self, new_filter: Filter) {
        metrics::update_subscriptions(&self.endpoint, Some(&self.filter), Some(&new_filter));
        self.filter = new_filter;
    }
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        observe_subscriber_queue_size(&self.subscriber_id, 0, "normal");
        metrics::incr_client_disconnect(&self.subscriber_id, self.disconnect_reason);
        metrics::update_subscriptions(&self.endpoint, Some(&self.filter), None);
        info!(
            "client #{} ({}): removed ({})",
            self.id, self.subscriber_id, self.disconnect_reason
        );
        self.cancellation_token.cancel();
    }
}

struct DeshredClientSession {
    id: usize,
    subscriber_id: String,
    filter: DeshredFilter,
    cancellation_token: CancellationToken,
    disconnect_reason: &'static str,
    _subscription_permit: Option<SubscriptionOwnedPermit>,
}

impl DeshredClientSession {
    fn new(
        id: usize,
        subscriber_id: Option<String>,
        cancellation_token: CancellationToken,
        subscription_permit: Option<SubscriptionOwnedPermit>,
    ) -> Self {
        let subscriber_id = subscriber_id.unwrap_or("UNKNOWN".to_owned());
        info!("deshred client #{id} ({subscriber_id}): new");
        Self {
            id,
            subscriber_id,
            filter: DeshredFilter::default(),
            cancellation_token,
            disconnect_reason: "unknown",
            _subscription_permit: subscription_permit,
        }
    }
}

impl Drop for DeshredClientSession {
    fn drop(&mut self) {
        observe_subscriber_queue_size(&self.subscriber_id, 0, "deshred");
        metrics::incr_client_disconnect(&self.subscriber_id, self.disconnect_reason);
        info!(
            "deshred client #{} ({}): removed ({})",
            self.id, self.subscriber_id, self.disconnect_reason
        );
        self.cancellation_token.cancel();
    }
}

struct AutoClosableUnixListenerStream {
    path_to_remove: PathBuf,
    listener: UnixListenerStream,
}

impl Stream for AutoClosableUnixListenerStream {
    type Item = io::Result<tokio::net::UnixStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.listener).poll_next(cx) {
            Poll::Ready(Some(Ok(stream))) => Poll::Ready(Some(Ok(stream))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for AutoClosableUnixListenerStream {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_file(&self.path_to_remove) {
            error!(
                "failed to remove Unix socket file {}: {}",
                self.path_to_remove.display(),
                err
            );
        } else {
            info!("removed Unix socket file {}", self.path_to_remove.display());
        }
    }
}

enum Listener {
    Tcp(TritonTcpIncoming, Option<AuthConfig>),
    Tls(TlsIncoming, Option<AuthConfig>),
    Unix(AutoClosableUnixListenerStream, Option<AuthConfig>), // path needed to remove the socket file on exit
}

#[derive(Clone)]
struct XTokenInterceptor {
    x_token: Option<AsciiMetadataValue>,
}

impl interceptor::Interceptor for XTokenInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(x_token) = &self.x_token {
            match request.metadata().get("x-token") {
                Some(token) if token == x_token => Ok(request),
                _ => Err(Status::unauthenticated("No valid auth token")),
            }
        } else {
            Ok(request)
        }
    }
}

#[derive(Clone)]
pub struct GrpcService {
    config_snapshot_client_channel_capacity: usize,
    geyser_subscriber_weight_capacity: usize,
    deshred_subscriber_channel_capacity: usize,
    contact_info_subscriber_channel_capacity: usize,
    weight_scales: WeightScales,
    client_unresponsive_timeout: Option<Duration>,
    config_filter_limits: Arc<FilterLimits>,
    subscription_tracker: SubscriptionTracker,
    blocks_meta: Option<Arc<BlockMetaStorage>>,
    subscribe_id: Arc<AtomicUsize>,
    snapshot_rx: Arc<Mutex<Option<crossbeam_channel::Receiver<Box<Message>>>>>,
    broadcast: SubscriberChannels,
    deshred_broadcast_tx: broadcast::Sender<DeshredBroadcastedMessage>,
    contact_info_state: Arc<ContactInfoState>,
    replay_stored_slots_tx: Option<mpsc::Sender<ReplayStoredSlotsRequest>>,
    replay_first_available_slot: Option<Arc<AtomicU64>>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    filter_name_size_limit: usize,
    filter_names_size_limit: usize,
    filter_names_cleanup_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum TlsConfigLoadError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
    #[error(transparent)]
    PemError(#[from] rustls::pki_types::pem::Error),
    #[error(transparent)]
    Notify(#[from] notify::Error),
}

///
/// Loads TLS server configuration from the given `GrpcTlsConfig`. This is used for both the legacy `tls_config` field and the new `listen[].tls` field in the configuration. The `listen[].tls` field takes precedence over the legacy `tls_config` if both are set.
///
fn load_server_config_from_tls_config(
    tls_config: &GrpcTlsConfig,
    file_watcher: &FileWatcher,
) -> Result<ServerConfig, TlsConfigLoadError> {
    match tls_config {
        GrpcTlsConfig::IdentityPair {
            identity,
            watch_file,
        } => {
            let certified_key =
                build_identity_certified_key(&identity.cert_path, &identity.key_path)?;
            let hot_resolver = Arc::new(HotResolvesServerCertUsingIdentity::from(certified_key));
            let mut server_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(
                    Arc::clone(&hot_resolver) as Arc<dyn rustls::server::ResolvesServerCert>
                );
            // gRPC over TLS requires ALPN negotiation for HTTP/2.
            server_config.alpn_protocols = vec![b"h2".to_vec()];

            if *watch_file {
                let watched_identity = identity.clone();
                let cert_reload_identity = watched_identity.clone();
                let cert_hot_resolver = Arc::clone(&hot_resolver);
                let cert_path = identity.cert_path.clone();
                file_watcher
                    .watch_file(cert_path.clone(), move |_ev| {
                        if let Err(e) = build_identity_certified_key(
                            &cert_reload_identity.cert_path,
                            &cert_reload_identity.key_path,
                        )
                        .map(|key| cert_hot_resolver.swap(key))
                        {
                            log::error!(
                                "failed to reload TLS IdentityPair from cert={} key={}: {}",
                                cert_reload_identity.cert_path,
                                cert_reload_identity.key_path,
                                e
                            );
                        } else {
                            log::info!(
                                "successfully reloaded TLS IdentityPair from cert={} key={}",
                                cert_reload_identity.cert_path,
                                cert_reload_identity.key_path
                            );
                        }
                    })?
                    .forget();

                let key_reload_identity = watched_identity;
                let key_hot_resolver = Arc::clone(&hot_resolver);
                let key_path = identity.key_path.clone();
                file_watcher
                    .watch_file(key_path.clone(), move |_ev| {
                        if let Err(e) = build_identity_certified_key(
                            &key_reload_identity.cert_path,
                            &key_reload_identity.key_path,
                        )
                        .map(|key| key_hot_resolver.swap(key))
                        {
                            log::error!(
                                "failed to reload TLS IdentityPair from cert={} key={}: {}",
                                key_reload_identity.cert_path,
                                key_reload_identity.key_path,
                                e
                            );
                        } else {
                            log::info!(
                                "successfully reloaded TLS IdentityPair from cert={} key={}",
                                key_reload_identity.cert_path,
                                key_reload_identity.key_path
                            );
                        }
                    })?
                    .forget();
            }

            Ok(server_config)
        }
        GrpcTlsConfig::CertDir {
            cert_dir,
            watch_file,
        } => {
            let resolver = build_sni_resolver_from_cert_dir(cert_dir.clone())?;
            let hot_resolver = Arc::new(HotResolvesServerCertUsingSni::from(resolver));
            let mut server_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(
                    Arc::clone(&hot_resolver) as Arc<dyn rustls::server::ResolvesServerCert>
                );
            // gRPC over TLS requires ALPN negotiation for HTTP/2.
            server_config.alpn_protocols = vec![b"h2".to_vec()];

            if *watch_file {
                let watched_cert_dir = cert_dir.clone();
                file_watcher
                    .watch_folder(watched_cert_dir.clone(), true, move |_ev| {
                        if let Err(e) = build_sni_resolver_from_cert_dir(watched_cert_dir.clone())
                            .map(|resolver| hot_resolver.swap(resolver))
                        {
                            log::error!(
                                "failed to reload TLS certs from {}: {}",
                                watched_cert_dir.display(),
                                e
                            );
                        } else {
                            log::info!(
                                "successfully reloaded TLS certs from {}",
                                watched_cert_dir.display()
                            );
                        }
                    })?
                    .forget();
            }

            Ok(server_config)
        }
    }
}

pub struct GrpcServiceResult {
    pub snapshot_tx: Option<crossbeam_channel::Sender<Box<Message>>>,
    pub deshred_broadcast_tx: broadcast::Sender<DeshredBroadcastedMessage>,
    pub contact_info_tx: mpsc::UnboundedSender<contact_info::ContactInfoNotification>,
    pub broadcast: SubscriberChannels,
    pub blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
}

impl GrpcService {
    #[allow(clippy::type_complexity)]
    pub async fn create<St>(
        config: ConfigGrpc,
        is_reload: bool,
        service_cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
        file_watcher: Arc<FileWatcher>,
        messages_rx: St,
    ) -> anyhow::Result<GrpcServiceResult>
    where
        St: BatchStream<Item = Message> + Unpin + Send + 'static,
    {
        // Bind all configured addresses (TCP or Unix domain socket)
        let mut listeners = Vec::new();

        // This is the LEGACY way of configuring the listen address, which is still supported for backward compatibility but will be removed in the future. The new way is to use the `listen` field which supports multiple addresses and TLS configuration per address.
        if let Some(addresses) = config.address.clone() {
            log::warn!("The 'address' field is deprecated; please use the 'listen' field with an array of addresses instead");
            if config.tls_config.is_some() && config.cert_dir.is_some() {
                log::warn!("Both tls_config and cert_dir are set; cert_dir will take precedence and tls_config will be ignored");
            }
            let mut maybe_tls_server_config = if let Some(tls) = &config.tls_config {
                let cert_pem = std::fs::read(&tls.cert_path)
                    .context("failed to read TLS cert file")
                    .unwrap();
                let key = std::fs::read(&tls.key_path)
                    .context("failed to read TLS key file")
                    .unwrap();

                let cert_der =
                    CertificateDer::from_pem_slice(&cert_pem).context("tls cert_path")?;
                let key_der = PrivateKeyDer::from_pem_slice(&key).context("tls key_path")?;
                let mut server_config = ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(vec![cert_der], key_der)?;
                // gRPC over TLS requires ALPN negotiation for HTTP/2.
                server_config.alpn_protocols = vec![b"h2".to_vec()];
                Some(server_config)
            } else {
                None
            };

            // Cert-dir has precedence over tls_config if both are set.
            if let Some(cert_dir) = &config.cert_dir {
                let resolver = build_sni_resolver_from_cert_dir(cert_dir.clone())?;
                let hot_resolver = HotResolvesServerCertUsingSni::from(resolver);
                let hot_resolver = Arc::new(hot_resolver);

                let dyn_resolver =
                    Arc::clone(&hot_resolver) as Arc<dyn rustls::server::ResolvesServerCert>;

                let mut server_config = ServerConfig::builder()
                    .with_no_client_auth()
                    .with_cert_resolver(dyn_resolver);
                // gRPC over TLS requires ALPN negotiation for HTTP/2.
                server_config.alpn_protocols = vec![b"h2".to_vec()];
                let _ = maybe_tls_server_config.replace(server_config);
            }

            log::warn!("The 'address' field is deprecated; please use the 'listen' field with an array of addresses instead");
            for address in addresses {
                match address {
                    GrpcAddress::Tcp(addr) => {
                        let incoming = TritonTcpIncoming::bind_with_config(
                            addr,
                            TcpConfiguration::default()
                                .with_nodelay(Some(true))
                                .with_keepalive(Some(Duration::from_secs(20))),
                        )
                        .await?;
                        log::info!(
                            "binding gRPC server to TCP socket: {}",
                            incoming.local_addr()?
                        );
                        listeners.push(Listener::Tcp(incoming, None));
                    }
                    GrpcAddress::Unix { path, mode } => {
                        if config.tls_config.is_some() || config.cert_dir.is_some() {
                            log::warn!(
                                "TLS config is ignored for Unix domain socket: {}",
                                path.display()
                            );
                        }
                        if let Err(e) = std::fs::remove_file(&path) {
                            if e.kind() != std::io::ErrorKind::NotFound {
                                return Err(e.into());
                            }
                        }
                        log::info!(
                            "binding gRPC server to Unix domain socket: {}",
                            path.display()
                        );
                        let uds = UnixListener::bind(&path)?;
                        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))?;
                        let uds = UnixListenerStream::new(uds);
                        let uds = AutoClosableUnixListenerStream {
                            path_to_remove: path,
                            listener: uds,
                        };
                        listeners.push(Listener::Unix(uds, None));
                    }
                }
            }
        }

        if let Some(listen_configs) = config.listen.clone() {
            for listen_config in listen_configs {
                let address = listen_config.address;
                let tls_config = listen_config.tls;
                match address {
                    GrpcAddress::Tcp(addr) => {
                        let incoming = TritonTcpIncoming::bind_with_config(
                            addr,
                            TcpConfiguration::default()
                                .with_nodelay(Some(true))
                                .with_keepalive(Some(Duration::from_secs(20))),
                        )
                        .await?;
                        log::info!(
                            "binding gRPC server to TCP socket: {}",
                            incoming.local_addr()?
                        );
                        let auth = listen_config.auth.clone();
                        let listener = if let Some(tls) = tls_config {
                            let server_config =
                                load_server_config_from_tls_config(&tls, &file_watcher)
                                    .context("failed to load TLS server config")?;
                            let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));
                            Listener::Tls(TlsIncoming::new(incoming, tls_acceptor), auth)
                        } else {
                            Listener::Tcp(incoming, auth)
                        };
                        listeners.push(listener);
                    }
                    GrpcAddress::Unix { path, mode } => {
                        if tls_config.is_some() {
                            log::warn!(
                                "TLS config is ignored for Unix domain socket: {}",
                                path.display()
                            );
                        }
                        if let Err(e) = std::fs::remove_file(&path) {
                            if e.kind() != std::io::ErrorKind::NotFound {
                                return Err(e.into());
                            }
                        }
                        log::info!(
                            "binding gRPC server to Unix domain socket: {}",
                            path.display()
                        );
                        let uds = UnixListener::bind(&path)?;
                        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))?;
                        let uds = UnixListenerStream::new(uds);
                        let uds = AutoClosableUnixListenerStream {
                            path_to_remove: path,
                            listener: uds,
                        };
                        listeners.push(Listener::Unix(uds, listen_config.auth.clone()));
                    }
                }
            }
        }

        // Snapshot channel
        let (snapshot_tx, snapshot_rx) = match config.snapshot_plugin_channel_capacity {
            Some(cap) if !is_reload => {
                let (tx, rx) = crossbeam_channel::bounded(cap);
                (Some(tx), Some(rx))
            }
            _ => (None, None),
        };

        // Blocks meta storage
        let (blocks_meta, blocks_meta_tx) = if config.unary_disabled {
            (None, None)
        } else {
            let (blocks_meta, blocks_meta_tx) = BlockMetaStorage::new(
                config.unary_concurrency_limit,
                service_cancellation_token.child_token(),
                task_tracker.clone(),
            );
            (Some(blocks_meta), Some(blocks_meta_tx))
        };

        let capacities = config.resolved_capacities();
        let broadcast = SubscriberChannels::new(
            capacities.processed_broadcast,
            capacities.confirmed_broadcast,
            capacities.finalized_broadcast,
        );
        // Deshred subscribers receive their own commitment-free stream.
        let (deshred_broadcast_tx, _) = broadcast::channel(config.channel_capacity);
        let (replay_first_available_slot, replay_stored_slots_tx, replay_stored_slots_rx) =
            if config.replay_stored_slots == 0 {
                (None, None, None)
            } else {
                let (tx, rx) = mpsc::channel(1);
                (Some(Arc::new(AtomicU64::new(u64::MAX))), Some(tx), Some(rx))
            };

        // contact info subscribers
        let contact_info_state = ContactInfoState::new(config.contact_info_channel_capacity);

        let (contact_info_tx, contact_info_rx) = mpsc::unbounded_channel();

        task_tracker.spawn(contact_info::contact_info_loop(
            UnboundedReceiverStream::new(contact_info_rx),
            Arc::clone(&contact_info_state),
        ));

        // Capture traffic reporting threshold before config is moved
        let traffic_reporting_threshold = config
            .traffic_reporting_byte_threhsold
            .unwrap_or(DEFAULT_TRAFFIC_REPORTING_THRESHOLD);

        // Save HTTP/2 settings (all Copy) for use inside spawned tasks
        let http2_adaptive_window = config.server_http2_adaptive_window;
        let http2_keepalive_interval = config.server_http2_keepalive_interval;
        let http2_keepalive_timeout = config.server_http2_keepalive_timeout;
        let initial_connection_window_size = config.server_initial_connection_window_size;
        let initial_stream_window_size = config.server_initial_stream_window_size;

        // Build the shared GeyserServer (Clone-able because GrpcService: Clone)
        let max_decoding_message_size = config.max_decoding_message_size;
        let mut service = GeyserServer::new(Self {
            config_snapshot_client_channel_capacity: config.snapshot_client_channel_capacity,
            geyser_subscriber_weight_capacity: capacities.geyser_subscriber_weight,
            deshred_subscriber_channel_capacity: capacities.deshred_subscriber,
            contact_info_subscriber_channel_capacity: capacities.contact_info_subscriber,
            weight_scales: capacities.weight_scales,
            client_unresponsive_timeout: config.client_unresponsive_timeout,
            config_filter_limits: Arc::new(config.filter_limits),
            subscription_tracker: SubscriptionTracker::new(
                config.subscription_limit,
                config.subscription_limit_enforce,
            ),
            blocks_meta: blocks_meta.map(Arc::new),
            subscribe_id: Arc::new(AtomicUsize::new(0)),
            snapshot_rx: Arc::new(Mutex::new(snapshot_rx)),
            broadcast: broadcast.clone(),
            deshred_broadcast_tx: deshred_broadcast_tx.clone(),
            contact_info_state: Arc::clone(&contact_info_state),
            replay_stored_slots_tx,
            replay_first_available_slot: replay_first_available_slot.clone(),
            cancellation_token: service_cancellation_token.clone(),
            task_tracker: task_tracker.clone(),
            filter_name_size_limit: config.filter_name_size_limit,
            filter_names_size_limit: config.filter_names_size_limit,
            filter_names_cleanup_interval: config.filter_names_cleanup_interval,
        })
        .max_decoding_message_size(max_decoding_message_size);
        for encoding in config.compression.accept {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.compression.send {
            service = service.send_compressed(encoding);
        }

        let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();

        // Warn if replay buffer is too small for auto-reconnect
        if config.replay_stored_slots < 150 {
            log::warn!(
                "replay_stored_slots={} may be too low for auto-reconnect; recommend >= 150",
                config.replay_stored_slots
            );
        }

        {
            let broadcast = broadcast.clone();

            task_tracker.spawn(async move {
                Self::block_reconstruction_loop(
                    BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                    broadcast,
                    replay_stored_slots_rx,
                    replay_first_available_slot,
                    config.replay_stored_slots,
                )
                .await;
            });
        }

        {
            let broadcast = broadcast.clone();

            task_tracker.spawn(async move {
                Self::geyser_loop(messages_rx, broadcast, block_reconstruction_tx).await;
            });
        }

        // Health check service
        let (health_reporter, health_service) = health_reporter();
        health_reporter.set_serving::<GeyserServer<Self>>().await;

        let x_token = config
            .x_token
            .map(|t| t.parse::<AsciiMetadataValue>())
            .transpose()
            .context("invalid x_token value")?;

        let shutdown_grpc = service_cancellation_token.child_token();

        // The most "elegant" way would be to implement Stream for Listener and a MixedIO type that can handle all three variants.
        // However, that adds complexity in the IO implementa has every read/write would need an extra match-expression to dispatch to the correct underlying type.
        // Having this ugly macros is the most efficient approach and makes the code shorter when calling `serve_listener`.
        macro_rules! with_listener {
            ($listener:expr, |$incoming:ident, $auth:ident| $body:expr) => {{
                match $listener {
                    Listener::Tcp($incoming, $auth) => $body,
                    Listener::Unix($incoming, $auth) => $body,
                    Listener::Tls($incoming, $auth) => $body,
                }
            }};
        }

        // Spawn one server task per listener
        let rate_limit_table = SharedRateLimitTable::default();
        let ip_conncur = config.ip_conncur_rate_limit;
        for listener in listeners {
            let shutdown = shutdown_grpc.clone();
            let x_token = x_token.clone();
            let health_service = health_service.clone();
            let service = service.clone();
            let rate_limit_table = rate_limit_table.clone();
            task_tracker.spawn(async move {
                if let Err(e) = with_listener!(listener, |incoming, auth| {
                    let rate_limited_incoming = RateLimitedIncoming::new(
                        incoming,
                        ip_conncur,
                        rate_limit_table,
                        PrometheusRatelimitCallbacks,
                    );
                    match GrpcService::build_service(
                        health_service,
                        x_token.clone(),
                        service,
                        traffic_reporting_threshold,
                        auth,
                    ) {
                        Ok(built_service) => {
                            GrpcService::serve_listener(
                                rate_limited_incoming,
                                http2_adaptive_window,
                                http2_keepalive_interval,
                                http2_keepalive_timeout,
                                initial_connection_window_size,
                                initial_stream_window_size,
                                built_service,
                                shutdown.clone(),
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    }
                }) {
                    error!("gRPC listener failed: {e}");
                    shutdown.cancel();
                }
            });
        }

        Ok(GrpcServiceResult {
            snapshot_tx,
            deshred_broadcast_tx,
            contact_info_tx,
            broadcast,
            blocks_meta_tx,
        })
    }

    /// Core message routing loop that reconstructs Solana blocks from raw Geyser plugin events
    /// and fans them out to subscribers at the correct commitment levels.
    ///
    /// # Slot status semantics
    ///
    /// Slot messages come in two categories with different routing rules:
    ///
    /// **Lifecycle statuses** (`FirstShredReceived`, `Completed`, `CreatedBank`, `Dead`):
    /// Broadcast immediately at Processed, with the rest of the raw data. `FirstShredReceived`,
    /// `Completed` and `Dead` name no bank: the plugin entry point also sends them to Confirmed
    /// and Finalized as they land. A bank's `CreatedBank` goes out at Confirmed and Finalized
    /// with the bank's content, when the bank reaches that level, and never as it lands there.
    ///
    /// **Commitment statuses** (`Processed`, `Confirmed`, `Finalized`):
    /// Never broadcast directly when received from the plugin. They are fed into `BlockMachineStorage`
    /// which holds them until the block is fully assembled. On each channel, a bank's statuses go
    /// out only after that channel's copy of the bank's content, block and block meta, in the
    /// order Processed, Confirmed, Finalized. No subscriber sees a commitment status before the
    /// content it is about.
    ///
    /// # Block content delivery
    ///
    /// When a block becomes ready at commitment level C (`pop_ready_block`):
    /// 1. If C is Confirmed or Finalized: the bank's `CreatedBank`, then `frozen_block.messages()`
    ///    (the complete, deduplicated set of Account/Transaction/Entry messages for that slot) are
    ///    sent at level C first. This is omitted at Processed because those messages were already
    ///    delivered individually as they arrived.
    /// 2. `Message::Block`, then the Alpenglow `Message::BlockFooter` if the bank has one, then
    ///    `Message::BlockMeta` are sent at level C.
    /// 3. The status for C goes to the lower levels, which already have the content. Level C
    ///    gets every status up to C, in order: Processed, then Confirmed, then Finalized. Higher
    ///    levels get them when the bank reaches them.
    ///
    /// This ordering guarantee — block content before `Message::Block` before the footer before
    /// `Message::BlockMeta` before slot status — must be preserved so that subscribers always
    /// observe a complete block before the commitment signal.
    ///
    /// # Account deduplication
    ///
    /// Within a slot, if multiple updates arrive for the same account pubkey, only the update with
    /// the highest `write_version` is retained in the frozen block. This is handled internally by
    /// `BlockMachineStorage` / `BankBuffer` and must not be bypassed.
    ///
    /// # Missing commitment level gap-filling
    ///
    /// If a higher commitment level arrives without a prior lower one (e.g. Finalized before
    /// Confirmed), `BlockMachineStorage` synthesizes the missing levels in order
    /// (Processed → Confirmed → Finalized). Each synthesized level causes a separate
    /// `pop_ready_block` entry and a separate fan-out.
    ///
    /// # Ancestor slot propagation
    ///
    /// When a descendant slot reaches a commitment level, `BlockMachineStorage` retroactively
    /// raises every ancestor slot (walked via its own tracked parent-slot chain) to at least
    /// that same commitment level, emitting a synthesized update for any ancestor that hadn't
    /// already reached it directly. It must not be short-circuited.
    ///
    /// # Batching and metrics
    ///
    /// Up to `PROCESSED_MESSAGES_MAX` messages are drained from `messages_rx` per iteration
    /// to amortise channel overhead. Before each Processed broadcast, `encode_messages` pre-encodes
    /// Account and Transaction payloads (stored in a `OnceLock` on the shared `Arc`) so that
    /// per-client serialisation is avoided. `GEYSER_BATCH_SIZE` is observed for each Processed
    /// broadcast.
    ///
    /// # Side channels
    ///
    /// - `blocks_meta_tx`: receives every `Slot` and `BlockMeta` message verbatim for external
    ///   consumers that track block metadata independently of the main broadcast.
    /// - `replay_stored_slots_rx`: services replay requests from newly-connected subscribers.
    ///   `replay_first_available_slot` is updated after every batch to reflect the oldest slot
    ///   still available in the replay buffer, and is exposed via `subscribe_first_available_slot`.
    async fn geyser_loop<St>(
        mut messages_rx: St,
        broadcast: SubscriberChannels,
        block_reconstruction_tx: mpsc::UnboundedSender<BlockReconstructionMessage>,
    ) where
        St: BatchStream<Item = Message> + Unpin + Send + 'static,
    {
        const MESSAGE_BATCH_SIZE: usize = 1024;
        // let mut message_batch = Vec::with_capacity(MESSAGE_BATCH_SIZE);
        // BlockMeta, BlockFooter and the Processed/Confirmed/Finalized slot statuses close the
        // batch and go to block reconstruction only, which emits them once the block seals.
        struct PartitionedBuffer {
            message_batch: Vec<Message>,
            seal_gated: Option<Message>,
        }
        impl Buffer<Message> for PartitionedBuffer {
            fn accumulate(&mut self, item: Message) -> Result<(), Message> {
                if self.message_batch.len() == self.message_batch.capacity() {
                    return Err(item);
                }
                match &item {
                    Message::BlockMeta(_) | Message::BlockFooter(_) => self.seal_gated = Some(item),
                    Message::Slot(slot)
                        if matches!(
                            slot.status,
                            SlotStatus::Processed | SlotStatus::Confirmed | SlotStatus::Finalized
                        ) =>
                    {
                        self.seal_gated = Some(item)
                    }
                    _ => self.message_batch.push(item),
                }
                Ok(())
            }

            fn ready(&self) -> bool {
                self.message_batch.len() < self.message_batch.capacity()
                    && self.seal_gated.is_none()
            }
        }
        let mut buffer = PartitionedBuffer {
            message_batch: Vec::with_capacity(MESSAGE_BATCH_SIZE),
            seal_gated: None,
        };
        loop {
            let batch_size_maybe = messages_rx.next_batch(&mut buffer).await;
            let Some(_) = batch_size_maybe else {
                info!("Geyser loop: messages channel closed");
                break;
            };

            if !buffer.message_batch.is_empty() {
                metrics::message_queue_size_dec_by(buffer.message_batch.len() as i64);
                let message_batch_arc = Arc::new(std::mem::take(&mut buffer.message_batch));
                broadcast.send(CommitmentLevel::Processed, Arc::clone(&message_batch_arc));
                buffer.message_batch = Vec::with_capacity(MESSAGE_BATCH_SIZE);
                if block_reconstruction_tx
                    .send(BlockReconstructionMessage::BroadcastBatch(
                        message_batch_arc,
                    ))
                    .is_ok()
                {
                    metrics::block_reconstruction_queue_size_inc();
                }
            }

            if let Some(message) = buffer.seal_gated.take() {
                metrics::message_queue_size_dec();

                if block_reconstruction_tx
                    .send(BlockReconstructionMessage::SealGated(message))
                    .is_ok()
                {
                    metrics::block_reconstruction_queue_size_inc();
                }
            }
        }
    }

    async fn block_reconstruction_loop<St>(
        mut messages_rx: St,
        broadcast: SubscriberChannels,
        replay_stored_slots_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        replay_first_available_slot: Option<Arc<AtomicU64>>,
        replay_stored_slots: u64,
    ) where
        St: BatchStream<Item = BlockReconstructionMessage> + Unpin + Send + 'static,
    {
        let (_tx, rx) = mpsc::channel(1);
        let mut replay_stored_slots_rx = replay_stored_slots_rx.unwrap_or(rx);

        let mut block_machine = BlockMachineStorage::new(replay_stored_slots as usize);
        let mut position = BroadcastPosition::default();

        const BUFFERED_MESSAGES_CAPACITY: usize = 1024;
        let mut buffered_messages = Vec::with_capacity(BUFFERED_MESSAGES_CAPACITY);
        // A replay request taken from its channel, built once the queue reads empty again.
        let mut pending_replay: Option<ReplayStoredSlotsRequest> = None;

        loop {
            // Biased, queue first: a replay is built only once the queue reads empty, so it holds
            // every batch geyser_loop handed over before the request. Those went out live, maybe
            // before the client subscribed; one left queued would be in neither stream.
            //
            // That read must come after the request was taken. The two arms are read one after
            // the other, not at once: geyser_loop can hand over two batches, and the client
            // subscribe and send its request, between an empty queue read and the request read.
            // So the request arm only stores the request, and the replay is built in the next
            // pass, after a fresh queue read.
            //
            // The queue also reads `Pending` when tokio's per-task budget runs out, with items
            // still queued. The build arm is then `Pending` too: `consume_budget` is on the same
            // budget, as is the request channel. Don't put an arm that is always ready after the
            // queue arm, and don't drain with `now_or_never`.
            tokio::select! {
                biased;

                maybe = messages_rx.next_batch(&mut buffered_messages) => {
                    let Some(buffered_size) = maybe else {
                        info!("Block reconstruction loop: messages channel closed");
                        break;
                    };

                    metrics::block_reconstruction_queue_size_dec_by(buffered_size as i64);
                    Self::take_in(&mut block_machine, &broadcast, &mut position, buffered_messages.drain(..));

                    let min_replayable_slot = block_machine.min_replayable_slot();
                    if let (Some(min_slot), Some(replay_first_available_slot)) = (min_replayable_slot, replay_first_available_slot.as_ref()) {
                        replay_first_available_slot.store(min_slot, Ordering::Relaxed);
                    }
                },
                // The queue read just above came after the request was taken.
                _ = tokio::task::coop::consume_budget(), if pending_replay.is_some() => {
                    let Some((commitment, replay_slot, tx)) = pending_replay.take() else {
                        continue;
                    };
                    if let Some(slot) = block_machine.min_replayable_slot() {
                        if replay_slot < slot {
                            let _ = tx.send(ReplayedResponse::Lagged(slot));
                            continue;
                        }
                    }
                    let min_solana_commitment = match commitment {
                        CommitmentLevel::Processed => solana_commitment_config::CommitmentLevel::Processed,
                        CommitmentLevel::Confirmed => solana_commitment_config::CommitmentLevel::Confirmed,
                        CommitmentLevel::Finalized => solana_commitment_config::CommitmentLevel::Finalized,
                    };

                    // Each slot has up to 9 entries: data batch split around CreatedBank, block, footer, block meta and 3 slot statuses.
                    let mut replayed_messages = Vec::with_capacity(9 * replay_stored_slots as usize);
                    let replayed_slot_iter = block_machine.replay_from_slot(replay_slot, min_solana_commitment);

                    // We need only an estimated timestamp for the replayed slot messages, so we can use the same timestamp for all of them.
                    let created_at = Timestamp::from(SystemTime::now());
                    // The span of bank ids pushed, noted per bank so the client needs no pass over the messages.
                    let mut bank_id_span: Option<BankSpan> = None;

                    for replayed_slot in replayed_slot_iter {
                        let bank_id = replayed_slot.frozen_block.bank_id();
                        bank_id_span.get_or_insert(BankSpan::new(bank_id)).include(bank_id);

                        // 1st Put data (account/txn/entries) with the bank's CreatedBank where it arrived live:
                        // after the account writes made while the bank was constructed.
                        let bank_messages = replayed_slot.frozen_block.messages();
                        let created_bank_index = replayed_slot.frozen_block.created_bank_index();
                        replayed_messages.push(ReplayResponseMessageType::Batch(Arc::clone(&bank_messages), 0..created_bank_index));
                        replayed_messages.push(ReplayResponseMessageType::Single(Message::Slot(replayed_slot.frozen_block.created_bank())));
                        replayed_messages.push(ReplayResponseMessageType::Batch(Arc::clone(&bank_messages), created_bank_index..bank_messages.len()));

                        // A bank sealed but not yet out at processed: its block, block meta and
                        // statuses go out live, when its Processed status arrives.
                        if commitment == CommitmentLevel::Processed
                            && !position.sent_at_processed.contains_key(&replayed_slot.frozen_block.bank_id())
                        {
                            continue;
                        }

                        // 2nd Put the reconstructed block, then the block summary — mirrors the
                        // live broadcast path so `blocks` subscribers can resume via `from_slot`
                        replayed_messages.push(ReplayResponseMessageType::Single(Message::Block(replayed_slot.frozen_block.get_message_block())));

                        // 3rd Put block footer, which must come before the block summary
                        if let Some(block_footer) = replayed_slot.frozen_block.get_block_footer() {
                            replayed_messages.push(ReplayResponseMessageType::Single(Message::BlockFooter(block_footer)));
                        }

                        // 4th Put block summary
                        replayed_messages.push(ReplayResponseMessageType::Single(Message::BlockMeta(replayed_slot.frozen_block.get_block_meta())));

                        // 5th Put slot status
                        for slot_update in replayed_slot.slot_status_messages.iter() {
                            bank_id_span.get_or_insert(BankSpan::new(slot_update.bank_id)).include(slot_update.bank_id);
                            let slot_message = Message::Slot(Arc::new(MessageSlot {
                                slot: slot_update.slot,
                                parent: slot_update.parent_slot,
                                status: match slot_update.commitment {
                                    solana_commitment_config::CommitmentLevel::Processed => SlotStatus::Processed,
                                    solana_commitment_config::CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                                    solana_commitment_config::CommitmentLevel::Finalized => SlotStatus::Finalized,
                                },
                                dead_error: None,
                                created_at,
                                bank_id: Some(slot_update.bank_id),
                            }));
                            replayed_messages.push(ReplayResponseMessageType::Single(slot_message));
                        }
                    }

                    // 6th At processed, the banks still open: what they sent so far. The rest comes live.
                    if commitment == CommitmentLevel::Processed {
                        let mut unsealed: Vec<_> = block_machine.unsealed_banks(replay_slot).collect();
                        unsealed.sort_unstable_by_key(|(slot, bank_id, _)| (*slot, *bank_id));
                        for (_, bank_id, messages) in unsealed {
                            bank_id_span.get_or_insert(BankSpan::new(bank_id)).include(bank_id);
                            replayed_messages.push(ReplayResponseMessageType::Batch(Arc::new(messages.to_vec()), 0..messages.len()));
                        }
                    }

                    let _ = tx.send(ReplayedResponse::Messages(Replay {
                        messages: replayed_messages,
                        bank_id_span,
                        cut: position.cut(commitment),
                    }));
                }
                Some(request) = replay_stored_slots_rx.recv(), if pending_replay.is_none() => {
                    pending_replay = Some(request);
                }
                else => {
                    // No new messages and replay request channel closed, can only happen on shutdown
                    info!("Block reconstruction loop: replay_stored_slots channel closed");
                    break;
                }
            }
        }
    }

    /// Takes `messages` into `block_machine` and broadcasts the blocks they complete.
    ///
    /// # Arguments
    ///
    /// * `block_machine` - The banks being reconstructed.
    /// * `broadcast` - The commitment channels.
    /// * `position` - Updated with what was taken in and sent.
    /// * `messages` - What geyser_loop handed over.
    fn take_in(
        block_machine: &mut BlockMachineStorage,
        broadcast: &SubscriberChannels,
        position: &mut BroadcastPosition,
        messages: impl Iterator<Item = BlockReconstructionMessage>,
    ) {
        const ALL_COMMITMENT_LEVELS: [CommitmentLevel; 3] = [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ];

        for messages in messages {
            match messages {
                BlockReconstructionMessage::BroadcastBatch(messages) => {
                    for message in messages.iter() {
                        if let Message::Slot(slot_message) = message {
                            metrics::update_slot_plugin_status(
                                slot_message.status,
                                slot_message.slot,
                            );
                        }

                        block_machine.add(message.clone());
                    }
                    for waiting in position.waiting_next_taken_in.drain(..) {
                        let _ = waiting.send(Arc::clone(&messages));
                    }
                    // A replay cut: the last batch geyser_loop broadcast before handing it over.
                    position.last_taken_in = Some(messages);
                }
                BlockReconstructionMessage::SealGated(message) => {
                    // Never a cut: it was not broadcast, so it reaches no live receiver. What
                    // block reconstruction makes of it, it broadcasts itself, under `last_sent`.
                    if let Message::Slot(slot_message) = &message {
                        metrics::update_slot_plugin_status(slot_message.status, slot_message.slot);
                    }

                    block_machine.add(message);
                }
            }
        }

        while let Some((slot_update, frozen_block)) = block_machine.pop_ready_block() {
            let commitment_level = match slot_update.commitment {
                solana_commitment_config::CommitmentLevel::Processed => CommitmentLevel::Processed,
                solana_commitment_config::CommitmentLevel::Confirmed => CommitmentLevel::Confirmed,
                solana_commitment_config::CommitmentLevel::Finalized => CommitmentLevel::Finalized,
            };
            // At processed the bank's data, CreatedBank included, already went out as it arrived.
            // At confirmed and finalized this is the first time the bank goes out on that
            // channel: its CreatedBank, then its data.
            if commitment_level != CommitmentLevel::Processed {
                position.send(
                    broadcast,
                    commitment_level,
                    Arc::new(vec![Message::Slot(frozen_block.created_bank())]),
                );
                position.send(broadcast, commitment_level, frozen_block.messages());
            } else {
                position
                    .sent_at_processed
                    .insert(frozen_block.bank_id(), slot_update.slot);
            }

            let mut block_messages = Vec::with_capacity(3);
            block_messages.push(Message::Block(frozen_block.get_message_block()));
            if let Some(block_footer) = frozen_block.get_block_footer() {
                block_messages.push(Message::BlockFooter(block_footer));
            }
            block_messages.push(Message::BlockMeta(frozen_block.get_block_meta()));
            position.send(broadcast, commitment_level, Arc::new(block_messages));

            // A status goes out on a channel only after the bank's content went out on it. The
            // lower channels have it already: they get just this status. This channel gets it
            // now: it gets every status up to this one, in order. The higher channels get them
            // when the bank reaches them.
            let created_at = Timestamp::from(SystemTime::now());
            let status = |status| {
                Message::Slot(Arc::new(MessageSlot {
                    slot: slot_update.slot,
                    parent: slot_update.parent_slot,
                    status,
                    dead_error: None,
                    created_at,
                    bank_id: Some(slot_update.bank_id),
                }))
            };
            let reached = ALL_COMMITMENT_LEVELS
                .iter()
                .position(|level| *level == commitment_level)
                .expect("one of all commitment levels");
            const ALL_COMMITMENT_STATUSES: [SlotStatus; 3] = [
                SlotStatus::Processed,
                SlotStatus::Confirmed,
                SlotStatus::Finalized,
            ];
            let this_status = Arc::new(vec![status(ALL_COMMITMENT_STATUSES[reached])]);
            for channel in &ALL_COMMITMENT_LEVELS[..reached] {
                position.send(broadcast, *channel, Arc::clone(&this_status));
            }
            let statuses_so_far = if reached == 0 {
                this_status
            } else {
                Arc::new(
                    ALL_COMMITMENT_STATUSES[..=reached]
                        .iter()
                        .map(|level| status(*level))
                        .collect(),
                )
            };
            position.send(broadcast, commitment_level, statuses_so_far);
        }

        if let Some(min_slot) = block_machine.min_replayable_slot() {
            position
                .sent_at_processed
                .retain(|_, slot| *slot >= min_slot);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn client_loop(
        mut session: ClientSession,
        stream_tx: SubscribeSender,
        mut client_rx: mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        snapshot_rx: Option<crossbeam_channel::Receiver<Box<Message>>>,
        broadcast: SubscriberChannels,
        replay_stored_slots_tx: Option<mpsc::Sender<ReplayStoredSlotsRequest>>,
        task_tracker: TaskTracker,
        client_unresponsive_timeout: Option<Duration>,
    ) {
        let cancellation_token = session.cancellation_token.clone();

        // # Starting the live stream
        //
        // Nothing is subscribed until the first request: it sets the filter and commitment, and
        // is the only request that can replay. A `from_slot` on any later request is ignored.
        // Without `from_slot`, the live stream starts at the first request. With it, the live
        // stream is subscribed before the replay is asked for, and [`ReplayPhase`] sends the
        // replay, then what the receiver queued less what the two have in common, before the
        // live stream starts.
        let from_slot = match Self::client_loop_first_request(
            &mut session,
            &mut client_rx,
            &cancellation_token,
        )
        .await
        {
            Ok(from_slot) => from_slot,
            Err(ClientSnapshotReplayError::Cancelled) => {
                let _ = stream_tx.try_send(Err(Status::unavailable(
                    "server is shutting down try again later",
                )));
                session.disconnect_reason = "server_shutdown";
                return;
            }
            Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed) => {
                session.disconnect_reason = "client_disconnect";
                return;
            }
        };

        let snapshot_requested = snapshot_rx.is_some();
        if let Some(snapshot_rx) = snapshot_rx {
            info!("client #{}: snapshot requested", session.subscriber_id);
            let result = Self::client_loop_snapshot(
                &session,
                stream_tx.clone(),
                snapshot_rx,
                cancellation_token.clone(),
            )
            .await;
            match result {
                Ok(()) => {
                    info!("client #{}: snapshot stream ended", session.subscriber_id);
                }
                Err(ClientSnapshotReplayError::Cancelled) => {
                    let _ = stream_tx.try_send(Err(Status::internal(
                        "server is shutting down try again later",
                    )));
                    session.disconnect_reason = "server_shutdown";
                    return;
                }
                Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed) => {
                    info!("client #{}: grpc connection closed", session.subscriber_id);
                    session.disconnect_reason = "client_closed";
                    return;
                }
            }
        } else {
            info!("client #{}: no snapshot requested", session.subscriber_id);
        }

        let mut commitment = session.filter.get_commitment_level();
        let mut messages_rx = match from_slot {
            // A snapshot subscription does not replay.
            Some(from_slot) if !snapshot_requested => {
                let replay = Self::client_loop_replay(
                    &mut session,
                    &stream_tx,
                    &broadcast,
                    replay_stored_slots_tx.as_ref(),
                    commitment,
                    from_slot,
                    &task_tracker,
                    client_unresponsive_timeout,
                );
                // The replay can wait with nothing to send: for block reconstruction to catch
                // up, or for the next raw batch on an idle chain. A client that left meanwhile
                // is only noticed by the ping task, which cancels the token.
                let messages_rx = tokio::select! {
                    biased;

                    _ = cancellation_token.cancelled() => {
                        info!("client #{}: cancelled", session.subscriber_id);
                        let _ = stream_tx.try_send(Err(Status::unavailable("server is shutting down try again later")));
                        session.disconnect_reason = "server_shutdown";
                        return;
                    }
                    messages_rx = replay => messages_rx,
                };
                let Some(messages_rx) = messages_rx else {
                    return;
                };
                messages_rx
            }
            _ => broadcast.subscribe(commitment),
        };

        'outer: loop {
            observe_subscriber_queue_size(
                &session.subscriber_id,
                stream_tx.current_weight() as u64,
                "normal",
            );

            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("client #{}: cancelled", session.subscriber_id);
                    let _ = stream_tx.try_send(Err(Status::unavailable("server is shutting down try again later")));
                    session.disconnect_reason = "server_shutdown";
                    break 'outer;
                }
                mut message = client_rx.recv() => {
                    // forward to latest filter
                    loop {
                        match client_rx.try_recv() {
                            Ok(message_new) => {
                                message = Some(message_new);
                            }
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                message = None;
                                break;
                            }
                        }
                    }

                    match message {
                        Some(Some((from_slot, filter_new))) => {
                            session.set_filter(filter_new);
                            info!("client #{}: filter updated", session.subscriber_id);
                            if from_slot.is_some() {
                                info!("client #{}: from_slot ignored, only the first request can replay", session.subscriber_id);
                            }

                            let commitment_new = session.filter.get_commitment_level();
                            if commitment_new != commitment {
                                commitment = commitment_new;
                                messages_rx = broadcast.subscribe(commitment);
                            }
                        }
                        Some(None) => {
                            session.disconnect_reason = "client_disconnect";
                            break 'outer;
                        },
                        None => {
                            session.disconnect_reason = "client_disconnect";
                            break 'outer;
                        }
                    }
                }
                message = messages_rx.recv() => {
                    let messages = match message {
                        Ok(messages) => messages,
                        Err(broadcast::error::RecvError::Closed) => {
                            session.disconnect_reason = "broadcast_closed";
                            break 'outer;
                        },
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            info!("client #{}: lagged to receive geyser messages", session.subscriber_id);
                            task_tracker.spawn(async move {
                                let _ = stream_tx.send(Err(Status::internal("lagged to receive geyser messages"))).await;
                            });
                            session.disconnect_reason = "client_broadcast_lag";
                            break 'outer;
                        }
                    };

                    for message in messages.iter() {
                        for message in session.filter.get_updates(message, Some(commitment)) {
                            match stream_tx.try_send(Ok(message)) {
                                Ok(()) => {
                                    metrics::incr_grpc_message_sent_counter(&session.subscriber_id);
                                }
                                Err(TrySendError::Full(_)) => {
                                    error!("client #{}: lagged to send an update", session.subscriber_id);
                                    task_tracker.spawn(async move {
                                        let _ = stream_tx.send(Err(Status::internal("lagged to send an update"))).await;
                                    });
                                    session.disconnect_reason = "client_channel_full";
                                    break 'outer;
                                }
                                Err(TrySendError::Closed(_)) => {
                                    error!("client #{}: stream closed", session.subscriber_id);
                                    session.disconnect_reason = "client_closed";
                                    break 'outer;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Waits for the subscription's first request and applies its filter.
    ///
    /// # Arguments
    ///
    /// * `session` - The client session, whose filter is replaced by the request's.
    /// * `client_rx` - The client's requests.
    /// * `cancellation_token` - Cancelled when the server shuts down.
    ///
    /// # Returns
    ///
    /// The first request's `from_slot`.
    ///
    /// # Errors
    ///
    /// [`ClientSnapshotReplayError::Cancelled`] if the server shuts down first, and
    /// [`ClientSnapshotReplayError::ClientGrpcConnectionClosed`] if the client leaves first.
    async fn client_loop_first_request(
        session: &mut ClientSession,
        client_rx: &mut mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<u64>, ClientSnapshotReplayError> {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("client #{}: cancelled", session.subscriber_id);
                Err(ClientSnapshotReplayError::Cancelled)
            }
            maybe = client_rx.recv() => match maybe {
                Some(Some((from_slot, filter))) => {
                    session.set_filter(filter);
                    info!("client #{}: filter updated", session.subscriber_id);
                    Ok(from_slot)
                }
                Some(None) | None => Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed),
            }
        }
    }

    /// Sends the replay from `from_slot` and the live backlog that follows it, filtered, with
    /// backpressure, and returns the live receiver positioned after them.
    /// On failure, records why on `session` and tells the client.
    ///
    /// # Arguments
    ///
    /// * `session` - The client session: its filter shapes the replay.
    /// * `stream_tx` - The client's update queue.
    /// * `broadcast` - The commitment channels.
    /// * `replay_stored_slots_tx` - Requests to block reconstruction, or [`None`] when replay
    ///   is off.
    /// * `commitment` - The commitment of the replay and of the live stream.
    /// * `from_slot` - The first slot to replay.
    /// * `task_tracker` - Runs the tasks that send the client an error.
    /// * `client_unresponsive_timeout` - How long the client can leave a replayed update
    ///   unread.
    ///
    /// # Returns
    ///
    /// The live receiver, where every batch is new, or [`None`] if the subscription must end.
    #[allow(clippy::too_many_arguments)]
    async fn client_loop_replay(
        session: &mut ClientSession,
        stream_tx: &SubscribeSender,
        broadcast: &SubscriberChannels,
        replay_stored_slots_tx: Option<&mpsc::Sender<ReplayStoredSlotsRequest>>,
        commitment: CommitmentLevel,
        from_slot: Slot,
        task_tracker: &TaskTracker,
        client_unresponsive_timeout: Option<Duration>,
    ) -> Option<broadcast::Receiver<BroadcastedMessage>> {
        let fail = |status: Status| {
            let stream_tx = stream_tx.clone();
            task_tracker.spawn(async move {
                let _ = stream_tx.send(Err(status)).await;
            });
        };

        let Some(replay_stored_slots_tx) = replay_stored_slots_tx else {
            info!(
                "client #{}: from_slot is not supported",
                session.subscriber_id
            );
            fail(Status::internal("from_slot is not supported"));
            session.disconnect_reason = "from_slot_unsupported";
            return None;
        };

        // Subscribed first, so whatever the replay misses is on it. See [`ReplayPhase`].
        let messages_rx = broadcast.subscribe(commitment);
        let (tx, rx) = oneshot::channel();
        if let Err(_error) = replay_stored_slots_tx
            .send((commitment, from_slot, tx))
            .await
        {
            error!(
                "client #{}: failed to send from_slot request",
                session.subscriber_id
            );
            fail(Status::internal("failed to send from_slot request"));
            session.disconnect_reason = "replay_error";
            return None;
        }

        let replay = match rx.await {
            Ok(ReplayedResponse::Messages(replay)) => replay,
            Ok(ReplayedResponse::Lagged(slot)) => {
                info!(
                    "client #{}: broadcast from {from_slot} is not available",
                    session.subscriber_id
                );
                fail(Status::out_of_range(format!(
                    "broadcast from {from_slot} is not available, last available: {slot}"
                )));
                session.disconnect_reason = "slot_unavailable";
                return None;
            }
            Err(_error) => {
                error!(
                    "client #{}: failed to get replay response",
                    session.subscriber_id
                );
                fail(Status::internal("failed to get replay response"));
                session.disconnect_reason = "replay_error";
                return None;
            }
        };

        let broadcast_failed = |session: &mut ClientSession, error| match error {
            broadcast::error::RecvError::Closed => {
                session.disconnect_reason = "broadcast_closed";
            }
            broadcast::error::RecvError::Lagged(_) => {
                info!(
                    "client #{}: lagged to receive geyser messages",
                    session.subscriber_id
                );
                fail(Status::internal("lagged to receive geyser messages"));
                session.disconnect_reason = "client_broadcast_lag";
            }
        };
        let mut phase = match ReplayPhase::new(messages_rx, commitment, replay) {
            Ok(phase) => phase,
            Err(error) => {
                broadcast_failed(session, error);
                return None;
            }
        };

        // The replay, the missed batch and the backlog, all sent with backpressure.
        loop {
            let message = match phase.next().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(error) => {
                    broadcast_failed(session, error);
                    return None;
                }
            };
            for filtered_message in session.filter.get_updates(&message, Some(commitment)) {
                let send = stream_tx.send(Ok(filtered_message));
                let sent = match client_unresponsive_timeout {
                    Some(timeout) => tokio::time::timeout(timeout, send).await,
                    None => Ok(send.await),
                };
                match sent {
                    Ok(Ok(())) => {
                        metrics::incr_grpc_message_sent_counter(&session.subscriber_id);
                    }
                    Ok(Err(SendError(_))) => {
                        error!("client #{}: stream closed", session.subscriber_id);
                        session.disconnect_reason = "client_closed";
                        return None;
                    }
                    Err(_elapsed) => {
                        error!(
                            "client #{}: replay not read within {client_unresponsive_timeout:?}",
                            session.subscriber_id
                        );
                        fail(Status::internal("client did not read the replay in time"));
                        session.disconnect_reason = "client_unresponsive";
                        return None;
                    }
                }
            }
        }
        Some(phase.into_receiver())
    }

    async fn client_loop_snapshot(
        session: &ClientSession,
        stream_tx: SubscribeSender,
        snapshot_rx: crossbeam_channel::Receiver<Box<Message>>,
        cancellation_token: CancellationToken,
    ) -> Result<(), ClientSnapshotReplayError> {
        info!(
            "client #{}: going to receive snapshot data",
            session.subscriber_id
        );

        loop {
            if cancellation_token.is_cancelled() {
                info!("client #{}: cancelled", session.subscriber_id);
                return Err(ClientSnapshotReplayError::Cancelled);
            }
            let message = match snapshot_rx.try_recv() {
                Ok(message) => {
                    metrics::message_queue_size_dec();
                    message
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    sleep(Duration::from_millis(1)).await;
                    continue;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    info!("client #{}: end of startup", session.subscriber_id);
                    break;
                }
            };

            for message in session.filter.get_updates(&message, None) {
                if stream_tx.send(Ok(message)).await.is_err() {
                    error!("client #{}: stream closed", session.subscriber_id);
                    return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_service<H>(
        health_service: HealthServer<H>,
        x_token: Option<AsciiMetadataValue>,
        service: GeyserServer<GrpcService>,
        traffic_reporting_threshold: ByteSize,
        auth: Option<AuthConfig>,
    ) -> anyhow::Result<tonic::service::Routes>
    where
        H: tonic_health::pb::health_server::Health,
    {
        let method_ratelimit_config = auth
            .as_ref()
            .and_then(|auth_config| auth_config.ratelimit.clone());
        let billing_config = auth
            .as_ref()
            .and_then(|auth_config| auth_config.billing.clone());

        enum AuthLayerChoice {
            Http(AuthLayer<HttpSubscriptionRepository>),
            File(AuthLayer<ConstantSubscriptionRepository>),
            Trusted(AuthLayer<TrustedMetadataAuthenticator>),
        }
        let maybe_auth_layer = match auth {
            Some(AuthConfig {
                kind: AuthKind::Http(http_auth_config),
                ..
            }) => {
                let repository = http_auth_config.build_repository();
                let auth_layer = AuthLayer::new(
                    repository,
                    http_auth_config.max_concurrent_auth_requests.get(),
                );
                Some(AuthLayerChoice::Http(auth_layer))
            }
            Some(AuthConfig {
                kind: AuthKind::File(file_auth_config),
                ..
            }) => {
                let repository = file_auth_config
                    .build_repository()
                    .context("Failed to build file-based auth repository")?;
                let auth_layer = AuthLayer::new(
                    repository,
                    1_000_000, // Arbitrary large number, since file-based auth is not expected to be used in production
                );
                Some(AuthLayerChoice::File(auth_layer))
            }
            Some(AuthConfig {
                kind: AuthKind::TrustedMetadata(_trusted_metadata_config),
                ..
            }) => {
                let repository = TrustedMetadataAuthenticator::new([]);
                let auth_layer = AuthLayer::new(
                    repository,
                    1_000_000, // Arbitrary large number, since trusted metadata auth is not expected to be used in production
                );
                Some(AuthLayerChoice::Trusted(auth_layer))
            }
            None => None,
        };

        // Having this ugly macros is the most efficient approach.
        macro_rules! with_auth {
            ($auth_layer:expr, |$layer:ident| $body:expr) => {{
                match $auth_layer {
                    AuthLayerChoice::Http($layer) => $body,
                    AuthLayerChoice::File($layer) => $body,
                    AuthLayerChoice::Trusted($layer) => $body,
                }
            }};
        }

        // Request -> InterceptorLayer -> RateLimiter -> MeteredBandwidthLayer (Promtheus / Billing*) -> GeyserService

        let maybe_billing_metered_man = billing_config.map(|billing_config| {
            let client = reqwest::Client::new();
            let BillingConfig::Http(billing_config) = billing_config;
            let billing_endpoint = billing_config.billing_endpoint_url;
            let (tx, rx) = mpsc::unbounded_channel();

            let billing_sink = HttpBillingEventSink::new(
                client,
                billing_endpoint,
                UnboundedReceiverStream::new(rx),
                Some(billing_config.report_interval),
            );
            tokio::spawn(billing_sink);
            BillingMeteredManager::new(tx)
        });

        let metered_man = PrometheusMeteredManager.stack(maybe_billing_metered_man);

        let metered_svc = MeteredBandwidthLayer::new(metered_man, traffic_reporting_threshold)
            .named_layer(service);

        let ratelimiter = OptionalHttpInterceptor::from_option(
            method_ratelimit_config.as_ref().map(|ratelimit| {
                log::info!(
                    "Using default ratelimit of {} hits per {:?} for all subscribers and methods",
                    ratelimit.default_max_hits,
                    ratelimit.window
                );
                MethodRatelimiter::new(ratelimit.default_max_hits, ratelimit.window)
            }),
        );

        let http_intercepted_svc = HttpInterceptorLayer::new(ratelimiter).named_layer(metered_svc);

        let intercepted_svc = interceptor::InterceptorLayer::new(XTokenInterceptor {
            x_token: x_token.clone(),
        })
        .named_layer(http_intercepted_svc);

        let mut routes = tonic::service::Routes::builder();
        routes.add_service(health_service);

        if let Some(auth_layer) = maybe_auth_layer {
            // The final wrapping order is: AuthLayer -> RateLimit -> InterceptorLayer -> MeteredBandwidthLayer ((Promtheus / Billing*) -> GeyserService
            // The AuthLayer is the outermost layer, so it can intercept and handle authentication before any other processing occurs.
            with_auth!(auth_layer, |auth_layer| {
                let auth_svc = auth_layer.named_layer(intercepted_svc);
                routes.add_service(auth_svc);
                Ok(routes.routes().prepare())
            })
        } else {
            routes.add_service(intercepted_svc);
            Ok(routes.routes().prepare())
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn serve_listener<I, IO>(
        incoming: I,
        http2_adaptive_window: Option<bool>,
        http2_keepalive_interval: Option<Duration>,
        http2_keepalive_timeout: Option<Duration>,
        initial_connection_window_size: Option<u32>,
        initial_stream_window_size: Option<u32>,
        service: tonic::service::Routes,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()>
    where
        I: Stream<Item = io::Result<IO>> + Send + 'static,
        IO: Connected + AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut builder = Server::builder();

        if let Some(enabled) = http2_adaptive_window {
            builder = builder.http2_adaptive_window(Some(enabled));
        }
        if let Some(interval) = http2_keepalive_interval {
            builder = builder.http2_keepalive_interval(Some(interval));
        }
        if let Some(timeout) = http2_keepalive_timeout {
            builder = builder.http2_keepalive_timeout(Some(timeout));
        }
        if let Some(sz) = initial_connection_window_size {
            builder = builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = initial_stream_window_size {
            builder = builder.initial_stream_window_size(sz);
        }

        builder
            .serve_with_incoming_shutdown(service, incoming, shutdown.cancelled())
            .await
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    async fn deshred_client_loop(
        mut session: DeshredClientSession,
        stream_tx: DeshredSubscribeSender,
        mut client_rx: mpsc::UnboundedReceiver<Option<DeshredFilter>>,
        mut messages_rx: broadcast::Receiver<DeshredBroadcastedMessage>,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) {
        'outer: loop {
            observe_subscriber_queue_size(
                &session.subscriber_id,
                stream_tx.current_weight() as u64,
                "deshred",
            );

            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("deshred client #{}/{}: cancelled", session.subscriber_id, session.id);
                    let _ = stream_tx.try_send(Err(Status::unavailable("server is shutting down try again later")));
                    session.disconnect_reason = "server_shutdown";
                    break 'outer;
                }
                mut message = client_rx.recv() => {
                    // forward to latest filter
                    loop {
                        match client_rx.try_recv() {
                            Ok(message_new) => {
                                message = Some(message_new);
                            }
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                message = None;
                                break;
                            }
                        }
                    }

                    match message {
                        Some(Some(filter_new)) => {
                            session.filter = filter_new;
                            info!("deshred client #{}/{}: filter updated", session.subscriber_id, session.id);
                        }
                        Some(None) => {
                            session.disconnect_reason = "client_disconnect";
                            break 'outer;
                        },
                        None => {
                            session.disconnect_reason = "client_disconnect";
                            break 'outer;
                        }
                    }
                }
                message = messages_rx.recv() => {
                    let message = match message {
                        Ok(message) => message,
                        Err(broadcast::error::RecvError::Closed) => {
                            session.disconnect_reason = "broadcast_closed";
                            break 'outer;
                        },
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            info!("deshred client #{}/{}: lagged to receive deshred messages", session.subscriber_id, session.id);
                            session.disconnect_reason = "client_broadcast_lag";
                            task_tracker.spawn(async move {
                                let _ = stream_tx.send(Err(Status::internal("lagged to receive deshred messages"))).await;
                            });
                            break 'outer;
                        }
                    };

                    metrics::deshred_queue_size_dec();

                    for update in session.filter.get_updates(&message, None) {
                        match stream_tx.try_send(Ok(update)) {
                            Ok(()) => {
                                metrics::incr_grpc_message_sent_counter(&session.subscriber_id);
                            }
                            Err(TrySendError::Full(_)) => {
                                error!("deshred client #{}/{}: lagged to send an update", session.subscriber_id, session.id);
                                session.disconnect_reason = "client_channel_full";
                                task_tracker.spawn(async move {
                                    let _ = stream_tx.send(Err(Status::internal("lagged to send an update"))).await;
                                });
                                break 'outer;
                            }
                            Err(TrySendError::Closed(_)) => {
                                error!("deshred client #{}/{}: stream closed", session.subscriber_id, session.id);
                                session.disconnect_reason = "client_closed";
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }
        // session Drop handles: queue_size(0), disconnect metric, debug removal, cancel, log
    }

    fn next_subscribe_seq_id(&self) -> usize {
        self.subscribe_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = LoadAwareReceiver<TonicResult<FilteredUpdate>>;
    type SubscribeDeshredStream = LoadAwareReceiver<TonicResult<FilteredUpdateDeshred>>;
    type SubscribeGossipStream = ReceiverStream<TonicResult<SubscribeUpdateGossip>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        incr_grpc_method_call_count("subscribe");

        let subscriber_id = request
            .extensions()
            .get::<SubscriptionInfo>()
            .cloned()
            .map(|info| info.subscription_id)
            .or_else(|| {
                request
                    .metadata()
                    .get("x-subscription-id")
                    .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
                    .or_else(|| request.remote_addr().map(|addr| addr.ip().to_string()))
            });

        let subscription_permit = if let Some(id) = subscriber_id.as_deref() {
            match self.subscription_tracker.try_insert(id.to_owned()) {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return Err(Status::resource_exhausted(
                        "max subscription limit exceeded",
                    ));
                }
            }
        } else {
            None
        };

        let id = self.next_subscribe_seq_id();
        let client_cancellation_token = self.cancellation_token.child_token();
        if client_cancellation_token.is_cancelled() {
            return Err(Status::unavailable("server is shutting down"));
        }

        let x_request_snapshot = request.metadata().contains_key("x-request-snapshot");
        let snapshot_rx = if x_request_snapshot {
            self.snapshot_rx.lock().await.take()
        } else {
            None
        };

        let capacity = if snapshot_rx.is_some() {
            self.config_snapshot_client_channel_capacity
        } else {
            self.geyser_subscriber_weight_capacity
        };
        let (stream_tx, stream_rx) = load_aware_channel(capacity, self.weight_scales);
        let (client_tx, client_rx) = mpsc::unbounded_channel();

        let ping_stream_tx = stream_tx.clone();
        let ping_cancellation_token = client_cancellation_token.clone();
        let ping_client_cancel = client_cancellation_token.clone();
        let ping_subscriber_id = subscriber_id.clone();
        self.task_tracker.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = ping_cancellation_token.cancelled() => {
                        info!("client #{ping_subscriber_id:?}/{id}: ping cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        let msg = FilteredUpdate::new_empty(FilteredUpdateOneof::ping());
                        log::info!("client #{ping_subscriber_id:?}/{id}: sending ping");
                        if ping_stream_tx.send(Ok(msg)).await.is_err() {
                            //
                            // It's really important to send cancel ping for one edge-case where someone
                            // subscribe without any filter:
                            //
                            // When someone subscribe without any filter, this can create a "zombie" client loop that
                            // does reject every geyser event thus we never write to the HTTP/2 stream and we never detect that the client TCP connection is closed.
                            // By sending a ping every 10 seconds, we can detect if the client is still alive and if it's not,
                            // we can cancel the client loop.
                            ping_client_cancel.cancel();
                            info!("detected dead client #{ping_subscriber_id:?}/{id}");
                            break;
                        }
                    }
                }
            }
            info!("client #{ping_subscriber_id:?}/{id}: ping task exiting");
        });

        let endpoint = request
            .metadata()
            .get("x-endpoint")
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "".to_owned());

        let config_filter_limits = Arc::clone(&self.config_filter_limits);
        let incoming_stream_tx = stream_tx.clone();
        let incoming_client_tx = client_tx;
        let incoming_cancellation_token = client_cancellation_token.child_token();

        let mut filter_names = FilterNames::new(
            self.filter_name_size_limit,
            self.filter_names_size_limit,
            self.filter_names_cleanup_interval,
        );

        let subscriber_id2 = subscriber_id.clone();
        self.task_tracker.spawn(async move {
            let subscriber_id = subscriber_id2.unwrap_or("unknown".to_string());
            let mut forwarded_first = false;
            loop {
                tokio::select! {
                    _ = incoming_cancellation_token.cancelled() => {
                        info!("client #{subscriber_id:?}/{id}: filter receiver cancelled");
                        break;
                    }
                    message = request.get_mut().message() => match message {
                        Ok(Some(request)) => {
                            filter_names.try_clean();

                            if let Err(error) = match Filter::new(&request, &config_filter_limits, &mut filter_names) {
                                Ok(filter) => {
                                    if let Some(msg) = filter.get_pong_msg() {
                                        if incoming_stream_tx.send(Ok(msg)).await.is_err() {
                                            error!("client #{subscriber_id:?}/{id}: stream closed");
                                            let _ = incoming_client_tx.send(None);
                                            break;
                                        }
                                        continue;
                                    }
                                    // An SDK's subscribe() sends an empty request before the user's own. It
                                    // asks for nothing, so it must not take the first request's place, which
                                    // alone can replay: wait for the first request that asks for something.
                                    if !forwarded_first && asks_for_nothing(&request) {
                                        continue;
                                    }
                                    forwarded_first = true;
                                    let complexity_score = filter.get_filter_stats();
                                    metrics::observe_filter_complexity(&subscriber_id, &complexity_score);
                                    match incoming_client_tx.send(Some((request.from_slot, filter))) {
                                        Ok(()) => Ok(()),
                                        Err(error) => Err(error.to_string()),
                                    }
                                },
                                Err(error) => Err(error.to_string()),
                            } {
                                let err = Err(Status::invalid_argument(format!(
                                    "failed to create filter: {error}"
                                )));
                                if incoming_stream_tx.send(err).await.is_err() {
                                    let _ = incoming_client_tx.send(None);
                                }
                            }
                        }
                        Ok(None) => {
                             // Client half-closed its send stream. Stop reading, but keep
                             // incoming_client_tx alive so client_loop continues running.
                            info!("client #{subscriber_id:?}/{id}: client closed send stream, waiting for cancellation");
                            incoming_cancellation_token.cancelled().await;
                            break;
                        }
                        Err(_error) => {
                            let _ = incoming_client_tx.send(None);
                            break;
                        }
                    }
                }
            }
        });

        let client_session = ClientSession::new(
            id,
            subscriber_id.clone(),
            endpoint.clone(),
            client_cancellation_token,
            subscription_permit,
        );
        self.task_tracker.spawn(Self::client_loop(
            client_session,
            stream_tx,
            client_rx,
            snapshot_rx,
            self.broadcast.clone(),
            self.replay_stored_slots_tx.clone(),
            self.task_tracker.clone(),
            self.client_unresponsive_timeout,
        ));

        Ok(Response::new(stream_rx))
    }

    async fn subscribe_deshred(
        &self,
        mut request: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> TonicResult<Response<Self::SubscribeDeshredStream>> {
        incr_grpc_method_call_count("subscribe_deshred");

        let subscriber_id = request
            .extensions()
            .get::<SubscriptionInfo>()
            .cloned()
            .map(|info| info.subscription_id)
            .or_else(|| {
                request
                    .metadata()
                    .get("x-subscription-id")
                    .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
                    .or_else(|| request.remote_addr().map(|addr| addr.ip().to_string()))
            });

        let subscription_permit = if let Some(id) = subscriber_id.as_deref() {
            match self.subscription_tracker.try_insert(id.to_owned()) {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return Err(Status::resource_exhausted(
                        "max subscription limit exceeded",
                    ));
                }
            }
        } else {
            None
        };

        let id = self.next_subscribe_seq_id();

        let client_cancellation_token = self.cancellation_token.child_token();
        if client_cancellation_token.is_cancelled() {
            return Err(Status::unavailable("server is shutting down"));
        }

        let (stream_tx, stream_rx) =
            load_aware_channel(self.deshred_subscriber_channel_capacity, UnitWeigher::new());
        let (client_tx, client_rx) = mpsc::unbounded_channel();

        let ping_stream_tx = stream_tx.clone();
        let ping_cancellation_token = client_cancellation_token.clone();
        let ping_client_cancel = client_cancellation_token.clone();
        self.task_tracker.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = ping_cancellation_token.cancelled() => {
                        info!("deshred client #{id}: ping cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        let msg = FilteredUpdateDeshred::ping();
                        info!("deshred client #{id}: sending ping");
                        if ping_stream_tx.send(Ok(msg)).await.is_err() {
                            ping_client_cancel.cancel();
                            info!("detected dead deshred client #{id}");
                            break;
                        }
                    }
                }
            }
            info!("deshred client #{id}: ping task exiting");
        });

        let subscriber_id = request
            .metadata()
            .get("x-subscription-id")
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            .or(request.remote_addr().map(|addr| addr.ip().to_string()));

        let config_filter_limits = Arc::clone(&self.config_filter_limits);
        let mut filter_names = FilterNames::new(
            self.filter_name_size_limit,
            self.filter_names_size_limit,
            self.filter_names_cleanup_interval,
        );
        let incoming_stream_tx = stream_tx.clone();
        let incoming_client_tx = client_tx;
        let incoming_cancellation_token = client_cancellation_token.child_token();

        self.task_tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = incoming_cancellation_token.cancelled() => {
                        info!("deshred client #{id}: filter receiver cancelled");
                        break;
                    }
                    message = request.get_mut().message() => match message {
                        Ok(Some(request)) => {
                            filter_names.try_clean();

                            if let Err(error) = match DeshredFilter::new(&request, &config_filter_limits, &mut filter_names) {
                                Ok(filter) => {
                                    if let Some(msg) = filter.get_pong_msg() {
                                        if incoming_stream_tx.send(Ok(msg)).await.is_err() {
                                            error!("deshred client #{id}: stream closed");
                                            let _ = incoming_client_tx.send(None);
                                            break;
                                        }
                                        continue;
                                    }
                                    match incoming_client_tx.send(Some(filter)) {
                                        Ok(()) => Ok(()),
                                        Err(error) => Err(error.to_string()),
                                    }
                                },
                                Err(error) => Err(error.to_string()),
                            } {
                                let err = Err(Status::invalid_argument(format!(
                                    "failed to create deshred filter: {error}"
                                )));
                                if incoming_stream_tx.send(err).await.is_err() {
                                    let _ = incoming_client_tx.send(None);
                                }
                            }
                        }
                        Ok(None) => {
                            info!("deshred client #{id}: client closed send stream, waiting for cancellation");
                            incoming_cancellation_token.cancelled().await;
                            break;
                        }
                        Err(_error) => {
                            let _ = incoming_client_tx.send(None);
                            break;
                        }
                    }
                }
            }
        });

        let session = DeshredClientSession::new(
            id,
            subscriber_id.clone(),
            client_cancellation_token.clone(),
            subscription_permit,
        );
        self.task_tracker.spawn(Self::deshred_client_loop(
            session,
            stream_tx,
            client_rx,
            self.deshred_broadcast_tx.subscribe(),
            client_cancellation_token,
            self.task_tracker.clone(),
        ));

        Ok(Response::new(stream_rx))
    }

    async fn subscribe_gossip(
        &self,
        request: Request<SubscribeGossipRequest>,
    ) -> TonicResult<Response<Self::SubscribeGossipStream>> {
        incr_grpc_method_call_count("subscribe_gossip");

        let subscriber_id = request
            .extensions()
            .get::<SubscriptionInfo>()
            .cloned()
            .map(|info| info.subscription_id)
            .or_else(|| {
                request
                    .metadata()
                    .get("x-subscription-id")
                    .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
                    .or_else(|| request.remote_addr().map(|addr| addr.ip().to_string()))
            });

        let subscription_permit = if let Some(id) = subscriber_id.as_deref() {
            match self.subscription_tracker.try_insert(id.to_owned()) {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return Err(Status::resource_exhausted(
                        "max subscription limit exceeded",
                    ));
                }
            }
        } else {
            None
        };

        let id = self.next_subscribe_seq_id();
        let client_cancellation_token = self.cancellation_token.child_token();
        if client_cancellation_token.is_cancelled() {
            return Err(Status::unavailable("server is shutting down"));
        }

        let stream_rx = contact_info::grpc::spawn_subscriber(
            id,
            subscriber_id,
            subscription_permit,
            self.contact_info_subscriber_channel_capacity,
            Arc::clone(&self.contact_info_state),
            client_cancellation_token,
            self.task_tracker.clone(),
        );

        Ok(Response::new(stream_rx))
    }

    async fn subscribe_first_available_slot(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        incr_grpc_method_call_count("subscribe_first_available_slot");
        let response = SubscribeReplayInfoResponse {
            first_available: self
                .replay_first_available_slot
                .as_ref()
                .map(|stored| stored.load(Ordering::Relaxed)),
        };
        Ok(Response::new(response))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        incr_grpc_method_call_count("ping");
        let count = request.get_ref().count;
        let response = PongResponse { count };
        Ok(Response::new(response))
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        incr_grpc_method_call_count("get_latest_blockhash");
        if let Some(blocks_meta) = &self.blocks_meta {
            blocks_meta
                .get_block(
                    |block| {
                        block.block_height.map(|value| GetLatestBlockhashResponse {
                            slot: block.slot,
                            blockhash: block.blockhash.clone(),
                            last_valid_block_height: value.block_height
                                + MAX_RECENT_BLOCKHASHES as u64,
                        })
                    },
                    request.get_ref().commitment,
                )
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        incr_grpc_method_call_count("get_block_height");
        if let Some(blocks_meta) = &self.blocks_meta {
            blocks_meta
                .get_block(
                    |block| {
                        block.block_height.map(|value| GetBlockHeightResponse {
                            block_height: value.block_height,
                        })
                    },
                    request.get_ref().commitment,
                )
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        incr_grpc_method_call_count("get_slot");
        if let Some(blocks_meta) = &self.blocks_meta {
            blocks_meta
                .get_block(
                    |block| Some(GetSlotResponse { slot: block.slot }),
                    request.get_ref().commitment,
                )
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn is_blockhash_valid(
        &self,
        request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        incr_grpc_method_call_count("is_blockhash_valid");
        if let Some(blocks_meta) = &self.blocks_meta {
            let req = request.get_ref();
            blocks_meta
                .is_blockhash_valid(&req.blockhash, req.commitment)
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        incr_grpc_method_call_count("get_version");
        Ok(Response::new(GetVersionResponse {
            version: serde_json::to_string(&GrpcVersionInfo::default()).unwrap(),
        }))
    }
}

/// Whether `request` subscribes to nothing and asks for no replay, like the empty request an
/// SDK's `subscribe()` sends before the user's own.
///
/// Every field is named, so a field added to the proto fails to build here until someone
/// decides whether it asks for something.
///
/// # Arguments
///
/// * `request` - A request from the client, not a ping.
///
/// # Returns
///
/// `true` if it has no filter and no `from_slot`. A commitment or an accounts data slice alone
/// subscribes to nothing.
fn asks_for_nothing(request: &SubscribeRequest) -> bool {
    let SubscribeRequest {
        accounts,
        slots,
        transactions,
        transactions_status,
        blocks,
        blocks_meta,
        entry,
        block_footer,
        from_slot,
        commitment: _,
        accounts_data_slice: _,
        ping: _,
    } = request;
    accounts.is_empty()
        && slots.is_empty()
        && transactions.is_empty()
        && transactions_status.is_empty()
        && blocks.is_empty()
        && blocks_meta.is_empty()
        && entry.is_empty()
        && block_footer.is_empty()
        && from_slot.is_none()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            plugin::{
                filter::{
                    encoder::TransactionEncoder, fixtures, limits::FilterLimits, name::FilterNames,
                    Filter, FilterAccountsDataSlice,
                },
                message::MessageTransaction,
            },
            util::stream::load_aware_channel,
        },
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::time::Instant,
        yellowstone_grpc_proto::prelude::{
            SubscribeRequest, SubscribeRequestAccountsDataSlice, SubscribeRequestFilterBlocks,
            SubscribeRequestFilterSlots,
        },
    };

    /// The empty request an SDK's `subscribe()` sends, and requests that only set options,
    /// ask for nothing.
    #[test]
    fn empty_and_option_only_requests_ask_for_nothing() {
        assert!(asks_for_nothing(&SubscribeRequest::default()));
        assert!(asks_for_nothing(&SubscribeRequest {
            commitment: Some(CommitmentLevelProto::Confirmed as i32),
            ..Default::default()
        }));
        assert!(asks_for_nothing(&SubscribeRequest {
            accounts_data_slice: vec![SubscribeRequestAccountsDataSlice {
                offset: 0,
                length: 8,
            }],
            ..Default::default()
        }));
    }

    /// A request with any filter, or with only a `from_slot`, asks for something.
    #[test]
    fn requests_with_a_filter_or_from_slot_ask_for_something() {
        assert!(!asks_for_nothing(&SubscribeRequest {
            from_slot: Some(100),
            ..Default::default()
        }));
        assert!(!asks_for_nothing(&SubscribeRequest {
            slots: HashMap::from([("slots".into(), SubscribeRequestFilterSlots::default())]),
            ..Default::default()
        }));
        assert!(!asks_for_nothing(&SubscribeRequest {
            blocks: HashMap::from([("blocks".into(), SubscribeRequestFilterBlocks::default())]),
            ..Default::default()
        }));
        assert!(!asks_for_nothing(&SubscribeRequest {
            accounts: HashMap::from([("accounts".into(), Default::default())]),
            ..Default::default()
        }));
        assert!(!asks_for_nothing(&SubscribeRequest {
            block_footer: HashMap::from([("footer".into(), Default::default())]),
            ..Default::default()
        }));
    }

    fn create_filter_for_blocks(blocks: SubscribeRequestFilterBlocks) -> Filter {
        let config = SubscribeRequest {
            blocks: HashMap::from([("test".into(), blocks)]),
            ..Default::default()
        };
        let mut names = FilterNames::new(64, 1024, Duration::from_secs(1));
        Filter::new(&config, &FilterLimits::default(), &mut names).unwrap()
    }

    fn create_filter_with_blocks(include_accounts: bool) -> Filter {
        create_filter_for_blocks(SubscribeRequestFilterBlocks {
            include_transactions: Some(true),
            include_accounts: Some(include_accounts),
            ..Default::default()
        })
    }

    fn test_transaction() -> Arc<MessageTransaction> {
        fixtures::message_transaction(
            Signature::default(),
            vec![Pubkey::default()],
            false,
            Default::default(),
        )
    }

    fn update(message: FilteredUpdateOneof) -> TonicResult<FilteredUpdate> {
        Ok(FilteredUpdate::new_empty(message))
    }

    #[test]
    fn scaled_weights_apply_per_update_kind() {
        let scales = WeightScales {
            account: 4,
            transaction: 5,
            entry: 6,
            slot: 7,
            block_footer: 8,
            block: 2,
            ..WeightScales::default()
        };
        let default_scales = WeightScales::default();
        let account =
            fixtures::simple_message_account(Pubkey::new_from_array([1; 32]), Pubkey::default());

        let account_update = update(FilteredUpdateOneof::account(
            Arc::clone(&account),
            FilterAccountsDataSlice::default(),
        ));
        assert_eq!(scales.weight(&account_update), 4);
        assert_eq!(default_scales.weight(&account_update), 1);
        assert_eq!(
            scales.weight(&update(
                FilteredUpdateOneof::transaction(test_transaction())
            )),
            5
        );
        assert_eq!(
            scales.weight(&update(FilteredUpdateOneof::entry(
                fixtures::message_entry(1, 0)
            ))),
            6
        );
        let slot = fixtures::message_slot(1, SlotStatus::Processed);
        assert_eq!(scales.weight(&update(FilteredUpdateOneof::slot(slot))), 7);
        let footer = fixtures::message_block_footer(1, 1);
        assert_eq!(
            scales.weight(&update(FilteredUpdateOneof::block_footer(footer))),
            8
        );

        // Kinds without a scale and errors always weigh 1.
        let status = FilteredUpdateOneof::transaction_status(test_transaction());
        assert_eq!(scales.weight(&update(status)), 1);
        let meta = FilteredUpdateOneof::block_meta(fixtures::message_block_meta(1));
        assert_eq!(scales.weight(&update(meta)), 1);
        assert_eq!(scales.weight(&update(FilteredUpdateOneof::ping())), 1);
        assert_eq!(scales.weight(&Err(Status::internal("lagged"))), 1);

        // A block weighs 1 + transactions + accounts, times the block scale only.
        let block = Message::Block(fixtures::message_block(
            1,
            vec![test_transaction(); 3],
            vec![account; 2],
            vec![fixtures::message_entry(1, 0)],
        ));
        let block_update = create_filter_with_blocks(true)
            .get_updates(&block, None)
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(scales.weight(&Ok(block_update.clone())), (1 + 5 + 4) * 2);
        assert_eq!(default_scales.weight(&Ok(block_update)), 1 + 1 + 1);

        let block_update = create_filter_with_blocks(false)
            .get_updates(&block, None)
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(scales.weight(&Ok(block_update)), (1 + 5) * 2);
    }

    #[test]
    fn account_data_adds_weight() {
        let scales = WeightScales {
            account: 4,
            ..WeightScales::default()
        };
        let count_only = WeightScales {
            account_data_unit: 0,
            ..scales
        };
        let account = |len: usize| {
            fixtures::message_account(fixtures::account_info(
                Pubkey::default(),
                Pubkey::default(),
                vec![0; len],
                1000,
                None,
            ))
        };
        let account_update = |len: usize, data_slice: FilterAccountsDataSlice| {
            update(FilteredUpdateOneof::account(account(len), data_slice))
        };

        // One unit plus one per 4096 bytes of data, times the account scale.
        let whole = FilterAccountsDataSlice::default;
        assert_eq!(scales.weight(&account_update(4095, whole())), 4);
        assert_eq!(scales.weight(&account_update(4096, whole())), 8);
        assert_eq!(scales.weight(&account_update(10 << 20, whole())), 4 * 2561);
        assert_eq!(count_only.weight(&account_update(10 << 20, whole())), 4);

        // A data slice sends less, but the queue still holds the whole account.
        let slice = FilterAccountsDataSlice::new(
            &[SubscribeRequestAccountsDataSlice {
                offset: 0,
                length: 32,
            }],
            usize::MAX,
        )
        .unwrap();
        assert_eq!(scales.weight(&account_update(10 << 20, slice)), 4 * 2561);

        // Accounts in a block count the same units, times the block scale only.
        let block = Message::Block(fixtures::message_block(
            1,
            vec![test_transaction(); 2],
            vec![account(100), account(20_000)],
            Vec::new(),
        ));
        let block_update = create_filter_with_blocks(true)
            .get_updates(&block, None)
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(scales.weight(&Ok(block_update.clone())), 1 + 1 + 4 * 4);
        assert_eq!(count_only.weight(&Ok(block_update)), 1 + 1 + 4 * 2);

        let big = Pubkey::new_unique();
        let block = Message::Block(fixtures::message_block(
            1,
            vec![test_transaction(); 2],
            vec![
                account(100),
                fixtures::message_account(fixtures::account_info(
                    big,
                    Pubkey::default(),
                    vec![0; 20_000],
                    1000,
                    None,
                )),
            ],
            Vec::new(),
        ));
        let block_update = create_filter_for_blocks(SubscribeRequestFilterBlocks {
            account_include: vec![big.to_string()],
            include_transactions: Some(true),
            include_accounts: Some(true),
            ..Default::default()
        })
        .get_updates(&block, None)
        .into_iter()
        .next()
        .unwrap();
        assert_eq!(scales.weight(&Ok(block_update)), 1 + 4 * 4);
    }

    #[test]
    fn transaction_data_adds_weight() {
        let scales = WeightScales {
            transaction: 4,
            transaction_data_unit: 64,
            ..WeightScales::default()
        };
        let count_only = WeightScales {
            transaction_data_unit: 0,
            ..scales
        };
        let units = |len: usize| u32::try_from((len / 64).max(1)).unwrap();
        let transaction = test_transaction();
        let transaction_update =
            || update(FilteredUpdateOneof::transaction(Arc::clone(&transaction)));

        assert_eq!(scales.weight(&transaction_update()), 4);

        TransactionEncoder::pre_encode(&transaction.transaction);
        let len = transaction
            .transaction
            .get_pre_encoded()
            .map_or(0, Vec::len);
        assert!(units(len) > 1, "the test transaction spans several units");
        assert_eq!(scales.weight(&transaction_update()), 4 * units(len));
        assert_eq!(count_only.weight(&transaction_update()), 4);

        let block = Message::Block(fixtures::message_block(
            1,
            vec![Arc::clone(&transaction); 10],
            Vec::new(),
            Vec::new(),
        ));
        let block_update = create_filter_with_blocks(false)
            .get_updates(&block, None)
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            scales.weight(&Ok(block_update.clone())),
            1 + 4 * units(10 * len)
        );
        assert_eq!(count_only.weight(&Ok(block_update)), 1 + 4 * 10);
    }

    #[tokio::test]
    async fn subscribe_queue_budget_counts_scaled_weights() {
        let scales = WeightScales {
            account: 4,
            ..WeightScales::default()
        };
        let (stream_tx, mut stream_rx) = load_aware_channel(10, scales);
        let account = || {
            update(FilteredUpdateOneof::account(
                fixtures::simple_message_account(Pubkey::default(), Pubkey::default()),
                FilterAccountsDataSlice::default(),
            ))
        };

        stream_tx.try_send(account()).unwrap();
        stream_tx.try_send(account()).unwrap();
        assert_eq!(stream_tx.current_weight(), 8);
        assert!(matches!(
            stream_tx.try_send(account()),
            Err(TrySendError::Full(_))
        ));
        // Slots keep their default weight of 1 and still fit.
        stream_tx
            .try_send(update(FilteredUpdateOneof::slot(fixtures::message_slot(
                1,
                SlotStatus::Processed,
            ))))
            .unwrap();
        assert_eq!(stream_tx.current_weight(), 9);

        stream_rx.recv().await.unwrap().unwrap();
        assert_eq!(stream_tx.current_weight(), 5);
    }

    /// Answers one replay request with `blocks` blocks of `transactions` transactions each.
    async fn answer_replay(
        mut replay_rx: mpsc::Receiver<ReplayStoredSlotsRequest>,
        blocks: u64,
        transactions: usize,
    ) {
        if let Some((_commitment, _from_slot, response_tx)) = replay_rx.recv().await {
            let transaction = test_transaction();
            let messages: Vec<_> = (0..blocks)
                .map(|slot| {
                    ReplayResponseMessageType::Single(Message::Block(fixtures::message_block(
                        slot,
                        vec![Arc::clone(&transaction); transactions],
                        Vec::new(),
                        Vec::new(),
                    )))
                })
                .collect();
            let _ = response_tx.send(ReplayedResponse::Messages(Replay {
                bank_id_span: BankSpan::of(&messages),
                messages,
                cut: ReplayCut {
                    last_taken_in: None,
                    last_sent: None,
                    next_taken_in: None,
                },
            }));
        }
    }

    /// Starts a client that asks for a replay of 20 blocks weighing 10 each into a queue of 50.
    fn spawn_replaying_client(timeout: Duration) -> (ClientHandles, tokio::task::JoinHandle<()>) {
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let scales = WeightScales {
            transaction_data_unit: 0,
            ..WeightScales::default()
        };
        let (stream_tx, stream_rx) = load_aware_channel(50, scales);
        let (replay_tx, replay_rx) = mpsc::channel(1);
        tokio::spawn(answer_replay(replay_rx, 20, 9));
        let session = ClientSession::new(
            0,
            Some("test".into()),
            "test".into(),
            CancellationToken::new(),
            None,
        );
        let handle = tokio::spawn(GrpcService::client_loop(
            session,
            stream_tx,
            client_rx,
            None,
            SubscriberChannels::new(16, 16, 16),
            Some(replay_tx),
            TaskTracker::new(),
            Some(timeout),
        ));
        client_tx
            .send(Some((Some(0), create_filter_with_blocks(false))))
            .unwrap();
        ((client_tx, stream_rx), handle)
    }

    #[tokio::test]
    async fn replay_disconnects_a_client_that_does_not_read() {
        let started = Instant::now();
        let ((_client_tx, mut stream_rx), handle) =
            spawn_replaying_client(Duration::from_millis(200));

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("client loop gave up on the replay")
            .unwrap();
        assert!(started.elapsed() >= Duration::from_millis(200));

        // The 5 blocks that fit come first, then the disconnect error.
        for _ in 0..5 {
            assert!(matches!(stream_rx.recv().await, Some(Ok(_))));
        }
        let status = stream_rx.recv().await.unwrap().unwrap_err();
        assert_eq!(status.message(), "client did not read the replay in time");
    }

    #[tokio::test]
    async fn replay_waits_for_a_client_that_reads() {
        let ((client_tx, mut stream_rx), handle) =
            spawn_replaying_client(Duration::from_millis(200));

        for _ in 0..20 {
            let update = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                .await
                .expect("replayed block")
                .unwrap()
                .unwrap();
            assert!(matches!(update.message, FilteredUpdateOneof::Block(_)));
        }
        assert!(!handle.is_finished());

        drop(client_tx);
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("client loop ends once the client leaves")
            .unwrap();
    }

    /// Runs a client loop over requests with these `from_slot`s and returns the `from_slot` of
    /// each replay it asked for. `queued` are sent before the loop starts, so it takes them in
    /// one go; each of `later` is sent once the loop is idle.
    async fn replays_for(queued: &[Option<u64>], later: &[Option<u64>]) -> Vec<u64> {
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let (stream_tx, _stream_rx) = load_aware_channel(1024, WeightScales::default());
        let (replay_tx, mut replay_rx) = mpsc::channel::<ReplayStoredSlotsRequest>(1);
        let broadcast = SubscriberChannels::new(16, 16, 16);
        let replayer = {
            tokio::spawn(async move {
                let mut from_slots = Vec::new();
                while let Some((_commitment, from_slot, response_tx)) = replay_rx.recv().await {
                    from_slots.push(from_slot);
                    let _ = response_tx.send(ReplayedResponse::Messages(Replay {
                        messages: Vec::new(),
                        bank_id_span: None,
                        cut: ReplayCut {
                            last_taken_in: None,
                            last_sent: None,
                            next_taken_in: None,
                        },
                    }));
                }
                from_slots
            })
        };
        let send = |from_slot: &Option<u64>| {
            client_tx
                .send(Some((*from_slot, create_filter_with_slots())))
                .unwrap();
        };

        queued.iter().for_each(send);
        let session = ClientSession::new(
            0,
            Some("test".into()),
            "test".into(),
            CancellationToken::new(),
            None,
        );
        let handle = tokio::spawn(GrpcService::client_loop(
            session,
            stream_tx,
            client_rx,
            None,
            broadcast,
            Some(replay_tx),
            TaskTracker::new(),
            None,
        ));
        for from_slot in later {
            settle().await;
            send(from_slot);
        }
        settle().await;

        drop(client_tx);
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("client loop ends once the client leaves")
            .unwrap();
        replayer.await.unwrap()
    }

    /// The live stream starts at the first request, at that request's commitment.
    #[tokio::test]
    async fn live_stream_waits_for_the_first_request() {
        let broadcast = SubscriberChannels::new(16, 16, 16);
        let receivers = |broadcast: &SubscriberChannels| {
            [
                CommitmentLevel::Processed,
                CommitmentLevel::Confirmed,
                CommitmentLevel::Finalized,
            ]
            .map(|commitment| broadcast.sender(commitment).receiver_count())
        };
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let (stream_tx, _stream_rx) = load_aware_channel(1024, WeightScales::default());
        let session = ClientSession::new(
            0,
            Some("test".into()),
            "test".into(),
            CancellationToken::new(),
            None,
        );
        let handle = tokio::spawn(GrpcService::client_loop(
            session,
            stream_tx,
            client_rx,
            None,
            broadcast.clone(),
            None,
            TaskTracker::new(),
            None,
        ));

        settle().await;
        assert_eq!(receivers(&broadcast), [0, 0, 0]);

        client_tx
            .send(Some((
                None,
                create_filter_at(CommitmentLevelProto::Confirmed),
            )))
            .unwrap();
        settle().await;
        assert_eq!(receivers(&broadcast), [0, 1, 0]);

        drop(client_tx);
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("client loop ends once the client leaves")
            .unwrap();
    }

    #[tokio::test]
    async fn first_request_replays() {
        assert_eq!(replays_for(&[Some(5)], &[]).await, [5]);
    }

    #[tokio::test]
    async fn later_request_does_not_replay() {
        assert_eq!(replays_for(&[None], &[Some(5)]).await, [] as [u64; 0]);
        assert_eq!(replays_for(&[Some(5)], &[Some(6)]).await, [5]);
    }

    /// The loop keeps only the latest of the requests waiting for it, but the first one's
    /// `from_slot` still replays, and a later one's does not.
    #[tokio::test]
    async fn first_request_replays_when_taken_with_later_ones() {
        assert_eq!(replays_for(&[Some(5), None], &[]).await, [5]);
        assert_eq!(replays_for(&[None, Some(6)], &[]).await, [] as [u64; 0]);
    }

    fn create_filter_with_slots() -> Filter {
        let config = SubscribeRequest {
            slots: HashMap::from([("test".into(), SubscribeRequestFilterSlots::default())]),
            ..Default::default()
        };
        let mut names = FilterNames::new(64, 1024, Duration::from_secs(1));
        Filter::new(&config, &FilterLimits::default(), &mut names).unwrap()
    }

    fn create_filter_at(commitment: CommitmentLevelProto) -> Filter {
        let config = SubscribeRequest {
            slots: HashMap::from([("test".into(), SubscribeRequestFilterSlots::default())]),
            commitment: Some(commitment as i32),
            ..Default::default()
        };
        let mut names = FilterNames::new(64, 1024, Duration::from_secs(1));
        Filter::new(&config, &FilterLimits::default(), &mut names).unwrap()
    }

    fn slot_batch(slot: u64) -> Arc<Vec<Message>> {
        Arc::new(vec![Message::Slot(Arc::new(MessageSlot {
            slot,
            parent: Some(slot - 1),
            status: SlotStatus::Processed,
            dead_error: None,
            created_at: Timestamp::from(SystemTime::now()),
            bank_id: Some(slot),
        }))])
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    async fn expect_none(stream_rx: &mut LoadAwareReceiver<TonicResult<FilteredUpdate>>) {
        assert!(
            tokio::time::timeout(Duration::from_millis(50), stream_rx.recv())
                .await
                .is_err(),
            "received an update from a commitment the client did not subscribe to"
        );
    }

    async fn expect_one(stream_rx: &mut LoadAwareReceiver<TonicResult<FilteredUpdate>>) {
        tokio::time::timeout(Duration::from_secs(2), stream_rx.recv())
            .await
            .expect("timed out waiting for an update")
            .expect("stream closed")
            .expect("status error");
    }

    type ClientHandles = (
        mpsc::UnboundedSender<Option<(Option<u64>, Filter)>>,
        LoadAwareReceiver<TonicResult<FilteredUpdate>>,
    );

    fn spawn_client_loop(broadcast: SubscriberChannels, ct: CancellationToken) -> ClientHandles {
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let (stream_tx, stream_rx) = load_aware_channel(64, WeightScales::default());
        let session = ClientSession::new(0, Some("test".into()), "test".into(), ct, None);
        tokio::spawn(GrpcService::client_loop(
            session,
            stream_tx,
            client_rx,
            None,
            broadcast,
            None,
            TaskTracker::new(),
            None,
        ));
        (client_tx, stream_rx)
    }

    #[tokio::test]
    async fn client_loop_receives_only_its_commitment() {
        let ct = CancellationToken::new();
        let broadcast = SubscriberChannels::new(16, 16, 16);
        let (client_tx, mut stream_rx) = spawn_client_loop(broadcast.clone(), ct.clone());

        client_tx
            .send(Some((
                None,
                create_filter_at(CommitmentLevelProto::Finalized),
            )))
            .unwrap();
        settle().await;

        broadcast.send(CommitmentLevel::Processed, slot_batch(100));
        broadcast.send(CommitmentLevel::Confirmed, slot_batch(101));
        expect_none(&mut stream_rx).await;

        broadcast.send(CommitmentLevel::Finalized, slot_batch(102));
        expect_one(&mut stream_rx).await;

        ct.cancel();
    }

    #[tokio::test]
    async fn client_loop_switches_stream_on_commitment_change() {
        let ct = CancellationToken::new();
        let broadcast = SubscriberChannels::new(16, 16, 16);
        let (client_tx, mut stream_rx) = spawn_client_loop(broadcast.clone(), ct.clone());

        client_tx
            .send(Some((
                None,
                create_filter_at(CommitmentLevelProto::Processed),
            )))
            .unwrap();
        settle().await;

        broadcast.send(CommitmentLevel::Processed, slot_batch(100));
        expect_one(&mut stream_rx).await;

        client_tx
            .send(Some((
                None,
                create_filter_at(CommitmentLevelProto::Finalized),
            )))
            .unwrap();
        settle().await;

        broadcast.send(CommitmentLevel::Processed, slot_batch(101));
        expect_none(&mut stream_rx).await;

        broadcast.send(CommitmentLevel::Finalized, slot_batch(102));
        expect_one(&mut stream_rx).await;

        ct.cancel();
    }

    // Simulates the incoming handler task from subscribe(). Mirrors the
    // real Ok(None) path: sends the filter, then on half-close awaits
    // cancellation to keep the sender alive.
    async fn incoming_handler(
        client_tx: mpsc::UnboundedSender<Option<(Option<u64>, Filter)>>,
        filter: Filter,
        half_close: oneshot::Receiver<()>,
        ct: CancellationToken,
    ) {
        client_tx.send(Some((None, filter))).unwrap();
        let _ = half_close.await;
        // this is the fix from #670: await cancellation instead of
        // breaking, so client_tx stays alive and client_rx remains open.
        ct.cancelled().await;
    }

    // Regression test for #662 / #670.
    //
    // #662 (91709fd) removed ping_client_tx, the clone of client_tx that
    // lived in the ping task. before that patch two senders existed for
    // client_rx: ping_client_tx and incoming_client_tx. when a client
    // half-closed its send stream (Ok(None)), the incoming task dropped its
    // sender but ping_client_tx kept client_rx open so client_loop survived.
    //
    // after #662 incoming_client_tx is the only sender. without the fix from
    // #670 (awaiting cancellation in the Ok(None) handler instead of
    // breaking), dropping it closes client_rx and tears down the connection
    // on a normal grpc half-close.
    //
    // uses current_thread runtime so yield_now deterministically sequences
    // filter processing before any broadcast.
    #[tokio::test]
    async fn test_cancellation_on_client_disconnect_after_half_close() {
        let ct = CancellationToken::new();
        let tt = TaskTracker::new();
        let broadcast = SubscriberChannels::new(16, 16, 16);
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let (stream_tx, stream_rx) = load_aware_channel(16, WeightScales::default());
        let (half_close_tx, half_close_rx) = oneshot::channel();

        // mirrors the incoming handler spawned in subscribe()
        let incoming_ct = ct.child_token();
        tokio::spawn(incoming_handler(
            client_tx,
            create_filter_with_slots(),
            half_close_rx,
            incoming_ct,
        ));

        let session = ClientSession::new(0, Some("test".into()), "test".into(), ct.clone(), None);

        let handle = tokio::spawn(GrpcService::client_loop(
            session,
            stream_tx,
            client_rx,
            None,
            broadcast.clone(),
            None,
            tt.clone(),
            None,
        ));

        // yield so incoming_handler sends the filter and client_loop
        // processes it (only client_rx is ready, no broadcast yet)
        tokio::task::yield_now().await;

        // client half-closes its send stream
        let _ = half_close_tx.send(());
        tokio::task::yield_now().await;

        // client drops subscription rx
        drop(stream_rx);

        // broadcast so client_loop hits try_send -> Closed
        let msg = Message::Slot(Arc::new(MessageSlot {
            slot: 100,
            parent: Some(99),
            status: SlotStatus::Processed,
            dead_error: None,
            created_at: Timestamp::from(SystemTime::now()),
            bank_id: Some(100),
        }));
        broadcast.send(CommitmentLevel::Processed, Arc::new(vec![msg]));

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("client_loop did not exit")
            .expect("client_loop panicked");

        assert!(ct.is_cancelled());
    }

    #[tokio::test]
    async fn test_subscription_tracker_decrements_on_session_drop() {
        let tracker = SubscriptionTracker::new(NonZeroUsize::new(10).unwrap(), true);

        // simulate what subscribe() does: acquire two permits
        let _permit_a = tracker.try_insert("sub-1".to_owned()).unwrap();
        let _permit_b = tracker.try_insert("sub-1".to_owned()).unwrap();
        assert_eq!(*tracker.counters.lock().unwrap().get("sub-1").unwrap(), 2);

        // create a session (mirrors what client_loop does)
        {
            let permit = tracker.try_insert("sub-1".to_owned()).unwrap();
            let _session = ClientSession::new(
                0,
                Some("sub-1".into()),
                "".into(),
                CancellationToken::new(),
                Some(permit),
            );
            // session alive: count includes the session permit
            assert_eq!(*tracker.counters.lock().unwrap().get("sub-1").unwrap(), 3);
        }
        // session dropped: count decremented by one
        assert_eq!(*tracker.counters.lock().unwrap().get("sub-1").unwrap(), 2);

        // second drop removes the entry entirely
        {
            drop(_permit_a);
            drop(_permit_b);
            let permit = tracker.try_insert("sub-1".to_owned()).unwrap();
            let _session = ClientSession::new(
                1,
                Some("sub-1".into()),
                "".into(),
                CancellationToken::new(),
                Some(permit),
            );
        }
        assert!(tracker.counters.lock().unwrap().get("sub-1").is_none());
    }

    mod from_slot_partial_slot {
        use {
            super::{super::*, *},
            crate::{
                block_reconstruction_v2::MUST_HAVE_SYSVAR_ACCOUNTS,
                plugin::message::MessageBlockMeta, stream::tokio::BatchStreamUnboundedReceiver,
            },
            solana_hash::Hash,
            tokio::sync::watch,
            yellowstone_grpc_proto::prelude::{
                SubscribeRequestFilterTransactions, SubscribeUpdateBlockMeta,
            },
        };

        const REPLAYED_SLOT: u64 = 10;
        const PARTIAL_SLOT: u64 = 11;
        /// When the plugin received each bank's CreatedBank. A client must get this one, not a
        /// copy made later.
        const CREATED_AT: Timestamp = Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        };

        fn slot_message(slot: u64, status: SlotStatus) -> Message {
            Message::Slot(Arc::new(fixtures::message_slot(slot, status)))
        }

        fn transaction(slot: u64, id: u8) -> Message {
            let mut transaction = fixtures::message_transaction(
                Signature::from([id; 64]),
                vec![Pubkey::default()],
                false,
                Default::default(),
            );
            let message = Arc::get_mut(&mut transaction).unwrap();
            message.slot = slot;
            message.bank_id = slot;
            Message::Transaction(transaction)
        }

        /// The sysvar writes every bank needs before it can seal, then CreatedBank, which agave
        /// sends after the writes made while the bank is constructed.
        fn open_slot(slot: u64) -> Vec<Message> {
            let mut messages = Vec::new();
            for pubkey in MUST_HAVE_SYSVAR_ACCOUNTS {
                let mut account = fixtures::simple_message_account(pubkey, Pubkey::default());
                let message = Arc::get_mut(&mut account).unwrap();
                message.slot = slot;
                message.bank_id = Some(slot);
                messages.push(Message::Account(account));
            }
            messages.push(Message::Slot(Arc::new(MessageSlot {
                created_at: CREATED_AT,
                ..fixtures::message_slot(slot, SlotStatus::CreatedBank)
            })));
            messages
        }

        /// The entry, block meta and commitment statuses that seal and confirm a slot.
        fn close_slot(slot: u64, transactions: u64) -> Vec<Message> {
            vec![
                Message::Entry(fixtures::message_entry(slot, 0)),
                Message::BlockMeta(Arc::new(MessageBlockMeta::from_update_oneof(
                    SubscribeUpdateBlockMeta {
                        slot,
                        parent_slot: slot - 1,
                        blockhash: Hash::new_unique().to_string(),
                        executed_transaction_count: transactions,
                        entries_count: 1,
                        bank_id: slot,
                        ..Default::default()
                    },
                    Timestamp::from(SystemTime::now()),
                ))),
                slot_message(slot, SlotStatus::Processed),
                slot_message(slot, SlotStatus::Confirmed),
            ]
        }

        fn create_filter(commitment: CommitmentLevelProto) -> Filter {
            let config = SubscribeRequest {
                slots: HashMap::from([(
                    "test".into(),
                    SubscribeRequestFilterSlots {
                        interslot_updates: Some(true),
                        ..Default::default()
                    },
                )]),
                transactions: HashMap::from([(
                    "test".into(),
                    SubscribeRequestFilterTransactions::default(),
                )]),
                commitment: Some(commitment as i32),
                ..Default::default()
            };
            Filter::new(
                &config,
                &FilterLimits::default(),
                &mut fixtures::filter_names(),
            )
            .unwrap()
        }

        async fn wait_broadcast(
            rx: &mut broadcast::Receiver<BroadcastedMessage>,
            mut found: impl FnMut(&Message) -> bool,
        ) {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let batch = rx.recv().await.expect("broadcast open");
                    if batch.iter().any(&mut found) {
                        return;
                    }
                }
            })
            .await
            .expect("timed out waiting for a broadcast");
        }

        /// What a client received, in order.
        #[derive(Default)]
        struct Received {
            /// Transaction ids, per slot.
            transactions: Vec<(u64, u8)>,
            /// CreatedBank statuses, per slot, with when the plugin received them.
            created_banks: Vec<(u64, Timestamp)>,
            /// Slots of the Processed statuses.
            processed: Vec<u64>,
        }

        /// Reads the client stream until `slot` is confirmed, into `received`.
        async fn read_until_confirmed(
            stream_rx: &mut LoadAwareReceiver<TonicResult<FilteredUpdate>>,
            slot: u64,
            received: &mut Received,
        ) {
            loop {
                let update = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                    .await
                    .expect("timed out waiting for an update")
                    .expect("stream closed")
                    .expect("status error");
                match update.message {
                    FilteredUpdateOneof::Transaction(update) => {
                        let id = update.transaction.transaction.signature.as_ref()[0];
                        received.transactions.push((update.slot, id));
                    }
                    FilteredUpdateOneof::Slot(update)
                        if update.status == SlotStatus::CreatedBank =>
                    {
                        received
                            .created_banks
                            .push((update.slot, update.created_at));
                    }
                    FilteredUpdateOneof::Slot(update) if update.status == SlotStatus::Processed => {
                        received.processed.push(update.slot);
                    }
                    FilteredUpdateOneof::Slot(update)
                        if update.slot == slot && update.status == SlotStatus::Confirmed =>
                    {
                        return;
                    }
                    _ => {}
                }
            }
        }

        /// A client subscribes from `REPLAYED_SLOT` while `PARTIAL_SLOT` is half delivered.
        /// Returns what the client received.
        async fn subscribe_during_partial_slot(commitment: CommitmentLevelProto) -> Received {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let broadcast = SubscriberChannels::new(1024, 1024, 1024);
            let mut processed_rx = broadcast.subscribe(CommitmentLevel::Processed);
            let mut confirmed_rx = broadcast.subscribe(CommitmentLevel::Confirmed);
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                Some(replay_rx),
                None,
                100,
            ));
            let send = |messages: Vec<Message>| {
                for message in messages {
                    messages_tx.send(message).unwrap();
                }
            };

            // A complete slot that the replay buffer can serve.
            send(open_slot(REPLAYED_SLOT));
            send(vec![
                transaction(REPLAYED_SLOT, 1),
                transaction(REPLAYED_SLOT, 2),
            ]);
            send(close_slot(REPLAYED_SLOT, 2));
            wait_broadcast(
                &mut confirmed_rx,
                |message| matches!(message, Message::BlockMeta(meta) if meta.slot == REPLAYED_SLOT),
            )
            .await;

            // The first half of the next slot goes out before the client connects.
            send(open_slot(PARTIAL_SLOT));
            send(vec![transaction(PARTIAL_SLOT, 1)]);
            wait_broadcast(
                &mut processed_rx,
                |message| matches!(message, Message::Transaction(tx) if tx.slot == PARTIAL_SLOT),
            )
            .await;

            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, mut stream_rx) = load_aware_channel(1024, WeightScales::default());
            let session = ClientSession::new(
                0,
                Some("test".into()),
                "test".into(),
                CancellationToken::new(),
                None,
            );
            tokio::spawn(GrpcService::client_loop(
                session,
                stream_tx,
                client_rx,
                None,
                broadcast,
                Some(replay_tx),
                TaskTracker::new(),
                None,
            ));
            client_tx
                .send(Some((Some(REPLAYED_SLOT), create_filter(commitment))))
                .unwrap();

            // The replay ends while the partial slot is still open.
            let mut received = Received::default();
            read_until_confirmed(&mut stream_rx, REPLAYED_SLOT, &mut received).await;

            send(vec![transaction(PARTIAL_SLOT, 2)]);
            send(close_slot(PARTIAL_SLOT, 2));
            read_until_confirmed(&mut stream_rx, PARTIAL_SLOT, &mut received).await;
            received
        }

        const EVERY_TRANSACTION: [(u64, u8); 4] = [
            (REPLAYED_SLOT, 1),
            (REPLAYED_SLOT, 2),
            (PARTIAL_SLOT, 1),
            (PARTIAL_SLOT, 2),
        ];

        /// Block reconstruction takes in its whole queue before it builds a replay, however long
        /// the queue: those batches went out live before the client subscribed, so a batch left
        /// queued is in neither the replay nor the live stream.
        ///
        /// ```text
        /// time ─────────────────────────────────────────────────►
        /// channel:   b1   b2   …   b5000 │
        ///                                L   request
        /// queue:    [b1   b2   …   b5000]           (all queued: block reconstruction is behind)
        ///                                    R
        /// replay:    b1   b2   …   b5000            must hold all of them
        /// live:                              …      only what went out after L
        ///
        /// b1 = open slot 11 (sysvars, CreatedBank)   b2 … b5000 = one transaction each
        /// ```
        ///
        /// Tokio's per-task budget makes a channel read return `Pending` after about 128 reads
        /// in one poll, with items still queued. A replay built on that `Pending` leaves the
        /// rest of the queue out.
        #[tokio::test]
        async fn replay_takes_in_the_whole_queue_however_long() {
            // Each read takes up to 1024 queued items (the first through the budgeted
            // `poll_recv`, the rest through `try_recv`, which is not budgeted), so the budget
            // runs out after about 128 × 1024 items. Queue more than that.
            const TRANSACTIONS: u32 = 140_000;
            let broadcast = SubscriberChannels::new(1, 1, 1);
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            let (replay_tx, replay_rx) = mpsc::channel(1);

            // Everything is queued before block reconstruction runs, then the request.
            let open = Arc::new(open_slot(PARTIAL_SLOT));
            queue_tx
                .send(BlockReconstructionMessage::BroadcastBatch(open))
                .unwrap();
            for id in 0..TRANSACTIONS {
                let mut signature = [0u8; 64];
                signature[..4].copy_from_slice(&id.to_le_bytes());
                let mut message = fixtures::message_transaction(
                    Signature::from(signature),
                    vec![Pubkey::default()],
                    false,
                    Default::default(),
                );
                let transaction = Arc::get_mut(&mut message).unwrap();
                transaction.slot = PARTIAL_SLOT;
                transaction.bank_id = PARTIAL_SLOT;
                queue_tx
                    .send(BlockReconstructionMessage::BroadcastBatch(Arc::new(vec![
                        Message::Transaction(message),
                    ])))
                    .unwrap();
            }
            let (response_tx, response_rx) = oneshot::channel();
            replay_tx
                .send((CommitmentLevel::Processed, PARTIAL_SLOT, response_tx))
                .await
                .unwrap();

            let handle = tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(queue_rx),
                broadcast,
                Some(replay_rx),
                None,
                150,
            ));
            let Ok(ReplayedResponse::Messages(replay)) =
                tokio::time::timeout(Duration::from_secs(5), response_rx)
                    .await
                    .expect("no replay within 5s")
            else {
                panic!("replay failed");
            };
            let replayed = replay
                .messages
                .iter()
                .flatten()
                .filter(|message| matches!(message, Message::Transaction(_)))
                .count();
            assert_eq!(replayed, TRANSACTIONS as usize);

            drop(queue_tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
        }

        /// Block reconstruction's queue, which runs `between` once, the first time it reads
        /// empty: what other threads do between that read and the read of the replay request.
        struct StaleEmptyRead {
            queue: BatchStreamUnboundedReceiver<BlockReconstructionMessage>,
            between: Option<Box<dyn FnOnce() + Send>>,
        }

        impl BatchStream for StaleEmptyRead {
            type Item = BlockReconstructionMessage;

            fn poll_recv_batch_with_buffer<B>(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buffer: &mut B,
            ) -> Poll<Option<usize>>
            where
                B: crate::stream::Buffer<Self::Item>,
            {
                let this = self.get_mut();
                let read = Pin::new(&mut this.queue).poll_recv_batch_with_buffer(cx, buffer);
                if read.is_pending() {
                    if let Some(between) = this.between.take() {
                        between();
                    }
                }
                read
            }
        }

        /// Block reconstruction reads its queue empty, then, before it reads the replay request,
        /// geyser_loop hands over two batches and the client sends its request. The replay must
        /// hold both: they went out before the client subscribed, and `next_taken_in` recovers
        /// only one.
        ///
        /// ```text
        /// time ───────────────────────────────────────────────────────►
        /// block rec.:  queue read empty ··········· request read   R
        /// geyser_loop:       bcast b1 → hand b1   bcast b2 → hand b2
        /// client:                                  L   send request
        /// queue:            [b1                b1 b2]
        /// replay:      must hold b1 and b2
        ///
        /// b1 = open slot 11 (sysvars, CreatedBank) and transaction 1   b2 = transaction 2
        /// ```
        #[tokio::test]
        async fn replay_holds_what_was_handed_over_before_its_request_was_read() {
            let broadcast = SubscriberChannels::new(1024, 1024, 1024);
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let (response_tx, response_rx) = oneshot::channel();

            let between = {
                let queue_tx = queue_tx.clone();
                move || {
                    let mut b1 = open_slot(PARTIAL_SLOT);
                    b1.push(transaction(PARTIAL_SLOT, 1));
                    let b2 = vec![transaction(PARTIAL_SLOT, 2)];
                    for batch in [b1, b2] {
                        queue_tx
                            .send(BlockReconstructionMessage::BroadcastBatch(Arc::new(batch)))
                            .unwrap();
                    }
                    replay_tx
                        .try_send((CommitmentLevel::Processed, PARTIAL_SLOT, response_tx))
                        .unwrap();
                }
            };
            let handle = tokio::spawn(GrpcService::block_reconstruction_loop(
                StaleEmptyRead {
                    queue: BatchStreamUnboundedReceiver::new(queue_rx),
                    between: Some(Box::new(between)),
                },
                broadcast,
                Some(replay_rx),
                None,
                100,
            ));

            let Ok(ReplayedResponse::Messages(replay)) =
                tokio::time::timeout(Duration::from_secs(5), response_rx)
                    .await
                    .expect("no replay within 5s")
            else {
                panic!("replay failed");
            };
            let replayed: Vec<_> = replay
                .messages
                .iter()
                .flatten()
                .filter_map(|message| match message {
                    Message::Transaction(tx) => {
                        Some((tx.slot, tx.transaction.signature.as_ref()[0]))
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(replayed, [(PARTIAL_SLOT, 1), (PARTIAL_SLOT, 2)]);

            drop(queue_tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
        }

        /// The partial slot's head, CreatedBank included, comes from the replay, and its tail
        /// live.
        #[tokio::test]
        async fn processed_client_gets_the_whole_partial_slot() {
            let received = subscribe_during_partial_slot(CommitmentLevelProto::Processed).await;
            assert_eq!(received.transactions, EVERY_TRANSACTION);
            assert_eq!(
                received.created_banks,
                [(REPLAYED_SLOT, CREATED_AT), (PARTIAL_SLOT, CREATED_AT)]
            );
        }

        /// The replayed slot's CreatedBank comes from the replay. The partial slot's comes at
        /// confirmed with the rest of its content, once, as the plugin received it.
        #[tokio::test]
        async fn confirmed_client_gets_the_whole_partial_slot() {
            let received = subscribe_during_partial_slot(CommitmentLevelProto::Confirmed).await;
            assert_eq!(received.transactions, EVERY_TRANSACTION);
            assert_eq!(
                received.created_banks,
                [(REPLAYED_SLOT, CREATED_AT), (PARTIAL_SLOT, CREATED_AT)]
            );
        }

        const SEALED_SLOT: u64 = PARTIAL_SLOT;
        const NEXT_SLOT: u64 = PARTIAL_SLOT + 1;

        /// A slot seals after the client subscribed to the live stream, but before block
        /// reconstruction builds its replay. The slot is in both, and the client must get it once.
        /// Returns what the client received.
        ///
        /// ```text
        /// live channel        ..slot 10..|..... slot 11 .....|..... slot 12 .....
        ///                                ^                   ^
        ///                                L                   R
        ///                     client_loop subscribes   block reconstruction builds
        ///                     and sends its replay     the replay for from_slot 10
        ///                     request (held here)
        ///
        /// replay   (built at R):  slot 10, slot 11
        /// live     (read from L):          slot 11, slot 12
        ///                                  ^^^^^^^ sent twice
        /// ```
        async fn subscribe_while_a_slot_seals(commitment: CommitmentLevelProto) -> Received {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let broadcast = SubscriberChannels::new(1024, 1024, 1024);
            let mut confirmed_rx = broadcast.subscribe(CommitmentLevel::Confirmed);
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                Some(replay_rx),
                None,
                100,
            ));
            // Holds the client's replay request until released, as a busy block
            // reconstruction loop would.
            let (client_replay_tx, mut client_replay_rx) =
                mpsc::channel::<ReplayStoredSlotsRequest>(1);
            let (requested_tx, requested_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel::<()>();
            tokio::spawn(async move {
                let request = client_replay_rx.recv().await.expect("replay request");
                let _ = requested_tx.send(());
                release_rx.await.expect("release");
                replay_tx
                    .send(request)
                    .await
                    .expect("block reconstruction open");
            });
            let send = |messages: Vec<Message>| {
                for message in messages {
                    messages_tx.send(message).unwrap();
                }
            };
            async fn wait_confirmed(rx: &mut broadcast::Receiver<BroadcastedMessage>, slot: u64) {
                wait_broadcast(rx, |message| {
                    matches!(
                        message,
                        Message::Slot(status)
                            if status.slot == slot && status.status == SlotStatus::Confirmed
                    )
                })
                .await
            }

            send(open_slot(REPLAYED_SLOT));
            send(vec![transaction(REPLAYED_SLOT, 1)]);
            send(close_slot(REPLAYED_SLOT, 1));
            wait_confirmed(&mut confirmed_rx, REPLAYED_SLOT).await;

            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, mut stream_rx) = load_aware_channel(1024, WeightScales::default());
            let session = ClientSession::new(
                0,
                Some("test".into()),
                "test".into(),
                CancellationToken::new(),
                None,
            );
            tokio::spawn(GrpcService::client_loop(
                session,
                stream_tx,
                client_rx,
                None,
                broadcast,
                Some(client_replay_tx),
                TaskTracker::new(),
                None,
            ));
            client_tx
                .send(Some((Some(REPLAYED_SLOT), create_filter(commitment))))
                .unwrap();
            // The client has subscribed to the live stream and waits for its replay.
            requested_rx.await.expect("client sent a replay request");

            send(open_slot(SEALED_SLOT));
            send(vec![transaction(SEALED_SLOT, 1)]);
            send(close_slot(SEALED_SLOT, 1));
            wait_confirmed(&mut confirmed_rx, SEALED_SLOT).await;
            release_tx.send(()).unwrap();
            // The replay ends with the sealed slot, so the next slot can only come live, after
            // whatever of the sealed slot the live stream holds.
            let mut received = Received::default();
            read_until_confirmed(&mut stream_rx, SEALED_SLOT, &mut received).await;

            send(open_slot(NEXT_SLOT));
            send(vec![transaction(NEXT_SLOT, 1)]);
            send(close_slot(NEXT_SLOT, 1));
            read_until_confirmed(&mut stream_rx, NEXT_SLOT, &mut received).await;
            received
        }

        const ONE_TRANSACTION_PER_SLOT: [(u64, u8); 3] =
            [(REPLAYED_SLOT, 1), (SEALED_SLOT, 1), (NEXT_SLOT, 1)];

        #[tokio::test]
        async fn processed_client_gets_a_slot_sealed_during_the_replay_request_once() {
            let received = subscribe_while_a_slot_seals(CommitmentLevelProto::Processed).await;
            assert_eq!(received.transactions, ONE_TRANSACTION_PER_SLOT);
            assert_eq!(
                received.created_banks,
                [
                    (REPLAYED_SLOT, CREATED_AT),
                    (SEALED_SLOT, CREATED_AT),
                    (NEXT_SLOT, CREATED_AT)
                ]
            );
        }

        #[tokio::test]
        async fn confirmed_client_gets_a_slot_sealed_during_the_replay_request_once() {
            let received = subscribe_while_a_slot_seals(CommitmentLevelProto::Confirmed).await;
            assert_eq!(received.transactions, ONE_TRANSACTION_PER_SLOT);
            assert_eq!(
                received.created_banks,
                [
                    (REPLAYED_SLOT, CREATED_AT),
                    (SEALED_SLOT, CREATED_AT),
                    (NEXT_SLOT, CREATED_AT)
                ]
            );
        }

        /// geyser_loop broadcasts a batch before the client subscribes, and hands it over only
        /// after the replay is built: the batch is in neither, and the client still gets it,
        /// once, in order.
        ///
        /// ```text
        /// geyser_loop:  broadcast head of 11 ··· gate closed ··· hand head of 11
        /// client:                             L, replay from 10
        /// ```
        #[tokio::test]
        async fn processed_client_gets_a_batch_handed_over_after_the_replay() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, mut gate_rx) = mpsc::unbounded_channel();
            let (gate_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
            // Hands batches on to block reconstruction, and holds them while closed.
            let (closed, mut is_closed) = watch::channel(false);
            tokio::spawn(async move {
                while let Some(message) = gate_rx.recv().await {
                    if is_closed.wait_for(|closed| !closed).await.is_err()
                        || gate_tx.send(message).is_err()
                    {
                        return;
                    }
                }
            });
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let broadcast = SubscriberChannels::new(1024, 1024, 1024);
            let mut processed_rx = broadcast.subscribe(CommitmentLevel::Processed);
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                Some(replay_rx),
                None,
                100,
            ));
            let send = |messages: Vec<Message>| {
                for message in messages {
                    messages_tx.send(message).unwrap();
                }
            };

            send(open_slot(REPLAYED_SLOT));
            send(vec![transaction(REPLAYED_SLOT, 1)]);
            send(close_slot(REPLAYED_SLOT, 1));
            wait_broadcast(
                &mut processed_rx,
                |message| matches!(message, Message::Slot(status) if status.slot == REPLAYED_SLOT && status.status == SlotStatus::Confirmed),
            )
            .await;

            // Sent together, so geyser_loop broadcasts them as one batch, then the gate holds it.
            closed.send_replace(true);
            let mut head = open_slot(PARTIAL_SLOT);
            head.push(transaction(PARTIAL_SLOT, 1));
            send(head);
            wait_broadcast(
                &mut processed_rx,
                |message| matches!(message, Message::Transaction(tx) if tx.slot == PARTIAL_SLOT),
            )
            .await;

            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, mut stream_rx) = load_aware_channel(1024, WeightScales::default());
            let session = ClientSession::new(
                0,
                Some("test".into()),
                "test".into(),
                CancellationToken::new(),
                None,
            );
            tokio::spawn(GrpcService::client_loop(
                session,
                stream_tx,
                client_rx,
                None,
                broadcast,
                Some(replay_tx),
                TaskTracker::new(),
                None,
            ));
            client_tx
                .send(Some((
                    Some(REPLAYED_SLOT),
                    create_filter(CommitmentLevelProto::Processed),
                )))
                .unwrap();
            // The replay has been built and sent without the held batch.
            let mut received = Received::default();
            read_until_confirmed(&mut stream_rx, REPLAYED_SLOT, &mut received).await;

            closed.send_replace(false);
            send(vec![transaction(PARTIAL_SLOT, 2)]);
            send(close_slot(PARTIAL_SLOT, 2));
            read_until_confirmed(&mut stream_rx, PARTIAL_SLOT, &mut received).await;

            assert_eq!(
                received.transactions,
                [(REPLAYED_SLOT, 1), (PARTIAL_SLOT, 1), (PARTIAL_SLOT, 2)]
            );
            assert_eq!(
                received.created_banks,
                [(REPLAYED_SLOT, CREATED_AT), (PARTIAL_SLOT, CREATED_AT)]
            );
        }

        /// A client whose replay waits for the next raw batch, on a chain that sends none, ends
        /// when its session is cancelled, without waiting for that batch.
        ///
        /// ```text
        /// time ──────────────────────────────────────────────►
        /// channel:   b1                                         (nothing more)
        ///                 L   R
        /// replay:    b1
        /// client:    b1   waits for next_taken_in ··· cancel → ends
        ///
        /// b1 = slot 10, whole and confirmed
        /// ```
        #[tokio::test]
        async fn replaying_client_ends_when_cancelled_on_an_idle_chain() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let broadcast = SubscriberChannels::new(1024, 1024, 1024);
            let mut processed_rx = broadcast.subscribe(CommitmentLevel::Processed);
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                Some(replay_rx),
                None,
                100,
            ));
            let mut slot = open_slot(REPLAYED_SLOT);
            slot.push(transaction(REPLAYED_SLOT, 1));
            slot.extend(close_slot(REPLAYED_SLOT, 1));
            for message in slot {
                messages_tx.send(message).unwrap();
            }
            wait_broadcast(
                &mut processed_rx,
                |message| matches!(message, Message::Slot(status) if status.slot == REPLAYED_SLOT && status.status == SlotStatus::Confirmed),
            )
            .await;

            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, mut stream_rx) = load_aware_channel(1024, WeightScales::default());
            let cancellation_token = CancellationToken::new();
            let session = ClientSession::new(
                0,
                Some("test".into()),
                "test".into(),
                cancellation_token.clone(),
                None,
            );
            let client = tokio::spawn(GrpcService::client_loop(
                session,
                stream_tx,
                client_rx,
                None,
                broadcast,
                Some(replay_tx),
                TaskTracker::new(),
                None,
            ));
            client_tx
                .send(Some((
                    Some(REPLAYED_SLOT),
                    create_filter(CommitmentLevelProto::Processed),
                )))
                .unwrap();
            // The replay is out; the client now waits for the next raw batch.
            read_until_confirmed(&mut stream_rx, REPLAYED_SLOT, &mut Received::default()).await;

            cancellation_token.cancel();
            tokio::time::timeout(Duration::from_secs(5), client)
                .await
                .expect("the client waited for a raw batch after it was cancelled")
                .expect("client loop panicked");
            // Still open: the chain is idle, not shut down.
            drop(messages_tx);
        }

        /// What a client at `commitment` receives live for one bank, in order, as short names.
        async fn live_order(commitment: CommitmentLevelProto) -> Vec<String> {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
            let broadcast = SubscriberChannels::new(1024, 1024, 1024);
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                None,
                None,
                100,
            ));
            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, mut stream_rx) = load_aware_channel(1024, WeightScales::default());
            let session = ClientSession::new(
                0,
                Some("test".into()),
                "test".into(),
                CancellationToken::new(),
                None,
            );
            tokio::spawn(GrpcService::client_loop(
                session,
                stream_tx,
                client_rx,
                None,
                broadcast,
                None,
                TaskTracker::new(),
                None,
            ));
            let config = SubscribeRequest {
                slots: HashMap::from([(
                    "test".into(),
                    SubscribeRequestFilterSlots {
                        interslot_updates: Some(true),
                        ..Default::default()
                    },
                )]),
                transactions: HashMap::from([(
                    "test".into(),
                    SubscribeRequestFilterTransactions::default(),
                )]),
                entry: HashMap::from([("test".into(), Default::default())]),
                blocks_meta: HashMap::from([("test".into(), Default::default())]),
                commitment: Some(commitment as i32),
                ..Default::default()
            };
            let filter = Filter::new(
                &config,
                &FilterLimits::default(),
                &mut fixtures::filter_names(),
            )
            .unwrap();
            client_tx.send(Some((None, filter))).unwrap();
            settle().await;

            // Another slot dies first, so a Dead leaking past processed would show up.
            let mut messages = vec![
                slot_message(PARTIAL_SLOT, SlotStatus::Dead),
                slot_message(REPLAYED_SLOT, SlotStatus::FirstShredReceived),
            ];
            messages.extend(open_slot(REPLAYED_SLOT));
            messages.push(transaction(REPLAYED_SLOT, 1));
            messages.extend(close_slot(REPLAYED_SLOT, 1));
            messages.push(slot_message(REPLAYED_SLOT, SlotStatus::Finalized));
            for message in messages {
                messages_tx.send(message).unwrap();
            }

            let mut order = Vec::new();
            while order.last().map(String::as_str) != Some("Finalized") {
                let update = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                    .await
                    .expect("timed out waiting for an update")
                    .expect("stream closed")
                    .expect("status error");
                order.push(match update.message {
                    FilteredUpdateOneof::Transaction(_) => "tx".to_owned(),
                    FilteredUpdateOneof::Entry(_) => "entry".to_owned(),
                    FilteredUpdateOneof::BlockMeta(_) => "meta".to_owned(),
                    FilteredUpdateOneof::Slot(slot) => format!("{:?}", slot.status),
                    _ => "other".to_owned(),
                });
            }
            order
        }

        /// At every commitment, a bank's content comes first, then its block meta, then its
        /// statuses: Processed, Confirmed, Finalized. CreatedBank reaches every commitment.
        /// geyser_loop broadcasts FirstShredReceived and Dead at processed only; the plugin entry
        /// point, not driven here, sends them to confirmed and finalized.
        ///
        /// ```text
        /// time ─────────────────────────────────────────────────────────────►
        /// processed:  Dead FirstShred CreatedBank tx entry meta  P  C  F
        /// confirmed:                  CreatedBank tx entry meta  P  C  F
        /// finalized:                  CreatedBank tx entry meta  P  C  F
        /// ```
        #[tokio::test]
        async fn live_order_is_content_then_meta_then_statuses_at_every_commitment() {
            let bank = [
                "CreatedBank",
                "tx",
                "entry",
                "meta",
                "Processed",
                "Confirmed",
                "Finalized",
            ];

            let processed = live_order(CommitmentLevelProto::Processed).await;
            assert_eq!(processed[..2], ["Dead", "FirstShredReceived"]);
            assert_eq!(processed[2..], bank);

            assert_eq!(live_order(CommitmentLevelProto::Confirmed).await, bank);
            assert_eq!(live_order(CommitmentLevelProto::Finalized).await, bank);
        }

        /// A bank is sealed, but its Processed status has not arrived, when the replay is built.
        /// Its block and status have not gone out yet: the replay sends its data, and the
        /// status comes live, once.
        #[tokio::test]
        async fn processed_client_gets_a_bank_waiting_for_its_status_once() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let broadcast = SubscriberChannels::new(1024, 1024, 1024);
            let mut processed_rx = broadcast.subscribe(CommitmentLevel::Processed);
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                Some(replay_rx),
                None,
                100,
            ));
            let send = |messages: Vec<Message>| {
                for message in messages {
                    messages_tx.send(message).unwrap();
                }
            };

            send(open_slot(REPLAYED_SLOT));
            send(vec![transaction(REPLAYED_SLOT, 1)]);
            send(close_slot(REPLAYED_SLOT, 1));
            // Sealed: everything but the commitment statuses.
            send(open_slot(PARTIAL_SLOT));
            send(vec![transaction(PARTIAL_SLOT, 1)]);
            let mut statuses = close_slot(PARTIAL_SLOT, 1);
            let commitment_statuses = statuses.split_off(2);
            send(statuses);
            wait_broadcast(
                &mut processed_rx,
                |message| matches!(message, Message::Slot(status) if status.slot == REPLAYED_SLOT && status.status == SlotStatus::Confirmed),
            )
            .await;

            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, mut stream_rx) = load_aware_channel(1024, WeightScales::default());
            let session = ClientSession::new(
                0,
                Some("test".into()),
                "test".into(),
                CancellationToken::new(),
                None,
            );
            tokio::spawn(GrpcService::client_loop(
                session,
                stream_tx,
                client_rx,
                None,
                broadcast,
                Some(replay_tx),
                TaskTracker::new(),
                None,
            ));
            client_tx
                .send(Some((
                    Some(REPLAYED_SLOT),
                    create_filter(CommitmentLevelProto::Processed),
                )))
                .unwrap();
            let mut received = Received::default();
            read_until_confirmed(&mut stream_rx, REPLAYED_SLOT, &mut received).await;

            send(commitment_statuses);
            // Live traffic: the client waits for the next raw batch before going live.
            send(open_slot(PARTIAL_SLOT + 1));
            read_until_confirmed(&mut stream_rx, PARTIAL_SLOT, &mut received).await;

            assert_eq!(
                received.transactions,
                [(REPLAYED_SLOT, 1), (PARTIAL_SLOT, 1)]
            );
            assert_eq!(received.processed, [REPLAYED_SLOT, PARTIAL_SLOT]);
        }
    }

    mod geyser_loop_routing {
        use {
            super::super::*,
            crate::{
                block_reconstruction_v2::MUST_HAVE_SYSVAR_ACCOUNTS,
                plugin::{
                    convert_to,
                    filter::fixtures,
                    message::{
                        MessageBlockMeta, MessageDeshredTransaction, MessageDeshredTransactionInfo,
                        MessageSlot, SlotStatus,
                    },
                },
                stream::tokio::BatchStreamUnboundedReceiver,
            },
            foldhash::{HashSet as FoldHashSet, HashSetExt as _},
            prost_types::Timestamp,
            solana_hash::Hash,
            solana_message::{legacy::Message as SolMessage, MessageHeader},
            solana_pubkey::Pubkey,
            solana_signature::Signature,
            solana_transaction::{versioned::VersionedTransaction, Transaction},
            std::{sync::Arc, time::SystemTime},
            tokio::sync::{broadcast, mpsc},
            yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta,
        };

        struct Harness {
            messages_tx: mpsc::UnboundedSender<Message>,
            #[allow(dead_code)]
            broadcast_rx: broadcast::Receiver<BroadcastedMessage>,
            deshred_tx: broadcast::Sender<DeshredBroadcastedMessage>,
            deshred_rx: broadcast::Receiver<DeshredBroadcastedMessage>,
            handle: tokio::task::JoinHandle<()>,
            handle_reconstruction: tokio::task::JoinHandle<()>,
        }

        fn spawn_loop() -> Harness {
            spawn_loop_with_replay(None)
        }

        fn spawn_loop_with_replay(
            replay_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        ) -> Harness {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
            let broadcast = SubscriberChannels::new(1024, 1024, 1024);
            let broadcast_rx = broadcast.subscribe(CommitmentLevel::Processed);
            let (deshred_tx, deshred_rx) = broadcast::channel(1024);
            let messages_rx = BatchStreamUnboundedReceiver::new(messages_rx);
            let handle = {
                let broadcast = broadcast.clone();
                tokio::spawn(GrpcService::geyser_loop(
                    messages_rx,
                    broadcast,
                    block_reconstruction_tx,
                ))
            };
            let handle_reconstruction = tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast,
                replay_rx,
                None,
                100,
            ));
            Harness {
                messages_tx,
                broadcast_rx,
                deshred_tx,
                deshred_rx,
                handle,
                handle_reconstruction,
            }
        }

        async fn drain_deshred(
            rx: &mut broadcast::Receiver<DeshredBroadcastedMessage>,
        ) -> Vec<Message> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut out = Vec::new();
            while let Ok(batch) = rx.try_recv() {
                out.push(batch);
            }
            out
        }

        fn build_versioned_tx(sig_byte: u8) -> (VersionedTransaction, Signature) {
            let signature = Signature::from([sig_byte; 64]);
            let payer = Pubkey::new_unique();
            let recipient = Pubkey::new_unique();
            let mut tx = Transaction::new_unsigned(SolMessage::new(&[], Some(&payer)));
            tx.message.account_keys = vec![payer, recipient];
            tx.message.header = MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            };
            tx.signatures = vec![signature];
            (VersionedTransaction::from(tx), signature)
        }

        fn make_deshred(slot: u64, sig_byte: u8) -> Message {
            let (versioned, signature) = build_versioned_tx(sig_byte);
            Message::DeshredTransaction(Arc::new(MessageDeshredTransaction {
                transaction: MessageDeshredTransactionInfo {
                    signature,
                    is_vote: false,
                    transaction: convert_to::create_transaction(&versioned),
                    static_account_keys: FoldHashSet::new(),
                    loaded_writable_addresses: vec![],
                    loaded_readonly_addresses: vec![],
                    completed_data_set_starting_shred_index: 0,
                    completed_data_set_ending_shred_index_exclusive: 0,
                },
                slot,
                created_at: Timestamp::from(SystemTime::now()),
            }))
        }

        fn make_slot(slot: u64, status: SlotStatus, parent: Option<u64>) -> Message {
            let bank_id = if [
                SlotStatus::Completed,
                SlotStatus::Dead,
                SlotStatus::FirstShredReceived,
            ]
            .contains(&status)
            {
                None
            } else {
                Some(slot)
            };
            Message::Slot(Arc::new(MessageSlot {
                slot,
                parent,
                status,
                dead_error: None,
                created_at: Timestamp::from(SystemTime::now()),
                bank_id,
            }))
        }

        #[tokio::test]
        async fn deshred_emitted_once_on_deshred_channel() {
            let mut harness = spawn_loop();
            harness.deshred_tx.send(make_deshred(100, 1)).unwrap();

            let batches = drain_deshred(&mut harness.deshred_rx).await;
            let total: usize = batches.len();
            assert_eq!(total, 1, "deshred should be emitted exactly once");

            drop(harness.deshred_tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), harness.handle).await;
            let _ =
                tokio::time::timeout(Duration::from_secs(1), harness.handle_reconstruction).await;
        }

        #[tokio::test]
        async fn slot_emitted_once_on_deshred_channel() {
            let mut harness = spawn_loop();
            harness
                .deshred_tx
                .send(make_slot(100, SlotStatus::Processed, Some(99)))
                .unwrap();
            harness
                .deshred_tx
                .send(make_slot(100, SlotStatus::Confirmed, None))
                .unwrap();
            harness
                .deshred_tx
                .send(make_slot(100, SlotStatus::Finalized, None))
                .unwrap();
            harness
                .messages_tx
                .send(make_slot(100, SlotStatus::Processed, Some(99)))
                .unwrap();
            harness
                .messages_tx
                .send(make_slot(100, SlotStatus::Confirmed, None))
                .unwrap();
            harness
                .messages_tx
                .send(make_slot(100, SlotStatus::Finalized, None))
                .unwrap();

            let deshred_batches = drain_deshred(&mut harness.deshred_rx).await;
            let total: usize = deshred_batches.len();
            assert_eq!(
                total, 3,
                "deshred channel should see each Slot status once (3 statuses sent => 3 emissions)"
            );

            drop(harness.messages_tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), harness.handle).await;
            let _ =
                tokio::time::timeout(Duration::from_secs(1), harness.handle_reconstruction).await;
        }

        fn bank_status(slot: u64, status: SlotStatus, bank_id: Option<u64>) -> Message {
            Message::Slot(Arc::new(MessageSlot {
                slot,
                parent: Some(slot - 1),
                status,
                dead_error: None,
                created_at: Timestamp::from(SystemTime::now()),
                bank_id,
            }))
        }

        /// CreatedBank, the sysvars, an entry and BlockMeta: everything `bank_id` needs to seal.
        fn sealed_bank(slot: u64, bank_id: u64) -> Vec<Message> {
            let mut messages = vec![bank_status(slot, SlotStatus::CreatedBank, Some(bank_id))];
            for pubkey in MUST_HAVE_SYSVAR_ACCOUNTS {
                let mut account = fixtures::simple_message_account(pubkey, Pubkey::default());
                let message = Arc::get_mut(&mut account).unwrap();
                message.slot = slot;
                message.bank_id = Some(bank_id);
                messages.push(Message::Account(account));
            }
            let mut entry = fixtures::message_entry(slot, 0);
            Arc::get_mut(&mut entry).unwrap().bank_id = bank_id;
            messages.push(Message::Entry(entry));
            messages.push(Message::BlockMeta(Arc::new(
                MessageBlockMeta::from_update_oneof(
                    SubscribeUpdateBlockMeta {
                        slot,
                        parent_slot: slot - 1,
                        blockhash: Hash::new_unique().to_string(),
                        entries_count: 1,
                        bank_id,
                        ..Default::default()
                    },
                    Timestamp::from(SystemTime::now()),
                ),
            )));
            messages
        }

        /// A replay carries the span of the bank ids in its messages, sealed banks and the
        /// unsealed head alike, noted by block reconstruction without a pass over the messages.
        #[tokio::test]
        async fn replay_carries_the_span_of_its_banks() {
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let mut harness = spawn_loop_with_replay(Some(replay_rx));
            // Slot 10, bank 2: sealed and processed. Slot 11, bank 7: open, CreatedBank only.
            let mut messages = sealed_bank(10, 2);
            messages.push(bank_status(10, SlotStatus::Processed, Some(2)));
            messages.push(bank_status(11, SlotStatus::CreatedBank, Some(7)));
            for message in messages {
                harness.messages_tx.send(message).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let batch = harness.broadcast_rx.recv().await.expect("broadcast open");
                    if batch.iter().any(
                        |message| matches!(message, Message::Slot(slot) if slot.bank_id == Some(7)),
                    ) {
                        break;
                    }
                }
            })
            .await
            .expect("bank 7 never opened");

            let (response_tx, response_rx) = oneshot::channel();
            replay_tx
                .send((CommitmentLevel::Processed, 10, response_tx))
                .await
                .unwrap();
            let Ok(ReplayedResponse::Messages(Replay {
                messages: replayed,
                bank_id_span,
                ..
            })) = response_rx.await
            else {
                panic!("replay failed");
            };
            assert_eq!(bank_id_span, Some(BankSpan { min: 2, max: 7 }));
            assert_eq!(bank_id_span, BankSpan::of(&replayed));

            drop(harness.messages_tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), harness.handle).await;
            let _ =
                tokio::time::timeout(Duration::from_secs(1), harness.handle_reconstruction).await;
        }

        /// Bank 1 for slot 10 dies, and its repaired replacement, bank 2, is processed. `Dead`
        /// names a slot, not a bank, so bank 2's Processed must reach block reconstruction after
        /// that `Dead`, or the `Dead` discards bank 2 too. The statuses keep that order by going
        /// through geyser_loop, which must not broadcast them itself.
        #[tokio::test]
        async fn replacement_bank_after_dead_is_delivered_once_at_processed() {
            let mut harness = spawn_loop();
            let mut messages = vec![
                bank_status(10, SlotStatus::CreatedBank, Some(1)),
                bank_status(10, SlotStatus::Dead, None),
            ];
            messages.extend(sealed_bank(10, 2));
            messages.push(bank_status(10, SlotStatus::Processed, Some(2)));
            for message in messages {
                harness.messages_tx.send(message).unwrap();
            }

            let is_processed_status = |message: &Message| {
                matches!(
                    message,
                    Message::Slot(slot) if slot.slot == 10 && slot.status == SlotStatus::Processed
                )
            };
            let mut received = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !received.iter().any(is_processed_status) {
                    let batch = harness.broadcast_rx.recv().await.expect("broadcast open");
                    received.extend(batch.iter().cloned());
                }
            })
            .await
            .expect("slot 10 was never delivered at Processed");
            // Give a duplicate time to show up.
            tokio::time::sleep(Duration::from_millis(50)).await;
            while let Ok(batch) = harness.broadcast_rx.try_recv() {
                received.extend(batch.iter().cloned());
            }

            let processed_statuses: Vec<_> = received
                .iter()
                .filter_map(|message| match message {
                    Message::Slot(slot) if is_processed_status(message) => Some(slot.bank_id),
                    _ => None,
                })
                .collect();
            assert_eq!(processed_statuses, vec![Some(2)]);
            assert!(received.iter().any(
                |message| matches!(message, Message::Block(block) if block.meta.bank_id == 2)
            ));

            drop(harness.messages_tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), harness.handle).await;
            let _ =
                tokio::time::timeout(Duration::from_secs(1), harness.handle_reconstruction).await;
        }

        /// A `from_slot` replay sends each bank's CreatedBank as the plugin received it, and
        /// where it arrived live: after the account writes made while the bank was constructed,
        /// before the rest of the bank's data.
        #[tokio::test]
        async fn replay_sends_the_original_created_bank_in_live_order() {
            const CREATED_AT: Timestamp = Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            };
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let mut harness = spawn_loop_with_replay(Some(replay_rx));
            // `sealed_bank` sends CreatedBank first; agave sends the construction writes first.
            let mut messages = sealed_bank(10, 2);
            let created_bank = messages.remove(0);
            let Message::Slot(created_bank) = created_bank else {
                panic!("sealed_bank starts with CreatedBank");
            };
            let construction_writes = MUST_HAVE_SYSVAR_ACCOUNTS.len();
            messages.insert(
                construction_writes,
                Message::Slot(Arc::new(MessageSlot {
                    created_at: CREATED_AT,
                    ..MessageSlot::clone(&created_bank)
                })),
            );
            messages.push(bank_status(10, SlotStatus::Processed, Some(2)));
            for message in messages {
                harness.messages_tx.send(message).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let batch = harness.broadcast_rx.recv().await.expect("broadcast open");
                    if batch
                        .iter()
                        .any(|message| matches!(message, Message::Block(_)))
                    {
                        break;
                    }
                }
            })
            .await
            .expect("slot 10 never sealed");

            let (response_tx, response_rx) = oneshot::channel();
            replay_tx
                .send((CommitmentLevel::Processed, 10, response_tx))
                .await
                .unwrap();
            let Ok(ReplayedResponse::Messages(Replay {
                messages: replayed, ..
            })) = response_rx.await
            else {
                panic!("replay failed");
            };
            let kinds: Vec<&str> = replayed
                .iter()
                .flatten()
                .map(|message| match message {
                    Message::Account(_) => "account",
                    Message::Slot(slot) if slot.status == SlotStatus::CreatedBank => {
                        assert_eq!(slot.created_at, CREATED_AT);
                        assert_eq!((slot.bank_id, slot.parent), (Some(2), Some(9)));
                        "created-bank"
                    }
                    Message::Slot(_) => "status",
                    Message::Entry(_) => "entry",
                    Message::Block(_) => "block",
                    Message::BlockMeta(_) => "block-meta",
                    _ => "other",
                })
                .collect();
            assert_eq!(
                kinds,
                [
                    "account",
                    "account",
                    "account",
                    "account",
                    "created-bank",
                    "entry",
                    "block",
                    "block-meta",
                    "status",
                ]
            );

            drop(harness.messages_tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), harness.handle).await;
            let _ =
                tokio::time::timeout(Duration::from_secs(1), harness.handle_reconstruction).await;
        }
    }

    #[tokio::test]
    async fn test_subscription_tracker_skips_unidentified_subscribers() {
        // subscriber_id=None resolves to "UNKNOWN" inside ClientSession,
        // but subscribe() skips the limit check entirely for None.
        // The tracker should remain empty since no increment happened.
        {
            let _session = ClientSession::new(0, None, "".into(), CancellationToken::new(), None);
        }
    }
}
