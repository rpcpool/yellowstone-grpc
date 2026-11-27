use {
    crate::{
        auth::{
            ConstantSubscriptionRepository, HttpSubscriptionRepository, SubscriptionInfo,
            TrustedMetadataAuthenticator,
        },
        billing::{BillingMeteredManager, HttpBillingEventSink},
        block_reconstruction_v2::BlockMachineStorage,
        body::block_shape::{BlockShapeBudget, BlockShapeWriteSettings},
        compression::{
            pool::{CompressorPools, CompressorPoolsOwner},
            slot::GroupCompressionSettings,
            EnabledEncodings, PooledStreamCompressor, ResponseEncoding, StandardCompressionCodec,
            WholeMessageCompressor,
        },
        config::{
            AuthConfig, AuthKind, BillingConfig, CompressionDefaults, CompressionOverrides,
            CompressionSharingMode, ConfigGrpc, GrpcAddress, GrpcTlsConfig,
        },
        contact_info::{self, ContactInfoState},
        control::compile_lane::FilterCompileLane,
        extension::seams::ThreadRole,
        file_watcher::FileWatcher,
        geyser_subscriber::{BankSpan, ReplayCut, ReplayPhase},
        metered::PrometheusMeteredManager,
        metrics::{self, incr_grpc_method_call_count, subscription_limit_exceeded_inc},
        plugin::{
            filter::{
                limits::FilterLimits,
                message::{
                    FilteredUpdate, FilteredUpdateDeshred, FilteredUpdateOneof, WeightScales,
                },
                name::FilterNames,
                DeshredFilter, Filter, MatchScratch,
            },
            message::{CommitmentLevel, Message, MessageBlockMeta, MessageSlot, SlotStatus},
            proto::geyser_server::{Geyser, GeyserServer},
        },
        ratelimit::{MethodRatelimiter, PrometheusRatelimitCallbacks},
        reverse_index::{
            let_woken_task_run_first, BankKeyIndex, BankThread, BankThreadHandle, ClientIndexing,
            IndexChange, IndexKey, IndexWorkLimits, LeaveOnDrop, OwnerIndexes, ProcessedOwner,
            RelayedItem, INDEX_KEYS_PER_STEP,
        },
        stream::{tokio::BatchStreamUnboundedReceiver, BatchStream, BatchStreamExt, Buffer},
        subscribe::{
            entry::SubscribeQueueEntry,
            group::MemberQueueEntries,
            response_body::GroupSharingSettings,
            sender::{
                try_send_member_entries, LiveSendFailure, StreamUpdateSender, WaitingSendError,
            },
            service::{GeyserServiceSplitter, SubscribeCallSettings, SubscribeService},
        },
        telemetry::{
            data_plane_counts::{DataPlaneCount, DATA_PLANE_COUNTS},
            extended_metrics::{CountedSubscribeStream, RetainedBudgets},
            stream_counters::{StreamCounters, StreamQueueKind, SUBSCRIBER_COUNTS},
        },
        threads::PluginThreads,
        util::stream::{
            load_aware_channel, LoadAwareReceiver, LoadAwareSender, TrySendError, UnitWeigher,
            Weigher,
        },
        version::GrpcVersionInfo,
    },
    anyhow::Context as _,
    bytesize::ByteSize,
    futures::{future::OptionFuture, Stream, StreamExt},
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
            Arc, Mutex as StdMutex, PoisonError,
        },
        task::{Context, Poll},
        time::SystemTime,
    },
    tokio::{
        io::{AsyncRead, AsyncWrite},
        net::UnixListener,
        sync::{broadcast, mpsc, oneshot, Mutex, RwLock, Semaphore},
        time::{sleep, sleep_until, Duration, Instant},
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
        Code, Request, Response, Result as TonicResult, Status, Streaming,
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

/// Longest wait for room to send a client its last status before the stream ends without it.
const LAST_STATUS_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// Starts the storage's thread through `plugin_threads`. The plugin's token stops it, and
    /// the unload joins it.
    fn new(
        unary_concurrency_limit: usize,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
        plugin_threads: &PluginThreads,
    ) -> io::Result<(Self, mpsc::UnboundedSender<Message>)> {
        let inner = Arc::new(RwLock::new(BlockMetaStorageInner::default()));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let storage = Arc::clone(&inner);
        let completion_token = task_tracker.token();
        plugin_threads.spawn_plugin_thread(ThreadRole::BlockMetaStorage, 0, move || {
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
        })?;

        Ok((
            Self {
                read_sem: Semaphore::new(unary_concurrency_limit),
                inner,
            },
            tx,
        ))
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
    /// An event never in a raw batch: a block meta, footer or commitment status,
    /// which block reconstruction holds until the bank seals and then sends in its own
    /// batches.
    SealGated(Message),
}

pub type BroadcastedMessage = Arc<Vec<Message>>;

/// Weighs a Subscribe queue update with `WeightScales::update_weight`. Errors weigh `1`.
impl Weigher for WeightScales {
    type Item = TonicResult<SubscribeQueueEntry>;

    fn weight(&self, update: &Self::Item) -> u32 {
        update
            .as_ref()
            .map_or(1, |queue_entry| queue_entry.update_weight(self))
    }
}

/// A Subscribe stream's ping task: every 10 s, from its first tick, it samples the stream's queue
/// size and queues a ping. A refused ping cancels the client, whose loop may never write.
pub async fn run_subscribe_pings(
    session_id: usize,
    ping_subscriber_id: Option<String>,
    ping_stream_tx: SubscribeSender,
    ping_stream_counters: Arc<StreamCounters>,
    ping_cancellation_token: CancellationToken,
    ping_client_cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = ping_cancellation_token.cancelled() => {
                info!("client #{ping_subscriber_id:?}/{session_id}: ping cancelled");
                break;
            }
            _ = interval.tick() => {
                ping_stream_counters.observe_queue_size(ping_stream_tx.current_weight() as u64);
                let ping_update = FilteredUpdate::new_empty(FilteredUpdateOneof::ping());
                log::info!("client #{ping_subscriber_id:?}/{session_id}: sending ping");
                let ping_entry = SubscribeQueueEntry::Filtered(ping_update);
                if ping_stream_tx.send(Ok(ping_entry)).await.is_err() {
                    // A client with no filter gets no writes, so only a refused ping shows that its
                    // connection closed.
                    ping_client_cancel.cancel();
                    info!("detected dead client #{ping_subscriber_id:?}/{session_id}");
                    break;
                }
            }
        }
    }
    info!("client #{ping_subscriber_id:?}/{session_id}: ping task exiting");
}

/// An unrouted client's broadcast step for one message: [`try_send_member_entries`], then the
/// entries queued, counted in the client's stream counters.
pub fn queue_unrouted_updates(
    stream_tx: &SubscribeSender,
    member_filter: &Arc<Filter>,
    routed_message: &Message,
    commitment_level: Option<CommitmentLevel>,
    match_scratch: &mut MatchScratch,
    stream_counters: &StreamCounters,
) -> Result<(), LiveSendFailure> {
    let mut sent_entry_count = 0;
    let send_result = try_send_member_entries(
        stream_tx,
        member_filter,
        routed_message,
        commitment_level,
        match_scratch,
        &mut sent_entry_count,
    );
    if sent_entry_count > 0 {
        stream_counters.add_sent_updates(sent_entry_count);
    }
    send_result
}

/// The build's compression: its default values, and the codec the plugin's Subscribe body
/// compresses with, built at those values with the config's overrides.
pub struct BuildCompression {
    pub build_compression_defaults: CompressionDefaults,
    pub stream_compressor: Arc<dyn WholeMessageCompressor>,
}

impl BuildCompression {
    /// The standard codec at `build_compression_defaults` with `compression_overrides` over them.
    ///
    /// # Errors
    ///
    /// Fails when a level is outside its codec's range.
    pub fn standard(
        build_compression_defaults: CompressionDefaults,
        compression_overrides: CompressionOverrides,
    ) -> Result<Self, crate::extension::seams::ExtensionError> {
        let effective_compression_defaults =
            build_compression_defaults.with_overrides(compression_overrides);
        let standard_codec = StandardCompressionCodec::new(&effective_compression_defaults)?;
        Ok(Self {
            build_compression_defaults,
            stream_compressor: Arc::new(PooledStreamCompressor::new(standard_codec)),
        })
    }
}

/// Sender half of a Subscribe queue.
pub type SubscribeSender = LoadAwareSender<TonicResult<SubscribeQueueEntry>, WeightScales>;

/// A Subscribe response's stream: the receiving half of the client's queue.
pub type SubscribeResponseStream = LoadAwareReceiver<TonicResult<SubscribeQueueEntry>>;

/// Sender half of a SubscribeDeshred queue, which bounds the number of queued updates.
type DeshredSubscribeSender = LoadAwareSender<
    TonicResult<FilteredUpdateDeshred>,
    UnitWeigher<TonicResult<FilteredUpdateDeshred>>,
>;

/// The broadcast channel of each commitment level, and the index change senders of the owners
/// that deliver through an index.
///
/// Each level has one owner, its only sender: `geyser_loop` at processed, and block
/// reconstruction at confirmed and finalized. A client whose filter has index keys sends its
/// index changes to its level's owner, which then queues the client's updates straight into the
/// client's queue. Any other client reads its level's channel.
#[derive(Debug, Clone)]
pub struct SubscriberChannels {
    processed: broadcast::Sender<BroadcastedMessage>,
    confirmed: broadcast::Sender<BroadcastedMessage>,
    finalized: broadcast::Sender<BroadcastedMessage>,
    /// The index change senders of [`GrpcService::geyser_loop`] and
    /// [`GrpcService::block_reconstruction_loop`], or [`None`] when indexing is off.
    index_change_txs: Option<[mpsc::UnboundedSender<IndexChange>; 2]>,
    /// How much index work each owner does, and how often.
    index_work_limits: IndexWorkLimits,
}

impl SubscriberChannels {
    /// Creates the three broadcast channels, with indexing off.
    ///
    /// # Panics
    ///
    /// Panics if a capacity is `0` or above `usize::MAX / 2`, as [`broadcast::channel`] does.
    pub fn new(processed: usize, confirmed: usize, finalized: usize) -> Self {
        Self {
            processed: broadcast::channel(processed).0,
            confirmed: broadcast::channel(confirmed).0,
            finalized: broadcast::channel(finalized).0,
            index_change_txs: None,
            index_work_limits: IndexWorkLimits::default(),
        }
    }

    /// Turns indexing on, with an index change channel for each owner.
    fn with_indexing(
        mut self,
        index_work_limits: IndexWorkLimits,
    ) -> (Self, [mpsc::UnboundedReceiver<IndexChange>; 2]) {
        let (geyser_loop_changes_tx, geyser_loop_changes_rx) = mpsc::unbounded_channel();
        let (reconstruction_changes_tx, reconstruction_changes_rx) = mpsc::unbounded_channel();
        self.index_change_txs = Some([geyser_loop_changes_tx, reconstruction_changes_tx]);
        self.index_work_limits = index_work_limits;
        (self, [geyser_loop_changes_rx, reconstruction_changes_rx])
    }

    /// The index limits each owner's [`OwnerIndexes`] keeps to.
    pub const fn index_work_limits(&self) -> IndexWorkLimits {
        self.index_work_limits
    }

    /// Sends an index change to the owner of `commitment_level`: [`GrpcService::geyser_loop`] at
    /// processed, [`GrpcService::block_reconstruction_loop`] at confirmed and finalized.
    #[allow(rustdoc::private_intra_doc_links)]
    pub fn send_index_change(
        &self,
        commitment_level: CommitmentLevel,
        index_change: IndexChange,
    ) -> Option<IndexChange> {
        let Some(index_change_txs) = &self.index_change_txs else {
            return Some(index_change);
        };
        let index_change_tx_index = usize::from(commitment_level != CommitmentLevel::Processed);
        index_change_txs[index_change_tx_index]
            .send(index_change)
            .err()
            .map(|unsent_change| unsent_change.0)
    }

    /// The index keys of `client_filter`, when indexing is on, in the [`Arc`] of
    /// `held_index_keys` when the two lists are equal.
    ///
    /// An owner compares key lists by [`Arc`] only, so a client that keeps its keys keeps its
    /// [`Arc`] too, and its owner replaces the indexed subscriber's filter in place.
    pub fn index_keys(
        &self,
        client_filter: &Filter,
        held_index_keys: Option<&Arc<[IndexKey]>>,
    ) -> Option<Arc<[IndexKey]>> {
        self.index_change_txs.as_ref()?;
        let listed_index_keys = client_filter.index_keys()?;
        match held_index_keys {
            Some(held_index_keys) if held_index_keys[..] == listed_index_keys[..] => {
                Some(Arc::clone(held_index_keys))
            }
            _ => Some(listed_index_keys.into()),
        }
    }

    /// A guard that has the owners remove the client's indexed subscriber when the client's task
    /// ends.
    pub fn leave_on_drop(&self, session_id: usize) -> Option<LeaveOnDrop> {
        let index_change_txs = self.index_change_txs.clone()?;
        Some(LeaveOnDrop {
            session_id,
            index_change_txs: index_change_txs.map(Some),
        })
    }

    /// The broadcast sender of a commitment channel.
    #[inline]
    const fn sender(&self, commitment: CommitmentLevel) -> &broadcast::Sender<BroadcastedMessage> {
        match commitment {
            CommitmentLevel::Processed => &self.processed,
            CommitmentLevel::Confirmed => &self.confirmed,
            CommitmentLevel::Finalized => &self.finalized,
        }
    }

    /// Broadcasts `messages` on the `commitment` channel, and drops them if no receiver is
    /// subscribed.
    ///
    /// An indexed client gets nothing sent here: each level's owner sends through its
    /// [`OwnerIndexes`], which calls this after it delivers the batch.
    #[inline]
    pub fn send(&self, commitment: CommitmentLevel, messages: BroadcastedMessage) {
        let _ = self.sender(commitment).send(messages);
    }

    /// Subscribes a new receiver to the `commitment` channel.
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

/// One item of a [`Replay`]: one message, or the messages of a batch in a range.
pub enum ReplayResponseMessageType {
    /// One message.
    Single(Message),
    /// The messages of `batch` in `range`, so part of a shared batch is replayed without a copy.
    Batch(Arc<Vec<Message>>, Range<usize>),
    /// The messages of a sealed bank in `range`, all of them that bank's accounts,
    /// transactions and entries, with the bank's [`BankKeyIndex`] once the bank thread
    /// built it, so a client with index keys visits only the messages its keys find.
    SealedBank(Arc<Vec<Message>>, Range<usize>, Option<Arc<BankKeyIndex>>),
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
            ReplayResponseMessageType::Batch(batch, range)
            | ReplayResponseMessageType::SealedBank(batch, range, _) => {
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
/// at L. To join the two, the client needs R as a point on the channel. Two kinds of batches
/// feed the processed channel:
///
/// - [`GrpcService::geyser_loop`] broadcasts the raw batches (accounts, transactions, entries,
///   lifecycle statuses) as they arrive from the plugin, then hands each to block
///   reconstruction;
/// - [`GrpcService::block_reconstruction_loop`] relays what it builds once a bank seals:
///   its Block, footer and block meta, and its commitment statuses.
///
/// So R is two batches: the last raw batch block reconstruction took in from geyser_loop, and
/// the last batch it sent itself. Both are kept here as the very `Arc` that went out, so
/// the client can find them on its receiver with [`Arc::ptr_eq`]. The confirmed and finalized
/// channels are fed by block reconstruction, so only the second batch counts for them.
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
/// // In `take_in`: record each raw batch taken in, and send each built batch through `send`.
/// position.last_taken_in = Some(Arc::clone(&batch));
/// position.send(CommitmentLevel::Processed, block_messages);
/// // When building a replay at `commitment`:
/// let cut = position.cut(commitment);
/// ```
#[derive(Default)]
struct BroadcastPosition {
    /// The last raw batch taken in from geyser_loop. Raw batches go out on the processed
    /// channel only.
    last_taken_in: Option<BroadcastedMessage>,
    /// The last batch block reconstruction sent on each channel, indexed by
    /// [`Self::index`]: processed, confirmed, finalized.
    last_sent: [Option<BroadcastedMessage>; 3],
    /// Banks whose block, block meta and Processed status went out at processed, with their
    /// slot. A bank sealed but not in here is still waiting for its Processed status, so a
    /// processed replay sends only its data. Pruned below the oldest replayable slot.
    sent_at_processed: HashMap<BankId, Slot>,
    /// Processed replays waiting for the next raw batch taken in. See
    /// [`ReplayCut::next_taken_in`].
    waiting_next_taken_in: Vec<oneshot::Sender<BroadcastedMessage>>,
    /// The confirmed and finalized batches of the current pass, in send order. At the end of
    /// the pass, [`GrpcService::take_in`] sends them through block reconstruction's
    /// [`OwnerIndexes`].
    held_batches: Vec<(CommitmentLevel, BroadcastedMessage)>,
    /// The processed batches of the current pass, in send order. [`GrpcService::take_in`]
    /// relays them to geyser_loop before the held batches when the pass seals a bank at
    /// processed, and last in the pass otherwise.
    held_relays: Vec<BroadcastedMessage>,
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

    /// Holds `messages` for the `commitment` channel until the end of the pass, in
    /// [`Self::held_relays`] at processed and in [`Self::held_batches`] otherwise, and records
    /// it as that channel's last batch sent.
    ///
    /// # Arguments
    ///
    /// * `commitment` - The channel to send on.
    /// * `messages` - The batch to send.
    fn send(&mut self, commitment: CommitmentLevel, messages: BroadcastedMessage) {
        if commitment == CommitmentLevel::Processed {
            self.held_relays.push(Arc::clone(&messages));
        } else {
            self.held_batches.push((commitment, Arc::clone(&messages)));
        }
        self.last_sent[Self::index(commitment)] = Some(messages);
    }

    /// Relays the held processed batches to geyser_loop in send order.
    fn relay_held_batches(&mut self, relay_tx: &mpsc::UnboundedSender<RelayedItem>) -> bool {
        let relays_a_batch = !self.held_relays.is_empty();
        for held_relay in self.held_relays.drain(..) {
            let _ = relay_tx.send(RelayedItem::Batch(held_relay));
        }
        relays_a_batch
    }

    /// Where a replay at `commitment` built now ends. At processed, it also registers the
    /// replay for the next raw batch taken in.
    ///
    /// The cut means the same at every commitment: where the replay ends on that channel. It
    /// needs more at processed only because that channel has two kinds of batches out of step:
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
    ///   sends. Everything up to [`ReplayCut::last_sent`] is in the replay, everything after
    ///   is not, and no raw batch is in flight between two tasks: `last_taken_in` and
    ///   `next_taken_in` are [`None`]. The lifecycle statuses it also sends there are never in a
    ///   replay, so the client drops none of them, on either side of the cut.
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
                .unwrap_or_else(PoisonError::into_inner);
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
        let mut tracker = self.counters.lock().unwrap_or_else(PoisonError::into_inner);
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
    #[error("gRPC connection closed: {}", .0.reason)]
    ClientGrpcConnectionClosed(Disconnect),
    #[error("client session is cancelled by plugin")]
    Cancelled,
}

/// Why a client session ends, and the last status the client gets, if any.
///
/// # Examples
///
/// ```ignore
/// let disconnect = Disconnect::from_read_error(&error, "SubscribeRequest");
/// session.disconnect_reason = disconnect.end(stream_tx, &task_tracker);
/// ```
#[derive(Debug)]
pub struct Disconnect {
    reason: &'static str,
    status: Option<Status>,
}

impl Disconnect {
    /// Builds a disconnect that sends the client no status.
    ///
    /// # Arguments
    ///
    /// * `reason` - The disconnect reason the session logs and counts.
    ///
    /// # Returns
    ///
    /// A [`Disconnect`] with `reason` and no status.
    const fn quiet(reason: &'static str) -> Self {
        Self {
            reason,
            status: None,
        }
    }

    /// Builds the disconnect for a failed read of a client's request stream.
    ///
    /// An oversized request is a decoder error: [`Code::OutOfRange`], or
    /// [`Code::ResourceExhausted`] while decompressing, with no source error.
    ///
    /// # Arguments
    ///
    /// * `error` - The error tonic returned while reading the next request.
    /// * `request_name` - The request type, for example `SubscribeRequest`, named in the status.
    ///
    /// # Returns
    ///
    /// `request_too_large` with a [`Code::ResourceExhausted`] status that carries tonic's
    /// message for an oversized request, and a quiet `client_disconnect` otherwise.
    fn from_read_error(error: &Status, request_name: &str) -> Self {
        let decoder_size_error = matches!(error.code(), Code::OutOfRange | Code::ResourceExhausted)
            && std::error::Error::source(error).is_none();
        if !decoder_size_error {
            return Self::quiet("client_disconnect");
        }
        Self {
            reason: "request_too_large",
            status: Some(Status::resource_exhausted(format!(
                "{request_name} is too large: {}",
                error.message()
            ))),
        }
    }

    /// Ends a client session: sends the status, if any, after the queued updates.
    ///
    /// The client loop is the only sender of updates, so the status is the stream's last item.
    ///
    /// # Arguments
    ///
    /// * `stream_tx` - The client's update stream.
    /// * `task_tracker` - Tracks the task that waits up to [`LAST_STATUS_TIMEOUT`] for room.
    ///
    /// # Returns
    ///
    /// The disconnect reason for the session to log and count.
    fn end<T, W>(
        self,
        stream_tx: LoadAwareSender<Result<T, Status>, W>,
        task_tracker: &TaskTracker,
    ) -> &'static str
    where
        T: Send + 'static,
        W: Weigher<Item = Result<T, Status>> + Send + Sync + 'static,
    {
        if let Some(status) = self.status {
            task_tracker.spawn(async move {
                let _ =
                    tokio::time::timeout(LAST_STATUS_TIMEOUT, stream_tx.send(Err(status))).await;
            });
        }
        self.reason
    }
}

/// What a request reader sends its client loop: a new filter, or why it stopped reading.
pub type ClientRequest<F> = Result<F, Disconnect>;

/// One Subscribe client's session: its session id, filter and disconnect reason, its
/// subscription permit, and the sender whose drop tells the request task that the client's live
/// stream has started.
///
/// Dropping it counts the disconnect under its reason, takes it out of the subscription
/// metrics, and cancels its cancellation token.
pub struct ClientSession {
    pub id: usize,
    pub subscriber_id: String,
    endpoint: String,
    /// The client's filter, shared with its indexed subscriber while the client is indexed.
    pub filter: Arc<Filter>,
    pub cancellation_token: CancellationToken,
    pub disconnect_reason: &'static str,
    /// Must own it so that when the session is dropped, the subscription count is decremented.
    _subscription_permit: Option<SubscriptionOwnedPermit>,
    /// Dropped where [`GrpcService::client_loop`] starts the live stream, which lets
    /// [`GrpcService::read_requests`] post the client's later filter changes. [`None`] when no
    /// request task waits for it.
    client_live_tx: Option<oneshot::Sender<()>>,
    /// The session's stream in `subscribe_streams`, counted as ended when the session drops.
    counted_stream: Option<CountedSubscribeStream>,
    /// The stream's counters, which the drop closes after the stream's last queue size sample.
    stream_counters: Option<Arc<StreamCounters>>,
}

impl ClientSession {
    /// Creates a session with the default filter, which matches nothing, and counts it in the
    /// subscription metrics.
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
        let stream_counters =
            SUBSCRIBER_COUNTS.open_stream(&subscriber_id, Some(StreamQueueKind::Normal));
        Self {
            id,
            subscriber_id,
            endpoint,
            filter: Arc::new(filter),
            cancellation_token,
            disconnect_reason: "unknown",
            _subscription_permit: subscription_permit,
            client_live_tx: None,
            counted_stream: None,
            stream_counters: Some(stream_counters),
        }
    }

    /// The stream's counters, which every producer of the stream counts into.
    pub const fn stream_counters(&self) -> &Arc<StreamCounters> {
        (self.stream_counters.as_ref()).expect("a session holds its stream counters until it drops")
    }

    /// Replaces the client's filter, which keeps the session's block pins, and moves the
    /// subscription metrics from the old filter to the new one.
    fn set_filter(&mut self, mut new_filter: Filter) {
        metrics::update_subscriptions(&self.endpoint, Some(&self.filter), Some(&new_filter));
        new_filter.inherit_block_pins(&self.filter);
        self.filter = Arc::new(new_filter);
    }
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        if let Some(stream_counters) = self.stream_counters.take() {
            stream_counters.observe_queue_size(0);
            SUBSCRIBER_COUNTS.close_stream(&self.subscriber_id, stream_counters);
        }
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
    /// The stream's counters, which the drop closes after the stream's last queue size sample.
    stream_counters: Option<Arc<StreamCounters>>,
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
        let stream_counters =
            SUBSCRIBER_COUNTS.open_stream(&subscriber_id, Some(StreamQueueKind::Deshred));
        Self {
            id,
            subscriber_id,
            filter: DeshredFilter::default(),
            cancellation_token,
            disconnect_reason: "unknown",
            _subscription_permit: subscription_permit,
            stream_counters: Some(stream_counters),
        }
    }

    /// The stream's counters, which the deshred loop counts into.
    const fn stream_counters(&self) -> &Arc<StreamCounters> {
        (self.stream_counters.as_ref()).expect("a session holds its stream counters until it drops")
    }
}

impl Drop for DeshredClientSession {
    fn drop(&mut self) {
        if let Some(stream_counters) = self.stream_counters.take() {
            stream_counters.observe_queue_size(0);
            SUBSCRIBER_COUNTS.close_stream(&self.subscriber_id, stream_counters);
        }
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

/// The Geyser gRPC service: what every `Subscribe` stream and unary call shares, with the
/// limits and intervals of its [`ConfigGrpc`]. [`GrpcService::create`] builds it, serves it on
/// every listener, and starts the loops that feed its clients.
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
    /// The shortest time between two filter changes posted for one client, from
    /// [`ConfigGrpc::filter_apply_interval`].
    filter_apply_interval: Duration,
    /// Compiles filters off the delivery workers; `None` compiles them inline.
    filter_compile_lane: Option<FilterCompileLane>,
    /// The delivery runtime, where every client task of the setups runs.
    delivery_runtime: tokio::runtime::Handle,
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

/// An owner task's end before the plugin token is cancelled: its drop then logs one error line and
/// counts it. A clean unload cancels first, so it logs nothing (arch 11.3).
pub struct OwnerEndGuard {
    owner_name: &'static str,
    plugin_token: CancellationToken,
}

impl OwnerEndGuard {
    pub fn new(owner_name: &'static str, plugin_token: &CancellationToken) -> Self {
        Self {
            owner_name,
            plugin_token: plugin_token.clone(),
        }
    }
}

impl Drop for OwnerEndGuard {
    fn drop(&mut self) {
        if !self.plugin_token.is_cancelled() {
            log::error!("the {} ended before the unload", self.owner_name);
            DATA_PLANE_COUNTS.add(DataPlaneCount::OwnerEarlyEnd, 1);
        }
    }
}

/// The senders [`GrpcService::create`] hands back to the plugin, whose callbacks feed them, and
/// the handle of the bank thread, which the plugin joins when it unloads.
pub struct GrpcServiceResult {
    pub snapshot_tx: Option<crossbeam_channel::Sender<Box<Message>>>,
    pub deshred_broadcast_tx: broadcast::Sender<DeshredBroadcastedMessage>,
    pub contact_info_tx: mpsc::UnboundedSender<contact_info::ContactInfoNotification>,
    pub blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
    /// Stops and joins the bank thread once the plugin's runtime has shut down.
    pub bank_thread_handle: BankThreadHandle,
    /// The compressor pools' owner: `on_unload` stops them at its step 4, and so does its drop.
    pub compressor_pools_owner: CompressorPoolsOwner,
    /// The budgets the extended families read.
    pub retained_budgets: RetainedBudgets,
}

impl GrpcService {
    /// Binds the listeners, spawns the owner loops, the bank thread and one server task per listener,
    /// and hands back the senders the plugin feeds. Fails on a listener, TLS or thread setup error.
    /// Panics on a zero broadcast capacity or an unreadable legacy TLS file.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub async fn create<St>(
        config: ConfigGrpc,
        is_reload: bool,
        service_cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
        file_watcher: &FileWatcher,
        plugin_threads: &PluginThreads,
        build_compression: BuildCompression,
        delivery_runtime: tokio::runtime::Handle,
        messages_rx: St,
    ) -> anyhow::Result<GrpcServiceResult>
    where
        St: BatchStream<Item = Message> + Unpin + Send + 'static,
    {
        // Threads start before listeners bind, so a bind or TLS failure stops and joins them too.
        let (blocks_meta, blocks_meta_tx) = if config.unary_disabled {
            (None, None)
        } else {
            let (blocks_meta, blocks_meta_tx) = BlockMetaStorage::new(
                config.unary_concurrency_limit,
                service_cancellation_token.child_token(),
                task_tracker.clone(),
                plugin_threads,
            )
            .context("failed to spawn the block meta thread")?;
            (Some(blocks_meta), Some(blocks_meta_tx))
        };
        let (bank_thread, bank_thread_handle) = BankThread::spawn_bank_thread(plugin_threads)
            .context("failed to spawn the bank thread")?;
        let filter_compile_lane = FilterCompileLane::start(
            plugin_threads,
            config.data_plane.threads.filter_compile_threads,
        )
        .context("failed to spawn the filter compile lane")?;

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
                                load_server_config_from_tls_config(&tls, file_watcher)
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

        let capacities = config.resolved_capacities();
        let index_work_limits = IndexWorkLimits {
            slice_interval: config.route_index_interval,
            slice_budget: config.route_index_budget,
            keys_per_step: INDEX_KEYS_PER_STEP,
        };
        let (broadcast, [geyser_loop_changes_rx, reconstruction_changes_rx]) =
            SubscriberChannels::new(
                capacities.processed_broadcast,
                capacities.confirmed_broadcast,
                capacities.finalized_broadcast,
            )
            .with_indexing(index_work_limits);
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

        let max_decoding_message_size = config.max_decoding_message_size;
        let compression_config = config.data_plane.compression;
        let compressor_pools_owner =
            CompressorPoolsOwner::new(CompressorPools::new(&compression_config));
        let stream_compressor_for_pools = Arc::clone(&build_compression.stream_compressor);
        let prefer_zstd = build_compression
            .build_compression_defaults
            .with_overrides(config.data_plane.compression.compression_overrides)
            .prefer_zstd;
        let subscribe_call_settings = SubscribeCallSettings {
            accept_encodings: EnabledEncodings::from_tonic_encodings(&config.compression.accept),
            send_encodings: EnabledEncodings::from_tonic_encodings(&config.compression.send),
            prefer_zstd,
            max_decoding_message_size: Some(max_decoding_message_size),
            // The same limits as `geyser_server` below, which sets no encoding limit.
            max_encoding_message_size: None,
            subscribe_body: config.data_plane.subscribe_body,
            copy_threshold_bytes: config.data_plane.staging.copy_threshold_bytes,
        };
        let group_sharing =
            (compression_config.mode == CompressionSharingMode::Shared).then(|| {
                GroupSharingSettings {
                    group_settings: Arc::new(GroupCompressionSettings::from_config(
                        &compression_config,
                    )),
                    compressor_pools: compressor_pools_owner.compressor_pools().clone(),
                }
            });
        let block_shape_settings = BlockShapeWriteSettings {
            shape_budget: Arc::new(BlockShapeBudget::new(
                compression_config.block_shape_budget_bytes,
            )),
            waiter_backstop: compression_config.waiter_backstop,
        };
        let retained_budgets = RetainedBudgets {
            compression_budget: group_sharing.as_ref().map(|group_sharing_settings| {
                Arc::clone(&group_sharing_settings.group_settings.retained_budget)
            }),
            block_shape_budget: Arc::clone(&block_shape_settings.shape_budget),
        };
        let grpc_service = Arc::new(Self {
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
            filter_apply_interval: config.filter_apply_interval,
            filter_compile_lane,
            delivery_runtime,
        });
        let mut geyser_server = GeyserServer::from_arc(Arc::clone(&grpc_service))
            .max_decoding_message_size(max_decoding_message_size);
        for encoding in config.compression.accept {
            geyser_server = geyser_server.accept_compressed(encoding);
        }
        for encoding in config.compression.send {
            geyser_server = geyser_server.send_compressed(encoding);
        }
        let service = GeyserServiceSplitter::new(
            SubscribeService::new(
                grpc_service,
                subscribe_call_settings,
                build_compression.stream_compressor,
                group_sharing,
                block_shape_settings,
            ),
            geyser_server,
        );

        let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
        let (relay_tx, relay_rx) = mpsc::unbounded_channel();

        // Warn if replay buffer is too small for auto-reconnect
        if config.replay_stored_slots < 150 {
            log::warn!(
                "replay_stored_slots={} may be too low for auto-reconnect; recommend >= 150",
                config.replay_stored_slots
            );
        }

        {
            let broadcast = broadcast.clone();
            let owner_end_guard =
                OwnerEndGuard::new("block reconstruction loop", &service_cancellation_token);

            task_tracker.spawn(async move {
                let _owner_end_guard = owner_end_guard;
                Self::block_reconstruction_loop(
                    BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                    broadcast,
                    reconstruction_changes_rx,
                    relay_tx,
                    replay_stored_slots_rx,
                    replay_first_available_slot,
                    config.replay_stored_slots,
                    bank_thread,
                )
                .await;
            });
        }

        {
            let broadcast = broadcast.clone();
            let owner_end_guard = OwnerEndGuard::new("geyser loop", &service_cancellation_token);

            task_tracker.spawn(async move {
                let _owner_end_guard = owner_end_guard;
                Self::geyser_loop(
                    messages_rx,
                    broadcast,
                    geyser_loop_changes_rx,
                    relay_rx,
                    block_reconstruction_tx,
                )
                .await;
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

        // Last, so no later failure leaves pool threads parked.
        compressor_pools_owner
            .compressor_pools()
            .start_workers(plugin_threads, &stream_compressor_for_pools)
            .context("failed to start the compressor pools")?;

        Ok(GrpcServiceResult {
            snapshot_tx,
            deshred_broadcast_tx,
            contact_info_tx,
            blocks_meta_tx,
            bank_thread_handle,
            compressor_pools_owner,
            retained_budgets,
        })
    }

    /// Sends the plugin's raw batches at processed, and hands every message to block
    /// reconstruction.
    ///
    /// It is the only sender at processed, through its [`ProcessedOwner`]. Block reconstruction
    /// relays to it ([`RelayedItem`]) the batches it builds for processed, and each replay behind
    /// the batches relayed before it. Until the plugin's next batch is ready,
    /// [`ProcessedOwner::next_plugin_batch`] sends those, takes in index changes and runs index
    /// slices. It sends each raw batch, then hands it over as a
    /// [`BlockReconstructionMessage::BroadcastBatch`], with no `await` in between. The sections
    /// below follow a message through both tasks.
    ///
    /// # Slot status semantics
    ///
    /// Slot messages come in two categories with different routing rules:
    ///
    /// **Lifecycle statuses** ([`FirstShredReceived`](SlotStatus::FirstShredReceived),
    /// [`Completed`](SlotStatus::Completed), [`CreatedBank`](SlotStatus::CreatedBank),
    /// [`Dead`](SlotStatus::Dead)):
    /// Sent immediately at Processed, with the rest of the raw data. `FirstShredReceived`,
    /// `Completed` and `Dead` name no bank: block reconstruction also sends them to Confirmed
    /// and Finalized as they land. A bank's `CreatedBank` goes out at Confirmed and Finalized
    /// with the bank's content, when the bank reaches that level, and never as it lands there.
    ///
    /// **Commitment statuses** ([`Processed`](SlotStatus::Processed),
    /// [`Confirmed`](SlotStatus::Confirmed), [`Finalized`](SlotStatus::Finalized)):
    /// Never sent directly when received from the plugin. They are fed into
    /// [`BlockMachineStorage`] which holds them until the block is fully assembled. On each
    /// channel, a bank's statuses go out only after that channel's copy of the bank's content,
    /// block and block meta, in the order Processed, Confirmed, Finalized. No subscriber sees a
    /// commitment status before the content it is about.
    ///
    /// # Block content delivery
    ///
    /// When a block becomes ready at commitment level C ([`BlockMachineStorage::pop_ready_block`]):
    /// 1. If C is Confirmed or Finalized: the bank's `CreatedBank`, then its
    ///    [`FrozenBank::messages`](crate::block_reconstruction_v2::FrozenBank::messages)
    ///    (the complete, deduplicated set of Account/Transaction/Entry messages for that slot) are
    ///    sent at level C first. This is omitted at Processed because those messages were already
    ///    delivered individually as they arrived.
    /// 2. [`Message::Block`], then the Alpenglow [`Message::BlockFooter`] if the bank has one,
    ///    then [`Message::BlockMeta`] are sent at level C.
    /// 3. The status for C goes to the lower levels, which already have the content. Level C
    ///    gets every status up to C, in order: Processed, then Confirmed, then Finalized. Higher
    ///    levels get them when the bank reaches them.
    ///
    /// This ordering guarantee — block content before [`Message::Block`] before the footer
    /// before [`Message::BlockMeta`] before slot status — must be preserved so that subscribers
    /// always observe a complete block before the commitment signal.
    ///
    /// # Account deduplication
    ///
    /// Within a slot, if multiple updates arrive for the same account pubkey, only the update with
    /// the highest `write_version` is retained in the frozen block. This is handled internally by
    /// [`BlockMachineStorage`] / `BankBuffer` and must not be bypassed.
    ///
    /// # Missing commitment level gap-filling
    ///
    /// If a higher commitment level arrives without a prior lower one (e.g. Finalized before
    /// Confirmed), [`BlockMachineStorage`] synthesizes the missing levels in order
    /// (Processed → Confirmed → Finalized). Each synthesized level causes a separate
    /// [`BlockMachineStorage::pop_ready_block`] entry and a separate fan-out.
    ///
    /// # Ancestor slot propagation
    ///
    /// When a descendant slot reaches a commitment level, [`BlockMachineStorage`] retroactively
    /// raises every ancestor slot (walked via its own tracked parent-slot chain) to at least
    /// that same commitment level, emitting a synthesized update for any ancestor that hadn't
    /// already reached it directly. It must not be short-circuited.
    ///
    /// # Batching
    ///
    /// A raw batch holds up to 1024 messages. It goes out in the buffer that collected it, and a
    /// new buffer collects the next batch. A block meta, block footer or commitment status ends
    /// the batch. It goes to block reconstruction only, after the batch, as a
    /// [`BlockReconstructionMessage::SealGated`].
    ///
    /// # Arguments
    ///
    /// * `messages_rx` - The plugin's messages.
    /// * `broadcast` - The broadcast channels its [`OwnerIndexes`] broadcast on.
    /// * `index_changes_rx` - The index changes of processed clients, and every client's last
    ///   leave.
    /// * `relay_rx` - What block reconstruction relays: its processed batches and its replays.
    /// * `block_reconstruction_tx` - Hands each raw batch and each seal-gated message to block
    ///   reconstruction.
    async fn geyser_loop<St>(
        mut messages_rx: St,
        broadcast: SubscriberChannels,
        index_changes_rx: mpsc::UnboundedReceiver<IndexChange>,
        relay_rx: mpsc::UnboundedReceiver<RelayedItem>,
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
        let mut processed_owner = ProcessedOwner::new(broadcast, index_changes_rx, relay_rx);
        loop {
            let batch_size_maybe = processed_owner
                .next_plugin_batch(&mut messages_rx, &mut buffer)
                .await;
            let Some(_) = batch_size_maybe else {
                info!("Geyser loop: messages channel closed");
                break;
            };

            if !buffer.message_batch.is_empty() {
                metrics::message_queue_size_dec_by(buffer.message_batch.len() as u64);

                let message_batch_arc = Arc::new(buffer.message_batch);
                processed_owner.send_batch(Arc::clone(&message_batch_arc));

                if block_reconstruction_tx
                    .send(BlockReconstructionMessage::BroadcastBatch(
                        message_batch_arc,
                    ))
                    .is_ok()
                {
                    metrics::block_reconstruction_queue_size_inc();
                }

                buffer.message_batch = Vec::with_capacity(MESSAGE_BATCH_SIZE);
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

    /// Rebuilds banks from what geyser_loop hands over, sends each bank at every level it reaches and
    /// builds the replays. AGENTS.md has its ordering rules: the relay, the LIFO slot, the replay read.
    #[allow(clippy::too_many_arguments)]
    async fn block_reconstruction_loop<St>(
        mut messages_rx: St,
        broadcast: SubscriberChannels,
        mut index_changes_rx: mpsc::UnboundedReceiver<IndexChange>,
        relay_tx: mpsc::UnboundedSender<RelayedItem>,
        replay_stored_slots_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        replay_first_available_slot: Option<Arc<AtomicU64>>,
        replay_stored_slots: u64,
        bank_thread: BankThread,
    ) where
        St: BatchStream<Item = BlockReconstructionMessage> + Unpin + Send + 'static,
    {
        let (_tx, rx) = mpsc::channel(1);
        let mut replay_stored_slots_rx = replay_stored_slots_rx.unwrap_or(rx);

        let mut block_machine = BlockMachineStorage::new(replay_stored_slots as usize);
        let mut position = BroadcastPosition::default();

        const BUFFERED_MESSAGES_CAPACITY: usize = 1024;
        let mut buffered_messages = Vec::with_capacity(BUFFERED_MESSAGES_CAPACITY);
        let mut owner_indexes = OwnerIndexes::new(broadcast);
        let index_slice_timer = sleep_until(owner_indexes.next_index_slice_at());
        tokio::pin!(index_slice_timer);
        // A replay request taken from its channel, built once the queue reads empty again.
        let mut pending_replay: Option<ReplayStoredSlotsRequest> = None;

        loop {
            owner_indexes.take_queued_work(&mut index_changes_rx, index_slice_timer.as_mut());
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

                    metrics::block_reconstruction_queue_size_dec_by(buffered_size as u64);
                    let relayed_at_pass_end = Self::take_in(&mut block_machine, &mut owner_indexes, &relay_tx, &bank_thread, &mut position, buffered_messages.drain(..)).await;

                    let min_replayable_slot = block_machine.min_replayable_slot();
                    if let (Some(min_slot), Some(replay_first_available_slot)) = (min_replayable_slot, replay_first_available_slot.as_ref()) {
                        replay_first_available_slot.store(min_slot, Ordering::Relaxed);
                    }
                    if relayed_at_pass_end {
                        let_woken_task_run_first().await;
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
                        let bank_key_index = replayed_slot.frozen_block.bank_key_index();
                        replayed_messages.push(ReplayResponseMessageType::SealedBank(Arc::clone(&bank_messages), 0..created_bank_index, bank_key_index.clone()));
                        replayed_messages.push(ReplayResponseMessageType::Single(Message::Slot(replayed_slot.frozen_block.created_bank())));
                        replayed_messages.push(ReplayResponseMessageType::SealedBank(Arc::clone(&bank_messages), created_bank_index..bank_messages.len(), bank_key_index));

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
                            let slot_message = Message::Slot(Arc::new(MessageSlot::new(
                                slot_update.slot,
                                slot_update.parent_slot,
                                match slot_update.commitment {
                                    solana_commitment_config::CommitmentLevel::Processed => SlotStatus::Processed,
                                    solana_commitment_config::CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                                    solana_commitment_config::CommitmentLevel::Finalized => SlotStatus::Finalized,
                                },
                                None,
                                Some(slot_update.bank_id),
                                created_at,
                            )));
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

                    let _ = relay_tx.send(RelayedItem::Answer(tx, ReplayedResponse::Messages(Replay {
                        messages: replayed_messages,
                        bank_id_span,
                        cut: position.cut(commitment),
                    })));
                    let_woken_task_run_first().await;
                }
                Some(index_change) = index_changes_rx.recv() => {
                    owner_indexes.take_index_change(index_change);
                }
                Some(request) = replay_stored_slots_rx.recv(), if pending_replay.is_none() => {
                    pending_replay = Some(request);
                }
                // A Sleep reads Pending when the budget is spent, so the queue rule above holds.
                () = &mut index_slice_timer, if owner_indexes.has_index_work() => {
                    owner_indexes.run_index_slice_and_reset_timer(index_slice_timer.as_mut());
                }
                else => {
                    // No new messages and replay request channel closed, can only happen on shutdown
                    info!("Block reconstruction loop: replay_stored_slots channel closed");
                    break;
                }
            }
        }
    }

    /// Takes `messages` into `block_machine` and sends the banks they complete, at each level
    /// they reach.
    ///
    /// The lifecycle statuses of a raw batch, except [`CreatedBank`](SlotStatus::CreatedBank), go
    /// to confirmed and finalized as they land. Every batch waits for the end of the pass. Then
    /// the confirmed and finalized ones, in [`BroadcastPosition::held_batches`], go out through
    /// `owner_indexes` in order. The processed ones, in [`BroadcastPosition::held_relays`], go on
    /// the relay to [`GrpcService::geyser_loop`] last: the relay wakes geyser_loop into this
    /// worker's LIFO slot, where it waits for whatever the pass does after it. A pass that seals
    /// a bank at processed relays them first instead, and lets geyser_loop run with
    /// [`let_woken_task_run_first`] before its confirmed and finalized sends, so a block meta at
    /// processed never waits for their delivery. The [`BankThread`] frees the banks of the slots
    /// evicted in the pass, since freeing a whole bank takes milliseconds, and builds the key
    /// indexes of the banks sealed in it.
    ///
    /// # Arguments
    ///
    /// * `block_machine` - The banks being reconstructed.
    /// * `owner_indexes` - Sends the confirmed and finalized batches.
    /// * `relay_tx` - The relay to geyser_loop.
    /// * `bank_thread` - Takes the banks of the slots evicted in the pass, and the banks sealed
    ///   in it.
    /// * `position` - Updated with what was taken in and sent.
    /// * `messages` - What geyser_loop handed over.
    ///
    /// # Returns
    ///
    /// `true` if the pass relayed at its end. geyser_loop then waits in this worker's LIFO slot
    /// until the caller lets it run with [`let_woken_task_run_first`].
    async fn take_in(
        block_machine: &mut BlockMachineStorage,
        owner_indexes: &mut OwnerIndexes,
        relay_tx: &mpsc::UnboundedSender<RelayedItem>,
        bank_thread: &BankThread,
        position: &mut BroadcastPosition,
        messages: impl Iterator<Item = BlockReconstructionMessage>,
    ) -> bool {
        const ALL_COMMITMENT_LEVELS: [CommitmentLevel; 3] = [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ];

        for messages in messages {
            match messages {
                BlockReconstructionMessage::BroadcastBatch(messages) => {
                    let mut lifecycle_statuses = Vec::new();
                    for message in messages.iter() {
                        if let Message::Slot(slot_message) = message {
                            metrics::update_slot_plugin_status(
                                slot_message.status,
                                slot_message.slot,
                            );
                            if slot_message.status != SlotStatus::CreatedBank {
                                lifecycle_statuses.push(message.clone());
                            }
                        }

                        block_machine.add(message.clone());
                    }
                    if !lifecycle_statuses.is_empty() {
                        let lifecycle_statuses = Arc::new(lifecycle_statuses);
                        position.send(CommitmentLevel::Confirmed, Arc::clone(&lifecycle_statuses));
                        position.send(CommitmentLevel::Finalized, lifecycle_statuses);
                    }
                    for waiting in position.waiting_next_taken_in.drain(..) {
                        let _ = waiting.send(Arc::clone(&messages));
                    }
                    // A replay cut: the last batch geyser_loop broadcast before handing it over.
                    position.last_taken_in = Some(messages);
                }
                BlockReconstructionMessage::SealGated(message) => {
                    // Never a cut: it was not broadcast, so it reaches no live receiver. What
                    // block reconstruction makes of it, it sends itself, under `last_sent`.
                    if let Message::Slot(slot_message) = &message {
                        metrics::update_slot_plugin_status(slot_message.status, slot_message.slot);
                    }

                    block_machine.add(message);
                }
            }
        }

        let mut sealed_at_processed = false;
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
                    commitment_level,
                    Arc::new(vec![Message::Slot(frozen_block.created_bank())]),
                );
                position.send(commitment_level, frozen_block.messages());
            } else {
                sealed_at_processed = true;
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
            position.send(commitment_level, Arc::new(block_messages));

            // A status goes out on a channel only after the bank's content went out on it. The
            // lower channels have it already: they get just this status. This channel gets it
            // now: it gets every status up to this one, in order. The higher channels get them
            // when the bank reaches them.
            let created_at = Timestamp::from(SystemTime::now());
            let status = |status| {
                Message::Slot(Arc::new(MessageSlot::new(
                    slot_update.slot,
                    slot_update.parent_slot,
                    status,
                    None,
                    Some(slot_update.bank_id),
                    created_at,
                )))
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
                position.send(*channel, Arc::clone(&this_status));
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
            position.send(commitment_level, statuses_so_far);
        }
        if sealed_at_processed && !position.held_batches.is_empty() {
            position.relay_held_batches(relay_tx);
            let_woken_task_run_first().await;
        }
        for (commitment_level, held_batch) in position.held_batches.drain(..) {
            owner_indexes.send_batch(commitment_level, held_batch);
        }
        bank_thread.hand_over_banks(block_machine);

        if let Some(min_slot) = block_machine.min_replayable_slot() {
            position
                .sent_at_processed
                .retain(|_, slot| *slot >= min_slot);
        }
        position.relay_held_batches(relay_tx)
    }

    /// Reads one Subscribe client's requests: answers pings at once, and posts at most one filter
    /// change per `filter_apply_interval`, once the live stream starts. AGENTS.md has the details.
    #[allow(clippy::too_many_arguments)]
    async fn read_requests(
        mut subscribe_requests: impl Stream<Item = TonicResult<SubscribeRequest>> + Unpin,
        session_id: usize,
        subscriber_id: String,
        config_filter_limits: Arc<FilterLimits>,
        mut filter_names: FilterNames,
        filter_compile_lane: Option<FilterCompileLane>,
        filter_apply_interval: Duration,
        stream_tx: SubscribeSender,
        client_tx: mpsc::UnboundedSender<ClientRequest<(Option<u64>, Filter)>>,
        mut client_live_rx: oneshot::Receiver<()>,
        cancellation_token: CancellationToken,
    ) {
        let mut forwarded_first = false;
        let mut client_live = false;
        let mut waiting_request: Option<SubscribeRequest> = None;
        let mut next_change_at = Instant::now();
        let mut reading_requests = true;
        loop {
            let mut subscribe_request = tokio::select! {
                biased;

                () = cancellation_token.cancelled() => {
                    info!("client #{subscriber_id:?}/{session_id}: filter receiver cancelled");
                    break;
                }
                _ = &mut client_live_rx, if !client_live => {
                    client_live = true;
                    continue;
                }
                request_read = subscribe_requests.next(), if reading_requests => match request_read {
                    Some(Ok(subscribe_request)) => {
                        if forwarded_first && subscribe_request.ping.is_none() {
                            if !client_live || Instant::now() < next_change_at {
                                waiting_request = Some(subscribe_request);
                                continue;
                            }
                            waiting_request = None;
                        }
                        subscribe_request
                    }
                    None => {
                        // Client half-closed its send stream. Stop reading, but keep client_tx
                        // alive so client_loop continues running.
                        info!("client #{subscriber_id:?}/{session_id}: client closed send stream, waiting for cancellation");
                        reading_requests = false;
                        continue;
                    }
                    Some(Err(error)) => {
                        let disconnect = Disconnect::from_read_error(&error, "SubscribeRequest");
                        if let Some(status) = &disconnect.status {
                            info!("client #{subscriber_id:?}/{session_id}: {}", status.message());
                        }
                        let _ = client_tx.send(Err(disconnect));
                        break;
                    }
                },
                () = sleep_until(next_change_at), if client_live && waiting_request.is_some() => {
                    let Some(subscribe_request) = waiting_request.take() else {
                        continue;
                    };
                    subscribe_request
                }
            };
            filter_names.try_clean();
            // A ping alone has nothing to compile, so its pong never waits behind the lane.
            let asks_only_for_a_pong =
                subscribe_request.ping.is_some() && asks_for_nothing(&subscribe_request);
            let compile_lane = filter_compile_lane
                .as_ref()
                .filter(|_| !asks_only_for_a_pong);
            let compile_result = match compile_lane {
                Some(filter_compile_lane) => {
                    let lane_compile = filter_compile_lane.compile(
                        subscribe_request,
                        &config_filter_limits,
                        filter_names,
                    );
                    let compiled_filter = tokio::select! {
                        biased;

                        () = cancellation_token.cancelled() => break,
                        compiled_filter = lane_compile => compiled_filter,
                    };
                    let Some(compiled_filter) = compiled_filter else {
                        error!("client #{subscriber_id:?}/{session_id}: the filter compile lane stopped");
                        break;
                    };
                    subscribe_request = compiled_filter.subscribe_request;
                    filter_names = compiled_filter.filter_names;
                    compiled_filter
                        .compile_result
                        .unwrap_or_else(|compile_panic| std::panic::resume_unwind(compile_panic))
                }
                None => Filter::new(&subscribe_request, &config_filter_limits, &mut filter_names),
            };

            if let Err(error) = match compile_result {
                Ok(filter) => {
                    if let Some(msg) = filter.get_pong_msg() {
                        if stream_tx
                            .send(Ok(SubscribeQueueEntry::Filtered(msg)))
                            .await
                            .is_err()
                        {
                            error!("client #{subscriber_id:?}/{session_id}: stream closed");
                            let _ = client_tx.send(Err(Disconnect::quiet("client_disconnect")));
                            break;
                        }
                        continue;
                    }
                    // An SDK's subscribe() sends an empty request before the user's own, and
                    // only the first request that asks for something can replay.
                    if !forwarded_first && asks_for_nothing(&subscribe_request) {
                        continue;
                    }
                    let complexity_score = filter.get_filter_stats();
                    metrics::observe_filter_complexity(&subscriber_id, &complexity_score);
                    let filter_post = Ok((subscribe_request.from_slot, filter));
                    let post_result = match client_tx.send(filter_post) {
                        Ok(()) => Ok(()),
                        Err(error) => Err(error.to_string()),
                    };
                    forwarded_first = true;
                    next_change_at = Instant::now() + filter_apply_interval;
                    post_result
                }
                Err(error) => Err(error.to_string()),
            } {
                let err = Err(Status::invalid_argument(format!(
                    "failed to create filter: {error}"
                )));
                if stream_tx.send(err).await.is_err() {
                    let _ = client_tx.send(Err(Disconnect::quiet("client_disconnect")));
                }
            }
        }
    }

    /// Serves one Subscribe client: its snapshot or replay, then its live updates, indexed or from the
    /// broadcast. [`ClientIndexing`] joins and leaves the index, as AGENTS.md describes.
    #[allow(clippy::too_many_arguments)]
    async fn client_loop(
        mut session: ClientSession,
        stream_tx: SubscribeSender,
        mut client_rx: mpsc::UnboundedReceiver<ClientRequest<(Option<u64>, Filter)>>,
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
            Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed(disconnect)) => {
                session.disconnect_reason = disconnect.end(stream_tx, &task_tracker);
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
                Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed(disconnect)) => {
                    info!("client #{}: grpc connection closed", session.subscriber_id);
                    session.disconnect_reason = disconnect.end(stream_tx, &task_tracker);
                    return;
                }
            }
        } else {
            info!("client #{}: no snapshot requested", session.subscriber_id);
        }

        let mut commitment = session.filter.get_commitment_level();
        let index_keys = broadcast.index_keys(&session.filter, None);
        let mut messages_rx = match from_slot {
            // A snapshot subscription does not replay.
            Some(from_slot) if !snapshot_requested => {
                let replay = Self::client_loop_replay(
                    &mut session,
                    &stream_tx,
                    &broadcast,
                    replay_stored_slots_tx.as_ref(),
                    commitment,
                    index_keys.as_deref(),
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
                Some(messages_rx)
            }
            _ => Some(broadcast.subscribe(commitment)),
        };
        session.client_live_tx = None;
        let mut client_indexing = ClientIndexing::new(&session, &stream_tx, index_keys, &broadcast);
        let mut match_scratch = MatchScratch::default();
        let stream_counters = Arc::clone(session.stream_counters());

        'outer: loop {
            stream_counters.observe_queue_size(stream_tx.current_weight() as u64);
            client_indexing.send_join_step(&session, &mut messages_rx, &client_rx, commitment);

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
                                // Keep a disconnect already received: it carries the reason.
                                if !matches!(message, Some(Err(_))) {
                                    message = None;
                                }
                                break;
                            }
                        }
                    }

                    match message.unwrap_or_else(|| Err(Disconnect::quiet("client_disconnect"))) {
                        Ok((from_slot, filter_new)) => {
                            session.set_filter(filter_new);
                            info!("client #{}: filter updated", session.subscriber_id);
                            if from_slot.is_some() {
                                info!("client #{}: from_slot ignored, only the first request can replay", session.subscriber_id);
                            }

                            let commitment_new = session.filter.get_commitment_level();
                            client_indexing
                                .change_filter(&session, &mut messages_rx, commitment, commitment_new)
                                .await;
                            if commitment_new != commitment {
                                commitment = commitment_new;
                                messages_rx = Some(broadcast.subscribe(commitment));
                            }
                        }
                        Err(disconnect) => {
                            session.disconnect_reason = disconnect.end(stream_tx, &task_tracker);
                            break 'outer;
                        }
                    }
                }
                Some(ready_answer) = OptionFuture::from(client_indexing.ready_rx()) => {
                    client_indexing.take_ready_answer(ready_answer);
                }
                Some(message) = OptionFuture::from(
                    messages_rx.as_mut().map(broadcast::Receiver::recv),
                ) => {
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
                        let send_result = queue_unrouted_updates(
                            &stream_tx,
                            &session.filter,
                            message,
                            Some(commitment),
                            &mut match_scratch,
                            &stream_counters,
                        );
                        match send_result {
                            Ok(()) => {}
                            Err(LiveSendFailure::Full) => {
                                error!("client #{}: lagged to send an update", session.subscriber_id);
                                task_tracker.spawn(async move {
                                    let _ = stream_tx.send(Err(Status::internal("lagged to send an update"))).await;
                                });
                                session.disconnect_reason = "client_channel_full";
                                break 'outer;
                            }
                            Err(LiveSendFailure::Closed) => {
                                error!("client #{}: stream closed", session.subscriber_id);
                                session.disconnect_reason = "client_closed";
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }
        client_indexing.end_session(session, messages_rx.is_none(), commitment);
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
    /// [`ClientSnapshotReplayError::ClientGrpcConnectionClosed`] with the [`Disconnect`] if the
    /// client's request stream ends first.
    async fn client_loop_first_request(
        session: &mut ClientSession,
        client_rx: &mut mpsc::UnboundedReceiver<ClientRequest<(Option<u64>, Filter)>>,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<u64>, ClientSnapshotReplayError> {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("client #{}: cancelled", session.subscriber_id);
                Err(ClientSnapshotReplayError::Cancelled)
            }
            maybe = client_rx.recv() => match maybe {
                Some(Ok((from_slot, filter))) => {
                    session.set_filter(filter);
                    info!("client #{}: filter updated", session.subscriber_id);
                    Ok(from_slot)
                }
                Some(Err(disconnect)) => Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed(disconnect)),
                None => Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed(Disconnect::quiet(
                    "client_disconnect",
                ))),
            }
        }
    }

    /// How many replayed messages that give a client with index keys no update spend one unit of
    /// tokio's cooperative budget in [`GrpcService::client_loop_replay`]. A poll of 128 units
    /// then scans at most 16,384 such messages.
    #[allow(rustdoc::private_intra_doc_links)]
    pub const REPLAYED_MESSAGES_WITHOUT_UPDATE_PER_BUDGET_UNIT: u64 = 128;

    /// Sends the replay from `from_slot` and the live backlog that follows it, filtered, with
    /// backpressure, and returns the live receiver positioned after them.
    /// On failure, records why on `session` and tells the client.
    ///
    /// A send that finds room in `stream_tx` does not yield. So the loop spends one unit of
    /// tokio's cooperative budget with [`consume_budget`](tokio::task::coop::consume_budget) for
    /// each message it takes from the [`ReplayPhase`] that gives the client an update, and one for
    /// every [`GrpcService::REPLAYED_MESSAGES_WITHOUT_UPDATE_PER_BUDGET_UNIT`] messages that give
    /// it none. A client without index keys spends one for every message it takes, since its
    /// replay visits every stored message. With tokio's budget of 128 units per poll, a replay to
    /// a client that keeps up yields after at most 128 updates or 16,384 messages without one,
    /// always between two messages, and the runtime runs other tasks meanwhile, such as
    /// [`GrpcService::geyser_loop`] and [`GrpcService::block_reconstruction_loop`]. So a scan of
    /// banks with no bank key index yields too, while a replay that sends few updates over the
    /// open banks and a short backlog still ends in one poll.
    ///
    /// # Arguments
    ///
    /// * `session` - The client session: its filter shapes the replay.
    /// * `stream_tx` - The client's update queue.
    /// * `broadcast` - The commitment channels.
    /// * `replay_stored_slots_tx` - Requests to block reconstruction, or [`None`] when replay
    ///   is off.
    /// * `commitment` - The commitment of the replay and of the live stream.
    /// * `index_keys` - The client's index keys, or [`None`]. With them, each sealed bank with a
    ///   [`BankKeyIndex`] hands out only the messages they find.
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
        index_keys: Option<&[IndexKey]>,
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
        let mut phase = match ReplayPhase::new(messages_rx, commitment, replay, index_keys) {
            Ok(phase) => phase,
            Err(error) => {
                broadcast_failed(session, error);
                return None;
            }
        };

        // The replay, the missed batch and the backlog, all sent with backpressure.
        let stream_sender = StreamUpdateSender::new(stream_tx);
        let mut match_scratch = MatchScratch::default();
        let mut replayed_messages_without_update: u64 = 0;
        loop {
            let message = match phase.next().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(error) => {
                    broadcast_failed(session, error);
                    return None;
                }
            };
            let member_entries = MemberQueueEntries::of_message(
                &session.filter,
                &message,
                Some(commitment),
                &mut match_scratch,
                stream_sender.weight_scales(),
            );
            let spends_budget_unit = if index_keys.is_none() || member_entries.len() > 0 {
                true
            } else {
                replayed_messages_without_update += 1;
                replayed_messages_without_update
                    .is_multiple_of(Self::REPLAYED_MESSAGES_WITHOUT_UPDATE_PER_BUDGET_UNIT)
            };
            for queue_entry in member_entries {
                let send_result = stream_sender
                    .send_waiting(queue_entry, client_unresponsive_timeout)
                    .await;
                match send_result {
                    Ok(()) => {
                        session.stream_counters().add_sent_updates(1);
                    }
                    Err(WaitingSendError::Closed) => {
                        error!("client #{}: stream closed", session.subscriber_id);
                        session.disconnect_reason = "client_closed";
                        return None;
                    }
                    Err(WaitingSendError::TimedOut) => {
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
            if spends_budget_unit {
                tokio::task::coop::consume_budget().await;
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

        let stream_sender = StreamUpdateSender::new(&stream_tx);
        let mut match_scratch = MatchScratch::default();
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

            let member_entries = MemberQueueEntries::of_message(
                &session.filter,
                &message,
                None,
                &mut match_scratch,
                stream_sender.weight_scales(),
            );
            for queue_entry in member_entries {
                if stream_sender.send_waiting(queue_entry, None).await.is_err() {
                    error!("client #{}: stream closed", session.subscriber_id);
                    return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed(
                        Disconnect::quiet("client_closed"),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Opens a Subscribe stream: takes a permit for a known subscriber, then spawns the client's ping
    /// task (a ping and a queue size sample every 10 s), its request task and its `client_loop`.
    /// Fails when the subscriber is at its limit or the server is shutting down.
    pub async fn start_subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
        response_encoding: ResponseEncoding,
    ) -> TonicResult<Response<SubscribeResponseStream>> {
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

        let endpoint = request
            .metadata()
            .get("x-endpoint")
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "".to_owned());
        let mut client_session = ClientSession::new(
            id,
            subscriber_id.clone(),
            endpoint,
            client_cancellation_token.clone(),
            subscription_permit,
        );

        let ping_stream_tx = stream_tx.clone();
        let ping_cancellation_token = client_cancellation_token.clone();
        let ping_client_cancel = client_cancellation_token.clone();
        let ping_subscriber_id = subscriber_id.clone();
        let ping_stream_counters = Arc::clone(client_session.stream_counters());
        let subscribe_ping_task = run_subscribe_pings(
            id,
            ping_subscriber_id,
            ping_stream_tx,
            ping_stream_counters,
            ping_cancellation_token,
            ping_client_cancel,
        );
        self.task_tracker
            .spawn_on(subscribe_ping_task, &self.delivery_runtime);

        let config_filter_limits = Arc::clone(&self.config_filter_limits);
        let incoming_stream_tx = stream_tx.clone();
        let incoming_client_tx = client_tx;
        let incoming_cancellation_token = client_cancellation_token.child_token();

        let filter_names = FilterNames::new(
            self.filter_name_size_limit,
            self.filter_names_size_limit,
            self.filter_names_cleanup_interval,
        );

        let (client_live_tx, client_live_rx) = oneshot::channel();
        self.task_tracker.spawn_on(
            Self::read_requests(
                request.into_inner(),
                id,
                subscriber_id.clone().unwrap_or("unknown".to_string()),
                config_filter_limits,
                filter_names,
                self.filter_compile_lane.clone(),
                self.filter_apply_interval,
                incoming_stream_tx,
                incoming_client_tx,
                client_live_rx,
                incoming_cancellation_token,
            ),
            &self.delivery_runtime,
        );

        client_session.client_live_tx = Some(client_live_tx);
        client_session.counted_stream = Some(CountedSubscribeStream::start(response_encoding));
        self.task_tracker.spawn_on(
            Self::client_loop(
                client_session,
                stream_tx,
                client_rx,
                snapshot_rx,
                self.broadcast.clone(),
                self.replay_stored_slots_tx.clone(),
                self.task_tracker.clone(),
                self.client_unresponsive_timeout,
            ),
            &self.delivery_runtime,
        );

        Ok(Response::new(stream_rx))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_service<H>(
        health_service: HealthServer<H>,
        x_token: Option<AsciiMetadataValue>,
        service: GeyserServiceSplitter,
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
    pub async fn serve_listener<I, IO>(
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
        mut client_rx: mpsc::UnboundedReceiver<ClientRequest<DeshredFilter>>,
        mut messages_rx: broadcast::Receiver<DeshredBroadcastedMessage>,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) {
        let stream_counters = Arc::clone(session.stream_counters());
        'outer: loop {
            stream_counters.observe_queue_size(stream_tx.current_weight() as u64);

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
                                // Keep a disconnect already received: it carries the reason.
                                if !matches!(message, Some(Err(_))) {
                                    message = None;
                                }
                                break;
                            }
                        }
                    }

                    match message.unwrap_or_else(|| Err(Disconnect::quiet("client_disconnect"))) {
                        Ok(filter_new) => {
                            session.filter = filter_new;
                            info!("deshred client #{}/{}: filter updated", session.subscriber_id, session.id);
                        }
                        Err(disconnect) => {
                            session.disconnect_reason = disconnect.end(stream_tx, &task_tracker);
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

                    for update in session.filter.get_updates(&message, None) {
                        match stream_tx.try_send(Ok(update)) {
                            Ok(()) => {
                                stream_counters.add_sent_updates(1);
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
    type SubscribeStream = SubscribeResponseStream;
    type SubscribeDeshredStream = LoadAwareReceiver<TonicResult<FilteredUpdateDeshred>>;
    type SubscribeGossipStream = ReceiverStream<TonicResult<SubscribeUpdateGossip>>;

    /// Opens a Subscribe stream with [`GrpcService::start_subscribe`]; tonic's own path, which
    /// only tests serve, counts it as identity.
    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        self.start_subscribe(request, ResponseEncoding::Identity)
            .await
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
        self.task_tracker.spawn_on(
            async move {
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
            },
            &self.delivery_runtime,
        );

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
        let incoming_subscriber_id = subscriber_id.clone();
        let incoming_stream_tx = stream_tx.clone();
        let incoming_client_tx = client_tx;
        let incoming_cancellation_token = client_cancellation_token.child_token();

        let deshred_request_task = async move {
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
                                            let _ = incoming_client_tx.send(Err(Disconnect::quiet("client_disconnect")));
                                            break;
                                        }
                                        continue;
                                    }
                                    match incoming_client_tx.send(Ok(filter)) {
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
                                    let _ = incoming_client_tx.send(Err(Disconnect::quiet("client_disconnect")));
                                }
                            }
                        }
                        Ok(None) => {
                            info!("deshred client #{id}: client closed send stream, waiting for cancellation");
                            incoming_cancellation_token.cancelled().await;
                            break;
                        }
                        Err(error) => {
                            let disconnect = Disconnect::from_read_error(&error, "SubscribeDeshredRequest");
                            if let Some(status) = &disconnect.status {
                                info!("deshred client #{incoming_subscriber_id:?}/{id}: {}", status.message());
                            }
                            let _ = incoming_client_tx.send(Err(disconnect));
                            break;
                        }
                    }
                }
            }
        };
        self.task_tracker
            .spawn_on(deshred_request_task, &self.delivery_runtime);

        let session = DeshredClientSession::new(
            id,
            subscriber_id.clone(),
            client_cancellation_token.clone(),
            subscription_permit,
        );
        self.task_tracker.spawn_on(
            Self::deshred_client_loop(
                session,
                stream_tx,
                client_rx,
                self.deshred_broadcast_tx.subscribe(),
                client_cancellation_token,
                self.task_tracker.clone(),
            ),
            &self.delivery_runtime,
        );

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
            &self.delivery_runtime,
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
            body::StandardTransactionBodyWriter,
            plugin::{
                filter::{
                    fixtures,
                    limits::FilterLimits,
                    name::{FilterName, FilterNames},
                    Filter, FilterAccountsDataSlice,
                },
                message::MessageTransaction,
            },
            subscribe::queue_item_decoding::{DecodeQueueItem, SubscribeReceiver},
            util::stream::load_aware_channel,
        },
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::time::Instant,
        yellowstone_grpc_proto::prelude::{
            subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestAccountsDataSlice,
            SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots,
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

    /// An oversized first, later or compressed request gets `ResourceExhausted` with the limit,
    /// and counts as `request_too_large`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_returns_resource_exhausted_for_oversized_request() {
        use {
            crate::{file_watcher::FileWatcher, stream::tokio::BatchStreamUnboundedReceiver},
            std::collections::HashMap,
            yellowstone_grpc_proto::prelude::{
                geyser_client::GeyserClient, SubscribeRequestFilterAccounts,
            },
        };

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        // Rare race: if create() fails with AddrInUse, another process took this port first; rerun.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let config = serde_json::json!({
            "listen": [{ "address": format!("127.0.0.1:{port}") }],
            "max_decoding_message_size": 1024,
            "compression": { "accept": ["gzip"] },
        });
        let config: crate::config::ConfigGrpc = serde_json::from_str(&config.to_string()).unwrap();

        let cancellation_token = CancellationToken::new();
        let (_messages_tx, messages_rx) = mpsc::unbounded_channel();
        let _service = GrpcService::create(
            config,
            false,
            cancellation_token.clone(),
            TaskTracker::new(),
            &FileWatcher::new().unwrap(),
            &PluginThreads::default(),
            BuildCompression::standard(CompressionDefaults::PUBLIC, Default::default()).unwrap(),
            tokio::runtime::Handle::current(),
            BatchStreamUnboundedReceiver::new(messages_rx),
        )
        .await
        .unwrap();

        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let oversized = SubscribeRequest {
            accounts: HashMap::from([(
                "big".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: vec![Pubkey::new_unique().to_string(); 100],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let valid = SubscribeRequest {
            slots: HashMap::from([("slots".into(), SubscribeRequestFilterSlots::default())]),
            ..Default::default()
        };
        // A gzip request fits on the wire and goes over the limit while decompressing.
        let cases = [
            ("too-large-first", vec![oversized.clone()], false),
            (
                "too-large-after-valid",
                vec![valid, oversized.clone()],
                false,
            ),
            ("too-large-compressed", vec![oversized], true),
        ];

        for (subscriber_id, requests, gzip) in cases {
            let mut client = GeyserClient::new(channel.clone());
            if gzip {
                client = client.send_compressed(tonic::codec::CompressionEncoding::Gzip);
            }
            let requests = futures::StreamExt::chain(
                futures::stream::iter(requests),
                futures::stream::pending(),
            );
            let mut request = Request::new(requests);
            request
                .metadata_mut()
                .insert("x-subscription-id", subscriber_id.parse().unwrap());

            let mut response = client.subscribe(request).await.unwrap().into_inner();
            let status = loop {
                match response.message().await {
                    Ok(Some(_update)) => continue,
                    Ok(None) => panic!("{subscriber_id}: stream closed without a status"),
                    Err(status) => break status,
                }
            };

            assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
            assert!(
                status.message().contains("1024 bytes"),
                "{}",
                status.message()
            );
            // The session drops after the client sees the status, so wait for its counter.
            tokio::time::timeout(Duration::from_secs(2), async {
                while metrics::client_disconnect_count(subscriber_id, "request_too_large") == 0 {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{subscriber_id}: request_too_large not counted"));
        }
        cancellation_token.cancel();
    }

    /// Only a decoder size error, plain or compressed, is too large; transport errors are quiet.
    #[test]
    fn only_decoder_size_errors_are_too_large() {
        let compressed = Status::resource_exhausted("size limit, of 1024 bytes, exceeded");
        let disconnect = Disconnect::from_read_error(&compressed, "SubscribeRequest");
        assert_eq!(disconnect.reason, "request_too_large");
        let status = disconnect.status.unwrap();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert!(
            status.message().contains("1024 bytes"),
            "{}",
            status.message()
        );

        let mut eof = Status::out_of_range("unexpected end of file");
        eof.set_source(Arc::new(io::Error::from(io::ErrorKind::UnexpectedEof)));
        for error in [eof, Status::internal("boom")] {
            let disconnect = Disconnect::from_read_error(&error, "SubscribeRequest");
            assert_eq!(disconnect.reason, "client_disconnect");
            assert!(disconnect.status.is_none());
        }
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

    fn update(message: FilteredUpdateOneof) -> TonicResult<SubscribeQueueEntry> {
        filtered_queue_item(FilteredUpdate::new_empty(message))
    }

    fn filtered_queue_item(filtered_update: FilteredUpdate) -> TonicResult<SubscribeQueueEntry> {
        Ok(SubscribeQueueEntry::Filtered(filtered_update))
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
        assert_eq!(
            scales.weight(&update(FilteredUpdateOneof::slot(Arc::new(slot)))),
            7
        );
        let footer = fixtures::message_block_footer(1, 1);
        assert_eq!(
            scales.weight(&update(FilteredUpdateOneof::block_footer(footer, true))),
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
        assert_eq!(
            scales.weight(&filtered_queue_item(block_update.clone())),
            (1 + 5 + 4) * 2
        );
        assert_eq!(
            default_scales.weight(&filtered_queue_item(block_update)),
            1 + 1 + 1
        );

        let block_update = create_filter_with_blocks(false)
            .get_updates(&block, None)
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            scales.weight(&filtered_queue_item(block_update)),
            (1 + 5) * 2
        );
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
            fixtures::message_account(fixtures::account_write(
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
        assert_eq!(
            scales.weight(&filtered_queue_item(block_update.clone())),
            1 + 1 + 4 * 4
        );
        assert_eq!(
            count_only.weight(&filtered_queue_item(block_update)),
            1 + 1 + 4 * 2
        );

        let big = Pubkey::new_unique();
        let block = Message::Block(fixtures::message_block(
            1,
            vec![test_transaction(); 2],
            vec![
                account(100),
                fixtures::message_account(fixtures::account_write(
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
        assert_eq!(scales.weight(&filtered_queue_item(block_update)), 1 + 4 * 4);
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

        transaction.ensure_transaction_info(&StandardTransactionBodyWriter);
        let len = transaction
            .transaction_bodies()
            .built_info_len()
            .unwrap_or(0);
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
            scales.weight(&filtered_queue_item(block_update.clone())),
            1 + 4 * units(10 * len)
        );
        assert_eq!(
            count_only.weight(&filtered_queue_item(block_update)),
            1 + 4 * 10
        );
    }

    #[test]
    fn filter_names_add_weight() {
        let weight_scales = WeightScales {
            account: 4,
            ..WeightScales::default()
        };
        let names_unweighted = WeightScales {
            filter_name_data_unit: 0,
            ..weight_scales
        };
        let update_with_names = |name_count: usize, update_message: FilteredUpdateOneof| {
            let filter_names = vec![FilterName::new("a".repeat(128)); name_count];
            filtered_queue_item(FilteredUpdate::new(
                filter_names.into(),
                update_message,
                Timestamp::default(),
            ))
        };
        let account_with_names = |name_count: usize| {
            let account_message = FilteredUpdateOneof::account(
                fixtures::simple_message_account(Pubkey::default(), Pubkey::default()),
                FilterAccountsDataSlice::default(),
            );
            update_with_names(name_count, account_message)
        };

        // One unit per 1024 bytes of filter names, added after the update kind's scale.
        assert_eq!(weight_scales.weight(&account_with_names(7)), 4);
        assert_eq!(weight_scales.weight(&account_with_names(8)), 4 + 1);
        assert_eq!(weight_scales.weight(&account_with_names(100)), 4 + 12);
        let status_message = FilteredUpdateOneof::transaction_status(test_transaction());
        let status_with_names = update_with_names(16, status_message);
        assert_eq!(weight_scales.weight(&status_with_names), 1 + 2);
        assert_eq!(names_unweighted.weight(&account_with_names(100)), 4);
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
            .try_send(update(FilteredUpdateOneof::slot(Arc::new(
                fixtures::message_slot(1, SlotStatus::Processed),
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
            .send(Ok((Some(0), create_filter_with_blocks(false))))
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
            let replayed_update = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                .await
                .expect("replayed block")
                .unwrap()
                .unwrap()
                .decode_update();
            let is_block = matches!(replayed_update.update_oneof, Some(UpdateOneof::Block(_)));
            assert!(is_block);
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
                .send(Ok((*from_slot, create_filter_with_slots())))
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
            .send(Ok((
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
        Arc::new(vec![Message::Slot(Arc::new(MessageSlot::new(
            slot,
            Some(slot - 1),
            SlotStatus::Processed,
            None,
            Some(slot),
            Timestamp::from(SystemTime::now()),
        )))])
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    /// A bank thread for a test's block reconstruction. Its handle is forgotten, so the thread
    /// runs until the loop drops its sender and every queued chore is done.
    fn spawn_test_bank_thread() -> BankThread {
        let (bank_thread, bank_thread_handle) =
            BankThread::spawn_bank_thread(&PluginThreads::default()).unwrap();
        std::mem::forget(bank_thread_handle);
        bank_thread
    }

    async fn expect_none(stream_rx: &mut SubscribeReceiver) {
        assert!(
            tokio::time::timeout(Duration::from_millis(50), stream_rx.recv())
                .await
                .is_err(),
            "received an update from a commitment the client did not subscribe to"
        );
    }

    async fn expect_one(stream_rx: &mut SubscribeReceiver) {
        tokio::time::timeout(Duration::from_secs(2), stream_rx.recv())
            .await
            .expect("timed out waiting for an update")
            .expect("stream closed")
            .expect("status error")
            .decode_update();
    }

    type ClientHandles = (
        mpsc::UnboundedSender<ClientRequest<(Option<u64>, Filter)>>,
        SubscribeReceiver,
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
            .send(Ok((
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
            .send(Ok((
                None,
                create_filter_at(CommitmentLevelProto::Processed),
            )))
            .unwrap();
        settle().await;

        broadcast.send(CommitmentLevel::Processed, slot_batch(100));
        expect_one(&mut stream_rx).await;

        client_tx
            .send(Ok((
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
        client_tx: mpsc::UnboundedSender<ClientRequest<(Option<u64>, Filter)>>,
        filter: Filter,
        half_close: oneshot::Receiver<()>,
        ct: CancellationToken,
    ) {
        client_tx.send(Ok((None, filter))).unwrap();
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
        let msg = Message::Slot(Arc::new(MessageSlot::new(
            100,
            Some(99),
            SlotStatus::Processed,
            None,
            Some(100),
            Timestamp::from(SystemTime::now()),
        )));
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

    #[test]
    fn test_subscription_tracker_survives_colliding_subscriber_ids() {
        // Same FNV-1a-64 hash, so prometheus keys both ids to one gauge child.
        const COLLIDING_SUBSCRIBER_ID_A: &str = "sub-936f43f9a9acf07b";
        const COLLIDING_SUBSCRIBER_ID_B: &str = "sub-37a15883b1b600c5";
        let subscription_tracker = SubscriptionTracker::new(NonZeroUsize::new(1).unwrap(), true);
        let first_colliding_permit = subscription_tracker
            .try_insert(COLLIDING_SUBSCRIBER_ID_A.to_owned())
            .unwrap();
        let second_colliding_permit = subscription_tracker
            .try_insert(COLLIDING_SUBSCRIBER_ID_B.to_owned())
            .unwrap();

        drop(first_colliding_permit);
        drop(second_colliding_permit);

        assert!(subscription_tracker
            .try_insert(COLLIDING_SUBSCRIBER_ID_A.to_owned())
            .is_ok());
        assert!(subscription_tracker
            .try_insert(COLLIDING_SUBSCRIBER_ID_B.to_owned())
            .is_ok());
    }

    #[test]
    fn test_subscription_tracker_recovers_from_poisoned_lock() {
        let subscription_tracker = SubscriptionTracker::new(NonZeroUsize::new(1).unwrap(), true);
        let only_slot_permit = subscription_tracker
            .try_insert("poisoned-tracker".to_owned())
            .unwrap();
        let _ = std::panic::catch_unwind(|| {
            let _counters_guard = subscription_tracker.counters.lock().unwrap();
            panic!("poison the subscription tracker");
        });
        assert!(subscription_tracker.counters.is_poisoned());

        drop(only_slot_permit);
        assert!(subscription_tracker
            .try_insert("poisoned-tracker".to_owned())
            .is_ok());
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
                let account = fixtures::bank_account(pubkey, Pubkey::default(), slot, slot);
                messages.push(Message::Account(account));
            }
            let created_bank = fixtures::message_slot(slot, SlotStatus::CreatedBank);
            messages.push(Message::Slot(Arc::new(MessageSlot::new(
                created_bank.slot,
                created_bank.parent,
                created_bank.status,
                created_bank.dead_error,
                created_bank.bank_id,
                CREATED_AT,
            ))));
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
                    SubscribeRequestFilterTransactions {
                        account_include: vec![Pubkey::default().to_string()],
                        ..Default::default()
                    },
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
            stream_rx: &mut SubscribeReceiver,
            slot: u64,
            received: &mut Received,
        ) {
            read_slowly_until_confirmed(stream_rx, slot, received, Duration::ZERO).await;
        }

        /// Reads the client stream until `slot` is confirmed, into `received`, waiting
        /// `read_pause` before each read.
        async fn read_slowly_until_confirmed(
            stream_rx: &mut SubscribeReceiver,
            slot: u64,
            received: &mut Received,
            read_pause: Duration,
        ) {
            loop {
                if !read_pause.is_zero() {
                    tokio::time::sleep(read_pause).await;
                }
                let received_update =
                    tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                        .await
                        .expect("timed out waiting for an update")
                        .expect("stream closed")
                        .expect("status error")
                        .decode_update();
                match received_update.update_oneof {
                    Some(UpdateOneof::Transaction(transaction_update)) => {
                        let transaction_info =
                            transaction_update.transaction.expect("a transaction");
                        let transaction_id = transaction_info.signature[0];
                        received
                            .transactions
                            .push((transaction_update.slot, transaction_id));
                    }
                    Some(UpdateOneof::Slot(slot_update)) => {
                        match SlotStatus::from(slot_update.status()) {
                            SlotStatus::CreatedBank => {
                                let created_at =
                                    received_update.created_at.expect("a creation time");
                                received.created_banks.push((slot_update.slot, created_at));
                            }
                            SlotStatus::Processed => received.processed.push(slot_update.slot),
                            SlotStatus::Confirmed if slot_update.slot == slot => return,
                            _ => {}
                        }
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
            let (relay_tx, relay_rx) = mpsc::unbounded_channel();
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_rx,
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_tx,
                Some(replay_rx),
                None,
                100,
                spawn_test_bank_thread(),
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
                .send(Ok((Some(REPLAYED_SLOT), create_filter(commitment))))
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
            let (relay_tx, mut relay_rx) = mpsc::unbounded_channel();

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
            let (response_tx, _response_rx) = oneshot::channel();
            replay_tx
                .send((CommitmentLevel::Processed, PARTIAL_SLOT, response_tx))
                .await
                .unwrap();

            let handle = tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(queue_rx),
                broadcast,
                mpsc::unbounded_channel().1,
                relay_tx,
                Some(replay_rx),
                None,
                150,
                spawn_test_bank_thread(),
            ));
            let Some(RelayedItem::Answer(_, ReplayedResponse::Messages(replay))) =
                tokio::time::timeout(Duration::from_secs(5), relay_rx.recv())
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
            let (response_tx, _response_rx) = oneshot::channel();
            let (relay_tx, mut relay_rx) = mpsc::unbounded_channel();

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
                mpsc::unbounded_channel().1,
                relay_tx,
                Some(replay_rx),
                None,
                100,
                spawn_test_bank_thread(),
            ));

            let Some(RelayedItem::Answer(_, ReplayedResponse::Messages(replay))) =
                tokio::time::timeout(Duration::from_secs(5), relay_rx.recv())
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
            let (broadcast, [geyser_loop_changes_rx, reconstruction_changes_rx]) =
                SubscriberChannels::new(1024, 1024, 1024).with_indexing(IndexWorkLimits::default());
            let mut confirmed_rx = broadcast.subscribe(CommitmentLevel::Confirmed);
            let (relay_tx, relay_rx) = mpsc::unbounded_channel();
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                geyser_loop_changes_rx,
                relay_rx,
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                reconstruction_changes_rx,
                relay_tx,
                Some(replay_rx),
                None,
                100,
                spawn_test_bank_thread(),
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
                .send(Ok((Some(REPLAYED_SLOT), create_filter(commitment))))
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
            assert_eq!(received.processed, [REPLAYED_SLOT, SEALED_SLOT, NEXT_SLOT]);
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
            let (relay_tx, relay_rx) = mpsc::unbounded_channel();
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_rx,
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_tx,
                Some(replay_rx),
                None,
                100,
                spawn_test_bank_thread(),
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
                .send(Ok((
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
            let (relay_tx, relay_rx) = mpsc::unbounded_channel();
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_rx,
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_tx,
                Some(replay_rx),
                None,
                100,
                spawn_test_bank_thread(),
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
                .send(Ok((
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
            let (relay_tx, relay_rx) = mpsc::unbounded_channel();
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_rx,
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_tx,
                None,
                None,
                100,
                spawn_test_bank_thread(),
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
            client_tx.send(Ok((None, filter))).unwrap();
            settle().await;

            // Another slot dies first, so its Dead goes out before the bank.
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
                let received_update =
                    tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                        .await
                        .expect("timed out waiting for an update")
                        .expect("stream closed")
                        .expect("status error")
                        .decode_update();
                order.push(match received_update.update_oneof {
                    Some(UpdateOneof::Transaction(_)) => "tx".to_owned(),
                    Some(UpdateOneof::Entry(_)) => "entry".to_owned(),
                    Some(UpdateOneof::BlockMeta(_)) => "meta".to_owned(),
                    Some(UpdateOneof::Slot(slot_update)) => {
                        format!("{:?}", SlotStatus::from(slot_update.status()))
                    }
                    _ => "other".to_owned(),
                });
            }
            order
        }

        /// At every commitment, a bank's content comes first, then its block meta, then its
        /// statuses: Processed, Confirmed, Finalized. CreatedBank, FirstShredReceived and Dead
        /// reach every commitment.
        ///
        /// ```text
        /// time ─────────────────────────────────────────────────────────────►
        /// processed:  Dead FirstShred CreatedBank tx entry meta  P  C  F
        /// confirmed:  Dead FirstShred CreatedBank tx entry meta  P  C  F
        /// finalized:  Dead FirstShred CreatedBank tx entry meta  P  C  F
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

            for proto_commitment in [
                CommitmentLevelProto::Processed,
                CommitmentLevelProto::Confirmed,
                CommitmentLevelProto::Finalized,
            ] {
                let received_order = live_order(proto_commitment).await;
                assert_eq!(received_order[..2], ["Dead", "FirstShredReceived"]);
                assert_eq!(received_order[2..], bank);
            }
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
            let (relay_tx, relay_rx) = mpsc::unbounded_channel();
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_rx,
                block_reconstruction_tx,
            ));
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                mpsc::unbounded_channel().1,
                relay_tx,
                Some(replay_rx),
                None,
                100,
                spawn_test_bank_thread(),
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
                .send(Ok((
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
