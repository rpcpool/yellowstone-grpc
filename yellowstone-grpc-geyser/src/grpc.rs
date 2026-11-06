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
        route::{
            let_woken_task_run_first, BankRouteKeyIndex, BankThread, ClientRouting, LeaveOnDrop,
            OwnerRoutes, ProcessedOwner, RelayedItem, RouteChange, RouteIndexLimits, RouteKey,
            ROUTE_KEYS_PER_INDEX_STEP,
        },
        stream::{tokio::BatchStreamUnboundedReceiver, BatchStream, BatchStreamExt, Buffer},
        util::stream::{
            load_aware_channel, LoadAwareReceiver, LoadAwareSender, SendError, TrySendError,
            UnitWeigher, Weigher,
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
            Arc, Mutex as StdMutex,
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
    /// An event never in a raw batch: a block meta, footer or commitment status,
    /// which block reconstruction holds until the bank seals and then sends in its own
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
pub type SubscribeSender = LoadAwareSender<TonicResult<FilteredUpdate>, WeightScales>;

/// Sender half of a SubscribeDeshred queue, which bounds the number of queued updates.
type DeshredSubscribeSender = LoadAwareSender<
    TonicResult<FilteredUpdateDeshred>,
    UnitWeigher<TonicResult<FilteredUpdateDeshred>>,
>;

/// The broadcast channel of each commitment level, and the route change senders of the owners
/// that route.
///
/// Each level has one owner, its only sender: `geyser_loop` at processed, and block
/// reconstruction at confirmed and finalized. A client whose filter has route keys sends its
/// route changes to its level's owner, which then queues the client's updates straight into the
/// client's queue. Any other client reads its level's channel.
///
/// # Examples
///
/// ```ignore
/// let (broadcast, [geyser_loop_changes_rx, mut reconstruction_changes_rx]) =
///     SubscriberChannels::new(processed, confirmed, finalized)
///         .with_routing(RouteIndexLimits::default());
/// let broadcast_rx = broadcast.subscribe(CommitmentLevel::Confirmed);
/// let route_change = RouteChange::Join(new_route, Some(broadcast_rx));
/// broadcast.send_route_change(CommitmentLevel::Confirmed, route_change);
/// // Block reconstruction, the confirmed owner, takes it in and sends through its routes.
/// let mut owner_routes = OwnerRoutes::new(broadcast);
/// let received_route_change = reconstruction_changes_rx.recv().await.expect("a route change");
/// owner_routes.take_route_change(received_route_change);
/// owner_routes.send_batch(CommitmentLevel::Confirmed, message_batch);
/// ```
#[derive(Debug, Clone)]
pub struct SubscriberChannels {
    processed: broadcast::Sender<BroadcastedMessage>,
    confirmed: broadcast::Sender<BroadcastedMessage>,
    finalized: broadcast::Sender<BroadcastedMessage>,
    /// The route change senders of [`GrpcService::geyser_loop`] and
    /// [`GrpcService::block_reconstruction_loop`], or [`None`] when routing is off.
    route_change_txs: Option<[mpsc::UnboundedSender<RouteChange>; 2]>,
    /// How much index work each owner does, and how often.
    route_index_limits: RouteIndexLimits,
}

impl SubscriberChannels {
    /// Creates the three broadcast channels, with routing off.
    ///
    /// # Arguments
    ///
    /// * `processed` - The processed channel's capacity, in batches.
    /// * `confirmed` - The confirmed channel's capacity, in batches.
    /// * `finalized` - The finalized channel's capacity, in batches.
    ///
    /// # Returns
    ///
    /// Channels with no owner to route through, until `with_routing` adds them, and the default
    /// [`RouteIndexLimits`].
    ///
    /// # Panics
    ///
    /// Panics if a capacity is `0` or above `usize::MAX / 2`, as [`broadcast::channel`] does.
    pub fn new(processed: usize, confirmed: usize, finalized: usize) -> Self {
        Self {
            processed: broadcast::channel(processed).0,
            confirmed: broadcast::channel(confirmed).0,
            finalized: broadcast::channel(finalized).0,
            route_change_txs: None,
            route_index_limits: RouteIndexLimits::default(),
        }
    }

    /// Turns routing on, with a route change channel for each owner.
    ///
    /// # Arguments
    ///
    /// * `route_index_limits` - How much index work each owner does, and how often.
    ///
    /// # Returns
    ///
    /// The channels with the owners' route change senders and `route_index_limits`, and the
    /// route change receivers of [`GrpcService::geyser_loop`] and
    /// [`GrpcService::block_reconstruction_loop`], in that order.
    fn with_routing(
        mut self,
        route_index_limits: RouteIndexLimits,
    ) -> (Self, [mpsc::UnboundedReceiver<RouteChange>; 2]) {
        let (geyser_loop_changes_tx, geyser_loop_changes_rx) = mpsc::unbounded_channel();
        let (reconstruction_changes_tx, reconstruction_changes_rx) = mpsc::unbounded_channel();
        self.route_change_txs = Some([geyser_loop_changes_tx, reconstruction_changes_tx]);
        self.route_index_limits = route_index_limits;
        (self, [geyser_loop_changes_rx, reconstruction_changes_rx])
    }

    /// The index limits each owner's [`OwnerRoutes`] keeps to.
    ///
    /// # Returns
    ///
    /// The [`RouteIndexLimits`] from `with_routing`, or the default ones.
    pub const fn route_index_limits(&self) -> RouteIndexLimits {
        self.route_index_limits
    }

    /// Sends a route change to the owner of `commitment_level`: [`GrpcService::geyser_loop`] at
    /// processed, [`GrpcService::block_reconstruction_loop`] at confirmed and finalized.
    ///
    /// It drops the route change when routing is off or the owner has stopped.
    ///
    /// # Arguments
    ///
    /// * `commitment_level` - The commitment level of the client's route.
    /// * `route_change` - The route change to send.
    #[allow(rustdoc::private_intra_doc_links)]
    pub fn send_route_change(&self, commitment_level: CommitmentLevel, route_change: RouteChange) {
        if let Some(route_change_txs) = &self.route_change_txs {
            let route_change_tx_index = usize::from(commitment_level != CommitmentLevel::Processed);
            let _ = route_change_txs[route_change_tx_index].send(route_change);
        }
    }

    /// The route keys of `client_filter`, when routing is on, in the [`Arc`] of
    /// `held_route_keys` when the two lists are equal.
    ///
    /// An owner compares key lists by [`Arc`] only, so a client that keeps its keys keeps its
    /// [`Arc`] too, and its owner replaces the route's filter in place.
    ///
    /// # Arguments
    ///
    /// * `client_filter` - The client's filter.
    /// * `held_route_keys` - The key list the client holds, or [`None`].
    ///
    /// # Returns
    ///
    /// The keys from [`Filter::route_keys`], `held_route_keys` itself when it lists the same
    /// keys, or [`None`] when routing is off or [`Filter::route_keys`] gives [`None`].
    pub fn route_keys(
        &self,
        client_filter: &Filter,
        held_route_keys: Option<&Arc<[RouteKey]>>,
    ) -> Option<Arc<[RouteKey]>> {
        self.route_change_txs.as_ref()?;
        let listed_route_keys = client_filter.route_keys()?;
        match held_route_keys {
            Some(held_route_keys) if held_route_keys[..] == listed_route_keys[..] => {
                Some(Arc::clone(held_route_keys))
            }
            _ => Some(listed_route_keys.into()),
        }
    }

    /// A guard that has the owners remove the client's route when the client's task ends.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
    ///
    /// # Returns
    ///
    /// The [`LeaveOnDrop`], which leaves both owners until [`LeaveOnDrop::leave_only`] names
    /// one, or [`None`] when routing is off.
    pub fn leave_on_drop(&self, session_id: usize) -> Option<LeaveOnDrop> {
        let route_change_txs = self.route_change_txs.clone()?;
        Some(LeaveOnDrop {
            session_id,
            route_change_txs: route_change_txs.map(Some),
        })
    }

    /// The broadcast sender of a commitment channel.
    ///
    /// # Arguments
    ///
    /// * `commitment` - The channel.
    ///
    /// # Returns
    ///
    /// The channel's [`broadcast::Sender`].
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
    /// A routed client gets nothing sent here: each level's owner sends through its
    /// [`OwnerRoutes`], which calls this after it routes the batch.
    ///
    /// # Arguments
    ///
    /// * `commitment` - The channel.
    /// * `messages` - The batch to broadcast.
    #[inline]
    pub fn send(&self, commitment: CommitmentLevel, messages: BroadcastedMessage) {
        let _ = self.sender(commitment).send(messages);
    }

    /// Subscribes a new receiver to the `commitment` channel.
    ///
    /// # Arguments
    ///
    /// * `commitment` - The channel.
    ///
    /// # Returns
    ///
    /// A receiver that gets every batch broadcast on the channel after this call.
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
    /// transactions and entries, with the bank's [`BankRouteKeyIndex`] once the bank thread
    /// built it, so a client with route keys visits only the messages its keys find.
    SealedBank(
        Arc<Vec<Message>>,
        Range<usize>,
        Option<Arc<BankRouteKeyIndex>>,
    ),
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
    /// [`OwnerRoutes`].
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
    ///
    /// # Arguments
    ///
    /// * `relay_tx` - The relay to [`GrpcService::geyser_loop`].
    ///
    /// # Returns
    ///
    /// `true` if it relayed a batch, and `false` if no batch was held.
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

/// One Subscribe client's session: its session id, filter and disconnect reason, its
/// subscription permit, and the sender whose drop tells the request task that the client's live
/// stream has started.
///
/// Dropping it counts the disconnect under its reason, takes it out of the subscription
/// metrics, and cancels its cancellation token.
///
/// # Examples
///
/// ```ignore
/// let (client_live_tx, client_live_rx) = oneshot::channel();
/// let mut session = ClientSession::new(
///     session_id,
///     subscriber_id,
///     endpoint,
///     cancellation_token,
///     subscription_permit,
/// );
/// session.client_live_tx = Some(client_live_tx);
/// session.set_filter(new_filter);
/// session.client_live_tx = None;
/// session.disconnect_reason = "client_disconnect";
/// drop(session);
/// ```
pub struct ClientSession {
    pub id: usize,
    pub subscriber_id: String,
    endpoint: String,
    /// The client's filter, shared with its route while the client is routed.
    pub filter: Arc<Filter>,
    pub cancellation_token: CancellationToken,
    pub disconnect_reason: &'static str,
    /// Must own it so that when the session is dropped, the subscription count is decremented.
    _subscription_permit: Option<SubscriptionOwnedPermit>,
    /// Dropped where [`GrpcService::client_loop`] starts the live stream, which lets
    /// [`GrpcService::read_requests`] post the client's later filter changes. [`None`] when no
    /// request task waits for it.
    client_live_tx: Option<oneshot::Sender<()>>,
}

impl ClientSession {
    /// Creates a session with the default filter, which matches nothing, and counts it in the
    /// subscription metrics.
    ///
    /// # Arguments
    ///
    /// * `id` - The client's session id.
    /// * `subscriber_id` - The client's subscriber id, or [`None`] for `UNKNOWN`.
    /// * `endpoint` - The endpoint the client named in its `x-endpoint` header, or empty.
    /// * `cancellation_token` - The client's cancellation token, cancelled when the session drops.
    /// * `subscription_permit` - The subscriber's [`SubscriptionOwnedPermit`], released when
    ///   the session drops, or [`None`] when the client has no subscriber id.
    ///
    /// # Returns
    ///
    /// The session, with the disconnect reason `unknown` and no live signal sender.
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
            filter: Arc::new(filter),
            cancellation_token,
            disconnect_reason: "unknown",
            _subscription_permit: subscription_permit,
            client_live_tx: None,
        }
    }

    /// Replaces the client's filter, and moves the subscription metrics from the old filter to
    /// the new one.
    ///
    /// # Arguments
    ///
    /// * `new_filter` - The client's new filter.
    fn set_filter(&mut self, new_filter: Filter) {
        metrics::update_subscriptions(&self.endpoint, Some(&self.filter), Some(&new_filter));
        self.filter = Arc::new(new_filter);
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

/// The Geyser gRPC service: what every `Subscribe` stream and unary call shares, with the
/// limits and intervals of its [`ConfigGrpc`]. [`GrpcService::create`] builds it, serves it on
/// every listener, and starts the loops that feed its clients.
///
/// # Examples
///
/// ```ignore
/// let grpc_service_result = GrpcService::create(
///     config_grpc,
///     false,
///     service_cancellation_token,
///     task_tracker,
///     file_watcher,
///     messages_rx,
/// )
/// .await?;
/// let snapshot_tx = grpc_service_result.snapshot_tx;
/// ```
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

/// The senders [`GrpcService::create`] hands back to the plugin, whose callbacks feed them.
pub struct GrpcServiceResult {
    pub snapshot_tx: Option<crossbeam_channel::Sender<Box<Message>>>,
    pub deshred_broadcast_tx: broadcast::Sender<DeshredBroadcastedMessage>,
    pub contact_info_tx: mpsc::UnboundedSender<contact_info::ContactInfoNotification>,
    pub blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
}

impl GrpcService {
    /// Binds the configured listeners, spawns the routing loops, the thread that frees evicted
    /// banks and one server task per listener, and hands back the senders the plugin feeds.
    ///
    /// # Arguments
    ///
    /// * `config` - The gRPC configuration.
    /// * `is_reload` - Whether the validator is reloading the plugin. A reload takes no
    ///   snapshot.
    /// * `service_cancellation_token` - Cancelled to shut the service down.
    /// * `task_tracker` - Tracks the loops, listeners and client tasks the service spawns.
    /// * `file_watcher` - Reloads the TLS files of the [`listen`](ConfigGrpc::listen) entries
    ///   whose [`GrpcTlsConfig`] sets `watch_file`.
    /// * `messages_rx` - The plugin's messages, which geyser_loop reads.
    ///
    /// # Returns
    ///
    /// The [`GrpcServiceResult`]: the senders for snapshot accounts, deshred messages, contact
    /// info and block metas.
    ///
    /// # Errors
    ///
    /// Fails if a TCP or Unix listener cannot be bound, a stale Unix socket file cannot be
    /// removed or a new one's mode set, a TLS certificate, key or cert dir cannot be loaded or
    /// watched, the thread that frees evicted banks cannot be spawned, or
    /// [`x_token`](ConfigGrpc::x_token) is not a valid header value.
    ///
    /// # Panics
    ///
    /// Panics if the legacy [`address`](ConfigGrpc::address) list is set and
    /// [`tls_config`](ConfigGrpc::tls_config) names a certificate or key file that cannot be
    /// read, or if a broadcast channel capacity in `config` is `0`.
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
        let route_index_limits = RouteIndexLimits {
            slice_interval: config.route_index_interval,
            slice_budget: config.route_index_budget,
            keys_per_step: ROUTE_KEYS_PER_INDEX_STEP,
        };
        let (broadcast, [geyser_loop_changes_rx, reconstruction_changes_rx]) =
            SubscriberChannels::new(
                capacities.processed_broadcast,
                capacities.confirmed_broadcast,
                capacities.finalized_broadcast,
            )
            .with_routing(route_index_limits);
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
            filter_apply_interval: config.filter_apply_interval,
        })
        .max_decoding_message_size(max_decoding_message_size);
        for encoding in config.compression.accept {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.compression.send {
            service = service.send_compressed(encoding);
        }

        let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
        let (relay_tx, relay_rx) = mpsc::unbounded_channel();

        // Warn if replay buffer is too small for auto-reconnect
        if config.replay_stored_slots < 150 {
            log::warn!(
                "replay_stored_slots={} may be too low for auto-reconnect; recommend >= 150",
                config.replay_stored_slots
            );
        }

        let bank_thread = BankThread::spawn().context("failed to spawn the bank thread")?;
        {
            let broadcast = broadcast.clone();

            task_tracker.spawn(async move {
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

            task_tracker.spawn(async move {
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

        Ok(GrpcServiceResult {
            snapshot_tx,
            deshred_broadcast_tx,
            contact_info_tx,
            blocks_meta_tx,
        })
    }

    /// Sends the plugin's raw batches at processed, and hands every message to block
    /// reconstruction.
    ///
    /// It is the only sender at processed, through its [`ProcessedOwner`]. Block reconstruction
    /// relays to it ([`RelayedItem`]) the batches it builds for processed, and each replay behind
    /// the batches relayed before it. Until the plugin's next batch is ready,
    /// [`ProcessedOwner::next_plugin_batch`] sends those, takes in route changes and runs index
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
    /// A raw batch holds up to 1024 messages, and goes out in a [`Vec`] of its own size, so the
    /// buffer keeps its capacity. A block meta, block footer or commitment status ends the
    /// batch. It goes to block reconstruction only, after the batch, as a
    /// [`BlockReconstructionMessage::SealGated`].
    ///
    /// # Arguments
    ///
    /// * `messages_rx` - The plugin's messages.
    /// * `broadcast` - The broadcast channels its [`OwnerRoutes`] broadcast on.
    /// * `route_changes_rx` - The route changes of processed clients, and every client's last
    ///   leave.
    /// * `relay_rx` - What block reconstruction relays: its processed batches and its replays.
    /// * `block_reconstruction_tx` - Hands each raw batch and each seal-gated message to block
    ///   reconstruction.
    async fn geyser_loop<St>(
        mut messages_rx: St,
        broadcast: SubscriberChannels,
        route_changes_rx: mpsc::UnboundedReceiver<RouteChange>,
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
        let mut processed_owner = ProcessedOwner::new(broadcast, route_changes_rx, relay_rx);
        loop {
            let batch_size_maybe = processed_owner
                .next_plugin_batch(&mut messages_rx, &mut buffer)
                .await;
            let Some(_) = batch_size_maybe else {
                info!("Geyser loop: messages channel closed");
                break;
            };

            if !buffer.message_batch.is_empty() {
                metrics::message_queue_size_dec_by(buffer.message_batch.len() as i64);

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

    /// Rebuilds banks from what geyser_loop hands over, sends each bank at every commitment level
    /// it reaches, and builds the replays.
    ///
    /// It is the only sender at confirmed and finalized, through its [`OwnerRoutes`]. Its processed
    /// batches, and each replay behind them, go on the relay to [`GrpcService::geyser_loop`],
    /// the only sender at processed. Every batch it sends goes through
    /// [`BroadcastPosition::send`], so [`BroadcastPosition::cut`] can tell a replay where it
    /// ends. A route change lands between two passes. Each pass starts by taking in at most
    /// [`MAX_ROUTE_CHANGES_PER_PASS`](crate::route::MAX_ROUTE_CHANGES_PER_PASS) queued changes,
    /// so a change waits for one pass or one replay build, plus one pass per 64 changes queued
    /// ahead of it, however far behind the queue is.
    ///
    /// Index work runs in slices: at the top of a pass once it is overdue, or in the last arm of
    /// the `select!`, the index slice timer, once the other arms read `Pending`.
    ///
    /// A relay wakes geyser_loop into this worker's LIFO slot, which no other worker steals
    /// from, so geyser_loop waits there until this task's poll ends. So a pass that seals a bank
    /// at processed relays first, then lets geyser_loop run with [`let_woken_task_run_first`]
    /// before its confirmed and finalized sends. Any other pass relays only at its end, after
    /// them, and lets geyser_loop run once the pass has stored the oldest replayable slot,
    /// before the next pass takes in a change or runs a slice. A replay's answer goes on the
    /// relay too, and geyser_loop runs right after it.
    ///
    /// It builds a replay only after the queue reads empty, on a read made after it took the
    /// request. Everything in the queue went out live, maybe before the client subscribed, so
    /// the replay must hold all of it. A request for a slot older than every stored slot gets
    /// [`ReplayedResponse::Lagged`] straight on its oneshot.
    ///
    /// # Arguments
    ///
    /// * `messages_rx` - What geyser_loop hands over, in order.
    /// * `broadcast` - The broadcast channels its [`OwnerRoutes`] broadcast on.
    /// * `route_changes_rx` - The route changes of confirmed and finalized clients, and every
    ///   client's last leave.
    /// * `relay_tx` - The relay to geyser_loop, for the processed batches and the replays.
    /// * `replay_stored_slots_rx` - The replay requests, or [`None`] when replay is off.
    /// * `replay_first_available_slot` - Set to the oldest replayable slot after each pass, or
    ///   [`None`] when replay is off.
    /// * `replay_stored_slots` - How many slots of sealed banks to keep for replay.
    /// * `bank_thread` - Frees the banks of the evicted slots and indexes the route keys of the
    ///   sealed banks on its own thread, and joins that thread when the loop drops it.
    #[allow(clippy::too_many_arguments)]
    async fn block_reconstruction_loop<St>(
        mut messages_rx: St,
        broadcast: SubscriberChannels,
        mut route_changes_rx: mpsc::UnboundedReceiver<RouteChange>,
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
        let mut owner_routes = OwnerRoutes::new(broadcast);
        let index_slice_timer = sleep_until(owner_routes.next_index_slice_at());
        tokio::pin!(index_slice_timer);
        // A replay request taken from its channel, built once the queue reads empty again.
        let mut pending_replay: Option<ReplayStoredSlotsRequest> = None;

        loop {
            owner_routes.take_queued_work(&mut route_changes_rx, index_slice_timer.as_mut());
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
                    let relayed_at_pass_end = Self::take_in(&mut block_machine, &mut owner_routes, &relay_tx, &bank_thread, &mut position, buffered_messages.drain(..)).await;

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
                        let route_key_index = replayed_slot.frozen_block.route_key_index();
                        replayed_messages.push(ReplayResponseMessageType::SealedBank(Arc::clone(&bank_messages), 0..created_bank_index, route_key_index.clone()));
                        replayed_messages.push(ReplayResponseMessageType::Single(Message::Slot(replayed_slot.frozen_block.created_bank())));
                        replayed_messages.push(ReplayResponseMessageType::SealedBank(Arc::clone(&bank_messages), created_bank_index..bank_messages.len(), route_key_index));

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

                    let _ = relay_tx.send(RelayedItem::Answer(tx, ReplayedResponse::Messages(Replay {
                        messages: replayed_messages,
                        bank_id_span,
                        cut: position.cut(commitment),
                    })));
                    let_woken_task_run_first().await;
                }
                Some(route_change) = route_changes_rx.recv() => {
                    owner_routes.take_route_change(route_change);
                }
                Some(request) = replay_stored_slots_rx.recv(), if pending_replay.is_none() => {
                    pending_replay = Some(request);
                }
                // A Sleep reads Pending when the budget is spent, so the queue rule above holds.
                () = &mut index_slice_timer, if owner_routes.has_index_work() => {
                    owner_routes.run_index_slice_and_reset_timer(index_slice_timer.as_mut());
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
    /// `owner_routes` in order. The processed ones, in [`BroadcastPosition::held_relays`], go on
    /// the relay to [`GrpcService::geyser_loop`] last: the relay wakes geyser_loop into this
    /// worker's LIFO slot, where it waits for whatever the pass does after it. A pass that seals
    /// a bank at processed relays them first instead, and lets geyser_loop run with
    /// [`let_woken_task_run_first`] before its confirmed and finalized sends, so a block meta at
    /// processed never waits for their routing. The [`BankThread`] frees the banks of the slots
    /// evicted in the pass, since freeing a whole bank takes milliseconds, and indexes the route
    /// keys of the banks sealed in it.
    ///
    /// # Arguments
    ///
    /// * `block_machine` - The banks being reconstructed.
    /// * `owner_routes` - Sends the confirmed and finalized batches.
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
        owner_routes: &mut OwnerRoutes,
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
            owner_routes.send_batch(commitment_level, held_batch);
        }
        bank_thread.hand_over_banks(block_machine);

        if let Some(min_slot) = block_machine.min_replayable_slot() {
            position
                .sent_at_processed
                .retain(|_, slot| *slot >= min_slot);
        }
        position.relay_held_batches(relay_tx)
    }

    /// Reads one Subscribe client's requests: answers its pings, and posts its filters to
    /// [`GrpcService::client_loop`], at most one filter change per `filter_apply_interval`.
    ///
    /// Each request is compiled with [`Filter::new`] when it is handled, and a filter error goes
    /// to the client as a [`Status::invalid_argument`] status. A ping is handled at once: its pong
    /// goes on `stream_tx`, and it changes neither the waiting request nor the interval. Before
    /// the first post, every request is handled at once, and one for which [`asks_for_nothing`]
    /// holds is skipped after its compile, so it neither posts nor starts the interval. A pong or
    /// a filter error waits for room in the Subscribe queue, and the task reads no request
    /// meanwhile.
    ///
    /// After the first post, a request read before `filter_apply_interval` has passed since the
    /// last post waits, uncompiled, and a newer request replaces it. So a waiting request's filter
    /// error comes when it would post, a replaced request is never compiled, and a failed compile
    /// does not restart the interval. A later `from_slot` and a commitment change wait like any
    /// change. The [`tokio::select!`] is biased, and reads requests before the waiting request's
    /// timer: a request read once the interval has passed posts at once and drops the waiting one.
    ///
    /// No change posts until `client_live_rx` resolves, which [`GrpcService::client_loop`] makes
    /// happen where its live stream starts. During a snapshot or a replay,
    /// [`GrpcService::client_loop`] reads no request: the snapshot or replay, and the backlog
    /// after it, use the first request's filter. The newest change sent meanwhile posts once the
    /// live stream starts, or when its interval ends if that is later, and its filter error comes
    /// then. Pings are still answered, and no request is dropped.
    ///
    /// A half-close stops the reads, but a waiting request still posts, and `client_tx` stays
    /// open until the cancellation. A read error posts [`None`] and ends the task.
    ///
    /// # Arguments
    ///
    /// * `subscribe_requests` - The client's request stream.
    /// * `session_id` - The client's session id, for the logs.
    /// * `subscriber_id` - The client's subscriber id, for the logs and the filter metrics.
    /// * `config_filter_limits` - The [`FilterLimits`] every filter must meet.
    /// * `filter_names` - The client's [`FilterNames`].
    /// * `filter_apply_interval` - The shortest time between two posts, from
    ///   [`ConfigGrpc::filter_apply_interval`].
    /// * `stream_tx` - The client's Subscribe queue, for pongs and filter errors.
    /// * `client_tx` - Posts each filter, with its request's `from_slot`, to
    ///   [`GrpcService::client_loop`], and [`None`] once the request stream fails.
    /// * `client_live_rx` - Resolves when [`GrpcService::client_loop`] drops [`ClientSession`]'s
    ///   live signal sender, where its live stream starts, or when the session ends.
    /// * `cancellation_token` - Cancelled when the session ends or the server shuts down.
    #[allow(clippy::too_many_arguments)]
    async fn read_requests(
        mut subscribe_requests: impl Stream<Item = TonicResult<SubscribeRequest>> + Unpin,
        session_id: usize,
        subscriber_id: String,
        config_filter_limits: Arc<FilterLimits>,
        mut filter_names: FilterNames,
        filter_apply_interval: Duration,
        stream_tx: SubscribeSender,
        client_tx: mpsc::UnboundedSender<Option<(Option<u64>, Filter)>>,
        mut client_live_rx: oneshot::Receiver<()>,
        cancellation_token: CancellationToken,
    ) {
        let mut forwarded_first = false;
        let mut client_live = false;
        let mut waiting_request: Option<SubscribeRequest> = None;
        let mut next_change_at = Instant::now();
        let mut reading_requests = true;
        loop {
            let subscribe_request = tokio::select! {
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
                    Some(Err(_error)) => {
                        let _ = client_tx.send(None);
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

            if let Err(error) =
                match Filter::new(&subscribe_request, &config_filter_limits, &mut filter_names) {
                    Ok(filter) => {
                        if let Some(msg) = filter.get_pong_msg() {
                            if stream_tx.send(Ok(msg)).await.is_err() {
                                error!("client #{subscriber_id:?}/{session_id}: stream closed");
                                let _ = client_tx.send(None);
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
                        let filter_post = Some((subscribe_request.from_slot, filter));
                        let post_result = match client_tx.send(filter_post) {
                            Ok(()) => Ok(()),
                            Err(error) => Err(error.to_string()),
                        };
                        forwarded_first = true;
                        next_change_at = Instant::now() + filter_apply_interval;
                        post_result
                    }
                    Err(error) => Err(error.to_string()),
                }
            {
                let err = Err(Status::invalid_argument(format!(
                    "failed to create filter: {error}"
                )));
                if stream_tx.send(err).await.is_err() {
                    let _ = client_tx.send(None);
                }
            }
        }
    }

    /// Serves one Subscribe client: takes its requests, sends its snapshot or replay, then its
    /// live updates, routed or from the broadcast, until the client or the server ends.
    ///
    /// It reads no request during its snapshot or replay: those, and the backlog after a replay,
    /// use the first request's filter. Where the live stream starts, it drops the session's live
    /// signal sender, so [`GrpcService::read_requests`] posts the newest filter change sent
    /// meanwhile, and the later ones. That change can reach the client after its first route
    /// change, and then moves the route as any later change does.
    ///
    /// A client whose filter has route keys joins its route in two steps. It sends each step to
    /// its level's owner at a read that finds its broadcast receiver and its requests empty, so a
    /// queued filter change applies first:
    ///
    /// 1. A [`RouteChange::Prepare`]. The client keeps reading and filtering its receiver while
    ///    the owner claims the route's keys.
    /// 2. Once the owner answers, a [`RouteChange::Join`] with the receiver. The owner delivers
    ///    what the receiver got since, then queues the client's updates straight into
    ///    `stream_tx`. The client holds no receiver then, so it wakes only for a request or its
    ///    cancellation token.
    ///
    /// [`ClientRouting`] sends these steps, moves the route when a filter change applies, and
    /// sends the last leave when the loop ends.
    ///
    /// # Arguments
    ///
    /// * `session` - The client's session, which records the disconnect reason when it drops.
    /// * `stream_tx` - The client's Subscribe queue.
    /// * `client_rx` - The client's requests: each one's `from_slot` and filter, or [`None`]
    ///   once the client has gone.
    /// * `snapshot_rx` - The startup snapshot to send first, or [`None`].
    /// * `broadcast` - The broadcast channels, and the owners' route change senders.
    /// * `replay_stored_slots_tx` - The replay requests to block reconstruction, or [`None`]
    ///   when replay is off.
    /// * `task_tracker` - Runs the tasks that send the client an error.
    /// * `client_unresponsive_timeout` - How long the client can leave a replayed update
    ///   unread.
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
        let route_keys = broadcast.route_keys(&session.filter, None);
        let mut messages_rx = match from_slot {
            // A snapshot subscription does not replay.
            Some(from_slot) if !snapshot_requested => {
                let replay = Self::client_loop_replay(
                    &mut session,
                    &stream_tx,
                    &broadcast,
                    replay_stored_slots_tx.as_ref(),
                    commitment,
                    route_keys.as_deref(),
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
        let mut client_routing = ClientRouting::new(&session, &stream_tx, route_keys, &broadcast);

        'outer: loop {
            observe_subscriber_queue_size(
                &session.subscriber_id,
                stream_tx.current_weight() as u64,
                "normal",
            );
            client_routing.send_join_step(&session, &mut messages_rx, &client_rx, commitment);

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
                            client_routing
                                .change_filter(&session, &mut messages_rx, commitment, commitment_new)
                                .await;
                            if commitment_new != commitment {
                                commitment = commitment_new;
                                messages_rx = Some(broadcast.subscribe(commitment));
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
                Some(ready_answer) = OptionFuture::from(client_routing.ready_rx()) => {
                    client_routing.take_ready_answer(ready_answer);
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
        client_routing.end_session(session, messages_rx.is_none(), commitment);
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
    /// A send that finds room in `stream_tx` does not yield. So the loop spends one unit of
    /// tokio's cooperative budget with [`consume_budget`](tokio::task::coop::consume_budget) for
    /// each message it takes from the [`ReplayPhase`] that gives the client an update. A client
    /// without route keys spends one for every message it takes, since its replay visits every
    /// stored message. With tokio's budget of 128 units per poll, a replay to a client that keeps
    /// up yields about every 128 such messages, always between two messages, and the runtime runs
    /// other tasks meanwhile, such as [`GrpcService::geyser_loop`] and
    /// [`GrpcService::block_reconstruction_loop`].
    ///
    /// # Arguments
    ///
    /// * `session` - The client session: its filter shapes the replay.
    /// * `stream_tx` - The client's update queue.
    /// * `broadcast` - The commitment channels.
    /// * `replay_stored_slots_tx` - Requests to block reconstruction, or [`None`] when replay
    ///   is off.
    /// * `commitment` - The commitment of the replay and of the live stream.
    /// * `route_keys` - The client's route keys, or [`None`]. With them, each sealed bank with a
    ///   [`BankRouteKeyIndex`] hands out only the messages they find.
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
        route_keys: Option<&[RouteKey]>,
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
        let mut phase = match ReplayPhase::new(messages_rx, commitment, replay, route_keys) {
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
            let filtered_updates = session.filter.get_updates(&message, Some(commitment));
            let spends_budget_unit = route_keys.is_none() || !filtered_updates.is_empty();
            for filtered_message in filtered_updates {
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

    /// Opens a Subscribe stream: takes a subscription permit when the subscriber is known, then
    /// spawns the client's ping task, its request task and its `client_loop`.
    ///
    /// The ping task sends a ping every 10 s, which also finds a client that has gone. It also
    /// samples the client's queue size, since a routed client's loop does not wake for each
    /// update.
    ///
    /// The request task, `GrpcService::read_requests`, posts at most one filter change per
    /// `filter_apply_interval`. It posts no change until `client_loop` drops the sender of a
    /// oneshot channel, where the client's live stream starts.
    ///
    /// # Arguments
    ///
    /// * `request` - The client's request stream. Its metadata can name the subscription
    ///   (`x-subscription-id`), ask for the startup snapshot (`x-request-snapshot`) and name an
    ///   endpoint (`x-endpoint`).
    ///
    /// # Returns
    ///
    /// The response stream, which reads the client's Subscribe queue.
    ///
    /// # Errors
    ///
    /// [`Status::resource_exhausted`] if the subscriber is at its enforced subscription limit,
    /// and [`Status::unavailable`] if the server is shutting down.
    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
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
                        observe_subscriber_queue_size(ping_subscriber_id.as_deref().unwrap_or("UNKNOWN"), ping_stream_tx.current_weight() as u64, "normal");
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

        let filter_names = FilterNames::new(
            self.filter_name_size_limit,
            self.filter_names_size_limit,
            self.filter_names_cleanup_interval,
        );

        let (client_live_tx, client_live_rx) = oneshot::channel();
        self.task_tracker.spawn(Self::read_requests(
            request.into_inner(),
            id,
            subscriber_id.clone().unwrap_or("unknown".to_string()),
            config_filter_limits,
            filter_names,
            self.filter_apply_interval,
            incoming_stream_tx,
            incoming_client_tx,
            client_live_rx,
            incoming_cancellation_token,
        ));

        let mut client_session = ClientSession::new(
            id,
            subscriber_id.clone(),
            endpoint.clone(),
            client_cancellation_token,
            subscription_permit,
        );
        client_session.client_live_tx = Some(client_live_tx);
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

    /// Replays 10,000 slot statuses to a client through a filter that keeps the Processed ones,
    /// every other message, and notes how many updates the client's queue holds each time the
    /// test's own task runs during the replay.
    ///
    /// The test's own task is the counter task. The `current_thread` runtime polls it first in
    /// each pass, so it runs once between two polls of the client. A spawned counter would not
    /// show this: the runtime wakes yielded tasks last in, first out, so it would run twice
    /// between some polls of the client and not at all between others.
    ///
    /// # Arguments
    ///
    /// * `subscriber_channels` - The client's channels. With routing on, the client has route
    ///   keys.
    ///
    /// # Returns
    ///
    /// How many updates the client queued between two runs of the counter task, from the start
    /// of the replay.
    async fn updates_queued_between_counter_runs(
        subscriber_channels: SubscriberChannels,
    ) -> Vec<usize> {
        const REPLAYED_MESSAGE_COUNT: u64 = 10_000;
        const QUEUED_UPDATE_COUNT: usize = REPLAYED_MESSAGE_COUNT as usize / 2;
        let (replay_tx, mut replay_rx) = mpsc::channel::<ReplayStoredSlotsRequest>(1);
        tokio::spawn(async move {
            let Some((_replay_commitment, _replay_from_slot, response_tx)) = replay_rx.recv().await
            else {
                return;
            };
            let replayed_messages: Vec<_> = (0..REPLAYED_MESSAGE_COUNT)
                .map(|message_number| {
                    let slot_status = if message_number.is_multiple_of(2) {
                        SlotStatus::Processed
                    } else {
                        SlotStatus::Completed
                    };
                    let slot_message = fixtures::message_slot(message_number / 2, slot_status);
                    ReplayResponseMessageType::Single(Message::Slot(Arc::new(slot_message)))
                })
                .collect();
            let _ = response_tx.send(ReplayedResponse::Messages(Replay {
                bank_id_span: BankSpan::of(&replayed_messages),
                messages: replayed_messages,
                cut: ReplayCut {
                    last_taken_in: None,
                    last_sent: None,
                    next_taken_in: None,
                },
            }));
        });
        let (stream_tx, _stream_rx) =
            load_aware_channel(REPLAYED_MESSAGE_COUNT as usize, WeightScales::default());
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let client_session = ClientSession::new(
            0,
            Some("test".into()),
            "test".into(),
            CancellationToken::new(),
            None,
        );
        let client_task = tokio::spawn(GrpcService::client_loop(
            client_session,
            stream_tx.clone(),
            client_rx,
            None,
            subscriber_channels,
            Some(replay_tx),
            TaskTracker::new(),
            Some(Duration::from_secs(60)),
        ));
        client_tx
            .send(Some((Some(0), create_filter_with_slots())))
            .unwrap();

        let mut lengths_during_replay = vec![0];
        yield_until(|| {
            let queue_length = stream_tx.current_weight();
            if 0 < queue_length && queue_length < QUEUED_UPDATE_COUNT {
                lengths_during_replay.push(queue_length);
            }
            queue_length == QUEUED_UPDATE_COUNT
        })
        .await;
        lengths_during_replay.dedup();
        drop(client_tx);
        tokio::time::timeout(Duration::from_secs(5), client_task)
            .await
            .expect("client loop ends once the client leaves")
            .unwrap();
        lengths_during_replay
            .windows(2)
            .map(|consecutive_lengths| consecutive_lengths[1] - consecutive_lengths[0])
            .collect()
    }

    /// A replay to a client without route keys spends one unit of tokio's budget per message it
    /// takes from its [`ReplayPhase`], so it yields about every 128 messages, and other tasks run
    /// between. The filter keeps every other message, so the messages it drops count too: the
    /// replay queues 64 updates between two runs of the counter task, give or take one.
    #[tokio::test]
    async fn a_replaying_client_yields_about_every_128_messages() {
        let updates_between_runs =
            updates_queued_between_counter_runs(SubscriberChannels::new(16, 16, 16)).await;
        assert!(
            updates_between_runs.len() >= 70,
            "updates queued between two runs of the counter: {updates_between_runs:?}"
        );
        assert!(
            updates_between_runs
                .iter()
                .all(|update_count| (63..=65).contains(update_count)),
            "updates queued between two runs of the counter: {updates_between_runs:?}"
        );
    }

    /// A replay to a client with route keys spends one unit of tokio's budget per message that
    /// gives the client an update, so the messages the filter drops spend none: the replay queues
    /// 128 updates between two runs of the counter task, give or take two.
    #[tokio::test]
    async fn a_replay_with_route_keys_yields_about_every_128_updates() {
        let (subscriber_channels, _route_changes_rxs) =
            SubscriberChannels::new(16, 16, 16).with_routing(RouteIndexLimits::default());
        let updates_between_runs = updates_queued_between_counter_runs(subscriber_channels).await;
        assert!(
            updates_between_runs.len() >= 35,
            "updates queued between two runs of the counter: {updates_between_runs:?}"
        );
        assert!(
            updates_between_runs
                .iter()
                .all(|update_count| (126..=130).contains(update_count)),
            "updates queued between two runs of the counter: {updates_between_runs:?}"
        );
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

    /// Yields until `awaited_condition` holds, so that the other tasks run as far as it needs.
    /// Yields do not move paused time.
    ///
    /// # Arguments
    ///
    /// * `awaited_condition` - What to wait for, checked before each yield.
    ///
    /// # Panics
    ///
    /// Panics if `awaited_condition` still does not hold after 10,000 yields.
    async fn yield_until(mut awaited_condition: impl FnMut() -> bool) {
        for _ in 0..10_000 {
            if awaited_condition() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the condition does not hold after 10,000 yields");
    }

    /// Index limits under which each index slice claims one key, at most once per millisecond.
    const ONE_KEY_PER_MILLISECOND_LIMITS: RouteIndexLimits = RouteIndexLimits {
        slice_interval: Duration::from_millis(1),
        slice_budget: Duration::ZERO,
        keys_per_step: NonZeroUsize::MIN,
    };

    /// A key list of new account keys, which no message of a test carries.
    ///
    /// # Arguments
    ///
    /// * `key_count` - How many keys the list holds.
    ///
    /// # Returns
    ///
    /// The keys, sorted, as [`Filter::route_keys`] gives them.
    fn unmatched_account_keys(key_count: usize) -> Arc<[RouteKey]> {
        let mut route_keys = (0..key_count)
            .map(|_| RouteKey::Account(Pubkey::new_unique()))
            .collect::<Vec<_>>();
        route_keys.sort_unstable();
        route_keys.into()
    }

    /// An owner's queue that counts its reads. Each `select!` of the owner's loop reads its
    /// queue once, so the count says how often the loop ran.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let read_count = Arc::new(AtomicUsize::new(0));
    /// let read_counting_queue = ReadCountingQueue {
    ///     counted_queue: BatchStreamUnboundedReceiver::new(messages_rx),
    ///     read_count: Arc::clone(&read_count),
    /// };
    /// tokio::spawn(GrpcService::geyser_loop(read_counting_queue, ...));
    /// ```
    struct ReadCountingQueue<St> {
        /// The queue it reads.
        counted_queue: St,
        /// How many reads were made.
        read_count: Arc<AtomicUsize>,
    }

    impl<St: BatchStream + Unpin> BatchStream for ReadCountingQueue<St> {
        type Item = St::Item;

        /// Counts the read, then reads the counted queue.
        ///
        /// # Arguments
        ///
        /// * `task_context` - The context of the owner's task.
        /// * `batch_buffer` - Gets what the read takes in.
        ///
        /// # Returns
        ///
        /// What the counted queue reads.
        fn poll_recv_batch_with_buffer<B>(
            self: Pin<&mut Self>,
            task_context: &mut Context<'_>,
            batch_buffer: &mut B,
        ) -> Poll<Option<usize>>
        where
            B: crate::stream::Buffer<Self::Item>,
        {
            let read_counting_queue = self.get_mut();
            read_counting_queue
                .read_count
                .fetch_add(1, Ordering::Relaxed);
            Pin::new(&mut read_counting_queue.counted_queue)
                .poll_recv_batch_with_buffer(task_context, batch_buffer)
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
                plugin::message::MessageBlockMeta,
                route::{
                    tests::{allocator_calls_during, STEP_EVERY_PASS_LIMITS},
                    Route, RouteLeave, MAX_ROUTE_CHANGES_PER_PASS,
                },
                stream::tokio::BatchStreamUnboundedReceiver,
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
                BankThread::spawn().unwrap(),
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
                BankThread::spawn().unwrap(),
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
                BankThread::spawn().unwrap(),
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
                SubscriberChannels::new(1024, 1024, 1024).with_routing(RouteIndexLimits::default());
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
                BankThread::spawn().unwrap(),
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
                BankThread::spawn().unwrap(),
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
                BankThread::spawn().unwrap(),
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
                BankThread::spawn().unwrap(),
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
                BankThread::spawn().unwrap(),
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

        /// The banks block reconstruction has queued when it is behind.
        const BACKLOG_SLOTS: Range<u64> = 20..24;

        /// The four items geyser_loop hands over for one bank: its raw batch (sysvars,
        /// CreatedBank, a transaction, the entry), then its block meta and two statuses.
        ///
        /// # Arguments
        ///
        /// * `bank_slot` - The bank's slot, which is also its bank id.
        ///
        /// # Returns
        ///
        /// The items, in the order geyser_loop hands them over.
        fn handed_over_items(bank_slot: u64) -> Vec<BlockReconstructionMessage> {
            let mut raw_batch = open_slot(bank_slot);
            raw_batch.push(transaction(bank_slot, 1));
            let mut seal_gated_messages = close_slot(bank_slot, 1);
            raw_batch.push(seal_gated_messages.remove(0));
            let raw_batch_item = BlockReconstructionMessage::BroadcastBatch(Arc::new(raw_batch));
            let seal_gated_items = seal_gated_messages
                .into_iter()
                .map(BlockReconstructionMessage::SealGated);
            std::iter::once(raw_batch_item)
                .chain(seal_gated_items)
                .collect()
        }

        /// Block reconstruction's queue while it is behind: one item per read, and never an
        /// empty read while items wait. Before each item it spends the task's budget, so the loop
        /// yields with every arm `Pending`, and tokio runs the other tasks first, as other
        /// threads would.
        ///
        /// # Examples
        ///
        /// ```ignore
        /// let backlog_queue = BacklogQueue {
        ///     queue_rx,
        ///     taken_in_count: Arc::clone(&taken_in_count),
        ///     budget_spent: false,
        /// };
        /// tokio::spawn(GrpcService::block_reconstruction_loop(backlog_queue, ...));
        /// ```
        struct BacklogQueue {
            /// What geyser_loop handed over.
            queue_rx: mpsc::UnboundedReceiver<BlockReconstructionMessage>,
            /// The items taken in so far: the steps the tests count in.
            taken_in_count: Arc<AtomicUsize>,
            /// Whether the last read spent the budget instead of taking an item in.
            budget_spent: bool,
        }

        impl BatchStream for BacklogQueue {
            type Item = BlockReconstructionMessage;

            /// Spends the task's budget if items wait and the last read did not spend it, or
            /// takes in one item.
            ///
            /// # Arguments
            ///
            /// * `task_context` - The context of block reconstruction's task.
            /// * `batch_buffer` - Gets the item taken in.
            ///
            /// # Returns
            ///
            /// `Pending` once the budget is spent, `Ready(Some(1))` once an item is taken in, or
            /// what an empty or closed queue reads.
            fn poll_recv_batch_with_buffer<B>(
                self: Pin<&mut Self>,
                task_context: &mut Context<'_>,
                batch_buffer: &mut B,
            ) -> Poll<Option<usize>>
            where
                B: crate::stream::Buffer<Self::Item>,
            {
                let backlog_queue = self.get_mut();
                if !backlog_queue.queue_rx.is_empty()
                    && !std::mem::replace(&mut backlog_queue.budget_spent, true)
                {
                    while let Poll::Ready(budget_unit) =
                        tokio::task::coop::poll_proceed(task_context)
                    {
                        budget_unit.made_progress();
                    }
                    return Poll::Pending;
                }
                backlog_queue.budget_spent = false;
                match backlog_queue.queue_rx.poll_recv(task_context) {
                    Poll::Ready(Some(queued_item)) => {
                        backlog_queue.taken_in_count.fetch_add(1, Ordering::Relaxed);
                        if batch_buffer.accumulate(queued_item).is_err() {
                            unreachable!("the buffer has room for one item");
                        }
                        Poll::Ready(Some(1))
                    }
                    queue_read => queue_read.map(|_| None),
                }
            }
        }

        /// Starts block reconstruction alone on a [`BacklogQueue`] that reads `queue_rx`.
        ///
        /// # Arguments
        ///
        /// * `queue_rx` - What geyser_loop handed over.
        /// * `broadcast_channels` - The channels block reconstruction sends on.
        /// * `route_changes_rx` - Block reconstruction's route changes.
        ///
        /// # Returns
        ///
        /// The count of items block reconstruction has taken in so far.
        fn spawn_reconstruction_behind(
            queue_rx: mpsc::UnboundedReceiver<BlockReconstructionMessage>,
            broadcast_channels: SubscriberChannels,
            route_changes_rx: mpsc::UnboundedReceiver<RouteChange>,
        ) -> Arc<AtomicUsize> {
            let taken_in_count = Arc::new(AtomicUsize::new(0));
            let backlog_queue = BacklogQueue {
                queue_rx,
                taken_in_count: Arc::clone(&taken_in_count),
                budget_spent: false,
            };
            // No geyser_loop: what block reconstruction relays at processed goes nowhere.
            let (relay_tx, _) = mpsc::unbounded_channel();
            tokio::spawn(GrpcService::block_reconstruction_loop(
                backlog_queue,
                broadcast_channels,
                route_changes_rx,
                relay_tx,
                None,
                None,
                100,
                BankThread::spawn().unwrap(),
            ));
            taken_in_count
        }

        /// The broadcast channels, with routing on when `routed`.
        ///
        /// # Arguments
        ///
        /// * `routed` - Whether clients with route keys join a route, with an index slice at the
        ///   top of each pass while index work waits. Otherwise each client reads the broadcast
        ///   and filters every batch itself.
        ///
        /// # Returns
        ///
        /// The channels, and the route change receiver of block reconstruction, which gets
        /// nothing when routing is off.
        fn backlog_channels(
            routed: bool,
        ) -> (SubscriberChannels, mpsc::UnboundedReceiver<RouteChange>) {
            let broadcast_channels = SubscriberChannels::new(1024, 1024, 1024);
            if routed {
                let (broadcast_channels, [_, reconstruction_changes_rx]) =
                    broadcast_channels.with_routing(STEP_EVERY_PASS_LIMITS);
                (broadcast_channels, reconstruction_changes_rx)
            } else {
                (broadcast_channels, mpsc::unbounded_channel().1)
            }
        }

        /// Starts a client with a queue of `queue_capacity` and sends it `client_filter` as its
        /// first request.
        ///
        /// # Arguments
        ///
        /// * `broadcast_channels` - The channels, with the owners' route change senders.
        /// * `subscriber_id` - The client's subscriber id.
        /// * `queue_capacity` - The capacity of the client's Subscribe queue.
        /// * `client_filter` - The client's first filter.
        ///
        /// # Returns
        ///
        /// The client's request sender and its Subscribe queue.
        fn spawn_backlog_client(
            broadcast_channels: SubscriberChannels,
            subscriber_id: &str,
            queue_capacity: usize,
            client_filter: Filter,
        ) -> ClientHandles {
            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, stream_rx) =
                load_aware_channel(queue_capacity, WeightScales::default());
            let client_session = ClientSession::new(
                0,
                Some(subscriber_id.into()),
                "test".into(),
                CancellationToken::new(),
                None,
            );
            tokio::spawn(GrpcService::client_loop(
                client_session,
                stream_tx,
                client_rx,
                None,
                broadcast_channels,
                None,
                TaskTracker::new(),
                None,
            ));
            client_tx.send(Some((None, client_filter))).unwrap();
            (client_tx, stream_rx)
        }

        /// What a client got: each update as a short name, or the error status that closed its
        /// stream, with the count of items block reconstruction had taken in when it arrived.
        type SteppedUpdates = Vec<(usize, String)>;

        /// Reads the client's stream until `last_slot` is confirmed or the stream ends.
        ///
        /// # Arguments
        ///
        /// * `stream_rx` - The client's Subscribe queue.
        /// * `taken_in_count` - The count of items block reconstruction has taken in.
        /// * `last_slot` - The slot whose Confirmed status ends the read.
        ///
        /// # Returns
        ///
        /// Each update read, as a short name, with the step it arrived at.
        ///
        /// # Panics
        ///
        /// Panics if no update arrives within 5 s.
        async fn read_stepped_updates(
            stream_rx: &mut LoadAwareReceiver<TonicResult<FilteredUpdate>>,
            taken_in_count: &AtomicUsize,
            last_slot: u64,
        ) -> SteppedUpdates {
            let mut stepped_updates = SteppedUpdates::new();
            while let Some(queued_item) =
                tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                    .await
                    .expect("timed out waiting for an update")
            {
                let update_name = match queued_item.map(|filtered_update| filtered_update.message) {
                    Ok(FilteredUpdateOneof::Transaction(transaction_update)) => {
                        format!("tx {}", transaction_update.slot)
                    }
                    Ok(FilteredUpdateOneof::Slot(slot_update)) => {
                        format!("{:?} {}", slot_update.status, slot_update.slot)
                    }
                    Ok(other_update) => format!("{other_update:?}"),
                    Err(error_status) => error_status.message().to_owned(),
                };
                let is_last_update = update_name == format!("Confirmed {last_slot}");
                stepped_updates.push((taken_in_count.load(Ordering::Relaxed), update_name));
                if is_last_update {
                    break;
                }
            }
            stepped_updates
        }

        /// The names in `stepped_updates`.
        ///
        /// # Arguments
        ///
        /// * `stepped_updates` - What a client got.
        ///
        /// # Returns
        ///
        /// Each update's short name, in order.
        fn update_names(stepped_updates: &SteppedUpdates) -> Vec<&str> {
            stepped_updates
                .iter()
                .map(|(_, update_name)| update_name.as_str())
                .collect()
        }

        /// The steps in `stepped_updates`.
        ///
        /// # Arguments
        ///
        /// * `stepped_updates` - What a client got.
        ///
        /// # Returns
        ///
        /// The count of items block reconstruction had taken in when each update arrived.
        fn update_steps(stepped_updates: &SteppedUpdates) -> Vec<usize> {
            stepped_updates
                .iter()
                .map(|(taken_in_step, _)| *taken_in_step)
                .collect()
        }

        /// What a confirmed client gets of each bank: CreatedBank, the transaction, Processed
        /// and Confirmed.
        ///
        /// # Arguments
        ///
        /// * `bank_slots` - The banks, in order.
        ///
        /// # Returns
        ///
        /// The short names of the updates, bank by bank.
        fn confirmed_bank_updates(bank_slots: Range<u64>) -> Vec<String> {
            let bank_updates = |bank_slot| {
                ["CreatedBank", "tx", "Processed", "Confirmed"]
                    .map(|update_kind| format!("{update_kind} {bank_slot}"))
            };
            bank_slots.flat_map(bank_updates).collect()
        }

        /// A confirmed client with a queue of 8 subscribes while block reconstruction has the
        /// [`BACKLOG_SLOTS`] banks queued.
        ///
        /// # Arguments
        ///
        /// * `routed` - Whether the client joins a route or reads the broadcast.
        /// * `subscriber_id` - The client's subscriber id.
        ///
        /// # Returns
        ///
        /// What the client got until the last bank was confirmed.
        ///
        /// # Panics
        ///
        /// Panics if a routed client still holds its receiver once the last bank is confirmed,
        /// or as [`read_stepped_updates`] does.
        async fn subscribe_behind_backlog(routed: bool, subscriber_id: &str) -> SteppedUpdates {
            let (broadcast_channels, route_changes_rx) = backlog_channels(routed);
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            for handed_over_item in BACKLOG_SLOTS.flat_map(handed_over_items) {
                queue_tx.send(handed_over_item).unwrap();
            }
            let client_filter = create_filter(CommitmentLevelProto::Confirmed);
            let (_client_tx, mut stream_rx) =
                spawn_backlog_client(broadcast_channels.clone(), subscriber_id, 8, client_filter);
            // The client subscribes, and sends its preparation, before block reconstruction runs.
            settle().await;
            let reconstruction_channels = broadcast_channels.clone();
            let taken_in_count =
                spawn_reconstruction_behind(queue_rx, reconstruction_channels, route_changes_rx);
            let last_slot = BACKLOG_SLOTS.end - 1;
            let stepped_updates =
                read_stepped_updates(&mut stream_rx, &taken_in_count, last_slot).await;
            if routed {
                let confirmed_sender = broadcast_channels.sender(CommitmentLevel::Confirmed);
                assert_eq!(
                    confirmed_sender.receiver_count(),
                    0,
                    "the client handed over"
                );
            }
            // A closed queue would end block reconstruction before it reads the queue empty.
            drop(queue_tx);
            stepped_updates
        }

        /// A confirmed client with a queue of 8 joins its route while block reconstruction has
        /// 16 updates for it queued. Block reconstruction answers its preparation in the first
        /// pass, and takes its hand-over in before it sends anything at confirmed. So the client
        /// gets each bank through its route as block reconstruction takes it in, as its twin on
        /// the broadcast does, and its queue never fills.
        ///
        /// ```text
        /// items taken:  0         1      4     8     12    16
        /// twin:                          b20   b21   b22   b23
        /// routed:       Prepare   Join   b20   b21   b22   b23
        ///
        /// bN = bank N's CreatedBank, transaction, Processed and Confirmed, which block
        ///      reconstruction sends at confirmed when it takes in the bank's fourth item
        /// ```
        #[tokio::test]
        async fn a_routed_client_behind_a_backlog_keeps_up_as_its_twin_does() {
            const SUBSCRIBER_ID: &str = "keeps-up";
            let routed_updates = subscribe_behind_backlog(true, SUBSCRIBER_ID).await;
            let twin_updates = subscribe_behind_backlog(false, "keeps-up-twin").await;
            assert_eq!(
                update_names(&twin_updates),
                confirmed_bank_updates(BACKLOG_SLOTS)
            );
            assert_eq!(
                update_steps(&twin_updates),
                [4, 4, 4, 4, 8, 8, 8, 8, 12, 12, 12, 12, 16, 16, 16, 16]
            );
            assert_eq!(routed_updates, twin_updates);
            assert_eq!(
                metrics::client_disconnect_count(SUBSCRIBER_ID, "client_channel_full"),
                0
            );
        }

        /// Block reconstruction takes in at most 64 queued route changes per pass, and waits
        /// neither for the queue to read empty nor for tokio's budget: of 1,000 leaves queued
        /// behind a backlog, 64 are answered before it takes in its first item, and 64 more
        /// before each next item.
        #[tokio::test]
        async fn block_reconstruction_takes_in_at_most_64_route_changes_per_pass() {
            let (broadcast_channels, route_changes_rx) = backlog_channels(true);
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            for handed_over_item in BACKLOG_SLOTS.flat_map(handed_over_items) {
                queue_tx.send(handed_over_item).unwrap();
            }
            let commitment_level = CommitmentLevel::Confirmed;
            let mut answer_rxs: Vec<_> = (0..1000)
                .map(|session_id| {
                    let (answer_tx, answer_rx) = oneshot::channel();
                    let client_leave = RouteLeave {
                        session_id,
                        receiver_request: Some((commitment_level, answer_tx)),
                    };
                    let route_change = RouteChange::Leave(client_leave);
                    broadcast_channels.send_route_change(commitment_level, route_change);
                    answer_rx
                })
                .collect();
            let taken_in_count =
                spawn_reconstruction_behind(queue_rx, broadcast_channels, route_changes_rx);
            let mut answered_count = 0;
            let mut answered_by_taken_in = Vec::new();
            while answered_count < answer_rxs.len() {
                // Block reconstruction runs one pass, and yields before it takes in an item.
                tokio::task::yield_now().await;
                while let Some(answer_rx) = answer_rxs.get_mut(answered_count) {
                    if answer_rx.try_recv().is_err() {
                        break;
                    }
                    answered_count += 1;
                }
                let taken_in = taken_in_count.load(Ordering::Relaxed);
                answered_by_taken_in.push((taken_in, answered_count));
            }
            let expected_answered_by_taken_in = (0..=15)
                .map(|taken_in| (taken_in, (64 * (taken_in + 1)).min(1000)))
                .collect::<Vec<_>>();
            assert_eq!(answered_by_taken_in, expected_answered_by_taken_in);
            drop(queue_tx);
        }

        /// A preparation at confirmed whose own key list, filter and queue the test holds.
        ///
        /// # Arguments
        ///
        /// * `route_keys` - The route's key list.
        ///
        /// # Returns
        ///
        /// The route change, and the receiver the owner answers on.
        fn confirmed_preparation(
            route_keys: &Arc<[RouteKey]>,
        ) -> (RouteChange, oneshot::Receiver<()>) {
            let (stream_tx, _stream_rx) = load_aware_channel(16, WeightScales::default());
            let route_template = Route::new(1, "test", &stream_tx, &CancellationToken::new());
            let confirmed_filter = Arc::new(create_filter(CommitmentLevelProto::Confirmed));
            let prepared_route = route_template.with_filter(&confirmed_filter, route_keys);
            let (ready_tx, ready_rx) = oneshot::channel();
            (RouteChange::Prepare(prepared_route, ready_tx), ready_rx)
        }

        /// Starts block reconstruction alone on `queue_rx`, with no replay channel.
        ///
        /// # Arguments
        ///
        /// * `queue_rx` - What geyser_loop hands over.
        /// * `broadcast_channels` - The channels block reconstruction sends on.
        /// * `route_changes_rx` - Block reconstruction's route changes.
        /// * `relay_tx` - The relay the test reads in geyser_loop's place.
        ///
        /// # Returns
        ///
        /// The handle of block reconstruction's task, which ends once `queue_rx` closes.
        fn spawn_reconstruction_alone(
            queue_rx: mpsc::UnboundedReceiver<BlockReconstructionMessage>,
            broadcast_channels: SubscriberChannels,
            route_changes_rx: mpsc::UnboundedReceiver<RouteChange>,
            relay_tx: mpsc::UnboundedSender<RelayedItem>,
        ) -> tokio::task::JoinHandle<()> {
            tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(queue_rx),
                broadcast_channels,
                route_changes_rx,
                relay_tx,
                None,
                None,
                100,
                BankThread::spawn().unwrap(),
            ))
        }

        /// Stands in for geyser_loop's waker on the relay. When the relay wakes it, it records
        /// how many batches the confirmed channel holds.
        ///
        /// # Examples
        ///
        /// ```ignore
        /// let relay_wake_probe = Arc::new(RelayWakeProbe {
        ///     confirmed_rx: StdMutex::new(broadcast_channels.subscribe(CommitmentLevel::Confirmed)),
        ///     confirmed_len_at_wake: StdMutex::new(None),
        /// });
        /// let probe_waker = Waker::from(Arc::clone(&relay_wake_probe));
        /// assert!(relay_rx.poll_recv(&mut Context::from_waker(&probe_waker)).is_pending());
        /// // Block reconstruction relays, and the probe records the confirmed receiver's length.
        /// ```
        struct RelayWakeProbe {
            /// A receiver on the confirmed channel, which reads nothing.
            confirmed_rx: StdMutex<broadcast::Receiver<BroadcastedMessage>>,
            /// How many batches `confirmed_rx` held at the first wake.
            confirmed_len_at_wake: StdMutex<Option<usize>>,
        }

        impl std::task::Wake for RelayWakeProbe {
            /// Records the confirmed receiver's length, as [`RelayWakeProbe::wake_by_ref`] does.
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }

            /// Records how many batches the confirmed receiver holds, at the first wake only.
            fn wake_by_ref(self: &Arc<Self>) {
                let confirmed_len = self.confirmed_rx.lock().unwrap().len();
                let mut confirmed_len_at_wake = self.confirmed_len_at_wake.lock().unwrap();
                confirmed_len_at_wake.get_or_insert(confirmed_len);
            }
        }

        /// A pass that seals a bank at processed relays before its confirmed sends, and lets
        /// geyser_loop run before them. Bank 20 seals and is confirmed in one pass. A task in
        /// geyser_loop's place reads the relay while the confirmed channel holds none of the four
        /// batches the pass sends there, and they all come after it.
        #[tokio::test]
        async fn a_pass_that_seals_a_bank_at_processed_relays_before_its_confirmed_sends() {
            let (broadcast_channels, [_, reconstruction_changes_rx]) =
                SubscriberChannels::new(1024, 1024, 1024).with_routing(RouteIndexLimits::default());
            let confirmed_rx = broadcast_channels.subscribe(CommitmentLevel::Confirmed);
            let (relay_tx, mut relay_rx) = mpsc::unbounded_channel();
            // Spawned first, so it waits on the relay before block reconstruction runs.
            let relay_reader_handle = tokio::spawn(async move {
                let relayed_item = relay_rx.recv().await;
                (relayed_item, confirmed_rx.len(), confirmed_rx)
            });
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            for handed_over_item in handed_over_items(20) {
                queue_tx.send(handed_over_item).unwrap();
            }
            let reconstruction_handle = spawn_reconstruction_alone(
                queue_rx,
                broadcast_channels,
                reconstruction_changes_rx,
                relay_tx,
            );
            let (relayed_item, confirmed_len_at_relay_read, confirmed_rx) =
                relay_reader_handle.await.unwrap();
            assert!(matches!(relayed_item, Some(RelayedItem::Batch(_))));
            assert_eq!(confirmed_len_at_relay_read, 0);
            // CreatedBank, the bank's data, its block and its statuses.
            yield_until(|| confirmed_rx.len() == 4).await;
            drop(queue_tx);
            let join_result = tokio::time::timeout(Duration::from_secs(5), reconstruction_handle);
            join_result
                .await
                .expect("block reconstruction ends")
                .unwrap();
        }

        /// A pass that only confirms a bank relays to geyser_loop after its confirmed sends.
        /// Bank 20 seals at processed in one pass, and its Confirmed status comes in a later pass.
        /// When that pass's relay of the status wakes geyser_loop, whose waker a probe stands in
        /// for, the confirmed channel already holds all four batches the pass sends there. So
        /// geyser_loop never waits for them in the worker's LIFO slot.
        #[tokio::test]
        async fn a_pass_that_only_confirms_a_bank_relays_after_its_confirmed_sends() {
            let (broadcast_channels, [_, reconstruction_changes_rx]) =
                SubscriberChannels::new(1024, 1024, 1024).with_routing(RouteIndexLimits::default());
            let confirmed_rx = broadcast_channels.subscribe(CommitmentLevel::Confirmed);
            let (relay_tx, mut relay_rx) = mpsc::unbounded_channel();
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            let mut handed_over_items = handed_over_items(20);
            let confirmed_status_item = handed_over_items.pop().unwrap();
            for handed_over_item in handed_over_items {
                queue_tx.send(handed_over_item).unwrap();
            }
            let reconstruction_handle = spawn_reconstruction_alone(
                queue_rx,
                broadcast_channels,
                reconstruction_changes_rx,
                relay_tx,
            );
            // The block and its Processed status.
            for _ in 0..2 {
                assert!(matches!(relay_rx.recv().await, Some(RelayedItem::Batch(_))));
            }
            assert!(confirmed_rx.is_empty());
            let relay_wake_probe = Arc::new(RelayWakeProbe {
                confirmed_rx: StdMutex::new(confirmed_rx),
                confirmed_len_at_wake: StdMutex::new(None),
            });
            let probe_waker = std::task::Waker::from(Arc::clone(&relay_wake_probe));
            let next_relay_read = relay_rx.poll_recv(&mut Context::from_waker(&probe_waker));
            assert!(next_relay_read.is_pending());
            queue_tx.send(confirmed_status_item).unwrap();
            let confirmed_len_at_wake = &relay_wake_probe.confirmed_len_at_wake;
            yield_until(|| confirmed_len_at_wake.lock().unwrap().is_some()).await;
            // CreatedBank, the bank's data, its block and its statuses.
            assert_eq!(*confirmed_len_at_wake.lock().unwrap(), Some(4));
            assert!(matches!(relay_rx.try_recv(), Ok(RelayedItem::Batch(_))));
            drop(queue_tx);
            let join_result = tokio::time::timeout(Duration::from_secs(5), reconstruction_handle);
            join_result
                .await
                .expect("block reconstruction ends")
                .unwrap();
        }

        /// Block reconstruction lets geyser_loop run right after a pass that relayed to it, before
        /// it takes in another route change. Of 65 queued leaves, 64 go in at the top of the pass
        /// that seals bank 20 and relays. The last, with a receiver request, is still unanswered
        /// when a task in geyser_loop's place reads the relay. The test body is the runtime's
        /// `block_on` future, which runs only once no task is ready, so a spawned task reads it.
        ///
        /// ```text
        /// block reconstruction:  64 leaves  bank 20  relay  .           65th leave
        /// relay reader:                                     relay read
        /// ```
        #[tokio::test]
        async fn block_reconstruction_lets_geyser_loop_run_right_after_it_relays() {
            let (broadcast_channels, [_, reconstruction_changes_rx]) =
                SubscriberChannels::new(1024, 1024, 1024).with_routing(RouteIndexLimits::default());
            let commitment_level = CommitmentLevel::Confirmed;
            for session_id in 0..MAX_ROUTE_CHANGES_PER_PASS {
                let client_leave = RouteLeave {
                    session_id,
                    receiver_request: None,
                };
                let route_change = RouteChange::Leave(client_leave);
                broadcast_channels.send_route_change(commitment_level, route_change);
            }
            let (answer_tx, mut answer_rx) = oneshot::channel();
            let last_leave = RouteLeave {
                session_id: MAX_ROUTE_CHANGES_PER_PASS,
                receiver_request: Some((commitment_level, answer_tx)),
            };
            broadcast_channels.send_route_change(commitment_level, RouteChange::Leave(last_leave));
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            for handed_over_item in handed_over_items(20) {
                queue_tx.send(handed_over_item).unwrap();
            }
            let (relay_tx, mut relay_rx) = mpsc::unbounded_channel();
            // Spawned first, so it waits on the relay before block reconstruction runs.
            let relay_reader_handle = tokio::spawn(async move {
                let relayed_item = relay_rx.recv().await;
                let answer_at_relay_read = answer_rx.try_recv();
                (relayed_item, answer_at_relay_read.is_ok(), answer_rx)
            });
            let reconstruction_handle = spawn_reconstruction_alone(
                queue_rx,
                broadcast_channels,
                reconstruction_changes_rx,
                relay_tx,
            );
            let (relayed_item, answered_at_relay_read, mut answer_rx) =
                relay_reader_handle.await.unwrap();
            assert!(matches!(relayed_item, Some(RelayedItem::Batch(_))));
            assert!(!answered_at_relay_read);
            let mut answered = false;
            yield_until(|| {
                answered = answered || answer_rx.try_recv().is_ok();
                answered
            })
            .await;
            drop(queue_tx);
            let join_result = tokio::time::timeout(Duration::from_secs(5), reconstruction_handle);
            join_result
                .await
                .expect("block reconstruction ends")
                .unwrap();
        }

        /// A pass that relays only at its end lets geyser_loop run before block reconstruction's
        /// next pass. Bank 20 seals at processed, and block reconstruction waits. Then a leave
        /// with a receiver request and the bank's Confirmed status arrive. The status's pass
        /// relays last, and the leave is still unanswered when a task in geyser_loop's place
        /// reads that relay. The test body is the runtime's `block_on` future, which runs only
        /// once no task is ready, so a spawned task reads it.
        ///
        /// ```text
        /// block reconstruction:  bank 20  .  Confirmed  relay  .           leave
        /// relay reader:                                        relay read
        /// ```
        #[tokio::test]
        async fn a_pass_that_relays_at_its_end_lets_geyser_loop_run_before_the_next_pass() {
            let (broadcast_channels, [_, reconstruction_changes_rx]) =
                SubscriberChannels::new(1024, 1024, 1024).with_routing(RouteIndexLimits::default());
            let (relay_tx, mut relay_rx) = mpsc::unbounded_channel();
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            let mut handed_over_items = handed_over_items(20);
            let confirmed_status_item = handed_over_items.pop().unwrap();
            for handed_over_item in handed_over_items {
                queue_tx.send(handed_over_item).unwrap();
            }
            let reconstruction_handle = spawn_reconstruction_alone(
                queue_rx,
                broadcast_channels.clone(),
                reconstruction_changes_rx,
                relay_tx,
            );
            // The block and its Processed status, then block reconstruction waits.
            for _ in 0..2 {
                assert!(matches!(relay_rx.recv().await, Some(RelayedItem::Batch(_))));
            }
            settle().await;
            let (answer_tx, mut answer_rx) = oneshot::channel();
            let client_leave = RouteLeave {
                session_id: 1,
                receiver_request: Some((CommitmentLevel::Confirmed, answer_tx)),
            };
            let route_change = RouteChange::Leave(client_leave);
            broadcast_channels.send_route_change(CommitmentLevel::Confirmed, route_change);
            let relay_reader_handle = tokio::spawn(async move {
                let relayed_item = relay_rx.recv().await;
                let answer_at_relay_read = answer_rx.try_recv();
                (relayed_item, answer_at_relay_read.is_ok(), answer_rx)
            });
            queue_tx.send(confirmed_status_item).unwrap();
            let (relayed_item, answered_at_relay_read, mut answer_rx) =
                relay_reader_handle.await.unwrap();
            assert!(matches!(relayed_item, Some(RelayedItem::Batch(_))));
            assert!(!answered_at_relay_read);
            let mut answered = false;
            yield_until(|| {
                answered = answered || answer_rx.try_recv().is_ok();
                answered
            })
            .await;
            drop(queue_tx);
            let join_result = tokio::time::timeout(Duration::from_secs(5), reconstruction_handle);
            join_result
                .await
                .expect("block reconstruction ends")
                .unwrap();
        }

        /// Block reconstruction takes in its whole queue of 20,000 transactions before it builds a
        /// replay, while an 8,192-key preparation waits: the index slice timer reads `Pending`
        /// like the queue when the budget is spent, so the preparation is still unanswered when
        /// the replay arrives. A spawned task reads the relay, so it reads the replay before
        /// block reconstruction's next pass. A slice answers the preparation afterwards, and block
        /// reconstruction ends when its queue closes.
        #[tokio::test(start_paused = true)]
        async fn a_replay_takes_in_the_whole_queue_while_index_work_waits() {
            const QUEUED_TRANSACTION_COUNT: u32 = 20_000;
            let (broadcast_channels, [_, reconstruction_changes_rx]) =
                SubscriberChannels::new(1, 1, 1).with_routing(RouteIndexLimits::default());
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let (relay_tx, mut relay_rx) = mpsc::unbounded_channel();
            let open_slot_batch = Arc::new(open_slot(PARTIAL_SLOT));
            queue_tx
                .send(BlockReconstructionMessage::BroadcastBatch(open_slot_batch))
                .unwrap();
            for transaction_number in 0..QUEUED_TRANSACTION_COUNT {
                let mut signature_bytes = [0u8; 64];
                signature_bytes[..4].copy_from_slice(&transaction_number.to_le_bytes());
                let mut message_transaction = fixtures::message_transaction(
                    Signature::from(signature_bytes),
                    vec![Pubkey::default()],
                    false,
                    Default::default(),
                );
                let transaction_info = Arc::get_mut(&mut message_transaction).unwrap();
                transaction_info.slot = PARTIAL_SLOT;
                transaction_info.bank_id = PARTIAL_SLOT;
                let transaction_batch = Arc::new(vec![Message::Transaction(message_transaction)]);
                queue_tx
                    .send(BlockReconstructionMessage::BroadcastBatch(
                        transaction_batch,
                    ))
                    .unwrap();
            }
            let mut route_keys = (0..8192)
                .map(|_| RouteKey::Account(Pubkey::new_unique()))
                .collect::<Vec<_>>();
            route_keys.sort_unstable();
            let (route_change, mut ready_rx) = confirmed_preparation(&route_keys.into());
            broadcast_channels.send_route_change(CommitmentLevel::Confirmed, route_change);
            let (response_tx, _response_rx) = oneshot::channel();
            replay_tx
                .send((CommitmentLevel::Processed, PARTIAL_SLOT, response_tx))
                .await
                .unwrap();
            // One item per read, and the budget spent before each, as in a long backlog.
            let backlog_queue = BacklogQueue {
                queue_rx,
                taken_in_count: Arc::new(AtomicUsize::new(0)),
                budget_spent: false,
            };
            // Spawned first, so it waits on the relay before block reconstruction runs.
            let relay_reader_handle = tokio::spawn(async move {
                let relayed_item = tokio::time::timeout(Duration::from_secs(5), relay_rx.recv())
                    .await
                    .expect("no replay within 5s");
                let answer_at_relay_read = ready_rx.try_recv();
                (relayed_item, answer_at_relay_read, ready_rx)
            });
            let reconstruction_handle = tokio::spawn(GrpcService::block_reconstruction_loop(
                backlog_queue,
                broadcast_channels,
                reconstruction_changes_rx,
                relay_tx,
                Some(replay_rx),
                None,
                150,
                BankThread::spawn().unwrap(),
            ));
            let (relayed_item, answer_at_relay_read, mut ready_rx) =
                relay_reader_handle.await.unwrap();
            let Some(RelayedItem::Answer(_, ReplayedResponse::Messages(replay))) = relayed_item
            else {
                panic!("replay failed");
            };
            assert_eq!(
                answer_at_relay_read,
                Err(oneshot::error::TryRecvError::Empty)
            );
            let replayed_transaction_count = replay
                .messages
                .iter()
                .flatten()
                .filter(|replayed_message| matches!(replayed_message, Message::Transaction(_)))
                .count();
            assert_eq!(
                replayed_transaction_count,
                QUEUED_TRANSACTION_COUNT as usize
            );
            tokio::time::timeout(Duration::from_secs(5), &mut ready_rx)
                .await
                .expect("a slice answers the preparation")
                .expect("the preparation is answered");
            drop(queue_tx);
            let join_result = tokio::time::timeout(Duration::from_secs(5), reconstruction_handle);
            join_result
                .await
                .expect("block reconstruction ends")
                .unwrap();
        }

        /// While index work waits and its queue reads empty, block reconstruction runs one index
        /// slice per slice interval, with the limits its channels carry: a confirmed preparation
        /// of 4 keys, claimed one key per slice, is answered by the fourth slice, 3 ms after the
        /// first.
        #[tokio::test(start_paused = true)]
        async fn block_reconstruction_runs_one_idle_index_slice_per_slice_interval() {
            let (broadcast_channels, [_, reconstruction_changes_rx]) =
                SubscriberChannels::new(16, 16, 16).with_routing(ONE_KEY_PER_MILLISECOND_LIMITS);
            let (route_change, ready_rx) = confirmed_preparation(&unmatched_account_keys(4));
            broadcast_channels.send_route_change(CommitmentLevel::Confirmed, route_change);
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            let started_at = tokio::time::Instant::now();
            spawn_reconstruction_behind(queue_rx, broadcast_channels, reconstruction_changes_rx);
            tokio::time::timeout(Duration::from_secs(5), ready_rx)
                .await
                .expect("the slices answer the preparation")
                .expect("the preparation is answered");
            assert_eq!(started_at.elapsed(), Duration::from_millis(3));
            drop(queue_tx);
        }

        /// Block reconstruction wakes for its index slice timer only while index work waits:
        /// with no route change, a second passes with one read of its queue, and once a
        /// preparation is answered, the timer wakes it at most once more.
        #[tokio::test(start_paused = true)]
        async fn block_reconstruction_wakes_for_its_timer_only_while_index_work_waits() {
            let (broadcast_channels, [_, reconstruction_changes_rx]) =
                SubscriberChannels::new(16, 16, 16).with_routing(RouteIndexLimits::default());
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            let read_count = Arc::new(AtomicUsize::new(0));
            let read_counting_queue = ReadCountingQueue {
                counted_queue: BatchStreamUnboundedReceiver::new(queue_rx),
                read_count: Arc::clone(&read_count),
            };
            tokio::spawn(GrpcService::block_reconstruction_loop(
                read_counting_queue,
                broadcast_channels.clone(),
                reconstruction_changes_rx,
                mpsc::unbounded_channel().0,
                None,
                None,
                100,
                BankThread::spawn().unwrap(),
            ));
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(read_count.load(Ordering::Relaxed), 1);
            let (route_change, ready_rx) = confirmed_preparation(&unmatched_account_keys(1));
            broadcast_channels.send_route_change(CommitmentLevel::Confirmed, route_change);
            tokio::time::timeout(Duration::from_secs(5), ready_rx)
                .await
                .expect("an idle slice answers the preparation")
                .expect("the preparation is answered");
            let reads_at_answer = read_count.load(Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(read_count.load(Ordering::Relaxed) <= reads_at_answer + 2);
            drop(queue_tx);
        }

        /// Block reconstruction runs an overdue index slice at the top of a pass while its queue
        /// never reads empty: a preparation taken in at the start is answered once it has waited
        /// ten slice intervals, with most of the backlog still queued.
        #[tokio::test(start_paused = true)]
        async fn block_reconstruction_runs_an_overdue_index_slice_behind_a_backlog() {
            const BACKLOG_TRANSACTION_COUNT: u8 = 40;
            let (broadcast_channels, [_, reconstruction_changes_rx]) =
                SubscriberChannels::new(16, 16, 16).with_routing(RouteIndexLimits::default());
            let (route_change, mut ready_rx) = confirmed_preparation(&unmatched_account_keys(1));
            broadcast_channels.send_route_change(CommitmentLevel::Confirmed, route_change);
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            let open_slot_batch = Arc::new(open_slot(PARTIAL_SLOT));
            queue_tx
                .send(BlockReconstructionMessage::BroadcastBatch(open_slot_batch))
                .unwrap();
            for transaction_number in 0..BACKLOG_TRANSACTION_COUNT {
                let transaction_batch =
                    Arc::new(vec![transaction(PARTIAL_SLOT, transaction_number)]);
                queue_tx
                    .send(BlockReconstructionMessage::BroadcastBatch(
                        transaction_batch,
                    ))
                    .unwrap();
            }
            let started_at = tokio::time::Instant::now();
            let taken_in_count = spawn_reconstruction_behind(
                queue_rx,
                broadcast_channels,
                reconstruction_changes_rx,
            );
            // Block reconstruction takes the preparation in, then spends its budget.
            tokio::task::yield_now().await;
            let mut answered_after = None;
            while answered_after.is_none() && started_at.elapsed() < Duration::from_millis(20) {
                tokio::time::advance(Duration::from_millis(1)).await;
                if ready_rx.try_recv().is_ok() {
                    answered_after = Some(started_at.elapsed());
                }
            }
            assert_eq!(answered_after, Some(Duration::from_millis(10)));
            let backlog_item_count = usize::from(BACKLOG_TRANSACTION_COUNT) + 1;
            assert!(taken_in_count.load(Ordering::Relaxed) < backlog_item_count / 2);
            drop(queue_tx);
        }

        /// A routed confirmed client ends while block reconstruction is behind. Its last Leave
        /// lands at the top of the next pass and drops the route's queue sender, so the stream
        /// ends there, and nothing is routed to the client after it ended.
        ///
        /// ```text
        /// items taken:  0             1
        /// client:       ends          stream ends
        /// ```
        #[tokio::test]
        async fn a_routed_client_that_ends_behind_a_backlog_ends_its_stream_at_the_next_pass() {
            let (broadcast_channels, route_changes_rx) = backlog_channels(true);
            let (queue_tx, queue_rx) = mpsc::unbounded_channel();
            let taken_in_count =
                spawn_reconstruction_behind(queue_rx, broadcast_channels.clone(), route_changes_rx);
            let client_filter = create_filter(CommitmentLevelProto::Confirmed);
            let (client_tx, mut stream_rx) =
                spawn_backlog_client(broadcast_channels.clone(), "ends", 1024, client_filter);
            // Block reconstruction is idle: the preparation is answered and the hand-over lands.
            let confirmed_sender = broadcast_channels.sender(CommitmentLevel::Confirmed);
            settle().await;
            yield_until(|| confirmed_sender.receiver_count() == 0).await;

            for handed_over_item in BACKLOG_SLOTS.flat_map(handed_over_items) {
                queue_tx.send(handed_over_item).unwrap();
            }
            drop(client_tx);
            let stream_end = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv()).await;
            assert!(stream_end.expect("the stream ends").is_none());
            assert_eq!(taken_in_count.load(Ordering::Relaxed), 1);
        }

        /// Block reconstruction hands each slot it evicts, and each bank it seals, to the
        /// [`BankThread`] with no allocator call of its own, from the first eviction on. Dropping
        /// the bank thread joins it once it has freed the evicted banks and indexed the sealed
        /// ones, and the slots kept for replay stay.
        #[test]
        fn evicted_and_sealed_banks_go_to_the_bank_thread_without_allocating() {
            let mut block_machine = BlockMachineStorage::new(2);
            let bank_thread = BankThread::spawn().unwrap();
            let mut finalized_banks = Vec::new();
            let mut handed_over_slots = 0;
            for bank_slot in 100..116 {
                let mut bank_messages = open_slot(bank_slot);
                bank_messages.push(transaction(bank_slot, 1));
                bank_messages.extend(close_slot(bank_slot, 1));
                bank_messages.push(slot_message(bank_slot, SlotStatus::Finalized));
                for bank_message in bank_messages {
                    block_machine.add(bank_message);
                }
                while let Some((slot_update, frozen_bank)) = block_machine.pop_ready_block() {
                    let finalized_commitment = solana_commitment_config::CommitmentLevel::Finalized;
                    if slot_update.commitment == finalized_commitment {
                        finalized_banks.push(Arc::downgrade(&frozen_bank));
                    }
                }
                let allocator_calls = allocator_calls_during(|| {
                    for evicted_slot_banks in block_machine.drain_evicted_banks() {
                        bank_thread.free_evicted_banks(evicted_slot_banks);
                        handed_over_slots += 1;
                    }
                    for sealed_bank in block_machine.drain_sealed_banks() {
                        bank_thread.index_route_keys(sealed_bank);
                    }
                });
                assert_eq!(allocator_calls, 0, "slot {bank_slot}");
            }
            // A slot is evicted once more than ten kept slots are finalized: 100 to 105.
            assert_eq!(handed_over_slots, 6);
            drop(bank_thread);
            let kept_banks = finalized_banks
                .iter()
                .map(|finalized_bank| finalized_bank.upgrade().is_some());
            assert!(kept_banks.eq([false; 6].into_iter().chain([true; 10])));
            let kept_banks_indexed = finalized_banks[6..].iter().all(|finalized_bank| {
                let kept_bank = finalized_bank.upgrade().expect("a kept bank");
                kept_bank.route_key_index().is_some()
            });
            assert!(kept_banks_indexed);
        }

        /// A hand-over to a bank thread whose channel is full frees the banks where it is made,
        /// at once, and does not wait for the bank thread.
        #[test]
        fn a_hand_over_to_a_full_bank_thread_frees_the_banks_in_place() {
            let mut block_machine = BlockMachineStorage::new(2);
            let mut bank_messages = open_slot(REPLAYED_SLOT);
            bank_messages.push(transaction(REPLAYED_SLOT, 1));
            bank_messages.extend(close_slot(REPLAYED_SLOT, 1));
            for bank_message in bank_messages {
                block_machine.add(bank_message);
            }
            let (_, frozen_bank) = block_machine.pop_ready_block().expect("a sealed bank");
            drop(block_machine);
            assert_eq!(Arc::strong_count(&frozen_bank), 1);
            let bank_handle = Arc::downgrade(&frozen_bank);
            // A channel of no slots, which no thread reads, is full for every hand-over.
            let (bank_chores_tx, _bank_chores_rx) = crossbeam_channel::bounded(0);
            let full_bank_thread = BankThread {
                bank_chores_tx: Some(bank_chores_tx),
                chore_thread: None,
            };
            full_bank_thread.free_evicted_banks(vec![frozen_bank]);
            assert!(bank_handle.upgrade().is_none());
        }
    }

    mod geyser_loop_routing {
        use {
            super::{
                super::*, answer_replay, settle, unmatched_account_keys, yield_until,
                ClientHandles, ReadCountingQueue, SubscribeRequestAccountsDataSlice,
                SubscribeRequestFilterSlots, ONE_KEY_PER_MILLISECOND_LIMITS,
            },
            crate::{
                block_reconstruction_v2::MUST_HAVE_SYSVAR_ACCOUNTS,
                plugin::{
                    convert_to,
                    filter::fixtures,
                    message::{
                        CommitmentLevel::Processed, MessageBlockMeta, MessageDeshredTransaction,
                        MessageDeshredTransactionInfo, MessageSlot, SlotStatus,
                    },
                },
                route::{tests::STEP_EVERY_PASS_LIMITS, Route},
                stream::tokio::BatchStreamUnboundedReceiver,
            },
            foldhash::{HashSet as FoldHashSet, HashSetExt as _},
            futures::FutureExt,
            prost_types::Timestamp,
            solana_hash::Hash,
            solana_message::{legacy::Message as SolMessage, MessageHeader},
            solana_pubkey::Pubkey,
            solana_signature::Signature,
            solana_transaction::{versioned::VersionedTransaction, Transaction},
            std::{sync::Arc, time::SystemTime},
            tokio::sync::{broadcast, mpsc},
            yellowstone_grpc_proto::prelude::{
                SubscribeRequestFilterAccounts, SubscribeUpdateBlockMeta,
            },
        };

        /// The geyser_loop and block reconstruction tasks, spawned with routing on, and the
        /// channels a test drives them through.
        ///
        /// # Examples
        ///
        /// ```ignore
        /// let mut harness = spawn_loop();
        /// let base_receiver_counts = harness.receiver_counts();
        /// let (client_tx, mut stream_rx) = harness.spawn_client(client_filter);
        /// yield_until(|| harness.receiver_counts() == base_receiver_counts).await;
        /// harness.send_messages(finalized_bank(10));
        /// let received_updates = read_through(&mut stream_rx, 10, SlotStatus::Finalized).await;
        /// ```
        struct Harness {
            messages_tx: mpsc::UnboundedSender<Message>,
            #[allow(dead_code)]
            broadcast_rx: broadcast::Receiver<BroadcastedMessage>,
            deshred_tx: broadcast::Sender<DeshredBroadcastedMessage>,
            deshred_rx: broadcast::Receiver<DeshredBroadcastedMessage>,
            handle: tokio::task::JoinHandle<()>,
            handle_reconstruction: tokio::task::JoinHandle<()>,
            /// The broadcast channels the loops send on, with routing on.
            broadcast_channels: SubscriberChannels,
            /// The session id that [`Harness::spawn_client`] gave last.
            last_session_id: usize,
        }

        impl Harness {
            /// Feeds `plugin_messages` to geyser_loop, as the plugin's callbacks do.
            ///
            /// # Arguments
            ///
            /// * `plugin_messages` - The messages, in order.
            ///
            /// # Panics
            ///
            /// Panics if geyser_loop has stopped.
            fn send_messages(&self, plugin_messages: impl IntoIterator<Item = Message>) {
                for plugin_message in plugin_messages {
                    self.messages_tx.send(plugin_message).unwrap();
                }
            }

            /// How many receivers the `commitment_level` broadcast channel has.
            ///
            /// # Arguments
            ///
            /// * `commitment_level` - The channel's commitment level.
            ///
            /// # Returns
            ///
            /// The channel's receiver count. A routed client holds no receiver.
            fn receiver_count(&self, commitment_level: CommitmentLevel) -> usize {
                self.broadcast_channels
                    .sender(commitment_level)
                    .receiver_count()
            }

            /// How many receivers each broadcast channel has. Once every client with route keys
            /// is routed, only the clients on the broadcast hold one.
            ///
            /// # Returns
            ///
            /// The receiver counts of the processed, confirmed and finalized channels.
            fn receiver_counts(&self) -> [usize; 3] {
                [
                    CommitmentLevel::Processed,
                    CommitmentLevel::Confirmed,
                    CommitmentLevel::Finalized,
                ]
                .map(|commitment_level| self.receiver_count(commitment_level))
            }

            /// Spawns a client_loop with the next session id, and sends it `client_filter` with no
            /// `from_slot`.
            ///
            /// # Arguments
            ///
            /// * `client_filter` - The client's first filter.
            ///
            /// # Returns
            ///
            /// The client's request sender and its Subscribe queue.
            fn spawn_client(&mut self, client_filter: Filter) -> ClientHandles {
                self.last_session_id += 1;
                let session_id = self.last_session_id;
                spawn_client_with_session_id(&self.broadcast_channels, session_id, client_filter)
            }
        }

        /// Spawns a client_loop with `session_id` and a Subscribe queue of 1,024 on
        /// `broadcast_channels`, and sends it `client_filter` with no `from_slot`.
        ///
        /// # Arguments
        ///
        /// * `broadcast_channels` - The channels the client subscribes to and sends its route
        ///   changes on.
        /// * `session_id` - The client's session id.
        /// * `client_filter` - The client's first filter.
        ///
        /// # Returns
        ///
        /// The client's request sender and its Subscribe queue.
        fn spawn_client_with_session_id(
            broadcast_channels: &SubscriberChannels,
            session_id: usize,
            client_filter: Filter,
        ) -> ClientHandles {
            let client_session = ClientSession::new(
                session_id,
                Some("test".into()),
                "test".into(),
                CancellationToken::new(),
                None,
            );
            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, stream_rx) = load_aware_channel(1024, WeightScales::default());
            tokio::spawn(GrpcService::client_loop(
                client_session,
                stream_tx,
                client_rx,
                None,
                broadcast_channels.clone(),
                None,
                TaskTracker::new(),
                None,
            ));
            client_tx.send(Some((None, client_filter))).unwrap();
            (client_tx, stream_rx)
        }

        fn spawn_loop() -> Harness {
            spawn_loop_with_replay(None)
        }

        /// Spawns geyser_loop and block reconstruction with routing on, and an index slice at
        /// the top of each pass while index work waits.
        ///
        /// # Arguments
        ///
        /// * `replay_rx` - Block reconstruction's replay requests, or [`None`] when replay is off.
        ///
        /// # Returns
        ///
        /// The [`Harness`] that drives the two loops.
        fn spawn_loop_with_replay(
            replay_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        ) -> Harness {
            spawn_loop_with(replay_rx, STEP_EVERY_PASS_LIMITS)
        }

        /// Spawns geyser_loop and block reconstruction with routing on.
        ///
        /// # Arguments
        ///
        /// * `replay_rx` - Block reconstruction's replay requests, or [`None`] when replay is off.
        /// * `route_index_limits` - How much index work each owner does, and how often.
        ///
        /// # Returns
        ///
        /// The [`Harness`] that drives the two loops.
        fn spawn_loop_with(
            replay_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
            route_index_limits: RouteIndexLimits,
        ) -> Harness {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
            let (relay_tx, relay_rx) = mpsc::unbounded_channel();
            let (broadcast, [geyser_loop_changes_rx, reconstruction_changes_rx]) =
                SubscriberChannels::new(1024, 1024, 1024).with_routing(route_index_limits);
            let broadcast_rx = broadcast.subscribe(CommitmentLevel::Processed);
            let (deshred_tx, deshred_rx) = broadcast::channel(1024);
            let messages_rx = BatchStreamUnboundedReceiver::new(messages_rx);
            let handle = {
                let broadcast = broadcast.clone();
                tokio::spawn(GrpcService::geyser_loop(
                    messages_rx,
                    broadcast,
                    geyser_loop_changes_rx,
                    relay_rx,
                    block_reconstruction_tx,
                ))
            };
            let handle_reconstruction = tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast.clone(),
                reconstruction_changes_rx,
                relay_tx,
                replay_rx,
                None,
                100,
                BankThread::spawn().unwrap(),
            ));
            Harness {
                messages_tx,
                broadcast_rx,
                deshred_tx,
                deshred_rx,
                handle,
                handle_reconstruction,
                broadcast_channels: broadcast,
                last_session_id: 0,
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

        /// geyser_loop sends at most 1024 messages per batch, and each batch it sends holds no
        /// spare capacity: its buffer keeps the capacity for the next batch.
        #[tokio::test]
        async fn geyser_loop_sends_exact_size_batches_of_at_most_1024_messages() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, _block_reconstruction_rx) = mpsc::unbounded_channel();
            let broadcast_channels = SubscriberChannels::new(16, 16, 16);
            let mut processed_rx = broadcast_channels.subscribe(CommitmentLevel::Processed);
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast_channels,
                mpsc::unbounded_channel().1,
                mpsc::unbounded_channel().1,
                block_reconstruction_tx,
            ));
            for shred_slot in 0..2500 {
                let shred_status = make_slot(shred_slot, SlotStatus::FirstShredReceived, None);
                messages_tx.send(shred_status).unwrap();
            }
            let mut batch_lengths = Vec::new();
            while batch_lengths.iter().sum::<usize>() < 2500 {
                let message_batch =
                    tokio::time::timeout(Duration::from_secs(5), processed_rx.recv())
                        .await
                        .expect("timed out waiting for a batch")
                        .expect("broadcast open");
                assert_eq!(message_batch.capacity(), message_batch.len());
                batch_lengths.push(message_batch.len());
            }
            assert_eq!(batch_lengths, [1024, 1024, 452]);
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

        /// Waits until the Processed status of bank `bank_id` goes out on the processed channel.
        ///
        /// # Arguments
        ///
        /// * `broadcast_rx` - A receiver of the processed channel.
        /// * `bank_id` - The bank whose Processed status to wait for.
        ///
        /// # Panics
        ///
        /// Panics if the channel closes, or the status does not go out within 5 s.
        async fn wait_for_processed_status(
            broadcast_rx: &mut broadcast::Receiver<BroadcastedMessage>,
            bank_id: u64,
        ) {
            let is_bank_processed_status = |broadcast_message: &Message| match broadcast_message {
                Message::Slot(slot_message) => {
                    let is_processed = slot_message.status == SlotStatus::Processed;
                    is_processed && slot_message.bank_id == Some(bank_id)
                }
                _ => false,
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let broadcast_batch = broadcast_rx.recv().await.expect("broadcast open");
                    if broadcast_batch.iter().any(is_bank_processed_status) {
                        break;
                    }
                }
            })
            .await
            .expect("the bank never went out at processed");
        }

        /// Asks block reconstruction for processed replays from `from_slot` until one holds the
        /// two [`ReplayResponseMessageType::SealedBank`] parts of a bank with their index. They
        /// carry it once the bank thread has indexed the bank.
        ///
        /// # Arguments
        ///
        /// * `replay_tx` - Block reconstruction's replay requests.
        /// * `from_slot` - The slot of the bank, the one sealed bank the replay holds.
        ///
        /// # Returns
        ///
        /// The two parts, each as its bank's messages, its range and the bank's index.
        ///
        /// # Panics
        ///
        /// Panics if a replay fails, or no replay holds both parts with an index within 5 s.
        async fn indexed_sealed_bank_parts(
            replay_tx: &mpsc::Sender<ReplayStoredSlotsRequest>,
            from_slot: u64,
        ) -> Vec<(Arc<Vec<Message>>, Range<usize>, Arc<BankRouteKeyIndex>)> {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let (response_tx, response_rx) = oneshot::channel();
                    replay_tx
                        .send((CommitmentLevel::Processed, from_slot, response_tx))
                        .await
                        .unwrap();
                    let Ok(ReplayedResponse::Messages(bank_replay)) = response_rx.await else {
                        panic!("replay failed");
                    };
                    let sealed_bank_parts: Vec<_> = bank_replay
                        .messages
                        .into_iter()
                        .filter_map(|replay_item| match replay_item {
                            ReplayResponseMessageType::SealedBank(
                                bank_messages,
                                bank_range,
                                Some(route_key_index),
                            ) => Some((bank_messages, bank_range, route_key_index)),
                            _ => None,
                        })
                        .collect();
                    if sealed_bank_parts.len() == 2 {
                        break sealed_bank_parts;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("no replay held the bank with its route key index")
        }

        /// A replay holds a sealed bank as two [`ReplayResponseMessageType::SealedBank`] parts
        /// around its CreatedBank, and once the bank thread has indexed the bank, both parts
        /// carry the one index, which finds the bank's entry under [`RouteKey::Entries`].
        #[tokio::test]
        async fn block_reconstruction_hands_a_replay_each_sealed_bank_with_its_route_key_index() {
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let mut replay_harness = spawn_loop_with_replay(Some(replay_rx));
            let mut bank_messages = sealed_bank(10, 2);
            bank_messages.push(bank_status(10, SlotStatus::Processed, Some(2)));
            replay_harness.send_messages(bank_messages);
            wait_for_processed_status(&mut replay_harness.broadcast_rx, 2).await;
            let indexed_bank_parts = indexed_sealed_bank_parts(&replay_tx, 10).await;

            let [first_part, second_part] = &indexed_bank_parts[..] else {
                unreachable!("two parts");
            };
            let (first_messages, first_range, first_index) = first_part;
            let (second_messages, second_range, second_index) = second_part;
            assert!(Arc::ptr_eq(first_messages, second_messages));
            assert!(Arc::ptr_eq(first_index, second_index));
            assert_eq!(first_range.end, second_range.start);
            assert_eq!(second_range.end, first_messages.len());
            let entry_position = first_messages
                .iter()
                .position(|bank_message| matches!(bank_message, Message::Entry(_)))
                .expect("the bank's entry");
            let entries_route_key_hash = BankRouteKeyIndex::route_key_hash(&RouteKey::Entries);
            let mut candidate_positions = Vec::new();
            first_index
                .collect_candidate_positions(&[entries_route_key_hash], &mut candidate_positions);
            assert_eq!(candidate_positions, [entry_position as u32]);

            drop(replay_harness.messages_tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), replay_harness.handle).await;
            let reconstruction_handle = replay_harness.handle_reconstruction;
            let _ = tokio::time::timeout(Duration::from_secs(1), reconstruction_handle).await;
        }

        /// A routed client replays a bank that the bank thread has indexed. It gets what its twin
        /// gets: one of the bank's four accounts and its entry, between the bank's statuses. The
        /// twin has no route keys, so its replay scans the whole bank.
        #[tokio::test]
        async fn a_routed_replay_of_an_indexed_bank_gets_what_its_full_scan_twin_gets() {
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let mut replay_harness = spawn_loop_with_replay(Some(replay_rx));
            let mut bank_messages = sealed_bank(10, 2);
            bank_messages.push(bank_status(10, SlotStatus::Processed, Some(2)));
            replay_harness.send_messages(bank_messages);
            wait_for_processed_status(&mut replay_harness.broadcast_rx, 2).await;
            indexed_sealed_bank_parts(&replay_tx, 10).await;

            let [found_pubkey, ..] = MUST_HAVE_SYSVAR_ACCOUNTS;
            let slots_filter = SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            };
            let mut subscribe_request = accounts_request(&[found_pubkey], &[]);
            subscribe_request.slots = HashMap::from([("s".into(), slots_filter)]);
            subscribe_request.entry = HashMap::from([("e".into(), Default::default())]);
            let routed_filter = fixtures::filter_from_request(&subscribe_request);
            subscribe_request.transactions = HashMap::from([("t".into(), Default::default())]);
            let full_scan_filter = fixtures::filter_from_request(&subscribe_request);
            assert!(routed_filter.route_keys().is_some());
            assert!(full_scan_filter.route_keys().is_none());

            let mut replayed_streams = Vec::new();
            for (session_id, client_filter) in [(1, routed_filter), (2, full_scan_filter)] {
                let client_session = ClientSession::new(
                    session_id,
                    Some("test".into()),
                    "test".into(),
                    CancellationToken::new(),
                    None,
                );
                let (client_tx, client_rx) = mpsc::unbounded_channel();
                let (stream_tx, mut stream_rx) = load_aware_channel(1024, WeightScales::default());
                tokio::spawn(GrpcService::client_loop(
                    client_session,
                    stream_tx,
                    client_rx,
                    None,
                    replay_harness.broadcast_channels.clone(),
                    Some(replay_tx.clone()),
                    TaskTracker::new(),
                    None,
                ));
                client_tx.send(Some((Some(10), client_filter))).unwrap();
                replayed_streams
                    .push(read_through(&mut stream_rx, 10, SlotStatus::Processed).await);
            }
            // A replay stamps its statuses when it is built, so updates compare by name.
            let update_names = |replayed_updates: &[FilteredUpdate]| -> Vec<String> {
                replayed_updates
                    .iter()
                    .map(|replayed_update| match &replayed_update.message {
                        FilteredUpdateOneof::Slot(slot_update) => {
                            format!("{:?} {}", slot_update.status, slot_update.slot)
                        }
                        FilteredUpdateOneof::Account(account_update) => {
                            format!("account {}", account_update.account.account.pubkey)
                        }
                        FilteredUpdateOneof::Entry(_) => "entry".to_owned(),
                        other_update => format!("{other_update:?}"),
                    })
                    .collect()
            };
            let [routed_names, full_scan_names] = [&replayed_streams[0], &replayed_streams[1]]
                .map(|replayed_updates| update_names(replayed_updates));
            assert_eq!(routed_names, full_scan_names);
            let found_account_name = format!("account {found_pubkey}");
            let expected_names = [
                "CreatedBank 10",
                found_account_name.as_str(),
                "entry",
                "Processed 10",
            ];
            assert_eq!(routed_names, expected_names);

            drop(replay_harness.messages_tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), replay_harness.handle).await;
            let reconstruction_handle = replay_harness.handle_reconstruction;
            let _ = tokio::time::timeout(Duration::from_secs(1), reconstruction_handle).await;
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

        /// Each commitment level, with the commitment status a bank gets when it reaches that
        /// level.
        const LEVEL_STATUSES: [(CommitmentLevelProto, SlotStatus); 3] = [
            (CommitmentLevelProto::Processed, SlotStatus::Processed),
            (CommitmentLevelProto::Confirmed, SlotStatus::Confirmed),
            (CommitmentLevelProto::Finalized, SlotStatus::Finalized),
        ];

        /// A bank that seals and reaches every commitment level.
        ///
        /// # Arguments
        ///
        /// * `bank_slot` - The bank's slot, which is also its bank id.
        ///
        /// # Returns
        ///
        /// The messages of [`sealed_bank`], then the bank's Processed, Confirmed and Finalized
        /// statuses.
        fn finalized_bank(bank_slot: u64) -> Vec<Message> {
            let mut bank_messages = sealed_bank(bank_slot, bank_slot);
            bank_messages.extend(LEVEL_STATUSES.map(|(_, commitment_status)| {
                bank_status(bank_slot, commitment_status, Some(bank_slot))
            }));
            bank_messages
        }

        /// One filter per commitment level, each with `slots_filter` only.
        ///
        /// # Arguments
        ///
        /// * `slots_filter` - The slots filter of each request.
        ///
        /// # Returns
        ///
        /// The processed, confirmed and finalized filters, in that order.
        fn level_filters(slots_filter: SubscribeRequestFilterSlots) -> [Filter; 3] {
            let mut subscribe_request = SubscribeRequest {
                slots: HashMap::from([("test".into(), slots_filter)]),
                ..Default::default()
            };
            LEVEL_STATUSES.map(|(proto_commitment, _)| {
                subscribe_request.commitment = Some(proto_commitment as i32);
                fixtures::filter_from_request(&subscribe_request)
            })
        }

        /// A routed filter at `proto_commitment`, and its twin, which also has a transactions
        /// filter and so stays on the broadcast.
        ///
        /// # Arguments
        ///
        /// * `proto_commitment` - The commitment level of both filters.
        ///
        /// # Returns
        ///
        /// The routed filter, then its twin. Both make the same updates from a stream without
        /// transactions.
        fn twin_filters(proto_commitment: CommitmentLevelProto) -> [Filter; 2] {
            let slots_filter = SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            };
            let mut subscribe_request = SubscribeRequest {
                slots: HashMap::from([("s".into(), slots_filter)]),
                entry: HashMap::from([("e".into(), Default::default())]),
                blocks_meta: HashMap::from([("m".into(), Default::default())]),
                commitment: Some(proto_commitment as i32),
                ..Default::default()
            };
            let routed_filter = fixtures::filter_from_request(&subscribe_request);
            subscribe_request.transactions = HashMap::from([("t".into(), Default::default())]);
            let twin_filter = fixtures::filter_from_request(&subscribe_request);
            [routed_filter, twin_filter]
        }

        /// Reads `stream_rx` up to and including the slot update for `last_slot` at
        /// `last_status`.
        ///
        /// # Arguments
        ///
        /// * `stream_rx` - The client's Subscribe queue.
        /// * `last_slot` - The slot of the last update to read.
        /// * `last_status` - The status of the last update to read.
        ///
        /// # Returns
        ///
        /// The updates read, in order.
        ///
        /// # Panics
        ///
        /// Panics if no update arrives within 5 s, the queue closes, or the queue yields an
        /// error status.
        async fn read_through(
            stream_rx: &mut LoadAwareReceiver<TonicResult<FilteredUpdate>>,
            last_slot: u64,
            last_status: SlotStatus,
        ) -> Vec<FilteredUpdate> {
            let mut received_updates = Vec::new();
            loop {
                let next_update = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                    .await
                    .expect("timed out waiting for an update")
                    .expect("stream closed")
                    .expect("status error");
                let is_last_update = matches!(
                    &next_update.message,
                    FilteredUpdateOneof::Slot(slot_update)
                        if (slot_update.slot, slot_update.status) == (last_slot, last_status)
                );
                received_updates.push(next_update);
                if is_last_update {
                    return received_updates;
                }
            }
        }

        /// The slot and status of each update that [`read_through`] reads.
        ///
        /// # Arguments
        ///
        /// * `stream_rx` - The client's Subscribe queue.
        /// * `last_slot` - The slot of the last update to read.
        /// * `last_status` - The status of the last update to read.
        ///
        /// # Returns
        ///
        /// Each update's slot and status, with [`None`] as the status of a block meta.
        ///
        /// # Panics
        ///
        /// Panics as [`read_through`] does, or on an update that is neither a slot update nor a
        /// block meta.
        async fn statuses_through(
            stream_rx: &mut LoadAwareReceiver<TonicResult<FilteredUpdate>>,
            last_slot: u64,
            last_status: SlotStatus,
        ) -> Vec<(u64, Option<SlotStatus>)> {
            let received_updates = read_through(stream_rx, last_slot, last_status).await;
            let slot_and_status = |received_update: FilteredUpdate| match received_update.message {
                FilteredUpdateOneof::Slot(slot_update) => {
                    (slot_update.slot, Some(slot_update.status))
                }
                FilteredUpdateOneof::BlockMeta(block_meta) => (block_meta.slot, None),
                unexpected_update => panic!("unexpected update {unexpected_update:?}"),
            };
            received_updates.into_iter().map(slot_and_status).collect()
        }

        /// Routed clients at every level, and one that moves from processed to finalized, each
        /// get a bank's lifecycle statuses, then its commitment statuses, in order.
        #[tokio::test]
        async fn each_level_gets_the_commitment_statuses_after_the_lifecycle_statuses() {
            let mut harness = spawn_loop();
            let base_receiver_counts = harness.receiver_counts();
            let interslot_filter = SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            };
            let mut level_clients = level_filters(interslot_filter)
                .map(|level_filter| harness.spawn_client(level_filter));
            let [processed_filter, _, finalized_filter] = level_filters(interslot_filter);
            let mut moved_client = harness.spawn_client(processed_filter);
            // The counts read the base before the clients run too, so settle first.
            settle().await;
            yield_until(|| harness.receiver_counts() == base_receiver_counts).await;
            moved_client.0.send(Some((None, finalized_filter))).unwrap();
            settle().await;
            yield_until(|| harness.receiver_counts() == base_receiver_counts).await;
            let shred_statuses = [SlotStatus::FirstShredReceived, SlotStatus::Completed];
            harness.send_messages(
                shred_statuses.map(|shred_status| make_slot(10, shred_status, Some(9))),
            );
            harness.send_messages(finalized_bank(10));
            let expected_statuses = [
                SlotStatus::FirstShredReceived,
                SlotStatus::Completed,
                SlotStatus::CreatedBank,
                SlotStatus::Processed,
                SlotStatus::Confirmed,
                SlotStatus::Finalized,
            ]
            .map(|expected_status| (10, Some(expected_status)));
            for (_, stream_rx) in level_clients.iter_mut().chain([&mut moved_client]) {
                assert_eq!(
                    statuses_through(stream_rx, 10, SlotStatus::Finalized).await,
                    expected_statuses
                );
            }
        }

        /// A client that switches at random between a routed filter and its twin on the
        /// broadcast gets what a client with the twin filter gets, and so does a client that
        /// stays routed, at every level.
        #[tokio::test]
        async fn a_client_switching_between_routed_and_broadcast_gets_what_its_twin_gets() {
            for rng_seed in 0..30 {
                let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
                let (proto_commitment, _) = LEVEL_STATUSES[rng_seed as usize % 3];
                let commitment_level = CommitmentLevel::from(proto_commitment);
                let [routed_filter, twin_filter] = twin_filters(proto_commitment);
                let mut harness = spawn_loop();
                let initial_receiver_count = harness.receiver_count(commitment_level);
                let mut compared_clients = [&twin_filter, &routed_filter, &routed_filter]
                    .map(|client_filter| harness.spawn_client(client_filter.clone()));
                let routed_receiver_count = initial_receiver_count + 1;
                yield_until(|| harness.receiver_count(commitment_level) == routed_receiver_count)
                    .await;
                assert_eq!(
                    harness.receiver_count(commitment_level),
                    initial_receiver_count + 1
                );
                let (switching_client_tx, _) = &compared_clients[2];
                for plugin_message in (10..20).flat_map(finalized_bank) {
                    if seeded_rng.u8(..3) == 0 {
                        let next_filter =
                            [&routed_filter, &twin_filter][seeded_rng.usize(..2)].clone();
                        switching_client_tx.send(Some((None, next_filter))).unwrap();
                    }
                    harness.send_messages([plugin_message]);
                    tokio::task::yield_now().await;
                }
                let mut received_streams = Vec::new();
                for (_, stream_rx) in &mut compared_clients {
                    received_streams.push(read_through(stream_rx, 19, SlotStatus::Finalized).await);
                }
                assert_eq!(received_streams[1], received_streams[0], "seed {rng_seed}");
                assert_eq!(received_streams[2], received_streams[0], "seed {rng_seed}");
                for (client_tx, mut stream_rx) in compared_clients {
                    drop(client_tx);
                    let next_item = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv());
                    assert!(next_item.await.expect("the queue closes").is_none());
                }
                drop(harness.messages_tx);
                for loop_handle in [harness.handle, harness.handle_reconstruction] {
                    let join_result =
                        tokio::time::timeout(Duration::from_secs(5), loop_handle).await;
                    join_result.expect("the loop ends").unwrap();
                }
            }
        }

        /// A routed client whose later request repeats its filter with a `from_slot` replaces
        /// its route's filter in place, keeps the key list, and leaves nothing.
        #[tokio::test]
        async fn a_routed_client_ignores_a_later_from_slot_and_keeps_its_route() {
            let (broadcast_channels, [mut spied_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let mut spy_owner = OwnerRoutes::new(broadcast_channels.clone());
            let (client_tx, _stream_rx) =
                super::spawn_client_loop(broadcast_channels, CancellationToken::new());
            let routed_request = accounts_request(&[Pubkey::new_unique()], &[]);
            let routed_filter = || fixtures::filter_from_request(&routed_request);
            client_tx.send(Some((None, routed_filter()))).unwrap();
            settle().await;
            let spied_changes = take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
            assert_eq!(spied_changes, ["prepare"]);
            run_index_work(&mut spy_owner);
            settle().await;
            let spied_changes = take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
            assert_eq!(spied_changes, ["hand-over"]);
            client_tx.send(Some((Some(10), routed_filter()))).unwrap();
            settle().await;
            let spied_changes = take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
            assert_eq!(spied_changes, ["refilter"]);
            // The refilter keeps the route's key list Arc, so the owner swaps the filter in place.
            assert!(!spy_owner.has_index_work());
        }

        /// A routed client whose queue fills ends with the status and the disconnect reason of
        /// a full queue.
        #[tokio::test]
        async fn a_full_routed_client_ends_with_client_channel_full() {
            const SUBSCRIBER_ID: &str = "full-routed-client";
            let harness = spawn_loop();
            let base_receiver_counts = harness.receiver_counts();
            let cancellation_token = CancellationToken::new();
            let client_session = ClientSession::new(
                0,
                Some(SUBSCRIBER_ID.into()),
                "test".into(),
                cancellation_token,
                None,
            );
            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, mut stream_rx) = load_aware_channel(4, WeightScales::default());
            tokio::spawn(GrpcService::client_loop(
                client_session,
                stream_tx,
                client_rx,
                None,
                harness.broadcast_channels.clone(),
                None,
                TaskTracker::new(),
                None,
            ));
            let [routed_filter, _] = twin_filters(CommitmentLevelProto::Processed);
            client_tx.send(Some((None, routed_filter))).unwrap();
            settle().await;
            yield_until(|| harness.receiver_counts() == base_receiver_counts).await;
            harness.send_messages(
                (1..=5)
                    .map(|shred_slot| make_slot(shred_slot, SlotStatus::FirstShredReceived, None)),
            );
            let mut received_items = Vec::new();
            while let Some(next_item) =
                tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                    .await
                    .expect("the queue closes")
            {
                received_items
                    .push(next_item.map_err(|error_status| error_status.message().to_owned()));
            }
            assert_eq!(received_items.len(), 5);
            assert_eq!(
                received_items[4],
                Err("lagged to send an update".to_owned())
            );
            assert_eq!(
                metrics::client_disconnect_count(SUBSCRIBER_ID, "client_channel_full"),
                1
            );
        }

        /// An update of the account `account_pubkey`, with `account_data`, owned by the default
        /// pubkey.
        ///
        /// # Arguments
        ///
        /// * `account_pubkey` - The account's pubkey.
        /// * `account_data` - The account's data.
        ///
        /// # Returns
        ///
        /// The [`Message::Account`].
        fn account_message(account_pubkey: Pubkey, account_data: Vec<u8>) -> Message {
            let account_info =
                fixtures::account_info(account_pubkey, Pubkey::default(), account_data, 1, None);
            Message::Account(fixtures::message_account(account_info))
        }

        /// A request with one accounts filter, named `a`, for `account_pubkeys`.
        ///
        /// # Arguments
        ///
        /// * `account_pubkeys` - The accounts the filter lists, which are its route keys.
        /// * `data_slices` - The request's account data slices, which leave the route keys alone.
        ///
        /// # Returns
        ///
        /// The request.
        fn accounts_request(
            account_pubkeys: &[Pubkey],
            data_slices: &[SubscribeRequestAccountsDataSlice],
        ) -> SubscribeRequest {
            let accounts_filter = SubscribeRequestFilterAccounts {
                account: account_pubkeys.iter().map(Pubkey::to_string).collect(),
                ..Default::default()
            };
            SubscribeRequest {
                accounts: HashMap::from([("a".into(), accounts_filter)]),
                accounts_data_slice: data_slices.to_vec(),
                ..Default::default()
            }
        }

        /// A routed filter for `account_pubkeys`, and its twin, which also has a transactions
        /// filter and so stays on the broadcast.
        ///
        /// # Arguments
        ///
        /// * `account_pubkeys` - The accounts both filters list.
        ///
        /// # Returns
        ///
        /// The routed filter, then its twin. Both make the same updates from a stream without
        /// transactions.
        fn account_twin_filters(account_pubkeys: &[Pubkey]) -> [Filter; 2] {
            let mut subscribe_request = accounts_request(account_pubkeys, &[]);
            let routed_filter = fixtures::filter_from_request(&subscribe_request);
            subscribe_request.transactions = HashMap::from([("t".into(), Default::default())]);
            [
                routed_filter,
                fixtures::filter_from_request(&subscribe_request),
            ]
        }

        /// Reads the next `update_count` updates of a client.
        ///
        /// # Arguments
        ///
        /// * `stream_rx` - The client's Subscribe queue.
        /// * `update_count` - How many updates to read.
        ///
        /// # Returns
        ///
        /// The updates, in order.
        ///
        /// # Panics
        ///
        /// Panics if an update does not arrive within 5 s, the queue closes, or the queue yields
        /// an error status.
        async fn read_updates(
            stream_rx: &mut LoadAwareReceiver<TonicResult<FilteredUpdate>>,
            update_count: usize,
        ) -> Vec<FilteredUpdate> {
            let mut received_updates = Vec::with_capacity(update_count);
            for _ in 0..update_count {
                let next_update = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                    .await
                    .expect("timed out waiting for an update")
                    .expect("stream closed")
                    .expect("status error");
                received_updates.push(next_update);
            }
            received_updates
        }

        /// The pubkeys of the accounts that `received_updates` update.
        ///
        /// # Arguments
        ///
        /// * `received_updates` - Account updates.
        ///
        /// # Returns
        ///
        /// Each update's account pubkey, in order.
        ///
        /// # Panics
        ///
        /// Panics on an update that is not an account update.
        fn updated_account_pubkeys(received_updates: &[FilteredUpdate]) -> Vec<Pubkey> {
            let updated_account_pubkey = |received_update: &FilteredUpdate| {
                let FilteredUpdateOneof::Account(account_update) = &received_update.message else {
                    panic!("an account update");
                };
                account_update.account.account.pubkey
            };
            received_updates
                .iter()
                .map(updated_account_pubkey)
                .collect()
        }

        /// Takes in, at `spy_owner`, every route change queued in `spied_changes_rx`, as the
        /// owner it stands in for would.
        ///
        /// # Arguments
        ///
        /// * `spied_changes_rx` - The route change receiver a test holds in place of an owner.
        /// * `spy_owner` - The routes that stand in for the owner.
        ///
        /// # Returns
        ///
        /// The kind of each route change, in order: `prepare`, `hand-over` (a join with a
        /// receiver that reads empty), `hand-over with a backlog`, `refilter` (a join without a
        /// receiver), `leave for a receiver` or `leave`.
        fn take_in_spied_route_changes(
            spied_changes_rx: &mut mpsc::UnboundedReceiver<RouteChange>,
            spy_owner: &mut OwnerRoutes,
        ) -> Vec<&'static str> {
            let mut change_kinds = Vec::new();
            while let Ok(route_change) = spied_changes_rx.try_recv() {
                change_kinds.push(match &route_change {
                    RouteChange::Prepare(..) => "prepare",
                    RouteChange::Join(_, Some(handed_over_rx)) if handed_over_rx.is_empty() => {
                        "hand-over"
                    }
                    RouteChange::Join(_, Some(_)) => "hand-over with a backlog",
                    RouteChange::Join(_, None) => "refilter",
                    RouteChange::Leave(client_leave) if client_leave.receiver_request.is_some() => {
                        "leave for a receiver"
                    }
                    RouteChange::Leave(_) => "leave",
                });
                spy_owner.take_route_change(route_change);
            }
            change_kinds
        }

        /// Runs the index work of `spy_owner` to the end, which answers every preparation.
        ///
        /// # Arguments
        ///
        /// * `spy_owner` - The routes that stand in for an owner.
        fn run_index_work(spy_owner: &mut OwnerRoutes) {
            while spy_owner.has_index_work() {
                spy_owner.run_index_slice();
            }
        }

        /// Forwards the route changes of `route_changes_rx` to geyser_loop in order, and holds
        /// the first hand-over until geyser_loop has broadcast `window_message` into the
        /// handed-over receiver. The owner must then deliver it through the drain.
        ///
        /// # Arguments
        ///
        /// * `route_changes_rx` - The route changes the clients send to geyser_loop.
        /// * `messages_tx` - geyser_loop's plugin messages.
        /// * `window_message` - The message geyser_loop broadcasts while the hand-over waits.
        ///
        /// # Returns
        ///
        /// The receiver geyser_loop takes the route changes from.
        fn hold_the_first_hand_over(
            mut route_changes_rx: mpsc::UnboundedReceiver<RouteChange>,
            messages_tx: mpsc::UnboundedSender<Message>,
            window_message: Message,
        ) -> mpsc::UnboundedReceiver<RouteChange> {
            let (forwarded_changes_tx, forwarded_changes_rx) = mpsc::unbounded_channel();
            tokio::spawn(async move {
                let mut window_message = Some(window_message);
                while let Some(route_change) = route_changes_rx.recv().await {
                    if let RouteChange::Join(_, Some(handed_over_rx)) = &route_change {
                        if let Some(window_message) = window_message.take() {
                            messages_tx.send(window_message).unwrap();
                            yield_until(|| !handed_over_rx.is_empty()).await;
                        }
                    }
                    if forwarded_changes_tx.send(route_change).is_err() {
                        return;
                    }
                }
            });
            forwarded_changes_rx
        }

        /// A client whose filter has 1,000 route keys reads its broadcast while geyser_loop
        /// claims one key per millisecond. It gets what its twin on the broadcast gets, batch by
        /// batch from the first batch on, and holds its receiver until the owner answers, 999 ms
        /// after the first slice. A batch broadcast between its hand-over and the take-in comes
        /// through the drain.
        ///
        /// ```text
        /// time (ms):  0    100  ...  900  999                        1000  ...  1400
        /// receiver:   b0   b1   ...  b9   w, after the Join is sent
        /// route:                          w, drained at the take-in  b10   ...  b14
        ///
        /// bN = batch N, read by the client itself until the answer
        /// w  = the window batch the test's forwarder holds the hand-over for
        /// ```
        #[tokio::test(start_paused = true)]
        async fn a_joining_client_reads_its_broadcast_until_its_route_is_prepared() {
            const ACCOUNT_COUNT: usize = 1000;
            const BATCH_INTERVAL: Duration = Duration::from_millis(100);
            // One key per slice, one slice per millisecond, the first slice at 0 ms.
            const ANSWERED_AT: Duration = Duration::from_millis(999);
            let started_at = Instant::now();
            let account_pubkeys = (0..ACCOUNT_COUNT)
                .map(|_| Pubkey::new_unique())
                .collect::<Vec<_>>();
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, _block_reconstruction_rx) = mpsc::unbounded_channel();
            let (broadcast_channels, [geyser_loop_changes_rx, _]) =
                SubscriberChannels::new(1024, 1024, 1024)
                    .with_routing(ONE_KEY_PER_MILLISECOND_LIMITS);
            let window_message = account_message(account_pubkeys[ACCOUNT_COUNT - 1], Vec::new());
            let forwarded_changes_rx = hold_the_first_hand_over(
                geyser_loop_changes_rx,
                messages_tx.clone(),
                window_message,
            );
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast_channels.clone(),
                forwarded_changes_rx,
                mpsc::unbounded_channel().1,
                block_reconstruction_tx,
            ));
            let processed_receiver_count = || {
                broadcast_channels
                    .sender(CommitmentLevel::Processed)
                    .receiver_count()
            };
            let [routed_filter, twin_filter] = account_twin_filters(&account_pubkeys);
            let (_twin_tx, mut twin_rx) =
                spawn_client_with_session_id(&broadcast_channels, 1, twin_filter);
            let (_client_tx, mut client_rx) =
                spawn_client_with_session_id(&broadcast_channels, 2, routed_filter);
            settle().await;
            for batch_number in 0..15 {
                let batch_at = BATCH_INTERVAL * batch_number;
                sleep_until(started_at + batch_at).await;
                if batch_number == 10 {
                    let window_updates = read_updates(&mut twin_rx, 1).await;
                    assert_eq!(read_updates(&mut client_rx, 1).await, window_updates);
                }
                let receiver_count = if batch_at < ANSWERED_AT { 2 } else { 1 };
                assert_eq!(processed_receiver_count(), receiver_count, "{batch_at:?}");
                let batch_number = batch_number as usize;
                let batch_accounts = [batch_number, batch_number + 300, batch_number + 600]
                    .map(|account_number| account_pubkeys[account_number]);
                let unmatched_account = Pubkey::new_unique();
                for account_pubkey in batch_accounts.into_iter().chain([unmatched_account]) {
                    messages_tx
                        .send(account_message(account_pubkey, Vec::new()))
                        .unwrap();
                }
                let twin_updates = read_updates(&mut twin_rx, 3).await;
                assert_eq!(updated_account_pubkeys(&twin_updates), batch_accounts);
                assert_eq!(read_updates(&mut client_rx, 3).await, twin_updates);
            }
            assert!(client_rx.recv().now_or_never().is_none());
        }

        /// A client changes its filter while it prepares, with an owner that only answers when
        /// the test says. New keys leave the preparation and prepare again. Equal keys send
        /// nothing, and the hand-over then routes with the newest filter and claims nothing more.
        /// Once routed, new keys leave for a receiver and prepare again, and a filter without
        /// route keys leaves that preparation and prepares nothing.
        #[tokio::test]
        async fn a_client_that_changes_filter_while_preparing_restarts_its_preparation() {
            let (broadcast_channels, [mut spied_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let mut spy_owner = OwnerRoutes::new(broadcast_channels.clone());
            let (client_tx, mut stream_rx) =
                super::spawn_client_loop(broadcast_channels, CancellationToken::new());
            let [first_pubkey, second_pubkey, third_pubkey] =
                [(); 3].map(|()| Pubkey::new_unique());
            let send_request = |subscribe_request: SubscribeRequest| {
                let client_filter = fixtures::filter_from_request(&subscribe_request);
                client_tx.send(Some((None, client_filter))).unwrap();
            };
            let mut take_in_route_changes = |spy_owner: &mut OwnerRoutes| {
                take_in_spied_route_changes(&mut spied_changes_rx, spy_owner)
            };
            send_request(accounts_request(&[first_pubkey], &[]));
            settle().await;
            assert_eq!(take_in_route_changes(&mut spy_owner), ["prepare"]);
            send_request(accounts_request(&[second_pubkey], &[]));
            settle().await;
            assert_eq!(take_in_route_changes(&mut spy_owner), ["leave", "prepare"]);
            let data_slice = SubscribeRequestAccountsDataSlice {
                offset: 1,
                length: 2,
            };
            let sliced_request = accounts_request(&[second_pubkey], &[data_slice]);
            send_request(sliced_request.clone());
            settle().await;
            assert!(take_in_route_changes(&mut spy_owner).is_empty());
            run_index_work(&mut spy_owner);
            settle().await;
            assert_eq!(take_in_route_changes(&mut spy_owner), ["hand-over"]);
            // The staged route kept the key list Arc, so the hand-over claims and releases nothing.
            assert!(!spy_owner.has_index_work());
            let account_update = account_message(second_pubkey, vec![1, 2, 3, 4]);
            spy_owner.send_batch(Processed, Arc::new(vec![account_update.clone()]));
            let sliced_filter = fixtures::filter_from_request(&sliced_request);
            let sliced_updates = sliced_filter.get_updates(&account_update, Some(Processed));
            assert_eq!(
                read_updates(&mut stream_rx, 1).await,
                sliced_updates.to_vec()
            );
            send_request(accounts_request(&[third_pubkey], &[]));
            settle().await;
            assert_eq!(
                take_in_route_changes(&mut spy_owner),
                ["leave for a receiver"]
            );
            settle().await;
            assert_eq!(take_in_route_changes(&mut spy_owner), ["prepare"]);
            let keyless_request = SubscribeRequest {
                transactions: HashMap::from([("t".into(), Default::default())]),
                ..Default::default()
            };
            send_request(keyless_request);
            settle().await;
            assert_eq!(take_in_route_changes(&mut spy_owner), ["leave"]);
            spy_owner.send_batch(Processed, Arc::new(vec![account_update]));
            settle().await;
            assert!(take_in_route_changes(&mut spy_owner).is_empty());
        }

        /// An owner that drops a preparation unanswered has stopped. The client sends no other
        /// route change, gets every later batch from its broadcast, and prepares again only after
        /// its next filter change.
        #[tokio::test]
        async fn a_client_whose_owner_drops_its_preparation_stays_on_the_broadcast() {
            let (broadcast_channels, [mut spied_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let (client_tx, mut stream_rx) =
                super::spawn_client_loop(broadcast_channels.clone(), CancellationToken::new());
            let account_pubkey = Pubkey::new_unique();
            let routed_request = accounts_request(&[account_pubkey], &[]);
            let routed_filter = || fixtures::filter_from_request(&routed_request);
            client_tx.send(Some((None, routed_filter()))).unwrap();
            settle().await;
            let dropped_preparation = spied_changes_rx.try_recv();
            assert!(matches!(dropped_preparation, Ok(RouteChange::Prepare(..))));
            drop(dropped_preparation);
            settle().await;
            for update_number in 0..3 {
                let account_update = account_message(account_pubkey, Vec::new());
                broadcast_channels.send(Processed, Arc::new(vec![account_update]));
                let received_updates = read_updates(&mut stream_rx, 1).await;
                assert_eq!(updated_account_pubkeys(&received_updates), [account_pubkey]);
                settle().await;
                assert!(
                    spied_changes_rx.try_recv().is_err(),
                    "update {update_number}"
                );
            }
            client_tx.send(Some((None, routed_filter()))).unwrap();
            settle().await;
            let next_preparation = spied_changes_rx.try_recv();
            assert!(matches!(next_preparation, Ok(RouteChange::Prepare(..))));
        }

        /// A routed client changes to other keys while 500 staged preparations of 1,600 keys
        /// wait at geyser_loop, which claims one key per millisecond. It leaves its route for a
        /// receiver, gets what its new filter makes of every batch from the next one on, as its
        /// twin on the broadcast does, and goes live again only once the 800,000 keys ahead of
        /// it are claimed.
        #[tokio::test(start_paused = true)]
        async fn a_routed_client_that_changes_keys_reads_the_broadcast_from_the_next_batch() {
            const STAGED_ROUTE_COUNT: usize = 500;
            const STAGED_KEY_COUNT: usize = 1600;
            let started_at = Instant::now();
            let mut loop_harness = spawn_loop_with(None, ONE_KEY_PER_MILLISECOND_LIMITS);
            let first_pubkeys = [(); 2].map(|()| Pubkey::new_unique());
            let second_pubkeys = [(); 2].map(|()| Pubkey::new_unique());
            let [first_routed_filter, first_twin_filter] = account_twin_filters(&first_pubkeys);
            let [second_routed_filter, second_twin_filter] = account_twin_filters(&second_pubkeys);
            let (twin_tx, mut twin_rx) = loop_harness.spawn_client(first_twin_filter);
            settle().await;
            let twin_receiver_count = loop_harness.receiver_count(Processed);
            let (client_tx, mut client_rx) = loop_harness.spawn_client(first_routed_filter);
            // Its two keys are claimed by 1 ms, and it joins at once.
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(loop_harness.receiver_count(Processed), twin_receiver_count);
            let mut ready_rxs = Vec::new();
            let mut staged_queue_rxs = Vec::new();
            for session_id in 100..100 + STAGED_ROUTE_COUNT {
                let route_keys = unmatched_account_keys(STAGED_KEY_COUNT);
                let (staged_route, staged_queue_rx) =
                    route_on_own_queue(session_id, &Arc::default(), &route_keys);
                let (ready_tx, ready_rx) = oneshot::channel();
                let route_change = RouteChange::Prepare(staged_route, ready_tx);
                loop_harness
                    .broadcast_channels
                    .send_route_change(Processed, route_change);
                ready_rxs.push(ready_rx);
                staged_queue_rxs.push(staged_queue_rx);
            }
            let every_account_update = || {
                first_pubkeys
                    .into_iter()
                    .chain(second_pubkeys)
                    .map(|account_pubkey| account_message(account_pubkey, Vec::new()))
            };
            loop_harness.send_messages(every_account_update());
            let twin_updates = read_updates(&mut twin_rx, 2).await;
            assert_eq!(updated_account_pubkeys(&twin_updates), first_pubkeys);
            assert_eq!(read_updates(&mut client_rx, 2).await, twin_updates);
            twin_tx.send(Some((None, second_twin_filter))).unwrap();
            client_tx.send(Some((None, second_routed_filter))).unwrap();
            for batch_number in 1..=5 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let receiver_count = loop_harness.receiver_count(Processed);
                assert_eq!(
                    receiver_count,
                    twin_receiver_count + 1,
                    "batch {batch_number}"
                );
                loop_harness.send_messages(every_account_update());
                let twin_updates = read_updates(&mut twin_rx, 2).await;
                assert_eq!(updated_account_pubkeys(&twin_updates), second_pubkeys);
                assert_eq!(read_updates(&mut client_rx, 2).await, twin_updates);
            }
            let answered_count = |ready_rxs: &[oneshot::Receiver<()>]| {
                ready_rxs
                    .iter()
                    .filter(|ready_rx| !ready_rx.is_empty())
                    .count()
            };
            sleep_until(started_at + Duration::from_secs(799)).await;
            assert_eq!(
                loop_harness.receiver_count(Processed),
                twin_receiver_count + 1
            );
            assert!(answered_count(&ready_rxs) < STAGED_ROUTE_COUNT);
            sleep_until(started_at + Duration::from_secs(801)).await;
            assert_eq!(loop_harness.receiver_count(Processed), twin_receiver_count);
            assert_eq!(answered_count(&ready_rxs), STAGED_ROUTE_COUNT);
            loop_harness.send_messages(every_account_update());
            let twin_updates = read_updates(&mut twin_rx, 2).await;
            assert_eq!(read_updates(&mut client_rx, 2).await, twin_updates);
        }

        /// A client hands its receiver over only once it has read it to the end. In each of 32
        /// rounds, the owner broadcasts a batch, then answers the preparation before the client
        /// runs. Whichever the client takes first, its hand-over carries an empty receiver, and
        /// the batch reaches its queue once. A key change then sends it back for the next round.
        #[tokio::test]
        async fn a_client_hands_its_receiver_over_only_after_reading_it_to_the_end() {
            let (broadcast_channels, [mut spied_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let mut spy_owner = OwnerRoutes::new(broadcast_channels.clone());
            let (client_tx, mut stream_rx) =
                super::spawn_client_loop(broadcast_channels, CancellationToken::new());
            let mut account_pubkey = Pubkey::new_unique();
            let request_filter = |account_pubkey| {
                fixtures::filter_from_request(&accounts_request(&[account_pubkey], &[]))
            };
            client_tx
                .send(Some((None, request_filter(account_pubkey))))
                .unwrap();
            settle().await;
            for round_number in 0..32 {
                let spied_changes =
                    take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
                assert_eq!(spied_changes, ["prepare"], "round {round_number}");
                let account_update = account_message(account_pubkey, Vec::new());
                spy_owner.send_batch(Processed, Arc::new(vec![account_update]));
                run_index_work(&mut spy_owner);
                settle().await;
                let spied_changes =
                    take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
                assert_eq!(spied_changes, ["hand-over"], "round {round_number}");
                let received_updates = read_updates(&mut stream_rx, 1).await;
                assert_eq!(updated_account_pubkeys(&received_updates), [account_pubkey]);
                assert!(stream_rx.recv().now_or_never().is_none());
                account_pubkey = Pubkey::new_unique();
                client_tx
                    .send(Some((None, request_filter(account_pubkey))))
                    .unwrap();
                settle().await;
                let spied_changes =
                    take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
                assert_eq!(
                    spied_changes,
                    ["leave for a receiver"],
                    "round {round_number}"
                );
                settle().await;
            }
        }

        /// A client prepares only once it has read its receiver to the end. A key change reaches
        /// the preparing client with 512 batches behind it, more than one poll of the client
        /// reads. The client leaves its preparation in that first poll, and prepares again with
        /// the new keys only once it has read every batch.
        #[tokio::test]
        async fn a_client_prepares_only_once_it_has_read_its_receiver_to_the_end() {
            const QUEUED_BATCH_COUNT: usize = 512;
            let (broadcast_channels, [mut spied_changes_rx, _]) =
                SubscriberChannels::new(1024, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let mut spy_owner = OwnerRoutes::new(broadcast_channels.clone());
            let (client_tx, mut stream_rx) =
                super::spawn_client_loop(broadcast_channels, CancellationToken::new());
            let request_filter = |account_pubkey| {
                fixtures::filter_from_request(&accounts_request(&[account_pubkey], &[]))
            };
            let [first_pubkey, newest_pubkey] = [(); 2].map(|()| Pubkey::new_unique());
            client_tx
                .send(Some((None, request_filter(first_pubkey))))
                .unwrap();
            settle().await;
            let spied_changes = take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
            assert_eq!(spied_changes, ["prepare"]);
            for _ in 0..QUEUED_BATCH_COUNT {
                let unmatched_update = account_message(Pubkey::new_unique(), Vec::new());
                spy_owner.send_batch(Processed, Arc::new(vec![unmatched_update]));
            }
            client_tx
                .send(Some((None, request_filter(newest_pubkey))))
                .unwrap();
            // One poll of the client reads at most tokio's budget of 128 items.
            tokio::task::yield_now().await;
            let spied_changes = take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
            assert_eq!(spied_changes, ["leave"]);
            yield_until(|| !spied_changes_rx.is_empty()).await;
            let spied_changes = take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
            assert_eq!(spied_changes, ["prepare"]);
            run_index_work(&mut spy_owner);
            settle().await;
            let spied_changes = take_in_spied_route_changes(&mut spied_changes_rx, &mut spy_owner);
            assert_eq!(spied_changes, ["hand-over"]);
            let account_update = account_message(newest_pubkey, Vec::new());
            spy_owner.send_batch(Processed, Arc::new(vec![account_update]));
            let routed_updates = read_updates(&mut stream_rx, 1).await;
            assert_eq!(updated_account_pubkeys(&routed_updates), [newest_pubkey]);
        }

        /// A filter change that comes with the owner's answer applies before the hand-over. In
        /// each of 128 rounds the owner answers, and the client gets a change, before the client
        /// runs. Whichever it takes first, a change to other keys leaves the answered preparation
        /// and prepares again, and a change that keeps the keys hands over once, with its filter.
        #[tokio::test]
        async fn a_change_that_comes_with_the_answer_applies_before_the_hand_over() {
            let (broadcast_channels, [mut spied_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let mut spy_owner = OwnerRoutes::new(broadcast_channels.clone());
            let (client_tx, mut stream_rx) =
                super::spawn_client_loop(broadcast_channels, CancellationToken::new());
            let send_request = |subscribe_request: &SubscribeRequest| {
                let client_filter = fixtures::filter_from_request(subscribe_request);
                client_tx.send(Some((None, client_filter))).unwrap();
            };
            let mut take_in_route_changes = |spy_owner: &mut OwnerRoutes| {
                take_in_spied_route_changes(&mut spied_changes_rx, spy_owner)
            };
            let mut account_pubkey = Pubkey::new_unique();
            send_request(&accounts_request(&[account_pubkey], &[]));
            settle().await;
            assert_eq!(take_in_route_changes(&mut spy_owner), ["prepare"]);
            for round_number in 0..128 {
                run_index_work(&mut spy_owner);
                if round_number % 2 == 0 {
                    account_pubkey = Pubkey::new_unique();
                    send_request(&accounts_request(&[account_pubkey], &[]));
                    settle().await;
                    let spied_changes = take_in_route_changes(&mut spy_owner);
                    assert_eq!(spied_changes, ["leave", "prepare"], "round {round_number}");
                    continue;
                }
                let data_slice = SubscribeRequestAccountsDataSlice {
                    offset: 1,
                    length: 2,
                };
                let sliced_request = accounts_request(&[account_pubkey], &[data_slice]);
                send_request(&sliced_request);
                settle().await;
                let spied_changes = take_in_route_changes(&mut spy_owner);
                assert_eq!(spied_changes, ["hand-over"], "round {round_number}");
                let account_update = account_message(account_pubkey, vec![1, 2, 3, 4]);
                spy_owner.send_batch(Processed, Arc::new(vec![account_update.clone()]));
                let sliced_filter = fixtures::filter_from_request(&sliced_request);
                let sliced_updates = sliced_filter.get_updates(&account_update, Some(Processed));
                assert_eq!(
                    read_updates(&mut stream_rx, 1).await,
                    sliced_updates.to_vec()
                );
                account_pubkey = Pubkey::new_unique();
                send_request(&accounts_request(&[account_pubkey], &[]));
                settle().await;
                let spied_changes = take_in_route_changes(&mut spy_owner);
                assert_eq!(
                    spied_changes,
                    ["leave for a receiver"],
                    "round {round_number}"
                );
                settle().await;
                let spied_changes = take_in_route_changes(&mut spy_owner);
                assert_eq!(spied_changes, ["prepare"], "round {round_number}");
            }
        }

        /// A preparing client that moves to another level leaves its preparation at the old
        /// level's owner, prepares at the new level's owner, and hands over there.
        #[tokio::test]
        async fn a_client_that_changes_level_while_preparing_leaves_at_the_old_owner() {
            let (broadcast_channels, [mut processed_changes_rx, mut confirmed_changes_rx]) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let mut processed_owner = OwnerRoutes::new(broadcast_channels.clone());
            let mut confirmed_owner = OwnerRoutes::new(broadcast_channels.clone());
            let (client_tx, mut stream_rx) =
                super::spawn_client_loop(broadcast_channels, CancellationToken::new());
            let account_pubkey = Pubkey::new_unique();
            let level_filter = |proto_commitment: CommitmentLevelProto| {
                let subscribe_request = SubscribeRequest {
                    commitment: Some(proto_commitment as i32),
                    ..accounts_request(&[account_pubkey], &[])
                };
                fixtures::filter_from_request(&subscribe_request)
            };
            let processed_filter = level_filter(CommitmentLevelProto::Processed);
            client_tx.send(Some((None, processed_filter))).unwrap();
            settle().await;
            let processed_changes =
                take_in_spied_route_changes(&mut processed_changes_rx, &mut processed_owner);
            assert_eq!(processed_changes, ["prepare"]);
            let confirmed_filter = level_filter(CommitmentLevelProto::Confirmed);
            client_tx.send(Some((None, confirmed_filter))).unwrap();
            settle().await;
            let processed_changes =
                take_in_spied_route_changes(&mut processed_changes_rx, &mut processed_owner);
            assert_eq!(processed_changes, ["leave"]);
            let confirmed_changes =
                take_in_spied_route_changes(&mut confirmed_changes_rx, &mut confirmed_owner);
            assert_eq!(confirmed_changes, ["prepare"]);
            run_index_work(&mut confirmed_owner);
            settle().await;
            let confirmed_changes =
                take_in_spied_route_changes(&mut confirmed_changes_rx, &mut confirmed_owner);
            assert_eq!(confirmed_changes, ["hand-over"]);
            let account_update = account_message(account_pubkey, Vec::new());
            confirmed_owner.send_batch(CommitmentLevel::Confirmed, Arc::new(vec![account_update]));
            let routed_updates = read_updates(&mut stream_rx, 1).await;
            assert_eq!(updated_account_pubkeys(&routed_updates), [account_pubkey]);
        }

        /// A client that ends sends its last leave only to the owner that holds its route,
        /// staged or live. It sends none when no owner holds one: after it left its preparation
        /// for a filter without route keys, or when its filter never had route keys.
        #[tokio::test]
        async fn a_client_that_ends_leaves_only_the_owner_that_holds_its_route() {
            let (broadcast_channels, mut spied_changes_rxs) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let mut spy_owners = [(); 2].map(|()| OwnerRoutes::new(broadcast_channels.clone()));
            // What the spy of geyser_loop, then the spy of block reconstruction, took in.
            let take_in_changes =
                |spied_changes_rxs: &mut [mpsc::UnboundedReceiver<RouteChange>; 2],
                 spy_owners: &mut [OwnerRoutes; 2]| {
                    let [processed_changes_rx, confirmed_changes_rx] = spied_changes_rxs;
                    let [processed_owner, confirmed_owner] = spy_owners;
                    [
                        take_in_spied_route_changes(processed_changes_rx, processed_owner),
                        take_in_spied_route_changes(confirmed_changes_rx, confirmed_owner),
                    ]
                };
            let level_filter = |proto_commitment: CommitmentLevelProto| {
                let subscribe_request = SubscribeRequest {
                    commitment: Some(proto_commitment as i32),
                    ..accounts_request(&[Pubkey::new_unique()], &[])
                };
                fixtures::filter_from_request(&subscribe_request)
            };
            let keyless_filter = || {
                let keyless_request = SubscribeRequest {
                    transactions: HashMap::from([("t".into(), Default::default())]),
                    ..Default::default()
                };
                fixtures::filter_from_request(&keyless_request)
            };
            let processed_leave = [vec!["leave"], vec![]];
            let client_standings = [
                ("preparing at processed", processed_leave.clone()),
                ("routed at processed", processed_leave),
                ("preparing at confirmed", [vec![], vec!["leave"]]),
                ("left for a keyless filter", [vec![], vec![]]),
                ("never keyed", [vec![], vec![]]),
            ];
            for (client_standing, leaves_at_the_end) in client_standings {
                let (client_tx, _stream_rx) =
                    super::spawn_client_loop(broadcast_channels.clone(), CancellationToken::new());
                let send_filter =
                    |client_filter| client_tx.send(Some((None, client_filter))).unwrap();
                let first_changes = match client_standing {
                    "preparing at confirmed" => {
                        send_filter(level_filter(CommitmentLevelProto::Confirmed));
                        [vec![], vec!["prepare"]]
                    }
                    "never keyed" => {
                        send_filter(keyless_filter());
                        [vec![], vec![]]
                    }
                    _ => {
                        send_filter(level_filter(CommitmentLevelProto::Processed));
                        [vec!["prepare"], vec![]]
                    }
                };
                settle().await;
                let taken_in_changes = take_in_changes(&mut spied_changes_rxs, &mut spy_owners);
                assert_eq!(taken_in_changes, first_changes, "{client_standing}");
                if client_standing == "routed at processed" {
                    run_index_work(&mut spy_owners[0]);
                    settle().await;
                    let taken_in_changes = take_in_changes(&mut spied_changes_rxs, &mut spy_owners);
                    assert_eq!(taken_in_changes, [vec!["hand-over"], vec![]]);
                }
                if client_standing == "left for a keyless filter" {
                    send_filter(keyless_filter());
                    settle().await;
                    let taken_in_changes = take_in_changes(&mut spied_changes_rxs, &mut spy_owners);
                    assert_eq!(taken_in_changes, [vec!["leave"], vec![]]);
                }
                drop(client_tx);
                settle().await;
                let taken_in_changes = take_in_changes(&mut spied_changes_rxs, &mut spy_owners);
                assert_eq!(taken_in_changes, leaves_at_the_end, "{client_standing}");
            }
        }

        /// The owner that holds a client's route takes in the client's last leave right after the
        /// client's task ends, before the task that waits on the client's cancellation: the leave
        /// is the last wake of the client's end, so the owner holds the worker's LIFO slot.
        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn the_owner_that_holds_the_route_runs_right_after_its_client_ends() {
            let (broadcast_channels, [mut processed_changes_rx, _confirmed_changes_rx]) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let run_order = Arc::new(std::sync::Mutex::new(Vec::new()));
            let (prepared_tx, prepared_rx) = oneshot::channel();
            let owner_run_order = Arc::clone(&run_order);
            let owner_task = tokio::spawn(async move {
                let mut prepared_tx = Some(prepared_tx);
                // Holds the preparation unanswered, so the client stays preparing.
                let mut held_changes = Vec::new();
                while let Some(route_change) = processed_changes_rx.recv().await {
                    if matches!(route_change, RouteChange::Leave(_)) {
                        owner_run_order.lock().unwrap().push("owner");
                        return held_changes.len();
                    }
                    held_changes.push(route_change);
                    if let Some(prepared_tx) = prepared_tx.take() {
                        prepared_tx.send(()).unwrap();
                    }
                }
                panic!("the client left no leave");
            });
            let client_cancellation_token = CancellationToken::new();
            let waiter_cancellation_token = client_cancellation_token.clone();
            let waiter_run_order = Arc::clone(&run_order);
            let waiter_task = tokio::spawn(async move {
                waiter_cancellation_token.cancelled().await;
                waiter_run_order.lock().unwrap().push("cancellation waiter");
            });
            let (client_tx, _stream_rx) =
                super::spawn_client_loop(broadcast_channels.clone(), client_cancellation_token);
            let routed_filter =
                fixtures::filter_from_request(&accounts_request(&[Pubkey::new_unique()], &[]));
            client_tx.send(Some((None, routed_filter))).unwrap();
            prepared_rx.await.unwrap();
            drop(client_tx);
            assert_eq!(owner_task.await.unwrap(), 1);
            waiter_task.await.unwrap();
            assert_eq!(*run_order.lock().unwrap(), ["owner", "cancellation waiter"]);
        }

        /// A change queued behind the first request applies before the client's first route
        /// change: the client prepares once, with the newest filter's keys, and leaves nothing.
        #[tokio::test]
        async fn a_change_queued_behind_the_first_request_prepares_once_with_the_newest_keys() {
            let (broadcast_channels, [mut spied_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let mut spy_owner = OwnerRoutes::new(broadcast_channels.clone());
            let (client_tx, mut stream_rx) =
                super::spawn_client_loop(broadcast_channels, CancellationToken::new());
            let [first_pubkey, newest_pubkey] = [(); 2].map(|()| Pubkey::new_unique());
            for account_pubkey in [first_pubkey, newest_pubkey] {
                let client_filter =
                    fixtures::filter_from_request(&accounts_request(&[account_pubkey], &[]));
                client_tx.send(Some((None, client_filter))).unwrap();
            }
            expect_one_preparation_with_the_newest_keys(
                &mut spied_changes_rx,
                &mut spy_owner,
                &mut stream_rx,
                [first_pubkey, newest_pubkey],
            )
            .await;
        }

        /// A change that waits in the client's requests when its replay ends applies before the
        /// client's first route change: the client prepares once, with the newest filter's keys,
        /// and leaves nothing.
        #[tokio::test]
        async fn a_change_waiting_when_the_replay_ends_prepares_once_with_the_newest_keys() {
            let (broadcast_channels, [mut spied_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(STEP_EVERY_PASS_LIMITS);
            let mut spy_owner = OwnerRoutes::new(broadcast_channels.clone());
            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let (stream_tx, mut stream_rx) = load_aware_channel(64, WeightScales::default());
            let (replay_tx, replay_rx) = mpsc::channel(1);
            let client_session = ClientSession::new(
                1,
                Some("test".into()),
                "test".into(),
                CancellationToken::new(),
                None,
            );
            tokio::spawn(GrpcService::client_loop(
                client_session,
                stream_tx,
                client_rx,
                None,
                broadcast_channels,
                Some(replay_tx),
                TaskTracker::new(),
                None,
            ));
            let [first_pubkey, newest_pubkey] = [(); 2].map(|()| Pubkey::new_unique());
            let request_filter = |account_pubkey| {
                fixtures::filter_from_request(&accounts_request(&[account_pubkey], &[]))
            };
            client_tx
                .send(Some((Some(0), request_filter(first_pubkey))))
                .unwrap();
            // The client subscribed and waits for its replay, which nobody answers yet.
            settle().await;
            assert!(spied_changes_rx.is_empty());
            client_tx
                .send(Some((None, request_filter(newest_pubkey))))
                .unwrap();
            tokio::spawn(answer_replay(replay_rx, 0, 0));
            expect_one_preparation_with_the_newest_keys(
                &mut spied_changes_rx,
                &mut spy_owner,
                &mut stream_rx,
                [first_pubkey, newest_pubkey],
            )
            .await;
        }

        /// Checks that a client whose first filter listed `account_pubkeys[0]`, and whose
        /// queued change lists `account_pubkeys[1]`, prepares once with the newest keys and
        /// hands over, and that its route delivers what the newest filter makes.
        ///
        /// # Arguments
        ///
        /// * `spied_changes_rx` - The route changes the client sends to geyser_loop.
        /// * `spy_owner` - The routes that stand in for geyser_loop.
        /// * `stream_rx` - The client's Subscribe queue.
        /// * `account_pubkeys` - The first filter's account, then the newest filter's.
        ///
        /// # Panics
        ///
        /// Panics if the client sends another route change than one preparation, then one
        /// hand-over, if the hand-over leaves index work, or if the route delivers another
        /// account.
        async fn expect_one_preparation_with_the_newest_keys(
            spied_changes_rx: &mut mpsc::UnboundedReceiver<RouteChange>,
            spy_owner: &mut OwnerRoutes,
            stream_rx: &mut LoadAwareReceiver<TonicResult<FilteredUpdate>>,
            account_pubkeys: [Pubkey; 2],
        ) {
            settle().await;
            let spied_changes = take_in_spied_route_changes(spied_changes_rx, spy_owner);
            assert_eq!(spied_changes, ["prepare"]);
            run_index_work(spy_owner);
            settle().await;
            let spied_changes = take_in_spied_route_changes(spied_changes_rx, spy_owner);
            assert_eq!(spied_changes, ["hand-over"]);
            // The preparation claimed the newest keys, so the hand-over claims nothing more.
            assert!(!spy_owner.has_index_work());
            let account_updates =
                account_pubkeys.map(|account_pubkey| account_message(account_pubkey, Vec::new()));
            spy_owner.send_batch(Processed, Arc::new(account_updates.to_vec()));
            let routed_updates = read_updates(stream_rx, 1).await;
            assert_eq!(
                updated_account_pubkeys(&routed_updates),
                [account_pubkeys[1]]
            );
        }

        /// geyser_loop's queue of plugin messages, which notes how many of `ready_rxs` the owner
        /// has answered each time it hands geyser_loop a batch.
        ///
        /// # Examples
        ///
        /// ```ignore
        /// let answer_counting_queue = AnswerCountingQueue {
        ///     messages_rx: BatchStreamUnboundedReceiver::new(messages_rx),
        ///     ready_rxs,
        ///     answered_counts: Arc::clone(&answered_counts),
        /// };
        /// tokio::spawn(GrpcService::geyser_loop(answer_counting_queue, ...));
        /// ```
        struct AnswerCountingQueue {
            /// The plugin's messages.
            messages_rx: BatchStreamUnboundedReceiver<Message>,
            /// The answer receivers of the preparations the test sent.
            ready_rxs: Vec<oneshot::Receiver<()>>,
            /// How many preparations were answered at each batch handed over, in order.
            answered_counts: Arc<StdMutex<Vec<usize>>>,
        }

        impl BatchStream for AnswerCountingQueue {
            type Item = Message;

            /// Reads the next batch, and notes the answered count when one is ready.
            ///
            /// # Arguments
            ///
            /// * `task_context` - The context of geyser_loop's task.
            /// * `batch_buffer` - Gets the batch.
            ///
            /// # Returns
            ///
            /// What the plugin's message queue reads.
            fn poll_recv_batch_with_buffer<B>(
                self: Pin<&mut Self>,
                task_context: &mut Context<'_>,
                batch_buffer: &mut B,
            ) -> Poll<Option<usize>>
            where
                B: crate::stream::Buffer<Self::Item>,
            {
                let answer_counting_queue = self.get_mut();
                let batch_read = Pin::new(&mut answer_counting_queue.messages_rx)
                    .poll_recv_batch_with_buffer(task_context, batch_buffer);
                if let Poll::Ready(Some(_)) = batch_read {
                    let ready_rxs = &answer_counting_queue.ready_rxs;
                    let answered_count = ready_rxs
                        .iter()
                        .filter(|ready_rx| !ready_rx.is_empty())
                        .count();
                    let mut answered_counts = answer_counting_queue.answered_counts.lock().unwrap();
                    answered_counts.push(answered_count);
                }
                batch_read
            }
        }

        /// A route of `session_id` for `client_filter` with `route_keys`, on a queue of its own.
        ///
        /// # Arguments
        ///
        /// * `session_id` - The route's session id.
        /// * `client_filter` - The route's filter.
        /// * `route_keys` - The route's key list.
        ///
        /// # Returns
        ///
        /// The route, and the receiving half of its queue.
        fn route_on_own_queue(
            session_id: usize,
            client_filter: &Arc<Filter>,
            route_keys: &Arc<[RouteKey]>,
        ) -> (Route, LoadAwareReceiver<TonicResult<FilteredUpdate>>) {
            let (stream_tx, stream_rx) = load_aware_channel(64, WeightScales::default());
            let route_template =
                Route::new(session_id, "test", &stream_tx, &CancellationToken::new());
            (
                route_template.with_filter(client_filter, route_keys),
                stream_rx,
            )
        }

        /// A filter for entries, routed under [`RouteKey::Entries`], with its key list.
        ///
        /// # Returns
        ///
        /// The filter and its route keys.
        fn entries_route_filter() -> (Arc<Filter>, Arc<[RouteKey]>) {
            let entries_request = SubscribeRequest {
                entry: HashMap::from([("e".into(), Default::default())]),
                ..Default::default()
            };
            let entries_filter = Arc::new(fixtures::filter_from_request(&entries_request));
            let route_keys = entries_filter.route_keys().expect("an indexable filter");
            (entries_filter, route_keys.into())
        }

        /// With 500 preparations of 1,600 keys each queued behind a probe's join and a batch,
        /// geyser_loop sends the batch, which reaches the routed probe, before it answers any
        /// preparation.
        #[tokio::test(start_paused = true)]
        async fn geyser_loop_sends_the_next_batch_while_500_clients_prepare() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, _block_reconstruction_rx) = mpsc::unbounded_channel();
            let (broadcast_channels, [geyser_loop_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(RouteIndexLimits::default());
            let (entries_filter, entries_route_keys) = entries_route_filter();
            let (probe_route, mut probe_rx) =
                route_on_own_queue(1, &entries_filter, &entries_route_keys);
            let probe_join = RouteChange::Join(probe_route, None);
            broadcast_channels.send_route_change(CommitmentLevel::Processed, probe_join);
            let mut ready_rxs = Vec::new();
            let mut preparing_rxs = Vec::new();
            for session_id in 2..502 {
                let mut route_keys = (0..1600)
                    .map(|_| RouteKey::Account(Pubkey::new_unique()))
                    .collect::<Vec<_>>();
                route_keys.sort_unstable();
                let (prepared_route, preparing_rx) =
                    route_on_own_queue(session_id, &Arc::default(), &route_keys.into());
                let (ready_tx, ready_rx) = oneshot::channel();
                let route_change = RouteChange::Prepare(prepared_route, ready_tx);
                broadcast_channels.send_route_change(CommitmentLevel::Processed, route_change);
                ready_rxs.push(ready_rx);
                preparing_rxs.push(preparing_rx);
            }
            messages_tx
                .send(Message::Entry(fixtures::message_entry(10, 0)))
                .unwrap();
            let answered_counts = Arc::new(StdMutex::new(Vec::new()));
            let answer_counting_queue = AnswerCountingQueue {
                messages_rx: BatchStreamUnboundedReceiver::new(messages_rx),
                ready_rxs,
                answered_counts: Arc::clone(&answered_counts),
            };
            tokio::spawn(GrpcService::geyser_loop(
                answer_counting_queue,
                broadcast_channels,
                geyser_loop_changes_rx,
                mpsc::unbounded_channel().1,
                block_reconstruction_tx,
            ));
            let probe_update = tokio::time::timeout(Duration::from_secs(5), probe_rx.recv())
                .await
                .expect("the probe gets the batch")
                .expect("the probe's queue is open")
                .expect("an update");
            assert!(matches!(
                probe_update.message,
                FilteredUpdateOneof::Entry(_)
            ));
            assert_eq!(*answered_counts.lock().unwrap(), [0]);
        }

        /// A batch that is ready when an index slice is due goes out first: the preparation the
        /// slice answers is still unanswered when geyser_loop takes the batch, and answered by
        /// the next one.
        #[tokio::test(start_paused = true)]
        async fn a_batch_ready_when_an_index_slice_is_due_goes_out_before_the_slice() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, _block_reconstruction_rx) = mpsc::unbounded_channel();
            let (broadcast_channels, [geyser_loop_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(RouteIndexLimits::default());
            let mut processed_rx = broadcast_channels.subscribe(CommitmentLevel::Processed);
            let (entries_filter, entries_route_keys) = entries_route_filter();
            let (prepared_route, _prepared_rx) =
                route_on_own_queue(1, &entries_filter, &entries_route_keys);
            let (ready_tx, ready_rx) = oneshot::channel();
            let route_change = RouteChange::Prepare(prepared_route, ready_tx);
            broadcast_channels.send_route_change(CommitmentLevel::Processed, route_change);
            let entry_message = |entry_slot| Message::Entry(fixtures::message_entry(entry_slot, 0));
            messages_tx.send(entry_message(10)).unwrap();
            let answered_counts = Arc::new(StdMutex::new(Vec::new()));
            let answer_counting_queue = AnswerCountingQueue {
                messages_rx: BatchStreamUnboundedReceiver::new(messages_rx),
                ready_rxs: vec![ready_rx],
                answered_counts: Arc::clone(&answered_counts),
            };
            tokio::spawn(GrpcService::geyser_loop(
                answer_counting_queue,
                broadcast_channels,
                geyser_loop_changes_rx,
                mpsc::unbounded_channel().1,
                block_reconstruction_tx,
            ));
            for next_entry_slot in [Some(11), None] {
                tokio::time::timeout(Duration::from_secs(5), processed_rx.recv())
                    .await
                    .expect("geyser_loop sends the batch")
                    .expect("the channel is open");
                if let Some(entry_slot) = next_entry_slot {
                    messages_tx.send(entry_message(entry_slot)).unwrap();
                }
            }
            assert_eq!(*answered_counts.lock().unwrap(), [0, 1]);
        }

        /// A plugin queue that always holds a message. Each read spends one unit of the task's
        /// budget and takes in one entry, so geyser_loop is never idle and yields only when its
        /// budget is spent.
        ///
        /// # Examples
        ///
        /// ```ignore
        /// tokio::spawn(GrpcService::geyser_loop(EndlessEntryQueue, ...));
        /// ```
        struct EndlessEntryQueue;

        impl BatchStream for EndlessEntryQueue {
            type Item = Message;

            /// Takes in one entry, unless the task's budget is spent.
            ///
            /// # Arguments
            ///
            /// * `task_context` - The context of geyser_loop's task.
            /// * `batch_buffer` - Gets the entry.
            ///
            /// # Returns
            ///
            /// `Pending` once the budget is spent, and `Ready(Some(1))` otherwise.
            fn poll_recv_batch_with_buffer<B>(
                self: Pin<&mut Self>,
                task_context: &mut Context<'_>,
                batch_buffer: &mut B,
            ) -> Poll<Option<usize>>
            where
                B: crate::stream::Buffer<Self::Item>,
            {
                let Poll::Ready(budget_unit) = tokio::task::coop::poll_proceed(task_context) else {
                    return Poll::Pending;
                };
                budget_unit.made_progress();
                let queued_entry = Message::Entry(fixtures::message_entry(1, 0));
                if batch_buffer.accumulate(queued_entry).is_err() {
                    unreachable!("geyser_loop's buffer has room for one message");
                }
                Poll::Ready(Some(1))
            }
        }

        /// While index work waits and nothing else is ready, geyser_loop runs one index slice
        /// per slice interval, with the limits its channels carry: a preparation of 4 keys,
        /// claimed one key per slice, is answered by the fourth slice, 3 ms after the first.
        #[tokio::test(start_paused = true)]
        async fn geyser_loop_runs_one_idle_index_slice_per_slice_interval() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, _block_reconstruction_rx) = mpsc::unbounded_channel();
            let (broadcast_channels, [geyser_loop_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(ONE_KEY_PER_MILLISECOND_LIMITS);
            let (prepared_route, _prepared_rx) =
                route_on_own_queue(1, &Arc::default(), &unmatched_account_keys(4));
            let (ready_tx, ready_rx) = oneshot::channel();
            let route_change = RouteChange::Prepare(prepared_route, ready_tx);
            broadcast_channels.send_route_change(CommitmentLevel::Processed, route_change);
            let started_at = Instant::now();
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast_channels,
                geyser_loop_changes_rx,
                mpsc::unbounded_channel().1,
                block_reconstruction_tx,
            ));
            tokio::time::timeout(Duration::from_secs(5), ready_rx)
                .await
                .expect("the slices answer the preparation")
                .expect("the preparation is answered");
            assert_eq!(started_at.elapsed(), Duration::from_millis(3));
            drop(messages_tx);
        }

        /// geyser_loop wakes for its index slice timer only while index work waits: with no
        /// route change, a second passes with one read of its queue, and once a preparation is
        /// answered, the timer wakes it at most once more.
        #[tokio::test(start_paused = true)]
        async fn geyser_loop_wakes_for_its_timer_only_while_index_work_waits() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, _block_reconstruction_rx) = mpsc::unbounded_channel();
            let (broadcast_channels, [geyser_loop_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(RouteIndexLimits::default());
            let read_count = Arc::new(AtomicUsize::new(0));
            let read_counting_queue = ReadCountingQueue {
                counted_queue: BatchStreamUnboundedReceiver::new(messages_rx),
                read_count: Arc::clone(&read_count),
            };
            tokio::spawn(GrpcService::geyser_loop(
                read_counting_queue,
                broadcast_channels.clone(),
                geyser_loop_changes_rx,
                mpsc::unbounded_channel().1,
                block_reconstruction_tx,
            ));
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(read_count.load(Ordering::Relaxed), 1);
            let (prepared_route, _prepared_rx) =
                route_on_own_queue(1, &Arc::default(), &unmatched_account_keys(1));
            let (ready_tx, ready_rx) = oneshot::channel();
            let route_change = RouteChange::Prepare(prepared_route, ready_tx);
            broadcast_channels.send_route_change(CommitmentLevel::Processed, route_change);
            tokio::time::timeout(Duration::from_secs(5), ready_rx)
                .await
                .expect("an idle slice answers the preparation")
                .expect("the preparation is answered");
            let reads_at_answer = read_count.load(Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(read_count.load(Ordering::Relaxed) <= reads_at_answer + 2);
            drop(messages_tx);
        }

        /// geyser_loop runs an overdue index slice at the top of a pass while its queue never
        /// reads empty: a preparation taken in at the start is answered between two batches
        /// once it has waited ten slice intervals, and not before.
        #[tokio::test(start_paused = true)]
        async fn geyser_loop_runs_an_overdue_index_slice_while_its_queue_never_reads_empty() {
            let (block_reconstruction_tx, _block_reconstruction_rx) = mpsc::unbounded_channel();
            let (broadcast_channels, [geyser_loop_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(RouteIndexLimits::default());
            let (prepared_route, _prepared_rx) =
                route_on_own_queue(1, &Arc::default(), &unmatched_account_keys(1));
            let (ready_tx, mut ready_rx) = oneshot::channel();
            let route_change = RouteChange::Prepare(prepared_route, ready_tx);
            broadcast_channels.send_route_change(CommitmentLevel::Processed, route_change);
            let started_at = Instant::now();
            tokio::spawn(GrpcService::geyser_loop(
                EndlessEntryQueue,
                broadcast_channels,
                geyser_loop_changes_rx,
                mpsc::unbounded_channel().1,
                block_reconstruction_tx,
            ));
            // geyser_loop takes the preparation in, then runs until its budget is spent.
            tokio::task::yield_now().await;
            let mut answered_after = None;
            while answered_after.is_none() && started_at.elapsed() < Duration::from_millis(20) {
                tokio::time::advance(Duration::from_millis(1)).await;
                if ready_rx.try_recv().is_ok() {
                    answered_after = Some(started_at.elapsed());
                }
            }
            assert_eq!(answered_after, Some(Duration::from_millis(10)));
        }

        /// A relayed item that is ready together with a plugin batch goes out first, in each of
        /// 64 rounds that queue one of each while geyser_loop waits.
        #[tokio::test]
        async fn geyser_loop_sends_a_ready_relayed_item_before_a_ready_plugin_batch() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, _block_reconstruction_rx) = mpsc::unbounded_channel();
            let (relay_tx, relay_rx) = mpsc::unbounded_channel();
            let (broadcast_channels, [geyser_loop_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(RouteIndexLimits::default());
            let mut processed_rx = broadcast_channels.subscribe(CommitmentLevel::Processed);
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast_channels,
                geyser_loop_changes_rx,
                relay_rx,
                block_reconstruction_tx,
            ));
            let entry_message = |entry_slot| Message::Entry(fixtures::message_entry(entry_slot, 0));
            for round_number in 0..64 {
                let relayed_batch = Arc::new(vec![entry_message(2 * round_number)]);
                let relayed_item = RelayedItem::Batch(Arc::clone(&relayed_batch));
                relay_tx.send(relayed_item).unwrap();
                messages_tx
                    .send(entry_message(2 * round_number + 1))
                    .unwrap();
                let first_sent = processed_rx.recv().await.expect("the relayed batch");
                assert!(
                    Arc::ptr_eq(&first_sent, &relayed_batch),
                    "round {round_number}"
                );
                let second_sent = processed_rx.recv().await.expect("the plugin batch");
                assert_eq!(*second_sent, [entry_message(2 * round_number + 1)]);
            }
        }

        /// A burst of 300 replay answers on the relay goes out in geyser_loop's first poll,
        /// with the plugin batch queued behind it. That is more answers than tokio's budget of
        /// 128 units per poll, so with one unit per answer the loop would answer 128 and leave
        /// the rest and the batch for a later poll. The test's own task runs first in each pass
        /// of the `current_thread` runtime, so it looks right after that first poll.
        #[tokio::test]
        async fn geyser_loop_sends_a_burst_of_relayed_items_in_one_poll() {
            const ANSWER_COUNT: usize = 300;
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, _block_reconstruction_rx) = mpsc::unbounded_channel();
            let (relay_tx, relay_rx) = mpsc::unbounded_channel();
            let (broadcast_channels, [geyser_loop_changes_rx, _]) =
                SubscriberChannels::new(16, 16, 16).with_routing(RouteIndexLimits::default());
            let mut processed_rx = broadcast_channels.subscribe(CommitmentLevel::Processed);
            let mut answer_rxs = Vec::with_capacity(ANSWER_COUNT);
            for _ in 0..ANSWER_COUNT {
                let (answer_tx, answer_rx) = oneshot::channel();
                let replay_answer = RelayedItem::Answer(answer_tx, ReplayedResponse::Lagged(0));
                relay_tx.send(replay_answer).unwrap();
                answer_rxs.push(answer_rx);
            }
            let plugin_message = Message::Entry(fixtures::message_entry(1, 0));
            messages_tx.send(plugin_message.clone()).unwrap();
            tokio::spawn(GrpcService::geyser_loop(
                BatchStreamUnboundedReceiver::new(messages_rx),
                broadcast_channels,
                geyser_loop_changes_rx,
                relay_rx,
                block_reconstruction_tx,
            ));

            let mut answered_count = 0;
            yield_until(|| {
                answered_count += answer_rxs
                    .iter_mut()
                    .filter_map(|answer_rx| answer_rx.try_recv().ok())
                    .count();
                answered_count > 0
            })
            .await;
            assert_eq!(
                answered_count, ANSWER_COUNT,
                "answers sent in the first poll"
            );
            let plugin_batch = processed_rx
                .try_recv()
                .expect("the plugin batch, sent in the same poll");
            assert_eq!(*plugin_batch, [plugin_message]);
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

    /// When [`GrpcService::read_requests`] posts a client's filters: alone, and wired to
    /// [`GrpcService::client_loop`] as `subscribe` wires them. Times are milliseconds of paused
    /// time from a harness's start.
    mod filter_apply_interval {
        use {
            super::{
                super::*, answer_replay, create_filter_with_blocks, settle, slot_batch,
                test_transaction, ClientHandles, SubscribeRequestFilterBlocks,
                SubscribeRequestFilterSlots,
            },
            crate::plugin::filter::fixtures,
            futures::FutureExt,
            std::future::Future,
            tokio::{sync::oneshot::error::TryRecvError, time::Instant},
            yellowstone_grpc_proto::prelude::SubscribeRequestPing,
            CommitmentLevel::{Confirmed, Processed},
        };

        /// The interval these tests use, the default.
        const FILTER_APPLY_INTERVAL: Duration = Duration::from_secs(5);
        /// How long the harnesses wait for the next post, reply or update before they report
        /// none.
        const LONGEST_WAIT: Duration = Duration::from_secs(60);
        /// The blocks of each replay. They weigh 10 each, against a Subscribe queue of 50.
        const REPLAYED_BLOCKS: usize = 20;

        /// What the request task posted to [`GrpcService::client_loop`], and when.
        #[derive(Debug, PartialEq, Eq)]
        enum RequestTaskPost {
            /// A filter, named by the label of its request, with that request's `from_slot`.
            FilterPosted {
                posted_at: Duration,
                request_label: String,
                from_slot: Option<u64>,
                commitment_level: CommitmentLevel,
            },
            /// The task ended and dropped its sender.
            TaskEnded { ended_at: Duration },
        }

        /// What the request task queued for the client, and when.
        #[derive(Debug, PartialEq, Eq)]
        enum QueuedReply {
            /// The pong to the ping with `ping_id`.
            PingAnswered { queued_at: Duration, ping_id: i32 },
            /// A filter error, with its status message.
            FilterRejected {
                queued_at: Duration,
                status_message: String,
            },
        }

        /// [`GrpcService::read_requests`] alone, with two tasks that stamp each post and each
        /// queued reply with its time.
        ///
        /// # Examples
        ///
        /// ```ignore
        /// let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
        /// request_task.send_labelled_at(0, "first").await;
        /// request_task.send_labelled_at(1_000, "second").await;
        /// request_task.expect_filter_post("first", 0).await;
        /// request_task.expect_filter_post("second", 5_000).await;
        /// ```
        struct RequestTaskHarness {
            /// The request stream's sender, or [`None`] after a half-close.
            request_tx: Option<mpsc::UnboundedSender<TonicResult<SubscribeRequest>>>,
            /// The live signal's sender, or [`None`] once the live stream has started.
            client_live_tx: Option<oneshot::Sender<()>>,
            /// The parent of the request task's token.
            cancellation_token: CancellationToken,
            /// When the harness started.
            started_at: Instant,
            /// The posts, stamped.
            stamped_posts_rx: mpsc::UnboundedReceiver<RequestTaskPost>,
            /// The queued replies, stamped.
            stamped_replies_rx: mpsc::UnboundedReceiver<QueuedReply>,
        }

        impl RequestTaskHarness {
            /// Spawns the request task for a client whose live stream has started.
            ///
            /// # Arguments
            ///
            /// * `filter_apply_interval` - The request task's interval.
            ///
            /// # Returns
            ///
            /// The harness, started now.
            fn spawn_live_client(filter_apply_interval: Duration) -> Self {
                let mut request_task = Self::spawn_before_live_stream(filter_apply_interval);
                request_task.client_live_tx = None;
                request_task
            }

            /// Spawns the request task for a client whose live stream has not started, as
            /// during a snapshot or a replay.
            ///
            /// # Arguments
            ///
            /// * `filter_apply_interval` - The request task's interval.
            ///
            /// # Returns
            ///
            /// The harness, started now, holding the live signal's sender.
            fn spawn_before_live_stream(filter_apply_interval: Duration) -> Self {
                let started_at = Instant::now();
                let (client_tx, mut posted_rx) = mpsc::unbounded_channel();
                let (stream_tx, mut stream_rx) = load_aware_channel(1024, WeightScales::default());
                let (client_live_tx, client_live_rx) = oneshot::channel();
                let cancellation_token = CancellationToken::new();
                let request_tx = spawn_request_task(
                    "test",
                    filter_apply_interval,
                    stream_tx,
                    client_tx,
                    client_live_rx,
                    &cancellation_token,
                );
                let (stamped_posts_tx, stamped_posts_rx) = mpsc::unbounded_channel();
                tokio::spawn(async move {
                    while let Some(filter_post) = posted_rx.recv().await {
                        let (from_slot, posted_filter) =
                            filter_post.expect("the request stream does not fail here");
                        let _ = stamped_posts_tx.send(RequestTaskPost::FilterPosted {
                            posted_at: started_at.elapsed(),
                            request_label: request_label(&posted_filter),
                            from_slot,
                            commitment_level: posted_filter.get_commitment_level(),
                        });
                    }
                    let ended_at = started_at.elapsed();
                    let _ = stamped_posts_tx.send(RequestTaskPost::TaskEnded { ended_at });
                });
                let (stamped_replies_tx, stamped_replies_rx) = mpsc::unbounded_channel();
                tokio::spawn(async move {
                    while let Some(queued_item) = stream_rx.recv().await {
                        let queued_at = started_at.elapsed();
                        let queued_reply = match queued_item {
                            Ok(FilteredUpdate {
                                message: FilteredUpdateOneof::Pong(pong_message),
                                ..
                            }) => QueuedReply::PingAnswered {
                                queued_at,
                                ping_id: pong_message.id,
                            },
                            Ok(_filtered_update) => panic!("the request task queued an update"),
                            Err(error_status) => QueuedReply::FilterRejected {
                                queued_at,
                                status_message: error_status.message().to_owned(),
                            },
                        };
                        let _ = stamped_replies_tx.send(queued_reply);
                    }
                });
                Self {
                    request_tx: Some(request_tx),
                    client_live_tx: Some(client_live_tx),
                    cancellation_token,
                    started_at,
                    stamped_posts_rx,
                    stamped_replies_rx,
                }
            }

            /// Sends `subscribe_request` at `sent_at_ms`, or now if that has passed.
            ///
            /// # Arguments
            ///
            /// * `sent_at_ms` - When to send it.
            /// * `subscribe_request` - The request.
            ///
            /// # Panics
            ///
            /// Panics after a half-close, or once the request task has ended.
            async fn send_at(&self, sent_at_ms: u64, subscribe_request: SubscribeRequest) {
                sleep_until(self.started_at + Duration::from_millis(sent_at_ms)).await;
                let request_tx = self.request_tx.as_ref().expect("not half-closed");
                request_tx
                    .send(Ok(subscribe_request))
                    .expect("the request task reads");
            }

            /// Sends [`labelled_request`] at `sent_at_ms`.
            ///
            /// # Arguments
            ///
            /// * `sent_at_ms` - When to send it.
            /// * `request_label` - The request's label.
            async fn send_labelled_at(&self, sent_at_ms: u64, request_label: &str) {
                self.send_at(sent_at_ms, labelled_request(request_label))
                    .await;
            }

            /// Half-closes the request stream at `half_closed_at_ms`.
            ///
            /// # Arguments
            ///
            /// * `half_closed_at_ms` - When to half-close it.
            async fn half_close_at(&mut self, half_closed_at_ms: u64) {
                let half_closed_at = Duration::from_millis(half_closed_at_ms);
                sleep_until(self.started_at + half_closed_at).await;
                self.request_tx = None;
            }

            /// Starts the client's live stream at `live_at_ms`, as [`GrpcService::client_loop`]
            /// does where its snapshot or replay ends.
            ///
            /// # Arguments
            ///
            /// * `live_at_ms` - When the live stream starts.
            async fn start_live_stream_at(&mut self, live_at_ms: u64) {
                sleep_until(self.started_at + Duration::from_millis(live_at_ms)).await;
                self.client_live_tx = None;
            }

            /// Cancels the request task's token, as the end of the session does.
            fn cancel_session(&self) {
                self.cancellation_token.cancel();
            }

            /// Waits for the next post.
            ///
            /// # Returns
            ///
            /// The post, or [`None`] if none comes within [`LONGEST_WAIT`].
            async fn next_post(&mut self) -> Option<RequestTaskPost> {
                let next_post = self.stamped_posts_rx.recv();
                let next_post = tokio::time::timeout(LONGEST_WAIT, next_post).await;
                next_post.ok().flatten()
            }

            /// Checks that the next post is `expected_post`.
            ///
            /// # Arguments
            ///
            /// * `expected_post` - The post the task must make next.
            ///
            /// # Panics
            ///
            /// Panics if the next post differs, or none comes within [`LONGEST_WAIT`].
            async fn expect_post(&mut self, expected_post: RequestTaskPost) {
                assert_eq!(self.next_post().await, Some(expected_post));
            }

            /// Checks that the next post is [`filter_post`]'s.
            ///
            /// # Arguments
            ///
            /// * `request_label` - The label of the request it must come from.
            /// * `posted_at_ms` - When it must post.
            ///
            /// # Panics
            ///
            /// Panics if the next post differs, or none comes within [`LONGEST_WAIT`].
            async fn expect_filter_post(&mut self, request_label: &str, posted_at_ms: u64) {
                self.expect_post(filter_post(request_label, posted_at_ms))
                    .await;
            }

            /// Checks that the next reply the task queues for the client is `expected_reply`.
            ///
            /// # Arguments
            ///
            /// * `expected_reply` - The reply the task must queue next, or [`None`] for none
            ///   within [`LONGEST_WAIT`].
            ///
            /// # Panics
            ///
            /// Panics if the next reply differs.
            async fn expect_reply(&mut self, expected_reply: Option<QueuedReply>) {
                let next_reply = self.stamped_replies_rx.recv();
                let next_reply = tokio::time::timeout(LONGEST_WAIT, next_reply).await;
                assert_eq!(next_reply.ok().flatten(), expected_reply);
            }
        }

        /// A replaying client wired as `subscribe` wires it: [`GrpcService::read_requests`] posts
        /// to [`GrpcService::client_loop`], whose session holds the live signal's sender. Each
        /// replay is [`REPLAYED_BLOCKS`] blocks into a Subscribe queue that holds five, so the
        /// replay waits while the test does not read. Routing is off.
        ///
        /// # Examples
        ///
        /// ```ignore
        /// let mut replaying_client = ReplayingClient::spawn("subscriber-id");
        /// replaying_client.send_at(0, replaying_request("first")).await;
        /// replaying_client.send_labelled_at(1_000, "newest").await;
        /// let replayed_labels = replaying_client.read_replayed_blocks().await;
        /// assert_eq!(replayed_labels, ["first"; REPLAYED_BLOCKS]);
        /// replaying_client.expect_live_filter_after_interval("newest").await;
        /// assert_eq!(replaying_client.end_session().await, [0]);
        /// ```
        struct ReplayingClient {
            /// The request stream's sender, or [`None`] after a half-close.
            request_tx: Option<mpsc::UnboundedSender<TonicResult<SubscribeRequest>>>,
            /// The client's Subscribe queue.
            stream_rx: LoadAwareReceiver<TonicResult<FilteredUpdate>>,
            /// The broadcast channels, for live batches.
            broadcast_channels: SubscriberChannels,
            /// The session's token, the parent of the request task's.
            cancellation_token: CancellationToken,
            /// The [`GrpcService::client_loop`] task.
            client_loop_handle: tokio::task::JoinHandle<()>,
            /// The task that answers replay requests, and returns each request's `from_slot`.
            replayer_handle: tokio::task::JoinHandle<Vec<u64>>,
            /// When the client started.
            started_at: Instant,
        }

        impl ReplayingClient {
            /// Spawns the request task and the [`GrpcService::client_loop`] of a client under
            /// `subscriber_id`.
            ///
            /// # Arguments
            ///
            /// * `subscriber_id` - The client's subscriber id, which its metrics are counted
            ///   under.
            ///
            /// # Returns
            ///
            /// The client, started now.
            fn spawn(subscriber_id: &str) -> Self {
                let started_at = Instant::now();
                let broadcast_channels = SubscriberChannels::new(16, 16, 16);
                let (replay_tx, mut replay_rx) = mpsc::channel::<ReplayStoredSlotsRequest>(1);
                let (answered_replay_tx, answered_replay_rx) = mpsc::channel(1);
                tokio::spawn(answer_replay(answered_replay_rx, REPLAYED_BLOCKS as u64, 9));
                let replayer_handle = tokio::spawn(async move {
                    let mut replayed_from_slots = Vec::new();
                    while let Some(replay_request) = replay_rx.recv().await {
                        replayed_from_slots.push(replay_request.1);
                        let _ = answered_replay_tx.send(replay_request).await;
                    }
                    replayed_from_slots
                });
                let weight_scales = WeightScales {
                    transaction_data_unit: 0,
                    ..WeightScales::default()
                };
                let (stream_tx, stream_rx) = load_aware_channel(50, weight_scales);
                let (client_tx, client_rx) = mpsc::unbounded_channel();
                let (client_live_tx, client_live_rx) = oneshot::channel();
                let cancellation_token = CancellationToken::new();
                let request_tx = spawn_request_task(
                    subscriber_id,
                    FILTER_APPLY_INTERVAL,
                    stream_tx.clone(),
                    client_tx,
                    client_live_rx,
                    &cancellation_token,
                );
                let mut client_session = ClientSession::new(
                    0,
                    Some(subscriber_id.to_owned()),
                    "test".to_owned(),
                    cancellation_token.clone(),
                    None,
                );
                client_session.client_live_tx = Some(client_live_tx);
                let client_loop_handle = tokio::spawn(GrpcService::client_loop(
                    client_session,
                    stream_tx,
                    client_rx,
                    None,
                    broadcast_channels.clone(),
                    Some(replay_tx),
                    TaskTracker::new(),
                    None,
                ));
                Self {
                    request_tx: Some(request_tx),
                    stream_rx,
                    broadcast_channels,
                    cancellation_token,
                    client_loop_handle,
                    replayer_handle,
                    started_at,
                }
            }

            /// Puts `request_item` on the request stream at `sent_at_ms`, or now if that has
            /// passed.
            ///
            /// # Arguments
            ///
            /// * `sent_at_ms` - When to send it.
            /// * `request_item` - A request, or a stream error.
            ///
            /// # Panics
            ///
            /// Panics after a half-close, or once the request task has ended.
            async fn send_item_at(
                &self,
                sent_at_ms: u64,
                request_item: TonicResult<SubscribeRequest>,
            ) {
                sleep_until(self.started_at + Duration::from_millis(sent_at_ms)).await;
                let request_tx = self.request_tx.as_ref().expect("not half-closed");
                request_tx
                    .send(request_item)
                    .expect("the request task reads");
            }

            /// Sends `subscribe_request` at `sent_at_ms`.
            ///
            /// # Arguments
            ///
            /// * `sent_at_ms` - When to send it.
            /// * `subscribe_request` - The request.
            async fn send_at(&self, sent_at_ms: u64, subscribe_request: SubscribeRequest) {
                self.send_item_at(sent_at_ms, Ok(subscribe_request)).await;
            }

            /// Sends [`labelled_request`] at `sent_at_ms`.
            ///
            /// # Arguments
            ///
            /// * `sent_at_ms` - When to send it.
            /// * `request_label` - The request's label.
            async fn send_labelled_at(&self, sent_at_ms: u64, request_label: &str) {
                self.send_at(sent_at_ms, labelled_request(request_label))
                    .await;
            }

            /// Fails the request stream at `failed_at_ms`, as a broken connection does.
            ///
            /// # Arguments
            ///
            /// * `failed_at_ms` - When the stream fails.
            async fn fail_request_stream_at(&self, failed_at_ms: u64) {
                let stream_error = Status::internal("the request stream failed");
                self.send_item_at(failed_at_ms, Err(stream_error)).await;
            }

            /// Half-closes the request stream at `half_closed_at_ms`.
            ///
            /// # Arguments
            ///
            /// * `half_closed_at_ms` - When to half-close it.
            async fn half_close_at(&mut self, half_closed_at_ms: u64) {
                let half_closed_at = Duration::from_millis(half_closed_at_ms);
                sleep_until(self.started_at + half_closed_at).await;
                self.request_tx = None;
            }

            /// Reads the client's next update.
            ///
            /// # Returns
            ///
            /// The update.
            ///
            /// # Panics
            ///
            /// Panics if none comes within [`LONGEST_WAIT`], if the stream ends, or on an error
            /// status.
            async fn next_update(&mut self) -> FilteredUpdate {
                tokio::time::timeout(LONGEST_WAIT, self.stream_rx.recv())
                    .await
                    .expect("an update comes")
                    .expect("the stream is open")
                    .expect("an update, not an error status")
            }

            /// Reads the replay's blocks.
            ///
            /// # Returns
            ///
            /// The label of each replayed block, in order.
            ///
            /// # Panics
            ///
            /// Panics if an update is not a block, as [`ReplayingClient::next_update`] does.
            async fn read_replayed_blocks(&mut self) -> Vec<String> {
                let mut replayed_labels = Vec::new();
                for _ in 0..REPLAYED_BLOCKS {
                    let replayed_update = self.next_update().await;
                    let is_block = matches!(replayed_update.message, FilteredUpdateOneof::Block(_));
                    assert!(is_block, "the replay holds blocks only");
                    replayed_labels.push(update_label(&replayed_update));
                }
                replayed_labels
            }

            /// Broadcasts a live Processed slot status once an interval after the first post has
            /// passed, and checks which request's filter the client applies to it.
            ///
            /// # Arguments
            ///
            /// * `request_label` - The label of the request whose filter must apply.
            ///
            /// # Panics
            ///
            /// Panics if another filter applies, as [`ReplayingClient::next_update`] does.
            async fn expect_live_filter_after_interval(&mut self, request_label: &str) {
                let interval_passed_at = FILTER_APPLY_INTERVAL + Duration::from_secs(1);
                sleep_until(self.started_at + interval_passed_at).await;
                self.broadcast_channels
                    .send(CommitmentLevel::Processed, slot_batch(100));
                assert_eq!(update_label(&self.next_update().await), request_label);
            }

            /// Waits for the client's stream to end.
            ///
            /// # Panics
            ///
            /// Panics if it does not end within [`LONGEST_WAIT`], or if anything else comes first.
            async fn expect_stream_end(&mut self) {
                let stream_item = tokio::time::timeout(LONGEST_WAIT, self.stream_rx.recv())
                    .await
                    .expect("the stream ends");
                assert!(stream_item.is_none(), "the stream ends with nothing more");
            }

            /// Ends the session and waits for its tasks.
            ///
            /// # Returns
            ///
            /// The `from_slot` of each replay the client asked for.
            ///
            /// # Panics
            ///
            /// Panics if [`GrpcService::client_loop`] or the replayer panicked.
            async fn end_session(self) -> Vec<u64> {
                self.cancellation_token.cancel();
                self.client_loop_handle.await.expect("client_loop ends");
                self.replayer_handle.await.expect("the replayer ends")
            }
        }

        /// Spawns [`GrpcService::read_requests`] over an mpsc request stream, as `subscribe`
        /// spawns it.
        ///
        /// # Arguments
        ///
        /// * `subscriber_id` - The client's subscriber id.
        /// * `filter_apply_interval` - The request task's interval.
        /// * `stream_tx` - The client's Subscribe queue.
        /// * `client_tx` - Where the task posts the client's filters.
        /// * `client_live_rx` - The live signal's receiver.
        /// * `cancellation_token` - The session's token. The task gets a child of it.
        ///
        /// # Returns
        ///
        /// The request stream's sender.
        fn spawn_request_task(
            subscriber_id: &str,
            filter_apply_interval: Duration,
            stream_tx: SubscribeSender,
            client_tx: mpsc::UnboundedSender<Option<(Option<u64>, Filter)>>,
            client_live_rx: oneshot::Receiver<()>,
            cancellation_token: &CancellationToken,
        ) -> mpsc::UnboundedSender<TonicResult<SubscribeRequest>> {
            let (request_tx, request_rx) = mpsc::unbounded_channel();
            tokio::spawn(GrpcService::read_requests(
                UnboundedReceiverStream::new(request_rx),
                0,
                subscriber_id.to_owned(),
                Arc::new(FilterLimits::default()),
                fixtures::filter_names(),
                filter_apply_interval,
                stream_tx,
                client_tx,
                client_live_rx,
                cancellation_token.child_token(),
            ));
            request_tx
        }

        /// A request with one slots filter named `request_label`, so that what it posts tells
        /// which request it came from.
        ///
        /// # Arguments
        ///
        /// * `request_label` - The request's label.
        ///
        /// # Returns
        ///
        /// The request, at the default commitment.
        fn labelled_request(request_label: &str) -> SubscribeRequest {
            SubscribeRequest {
                slots: HashMap::from([(
                    request_label.to_owned(),
                    SubscribeRequestFilterSlots::default(),
                )]),
                ..Default::default()
            }
        }

        /// A labelled request whose commitment no filter accepts.
        ///
        /// # Arguments
        ///
        /// * `request_label` - The request's label.
        /// * `invalid_commitment` - A value that names no commitment level.
        ///
        /// # Returns
        ///
        /// The request, which fails to compile.
        fn invalid_request(request_label: &str, invalid_commitment: i32) -> SubscribeRequest {
            SubscribeRequest {
                commitment: Some(invalid_commitment),
                ..labelled_request(request_label)
            }
        }

        /// A labelled request that replays from slot 0, with a blocks filter for the replay and a
        /// slots filter for the live stream, both named `request_label`.
        ///
        /// # Arguments
        ///
        /// * `request_label` - The request's label.
        ///
        /// # Returns
        ///
        /// The request.
        fn replaying_request(request_label: &str) -> SubscribeRequest {
            let replayed_blocks = SubscribeRequestFilterBlocks {
                include_transactions: Some(true),
                include_accounts: Some(false),
                ..Default::default()
            };
            SubscribeRequest {
                blocks: HashMap::from([(request_label.to_owned(), replayed_blocks)]),
                from_slot: Some(0),
                ..labelled_request(request_label)
            }
        }

        /// A ping.
        ///
        /// # Arguments
        ///
        /// * `ping_id` - The id its pong carries.
        ///
        /// # Returns
        ///
        /// The request.
        fn ping_request(ping_id: i32) -> SubscribeRequest {
            SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: ping_id }),
                ..Default::default()
            }
        }

        /// The label of the request a posted filter came from.
        ///
        /// # Arguments
        ///
        /// * `posted_filter` - A filter the request task posted.
        ///
        /// # Returns
        ///
        /// The names of the filters that match a Processed slot status, joined with commas, or
        /// an empty string if none does.
        fn request_label(posted_filter: &Filter) -> String {
            let processed_slot =
                Message::Slot(Arc::new(fixtures::message_slot(1, SlotStatus::Processed)));
            posted_filter
                .get_updates(&processed_slot, None)
                .first()
                .map(update_label)
                .unwrap_or_default()
        }

        /// The label of the request whose filter made an update.
        ///
        /// # Arguments
        ///
        /// * `filtered_update` - The update.
        ///
        /// # Returns
        ///
        /// The names of the filters that matched, joined with commas.
        fn update_label(filtered_update: &FilteredUpdate) -> String {
            filtered_update.filters.join(",")
        }

        /// A post of a labelled request's filter.
        ///
        /// # Arguments
        ///
        /// * `request_label` - The label of the request it came from.
        /// * `posted_at_ms` - When it posted.
        /// * `from_slot` - The request's `from_slot`.
        /// * `commitment_level` - The filter's commitment level.
        ///
        /// # Returns
        ///
        /// The post.
        fn request_post(
            request_label: &str,
            posted_at_ms: u64,
            from_slot: Option<u64>,
            commitment_level: CommitmentLevel,
        ) -> RequestTaskPost {
            RequestTaskPost::FilterPosted {
                posted_at: Duration::from_millis(posted_at_ms),
                request_label: request_label.to_owned(),
                from_slot,
                commitment_level,
            }
        }

        /// A post of a labelled request's processed filter, with no `from_slot`.
        ///
        /// # Arguments
        ///
        /// * `request_label` - The label of the request it came from.
        /// * `posted_at_ms` - When it posted.
        ///
        /// # Returns
        ///
        /// The post.
        fn filter_post(request_label: &str, posted_at_ms: u64) -> RequestTaskPost {
            request_post(request_label, posted_at_ms, None, Processed)
        }

        /// The pong the request task queues for a ping.
        ///
        /// # Arguments
        ///
        /// * `ping_id` - The ping's id.
        /// * `queued_at_ms` - When the pong was queued.
        ///
        /// # Returns
        ///
        /// The reply.
        fn pong_reply(ping_id: i32, queued_at_ms: u64) -> QueuedReply {
            let queued_at = Duration::from_millis(queued_at_ms);
            QueuedReply::PingAnswered { queued_at, ping_id }
        }

        /// The filter error of a request with an invalid commitment.
        ///
        /// # Arguments
        ///
        /// * `invalid_commitment` - The request's commitment value.
        /// * `queued_at_ms` - When the error was queued.
        ///
        /// # Returns
        ///
        /// The reply.
        fn commitment_error(invalid_commitment: i32, queued_at_ms: u64) -> QueuedReply {
            QueuedReply::FilterRejected {
                queued_at: Duration::from_millis(queued_at_ms),
                status_message: format!(
                    "failed to create filter: failed to create CommitmentLevel from \
                     {invalid_commitment}"
                ),
            }
        }

        /// Polls `request_task` once, with a waker that does nothing, so that no other task runs
        /// between what the test does and what the task reads.
        ///
        /// # Arguments
        ///
        /// * `request_task` - The request task's future.
        ///
        /// # Panics
        ///
        /// Panics if the task ended.
        fn poll_request_task_once(request_task: Pin<&mut impl Future<Output = ()>>) {
            let task_ended = request_task.now_or_never().is_some();
            assert!(!task_ended, "the request task ended");
        }

        /// Spawns a [`GrpcService::client_loop`] whose session holds the live signal's sender,
        /// with a Subscribe queue that holds five replayed blocks.
        ///
        /// # Arguments
        ///
        /// * `snapshot_rx` - The startup snapshot, or [`None`].
        /// * `replay_stored_slots_tx` - The replay requests, or [`None`] when replay is off.
        ///
        /// # Returns
        ///
        /// The client's request sender and Subscribe queue, and the live signal's receiver.
        fn spawn_client_loop_with_live_signal(
            snapshot_rx: Option<crossbeam_channel::Receiver<Box<Message>>>,
            replay_stored_slots_tx: Option<mpsc::Sender<ReplayStoredSlotsRequest>>,
        ) -> (ClientHandles, oneshot::Receiver<()>) {
            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let weight_scales = WeightScales {
                transaction_data_unit: 0,
                ..WeightScales::default()
            };
            let (stream_tx, stream_rx) = load_aware_channel(50, weight_scales);
            let (client_live_tx, client_live_rx) = oneshot::channel();
            let mut client_session = ClientSession::new(
                0,
                Some("test".into()),
                "test".into(),
                CancellationToken::new(),
                None,
            );
            client_session.client_live_tx = Some(client_live_tx);
            tokio::spawn(GrpcService::client_loop(
                client_session,
                stream_tx,
                client_rx,
                snapshot_rx,
                SubscriberChannels::new(16, 16, 16),
                replay_stored_slots_tx,
                TaskTracker::new(),
                None,
            ));
            ((client_tx, stream_rx), client_live_rx)
        }

        /// Reads the next update and checks that it is a block.
        ///
        /// # Arguments
        ///
        /// * `stream_rx` - The client's Subscribe queue.
        ///
        /// # Panics
        ///
        /// Panics if no block comes within 5 s.
        async fn expect_block(stream_rx: &mut LoadAwareReceiver<TonicResult<FilteredUpdate>>) {
            let client_update = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
                .await
                .expect("an update comes")
                .expect("the stream is open")
                .expect("an update, not an error status");
            assert!(matches!(
                client_update.message,
                FilteredUpdateOneof::Block(_)
            ));
        }

        /// The first request posts at once. Each later change waits until the interval after
        /// the last post.
        #[tokio::test(start_paused = true)]
        async fn the_first_request_applies_at_once_and_later_changes_once_per_interval() {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            request_task.send_labelled_at(1_000, "second").await;
            request_task.expect_filter_post("first", 0).await;
            request_task.expect_filter_post("second", 5_000).await;
            request_task.send_labelled_at(5_500, "third").await;
            request_task.expect_filter_post("third", 10_000).await;
        }

        /// A hundred changes within one interval post once, as the last of them. After a quiet
        /// interval, a change posts at once.
        #[tokio::test(start_paused = true)]
        async fn a_hundred_changes_in_five_seconds_apply_as_the_last_one() {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            for change_number in 0..100 {
                let change_label = format!("change-{change_number}");
                request_task
                    .send_labelled_at(50 * change_number, &change_label)
                    .await;
            }
            request_task.expect_filter_post("first", 0).await;
            request_task.expect_filter_post("change-99", 5_000).await;
            request_task.send_labelled_at(15_000, "after-quiet").await;
            request_task.expect_filter_post("after-quiet", 15_000).await;
        }

        /// An empty request before the first real one does not post, and the real one posts at
        /// once with its `from_slot`. The interval starts at that post.
        #[tokio::test(start_paused = true)]
        async fn empty_requests_before_the_first_neither_post_nor_start_the_interval() {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            request_task.send_at(0, SubscribeRequest::default()).await;
            let first_request = SubscribeRequest {
                from_slot: Some(42),
                ..labelled_request("first")
            };
            request_task.send_at(2_000, first_request).await;
            let first_post = request_post("first", 2_000, Some(42), Processed);
            request_task.expect_post(first_post).await;
            request_task.send_labelled_at(3_000, "change").await;
            request_task.expect_filter_post("change", 7_000).await;
        }

        /// A later request with `from_slot` waits for the interval like any change, and posts
        /// with its `from_slot`, which [`GrpcService::client_loop`] ignores.
        #[tokio::test(start_paused = true)]
        async fn a_later_from_slot_request_waits_like_any_change() {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            let later_request = SubscribeRequest {
                from_slot: Some(7),
                ..labelled_request("later")
            };
            request_task.send_at(1_000, later_request).await;
            request_task.expect_filter_post("first", 0).await;
            let later_post = request_post("later", 5_000, Some(7), Processed);
            request_task.expect_post(later_post).await;
        }

        /// A ping gets its pong at once, and the change that waits still posts at its time.
        #[tokio::test(start_paused = true)]
        async fn a_ping_is_answered_at_once_and_neither_drops_nor_delays_a_waiting_change() {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            request_task.send_labelled_at(1_000, "change").await;
            request_task.send_at(2_000, ping_request(7)).await;
            request_task.expect_reply(Some(pong_reply(7, 2_000))).await;
            request_task.expect_filter_post("first", 0).await;
            request_task.expect_filter_post("change", 5_000).await;
        }

        /// A half-close stops the reads, but the waiting change still posts at its time, and the
        /// task keeps its sender open until the cancellation.
        #[tokio::test(start_paused = true)]
        async fn a_waiting_change_posts_after_a_half_close() {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            request_task.send_labelled_at(1_000, "change").await;
            request_task.half_close_at(1_000).await;
            request_task.expect_filter_post("first", 0).await;
            request_task.expect_filter_post("change", 5_000).await;
            assert_eq!(request_task.next_post().await, None);
            request_task.cancel_session();
            let ended_at = Duration::from_secs(5) + LONGEST_WAIT;
            let task_ended = RequestTaskPost::TaskEnded { ended_at };
            request_task.expect_post(task_ended).await;
        }

        /// A waiting change is compiled only when it would post, so its filter error comes then,
        /// and a change that a newer one replaced never reports one. The error does not restart
        /// the interval.
        #[tokio::test(start_paused = true)]
        async fn an_invalid_change_reports_its_error_when_it_would_apply_and_a_replaced_one_never_does(
        ) {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            let replaced_request = invalid_request("replaced", 77);
            request_task.send_at(1_000, replaced_request).await;
            let newest_request = invalid_request("newest", 99);
            request_task.send_at(2_000, newest_request).await;
            let newest_error = commitment_error(99, 5_000);
            request_task.expect_reply(Some(newest_error)).await;
            request_task.send_labelled_at(6_000, "valid").await;
            request_task.expect_filter_post("first", 0).await;
            request_task.expect_filter_post("valid", 6_000).await;
            request_task.expect_reply(None).await;
        }

        /// A commitment change waits for the interval like any change.
        #[tokio::test(start_paused = true)]
        async fn a_commitment_change_waits_for_the_interval() {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            let confirmed_request = SubscribeRequest {
                commitment: Some(CommitmentLevelProto::Confirmed as i32),
                ..labelled_request("confirmed")
            };
            request_task.send_at(1_000, confirmed_request).await;
            request_task.expect_filter_post("first", 0).await;
            let confirmed_post = request_post("confirmed", 5_000, None, Confirmed);
            request_task.expect_post(confirmed_post).await;
        }

        /// With a zero interval, every change posts at once, two at the same time included.
        #[tokio::test(start_paused = true)]
        async fn a_zero_interval_posts_every_change_at_once() {
            let mut request_task = RequestTaskHarness::spawn_live_client(Duration::ZERO);
            request_task.send_labelled_at(0, "first").await;
            request_task.send_labelled_at(1_000, "second").await;
            request_task.send_labelled_at(1_000, "third").await;
            request_task.send_labelled_at(2_000, "fourth").await;
            request_task.expect_filter_post("first", 0).await;
            request_task.expect_filter_post("second", 1_000).await;
            request_task.expect_filter_post("third", 1_000).await;
            request_task.expect_filter_post("fourth", 2_000).await;
        }

        /// A first request that fails to compile gets its error and does not count as the first:
        /// an empty request after it is still skipped, and the next real one posts at once.
        #[tokio::test(start_paused = true)]
        async fn an_invalid_first_request_does_not_count_as_the_first() {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            request_task
                .send_at(0, invalid_request("invalid", 99))
                .await;
            request_task
                .send_at(1_000, SubscribeRequest::default())
                .await;
            request_task.send_labelled_at(2_000, "first").await;
            request_task.send_labelled_at(3_000, "change").await;
            request_task
                .expect_reply(Some(commitment_error(99, 0)))
                .await;
            request_task.expect_filter_post("first", 2_000).await;
            request_task.expect_filter_post("change", 7_000).await;
        }

        /// An empty request is compiled before it is skipped, so an invalid commitment on it gets
        /// the filter error.
        #[tokio::test(start_paused = true)]
        async fn an_empty_first_request_with_an_invalid_commitment_gets_the_filter_error() {
            let mut request_task = RequestTaskHarness::spawn_live_client(FILTER_APPLY_INTERVAL);
            let empty_request = SubscribeRequest {
                commitment: Some(99),
                ..Default::default()
            };
            request_task.send_at(0, empty_request).await;
            request_task.send_labelled_at(1_000, "first").await;
            request_task
                .expect_reply(Some(commitment_error(99, 0)))
                .await;
            request_task.expect_filter_post("first", 1_000).await;
        }

        /// The waiting change's timer and a newer request are ready in the same poll: the newer
        /// request posts at once, and the waiting change never does. The test polls the task
        /// itself, because `tokio::time::advance` yields, and a spawned task would run first.
        #[tokio::test(start_paused = true)]
        async fn a_request_read_after_the_interval_replaces_the_waiting_one() {
            let started_at = Instant::now();
            let (request_tx, request_rx) = mpsc::unbounded_channel();
            let (client_tx, mut posted_rx) = mpsc::unbounded_channel();
            let (stream_tx, _stream_rx) = load_aware_channel(16, WeightScales::default());
            let (client_live_tx, client_live_rx) = oneshot::channel();
            drop(client_live_tx);
            let request_task = GrpcService::read_requests(
                UnboundedReceiverStream::new(request_rx),
                0,
                "test".to_owned(),
                Arc::new(FilterLimits::default()),
                fixtures::filter_names(),
                FILTER_APPLY_INTERVAL,
                stream_tx,
                client_tx,
                client_live_rx,
                CancellationToken::new(),
            );
            tokio::pin!(request_task);
            let mut next_posted_label = || {
                let filter_post = posted_rx.try_recv().ok().flatten();
                filter_post.map(|(_from_slot, posted_filter)| request_label(&posted_filter))
            };

            request_tx.send(Ok(labelled_request("first"))).unwrap();
            poll_request_task_once(request_task.as_mut());
            assert_eq!(next_posted_label().as_deref(), Some("first"));

            tokio::time::advance(Duration::from_secs(1)).await;
            request_tx.send(Ok(labelled_request("waiting"))).unwrap();
            poll_request_task_once(request_task.as_mut());
            assert_eq!(next_posted_label(), None);

            tokio::time::advance(FILTER_APPLY_INTERVAL - Duration::from_secs(1)).await;
            request_tx.send(Ok(labelled_request("newer"))).unwrap();
            poll_request_task_once(request_task.as_mut());
            assert_eq!(started_at.elapsed(), FILTER_APPLY_INTERVAL);
            assert_eq!(next_posted_label().as_deref(), Some("newer"));

            tokio::time::advance(FILTER_APPLY_INTERVAL * 2).await;
            poll_request_task_once(request_task.as_mut());
            assert_eq!(next_posted_label(), None);
        }

        /// While the client is not live, as during a 12 s replay, no change posts. Once it is,
        /// the newest posts at once, and the next one waits a whole interval.
        #[tokio::test(start_paused = true)]
        async fn no_change_posts_until_the_client_is_live_then_the_newest_posts_at_once() {
            let mut request_task =
                RequestTaskHarness::spawn_before_live_stream(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            for change_number in 1..24 {
                let change_label = format!("change-{change_number}");
                request_task
                    .send_labelled_at(500 * change_number, &change_label)
                    .await;
            }
            request_task.start_live_stream_at(12_000).await;
            request_task.expect_filter_post("first", 0).await;
            request_task.expect_filter_post("change-23", 12_000).await;
            request_task.send_labelled_at(12_500, "live").await;
            request_task.expect_filter_post("live", 17_000).await;
        }

        /// A change sent during a 3 s replay posts when its interval ends, not when the replay
        /// does.
        #[tokio::test(start_paused = true)]
        async fn a_short_replay_posts_no_change_and_the_newest_posts_when_the_interval_ends() {
            let mut request_task =
                RequestTaskHarness::spawn_before_live_stream(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            request_task.send_labelled_at(1_000, "older").await;
            request_task.send_labelled_at(2_000, "newest").await;
            request_task.start_live_stream_at(3_000).await;
            request_task.expect_filter_post("first", 0).await;
            request_task.expect_filter_post("newest", 5_000).await;
        }

        /// An invalid change sent while the client is not live reports its error once the
        /// client is, and one that a newer change replaced never does. Neither restarts the
        /// interval.
        #[tokio::test(start_paused = true)]
        async fn an_invalid_change_sent_during_a_replay_reports_when_it_posts_and_a_replaced_one_never_does(
        ) {
            let mut request_task =
                RequestTaskHarness::spawn_before_live_stream(FILTER_APPLY_INTERVAL);
            request_task.send_labelled_at(0, "first").await;
            let replaced_request = invalid_request("replaced", 77);
            request_task.send_at(1_000, replaced_request).await;
            let newest_request = invalid_request("newest", 99);
            request_task.send_at(2_000, newest_request).await;
            request_task.start_live_stream_at(8_000).await;
            let newest_error = commitment_error(99, 8_000);
            request_task.expect_reply(Some(newest_error)).await;
            request_task.send_labelled_at(9_000, "valid").await;
            request_task.expect_filter_post("first", 0).await;
            request_task.expect_filter_post("valid", 9_000).await;
            request_task.expect_reply(None).await;
        }

        /// The replay and its backlog use the first filter. Of the changes sent during the
        /// replay, only the newest applies, after it, and the later `from_slot` asks for no
        /// second replay.
        #[tokio::test(start_paused = true)]
        async fn changes_sent_during_a_replay_apply_after_it_and_only_the_newest() {
            let mut replaying_client = ReplayingClient::spawn("changes-during-a-replay");
            replaying_client
                .send_at(0, replaying_request("first"))
                .await;
            let middle_request = SubscribeRequest {
                from_slot: Some(7),
                ..labelled_request("middle")
            };
            replaying_client.send_at(1_000, middle_request).await;
            replaying_client.send_labelled_at(2_000, "newest").await;
            let replayed_labels = replaying_client.read_replayed_blocks().await;
            assert_eq!(replayed_labels, ["first"; REPLAYED_BLOCKS]);
            replaying_client
                .expect_live_filter_after_interval("newest")
                .await;
            assert_eq!(replaying_client.end_session().await, [0]);
        }

        /// A ping sent while the replay waits for queue room gets its pong in queue order,
        /// before the replay ends, and the change sent before it still applies after the
        /// replay.
        #[tokio::test(start_paused = true)]
        async fn a_ping_during_a_replay_is_answered_during_it_and_keeps_the_waiting_change() {
            let mut replaying_client = ReplayingClient::spawn("ping-during-a-replay");
            replaying_client
                .send_at(0, replaying_request("first"))
                .await;
            replaying_client.send_labelled_at(1_000, "newest").await;
            replaying_client.send_at(2_000, ping_request(7)).await;
            let mut replayed_labels = Vec::new();
            let mut blocks_before_the_pong = None;
            while replayed_labels.len() < REPLAYED_BLOCKS {
                let client_update = replaying_client.next_update().await;
                match &client_update.message {
                    FilteredUpdateOneof::Block(_) => {
                        replayed_labels.push(update_label(&client_update));
                    }
                    FilteredUpdateOneof::Pong(pong_message) if pong_message.id == 7 => {
                        blocks_before_the_pong = Some(replayed_labels.len());
                    }
                    _ => panic!("a replayed block or the pong comes"),
                }
            }
            assert_eq!(replayed_labels, ["first"; REPLAYED_BLOCKS]);
            let pong_came = blocks_before_the_pong.is_some();
            assert!(pong_came, "the pong comes before the replay ends");
            replaying_client
                .expect_live_filter_after_interval("newest")
                .await;
            assert_eq!(replaying_client.end_session().await, [0]);
        }

        /// A half-close during a replay keeps the client's stream, and the change sent before it
        /// applies after the replay.
        #[tokio::test(start_paused = true)]
        async fn a_half_close_during_a_replay_keeps_the_stream_and_applies_the_waiting_change() {
            let mut replaying_client = ReplayingClient::spawn("half-close-during-a-replay");
            replaying_client
                .send_at(0, replaying_request("first"))
                .await;
            replaying_client.send_labelled_at(1_000, "newest").await;
            replaying_client.half_close_at(1_000).await;
            let replayed_labels = replaying_client.read_replayed_blocks().await;
            assert_eq!(replayed_labels, ["first"; REPLAYED_BLOCKS]);
            replaying_client
                .expect_live_filter_after_interval("newest")
                .await;
            assert!(!replaying_client.client_loop_handle.is_finished());
            assert_eq!(replaying_client.end_session().await, [0]);
        }

        /// A request stream error during a replay ends the client only after the whole replay,
        /// with no error status and the disconnect reason `client_disconnect`.
        #[tokio::test(start_paused = true)]
        async fn a_request_stream_error_during_a_replay_ends_the_stream_after_the_replay() {
            const SUBSCRIBER_ID: &str = "request-stream-error-during-a-replay";
            let mut replaying_client = ReplayingClient::spawn(SUBSCRIBER_ID);
            replaying_client
                .send_at(0, replaying_request("first"))
                .await;
            replaying_client.fail_request_stream_at(1_000).await;
            let replayed_labels = replaying_client.read_replayed_blocks().await;
            assert_eq!(replayed_labels, ["first"; REPLAYED_BLOCKS]);
            replaying_client.expect_stream_end().await;
            let disconnect_count =
                metrics::client_disconnect_count(SUBSCRIBER_ID, "client_disconnect");
            assert_eq!(disconnect_count, 1);
            assert_eq!(replaying_client.end_session().await, [0]);
        }

        /// The live signal stays pending until [`GrpcService::client_loop`] starts its live
        /// stream: at the first request with no replay or snapshot, once the replay's last block
        /// is queued, and at the end of the snapshot.
        #[tokio::test]
        async fn the_live_signal_fires_only_when_the_live_stream_starts() {
            let ((client_tx, _stream_rx), mut client_live_rx) =
                spawn_client_loop_with_live_signal(None, None);
            settle().await;
            assert_eq!(client_live_rx.try_recv(), Err(TryRecvError::Empty));
            let first_filter = create_filter_with_blocks(false);
            client_tx.send(Some((None, first_filter))).unwrap();
            settle().await;
            assert_eq!(client_live_rx.try_recv(), Err(TryRecvError::Closed));

            let (replay_tx, replay_rx) = mpsc::channel(1);
            tokio::spawn(answer_replay(replay_rx, REPLAYED_BLOCKS as u64, 9));
            let ((client_tx, mut stream_rx), mut client_live_rx) =
                spawn_client_loop_with_live_signal(None, Some(replay_tx));
            let first_filter = create_filter_with_blocks(false);
            client_tx.send(Some((Some(0), first_filter))).unwrap();
            for _ in 0..REPLAYED_BLOCKS / 2 {
                expect_block(&mut stream_rx).await;
            }
            settle().await;
            assert_eq!(client_live_rx.try_recv(), Err(TryRecvError::Empty));
            for _ in REPLAYED_BLOCKS / 2..REPLAYED_BLOCKS {
                expect_block(&mut stream_rx).await;
            }
            settle().await;
            assert_eq!(client_live_rx.try_recv(), Err(TryRecvError::Closed));

            let (snapshot_tx, snapshot_rx) = crossbeam_channel::unbounded();
            let snapshot_block =
                fixtures::message_block(1, vec![test_transaction()], Vec::new(), Vec::new());
            snapshot_tx
                .send(Box::new(Message::Block(snapshot_block)))
                .unwrap();
            let ((client_tx, mut stream_rx), mut client_live_rx) =
                spawn_client_loop_with_live_signal(Some(snapshot_rx), None);
            let first_filter = create_filter_with_blocks(false);
            client_tx.send(Some((None, first_filter))).unwrap();
            expect_block(&mut stream_rx).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(client_live_rx.try_recv(), Err(TryRecvError::Empty));
            drop(snapshot_tx);
            let live_signal = tokio::time::timeout(Duration::from_secs(5), client_live_rx)
                .await
                .expect("the live stream starts");
            assert!(live_signal.is_err(), "the sender drops without a value");
        }
    }
}
