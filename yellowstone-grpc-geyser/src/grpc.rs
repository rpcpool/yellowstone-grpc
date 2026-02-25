use {
    crate::{
        config::ConfigGrpc,
        metered::MeteredLayer,
        metrics::{
            self, grpc_subscription_kick_count_inc, incr_grpc_method_call_count,
            set_subscriber_queue_size, DebugClientMessage,
        },
        plugin::{
            filter::{
                limits::FilterLimits,
                message::{FilteredUpdate, FilteredUpdateOneof},
                name::FilterNames,
                Filter,
            },
            message::{
                CommitmentLevel, Message, MessageBlock, MessageBlockMeta, MessageEntry,
                MessageSlot, MessageTransactionInfo, SlotStatus,
            },
            proto::geyser_server::{Geyser, GeyserServer},
        },
        transport::{SpyIncoming, SpyIncomingConfig, DEFAULT_TRAFFIC_REPORTING_THRESHOLD},
        util::stream::{load_aware_channel, LoadAwareReceiver, LoadAwareSender},
        version::GrpcVersionInfo,
    },
    anyhow::Context,
    bytesize::ByteSize,
    log::{error, info},
    prost_types::Timestamp,
    solana_clock::{Slot, MAX_RECENT_BLOCKHASHES},
    solana_pubkey::Pubkey,
    std::{
        collections::{BTreeMap, HashMap},
        net::SocketAddr,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, LazyLock, Mutex as StdMutex,
        },
        time::SystemTime,
    },
    tokio::{
        fs,
        sync::{broadcast, mpsc, oneshot, Mutex, RwLock, Semaphore},
        time::{sleep, Duration, Instant},
    },
    tokio_util::{sync::CancellationToken, task::TaskTracker},
    tonic::{
        service::interceptor,
        transport::{
            server::{Server, TcpConnectInfo, TcpIncoming, TlsConnectInfo},
            Identity, ServerTlsConfig,
        },
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    tonic_health::server::health_reporter,
    yellowstone_grpc_proto::{
        prelude::{
            CommitmentLevel as CommitmentLevelProto, GetBlockHeightRequest, GetBlockHeightResponse,
            GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
            GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
            IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeDeshredRequest,
            SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest,
        },
        prost::Message as ProstMessage,
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
        task_tracker.spawn(async move {
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

#[derive(Debug, Default)]
struct MessageId {
    id: u64,
}

impl MessageId {
    const fn next(&mut self) -> u64 {
        self.id = self.id.checked_add(1).expect("message id overflow");
        self.id
    }
}

#[derive(Debug, Default)]
struct SlotMessages {
    messages: Vec<Option<(u64, Message)>>, // Option is used for accounts with low write_version
    messages_slots: Vec<(u64, Message)>,
    block_meta: Option<Arc<MessageBlockMeta>>,
    transactions: Vec<Arc<MessageTransactionInfo>>,
    accounts_dedup: HashMap<Pubkey, (u64, usize)>, // (write_version, message_index)
    entries: Vec<Arc<MessageEntry>>,
    sealed: bool,
    entries_count: usize,
    confirmed_at: Option<usize>,
    finalized_at: Option<usize>,
    parent_slot: Option<Slot>,
    confirmed: bool,
    finalized: bool,
}

impl SlotMessages {
    pub fn try_seal(&mut self, msgid_gen: &mut MessageId) -> Option<(u64, Message)> {
        if !self.sealed {
            if let Some(block_meta) = &self.block_meta {
                let executed_transaction_count = block_meta.executed_transaction_count as usize;
                let entries_count = block_meta.entries_count as usize;

                // Additional check `entries_count == 0` due to bug of zero entries on block produced by validator
                // See GitHub issue: https://github.com/solana-labs/solana/issues/33823
                if self.transactions.len() == executed_transaction_count
                    && (entries_count == 0 || self.entries.len() == entries_count)
                {
                    let transactions = std::mem::take(&mut self.transactions);
                    let mut entries = std::mem::take(&mut self.entries);
                    if entries_count == 0 {
                        entries.clear();
                    }

                    let mut accounts = Vec::with_capacity(self.messages.len());
                    for item in self.messages.iter().flatten() {
                        if let (_msgid, Message::Account(account)) = item {
                            accounts.push(Arc::clone(&account.account));
                        }
                    }

                    let message_block = Message::Block(Arc::new(MessageBlock::new(
                        Arc::clone(block_meta),
                        transactions,
                        accounts,
                        entries,
                    )));
                    let message = (msgid_gen.next(), message_block);
                    self.messages.push(Some(message.clone()));

                    self.sealed = true;
                    self.entries_count = entries_count;
                    return Some(message);
                }
            }
        }

        None
    }
}

type BroadcastedMessage = (CommitmentLevel, Arc<Vec<(u64, Message)>>);

enum ReplayedResponse {
    Messages(Vec<(u64, Message)>),
    Lagged(Slot),
}

type ReplayStoredSlotsRequest = (CommitmentLevel, Slot, oneshot::Sender<ReplayedResponse>);

static CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR: LazyLock<
    StdMutex<HashMap<SocketAddr, usize>>,
> = LazyLock::new(|| StdMutex::new(HashMap::new()));

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
    debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
    cancellation_token: CancellationToken,
    disconnect_reason: &'static str,
    maybe_remote_peer_sk_addr: Option<SocketAddr>,
}

impl ClientSession {
    fn new(
        id: usize,
        subscriber_id: Option<String>,
        endpoint: String,
        maybe_remote_peer_sk_addr: Option<SocketAddr>,
        debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let filter = Filter::default();
        let subscriber_id = subscriber_id.unwrap_or("UNKNOWN".to_owned());
        if let Some(remote_peer_sk_addr) = maybe_remote_peer_sk_addr {
            let mut subscriptions_per_remote_addr =
                CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR
                    .lock()
                    .expect("CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR mutex poisoned");
            let count = subscriptions_per_remote_addr
                .entry(remote_peer_sk_addr)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            metrics::set_grpc_concurrent_subscribe_per_tcp_connection(
                remote_peer_sk_addr.to_string(),
                *count as u64,
            );
        }
        metrics::update_subscriptions(&endpoint, None, Some(&filter));
        DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateFilter {
            id,
            filter: Box::new(filter.clone()),
        });
        info!("client #{id} ({subscriber_id}): new");
        Self {
            id,
            subscriber_id,
            endpoint,
            filter,
            debug_client_tx,
            cancellation_token,
            disconnect_reason: "unknown",
            maybe_remote_peer_sk_addr,
        }
    }

    fn set_filter(&mut self, new_filter: Filter) {
        metrics::update_subscriptions(&self.endpoint, Some(&self.filter), Some(&new_filter));
        DebugClientMessage::maybe_send(&self.debug_client_tx, || {
            DebugClientMessage::UpdateFilter {
                id: self.id,
                filter: Box::new(new_filter.clone()),
            }
        });
        self.filter = new_filter;
    }
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        if let Some(remote_peer_sk_addr) = self.maybe_remote_peer_sk_addr {
            let mut subscriptions_per_remote_addr =
                CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR
                    .lock()
                    .expect("CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR mutex poisoned");
            if let Some(count) = subscriptions_per_remote_addr.get_mut(&remote_peer_sk_addr) {
                if *count > 1 {
                    *count -= 1;
                    metrics::set_grpc_concurrent_subscribe_per_tcp_connection(
                        remote_peer_sk_addr.to_string(),
                        *count as u64,
                    );
                } else {
                    subscriptions_per_remote_addr.remove(&remote_peer_sk_addr);
                    metrics::set_grpc_concurrent_subscribe_per_tcp_connection(
                        remote_peer_sk_addr.to_string(),
                        0,
                    );
                    metrics::remove_grpc_concurrent_subscribe_per_tcp_connection(
                        remote_peer_sk_addr.to_string(),
                    );
                }
            }
        }
        set_subscriber_queue_size(&self.subscriber_id, 0);
        metrics::incr_client_disconnect(&self.subscriber_id, self.disconnect_reason);
        metrics::update_subscriptions(&self.endpoint, Some(&self.filter), None);
        DebugClientMessage::maybe_send(&self.debug_client_tx, || DebugClientMessage::Removed {
            id: self.id,
        });
        info!(
            "client #{} ({}): removed ({})",
            self.id, self.subscriber_id, self.disconnect_reason
        );
        self.cancellation_token.cancel();
    }
}

type SubscriptionTracker = Arc<Mutex<HashMap<String, usize>>>;

#[derive(Debug)]
pub struct GrpcService {
    config_snapshot_client_channel_capacity: usize,
    config_channel_capacity: usize,
    config_filter_limits: Arc<FilterLimits>,
    config_max_subscription_limit: usize,
    config_max_subscription_limit_dryrun: bool,
    blocks_meta: Option<BlockMetaStorage>,
    subscribe_id: AtomicUsize,
    snapshot_rx: Mutex<Option<crossbeam_channel::Receiver<Box<Message>>>>,
    broadcast_tx: broadcast::Sender<BroadcastedMessage>,
    replay_stored_slots_tx: Option<mpsc::Sender<ReplayStoredSlotsRequest>>,
    replay_first_available_slot: Option<Arc<AtomicU64>>,
    debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
    filter_names: Arc<Mutex<FilterNames>>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    subscription_tracker: SubscriptionTracker,
}

impl GrpcService {
    #[allow(clippy::type_complexity)]
    pub async fn create(
        config: ConfigGrpc,
        debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        is_reload: bool,
        service_cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> anyhow::Result<(
        Option<crossbeam_channel::Sender<Box<Message>>>,
        mpsc::UnboundedSender<Message>,
    )> {
        // Bind service address
        let incoming = TcpIncoming::bind(config.address)?
            .with_nodelay(Some(true))
            .with_keepalive(Some(Duration::from_secs(20)));

        let spy_incoming_config = SpyIncomingConfig {
            traffic_reporting_threshold: config
                .traffic_reporting_byte_threhsold
                .unwrap_or_else(|| ByteSize::b(DEFAULT_TRAFFIC_REPORTING_THRESHOLD)),
        };
        let spy_incoming = SpyIncoming::new(incoming, spy_incoming_config);

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

        // Messages to clients combined by commitment
        let (broadcast_tx, _) = broadcast::channel(config.channel_capacity);
        // attempt to prevent spam of geyser loop with capacity eq 1
        let (replay_first_available_slot, replay_stored_slots_tx, replay_stored_slots_rx) =
            if config.replay_stored_slots == 0 {
                (None, None, None)
            } else {
                let (tx, rx) = mpsc::channel(1);
                (Some(Arc::new(AtomicU64::new(u64::MAX))), Some(tx), Some(rx))
            };

        // gRPC server builder with optional TLS
        let mut server_builder = Server::builder();
        if let Some(tls_config) = &config.tls_config {
            let (cert, key) = tokio::try_join!(
                fs::read(&tls_config.cert_path),
                fs::read(&tls_config.key_path)
            )
            .context("failed to load tls_config files")?;
            server_builder = server_builder
                .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
                .context("failed to apply tls_config")?;
        }
        if let Some(enabled) = config.server_http2_adaptive_window {
            server_builder = server_builder.http2_adaptive_window(Some(enabled));
        }
        if let Some(http2_keepalive_interval) = config.server_http2_keepalive_interval {
            server_builder =
                server_builder.http2_keepalive_interval(Some(http2_keepalive_interval));
        }
        if let Some(http2_keepalive_timeout) = config.server_http2_keepalive_timeout {
            server_builder = server_builder.http2_keepalive_timeout(Some(http2_keepalive_timeout));
        }
        if let Some(sz) = config.server_initial_connection_window_size {
            server_builder = server_builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = config.server_initial_stream_window_size {
            server_builder = server_builder.initial_stream_window_size(sz);
        }

        let filter_names = Arc::new(Mutex::new(FilterNames::new(
            config.filter_name_size_limit,
            config.filter_names_size_limit,
            config.filter_names_cleanup_interval,
        )));

        // Create Server
        let max_decoding_message_size = config.max_decoding_message_size;
        let mut service = GeyserServer::new(Self {
            config_snapshot_client_channel_capacity: config.snapshot_client_channel_capacity,
            config_channel_capacity: config.channel_capacity,
            config_filter_limits: Arc::new(config.filter_limits),
            config_max_subscription_limit: config.max_subscription_limit,
            config_max_subscription_limit_dryrun: config.max_subscription_limit_dryrun,
            blocks_meta,
            subscribe_id: AtomicUsize::new(0),
            snapshot_rx: Mutex::new(snapshot_rx),
            broadcast_tx: broadcast_tx.clone(),
            replay_stored_slots_tx,
            replay_first_available_slot: replay_first_available_slot.clone(),
            debug_clients_tx,
            filter_names,
            cancellation_token: service_cancellation_token.clone(),
            task_tracker: task_tracker.clone(),
            subscription_tracker: Arc::new(Mutex::new(HashMap::new())),
        })
        .max_decoding_message_size(max_decoding_message_size);
        for encoding in config.compression.accept {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.compression.send {
            service = service.send_compressed(encoding);
        }

        // Run geyser message loop
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        task_tracker.spawn(async move {
            Self::geyser_loop(
                messages_rx,
                blocks_meta_tx,
                broadcast_tx,
                replay_stored_slots_rx,
                replay_first_available_slot,
                config.replay_stored_slots,
            )
            .await;
        });

        // Run Server
        let shutdown_grpc = service_cancellation_token.child_token();
        task_tracker.spawn(async move {
            // gRPC Health check service
            let (health_reporter, health_service) = health_reporter();
            health_reporter.set_serving::<GeyserServer<Self>>().await;

            let result = server_builder
                .layer(MeteredLayer::new())
                .layer(interceptor::InterceptorLayer::new(
                    move |request: Request<()>| {
                        if let Some(x_token) = &config.x_token {
                            match request.metadata().get("x-token") {
                                Some(token) if x_token == token => Ok(request),
                                _ => Err(Status::unauthenticated("No valid auth token")),
                            }
                        } else {
                            Ok(request)
                        }
                    },
                ))
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming_shutdown(spy_incoming, shutdown_grpc.cancelled())
                .await;
            info!("gRPC server shut down with result: {result:?}");
        });

        Ok((snapshot_tx, messages_tx))
    }

    async fn geyser_loop(
        mut messages_rx: mpsc::UnboundedReceiver<Message>,
        blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        replay_stored_slots_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        replay_first_available_slot: Option<Arc<AtomicU64>>,
        replay_stored_slots: u64,
    ) {
        const PROCESSED_MESSAGES_MAX: usize = 31;
        const PROCESSED_MESSAGES_SLEEP: Duration = Duration::from_millis(10);
        let mut msgid_gen = MessageId::default();
        let mut messages: BTreeMap<u64, SlotMessages> = Default::default();
        let mut processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
        let mut processed_first_slot = None;
        let processed_sleep = sleep(PROCESSED_MESSAGES_SLEEP);
        tokio::pin!(processed_sleep);
        let (_tx, rx) = mpsc::channel(1);
        let mut replay_stored_slots_rx = replay_stored_slots_rx.unwrap_or(rx);

        loop {
            tokio::select! {
                maybe = messages_rx.recv() => {
                    let Some(message) = maybe else {
                        info!("Geyser loop: messages channel closed");
                        break;
                    };
                    metrics::message_queue_size_dec();
                    let msgid = msgid_gen.next();

                    // Update metrics
                    if let Message::Slot(slot_message) = &message {
                        metrics::update_slot_plugin_status(slot_message.status, slot_message.slot);
                    }

                    // Update blocks info
                    if let Some(blocks_meta_tx) = &blocks_meta_tx {
                        if matches!(&message, Message::Slot(_) | Message::BlockMeta(_)) {
                            let _ = blocks_meta_tx.send(message.clone());
                        }
                    }

                    // Remove outdated block reconstruction info
                    match &message {
                        // On startup we can receive multiple Confirmed/Finalized slots without BlockMeta message
                        // With saved first Processed slot we can ignore errors caused by startup process
                        Message::Slot(msg) if processed_first_slot.is_none() && msg.status == SlotStatus::Processed => {
                            processed_first_slot = Some(msg.slot);
                        }
                        Message::Slot(msg) if msg.status == SlotStatus::Finalized => {
                            // keep extra 10 slots + slots for replay
                            if let Some(msg_slot) = msg.slot.checked_sub(10 + replay_stored_slots) {
                                loop {
                                    match messages.keys().next().cloned() {
                                        Some(slot) if slot < msg_slot => {
                                            if let Some(slot_messages) = messages.remove(&slot) {
                                                match processed_first_slot {
                                                    Some(processed_first) if slot <= processed_first => continue,
                                                    None => continue,
                                                    _ => {}
                                                }

                                                if !slot_messages.sealed && slot_messages.finalized_at.is_some() {
                                                    let mut reasons = vec![];
                                                    if let Some(block_meta) = slot_messages.block_meta {
                                                        let block_txn_count = block_meta.executed_transaction_count as usize;
                                                        let msg_txn_count = slot_messages.transactions.len();
                                                        if block_txn_count != msg_txn_count {
                                                            reasons.push("InvalidTxnCount");
                                                            error!("failed to reconstruct #{slot} -- tx count: {block_txn_count} vs {msg_txn_count}");
                                                        }
                                                        let block_entries_count = block_meta.entries_count as usize;
                                                        let msg_entries_count = slot_messages.entries.len();
                                                        if block_entries_count != msg_entries_count {
                                                            reasons.push("InvalidEntriesCount");
                                                            error!("failed to reconstruct #{slot} -- entries count: {block_entries_count} vs {msg_entries_count}");
                                                        }
                                                    } else {
                                                        reasons.push("NoBlockMeta");
                                                    }
                                                    let reason = reasons.join(",");

                                                    metrics::update_invalid_blocks(format!("failed reconstruct {reason}"));
                                                }
                                            }
                                        }
                                        _ => break,
                                    }
                                }
                                if let Some(stored) = &replay_first_available_slot {
                                    if let Some(slot) = messages.keys().next().copied() {
                                        stored.store(slot, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }

                    // Update block reconstruction info
                    let slot_messages = messages.entry(message.get_slot()).or_default();
                    if let Message::Slot(msg) = &message {
                        match msg.status {
                            SlotStatus::Processed => {
                                slot_messages.parent_slot = msg.parent;
                            },
                            SlotStatus::Confirmed => {
                                slot_messages.confirmed = true;
                            },
                            SlotStatus::Finalized => {
                                slot_messages.finalized = true;
                            },
                            _ => {}
                        }
                    }
                    if matches!(&message, Message::Slot(_)) {
                        slot_messages.messages_slots.push((msgid, message.clone()));
                    } else {
                        slot_messages.messages.push(Some((msgid, message.clone())));

                        // If we already build Block message, new message will be a problem
                        if slot_messages.sealed && !(matches!(&message, Message::Entry(_)) && slot_messages.entries_count == 0) {
                            let kind = match &message {
                                Message::Slot(_) => "Slot",
                                Message::Account(_) => "Account",
                                Message::Transaction(_) => "Transaction",
                                Message::Entry(_) => "Entry",
                                Message::BlockMeta(_) => "BlockMeta",
                                Message::Block(_) => "Block",
                            };
                            metrics::update_invalid_blocks(format!("unexpected message {kind}"));
                        }
                    }
                    let mut sealed_block_msg = None;
                    match &message {
                        Message::BlockMeta(msg) => {
                            if slot_messages.block_meta.is_some() {
                                metrics::update_invalid_blocks("unexpected message: BlockMeta (duplicate)");
                            }
                            slot_messages.block_meta = Some(Arc::clone(msg));
                            sealed_block_msg = slot_messages.try_seal(&mut msgid_gen);
                        }
                        Message::Transaction(msg) => {
                            slot_messages.transactions.push(Arc::clone(&msg.transaction));
                            sealed_block_msg = slot_messages.try_seal(&mut msgid_gen);
                        }
                        // Dedup accounts by max write_version
                        Message::Account(msg) => {
                            metrics::observe_geyser_account_update_received(msg.account.data.len());
                            let write_version = msg.account.write_version;
                            let msg_index = slot_messages.messages.len() - 1;
                            if let Some(entry) = slot_messages.accounts_dedup.get_mut(&msg.account.pubkey) {
                                if entry.0 < write_version {
                                    // We can replace the message, but in this case we will lose the order
                                    slot_messages.messages[entry.1] = None;
                                    *entry = (write_version, msg_index);
                                } else {
                                    // If the new write_version is lower than the latest one, we need to drop this message
                                    // because we would have more than 1 image in slot_messages.messages
                                    slot_messages.messages[msg_index] = None;
                                }
                            } else {
                                slot_messages.accounts_dedup.insert(msg.account.pubkey, (write_version, msg_index));
                            }
                        }
                        Message::Entry(msg) => {
                            slot_messages.entries.push(Arc::clone(msg));
                            sealed_block_msg = slot_messages.try_seal(&mut msgid_gen);
                        }
                        _ => {}
                    }

                    // Send messages to filter (and to clients)
                    let mut messages_vec = Vec::with_capacity(4);
                    if let Some(sealed_block_msg) = sealed_block_msg {
                        messages_vec.push(sealed_block_msg);
                    }
                    let slot_status = if let Message::Slot(msg) = &message {
                        Some((msg.slot, msg.status))
                    } else {
                        None
                    };
                    messages_vec.push((msgid, message));

                    // sometimes we do not receive all statuses
                    if let Some((slot, status)) = slot_status {
                        let mut slots = vec![slot];
                        while let Some((parent, Some(entry))) = slots
                            .pop()
                            .and_then(|slot| messages.get(&slot))
                            .and_then(|entry| entry.parent_slot)
                            .map(|parent| (parent, messages.get_mut(&parent)))
                        {
                            if (status == SlotStatus::Confirmed && !entry.confirmed) ||
                                (status == SlotStatus::Finalized && !entry.finalized)
                            {
                                if status == SlotStatus::Confirmed {
                                    entry.confirmed = true;
                                } else if status == SlotStatus::Finalized {
                                    entry.finalized = true;
                                }

                                slots.push(parent);
                                let message_slot = Message::Slot(MessageSlot {
                                    slot: parent,
                                    parent: entry.parent_slot,
                                    status,
                                    dead_error: None,
                                    created_at: Timestamp::from(SystemTime::now())
                                });
                                messages_vec.push((msgid_gen.next(), message_slot));
                                metrics::missed_status_message_inc(status);
                            }
                        }
                    }

                    for message in messages_vec.into_iter().rev() {
                        if let Message::Slot(slot) = &message.1 {
                            let (mut confirmed_messages, mut finalized_messages) = match slot.status {
                                SlotStatus::Processed | SlotStatus::FirstShredReceived | SlotStatus::Completed | SlotStatus::CreatedBank | SlotStatus::Dead => {
                                    (Vec::with_capacity(1), Vec::with_capacity(1))
                                }
                                SlotStatus::Confirmed => {
                                    if let Some(slot_messages) = messages.get_mut(&slot.slot) {
                                        if !slot_messages.sealed {
                                            slot_messages.confirmed_at = Some(slot_messages.messages.len());
                                        }
                                    }

                                    let vec = messages
                                        .get(&slot.slot)
                                        .map(|slot_messages| slot_messages.messages.iter().flatten().cloned().collect())
                                        .unwrap_or_default();
                                    (vec, Vec::with_capacity(1))
                                }
                                SlotStatus::Finalized => {
                                    if let Some(slot_messages) = messages.get_mut(&slot.slot) {
                                        if !slot_messages.sealed {
                                            slot_messages.finalized_at = Some(slot_messages.messages.len());
                                        }
                                    }

                                    let vec = messages
                                        .get_mut(&slot.slot)
                                        .map(|slot_messages| slot_messages.messages.iter().flatten().cloned().collect())
                                        .unwrap_or_default();
                                    (Vec::with_capacity(1), vec)
                                }
                            };

                            // processed
                            processed_messages.push(message.clone());
                            let _ =
                                broadcast_tx.send((CommitmentLevel::Processed, processed_messages.into()));
                            processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
                            processed_sleep
                                .as_mut()
                                .reset(Instant::now() + PROCESSED_MESSAGES_SLEEP);

                            // confirmed
                            confirmed_messages.push(message.clone());
                            let _ =
                                broadcast_tx.send((CommitmentLevel::Confirmed, confirmed_messages.into()));

                            // finalized
                            finalized_messages.push(message);
                            let _ =
                                broadcast_tx.send((CommitmentLevel::Finalized, finalized_messages.into()));
                        } else {
                            let mut confirmed_messages = vec![];
                            let mut finalized_messages = vec![];
                            if matches!(&message.1, Message::Block(_)) {
                                if let Some(slot_messages) = messages.get(&message.1.get_slot()) {
                                    if let Some(confirmed_at) = slot_messages.confirmed_at {
                                        confirmed_messages.extend(
                                            slot_messages.messages.as_slice()[confirmed_at..].iter().filter_map(|x| x.clone())
                                        );
                                    }
                                    if let Some(finalized_at) = slot_messages.finalized_at {
                                        finalized_messages.extend(
                                            slot_messages.messages.as_slice()[finalized_at..].iter().filter_map(|x| x.clone())
                                        );
                                    }
                                }
                            }

                            processed_messages.push(message);
                            if processed_messages.len() >= PROCESSED_MESSAGES_MAX
                                || !confirmed_messages.is_empty()
                                || !finalized_messages.is_empty()
                            {
                                let _ = broadcast_tx
                                    .send((CommitmentLevel::Processed, processed_messages.into()));
                                processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
                                processed_sleep
                                    .as_mut()
                                    .reset(Instant::now() + PROCESSED_MESSAGES_SLEEP);
                            }

                            if !confirmed_messages.is_empty() {
                                let _ =
                                    broadcast_tx.send((CommitmentLevel::Confirmed, confirmed_messages.into()));
                            }

                            if !finalized_messages.is_empty() {
                                let _ =
                                    broadcast_tx.send((CommitmentLevel::Finalized, finalized_messages.into()));
                            }
                        }
                    }
                }
                () = &mut processed_sleep => {
                    if !processed_messages.is_empty() {
                        let _ = broadcast_tx.send((CommitmentLevel::Processed, processed_messages.into()));
                        processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
                    }
                    processed_sleep.as_mut().reset(Instant::now() + PROCESSED_MESSAGES_SLEEP);
                }
                Some((commitment, replay_slot, tx)) = replay_stored_slots_rx.recv() => {
                    if let Some((slot, _)) = messages.first_key_value() {
                        if replay_slot < *slot {
                            let _ = tx.send(ReplayedResponse::Lagged(*slot));
                            continue;
                        }
                    }

                    let mut replayed_messages = Vec::with_capacity(32_768);
                    for (slot, messages) in messages.iter() {
                        if *slot >= replay_slot {
                            replayed_messages.extend_from_slice(&messages.messages_slots);
                            if commitment == CommitmentLevel::Processed
                                || (commitment == CommitmentLevel::Finalized && messages.finalized)
                                || (commitment == CommitmentLevel::Confirmed && messages.confirmed)
                            {
                                replayed_messages.extend(messages.messages.iter().filter_map(|v| v.clone()));
                            }
                        }
                    }
                    let _ = tx.send(ReplayedResponse::Messages(replayed_messages));
                }
                else => break,
            }
        }

        info!("Geyser loop exiting");
    }

    #[allow(clippy::too_many_arguments)]
    async fn client_loop(
        id: usize,
        subscriber_id: Option<String>,
        endpoint: String,
        stream_tx: LoadAwareSender<TonicResult<FilteredUpdate>>,
        mut client_rx: mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        mut snapshot_rx: Option<crossbeam_channel::Receiver<Box<Message>>>,
        mut messages_rx: broadcast::Receiver<BroadcastedMessage>,
        replay_stored_slots_tx: Option<mpsc::Sender<ReplayStoredSlotsRequest>>,
        debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        maybe_remote_peer_sk_addr: Option<SocketAddr>,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
        subscription_tracker: SubscriptionTracker,
    ) {
        let mut session = ClientSession::new(
            id,
            subscriber_id.clone(),
            endpoint,
            maybe_remote_peer_sk_addr,
            debug_client_tx,
            cancellation_token,
        );
        let cancellation_token = session.cancellation_token.clone();

        if let Some(snapshot_rx) = snapshot_rx.take() {
            info!("client #{id}: snapshot requested");
            let result = Self::client_loop_snapshot(
                id,
                &session.endpoint,
                stream_tx.clone(),
                &mut client_rx,
                snapshot_rx,
                &mut session.filter,
                cancellation_token.clone(),
            )
            .await;
            match result {
                Ok(()) => {
                    info!("client #{id}: snapshot stream ended");
                }
                Err(ClientSnapshotReplayError::Cancelled) => {
                    let _ = stream_tx.try_send(Err(Status::internal(
                        "server is shutting down try again later",
                    )));
                    session.disconnect_reason = "server_shutdown";
                    return;
                }
                Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed) => {
                    info!("client #{id}: grpc connection closed");
                    session.disconnect_reason = "client_closed";
                    return;
                }
            }
        } else {
            info!("client #{id}: no snapshot requested");
        }

        'outer: loop {
            set_subscriber_queue_size(&session.subscriber_id, stream_tx.queue_size());

            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("client #{id}: cancelled");
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
                            info!("client #{id}: filter updated");

                            if let Some(from_slot) = from_slot {
                                let Some(replay_stored_slots_tx) = &replay_stored_slots_tx else {
                                    info!("client #{id}: from_slot is not supported");
                                    task_tracker.spawn(async move {
                                        let _ = stream_tx.send(Err(Status::internal("from_slot is not supported"))).await;
                                    });
                                    session.disconnect_reason = "from_slot_unsupported";
                                    break 'outer;
                                };

                                let (tx, rx) = oneshot::channel();
                                let commitment = session.filter.get_commitment_level();
                                if let Err(_error) = replay_stored_slots_tx.send((commitment, from_slot, tx)).await {
                                    error!("client #{id}: failed to send from_slot request");
                                    task_tracker.spawn(async move {
                                        let _ = stream_tx.send(Err(Status::internal("failed to send from_slot request"))).await;
                                    });
                                    session.disconnect_reason = "replay_error";
                                    break 'outer;
                                }

                                let mut messages = match rx.await {
                                    Ok(ReplayedResponse::Messages(messages)) => messages,
                                    Ok(ReplayedResponse::Lagged(slot)) => {
                                        info!("client #{id}: broadcast from {from_slot} is not available");
                                        task_tracker.spawn(async move {
                                            let message = format!(
                                                "broadcast from {from_slot} is not available, last available: {slot}"
                                            );
                                            let _ = stream_tx.send(Err(Status::internal(message))).await;
                                        });
                                        session.disconnect_reason = "slot_unavailable";
                                        break 'outer;
                                    },
                                    Err(_error) => {
                                        error!("client #{id}: failed to get replay response");
                                        task_tracker.spawn(async move {
                                            let _ = stream_tx.send(Err(Status::internal("failed to get replay response"))).await;
                                        });
                                        session.disconnect_reason = "replay_error";
                                        break 'outer;
                                    }
                                };

                                messages.sort_by_key(|msg| msg.0);
                                for (_msgid, message) in messages.iter() {
                                    for message in session.filter.get_updates(message, Some(commitment)) {
                                        let proto_size = message.encoded_len().min(u32::MAX as usize) as u32;
                                        match stream_tx.send(Ok(message)).await {
                                            Ok(()) => {
                                                metrics::incr_grpc_message_sent_counter(&session.subscriber_id);
                                                metrics::incr_grpc_bytes_sent(&session.subscriber_id, proto_size);
                                            }
                                            Err(mpsc::error::SendError(_)) => {
                                                error!("client #{id}: stream closed");
                                                session.disconnect_reason = "client_closed";
                                                break 'outer;
                                            }
                                        }
                                    }
                                }
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
                    let (commitment, messages) = match message {
                        Ok((commitment, messages)) => (commitment, messages),
                        Err(broadcast::error::RecvError::Closed) => {
                            session.disconnect_reason = "broadcast_closed";
                            break 'outer;
                        },
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            info!("client #{id}: lagged to receive geyser messages");
                            task_tracker.spawn(async move {
                                let _ = stream_tx.send(Err(Status::internal("lagged to receive geyser messages"))).await;
                            });
                            session.disconnect_reason = "client_broadcast_lag";
                            break 'outer;
                        }
                    };

                    if commitment == session.filter.get_commitment_level() {
                        for (_msgid, message) in messages.iter() {
                            for message in session.filter.get_updates(message, Some(commitment)) {
                                let proto_size = message.encoded_len().min(u32::MAX as usize) as u32;
                                match stream_tx.try_send(Ok(message)) {
                                    Ok(()) => {
                                        metrics::incr_grpc_message_sent_counter(&session.subscriber_id);
                                        metrics::incr_grpc_bytes_sent(&session.subscriber_id, proto_size);
                                    }
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        error!("client #{id}: lagged to send an update");
                                        task_tracker.spawn(async move {
                                            let _ = stream_tx.send(Err(Status::internal("lagged to send an update"))).await;
                                        });
                                        session.disconnect_reason = "client_channel_full";
                                        break 'outer;
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        error!("client #{id}: stream closed");
                                        session.disconnect_reason = "client_closed";
                                        break 'outer;
                                    }
                                }
                            }
                        }
                    }

                    if commitment == CommitmentLevel::Processed && session.debug_client_tx.is_some() {
                        for message in messages.iter() {
                            if let Message::Slot(slot_message) = &message.1 {
                                DebugClientMessage::maybe_send(&session.debug_client_tx, || DebugClientMessage::UpdateSlot { id, slot: slot_message.slot });
                            }
                        }
                    }
                }
            }
        }

        // Decrement SubscriptionTracker when client_loop ends.
        if let Some(subscriber_id) = subscriber_id {
            let mut tracker = subscription_tracker.lock().await;
            if let Some(count) = tracker.get_mut(&subscriber_id) {
                info!("decrementing subscription tracker count ({count}) for {subscriber_id:?}");
                *count -= 1;
                info!("new count for {subscriber_id:?} is {count}");
                if *count <= 0 {
                    info!("no more open clients for {subscriber_id:?} removing from tracker");
                    tracker.remove(&subscriber_id);
                }
            }
        }
    }

    async fn client_loop_snapshot(
        id: usize,
        endpoint: &str,
        stream_tx: LoadAwareSender<TonicResult<FilteredUpdate>>,
        client_rx: &mut mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        snapshot_rx: crossbeam_channel::Receiver<Box<Message>>,
        filter: &mut Filter,
        cancellation_token: CancellationToken,
    ) -> Result<(), ClientSnapshotReplayError> {
        info!("client #{id}: going to receive snapshot data");

        // we start with default filter, for snapshot we need wait actual filter first
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("client #{id}: cancelled");
                    return Err(ClientSnapshotReplayError::Cancelled);
                }
                maybe = client_rx.recv() => {
                    match maybe {
                        Some(Some((_from_slot, filter_new))) => {
                            if let Some(msg) = filter_new.get_pong_msg() {
                                if stream_tx.send(Ok(msg)).await.is_err() {
                                    error!("client #{id}: stream closed");
                                    return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                                }
                                continue;
                            }

                            metrics::update_subscriptions(endpoint, Some(filter), Some(&filter_new));
                            *filter = filter_new;
                            info!("client #{id}: filter updated");
                            break;
                        }
                        Some(None) => {
                            return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                        }
                        None => {
                            return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                        }
                    }
                }

            }
        }

        loop {
            if cancellation_token.is_cancelled() {
                info!("client #{id}: cancelled");
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
                    info!("client #{id}: end of startup");
                    break;
                }
            };

            for message in filter.get_updates(&message, None) {
                if stream_tx.send(Ok(message)).await.is_err() {
                    error!("client #{id}: stream closed");
                    return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                }
            }
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = LoadAwareReceiver<TonicResult<FilteredUpdate>>;
    type SubscribeDeshredStream =
        LoadAwareReceiver<TonicResult<yellowstone_grpc_proto::geyser::SubscribeUpdateDeshred>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let subscriber_id = request
            .metadata()
            .get("x-subscription-id")
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            .or(request.remote_addr().map(|addr| addr.ip().to_string()));

        if let Some(id) = subscriber_id.clone() {
            let subscription_tracker_ref = self.subscription_tracker.clone();
            let mut tracker = subscription_tracker_ref.lock().await;
            let count = tracker.entry(id.clone()).or_insert_with(|| 0);

            // Check limit.
            if *count >= self.config_max_subscription_limit {
                info!("{subscriber_id:?} reached max subscription limit. kicking");
                grpc_subscription_kick_count_inc(&id);

                // Kick if not dryrun.
                if !self.config_max_subscription_limit_dryrun {
                    return Err(Status::resource_exhausted(
                        "max subscription limit exceeded",
                    ));
                }
            }

            info!("incrementing subscription tracker count for {subscriber_id:?}");
            *count += 1;
            info!("new count for {subscriber_id:?} is {count}");
        } // Lock dropped here.

        incr_grpc_method_call_count("subscribe");
        let maybe_remote_peer_sk_addr = request
            .extensions()
            .get::<TcpConnectInfo>()
            .or_else(|| {
                request
                    .extensions()
                    .get::<TlsConnectInfo<TcpConnectInfo>>()
                    .map(|tls_info| tls_info.get_ref())
            })
            .and_then(|info| info.remote_addr());

        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);
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

        let (stream_tx, stream_rx) = load_aware_channel(if snapshot_rx.is_some() {
            self.config_snapshot_client_channel_capacity
        } else {
            self.config_channel_capacity
        });
        let (client_tx, client_rx) = mpsc::unbounded_channel();

        let ping_stream_tx = stream_tx.clone();
        let ping_cancellation_token = client_cancellation_token.clone();
        let ping_client_cancel = client_cancellation_token.clone();
        self.task_tracker.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = ping_cancellation_token.cancelled() => {
                        info!("client #{id}: ping cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        let msg = FilteredUpdate::new_empty(FilteredUpdateOneof::ping());
                        log::info!("client #{id}: sending ping");
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
                            info!("detected dead client #{id}");
                            break;
                        }
                    }
                }
            }
            info!("client #{id}: ping task exiting");
        });

        let endpoint = request
            .metadata()
            .get("x-endpoint")
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "".to_owned());

        let config_filter_limits = Arc::clone(&self.config_filter_limits);
        let filter_names = Arc::clone(&self.filter_names);
        let incoming_stream_tx = stream_tx.clone();
        let incoming_client_tx = client_tx;
        let incoming_cancellation_token = client_cancellation_token.child_token();

        self.task_tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = incoming_cancellation_token.cancelled() => {
                        info!("client #{id}: filter receiver cancelled");
                        break;
                    }
                    message = request.get_mut().message() => match message {
                        Ok(Some(request)) => {
                            let mut filter_names = filter_names.lock().await;
                            filter_names.try_clean();

                            if let Err(error) = match Filter::new(&request, &config_filter_limits, &mut filter_names) {
                                Ok(filter) => {
                                    if let Some(msg) = filter.get_pong_msg() {
                                        if incoming_stream_tx.send(Ok(msg)).await.is_err() {
                                            error!("client #{id}: stream closed");
                                            let _ = incoming_client_tx.send(None);
                                            break;
                                        }
                                        continue;
                                    }
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
                            info!("client #{id}: client closed send stream, waiting for cancellation");
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

        self.task_tracker.spawn(Self::client_loop(
            id,
            subscriber_id,
            endpoint,
            stream_tx,
            client_rx,
            snapshot_rx,
            self.broadcast_tx.subscribe(),
            self.replay_stored_slots_tx.clone(),
            self.debug_clients_tx.clone(),
            maybe_remote_peer_sk_addr,
            client_cancellation_token,
            self.task_tracker.clone(),
            self.subscription_tracker.clone(),
        ));

        Ok(Response::new(stream_rx))
    }

    async fn subscribe_deshred(
        &self,
        _request: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> TonicResult<Response<Self::SubscribeDeshredStream>> {
        incr_grpc_method_call_count("subscribe_deshred");
        Err(Status::unimplemented(
            "SubscribeDeshred is not available on this server",
        ))
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            plugin::filter::{limits::FilterLimits, name::FilterNames, Filter},
            util::stream::load_aware_channel,
        },
        yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeRequestFilterSlots},
    };

    fn create_filter_with_slots() -> Filter {
        let config = SubscribeRequest {
            slots: HashMap::from([("test".into(), SubscribeRequestFilterSlots::default())]),
            ..Default::default()
        };
        let mut names = FilterNames::new(64, 1024, Duration::from_secs(1));
        Filter::new(&config, &FilterLimits::default(), &mut names).unwrap()
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
        let (broadcast_tx, _) = broadcast::channel::<BroadcastedMessage>(16);
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let (stream_tx, stream_rx) = load_aware_channel(16);
        let (half_close_tx, half_close_rx) = oneshot::channel();

        // mirrors the incoming handler spawned in subscribe()
        let incoming_ct = ct.child_token();
        tokio::spawn(incoming_handler(
            client_tx,
            create_filter_with_slots(),
            half_close_rx,
            incoming_ct,
        ));

        let handle = tokio::spawn(GrpcService::client_loop(
            0,
            Some("test".into()),
            "test".into(),
            stream_tx,
            client_rx,
            None,
            broadcast_tx.subscribe(),
            None,
            None,
            None,
            ct.clone(),
            tt.clone(),
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
        let msg = Message::Slot(MessageSlot {
            slot: 100,
            parent: Some(99),
            status: SlotStatus::Processed,
            dead_error: None,
            created_at: Timestamp::from(SystemTime::now()),
        });
        let _ = broadcast_tx.send((CommitmentLevel::Processed, Arc::new(vec![(1, msg)])));

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("client_loop did not exit")
            .expect("client_loop panicked");

        assert!(ct.is_cancelled());
    }
}
