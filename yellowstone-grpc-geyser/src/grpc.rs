use {
    crate::{
        config::{ConfigGrpc, ConfigTokio},
        metrics::{self, DebugClientMessage},
        version::GrpcVersionInfo,
    },
    anyhow::Context,
    log::{error, info},
    solana_sdk::{
        clock::{Slot, MAX_RECENT_BLOCKHASHES},
        pubkey::Pubkey,
    },
    std::{
        collections::{BTreeMap, HashMap},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    },
    tokio::{
        fs,
        runtime::Builder,
        sync::{broadcast, mpsc, Mutex, Notify, RwLock, Semaphore},
        task::spawn_blocking,
        time::{sleep, Duration, Instant},
    },
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        service::interceptor::interceptor,
        transport::{
            server::{Server, TcpIncoming},
            Identity, ServerTlsConfig,
        },
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    tonic_health::server::health_reporter,
    yellowstone_grpc_proto::{
        plugin::{
            filter::{
                limits::FilterLimits,
                message::{FilteredUpdate, FilteredUpdateOneof},
                name::FilterNames,
                Filter,
            },
            message::{
                CommitmentLevel, Message, MessageBlock, MessageBlockMeta, MessageEntry,
                MessageSlot, MessageTransactionInfo,
            },
            proto::geyser_server::{Geyser, GeyserServer},
        },
        prelude::{
            CommitmentLevel as CommitmentLevelProto, GetBlockHeightRequest, GetBlockHeightResponse,
            GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
            GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
            IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeRequest,
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
    fn new(unary_concurrency_limit: usize) -> (Self, mpsc::UnboundedSender<Message>) {
        let inner = Arc::new(RwLock::new(BlockMetaStorageInner::default()));
        let (tx, mut rx) = mpsc::unbounded_channel();

        let storage = Arc::clone(&inner);
        tokio::spawn(async move {
            const KEEP_SLOTS: u64 = 3;

            while let Some(message) = rx.recv().await {
                let mut storage = storage.write().await;
                match message {
                    Message::Slot(msg) => {
                        match msg.status {
                            CommitmentLevel::Processed => &mut storage.processed,
                            CommitmentLevel::Confirmed => &mut storage.confirmed,
                            CommitmentLevel::Finalized => &mut storage.finalized,
                        }
                        .replace(msg.slot);

                        if let Some(blockhash) = storage
                            .blocks
                            .get(&msg.slot)
                            .map(|block| block.blockhash.clone())
                        {
                            let entry = storage
                                .blockhashes
                                .entry(blockhash)
                                .or_insert_with(|| BlockhashStatus::new(msg.slot));

                            let status = match msg.status {
                                CommitmentLevel::Processed => &mut entry.processed,
                                CommitmentLevel::Confirmed => &mut entry.confirmed,
                                CommitmentLevel::Finalized => &mut entry.finalized,
                            };
                            *status = true;
                        }

                        if msg.status == CommitmentLevel::Finalized {
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
struct SlotMessages {
    messages: Vec<Option<Message>>, // Option is used for accounts with low write_version
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
    pub fn try_seal(&mut self) -> Option<Message> {
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
                        if let Message::Account(account) = item {
                            accounts.push(Arc::clone(&account.account));
                        }
                    }

                    let message = Message::Block(Arc::new(MessageBlock::new(
                        Arc::clone(block_meta),
                        transactions,
                        accounts,
                        entries,
                    )));
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

#[derive(Debug)]
pub struct GrpcService {
    config_snapshot_client_channel_capacity: usize,
    config_channel_capacity: usize,
    config_filter_limits: Arc<FilterLimits>,
    blocks_meta: Option<BlockMetaStorage>,
    subscribe_id: AtomicUsize,
    snapshot_rx: Mutex<Option<crossbeam_channel::Receiver<Box<Message>>>>,
    broadcast_tx: broadcast::Sender<(CommitmentLevel, Arc<Vec<Message>>)>,
    debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
    filter_names: Arc<Mutex<FilterNames>>,
}

impl GrpcService {
    #[allow(clippy::type_complexity)]
    pub async fn create(
        config_tokio: ConfigTokio,
        config: ConfigGrpc,
        debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        is_reload: bool,
    ) -> anyhow::Result<(
        Option<crossbeam_channel::Sender<Box<Message>>>,
        mpsc::UnboundedSender<Message>,
        Arc<Notify>,
    )> {
        // Bind service address
        let incoming = TcpIncoming::new(
            config.address,
            true,                          // tcp_nodelay
            Some(Duration::from_secs(20)), // tcp_keepalive
        )
        .map_err(|error| anyhow::anyhow!(error))?;

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
            let (blocks_meta, blocks_meta_tx) =
                BlockMetaStorage::new(config.unary_concurrency_limit);
            (Some(blocks_meta), Some(blocks_meta_tx))
        };

        // Messages to clients combined by commitment
        let (broadcast_tx, _) = broadcast::channel(config.channel_capacity);

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
            blocks_meta,
            subscribe_id: AtomicUsize::new(0),
            snapshot_rx: Mutex::new(snapshot_rx),
            broadcast_tx: broadcast_tx.clone(),
            debug_clients_tx,
            filter_names,
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
        spawn_blocking(move || {
            let mut builder = Builder::new_multi_thread();
            if let Some(worker_threads) = config_tokio.worker_threads {
                builder.worker_threads(worker_threads);
            }
            if let Some(tokio_cpus) = config_tokio.affinity.clone() {
                builder.on_thread_start(move || {
                    affinity::set_thread_affinity(&tokio_cpus).expect("failed to set affinity")
                });
            }
            builder
                .thread_name_fn(crate::get_thread_name)
                .enable_all()
                .build()
                .expect("Failed to create a new runtime for geyser loop")
                .block_on(Self::geyser_loop(messages_rx, blocks_meta_tx, broadcast_tx));
        });

        // Run Server
        let shutdown = Arc::new(Notify::new());
        let shutdown_grpc = Arc::clone(&shutdown);
        tokio::spawn(async move {
            // gRPC Health check service
            let (mut health_reporter, health_service) = health_reporter();
            health_reporter.set_serving::<GeyserServer<Self>>().await;

            server_builder
                .http2_keepalive_interval(Some(Duration::from_secs(5)))
                .layer(interceptor(move |request: Request<()>| {
                    if let Some(x_token) = &config.x_token {
                        match request.metadata().get("x-token") {
                            Some(token) if x_token == token => Ok(request),
                            _ => Err(Status::unauthenticated("No valid auth token")),
                        }
                    } else {
                        Ok(request)
                    }
                }))
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown_grpc.notified())
                .await
        });

        Ok((snapshot_tx, messages_tx, shutdown))
    }

    async fn geyser_loop(
        mut messages_rx: mpsc::UnboundedReceiver<Message>,
        blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
        broadcast_tx: broadcast::Sender<(CommitmentLevel, Arc<Vec<Message>>)>,
    ) {
        const PROCESSED_MESSAGES_MAX: usize = 31;
        const PROCESSED_MESSAGES_SLEEP: Duration = Duration::from_millis(10);

        let mut messages: BTreeMap<u64, SlotMessages> = Default::default();
        let mut processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
        let mut processed_first_slot = None;
        let processed_sleep = sleep(PROCESSED_MESSAGES_SLEEP);
        tokio::pin!(processed_sleep);

        loop {
            tokio::select! {
                Some(message) = messages_rx.recv() => {
                    metrics::message_queue_size_dec();

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
                        Message::Slot(msg) if processed_first_slot.is_none() && msg.status == CommitmentLevel::Processed => {
                            processed_first_slot = Some(msg.slot);
                        }
                        Message::Slot(msg) if msg.status == CommitmentLevel::Finalized => {
                            // keep extra 10 slots
                            if let Some(msg_slot) = msg.slot.checked_sub(10) {
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
                            }
                        }
                        _ => {}
                    }

                    // Update block reconstruction info
                    let slot_messages = messages.entry(message.get_slot()).or_default();
                    if let Message::Slot(msg) = &message {
                        match msg.status {
                            CommitmentLevel::Processed => {
                                slot_messages.parent_slot = msg.parent;
                            },
                            CommitmentLevel::Confirmed => {
                                slot_messages.confirmed = true;
                            },
                            CommitmentLevel::Finalized => {
                                slot_messages.finalized = true;
                            },
                        }
                    }
                    if !matches!(&message, Message::Slot(_)) {
                        slot_messages.messages.push(Some(message.clone()));

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
                            sealed_block_msg = slot_messages.try_seal();
                        }
                        Message::Transaction(msg) => {
                            slot_messages.transactions.push(Arc::clone(&msg.transaction));
                            sealed_block_msg = slot_messages.try_seal();
                        }
                        // Dedup accounts by max write_version
                        Message::Account(msg) => {
                            let write_version = msg.account.write_version;
                            let msg_index = slot_messages.messages.len() - 1;
                            if let Some(entry) = slot_messages.accounts_dedup.get_mut(&msg.account.pubkey) {
                                if entry.0 < write_version {
                                    // We can replace the message, but in this case we will lose the order
                                    slot_messages.messages[entry.1] = None;
                                    *entry = (write_version, msg_index);
                                }
                            } else {
                                slot_messages.accounts_dedup.insert(msg.account.pubkey, (write_version, msg_index));
                            }
                        }
                        Message::Entry(msg) => {
                            slot_messages.entries.push(Arc::clone(msg));
                            sealed_block_msg = slot_messages.try_seal();
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
                    messages_vec.push(message);

                    // sometimes we do not receive all statuses
                    if let Some((slot, status)) = slot_status {
                        let mut slots = vec![slot];
                        while let Some((parent, Some(entry))) = slots
                            .pop()
                            .and_then(|slot| messages.get(&slot))
                            .and_then(|entry| entry.parent_slot)
                            .map(|parent| (parent, messages.get_mut(&parent)))
                        {
                            if (status == CommitmentLevel::Confirmed && !entry.confirmed) ||
                                (status == CommitmentLevel::Finalized && !entry.finalized)
                            {
                                if status == CommitmentLevel::Confirmed {
                                    entry.confirmed = true;
                                } else if status == CommitmentLevel::Finalized {
                                    entry.finalized = true;
                                }

                                slots.push(parent);
                                messages_vec.push(Message::Slot(MessageSlot {
                                    slot: parent,
                                    parent: entry.parent_slot,
                                    status,
                                }));
                                metrics::missed_status_message_inc(status);
                            }
                        }
                    }

                    for message in messages_vec.into_iter().rev() {
                        if let Message::Slot(slot) = &message {
                            let (mut confirmed_messages, mut finalized_messages) = match slot.status {
                                CommitmentLevel::Processed => {
                                    (Vec::with_capacity(1), Vec::with_capacity(1))
                                }
                                CommitmentLevel::Confirmed => {
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
                                CommitmentLevel::Finalized => {
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
                            if matches!(&message, Message::Block(_)) {
                                if let Some(slot_messages) = messages.get(&message.get_slot()) {
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
                else => break,
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn client_loop(
        id: usize,
        endpoint: String,
        stream_tx: mpsc::Sender<TonicResult<FilteredUpdate>>,
        mut client_rx: mpsc::UnboundedReceiver<Option<Filter>>,
        mut snapshot_rx: Option<crossbeam_channel::Receiver<Box<Message>>>,
        mut messages_rx: broadcast::Receiver<(CommitmentLevel, Arc<Vec<Message>>)>,
        debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        drop_client: impl FnOnce(),
    ) {
        let mut filter = Filter::default();
        metrics::update_subscriptions(&endpoint, None, Some(&filter));

        metrics::connections_total_inc();
        DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateFilter {
            id,
            filter: Box::new(filter.clone()),
        });
        info!("client #{id}: new");

        let mut is_alive = true;
        if let Some(snapshot_rx) = snapshot_rx.take() {
            Self::client_loop_snapshot(
                id,
                &endpoint,
                &stream_tx,
                &mut client_rx,
                snapshot_rx,
                &mut is_alive,
                &mut filter,
            )
            .await;
        }

        if is_alive {
            'outer: loop {
                tokio::select! {
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
                                if let Some(msg) = filter_new.get_pong_msg() {
                                    if stream_tx.send(Ok(msg)).await.is_err() {
                                        error!("client #{id}: stream closed");
                                        break 'outer;
                                    }
                                    continue;
                                }

                                metrics::update_subscriptions(&endpoint, Some(&filter), Some(&filter_new));
                                filter = filter_new;
                                DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateFilter { id, filter: Box::new(filter.clone()) });
                                info!("client #{id}: filter updated");
                            }
                            Some(None) => {
                                break 'outer;
                            },
                            None => {
                                break 'outer;
                            }
                        }
                    }
                    message = messages_rx.recv() => {
                        let (commitment, messages) = match message {
                            Ok((commitment, messages)) => (commitment, messages),
                            Err(broadcast::error::RecvError::Closed) => {
                                break 'outer;
                            },
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                info!("client #{id}: lagged to receive geyser messages");
                                tokio::spawn(async move {
                                    let _ = stream_tx.send(Err(Status::internal("lagged"))).await;
                                });
                                break 'outer;
                            }
                        };

                        if commitment == filter.get_commitment_level() {
                            for message in messages.iter() {
                                for message in filter.get_updates(message, Some(commitment)) {
                                    match stream_tx.try_send(Ok(message)) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(_)) => {
                                            error!("client #{id}: lagged to send update");
                                            tokio::spawn(async move {
                                                let _ = stream_tx.send(Err(Status::internal("lagged"))).await;
                                            });
                                            break 'outer;
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            error!("client #{id}: stream closed");
                                            break 'outer;
                                        }
                                    }
                                }
                            }
                        }

                        if commitment == CommitmentLevel::Processed && debug_client_tx.is_some() {
                            for message in messages.iter() {
                                if let Message::Slot(slot_message) = &message {
                                    DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateSlot { id, slot: slot_message.slot });
                                }
                            }
                        }
                    }
                }
            }
        }

        metrics::connections_total_dec();
        DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::Removed { id });
        metrics::update_subscriptions(&endpoint, Some(&filter), None);
        info!("client #{id}: removed");
        drop_client();
    }

    async fn client_loop_snapshot(
        id: usize,
        endpoint: &str,
        stream_tx: &mpsc::Sender<TonicResult<FilteredUpdate>>,
        client_rx: &mut mpsc::UnboundedReceiver<Option<Filter>>,
        snapshot_rx: crossbeam_channel::Receiver<Box<Message>>,
        is_alive: &mut bool,
        filter: &mut Filter,
    ) {
        info!("client #{id}: going to receive snapshot data");

        // we start with default filter, for snapshot we need wait actual filter first
        while *is_alive {
            match client_rx.recv().await {
                Some(Some(filter_new)) => {
                    if let Some(msg) = filter_new.get_pong_msg() {
                        if stream_tx.send(Ok(msg)).await.is_err() {
                            error!("client #{id}: stream closed");
                            *is_alive = false;
                        }
                        continue;
                    }

                    metrics::update_subscriptions(endpoint, Some(filter), Some(&filter_new));
                    *filter = filter_new;
                    info!("client #{id}: filter updated");
                    break;
                }
                Some(None) => {
                    *is_alive = false;
                }
                None => {
                    *is_alive = false;
                }
            };
        }

        while *is_alive {
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
                    *is_alive = false;
                    break;
                }
            }
        }
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream<TonicResult<FilteredUpdate>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);

        let x_request_snapshot = request.metadata().contains_key("x-request-snapshot");
        let snapshot_rx = if x_request_snapshot {
            self.snapshot_rx.lock().await.take()
        } else {
            None
        };
        let (stream_tx, stream_rx) = mpsc::channel(if snapshot_rx.is_some() {
            self.config_snapshot_client_channel_capacity
        } else {
            self.config_channel_capacity
        });
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let notify_exit1 = Arc::new(Notify::new());
        let notify_exit2 = Arc::new(Notify::new());

        let ping_stream_tx = stream_tx.clone();
        let ping_client_tx = client_tx.clone();
        let ping_exit = Arc::clone(&notify_exit1);
        tokio::spawn(async move {
            let exit = ping_exit.notified();
            tokio::pin!(exit);

            loop {
                tokio::select! {
                    _ = &mut exit => {
                        break;
                    }
                    _ = sleep(Duration::from_secs(10)) => {
                        let msg = FilteredUpdate::new_empty(FilteredUpdateOneof::ping());
                        match ping_stream_tx.try_send(Ok(msg)) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {}
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                let _ = ping_client_tx.send(None);
                                break;
                            }
                        }
                    }
                }
            }
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
        let incoming_exit = Arc::clone(&notify_exit2);
        tokio::spawn(async move {
            let exit = incoming_exit.notified();
            tokio::pin!(exit);

            loop {
                tokio::select! {
                    _ = &mut exit => {
                        break;
                    }
                    message = request.get_mut().message() => match message {
                        Ok(Some(request)) => {
                            let mut filter_names = filter_names.lock().await;
                            filter_names.try_clean();

                            if let Err(error) = match Filter::new(&request, &config_filter_limits, &mut filter_names) {
                                Ok(filter) => match incoming_client_tx.send(Some(filter)) {
                                    Ok(()) => Ok(()),
                                    Err(error) => Err(error.to_string()),
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

        tokio::spawn(Self::client_loop(
            id,
            endpoint,
            stream_tx,
            client_rx,
            snapshot_rx,
            self.broadcast_tx.subscribe(),
            self.debug_clients_tx.clone(),
            move || {
                notify_exit1.notify_one();
                notify_exit2.notify_one();
            },
        ));

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        let count = request.get_ref().count;
        let response = PongResponse { count };
        Ok(Response::new(response))
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
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
        Ok(Response::new(GetVersionResponse {
            version: serde_json::to_string(&GrpcVersionInfo::default()).unwrap(),
        }))
    }
}
