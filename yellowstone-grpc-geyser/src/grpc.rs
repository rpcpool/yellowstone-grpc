use {
    crate::{
        config::ConfigGrpc,
        filters::Filter,
        prom::{CONNECTIONS_TOTAL, MESSAGE_QUEUE_SIZE},
        proto::{
            self,
            geyser_server::{Geyser, GeyserServer},
            subscribe_update::UpdateOneof,
            CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse,
            GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
            GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
            IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeRequest, SubscribeUpdate,
            SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlock,
            SubscribeUpdateBlockMeta, SubscribeUpdatePing, SubscribeUpdateSlot,
            SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        },
        version::VERSION,
    },
    log::*,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV2, ReplicaBlockInfoV2, ReplicaTransactionInfoV2, SlotStatus,
    },
    solana_sdk::{
        clock::{UnixTimestamp, MAX_RECENT_BLOCKHASHES},
        pubkey::Pubkey,
        signature::Signature,
        transaction::SanitizedTransaction,
    },
    solana_transaction_status::{Reward, TransactionStatusMeta},
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    },
    tokio::{
        sync::{broadcast, mpsc, oneshot, RwLock},
        time::{sleep, Duration, Instant},
    },
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        codec::CompressionEncoding,
        transport::server::{Server, TcpIncoming},
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    tonic_health::server::health_reporter,
};

#[derive(Debug, Clone)]
pub struct MessageAccountInfo {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub write_version: u64,
    pub txn_signature: Option<Signature>,
}

#[derive(Debug, Clone)]
pub struct MessageAccount {
    pub account: MessageAccountInfo,
    pub slot: u64,
    pub is_startup: bool,
}

impl<'a> From<(&'a ReplicaAccountInfoV2<'a>, u64, bool)> for MessageAccount {
    fn from((account, slot, is_startup): (&'a ReplicaAccountInfoV2<'a>, u64, bool)) -> Self {
        Self {
            account: MessageAccountInfo {
                pubkey: Pubkey::try_from(account.pubkey).expect("valid Pubkey"),
                lamports: account.lamports,
                owner: Pubkey::try_from(account.owner).expect("valid Pubkey"),
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: account.data.into(),
                write_version: account.write_version,
                txn_signature: account.txn_signature.cloned(),
            },
            slot,
            is_startup,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MessageSlot {
    pub slot: u64,
    pub parent: Option<u64>,
    pub status: CommitmentLevel,
}

impl From<(u64, Option<u64>, SlotStatus)> for MessageSlot {
    fn from((slot, parent, status): (u64, Option<u64>, SlotStatus)) -> Self {
        Self {
            slot,
            parent,
            status: match status {
                SlotStatus::Processed => CommitmentLevel::Processed,
                SlotStatus::Confirmed => CommitmentLevel::Confirmed,
                SlotStatus::Rooted => CommitmentLevel::Finalized,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageTransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: SanitizedTransaction,
    pub meta: TransactionStatusMeta,
    pub index: usize,
}

impl From<&MessageTransactionInfo> for SubscribeUpdateTransactionInfo {
    fn from(tx: &MessageTransactionInfo) -> Self {
        Self {
            signature: tx.signature.as_ref().into(),
            is_vote: tx.is_vote,
            transaction: Some(proto::convert::create_transaction(&tx.transaction)),
            meta: Some(proto::convert::create_transaction_meta(&tx.meta)),
            index: tx.index as u64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageTransaction {
    pub transaction: MessageTransactionInfo,
    pub slot: u64,
}

impl<'a> From<(&'a ReplicaTransactionInfoV2<'a>, u64)> for MessageTransaction {
    fn from((transaction, slot): (&'a ReplicaTransactionInfoV2<'a>, u64)) -> Self {
        Self {
            transaction: MessageTransactionInfo {
                signature: *transaction.signature,
                is_vote: transaction.is_vote,
                transaction: transaction.transaction.clone(),
                meta: transaction.transaction_status_meta.clone(),
                index: transaction.index,
            },
            slot,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageBlock {
    pub parent_slot: u64,
    pub slot: u64,
    pub parent_blockhash: String,
    pub blockhash: String,
    pub rewards: Vec<Reward>,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
    pub transactions: Vec<MessageTransactionInfo>,
}

impl From<(MessageBlockMeta, Vec<MessageTransactionInfo>)> for MessageBlock {
    fn from((blockinfo, transactions): (MessageBlockMeta, Vec<MessageTransactionInfo>)) -> Self {
        Self {
            parent_slot: blockinfo.parent_slot,
            slot: blockinfo.slot,
            blockhash: blockinfo.blockhash,
            parent_blockhash: blockinfo.parent_blockhash,
            rewards: blockinfo.rewards,
            block_time: blockinfo.block_time,
            block_height: blockinfo.block_height,
            transactions,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageBlockMeta {
    pub parent_slot: u64,
    pub slot: u64,
    pub parent_blockhash: String,
    pub blockhash: String,
    pub rewards: Vec<Reward>,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
}

impl<'a> From<&'a ReplicaBlockInfoV2<'a>> for MessageBlockMeta {
    fn from(blockinfo: &'a ReplicaBlockInfoV2<'a>) -> Self {
        Self {
            parent_slot: blockinfo.parent_slot,
            slot: blockinfo.slot,
            parent_blockhash: blockinfo.parent_blockhash.to_string(),
            blockhash: blockinfo.blockhash.to_string(),
            rewards: blockinfo.rewards.into(),
            block_time: blockinfo.block_time,
            block_height: blockinfo.block_height,
            executed_transaction_count: blockinfo.executed_transaction_count,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Message {
    Slot(MessageSlot),
    Account(MessageAccount),
    Transaction(MessageTransaction),
    Block(MessageBlock),
    BlockMeta(MessageBlockMeta),
}

impl From<&Message> for UpdateOneof {
    fn from(message: &Message) -> Self {
        match message {
            Message::Slot(message) => UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: message.slot,
                parent: message.parent,
                status: message.status as i32,
            }),
            Message::Account(message) => UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: message.account.pubkey.as_ref().into(),
                    lamports: message.account.lamports,
                    owner: message.account.owner.as_ref().into(),
                    executable: message.account.executable,
                    rent_epoch: message.account.rent_epoch,
                    data: message.account.data.clone(),
                    write_version: message.account.write_version,
                    txn_signature: message.account.txn_signature.map(|s| s.as_ref().into()),
                }),
                slot: message.slot,
                is_startup: message.is_startup,
            }),
            Message::Transaction(message) => UpdateOneof::Transaction(SubscribeUpdateTransaction {
                transaction: Some((&message.transaction).into()),
                slot: message.slot,
            }),
            Message::Block(message) => UpdateOneof::Block(SubscribeUpdateBlock {
                slot: message.slot,
                blockhash: message.blockhash.clone(),
                rewards: Some(proto::convert::create_rewards(message.rewards.as_slice())),
                block_time: message.block_time.map(proto::convert::create_timestamp),
                block_height: message
                    .block_height
                    .map(proto::convert::create_block_height),
                transactions: message.transactions.iter().map(Into::into).collect(),
                parent_slot: message.parent_slot,
                parent_blockhash: message.parent_blockhash.clone(),
            }),
            Message::BlockMeta(message) => UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: message.slot,
                blockhash: message.blockhash.clone(),
                rewards: Some(proto::convert::create_rewards(message.rewards.as_slice())),
                block_time: message.block_time.map(proto::convert::create_timestamp),
                block_height: message
                    .block_height
                    .map(proto::convert::create_block_height),
                parent_slot: message.parent_slot,
                parent_blockhash: message.parent_blockhash.clone(),
                executed_transaction_count: message.executed_transaction_count,
            }),
        }
    }
}

impl Message {
    pub const fn get_slot(&self) -> u64 {
        match self {
            Self::Slot(msg) => msg.slot,
            Self::Account(msg) => msg.slot,
            Self::Transaction(msg) => msg.slot,
            Self::Block(msg) => msg.slot,
            Self::BlockMeta(msg) => msg.slot,
        }
    }
}

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
    blocks: HashMap<u64, MessageBlockMeta>,
    blockhashes: HashMap<String, BlockhashStatus>,
    processed: Option<u64>,
    confirmed: Option<u64>,
    finalized: Option<u64>,
}

#[derive(Debug)]
struct BlockMetaStorage {
    inner: Arc<RwLock<BlockMetaStorageInner>>,
}

impl BlockMetaStorage {
    fn new() -> (Self, mpsc::UnboundedSender<Message>) {
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
                            let keep_slot = msg.slot - KEEP_SLOTS;
                            storage.blocks.retain(|slot, _block| *slot >= keep_slot);

                            let keep_slot = msg.slot - MAX_RECENT_BLOCKHASHES as u64 - 32;
                            storage
                                .blockhashes
                                .retain(|_blockhash, status| status.slot >= keep_slot);
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

        (Self { inner }, tx)
    }

    fn parse_commitment(commitment: Option<i32>) -> Result<CommitmentLevel, Status> {
        let commitment = commitment.unwrap_or(CommitmentLevel::Processed as i32);
        CommitmentLevel::from_i32(commitment).ok_or_else(|| {
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

#[derive(Debug)]
pub struct GrpcService {
    config: ConfigGrpc,
    blocks_meta: BlockMetaStorage,
    subscribe_id: AtomicUsize,
    broadcast_tx: broadcast::Sender<(CommitmentLevel, Arc<Vec<Message>>)>,
}

impl GrpcService {
    pub fn create(
        config: ConfigGrpc,
    ) -> Result<
        (mpsc::UnboundedSender<Message>, oneshot::Sender<()>),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        // Bind service address
        let incoming = TcpIncoming::new(
            config.address,
            true,                          // tcp_nodelay
            Some(Duration::from_secs(20)), // tcp_keepalive
        )?;

        // Blocks meta storage
        let (blocks_meta, blocks_meta_tx) = BlockMetaStorage::new();

        // Messages to clients combined by commitment
        let (broadcast_tx, _) = broadcast::channel(config.channel_capacity);

        // Create Server
        let service = GeyserServer::new(Self {
            config,
            blocks_meta,
            subscribe_id: AtomicUsize::new(0),
            broadcast_tx: broadcast_tx.clone(),
        })
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

        // Run geyser message loop
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        tokio::spawn(Self::geyser_loop(messages_rx, blocks_meta_tx, broadcast_tx));

        // Run Server
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            // gRPC Health check service
            let (mut health_reporter, health_service) = health_reporter();
            health_reporter.set_serving::<GeyserServer<Self>>().await;

            Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(5)))
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        Ok((messages_tx, shutdown_tx))
    }

    async fn geyser_loop(
        mut messages_rx: mpsc::UnboundedReceiver<Message>,
        blocks_meta_tx: mpsc::UnboundedSender<Message>,
        broadcast_tx: broadcast::Sender<(CommitmentLevel, Arc<Vec<Message>>)>,
    ) {
        const PROCESSED_MESSAGES_MAX: usize = 31;
        const PROCESSED_MESSAGES_SLEEP: Duration = Duration::from_millis(10);

        let mut messages: HashMap<u64, Vec<Message>> = HashMap::new();
        let mut processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
        let processed_sleep = sleep(PROCESSED_MESSAGES_SLEEP);
        tokio::pin!(processed_sleep);
        loop {
            tokio::select! {
                Some(message) = messages_rx.recv() => {
                    MESSAGE_QUEUE_SIZE.dec();

                    if matches!(message, Message::Slot(_) | Message::BlockMeta(_)) {
                        let _ = blocks_meta_tx.send(message.clone());
                    }

                    if let Message::Slot(slot) = message {
                        let (mut confirmed_messages, mut finalized_messages) = match slot.status {
                            CommitmentLevel::Processed => (Vec::with_capacity(1), Vec::with_capacity(1)),
                            CommitmentLevel::Confirmed => {
                                let messages = messages.get(&slot.slot).cloned().unwrap_or_default();
                                (messages, Vec::with_capacity(1))
                            }
                            CommitmentLevel::Finalized => {
                                messages.retain(|msg_slot, _messages| *msg_slot >= slot.slot);
                                let messages = messages.remove(&slot.slot).unwrap_or_default();
                                (Vec::with_capacity(1), messages)
                            }
                        };

                        // processed
                        processed_messages.push(message.clone());
                        let _ = broadcast_tx.send((CommitmentLevel::Processed, processed_messages.into()));
                        processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
                        processed_sleep.as_mut().reset(Instant::now() + PROCESSED_MESSAGES_SLEEP);

                        // confirmed
                        confirmed_messages.push(message.clone());
                        let _ = broadcast_tx.send((CommitmentLevel::Confirmed, confirmed_messages.into()));

                        // finalized
                        finalized_messages.push(message);
                        let _ = broadcast_tx.send((CommitmentLevel::Finalized, finalized_messages.into()));
                    } else {
                        processed_messages.push(message.clone());
                        if processed_messages.len() >= PROCESSED_MESSAGES_MAX {
                            let _ = broadcast_tx.send((CommitmentLevel::Processed, processed_messages.into()));
                            processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
                            processed_sleep.as_mut().reset(Instant::now() + PROCESSED_MESSAGES_SLEEP);
                        }
                        messages.entry(message.get_slot()).or_default().push(message);
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

    async fn client_loop(
        id: usize,
        mut filter: Filter,
        stream_tx: mpsc::Sender<TonicResult<SubscribeUpdate>>,
        mut client_rx: mpsc::UnboundedReceiver<Option<Filter>>,
        mut messages_rx: broadcast::Receiver<(CommitmentLevel, Arc<Vec<Message>>)>,
        exit: Arc<AtomicBool>,
    ) {
        CONNECTIONS_TOTAL.inc();
        info!("client #{id}: new");
        'outer: loop {
            tokio::select! {
                message = client_rx.recv() => {
                    match message {
                        Some(Some(filter_new)) => {
                            filter = filter_new;
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
                    match message {
                        Ok((commitment, messages)) => {
                            if commitment == filter.get_commitment_level() {
                                for message in messages.iter() {
                                    if let Some(message) = filter.get_update(message) {
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
                        }
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
                    }
                }
            }
        }
        info!("client #{id}: removed");
        CONNECTIONS_TOTAL.dec();
        exit.store(true, Ordering::Relaxed);
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream<TonicResult<SubscribeUpdate>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let id = self.subscribe_id.fetch_add(1, Ordering::SeqCst);
        let filter = Filter::new(
            &SubscribeRequest {
                accounts: HashMap::new(),
                slots: HashMap::new(),
                transactions: HashMap::new(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                commitment: None,
            },
            &self.config.filters,
        )
        .expect("empty filter");
        let (stream_tx, stream_rx) = mpsc::channel(self.config.channel_capacity);
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let exit = Arc::new(AtomicBool::new(false));

        tokio::spawn(Self::client_loop(
            id,
            filter,
            stream_tx.clone(),
            client_rx,
            self.broadcast_tx.subscribe(),
            Arc::clone(&exit),
        ));

        let ping_stream_tx = stream_tx.clone();
        let ping_client_tx = client_tx.clone();
        let ping_exit = Arc::clone(&exit);
        tokio::spawn(async move {
            while !ping_exit.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(10)).await;
                match ping_stream_tx.try_send(Ok(SubscribeUpdate {
                    filters: vec![],
                    update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
                })) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        let _ = ping_client_tx.send(None);
                        break;
                    }
                }
            }
        });

        let config_filters_limit = self.config.filters.clone();
        tokio::spawn(async move {
            while !exit.load(Ordering::Relaxed) {
                match request.get_mut().message().await {
                    Ok(Some(request)) => {
                        if let Err(error) = match Filter::new(&request, &config_filters_limit) {
                            Ok(filter) => match client_tx.send(Some(filter)) {
                                Ok(()) => Ok(()),
                                Err(error) => Err(error.to_string()),
                            },
                            Err(error) => Err(error.to_string()),
                        } {
                            let _ = stream_tx
                                .send(Err(Status::invalid_argument(format!(
                                    "failed to create filter: {error}"
                                ))))
                                .await;
                        }
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(_error) => {
                        let _ = client_tx.send(None);
                        break;
                    }
                }
            }
        });

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
        self.blocks_meta
            .get_block(
                |block| {
                    block
                        .block_height
                        .map(|last_valid_block_height| GetLatestBlockhashResponse {
                            slot: block.slot,
                            blockhash: block.blockhash.clone(),
                            last_valid_block_height,
                        })
                },
                request.get_ref().commitment,
            )
            .await
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        self.blocks_meta
            .get_block(
                |block| {
                    block
                        .block_height
                        .map(|block_height| GetBlockHeightResponse { block_height })
                },
                request.get_ref().commitment,
            )
            .await
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        self.blocks_meta
            .get_block(
                |block| Some(GetSlotResponse { slot: block.slot }),
                request.get_ref().commitment,
            )
            .await
    }

    async fn is_blockhash_valid(
        &self,
        request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        let req = request.get_ref();
        self.blocks_meta
            .is_blockhash_valid(&req.blockhash, req.commitment)
            .await
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: serde_json::to_string(&VERSION).unwrap(),
        }))
    }
}
