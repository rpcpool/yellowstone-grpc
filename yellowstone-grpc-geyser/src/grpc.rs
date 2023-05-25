use {
    crate::{
        config::ConfigGrpc,
        filters::Filter,
        prom::CONNECTIONS_TOTAL,
        proto::{
            self,
            geyser_server::{Geyser, GeyserServer},
            subscribe_update::UpdateOneof,
            CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse,
            GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
            GetVersionRequest, GetVersionResponse, PingRequest, PongResponse, SubscribeRequest,
            SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
            SubscribeUpdateBlock, SubscribeUpdateBlockMeta, SubscribeUpdatePing,
            SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        },
        version::VERSION,
    },
    log::*,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV2, ReplicaBlockInfoV2, ReplicaTransactionInfoV2, SlotStatus,
    },
    solana_sdk::{
        clock::UnixTimestamp, pubkey::Pubkey, signature::Signature,
        transaction::SanitizedTransaction,
    },
    solana_transaction_status::{Reward, TransactionStatusMeta},
    std::{
        collections::{BTreeMap, HashMap},
        sync::atomic::{AtomicUsize, Ordering},
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::{mpsc, oneshot, RwLock},
        time::sleep,
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
    pub fn get_slot(&self) -> u64 {
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
enum ClientMessage {
    New {
        id: usize,
        filter: Filter,
        stream_tx: mpsc::Sender<TonicResult<SubscribeUpdate>>,
    },
    Update {
        id: usize,
        filter: Filter,
    },
    Drop {
        id: usize,
    },
}

#[derive(Debug)]
struct ClientConnection {
    filter: Filter,
    stream_tx: mpsc::Sender<TonicResult<SubscribeUpdate>>,
}

#[derive(Debug, Default)]
struct BlockMetaStorageInner {
    blocks: BTreeMap<u64, MessageBlockMeta>,
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

                        if msg.status == CommitmentLevel::Finalized {
                            clean_btree_slots(&mut storage.blocks, msg.slot - KEEP_SLOTS);
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

    async fn get_block<F, T>(
        &self,
        handler: F,
        commitment: Option<i32>,
    ) -> Result<Response<T>, Status>
    where
        F: FnOnce(&MessageBlockMeta) -> Option<T>,
    {
        let commitment = commitment.unwrap_or(CommitmentLevel::Finalized as i32);
        let commitment = CommitmentLevel::from_i32(commitment).ok_or_else(|| {
            let msg = format!("failed to create CommitmentLevel from {commitment:?}");
            Status::unknown(msg)
        })?;

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
}

fn clean_btree_slots<T>(storage: &mut BTreeMap<u64, T>, keep_slot: u64) {
    while let Some(slot) = storage.keys().next().cloned() {
        if slot < keep_slot {
            storage.remove(&slot);
        } else {
            break;
        }
    }
}

#[derive(Debug)]
pub struct GrpcService {
    config: ConfigGrpc,
    subscribe_id: AtomicUsize,
    new_clients_tx: mpsc::UnboundedSender<ClientMessage>,
    blocks_meta: BlockMetaStorage,
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
        let (blocks_meta, update_blocks_meta_tx) = BlockMetaStorage::new();

        // Create Server
        let (new_clients_tx, new_clients_rx) = mpsc::unbounded_channel();
        let service = GeyserServer::new(Self {
            config,
            subscribe_id: AtomicUsize::new(0),
            new_clients_tx,
            blocks_meta,
        })
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

        // Run filter and send loop
        let (update_channel_tx, update_channel_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            Self::send_loop(update_channel_rx, new_clients_rx, update_blocks_meta_tx).await
        });

        // gRPC Health check service
        let (mut health_reporter, health_service) = health_reporter();
        tokio::spawn(async move { health_reporter.set_serving::<GeyserServer<Self>>().await });

        // Run Server
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(5)))
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        Ok((update_channel_tx, shutdown_tx))
    }

    async fn send_loop(
        mut update_channel_rx: mpsc::UnboundedReceiver<Message>,
        mut new_clients_rx: mpsc::UnboundedReceiver<ClientMessage>,
        update_blocks_meta_tx: mpsc::UnboundedSender<Message>,
    ) {
        // Number of slots hold in memory after finalized
        const KEEP_SLOTS: u64 = 3;

        let mut clients: HashMap<usize, ClientConnection> = HashMap::new();
        let mut messages: BTreeMap<u64, Vec<Message>> = BTreeMap::new();
        loop {
            tokio::select! {
                Some(message) = update_channel_rx.recv() => {
                    if matches!(message, Message::Slot(_) | Message::BlockMeta(_)) {
                        let _ = update_blocks_meta_tx.send(message.clone());
                    }

                    let slot = if let Message::Slot(slot) = message {
                        slot
                    } else {
                        messages.entry(message.get_slot()).or_default().push(message);
                        continue;
                    };

                    let slot_messages = messages.get(&slot.slot).map(|x| x.as_slice()).unwrap_or_default();
                    for message in slot_messages.iter().chain(std::iter::once(&message)) {
                        let mut ids_full = vec![];
                        let mut ids_closed = vec![];

                        for (id, client) in clients.iter() {
                            if let Some(msg) = client.filter.get_update(message, slot.status) {
                                match client.stream_tx.try_send(Ok(msg)) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => ids_full.push(*id),
                                    Err(mpsc::error::TrySendError::Closed(_)) => ids_closed.push(*id),
                                }
                            }
                        }

                        for id in ids_full {
                            if let Some(client) = clients.remove(&id) {
                                tokio::spawn(async move {
                                    CONNECTIONS_TOTAL.dec();
                                    error!("{}, lagged, close stream", id);
                                    let _ = client.stream_tx.send(Err(Status::internal("lagged"))).await;
                                });
                            }
                        }
                        for id in ids_closed {
                            if let Some(_client) = clients.remove(&id) {
                                CONNECTIONS_TOTAL.dec();
                                error!("{}, client closed stream", id);
                            }
                        }
                    }

                    if slot.status == CommitmentLevel::Finalized {
                        clean_btree_slots(&mut messages, slot.slot - KEEP_SLOTS);
                    }
                },
                Some(msg) = new_clients_rx.recv() => {
                    match msg {
                        ClientMessage::New { id, filter, stream_tx } => {
                            info!("{}, add client to receivers", id);
                            clients.insert(id, ClientConnection { filter, stream_tx });
                            CONNECTIONS_TOTAL.inc();
                        }
                        ClientMessage::Update { id, filter } => {
                            if let Some(client) = clients.get_mut(&id) {
                                info!("{}, update client", id);
                                client.filter = filter;
                            }
                        }
                        ClientMessage::Drop { id } => {
                            if clients.remove(&id).is_some() {
                                CONNECTIONS_TOTAL.dec();
                            }
                        }
                    }
                }
                else => break,
            };
        }
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
        info!("{}, new subscriber", id);

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
        if let Err(_error) = self.new_clients_tx.send(ClientMessage::New {
            id,
            filter,
            stream_tx: stream_tx.clone(),
        }) {
            return Err(Status::internal("failed to add client"));
        }

        let ping_stream_tx = stream_tx.clone();
        let new_clients_tx = self.new_clients_tx.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(10)).await;
                match ping_stream_tx.try_send(Ok(SubscribeUpdate {
                    filters: vec![],
                    update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
                })) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            let _ = new_clients_tx.send(ClientMessage::Drop { id });
        });

        let config_filters_limit = self.config.filters.clone();
        let new_clients_tx = self.new_clients_tx.clone();
        tokio::spawn(async move {
            loop {
                match request.get_mut().message().await {
                    Ok(Some(request)) => {
                        if let Err(error) = match Filter::new(&request, &config_filters_limit) {
                            Ok(filter) => {
                                match new_clients_tx.send(ClientMessage::Update { id, filter }) {
                                    Ok(()) => Ok(()),
                                    Err(error) => Err(error.to_string()),
                                }
                            }
                            Err(error) => Err(error.to_string()),
                        } {
                            let _ = stream_tx
                                .send(Err(Status::invalid_argument(format!(
                                    "failed to create filter: {error}"
                                ))))
                                .await;
                        }
                    }
                    Ok(None) => break,
                    Err(_error) => break,
                }
            }
            let _ = new_clients_tx.send(ClientMessage::Drop { id });
        });

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        let count = request.get_ref().count;
        let response = PongResponse { count: count + 1 };
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

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: serde_json::to_string(&VERSION).unwrap(),
        }))
    }
}
