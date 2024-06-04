use {
    crate::{
        config::{ConfigBlockFailAction, ConfigGrpc, ConfigGrpcFilters},
        filters::{Filter, FilterAccountsDataSlice},
        prom::{self, DebugClientMessage, CONNECTIONS_TOTAL, MESSAGE_QUEUE_SIZE},
        version::GrpcVersionInfo,
    },
    anyhow::Context,
    log::{error, info},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV3, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus,
    },
    solana_sdk::{
        clock::{UnixTimestamp, MAX_RECENT_BLOCKHASHES},
        pubkey::Pubkey,
        signature::Signature,
        transaction::SanitizedTransaction,
    },
    solana_transaction_status::{Reward, TransactionStatusMeta},
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
        codec::CompressionEncoding,
        service::{interceptor::InterceptedService, Interceptor},
        transport::{
            server::{Server, TcpIncoming},
            Identity, ServerTlsConfig,
        },
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    tonic_health::server::health_reporter,
    yellowstone_grpc_proto::{
        convert_to,
        prelude::{
            geyser_server::{Geyser, GeyserServer},
            subscribe_update::UpdateOneof,
            CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse,
            GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
            GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
            IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeRequest, SubscribeUpdate,
            SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlock,
            SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdatePing,
            SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            SubscribeUpdateTransactionStatus, TransactionError as SubscribeUpdateTransactionError,
        },
    },
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

impl MessageAccountInfo {
    fn to_proto(
        &self,
        accounts_data_slice: &[FilterAccountsDataSlice],
    ) -> SubscribeUpdateAccountInfo {
        let data = if accounts_data_slice.is_empty() {
            self.data.clone()
        } else {
            let mut data = Vec::with_capacity(accounts_data_slice.iter().map(|ds| ds.length).sum());
            for data_slice in accounts_data_slice {
                if self.data.len() >= data_slice.end {
                    data.extend_from_slice(&self.data[data_slice.start..data_slice.end]);
                }
            }
            data
        };
        SubscribeUpdateAccountInfo {
            pubkey: self.pubkey.as_ref().into(),
            lamports: self.lamports,
            owner: self.owner.as_ref().into(),
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            data,
            write_version: self.write_version,
            txn_signature: self.txn_signature.map(|s| s.as_ref().into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageAccount {
    pub account: MessageAccountInfo,
    pub slot: u64,
    pub is_startup: bool,
}

impl<'a> From<(&'a ReplicaAccountInfoV3<'a>, u64, bool)> for MessageAccount {
    fn from((account, slot, is_startup): (&'a ReplicaAccountInfoV3<'a>, u64, bool)) -> Self {
        Self {
            account: MessageAccountInfo {
                pubkey: Pubkey::try_from(account.pubkey).expect("valid Pubkey"),
                lamports: account.lamports,
                owner: Pubkey::try_from(account.owner).expect("valid Pubkey"),
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: account.data.into(),
                write_version: account.write_version,
                txn_signature: account.txn.map(|txn| *txn.signature()),
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

impl MessageTransactionInfo {
    fn to_proto(&self) -> SubscribeUpdateTransactionInfo {
        SubscribeUpdateTransactionInfo {
            signature: self.signature.as_ref().into(),
            is_vote: self.is_vote,
            transaction: Some(convert_to::create_transaction(&self.transaction)),
            meta: Some(convert_to::create_transaction_meta(&self.meta)),
            index: self.index as u64,
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
pub struct MessageEntry {
    pub slot: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Vec<u8>,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: u64,
}

impl From<&ReplicaEntryInfoV2<'_>> for MessageEntry {
    fn from(entry: &ReplicaEntryInfoV2) -> Self {
        Self {
            slot: entry.slot,
            index: entry.index,
            num_hashes: entry.num_hashes,
            hash: entry.hash.into(),
            executed_transaction_count: entry.executed_transaction_count,
            starting_transaction_index: entry
                .starting_transaction_index
                .try_into()
                .expect("failed convert usize to u64"),
        }
    }
}

impl MessageEntry {
    fn to_proto(&self) -> SubscribeUpdateEntry {
        SubscribeUpdateEntry {
            slot: self.slot,
            index: self.index as u64,
            num_hashes: self.num_hashes,
            hash: self.hash.clone(),
            executed_transaction_count: self.executed_transaction_count,
            starting_transaction_index: self.starting_transaction_index,
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
    pub executed_transaction_count: u64,
    pub transactions: Vec<MessageTransactionInfo>,
    pub updated_account_count: u64,
    pub accounts: Vec<MessageAccountInfo>,
    pub entries_count: u64,
    pub entries: Vec<MessageEntry>,
}

impl
    From<(
        MessageBlockMeta,
        Vec<MessageTransactionInfo>,
        Vec<MessageAccountInfo>,
        Vec<MessageEntry>,
    )> for MessageBlock
{
    fn from(
        (blockinfo, transactions, accounts, entries): (
            MessageBlockMeta,
            Vec<MessageTransactionInfo>,
            Vec<MessageAccountInfo>,
            Vec<MessageEntry>,
        ),
    ) -> Self {
        Self {
            parent_slot: blockinfo.parent_slot,
            slot: blockinfo.slot,
            blockhash: blockinfo.blockhash,
            parent_blockhash: blockinfo.parent_blockhash,
            rewards: blockinfo.rewards,
            block_time: blockinfo.block_time,
            block_height: blockinfo.block_height,
            executed_transaction_count: blockinfo.executed_transaction_count,
            transactions,
            updated_account_count: accounts.len() as u64,
            accounts,
            entries_count: entries.len() as u64,
            entries,
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
    pub entries_count: u64,
}

impl<'a> From<&'a ReplicaBlockInfoV3<'a>> for MessageBlockMeta {
    fn from(blockinfo: &'a ReplicaBlockInfoV3<'a>) -> Self {
        Self {
            parent_slot: blockinfo.parent_slot,
            slot: blockinfo.slot,
            parent_blockhash: blockinfo.parent_blockhash.to_string(),
            blockhash: blockinfo.blockhash.to_string(),
            rewards: blockinfo.rewards.into(),
            block_time: blockinfo.block_time,
            block_height: blockinfo.block_height,
            executed_transaction_count: blockinfo.executed_transaction_count,
            entries_count: blockinfo.entry_count,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Message {
    Slot(MessageSlot),
    Account(MessageAccount),
    Transaction(MessageTransaction),
    Entry(MessageEntry),
    Block(MessageBlock),
    BlockMeta(MessageBlockMeta),
}

impl Message {
    pub const fn get_slot(&self) -> u64 {
        match self {
            Self::Slot(msg) => msg.slot,
            Self::Account(msg) => msg.slot,
            Self::Transaction(msg) => msg.slot,
            Self::Entry(msg) => msg.slot,
            Self::Block(msg) => msg.slot,
            Self::BlockMeta(msg) => msg.slot,
        }
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Slot(_) => "Slot",
            Self::Account(_) => "Account",
            Self::Transaction(_) => "Transaction",
            Self::Entry(_) => "Entry",
            Self::Block(_) => "Block",
            Self::BlockMeta(_) => "BlockMeta",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageBlockRef<'a> {
    pub parent_slot: u64,
    pub slot: u64,
    pub parent_blockhash: &'a String,
    pub blockhash: &'a String,
    pub rewards: &'a Vec<Reward>,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
    pub transactions: Vec<&'a MessageTransactionInfo>,
    pub updated_account_count: u64,
    pub accounts: Vec<&'a MessageAccountInfo>,
    pub entries_count: u64,
    pub entries: Vec<&'a MessageEntry>,
}

impl<'a>
    From<(
        &'a MessageBlock,
        Vec<&'a MessageTransactionInfo>,
        Vec<&'a MessageAccountInfo>,
        Vec<&'a MessageEntry>,
    )> for MessageBlockRef<'a>
{
    fn from(
        (block, transactions, accounts, entries): (
            &'a MessageBlock,
            Vec<&'a MessageTransactionInfo>,
            Vec<&'a MessageAccountInfo>,
            Vec<&'a MessageEntry>,
        ),
    ) -> Self {
        Self {
            parent_slot: block.parent_slot,
            slot: block.slot,
            parent_blockhash: &block.parent_blockhash,
            blockhash: &block.blockhash,
            rewards: &block.rewards,
            block_time: block.block_time,
            block_height: block.block_height,
            executed_transaction_count: block.executed_transaction_count,
            transactions,
            updated_account_count: block.updated_account_count,
            accounts,
            entries_count: block.entries_count,
            entries,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum MessageRef<'a> {
    Slot(&'a MessageSlot),
    Account(&'a MessageAccount),
    Transaction(&'a MessageTransaction),
    TransactionStatus(&'a MessageTransaction),
    Entry(&'a MessageEntry),
    Block(MessageBlockRef<'a>),
    BlockMeta(&'a MessageBlockMeta),
}

impl<'a> MessageRef<'a> {
    pub fn to_proto(&self, accounts_data_slice: &[FilterAccountsDataSlice]) -> UpdateOneof {
        match self {
            Self::Slot(message) => UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: message.slot,
                parent: message.parent,
                status: message.status as i32,
            }),
            Self::Account(message) => UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(message.account.to_proto(accounts_data_slice)),
                slot: message.slot,
                is_startup: message.is_startup,
            }),
            Self::Transaction(message) => UpdateOneof::Transaction(SubscribeUpdateTransaction {
                transaction: Some(message.transaction.to_proto()),
                slot: message.slot,
            }),
            Self::TransactionStatus(message) => {
                UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                    slot: message.slot,
                    signature: message.transaction.signature.as_ref().into(),
                    is_vote: message.transaction.is_vote,
                    index: message.transaction.index as u64,
                    err: match &message.transaction.meta.status {
                        Ok(()) => None,
                        Err(err) => Some(SubscribeUpdateTransactionError {
                            err: bincode::serialize(&err)
                                .expect("transaction error to serialize to bytes"),
                        }),
                    },
                })
            }
            Self::Entry(message) => UpdateOneof::Entry(message.to_proto()),
            Self::Block(message) => UpdateOneof::Block(SubscribeUpdateBlock {
                slot: message.slot,
                blockhash: message.blockhash.clone(),
                rewards: Some(convert_to::create_rewards_obj(message.rewards.as_slice())),
                block_time: message.block_time.map(convert_to::create_timestamp),
                block_height: message.block_height.map(convert_to::create_block_height),
                parent_slot: message.parent_slot,
                parent_blockhash: message.parent_blockhash.clone(),
                executed_transaction_count: message.executed_transaction_count,
                transactions: message
                    .transactions
                    .iter()
                    .map(|tx| tx.to_proto())
                    .collect(),
                updated_account_count: message.updated_account_count,
                accounts: message
                    .accounts
                    .iter()
                    .map(|acc| acc.to_proto(accounts_data_slice))
                    .collect(),
                entries_count: message.entries_count,
                entries: message
                    .entries
                    .iter()
                    .map(|entry| entry.to_proto())
                    .collect(),
            }),
            Self::BlockMeta(message) => UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: message.slot,
                blockhash: message.blockhash.clone(),
                rewards: Some(convert_to::create_rewards_obj(message.rewards.as_slice())),
                block_time: message.block_time.map(convert_to::create_timestamp),
                block_height: message.block_height.map(convert_to::create_block_height),
                parent_slot: message.parent_slot,
                parent_blockhash: message.parent_blockhash.clone(),
                executed_transaction_count: message.executed_transaction_count,
                entries_count: message.entries_count,
            }),
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
        let commitment = commitment.unwrap_or(CommitmentLevel::Processed as i32);
        CommitmentLevel::try_from(commitment).map_err(|_error| {
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
    messages: Vec<Option<Arc<Message>>>, // Option is used for accounts with low write_version
    block_meta: Option<MessageBlockMeta>,
    transactions: Vec<MessageTransactionInfo>,
    accounts_dedup: HashMap<Pubkey, (u64, usize)>, // (write_version, message_index)
    entries: Vec<MessageEntry>,
    sealed: bool,
    entries_count: usize,
    confirmed_at: Option<usize>,
    finalized_at: Option<usize>,
}

impl SlotMessages {
    pub fn try_seal(&mut self) -> Option<Arc<Message>> {
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
                        if let Message::Account(account) = item.as_ref() {
                            accounts.push(account.account.clone());
                        }
                    }

                    let message = Arc::new(Message::Block(
                        (block_meta.clone(), transactions, accounts, entries).into(),
                    ));
                    self.messages.push(Some(Arc::clone(&message)));

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
    config: ConfigGrpc,
    config_filters: Arc<ConfigGrpcFilters>,
    blocks_meta: Option<BlockMetaStorage>,
    subscribe_id: AtomicUsize,
    snapshot_rx: Mutex<Option<crossbeam_channel::Receiver<Option<Message>>>>,
    broadcast_tx: broadcast::Sender<(CommitmentLevel, Arc<Vec<Arc<Message>>>)>,
    debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
}

impl GrpcService {
    #[allow(clippy::type_complexity)]
    pub async fn create(
        config: ConfigGrpc,
        block_fail_action: ConfigBlockFailAction,
        debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        is_reload: bool,
    ) -> anyhow::Result<(
        Option<crossbeam_channel::Sender<Option<Message>>>,
        mpsc::UnboundedSender<Arc<Message>>,
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

        // Create Server
        let max_decoding_message_size = config.max_decoding_message_size;
        let x_token = XTokenChecker::new(config.x_token.clone());
        let config_filters = Arc::new(config.filters.clone());
        let service = GeyserServer::new(Self {
            config,
            config_filters,
            blocks_meta,
            subscribe_id: AtomicUsize::new(0),
            snapshot_rx: Mutex::new(snapshot_rx),
            broadcast_tx: broadcast_tx.clone(),
            debug_clients_tx,
        })
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(max_decoding_message_size);
        let service = InterceptedService::new(service, x_token);

        // Run geyser message loop
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        spawn_blocking(move || {
            Builder::new_multi_thread()
                .thread_name_fn(crate::get_thread_name)
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("Failed to create a new runtime for geyser loop")
                .block_on(Self::geyser_loop(
                    messages_rx,
                    blocks_meta_tx,
                    broadcast_tx,
                    block_fail_action,
                ));
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
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown_grpc.notified())
                .await
        });

        Ok((snapshot_tx, messages_tx, shutdown))
    }

    async fn geyser_loop(
        mut messages_rx: mpsc::UnboundedReceiver<Arc<Message>>,
        blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
        broadcast_tx: broadcast::Sender<(CommitmentLevel, Arc<Vec<Arc<Message>>>)>,
        block_fail_action: ConfigBlockFailAction,
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
                    MESSAGE_QUEUE_SIZE.dec();

                    // Update metrics
                    if let Message::Slot(slot_message) = message.as_ref() {
                        prom::update_slot_plugin_status(slot_message.status, slot_message.slot);
                    }

                    // Update blocks info
                    if let Some(blocks_meta_tx) = &blocks_meta_tx {
                        if matches!(message.as_ref(), Message::Slot(_) | Message::BlockMeta(_)) {
                            let _ = blocks_meta_tx.send(message.as_ref().clone());
                        }
                    }

                    // Remove outdated block reconstruction info
                    match message.as_ref() {
                        // On startup we can receive few Confirmed/Finalized slots without BlockMeta message
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

                                                    prom::update_invalid_blocks(format!("failed reconstruct {reason}"));
                                                    match block_fail_action {
                                                        ConfigBlockFailAction::Log => {
                                                            error!("failed reconstruct #{slot} {reason}");
                                                        }
                                                        ConfigBlockFailAction::Panic => {
                                                            panic!("failed reconstruct #{slot} {reason}");
                                                        }
                                                    }
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
                    if !matches!(message.as_ref(), Message::Slot(_)) {
                        slot_messages.messages.push(Some(Arc::clone(&message)));

                        // If we already build Block message, new message will be a problem
                        if slot_messages.sealed && !(matches!(message.as_ref(), Message::Entry(_)) && slot_messages.entries_count == 0) {
                            prom::update_invalid_blocks(format!("unexpected message {}", message.kind()));
                            match block_fail_action {
                                ConfigBlockFailAction::Log => {
                                    error!("unexpected message #{} -- {} (invalid order)", message.get_slot(), message.kind());
                                }
                                ConfigBlockFailAction::Panic => {
                                    panic!("unexpected message #{} -- {} (invalid order)", message.get_slot(), message.kind());
                                }
                            }
                        }
                    }
                    let mut sealed_block_msg = None;
                    match message.as_ref() {
                        Message::BlockMeta(msg) => {
                            if slot_messages.block_meta.is_some() {
                                prom::update_invalid_blocks("unexpected message: BlockMeta (duplicate)");
                                match block_fail_action {
                                    ConfigBlockFailAction::Log => {
                                        error!("unexpected message #{} -- BlockMeta (duplicate)", message.get_slot());
                                    }
                                    ConfigBlockFailAction::Panic => {
                                        panic!("unexpected message #{} -- BlockMeta (duplicate)", message.get_slot());
                                    }
                                }
                            }
                            slot_messages.block_meta = Some(msg.clone());
                            sealed_block_msg = slot_messages.try_seal();
                        }
                        Message::Transaction(msg) => {
                            slot_messages.transactions.push(msg.transaction.clone());
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
                            slot_messages.entries.push(msg.clone());
                            sealed_block_msg = slot_messages.try_seal();
                        }
                        _ => {}
                    }

                    // Send messages to filter (and to clients)
                    let mut messages_vec = vec![message];
                    if let Some(sealed_block_msg) = sealed_block_msg {
                        messages_vec.push(sealed_block_msg);
                    }

                    for message in messages_vec {
                        if let Message::Slot(slot) = message.as_ref() {
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
                            processed_messages.push(Arc::clone(&message));
                            let _ =
                                broadcast_tx.send((CommitmentLevel::Processed, processed_messages.into()));
                            processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
                            processed_sleep
                                .as_mut()
                                .reset(Instant::now() + PROCESSED_MESSAGES_SLEEP);

                            // confirmed
                            confirmed_messages.push(Arc::clone(&message));
                            let _ =
                                broadcast_tx.send((CommitmentLevel::Confirmed, confirmed_messages.into()));

                            // finalized
                            finalized_messages.push(message);
                            let _ =
                                broadcast_tx.send((CommitmentLevel::Finalized, finalized_messages.into()));
                        } else {
                            let mut confirmed_messages = vec![];
                            let mut finalized_messages = vec![];
                            if matches!(message.as_ref(), Message::Block(_)) {
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
        config_filters: Arc<ConfigGrpcFilters>,
        stream_tx: mpsc::Sender<TonicResult<SubscribeUpdate>>,
        mut client_rx: mpsc::UnboundedReceiver<Option<Filter>>,
        mut snapshot_rx: Option<crossbeam_channel::Receiver<Option<Message>>>,
        mut messages_rx: broadcast::Receiver<(CommitmentLevel, Arc<Vec<Arc<Message>>>)>,
        debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        drop_client: impl FnOnce(),
    ) {
        let mut filter = Filter::new(
            &SubscribeRequest {
                accounts: HashMap::new(),
                slots: HashMap::new(),
                transactions: HashMap::new(),
                transactions_status: HashMap::new(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                entry: HashMap::new(),
                commitment: None,
                accounts_data_slice: Vec::new(),
                ping: None,
            },
            &config_filters,
        )
        .expect("empty filter");
        prom::update_subscriptions(None, Some(&filter));

        CONNECTIONS_TOTAL.inc();
        DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateFilter {
            id,
            filter: Box::new(filter.clone()),
        });
        info!("client #{id}: new");

        let mut is_alive = true;
        if let Some(snapshot_rx) = snapshot_rx.take() {
            info!("client #{id}: going to receive snapshot data");

            // we start with default filter, for snapshot we need wait actual filter first
            while is_alive {
                match client_rx.recv().await {
                    Some(Some(filter_new)) => {
                        if let Some(msg) = filter_new.get_pong_msg() {
                            if stream_tx.send(Ok(msg)).await.is_err() {
                                error!("client #{id}: stream closed");
                                is_alive = false;
                                break;
                            }
                            continue;
                        }

                        prom::update_subscriptions(Some(&filter), Some(&filter_new));
                        filter = filter_new;
                        info!("client #{id}: filter updated");
                    }
                    Some(None) => {
                        is_alive = false;
                    }
                    None => {
                        is_alive = false;
                    }
                };
            }

            while is_alive {
                let message = match snapshot_rx.try_recv() {
                    Ok(message) => {
                        MESSAGE_QUEUE_SIZE.dec();
                        match message {
                            Some(message) => message,
                            None => break,
                        }
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        sleep(Duration::from_millis(1)).await;
                        continue;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        error!("client #{id}: snapshot channel disconnected");
                        is_alive = false;
                        break;
                    }
                };

                for message in filter.get_update(&message, None) {
                    if stream_tx.send(Ok(message)).await.is_err() {
                        error!("client #{id}: stream closed");
                        is_alive = false;
                        break;
                    }
                }
            }
        }

        if is_alive {
            'outer: loop {
                tokio::select! {
                    message = client_rx.recv() => {
                        match message {
                            Some(Some(filter_new)) => {
                                if let Some(msg) = filter_new.get_pong_msg() {
                                    if stream_tx.send(Ok(msg)).await.is_err() {
                                        error!("client #{id}: stream closed");
                                        break 'outer;
                                    }
                                    continue;
                                }

                                prom::update_subscriptions(Some(&filter), Some(&filter_new));
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
                                for message in filter.get_update(message, Some(commitment)) {
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
                                if let Message::Slot(slot_message) = message.as_ref() {
                                    DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateSlot { id, slot: slot_message.slot });
                                }
                            }
                        }
                    }
                }
            }
        }

        CONNECTIONS_TOTAL.dec();
        DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::Removed { id });
        prom::update_subscriptions(Some(&filter), None);
        info!("client #{id}: removed");
        drop_client();
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream<TonicResult<SubscribeUpdate>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);
        let snapshot_rx = self.snapshot_rx.lock().await.take();
        let (stream_tx, stream_rx) = mpsc::channel(if snapshot_rx.is_some() {
            self.config.snapshot_client_channel_capacity
        } else {
            self.config.channel_capacity
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

            let ping_msg = SubscribeUpdate {
                filters: vec![],
                update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            };

            loop {
                tokio::select! {
                    _ = &mut exit => {
                        break;
                    }
                    _ = sleep(Duration::from_secs(10)) => {
                        match ping_stream_tx.try_send(Ok(ping_msg.clone())) {
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

        let config_filters = Arc::clone(&self.config_filters);
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
                            if let Err(error) = match Filter::new(&request, &config_filters) {
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
            Arc::clone(&self.config_filters),
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
                        block
                            .block_height
                            .map(|block_height| GetLatestBlockhashResponse {
                                slot: block.slot,
                                blockhash: block.blockhash.clone(),
                                last_valid_block_height: block_height
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
                        block
                            .block_height
                            .map(|block_height| GetBlockHeightResponse { block_height })
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

#[derive(Clone)]
struct XTokenChecker {
    x_token: Option<String>,
}

impl XTokenChecker {
    const fn new(x_token: Option<String>) -> Self {
        Self { x_token }
    }
}

impl Interceptor for XTokenChecker {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(x_token) = &self.x_token {
            match req.metadata().get("x-token") {
                Some(token) if x_token == token => Ok(req),
                _ => Err(Status::unauthenticated("No valid auth token")),
            }
        } else {
            Ok(req)
        }
    }
}
