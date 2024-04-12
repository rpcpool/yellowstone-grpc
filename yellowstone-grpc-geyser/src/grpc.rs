use {
    crate::{
        config::{ConfigBlockFailAction, ConfigGrpc},
        filters::{Filter, FilterAccountsDataSlice, FilterIdRaw, Filters},
        geyser::{GeyserMessage, GeyserMessageBlockMeta},
        prom::{self, CONNECTIONS_TOTAL, MESSAGE_QUEUE_SIZE},
        version::VERSION,
    },
    futures::{
        future::{BoxFuture, FutureExt},
        stream::Stream,
    },
    log::{error, info},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV3, ReplicaEntryInfo, ReplicaTransactionInfoV2,
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
        collections::{BTreeMap, HashMap, VecDeque},
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Weak,
        },
        task::{Context, Poll},
    },
    tokio::{
        fs,
        sync::{broadcast, mpsc, Mutex, Notify, RwLock, Semaphore},
        time::{sleep, Duration, Instant},
    },
    tonic::{
        codec::CompressionEncoding,
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
struct BlocksMetaStorageInner {
    blocks: HashMap<u64, GeyserMessageBlockMeta>,
    blockhashes: HashMap<String, BlockhashStatus>,
    processed: Option<u64>,
    confirmed: Option<u64>,
    finalized: Option<u64>,
}

#[derive(Debug)]
pub struct BlocksMetaStorage {
    read_sem: Semaphore,
    inner: Arc<RwLock<BlocksMetaStorageInner>>,
}

impl BlocksMetaStorage {
    pub fn new(unary_concurrency_limit: usize) -> (Self, mpsc::UnboundedSender<GeyserMessage>) {
        let inner = Arc::new(RwLock::new(BlocksMetaStorageInner::default()));
        let (tx, mut rx) = mpsc::unbounded_channel();

        let storage = Arc::clone(&inner);
        tokio::spawn(async move {
            const KEEP_SLOTS: u64 = 3;

            while let Some(message) = rx.recv().await {
                let mut storage = storage.write().await;
                match message {
                    GeyserMessage::Slot(msg) => {
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
                    GeyserMessage::BlockMeta(msg) => {
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
        F: FnOnce(&GeyserMessageBlockMeta) -> Option<T>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcClientId(pub u64); // TODO: remove pub

#[derive(Debug, Clone, Copy)]
pub enum GrpcMessageId {
    Client(GrpcClientId),
    Filter(FilterIdRaw),
}

impl PartialEq<GrpcClientId> for GrpcMessageId {
    fn eq(&self, other: &GrpcClientId) -> bool {
        if let Self::Client(value) = self {
            value == other
        } else {
            false
        }
    }
}

impl PartialEq<FilterIdRaw> for GrpcMessageId {
    fn eq(&self, other: &FilterIdRaw) -> bool {
        if let Self::Filter(value) = self {
            value == other
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
pub enum GrpcMessage {
    Tonic(TonicResult<Weak<SubscribeUpdate>>),
    ClientNewFilter(FilterIdRaw),
}

#[derive(Debug, Clone)]
pub struct GrpcMessageWithId {
    id: GrpcMessageId,
    message: GrpcMessage,
}

impl GrpcMessageWithId {
    pub const fn new(id: GrpcMessageId, message: GrpcMessage) -> Self {
        Self { id, message }
    }
}

#[derive(Debug)]
pub struct GrpcMessages {
    items: VecDeque<(Arc<SubscribeUpdate>, usize)>,
    total_bytes: usize,
    max_len: usize,
    max_total_bytes: usize,
}

impl GrpcMessages {
    pub const fn new(max_len: usize, max_total_bytes: usize) -> Self {
        Self {
            items: VecDeque::new(),
            total_bytes: 0,
            max_len,
            max_total_bytes,
        }
    }

    pub fn push(&mut self, (item, item_len): (Arc<SubscribeUpdate>, usize)) {
        self.total_bytes = self
            .total_bytes
            .checked_add(item_len)
            .expect("total bytes overflow");
        self.items.push_back((item, item_len));

        while self.total_bytes > self.max_total_bytes || self.items.len() > self.max_len {
            let (_item, item_len) = self.items.pop_front().expect("can't be empty");
            self.total_bytes = self
                .total_bytes
                .checked_sub(item_len)
                .expect("total bytes underflow");
        }
    }
}

type ReceiverStreamRx = (
    Result<GrpcMessageWithId, broadcast::error::RecvError>,
    broadcast::Receiver<GrpcMessageWithId>,
);

pub struct ReceiverStream {
    client_id: GrpcClientId,
    rx: BoxFuture<'static, ReceiverStreamRx>,
    finished: bool,
}

impl ReceiverStream {
    fn new(client_id: GrpcClientId, rx: broadcast::Receiver<GrpcMessageWithId>) -> Self {
        Self {
            client_id,
            rx: Self::make_fut(rx),
            finished: false,
        }
    }

    fn make_fut(
        mut rx: broadcast::Receiver<GrpcMessageWithId>,
    ) -> BoxFuture<'static, ReceiverStreamRx> {
        async move { (rx.recv().await, rx) }.boxed()
    }
}

impl Stream for ReceiverStream {
    type Item = TonicResult<SubscribeUpdate>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        Poll::Ready(Some(loop {
            let (result, rx) = futures::ready!(self.rx.as_mut().poll(cx));
            self.rx = Self::make_fut(rx);

            match result {
                Ok(GrpcMessageWithId { id, message }) if id == self.client_id => {
                    if let GrpcMessage::ClientNewFilter(filter_id) = message {
                        //
                        todo!()
                    }
                }
                Ok(_message) => {}
                Err(error) => {
                    self.finished = true;
                    let msg = match error {
                        broadcast::error::RecvError::Closed => "stream: filter channel closed",
                        broadcast::error::RecvError::Lagged(_) => "stream: filter channel lagged",
                    };
                    break Err(Status::internal(msg));
                }
            }
        }))

        // let msg = futures::ready!(self.inner.poll_recv(cx));
        // CONNECTION_INFO.with_label_values(&[&self.id, "size"]).dec();
        // // if let Some(Ok(SubscribeUpdate {
        // //     update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot { slot, status, .. })),
        // //     ..
        // // })) = &msg
        // // {
        // //     if *status == CommitmentLevel::Finalized as i32 {
        // //         CONNECTION_INFO
        // //             .with_label_values(&[&self.id, "ppop"])
        // //             .set(*slot as i64);
        // //     }
        // // }
        // if let Some(Ok(msg)) = &msg {
        //     if let Some(slot) = msg.finalized_slot {
        //         CONNECTION_INFO
        //             .with_label_values(&[&self.id, "ppop"])
        //             .set(slot as i64);
        //     }
        // }
        // Poll::Ready(msg)
    }
}

#[derive(Debug)]
pub struct GrpcService {
    config: ConfigGrpc,
    blocks_meta: Option<BlocksMetaStorage>,
    subscribe_client_id: AtomicU64,
    snapshot_rx: Mutex<Option<crossbeam_channel::Receiver<Option<GeyserMessage>>>>,
    filters: Arc<Filters>,
}

impl GrpcService {
    #[allow(clippy::type_complexity)]
    pub async fn create(
        filters: Arc<Filters>,
        blocks_meta: Option<BlocksMetaStorage>,
        config: ConfigGrpc,
    ) -> Result<
        (
            Option<crossbeam_channel::Sender<Option<GeyserMessage>>>,
            Arc<Notify>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        // Bind service address
        let incoming = TcpIncoming::new(
            config.address,
            true,                          // tcp_nodelay
            Some(Duration::from_secs(20)), // tcp_keepalive
        )?;

        // Snapshot channel
        let (snapshot_tx, snapshot_rx) = match config.snapshot_plugin_channel_capacity {
            Some(cap) => {
                let (tx, rx) = crossbeam_channel::bounded(cap);
                (Some(tx), Some(rx))
            }
            None => (None, None),
        };

        // gRPC server builder with optional TLS
        let mut server_builder = Server::builder();
        if let Some(tls_config) = &config.tls_config {
            let (cert, key) = tokio::try_join!(
                fs::read(&tls_config.cert_path),
                fs::read(&tls_config.key_path)
            )?;
            server_builder = server_builder
                .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))?;
        }

        // Create Server
        let service = GeyserServer::new(Self {
            config,
            blocks_meta,
            subscribe_client_id: AtomicU64::new(0),
            snapshot_rx: Mutex::new(snapshot_rx),
            filters,
        })
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

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

        Ok((snapshot_tx, shutdown))
    }

    async fn client_loop(
        id: u64,
        mut filter: Filter,
        stream_tx: mpsc::Sender<TonicResult<SubscribeUpdate>>,
        mut client_rx: mpsc::UnboundedReceiver<Option<Filter>>,
        mut snapshot_rx: Option<crossbeam_channel::Receiver<Option<GeyserMessage>>>,
        // mut messages_rx: broadcast::Receiver<(CommitmentLevel, Arc<Vec<Message>>)>,
        filters: Arc<Filters>,
        drop_client: impl FnOnce(),
    ) {
        CONNECTIONS_TOTAL.inc();
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
                    // message = messages_rx.recv() => {
                    //     let (commitment, messages) = match message {
                    //         Ok((commitment, messages)) => (commitment, messages),
                    //         Err(broadcast::error::RecvError::Closed) => {
                    //             break 'outer;
                    //         },
                    //         Err(broadcast::error::RecvError::Lagged(_)) => {
                    //             info!("client #{id}: lagged to receive geyser messages");
                    //             tokio::spawn(async move {
                    //                 let _ = stream_tx.send(Err(Status::internal("lagged"))).await;
                    //             });
                    //             break 'outer;
                    //         }
                    //     };

                    //     if commitment == filter.get_commitment_level() {
                    //         for message in messages.iter() {
                    //             for message in filter.get_update(message, Some(commitment)) {
                    //                 match stream_tx.try_send(Ok(message)) {
                    //                     Ok(()) => {}
                    //                     Err(mpsc::error::TrySendError::Full(_)) => {
                    //                         error!("client #{id}: lagged to send update");
                    //                         tokio::spawn(async move {
                    //                             let _ = stream_tx.send(Err(Status::internal("lagged"))).await;
                    //                         });
                    //                         break 'outer;
                    //                     }
                    //                     Err(mpsc::error::TrySendError::Closed(_)) => {
                    //                         error!("client #{id}: stream closed");
                    //                         break 'outer;
                    //                     }
                    //                 }
                    //             }
                    //         }
                    //     }
                    // }
                }
            }
        }

        info!("client #{id}: removed");
        CONNECTIONS_TOTAL.dec();
        drop_client();
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let id = GrpcClientId(self.subscribe_client_id.fetch_add(1, Ordering::Relaxed));

        Ok(Response::new(ReceiverStream::new(
            id,
            self.filters.subscribe(),
        )))

        // let filter = Filter::new(
        //     &SubscribeRequest {
        //         accounts: HashMap::new(),
        //         slots: HashMap::new(),
        //         transactions: HashMap::new(),
        //         blocks: HashMap::new(),
        //         blocks_meta: HashMap::new(),
        //         entry: HashMap::new(),
        //         commitment: None,
        //         accounts_data_slice: Vec::new(),
        //         ping: None,
        //     },
        //     &self.config.filters,
        // )
        // .expect("empty filter");
        // let snapshot_rx = self.snapshot_rx.lock().await.take();
        // let (stream_tx, stream_rx) = mpsc::channel(if snapshot_rx.is_some() {
        //     self.config.snapshot_client_channel_capacity
        // } else {
        //     // self.config.channel_capacity
        //     250_000 // TODO
        // });
        // let (client_tx, client_rx) = mpsc::unbounded_channel();
        // let notify_exit1 = Arc::new(Notify::new());
        // let notify_exit2 = Arc::new(Notify::new());

        // let ping_stream_tx = stream_tx.clone();
        // let ping_client_tx = client_tx.clone();
        // let ping_exit = Arc::clone(&notify_exit1);
        // tokio::spawn(async move {
        //     let exit = ping_exit.notified();
        //     tokio::pin!(exit);

        //     let ping_msg = SubscribeUpdate {
        //         filters: vec![],
        //         update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
        //     };

        //     loop {
        //         tokio::select! {
        //             _ = &mut exit => {
        //                 break;
        //             }
        //             _ = sleep(Duration::from_secs(10)) => {
        //                 match ping_stream_tx.try_send(Ok(ping_msg.clone())) {
        //                     Ok(()) => {}
        //                     Err(mpsc::error::TrySendError::Full(_)) => {}
        //                     Err(mpsc::error::TrySendError::Closed(_)) => {
        //                         let _ = ping_client_tx.send(None);
        //                         break;
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // });

        // let config_filters_limit = self.config.filters.clone();
        // let incoming_stream_tx = stream_tx.clone();
        // let incoming_client_tx = client_tx;
        // let incoming_exit = Arc::clone(&notify_exit2);
        // tokio::spawn(async move {
        //     let exit = incoming_exit.notified();
        //     tokio::pin!(exit);

        //     loop {
        //         tokio::select! {
        //             _ = &mut exit => {
        //                 break;
        //             }
        //             message = request.get_mut().message() => match message {
        //                 Ok(Some(request)) => {
        //                     if let Err(error) = match Filter::new(&request, &config_filters_limit) {
        //                         Ok(filter) => match incoming_client_tx.send(Some(filter)) {
        //                             Ok(()) => Ok(()),
        //                             Err(error) => Err(error.to_string()),
        //                         },
        //                         Err(error) => Err(error.to_string()),
        //                     } {
        //                         let err = Err(Status::invalid_argument(format!(
        //                             "failed to create filter: {error}"
        //                         )));
        //                         if incoming_stream_tx.send(err).await.is_err() {
        //                             let _ = incoming_client_tx.send(None);
        //                         }
        //                     }
        //                 }
        //                 Ok(None) => {
        //                     break;
        //                 }
        //                 Err(_error) => {
        //                     let _ = incoming_client_tx.send(None);
        //                     break;
        //                 }
        //             }
        //         }
        //     }
        // });

        // tokio::spawn(Self::client_loop(
        //     id,
        //     filter,
        //     stream_tx,
        //     client_rx,
        //     snapshot_rx,
        //     Arc::clone(&self.filters),
        //     move || {
        //         notify_exit1.notify_one();
        //         notify_exit2.notify_one();
        //     },
        // ));

        // Ok(Response::new(ReceiverStream::new(stream_rx)))
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
                        block.block_height.map(|last_valid_block_height| {
                            GetLatestBlockhashResponse {
                                slot: block.slot,
                                blockhash: block.blockhash.clone(),
                                last_valid_block_height,
                            }
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
            version: serde_json::to_string(&VERSION).unwrap(),
        }))
    }
}
