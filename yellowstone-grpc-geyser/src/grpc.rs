use {
    crate::{
        block_reconstruction::BlockMachineStorage,
        config::{ConfigGrpc, GrpcAddress, GrpcTlsConfig},
        metered::PrometheusMeteredManager,
        metrics::{
            self, incr_grpc_method_call_count, set_subscriber_queue_size,
            subscription_limit_exceeded_inc, DebugClientMessage, GEYSER_BATCH_SIZE,
        },
        plugin::{
            filter::{
                encoder::encode_messages,
                limits::FilterLimits,
                message::{FilteredUpdate, FilteredUpdateOneof},
                name::FilterNames,
                Filter,
            },
            message::{CommitmentLevel, Message, MessageBlockMeta, MessageSlot, SlotStatus},
            proto::geyser_server::{Geyser, GeyserServer},
        },
        ratelimit::PrometheusRatelimitCallbacks,
        util::stream::{load_aware_channel, LoadAwareReceiver, LoadAwareSender},
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
    smallvec::SmallVec,
    solana_clock::{Slot, MAX_RECENT_BLOCKHASHES},
    std::{
        collections::HashMap,
        io,
        net::SocketAddr,
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, LazyLock, Mutex as StdMutex,
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
    tokio_stream::wrappers::UnixListenerStream,
    tokio_util::{sync::CancellationToken, task::TaskTracker},
    tonic::{
        metadata::AsciiMetadataValue,
        service::interceptor,
        transport::{
            server::{Connected, Server, TcpConnectInfo, TlsConnectInfo},
            CertificateDer,
        },
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    tonic_health::{pb::health_server::HealthServer, server::health_reporter},
    yellowstone_grpc_proto::prelude::{
        CommitmentLevel as CommitmentLevelProto, GetBlockHeightRequest, GetBlockHeightResponse,
        GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
        GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse,
        PingRequest, PongResponse, SubscribeDeshredRequest, SubscribeReplayInfoRequest,
        SubscribeReplayInfoResponse, SubscribeRequest,
    },
    yellowstone_grpc_tools::server::{
        tcp::{TcpConfiguration, TcpIncoming as TritonTcpIncoming},
        tls::{build_sni_resolver_from_cert_dir, HotResolvesServerCertUsingSni, TlsIncoming},
        tonic::{
            metered::{MeteredBandwidthLayer, DEFAULT_TRAFFIC_REPORTING_THRESHOLD},
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

type BroadcastedMessage = (CommitmentLevel, Arc<Vec<Message>>);

enum ReplayedResponse {
    Messages(Vec<Message>),
    Lagged(Slot),
}

type ReplayStoredSlotsRequest = (CommitmentLevel, Slot, oneshot::Sender<ReplayedResponse>);

type SubscriptionTracker = Arc<StdMutex<HashMap<String, usize>>>;

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
    subscription_tracker: SubscriptionTracker,
}

impl ClientSession {
    fn new(
        id: usize,
        subscriber_id: Option<String>,
        endpoint: String,
        maybe_remote_peer_sk_addr: Option<SocketAddr>,
        debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        cancellation_token: CancellationToken,
        subscription_tracker: SubscriptionTracker,
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
            subscription_tracker,
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
        {
            let mut tracker = self
                .subscription_tracker
                .lock()
                .expect("subscription_tracker mutex poisoned");
            if let Some(count) = tracker.get_mut(&self.subscriber_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    tracker.remove(&self.subscriber_id);
                }
            }
        }
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
    Tcp(TritonTcpIncoming),
    Tls(TlsIncoming),
    Unix(AutoClosableUnixListenerStream), // path needed to remove the socket file on exit
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

#[derive(Debug, Clone)]
pub struct GrpcService {
    config_snapshot_client_channel_capacity: usize,
    config_channel_capacity: usize,
    config_filter_limits: Arc<FilterLimits>,
    subscription_limit: usize,
    subscription_limit_enforce: bool,
    subscription_tracker: SubscriptionTracker,
    blocks_meta: Option<Arc<BlockMetaStorage>>,
    subscribe_id: Arc<AtomicUsize>,
    snapshot_rx: Arc<Mutex<Option<crossbeam_channel::Receiver<Box<Message>>>>>,
    broadcast_tx: broadcast::Sender<BroadcastedMessage>,
    replay_stored_slots_tx: Option<mpsc::Sender<ReplayStoredSlotsRequest>>,
    replay_first_available_slot: Option<Arc<AtomicU64>>,
    debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
    filter_names: Arc<Mutex<FilterNames>>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

#[derive(Debug, thiserror::Error)]
pub enum TlsConfigLoadError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
    #[error(transparent)]
    PemError(#[from] rustls::pki_types::pem::Error),
}

///
/// Loads TLS server configuration from the given `GrpcTlsConfig`. This is used for both the legacy `tls_config` field and the new `listen[].tls` field in the configuration. The `listen[].tls` field takes precedence over the legacy `tls_config` if both are set.
///
fn load_server_config_from_tls_config(
    tls_config: &GrpcTlsConfig,
) -> Result<ServerConfig, TlsConfigLoadError> {
    match tls_config {
        GrpcTlsConfig::IdentityPair { identity } => {
            let cert_pem = std::fs::read(&identity.cert_path)?;
            let key = std::fs::read(&identity.key_path)?;

            let cert_der = CertificateDer::from_pem_slice(&cert_pem)?;
            let key_der = PrivateKeyDer::from_pem_slice(&key)?;
            let mut server_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert_der], key_der)?;
            // gRPC over TLS requires ALPN negotiation for HTTP/2.
            server_config.alpn_protocols = vec![b"h2".to_vec()];
            Ok(server_config)
        }
        GrpcTlsConfig::CertDir { cert_dir } => {
            let resolver = build_sni_resolver_from_cert_dir(cert_dir.clone())?;
            let hot_resolver = HotResolvesServerCertUsingSni::from(resolver);
            let mut server_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(hot_resolver));
            // gRPC over TLS requires ALPN negotiation for HTTP/2.
            server_config.alpn_protocols = vec![b"h2".to_vec()];
            Ok(server_config)
        }
    }
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
            // TODO: supports hot reload via sighub signal and watching cert_dir changes
            if let Some(cert_dir) = &config.cert_dir {
                let resolver = build_sni_resolver_from_cert_dir(cert_dir.clone())?;
                let hot_resolver = HotResolvesServerCertUsingSni::from(resolver);
                let mut server_config = ServerConfig::builder()
                    .with_no_client_auth()
                    .with_cert_resolver(Arc::new(hot_resolver));
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
                        listeners.push(Listener::Tcp(incoming));
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
                        listeners.push(Listener::Unix(uds));
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
                        let listener = if let Some(tls) = tls_config {
                            let server_config = load_server_config_from_tls_config(&tls)
                                .context("failed to load TLS server config")?;
                            let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));
                            Listener::Tls(TlsIncoming::new(incoming, tls_acceptor))
                        } else {
                            Listener::Tcp(incoming)
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
                        listeners.push(Listener::Unix(uds));
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

        // Messages to clients combined by commitment
        let (broadcast_tx, _) = broadcast::channel(config.channel_capacity);
        let (replay_first_available_slot, replay_stored_slots_tx, replay_stored_slots_rx) =
            if config.replay_stored_slots == 0 {
                (None, None, None)
            } else {
                let (tx, rx) = mpsc::channel(1);
                (Some(Arc::new(AtomicU64::new(u64::MAX))), Some(tx), Some(rx))
            };

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

        let filter_names = Arc::new(Mutex::new(FilterNames::new(
            config.filter_name_size_limit,
            config.filter_names_size_limit,
            config.filter_names_cleanup_interval,
        )));

        // Build the shared GeyserServer (Clone-able because GrpcService: Clone)
        let max_decoding_message_size = config.max_decoding_message_size;
        let mut service = GeyserServer::new(Self {
            config_snapshot_client_channel_capacity: config.snapshot_client_channel_capacity,
            config_channel_capacity: config.channel_capacity,
            config_filter_limits: Arc::new(config.filter_limits),
            subscription_limit: config.subscription_limit,
            subscription_limit_enforce: config.subscription_limit_enforce,
            subscription_tracker: Arc::new(StdMutex::new(HashMap::new())),
            blocks_meta: blocks_meta.map(Arc::new),
            subscribe_id: Arc::new(AtomicUsize::new(0)),
            snapshot_rx: Arc::new(Mutex::new(snapshot_rx)),
            broadcast_tx: broadcast_tx.clone(),
            replay_stored_slots_tx,
            replay_first_available_slot: replay_first_available_slot.clone(),
            debug_clients_tx,
            filter_names,
            cancellation_token: service_cancellation_token.clone(),
            task_tracker: task_tracker.clone(),
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
        let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();

        // Warn if replay buffer is too small for auto-reconnect
        if config.replay_stored_slots < 150 {
            log::warn!(
                "replay_stored_slots={} may be too low for auto-reconnect; recommend >= 150",
                config.replay_stored_slots
            );
        }

        {
            let broadcast_tx = broadcast_tx.clone();
            let completion_token = task_tracker.token();
            let _ = std::thread::Builder::new()
                .name("solGrpcBlockReconstruction".to_string())
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to create Tokio runtime for BlockReconstruction");

                    runtime.block_on(async move {
                        Self::block_reconstruction_loop(
                            block_reconstruction_rx,
                            blocks_meta_tx,
                            broadcast_tx,
                            replay_stored_slots_rx,
                            replay_first_available_slot,
                            config.replay_stored_slots,
                        )
                        .await;
                    });

                    drop(completion_token);
                });
        }

        task_tracker.spawn(async move {
            Self::geyser_loop(messages_rx, broadcast_tx, block_reconstruction_tx).await;
        });

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
            ($listener:expr, |$incoming:ident| $body:expr) => {{
                match $listener {
                    Listener::Tcp($incoming) => $body,
                    Listener::Unix($incoming) => $body,
                    Listener::Tls($incoming) => $body,
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
                if let Err(e) = with_listener!(listener, |incoming| {
                    let rate_limited_incoming = RateLimitedIncoming::new(
                        incoming,
                        ip_conncur,
                        rate_limit_table,
                        PrometheusRatelimitCallbacks,
                    );

                    GrpcService::serve_listener(
                        rate_limited_incoming,
                        http2_adaptive_window,
                        http2_keepalive_interval,
                        http2_keepalive_timeout,
                        initial_connection_window_size,
                        initial_stream_window_size,
                        x_token,
                        health_service,
                        service,
                        traffic_reporting_threshold,
                        shutdown.clone(),
                    )
                    .await
                }) {
                    error!("gRPC listener failed: {e}");
                    shutdown.cancel();
                }
            });
        }

        Ok((snapshot_tx, messages_tx))
    }

    /// Core message routing loop that reconstructs Solana blocks from raw Geyser plugin events
    /// and fans them out to subscribers at the correct commitment levels.
    ///
    /// # Slot status semantics
    ///
    /// Slot messages come in two categories with different routing rules:
    ///
    /// **Lifecycle statuses** (`FirstShredReceived`, `Completed`, `CreatedBank`, `Dead`):
    /// Broadcast immediately to **all three** commitment levels (Processed, Confirmed, Finalized).
    /// These are not commitment signals — they describe the physical state of the slot and must
    /// reach every subscriber regardless of the commitment level they subscribed at, so they can
    /// track slot lifecycle and detect skipped slots.
    ///
    /// **Commitment statuses** (`Processed`, `Confirmed`, `Finalized`):
    /// Never broadcast directly when received from the plugin. They are fed into `BlockMachineStorage`
    /// which holds them until the block is fully assembled. Once the block is frozen, the synthetic
    /// slot message is broadcast to **all three** commitment levels. This guarantees that no
    /// subscriber sees a commitment status before the block content for that slot.
    ///
    /// # Block content delivery
    ///
    /// When a block becomes ready at commitment level C (`pop_ready_block`):
    /// 1. If C is Confirmed or Finalized: `frozen_block.messages()` (the complete, deduplicated
    ///    set of Account/Transaction/Entry messages for that slot) is sent at level C first.
    ///    This is omitted at Processed because those messages were already delivered individually
    ///    as they arrived.
    /// 2. `Message::Block` (the precomputed block summary) is sent at level C.
    /// 3. The synthetic Processed/Confirmed/Finalized slot status message is sent to all three
    ///    commitment levels.
    ///
    /// This ordering guarantee — block content before `Message::Block` before slot status — must
    /// be preserved so that subscribers always observe a complete block before the commitment signal.
    ///
    /// # Account deduplication
    ///
    /// Within a slot, if multiple updates arrive for the same account pubkey, only the update with
    /// the highest `write_version` is retained in the frozen block. This is handled internally by
    /// `BlockMachineStorage` / `ProcessingSlot` and must not be bypassed.
    ///
    /// # Missing commitment level gap-filling
    ///
    /// If a higher commitment level arrives without a prior lower one (e.g. Finalized before
    /// Confirmed), `BlocksStateMachine` synthesizes the missing levels in order
    /// (Processed → Confirmed → Finalized). Each synthesized level causes a separate
    /// `pop_ready_block` entry and a separate fan-out.
    ///
    /// # Ancestor slot propagation
    ///
    /// When a descendant slot is finalized, `BlocksStateMachine` retroactively finalizes all
    /// ancestor slots that were not yet finalized. This mirrors the parent-chain walk that
    /// `geyser_loop` performs manually. It must not be short-circuited.
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
    async fn geyser_loop(
        mut messages_rx: mpsc::UnboundedReceiver<Message>,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        block_reconstruction_tx: mpsc::UnboundedSender<Message>,
    ) {
        const PROCESSED_MESSAGES_MAX: usize = 31;
        const STATE_MESSAGES_MAX: usize = 4; /* In a reasonable loop, we don't expect to receive more than FirstShredReceived, Completed, CreatedBank, or Finalized messages per iteration */

        let mut processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);
        let mut confirmed_messages = Vec::with_capacity(STATE_MESSAGES_MAX);
        let mut finalized_messages = Vec::with_capacity(STATE_MESSAGES_MAX);

        let mut blockmeta_detected: SmallVec<[Message; 4]> = SmallVec::new();
        loop {
            tokio::select! {
                maybe = messages_rx.recv() => {
                    let Some(message) = maybe else {
                        info!("Geyser loop: messages channel closed");
                        break;
                    };
                    metrics::message_queue_size_dec();

                    if matches!(&message, Message::BlockMeta(_)) {
                        let _ = block_reconstruction_tx.send(message);
                        continue;
                    }

                    processed_messages.push(message);

                    while let Ok(message) = messages_rx.try_recv() {
                        metrics::message_queue_size_dec();

                        if matches!(&message, Message::BlockMeta(_)) {
                            blockmeta_detected.push(message);
                            continue;
                        }

                        processed_messages.push(message);
                        if processed_messages.len() >= PROCESSED_MESSAGES_MAX {
                            break;
                        }
                    }

                    for message in processed_messages.iter() {
                        let _ = block_reconstruction_tx.send(message.clone());

                        match message {
                            Message::Slot(slot_message) => {
                                metrics::update_slot_plugin_status(slot_message.status, slot_message.slot);
                                // Only match on slot lifecycle update not commitment update, as
                                // we must go through the block machine to make sure users sees block content before any commitment update.
                                if matches!(slot_message.status,
                                    SlotStatus::FirstShredReceived |
                                    SlotStatus::Completed |
                                    SlotStatus::CreatedBank |
                                    SlotStatus::Dead
                                ) {
                                    confirmed_messages.push(Message::Slot(slot_message.clone()));
                                    finalized_messages.push(Message::Slot(slot_message.clone()));
                                }
                            }
                            Message::Block(_) => {
                               unreachable!("Block message should not be sent by plugin directly, it is constructed in geyser loop after receiving all necessary messages for the slot and then broadcasted to subscribers");
                            }
                            _ => {
                                /* We don't need to process anything here.  */
                            }
                        }
                    }

                    encode_messages(&processed_messages);
                    GEYSER_BATCH_SIZE.observe(processed_messages.len() as f64);
                    let _ = broadcast_tx.send((CommitmentLevel::Processed, Arc::new(processed_messages)));
                    processed_messages = Vec::with_capacity(PROCESSED_MESSAGES_MAX);

                    if !confirmed_messages.is_empty() {
                        let _ = broadcast_tx.send((CommitmentLevel::Confirmed, Arc::new(confirmed_messages)));
                        confirmed_messages = Vec::with_capacity(STATE_MESSAGES_MAX);
                    }

                    if !finalized_messages.is_empty() {
                        let _ = broadcast_tx.send((CommitmentLevel::Finalized, Arc::new(finalized_messages)));
                        finalized_messages = Vec::with_capacity(STATE_MESSAGES_MAX);
                    }

                    // Make sure that blockmeta is always after all kind of other events so the block-machine sees every block
                    // updates.
                    for blockmeta_message in blockmeta_detected.drain(..) {
                        let _ = block_reconstruction_tx.send(blockmeta_message);
                    }
                }
            }
        }
    }

    async fn block_reconstruction_loop(
        mut messages_rx: mpsc::UnboundedReceiver<Message>,
        blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        replay_stored_slots_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        replay_first_available_slot: Option<Arc<AtomicU64>>,
        replay_stored_slots: u64,
    ) {
        let (_tx, rx) = mpsc::channel(1);
        let mut replay_stored_slots_rx = replay_stored_slots_rx.unwrap_or(rx);

        let mut block_machine = BlockMachineStorage::new(replay_stored_slots as usize);
        const ALL_COMMITMENT_LEVELS: [CommitmentLevel; 3] = [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ];

        loop {
            tokio::select! {
                maybe = messages_rx.recv() => {
                    let Some(message) = maybe else {
                        info!("Geyser loop: messages channel closed");
                        break;
                    };


                    if let Some(blocks_meta_tx) = &blocks_meta_tx {
                        if matches!(&message, Message::Slot(_) | Message::BlockMeta(_)) {
                            let _ = blocks_meta_tx.send(message.clone());
                        }
                    }

                    block_machine.add(message);

                    while let Some((slot_update, frozen_block)) = block_machine.pop_ready_block() {
                        let commitment_level = match slot_update.commitment {
                            solana_commitment_config::CommitmentLevel::Processed => CommitmentLevel::Processed,
                            solana_commitment_config::CommitmentLevel::Confirmed => CommitmentLevel::Confirmed,
                            solana_commitment_config::CommitmentLevel::Finalized => CommitmentLevel::Finalized,
                        };
                        // Processed must be sent differently, since processed geyser event were individually sent,
                        // we only need to send Message::Block for block subscriber downstream.
                        // While, confirmed,finalized must be sent in the two flavors: as a stream of individual events and block.
                        if commitment_level != CommitmentLevel::Processed {
                            let _ = broadcast_tx.send((commitment_level, frozen_block.messages()));
                        }

                        let block_meta = Message::BlockMeta(frozen_block.get_block_meta());
                        let msg_block = Message::Block(Arc::new(frozen_block.get_message_block()));
                        let _ = broadcast_tx.send((commitment_level, Arc::new(vec![msg_block, block_meta])));

                        let slot_message = Message::Slot(MessageSlot {
                            slot: slot_update.slot,
                            parent: slot_update.parent_slot,
                            status: match slot_update.commitment {
                                solana_commitment_config::CommitmentLevel::Processed => SlotStatus::Processed,
                                solana_commitment_config::CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                                solana_commitment_config::CommitmentLevel::Finalized => SlotStatus::Finalized,
                            },
                            dead_error: None,
                            created_at: Timestamp::from(SystemTime::now())
                        });
                        let slot_message_singleton_vec = Arc::new(vec![slot_message.clone()]);
                        for commitment_level in ALL_COMMITMENT_LEVELS {
                            let _ = broadcast_tx.send((commitment_level, Arc::clone(&slot_message_singleton_vec)));
                        }
                    }
                    let min_replayable_slot = block_machine.min_replayable_slot();
                    if let (Some(min_slot), Some(replay_first_available_slot)) = (min_replayable_slot, replay_first_available_slot.as_ref()) {
                        replay_first_available_slot.store(min_slot, Ordering::Relaxed);
                    }
                },
                Some((commitment, replay_slot, tx)) = replay_stored_slots_rx.recv() => {

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

                    let mut replayed_messages = Vec::with_capacity(32_768);
                    let replayed_slot_iter = block_machine.replay_from_slot(replay_slot, min_solana_commitment);

                    for replayed_slot in replayed_slot_iter {
                        let xs = replayed_slot
                            .slot_status_messages
                            .iter()
                            .map(|s| {
                                Message::Slot(MessageSlot {
                                    slot: s.slot,
                                    parent: None,
                                    status: match s.commitment {
                                        solana_commitment_config::CommitmentLevel::Processed => SlotStatus::Processed,
                                        solana_commitment_config::CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                                        solana_commitment_config::CommitmentLevel::Finalized => SlotStatus::Finalized,
                                    },
                                    dead_error: None,
                                    created_at: Timestamp::from(SystemTime::now())
                                })
                            });
                        replayed_messages.extend(xs);
                        replayed_messages.extend(replayed_slot.frozen_block.messages().iter().cloned());
                    }
                    let _ = tx.send(ReplayedResponse::Messages(replayed_messages));
                }
                else => {
                    // No new messages and replay request channel closed, can only happen on shutdown
                    info!("Block reconstruction loop: replay_stored_slots channel closed");
                    break;
                }
            }
        }
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
            subscriber_id,
            endpoint,
            maybe_remote_peer_sk_addr,
            debug_client_tx,
            cancellation_token,
            subscription_tracker,
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

                                let messages = match rx.await {
                                    Ok(ReplayedResponse::Messages(messages)) => messages,
                                    Ok(ReplayedResponse::Lagged(slot)) => {
                                        info!("client #{id}: broadcast from {from_slot} is not available");
                                        task_tracker.spawn(async move {
                                            let message = format!(
                                                "broadcast from {from_slot} is not available, last available: {slot}"
                                            );
                                            let _ = stream_tx.send(Err(Status::out_of_range(message))).await;
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

                                for message in messages.iter() {
                                    for message in session.filter.get_updates(message, Some(commitment)) {
                                        match stream_tx.send(Ok(message)).await {
                                            Ok(()) => {
                                                metrics::incr_grpc_message_sent_counter(&session.subscriber_id);
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
                        for message in messages.iter() {
                            for message in session.filter.get_updates(message, Some(commitment)) {
                                match stream_tx.try_send(Ok(message)) {
                                    Ok(()) => {
                                        metrics::incr_grpc_message_sent_counter(&session.subscriber_id);
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
                            if let Message::Slot(slot_message) = &message {
                                DebugClientMessage::maybe_send(&session.debug_client_tx, || DebugClientMessage::UpdateSlot { id, slot: slot_message.slot });
                            }
                        }
                    }
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

    #[allow(clippy::too_many_arguments)]
    async fn serve_listener<H, I, IO>(
        incoming: I,
        http2_adaptive_window: Option<bool>,
        http2_keepalive_interval: Option<Duration>,
        http2_keepalive_timeout: Option<Duration>,
        initial_connection_window_size: Option<u32>,
        initial_stream_window_size: Option<u32>,
        x_token: Option<AsciiMetadataValue>,
        health_service: HealthServer<H>,
        service: GeyserServer<GrpcService>,
        traffic_reporting_threshold: ByteSize,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()>
    where
        I: Stream<Item = io::Result<IO>> + Send + 'static,
        IO: Connected + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        H: tonic_health::pb::health_server::Health,
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
            .layer(MeteredBandwidthLayer::new(
                PrometheusMeteredManager,
                traffic_reporting_threshold,
            ))
            .layer(interceptor::InterceptorLayer::new(XTokenInterceptor {
                x_token,
            }))
            .add_service(health_service)
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, shutdown.cancelled())
            .await
            .map_err(Into::into)
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
        incr_grpc_method_call_count("subscribe");

        let subscriber_id = request
            .metadata()
            .get("x-subscription-id")
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            .or_else(|| request.remote_addr().map(|addr| addr.ip().to_string()));

        // Per-subscriber subscription limit: check and increment under a
        // single lock hold so no two calls can race past the limit.
        // Cleanup (decrement) is handled by `ClientSession::drop()`.
        // When subscriber_id is None (no x-subscription-id header and no
        // remote address) we skip the limit check entirely rather than
        // grouping all unidentified clients into a shared bucket.
        if let Some(id) = subscriber_id.as_deref() {
            if self.subscription_limit > 0 {
                let mut tracker = self
                    .subscription_tracker
                    .lock()
                    .expect("subscription_tracker mutex poisoned");
                let count = tracker.entry(id.to_owned()).or_insert(0);

                if *count >= self.subscription_limit {
                    subscription_limit_exceeded_inc(id);
                    if self.subscription_limit_enforce {
                        return Err(Status::resource_exhausted(
                            "max subscription limit exceeded",
                        ));
                    }
                    info!(
                        "subscriber {id:?} over limit ({count}/{}), not enforcing",
                        self.subscription_limit
                    );
                }
                *count += 1;
            }
        }

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
            Arc::clone(&self.subscription_tracker),
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
        let st: SubscriptionTracker = Arc::new(StdMutex::new(HashMap::new()));
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
            Arc::clone(&st),
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
        let _ = broadcast_tx.send((CommitmentLevel::Processed, Arc::new(vec![msg])));

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("client_loop did not exit")
            .expect("client_loop panicked");

        assert!(ct.is_cancelled());
    }

    #[tokio::test]
    async fn test_subscription_tracker_decrements_on_session_drop() {
        let tracker: SubscriptionTracker = Arc::new(StdMutex::new(HashMap::new()));

        // simulate what subscribe() does: increment under lock
        {
            let mut map = tracker.lock().unwrap();
            *map.entry("sub-1".to_owned()).or_insert(0) += 1;
            *map.entry("sub-1".to_owned()).or_insert(0) += 1;
        }
        assert_eq!(*tracker.lock().unwrap().get("sub-1").unwrap(), 2);

        // create a session (mirrors what client_loop does)
        {
            let _session = ClientSession::new(
                0,
                Some("sub-1".into()),
                "".into(),
                None,
                None,
                CancellationToken::new(),
                Arc::clone(&tracker),
            );
            // session alive: count unchanged
            assert_eq!(*tracker.lock().unwrap().get("sub-1").unwrap(), 2);
        }
        // session dropped: count decremented
        assert_eq!(*tracker.lock().unwrap().get("sub-1").unwrap(), 1);

        // second drop removes the entry entirely
        {
            let _session = ClientSession::new(
                1,
                Some("sub-1".into()),
                "".into(),
                None,
                None,
                CancellationToken::new(),
                Arc::clone(&tracker),
            );
        }
        assert!(tracker.lock().unwrap().get("sub-1").is_none());
    }

    #[tokio::test]
    async fn test_subscription_tracker_skips_unidentified_subscribers() {
        let tracker: SubscriptionTracker = Arc::new(StdMutex::new(HashMap::new()));

        // subscriber_id=None resolves to "UNKNOWN" inside ClientSession,
        // but subscribe() skips the limit check entirely for None.
        // The tracker should remain empty since no increment happened.
        {
            let _session = ClientSession::new(
                0,
                None,
                "".into(),
                None,
                None,
                CancellationToken::new(),
                Arc::clone(&tracker),
            );
        }
        // drop fires but "UNKNOWN" was never in the tracker, so nothing changes
        assert!(tracker.lock().unwrap().is_empty());
    }
}
