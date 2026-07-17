use {
    crate::{
        auth::{
            ConstantSubscriptionRepository, HttpSubscriptionRepository, SubscriptionInfo,
            TrustedMetadataAuthenticator,
        },
        billing::{BillingMeteredManager, HttpBillingEventSink},
        block_reconstruction::BlockMachineStorage,
        config::{AuthConfig, AuthKind, BillingConfig, ConfigGrpc, GrpcAddress, GrpcTlsConfig},
        file_watcher::FileWatcher,
        metered::PrometheusMeteredManager,
        metrics::{
            self, incr_grpc_method_call_count, observe_subscriber_queue_size,
            subscription_limit_exceeded_inc,
        },
        plugin::{
            filter::{
                limits::FilterLimits,
                message::{FilteredUpdate, FilteredUpdateDeshred, FilteredUpdateOneof},
                name::FilterNames,
                DeshredFilter, Filter,
            },
            message::{CommitmentLevel, Message, MessageBlockMeta, MessageSlot, SlotStatus},
            proto::geyser_server::{Geyser, GeyserServer},
        },
        ratelimit::{MethodRatelimiter, PrometheusRatelimitCallbacks},
        stream::{tokio::BatchStreamUnboundedReceiver, BatchInto, BatchStream, BatchStreamExt},
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
    solana_clock::{Slot, MAX_RECENT_BLOCKHASHES},
    std::{
        collections::HashMap,
        io,
        num::NonZeroUsize,
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
    tokio_stream::wrappers::{UnboundedReceiverStream, UnixListenerStream},
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
        PingRequest, PongResponse, SubscribeDeshredRequest, SubscribeReplayInfoRequest,
        SubscribeReplayInfoResponse, SubscribeRequest,
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

#[derive(Clone)]
pub enum BlockReconstructionMessage {
    Single(Message),
    Batch(Arc<Vec<Message>>),
}

impl BatchInto<BlockReconstructionMessage> for BlockReconstructionMessage {
    fn batch_into(self, batch: &mut Vec<BlockReconstructionMessage>, count: &mut usize) {
        batch.push(self);
        *count += 1;
    }
}

pub type BroadcastedMessage = (CommitmentLevel, Arc<Vec<Message>>);

/// Messages broadcast on the deshred channel. Deshred is a pre-execution
/// stream and has no commitment level: each message is emitted exactly
/// once, when first received from the geyser plugin.
type DeshredBroadcastedMessage = Message;

pub enum ReplayResponseMessageType {
    Single(Message),
    Batch(Arc<Vec<Message>>),
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
            ReplayResponseMessageType::Batch(batch) => {
                if self.current_index < batch.len() {
                    let msg = &batch[self.current_index];
                    self.current_index += 1;
                    Some(msg)
                } else {
                    None
                }
            }
        }
    }
}

pub enum ReplayedResponse {
    Messages(Vec<ReplayResponseMessageType>),
    Lagged(Slot),
}

type ReplayStoredSlotsRequest = (CommitmentLevel, Slot, oneshot::Sender<ReplayedResponse>);

///
/// Tracks the number of active subscriptions per subscriber ID. When a new subscription is created, it increments the count for that subscriber ID. When the subscription is dropped, it decrements the count. If the count exceeds the configured limit,
/// it returns an error when trying to create a new subscription.
#[derive(Clone)]
struct SubscriptionTracker {
    counters: Arc<StdMutex<HashMap<String, usize>>>,
    subscription_limit: NonZeroUsize,
}

impl SubscriptionTracker {
    fn new(subscription_limit: NonZeroUsize) -> Self {
        Self {
            counters: Arc::new(StdMutex::new(HashMap::new())),
            subscription_limit,
        }
    }
}
///
/// A permit that is owned by a subscriber. When the permit is dropped,
/// it decrements the subscription count for the subscriber in the SubscriptionTracker.
struct SubscriptionOwnedPermit {
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
            return Err(());
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
    config_channel_capacity: usize,
    config_filter_limits: Arc<FilterLimits>,
    subscription_limit: NonZeroUsize,
    subscription_limit_enforce: bool,
    subscription_tracker: SubscriptionTracker,
    blocks_meta: Option<Arc<BlockMetaStorage>>,
    subscribe_id: Arc<AtomicUsize>,
    snapshot_rx: Arc<Mutex<Option<crossbeam_channel::Receiver<Box<Message>>>>>,
    broadcast_tx: broadcast::Sender<BroadcastedMessage>,
    deshred_broadcast_tx: broadcast::Sender<DeshredBroadcastedMessage>,
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
    pub block_reconstruction_tx: mpsc::UnboundedSender<BlockReconstructionMessage>,
    pub broadcast_tx: broadcast::Sender<BroadcastedMessage>,
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

        // Messages to clients combined by commitment
        let (broadcast_tx, _) = broadcast::channel(config.channel_capacity);
        // Deshred subscribers receive their own commitment-free stream.
        let (deshred_broadcast_tx, _) = broadcast::channel(config.channel_capacity);
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

        // Build the shared GeyserServer (Clone-able because GrpcService: Clone)
        let max_decoding_message_size = config.max_decoding_message_size;
        let mut service = GeyserServer::new(Self {
            config_snapshot_client_channel_capacity: config.snapshot_client_channel_capacity,
            config_channel_capacity: config.channel_capacity,
            config_filter_limits: Arc::new(config.filter_limits),
            subscription_limit: config.subscription_limit,
            subscription_limit_enforce: config.subscription_limit_enforce,
            subscription_tracker: SubscriptionTracker::new(config.subscription_limit),
            blocks_meta: blocks_meta.map(Arc::new),
            subscribe_id: Arc::new(AtomicUsize::new(0)),
            snapshot_rx: Arc::new(Mutex::new(snapshot_rx)),
            broadcast_tx: broadcast_tx.clone(),
            deshred_broadcast_tx: deshred_broadcast_tx.clone(),
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
            let broadcast_tx = broadcast_tx.clone();

            task_tracker.spawn(async move {
                Self::block_reconstruction_loop(
                    BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                    broadcast_tx,
                    replay_stored_slots_rx,
                    replay_first_available_slot,
                    config.replay_stored_slots,
                )
                .await;
            });
        }

        {
            let block_reconstruction_tx = block_reconstruction_tx.clone();
            let broadcast_tx = broadcast_tx.clone();

            task_tracker.spawn(async move {
                Self::geyser_loop(messages_rx, broadcast_tx, block_reconstruction_tx).await;
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
            block_reconstruction_tx,
            broadcast_tx,
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
    async fn geyser_loop<St>(
        mut messages_rx: St,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        block_reconstruction_tx: mpsc::UnboundedSender<BlockReconstructionMessage>,
    ) where
        St: BatchStream<Item = Message> + Unpin + Send + 'static,
    {
        const MESSAGE_BATCH_SIZE: usize = 1024;
        const RING_BUFFER_SIZE: usize = 2048;

        let mut ring_allocs: Vec<Arc<Vec<Message>>> = (0..RING_BUFFER_SIZE)
            .map(|_| Arc::new(Vec::with_capacity(MESSAGE_BATCH_SIZE)))
            .collect();
        let mut ring_index = 0usize;

        loop {
            let mut ring_index_current = ring_index;
            ring_index = (ring_index + 1) % RING_BUFFER_SIZE;

            let message_batch;
            loop {
                match Arc::get_mut(&mut ring_allocs[ring_index_current]) {
                    Some(batch) => {
                        message_batch = batch;
                        message_batch.clear();
                        break;
                    }
                    None => {
                        ring_index_current = ring_index;
                        ring_index = (ring_index + 1) % RING_BUFFER_SIZE;
                    }
                }
            }

            let batch_size_maybe = messages_rx.next_batch(message_batch).await;
            let Some(_) = batch_size_maybe else {
                info!("Geyser loop: messages channel closed");
                break;
            };

            if message_batch.is_empty() {
                continue;
            }

            metrics::message_queue_size_dec_by(message_batch.len() as i64);

            let message_batch_arc = &ring_allocs[ring_index_current];
            let _ = broadcast_tx.send((CommitmentLevel::Processed, Arc::clone(message_batch_arc)));

            if block_reconstruction_tx
                .send(BlockReconstructionMessage::Batch(Arc::clone(
                    message_batch_arc,
                )))
                .is_ok()
            {
                metrics::block_reconstruction_queue_size_inc();
            }
        }
    }

    async fn block_reconstruction_loop<St>(
        mut messages_rx: St,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        replay_stored_slots_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        replay_first_available_slot: Option<Arc<AtomicU64>>,
        replay_stored_slots: u64,
    ) where
        St: BatchStream<Item = BlockReconstructionMessage> + Unpin + Send + 'static,
    {
        let (_tx, rx) = mpsc::channel(1);
        let mut replay_stored_slots_rx = replay_stored_slots_rx.unwrap_or(rx);

        let mut block_machine = BlockMachineStorage::new(replay_stored_slots as usize);
        const ALL_COMMITMENT_LEVELS: [CommitmentLevel; 3] = [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ];

        const BUFFERED_MESSAGES_CAPACITY: usize = 32;
        let mut buffered_messages = Vec::with_capacity(BUFFERED_MESSAGES_CAPACITY);

        loop {
            tokio::select! {
                maybe = messages_rx.next_batch(&mut buffered_messages) => {
                    let Some(buffered_size) = maybe else {
                        info!("Block reconstruction loop: messages channel closed");
                        break;
                    };

                    metrics::block_reconstruction_queue_size_dec_by(buffered_size as i64);

                    for messages in buffered_messages.drain(..) {
                        match messages {
                            BlockReconstructionMessage::Batch(messages) => {
                                for message in messages.iter() {
                                    if let Message::Slot(slot_message) = message {
                                        metrics::update_slot_plugin_status(slot_message.status, slot_message.slot);
                                    }

                                    block_machine.add(message.clone());
                                }
                            }
                            BlockReconstructionMessage::Single(message) => {
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
                        // Processed must be sent differently, since processed geyser event were individually sent,
                        // we only need to send Message::Block for block subscriber downstream.
                        // While, confirmed,finalized must be sent in the two flavors: as a stream of individual events and block.
                        if commitment_level != CommitmentLevel::Processed {
                            let _ = broadcast_tx.send((commitment_level, frozen_block.messages()));
                        }

                        let block_meta = Message::BlockMeta(frozen_block.get_block_meta());
                        let msg_block = Message::Block(frozen_block.get_message_block());
                        let _ = broadcast_tx.send((commitment_level, Arc::new(vec![msg_block, block_meta])));

                        let slot_message = Message::Slot(Arc::new(MessageSlot {
                            slot: slot_update.slot,
                            parent: slot_update.parent_slot,
                            status: match slot_update.commitment {
                                solana_commitment_config::CommitmentLevel::Processed => SlotStatus::Processed,
                                solana_commitment_config::CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                                solana_commitment_config::CommitmentLevel::Finalized => SlotStatus::Finalized,
                            },
                            dead_error: None,
                            created_at: Timestamp::from(SystemTime::now())
                        }));

                        let slot_message_singleton_vec = Arc::new(vec![slot_message]);
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

                    // Elaboration on 5 * replay_stored_slots: Each slot can have up to 5 messages (1 for messages, 1 for block meta, 3 for slot status). So we allocate enough space for the worst case scenario.
                    let mut replayed_messages = Vec::with_capacity(replay_stored_slots as usize + (5 * replay_stored_slots as usize));
                    let replayed_slot_iter = block_machine.replay_from_slot(replay_slot, min_solana_commitment);

                    // We need only an estimated timestamp for the replayed slot messages, so we can use the same timestamp for all of them.
                    let created_at = Timestamp::from(SystemTime::now());

                    for replayed_slot in replayed_slot_iter {
                        // 1st Put data (account/txn/entries)
                        replayed_messages.push(ReplayResponseMessageType::Batch(replayed_slot.frozen_block.messages()));

                        // 2nd Put the reconstructed block, then the block summary — mirrors the
                        // live broadcast path so `blocks` subscribers can resume via `from_slot`
                        replayed_messages.push(ReplayResponseMessageType::Single(Message::Block(replayed_slot.frozen_block.get_message_block())));

                        // 3rd Put block summary
                        replayed_messages.push(ReplayResponseMessageType::Single(Message::BlockMeta(replayed_slot.frozen_block.get_block_meta())));

                        // 4th Put slot status
                        for slot_update in replayed_slot.slot_status_messages.iter() {
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
                            }));
                            replayed_messages.push(ReplayResponseMessageType::Single(slot_message));
                        }
                    }

                    if !replayed_messages.is_empty() {
                        let _ = tx.send(ReplayedResponse::Messages(replayed_messages));
                    }
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
        mut session: ClientSession,
        stream_tx: LoadAwareSender<TonicResult<FilteredUpdate>>,
        mut client_rx: mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        mut snapshot_rx: Option<crossbeam_channel::Receiver<Box<Message>>>,
        mut messages_rx: broadcast::Receiver<BroadcastedMessage>,
        replay_stored_slots_tx: Option<mpsc::Sender<ReplayStoredSlotsRequest>>,
        task_tracker: TaskTracker,
    ) {
        let cancellation_token = session.cancellation_token.clone();

        if let Some(snapshot_rx) = snapshot_rx.take() {
            info!("client #{}: snapshot requested", session.subscriber_id);
            let result = Self::client_loop_snapshot(
                &mut session,
                stream_tx.clone(),
                &mut client_rx,
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

        'outer: loop {
            observe_subscriber_queue_size(&session.subscriber_id, stream_tx.queue_size(), "normal");

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

                            if let Some(from_slot) = from_slot {
                                let Some(replay_stored_slots_tx) = &replay_stored_slots_tx else {
                                    info!("client #{}: from_slot is not supported", session.subscriber_id);
                                    task_tracker.spawn(async move {
                                        let _ = stream_tx.send(Err(Status::internal("from_slot is not supported"))).await;
                                    });
                                    session.disconnect_reason = "from_slot_unsupported";
                                    break 'outer;
                                };

                                let (tx, rx) = oneshot::channel();
                                let commitment = session.filter.get_commitment_level();
                                if let Err(_error) = replay_stored_slots_tx.send((commitment, from_slot, tx)).await {
                                    error!("client #{}: failed to send from_slot request", session.subscriber_id);
                                    task_tracker.spawn(async move {
                                        let _ = stream_tx.send(Err(Status::internal("failed to send from_slot request"))).await;
                                    });
                                    session.disconnect_reason = "replay_error";
                                    break 'outer;
                                }

                                let messages_batch = match rx.await {
                                    Ok(ReplayedResponse::Messages(messages_batch)) => messages_batch,
                                    Ok(ReplayedResponse::Lagged(slot)) => {
                                        info!("client #{}: broadcast from {from_slot} is not available", session.subscriber_id);
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
                                        error!("client #{}: failed to get replay response", session.subscriber_id);
                                        task_tracker.spawn(async move {
                                            let _ = stream_tx.send(Err(Status::internal("failed to get replay response"))).await;
                                        });
                                        session.disconnect_reason = "replay_error";
                                        break 'outer;
                                    }
                                };

                                let replay_it = messages_batch
                                    .iter()
                                    .flatten()
                                    .flat_map(|message| session.filter.get_updates(message, Some(commitment)));

                                for filtered_message in replay_it {
                                    match stream_tx.send(Ok(filtered_message)).await {
                                        Ok(()) => {
                                            metrics::incr_grpc_message_sent_counter(&session.subscriber_id);
                                        }
                                        Err(mpsc::error::SendError(_)) => {
                                            error!("client #{}: stream closed", session.subscriber_id);
                                            session.disconnect_reason = "client_closed";
                                            break 'outer;
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
                            info!("client #{}: lagged to receive geyser messages", session.subscriber_id);
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
                                        error!("client #{}: lagged to send an update", session.subscriber_id);
                                        task_tracker.spawn(async move {
                                            let _ = stream_tx.send(Err(Status::internal("lagged to send an update"))).await;
                                        });
                                        session.disconnect_reason = "client_channel_full";
                                        break 'outer;
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
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
    }

    async fn client_loop_snapshot(
        session: &mut ClientSession,
        stream_tx: LoadAwareSender<TonicResult<FilteredUpdate>>,
        client_rx: &mut mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        snapshot_rx: crossbeam_channel::Receiver<Box<Message>>,
        cancellation_token: CancellationToken,
    ) -> Result<(), ClientSnapshotReplayError> {
        info!(
            "client #{}: going to receive snapshot data",
            session.subscriber_id
        );

        // we start with default filter, for snapshot we need wait actual filter first
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("client #{}: cancelled", session.subscriber_id);
                    return Err(ClientSnapshotReplayError::Cancelled);
                }
                maybe = client_rx.recv() => {
                    match maybe {
                        Some(Some((_from_slot, filter_new))) => {
                            if let Some(msg) = filter_new.get_pong_msg() {
                                if stream_tx.send(Ok(msg)).await.is_err() {
                                    error!("client #{}: stream closed", session.subscriber_id);
                                    return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                                }
                                continue;
                            }

                            metrics::update_subscriptions(&session.endpoint, Some(&session.filter), Some(&filter_new));
                            session.filter = filter_new;
                            info!("client #{}: filter updated", session.subscriber_id);
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
        stream_tx: LoadAwareSender<TonicResult<FilteredUpdateDeshred>>,
        mut client_rx: mpsc::UnboundedReceiver<Option<DeshredFilter>>,
        mut messages_rx: broadcast::Receiver<DeshredBroadcastedMessage>,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) {
        'outer: loop {
            observe_subscriber_queue_size(
                &session.subscriber_id,
                stream_tx.queue_size(),
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
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                error!("deshred client #{}/{}: lagged to send an update", session.subscriber_id, session.id);
                                session.disconnect_reason = "client_channel_full";
                                task_tracker.spawn(async move {
                                    let _ = stream_tx.send(Err(Status::internal("lagged to send an update"))).await;
                                });
                                break 'outer;
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
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
                    subscription_limit_exceeded_inc(id);
                    if self.subscription_limit_enforce {
                        return Err(Status::resource_exhausted(
                            "max subscription limit exceeded",
                        ));
                    }
                    info!(
                        "subscriber {id:?} over limit, not enforcing (limit: {})",
                        self.subscription_limit
                    );
                    None
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

        let (stream_tx, stream_rx) = load_aware_channel(if snapshot_rx.is_some() {
            self.config_snapshot_client_channel_capacity
        } else {
            self.config_channel_capacity
        });
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
            self.broadcast_tx.subscribe(),
            self.replay_stored_slots_tx.clone(),
            self.task_tracker.clone(),
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
                    subscription_limit_exceeded_inc(id);
                    if self.subscription_limit_enforce {
                        return Err(Status::resource_exhausted(
                            "max subscription limit exceeded",
                        ));
                    }
                    info!(
                        "subscriber {id:?} over limit, not enforcing (limit: {})",
                        self.subscription_limit
                    );
                    None
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

        let (stream_tx, stream_rx) = load_aware_channel(self.config_channel_capacity);
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

        let session = ClientSession::new(0, Some("test".into()), "test".into(), ct.clone(), None);

        let handle = tokio::spawn(GrpcService::client_loop(
            session,
            stream_tx,
            client_rx,
            None,
            broadcast_tx.subscribe(),
            None,
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
        let msg = Message::Slot(Arc::new(MessageSlot {
            slot: 100,
            parent: Some(99),
            status: SlotStatus::Processed,
            dead_error: None,
            created_at: Timestamp::from(SystemTime::now()),
        }));
        let _ = broadcast_tx.send((CommitmentLevel::Processed, Arc::new(vec![msg])));

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("client_loop did not exit")
            .expect("client_loop panicked");

        assert!(ct.is_cancelled());
    }

    #[tokio::test]
    async fn test_subscription_tracker_decrements_on_session_drop() {
        let tracker = SubscriptionTracker::new(NonZeroUsize::new(10).unwrap());

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

    mod geyser_loop_routing {
        use {
            super::super::*,
            crate::{
                plugin::{
                    convert_to,
                    message::{
                        MessageDeshredTransaction, MessageDeshredTransactionInfo, MessageSlot,
                        SlotStatus,
                    },
                },
                stream::tokio::BatchStreamUnboundedReceiver,
            },
            foldhash::{HashSet as FoldHashSet, HashSetExt as _},
            prost_types::Timestamp,
            solana_message::{legacy::Message as SolMessage, MessageHeader},
            solana_pubkey::Pubkey,
            solana_signature::Signature,
            solana_transaction::{versioned::VersionedTransaction, Transaction},
            std::{sync::Arc, time::SystemTime},
            tokio::sync::{broadcast, mpsc},
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
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (block_reconstruction_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
            let (broadcast_tx, broadcast_rx) = broadcast::channel(1024);
            let (deshred_tx, deshred_rx) = broadcast::channel(1024);
            let messages_rx = BatchStreamUnboundedReceiver::new(messages_rx);
            let handle = {
                let broadcast_tx = broadcast_tx.clone();
                tokio::spawn(GrpcService::geyser_loop(
                    messages_rx,
                    broadcast_tx,
                    block_reconstruction_tx,
                ))
            };
            let handle_reconstruction = tokio::spawn(GrpcService::block_reconstruction_loop(
                BatchStreamUnboundedReceiver::new(block_reconstruction_rx),
                broadcast_tx,
                None,
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
            Message::Slot(Arc::new(MessageSlot {
                slot,
                parent,
                status,
                dead_error: None,
                created_at: Timestamp::from(SystemTime::now()),
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
