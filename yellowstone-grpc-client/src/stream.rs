use {
    crate::{GeyserGrpcClient, GeyserGrpcClientError, InterceptorXToken, ReconnectConfig},
    futures::stream::{BoxStream, Stream, StreamExt},
    pin_project::pin_project,
    std::{
        collections::{HashMap, HashSet, VecDeque},
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    },
    tonic::{
        codec::CompressionEncoding, metadata::AsciiMetadataValue, transport::Endpoint, Code, Status,
    },
    tonic_health::pb::health_client::HealthClient,
    yellowstone_grpc_proto::{
        geyser::geyser_client::GeyserClient,
        prelude::{subscribe_update::UpdateOneof, SubscribeRequest, SubscribeUpdate},
    },
};

/// Number of slots behind the last block_meta to checkpoint.
/// Conservative buffer to account for late-arriving events
/// and out-of-order delivery within a slot.
const CHECKPOINT_SLOT_BUFFER: u64 = 2;

#[derive(Debug, Clone)]
pub struct DedupConfig {
    pub max_slot: usize,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self { max_slot: 100 }
    }
}

#[pin_project]
pub struct DedupStream<S> {
    state: DedupState,
    #[pin]
    inner: S,
}

impl<S> DedupStream<S> {
    pub const fn new(inner: S, state: DedupState) -> Self {
        Self { state, inner }
    }

    pub fn into_parts(self) -> (DedupState, S) {
        (self.state, self.inner)
    }
}

impl<S> Stream for DedupStream<S>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>>,
{
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    if this.state.is_duplicate(&msg) {
                        continue;
                    }

                    this.state.record(&msg);
                    return Poll::Ready(Some(Ok(msg)));
                }
                other => return other,
            }
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum DedupKey {
    Slot(i32),                           // status
    Account([u8; 32], Option<[u8; 64]>), // pubkey, txn_signature
    Transaction(u64),                    // index
    TransactionStatus(u64),              // index
    Entry(u64),                          // index
    BlockMeta,
    Block,
}

#[derive(Debug, Clone, Default)]
pub struct DedupState {
    seen_messages: HashMap<u64, HashSet<DedupKey>>, // slot -> message_key
    slot_order: VecDeque<u64>,
    config: DedupConfig,
}

impl DedupState {
    pub fn new(config: DedupConfig) -> Self {
        Self {
            seen_messages: HashMap::new(),
            slot_order: VecDeque::new(),
            config,
        }
    }

    fn extract_key(msg: &SubscribeUpdate) -> Option<(u64, DedupKey)> {
        let oneof = msg.update_oneof.as_ref()?;
        match oneof {
            UpdateOneof::Slot(m) => Some((m.slot, DedupKey::Slot(m.status))),
            UpdateOneof::Account(m) => {
                let info = m.account.as_ref()?;
                let pubkey = <[u8; 32]>::try_from(info.pubkey.as_slice()).ok()?;

                let sig = info
                    .txn_signature
                    .as_ref()
                    .and_then(|s| <[u8; 64]>::try_from(s.as_slice()).ok());
                Some((m.slot, DedupKey::Account(pubkey, sig)))
            }
            UpdateOneof::Transaction(m) => {
                let info = m.transaction.as_ref()?;
                Some((m.slot, DedupKey::Transaction(info.index)))
            }
            UpdateOneof::TransactionStatus(m) => {
                Some((m.slot, DedupKey::TransactionStatus(m.index)))
            }
            UpdateOneof::Entry(m) => Some((m.slot, DedupKey::Entry(m.index))),
            UpdateOneof::BlockMeta(m) => Some((m.slot, DedupKey::BlockMeta)),
            UpdateOneof::Block(m) => Some((m.slot, DedupKey::Block)),
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => None,
        }
    }

    pub fn is_duplicate(&self, msg: &SubscribeUpdate) -> bool {
        if let Some((slot, key)) = Self::extract_key(msg) {
            if let Some(keys) = self.seen_messages.get(&slot) {
                return keys.contains(&key);
            }
        }
        false
    }

    pub fn record(&mut self, msg: &SubscribeUpdate) {
        if let Some((slot, key)) = Self::extract_key(msg) {
            if self.seen_messages.entry(slot).or_default().insert(key) {
                // new slot we haven't tracked yet
                if !self.slot_order.contains(&slot) {
                    self.slot_order.push_back(slot);
                    self.prune();
                }
            }
        }
    }

    pub fn prune(&mut self) {
        while self.slot_order.len() > self.config.max_slot {
            if let Some(slot) = self.slot_order.pop_front() {
                self.seen_messages.remove(&slot);
            }
        }
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.seen_messages.clear();
        self.slot_order.clear();
    }
}

/// A boxed future that resolves to a new raw stream or an error.
type ConnectFuture =
    Pin<Box<dyn Future<Output = Result<GeyserStream, GeyserGrpcClientError>> + Send>>;

/// A boxed sleep timer for backoff between reconnect attempts.
type SleepFuture = Pin<Box<tokio::time::Sleep>>;

type GeyserStream = BoxStream<'static, Result<SubscribeUpdate, Status>>;

pub enum ConnectionState {
    Streaming(DedupStream<GeyserStream>),
    /// Connection died — waiting for backoff timer before retrying.
    /// Carries DedupState so it survives to the next connection.
    Waiting(DedupState, SleepFuture),
    /// Backoff done — async connect/subscribe in progress.
    /// Carries DedupState to pass into the new DedupStream on success.
    Connecting(DedupState, ConnectFuture),
}

#[derive(Debug, Clone)]
pub struct Backoff {
    pub initial_interval: Duration,
    pub max_interval: Duration,
    pub multiplier: f64,
    pub max_retries: u32,
    current_interval: Duration,
    attempts: u32,
}

impl Backoff {
    const fn default_initial_interval() -> Duration {
        Duration::from_millis(500)
    }

    const fn default_max_interval() -> Duration {
        Duration::from_secs(30)
    }

    const fn default_multiplier() -> f64 {
        2.0
    }

    const fn default_max_retries() -> u32 {
        10
    }

    pub const fn new(
        initial_interval: Duration,
        max_interval: Duration,
        multiplier: f64,
        max_retries: u32,
    ) -> Self {
        Self {
            initial_interval,
            max_interval,
            multiplier,
            max_retries,
            current_interval: initial_interval,
            attempts: 0,
        }
    }

    pub const fn reset(&mut self) {
        self.current_interval = self.initial_interval;
        self.attempts = 0;
    }

    pub const fn exhausted(&self) -> bool {
        self.attempts >= self.max_retries
    }

    pub fn advance(&mut self) {
        self.attempts += 1;
        self.current_interval = self
            .current_interval
            .mul_f64(self.multiplier)
            .min(self.max_interval);
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(
            Self::default_initial_interval(),
            Self::default_max_interval(),
            Self::default_multiplier(),
            Self::default_max_retries(),
        )
    }
}

pub struct AutoReconnect {
    state: ConnectionState,
    backoff: Backoff,
    endpoint: Endpoint,
    request: SubscribeRequest,
    last_checkpoint: Option<u64>,
    // client config for rebuilding
    x_token: Option<AsciiMetadataValue>,
    x_request_snapshot: bool,
    send_compressed: Option<CompressionEncoding>,
    accept_compressed: Option<CompressionEncoding>,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

impl AutoReconnect {
    pub fn new(
        stream: GeyserStream,
        request: SubscribeRequest,
        dedup_config: DedupConfig,
        config: ReconnectConfig,
    ) -> Self {
        Self {
            state: ConnectionState::Streaming(DedupStream::new(
                stream,
                DedupState::new(dedup_config),
            )),
            backoff: config.backoff,
            endpoint: config.endpoint,
            request,
            last_checkpoint: None,
            x_token: config.x_token,
            x_request_snapshot: config.x_request_snapshot,
            send_compressed: config.send_compressed,
            accept_compressed: config.accept_compressed,
            max_decoding_message_size: config.max_decoding_message_size,
            max_encoding_message_size: config.max_encoding_message_size,
        }
    }

    pub fn make_connection_future(&self) -> ConnectFuture {
        let endpoint = self.endpoint.clone(); // clone before the async block
        let x_token = self.x_token.clone();
        let x_request_snapshot = self.x_request_snapshot;
        let send_compressed = self.send_compressed;
        let accept_compressed = self.accept_compressed;
        let max_decoding_message_size = self.max_decoding_message_size;
        let max_encoding_message_size = self.max_encoding_message_size;
        let mut request = self.request.clone();
        request.from_slot = self.last_checkpoint;

        Box::pin(async move {
            let channel = endpoint
                .connect()
                .await
                .map_err(|e| GeyserGrpcClientError::TonicStatus(Status::internal(e.to_string())))?;

            let interceptor = InterceptorXToken {
                x_token,
                x_request_snapshot,
            };

            let mut geyser = GeyserClient::with_interceptor(channel.clone(), interceptor.clone());
            if let Some(encoding) = send_compressed {
                geyser = geyser.send_compressed(encoding);
            }
            if let Some(encoding) = accept_compressed {
                geyser = geyser.accept_compressed(encoding);
            }
            if let Some(limit) = max_decoding_message_size {
                geyser = geyser.max_decoding_message_size(limit);
            }
            if let Some(limit) = max_encoding_message_size {
                geyser = geyser.max_encoding_message_size(limit);
            }

            let mut client =
                GeyserGrpcClient::new(HealthClient::with_interceptor(channel, interceptor), geyser);

            let (_sink, stream) = client.subscribe_with_request(Some(request)).await?;

            Ok(stream.boxed())
        })
    }
}

const fn should_reconnect(code: Code) -> bool {
    matches!(
        code,
        Code::Cancelled
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Internal
            | Code::Unavailable
            | Code::DataLoss
    )
}

impl Stream for AutoReconnect {
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();

        let state = std::mem::replace(
            &mut me.state,
            ConnectionState::Waiting(
                DedupState::default(),
                Box::pin(tokio::time::sleep(Duration::from_secs(0))),
            ),
        );

        match state {
            ConnectionState::Streaming(mut stream) => match Pin::new(&mut stream).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    me.backoff.reset();
                    if let Some(UpdateOneof::BlockMeta(_)) = msg.update_oneof.as_ref() {
                        if let Some(slot) = extract_slot(&msg) {
                            // checkpoint a few slots behind the last fully built slot.
                            // block_meta can arrive late and events within a slot
                            // arrive in random order — replaying from a couple slots
                            // back guarantees we don't miss data. dedup handles
                            // the duplicates this creates.
                            me.last_checkpoint = Some(slot.saturating_sub(CHECKPOINT_SLOT_BUFFER));
                        }
                    }
                    me.state = ConnectionState::Streaming(stream);
                    Poll::Ready(Some(Ok(msg)))
                }
                Poll::Ready(Some(Err(status))) if should_reconnect(status.code()) => {
                    log::warn!("stream error, reconnecting: {status}");
                    let (dedup_state, _) = stream.into_parts();
                    me.backoff.advance();
                    me.state = ConnectionState::Waiting(
                        dedup_state,
                        Box::pin(tokio::time::sleep(me.backoff.current_interval)),
                    );
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(Some(Err(status))) => Poll::Ready(Some(Err(status))),
                Poll::Ready(None) => {
                    log::info!("stream ended, reconnecting");
                    let (dedup_state, _) = stream.into_parts();
                    me.backoff.advance();
                    me.state = ConnectionState::Waiting(
                        dedup_state,
                        Box::pin(tokio::time::sleep(me.backoff.current_interval)),
                    );
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => {
                    me.state = ConnectionState::Streaming(stream);
                    Poll::Pending
                }
            },
            ConnectionState::Waiting(dedup_state, mut sleep) => {
                match Pin::new(&mut sleep).poll(cx) {
                    Poll::Ready(()) => {
                        let fut = me.make_connection_future();
                        me.state = ConnectionState::Connecting(dedup_state, fut);
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Pending => {
                        me.state = ConnectionState::Waiting(dedup_state, sleep);
                        Poll::Pending
                    }
                }
            }
            ConnectionState::Connecting(dedup_state, mut fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(stream)) => {
                    me.backoff.reset();
                    me.state = ConnectionState::Streaming(DedupStream::new(stream, dedup_state));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(Err(error)) => {
                    log::error!("connect failed: {error}");
                    if me.backoff.exhausted() {
                        return Poll::Ready(Some(Err(Status::internal(
                            "max reconnect retries exhausted",
                        ))));
                    }
                    me.backoff.advance();
                    me.state = ConnectionState::Waiting(
                        dedup_state,
                        Box::pin(tokio::time::sleep(me.backoff.current_interval)),
                    );
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => {
                    me.state = ConnectionState::Connecting(dedup_state, fut);
                    Poll::Pending
                }
            },
        }
    }
}

pub(crate) fn extract_slot(msg: &SubscribeUpdate) -> Option<u64> {
    match msg.update_oneof.as_ref()? {
        UpdateOneof::Account(m) => Some(m.slot),
        UpdateOneof::Slot(m) => Some(m.slot),
        UpdateOneof::Transaction(m) => Some(m.slot),
        UpdateOneof::Block(m) => Some(m.slot),
        UpdateOneof::BlockMeta(m) => Some(m.slot),
        UpdateOneof::Entry(m) => Some(m.slot),
        UpdateOneof::TransactionStatus(m) => Some(m.slot),
        UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        tonic::Code,
        yellowstone_grpc_proto::prelude::{
            subscribe_update::UpdateOneof, SubscribeUpdatePing, SubscribeUpdateSlot,
        },
    };

    #[test]
    fn test_backoff_default() {
        let backoff = Backoff::default();
        assert_eq!(backoff.initial_interval, Duration::from_millis(500));
        assert_eq!(backoff.max_interval, Duration::from_secs(30));
        assert_eq!(backoff.multiplier, 2.0);
        assert_eq!(backoff.max_retries, 10);
        assert!(!backoff.exhausted());
    }

    #[test]
    fn test_backoff_exhaustion() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(1), 2.0, 3);
        assert!(!backoff.exhausted());
        backoff.attempts = 3;
        assert!(backoff.exhausted());
    }

    #[test]
    fn test_backoff_reset() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(1), 2.0, 3);
        backoff.attempts = 2;
        backoff.current_interval = Duration::from_secs(1);
        backoff.reset();
        assert_eq!(backoff.attempts, 0);
        assert_eq!(backoff.current_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_backoff_advance() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(1), 2.0, 5);
        assert_eq!(backoff.current_interval, Duration::from_millis(100));

        backoff.advance();
        assert_eq!(backoff.attempts, 1);
        assert_eq!(backoff.current_interval, Duration::from_millis(200));

        backoff.advance();
        assert_eq!(backoff.attempts, 2);
        assert_eq!(backoff.current_interval, Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_advance_caps_at_max() {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(1), 2.0, 10);
        backoff.advance(); // 1s
        backoff.advance(); // would be 2s, capped at 1s
        assert_eq!(backoff.current_interval, Duration::from_secs(1));
    }

    #[test]
    fn test_should_reconnect() {
        assert!(should_reconnect(Code::Unavailable));
        assert!(should_reconnect(Code::Internal));
        assert!(should_reconnect(Code::Unknown));
        assert!(should_reconnect(Code::Cancelled));
        assert!(should_reconnect(Code::DeadlineExceeded));
        assert!(should_reconnect(Code::ResourceExhausted));
        assert!(should_reconnect(Code::Aborted));
        assert!(should_reconnect(Code::DataLoss));

        assert!(!should_reconnect(Code::InvalidArgument));
        assert!(!should_reconnect(Code::NotFound));
        assert!(!should_reconnect(Code::PermissionDenied));
        assert!(!should_reconnect(Code::Unauthenticated));
        assert!(!should_reconnect(Code::Unimplemented));
        assert!(!should_reconnect(Code::FailedPrecondition));
        assert!(!should_reconnect(Code::OutOfRange));
    }

    #[test]
    fn test_extract_slot_from_slot_update() {
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 42,
                parent: None,
                status: 0,
                dead_error: None,
            })),
            created_at: None,
        };
        assert_eq!(extract_slot(&msg), Some(42));
    }

    #[test]
    fn test_extract_slot_from_ping() {
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        };
        assert_eq!(extract_slot(&msg), None);
    }

    #[test]
    fn test_extract_slot_none() {
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: None,
            created_at: None,
        };
        assert_eq!(extract_slot(&msg), None);
    }

    fn make_slot_msg(slot: u64, status: i32) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent: None,
                status,
                dead_error: None,
            })),
            created_at: None,
        }
    }

    #[test]
    fn test_dedup_record_and_detect() {
        let mut dedup = DedupState::default();
        let msg = make_slot_msg(100, 0);

        assert!(!dedup.is_duplicate(&msg));
        dedup.record(&msg);
        assert!(dedup.is_duplicate(&msg));
    }

    #[test]
    fn test_dedup_different_slots_not_duplicate() {
        let mut dedup = DedupState::default();
        let msg1 = make_slot_msg(100, 0);
        let msg2 = make_slot_msg(101, 0);

        dedup.record(&msg1);
        assert!(!dedup.is_duplicate(&msg2));
    }

    #[test]
    fn test_dedup_ping_ignored() {
        let mut dedup = DedupState::default();
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        };

        dedup.record(&msg);
        assert!(!dedup.is_duplicate(&msg));
    }

    #[test]
    fn test_dedup_clear() {
        let mut dedup = DedupState::default();
        let msg = make_slot_msg(100, 0);

        dedup.record(&msg);
        assert!(dedup.is_duplicate(&msg));
        dedup.clear();
        assert!(!dedup.is_duplicate(&msg));
    }

    #[test]
    fn test_dedup_same_slot_different_status() {
        let mut dedup = DedupState::default();
        let msg1 = make_slot_msg(100, 0);
        let msg2 = make_slot_msg(100, 1);

        dedup.record(&msg1);
        assert!(!dedup.is_duplicate(&msg2));
    }

    #[test]
    fn test_dedup_prune() {
        let mut dedup = DedupState::new(DedupConfig { max_slot: 3 });

        dedup.record(&make_slot_msg(100, 0));
        dedup.record(&make_slot_msg(101, 0));
        dedup.record(&make_slot_msg(102, 0));
        assert!(dedup.is_duplicate(&make_slot_msg(100, 0)));

        dedup.record(&make_slot_msg(103, 0));
        assert!(!dedup.is_duplicate(&make_slot_msg(100, 0)));
        assert!(dedup.is_duplicate(&make_slot_msg(101, 0)));
    }
}
