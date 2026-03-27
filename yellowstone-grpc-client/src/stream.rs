use {
    crate::{GeyserGrpcClient, GeyserGrpcClientError, InterceptorXToken, ReconnectConfig},
    futures::stream::{BoxStream, Stream, StreamExt},
    std::{
        collections::{HashMap, HashSet, VecDeque}, future::Future, pin::Pin, sync::{Arc, Mutex}, task::{Context, Poll}, time::Duration
    },
    tonic::{
        Code, Status, Streaming, codec::CompressionEncoding, metadata::AsciiMetadataValue, transport::Endpoint
    },
    tonic_health::pb::health_client::HealthClient,
    yellowstone_grpc_proto::{
        geyser::geyser_client::GeyserClient,
        prelude::{SubscribeRequest, SubscribeUpdate, subscribe_update::UpdateOneof},
    },
};

/// Number of slots behind the last block_meta to checkpoint.
/// Conservative buffer to account for late-arriving events
/// and out-of-order delivery within a slot.
const CHECKPOINT_SLOT_BUFFER: u64 = 2;


pub struct DedupStream<S> {
    state: DedupState,
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
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.inner.poll_next_unpin(cx) {
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

#[derive(Debug, Clone)]
pub struct DedupState {
    seen_messages: HashMap<u64, HashSet<DedupKey>>, // slot -> message_key
    slot_order: VecDeque<u64>,
    slot_retention: usize,
}

impl Default for DedupState {
    fn default() -> Self {
        Self {
            seen_messages: Default::default(),
            slot_order: Default::default(),
            slot_retention: 1000, // default retention of 1000 slots
        }
    }
}

impl DedupState {
    pub fn with_slot_retention(slot_retention: usize) -> Self {
        Self {
            slot_retention: slot_retention,
            ..Default::default()
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
        while self.slot_order.len() > self.slot_retention {
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
type ConnectFuture<S> =
    Pin<Box<dyn Future<Output = Result<DedupStream<S>, GeyserGrpcClientError>> + Send + 'static>>;



#[derive(Debug, Clone)]
pub struct Backoff {
    pub initial_interval: Duration,
    pub max_interval: Duration,
    pub multiplier: f64,
    pub max_retries: u32,
    current_interval: Duration,
    attempts: u32,
}

pub(crate) enum ErrorCategory<E> {
    Retryable(E),
    Unrecoverable(E),
}

impl Backoff {
    const fn default_initial_interval() -> Duration {
        Duration::from_millis(10)
    }

    const fn default_max_interval() -> Duration {
        Duration::from_secs(30)
    }

    const fn default_multiplier() -> f64 {
        2.0
    }

    const fn default_max_retries() -> u32 {
        3
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

    fn advance(&mut self) {
        self.attempts += 1;
        self.current_interval = self
            .current_interval
            .mul_f64(self.multiplier)
            .min(self.max_interval);
    }

    pub fn retry<F, Fut, T, E>(&self, mut operation: F) -> impl Future<Output = Result<T, E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ErrorCategory<E>>>,
    {
        let mut this = self.clone();
        async move {
            this.reset();

            loop {
                match operation().await {
                    Ok(value) => {
                        this.reset();
                        return Ok(value);
                    }
                    Err(error) => {
                        if this.exhausted() {
                            match error {
                                ErrorCategory::Retryable(e) | ErrorCategory::Unrecoverable(e) => {
                                    return Err(e);
                                }
                            }
                        }
                        match error {
                            ErrorCategory::Unrecoverable(e) => return Err(e),
                            ErrorCategory::Retryable(_) => {
                                // continue to retry
                            }
                        }

                        tokio::time::sleep(this.current_interval).await;
                        this.advance();
                    }
                }
            }

        }
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

#[derive(Clone)]
struct ConsumeOnce<T> {
    inner: Arc<Mutex<Option<T>>>,
}

impl<T> ConsumeOnce<T> {
    fn new(value: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(value))),
        }
    }

    fn take(&self) -> Option<T> {
        self.inner.lock().ok()?.take()
    }
}

pub trait GrpcConnector: Clone + Send + Sync + 'static {
    type Stream: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static;
    type ConnectError: std::error::Error + Send + Sync + 'static;
    type ConnectFuture: Future<Output = Result<Self::Stream, Self::ConnectError>> + Send + 'static;
    fn connect(&self, request: SubscribeRequest, from_slot: Option<u64>) -> Self::ConnectFuture;
}

#[derive(Clone)]
pub struct TonicGrpcConnector {
    endpoint: Endpoint,
    x_token: Option<AsciiMetadataValue>,
    x_request_snapshot: bool,
    send_compressed: Option<CompressionEncoding>,
    accept_compressed: Option<CompressionEncoding>,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

impl TonicGrpcConnector {
    pub fn new(
        endpoint: Endpoint,
        config: ReconnectConfig,
        x_token: Option<AsciiMetadataValue>,
    ) -> Self {
        Self {
            endpoint,
            x_token,
            x_request_snapshot: config.x_request_snapshot,
            send_compressed: config.send_compressed,
            accept_compressed: config.accept_compressed,
            max_decoding_message_size: config.max_decoding_message_size,
            max_encoding_message_size: config.max_encoding_message_size,
        }
    }
}

impl GrpcConnector for TonicGrpcConnector {
    type Stream = Streaming<SubscribeUpdate>;
    type ConnectError = GeyserGrpcClientError;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Self::Stream, Self::ConnectError>> + Send + 'static>>;

    fn connect(&self, mut request: SubscribeRequest, from_slot: Option<u64>) -> Self::ConnectFuture {
        let endpoint = self.endpoint.clone();
        let x_token = self.x_token.clone();
        let x_request_snapshot = self.x_request_snapshot;
        let send_compressed = self.send_compressed;
        let accept_compressed = self.accept_compressed;
        let max_decoding_message_size = self.max_decoding_message_size;
        let max_encoding_message_size = self.max_encoding_message_size;
        request.from_slot = from_slot;

        Box::pin(async move {
            let channel = endpoint.connect().await.map_err(GeyserGrpcClientError::TransportError)?;

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

            let (_sink, stream) = client.subscribe_raw(Some(request)).await?;
            Ok(stream)
        })
    }
}


pub struct AutoReconnect<GrpcStream, Connector> 
{
    request: SubscribeRequest,
    last_checkpoint: Option<u64>,
    // client config for rebuilding
    config: ReconnectConfig,
    stop: bool,
    connector: Connector,
    inner_stream: Option<DedupStream<GrpcStream>>,
    pending_connecting_task: Option<ConnectFuture<GrpcStream>>,
}

impl<S, Connector> AutoReconnect<S, Connector>
    where
        S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
        Connector: GrpcConnector<Stream = S, ConnectError = GeyserGrpcClientError>,
{
    pub(crate) fn new(
        stream: DedupStream<S>,
        connector: Connector,
        request: SubscribeRequest,
        config: ReconnectConfig,
    ) -> Self {
        Self {
            request,
            last_checkpoint: None,
            config,
            inner_stream: Some(stream),
            pending_connecting_task: None,
            stop: false,
            connector,
        }
    }


    fn make_connection_future(&self, dedup_state: DedupState) -> ConnectFuture<S> {
        let backoff = self.config.backoff.clone();
        let connector = self.connector.clone();
        let request = self.request.clone();
        let from_slot = self.last_checkpoint;
        let dedup_state = ConsumeOnce::new(dedup_state);
        let fut = backoff.retry(move || {
            let connector = connector.clone();
            let request = request.clone();
            let from_slot = from_slot;
            let dedup_state = dedup_state.clone();
            async move {
                let stream = connector.connect(request, from_slot).await.map_err(|e| {
                    if is_recoverable_client_error(&e) {
                        ErrorCategory::Retryable(e)
                    } else {
                        ErrorCategory::Unrecoverable(e)
                    }
                })?;

                let dedup_state = dedup_state.take().expect("dedup_state must exists");

                Ok(DedupStream::new(stream, dedup_state))
            }
        });
        Box::pin(fut)
    }

}



fn is_recoverable_client_error(err: &GeyserGrpcClientError) -> bool {
    match err {
        GeyserGrpcClientError::TonicStatus(status) => is_recoverable_status_code(&status.code()),
        GeyserGrpcClientError::TransportError(_) => true,
        _ => false,
    }
}


const fn is_recoverable_status_code(code: &Code) -> bool {
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


impl<S, Connector> Stream for AutoReconnect<S, Connector>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
    Connector: GrpcConnector<Stream = S, ConnectError = GeyserGrpcClientError> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stop {
            return Poll::Ready(None);
        }
        let me = self.get_mut();
        while !me.stop {

            if let Some(mut stream) = me.inner_stream.take() {
                match Pin::new(&mut stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
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
                        me.inner_stream = Some(stream);
                        return Poll::Ready(Some(Ok(msg)));
                    }
                    Poll::Ready(Some(Err(status))) if is_recoverable_status_code(&status.code()) => {
                        if me.config.backoff.max_retries == 0 {
                            me.stop = true;
                            return Poll::Ready(Some(Err(status)));
                        }

                        // drop the stream, recover the dedup state and start reconnecting
                        let dedup_state = stream.state;
                        me.pending_connecting_task = Some(me.make_connection_future(dedup_state));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        me.stop = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        me.stop = true;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => {
                        me.inner_stream = Some(stream);
                        return Poll::Pending;
                    }
                }
            }

            if let Some(mut fut) = me.pending_connecting_task.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(result) => match result {
                        Ok(stream) => {
                            // Next loop iteration it will be `poll_next`ed and we can detect if it fails immediately or not.
                            me.inner_stream = Some(stream);
                        }
                        Err(error) => {
                            me.stop = true;
                            return Poll::Ready(Some(Err(Status::internal(format!(
                                "reconnect failed: {error}",
                            )))));
                        }
                    },
                    Poll::Pending => {
                        me.pending_connecting_task = Some(fut);
                        return Poll::Pending;
                    }
                }
            }
            assert!(me.inner_stream.is_some() || me.pending_connecting_task.is_some() || me.stop, "must have either an active stream, a pending connecting task, or be stopped");
        }

        Poll::Ready(None)
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
        futures::stream,
        std::{collections::VecDeque, sync::{Arc, Mutex}},
        tonic::Code,
        yellowstone_grpc_proto::prelude::{
            subscribe_update::UpdateOneof, SubscribeUpdatePing, SubscribeUpdateSlot,
        },
    };

    #[derive(Clone)]
    struct MockGrpcConnector {
        plans: Arc<Mutex<VecDeque<ConnectPlan>>>,
        from_slot_calls: Arc<Mutex<Vec<Option<u64>>>>,
    }

    struct ConnectPlan {
        expected_from_slot: Option<Option<u64>>,
        result: Result<Vec<Result<SubscribeUpdate, Status>>, GeyserGrpcClientError>,
    }

    impl MockGrpcConnector {
        fn new(plans: Vec<ConnectPlan>) -> Self {
            Self {
                plans: Arc::new(Mutex::new(plans.into())),
                from_slot_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<Option<u64>> {
            self.from_slot_calls.lock().expect("calls mutex poisoned").clone()
        }
    }

    impl GrpcConnector for MockGrpcConnector {
        type Stream = BoxStream<'static, Result<SubscribeUpdate, Status>>;
        type ConnectError = GeyserGrpcClientError;
        type ConnectFuture = Pin<
            Box<dyn Future<Output = Result<Self::Stream, Self::ConnectError>> + Send + 'static>,
        >;

        fn connect(
            &self,
            _request: SubscribeRequest,
            from_slot: Option<u64>,
        ) -> Self::ConnectFuture {
            let mut plans = self.plans.lock().expect("plans mutex poisoned");
            let plan = plans.pop_front().expect("unexpected connect call without a test plan");
            drop(plans);

            self.from_slot_calls
                .lock()
                .expect("calls mutex poisoned")
                .push(from_slot);

            if let Some(expected_from_slot) = plan.expected_from_slot {
                assert_eq!(from_slot, expected_from_slot, "unexpected reconnect from_slot");
            }

            Box::pin(async move {
                match plan.result {
                    Ok(items) => Ok(stream::iter(items).boxed()),
                    Err(err) => Err(err),
                }
            })
        }
    }

    fn reconnect_config_with_retries(max_retries: u32) -> ReconnectConfig {
        ReconnectConfig {
            backoff: Backoff::new(
                Duration::from_millis(0),
                Duration::from_millis(0),
                1.0,
                max_retries,
            ),
            x_request_snapshot: false,
            send_compressed: None,
            accept_compressed: None,
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        }
    }

    #[test]
    fn test_backoff_default() {
        let backoff = Backoff::default();
        assert_eq!(backoff.initial_interval, Duration::from_millis(10));
        assert_eq!(backoff.max_interval, Duration::from_secs(30));
        assert_eq!(backoff.multiplier, 2.0);
        assert_eq!(backoff.max_retries, 3);
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

    #[tokio::test]
    async fn test_backoff_retry_eventually_succeeds() {
        let backoff = Backoff::new(Duration::from_millis(0), Duration::from_millis(0), 2.0, 5);
        let mut calls = 0;

        let result = backoff
            .retry(|| {
                let current = calls;
                calls += 1;
                async move {
                    if current < 2 {
                        Err(ErrorCategory::Retryable("transient"))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result, Ok(42));
        assert_eq!(calls, 3);
        assert_eq!(backoff.attempts, 0);
        assert_eq!(backoff.current_interval, Duration::from_millis(0));
    }

    #[tokio::test]
    async fn test_backoff_retry_exhausted_returns_last_error() {
        let backoff = Backoff::new(Duration::from_millis(0), Duration::from_millis(0), 2.0, 2);
        let mut calls = 0;

        let result = backoff
            .retry(|| {
                calls += 1;
                async { Err::<(), _>(ErrorCategory::Retryable("still failing")) }
            })
            .await;

        assert_eq!(result, Err("still failing"));
        assert_eq!(calls, 3);
        assert_eq!(backoff.attempts, 0);
    }

    #[test]
    fn test_should_reconnect() {
        assert!(is_recoverable_status_code(&Code::Unavailable));
        assert!(is_recoverable_status_code(&Code::Internal));
        assert!(is_recoverable_status_code(&Code::Unknown));
        assert!(is_recoverable_status_code(&Code::Cancelled));
        assert!(is_recoverable_status_code(&Code::DeadlineExceeded));
        assert!(is_recoverable_status_code(&Code::ResourceExhausted));
        assert!(is_recoverable_status_code(&Code::Aborted));
        assert!(is_recoverable_status_code(&Code::DataLoss));

        assert!(!is_recoverable_status_code(&Code::InvalidArgument));
        assert!(!is_recoverable_status_code(&Code::NotFound));
        assert!(!is_recoverable_status_code(&Code::PermissionDenied));
        assert!(!is_recoverable_status_code(&Code::Unauthenticated));
        assert!(!is_recoverable_status_code(&Code::Unimplemented));
        assert!(!is_recoverable_status_code(&Code::FailedPrecondition));
        assert!(!is_recoverable_status_code(&Code::OutOfRange));
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
        let mut dedup = DedupState::with_slot_retention(3);

        dedup.record(&make_slot_msg(100, 0));
        dedup.record(&make_slot_msg(101, 0));
        dedup.record(&make_slot_msg(102, 0));
        assert!(dedup.is_duplicate(&make_slot_msg(100, 0)));

        dedup.record(&make_slot_msg(103, 0));
        assert!(!dedup.is_duplicate(&make_slot_msg(100, 0)));
        assert!(dedup.is_duplicate(&make_slot_msg(101, 0)));
    }

    #[tokio::test]
    async fn test_autoreconnect_recovers_from_recoverable_stream_error() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(None),
            result: Ok(vec![Ok(make_slot_msg(42, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            DedupStream::new(initial, DedupState::default()),
            connector.clone(),
            SubscribeRequest::default(),
            reconnect_config_with_retries(1),
        );

        let next = auto.next().await;
        let slot = next
            .expect("expected one item")
            .expect("expected successful reconnect message");
        assert_eq!(extract_slot(&slot), Some(42));
        assert_eq!(connector.calls(), vec![None]);
    }

    #[tokio::test]
    async fn test_autoreconnect_no_reconnect_when_max_retries_zero() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();
        let connector = MockGrpcConnector::new(vec![]);

        let mut auto = AutoReconnect::new(
            DedupStream::new(initial, DedupState::default()),
            connector.clone(),
            SubscribeRequest::default(),
            reconnect_config_with_retries(0),
        );

        let first = auto.next().await.expect("expected one item");
        assert!(first.is_err());
        assert_eq!(first.expect_err("expected recoverable error").code(), Code::Unavailable);
        assert_eq!(connector.calls().len(), 0);
    }

    #[tokio::test]
    async fn test_autoreconnect_passes_checkpoint_as_from_slot() {
        let initial = stream::iter(vec![Err(Status::unavailable("disconnect"))]).boxed();
        let connector = MockGrpcConnector::new(vec![ConnectPlan {
            expected_from_slot: Some(Some(77)),
            result: Ok(vec![Ok(make_slot_msg(90, 0))]),
        }]);

        let mut auto = AutoReconnect::new(
            DedupStream::new(initial, DedupState::default()),
            connector.clone(),
            SubscribeRequest::default(),
            reconnect_config_with_retries(1),
        );
        auto.last_checkpoint = Some(77);

        let msg = auto
            .next()
            .await
            .expect("expected one item")
            .expect("expected message after reconnect");
        assert_eq!(extract_slot(&msg), Some(90));
        assert_eq!(connector.calls(), vec![Some(77)]);
    }
}
