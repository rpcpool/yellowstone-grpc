use {
    crate::{GeyserGrpcBuilder, GeyserGrpcClientError},
    futures::stream::{try_unfold, BoxStream, Stream, StreamExt},
    std::{
        collections::{BTreeMap, HashSet},
        sync::Arc,
        time::Duration,
    },
    tonic::{Code, Status},
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeUpdate,
    },
};

#[derive(Debug, Default)]
struct DedupTracker {
    /// Per-slot set of seen message keys
    seen: BTreeMap<u64, HashSet<DedupKey>>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum DedupKey {
    Slot(i32),                  // status
    Account(Vec<u8>, u64),      // pubkey, write_version
    Transaction(Vec<u8>),       // signature
    TransactionStatus(Vec<u8>), // signature
    Entry(u64),                 // index
    BlockMeta,
    Block,
}

impl DedupTracker {
    fn extract_key(msg: &SubscribeUpdate) -> Option<(u64, DedupKey)> {
        let oneof = msg.update_oneof.as_ref()?;
        match oneof {
            UpdateOneof::Slot(m) => Some((m.slot, DedupKey::Slot(m.status))),
            UpdateOneof::Account(m) => {
                let info = m.account.as_ref()?;
                Some((
                    m.slot,
                    DedupKey::Account(info.pubkey.clone(), info.write_version),
                ))
            }
            UpdateOneof::Transaction(m) => {
                let info = m.transaction.as_ref()?;
                Some((m.slot, DedupKey::Transaction(info.signature.clone())))
            }
            UpdateOneof::TransactionStatus(m) => {
                Some((m.slot, DedupKey::TransactionStatus(m.signature.clone())))
            }
            UpdateOneof::Entry(m) => Some((m.slot, DedupKey::Entry(m.index))),
            UpdateOneof::BlockMeta(m) => Some((m.slot, DedupKey::BlockMeta)),
            UpdateOneof::Block(m) => Some((m.slot, DedupKey::Block)),
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => None,
        }
    }

    /// Returns true if this message was already seen
    fn is_duplicate(&self, msg: &SubscribeUpdate) -> bool {
        if let Some((slot, key)) = Self::extract_key(msg) {
            if let Some(keys) = self.seen.get(&slot) {
                return keys.contains(&key);
            }
        }
        false
    }

    /// Record a message as seen
    fn record(&mut self, msg: &SubscribeUpdate) {
        if let Some((slot, key)) = Self::extract_key(msg) {
            self.seen.entry(slot).or_default().insert(key);
        }
    }

    /// Clear all tracked state (called on reconnect to stop dedup
    /// once we've passed the replay window)
    fn clear(&mut self) {
        self.seen.clear();
    }
}

#[allow(clippy::type_complexity)]
pub struct ReconnectCallback(Arc<dyn Fn(u32, &Status) + Send + Sync>); // open to suggestions against Arc @lvboudre and @leafaar

impl Clone for ReconnectCallback {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl ReconnectCallback {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(u32, &Status) + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    pub fn call(&self, attempt: u32, status: &Status) {
        (self.0)(attempt, status);
    }
}

impl std::fmt::Debug for ReconnectCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReconnectCallback(..)")
    }
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

    pub async fn sleep(&mut self) {
        self.attempts += 1;
        tokio::time::sleep(self.current_interval).await;
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

struct ReconnectState {
    builder: GeyserGrpcBuilder,
    request: SubscribeRequest,
    backoff: Backoff,
    on_reconnect: Option<ReconnectCallback>,
    inner: Option<BoxStream<'static, Result<SubscribeUpdate, Status>>>,
    last_seen_slot: Option<u64>,
    dedup: DedupTracker,
    needs_dedup: bool,
}

pub fn subscribe_with_reconnect(
    mut builder: GeyserGrpcBuilder,
    request: SubscribeRequest,
) -> impl Stream<Item = Result<SubscribeUpdate, Status>> {
    let backoff = builder.reconnect.take().unwrap_or_default();
    let on_reconnect = builder.on_reconnect.take();

    try_unfold(
        ReconnectState {
            builder,
            request,
            backoff,
            on_reconnect,
            inner: None,
            last_seen_slot: None,
            dedup: DedupTracker::default(),
            needs_dedup: false,
        },
        |mut state| async move {
            loop {
                // If we have a live stream, poll it
                if let Some(stream) = state.inner.as_mut() {
                    match stream.next().await {
                        Some(Ok(msg)) => {
                            state.backoff.reset();

                            // Skip duplicates after reconnect replay
                            if state.needs_dedup {
                                if state.dedup.is_duplicate(&msg) {
                                    if let Some(slot) = extract_slot(&msg) {
                                        state.last_seen_slot = Some(slot);
                                    }
                                    continue;
                                }
                                // First non-duplicate means we've passed the replay window
                                state.needs_dedup = false;
                                state.dedup.clear();
                            }

                            if let Some(slot) = extract_slot(&msg) {
                                state.last_seen_slot = Some(slot);
                            }

                            state.dedup.record(&msg);
                            return Ok(Some((msg, state)));
                        }
                        Some(Err(status)) => {
                            if !should_reconnect(status.code()) {
                                // Fatal error — surface to consumer
                                return Err(status);
                            }
                            log::warn!("stream error, reconnecting: {status}");
                            if let Some(cb) = &state.on_reconnect {
                                cb.call(state.backoff.attempts, &status);
                            }
                        }
                        None => {
                            log::info!("stream ended, reconnecting");
                        }
                    }
                    // Stream broke — null it, backoff, retry
                    state.inner = None;
                    if state.backoff.exhausted() {
                        return Err(Status::internal("max reconnect retries exhausted"));
                    }
                    state.backoff.sleep().await;
                } else {
                    // No stream — try to connect
                    match state.builder.connect().await {
                        Ok(mut client) => {
                            let mut request = state.request.clone();
                            request.from_slot = state.last_seen_slot;
                            match client
                                .subscribe_with_request(Some(state.request.clone()))
                                .await
                            {
                                Ok((_sink, stream)) => {
                                    state.inner = Some(stream.boxed());
                                    state.backoff.reset();
                                    if state.last_seen_slot.is_some() {
                                        state.needs_dedup = true;
                                    }
                                }
                                Err(error) => {
                                    let status = match error {
                                        GeyserGrpcClientError::TonicStatus(s) => s,
                                        other => Status::internal(other.to_string()),
                                    };
                                    log::error!("subscribe failed: {status}");
                                    if let Some(cb) = &state.on_reconnect {
                                        cb.call(state.backoff.attempts, &status);
                                    }
                                    if state.backoff.exhausted() {
                                        return Err(status);
                                    }
                                    state.backoff.sleep().await;
                                }
                            }
                        }
                        Err(error) => {
                            let status = Status::internal(error.to_string());
                            log::error!("connect failed: {status}");
                            if let Some(cb) = &state.on_reconnect {
                                cb.call(state.backoff.attempts, &status);
                            }
                            if state.backoff.exhausted() {
                                return Err(status);
                            }
                            state.backoff.sleep().await;
                        }
                    }
                }
            }
        },
    )
}

fn extract_slot(msg: &SubscribeUpdate) -> Option<u64> {
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
    fn test_should_reconnect() {
        // Should reconnect
        assert!(should_reconnect(Code::Unavailable));
        assert!(should_reconnect(Code::Internal));
        assert!(should_reconnect(Code::Unknown));
        assert!(should_reconnect(Code::Cancelled));
        assert!(should_reconnect(Code::DeadlineExceeded));
        assert!(should_reconnect(Code::ResourceExhausted));
        assert!(should_reconnect(Code::Aborted));
        assert!(should_reconnect(Code::DataLoss));

        // Should NOT reconnect
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

    #[test]
    fn test_dedup_tracker_record_and_detect() {
        let mut dedup = DedupTracker::default();
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 100,
                parent: None,
                status: 0,
                dead_error: None,
            })),
            created_at: None,
        };

        assert!(!dedup.is_duplicate(&msg));
        dedup.record(&msg);
        assert!(dedup.is_duplicate(&msg));
    }

    #[test]
    fn test_dedup_tracker_different_slots_not_duplicate() {
        let mut dedup = DedupTracker::default();
        let msg1 = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 100,
                parent: None,
                status: 0,
                dead_error: None,
            })),
            created_at: None,
        };
        let msg2 = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 101,
                parent: None,
                status: 0,
                dead_error: None,
            })),
            created_at: None,
        };

        dedup.record(&msg1);
        assert!(!dedup.is_duplicate(&msg2));
    }

    #[test]
    fn test_dedup_tracker_ping_ignored() {
        let mut dedup = DedupTracker::default();
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        };

        // Ping has no key, so it's never a duplicate
        dedup.record(&msg);
        assert!(!dedup.is_duplicate(&msg));
    }

    #[test]
    fn test_dedup_tracker_clear() {
        let mut dedup = DedupTracker::default();
        let msg = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 100,
                parent: None,
                status: 0,
                dead_error: None,
            })),
            created_at: None,
        };

        dedup.record(&msg);
        assert!(dedup.is_duplicate(&msg));
        dedup.clear();
        assert!(!dedup.is_duplicate(&msg));
    }

    #[test]
    fn test_dedup_tracker_same_slot_different_status() {
        let mut dedup = DedupTracker::default();
        let msg1 = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 100,
                parent: None,
                status: 0,
                dead_error: None,
            })),
            created_at: None,
        };
        let msg2 = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 100,
                parent: None,
                status: 1,
                dead_error: None,
            })),
            created_at: None,
        };

        dedup.record(&msg1);
        assert!(!dedup.is_duplicate(&msg2));
    }
}
