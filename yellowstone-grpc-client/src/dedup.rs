use {
    futures::stream::{Stream, StreamExt},
    std::{
        collections::{HashMap, HashSet, VecDeque},
        task::Poll,
    },
    tonic::Status,
    yellowstone_grpc_proto::{
        geyser::SubscribeUpdateDeshred,
        prelude::{
            SubscribeUpdate, subscribe_update::UpdateOneof, subscribe_update_deshred::UpdateOneof as DeshredUpdateOneof
        },
    },
};

pub(crate) const DEFAULT_SLOT_RETENTION: usize = 250;

/// Wrapper stream that filters out duplicate subscribe updates.
pub struct DedupStream<S> {
    pub(crate) state: DedupState,
    inner: S,
}

impl<S> DedupStream<S> {
    pub const fn new(inner: S, state: DedupState) -> Self {
        Self { state, inner }
    }
}

impl<S, T> Stream for DedupStream<S>
where
    T: Dedupable,
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;

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
pub(crate) enum DedupKey {
    Slot(i32),                           // status
    Account([u8; 32], Option<[u8; 64]>), // pubkey, txn_signature
    Transaction(u64),                    // index
    TransactionStatus(u64),              // index
    Entry(u64),                          // index
    BlockMeta,
    Block,
    DeshredTransaction([u8; 64]), // signature
}

#[derive(Debug, Clone)]
/// Tracks seen messages per slot so we can filter duplicates during replay after reconnect.
pub struct DedupState {
    // slot being processed -> set of message keys seen for that slot
    inflight_slots: HashMap<u64, HashSet<DedupKey>>, // slot -> message_key
    // slot being processed -> set of visited slot statuses
    inflight_slot_statuses: HashMap<u64, HashSet<i32>>, // slot -> slot status
    // slot already finalized by BlockMeta -> set of slot statuses seen for that slot
    slot_processed: HashMap<u64, HashSet<i32>>, // slot -> slot status
    slot_order: VecDeque<u64>,
    slot_retention: usize,
}

impl Default for DedupState {
    fn default() -> Self {
        Self {
            inflight_slots: Default::default(),
            inflight_slot_statuses: Default::default(),
            slot_processed: Default::default(),
            slot_order: Default::default(),
            slot_retention: DEFAULT_SLOT_RETENTION,
        }
    }
}

pub(crate) trait Dedupable {
    fn extract_key(&self) -> Option<(u64, DedupKey)>;
}

impl Dedupable for SubscribeUpdate {
    /// Extracts a comparable `(slot, key)` pair from a subscribe update.
    fn extract_key(&self) -> Option<(u64, DedupKey)> {
        let oneof = self.update_oneof.as_ref()?;
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
}

impl Dedupable for SubscribeUpdateDeshred {
    /// Extracts a comparable `(slot, key)` pair from a subscribe update.
    fn extract_key(&self) -> Option<(u64, DedupKey)> {
        let oneof = self.update_oneof.as_ref()?;
        match oneof {
            DeshredUpdateOneof::DeshredTransaction(m) => {
                let info = m.transaction.as_ref()?;
                let sig = <[u8; 64]>::try_from(info.signature.as_slice()).ok()?;
                Some((m.slot, DedupKey::DeshredTransaction(sig)))
            }
            DeshredUpdateOneof::Ping(_) | DeshredUpdateOneof::Pong(_) => None,
        }
    }
}

impl DedupState {
    /// Creates dedup state with a custom retained slot window size.
    pub fn with_slot_retention(slot_retention: usize) -> Self {
        Self {
            slot_retention,
            ..Default::default()
        }
    }

    /// Returns true if this update was previously recorded for its slot.
    pub(crate) fn is_duplicate(&self, msg: &impl Dedupable) -> bool {
        if let Some((slot, key)) = msg.extract_key() {
            if let DedupKey::Slot(status) = key {
                if self
                    .slot_processed
                    .get(&slot)
                    .is_some_and(|statuses| statuses.contains(&status))
                {
                    return true;
                }

                return self
                    .inflight_slot_statuses
                    .get(&slot)
                    .is_some_and(|statuses| statuses.contains(&status));
            }

            if let Some(keys) = self.inflight_slots.get(&slot) {
                return keys.contains(&key);
            }
        }
        false
    }

    /// Records an update key and prunes old slots when retention is exceeded.
    pub(crate) fn record(&mut self, msg: &impl Dedupable) {
        if let Some((slot, key)) = msg.extract_key() {
            self.track_slot(slot);

            match key {
                DedupKey::Slot(status) => {
                    if let Some(processed) = self.slot_processed.get_mut(&slot) {
                        processed.insert(status);
                    } else {
                        self.inflight_slot_statuses
                            .entry(slot)
                            .or_default()
                            .insert(status);
                    }
                }
                DedupKey::BlockMeta => {
                    self.mark_slot_processed(slot);
                }
                key => {
                    self.inflight_slots.entry(slot).or_default().insert(key);
                }
            }

            self.prune();
        }
    }

    fn track_slot(&mut self, slot: u64) {
        if self.inflight_slots.contains_key(&slot)
            || self.inflight_slot_statuses.contains_key(&slot)
            || self.slot_processed.contains_key(&slot)
        {
            return;
        }
        self.slot_order.push_back(slot);
    }

    fn mark_slot_processed(&mut self, slot: u64) {
        self.inflight_slots.remove(&slot);
        let statuses = self.inflight_slot_statuses.remove(&slot).unwrap_or_default();

        if !statuses.is_empty() {
            self.slot_processed.entry(slot).or_default().extend(statuses);
        }
    }

    /// Keeps seen_messages bounded to slot_retention window.
    pub fn prune(&mut self) {
        while self.slot_order.len() > self.slot_retention {
            if let Some(slot) = self.slot_order.pop_front() {
                self.inflight_slots.remove(&slot);
                self.inflight_slot_statuses.remove(&slot);
                self.slot_processed.remove(&slot);
            } else {
                break;
            }
        }
        // HashMap doesn't release capacity on remove; reclaim it after pruning.
        self.inflight_slots.shrink_to_fit();
        self.inflight_slot_statuses.shrink_to_fit();
        self.slot_processed.shrink_to_fit();
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.inflight_slots.clear();
        self.inflight_slot_statuses.clear();
        self.slot_processed.clear();
        self.slot_order.clear();
    }
}
