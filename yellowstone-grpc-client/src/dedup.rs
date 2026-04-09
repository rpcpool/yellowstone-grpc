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
            subscribe_update::UpdateOneof,
            subscribe_update_deshred::UpdateOneof as DeshredUpdateOneof, SubscribeUpdate,
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
    seen_messages: HashMap<u64, HashSet<DedupKey>>, // slot -> message_key
    slot_order: VecDeque<u64>,
    slot_retention: usize,
}

impl Default for DedupState {
    fn default() -> Self {
        Self {
            seen_messages: Default::default(),
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
            if let Some(keys) = self.seen_messages.get(&slot) {
                return keys.contains(&key);
            }
        }
        false
    }

    /// Records an update key and prunes old slots when retention is exceeded.
    pub(crate) fn record(&mut self, msg: &impl Dedupable) {
        if let Some((slot, key)) = msg.extract_key() {
            let is_new_slot = !self.seen_messages.contains_key(&slot);
            self.seen_messages.entry(slot).or_default().insert(key);
            if is_new_slot {
                self.slot_order.push_back(slot);
                self.prune();
            }
        }
    }

    /// Keeps seen_messages bounded to slot_retention window.
    pub fn prune(&mut self) {
        while self.slot_order.len() > self.slot_retention {
            if let Some(slot) = self.slot_order.pop_front() {
                self.seen_messages.remove(&slot);
            } else {
                break;
            }
        }
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.seen_messages.clear();
        self.slot_order.clear();
    }
}
