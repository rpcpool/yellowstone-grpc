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
            subscribe_update_deshred::UpdateOneof as DeshredUpdateOneof, SlotStatus,
            SubscribeUpdate,
        },
    },
};

pub(crate) const DEFAULT_SLOT_RETENTION: usize = 250;

const CREATED_BANK_STATUS: i32 = SlotStatus::SlotCreatedBank as i32;

pub(crate) enum Observation {
    New,
    Duplicate,
    Replay,
    ReplayComplete { same: bool },
}

#[derive(Debug, Clone)]
struct SealedSlot {
    blockhash: Option<String>,
    statuses: HashSet<i32>,
}

struct ReplayBuffer<T> {
    quarantine: HashMap<u64, Vec<T>>,
    flush_queue: VecDeque<T>,
}

impl<T> ReplayBuffer<T> {
    fn new() -> Self {
        Self {
            quarantine: HashMap::new(),
            flush_queue: VecDeque::new(),
        }
    }

    fn hold(&mut self, slot: u64, msg: T) {
        self.quarantine.entry(slot).or_default().push(msg);
    }

    fn flush(&mut self, slot: u64, blockmeta: T) {
        if let Some(buffered) = self.quarantine.remove(&slot) {
            self.flush_queue.extend(buffered);
        }
        self.flush_queue.push_back(blockmeta);
    }

    fn discard(&mut self, slot: u64) {
        self.quarantine.remove(&slot);
    }

    fn drain_next(&mut self) -> Option<T> {
        self.flush_queue.pop_front()
    }
}

/// Wrapper stream that filters out duplicate subscribe updates.
pub struct DedupStream<S, T = SubscribeUpdate> {
    pub(crate) state: DedupState,
    inner: S,
    replay: ReplayBuffer<T>,
}

impl<S, T> DedupStream<S, T> {
    pub fn new(inner: S, state: DedupState) -> Self {
        Self {
            state,
            inner,
            replay: ReplayBuffer::new(),
        }
    }
}

impl<S, T> Stream for DedupStream<S, T>
where
    T: Dedupable + Unpin,
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(msg) = this.replay.drain_next() {
                return Poll::Ready(Some(Ok(msg)));
            }

            match this.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(msg))) => match msg.extract_key() {
                    None => return Poll::Ready(Some(Ok(msg))),
                    Some((slot, key)) => match this.state.observe(slot, key) {
                        Observation::New => return Poll::Ready(Some(Ok(msg))),
                        Observation::Duplicate => continue,
                        Observation::Replay => {
                            this.replay.hold(slot, msg);
                            continue;
                        }
                        Observation::ReplayComplete { same: true } => {
                            this.replay.discard(slot);
                            continue;
                        }
                        Observation::ReplayComplete { same: false } => {
                            this.replay.flush(slot, msg);
                            continue;
                        }
                    },
                },
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
    BlockMeta(String),                   // blockhash,
    Block(u64),
    DeshredTransaction([u8; 64]), // signature
}

#[derive(Debug, Default, Clone)]
struct SlotState {
    keys: HashSet<DedupKey>, // inflight_slots[slot]
    statuses: HashSet<i32>,  // inflight_slot_statuses[slot] + slot_processed[slot]
}

#[derive(Debug, Clone)]
/// Tracks seen messages per slot so we can filter duplicates during replay after reconnect.
pub struct DedupState {
    inflight: HashMap<u64, SlotState>,
    sealed: HashMap<u64, SealedSlot>,
    slot_order: VecDeque<u64>,
    slot_retention: usize,
}

impl Default for DedupState {
    fn default() -> Self {
        Self {
            inflight: Default::default(),
            sealed: Default::default(),
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
            UpdateOneof::BlockMeta(m) => Some((m.slot, DedupKey::BlockMeta(m.blockhash.clone()))),
            UpdateOneof::Block(m) => Some((m.slot, DedupKey::Block(m.slot))),
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
            DeshredUpdateOneof::Slot(m) => Some((m.slot, DedupKey::Slot(m.status))),
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

    fn slot_mut(&mut self, slot: u64) -> &mut SlotState {
        if !self.inflight.contains_key(&slot) && !self.sealed.contains_key(&slot) {
            self.slot_order.push_back(slot);
        }
        self.inflight.entry(slot).or_default()
    }

    /// Classify a message for the dedup/quarantine layer
    pub(crate) fn observe(&mut self, slot: u64, key: DedupKey) -> Observation {
        match key {
            DedupKey::Slot(status) => {
                // CreatedBank means a bank was just created for this slot. Wipe any
                // prior state unconditionally: on first creation this is a near no-op,
                // on a repeated creation it recovers from a rollback.
                //
                // Rollback detection depends on receiving CreatedBank. The server only
                // emits interslot statuses (CreatedBank, etc.) to filters with
                // interslot_updates=true (see FilterSlots::get_updates server-side).
                // Without it this wipe is dormant. That is by design: we do not inject
                // the interslot flag; the user opts in by accepting the extra traffic.
                if status == CREATED_BANK_STATUS {
                    self.clear_slot(slot);
                }

                // Sealed slots keep a compressed status set for post-seal dedup
                // (Confirmed / Finalized arrive after BlockMeta).
                if let Some(s) = self.sealed.get_mut(&slot) {
                    if !s.statuses.insert(status) {
                        return Observation::Duplicate;
                    }
                    return Observation::New;
                }

                let state = self.slot_mut(slot);
                if !state.statuses.insert(status) {
                    return Observation::Duplicate;
                }
                self.prune();
                Observation::New
            }

            DedupKey::BlockMeta(blockhash) => {
                // Replayed BlockMeta for a sealed slot: this is the verdict.
                // Compare the stored blockhash to decide whether the block changed
                // across the reconnect. If no blockhash is stored (slot was partial
                // when we disconnected), treat as changed and flush always.
                if let Some(sealed) = self.sealed.get_mut(&slot) {
                    let same = sealed.blockhash.as_ref() == Some(&blockhash);
                    sealed.blockhash = Some(blockhash);
                    return Observation::ReplayComplete { same };
                }

                // First BlockMeta for this slot: seal it. The SlotState is destroyed;
                // only the blockhash and statuses survive in the compressed SealedSlot.
                let state = self.inflight.remove(&slot).unwrap_or_default();
                self.sealed.insert(
                    slot,
                    SealedSlot {
                        blockhash: Some(blockhash),
                        statuses: state.statuses,
                    },
                );
                self.prune();
                Observation::New
            }

            // Accounts, transactions, entries, blocks, etc.
            payload => {
                // Sealed slot: hold for quarantine. The verdict comes when the
                // replayed BlockMeta arrives; until then we buffer without deduping.
                if self.sealed.contains_key(&slot) {
                    return Observation::Replay;
                }

                let state = self.slot_mut(slot);
                if state.keys.contains(&payload) {
                    return Observation::Duplicate;
                }
                state.keys.insert(payload);
                self.prune();
                Observation::New
            }
        }
    }

    /// Keeps tracked slots bounded to the retention window.
    pub fn prune(&mut self) {
        while self.slot_order.len() > self.slot_retention {
            match self.slot_order.pop_front() {
                Some(slot) => {
                    self.inflight.remove(&slot);
                    self.sealed.remove(&slot);
                }
                None => break,
            }
        }
    }

    /// Promote all inflight slots to sealed-without-blockhash before replay.
    /// Replayed content for these slots will be quarantined and flushed at
    /// BlockMeta (no stored blockhash to compare, so always flush).
    pub(crate) fn prepare_for_replay(&mut self) {
        for (slot, state) in self.inflight.drain() {
            self.sealed.insert(
                slot,
                SealedSlot {
                    blockhash: None,
                    statuses: state.statuses,
                },
            );
        }
    }

    pub(crate) fn clear_slot(&mut self, slot: u64) {
        self.inflight.remove(&slot);
        self.sealed.remove(&slot);
        self.slot_order.retain(|s| *s != slot);
    }
}
