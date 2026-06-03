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

// SLOT_CREATED_BANK = 5
const CREATED_BANK_STATUS: i32 = SlotStatus::SlotCreatedBank as i32;

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
                Poll::Ready(Some(Ok(msg))) => match msg.extract_key() {
                    None => return Poll::Ready(Some(Ok(msg))),
                    Some((slot, key)) => match this.state.observe(slot, key) {
                        Ok(true) => continue,                           // duplicate: skip
                        Ok(false) => return Poll::Ready(Some(Ok(msg))), // new: forward
                        // divergence: FailedPrecondition is not in is_recoverable_status_code,
                        // so AutoReconnect terminates instead of reconnecting.
                        Err(status) => return Poll::Ready(Some(Err(status))),
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
    Entry(u64, [u8; 32]),                // index, hash
    BlockMeta,
    Block(u64),
    DeshredTransaction([u8; 64]), // signature
}

#[derive(Debug, Default, Clone)]
struct SlotState {
    keys: HashSet<DedupKey>,         // inflight_slots[slot]
    statuses: HashSet<i32>,          // inflight_slot_statuses[slot] + slot_processed[slot]
    entries: HashMap<u64, [u8; 32]>, // entry_hashes[slot]; index -> hash
    sealed: bool,                    // "is this slot a key in slot_processed?"
}

#[derive(Debug, Clone)]
/// Tracks seen messages per slot so we can filter duplicates during replay after reconnect.
pub struct DedupState {
    slots: HashMap<u64, SlotState>,
    slot_order: VecDeque<u64>,
    slot_retention: usize,
}

impl Default for DedupState {
    fn default() -> Self {
        Self {
            slots: Default::default(),
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
            UpdateOneof::Entry(m) => {
                let hash = <[u8; 32]>::try_from(m.hash.as_slice()).ok()?;
                Some((m.slot, DedupKey::Entry(m.index, hash)))
            }
            UpdateOneof::BlockMeta(m) => Some((m.slot, DedupKey::BlockMeta)),
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
        if !self.slots.contains_key(&slot) {
            self.slot_order.push_back(slot);
        }
        self.slots.entry(slot).or_default()
    }

    fn has_created_bank(&self, slot: u64) -> bool {
        self.slots
            .get(&slot)
            .is_some_and(|s| s.statuses.contains(&CREATED_BANK_STATUS))
    }

    /// Ok(true) = duplicate (skip), Ok(false) = new (forward), Err = divergence (terminate).
    pub(crate) fn observe(&mut self, slot: u64, key: DedupKey) -> Result<bool, Status> {
        match key {
            DedupKey::Slot(status) => {
                // Rollback invalidation depends on receiving CreatedBank. The server only emits
                // interslot statuses (CreatedBank etc.) to filters with interslot_updates=true
                // (see FilterSlots::get_updates). So this guard is dormant unless the user's slot filter sets interslot_updates.

                // second CreatedBank => rollback: wipe the whole slot, then record fresh
                if status == CREATED_BANK_STATUS && self.has_created_bank(slot) {
                    self.clear_slot(slot);
                } else if self
                    .slots
                    .get(&slot)
                    .is_some_and(|s| s.statuses.contains(&status))
                {
                    return Ok(true);
                }
                self.slot_mut(slot).statuses.insert(status);
            }
            DedupKey::Entry(index, hash) => {
                let state = self.slot_mut(slot);
                if state.sealed {
                    return Ok(true); // sealed slot: not tracked (partial slots only)
                }
                match state.entries.get(&index) {
                    Some(&seen) if seen == hash => return Ok(true),
                    Some(&seen) => {
                        return Err(Status::failed_precondition(format!(
                            "equivocation: slot {slot} entry {index} hash mismatch: \
                            expected {seen:?}, got {hash:?}"
                        )));
                    }
                    None => {
                        state.entries.insert(index, hash);
                    }
                }
            }
            DedupKey::BlockMeta => {
                let state = self.slot_mut(slot);
                if state.sealed {
                    return Ok(true); // replayed BlockMeta
                }
                // seal: drop per-slot detail, keep statuses + marker
                state.keys.clear();
                state.entries.clear();
                state.sealed = true;
            }
            payload => {
                let state = self.slot_mut(slot);
                if state.sealed || state.keys.contains(&payload) {
                    return Ok(true);
                }
                state.keys.insert(payload);
            }
        }
        self.prune();
        Ok(false)
    }

    /// Keeps tracked slots bounded to the retention window.
    pub fn prune(&mut self) {
        while self.slot_order.len() > self.slot_retention {
            match self.slot_order.pop_front() {
                Some(slot) => {
                    self.slots.remove(&slot);
                }
                None => break,
            }
        }
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.slots.clear();
        self.slot_order.clear();
    }
}

impl DedupState {
    /// Drop all dedup + entry state for a single slot (rollback recovery).
    pub(crate) fn clear_slot(&mut self, slot: u64) {
        self.slots.remove(&slot);
        self.slot_order.retain(|s| *s != slot);
    }
}
