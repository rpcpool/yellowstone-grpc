use {
    futures::stream::{Stream, StreamExt},
    std::{
        collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
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
    keys: HashSet<DedupKey>,
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
        assert!(slot_retention > 0, "slot_retention must be > 0");
        Self {
            slot_retention,
            ..Default::default()
        }
    }

    fn slot_mut(&mut self, slot: u64) -> &mut SlotState {
        match self.inflight.entry(slot) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                if !self.sealed.contains_key(&slot) {
                    self.slot_order.push_back(slot);
                }
                e.insert(SlotState::default())
            }
        }
    }

    /// Classify a message for the dedup/quarantine layer
    pub(crate) fn observe(&mut self, slot: u64, key: DedupKey) -> Observation {
        match key {
            DedupKey::Slot(status) => {
                // CreatedBank wipes prior state to recover from a rollback.
                // During replay, CreatedBank for a sealed slot is a replay
                // artifact, not a genuine rollback.
                if status == CREATED_BANK_STATUS {
                    if self.sealed.contains_key(&slot) {
                        return Observation::Duplicate;
                    }
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
                        keys: HashSet::new(),
                    },
                );
                self.prune();
                Observation::New
            }

            // Accounts, transactions, entries, blocks, etc.
            payload => {
                // Sealed slot: check if we already forwarded this event before
                // the disconnect. If so, filter it. Otherwise quarantine it
                // until the replayed BlockMeta arrives with the verdict.
                if let Some(sealed) = self.sealed.get(&slot) {
                    if sealed.keys.contains(&payload) {
                        return Observation::Duplicate;
                    }
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
    pub(crate) fn prune(&mut self) {
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
                    keys: state.keys,
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        futures::{stream, StreamExt},
        yellowstone_grpc_proto::prelude::{
            subscribe_update::UpdateOneof, SubscribeUpdatePing, SubscribeUpdateSlot,
        },
    };

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

    fn make_block_meta_msg(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::BlockMeta(
                yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta {
                    slot,
                    blockhash: "test_hash".to_string(),
                    rewards: None,
                    block_time: None,
                    block_height: None,
                    parent_slot: slot.saturating_sub(1),
                    parent_blockhash: String::new(),
                    executed_transaction_count: 0,
                    entries_count: 0,
                },
            )),
            created_at: None,
        }
    }

    fn make_block_meta_msg_with_hash(slot: u64, hash: &str) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::BlockMeta(
                yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta {
                    slot,
                    blockhash: hash.to_string(),
                    rewards: None,
                    block_time: None,
                    block_height: None,
                    parent_slot: slot.saturating_sub(1),
                    parent_blockhash: String::new(),
                    executed_transaction_count: 0,
                    entries_count: 0,
                },
            )),
            created_at: None,
        }
    }

    fn make_account_msg(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Account(
                yellowstone_grpc_proto::geyser::SubscribeUpdateAccount {
                    account: Some(yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo {
                        pubkey: vec![1; 32],
                        lamports: 100,
                        owner: vec![0; 32],
                        executable: false,
                        rent_epoch: 0,
                        data: vec![].into(),
                        write_version: 1,
                        txn_signature: Some(vec![0; 64]),
                    }),
                    slot,
                    is_startup: false,
                },
            )),
            created_at: None,
        }
    }

    fn observe(dedup: &mut DedupState, msg: &SubscribeUpdate) -> Observation {
        let (slot, key) = msg.extract_key().expect("test msg has a key");
        dedup.observe(slot, key)
    }

    #[test]
    fn test_dedup_record_and_detect() {
        let mut dedup = DedupState::default();
        let msg = make_slot_msg(100, 0);

        assert!(matches!(observe(&mut dedup, &msg), Observation::New));
        assert!(matches!(observe(&mut dedup, &msg), Observation::Duplicate));
    }

    #[test]
    fn test_dedup_different_slots_not_duplicate() {
        let mut dedup = DedupState::default();

        assert!(matches!(
            observe(&mut dedup, &make_slot_msg(100, 0)),
            Observation::New
        ));
        assert!(matches!(
            observe(&mut dedup, &make_slot_msg(101, 0)),
            Observation::New
        ));
    }

    #[test]
    fn test_dedup_ping_ignored() {
        let ping = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        };
        assert!(ping.extract_key().is_none());
    }

    #[test]
    fn test_dedup_same_slot_different_status() {
        let mut dedup = DedupState::default();

        assert!(matches!(
            observe(&mut dedup, &make_slot_msg(100, 0)),
            Observation::New
        ));
        assert!(matches!(
            observe(&mut dedup, &make_slot_msg(100, 1)),
            Observation::New
        ));
    }

    #[test]
    fn test_dedup_prune() {
        let mut dedup = DedupState::with_slot_retention(3);

        observe(&mut dedup, &make_slot_msg(100, 0));
        observe(&mut dedup, &make_slot_msg(101, 0));
        observe(&mut dedup, &make_slot_msg(102, 0));
        observe(&mut dedup, &make_slot_msg(103, 0));

        assert!(matches!(
            observe(&mut dedup, &make_slot_msg(101, 0)),
            Observation::Duplicate
        ));
        assert!(matches!(
            observe(&mut dedup, &make_slot_msg(100, 0)),
            Observation::New
        ));
    }

    #[test]
    fn test_dedup_slot_sealed_on_blockmeta() {
        let mut dedup = DedupState::default();

        observe(&mut dedup, &make_slot_msg(200, 0));
        observe(&mut dedup, &make_block_meta_msg(200));

        assert!(matches!(
            observe(&mut dedup, &make_slot_msg(200, 0)),
            Observation::Duplicate
        ));
        assert!(matches!(
            observe(&mut dedup, &make_slot_msg(200, 1)),
            Observation::New
        ));
    }

    #[test]
    fn test_custom_slot_retention_honored() {
        let mut dedup = DedupState::with_slot_retention(5);

        for slot in 1..=10 {
            observe(&mut dedup, &make_slot_msg(slot, 0));
        }

        for slot in 6..=10 {
            assert!(
                matches!(
                    observe(&mut dedup, &make_slot_msg(slot, 0)),
                    Observation::Duplicate
                ),
                "slot {slot} should remain"
            );
        }

        for slot in 1..=5 {
            assert!(
                matches!(
                    observe(&mut dedup, &make_slot_msg(slot, 0)),
                    Observation::New
                ),
                "slot {slot} should be pruned"
            );
        }
    }

    #[tokio::test]
    async fn test_dedup_stream_standalone() {
        let messages = vec![
            Ok(make_slot_msg(100, 0)),
            Ok(make_slot_msg(100, 0)),
            Ok(make_slot_msg(101, 0)),
        ];

        let inner = stream::iter(messages).boxed();
        let mut dedup = DedupStream::new(inner, DedupState::default());

        let msg1 = dedup
            .next()
            .await
            .expect("expected item")
            .expect("expected ok");
        assert_eq!(crate::reconnect::extract_slot(&msg1), Some(100));

        let msg2 = dedup
            .next()
            .await
            .expect("expected item")
            .expect("expected ok");
        assert_eq!(crate::reconnect::extract_slot(&msg2), Some(101));

        assert!(dedup.next().await.is_none());
    }

    #[test]
    fn test_sealed_slot_payload_returns_replay() {
        let mut dedup = DedupState::default();

        // build and seal slot 300
        observe(&mut dedup, &make_slot_msg(300, 0));
        observe(&mut dedup, &make_block_meta_msg(300));

        // a replayed account for a sealed slot should be quarantined
        let account_msg = make_account_msg(300);
        assert!(matches!(
            observe(&mut dedup, &account_msg),
            Observation::Replay
        ));
    }

    #[test]
    fn test_replay_complete_same_blockhash_drops_buffer() {
        let mut dedup = DedupState::default();

        // seal slot 400 with blockhash "abc"
        observe(&mut dedup, &make_slot_msg(400, 0));
        observe(&mut dedup, &make_block_meta_msg_with_hash(400, "abc"));

        // replayed BlockMeta with same hash: same block, discard
        assert!(matches!(
            observe(&mut dedup, &make_block_meta_msg_with_hash(400, "abc")),
            Observation::ReplayComplete { same: true }
        ));
    }

    #[test]
    fn test_replay_complete_different_blockhash_flushes() {
        let mut dedup = DedupState::default();

        // seal slot 500 with blockhash "block_a"
        observe(&mut dedup, &make_slot_msg(500, 0));
        observe(&mut dedup, &make_block_meta_msg_with_hash(500, "block_a"));

        // replayed BlockMeta with different hash: block changed, flush
        assert!(matches!(
            observe(&mut dedup, &make_block_meta_msg_with_hash(500, "block_b")),
            Observation::ReplayComplete { same: false }
        ));
    }

    #[test]
    fn test_prepare_for_replay_promotes_partial_to_sealed() {
        let mut dedup = DedupState::default();

        // slot 600 is inflight (no BlockMeta yet)
        observe(&mut dedup, &make_slot_msg(600, 0));

        // simulate reconnect: promote all inflight to sealed-without-blockhash
        dedup.prepare_for_replay();

        // replayed payload for the now-sealed slot should be quarantined
        let account_msg = make_account_msg(600);
        assert!(matches!(
            observe(&mut dedup, &account_msg),
            Observation::Replay
        ));
    }

    #[test]
    fn test_partial_slot_always_flushes_on_replay_complete() {
        let mut dedup = DedupState::default();

        // slot 700 is inflight
        observe(&mut dedup, &make_slot_msg(700, 0));

        // promote to sealed without blockhash
        dedup.prepare_for_replay();

        // replayed BlockMeta: no stored hash to compare, always flush
        assert!(matches!(
            observe(&mut dedup, &make_block_meta_msg_with_hash(700, "any_hash")),
            Observation::ReplayComplete { same: false }
        ));
    }

    #[test]
    fn test_created_bank_clears_sealed_slot() {
        let mut dedup = DedupState::default();

        // seal slot 800
        observe(&mut dedup, &make_slot_msg(800, 0));
        observe(&mut dedup, &make_block_meta_msg(800));

        // CreatedBank wipes the slot (rollback)
        let created_bank_status = SlotStatus::SlotCreatedBank as i32;
        observe(&mut dedup, &make_slot_msg(800, created_bank_status));

        // slot is fresh now: a new status is New, not Duplicate
        assert!(matches!(
            observe(&mut dedup, &make_slot_msg(800, 0)),
            Observation::New
        ));
    }
}
