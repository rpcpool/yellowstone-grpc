use {
    futures::stream::{Stream, StreamExt},
    std::{
        collections::{BTreeMap, HashMap, HashSet, VecDeque},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Pending,
    SameBlock,
    NewBlock,
}

#[derive(Debug, Clone)]
struct SealedSlot {
    blockhash: Option<String>,
    statuses: HashSet<i32>,
    delivered: HashSet<DedupKey>,
    verdict: Verdict,
}

impl SealedSlot {
    fn sealed(blockhash: String, statuses: HashSet<i32>) -> Self {
        Self {
            blockhash: Some(blockhash),
            statuses,
            delivered: HashSet::new(),
            verdict: Verdict::Pending,
        }
    }

    fn partial() -> Self {
        Self {
            blockhash: None,
            statuses: HashSet::new(),
            delivered: HashSet::new(),
            verdict: Verdict::Pending,
        }
    }

    fn absorb(&mut self, state: SlotState) {
        self.statuses.extend(state.statuses);
        self.delivered.extend(state.keys);
    }

    fn dedup_delivered(&mut self, payload: DedupKey) -> Observation {
        if self.delivered.insert(payload) {
            Observation::New
        } else {
            Observation::Duplicate
        }
    }

    fn observe_replayed(&mut self, payload: DedupKey) -> Observation {
        match self.verdict {
            Verdict::Pending if self.blockhash.is_some() => Observation::Replay,
            _ => self.dedup_delivered(payload),
        }
    }
}

struct ReplayBuffer<T> {
    quarantine: BTreeMap<u64, Vec<T>>,
    flush_queue: VecDeque<T>,
}

impl<T> Default for ReplayBuffer<T> {
    fn default() -> Self {
        Self {
            quarantine: BTreeMap::new(),
            flush_queue: VecDeque::new(),
        }
    }
}

impl<T> ReplayBuffer<T> {
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

    fn flush_all(&mut self) {
        for (_, buffered) in std::mem::take(&mut self.quarantine) {
            self.flush_queue.extend(buffered);
        }
    }

    fn enqueue(&mut self, msg: T) {
        self.flush_queue.push_back(msg);
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
            replay: ReplayBuffer::default(),
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
                    Some((slot, key)) => {
                        let released = this.state.finish_replay_if_past(slot);
                        if released {
                            this.replay.flush_all();
                        }

                        match this.state.observe(slot, key) {
                            Observation::New if released => {
                                this.replay.enqueue(msg);
                                continue;
                            }
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
                        }
                    }
                },
                other => return other,
            }
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) enum DedupKey {
    Slot(i32),                                // status
    Account([u8; 32], Option<[u8; 64]>, u64), // pubkey, txn_signature, write_version
    Transaction([u8; 64]),                    // signature
    TransactionStatus([u8; 64]),              // signature
    Entry(u64, [u8; 32]),                     // index, hash
    BlockMeta(String),                        // blockhash
    Block(String),                            // blockhash
    DeshredTransaction([u8; 64]),             // signature
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
    replaying: bool,
    replay_high_water: u64,
}

impl Default for DedupState {
    fn default() -> Self {
        Self {
            inflight: Default::default(),
            sealed: Default::default(),
            slot_order: Default::default(),
            slot_retention: DEFAULT_SLOT_RETENTION,
            replaying: false,
            replay_high_water: 0,
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
                Some((m.slot, DedupKey::Account(pubkey, sig, info.write_version)))
            }
            UpdateOneof::Transaction(m) => {
                let info = m.transaction.as_ref()?;
                let sig = <[u8; 64]>::try_from(info.signature.as_slice()).ok()?;
                Some((m.slot, DedupKey::Transaction(sig)))
            }
            UpdateOneof::TransactionStatus(m) => {
                let sig = <[u8; 64]>::try_from(m.signature.as_slice()).ok()?;
                Some((m.slot, DedupKey::TransactionStatus(sig)))
            }
            UpdateOneof::Entry(m) => {
                let hash = <[u8; 32]>::try_from(m.hash.as_slice()).ok()?;
                Some((m.slot, DedupKey::Entry(m.index, hash)))
            }
            UpdateOneof::BlockMeta(m) => Some((m.slot, DedupKey::BlockMeta(m.blockhash.clone()))),
            UpdateOneof::Block(m) => Some((m.slot, DedupKey::Block(m.blockhash.clone()))),
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
                if let Some(sealed) = self.sealed.get_mut(&slot) {
                    let Some(known) = sealed.blockhash.replace(blockhash.clone()) else {
                        sealed.verdict = Verdict::SameBlock;
                        return Observation::New;
                    };

                    let same = known == blockhash;
                    sealed.verdict = if same {
                        Verdict::SameBlock
                    } else {
                        sealed.delivered.clear();
                        Verdict::NewBlock
                    };
                    return Observation::ReplayComplete { same };
                }

                // `slot_mut` is the only other writer of `slot_order`, and it
                // does not run when BlockMeta is the first message seen for a slot.
                if !self.inflight.contains_key(&slot) {
                    self.slot_order.push_back(slot);
                }
                let state = self.inflight.remove(&slot).unwrap_or_default();
                self.sealed
                    .insert(slot, SealedSlot::sealed(blockhash, state.statuses));
                self.prune();
                Observation::New
            }

            // Accounts, transactions, entries, blocks, etc.
            payload => {
                // Outside a replay this is late data, not a repeat. Quarantining
                // it would drop it for good: no second BlockMeta is coming.
                if self.replaying {
                    if let Some(sealed) = self.sealed.get_mut(&slot) {
                        return sealed.observe_replayed(payload);
                    }
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

    /// Promotes inflight slots to sealed and opens the replay window.
    pub(crate) fn prepare_for_replay(&mut self) {
        for (slot, state) in self.inflight.drain() {
            self.sealed
                .entry(slot)
                .or_insert_with(SealedSlot::partial)
                .absorb(state);
        }

        for sealed in self.sealed.values_mut() {
            sealed.verdict = Verdict::Pending;
        }

        self.replaying = true;
        self.replay_high_water = self
            .sealed
            .keys()
            .copied()
            .max()
            .unwrap_or(self.replay_high_water);
    }

    /// Closes the replay window. Returns true on the transition, so the caller
    /// can release whatever is still quarantined.
    pub(crate) fn finish_replay_if_past(&mut self, slot: u64) -> bool {
        if !self.replaying || slot <= self.replay_high_water {
            return false;
        }
        self.replaying = false;
        for sealed in self.sealed.values_mut() {
            sealed.delivered = HashSet::new();
        }
        true
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

    fn make_tx_msg(slot: u64, index: u64, signature_byte: u8) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Transaction(
                yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction {
                    transaction: Some(
                        yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo {
                            signature: vec![signature_byte; 64],
                            is_vote: false,
                            transaction: None,
                            meta: None,
                            index,
                        },
                    ),
                    slot,
                },
            )),
            created_at: None,
        }
    }

    fn make_account_msg_with_pubkey(slot: u64, pubkey_byte: u8) -> SubscribeUpdate {
        let mut msg = make_account_msg(slot);
        if let Some(UpdateOneof::Account(account)) = msg.update_oneof.as_mut() {
            if let Some(info) = account.account.as_mut() {
                info.pubkey = vec![pubkey_byte; 32];
            }
        }
        msg
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

        dedup.prepare_for_replay();

        // a replayed account for a sealed slot should be quarantined
        let account_msg = make_account_msg(300);
        assert!(matches!(
            observe(&mut dedup, &account_msg),
            Observation::Replay
        ));
    }

    #[test]
    fn test_late_payload_outside_replay_window_is_not_quarantined() {
        let mut dedup = DedupState::default();

        observe(&mut dedup, &make_slot_msg(300, 0));
        observe(&mut dedup, &make_block_meta_msg(300));

        assert!(matches!(
            observe(&mut dedup, &make_account_msg(300)),
            Observation::New
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
    fn test_partial_slot_replay_dedups_on_delivered_keys() {
        let mut dedup = DedupState::default();

        observe(&mut dedup, &make_slot_msg(600, 0));
        observe(&mut dedup, &make_account_msg(600));

        // simulate reconnect: promote all inflight to sealed-without-blockhash
        dedup.prepare_for_replay();

        assert!(matches!(
            observe(&mut dedup, &make_account_msg(600)),
            Observation::Duplicate
        ));

        let unseen = make_account_msg_with_pubkey(600, 7);
        assert!(matches!(observe(&mut dedup, &unseen), Observation::New));
    }

    #[test]
    fn test_equivocated_partial_slot_delivers_new_block_content() {
        let mut dedup = DedupState::default();

        observe(&mut dedup, &make_slot_msg(900, 0));
        observe(&mut dedup, &make_tx_msg(900, 0, 0xAA));

        dedup.prepare_for_replay();

        assert!(matches!(
            observe(&mut dedup, &make_tx_msg(900, 0, 0xBB)),
            Observation::New
        ));

        assert!(matches!(
            observe(&mut dedup, &make_tx_msg(900, 0, 0xAA)),
            Observation::Duplicate
        ));
    }

    #[tokio::test]
    async fn test_released_backlog_is_emitted_before_the_newer_message() {
        let mut state = DedupState::default();
        observe(&mut state, &make_slot_msg(200, 0));
        observe(&mut state, &make_block_meta_msg_with_hash(200, "h200"));
        state.prepare_for_replay();

        let messages = vec![Ok(make_account_msg(200)), Ok(make_slot_msg(201, 0))];

        let inner = stream::iter(messages).boxed();
        let got: Vec<_> = DedupStream::new(inner, state)
            .filter_map(|r| async move { r.ok() })
            .collect()
            .await;

        assert_eq!(got.len(), 2);
        assert!(
            matches!(got[0].update_oneof.as_ref(), Some(UpdateOneof::Account(_))),
            "expected the released backlog first, got {:?}",
            got[0].update_oneof
        );
        assert!(matches!(
            got[1].update_oneof.as_ref(),
            Some(UpdateOneof::Slot(_))
        ));
    }

    #[test]
    fn test_partial_slot_block_meta_is_new_not_a_verdict() {
        let mut dedup = DedupState::default();

        observe(&mut dedup, &make_slot_msg(700, 0));
        dedup.prepare_for_replay();

        assert!(matches!(
            observe(&mut dedup, &make_block_meta_msg_with_hash(700, "any_hash")),
            Observation::New
        ));
    }

    #[test]
    fn test_post_block_meta_replay_is_deduped_not_blanket_dropped() {
        let mut dedup = DedupState::default();

        observe(&mut dedup, &make_slot_msg(800, 0));
        observe(&mut dedup, &make_block_meta_msg_with_hash(800, "h800"));
        observe(&mut dedup, &make_account_msg(800));

        dedup.prepare_for_replay();

        assert!(matches!(
            observe(&mut dedup, &make_block_meta_msg_with_hash(800, "h800")),
            Observation::ReplayComplete { same: true }
        ));

        assert!(matches!(
            observe(&mut dedup, &make_account_msg(800)),
            Observation::Duplicate
        ));
        assert!(matches!(
            observe(&mut dedup, &make_account_msg_with_pubkey(800, 9)),
            Observation::New
        ));
    }

    #[test]
    fn test_block_meta_only_slots_are_pruned() {
        let mut dedup = DedupState::with_slot_retention(3);

        for slot in 1..=10 {
            observe(&mut dedup, &make_block_meta_msg(slot));
        }

        assert!(
            dedup.sealed.len() <= 3,
            "sealed grew past the retention window: {} entries",
            dedup.sealed.len()
        );
        assert!(
            !dedup.sealed.contains_key(&1),
            "slot 1 should have been pruned"
        );
    }

    #[tokio::test]
    async fn test_late_payload_is_forwarded_in_steady_state() {
        let messages = vec![
            Ok(make_slot_msg(100, 0)),
            Ok(make_block_meta_msg(100)),
            Ok(make_account_msg(100)),
            Ok(make_slot_msg(101, 0)),
        ];

        let inner = stream::iter(messages).boxed();
        let got: Vec<_> = DedupStream::new(inner, DedupState::default())
            .filter_map(|r| async move { r.ok() })
            .collect()
            .await;

        assert_eq!(got.len(), 4, "late account update was dropped");
        assert!(matches!(
            got[2].update_oneof.as_ref(),
            Some(UpdateOneof::Account(_))
        ));
    }

    #[tokio::test]
    async fn test_replay_window_closes_and_releases_quarantine() {
        let mut state = DedupState::default();
        observe(&mut state, &make_slot_msg(200, 0));
        observe(&mut state, &make_block_meta_msg(200));
        state.prepare_for_replay();

        let messages = vec![Ok(make_account_msg(200)), Ok(make_slot_msg(201, 0))];

        let inner = stream::iter(messages).boxed();
        let got: Vec<_> = DedupStream::new(inner, state)
            .filter_map(|r| async move { r.ok() })
            .collect()
            .await;

        assert_eq!(
            got.len(),
            2,
            "quarantined message was never released after the replay window closed"
        );
    }

    #[test]
    fn test_prepare_for_replay_keeps_known_blockhash() {
        let mut dedup = DedupState::default();

        observe(&mut dedup, &make_block_meta_msg_with_hash(300, "abc"));
        observe(&mut dedup, &make_account_msg(300));

        dedup.prepare_for_replay();

        assert!(matches!(
            observe(&mut dedup, &make_block_meta_msg_with_hash(300, "abc")),
            Observation::ReplayComplete { same: true }
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
