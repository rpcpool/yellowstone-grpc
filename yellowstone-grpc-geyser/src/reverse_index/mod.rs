#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
//! Delivers updates straight into the queues of the clients whose index keys find them. Each
//! commitment level has one owner, its only sender: it sends every batch through its
//! [`OwnerIndexes`] and takes each [`IndexChange`] between two sends. AGENTS.md describes the
//! join and leave protocol and the ordering it keeps.

pub mod bank_thread;
pub mod claimed_key_table;
pub mod client_indexing;
pub mod key_claim_index;
pub mod processed_owner;
pub mod relay;

#[allow(clippy::disallowed_types, reason = "set-once disconnect reason")]
use {
    crate::{
        grpc::{BroadcastedMessage, SubscribeSender, SubscriberChannels},
        plugin::{
            filter::{Filter, MatchScratch, MatchedUpdate},
            message::{CommitmentLevel, Message},
        },
        subscribe::{
            entry::{IndexedUpdateRef, SubscribeQueueEntry},
            group::{indexed_updates_per_member_bound, takes_reference_path, GroupEntries},
        },
        telemetry::stream_counters::StreamCounters,
        util::stream::TrySendError,
    },
    foldhash::HashMap,
    hashbrown::HashTable,
    key_claim_index::{KeyClaimIndex, MAX_SLOT_COUNT},
    solana_pubkey::Pubkey,
    std::{
        collections::VecDeque,
        hash::BuildHasher,
        num::NonZeroUsize,
        panic::{catch_unwind, AssertUnwindSafe},
        pin::Pin,
        sync::{Arc, LazyLock, OnceLock},
        time::Duration,
    },
    tokio::{
        sync::{
            broadcast::{self, error::TryRecvError},
            mpsc, oneshot,
        },
        time::{Instant, Sleep},
    },
    tokio_util::sync::CancellationToken,
    tonic::Status,
};
pub use {
    bank_thread::{BankThread, BankThreadHandle},
    client_indexing::ClientIndexing,
    processed_owner::ProcessedOwner,
    relay::{let_woken_task_run_first, RelayedItem},
};

/// The most keys an indexed filter has, besides one [`IndexKey::UngatedMemcmp`]; one with more stays
/// on the broadcast. It bounds one subscriber's index work, and a join's claims made at once.
pub const MAX_INDEX_KEYS: usize = 8192;

/// The account data offsets a 32-byte memcmp is indexed at. A token account holds its mint at
/// offset 0 and its owner at offset 32.
pub const INDEXED_MEMCMP_OFFSETS: [usize; 2] = [0, 32];

/// The most index changes an owner takes in per pass.
pub const MAX_INDEX_CHANGES_PER_PASS: usize = 64;

/// The most index keys one index step claims or releases.
pub const INDEX_KEYS_PER_STEP: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// The most preparations one index slice answers, so the hand-overs that follow come spread out.
const MAX_ANSWERS_PER_INDEX_SLICE: usize = 8;

/// How many slice intervals index work waits before a slice runs at the top of a busy pass.
const INDEX_SLICE_OVERDUE_INTERVALS: u32 = 10;

/// How much index work an owner does, and how often.
///
/// Index work claims and releases index keys for index changes. The owner runs it in slices,
/// with [`OwnerIndexes::run_index_slice`], and each slice runs index steps of at most
/// `keys_per_step` keys.
#[derive(Debug, Clone, Copy)]
pub struct IndexWorkLimits {
    /// The shortest time between two index slices.
    pub slice_interval: Duration,
    /// The longest one slice runs, plus one step.
    pub slice_budget: Duration,
    /// The most index keys one step claims or releases.
    pub keys_per_step: NonZeroUsize,
}

impl Default for IndexWorkLimits {
    /// The limits the plugin runs with unless its config sets others.
    fn default() -> Self {
        Self {
            slice_interval: Duration::from_millis(1),
            slice_budget: Duration::from_micros(100),
            keys_per_step: INDEX_KEYS_PER_STEP,
        }
    }
}

/// A key that finds candidate subscribers in an owner's index. The per-kind keys, from
/// [`IndexKey::Slots`] on, sort last (the derived [`Ord`]), so they end a sorted key list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IndexKey {
    /// An update of the account with this pubkey.
    Account(Pubkey),
    /// An update of an account this program owns.
    Owner(Pubkey),
    /// An update of an account whose data holds these 32 bytes at this offset, one of
    /// [`INDEXED_MEMCMP_OFFSETS`].
    Memcmp(usize, [u8; 32]),
    /// No message carries it: it opens the memcmp probes of the accounts this program owns, for an
    /// account filter that a memcmp key indexes and that lists this owner.
    MemcmpGate(Pubkey),
    /// No message carries it: it opens the memcmp probes of every account, for a memcmp key whose
    /// account filter lists no owner, or whose gates do not fit in [`MAX_INDEX_KEYS`].
    UngatedMemcmp,
    /// A transaction whose account keys hold this pubkey.
    Transaction(Pubkey),
    /// Any [`Message::Slot`].
    Slots,
    /// Any [`Message::Entry`].
    Entries,
    /// Any [`Message::EntryUpdateParent`].
    UpdateParent,
    /// Any [`Message::BlockMeta`].
    BlocksMeta,
    /// Any [`Message::BlockFooter`].
    BlockFooter,
}

/// A client's receiver on one commitment level's broadcast channel.
#[allow(clippy::disallowed_types, reason = "the unindexed tokio broadcast")]
type BroadcastReceiver = broadcast::Receiver<BroadcastedMessage>;

/// A client's change to its indexed subscriber, sent to the owner of its level and taken in by
/// [`OwnerIndexes::take_index_change`] between two of the owner's sends.
pub enum IndexChange {
    /// Stages the indexed subscriber. The owner claims its keys in index slices, then answers on
    /// the sender. The client keeps reading its broadcast receiver until then.
    Prepare(IndexedSubscriber, oneshot::Sender<()>),
    /// Makes the indexed subscriber live between two sends. With the receiver the client read to
    /// the end, the owner first delivers through the indexed subscriber what the receiver got
    /// since. Without one, when the session's live subscriber at this level holds the same key list
    /// [`Arc`], the owner only replaces that indexed subscriber's filter. Otherwise it claims at
    /// once every key the session has not claimed for this indexed subscriber, which is none after
    /// an answered [`IndexChange::Prepare`] of the same key list.
    Join(IndexedSubscriber, Option<BroadcastReceiver>),
    /// Removes the session's indexed subscriber at every level, staged or live, then answers the
    /// receiver request, if any, at once. The indexed subscriber's per-kind keys are released at
    /// once, and its other keys in later index steps.
    Leave(IndexLeave),
}

/// A client's leave: which indexed subscriber to remove, and whether the client wants a receiver
/// back.
pub struct IndexLeave {
    /// The session id of the indexed subscriber to remove.
    pub session_id: usize,
    /// A level and an answer sender, or [`None`]. With them, the owner answers with a receiver on
    /// that level's channel, subscribed right after its last indexed send.
    pub receiver_request: Option<(CommitmentLevel, oneshot::Sender<BroadcastReceiver>)>,
}

/// Sends a leave to the owners when it drops, so they remove the client's indexed subscriber once
/// the client's task ends. An indexed subscriber holds a clone of the client's queue sender, so an
/// indexed subscriber left behind would keep the client's stream open.
///
/// Both owners get the leave, unless [`LeaveOnDrop::leave_only`] named the owner that holds the
/// indexed subscriber, or said that none does.
pub struct LeaveOnDrop {
    /// The client's session id.
    pub session_id: usize,
    /// The index change senders of `geyser_loop` and block reconstruction, in that order. Each
    /// sender left here gets the leave.
    #[allow(clippy::disallowed_types, reason = "index changes on tokio channels")]
    pub index_change_txs: [Option<mpsc::UnboundedSender<IndexChange>>; 2],
}

impl LeaveOnDrop {
    /// Keeps only the sender of the owner that holds the client's indexed subscriber, staged or
    /// live.
    pub fn leave_only(&mut self, subscriber_level: Option<CommitmentLevel>) {
        let subscriber_owner_index = subscriber_level
            .map(|subscriber_level| usize::from(subscriber_level != CommitmentLevel::Processed));
        for (owner_index, index_change_tx) in self.index_change_txs.iter_mut().enumerate() {
            if subscriber_owner_index != Some(owner_index) {
                *index_change_tx = None;
            }
        }
    }
}

impl Drop for LeaveOnDrop {
    /// Sends an [`IndexChange::Leave`] with no receiver request to each owner whose sender is
    /// left, skipping an owner that has stopped.
    fn drop(&mut self) {
        for index_change_tx in self.index_change_txs.iter().flatten() {
            let client_leave = IndexLeave {
                session_id: self.session_id,
                receiver_request: None,
            };
            let _ = index_change_tx.send(IndexChange::Leave(client_leave));
        }
    }
}

/// Why an owner disconnects an indexed client.
enum DeliveryFailure {
    /// The client's queue has no room for an update.
    Full,
    /// The client's queue is closed.
    Closed,
    /// The client's filter panicked.
    Panicked,
    /// The receiver the client handed over lagged behind its channel.
    Lagged,
}

/// One client's entry in an owner's index: its filter and index keys, and what the owner needs
/// to deliver to the client and to disconnect it.
///
/// Clones share the client's queue, cancellation token, disconnect reason and stream counters.
/// So `client_loop` keeps one from [`IndexedSubscriber::new`] as a template, sends a copy made by
/// [`IndexedSubscriber::with_filter`] with each filter, and reads
/// [`IndexedSubscriber::disconnect_reason`] from the template when it ends.
#[derive(Clone)]
pub struct IndexedSubscriber {
    /// The client's session id.
    session_id: usize,
    /// The client's filter, which makes the exact match for each candidate message.
    client_filter: Arc<Filter>,
    /// The filter's index keys, which the indexed subscriber is indexed under.
    index_keys: Arc<[IndexKey]>,
    /// The client's Subscribe queue.
    stream_tx: SubscribeSender,
    /// The client's cancellation token, cancelled when an owner disconnects the client.
    cancellation_token: CancellationToken,
    /// Why an owner disconnected the client, set once for every clone.
    #[allow(clippy::disallowed_types, reason = "set-once disconnect reason")]
    disconnect_reason: Arc<OnceLock<&'static str>>,
    /// The client's stream counters, which count each update queued here.
    stream_counters: Arc<StreamCounters>,
}

impl IndexedSubscriber {
    /// Creates a client's subscriber template, with the default filter, which matches nothing, and
    /// no keys.
    pub fn new(
        session_id: usize,
        stream_counters: &Arc<StreamCounters>,
        stream_tx: &SubscribeSender,
        cancellation_token: &CancellationToken,
    ) -> Self {
        stream_counters.make_sent_series();
        Self {
            session_id,
            client_filter: Arc::default(),
            index_keys: Arc::default(),
            stream_tx: stream_tx.clone(),
            cancellation_token: cancellation_token.clone(),
            disconnect_reason: Arc::default(),
            stream_counters: Arc::clone(stream_counters),
        }
    }

    /// Copies this indexed subscriber with a new filter and its keys.
    pub fn with_filter(&self, client_filter: &Arc<Filter>, index_keys: &Arc<[IndexKey]>) -> Self {
        Self {
            client_filter: Arc::clone(client_filter),
            index_keys: Arc::clone(index_keys),
            ..self.clone()
        }
    }

    /// Why an owner disconnected the client, if one did.
    pub fn disconnect_reason(&self) -> Option<&'static str> {
        self.disconnect_reason.get().copied()
    }

    /// The commitment level of the indexed subscriber's filter, which picks its owner and its level
    /// index.
    fn commitment_level(&self) -> CommitmentLevel {
        self.client_filter.get_commitment_level()
    }

    /// Applies the filter to `delivered_message` and queues each update it makes, without waiting.
    ///
    /// # Errors
    ///
    /// [`DeliveryFailure::Panicked`] if the filter panics, [`DeliveryFailure::Full`] if the queue
    /// has no room for an update, and [`DeliveryFailure::Closed`] if the queue is closed. The
    /// updates queued before the failure stay queued.
    fn queue_updates(&self, delivered_message: &Message) -> Result<(), DeliveryFailure> {
        let filtered_updates = catch_unwind(AssertUnwindSafe(|| {
            self.client_filter
                .get_updates(delivered_message, Some(self.commitment_level()))
        }))
        .map_err(|_| DeliveryFailure::Panicked)?;
        for filtered_update in filtered_updates {
            match self
                .stream_tx
                .try_send(Ok(SubscribeQueueEntry::Filtered(filtered_update)))
            {
                Ok(()) => self.stream_counters.add_sent_updates(1),
                Err(TrySendError::Full(_)) => return Err(DeliveryFailure::Full),
                Err(TrySendError::Closed(_)) => return Err(DeliveryFailure::Closed),
            }
        }
        Ok(())
    }

    /// The first step of a grouped send: the filter's matched updates of `delivered_message`,
    /// written into `match_scratch`. A panic in the filter gives [`DeliveryFailure::Panicked`].
    fn match_message(
        &self,
        delivered_message: &Message,
        match_scratch: &mut MatchScratch,
    ) -> Result<(), DeliveryFailure> {
        catch_unwind(AssertUnwindSafe(|| {
            self.client_filter.match_update(
                delivered_message,
                Some(self.commitment_level()),
                match_scratch,
            )
        }))
        .map_err(|_| DeliveryFailure::Panicked)
    }

    /// The last step of a grouped send: queues `member_updates` as entries of their message,
    /// taken from `group_entries`, without waiting; an update that does not fit ends the send.
    fn queue_indexed_updates(
        &self,
        member_updates: &[MatchedUpdate],
        group_entries: &mut GroupEntries<'_>,
    ) -> Result<(), DeliveryFailure> {
        for matched_update in member_updates {
            let message_entry = group_entries
                .next_entry()
                .expect("the group added an entry for each indexed update");
            let indexed_update = IndexedUpdateRef::new(
                message_entry,
                Arc::clone(&self.client_filter),
                *matched_update,
            );
            match self
                .stream_tx
                .try_send(Ok(SubscribeQueueEntry::Indexed(indexed_update)))
            {
                Ok(()) => self.stream_counters.add_sent_updates(1),
                Err(TrySendError::Full(_)) => return Err(DeliveryFailure::Full),
                Err(TrySendError::Closed(_)) => return Err(DeliveryFailure::Closed),
            }
        }
        Ok(())
    }

    /// Disconnects the client for `delivery_failure`: records the reason, closes the queue behind
    /// what it holds, and cancels the client's cancellation token. Only the first call on any clone
    /// does anything.
    ///
    /// A full queue, a closed queue and a lagged receiver get the reasons and the error statuses
    /// that [`GrpcService::client_loop`](crate::grpc::GrpcService::client_loop) uses on the
    /// broadcast. A closed queue gets no status.
    fn disconnect_client(&self, delivery_failure: DeliveryFailure) {
        let (disconnect_reason, status_message) = match delivery_failure {
            DeliveryFailure::Full => ("client_channel_full", Some("lagged to send an update")),
            DeliveryFailure::Closed => ("client_closed", None),
            DeliveryFailure::Panicked => ("filter_failed", Some("failed to apply the filter")),
            DeliveryFailure::Lagged => (
                "client_broadcast_lag",
                Some("lagged to receive geyser messages"),
            ),
        };
        if self.disconnect_reason.set(disconnect_reason).is_ok() {
            if let Some(status_message) = status_message {
                let last_item = Err(Status::internal(status_message));
                self.stream_tx.close_with(last_item);
            }
            self.cancellation_token.cancel();
        }
    }
}

/// An indexed subscriber whose keys the owner claims in index steps before it can go live.
struct StagedSubscriber {
    /// The indexed subscriber the client prepared, with its filter and key list.
    prepared_subscriber: IndexedSubscriber,
    /// The slot the keys are claimed for, at the indexed subscriber's level.
    staged_slot: u32,
    /// How many of the indexed subscriber's keys, from the first, the slot has claimed.
    claimed_key_count: usize,
    /// Answers the client once every key is claimed; [`None`] once answered.
    ready_tx: Option<oneshot::Sender<()>>,
    /// The sequence number of this staging, which its entry in `staging_order` carries.
    staging_sequence: u64,
}

/// Claims a retired slot still holds, released oldest first in index steps.
struct ClaimsToRelease {
    /// The retired slot, free once these claims are released.
    retired_slot: u32,
    /// The key list of the slot's last indexed subscriber.
    index_keys: Arc<[IndexKey]>,
    /// How many of the keys, from the first, are released already.
    released_key_count: usize,
    /// The end of the keys to release: the claims are those of `index_keys[..claimed_key_count]`.
    claimed_key_count: usize,
}

/// What one index step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexStep {
    /// No claim or release was left to do.
    NoWorkLeft,
    /// The step claimed or released index keys, and answered no preparation.
    Advanced,
    /// The step claimed the last key of a staged subscriber and answered its preparation.
    AnsweredPreparation,
}

/// Calls `visit_index_key` with each index key `keyed_message` carries.
///
/// An account carries its pubkey, its owner program, and the 32 bytes of its data at each of
/// [`INDEXED_MEMCMP_OFFSETS`] that its data is long enough for. A transaction carries each of
/// its account keys. Any other kind carries its kind's index key, except a [`Message::Block`]
/// or a deshred message, which carry none. The live level index and [`BankKeyIndex`] use this
/// one list.
pub fn for_each_index_key(keyed_message: &Message, mut visit_index_key: impl FnMut(IndexKey)) {
    match keyed_message {
        Message::Account(message_account) => {
            visit_index_key(IndexKey::Account(message_account.pubkey));
            visit_index_key(IndexKey::Owner(message_account.owner));
            for memcmp_offset in INDEXED_MEMCMP_OFFSETS {
                let data_at_offset = message_account.data().get(memcmp_offset..);
                if let Some(memcmp_bytes) = data_at_offset.and_then(<[u8]>::first_chunk) {
                    visit_index_key(IndexKey::Memcmp(memcmp_offset, *memcmp_bytes));
                }
            }
        }
        Message::Transaction(message_transaction) => {
            for account_key in &message_transaction.transaction.account_keys {
                visit_index_key(IndexKey::Transaction(*account_key));
            }
        }
        Message::Slot(_) => visit_index_key(IndexKey::Slots),
        Message::Entry(_) => visit_index_key(IndexKey::Entries),
        Message::EntryUpdateParent(_) => visit_index_key(IndexKey::UpdateParent),
        Message::BlockMeta(_) => visit_index_key(IndexKey::BlocksMeta),
        Message::BlockFooter(_) => visit_index_key(IndexKey::BlockFooter),
        Message::Block(_) | Message::DeshredTransaction(_) | Message::DeshredUpdateParent(_) => {}
    }
}

/// The one hasher of every [`BankKeyIndex`] in the process, so a replay hashes its index
/// keys once for all banks. Its seed is random, so chain data is unlikely to crowd one bucket.
static BANK_KEY_INDEX_HASHER: LazyLock<foldhash::fast::RandomState> =
    LazyLock::new(Default::default);

/// Where the messages that carry each index key are in one sealed bank.
///
/// It holds hashes, not keys: two keys with one hash share their positions, so a probe can
/// return a message that carries none of the probed keys, but never misses one that carries a
/// probed key, and [`Filter::get_updates`] makes the exact match. It is built once per sealed
/// bank, off the owners, and never changes.
pub struct BankKeyIndex {
    /// One entry per index key hash, with its run in `message_positions`.
    key_positions: HashTable<KeyPositions>,
    /// The bank's message positions, grouped by index key hash, ascending in a group.
    message_positions: Box<[u32]>,
}

/// One index key hash of a [`BankKeyIndex`], and where its positions are.
struct KeyPositions {
    /// The index key hash.
    index_key_hash: u64,
    /// The first of its positions in `message_positions`.
    positions_start: u32,
    /// One past its last position there.
    positions_end: u32,
}

impl BankKeyIndex {
    /// The hash a [`BankKeyIndex`] files `index_key` under, the same in every bank.
    pub fn index_key_hash(index_key: &IndexKey) -> u64 {
        BANK_KEY_INDEX_HASHER.hash_one(index_key)
    }

    /// Indexes the messages of one sealed bank under the index keys each one carries, as
    /// [`for_each_index_key`] lists them.
    pub fn new(bank_messages: &[Message], scratch_postings: &mut Vec<(u64, u32)>) -> Self {
        scratch_postings.clear();
        for (message_position, bank_message) in bank_messages.iter().enumerate() {
            for_each_index_key(bank_message, |index_key| {
                let index_key_hash = Self::index_key_hash(&index_key);
                scratch_postings.push((index_key_hash, message_position as u32));
            });
        }
        scratch_postings.sort_unstable();
        scratch_postings.dedup();
        let message_positions = scratch_postings
            .iter()
            .map(|(_, message_position)| *message_position)
            .collect();
        let same_index_key_hash = |first_posting: &(u64, u32), second_posting: &(u64, u32)| {
            first_posting.0 == second_posting.0
        };
        let index_key_count = scratch_postings.chunk_by(same_index_key_hash).count();
        let mut key_positions = HashTable::with_capacity(index_key_count);
        let mut positions_start = 0;
        for key_postings in scratch_postings.chunk_by(same_index_key_hash) {
            let index_key_hash = key_postings[0].0;
            let positions_end = positions_start + key_postings.len() as u32;
            let indexed_key = KeyPositions {
                index_key_hash,
                positions_start,
                positions_end,
            };
            key_positions.insert_unique(index_key_hash, indexed_key, |indexed_key| {
                indexed_key.index_key_hash
            });
            positions_start = positions_end;
        }
        Self {
            key_positions,
            message_positions,
        }
    }

    /// Collects the positions of the messages filed under any of `index_key_hashes`: every
    /// message that carries one of the hashed index keys, and any message whose index key
    /// shares a hash with one.
    pub fn collect_candidate_positions(
        &self,
        index_key_hashes: &[u64],
        candidate_positions: &mut Vec<u32>,
    ) {
        candidate_positions.clear();
        for index_key_hash in index_key_hashes {
            let indexed_key = self.key_positions.find(*index_key_hash, |indexed_key| {
                indexed_key.index_key_hash == *index_key_hash
            });
            if let Some(indexed_key) = indexed_key {
                let positions_run =
                    indexed_key.positions_start as usize..indexed_key.positions_end as usize;
                candidate_positions.extend_from_slice(&self.message_positions[positions_run]);
            }
        }
        candidate_positions.sort_unstable();
        candidate_positions.dedup();
    }
}

/// One commitment level's indexed subscribers at an owner, each in a slot that claims its keys.
/// A departed slot is emptied at once, and reused only once its last claim is released.
#[derive(Default)]
pub struct LevelIndex {
    /// Which slots claim each index key, and the hit words of the message being delivered.
    key_claim_index: KeyClaimIndex,
    /// The live subscriber of each slot, by slot number: [`None`] for a free, staged or retired
    /// slot.
    slot_subscribers: Vec<Option<IndexedSubscriber>>,
    /// The session id each slot was last opened for, by slot number.
    slot_session_ids: Vec<usize>,
    /// The free slots; the last one freed is opened first.
    free_slots: Vec<u32>,
    /// The slot of each session's live subscriber. Index changes and failed deliveries read it.
    live_slots_by_session_id: HashMap<usize, u32>,
    /// The claims of retired slots that wait for release, oldest first. It keeps its capacity.
    claims_to_release: VecDeque<ClaimsToRelease>,
    /// The matched updates of the member being delivered to. It keeps its capacity.
    match_scratch: MatchScratch,
}

impl LevelIndex {
    /// Opens a slot for a subscriber of the session: the last one freed, or a new one, which may widen
    /// the rows of slot bits. A level has at most [`MAX_SLOT_COUNT`] slots.
    pub fn open_slot(&mut self, session_id: usize) -> u32 {
        let opened_slot = self.free_slots.pop().unwrap_or_else(|| {
            let new_slot = self.slot_subscribers.len();
            assert!(
                new_slot < MAX_SLOT_COUNT,
                "a level index has at most 2^30 slots"
            );
            self.slot_subscribers.push(None);
            self.slot_session_ids.push(session_id);
            self.key_claim_index.fit_slot(new_slot as u32);
            new_slot as u32
        });
        self.slot_session_ids[opened_slot as usize] = session_id;
        opened_slot
    }

    /// Claims a key the slot does not claim yet; `true` when a table shard grew.
    pub fn claim_index_key(&mut self, index_key: &IndexKey, claiming_slot: u32) -> bool {
        self.key_claim_index
            .claim_index_key(index_key, claiming_slot)
    }

    /// Claims the keys for the slot, all at once.
    pub fn claim_index_keys(&mut self, claiming_slot: u32, index_keys: &[IndexKey]) {
        for index_key in index_keys {
            self.key_claim_index
                .claim_index_key(index_key, claiming_slot);
        }
    }

    /// Retires a slot that holds no live subscriber, and releases the claims of its claimed keys: the
    /// per-kind keys at once, the others in index steps. The slot is free once all are released.
    pub fn retire_slot(
        &mut self,
        retired_slot: u32,
        index_keys: &Arc<[IndexKey]>,
        claimed_key_count: usize,
    ) {
        debug_assert!(self.slot_subscribers[retired_slot as usize].is_none());
        let claimed_index_keys = &index_keys[..claimed_key_count];
        let per_kind_start =
            claimed_index_keys.partition_point(|index_key| *index_key < IndexKey::Slots);
        for index_key in &claimed_index_keys[per_kind_start..] {
            self.key_claim_index
                .release_index_key(index_key, retired_slot);
        }
        if per_kind_start == 0 {
            self.free_slots.push(retired_slot);
            return;
        }
        self.claims_to_release.push_back(ClaimsToRelease {
            retired_slot,
            index_keys: Arc::clone(index_keys),
            released_key_count: 0,
            claimed_key_count: per_kind_start,
        });
    }

    /// Removes the session's live subscriber, if the level index holds one, and retires its slot.
    pub fn depart_subscriber(&mut self, session_id: usize) {
        let Some(departed_slot) = self.live_slots_by_session_id.remove(&session_id) else {
            return;
        };
        let Some(departed_subscriber) = self.slot_subscribers[departed_slot as usize].take() else {
            return;
        };
        let index_keys = &departed_subscriber.index_keys;
        self.retire_slot(departed_slot, index_keys, index_keys.len());
    }

    /// Puts `joined_subscriber` in the slot of the session's live subscriber when both hold the same
    /// key list [`Arc`]; otherwise gives it back.
    pub fn replace_live_filter(
        &mut self,
        joined_subscriber: IndexedSubscriber,
    ) -> Option<IndexedSubscriber> {
        let live_slot = self
            .live_slots_by_session_id
            .get(&joined_subscriber.session_id);
        let live_subscriber =
            live_slot.and_then(|live_slot| self.slot_subscribers[*live_slot as usize].as_mut());
        match live_subscriber {
            Some(live_subscriber)
                if Arc::ptr_eq(&live_subscriber.index_keys, &joined_subscriber.index_keys) =>
            {
                *live_subscriber = joined_subscriber;
                None
            }
            _ => Some(joined_subscriber),
        }
    }

    /// Delivers what the receiver holds, then makes the subscriber live in its slot, which claims all
    /// its keys, and retires the session's earlier live slot. A failed delivery disconnects instead.
    pub fn make_subscriber_live(
        &mut self,
        live_subscriber: IndexedSubscriber,
        live_slot: u32,
        broadcast_rx: Option<BroadcastReceiver>,
    ) {
        if let Err(delivery_failure) = drain_into_subscriber(broadcast_rx, &live_subscriber) {
            live_subscriber.disconnect_client(delivery_failure);
            let index_keys = &live_subscriber.index_keys;
            self.retire_slot(live_slot, index_keys, index_keys.len());
            return;
        }
        let session_id = live_subscriber.session_id;
        self.slot_subscribers[live_slot as usize] = Some(live_subscriber);
        let replaced_slot = self.live_slots_by_session_id.insert(session_id, live_slot);
        if let Some(replaced_slot) =
            replaced_slot.filter(|replaced_slot| *replaced_slot != live_slot)
        {
            if let Some(replaced_subscriber) = self.slot_subscribers[replaced_slot as usize].take()
            {
                let index_keys = &replaced_subscriber.index_keys;
                self.retire_slot(replaced_slot, index_keys, index_keys.len());
            }
        }
    }

    /// Releases up to `keys_per_step` claims of the oldest retired slot, and frees the slot once all
    /// are released; `true` if any claim waited.
    pub fn release_queued_claims(&mut self, keys_per_step: usize) -> bool {
        let Some(claims_to_release) = self.claims_to_release.front_mut() else {
            return false;
        };
        let released_key_count = claims_to_release.released_key_count;
        let release_end =
            (released_key_count + keys_per_step).min(claims_to_release.claimed_key_count);
        for index_key in &claims_to_release.index_keys[released_key_count..release_end] {
            self.key_claim_index
                .release_index_key(index_key, claims_to_release.retired_slot);
        }
        claims_to_release.released_key_count = release_end;
        if release_end == claims_to_release.claimed_key_count {
            let retired_slot = claims_to_release.retired_slot;
            self.claims_to_release.pop_front();
            self.free_slots.push(retired_slot);
        }
        true
    }

    /// Delivers each message once to each live subscriber whose slot a key of the message finds. A
    /// failed delivery departs that subscriber, so the batch releases only its per-kind keys.
    pub fn deliver_batch(&mut self, message_batch: &[Message]) {
        if self.live_slots_by_session_id.is_empty() || !self.key_claim_index.holds_slot_claims() {
            return;
        }
        for delivered_message in message_batch {
            if !self.key_claim_index.gather_hit_slots(delivered_message) {
                continue;
            }
            let mut group_entries = GroupEntries::new(delivered_message);
            for hit_word_number in 0..self.key_claim_index.hit_word_count() {
                let mut hit_word = self.key_claim_index.take_hit_word(hit_word_number);
                while hit_word != 0 {
                    let hit_slot = hit_word_number * 64 + hit_word.trailing_zeros() as usize;
                    hit_word &= hit_word - 1;
                    self.deliver_to_slot(hit_slot, hit_word, delivered_message, &mut group_entries);
                }
            }
        }
    }

    /// Makes the exact match for the slot's live subscriber, if any, and queues its updates at once
    /// as entries of the message's group. `later_hit_word` holds this word's later hits.
    #[inline]
    fn deliver_to_slot(
        &mut self,
        hit_slot: usize,
        later_hit_word: u64,
        delivered_message: &Message,
        group_entries: &mut GroupEntries<'_>,
    ) {
        let Some(candidate_subscriber) = &self.slot_subscribers[hit_slot] else {
            return;
        };
        let delivery_result = candidate_subscriber
            .match_message(delivered_message, &mut self.match_scratch)
            .and_then(|()| {
                let member_updates = self.match_scratch.matched_updates();
                let takes_reference_path = takes_reference_path(member_updates);
                if !takes_reference_path && member_updates.len() > group_entries.entries_left() {
                    let later_hit_count = later_hit_word.count_ones() as usize
                        + self.key_claim_index.untaken_hit_count();
                    let later_entry_bound =
                        later_hit_count * indexed_updates_per_member_bound(delivered_message);
                    group_entries.add_entries(member_updates.len() + later_entry_bound);
                }
                if takes_reference_path || member_updates.len() > group_entries.entries_left() {
                    return candidate_subscriber.queue_updates(delivered_message);
                }
                candidate_subscriber.queue_indexed_updates(member_updates, group_entries)
            });
        if let Err(delivery_failure) = delivery_result {
            candidate_subscriber.disconnect_client(delivery_failure);
            let departed_session_id = candidate_subscriber.session_id;
            self.depart_subscriber(departed_session_id);
        }
    }
}

/// An owner's indexes at each level, its broadcast channels and its index work. Every send and
/// index change goes through it, and index slices claim and release the keys.
pub struct OwnerIndexes {
    /// The broadcast channels, for the clients that are not indexed.
    broadcast_channels: SubscriberChannels,
    /// The indexed subscribers at each commitment level, indexed by the [`CommitmentLevel`] as
    /// `usize`.
    level_indexes: [LevelIndex; 3],
    /// The staged subscriber of each session that prepared one, until its join or its leave.
    staged_subscribers_by_session_id: HashMap<usize, StagedSubscriber>,
    /// The session id and staging sequence of each staging, oldest first. An entry whose staged
    /// subscriber is gone, or has another sequence, is stale, and a claim step skips it.
    staging_order: VecDeque<(usize, u64)>,
    /// The staging sequence the last staged subscriber got.
    last_staging_sequence: u64,
    /// How much index work one slice does, and how often slices run.
    index_work_limits: IndexWorkLimits,
    /// When the next idle index slice can run.
    next_index_slice_at: Instant,
    /// When waiting index work makes a slice run at the top of a pass.
    index_slice_overdue_at: Instant,
}

impl OwnerIndexes {
    /// Creates an owner's indexes, with no indexed subscriber yet.
    pub fn new(broadcast_channels: SubscriberChannels) -> Self {
        let index_work_limits = broadcast_channels.index_work_limits();
        let created_at = Instant::now();
        Self {
            broadcast_channels,
            level_indexes: Default::default(),
            staged_subscribers_by_session_id: HashMap::default(),
            staging_order: VecDeque::new(),
            last_staging_sequence: 0,
            index_work_limits,
            next_index_slice_at: created_at,
            index_slice_overdue_at: created_at,
        }
    }

    /// Delivers `message_batch` to the indexed clients at `commitment_level`, then
    /// broadcasts it on that level's channel.
    ///
    /// When a failed delivery departs an indexed subscriber and leaves index work where none
    /// waited, that work is overdue ten slice intervals from now, as the work of an index change
    /// is.
    pub fn send_batch(
        &mut self,
        commitment_level: CommitmentLevel,
        message_batch: BroadcastedMessage,
    ) {
        let had_index_work = self.has_index_work();
        self.level_indexes[commitment_level as usize].deliver_batch(&message_batch);
        self.start_overdue_wait(had_index_work);
        self.broadcast_channels
            .send(commitment_level, message_batch);
    }

    /// Takes in a client's index change between two sends: a prepare stages, a join goes live, or
    /// disconnects the client when its hand-over fails, and a leave removes it at every level.
    pub fn take_index_change(&mut self, index_change: IndexChange) {
        let had_index_work = self.has_index_work();
        match index_change {
            IndexChange::Prepare(prepared_subscriber, ready_tx) => {
                self.stage_subscriber(prepared_subscriber, ready_tx);
            }
            IndexChange::Join(joined_subscriber, broadcast_rx) => {
                self.join_subscriber(joined_subscriber, broadcast_rx);
            }
            IndexChange::Leave(client_leave) => {
                self.unstage_subscriber(client_leave.session_id);
                for level_index in &mut self.level_indexes {
                    level_index.depart_subscriber(client_leave.session_id);
                }
                if let Some((commitment_level, answer_tx)) = client_leave.receiver_request {
                    let _ = answer_tx.send(self.broadcast_channels.subscribe(commitment_level));
                }
            }
        }
        self.start_overdue_wait(had_index_work);
    }

    /// Makes index work that just started to wait overdue ten slice intervals from now.
    ///
    /// It does nothing when index work waited already, or when none waits now.
    fn start_overdue_wait(&mut self, had_index_work: bool) {
        if !had_index_work && self.has_index_work() {
            let overdue_wait =
                self.index_work_limits.slice_interval * INDEX_SLICE_OVERDUE_INTERVALS;
            self.index_slice_overdue_at = Instant::now() + overdue_wait;
        }
    }

    /// Takes in the index changes queued in `index_changes_rx`, oldest first, at most
    /// [`MAX_INDEX_CHANGES_PER_PASS`] of them.
    ///
    /// With an empty channel it reads the channel's length and nothing else. It never waits, and
    /// spends no tokio budget.
    #[allow(clippy::disallowed_types, reason = "index changes on tokio channels")]
    pub fn take_queued_index_changes(
        &mut self,
        index_changes_rx: &mut mpsc::UnboundedReceiver<IndexChange>,
    ) {
        let queued_change_count = index_changes_rx.len().min(MAX_INDEX_CHANGES_PER_PASS);
        for _ in 0..queued_change_count {
            let Ok(index_change) = index_changes_rx.try_recv() else {
                break;
            };
            self.take_index_change(index_change);
        }
    }

    /// Whether index work waits: keys to claim for a staged subscriber, or claims to release.
    pub fn has_index_work(&self) -> bool {
        !self.staging_order.is_empty()
            || self
                .level_indexes
                .iter()
                .any(|level_index| !level_index.claims_to_release.is_empty())
    }

    /// Whether waiting index work is overdue, so a slice runs at the top of a busy pass.
    ///
    /// It reads the clock only while index work waits.
    pub fn index_slice_is_overdue(&self) -> bool {
        self.has_index_work() && Instant::now() >= self.index_slice_overdue_at
    }

    /// Runs one slice of index work: steps of at most `keys_per_step` keys, releases first, until no
    /// work is left, eight preparations are answered or `slice_budget` is spent.
    pub fn run_index_slice(&mut self) {
        let slice_deadline = Instant::now() + self.index_work_limits.slice_budget;
        let mut answered_count = 0;
        let slice_end = loop {
            let index_step = self.advance_index_step();
            answered_count += usize::from(index_step == IndexStep::AnsweredPreparation);
            let step_end = Instant::now();
            if index_step == IndexStep::NoWorkLeft
                || answered_count == MAX_ANSWERS_PER_INDEX_SLICE
                || step_end >= slice_deadline
            {
                break step_end;
            }
        };
        let slice_interval = self.index_work_limits.slice_interval;
        self.next_index_slice_at = slice_end + slice_interval;
        self.index_slice_overdue_at = slice_end + slice_interval * INDEX_SLICE_OVERDUE_INTERVALS;
    }

    /// When the next idle index slice can run.
    pub const fn next_index_slice_at(&self) -> Instant {
        self.next_index_slice_at
    }

    /// Takes in the index changes queued in `index_changes_rx` with
    /// [`OwnerIndexes::take_queued_index_changes`], then runs a slice with
    /// [`OwnerIndexes::run_index_slice_and_reset_timer`] if
    /// [`OwnerIndexes::index_slice_is_overdue`] holds.
    ///
    /// An owner calls it at the top of each pass, so a change never waits for the owner's queue
    /// to read empty, and index work runs on an owner whose queue never reads empty. It never
    /// waits, and spends no tokio budget.
    #[inline]
    #[allow(clippy::disallowed_types, reason = "index changes on tokio channels")]
    pub fn take_queued_work(
        &mut self,
        index_changes_rx: &mut mpsc::UnboundedReceiver<IndexChange>,
        index_slice_timer: Pin<&mut Sleep>,
    ) {
        self.take_queued_index_changes(index_changes_rx);
        if self.index_slice_is_overdue() {
            self.run_index_slice_and_reset_timer(index_slice_timer);
        }
    }

    /// Runs one slice of index work with [`OwnerIndexes::run_index_slice`], then resets
    /// `index_slice_timer` to [`OwnerIndexes::next_index_slice_at`].
    #[inline]
    pub fn run_index_slice_and_reset_timer(&mut self, index_slice_timer: Pin<&mut Sleep>) {
        self.run_index_slice();
        index_slice_timer.reset(self.next_index_slice_at());
    }

    /// Runs one index step: a release step if any level index has claims to release, else a
    /// claim step for the oldest staging.
    fn advance_index_step(&mut self) -> IndexStep {
        let keys_per_step = self.index_work_limits.keys_per_step.get();
        for level_index in &mut self.level_indexes {
            if level_index.release_queued_claims(keys_per_step) {
                return IndexStep::Advanced;
            }
        }
        self.claim_staged_index_keys(keys_per_step)
    }

    /// Claims up to `keys_per_step` keys of the oldest staged subscriber, and answers its
    /// preparation once every key is claimed.
    ///
    /// It first drops the front entries of `staging_order` whose staged subscriber is gone or has
    /// another staging sequence. The step ends right after a claim that may grow a shard. If the
    /// answer finds that the client dropped its receiver, the client is gone, and the step
    /// unstages its indexed subscriber.
    fn claim_staged_index_keys(&mut self, keys_per_step: usize) -> IndexStep {
        while let Some(&(session_id, staging_sequence)) = self.staging_order.front() {
            let staged_subscriber = self
                .staged_subscribers_by_session_id
                .get_mut(&session_id)
                .filter(|staged_subscriber| staged_subscriber.staging_sequence == staging_sequence);
            let Some(staged_subscriber) = staged_subscriber else {
                self.staging_order.pop_front();
                continue;
            };
            let staged_level = staged_subscriber.prepared_subscriber.commitment_level() as usize;
            let level_index = &mut self.level_indexes[staged_level];
            let index_keys = &staged_subscriber.prepared_subscriber.index_keys;
            let claim_end =
                (staged_subscriber.claimed_key_count + keys_per_step).min(index_keys.len());
            while staged_subscriber.claimed_key_count < claim_end {
                let index_key = index_keys[staged_subscriber.claimed_key_count];
                staged_subscriber.claimed_key_count += 1;
                // A shard growth moves that shard's keys, so the step ends with it.
                if level_index.claim_index_key(&index_key, staged_subscriber.staged_slot) {
                    break;
                }
            }
            if staged_subscriber.claimed_key_count < index_keys.len() {
                return IndexStep::Advanced;
            }
            self.staging_order.pop_front();
            let ready_tx = staged_subscriber.ready_tx.take();
            if ready_tx.is_some_and(|ready_tx| ready_tx.send(()).is_ok()) {
                return IndexStep::AnsweredPreparation;
            }
            self.unstage_subscriber(session_id);
            return IndexStep::Advanced;
        }
        IndexStep::NoWorkLeft
    }

    /// Stages `prepared_subscriber` in a new slot at its level, with no key claimed yet, in place
    /// of the session's staged subscriber.
    fn stage_subscriber(
        &mut self,
        prepared_subscriber: IndexedSubscriber,
        ready_tx: oneshot::Sender<()>,
    ) {
        let session_id = prepared_subscriber.session_id;
        self.unstage_subscriber(session_id);
        self.last_staging_sequence += 1;
        let staging_sequence = self.last_staging_sequence;
        self.staging_order.push_back((session_id, staging_sequence));
        let staged_level = prepared_subscriber.commitment_level() as usize;
        let staged_slot = self.level_indexes[staged_level].open_slot(session_id);
        let staged_subscriber = StagedSubscriber {
            prepared_subscriber,
            staged_slot,
            claimed_key_count: 0,
            ready_tx: Some(ready_tx),
            staging_sequence,
        };
        self.staged_subscribers_by_session_id
            .insert(session_id, staged_subscriber);
    }

    /// Drops the session's staged subscriber, if it has one, and retires its slot, which releases
    /// the claims the slot made.
    ///
    /// The indexed subscriber's answer sender and its queue sender drop with it, and its entry in
    /// `staging_order` goes stale.
    fn unstage_subscriber(&mut self, session_id: usize) {
        let Some(unstaged_subscriber) = self.staged_subscribers_by_session_id.remove(&session_id)
        else {
            return;
        };
        let staged_level = unstaged_subscriber.prepared_subscriber.commitment_level() as usize;
        let index_keys = &unstaged_subscriber.prepared_subscriber.index_keys;
        self.level_indexes[staged_level].retire_slot(
            unstaged_subscriber.staged_slot,
            index_keys,
            unstaged_subscriber.claimed_key_count,
        );
    }

    /// Makes `joined_subscriber` live at its filter's level, as [`IndexChange::Join`] says.
    ///
    /// Without a receiver, a live subscriber of the session with the same key list [`Arc`] only
    /// gets the new filter, in its slot. Otherwise a staged subscriber of the session with the
    /// same level and key list [`Arc`] gives its slot and claims, any other is retired, and a new
    /// slot opens. Then the owner claims the keys not claimed yet, at once, and makes the indexed
    /// subscriber live with [`LevelIndex::make_subscriber_live`].
    fn join_subscriber(
        &mut self,
        joined_subscriber: IndexedSubscriber,
        broadcast_rx: Option<BroadcastReceiver>,
    ) {
        let session_id = joined_subscriber.session_id;
        // An owner disconnected this client, and its queue is closed: the indexed subscriber stays
        // out.
        if joined_subscriber.disconnect_reason().is_some() {
            self.unstage_subscriber(session_id);
            return;
        }
        let joined_level = joined_subscriber.commitment_level() as usize;
        let joined_subscriber = match broadcast_rx {
            None => match self.level_indexes[joined_level].replace_live_filter(joined_subscriber) {
                Some(unreplaced_subscriber) => unreplaced_subscriber,
                None => return,
            },
            Some(_) => joined_subscriber,
        };
        let mut staged_slot_and_claims = None;
        if let Some(staged_subscriber) = self.staged_subscribers_by_session_id.remove(&session_id) {
            let staged_level = staged_subscriber.prepared_subscriber.commitment_level() as usize;
            let staged_keys = &staged_subscriber.prepared_subscriber.index_keys;
            let staged_slot = staged_subscriber.staged_slot;
            if staged_level == joined_level
                && Arc::ptr_eq(staged_keys, &joined_subscriber.index_keys)
            {
                staged_slot_and_claims = Some((staged_slot, staged_subscriber.claimed_key_count));
            } else {
                self.level_indexes[staged_level].retire_slot(
                    staged_slot,
                    staged_keys,
                    staged_subscriber.claimed_key_count,
                );
            }
        }
        let level_index = &mut self.level_indexes[joined_level];
        let (joined_slot, claimed_key_count) =
            staged_slot_and_claims.unwrap_or_else(|| (level_index.open_slot(session_id), 0));
        level_index.claim_index_keys(
            joined_slot,
            &joined_subscriber.index_keys[claimed_key_count..],
        );
        level_index.make_subscriber_live(joined_subscriber, joined_slot, broadcast_rx);
    }
}

/// Delivers through `new_subscriber` every batch `broadcast_rx` holds.
///
/// # Errors
///
/// [`DeliveryFailure::Lagged`] if `broadcast_rx` lagged, and the error of
/// [`IndexedSubscriber::queue_updates`] if a delivery fails.
fn drain_into_subscriber(
    broadcast_rx: Option<BroadcastReceiver>,
    new_subscriber: &IndexedSubscriber,
) -> Result<(), DeliveryFailure> {
    let Some(mut broadcast_rx) = broadcast_rx else {
        return Ok(());
    };
    loop {
        match broadcast_rx.try_recv() {
            Ok(message_batch) => message_batch.iter().try_for_each(|delivered_message| {
                new_subscriber.queue_updates(delivered_message)
            })?,
            Err(TryRecvError::Empty | TryRecvError::Closed) => return Ok(()),
            Err(TryRecvError::Lagged(_)) => return Err(DeliveryFailure::Lagged),
        }
    }
}
