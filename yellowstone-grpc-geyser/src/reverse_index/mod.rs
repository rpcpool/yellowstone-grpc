//! Delivers updates straight into the queues of the clients whose index keys find them.
//!
//! Each commitment level has one owner, its only sender: `geyser_loop` at processed, and block
//! reconstruction at confirmed and finalized. An owner sends each batch through its
//! [`OwnerIndexes`], which delivers the batch to the indexed clients at that level, then broadcasts
//! it for the others.
//!
//! A client is indexed when its filter has index keys, from [`Filter::index_keys`]: every message
//! the filter matches carries one of them. Its [`IndexedSubscriber`] takes a slot in its owner's
//! [`LevelIndex`], which claims each of those [`IndexKey`]s for the slot. For each message, the
//! owner probes the index keys it carries, such as an account's pubkey and owner program, and
//! collects each claiming slot once. It applies each candidate's filter with [`Filter::get_updates`]
//! for the exact match, and queues the updates straight into the client's queue, so only the
//! clients that get an update wake. A client whose filter gets [`None`] from [`Filter::index_keys`]
//! stays on the broadcast, and its own task filters every batch.
//!
//! A client joins and leaves by sending an [`IndexChange`] to its level's owner, which takes it in
//! between two of its sends. Each level has one sender, so a client that moves between the
//! broadcast and the index neither misses nor repeats an update at that level:
//!
//! - [`IndexChange::Prepare`] stages an indexed subscriber. The owner claims the indexed
//!   subscriber's keys in index slices, then answers, and the client reads its broadcast receiver
//!   meanwhile.
//! - [`IndexChange::Join`] makes an indexed subscriber live between two sends, and claims at once
//!   any of its keys not claimed yet. With the client's broadcast receiver, read to the end, the
//!   owner first delivers through the indexed subscriber what the receiver got since. A join
//!   without a receiver replaces the filter of the live subscriber with the same key list.
//! - [`IndexChange::Leave`] removes an indexed subscriber at once. A client that goes back to the
//!   broadcast asks for a receiver, which the owner subscribes right after its last indexed send.
//!
//! A client joins in two steps, once it has read its broadcast receiver to the end: an
//! [`IndexChange::Prepare`], then, once the owner answers, an [`IndexChange::Join`] with the
//! receiver. So the client reads its own backlog, and the owner drains only what the receiver got
//! since the client's last read. A filter change to other keys or another level leaves the index
//! for a receiver and joins again the same way. A change that keeps the key list [`Arc`] replaces
//! the live subscriber's filter in place.
//!
//! The owner claims and releases index keys in index slices, at its idle points. Every key of a
//! live subscriber holds a claim for its slot, and a departed slot is reused only once all its
//! claims are released. The config keys `reverse_index_interval` and `reverse_index_budget` set
//! the slices' [`IndexWorkLimits`].
//!
//! [`LeaveOnDrop`] sends the last leave when the client's task ends. An owner disconnects an
//! indexed client when its queue is full or closed, its filter panics, or its handed-over receiver
//! lagged. The owner records the reason, closes the queue behind what the queue holds, and cancels
//! the client's cancellation token.

pub mod bank_thread;
pub mod claimed_key_table;
pub mod client_indexing;
pub mod key_claim_index;
pub mod processed_owner;
pub mod relay;

use {
    crate::{
        grpc::{BroadcastedMessage, SubscribeSender, SubscriberChannels},
        metrics,
        plugin::{
            filter::Filter,
            message::{CommitmentLevel, Message},
        },
        util::stream::TrySendError,
    },
    foldhash::HashMap,
    hashbrown::HashTable,
    key_claim_index::{KeyClaimIndex, MAX_SLOT_COUNT},
    prometheus::IntCounter,
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
///
/// # Examples
///
/// ```ignore
/// let index_work_limits = IndexWorkLimits {
///     slice_interval: Duration::from_millis(1),
///     slice_budget: Duration::from_micros(100),
///     keys_per_step: INDEX_KEYS_PER_STEP,
/// };
/// let (broadcast_channels, index_changes_rxs) =
///     SubscriberChannels::new(1024, 1024, 1024).with_indexing(index_work_limits);
/// assert_eq!(broadcast_channels.index_work_limits().keys_per_step.get(), 64);
/// ```
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
    ///
    /// # Returns
    ///
    /// A slice interval of 1 ms, a slice budget of 100 µs, and [`INDEX_KEYS_PER_STEP`]
    /// keys per step.
    fn default() -> Self {
        Self {
            slice_interval: Duration::from_millis(1),
            slice_budget: Duration::from_micros(100),
            keys_per_step: INDEX_KEYS_PER_STEP,
        }
    }
}

/// A key that finds candidate subscribers in an owner's index.
///
/// An owner indexes each indexed subscriber under its filter's index keys, and a message carries
/// the index keys of what it is about. A subscriber is a candidate for a message when the message
/// carries one of its index keys.
///
/// The per-kind keys, from [`IndexKey::Slots`] on, sort after every account, owner, memcmp and
/// transaction key (the derived [`Ord`]), so they are the end of a sorted key list.
/// [`Filter::index_keys`] sorts its keys and drops repeats, so each key list is a set in that
/// order.
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
///
/// # Examples
///
/// ```ignore
/// let mut leave_on_drop = LeaveOnDrop {
///     session_id: session.id,
///     index_change_txs: index_change_txs.clone().map(Some),
/// };
/// let _ = index_change_txs[0].send(IndexChange::Join(new_subscriber, Some(broadcast_rx)));
/// leave_on_drop.leave_only(Some(CommitmentLevel::Processed));
/// drop(leave_on_drop);
/// // geyser_loop gets a leave of `session.id` with no receiver request, and block
/// // reconstruction gets nothing.
/// ```
pub struct LeaveOnDrop {
    /// The client's session id.
    pub session_id: usize,
    /// The index change senders of `geyser_loop` and block reconstruction, in that order. Each
    /// sender left here gets the leave.
    pub index_change_txs: [Option<mpsc::UnboundedSender<IndexChange>>; 2],
}

impl LeaveOnDrop {
    /// Keeps only the sender of the owner that holds the client's indexed subscriber, staged or
    /// live.
    ///
    /// # Arguments
    ///
    /// * `subscriber_level` - The commitment level of the client's indexed subscriber, whose owner
    ///   gets the leave, or [`None`] when no owner holds an indexed subscriber of the client, and
    ///   no leave goes out.
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
/// Clones share the client's queue, cancellation token, disconnect reason and message sent counter.
/// So `client_loop` keeps one from [`IndexedSubscriber::new`] as a template, sends a copy made by
/// [`IndexedSubscriber::with_filter`] with each filter, and reads
/// [`IndexedSubscriber::disconnect_reason`] from the template when it ends.
///
/// # Examples
///
/// ```ignore
/// let subscriber_template =
///     IndexedSubscriber::new(session.id, &session.subscriber_id, &stream_tx, &cancellation_token);
/// let index_keys: Arc<[IndexKey]> =
///     client_filter.index_keys().expect("an indexable filter").into();
/// let new_subscriber = subscriber_template.with_filter(&client_filter, &index_keys);
/// owner_indexes.take_index_change(IndexChange::Join(new_subscriber, None));
/// // Once the client's task ends:
/// let disconnect_reason = subscriber_template.disconnect_reason();
/// ```
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
    disconnect_reason: Arc<OnceLock<&'static str>>,
    /// The sent-updates counter of the client's subscriber id, shared by every client with that id.
    message_sent_counter: IntCounter,
}

impl IndexedSubscriber {
    /// Creates a client's subscriber template, with the default filter, which matches nothing, and
    /// no keys.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
    /// * `subscriber_id` - The subscriber id whose [`metrics::grpc_message_sent_counter`] the
    ///   indexed subscriber bumps for each queued update.
    /// * `stream_tx` - The client's Subscribe queue.
    /// * `cancellation_token` - The client's cancellation token.
    ///
    /// # Returns
    ///
    /// The template, which [`IndexedSubscriber::with_filter`] copies with a filter to index.
    pub fn new(
        session_id: usize,
        subscriber_id: &str,
        stream_tx: &SubscribeSender,
        cancellation_token: &CancellationToken,
    ) -> Self {
        Self {
            session_id,
            client_filter: Arc::default(),
            index_keys: Arc::default(),
            stream_tx: stream_tx.clone(),
            cancellation_token: cancellation_token.clone(),
            disconnect_reason: Arc::default(),
            message_sent_counter: metrics::grpc_message_sent_counter(subscriber_id),
        }
    }

    /// Copies this indexed subscriber with a new filter and its keys.
    ///
    /// # Arguments
    ///
    /// * `client_filter` - The client's filter.
    /// * `index_keys` - The filter's index keys, from [`Filter::index_keys`].
    ///
    /// # Returns
    ///
    /// The copy, which shares this indexed subscriber's queue, cancellation token, disconnect
    /// reason and message sent counter.
    pub fn with_filter(&self, client_filter: &Arc<Filter>, index_keys: &Arc<[IndexKey]>) -> Self {
        Self {
            client_filter: Arc::clone(client_filter),
            index_keys: Arc::clone(index_keys),
            ..self.clone()
        }
    }

    /// Why an owner disconnected the client, if one did.
    ///
    /// # Returns
    ///
    /// The disconnect reason, such as `client_channel_full`, or [`None`] while no owner has
    /// disconnected the client.
    pub fn disconnect_reason(&self) -> Option<&'static str> {
        self.disconnect_reason.get().copied()
    }

    /// The commitment level of the indexed subscriber's filter, which picks its owner and its level
    /// index.
    ///
    /// # Returns
    ///
    /// The filter's [`CommitmentLevel`], from [`Filter::get_commitment_level`].
    fn commitment_level(&self) -> CommitmentLevel {
        self.client_filter.get_commitment_level()
    }

    /// Applies the filter to `delivered_message` and queues each update it makes, without waiting.
    ///
    /// # Arguments
    ///
    /// * `delivered_message` - A message the indexed subscriber is a candidate for.
    ///
    /// # Returns
    ///
    /// `Ok(())` once every update is queued, or when the filter makes none.
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
            match self.stream_tx.try_send(Ok(filtered_update)) {
                Ok(()) => self.message_sent_counter.inc(),
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
    ///
    /// # Arguments
    ///
    /// * `delivery_failure` - Why the owner disconnects the client.
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
///
/// # Arguments
///
/// * `keyed_message` - The message whose index keys to visit.
/// * `visit_index_key` - Gets each index key the message carries.
pub fn for_each_index_key(keyed_message: &Message, mut visit_index_key: impl FnMut(IndexKey)) {
    match keyed_message {
        Message::Account(message_account) => {
            let account_info = &message_account.account;
            visit_index_key(IndexKey::Account(account_info.pubkey));
            visit_index_key(IndexKey::Owner(account_info.owner));
            for memcmp_offset in INDEXED_MEMCMP_OFFSETS {
                let data_at_offset = account_info.data.get(memcmp_offset..);
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
///
/// # Examples
///
/// ```ignore
/// let bank_key_index = BankKeyIndex::new(&bank_messages, &mut scratch_postings);
/// let index_key_hashes: Vec<u64> =
///     index_keys.iter().map(BankKeyIndex::index_key_hash).collect();
/// bank_key_index.collect_candidate_positions(&index_key_hashes, &mut candidate_positions);
/// for candidate_position in &candidate_positions {
///     send(&bank_messages[*candidate_position as usize]);
/// }
/// ```
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
    ///
    /// # Arguments
    ///
    /// * `index_key` - The index key to hash.
    ///
    /// # Returns
    ///
    /// The key's hash, from the process's one bank index key hasher.
    pub fn index_key_hash(index_key: &IndexKey) -> u64 {
        BANK_KEY_INDEX_HASHER.hash_one(index_key)
    }

    /// Indexes the messages of one sealed bank under the index keys each one carries, as
    /// [`for_each_index_key`] lists them.
    ///
    /// # Arguments
    ///
    /// * `bank_messages` - The bank's messages, in bank order.
    /// * `scratch_postings` - Holds the `(index key hash, message position)` pairs while
    ///   the index is built. Its contents are replaced, and the caller keeps it for the next bank.
    ///
    /// # Returns
    ///
    /// The index, with the positions of each index key hash ascending, each once.
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
    ///
    /// # Arguments
    ///
    /// * `index_key_hashes` - The hashes of the index keys to look up, from
    ///   [`BankKeyIndex::index_key_hash`].
    /// * `candidate_positions` - Gets the positions, ascending and each once, in place of what
    ///   it held. It keeps its capacity.
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
    /// How many messages [`LevelIndex::deliver_batch`] looked up.
    #[cfg(test)]
    looked_up_message_count: u64,
    /// How many times each slot's live subscriber was a candidate, by slot number.
    #[cfg(test)]
    candidate_counts: Vec<u64>,
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
            #[cfg(test)]
            self.candidate_counts.push(0);
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
            #[cfg(test)]
            {
                self.looked_up_message_count += 1;
            }
            if !self.key_claim_index.gather_hit_slots(delivered_message) {
                continue;
            }
            for hit_word_number in 0..self.key_claim_index.hit_word_count() {
                let mut hit_word = self.key_claim_index.take_hit_word(hit_word_number);
                while hit_word != 0 {
                    let hit_slot = hit_word_number * 64 + hit_word.trailing_zeros() as usize;
                    hit_word &= hit_word - 1;
                    self.deliver_to_slot(hit_slot, delivered_message);
                }
            }
        }
    }

    /// Makes the exact match for the slot's live subscriber, if any, and queues its updates.
    #[inline]
    fn deliver_to_slot(&mut self, hit_slot: usize, delivered_message: &Message) {
        let Some(candidate_subscriber) = &self.slot_subscribers[hit_slot] else {
            return;
        };
        #[cfg(test)]
        {
            self.candidate_counts[hit_slot] += 1;
        }
        if let Err(delivery_failure) = candidate_subscriber.queue_updates(delivered_message) {
            candidate_subscriber.disconnect_client(delivery_failure);
            let departed_session_id = candidate_subscriber.session_id;
            self.depart_subscriber(departed_session_id);
        }
    }

    /// The claims' and slot table's bytes, by capacity, without the subscribers' filters and keys.
    pub fn held_bytes(&self) -> usize {
        let slot_bytes = std::mem::size_of::<Option<IndexedSubscriber>>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<u32>();
        self.key_claim_index.held_bytes() + self.slot_subscribers.capacity() * slot_bytes
    }
}

/// An owner's indexes at each commitment level, the broadcast channels it broadcasts on, and its
/// index work.
///
/// The owner sends every batch and takes in every index change through it, so each change lands
/// between two sends. A change claims or releases at once only the keys of a join that the owner
/// did not prepare, and the per-kind keys of an indexed subscriber that departs. Staged subscribers
/// claim their other keys, and departed subscribers release theirs, in index slices: the owner runs
/// one with [`OwnerIndexes::run_index_slice`] at an idle point, at most once per slice interval, or
/// at the top of a busy pass once [`OwnerIndexes::index_slice_is_overdue`] holds.
///
/// # Examples
///
/// ```ignore
/// let mut owner_indexes = OwnerIndexes::new(broadcast_channels);
/// owner_indexes.take_index_change(IndexChange::Prepare(new_subscriber.clone(), ready_tx));
/// while owner_indexes.has_index_work() {
///     owner_indexes.run_index_slice();
/// }
/// // The client got the answer on `ready_rx` and read `broadcast_rx` to the end.
/// owner_indexes.take_index_change(IndexChange::Join(new_subscriber, Some(broadcast_rx)));
/// owner_indexes.send_batch(CommitmentLevel::Processed, message_batch);
/// let client_leave = IndexLeave {
///     session_id,
///     receiver_request: None,
/// };
/// owner_indexes.take_index_change(IndexChange::Leave(client_leave));
/// ```
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
    ///
    /// # Arguments
    ///
    /// * `broadcast_channels` - The broadcast channels the owner broadcasts on, whose
    ///   [`SubscriberChannels::index_work_limits`] the owner's index work keeps to.
    ///
    /// # Returns
    ///
    /// A new `OwnerIndexes` that broadcasts every batch, and delivers none until a client joins.
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
    ///
    /// # Arguments
    ///
    /// * `commitment_level` - The commitment level `message_batch` goes out at.
    /// * `message_batch` - The batch to send.
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

    /// Takes in a client's index change, between two sends.
    ///
    /// - An [`IndexChange::Prepare`] stages the indexed subscriber, in place of the session's
    ///   staged subscriber. Index steps claim its keys later.
    /// - An [`IndexChange::Join`] makes the indexed subscriber live, as its docs say. If an owner
    ///   disconnected the client already, it only drops the session's staged subscriber. If the
    ///   delivery of what the receiver holds fails, or the receiver lagged, it disconnects the
    ///   client instead.
    /// - An [`IndexChange::Leave`] drops the session's staged subscriber and removes its live
    ///   subscriber at every level, and releases their per-kind keys at once. Index steps release
    ///   their other keys later. If the client asks for a receiver, the owner subscribes one right
    ///   then and answers with it.
    ///
    /// When the change leaves index work where none waited, that work is overdue ten slice
    /// intervals from now.
    ///
    /// # Arguments
    ///
    /// * `index_change` - The index change to take in.
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
    ///
    /// # Arguments
    ///
    /// * `had_index_work` - Whether index work waited before the change or the batch that the
    ///   caller just took in or sent.
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
    ///
    /// # Arguments
    ///
    /// * `index_changes_rx` - The owner's index change receiver.
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
    ///
    /// # Returns
    ///
    /// `true` while index work waits, and `false` otherwise.
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
    ///
    /// # Returns
    ///
    /// `true` when index work waits and ten slice intervals have passed since the work started to
    /// wait or since the last slice ended, and `false` otherwise.
    pub fn index_slice_is_overdue(&self) -> bool {
        self.has_index_work() && Instant::now() >= self.index_slice_overdue_at
    }

    /// Runs one slice of index work.
    ///
    /// It runs index steps, the first one always, until no work is left, it has answered eight
    /// preparations, or its [`IndexWorkLimits::slice_budget`] is spent. While any claim waits
    /// for release, a step releases claims, so releases run before claims. A step claims or
    /// releases at most [`IndexWorkLimits::keys_per_step`] keys, and a claim step ends right
    /// after a claim that may grow a shard of the index. The next idle slice can run one
    /// [`IndexWorkLimits::slice_interval`] after this one ends, and waiting work is overdue ten
    /// intervals after it ends.
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
    ///
    /// # Returns
    ///
    /// The time the owner's index slice timer waits for.
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
    ///
    /// # Arguments
    ///
    /// * `index_changes_rx` - The owner's index change receiver.
    /// * `index_slice_timer` - The owner's index slice timer.
    #[inline]
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
    ///
    /// # Arguments
    ///
    /// * `index_slice_timer` - The owner's index slice timer.
    #[inline]
    pub fn run_index_slice_and_reset_timer(&mut self, index_slice_timer: Pin<&mut Sleep>) {
        self.run_index_slice();
        index_slice_timer.reset(self.next_index_slice_at());
    }

    /// Takes in `index_change`, then runs every index step that waits, in one call.
    ///
    /// It is the synchronous form of an index change, for the tests. The owners never run index
    /// work whole.
    ///
    /// # Arguments
    ///
    /// * `index_change` - The index change to take in.
    #[cfg(test)]
    pub fn apply_index_change(&mut self, index_change: IndexChange) {
        self.take_index_change(index_change);
        while self.advance_index_step() != IndexStep::NoWorkLeft {}
    }

    /// Runs one index step: a release step if any level index has claims to release, else a
    /// claim step for the oldest staging.
    ///
    /// # Returns
    ///
    /// What the step did, [`IndexStep::NoWorkLeft`] when no index work waited.
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
    ///
    /// # Arguments
    ///
    /// * `keys_per_step` - The most keys to claim.
    ///
    /// # Returns
    ///
    /// [`IndexStep::AnsweredPreparation`] when the step answered a preparation,
    /// [`IndexStep::Advanced`] when it claimed keys and answered none, and
    /// [`IndexStep::NoWorkLeft`] when no staged subscriber was claiming keys.
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
    ///
    /// # Arguments
    ///
    /// * `prepared_subscriber` - The indexed subscriber the client prepared.
    /// * `ready_tx` - Answers the client once every key of the indexed subscriber is claimed.
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
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
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
    ///
    /// # Arguments
    ///
    /// * `joined_subscriber` - The indexed subscriber to make live.
    /// * `broadcast_rx` - The receiver the client read to the end, or [`None`].
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
/// # Arguments
///
/// * `broadcast_rx` - The receiver the client handed over, or [`None`] for a join without one.
/// * `new_subscriber` - The client's indexed subscriber.
///
/// # Returns
///
/// `Ok(())` once `broadcast_rx` reads empty or closed, or at once without a receiver.
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

#[cfg(test)]
pub mod tests {
    use {
        super::{
            claimed_key_table::{shard_number_of, ClaimRole, ForcedPairHash},
            key_claim_index::ClaimPlacement,
            *,
        },
        crate::{
            geyser_subscriber::{ReplayCut, ReplayPhase},
            grpc::{Replay, ReplayResponseMessageType},
            plugin::{
                filter::{fixtures, message::FilteredUpdate},
                message::{CommitmentLevel::Processed, MessageEntryUpdateParent, SlotStatus},
            },
            util::stream::{load_aware_channel, LoadAwareReceiver, MAX_CAPACITY},
        },
        futures::FutureExt,
        solana_signature::Signature,
        std::{
            alloc::{GlobalAlloc, Layout, System},
            cell::Cell,
            hint::black_box,
        },
        yellowstone_grpc_proto::{
            cuckoo::CuckooFilter,
            geyser::{
                subscribe_request_filter_accounts_filter::Filter as AccountsFilterOneof,
                subscribe_request_filter_accounts_filter_memcmp::Data as MemcmpData,
                CuckooFilter as ProtoCuckooFilter, SubscribeRequest,
                SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
                SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
                SubscribeRequestFilterBlockFooter, SubscribeRequestFilterEntry,
                SubscribeRequestFilterTransactions,
            },
        },
    };

    /// The 32 bytes that the memcmp filters and the matching account data use.
    const MEMCMP_BYTES: [u8; 32] = [7; 32];

    /// The receiving half of a client's Subscribe queue.
    type SubscribeReceiver = LoadAwareReceiver<tonic::Result<FilteredUpdate>>;
    /// What a Subscribe queue yields, with each error status as its message.
    type QueueItems = Vec<Result<FilteredUpdate, String>>;

    /// Copies `subscriber_template` with the filter that `subscribe_request` compiles to, and its
    /// index keys.
    ///
    /// # Arguments
    ///
    /// * `subscriber_template` - The client's indexed subscriber, whose queue the copy shares.
    /// * `subscribe_request` - The request whose filter the copy gets.
    ///
    /// # Returns
    ///
    /// The copy, from [`IndexedSubscriber::with_filter`].
    ///
    /// # Panics
    ///
    /// Panics if the filter of `subscribe_request` has no index keys.
    fn refilter_subscriber(
        subscriber_template: &IndexedSubscriber,
        subscribe_request: &SubscribeRequest,
    ) -> IndexedSubscriber {
        let client_filter = Arc::new(fixtures::filter_from_request(subscribe_request));
        let index_keys = client_filter.index_keys().expect("an indexable filter");
        subscriber_template.with_filter(&client_filter, &index_keys.into())
    }

    /// An indexed client: its indexed subscriber for `subscribe_request`, and its Subscribe queue.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
    /// * `subscribe_request` - The request whose filter the indexed subscriber gets.
    /// * `queue_capacity` - The capacity of the client's Subscribe queue.
    ///
    /// # Returns
    ///
    /// The client's indexed subscriber, and the receiving half of its Subscribe queue.
    ///
    /// # Panics
    ///
    /// Panics if the filter of `subscribe_request` has no index keys.
    fn new_client(
        session_id: usize,
        subscribe_request: &SubscribeRequest,
        queue_capacity: usize,
    ) -> (IndexedSubscriber, SubscribeReceiver) {
        let (stream_tx, stream_rx) = load_aware_channel(queue_capacity, Default::default());
        let subscriber_template =
            IndexedSubscriber::new(session_id, "test", &stream_tx, &CancellationToken::new());
        let indexed_subscriber = refilter_subscriber(&subscriber_template, subscribe_request);
        (indexed_subscriber, stream_rx)
    }

    /// Takes everything `stream_rx` holds now, without waiting.
    ///
    /// # Arguments
    ///
    /// * `stream_rx` - The client's Subscribe queue.
    ///
    /// # Returns
    ///
    /// The items in queue order, with each error status as its message.
    fn received_items(stream_rx: &mut SubscribeReceiver) -> QueueItems {
        std::iter::from_fn(|| stream_rx.recv().now_or_never().flatten())
            .map(|queued_item| {
                queued_item.map_err(|error_status| error_status.message().to_owned())
            })
            .collect()
    }

    /// What the filter of `indexed_subscriber` makes of `sent_messages`, in order.
    ///
    /// # Arguments
    ///
    /// * `indexed_subscriber` - The indexed subscriber whose filter to apply, at the filter's
    ///   commitment level.
    /// * `sent_messages` - The messages sent, in order.
    ///
    /// # Returns
    ///
    /// The updates the client gets, each as an `Ok` item.
    fn expected_updates(
        indexed_subscriber: &IndexedSubscriber,
        sent_messages: &[Message],
    ) -> QueueItems {
        let client_filter = &indexed_subscriber.client_filter;
        let commitment_level = Some(indexed_subscriber.commitment_level());
        sent_messages
            .iter()
            .flat_map(|sent_message| client_filter.get_updates(sent_message, commitment_level))
            .map(Ok)
            .collect()
    }

    /// Sends `message_batch` through `owner_indexes` at processed.
    ///
    /// # Arguments
    ///
    /// * `owner_indexes` - The owner's indexes.
    /// * `message_batch` - The messages to send, as one batch.
    fn send_processed(owner_indexes: &mut OwnerIndexes, message_batch: &[Message]) {
        owner_indexes.send_batch(Processed, Arc::new(message_batch.to_vec()));
    }

    /// An entry message at `entry_slot`.
    ///
    /// # Arguments
    ///
    /// * `entry_slot` - The entry's slot.
    ///
    /// # Returns
    ///
    /// A [`Message::Entry`] with index 0.
    fn entry_message(entry_slot: u64) -> Message {
        Message::Entry(fixtures::message_entry(entry_slot, 0))
    }

    /// A request with one entry filter.
    ///
    /// # Returns
    ///
    /// A request whose filter is indexed under [`IndexKey::Entries`].
    fn entries_request() -> SubscribeRequest {
        SubscribeRequest {
            entry: [("e".to_owned(), Default::default())].into(),
            ..Default::default()
        }
    }

    /// A request with one accounts filter.
    ///
    /// # Arguments
    ///
    /// * `account_pubkeys` - The filter's `account` list.
    /// * `owner_pubkeys` - The filter's `owner` list.
    /// * `memcmp_offset` - Where a memcmp of [`MEMCMP_BYTES`] matches, or [`None`] for no memcmp.
    /// * `cuckoo_pubkeys` - What the filter's cuckoo filter holds, or empty for no cuckoo filter.
    ///
    /// # Returns
    ///
    /// A request with that accounts filter only.
    fn accounts_request(
        account_pubkeys: &[Pubkey],
        owner_pubkeys: &[Pubkey],
        memcmp_offset: Option<u64>,
        cuckoo_pubkeys: &[Pubkey],
    ) -> SubscribeRequest {
        let accounts_filter = SubscribeRequestFilterAccounts {
            account: pubkey_strings(account_pubkeys),
            owner: pubkey_strings(owner_pubkeys),
            filters: memcmp_offset.map(memcmp_filter).into_iter().collect(),
            cuckoo_accounts_filter: (!cuckoo_pubkeys.is_empty())
                .then(|| create_cuckoo(cuckoo_pubkeys)),
            ..Default::default()
        };
        SubscribeRequest {
            accounts: [("a".to_owned(), accounts_filter)].into(),
            ..Default::default()
        }
    }

    /// A request with one transactions filter.
    ///
    /// # Arguments
    ///
    /// * `include_pubkeys` - The filter's `account_include` list.
    /// * `required_pubkeys` - The filter's `account_required` list.
    /// * `token_accounts` - The filter's `token_accounts` mode, or [`None`].
    ///
    /// # Returns
    ///
    /// A request with that transactions filter only.
    fn transactions_request(
        include_pubkeys: &[Pubkey],
        required_pubkeys: &[Pubkey],
        token_accounts: Option<i32>,
    ) -> SubscribeRequest {
        let transactions_filter = SubscribeRequestFilterTransactions {
            account_include: pubkey_strings(include_pubkeys),
            account_required: pubkey_strings(required_pubkeys),
            token_accounts,
            ..Default::default()
        };
        SubscribeRequest {
            transactions: [("t".to_owned(), transactions_filter)].into(),
            ..Default::default()
        }
    }

    /// An accounts filter that matches [`MEMCMP_BYTES`] at `memcmp_offset`.
    ///
    /// # Arguments
    ///
    /// * `memcmp_offset` - Where the bytes are in the account data.
    ///
    /// # Returns
    ///
    /// The memcmp, as one of an accounts filter's `filters`.
    fn memcmp_filter(memcmp_offset: u64) -> SubscribeRequestFilterAccountsFilter {
        let proto_memcmp = SubscribeRequestFilterAccountsFilterMemcmp {
            offset: memcmp_offset,
            data: Some(MemcmpData::Bytes(MEMCMP_BYTES.to_vec())),
        };
        SubscribeRequestFilterAccountsFilter {
            filter: Some(AccountsFilterOneof::Memcmp(proto_memcmp)),
        }
    }

    /// `pubkeys` as base58 strings, as a request names them.
    ///
    /// # Arguments
    ///
    /// * `pubkeys` - The pubkeys to name.
    ///
    /// # Returns
    ///
    /// One base58 string per pubkey, in order.
    fn pubkey_strings(pubkeys: &[Pubkey]) -> Vec<String> {
        pubkeys.iter().map(ToString::to_string).collect()
    }

    /// A proto cuckoo filter that holds `pubkeys`.
    ///
    /// # Arguments
    ///
    /// * `pubkeys` - The pubkeys to insert.
    ///
    /// # Returns
    ///
    /// The cuckoo filter, as a request carries it.
    fn create_cuckoo(pubkeys: &[Pubkey]) -> ProtoCuckooFilter {
        let mut cuckoo_filter =
            CuckooFilter::<[u8; 32]>::with_capacity(pubkeys.len().max(1)).unwrap();
        for pubkey in pubkeys {
            cuckoo_filter.insert(&pubkey.to_bytes()).unwrap();
        }
        ProtoCuckooFilter::from(&cuckoo_filter)
    }

    /// Up to `max_count` pubkeys from `pubkey_pool`, with repeats.
    ///
    /// # Arguments
    ///
    /// * `seeded_rng` - Picks the count and the pubkeys.
    /// * `pubkey_pool` - The pubkeys to pick from.
    /// * `max_count` - The most pubkeys to pick.
    ///
    /// # Returns
    ///
    /// Between 0 and `max_count` pubkeys.
    ///
    /// # Panics
    ///
    /// Panics if `pubkey_pool` is empty and the count is not 0.
    fn pick_pubkeys(
        seeded_rng: &mut fastrand::Rng,
        pubkey_pool: &[Pubkey],
        max_count: usize,
    ) -> Vec<Pubkey> {
        (0..seeded_rng.usize(..=max_count))
            .map(|_| *seeded_rng.choice(pubkey_pool).unwrap())
            .collect()
    }

    /// A random request whose filters name pubkeys from `pubkey_pool`. Some of these requests
    /// have no index keys.
    ///
    /// # Arguments
    ///
    /// * `seeded_rng` - Picks the filters.
    /// * `pubkey_pool` - The pubkeys the filters can name.
    ///
    /// # Returns
    ///
    /// The request, which can ask for nothing.
    ///
    /// # Panics
    ///
    /// Panics if `pubkey_pool` is empty and a filter names a pubkey.
    fn random_request(seeded_rng: &mut fastrand::Rng, pubkey_pool: &[Pubkey]) -> SubscribeRequest {
        let mut subscribe_request = SubscribeRequest::default();
        for filter_number in 0..seeded_rng.usize(..3) {
            let accounts_filter = SubscribeRequestFilterAccounts {
                account: pubkey_strings(&pick_pubkeys(seeded_rng, pubkey_pool, 2)),
                owner: pubkey_strings(&pick_pubkeys(seeded_rng, pubkey_pool, 2)),
                filters: [None, Some(0), Some(32)][seeded_rng.usize(..3)]
                    .map(memcmp_filter)
                    .into_iter()
                    .collect(),
                cuckoo_accounts_filter: (seeded_rng.u8(..4) == 0)
                    .then(|| create_cuckoo(&pick_pubkeys(seeded_rng, pubkey_pool, 2))),
                ..Default::default()
            };
            subscribe_request
                .accounts
                .insert(format!("a{filter_number}"), accounts_filter);
        }
        for filter_number in 0..seeded_rng.usize(..3) {
            let transactions_filter = SubscribeRequestFilterTransactions {
                account_include: pubkey_strings(&pick_pubkeys(seeded_rng, pubkey_pool, 3)),
                account_required: pubkey_strings(&pick_pubkeys(seeded_rng, pubkey_pool, 1)),
                cuckoo_account_include: (seeded_rng.u8(..4) == 0)
                    .then(|| create_cuckoo(&pick_pubkeys(seeded_rng, pubkey_pool, 2))),
                ..Default::default()
            };
            let transaction_filter_maps = [
                &mut subscribe_request.transactions,
                &mut subscribe_request.transactions_status,
            ];
            transaction_filter_maps[seeded_rng.usize(..2)]
                .insert(format!("t{filter_number}"), transactions_filter);
        }
        if seeded_rng.bool() {
            subscribe_request
                .slots
                .insert("s".into(), Default::default());
        }
        if seeded_rng.bool() {
            let entry_filter = SubscribeRequestFilterEntry {
                include_update_parent: Some(seeded_rng.bool()),
            };
            subscribe_request.entry.insert("e".into(), entry_filter);
        }
        if seeded_rng.bool() {
            subscribe_request
                .blocks_meta
                .insert("m".into(), Default::default());
        }
        if seeded_rng.bool() {
            let block_footer_filter = SubscribeRequestFilterBlockFooter {
                include_certificates: Some(seeded_rng.bool()),
            };
            subscribe_request
                .block_footer
                .insert("f".into(), block_footer_filter);
        }
        subscribe_request
    }

    /// A random message of a kind that an index key finds, about pubkeys from `pubkey_pool`.
    ///
    /// # Arguments
    ///
    /// * `seeded_rng` - Picks the kind and the content.
    /// * `pubkey_pool` - The pubkeys an account or a transaction can name.
    ///
    /// # Returns
    ///
    /// An account, transaction, entry, slot, block meta, block footer or UpdateParent message.
    ///
    /// # Panics
    ///
    /// Panics if `pubkey_pool` is empty and the message names a pubkey.
    fn random_message(seeded_rng: &mut fastrand::Rng, pubkey_pool: &[Pubkey]) -> Message {
        let fixed_slot = 100;
        match seeded_rng.u8(..10) {
            0..=2 => {
                let data_halves = [[0; 32], MEMCMP_BYTES];
                let mut account_data = [
                    data_halves[seeded_rng.usize(..2)],
                    data_halves[seeded_rng.usize(..2)],
                ]
                .concat();
                account_data.truncate([0, 40, 64][seeded_rng.usize(..3)]);
                let account_pubkey = *seeded_rng.choice(pubkey_pool).unwrap();
                let owner_pubkey = *seeded_rng.choice(pubkey_pool).unwrap();
                let account_info =
                    fixtures::account_info(account_pubkey, owner_pubkey, account_data, 1, None);
                Message::Account(fixtures::message_account(account_info))
            }
            3 | 4 => Message::Transaction(fixtures::message_transaction(
                Signature::default(),
                pick_pubkeys(seeded_rng, pubkey_pool, 4),
                false,
                Default::default(),
            )),
            5 => entry_message(fixed_slot),
            6 => Message::Slot(Arc::new(fixtures::message_slot(
                fixed_slot,
                SlotStatus::Processed,
            ))),
            7 => Message::BlockMeta(fixtures::message_block_meta(fixed_slot)),
            8 => Message::BlockFooter(fixtures::message_block_footer(fixed_slot, fixed_slot)),
            _ => Message::EntryUpdateParent(Arc::new(MessageEntryUpdateParent {
                update_parent: Default::default(),
                created_at: Default::default(),
            })),
        }
    }

    /// The per-kind index keys, in their sort order.
    const PER_KIND_INDEX_KEYS: [IndexKey; 5] = [
        IndexKey::Slots,
        IndexKey::Entries,
        IndexKey::UpdateParent,
        IndexKey::BlocksMeta,
        IndexKey::BlockFooter,
    ];

    /// A pubkey whose first bytes are `key_number`, so each number gives its own pubkey.
    ///
    /// # Arguments
    ///
    /// * `key_number` - The pubkey's number.
    ///
    /// # Returns
    ///
    /// The pubkey, zero after its first 8 bytes.
    fn numbered_pubkey(key_number: u64) -> Pubkey {
        let mut pubkey_bytes = [0; 32];
        pubkey_bytes[..8].copy_from_slice(&key_number.to_le_bytes());
        Pubkey::new_from_array(pubkey_bytes)
    }

    /// An account index key that no other number gives.
    ///
    /// # Arguments
    ///
    /// * `key_number` - The key's number.
    ///
    /// # Returns
    ///
    /// The [`IndexKey::Account`] key of [`numbered_pubkey`]`(key_number)`.
    fn numbered_index_key(key_number: u64) -> IndexKey {
        IndexKey::Account(numbered_pubkey(key_number))
    }

    /// The candidates of the reference index: the session ids that claim the keys a message carries,
    /// as the session-keyed index found them, with every memcmp key probed.
    #[derive(Default)]
    struct ReferenceKeyIndex {
        claiming_session_ids_by_key: HashMap<IndexKey, Vec<usize>>,
    }

    impl ReferenceKeyIndex {
        fn claim_index_key(&mut self, index_key: IndexKey, session_id: usize) {
            let claiming_session_ids = self.claiming_session_ids_by_key.entry(index_key);
            claiming_session_ids.or_default().push(session_id);
        }

        fn claiming_session_ids(&self, index_key: &IndexKey) -> &[usize] {
            let claiming_session_ids = self.claiming_session_ids_by_key.get(index_key);
            claiming_session_ids.map_or(&[], Vec::as_slice)
        }
    }

    /// Every pair in shard 0, at one of the last four home buckets and with one of two tags, so
    /// probe runs grow long and wrap around.
    fn colliding_pair_hash(table_key: &[u8; 32], claim_role: ClaimRole) -> u64 {
        let spread_bits = u64::from(table_key[0] ^ table_key[31] ^ claim_role as u8);
        let home_bits = ((1_u64 << 40) - 1) - (spread_bits & 3);
        home_bits | (((spread_bits >> 2) & 1) << 57)
    }

    /// The sorted session ids of the live subscribers in the slots the message's probes find.
    fn candidate_session_ids(
        level_index: &mut LevelIndex,
        delivered_message: &Message,
    ) -> Vec<usize> {
        let mut candidate_session_ids = Vec::new();
        if level_index
            .key_claim_index
            .gather_hit_slots(delivered_message)
        {
            for hit_word_number in 0..level_index.key_claim_index.hit_word_count() {
                let mut hit_word = level_index.key_claim_index.take_hit_word(hit_word_number);
                while hit_word != 0 {
                    let hit_slot = hit_word_number * 64 + hit_word.trailing_zeros() as usize;
                    hit_word &= hit_word - 1;
                    if let Some(live_subscriber) = &level_index.slot_subscribers[hit_slot] {
                        candidate_session_ids.push(live_subscriber.session_id);
                    }
                }
            }
        }
        candidate_session_ids.sort_unstable();
        candidate_session_ids
    }

    /// What [`KeyClaimIndex::slots_claiming`] gives for a key the slots claim: the slots, sorted, or
    /// for a memcmp gate or an ungated memcmp key one [`u32::MAX`] per claim.
    fn expected_claim_view(index_key: &IndexKey, claiming_slots: &[u32]) -> Vec<u32> {
        let mut claim_view = claiming_slots.to_vec();
        if matches!(index_key, IndexKey::MemcmpGate(_) | IndexKey::UngatedMemcmp) {
            claim_view.fill(u32::MAX);
        }
        claim_view.sort_unstable();
        claim_view
    }

    /// Index limits under which every slice is due at once and runs one index step of up to
    /// [`INDEX_KEYS_PER_STEP`] keys, so each owner pass runs one step.
    pub const STEP_EVERY_PASS_LIMITS: IndexWorkLimits = IndexWorkLimits {
        slice_interval: Duration::ZERO,
        slice_budget: Duration::ZERO,
        keys_per_step: INDEX_KEYS_PER_STEP,
    };

    /// Index limits under which every slice is due at once and runs one index step of one key.
    pub const SINGLE_KEY_STEP_LIMITS: IndexWorkLimits = IndexWorkLimits {
        slice_interval: Duration::ZERO,
        slice_budget: Duration::ZERO,
        keys_per_step: NonZeroUsize::MIN,
    };

    /// An owner's indexes on broadcast channels of `broadcast_capacity` batches each.
    ///
    /// # Arguments
    ///
    /// * `broadcast_capacity` - The capacity of each broadcast channel.
    /// * `index_work_limits` - The owner's index limits.
    ///
    /// # Returns
    ///
    /// The owner's indexes, with no indexed subscriber yet.
    fn owner_indexes_with(
        broadcast_capacity: usize,
        index_work_limits: IndexWorkLimits,
    ) -> OwnerIndexes {
        let broadcast_channels =
            SubscriberChannels::new(broadcast_capacity, broadcast_capacity, broadcast_capacity);
        let mut owner_indexes = OwnerIndexes::new(broadcast_channels);
        owner_indexes.index_work_limits = index_work_limits;
        owner_indexes
    }

    /// Takes in an [`IndexChange::Prepare`] of `prepared_subscriber`, as a client sends it.
    ///
    /// # Arguments
    ///
    /// * `owner_indexes` - The owner's indexes.
    /// * `prepared_subscriber` - The indexed subscriber to stage.
    ///
    /// # Returns
    ///
    /// The receiver the owner answers on once every key of the indexed subscriber is claimed.
    fn prepare_subscriber(
        owner_indexes: &mut OwnerIndexes,
        prepared_subscriber: IndexedSubscriber,
    ) -> oneshot::Receiver<()> {
        let (ready_tx, ready_rx) = oneshot::channel();
        owner_indexes.take_index_change(IndexChange::Prepare(prepared_subscriber, ready_tx));
        ready_rx
    }

    /// Takes in an [`IndexChange::Leave`] of `session_id` with no receiver request.
    ///
    /// # Arguments
    ///
    /// * `owner_indexes` - The owner's indexes.
    /// * `session_id` - The session id that leaves.
    fn leave_index(owner_indexes: &mut OwnerIndexes, session_id: usize) {
        let client_leave = IndexLeave {
            session_id,
            receiver_request: None,
        };
        owner_indexes.take_index_change(IndexChange::Leave(client_leave));
    }

    /// `index_keys` as one key list, sorted and without repeats, as [`Filter::index_keys`] gives
    /// them.
    ///
    /// # Arguments
    ///
    /// * `index_keys` - The index keys.
    ///
    /// # Returns
    ///
    /// The key list.
    fn sorted_key_list(index_keys: impl IntoIterator<Item = IndexKey>) -> Arc<[IndexKey]> {
        let mut sorted_index_keys = index_keys.into_iter().collect::<Vec<_>>();
        sorted_index_keys.sort_unstable();
        sorted_index_keys.dedup();
        sorted_index_keys.into()
    }

    /// A client whose indexed subscriber has `index_keys` and the default filter, which matches
    /// nothing.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
    /// * `index_keys` - The indexed subscriber's key list.
    ///
    /// # Returns
    ///
    /// The client's indexed subscriber, and the receiving half of its Subscribe queue.
    fn keyed_client(
        session_id: usize,
        index_keys: &Arc<[IndexKey]>,
    ) -> (IndexedSubscriber, SubscribeReceiver) {
        let (stream_tx, stream_rx) = load_aware_channel(16, Default::default());
        let subscriber_template =
            IndexedSubscriber::new(session_id, "test", &stream_tx, &CancellationToken::new());
        (
            subscriber_template.with_filter(&Arc::default(), index_keys),
            stream_rx,
        )
    }

    /// How many slots opened for the session claim a keyed or per-kind index key.
    fn claim_count(level_index: &LevelIndex, index_key: &IndexKey, session_id: usize) -> usize {
        let claiming_slots = level_index.key_claim_index.slots_claiming(index_key);
        claiming_slots
            .iter()
            .filter(|claiming_slot| {
                level_index.slot_session_ids[**claiming_slot as usize] == session_id
            })
            .count()
    }

    /// How many keys the staged subscriber of `session_id` has claimed.
    ///
    /// # Arguments
    ///
    /// * `owner_indexes` - The owner's indexes.
    /// * `session_id` - The session id of the staged subscriber.
    ///
    /// # Returns
    ///
    /// The staged subscriber's claimed key count.
    ///
    /// # Panics
    ///
    /// Panics if the session has no staged subscriber.
    fn staged_key_count(owner_indexes: &OwnerIndexes, session_id: usize) -> usize {
        owner_indexes.staged_subscribers_by_session_id[&session_id].claimed_key_count
    }

    /// Runs index steps until no index work is left.
    ///
    /// # Arguments
    ///
    /// * `owner_indexes` - The owner's indexes.
    ///
    /// # Returns
    ///
    /// How many steps did work.
    fn run_every_index_step(owner_indexes: &mut OwnerIndexes) -> usize {
        std::iter::from_fn(|| {
            (owner_indexes.advance_index_step() != IndexStep::NoWorkLeft).then_some(())
        })
        .count()
    }

    /// Each slot is live, staged, retired with claims to release, or free, and only one of them, and
    /// each level holds exactly the claims its slots made.
    fn assert_claims_match_subscribers(owner_indexes: &OwnerIndexes) {
        for (level_number, level_index) in owner_indexes.level_indexes.iter().enumerate() {
            let mut expected_claims = HashMap::<IndexKey, Vec<u32>>::default();
            let mut slot_states = vec![None; level_index.slot_subscribers.len()];
            let mut add_claims = |claiming_slot: u32, claimed_index_keys: &[IndexKey]| {
                for index_key in claimed_index_keys {
                    expected_claims
                        .entry(*index_key)
                        .or_default()
                        .push(claiming_slot);
                }
            };
            let mut mark_slot = |marked_slot: u32, slot_state: &'static str| {
                let earlier_state = slot_states[marked_slot as usize].replace(slot_state);
                assert_eq!(
                    earlier_state, None,
                    "level {level_number}, slot {marked_slot}"
                );
            };
            for (session_id, live_slot) in &level_index.live_slots_by_session_id {
                let live_subscriber = level_index.slot_subscribers[*live_slot as usize]
                    .as_ref()
                    .expect("a live slot holds its subscriber");
                assert_eq!(live_subscriber.session_id, *session_id);
                assert_eq!(
                    level_index.slot_session_ids[*live_slot as usize],
                    *session_id
                );
                mark_slot(*live_slot, "live");
                add_claims(*live_slot, &live_subscriber.index_keys);
            }
            for (session_id, staged_subscriber) in &owner_indexes.staged_subscribers_by_session_id {
                let prepared_subscriber = &staged_subscriber.prepared_subscriber;
                if prepared_subscriber.commitment_level() as usize == level_number {
                    let staged_slot = staged_subscriber.staged_slot;
                    assert_eq!(
                        level_index.slot_session_ids[staged_slot as usize],
                        *session_id
                    );
                    mark_slot(staged_slot, "staged");
                    let claimed_index_keys =
                        &prepared_subscriber.index_keys[..staged_subscriber.claimed_key_count];
                    add_claims(staged_slot, claimed_index_keys);
                }
            }
            for claims_to_release in &level_index.claims_to_release {
                mark_slot(claims_to_release.retired_slot, "retired");
                let unreleased_keys = &claims_to_release.index_keys
                    [claims_to_release.released_key_count..claims_to_release.claimed_key_count];
                add_claims(claims_to_release.retired_slot, unreleased_keys);
            }
            for free_slot in &level_index.free_slots {
                mark_slot(*free_slot, "free");
            }
            for (slot_number, slot_state) in slot_states.iter().enumerate() {
                assert!(
                    slot_state.is_some(),
                    "level {level_number}, slot {slot_number}"
                );
                let holds_subscriber = level_index.slot_subscribers[slot_number].is_some();
                assert_eq!(holds_subscriber, *slot_state == Some("live"));
            }
            for (index_key, expected_slots) in &expected_claims {
                assert_eq!(
                    level_index.key_claim_index.slots_claiming(index_key),
                    expected_claim_view(index_key, expected_slots),
                    "level {level_number}, {index_key:?}"
                );
            }
            let mut expected_keys = expected_claims.into_keys().collect::<Vec<_>>();
            expected_keys.sort_unstable();
            assert_eq!(
                level_index.key_claim_index.held_index_keys(),
                expected_keys,
                "level {level_number}"
            );
        }
    }

    /// No subscriber, staging, claim or claimed key is left at any level, and every slot is free.
    fn assert_owner_holds_nothing(owner_indexes: &OwnerIndexes) {
        assert!(owner_indexes.staged_subscribers_by_session_id.is_empty());
        assert!(owner_indexes.staging_order.is_empty());
        assert!(!owner_indexes.has_index_work());
        for level_index in &owner_indexes.level_indexes {
            assert!(level_index.live_slots_by_session_id.is_empty());
            assert!(level_index.claims_to_release.is_empty());
            assert!(level_index.key_claim_index.held_index_keys().is_empty());
            assert!(!level_index.key_claim_index.holds_slot_claims());
            let claimed_key_table = level_index.key_claim_index.claimed_key_table();
            assert_eq!(claimed_key_table.held_bucket_count(), 0);
            assert!(level_index.slot_subscribers.iter().all(Option::is_none));
            assert_eq!(
                level_index.free_slots.len(),
                level_index.slot_subscribers.len()
            );
        }
    }

    /// Forwards each allocation and free to [`System`], and counts it in
    /// [`THREAD_ALLOCATOR_CALLS`]. It is the allocator of the whole lib test binary.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let allocator_calls = allocator_calls_during(|| drop(black_box(Box::new(1_u64))));
    /// assert_eq!(allocator_calls, 2);
    /// ```
    struct CountingAllocator;

    /// Makes [`CountingAllocator`] the allocator of the lib test binary.
    #[global_allocator]
    static COUNTING_ALLOCATOR: CountingAllocator = CountingAllocator;

    thread_local! {
        /// The allocations and frees made on this thread so far.
        static THREAD_ALLOCATOR_CALLS: Cell<usize> = const { Cell::new(0) };
    }

    // SAFETY: each method passes its arguments to `System` unchanged.
    unsafe impl GlobalAlloc for CountingAllocator {
        /// Counts the call, then allocates the block with [`System`].
        ///
        /// # Arguments
        ///
        /// * `block_layout` - The size and alignment of the block.
        ///
        /// # Returns
        ///
        /// The block, or null if [`System`] fails to allocate it.
        ///
        /// # Safety
        ///
        /// `block_layout` has a non-zero size, as [`GlobalAlloc::alloc`] requires.
        unsafe fn alloc(&self, block_layout: Layout) -> *mut u8 {
            THREAD_ALLOCATOR_CALLS.set(THREAD_ALLOCATOR_CALLS.get() + 1);
            System.alloc(block_layout)
        }

        /// Counts the call, then frees the block with [`System`].
        ///
        /// # Arguments
        ///
        /// * `block_pointer` - The block to free.
        /// * `block_layout` - The layout the block was allocated with.
        ///
        /// # Safety
        ///
        /// This allocator allocated `block_pointer` with `block_layout`, as
        /// [`GlobalAlloc::dealloc`] requires.
        unsafe fn dealloc(&self, block_pointer: *mut u8, block_layout: Layout) {
            THREAD_ALLOCATOR_CALLS.set(THREAD_ALLOCATOR_CALLS.get() + 1);
            System.dealloc(block_pointer, block_layout)
        }
    }

    /// Runs `counted_work` and counts the allocator calls it makes on this thread.
    ///
    /// # Arguments
    ///
    /// * `counted_work` - The work to count.
    ///
    /// # Returns
    ///
    /// The allocations and frees made on this thread while `counted_work` ran. A reallocation
    /// counts as one of each.
    pub fn allocator_calls_during(counted_work: impl FnOnce()) -> usize {
        let calls_before = THREAD_ALLOCATOR_CALLS.get();
        counted_work();
        THREAD_ALLOCATOR_CALLS.get() - calls_before
    }

    #[test]
    fn each_indexed_client_gets_what_its_filter_makes_of_every_message() {
        let pubkey_pool = fixtures::deterministic_pubkeys(1, 8);
        let mut indexed_count = 0;
        for rng_seed in 0..60 {
            let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
            let mut owner_indexes = OwnerIndexes::new(SubscriberChannels::new(1, 1, 1));
            let mut indexed_clients = Vec::new();
            for session_id in 0..16 {
                let subscribe_request = random_request(&mut seeded_rng, &pubkey_pool);
                let client_filter = fixtures::filter_from_request(&subscribe_request);
                if client_filter.index_keys().is_some() {
                    let (indexed_subscriber, stream_rx) =
                        new_client(session_id, &subscribe_request, MAX_CAPACITY);
                    owner_indexes
                        .apply_index_change(IndexChange::Join(indexed_subscriber.clone(), None));
                    indexed_clients.push((indexed_subscriber, stream_rx));
                }
            }
            indexed_count += indexed_clients.len();
            let sent_messages = (0..256)
                .map(|_| random_message(&mut seeded_rng, &pubkey_pool))
                .collect::<Vec<_>>();
            for message_batch in sent_messages.chunks(32) {
                send_processed(&mut owner_indexes, message_batch);
            }
            for (indexed_subscriber, stream_rx) in &mut indexed_clients {
                assert_eq!(
                    received_items(stream_rx),
                    expected_updates(indexed_subscriber, &sent_messages),
                    "seed {rng_seed}"
                );
            }
        }
        assert!(indexed_count > 500, "{indexed_count}");
    }

    /// A bank of `message_count` messages from [`random_message`].
    ///
    /// # Arguments
    ///
    /// * `seeded_rng` - Picks the messages.
    /// * `pubkey_pool` - The pubkeys the messages can name.
    /// * `message_count` - How many messages the bank holds.
    ///
    /// # Returns
    ///
    /// The bank's messages, shared as replay items share them.
    fn random_bank(
        seeded_rng: &mut fastrand::Rng,
        pubkey_pool: &[Pubkey],
        message_count: usize,
    ) -> Arc<Vec<Message>> {
        let bank_messages = (0..message_count).map(|_| random_message(seeded_rng, pubkey_pool));
        Arc::new(bank_messages.collect())
    }

    /// The positions of the messages of `bank_messages` that carry one of
    /// `looked_up_index_keys`, found by a pass over every message.
    ///
    /// # Arguments
    ///
    /// * `bank_messages` - The bank's messages.
    /// * `looked_up_index_keys` - The index keys to look for.
    ///
    /// # Returns
    ///
    /// The positions, ascending.
    fn positions_carrying_index_keys(
        bank_messages: &[Message],
        looked_up_index_keys: &[IndexKey],
    ) -> Vec<u32> {
        let carries_looked_up_key = |bank_message: &Message| {
            let mut carries_index_key = false;
            for_each_index_key(bank_message, |index_key| {
                carries_index_key |= looked_up_index_keys.contains(&index_key);
            });
            carries_index_key
        };
        (0..bank_messages.len())
            .filter(|message_position| carries_looked_up_key(&bank_messages[*message_position]))
            .map(|message_position| message_position as u32)
            .collect()
    }

    /// Probes `bank_key_index` for `probed_index_keys`, as a replay with those index keys
    /// does.
    ///
    /// # Arguments
    ///
    /// * `bank_key_index` - The bank's index.
    /// * `probed_index_keys` - The index keys to look up.
    /// * `candidate_positions` - Gets what [`BankKeyIndex::collect_candidate_positions`]
    ///   finds.
    fn probe_index_keys(
        bank_key_index: &BankKeyIndex,
        probed_index_keys: &[IndexKey],
        candidate_positions: &mut Vec<u32>,
    ) {
        let index_key_hashes: Vec<u64> = probed_index_keys
            .iter()
            .map(BankKeyIndex::index_key_hash)
            .collect();
        bank_key_index.collect_candidate_positions(&index_key_hashes, candidate_positions);
    }

    /// What `client_filter` makes of a replay of `replay_items` to a client with `index_keys`,
    /// with no cut and nothing on the live receiver.
    ///
    /// # Arguments
    ///
    /// * `client_filter` - The client's filter, applied at its commitment level.
    /// * `replay_items` - The replay's items.
    /// * `index_keys` - The client's index keys.
    ///
    /// # Returns
    ///
    /// The updates, in the order the client gets them, each as an `Ok` item.
    fn replayed_updates(
        client_filter: &Filter,
        replay_items: Vec<ReplayResponseMessageType>,
        index_keys: &[IndexKey],
    ) -> QueueItems {
        let commitment_level = client_filter.get_commitment_level();
        let client_replay = Replay {
            messages: replay_items,
            bank_id_span: None,
            cut: ReplayCut {
                last_taken_in: None,
                last_sent: None,
                next_taken_in: None,
            },
        };
        let (_live_tx, live_rx) = broadcast::channel::<BroadcastedMessage>(1);
        let mut replay_phase =
            ReplayPhase::new(live_rx, commitment_level, client_replay, Some(index_keys)).unwrap();
        let mut client_updates = QueueItems::new();
        while let Some(replayed_message) = replay_phase
            .next()
            .now_or_never()
            .expect("a replay with no cut never waits")
            .unwrap()
        {
            let message_updates =
                client_filter.get_updates(&replayed_message, Some(commitment_level));
            client_updates.extend(message_updates.into_iter().map(Ok));
        }
        client_updates
    }

    /// A bank key index finds, for one index key or two, the positions of the bank's
    /// messages that carry one of them, ascending and each once, as a pass over the bank finds
    /// them. A key no message carries finds nothing.
    #[test]
    fn a_bank_key_index_finds_every_message_that_carries_a_key() {
        let pubkey_pool = fixtures::deterministic_pubkeys(1, 8);
        let absent_pubkey = fixtures::deterministic_pubkeys(2, 1)[0];
        let absent_index_keys = [
            IndexKey::Account(absent_pubkey),
            IndexKey::Memcmp(32, [1; 32]),
            IndexKey::Transaction(absent_pubkey),
        ];
        let mut scratch_postings = Vec::new();
        let mut candidate_positions = Vec::new();
        for rng_seed in 0..60 {
            let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
            let bank_messages = random_bank(&mut seeded_rng, &pubkey_pool, 256);
            let bank_key_index = BankKeyIndex::new(&bank_messages, &mut scratch_postings);
            let mut carried_index_keys = Vec::new();
            for bank_message in bank_messages.iter() {
                for_each_index_key(bank_message, |index_key| carried_index_keys.push(index_key));
            }
            carried_index_keys.sort_unstable();
            carried_index_keys.dedup();
            for absent_index_key in &absent_index_keys {
                assert!(!carried_index_keys.contains(absent_index_key));
            }
            for probed_index_key in carried_index_keys.iter().chain(&absent_index_keys) {
                let probed_index_keys = [*probed_index_key];
                probe_index_keys(
                    &bank_key_index,
                    &probed_index_keys,
                    &mut candidate_positions,
                );
                assert_eq!(
                    candidate_positions,
                    positions_carrying_index_keys(&bank_messages, &probed_index_keys),
                    "seed {rng_seed}, {probed_index_key:?}"
                );
            }
            for _ in 0..50 {
                let probed_index_keys =
                    [(); 2].map(|_| *seeded_rng.choice(&carried_index_keys).unwrap());
                probe_index_keys(
                    &bank_key_index,
                    &probed_index_keys,
                    &mut candidate_positions,
                );
                assert_eq!(
                    candidate_positions,
                    positions_carrying_index_keys(&bank_messages, &probed_index_keys),
                    "seed {rng_seed}, {probed_index_keys:?}"
                );
            }
        }
    }

    /// A replay to a client with index keys hands out, from each indexed sealed bank, only what
    /// the keys find, and the client gets what its filter makes of every message the replay
    /// holds, in order. The items hold an indexed bank split around a message, a second indexed
    /// bank, an unindexed bank, an unsealed batch and an empty range of an indexed bank.
    #[test]
    fn a_replay_with_index_keys_hands_out_what_a_full_scan_hands_out() {
        let pubkey_pool = fixtures::deterministic_pubkeys(1, 8);
        let mut scratch_postings = Vec::new();
        let mut compared_client_count = 0;
        for rng_seed in 0..60 {
            let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
            let client_filters: Vec<Filter> = (0..16)
                .map(|_| random_request(&mut seeded_rng, &pubkey_pool))
                .map(|subscribe_request| fixtures::filter_from_request(&subscribe_request))
                .collect();
            let [split_bank, second_bank, unindexed_bank, unsealed_messages, empty_range_bank] =
                [64, 64, 32, 32, 16]
                    .map(|message_count| random_bank(&mut seeded_rng, &pubkey_pool, message_count));
            let split_position = seeded_rng.usize(..=split_bank.len());
            let empty_range_position = seeded_rng.usize(..=empty_range_bank.len());
            let message_between_parts = random_message(&mut seeded_rng, &pubkey_pool);
            let [split_bank_index, second_bank_index, empty_range_bank_index] =
                [&split_bank, &second_bank, &empty_range_bank].map(|indexed_bank| {
                    Arc::new(BankKeyIndex::new(indexed_bank, &mut scratch_postings))
                });
            let replay_items = || {
                vec![
                    ReplayResponseMessageType::SealedBank(
                        Arc::clone(&split_bank),
                        0..split_position,
                        Some(Arc::clone(&split_bank_index)),
                    ),
                    ReplayResponseMessageType::Single(message_between_parts.clone()),
                    ReplayResponseMessageType::SealedBank(
                        Arc::clone(&split_bank),
                        split_position..split_bank.len(),
                        Some(Arc::clone(&split_bank_index)),
                    ),
                    ReplayResponseMessageType::SealedBank(
                        Arc::clone(&second_bank),
                        0..second_bank.len(),
                        Some(Arc::clone(&second_bank_index)),
                    ),
                    ReplayResponseMessageType::SealedBank(
                        Arc::clone(&unindexed_bank),
                        0..unindexed_bank.len(),
                        None,
                    ),
                    ReplayResponseMessageType::Batch(
                        Arc::clone(&unsealed_messages),
                        0..unsealed_messages.len(),
                    ),
                    ReplayResponseMessageType::SealedBank(
                        Arc::clone(&empty_range_bank),
                        empty_range_position..empty_range_position,
                        Some(Arc::clone(&empty_range_bank_index)),
                    ),
                ]
            };
            let scanned_messages: Vec<Message> = replay_items().iter().flatten().cloned().collect();
            for client_filter in &client_filters {
                let Some(index_keys) = client_filter.index_keys() else {
                    continue;
                };
                let commitment_level = Some(client_filter.get_commitment_level());
                let full_scan_updates: QueueItems = scanned_messages
                    .iter()
                    .flat_map(|scanned_message| {
                        client_filter.get_updates(scanned_message, commitment_level)
                    })
                    .map(Ok)
                    .collect();
                assert_eq!(
                    replayed_updates(client_filter, replay_items(), &index_keys),
                    full_scan_updates,
                    "seed {rng_seed}"
                );
                compared_client_count += 1;
            }
        }
        assert!(compared_client_count > 500, "{compared_client_count}");
    }

    /// The index keys of `listed_pubkeys`, sorted as [`Filter::index_keys`] returns them.
    ///
    /// # Arguments
    ///
    /// * `listed_pubkeys` - The listed pubkeys, each once.
    /// * `index_key_of_pubkey` - The kind of index key a pubkey makes.
    ///
    /// # Returns
    ///
    /// One index key per pubkey, sorted.
    fn sorted_index_keys(
        listed_pubkeys: &[Pubkey],
        index_key_of_pubkey: fn(Pubkey) -> IndexKey,
    ) -> Vec<IndexKey> {
        let mut index_keys: Vec<IndexKey> = listed_pubkeys
            .iter()
            .copied()
            .map(index_key_of_pubkey)
            .collect();
        index_keys.sort_unstable();
        index_keys
    }

    /// A transactions filter and a transactions status filter that list the same 5,000 keys are
    /// indexed with those 5,000 keys, since a key that several parts list counts once toward
    /// [`MAX_INDEX_KEYS`].
    #[test]
    fn transaction_filters_that_list_the_same_keys_count_them_once() {
        let include_pubkeys = (0..5_000).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        let transactions_filter = SubscribeRequestFilterTransactions {
            account_include: pubkey_strings(&include_pubkeys),
            ..Default::default()
        };
        let subscribe_request = SubscribeRequest {
            transactions: [("t".to_owned(), transactions_filter.clone())].into(),
            transactions_status: [("s".to_owned(), transactions_filter)].into(),
            ..Default::default()
        };
        assert_eq!(
            fixtures::filter_from_request(&subscribe_request).index_keys(),
            Some(sorted_index_keys(&include_pubkeys, IndexKey::Transaction))
        );
    }

    /// Two account filters that list the same 5,000 accounts, or the same 5,000 owners, are
    /// indexed with those 5,000 keys, since a key that several parts list counts once toward
    /// [`MAX_INDEX_KEYS`].
    #[test]
    fn account_filters_that_list_the_same_accounts_or_owners_count_them_once() {
        let listed_pubkeys = (0..5_000).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        let accounts_filter = SubscribeRequestFilterAccounts {
            account: pubkey_strings(&listed_pubkeys),
            ..Default::default()
        };
        let owners_filter = SubscribeRequestFilterAccounts {
            owner: pubkey_strings(&listed_pubkeys),
            ..Default::default()
        };
        let account_index_keys = sorted_index_keys(&listed_pubkeys, IndexKey::Account);
        let owner_index_keys = sorted_index_keys(&listed_pubkeys, IndexKey::Owner);
        let index_key_cases = [
            (accounts_filter, account_index_keys),
            (owners_filter, owner_index_keys),
        ];
        for (repeated_filter, expected_index_keys) in index_key_cases {
            let subscribe_request = SubscribeRequest {
                accounts: [
                    ("a1".to_owned(), repeated_filter.clone()),
                    ("a2".to_owned(), repeated_filter),
                ]
                .into(),
                ..Default::default()
            };
            assert_eq!(
                fixtures::filter_from_request(&subscribe_request).index_keys(),
                Some(expected_index_keys)
            );
        }
    }

    /// A filter with [`MAX_INDEX_KEYS`] distinct keys is indexed, and one with a key more is not,
    /// whether one part lists the keys, or two parts that share a third of them do.
    #[test]
    fn a_filter_is_indexed_with_8192_distinct_keys_and_not_with_8193() {
        let listed_pubkeys = (0..=MAX_INDEX_KEYS)
            .map(|_| Pubkey::new_unique())
            .collect::<Vec<_>>();
        let transactions_filter = |include_pubkeys: &[Pubkey]| SubscribeRequestFilterTransactions {
            account_include: pubkey_strings(include_pubkeys),
            ..Default::default()
        };
        for distinct_key_count in [MAX_INDEX_KEYS, MAX_INDEX_KEYS + 1] {
            let distinct_pubkeys = &listed_pubkeys[..distinct_key_count];
            let one_part_request = transactions_request(distinct_pubkeys, &[], None);
            let shared_keys_start = distinct_key_count / 3;
            let shared_keys_end = distinct_key_count - shared_keys_start;
            let two_part_request = SubscribeRequest {
                transactions: [(
                    "t".to_owned(),
                    transactions_filter(&distinct_pubkeys[..shared_keys_end]),
                )]
                .into(),
                transactions_status: [(
                    "s".to_owned(),
                    transactions_filter(&distinct_pubkeys[shared_keys_start..]),
                )]
                .into(),
                ..Default::default()
            };
            let expected_index_keys = (distinct_key_count <= MAX_INDEX_KEYS)
                .then(|| sorted_index_keys(distinct_pubkeys, IndexKey::Transaction));
            for subscribe_request in [one_part_request, two_part_request] {
                assert_eq!(
                    fixtures::filter_from_request(&subscribe_request).index_keys(),
                    expected_index_keys,
                    "{distinct_key_count} distinct keys"
                );
            }
        }
    }

    #[test]
    fn indexes_only_filters_whose_keys_find_every_match() {
        let [account_pubkey, owner_pubkey] = [Pubkey::new_unique(), Pubkey::new_unique()];
        let too_many_pubkeys = (0..=MAX_INDEX_KEYS)
            .map(|_| Pubkey::new_unique())
            .collect::<Vec<_>>();
        let blocks_request = SubscribeRequest {
            blocks: [("b".to_owned(), Default::default())].into(),
            ..Default::default()
        };
        let index_key_cases = [
            (
                accounts_request(&[account_pubkey], &[owner_pubkey], None, &[]),
                Some(vec![IndexKey::Account(account_pubkey)]),
            ),
            (
                accounts_request(&[account_pubkey], &[owner_pubkey], None, &[account_pubkey]),
                Some(vec![IndexKey::Owner(owner_pubkey)]),
            ),
            (
                accounts_request(&[], &too_many_pubkeys, Some(32), &[]),
                Some(vec![
                    IndexKey::Memcmp(32, MEMCMP_BYTES),
                    IndexKey::UngatedMemcmp,
                ]),
            ),
            (
                accounts_request(&[], &[owner_pubkey], Some(0), &[]),
                Some(vec![
                    IndexKey::Memcmp(0, MEMCMP_BYTES),
                    IndexKey::MemcmpGate(owner_pubkey),
                ]),
            ),
            (
                accounts_request(&[], &[], Some(32), &[account_pubkey]),
                Some(vec![
                    IndexKey::Memcmp(32, MEMCMP_BYTES),
                    IndexKey::UngatedMemcmp,
                ]),
            ),
            (
                accounts_request(&[], &[owner_pubkey], Some(8), &[]),
                Some(vec![IndexKey::Owner(owner_pubkey)]),
            ),
            (
                transactions_request(&[account_pubkey], &too_many_pubkeys, None),
                Some(vec![IndexKey::Transaction(
                    *too_many_pubkeys.iter().min().unwrap(),
                )]),
            ),
            (SubscribeRequest::default(), Some(vec![])),
            (accounts_request(&[], &[], None, &[account_pubkey]), None),
            (accounts_request(&[], &[], None, &[]), None),
            (accounts_request(&too_many_pubkeys, &[], None, &[]), None),
            (transactions_request(&[account_pubkey], &[], Some(0)), None),
            (transactions_request(&[], &[], None), None),
            (blocks_request, None),
        ];
        for (case_index, (subscribe_request, expected_index_keys)) in
            index_key_cases.into_iter().enumerate()
        {
            assert_eq!(
                fixtures::filter_from_request(&subscribe_request).index_keys(),
                expected_index_keys,
                "case {case_index}"
            );
        }
    }

    /// A preparation delivers nothing. The hand-over that follows delivers what the receiver got
    /// since, and the leave answers with a receiver subscribed right after the last indexed send.
    #[test]
    fn a_join_and_a_leave_land_between_two_sends() {
        let mut owner_indexes = OwnerIndexes::new(SubscriberChannels::new(16, 16, 16));
        let (indexed_subscriber, mut stream_rx) = new_client(1, &entries_request(), 16);
        let broadcast_rx = owner_indexes.broadcast_channels.subscribe(Processed);
        let message_batches = [1, 2, 3, 4].map(|entry_slot| vec![entry_message(entry_slot)]);
        let (ready_tx, mut ready_rx) = oneshot::channel();
        owner_indexes
            .apply_index_change(IndexChange::Prepare(indexed_subscriber.clone(), ready_tx));
        assert_eq!(ready_rx.try_recv(), Ok(()));
        send_processed(&mut owner_indexes, &message_batches[0]);
        send_processed(&mut owner_indexes, &message_batches[1]);
        let index_change = IndexChange::Join(indexed_subscriber.clone(), Some(broadcast_rx));
        owner_indexes.apply_index_change(index_change);
        send_processed(&mut owner_indexes, &message_batches[2]);
        let (answer_tx, mut answer_rx) = oneshot::channel();
        let client_leave = IndexLeave {
            session_id: 1,
            receiver_request: Some((Processed, answer_tx)),
        };
        owner_indexes.apply_index_change(IndexChange::Leave(client_leave));
        send_processed(&mut owner_indexes, &message_batches[3]);
        assert_eq!(
            received_items(&mut stream_rx),
            expected_updates(&indexed_subscriber, &message_batches[..3].concat())
        );
        let mut broadcast_rx = answer_rx.try_recv().unwrap();
        assert_eq!(
            broadcast_rx.try_recv().unwrap(),
            Arc::new(message_batches[3].clone())
        );
        assert!(broadcast_rx.is_empty());
    }

    /// A hand-over of a receiver that lagged disconnects the client with the lag status, and
    /// releases the claims the join made.
    #[test]
    fn a_join_whose_receiver_lagged_disconnects_the_client() {
        let mut owner_indexes = OwnerIndexes::new(SubscriberChannels::new(2, 2, 2));
        let (indexed_subscriber, mut stream_rx) = new_client(1, &entries_request(), 16);
        let broadcast_rx = owner_indexes.broadcast_channels.subscribe(Processed);
        for entry_slot in 0..3 {
            send_processed(&mut owner_indexes, &[entry_message(entry_slot)]);
        }
        let index_change = IndexChange::Join(indexed_subscriber.clone(), Some(broadcast_rx));
        owner_indexes.apply_index_change(index_change);
        assert_owner_holds_nothing(&owner_indexes);
        let lagged_error = Err("lagged to receive geyser messages".to_owned());
        assert_eq!(received_items(&mut stream_rx), [lagged_error]);
        assert_eq!(
            indexed_subscriber.disconnect_reason(),
            Some("client_broadcast_lag")
        );
    }

    /// A join without a receiver replaces a held subscriber's filter, and when the index keys
    /// change, the index then holds the new keys only.
    #[test]
    fn a_join_without_a_receiver_swaps_or_reindexes_the_subscriber() {
        let mut owner_indexes = OwnerIndexes::new(SubscriberChannels::new(16, 16, 16));
        let [first_pubkey, second_pubkey] = [Pubkey::new_unique(), Pubkey::new_unique()];
        let (indexed_subscriber, mut stream_rx) =
            new_client(1, &accounts_request(&[first_pubkey], &[], Some(0), &[]), 16);
        owner_indexes.apply_index_change(IndexChange::Join(indexed_subscriber.clone(), None));
        let message_batch = [first_pubkey, second_pubkey].map(|account_pubkey| {
            Message::Account(fixtures::simple_message_account(
                account_pubkey,
                Pubkey::default(),
            ))
        });
        for (subscribe_request, indexed_key_count) in [
            (accounts_request(&[first_pubkey], &[], None, &[]), 1),
            (accounts_request(&[second_pubkey], &[], None, &[]), 1),
            (SubscribeRequest::default(), 0),
        ] {
            let refiltered_subscriber =
                refilter_subscriber(&indexed_subscriber, &subscribe_request);
            owner_indexes
                .apply_index_change(IndexChange::Join(refiltered_subscriber.clone(), None));
            send_processed(&mut owner_indexes, &message_batch);
            assert_eq!(
                received_items(&mut stream_rx),
                expected_updates(&refiltered_subscriber, &message_batch)
            );
            let processed_index = &owner_indexes.level_indexes[0];
            let held_index_keys = processed_index.key_claim_index.held_index_keys();
            assert_eq!(held_index_keys.len(), indexed_key_count);
        }
    }

    /// An indexed client whose queue fills gets what fits, then the full-queue status, and its
    /// indexed subscriber leaves with its per-kind claims released at once.
    #[test]
    fn a_full_queue_disconnects_the_client_behind_what_it_holds() {
        let mut owner_indexes = OwnerIndexes::new(SubscriberChannels::new(16, 16, 16));
        let (indexed_subscriber, mut stream_rx) = new_client(1, &entries_request(), 2);
        owner_indexes.apply_index_change(IndexChange::Join(indexed_subscriber.clone(), None));
        let message_batch = [1, 2, 3].map(entry_message);
        send_processed(&mut owner_indexes, &message_batch);
        let mut expected_items = expected_updates(&indexed_subscriber, &message_batch[..2]);
        expected_items.push(Err("lagged to send an update".to_owned()));
        assert_eq!(received_items(&mut stream_rx), expected_items);
        assert_eq!(
            indexed_subscriber.disconnect_reason(),
            Some("client_channel_full")
        );
        assert!(indexed_subscriber.cancellation_token.is_cancelled());
        assert_owner_holds_nothing(&owner_indexes);
    }

    /// A warm level index delivers a batch without an allocator call. Each subscriber is a candidate
    /// for one message, which its filter does not match, or none when its owner gate stays shut.
    #[test]
    fn a_warm_level_index_delivers_a_batch_without_allocating() {
        let one_box_allocator_calls = allocator_calls_during(|| drop(black_box(Box::new(1_u64))));
        assert_eq!(one_box_allocator_calls, 2);
        let mut owner_indexes = OwnerIndexes::new(SubscriberChannels::new(16, 16, 16));
        let _processed_rx = owner_indexes.broadcast_channels.subscribe(Processed);
        let [account_pubkey, owner_pubkey, missing_pubkey] =
            std::array::from_fn(|_| Pubkey::new_unique());
        let slots_request = SubscribeRequest {
            slots: [("s".to_owned(), Default::default())].into(),
            ..Default::default()
        };
        let near_miss_requests = [
            slots_request,
            accounts_request(&[account_pubkey], &[], Some(0), &[]),
            accounts_request(&[], &[missing_pubkey], Some(32), &[]),
            transactions_request(&[], &[account_pubkey, missing_pubkey], None),
        ];
        let mut stream_rxs = Vec::new();
        for session_id in 0..64 {
            let near_miss_request = &near_miss_requests[session_id % near_miss_requests.len()];
            let (indexed_subscriber, stream_rx) = new_client(session_id, near_miss_request, 16);
            owner_indexes.apply_index_change(IndexChange::Join(indexed_subscriber, None));
            stream_rxs.push(stream_rx);
        }
        let slot_message = fixtures::message_slot(100, SlotStatus::FirstShredReceived);
        let account_data = [[0; 32], MEMCMP_BYTES].concat();
        let account_info =
            fixtures::account_info(account_pubkey, owner_pubkey, account_data, 1, None);
        let message_batch = Arc::new(vec![
            Message::Slot(Arc::new(slot_message)),
            Message::Account(fixtures::message_account(account_info)),
            Message::Transaction(fixtures::message_transaction(
                Signature::default(),
                vec![account_pubkey.min(missing_pubkey)],
                false,
                Default::default(),
            )),
        ]);
        owner_indexes.send_batch(Processed, Arc::clone(&message_batch));
        let allocator_calls = allocator_calls_during(|| {
            for _ in 0..32 {
                owner_indexes.send_batch(Processed, Arc::clone(&message_batch));
            }
        });
        assert_eq!(allocator_calls, 0);
        // Each request's indexed subscribers are candidates for one message of each batch.
        let candidate_counts_per_request = [33, 33, 0, 33];
        let processed_index = &owner_indexes.level_indexes[0];
        assert_eq!(processed_index.live_slots_by_session_id.len(), 64);
        for (session_id, live_slot) in &processed_index.live_slots_by_session_id {
            let expected_count =
                candidate_counts_per_request[session_id % near_miss_requests.len()];
            let candidate_count = processed_index.candidate_counts[*live_slot as usize];
            assert_eq!(candidate_count, expected_count, "session {session_id}");
        }
        for stream_rx in &mut stream_rxs {
            assert!(received_items(stream_rx).is_empty());
        }
    }

    /// A warm level index queues each matched update without an allocator call, while every client
    /// queue has room in the block its channel started with.
    #[test]
    fn a_warm_level_index_queues_matched_updates_without_allocating() {
        let mut owner_indexes = OwnerIndexes::new(SubscriberChannels::new(16, 16, 16));
        let mut stream_rxs = Vec::new();
        for session_id in 0..16 {
            let (indexed_subscriber, stream_rx) = new_client(session_id, &entries_request(), 64);
            owner_indexes.apply_index_change(IndexChange::Join(indexed_subscriber, None));
            stream_rxs.push(stream_rx);
        }
        let message_batch = Arc::new(vec![entry_message(1)]);
        owner_indexes.send_batch(Processed, Arc::clone(&message_batch));
        let allocator_calls = allocator_calls_during(|| {
            for _ in 0..8 {
                owner_indexes.send_batch(Processed, Arc::clone(&message_batch));
            }
        });
        assert_eq!(allocator_calls, 0);
        for stream_rx in &mut stream_rxs {
            assert_eq!(received_items(stream_rx).len(), 9);
        }
    }

    /// The candidates cover every exact match, and are the reference index's less those the owner
    /// gate prunes, under the seeded pair hash and under one where every pair collides.
    #[test]
    fn the_candidates_cover_every_exact_match_and_match_the_reference_index() {
        let pubkey_pool = fixtures::deterministic_pubkeys(1, 8);
        let forced_hashes: [Option<ForcedPairHash>; 2] = [None, Some(colliding_pair_hash)];
        let (mut exact_match_count, mut pruned_candidate_count) = (0, 0);
        for forced_pair_hash in forced_hashes {
            for rng_seed in 0..40 {
                let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
                let mut level_index = LevelIndex::default();
                level_index
                    .key_claim_index
                    .claimed_key_table_mut()
                    .forced_pair_hash = forced_pair_hash;
                let mut reference_index = ReferenceKeyIndex::default();
                let mut indexed_subscribers = Vec::new();
                for session_id in 0..24 {
                    let subscribe_request = random_request(&mut seeded_rng, &pubkey_pool);
                    let client_filter = fixtures::filter_from_request(&subscribe_request);
                    if client_filter.index_keys().is_none() {
                        continue;
                    }
                    let (indexed_subscriber, _stream_rx) =
                        new_client(session_id, &subscribe_request, 1);
                    let live_slot = level_index.open_slot(session_id);
                    level_index.claim_index_keys(live_slot, &indexed_subscriber.index_keys);
                    for index_key in indexed_subscriber.index_keys.iter() {
                        reference_index.claim_index_key(*index_key, session_id);
                    }
                    level_index.make_subscriber_live(indexed_subscriber.clone(), live_slot, None);
                    indexed_subscribers.push(indexed_subscriber);
                }
                for _ in 0..256 {
                    let delivered_message = random_message(&mut seeded_rng, &pubkey_pool);
                    let candidate_ids = candidate_session_ids(&mut level_index, &delivered_message);
                    let mut reference_ids = Vec::new();
                    for_each_index_key(&delivered_message, |index_key| {
                        reference_ids
                            .extend_from_slice(reference_index.claiming_session_ids(&index_key));
                    });
                    reference_ids.sort_unstable();
                    reference_ids.dedup();
                    for indexed_subscriber in &indexed_subscribers {
                        let session_id = indexed_subscriber.session_id;
                        let delivered_messages = std::slice::from_ref(&delivered_message);
                        let has_update =
                            !expected_updates(indexed_subscriber, delivered_messages).is_empty();
                        let is_candidate = candidate_ids.binary_search(&session_id).is_ok();
                        let is_reference = reference_ids.binary_search(&session_id).is_ok();
                        assert!(
                            is_candidate || !has_update,
                            "seed {rng_seed}, session {session_id}"
                        );
                        assert!(
                            is_reference || !is_candidate,
                            "seed {rng_seed}, session {session_id}"
                        );
                        exact_match_count += usize::from(has_update);
                        pruned_candidate_count += usize::from(is_reference && !is_candidate);
                    }
                }
            }
        }
        assert!(exact_match_count > 2000, "{exact_match_count}");
        assert!(pruned_candidate_count > 100, "{pruned_candidate_count}");
    }

    /// A departed slot stays retired while index steps release its claims; once reused, its new
    /// subscriber is no candidate for the departed subscriber's keys.
    #[test]
    fn a_slot_is_reused_only_after_its_last_claim_is_released() {
        let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
        let departed_keys =
            sorted_key_list((0..200).map(numbered_index_key).chain([IndexKey::Slots]));
        let (departed_subscriber, _departed_rx) = keyed_client(1, &departed_keys);
        owner_indexes.take_index_change(IndexChange::Join(departed_subscriber, None));
        let departed_slot = owner_indexes.level_indexes[0].live_slots_by_session_id[&1];
        leave_index(&mut owner_indexes, 1);
        let mut slots_opened_meanwhile = Vec::new();
        for waiting_session_id in 2.. {
            assert_claims_match_subscribers(&owner_indexes);
            if !owner_indexes.has_index_work() {
                break;
            }
            let waiting_keys =
                sorted_key_list([numbered_index_key(1000 + waiting_session_id as u64)]);
            let (waiting_subscriber, _waiting_rx) = keyed_client(waiting_session_id, &waiting_keys);
            owner_indexes.take_index_change(IndexChange::Join(waiting_subscriber, None));
            let processed_index = &owner_indexes.level_indexes[0];
            slots_opened_meanwhile
                .push(processed_index.live_slots_by_session_id[&waiting_session_id]);
            assert_eq!(owner_indexes.advance_index_step(), IndexStep::Advanced);
        }
        // 200 claims, released 64 per step.
        assert_eq!(slots_opened_meanwhile.len(), 4);
        assert!(!slots_opened_meanwhile.contains(&departed_slot));
        let (reusing_subscriber, mut reusing_rx) = new_client(99, &entries_request(), 16);
        owner_indexes.take_index_change(IndexChange::Join(reusing_subscriber.clone(), None));
        let processed_index = &owner_indexes.level_indexes[0];
        assert_eq!(processed_index.live_slots_by_session_id[&99], departed_slot);
        let slot_message = fixtures::message_slot(100, SlotStatus::Processed);
        let message_batch = [
            Message::Account(fixtures::simple_message_account(
                numbered_pubkey(0),
                Pubkey::default(),
            )),
            Message::Slot(Arc::new(slot_message)),
            entry_message(5),
        ];
        send_processed(&mut owner_indexes, &message_batch);
        let processed_index = &owner_indexes.level_indexes[0];
        assert_eq!(processed_index.candidate_counts[departed_slot as usize], 1);
        assert_eq!(
            received_items(&mut reusing_rx),
            expected_updates(&reusing_subscriber, &message_batch)
        );
        assert_claims_match_subscribers(&owner_indexes);
    }

    /// Clients that share two hot keys join past 64, 128 and 192 slots, so rows widen while dense rows
    /// hold slots; around each widening, every client gets what its twin gets.
    #[test]
    fn the_rows_widen_while_dense_rows_hold_slots() {
        let mut owner_indexes = OwnerIndexes::new(SubscriberChannels::new(16, 16, 16));
        let hot_pubkeys = [numbered_pubkey(1), numbered_pubkey(2)];
        let account_message = |account_pubkey| {
            Message::Account(fixtures::simple_message_account(
                account_pubkey,
                Pubkey::default(),
            ))
        };
        let mut indexed_clients = Vec::new();
        for session_id in 0..200 {
            let own_pubkey = numbered_pubkey(1000 + session_id as u64);
            let watched_pubkeys = [hot_pubkeys[session_id % 2], own_pubkey];
            let mut subscribe_request = accounts_request(&watched_pubkeys, &[], None, &[]);
            if session_id % 3 == 0 {
                subscribe_request.slots = [("s".to_owned(), Default::default())].into();
            }
            let (indexed_subscriber, stream_rx) =
                new_client(session_id, &subscribe_request, MAX_CAPACITY);
            owner_indexes.apply_index_change(IndexChange::Join(indexed_subscriber.clone(), None));
            indexed_clients.push((indexed_subscriber, stream_rx));
            if ![63, 64, 127, 128, 191, 192, 199].contains(&session_id) {
                continue;
            }
            let slot_message = fixtures::message_slot(100, SlotStatus::Processed);
            let message_batch = [
                account_message(hot_pubkeys[0]),
                account_message(hot_pubkeys[1]),
                account_message(own_pubkey),
                account_message(numbered_pubkey(1000)),
                Message::Slot(Arc::new(slot_message)),
            ];
            send_processed(&mut owner_indexes, &message_batch);
            for (indexed_subscriber, stream_rx) in &mut indexed_clients {
                assert_eq!(
                    received_items(stream_rx),
                    expected_updates(indexed_subscriber, &message_batch),
                    "{} joined, session {}",
                    session_id + 1,
                    indexed_subscriber.session_id
                );
            }
        }
        assert_eq!(
            owner_indexes.level_indexes[0]
                .key_claim_index
                .hit_word_count(),
            4
        );
        assert_claims_match_subscribers(&owner_indexes);
    }

    /// A memcmp key is probed only for accounts of a gated owner, or for every account while an
    /// ungated memcmp filter is indexed; the exact match then drops what the owner list misses.
    #[test]
    fn memcmp_probes_run_only_for_gated_owners_or_while_a_memcmp_is_ungated() {
        let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
        let [token_program, other_program] = [numbered_pubkey(7), numbered_pubkey(8)];
        let account_of = |owner_pubkey| {
            let account_data = [[0; 32], MEMCMP_BYTES].concat();
            let account_info =
                fixtures::account_info(Pubkey::new_unique(), owner_pubkey, account_data, 1, None);
            Message::Account(fixtures::message_account(account_info))
        };
        let message_batch = [account_of(token_program), account_of(other_program)];
        let tracker_request = accounts_request(&[], &[token_program], Some(32), &[]);
        let (tracker_subscriber, mut tracker_rx) = new_client(1, &tracker_request, 16);
        owner_indexes.apply_index_change(IndexChange::Join(tracker_subscriber.clone(), None));
        let tracker_slot = owner_indexes.level_indexes[0].live_slots_by_session_id[&1] as usize;
        let tracker_updates = expected_updates(&tracker_subscriber, &message_batch);
        assert_eq!(tracker_updates.len(), 1);
        let ungated_request = accounts_request(&[], &[], Some(32), &[]);
        let (ungated_subscriber, mut ungated_rx) = new_client(2, &ungated_request, 16);
        // Shut gate: 1 candidate; ungated filter joined: 2 more; ungated filter gone: 1 more.
        for (phase_number, tracker_candidate_count) in [1, 3, 4].into_iter().enumerate() {
            if phase_number == 1 {
                let ungated_join = IndexChange::Join(ungated_subscriber.clone(), None);
                owner_indexes.apply_index_change(ungated_join);
            }
            if phase_number == 2 {
                leave_index(&mut owner_indexes, 2);
                run_every_index_step(&mut owner_indexes);
            }
            send_processed(&mut owner_indexes, &message_batch);
            let processed_index = &owner_indexes.level_indexes[0];
            assert_eq!(
                processed_index.candidate_counts[tracker_slot], tracker_candidate_count,
                "phase {phase_number}"
            );
            assert_eq!(received_items(&mut tracker_rx), tracker_updates);
            assert_claims_match_subscribers(&owner_indexes);
        }
        assert_eq!(
            received_items(&mut ungated_rx),
            expected_updates(&ungated_subscriber, &message_batch)
        );
    }

    /// The per-kind index keys sort after every account, owner, memcmp, memcmp gate and transaction
    /// key, so they are the end of a filter's sorted key list.
    #[test]
    fn the_per_kind_index_keys_sort_after_every_pubkey_key() {
        let highest_pubkey = Pubkey::new_from_array([u8::MAX; 32]);
        let highest_pubkey_keys = [
            IndexKey::Account(highest_pubkey),
            IndexKey::Owner(highest_pubkey),
            IndexKey::Memcmp(usize::MAX, [u8::MAX; 32]),
            IndexKey::MemcmpGate(highest_pubkey),
            IndexKey::UngatedMemcmp,
            IndexKey::Transaction(highest_pubkey),
        ];
        for pubkey_index_key in highest_pubkey_keys {
            for per_kind_index_key in PER_KIND_INDEX_KEYS {
                assert!(
                    pubkey_index_key < per_kind_index_key,
                    "{pubkey_index_key:?}"
                );
            }
        }
        assert!(PER_KIND_INDEX_KEYS.is_sorted());
        let mut subscribe_request = accounts_request(&[highest_pubkey], &[], None, &[]);
        subscribe_request.transactions =
            transactions_request(&[highest_pubkey], &[], None).transactions;
        subscribe_request.slots = [("s".to_owned(), Default::default())].into();
        let entry_filter = SubscribeRequestFilterEntry {
            include_update_parent: Some(true),
        };
        subscribe_request.entry = [("e".to_owned(), entry_filter)].into();
        subscribe_request.blocks_meta = [("m".to_owned(), Default::default())].into();
        subscribe_request.block_footer = [("f".to_owned(), Default::default())].into();
        let index_keys = fixtures::filter_from_request(&subscribe_request)
            .index_keys()
            .expect("an indexable filter");
        let per_kind_start = index_keys.partition_point(|index_key| *index_key < IndexKey::Slots);
        let pubkey_index_keys = [
            IndexKey::Account(highest_pubkey),
            IndexKey::Transaction(highest_pubkey),
        ];
        assert_eq!(index_keys[..per_kind_start], pubkey_index_keys);
        assert_eq!(index_keys[per_kind_start..], PER_KIND_INDEX_KEYS);
    }

    /// A level index whose subscribers claim no index key, as when their filters ask for nothing,
    /// looks up no message of a batch, and looks messages up again once a subscriber claims a key.
    #[test]
    fn a_level_index_whose_subscribers_claim_no_index_key_looks_up_no_message() {
        let mut owner_indexes = OwnerIndexes::new(SubscriberChannels::new(16, 16, 16));
        let (keyless_subscriber, _keyless_rx) = new_client(1, &SubscribeRequest::default(), 16);
        let (entries_subscriber, mut entries_rx) = new_client(2, &entries_request(), 16);
        let message_batch = [entry_message(1)];
        owner_indexes.apply_index_change(IndexChange::Join(keyless_subscriber, None));
        send_processed(&mut owner_indexes, &message_batch);
        assert_eq!(owner_indexes.level_indexes[0].looked_up_message_count, 0);
        owner_indexes.apply_index_change(IndexChange::Join(entries_subscriber.clone(), None));
        send_processed(&mut owner_indexes, &message_batch);
        assert_eq!(owner_indexes.level_indexes[0].looked_up_message_count, 1);
        assert_eq!(
            received_items(&mut entries_rx),
            expected_updates(&entries_subscriber, &message_batch)
        );
        let keyless_refilter =
            refilter_subscriber(&entries_subscriber, &SubscribeRequest::default());
        owner_indexes.apply_index_change(IndexChange::Join(keyless_refilter, None));
        send_processed(&mut owner_indexes, &message_batch);
        let processed_index = &owner_indexes.level_indexes[0];
        assert_eq!(processed_index.live_slots_by_session_id.len(), 2);
        assert_eq!(processed_index.looked_up_message_count, 1);
    }

    /// Where a claim step from `first_unclaimed_key` ends: after its keys, at the list's end, or after
    /// the first new pair that allocates or doubles its shard, and whether a growth ended it.
    fn predicted_claim_step_end(
        level_index: &LevelIndex,
        index_keys: &[IndexKey],
        first_unclaimed_key: usize,
        keys_per_step: usize,
    ) -> (usize, bool) {
        let claimed_key_table = level_index.key_claim_index.claimed_key_table();
        let step_end = (first_unclaimed_key + keys_per_step).min(index_keys.len());
        let mut added_by_shard = HashMap::<usize, usize>::default();
        let stepped_keys = index_keys[..step_end].iter().enumerate();
        for (key_position, index_key) in stepped_keys.skip(first_unclaimed_key) {
            let ClaimPlacement::Keyed(table_key, claim_role) =
                ClaimPlacement::of_index_key(index_key)
            else {
                continue;
            };
            if claimed_key_table
                .find_bucket(&table_key, claim_role)
                .is_some()
            {
                continue;
            }
            let shard_number = shard_number_of(claimed_key_table.pair_hash(&table_key, claim_role));
            let table_shard = claimed_key_table.table_shards().get(shard_number);
            let (held_count, bucket_count) = table_shard.map_or((0, 0), |table_shard| {
                (table_shard.held_bucket_count(), table_shard.bucket_count())
            });
            let added_count = added_by_shard.entry(shard_number).or_default();
            if (held_count + *added_count + 1) * 2 > bucket_count {
                return (key_position + 1, true);
            }
            *added_count += 1;
        }
        (step_end, false)
    }

    /// An index step claims at most its keys and ends after a shard growth; in shards with room, every
    /// step but the last claims a whole step of keys.
    #[test]
    fn an_index_step_claims_at_most_its_keys_and_one_shard_growth() {
        let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
        let keys_per_step = INDEX_KEYS_PER_STEP.get();
        let first_index_keys = sorted_key_list((0..8192).map(numbered_index_key));
        let (first_subscriber, _first_rx) = keyed_client(1, &first_index_keys);
        let _first_ready_rx = prepare_subscriber(&mut owner_indexes, first_subscriber);
        let mut growth_ended_steps = 0;
        loop {
            let claimed_before = staged_key_count(&owner_indexes, 1);
            let (predicted_end, ended_by_growth) = predicted_claim_step_end(
                &owner_indexes.level_indexes[0],
                &first_index_keys,
                claimed_before,
                keys_per_step,
            );
            let index_step = owner_indexes.advance_index_step();
            assert_eq!(staged_key_count(&owner_indexes, 1), predicted_end);
            growth_ended_steps += usize::from(ended_by_growth);
            if index_step == IndexStep::AnsweredPreparation {
                break;
            }
            assert_eq!(index_step, IndexStep::Advanced);
        }
        assert!(growth_ended_steps > 0);
        // 65,536 more keys claimed and released leave room in the shards, which keep their buckets.
        let level_index = &mut owner_indexes.level_indexes[0];
        let churned_index_keys = sorted_key_list((100_000..165_536).map(numbered_index_key));
        let churned_slot = level_index.open_slot(2);
        level_index.claim_index_keys(churned_slot, &churned_index_keys);
        level_index.retire_slot(churned_slot, &churned_index_keys, churned_index_keys.len());
        while level_index.release_queued_claims(usize::MAX) {}
        // 8,191 new keys, each in a shard with room left for it.
        let claimed_key_table = level_index.key_claim_index.claimed_key_table();
        let mut room_by_shard = claimed_key_table
            .table_shards()
            .iter()
            .map(|table_shard| table_shard.bucket_count() / 2 - table_shard.held_bucket_count())
            .collect::<Vec<_>>();
        let fitting_index_keys = (200_000..)
            .map(numbered_index_key)
            .filter(|index_key| {
                let ClaimPlacement::Keyed(table_key, claim_role) =
                    ClaimPlacement::of_index_key(index_key)
                else {
                    unreachable!("an account key is keyed");
                };
                let pair_hash = claimed_key_table.pair_hash(&table_key, claim_role);
                let shard_room = &mut room_by_shard[shard_number_of(pair_hash)];
                let key_fits = *shard_room > 0;
                *shard_room -= usize::from(key_fits);
                key_fits
            })
            .take(8191);
        let second_index_keys = sorted_key_list(fitting_index_keys);
        let (second_subscriber, _second_rx) = keyed_client(3, &second_index_keys);
        let _second_ready_rx = prepare_subscriber(&mut owner_indexes, second_subscriber);
        let mut claims_per_step = Vec::new();
        loop {
            let claimed_before = staged_key_count(&owner_indexes, 3);
            let index_step = owner_indexes.advance_index_step();
            claims_per_step.push(staged_key_count(&owner_indexes, 3) - claimed_before);
            if index_step == IndexStep::AnsweredPreparation {
                break;
            }
        }
        let (last_step_claims, earlier_step_claims) = claims_per_step.split_last().unwrap();
        assert!(earlier_step_claims
            .iter()
            .all(|step_claims| *step_claims == keys_per_step));
        assert_eq!(*last_step_claims, 8191 % keys_per_step);
    }

    /// A staged subscriber of 1,000 keys gets nothing delivered while slices claim its keys between
    /// sends, and gets exactly what its filter makes from its hand-over on.
    #[test]
    fn a_prepared_subscriber_gets_nothing_until_its_hand_over() {
        let mut owner_indexes = owner_indexes_with(1024, STEP_EVERY_PASS_LIMITS);
        let account_pubkeys = fixtures::deterministic_pubkeys(3, 1000);
        let subscribe_request = accounts_request(&account_pubkeys, &[], None, &[]);
        let (indexed_subscriber, mut stream_rx) = new_client(1, &subscribe_request, MAX_CAPACITY);
        assert_eq!(indexed_subscriber.index_keys.len(), 1000);
        let mut broadcast_rx = owner_indexes.broadcast_channels.subscribe(Processed);
        let mut ready_rx = prepare_subscriber(&mut owner_indexes, indexed_subscriber.clone());
        let mut sent_messages = Vec::new();
        let mut send_account_update = |owner_indexes: &mut OwnerIndexes| {
            let account_pubkey = account_pubkeys[sent_messages.len() % account_pubkeys.len()];
            let account_message = Message::Account(fixtures::simple_message_account(
                account_pubkey,
                Pubkey::default(),
            ));
            send_processed(owner_indexes, std::slice::from_ref(&account_message));
            sent_messages.push(account_message);
        };
        let mut staged_sends = 0;
        while ready_rx.try_recv().is_err() {
            owner_indexes.run_index_slice();
            send_account_update(&mut owner_indexes);
            staged_sends += 1;
            assert!(
                received_items(&mut stream_rx).is_empty(),
                "delivered before its hand-over"
            );
        }
        assert!(staged_sends > 16, "{staged_sends}");
        // The client reads its receiver to the end, then hands it over.
        while let Ok(message_batch) = broadcast_rx.try_recv() {
            for broadcast_message in message_batch.iter() {
                assert!(indexed_subscriber.queue_updates(broadcast_message).is_ok());
            }
        }
        let index_change = IndexChange::Join(indexed_subscriber.clone(), Some(broadcast_rx));
        owner_indexes.take_index_change(index_change);
        for _ in 0..3 {
            send_account_update(&mut owner_indexes);
        }
        assert_eq!(
            received_items(&mut stream_rx),
            expected_updates(&indexed_subscriber, &sent_messages)
        );
    }

    /// A preparation is answered by the step that claims the indexed subscriber's last key, and not
    /// before. An indexed subscriber with no keys is answered at its first step, and an indexed
    /// subscriber whose client is gone is unstaged at its answer.
    #[test]
    fn a_preparation_is_answered_at_the_step_that_claims_its_last_key() {
        let mut owner_indexes = owner_indexes_with(16, SINGLE_KEY_STEP_LIMITS);
        let index_keys = sorted_key_list((0..5).map(numbered_index_key).chain([IndexKey::Slots]));
        let (indexed_subscriber, _stream_rx) = keyed_client(1, &index_keys);
        let mut ready_rx = prepare_subscriber(&mut owner_indexes, indexed_subscriber);
        for claimed_key_count in 1..index_keys.len() {
            assert_eq!(owner_indexes.advance_index_step(), IndexStep::Advanced);
            assert_eq!(staged_key_count(&owner_indexes, 1), claimed_key_count);
            assert_eq!(
                ready_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            );
        }
        let last_step = owner_indexes.advance_index_step();
        assert_eq!(last_step, IndexStep::AnsweredPreparation);
        assert_eq!(ready_rx.try_recv(), Ok(()));
        assert!(!owner_indexes.has_index_work());
        let (keyless_subscriber, _keyless_rx) = keyed_client(2, &sorted_key_list([]));
        let mut keyless_ready_rx = prepare_subscriber(&mut owner_indexes, keyless_subscriber);
        let keyless_step = owner_indexes.advance_index_step();
        assert_eq!(keyless_step, IndexStep::AnsweredPreparation);
        assert_eq!(keyless_ready_rx.try_recv(), Ok(()));
        assert_eq!(owner_indexes.advance_index_step(), IndexStep::NoWorkLeft);
        // A client that dropped its answer receiver is gone: its indexed subscriber is unstaged at
        // the answer.
        let (gone_subscriber, _gone_rx) = keyed_client(3, &sorted_key_list([IndexKey::Slots]));
        drop(prepare_subscriber(&mut owner_indexes, gone_subscriber));
        assert_eq!(owner_indexes.advance_index_step(), IndexStep::Advanced);
        assert!(!owner_indexes
            .staged_subscribers_by_session_id
            .contains_key(&3));
        assert_claims_match_subscribers(&owner_indexes);
        let processed_index = &owner_indexes.level_indexes[0];
        assert_eq!(claim_count(processed_index, &IndexKey::Slots, 3), 0);
    }

    /// For every point the client last reads its receiver and every later point its hand-over
    /// lands, across 6 sends, its own reads, the owner's drain and the indexed sends make exactly
    /// what its twin gets, which reads every batch.
    #[test]
    fn a_hand_over_delivers_the_receivers_backlog_between_two_sends() {
        const SEND_COUNT: usize = 6;
        let message_batches = (1..=SEND_COUNT as u64)
            .map(|entry_slot| vec![entry_message(entry_slot), entry_message(entry_slot + 100)])
            .collect::<Vec<_>>();
        let mut drained_hand_overs = 0;
        for last_read_point in 0..=SEND_COUNT {
            for hand_over_point in last_read_point..=SEND_COUNT {
                let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
                let (indexed_subscriber, mut stream_rx) =
                    new_client(1, &entries_request(), MAX_CAPACITY);
                let mut broadcast_rx = Some(owner_indexes.broadcast_channels.subscribe(Processed));
                let mut ready_rx =
                    prepare_subscriber(&mut owner_indexes, indexed_subscriber.clone());
                run_every_index_step(&mut owner_indexes);
                assert_eq!(ready_rx.try_recv(), Ok(()));
                for send_point in 0..=SEND_COUNT {
                    if send_point == last_read_point {
                        let client_rx = broadcast_rx.as_mut().unwrap();
                        while let Ok(message_batch) = client_rx.try_recv() {
                            for broadcast_message in message_batch.iter() {
                                assert!(indexed_subscriber
                                    .queue_updates(broadcast_message)
                                    .is_ok());
                            }
                        }
                    }
                    if send_point == hand_over_point {
                        let handed_over_rx = broadcast_rx.take();
                        drained_hand_overs +=
                            usize::from(!handed_over_rx.as_ref().unwrap().is_empty());
                        let index_change =
                            IndexChange::Join(indexed_subscriber.clone(), handed_over_rx);
                        owner_indexes.take_index_change(index_change);
                    }
                    if let Some(message_batch) = message_batches.get(send_point) {
                        send_processed(&mut owner_indexes, message_batch);
                    }
                }
                let twin_items = expected_updates(&indexed_subscriber, &message_batches.concat());
                assert_eq!(
                    received_items(&mut stream_rx),
                    twin_items,
                    "read at {last_read_point}, hand-over at {hand_over_point}"
                );
            }
        }
        assert_eq!(drained_hand_overs, 21);
    }

    /// A join that no preparation came before claims every key of its indexed subscriber at once,
    /// leaves no index work, and delivers exactly what the filter makes.
    #[test]
    fn a_join_without_a_preparation_claims_its_keys_at_once() {
        let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
        let account_pubkeys = [Pubkey::new_unique(), Pubkey::new_unique()];
        let mut subscribe_request = accounts_request(&account_pubkeys, &[], None, &[]);
        subscribe_request.entry = entries_request().entry;
        let (indexed_subscriber, mut stream_rx) = new_client(1, &subscribe_request, 16);
        let broadcast_rx = owner_indexes.broadcast_channels.subscribe(Processed);
        let handed_over_batch = [entry_message(1)];
        send_processed(&mut owner_indexes, &handed_over_batch);
        let index_change = IndexChange::Join(indexed_subscriber.clone(), Some(broadcast_rx));
        owner_indexes.take_index_change(index_change);
        assert!(!owner_indexes.has_index_work());
        assert_claims_match_subscribers(&owner_indexes);
        let delivered_batch = account_pubkeys
            .map(|account_pubkey| {
                Message::Account(fixtures::simple_message_account(
                    account_pubkey,
                    Pubkey::default(),
                ))
            })
            .into_iter()
            .chain([entry_message(2)])
            .collect::<Vec<_>>();
        send_processed(&mut owner_indexes, &delivered_batch);
        let sent_messages = [&handed_over_batch[..], &delivered_batch].concat();
        assert_eq!(
            received_items(&mut stream_rx),
            expected_updates(&indexed_subscriber, &sent_messages)
        );
    }

    /// A join without a receiver whose indexed subscriber holds the live subscriber's key list
    /// replaces the filter in place, at processed and at confirmed: no index work, and each message
    /// meets one filter. An equal key list in another [`Arc`] is claimed again instead, and the old
    /// claims wait for release.
    #[test]
    fn a_refilter_with_the_same_key_list_replaces_the_subscriber_in_place() {
        for commitment_level in [Processed, CommitmentLevel::Confirmed] {
            let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
            let account_pubkey = Pubkey::new_unique();
            let mut whole_request = accounts_request(&[account_pubkey], &[], None, &[]);
            whole_request.commitment = Some(commitment_level as i32);
            let mut sliced_request = whole_request.clone();
            sliced_request.accounts_data_slice = vec![SubscribeRequestAccountsDataSlice {
                offset: 1,
                length: 2,
            }];
            let (indexed_subscriber, mut stream_rx) = new_client(1, &whole_request, 16);
            owner_indexes.take_index_change(IndexChange::Join(indexed_subscriber.clone(), None));
            let account_info = fixtures::account_info(
                account_pubkey,
                Pubkey::default(),
                vec![1, 2, 3, 4],
                1,
                None,
            );
            let account_message = vec![Message::Account(fixtures::message_account(account_info))];
            owner_indexes.send_batch(commitment_level, Arc::new(account_message.clone()));
            let sliced_filter = Arc::new(fixtures::filter_from_request(&sliced_request));
            let sliced_subscriber =
                indexed_subscriber.with_filter(&sliced_filter, &indexed_subscriber.index_keys);
            owner_indexes.take_index_change(IndexChange::Join(sliced_subscriber.clone(), None));
            assert!(!owner_indexes.has_index_work(), "{commitment_level:?}");
            assert_claims_match_subscribers(&owner_indexes);
            owner_indexes.send_batch(commitment_level, Arc::new(account_message.clone()));
            let mut expected_items = expected_updates(&indexed_subscriber, &account_message);
            expected_items.extend(expected_updates(&sliced_subscriber, &account_message));
            assert_eq!(expected_items.len(), 2);
            assert_ne!(expected_items[0], expected_items[1]);
            assert_eq!(received_items(&mut stream_rx), expected_items);
            let copied_index_keys = Arc::<[IndexKey]>::from(indexed_subscriber.index_keys.to_vec());
            let copied_subscriber = indexed_subscriber
                .with_filter(&indexed_subscriber.client_filter, &copied_index_keys);
            owner_indexes.take_index_change(IndexChange::Join(copied_subscriber, None));
            assert!(owner_indexes.has_index_work(), "{commitment_level:?}");
            assert_claims_match_subscribers(&owner_indexes);
        }
    }

    /// A leave of a staged subscriber, at every count of claimed keys, releases the claimed prefix:
    /// its per-kind keys at once, the rest in index steps. The index ends empty and the indexed
    /// subscriber's queue closes.
    #[test]
    fn a_leave_releases_the_claimed_prefix_of_a_staged_subscriber() {
        let index_keys = sorted_key_list(
            (0..4)
                .map(numbered_index_key)
                .chain([IndexKey::Slots, IndexKey::Entries]),
        );
        for claimed_key_count in 0..=index_keys.len() {
            let mut owner_indexes = owner_indexes_with(16, SINGLE_KEY_STEP_LIMITS);
            let (indexed_subscriber, mut stream_rx) = keyed_client(1, &index_keys);
            let _ready_rx = prepare_subscriber(&mut owner_indexes, indexed_subscriber);
            for _ in 0..claimed_key_count {
                owner_indexes.advance_index_step();
            }
            assert_eq!(staged_key_count(&owner_indexes, 1), claimed_key_count);
            leave_index(&mut owner_indexes, 1);
            assert_claims_match_subscribers(&owner_indexes);
            let processed_index = &owner_indexes.level_indexes[0];
            for per_kind_index_key in [IndexKey::Slots, IndexKey::Entries] {
                assert_eq!(claim_count(processed_index, &per_kind_index_key, 1), 0);
            }
            run_every_index_step(&mut owner_indexes);
            assert_owner_holds_nothing(&owner_indexes);
            assert!(matches!(stream_rx.recv().now_or_never(), Some(None)));
        }
    }

    /// Session 1 stages A, which is partly claimed, session 2 stages C, then session 1 leaves and
    /// stages B. The stale entry of A is skipped: C is answered before B, B once, A never, and the
    /// index holds the keys of B and C only.
    #[test]
    fn a_restaged_session_skips_its_stale_staging_entry() {
        let mut owner_indexes = owner_indexes_with(16, SINGLE_KEY_STEP_LIMITS);
        let first_index_keys = sorted_key_list((0..3).map(numbered_index_key));
        let other_index_keys = sorted_key_list((10..12).map(numbered_index_key));
        let restaged_index_keys = sorted_key_list((20..22).map(numbered_index_key));
        let (first_subscriber, _first_rx) = keyed_client(1, &first_index_keys);
        let mut first_ready_rx = prepare_subscriber(&mut owner_indexes, first_subscriber);
        assert_eq!(owner_indexes.advance_index_step(), IndexStep::Advanced);
        let (other_subscriber, _other_rx) = keyed_client(2, &other_index_keys);
        let mut other_ready_rx = prepare_subscriber(&mut owner_indexes, other_subscriber);
        leave_index(&mut owner_indexes, 1);
        let (restaged_subscriber, _restaged_rx) = keyed_client(1, &restaged_index_keys);
        let mut restaged_ready_rx = prepare_subscriber(&mut owner_indexes, restaged_subscriber);
        let mut answer_order = Vec::new();
        while owner_indexes.advance_index_step() != IndexStep::NoWorkLeft {
            assert_claims_match_subscribers(&owner_indexes);
            if other_ready_rx.try_recv().is_ok() {
                answer_order.push("other");
            }
            if restaged_ready_rx.try_recv().is_ok() {
                answer_order.push("restaged");
            }
        }
        assert_eq!(answer_order, ["other", "restaged"]);
        assert_eq!(
            first_ready_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        );
        let processed_index = &owner_indexes.level_indexes[0];
        for index_key in other_index_keys.iter() {
            assert_eq!(claim_count(processed_index, index_key, 2), 1);
        }
        for index_key in restaged_index_keys.iter() {
            assert_eq!(claim_count(processed_index, index_key, 1), 1);
        }
        assert_eq!(processed_index.key_claim_index.held_index_keys().len(), 4);
    }

    /// An index change in a [`releases_never_remove_a_claim_that_a_live_subscriber_needs`] script,
    /// by session id, key set and commitment level.
    #[derive(Debug, Clone, Copy)]
    enum ScriptedChange {
        /// A join with no receiver, which claims its keys at once when no preparation did.
        JoinAtOnce(usize, usize, CommitmentLevel),
        /// A preparation.
        Prepare(usize, usize, CommitmentLevel),
        /// A join with an empty receiver.
        HandOver(usize, usize, CommitmentLevel),
        /// A leave with no receiver request.
        Leave(usize),
    }

    /// The key lists and level filters that [`ScriptedChange`]s name.
    struct ScriptedSubscribers {
        /// The key lists, by key set number.
        key_sets: Vec<Arc<[IndexKey]>>,
        /// A filter that matches nothing at each commitment level, by the level as `usize`.
        level_filters: [Arc<Filter>; 3],
    }

    impl ScriptedSubscribers {
        /// The indexed subscriber of `session_id` with key set `key_set_number`, and the filter at
        /// `commitment_level`.
        ///
        /// # Arguments
        ///
        /// * `session_id` - The indexed subscriber's session id.
        /// * `key_set_number` - The position of the indexed subscriber's key list in `key_sets`.
        /// * `commitment_level` - The level of the indexed subscriber's filter.
        ///
        /// # Returns
        ///
        /// The indexed subscriber, which shares the key list's [`Arc`].
        fn scripted_subscriber(
            &self,
            session_id: usize,
            key_set_number: usize,
            commitment_level: CommitmentLevel,
        ) -> IndexedSubscriber {
            let index_keys = &self.key_sets[key_set_number];
            let (keyed_subscriber, _stream_rx) = keyed_client(session_id, index_keys);
            keyed_subscriber.with_filter(&self.level_filters[commitment_level as usize], index_keys)
        }
    }

    /// Runs `change_script` with `gap_steps[i]` index steps after its change `i`, and checks the
    /// claims after every change and step. Then every session leaves, every step runs, and the
    /// owner must hold nothing, so the next run starts from an empty owner.
    ///
    /// # Arguments
    ///
    /// * `owner_indexes` - An owner that holds nothing.
    /// * `scripted_subscribers` - The key lists and filters the script uses.
    /// * `change_script` - The index changes, one per gap.
    /// * `gap_steps` - How many index steps run after each change.
    ///
    /// # Returns
    ///
    /// Whether every step asked for did index work.
    ///
    /// # Panics
    ///
    /// Panics if the claims ever differ from what the indexed subscribers made, or if anything is
    /// left at the end.
    fn run_scripted_changes(
        owner_indexes: &mut OwnerIndexes,
        scripted_subscribers: &ScriptedSubscribers,
        change_script: &[ScriptedChange],
        gap_steps: &[usize],
    ) -> bool {
        let mut every_step_did_work = true;
        let mut ready_rxs = Vec::new();
        for (scripted_change, step_count) in change_script.iter().zip(gap_steps) {
            let index_change = match *scripted_change {
                ScriptedChange::JoinAtOnce(session_id, key_set_number, commitment_level) => {
                    let joined_subscriber = scripted_subscribers.scripted_subscriber(
                        session_id,
                        key_set_number,
                        commitment_level,
                    );
                    IndexChange::Join(joined_subscriber, None)
                }
                ScriptedChange::Prepare(session_id, key_set_number, commitment_level) => {
                    let prepared_subscriber = scripted_subscribers.scripted_subscriber(
                        session_id,
                        key_set_number,
                        commitment_level,
                    );
                    let (ready_tx, ready_rx) = oneshot::channel();
                    ready_rxs.push(ready_rx);
                    IndexChange::Prepare(prepared_subscriber, ready_tx)
                }
                ScriptedChange::HandOver(session_id, key_set_number, commitment_level) => {
                    let joined_subscriber = scripted_subscribers.scripted_subscriber(
                        session_id,
                        key_set_number,
                        commitment_level,
                    );
                    let broadcast_rx = owner_indexes.broadcast_channels.subscribe(commitment_level);
                    IndexChange::Join(joined_subscriber, Some(broadcast_rx))
                }
                ScriptedChange::Leave(session_id) => IndexChange::Leave(IndexLeave {
                    session_id,
                    receiver_request: None,
                }),
            };
            owner_indexes.take_index_change(index_change);
            assert_claims_match_subscribers(owner_indexes);
            for _ in 0..*step_count {
                if owner_indexes.advance_index_step() == IndexStep::NoWorkLeft {
                    every_step_did_work = false;
                    break;
                }
                assert_claims_match_subscribers(owner_indexes);
            }
            if !every_step_did_work {
                break;
            }
        }
        for session_id in [1, 2] {
            leave_index(owner_indexes, session_id);
        }
        while owner_indexes.advance_index_step() != IndexStep::NoWorkLeft {
            assert_claims_match_subscribers(owner_indexes);
        }
        assert_owner_holds_nothing(owner_indexes);
        every_step_did_work
    }

    /// Runs `change_script` under every placement of index steps between its changes: each gap
    /// gets every step count from 0 up to the work that waits there.
    ///
    /// # Arguments
    ///
    /// * `owner_indexes` - An owner that holds nothing.
    /// * `scripted_subscribers` - The key lists and filters the script uses.
    /// * `change_script` - The index changes.
    /// * `gap_steps` - The step counts chosen for the gaps before this one.
    ///
    /// # Returns
    ///
    /// How many placements ran whole.
    fn run_every_interleaving(
        owner_indexes: &mut OwnerIndexes,
        scripted_subscribers: &ScriptedSubscribers,
        change_script: &[ScriptedChange],
        gap_steps: &mut Vec<usize>,
    ) -> usize {
        let mut whole_runs = 0;
        for step_count in 0.. {
            gap_steps.push(step_count);
            let scripted_prefix = &change_script[..gap_steps.len()];
            if !run_scripted_changes(
                owner_indexes,
                scripted_subscribers,
                scripted_prefix,
                gap_steps,
            ) {
                gap_steps.pop();
                break;
            }
            whole_runs += if gap_steps.len() == change_script.len() {
                1
            } else {
                run_every_interleaving(
                    owner_indexes,
                    scripted_subscribers,
                    change_script,
                    gap_steps,
                )
            };
            gap_steps.pop();
        }
        whole_runs
    }

    /// Under every interleaving of one-key index steps with joins, preparations, hand-overs and
    /// leaves of overlapping key sets, the claims are exactly those of the live, staged and
    /// departed subscribers after every change and step. So a release never removes a claim that a
    /// live subscriber needs, and the owner holds nothing once both sessions have left.
    #[test]
    fn releases_never_remove_a_claim_that_a_live_subscriber_needs() {
        use {
            CommitmentLevel::Confirmed,
            ScriptedChange::{HandOver, JoinAtOnce, Leave, Prepare},
        };
        let key_sets = vec![
            sorted_key_list(
                [1, 2, 3]
                    .map(numbered_index_key)
                    .into_iter()
                    .chain([IndexKey::Slots]),
            ),
            sorted_key_list(
                [2, 3, 4]
                    .map(numbered_index_key)
                    .into_iter()
                    .chain([IndexKey::Slots, IndexKey::Entries]),
            ),
            sorted_key_list(
                [1, 4]
                    .map(numbered_index_key)
                    .into_iter()
                    .chain([IndexKey::Entries]),
            ),
        ];
        let change_scripts: [&[ScriptedChange]; 6] = [
            // Leave, then prepare an overlapping set and hand over.
            &[
                JoinAtOnce(1, 0, Processed),
                Leave(1),
                Prepare(1, 1, Processed),
                HandOver(1, 1, Processed),
                Leave(1),
            ],
            // Leave, prepare, then join another set at once and prepare again.
            &[
                JoinAtOnce(1, 0, Processed),
                Leave(1),
                Prepare(1, 1, Processed),
                JoinAtOnce(1, 2, Processed),
                Prepare(1, 0, Processed),
                HandOver(1, 0, Processed),
            ],
            // Prepare while live, hand over, then join another set at once.
            &[
                JoinAtOnce(1, 0, Processed),
                Prepare(1, 1, Processed),
                HandOver(1, 1, Processed),
                JoinAtOnce(1, 2, Processed),
                Leave(1),
            ],
            // Two sessions on shared keys.
            &[
                JoinAtOnce(1, 0, Processed),
                Prepare(2, 1, Processed),
                Leave(1),
                HandOver(2, 1, Processed),
                Prepare(1, 2, Processed),
                JoinAtOnce(1, 1, Processed),
                Leave(2),
            ],
            // A preparation at another level than the join.
            &[
                Prepare(1, 0, Confirmed),
                JoinAtOnce(1, 0, Processed),
                Prepare(1, 1, Confirmed),
                HandOver(1, 1, Confirmed),
                Leave(1),
            ],
            // A second preparation in place of a partly claimed one.
            &[
                Prepare(1, 0, Processed),
                Prepare(1, 1, Processed),
                HandOver(1, 1, Processed),
                Leave(1),
            ],
        ];
        let level_filters =
            [Processed, Confirmed, CommitmentLevel::Finalized].map(|commitment_level| {
                let level_request = SubscribeRequest {
                    commitment: Some(commitment_level as i32),
                    ..Default::default()
                };
                Arc::new(fixtures::filter_from_request(&level_request))
            });
        let scripted_subscribers = ScriptedSubscribers {
            key_sets,
            level_filters,
        };
        let mut owner_indexes = owner_indexes_with(16, SINGLE_KEY_STEP_LIMITS);
        for change_script in change_scripts {
            let whole_runs = run_every_interleaving(
                &mut owner_indexes,
                &scripted_subscribers,
                change_script,
                &mut Vec::new(),
            );
            assert!(whole_runs > 100, "{change_script:?}: {whole_runs}");
        }
    }

    /// A pass takes in at most 64 queued index changes: 1,000 leaves with receiver requests are
    /// answered over 16 passes, 64 per pass, in order.
    #[test]
    fn each_pass_takes_in_at_most_64_index_changes() {
        let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
        let (index_changes_tx, mut index_changes_rx) = mpsc::unbounded_channel();
        let mut answer_rxs = (0..1000)
            .map(|session_id| {
                let (answer_tx, answer_rx) = oneshot::channel();
                let client_leave = IndexLeave {
                    session_id,
                    receiver_request: Some((Processed, answer_tx)),
                };
                index_changes_tx
                    .send(IndexChange::Leave(client_leave))
                    .unwrap();
                answer_rx
            })
            .collect::<Vec<_>>();
        let mut answered_count = 0;
        for pass_number in 1..=16 {
            owner_indexes.take_queued_index_changes(&mut index_changes_rx);
            let expected_answered_count = (MAX_INDEX_CHANGES_PER_PASS * pass_number).min(1000);
            for answer_rx in &mut answer_rxs[answered_count..expected_answered_count] {
                assert!(answer_rx.try_recv().is_ok(), "pass {pass_number}");
            }
            if let Some(next_answer_rx) = answer_rxs.get_mut(expected_answered_count) {
                assert!(next_answer_rx.try_recv().is_err(), "pass {pass_number}");
            }
            answered_count = expected_answered_count;
        }
        assert_eq!(answered_count, 1000);
        assert!(index_changes_rx.is_empty());
    }

    /// An indexed client whose queue fills in a batch is disconnected, and its indexed subscriber
    /// leaves at once. The batch releases only its per-kind keys; its other keys wait in
    /// `claims_to_release`. Those stale claims find its retired slot, which delivers nothing, a
    /// refilter that lands later does not bring its indexed subscriber back and drops the
    /// preparation sent before it, and its stream ends.
    #[test]
    fn a_disconnect_in_deliver_batch_does_no_index_work_on_the_batch() {
        let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
        let account_pubkeys = [Pubkey::new_unique(), Pubkey::new_unique()];
        let accounts_only_request = accounts_request(&account_pubkeys, &[], None, &[]);
        let mut failing_request = accounts_only_request.clone();
        failing_request.entry = entries_request().entry;
        failing_request.slots = [("s".to_owned(), Default::default())].into();
        let (failing_subscriber, mut failing_rx) = new_client(1, &failing_request, 2);
        let (healthy_subscriber, mut healthy_rx) =
            new_client(2, &accounts_only_request, MAX_CAPACITY);
        owner_indexes.take_index_change(IndexChange::Join(failing_subscriber.clone(), None));
        owner_indexes.take_index_change(IndexChange::Join(healthy_subscriber.clone(), None));
        let entry_batch = [1, 2, 3].map(entry_message);
        let mut expected_failing_items = expected_updates(&failing_subscriber, &entry_batch[..2]);
        expected_failing_items.push(Err("lagged to send an update".to_owned()));
        let late_preparation = refilter_subscriber(&failing_subscriber, &accounts_only_request);
        let late_refilter = refilter_subscriber(&failing_subscriber, &accounts_only_request);
        drop(failing_subscriber);
        send_processed(&mut owner_indexes, &entry_batch);
        let processed_index = &owner_indexes.level_indexes[0];
        assert!(!processed_index.live_slots_by_session_id.contains_key(&1));
        for per_kind_index_key in [IndexKey::Slots, IndexKey::Entries] {
            assert_eq!(claim_count(processed_index, &per_kind_index_key, 1), 0);
        }
        for account_pubkey in account_pubkeys {
            let account_index_key = IndexKey::Account(account_pubkey);
            assert_eq!(claim_count(processed_index, &account_index_key, 1), 1);
        }
        assert_eq!(processed_index.claims_to_release.len(), 1);
        assert_eq!(processed_index.claims_to_release[0].claimed_key_count, 2);
        assert_claims_match_subscribers(&owner_indexes);
        let account_batch = account_pubkeys.map(|account_pubkey| {
            Message::Account(fixtures::simple_message_account(
                account_pubkey,
                Pubkey::default(),
            ))
        });
        let looked_up_before = owner_indexes.level_indexes[0].looked_up_message_count;
        send_processed(&mut owner_indexes, &account_batch);
        assert_eq!(
            owner_indexes.level_indexes[0].looked_up_message_count,
            looked_up_before + 2
        );
        assert_eq!(
            received_items(&mut healthy_rx),
            expected_updates(&healthy_subscriber, &account_batch)
        );
        let _late_ready_rx = prepare_subscriber(&mut owner_indexes, late_preparation);
        owner_indexes.take_index_change(IndexChange::Join(late_refilter, None));
        assert!(!owner_indexes.level_indexes[0]
            .live_slots_by_session_id
            .contains_key(&1));
        assert!(!owner_indexes
            .staged_subscribers_by_session_id
            .contains_key(&1));
        assert_eq!(received_items(&mut failing_rx), expected_failing_items);
        assert!(matches!(failing_rx.recv().now_or_never(), Some(None)));
        run_every_index_step(&mut owner_indexes);
        assert_claims_match_subscribers(&owner_indexes);
        let processed_index = &owner_indexes.level_indexes[0];
        assert_eq!(processed_index.key_claim_index.held_index_keys().len(), 2);
    }

    /// A level index whose live subscribers are all gone, but whose departed subscribers still hold
    /// claims, looks up no message of a batch.
    #[test]
    fn a_level_index_holding_only_claims_to_release_looks_up_no_message() {
        let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
        let account_pubkey = Pubkey::new_unique();
        let (indexed_subscriber, _stream_rx) =
            new_client(1, &accounts_request(&[account_pubkey], &[], None, &[]), 16);
        owner_indexes.take_index_change(IndexChange::Join(indexed_subscriber, None));
        leave_index(&mut owner_indexes, 1);
        let processed_index = &owner_indexes.level_indexes[0];
        assert!(processed_index.live_slots_by_session_id.is_empty());
        assert!(processed_index.key_claim_index.holds_slot_claims());
        let account_message = [Message::Account(fixtures::simple_message_account(
            account_pubkey,
            Pubkey::default(),
        ))];
        send_processed(&mut owner_indexes, &account_message);
        assert_eq!(owner_indexes.level_indexes[0].looked_up_message_count, 0);
    }

    /// A departed subscriber releases its per-kind keys as it leaves, and its other keys only in
    /// index steps. So does a staged subscriber whose claimed prefix reaches its per-kind keys.
    #[test]
    fn a_departed_subscriber_releases_its_per_kind_keys_at_once() {
        let mut owner_indexes = owner_indexes_with(16, SINGLE_KEY_STEP_LIMITS);
        let index_keys = sorted_key_list([
            numbered_index_key(1),
            IndexKey::Transaction(numbered_pubkey(2)),
            IndexKey::Slots,
            IndexKey::Entries,
            IndexKey::BlocksMeta,
        ]);
        let per_kind_start = index_keys.partition_point(|index_key| *index_key < IndexKey::Slots);
        assert_eq!(per_kind_start, 2);
        let (live_subscriber, _live_rx) = keyed_client(1, &index_keys);
        owner_indexes.take_index_change(IndexChange::Join(live_subscriber, None));
        leave_index(&mut owner_indexes, 1);
        let processed_index = &owner_indexes.level_indexes[0];
        for index_key in &index_keys[per_kind_start..] {
            assert_eq!(claim_count(processed_index, index_key, 1), 0);
        }
        for index_key in &index_keys[..per_kind_start] {
            assert_eq!(claim_count(processed_index, index_key, 1), 1);
        }
        assert_eq!(run_every_index_step(&mut owner_indexes), 2);
        assert_owner_holds_nothing(&owner_indexes);
        let (staged_subscriber, _staged_rx) = keyed_client(2, &index_keys);
        let _ready_rx = prepare_subscriber(&mut owner_indexes, staged_subscriber);
        for _ in 0..4 {
            owner_indexes.advance_index_step();
        }
        leave_index(&mut owner_indexes, 2);
        let processed_index = &owner_indexes.level_indexes[0];
        for index_key in index_keys.iter() {
            let expected_claims = usize::from(*index_key < IndexKey::Slots);
            assert_eq!(claim_count(processed_index, index_key, 2), expected_claims);
        }
        run_every_index_step(&mut owner_indexes);
        assert_owner_holds_nothing(&owner_indexes);
    }

    /// One index slice answers at most eight preparations, however large its budget.
    #[test]
    fn an_index_slice_answers_at_most_eight_preparations() {
        let large_budget_limits = IndexWorkLimits {
            slice_interval: Duration::ZERO,
            slice_budget: Duration::from_secs(3600),
            keys_per_step: INDEX_KEYS_PER_STEP,
        };
        let mut owner_indexes = owner_indexes_with(16, large_budget_limits);
        let slots_key_list = sorted_key_list([IndexKey::Slots]);
        let mut ready_rxs = (0..100)
            .map(|session_id| {
                let (staged_subscriber, _stream_rx) = keyed_client(session_id, &slots_key_list);
                prepare_subscriber(&mut owner_indexes, staged_subscriber)
            })
            .collect::<Vec<_>>();
        for answered_so_far in [8, 16] {
            owner_indexes.run_index_slice();
            let answered_count = ready_rxs
                .iter_mut()
                .filter(|ready_rx| !ready_rx.is_empty())
                .count();
            assert_eq!(answered_count, answered_so_far);
        }
    }

    /// Index work is overdue only once it has waited ten slice intervals: from when it started
    /// to wait, or from the end of the last slice. With no work, a slice is never overdue.
    #[tokio::test(start_paused = true)]
    async fn an_index_slice_is_overdue_only_after_ten_intervals_of_waiting_work() {
        let slice_interval = Duration::from_millis(1);
        let paused_limits = IndexWorkLimits {
            slice_interval,
            slice_budget: Duration::ZERO,
            keys_per_step: NonZeroUsize::MIN,
        };
        let mut owner_indexes = owner_indexes_with(16, paused_limits);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!owner_indexes.index_slice_is_overdue());
        let first_index_keys = sorted_key_list((0..4).map(numbered_index_key));
        let (first_subscriber, _first_rx) = keyed_client(1, &first_index_keys);
        let _first_ready_rx = prepare_subscriber(&mut owner_indexes, first_subscriber);
        tokio::time::advance(Duration::from_micros(9_999)).await;
        assert!(!owner_indexes.index_slice_is_overdue());
        // A change while work already waits keeps the time the work is overdue.
        let (second_subscriber, _second_rx) = keyed_client(2, &first_index_keys);
        let _second_ready_rx = prepare_subscriber(&mut owner_indexes, second_subscriber);
        tokio::time::advance(Duration::from_micros(1)).await;
        assert!(owner_indexes.index_slice_is_overdue());
        owner_indexes.run_index_slice();
        let slice_end = Instant::now();
        assert!(owner_indexes.has_index_work());
        assert!(!owner_indexes.index_slice_is_overdue());
        assert_eq!(
            owner_indexes.next_index_slice_at(),
            slice_end + slice_interval
        );
        tokio::time::advance(Duration::from_micros(9_999)).await;
        assert!(!owner_indexes.index_slice_is_overdue());
        tokio::time::advance(Duration::from_micros(1)).await;
        assert!(owner_indexes.index_slice_is_overdue());
        while owner_indexes.has_index_work() {
            owner_indexes.run_index_slice();
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!owner_indexes.index_slice_is_overdue());
        leave_index(&mut owner_indexes, 1);
        assert!(owner_indexes.has_index_work());
        assert!(!owner_indexes.index_slice_is_overdue());
    }

    /// Index work that a disconnect in a batch leaves waits ten slice intervals before it is
    /// overdue, as the work of an index change does. So the owner runs no slice at the top of its
    /// next pass only because its last slice ended long ago.
    #[tokio::test(start_paused = true)]
    async fn index_work_a_batch_disconnect_leaves_is_overdue_only_after_ten_intervals() {
        let paused_limits = IndexWorkLimits {
            slice_interval: Duration::from_millis(1),
            slice_budget: Duration::ZERO,
            keys_per_step: NonZeroUsize::MIN,
        };
        let mut owner_indexes = owner_indexes_with(16, paused_limits);
        let account_pubkey = Pubkey::new_unique();
        let account_only_request = accounts_request(&[account_pubkey], &[], None, &[]);
        let (indexed_subscriber, stream_rx) = new_client(1, &account_only_request, MAX_CAPACITY);
        owner_indexes.take_index_change(IndexChange::Join(indexed_subscriber, None));
        assert!(!owner_indexes.has_index_work());
        tokio::time::advance(Duration::from_secs(1)).await;
        drop(stream_rx);
        let account_message = fixtures::simple_message_account(account_pubkey, Pubkey::default());
        send_processed(&mut owner_indexes, &[Message::Account(account_message)]);
        assert!(!owner_indexes.level_indexes[0]
            .live_slots_by_session_id
            .contains_key(&1));
        assert!(owner_indexes.has_index_work());
        assert!(!owner_indexes.index_slice_is_overdue());
        tokio::time::advance(Duration::from_micros(9_999)).await;
        assert!(!owner_indexes.index_slice_is_overdue());
        tokio::time::advance(Duration::from_micros(1)).await;
        assert!(owner_indexes.index_slice_is_overdue());
    }

    /// While a departed subscriber's claims wait for release, index steps release them before they
    /// claim any key of a staged subscriber.
    #[test]
    fn releases_run_before_claims() {
        let mut owner_indexes = owner_indexes_with(16, SINGLE_KEY_STEP_LIMITS);
        let departed_index_keys = sorted_key_list((0..3).map(numbered_index_key));
        let (departed_subscriber, _departed_rx) = keyed_client(1, &departed_index_keys);
        owner_indexes.take_index_change(IndexChange::Join(departed_subscriber, None));
        leave_index(&mut owner_indexes, 1);
        let staged_index_keys = sorted_key_list((10..13).map(numbered_index_key));
        let (staged_subscriber, _staged_rx) = keyed_client(2, &staged_index_keys);
        let _ready_rx = prepare_subscriber(&mut owner_indexes, staged_subscriber);
        for released_key_count in 1..=3 {
            assert_eq!(owner_indexes.advance_index_step(), IndexStep::Advanced);
            let processed_index = &owner_indexes.level_indexes[0];
            let unreleased_claims = departed_index_keys
                .iter()
                .filter(|index_key| claim_count(processed_index, index_key, 1) == 1)
                .count();
            assert_eq!(unreleased_claims, 3 - released_key_count);
            assert_eq!(staged_key_count(&owner_indexes, 2), 0);
        }
        assert_eq!(owner_indexes.advance_index_step(), IndexStep::Advanced);
        assert_eq!(staged_key_count(&owner_indexes, 2), 1);
    }

    /// A warm owner that has no index work makes no allocator call in a pass: an empty take-in,
    /// the two index work checks and an indexed send.
    #[test]
    fn a_pass_with_no_index_work_makes_no_allocator_call() {
        let mut owner_indexes = owner_indexes_with(16, IndexWorkLimits::default());
        let _processed_rx = owner_indexes.broadcast_channels.subscribe(Processed);
        let (_index_changes_tx, mut index_changes_rx) = mpsc::unbounded_channel();
        let [account_pubkey, missing_pubkey] = std::array::from_fn(|_| Pubkey::new_unique());
        let mut stream_rxs = Vec::new();
        for session_id in 0..16 {
            let near_miss_request = accounts_request(&[account_pubkey], &[], Some(0), &[]);
            let (indexed_subscriber, stream_rx) = new_client(session_id, &near_miss_request, 16);
            owner_indexes.take_index_change(IndexChange::Join(indexed_subscriber, None));
            stream_rxs.push(stream_rx);
        }
        let account_data = [[0; 32], MEMCMP_BYTES].concat();
        let account_info =
            fixtures::account_info(account_pubkey, missing_pubkey, account_data, 1, None);
        let message_batch = Arc::new(vec![Message::Account(fixtures::message_account(
            account_info,
        ))]);
        owner_indexes.send_batch(Processed, Arc::clone(&message_batch));
        let mut work_found = false;
        let allocator_calls = allocator_calls_during(|| {
            for _ in 0..32 {
                owner_indexes.take_queued_index_changes(&mut index_changes_rx);
                work_found |= owner_indexes.has_index_work();
                work_found |= owner_indexes.index_slice_is_overdue();
                owner_indexes.send_batch(Processed, Arc::clone(&message_batch));
            }
        });
        assert_eq!(allocator_calls, 0);
        assert!(!work_found);
        assert_eq!(owner_indexes.level_indexes[0].looked_up_message_count, 33);
        for stream_rx in &mut stream_rxs {
            assert!(received_items(stream_rx).is_empty());
        }
    }

    /// Once a cycle has warmed the owner up, a preparation's take-in, its claim step and answer,
    /// a hand-over of an empty receiver and a leave with a receiver request make no allocator
    /// call, and nor do the releases that follow.
    #[test]
    fn a_warm_take_in_and_hand_over_make_no_allocator_call() {
        let mut owner_indexes = owner_indexes_with(16, STEP_EVERY_PASS_LIMITS);
        let index_keys = sorted_key_list(
            (0..8)
                .map(numbered_index_key)
                .chain([IndexKey::Slots, IndexKey::Entries]),
        );
        let (indexed_subscriber, _stream_rx) = keyed_client(1, &index_keys);
        let join_leave_cycle = |owner_indexes: &mut OwnerIndexes| {
            let (ready_tx, mut ready_rx) = oneshot::channel();
            let broadcast_rx = owner_indexes.broadcast_channels.subscribe(Processed);
            let (answer_tx, mut answer_rx) = oneshot::channel();
            let (prepared_subscriber, joined_subscriber) =
                (indexed_subscriber.clone(), indexed_subscriber.clone());
            let allocator_calls = allocator_calls_during(|| {
                owner_indexes
                    .take_index_change(IndexChange::Prepare(prepared_subscriber, ready_tx));
                while owner_indexes.has_index_work() {
                    owner_indexes.run_index_slice();
                }
                owner_indexes
                    .take_index_change(IndexChange::Join(joined_subscriber, Some(broadcast_rx)));
                let client_leave = IndexLeave {
                    session_id: 1,
                    receiver_request: Some((Processed, answer_tx)),
                };
                owner_indexes.take_index_change(IndexChange::Leave(client_leave));
                while owner_indexes.has_index_work() {
                    owner_indexes.run_index_slice();
                }
            });
            assert_eq!(ready_rx.try_recv(), Ok(()));
            assert!(answer_rx.try_recv().is_ok());
            assert_owner_holds_nothing(owner_indexes);
            allocator_calls
        };
        join_leave_cycle(&mut owner_indexes);
        assert_eq!(join_leave_cycle(&mut owner_indexes), 0);
    }

    /// How far the preparation of a client on the broadcast got, in
    /// [`indexed_clients_match_their_twins_under_random_index_work`].
    enum ModelledPreparation {
        /// The client sent no preparation, or left the one it sent.
        Unprepared,
        /// The client waits for the owner's answer.
        Preparing(oneshot::Receiver<()>),
        /// The owner answered, so the client hands over at its next empty read.
        Prepared,
    }

    /// Where a client of [`indexed_clients_match_their_twins_under_random_index_work`] stands, as
    /// `client_loop` would see it.
    enum ModelledStanding {
        /// On the broadcast, with its receiver.
        OnTheBroadcast(BroadcastReceiver, ModelledPreparation),
        /// The client handed its receiver over, so the owner delivers to it once the join lands.
        Indexed,
        /// The client sent a leave with a receiver request and waits for the answer.
        AwaitingReceiver(oneshot::Receiver<BroadcastReceiver>),
        /// The client left, or a full queue ended it.
        Ended,
    }

    /// How a modelled client's stream ended before the end of the run.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum EarlyEnd {
        /// The owner disconnected the client: its queue had no room for an indexed update.
        OwnerDisconnected,
        /// The client's own read of its receiver found no room in its queue.
        OwnReadOverflowed,
    }

    /// One client of [`indexed_clients_match_their_twins_under_random_index_work`]: what it holds,
    /// where it stands, and what the model says it gets.
    struct ModelledClient {
        /// The client's session id.
        session_id: usize,
        /// The client's subscriber template, which every indexed subscriber it sends copies.
        subscriber_template: IndexedSubscriber,
        /// The client's own sender into its queue, for what it reads from the broadcast.
        stream_tx: SubscribeSender,
        /// The receiving half of the client's queue.
        stream_rx: SubscribeReceiver,
        /// The client's current request.
        subscribe_request: SubscribeRequest,
        /// The client's current filter.
        client_filter: Arc<Filter>,
        /// The current filter's key list, kept as one [`Arc`] while the keys stay equal.
        index_keys: Option<Arc<[IndexKey]>>,
        /// Where the client stands.
        client_standing: ModelledStanding,
        /// The filter the owner delivers to the client with, by the model: set when a join lands,
        /// and cleared when a leave lands.
        indexed_filter: Option<Arc<Filter>>,
        /// The batches the client's current receiver holds unread, by the model.
        unread_batches: Option<Vec<BroadcastedMessage>>,
        /// What the client gets, by the model, in order.
        expected_items: QueueItems,
        /// What the client's queue held so far.
        received_items: QueueItems,
        /// How the client's stream ended early, if it did.
        early_end: Option<EarlyEnd>,
    }

    /// An index change in flight to the owner, as the model sees it.
    enum ModelledChange {
        /// A preparation.
        Prepare,
        /// A join with this filter, with the client's receiver or without one.
        Join(Arc<Filter>, bool),
        /// A leave, with a receiver request or without one.
        Leave(bool),
    }

    /// The owner and its clients in [`indexed_clients_match_their_twins_under_random_index_work`],
    /// with the index changes in flight between them.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut modelled_owner = ModelledOwner::new(SINGLE_KEY_STEP_LIMITS);
    /// modelled_owner.spawn_client(&mut seeded_rng, &pubkey_pool);
    /// modelled_owner.run_client_loop_top(0);
    /// modelled_owner.take_in_queued_changes();
    /// modelled_owner.send_batch(message_batch);
    /// modelled_owner.end_every_client();
    /// ```
    struct ModelledOwner {
        /// The owner under test.
        owner_indexes: OwnerIndexes,
        /// The clients, ended ones included.
        modelled_clients: Vec<ModelledClient>,
        /// Sends the clients' index changes to the owner.
        index_changes_tx: mpsc::UnboundedSender<IndexChange>,
        /// The owner's index change receiver.
        index_changes_rx: mpsc::UnboundedReceiver<IndexChange>,
        /// The client and the model of each index change in flight, oldest first.
        changes_in_flight: VecDeque<(usize, ModelledChange)>,
        /// How often each modelled event happened, by name, over the run.
        event_counts: HashMap<&'static str, usize>,
    }

    impl ModelledOwner {
        /// Creates an owner with no client, on channels that never lag in a run.
        ///
        /// # Arguments
        ///
        /// * `index_work_limits` - The owner's index limits.
        ///
        /// # Returns
        ///
        /// The modelled owner.
        fn new(index_work_limits: IndexWorkLimits) -> Self {
            let (index_changes_tx, index_changes_rx) = mpsc::unbounded_channel();
            Self {
                owner_indexes: owner_indexes_with(4096, index_work_limits),
                modelled_clients: Vec::new(),
                index_changes_tx,
                index_changes_rx,
                changes_in_flight: VecDeque::new(),
                event_counts: HashMap::default(),
            }
        }

        /// Counts one `event_name` event.
        ///
        /// # Arguments
        ///
        /// * `event_name` - The event's name.
        fn count_event(&mut self, event_name: &'static str) {
            *self.event_counts.entry(event_name).or_default() += 1;
        }

        /// Sends `index_change` of client `client_number` to the owner, and records its model.
        ///
        /// # Arguments
        ///
        /// * `client_number` - The client's position in `modelled_clients`.
        /// * `index_change` - The index change.
        /// * `modelled_change` - What the model makes of it when it lands.
        fn send_index_change(
            &mut self,
            client_number: usize,
            index_change: IndexChange,
            modelled_change: ModelledChange,
        ) {
            self.index_changes_tx.send(index_change).unwrap();
            self.changes_in_flight
                .push_back((client_number, modelled_change));
        }

        /// Starts a client with a random request and queue capacity, on the broadcast.
        ///
        /// # Arguments
        ///
        /// * `seeded_rng` - Picks the request and the capacity.
        /// * `pubkey_pool` - The pubkeys the request can name.
        fn spawn_client(&mut self, seeded_rng: &mut fastrand::Rng, pubkey_pool: &[Pubkey]) {
            let session_id = self.modelled_clients.len() + 1;
            let queue_capacity = [3, 16, MAX_CAPACITY][seeded_rng.usize(..3)];
            let (stream_tx, stream_rx) = load_aware_channel(queue_capacity, Default::default());
            let subscriber_template =
                IndexedSubscriber::new(session_id, "test", &stream_tx, &CancellationToken::new());
            let subscribe_request = random_request(seeded_rng, pubkey_pool);
            let client_filter = Arc::new(fixtures::filter_from_request(&subscribe_request));
            let index_keys = client_filter.index_keys().map(Arc::from);
            let broadcast_rx = self.owner_indexes.broadcast_channels.subscribe(Processed);
            self.modelled_clients.push(ModelledClient {
                session_id,
                subscriber_template,
                stream_tx,
                stream_rx,
                subscribe_request,
                client_filter,
                index_keys,
                client_standing: ModelledStanding::OnTheBroadcast(
                    broadcast_rx,
                    ModelledPreparation::Unprepared,
                ),
                indexed_filter: None,
                unread_batches: Some(Vec::new()),
                expected_items: Vec::new(),
                received_items: Vec::new(),
                early_end: None,
            });
        }

        /// Ends client `client_number` as its task's end would: its last leave goes out.
        ///
        /// # Arguments
        ///
        /// * `client_number` - The client's position in `modelled_clients`.
        /// * `early_end` - How its stream ended early, if it did.
        fn end_client(&mut self, client_number: usize, early_end: Option<EarlyEnd>) {
            let modelled_client = &mut self.modelled_clients[client_number];
            modelled_client.client_standing = ModelledStanding::Ended;
            modelled_client.early_end = early_end;
            let client_leave = IndexLeave {
                session_id: modelled_client.session_id,
                receiver_request: None,
            };
            let index_change = IndexChange::Leave(client_leave);
            self.send_index_change(client_number, index_change, ModelledChange::Leave(false));
        }

        /// Reads the receiver of client `client_number` to the end, as `client_loop` does, and
        /// checks that it holds what the model says.
        ///
        /// # Arguments
        ///
        /// * `client_number` - The client's position in `modelled_clients`.
        ///
        /// # Returns
        ///
        /// `false` if the client's queue had no room and the client ended, `true` otherwise.
        ///
        /// # Panics
        ///
        /// Panics if the receiver holds other batches than the model says, or if the client is
        /// not on the broadcast.
        fn read_to_the_end(&mut self, client_number: usize) -> bool {
            let modelled_client = &mut self.modelled_clients[client_number];
            let ModelledStanding::OnTheBroadcast(broadcast_rx, _) =
                &mut modelled_client.client_standing
            else {
                panic!("the client is not on the broadcast");
            };
            let unread_batches = modelled_client.unread_batches.replace(Vec::new()).unwrap();
            for unread_batch in &unread_batches {
                let read_batch = broadcast_rx.try_recv().expect("an unread batch");
                assert!(Arc::ptr_eq(&read_batch, unread_batch));
            }
            assert!(broadcast_rx.is_empty());
            let client_filter = Arc::clone(&modelled_client.client_filter);
            let read_messages = unread_batches
                .iter()
                .flat_map(|unread_batch| unread_batch.iter());
            let queue_had_room = read_messages
                .flat_map(|read_message| client_filter.get_updates(read_message, Some(Processed)))
                .all(|filtered_update| {
                    let queued_update = Ok(filtered_update.clone());
                    modelled_client.expected_items.push(Ok(filtered_update));
                    modelled_client.stream_tx.try_send(queued_update).is_ok()
                });
            if !queue_had_room {
                self.end_client(client_number, Some(EarlyEnd::OwnReadOverflowed));
                self.count_event("own read overflow");
            }
            queue_had_room
        }

        /// One pass of client `client_number`'s loop: on the broadcast it reads to the end, then
        /// prepares, notes the answer, or hands over; waiting for a receiver it takes the answer.
        ///
        /// # Arguments
        ///
        /// * `client_number` - The client's position in `modelled_clients`.
        fn run_client_loop_top(&mut self, client_number: usize) {
            let modelled_client = &mut self.modelled_clients[client_number];
            match &mut modelled_client.client_standing {
                ModelledStanding::OnTheBroadcast(..) => {}
                ModelledStanding::AwaitingReceiver(answer_rx) => {
                    if let Ok(broadcast_rx) = answer_rx.try_recv() {
                        modelled_client.client_standing = ModelledStanding::OnTheBroadcast(
                            broadcast_rx,
                            ModelledPreparation::Unprepared,
                        );
                    }
                    return;
                }
                ModelledStanding::Indexed | ModelledStanding::Ended => return,
            }
            if !self.read_to_the_end(client_number) {
                return;
            }
            let modelled_client = &mut self.modelled_clients[client_number];
            let Some(index_keys) = modelled_client.index_keys.clone() else {
                return;
            };
            let indexed_subscriber = modelled_client
                .subscriber_template
                .with_filter(&modelled_client.client_filter, &index_keys);
            let ModelledStanding::OnTheBroadcast(_, preparation) =
                &mut modelled_client.client_standing
            else {
                unreachable!("the client read its receiver");
            };
            let change_to_send = match preparation {
                ModelledPreparation::Unprepared => {
                    let (ready_tx, ready_rx) = oneshot::channel();
                    *preparation = ModelledPreparation::Preparing(ready_rx);
                    let index_change = IndexChange::Prepare(indexed_subscriber, ready_tx);
                    Some((index_change, ModelledChange::Prepare))
                }
                ModelledPreparation::Preparing(ready_rx) => {
                    match ready_rx.try_recv() {
                        Ok(()) => {
                            let session_id = modelled_client.session_id;
                            let staged_subscribers =
                                &self.owner_indexes.staged_subscribers_by_session_id;
                            let claimed_key_count =
                                staged_subscribers[&session_id].claimed_key_count;
                            assert_eq!(claimed_key_count, index_keys.len(), "answered early");
                            *preparation = ModelledPreparation::Prepared;
                        }
                        Err(oneshot::error::TryRecvError::Empty) => {}
                        Err(oneshot::error::TryRecvError::Closed) => {
                            panic!("the owner dropped a preparation")
                        }
                    }
                    None
                }
                ModelledPreparation::Prepared => {
                    let broadcast_standing = std::mem::replace(
                        &mut modelled_client.client_standing,
                        ModelledStanding::Indexed,
                    );
                    let ModelledStanding::OnTheBroadcast(broadcast_rx, _) = broadcast_standing
                    else {
                        unreachable!("the client read its receiver");
                    };
                    let joined_filter = Arc::clone(&modelled_client.client_filter);
                    let index_change = IndexChange::Join(indexed_subscriber, Some(broadcast_rx));
                    Some((index_change, ModelledChange::Join(joined_filter, true)))
                }
            };
            if let Some((index_change, modelled_change)) = change_to_send {
                if matches!(modelled_change, ModelledChange::Join(..)) {
                    self.count_event("hand-over sent");
                }
                self.send_index_change(client_number, index_change, modelled_change);
            }
        }

        /// Changes the filter of client `client_number`, as `client_loop` does once its owner
        /// runs two-phase joins: a same-key change of an indexed client replaces the indexed
        /// subscriber's filter, and any other change of an indexed client leaves with a receiver
        /// request. On the broadcast, a key change leaves the preparation.
        ///
        /// # Arguments
        ///
        /// * `client_number` - The client's position in `modelled_clients`.
        /// * `seeded_rng` - Picks the new request.
        /// * `pubkey_pool` - The pubkeys the request can name.
        fn change_filter(
            &mut self,
            client_number: usize,
            seeded_rng: &mut fastrand::Rng,
            pubkey_pool: &[Pubkey],
        ) {
            let modelled_client = &mut self.modelled_clients[client_number];
            if matches!(
                modelled_client.client_standing,
                ModelledStanding::AwaitingReceiver(_) | ModelledStanding::Ended
            ) {
                return;
            }
            let new_request = if seeded_rng.bool() {
                let mut same_key_request = modelled_client.subscribe_request.clone();
                let data_slices = &mut same_key_request.accounts_data_slice;
                *data_slices = if data_slices.is_empty() {
                    let data_slice = SubscribeRequestAccountsDataSlice {
                        offset: 1,
                        length: 2,
                    };
                    vec![data_slice]
                } else {
                    Vec::new()
                };
                same_key_request
            } else {
                random_request(seeded_rng, pubkey_pool)
            };
            let new_filter = Arc::new(fixtures::filter_from_request(&new_request));
            let new_index_keys = match (new_filter.index_keys(), &modelled_client.index_keys) {
                (Some(listed_keys), Some(held_keys)) if held_keys[..] == listed_keys[..] => {
                    Some(Arc::clone(held_keys))
                }
                (listed_keys, _) => listed_keys.map(Arc::from),
            };
            let subscriber_moved = match (&new_index_keys, &modelled_client.index_keys) {
                (Some(new_keys), Some(held_keys)) => !Arc::ptr_eq(new_keys, held_keys),
                (new_keys, held_keys) => new_keys.is_some() != held_keys.is_some(),
            };
            modelled_client.subscribe_request = new_request;
            modelled_client.client_filter = Arc::clone(&new_filter);
            modelled_client.index_keys = new_index_keys.clone();
            let session_id = modelled_client.session_id;
            let (index_change, modelled_change, event_name) = match &mut modelled_client
                .client_standing
            {
                ModelledStanding::Indexed if !subscriber_moved => {
                    let index_keys = new_index_keys.expect("an indexed client's keys");
                    let refiltered_subscriber = modelled_client
                        .subscriber_template
                        .with_filter(&new_filter, &index_keys);
                    let index_change = IndexChange::Join(refiltered_subscriber, None);
                    let modelled_change = ModelledChange::Join(new_filter, false);
                    (index_change, modelled_change, "same-key refilter sent")
                }
                ModelledStanding::Indexed => {
                    let (answer_tx, answer_rx) = oneshot::channel();
                    modelled_client.client_standing = ModelledStanding::AwaitingReceiver(answer_rx);
                    let client_leave = IndexLeave {
                        session_id,
                        receiver_request: Some((Processed, answer_tx)),
                    };
                    let index_change = IndexChange::Leave(client_leave);
                    (
                        index_change,
                        ModelledChange::Leave(true),
                        "key-changing swap sent",
                    )
                }
                ModelledStanding::OnTheBroadcast(_, preparation) => {
                    if !subscriber_moved || matches!(preparation, ModelledPreparation::Unprepared) {
                        return;
                    }
                    *preparation = ModelledPreparation::Unprepared;
                    let client_leave = IndexLeave {
                        session_id,
                        receiver_request: None,
                    };
                    let index_change = IndexChange::Leave(client_leave);
                    (
                        index_change,
                        ModelledChange::Leave(false),
                        "preparation left",
                    )
                }
                ModelledStanding::AwaitingReceiver(_) | ModelledStanding::Ended => {
                    unreachable!("checked above")
                }
            };
            self.count_event(event_name);
            self.send_index_change(client_number, index_change, modelled_change);
        }

        /// Ends, through its last leave, each client that an owner disconnected since the last
        /// check.
        fn end_disconnected_clients(&mut self) {
            for client_number in 0..self.modelled_clients.len() {
                let modelled_client = &self.modelled_clients[client_number];
                let subscriber_template = &modelled_client.subscriber_template;
                let disconnected_by_owner = subscriber_template.disconnect_reason().is_some();
                if disconnected_by_owner && modelled_client.early_end.is_none() {
                    self.modelled_clients[client_number].indexed_filter = None;
                    self.end_client(client_number, Some(EarlyEnd::OwnerDisconnected));
                    self.count_event("owner disconnect");
                }
            }
        }

        /// One owner pass: takes in what the owner's channel holds, at most 64 changes, and
        /// applies the model of each change that landed.
        fn take_in_queued_changes(&mut self) {
            let landed_count = self.changes_in_flight.len().min(MAX_INDEX_CHANGES_PER_PASS);
            self.owner_indexes
                .take_queued_index_changes(&mut self.index_changes_rx);
            let landed_changes = self.changes_in_flight.drain(..landed_count);
            for (client_number, modelled_change) in landed_changes {
                let modelled_client = &mut self.modelled_clients[client_number];
                if modelled_client.early_end == Some(EarlyEnd::OwnerDisconnected) {
                    continue;
                }
                match modelled_change {
                    ModelledChange::Prepare => {}
                    ModelledChange::Join(joined_filter, true) => {
                        let handed_over_batches = modelled_client.unread_batches.take().unwrap();
                        if !handed_over_batches.is_empty() {
                            *self.event_counts.entry("backlog drained").or_default() += 1;
                        }
                        let drained_messages = handed_over_batches
                            .iter()
                            .flat_map(|handed_over_batch| handed_over_batch.iter());
                        for drained_message in drained_messages {
                            let filtered_updates =
                                joined_filter.get_updates(drained_message, Some(Processed));
                            modelled_client
                                .expected_items
                                .extend(filtered_updates.into_iter().map(Ok));
                        }
                        modelled_client.indexed_filter = Some(joined_filter);
                    }
                    ModelledChange::Join(joined_filter, false) => {
                        if modelled_client.indexed_filter.is_some() {
                            modelled_client.indexed_filter = Some(joined_filter);
                            *self
                                .event_counts
                                .entry("live subscriber refiltered")
                                .or_default() += 1;
                        }
                    }
                    ModelledChange::Leave(true) => {
                        modelled_client.indexed_filter = None;
                        modelled_client.unread_batches = Some(Vec::new());
                    }
                    ModelledChange::Leave(false) => modelled_client.indexed_filter = None,
                }
            }
            self.end_disconnected_clients();
        }

        /// Sends `message_batch` through the owner, after checking the index's claims, and
        /// applies its model: an indexed client gets what its indexed filter makes, and a client's
        /// receiver holds the batch.
        ///
        /// # Arguments
        ///
        /// * `message_batch` - The batch to send.
        fn send_batch(&mut self, message_batch: Vec<Message>) {
            assert_claims_match_subscribers(&self.owner_indexes);
            let message_batch = Arc::new(message_batch);
            for modelled_client in &mut self.modelled_clients {
                if let Some(indexed_filter) = &modelled_client.indexed_filter {
                    for sent_message in message_batch.iter() {
                        let filtered_updates =
                            indexed_filter.get_updates(sent_message, Some(Processed));
                        modelled_client
                            .expected_items
                            .extend(filtered_updates.into_iter().map(Ok));
                    }
                }
                if let Some(unread_batches) = &mut modelled_client.unread_batches {
                    unread_batches.push(Arc::clone(&message_batch));
                }
            }
            self.owner_indexes.send_batch(Processed, message_batch);
            self.end_disconnected_clients();
        }

        /// Moves what the queue of client `client_number` holds into its received items, as the
        /// client's stream does when it keeps up.
        ///
        /// # Arguments
        ///
        /// * `client_number` - The client's position in `modelled_clients`.
        fn drain_queue(&mut self, client_number: usize) {
            let modelled_client = &mut self.modelled_clients[client_number];
            let queued_items = received_items(&mut modelled_client.stream_rx);
            modelled_client.received_items.extend(queued_items);
        }

        /// Moves what each client's queue holds into its received items.
        fn drain_queues(&mut self) {
            for client_number in 0..self.modelled_clients.len() {
                self.drain_queue(client_number);
            }
        }

        /// Ends every client, lands every change, runs every index step, and checks that the
        /// owner holds nothing and that each client got what the model says.
        ///
        /// # Panics
        ///
        /// Panics if anything is left at the owner, if a queue stays open, or if a client got
        /// other updates than the model says.
        fn end_every_client(mut self) -> HashMap<&'static str, usize> {
            for client_number in 0..self.modelled_clients.len() {
                if !matches!(
                    self.modelled_clients[client_number].client_standing,
                    ModelledStanding::Ended
                ) {
                    self.end_client(client_number, None);
                }
            }
            while !self.changes_in_flight.is_empty() {
                self.take_in_queued_changes();
            }
            while self.owner_indexes.has_index_work() {
                self.owner_indexes.run_index_slice();
            }
            assert_owner_holds_nothing(&self.owner_indexes);
            self.drain_queues();
            for modelled_client in std::mem::take(&mut self.modelled_clients) {
                let ModelledClient {
                    session_id,
                    subscriber_template,
                    stream_tx,
                    mut stream_rx,
                    expected_items,
                    received_items,
                    early_end,
                    ..
                } = modelled_client;
                drop((subscriber_template, stream_tx));
                assert!(
                    matches!(stream_rx.recv().now_or_never(), Some(None)),
                    "session {session_id}"
                );
                match early_end {
                    None => assert_eq!(received_items, expected_items, "session {session_id}"),
                    Some(EarlyEnd::OwnerDisconnected) => {
                        let (last_item, delivered_items) = received_items.split_last().unwrap();
                        assert_eq!(*last_item, Err("lagged to send an update".to_owned()));
                        assert_eq!(delivered_items, &expected_items[..delivered_items.len()]);
                    }
                    Some(EarlyEnd::OwnReadOverflowed) => {
                        assert!(received_items.len() < expected_items.len());
                        assert_eq!(received_items, expected_items[..received_items.len()]);
                    }
                }
            }
            self.event_counts
        }
    }

    /// Random preparations, hand-overs after answers, same-key refilters, key-changing swaps
    /// through a leave and a new preparation, leaves, ends, full queues, index slices and sends,
    /// 300 steps per seed: before every send the claims are exactly those of the indexed
    /// subscribers, each queue gets what the filter in effect makes of every batch, in order, and
    /// the owner holds nothing once every client has left.
    #[test]
    fn indexed_clients_match_their_twins_under_random_index_work() {
        let pubkey_pool = fixtures::deterministic_pubkeys(5, 8);
        let mut event_counts = HashMap::<&'static str, usize>::default();
        for rng_seed in 0..60 {
            let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
            let index_work_limits = [SINGLE_KEY_STEP_LIMITS, STEP_EVERY_PASS_LIMITS];
            let mut modelled_owner = ModelledOwner::new(index_work_limits[rng_seed as usize % 2]);
            for _ in 0..4 {
                modelled_owner.spawn_client(&mut seeded_rng, &pubkey_pool);
            }
            for _ in 0..300 {
                let client_count = modelled_owner.modelled_clients.len();
                let client_number = seeded_rng.usize(..client_count);
                match seeded_rng.u8(..100) {
                    0..=19 => modelled_owner.run_client_loop_top(client_number),
                    20..=34 => {
                        // An indexed client's stream drains only in the rarer global drains.
                        let modelled_client = &modelled_owner.modelled_clients[client_number];
                        if !matches!(modelled_client.client_standing, ModelledStanding::Indexed) {
                            modelled_owner.drain_queue(client_number);
                        }
                        modelled_owner.run_client_loop_top(client_number);
                    }
                    35..=39 => {
                        modelled_owner.change_filter(client_number, &mut seeded_rng, &pubkey_pool)
                    }
                    40..=42 => {
                        if !matches!(
                            modelled_owner.modelled_clients[client_number].client_standing,
                            ModelledStanding::Ended
                        ) {
                            modelled_owner.end_client(client_number, None);
                        }
                        modelled_owner.spawn_client(&mut seeded_rng, &pubkey_pool);
                    }
                    43..=59 => modelled_owner.take_in_queued_changes(),
                    60..=74 => modelled_owner.owner_indexes.run_index_slice(),
                    75..=97 => {
                        let message_batch = (0..seeded_rng.usize(1..=4))
                            .map(|_| random_message(&mut seeded_rng, &pubkey_pool))
                            .collect();
                        modelled_owner.send_batch(message_batch);
                    }
                    _ => modelled_owner.drain_queues(),
                }
            }
            for (event_name, event_count) in modelled_owner.end_every_client() {
                *event_counts.entry(event_name).or_default() += event_count;
            }
        }
        for event_name in [
            "hand-over sent",
            "backlog drained",
            "same-key refilter sent",
            "live subscriber refiltered",
            "key-changing swap sent",
            "preparation left",
            "owner disconnect",
            "own read overflow",
        ] {
            let event_count = event_counts.get(event_name).copied().unwrap_or_default();
            assert!(
                event_count >= 10,
                "{event_name}: {event_count} of {event_counts:?}"
            );
        }
    }
}
