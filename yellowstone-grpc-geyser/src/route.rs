//! Routes updates straight into the queues of the clients whose route keys find them.
//!
//! Each commitment level has one owner, its only sender: `geyser_loop` at processed, and block
//! reconstruction at confirmed and finalized. An owner sends each batch through its
//! [`OwnerRoutes`], which routes the batch to the routed clients at that level, then broadcasts it
//! for the others.
//!
//! A client is routed when its filter has route keys, from [`Filter::route_keys`]: every message
//! the filter matches carries one of them. Its [`Route`] sits in its owner's inverted index under
//! each of those [`RouteKey`]s. For each message, the owner looks up the route keys the message
//! carries, such as an account's pubkey and owner program, to find the candidate subscribers. It
//! applies each candidate subscriber's filter with [`Filter::get_updates`] for the exact match, and
//! queues the updates straight into the client's queue, so only the clients that get an update
//! wake. A client whose filter gets [`None`] from [`Filter::route_keys`] stays on the broadcast,
//! and its own task filters every batch.
//!
//! A client joins and leaves by sending a [`RouteChange`] to its level's owner, which takes it in
//! between two of its sends. Each level has one sender, so a client that moves between the
//! broadcast and its route neither misses nor repeats an update at that level:
//!
//! - [`RouteChange::Prepare`] stages a route. The owner claims the route's keys in index slices,
//!   then answers, and the client reads its broadcast receiver meanwhile.
//! - [`RouteChange::Join`] makes a route live between two sends, and claims at once any of its keys
//!   not claimed yet. With the client's broadcast receiver, read to the end, the owner first
//!   delivers through the route what the receiver got since. A join without a receiver replaces
//!   the filter of the live route with the same key list.
//! - [`RouteChange::Leave`] removes a route at once. A client that goes back to the broadcast asks
//!   for a receiver, which the owner subscribes right after its last routed send.
//!
//! A client joins in two steps, once it has read its broadcast receiver to the end: a
//! [`RouteChange::Prepare`], then, once the owner answers, a [`RouteChange::Join`] with the
//! receiver. So the client reads its own backlog, and the owner drains only what the receiver got
//! since the client's last read. A filter change to other keys or another level leaves the route
//! for a receiver and joins again the same way. A change that keeps the key list [`Arc`] replaces
//! the live route's filter in place.
//!
//! The owner claims and releases route keys in index slices, at its idle points. Every key of a
//! live route holds a claim for the route's session id, and the index counts claims, so claims
//! and releases can run in any order.
//!
//! [`LeaveOnDrop`] sends the last leave when the client's task ends. An owner disconnects a routed
//! client when its queue is full or closed, its filter panics, or its handed-over receiver lagged.
//! The owner records the reason, closes the queue behind what the queue holds, and cancels the
//! client's cancellation token.

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
    prometheus::IntCounter,
    smallvec::{smallvec, SmallVec},
    solana_pubkey::Pubkey,
    std::{
        collections::VecDeque,
        hash::BuildHasher,
        num::NonZeroUsize,
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{Arc, OnceLock},
        time::Duration,
    },
    tokio::{
        sync::{
            broadcast::{self, error::TryRecvError},
            mpsc, oneshot,
        },
        time::Instant,
    },
    tokio_util::sync::CancellationToken,
    tonic::Status,
};

/// The most route keys a routed filter can have. A filter with more stays on the broadcast: the
/// cap bounds one route's index work, and a join that claims all its keys between two sends.
pub const MAX_ROUTE_KEYS: usize = 8192;

/// The account data offsets a 32-byte memcmp is indexed at. A token account holds its mint at
/// offset 0 and its owner at offset 32.
pub const INDEXED_MEMCMP_OFFSETS: [usize; 2] = [0, 32];

/// The most route changes an owner takes in per pass.
pub const MAX_ROUTE_CHANGES_PER_PASS: usize = 64;

/// The most route keys one index step claims or releases.
pub const ROUTE_KEYS_PER_INDEX_STEP: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// The most preparations one index slice answers, so the hand-overs that follow come spread out.
const MAX_ANSWERS_PER_INDEX_SLICE: usize = 8;

/// How many slice intervals index work waits before a slice runs at the top of a busy pass.
const INDEX_SLICE_OVERDUE_INTERVALS: u32 = 10;

/// How much index work an owner does, and how often.
///
/// Index work claims and releases route keys for route changes. The owner runs it in slices,
/// with [`OwnerRoutes::run_index_slice`], and each slice runs index steps of at most
/// `keys_per_step` keys.
///
/// # Examples
///
/// ```ignore
/// let route_index_limits = RouteIndexLimits {
///     slice_interval: Duration::from_millis(1),
///     slice_budget: Duration::from_micros(100),
///     keys_per_step: ROUTE_KEYS_PER_INDEX_STEP,
/// };
/// let (broadcast_channels, route_changes_rxs) =
///     SubscriberChannels::new(1024, 1024, 1024).with_routing(route_index_limits);
/// assert_eq!(broadcast_channels.route_index_limits().keys_per_step.get(), 64);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct RouteIndexLimits {
    /// The shortest time between two index slices.
    pub slice_interval: Duration,
    /// The longest one slice runs, plus one step.
    pub slice_budget: Duration,
    /// The most route keys one step claims or releases.
    pub keys_per_step: NonZeroUsize,
}

impl Default for RouteIndexLimits {
    /// The limits the plugin runs with unless its config sets others.
    ///
    /// # Returns
    ///
    /// A slice interval of 1 ms, a slice budget of 100 µs, and [`ROUTE_KEYS_PER_INDEX_STEP`]
    /// keys per step.
    fn default() -> Self {
        Self {
            slice_interval: Duration::from_millis(1),
            slice_budget: Duration::from_micros(100),
            keys_per_step: ROUTE_KEYS_PER_INDEX_STEP,
        }
    }
}

/// A key that finds candidate subscribers in an owner's index.
///
/// An owner indexes each route under its filter's route keys, and a message carries the route
/// keys of what it is about. A subscriber is a candidate for a message when the message carries
/// one of its route keys.
///
/// The per-kind keys, from [`RouteKey::Slots`] on, sort after every account, owner, memcmp and
/// transaction key (the derived [`Ord`]), so they are the end of a sorted key list.
/// [`Filter::route_keys`] sorts its keys and drops repeats, so each key list is a set in that
/// order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RouteKey {
    /// An update of the account with this pubkey.
    Account(Pubkey),
    /// An update of an account this program owns.
    Owner(Pubkey),
    /// An update of an account whose data holds these 32 bytes at this offset, one of
    /// [`INDEXED_MEMCMP_OFFSETS`].
    Memcmp(usize, [u8; 32]),
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

/// A client's change to its route, sent to the owner of its level and taken in by
/// [`OwnerRoutes::take_route_change`] between two of the owner's sends.
pub enum RouteChange {
    /// Stages the route. The owner claims its keys in index slices, then answers on the sender.
    /// The client keeps reading its broadcast receiver until then.
    Prepare(Route, oneshot::Sender<()>),
    /// Makes the route live between two sends. With the receiver the client read to the end, the
    /// owner first delivers through the route what the receiver got since. Without one, when the
    /// session's live route at this level holds the same key list [`Arc`], the owner only
    /// replaces that route's filter. Otherwise it claims at once every key the session has not
    /// claimed for this route, which is none after an answered [`RouteChange::Prepare`] of the
    /// same key list.
    Join(Route, Option<BroadcastReceiver>),
    /// Removes the session's route at every level, staged or live, then answers the receiver
    /// request, if any, at once. The route's per-kind keys are released at once, and its other
    /// keys in later index steps.
    Leave(RouteLeave),
}

/// A client's leave: which route to remove, and whether the client wants a receiver back.
pub struct RouteLeave {
    /// The session id of the route to remove.
    pub session_id: usize,
    /// A level and an answer sender, or [`None`]. With them, the owner answers with a receiver on
    /// that level's channel, subscribed right after its last routed send.
    pub receiver_request: Option<(CommitmentLevel, oneshot::Sender<BroadcastReceiver>)>,
}

/// Sends a leave to the owners when it drops, so they remove the client's route once the
/// client's task ends. A route holds a clone of the client's queue sender, so a route left
/// behind would keep the client's stream open.
///
/// Both owners get the leave, unless [`LeaveOnDrop::leave_only`] named the owner that holds the
/// route, or said that none does.
///
/// # Examples
///
/// ```ignore
/// let mut leave_on_drop = LeaveOnDrop {
///     session_id: session.id,
///     route_change_txs: route_change_txs.clone().map(Some),
/// };
/// let _ = route_change_txs[0].send(RouteChange::Join(new_route, Some(broadcast_rx)));
/// leave_on_drop.leave_only(Some(CommitmentLevel::Processed));
/// drop(leave_on_drop);
/// // geyser_loop gets a leave of `session.id` with no receiver request, and block
/// // reconstruction gets nothing.
/// ```
pub struct LeaveOnDrop {
    /// The client's session id.
    pub session_id: usize,
    /// The route change senders of `geyser_loop` and block reconstruction, in that order. Each
    /// sender left here gets the leave.
    pub route_change_txs: [Option<mpsc::UnboundedSender<RouteChange>>; 2],
}

impl LeaveOnDrop {
    /// Keeps only the sender of the owner that holds the client's route, staged or live.
    ///
    /// # Arguments
    ///
    /// * `route_level` - The commitment level of the client's route, whose owner gets the leave,
    ///   or [`None`] when no owner holds a route of the client, and no leave goes out.
    pub fn leave_only(&mut self, route_level: Option<CommitmentLevel>) {
        let route_owner_index =
            route_level.map(|route_level| usize::from(route_level != CommitmentLevel::Processed));
        for (owner_index, route_change_tx) in self.route_change_txs.iter_mut().enumerate() {
            if route_owner_index != Some(owner_index) {
                *route_change_tx = None;
            }
        }
    }
}

impl Drop for LeaveOnDrop {
    /// Sends a [`RouteChange::Leave`] with no receiver request to each owner whose sender is
    /// left, skipping an owner that has stopped.
    fn drop(&mut self) {
        for route_change_tx in self.route_change_txs.iter().flatten() {
            let client_leave = RouteLeave {
                session_id: self.session_id,
                receiver_request: None,
            };
            let _ = route_change_tx.send(RouteChange::Leave(client_leave));
        }
    }
}

/// Why an owner disconnects a routed client.
enum RouteFailure {
    /// The client's queue has no room for an update.
    Full,
    /// The client's queue is closed.
    Closed,
    /// The client's filter panicked.
    Panicked,
    /// The receiver the client handed over lagged behind its channel.
    Lagged,
}

/// One client's entry in an owner's index: its filter and route keys, and what the owner needs
/// to deliver to the client and to disconnect it.
///
/// Clones share the client's queue, cancellation token, disconnect reason and message sent counter.
/// So `client_loop` keeps one from [`Route::new`] as a template, sends a copy made by
/// [`Route::with_filter`] with each filter, and reads [`Route::disconnect_reason`] from the
/// template when it ends.
///
/// # Examples
///
/// ```ignore
/// let route_template =
///     Route::new(session.id, &session.subscriber_id, &stream_tx, &cancellation_token);
/// let route_keys: Arc<[RouteKey]> =
///     client_filter.route_keys().expect("an indexable filter").into();
/// let new_route = route_template.with_filter(&client_filter, &route_keys);
/// owner_routes.take_route_change(RouteChange::Join(new_route, None));
/// // Once the client's task ends:
/// let disconnect_reason = route_template.disconnect_reason();
/// ```
#[derive(Clone)]
pub struct Route {
    /// The client's session id.
    session_id: usize,
    /// The client's filter, which makes the exact match for each candidate message.
    client_filter: Arc<Filter>,
    /// The filter's route keys, which the route is indexed under.
    route_keys: Arc<[RouteKey]>,
    /// The client's Subscribe queue.
    stream_tx: SubscribeSender,
    /// The client's cancellation token, cancelled when an owner disconnects the client.
    cancellation_token: CancellationToken,
    /// Why an owner disconnected the client, set once for every clone.
    disconnect_reason: Arc<OnceLock<&'static str>>,
    /// The sent-updates counter of the client's subscriber id, shared by every client with that id.
    message_sent_counter: IntCounter,
}

impl Route {
    /// Creates a client's route template, with the default filter, which matches nothing, and no
    /// keys.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
    /// * `subscriber_id` - The subscriber id whose [`metrics::grpc_message_sent_counter`] the
    ///   route bumps for each queued update.
    /// * `stream_tx` - The client's Subscribe queue.
    /// * `cancellation_token` - The client's cancellation token.
    ///
    /// # Returns
    ///
    /// The template, which [`Route::with_filter`] copies with a filter to route.
    pub fn new(
        session_id: usize,
        subscriber_id: &str,
        stream_tx: &SubscribeSender,
        cancellation_token: &CancellationToken,
    ) -> Self {
        Self {
            session_id,
            client_filter: Arc::default(),
            route_keys: Arc::default(),
            stream_tx: stream_tx.clone(),
            cancellation_token: cancellation_token.clone(),
            disconnect_reason: Arc::default(),
            message_sent_counter: metrics::grpc_message_sent_counter(subscriber_id),
        }
    }

    /// Copies this route with a new filter and its keys.
    ///
    /// # Arguments
    ///
    /// * `client_filter` - The client's filter.
    /// * `route_keys` - The filter's route keys, from [`Filter::route_keys`].
    ///
    /// # Returns
    ///
    /// The copy, which shares this route's queue, cancellation token, disconnect reason and
    /// message sent counter.
    pub fn with_filter(&self, client_filter: &Arc<Filter>, route_keys: &Arc<[RouteKey]>) -> Self {
        Self {
            client_filter: Arc::clone(client_filter),
            route_keys: Arc::clone(route_keys),
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

    /// The commitment level of the route's filter, which picks its owner and its router.
    ///
    /// # Returns
    ///
    /// The filter's [`CommitmentLevel`], from [`Filter::get_commitment_level`].
    fn commitment_level(&self) -> CommitmentLevel {
        self.client_filter.get_commitment_level()
    }

    /// Applies the filter to `routed_message` and queues each update it makes, without waiting.
    ///
    /// # Arguments
    ///
    /// * `routed_message` - A message the route is a candidate for.
    ///
    /// # Returns
    ///
    /// `Ok(())` once every update is queued, or when the filter makes none.
    ///
    /// # Errors
    ///
    /// [`RouteFailure::Panicked`] if the filter panics, [`RouteFailure::Full`] if the queue has no
    /// room for an update, and [`RouteFailure::Closed`] if the queue is closed. The updates queued
    /// before the failure stay queued.
    fn queue_updates(&self, routed_message: &Message) -> Result<(), RouteFailure> {
        let filtered_updates = catch_unwind(AssertUnwindSafe(|| {
            self.client_filter
                .get_updates(routed_message, Some(self.commitment_level()))
        }))
        .map_err(|_| RouteFailure::Panicked)?;
        for filtered_update in filtered_updates {
            match self.stream_tx.try_send(Ok(filtered_update)) {
                Ok(()) => self.message_sent_counter.inc(),
                Err(TrySendError::Full(_)) => return Err(RouteFailure::Full),
                Err(TrySendError::Closed(_)) => return Err(RouteFailure::Closed),
            }
        }
        Ok(())
    }

    /// Disconnects the client for `route_failure`: records the reason, closes the queue behind what
    /// it holds, and cancels the client's cancellation token. Only the first call on any clone does
    /// anything.
    ///
    /// A full queue, a closed queue and a lagged receiver get the reasons and the error statuses
    /// that [`GrpcService::client_loop`](crate::grpc::GrpcService::client_loop) uses on the
    /// broadcast. A closed queue gets no status.
    ///
    /// # Arguments
    ///
    /// * `route_failure` - Why the owner disconnects the client.
    fn disconnect_client(&self, route_failure: RouteFailure) {
        let (disconnect_reason, status_message) = match route_failure {
            RouteFailure::Full => ("client_channel_full", Some("lagged to send an update")),
            RouteFailure::Closed => ("client_closed", None),
            RouteFailure::Panicked => ("filter_failed", Some("failed to apply the filter")),
            RouteFailure::Lagged => (
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

/// The shards of a level's route index. A shard grows on its own, so one growth moves about
/// 1/4096 of the index.
const ROUTE_INDEX_SHARDS: usize = 4096;

/// One route key in the index, and the session ids that claim it, one copy per claim.
struct IndexedRouteKey {
    /// The route key.
    route_key: RouteKey,
    /// The session ids of the routes that claim the key, one copy per claim, in no order. Two
    /// claims fit inline, so the list allocates only from its third claim on.
    claiming_session_ids: SmallVec<[usize; 2]>,
}

/// A level's inverted index, in [`ROUTE_INDEX_SHARDS`] hash tables that share one hash.
///
/// A route key's hash picks its shard with [`index_shard_number`] and its place in that shard,
/// so a key is hashed once. A shard grows on its own, so a growth moves that shard's keys only.
/// Each claim of a key adds one copy of a session id, and each release removes one copy, so a
/// key stays in the index while any claim of it is left.
///
/// # Examples
///
/// ```ignore
/// let mut route_index = ShardedRouteIndex::default();
/// let shard_may_grow = route_index.claim_route_key(RouteKey::Slots, session_id);
/// assert_eq!(route_index.claiming_session_ids(&RouteKey::Slots), [session_id]);
/// route_index.release_route_key(&RouteKey::Slots, session_id);
/// assert!(route_index.claiming_session_ids(&RouteKey::Slots).is_empty());
/// ```
struct ShardedRouteIndex {
    /// Hashes each route key once, for both its shard and its place in the shard.
    route_key_hasher: foldhash::fast::RandomState,
    /// The shards, by shard number.
    index_shards: Box<[HashTable<IndexedRouteKey>]>,
    /// The route keys the shards hold, so [`ShardedRouteIndex::is_empty`] reads one field.
    held_route_key_count: usize,
}

impl Default for ShardedRouteIndex {
    /// Creates an empty index, whose shards allocate nothing until they get a key.
    ///
    /// # Returns
    ///
    /// The index, with [`ROUTE_INDEX_SHARDS`] empty shards and a new random hasher.
    fn default() -> Self {
        let index_shards = std::iter::repeat_with(HashTable::new)
            .take(ROUTE_INDEX_SHARDS)
            .collect();
        Self {
            route_key_hasher: Default::default(),
            index_shards,
            held_route_key_count: 0,
        }
    }
}

impl ShardedRouteIndex {
    /// Adds one claim of `route_key` for `session_id`, and indexes the key if no route claims it
    /// yet.
    ///
    /// A key the index holds gets one more copy of the session id, and its shard never grows
    /// for it. A new key goes into its shard, which can grow then.
    ///
    /// # Arguments
    ///
    /// * `route_key` - The route key to claim.
    /// * `session_id` - The session id of the route that claims it.
    ///
    /// # Returns
    ///
    /// `true` when the key was new and its shard had no room left, so the insert may have grown
    /// or rehashed that shard, and `false` otherwise.
    fn claim_route_key(&mut self, route_key: RouteKey, session_id: usize) -> bool {
        let route_key_hash = self.route_key_hasher.hash_one(route_key);
        let index_shard = &mut self.index_shards[index_shard_number(route_key_hash)];
        let held_route_key = index_shard.find_mut(route_key_hash, |indexed_route_key| {
            indexed_route_key.route_key == route_key
        });
        if let Some(indexed_route_key) = held_route_key {
            indexed_route_key.claiming_session_ids.push(session_id);
            return false;
        }
        // len() == capacity() means no growth is left, the only state in which an insert can
        // resize or rehash the shard.
        let shard_may_grow = index_shard.len() == index_shard.capacity();
        let new_route_key = IndexedRouteKey {
            route_key,
            claiming_session_ids: smallvec![session_id],
        };
        let route_key_hasher = &self.route_key_hasher;
        index_shard.insert_unique(route_key_hash, new_route_key, |indexed_route_key| {
            route_key_hasher.hash_one(indexed_route_key.route_key)
        });
        self.held_route_key_count += 1;
        shard_may_grow
    }

    /// Removes one claim of `route_key` for `session_id`, and the key once no claim of it is
    /// left.
    ///
    /// # Arguments
    ///
    /// * `route_key` - The route key to release.
    /// * `session_id` - The session id whose claim to remove.
    ///
    /// # Panics
    ///
    /// In a debug build, panics if `session_id` holds no claim of `route_key`. A release build
    /// then leaves the index as it is.
    fn release_route_key(&mut self, route_key: &RouteKey, session_id: usize) {
        let route_key_hash = self.route_key_hasher.hash_one(route_key);
        let index_shard = &mut self.index_shards[index_shard_number(route_key_hash)];
        let held_entry = index_shard.find_entry(route_key_hash, |indexed_route_key| {
            indexed_route_key.route_key == *route_key
        });
        debug_assert!(held_entry.is_ok(), "no claim of {route_key:?} to release");
        let Ok(mut indexed_entry) = held_entry else {
            return;
        };
        let claiming_session_ids = &mut indexed_entry.get_mut().claiming_session_ids;
        let claim_position = claiming_session_ids
            .iter()
            .position(|claiming_session_id| *claiming_session_id == session_id);
        debug_assert!(
            claim_position.is_some(),
            "no claim of {route_key:?} for session id {session_id} to release"
        );
        let Some(claim_position) = claim_position else {
            return;
        };
        claiming_session_ids.swap_remove(claim_position);
        if claiming_session_ids.is_empty() {
            indexed_entry.remove();
            self.held_route_key_count -= 1;
        }
    }

    /// The session ids that claim `route_key`, one copy per claim.
    ///
    /// # Arguments
    ///
    /// * `route_key` - The route key to look up.
    ///
    /// # Returns
    ///
    /// The claiming session ids, in no order, or an empty slice when no route claims the key.
    fn claiming_session_ids(&self, route_key: &RouteKey) -> &[usize] {
        let route_key_hash = self.route_key_hasher.hash_one(route_key);
        self.index_shards[index_shard_number(route_key_hash)]
            .find(route_key_hash, |indexed_route_key| {
                indexed_route_key.route_key == *route_key
            })
            .map(|indexed_route_key| indexed_route_key.claiming_session_ids.as_slice())
            .unwrap_or_default()
    }

    /// Whether no route claims any route key.
    ///
    /// # Returns
    ///
    /// `true` when the index holds no route key, and `false` otherwise.
    const fn is_empty(&self) -> bool {
        self.held_route_key_count == 0
    }

    /// Counts the route keys in every shard.
    ///
    /// # Returns
    ///
    /// The number of route keys with at least one claim.
    #[cfg(test)]
    fn indexed_key_count(&self) -> usize {
        self.index_shards.iter().map(HashTable::len).sum()
    }
}

/// The shard of a route key: bits 32 to 43 of its hash.
///
/// hashbrown takes a key's bucket position from the low bits of its hash and its control byte
/// from the top 7 bits, so these bits are independent of both in a shard below 4G buckets.
///
/// # Arguments
///
/// * `route_key_hash` - The route key's hash, from the index's hasher.
///
/// # Returns
///
/// The shard number, below [`ROUTE_INDEX_SHARDS`].
const fn index_shard_number(route_key_hash: u64) -> usize {
    (route_key_hash >> 32) as usize & (ROUTE_INDEX_SHARDS - 1)
}

/// A route whose keys the owner claims in index steps before it can go live.
struct StagedRoute {
    /// The route the client prepared, with its filter and key list.
    prepared_route: Route,
    /// How many of the route's keys, from the first, the session has claimed for it.
    claimed_key_count: usize,
    /// Answers the client once every key is claimed; [`None`] once answered.
    ready_tx: Option<oneshot::Sender<()>>,
    /// The sequence number of this staging, which its entry in `staging_order` carries.
    staging_sequence: u64,
}

/// Claims a departed route still holds, released oldest first in index steps.
struct ClaimsToRelease {
    /// The session id whose claims these are.
    session_id: usize,
    /// The departed route's key list.
    route_keys: Arc<[RouteKey]>,
    /// How many of the keys, from the first, are released already.
    released_key_count: usize,
    /// The end of the keys to release: the claims are those of `route_keys[..claimed_key_count]`.
    claimed_key_count: usize,
}

/// What one index step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexStep {
    /// No claim or release was left to do.
    NoWorkLeft,
    /// The step claimed or released route keys, and answered no preparation.
    Advanced,
    /// The step claimed the last key of a staged route and answered its preparation.
    AnsweredPreparation,
}

/// One commitment level's routes at an owner, indexed by route key.
///
/// # Examples
///
/// ```ignore
/// let mut level_router = LevelRouter::default();
/// level_router.claim_route_keys(&new_route, 0);
/// level_router.make_route_live(new_route, None);
/// level_router.route_batch(&message_batch);
/// level_router.depart_route(session_id);
/// while level_router.release_queued_claims(ROUTE_KEYS_PER_INDEX_STEP.get()) {}
/// ```
#[derive(Default)]
struct LevelRouter {
    /// The inverted index: the session ids of the subscribers that claim each route key.
    route_index: ShardedRouteIndex,
    /// Each live route by session id, with the stamp of the last message routed to it.
    routes_by_session_id: HashMap<usize, (Route, u64)>,
    /// Counts the messages routed so far, so each one gets its own stamp.
    message_stamp: u64,
    /// The buffer [`LevelRouter::route_batch`] collects each message's candidate subscribers in. It
    /// keeps its capacity between batches, so collecting candidate subscribers does not allocate
    /// once it has grown.
    scratch_subscribers: Vec<usize>,
    /// The claims of departed routes that wait for release, oldest first. It keeps its capacity.
    claims_to_release: VecDeque<ClaimsToRelease>,
}

impl LevelRouter {
    /// Claims `route_keys[first_unclaimed_key..]` of `claimed_route` for its session id, all at
    /// once.
    ///
    /// # Arguments
    ///
    /// * `claimed_route` - The route whose keys to claim.
    /// * `first_unclaimed_key` - The position of the first key the session has not claimed for
    ///   this route.
    ///
    /// # Panics
    ///
    /// Panics if `first_unclaimed_key` is past the end of the route's key list.
    fn claim_route_keys(&mut self, claimed_route: &Route, first_unclaimed_key: usize) {
        for route_key in &claimed_route.route_keys[first_unclaimed_key..] {
            self.route_index
                .claim_route_key(*route_key, claimed_route.session_id);
        }
    }

    /// Releases the claims of `route_keys[..claimed_key_count]` for `session_id`: the per-kind
    /// keys at once, and the others in later index steps, through `claims_to_release`.
    ///
    /// A key list is sorted, so its claimed per-kind keys are the end of the claimed part. They
    /// are few, and most messages carry one, so their stale claims would cost the most lookups.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The session id whose claims to release.
    /// * `route_keys` - The departed route's key list.
    /// * `claimed_key_count` - How many of the keys, from the first, the session claimed.
    ///
    /// # Panics
    ///
    /// Panics if `claimed_key_count` is past the end of `route_keys`.
    fn release_claims(
        &mut self,
        session_id: usize,
        route_keys: &Arc<[RouteKey]>,
        claimed_key_count: usize,
    ) {
        let claimed_route_keys = &route_keys[..claimed_key_count];
        let per_kind_start =
            claimed_route_keys.partition_point(|route_key| *route_key < RouteKey::Slots);
        for route_key in &claimed_route_keys[per_kind_start..] {
            self.route_index.release_route_key(route_key, session_id);
        }
        if per_kind_start > 0 {
            self.claims_to_release.push_back(ClaimsToRelease {
                session_id,
                route_keys: Arc::clone(route_keys),
                released_key_count: 0,
                claimed_key_count: per_kind_start,
            });
        }
    }

    /// Removes the live route with this session id, if the router holds one, and releases its
    /// claims with [`LevelRouter::release_claims`].
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
    fn depart_route(&mut self, session_id: usize) {
        let Some((departed_route, _)) = self.routes_by_session_id.remove(&session_id) else {
            return;
        };
        let route_keys = &departed_route.route_keys;
        self.release_claims(session_id, route_keys, route_keys.len());
    }

    /// Delivers what `broadcast_rx` holds through `live_route`, then makes the route live in
    /// place of the session's live route, whose claims it releases.
    ///
    /// Every key of `live_route` must hold a claim for its session id already. If the delivery
    /// fails, it disconnects the client and releases the route's claims instead.
    ///
    /// # Arguments
    ///
    /// * `live_route` - The route to make live.
    /// * `broadcast_rx` - The receiver the client read to the end, or [`None`].
    fn make_route_live(&mut self, live_route: Route, broadcast_rx: Option<BroadcastReceiver>) {
        let session_id = live_route.session_id;
        if let Err(route_failure) = drain_into_route(broadcast_rx, &live_route) {
            live_route.disconnect_client(route_failure);
            let route_keys = &live_route.route_keys;
            self.release_claims(session_id, route_keys, route_keys.len());
            return;
        }
        let replaced_route = self
            .routes_by_session_id
            .insert(session_id, (live_route, 0));
        if let Some((replaced_route, _)) = replaced_route {
            let route_keys = &replaced_route.route_keys;
            self.release_claims(session_id, route_keys, route_keys.len());
        }
    }

    /// Releases up to `keys_per_step` claims of the oldest departed route that holds any, and
    /// drops its entry once all are released.
    ///
    /// # Arguments
    ///
    /// * `keys_per_step` - The most claims to release.
    ///
    /// # Returns
    ///
    /// `true` if any claim waited for release, and `false` otherwise.
    fn release_queued_claims(&mut self, keys_per_step: usize) -> bool {
        let Some(claims_to_release) = self.claims_to_release.front_mut() else {
            return false;
        };
        let released_key_count = claims_to_release.released_key_count;
        let release_end =
            (released_key_count + keys_per_step).min(claims_to_release.claimed_key_count);
        for route_key in &claims_to_release.route_keys[released_key_count..release_end] {
            self.route_index
                .release_route_key(route_key, claims_to_release.session_id);
        }
        claims_to_release.released_key_count = release_end;
        if release_end == claims_to_release.claimed_key_count {
            self.claims_to_release.pop_front();
        }
        true
    }

    /// Delivers each message of `message_batch` to its candidate subscribers, once per subscriber
    /// however many of its route keys the message carries.
    ///
    /// When a delivery fails, it disconnects that client with [`Route::disconnect_client`] and
    /// departs its route with [`LevelRouter::depart_route`], so only the per-kind keys are
    /// released in the batch. It looks up no message when the router holds no live route, or
    /// when no route claims a route key, as when every route's filter asks for nothing.
    ///
    /// # Arguments
    ///
    /// * `message_batch` - The messages to route, in order.
    fn route_batch(&mut self, message_batch: &[Message]) {
        if self.routes_by_session_id.is_empty() || self.route_index.is_empty() {
            return;
        }
        let mut candidate_subscribers = std::mem::take(&mut self.scratch_subscribers);
        for routed_message in message_batch {
            self.message_stamp += 1;
            self.collect_candidate_subscribers(routed_message, &mut candidate_subscribers);
            for session_id in candidate_subscribers.drain(..) {
                let Some((candidate_route, last_routed_stamp)) =
                    self.routes_by_session_id.get_mut(&session_id)
                else {
                    continue;
                };
                if *last_routed_stamp == self.message_stamp {
                    continue;
                }
                *last_routed_stamp = self.message_stamp;
                if let Err(route_failure) = candidate_route.queue_updates(routed_message) {
                    candidate_route.disconnect_client(route_failure);
                    self.depart_route(session_id);
                }
            }
        }
        self.scratch_subscribers = candidate_subscribers;
    }

    /// Collects the candidate subscribers: the session ids that claim the route keys
    /// `routed_message` carries.
    ///
    /// An account carries its pubkey, its owner program, and the 32 bytes of its data at each of
    /// [`INDEXED_MEMCMP_OFFSETS`] that its data is long enough for. A transaction carries each of
    /// its account keys. Any other kind carries its kind's route key, except a [`Message::Block`]
    /// or a deshred message, which carry none.
    ///
    /// # Arguments
    ///
    /// * `routed_message` - The message to find the candidate subscribers of.
    /// * `candidate_subscribers` - Gets the session ids, with repeats when the message carries
    ///   several route keys of one subscriber.
    fn collect_candidate_subscribers(
        &self,
        routed_message: &Message,
        candidate_subscribers: &mut Vec<usize>,
    ) {
        let mut add_indexed_subscribers = |route_key| {
            let claiming_session_ids = self.route_index.claiming_session_ids(&route_key);
            candidate_subscribers.extend_from_slice(claiming_session_ids);
        };
        match routed_message {
            Message::Account(message_account) => {
                let account_info = &message_account.account;
                add_indexed_subscribers(RouteKey::Account(account_info.pubkey));
                add_indexed_subscribers(RouteKey::Owner(account_info.owner));
                for memcmp_offset in INDEXED_MEMCMP_OFFSETS {
                    let data_at_offset = account_info.data.get(memcmp_offset..);
                    if let Some(memcmp_bytes) = data_at_offset.and_then(<[u8]>::first_chunk) {
                        add_indexed_subscribers(RouteKey::Memcmp(memcmp_offset, *memcmp_bytes));
                    }
                }
            }
            Message::Transaction(message_transaction) => {
                for account_key in &message_transaction.transaction.account_keys {
                    add_indexed_subscribers(RouteKey::Transaction(*account_key));
                }
            }
            Message::Slot(_) => add_indexed_subscribers(RouteKey::Slots),
            Message::Entry(_) => add_indexed_subscribers(RouteKey::Entries),
            Message::EntryUpdateParent(_) => add_indexed_subscribers(RouteKey::UpdateParent),
            Message::BlockMeta(_) => add_indexed_subscribers(RouteKey::BlocksMeta),
            Message::BlockFooter(_) => add_indexed_subscribers(RouteKey::BlockFooter),
            Message::Block(_)
            | Message::DeshredTransaction(_)
            | Message::DeshredUpdateParent(_) => {}
        }
    }
}

/// An owner's routes at each commitment level, the broadcast channels it broadcasts on, and its
/// index work.
///
/// The owner sends every batch and takes in every route change through it, so each change lands
/// between two sends. A change claims or releases at once only the keys of a join that the owner
/// did not prepare, and the per-kind keys of a route that departs. Staged routes claim their
/// other keys, and departed routes release theirs, in index slices: the owner runs one with
/// [`OwnerRoutes::run_index_slice`] at an idle point, at most once per slice interval, or at the
/// top of a busy pass once [`OwnerRoutes::index_slice_is_overdue`] holds.
///
/// # Examples
///
/// ```ignore
/// let mut owner_routes = OwnerRoutes::new(broadcast_channels);
/// owner_routes.take_route_change(RouteChange::Prepare(new_route.clone(), ready_tx));
/// while owner_routes.has_index_work() {
///     owner_routes.run_index_slice();
/// }
/// // The client got the answer on `ready_rx` and read `broadcast_rx` to the end.
/// owner_routes.take_route_change(RouteChange::Join(new_route, Some(broadcast_rx)));
/// owner_routes.send_batch(CommitmentLevel::Processed, message_batch);
/// let client_leave = RouteLeave {
///     session_id,
///     receiver_request: None,
/// };
/// owner_routes.take_route_change(RouteChange::Leave(client_leave));
/// ```
pub struct OwnerRoutes {
    /// The broadcast channels, for the clients that are not routed.
    broadcast_channels: SubscriberChannels,
    /// The routes at each commitment level, indexed by the [`CommitmentLevel`] as `usize`.
    level_routers: [LevelRouter; 3],
    /// The staged route of each session that prepared one, until its join or its leave.
    staged_routes_by_session_id: HashMap<usize, StagedRoute>,
    /// The session id and staging sequence of each staging, oldest first. An entry whose staged
    /// route is gone, or has another sequence, is stale, and a claim step skips it.
    staging_order: VecDeque<(usize, u64)>,
    /// The staging sequence the last staged route got.
    last_staging_sequence: u64,
    /// How much index work one slice does, and how often slices run.
    route_index_limits: RouteIndexLimits,
    /// When the next idle index slice can run.
    next_index_slice_at: Instant,
    /// When waiting index work makes a slice run at the top of a pass.
    index_slice_overdue_at: Instant,
}

impl OwnerRoutes {
    /// Creates an owner's routes, with no route yet.
    ///
    /// # Arguments
    ///
    /// * `broadcast_channels` - The broadcast channels the owner broadcasts on, whose
    ///   [`SubscriberChannels::route_index_limits`] the owner's index work keeps to.
    ///
    /// # Returns
    ///
    /// A new `OwnerRoutes` that broadcasts every batch, and routes none until a client joins.
    pub fn new(broadcast_channels: SubscriberChannels) -> Self {
        let route_index_limits = broadcast_channels.route_index_limits();
        let created_at = Instant::now();
        Self {
            broadcast_channels,
            level_routers: Default::default(),
            staged_routes_by_session_id: HashMap::default(),
            staging_order: VecDeque::new(),
            last_staging_sequence: 0,
            route_index_limits,
            next_index_slice_at: created_at,
            index_slice_overdue_at: created_at,
        }
    }

    /// Routes `message_batch` to the routed clients at `commitment_level`, then broadcasts it on
    /// that level's channel.
    ///
    /// When a failed delivery departs a route and leaves index work where none waited, that work
    /// is overdue ten slice intervals from now, as the work of a route change is.
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
        self.level_routers[commitment_level as usize].route_batch(&message_batch);
        self.start_overdue_wait(had_index_work);
        self.broadcast_channels
            .send(commitment_level, message_batch);
    }

    /// Takes in a client's route change, between two sends.
    ///
    /// - A [`RouteChange::Prepare`] stages the route, in place of the session's staged route.
    ///   Index steps claim its keys later.
    /// - A [`RouteChange::Join`] makes the route live, as its docs say. If an owner disconnected
    ///   the client already, it only drops the session's staged route. If the delivery of what the
    ///   receiver holds fails, or the receiver lagged, it disconnects the client instead.
    /// - A [`RouteChange::Leave`] drops the session's staged route and removes its live route at
    ///   every level, and releases their per-kind keys at once. Index steps release their other
    ///   keys later. If the client asks for a receiver, the owner subscribes one right then and
    ///   answers with it.
    ///
    /// When the change leaves index work where none waited, that work is overdue ten slice
    /// intervals from now.
    ///
    /// # Arguments
    ///
    /// * `route_change` - The route change to take in.
    pub fn take_route_change(&mut self, route_change: RouteChange) {
        let had_index_work = self.has_index_work();
        match route_change {
            RouteChange::Prepare(prepared_route, ready_tx) => {
                self.stage_route(prepared_route, ready_tx);
            }
            RouteChange::Join(joined_route, broadcast_rx) => {
                self.join_route(joined_route, broadcast_rx);
            }
            RouteChange::Leave(client_leave) => {
                self.unstage_route(client_leave.session_id);
                for level_router in &mut self.level_routers {
                    level_router.depart_route(client_leave.session_id);
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
                self.route_index_limits.slice_interval * INDEX_SLICE_OVERDUE_INTERVALS;
            self.index_slice_overdue_at = Instant::now() + overdue_wait;
        }
    }

    /// Takes in the route changes queued in `route_changes_rx`, oldest first, at most
    /// [`MAX_ROUTE_CHANGES_PER_PASS`] of them.
    ///
    /// With an empty channel it reads the channel's length and nothing else. It never waits, and
    /// spends no tokio budget.
    ///
    /// # Arguments
    ///
    /// * `route_changes_rx` - The owner's route change receiver.
    pub fn take_queued_route_changes(
        &mut self,
        route_changes_rx: &mut mpsc::UnboundedReceiver<RouteChange>,
    ) {
        let queued_change_count = route_changes_rx.len().min(MAX_ROUTE_CHANGES_PER_PASS);
        for _ in 0..queued_change_count {
            let Ok(route_change) = route_changes_rx.try_recv() else {
                break;
            };
            self.take_route_change(route_change);
        }
    }

    /// Whether index work waits: keys to claim for a staged route, or claims to release.
    ///
    /// # Returns
    ///
    /// `true` while index work waits, and `false` otherwise.
    pub fn has_index_work(&self) -> bool {
        !self.staging_order.is_empty()
            || self
                .level_routers
                .iter()
                .any(|level_router| !level_router.claims_to_release.is_empty())
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
    /// preparations, or its [`RouteIndexLimits::slice_budget`] is spent. While any claim waits
    /// for release, a step releases claims, so releases run before claims. A step claims or
    /// releases at most [`RouteIndexLimits::keys_per_step`] keys, and a claim step ends right
    /// after a claim that may grow a shard of the index. The next idle slice can run one
    /// [`RouteIndexLimits::slice_interval`] after this one ends, and waiting work is overdue ten
    /// intervals after it ends.
    pub fn run_index_slice(&mut self) {
        let slice_deadline = Instant::now() + self.route_index_limits.slice_budget;
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
        let slice_interval = self.route_index_limits.slice_interval;
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

    /// Takes in `route_change`, then runs every index step that waits, in one call.
    ///
    /// It is the synchronous form of a route change, for the tests. The owners never run index
    /// work whole.
    ///
    /// # Arguments
    ///
    /// * `route_change` - The route change to take in.
    #[cfg(test)]
    pub fn apply_route_change(&mut self, route_change: RouteChange) {
        self.take_route_change(route_change);
        while self.advance_index_step() != IndexStep::NoWorkLeft {}
    }

    /// Runs one index step: a release step if any level router has claims to release, else a
    /// claim step for the oldest staging.
    ///
    /// # Returns
    ///
    /// What the step did, [`IndexStep::NoWorkLeft`] when no index work waited.
    fn advance_index_step(&mut self) -> IndexStep {
        let keys_per_step = self.route_index_limits.keys_per_step.get();
        for level_router in &mut self.level_routers {
            if level_router.release_queued_claims(keys_per_step) {
                return IndexStep::Advanced;
            }
        }
        self.claim_staged_route_keys(keys_per_step)
    }

    /// Claims up to `keys_per_step` keys of the oldest staged route, and answers its preparation
    /// once every key is claimed.
    ///
    /// It first drops the front entries of `staging_order` whose staged route is gone or has
    /// another staging sequence. The step ends right after a claim that may grow a shard. If the
    /// answer finds that the client dropped its receiver, the client is gone, and the step
    /// unstages its route.
    ///
    /// # Arguments
    ///
    /// * `keys_per_step` - The most keys to claim.
    ///
    /// # Returns
    ///
    /// [`IndexStep::AnsweredPreparation`] when the step answered a preparation,
    /// [`IndexStep::Advanced`] when it claimed keys and answered none, and
    /// [`IndexStep::NoWorkLeft`] when no staged route was claiming keys.
    fn claim_staged_route_keys(&mut self, keys_per_step: usize) -> IndexStep {
        while let Some(&(session_id, staging_sequence)) = self.staging_order.front() {
            let staged_route = self
                .staged_routes_by_session_id
                .get_mut(&session_id)
                .filter(|staged_route| staged_route.staging_sequence == staging_sequence);
            let Some(staged_route) = staged_route else {
                self.staging_order.pop_front();
                continue;
            };
            let staged_level = staged_route.prepared_route.commitment_level() as usize;
            let route_index = &mut self.level_routers[staged_level].route_index;
            let route_keys = &staged_route.prepared_route.route_keys;
            let claim_end = (staged_route.claimed_key_count + keys_per_step).min(route_keys.len());
            while staged_route.claimed_key_count < claim_end {
                let route_key = route_keys[staged_route.claimed_key_count];
                staged_route.claimed_key_count += 1;
                // A shard growth moves that shard's keys, so the step ends with it.
                if route_index.claim_route_key(route_key, session_id) {
                    break;
                }
            }
            if staged_route.claimed_key_count < route_keys.len() {
                return IndexStep::Advanced;
            }
            self.staging_order.pop_front();
            let ready_tx = staged_route.ready_tx.take();
            if ready_tx.is_some_and(|ready_tx| ready_tx.send(()).is_ok()) {
                return IndexStep::AnsweredPreparation;
            }
            self.unstage_route(session_id);
            return IndexStep::Advanced;
        }
        IndexStep::NoWorkLeft
    }

    /// Stages `prepared_route` with no key claimed yet, in place of the session's staged route.
    ///
    /// # Arguments
    ///
    /// * `prepared_route` - The route the client prepared.
    /// * `ready_tx` - Answers the client once every key of the route is claimed.
    fn stage_route(&mut self, prepared_route: Route, ready_tx: oneshot::Sender<()>) {
        let session_id = prepared_route.session_id;
        self.unstage_route(session_id);
        self.last_staging_sequence += 1;
        let staging_sequence = self.last_staging_sequence;
        self.staging_order.push_back((session_id, staging_sequence));
        let staged_route = StagedRoute {
            prepared_route,
            claimed_key_count: 0,
            ready_tx: Some(ready_tx),
            staging_sequence,
        };
        self.staged_routes_by_session_id
            .insert(session_id, staged_route);
    }

    /// Drops the session's staged route, if it has one, and releases the claims the route made.
    ///
    /// The route's answer sender and its queue sender drop with it, and its entry in
    /// `staging_order` goes stale.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
    fn unstage_route(&mut self, session_id: usize) {
        let Some(unstaged_route) = self.staged_routes_by_session_id.remove(&session_id) else {
            return;
        };
        let staged_level = unstaged_route.prepared_route.commitment_level() as usize;
        let route_keys = &unstaged_route.prepared_route.route_keys;
        self.level_routers[staged_level].release_claims(
            session_id,
            route_keys,
            unstaged_route.claimed_key_count,
        );
    }

    /// Makes `joined_route` live at its filter's level, as [`RouteChange::Join`] says.
    ///
    /// Without a receiver, a live route of the session with the same key list [`Arc`] only gets
    /// the new filter, and keeps its stamp. Otherwise the session's staged route, if any, gives
    /// its claims to `joined_route` when it has the same level and key list [`Arc`], and releases
    /// them when not. Then the owner claims the keys not claimed yet, at once, and makes the route
    /// live with [`LevelRouter::make_route_live`].
    ///
    /// # Arguments
    ///
    /// * `joined_route` - The route to make live.
    /// * `broadcast_rx` - The receiver the client read to the end, or [`None`].
    fn join_route(&mut self, joined_route: Route, broadcast_rx: Option<BroadcastReceiver>) {
        let session_id = joined_route.session_id;
        // An owner disconnected this client, and its queue is closed: the route stays out.
        if joined_route.disconnect_reason().is_some() {
            self.unstage_route(session_id);
            return;
        }
        let joined_level = joined_route.commitment_level() as usize;
        if broadcast_rx.is_none() {
            let live_routes = &mut self.level_routers[joined_level].routes_by_session_id;
            if let Some((live_route, _)) = live_routes.get_mut(&session_id) {
                if Arc::ptr_eq(&live_route.route_keys, &joined_route.route_keys) {
                    *live_route = joined_route;
                    return;
                }
            }
        }
        let mut claimed_key_count = 0;
        if let Some(staged_route) = self.staged_routes_by_session_id.remove(&session_id) {
            let staged_level = staged_route.prepared_route.commitment_level() as usize;
            let staged_keys = &staged_route.prepared_route.route_keys;
            if staged_level == joined_level && Arc::ptr_eq(staged_keys, &joined_route.route_keys) {
                claimed_key_count = staged_route.claimed_key_count;
            } else {
                self.level_routers[staged_level].release_claims(
                    session_id,
                    staged_keys,
                    staged_route.claimed_key_count,
                );
            }
        }
        let level_router = &mut self.level_routers[joined_level];
        level_router.claim_route_keys(&joined_route, claimed_key_count);
        level_router.make_route_live(joined_route, broadcast_rx);
    }
}

/// Delivers through `new_route` every batch `broadcast_rx` holds.
///
/// # Arguments
///
/// * `broadcast_rx` - The receiver the client handed over, or [`None`] for a join without one.
/// * `new_route` - The client's route.
///
/// # Returns
///
/// `Ok(())` once `broadcast_rx` reads empty or closed, or at once without a receiver.
///
/// # Errors
///
/// [`RouteFailure::Lagged`] if `broadcast_rx` lagged, and the error of [`Route::queue_updates`]
/// if a delivery fails.
fn drain_into_route(
    broadcast_rx: Option<BroadcastReceiver>,
    new_route: &Route,
) -> Result<(), RouteFailure> {
    let Some(mut broadcast_rx) = broadcast_rx else {
        return Ok(());
    };
    loop {
        match broadcast_rx.try_recv() {
            Ok(message_batch) => message_batch
                .iter()
                .try_for_each(|routed_message| new_route.queue_updates(routed_message))?,
            Err(TryRecvError::Empty | TryRecvError::Closed) => return Ok(()),
            Err(TryRecvError::Lagged(_)) => return Err(RouteFailure::Lagged),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use {
        super::*,
        crate::{
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

    /// Copies `route_template` with the filter that `subscribe_request` compiles to, and its
    /// route keys.
    ///
    /// # Arguments
    ///
    /// * `route_template` - The client's route, whose queue the copy shares.
    /// * `subscribe_request` - The request whose filter the copy routes.
    ///
    /// # Returns
    ///
    /// The copy, from [`Route::with_filter`].
    ///
    /// # Panics
    ///
    /// Panics if the filter of `subscribe_request` has no route keys.
    fn refilter_route(route_template: &Route, subscribe_request: &SubscribeRequest) -> Route {
        let client_filter = Arc::new(fixtures::filter_from_request(subscribe_request));
        let route_keys = client_filter.route_keys().expect("an indexable filter");
        route_template.with_filter(&client_filter, &route_keys.into())
    }

    /// A routed client: its route for `subscribe_request`, and its Subscribe queue.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
    /// * `subscribe_request` - The request whose filter the route routes.
    /// * `queue_capacity` - The capacity of the client's Subscribe queue.
    ///
    /// # Returns
    ///
    /// The client's route, and the receiving half of its Subscribe queue.
    ///
    /// # Panics
    ///
    /// Panics if the filter of `subscribe_request` has no route keys.
    fn new_client(
        session_id: usize,
        subscribe_request: &SubscribeRequest,
        queue_capacity: usize,
    ) -> (Route, SubscribeReceiver) {
        let (stream_tx, stream_rx) = load_aware_channel(queue_capacity, Default::default());
        let route_template = Route::new(session_id, "test", &stream_tx, &CancellationToken::new());
        let client_route = refilter_route(&route_template, subscribe_request);
        (client_route, stream_rx)
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

    /// What the filter of `client_route` makes of `sent_messages`, in order.
    ///
    /// # Arguments
    ///
    /// * `client_route` - The route whose filter to apply, at the filter's commitment level.
    /// * `sent_messages` - The messages sent, in order.
    ///
    /// # Returns
    ///
    /// The updates the client gets, each as an `Ok` item.
    fn expected_updates(client_route: &Route, sent_messages: &[Message]) -> QueueItems {
        let client_filter = &client_route.client_filter;
        let commitment_level = Some(client_route.commitment_level());
        sent_messages
            .iter()
            .flat_map(|sent_message| client_filter.get_updates(sent_message, commitment_level))
            .map(Ok)
            .collect()
    }

    /// Sends `message_batch` through `owner_routes` at processed.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - The owner's routes.
    /// * `message_batch` - The messages to send, as one batch.
    fn send_processed(owner_routes: &mut OwnerRoutes, message_batch: &[Message]) {
        owner_routes.send_batch(Processed, Arc::new(message_batch.to_vec()));
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
    /// A request whose filter routes under [`RouteKey::Entries`].
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
    /// have no route keys.
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

    /// A random message of a kind that a route key finds, about pubkeys from `pubkey_pool`.
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

    /// The per-kind route keys, in their sort order.
    const PER_KIND_ROUTE_KEYS: [RouteKey; 5] = [
        RouteKey::Slots,
        RouteKey::Entries,
        RouteKey::UpdateParent,
        RouteKey::BlocksMeta,
        RouteKey::BlockFooter,
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

    /// An account route key that no other number gives.
    ///
    /// # Arguments
    ///
    /// * `key_number` - The key's number.
    ///
    /// # Returns
    ///
    /// The [`RouteKey::Account`] key of [`numbered_pubkey`]`(key_number)`.
    fn numbered_route_key(key_number: u64) -> RouteKey {
        RouteKey::Account(numbered_pubkey(key_number))
    }

    /// Route keys of every kind. Each pubkey gives five keys with the same bytes, so the index must
    /// tell them apart by their kind.
    ///
    /// # Arguments
    ///
    /// * `pubkey_count` - How many pubkeys to make keys from.
    ///
    /// # Returns
    ///
    /// The account, owner, memcmp at both [`INDEXED_MEMCMP_OFFSETS`] and transaction key of each
    /// pubkey, then [`PER_KIND_ROUTE_KEYS`]: `5 * pubkey_count + 5` distinct keys.
    fn mixed_route_keys(pubkey_count: u64) -> Vec<RouteKey> {
        (0..pubkey_count)
            .map(numbered_pubkey)
            .flat_map(|shared_pubkey| {
                let [first_offset, second_offset] = INDEXED_MEMCMP_OFFSETS;
                [
                    RouteKey::Account(shared_pubkey),
                    RouteKey::Owner(shared_pubkey),
                    RouteKey::Memcmp(first_offset, shared_pubkey.to_bytes()),
                    RouteKey::Memcmp(second_offset, shared_pubkey.to_bytes()),
                    RouteKey::Transaction(shared_pubkey),
                ]
            })
            .chain(PER_KIND_ROUTE_KEYS)
            .collect()
    }

    /// New account route keys that fall in one shard of an index.
    ///
    /// # Arguments
    ///
    /// * `route_key_hasher` - A copy of the index's hasher.
    /// * `shard_number` - The shard the keys fall in.
    ///
    /// # Returns
    ///
    /// The [`numbered_route_key`] keys whose [`index_shard_number`] is `shard_number`, in number
    /// order, each once.
    fn route_keys_in_shard(
        route_key_hasher: foldhash::fast::RandomState,
        shard_number: usize,
    ) -> impl Iterator<Item = RouteKey> {
        (0..).map(numbered_route_key).filter(move |route_key| {
            index_shard_number(route_key_hasher.hash_one(route_key)) == shard_number
        })
    }

    /// How full one shard of `route_index` is.
    ///
    /// # Arguments
    ///
    /// * `route_index` - The index.
    /// * `shard_number` - The shard to look at.
    ///
    /// # Returns
    ///
    /// The shard's [`HashTable::len`] and [`HashTable::capacity`]. Its capacity is its length
    /// plus the growth it has left, so a release that leaves a tombstone lowers the capacity by
    /// one, and an insert that reuses one raises it by one.
    fn shard_len_and_capacity(
        route_index: &ShardedRouteIndex,
        shard_number: usize,
    ) -> (usize, usize) {
        let index_shard = &route_index.index_shards[shard_number];
        (index_shard.len(), index_shard.capacity())
    }

    /// A [`ShardedRouteIndex`] beside a model of its claims: a sorted multiset of session ids per
    /// route key.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut modelled_index = ModelledRouteIndex::default();
    /// modelled_index.claim_route_key(RouteKey::Slots, 1);
    /// modelled_index.release_held_claim(0);
    /// modelled_index.assert_matches_model(&[RouteKey::Slots]);
    /// ```
    #[derive(Default)]
    struct ModelledRouteIndex {
        /// The index under test.
        route_index: ShardedRouteIndex,
        /// The claiming session ids of each claimed route key, sorted, with one copy per claim.
        modelled_claims: HashMap<RouteKey, Vec<usize>>,
        /// Every claim not released yet, as a route key and a session id, in no order.
        held_claims: Vec<(RouteKey, usize)>,
    }

    impl ModelledRouteIndex {
        /// Claims `route_key` for `session_id` in the index and in the model, then checks the key.
        ///
        /// # Arguments
        ///
        /// * `route_key` - The route key to claim.
        /// * `session_id` - The session id that claims it.
        ///
        /// # Panics
        ///
        /// Panics if the index then holds other claims of `route_key` than the model.
        fn claim_route_key(&mut self, route_key: RouteKey, session_id: usize) {
            self.route_index.claim_route_key(route_key, session_id);
            let modelled_session_ids = self.modelled_claims.entry(route_key).or_default();
            let insert_position = modelled_session_ids
                .partition_point(|modelled_session_id| *modelled_session_id < session_id);
            modelled_session_ids.insert(insert_position, session_id);
            self.held_claims.push((route_key, session_id));
            self.assert_key_matches_model(&route_key);
        }

        /// Releases one held claim in the index and in the model, then checks its key.
        ///
        /// # Arguments
        ///
        /// * `held_claim_position` - The claim's position in `held_claims`.
        ///
        /// # Panics
        ///
        /// Panics if `held_claim_position` is out of range, or if the index then holds other
        /// claims of the released key than the model.
        fn release_held_claim(&mut self, held_claim_position: usize) {
            let (route_key, session_id) = self.held_claims.swap_remove(held_claim_position);
            self.route_index.release_route_key(&route_key, session_id);
            let modelled_session_ids = self.modelled_claims.get_mut(&route_key).unwrap();
            let claim_position = modelled_session_ids
                .iter()
                .position(|modelled_session_id| *modelled_session_id == session_id)
                .unwrap();
            modelled_session_ids.remove(claim_position);
            if modelled_session_ids.is_empty() {
                self.modelled_claims.remove(&route_key);
            }
            self.assert_key_matches_model(&route_key);
        }

        /// Asserts that the index holds the model's claims of `route_key`.
        ///
        /// # Arguments
        ///
        /// * `route_key` - The route key to check.
        ///
        /// # Panics
        ///
        /// Panics if the claims differ, as multisets.
        fn assert_key_matches_model(&self, route_key: &RouteKey) {
            let mut claiming_session_ids =
                self.route_index.claiming_session_ids(route_key).to_vec();
            claiming_session_ids.sort_unstable();
            let modelled_session_ids = self.modelled_claims.get(route_key);
            let modelled_session_ids = modelled_session_ids.map_or(&[][..], Vec::as_slice);
            assert_eq!(claiming_session_ids, modelled_session_ids, "{route_key:?}");
        }

        /// Asserts that the index holds the model's claims of each of `route_keys`, and as many
        /// keys as the model, both in its shards and in its count.
        ///
        /// # Arguments
        ///
        /// * `route_keys` - The route keys to check, every key claimed so far among them.
        ///
        /// # Panics
        ///
        /// Panics if any claims differ, or if the index holds or counts another number of keys.
        fn assert_matches_model(&self, route_keys: &[RouteKey]) {
            for route_key in route_keys {
                self.assert_key_matches_model(route_key);
            }
            let modelled_key_count = self.modelled_claims.len();
            assert_eq!(self.route_index.indexed_key_count(), modelled_key_count);
            assert_eq!(self.route_index.held_route_key_count, modelled_key_count);
            assert_eq!(self.route_index.is_empty(), modelled_key_count == 0);
        }
    }

    /// Index limits under which every slice is due at once and runs one index step of up to
    /// [`ROUTE_KEYS_PER_INDEX_STEP`] keys, so each owner pass runs one step.
    pub const STEP_EVERY_PASS_LIMITS: RouteIndexLimits = RouteIndexLimits {
        slice_interval: Duration::ZERO,
        slice_budget: Duration::ZERO,
        keys_per_step: ROUTE_KEYS_PER_INDEX_STEP,
    };

    /// Index limits under which every slice is due at once and runs one index step of one key.
    pub const SINGLE_KEY_STEP_LIMITS: RouteIndexLimits = RouteIndexLimits {
        slice_interval: Duration::ZERO,
        slice_budget: Duration::ZERO,
        keys_per_step: NonZeroUsize::MIN,
    };

    /// An owner's routes on broadcast channels of `broadcast_capacity` batches each.
    ///
    /// # Arguments
    ///
    /// * `broadcast_capacity` - The capacity of each broadcast channel.
    /// * `route_index_limits` - The owner's index limits.
    ///
    /// # Returns
    ///
    /// The owner's routes, with no route yet.
    fn owner_routes_with(
        broadcast_capacity: usize,
        route_index_limits: RouteIndexLimits,
    ) -> OwnerRoutes {
        let broadcast_channels =
            SubscriberChannels::new(broadcast_capacity, broadcast_capacity, broadcast_capacity);
        let mut owner_routes = OwnerRoutes::new(broadcast_channels);
        owner_routes.route_index_limits = route_index_limits;
        owner_routes
    }

    /// Takes in a [`RouteChange::Prepare`] of `prepared_route`, as a client sends it.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - The owner's routes.
    /// * `prepared_route` - The route to stage.
    ///
    /// # Returns
    ///
    /// The receiver the owner answers on once every key of the route is claimed.
    fn prepare_route(
        owner_routes: &mut OwnerRoutes,
        prepared_route: Route,
    ) -> oneshot::Receiver<()> {
        let (ready_tx, ready_rx) = oneshot::channel();
        owner_routes.take_route_change(RouteChange::Prepare(prepared_route, ready_tx));
        ready_rx
    }

    /// Takes in a [`RouteChange::Leave`] of `session_id` with no receiver request.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - The owner's routes.
    /// * `session_id` - The session id that leaves.
    fn leave_route(owner_routes: &mut OwnerRoutes, session_id: usize) {
        let client_leave = RouteLeave {
            session_id,
            receiver_request: None,
        };
        owner_routes.take_route_change(RouteChange::Leave(client_leave));
    }

    /// `route_keys` as one key list, sorted and without repeats, as [`Filter::route_keys`] gives
    /// them.
    ///
    /// # Arguments
    ///
    /// * `route_keys` - The route keys.
    ///
    /// # Returns
    ///
    /// The key list.
    fn sorted_key_list(route_keys: impl IntoIterator<Item = RouteKey>) -> Arc<[RouteKey]> {
        let mut sorted_route_keys = route_keys.into_iter().collect::<Vec<_>>();
        sorted_route_keys.sort_unstable();
        sorted_route_keys.dedup();
        sorted_route_keys.into()
    }

    /// A client whose route has `route_keys` and the default filter, which matches nothing.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The client's session id.
    /// * `route_keys` - The route's key list.
    ///
    /// # Returns
    ///
    /// The client's route, and the receiving half of its Subscribe queue.
    fn keyed_client(session_id: usize, route_keys: &Arc<[RouteKey]>) -> (Route, SubscribeReceiver) {
        let (stream_tx, stream_rx) = load_aware_channel(16, Default::default());
        let route_template = Route::new(session_id, "test", &stream_tx, &CancellationToken::new());
        (
            route_template.with_filter(&Arc::default(), route_keys),
            stream_rx,
        )
    }

    /// How many claims of `route_key` the index of `level_router` holds for `session_id`.
    ///
    /// # Arguments
    ///
    /// * `level_router` - The level's router.
    /// * `route_key` - The route key.
    /// * `session_id` - The session id.
    ///
    /// # Returns
    ///
    /// The number of copies of `session_id` among the key's claims.
    fn claim_count(level_router: &LevelRouter, route_key: &RouteKey, session_id: usize) -> usize {
        let claiming_session_ids = level_router.route_index.claiming_session_ids(route_key);
        claiming_session_ids
            .iter()
            .filter(|claiming_session_id| **claiming_session_id == session_id)
            .count()
    }

    /// How many keys the staged route of `session_id` has claimed.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - The owner's routes.
    /// * `session_id` - The session id of the staged route.
    ///
    /// # Returns
    ///
    /// The staged route's claimed key count.
    ///
    /// # Panics
    ///
    /// Panics if the session has no staged route.
    fn staged_key_count(owner_routes: &OwnerRoutes, session_id: usize) -> usize {
        owner_routes.staged_routes_by_session_id[&session_id].claimed_key_count
    }

    /// Runs index steps until no index work is left.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - The owner's routes.
    ///
    /// # Returns
    ///
    /// How many steps did work.
    fn run_every_index_step(owner_routes: &mut OwnerRoutes) -> usize {
        std::iter::from_fn(|| {
            (owner_routes.advance_index_step() != IndexStep::NoWorkLeft).then_some(())
        })
        .count()
    }

    /// Asserts that each level's index holds exactly the claims its routes made: one for each
    /// key of each live route, each claimed key of each staged route, and each key a departed
    /// route has not released yet. So every key of a live route holds a claim for its session.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - The owner's routes.
    ///
    /// # Panics
    ///
    /// Panics if a level's index holds other claims, or another number of keys.
    fn assert_claims_match_routes(owner_routes: &OwnerRoutes) {
        for (level_number, level_router) in owner_routes.level_routers.iter().enumerate() {
            let mut expected_claims = HashMap::<RouteKey, Vec<usize>>::default();
            let mut add_claims = |session_id: usize, claimed_route_keys: &[RouteKey]| {
                for route_key in claimed_route_keys {
                    expected_claims
                        .entry(*route_key)
                        .or_default()
                        .push(session_id);
                }
            };
            for (session_id, (live_route, _)) in &level_router.routes_by_session_id {
                add_claims(*session_id, &live_route.route_keys);
            }
            for (session_id, staged_route) in &owner_routes.staged_routes_by_session_id {
                if staged_route.prepared_route.commitment_level() as usize == level_number {
                    let claimed_route_keys =
                        &staged_route.prepared_route.route_keys[..staged_route.claimed_key_count];
                    add_claims(*session_id, claimed_route_keys);
                }
            }
            for claims_to_release in &level_router.claims_to_release {
                let unreleased_keys = &claims_to_release.route_keys
                    [claims_to_release.released_key_count..claims_to_release.claimed_key_count];
                add_claims(claims_to_release.session_id, unreleased_keys);
            }
            for (route_key, expected_session_ids) in &mut expected_claims {
                let route_index = &level_router.route_index;
                let mut claiming_session_ids = route_index.claiming_session_ids(route_key).to_vec();
                claiming_session_ids.sort_unstable();
                expected_session_ids.sort_unstable();
                assert_eq!(
                    claiming_session_ids, *expected_session_ids,
                    "level {level_number}, {route_key:?}"
                );
            }
            assert_eq!(
                level_router.route_index.held_route_key_count,
                expected_claims.len(),
                "level {level_number}"
            );
        }
    }

    /// Asserts that the owner holds no route, no staging and no claim at any level.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - The owner's routes.
    ///
    /// # Panics
    ///
    /// Panics if any route, staged route, staging entry, claim to release or route key is left.
    fn assert_owner_holds_nothing(owner_routes: &OwnerRoutes) {
        assert!(owner_routes.staged_routes_by_session_id.is_empty());
        assert!(owner_routes.staging_order.is_empty());
        assert!(!owner_routes.has_index_work());
        for level_router in &owner_routes.level_routers {
            assert!(level_router.routes_by_session_id.is_empty());
            assert!(level_router.claims_to_release.is_empty());
            assert!(level_router.route_index.is_empty());
            assert_eq!(level_router.route_index.indexed_key_count(), 0);
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
    fn each_routed_client_gets_what_its_filter_makes_of_every_message() {
        let pubkey_pool = fixtures::deterministic_pubkeys(1, 8);
        let mut routed_count = 0;
        for rng_seed in 0..60 {
            let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
            let mut owner_routes = OwnerRoutes::new(SubscriberChannels::new(1, 1, 1));
            let mut routed_clients = Vec::new();
            for session_id in 0..16 {
                let subscribe_request = random_request(&mut seeded_rng, &pubkey_pool);
                let client_filter = fixtures::filter_from_request(&subscribe_request);
                if client_filter.route_keys().is_some() {
                    let (client_route, stream_rx) =
                        new_client(session_id, &subscribe_request, MAX_CAPACITY);
                    owner_routes.apply_route_change(RouteChange::Join(client_route.clone(), None));
                    routed_clients.push((client_route, stream_rx));
                }
            }
            routed_count += routed_clients.len();
            let sent_messages = (0..256)
                .map(|_| random_message(&mut seeded_rng, &pubkey_pool))
                .collect::<Vec<_>>();
            for message_batch in sent_messages.chunks(32) {
                send_processed(&mut owner_routes, message_batch);
            }
            for (client_route, stream_rx) in &mut routed_clients {
                assert_eq!(
                    received_items(stream_rx),
                    expected_updates(client_route, &sent_messages),
                    "seed {rng_seed}"
                );
            }
        }
        assert!(routed_count > 500, "{routed_count}");
    }

    #[test]
    fn indexes_only_filters_whose_keys_find_every_match() {
        let [account_pubkey, owner_pubkey] = [Pubkey::new_unique(), Pubkey::new_unique()];
        let too_many_pubkeys = (0..=MAX_ROUTE_KEYS)
            .map(|_| Pubkey::new_unique())
            .collect::<Vec<_>>();
        let blocks_request = SubscribeRequest {
            blocks: [("b".to_owned(), Default::default())].into(),
            ..Default::default()
        };
        let route_key_cases = [
            (
                accounts_request(&[account_pubkey], &[owner_pubkey], None, &[]),
                Some(vec![RouteKey::Account(account_pubkey)]),
            ),
            (
                accounts_request(&[account_pubkey], &[owner_pubkey], None, &[account_pubkey]),
                Some(vec![RouteKey::Owner(owner_pubkey)]),
            ),
            (
                accounts_request(&[], &too_many_pubkeys, Some(32), &[]),
                Some(vec![RouteKey::Memcmp(32, MEMCMP_BYTES)]),
            ),
            (
                accounts_request(&[], &[owner_pubkey], Some(8), &[]),
                Some(vec![RouteKey::Owner(owner_pubkey)]),
            ),
            (
                transactions_request(&[account_pubkey], &too_many_pubkeys, None),
                Some(vec![RouteKey::Transaction(
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
        for (case_index, (subscribe_request, expected_route_keys)) in
            route_key_cases.into_iter().enumerate()
        {
            assert_eq!(
                fixtures::filter_from_request(&subscribe_request).route_keys(),
                expected_route_keys,
                "case {case_index}"
            );
        }
    }

    /// A preparation routes nothing. The hand-over that follows delivers what the receiver got
    /// since, and the leave answers with a receiver subscribed right after the last routed send.
    #[test]
    fn a_join_and_a_leave_land_between_two_sends() {
        let mut owner_routes = OwnerRoutes::new(SubscriberChannels::new(16, 16, 16));
        let (client_route, mut stream_rx) = new_client(1, &entries_request(), 16);
        let broadcast_rx = owner_routes.broadcast_channels.subscribe(Processed);
        let message_batches = [1, 2, 3, 4].map(|entry_slot| vec![entry_message(entry_slot)]);
        let (ready_tx, mut ready_rx) = oneshot::channel();
        owner_routes.apply_route_change(RouteChange::Prepare(client_route.clone(), ready_tx));
        assert_eq!(ready_rx.try_recv(), Ok(()));
        send_processed(&mut owner_routes, &message_batches[0]);
        send_processed(&mut owner_routes, &message_batches[1]);
        let route_change = RouteChange::Join(client_route.clone(), Some(broadcast_rx));
        owner_routes.apply_route_change(route_change);
        send_processed(&mut owner_routes, &message_batches[2]);
        let (answer_tx, mut answer_rx) = oneshot::channel();
        let client_leave = RouteLeave {
            session_id: 1,
            receiver_request: Some((Processed, answer_tx)),
        };
        owner_routes.apply_route_change(RouteChange::Leave(client_leave));
        send_processed(&mut owner_routes, &message_batches[3]);
        assert_eq!(
            received_items(&mut stream_rx),
            expected_updates(&client_route, &message_batches[..3].concat())
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
        let mut owner_routes = OwnerRoutes::new(SubscriberChannels::new(2, 2, 2));
        let (client_route, mut stream_rx) = new_client(1, &entries_request(), 16);
        let broadcast_rx = owner_routes.broadcast_channels.subscribe(Processed);
        for entry_slot in 0..3 {
            send_processed(&mut owner_routes, &[entry_message(entry_slot)]);
        }
        let route_change = RouteChange::Join(client_route.clone(), Some(broadcast_rx));
        owner_routes.apply_route_change(route_change);
        let processed_router = &owner_routes.level_routers[0];
        assert!(processed_router.routes_by_session_id.is_empty());
        assert!(processed_router.route_index.is_empty());
        assert!(processed_router.claims_to_release.is_empty());
        let lagged_error = Err("lagged to receive geyser messages".to_owned());
        assert_eq!(received_items(&mut stream_rx), [lagged_error]);
        assert_eq!(
            client_route.disconnect_reason(),
            Some("client_broadcast_lag")
        );
    }

    /// A join without a receiver replaces a held route's filter, and when the route keys change,
    /// the index then holds the new keys only.
    #[test]
    fn a_join_without_a_receiver_swaps_or_reindexes_the_route() {
        let mut owner_routes = OwnerRoutes::new(SubscriberChannels::new(16, 16, 16));
        let [first_pubkey, second_pubkey] = [Pubkey::new_unique(), Pubkey::new_unique()];
        let (client_route, mut stream_rx) =
            new_client(1, &accounts_request(&[first_pubkey], &[], Some(0), &[]), 16);
        owner_routes.apply_route_change(RouteChange::Join(client_route.clone(), None));
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
            let refiltered_route = refilter_route(&client_route, &subscribe_request);
            owner_routes.apply_route_change(RouteChange::Join(refiltered_route.clone(), None));
            send_processed(&mut owner_routes, &message_batch);
            assert_eq!(
                received_items(&mut stream_rx),
                expected_updates(&refiltered_route, &message_batch)
            );
            let processed_router = &owner_routes.level_routers[0];
            assert_eq!(
                processed_router.route_index.indexed_key_count(),
                indexed_key_count
            );
        }
    }

    /// A routed client whose queue fills gets what fits, then the full-queue status, and its
    /// route leaves with its per-kind claims released at once.
    #[test]
    fn a_full_queue_disconnects_the_client_behind_what_it_holds() {
        let mut owner_routes = OwnerRoutes::new(SubscriberChannels::new(16, 16, 16));
        let (client_route, mut stream_rx) = new_client(1, &entries_request(), 2);
        owner_routes.apply_route_change(RouteChange::Join(client_route.clone(), None));
        let message_batch = [1, 2, 3].map(entry_message);
        send_processed(&mut owner_routes, &message_batch);
        let mut expected_items = expected_updates(&client_route, &message_batch[..2]);
        expected_items.push(Err("lagged to send an update".to_owned()));
        assert_eq!(received_items(&mut stream_rx), expected_items);
        assert_eq!(
            client_route.disconnect_reason(),
            Some("client_channel_full")
        );
        assert!(client_route.cancellation_token.is_cancelled());
        let processed_router = &owner_routes.level_routers[0];
        assert!(processed_router.routes_by_session_id.is_empty());
        assert!(processed_router.route_index.is_empty());
        assert!(processed_router.claims_to_release.is_empty());
    }

    /// A warm router routes a batch without an allocator call. Each route is a candidate for one
    /// message of the batch, and its filter does not match that message.
    #[test]
    fn a_warm_router_routes_a_batch_without_allocating() {
        let one_box_allocator_calls = allocator_calls_during(|| drop(black_box(Box::new(1_u64))));
        assert_eq!(one_box_allocator_calls, 2);
        let mut owner_routes = OwnerRoutes::new(SubscriberChannels::new(16, 16, 16));
        let _processed_rx = owner_routes.broadcast_channels.subscribe(Processed);
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
            let (client_route, stream_rx) = new_client(session_id, near_miss_request, 16);
            owner_routes.apply_route_change(RouteChange::Join(client_route, None));
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
        owner_routes.send_batch(Processed, Arc::clone(&message_batch));
        let allocator_calls = allocator_calls_during(|| {
            for _ in 0..32 {
                owner_routes.send_batch(Processed, Arc::clone(&message_batch));
            }
        });
        assert_eq!(allocator_calls, 0);
        // Each request's routes are candidates for one message of the batch only.
        let candidate_message_indexes = [0, 1, 1, 2];
        let processed_router = &owner_routes.level_routers[0];
        let first_message_stamp = processed_router.message_stamp - 2;
        assert_eq!(processed_router.routes_by_session_id.len(), 64);
        for (session_id, (_, last_routed_stamp)) in &processed_router.routes_by_session_id {
            let candidate_message_index =
                candidate_message_indexes[session_id % near_miss_requests.len()];
            assert_eq!(
                *last_routed_stamp,
                first_message_stamp + candidate_message_index
            );
        }
        for stream_rx in &mut stream_rxs {
            assert!(received_items(stream_rx).is_empty());
        }
    }

    /// A warm router queues each matched update without an allocator call, while every client
    /// queue has room in the block its channel started with.
    #[test]
    fn a_warm_router_queues_matched_updates_without_allocating() {
        let mut owner_routes = OwnerRoutes::new(SubscriberChannels::new(16, 16, 16));
        let mut stream_rxs = Vec::new();
        for session_id in 0..16 {
            let (client_route, stream_rx) = new_client(session_id, &entries_request(), 64);
            owner_routes.apply_route_change(RouteChange::Join(client_route, None));
            stream_rxs.push(stream_rx);
        }
        let message_batch = Arc::new(vec![entry_message(1)]);
        owner_routes.send_batch(Processed, Arc::clone(&message_batch));
        let allocator_calls = allocator_calls_during(|| {
            for _ in 0..8 {
                owner_routes.send_batch(Processed, Arc::clone(&message_batch));
            }
        });
        assert_eq!(allocator_calls, 0);
        for stream_rx in &mut stream_rxs {
            assert_eq!(received_items(stream_rx).len(), 9);
        }
    }

    /// Seeded claims and releases leave the index holding what a multiset model holds: a release
    /// removes one copy of its session id, and a key leaves the index with its last claim.
    #[test]
    fn the_sharded_index_matches_a_model_through_random_claims_and_releases() {
        let mut route_index = ShardedRouteIndex::default();
        for claiming_session_id in [7, 7, 8] {
            route_index.claim_route_key(RouteKey::Slots, claiming_session_id);
        }
        for (released_session_id, claims_left) in [(7, &[7, 8][..]), (7, &[8]), (8, &[])] {
            route_index.release_route_key(&RouteKey::Slots, released_session_id);
            let mut claiming_session_ids =
                route_index.claiming_session_ids(&RouteKey::Slots).to_vec();
            claiming_session_ids.sort_unstable();
            assert_eq!(claiming_session_ids, claims_left);
        }
        assert_eq!(route_index.indexed_key_count(), 0);
        let route_key_pool = mixed_route_keys(512);
        let mut grown_shard_count = 0;
        for rng_seed in 0..64 {
            let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
            let mut modelled_index = ModelledRouteIndex::default();
            let mut shuffled_pool = route_key_pool.clone();
            seeded_rng.shuffle(&mut shuffled_pool);
            // Every key gets a claim, so shards grow, and a quarter get a second claim.
            for route_key in &shuffled_pool {
                modelled_index.claim_route_key(*route_key, seeded_rng.usize(..4));
                if seeded_rng.u8(..4) == 0 {
                    modelled_index.claim_route_key(*route_key, seeded_rng.usize(..4));
                }
            }
            modelled_index.assert_matches_model(&route_key_pool);
            let index_shards = modelled_index.route_index.index_shards.iter();
            grown_shard_count += index_shards
                .filter(|index_shard| index_shard.len() > 3)
                .count();
            // Half the claims go to a few hot keys, whose claim lists then spill to the heap.
            let hot_route_keys = &shuffled_pool[..8];
            for _ in 0..2048 {
                if seeded_rng.bool() {
                    let claim_pool = [hot_route_keys, &shuffled_pool][seeded_rng.usize(..2)];
                    let route_key = *seeded_rng.choice(claim_pool).unwrap();
                    modelled_index.claim_route_key(route_key, seeded_rng.usize(..4));
                } else {
                    let held_claim_count = modelled_index.held_claims.len();
                    modelled_index.release_held_claim(seeded_rng.usize(..held_claim_count));
                }
            }
            modelled_index.assert_matches_model(&route_key_pool);
            while !modelled_index.held_claims.is_empty() {
                let held_claim_count = modelled_index.held_claims.len();
                modelled_index.release_held_claim(seeded_rng.usize(..held_claim_count));
            }
            modelled_index.assert_matches_model(&route_key_pool);
            assert_eq!(modelled_index.route_index.indexed_key_count(), 0);
        }
        // A shard that holds more than 3 keys grew past its first allocation.
        assert!(grown_shard_count >= 64, "{grown_shard_count}");
    }

    /// A claim reports a possible growth exactly when a new key meets a full shard: never for a
    /// held key, and never in a shard with room, even when the insert reuses a tombstone.
    #[test]
    fn a_claim_reports_a_growth_only_when_a_new_key_meets_a_full_shard() {
        let mut route_index = ShardedRouteIndex::default();
        let shard_number = ROUTE_INDEX_SHARDS / 3;
        let mut new_shard_keys =
            route_keys_in_shard(route_index.route_key_hasher.clone(), shard_number);
        let mut held_shard_keys = Vec::new();
        // In a shard with no tombstone, a claim reports a growth exactly when it changes the
        // capacity.
        let mut claim_new_shard_key =
            |route_index: &mut ShardedRouteIndex, held_shard_keys: &mut Vec<RouteKey>| {
                let (len_before, capacity_before) =
                    shard_len_and_capacity(route_index, shard_number);
                let new_route_key = new_shard_keys.next().unwrap();
                let shard_may_grow = route_index.claim_route_key(new_route_key, 1);
                let (_, capacity_after) = shard_len_and_capacity(route_index, shard_number);
                assert_eq!(shard_may_grow, len_before == capacity_before);
                assert_eq!(shard_may_grow, capacity_after != capacity_before);
                held_shard_keys.push(new_route_key);
                shard_may_grow
            };
        let full_shard = loop {
            let (shard_len, shard_capacity) = shard_len_and_capacity(&route_index, shard_number);
            if shard_len == shard_capacity && shard_capacity >= 28 {
                break (shard_len, shard_capacity);
            }
            claim_new_shard_key(&mut route_index, &mut held_shard_keys);
        };
        // A full shard: a held key's claim reports no growth and keeps the capacity, a new one's
        // reports a growth.
        let held_route_key = held_shard_keys[0];
        assert!(!route_index.claim_route_key(held_route_key, 2));
        assert_eq!(
            shard_len_and_capacity(&route_index, shard_number),
            full_shard
        );
        route_index.release_route_key(&held_route_key, 2);
        assert!(claim_new_shard_key(&mut route_index, &mut held_shard_keys));
        // In a shard with room, a released key's claim reuses the tombstone its release left,
        // which raises the capacity by one: it must report no growth.
        let mut tombstone_reused = false;
        while !tombstone_reused {
            let (shard_len, shard_capacity) = shard_len_and_capacity(&route_index, shard_number);
            assert!(shard_capacity <= 4096, "no release left a tombstone");
            for _ in shard_len + 1..shard_capacity {
                claim_new_shard_key(&mut route_index, &mut held_shard_keys);
            }
            for released_route_key in held_shard_keys.clone() {
                let shard_before = shard_len_and_capacity(&route_index, shard_number);
                route_index.release_route_key(&released_route_key, 1);
                let (_, capacity_released) = shard_len_and_capacity(&route_index, shard_number);
                assert!(!route_index.claim_route_key(released_route_key, 1));
                let shard_after = shard_len_and_capacity(&route_index, shard_number);
                assert_eq!(shard_after, shard_before);
                tombstone_reused = capacity_released < shard_before.1;
                if tombstone_reused {
                    break;
                }
            }
            if !tombstone_reused {
                while !claim_new_shard_key(&mut route_index, &mut held_shard_keys) {}
            }
        }
    }

    /// Over 600,000 claims, each reported growth is of a shard that holds at most twice the mean
    /// shard length of 4,096 shards, and no shard ends with more, so a growth moves a small part
    /// of the index.
    #[test]
    fn a_shard_growth_moves_one_shard_only() {
        const CLAIMED_KEY_COUNT: u64 = 600_000;
        // smallvec's union feature keeps an entry as small as a route key and a Vec.
        let key_and_vec_size = std::mem::size_of::<(RouteKey, Vec<usize>)>();
        assert_eq!(std::mem::size_of::<IndexedRouteKey>(), key_and_vec_size);
        let mut route_index = ShardedRouteIndex::default();
        let route_key_hasher = route_index.route_key_hasher.clone();
        // A literal 4,096, so an index with fewer shards fails the limit.
        let grown_shard_len_limit = 2 * CLAIMED_KEY_COUNT as usize / 4096;
        let mut reported_growth_count = 0;
        for key_number in 0..CLAIMED_KEY_COUNT {
            let route_key = numbered_route_key(key_number);
            if route_index.claim_route_key(route_key, 1) {
                let shard_number = index_shard_number(route_key_hasher.hash_one(route_key));
                let grown_shard_len = route_index.index_shards[shard_number].len();
                assert!(
                    grown_shard_len <= grown_shard_len_limit,
                    "{grown_shard_len}"
                );
                reported_growth_count += 1;
            }
        }
        assert!(
            reported_growth_count > ROUTE_INDEX_SHARDS,
            "{reported_growth_count}"
        );
        let shard_lens = route_index.index_shards.iter().map(HashTable::len);
        let largest_shard_len = shard_lens.max().unwrap();
        assert!(
            largest_shard_len <= grown_shard_len_limit,
            "{largest_shard_len}"
        );
        assert_eq!(route_index.indexed_key_count(), CLAIMED_KEY_COUNT as usize);
    }

    /// The per-kind route keys sort after every account, owner, memcmp and transaction key, so they
    /// are the end of a filter's sorted key list.
    #[test]
    fn the_per_kind_route_keys_sort_after_every_pubkey_key() {
        let highest_pubkey = Pubkey::new_from_array([u8::MAX; 32]);
        let highest_pubkey_keys = [
            RouteKey::Account(highest_pubkey),
            RouteKey::Owner(highest_pubkey),
            RouteKey::Memcmp(usize::MAX, [u8::MAX; 32]),
            RouteKey::Transaction(highest_pubkey),
        ];
        for pubkey_route_key in highest_pubkey_keys {
            for per_kind_route_key in PER_KIND_ROUTE_KEYS {
                assert!(
                    pubkey_route_key < per_kind_route_key,
                    "{pubkey_route_key:?}"
                );
            }
        }
        assert!(PER_KIND_ROUTE_KEYS.is_sorted());
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
        let route_keys = fixtures::filter_from_request(&subscribe_request)
            .route_keys()
            .expect("an indexable filter");
        let per_kind_start = route_keys.partition_point(|route_key| *route_key < RouteKey::Slots);
        let pubkey_route_keys = [
            RouteKey::Account(highest_pubkey),
            RouteKey::Transaction(highest_pubkey),
        ];
        assert_eq!(route_keys[..per_kind_start], pubkey_route_keys);
        assert_eq!(route_keys[per_kind_start..], PER_KIND_ROUTE_KEYS);
    }

    /// The keys of one shard take many bucket positions and control bytes, and both values of
    /// every hash bit that a shard below 4G buckets places or matches keys by, so the bits that
    /// pick a shard leave its keys spread over the shard's table.
    #[test]
    fn the_keys_of_one_shard_spread_over_its_buckets_and_control_bytes() {
        let route_key_hasher = ShardedRouteIndex::default().route_key_hasher;
        let shard_key_hashes = route_keys_in_shard(route_key_hasher.clone(), 0)
            .take(64)
            .map(|route_key| route_key_hasher.hash_one(route_key))
            .collect::<Vec<_>>();
        // hashbrown places a key by the low bits of its hash and matches it by the top 7 bits.
        let bucket_positions = shard_key_hashes
            .iter()
            .map(|route_key_hash| route_key_hash % 64)
            .collect::<foldhash::HashSet<_>>();
        let control_bytes = shard_key_hashes
            .iter()
            .map(|route_key_hash| route_key_hash >> 57)
            .collect::<foldhash::HashSet<_>>();
        assert!(bucket_positions.len() >= 24, "{bucket_positions:?}");
        assert!(control_bytes.len() >= 24, "{control_bytes:?}");
        let bits_set_in_every_hash = shard_key_hashes
            .iter()
            .fold(u64::MAX, |common_bits, route_key_hash| {
                common_bits & route_key_hash
            });
        let bits_set_in_any_hash = shard_key_hashes
            .iter()
            .fold(0, |seen_bits, route_key_hash| seen_bits | route_key_hash);
        // Bits 0 to 31 place a key in a shard below 4G buckets, and bits 57 to 63 match it.
        let bucket_and_control_bits = u64::from(u32::MAX) | u64::MAX << 57;
        let fixed_bits = bits_set_in_every_hash | !bits_set_in_any_hash;
        let fixed_bucket_and_control_bits = fixed_bits & bucket_and_control_bits;
        assert_eq!(
            fixed_bucket_and_control_bits, 0,
            "{fixed_bucket_and_control_bits:#018x}"
        );
    }

    /// A router whose routes claim no route key, as when their filters ask for nothing, looks up
    /// no message of a batch, and looks messages up again once a route claims a key.
    #[test]
    fn a_router_whose_routes_claim_no_route_key_looks_up_no_message() {
        let mut owner_routes = OwnerRoutes::new(SubscriberChannels::new(16, 16, 16));
        let (keyless_route, _keyless_rx) = new_client(1, &SubscribeRequest::default(), 16);
        let (entries_route, mut entries_rx) = new_client(2, &entries_request(), 16);
        let message_batch = [entry_message(1)];
        owner_routes.apply_route_change(RouteChange::Join(keyless_route, None));
        send_processed(&mut owner_routes, &message_batch);
        assert_eq!(owner_routes.level_routers[0].message_stamp, 0);
        owner_routes.apply_route_change(RouteChange::Join(entries_route.clone(), None));
        send_processed(&mut owner_routes, &message_batch);
        assert_eq!(owner_routes.level_routers[0].message_stamp, 1);
        assert_eq!(
            received_items(&mut entries_rx),
            expected_updates(&entries_route, &message_batch)
        );
        let keyless_refilter = refilter_route(&entries_route, &SubscribeRequest::default());
        owner_routes.apply_route_change(RouteChange::Join(keyless_refilter, None));
        send_processed(&mut owner_routes, &message_batch);
        let processed_router = &owner_routes.level_routers[0];
        assert_eq!(processed_router.routes_by_session_id.len(), 2);
        assert_eq!(processed_router.message_stamp, 1);
    }

    /// Where a claim step that starts at `first_unclaimed_key` must end: after `keys_per_step`
    /// keys, at the end of `route_keys`, or right after the first new key that meets a full
    /// shard, whichever comes first.
    ///
    /// It is exact while the index's shards hold no tombstone.
    ///
    /// # Arguments
    ///
    /// * `route_index` - The index the step claims in.
    /// * `route_keys` - The staged route's key list.
    /// * `first_unclaimed_key` - The position of the first key the step claims.
    /// * `keys_per_step` - The most keys the step claims.
    ///
    /// # Returns
    ///
    /// The claimed key count after the step, and whether a full shard ended the step.
    fn predicted_claim_step_end(
        route_index: &ShardedRouteIndex,
        route_keys: &[RouteKey],
        first_unclaimed_key: usize,
        keys_per_step: usize,
    ) -> (usize, bool) {
        let step_end = (first_unclaimed_key + keys_per_step).min(route_keys.len());
        let mut added_by_shard = HashMap::<usize, usize>::default();
        let stepped_keys = route_keys[..step_end].iter().enumerate();
        for (key_position, route_key) in stepped_keys.skip(first_unclaimed_key) {
            if !route_index.claiming_session_ids(route_key).is_empty() {
                continue;
            }
            let shard_number = index_shard_number(route_index.route_key_hasher.hash_one(route_key));
            let index_shard = &route_index.index_shards[shard_number];
            let added_count = added_by_shard.entry(shard_number).or_default();
            if index_shard.len() + *added_count == index_shard.capacity() {
                return (key_position + 1, true);
            }
            *added_count += 1;
        }
        (step_end, false)
    }

    /// Each index step claims at most its keys per step, and ends right after a claim that may
    /// grow a shard. In shards that releases left with tombstones and room, every step but the
    /// last claims a whole step of keys.
    #[test]
    fn an_index_step_claims_at_most_its_keys_and_one_shard_growth() {
        let mut owner_routes = owner_routes_with(16, STEP_EVERY_PASS_LIMITS);
        let keys_per_step = ROUTE_KEYS_PER_INDEX_STEP.get();
        let first_route_keys = sorted_key_list((0..8192).map(numbered_route_key));
        let (first_route, _first_rx) = keyed_client(1, &first_route_keys);
        let _first_ready_rx = prepare_route(&mut owner_routes, first_route);
        let mut growth_ended_steps = 0;
        loop {
            let claimed_before = staged_key_count(&owner_routes, 1);
            let (predicted_end, ended_by_growth) = predicted_claim_step_end(
                &owner_routes.level_routers[0].route_index,
                &first_route_keys,
                claimed_before,
                keys_per_step,
            );
            let index_step = owner_routes.advance_index_step();
            assert_eq!(staged_key_count(&owner_routes, 1), predicted_end);
            growth_ended_steps += usize::from(ended_by_growth);
            if index_step == IndexStep::AnsweredPreparation {
                break;
            }
            assert_eq!(index_step, IndexStep::Advanced);
        }
        assert!(growth_ended_steps > 0);
        // 65,536 more keys claimed and released leave tombstones and room in the shards.
        let route_index = &mut owner_routes.level_routers[0].route_index;
        let churned_route_keys = (100_000..165_536)
            .map(numbered_route_key)
            .collect::<Vec<_>>();
        for route_key in &churned_route_keys {
            route_index.claim_route_key(*route_key, 2);
        }
        let capacities_before_release = route_index
            .index_shards
            .iter()
            .map(HashTable::capacity)
            .collect::<Vec<_>>();
        for route_key in &churned_route_keys {
            route_index.release_route_key(route_key, 2);
        }
        let index_shards = route_index.index_shards.iter();
        let tombstone_count = index_shards
            .zip(&capacities_before_release)
            .map(|(index_shard, capacity_before)| capacity_before - index_shard.capacity())
            .sum::<usize>();
        assert!(tombstone_count > 0);
        // 8,191 new keys, each in a shard with room left for it.
        let mut room_by_shard = route_index
            .index_shards
            .iter()
            .map(|index_shard| index_shard.capacity() - index_shard.len())
            .collect::<Vec<_>>();
        let route_key_hasher = route_index.route_key_hasher.clone();
        let fitting_route_keys = (200_000..)
            .map(numbered_route_key)
            .filter(|route_key| {
                let shard_number = index_shard_number(route_key_hasher.hash_one(route_key));
                let shard_room = &mut room_by_shard[shard_number];
                let key_fits = *shard_room > 0;
                *shard_room -= usize::from(key_fits);
                key_fits
            })
            .take(8191);
        let second_route_keys = sorted_key_list(fitting_route_keys);
        let (second_route, _second_rx) = keyed_client(3, &second_route_keys);
        let _second_ready_rx = prepare_route(&mut owner_routes, second_route);
        let mut claims_per_step = Vec::new();
        loop {
            let claimed_before = staged_key_count(&owner_routes, 3);
            let index_step = owner_routes.advance_index_step();
            claims_per_step.push(staged_key_count(&owner_routes, 3) - claimed_before);
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

    /// A staged route of 1,000 keys gets nothing routed while slices claim its keys between
    /// sends, and gets exactly what its filter makes from its hand-over on.
    #[test]
    fn a_prepared_route_routes_nothing_until_its_hand_over() {
        let mut owner_routes = owner_routes_with(1024, STEP_EVERY_PASS_LIMITS);
        let account_pubkeys = fixtures::deterministic_pubkeys(3, 1000);
        let subscribe_request = accounts_request(&account_pubkeys, &[], None, &[]);
        let (client_route, mut stream_rx) = new_client(1, &subscribe_request, MAX_CAPACITY);
        assert_eq!(client_route.route_keys.len(), 1000);
        let mut broadcast_rx = owner_routes.broadcast_channels.subscribe(Processed);
        let mut ready_rx = prepare_route(&mut owner_routes, client_route.clone());
        let mut sent_messages = Vec::new();
        let mut send_account_update = |owner_routes: &mut OwnerRoutes| {
            let account_pubkey = account_pubkeys[sent_messages.len() % account_pubkeys.len()];
            let account_message = Message::Account(fixtures::simple_message_account(
                account_pubkey,
                Pubkey::default(),
            ));
            send_processed(owner_routes, std::slice::from_ref(&account_message));
            sent_messages.push(account_message);
        };
        let mut staged_sends = 0;
        while ready_rx.try_recv().is_err() {
            owner_routes.run_index_slice();
            send_account_update(&mut owner_routes);
            staged_sends += 1;
            assert!(
                received_items(&mut stream_rx).is_empty(),
                "routed before its hand-over"
            );
        }
        assert!(staged_sends > 16, "{staged_sends}");
        // The client reads its receiver to the end, then hands it over.
        while let Ok(message_batch) = broadcast_rx.try_recv() {
            for broadcast_message in message_batch.iter() {
                assert!(client_route.queue_updates(broadcast_message).is_ok());
            }
        }
        let route_change = RouteChange::Join(client_route.clone(), Some(broadcast_rx));
        owner_routes.take_route_change(route_change);
        for _ in 0..3 {
            send_account_update(&mut owner_routes);
        }
        assert_eq!(
            received_items(&mut stream_rx),
            expected_updates(&client_route, &sent_messages)
        );
    }

    /// A preparation is answered by the step that claims the route's last key, and not before.
    /// A route with no keys is answered at its first step, and a route whose client is gone is
    /// unstaged at its answer.
    #[test]
    fn a_preparation_is_answered_at_the_step_that_claims_its_last_key() {
        let mut owner_routes = owner_routes_with(16, SINGLE_KEY_STEP_LIMITS);
        let route_keys = sorted_key_list((0..5).map(numbered_route_key).chain([RouteKey::Slots]));
        let (client_route, _stream_rx) = keyed_client(1, &route_keys);
        let mut ready_rx = prepare_route(&mut owner_routes, client_route);
        for claimed_key_count in 1..route_keys.len() {
            assert_eq!(owner_routes.advance_index_step(), IndexStep::Advanced);
            assert_eq!(staged_key_count(&owner_routes, 1), claimed_key_count);
            assert_eq!(
                ready_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            );
        }
        let last_step = owner_routes.advance_index_step();
        assert_eq!(last_step, IndexStep::AnsweredPreparation);
        assert_eq!(ready_rx.try_recv(), Ok(()));
        assert!(!owner_routes.has_index_work());
        let (keyless_route, _keyless_rx) = keyed_client(2, &sorted_key_list([]));
        let mut keyless_ready_rx = prepare_route(&mut owner_routes, keyless_route);
        let keyless_step = owner_routes.advance_index_step();
        assert_eq!(keyless_step, IndexStep::AnsweredPreparation);
        assert_eq!(keyless_ready_rx.try_recv(), Ok(()));
        assert_eq!(owner_routes.advance_index_step(), IndexStep::NoWorkLeft);
        // A client that dropped its answer receiver is gone: its route is unstaged at the answer.
        let (gone_route, _gone_rx) = keyed_client(3, &sorted_key_list([RouteKey::Slots]));
        drop(prepare_route(&mut owner_routes, gone_route));
        assert_eq!(owner_routes.advance_index_step(), IndexStep::Advanced);
        assert!(!owner_routes.staged_routes_by_session_id.contains_key(&3));
        assert_claims_match_routes(&owner_routes);
        let processed_router = &owner_routes.level_routers[0];
        assert_eq!(claim_count(processed_router, &RouteKey::Slots, 3), 0);
    }

    /// For every point the client last reads its receiver and every later point its hand-over
    /// lands, across 6 sends, its own reads, the owner's drain and the routed sends make exactly
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
                let mut owner_routes = owner_routes_with(16, STEP_EVERY_PASS_LIMITS);
                let (client_route, mut stream_rx) = new_client(1, &entries_request(), MAX_CAPACITY);
                let mut broadcast_rx = Some(owner_routes.broadcast_channels.subscribe(Processed));
                let mut ready_rx = prepare_route(&mut owner_routes, client_route.clone());
                run_every_index_step(&mut owner_routes);
                assert_eq!(ready_rx.try_recv(), Ok(()));
                for send_point in 0..=SEND_COUNT {
                    if send_point == last_read_point {
                        let client_rx = broadcast_rx.as_mut().unwrap();
                        while let Ok(message_batch) = client_rx.try_recv() {
                            for broadcast_message in message_batch.iter() {
                                assert!(client_route.queue_updates(broadcast_message).is_ok());
                            }
                        }
                    }
                    if send_point == hand_over_point {
                        let handed_over_rx = broadcast_rx.take();
                        drained_hand_overs +=
                            usize::from(!handed_over_rx.as_ref().unwrap().is_empty());
                        let route_change = RouteChange::Join(client_route.clone(), handed_over_rx);
                        owner_routes.take_route_change(route_change);
                    }
                    if let Some(message_batch) = message_batches.get(send_point) {
                        send_processed(&mut owner_routes, message_batch);
                    }
                }
                let twin_items = expected_updates(&client_route, &message_batches.concat());
                assert_eq!(
                    received_items(&mut stream_rx),
                    twin_items,
                    "read at {last_read_point}, hand-over at {hand_over_point}"
                );
            }
        }
        assert_eq!(drained_hand_overs, 21);
    }

    /// A join that no preparation came before claims every key of its route at once, leaves no
    /// index work, and delivers exactly what the filter makes.
    #[test]
    fn a_join_without_a_preparation_claims_its_keys_at_once() {
        let mut owner_routes = owner_routes_with(16, STEP_EVERY_PASS_LIMITS);
        let account_pubkeys = [Pubkey::new_unique(), Pubkey::new_unique()];
        let mut subscribe_request = accounts_request(&account_pubkeys, &[], None, &[]);
        subscribe_request.entry = entries_request().entry;
        let (client_route, mut stream_rx) = new_client(1, &subscribe_request, 16);
        let broadcast_rx = owner_routes.broadcast_channels.subscribe(Processed);
        let handed_over_batch = [entry_message(1)];
        send_processed(&mut owner_routes, &handed_over_batch);
        let route_change = RouteChange::Join(client_route.clone(), Some(broadcast_rx));
        owner_routes.take_route_change(route_change);
        assert!(!owner_routes.has_index_work());
        assert_claims_match_routes(&owner_routes);
        let routed_batch = account_pubkeys
            .map(|account_pubkey| {
                Message::Account(fixtures::simple_message_account(
                    account_pubkey,
                    Pubkey::default(),
                ))
            })
            .into_iter()
            .chain([entry_message(2)])
            .collect::<Vec<_>>();
        send_processed(&mut owner_routes, &routed_batch);
        let sent_messages = [&handed_over_batch[..], &routed_batch].concat();
        assert_eq!(
            received_items(&mut stream_rx),
            expected_updates(&client_route, &sent_messages)
        );
    }

    /// A join without a receiver whose route holds the live route's key list replaces the
    /// filter in place, at processed and at confirmed: no index work, and each message meets one
    /// filter. An equal key list in another [`Arc`] is claimed again instead, and the old claims
    /// wait for release.
    #[test]
    fn a_refilter_with_the_same_key_list_replaces_the_route_in_place() {
        for commitment_level in [Processed, CommitmentLevel::Confirmed] {
            let mut owner_routes = owner_routes_with(16, STEP_EVERY_PASS_LIMITS);
            let account_pubkey = Pubkey::new_unique();
            let mut whole_request = accounts_request(&[account_pubkey], &[], None, &[]);
            whole_request.commitment = Some(commitment_level as i32);
            let mut sliced_request = whole_request.clone();
            sliced_request.accounts_data_slice = vec![SubscribeRequestAccountsDataSlice {
                offset: 1,
                length: 2,
            }];
            let (client_route, mut stream_rx) = new_client(1, &whole_request, 16);
            owner_routes.take_route_change(RouteChange::Join(client_route.clone(), None));
            let account_info = fixtures::account_info(
                account_pubkey,
                Pubkey::default(),
                vec![1, 2, 3, 4],
                1,
                None,
            );
            let account_message = vec![Message::Account(fixtures::message_account(account_info))];
            owner_routes.send_batch(commitment_level, Arc::new(account_message.clone()));
            let sliced_filter = Arc::new(fixtures::filter_from_request(&sliced_request));
            let sliced_route = client_route.with_filter(&sliced_filter, &client_route.route_keys);
            owner_routes.take_route_change(RouteChange::Join(sliced_route.clone(), None));
            assert!(!owner_routes.has_index_work(), "{commitment_level:?}");
            assert_claims_match_routes(&owner_routes);
            owner_routes.send_batch(commitment_level, Arc::new(account_message.clone()));
            let mut expected_items = expected_updates(&client_route, &account_message);
            expected_items.extend(expected_updates(&sliced_route, &account_message));
            assert_eq!(expected_items.len(), 2);
            assert_ne!(expected_items[0], expected_items[1]);
            assert_eq!(received_items(&mut stream_rx), expected_items);
            let copied_route_keys = Arc::<[RouteKey]>::from(client_route.route_keys.to_vec());
            let copied_route =
                client_route.with_filter(&client_route.client_filter, &copied_route_keys);
            owner_routes.take_route_change(RouteChange::Join(copied_route, None));
            assert!(owner_routes.has_index_work(), "{commitment_level:?}");
            assert_claims_match_routes(&owner_routes);
        }
    }

    /// A leave of a staged route, at every count of claimed keys, releases the claimed prefix:
    /// its per-kind keys at once, the rest in index steps. The index ends empty and the route's
    /// queue closes.
    #[test]
    fn a_leave_releases_the_claimed_prefix_of_a_staged_route() {
        let route_keys = sorted_key_list(
            (0..4)
                .map(numbered_route_key)
                .chain([RouteKey::Slots, RouteKey::Entries]),
        );
        for claimed_key_count in 0..=route_keys.len() {
            let mut owner_routes = owner_routes_with(16, SINGLE_KEY_STEP_LIMITS);
            let (client_route, mut stream_rx) = keyed_client(1, &route_keys);
            let _ready_rx = prepare_route(&mut owner_routes, client_route);
            for _ in 0..claimed_key_count {
                owner_routes.advance_index_step();
            }
            assert_eq!(staged_key_count(&owner_routes, 1), claimed_key_count);
            leave_route(&mut owner_routes, 1);
            assert_claims_match_routes(&owner_routes);
            let processed_router = &owner_routes.level_routers[0];
            for per_kind_route_key in [RouteKey::Slots, RouteKey::Entries] {
                assert_eq!(claim_count(processed_router, &per_kind_route_key, 1), 0);
            }
            run_every_index_step(&mut owner_routes);
            assert_owner_holds_nothing(&owner_routes);
            assert!(matches!(stream_rx.recv().now_or_never(), Some(None)));
        }
    }

    /// Session 1 stages A, which is partly claimed, session 2 stages C, then session 1 leaves and
    /// stages B. The stale entry of A is skipped: C is answered before B, B once, A never, and the
    /// index holds the keys of B and C only.
    #[test]
    fn a_restaged_session_skips_its_stale_staging_entry() {
        let mut owner_routes = owner_routes_with(16, SINGLE_KEY_STEP_LIMITS);
        let first_route_keys = sorted_key_list((0..3).map(numbered_route_key));
        let other_route_keys = sorted_key_list((10..12).map(numbered_route_key));
        let restaged_route_keys = sorted_key_list((20..22).map(numbered_route_key));
        let (first_route, _first_rx) = keyed_client(1, &first_route_keys);
        let mut first_ready_rx = prepare_route(&mut owner_routes, first_route);
        assert_eq!(owner_routes.advance_index_step(), IndexStep::Advanced);
        let (other_route, _other_rx) = keyed_client(2, &other_route_keys);
        let mut other_ready_rx = prepare_route(&mut owner_routes, other_route);
        leave_route(&mut owner_routes, 1);
        let (restaged_route, _restaged_rx) = keyed_client(1, &restaged_route_keys);
        let mut restaged_ready_rx = prepare_route(&mut owner_routes, restaged_route);
        let mut answer_order = Vec::new();
        while owner_routes.advance_index_step() != IndexStep::NoWorkLeft {
            assert_claims_match_routes(&owner_routes);
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
        let processed_router = &owner_routes.level_routers[0];
        for route_key in other_route_keys.iter() {
            assert_eq!(claim_count(processed_router, route_key, 2), 1);
        }
        for route_key in restaged_route_keys.iter() {
            assert_eq!(claim_count(processed_router, route_key, 1), 1);
        }
        assert_eq!(processed_router.route_index.held_route_key_count, 4);
    }

    /// A route change in a [`releases_never_remove_a_claim_that_a_live_route_needs`] script, by
    /// session id, key set and commitment level.
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
    struct ScriptedRoutes {
        /// The key lists, by key set number.
        key_sets: Vec<Arc<[RouteKey]>>,
        /// A filter that matches nothing at each commitment level, by the level as `usize`.
        level_filters: [Arc<Filter>; 3],
    }

    impl ScriptedRoutes {
        /// The route of `session_id` with key set `key_set_number`, and the filter at
        /// `commitment_level`.
        ///
        /// # Arguments
        ///
        /// * `session_id` - The route's session id.
        /// * `key_set_number` - The position of the route's key list in `key_sets`.
        /// * `commitment_level` - The level of the route's filter.
        ///
        /// # Returns
        ///
        /// The route, which shares the key list's [`Arc`].
        fn scripted_route(
            &self,
            session_id: usize,
            key_set_number: usize,
            commitment_level: CommitmentLevel,
        ) -> Route {
            let route_keys = &self.key_sets[key_set_number];
            let (keyed_route, _stream_rx) = keyed_client(session_id, route_keys);
            keyed_route.with_filter(&self.level_filters[commitment_level as usize], route_keys)
        }
    }

    /// Runs `change_script` with `gap_steps[i]` index steps after its change `i`, and checks the
    /// claims after every change and step. Then every session leaves, every step runs, and the
    /// owner must hold nothing, so the next run starts from an empty owner.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - An owner that holds nothing.
    /// * `scripted_routes` - The key lists and filters the script uses.
    /// * `change_script` - The route changes, one per gap.
    /// * `gap_steps` - How many index steps run after each change.
    ///
    /// # Returns
    ///
    /// Whether every step asked for did index work.
    ///
    /// # Panics
    ///
    /// Panics if the claims ever differ from what the routes made, or if anything is left at the
    /// end.
    fn run_scripted_changes(
        owner_routes: &mut OwnerRoutes,
        scripted_routes: &ScriptedRoutes,
        change_script: &[ScriptedChange],
        gap_steps: &[usize],
    ) -> bool {
        let mut every_step_did_work = true;
        let mut ready_rxs = Vec::new();
        for (scripted_change, step_count) in change_script.iter().zip(gap_steps) {
            let route_change = match *scripted_change {
                ScriptedChange::JoinAtOnce(session_id, key_set_number, commitment_level) => {
                    let joined_route = scripted_routes.scripted_route(
                        session_id,
                        key_set_number,
                        commitment_level,
                    );
                    RouteChange::Join(joined_route, None)
                }
                ScriptedChange::Prepare(session_id, key_set_number, commitment_level) => {
                    let prepared_route = scripted_routes.scripted_route(
                        session_id,
                        key_set_number,
                        commitment_level,
                    );
                    let (ready_tx, ready_rx) = oneshot::channel();
                    ready_rxs.push(ready_rx);
                    RouteChange::Prepare(prepared_route, ready_tx)
                }
                ScriptedChange::HandOver(session_id, key_set_number, commitment_level) => {
                    let joined_route = scripted_routes.scripted_route(
                        session_id,
                        key_set_number,
                        commitment_level,
                    );
                    let broadcast_rx = owner_routes.broadcast_channels.subscribe(commitment_level);
                    RouteChange::Join(joined_route, Some(broadcast_rx))
                }
                ScriptedChange::Leave(session_id) => RouteChange::Leave(RouteLeave {
                    session_id,
                    receiver_request: None,
                }),
            };
            owner_routes.take_route_change(route_change);
            assert_claims_match_routes(owner_routes);
            for _ in 0..*step_count {
                if owner_routes.advance_index_step() == IndexStep::NoWorkLeft {
                    every_step_did_work = false;
                    break;
                }
                assert_claims_match_routes(owner_routes);
            }
            if !every_step_did_work {
                break;
            }
        }
        for session_id in [1, 2] {
            leave_route(owner_routes, session_id);
        }
        while owner_routes.advance_index_step() != IndexStep::NoWorkLeft {
            assert_claims_match_routes(owner_routes);
        }
        assert_owner_holds_nothing(owner_routes);
        every_step_did_work
    }

    /// Runs `change_script` under every placement of index steps between its changes: each gap
    /// gets every step count from 0 up to the work that waits there.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - An owner that holds nothing.
    /// * `scripted_routes` - The key lists and filters the script uses.
    /// * `change_script` - The route changes.
    /// * `gap_steps` - The step counts chosen for the gaps before this one.
    ///
    /// # Returns
    ///
    /// How many placements ran whole.
    fn run_every_interleaving(
        owner_routes: &mut OwnerRoutes,
        scripted_routes: &ScriptedRoutes,
        change_script: &[ScriptedChange],
        gap_steps: &mut Vec<usize>,
    ) -> usize {
        let mut whole_runs = 0;
        for step_count in 0.. {
            gap_steps.push(step_count);
            let scripted_prefix = &change_script[..gap_steps.len()];
            if !run_scripted_changes(owner_routes, scripted_routes, scripted_prefix, gap_steps) {
                gap_steps.pop();
                break;
            }
            whole_runs += if gap_steps.len() == change_script.len() {
                1
            } else {
                run_every_interleaving(owner_routes, scripted_routes, change_script, gap_steps)
            };
            gap_steps.pop();
        }
        whole_runs
    }

    /// Under every interleaving of one-key index steps with joins, preparations, hand-overs and
    /// leaves of overlapping key sets, the claims are exactly those of the live, staged and
    /// departed routes after every change and step. So a release never removes a claim that a
    /// live route needs, and the owner holds nothing once both sessions have left.
    #[test]
    fn releases_never_remove_a_claim_that_a_live_route_needs() {
        use {
            CommitmentLevel::Confirmed,
            ScriptedChange::{HandOver, JoinAtOnce, Leave, Prepare},
        };
        let key_sets = vec![
            sorted_key_list(
                [1, 2, 3]
                    .map(numbered_route_key)
                    .into_iter()
                    .chain([RouteKey::Slots]),
            ),
            sorted_key_list(
                [2, 3, 4]
                    .map(numbered_route_key)
                    .into_iter()
                    .chain([RouteKey::Slots, RouteKey::Entries]),
            ),
            sorted_key_list(
                [1, 4]
                    .map(numbered_route_key)
                    .into_iter()
                    .chain([RouteKey::Entries]),
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
        let scripted_routes = ScriptedRoutes {
            key_sets,
            level_filters,
        };
        let mut owner_routes = owner_routes_with(16, SINGLE_KEY_STEP_LIMITS);
        for change_script in change_scripts {
            let whole_runs = run_every_interleaving(
                &mut owner_routes,
                &scripted_routes,
                change_script,
                &mut Vec::new(),
            );
            assert!(whole_runs > 100, "{change_script:?}: {whole_runs}");
        }
    }

    /// A pass takes in at most 64 queued route changes: 1,000 leaves with receiver requests are
    /// answered over 16 passes, 64 per pass, in order.
    #[test]
    fn each_pass_takes_in_at_most_64_route_changes() {
        let mut owner_routes = owner_routes_with(16, STEP_EVERY_PASS_LIMITS);
        let (route_changes_tx, mut route_changes_rx) = mpsc::unbounded_channel();
        let mut answer_rxs = (0..1000)
            .map(|session_id| {
                let (answer_tx, answer_rx) = oneshot::channel();
                let client_leave = RouteLeave {
                    session_id,
                    receiver_request: Some((Processed, answer_tx)),
                };
                route_changes_tx
                    .send(RouteChange::Leave(client_leave))
                    .unwrap();
                answer_rx
            })
            .collect::<Vec<_>>();
        let mut answered_count = 0;
        for pass_number in 1..=16 {
            owner_routes.take_queued_route_changes(&mut route_changes_rx);
            let expected_answered_count = (MAX_ROUTE_CHANGES_PER_PASS * pass_number).min(1000);
            for answer_rx in &mut answer_rxs[answered_count..expected_answered_count] {
                assert!(answer_rx.try_recv().is_ok(), "pass {pass_number}");
            }
            if let Some(next_answer_rx) = answer_rxs.get_mut(expected_answered_count) {
                assert!(next_answer_rx.try_recv().is_err(), "pass {pass_number}");
            }
            answered_count = expected_answered_count;
        }
        assert_eq!(answered_count, 1000);
        assert!(route_changes_rx.is_empty());
    }

    /// A routed client whose queue fills in a batch is disconnected, and its route leaves at
    /// once. The batch releases only its per-kind keys; its other keys wait in
    /// `claims_to_release`. Those stale claims find it but deliver nothing, a refilter that lands
    /// later does not bring its route back and drops the preparation sent before it, and its
    /// stream ends.
    #[test]
    fn a_disconnect_in_route_batch_does_no_index_work_on_the_batch() {
        let mut owner_routes = owner_routes_with(16, STEP_EVERY_PASS_LIMITS);
        let account_pubkeys = [Pubkey::new_unique(), Pubkey::new_unique()];
        let accounts_only_request = accounts_request(&account_pubkeys, &[], None, &[]);
        let mut failing_request = accounts_only_request.clone();
        failing_request.entry = entries_request().entry;
        failing_request.slots = [("s".to_owned(), Default::default())].into();
        let (failing_route, mut failing_rx) = new_client(1, &failing_request, 2);
        let (healthy_route, mut healthy_rx) = new_client(2, &accounts_only_request, MAX_CAPACITY);
        owner_routes.take_route_change(RouteChange::Join(failing_route.clone(), None));
        owner_routes.take_route_change(RouteChange::Join(healthy_route.clone(), None));
        let entry_batch = [1, 2, 3].map(entry_message);
        let mut expected_failing_items = expected_updates(&failing_route, &entry_batch[..2]);
        expected_failing_items.push(Err("lagged to send an update".to_owned()));
        let late_preparation = refilter_route(&failing_route, &accounts_only_request);
        let late_refilter = refilter_route(&failing_route, &accounts_only_request);
        drop(failing_route);
        send_processed(&mut owner_routes, &entry_batch);
        let processed_router = &owner_routes.level_routers[0];
        assert!(!processed_router.routes_by_session_id.contains_key(&1));
        for per_kind_route_key in [RouteKey::Slots, RouteKey::Entries] {
            assert_eq!(claim_count(processed_router, &per_kind_route_key, 1), 0);
        }
        for account_pubkey in account_pubkeys {
            let account_route_key = RouteKey::Account(account_pubkey);
            assert_eq!(claim_count(processed_router, &account_route_key, 1), 1);
        }
        assert_eq!(processed_router.claims_to_release.len(), 1);
        assert_eq!(processed_router.claims_to_release[0].claimed_key_count, 2);
        assert_claims_match_routes(&owner_routes);
        let account_batch = account_pubkeys.map(|account_pubkey| {
            Message::Account(fixtures::simple_message_account(
                account_pubkey,
                Pubkey::default(),
            ))
        });
        let stamp_before = owner_routes.level_routers[0].message_stamp;
        send_processed(&mut owner_routes, &account_batch);
        assert_eq!(
            owner_routes.level_routers[0].message_stamp,
            stamp_before + 2
        );
        assert_eq!(
            received_items(&mut healthy_rx),
            expected_updates(&healthy_route, &account_batch)
        );
        let _late_ready_rx = prepare_route(&mut owner_routes, late_preparation);
        owner_routes.take_route_change(RouteChange::Join(late_refilter, None));
        assert!(!owner_routes.level_routers[0]
            .routes_by_session_id
            .contains_key(&1));
        assert!(!owner_routes.staged_routes_by_session_id.contains_key(&1));
        assert_eq!(received_items(&mut failing_rx), expected_failing_items);
        assert!(matches!(failing_rx.recv().now_or_never(), Some(None)));
        run_every_index_step(&mut owner_routes);
        assert_claims_match_routes(&owner_routes);
        assert_eq!(
            owner_routes.level_routers[0]
                .route_index
                .held_route_key_count,
            2
        );
    }

    /// A router whose live routes are all gone, but whose departed routes still hold claims,
    /// looks up no message of a batch.
    #[test]
    fn a_router_holding_only_claims_to_release_looks_up_no_message() {
        let mut owner_routes = owner_routes_with(16, STEP_EVERY_PASS_LIMITS);
        let account_pubkey = Pubkey::new_unique();
        let (client_route, _stream_rx) =
            new_client(1, &accounts_request(&[account_pubkey], &[], None, &[]), 16);
        owner_routes.take_route_change(RouteChange::Join(client_route, None));
        leave_route(&mut owner_routes, 1);
        let processed_router = &owner_routes.level_routers[0];
        assert!(processed_router.routes_by_session_id.is_empty());
        assert!(!processed_router.route_index.is_empty());
        let account_message = [Message::Account(fixtures::simple_message_account(
            account_pubkey,
            Pubkey::default(),
        ))];
        send_processed(&mut owner_routes, &account_message);
        assert_eq!(owner_routes.level_routers[0].message_stamp, 0);
    }

    /// A route that departs releases its per-kind keys as it leaves, and its other keys only in
    /// index steps. A staged route whose claimed prefix reaches its per-kind keys does the same.
    #[test]
    fn a_departed_route_releases_its_per_kind_keys_at_once() {
        let mut owner_routes = owner_routes_with(16, SINGLE_KEY_STEP_LIMITS);
        let route_keys = sorted_key_list([
            numbered_route_key(1),
            RouteKey::Transaction(numbered_pubkey(2)),
            RouteKey::Slots,
            RouteKey::Entries,
            RouteKey::BlocksMeta,
        ]);
        let per_kind_start = route_keys.partition_point(|route_key| *route_key < RouteKey::Slots);
        assert_eq!(per_kind_start, 2);
        let (live_route, _live_rx) = keyed_client(1, &route_keys);
        owner_routes.take_route_change(RouteChange::Join(live_route, None));
        leave_route(&mut owner_routes, 1);
        let processed_router = &owner_routes.level_routers[0];
        for route_key in &route_keys[per_kind_start..] {
            assert_eq!(claim_count(processed_router, route_key, 1), 0);
        }
        for route_key in &route_keys[..per_kind_start] {
            assert_eq!(claim_count(processed_router, route_key, 1), 1);
        }
        assert_eq!(run_every_index_step(&mut owner_routes), 2);
        assert_owner_holds_nothing(&owner_routes);
        let (staged_route, _staged_rx) = keyed_client(2, &route_keys);
        let _ready_rx = prepare_route(&mut owner_routes, staged_route);
        for _ in 0..4 {
            owner_routes.advance_index_step();
        }
        leave_route(&mut owner_routes, 2);
        let processed_router = &owner_routes.level_routers[0];
        for route_key in route_keys.iter() {
            let expected_claims = usize::from(*route_key < RouteKey::Slots);
            assert_eq!(claim_count(processed_router, route_key, 2), expected_claims);
        }
        run_every_index_step(&mut owner_routes);
        assert_owner_holds_nothing(&owner_routes);
    }

    /// One index slice answers at most eight preparations, however large its budget.
    #[test]
    fn an_index_slice_answers_at_most_eight_preparations() {
        let large_budget_limits = RouteIndexLimits {
            slice_interval: Duration::ZERO,
            slice_budget: Duration::from_secs(3600),
            keys_per_step: ROUTE_KEYS_PER_INDEX_STEP,
        };
        let mut owner_routes = owner_routes_with(16, large_budget_limits);
        let slots_key_list = sorted_key_list([RouteKey::Slots]);
        let mut ready_rxs = (0..100)
            .map(|session_id| {
                let (staged_route, _stream_rx) = keyed_client(session_id, &slots_key_list);
                prepare_route(&mut owner_routes, staged_route)
            })
            .collect::<Vec<_>>();
        for answered_so_far in [8, 16] {
            owner_routes.run_index_slice();
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
        let paused_limits = RouteIndexLimits {
            slice_interval,
            slice_budget: Duration::ZERO,
            keys_per_step: NonZeroUsize::MIN,
        };
        let mut owner_routes = owner_routes_with(16, paused_limits);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!owner_routes.index_slice_is_overdue());
        let first_route_keys = sorted_key_list((0..4).map(numbered_route_key));
        let (first_route, _first_rx) = keyed_client(1, &first_route_keys);
        let _first_ready_rx = prepare_route(&mut owner_routes, first_route);
        tokio::time::advance(Duration::from_micros(9_999)).await;
        assert!(!owner_routes.index_slice_is_overdue());
        // A change while work already waits keeps the time the work is overdue.
        let (second_route, _second_rx) = keyed_client(2, &first_route_keys);
        let _second_ready_rx = prepare_route(&mut owner_routes, second_route);
        tokio::time::advance(Duration::from_micros(1)).await;
        assert!(owner_routes.index_slice_is_overdue());
        owner_routes.run_index_slice();
        let slice_end = Instant::now();
        assert!(owner_routes.has_index_work());
        assert!(!owner_routes.index_slice_is_overdue());
        assert_eq!(
            owner_routes.next_index_slice_at(),
            slice_end + slice_interval
        );
        tokio::time::advance(Duration::from_micros(9_999)).await;
        assert!(!owner_routes.index_slice_is_overdue());
        tokio::time::advance(Duration::from_micros(1)).await;
        assert!(owner_routes.index_slice_is_overdue());
        while owner_routes.has_index_work() {
            owner_routes.run_index_slice();
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!owner_routes.index_slice_is_overdue());
        leave_route(&mut owner_routes, 1);
        assert!(owner_routes.has_index_work());
        assert!(!owner_routes.index_slice_is_overdue());
    }

    /// Index work that a disconnect in a batch leaves waits ten slice intervals before it is
    /// overdue, as the work of a route change does. So the owner runs no slice at the top of its
    /// next pass only because its last slice ended long ago.
    #[tokio::test(start_paused = true)]
    async fn index_work_a_batch_disconnect_leaves_is_overdue_only_after_ten_intervals() {
        let paused_limits = RouteIndexLimits {
            slice_interval: Duration::from_millis(1),
            slice_budget: Duration::ZERO,
            keys_per_step: NonZeroUsize::MIN,
        };
        let mut owner_routes = owner_routes_with(16, paused_limits);
        let account_pubkey = Pubkey::new_unique();
        let account_only_request = accounts_request(&[account_pubkey], &[], None, &[]);
        let (client_route, stream_rx) = new_client(1, &account_only_request, MAX_CAPACITY);
        owner_routes.take_route_change(RouteChange::Join(client_route, None));
        assert!(!owner_routes.has_index_work());
        tokio::time::advance(Duration::from_secs(1)).await;
        drop(stream_rx);
        let account_message = fixtures::simple_message_account(account_pubkey, Pubkey::default());
        send_processed(&mut owner_routes, &[Message::Account(account_message)]);
        assert!(!owner_routes.level_routers[0]
            .routes_by_session_id
            .contains_key(&1));
        assert!(owner_routes.has_index_work());
        assert!(!owner_routes.index_slice_is_overdue());
        tokio::time::advance(Duration::from_micros(9_999)).await;
        assert!(!owner_routes.index_slice_is_overdue());
        tokio::time::advance(Duration::from_micros(1)).await;
        assert!(owner_routes.index_slice_is_overdue());
    }

    /// While a departed route's claims wait for release, index steps release them before they
    /// claim any key of a staged route.
    #[test]
    fn releases_run_before_claims() {
        let mut owner_routes = owner_routes_with(16, SINGLE_KEY_STEP_LIMITS);
        let departed_route_keys = sorted_key_list((0..3).map(numbered_route_key));
        let (departed_route, _departed_rx) = keyed_client(1, &departed_route_keys);
        owner_routes.take_route_change(RouteChange::Join(departed_route, None));
        leave_route(&mut owner_routes, 1);
        let staged_route_keys = sorted_key_list((10..13).map(numbered_route_key));
        let (staged_route, _staged_rx) = keyed_client(2, &staged_route_keys);
        let _ready_rx = prepare_route(&mut owner_routes, staged_route);
        for released_key_count in 1..=3 {
            assert_eq!(owner_routes.advance_index_step(), IndexStep::Advanced);
            let processed_router = &owner_routes.level_routers[0];
            let unreleased_claims = departed_route_keys
                .iter()
                .filter(|route_key| claim_count(processed_router, route_key, 1) == 1)
                .count();
            assert_eq!(unreleased_claims, 3 - released_key_count);
            assert_eq!(staged_key_count(&owner_routes, 2), 0);
        }
        assert_eq!(owner_routes.advance_index_step(), IndexStep::Advanced);
        assert_eq!(staged_key_count(&owner_routes, 2), 1);
    }

    /// A warm owner that has no route work makes no allocator call in a pass: an empty take-in,
    /// the two index work checks and a routed send.
    #[test]
    fn a_pass_with_no_route_work_makes_no_allocator_call() {
        let mut owner_routes = owner_routes_with(16, RouteIndexLimits::default());
        let _processed_rx = owner_routes.broadcast_channels.subscribe(Processed);
        let (_route_changes_tx, mut route_changes_rx) = mpsc::unbounded_channel();
        let [account_pubkey, missing_pubkey] = std::array::from_fn(|_| Pubkey::new_unique());
        let mut stream_rxs = Vec::new();
        for session_id in 0..16 {
            let near_miss_request = accounts_request(&[account_pubkey], &[], Some(0), &[]);
            let (client_route, stream_rx) = new_client(session_id, &near_miss_request, 16);
            owner_routes.take_route_change(RouteChange::Join(client_route, None));
            stream_rxs.push(stream_rx);
        }
        let account_data = [[0; 32], MEMCMP_BYTES].concat();
        let account_info =
            fixtures::account_info(account_pubkey, missing_pubkey, account_data, 1, None);
        let message_batch = Arc::new(vec![Message::Account(fixtures::message_account(
            account_info,
        ))]);
        owner_routes.send_batch(Processed, Arc::clone(&message_batch));
        let mut work_found = false;
        let allocator_calls = allocator_calls_during(|| {
            for _ in 0..32 {
                owner_routes.take_queued_route_changes(&mut route_changes_rx);
                work_found |= owner_routes.has_index_work();
                work_found |= owner_routes.index_slice_is_overdue();
                owner_routes.send_batch(Processed, Arc::clone(&message_batch));
            }
        });
        assert_eq!(allocator_calls, 0);
        assert!(!work_found);
        assert_eq!(owner_routes.level_routers[0].message_stamp, 33);
        for stream_rx in &mut stream_rxs {
            assert!(received_items(stream_rx).is_empty());
        }
    }

    /// Once a cycle has warmed the owner up, a preparation's take-in, its claim step and answer,
    /// a hand-over of an empty receiver and a leave with a receiver request make no allocator
    /// call, and nor do the releases that follow.
    #[test]
    fn a_warm_take_in_and_hand_over_make_no_allocator_call() {
        let mut owner_routes = owner_routes_with(16, STEP_EVERY_PASS_LIMITS);
        let route_keys = sorted_key_list(
            (0..8)
                .map(numbered_route_key)
                .chain([RouteKey::Slots, RouteKey::Entries]),
        );
        let (client_route, _stream_rx) = keyed_client(1, &route_keys);
        let route_cycle = |owner_routes: &mut OwnerRoutes| {
            let (ready_tx, mut ready_rx) = oneshot::channel();
            let broadcast_rx = owner_routes.broadcast_channels.subscribe(Processed);
            let (answer_tx, mut answer_rx) = oneshot::channel();
            let (prepared_route, joined_route) = (client_route.clone(), client_route.clone());
            let allocator_calls = allocator_calls_during(|| {
                owner_routes.take_route_change(RouteChange::Prepare(prepared_route, ready_tx));
                while owner_routes.has_index_work() {
                    owner_routes.run_index_slice();
                }
                owner_routes.take_route_change(RouteChange::Join(joined_route, Some(broadcast_rx)));
                let client_leave = RouteLeave {
                    session_id: 1,
                    receiver_request: Some((Processed, answer_tx)),
                };
                owner_routes.take_route_change(RouteChange::Leave(client_leave));
                while owner_routes.has_index_work() {
                    owner_routes.run_index_slice();
                }
            });
            assert_eq!(ready_rx.try_recv(), Ok(()));
            assert!(answer_rx.try_recv().is_ok());
            assert_owner_holds_nothing(owner_routes);
            allocator_calls
        };
        route_cycle(&mut owner_routes);
        assert_eq!(route_cycle(&mut owner_routes), 0);
    }

    /// How far the preparation of a client on the broadcast got, in
    /// [`routed_clients_match_their_twins_under_random_route_work`].
    enum ModelledPreparation {
        /// The client sent no preparation, or left the one it sent.
        Unprepared,
        /// The client waits for the owner's answer.
        Preparing(oneshot::Receiver<()>),
        /// The owner answered, so the client hands over at its next empty read.
        Prepared,
    }

    /// Where a client of [`routed_clients_match_their_twins_under_random_route_work`] stands, as
    /// `client_loop` would see it.
    enum ModelledStanding {
        /// On the broadcast, with its receiver.
        OnTheBroadcast(BroadcastReceiver, ModelledPreparation),
        /// The client handed its receiver over, so the owner routes it once the join lands.
        Routed,
        /// The client sent a leave with a receiver request and waits for the answer.
        AwaitingReceiver(oneshot::Receiver<BroadcastReceiver>),
        /// The client left, or a full queue ended it.
        Ended,
    }

    /// How a modelled client's stream ended before the end of the run.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum EarlyEnd {
        /// The owner disconnected the client: its queue had no room for a routed update.
        OwnerDisconnected,
        /// The client's own read of its receiver found no room in its queue.
        OwnReadOverflowed,
    }

    /// One client of [`routed_clients_match_their_twins_under_random_route_work`]: what it holds,
    /// where it stands, and what the model says it gets.
    struct ModelledClient {
        /// The client's session id.
        session_id: usize,
        /// The client's route template, which every route it sends copies.
        route_template: Route,
        /// The client's own sender into its queue, for what it reads from the broadcast.
        stream_tx: SubscribeSender,
        /// The receiving half of the client's queue.
        stream_rx: SubscribeReceiver,
        /// The client's current request.
        subscribe_request: SubscribeRequest,
        /// The client's current filter.
        client_filter: Arc<Filter>,
        /// The current filter's key list, kept as one [`Arc`] while the keys stay equal.
        route_keys: Option<Arc<[RouteKey]>>,
        /// Where the client stands.
        client_standing: ModelledStanding,
        /// The filter the owner routes the client with, by the model: set when a join lands,
        /// and cleared when a leave lands.
        routed_filter: Option<Arc<Filter>>,
        /// The batches the client's current receiver holds unread, by the model.
        unread_batches: Option<Vec<BroadcastedMessage>>,
        /// What the client gets, by the model, in order.
        expected_items: QueueItems,
        /// What the client's queue held so far.
        received_items: QueueItems,
        /// How the client's stream ended early, if it did.
        early_end: Option<EarlyEnd>,
    }

    /// A route change in flight to the owner, as the model sees it.
    enum ModelledChange {
        /// A preparation.
        Prepare,
        /// A join with this filter, with the client's receiver or without one.
        Join(Arc<Filter>, bool),
        /// A leave, with a receiver request or without one.
        Leave(bool),
    }

    /// The owner and its clients in [`routed_clients_match_their_twins_under_random_route_work`],
    /// with the route changes in flight between them.
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
        owner_routes: OwnerRoutes,
        /// The clients, ended ones included.
        modelled_clients: Vec<ModelledClient>,
        /// Sends the clients' route changes to the owner.
        route_changes_tx: mpsc::UnboundedSender<RouteChange>,
        /// The owner's route change receiver.
        route_changes_rx: mpsc::UnboundedReceiver<RouteChange>,
        /// The client and the model of each route change in flight, oldest first.
        changes_in_flight: VecDeque<(usize, ModelledChange)>,
        /// How often each modelled event happened, by name, over the run.
        event_counts: HashMap<&'static str, usize>,
    }

    impl ModelledOwner {
        /// Creates an owner with no client, on channels that never lag in a run.
        ///
        /// # Arguments
        ///
        /// * `route_index_limits` - The owner's index limits.
        ///
        /// # Returns
        ///
        /// The modelled owner.
        fn new(route_index_limits: RouteIndexLimits) -> Self {
            let (route_changes_tx, route_changes_rx) = mpsc::unbounded_channel();
            Self {
                owner_routes: owner_routes_with(4096, route_index_limits),
                modelled_clients: Vec::new(),
                route_changes_tx,
                route_changes_rx,
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

        /// Sends `route_change` of client `client_number` to the owner, and records its model.
        ///
        /// # Arguments
        ///
        /// * `client_number` - The client's position in `modelled_clients`.
        /// * `route_change` - The route change.
        /// * `modelled_change` - What the model makes of it when it lands.
        fn send_route_change(
            &mut self,
            client_number: usize,
            route_change: RouteChange,
            modelled_change: ModelledChange,
        ) {
            self.route_changes_tx.send(route_change).unwrap();
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
            let route_template =
                Route::new(session_id, "test", &stream_tx, &CancellationToken::new());
            let subscribe_request = random_request(seeded_rng, pubkey_pool);
            let client_filter = Arc::new(fixtures::filter_from_request(&subscribe_request));
            let route_keys = client_filter.route_keys().map(Arc::from);
            let broadcast_rx = self.owner_routes.broadcast_channels.subscribe(Processed);
            self.modelled_clients.push(ModelledClient {
                session_id,
                route_template,
                stream_tx,
                stream_rx,
                subscribe_request,
                client_filter,
                route_keys,
                client_standing: ModelledStanding::OnTheBroadcast(
                    broadcast_rx,
                    ModelledPreparation::Unprepared,
                ),
                routed_filter: None,
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
            let client_leave = RouteLeave {
                session_id: modelled_client.session_id,
                receiver_request: None,
            };
            let route_change = RouteChange::Leave(client_leave);
            self.send_route_change(client_number, route_change, ModelledChange::Leave(false));
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
                ModelledStanding::Routed | ModelledStanding::Ended => return,
            }
            if !self.read_to_the_end(client_number) {
                return;
            }
            let modelled_client = &mut self.modelled_clients[client_number];
            let Some(route_keys) = modelled_client.route_keys.clone() else {
                return;
            };
            let client_route = modelled_client
                .route_template
                .with_filter(&modelled_client.client_filter, &route_keys);
            let ModelledStanding::OnTheBroadcast(_, preparation) =
                &mut modelled_client.client_standing
            else {
                unreachable!("the client read its receiver");
            };
            let change_to_send = match preparation {
                ModelledPreparation::Unprepared => {
                    let (ready_tx, ready_rx) = oneshot::channel();
                    *preparation = ModelledPreparation::Preparing(ready_rx);
                    let route_change = RouteChange::Prepare(client_route, ready_tx);
                    Some((route_change, ModelledChange::Prepare))
                }
                ModelledPreparation::Preparing(ready_rx) => {
                    match ready_rx.try_recv() {
                        Ok(()) => {
                            let session_id = modelled_client.session_id;
                            let staged_routes = &self.owner_routes.staged_routes_by_session_id;
                            let claimed_key_count = staged_routes[&session_id].claimed_key_count;
                            assert_eq!(claimed_key_count, route_keys.len(), "answered early");
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
                        ModelledStanding::Routed,
                    );
                    let ModelledStanding::OnTheBroadcast(broadcast_rx, _) = broadcast_standing
                    else {
                        unreachable!("the client read its receiver");
                    };
                    let joined_filter = Arc::clone(&modelled_client.client_filter);
                    let route_change = RouteChange::Join(client_route, Some(broadcast_rx));
                    Some((route_change, ModelledChange::Join(joined_filter, true)))
                }
            };
            if let Some((route_change, modelled_change)) = change_to_send {
                if matches!(modelled_change, ModelledChange::Join(..)) {
                    self.count_event("hand-over sent");
                }
                self.send_route_change(client_number, route_change, modelled_change);
            }
        }

        /// Changes the filter of client `client_number`, as `client_loop` does once its owner
        /// runs two-phase joins: a same-key change of a routed client replaces the route's
        /// filter, and any other change of a routed client leaves with a receiver request. On
        /// the broadcast, a key change leaves the preparation.
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
            let new_route_keys = match (new_filter.route_keys(), &modelled_client.route_keys) {
                (Some(listed_keys), Some(held_keys)) if held_keys[..] == listed_keys[..] => {
                    Some(Arc::clone(held_keys))
                }
                (listed_keys, _) => listed_keys.map(Arc::from),
            };
            let route_moved = match (&new_route_keys, &modelled_client.route_keys) {
                (Some(new_keys), Some(held_keys)) => !Arc::ptr_eq(new_keys, held_keys),
                (new_keys, held_keys) => new_keys.is_some() != held_keys.is_some(),
            };
            modelled_client.subscribe_request = new_request;
            modelled_client.client_filter = Arc::clone(&new_filter);
            modelled_client.route_keys = new_route_keys.clone();
            let session_id = modelled_client.session_id;
            let (route_change, modelled_change, event_name) = match &mut modelled_client
                .client_standing
            {
                ModelledStanding::Routed if !route_moved => {
                    let route_keys = new_route_keys.expect("a routed client's keys");
                    let refiltered_route = modelled_client
                        .route_template
                        .with_filter(&new_filter, &route_keys);
                    let route_change = RouteChange::Join(refiltered_route, None);
                    let modelled_change = ModelledChange::Join(new_filter, false);
                    (route_change, modelled_change, "same-key refilter sent")
                }
                ModelledStanding::Routed => {
                    let (answer_tx, answer_rx) = oneshot::channel();
                    modelled_client.client_standing = ModelledStanding::AwaitingReceiver(answer_rx);
                    let client_leave = RouteLeave {
                        session_id,
                        receiver_request: Some((Processed, answer_tx)),
                    };
                    let route_change = RouteChange::Leave(client_leave);
                    (
                        route_change,
                        ModelledChange::Leave(true),
                        "key-changing swap sent",
                    )
                }
                ModelledStanding::OnTheBroadcast(_, preparation) => {
                    if !route_moved || matches!(preparation, ModelledPreparation::Unprepared) {
                        return;
                    }
                    *preparation = ModelledPreparation::Unprepared;
                    let client_leave = RouteLeave {
                        session_id,
                        receiver_request: None,
                    };
                    let route_change = RouteChange::Leave(client_leave);
                    (
                        route_change,
                        ModelledChange::Leave(false),
                        "preparation left",
                    )
                }
                ModelledStanding::AwaitingReceiver(_) | ModelledStanding::Ended => {
                    unreachable!("checked above")
                }
            };
            self.count_event(event_name);
            self.send_route_change(client_number, route_change, modelled_change);
        }

        /// Ends, through its last leave, each client that an owner disconnected since the last
        /// check.
        fn end_disconnected_clients(&mut self) {
            for client_number in 0..self.modelled_clients.len() {
                let modelled_client = &self.modelled_clients[client_number];
                let route_template = &modelled_client.route_template;
                let disconnected_by_owner = route_template.disconnect_reason().is_some();
                if disconnected_by_owner && modelled_client.early_end.is_none() {
                    self.modelled_clients[client_number].routed_filter = None;
                    self.end_client(client_number, Some(EarlyEnd::OwnerDisconnected));
                    self.count_event("owner disconnect");
                }
            }
        }

        /// One owner pass: takes in what the owner's channel holds, at most 64 changes, and
        /// applies the model of each change that landed.
        fn take_in_queued_changes(&mut self) {
            let landed_count = self.changes_in_flight.len().min(MAX_ROUTE_CHANGES_PER_PASS);
            self.owner_routes
                .take_queued_route_changes(&mut self.route_changes_rx);
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
                        modelled_client.routed_filter = Some(joined_filter);
                    }
                    ModelledChange::Join(joined_filter, false) => {
                        if modelled_client.routed_filter.is_some() {
                            modelled_client.routed_filter = Some(joined_filter);
                            *self
                                .event_counts
                                .entry("live route refiltered")
                                .or_default() += 1;
                        }
                    }
                    ModelledChange::Leave(true) => {
                        modelled_client.routed_filter = None;
                        modelled_client.unread_batches = Some(Vec::new());
                    }
                    ModelledChange::Leave(false) => modelled_client.routed_filter = None,
                }
            }
            self.end_disconnected_clients();
        }

        /// Sends `message_batch` through the owner, after checking the index's claims, and
        /// applies its model: a routed client gets what its routed filter makes, and a client's
        /// receiver holds the batch.
        ///
        /// # Arguments
        ///
        /// * `message_batch` - The batch to send.
        fn send_batch(&mut self, message_batch: Vec<Message>) {
            assert_claims_match_routes(&self.owner_routes);
            let message_batch = Arc::new(message_batch);
            for modelled_client in &mut self.modelled_clients {
                if let Some(routed_filter) = &modelled_client.routed_filter {
                    for sent_message in message_batch.iter() {
                        let filtered_updates =
                            routed_filter.get_updates(sent_message, Some(Processed));
                        modelled_client
                            .expected_items
                            .extend(filtered_updates.into_iter().map(Ok));
                    }
                }
                if let Some(unread_batches) = &mut modelled_client.unread_batches {
                    unread_batches.push(Arc::clone(&message_batch));
                }
            }
            self.owner_routes.send_batch(Processed, message_batch);
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
            while self.owner_routes.has_index_work() {
                self.owner_routes.run_index_slice();
            }
            assert_owner_holds_nothing(&self.owner_routes);
            self.drain_queues();
            for modelled_client in std::mem::take(&mut self.modelled_clients) {
                let ModelledClient {
                    session_id,
                    route_template,
                    stream_tx,
                    mut stream_rx,
                    expected_items,
                    received_items,
                    early_end,
                    ..
                } = modelled_client;
                drop((route_template, stream_tx));
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
    /// 300 steps per seed: before every send the claims are exactly those of the routes, each
    /// queue gets what the filter in effect makes of every batch, in order, and the owner holds
    /// nothing once every client has left.
    #[test]
    fn routed_clients_match_their_twins_under_random_route_work() {
        let pubkey_pool = fixtures::deterministic_pubkeys(5, 8);
        let mut event_counts = HashMap::<&'static str, usize>::default();
        for rng_seed in 0..60 {
            let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
            let route_index_limits = [SINGLE_KEY_STEP_LIMITS, STEP_EVERY_PASS_LIMITS];
            let mut modelled_owner = ModelledOwner::new(route_index_limits[rng_seed as usize % 2]);
            for _ in 0..4 {
                modelled_owner.spawn_client(&mut seeded_rng, &pubkey_pool);
            }
            for _ in 0..300 {
                let client_count = modelled_owner.modelled_clients.len();
                let client_number = seeded_rng.usize(..client_count);
                match seeded_rng.u8(..100) {
                    0..=19 => modelled_owner.run_client_loop_top(client_number),
                    20..=34 => {
                        // A routed client's stream drains only in the rarer global drains.
                        let modelled_client = &modelled_owner.modelled_clients[client_number];
                        if !matches!(modelled_client.client_standing, ModelledStanding::Routed) {
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
                    60..=74 => modelled_owner.owner_routes.run_index_slice(),
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
            "live route refiltered",
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
