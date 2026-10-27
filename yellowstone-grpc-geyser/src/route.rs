//! Routes each update to the subscriber streams whose filters match it. One owner per commitment
//! level applies batches and route changes in order. An index over the filters' keys finds the
//! streams an update can match, so a stream that cannot match is never woken. A stream the index
//! does not cover reads the level's raw ring and filters its own batches.

use {
    crate::{
        grpc::{BroadcastedMessage, SubscribeSender},
        metrics::SubscriberMetrics,
        plugin::{
            filter::{token_balance_owners, Filter, FilterKeys},
            message::{CommitmentLevel, Message},
        },
        trace,
        util::stream::TrySendError,
    },
    foldhash::HashMap as FoldHashMap,
    smallvec::SmallVec,
    solana_pubkey::Pubkey,
    std::{
        fmt,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    },
    tokio::sync::{broadcast, mpsc, oneshot},
};

/// Limits on the streams the router filters itself, so the shared path stays cheap. A stream
/// over them filters its own batches, and lags and is dropped on its own.
const MAX_INDEXED_KEYS: usize = 8192;
const MAX_INDEXED_ACCOUNT_FILTERS: usize = 64;
const MAX_INDEXED_TRANSACTION_FILTERS: usize = 8;
/// The most unread raw batches the owner filters for a stream that goes live.
const MAX_GO_LIVE_BACKLOG: usize = 4;

/// Whether the router can filter for a stream with these keys. Blocks filters walk whole
/// blocks, and token balance owners would be parsed for every transaction.
const fn indexable(keys: &FilterKeys) -> bool {
    !keys.accounts_any
        && !keys.transactions_any
        && !keys.blocks
        && !keys.token_owners
        && keys.account_filters <= MAX_INDEXED_ACCOUNT_FILTERS
        && keys.transaction_filters <= MAX_INDEXED_TRANSACTION_FILTERS
}

/// Route slots as bits, so the candidates of several keys and update kinds combine with word ORs.
#[derive(Debug, Default, Clone)]
struct RouteSet(Vec<u64>);

impl RouteSet {
    fn insert(&mut self, slot: u32) {
        let word = slot as usize / 64;
        if word >= self.0.len() {
            self.0.resize(word + 1, 0);
        }
        self.0[word] |= 1 << (slot % 64);
    }

    fn remove(&mut self, slot: u32) {
        if let Some(word) = self.0.get_mut(slot as usize / 64) {
            *word &= !(1 << (slot % 64));
        }
    }

    fn union(&mut self, other: &Self) {
        if other.0.len() > self.0.len() {
            self.0.resize(other.0.len(), 0);
        }
        for (word, other) in self.0.iter_mut().zip(&other.0) {
            *word |= other;
        }
    }

    fn clear(&mut self) {
        self.0.fill(0);
    }

    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter().enumerate().flat_map(|(index, &word)| {
            let mut word = word;
            std::iter::from_fn(move || {
                (word != 0).then(|| {
                    let bit = word.trailing_zeros();
                    word &= word - 1;
                    index as u32 * 64 + bit
                })
            })
        })
    }
}

/// The route slots listed under one key. Most keys belong to one stream.
type RouteSlots = SmallVec<[u32; 2]>;

/// Finds the routes whose filters may match an update. It errs on the side of too many: each
/// candidate's own [`Filter`] makes the exact match.
#[derive(Debug, Default)]
struct Index {
    accounts: FoldHashMap<Pubkey, RouteSlots>,
    /// Keyed by memcmp offset and bytes; `data_offsets` counts the keys at each offset.
    data: FoldHashMap<(usize, Pubkey), RouteSlots>,
    data_offsets: Vec<(usize, usize)>,
    owners: FoldHashMap<Pubkey, RouteSlots>,
    transactions: FoldHashMap<Pubkey, RouteSlots>,
    /// How many routes also look up the token balance owners of a transaction.
    token_owners: usize,
    accounts_any: RouteSet,
    transactions_any: RouteSet,
    entries: RouteSet,
    update_parent: RouteSet,
    slots: RouteSet,
    blocks: RouteSet,
    blocks_meta: RouteSet,
    block_footer: RouteSet,
}

impl Index {
    fn insert(&mut self, slot: u32, keys: &FilterKeys) {
        self.replace(slot, &FilterKeys::default(), keys);
    }

    fn remove(&mut self, slot: u32, keys: &FilterKeys) {
        self.replace(slot, keys, &FilterKeys::default());
    }

    /// Lists `slot` under `new` instead of `old`, touching only the keys that differ.
    fn replace(&mut self, slot: u32, old: &FilterKeys, new: &FilterKeys) {
        for (map, old, new) in [
            (&mut self.accounts, &old.accounts, &new.accounts),
            (&mut self.owners, &old.owners, &new.owners),
            (&mut self.transactions, &old.transactions, &new.transactions),
        ] {
            Self::replace_keys(map, slot, old, new, |_, _| {});
        }
        let offsets = &mut self.data_offsets;
        Self::replace_keys(
            &mut self.data,
            slot,
            &old.data,
            &new.data,
            |(offset, _), delta| match offsets.iter().position(|(known, _)| known == offset) {
                Some(position) => {
                    offsets[position].1 = offsets[position].1.wrapping_add_signed(delta);
                    if offsets[position].1 == 0 {
                        offsets.swap_remove(position);
                    }
                }
                None => offsets.push((*offset, 1)),
            },
        );
        self.token_owners =
            self.token_owners + usize::from(new.token_owners) - usize::from(old.token_owners);
        for (set, old, new) in [
            (&mut self.accounts_any, old.accounts_any, new.accounts_any),
            (
                &mut self.transactions_any,
                old.transactions_any,
                new.transactions_any,
            ),
            (&mut self.entries, old.entries, new.entries),
            (
                &mut self.update_parent,
                old.update_parent,
                new.update_parent,
            ),
            (&mut self.slots, old.slots, new.slots),
            (&mut self.blocks, old.blocks, new.blocks),
            (&mut self.blocks_meta, old.blocks_meta, new.blocks_meta),
            (&mut self.block_footer, old.block_footer, new.block_footer),
        ] {
            match (old, new) {
                (false, true) => set.insert(slot),
                (true, false) => set.remove(slot),
                _ => {}
            }
        }
    }

    /// Walks two sorted key lists together and updates only the keys in one of them. Calls
    /// `on_key` with +1 when a key enters the map and -1 when it leaves it.
    fn replace_keys<K: Copy + Ord + std::hash::Hash>(
        map: &mut FoldHashMap<K, RouteSlots>,
        slot: u32,
        old: &[K],
        new: &[K],
        mut on_key: impl FnMut(&K, isize),
    ) {
        let (mut old, mut new) = (old.iter().peekable(), new.iter().peekable());
        loop {
            match (old.peek(), new.peek()) {
                (Some(o), Some(n)) if o == n => {
                    old.next();
                    new.next();
                }
                (Some(o), next) if next.is_none_or(|n| o < n) => {
                    if let Some(slots) = map.get_mut(*o) {
                        slots.retain(|other| *other != slot);
                        if slots.is_empty() {
                            map.remove(*o);
                            on_key(o, -1);
                        }
                    }
                    old.next();
                }
                (_, Some(n)) => {
                    let slots = map.entry(**n).or_default();
                    if slots.is_empty() {
                        on_key(n, 1);
                    }
                    slots.push(slot);
                    new.next();
                }
                (_, None) => break,
            }
        }
    }

    /// Sets `out` to the routes whose filters may match `message`.
    fn candidates(&self, message: &Message, out: &mut RouteSet) {
        fn add(out: &mut RouteSet, slots: Option<&RouteSlots>) {
            for slot in slots.into_iter().flatten() {
                out.insert(*slot);
            }
        }

        out.clear();
        match message {
            Message::Account(message) => {
                out.union(&self.accounts_any);
                add(out, self.accounts.get(&message.account.pubkey));
                add(out, self.owners.get(&message.account.owner));
                let data = &message.account.data;
                for (offset, _) in &self.data_offsets {
                    let key = offset
                        .checked_add(32)
                        .and_then(|end| data.get(*offset..end))
                        .and_then(|bytes| Pubkey::try_from(bytes).ok());
                    if let Some(key) = key {
                        add(out, self.data.get(&(*offset, key)));
                    }
                }
            }
            Message::Transaction(message) => {
                out.union(&self.transactions_any);
                if !self.transactions.is_empty() {
                    let transaction = &message.transaction;
                    for key in &transaction.account_keys {
                        add(out, self.transactions.get(key));
                    }
                    if self.token_owners > 0 {
                        for owner in token_balance_owners(transaction) {
                            add(out, self.transactions.get(owner));
                        }
                    }
                }
            }
            Message::Entry(_) => out.union(&self.entries),
            Message::EntryUpdateParent(_) => out.union(&self.update_parent),
            Message::Slot(_) => out.union(&self.slots),
            Message::Block(_) => out.union(&self.blocks),
            Message::BlockMeta(_) => out.union(&self.blocks_meta),
            Message::BlockFooter(_) => out.union(&self.block_footer),
            Message::DeshredTransaction(_) | Message::DeshredUpdateParent(_) => {}
        }
    }
}

/// Why a stream stopped getting updates from its route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteClosed {
    /// The stream's queue had no room for an update.
    QueueFull,
    /// The stream is gone.
    StreamClosed,
    /// The stream fell behind the raw ring.
    Lagged,
    /// The stream's filter panicked.
    FilterFailed,
    /// The router stopped, at shutdown.
    Shutdown,
}

/// What the router needs to filter for a stream.
#[derive(Clone)]
pub(crate) struct Indexed {
    filter: Arc<Filter>,
    keys: FilterKeys,
}

/// Unread batches of a stream that filters its own batches.
type RawReceiver = broadcast::Receiver<BroadcastedMessage>;

/// A change to one stream's route. With `live` unset, the stream reads the raw ring.
pub(crate) enum RouteChange {
    Add {
        stream: u64,
        stream_tx: SubscribeSender,
        metrics: SubscriberMetrics,
        closed: oneshot::Sender<RouteClosed>,
        live: Option<Indexed>,
        reply: oneshot::Sender<Option<RawReceiver>>,
    },
    /// A live route takes the new filter at once; a raw route stays raw until GoLive.
    Update {
        stream: u64,
        live: Option<Indexed>,
        reply: oneshot::Sender<Option<RawReceiver>>,
    },
    /// Filters the few unread raw batches into the stream and makes the route live. With more
    /// unread batches, the receiver comes back untouched.
    GoLive {
        stream: u64,
        raw: RawReceiver,
        live: Indexed,
        reply: oneshot::Sender<Option<RawReceiver>>,
    },
    Remove {
        stream: u64,
        done: Option<oneshot::Sender<()>>,
    },
}

/// An input of a router's owner.
pub(crate) enum RouterInput {
    Batch(BroadcastedMessage),
    Change(Box<RouteChange>),
}

/// One subscriber stream at one commitment level.
struct Route {
    stream_tx: SubscribeSender,
    metrics: SubscriberMetrics,
    closed: Option<oneshot::Sender<RouteClosed>>,
    /// Set while the router filters for the stream, unset while the stream reads the raw ring.
    live: Option<Indexed>,
}

/// Queues the updates `filter` yields for `message`. Returns whether it queued any. A filter
/// that panics fails only its own stream.
fn deliver(
    filter: &Filter,
    stream_tx: &SubscribeSender,
    metrics: &SubscriberMetrics,
    message: &Message,
    commitment: CommitmentLevel,
) -> Result<bool, RouteClosed> {
    let updates = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        filter.get_updates(message, Some(commitment))
    }))
    .map_err(|_| RouteClosed::FilterFailed)?;
    let queued = !updates.is_empty();
    for update in updates {
        match stream_tx.try_send(Ok(update)) {
            Ok(()) => metrics.incr_sent(),
            Err(TrySendError::Full(_)) => return Err(RouteClosed::QueueFull),
            Err(TrySendError::Closed(_)) => return Err(RouteClosed::StreamClosed),
        }
    }
    Ok(queued)
}

/// The routes of one commitment level. Only its owner touches it.
pub(crate) struct Router {
    commitment: CommitmentLevel,
    routes: Vec<Option<Route>>,
    slots: FoldHashMap<u64, u32>,
    free: Vec<u32>,
    index: Index,
    raw: broadcast::Sender<BroadcastedMessage>,
    candidates: RouteSet,
    delivered: RouteSet,
}

impl Router {
    fn new(commitment: CommitmentLevel, raw_capacity: usize) -> Self {
        Self {
            commitment,
            routes: Vec::new(),
            slots: FoldHashMap::default(),
            free: Vec::new(),
            index: Index::default(),
            raw: broadcast::channel(raw_capacity.max(1)).0,
            candidates: RouteSet::default(),
            delivered: RouteSet::default(),
        }
    }

    pub(crate) fn handle(&mut self, input: RouterInput) {
        match input {
            RouterInput::Batch(batch) => self.dispatch(&batch),
            RouterInput::Change(change) => self.change(*change),
        }
    }

    /// Sends `batch` to the raw ring and queues its updates on every live route they match.
    pub(crate) fn dispatch(&mut self, batch: &BroadcastedMessage) {
        let _ = self.raw.send(Arc::clone(batch));
        let mut candidates = std::mem::take(&mut self.candidates);
        let mut delivered = std::mem::take(&mut self.delivered);
        delivered.clear();
        let mut closed = Vec::new();
        let (mut examined, mut queued) = (0, 0);
        for message in batch.iter() {
            self.index.candidates(message, &mut candidates);
            for slot in candidates.iter() {
                examined += 1;
                let route = self.routes[slot as usize]
                    .as_mut()
                    .expect("indexed routes exist");
                let live = route.live.as_ref().expect("indexed routes are live");
                match deliver(
                    &live.filter,
                    &route.stream_tx,
                    &route.metrics,
                    message,
                    self.commitment,
                ) {
                    Ok(true) => {
                        queued += 1;
                        delivered.insert(slot)
                    }
                    Ok(false) => {}
                    Err(reason) => {
                        // Leaves the index now, so the rest of the batch skips it.
                        let live = route.live.take().expect("indexed routes are live");
                        self.index.remove(slot, &live.keys);
                        closed.push((slot, reason));
                    }
                }
            }
        }
        for slot in delivered.iter() {
            if let Some(route) = &self.routes[slot as usize] {
                route
                    .metrics
                    .observe_queue_size(route.stream_tx.current_weight() as u64);
            }
        }
        self.candidates = candidates;
        self.delivered = delivered;
        if trace::ENABLED && self.commitment == CommitmentLevel::Processed {
            trace::record(trace::Stage::RouteCandidates, examined);
            trace::record(trace::Stage::RouteDeliveries, queued);
        }
        for (slot, reason) in closed {
            self.close(slot, reason);
        }
    }

    fn change(&mut self, change: RouteChange) {
        match change {
            RouteChange::Add {
                stream,
                stream_tx,
                metrics,
                closed,
                live,
                reply,
            } => {
                let slot = self.free.pop().unwrap_or_else(|| {
                    self.routes.push(None);
                    u32::try_from(self.routes.len() - 1).expect("fewer than 2^32 routes")
                });
                self.slots.insert(stream, slot);
                self.routes[slot as usize] = Some(Route {
                    stream_tx,
                    metrics,
                    closed: Some(closed),
                    live: None,
                });
                let raw = match live {
                    Some(live) => {
                        self.index.insert(slot, &live.keys);
                        self.route(slot).live = Some(live);
                        None
                    }
                    None => Some(self.raw.subscribe()),
                };
                let _ = reply.send(raw);
            }
            RouteChange::Update {
                stream,
                live,
                reply,
            } => {
                let Some(&slot) = self.slots.get(&stream) else {
                    return;
                };
                let raw = match (self.route(slot).live.take(), live) {
                    (Some(old), Some(new)) => {
                        self.index.replace(slot, &old.keys, &new.keys);
                        self.route(slot).live = Some(new);
                        None
                    }
                    (Some(old), None) => {
                        self.index.remove(slot, &old.keys);
                        Some(self.raw.subscribe())
                    }
                    (None, _) => None,
                };
                let _ = reply.send(raw);
            }
            RouteChange::GoLive {
                stream,
                mut raw,
                live,
                reply,
            } => {
                let Some(&slot) = self.slots.get(&stream) else {
                    return;
                };
                if raw.len() > MAX_GO_LIVE_BACKLOG {
                    let _ = reply.send(Some(raw));
                    return;
                }
                match self.drain(slot, &mut raw, &live.filter) {
                    Ok(()) if self.route(slot).closed.is_some() => {
                        self.index.insert(slot, &live.keys);
                        self.route(slot).live = Some(live);
                    }
                    Ok(()) => {}
                    Err(reason) => self.close(slot, reason),
                }
                let _ = reply.send(None);
            }
            RouteChange::Remove { stream, done } => {
                if let Some(slot) = self.slots.remove(&stream) {
                    if let Some(live) = self.routes[slot as usize].take().and_then(|r| r.live) {
                        self.index.remove(slot, &live.keys);
                    }
                    self.free.push(slot);
                }
                if let Some(done) = done {
                    let _ = done.send(());
                }
            }
        }
    }

    fn route(&mut self, slot: u32) -> &mut Route {
        self.routes[slot as usize].as_mut().expect("route exists")
    }

    /// Filters the unread raw batches of a stream that goes live into its queue.
    fn drain(
        &mut self,
        slot: u32,
        raw: &mut RawReceiver,
        filter: &Filter,
    ) -> Result<(), RouteClosed> {
        let route = self.routes[slot as usize].as_ref().expect("route exists");
        loop {
            let batch = match raw.try_recv() {
                Ok(batch) => batch,
                Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
                Err(broadcast::error::TryRecvError::Lagged(_)) => return Err(RouteClosed::Lagged),
                Err(broadcast::error::TryRecvError::Closed) => return Err(RouteClosed::Shutdown),
            };
            for message in batch.iter() {
                deliver(
                    filter,
                    &route.stream_tx,
                    &route.metrics,
                    message,
                    self.commitment,
                )?;
            }
        }
    }

    /// Stops routing to `slot` and tells its client loop why.
    fn close(&mut self, slot: u32, reason: RouteClosed) {
        let route = self.route(slot);
        let live = route.live.take();
        if let Some(closed) = route.closed.take() {
            let _ = closed.send(reason);
        }
        if let Some(live) = live {
            self.index.remove(slot, &live.keys);
        }
    }
}

/// A router and the channel its owner reads.
pub(crate) struct RouterInputs {
    pub(crate) router: Router,
    pub(crate) inputs: mpsc::UnboundedReceiver<RouterInput>,
}

impl RouterInputs {
    fn new(
        commitment: CommitmentLevel,
        raw_capacity: usize,
    ) -> (Self, mpsc::UnboundedSender<RouterInput>) {
        let (tx, inputs) = mpsc::unbounded_channel();
        let router = Router::new(commitment, raw_capacity);
        (Self { router, inputs }, tx)
    }

    /// Runs the router on a task of its own.
    pub(crate) fn spawn(mut self) {
        tokio::spawn(async move {
            while let Some(input) = self.inputs.recv().await {
                let traced = trace::ENABLED && matches!(input, RouterInput::Batch(_));
                let start = trace::mono_ns();
                self.router.handle(input);
                if traced {
                    trace::record(trace::Stage::CommittedRouteBatch, trace::mono_ns() - start);
                }
            }
        });
    }
}

/// The input channels of the three routers. geyser_loop owns the processed router and routes
/// its own batches inline; a task owns each of the others, so their senders never wait.
#[derive(Clone)]
pub struct SubscriberChannels {
    inputs: [mpsc::UnboundedSender<RouterInput>; 3],
    next_stream: Arc<AtomicU64>,
}

impl fmt::Debug for SubscriberChannels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriberChannels").finish_non_exhaustive()
    }
}

impl SubscriberChannels {
    /// Each capacity bounds a level's raw ring, in batches. Spawns the confirmed and finalized
    /// routers and returns the processed one for its owner to run.
    pub(crate) fn new(
        processed: usize,
        confirmed: usize,
        finalized: usize,
    ) -> (Self, RouterInputs) {
        let (processed, processed_tx) = RouterInputs::new(CommitmentLevel::Processed, processed);
        let (confirmed, confirmed_tx) = RouterInputs::new(CommitmentLevel::Confirmed, confirmed);
        let (finalized, finalized_tx) = RouterInputs::new(CommitmentLevel::Finalized, finalized);
        confirmed.spawn();
        finalized.spawn();
        let channels = Self {
            inputs: [processed_tx, confirmed_tx, finalized_tx],
            next_stream: Arc::new(AtomicU64::new(0)),
        };
        (channels, processed)
    }

    const fn inputs(&self, commitment: CommitmentLevel) -> &mpsc::UnboundedSender<RouterInput> {
        match commitment {
            CommitmentLevel::Processed => &self.inputs[0],
            CommitmentLevel::Confirmed => &self.inputs[1],
            CommitmentLevel::Finalized => &self.inputs[2],
        }
    }

    pub fn send(&self, commitment: CommitmentLevel, messages: BroadcastedMessage) {
        let _ = self.inputs(commitment).send(RouterInput::Batch(messages));
    }

    /// Adds a route for one stream. With `catch_up`, or a filter the router cannot index, the
    /// stream reads the raw ring until [`RouteHandle::try_go_live`].
    pub async fn route(
        &self,
        commitment: CommitmentLevel,
        filter: Arc<Filter>,
        stream_tx: SubscribeSender,
        metrics: SubscriberMetrics,
        catch_up: bool,
    ) -> RouteHandle {
        let indexed = Indexed::new(filter);
        let inputs = self.inputs(commitment).clone();
        let stream = self.next_stream.fetch_add(1, Ordering::Relaxed);
        let (closed, closed_rx) = oneshot::channel();
        let (reply, reply_rx) = oneshot::channel();
        let _ = inputs.send(RouterInput::Change(Box::new(RouteChange::Add {
            stream,
            stream_tx,
            metrics,
            closed,
            live: indexed.clone().filter(|_| !catch_up),
            reply,
        })));
        RouteHandle {
            inputs,
            stream,
            commitment,
            indexed,
            raw: reply_rx.await.ok().flatten(),
            closed: Some(closed_rx),
            removed: false,
        }
    }
}

impl Indexed {
    /// The filter with its keys, if the router can index it.
    fn new(filter: Arc<Filter>) -> Option<Self> {
        if filter.keys_upper_bound() > MAX_INDEXED_KEYS {
            return None;
        }
        let keys = filter.keys();
        indexable(&keys).then_some(Self { filter, keys })
    }
}

/// A client loop's route. Dropping it removes the route.
pub struct RouteHandle {
    inputs: mpsc::UnboundedSender<RouterInput>,
    stream: u64,
    commitment: CommitmentLevel,
    /// The current filter with its keys, unless the router cannot index it.
    indexed: Option<Indexed>,
    raw: Option<RawReceiver>,
    closed: Option<oneshot::Receiver<RouteClosed>>,
    removed: bool,
}

impl RouteHandle {
    pub const fn commitment(&self) -> CommitmentLevel {
        self.commitment
    }

    fn send(&self, change: RouteChange) {
        let _ = self.inputs.send(RouterInput::Change(Box::new(change)));
    }

    /// Replaces the filter. With `catch_up`, the stream reads the raw ring until
    /// [`Self::try_go_live`]; a stream that reads it already keeps reading it.
    pub async fn update(&mut self, filter: Arc<Filter>, catch_up: bool) {
        self.indexed = Indexed::new(filter);
        let (reply, reply_rx) = oneshot::channel();
        self.send(RouteChange::Update {
            stream: self.stream,
            live: self.indexed.clone().filter(|_| !catch_up),
            reply,
        });
        if let Ok(Some(raw)) = reply_rx.await {
            self.raw = Some(raw);
        }
    }

    /// Hands the stream back to the router once its raw backlog is read. Returns whether the
    /// router filters for the stream.
    pub async fn try_go_live(&mut self) -> bool {
        if self.raw.as_ref().is_some_and(|raw| !raw.is_empty()) {
            return false;
        }
        let Some(raw) = self.raw.take() else {
            return true;
        };
        let Some(live) = self.indexed.clone() else {
            self.raw = Some(raw);
            return false;
        };
        let (reply, reply_rx) = oneshot::channel();
        self.send(RouteChange::GoLive {
            stream: self.stream,
            raw,
            live,
            reply,
        });
        match reply_rx.await {
            Ok(None) => true,
            Ok(Some(raw)) => {
                self.raw = Some(raw);
                false
            }
            Err(_) => false,
        }
    }

    /// The next raw batch while the stream reads the raw ring; pending while the router
    /// filters for it. Fails once the stream cannot get more updates from this route.
    pub async fn recv(&mut self) -> Result<BroadcastedMessage, RouteClosed> {
        let Some(closed) = self.closed.as_mut() else {
            return Err(RouteClosed::Shutdown);
        };
        let raw = self.raw.as_mut();
        let result = tokio::select! {
            reason = closed => Err(reason.unwrap_or(RouteClosed::Shutdown)),
            batch = async {
                match raw {
                    Some(raw) => raw.recv().await,
                    None => std::future::pending().await,
                }
            } => match batch {
                Ok(batch) => return Ok(batch),
                Err(broadcast::error::RecvError::Lagged(_)) => Err(RouteClosed::Lagged),
                Err(broadcast::error::RecvError::Closed) => Err(RouteClosed::Shutdown),
            },
        };
        self.closed = None;
        result
    }

    /// Removes the route and waits until the router has stopped sending to the stream.
    pub async fn close(mut self) {
        let (done, done_rx) = oneshot::channel();
        self.send(RouteChange::Remove {
            stream: self.stream,
            done: Some(done),
        });
        self.removed = true;
        let _ = done_rx.await;
    }
}

impl Drop for RouteHandle {
    fn drop(&mut self) {
        if !self.removed {
            self.send(RouteChange::Remove {
                stream: self.stream,
                done: None,
            });
        }
    }
}

#[cfg(test)]
impl SubscriberChannels {
    /// Channels whose processed router runs on a task of its own, for tests without geyser_loop.
    pub fn spawned(capacity: usize) -> Self {
        let (channels, processed) = Self::new(capacity, capacity, capacity);
        processed.spawn();
        channels
    }

    /// Every batch of `commitment`, unfiltered.
    pub async fn tap(&self, commitment: CommitmentLevel) -> RouteHandle {
        let (stream_tx, _) = crate::util::stream::load_aware_channel(
            1,
            crate::plugin::filter::message::WeightScales::default(),
        );
        self.route(
            commitment,
            Arc::new(Filter::default()),
            stream_tx,
            SubscriberMetrics::new("tap", "normal"),
            true,
        )
        .await
    }
}

#[cfg(test)]
impl RouteHandle {
    pub fn try_recv(&mut self) -> Option<BroadcastedMessage> {
        self.raw.as_mut()?.try_recv().ok()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            plugin::{
                filter::{
                    fixtures,
                    limits::FilterLimits,
                    message::{FilteredUpdate, WeightScales},
                },
                message::{MessageEntryUpdateParent, SlotStatus},
            },
            util::stream::{load_aware_channel, LoadAwareReceiver},
        },
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryUpdateParentInfo,
        futures::FutureExt,
        prost::Message as _,
        solana_hash::Hash,
        solana_signature::Signature,
        std::collections::{HashMap, HashSet},
        yellowstone_grpc_proto::{
            geyser::{
                subscribe_request_filter_accounts_filter::Filter as AccountsFilterOneof,
                subscribe_request_filter_accounts_filter_lamports::Cmp as LamportsCmp,
                subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
                CuckooFilter as ProtoCuckooFilter, SubscribeRequest,
                SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
                SubscribeRequestFilterAccountsFilterLamports,
                SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlockFooter,
                SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
                SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
                SubscribeRequestFilterTransactions, TokenAccountExpansionControlFlag,
            },
            solana::storage::confirmed_block,
        },
    };

    type Stream = LoadAwareReceiver<tonic::Result<FilteredUpdate>>;

    fn filter(request: SubscribeRequest) -> Arc<Filter> {
        let mut names = fixtures::filter_names();
        Arc::new(Filter::new(&request, &FilterLimits::default(), &mut names).unwrap())
    }

    fn strings(keys: &[Pubkey]) -> Vec<String> {
        keys.iter().map(ToString::to_string).collect()
    }

    fn accounts(account: &[Pubkey], owner: &[Pubkey]) -> SubscribeRequest {
        SubscribeRequest {
            accounts: HashMap::from([(
                "accounts".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: strings(account),
                    owner: strings(owner),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }
    }

    fn transactions(filter: SubscribeRequestFilterTransactions) -> SubscribeRequest {
        SubscribeRequest {
            transactions: HashMap::from([("transactions".to_owned(), filter)]),
            ..Default::default()
        }
    }

    fn slots() -> Arc<Filter> {
        filter(SubscribeRequest {
            slots: HashMap::from([("slots".to_owned(), SubscribeRequestFilterSlots::default())]),
            ..Default::default()
        })
    }

    fn cuckoo(pubkeys: &[Pubkey]) -> ProtoCuckooFilter {
        let mut filter =
            yellowstone_grpc_proto::cuckoo::CuckooFilter::<[u8; 32]>::with_capacity(4).unwrap();
        for pubkey in pubkeys {
            filter.insert(&pubkey.to_bytes()).unwrap();
        }
        ProtoCuckooFilter::from(&filter)
    }

    fn token_balance(owner: Pubkey, index: u32, amount: &str) -> confirmed_block::TokenBalance {
        confirmed_block::TokenBalance {
            account_index: index,
            mint: Pubkey::new_unique().to_string(),
            ui_token_amount: Some(confirmed_block::UiTokenAmount {
                amount: amount.to_owned(),
                ..Default::default()
            }),
            owner: owner.to_string(),
            program_id: spl_token_2022_interface::id().to_string(),
        }
    }

    fn slot_batch(slot: u64) -> BroadcastedMessage {
        Arc::new(vec![Message::Slot(Arc::new(fixtures::message_slot(
            slot,
            SlotStatus::Processed,
        )))])
    }

    fn drain(stream: &mut Stream) -> Vec<Vec<u8>> {
        let mut updates = Vec::new();
        while let Some(Some(update)) = stream.recv().now_or_never() {
            updates.push(update.unwrap().encode_to_vec());
        }
        updates
    }

    /// A route added straight to `router`, as its owner would apply it.
    struct TestRoute {
        stream: Stream,
        raw: Option<RawReceiver>,
        closed: oneshot::Receiver<RouteClosed>,
    }

    fn add(
        router: &mut Router,
        stream: u64,
        filter: Arc<Filter>,
        catch_up: bool,
        capacity: usize,
    ) -> TestRoute {
        let (stream_tx, stream_rx) = load_aware_channel(capacity, WeightScales::default());
        let (closed, closed_rx) = oneshot::channel();
        let (reply, mut reply_rx) = oneshot::channel();
        router.change(RouteChange::Add {
            stream,
            stream_tx,
            metrics: SubscriberMetrics::new("test", "normal"),
            closed,
            live: Indexed::new(filter).filter(|_| !catch_up),
            reply,
        });
        TestRoute {
            stream: stream_rx,
            raw: reply_rx.try_recv().expect("the router replies at once"),
            closed: closed_rx,
        }
    }

    fn go_live(
        router: &mut Router,
        stream: u64,
        raw: RawReceiver,
        filter: Arc<Filter>,
    ) -> Option<RawReceiver> {
        let (reply, mut reply_rx) = oneshot::channel();
        router.change(RouteChange::GoLive {
            stream,
            raw,
            live: Indexed::new(filter).expect("indexable"),
            reply,
        });
        reply_rx.try_recv().expect("the router replies at once")
    }

    #[test]
    fn routes_exactly_the_updates_each_filter_yields() {
        let pubkeys = fixtures::deterministic_pubkeys(1, 8);
        let owners = fixtures::deterministic_pubkeys(2, 4);
        let tx_keys = fixtures::deterministic_pubkeys(3, 6);
        let token_owner = Pubkey::new_unique();
        let filters = vec![
            filter(accounts(&pubkeys[..1], &[])),
            filter(accounts(&[], &owners[..1])),
            filter(accounts(&pubkeys[1..2], &owners[1..2])),
            filter(SubscribeRequest {
                accounts: HashMap::from([(
                    "cuckoo".to_owned(),
                    SubscribeRequestFilterAccounts {
                        account: strings(&pubkeys[3..4]),
                        owner: strings(&owners[3..4]),
                        cuckoo_accounts_filter: Some(cuckoo(&pubkeys[2..3])),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }),
            filter(transactions(SubscribeRequestFilterTransactions {
                account_include: strings(&tx_keys[..1]),
                ..Default::default()
            })),
            filter(transactions(SubscribeRequestFilterTransactions {
                account_required: strings(&tx_keys[1..3]),
                ..Default::default()
            })),
            filter(transactions(SubscribeRequestFilterTransactions {
                account_include: strings(&[token_owner]),
                token_accounts: Some(TokenAccountExpansionControlFlag::All as i32),
                ..Default::default()
            })),
            filter(SubscribeRequest {
                transactions_status: HashMap::from([(
                    "status".to_owned(),
                    SubscribeRequestFilterTransactions {
                        account_include: strings(&tx_keys[1..2]),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }),
            filter(SubscribeRequest {
                entry: HashMap::from([(
                    "entries".to_owned(),
                    SubscribeRequestFilterEntry {
                        include_update_parent: Some(true),
                    },
                )]),
                slots: HashMap::from([(
                    "slots".to_owned(),
                    SubscribeRequestFilterSlots {
                        interslot_updates: Some(true),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }),
            filter(SubscribeRequest {
                blocks: HashMap::from([(
                    "blocks".to_owned(),
                    SubscribeRequestFilterBlocks {
                        account_include: strings(&tx_keys[..1]),
                        ..Default::default()
                    },
                )]),
                blocks_meta: HashMap::from([(
                    "meta".to_owned(),
                    SubscribeRequestFilterBlocksMeta::default(),
                )]),
                block_footer: HashMap::from([(
                    "footer".to_owned(),
                    SubscribeRequestFilterBlockFooter::default(),
                )]),
                ..Default::default()
            }),
            filter(SubscribeRequest::default()),
        ];

        let account = |pubkey, owner| {
            Message::Account(fixtures::message_account(fixtures::account_info(
                pubkey,
                owner,
                Vec::new(),
                1,
                None,
            )))
        };
        let transaction = |keys: Vec<Pubkey>, meta| {
            Message::Transaction(fixtures::message_transaction(
                Signature::from([keys.len() as u8; 64]),
                keys,
                false,
                meta,
            ))
        };
        let token_meta = confirmed_block::TransactionStatusMeta {
            pre_token_balances: vec![token_balance(token_owner, 1, "5")],
            post_token_balances: vec![token_balance(token_owner, 1, "5")],
            ..Default::default()
        };
        let hash = Hash::new_from_array([7; 32]);
        let batch = Arc::new(vec![
            account(pubkeys[0], owners[2]),
            account(pubkeys[5], owners[0]),
            account(pubkeys[1], owners[1]),
            account(pubkeys[1], owners[2]),
            account(pubkeys[2], owners[3]),
            account(pubkeys[3], owners[3]),
            account(pubkeys[7], owners[2]),
            transaction(vec![tx_keys[0], tx_keys[5]], Default::default()),
            transaction(vec![tx_keys[1], tx_keys[5]], Default::default()),
            transaction(vec![tx_keys[1], tx_keys[2]], Default::default()),
            transaction(vec![tx_keys[5]], token_meta),
            Message::Entry(fixtures::message_entry(100, 0)),
            Message::EntryUpdateParent(Arc::new(MessageEntryUpdateParent::from_geyser(
                &ReplicaEntryUpdateParentInfo {
                    slot: 100,
                    cleared_bank_id: 3,
                    parent_slot: 98,
                    parent_block_id: &hash,
                },
            ))),
            Message::Slot(Arc::new(fixtures::message_slot(
                100,
                SlotStatus::CreatedBank,
            ))),
            Message::Block(fixtures::message_block(
                100,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )),
            Message::BlockMeta(fixtures::message_block_meta(100)),
            Message::BlockFooter(fixtures::message_block_footer(100, 100)),
        ]);

        let mut router = Router::new(CommitmentLevel::Processed, 16);
        let mut routes: Vec<TestRoute> = filters
            .iter()
            .enumerate()
            .map(|(stream, filter)| {
                add(
                    &mut router,
                    stream as u64,
                    Arc::clone(filter),
                    false,
                    1 << 20,
                )
            })
            .collect();
        router.dispatch(&batch);

        for (index, (filter, route)) in filters.iter().zip(&mut routes).enumerate() {
            if let Some(raw) = route.raw.as_mut() {
                // Not indexed: the stream gets the raw batch and filters it on its own task.
                assert!(Indexed::new(Arc::clone(filter)).is_none(), "filter {index}");
                assert!(
                    Arc::ptr_eq(&raw.try_recv().unwrap(), &batch),
                    "filter {index}"
                );
                assert!(drain(&mut route.stream).is_empty(), "filter {index}");
                continue;
            }
            let expected: Vec<Vec<u8>> = batch
                .iter()
                .flat_map(|message| filter.get_updates(message, Some(CommitmentLevel::Processed)))
                .map(|update| update.encode_to_vec())
                .collect();
            assert_eq!(drain(&mut route.stream), expected, "filter {index}");
        }
    }

    #[test]
    fn index_leaves_out_filters_an_update_cannot_match() {
        let pubkeys = fixtures::deterministic_pubkeys(1, 3);
        let mut router = Router::new(CommitmentLevel::Processed, 16);
        for (stream, pubkey) in pubkeys.iter().enumerate() {
            add(
                &mut router,
                stream as u64,
                filter(accounts(&[*pubkey], &[])),
                false,
                16,
            );
        }
        let message = Message::Account(fixtures::simple_message_account(
            pubkeys[1],
            Pubkey::new_unique(),
        ));
        let mut candidates = RouteSet::default();
        router.index.candidates(&message, &mut candidates);
        assert_eq!(candidates.iter().collect::<Vec<_>>(), [1]);
    }

    #[test]
    fn filters_the_router_cannot_index_read_the_raw_ring() {
        let any_account = filter(SubscribeRequest {
            accounts: HashMap::from([(
                "all".to_owned(),
                SubscribeRequestFilterAccounts::default(),
            )]),
            ..Default::default()
        });
        let too_many_keys = filter(accounts(
            &fixtures::deterministic_pubkeys(1, MAX_INDEXED_KEYS + 1),
            &[],
        ));
        let mut router = Router::new(CommitmentLevel::Processed, 16);
        for (stream, filter) in [any_account, too_many_keys].into_iter().enumerate() {
            assert!(Indexed::new(Arc::clone(&filter)).is_none());
            let mut route = add(&mut router, stream as u64, filter, false, 16);
            router.dispatch(&slot_batch(1));
            assert_eq!(
                route.raw.as_mut().unwrap().try_recv().unwrap()[0].get_slot(),
                1
            );
            assert!(drain(&mut route.stream).is_empty());
        }
        assert!(
            router.index.accounts.is_empty() && router.index.accounts_any.iter().next().is_none()
        );
    }

    #[test]
    fn full_queue_closes_only_that_route() {
        let mut router = Router::new(CommitmentLevel::Processed, 16);
        let mut small = add(&mut router, 0, slots(), false, 1);
        let mut large = add(&mut router, 1, slots(), false, 16);
        for slot in 1..=3 {
            router.dispatch(&slot_batch(slot));
        }
        assert_eq!(drain(&mut small.stream).len(), 1);
        assert_eq!(drain(&mut large.stream).len(), 3);
        assert_eq!(small.closed.try_recv(), Ok(RouteClosed::QueueFull));
        assert!(large.closed.try_recv().is_err());
        assert!(router.index.slots.iter().eq([1]));
    }

    #[test]
    fn a_route_goes_live_after_its_raw_backlog() {
        let mut router = Router::new(CommitmentLevel::Processed, 1024);
        let mut route = add(&mut router, 0, slots(), true, 1 << 16);
        router.dispatch(&slot_batch(1));
        router.dispatch(&slot_batch(2));
        assert!(drain(&mut route.stream).is_empty());

        // The client loop takes slot 1 itself; the router filters slot 2 for it when it goes live.
        let mut raw = route.raw.take().unwrap();
        assert_eq!(raw.try_recv().unwrap()[0].get_slot(), 1);
        assert!(go_live(&mut router, 0, raw, slots()).is_none());
        assert_eq!(drain(&mut route.stream).len(), 1);
        router.dispatch(&slot_batch(3));
        assert_eq!(drain(&mut route.stream).len(), 1);
    }

    #[test]
    fn go_live_hands_back_a_long_backlog() {
        let mut router = Router::new(CommitmentLevel::Processed, 1024);
        let mut route = add(&mut router, 0, slots(), true, 1024);
        for slot in 0..=MAX_GO_LIVE_BACKLOG as u64 {
            router.dispatch(&slot_batch(slot));
        }
        let raw = go_live(&mut router, 0, route.raw.take().unwrap(), slots());
        let mut raw = raw.expect("too many batches to filter at once");
        assert!(drain(&mut route.stream).is_empty());
        assert_eq!(raw.len(), MAX_GO_LIVE_BACKLOG + 1);

        raw.try_recv().unwrap();
        assert!(go_live(&mut router, 0, raw, slots()).is_none());
        assert_eq!(drain(&mut route.stream).len(), MAX_GO_LIVE_BACKLOG);
    }

    #[test]
    fn memcmp_keys_leave_out_accounts_of_the_same_owner() {
        let owner = Pubkey::new_unique();
        let mints = fixtures::deterministic_pubkeys(5, 2);
        let token_filter = |mint: &Pubkey| {
            filter(SubscribeRequest {
                accounts: HashMap::from([(
                    "mint".to_owned(),
                    SubscribeRequestFilterAccounts {
                        owner: strings(&[owner]),
                        filters: vec![SubscribeRequestFilterAccountsFilter {
                            filter: Some(AccountsFilterOneof::Memcmp(
                                SubscribeRequestFilterAccountsFilterMemcmp {
                                    offset: 0,
                                    data: Some(AccountsFilterMemcmpOneof::Bytes(
                                        mint.to_bytes().to_vec(),
                                    )),
                                },
                            )),
                        }],
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            })
        };
        let mut router = Router::new(CommitmentLevel::Processed, 16);
        let mut routes: Vec<TestRoute> = mints
            .iter()
            .enumerate()
            .map(|(stream, mint)| add(&mut router, stream as u64, token_filter(mint), false, 16))
            .collect();
        let mut data = mints[1].to_bytes().to_vec();
        data.extend([0; 133]);
        let message = Message::Account(fixtures::message_account(fixtures::account_info(
            Pubkey::new_unique(),
            owner,
            data,
            1,
            None,
        )));
        let mut candidates = RouteSet::default();
        router.index.candidates(&message, &mut candidates);
        assert_eq!(candidates.iter().collect::<Vec<_>>(), [1]);
        router.dispatch(&Arc::new(vec![message]));
        assert!(drain(&mut routes[0].stream).is_empty());
        assert_eq!(drain(&mut routes[1].stream).len(), 1);
    }

    #[test]
    fn memcmp_past_the_end_of_the_address_space_matches_nothing() {
        let overflowing = filter(SubscribeRequest {
            accounts: HashMap::from([(
                "far".to_owned(),
                SubscribeRequestFilterAccounts {
                    owner: strings(&[Pubkey::default()]),
                    filters: vec![SubscribeRequestFilterAccountsFilter {
                        filter: Some(AccountsFilterOneof::Memcmp(
                            SubscribeRequestFilterAccountsFilterMemcmp {
                                offset: u64::MAX,
                                data: Some(AccountsFilterMemcmpOneof::Bytes(vec![0; 32])),
                            },
                        )),
                    }],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        });
        let mut router = Router::new(CommitmentLevel::Processed, 16);
        let mut route = add(&mut router, 0, overflowing, false, 16);
        let account = Message::Account(fixtures::message_account(fixtures::account_info(
            Pubkey::new_unique(),
            Pubkey::default(),
            vec![0; 165],
            1,
            None,
        )));
        router.dispatch(&Arc::new(vec![account]));
        assert!(drain(&mut route.stream).is_empty());
        assert!(route.closed.try_recv().is_err());
    }

    #[test]
    fn blocks_and_token_owner_filters_read_the_raw_ring() {
        let blocks = filter(SubscribeRequest {
            blocks: HashMap::from([("blocks".to_owned(), SubscribeRequestFilterBlocks::default())]),
            ..Default::default()
        });
        let token_owners = filter(transactions(SubscribeRequestFilterTransactions {
            account_include: strings(&[Pubkey::new_unique()]),
            token_accounts: Some(TokenAccountExpansionControlFlag::All as i32),
            ..Default::default()
        }));
        for filter in [blocks, token_owners] {
            assert!(Indexed::new(filter).is_none());
        }
    }

    #[test]
    fn go_live_after_lagging_closes_the_route() {
        let mut router = Router::new(CommitmentLevel::Processed, 2);
        let mut route = add(&mut router, 0, slots(), true, 16);
        for slot in 0..3 {
            router.dispatch(&slot_batch(slot));
        }
        assert!(go_live(&mut router, 0, route.raw.take().unwrap(), slots()).is_none());
        assert_eq!(route.closed.try_recv(), Ok(RouteClosed::Lagged));
        router.dispatch(&slot_batch(6));
        assert!(drain(&mut route.stream).is_empty());
    }

    #[test]
    fn update_lists_only_the_changed_keys() {
        let pubkeys = fixtures::deterministic_pubkeys(1, 3);
        let mut router = Router::new(CommitmentLevel::Processed, 16);
        let mut route = add(
            &mut router,
            0,
            filter(accounts(&pubkeys[..2], &[])),
            false,
            16,
        );
        let (reply, mut reply_rx) = oneshot::channel();
        router.change(RouteChange::Update {
            stream: 0,
            live: Indexed::new(filter(accounts(&pubkeys[1..], &[]))),
            reply,
        });
        assert!(reply_rx.try_recv().unwrap().is_none());
        let listed: HashSet<Pubkey> = router.index.accounts.keys().copied().collect();
        assert_eq!(listed, HashSet::from([pubkeys[1], pubkeys[2]]));

        let account = |pubkey| {
            Arc::new(vec![Message::Account(fixtures::simple_message_account(
                pubkey,
                Pubkey::new_unique(),
            ))])
        };
        router.dispatch(&account(pubkeys[0]));
        assert!(drain(&mut route.stream).is_empty());
        router.dispatch(&account(pubkeys[2]));
        assert_eq!(drain(&mut route.stream).len(), 1);

        router.change(RouteChange::Remove {
            stream: 0,
            done: None,
        });
        assert!(router.index.accounts.is_empty());
        assert_eq!(router.free, [0]);
    }

    #[tokio::test]
    async fn a_stream_catches_up_through_the_channels() {
        let channels = SubscriberChannels::spawned(16);
        let (stream_tx, mut stream) = load_aware_channel(16, WeightScales::default());
        let mut route = channels
            .route(
                CommitmentLevel::Finalized,
                slots(),
                stream_tx,
                SubscriberMetrics::new("test", "normal"),
                true,
            )
            .await;
        channels.send(CommitmentLevel::Finalized, slot_batch(1));
        assert_eq!(route.recv().await.unwrap()[0].get_slot(), 1);
        assert!(route.try_go_live().await);
        channels.send(CommitmentLevel::Finalized, slot_batch(2));
        let update = tokio::time::timeout(std::time::Duration::from_secs(2), stream.recv())
            .await
            .unwrap();
        assert!(update.is_some());
        route.close().await;
    }

    /// Random filters and messages over a few keys, so most of them match something.
    mod random {
        use super::*;

        pub struct Rng(pub fastrand::Rng);

        impl Rng {
            pub fn chance(&mut self, p: f64) -> bool {
                self.0.f64() < p
            }

            pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
                &items[self.0.usize(..items.len())]
            }

            fn subset(&mut self, universe: &[Pubkey], max: usize) -> Vec<Pubkey> {
                let mut out: Vec<Pubkey> = (0..self.0.usize(..=max))
                    .map(|_| *self.pick(universe))
                    .collect();
                out.sort();
                out.dedup();
                out
            }

            fn opt_bool(&mut self) -> Option<bool> {
                match self.0.u8(..3) {
                    0 => None,
                    1 => Some(false),
                    _ => Some(true),
                }
            }
        }

        pub fn filter(rng: &mut Rng, universe: &[Pubkey]) -> Arc<Filter> {
            let mut request = SubscribeRequest::default();
            for n in 0..rng.0.usize(..=3) {
                let mut f = SubscribeRequestFilterAccounts {
                    account: strings(&rng.subset(universe, 3)),
                    owner: strings(&rng.subset(universe, 2)),
                    nonempty_txn_signature: rng.opt_bool(),
                    ..Default::default()
                };
                if rng.chance(0.25) {
                    f.cuckoo_accounts_filter = Some(cuckoo(&rng.subset(universe, 3)));
                }
                if rng.chance(0.3) {
                    f.filters.push(SubscribeRequestFilterAccountsFilter {
                        filter: Some(AccountsFilterOneof::Datasize(*rng.pick(&[0, 2, 64]))),
                    });
                }
                if rng.chance(0.4) {
                    let key = rng.pick(universe).to_bytes();
                    let len = *rng.pick(&[8, 32]);
                    f.filters.push(SubscribeRequestFilterAccountsFilter {
                        filter: Some(AccountsFilterOneof::Memcmp(
                            SubscribeRequestFilterAccountsFilterMemcmp {
                                offset: *rng.pick(&[0, 32]),
                                data: Some(AccountsFilterMemcmpOneof::Bytes(key[..len].to_vec())),
                            },
                        )),
                    });
                }
                if rng.chance(0.3) {
                    f.filters.push(SubscribeRequestFilterAccountsFilter {
                        filter: Some(AccountsFilterOneof::Lamports(
                            SubscribeRequestFilterAccountsFilterLamports {
                                cmp: Some(LamportsCmp::Gt(rng.0.u64(..3))),
                            },
                        )),
                    });
                }
                request.accounts.insert(format!("a{n}"), f);
            }
            for (map, prefix) in [
                (&mut request.transactions, "t"),
                (&mut request.transactions_status, "s"),
            ] {
                for n in 0..rng.0.usize(..=2) {
                    let mut f = SubscribeRequestFilterTransactions {
                        vote: rng.opt_bool(),
                        failed: rng.opt_bool(),
                        account_include: strings(&rng.subset(universe, 3)),
                        account_exclude: strings(&rng.subset(universe, 1)),
                        account_required: strings(&rng.subset(universe, 2)),
                        ..Default::default()
                    };
                    if rng.chance(0.1) {
                        f.signature = Some(Signature::from([rng.0.u8(1..3); 64]).to_string());
                    }
                    if rng.chance(0.2) {
                        f.cuckoo_account_include = Some(cuckoo(&rng.subset(universe, 2)));
                    }
                    f.token_accounts = match rng.0.u8(..3) {
                        0 => None,
                        1 => Some(TokenAccountExpansionControlFlag::All as i32),
                        _ => Some(TokenAccountExpansionControlFlag::BalanceChanged as i32),
                    };
                    map.insert(format!("{prefix}{n}"), f);
                }
            }
            if rng.chance(0.3) {
                request.entry.insert(
                    "e".into(),
                    SubscribeRequestFilterEntry {
                        include_update_parent: rng.opt_bool(),
                    },
                );
            }
            if rng.chance(0.3) {
                request.slots.insert(
                    "sl".into(),
                    SubscribeRequestFilterSlots {
                        filter_by_commitment: rng.opt_bool(),
                        interslot_updates: rng.opt_bool(),
                    },
                );
            }
            if rng.chance(0.2) {
                request.blocks.insert(
                    "b".into(),
                    SubscribeRequestFilterBlocks {
                        account_include: strings(&rng.subset(universe, 2)),
                        include_transactions: rng.opt_bool(),
                        include_accounts: rng.opt_bool(),
                        include_entries: rng.opt_bool(),
                        ..Default::default()
                    },
                );
            }
            if rng.chance(0.2) {
                request
                    .blocks_meta
                    .insert("m".into(), SubscribeRequestFilterBlocksMeta::default());
            }
            for n in 0..rng.0.usize(..=2) {
                request.block_footer.insert(
                    format!("f{n}"),
                    SubscribeRequestFilterBlockFooter {
                        include_certificates: rng.opt_bool(),
                    },
                );
            }
            super::filter(request)
        }

        pub fn messages(rng: &mut Rng, universe: &[Pubkey]) -> Vec<Message> {
            let mut out = Vec::new();
            for _ in 0..60 {
                let data = if rng.chance(0.5) {
                    [rng.pick(universe).to_bytes(), rng.pick(universe).to_bytes()].concat()
                } else {
                    vec![0u8; rng.0.usize(..3)]
                };
                let signature = rng.chance(0.5).then(|| Signature::from([5; 64]));
                out.push(Message::Account(fixtures::message_account(
                    fixtures::account_info(
                        *rng.pick(universe),
                        *rng.pick(universe),
                        data,
                        rng.0.u64(..4),
                        signature,
                    ),
                )));
            }
            for i in 0..80 {
                let mut keys = rng.subset(universe, 3);
                if keys.is_empty() {
                    keys.push(*rng.pick(universe));
                }
                let mut meta = confirmed_block::TransactionStatusMeta::default();
                if rng.chance(0.3) {
                    meta.err = Some(Default::default());
                }
                for _ in 0..rng.0.usize(..3) {
                    let index = rng.0.u32(..4);
                    let owner = *rng.pick(universe);
                    match rng.0.u8(..3) {
                        0 => meta
                            .pre_token_balances
                            .push(token_balance(owner, index, "5")),
                        1 => meta
                            .post_token_balances
                            .push(token_balance(owner, index, "6")),
                        _ => {
                            meta.pre_token_balances
                                .push(token_balance(owner, index, "5"));
                            meta.post_token_balances
                                .push(token_balance(owner, index, "5"));
                        }
                    }
                }
                let signature = if rng.chance(0.2) {
                    1
                } else {
                    10 + (i % 200) as u8
                };
                out.push(Message::Transaction(fixtures::message_transaction(
                    Signature::from([signature; 64]),
                    keys,
                    rng.chance(0.3),
                    meta,
                )));
            }
            out.push(Message::Entry(fixtures::message_entry(100, 0)));
            let hash = Hash::new_from_array([7; 32]);
            out.push(Message::EntryUpdateParent(Arc::new(
                MessageEntryUpdateParent::from_geyser(&ReplicaEntryUpdateParentInfo {
                    slot: 100,
                    cleared_bank_id: 3,
                    parent_slot: 98,
                    parent_block_id: &hash,
                }),
            )));
            for status in [
                SlotStatus::Processed,
                SlotStatus::Confirmed,
                SlotStatus::Finalized,
                SlotStatus::FirstShredReceived,
                SlotStatus::Completed,
                SlotStatus::CreatedBank,
                SlotStatus::Dead,
            ] {
                out.push(Message::Slot(Arc::new(fixtures::message_slot(100, status))));
            }
            out.push(Message::Block(fixtures::message_block(
                100,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )));
            out.push(Message::BlockMeta(fixtures::message_block_meta(100)));
            out.push(Message::BlockFooter(fixtures::message_block_footer(
                100, 100,
            )));
            out
        }
    }

    /// Every filter is indexed here, including those the router leaves to the raw ring, so the
    /// keys of every filter shape are checked.
    #[test]
    fn candidates_cover_every_matching_filter() {
        let universe = fixtures::deterministic_pubkeys(42, 6);
        let mut checked = 0usize;
        for seed in 0..40u64 {
            let mut rng = random::Rng(fastrand::Rng::with_seed(seed));
            let mut index = Index::default();
            let filters: Vec<Arc<Filter>> = (0..150)
                .map(|slot| {
                    let filter = random::filter(&mut rng, &universe);
                    index.insert(slot, &filter.keys());
                    filter
                })
                .collect();
            let mut candidates = RouteSet::default();
            for message in random::messages(&mut rng, &universe) {
                index.candidates(&message, &mut candidates);
                let candidates: HashSet<u32> = candidates.iter().collect();
                for commitment in [
                    CommitmentLevel::Processed,
                    CommitmentLevel::Confirmed,
                    CommitmentLevel::Finalized,
                ] {
                    for (slot, filter) in filters.iter().enumerate() {
                        if !filter.get_updates(&message, Some(commitment)).is_empty() {
                            checked += 1;
                            assert!(
                                candidates.contains(&(slot as u32)),
                                "seed {seed}: filter {slot} matches {message:?} but is no \
                                 candidate; keys {:?}",
                                filter.keys()
                            );
                        }
                    }
                }
            }
        }
        assert!(checked > 10_000, "only {checked} matches checked");
    }

    type Listed = (Vec<(Pubkey, Vec<u32>)>, usize, Vec<Vec<u32>>);

    /// The index in a canonical form, so two indexes compare whatever their insertion order.
    fn listed(index: &Index) -> Listed {
        let mut keys: Vec<(Pubkey, Vec<u32>)> = Vec::new();
        for (tag, map) in [&index.accounts, &index.owners, &index.transactions]
            .into_iter()
            .enumerate()
        {
            for (key, slots) in map {
                let mut slots = slots.to_vec();
                slots.sort_unstable();
                slots.push(u32::MAX - tag as u32);
                keys.push((*key, slots));
            }
        }
        keys.sort_unstable();
        let sets = [
            &index.accounts_any,
            &index.transactions_any,
            &index.entries,
            &index.update_parent,
            &index.slots,
            &index.blocks,
            &index.blocks_meta,
            &index.block_footer,
        ]
        .map(|set| set.iter().collect())
        .to_vec();
        (keys, index.token_owners, sets)
    }

    #[test]
    fn index_matches_a_rebuild_after_random_changes() {
        let universe = fixtures::deterministic_pubkeys(43, 6);
        for seed in 0..12u64 {
            let mut rng = random::Rng(fastrand::Rng::with_seed(1000 + seed));
            let mut router = Router::new(CommitmentLevel::Processed, 1 << 16);
            let mut routes: HashMap<u64, TestRoute> = HashMap::new();
            let mut next_stream = 0;
            for step in 0..400 {
                let streams: Vec<u64> = routes.keys().copied().collect();
                let op = rng.0.u8(..6);
                match op {
                    0 | 1 => {
                        let filter = random::filter(&mut rng, &universe);
                        let catch_up = rng.chance(0.3);
                        let route = add(&mut router, next_stream, filter, catch_up, 1 << 20);
                        routes.insert(next_stream, route);
                        next_stream += 1;
                    }
                    2 if !streams.is_empty() => {
                        let stream = *rng.pick(&streams);
                        let catch_up = rng.chance(0.3);
                        let live =
                            Indexed::new(random::filter(&mut rng, &universe)).filter(|_| !catch_up);
                        let (reply, mut reply_rx) = oneshot::channel();
                        router.change(RouteChange::Update {
                            stream,
                            live,
                            reply,
                        });
                        if let Some(raw) = reply_rx.try_recv().unwrap() {
                            routes.get_mut(&stream).unwrap().raw = Some(raw);
                        }
                    }
                    3 if !streams.is_empty() => {
                        let stream = *rng.pick(&streams);
                        let filter = random::filter(&mut rng, &universe);
                        let route = routes.get_mut(&stream).unwrap();
                        if let (Some(raw), Some(live)) = (route.raw.take(), Indexed::new(filter)) {
                            let (reply, mut reply_rx) = oneshot::channel();
                            router.change(RouteChange::GoLive {
                                stream,
                                raw,
                                live,
                                reply,
                            });
                            route.raw = reply_rx.try_recv().unwrap();
                        }
                    }
                    4 if !streams.is_empty() => {
                        router.dispatch(&Arc::new(random::messages(&mut rng, &universe)));
                    }
                    5 if !streams.is_empty() => {
                        let stream = *rng.pick(&streams);
                        routes.remove(&stream);
                        router.change(RouteChange::Remove { stream, done: None });
                    }
                    _ => {}
                }
                let mut rebuilt = Index::default();
                for (slot, route) in router.routes.iter().enumerate() {
                    if let Some(live) = route.as_ref().and_then(|route| route.live.as_ref()) {
                        rebuilt.insert(slot as u32, &live.keys);
                    }
                }
                assert_eq!(
                    listed(&router.index),
                    listed(&rebuilt),
                    "seed {seed} step {step} op {op}"
                );
                assert_eq!(router.slots.len(), routes.len());
            }
        }
    }
}
