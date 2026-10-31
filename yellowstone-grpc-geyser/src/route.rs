//! Routes updates straight into the queues of the clients whose filter keys find them.
//! Each level's only sender owns its routes and applies route changes between two of its sends.

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
    prometheus::IntCounter,
    solana_pubkey::Pubkey,
    std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{Arc, OnceLock},
    },
    tokio::sync::{
        broadcast::{self, error::TryRecvError},
        mpsc, oneshot,
    },
    tokio_util::sync::CancellationToken,
    tonic::Status,
};

pub const MAX_ROUTE_KEYS: usize = 8192;
pub const MEMCMP_OFFSETS: [usize; 2] = [0, 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RouteKey {
    Account(Pubkey),
    Owner(Pubkey),
    Memcmp(usize, [u8; 32]),
    Transaction(Pubkey),
    Slots,
    Entries,
    UpdateParent,
    BlocksMeta,
    BlockFooter,
}

type Receiver = broadcast::Receiver<BroadcastedMessage>;

pub enum RouteChange {
    Join(Route, Option<Receiver>),
    Leave(usize, Option<(CommitmentLevel, oneshot::Sender<Receiver>)>),
}

pub struct LeaveOnDrop(pub usize, pub [mpsc::UnboundedSender<RouteChange>; 2]);

impl Drop for LeaveOnDrop {
    fn drop(&mut self) {
        for owner in &self.1 {
            let _ = owner.send(RouteChange::Leave(self.0, None));
        }
    }
}

enum Failure {
    Full,
    Closed,
    Panicked,
    Lagged,
}

#[derive(Clone)]
pub struct Route {
    id: usize,
    filter: Arc<Filter>,
    keys: Arc<[RouteKey]>,
    stream_tx: SubscribeSender,
    token: CancellationToken,
    disconnect_reason: Arc<OnceLock<&'static str>>,
    sent: IntCounter,
}

impl Route {
    pub fn new(
        id: usize,
        subscriber_id: &str,
        stream_tx: &SubscribeSender,
        token: &CancellationToken,
    ) -> Self {
        Self {
            id,
            filter: Arc::default(),
            keys: Arc::default(),
            stream_tx: stream_tx.clone(),
            token: token.clone(),
            disconnect_reason: Arc::default(),
            sent: metrics::grpc_message_sent_counter(subscriber_id),
        }
    }

    pub fn with_filter(&self, filter: &Arc<Filter>, keys: &Arc<[RouteKey]>) -> Self {
        Self {
            filter: Arc::clone(filter),
            keys: Arc::clone(keys),
            ..self.clone()
        }
    }

    pub fn disconnect_reason(&self) -> Option<&'static str> {
        self.disconnect_reason.get().copied()
    }

    fn level(&self) -> CommitmentLevel {
        self.filter.get_commitment_level()
    }

    fn deliver(&self, message: &Message) -> Result<(), Failure> {
        let updates = catch_unwind(AssertUnwindSafe(|| {
            self.filter.get_updates(message, Some(self.level()))
        }))
        .map_err(|_| Failure::Panicked)?;
        for update in updates {
            match self.stream_tx.try_send(Ok(update)) {
                Ok(()) => self.sent.inc(),
                Err(TrySendError::Full(_)) => return Err(Failure::Full),
                Err(TrySendError::Closed(_)) => return Err(Failure::Closed),
            }
        }
        Ok(())
    }

    fn close(&self, failure: Failure) {
        let (reason, status) = match failure {
            Failure::Full => ("client_channel_full", Some("lagged to send an update")),
            Failure::Closed => ("client_closed", None),
            Failure::Panicked => ("filter_failed", Some("failed to apply the filter")),
            Failure::Lagged => (
                "client_broadcast_lag",
                Some("lagged to receive geyser messages"),
            ),
        };
        if self.disconnect_reason.set(reason).is_ok() {
            if let Some(status) = status {
                self.stream_tx.close(Err(Status::internal(status)));
            }
            self.token.cancel();
        }
    }
}

#[derive(Default)]
struct Router {
    index: HashMap<RouteKey, Vec<usize>>,
    routes: HashMap<usize, (Route, u64)>,
    stamp: u64,
}

impl Router {
    fn insert(&mut self, route: Route) {
        let routed = self.routes.get_mut(&route.id);
        if let Some((routed, _)) = routed.filter(|(routed, _)| routed.keys == route.keys) {
            *routed = route;
            return;
        }
        self.remove(route.id);
        if route.keys.is_empty() {
            return;
        }
        for key in route.keys.iter() {
            self.index.entry(*key).or_default().push(route.id);
        }
        self.routes.insert(route.id, (route, 0));
    }

    fn remove(&mut self, id: usize) {
        let Some((route, _)) = self.routes.remove(&id) else {
            return;
        };
        for key in route.keys.iter() {
            if let Some(ids) = self.index.get_mut(key) {
                ids.retain(|other| *other != id);
                if ids.is_empty() {
                    self.index.remove(key);
                }
            }
        }
    }

    fn route(&mut self, batch: &[Message]) {
        if self.index.is_empty() {
            return;
        }
        let mut candidates = Vec::new();
        for message in batch {
            self.stamp += 1;
            self.candidates(message, &mut candidates);
            for id in candidates.drain(..) {
                let Some((route, seen)) = self.routes.get_mut(&id) else {
                    continue;
                };
                if *seen == self.stamp {
                    continue;
                }
                *seen = self.stamp;
                if let Err(failure) = route.deliver(message) {
                    route.close(failure);
                    self.remove(id);
                }
            }
        }
    }

    fn candidates(&self, message: &Message, out: &mut Vec<usize>) {
        let mut add = |key| {
            if let Some(ids) = self.index.get(&key) {
                out.extend_from_slice(ids);
            }
        };
        match message {
            Message::Account(account) => {
                let account = &account.account;
                add(RouteKey::Account(account.pubkey));
                add(RouteKey::Owner(account.owner));
                for offset in MEMCMP_OFFSETS {
                    if let Some(bytes) = account.data.get(offset..).and_then(<[u8]>::first_chunk) {
                        add(RouteKey::Memcmp(offset, *bytes));
                    }
                }
            }
            Message::Transaction(transaction) => {
                for key in &transaction.transaction.account_keys {
                    add(RouteKey::Transaction(*key));
                }
            }
            Message::Slot(_) => add(RouteKey::Slots),
            Message::Entry(_) => add(RouteKey::Entries),
            Message::EntryUpdateParent(_) => add(RouteKey::UpdateParent),
            Message::BlockMeta(_) => add(RouteKey::BlocksMeta),
            Message::BlockFooter(_) => add(RouteKey::BlockFooter),
            Message::Block(_)
            | Message::DeshredTransaction(_)
            | Message::DeshredUpdateParent(_) => {}
        }
    }
}

pub struct Routes {
    broadcast: SubscriberChannels,
    routers: [Router; 3],
}

impl Routes {
    pub fn new(broadcast: SubscriberChannels) -> Self {
        let routers = Default::default();
        Self { broadcast, routers }
    }

    pub fn send(&mut self, level: CommitmentLevel, batch: BroadcastedMessage) {
        self.routers[level as usize].route(&batch);
        self.broadcast.send(level, batch);
    }

    pub fn apply(&mut self, change: RouteChange) {
        match change {
            RouteChange::Join(route, rx) => match rx.map_or(Ok(()), |rx| drain(rx, &route)) {
                Ok(()) => self.routers[route.level() as usize].insert(route),
                Err(failure) => route.close(failure),
            },
            RouteChange::Leave(id, answer) => {
                for router in &mut self.routers {
                    router.remove(id);
                }
                if let Some((level, answer)) = answer {
                    let _ = answer.send(self.broadcast.subscribe(level));
                }
            }
        }
    }
}

fn drain(mut rx: Receiver, route: &Route) -> Result<(), Failure> {
    loop {
        match rx.try_recv() {
            Ok(batch) => batch
                .iter()
                .try_for_each(|message| route.deliver(message))?,
            Err(TryRecvError::Empty | TryRecvError::Closed) => return Ok(()),
            Err(TryRecvError::Lagged(_)) => return Err(Failure::Lagged),
        }
    }
}

#[cfg(test)]
mod tests {
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
        yellowstone_grpc_proto::{
            cuckoo::CuckooFilter,
            geyser::{
                subscribe_request_filter_accounts_filter::Filter as AccountsFilterOneof,
                subscribe_request_filter_accounts_filter_memcmp::Data as MemcmpData,
                CuckooFilter as ProtoCuckooFilter, SubscribeRequest,
                SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
                SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlockFooter,
                SubscribeRequestFilterEntry, SubscribeRequestFilterTransactions,
            },
        },
    };

    const NEEDLE: [u8; 32] = [7; 32];

    type Queue = LoadAwareReceiver<tonic::Result<FilteredUpdate>>;
    type Items = Vec<Result<FilteredUpdate, String>>;

    fn refilter(route: &Route, request: &SubscribeRequest) -> Route {
        let filter = Arc::new(fixtures::filter(request));
        let keys = filter.route_keys().expect("an indexable filter");
        route.with_filter(&filter, &keys.into())
    }

    fn client(id: usize, request: &SubscribeRequest, capacity: usize) -> (Route, Queue) {
        let (stream_tx, queue) = load_aware_channel(capacity, Default::default());
        let route = Route::new(id, "test", &stream_tx, &CancellationToken::new());
        (refilter(&route, request), queue)
    }

    fn received(queue: &mut Queue) -> Items {
        std::iter::from_fn(|| queue.recv().now_or_never().flatten())
            .map(|item| item.map_err(|status| status.message().to_owned()))
            .collect()
    }

    fn updates(route: &Route, messages: &[Message]) -> Items {
        let level = Some(route.level());
        messages
            .iter()
            .flat_map(|message| route.filter.get_updates(message, level))
            .map(Ok)
            .collect()
    }

    fn send(routes: &mut Routes, batch: &[Message]) {
        routes.send(Processed, Arc::new(batch.to_vec()));
    }

    fn entry(slot: u64) -> Message {
        Message::Entry(fixtures::message_entry(slot, 0))
    }

    fn entries() -> SubscribeRequest {
        SubscribeRequest {
            entry: [("e".to_owned(), Default::default())].into(),
            ..Default::default()
        }
    }

    fn accounts(
        account: &[Pubkey],
        owner: &[Pubkey],
        offset: Option<u64>,
        cuckoo_keys: &[Pubkey],
    ) -> SubscribeRequest {
        let filter = SubscribeRequestFilterAccounts {
            account: strings(account),
            owner: strings(owner),
            filters: offset.map(memcmp).into_iter().collect(),
            cuckoo_accounts_filter: (!cuckoo_keys.is_empty()).then(|| cuckoo(cuckoo_keys)),
            ..Default::default()
        };
        SubscribeRequest {
            accounts: [("a".to_owned(), filter)].into(),
            ..Default::default()
        }
    }

    fn transactions(
        include: &[Pubkey],
        required: &[Pubkey],
        token_accounts: Option<i32>,
    ) -> SubscribeRequest {
        let filter = SubscribeRequestFilterTransactions {
            account_include: strings(include),
            account_required: strings(required),
            token_accounts,
            ..Default::default()
        };
        SubscribeRequest {
            transactions: [("t".to_owned(), filter)].into(),
            ..Default::default()
        }
    }

    fn memcmp(offset: u64) -> SubscribeRequestFilterAccountsFilter {
        let memcmp = SubscribeRequestFilterAccountsFilterMemcmp {
            offset,
            data: Some(MemcmpData::Bytes(NEEDLE.to_vec())),
        };
        SubscribeRequestFilterAccountsFilter {
            filter: Some(AccountsFilterOneof::Memcmp(memcmp)),
        }
    }

    fn strings(keys: &[Pubkey]) -> Vec<String> {
        keys.iter().map(ToString::to_string).collect()
    }

    fn cuckoo(keys: &[Pubkey]) -> ProtoCuckooFilter {
        let mut filter = CuckooFilter::<[u8; 32]>::with_capacity(keys.len().max(1)).unwrap();
        for key in keys {
            filter.insert(&key.to_bytes()).unwrap();
        }
        ProtoCuckooFilter::from(&filter)
    }

    fn pick(rng: &mut fastrand::Rng, keys: &[Pubkey], max: usize) -> Vec<Pubkey> {
        (0..rng.usize(..=max))
            .map(|_| *rng.choice(keys).unwrap())
            .collect()
    }

    fn random_request(rng: &mut fastrand::Rng, keys: &[Pubkey]) -> SubscribeRequest {
        let mut request = SubscribeRequest::default();
        for name in 0..rng.usize(..3) {
            let filter = SubscribeRequestFilterAccounts {
                account: strings(&pick(rng, keys, 2)),
                owner: strings(&pick(rng, keys, 2)),
                filters: [None, Some(0), Some(32)][rng.usize(..3)]
                    .map(memcmp)
                    .into_iter()
                    .collect(),
                cuckoo_accounts_filter: (rng.u8(..4) == 0).then(|| cuckoo(&pick(rng, keys, 2))),
                ..Default::default()
            };
            request.accounts.insert(format!("a{name}"), filter);
        }
        for name in 0..rng.usize(..3) {
            let filter = SubscribeRequestFilterTransactions {
                account_include: strings(&pick(rng, keys, 3)),
                account_required: strings(&pick(rng, keys, 1)),
                cuckoo_account_include: (rng.u8(..4) == 0).then(|| cuckoo(&pick(rng, keys, 2))),
                ..Default::default()
            };
            let lists = [&mut request.transactions, &mut request.transactions_status];
            lists[rng.usize(..2)].insert(format!("t{name}"), filter);
        }
        if rng.bool() {
            request.slots.insert("s".into(), Default::default());
        }
        if rng.bool() {
            let filter = SubscribeRequestFilterEntry {
                include_update_parent: Some(rng.bool()),
            };
            request.entry.insert("e".into(), filter);
        }
        if rng.bool() {
            request.blocks_meta.insert("m".into(), Default::default());
        }
        if rng.bool() {
            let filter = SubscribeRequestFilterBlockFooter {
                include_certificates: Some(rng.bool()),
            };
            request.block_footer.insert("f".into(), filter);
        }
        request
    }

    fn random_message(rng: &mut fastrand::Rng, keys: &[Pubkey]) -> Message {
        let slot = 100;
        match rng.u8(..10) {
            0..=2 => {
                let halves = [[0; 32], NEEDLE];
                let mut data = [halves[rng.usize(..2)], halves[rng.usize(..2)]].concat();
                data.truncate([0, 40, 64][rng.usize(..3)]);
                let pubkey = *rng.choice(keys).unwrap();
                let owner = *rng.choice(keys).unwrap();
                let account = fixtures::account_info(pubkey, owner, data, 1, None);
                Message::Account(fixtures::message_account(account))
            }
            3 | 4 => Message::Transaction(fixtures::message_transaction(
                Signature::default(),
                pick(rng, keys, 4),
                false,
                Default::default(),
            )),
            5 => entry(slot),
            6 => Message::Slot(Arc::new(fixtures::message_slot(
                slot,
                SlotStatus::Processed,
            ))),
            7 => Message::BlockMeta(fixtures::message_block_meta(slot)),
            8 => Message::BlockFooter(fixtures::message_block_footer(slot, slot)),
            _ => Message::EntryUpdateParent(Arc::new(MessageEntryUpdateParent {
                update_parent: Default::default(),
                created_at: Default::default(),
            })),
        }
    }

    #[test]
    fn each_routed_client_gets_what_its_filter_makes_of_every_message() {
        let keys = fixtures::deterministic_pubkeys(1, 8);
        let mut routed = 0;
        for seed in 0..60 {
            let mut rng = fastrand::Rng::with_seed(seed);
            let mut routes = Routes::new(SubscriberChannels::new(1, 1, 1));
            let mut clients = Vec::new();
            for id in 0..16 {
                let request = random_request(&mut rng, &keys);
                if fixtures::filter(&request).route_keys().is_some() {
                    let (route, queue) = client(id, &request, MAX_CAPACITY);
                    routes.apply(RouteChange::Join(route.clone(), None));
                    clients.push((route, queue));
                }
            }
            routed += clients.len();
            let messages = (0..256)
                .map(|_| random_message(&mut rng, &keys))
                .collect::<Vec<_>>();
            for batch in messages.chunks(32) {
                send(&mut routes, batch);
            }
            for (route, queue) in &mut clients {
                assert_eq!(received(queue), updates(route, &messages), "seed {seed}");
            }
        }
        assert!(routed > 500, "{routed}");
    }

    #[test]
    fn indexes_only_filters_whose_keys_find_every_match() {
        let [key, owner] = [Pubkey::new_unique(), Pubkey::new_unique()];
        let many = (0..=MAX_ROUTE_KEYS)
            .map(|_| Pubkey::new_unique())
            .collect::<Vec<_>>();
        let blocks = SubscribeRequest {
            blocks: [("b".to_owned(), Default::default())].into(),
            ..Default::default()
        };
        let cases = [
            (
                accounts(&[key], &[owner], None, &[]),
                Some(vec![RouteKey::Account(key)]),
            ),
            (
                accounts(&[key], &[owner], None, &[key]),
                Some(vec![RouteKey::Owner(owner)]),
            ),
            (
                accounts(&[], &many, Some(32), &[]),
                Some(vec![RouteKey::Memcmp(32, NEEDLE)]),
            ),
            (
                accounts(&[], &[owner], Some(8), &[]),
                Some(vec![RouteKey::Owner(owner)]),
            ),
            (
                transactions(&[key], &many, None),
                Some(vec![RouteKey::Transaction(*many.iter().min().unwrap())]),
            ),
            (SubscribeRequest::default(), Some(vec![])),
            (accounts(&[], &[], None, &[key]), None),
            (accounts(&[], &[], None, &[]), None),
            (accounts(&many, &[], None, &[]), None),
            (transactions(&[key], &[], Some(0)), None),
            (transactions(&[], &[], None), None),
            (blocks, None),
        ];
        for (case, (request, keys)) in cases.into_iter().enumerate() {
            assert_eq!(fixtures::filter(&request).route_keys(), keys, "case {case}");
        }
    }

    #[test]
    fn a_join_and_a_leave_cut_the_stream_between_two_sends() {
        let mut routes = Routes::new(SubscriberChannels::new(16, 16, 16));
        let (route, mut queue) = client(1, &entries(), 16);
        let rx = routes.broadcast.subscribe(Processed);
        let batches = [1, 2, 3, 4].map(|slot| vec![entry(slot)]);
        send(&mut routes, &batches[0]);
        send(&mut routes, &batches[1]);
        routes.apply(RouteChange::Join(route.clone(), Some(rx)));
        send(&mut routes, &batches[2]);
        let (answer, mut answered) = oneshot::channel();
        routes.apply(RouteChange::Leave(1, Some((Processed, answer))));
        send(&mut routes, &batches[3]);
        assert_eq!(
            received(&mut queue),
            updates(&route, &batches[..3].concat())
        );
        let mut rx = answered.try_recv().unwrap();
        assert_eq!(rx.try_recv().unwrap(), Arc::new(batches[3].clone()));
        assert!(rx.is_empty());
    }

    #[test]
    fn a_join_whose_receiver_lagged_closes_the_client() {
        let mut routes = Routes::new(SubscriberChannels::new(2, 2, 2));
        let (route, mut queue) = client(1, &entries(), 16);
        let rx = routes.broadcast.subscribe(Processed);
        for slot in 0..3 {
            send(&mut routes, &[entry(slot)]);
        }
        routes.apply(RouteChange::Join(route.clone(), Some(rx)));
        assert!(routes.routers[0].routes.is_empty());
        let lagged = Err("lagged to receive geyser messages".to_owned());
        assert_eq!(received(&mut queue), [lagged]);
        assert_eq!(route.disconnect_reason(), Some("client_broadcast_lag"));
    }

    #[test]
    fn a_join_without_a_receiver_swaps_reindexes_or_removes_the_route() {
        let mut routes = Routes::new(SubscriberChannels::new(16, 16, 16));
        let [a, b] = [Pubkey::new_unique(), Pubkey::new_unique()];
        let (route, mut queue) = client(1, &accounts(&[a], &[], Some(0), &[]), 16);
        routes.apply(RouteChange::Join(route.clone(), None));
        let batch = [a, b]
            .map(|key| Message::Account(fixtures::simple_message_account(key, Pubkey::default())));
        for (request, indexed) in [
            (accounts(&[a], &[], None, &[]), 1),
            (accounts(&[b], &[], None, &[]), 1),
            (SubscribeRequest::default(), 0),
        ] {
            let route = refilter(&route, &request);
            routes.apply(RouteChange::Join(route.clone(), None));
            send(&mut routes, &batch);
            assert_eq!(received(&mut queue), updates(&route, &batch));
            assert_eq!(routes.routers[0].index.len(), indexed);
        }
        assert!(routes.routers[0].routes.is_empty());
    }

    #[test]
    fn a_full_queue_closes_the_client_behind_what_it_holds() {
        let mut routes = Routes::new(SubscriberChannels::new(16, 16, 16));
        let (route, mut queue) = client(1, &entries(), 2);
        routes.apply(RouteChange::Join(route.clone(), None));
        let batch = [1, 2, 3].map(entry);
        send(&mut routes, &batch);
        let mut expected = updates(&route, &batch[..2]);
        expected.push(Err("lagged to send an update".to_owned()));
        assert_eq!(received(&mut queue), expected);
        assert_eq!(route.disconnect_reason(), Some("client_channel_full"));
        assert!(route.token.is_cancelled());
        assert!(routes.routers[0].routes.is_empty());
    }
}
