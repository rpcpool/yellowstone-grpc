use {
    crate::plugin::message::{ContactInfoMessage, MessageContactInfo},
    futures::stream::{Stream, StreamExt},
    log::info,
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    },
    tokio::sync::{broadcast, watch},
    tokio_stream::wrappers::BroadcastStream,
};

pub struct ContactInfoNotification {
    pub message: ContactInfoMessage,
    pub is_startup: bool,
}

#[derive(Clone)]
pub struct ContactInfoEvent {
    /// Map version at which this change was applied.
    pub seq: u64,
    pub message: ContactInfoMessage,
}

/// The gossip table at one revision.
pub struct TopologySnapshot {
    /// Revision the copy was taken at. Updates with `seq <= rev` are already in `topology`.
    pub rev: u64,
    pub topology: Vec<MessageContactInfo>,
}

pub struct ContactInfoTable {
    nodes: HashMap<Pubkey, Arc<MessageContactInfo>>,
    version: u64,
}

pub struct ContactInfoState {
    map: Mutex<ContactInfoTable>,
    tx: broadcast::Sender<ContactInfoEvent>,
    /// False until the startup replay is complete. Subscribers wait on this so no client is
    /// ever handed a partial table it cannot distinguish from a complete one.
    complete_tx: watch::Sender<bool>,
}

impl ContactInfoState {
    pub fn new(capacity: usize) -> Arc<Self> {
        let (tx, _) = broadcast::channel(capacity);
        Arc::new(Self {
            map: Mutex::new(ContactInfoTable {
                nodes: HashMap::new(),
                version: 0,
            }),
            tx,
            complete_tx: watch::channel(false).0,
        })
    }

    /// Resolves once the startup replay is done. See [`ContactInfoNotification`].
    async fn wait_until_complete(&self) {
        let mut rx = self.complete_tx.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// A copy of the table plus a receiver that sees every event after it.
    ///
    /// Safe only because `contact_info_loop` bumps the version, mutates and broadcasts under one
    /// lock: nothing can be published while this holds it. Moving that broadcast out from under
    /// the lock breaks this function, however correct this function looks alone.
    ///
    /// Events at or below `rev` may still arrive; the caller drops them.
    pub fn subscribe_and_snapshot(&self) -> (BroadcastStream<ContactInfoEvent>, TopologySnapshot) {
        let messages_rx = BroadcastStream::new(self.tx.subscribe());
        let table = self.map.lock().expect("contact info map mutex poisoned");
        (
            messages_rx,
            TopologySnapshot {
                rev: table.version,
                topology: table.nodes.values().map(|node| (**node).clone()).collect(),
            },
        )
    }
}

/// The single writer. Owns every mutation of the table.
///
/// Ends when `notifications` runs dry, which happens when the plugin drops its sender. That is
/// the only exit, so the loop cannot stop with work still queued.
pub async fn contact_info_loop<St>(mut notifications: St, state: Arc<ContactInfoState>)
where
    St: Stream<Item = ContactInfoNotification> + Unpin,
{
    while let Some(ContactInfoNotification {
        message,
        is_startup,
    }) = notifications.next().await
    {
        if !is_startup && !*state.complete_tx.borrow() {
            let nodes = state.map.lock().expect("poisoned").nodes.len();
            info!("contact info startup replay complete: {nodes} nodes");
            let _ = state.complete_tx.send(true);
        }

        // One critical section: subscribers must never observe a revision the map has not
        // applied. See `subscribe_and_snapshot`.
        let mut table = state.map.lock().unwrap();
        table.version += 1;
        let seq = table.version;
        match &message {
            ContactInfoMessage::Node(n) => {
                table.nodes.insert(n.pubkey, Arc::clone(n));
            }
            ContactInfoMessage::Removed(r) => {
                table.nodes.remove(&r.pubkey);
            }
        }
        let _ = state.tx.send(ContactInfoEvent { seq, message });
    }
}

/// gRPC subscriber side of the gossip contact info feed.
pub mod grpc {
    use {
        super::ContactInfoState,
        crate::{grpc::SubscriptionOwnedPermit, metrics, plugin::convert_to},
        futures::{
            sink::{Sink, SinkExt},
            stream::StreamExt as _,
        },
        log::{error, info},
        std::{sync::Arc, time::Duration},
        tokio::sync::mpsc,
        tokio_stream::wrappers::{errors::BroadcastStreamRecvError, ReceiverStream},
        tokio_util::{
            sync::{CancellationToken, PollSender},
            task::TaskTracker,
        },
        tonic::{Result as TonicResult, Status},
        yellowstone_grpc_proto::prelude::SubscribeUpdateGossip,
    };

    type GossipItem = TonicResult<SubscribeUpdateGossip>;

    struct ContactInfoClientSession {
        id: usize,
        subscriber_id: String,
        disconnect_reason: &'static str,
        cancellation_token: CancellationToken,
        _permit: Option<SubscriptionOwnedPermit>,
    }

    impl ContactInfoClientSession {
        fn new(
            id: usize,
            subscriber_id: Option<String>,
            cancellation_token: CancellationToken,
            permit: Option<SubscriptionOwnedPermit>,
        ) -> Self {
            Self {
                id,
                subscriber_id: subscriber_id.unwrap_or_default(),
                disconnect_reason: "unknown",
                cancellation_token,
                _permit: permit,
            }
        }
    }

    impl Drop for ContactInfoClientSession {
        fn drop(&mut self) {
            metrics::incr_client_disconnect(&self.subscriber_id, self.disconnect_reason);
            self.cancellation_token.cancel();
        }
    }

    const PING_INTERVAL: Duration = Duration::from_secs(20);

    /// A client that cannot drain a snapshot before the broadcast ring wraps never catches up,
    /// so retrying forever would livelock instead of surfacing the problem.
    const MAX_RESNAPSHOT_ATTEMPTS: usize = 3;

    pub fn spawn_subscriber(
        id: usize,
        subscriber_id: Option<String>,
        permit: Option<SubscriptionOwnedPermit>,
        channel_capacity: usize,
        state: Arc<ContactInfoState>,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> ReceiverStream<GossipItem> {
        let (stream_tx, stream_rx) = mpsc::channel(channel_capacity);

        let ping_stream_tx = stream_tx.clone();
        let ping_cancellation_token = cancellation_token.clone();
        let ping_client_cancel = cancellation_token.clone();
        task_tracker.spawn(async move {
            let mut interval = tokio::time::interval(PING_INTERVAL);
            loop {
                tokio::select! {
                    _ = ping_cancellation_token.cancelled() => {
                        info!("contact info client #{id}: ping cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if ping_stream_tx.send(Ok(convert_to::create_gossip_ping())).await.is_err() {
                            ping_client_cancel.cancel();
                            info!("detected dead contact info client #{id}");
                            break;
                        }
                    }
                }
            }
            info!("contact info client #{id}: ping task exiting");
        });

        let session =
            ContactInfoClientSession::new(id, subscriber_id, cancellation_token.clone(), permit);

        task_tracker.spawn(contact_info_client_loop(
            session,
            PollSender::new(stream_tx),
            state,
            cancellation_token,
        ));

        ReceiverStream::new(stream_rx)
    }

    async fn contact_info_client_loop<S>(
        mut session: ContactInfoClientSession,
        mut sink: S,
        state: Arc<ContactInfoState>,
        cancellation_token: CancellationToken,
    ) where
        S: Sink<GossipItem> + Unpin,
    {
        // Never hand a client a partial table: agave replays the gossip state at validator start
        // with no end-of-replay marker, so wait until a live notification proves it is done.
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                let _ = sink.send(Err(Status::unavailable("server is shutting down try again later"))).await;
                session.disconnect_reason = "server_shutdown";
                return;
            }
            _ = state.wait_until_complete() => {}
        }

        let mut resnapshot_attempts = 0;

        'handoff: loop {
            let (mut messages_rx, snapshot) = state.subscribe_and_snapshot();
            let snapshot_seq = snapshot.rev;

            info!(
                "contact info client #{}/{}: sending snapshot of {} nodes at seq {}",
                session.subscriber_id,
                session.id,
                snapshot.topology.len(),
                snapshot_seq
            );

            let update = convert_to::create_gossip_snapshot(snapshot_seq, &snapshot.topology);
            drop(snapshot);
            if sink.send(Ok(update)).await.is_err() {
                session.disconnect_reason = "client_closed";
                break 'handoff;
            }
            metrics::incr_grpc_message_sent_counter(&session.subscriber_id);

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        let _ = sink.send(Err(Status::unavailable("server is shutting down try again later"))).await;
                        session.disconnect_reason = "server_shutdown";
                        break 'handoff;
                    }
                    message = messages_rx.next() => {
                        let event = match message {
                            Some(Ok(event)) => event,
                            None => {
                                session.disconnect_reason = "broadcast_closed";
                                break 'handoff;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                // The client's view now has a hole and skipping ahead leaves it
                                // permanently wrong. The table is authoritative: resynchronise.
                                resnapshot_attempts += 1;
                                if resnapshot_attempts > MAX_RESNAPSHOT_ATTEMPTS {
                                    error!(
                                        "contact info client #{}/{}: lagged {skipped} events, giving up after {MAX_RESNAPSHOT_ATTEMPTS} re-snapshots",
                                        session.subscriber_id, session.id
                                    );
                                    session.disconnect_reason = "client_broadcast_lag";
                                    let _ = sink.send(Err(Status::internal("lagged to receive contact info messages"))).await;
                                    break 'handoff;
                                }
                                info!(
                                    "contact info client #{}/{}: lagged {skipped} events, re-snapshotting (attempt {resnapshot_attempts})",
                                    session.subscriber_id, session.id
                                );
                                continue 'handoff;
                            }
                        };

                        // Already reflected in the snapshot this client was primed with.
                        if event.seq <= snapshot_seq {
                            continue;
                        }

                        let update = convert_to::create_gossip_update(event.seq, &event.message);
                        if sink.send(Ok(update)).await.is_err() {
                            error!("contact info client #{}/{}: stream closed", session.subscriber_id, session.id);
                            session.disconnect_reason = "client_closed";
                            break 'handoff;
                        }
                        resnapshot_attempts = 0;
                        metrics::incr_grpc_message_sent_counter(&session.subscriber_id);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::message::MessageContactInfoRemoved,
        prost_types::Timestamp,
        std::{collections::HashMap as Map, time::Duration},
        tokio::sync::mpsc,
        tokio_stream::wrappers::{errors::BroadcastStreamRecvError, UnboundedReceiverStream},
    };

    /// `shred_version` doubles as a marker so a reconstructed table can be compared field-wise
    /// against the writer's own table without holding the whole node.
    fn node(pubkey: Pubkey, marker: u16) -> ContactInfoMessage {
        ContactInfoMessage::Node(Arc::new(MessageContactInfo {
            pubkey,
            wallclock: 0,
            outset: 0,
            shred_version: marker,
            version_major: 0,
            version_minor: 0,
            version_patch: 0,
            version_commit: 0,
            version_feature_set: 0,
            version_client_id: 0,
            gossip: None,
            tpu_quic: None,
            tpu_forwards_quic: None,
            tpu_vote_udp: None,
            tpu_vote_quic: None,
            tvu_udp: None,
            tvu_quic: None,
            serve_repair_udp: None,
            serve_repair_quic: None,
            rpc: None,
            rpc_pubsub: None,
            alpenglow: None,
            created_at: Timestamp::default(),
        }))
    }

    fn removed(pubkey: Pubkey) -> ContactInfoMessage {
        ContactInfoMessage::Removed(Arc::new(MessageContactInfoRemoved {
            pubkey,
            created_at: Timestamp::default(),
        }))
    }

    fn live(message: ContactInfoMessage) -> ContactInfoNotification {
        ContactInfoNotification {
            message,
            is_startup: false,
        }
    }

    /// Flattens the table to marker values so tests can compare it to a reconstruction.
    fn table_markers(state: &ContactInfoState) -> Map<Pubkey, u16> {
        state
            .map
            .lock()
            .unwrap()
            .nodes
            .iter()
            .map(|(pubkey, info)| (*pubkey, info.shred_version))
            .collect()
    }

    /// Applies one event to a client-side reconstruction of the table.
    fn apply(view: &mut Map<Pubkey, u16>, message: &ContactInfoMessage) {
        match message {
            ContactInfoMessage::Node(n) => {
                view.insert(n.pubkey, n.shred_version);
            }
            ContactInfoMessage::Removed(r) => {
                view.remove(&r.pubkey);
            }
        }
    }

    /// Deterministic stand-in for a random number generator.
    fn lcg(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *seed >> 33
    }

    #[tokio::test]
    async fn applies_updates_and_removals_in_order() {
        let state = ContactInfoState::new(64);
        let (tx, rx) = mpsc::unbounded_channel();
        let loop_handle = tokio::spawn(contact_info_loop(
            UnboundedReceiverStream::new(rx),
            Arc::clone(&state),
        ));

        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        tx.send(live(node(a, 1))).unwrap();
        tx.send(live(node(b, 2))).unwrap();
        tx.send(live(node(a, 3))).unwrap();
        tx.send(live(removed(b))).unwrap();
        drop(tx);
        loop_handle.await.unwrap();

        let table = state.map.lock().unwrap();
        assert_eq!(table.version, 4, "version counts every applied event");
        assert_eq!(table.nodes.len(), 1);
        assert_eq!(table.nodes[&a].shred_version, 3, "latest write wins");
        assert!(!table.nodes.contains_key(&b), "removal deletes the entry");
    }

    #[tokio::test]
    async fn table_is_incomplete_until_startup_replay_ends() {
        let state = ContactInfoState::new(64);
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(contact_info_loop(
            UnboundedReceiverStream::new(rx),
            Arc::clone(&state),
        ));

        // Startup replay: entries land in the table but it is not yet complete.
        for i in 0..3 {
            tx.send(ContactInfoNotification {
                message: node(Pubkey::new_unique(), i),
                is_startup: true,
            })
            .unwrap();
        }
        tokio::task::yield_now().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), state.wait_until_complete())
                .await
                .is_err(),
            "subscribers must not be released during the startup replay"
        );

        // The first live notification marks the replay as finished.
        tx.send(live(node(Pubkey::new_unique(), 9))).unwrap();
        tokio::time::timeout(Duration::from_secs(5), state.wait_until_complete())
            .await
            .expect("completes once a live notification arrives");
    }

    #[tokio::test]
    async fn snapshot_at_or_below_seq_is_already_applied() {
        let state = ContactInfoState::new(64);
        let (tx, rx) = mpsc::unbounded_channel();
        let loop_handle = tokio::spawn(contact_info_loop(
            UnboundedReceiverStream::new(rx),
            Arc::clone(&state),
        ));

        let a = Pubkey::new_unique();
        tx.send(live(node(a, 7))).unwrap();
        drop(tx);
        loop_handle.await.unwrap();

        let (_rx, snapshot) = state.subscribe_and_snapshot();
        assert_eq!(snapshot.rev, 1);
        assert_eq!(snapshot.topology.len(), 1);
        assert_eq!(
            snapshot.topology[0].shred_version, 7,
            "the snapshot already reflects every event up to and including `seq`, \
             which is why the client drops events with seq <= snapshot_seq"
        );
    }

    /// A subscriber that falls behind can always be rebuilt from the table.
    ///
    /// This is the premise the `Lagged` arm rests on: events dropped from the broadcast ring are
    /// unrecoverable from the stream, but their effect is in the table, so a fresh snapshot
    /// restores the client's view. If that were not true, re-snapshotting would be pointless and
    /// a lagging client would have to be disconnected.
    #[tokio::test]
    async fn a_lagged_subscriber_is_restored_by_a_fresh_snapshot() {
        // Two slots, so overrunning the ring takes three events.
        let state = ContactInfoState::new(2);
        let (tx, rx) = mpsc::unbounded_channel();
        let loop_handle = tokio::spawn(contact_info_loop(
            UnboundedReceiverStream::new(rx),
            Arc::clone(&state),
        ));

        let a = Pubkey::new_unique();
        tx.send(live(node(a, 1))).unwrap();
        tokio::task::yield_now().await;

        // Subscribe, then stop reading.
        let (mut messages_rx, snapshot) = state.subscribe_and_snapshot();
        assert_eq!(snapshot.rev, 1);

        // Overrun the ring while the subscriber is not draining it.
        let b = Pubkey::new_unique();
        for marker in 2..6 {
            tx.send(live(node(a, marker))).unwrap();
        }
        tx.send(live(node(b, 99))).unwrap();
        drop(tx);
        loop_handle.await.unwrap();

        // The stream reports loss rather than silently skipping.
        let skipped = match messages_rx.next().await {
            Some(Err(BroadcastStreamRecvError::Lagged(n))) => n,
            Some(Ok(_)) => panic!("expected Lagged, got an event"),
            None => panic!("expected Lagged, got end of stream"),
        };
        assert!(
            skipped > 0,
            "lag should report how many events were dropped"
        );

        // The table still holds everything the subscriber missed.
        let (_rx, recovered) = state.subscribe_and_snapshot();
        let markers: Map<Pubkey, u16> = recovered
            .topology
            .iter()
            .map(|info| (info.pubkey, info.shred_version))
            .collect();
        assert_eq!(
            markers.get(&a),
            Some(&5),
            "latest write for a is in the table"
        );
        assert_eq!(
            markers.get(&b),
            Some(&99),
            "b was only ever announced in a dropped event"
        );
    }

    /// A client that connects mid-stream must end up with the same table the server has.
    ///
    /// Setup: one writer applies 3000 inserts and removals. Six subscribers repeatedly connect
    /// while it runs. Each takes a snapshot, applies the events after it, and builds its own
    /// copy of the table.
    ///
    /// Passes when every copy equals the writer's table. Fails if the snapshot is incomplete,
    /// or the `seq <= snapshot_seq` filter drops the wrong events.
    ///
    /// Does NOT cover the locking in `contact_info_loop`. Breaking that critical section opens
    /// a window only nanoseconds wide and this test does not land in, still passes with
    /// the ordering reversed. That correctness comes from review, not from here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn subscriber_reconstruction_matches_table_under_concurrent_writes() {
        const EVENTS: u64 = 3_000;
        const SUBSCRIBERS: u64 = 6;
        const KEYS: usize = 40;

        let state = ContactInfoState::new(1 << 16);
        let (tx, rx) = mpsc::unbounded_channel();
        let loop_handle = tokio::spawn(contact_info_loop(
            UnboundedReceiverStream::new(rx),
            Arc::clone(&state),
        ));

        let keys: Vec<Pubkey> = (0..KEYS).map(|_| Pubkey::new_unique()).collect();

        let writer_keys = keys.clone();
        let writer = tokio::spawn(async move {
            let mut seed = 0x5EED_u64;
            for i in 0..EVENTS {
                let key = writer_keys[(lcg(&mut seed) as usize) % KEYS];
                let message = if lcg(&mut seed).is_multiple_of(4) {
                    removed(key)
                } else {
                    node(key, i as u16)
                };
                if tx.send(live(message)).is_err() {
                    break;
                }
                if i.is_multiple_of(64) {
                    tokio::task::yield_now().await;
                }
            }
            drop(tx);
        });

        // Each subscriber repeatedly performs the handoff while the writer is running and
        // reconstructs the table from its snapshot plus the events that follow it.
        let subscribers: Vec<_> = (0..SUBSCRIBERS)
            .map(|_| {
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    loop {
                        let (mut messages_rx, snapshot) = state.subscribe_and_snapshot();
                        let snapshot_seq = snapshot.rev;
                        let mut view: Map<Pubkey, u16> = snapshot
                            .topology
                            .iter()
                            .map(|info| (info.pubkey, info.shred_version))
                            .collect();

                        // Reconnected after the writer finished; the snapshot is the final table.
                        if snapshot_seq >= EVENTS {
                            return view;
                        }

                        let mut drained = 0u64;
                        loop {
                            match messages_rx.next().await {
                                Some(Ok(event)) => {
                                    if event.seq > snapshot_seq {
                                        apply(&mut view, &event.message);
                                    }
                                    // Every notification bumps the revision by one, so the
                                    // writer's last event is the one numbered `EVENTS`.
                                    if event.seq >= EVENTS {
                                        return view;
                                    }
                                    drained += 1;
                                    // Reconnect periodically so the handoff runs at many
                                    // different points in the writer's stream.
                                    if drained.is_multiple_of(128) {
                                        break;
                                    }
                                }
                                // The sender lives in `state`, which this task holds, so this
                                // only fires if the writer is torn down early.
                                None => return view,
                                Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                    panic!("broadcast lagged by {n}; capacity is undersized")
                                }
                            }
                        }
                    }
                })
            })
            .collect();

        writer.await.unwrap();
        loop_handle.await.unwrap();

        let expected = table_markers(&state);
        assert!(
            !expected.is_empty(),
            "writer should have populated the table"
        );

        for (i, subscriber) in subscribers.into_iter().enumerate() {
            let view = subscriber.await.unwrap();
            assert_eq!(
                view, expected,
                "subscriber {i} diverged from the writer's table"
            );
        }
    }
}
