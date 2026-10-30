//! [`ReconnectStream`]: a subscription that reconnects after a recoverable failure, and tells
//! the user which banks the failure left partial.

use {
    crate::{
        reconnect::{
            is_recoverable_status_code, BackoffInstance, ReconnectPolicy, ReconnectRequest,
        },
        tracked_stream::{BankRef, BankTracker, TrackedStream},
        Backoff, GeyserGrpcClientError, GrpcConnector, ReconnectConfig, TonicGrpcConnector,
    },
    futures::{stream::Stream, StreamExt},
    std::{
        future::Future,
        mem,
        pin::Pin,
        task::{Context, Poll},
    },
    tonic::Status,
    yellowstone_grpc_proto::prelude::SubscribeUpdate,
};

/// An item of a [`ReconnectStream`].
#[derive(Debug, Clone, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "Keep subscription updates inline to avoid an allocation per message"
)]
pub enum ReconnectEvent {
    /// An update, as a plain subscription delivers it.
    Update(SubscribeUpdate),
    /// The connection dropped and the stream is reconnecting. Every update after this event
    /// comes from the new connection.
    Disconnected(Disconnected),
}

/// What a dropped connection leaves behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disconnected {
    /// Banks the user received content of (account writes, transactions, transaction statuses,
    /// entries or CreatedBank) but not their Block, block footer or block meta, in slot order.
    ///
    /// The server sends a bank's Block, block footer and block meta after all of its content,
    /// so these are the only banks the user may hold in part: discard them. A replay from
    /// [`Disconnected::replay_from_slot`] resends the ones at or above it whole. The stream
    /// drops whatever a later connection sends of the others.
    pub partial_banks: Vec<BankRef>,
    /// The slot the new connection replays from, or [`None`] if it resumes at the live tip.
    ///
    /// The replay resends banks the user already holds whole: the user must handle duplicates.
    /// A replayed bank carries only the latest write of each account (the server keeps one
    /// write per account and bank), so it can have fewer account updates than the same bank
    /// delivered live, with the same final state.
    pub replay_from_slot: Option<u64>,
}

/// A reconnect: the backoff delay, then the connect.
type ConnectFuture<S> = Pin<Box<dyn Future<Output = Result<S, GeyserGrpcClientError>> + Send>>;

/// A connection that is being established, and the bank state it continues.
struct Connecting<S> {
    future: ConnectFuture<S>,
    tracker: BankTracker,
}

/// Where a [`ReconnectStream`] is in its connection's life.
enum Connection<C: GrpcConnector> {
    Open(TrackedStream<C::Stream>),
    Connecting(Connecting<C::Stream>),
    Closed,
}

/// A subscription that reconnects after a recoverable failure.
///
/// It yields [`ReconnectEvent::Update`] for each update, as a plain subscription does. When the
/// connection drops on a recoverable error, it yields one [`ReconnectEvent::Disconnected`]
/// naming the banks the user holds in part, then reconnects with the latest request sent on the
/// [`SubscribeRequestSink`](crate::SubscribeRequestSink), from the slot the
/// [`ReconnectPolicy`] picks.
///
/// The stream adds the [`RECONNECT_FILTER_KEY`](crate::RECONNECT_FILTER_KEY) block meta and
/// slot filters to every request, and hides what only they match.
///
/// A connection that ends, even cleanly, is a disconnect. The stream yields an error and ends
/// on an unrecoverable status, on a failed reconnect, or once its retry budget is spent.
///
/// # Retry budget
///
/// The [`Backoff`] of the [`ReconnectConfig`] is a budget of
/// [`Backoff::max_retries`] reconnects in a row:
///
/// - Each reconnect spends one, and first waits a delay: [`Backoff::initial_interval`] for the
///   first, multiplied by [`Backoff::multiplier`] for each next one.
/// - A connection that makes progress refills the budget, so the next reconnect starts again
///   at one retry and the initial delay. Progress means the connection sealed a bank: it
///   delivered a Block, block footer or block meta, hidden ones included. A ping or a slot
///   status alone is not progress, so a server that accepts, sends one, then drops cannot
///   keep the stream reconnecting forever.
/// - With the budget spent, the next disconnect ends the stream with its error. With
///   `max_retries` at 0, the stream never reconnects.
///
/// Within one reconnect, the connector retries a failed connect on its own:
/// [`TonicGrpcConnector`] makes up to `max_retries` more attempts with the same delays.
///
/// Generic over the [`GrpcConnector`], so tests can script the connections.
///
/// # Examples
///
/// ```ignore
/// let request = ReconnectRequest::new(SubscribeRequest::default());
/// let mut stream = ReconnectStream::connect(connector, request, ReconnectConfig::default()).await?;
/// while let Some(event) = stream.next().await {
///     match event? {
///         ReconnectEvent::Update(update) => apply(update),
///         ReconnectEvent::Disconnected(disconnected) => discard(&disconnected.partial_banks),
///     }
/// }
/// ```
pub struct ReconnectStream<C: GrpcConnector = TonicGrpcConnector> {
    connector: C,
    request: ReconnectRequest,
    policy: ReconnectPolicy,
    backoff: Backoff,
    /// What is left of the retry budget: the delays of the next reconnects.
    retries: BackoffInstance,
    connection: Connection<C>,
}

impl<C> ReconnectStream<C>
where
    C: GrpcConnector<ConnectError = GeyserGrpcClientError>,
{
    /// Opens the first connection with the connector, from the request's own `from_slot`.
    ///
    /// # Arguments
    ///
    /// * `connector` - The [`GrpcConnector`] that opens this connection and every later one.
    /// * `request` - The [`ReconnectRequest`] to subscribe with. A reconnect subscribes with
    ///   the latest request stored there.
    /// * `config` - The [`ReconnectConfig`] with the retry budget and the replay policy.
    ///
    /// # Returns
    ///
    /// A [`ReconnectStream`] on the first connection.
    ///
    /// # Errors
    ///
    /// Returns the connector's error if the first connection fails.
    pub async fn connect(
        connector: C,
        request: ReconnectRequest,
        config: ReconnectConfig,
    ) -> Result<Self, GeyserGrpcClientError> {
        let first = request.load();
        let from_slot = first.from_slot;
        let stream = connector.connect(first, from_slot).await?;
        Ok(Self::open(stream, connector, request, config))
    }

    /// Starts on a first connection opened elsewhere.
    ///
    /// # Arguments
    ///
    /// * `stream` - The first connection, subscribed with the latest request in `request`.
    /// * `connector` - The [`GrpcConnector`] that opens every later connection.
    /// * `request` - The [`ReconnectRequest`] a reconnect subscribes with.
    /// * `config` - The [`ReconnectConfig`] with the retry budget and the replay policy.
    ///
    /// # Returns
    ///
    /// A [`ReconnectStream`] on `stream`.
    pub(crate) fn open(
        stream: C::Stream,
        connector: C,
        request: ReconnectRequest,
        config: ReconnectConfig,
    ) -> Self {
        let tracker = BankTracker::new(request.load().from_slot);
        Self {
            connector,
            request,
            policy: config.policy,
            retries: config.backoff.instance(),
            backoff: config.backoff,
            connection: Connection::Open(TrackedStream::new(stream, tracker)),
        }
    }

    /// Handles the end of the open connection.
    ///
    /// # Arguments
    ///
    /// * `stream` - The connection that ended.
    /// * `status` - Why it ended.
    ///
    /// # Returns
    ///
    /// [`ReconnectEvent::Disconnected`] once a new connection is under way.
    ///
    /// # Errors
    ///
    /// Returns `status`, and leaves the stream closed, if it is not recoverable or if the
    /// retry budget is spent.
    ///
    /// # Panics
    ///
    /// The reconnect sleeps with tokio's timer: polling the stream outside a tokio runtime
    /// panics.
    fn reconnect(
        &mut self,
        stream: TrackedStream<C::Stream>,
        status: Status,
    ) -> Result<ReconnectEvent, Status> {
        if !is_recoverable_status_code(status.code()) {
            return Err(status);
        }
        if stream.sealed_a_bank() {
            self.retries = self.backoff.instance();
        }
        let Some(delay) = self.retries.next() else {
            return Err(status);
        };
        let mut tracker = stream.into_tracker();
        let disconnected = tracker.disconnect(self.policy);
        log::warn!(
            "subscription dropped: {status}; discarding {} partial banks, reconnecting from slot {:?} in {delay:?}",
            disconnected.partial_banks.len(),
            disconnected.replay_from_slot
        );
        let connector = self.connector.clone();
        let request = self.request.clone();
        let from_slot = disconnected.replay_from_slot;
        // Read the request after the delay: a filter change sent meanwhile applies.
        let future = async move {
            tokio::time::sleep(delay).await;
            connector.connect(request.load(), from_slot).await
        };
        self.connection = Connection::Connecting(Connecting {
            future: Box::pin(future),
            tracker,
        });
        Ok(ReconnectEvent::Disconnected(disconnected))
    }
}

impl<C> Stream for ReconnectStream<C>
where
    C: GrpcConnector<ConnectError = GeyserGrpcClientError> + Unpin,
{
    type Item = Result<ReconnectEvent, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            // Every arm puts the connection back, unless it ends the stream.
            match mem::replace(&mut this.connection, Connection::Closed) {
                Connection::Open(mut stream) => {
                    let status = match stream.poll_next_unpin(cx) {
                        Poll::Pending => {
                            this.connection = Connection::Open(stream);
                            return Poll::Pending;
                        }
                        Poll::Ready(Some(Ok(update))) => {
                            this.connection = Connection::Open(stream);
                            return Poll::Ready(Some(Ok(ReconnectEvent::Update(update))));
                        }
                        Poll::Ready(Some(Err(status))) => status,
                        Poll::Ready(None) => {
                            Status::unavailable("the server ended the subscription")
                        }
                    };
                    return Poll::Ready(Some(this.reconnect(stream, status)));
                }
                Connection::Connecting(mut connecting) => {
                    match connecting.future.as_mut().poll(cx) {
                        Poll::Pending => {
                            this.connection = Connection::Connecting(connecting);
                            return Poll::Pending;
                        }
                        Poll::Ready(Ok(stream)) => {
                            this.connection =
                                Connection::Open(TrackedStream::new(stream, connecting.tracker));
                        }
                        Poll::Ready(Err(error)) => {
                            return Poll::Ready(Some(Err(reconnect_error(error))));
                        }
                    }
                }
                Connection::Closed => return Poll::Ready(None),
            }
        }
    }
}

/// Turns a failed reconnect into the stream's final error.
///
/// # Arguments
///
/// * `error` - The [`GeyserGrpcClientError`] the connector failed with.
///
/// # Returns
///
/// The server's [`Status`] if there is one, else [`Status::unavailable`] describing `error`.
fn reconnect_error(error: GeyserGrpcClientError) -> Status {
    match error {
        GeyserGrpcClientError::TonicStatus(status) => status,
        GeyserGrpcClientError::TransportError(error) => {
            Status::unavailable(format!("reconnect failed: {error}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            reconnect::RECONNECT_FILTER_KEY,
            test_tools::{ConnectCall, ScriptedConnector},
            Backoff,
        },
        futures::channel::mpsc::UnboundedSender,
        std::time::Duration,
        tonic::Code,
        yellowstone_grpc_proto::prelude::{
            subscribe_update::UpdateOneof, SlotStatus, SubscribeRequest, SubscribeUpdateAccount,
            SubscribeUpdateBlockMeta, SubscribeUpdateSlot,
        },
    };

    type Server = UnboundedSender<Result<SubscribeUpdate, Status>>;

    fn account(slot: u64, bank_id: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["user".to_owned()],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                bank_id: Some(bank_id),
                ..Default::default()
            })),
            created_at: None,
        }
    }

    fn internal_meta(slot: u64, bank_id: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![RECONNECT_FILTER_KEY.to_owned()],
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                bank_id,
                ..Default::default()
            })),
            created_at: None,
        }
    }

    fn confirmed(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![RECONNECT_FILTER_KEY.to_owned()],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                bank_id: Some(slot),
                status: SlotStatus::SlotConfirmed as i32,
                ..Default::default()
            })),
            created_at: None,
        }
    }

    fn config(policy: ReconnectPolicy, max_retries: u32) -> ReconnectConfig {
        ReconnectConfig {
            backoff: Backoff::new(Duration::ZERO, 1.0, max_retries),
            policy,
        }
    }

    fn request(from_slot: Option<u64>) -> ReconnectRequest {
        ReconnectRequest::new(SubscribeRequest {
            from_slot,
            ..Default::default()
        })
    }

    async fn open(
        connector: &ScriptedConnector,
        config: ReconnectConfig,
    ) -> (Server, ReconnectStream<ScriptedConnector>) {
        let server = connector.accept();
        let stream = ReconnectStream::connect(connector.clone(), request(None), config)
            .await
            .expect("first connection");
        (server, stream)
    }

    async fn next(stream: &mut ReconnectStream<ScriptedConnector>) -> ReconnectEvent {
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended")
            .expect("stream failed")
    }

    async fn next_error(stream: &mut ReconnectStream<ScriptedConnector>) -> Status {
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended")
            .expect_err("stream must fail")
    }

    fn disconnected(partial_banks: Vec<BankRef>, replay_from_slot: Option<u64>) -> ReconnectEvent {
        ReconnectEvent::Disconnected(Disconnected {
            partial_banks,
            replay_from_slot,
        })
    }

    #[tokio::test]
    async fn the_first_connection_carries_the_reconnect_filters_and_the_users_from_slot() {
        let connector = ScriptedConnector::default();
        connector.accept();
        let request = request(Some(42));
        let _stream =
            ReconnectStream::connect(connector.clone(), request, ReconnectConfig::default())
                .await
                .unwrap();

        let calls = connector.calls();
        assert_eq!(calls.len(), 1);
        let ConnectCall {
            request: sent,
            from_slot,
        } = &calls[0];
        assert_eq!(*from_slot, Some(42));
        assert!(sent.blocks_meta.contains_key(RECONNECT_FILTER_KEY));
        assert!(sent.slots.contains_key(RECONNECT_FILTER_KEY));
    }

    #[tokio::test]
    async fn the_first_connection_failing_is_an_error() {
        let connector = ScriptedConnector::default();
        connector.refuse(Status::permission_denied("token"));
        let result =
            ReconnectStream::connect(connector, request(None), ReconnectConfig::default()).await;
        assert!(
            matches!(result, Err(GeyserGrpcClientError::TonicStatus(s)) if s.code() == Code::PermissionDenied)
        );
    }

    #[tokio::test]
    async fn a_disconnect_before_any_confirmed_slot_replays_from_the_requested_slot() {
        let connector = ScriptedConnector::default();
        let first = connector.accept();
        let mut stream = ReconnectStream::connect(
            connector.clone(),
            request(Some(42)),
            ReconnectConfig::default(),
        )
        .await
        .unwrap();
        first.unbounded_send(Ok(account(50, 1))).unwrap();
        assert_eq!(
            next(&mut stream).await,
            ReconnectEvent::Update(account(50, 1))
        );

        let second = connector.accept();
        drop(first);
        assert_eq!(
            next(&mut stream).await,
            disconnected(
                vec![BankRef {
                    slot: 50,
                    bank_id: 1
                }],
                Some(42)
            )
        );
        second.unbounded_send(Ok(account(50, 1))).unwrap();
        assert_eq!(
            next(&mut stream).await,
            ReconnectEvent::Update(account(50, 1))
        );
        assert_eq!(connector.calls()[1].from_slot, Some(42));
    }

    #[tokio::test]
    async fn a_disconnect_reports_partial_banks_then_replays_from_the_last_confirmed_slot() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, ReconnectConfig::default()).await;

        first.unbounded_send(Ok(account(10, 1))).unwrap();
        first.unbounded_send(Ok(internal_meta(10, 1))).unwrap();
        first.unbounded_send(Ok(confirmed(10))).unwrap();
        first.unbounded_send(Ok(account(11, 2))).unwrap();
        assert_eq!(
            next(&mut stream).await,
            ReconnectEvent::Update(account(10, 1))
        );
        assert_eq!(
            next(&mut stream).await,
            ReconnectEvent::Update(account(11, 2))
        );

        let second = connector.accept();
        first
            .unbounded_send(Err(Status::unavailable("drop")))
            .unwrap();
        assert_eq!(
            next(&mut stream).await,
            disconnected(
                vec![BankRef {
                    slot: 11,
                    bank_id: 2
                }],
                Some(10)
            )
        );

        second.unbounded_send(Ok(account(11, 2))).unwrap();
        assert_eq!(
            next(&mut stream).await,
            ReconnectEvent::Update(account(11, 2))
        );
        assert_eq!(connector.calls()[1].from_slot, Some(10));
    }

    #[tokio::test]
    async fn no_replay_reconnects_at_the_tip_and_drops_the_partial_banks_tail() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, config(ReconnectPolicy::NoReplay, 3)).await;
        first.unbounded_send(Ok(confirmed(9))).unwrap();
        first.unbounded_send(Ok(account(10, 1))).unwrap();
        assert_eq!(
            next(&mut stream).await,
            ReconnectEvent::Update(account(10, 1))
        );

        let second = connector.accept();
        drop(first);
        assert_eq!(
            next(&mut stream).await,
            disconnected(
                vec![BankRef {
                    slot: 10,
                    bank_id: 1
                }],
                None
            )
        );

        second.unbounded_send(Ok(account(10, 1))).unwrap();
        second.unbounded_send(Ok(account(11, 2))).unwrap();
        assert_eq!(
            next(&mut stream).await,
            ReconnectEvent::Update(account(11, 2))
        );
        assert_eq!(connector.calls()[1].from_slot, None);
    }

    #[tokio::test]
    async fn a_reconnect_subscribes_with_the_request_stored_during_its_delay() {
        let connector = ScriptedConnector::default();
        let first = connector.accept();
        let request = request(None);
        let config = ReconnectConfig::default().with_backoff(Backoff::new(
            Duration::from_millis(50),
            1.0,
            3,
        ));
        let mut stream = ReconnectStream::connect(connector.clone(), request.clone(), config)
            .await
            .unwrap();

        let second = connector.accept();
        first.unbounded_send(Err(Status::internal("drop"))).unwrap();
        assert!(matches!(
            next(&mut stream).await,
            ReconnectEvent::Disconnected(_)
        ));
        // The reconnect is still waiting its delay.
        let mut updated = SubscribeRequest::default();
        updated.slots.insert("user".to_owned(), Default::default());
        let stored = request.store(updated);

        second.unbounded_send(Ok(account(11, 2))).unwrap();
        next(&mut stream).await;
        assert_eq!(connector.calls()[1].request, *stored);
        assert!(stored.slots.contains_key("user"));
        assert!(stored.slots.contains_key(RECONNECT_FILTER_KEY));
    }

    #[tokio::test]
    async fn an_unrecoverable_status_ends_the_stream() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, ReconnectConfig::default()).await;
        first
            .unbounded_send(Err(Status::out_of_range("from_slot is gone")))
            .unwrap();
        assert_eq!(next_error(&mut stream).await.code(), Code::OutOfRange);
        assert!(stream.next().await.is_none());
        assert_eq!(connector.calls().len(), 1);
    }

    #[tokio::test]
    async fn a_failed_reconnect_ends_the_stream() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, ReconnectConfig::default()).await;
        connector.refuse(Status::unavailable("still down"));
        drop(first);

        assert!(matches!(
            next(&mut stream).await,
            ReconnectEvent::Disconnected(_)
        ));
        assert_eq!(next_error(&mut stream).await.code(), Code::Unavailable);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn connections_that_deliver_nothing_spend_the_retry_budget() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, config(ReconnectPolicy::default(), 2)).await;

        let second = connector.accept();
        drop(first);
        assert!(matches!(
            next(&mut stream).await,
            ReconnectEvent::Disconnected(_)
        ));
        let third = connector.accept();
        drop(second);
        assert!(matches!(
            next(&mut stream).await,
            ReconnectEvent::Disconnected(_)
        ));
        drop(third);
        assert_eq!(next_error(&mut stream).await.code(), Code::Unavailable);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn a_hidden_status_alone_does_not_restore_the_retry_budget() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, config(ReconnectPolicy::default(), 1)).await;

        let second = connector.accept();
        drop(first);
        assert!(matches!(
            next(&mut stream).await,
            ReconnectEvent::Disconnected(_)
        ));
        second.unbounded_send(Ok(confirmed(5))).unwrap();
        drop(second);
        assert_eq!(next_error(&mut stream).await.code(), Code::Unavailable);
        assert_eq!(connector.calls().len(), 2);
    }

    #[tokio::test]
    async fn a_hidden_block_meta_restores_the_retry_budget() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, config(ReconnectPolicy::default(), 1)).await;

        let second = connector.accept();
        drop(first);
        assert!(matches!(
            next(&mut stream).await,
            ReconnectEvent::Disconnected(_)
        ));
        second.unbounded_send(Ok(confirmed(5))).unwrap();
        second.unbounded_send(Ok(internal_meta(6, 6))).unwrap();
        connector.accept();
        drop(second);
        assert_eq!(next(&mut stream).await, disconnected(vec![], Some(5)));
    }

    #[tokio::test]
    async fn each_reconnect_in_a_row_waits_a_longer_delay() {
        let connector = ScriptedConnector::default();
        let config = ReconnectConfig::default().with_backoff(Backoff::new(
            Duration::from_millis(50),
            2.0,
            3,
        ));
        let (first, mut stream) = open(&connector, config).await;

        // 50 ms, then 100 ms: the connections in between seal no bank.
        let mut server = first;
        for delay in [50, 100] {
            let next_server = connector.accept();
            drop(server);
            assert!(matches!(
                next(&mut stream).await,
                ReconnectEvent::Disconnected(_)
            ));
            let started = std::time::Instant::now();
            // A bank of its own: the previous one was reported partial and is dropped.
            next_server.unbounded_send(Ok(account(delay, 1))).unwrap();
            next(&mut stream).await;
            assert!(
                started.elapsed() >= Duration::from_millis(delay),
                "reconnected after {:?}, before its {delay} ms delay",
                started.elapsed()
            );
            server = next_server;
        }
    }

    /// Adapted from `refactor/auto-reconnect`: a server that accepts, sends an update that
    /// completes no bank, then drops, must not get reconnects forever.
    #[tokio::test]
    async fn a_flapping_server_without_complete_banks_spends_the_retry_budget() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, config(ReconnectPolicy::default(), 2)).await;
        let flap = |server: &Server| {
            server.unbounded_send(Ok(account(50, 1))).unwrap();
            server
                .unbounded_send(Err(Status::unavailable("flap")))
                .unwrap();
        };
        flap(&first);
        for _ in 0..3 {
            flap(&connector.accept());
        }
        let mut errors = 0;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("stream stalled")
        {
            errors += usize::from(item.is_err());
        }
        assert_eq!(errors, 1, "stops with one error once the budget is spent");
        assert_eq!(
            connector.calls().len(),
            3,
            "the first connect, then max_retries reconnects"
        );
    }

    /// Adapted from `refactor/auto-reconnect`: connections that complete a bank before they
    /// drop never run out of retries.
    #[tokio::test]
    async fn complete_banks_restore_the_retry_budget() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, config(ReconnectPolicy::default(), 2)).await;
        let healthy = |server: &Server, slot| {
            server
                .unbounded_send(Ok(internal_meta(slot, slot)))
                .unwrap();
            server
                .unbounded_send(Err(Status::unavailable("drop")))
                .unwrap();
        };
        healthy(&first, 10);
        for slot in 11..14 {
            healthy(&connector.accept(), slot);
        }
        let last = connector.accept();
        last.unbounded_send(Ok(internal_meta(14, 14))).unwrap();
        last.unbounded_send(Err(Status::invalid_argument("end of test")))
            .unwrap();

        let mut disconnects = 0;
        let error = loop {
            match tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("stream stalled")
                .expect("stream ended")
            {
                Ok(ReconnectEvent::Disconnected(_)) => disconnects += 1,
                Ok(ReconnectEvent::Update(_)) => {}
                Err(error) => break error,
            }
        };
        // Four reconnects with a budget of two: each complete bank refilled it.
        assert_eq!(disconnects, 4);
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(connector.calls().len(), 5);
    }

    #[tokio::test]
    async fn zero_retries_never_reconnects() {
        let connector = ScriptedConnector::default();
        let (first, mut stream) = open(&connector, config(ReconnectPolicy::default(), 0)).await;
        first.unbounded_send(Ok(account(10, 1))).unwrap();
        next(&mut stream).await;
        first
            .unbounded_send(Err(Status::unavailable("drop")))
            .unwrap();
        assert_eq!(next_error(&mut stream).await.code(), Code::Unavailable);
        assert_eq!(connector.calls().len(), 1);
    }
}
