//! A Subscribe client's side of its route: where it stands in joining the route, and the
//! route changes it sends.

#![allow(rustdoc::private_intra_doc_links)]

#[cfg(doc)]
use crate::grpc::GrpcService;
use {
    crate::{
        grpc::{BroadcastedMessage, ClientSession, SubscribeSender, SubscriberChannels},
        plugin::{filter::Filter, message::CommitmentLevel},
        route::{LeaveOnDrop, Route, RouteChange, RouteKey, RouteLeave},
    },
    std::sync::Arc,
    tokio::sync::{broadcast, mpsc, oneshot},
};

/// Where a client on the broadcast stands in joining its route.
///
/// [`GrpcService::client_loop`] keeps it while the client reads a broadcast receiver and its
/// filter has route keys. At a read that finds its receiver and its requests empty, the client
/// sends a [`RouteChange::Prepare`] from `Unprepared`, and a [`RouteChange::Join`] with the
/// receiver from `Prepared`.
enum RouteJoinState {
    /// No preparation of the client's route waits at its owner.
    Unprepared,
    /// The owner claims the route's keys, and answers on this receiver once all are claimed.
    Preparing(oneshot::Receiver<()>),
    /// The owner has claimed every key, so the client hands its receiver over next.
    Prepared,
}

/// A Subscribe client's side of its route: the template its routes copy, its route keys, where
/// it stands in joining its route, and its last leave.
///
/// [`GrpcService::client_loop`] makes it where the client's live stream starts. It sends the two
/// steps of the client's join with [`ClientRouting::send_join_step`], moves the route when a
/// filter change applies with [`ClientRouting::change_filter`], and ends the client with
/// [`ClientRouting::end_session`].
///
/// # Examples
///
/// ```ignore
/// let mut client_routing = ClientRouting::new(&session, &stream_tx, route_keys, &broadcast);
/// client_routing.send_join_step(&session, &mut messages_rx, &client_rx, commitment);
/// if let Some(ready_rx) = client_routing.ready_rx() {
///     let ready_answer = ready_rx.await;
///     client_routing.take_ready_answer(ready_answer);
/// }
/// client_routing
///     .change_filter(&session, &mut messages_rx, commitment, commitment_new)
///     .await;
/// client_routing.end_session(session, messages_rx.is_none(), commitment);
/// ```
pub struct ClientRouting<'a> {
    /// The broadcast channels, and the owners' route change senders.
    broadcast: &'a SubscriberChannels,
    /// The client's route template, which each route change copies with the client's filter.
    route_template: Route,
    /// The route keys of the client's filter, or [`None`] when it has none, routing is off, or
    /// the client's owner stopped.
    route_keys: Option<Arc<[RouteKey]>>,
    /// Where the client stands in joining its route while it reads its broadcast receiver.
    route_join_state: RouteJoinState,
    /// Sends the client's last leave when it drops, or [`None`] when routing is off.
    leave_on_drop: Option<LeaveOnDrop>,
}

impl<'a> ClientRouting<'a> {
    /// Creates a client's routing, with no preparation sent.
    ///
    /// # Arguments
    ///
    /// * `session` - The client's session.
    /// * `stream_tx` - The client's Subscribe queue, which its routes queue updates into.
    /// * `route_keys` - The route keys of the client's filter, from
    ///   [`SubscriberChannels::route_keys`].
    /// * `broadcast` - The broadcast channels, and the owners' route change senders.
    ///
    /// # Returns
    ///
    /// The client's routing, [`Unprepared`](RouteJoinState::Unprepared).
    pub fn new(
        session: &ClientSession,
        stream_tx: &SubscribeSender,
        route_keys: Option<Arc<[RouteKey]>>,
        broadcast: &'a SubscriberChannels,
    ) -> Self {
        Self {
            broadcast,
            route_template: Route::new(
                session.id,
                &session.subscriber_id,
                stream_tx,
                &session.cancellation_token,
            ),
            route_keys,
            route_join_state: RouteJoinState::Unprepared,
            leave_on_drop: broadcast.leave_on_drop(session.id),
        }
    }

    /// Sends the next step of the client's join to its owner, at a read that finds its broadcast
    /// receiver and its requests empty, so a queued filter change applies first.
    ///
    /// From [`Unprepared`](RouteJoinState::Unprepared) it sends a [`RouteChange::Prepare`], and
    /// from [`Prepared`](RouteJoinState::Prepared) a [`RouteChange::Join`] with the receiver. It
    /// sends nothing while the client is routed, has no route keys, or waits for the owner's
    /// answer, and a client that waits skips the receiver check and its tail lock.
    ///
    /// # Arguments
    ///
    /// * `session` - The client's session, whose filter the route copies.
    /// * `messages_rx` - The client's broadcast receiver, which the join takes, or [`None`]
    ///   while the client is routed.
    /// * `client_rx` - The client's requests.
    /// * `commitment` - The client's commitment level, whose owner gets the step.
    #[inline]
    pub fn send_join_step(
        &mut self,
        session: &ClientSession,
        messages_rx: &mut Option<broadcast::Receiver<BroadcastedMessage>>,
        client_rx: &mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        commitment: CommitmentLevel,
    ) {
        let (Some(broadcast_rx), Some(route_keys)) = (&*messages_rx, &self.route_keys) else {
            return;
        };
        let awaits_answer = matches!(self.route_join_state, RouteJoinState::Preparing(_));
        if awaits_answer || !broadcast_rx.is_empty() || !client_rx.is_empty() {
            return;
        }
        match self.route_join_state {
            RouteJoinState::Unprepared => {
                let (ready_tx, ready_rx) = oneshot::channel();
                let new_route = self.route_template.with_filter(&session.filter, route_keys);
                let route_change = RouteChange::Prepare(new_route, ready_tx);
                self.broadcast.send_route_change(commitment, route_change);
                self.route_join_state = RouteJoinState::Preparing(ready_rx);
            }
            RouteJoinState::Prepared => {
                let new_route = self.route_template.with_filter(&session.filter, route_keys);
                let route_change = RouteChange::Join(new_route, messages_rx.take());
                self.broadcast.send_route_change(commitment, route_change);
                self.route_join_state = RouteJoinState::Unprepared;
            }
            RouteJoinState::Preparing(_) => {}
        }
    }

    /// Moves the client's route after a filter change.
    ///
    /// The new key list is the held key list [`Arc`] when it lists the same keys. A routed client
    /// whose change keeps the key list and the level replaces the route's filter in place. Any
    /// other change of a routed client leaves the route, and waits for the owner's answer: a
    /// receiver subscribed right after the owner's last routed send, or a new receiver when the
    /// owner has stopped. So the client neither misses nor repeats an update at that level, and
    /// its new filter applies from the next batch. On the broadcast, a change to other keys or
    /// another level leaves the preparation, and the client prepares again.
    ///
    /// # Arguments
    ///
    /// * `session` - The client's session, with its new filter.
    /// * `messages_rx` - The client's broadcast receiver, or [`None`] while the client is
    ///   routed, which gets the owner's answer.
    /// * `commitment` - The client's commitment level before the change.
    /// * `commitment_new` - The commitment level of the new filter.
    pub async fn change_filter(
        &mut self,
        session: &ClientSession,
        messages_rx: &mut Option<broadcast::Receiver<BroadcastedMessage>>,
        commitment: CommitmentLevel,
        commitment_new: CommitmentLevel,
    ) {
        let route_keys_new = self
            .broadcast
            .route_keys(&session.filter, self.route_keys.as_ref());
        let route_moved = commitment_new != commitment || route_keys_new != self.route_keys;
        if messages_rx.is_none() {
            match &route_keys_new {
                Some(route_keys_new) if !route_moved => {
                    let new_route = self
                        .route_template
                        .with_filter(&session.filter, route_keys_new);
                    let route_change = RouteChange::Join(new_route, None);
                    self.broadcast.send_route_change(commitment, route_change);
                }
                _ => {
                    let (answer_tx, answer_rx) = oneshot::channel();
                    let client_leave = RouteLeave {
                        session_id: session.id,
                        receiver_request: Some((commitment, answer_tx)),
                    };
                    let route_change = RouteChange::Leave(client_leave);
                    self.broadcast.send_route_change(commitment, route_change);
                    let leave_answer = answer_rx.await;
                    *messages_rx =
                        Some(leave_answer.unwrap_or_else(|_| self.broadcast.subscribe(commitment)));
                }
            }
        } else if route_moved && !matches!(self.route_join_state, RouteJoinState::Unprepared) {
            let client_leave = RouteLeave {
                session_id: session.id,
                receiver_request: None,
            };
            let route_change = RouteChange::Leave(client_leave);
            self.broadcast.send_route_change(commitment, route_change);
            self.route_join_state = RouteJoinState::Unprepared;
        }
        self.route_keys = route_keys_new;
    }

    /// The receiver of the owner's answer to the client's preparation.
    ///
    /// # Returns
    ///
    /// The receiver while the client waits for the answer, and [`None`] otherwise.
    #[inline]
    pub const fn ready_rx(&mut self) -> Option<&mut oneshot::Receiver<()>> {
        match &mut self.route_join_state {
            RouteJoinState::Preparing(ready_rx) => Some(ready_rx),
            RouteJoinState::Unprepared | RouteJoinState::Prepared => None,
        }
    }

    /// Takes the owner's answer to the client's preparation.
    ///
    /// An answer makes the client [`Prepared`](RouteJoinState::Prepared). An owner that drops the
    /// preparation unanswered has stopped, so the client stays on the broadcast until its filter
    /// changes.
    ///
    /// # Arguments
    ///
    /// * `ready_answer` - What the answer's receiver gave.
    #[inline]
    pub fn take_ready_answer(&mut self, ready_answer: Result<(), oneshot::error::RecvError>) {
        if ready_answer.is_ok() {
            self.route_join_state = RouteJoinState::Prepared;
        } else {
            self.route_keys = None;
            self.route_join_state = RouteJoinState::Unprepared;
        }
    }

    /// Ends the client: an owner's disconnect reason replaces the session's, the session drops,
    /// and then the last leave goes to the owner that holds the client's route, staged or live,
    /// or to no owner when none does.
    ///
    /// The session's drop wakes the client's other tasks, so the owner the leave wakes is the last
    /// task the client's end wakes, and runs next on this worker instead of waiting behind them.
    ///
    /// # Arguments
    ///
    /// * `session` - The client's session.
    /// * `routed` - Whether the client is routed, holding no broadcast receiver.
    /// * `commitment` - The client's commitment level, whose owner holds its route.
    pub fn end_session(
        mut self,
        mut session: ClientSession,
        routed: bool,
        commitment: CommitmentLevel,
    ) {
        if let Some(disconnect_reason) = self.route_template.disconnect_reason() {
            session.disconnect_reason = disconnect_reason;
        }
        if let Some(leave_on_drop) = &mut self.leave_on_drop {
            let owner_holds_route =
                routed || !matches!(self.route_join_state, RouteJoinState::Unprepared);
            leave_on_drop.leave_only(owner_holds_route.then_some(commitment));
        }
        drop(session);
        drop(self.leave_on_drop);
    }
}
