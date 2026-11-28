//! A Subscribe client's side of the index: where it stands in joining the index, and the index
//! changes it sends.

#![allow(rustdoc::private_intra_doc_links)]

#[cfg(doc)]
use crate::grpc::GrpcService;
use {
    crate::{
        grpc::{
            BroadcastedMessage, ClientRequest, ClientSession, SubscribeSender, SubscriberChannels,
        },
        plugin::{filter::Filter, message::CommitmentLevel},
        reverse_index::{IndexChange, IndexKey, IndexLeave, IndexedSubscriber, LeaveOnDrop},
    },
    std::sync::Arc,
    tokio::sync::{broadcast, mpsc, oneshot},
};

/// Where a client on the broadcast stands in joining the index.
///
/// [`GrpcService::client_loop`] keeps it while the client reads a broadcast receiver and its
/// filter has index keys. At a read that finds its receiver and its requests empty, the client
/// sends an [`IndexChange::Prepare`] from `Unprepared`, and an [`IndexChange::Join`] with the
/// receiver from `Prepared`.
enum IndexJoinState {
    /// No preparation of the client's indexed subscriber waits at its owner.
    Unprepared,
    /// The owner claims the indexed subscriber's keys, and answers on this receiver once all are
    /// claimed.
    Preparing(oneshot::Receiver<()>),
    /// The owner has claimed every key, so the client hands its receiver over next.
    Prepared,
}

/// A Subscribe client's side of the index: the template its indexed subscribers copy, its index
/// keys, where it stands in joining the index, and its last leave.
///
/// [`GrpcService::client_loop`] makes it where the client's live stream starts. It sends the two
/// steps of the client's join with [`ClientIndexing::send_join_step`], moves the indexed subscriber
/// when a filter change applies with [`ClientIndexing::change_filter`], and ends the client with
/// [`ClientIndexing::end_session`].
///
/// # Examples
///
/// ```ignore
/// let mut client_indexing = ClientIndexing::new(&session, &stream_tx, index_keys, &broadcast);
/// client_indexing.send_join_step(&session, &mut messages_rx, &client_rx, commitment);
/// if let Some(ready_rx) = client_indexing.ready_rx() {
///     let ready_answer = ready_rx.await;
///     client_indexing.take_ready_answer(ready_answer);
/// }
/// client_indexing
///     .change_filter(&session, &mut messages_rx, commitment, commitment_new)
///     .await;
/// client_indexing.end_session(session, messages_rx.is_none(), commitment);
/// ```
pub struct ClientIndexing<'a> {
    /// The broadcast channels, and the owners' index change senders.
    broadcast: &'a SubscriberChannels,
    /// The client's subscriber template, which each index change copies with the client's filter.
    subscriber_template: IndexedSubscriber,
    /// The index keys of the client's filter, or [`None`] when it has none, indexing is off, or
    /// the client's owner stopped.
    index_keys: Option<Arc<[IndexKey]>>,
    /// Where the client stands in joining the index while it reads its broadcast receiver.
    index_join_state: IndexJoinState,
    /// Sends the client's last leave when it drops, or [`None`] when indexing is off.
    leave_on_drop: Option<LeaveOnDrop>,
}

impl<'a> ClientIndexing<'a> {
    /// Creates a client's indexing, with no preparation sent.
    ///
    /// # Arguments
    ///
    /// * `session` - The client's session.
    /// * `stream_tx` - The client's Subscribe queue, which its indexed subscribers queue updates
    ///   into.
    /// * `index_keys` - The index keys of the client's filter, from
    ///   [`SubscriberChannels::index_keys`].
    /// * `broadcast` - The broadcast channels, and the owners' index change senders.
    ///
    /// # Returns
    ///
    /// The client's indexing, [`Unprepared`](IndexJoinState::Unprepared).
    pub fn new(
        session: &ClientSession,
        stream_tx: &SubscribeSender,
        index_keys: Option<Arc<[IndexKey]>>,
        broadcast: &'a SubscriberChannels,
    ) -> Self {
        Self {
            broadcast,
            subscriber_template: IndexedSubscriber::new(
                session.id,
                &session.subscriber_id,
                stream_tx,
                &session.cancellation_token,
            ),
            index_keys,
            index_join_state: IndexJoinState::Unprepared,
            leave_on_drop: broadcast.leave_on_drop(session.id),
        }
    }

    /// Sends the next step of the client's join to its owner, at a read that finds its broadcast
    /// receiver and its requests empty, so a queued filter change applies first.
    ///
    /// From [`Unprepared`](IndexJoinState::Unprepared) it sends an [`IndexChange::Prepare`], and
    /// from [`Prepared`](IndexJoinState::Prepared) an [`IndexChange::Join`] with the receiver. A
    /// join that its owner can no longer take, since the owner has stopped, gives the receiver
    /// back. It sends nothing while the client is indexed, has no index keys, or waits for the
    /// owner's answer, and a client that waits skips the receiver check and its tail lock.
    ///
    /// # Arguments
    ///
    /// * `session` - The client's session, whose filter the indexed subscriber copies.
    /// * `messages_rx` - The client's broadcast receiver, which the join takes, or [`None`]
    ///   while the client is indexed.
    /// * `client_rx` - The client's requests.
    /// * `commitment` - The client's commitment level, whose owner gets the step.
    #[inline]
    pub fn send_join_step(
        &mut self,
        session: &ClientSession,
        messages_rx: &mut Option<broadcast::Receiver<BroadcastedMessage>>,
        client_rx: &mpsc::UnboundedReceiver<ClientRequest<(Option<u64>, Filter)>>,
        commitment: CommitmentLevel,
    ) {
        let (Some(broadcast_rx), Some(index_keys)) = (&*messages_rx, &self.index_keys) else {
            return;
        };
        let awaits_answer = matches!(self.index_join_state, IndexJoinState::Preparing(_));
        if awaits_answer || !broadcast_rx.is_empty() || !client_rx.is_empty() {
            return;
        }
        match self.index_join_state {
            IndexJoinState::Unprepared => {
                let (ready_tx, ready_rx) = oneshot::channel();
                let new_subscriber = self
                    .subscriber_template
                    .with_filter(&session.filter, index_keys);
                let index_change = IndexChange::Prepare(new_subscriber, ready_tx);
                self.broadcast.send_index_change(commitment, index_change);
                self.index_join_state = IndexJoinState::Preparing(ready_rx);
            }
            IndexJoinState::Prepared => {
                let new_subscriber = self
                    .subscriber_template
                    .with_filter(&session.filter, index_keys);
                let index_change = IndexChange::Join(new_subscriber, messages_rx.take());
                let unsent_change = self.broadcast.send_index_change(commitment, index_change);
                // A stopped owner cannot take the join: the client keeps reading its receiver.
                if let Some(IndexChange::Join(_, broadcast_rx)) = unsent_change {
                    *messages_rx = broadcast_rx;
                }
                self.index_join_state = IndexJoinState::Unprepared;
            }
            IndexJoinState::Preparing(_) => {}
        }
    }

    /// Moves the client's indexed subscriber after a filter change.
    ///
    /// The new key list is the held key list [`Arc`] when it lists the same keys. An indexed client
    /// whose change keeps the key list and the level replaces the indexed subscriber's filter in
    /// place, or takes a new receiver when the owner has stopped. Any other change of an indexed
    /// client leaves the index, and waits for the owner's answer: a receiver subscribed right after
    /// the owner's last indexed send, or a new receiver when the owner has stopped. So the client
    /// neither misses nor repeats an update at that level, and its new filter applies from the next
    /// batch. On the broadcast, a change to other keys or another level leaves the preparation, and
    /// the client prepares again.
    ///
    /// # Arguments
    ///
    /// * `session` - The client's session, with its new filter.
    /// * `messages_rx` - The client's broadcast receiver, or [`None`] while the client is
    ///   indexed, which gets the owner's answer.
    /// * `commitment` - The client's commitment level before the change.
    /// * `commitment_new` - The commitment level of the new filter.
    pub async fn change_filter(
        &mut self,
        session: &ClientSession,
        messages_rx: &mut Option<broadcast::Receiver<BroadcastedMessage>>,
        commitment: CommitmentLevel,
        commitment_new: CommitmentLevel,
    ) {
        let index_keys_new = self
            .broadcast
            .index_keys(&session.filter, self.index_keys.as_ref());
        let subscriber_moved = commitment_new != commitment || index_keys_new != self.index_keys;
        if messages_rx.is_none() {
            match &index_keys_new {
                Some(index_keys_new) if !subscriber_moved => {
                    let new_subscriber = self
                        .subscriber_template
                        .with_filter(&session.filter, index_keys_new);
                    let index_change = IndexChange::Join(new_subscriber, None);
                    if self
                        .broadcast
                        .send_index_change(commitment, index_change)
                        .is_some()
                    {
                        *messages_rx = Some(self.broadcast.subscribe(commitment));
                    }
                }
                _ => {
                    let (answer_tx, answer_rx) = oneshot::channel();
                    let client_leave = IndexLeave {
                        session_id: session.id,
                        receiver_request: Some((commitment, answer_tx)),
                    };
                    let index_change = IndexChange::Leave(client_leave);
                    self.broadcast.send_index_change(commitment, index_change);
                    let leave_answer = answer_rx.await;
                    *messages_rx =
                        Some(leave_answer.unwrap_or_else(|_| self.broadcast.subscribe(commitment)));
                }
            }
        } else if subscriber_moved && !matches!(self.index_join_state, IndexJoinState::Unprepared) {
            let client_leave = IndexLeave {
                session_id: session.id,
                receiver_request: None,
            };
            let index_change = IndexChange::Leave(client_leave);
            self.broadcast.send_index_change(commitment, index_change);
            self.index_join_state = IndexJoinState::Unprepared;
        }
        self.index_keys = index_keys_new;
    }

    /// The receiver of the owner's answer to the client's preparation.
    ///
    /// # Returns
    ///
    /// The receiver while the client waits for the answer, and [`None`] otherwise.
    #[inline]
    pub const fn ready_rx(&mut self) -> Option<&mut oneshot::Receiver<()>> {
        match &mut self.index_join_state {
            IndexJoinState::Preparing(ready_rx) => Some(ready_rx),
            IndexJoinState::Unprepared | IndexJoinState::Prepared => None,
        }
    }

    /// Takes the owner's answer to the client's preparation.
    ///
    /// An answer makes the client [`Prepared`](IndexJoinState::Prepared). An owner that drops the
    /// preparation unanswered has stopped, so the client stays on the broadcast until its filter
    /// changes.
    ///
    /// # Arguments
    ///
    /// * `ready_answer` - What the answer's receiver gave.
    #[inline]
    pub fn take_ready_answer(&mut self, ready_answer: Result<(), oneshot::error::RecvError>) {
        if ready_answer.is_ok() {
            self.index_join_state = IndexJoinState::Prepared;
        } else {
            self.index_keys = None;
            self.index_join_state = IndexJoinState::Unprepared;
        }
    }

    /// Ends the client: an owner's disconnect reason replaces the session's, the session drops,
    /// and then the last leave goes to the owner that holds the client's indexed subscriber, staged
    /// or live, or to no owner when none does.
    ///
    /// The session's drop wakes the client's other tasks, so the owner the leave wakes is the last
    /// task the client's end wakes, and runs next on this worker instead of waiting behind them.
    ///
    /// # Arguments
    ///
    /// * `session` - The client's session.
    /// * `is_indexed` - Whether the client is indexed, holding no broadcast receiver.
    /// * `commitment` - The client's commitment level, whose owner holds its indexed subscriber.
    pub fn end_session(
        mut self,
        mut session: ClientSession,
        is_indexed: bool,
        commitment: CommitmentLevel,
    ) {
        if let Some(disconnect_reason) = self.subscriber_template.disconnect_reason() {
            session.disconnect_reason = disconnect_reason;
        }
        if let Some(leave_on_drop) = &mut self.leave_on_drop {
            let owner_holds_subscriber =
                is_indexed || !matches!(self.index_join_state, IndexJoinState::Unprepared);
            leave_on_drop.leave_only(owner_holds_subscriber.then_some(commitment));
        }
        drop(session);
        drop(self.leave_on_drop);
    }
}
