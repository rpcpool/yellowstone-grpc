//! The inner stream of a [`ReconnectStream`](crate::ReconnectStream): one connection's updates,
//! with the [`RECONNECT_FILTER_KEY`] filters read and hidden, and the bank state that outlives
//! the connection.

use {
    crate::{
        reconnect::{ReconnectPolicy, RECONNECT_FILTER_KEY},
        reconnect_stream::Disconnected,
    },
    futures::{ready, stream::Stream, StreamExt},
    std::{
        collections::HashSet,
        pin::Pin,
        task::{Context, Poll},
    },
    tonic::Status,
    yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate},
};

/// Identifies one bank: its slot and the `bank_id` the server's validator gave it.
///
/// A `bank_id` is scoped to one validator process. A reconnect that lands on another server, or
/// on a restarted one, can reuse it for a different bank.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BankRef {
    pub slot: u64,
    pub bank_id: u64,
}

/// What one update tells the [`BankTracker`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Observed {
    /// Part of a bank's content, delivered before its block meta.
    Content(BankRef),
    /// The bank's Block, block footer or block meta. The server sends each of them after all
    /// of the bank's content, so the bank's content is all delivered.
    Sealed(BankRef),
    /// The slot reached confirmed.
    Confirmed(u64),
    /// The slot reached finalized.
    Finalized(u64),
    /// Nothing about bank delivery.
    Other,
}

impl Observed {
    /// Classifies an update by what it says about bank delivery.
    ///
    /// A bank's content is its account writes, transactions, transaction statuses, entries and
    /// CreatedBank status. Its Block, block footer and block meta follow all of them, in that
    /// order, and each seals the bank. The Processed, Confirmed and Finalized statuses come
    /// last, and are not the bank's content.
    ///
    /// # Arguments
    ///
    /// * `update` - The [`SubscribeUpdate`] to classify.
    ///
    /// # Returns
    ///
    /// The [`Observed`] kind of `update`.
    fn of(update: &SubscribeUpdate) -> Self {
        let bank = |slot, bank_id| BankRef { slot, bank_id };
        match update.update_oneof.as_ref() {
            Some(UpdateOneof::Account(m)) => m
                .bank_id
                .map_or(Self::Other, |bank_id| Self::Content(bank(m.slot, bank_id))),
            Some(UpdateOneof::Transaction(m)) => Self::Content(bank(m.slot, m.bank_id)),
            Some(UpdateOneof::TransactionStatus(m)) => Self::Content(bank(m.slot, m.bank_id)),
            Some(UpdateOneof::Entry(m)) => Self::Content(bank(m.slot, m.bank_id)),
            Some(UpdateOneof::Block(m)) => Self::Sealed(bank(m.slot, m.bank_id)),
            Some(UpdateOneof::BlockFooter(m)) => Self::Sealed(bank(m.slot, m.bank_id)),
            Some(UpdateOneof::BlockMeta(m)) => Self::Sealed(bank(m.slot, m.bank_id)),
            Some(UpdateOneof::Slot(m)) => match (SlotStatus::try_from(m.status), m.bank_id) {
                (Ok(SlotStatus::SlotCreatedBank), Some(bank_id)) => {
                    Self::Content(bank(m.slot, bank_id))
                }
                (Ok(SlotStatus::SlotConfirmed), _) => Self::Confirmed(m.slot),
                (Ok(SlotStatus::SlotFinalized), _) => Self::Finalized(m.slot),
                _ => Self::Other,
            },
            Some(
                UpdateOneof::EntryUpdateParent(_) | UpdateOneof::Ping(_) | UpdateOneof::Pong(_),
            )
            | None => Self::Other,
        }
    }
}

/// Removes [`RECONNECT_FILTER_KEY`] from an update's filters.
///
/// # Arguments
///
/// * `update` - The [`SubscribeUpdate`] to clean, in place.
///
/// # Returns
///
/// `true` if a user filter matched `update` too, so the user should see it; `false` if only
/// [`RECONNECT_FILTER_KEY`] matched it.
fn strip_reconnect_filter(update: &mut SubscribeUpdate) -> bool {
    let internal_only = !update.filters.is_empty()
        && update
            .filters
            .iter()
            .all(|filter| filter == RECONNECT_FILTER_KEY);
    update
        .filters
        .retain(|filter| filter != RECONNECT_FILTER_KEY);
    !internal_only
}

/// Which banks the user holds in part, across every connection of one
/// [`ReconnectStream`](crate::ReconnectStream).
///
/// The server sends a bank's Block, block footer and block meta after all of its content, so a
/// bank one of them arrived for was delivered whole. A bank with content but none of them when
/// the connection drops is partial: [`BankTracker::disconnect`] reports it, so the user discards
/// it. The block meta always arrives, through the [`RECONNECT_FILTER_KEY`] filter.
///
/// Every bank at or below a finalized slot is either complete or dead, so the tracker forgets
/// it. That bounds its state to the banks above the last finalized slot.
///
/// # Examples
///
/// ```ignore
/// let mut tracker = BankTracker::default();
/// if let Some(update) = tracker.observe(update) {
///     // deliver `update`
/// }
/// let disconnected = tracker.disconnect(ReconnectPolicy::default());
/// ```
#[derive(Debug, Default)]
pub(crate) struct BankTracker {
    /// Banks with content delivered and not sealed yet.
    open: HashSet<BankRef>,
    /// Sealed banks. A replay resends them, and a disconnect in the middle of that must not
    /// report them: the user already holds them whole.
    complete: HashSet<BankRef>,
    /// Banks reported partial that no replay resends. A later connection can still deliver
    /// their tail, which would look complete once their block meta arrives, so it is dropped.
    discarded: HashSet<BankRef>,
    /// The highest confirmed slot seen, where a replay starts from.
    last_confirmed: Option<u64>,
    /// The highest finalized slot seen. Banks at or below it are settled.
    finalized: Option<u64>,
    /// The first request's `from_slot`: where a replay starts before any confirmed slot is
    /// known.
    requested_from_slot: Option<u64>,
    /// Sealing updates seen, hidden ones included: what the retry budget counts as progress.
    sealed: u64,
}

impl BankTracker {
    /// Starts tracking a subscription.
    ///
    /// # Arguments
    ///
    /// * `requested_from_slot` - The first request's `from_slot`, if it set one.
    ///
    /// # Returns
    ///
    /// A [`BankTracker`] that has seen nothing yet.
    pub(crate) fn new(requested_from_slot: Option<u64>) -> Self {
        Self {
            requested_from_slot,
            ..Self::default()
        }
    }

    /// Records one update and decides whether the user sees it.
    ///
    /// # Arguments
    ///
    /// * `update` - The next [`SubscribeUpdate`] of the current connection.
    ///
    /// # Returns
    ///
    /// `update` without [`RECONNECT_FILTER_KEY`] in its filters, or [`None`] if only
    /// [`RECONNECT_FILTER_KEY`] matched it, or if it belongs to a discarded bank.
    pub(crate) fn observe(&mut self, mut update: SubscribeUpdate) -> Option<SubscribeUpdate> {
        let observed = Observed::of(&update);
        if let Observed::Sealed(_) = observed {
            self.sealed += 1;
        }
        if let Observed::Content(bank) | Observed::Sealed(bank) = observed {
            if self.discarded.contains(&bank) {
                return None;
            }
        }
        let visible = strip_reconnect_filter(&mut update);
        match observed {
            Observed::Content(bank) => {
                if visible && self.is_unsettled(bank) && !self.complete.contains(&bank) {
                    self.open.insert(bank);
                }
            }
            Observed::Sealed(bank) => {
                self.open.remove(&bank);
                if self.is_unsettled(bank) {
                    self.complete.insert(bank);
                }
            }
            Observed::Confirmed(slot) => self.confirm(slot),
            Observed::Finalized(slot) => {
                self.confirm(slot);
                self.finalize(slot);
            }
            Observed::Other => {}
        }
        visible.then_some(update)
    }

    /// Reports the banks the dropped connection left partial, and picks where to replay from.
    ///
    /// A partial bank at or above the replay's `from_slot` comes back whole in the replay.
    /// One below it, or every one without a replay, never does: the tracker drops whatever
    /// a later connection sends of it.
    ///
    /// # Arguments
    ///
    /// * `policy` - The [`ReconnectPolicy`] that picks the `from_slot`.
    ///
    /// # Returns
    ///
    /// A [`Disconnected`] listing the partial banks in slot order, and the replay's `from_slot`.
    pub(crate) fn disconnect(&mut self, policy: ReconnectPolicy) -> Disconnected {
        let replay_from_slot =
            policy.replay_from_slot(self.last_confirmed, self.requested_from_slot);
        let replayed = |bank: &BankRef| replay_from_slot.is_some_and(|from| bank.slot >= from);
        // This replay resends them whole: deliver them again.
        self.discarded.retain(|bank| !replayed(bank));
        let mut partial_banks: Vec<_> = self.open.drain().collect();
        partial_banks.sort_unstable();
        self.discarded
            .extend(partial_banks.iter().filter(|bank| !replayed(bank)));
        Disconnected {
            partial_banks,
            replay_from_slot,
        }
    }

    /// Tells whether a bank can still be partial.
    ///
    /// # Arguments
    ///
    /// * `bank` - The [`BankRef`] to check.
    ///
    /// # Returns
    ///
    /// `false` if `bank` is at or below the last finalized slot: it is then complete or dead.
    fn is_unsettled(&self, bank: BankRef) -> bool {
        self.finalized.is_none_or(|finalized| bank.slot > finalized)
    }

    /// Raises the last confirmed slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - A slot that reached confirmed or finalized.
    fn confirm(&mut self, slot: u64) {
        self.last_confirmed = self.last_confirmed.max(Some(slot));
    }

    /// Raises the last finalized slot and forgets every bank at or below it.
    ///
    /// # Arguments
    ///
    /// * `slot` - A slot that reached finalized.
    fn finalize(&mut self, slot: u64) {
        if self.finalized.is_some_and(|finalized| finalized >= slot) {
            return;
        }
        self.finalized = Some(slot);
        let unsettled = |bank: &BankRef| bank.slot > slot;
        self.open.retain(unsettled);
        self.complete.retain(unsettled);
        self.discarded.retain(unsettled);
    }
}

/// One connection's updates as the user sees them, tracked by a [`BankTracker`] that moves
/// from one connection to the next.
///
/// It yields only the updates [`BankTracker::observe`] lets through, and passes the
/// connection's error or end through unchanged.
///
/// # Examples
///
/// ```ignore
/// let mut stream = TrackedStream::new(connection, BankTracker::default());
/// while let Some(Ok(update)) = stream.next().await {
///     // deliver `update`
/// }
/// let made_progress = stream.sealed_a_bank();
/// let tracker = stream.into_tracker();
/// ```
pub(crate) struct TrackedStream<S> {
    stream: S,
    tracker: BankTracker,
    /// The tracker's sealed count when this connection started.
    sealed_at_start: u64,
}

impl<S> TrackedStream<S> {
    /// Starts tracking a new connection.
    ///
    /// # Arguments
    ///
    /// * `stream` - The connection's raw updates.
    /// * `tracker` - The [`BankTracker`] of the previous connection, or a fresh one.
    ///
    /// # Returns
    ///
    /// A [`TrackedStream`] that has received nothing yet.
    pub(crate) const fn new(stream: S, tracker: BankTracker) -> Self {
        Self {
            stream,
            sealed_at_start: tracker.sealed,
            tracker,
        }
    }

    /// Tells whether the connection made progress: it delivered a bank's Block, block footer
    /// or block meta, hidden ones included. A ping or a slot status alone does not count: a
    /// server that accepts, sends one, then drops makes no progress.
    ///
    /// # Returns
    ///
    /// `true` once the connection sealed a bank.
    pub(crate) const fn sealed_a_bank(&self) -> bool {
        self.tracker.sealed > self.sealed_at_start
    }

    /// Drops the connection and keeps its bank state for the next one.
    ///
    /// # Returns
    ///
    /// The [`BankTracker`] with every update this connection delivered recorded.
    pub(crate) fn into_tracker(self) -> BankTracker {
        self.tracker
    }
}

impl<S> Stream for TrackedStream<S>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match ready!(this.stream.poll_next_unpin(cx)) {
                Some(Ok(update)) => {
                    if let Some(update) = this.tracker.observe(update) {
                        return Poll::Ready(Some(Ok(update)));
                    }
                }
                ended => return Poll::Ready(ended),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        futures::stream,
        yellowstone_grpc_proto::prelude::{
            SubscribeUpdateAccount, SubscribeUpdateBlock, SubscribeUpdateBlockFooter,
            SubscribeUpdateBlockMeta, SubscribeUpdatePing, SubscribeUpdateSlot,
            SubscribeUpdateTransaction,
        },
    };

    const USER: &str = "user";

    fn bank(slot: u64, bank_id: u64) -> BankRef {
        BankRef { slot, bank_id }
    }

    fn update(filters: &[&str], oneof: UpdateOneof) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: filters.iter().map(|filter| (*filter).to_owned()).collect(),
            update_oneof: Some(oneof),
            created_at: None,
        }
    }

    fn account(slot: u64, bank_id: u64) -> SubscribeUpdate {
        update(
            &[USER],
            UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                bank_id: Some(bank_id),
                ..Default::default()
            }),
        )
    }

    fn transaction(slot: u64, bank_id: u64) -> SubscribeUpdate {
        update(
            &[USER],
            UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot,
                bank_id,
                ..Default::default()
            }),
        )
    }

    fn block_meta(filters: &[&str], slot: u64, bank_id: u64) -> SubscribeUpdate {
        update(
            filters,
            UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                bank_id,
                ..Default::default()
            }),
        )
    }

    fn status(filters: &[&str], slot: u64, bank_id: u64, status: SlotStatus) -> SubscribeUpdate {
        update(
            filters,
            UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                bank_id: Some(bank_id),
                status: status as i32,
                ..Default::default()
            }),
        )
    }

    fn internal_meta(slot: u64, bank_id: u64) -> SubscribeUpdate {
        block_meta(&[RECONNECT_FILTER_KEY], slot, bank_id)
    }

    fn confirmed(slot: u64) -> SubscribeUpdate {
        status(
            &[RECONNECT_FILTER_KEY],
            slot,
            slot,
            SlotStatus::SlotConfirmed,
        )
    }

    fn finalized(slot: u64) -> SubscribeUpdate {
        status(
            &[RECONNECT_FILTER_KEY],
            slot,
            slot,
            SlotStatus::SlotFinalized,
        )
    }

    const REPLAY: ReconnectPolicy = ReconnectPolicy::Replay { slots_behind: 0 };

    #[test]
    fn hides_updates_only_the_reconnect_filter_matched() {
        let mut tracker = BankTracker::default();
        assert_eq!(tracker.observe(internal_meta(10, 1)), None);
        assert_eq!(tracker.observe(confirmed(10)), None);
    }

    #[test]
    fn strips_the_reconnect_filter_from_shared_updates() {
        let mut tracker = BankTracker::default();
        let seen = tracker
            .observe(block_meta(&[RECONNECT_FILTER_KEY, USER], 10, 1))
            .expect("a user filter matched it");
        assert_eq!(seen.filters, vec![USER.to_owned()]);
    }

    #[test]
    fn passes_updates_without_filters() {
        let mut tracker = BankTracker::default();
        let ping = update(&[], UpdateOneof::Ping(SubscribeUpdatePing {}));
        assert_eq!(tracker.observe(ping.clone()), Some(ping));
    }

    #[test]
    fn a_bank_without_block_meta_is_partial() {
        let mut tracker = BankTracker::default();
        tracker.observe(account(10, 1));
        tracker.observe(transaction(11, 2));
        tracker.observe(internal_meta(11, 2));

        let disconnected = tracker.disconnect(REPLAY);
        assert_eq!(disconnected.partial_banks, vec![bank(10, 1)]);
    }

    #[test]
    fn partial_banks_are_reported_once_in_slot_order() {
        let mut tracker = BankTracker::default();
        tracker.observe(account(12, 3));
        tracker.observe(account(10, 1));
        tracker.observe(account(10, 2));

        assert_eq!(
            tracker.disconnect(REPLAY).partial_banks,
            vec![bank(10, 1), bank(10, 2), bank(12, 3)]
        );
        assert!(tracker.disconnect(REPLAY).partial_banks.is_empty());
    }

    #[test]
    fn a_created_bank_status_is_content() {
        let mut tracker = BankTracker::default();
        tracker.observe(status(&[USER], 10, 1, SlotStatus::SlotCreatedBank));
        assert_eq!(tracker.disconnect(REPLAY).partial_banks, vec![bank(10, 1)]);
    }

    fn block(slot: u64, bank_id: u64) -> SubscribeUpdate {
        update(
            &[USER],
            UpdateOneof::Block(SubscribeUpdateBlock {
                slot,
                bank_id,
                ..Default::default()
            }),
        )
    }

    fn footer(slot: u64, bank_id: u64) -> SubscribeUpdate {
        update(
            &[USER],
            UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
                slot,
                bank_id,
                ..Default::default()
            }),
        )
    }

    #[test]
    fn a_block_alone_is_never_partial() {
        let mut tracker = BankTracker::default();
        assert!(tracker.observe(block(10, 1)).is_some());
        assert!(tracker
            .disconnect(ReconnectPolicy::NoReplay)
            .partial_banks
            .is_empty());
        // Its block meta is not a discarded bank's tail: it still goes out.
        assert!(tracker.observe(block_meta(&[USER], 10, 1)).is_some());
    }

    #[test]
    fn a_footer_alone_is_never_partial() {
        let mut tracker = BankTracker::default();
        assert!(tracker.observe(footer(10, 1)).is_some());
        assert!(tracker
            .disconnect(ReconnectPolicy::NoReplay)
            .partial_banks
            .is_empty());
        assert!(tracker.observe(block_meta(&[USER], 10, 1)).is_some());
    }

    #[test]
    fn a_block_or_footer_seals_the_content_before_it() {
        let mut tracker = BankTracker::default();
        tracker.observe(account(10, 1));
        tracker.observe(block(10, 1));
        tracker.observe(transaction(11, 2));
        tracker.observe(footer(11, 2));
        tracker.observe(account(12, 3));
        assert_eq!(tracker.disconnect(REPLAY).partial_banks, vec![bank(12, 3)]);
    }

    #[test]
    fn statuses_after_block_meta_do_not_reopen_a_bank() {
        let mut tracker = BankTracker::default();
        tracker.observe(account(10, 1));
        tracker.observe(internal_meta(10, 1));
        tracker.observe(status(&[USER], 10, 1, SlotStatus::SlotProcessed));
        tracker.observe(status(&[USER], 10, 1, SlotStatus::SlotConfirmed));
        assert!(tracker.disconnect(REPLAY).partial_banks.is_empty());
    }

    #[test]
    fn hidden_content_does_not_make_a_bank_partial() {
        let mut tracker = BankTracker::default();
        let created = status(&[RECONNECT_FILTER_KEY], 10, 1, SlotStatus::SlotCreatedBank);
        assert_eq!(tracker.observe(created), None);
        assert!(tracker.disconnect(REPLAY).partial_banks.is_empty());
    }

    #[test]
    fn replay_starts_at_the_last_confirmed_slot() {
        let mut tracker = BankTracker::default();
        assert_eq!(tracker.disconnect(REPLAY).replay_from_slot, None);

        tracker.observe(confirmed(10));
        tracker.observe(confirmed(8));
        assert_eq!(tracker.disconnect(REPLAY).replay_from_slot, Some(10));

        tracker.observe(finalized(12));
        let policy = ReconnectPolicy::Replay { slots_behind: 4 };
        assert_eq!(tracker.disconnect(policy).replay_from_slot, Some(8));
        assert_eq!(
            tracker
                .disconnect(ReconnectPolicy::NoReplay)
                .replay_from_slot,
            None
        );
    }

    #[test]
    fn a_complete_bank_replayed_in_part_is_not_reported() {
        let mut tracker = BankTracker::default();
        tracker.observe(confirmed(9));
        tracker.observe(account(10, 1));
        tracker.observe(internal_meta(10, 1));
        assert!(tracker.disconnect(REPLAY).partial_banks.is_empty());

        // The replay resends bank 10 and drops before its block meta.
        assert!(tracker.observe(account(10, 1)).is_some());
        assert!(tracker.disconnect(REPLAY).partial_banks.is_empty());
    }

    #[test]
    fn a_replayed_partial_bank_is_delivered_again() {
        let mut tracker = BankTracker::default();
        tracker.observe(confirmed(9));
        tracker.observe(account(10, 1));
        let disconnected = tracker.disconnect(REPLAY);
        assert_eq!(disconnected.partial_banks, vec![bank(10, 1)]);
        assert_eq!(disconnected.replay_from_slot, Some(9));

        assert!(tracker.observe(account(10, 1)).is_some());
        assert!(tracker.observe(block_meta(&[USER], 10, 1)).is_some());
    }

    #[test]
    fn the_tail_of_a_bank_no_replay_resends_is_dropped() {
        let mut tracker = BankTracker::default();
        tracker.observe(confirmed(9));
        tracker.observe(account(10, 1));
        tracker.disconnect(ReconnectPolicy::NoReplay);

        assert_eq!(tracker.observe(transaction(10, 1)), None);
        assert_eq!(tracker.observe(block_meta(&[USER], 10, 1)), None);
        // Statuses are not the bank's content: they still go out.
        assert!(tracker
            .observe(status(&[USER], 10, 1, SlotStatus::SlotProcessed))
            .is_some());
        // Other banks are unaffected.
        assert!(tracker.observe(account(10, 2)).is_some());
        assert!(tracker.observe(account(11, 3)).is_some());
    }

    #[test]
    fn the_tail_of_a_bank_below_the_replay_is_dropped() {
        let mut tracker = BankTracker::default();
        tracker.observe(account(10, 1));
        tracker.observe(confirmed(20));
        tracker.disconnect(REPLAY);

        assert_eq!(tracker.observe(account(10, 1)), None);
    }

    #[test]
    fn before_any_confirmed_slot_a_replay_starts_at_the_requested_from_slot() {
        let mut tracker = BankTracker::new(Some(5));
        tracker.observe(account(10, 1));
        let disconnected = tracker.disconnect(REPLAY);
        assert_eq!(disconnected.partial_banks, vec![bank(10, 1)]);
        assert_eq!(disconnected.replay_from_slot, Some(5));
        // The replay resends the partial bank: it is delivered again, not dropped.
        assert!(tracker.observe(account(10, 1)).is_some());

        tracker.observe(confirmed(20));
        assert_eq!(tracker.disconnect(REPLAY).replay_from_slot, Some(20));
        assert_eq!(
            tracker
                .disconnect(ReconnectPolicy::NoReplay)
                .replay_from_slot,
            None
        );
    }

    #[test]
    fn a_later_replay_that_covers_a_discarded_bank_delivers_it_again() {
        let mut tracker = BankTracker::default();
        tracker.observe(account(10, 1));
        // No confirmed slot yet: nothing to replay from.
        assert_eq!(tracker.disconnect(REPLAY).replay_from_slot, None);
        assert_eq!(tracker.observe(account(10, 1)), None);

        tracker.observe(confirmed(9));
        assert_eq!(tracker.disconnect(REPLAY).replay_from_slot, Some(9));
        assert!(tracker.observe(account(10, 1)).is_some());
    }

    #[test]
    fn finality_forgets_settled_banks() {
        let mut tracker = BankTracker::default();
        tracker.observe(account(10, 1));
        tracker.observe(account(11, 2));
        tracker.observe(account(12, 3));
        tracker.observe(internal_meta(12, 3));
        tracker.observe(finalized(11));

        assert!(tracker.complete.contains(&bank(12, 3)));
        assert_eq!(tracker.disconnect(REPLAY).partial_banks, vec![]);

        // A replay below the finalized slot never opens a bank again.
        tracker.observe(account(11, 2));
        assert!(tracker.open.is_empty());
    }

    /// Adapted from `refactor/auto-reconnect`: two banks in one slot, one complete and one
    /// cut off. Only the cut one is partial.
    #[test]
    fn a_complete_bank_in_the_slot_of_a_partial_one_is_not_reported() {
        let mut tracker = BankTracker::default();
        tracker.observe(finalized(100));
        tracker.observe(account(101, 1));
        tracker.observe(internal_meta(101, 1));
        tracker.observe(account(101, 2));
        assert_eq!(tracker.disconnect(REPLAY).partial_banks, vec![bank(101, 2)]);
    }

    #[test]
    fn finality_never_moves_backwards() {
        let mut tracker = BankTracker::default();
        tracker.observe(finalized(20));
        tracker.observe(finalized(10));
        tracker.observe(account(15, 1));
        assert!(tracker.disconnect(REPLAY).partial_banks.is_empty());
    }

    #[tokio::test]
    async fn tracked_stream_yields_visible_updates_and_passes_the_end_through() {
        let source = stream::iter(vec![
            Ok(account(10, 1)),
            Ok(confirmed(10)),
            Ok(internal_meta(9, 9)),
            Err(Status::unavailable("drop")),
        ]);
        let mut stream = TrackedStream::new(source, BankTracker::default());

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first, account(10, 1));
        // Content and a status are not progress; the hidden block meta is.
        assert!(!stream.sealed_a_bank());
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(stream.sealed_a_bank());

        let mut tracker = stream.into_tracker();
        let disconnected = tracker.disconnect(REPLAY);
        assert_eq!(disconnected.partial_banks, vec![bank(10, 1)]);
        assert_eq!(disconnected.replay_from_slot, Some(10));
    }

    #[test]
    fn progress_counts_only_from_the_connection_start() {
        let mut tracker = BankTracker::default();
        tracker.observe(internal_meta(9, 9));
        let stream =
            TrackedStream::new(stream::empty::<Result<SubscribeUpdate, Status>>(), tracker);
        assert!(!stream.sealed_a_bank());
    }
}
