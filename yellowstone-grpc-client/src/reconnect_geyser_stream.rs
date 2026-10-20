//! [`ReconnectGeyserStream`] and the bank tracking behind it: across reconnects, each bank is
//! delivered complete once or retracted before its replacement updates.

use {
    crate::reconnect::{
        self, AutoReconnect, DiscardReason, ReplacementReplay, SlotWinner, TonicGrpcConnector,
    },
    futures::{ready, stream::Stream},
    std::{
        collections::{HashSet, VecDeque},
        pin::Pin,
        task::{Context, Poll},
    },
    tonic::{codec::Streaming, Status},
    yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
};

/// Identifies a bank within this subscription's connection history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BankRef {
    pub generation: u64,
    pub slot: u64,
    pub bank_id: u64,
}

impl BankRef {
    pub(crate) fn from_update(generation: u64, update: &SubscribeUpdate) -> Option<Self> {
        let (slot, bank_id) = match update.update_oneof.as_ref()? {
            UpdateOneof::Account(m) => (m.slot, m.bank_id?),
            UpdateOneof::Slot(m) => (m.slot, m.bank_id?),
            UpdateOneof::Transaction(m) => (m.slot, m.bank_id),
            UpdateOneof::TransactionStatus(m) => (m.slot, m.bank_id),
            UpdateOneof::Entry(m) => (m.slot, m.bank_id),
            UpdateOneof::Block(m) => (m.slot, m.bank_id),
            UpdateOneof::BlockMeta(m) => (m.slot, m.bank_id),
            UpdateOneof::BlockFooter(m) => (m.slot, m.bank_id),
            _ => return None,
        };
        Some(Self {
            generation,
            slot,
            bank_id,
        })
    }
}

#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "Keep subscription updates inline to avoid an allocation per message"
)]
pub enum ReconnectEvent {
    Update {
        generation: u64,
        update: SubscribeUpdate,
    },

    /// Remove the listed bank state, then rebuild it from the specified replacement stream.
    /// Winner identities come from finality evidence on the replacement connection.
    DiscardBanks {
        /// Exact connection-scoped banks to remove; do not discard other banks in their slots.
        banks: Vec<BankRef>,
        reason: DiscardReason,
        /// Replacement updates must follow this event. Replay failure terminates with an error.
        replacement: ReplacementReplay,
        /// One outcome per distinct slot in `banks`; skipped slots have an unknown winner.
        winners: Vec<SlotWinner>,
    },
}

/// Immediate subscription updates with connection-scoped bank identities.
///
/// Reconnects invalidate retained banks in the requested replay range before replacement updates.
/// Recovery waits for finalized block identities or proven skipped slots before emitting a discard.
pub struct ReconnectStream<S> {
    pub(crate) inner: S,
    pub(crate) generation: u64,
    pub(crate) delivered_banks: HashSet<BankRef>,
    pub(crate) bank_limit: usize,
    pub(crate) dedup: crate::dedup::CompleteBankDedup,
    pub(crate) pending: VecDeque<SubscribeUpdate>,
    pub(crate) recovery: Option<reconnect::RecoveryDecision>,
    pub(crate) finalized: Option<u64>,
    pub(crate) stopped: bool,
}

impl<S> ReconnectStream<S> {
    fn deliver(&mut self, mut update: SubscribeUpdate) -> Result<Option<ReconnectEvent>, Status> {
        let bank = BankRef::from_update(self.generation, &update);
        let visible = reconnect::visible_update(&mut update);
        if visible {
            if matches!(update.update_oneof.as_ref(), Some(UpdateOneof::Account(account)) if account.bank_id.is_none())
            {
                return Err(Status::failed_precondition(
                    "cannot track an account update without bank_id",
                ));
            }
            if let Some(bank) = bank {
                if !self.delivered_banks.contains(&bank)
                    && self.delivered_banks.len() >= self.bank_limit
                {
                    return Err(Status::resource_exhausted(
                        "bank tracking limit reached; cannot forget retained banks before recovery",
                    ));
                }
                self.delivered_banks.insert(bank);
            }
        }
        if let Some(bank) = bank.filter(|bank| self.delivered_banks.contains(bank)) {
            self.dedup.delivered(bank, &update);
        }
        Ok(visible.then_some(ReconnectEvent::Update {
            generation: self.generation,
            update,
        }))
    }

    /// Every bank at or below a finalized slot is decided: the finalized bank is complete and
    /// the others are dead forks or cleared by an update parent. The next reconnect starts
    /// after the finalized slot, or at the earliest bank still waiting for recovery, so the
    /// replay boundary stays inside the server's replay window. Only state below that boundary
    /// is dropped: a replay from the boundary resends every complete bank above it, and those
    /// must still be recognised as duplicates.
    fn settle_finalized(&mut self, slot: u64)
    where
        S: crate::dedup::ReconnectCounter,
    {
        if self.finalized.is_some_and(|finalized| finalized >= slot) {
            return;
        }
        self.finalized = Some(slot);
        let recovering = self
            .recovery
            .as_ref()
            .map(|recovery| recovery.banks.as_slice())
            .unwrap_or_default();
        let boundary = recovering
            .iter()
            .map(|bank| bank.slot)
            .fold(slot.saturating_add(1), u64::min);
        // Every recovering bank is at or above the boundary, so this keeps them too.
        self.delivered_banks.retain(|bank| bank.slot >= boundary);
        self.dedup.prune_before(boundary);
        self.inner.settle_before(boundary);
    }

    fn begin_replay(&mut self, generation: u64, from_slot: Option<u64>) -> Result<(), Status> {
        if !self.delivered_banks.is_empty() && from_slot.is_none() {
            return Err(Status::failed_precondition(
                "reconnected without a replay boundary for delivered banks",
            ));
        }
        self.generation = generation;
        self.recovery = None;
        self.pending.clear();
        if let Some(from_slot) = from_slot {
            let mut banks: Vec<_> = self
                .delivered_banks
                .iter()
                .copied()
                .filter(|bank| bank.slot >= from_slot && !self.dedup.is_complete(bank))
                .collect();
            banks.sort_unstable_by_key(|bank| (bank.slot, bank.generation, bank.bank_id));
            self.dedup.begin_replay(&banks);
            if !banks.is_empty() {
                self.recovery = Some(reconnect::RecoveryDecision::new(
                    banks,
                    ReplacementReplay {
                        from_slot,
                        generation,
                    },
                    self.bank_limit,
                ));
            }
        }
        Ok(())
    }
}

impl<S: crate::dedup::ReconnectCounter> ReconnectStream<S> {
    pub fn new(inner: S) -> Self {
        Self::with_bank_limit(inner, 65_536)
    }

    /// Stop before delivering an untrackable bank rather than evict recovery state.
    pub fn with_bank_limit(inner: S, bank_limit: usize) -> Self {
        let generation = u64::from(inner.reconnect_count());
        Self {
            inner,
            generation,
            delivered_banks: HashSet::new(),
            bank_limit,
            dedup: Default::default(),
            pending: Default::default(),
            recovery: None,
            finalized: None,
            stopped: false,
        }
    }
}

impl<S> ReconnectStream<S>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + crate::dedup::ReconnectCounter + Unpin,
{
    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<ReconnectEvent, Status>> {
        for _ in 0..64 {
            if self.recovery.is_none() {
                if let Some(update) = self.pending.pop_front() {
                    if let Some(event) = self.deliver(update)? {
                        return Poll::Ready(Ok(event));
                    }
                    continue;
                }
            }
            let polled = Pin::new(&mut self.inner).poll_next(cx);
            let generation = u64::from(self.inner.reconnect_count());
            if generation != self.generation && !matches!(polled, Poll::Ready(Some(Err(_)))) {
                self.begin_replay(generation, self.inner.replay_from_slot())?;
            }
            let update = ready!(polled).ok_or_else(|| {
                Status::failed_precondition(
                    "subscription ended before reconnect recovery completed",
                )
            })??;
            if let Some(recovery) = &mut self.recovery {
                recovery.observe(&update)?;
            }
            if let Some(slot) = reconnect::finalized_slot(&update) {
                self.settle_finalized(slot);
            }
            self.dedup
                .filter(self.generation, update, &mut self.pending);
            if let Some(winners) = self
                .recovery
                .as_ref()
                .and_then(reconnect::RecoveryDecision::winners)
            {
                let recovery = self.recovery.take().unwrap();
                for bank in &recovery.banks {
                    self.delivered_banks.remove(bank);
                }
                return Poll::Ready(Ok(ReconnectEvent::DiscardBanks {
                    banks: recovery.banks,
                    reason: DiscardReason::IncompleteDelivery,
                    replacement: recovery.replacement,
                    winners,
                }));
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl<S> Stream for ReconnectStream<S>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + crate::dedup::ReconnectCounter + Unpin,
{
    type Item = Result<ReconnectEvent, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.stopped {
            return Poll::Ready(None);
        }
        let result = ready!(this.poll_event(cx));
        if result.is_err() {
            this.stopped = true;
            this.recovery = None;
            this.pending.clear();
        }
        Poll::Ready(Some(result))
    }
}

/// Stream returned by [`crate::GeyserGrpcClient::subscribe_with_reconnect`]. Across reconnects,
/// each bank is delivered complete once or retracted with [`ReconnectEvent::DiscardBanks`].
pub struct ReconnectGeyserStream {
    pub(crate) inner:
        ReconnectStream<AutoReconnect<Streaming<SubscribeUpdate>, TonicGrpcConnector>>,
}

impl ReconnectGeyserStream {
    pub(crate) fn new(
        inner: AutoReconnect<Streaming<SubscribeUpdate>, TonicGrpcConnector>,
    ) -> Self {
        Self {
            inner: ReconnectStream::new(inner),
        }
    }
}

impl Stream for ReconnectGeyserStream {
    type Item = Result<ReconnectEvent, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
