//! Adapts bank recovery events to block reconstruction.
//! Retains input history for the stream lifetime; intended for bounded E2E runs.

use {
    crate::grpc::E2EGeyserEventAdapter,
    futures::{channel::mpsc, Stream},
    solana_commitment_config::CommitmentLevel,
    std::{
        collections::{HashMap, HashSet, VecDeque},
        pin::Pin,
        task::{Context, Poll},
    },
    yellowstone_block_machine::stream::{
        BlockMachineOutput, BlockStream, SimpleBlockAccumulator, SimpleBlockStore,
    },
    yellowstone_grpc_client::{
        BankRef, DiscardReason, ReconnectEvent, ReplacementReplay, SlotWinner,
    },
    yellowstone_grpc_proto::{
        geyser::{subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate, SubscribeUpdateSlot},
        tonic::Status,
    },
};

type Machine = BlockStream<
    mpsc::Receiver<Result<SubscribeUpdate, Status>>,
    E2EGeyserEventAdapter,
    SimpleBlockAccumulator<SubscribeUpdate>,
>;

pub enum ReconnectBlockEvent {
    Output {
        generation: u64,
        output: BlockMachineOutput<SimpleBlockStore<SubscribeUpdate>>,
    },
    DiscardBanks {
        banks: Vec<BankRef>,
        reason: DiscardReason,
        replacement: ReplacementReplay,
        winners: Vec<SlotWinner>,
    },
}

struct InitializedBank {
    bank_id: u64,
    from_replay: bool,
}

/// Reconstructs unaffected bank state from retained inputs after each discard.
/// Consumers must invalidate previously emitted blocks named by a discard event.
pub struct ReconnectBlockStream<S> {
    source: S,
    incoming: VecDeque<Result<ReconnectEvent, Status>>,
    source_ended: bool,
    sender: Option<mpsc::Sender<Result<SubscribeUpdate, Status>>>,
    machine: Machine,
    generation: u64,
    history: Vec<(u64, SubscribeUpdate)>,
    initialized_banks: HashMap<u64, InitializedBank>,
    replay_cursor: Option<usize>,
    done: bool,
}

fn machine() -> (mpsc::Sender<Result<SubscribeUpdate, Status>>, Machine) {
    let (sender, receiver) = mpsc::channel(2);
    (
        sender,
        BlockStream::new(
            receiver,
            SimpleBlockAccumulator::default(),
            CommitmentLevel::Processed,
        ),
    )
}

const fn update_bank(update: &SubscribeUpdate) -> Option<(u64, Option<u64>)> {
    Some(match update.update_oneof.as_ref() {
        Some(UpdateOneof::Account(m)) => (m.slot, m.bank_id),
        Some(UpdateOneof::Slot(m)) => (m.slot, m.bank_id),
        Some(UpdateOneof::Transaction(m)) => (m.slot, Some(m.bank_id)),
        Some(UpdateOneof::TransactionStatus(m)) => (m.slot, Some(m.bank_id)),
        Some(UpdateOneof::Entry(m)) => (m.slot, Some(m.bank_id)),
        Some(UpdateOneof::Block(m)) => (m.slot, Some(m.bank_id)),
        Some(UpdateOneof::BlockMeta(m)) => (m.slot, Some(m.bank_id)),
        Some(UpdateOneof::BlockFooter(m)) => (m.slot, Some(m.bank_id)),
        _ => return None,
    })
}

fn is_discarded(generation: u64, update: &SubscribeUpdate, banks: &HashSet<BankRef>) -> bool {
    let Some((slot, bank_id)) = update_bank(update) else {
        return false;
    };
    match bank_id {
        Some(bank_id) => banks.contains(&BankRef {
            generation,
            slot,
            bank_id,
        }),
        // Unidentified lifecycle events cannot be safely associated with a surviving bank.
        None => banks
            .iter()
            .any(|bank| bank.generation == generation && bank.slot == slot),
    }
}

impl<S> ReconnectBlockStream<S> {
    fn feed(&mut self, generation: u64, mut update: SubscribeUpdate) -> Result<(), Status> {
        let mut initialization = None;
        if let Some((slot, Some(bank_id))) = update_bank(&update) {
            let known = self.initialized_banks.get(&slot);
            let same_bank = known.is_some_and(|bank| bank.bank_id == bank_id);
            let created = matches!(update.update_oneof.as_ref(),
                Some(UpdateOneof::Slot(status)) if status.status() == SlotStatus::SlotCreatedBank);
            if created {
                if same_bank && known.is_some_and(|bank| bank.from_replay) {
                    // Preserve the parent link without resetting payloads already accepted for this bank.
                    if let Some(UpdateOneof::Slot(status)) = update.update_oneof.as_mut() {
                        status.status = SlotStatus::SlotFirstShredReceived as i32;
                    }
                } else {
                    self.initialized_banks.insert(
                        slot,
                        InitializedBank {
                            bank_id,
                            from_replay: false,
                        },
                    );
                }
            } else if generation > 0 && !same_bank {
                // Server replay omits CreatedBank; initialize the block machine before its payloads.
                let parent = match update.update_oneof.as_ref() {
                    Some(UpdateOneof::Slot(status)) => status.parent,
                    Some(UpdateOneof::BlockMeta(meta)) => Some(meta.parent_slot),
                    _ => None,
                };
                initialization = Some(SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                        slot,
                        parent,
                        bank_id: Some(bank_id),
                        status: SlotStatus::SlotCreatedBank as i32,
                        ..Default::default()
                    })),
                    ..Default::default()
                });
                self.initialized_banks.insert(
                    slot,
                    InitializedBank {
                        bank_id,
                        from_replay: true,
                    },
                );
                log::info!("block recovery: initialized replay bank generation={generation}, slot={slot}, bank_id={bank_id}");
            }
        }
        let sender = self.sender.as_mut().expect("active source has sender");
        for input in initialization.into_iter().chain(std::iter::once(update)) {
            sender
                .try_send(Ok(input))
                .map_err(|_| Status::internal("block machine did not consume its input"))?;
        }
        Ok(())
    }

    pub fn new(source: S) -> Self {
        let (sender, machine) = machine();
        Self {
            source,
            incoming: VecDeque::new(),
            source_ended: false,
            sender: Some(sender),
            machine,
            generation: 0,
            history: Vec::new(),
            initialized_banks: HashMap::new(),
            replay_cursor: None,
            done: false,
        }
    }
}

impl<S> Stream for ReconnectBlockStream<S>
where
    S: Stream<Item = Result<ReconnectEvent, Status>> + Unpin,
{
    type Item = Result<ReconnectBlockEvent, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        let mut source_pending = false;
        // Read ahead even while retained history is being rebuilt.
        for _ in 0..128 {
            if this.source_ended {
                break;
            }
            match Pin::new(&mut this.source).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.source_ended = item.is_err();
                    this.incoming.push_back(item);
                }
                Poll::Ready(None) => {
                    this.source_ended = true;
                    break;
                }
                Poll::Pending => {
                    source_pending = true;
                    break;
                }
            }
        }
        for _ in 0..128 {
            // Drain the machine before feeding its next reconstruction input.
            match Pin::new(&mut this.machine).poll_next(cx) {
                Poll::Ready(Some(Ok(output))) => {
                    if this.replay_cursor.is_some() {
                        continue;
                    }
                    return Poll::Ready(Some(Ok(ReconnectBlockEvent::Output {
                        generation: this.generation,
                        output,
                    })));
                }
                Poll::Ready(Some(Err(error))) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    this.done = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }
            if let Some(index) = this.replay_cursor {
                if let Some((generation, update)) = this.history.get(index) {
                    if let Err(error) = this.feed(*generation, update.clone()) {
                        this.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                    this.replay_cursor = Some(index + 1);
                    continue;
                }
                this.replay_cursor = None;
            }
            match this.incoming.pop_front() {
                None if !this.source_ended => {
                    if !source_pending {
                        cx.waker().wake_by_ref();
                    }
                    return Poll::Pending;
                }
                None => {
                    this.sender.take();
                }
                Some(Err(error)) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Some(Ok(ReconnectEvent::Update { generation, update })) => {
                    if generation < this.generation {
                        this.done = true;
                        return Poll::Ready(Some(Err(Status::failed_precondition(
                            "block update regressed to an older generation",
                        ))));
                    }
                    this.generation = generation;
                    if !matches!(
                        update.update_oneof,
                        None | Some(UpdateOneof::Ping(_) | UpdateOneof::Pong(_))
                    ) {
                        this.history.push((generation, update.clone()));
                    }
                    if let Err(error) = this.feed(generation, update) {
                        this.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Some(Ok(ReconnectEvent::DiscardBanks {
                    banks,
                    reason,
                    replacement,
                    winners,
                })) => {
                    if replacement.generation <= this.generation {
                        this.done = true;
                        return Poll::Ready(Some(Err(Status::failed_precondition(
                            "replacement generation did not advance",
                        ))));
                    }
                    let discarded: HashSet<_> = banks.iter().copied().collect();
                    this.history.retain(|(generation, update)| {
                        !is_discarded(*generation, update, &discarded)
                    });

                    // TODO(block-machine): add selective bank invalidation to avoid retaining and replaying inputs.
                    let (sender, machine) = machine();
                    this.sender = Some(sender);
                    this.machine = machine;
                    this.initialized_banks.clear();
                    this.replay_cursor = Some(0);
                    this.generation = replacement.generation;
                    return Poll::Ready(Some(Ok(ReconnectBlockEvent::DiscardBanks {
                        banks,
                        reason,
                        replacement,
                        winners,
                    })));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, futures::StreamExt, yellowstone_block_machine::stream::BlockEventStore,
        yellowstone_grpc_proto::geyser::*,
    };

    fn update(
        generation: u64,
        payload: subscribe_update::UpdateOneof,
    ) -> Result<ReconnectEvent, Status> {
        Ok(ReconnectEvent::Update {
            generation,
            update: SubscribeUpdate {
                update_oneof: Some(payload),
                ..Default::default()
            },
        })
    }

    fn created(generation: u64) -> Result<ReconnectEvent, Status> {
        update(
            generation,
            subscribe_update::UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 10,
                parent: Some(9),
                bank_id: Some(7),
                status: SlotStatus::SlotCreatedBank as i32,
                ..Default::default()
            }),
        )
    }

    fn account(generation: u64, lamports: u64) -> Result<ReconnectEvent, Status> {
        update(
            generation,
            subscribe_update::UpdateOneof::Account(SubscribeUpdateAccount {
                slot: 10,
                bank_id: Some(7),
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: vec![1; 32],
                    lamports,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
    }

    #[tokio::test]
    async fn discard_resets_partial_state_before_rebuilding() {
        use subscribe_update::UpdateOneof;
        let events = vec![
            created(0),
            account(0, 99),
            Ok(ReconnectEvent::DiscardBanks {
                banks: vec![BankRef {
                    generation: 0,
                    slot: 10,
                    bank_id: 7,
                }],
                reason: DiscardReason::IncompleteDelivery,
                replacement: ReplacementReplay {
                    generation: 1,
                    from_slot: 10,
                },
                winners: vec![],
            }),
            account(1, 42),
            update(
                1,
                UpdateOneof::Entry(SubscribeUpdateEntry {
                    slot: 10,
                    bank_id: 7,
                    hash: vec![3; 32],
                    ..Default::default()
                }),
            ),
            update(
                1,
                UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                    slot: 10,
                    parent_slot: 9,
                    bank_id: 7,
                    entries_count: 1,
                    blockhash: solana_hash::Hash::new_from_array([3; 32]).to_string(),
                    ..Default::default()
                }),
            ),
            update(
                1,
                UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 10,
                    parent: Some(9),
                    bank_id: Some(7),
                    status: SlotStatus::SlotProcessed as i32,
                    ..Default::default()
                }),
            ),
        ];
        let mut stream = ReconnectBlockStream::new(futures::stream::iter(events));
        let mut discarded = false;
        let mut rebuilt = false;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                ReconnectBlockEvent::DiscardBanks { replacement, .. } => {
                    assert_eq!(replacement.generation, 1);
                    discarded = true;
                }
                ReconnectBlockEvent::Output {
                    generation,
                    output: BlockMachineOutput::FrozenBlock(block),
                } => {
                    assert!(discarded);
                    assert_eq!(generation, 1);
                    assert_eq!(block.events.account_len(), 1);
                    let account = block.events.account_iter().next().unwrap();
                    let Some(UpdateOneof::Account(account)) = &account.update_oneof else {
                        panic!("expected account")
                    };
                    assert_eq!(account.account.as_ref().unwrap().lamports, 42);
                    rebuilt = true;
                }
                _ => {}
            }
        }
        assert!(rebuilt, "replacement block was not reconstructed");
        assert!(stream.next().await.is_none());
    }

    fn at_slot(event: Result<ReconnectEvent, Status>, slot: u64) -> Result<ReconnectEvent, Status> {
        let ReconnectEvent::Update {
            generation,
            mut update,
        } = event.unwrap()
        else {
            panic!("expected update")
        };
        match update.update_oneof.as_mut().unwrap() {
            UpdateOneof::Slot(m) => {
                m.slot = slot;
                m.parent = Some(slot - 1);
            }
            UpdateOneof::Account(m) => m.slot = slot,
            _ => panic!("unexpected fixture"),
        }
        Ok(ReconnectEvent::Update { generation, update })
    }

    fn finish(generation: u64, slot: u64) -> Vec<Result<ReconnectEvent, Status>> {
        vec![
            update(
                generation,
                UpdateOneof::Entry(SubscribeUpdateEntry {
                    slot,
                    bank_id: 7,
                    hash: vec![3; 32],
                    ..Default::default()
                }),
            ),
            update(
                generation,
                UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                    slot,
                    parent_slot: slot - 1,
                    bank_id: 7,
                    entries_count: 1,
                    blockhash: solana_hash::Hash::new_from_array([3; 32]).to_string(),
                    ..Default::default()
                }),
            ),
            update(
                generation,
                UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot,
                    parent: Some(slot - 1),
                    bank_id: Some(7),
                    status: SlotStatus::SlotProcessed as i32,
                    ..Default::default()
                }),
            ),
        ]
    }

    fn discard(generation: u64) -> Result<ReconnectEvent, Status> {
        Ok(ReconnectEvent::DiscardBanks {
            banks: vec![BankRef {
                generation,
                slot: 10,
                bank_id: 7,
            }],
            reason: DiscardReason::IncompleteDelivery,
            replacement: ReplacementReplay {
                generation: generation + 1,
                from_slot: 10,
            },
            winners: vec![],
        })
    }

    #[tokio::test]
    async fn late_created_bank_preserves_replayed_payloads() {
        let mut events = vec![
            created(0),
            account(0, 99),
            discard(0),
            account(1, 42),
            created(1),
        ];
        events.extend(finish(1, 10));
        let mut stream = ReconnectBlockStream::new(futures::stream::iter(events));
        let mut accounts = Vec::new();
        while let Some(event) = stream.next().await {
            if let ReconnectBlockEvent::Output {
                output: BlockMachineOutput::FrozenBlock(block),
                ..
            } = event.unwrap()
            {
                for event in block.events.account_iter() {
                    let Some(UpdateOneof::Account(account)) = &event.update_oneof else {
                        unreachable!()
                    };
                    accounts.push(account.account.as_ref().unwrap().lamports);
                }
            }
        }
        assert_eq!(accounts, vec![42]);
    }

    #[tokio::test]
    async fn replacement_bank_id_does_not_reuse_another_banks_payloads() {
        let mut replacement = vec![account(1, 42)];
        replacement.extend(finish(1, 10));
        for event in &mut replacement {
            let ReconnectEvent::Update { update, .. } = event.as_mut().unwrap() else {
                unreachable!()
            };
            match update.update_oneof.as_mut().unwrap() {
                UpdateOneof::Account(account) => account.bank_id = Some(8),
                UpdateOneof::Entry(entry) => entry.bank_id = 8,
                UpdateOneof::BlockMeta(meta) => meta.bank_id = 8,
                UpdateOneof::Slot(status) => status.bank_id = Some(8),
                _ => unreachable!(),
            }
        }
        let mut events = vec![created(0), account(0, 99), discard(0), account(1, 123)];
        events.extend(replacement);
        let mut stream = ReconnectBlockStream::new(futures::stream::iter(events));
        let mut accounts = Vec::new();
        while let Some(event) = stream.next().await {
            if let ReconnectBlockEvent::Output {
                output: BlockMachineOutput::FrozenBlock(block),
                ..
            } = event.unwrap()
            {
                for event in block.events.account_iter() {
                    let Some(UpdateOneof::Account(account)) = &event.update_oneof else {
                        unreachable!()
                    };
                    accounts.push((account.bank_id, account.account.as_ref().unwrap().lamports));
                }
            }
        }
        assert_eq!(accounts, vec![(Some(8), 42)]);
    }

    #[tokio::test]
    async fn preserves_unaffected_partial_bank_without_reemitting_complete_blocks() {
        let mut events = vec![at_slot(created(0), 8), at_slot(account(0, 8), 8)];
        events.extend(finish(0, 8));
        events.extend([
            at_slot(created(0), 9),
            at_slot(account(0, 9), 9),
            created(0),
            account(0, 99),
            discard(0),
        ]);
        events.extend(finish(1, 9));
        events.push(account(1, 42));
        events.extend(finish(1, 10));
        events.extend([discard(1), account(2, 43)]);
        events.extend(finish(2, 10));
        let mut stream = ReconnectBlockStream::new(futures::stream::iter(events));
        let mut observed = Vec::new();
        let mut discards = 0;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                ReconnectBlockEvent::DiscardBanks { .. } => discards += 1,
                ReconnectBlockEvent::Output {
                    output: BlockMachineOutput::FrozenBlock(block),
                    ..
                } => {
                    assert_eq!(block.events.account_len(), 1);
                    let account = block.events.account_iter().next().unwrap();
                    let Some(UpdateOneof::Account(account)) = &account.update_oneof else {
                        panic!("expected account")
                    };
                    observed.push((
                        block.slot,
                        account.account.as_ref().unwrap().lamports,
                        discards,
                    ));
                }
                _ => {}
            }
        }
        assert_eq!(
            observed,
            vec![(8, 8, 0), (9, 9, 1), (10, 42, 1), (10, 43, 2)]
        );
    }

    #[tokio::test]
    async fn reads_incoming_updates_while_retained_history_is_still_replaying() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let reads = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&reads);
        let source = futures::stream::iter((0..512).map(|_| account(1, 42))).inspect(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        let mut stream = ReconnectBlockStream::new(source);
        let ReconnectEvent::Update { update, .. } = account(0, 9).unwrap() else {
            unreachable!()
        };
        stream.history = vec![(0, update); 1024];
        stream.generation = 1;
        stream.replay_cursor = Some(0);

        assert!(futures::poll!(stream.next()).is_pending());
        assert!(reads.load(Ordering::Relaxed) > 0);
        assert_eq!(stream.incoming.len(), reads.load(Ordering::Relaxed));
        assert!(stream.replay_cursor.unwrap() < stream.history.len());
        let first_reads = reads.load(Ordering::Relaxed);
        assert!(futures::poll!(stream.next()).is_pending());
        assert!(reads.load(Ordering::Relaxed) > first_reads);
        assert!(stream.replay_cursor.unwrap() < stream.history.len());
    }

    #[tokio::test]
    async fn queued_discard_precedes_error_and_reader_stops_at_error() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let reads = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&reads);
        let source = futures::stream::iter(vec![
            discard(0),
            Err(Status::unavailable("source failed")),
            account(1, 42),
        ])
        .inspect(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        let mut stream = ReconnectBlockStream::new(source);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ReconnectBlockEvent::DiscardBanks { .. }
        ));
        assert_eq!(
            stream.next().await.unwrap().err().unwrap().code(),
            yellowstone_grpc_proto::tonic::Code::Unavailable
        );
        assert!(stream.next().await.is_none());
        assert_eq!(reads.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn discard_identity_includes_generation_and_bank_id() {
        let banks = HashSet::from([BankRef {
            generation: 0,
            slot: 10,
            bank_id: 7,
        }]);
        let ReconnectEvent::Update { mut update, .. } = account(0, 42).unwrap() else {
            unreachable!()
        };
        assert!(is_discarded(0, &update, &banks));
        assert!(!is_discarded(1, &update, &banks));
        if let Some(UpdateOneof::Account(account)) = update.update_oneof.as_mut() {
            account.bank_id = Some(8);
        }
        assert!(!is_discarded(0, &update, &banks));
    }

    #[tokio::test]
    async fn rejects_generation_regression_and_propagates_source_errors() {
        for inputs in [
            vec![account(1, 42), account(0, 42)],
            vec![Err(Status::unavailable("source failed"))],
        ] {
            let mut stream = ReconnectBlockStream::new(futures::stream::iter(inputs));
            assert!(stream.next().await.unwrap().is_err());
            assert!(stream.next().await.is_none());
        }
    }
}
