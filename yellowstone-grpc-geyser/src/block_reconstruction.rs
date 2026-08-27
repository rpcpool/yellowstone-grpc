use {
    crate::{
        metrics,
        plugin::message::{
            Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageEntry, MessageSlot,
            MessageTransaction, SlotStatus,
        },
    },
    foldhash::{HashMap as FoldHashMap, HashMapExt},
    solana_commitment_config::CommitmentLevel,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    std::{
        borrow::Borrow,
        collections::{btree_map::Range, BTreeMap, VecDeque},
        str::FromStr,
        sync::Arc,
    },
    yellowstone_block_machine::state_machine::{
        BlockReplayEvent, BlockStateMachineOutput, BlockSummary, BlocksStateMachine,
        SlotCommitmentStatusUpdate, SlotLifecycle, SlotLifecycleUpdate, UntrackedSlot,
    },
};

const MUST_HAVE_SYSVAR_ACCOUNTS: [Pubkey; 4] = [
    // written before bank created
    Pubkey::from_str_const(
        "SysvarC1ock11111111111111111111111111111111"
    ),
    Pubkey::from_str_const(
        "SysvarS1otHashes111111111111111111111111111"
    ),
    // Written when the block freeze
    Pubkey::from_str_const(
        "SysvarS1otHistory11111111111111111111111111"
    ),
    Pubkey::from_str_const(
        "SysvarRecentB1ockHashes11111111111111111111"
    ),
];


pub struct ProcessingSlot {
    original_messages: Vec<Message>,
    account_write_version_map: FoldHashMap<Pubkey, u64>,
    blockmeta: Option<Arc<MessageBlockMeta>>,
    transactions: Vec<Arc<MessageTransaction>>,
    accounts: Vec<Arc<MessageAccount>>,
    entries: Vec<Arc<MessageEntry>>,
    is_sealed: bool,
    musthave_sysvar_accounts_bitmask: u8,
}

impl Default for ProcessingSlot {
    fn default() -> Self {
        Self {
            original_messages: Vec::with_capacity(4096),
            account_write_version_map: FoldHashMap::with_capacity(4096),
            blockmeta: None,
            transactions: Vec::with_capacity(4096),
            accounts: Vec::with_capacity(4096),
            entries: Vec::with_capacity(64),
            is_sealed: false,
            musthave_sysvar_accounts_bitmask: 0,
        }
    }
}

enum TrySealError {
    NotSealable,
    AlreadySealed,
}

impl ProcessingSlot {
    pub fn add_event(&mut self, event: Message) {
        match event.borrow() {
            Message::Account(message_account) => {
                let write_version = message_account.account.write_version;
                self.account_write_version_map
                    .entry(message_account.account.pubkey)
                    .and_modify(|entry| {
                        if *entry < write_version {
                            *entry = write_version;
                        }
                    })
                    .or_insert(write_version);
                if let Some(position) = MUST_HAVE_SYSVAR_ACCOUNTS.iter().position(|sysvar| sysvar == &message_account.account.pubkey) {
                    self.musthave_sysvar_accounts_bitmask |= 1 << position;
                }

                self.accounts.push(Arc::clone(message_account));
                // Handle account event
            }
            Message::Transaction(message_transaction) => {
                self.transactions.push(Arc::clone(message_transaction));
                // Handle transaction event
            }
            Message::Entry(message_entry) => {
                self.entries.push(Arc::clone(message_entry));
                // Handle entry event
            }
            _ => {
                // Handle other events if necessary
                return;
            }
        }

        self.original_messages.push(event);
    }

    fn try_seal(&mut self) -> Result<(), TrySealError> {
        if self.is_sealed {
            return Err(TrySealError::AlreadySealed);
        }
        let Some(blockmeta) = self.blockmeta.as_ref() else {
            return Err(TrySealError::NotSealable);
        };

        let expected_txn_count = blockmeta.executed_transaction_count as usize;
        if self.transactions.len() < expected_txn_count {
            return Err(TrySealError::NotSealable);
        }

        if self.musthave_sysvar_accounts_bitmask != (1 << MUST_HAVE_SYSVAR_ACCOUNTS.len()) - 1 {
            return Err(TrySealError::NotSealable);
        }

        let expected_entry_count = blockmeta.entries_count as usize;
        if self.entries.len() < expected_entry_count {
            return Err(TrySealError::NotSealable);
        }
        self.is_sealed = true;
        Ok(())
    }

    pub fn seal(self) -> FrozenBlock {
        let block_meta = self.blockmeta.expect("should be sealable");
        let account_info_vec = self
            .accounts
            .into_iter()
            .filter_map(|account| {
                let write_version = self
                    .account_write_version_map
                    .get(&account.account.pubkey)?;
                if *write_version == account.account.write_version {
                    Some(account)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        // Yet another clone of all the messages, but that prevents from doing this later on anyway, while making iterator code easier to implement.
        let dedup_messages = self
            .original_messages
            .into_iter()
            .filter_map(|message| {
                if let Message::Account(account) = &message {
                    let write_version = self
                        .account_write_version_map
                        .get(&account.account.pubkey)?;
                    if *write_version == account.account.write_version {
                        Some(message)
                    } else {
                        None
                    }
                } else {
                    Some(message)
                }
            })
            .collect::<Vec<_>>();

        if self.transactions.len() != block_meta.executed_transaction_count as usize {
            metrics::incr_geyser_block_mismatch_transaction();
            log::warn!(
                "Block meta transaction count {} does not match actual transaction count {} for slot {}",
                block_meta.executed_transaction_count,
                self.transactions.len(),
                block_meta.slot
            );
        }

        let pre_computed_message_block = Arc::new(MessageBlock::new(
            Arc::clone(&block_meta),
            self.transactions,
            account_info_vec,
            self.entries,
        ));

        FrozenBlock {
            original_messages: Arc::new(dedup_messages),
            block_meta,
            pre_computed_message_block,
        }
    }
}

pub struct FrozenBlock {
    original_messages: Arc<Vec<Message>>,
    block_meta: Arc<MessageBlockMeta>,
    pre_computed_message_block: Arc<MessageBlock>,
}

impl FrozenBlock {
    pub fn get_message_block(&self) -> Arc<MessageBlock> {
        Arc::clone(&self.pre_computed_message_block)
    }

    pub fn messages(&self) -> Arc<Vec<Message>> {
        Arc::clone(&self.original_messages)
    }

    pub fn get_block_meta(&self) -> Arc<MessageBlockMeta> {
        Arc::clone(&self.block_meta)
    }
}

pub struct SlotProgression {
    commitment: Vec<SlotCommitmentStatusUpdate>,
    max_commitment: CommitmentLevel,
}

pub struct BlockMachineStorage {
    processing_slots: FoldHashMap<u64, ProcessingSlot>,
    // Block data (Account/Transaction/Entry) that arrived before the block-machine
    // started tracking its slot's lifecycle. Agave does not guarantee that
    // FirstShredReceived/CreatedBank precede every account write for a slot --
    // notably sysvar writes performed as part of bank construction (see
    // `MUST_HAVE_SYSVAR_ACCOUNTS`) can be observed first. Flushed into
    // `processing_slots` once the slot becomes tracked.
    pending_block_data: FoldHashMap<u64, Vec<Message>>,
    replayed_slot: BTreeMap<u64, Arc<FrozenBlock>>,
    slot_commitment_progression_map: FoldHashMap<u64, SlotProgression>,
    replayed_capacity: usize,
    ready_queue: VecDeque<(SlotCommitmentStatusUpdate, Arc<FrozenBlock>)>,
    state: BlocksStateMachine,
    min_slot: Option<u64>,
    num_buffered_finalized_slot: usize,
}

pub struct ReplayIter<'storage> {
    storage: &'storage BlockMachineStorage,
    iter: Range<'storage, u64, Arc<FrozenBlock>>,
    min_commitment: CommitmentLevel,
}

const fn cmp_commitment_level(a: CommitmentLevel, b: CommitmentLevel) -> std::cmp::Ordering {
    use CommitmentLevel::*;
    match (a, b) {
        (Processed, Processed) | (Confirmed, Confirmed) | (Finalized, Finalized) => {
            std::cmp::Ordering::Equal
        }
        (Processed, _) => std::cmp::Ordering::Less,
        (_, Processed) => std::cmp::Ordering::Greater,
        (Confirmed, Finalized) => std::cmp::Ordering::Less,
        (Finalized, Confirmed) => std::cmp::Ordering::Greater,
    }
}

pub struct ReplayedSlot<'frozen_block> {
    pub frozen_block: &'frozen_block FrozenBlock,
    pub slot_status_messages: Vec<SlotCommitmentStatusUpdate>,
}

impl<'storage> Iterator for ReplayIter<'storage> {
    type Item = ReplayedSlot<'storage>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (slot, block) = self.iter.next()?;
            let Some(progression) = self.storage.slot_commitment_progression_map.get(slot) else {
                continue;
            };
            let commitment_level = progression.max_commitment;
            if cmp_commitment_level(commitment_level, self.min_commitment)
                == std::cmp::Ordering::Less
            {
                continue;
            }
            return Some(ReplayedSlot {
                frozen_block: block.as_ref(),
                slot_status_messages: progression.commitment.clone(),
            });
        }
    }
}

///
/// As blocks are frozen, they are added to the `replayed_slot` map.
///
/// This map has a maximum capacity, and when that capacity is exceeded, the oldest slots are pruned.
///
/// The [`MINIMUM_FINALIZED_SLOT_TO_BUFFER`] constant makes sure we have enough finalized slot in our state to avoid pruning slots
/// that are still in the process of being finalized, which could lead to missing finalized/confirmed messages for those slots.
///
/// See [`BlockMachineStorage::gc`] for more details.
///
///
/// We used to do this in prior to version v13.2.0 of the plugin (before the reconstruction refactor)
///
pub const MINIMUM_FINALIZED_SLOT_TO_BUFFER: usize = 10;

impl BlockMachineStorage {
    pub fn new(replayed_capacity: usize) -> Self {
        Self {
            processing_slots: FoldHashMap::with_capacity(replayed_capacity),
            pending_block_data: FoldHashMap::new(),
            replayed_slot: BTreeMap::new(),
            replayed_capacity,
            slot_commitment_progression_map: FoldHashMap::with_capacity(replayed_capacity),
            ready_queue: VecDeque::with_capacity(replayed_capacity),
            state: BlocksStateMachine::default(),
            min_slot: None,
            num_buffered_finalized_slot: 0,
        }
    }

    fn refresh_min_slot(&mut self) {
        self.min_slot = self.replayed_slot.keys().next().copied();
    }

    /// `reason` is purely for the log line below -- it identifies which of
    /// `prune_slot`'s four call sites (GC capacity eviction, bank reset, fork
    /// detected, dead slot detected) triggered this removal, so a slot that
    /// vanishes from `replayed_slot` before every commitment level has been
    /// delivered for it can be traced back to the actual cause instead of
    /// just disappearing silently.
    fn prune_slot(&mut self, slot: u64, refresh_min_slot: bool, reason: &'static str) {
        self.processing_slots.remove(&slot);
        self.pending_block_data.remove(&slot);
        if self.replayed_slot.remove(&slot).is_some() {
            let progression = self.slot_commitment_progression_map.get(&slot);
            let max_commitment_seen = progression.map(|p| p.max_commitment);
            if max_commitment_seen != Some(CommitmentLevel::Finalized) {
                log::warn!(
                    "Pruning slot {slot} ({reason}) before it reached Finalized -- highest commitment delivered so far: {max_commitment_seen:?}. Any commitment level above that will never be delivered for this slot.",
                );
            }
        }
        if let Some(progression) = self.slot_commitment_progression_map.remove(&slot) {
            if progression.max_commitment == CommitmentLevel::Finalized {
                self.num_buffered_finalized_slot =
                    self.num_buffered_finalized_slot.saturating_sub(1);
            }
        }
        if refresh_min_slot {
            self.refresh_min_slot();
        }
    }

    fn slot_reset(&mut self, slot: u64) {
        self.prune_slot(slot, true, "bank_reset");
        self.ready_queue
            .retain(|(_, block)| block.block_meta.slot != slot);
    }

    fn on_message_slot(&mut self, slot_update: &MessageSlot) -> Result<(), UntrackedSlot> {
        let slot_status = slot_update.status;
        const LIFE_CYCLE_STATUS: [SlotStatus; 4] = [
            SlotStatus::FirstShredReceived,
            SlotStatus::Completed,
            SlotStatus::CreatedBank,
            SlotStatus::Dead,
        ];

        if LIFE_CYCLE_STATUS.contains(&slot_status) {
            let lifecycle_update = SlotLifecycleUpdate {
                slot: slot_update.slot,
                parent_slot: slot_update.parent,
                stage: match slot_status {
                    SlotStatus::FirstShredReceived => SlotLifecycle::FirstShredReceived,
                    SlotStatus::Completed => SlotLifecycle::Completed,
                    SlotStatus::CreatedBank => SlotLifecycle::CreatedBank,
                    SlotStatus::Dead => SlotLifecycle::Dead,
                    _ => unreachable!(),
                },
            };
            self.state.process_replay_event(lifecycle_update.into())?;
        } else if slot_update.dead_error.is_some() {
            // Downgrade to lifecycle update
            let lifecycle_update = SlotLifecycleUpdate {
                slot: slot_update.slot,
                parent_slot: slot_update.parent,
                stage: SlotLifecycle::Dead,
            };
            self.state.process_replay_event(lifecycle_update.into())?;
        } else {
            let commitment_level_update = SlotCommitmentStatusUpdate {
                parent_slot: slot_update.parent,
                slot: slot_update.slot,
                commitment: match slot_status {
                    SlotStatus::Processed => CommitmentLevel::Processed,
                    SlotStatus::Confirmed => CommitmentLevel::Confirmed,
                    SlotStatus::Finalized => CommitmentLevel::Finalized,
                    _ => unreachable!(),
                },
            };

            self.state
                .process_consensus_event(commitment_level_update.into());
        }
        Ok(())
    }

    fn try_seal(&mut self, slot: u64) {
        let Some(outcome) = self
            .processing_slots
            .get_mut(&slot)
            .map(|slot_buf| slot_buf.try_seal())
        else {
            return;
        };

        if outcome.is_ok() {
            // We cannot seal the same slot twice, so we won't update the state machine twice.
            let block_meta = self
                .processing_slots
                .get(&slot)
                .unwrap()
                .blockmeta
                .as_ref()
                .unwrap();
            let block_summary = BlockReplayEvent::BlockSummary(BlockSummary {
                slot: block_meta.slot,
                parent_slot: block_meta.parent_slot,
                blockhash: Hash::from_str(&block_meta.blockhash).expect("blockhash format"),
                entry_count: block_meta.entries_count,
                executed_transaction_count: block_meta.executed_transaction_count,
            });
            let _ = self.state.process_replay_event(block_summary);
        }
    }

    fn handle_block_meta(&mut self, block_meta: Arc<MessageBlockMeta>) {
        if let Some(block) = self.processing_slots.get_mut(&block_meta.slot) {
            block.blockmeta = Some(Arc::clone(&block_meta));
            self.try_seal(block_meta.slot);
        }
    }

    fn handle_block_data(&mut self, block_data: Message) {
        let slot = block_data.get_slot();
        if !self.state.is_slot_tracked(slot) {
            // Agave does not guarantee FirstShredReceived/CreatedBank precede every
            // account write for their slot -- sysvar writes performed as part of bank
            // construction (see `MUST_HAVE_SYSVAR_ACCOUNTS`) can be observed first.
            // Buffer instead of dropping; `flush_pending_block_data` replays this once
            // the slot starts being tracked.
            metrics::incr_geyser_pending_block_data_buffered();
            self.pending_block_data.entry(slot).or_default().push(block_data);
            return;
        }
        // Technically, once a block is sealed and put in the replay queue, we should not NEVER
        // receive any more block data for that slot.
        // We still add this line of code to be extra cautious!
        if self.replayed_slot.contains_key(&slot) {
            log::error!(
                "UNEXPECTED: Received block data for slot {} that is already sealed and in the replay queue. Dropping the message.",
                slot
            );
            return;
        }
        let slot_buf = self.processing_slots.entry(slot).or_default();
        slot_buf.add_event(block_data);
        self.try_seal(slot);
    }

    /// Replays any block data that was buffered by `handle_block_data` while `slot`
    /// wasn't tracked yet, now that it is.
    fn flush_pending_block_data(&mut self, slot: u64) {
        if !self.state.is_slot_tracked(slot) {
            return;
        }
        let Some(pending) = self.pending_block_data.remove(&slot) else {
            return;
        };
        for block_data in pending {
            self.handle_block_data(block_data);
        }
    }

    fn gc(&mut self) {
        while self.replayed_slot.len() > self.replayed_capacity
            && self.num_buffered_finalized_slot > MINIMUM_FINALIZED_SLOT_TO_BUFFER
        {
            if let Some((&oldest_slot, _)) = self.replayed_slot.iter().next() {
                // refresh_min_slot is set to false, since we don't want to refresh the min_slot on every prune, but only after the loop is done.
                self.prune_slot(oldest_slot, false, "gc_capacity_eviction");
            }
        }

        self.refresh_min_slot();
        self.state.gc(None);
    }

    fn on_blockmachine_output(&mut self, output: BlockStateMachineOutput) {
        match output {
            BlockStateMachineOutput::FrozenBlock(frozen_block) => {
                let Some(replayed_slot) = self.processing_slots.remove(&frozen_block.slot) else {
                    return;
                };
                let slot = frozen_block.slot;
                self.min_slot = Some(self.min_slot.map_or(slot, |min| min.min(slot)));
                let frozen_slot = replayed_slot.seal();
                self.replayed_slot
                    .insert(frozen_block.slot, Arc::new(frozen_slot));
            }
            BlockStateMachineOutput::SlotStatus(slot_commitment_status_update) => {
                let slot = slot_commitment_status_update.slot;

                self.slot_commitment_progression_map
                    .entry(slot)
                    .and_modify(|progression| {
                        // Make sure its not there already
                        if !progression
                            .commitment
                            .iter()
                            .any(|s| s.commitment == slot_commitment_status_update.commitment)
                        {
                            progression
                                .commitment
                                .push(slot_commitment_status_update.clone());

                            if matches!(
                                slot_commitment_status_update.commitment,
                                CommitmentLevel::Finalized
                            ) {
                                self.num_buffered_finalized_slot += 1;
                            }
                        }

                        if cmp_commitment_level(
                            slot_commitment_status_update.commitment,
                            progression.max_commitment,
                        ) == std::cmp::Ordering::Greater
                        {
                            progression.max_commitment = slot_commitment_status_update.commitment;
                        }
                    })
                    .or_insert_with(|| {
                        if matches!(
                            slot_commitment_status_update.commitment,
                            CommitmentLevel::Finalized
                        ) {
                            self.num_buffered_finalized_slot += 1;
                        }

                        SlotProgression {
                            commitment: vec![slot_commitment_status_update.clone()],
                            max_commitment: slot_commitment_status_update.commitment,
                        }
                    });

                let commitment_level = slot_commitment_status_update.commitment;

                if let Some(frozen_block) = self.replayed_slot.get(&slot) {
                    self.ready_queue
                        .push_back((slot_commitment_status_update, Arc::clone(frozen_block)));
                } else {
                    // The block-machine guarantees `FrozenBlock` is queued before any
                    // `SlotStatus` for the same slot (see `handle_slot_commitment_status_update`
                    // in `yellowstone-block-machine`), so this isn't an ordering race -- it means
                    // `slot` was pruned from `replayed_slot` (GC eviction, fork/dead-slot
                    // detection, or a bank reset) before this commitment level was delivered.
                    // Silent otherwise: this update is simply dropped.
                    let missed_status = match commitment_level {
                        CommitmentLevel::Processed => SlotStatus::Processed,
                        CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                        CommitmentLevel::Finalized => SlotStatus::Finalized,
                    };
                    metrics::missed_status_message_inc(missed_status);
                    log::warn!(
                        "Dropping {commitment_level:?} commitment update for slot {slot}: no frozen block found in `replayed_slot` (likely pruned before this commitment level arrived)",
                    );
                }

                if matches!(commitment_level, CommitmentLevel::Finalized) {
                    // Only gc on finalized, that should be enough.
                    self.gc();
                }
            }
            BlockStateMachineOutput::ForksDetected(fork_detected) => {
                self.prune_slot(fork_detected.slot, true, "fork_detected");
            }
            BlockStateMachineOutput::DeadSlotDetected(dead_block_detected) => {
                self.prune_slot(dead_block_detected.slot, true, "dead_slot_detected");
            }
            BlockStateMachineOutput::BankCreated(_) => {}
            BlockStateMachineOutput::BankReset(slot) => {
                self.slot_reset(slot);
            }
        }
    }

    pub fn add(&mut self, message: Message) {
        match message {
            Message::Slot(message_slot) => {
                let slot = message_slot.slot;
                if self.on_message_slot(&message_slot).is_err() {
                    // Symmetric with handle_block_data which increments the same metric
                    // when is_slot_tracked returns false.
                    metrics::incr_geyser_untrack_slot_event_dropped();
                } else {
                    self.flush_pending_block_data(slot);
                }
            }
            Message::Account(message_account) => {
                self.handle_block_data(Message::Account(message_account));
            }
            Message::Transaction(message_transaction) => {
                self.handle_block_data(Message::Transaction(message_transaction));
            }
            Message::Entry(message_entry) => {
                self.handle_block_data(Message::Entry(message_entry));
            }
            Message::BlockMeta(message_block_meta) => self.handle_block_meta(message_block_meta),
            _ => {
                // Handle other message types if necessary
            }
        }
        while let Some(output) = self.state.pop_next_unprocess_blockstore_update() {
            self.on_blockmachine_output(output);
        }

        while self.state.pop_next_dlq().is_some() {
            // For now we just log the dlq events, but we may want to handle them in the future if necessary
        }
    }

    pub fn pop_ready_block(&mut self) -> Option<(SlotCommitmentStatusUpdate, Arc<FrozenBlock>)> {
        self.ready_queue.pop_front()
    }

    pub fn replay_from_slot(&self, slot: u64, min_commitment: CommitmentLevel) -> ReplayIter<'_> {
        let iter = self.replayed_slot.range(slot..);
        ReplayIter {
            storage: self,
            iter,
            min_commitment,
        }
    }

    pub const fn min_replayable_slot(&self) -> Option<u64> {
        self.min_slot
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::message::{
            MessageAccount, MessageAccountInfo, MessageEntry, MessageSlot, MessageTransaction,
            MessageTransactionInfo, SlotStatus,
        },
        bytes::Bytes,
        foldhash::{HashSet as FoldHashSet, HashSetExt},
        prost_types::Timestamp,
        solana_hash::Hash,
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::{sync::OnceLock, time::SystemTime},
        yellowstone_grpc_proto::geyser::SubscribeUpdateBlockMeta,
    };

    fn ts() -> Timestamp {
        Timestamp::from(SystemTime::now())
    }

    fn make_account_msg(slot: u64, pubkey: Pubkey, write_version: u64) -> Message {
        Message::Account(Arc::new(MessageAccount {
            account: MessageAccountInfo {
                pubkey,
                lamports: 100,
                owner: Pubkey::default(),
                executable: false,
                rent_epoch: 0,
                data: Bytes::new(),
                write_version,
                txn_signature: None,
                pre_encoded: OnceLock::new(),
            },
            slot,
            is_startup: false,
            created_at: ts(),
            bank_id: Some(slot),
        }))
    }

    fn make_transaction_msg(slot: u64) -> Message {
        Message::Transaction(Arc::new(MessageTransaction {
            transaction: MessageTransactionInfo {
                signature: Signature::default(),
                is_vote: false,
                transaction: Default::default(),
                meta: Default::default(),
                index: 0,
                account_keys: FoldHashSet::new(),
                pre_encoded: OnceLock::new(),
                token_owners_all: OnceLock::new(),
                token_owners_changed: OnceLock::new(),
            },
            slot,
            created_at: ts(),
            bank_id: slot,
        }))
    }

    fn make_entry_msg(slot: u64, index: usize) -> Message {
        Message::Entry(Arc::new(MessageEntry {
            slot,
            index,
            num_hashes: 1,
            hash: Hash::default(),
            executed_transaction_count: 0,
            starting_transaction_index: 0,
            created_at: ts(),
            bank_id: slot,
        }))
    }

    fn make_slot_msg(slot: u64, parent: Option<u64>, status: SlotStatus) -> Message {
        let bank_id = if [SlotStatus::Completed, SlotStatus::Dead, SlotStatus::FirstShredReceived].contains(&status) {
            None
        } else {
            Some(slot)
        };
        Message::Slot(Arc::new(MessageSlot {
            slot,
            parent,
            status,
            dead_error: None,
            created_at: ts(),
            bank_id,
        }))
    }

    fn make_block_meta_msg(slot: u64, parent_slot: u64) -> Message {
        Message::BlockMeta(Arc::new(MessageBlockMeta::from_update_oneof(
            SubscribeUpdateBlockMeta {
                slot,
                parent_slot,
                blockhash: Hash::new_unique().to_string(),
                parent_blockhash: String::new(),
                rewards: None,
                block_time: None,
                block_height: None,
                executed_transaction_count: 0,
                entries_count: 0,
                bank_id: slot,
            },
            ts(),
        )))
    }

    fn make_block_meta_arc(slot: u64, parent_slot: u64) -> Arc<MessageBlockMeta> {
        Arc::new(MessageBlockMeta::from_update_oneof(
            SubscribeUpdateBlockMeta {
                slot,
                parent_slot,
                blockhash: Hash::new_unique().to_string(),
                parent_blockhash: String::new(),
                rewards: None,
                block_time: None,
                block_height: None,
                executed_transaction_count: 0,
                entries_count: 0,
                bank_id: slot
            },
            ts(),
        ))
    }

    /// Drives a slot through the full FirstShredReceived → Completed → BlockMeta → Processed
    /// pipeline. Adds a dummy entry so the slot appears in `processing_slots` and thus
    /// survives into `replayed_slot` after the block is frozen.
    fn drive_slot_to_processed(storage: &mut BlockMachineStorage, slot: u64, parent: Option<u64>) {
        storage.add(make_slot_msg(slot, parent, SlotStatus::FirstShredReceived));
        storage.add(make_slot_msg(slot, parent, SlotStatus::Completed));
        storage.add(make_entry_msg(slot, 0));
        storage.add(make_block_meta_msg(slot, parent.unwrap_or(0)));
        storage.add(make_slot_msg(slot, parent, SlotStatus::Processed));
    }

    // ─── ProcessingSlot ──────────────────────────────────────────────────────

    #[test]
    fn processing_slot_tracks_max_write_version_per_pubkey() {
        let pubkey = Pubkey::new_unique();
        let mut slot = ProcessingSlot::default();

        slot.add_event(make_account_msg(1, pubkey, 3));
        slot.add_event(make_account_msg(1, pubkey, 7)); // new max
        slot.add_event(make_account_msg(1, pubkey, 2)); // below current max, ignored

        assert_eq!(slot.account_write_version_map[&pubkey], 7);
        // All three messages still buffered (dedup happens in into_block, not here)
        assert_eq!(slot.original_messages.len(), 3);
    }

    #[test]
    fn processing_slot_into_block_keeps_only_highest_write_version() {
        let pubkey = Pubkey::new_unique();
        let mut slot = ProcessingSlot::default();
        slot.add_event(make_account_msg(1, pubkey, 1));
        slot.add_event(make_account_msg(1, pubkey, 5)); // winner
        slot.blockmeta = Some(make_block_meta_arc(1, 0));

        let frozen = slot.seal();
        let msgs = frozen.messages();
        let accounts: Vec<_> = msgs
            .iter()
            .filter_map(|m| {
                if let Message::Account(a) = m {
                    Some(a)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(accounts.len(), 1, "only one account should survive dedup");
        assert_eq!(accounts[0].account.write_version, 5);
    }

    #[test]
    fn processing_slot_into_block_deduplicates_independently_per_pubkey() {
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let mut slot = ProcessingSlot::default();

        slot.add_event(make_account_msg(1, pk1, 1));
        slot.add_event(make_account_msg(1, pk1, 4)); // pk1 winner
        slot.add_event(make_account_msg(1, pk2, 9)); // pk2 only entry
        slot.blockmeta = Some(make_block_meta_arc(1, 0));

        let frozen = slot.seal();
        let msgs = frozen.messages();
        let accounts: Vec<_> = msgs
            .iter()
            .filter_map(|m| {
                if let Message::Account(a) = m {
                    Some(a)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(accounts.len(), 2);
        let mut versions: Vec<u64> = accounts.iter().map(|a| a.account.write_version).collect();
        versions.sort_unstable();
        assert_eq!(versions, [4, 9]);
    }

    #[test]
    fn processing_slot_into_block_keeps_all_transactions() {
        let mut slot = ProcessingSlot::default();
        slot.add_event(make_transaction_msg(1));
        slot.add_event(make_transaction_msg(1));
        slot.blockmeta = Some(make_block_meta_arc(1, 0));

        let frozen = slot.seal();
        let tx_count = frozen
            .messages()
            .iter()
            .filter(|m| matches!(m, Message::Transaction(_)))
            .count();
        assert_eq!(tx_count, 2);
    }

    #[test]
    fn processing_slot_into_block_keeps_all_entries() {
        let mut slot = ProcessingSlot::default();
        slot.add_event(make_entry_msg(1, 0));
        slot.add_event(make_entry_msg(1, 1));
        slot.add_event(make_entry_msg(1, 2));
        slot.blockmeta = Some(make_block_meta_arc(1, 0));

        let frozen = slot.seal();
        let entry_count = frozen
            .messages()
            .iter()
            .filter(|m| matches!(m, Message::Entry(_)))
            .count();
        assert_eq!(entry_count, 3);
    }

    #[test]
    fn processing_slot_add_event_ignores_non_block_data() {
        let mut slot = ProcessingSlot::default();
        // Slot and Block messages are not block data — add_event should ignore them
        slot.add_event(make_slot_msg(1, None, SlotStatus::Processed));
        assert_eq!(slot.original_messages.len(), 0);
    }

    #[test]
    fn frozen_block_message_block_reflects_deduplication() {
        let pubkey = Pubkey::new_unique();
        let mut slot = ProcessingSlot::default();
        slot.add_event(make_account_msg(1, pubkey, 2));
        slot.add_event(make_account_msg(1, pubkey, 8)); // winner
        slot.blockmeta = Some(make_block_meta_arc(1, 0));

        let frozen = slot.seal();
        let mb = frozen.get_message_block();
        assert_eq!(mb.accounts.len(), 1);
        assert_eq!(mb.updated_account_count, 1);
        assert_eq!(mb.accounts[0].account.write_version, 8);
    }

    // ─── cmp_commitment_level ────────────────────────────────────────────────

    #[test]
    fn cmp_commitment_level_total_ordering() {
        use {solana_commitment_config::CommitmentLevel as CL, std::cmp::Ordering::*};

        assert_eq!(cmp_commitment_level(CL::Processed, CL::Processed), Equal);
        assert_eq!(cmp_commitment_level(CL::Confirmed, CL::Confirmed), Equal);
        assert_eq!(cmp_commitment_level(CL::Finalized, CL::Finalized), Equal);

        assert_eq!(cmp_commitment_level(CL::Processed, CL::Confirmed), Less);
        assert_eq!(cmp_commitment_level(CL::Processed, CL::Finalized), Less);
        assert_eq!(cmp_commitment_level(CL::Confirmed, CL::Finalized), Less);

        assert_eq!(cmp_commitment_level(CL::Confirmed, CL::Processed), Greater);
        assert_eq!(cmp_commitment_level(CL::Finalized, CL::Processed), Greater);
        assert_eq!(cmp_commitment_level(CL::Finalized, CL::Confirmed), Greater);
    }

    // ─── BlockMachineStorage ─────────────────────────────────────────────────

    #[test]
    fn block_machine_storage_starts_with_no_min_slot_and_empty_queue() {
        let mut storage = BlockMachineStorage::new(10);
        assert!(storage.min_replayable_slot().is_none());
        assert!(storage.pop_ready_block().is_none());
    }

    #[test]
    fn block_machine_storage_full_lifecycle_to_processed() {
        let mut storage = BlockMachineStorage::new(10);
        drive_slot_to_processed(&mut storage, 1, None);

        let (status, _frozen) = storage.pop_ready_block().expect("block should be ready");
        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment, CommitmentLevel::Processed);
        assert!(storage.pop_ready_block().is_none(), "no more ready blocks");
        assert_eq!(storage.min_replayable_slot(), Some(1));
    }

    #[test]
    fn block_machine_storage_buffers_block_data_that_arrives_before_slot_is_tracked() {
        let mut storage = BlockMachineStorage::new(10);

        // Sysvar writes performed during bank construction can be observed by the
        // plugin before FirstShredReceived/CreatedBank for their slot -- there is no
        // slot lifecycle registration yet when these arrive.
        for pubkey in MUST_HAVE_SYSVAR_ACCOUNTS {
            storage.add(make_account_msg(1, pubkey, 1));
        }
        assert!(
            storage.pop_ready_block().is_none(),
            "nothing should be ready before the slot is even tracked"
        );

        storage.add(make_slot_msg(1, None, SlotStatus::FirstShredReceived));
        storage.add(make_slot_msg(1, None, SlotStatus::Completed));
        storage.add(make_entry_msg(1, 0));
        storage.add(make_block_meta_msg(1, 0));
        storage.add(make_slot_msg(1, None, SlotStatus::Processed));

        let (_, frozen) = storage
            .pop_ready_block()
            .expect("block should seal once the buffered sysvar writes are flushed in");
        let accounts: Vec<_> = frozen
            .messages()
            .iter()
            .filter_map(|m| match m {
                Message::Account(a) => Some(a.account.pubkey),
                _ => None,
            })
            .collect();
        for pubkey in MUST_HAVE_SYSVAR_ACCOUNTS {
            assert!(
                accounts.contains(&pubkey),
                "sysvar {pubkey} buffered before tracking started should still make it into the frozen block"
            );
        }
    }

    #[test]
    fn block_machine_storage_account_deduplication_end_to_end() {
        let pubkey = Pubkey::new_unique();
        let mut storage = BlockMachineStorage::new(10);

        storage.add(make_slot_msg(1, None, SlotStatus::FirstShredReceived));
        storage.add(make_slot_msg(1, None, SlotStatus::Completed));
        storage.add(make_account_msg(1, pubkey, 2));
        storage.add(make_account_msg(1, pubkey, 9)); // winner
        storage.add(make_block_meta_msg(1, 0));
        storage.add(make_slot_msg(1, None, SlotStatus::Processed));

        let (_, frozen) = storage.pop_ready_block().expect("ready block");
        let msgs = frozen.messages();
        let accounts: Vec<_> = msgs
            .iter()
            .filter_map(|m| {
                if let Message::Account(a) = m {
                    Some(a)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].account.write_version, 9);
    }

    #[test]
    fn block_machine_storage_fills_missing_commitment_levels() {
        // Jump straight to Finalized without prior Processed/Confirmed.
        // BlocksStateMachine must synthesize Processed → Confirmed → Finalized.
        let mut storage = BlockMachineStorage::new(10);

        storage.add(make_slot_msg(1, None, SlotStatus::FirstShredReceived));
        storage.add(make_slot_msg(1, None, SlotStatus::Completed));
        storage.add(make_entry_msg(1, 0));
        storage.add(make_block_meta_msg(1, 0));
        storage.add(make_slot_msg(1, None, SlotStatus::Finalized));

        let expected = [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ];
        for expected_cl in expected {
            let (status, _) = storage.pop_ready_block().expect("ready block");
            assert_eq!(
                status.commitment, expected_cl,
                "expected {expected_cl:?} but got {:?}",
                status.commitment
            );
        }
        assert!(storage.pop_ready_block().is_none());
    }

    #[test]
    fn block_machine_storage_replay_excludes_slots_below_min_commitment() {
        let mut storage = BlockMachineStorage::new(10);
        drive_slot_to_processed(&mut storage, 1, None);
        // Drain the ready queue
        storage.pop_ready_block();

        // Slot 1 only reached Processed; min_commitment=Confirmed should exclude it
        let replayed: Vec<_> = storage
            .replay_from_slot(1, CommitmentLevel::Confirmed)
            .collect();
        assert!(
            replayed.is_empty(),
            "Processed-only slot must not appear when replaying at Confirmed"
        );
    }

    #[test]
    fn block_machine_storage_replay_includes_slot_at_exact_commitment() {
        let mut storage = BlockMachineStorage::new(10);
        drive_slot_to_processed(&mut storage, 1, None);
        storage.pop_ready_block();

        let replayed: Vec<_> = storage
            .replay_from_slot(1, CommitmentLevel::Processed)
            .collect();
        assert_eq!(replayed.len(), 1);
        assert_eq!(
            replayed[0].slot_status_messages[0].commitment,
            CommitmentLevel::Processed
        );
    }

    #[test]
    fn block_machine_storage_replay_includes_finalized_slot_when_min_is_confirmed() {
        let mut storage = BlockMachineStorage::new(10);

        // Drive slot 1 all the way to Finalized (gap-filling gives Processed+Confirmed+Finalized)
        storage.add(make_slot_msg(1, None, SlotStatus::FirstShredReceived));
        storage.add(make_slot_msg(1, None, SlotStatus::Completed));
        storage.add(make_entry_msg(1, 0));
        storage.add(make_block_meta_msg(1, 0));
        storage.add(make_slot_msg(1, None, SlotStatus::Finalized));
        // Drain ready queue
        while storage.pop_ready_block().is_some() {}

        // Even though we asked for Confirmed minimum, a Finalized slot satisfies it
        let replayed: Vec<_> = storage
            .replay_from_slot(1, CommitmentLevel::Confirmed)
            .collect();
        assert_eq!(replayed.len(), 1);
    }

    #[test]
    fn block_machine_storage_entry_messages_preserved_in_frozen_block() {
        let mut storage = BlockMachineStorage::new(10);

        storage.add(make_slot_msg(1, None, SlotStatus::FirstShredReceived));
        storage.add(make_slot_msg(1, None, SlotStatus::Completed));
        storage.add(make_entry_msg(1, 0));
        storage.add(make_entry_msg(1, 1));
        storage.add(make_block_meta_msg(1, 0));
        storage.add(make_slot_msg(1, None, SlotStatus::Processed));

        let (_, frozen) = storage.pop_ready_block().expect("ready block");
        let messages = frozen.messages();

        let entry_count = messages
            .iter()
            .filter(|m| matches!(m, Message::Entry(_)))
            .count();

        /* Last message is explicitly sent as BlockMeta, its no longer implicit inside of original_messages */
        assert_eq!(entry_count, 2);
    }

    #[test]
    fn block_machine_storage_reemit_same_block_every_commitment_progression() {
        let mut storage = BlockMachineStorage::new(10);

        storage.add(make_slot_msg(1, None, SlotStatus::FirstShredReceived));
        storage.add(make_slot_msg(1, None, SlotStatus::Completed));
        storage.add(make_entry_msg(1, 0));
        storage.add(make_entry_msg(1, 1));
        storage.add(make_block_meta_msg(1, 0));

        const ALL_COMMITMENT: [CommitmentLevel; 3] = [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ];

        for commitment_level in ALL_COMMITMENT {
            let status = match commitment_level {
                CommitmentLevel::Processed => SlotStatus::Processed,
                CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                CommitmentLevel::Finalized => SlotStatus::Finalized,
            };
            storage.add(make_slot_msg(1, None, status));
            let (actual_commitment, frozen) = storage.pop_ready_block().expect("ready block");
            assert!(
                actual_commitment.commitment == commitment_level,
                "expected commitment {commitment_level:?} but got {:?}",
                actual_commitment.commitment
            );
            let messages = frozen.messages();
            let entry_count = messages
                .iter()
                .filter(|m| matches!(m, Message::Entry(_)))
                .count();
            /* Last message is explicitly sent as BlockMeta, its no longer implicit inside of original_messages */
            assert_eq!(entry_count, 2);
        }
    }

    #[test]
    fn block_machine_should_yield_all_block_and_commitment_when_replayed_capacity_set_to_zero() {
        let mut storage = BlockMachineStorage::new(0);
        drive_slot_to_processed(&mut storage, 1, None);
        storage.add(make_slot_msg(1, None, SlotStatus::Confirmed));
        storage.add(make_slot_msg(1, None, SlotStatus::Finalized));

        // Drain the ready queue
        let (actual, _block) = storage.pop_ready_block().unwrap();
        assert!(actual.commitment == CommitmentLevel::Processed);

        let (actual, _block) = storage.pop_ready_block().unwrap();
        assert!(actual.commitment == CommitmentLevel::Confirmed);

        let (actual, _block) = storage.pop_ready_block().unwrap();
        assert!(actual.commitment == CommitmentLevel::Finalized);
    }
}
