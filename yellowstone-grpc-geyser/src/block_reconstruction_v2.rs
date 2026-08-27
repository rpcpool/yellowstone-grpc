use {
    crate::{
        metrics,
        plugin::message::{
            Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageEntry, MessageSlot,
            MessageTransaction, SlotStatus,
        },
    },
    foldhash::{HashMap as FoldHashMap, HashMapExt, HashSet as FoldHashSet, HashSetExt},
    solana_clock::{BankId, Slot},
    solana_commitment_config::CommitmentLevel,
    solana_pubkey::Pubkey,
    std::{
        collections::{btree_map::Range, BTreeMap, VecDeque},
        sync::Arc,
    },
};

// Sysvars that are rewritten (and thus notified) essentially every slot as part of bank
// construction. Presence is checked at seal time purely for observability -- see the long
// comment on `BankBuffer::seal` for why this must never gate sealing.
const BASIC_SYSVAR_ACCOUNTS: [Pubkey; 2] = [
    Pubkey::from_str_const("SysvarC1ock11111111111111111111111111111111"),
    Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111"),
];

const fn commitment_rank(level: CommitmentLevel) -> u8 {
    match level {
        CommitmentLevel::Processed => 0,
        CommitmentLevel::Confirmed => 1,
        CommitmentLevel::Finalized => 2,
    }
}

const MINIMUM_FINALIZED_SLOT_TO_BUFFER: usize = 10;

enum TrySealError {
    NotSealable,
    AlreadySealed,
}

/// Accumulates block content (Account/Transaction/Entry/BlockMeta) for a single bank
/// instance, keyed by `bank_id` rather than by slot. Unlike a slot number, a `bank_id` is
/// unique per bank instance, so there is no ambiguity to resolve when a slot ends up with
/// more than one bank (e.g. after a dump-and-repair replay) -- each gets its own buffer,
/// and neither can clobber the other's accumulated data.
struct BankBuffer {
    bank_id: BankId,
    slot: Slot,
    parent_slot: Option<Slot>,
    // Whether a `CreatedBank` status update has been observed for this bank_id. We do not
    // assume the first event seen for a bank_id is `CreatedBank` -- account/transaction/entry
    // data can legitimately arrive before it -- so this is tracked explicitly and required at
    // seal time instead.
    created_bank_seen: bool,
    original_messages: Vec<Message>,
    account_write_version_map: FoldHashMap<Pubkey, u64>,
    blockmeta: Option<Arc<MessageBlockMeta>>,
    transactions: Vec<Arc<MessageTransaction>>,
    accounts: Vec<Arc<MessageAccount>>,
    entries: Vec<Arc<MessageEntry>>,
    is_sealed: bool,
}

impl BankBuffer {
    fn new(bank_id: BankId, slot: Slot) -> Self {
        Self {
            bank_id,
            slot,
            parent_slot: None,
            created_bank_seen: false,
            original_messages: Vec::with_capacity(4096),
            account_write_version_map: FoldHashMap::with_capacity(4096),
            blockmeta: None,
            transactions: Vec::with_capacity(4096),
            accounts: Vec::with_capacity(4096),
            entries: Vec::with_capacity(64),
            is_sealed: false,
        }
    }

    fn add_event(&mut self, event: Message) {
        match &event {
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
                self.accounts.push(Arc::clone(message_account));
            }
            Message::Transaction(message_transaction) => {
                self.transactions.push(Arc::clone(message_transaction));
            }
            Message::Entry(message_entry) => {
                self.entries.push(Arc::clone(message_entry));
            }
            _ => return,
        }
        self.original_messages.push(event);
    }

    fn try_seal(&mut self) -> Result<(), TrySealError> {
        if self.is_sealed {
            return Err(TrySealError::AlreadySealed);
        }
        if !self.created_bank_seen {
            return Err(TrySealError::NotSealable);
        }
        let Some(blockmeta) = self.blockmeta.as_ref() else {
            return Err(TrySealError::NotSealable);
        };

        let expected_txn_count = blockmeta.executed_transaction_count as usize;
        if self.transactions.len() < expected_txn_count {
            return Err(TrySealError::NotSealable);
        }

        let expected_entry_count = blockmeta.entries_count as usize;
        if self.entries.len() < expected_entry_count {
            return Err(TrySealError::NotSealable);
        }
        self.is_sealed = true;
        Ok(())
    }

    fn seal(self) -> FrozenBank {
        // Informational only -- do NOT turn this into a sealing precondition. An earlier
        // version of this pipeline required every one of these sysvars to be observed
        // before a slot could seal, keyed by slot rather than bank_id; a spurious "bank
        // reset" (a `FirstShredReceived` placeholder being mistaken for a real prior bank)
        // discarded the handful of sysvar writes that land right before `CreatedBank` on
        // every single slot, and since a slot's own bank only ever gets one shot at
        // emitting them, sealing stalled forever, permanently, for every slot. Keying by
        // bank_id instead removes that specific failure mode, but there is still no
        // guarantee these are observed before the rest of the bank's content is -- so this
        // stays a log line, never a blocker.
        for pubkey in BASIC_SYSVAR_ACCOUNTS {
            if !self.account_write_version_map.contains_key(&pubkey) {
                log::warn!(
                    "bank {} (slot {}) sealed without observing sysvar account {pubkey}",
                    self.bank_id,
                    self.slot
                );
            }
        }

        let block_meta = self.blockmeta.expect("should be sealable");
        let account_info_vec = self
            .accounts
            .into_iter()
            .filter_map(|account| {
                let write_version = self
                    .account_write_version_map
                    .get(&account.account.pubkey)?;
                (*write_version == account.account.write_version).then_some(account)
            })
            .collect::<Vec<_>>();
        let dedup_messages = self
            .original_messages
            .into_iter()
            .filter_map(|message| {
                if let Message::Account(account) = &message {
                    let write_version = self
                        .account_write_version_map
                        .get(&account.account.pubkey)?;
                    (*write_version == account.account.write_version).then_some(message)
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

        FrozenBank {
            bank_id: self.bank_id,
            slot: self.slot,
            original_messages: Arc::new(dedup_messages),
            block_meta,
            pre_computed_message_block,
        }
    }
}

pub struct FrozenBank {
    bank_id: BankId,
    slot: Slot,
    original_messages: Arc<Vec<Message>>,
    block_meta: Arc<MessageBlockMeta>,
    pre_computed_message_block: Arc<MessageBlock>,
}

impl FrozenBank {
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

#[derive(Debug, Clone)]
pub struct SlotCommitmentStatusUpdate {
    pub slot: Slot,
    pub parent_slot: Option<Slot>,
    pub commitment: CommitmentLevel,
    pub bank_id: BankId,
}

struct SlotProgression {
    commitment: Vec<SlotCommitmentStatusUpdate>,
    max_commitment: CommitmentLevel,
}

pub struct ReplayedSlot<'frozen_bank> {
    pub frozen_block: &'frozen_bank FrozenBank,
    pub slot_status_messages: Vec<SlotCommitmentStatusUpdate>,
}

pub struct ReplayIter<'storage> {
    storage: &'storage BlockMachineStorage,
    iter: Range<'storage, Slot, Arc<FrozenBank>>,
    min_commitment: CommitmentLevel,
}

impl<'storage> Iterator for ReplayIter<'storage> {
    type Item = ReplayedSlot<'storage>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (slot, block) = self.iter.next()?;
            let Some(progression) = self.storage.slot_commitment_progression_map.get(slot) else {
                continue;
            };
            if commitment_rank(progression.max_commitment) < commitment_rank(self.min_commitment) {
                continue;
            }
            return Some(ReplayedSlot {
                frozen_block: block.as_ref(),
                slot_status_messages: progression.commitment.clone(),
            });
        }
    }
}

/// Bank-id-oriented block reconstruction. Replaces the earlier slot-keyed pipeline (backed
/// by the `yellowstone-block-machine` crate's `BlocksStateMachine`), which conflated
/// "a `FirstShredReceived` placeholder is sitting here" with "a genuine prior bank already
/// existed here" -- both looked identical from a slot-keyed perspective, which made every
/// slot's very first (and typically only) `CreatedBank` look like a reset, discarding the
/// sysvar writes that land right before it, on every slot, permanently.
///
/// Buffering by `bank_id` sidesteps that class of bug entirely: each bank instance -- real
/// or a repair replay -- gets its own independent buffer, so there is nothing for one to
/// clobber in another. A slot only ever has more than one live buffer while forks/repairs
/// are still being resolved; the moment any commitment status update names a `bank_id` as
/// the canonical bank for its slot, every sibling buffer for that slot is discarded.
pub struct BlockMachineStorage {
    // Bank instances still accumulating content, keyed by bank_id.
    banks: FoldHashMap<BankId, BankBuffer>,
    // Sealed bank instances not yet known to be their slot's winner (or not yet promoted).
    frozen_banks: FoldHashMap<BankId, Arc<FrozenBank>>,
    // Every bank_id ever seen for a slot, so the losers can be found once a winner is known.
    slot_to_banks: FoldHashMap<Slot, Vec<BankId>>,
    // bank_ids explicitly discarded as losers (via `discard_losing_banks`) or abandoned
    // (via `handle_dead_slot`). A straggler event arriving afterward for one of these is
    // ignored outright rather than reviving a fresh buffer for it -- distinct from a
    // bank_id that simply hasn't been resolved as a winner *yet*, which must still be
    // allowed to accumulate normally (see `processed_resolution_can_be_superseded_by_a_later_bank`).
    discarded_bank_ids: FoldHashSet<BankId>,
    // The bank_id currently believed to be a slot's canonical bank, learned from
    // Processed/Confirmed/Finalized status updates naming a bank_id for that slot. At
    // Processed this is provisional and can be superseded by a later, different bank_id
    // (see `supersede_resolved_bank`) -- e.g. a dump-and-repair replay correcting an
    // earlier bank whose hash was wrong. Once a slot reaches Confirmed or Finalized this
    // is treated as final and can no longer change.
    resolved_bank_per_slot: FoldHashMap<Slot, BankId>,
    // Ancestry: slot -> parent slot, learned from CreatedBank/BlockMeta.
    parent_of: FoldHashMap<Slot, Slot>,
    // The highest commitment level a slot must be treated as having reached, whether from a
    // direct status update or inherited from a confirmed/finalized descendant.
    slot_min_commitment: FoldHashMap<Slot, CommitmentLevel>,
    slot_commitment_progression_map: FoldHashMap<Slot, SlotProgression>,
    // Final, resolved content -- one entry per slot, once its winning bank has sealed.
    replayed_slot: BTreeMap<Slot, Arc<FrozenBank>>,
    ready_queue: VecDeque<(SlotCommitmentStatusUpdate, Arc<FrozenBank>)>,
    replayed_capacity: usize,
    num_buffered_finalized_slot: usize,
    min_slot: Option<Slot>,
}

impl BlockMachineStorage {
    pub fn new(replayed_capacity: usize) -> Self {
        Self {
            banks: FoldHashMap::with_capacity(replayed_capacity),
            frozen_banks: FoldHashMap::new(),
            slot_to_banks: FoldHashMap::with_capacity(replayed_capacity),
            discarded_bank_ids: FoldHashSet::new(),
            resolved_bank_per_slot: FoldHashMap::with_capacity(replayed_capacity),
            parent_of: FoldHashMap::with_capacity(replayed_capacity),
            slot_min_commitment: FoldHashMap::with_capacity(replayed_capacity),
            slot_commitment_progression_map: FoldHashMap::with_capacity(replayed_capacity),
            replayed_slot: BTreeMap::new(),
            ready_queue: VecDeque::with_capacity(replayed_capacity),
            replayed_capacity,
            num_buffered_finalized_slot: 0,
            min_slot: None,
        }
    }

    pub fn add(&mut self, message: Message) {
        match message {
            Message::Slot(message_slot) => self.handle_slot_status(&message_slot),
            Message::Account(_) | Message::Transaction(_) | Message::Entry(_) => {
                self.handle_block_data(message);
            }
            Message::BlockMeta(block_meta) => self.handle_block_meta(block_meta),
            _ => {
                // Message::Block is synthesized internally and never fed back in;
                // Message::DeshredTransaction goes through a separate pipeline entirely.
            }
        }
    }

    fn handle_slot_status(&mut self, message_slot: &MessageSlot) {
        match message_slot.status {
            // Neither carries a bank_id, and neither is needed: buffering is keyed by
            // bank_id and starts on first sight of any bank-scoped event, not on
            // FirstShredReceived: see the module doc comment for why that distinction
            // matters.
            SlotStatus::FirstShredReceived | SlotStatus::Completed => {}
            SlotStatus::Dead => self.handle_dead_slot(message_slot.slot),
            SlotStatus::CreatedBank => self.handle_created_bank(message_slot),
            SlotStatus::Processed | SlotStatus::Confirmed | SlotStatus::Finalized => {
                self.handle_commitment_update(message_slot);
            }
        }
    }

    fn register_bank_for_slot(&mut self, slot: Slot, bank_id: BankId) {
        let ids = self.slot_to_banks.entry(slot).or_default();
        if !ids.contains(&bank_id) {
            ids.push(bank_id);
        }
    }

    fn record_parent(&mut self, slot: Slot, parent_slot: Slot) {
        self.parent_of.entry(slot).or_insert(parent_slot);
    }

    fn handle_created_bank(&mut self, message_slot: &MessageSlot) {
        let Some(bank_id) = message_slot.bank_id else {
            log::warn!(
                "CreatedBank status for slot {} carries no bank_id; ignoring",
                message_slot.slot
            );
            return;
        };
        if self.discarded_bank_ids.contains(&bank_id) {
            return;
        }
        let slot = message_slot.slot;
        self.register_bank_for_slot(slot, bank_id);
        if let Some(parent) = message_slot.parent {
            self.record_parent(slot, parent);
        }
        let bank = self
            .banks
            .entry(bank_id)
            .or_insert_with(|| BankBuffer::new(bank_id, slot));
        bank.created_bank_seen = true;
        if bank.parent_slot.is_none() {
            bank.parent_slot = message_slot.parent;
        }
        self.try_seal_bank(bank_id);
    }

    fn handle_dead_slot(&mut self, slot: Slot) {
        if let Some(bank_ids) = self.slot_to_banks.remove(&slot) {
            for bank_id in bank_ids {
                self.banks.remove(&bank_id);
                self.frozen_banks.remove(&bank_id);
                self.discarded_bank_ids.insert(bank_id);
            }
        }
        self.resolved_bank_per_slot.remove(&slot);
        if self.replayed_slot.remove(&slot).is_some() {
            let max_commitment = self
                .slot_commitment_progression_map
                .get(&slot)
                .map(|p| p.max_commitment);
            if max_commitment != Some(CommitmentLevel::Finalized) {
                log::warn!(
                    "Slot {slot} marked Dead after already being partially delivered (highest commitment so far: {max_commitment:?}) -- any commitment level above that will never be delivered for this slot.",
                );
            }
        }
    }

    fn handle_commitment_update(&mut self, message_slot: &MessageSlot) {
        let Some(bank_id) = message_slot.bank_id else {
            log::warn!(
                "{:?} status for slot {} carries no bank_id; ignoring",
                message_slot.status,
                message_slot.slot
            );
            return;
        };
        if self.discarded_bank_ids.contains(&bank_id) {
            log::warn!(
                "commitment update for slot {} targets bank_id {bank_id}, which was already discarded as a loser -- dropping",
                message_slot.slot
            );
            return;
        }
        let slot = message_slot.slot;
        let commitment = match message_slot.status {
            SlotStatus::Processed => CommitmentLevel::Processed,
            SlotStatus::Confirmed => CommitmentLevel::Confirmed,
            SlotStatus::Finalized => CommitmentLevel::Finalized,
            _ => unreachable!("handle_commitment_update only called for commitment statuses"),
        };

        self.register_bank_for_slot(slot, bank_id);
        if let Some(parent) = message_slot.parent {
            self.record_parent(slot, parent);
        }

        if let Some(&existing) = self.resolved_bank_per_slot.get(&slot) {
            if existing != bank_id {
                // A different bank_id is now reporting commitment progress for a slot
                // that already resolved to another one. Processed is optimistic and can
                // legitimately be superseded (e.g. a dump-and-repair replay correcting an
                // earlier bank whose hash turned out to be wrong) -- but once a slot has
                // reached Confirmed or Finalized, Solana guarantees that resolution is
                // final, so a later, different bank_id at that point is unexpected and
                // must not be allowed to clobber it.
                let existing_level = self
                    .slot_commitment_progression_map
                    .get(&slot)
                    .map(|p| p.max_commitment);
                if matches!(
                    existing_level,
                    Some(CommitmentLevel::Confirmed) | Some(CommitmentLevel::Finalized)
                ) {
                    log::warn!(
                        "commitment update for slot {slot} targets bank_id {bank_id}, but bank_id {existing} was already {existing_level:?} for this slot -- dropping",
                    );
                    return;
                }
                log::warn!(
                    "slot {slot}'s resolved bank changed from {existing} to {bank_id} (previous commitment: {existing_level:?}) -- superseding, likely a dump-and-repair replay correcting an optimistic Processed bank",
                );
                self.supersede_resolved_bank(slot, bank_id);
            }
        } else {
            self.resolved_bank_per_slot.insert(slot, bank_id);
        }

        self.discard_losing_banks(slot, bank_id);
        self.bump_slot_commitment(slot, commitment);
    }

    /// Replaces `slot`'s resolved bank_id, discarding whatever content was optimistically
    /// delivered/promoted under the old (now-superseded) one so that any subsequent replay
    /// request serves the corrected bank's content instead of stale data. `slot_min_commitment`
    /// is deliberately left untouched: it's the slot's target commitment level, independent of
    /// which bank_id ultimately satisfies it, so it stays valid across the swap and will be
    /// applied to the new bank as soon as it (re)seals.
    fn supersede_resolved_bank(&mut self, slot: Slot, new_winner: BankId) {
        self.resolved_bank_per_slot.insert(slot, new_winner);
        self.replayed_slot.remove(&slot);
        self.slot_commitment_progression_map.remove(&slot);
    }

    fn discard_losing_banks(&mut self, slot: Slot, winner: BankId) {
        let Some(ids) = self.slot_to_banks.get_mut(&slot) else {
            return;
        };
        let losers: Vec<BankId> = ids.iter().copied().filter(|&id| id != winner).collect();
        ids.retain(|&id| id == winner);
        for loser in losers {
            self.banks.remove(&loser);
            self.frozen_banks.remove(&loser);
            self.discarded_bank_ids.insert(loser);
        }
    }

    /// Raises the effective commitment floor for `slot` to `level` (a no-op for the floor
    /// itself if it's already there or higher -- but promotion is still (re-)attempted
    /// regardless, since a `supersede_resolved_bank` can clear delivered content without
    /// changing the floor), emits the corresponding update(s) immediately if content is
    /// available, and retroactively propagates the same floor up the parent chain the first
    /// time it's raised, so an ancestor that never gets its own direct commitment status
    /// update from geyser (this does happen -- geyser does not guarantee one arrives for
    /// every slot) still ends up at least at its descendants' commitment level.
    fn bump_slot_commitment(&mut self, slot: Slot, level: CommitmentLevel) {
        let already_at_level = self
            .slot_min_commitment
            .get(&slot)
            .is_some_and(|&current| commitment_rank(current) >= commitment_rank(level));
        if !already_at_level {
            self.slot_min_commitment.insert(slot, level);
        }

        self.try_promote_now(slot);
        if let Some(frozen) = self.replayed_slot.get(&slot).cloned() {
            self.emit_commitment_levels_up_to(slot, level, &frozen);
        }

        if already_at_level {
            // The floor was already raised (and thus already propagated to ancestors) on
            // an earlier call; nothing further to walk up for.
            return;
        }
        if let Some(&parent) = self.parent_of.get(&slot) {
            self.bump_slot_commitment(parent, level);
        }
    }

    /// Attempts to promote `slot`'s resolved bank to `replayed_slot` if it has sealed but
    /// hasn't been promoted yet. If `slot` was never itself the target of a direct
    /// Processed/Confirmed/Finalized status update from geyser -- which can legitimately
    /// happen; a level is not guaranteed to be reported for every individual slot -- but is
    /// being resolved now purely because a descendant's commitment level is propagating up
    /// through it, infer its winning bank_id when there's exactly one known candidate for
    /// it. With more than one candidate and no direct confirmation, which one is canonical
    /// genuinely can't be determined here, so it's left unresolved and logged.
    fn try_promote_now(&mut self, slot: Slot) {
        if self.replayed_slot.contains_key(&slot) {
            return;
        }
        if !self.resolved_bank_per_slot.contains_key(&slot) {
            match self.slot_to_banks.get(&slot).map(Vec::as_slice) {
                Some([single]) => {
                    let inferred = *single;
                    self.resolved_bank_per_slot.insert(slot, inferred);
                }
                Some(ids) if ids.len() > 1 => {
                    log::warn!(
                        "slot {slot} needs to resolve (e.g. a descendant's commitment level is propagating up to it) but has {} competing bank_ids and none was ever directly confirmed -- cannot determine which is canonical",
                        ids.len()
                    );
                    return;
                }
                _ => return,
            }
        }
        let Some(&winner) = self.resolved_bank_per_slot.get(&slot) else {
            return;
        };
        if let Some(frozen) = self.frozen_banks.get(&winner).cloned() {
            self.promote_to_replayed(slot, frozen);
        }
    }

    fn try_seal_bank(&mut self, bank_id: BankId) {
        let Some(bank) = self.banks.get_mut(&bank_id) else {
            return;
        };
        if bank.try_seal().is_ok() {
            let bank = self.banks.remove(&bank_id).expect("just verified present");
            let slot = bank.slot;
            let frozen = Arc::new(bank.seal());
            self.on_bank_sealed(slot, bank_id, frozen);
        }
    }

    fn on_bank_sealed(&mut self, slot: Slot, bank_id: BankId, frozen: Arc<FrozenBank>) {
        self.frozen_banks.insert(bank_id, Arc::clone(&frozen));
        // `try_promote_now` promotes immediately if this bank_id was already resolved as
        // the winner by a commitment update, or -- since geyser doesn't guarantee a direct
        // commitment status arrives for every slot -- if it's the only bank_id ever seen
        // for this slot so far. If a second, different bank_id shows up later, this is
        // still safely superseded by `handle_commitment_update`/`supersede_resolved_bank`,
        // since no commitment was ever actually recorded for it yet.
        self.try_promote_now(slot);
    }

    /// Makes a sealed bank's content the slot's resolved, replayable content. Content is
    /// always promoted here -- before any commitment level for it is ever queued -- so
    /// subscribers always see block content, then block meta, then commitment updates, in
    /// that order.
    fn promote_to_replayed(&mut self, slot: Slot, frozen: Arc<FrozenBank>) {
        if self.replayed_slot.contains_key(&slot) {
            return;
        }
        self.frozen_banks.remove(&frozen.bank_id);
        self.min_slot = Some(self.min_slot.map_or(slot, |m| m.min(slot)));
        self.replayed_slot.insert(slot, Arc::clone(&frozen));
        if let Some(&level) = self.slot_min_commitment.get(&slot) {
            self.emit_commitment_levels_up_to(slot, level, &frozen);
        }
    }

    fn emit_commitment_levels_up_to(
        &mut self,
        slot: Slot,
        level: CommitmentLevel,
        frozen: &Arc<FrozenBank>,
    ) {
        let bank_id = frozen.bank_id;
        let parent_slot = self.parent_of.get(&slot).copied();
        let mut to_emit: Vec<SlotCommitmentStatusUpdate> = Vec::new();
        let mut newly_finalized = false;
        {
            let progression = self
                .slot_commitment_progression_map
                .entry(slot)
                .or_insert_with(|| SlotProgression {
                    commitment: Vec::new(),
                    max_commitment: CommitmentLevel::Processed,
                });
            for candidate in [
                CommitmentLevel::Processed,
                CommitmentLevel::Confirmed,
                CommitmentLevel::Finalized,
            ] {
                if commitment_rank(candidate) > commitment_rank(level) {
                    break;
                }
                if progression
                    .commitment
                    .iter()
                    .any(|u| u.commitment == candidate)
                {
                    continue;
                }
                let update = SlotCommitmentStatusUpdate {
                    slot,
                    parent_slot,
                    commitment: candidate,
                    bank_id,
                };
                progression.commitment.push(update.clone());
                if commitment_rank(candidate) > commitment_rank(progression.max_commitment) {
                    progression.max_commitment = candidate;
                }
                if candidate == CommitmentLevel::Finalized {
                    newly_finalized = true;
                }
                to_emit.push(update);
            }
        }
        if newly_finalized {
            self.num_buffered_finalized_slot += 1;
        }
        for update in to_emit {
            self.ready_queue.push_back((update, Arc::clone(frozen)));
        }
        if newly_finalized {
            self.gc();
        }
    }

    fn handle_block_data(&mut self, message: Message) {
        let slot = message.get_slot();
        let bank_id = match &message {
            Message::Account(a) => a.bank_id,
            Message::Transaction(t) => Some(t.bank_id),
            Message::Entry(e) => Some(e.bank_id),
            _ => None,
        };
        let Some(bank_id) = bank_id else {
            // No bank_id -- e.g. a startup snapshot account -- is not part of live bank
            // reconstruction, so it's ignored here rather than buffered.
            return;
        };
        if self.discarded_bank_ids.contains(&bank_id) {
            return;
        }
        // Only the bank_id that's already fully sealed *and* promoted is a genuine
        // "shouldn't happen" straggler here. Any other bank_id -- including one that
        // hasn't been resolved as a winner yet -- must still be allowed to accumulate
        // normally, since it may go on to supersede the currently-promoted bank (see
        // `supersede_resolved_bank`).
        if self.resolved_bank_per_slot.get(&slot) == Some(&bank_id)
            && self.replayed_slot.contains_key(&slot)
        {
            log::error!(
                "UNEXPECTED: received block data for bank {bank_id} (slot {slot}) that is already sealed and replayed. Dropping.",
            );
            return;
        }
        self.register_bank_for_slot(slot, bank_id);
        let bank = self
            .banks
            .entry(bank_id)
            .or_insert_with(|| BankBuffer::new(bank_id, slot));
        bank.add_event(message);
        self.try_seal_bank(bank_id);
    }

    fn handle_block_meta(&mut self, block_meta: Arc<MessageBlockMeta>) {
        let bank_id = block_meta.bank_id;
        let slot = block_meta.slot;
        if self.discarded_bank_ids.contains(&bank_id) {
            return;
        }
        if self.resolved_bank_per_slot.get(&slot) == Some(&bank_id)
            && self.replayed_slot.contains_key(&slot)
        {
            return;
        }
        self.register_bank_for_slot(slot, bank_id);
        self.record_parent(slot, block_meta.parent_slot);
        let bank = self
            .banks
            .entry(bank_id)
            .or_insert_with(|| BankBuffer::new(bank_id, slot));
        if bank.parent_slot.is_none() {
            bank.parent_slot = Some(block_meta.parent_slot);
        }
        bank.blockmeta = Some(Arc::clone(&block_meta));
        self.try_seal_bank(bank_id);
    }

    fn gc(&mut self) {
        while self.replayed_slot.len() > self.replayed_capacity
            && self.num_buffered_finalized_slot > MINIMUM_FINALIZED_SLOT_TO_BUFFER
        {
            let Some((&oldest_slot, _)) = self.replayed_slot.iter().next() else {
                break;
            };
            self.evict_slot(oldest_slot);
        }
        self.refresh_min_slot();
    }

    fn evict_slot(&mut self, slot: Slot) {
        self.replayed_slot.remove(&slot);
        if let Some(progression) = self.slot_commitment_progression_map.remove(&slot) {
            if progression.max_commitment == CommitmentLevel::Finalized {
                self.num_buffered_finalized_slot =
                    self.num_buffered_finalized_slot.saturating_sub(1);
            }
        }
        self.slot_min_commitment.remove(&slot);
        self.resolved_bank_per_slot.remove(&slot);
        self.slot_to_banks.remove(&slot);
        self.parent_of.remove(&slot);
    }

    fn refresh_min_slot(&mut self) {
        self.min_slot = self.replayed_slot.keys().next().copied();
    }

    pub fn pop_ready_block(&mut self) -> Option<(SlotCommitmentStatusUpdate, Arc<FrozenBank>)> {
        self.ready_queue.pop_front()
    }

    pub fn replay_from_slot(&self, slot: Slot, min_commitment: CommitmentLevel) -> ReplayIter<'_> {
        let iter = self.replayed_slot.range(slot..);
        ReplayIter {
            storage: self,
            iter,
            min_commitment,
        }
    }

    pub const fn min_replayable_slot(&self) -> Option<Slot> {
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

    fn make_account_msg(slot: u64, bank_id: BankId, pubkey: Pubkey, write_version: u64) -> Message {
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
            bank_id: Some(bank_id),
        }))
    }

    fn make_startup_account_msg(slot: u64, pubkey: Pubkey) -> Message {
        Message::Account(Arc::new(MessageAccount {
            account: MessageAccountInfo {
                pubkey,
                lamports: 100,
                owner: Pubkey::default(),
                executable: false,
                rent_epoch: 0,
                data: Bytes::new(),
                write_version: 1,
                txn_signature: None,
                pre_encoded: OnceLock::new(),
            },
            slot,
            is_startup: true,
            created_at: ts(),
            bank_id: None,
        }))
    }

    fn make_transaction_msg(slot: u64, bank_id: BankId) -> Message {
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
            bank_id,
        }))
    }

    fn make_entry_msg(slot: u64, index: usize, bank_id: BankId) -> Message {
        Message::Entry(Arc::new(MessageEntry {
            slot,
            index,
            num_hashes: 1,
            hash: Hash::default(),
            executed_transaction_count: 0,
            starting_transaction_index: 0,
            created_at: ts(),
            bank_id,
        }))
    }

    fn make_created_bank_msg(slot: u64, parent: Option<u64>, bank_id: BankId) -> Message {
        Message::Slot(Arc::new(MessageSlot {
            slot,
            parent,
            status: SlotStatus::CreatedBank,
            dead_error: None,
            created_at: ts(),
            bank_id: Some(bank_id),
        }))
    }

    fn make_commitment_msg(
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
        bank_id: BankId,
    ) -> Message {
        Message::Slot(Arc::new(MessageSlot {
            slot,
            parent,
            status,
            dead_error: None,
            created_at: ts(),
            bank_id: Some(bank_id),
        }))
    }

    fn make_lifecycle_msg(slot: u64, status: SlotStatus) -> Message {
        Message::Slot(Arc::new(MessageSlot {
            slot,
            parent: None,
            status,
            dead_error: None,
            created_at: ts(),
            bank_id: None,
        }))
    }

    fn make_block_meta_msg(slot: u64, parent_slot: u64, bank_id: BankId) -> Message {
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
                bank_id,
            },
            ts(),
        )))
    }

    /// Drives one bank through to a sealed, Processed-delivered slot: CreatedBank, an
    /// entry, block meta, then Processed. Event order deliberately doesn't start with
    /// CreatedBank in most tests -- this helper is only for the common/simple case.
    fn drive_bank_to_processed(
        storage: &mut BlockMachineStorage,
        slot: u64,
        bank_id: BankId,
        parent: Option<u64>,
    ) {
        storage.add(make_created_bank_msg(slot, parent, bank_id));
        storage.add(make_entry_msg(slot, 0, bank_id));
        storage.add(make_block_meta_msg(slot, parent.unwrap_or(0), bank_id));
        storage.add(make_commitment_msg(
            slot,
            parent,
            SlotStatus::Processed,
            bank_id,
        ));
    }

    #[test]
    fn seals_regardless_of_event_order_and_ignores_data_without_bank_id() {
        let mut storage = BlockMachineStorage::new(10);
        let bank_id = 101;

        // Account/entry data arrives before CreatedBank -- must still be buffered, not
        // dropped, and must not require CreatedBank to have been seen first.
        storage.add(make_account_msg(1, bank_id, Pubkey::new_unique(), 1));
        storage.add(make_entry_msg(1, 0, bank_id));
        // A startup-snapshot account carries no bank_id and must be ignored entirely.
        storage.add(make_startup_account_msg(1, Pubkey::new_unique()));

        assert!(
            storage.pop_ready_block().is_none(),
            "not sealable yet: no CreatedBank, no blockmeta"
        );

        storage.add(make_created_bank_msg(1, None, bank_id));
        storage.add(make_block_meta_msg(1, 0, bank_id));
        storage.add(make_commitment_msg(1, None, SlotStatus::Processed, bank_id));

        let (update, frozen) = storage.pop_ready_block().expect("block should be ready");
        assert_eq!(update.slot, 1);
        assert_eq!(update.bank_id, bank_id);
        assert_eq!(update.commitment, CommitmentLevel::Processed);
        let accounts: Vec<_> = frozen
            .messages()
            .iter()
            .filter_map(|m| match m {
                Message::Account(a) => Some(a.account.pubkey),
                _ => None,
            })
            .collect();
        assert_eq!(
            accounts.len(),
            1,
            "only the bank-scoped account should survive, not the startup one"
        );
    }

    #[test]
    fn does_not_seal_without_created_bank() {
        let mut storage = BlockMachineStorage::new(10);
        let bank_id = 1;
        storage.add(make_entry_msg(1, 0, bank_id));
        storage.add(make_block_meta_msg(1, 0, bank_id));
        storage.add(make_commitment_msg(1, None, SlotStatus::Processed, bank_id));
        assert!(
            storage.pop_ready_block().is_none(),
            "must not seal without ever observing CreatedBank for this bank_id"
        );
    }

    #[test]
    fn discards_losing_bank_and_delivers_only_the_winner() {
        let mut storage = BlockMachineStorage::new(10);
        let slot = 5;
        let loser_bank = 500;
        let winner_bank = 501;
        let loser_pubkey = Pubkey::new_unique();
        let winner_pubkey = Pubkey::new_unique();

        // A first bank attempt for this slot accumulates some data...
        storage.add(make_created_bank_msg(slot, None, loser_bank));
        storage.add(make_account_msg(slot, loser_bank, loser_pubkey, 1));

        // ...then gets superseded by a second, genuinely different bank instance for the
        // same slot (e.g. a dump-and-repair replay).
        storage.add(make_created_bank_msg(slot, None, winner_bank));
        storage.add(make_account_msg(slot, winner_bank, winner_pubkey, 1));
        storage.add(make_entry_msg(slot, 0, winner_bank));
        storage.add(make_block_meta_msg(slot, 0, winner_bank));

        // Only the second bank ever gets a commitment update -- it's the one on the
        // canonical fork.
        storage.add(make_commitment_msg(
            slot,
            None,
            SlotStatus::Processed,
            winner_bank,
        ));

        let (update, frozen) = storage
            .pop_ready_block()
            .expect("winner's block should be ready");
        assert_eq!(update.bank_id, winner_bank);
        let pubkeys: Vec<_> = frozen
            .messages()
            .iter()
            .filter_map(|m| match m {
                Message::Account(a) => Some(a.account.pubkey),
                _ => None,
            })
            .collect();
        assert!(pubkeys.contains(&winner_pubkey));
        assert!(
            !pubkeys.contains(&loser_pubkey),
            "the losing bank's data must not leak into the winner's block"
        );
        assert!(
            storage.pop_ready_block().is_none(),
            "no separate delivery for the discarded loser"
        );
    }

    #[test]
    fn processed_resolution_can_be_superseded_by_a_later_bank() {
        let mut storage = BlockMachineStorage::new(10);
        let slot = 9;
        let first_bank = 900;
        let corrected_bank = 901;
        let first_pubkey = Pubkey::new_unique();
        let corrected_pubkey = Pubkey::new_unique();

        // An optimistic Processed bank gets delivered...
        storage.add(make_created_bank_msg(slot, None, first_bank));
        storage.add(make_account_msg(slot, first_bank, first_pubkey, 1));
        storage.add(make_entry_msg(slot, 0, first_bank));
        storage.add(make_block_meta_msg(slot, 0, first_bank));
        storage.add(make_commitment_msg(
            slot,
            None,
            SlotStatus::Processed,
            first_bank,
        ));
        let (update, _) = storage
            .pop_ready_block()
            .expect("first bank's Processed delivery");
        assert_eq!(update.bank_id, first_bank);

        // ...then a dump-and-repair replay produces a genuinely different bank for the same
        // slot, which also reaches Processed.
        storage.add(make_created_bank_msg(slot, None, corrected_bank));
        storage.add(make_account_msg(slot, corrected_bank, corrected_pubkey, 1));
        storage.add(make_entry_msg(slot, 0, corrected_bank));
        storage.add(make_block_meta_msg(slot, 0, corrected_bank));
        storage.add(make_commitment_msg(
            slot,
            None,
            SlotStatus::Processed,
            corrected_bank,
        ));

        let (update, frozen) = storage
            .pop_ready_block()
            .expect("corrected bank's Processed delivery should supersede the first one");
        assert_eq!(update.bank_id, corrected_bank);
        let pubkeys: Vec<_> = frozen
            .messages()
            .iter()
            .filter_map(|m| match m {
                Message::Account(a) => Some(a.account.pubkey),
                _ => None,
            })
            .collect();
        assert!(pubkeys.contains(&corrected_pubkey));
        assert!(!pubkeys.contains(&first_pubkey));

        // Replay must now serve the corrected bank's content, not the superseded one.
        let replayed: Vec<_> = storage
            .replay_from_slot(slot, CommitmentLevel::Processed)
            .map(|r| r.frozen_block.messages())
            .collect();
        assert_eq!(replayed.len(), 1);
        let replayed_pubkeys: Vec<_> = replayed[0]
            .iter()
            .filter_map(|m| match m {
                Message::Account(a) => Some(a.account.pubkey),
                _ => None,
            })
            .collect();
        assert!(replayed_pubkeys.contains(&corrected_pubkey));
        assert!(!replayed_pubkeys.contains(&first_pubkey));
    }

    #[test]
    fn confirmed_resolution_cannot_be_superseded() {
        let mut storage = BlockMachineStorage::new(10);
        let slot = 20;
        let confirmed_bank = 2000;
        let rogue_bank = 2001;

        drive_bank_to_processed(&mut storage, slot, confirmed_bank, None);
        storage.pop_ready_block();
        storage.add(make_commitment_msg(
            slot,
            None,
            SlotStatus::Confirmed,
            confirmed_bank,
        ));
        storage.pop_ready_block();

        // A later, different bank_id reporting commitment for the same slot after it has
        // already reached Confirmed must be rejected, not allowed to clobber it.
        storage.add(make_created_bank_msg(slot, None, rogue_bank));
        storage.add(make_entry_msg(slot, 0, rogue_bank));
        storage.add(make_block_meta_msg(slot, 0, rogue_bank));
        storage.add(make_commitment_msg(
            slot,
            None,
            SlotStatus::Processed,
            rogue_bank,
        ));

        assert!(
            storage.pop_ready_block().is_none(),
            "a bank_id arriving after Confirmed must not produce a delivery"
        );
        let replayed: Vec<_> = storage
            .replay_from_slot(slot, CommitmentLevel::Processed)
            .map(|r| r.frozen_block.messages())
            .collect();
        assert_eq!(
            replayed.len(),
            1,
            "the Confirmed bank's content must still be the one served"
        );
    }

    #[test]
    fn retroactively_confirms_unconfirmed_ancestor() {
        let mut storage = BlockMachineStorage::new(10);
        let parent_bank = 1;
        let child_bank = 2;

        drive_bank_to_processed(&mut storage, 10, parent_bank, None);
        assert_eq!(
            storage.pop_ready_block().map(|(u, _)| u.commitment),
            Some(CommitmentLevel::Processed)
        );

        drive_bank_to_processed(&mut storage, 11, child_bank, Some(10));
        assert_eq!(
            storage.pop_ready_block().map(|(u, _)| u.commitment),
            Some(CommitmentLevel::Processed)
        );

        // Confirm the child directly. The parent (slot 10) never got its own Confirmed
        // update -- it must be retroactively confirmed too.
        storage.add(make_commitment_msg(
            11,
            Some(10),
            SlotStatus::Confirmed,
            child_bank,
        ));

        let mut seen = Vec::new();
        while let Some((update, _)) = storage.pop_ready_block() {
            seen.push((update.slot, update.commitment));
        }
        assert!(
            seen.contains(&(10, CommitmentLevel::Confirmed)),
            "parent slot must be retroactively confirmed: {seen:?}"
        );
        assert!(
            seen.contains(&(11, CommitmentLevel::Confirmed)),
            "child slot must be confirmed directly: {seen:?}"
        );
    }

    #[test]
    fn ancestor_without_its_own_commitment_update_still_resolves_via_retroactive_propagation() {
        let mut storage = BlockMachineStorage::new(10);
        let ancestor_bank = 1;
        let descendant_bank = 2;

        // The ancestor's bank seals but never gets a direct commitment status of its own --
        // geyser does not guarantee one arrives for every slot.
        storage.add(make_created_bank_msg(10, None, ancestor_bank));
        storage.add(make_entry_msg(10, 0, ancestor_bank));
        storage.add(make_block_meta_msg(10, 0, ancestor_bank));
        assert!(
            storage.pop_ready_block().is_none(),
            "no delivery yet -- no commitment update at all has arrived for slot 10"
        );

        // The descendant jumps straight to Finalized, skipping Processed/Confirmed for
        // itself too, to also exercise same-slot gap-filling in the same pass.
        storage.add(make_created_bank_msg(11, Some(10), descendant_bank));
        storage.add(make_entry_msg(11, 0, descendant_bank));
        storage.add(make_block_meta_msg(11, 10, descendant_bank));
        storage.add(make_commitment_msg(
            11,
            Some(10),
            SlotStatus::Finalized,
            descendant_bank,
        ));

        let mut seen = Vec::new();
        while let Some((update, _)) = storage.pop_ready_block() {
            seen.push((update.slot, update.commitment));
        }
        for level in [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ] {
            assert!(
                seen.contains(&(10, level)),
                "ancestor slot 10 must reach {level:?} purely via retroactive propagation from slot 11: {seen:?}"
            );
            assert!(
                seen.contains(&(11, level)),
                "descendant slot 11 must reach {level:?} via same-slot gap-filling: {seen:?}"
            );
        }
    }

    #[test]
    fn dead_slot_discards_all_its_banks() {
        let mut storage = BlockMachineStorage::new(10);
        let bank_id = 42;
        storage.add(make_created_bank_msg(7, None, bank_id));
        storage.add(make_account_msg(7, bank_id, Pubkey::new_unique(), 1));
        storage.add(make_lifecycle_msg(7, SlotStatus::Dead));
        storage.add(make_entry_msg(7, 0, bank_id));
        storage.add(make_block_meta_msg(7, 0, bank_id));
        storage.add(make_commitment_msg(7, None, SlotStatus::Processed, bank_id));
        assert!(
            storage.pop_ready_block().is_none(),
            "a bank abandoned after Dead should never seal into a delivered block"
        );
    }

    #[test]
    fn replay_from_slot_respects_min_commitment() {
        let mut storage = BlockMachineStorage::new(10);
        drive_bank_to_processed(&mut storage, 1, 1, None);
        storage.pop_ready_block();
        drive_bank_to_processed(&mut storage, 2, 2, None);
        storage.pop_ready_block();
        storage.add(make_commitment_msg(1, None, SlotStatus::Confirmed, 1));
        storage.pop_ready_block();

        let confirmed_only: Vec<_> = storage
            .replay_from_slot(1, CommitmentLevel::Confirmed)
            .map(|r| r.frozen_block.slot)
            .collect();
        assert_eq!(
            confirmed_only,
            vec![1],
            "slot 2 is only Processed, must be excluded"
        );
    }

    #[test]
    fn min_replayable_slot_tracks_earliest_sealed_slot() {
        let mut storage = BlockMachineStorage::new(10);
        assert_eq!(storage.min_replayable_slot(), None);
        drive_bank_to_processed(&mut storage, 5, 1, None);
        assert_eq!(storage.min_replayable_slot(), Some(5));
        drive_bank_to_processed(&mut storage, 3, 2, None);
        assert_eq!(storage.min_replayable_slot(), Some(3));
    }
}
