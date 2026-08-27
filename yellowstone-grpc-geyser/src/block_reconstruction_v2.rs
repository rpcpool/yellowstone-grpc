use {
    crate::{
        metrics,
        plugin::message::{
            Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageEntry, MessageSlot,
            MessageTransaction, SlotStatus,
        },
    },
    foldhash::{HashMap as FoldHashMap, HashMapExt},
    solana_clock::{BankId, Slot},
    solana_commitment_config::CommitmentLevel,
    solana_pubkey::Pubkey,
    std::{
        collections::{btree_map::Range, BTreeMap, VecDeque},
        sync::Arc,
    },
};

// Sysvars a bank must be observed to have written before it's allowed to seal. Clock and
// SlotHashes are rewritten inline as part of bank construction -- before `CreatedBank`
// itself fires -- for every single bank instance (confirmed against Agave's own source);
// SlotHistory and RecentBlockhashes are written later, as the block freezes. Requiring all
// four here is only safe because buffering is keyed by bank_id: an earlier, slot-keyed
// version of this pipeline required the same thing and stalled sealing forever, permanently,
// for every slot in production, because a spurious "bank reset" (a `FirstShredReceived`
// placeholder mistaken for a real prior bank -- a slot-keyed-only failure mode) discarded
// the sysvar writes that land right before `CreatedBank`, and a slot's bank only ever gets
// one shot at emitting them. Keyed by bank_id, nothing can clobber a bank's own buffer, so
// there's no equivalent failure mode -- and if a bank genuinely never sees one of these for
// some other reason, the blast radius is just that one bank never sealing (eventually swept
// by `sweep_stale_slots`), not every slot in the pipeline.
const MUST_HAVE_SYSVAR_ACCOUNTS: [Pubkey; 4] = [
    Pubkey::from_str_const("SysvarC1ock11111111111111111111111111111111"),
    Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111"),
    Pubkey::from_str_const("SysvarS1otHistory11111111111111111111111111"),
    Pubkey::from_str_const("SysvarRecentB1ockHashes11111111111111111111"),
];

const fn commitment_rank(level: CommitmentLevel) -> u8 {
    match level {
        CommitmentLevel::Processed => 0,
        CommitmentLevel::Confirmed => 1,
        CommitmentLevel::Finalized => 2,
    }
}

const MINIMUM_FINALIZED_SLOT_TO_BUFFER: usize = 10;

// How many slots' worth of lag before genuinely in-flight (unresolved) state for a slot is
// considered abandoned and swept -- e.g. a bank that seals but whose slot never receives any
// commitment update naming a winner (validator restart, a fork that's silently abandoned
// without ever reaching Dead, etc.), or a slot that stays ambiguous between multiple
// candidate banks forever. Must comfortably exceed normal Confirmed/Finalized lag (typically
// well under 100 slots even under load) so in-flight processing is never swept prematurely --
// this is strictly a backstop against genuinely orphaned state, not a normal code path.
const STALE_SLOT_THRESHOLD: Slot = 300;
// How often (in slots of forward progress) to run the stale-slot sweep. A full scan on every
// `add()` call would be wasteful; running it only this often keeps the amortized cost
// negligible relative to normal message volume.
const STALE_GC_INTERVAL: Slot = 50;

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
    // Bit `i` set means `MUST_HAVE_SYSVAR_ACCOUNTS[i]` has been observed for this bank.
    musthave_sysvar_accounts_bitmask: u8,
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
            musthave_sysvar_accounts_bitmask: 0,
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
                if let Some(position) = MUST_HAVE_SYSVAR_ACCOUNTS
                    .iter()
                    .position(|pubkey| pubkey == &message_account.account.pubkey)
                {
                    self.musthave_sysvar_accounts_bitmask |= 1 << position;
                }
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
        if self.musthave_sysvar_accounts_bitmask != (1 << MUST_HAVE_SYSVAR_ACCOUNTS.len()) - 1 {
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

/// Yields one `ReplayedSlot` per replay-servable candidate bank, in ascending slot order.
/// A resolved slot (Confirmed/Finalized, or the common single-candidate case) yields
/// exactly one; a slot still genuinely ambiguous between two or more Processed banks with
/// no confirmed winner yet yields one per candidate, all at the same slot number -- there
/// is no other way to expose "no winner is clear yet" through this API.
pub struct ReplayIter<'storage> {
    storage: &'storage BlockMachineStorage,
    iter: Range<'storage, Slot, Vec<Arc<FrozenBank>>>,
    min_commitment: CommitmentLevel,
    current: std::slice::Iter<'storage, Arc<FrozenBank>>,
}

impl<'storage> Iterator for ReplayIter<'storage> {
    type Item = ReplayedSlot<'storage>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(frozen) = self.current.next() {
                return Some(ReplayedSlot {
                    frozen_block: frozen.as_ref(),
                    slot_status_messages: self.storage.replay_status_for(frozen),
                });
            }
            let (&slot, candidates) = self.iter.next()?;
            // A slot with no recorded progression at all is purely sealed-and-waiting --
            // treat it as Processed-level-available (it can't be anything higher without a
            // progression entry), matching `try_infer_sole_candidate_winner`'s inference.
            let max_commitment = self
                .storage
                .slot_commitment_progression_map
                .get(&slot)
                .map_or(CommitmentLevel::Processed, |p| p.max_commitment);
            if commitment_rank(max_commitment) < commitment_rank(self.min_commitment) {
                continue;
            }
            self.current = candidates.iter();
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
    // Every bank_id ever seen for a slot, so the losers can be found once a winner is known.
    slot_to_banks: FoldHashMap<Slot, Vec<BankId>>,
    // bank_ids explicitly discarded as losers (via `discard_losing_banks`) or abandoned
    // (via `handle_dead_slot`), paired with the slot they belonged to so `sweep_stale_slots`
    // can age them out. A straggler event arriving afterward for one of these is ignored
    // outright rather than reviving a fresh buffer for it -- distinct from a bank_id that
    // simply hasn't been resolved as a winner *yet*, which must still be allowed to
    // accumulate normally (see `processed_banks_are_peers_until_one_is_confirmed`).
    discarded_bank_ids: FoldHashMap<BankId, Slot>,
    // The bank_id currently believed to be a slot's canonical bank. Multiple banks for the
    // same slot can be simultaneously Processed with no precedence between them -- a
    // Processed sighting never sets or changes this (see `handle_commitment_update`) --
    // only Confirmed/Finalized status updates do, since Solana guarantees at most one bank
    // per slot ever reaches those. It can still change once, from a bank that was only ever
    // Processed to a different one that reaches Confirmed first (see
    // `supersede_resolved_bank`) -- e.g. a dump-and-repair replay correcting an earlier
    // bank whose hash was wrong -- but once a slot reaches Confirmed or Finalized this is
    // treated as final and can no longer change. The one exception is
    // `try_infer_sole_candidate_winner`'s single-candidate inference, which may set this
    // ahead of any direct status update when there's only ever been one bank_id for the
    // slot at all.
    resolved_bank_per_slot: FoldHashMap<Slot, BankId>,
    // The highest commitment level a slot must be treated as having reached, whether from a
    // direct status update or inherited from a confirmed/finalized descendant.
    slot_min_commitment: FoldHashMap<Slot, CommitmentLevel>,
    slot_commitment_progression_map: FoldHashMap<Slot, SlotProgression>,
    // Content available for replay, per slot. Holds every sealed candidate bank for a slot
    // that hasn't resolved a winner yet -- so a replay request can be served even while a
    // slot is genuinely ambiguous between two or more banks, all at Processed, with none
    // having precedence -- and collapses to exactly the one resolved bank the moment the
    // slot reaches Confirmed or Finalized (see `discard_losing_banks`). Never empty for a
    // tracked slot: an entry only exists once at least one candidate has sealed.
    replayed_slot: BTreeMap<Slot, Vec<Arc<FrozenBank>>>,
    ready_queue: VecDeque<(SlotCommitmentStatusUpdate, Arc<FrozenBank>)>,
    replayed_capacity: usize,
    num_buffered_finalized_slot: usize,
    min_slot: Option<Slot>,
    // Highest slot number observed across any message so far, and the value it had at the
    // last stale-slot sweep -- together these throttle `sweep_stale_slots` to run only every
    // `STALE_GC_INTERVAL` slots of forward progress instead of on every single message.
    max_slot_seen: Slot,
    last_stale_gc_at: Slot,
}

impl BlockMachineStorage {
    pub fn new(replayed_capacity: usize) -> Self {
        Self {
            banks: FoldHashMap::with_capacity(replayed_capacity),
            slot_to_banks: FoldHashMap::with_capacity(replayed_capacity),
            discarded_bank_ids: FoldHashMap::new(),
            resolved_bank_per_slot: FoldHashMap::with_capacity(replayed_capacity),
            slot_min_commitment: FoldHashMap::with_capacity(replayed_capacity),
            slot_commitment_progression_map: FoldHashMap::with_capacity(replayed_capacity),
            replayed_slot: BTreeMap::new(),
            ready_queue: VecDeque::with_capacity(replayed_capacity),
            replayed_capacity,
            num_buffered_finalized_slot: 0,
            min_slot: None,
            max_slot_seen: 0,
            last_stale_gc_at: 0,
        }
    }

    pub fn add(&mut self, message: Message) {
        self.observe_slot(message.get_slot());
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

    /// Tracks forward progress and periodically sweeps genuinely orphaned per-slot state
    /// that fell too far behind to ever plausibly resolve -- see `STALE_SLOT_THRESHOLD`.
    fn observe_slot(&mut self, slot: Slot) {
        if slot <= self.max_slot_seen {
            return;
        }
        self.max_slot_seen = slot;
        if self.max_slot_seen - self.last_stale_gc_at >= STALE_GC_INTERVAL {
            self.sweep_stale_slots();
            self.last_stale_gc_at = self.max_slot_seen;
        }
    }

    /// Backstop garbage collection for per-slot/per-bank state that `gc()` (capacity-based,
    /// triggered only for slots that successfully reached `replayed_slot`) never reaches:
    /// a bank that seals but whose slot never gets a commitment update naming any winner, a
    /// slot left ambiguous between multiple never-resolved candidate banks, or a discarded
    /// bank_id sitting in `discarded_bank_ids` forever. Slots already in `replayed_slot` are
    /// deliberately left alone here -- they're bounded by `gc()` instead, and remain valid
    /// replay content regardless of age.
    fn sweep_stale_slots(&mut self) {
        let cutoff = self.max_slot_seen.saturating_sub(STALE_SLOT_THRESHOLD);

        let stale_slots: Vec<Slot> = self
            .slot_to_banks
            .keys()
            .copied()
            .filter(|&slot| slot < cutoff && !self.replayed_slot.contains_key(&slot))
            .collect();
        for slot in stale_slots {
            if let Some(bank_ids) = self.slot_to_banks.remove(&slot) {
                for bank_id in bank_ids {
                    self.banks.remove(&bank_id);
                }
            }
            self.resolved_bank_per_slot.remove(&slot);
            self.slot_min_commitment.remove(&slot);
            self.slot_commitment_progression_map.remove(&slot);
            log::warn!(
                "garbage-collecting slot {slot}: never resolved within {STALE_SLOT_THRESHOLD} slots of the latest seen ({}) -- likely an abandoned bank that never received a commitment update",
                self.max_slot_seen
            );
        }

        self.discarded_bank_ids
            .retain(|_, &mut slot| slot >= cutoff);
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

    fn handle_created_bank(&mut self, message_slot: &MessageSlot) {
        let Some(bank_id) = message_slot.bank_id else {
            log::warn!(
                "CreatedBank status for slot {} carries no bank_id; ignoring",
                message_slot.slot
            );
            return;
        };
        if self.discarded_bank_ids.contains_key(&bank_id) {
            return;
        }
        let slot = message_slot.slot;
        self.register_bank_for_slot(slot, bank_id);
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
                self.discarded_bank_ids.insert(bank_id, slot);
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
        if self.discarded_bank_ids.contains_key(&bank_id) {
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
            // Bank-scoped, not slot-scoped: two banks for the same slot can legitimately
            // disagree on their parent (skipped slots are normal, and an equivocating
            // leader isn't required to pick the same parent for both of its conflicting
            // versions), so this must only ever update *this* bank_id's own record, never
            // a slot-keyed cache shared across bank_ids.
            if let Some(bank) = self.banks.get_mut(&bank_id) {
                if bank.parent_slot.is_none() {
                    bank.parent_slot = Some(parent);
                }
            }
        }

        // Processed is optimistic, and multiple banks for the same slot can legitimately
        // be simultaneously Processed with no precedence between any of them -- so a
        // Processed sighting must never resolve, supersede, or discard anything. Only
        // Confirmed/Finalized is authoritative: Solana guarantees at most one bank per
        // slot ever reaches it, and once it does, that resolution is final.
        if commitment != CommitmentLevel::Processed {
            if let Some(&existing) = self.resolved_bank_per_slot.get(&slot) {
                if existing != bank_id {
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
        }

        self.bump_slot_commitment(slot, commitment);
    }

    /// Replaces `slot`'s resolved bank_id, dropping the live-delivery history that was
    /// built around the old (now-superseded) one so any subsequent commitment delivery is
    /// synthesized fresh for the new winner instead of being deduped against stale entries.
    /// `slot_min_commitment` is deliberately left untouched: it's the slot's target
    /// commitment level, independent of which bank_id ultimately satisfies it, so it stays
    /// valid across the swap. `replayed_slot` isn't touched here either -- the caller
    /// always follows this with `discard_losing_banks`, which collapses it down to just the
    /// new winner while preserving that winner's own already-sealed content if present.
    fn supersede_resolved_bank(&mut self, slot: Slot, new_winner: BankId) {
        self.resolved_bank_per_slot.insert(slot, new_winner);
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
            self.discarded_bank_ids.insert(loser, slot);
        }
        // Collapse the replay-servable set down to just the winner -- once a slot resolves,
        // any other candidate that had been sitting there while it was still ambiguous is
        // no longer valid replay content (see the module doc comment).
        if let Some(candidates) = self.replayed_slot.get_mut(&slot) {
            candidates.retain(|c| c.bank_id == winner);
        }
    }

    /// Raises the effective commitment floor for `slot` to `level` (a no-op for the floor
    /// itself if it's already there or higher -- but the resolved winner's delivery is
    /// still (re-)attempted regardless, since a `supersede_resolved_bank` can clear
    /// delivered history without changing the floor), emits the corresponding update(s)
    /// immediately if the resolved winner's content is available, and retroactively
    /// propagates the same floor up the parent chain the first time it's raised, so an
    /// ancestor that never gets its own direct commitment status update from geyser (this
    /// does happen -- geyser does not guarantee one arrives for every slot) still ends up
    /// at least at its descendants' commitment level.
    fn bump_slot_commitment(&mut self, slot: Slot, level: CommitmentLevel) {
        let already_at_level = self
            .slot_min_commitment
            .get(&slot)
            .is_some_and(|&current| commitment_rank(current) >= commitment_rank(level));
        if !already_at_level {
            self.slot_min_commitment.insert(slot, level);
        }

        self.try_infer_sole_candidate_winner(slot);
        if let Some(frozen) = self.resolved_frozen_bank(slot) {
            self.emit_commitment_levels_up_to(slot, level, &frozen);
        }

        if already_at_level {
            // The floor was already raised (and thus already propagated to ancestors) on
            // an earlier call; nothing further to walk up for.
            return;
        }
        if let Some(parent) = self.resolved_parent_of(slot) {
            self.bump_slot_commitment(parent, level);
        }
    }

    /// The parent slot of whichever bank_id is *currently* resolved as `slot`'s canonical
    /// bank -- looked up fresh from that bank's own record every time, never cached
    /// per-slot. Two banks for the same slot can legitimately disagree on their parent
    /// (skipped slots are normal, and an equivocating leader isn't required to pick the
    /// same parent for both of its conflicting versions), so the only parent that's ever
    /// safe to use is the one belonging to the specific bank instance actually being
    /// treated as canonical right now -- which can itself change via `supersede_resolved_bank`.
    fn resolved_parent_of(&self, slot: Slot) -> Option<Slot> {
        let bank_id = *self.resolved_bank_per_slot.get(&slot)?;
        if let Some(bank) = self.banks.get(&bank_id) {
            return bank.parent_slot;
        }
        self.replay_candidate(slot, bank_id)
            .map(|frozen| frozen.block_meta.parent_slot)
    }

    /// The resolved winner's frozen content for `slot`, if it has both a known resolution
    /// and sealed (replay-servable) content for that specific bank_id.
    fn resolved_frozen_bank(&self, slot: Slot) -> Option<Arc<FrozenBank>> {
        let bank_id = *self.resolved_bank_per_slot.get(&slot)?;
        self.replay_candidate(slot, bank_id).cloned()
    }

    /// The replay candidate for `slot` belonging to `bank_id`, if it has sealed.
    fn replay_candidate(&self, slot: Slot, bank_id: BankId) -> Option<&Arc<FrozenBank>> {
        self.replayed_slot
            .get(&slot)?
            .iter()
            .find(|frozen| frozen.bank_id == bank_id)
    }

    /// Adds `frozen` as a candidate available for replay at its slot. Idempotent: a second
    /// call for a bank_id already present is a no-op, so callers never need to check first.
    fn add_replay_candidate(&mut self, slot: Slot, frozen: Arc<FrozenBank>) {
        let candidates = self.replayed_slot.entry(slot).or_default();
        if !candidates.iter().any(|c| c.bank_id == frozen.bank_id) {
            candidates.push(frozen);
        }
    }

    /// Infers `slot`'s resolved winning bank_id when it hasn't been named by a direct
    /// Confirmed/Finalized status update yet but there's exactly one known candidate for
    /// it -- geyser doesn't guarantee a direct commitment status update arrives for every
    /// slot (see `resolved_bank_per_slot`). With more than one still-unresolved candidate,
    /// which one is canonical genuinely can't be determined here, so it's left unresolved
    /// and logged (both remain available for Processed-level replay regardless).
    fn try_infer_sole_candidate_winner(&mut self, slot: Slot) {
        if self.resolved_bank_per_slot.contains_key(&slot) {
            return;
        }
        match self.slot_to_banks.get(&slot).map(Vec::as_slice) {
            Some([single]) => {
                self.resolved_bank_per_slot.insert(slot, *single);
            }
            Some(ids) if ids.len() > 1 => {
                log::warn!(
                    "slot {slot} needs to resolve (e.g. a descendant's commitment level is propagating up to it) but has {} competing bank_ids and none was ever directly confirmed -- cannot determine which is canonical",
                    ids.len()
                );
            }
            _ => {}
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

    /// A sealed bank becomes replay-servable immediately, regardless of whether its slot
    /// has resolved a winner yet -- multiple banks can be simultaneously replay-servable
    /// while a slot is still ambiguous between them (see the module doc comment on
    /// `replayed_slot`). `discard_losing_banks` collapses this down to just the winner once
    /// the slot resolves. If this bank is (or becomes, via single-candidate inference) the
    /// resolved winner and the slot already has a known commitment floor -- e.g. inherited
    /// from a descendant before this bank even sealed -- its delivery is emitted now too.
    fn on_bank_sealed(&mut self, slot: Slot, bank_id: BankId, frozen: Arc<FrozenBank>) {
        self.min_slot = Some(self.min_slot.map_or(slot, |m| m.min(slot)));
        self.add_replay_candidate(slot, frozen);
        self.try_infer_sole_candidate_winner(slot);
        if self.resolved_bank_per_slot.get(&slot) == Some(&bank_id) {
            if let Some(&level) = self.slot_min_commitment.get(&slot) {
                if let Some(frozen) = self.resolved_frozen_bank(slot) {
                    self.emit_commitment_levels_up_to(slot, level, &frozen);
                }
            }
        }
    }

    fn emit_commitment_levels_up_to(
        &mut self,
        slot: Slot,
        level: CommitmentLevel,
        frozen: &Arc<FrozenBank>,
    ) {
        let bank_id = frozen.bank_id;
        // Read directly off this specific bank's own blockmeta -- authoritative and
        // always in sync with whichever bank is actually being delivered, unlike a
        // slot-keyed cache (two banks for the same slot can disagree on their parent).
        let parent_slot = Some(frozen.block_meta.parent_slot);
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
        if self.discarded_bank_ids.contains_key(&bank_id) {
            return;
        }
        // A bank that has already sealed is immutable -- more data for it afterward is
        // always anomalous, whether or not its slot has resolved a winner yet.
        if self.replay_candidate(slot, bank_id).is_some() {
            log::error!(
                "UNEXPECTED: received block data for bank {bank_id} (slot {slot}) that is already sealed. Dropping.",
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
        if self.discarded_bank_ids.contains_key(&bank_id) {
            return;
        }
        if self.replay_candidate(slot, bank_id).is_some() {
            return;
        }
        self.register_bank_for_slot(slot, bank_id);
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
            current: [].iter(),
        }
    }

    pub const fn min_replayable_slot(&self) -> Option<Slot> {
        self.min_slot
    }

    /// The status update history to attach to a replayed candidate bank. For the single
    /// resolved winner of a slot with a recorded progression, this is the real
    /// Processed/Confirmed/Finalized history. For one of several still-ambiguous
    /// candidates (or a resolved winner whose progression was never recorded, e.g. sealed
    /// purely via single-candidate inference with no direct status update at all), only a
    /// synthesized Processed entry for that specific bank is returned, since that's the
    /// only thing ever actually confirmed about it.
    fn replay_status_for(&self, frozen: &FrozenBank) -> Vec<SlotCommitmentStatusUpdate> {
        if self.resolved_bank_per_slot.get(&frozen.slot) == Some(&frozen.bank_id) {
            if let Some(progression) = self.slot_commitment_progression_map.get(&frozen.slot) {
                if !progression.commitment.is_empty() {
                    return progression.commitment.clone();
                }
            }
        }
        vec![SlotCommitmentStatusUpdate {
            slot: frozen.slot,
            parent_slot: Some(frozen.block_meta.parent_slot),
            commitment: CommitmentLevel::Processed,
            bank_id: frozen.bank_id,
        }]
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

    /// Feeds all four `MUST_HAVE_SYSVAR_ACCOUNTS` writes for `bank_id` -- required for any
    /// bank to be sealable now that `try_seal` hard-requires them.
    fn add_musthave_sysvars(storage: &mut BlockMachineStorage, slot: u64, bank_id: BankId) {
        for pubkey in MUST_HAVE_SYSVAR_ACCOUNTS {
            storage.add(make_account_msg(slot, bank_id, pubkey, 1));
        }
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
        add_musthave_sysvars(storage, slot, bank_id);
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

        let ordinary_pubkey = Pubkey::new_unique();

        // Account/entry data arrives before CreatedBank -- must still be buffered, not
        // dropped, and must not require CreatedBank to have been seen first.
        storage.add(make_account_msg(1, bank_id, ordinary_pubkey, 1));
        storage.add(make_entry_msg(1, 0, bank_id));
        // A startup-snapshot account carries no bank_id and must be ignored entirely.
        storage.add(make_startup_account_msg(1, Pubkey::new_unique()));

        assert!(
            storage.pop_ready_block().is_none(),
            "not sealable yet: no CreatedBank, no blockmeta, no sysvars"
        );

        storage.add(make_created_bank_msg(1, None, bank_id));
        add_musthave_sysvars(&mut storage, 1, bank_id);
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
        assert!(
            accounts.contains(&ordinary_pubkey),
            "the bank-scoped account seen before CreatedBank must survive"
        );
        assert!(
            !accounts.contains(&Pubkey::default()),
            "no accidental default/startup account should survive"
        );
        assert_eq!(
            accounts.len(),
            1 + MUST_HAVE_SYSVAR_ACCOUNTS.len(),
            "the bank-scoped account plus the sysvars, and nothing else (not the startup one)"
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
    fn does_not_seal_until_every_musthave_sysvar_is_observed() {
        let mut storage = BlockMachineStorage::new(10);
        let bank_id = 1;

        storage.add(make_created_bank_msg(1, None, bank_id));
        // All but the last must-have sysvar -- deliberately incomplete.
        for &pubkey in &MUST_HAVE_SYSVAR_ACCOUNTS[..MUST_HAVE_SYSVAR_ACCOUNTS.len() - 1] {
            storage.add(make_account_msg(1, bank_id, pubkey, 1));
        }
        storage.add(make_entry_msg(1, 0, bank_id));
        storage.add(make_block_meta_msg(1, 0, bank_id));
        storage.add(make_commitment_msg(1, None, SlotStatus::Processed, bank_id));
        assert!(
            storage.pop_ready_block().is_none(),
            "must not seal while any must-have sysvar account is still missing"
        );

        let last = MUST_HAVE_SYSVAR_ACCOUNTS[MUST_HAVE_SYSVAR_ACCOUNTS.len() - 1];
        storage.add(make_account_msg(1, bank_id, last, 1));
        assert!(
            storage.pop_ready_block().is_some(),
            "should seal once the last must-have sysvar account arrives"
        );
    }

    #[test]
    fn replay_yields_every_candidate_until_one_is_confirmed_then_only_the_winner() {
        let mut storage = BlockMachineStorage::new(10);
        let slot = 40;
        let bank_a = 4000;
        let bank_b = 4001;
        let pubkey_a = Pubkey::new_unique();
        let pubkey_b = Pubkey::new_unique();

        // Two competing banks for the same slot both seal -- neither ever gets a direct
        // commitment update, so no winner is clear yet.
        storage.add(make_created_bank_msg(slot, None, bank_a));
        storage.add(make_account_msg(slot, bank_a, pubkey_a, 1));
        add_musthave_sysvars(&mut storage, slot, bank_a);
        storage.add(make_entry_msg(slot, 0, bank_a));
        storage.add(make_block_meta_msg(slot, 0, bank_a));

        storage.add(make_created_bank_msg(slot, None, bank_b));
        storage.add(make_account_msg(slot, bank_b, pubkey_b, 1));
        add_musthave_sysvars(&mut storage, slot, bank_b);
        storage.add(make_entry_msg(slot, 0, bank_b));
        storage.add(make_block_meta_msg(slot, 0, bank_b));

        // A Processed-level replay request must yield both, since no winner is clear.
        let replayed: Vec<_> = storage
            .replay_from_slot(slot, CommitmentLevel::Processed)
            .collect();
        assert_eq!(
            replayed.len(),
            2,
            "both unresolved candidates must be replay-servable at Processed"
        );
        let replayed_bank_ids: Vec<_> = replayed
            .iter()
            .map(|r| {
                r.slot_status_messages
                    .first()
                    .expect("each candidate carries at least a Processed status")
                    .bank_id
            })
            .collect();
        assert!(replayed_bank_ids.contains(&bank_a));
        assert!(replayed_bank_ids.contains(&bank_b));

        // A Confirmed-level replay request must yield nothing yet -- neither bank has
        // actually reached Confirmed.
        assert_eq!(
            storage
                .replay_from_slot(slot, CommitmentLevel::Confirmed)
                .count(),
            0,
            "an unresolved slot must not satisfy a Confirmed-level replay request"
        );

        // bank_b is confirmed -- it becomes the sole winner, and bank_a must be discarded
        // from replay entirely.
        storage.add(make_commitment_msg(
            slot,
            None,
            SlotStatus::Confirmed,
            bank_b,
        ));

        let replayed: Vec<_> = storage
            .replay_from_slot(slot, CommitmentLevel::Processed)
            .collect();
        assert_eq!(
            replayed.len(),
            1,
            "only the confirmed winner should remain replay-servable"
        );
        let pubkeys: Vec<_> = replayed[0]
            .frozen_block
            .messages()
            .iter()
            .filter_map(|m| match m {
                Message::Account(a) => Some(a.account.pubkey),
                _ => None,
            })
            .collect();
        assert!(pubkeys.contains(&pubkey_b));
        assert!(!pubkeys.contains(&pubkey_a));
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
        add_musthave_sysvars(&mut storage, slot, winner_bank);
        storage.add(make_entry_msg(slot, 0, winner_bank));
        storage.add(make_block_meta_msg(slot, 0, winner_bank));

        // Only the second bank ever gets a commitment update -- it's the one on the
        // canonical fork. Confirmed, not Processed: with two competing bank_ids still
        // registered for this slot, only Confirmed/Finalized resolves which one wins (see
        // `processed_banks_are_peers_until_one_is_confirmed`) -- Processed alone wouldn't
        // discard the loser or promote the winner here.
        storage.add(make_commitment_msg(
            slot,
            None,
            SlotStatus::Confirmed,
            winner_bank,
        ));

        // winner_bank never got its own Processed status directly, only Confirmed, so this
        // also gap-fills the missing Processed level -- both deliveries are the winner's.
        let mut deliveries = Vec::new();
        while let Some((update, frozen)) = storage.pop_ready_block() {
            deliveries.push((update, frozen));
        }
        assert!(!deliveries.is_empty(), "winner's block should be ready");
        for (update, frozen) in &deliveries {
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
        }
    }

    #[test]
    fn processed_banks_are_peers_until_one_is_confirmed() {
        let mut storage = BlockMachineStorage::new(10);
        let slot = 9;
        let first_bank = 900;
        let corrected_bank = 901;
        let first_pubkey = Pubkey::new_unique();
        let corrected_pubkey = Pubkey::new_unique();

        // The first bank reaches Processed and gets delivered.
        storage.add(make_created_bank_msg(slot, None, first_bank));
        storage.add(make_account_msg(slot, first_bank, first_pubkey, 1));
        add_musthave_sysvars(&mut storage, slot, first_bank);
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

        // A second, genuinely different bank for the same slot also reaches Processed --
        // this must NOT supersede or discard the first. Multiple Processed banks are peers
        // with no precedence between them; only Confirmed/Finalized is authoritative.
        storage.add(make_created_bank_msg(slot, None, corrected_bank));
        storage.add(make_account_msg(slot, corrected_bank, corrected_pubkey, 1));
        add_musthave_sysvars(&mut storage, slot, corrected_bank);
        storage.add(make_entry_msg(slot, 0, corrected_bank));
        storage.add(make_block_meta_msg(slot, 0, corrected_bank));
        storage.add(make_commitment_msg(
            slot,
            None,
            SlotStatus::Processed,
            corrected_bank,
        ));
        assert!(
            storage.pop_ready_block().is_none(),
            "a second Processed bank must not produce a delivery or discard the first"
        );

        // Only once the second bank reaches Confirmed does it become the slot's permanent,
        // canonical bank -- correctly superseding the (still merely Processed) first one.
        storage.add(make_commitment_msg(
            slot,
            None,
            SlotStatus::Confirmed,
            corrected_bank,
        ));
        // corrected_bank never got its own Processed status directly, so this also
        // gap-fills the missing Processed level -- both deliveries are corrected_bank's.
        let mut deliveries = Vec::new();
        while let Some((update, frozen)) = storage.pop_ready_block() {
            deliveries.push((update, frozen));
        }
        assert!(
            deliveries
                .iter()
                .any(|(u, _)| u.commitment == CommitmentLevel::Confirmed),
            "corrected bank's Confirmed delivery must be present: {:?}",
            deliveries
                .iter()
                .map(|(u, _)| u.commitment)
                .collect::<Vec<_>>()
        );
        for (update, frozen) in &deliveries {
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
        }

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
    fn two_banks_for_the_same_slot_can_disagree_on_their_parent() {
        // Two banks for the same slot number are not required to agree on which slot is
        // their parent -- skipped slots are normal (the leader for N may build on N-2, not
        // N-1, if N-1's leader was offline), and an equivocating leader isn't required to
        // pick the same parent for both of its conflicting versions either. Whichever
        // bank_id is currently resolved as canonical must be the one whose parent is used,
        // not a slot-keyed cache shared across bank_ids.
        let mut storage = BlockMachineStorage::new(10);
        let slot = 30;
        let bank_with_close_parent = 3000;
        let bank_with_distant_parent = 3001;

        storage.add(make_created_bank_msg(
            slot,
            Some(29),
            bank_with_close_parent,
        ));
        add_musthave_sysvars(&mut storage, slot, bank_with_close_parent);
        storage.add(make_entry_msg(slot, 0, bank_with_close_parent));
        storage.add(make_block_meta_msg(slot, 29, bank_with_close_parent));
        storage.add(make_commitment_msg(
            slot,
            Some(29),
            SlotStatus::Processed,
            bank_with_close_parent,
        ));
        let (update, _) = storage
            .pop_ready_block()
            .expect("first bank's Processed delivery");
        assert_eq!(update.parent_slot, Some(29));

        // A dump-and-repair replay produces a genuinely different bank for the same slot,
        // built on a different, more distant parent (e.g. 29 and 28 turned out to be
        // skipped in this corrected version of history).
        storage.add(make_created_bank_msg(
            slot,
            Some(27),
            bank_with_distant_parent,
        ));
        add_musthave_sysvars(&mut storage, slot, bank_with_distant_parent);
        storage.add(make_entry_msg(slot, 0, bank_with_distant_parent));
        storage.add(make_block_meta_msg(slot, 27, bank_with_distant_parent));
        // Confirmed, not Processed: only Confirmed/Finalized ever supersedes a resolved
        // bank -- see `processed_banks_are_peers_until_one_is_confirmed`.
        storage.add(make_commitment_msg(
            slot,
            Some(27),
            SlotStatus::Confirmed,
            bank_with_distant_parent,
        ));

        let (update, _) = storage
            .pop_ready_block()
            .expect("corrected bank's Confirmed delivery should supersede the first one");
        assert_eq!(update.bank_id, bank_with_distant_parent);
        assert_eq!(
            update.parent_slot,
            Some(27),
            "must report the corrected bank's own parent, not the superseded bank's stale one"
        );
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
        add_musthave_sysvars(&mut storage, 10, ancestor_bank);
        storage.add(make_entry_msg(10, 0, ancestor_bank));
        storage.add(make_block_meta_msg(10, 0, ancestor_bank));
        assert!(
            storage.pop_ready_block().is_none(),
            "no delivery yet -- no commitment update at all has arrived for slot 10"
        );

        // The descendant jumps straight to Finalized, skipping Processed/Confirmed for
        // itself too, to also exercise same-slot gap-filling in the same pass.
        storage.add(make_created_bank_msg(11, Some(10), descendant_bank));
        add_musthave_sysvars(&mut storage, 11, descendant_bank);
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
    fn stale_slot_sweep_bounds_memory_for_orphaned_state() {
        let mut storage = BlockMachineStorage::new(10);

        // Two competing banks for slot 1 that never resolve -- neither ever gets a
        // commitment update, so `try_infer_sole_candidate_winner`'s single-candidate
        // inference can't kick in either (there are two candidates). Without the sweep,
        // this would sit in
        // `banks`/`slot_to_banks` forever.
        storage.add(make_created_bank_msg(1, None, 10));
        storage.add(make_account_msg(1, 10, Pubkey::new_unique(), 1));
        storage.add(make_created_bank_msg(1, None, 11));
        storage.add(make_account_msg(1, 11, Pubkey::new_unique(), 1));
        assert!(storage.banks.contains_key(&10));
        assert!(storage.banks.contains_key(&11));
        assert!(storage.slot_to_banks.contains_key(&1));

        // A discarded loser at slot 2, resolved normally via Confirmed -- without the
        // sweep, `discarded_bank_ids` would keep this entry forever too.
        storage.add(make_created_bank_msg(2, None, 20));
        storage.add(make_created_bank_msg(2, None, 21));
        add_musthave_sysvars(&mut storage, 2, 21);
        storage.add(make_entry_msg(2, 0, 21));
        storage.add(make_block_meta_msg(2, 0, 21));
        storage.add(make_commitment_msg(2, None, SlotStatus::Confirmed, 21));
        assert!(storage.discarded_bank_ids.contains_key(&20));

        // Advance far enough past both slots for the sweep to consider them abandoned and
        // run (a single message at a much later slot suffices: `observe_slot` runs on
        // every `add()` call).
        storage.add(make_lifecycle_msg(1000, SlotStatus::FirstShredReceived));

        assert!(
            !storage.banks.contains_key(&10),
            "orphaned bank 10 should have been swept"
        );
        assert!(
            !storage.banks.contains_key(&11),
            "orphaned bank 11 should have been swept"
        );
        assert!(
            !storage.slot_to_banks.contains_key(&1),
            "slot 1's bookkeeping should have been swept"
        );
        assert!(
            !storage.discarded_bank_ids.contains_key(&20),
            "the long-discarded loser should have aged out of discarded_bank_ids"
        );

        // A slot that's already been successfully resolved and delivered must NOT be
        // touched by the sweep, no matter how old it is -- it's bounded separately by the
        // capacity-based `gc()`, and stays valid replay content regardless of age.
        assert!(
            storage.replayed_slot.contains_key(&2),
            "a successfully resolved slot must survive the staleness sweep"
        );
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
