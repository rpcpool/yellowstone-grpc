//! Which slots claim each index key at one level, and the probes that gather a message's
//! candidate slots into hit words, so a slot that several keys find comes up once.

use {
    super::{
        claimed_key_table::{ClaimRole, ClaimedKeyTable, CLAIM_ROLE_COUNT},
        IndexKey, INDEXED_MEMCMP_OFFSETS,
    },
    crate::plugin::message::{Message, MessageAccountInfo},
    solana_pubkey::Pubkey,
    std::mem::size_of,
};

/// The most slots a slot list holds; the eighth moves a pair's slots to a dense row.
pub const SLOT_LIST_CAPACITY: usize = 7;

/// A dense row left with this many slots moves them back to a slot list.
pub const DENSE_ROW_DEMOTE_COUNT: u32 = 3;

/// The most slots a level can have: a claim ref names a slot in 30 bits.
pub const MAX_SLOT_COUNT: usize = 1 << 30;

/// The per-kind index keys, one kind row each, in their sort order.
pub const KIND_ROW_KEYS: [IndexKey; 5] = [
    IndexKey::Slots,
    IndexKey::Entries,
    IndexKey::UpdateParent,
    IndexKey::BlocksMeta,
    IndexKey::BlockFooter,
];

/// The slots that claim one pair: none, one inline, a slot list or a dense row, in the top two
/// bits, and the slot, list or row number in the other 30.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct ClaimRef(u32);

impl ClaimRef {
    pub const NONE: Self = Self(0);
    const PAYLOAD_MASK: u32 = (1 << 30) - 1;
    const ONE_SLOT: u32 = 1 << 30;
    const SLOT_LIST: u32 = 2 << 30;
    const DENSE_ROW: u32 = 3 << 30;

    pub const fn one_slot(slot_number: u32) -> Self {
        Self(Self::ONE_SLOT | slot_number)
    }

    pub const fn is_none(self) -> bool {
        self.0 == 0
    }

    const fn holding_bits(self) -> u32 {
        self.0 & !Self::PAYLOAD_MASK
    }

    const fn payload_number(self) -> u32 {
        self.0 & Self::PAYLOAD_MASK
    }
}

/// The slots of a pair that 2 to [`SLOT_LIST_CAPACITY`] slots claim, in no order.
#[derive(Debug, Clone, Copy, Default)]
pub struct SlotList {
    listed_slot_count: u32,
    listed_slots: [u32; SLOT_LIST_CAPACITY],
}

impl SlotList {
    pub fn listed_slots(&self) -> &[u32] {
        &self.listed_slots[..self.listed_slot_count as usize]
    }
}

#[inline(always)]
const fn set_slot_bit(slot_words: &mut [u64], slot_number: usize) {
    slot_words[slot_number / 64] |= 1 << (slot_number % 64);
}

/// Clears the slot's bit and tells whether it was set.
const fn clear_slot_bit(slot_words: &mut [u64], slot_number: usize) -> bool {
    let slot_word = &mut slot_words[slot_number / 64];
    let slot_bit = 1 << (slot_number % 64);
    let bit_was_set = *slot_word & slot_bit != 0;
    *slot_word &= !slot_bit;
    bit_was_set
}

/// Calls `visit_slot` with each set bit's slot number, ascending.
fn for_each_set_slot(slot_words: &[u64], mut visit_slot: impl FnMut(u32)) {
    for (word_number, slot_word) in slot_words.iter().enumerate() {
        let mut remaining_bits = *slot_word;
        while remaining_bits != 0 {
            visit_slot((word_number * 64) as u32 + remaining_bits.trailing_zeros());
            remaining_bits &= remaining_bits - 1;
        }
    }
}

/// Copies rows of `words_per_row` words into rows of `widened_words` words, zero-filled.
fn widen_row_words(row_words: &[u64], words_per_row: usize, widened_words: usize) -> Vec<u64> {
    let row_count = row_words.len().checked_div(words_per_row).unwrap_or(0);
    let mut widened_rows = vec![0; row_count * widened_words];
    for (old_row, widened_row) in row_words
        .chunks_exact(words_per_row.max(1))
        .zip(widened_rows.chunks_exact_mut(widened_words))
    {
        widened_row[..words_per_row].copy_from_slice(old_row);
    }
    widened_rows
}

/// The slot lists and dense rows that claim refs point at, with free lists that reuse them. A
/// dense row has one bit per slot, so its slots join a message's candidates in a few word ORs.
#[derive(Default)]
pub struct ClaimingSlots {
    /// The width of every row of slot bits: dense rows, kind rows and the hit words.
    words_per_row: usize,
    slot_lists: Vec<SlotList>,
    free_slot_lists: Vec<u32>,
    dense_rows: Vec<u64>,
    dense_row_slot_counts: Vec<u32>,
    free_dense_rows: Vec<u32>,
}

impl ClaimingSlots {
    pub const fn words_per_row(&self) -> usize {
        self.words_per_row
    }

    fn dense_row(&self, row_number: usize) -> &[u64] {
        let row_start = row_number * self.words_per_row;
        &self.dense_rows[row_start..row_start + self.words_per_row]
    }

    fn dense_row_mut(&mut self, row_number: usize) -> &mut [u64] {
        let row_start = row_number * self.words_per_row;
        &mut self.dense_rows[row_start..row_start + self.words_per_row]
    }

    /// Sets the hit word bit of each slot of `claim_ref`; `true` when it names any slot.
    #[inline(always)]
    pub fn mark_hit_slots(&self, claim_ref: ClaimRef, hit_words: &mut [u64]) -> bool {
        let payload_number = claim_ref.payload_number() as usize;
        match claim_ref.holding_bits() {
            ClaimRef::ONE_SLOT => set_slot_bit(hit_words, payload_number),
            ClaimRef::SLOT_LIST => {
                for listed_slot in self.slot_lists[payload_number].listed_slots() {
                    set_slot_bit(hit_words, *listed_slot as usize);
                }
            }
            ClaimRef::DENSE_ROW => {
                let dense_row = self.dense_row(payload_number);
                for (hit_word, row_word) in hit_words.iter_mut().zip(dense_row) {
                    *hit_word |= *row_word;
                }
            }
            _ => return false,
        }
        true
    }

    pub fn for_each_slot(&self, claim_ref: ClaimRef, mut visit_slot: impl FnMut(u32)) {
        let payload_number = claim_ref.payload_number() as usize;
        match claim_ref.holding_bits() {
            ClaimRef::ONE_SLOT => visit_slot(payload_number as u32),
            ClaimRef::SLOT_LIST => {
                for listed_slot in self.slot_lists[payload_number].listed_slots() {
                    visit_slot(*listed_slot);
                }
            }
            ClaimRef::DENSE_ROW => for_each_set_slot(self.dense_row(payload_number), visit_slot),
            _ => {}
        }
    }

    fn take_slot_list(&mut self) -> u32 {
        self.free_slot_lists.pop().unwrap_or_else(|| {
            self.slot_lists.push(SlotList::default());
            (self.slot_lists.len() - 1) as u32
        })
    }

    /// A cleared dense row, reused or new.
    fn take_dense_row(&mut self) -> u32 {
        self.free_dense_rows.pop().unwrap_or_else(|| {
            let row_count = self.dense_row_slot_counts.len();
            self.dense_rows
                .resize((row_count + 1) * self.words_per_row, 0);
            self.dense_row_slot_counts.push(0);
            row_count as u32
        })
    }

    /// The claim ref with `slot_number` added: inline for the first slot, then a slot list, then a
    /// dense row. The slot must not be in it yet, and must fit in the rows.
    pub fn add_slot(&mut self, claim_ref: ClaimRef, slot_number: u32) -> ClaimRef {
        let payload_number = claim_ref.payload_number();
        match claim_ref.holding_bits() {
            ClaimRef::ONE_SLOT => {
                debug_assert_ne!(payload_number, slot_number);
                let list_number = self.take_slot_list();
                let slot_list = &mut self.slot_lists[list_number as usize];
                slot_list.listed_slots[..2].copy_from_slice(&[payload_number, slot_number]);
                slot_list.listed_slot_count = 2;
                ClaimRef(ClaimRef::SLOT_LIST | list_number)
            }
            ClaimRef::SLOT_LIST => {
                let slot_list = self.slot_lists[payload_number as usize];
                debug_assert!(!slot_list.listed_slots().contains(&slot_number));
                if slot_list.listed_slots().len() < SLOT_LIST_CAPACITY {
                    let growing_list = &mut self.slot_lists[payload_number as usize];
                    growing_list.listed_slots[growing_list.listed_slot_count as usize] =
                        slot_number;
                    growing_list.listed_slot_count += 1;
                    return claim_ref;
                }
                let row_number = self.take_dense_row();
                let dense_row = self.dense_row_mut(row_number as usize);
                for listed_slot in slot_list.listed_slots() {
                    set_slot_bit(dense_row, *listed_slot as usize);
                }
                set_slot_bit(dense_row, slot_number as usize);
                self.dense_row_slot_counts[row_number as usize] = SLOT_LIST_CAPACITY as u32 + 1;
                self.free_slot_lists.push(payload_number);
                ClaimRef(ClaimRef::DENSE_ROW | row_number)
            }
            ClaimRef::DENSE_ROW => {
                let dense_row = self.dense_row_mut(payload_number as usize);
                let slot_word = dense_row[slot_number as usize / 64];
                debug_assert_eq!((slot_word >> (slot_number % 64)) & 1, 0);
                set_slot_bit(dense_row, slot_number as usize);
                self.dense_row_slot_counts[payload_number as usize] += 1;
                claim_ref
            }
            _ => ClaimRef::one_slot(slot_number),
        }
    }

    /// The claim ref without `slot_number`, or [`None`] when it does not hold the slot. A list left
    /// with one slot goes inline; a dense row left with three goes back to a list.
    pub fn remove_slot(&mut self, claim_ref: ClaimRef, slot_number: u32) -> Option<ClaimRef> {
        let payload_number = claim_ref.payload_number();
        match claim_ref.holding_bits() {
            ClaimRef::ONE_SLOT => (payload_number == slot_number).then_some(ClaimRef::NONE),
            ClaimRef::SLOT_LIST => {
                let slot_list = &mut self.slot_lists[payload_number as usize];
                let listed_position = slot_list
                    .listed_slots()
                    .iter()
                    .position(|listed_slot| *listed_slot == slot_number)?;
                let last_position = slot_list.listed_slot_count as usize - 1;
                slot_list.listed_slots[listed_position] = slot_list.listed_slots[last_position];
                slot_list.listed_slot_count -= 1;
                if slot_list.listed_slot_count > 1 {
                    return Some(claim_ref);
                }
                let remaining_slot = slot_list.listed_slots[0];
                self.free_slot_lists.push(payload_number);
                Some(ClaimRef::one_slot(remaining_slot))
            }
            ClaimRef::DENSE_ROW => {
                let row_number = payload_number as usize;
                if !clear_slot_bit(self.dense_row_mut(row_number), slot_number as usize) {
                    return None;
                }
                self.dense_row_slot_counts[row_number] -= 1;
                if self.dense_row_slot_counts[row_number] > DENSE_ROW_DEMOTE_COUNT {
                    return Some(claim_ref);
                }
                let list_number = self.take_slot_list();
                let mut demoted_list = SlotList::default();
                for_each_set_slot(self.dense_row(row_number), |row_slot| {
                    demoted_list.listed_slots[demoted_list.listed_slot_count as usize] = row_slot;
                    demoted_list.listed_slot_count += 1;
                });
                self.dense_row_mut(row_number).fill(0);
                self.dense_row_slot_counts[row_number] = 0;
                self.free_dense_rows.push(payload_number);
                self.slot_lists[list_number as usize] = demoted_list;
                Some(ClaimRef(ClaimRef::SLOT_LIST | list_number))
            }
            _ => None,
        }
    }

    fn widen_rows(&mut self, widened_words: usize) {
        self.dense_rows = widen_row_words(&self.dense_rows, self.words_per_row, widened_words);
        self.words_per_row = widened_words;
    }

    /// The slot lists' and dense rows' bytes, by capacity.
    pub const fn held_bytes(&self) -> usize {
        self.slot_lists.capacity() * size_of::<SlotList>()
            + self.dense_rows.capacity() * size_of::<u64>()
            + (self.dense_row_slot_counts.capacity()
                + self.free_slot_lists.capacity()
                + self.free_dense_rows.capacity())
                * size_of::<u32>()
    }
}

/// Where the claims of an index key are held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimPlacement {
    Keyed([u8; 32], ClaimRole),
    /// The memcmp gate count of the owner's bucket.
    MemcmpGate([u8; 32]),
    UngatedMemcmp,
    /// A kind row, by its position in [`KIND_ROW_KEYS`].
    KindRow(usize),
    /// Nowhere, since no message carries the key: a memcmp at an offset that is not indexed.
    Unprobed,
}

impl ClaimPlacement {
    pub const fn of_index_key(index_key: &IndexKey) -> Self {
        let [first_memcmp_offset, second_memcmp_offset] = INDEXED_MEMCMP_OFFSETS;
        match index_key {
            IndexKey::Account(account_pubkey) => {
                Self::Keyed(account_pubkey.to_bytes(), ClaimRole::Account)
            }
            IndexKey::Owner(owner_pubkey) => Self::Keyed(owner_pubkey.to_bytes(), ClaimRole::Owner),
            IndexKey::Memcmp(memcmp_offset, memcmp_bytes)
                if *memcmp_offset == first_memcmp_offset =>
            {
                Self::Keyed(*memcmp_bytes, ClaimRole::MemcmpAt0)
            }
            IndexKey::Memcmp(memcmp_offset, memcmp_bytes)
                if *memcmp_offset == second_memcmp_offset =>
            {
                Self::Keyed(*memcmp_bytes, ClaimRole::MemcmpAt32)
            }
            IndexKey::Memcmp(..) => Self::Unprobed,
            IndexKey::MemcmpGate(owner_pubkey) => Self::MemcmpGate(owner_pubkey.to_bytes()),
            IndexKey::UngatedMemcmp => Self::UngatedMemcmp,
            IndexKey::Transaction(account_key) => {
                Self::Keyed(account_key.to_bytes(), ClaimRole::Transaction)
            }
            IndexKey::Slots => Self::KindRow(0),
            IndexKey::Entries => Self::KindRow(1),
            IndexKey::UpdateParent => Self::KindRow(2),
            IndexKey::BlocksMeta => Self::KindRow(3),
            IndexKey::BlockFooter => Self::KindRow(4),
        }
    }
}

/// The kind row a message finds, as [`ClaimPlacement::of_index_key`] places its kind's key.
#[inline(always)]
const fn kind_row_of(delivered_message: &Message) -> Option<usize> {
    match delivered_message {
        Message::Slot(_) => Some(0),
        Message::Entry(_) => Some(1),
        Message::EntryUpdateParent(_) => Some(2),
        Message::BlockMeta(_) => Some(3),
        Message::BlockFooter(_) => Some(4),
        _ => None,
    }
}

/// One level's claims by slot: keyed pairs in the claimed key table, per-kind keys in rows of slot
/// bits, and counts that skip each probe no claim can answer.
#[derive(Default)]
pub struct KeyClaimIndex {
    claimed_key_table: ClaimedKeyTable,
    claiming_slots: ClaimingSlots,
    /// One row of slot bits per [`KIND_ROW_KEYS`] key.
    kind_rows: Vec<u64>,
    role_claim_counts: [usize; CLAIM_ROLE_COUNT],
    kind_claim_counts: [usize; KIND_ROW_KEYS.len()],
    memcmp_gate_count: usize,
    /// While above 0, every account update probes its memcmp keys.
    ungated_memcmp_count: usize,
    /// The candidate slots of the message being delivered; all zero between messages.
    hit_words: Vec<u64>,
}

impl KeyClaimIndex {
    /// Widens the rows of slot bits to the words `slot_number` needs, on the control path.
    pub fn fit_slot(&mut self, slot_number: u32) {
        let needed_words = slot_number as usize / 64 + 1;
        let words_per_row = self.claiming_slots.words_per_row();
        if needed_words <= words_per_row {
            return;
        }
        self.kind_rows = widen_row_words(&self.kind_rows, words_per_row, needed_words);
        if self.kind_rows.is_empty() {
            self.kind_rows = vec![0; KIND_ROW_KEYS.len() * needed_words];
        }
        self.claiming_slots.widen_rows(needed_words);
        self.hit_words.resize(needed_words, 0);
    }

    /// Adds the slot's claim of a key it does not claim yet; `true` when a table shard grew.
    pub fn claim_index_key(&mut self, index_key: &IndexKey, claiming_slot: u32) -> bool {
        match ClaimPlacement::of_index_key(index_key) {
            ClaimPlacement::Keyed(table_key, claim_role) => {
                let (bucket_place, table_grew) = self
                    .claimed_key_table
                    .find_or_insert(&table_key, claim_role);
                let claimed_bucket = self.claimed_key_table.bucket_at_mut(bucket_place);
                claimed_bucket.claim_ref = self
                    .claiming_slots
                    .add_slot(claimed_bucket.claim_ref, claiming_slot);
                self.role_claim_counts[claim_role as usize] += 1;
                table_grew
            }
            ClaimPlacement::MemcmpGate(owner_key) => {
                let (bucket_place, table_grew) = self
                    .claimed_key_table
                    .find_or_insert(&owner_key, ClaimRole::Owner);
                self.claimed_key_table
                    .bucket_at_mut(bucket_place)
                    .memcmp_gate_count += 1;
                self.memcmp_gate_count += 1;
                table_grew
            }
            ClaimPlacement::UngatedMemcmp => {
                self.ungated_memcmp_count += 1;
                false
            }
            ClaimPlacement::KindRow(kind_row_number) => {
                set_slot_bit(self.kind_row_mut(kind_row_number), claiming_slot as usize);
                self.kind_claim_counts[kind_row_number] += 1;
                false
            }
            ClaimPlacement::Unprobed => false,
        }
    }

    /// Removes the slot's claim of the key, and the key's bucket once nothing claims it. A debug
    /// build panics when the slot holds no such claim.
    pub fn release_index_key(&mut self, index_key: &IndexKey, released_slot: u32) {
        let claim_released = match ClaimPlacement::of_index_key(index_key) {
            ClaimPlacement::Keyed(table_key, claim_role) => {
                let bucket_place = self.claimed_key_table.locate_bucket(&table_key, claim_role);
                let kept_ref = bucket_place.and_then(|bucket_place| {
                    let released_bucket = self.claimed_key_table.bucket_at_mut(bucket_place);
                    let kept_ref = self
                        .claiming_slots
                        .remove_slot(released_bucket.claim_ref, released_slot)?;
                    released_bucket.claim_ref = kept_ref;
                    self.role_claim_counts[claim_role as usize] -= 1;
                    self.claimed_key_table.remove_if_unclaimed(bucket_place);
                    Some(kept_ref)
                });
                kept_ref.is_some()
            }
            ClaimPlacement::MemcmpGate(owner_key) => {
                let bucket_place = self
                    .claimed_key_table
                    .locate_bucket(&owner_key, ClaimRole::Owner);
                let gated_place = bucket_place.filter(|gated_place| {
                    self.claimed_key_table
                        .bucket_at(*gated_place)
                        .memcmp_gate_count
                        > 0
                });
                if let Some(gated_place) = gated_place {
                    self.claimed_key_table
                        .bucket_at_mut(gated_place)
                        .memcmp_gate_count -= 1;
                    self.memcmp_gate_count -= 1;
                    self.claimed_key_table.remove_if_unclaimed(gated_place);
                }
                gated_place.is_some()
            }
            ClaimPlacement::UngatedMemcmp => {
                let ungated_claim_held = self.ungated_memcmp_count > 0;
                self.ungated_memcmp_count -= usize::from(ungated_claim_held);
                ungated_claim_held
            }
            ClaimPlacement::KindRow(kind_row_number) => {
                let kind_row = self.kind_row_mut(kind_row_number);
                let bit_was_set = clear_slot_bit(kind_row, released_slot as usize);
                self.kind_claim_counts[kind_row_number] -= usize::from(bit_was_set);
                bit_was_set
            }
            ClaimPlacement::Unprobed => true,
        };
        debug_assert!(
            claim_released,
            "no claim of {index_key:?} for slot {released_slot}"
        );
    }

    fn kind_row_mut(&mut self, kind_row_number: usize) -> &mut [u64] {
        let words_per_row = self.claiming_slots.words_per_row();
        let row_start = kind_row_number * words_per_row;
        &mut self.kind_rows[row_start..row_start + words_per_row]
    }

    /// Whether any keyed or per-kind claim can make a slot a candidate.
    pub fn holds_slot_claims(&self) -> bool {
        self.role_claim_counts
            .iter()
            .any(|role_claims| *role_claims > 0)
            || self
                .kind_claim_counts
                .iter()
                .any(|kind_claims| *kind_claims > 0)
    }

    /// Sets the hit word bits of the slots that claim the keys the message carries, as
    /// [`super::for_each_index_key`] lists them; `true` when a probe found a claim.
    #[inline(always)]
    pub fn gather_hit_slots(&mut self, delivered_message: &Message) -> bool {
        match delivered_message {
            Message::Account(message_account) => self.gather_account_hits(&message_account.account),
            Message::Transaction(message_transaction) => {
                if self.role_claim_counts[ClaimRole::Transaction as usize] == 0 {
                    return false;
                }
                let mut found_claims = false;
                for account_key in &message_transaction.transaction.account_keys {
                    found_claims |= self.mark_pair(account_key.as_array(), ClaimRole::Transaction);
                }
                found_claims
            }
            kind_message => match kind_row_of(kind_message) {
                Some(kind_row_number) if self.kind_claim_counts[kind_row_number] > 0 => {
                    let words_per_row = self.hit_words.len();
                    let row_start = kind_row_number * words_per_row;
                    let kind_row = &self.kind_rows[row_start..row_start + words_per_row];
                    self.hit_words.copy_from_slice(kind_row);
                    true
                }
                _ => false,
            },
        }
    }

    /// An account probes its pubkey and owner, and its memcmp keys only when the owner's bucket
    /// holds a memcmp gate or a memcmp claim is ungated.
    #[inline(always)]
    fn gather_account_hits(&mut self, account_info: &MessageAccountInfo) -> bool {
        let mut found_claims = false;
        if self.role_claim_counts[ClaimRole::Account as usize] > 0 {
            found_claims |= self.mark_pair(account_info.pubkey.as_array(), ClaimRole::Account);
        }
        let mut memcmp_probes_open = self.ungated_memcmp_count > 0;
        if self.role_claim_counts[ClaimRole::Owner as usize] > 0 || self.memcmp_gate_count > 0 {
            let owner_key = account_info.owner.as_array();
            let owner_bucket = self
                .claimed_key_table
                .find_bucket(owner_key, ClaimRole::Owner);
            if let Some(owner_bucket) = owner_bucket {
                memcmp_probes_open |= owner_bucket.memcmp_gate_count > 0;
                found_claims |= self
                    .claiming_slots
                    .mark_hit_slots(owner_bucket.claim_ref, &mut self.hit_words);
            }
        }
        if memcmp_probes_open {
            let memcmp_roles = [ClaimRole::MemcmpAt0, ClaimRole::MemcmpAt32];
            for (memcmp_offset, memcmp_role) in INDEXED_MEMCMP_OFFSETS.into_iter().zip(memcmp_roles)
            {
                if self.role_claim_counts[memcmp_role as usize] == 0 {
                    continue;
                }
                let data_at_offset = account_info.data.get(memcmp_offset..);
                if let Some(memcmp_bytes) = data_at_offset.and_then(<[u8]>::first_chunk) {
                    found_claims |= self.mark_pair(memcmp_bytes, memcmp_role);
                }
            }
        }
        found_claims
    }

    #[inline(always)]
    fn mark_pair(&mut self, table_key: &[u8; 32], claim_role: ClaimRole) -> bool {
        match self.claimed_key_table.find_bucket(table_key, claim_role) {
            Some(found_bucket) => self
                .claiming_slots
                .mark_hit_slots(found_bucket.claim_ref, &mut self.hit_words),
            None => false,
        }
    }

    /// One hit word per 64 slots.
    #[inline(always)]
    pub const fn hit_word_count(&self) -> usize {
        self.hit_words.len()
    }

    /// Takes the hit word, leaving zero; its bit `b` stands for slot `64 * hit_word_number + b`.
    #[inline(always)]
    pub fn take_hit_word(&mut self, hit_word_number: usize) -> u64 {
        std::mem::take(&mut self.hit_words[hit_word_number])
    }

    /// The slots that claim the key, ascending; a memcmp gate or ungated memcmp claim counts as
    /// slot [`u32::MAX`].
    pub fn slots_claiming(&self, index_key: &IndexKey) -> Vec<u32> {
        let mut claiming_slot_numbers = Vec::new();
        match ClaimPlacement::of_index_key(index_key) {
            ClaimPlacement::Keyed(table_key, claim_role) => {
                let found_bucket = self.claimed_key_table.find_bucket(&table_key, claim_role);
                if let Some(found_bucket) = found_bucket {
                    self.claiming_slots
                        .for_each_slot(found_bucket.claim_ref, |claiming_slot| {
                            claiming_slot_numbers.push(claiming_slot)
                        });
                }
            }
            ClaimPlacement::MemcmpGate(owner_key) => {
                let gated_bucket = self
                    .claimed_key_table
                    .find_bucket(&owner_key, ClaimRole::Owner);
                let gate_count =
                    gated_bucket.map_or(0, |gated_bucket| gated_bucket.memcmp_gate_count);
                claiming_slot_numbers.resize(gate_count as usize, u32::MAX);
            }
            ClaimPlacement::UngatedMemcmp => {
                claiming_slot_numbers.resize(self.ungated_memcmp_count, u32::MAX);
            }
            ClaimPlacement::KindRow(kind_row_number) => {
                let words_per_row = self.claiming_slots.words_per_row();
                let row_start = kind_row_number * words_per_row;
                let kind_row = &self.kind_rows[row_start..row_start + words_per_row];
                for_each_set_slot(kind_row, |claiming_slot| {
                    claiming_slot_numbers.push(claiming_slot)
                });
            }
            ClaimPlacement::Unprobed => {}
        }
        claiming_slot_numbers.sort_unstable();
        claiming_slot_numbers
    }

    /// Every index key with a claim, sorted.
    pub fn held_index_keys(&self) -> Vec<IndexKey> {
        let [first_memcmp_offset, second_memcmp_offset] = INDEXED_MEMCMP_OFFSETS;
        let mut held_index_keys = Vec::new();
        self.claimed_key_table.for_each_held_bucket(|held_bucket| {
            let held_pubkey = Pubkey::new_from_array(held_bucket.table_key);
            if !held_bucket.claim_ref.is_none() {
                held_index_keys.push(match held_bucket.bucket_role() {
                    ClaimRole::Account => IndexKey::Account(held_pubkey),
                    ClaimRole::Owner => IndexKey::Owner(held_pubkey),
                    ClaimRole::Transaction => IndexKey::Transaction(held_pubkey),
                    ClaimRole::MemcmpAt0 => {
                        IndexKey::Memcmp(first_memcmp_offset, held_bucket.table_key)
                    }
                    ClaimRole::MemcmpAt32 => {
                        IndexKey::Memcmp(second_memcmp_offset, held_bucket.table_key)
                    }
                });
            }
            if held_bucket.memcmp_gate_count > 0 {
                held_index_keys.push(IndexKey::MemcmpGate(held_pubkey));
            }
        });
        if self.ungated_memcmp_count > 0 {
            held_index_keys.push(IndexKey::UngatedMemcmp);
        }
        for (kind_row_number, kind_index_key) in KIND_ROW_KEYS.into_iter().enumerate() {
            if self.kind_claim_counts[kind_row_number] > 0 {
                held_index_keys.push(kind_index_key);
            }
        }
        held_index_keys.sort_unstable();
        held_index_keys
    }

    pub const fn claimed_key_table(&self) -> &ClaimedKeyTable {
        &self.claimed_key_table
    }

    pub const fn claimed_key_table_mut(&mut self) -> &mut ClaimedKeyTable {
        &mut self.claimed_key_table
    }

    /// The table's, slot lists', dense rows', kind rows' and hit words' bytes, by capacity.
    pub fn held_bytes(&self) -> usize {
        self.claimed_key_table.held_bytes()
            + self.claiming_slots.held_bytes()
            + (self.kind_rows.capacity() + self.hit_words.capacity()) * size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{super::claimed_key_table::ForcedPairHash, *},
        crate::{
            plugin::{filter::fixtures, message::SlotStatus},
            reverse_index::for_each_index_key,
        },
        foldhash::HashMap,
        solana_signature::Signature,
        std::sync::Arc,
    };

    fn numbered_pubkey(key_number: u64) -> Pubkey {
        let mut pubkey_bytes = [0; 32];
        pubkey_bytes[..8].copy_from_slice(&key_number.to_le_bytes());
        Pubkey::new_from_array(pubkey_bytes)
    }

    /// Every key kind over the pubkeys, one 32-byte value under every role, then the ungated
    /// memcmp key and the per-kind keys.
    fn index_key_pool(pool_pubkeys: &[Pubkey]) -> Vec<IndexKey> {
        let [first_memcmp_offset, second_memcmp_offset] = INDEXED_MEMCMP_OFFSETS;
        let keyed_index_keys = pool_pubkeys.iter().flat_map(|pool_pubkey| {
            [
                IndexKey::Account(*pool_pubkey),
                IndexKey::Owner(*pool_pubkey),
                IndexKey::Memcmp(first_memcmp_offset, pool_pubkey.to_bytes()),
                IndexKey::Memcmp(second_memcmp_offset, pool_pubkey.to_bytes()),
                IndexKey::MemcmpGate(*pool_pubkey),
                IndexKey::Transaction(*pool_pubkey),
            ]
        });
        let unkeyed_index_keys = [IndexKey::UngatedMemcmp].into_iter().chain(KIND_ROW_KEYS);
        keyed_index_keys.chain(unkeyed_index_keys).collect()
    }

    /// A random message whose keys come from the pubkeys: an account with memcmp bytes at either
    /// offset, a transaction, or a message of a per-kind key.
    fn random_message(seeded_rng: &mut fastrand::Rng, pool_pubkeys: &[Pubkey]) -> Message {
        let mut pick_pubkey = || *seeded_rng.choice(pool_pubkeys).unwrap();
        let (account_pubkey, owner_pubkey) = (pick_pubkey(), pick_pubkey());
        let data_halves = [pick_pubkey().to_bytes(), pick_pubkey().to_bytes()];
        let account_keys = vec![pick_pubkey(), pick_pubkey(), pick_pubkey()];
        match seeded_rng.u8(..8) {
            0..=3 => {
                let mut account_data = data_halves.concat();
                account_data.truncate([0, 40, 64][seeded_rng.usize(..3)]);
                let account_info =
                    fixtures::account_info(account_pubkey, owner_pubkey, account_data, 1, None);
                Message::Account(fixtures::message_account(account_info))
            }
            4 | 5 => Message::Transaction(fixtures::message_transaction(
                Signature::default(),
                account_keys,
                false,
                Default::default(),
            )),
            6 => Message::Slot(Arc::new(fixtures::message_slot(1, SlotStatus::Processed))),
            _ => Message::Entry(fixtures::message_entry(1, 0)),
        }
    }

    /// A [`KeyClaimIndex`] beside a model of the slots that claim each key.
    #[derive(Default)]
    struct ModelledKeyClaims {
        key_claim_index: KeyClaimIndex,
        modelled_slots: HashMap<IndexKey, Vec<u32>>,
        held_claims: Vec<(IndexKey, u32)>,
    }

    impl ModelledKeyClaims {
        /// Claims the key for the slot in both, unless the slot claims it already.
        fn claim_index_key(&mut self, index_key: IndexKey, claiming_slot: u32) {
            if self.held_claims.contains(&(index_key, claiming_slot)) {
                return;
            }
            self.key_claim_index
                .claim_index_key(&index_key, claiming_slot);
            let modelled_slots = self.modelled_slots.entry(index_key).or_default();
            modelled_slots.push(claiming_slot);
            self.held_claims.push((index_key, claiming_slot));
            self.assert_key_matches_model(&index_key);
        }

        fn release_held_claim(&mut self, held_claim_position: usize) {
            let (index_key, claiming_slot) = self.held_claims.swap_remove(held_claim_position);
            self.key_claim_index
                .release_index_key(&index_key, claiming_slot);
            let modelled_slots = self.modelled_slots.get_mut(&index_key).unwrap();
            let claim_position = modelled_slots
                .iter()
                .position(|modelled_slot| *modelled_slot == claiming_slot)
                .unwrap();
            modelled_slots.swap_remove(claim_position);
            if modelled_slots.is_empty() {
                self.modelled_slots.remove(&index_key);
            }
            self.assert_key_matches_model(&index_key);
        }

        /// A gate or ungated memcmp claim shows as slot [`u32::MAX`], as in `slots_claiming`.
        fn assert_key_matches_model(&self, index_key: &IndexKey) {
            let mut expected_slots = self.modelled_slots.get(index_key).cloned();
            let expected_slots = expected_slots.get_or_insert_default();
            if matches!(index_key, IndexKey::MemcmpGate(_) | IndexKey::UngatedMemcmp) {
                expected_slots.fill(u32::MAX);
            }
            expected_slots.sort_unstable();
            let claiming_slots = self.key_claim_index.slots_claiming(index_key);
            assert_eq!(claiming_slots, *expected_slots, "{index_key:?}");
        }

        /// Checks each of `index_keys`, every claimed key among them, and that no other key is held.
        fn assert_matches_model(&self, index_keys: &[IndexKey]) {
            for index_key in index_keys {
                self.assert_key_matches_model(index_key);
            }
            let mut modelled_keys = self.modelled_slots.keys().copied().collect::<Vec<_>>();
            modelled_keys.sort_unstable();
            assert_eq!(self.key_claim_index.held_index_keys(), modelled_keys);
        }

        /// The model's candidates: the claims of the keys the message carries, a memcmp key's only
        /// when the owner has a gate claim or an ungated memcmp claim is held.
        fn modelled_hit_slots(&self, delivered_message: &Message) -> Vec<u32> {
            let memcmp_probes_open = match delivered_message {
                Message::Account(message_account) => {
                    let owner_gate = IndexKey::MemcmpGate(message_account.account.owner);
                    self.modelled_slots.contains_key(&owner_gate)
                        || self.modelled_slots.contains_key(&IndexKey::UngatedMemcmp)
                }
                _ => false,
            };
            let mut hit_slots = Vec::new();
            for_each_index_key(delivered_message, |carried_key| {
                let gated_shut = matches!(carried_key, IndexKey::Memcmp(..)) && !memcmp_probes_open;
                if let Some(claiming_slots) = self.modelled_slots.get(&carried_key) {
                    if !gated_shut {
                        hit_slots.extend_from_slice(claiming_slots);
                    }
                }
            });
            hit_slots.sort_unstable();
            hit_slots.dedup();
            hit_slots
        }

        /// The slots the index's probes find, taking every hit word so they end zero.
        fn gathered_hit_slots(&mut self, delivered_message: &Message) -> Vec<u32> {
            let mut hit_slots = Vec::new();
            if self.key_claim_index.gather_hit_slots(delivered_message) {
                for hit_word_number in 0..self.key_claim_index.hit_word_count() {
                    let hit_word = self.key_claim_index.take_hit_word(hit_word_number);
                    let hit_word_slots = std::slice::from_ref(&hit_word);
                    for_each_set_slot(hit_word_slots, |word_slot| {
                        hit_slots.push(hit_word_number as u32 * 64 + word_slot)
                    });
                }
            }
            hit_slots
        }
    }

    /// Seeded claims, releases and messages, also under a pair hash where every pair collides: the
    /// claims match a model, and each message gathers the model's candidates, memcmp gates included.
    #[test]
    fn claims_and_gathered_slots_match_a_model() {
        let pool_pubkeys = (0..24).map(numbered_pubkey).collect::<Vec<_>>();
        let pooled_index_keys = index_key_pool(&pool_pubkeys);
        let colliding_pair_hash = |table_key: &[u8; 32], claim_role: ClaimRole| {
            let spread_bits = u64::from(table_key[0] ^ claim_role as u8);
            ((1_u64 << 40) - 1 - (spread_bits & 3)) | (((spread_bits >> 2) & 1) << 57)
        };
        let forced_hashes: [Option<ForcedPairHash>; 2] = [None, Some(colliding_pair_hash)];
        let (mut gathered_message_count, mut dense_key_count) = (0, 0);
        for forced_pair_hash in forced_hashes {
            for rng_seed in 0..24 {
                let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
                let mut modelled_claims = ModelledKeyClaims::default();
                let claimed_key_table = modelled_claims.key_claim_index.claimed_key_table_mut();
                claimed_key_table.forced_pair_hash = forced_pair_hash;
                let slot_count = [8, 100][rng_seed as usize % 2];
                modelled_claims.key_claim_index.fit_slot(slot_count - 1);
                // A few hot keys get many slots, so their claims move to dense rows.
                let hot_index_keys = &pooled_index_keys[..12];
                for step_number in 0..3000 {
                    if step_number % 10 == 0 {
                        let delivered_message = random_message(&mut seeded_rng, &pool_pubkeys);
                        let gathered_slots = modelled_claims.gathered_hit_slots(&delivered_message);
                        let modelled_slots = modelled_claims.modelled_hit_slots(&delivered_message);
                        assert_eq!(gathered_slots, modelled_slots, "{delivered_message:?}");
                        gathered_message_count += usize::from(!gathered_slots.is_empty());
                    }
                    let held_claim_count = modelled_claims.held_claims.len();
                    if held_claim_count > 0 && seeded_rng.u8(..3) == 0 {
                        modelled_claims.release_held_claim(seeded_rng.usize(..held_claim_count));
                        continue;
                    }
                    let claim_pool = [hot_index_keys, &pooled_index_keys][seeded_rng.usize(..2)];
                    let index_key = *seeded_rng.choice(claim_pool).unwrap();
                    let claiming_slot = seeded_rng.u32(..slot_count);
                    modelled_claims.claim_index_key(index_key, claiming_slot);
                }
                modelled_claims.assert_matches_model(&pooled_index_keys);
                dense_key_count += modelled_claims
                    .modelled_slots
                    .values()
                    .filter(|claiming_slots| claiming_slots.len() > SLOT_LIST_CAPACITY)
                    .count();
                while !modelled_claims.held_claims.is_empty() {
                    let held_claim_count = modelled_claims.held_claims.len();
                    modelled_claims.release_held_claim(seeded_rng.usize(..held_claim_count));
                }
                modelled_claims.assert_matches_model(&pooled_index_keys);
                assert!(!modelled_claims.key_claim_index.holds_slot_claims());
                let claimed_key_table = modelled_claims.key_claim_index.claimed_key_table();
                assert_eq!(claimed_key_table.held_bucket_count(), 0);
            }
        }
        assert!(gathered_message_count > 3000, "{gathered_message_count}");
        assert!(dense_key_count >= 24, "{dense_key_count}");
    }

    /// Seeded adds and removes keep each claim ref holding the model's slots, through the moves
    /// between inline slots, lists and dense rows, and through a widening of the rows.
    #[test]
    fn claim_refs_hold_their_slots_through_lists_dense_rows_and_widening() {
        for rng_seed in 0..32 {
            let mut seeded_rng = fastrand::Rng::with_seed(rng_seed);
            let mut claiming_slots = ClaimingSlots::default();
            claiming_slots.widen_rows(1);
            let mut claim_refs = [ClaimRef::NONE; 16];
            let mut modelled_slots: HashMap<usize, Vec<u32>> = HashMap::default();
            let mut slot_limit = 64;
            for step_number in 0..4000 {
                if step_number == 2000 {
                    claiming_slots.widen_rows(4);
                    slot_limit = 256;
                }
                let ref_number = seeded_rng.usize(..claim_refs.len());
                let held_slots = modelled_slots.entry(ref_number).or_default();
                // The first refs see few slots, the last ones many.
                let slot_range = 2 + ref_number as u32 * (slot_limit / 16);
                let picked_slot = seeded_rng.u32(..slot_range);
                let held_position = held_slots
                    .iter()
                    .position(|held_slot| *held_slot == picked_slot);
                if let Some(held_position) = held_position {
                    held_slots.swap_remove(held_position);
                    claim_refs[ref_number] = claiming_slots
                        .remove_slot(claim_refs[ref_number], picked_slot)
                        .unwrap();
                } else {
                    held_slots.push(picked_slot);
                    claim_refs[ref_number] =
                        claiming_slots.add_slot(claim_refs[ref_number], picked_slot);
                }
                let mut found_slots = Vec::new();
                claiming_slots.for_each_slot(claim_refs[ref_number], |found_slot| {
                    found_slots.push(found_slot)
                });
                found_slots.sort_unstable();
                let mut expected_slots = held_slots.clone();
                expected_slots.sort_unstable();
                assert_eq!(
                    found_slots, expected_slots,
                    "seed {rng_seed} step {step_number}"
                );
                let holding_bits = claim_refs[ref_number].holding_bits();
                let expected_holding = match expected_slots.len() {
                    0 => 0,
                    1 => ClaimRef::ONE_SLOT,
                    2..=SLOT_LIST_CAPACITY => ClaimRef::SLOT_LIST,
                    _ => ClaimRef::DENSE_ROW,
                };
                // A dense row keeps more than three slots until it moves back to a list.
                let dense_above_demotion = holding_bits == ClaimRef::DENSE_ROW
                    && expected_slots.len() > DENSE_ROW_DEMOTE_COUNT as usize;
                assert!(holding_bits == expected_holding || dense_above_demotion);
                // No ref ever holds the last slot of the rows.
                let unheld_removal =
                    claiming_slots.remove_slot(claim_refs[ref_number], slot_limit - 1);
                assert!(unheld_removal.is_none());
            }
            let dense_ref_count = claim_refs
                .iter()
                .filter(|claim_ref| claim_ref.holding_bits() == ClaimRef::DENSE_ROW)
                .count();
            assert!(dense_ref_count > 0, "seed {rng_seed}");
            let mut hit_words = vec![0; claiming_slots.words_per_row()];
            for (ref_number, claim_ref) in claim_refs.iter().enumerate() {
                hit_words.fill(0);
                let any_slot_marked = claiming_slots.mark_hit_slots(*claim_ref, &mut hit_words);
                let mut marked_slots = Vec::new();
                for_each_set_slot(&hit_words, |marked_slot| marked_slots.push(marked_slot));
                let mut expected_slots =
                    modelled_slots.get(&ref_number).cloned().unwrap_or_default();
                expected_slots.sort_unstable();
                assert_eq!(any_slot_marked, !expected_slots.is_empty());
                assert_eq!(marked_slots, expected_slots);
            }
        }
    }

    #[test]
    fn widened_rows_keep_their_slots() {
        assert!(widen_row_words(&[], 0, 4).is_empty());
        let widened_rows = widen_row_words(&[1, 2, 3, 4], 2, 3);
        assert_eq!(widened_rows, [1, 2, 0, 3, 4, 0]);
    }
}
