//! Which slots claim each index key at one level, and the probes that gather a message's
//! candidate slots into hit words, so a slot that several keys find comes up once.

use {
    super::{
        claimed_key_table::{ClaimRole, ClaimedKeyTable, CLAIM_ROLE_COUNT},
        IndexKey, INDEXED_MEMCMP_OFFSETS,
    },
    crate::plugin::message::{Message, MessageAccount},
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
            Message::Account(message_account) => self.gather_account_hits(message_account),
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
    fn gather_account_hits(&mut self, message_account: &MessageAccount) -> bool {
        let mut found_claims = false;
        if self.role_claim_counts[ClaimRole::Account as usize] > 0 {
            found_claims |= self.mark_pair(message_account.pubkey.as_array(), ClaimRole::Account);
        }
        let mut memcmp_probes_open = self.ungated_memcmp_count > 0;
        if self.role_claim_counts[ClaimRole::Owner as usize] > 0 || self.memcmp_gate_count > 0 {
            let owner_key = message_account.owner.as_array();
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
                let data_at_offset = message_account.data().get(memcmp_offset..);
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

    /// The hits in the words not taken yet.
    pub fn untaken_hit_count(&self) -> usize {
        self.hit_words
            .iter()
            .map(|hit_word| hit_word.count_ones() as usize)
            .sum()
    }

    /// Takes the hit word, leaving zero; its bit `b` stands for slot `64 * hit_word_number + b`.
    #[inline(always)]
    pub fn take_hit_word(&mut self, hit_word_number: usize) -> u64 {
        std::mem::take(&mut self.hit_words[hit_word_number])
    }
}
