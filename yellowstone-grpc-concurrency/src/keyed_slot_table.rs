//! [`KeyedSlotTable`]: a fixed table whose slots are each keyed at most once by CAS, from free to
//! reserved by one writer to keyed. Finders match keyed slots only and confirm a tag hit by key.

use {
    crate::sync::{spin_loop, AtomicU64, Ordering, UnsafeCell},
    std::fmt,
};

/// Bounds the re-reads of each slot that another writer has reserved while it builds its key; past
/// the bound a lookup moves on, so two writers may still key one key twice.
const RESERVED_SLOT_REREADS: u32 = 64;

const KEY_STATE_BITS: u32 = 2;
const KEY_STATE_MASK: u64 = (1 << KEY_STATE_BITS) - 1;
const FREE_SLOT: u64 = 0;
const RESERVED_SLOT: u64 = 1;
const KEYED_SLOT: u64 = 2;

/// One slot: its key word (the state in the low bits, the key's tag above them), the key its
/// reserver wrote, and the value the table built it with.
struct KeyedSlot<SlotKey, SlotValue> {
    key_word: AtomicU64,
    slot_key: UnsafeCell<Option<SlotKey>>,
    slot_value: SlotValue,
}

impl<SlotKey, SlotValue> KeyedSlot<SlotKey, SlotValue> {
    /// Whether the slot is keyed with `keyed_word`'s tag and its key passes `matches_key`.
    fn holds_matching_key(
        &self,
        observed_word: u64,
        keyed_word: u64,
        matches_key: &mut impl FnMut(&SlotKey) -> bool,
    ) -> bool {
        // SAFETY: the acquire load of a KEYED word saw the key's release store, and a keyed slot's
        // key is never written again.
        observed_word == keyed_word
            && self
                .slot_key
                .with(|key_pointer| unsafe { (*key_pointer).as_ref().is_some_and(matches_key) })
    }
}

/// A fixed table of slots keyed once each. A key never moves or changes, so a keyed slot stays
/// its key's until the table drops; writers that race on one new key may key two slots.
pub struct KeyedSlotTable<SlotKey, SlotValue> {
    table_slots: Box<[KeyedSlot<SlotKey, SlotValue>]>,
}

// SAFETY: a slot's key is written by the one writer that reserved it, before the release store
// that keys it, and only read after an acquire load of that word.
unsafe impl<SlotKey: Send + Sync, SlotValue: Sync> Sync for KeyedSlotTable<SlotKey, SlotValue> {}

/// A keyed slot that a lookup found or keyed.
pub struct KeyedSlotRef<'table, SlotKey, SlotValue> {
    slot_index: usize,
    table_slot: &'table KeyedSlot<SlotKey, SlotValue>,
}

impl<'table, SlotKey, SlotValue> KeyedSlotRef<'table, SlotKey, SlotValue> {
    pub const fn slot_index(&self) -> usize {
        self.slot_index
    }

    pub const fn slot_value(&self) -> &'table SlotValue {
        &self.table_slot.slot_value
    }

    /// The slot's key, which no writer changes while the table lives.
    pub fn slot_key(&self) -> &'table SlotKey {
        // SAFETY: a slot reference exists only for a slot read as KEYED with acquire, and a keyed
        // slot's key is never written again.
        self.table_slot.slot_key.with(|key_pointer| unsafe {
            (*key_pointer)
                .as_ref()
                .unwrap_or_else(|| unreachable!("a keyed slot holds its key"))
        })
    }

    /// Reads the slot's key.
    pub fn with_slot_key<KeyReadResult>(
        &self,
        read_slot_key: impl FnOnce(&SlotKey) -> KeyReadResult,
    ) -> KeyReadResult {
        // SAFETY: a slot reference exists only for a slot read as KEYED with acquire.
        self.table_slot.slot_key.with(|key_pointer| unsafe {
            read_slot_key(
                (*key_pointer)
                    .as_ref()
                    .unwrap_or_else(|| unreachable!("a keyed slot holds its key")),
            )
        })
    }
}

/// What a lookup found.
pub enum SlotLookup<'table, SlotKey, SlotValue> {
    /// A slot keyed with an equal key.
    Matched(KeyedSlotRef<'table, SlotKey, SlotValue>),
    /// A free slot that this writer keyed. A reserved slot passed on the way may hold a racing
    /// writer's equal key.
    Keyed {
        keyed_slot: KeyedSlotRef<'table, SlotKey, SlotValue>,
        passed_reserved_slot: bool,
    },
    /// No slot matched, and none was free.
    TableFull,
}

impl<SlotKey, SlotValue> KeyedSlotTable<SlotKey, SlotValue> {
    /// A table of `slot_count` free slots, each holding a value from `new_slot_value`.
    pub fn new(slot_count: usize, mut new_slot_value: impl FnMut() -> SlotValue) -> Self {
        Self {
            table_slots: (0..slot_count)
                .map(|_| KeyedSlot {
                    key_word: AtomicU64::new(FREE_SLOT),
                    slot_key: UnsafeCell::new(None),
                    slot_value: new_slot_value(),
                })
                .collect(),
        }
    }

    pub fn slot_count(&self) -> usize {
        self.table_slots.len()
    }

    /// The first keyed slot whose key matches, else the first free slot, keyed with
    /// `new_slot_key()`. Equal keys must give equal `key_tag`s; its low two bits are ignored.
    /// A slot another writer has reserved is re-read a few times for its key before it is passed.
    pub fn find_or_key_slot(
        &self,
        key_tag: u64,
        mut matches_key: impl FnMut(&SlotKey) -> bool,
        new_slot_key: impl FnOnce() -> SlotKey,
    ) -> SlotLookup<'_, SlotKey, SlotValue> {
        let keyed_word = (key_tag & !KEY_STATE_MASK) | KEYED_SLOT;
        let mut passed_reserved_slot = false;
        for (slot_index, table_slot) in self.table_slots.iter().enumerate() {
            let mut observed_word = table_slot.key_word.load(Ordering::Acquire);
            if observed_word == FREE_SLOT {
                match table_slot.key_word.compare_exchange(
                    FREE_SLOT,
                    RESERVED_SLOT,
                    Ordering::Acquire,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        let slot_key = new_slot_key();
                        // SAFETY: RESERVED gives this writer the key until it stores KEYED.
                        table_slot
                            .slot_key
                            .with_mut(|key_pointer| unsafe { *key_pointer = Some(slot_key) });
                        table_slot.key_word.store(keyed_word, Ordering::Release);
                        return SlotLookup::Keyed {
                            keyed_slot: KeyedSlotRef {
                                slot_index,
                                table_slot,
                            },
                            passed_reserved_slot,
                        };
                    }
                    Err(current_word) => observed_word = current_word,
                }
            }
            let mut reserved_rereads = 0;
            while observed_word == RESERVED_SLOT && reserved_rereads < RESERVED_SLOT_REREADS {
                spin_loop();
                observed_word = table_slot.key_word.load(Ordering::Acquire);
                reserved_rereads += 1;
            }
            passed_reserved_slot |= observed_word == RESERVED_SLOT;
            if table_slot.holds_matching_key(observed_word, keyed_word, &mut matches_key) {
                return SlotLookup::Matched(KeyedSlotRef {
                    slot_index,
                    table_slot,
                });
            }
        }
        SlotLookup::TableFull
    }

    /// The first keyed slot whose key matches.
    pub fn find_keyed_slot(
        &self,
        key_tag: u64,
        mut matches_key: impl FnMut(&SlotKey) -> bool,
    ) -> Option<KeyedSlotRef<'_, SlotKey, SlotValue>> {
        let keyed_word = (key_tag & !KEY_STATE_MASK) | KEYED_SLOT;
        self.table_slots
            .iter()
            .enumerate()
            .find(|(_, table_slot)| {
                let observed_word = table_slot.key_word.load(Ordering::Acquire);
                table_slot.holds_matching_key(observed_word, keyed_word, &mut matches_key)
            })
            .map(|(slot_index, table_slot)| KeyedSlotRef {
                slot_index,
                table_slot,
            })
    }

    /// The slot at `slot_index`, if it is keyed.
    pub fn keyed_slot(&self, slot_index: usize) -> Option<KeyedSlotRef<'_, SlotKey, SlotValue>> {
        let table_slot = self.table_slots.get(slot_index)?;
        let observed_word = table_slot.key_word.load(Ordering::Acquire);
        (observed_word & KEY_STATE_MASK == KEYED_SLOT).then_some(KeyedSlotRef {
            slot_index,
            table_slot,
        })
    }

    /// The value of the slot at `slot_index`, keyed or not.
    pub fn slot_value(&self, slot_index: usize) -> Option<&SlotValue> {
        self.table_slots
            .get(slot_index)
            .map(|table_slot| &table_slot.slot_value)
    }

    /// Every slot's value, keyed or not, in slot order.
    pub fn slot_values(&self) -> impl Iterator<Item = &SlotValue> {
        self.table_slots
            .iter()
            .map(|table_slot| &table_slot.slot_value)
    }

    /// How many slots are keyed now.
    pub fn keyed_slot_count(&self) -> usize {
        self.table_slots
            .iter()
            .filter(|table_slot| {
                table_slot.key_word.load(Ordering::Acquire) & KEY_STATE_MASK == KEYED_SLOT
            })
            .count()
    }
}

impl<SlotKey, SlotValue> fmt::Debug for KeyedSlotTable<SlotKey, SlotValue> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("KeyedSlotTable")
            .field("slot_count", &self.slot_count())
            .field("keyed_slot_count", &self.keyed_slot_count())
            .finish_non_exhaustive()
    }
}
