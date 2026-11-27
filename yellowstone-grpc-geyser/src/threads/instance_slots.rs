//! Per-thread slots of one loaded plugin instance, freed with it. A thread caches only
//! (table key, slot index), so after a reload it claims again and never reads old memory.

#![deny(clippy::disallowed_types, clippy::disallowed_methods)]

use std::{
    cell::Cell,
    collections::hash_map::RandomState,
    fmt,
    hash::BuildHasher,
    marker::PhantomData,
    num::NonZeroU64,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Instant,
};

/// The owned slots of the plugin's table. Threads past them share its overflow slot.
pub const PLUGIN_THREAD_SLOT_COUNT: usize = 1024;

/// The cached index that names the overflow slot.
const OVERFLOW_SLOT_INDEX: usize = usize::MAX;

/// Names one loaded plugin instance: drawn at `on_load` and held by the instance, never by a
/// static. It is never 0, which a thread's cache uses for "no slot".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PluginInstanceGeneration(NonZeroU64);

impl PluginInstanceGeneration {
    /// Hashes the time of the draw with std's random hash keys, which the OS seeds once per
    /// thread, so two instances of one process differ.
    pub fn draw_instance_generation() -> Self {
        Self(draw_nonzero_bits())
    }
}

/// Hashes the time of the draw with std's random hash keys; never 0.
fn draw_nonzero_bits() -> NonZeroU64 {
    let drawn_bits = RandomState::new().hash_one(Instant::now());
    NonZeroU64::new(drawn_bits).unwrap_or(NonZeroU64::MIN)
}

/// A thread's slot in the table that served it last.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CachedThreadSlot {
    table_cache_key: u64,
    slot_index: usize,
}

impl CachedThreadSlot {
    /// Matches no table, since no table key is 0.
    const NO_SLOT: Self = Self {
        table_cache_key: 0,
        slot_index: 0,
    };
}

// A thread-local without Drop registers no destructor, so it never keeps the library mapped.
const _: () = assert!(!std::mem::needs_drop::<Cell<CachedThreadSlot>>());

thread_local! {
    /// Starts at `NO_SLOT` in a library mapped again; keeps an old key in a library kept
    /// mapped. Either way the next access claims from the current table.
    static CACHED_THREAD_SLOT: Cell<CachedThreadSlot> =
        const { Cell::new(CachedThreadSlot::NO_SLOT) };
}

/// Whether the calling thread owns its slot or shares the overflow slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadSlotOwnership {
    /// The thread is the slot's only writer: each word takes a relaxed load and store.
    Owned,
    /// Every thread past the owned slots writes it: each word takes an atomic RMW.
    Overflow,
}

/// One u64 of a thread slot. Its slot's thread writes it through [`ThreadSlotRef::add_to_word`];
/// any thread may read it.
#[derive(Debug, Default)]
pub struct ThreadSlotWord(AtomicU64);

/// One slot on 128 bytes of its own, so two threads' slots never share a cache line pair.
#[derive(Debug, Default)]
#[repr(align(128))]
struct AlignedThreadSlot<SlotContents>(SlotContents);

/// The per-thread slots of one plugin instance. A thread caches one slot of one table, so an
/// instance keeps one table and grows [`PluginThreadSlot`] for more per-thread state.
pub struct ThreadSlotTable<SlotContents> {
    instance_generation: PluginInstanceGeneration,
    /// Drawn per table, so a thread's cached index never names a slot of another table.
    thread_cache_key: NonZeroU64,
    owned_slots: Box<[AlignedThreadSlot<SlotContents>]>,
    overflow_slot: AlignedThreadSlot<SlotContents>,
    next_unclaimed_slot_index: AtomicUsize,
    overflow_thread_count: AtomicU64,
}

/// The calling thread's slot. Not `Send`, so the thread that claimed it stays its only writer.
#[derive(Debug)]
pub struct ThreadSlotRef<'table, SlotContents> {
    slot_contents: &'table SlotContents,
    slot_ownership: ThreadSlotOwnership,
    stays_on_its_thread: PhantomData<*const ()>,
}

impl<SlotContents: Default> ThreadSlotTable<SlotContents> {
    pub fn new(instance_generation: PluginInstanceGeneration, owned_slot_count: usize) -> Self {
        Self {
            instance_generation,
            thread_cache_key: draw_nonzero_bits(),
            owned_slots: (0..owned_slot_count)
                .map(|_| AlignedThreadSlot::default())
                .collect(),
            overflow_slot: AlignedThreadSlot::default(),
            next_unclaimed_slot_index: AtomicUsize::new(0),
            overflow_thread_count: AtomicU64::new(0),
        }
    }
}

impl<SlotContents> ThreadSlotTable<SlotContents> {
    /// The calling thread's slot: a thread-local read and one compare when the thread's cached
    /// key is this table's, else a claim, which it caches.
    #[inline]
    pub fn current_thread_slot(&self) -> ThreadSlotRef<'_, SlotContents> {
        let cached_thread_slot = CACHED_THREAD_SLOT.get();
        let slot_index = if cached_thread_slot.table_cache_key == self.thread_cache_key.get() {
            cached_thread_slot.slot_index
        } else {
            self.claim_current_thread_slot()
        };
        match self.owned_slots.get(slot_index) {
            Some(owned_slot) => ThreadSlotRef {
                slot_contents: &owned_slot.0,
                slot_ownership: ThreadSlotOwnership::Owned,
                stays_on_its_thread: PhantomData,
            },
            None => ThreadSlotRef {
                slot_contents: &self.overflow_slot.0,
                slot_ownership: ThreadSlotOwnership::Overflow,
                stays_on_its_thread: PhantomData,
            },
        }
    }

    #[cold]
    fn claim_current_thread_slot(&self) -> usize {
        let claimed_slot_index = self
            .next_unclaimed_slot_index
            .fetch_add(1, Ordering::Relaxed);
        let slot_index = if claimed_slot_index < self.owned_slots.len() {
            claimed_slot_index
        } else {
            self.overflow_thread_count.fetch_add(1, Ordering::Relaxed);
            OVERFLOW_SLOT_INDEX
        };
        CACHED_THREAD_SLOT.set(CachedThreadSlot {
            table_cache_key: self.thread_cache_key.get(),
            slot_index,
        });
        slot_index
    }

    pub fn owned_slot_count(&self) -> usize {
        self.owned_slots.len()
    }

    /// How many owned slots threads have claimed. They are the first ones, in claim order.
    pub fn claimed_slot_count(&self) -> usize {
        self.next_unclaimed_slot_index
            .load(Ordering::Relaxed)
            .min(self.owned_slots.len())
    }

    /// How many threads found every owned slot claimed and use the overflow slot.
    pub fn overflow_thread_count(&self) -> u64 {
        self.overflow_thread_count.load(Ordering::Relaxed)
    }
}

impl<SlotContents> fmt::Debug for ThreadSlotTable<SlotContents> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("ThreadSlotTable")
            .field("instance_generation", &self.instance_generation)
            .field("owned_slot_count", &self.owned_slot_count())
            .field("claimed_slot_count", &self.claimed_slot_count())
            .field("overflow_thread_count", &self.overflow_thread_count())
            .finish_non_exhaustive()
    }
}

impl<'table, SlotContents> ThreadSlotRef<'table, SlotContents> {
    /// Adds `added_amount` to the word `select_word` picks from this slot, as the slot's
    /// ownership requires.
    #[inline]
    pub fn add_to_word(
        &self,
        select_word: impl FnOnce(&'table SlotContents) -> &'table ThreadSlotWord,
        added_amount: u64,
    ) {
        let slot_word = &select_word(self.slot_contents).0;
        match self.slot_ownership {
            ThreadSlotOwnership::Owned => slot_word.store(
                slot_word.load(Ordering::Relaxed).wrapping_add(added_amount),
                Ordering::Relaxed,
            ),
            ThreadSlotOwnership::Overflow => {
                slot_word.fetch_add(added_amount, Ordering::Relaxed);
            }
        }
    }
}

/// The plugin's per-thread slot.
#[derive(Debug, Default)]
pub struct PluginThreadSlot {
    /// Callbacks the thread made into this instance.
    pub callback_count: ThreadSlotWord,
}

/// The table each loaded plugin instance draws a generation for and holds.
pub type PluginThreadSlotTable = ThreadSlotTable<PluginThreadSlot>;
