//! [`CountedMessageRef`]: the entry count in a captured message's header. While the count is
//! above 0 it holds one strong reference to the message, so each entry is one pointer.

use {
    crate::sync::{fence, Arc, AtomicUsize, Ordering},
    std::{fmt, iter::FusedIterator, mem::ManuallyDrop, ops::Deref, process, ptr::NonNull},
};

const MAX_OUTSTANDING_ENTRIES: usize = isize::MAX as usize;

/// A message's entry count. Each adder adds its entries with one `fetch_add`, and each entry
/// releases once; see [`AddMessageEntries`].
pub struct CountedMessageRef {
    outstanding_entries: AtomicUsize,
}

impl CountedMessageRef {
    /// A count with no entries.
    pub const fn new() -> Self {
        Self {
            outstanding_entries: AtomicUsize::new(0),
        }
    }

    /// The entries added and not yet released.
    pub fn outstanding_entries(&self) -> usize {
        self.outstanding_entries.load(Ordering::Acquire)
    }
}

impl Default for CountedMessageRef {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CountedMessageRef {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("CountedMessageRef")
            .field(
                "outstanding_entries",
                &self.outstanding_entries.load(Ordering::Relaxed),
            )
            .finish()
    }
}

/// # Safety
/// The message lives in an [`Arc`], and `entry_references` returns the same count all its life.
pub unsafe trait CountedMessageOwner: Sized + Send + Sync {
    /// The entry count in this message's header.
    fn entry_references(&self) -> &CountedMessageRef;

    /// # Safety
    /// Runs on a rise from 0, with `owner_pointer` from `Arc::as_ptr` of an `Arc` the caller holds.
    unsafe fn take_strong_reference(owner_pointer: NonNull<Self>) {
        // SAFETY: the caller's `Arc` keeps the strong count above 0 during the call.
        unsafe { Arc::increment_strong_count(owner_pointer.as_ptr()) };
    }

    /// # Safety
    /// Runs when the count falls to 0 and drops the rise's reference, which may free the message.
    unsafe fn drop_strong_reference(owner_pointer: NonNull<Self>) {
        // SAFETY: the count still holds the reference that `take_strong_reference` took.
        unsafe { Arc::decrement_strong_count(owner_pointer.as_ptr()) };
    }
}

/// Adds counted entries to the message that an [`Arc`] holds.
pub trait AddMessageEntries {
    /// The message the `Arc` holds.
    type MessageOwner: CountedMessageOwner;

    /// Adds `entry_count` entries with one `fetch_add`. A count that was 0 takes a strong
    /// reference before any of the entries exists.
    #[must_use = "dropping the entries releases them"]
    fn add_message_entries(&self, entry_count: usize) -> AddedMessageEntries<Self::MessageOwner>;
}

impl<MessageOwner: CountedMessageOwner> AddMessageEntries for Arc<MessageOwner> {
    type MessageOwner = MessageOwner;

    fn add_message_entries(&self, entry_count: usize) -> AddedMessageEntries<MessageOwner> {
        // Unlike a pointer made from `&MessageOwner`, this one may reach the `Arc`'s counts.
        let owner_pointer = NonNull::new(Arc::as_ptr(self).cast_mut())
            .unwrap_or_else(|| unreachable!("an `Arc` never points at null"));
        if entry_count > 0 {
            let previous_entries = self
                .entry_references()
                .outstanding_entries
                .fetch_add(entry_count, Ordering::Relaxed);
            let total_entries = previous_entries.checked_add(entry_count);
            if total_entries.is_none_or(|entry_total| entry_total > MAX_OUTSTANDING_ENTRIES) {
                process::abort();
            }
            if previous_entries == 0 {
                // SAFETY: the pointer comes from `Arc::as_ptr` of `self`, which the caller holds.
                unsafe { MessageOwner::take_strong_reference(owner_pointer) };
            }
        }
        AddedMessageEntries {
            owner_pointer,
            entries_left: entry_count,
        }
    }
}

/// The entries one [`AddMessageEntries::add_message_entries`] call added. It hands them out one
/// by one, and releases the ones it still holds when it drops.
pub struct AddedMessageEntries<MessageOwner: CountedMessageOwner> {
    owner_pointer: NonNull<MessageOwner>,
    entries_left: usize,
}

// SAFETY: the entries reach the message only through `&MessageOwner`, which is `Send` and `Sync`.
unsafe impl<MessageOwner: CountedMessageOwner> Send for AddedMessageEntries<MessageOwner> {}
// SAFETY: a shared `AddedMessageEntries` gives no access to the message.
unsafe impl<MessageOwner: CountedMessageOwner> Sync for AddedMessageEntries<MessageOwner> {}

impl<MessageOwner: CountedMessageOwner> Iterator for AddedMessageEntries<MessageOwner> {
    type Item = CountedMessageEntry<MessageOwner>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_left == 0 {
            return None;
        }
        self.entries_left -= 1;
        Some(CountedMessageEntry {
            owner_pointer: self.owner_pointer,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.entries_left, Some(self.entries_left))
    }
}

impl<MessageOwner: CountedMessageOwner> ExactSizeIterator for AddedMessageEntries<MessageOwner> {}

impl<MessageOwner: CountedMessageOwner> FusedIterator for AddedMessageEntries<MessageOwner> {}

impl<MessageOwner: CountedMessageOwner> Drop for AddedMessageEntries<MessageOwner> {
    fn drop(&mut self) {
        if self.entries_left > 0 {
            // SAFETY: the entries not handed out are released here, once.
            unsafe { release_entries(self.owner_pointer, self.entries_left) };
        }
    }
}

impl<MessageOwner: CountedMessageOwner> fmt::Debug for AddedMessageEntries<MessageOwner> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("AddedMessageEntries")
            .field("owner_pointer", &self.owner_pointer)
            .field("entries_left", &self.entries_left)
            .finish()
    }
}

/// One entry of a message, one pointer wide: it reads the message until it drops and releases.
pub struct CountedMessageEntry<MessageOwner: CountedMessageOwner> {
    owner_pointer: NonNull<MessageOwner>,
}

// SAFETY: the entry reaches the message only through `&MessageOwner`, which is `Send` and `Sync`.
unsafe impl<MessageOwner: CountedMessageOwner> Send for CountedMessageEntry<MessageOwner> {}
// SAFETY: as above; a shared entry gives `&MessageOwner` only.
unsafe impl<MessageOwner: CountedMessageOwner> Sync for CountedMessageEntry<MessageOwner> {}

impl<MessageOwner: CountedMessageOwner> CountedMessageEntry<MessageOwner> {
    /// The message pointer of an entry that stays unreleased; [`Self::from_raw`] takes it back.
    pub fn into_raw(self) -> NonNull<MessageOwner> {
        let unreleased_entry = ManuallyDrop::new(self);
        unreleased_entry.owner_pointer
    }

    /// # Safety
    /// `owner_pointer` comes from [`Self::into_raw`], and each such pointer comes back once.
    pub const unsafe fn from_raw(owner_pointer: NonNull<MessageOwner>) -> Self {
        Self { owner_pointer }
    }
}

impl<MessageOwner: CountedMessageOwner> Deref for CountedMessageEntry<MessageOwner> {
    type Target = MessageOwner;

    fn deref(&self) -> &MessageOwner {
        // SAFETY: the unreleased entry keeps the count above 0, which keeps the message alive.
        unsafe { self.owner_pointer.as_ref() }
    }
}

impl<MessageOwner: CountedMessageOwner> Drop for CountedMessageEntry<MessageOwner> {
    fn drop(&mut self) {
        // SAFETY: the entry is released here, once.
        unsafe { release_entries(self.owner_pointer, 1) };
    }
}

impl<MessageOwner: CountedMessageOwner> fmt::Debug for CountedMessageEntry<MessageOwner> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("CountedMessageEntry")
            .field("owner_pointer", &self.owner_pointer)
            .finish()
    }
}

/// # Safety
/// `released_entries` unreleased entries of the message at `owner_pointer` end here.
unsafe fn release_entries<MessageOwner: CountedMessageOwner>(
    owner_pointer: NonNull<MessageOwner>,
    released_entries: usize,
) {
    // SAFETY: the entries released here keep the message alive until the `fetch_sub`.
    let entry_references = unsafe { owner_pointer.as_ref() }.entry_references();
    let previous_entries = entry_references
        .outstanding_entries
        .fetch_sub(released_entries, Ordering::Release);
    debug_assert!(previous_entries >= released_entries);
    if previous_entries == released_entries {
        // Every read through the other entries happens before the strong reference drops.
        fence(Ordering::Acquire);
        // SAFETY: the count fell to 0, so the reference its last rise from 0 took drops here.
        unsafe { MessageOwner::drop_strong_reference(owner_pointer) };
    }
}
