//! [`SingleTaskCell`]: a value that two handles of one task use in turn, never at once. An entry
//! takes one compare-exchange and never waits: an overlapping entry fails instead.

use crate::sync::{AtomicBool, Ordering, UnsafeCell};

/// A value shared by handles that one task polls one after the other.
pub struct SingleTaskCell<CellValue> {
    entered: AtomicBool,
    cell_value: UnsafeCell<CellValue>,
}

// SAFETY: `try_with` gives the value to one entry at a time: the entry flag's compare-exchange
// admits one caller, and its release store happens after that caller's last access.
unsafe impl<CellValue: Send> Send for SingleTaskCell<CellValue> {}
// SAFETY: as above; a shared reference only reaches the value through `try_with`.
unsafe impl<CellValue: Send> Sync for SingleTaskCell<CellValue> {}

/// Clears the entry flag when an entry ends, a panicking one included.
struct EntryExit<'cell> {
    entered: &'cell AtomicBool,
}

impl Drop for EntryExit<'_> {
    fn drop(&mut self) {
        self.entered.store(false, Ordering::Release);
    }
}

impl<CellValue> SingleTaskCell<CellValue> {
    pub const fn new(cell_value: CellValue) -> Self {
        Self {
            entered: AtomicBool::new(false),
            cell_value: UnsafeCell::new(cell_value),
        }
    }

    /// Runs `use_value` on the value, or returns `None` at once when another entry is in progress.
    pub fn try_with<UseResult>(
        &self,
        use_value: impl FnOnce(&mut CellValue) -> UseResult,
    ) -> Option<UseResult> {
        self.entered
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()?;
        let _entry_exit = EntryExit {
            entered: &self.entered,
        };
        // SAFETY: the compare-exchange above admitted only this entry, until `_entry_exit` drops.
        Some(
            self.cell_value
                .with_mut(|value_pointer| use_value(unsafe { &mut *value_pointer })),
        )
    }
}
