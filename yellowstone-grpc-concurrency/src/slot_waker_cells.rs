//! [`SlotWakerCells`]: a slot's fixed waker cells. A waiter parks its task's waker in a free cell,
//! and when the slot stops reading CLAIMED, by publish, failure or retire, every parked waker wakes once.

use {
    crate::{
        claim_cell::ClaimEndHook,
        sync::{AtomicU8, Ordering, UnsafeCell},
    },
    std::{array, fmt, task::Waker},
};

const FREE_CELL: u8 = 0;
/// The waiter that moved the cell here owns its waker until it moves the cell on.
const REGISTERING_CELL: u8 = 1;
const PARKED_CELL: u8 = 2;
/// The slot stopped reading CLAIMED: this waker cell stays here for good, so no waiter parks
/// after the wake.
const WOKEN_CELL: u8 = 3;

/// One cell: its state word and the waker it holds while PARKED.
struct WakerCell {
    cell_state: AtomicU8,
    parked_waker: UnsafeCell<Option<Waker>>,
}

impl WakerCell {
    const fn new() -> Self {
        Self {
            cell_state: AtomicU8::new(FREE_CELL),
            parked_waker: UnsafeCell::new(None),
        }
    }

    /// Moves a REGISTERING cell to PARKED. When the slot woke the cells meanwhile, the waiter
    /// still owns the waker, so it drops it and reads the slot again instead of waiting.
    fn finish_parking(&self, waker_cell_index: usize) -> WakerRegistration {
        match self.cell_state.compare_exchange(
            REGISTERING_CELL,
            PARKED_CELL,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => WakerRegistration::Parked(WakerCellIndex(waker_cell_index)),
            Err(_) => {
                drop(self.take_owned_waker());
                WakerRegistration::ClaimEnded
            }
        }
    }

    /// The waker, taken by the one thread that owns it: a REGISTERING waiter, or the waker of the
    /// slot after its swap from PARKED.
    fn take_owned_waker(&self) -> Option<Waker> {
        // SAFETY: only the owner of the cell's waker calls this (see above).
        self.parked_waker
            .with_mut(|waker_pointer| unsafe { (*waker_pointer).take() })
    }
}

/// The fixed waker cells of one slot. They hold no allocation: a waiter that finds every cell
/// taken waits on a timer of its own instead.
pub struct SlotWakerCells<const CELL_COUNT: usize> {
    waker_cells: [WakerCell; CELL_COUNT],
}

// SAFETY: a cell's waker is reached only by the one thread its state word gives it to.
unsafe impl<const CELL_COUNT: usize> Sync for SlotWakerCells<CELL_COUNT> {}

/// The cell a waiter parked its waker in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakerCellIndex(usize);

impl WakerCellIndex {
    pub const fn cell_position(self) -> usize {
        self.0
    }
}

/// What a waiter's parking did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakerRegistration {
    /// The waker stays in this cell until the slot wakes it or the waiter cancels.
    Parked(WakerCellIndex),
    /// The slot stopped reading CLAIMED: read it again instead of waiting.
    ClaimEnded,
    /// Every cell holds another waiter: wait on a timer instead.
    NoFreeCell,
}

impl<const CELL_COUNT: usize> SlotWakerCells<CELL_COUNT> {
    pub fn new() -> Self {
        Self {
            waker_cells: array::from_fn(|_| WakerCell::new()),
        }
    }

    /// Parks a clone of `task_waker` in the first free cell.
    pub fn park_waker(&self, task_waker: &Waker) -> WakerRegistration {
        for (waker_cell_index, waker_cell) in self.waker_cells.iter().enumerate() {
            match waker_cell.cell_state.compare_exchange(
                FREE_CELL,
                REGISTERING_CELL,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // SAFETY: REGISTERING gives this waiter the cell's waker.
                    waker_cell.parked_waker.with_mut(|waker_pointer| unsafe {
                        *waker_pointer = Some(task_waker.clone());
                    });
                    return waker_cell.finish_parking(waker_cell_index);
                }
                Err(WOKEN_CELL) => return WakerRegistration::ClaimEnded,
                Err(_) => {}
            }
        }
        WakerRegistration::NoFreeCell
    }

    /// Keeps the waiter parked in `waker_cell_index`, with `task_waker` in place of its earlier
    /// waker unless both wake the same task.
    pub fn repark_waker(
        &self,
        waker_cell_index: WakerCellIndex,
        task_waker: &Waker,
    ) -> WakerRegistration {
        let waker_cell = &self.waker_cells[waker_cell_index.0];
        if waker_cell
            .cell_state
            .compare_exchange(
                PARKED_CELL,
                REGISTERING_CELL,
                Ordering::Acquire,
                Ordering::Acquire,
            )
            .is_err()
        {
            // Only the slot's wake moves a PARKED cell that its waiter does not move.
            return WakerRegistration::ClaimEnded;
        }
        // SAFETY: REGISTERING gives this waiter the cell's waker.
        let replaced_waker = waker_cell.parked_waker.with_mut(|waker_pointer| unsafe {
            match &mut *waker_pointer {
                Some(parked_waker) if parked_waker.will_wake(task_waker) => None,
                cell_waker => cell_waker.replace(task_waker.clone()),
            }
        });
        let waker_registration = waker_cell.finish_parking(waker_cell_index.0);
        drop(replaced_waker);
        waker_registration
    }

    /// Takes the waiter's waker back out of `waker_cell_index` and frees the cell, unless the
    /// slot has woken it already.
    pub fn cancel_parking(&self, waker_cell_index: WakerCellIndex) {
        let waker_cell = &self.waker_cells[waker_cell_index.0];
        if waker_cell
            .cell_state
            .compare_exchange(
                PARKED_CELL,
                REGISTERING_CELL,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return;
        }
        let cancelled_waker = waker_cell.take_owned_waker();
        // A wake that came meanwhile leaves the cell WOKEN, which it must stay.
        let _ = waker_cell.cell_state.compare_exchange(
            REGISTERING_CELL,
            FREE_CELL,
            Ordering::Release,
            Ordering::Relaxed,
        );
        drop(cancelled_waker);
    }

    /// Marks every cell WOKEN for good and wakes each parked waker once. Returns how many woke.
    pub fn wake_parked_wakers(&self) -> usize {
        let mut woken_count = 0;
        for waker_cell in &self.waker_cells {
            if waker_cell.cell_state.swap(WOKEN_CELL, Ordering::AcqRel) == PARKED_CELL {
                if let Some(parked_waker) = waker_cell.take_owned_waker() {
                    parked_waker.wake();
                    woken_count += 1;
                }
            }
        }
        woken_count
    }

    /// How many cells hold a parked waker now.
    pub fn parked_count(&self) -> usize {
        self.waker_cells
            .iter()
            .filter(|waker_cell| waker_cell.cell_state.load(Ordering::Relaxed) == PARKED_CELL)
            .count()
    }
}

impl<const CELL_COUNT: usize> Default for SlotWakerCells<CELL_COUNT> {
    fn default() -> Self {
        Self::new()
    }
}

/// A claim that ends wakes the slot's waiters: after a publish they read the value, and after a
/// failure or a retire they work privately.
impl<const CELL_COUNT: usize> ClaimEndHook for SlotWakerCells<CELL_COUNT> {
    fn claim_failed(&self) {
        self.wake_parked_wakers();
    }

    fn claim_published(&self) {
        self.wake_parked_wakers();
    }

    fn claim_retired(&self) {
        self.wake_parked_wakers();
    }
}

impl<const CELL_COUNT: usize> fmt::Debug for SlotWakerCells<CELL_COUNT> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_list()
            .entries(self.waker_cells.iter().map(|waker_cell| {
                match waker_cell.cell_state.load(Ordering::Relaxed) {
                    FREE_CELL => "FREE",
                    REGISTERING_CELL => "REGISTERING",
                    PARKED_CELL => "PARKED",
                    _ => "WOKEN",
                }
            }))
            .finish()
    }
}
