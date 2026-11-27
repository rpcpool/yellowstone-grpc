//! [`ClaimCell`]: a value that the one writer holding its claim builds and publishes through
//! `arc-swap`; readers share it until it retires, and build privately when it is not ready.

use {
    crate::sync::{AtomicU64, Ordering},
    arc_swap::{ArcSwapOption, Guard},
    std::{fmt, ops::Deref, ptr, sync::Arc},
};

const PHASE_BITS: u32 = 3;
const PHASE_MASK: u64 = (1 << PHASE_BITS) - 1;
const GENERATION_STEP: u64 = 1 << PHASE_BITS;
const EMPTY_PHASE: u64 = 0;
const CLAIMED_PHASE: u64 = 1;
const READY_PHASE: u64 = 2;
const FAILED_PHASE: u64 = 3;
const RETIRED_PHASE: u64 = 4;
/// The word a guard keeps once its claim was published or moved; no state word has these bits.
const ENDED_CLAIM_WORD: u64 = u64::MAX;

/// Runs when a cell stops reading CLAIMED, for example to wake the claim's waiters.
pub trait ClaimEndHook {
    /// Runs once per claim that ends without a publish, after the cell stopped reading CLAIMED.
    /// It must not panic.
    fn claim_failed(&self);

    /// Runs once per publish, after the cell stopped reading CLAIMED; by default nothing. It must
    /// not panic.
    fn claim_published(&self) {}

    /// Runs when a retire moves a CLAIMED cell on, before its claimant ends; by default nothing.
    /// It must not panic.
    fn claim_retired(&self) {}
}

/// The hook of a cell that nobody waits on.
impl ClaimEndHook for () {
    fn claim_failed(&self) {}
}

/// One value in a state word: EMPTY, CLAIMED by one [`ClaimGuard`], then READY or FAILED.
/// Any phase moves on to RETIRED, the last one, at the next generation.
pub struct ClaimCell<CellValue, EndHook = ()> {
    state_word: AtomicU64,
    published_value: ArcSwapOption<CellValue>,
    end_hook: EndHook,
}

impl<CellValue, EndHook: ClaimEndHook> ClaimCell<CellValue, EndHook> {
    /// An EMPTY cell at generation 0 that runs `end_hook` when it stops reading CLAIMED.
    pub fn new(end_hook: EndHook) -> Self {
        Self {
            state_word: AtomicU64::new(EMPTY_PHASE),
            published_value: ArcSwapOption::empty(),
            end_hook,
        }
    }

    /// The hook this cell runs when it stops reading CLAIMED.
    pub const fn end_hook(&self) -> &EndHook {
        &self.end_hook
    }

    /// Claims an EMPTY cell, or says what the cell holds instead. A cell is claimed at most once,
    /// so FAILED and RETIRED cells are never claimed again.
    pub fn try_claim(&self) -> Result<ClaimGuard<&Self>, ClaimCellContents<CellValue>> {
        let observed_word = self.state_word.load(Ordering::Acquire);
        if observed_word & PHASE_MASK != EMPTY_PHASE {
            return Err(self.contents_of(observed_word));
        }
        let claimed_word = with_phase(observed_word, CLAIMED_PHASE);
        match self.state_word.compare_exchange(
            observed_word,
            claimed_word,
            Ordering::Acquire,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(ClaimGuard {
                cell_holder: self,
                claimed_word,
            }),
            Err(current_word) => Err(self.contents_of(current_word)),
        }
    }

    /// What the cell holds now. A READY value comes with a guard that keeps it alive.
    pub fn read_contents(&self) -> ClaimCellContents<CellValue> {
        self.contents_of(self.state_word.load(Ordering::Acquire))
    }

    /// Moves the cell to RETIRED at the next generation and drops its value, without waiting for
    /// readers; a claim in flight runs `claim_retired` on this thread. False when RETIRED already.
    pub fn mark_retired(&self) -> bool {
        let mut observed_word = self.state_word.load(Ordering::Relaxed);
        loop {
            if observed_word & PHASE_MASK == RETIRED_PHASE {
                return false;
            }
            let retired_word = with_phase(observed_word + GENERATION_STEP, RETIRED_PHASE);
            match self.state_word.compare_exchange_weak(
                observed_word,
                retired_word,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current_word) => observed_word = current_word,
            }
        }
        drop(self.published_value.swap(None));
        if observed_word & PHASE_MASK == CLAIMED_PHASE {
            self.end_hook.claim_retired();
        }
        true
    }

    /// The cell's generation, which [`Self::mark_retired`] moves on by one.
    pub fn cell_generation(&self) -> u64 {
        self.state_word.load(Ordering::Acquire) >> PHASE_BITS
    }

    fn contents_of(&self, observed_word: u64) -> ClaimCellContents<CellValue> {
        match observed_word & PHASE_MASK {
            EMPTY_PHASE => ClaimCellContents::Empty,
            CLAIMED_PHASE => ClaimCellContents::Claimed,
            READY_PHASE => {
                // An empty load after READY means a retire has dropped the value since.
                let value_guard = self.published_value.load();
                if value_guard.is_some() {
                    ClaimCellContents::Ready(ReadyValueGuard { value_guard })
                } else {
                    ClaimCellContents::Retired
                }
            }
            FAILED_PHASE => ClaimCellContents::Failed,
            _ => ClaimCellContents::Retired,
        }
    }
}

impl<CellValue, EndHook: ClaimEndHook + Default> Default for ClaimCell<CellValue, EndHook> {
    fn default() -> Self {
        Self::new(EndHook::default())
    }
}

impl<CellValue, EndHook> fmt::Debug for ClaimCell<CellValue, EndHook> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let observed_word = self.state_word.load(Ordering::Relaxed);
        debug_formatter
            .debug_struct("ClaimCell")
            .field("phase", &phase_name(observed_word))
            .field("generation", &(observed_word >> PHASE_BITS))
            .finish_non_exhaustive()
    }
}

/// What a read of a [`ClaimCell`] found.
pub enum ClaimCellContents<CellValue> {
    /// Nobody has claimed the cell.
    Empty,
    /// A claimant is building the value: work privately, or wait for it.
    Claimed,
    /// The published value, which the guard keeps alive even after the cell retires.
    Ready(ReadyValueGuard<CellValue>),
    /// The claim ended without a value: work privately.
    Failed,
    /// The value was dropped, or never published: work privately.
    Retired,
}

/// Names the contents only, so it needs no `Debug` value and never prints a whole body.
impl<CellValue> fmt::Debug for ClaimCellContents<CellValue> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter.write_str(match self {
            Self::Empty => "Empty",
            Self::Claimed => "Claimed",
            Self::Ready(_) => "Ready",
            Self::Failed => "Failed",
            Self::Retired => "Retired",
        })
    }
}

/// A published value, valid while the guard lives. It is an `arc-swap` guard, so hold it briefly
/// or turn it into an `Arc`.
pub struct ReadyValueGuard<CellValue> {
    value_guard: Guard<Option<Arc<CellValue>>>,
}

impl<CellValue> ReadyValueGuard<CellValue> {
    /// The value as an `Arc`, to keep beyond the guard.
    pub fn into_shared_value(self) -> Arc<CellValue> {
        Guard::into_inner(self.value_guard)
            .unwrap_or_else(|| unreachable!("a ready guard always holds a value"))
    }
}

impl<CellValue> Deref for ReadyValueGuard<CellValue> {
    type Target = CellValue;

    fn deref(&self) -> &CellValue {
        match self.value_guard.as_deref() {
            Some(published_value) => published_value,
            None => unreachable!("a ready guard always holds a value"),
        }
    }
}

impl<CellValue: fmt::Debug> fmt::Debug for ReadyValueGuard<CellValue> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_tuple("ReadyValueGuard")
            .field(&**self)
            .finish()
    }
}

/// What a [`ClaimGuard`] reaches its cell through: a reference, or an owner of the cell, such
/// as an `Arc`, with which the claim can move to another thread. Every `Deref` to a cell is one.
pub trait ClaimCellHolder {
    /// The value the cell holds.
    type CellValue;
    /// The hook the cell runs when it stops reading CLAIMED.
    type EndHook: ClaimEndHook;

    /// The held cell, the same one on every call.
    fn held_cell(&self) -> &ClaimCell<Self::CellValue, Self::EndHook>;
}

impl<CellHolder, CellValue, EndHook> ClaimCellHolder for CellHolder
where
    CellHolder: Deref<Target = ClaimCell<CellValue, EndHook>>,
    EndHook: ClaimEndHook,
{
    type CellValue = CellValue;
    type EndHook = EndHook;

    fn held_cell(&self) -> &ClaimCell<CellValue, EndHook> {
        self
    }
}

/// The one claim on a [`ClaimCell`]. [`Self::publish`] makes the cell READY; dropped unpublished,
/// on unwind or early return, the guard stores FAILED with release, then runs the end hook.
#[must_use = "dropping the guard fails the claim"]
pub struct ClaimGuard<CellHolder: ClaimCellHolder> {
    cell_holder: CellHolder,
    claimed_word: u64,
}

impl<CellHolder: ClaimCellHolder> ClaimGuard<CellHolder> {
    /// Publishes `published_value` and hands it back. A cell retired during the claim keeps
    /// nothing, and its value stays the claimant's own.
    pub fn publish(
        mut self,
        published_value: Arc<CellHolder::CellValue>,
    ) -> Arc<CellHolder::CellValue> {
        let claimed_cell = self.cell_holder.held_cell();
        claimed_cell
            .published_value
            .store(Some(Arc::clone(&published_value)));
        let ready_word = with_phase(self.claimed_word, READY_PHASE);
        let publish_result = claimed_cell.state_word.compare_exchange(
            self.claimed_word,
            ready_word,
            Ordering::Release,
            Ordering::Relaxed,
        );
        if publish_result.is_err() {
            drop(claimed_cell.published_value.swap(None));
        }
        self.claimed_word = ENDED_CLAIM_WORD;
        claimed_cell.end_hook.claim_published();
        published_value
    }

    /// Moves the claim to `new_holder`, an owner of the same cell, for example to hand the work
    /// to another thread. Panics, and fails the claim, when `new_holder` holds another cell.
    pub fn into_held<NewHolder>(mut self, new_holder: NewHolder) -> ClaimGuard<NewHolder>
    where
        NewHolder:
            ClaimCellHolder<CellValue = CellHolder::CellValue, EndHook = CellHolder::EndHook>,
    {
        assert!(
            ptr::eq(self.cell_holder.held_cell(), new_holder.held_cell()),
            "a claim moves only to a holder of its own cell"
        );
        let moved_word = self.claimed_word;
        self.claimed_word = ENDED_CLAIM_WORD;
        ClaimGuard {
            cell_holder: new_holder,
            claimed_word: moved_word,
        }
    }

    /// The claimed cell.
    pub fn claimed_cell(&self) -> &ClaimCell<CellHolder::CellValue, CellHolder::EndHook> {
        self.cell_holder.held_cell()
    }
}

impl<CellHolder: ClaimCellHolder> Drop for ClaimGuard<CellHolder> {
    fn drop(&mut self) {
        if self.claimed_word == ENDED_CLAIM_WORD {
            return;
        }
        let claimed_cell = self.cell_holder.held_cell();
        let failed_word = with_phase(self.claimed_word, FAILED_PHASE);
        // A cell retired during the claim stays RETIRED, and its waiters still need the hook.
        let _ = claimed_cell.state_word.compare_exchange(
            self.claimed_word,
            failed_word,
            Ordering::Release,
            Ordering::Relaxed,
        );
        claimed_cell.end_hook.claim_failed();
    }
}

impl<CellHolder: ClaimCellHolder> fmt::Debug for ClaimGuard<CellHolder> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("ClaimGuard")
            .field("claimed_cell", self.claimed_cell())
            .finish_non_exhaustive()
    }
}

const fn with_phase(state_word: u64, cell_phase: u64) -> u64 {
    (state_word & !PHASE_MASK) | cell_phase
}

const fn phase_name(state_word: u64) -> &'static str {
    match state_word & PHASE_MASK {
        EMPTY_PHASE => "EMPTY",
        CLAIMED_PHASE => "CLAIMED",
        READY_PHASE => "READY",
        FAILED_PHASE => "FAILED",
        _ => "RETIRED",
    }
}
