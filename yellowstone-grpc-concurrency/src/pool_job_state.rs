//! One pool job's state in one word: queued, started by a pool thread, taken back by its writer,
//! or done. The input sits behind the word, and only the one CAS that leaves QUEUED takes it.

use crate::sync::{AtomicU8, Ordering, UnsafeCell};

const QUEUED_PHASE: u8 = 0;
const STARTED_PHASE: u8 = 1;
const TAKEN_BACK_PHASE: u8 = 2;
const DONE_PHASE: u8 = 3;

/// Where a job is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolJobPhase {
    Queued,
    Started,
    TakenBack,
    Done,
}

/// What a writer's take-back found.
#[derive(Debug)]
pub enum TakeBackOutcome<JobInput> {
    /// The job had not started: the writer runs it with this input.
    TookBack(JobInput),
    /// A pool thread runs it, or ran it: its result comes through the job's own channel.
    Started,
    /// The writer took it back before.
    AlreadyTakenBack,
}

/// A job's state word and its input. `try_start` and `try_take_back` race with one CAS from
/// QUEUED, so exactly one of them takes the input; dropped while QUEUED, the input drops too.
pub struct PoolJobState<JobInput> {
    job_phase: AtomicU8,
    job_input: UnsafeCell<Option<JobInput>>,
}

// SAFETY: the input moves between threads, so it must be `Send`. Only the thread whose CAS left
// QUEUED touches the cell, once, and the CAS's acquire orders it after the queuing.
unsafe impl<JobInput: Send> Sync for PoolJobState<JobInput> {}

// A pool thread and the writer share a job state whose input moves to one of them.
const _: () = {
    const fn require_send_and_sync<Shared: Send + Sync>() {}
    require_send_and_sync::<PoolJobState<Vec<u8>>>();
};

impl<JobInput> PoolJobState<JobInput> {
    /// A queued job with `job_input`.
    pub const fn new(job_input: JobInput) -> Self {
        Self {
            job_phase: AtomicU8::new(QUEUED_PHASE),
            job_input: UnsafeCell::new(Some(job_input)),
        }
    }

    /// A pool thread starts the job: its input, or `None` when the writer took it back first.
    pub fn try_start(&self) -> Option<JobInput> {
        self.leave_queued(STARTED_PHASE).then(|| self.take_input())
    }

    /// The writer takes the job back if no pool thread has started it.
    pub fn try_take_back(&self) -> TakeBackOutcome<JobInput> {
        if self.leave_queued(TAKEN_BACK_PHASE) {
            return TakeBackOutcome::TookBack(self.take_input());
        }
        match self.phase() {
            PoolJobPhase::TakenBack => TakeBackOutcome::AlreadyTakenBack,
            _ => TakeBackOutcome::Started,
        }
    }

    /// Marks a started job done. Only a diagnostic: the job's result is the real signal.
    pub fn mark_done(&self) {
        self.job_phase.store(DONE_PHASE, Ordering::Release);
    }

    pub fn phase(&self) -> PoolJobPhase {
        match self.job_phase.load(Ordering::Acquire) {
            QUEUED_PHASE => PoolJobPhase::Queued,
            STARTED_PHASE => PoolJobPhase::Started,
            TAKEN_BACK_PHASE => PoolJobPhase::TakenBack,
            _ => PoolJobPhase::Done,
        }
    }

    /// The one CAS out of QUEUED; acquire, so the winner sees the input as queued.
    fn leave_queued(&self, next_phase: u8) -> bool {
        self.job_phase
            .compare_exchange(
                QUEUED_PHASE,
                next_phase,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Takes the input; called once, by the CAS winner.
    fn take_input(&self) -> JobInput {
        // SAFETY: only the thread whose CAS left QUEUED reaches here, and no CAS leaves QUEUED
        // twice, so no other access to the cell runs at the same time or later.
        self.job_input
            .with_mut(|input_pointer| unsafe { (*input_pointer).take() })
            .expect("the input is taken once, by the CAS that leaves QUEUED")
    }
}

impl<JobInput> std::fmt::Debug for PoolJobState<JobInput> {
    fn fmt(&self, debug_formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug_formatter
            .debug_struct("PoolJobState")
            .field("job_phase", &self.phase())
            .finish_non_exhaustive()
    }
}
