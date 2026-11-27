//! The atomics and cells of every primitive here.

pub use std::{
    hint::spin_loop,
    sync::{
        atomic::{fence, AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
};

/// `std::cell::UnsafeCell` with a closure API: each access names its pointer's mutability.
#[derive(Debug, Default)]
pub struct UnsafeCell<CellValue>(std::cell::UnsafeCell<CellValue>);

impl<CellValue> UnsafeCell<CellValue> {
    /// Wraps `cell_value`.
    pub const fn new(cell_value: CellValue) -> Self {
        Self(std::cell::UnsafeCell::new(cell_value))
    }

    /// Calls `read_value` with a shared pointer to the value.
    pub fn with<ReadResult>(
        &self,
        read_value: impl FnOnce(*const CellValue) -> ReadResult,
    ) -> ReadResult {
        read_value(self.0.get())
    }

    /// Calls `write_value` with a unique pointer to the value.
    pub fn with_mut<WriteResult>(
        &self,
        write_value: impl FnOnce(*mut CellValue) -> WriteResult,
    ) -> WriteResult {
        write_value(self.0.get())
    }
}
