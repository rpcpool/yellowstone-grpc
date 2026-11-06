//! The thread that frees the banks block reconstruction evicts, and indexes the route keys of
//! the banks that seal.

#[cfg(doc)]
use crate::grpc::GrpcService;
use {
    crate::block_reconstruction_v2::{BlockMachineStorage, FrozenBank},
    std::{io, sync::Arc},
};

/// How many chores can wait for the [`BankThread`]. Past that, block reconstruction frees an
/// evicted slot's banks itself, and a sealed bank stays unindexed.
pub const BANK_CHORES_CAPACITY: usize = 128;

/// A chore block reconstruction hands to the [`BankThread`].
pub enum BankChore {
    /// Index the route keys of a bank that just sealed.
    IndexRouteKeys(Arc<FrozenBank>),
    /// Free the banks of one evicted slot.
    FreeEvictedBanks(Vec<Arc<FrozenBank>>),
}

/// The thread that frees the banks block reconstruction evicts, since freeing a whole bank takes
/// milliseconds, and indexes the route keys of the banks that seal while replay is on.
///
/// Block reconstruction hands it each chore through a bounded channel whose slots are allocated
/// up front, so a hand-over makes no allocator call. When the channel is full, block
/// reconstruction frees an evicted slot's banks itself, and a sealed bank stays unindexed: a
/// replay visits every message of a bank without an index. Dropping the bank thread closes the
/// channel and joins the thread, once the thread has done what was queued.
///
/// # Examples
///
/// ```ignore
/// let bank_thread = BankThread::spawn()?;
/// // At the end of each pass of block reconstruction:
/// bank_thread.hand_over_banks(&mut block_machine);
/// // Joins the thread.
/// drop(bank_thread);
/// ```
pub struct BankThread {
    /// Hands each chore to the thread, until the bank thread drops.
    pub bank_chores_tx: Option<crossbeam_channel::Sender<BankChore>>,
    /// The thread, until the bank thread drops and joins it.
    pub chore_thread: Option<std::thread::JoinHandle<()>>,
}

impl BankThread {
    /// Spawns the thread, named `solGrpcBankDrop`.
    ///
    /// A new thread takes the scheduling policy of the thread that spawns it.
    /// [`GrpcService::create`] calls this on the thread that loads the plugin, so the bank
    /// thread keeps that thread's policy, SCHED_OTHER by default, even when the runtime's
    /// workers run at a real-time one.
    ///
    /// # Returns
    ///
    /// The bank thread, with an empty channel of [`BANK_CHORES_CAPACITY`] slots.
    ///
    /// # Errors
    ///
    /// Fails if the OS cannot create the thread.
    pub fn spawn() -> io::Result<Self> {
        let (bank_chores_tx, bank_chores_rx) =
            crossbeam_channel::bounded::<BankChore>(BANK_CHORES_CAPACITY);
        let chore_thread = std::thread::Builder::new()
            .name("solGrpcBankDrop".to_owned())
            .spawn(move || {
                let mut scratch_postings = Vec::new();
                for bank_chore in bank_chores_rx {
                    match bank_chore {
                        BankChore::IndexRouteKeys(sealed_bank) => {
                            sealed_bank.index_route_keys(&mut scratch_postings)
                        }
                        BankChore::FreeEvictedBanks(evicted_slot_banks) => drop(evicted_slot_banks),
                    }
                }
            })?;
        Ok(Self {
            bank_chores_tx: Some(bank_chores_tx),
            chore_thread: Some(chore_thread),
        })
    }

    /// Hands the banks of one evicted slot to the thread, or frees them here when the channel is
    /// full.
    ///
    /// # Arguments
    ///
    /// * `evicted_slot_banks` - The banks of one evicted slot.
    pub fn free_evicted_banks(&self, evicted_slot_banks: Vec<Arc<FrozenBank>>) {
        if let Some(bank_chores_tx) = &self.bank_chores_tx {
            // A full channel gives the banks back in the error, which frees them here.
            let _ = bank_chores_tx.try_send(BankChore::FreeEvictedBanks(evicted_slot_banks));
        }
    }

    /// Hands a bank that just sealed to the thread, which builds its route key index. When the
    /// channel is full, the bank stays unindexed.
    ///
    /// # Arguments
    ///
    /// * `sealed_bank` - The bank to index.
    pub fn index_route_keys(&self, sealed_bank: Arc<FrozenBank>) {
        if let Some(bank_chores_tx) = &self.bank_chores_tx {
            let _ = bank_chores_tx.try_send(BankChore::IndexRouteKeys(sealed_bank));
        }
    }

    /// Hands the thread the banks of each slot `block_machine` evicted, with
    /// [`BankThread::free_evicted_banks`], then each bank it sealed, with
    /// [`BankThread::index_route_keys`], since the last hand-over.
    ///
    /// # Arguments
    ///
    /// * `block_machine` - The banks being reconstructed.
    #[inline]
    pub fn hand_over_banks(&self, block_machine: &mut BlockMachineStorage) {
        for evicted_slot_banks in block_machine.drain_evicted_banks() {
            self.free_evicted_banks(evicted_slot_banks);
        }
        for sealed_bank in block_machine.drain_sealed_banks() {
            self.index_route_keys(sealed_bank);
        }
    }
}

impl Drop for BankThread {
    /// Closes the channel, then waits for the thread to do what was queued and end.
    fn drop(&mut self) {
        drop(self.bank_chores_tx.take());
        if let Some(chore_thread) = self.chore_thread.take() {
            let _ = chore_thread.join();
        }
    }
}
