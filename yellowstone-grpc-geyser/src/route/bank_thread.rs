//! The thread that frees the banks block reconstruction evicts, and indexes the route keys of
//! the banks that seal.

#[cfg(doc)]
use crate::{grpc::GrpcService, plugin::entry::Plugin};
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

/// Block reconstruction's side of the thread that frees the banks block reconstruction evicts,
/// since freeing a whole bank takes milliseconds, and indexes the route keys of the banks that
/// seal while replay is on.
///
/// Block reconstruction hands it each chore through a bounded channel whose slots are allocated
/// up front, so a hand-over makes no allocator call. When the channel is full, or the thread has
/// stopped, block reconstruction frees an evicted slot's banks itself, and a sealed bank stays
/// unindexed: a replay visits every message of a bank without an index. Dropping it only closes
/// the channel: the [`BankThreadHandle`] stops and joins the thread.
///
/// # Examples
///
/// ```ignore
/// let (bank_thread, bank_thread_handle) = BankThread::spawn()?;
/// // At the end of each pass of block reconstruction:
/// bank_thread.hand_over_banks(&mut block_machine);
/// // Once the runtime that runs block reconstruction has shut down:
/// bank_thread_handle.stop_and_join();
/// ```
pub struct BankThread {
    /// Hands each chore to the thread.
    pub bank_chores_tx: crossbeam_channel::Sender<BankChore>,
}

/// The plugin's side of the bank thread, which stops and joins it.
///
/// [`Plugin`]'s `on_unload` calls [`BankThreadHandle::stop_and_join`] once the runtime has shut
/// down, off the runtime's workers. The thread then ends after its current chore, even while a
/// task that outlived the runtime's shutdown still holds the [`BankThread`].
#[derive(Debug)]
pub struct BankThreadHandle {
    /// Dropped to stop the thread: its receiver then reads disconnected.
    thread_stop_tx: Option<crossbeam_channel::Sender<()>>,
    /// The thread, until the handle joins it.
    chore_thread: Option<std::thread::JoinHandle<()>>,
}

impl BankThread {
    /// Spawns the thread, named `solGrpcBankDrop`.
    ///
    /// A new thread takes the scheduling policy of the thread that spawns it.
    /// [`GrpcService::create`] calls this on the thread that loads the plugin, so the bank
    /// thread keeps that thread's policy, SCHED_OTHER by default, even when the runtime's
    /// workers run at a real-time one.
    ///
    /// The thread runs each chore in turn until its [`BankThreadHandle`] stops it, or the
    /// [`BankThread`] drops and every chore is done. A stopped thread runs no further chore: it
    /// drops the queued ones, which frees their banks, and ends. A chore handed over while it
    /// ends stays queued until the [`BankThread`] drops.
    ///
    /// # Returns
    ///
    /// The bank thread, with an empty channel of [`BANK_CHORES_CAPACITY`] slots, and the handle
    /// that stops and joins it.
    ///
    /// # Errors
    ///
    /// Fails if the OS cannot create the thread.
    pub fn spawn() -> io::Result<(Self, BankThreadHandle)> {
        let (bank_chores_tx, bank_chores_rx) =
            crossbeam_channel::bounded::<BankChore>(BANK_CHORES_CAPACITY);
        // One slot, so a wait registers no heap packet. Nothing is ever sent on it.
        let (thread_stop_tx, thread_stop_rx) = crossbeam_channel::bounded::<()>(1);
        let chore_thread = std::thread::Builder::new()
            .name("solGrpcBankDrop".to_owned())
            .spawn(move || {
                let mut scratch_postings = Vec::new();
                loop {
                    crossbeam_channel::select_biased! {
                        recv(thread_stop_rx) -> _ => break,
                        recv(bank_chores_rx) -> bank_chore => {
                            let Ok(bank_chore) = bank_chore else {
                                break;
                            };
                            // A stop that lands while the select checks the chores wins, and
                            // this chore drops.
                            let stop_error = thread_stop_rx.try_recv().err();
                            if stop_error == Some(crossbeam_channel::TryRecvError::Disconnected) {
                                break;
                            }
                            match bank_chore {
                                BankChore::IndexRouteKeys(sealed_bank) => {
                                    sealed_bank.index_route_keys(&mut scratch_postings)
                                }
                                BankChore::FreeEvictedBanks(evicted_slot_banks) => {
                                    drop(evicted_slot_banks)
                                }
                            }
                        }
                    }
                }
                // Frees the banks of the chores still queued, and builds no index.
                bank_chores_rx.try_iter().for_each(drop);
            })?;
        let bank_thread_handle = BankThreadHandle {
            thread_stop_tx: Some(thread_stop_tx),
            chore_thread: Some(chore_thread),
        };
        Ok((Self { bank_chores_tx }, bank_thread_handle))
    }

    /// Hands the banks of one evicted slot to the thread, or frees them here when the channel is
    /// full or the thread has stopped.
    ///
    /// # Arguments
    ///
    /// * `evicted_slot_banks` - The banks of one evicted slot.
    pub fn free_evicted_banks(&self, evicted_slot_banks: Vec<Arc<FrozenBank>>) {
        // A failed send gives the banks back in the error, which frees them here.
        let _ = self
            .bank_chores_tx
            .try_send(BankChore::FreeEvictedBanks(evicted_slot_banks));
    }

    /// Hands a bank that just sealed to the thread, which builds its route key index. When the
    /// channel is full or the thread has stopped, the bank stays unindexed.
    ///
    /// # Arguments
    ///
    /// * `sealed_bank` - The bank to index.
    pub fn index_route_keys(&self, sealed_bank: Arc<FrozenBank>) {
        let _ = self
            .bank_chores_tx
            .try_send(BankChore::IndexRouteKeys(sealed_bank));
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

impl BankThreadHandle {
    /// Tells the thread to stop, and does not wait: it ends after its current chore and drops the
    /// chores still queued. It runs no chore handed over later.
    pub fn signal_stop(&mut self) {
        drop(self.thread_stop_tx.take());
    }

    /// Stops the thread and waits for it to end: at most its current chore, then the frees of
    /// the chores still queued.
    pub fn stop_and_join(mut self) {
        self.stop_and_join_in_place();
    }

    /// Stops the thread and waits for it to end, once.
    fn stop_and_join_in_place(&mut self) {
        self.signal_stop();
        if let Some(chore_thread) = self.chore_thread.take() {
            let _ = chore_thread.join();
        }
    }
}

impl Drop for BankThreadHandle {
    /// Stops and joins the thread, as [`BankThreadHandle::stop_and_join`] does.
    fn drop(&mut self) {
        self.stop_and_join_in_place();
    }
}
