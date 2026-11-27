//! The thread that frees the banks block reconstruction evicts, builds the key index of each
//! bank that seals, and retires the compression groups of settled banks.

#[cfg(doc)]
use crate::plugin::entry::Plugin;
use {
    crate::{
        block_reconstruction_v2::{
            BlockMachineStorage, FrozenBank, GroupRetirementQueue, RetiringBank,
        },
        extension::seams::ThreadRole,
        threads::{PluginThreadHandle, PluginThreads},
    },
    std::{
        io,
        sync::Arc,
        time::{Duration, Instant},
    },
};

/// How many chores can wait for the [`BankThread`]. Past that, block reconstruction frees an
/// evicted slot's banks itself, and a sealed bank stays unindexed.
pub const BANK_CHORES_CAPACITY: usize = 128;

/// A chore block reconstruction hands to the [`BankThread`].
pub enum BankChore {
    /// Build the key index of a bank that just sealed.
    BuildKeyIndex(Arc<FrozenBank>),
    /// Free the banks of one evicted slot.
    FreeEvictedBanks(Vec<Arc<FrozenBank>>),
    /// Retire a settled bank's compression groups once its finalized writers have run.
    RetireCompressedGroups(RetiringBank),
}

/// How long the thread waits for a chore when no retirement is due by time.
const IDLE_CHORE_WAIT: Duration = Duration::from_secs(3600);

/// Hands evicted banks to a thread that frees them (milliseconds per bank), and sealed banks for
/// key indexing. With the channel full or the thread stopped, block reconstruction frees inline.
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
    chore_thread: Option<PluginThreadHandle>,
}

impl BankThread {
    /// Spawns `solGrpcBankDrop` through `plugin_threads`. It runs each chore until its handle stops it,
    /// which drops and so frees the queued banks, or the [`BankThread`] drops.
    pub fn spawn_bank_thread(
        plugin_threads: &PluginThreads,
    ) -> io::Result<(Self, BankThreadHandle)> {
        let (bank_chores_tx, bank_chores_rx) =
            crossbeam_channel::bounded::<BankChore>(BANK_CHORES_CAPACITY);
        // One slot, so a wait registers no heap packet. Nothing is ever sent on it.
        let (thread_stop_tx, thread_stop_rx) = crossbeam_channel::bounded::<()>(1);
        let chore_thread =
            plugin_threads.spawn_plugin_thread(ThreadRole::BankRelease, 0, move || {
                let mut scratch_postings = Vec::new();
                let mut group_retirement_queue = GroupRetirementQueue::default();
                loop {
                    let retirement_wait = group_retirement_queue
                        .time_until_next_retirement(Instant::now())
                        .unwrap_or(IDLE_CHORE_WAIT);
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
                                BankChore::BuildKeyIndex(sealed_bank) => {
                                    sealed_bank.build_key_index(&mut scratch_postings)
                                }
                                BankChore::FreeEvictedBanks(evicted_slot_banks) => {
                                    drop(evicted_slot_banks)
                                }
                                BankChore::RetireCompressedGroups(retiring_bank) => {
                                    group_retirement_queue.push_retiring_bank(retiring_bank)
                                }
                            }
                        }
                        default(retirement_wait) => {}
                    }
                    group_retirement_queue.retire_due_banks(Instant::now());
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
    pub fn free_evicted_banks(&self, evicted_slot_banks: Vec<Arc<FrozenBank>>) {
        // A failed send gives the banks back in the error, which frees them here.
        let _ = self
            .bank_chores_tx
            .try_send(BankChore::FreeEvictedBanks(evicted_slot_banks));
    }

    /// Hands a bank that just sealed to the thread, which builds its bank key index. When the
    /// channel is full or the thread has stopped, the bank stays unindexed.
    pub fn build_key_index(&self, sealed_bank: Arc<FrozenBank>) {
        let _ = self
            .bank_chores_tx
            .try_send(BankChore::BuildKeyIndex(sealed_bank));
    }

    /// Hands the thread, since the last hand-over, the banks of each slot `block_machine` evicted,
    /// each bank it sealed, and each bank that settled, which a full channel keeps for later.
    #[inline]
    pub fn hand_over_banks(&self, block_machine: &mut BlockMachineStorage) {
        for evicted_slot_banks in block_machine.drain_evicted_banks() {
            self.free_evicted_banks(evicted_slot_banks);
        }
        for sealed_bank in block_machine.drain_sealed_banks() {
            self.build_key_index(sealed_bank);
        }
        while let Some(retiring_bank) = block_machine.take_retiring_bank() {
            let chore_send = self
                .bank_chores_tx
                .try_send(BankChore::RetireCompressedGroups(retiring_bank));
            if let Err(crossbeam_channel::TrySendError::Full(BankChore::RetireCompressedGroups(
                retiring_bank,
            ))) = chore_send
            {
                block_machine.return_refused_bank(retiring_bank);
                break;
            }
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
            chore_thread.join_plugin_thread();
        }
    }
}

impl Drop for BankThreadHandle {
    /// Stops and joins the thread, as [`BankThreadHandle::stop_and_join`] does.
    fn drop(&mut self) {
        self.stop_and_join_in_place();
    }
}
