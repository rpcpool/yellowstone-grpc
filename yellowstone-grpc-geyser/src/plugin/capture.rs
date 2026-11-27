#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
//! Capture: each agave callback builds its record here, with its shared body, on the calling
//! thread and before anything is sent. A rejected owner's account is dropped before any build.

use {
    crate::{
        plugin::message::{
            Message, MessageAccount, MessageBlockFooter, MessageBlockMeta,
            MessageDeshredTransaction, MessageDeshredUpdateParent, MessageEntry,
            MessageEntryUpdateParent, MessageSlot, MessageTransaction,
        },
        telemetry::data_plane_counts::{DataPlaneCount, PluginCallback, DATA_PLANE_COUNTS},
        threads::panic_payload_text,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockFooterInfo, ReplicaBlockInfoV5,
        ReplicaDeshredTransactionInfoVersions, ReplicaDeshredUpdateParentInfo, ReplicaEntryInfoV2,
        ReplicaEntryUpdateParentInfo, ReplicaTransactionInfoV4, SlotStatus as GeyserSlotStatus,
    },
    foldhash::HashSet as FoldHashSet,
    solana_clock::{BankId, Slot},
    solana_pubkey::Pubkey,
    std::{
        cell::Cell,
        panic::{self, AssertUnwindSafe},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    },
};

/// Builds the messages of agave's callbacks. The callback glue sends what it returns.
#[derive(Debug)]
pub struct CaptureHandle {
    /// Accounts of these owners are dropped at capture, also during startup.
    owner_reject: FoldHashSet<Pubkey>,
}

impl CaptureHandle {
    pub const fn new(owner_reject: FoldHashSet<Pubkey>) -> Self {
        Self { owner_reject }
    }

    /// Whether the account's owner is on the reject list; the callback then drops the write
    /// before anything is built.
    pub fn rejects_account_owner(&self, account_info: &ReplicaAccountInfoV3<'_>) -> bool {
        Pubkey::try_from(account_info.owner)
            .is_ok_and(|owner_pubkey| self.owner_reject.contains(&owner_pubkey))
    }

    /// The update of an account write whose owner is not rejected. A write without a bank comes
    /// from the startup snapshot.
    pub fn capture_account(
        &self,
        account_info: &ReplicaAccountInfoV3<'_>,
        account_slot: Slot,
        bank_id: Option<BankId>,
    ) -> Message {
        let is_startup = bank_id.is_none();
        let message_account =
            MessageAccount::from_geyser(account_info, account_slot, is_startup, bank_id);
        Message::Account(Arc::new(message_account))
    }

    pub fn capture_slot_status(
        &self,
        status_slot: Slot,
        parent_slot: Option<Slot>,
        slot_status: &GeyserSlotStatus,
        bank_id: Option<BankId>,
    ) -> Message {
        let message_slot = MessageSlot::from_geyser(status_slot, parent_slot, slot_status, bank_id);
        Message::Slot(Arc::new(message_slot))
    }

    pub fn capture_transaction(
        &self,
        transaction_info: &ReplicaTransactionInfoV4<'_>,
        transaction_slot: Slot,
        bank_id: BankId,
    ) -> Message {
        let message_transaction =
            MessageTransaction::from_geyser(transaction_info, transaction_slot, bank_id);
        Message::Transaction(Arc::new(message_transaction))
    }

    pub fn capture_entry(&self, entry_info: &ReplicaEntryInfoV2<'_>, bank_id: BankId) -> Message {
        Message::Entry(Arc::new(MessageEntry::from_geyser(entry_info, bank_id)))
    }

    pub fn capture_block_meta(
        &self,
        block_info: &ReplicaBlockInfoV5<'_>,
        bank_id: BankId,
    ) -> Message {
        Message::BlockMeta(Arc::new(MessageBlockMeta::from_geyser(block_info, bank_id)))
    }

    pub fn capture_block_footer(
        &self,
        footer_info: &ReplicaBlockFooterInfo<'_>,
        bank_id: BankId,
    ) -> Message {
        Message::BlockFooter(Arc::new(MessageBlockFooter::from_geyser(
            footer_info,
            bank_id,
        )))
    }

    pub fn capture_entry_update_parent(
        &self,
        update_parent_info: &ReplicaEntryUpdateParentInfo<'_>,
    ) -> Message {
        let update_parent = MessageEntryUpdateParent::from_geyser(update_parent_info);
        Message::EntryUpdateParent(Arc::new(update_parent))
    }

    pub fn capture_deshred_transaction(
        &self,
        deshred_info: ReplicaDeshredTransactionInfoVersions<'_>,
        transaction_slot: Slot,
    ) -> Message {
        let deshred_transaction =
            MessageDeshredTransaction::from_geyser_versioned(deshred_info, transaction_slot);
        Message::DeshredTransaction(Arc::new(deshred_transaction))
    }

    pub fn capture_deshred_update_parent(
        &self,
        update_parent_info: &ReplicaDeshredUpdateParentInfo<'_>,
    ) -> Message {
        let update_parent = MessageDeshredUpdateParent::from_geyser(update_parent_info);
        Message::DeshredUpdateParent(Arc::new(update_parent))
    }
}

const CALLBACK_KINDS: usize = PluginCallback::ALL.len();

/// No callback runs on the thread: past every callback's index.
const NO_RUNNING_CALLBACK: usize = usize::MAX;

thread_local! {
    /// The index of the callback the thread runs; const and Drop-free, so no TLS destructor.
    static RUNNING_CALLBACK: Cell<usize> = const { Cell::new(NO_RUNNING_CALLBACK) };
}

/// Per callback kind: set by its first caught panic, the one that logs an error line.
static PANIC_LINE_WRITTEN: [AtomicBool; CALLBACK_KINDS] =
    [const { AtomicBool::new(false) }; CALLBACK_KINDS];

/// Per callback kind: set when the plugin's panic hook passed its first panic report on.
static PANIC_REPORT_WRITTEN: [AtomicBool; CALLBACK_KINDS] =
    [const { AtomicBool::new(false) }; CALLBACK_KINDS];

/// Runs one callback's body and catches its panic, so none unwinds into agave: the panic counts in
/// the kind's word, the kind's first one logs one error line, and the call returns `None`.
pub fn run_callback<CallbackResult>(
    plugin_callback: PluginCallback,
    callback_body: impl FnOnce() -> CallbackResult,
) -> Option<CallbackResult> {
    let outer_running_callback = RUNNING_CALLBACK.replace(plugin_callback as usize);
    let callback_outcome = match panic::catch_unwind(AssertUnwindSafe(callback_body)) {
        Ok(callback_result) => Some(callback_result),
        Err(panic_payload) => {
            DATA_PLANE_COUNTS.add(DataPlaneCount::CallbackPanic(plugin_callback), 1);
            // The line and the payload's drop still run under the kind, and their panics stay here.
            let panic_handling = panic::catch_unwind(AssertUnwindSafe(move || {
                if !PANIC_LINE_WRITTEN[plugin_callback as usize].swap(true, Ordering::Relaxed) {
                    log::error!(
                        "{} panicked and its records were dropped: {}",
                        plugin_callback.label_value(),
                        panic_payload_text(panic_payload.as_ref())
                    );
                }
                drop(panic_payload);
            }));
            if let Err(second_payload) = panic_handling {
                std::mem::forget(second_payload);
            }
            None
        }
    };
    RUNNING_CALLBACK.set(outer_running_callback);
    callback_outcome
}

/// Whether the plugin's panic hook passes this panic's report on: yes, unless the thread runs a
/// callback whose kind already passed one.
pub fn panic_report_passes() -> bool {
    let running_callback = RUNNING_CALLBACK.get();
    PANIC_REPORT_WRITTEN
        .get(running_callback)
        .is_none_or(|report_written| !report_written.swap(true, Ordering::Relaxed))
}

/// Clears every kind's first-panic flags, so a newly loaded instance logs and reports again.
pub fn reset_callback_panic_flags() {
    for first_panic_flag in PANIC_LINE_WRITTEN.iter().chain(&PANIC_REPORT_WRITTEN) {
        first_panic_flag.store(false, Ordering::Relaxed);
    }
}
