use std::{borrow::Borrow, sync::Arc};

use foldhash::{HashMap as FoldHashMap, HashMapExt, HashSet as FoldHashSet};
use solana_clock::{BankId, Slot};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use yellowstone_block_machine::state_machine::SlotCommitmentStatusUpdate;

use crate::{metrics, plugin::message::{Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageEntry, MessageSlot, MessageTransaction}};



pub struct Bank {
    original_messages: Vec<Message>,
    account_write_version_map: FoldHashMap<Pubkey, u64>,
    blockmeta: Option<Arc<MessageBlockMeta>>,
    transactions: Vec<Arc<MessageTransaction>>,
    accounts: Vec<Arc<MessageAccount>>,
    entries: Vec<Arc<MessageEntry>>,
    is_sealed: bool,
}

impl Default for Bank {
    fn default() -> Self {
        Self {
            original_messages: Vec::with_capacity(4096),
            account_write_version_map: FoldHashMap::with_capacity(4096),
            blockmeta: None,
            transactions: Vec::with_capacity(4096),
            accounts: Vec::with_capacity(4096),
            entries: Vec::with_capacity(64),
            is_sealed: false,
        }
    }
}

enum TrySealError {
    NotSealable,
    AlreadySealed,
}

impl Bank {
    pub fn add_event(&mut self, event: Message) {
        match event.borrow() {
            Message::Account(message_account) => {
                let write_version = message_account.account.write_version;
                self.account_write_version_map
                    .entry(message_account.account.pubkey)
                    .and_modify(|entry| {
                        if *entry < write_version {
                            *entry = write_version;
                        }
                    })
                    .or_insert(write_version);
                self.accounts.push(Arc::clone(message_account));
                // Handle account event
            }
            Message::Transaction(message_transaction) => {
                self.transactions.push(Arc::clone(message_transaction));
                // Handle transaction event
            }
            Message::Entry(message_entry) => {
                self.entries.push(Arc::clone(message_entry));
                // Handle entry event
            }
            _ => {
                // Handle other events if necessary
                return;
            }
        }

        self.original_messages.push(event);
    }

    fn try_seal(&mut self) -> Result<(), TrySealError> {
        if self.is_sealed {
            return Err(TrySealError::AlreadySealed);
        }
        let Some(blockmeta) = self.blockmeta.as_ref() else {
            return Err(TrySealError::NotSealable);
        };

        let expected_txn_count = blockmeta.executed_transaction_count as usize;
        if self.transactions.len() < expected_txn_count {
            return Err(TrySealError::NotSealable);
        }

        let expected_entry_count = blockmeta.entries_count as usize;
        if self.entries.len() < expected_entry_count {
            return Err(TrySealError::NotSealable);
        }
        self.is_sealed = true;
        Ok(())
    }

    pub fn seal(self) -> FrozenBlock {
        let block_meta = self.blockmeta.expect("should be sealable");
        let account_info_vec = self
            .accounts
            .into_iter()
            .filter_map(|account| {
                let write_version = self
                    .account_write_version_map
                    .get(&account.account.pubkey)?;
                if *write_version == account.account.write_version {
                    Some(account)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        // Yet another clone of all the messages, but that prevents from doing this later on anyway, while making iterator code easier to implement.
        let dedup_messages = self
            .original_messages
            .into_iter()
            .filter_map(|message| {
                if let Message::Account(account) = &message {
                    let write_version = self
                        .account_write_version_map
                        .get(&account.account.pubkey)?;
                    if *write_version == account.account.write_version {
                        Some(message)
                    } else {
                        None
                    }
                } else {
                    Some(message)
                }
            })
            .collect::<Vec<_>>();

        if self.transactions.len() != block_meta.executed_transaction_count as usize {
            metrics::incr_geyser_block_mismatch_transaction();
            log::warn!(
                "Block meta transaction count {} does not match actual transaction count {} for slot {}",
                block_meta.executed_transaction_count,
                self.transactions.len(),
                block_meta.slot
            );
        }

        let pre_computed_message_block = Arc::new(MessageBlock::new(
            Arc::clone(&block_meta),
            self.transactions,
            account_info_vec,
            self.entries,
        ));

        FrozenBlock {
            original_messages: Arc::new(dedup_messages),
            block_meta,
            pre_computed_message_block,
        }
    }
}

pub struct FrozenBlock {
    original_messages: Arc<Vec<Message>>,
    block_meta: Arc<MessageBlockMeta>,
    pre_computed_message_block: Arc<MessageBlock>,
}

impl FrozenBlock {
    pub fn get_message_block(&self) -> Arc<MessageBlock> {
        Arc::clone(&self.pre_computed_message_block)
    }

    pub fn messages(&self) -> Arc<Vec<Message>> {
        Arc::clone(&self.original_messages)
    }

    pub fn get_block_meta(&self) -> Arc<MessageBlockMeta> {
        Arc::clone(&self.block_meta)
    }
}


pub struct BlockReconstructionV2 {
    /// Banks being currently replayed by solana
    live_banks: FoldHashMap<BankId, Bank>,
    frozen_banks: FoldHashMap<BankId, Bank>,
    // In alpenglow, each slot may have up to 7 banks, so vec is fine here.
    slot_to_banks: FoldHashMap<Slot, Vec<BankId>>,
}

pub struct SlotProgression {
    commitment: Vec<SlotCommitmentStatusUpdate>,
    max_commitment: CommitmentLevel,
}


impl BlockReconstructionV2 {
    


    fn handle_block_data(&mut self, message: Message) {
        // Implement the logic to handle block data messages here
    }

    fn handle_block_meta(&mut self, message_block_meta: Arc<MessageBlockMeta>) {
        // Implement the logic to handle block meta messages here
    }

    fn giveup_bank(&mut self, bank_id: BankId, slot: Slot) {
    }

    fn commitment_update(
        &mut self, 
        bank_id: BankId, 
        slot: Slot, 
        parent_slot: Option<Slot>, 
        commitment: CommitmentLevel
    ) {

    }

    fn on_message_slot(&mut self, message_slot: &MessageSlot) -> Result<(), ()> {
        // Implement the logic to handle slot messages here
        let Some(bank_id) = message_slot.bank_id else {
            return Err(());
        };
        let slot = message_slot.slot;

        macro_rules! bail_on_nobank {
            ($bank_id:expr) => {
                if !self.live_banks.contains_key(&$bank_id) && !self.frozen_banks.contains_key(&$bank_id) {
                    return Err(());
                }
            };
        }

        match message_slot.status {
            crate::plugin::message::SlotStatus::Processed => {
                bail_on_nobank!(bank_id);
                todo!()
            },
            crate::plugin::message::SlotStatus::Confirmed => {
                bail_on_nobank!(bank_id);
                todo!()
            }
            crate::plugin::message::SlotStatus::Finalized => {
                bail_on_nobank!(bank_id);
                todo!()
            }
            crate::plugin::message::SlotStatus::FirstShredReceived => { return Ok(()) },
            crate::plugin::message::SlotStatus::Completed => { return Ok(()) },
            crate::plugin::message::SlotStatus::CreatedBank => {
                let _ = self.live_banks.entry(bank_id).or_default();
            },
            crate::plugin::message::SlotStatus::Dead => {
                self.giveup_bank(bank_id, slot);
            }
        }

        Ok(())
    }

     pub fn add(&mut self, message: Message) {
        match message {
            Message::Slot(message_slot) => {
                if self.on_message_slot(&message_slot).is_err() {
                    // Symmetric with handle_block_data which increments the same metric
                    // when is_slot_tracked returns false.
                    metrics::incr_geyser_untrack_slot_event_dropped();
                }
            }
            Message::Account(message_account) => {
                self.handle_block_data(Message::Account(message_account));
            }
            Message::Transaction(message_transaction) => {
                self.handle_block_data(Message::Transaction(message_transaction));
            }
            Message::Entry(message_entry) => {
                self.handle_block_data(Message::Entry(message_entry));
            }
            Message::BlockMeta(message_block_meta) => self.handle_block_meta(message_block_meta),
            _ => {
                // Handle other message types if necessary
            }
        }
        // while let Some(output) = self.state.pop_next_unprocess_blockstore_update() {
        //     self.on_blockmachine_output(output);
        // }

        // while self.state.pop_next_dlq().is_some() {
        //     // For now we just log the dlq events, but we may want to handle them in the future if necessary
        // }
    }
}