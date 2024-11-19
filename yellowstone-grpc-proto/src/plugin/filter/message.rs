use {
    crate::{
        geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
            SubscribeUpdateAccountInfo, SubscribeUpdateBlock, SubscribeUpdateBlockMeta,
            SubscribeUpdateEntry, SubscribeUpdatePing, SubscribeUpdatePong, SubscribeUpdateSlot,
            SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            SubscribeUpdateTransactionStatus,
        },
        plugin::{
            filter::{name::FilterName, FilterAccountsDataSlice},
            message::{
                MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageEntry, MessageSlot,
                MessageTransaction, MessageTransactionInfo,
            },
        },
    },
    smallvec::SmallVec,
    std::sync::Arc,
};

pub type FilteredUpdates = SmallVec<[FilteredUpdate; 2]>;

#[derive(Debug, Clone)]
pub struct FilteredUpdate {
    pub filters: FilteredUpdateFilters,
    pub message: FilteredUpdateOneof,
}

impl FilteredUpdate {
    pub fn new(filters: FilteredUpdateFilters, message: FilteredUpdateOneof) -> Self {
        Self { filters, message }
    }

    fn as_subscribe_update_account(
        message: &MessageAccountInfo,
        data_slice: &FilterAccountsDataSlice,
    ) -> SubscribeUpdateAccountInfo {
        SubscribeUpdateAccountInfo {
            pubkey: message.pubkey.as_ref().into(),
            lamports: message.lamports,
            owner: message.owner.as_ref().into(),
            executable: message.executable,
            rent_epoch: message.rent_epoch,
            data: data_slice.apply(&message.data),
            write_version: message.write_version,
            txn_signature: message.txn_signature.map(|s| s.as_ref().into()),
        }
    }

    fn as_subscribe_update_transaction(
        message: &MessageTransactionInfo,
    ) -> SubscribeUpdateTransactionInfo {
        SubscribeUpdateTransactionInfo {
            signature: message.signature.as_ref().into(),
            is_vote: message.is_vote,
            transaction: Some(message.transaction.clone()),
            meta: Some(message.meta.clone()),
            index: message.index as u64,
        }
    }

    fn as_subscribe_update_entry(message: &MessageEntry) -> SubscribeUpdateEntry {
        SubscribeUpdateEntry {
            slot: message.slot,
            index: message.index as u64,
            num_hashes: message.num_hashes,
            hash: message.hash.to_bytes().to_vec(),
            executed_transaction_count: message.executed_transaction_count,
            starting_transaction_index: message.starting_transaction_index,
        }
    }

    pub fn as_subscribe_update(&self) -> SubscribeUpdate {
        let message = match &self.message {
            FilteredUpdateOneof::Account(msg) => UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(Self::as_subscribe_update_account(
                    msg.account.as_ref(),
                    &msg.data_slice,
                )),
                slot: msg.slot,
                is_startup: msg.is_startup,
            }),
            FilteredUpdateOneof::Slot(msg) => UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: msg.slot,
                parent: msg.parent,
                status: msg.status as i32,
            }),
            FilteredUpdateOneof::Transaction(msg) => {
                UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    transaction: Some(Self::as_subscribe_update_transaction(
                        msg.transaction.as_ref(),
                    )),
                    slot: msg.slot,
                })
            }
            FilteredUpdateOneof::TransactionStatus(msg) => {
                UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                    slot: msg.slot,
                    signature: msg.transaction.signature.as_ref().into(),
                    is_vote: msg.transaction.is_vote,
                    index: msg.transaction.index as u64,
                    err: msg.transaction.meta.err.clone(),
                })
            }
            FilteredUpdateOneof::Block(msg) => UpdateOneof::Block(SubscribeUpdateBlock {
                slot: msg.meta.slot,
                blockhash: msg.meta.blockhash.clone(),
                rewards: Some(msg.meta.rewards.clone()),
                block_time: msg.meta.block_time,
                block_height: msg.meta.block_height,
                parent_slot: msg.meta.parent_slot,
                parent_blockhash: msg.meta.parent_blockhash.clone(),
                executed_transaction_count: msg.meta.executed_transaction_count,
                transactions: msg
                    .transactions
                    .iter()
                    .map(|tx| Self::as_subscribe_update_transaction(tx.as_ref()))
                    .collect(),
                updated_account_count: msg.updated_account_count,
                accounts: msg
                    .accounts
                    .iter()
                    .map(|acc| {
                        Self::as_subscribe_update_account(acc.as_ref(), &msg.accounts_data_slice)
                    })
                    .collect(),
                entries_count: msg.meta.entries_count,
                entries: msg
                    .entries
                    .iter()
                    .map(|entry| Self::as_subscribe_update_entry(entry.as_ref()))
                    .collect(),
            }),
            FilteredUpdateOneof::Ping => UpdateOneof::Ping(SubscribeUpdatePing {}),
            FilteredUpdateOneof::Pong(msg) => UpdateOneof::Pong(*msg),
            FilteredUpdateOneof::BlockMeta(msg) => {
                UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                    slot: msg.slot,
                    blockhash: msg.blockhash.clone(),
                    rewards: Some(msg.rewards.clone()),
                    block_time: msg.block_time,
                    block_height: msg.block_height,
                    parent_slot: msg.parent_slot,
                    parent_blockhash: msg.parent_blockhash.clone(),
                    executed_transaction_count: msg.executed_transaction_count,
                    entries_count: msg.entries_count,
                })
            }
            FilteredUpdateOneof::Entry(msg) => {
                UpdateOneof::Entry(Self::as_subscribe_update_entry(msg))
            }
        };

        SubscribeUpdate {
            filters: self
                .filters
                .iter()
                .map(|name| name.as_ref().to_string())
                .collect(),
            update_oneof: Some(message),
        }
    }
}

pub type FilteredUpdateFilters = SmallVec<[FilterName; 4]>;

#[derive(Debug, Clone)]
pub enum FilteredUpdateOneof {
    Account(FilteredUpdateAccount),                     // 2
    Slot(MessageSlot),                                  // 3
    Transaction(FilteredUpdateTransaction),             // 4
    TransactionStatus(FilteredUpdateTransactionStatus), // 10
    Block(Box<FilteredUpdateBlock>),                    // 5
    Ping,                                               // 6
    Pong(SubscribeUpdatePong),                          // 9
    BlockMeta(Arc<MessageBlockMeta>),                   // 7
    Entry(Arc<MessageEntry>),                           // 8
}

impl FilteredUpdateOneof {
    pub fn account(message: &MessageAccount, data_slice: FilterAccountsDataSlice) -> Self {
        Self::Account(FilteredUpdateAccount {
            slot: message.slot,
            account: Arc::clone(&message.account),
            is_startup: message.is_startup,
            data_slice,
        })
    }

    pub const fn slot(message: MessageSlot) -> Self {
        Self::Slot(message)
    }

    pub fn transaction(message: &MessageTransaction) -> Self {
        Self::Transaction(FilteredUpdateTransaction {
            transaction: Arc::clone(&message.transaction),
            slot: message.slot,
        })
    }

    pub fn transaction_status(message: &MessageTransaction) -> Self {
        Self::TransactionStatus(FilteredUpdateTransactionStatus {
            transaction: Arc::clone(&message.transaction),
            slot: message.slot,
        })
    }

    pub const fn block(message: Box<FilteredUpdateBlock>) -> Self {
        Self::Block(message)
    }

    pub const fn pong(id: i32) -> Self {
        Self::Pong(SubscribeUpdatePong { id })
    }

    pub const fn block_meta(message: Arc<MessageBlockMeta>) -> Self {
        Self::BlockMeta(message)
    }

    pub const fn entry(message: Arc<MessageEntry>) -> Self {
        Self::Entry(message)
    }
}

#[derive(Debug, Clone)]
pub struct FilteredUpdateAccount {
    pub account: Arc<MessageAccountInfo>,
    pub slot: u64,
    pub is_startup: bool,
    pub data_slice: FilterAccountsDataSlice,
}

#[derive(Debug, Clone)]
pub struct FilteredUpdateTransaction {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct FilteredUpdateTransactionStatus {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct FilteredUpdateBlock {
    pub meta: Arc<MessageBlockMeta>,
    pub transactions: Vec<Arc<MessageTransactionInfo>>,
    pub updated_account_count: u64,
    pub accounts: Vec<Arc<MessageAccountInfo>>,
    pub accounts_data_slice: FilterAccountsDataSlice,
    pub entries: Vec<Arc<MessageEntry>>,
}
