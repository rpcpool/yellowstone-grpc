use crate::{
    geyser::{
        subscribe_update::UpdateOneof, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
        SubscribeUpdateBlock, SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateSlot,
        SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        SubscribeUpdateTransactionStatus,
    },
    plugin::{
        filter::FilterAccountsDataSlice,
        message::{
            MessageAccount, MessageAccountInfo, MessageBlock, MessageBlockMeta, MessageEntry,
            MessageSlot, MessageTransaction, MessageTransactionInfo,
        },
    },
};

#[derive(Debug, Clone)]
pub enum FilteredMessage<'a> {
    Slot(&'a MessageSlot),
    Account(&'a MessageAccount),
    Transaction(&'a MessageTransaction),
    TransactionStatus(&'a MessageTransaction),
    Entry(&'a MessageEntry),
    Block(MessageBlock),
    BlockMeta(&'a MessageBlockMeta),
}

impl<'a> FilteredMessage<'a> {
    fn as_proto_account(
        message: &MessageAccountInfo,
        data_slice: &FilterAccountsDataSlice,
    ) -> SubscribeUpdateAccountInfo {
        let data_slice = data_slice.as_ref();
        let data = if data_slice.is_empty() {
            message.data.clone()
        } else {
            let mut data = Vec::with_capacity(data_slice.iter().map(|ds| ds.end - ds.start).sum());
            for data_slice in data_slice {
                if message.data.len() >= data_slice.end {
                    data.extend_from_slice(&message.data[data_slice.start..data_slice.end]);
                }
            }
            data
        };
        SubscribeUpdateAccountInfo {
            pubkey: message.pubkey.as_ref().into(),
            lamports: message.lamports,
            owner: message.owner.as_ref().into(),
            executable: message.executable,
            rent_epoch: message.rent_epoch,
            data,
            write_version: message.write_version,
            txn_signature: message.txn_signature.map(|s| s.as_ref().into()),
        }
    }

    fn as_proto_transaction(message: &MessageTransactionInfo) -> SubscribeUpdateTransactionInfo {
        SubscribeUpdateTransactionInfo {
            signature: message.signature.as_ref().into(),
            is_vote: message.is_vote,
            transaction: Some(message.transaction.clone()),
            meta: Some(message.meta.clone()),
            index: message.index as u64,
        }
    }

    fn as_proto_entry(message: &MessageEntry) -> SubscribeUpdateEntry {
        SubscribeUpdateEntry {
            slot: message.slot,
            index: message.index as u64,
            num_hashes: message.num_hashes,
            hash: message.hash.to_bytes().to_vec(),
            executed_transaction_count: message.executed_transaction_count,
            starting_transaction_index: message.starting_transaction_index,
        }
    }

    pub fn as_proto(&self, accounts_data_slice: &FilterAccountsDataSlice) -> UpdateOneof {
        match self {
            Self::Slot(message) => UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: message.slot,
                parent: message.parent,
                status: message.status as i32,
            }),
            Self::Account(message) => UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(Self::as_proto_account(
                    message.account.as_ref(),
                    accounts_data_slice,
                )),
                slot: message.slot,
                is_startup: message.is_startup,
            }),
            Self::Transaction(message) => UpdateOneof::Transaction(SubscribeUpdateTransaction {
                transaction: Some(Self::as_proto_transaction(message.transaction.as_ref())),
                slot: message.slot,
            }),
            Self::TransactionStatus(message) => {
                UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                    slot: message.slot,
                    signature: message.transaction.signature.as_ref().into(),
                    is_vote: message.transaction.is_vote,
                    index: message.transaction.index as u64,
                    err: message.transaction.meta.err.clone(),
                })
            }
            Self::Entry(message) => UpdateOneof::Entry(Self::as_proto_entry(message)),
            Self::Block(message) => UpdateOneof::Block(SubscribeUpdateBlock {
                slot: message.meta.slot,
                blockhash: message.meta.blockhash.clone(),
                rewards: Some(message.meta.rewards.clone()),
                block_time: message.meta.block_time,
                block_height: message.meta.block_height,
                parent_slot: message.meta.parent_slot,
                parent_blockhash: message.meta.parent_blockhash.clone(),
                executed_transaction_count: message.meta.executed_transaction_count,
                transactions: message
                    .transactions
                    .iter()
                    .map(|tx| Self::as_proto_transaction(tx.as_ref()))
                    .collect(),
                updated_account_count: message.updated_account_count,
                accounts: message
                    .accounts
                    .iter()
                    .map(|acc| Self::as_proto_account(acc.as_ref(), accounts_data_slice))
                    .collect(),
                entries_count: message.meta.entries_count,
                entries: message
                    .entries
                    .iter()
                    .map(|entry| Self::as_proto_entry(entry.as_ref()))
                    .collect(),
            }),
            Self::BlockMeta(message) => UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: message.slot,
                blockhash: message.blockhash.clone(),
                rewards: Some(message.rewards.clone()),
                block_time: message.block_time,
                block_height: message.block_height,
                parent_slot: message.parent_slot,
                parent_blockhash: message.parent_blockhash.clone(),
                executed_transaction_count: message.executed_transaction_count,
                entries_count: message.entries_count,
            }),
        }
    }
}
