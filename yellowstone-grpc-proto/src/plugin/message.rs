use {
    crate::{
        convert_to, geyser::CommitmentLevel as CommitmentLevelProto,
        solana::storage::confirmed_block,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus,
    },
    solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature},
    std::{collections::HashSet, sync::Arc},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitmentLevel {
    Processed,
    Confirmed,
    Finalized,
}

impl From<SlotStatus> for CommitmentLevel {
    fn from(status: SlotStatus) -> Self {
        match status {
            SlotStatus::Processed => Self::Processed,
            SlotStatus::Confirmed => Self::Confirmed,
            SlotStatus::Rooted => Self::Finalized,
        }
    }
}

impl From<CommitmentLevel> for CommitmentLevelProto {
    fn from(commitment: CommitmentLevel) -> Self {
        match commitment {
            CommitmentLevel::Processed => Self::Processed,
            CommitmentLevel::Confirmed => Self::Confirmed,
            CommitmentLevel::Finalized => Self::Finalized,
        }
    }
}

impl From<CommitmentLevelProto> for CommitmentLevel {
    fn from(status: CommitmentLevelProto) -> Self {
        match status {
            CommitmentLevelProto::Processed => Self::Processed,
            CommitmentLevelProto::Confirmed => Self::Confirmed,
            CommitmentLevelProto::Finalized => Self::Finalized,
        }
    }
}

impl PartialEq<CommitmentLevelProto> for CommitmentLevel {
    fn eq(&self, other: &CommitmentLevelProto) -> bool {
        matches!(
            (self, other),
            (Self::Processed, CommitmentLevelProto::Processed)
                | (Self::Confirmed, CommitmentLevelProto::Confirmed)
                | (Self::Finalized, CommitmentLevelProto::Finalized)
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MessageSlot {
    pub slot: u64,
    pub parent: Option<u64>,
    pub status: CommitmentLevel,
}

impl From<(u64, Option<u64>, SlotStatus)> for MessageSlot {
    fn from((slot, parent, status): (u64, Option<u64>, SlotStatus)) -> Self {
        Self {
            slot,
            parent,
            status: status.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageAccountInfo {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub write_version: u64,
    pub txn_signature: Option<Signature>,
}

#[derive(Debug, Clone)]
pub struct MessageAccount {
    pub account: Arc<MessageAccountInfo>,
    pub slot: u64,
    pub is_startup: bool,
}

impl<'a> From<(&'a ReplicaAccountInfoV3<'a>, u64, bool)> for MessageAccount {
    fn from((account, slot, is_startup): (&'a ReplicaAccountInfoV3<'a>, u64, bool)) -> Self {
        Self {
            account: Arc::new(MessageAccountInfo {
                pubkey: Pubkey::try_from(account.pubkey).expect("valid Pubkey"),
                lamports: account.lamports,
                owner: Pubkey::try_from(account.owner).expect("valid Pubkey"),
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: account.data.into(),
                write_version: account.write_version,
                txn_signature: account.txn.map(|txn| *txn.signature()),
            }),
            slot,
            is_startup,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageTransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: confirmed_block::Transaction,
    pub meta: confirmed_block::TransactionStatusMeta,
    pub index: usize,
    pub account_keys: HashSet<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct MessageTransaction {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

impl<'a> From<(&'a ReplicaTransactionInfoV2<'a>, u64)> for MessageTransaction {
    fn from((transaction, slot): (&'a ReplicaTransactionInfoV2<'a>, u64)) -> Self {
        let account_keys = transaction
            .transaction
            .message()
            .account_keys()
            .iter()
            .copied()
            .collect();

        Self {
            transaction: Arc::new(MessageTransactionInfo {
                signature: *transaction.signature,
                is_vote: transaction.is_vote,
                transaction: convert_to::create_transaction(transaction.transaction),
                meta: convert_to::create_transaction_meta(transaction.transaction_status_meta),
                index: transaction.index,
                account_keys,
            }),
            slot,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MessageEntry {
    pub slot: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Hash,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: u64,
}

impl From<&ReplicaEntryInfoV2<'_>> for MessageEntry {
    fn from(entry: &ReplicaEntryInfoV2) -> Self {
        Self {
            slot: entry.slot,
            index: entry.index,
            num_hashes: entry.num_hashes,
            hash: Hash::new(entry.hash),
            executed_transaction_count: entry.executed_transaction_count,
            starting_transaction_index: entry
                .starting_transaction_index
                .try_into()
                .expect("failed convert usize to u64"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageBlockMeta {
    pub parent_slot: u64,
    pub slot: u64,
    pub parent_blockhash: String,
    pub blockhash: String,
    pub rewards: confirmed_block::Rewards,
    pub block_time: Option<confirmed_block::UnixTimestamp>,
    pub block_height: Option<confirmed_block::BlockHeight>,
    pub executed_transaction_count: u64,
    pub entries_count: u64,
}

impl<'a> From<&'a ReplicaBlockInfoV4<'a>> for MessageBlockMeta {
    fn from(blockinfo: &'a ReplicaBlockInfoV4<'a>) -> Self {
        Self {
            parent_slot: blockinfo.parent_slot,
            slot: blockinfo.slot,
            parent_blockhash: blockinfo.parent_blockhash.to_string(),
            blockhash: blockinfo.blockhash.to_string(),
            rewards: convert_to::create_rewards_obj(
                &blockinfo.rewards.rewards,
                blockinfo.rewards.num_partitions,
            ),
            block_time: blockinfo.block_time.map(convert_to::create_timestamp),
            block_height: blockinfo.block_height.map(convert_to::create_block_height),
            executed_transaction_count: blockinfo.executed_transaction_count,
            entries_count: blockinfo.entry_count,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageBlock {
    pub meta: Arc<MessageBlockMeta>,
    pub transactions: Vec<Arc<MessageTransactionInfo>>,
    pub updated_account_count: u64,
    pub accounts: Vec<Arc<MessageAccountInfo>>,
    pub entries: Vec<Arc<MessageEntry>>,
}

impl
    From<(
        Arc<MessageBlockMeta>,
        Vec<Arc<MessageTransactionInfo>>,
        Vec<Arc<MessageAccountInfo>>,
        Vec<Arc<MessageEntry>>,
    )> for MessageBlock
{
    fn from(
        (meta, transactions, accounts, entries): (
            Arc<MessageBlockMeta>,
            Vec<Arc<MessageTransactionInfo>>,
            Vec<Arc<MessageAccountInfo>>,
            Vec<Arc<MessageEntry>>,
        ),
    ) -> Self {
        Self {
            meta,
            transactions,
            updated_account_count: accounts.len() as u64,
            accounts,
            entries,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Message {
    Slot(MessageSlot),
    Account(MessageAccount),
    Transaction(MessageTransaction),
    Entry(Arc<MessageEntry>),
    BlockMeta(Arc<MessageBlockMeta>),
    Block(Arc<MessageBlock>),
}

impl Message {
    pub fn get_slot(&self) -> u64 {
        match self {
            Self::Slot(msg) => msg.slot,
            Self::Account(msg) => msg.slot,
            Self::Transaction(msg) => msg.slot,
            Self::Entry(msg) => msg.slot,
            Self::BlockMeta(msg) => msg.slot,
            Self::Block(msg) => msg.meta.slot,
        }
    }
}
