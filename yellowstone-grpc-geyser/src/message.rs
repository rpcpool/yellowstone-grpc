use {
    crate::filters::FilterAccountsDataSlice,
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus,
    },
    solana_sdk::{
        clock::UnixTimestamp, hash::HASH_BYTES, pubkey::Pubkey, signature::Signature,
        transaction::SanitizedTransaction,
    },
    solana_transaction_status::{Reward, TransactionStatusMeta},
    std::sync::Arc,
    yellowstone_grpc_proto::{
        convert_to,
        prelude::{
            CommitmentLevel, SubscribeUpdateAccountInfo, SubscribeUpdateEntry,
            SubscribeUpdateTransactionInfo,
        },
    },
};

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

impl MessageAccountInfo {
    pub fn as_proto(
        &self,
        accounts_data_slice: &[FilterAccountsDataSlice],
    ) -> SubscribeUpdateAccountInfo {
        let data = if accounts_data_slice.is_empty() {
            self.data.clone()
        } else {
            let mut data = Vec::with_capacity(accounts_data_slice.iter().map(|ds| ds.length).sum());
            for data_slice in accounts_data_slice {
                if self.data.len() >= data_slice.end {
                    data.extend_from_slice(&self.data[data_slice.start..data_slice.end]);
                }
            }
            data
        };
        SubscribeUpdateAccountInfo {
            pubkey: self.pubkey.as_ref().into(),
            lamports: self.lamports,
            owner: self.owner.as_ref().into(),
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            data,
            write_version: self.write_version,
            txn_signature: self.txn_signature.map(|s| s.as_ref().into()),
        }
    }
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
            status: match status {
                SlotStatus::Processed => CommitmentLevel::Processed,
                SlotStatus::Confirmed => CommitmentLevel::Confirmed,
                SlotStatus::Rooted => CommitmentLevel::Finalized,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageTransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: SanitizedTransaction,
    pub meta: TransactionStatusMeta,
    pub index: usize,
}

impl MessageTransactionInfo {
    pub fn as_proto(&self) -> SubscribeUpdateTransactionInfo {
        SubscribeUpdateTransactionInfo {
            signature: self.signature.as_ref().into(),
            is_vote: self.is_vote,
            transaction: Some(convert_to::create_transaction(&self.transaction)),
            meta: Some(convert_to::create_transaction_meta(&self.meta)),
            index: self.index as u64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageTransaction {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

impl<'a> From<(&'a ReplicaTransactionInfoV2<'a>, u64)> for MessageTransaction {
    fn from((transaction, slot): (&'a ReplicaTransactionInfoV2<'a>, u64)) -> Self {
        Self {
            transaction: Arc::new(MessageTransactionInfo {
                signature: *transaction.signature,
                is_vote: transaction.is_vote,
                transaction: transaction.transaction.clone(),
                meta: transaction.transaction_status_meta.clone(),
                index: transaction.index,
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
    pub hash: [u8; HASH_BYTES],
    pub executed_transaction_count: u64,
    pub starting_transaction_index: u64,
}

impl From<&ReplicaEntryInfoV2<'_>> for MessageEntry {
    fn from(entry: &ReplicaEntryInfoV2) -> Self {
        Self {
            slot: entry.slot,
            index: entry.index,
            num_hashes: entry.num_hashes,
            hash: entry.hash[0..32].try_into().expect("failed to create hash"),
            executed_transaction_count: entry.executed_transaction_count,
            starting_transaction_index: entry
                .starting_transaction_index
                .try_into()
                .expect("failed convert usize to u64"),
        }
    }
}

impl MessageEntry {
    pub fn as_proto(&self) -> SubscribeUpdateEntry {
        SubscribeUpdateEntry {
            slot: self.slot,
            index: self.index as u64,
            num_hashes: self.num_hashes,
            hash: self.hash.into(),
            executed_transaction_count: self.executed_transaction_count,
            starting_transaction_index: self.starting_transaction_index,
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
pub struct MessageBlockMeta {
    pub parent_slot: u64,
    pub slot: u64,
    pub parent_blockhash: String,
    pub blockhash: String,
    pub rewards: Vec<Reward>,
    pub num_partitions: Option<u64>,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
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
            rewards: blockinfo.rewards.rewards.clone(),
            num_partitions: blockinfo.rewards.num_partitions,
            block_time: blockinfo.block_time,
            block_height: blockinfo.block_height,
            executed_transaction_count: blockinfo.executed_transaction_count,
            entries_count: blockinfo.entry_count,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Message {
    Slot(MessageSlot),
    Account(MessageAccount),
    Transaction(MessageTransaction),
    Entry(Arc<MessageEntry>),
    Block(Arc<MessageBlock>),
    BlockMeta(Arc<MessageBlockMeta>),
}

impl Message {
    pub fn get_slot(&self) -> u64 {
        match self {
            Self::Slot(msg) => msg.slot,
            Self::Account(msg) => msg.slot,
            Self::Transaction(msg) => msg.slot,
            Self::Entry(msg) => msg.slot,
            Self::Block(msg) => msg.meta.slot,
            Self::BlockMeta(msg) => msg.slot,
        }
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Slot(_) => "Slot",
            Self::Account(_) => "Account",
            Self::Transaction(_) => "Transaction",
            Self::Entry(_) => "Entry",
            Self::Block(_) => "Block",
            Self::BlockMeta(_) => "BlockMeta",
        }
    }
}
