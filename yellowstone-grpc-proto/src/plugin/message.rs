use {
    crate::{
        convert_to,
        geyser::{CommitmentLevel as CommitmentLevelProto, SubscribeUpdateBlockMeta},
        solana::storage::confirmed_block,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus,
    },
    solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey, signature::Signature},
    std::{
        collections::HashSet,
        ops::{Deref, DerefMut},
        sync::Arc,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

#[derive(Debug, Clone, Copy)]
pub struct MessageSlot {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub status: CommitmentLevel,
}

impl MessageSlot {
    pub fn from_geyser(slot: Slot, parent: Option<Slot>, status: SlotStatus) -> Self {
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

impl MessageAccountInfo {
    pub fn from_geyser(info: &ReplicaAccountInfoV3<'_>) -> Self {
        Self {
            pubkey: Pubkey::try_from(info.pubkey).expect("valid Pubkey"),
            lamports: info.lamports,
            owner: Pubkey::try_from(info.owner).expect("valid Pubkey"),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            data: info.data.into(),
            write_version: info.write_version,
            txn_signature: info.txn.map(|txn| *txn.signature()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageAccount {
    pub account: Arc<MessageAccountInfo>,
    pub slot: Slot,
    pub is_startup: bool,
}

impl MessageAccount {
    pub fn from_geyser(info: &ReplicaAccountInfoV3<'_>, slot: Slot, is_startup: bool) -> Self {
        Self {
            account: Arc::new(MessageAccountInfo::from_geyser(info)),
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

impl MessageTransactionInfo {
    pub fn from_geyser(info: &ReplicaTransactionInfoV2<'_>) -> Self {
        let account_keys = info
            .transaction
            .message()
            .account_keys()
            .iter()
            .copied()
            .collect();

        Self {
            signature: *info.signature,
            is_vote: info.is_vote,
            transaction: convert_to::create_transaction(info.transaction),
            meta: convert_to::create_transaction_meta(info.transaction_status_meta),
            index: info.index,
            account_keys,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageTransaction {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

impl MessageTransaction {
    pub fn from_geyser(info: &ReplicaTransactionInfoV2<'_>, slot: Slot) -> Self {
        Self {
            transaction: Arc::new(MessageTransactionInfo::from_geyser(info)),
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

impl MessageEntry {
    pub fn from_geyser(info: &ReplicaEntryInfoV2) -> Self {
        Self {
            slot: info.slot,
            index: info.index,
            num_hashes: info.num_hashes,
            hash: Hash::new(info.hash),
            executed_transaction_count: info.executed_transaction_count,
            starting_transaction_index: info
                .starting_transaction_index
                .try_into()
                .expect("failed convert usize to u64"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageBlockMeta(pub SubscribeUpdateBlockMeta);

impl Deref for MessageBlockMeta {
    type Target = SubscribeUpdateBlockMeta;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MessageBlockMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl MessageBlockMeta {
    pub fn from_geyser(info: &ReplicaBlockInfoV4<'_>) -> Self {
        Self(SubscribeUpdateBlockMeta {
            parent_slot: info.parent_slot,
            slot: info.slot,
            parent_blockhash: info.parent_blockhash.to_string(),
            blockhash: info.blockhash.to_string(),
            rewards: Some(convert_to::create_rewards_obj(
                &info.rewards.rewards,
                info.rewards.num_partitions,
            )),
            block_time: info.block_time.map(convert_to::create_timestamp),
            block_height: info.block_height.map(convert_to::create_block_height),
            executed_transaction_count: info.executed_transaction_count,
            entries_count: info.entry_count,
        })
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

impl MessageBlock {
    pub fn new(
        meta: Arc<MessageBlockMeta>,
        transactions: Vec<Arc<MessageTransactionInfo>>,
        accounts: Vec<Arc<MessageAccountInfo>>,
        entries: Vec<Arc<MessageEntry>>,
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
