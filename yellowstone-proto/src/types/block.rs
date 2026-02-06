use std::time::SystemTime;

use solana_hash::Hash;

use crate::{AccountInfo, Entry, TransactionInfo, types::common::Reward};

#[derive(Debug, Clone, PartialEq)]
pub struct Rewards {
    pub rewards: Vec<Reward>,
    pub num_partitions: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockMeta {
    pub slot: u64,
    pub blockhash: Hash,
    pub rewards: Option<Rewards>,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub parent_slot: u64,
    pub parent_blockhash: Hash,
    pub executed_transaction_count: u64,
    pub entries_count: u64,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Block {
    pub meta: BlockMeta,
    pub transactions: Vec<TransactionInfo>,
    pub updated_account_count: u64,
    pub accounts: Vec<AccountInfo>,
    pub entries: Vec<Entry>,
    pub created_at: SystemTime,
}