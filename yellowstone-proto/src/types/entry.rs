use std::time::SystemTime;
use solana_hash::Hash;

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub slot: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Hash,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: u64,
    pub created_at: SystemTime,
}