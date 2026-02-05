use std::collections::HashSet;

use bytes::Bytes;
use solana_pubkey::Pubkey;
use solana_signature::Signature;

#[derive(Debug, Clone, PartialEq)]
pub struct TransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: Transaction,
    pub meta: TransactionStatusMeta,
    pub index: usize,
    pub account_keys: HashSet<Pubkey>,
    pub pre_encoded: Option<Bytes>,
}