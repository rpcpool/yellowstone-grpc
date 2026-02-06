use bytes::Bytes;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use crate::types::common::Reward;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageAddressTableLookup {
    pub account_key: Pubkey,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionError {
    pub err: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InnerInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Bytes,
    pub stack_height: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReturnData {
    pub program_id: Pubkey,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UiTokenAmount {
    pub ui_amount: f64,
    pub decimals: u8,
    pub amount: String,
    pub ui_amount_string: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TokenBalance {
    pub account_index: u8,
    pub mint: Pubkey,
    pub ui_token_amount: UiTokenAmount,
    pub owner: Pubkey,
    pub program_id: Pubkey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InnerInstructions {
    pub index: u8,
    pub instructions: Vec<InnerInstruction>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub header: MessageHeader,
    pub account_keys: Vec<Pubkey>,
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<CompiledInstruction>,
    pub versioned: bool,
    pub address_table_lookups: Vec<MessageAddressTableLookup>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    pub signatures: Vec<Signature>,
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransactionStatusMeta {
    pub err: Option<TransactionError>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Vec<TokenBalance>,
    pub post_token_balances: Vec<TokenBalance>,
    pub rewards: Vec<Reward>,
    pub loaded_writable_addresses: Vec<Pubkey>,
    pub loaded_readonly_addresses: Vec<Pubkey>,
    pub return_data: Option<ReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: Transaction,
    pub meta: TransactionStatusMeta,
    pub index: usize,
}