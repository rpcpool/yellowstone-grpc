use bytes::Bytes;
use solana_pubkey::Pubkey;
use solana_signature::Signature;

pub struct AccountInfo {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Bytes,
    pub write_version: u64,
    pub txn_signature: Option<Signature>,
}