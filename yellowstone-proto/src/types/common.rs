use solana_pubkey::Pubkey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommitmentLevel {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SlotStatus {
    Processed,
    Confirmed,
    Finalized,
    FirstShredReceived,
    Completed,
    CreatedBank,
    Dead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RewardType {
    Unspecified,
    Fee,
    Rent,
    Staking,
    Voting,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Reward {
    pub pubkey: Pubkey,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: RewardType,
    pub commission: Option<u8>,
}