use {
    crate::scylladb::types::{BlockchainEventType, ConsumerId, ProducerId, ShardOffset, Slot},
    serde::{Deserialize, Serialize}, std::ops::RangeInclusive,
};

pub type OldShardOffset = ShardOffset;

///
/// Initial position in the log when creating a new consumer.
///  
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub enum SeekLocation {
    Earliest,
    
    #[default]
    Latest,

    SlotApprox(RangeInclusive<Slot>),
}

pub struct ConsumerInfo {
    pub consumer_id: ConsumerId,
    pub producer_id: ProducerId,
    //pub initital_shard_offsets: Vec<ConsumerShardOffset>,
    pub subscribed_blockchain_event_types: Vec<BlockchainEventType>,
}
