use crate::scylladb::types::{BlockchainEventType, ConsumerId, ProducerId, ShardOffset};

pub type OldShardOffset = ShardOffset;

///
/// Initial position in the log when creating a new consumer.
///  
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialOffsetPolicy {
    Earliest,
    #[default]
    Latest,
    SlotApprox(i64),
}

pub struct ConsumerInfo {
    pub consumer_id: ConsumerId,
    pub producer_id: ProducerId,
    //pub initital_shard_offsets: Vec<ConsumerShardOffset>,
    pub subscribed_blockchain_event_types: Vec<BlockchainEventType>,
}
