use crate::scylladb::types::{BlockchainEventType, ProducerId, ShardId, ShardOffset};

pub type OldShardOffset = ShardOffset;

pub type ConsumerId = String;

///
/// Initial position in the log when creating a new consumer.
///  
#[derive(Default, Debug, Clone, Copy)]
pub enum InitialOffsetPolicy {
    Earliest,
    #[default]
    Latest,
    SlotApprox(i64),
}

pub struct ConsumerInfo {
    pub consumer_id: ConsumerId,
    pub producer_id: ProducerId,
    pub initital_shard_offsets: Vec<(ShardId, BlockchainEventType, ShardOffset)>,
    pub subscribed_blockchain_event_types: Vec<BlockchainEventType>,
}
