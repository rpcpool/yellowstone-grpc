use crate::scylladb::types::{BlockchainEventType, ConsumerId, ProducerId, ShardOffset, Slot};

pub type OldShardOffset = ShardOffset;

///
/// Initial position in the log when creating a new consumer.
///  
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialOffset {
    Earliest,
    #[default]
    Latest,
    SlotApprox {
        desired_slot: Slot,
        min_slot: Slot,
    },
}

pub struct ConsumerInfo {
    pub consumer_id: ConsumerId,
    pub producer_id: ProducerId,
    //pub initital_shard_offsets: Vec<ConsumerShardOffset>,
    pub subscribed_blockchain_event_types: Vec<BlockchainEventType>,
}
