use std::sync::Arc;

use scylla::Session;

use crate::scylladb::types::{BlockchainEventType, ConsumerGroupId, ConsumerId, ExecutionId, ProducerId, ShardOffsetMap};

use super::{consumer_group_store::ConsumerGroupStore, producer_queries::ProducerQueries, shard_iterator::ShardFilter};




#[derive(Clone)]
pub struct ConsumerContext {
    pub consumer_group_id: ConsumerGroupId,
    pub consumer_id: ConsumerId,
    pub shard_offset_map: ShardOffsetMap,
    pub producer_id: ProducerId,
    pub execution_id: ExecutionId,
    pub subscribed_event_types: Vec<BlockchainEventType>,
    pub acc_update_filter: Option<ShardFilter>,
    pub new_tx_filter: Option<ShardFilter>,
    session: Arc<Session>,
    etcd: etcd_client::Client,
    pub consumer_group_store: ConsumerGroupStore,
    pub producer_queries: ProducerQueries,
}

impl ConsumerContext {
    pub fn session(&self) -> Arc<Session> {
        Arc::clone(&self.session)
    }

    pub fn etcd(&self) -> etcd_client::Client {
        self.etcd.clone()
    }
}