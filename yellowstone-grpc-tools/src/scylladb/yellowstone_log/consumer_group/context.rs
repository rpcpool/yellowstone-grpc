use {
    super::{
        consumer_group_store::ScyllaConsumerGroupStore, lock::FencingTokenGenerator,
        producer::ScyllaProducerStore, shard_iterator::ShardFilter,
    },
    crate::scylladb::{
        etcd_utils::Revision,
        types::{BlockchainEventType, ConsumerGroupId, ConsumerId, ProducerId, ShardOffsetMap},
    },
    scylla::Session,
    std::sync::Arc,
};

#[derive(Clone)]
pub struct ConsumerContext {
    pub consumer_group_id: ConsumerGroupId,
    pub consumer_id: ConsumerId,
    pub producer_id: ProducerId,
    pub subscribed_event_types: Vec<BlockchainEventType>,
    pub session: Arc<Session>,
    pub etcd: etcd_client::Client,
    pub consumer_group_store: ScyllaConsumerGroupStore,
    pub fencing_token_generator: FencingTokenGenerator,
}

impl ConsumerContext {
    pub fn session(&self) -> Arc<Session> {
        Arc::clone(&self.session)
    }

    pub fn etcd(&self) -> etcd_client::Client {
        self.etcd.clone()
    }

    pub async fn get_shard_offset_map(
        &self,
        blockchain_event_type: BlockchainEventType,
    ) -> anyhow::Result<(Revision, ShardOffsetMap)> {
        self.consumer_group_store
            .get_shard_offset_map(
                &self.consumer_group_id,
                &self.consumer_id,
                &self.producer_id,
                blockchain_event_type,
            )
            .await
    }

    pub async fn generate_fencing_token(&self) -> anyhow::Result<Revision> {
        self.fencing_token_generator.generate().await
    }
}
