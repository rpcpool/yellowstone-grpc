use core::fmt;

use futures::future;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tonic::async_trait;

use crate::scylladb::{types::{ConsumerGroupId, ExecutionId, ProducerId, ShardOffsetMap}, yellowstone_log::common::SeekLocation};

use super::{consumer_group_store::ScyllaConsumerGroupStore, producer_queries::ProducerQueries};


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ComputingNextProducerState {
    pub consumer_group_id: ConsumerGroupId,
    pub revision: i64,
}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProducerProposalState {
    pub consumer_group_id: ConsumerGroupId,
    pub revision: i64,
    pub producer_id: ProducerId,
    pub execution_id: ExecutionId,
    pub new_shard_offsets: ShardOffsetMap,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TranslationState {
    ComputingNextProducer(ComputingNextProducerState),
    ProducerProposal(ProducerProposalState),
    Done
}


#[async_trait]
pub trait TimelineTranslator {
    fn begin_translation(
        &self, 
        consumer_group_id: ConsumerGroupId,
        revision: i64,
    ) -> TranslationState {
        TranslationState::ComputingNextProducer(
            ComputingNextProducerState {
                consumer_group_id,
                revision,
            }
        )
    }


    async fn next(&self, state: TranslationState) -> anyhow::Result<TranslationState> {
        match state {
            TranslationState::ComputingNextProducer(inner) => self.compute_next_producer(inner).await,
            TranslationState::ProducerProposal(inner) => self.accept_proposal(inner).await,
            TranslationState::Done => Ok(TranslationState::Done),
        }
    }

    async fn accept_proposal(&self, state: ProducerProposalState) -> anyhow::Result<TranslationState>;

    async fn compute_next_producer(&self, state: ComputingNextProducerState) -> anyhow::Result<TranslationState>;
}




struct ScyllaTimelineTranslator {
    consumer_group_store: ScyllaConsumerGroupStore,
    producer_queries: ProducerQueries,
}


#[derive(Debug, Clone, Error, Eq, PartialEq)]
pub enum TranslationStepError {
    ConsumerGroupNotFound
}

impl fmt::Display for TranslationStepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}



#[async_trait]
impl TimelineTranslator for ScyllaTimelineTranslator {

    async fn compute_next_producer(&self, state: ComputingNextProducerState) -> anyhow::Result<TranslationState> {

        let cg_info = self
            .consumer_group_store
            .get_consumer_group_info(&state.consumer_group_id).await?
            .ok_or(TranslationStepError::ConsumerGroupNotFound)?;


        let (lcs, _max_revision) = self
            .consumer_group_store
            .get_lowest_common_slot_number(&state.consumer_group_id, Some(state.revision))
            .await?;

        let slot_ranges = Some(lcs - 10..=lcs);
        let (producer_id, execution_id) = self
            .producer_queries
            .get_producer_id_with_least_assigned_consumer(
                slot_ranges,
                cg_info.commitment_level
            )
            .await?;

        let seek_loc = SeekLocation::SlotApprox {
            desired_slot: lcs,
            min_slot: lcs - 10,
        };
        let new_shard_offsets = self
            .producer_queries
            .compute_offset(producer_id, seek_loc, None)
            .await?;

        let new_state = TranslationState::ProducerProposal(ProducerProposalState { 
            consumer_group_id: state.consumer_group_id, 
            revision: state.revision, 
            producer_id, 
            execution_id,
            new_shard_offsets
        });

        Ok(new_state)
    }

    async fn accept_proposal(&self, state: ProducerProposalState) -> anyhow::Result<TranslationState> {
        self.consumer_group_store
            .set_static_group_members_shard_offset(
                &state.consumer_group_id,
                &state.producer_id,
                &state.execution_id,
                &state.new_shard_offsets,
                state.revision,
            )
            .await?;

        self.consumer_group_store
            .update_consumer_group_producer(
                &state.consumer_group_id,
                &state.producer_id,
                &state.execution_id,
                state.revision,
            )
            .await?;

        Ok(TranslationState::Done)
    }

}