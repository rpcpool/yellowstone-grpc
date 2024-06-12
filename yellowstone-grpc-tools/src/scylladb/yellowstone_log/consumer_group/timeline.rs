use core::fmt;

use futures::future;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tonic::async_trait;

use crate::scylladb::{types::{ConsumerGroupId, ExecutionId, ProducerId, ShardOffsetMap}, yellowstone_log::common::SeekLocation};

use super::{consumer_group_store::ScyllaConsumerGroupStore, producer_queries::ProducerQueries};


/// Represents the state of computing the next producer in the timeline translation process.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ComputingNextProducerState {
    pub consumer_group_id: ConsumerGroupId,
    pub revision: i64,
}

/// Represents the state of a producer proposal in the timeline translation process.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProducerProposalState {
    pub consumer_group_id: ConsumerGroupId,
    pub revision: i64,
    pub producer_id: ProducerId,
    pub execution_id: ExecutionId,
    pub new_shard_offsets: ShardOffsetMap,
}

/// Represents the state of a completed translation in the timeline translation process.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TranslationDoneState {
    pub producer_id: ProducerId,
    pub execution_id: ExecutionId,
    pub new_shard_offsets: ShardOffsetMap
}

/// Represents the possible states in the timeline translation process.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TranslationState {
    ComputingNextProducer(ComputingNextProducerState),
    ProducerProposal(ProducerProposalState),
    Done(TranslationDoneState)
}
/// Trait for timeline translators.
///
/// This trait represents an API to handle a timeline translation state machine. The translator
/// follows the visitor pattern, where each state is visited and processed accordingly.
/// 
/// This trait defines two methods: `begin_translation` and `next`. The `begin_translation` method
/// starts the translation process with the given consumer group ID and revision, and returns the
/// initial state of the translation. The `next` method advances the translation process to the
/// next state based on the current state, and returns the updated state.
#[async_trait]
pub trait TimelineTranslator {
    /// Begins the translation process with the given consumer group ID and revision.
    ///
    /// This method initializes the translation process by creating the initial state of the
    /// translation. It takes in the `consumer_group_id` and `revision` as parameters, and returns
    /// the initial state of the translation.
    ///
    /// # Arguments
    ///
    /// * `consumer_group_id` - The ID of the consumer group.
    /// * `revision` - The revision number.
    ///
    /// # Returns
    ///
    /// The initial state of the translation.
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

    /// Advances the translation process to the next state.
    ///
    /// This method takes in the current state of the translation as a parameter, and advances the
    /// translation process to the next state based on the current state. It returns the updated
    /// state of the translation.
    ///
    /// # Arguments
    ///
    /// * `state` - The current state of the translation.
    ///
    /// # Returns
    ///
    /// The updated state of the translation.
    async fn next(&self, state: TranslationState) -> anyhow::Result<TranslationState> {
        match state {
            TranslationState::ComputingNextProducer(inner) => self.compute_next_producer(inner).await,
            TranslationState::ProducerProposal(inner) => self.accept_proposal(inner).await,
            TranslationState::Done(inner) => Ok(TranslationState::Done(inner)),
        }
    }

    /// Computes the next producer in the timeline translation process.
    async fn compute_next_producer(&self, state: ComputingNextProducerState) -> anyhow::Result<TranslationState>;

    /// Accepts a producer proposal in the timeline translation process.
    async fn accept_proposal(&self, state: ProducerProposalState) -> anyhow::Result<TranslationState>;

}


pub struct ScyllaTimelineTranslator {
    pub consumer_group_store: ScyllaConsumerGroupStore,
    pub producer_queries: ProducerQueries,
}


#[derive(Debug, Clone, Error, Eq, PartialEq)]
pub enum TranslationStepError {
    ConsumerGroupNotFound,
    StaleProducerProposition(String)
}

impl fmt::Display for TranslationStepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TranslationStepError::ConsumerGroupNotFound => write!(f, "consumer group no longuer exists"),
            TranslationStepError::StaleProducerProposition(e) => write!(f, "producer proposal is stale: {}", e),
        }
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

        let maybe = self.producer_queries.get_execution_id(state.producer_id).await?;

        match maybe {
            Some((_, actual_execution_id)) => {
                anyhow::ensure!(
                    actual_execution_id == state.execution_id,
                    TranslationStepError::StaleProducerProposition(
                        format!("producer's execution id changed before translation could finish")
                    )
                )
            },
            None => {
                anyhow::bail!(
                    TranslationStepError::StaleProducerProposition(
                        format!("producer with id {:?} no longuer exists", state.producer_id)
                    )
                )
            },
        }

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
        let done_state = TranslationDoneState {
            producer_id: state.producer_id, 
            execution_id: state.execution_id, 
            new_shard_offsets: state.new_shard_offsets, 
        };
        Ok(TranslationState::Done(done_state))
    }

}