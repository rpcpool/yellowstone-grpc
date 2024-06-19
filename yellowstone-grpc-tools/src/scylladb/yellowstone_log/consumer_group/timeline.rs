use {
    super::{consumer_group_store::ScyllaConsumerGroupStore, producer::ScyllaProducerStore}, crate::scylladb::{
        types::{ConsumerGroupId, ProducerId, ShardOffsetMap},
        yellowstone_log::common::SeekLocation,
    }, core::fmt, futures::future, serde::{Deserialize, Serialize}, std::fmt::LowerExp, thiserror::Error, tonic::async_trait, tracing::{info, warn}, uuid::Uuid
};

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
    pub new_shard_offsets: ShardOffsetMap,
}

/// Represents the state of a completed translation in the timeline translation process.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TranslationDoneState {
    pub producer_id: ProducerId,
    pub new_shard_offsets: ShardOffsetMap,
}

/// Represents the possible states in the timeline translation process.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TranslationState {
    ComputingNextProducer(ComputingNextProducerState),
    ProducerProposal(ProducerProposalState),
    Done(TranslationDoneState),
}

#[derive(Debug, Clone, Error, Eq, PartialEq)]
pub enum TranslationStepError {
    ConsumerGroupNotFound,
    NoActiveProducer,
    InternalError(String),
    StaleProducerProposition(String),
}

impl fmt::Display for TranslationStepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TranslationStepError::ConsumerGroupNotFound => {
                write!(f, "consumer group no longuer exists")
            }
            TranslationStepError::StaleProducerProposition(e) => {
                write!(f, "producer proposal is stale: {}", e)
            }
            TranslationStepError::NoActiveProducer => write!(f, "no active producer found"),
            TranslationStepError::InternalError(e) => write!(f, "got an internal error: {}", e),
        }
    }
}

pub type TranslationStepResult = std::result::Result<TranslationState, TranslationStepError>;
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
        TranslationState::ComputingNextProducer(ComputingNextProducerState {
            consumer_group_id,
            revision,
        })
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
    async fn next(&self, state: TranslationState) -> TranslationStepResult {
        match state {
            TranslationState::ComputingNextProducer(inner) => {
                self.compute_next_producer(inner).await
            }
            TranslationState::ProducerProposal(inner) => self.accept_proposal(inner).await,
            TranslationState::Done(inner) => Ok(TranslationState::Done(inner)),
        }
    }

    /// Computes the next producer in the timeline translation process.
    async fn compute_next_producer(
        &self,
        state: ComputingNextProducerState,
    ) -> TranslationStepResult;

    /// Accepts a producer proposal in the timeline translation process.
    async fn accept_proposal(&self, state: ProducerProposalState) -> TranslationStepResult;
}

pub struct ScyllaTimelineTranslator {
    pub consumer_group_store: ScyllaConsumerGroupStore,
    pub producer_queries: ScyllaProducerStore,
}

#[async_trait]
impl TimelineTranslator for ScyllaTimelineTranslator {

    
    async fn compute_next_producer(
        &self,
        state: ComputingNextProducerState
    ) -> TranslationStepResult {
        info!("computing next producer for consumer group id {}", Uuid::from_bytes(state.consumer_group_id));
        let cg_info = self
            .consumer_group_store
            .get_consumer_group_info(&state.consumer_group_id)
            .await
            .map_err(|e| TranslationStepError::InternalError(e.to_string()))?
            .ok_or(TranslationStepError::ConsumerGroupNotFound)?;

        let (lcs, _max_revision) = self
            .consumer_group_store
            .get_lowest_common_slot_number(&state.consumer_group_id, Some(state.revision))
            .await
            .map_err(|e| TranslationStepError::InternalError(e.to_string()))?;
        let uuid_str = Uuid::from_bytes(state.consumer_group_id).to_string();
        info!(
            "lower common slot number is {} for consumer group id {}",
            lcs, uuid_str
        );
        let lower_bound = std::cmp::max(lcs - 10, 0);
        let slot_ranges = lower_bound..=lcs;
        let producer_id = self
            .producer_queries
            .get_producer_id_with_least_assigned_consumer(Some(slot_ranges.clone()), cg_info.commitment_level)
            .await
            .map_err(|e| TranslationStepError::InternalError(e.to_string()))?;
        info!("candidate producer id is {}", producer_id);
        let seek_loc = SeekLocation::SlotApprox(slot_ranges);
        let new_shard_offsets = self
            .producer_queries
            .compute_offset(producer_id, seek_loc)
            .await
            .map_err(|e| TranslationStepError::InternalError(e.to_string()));
        if new_shard_offsets.is_err() {
            warn!("got an error while computing offset for producer id {}, {:?}", producer_id, new_shard_offsets);
        }
        let new_shard_offsets = new_shard_offsets?;
        let new_state = TranslationState::ProducerProposal(ProducerProposalState {
            consumer_group_id: state.consumer_group_id,
            revision: state.revision,
            producer_id,
            new_shard_offsets,
        });

        Ok(new_state)
    }

    async fn accept_proposal(&self, state: ProducerProposalState) -> TranslationStepResult {
        let maybe = self
            .producer_queries
            .get_producer_info(state.producer_id)
            .await
            .map_err(|e| TranslationStepError::InternalError(e.to_string()))?;

        if maybe.is_none() {
            return Err(TranslationStepError::StaleProducerProposition(format!(
                "poducer with id {} no longuer exists",
                state.producer_id
            )));
        }

        self.consumer_group_store
            .set_static_group_members_shard_offset(
                &state.consumer_group_id,
                &state.producer_id,
                &state.new_shard_offsets,
                state.revision,
            )
            .await
            .map_err(|e| TranslationStepError::InternalError(e.to_string()))?;

        self.consumer_group_store
            .update_consumer_group_producer(
                &state.consumer_group_id,
                &state.producer_id,
                state.revision,
            )
            .await
            .map_err(|e| TranslationStepError::InternalError(e.to_string()))?;

        let done_state = TranslationDoneState {
            producer_id: state.producer_id,
            new_shard_offsets: state.new_shard_offsets,
        };
        Ok(TranslationState::Done(done_state))
    }
}
