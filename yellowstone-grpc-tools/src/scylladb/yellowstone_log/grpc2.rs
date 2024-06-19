use {
    super::{
        common::SeekLocation,
        consumer_group::{
            consumer_source::FromBlockchainEvent, coordinator::ConsumerGroupCoordinator,
        },
    },
    crate::scylladb::{
        etcd_utils::lock::TryLockError,
        types::{BlockchainEventType, CommitmentLevel, TranslationStrategy},
    },
    futures::{Stream, TryFutureExt},
    std::{pin::Pin, str::FromStr},
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tonic::Response,
    tracing::error,
    uuid::Uuid,
    yellowstone_grpc_proto::{
        geyser::{subscribe_update::UpdateOneof, SubscribeUpdate},
        yellowstone::log::{
            yellowstone_log_server::YellowstoneLog, ConsumeRequest,
            CreateStaticConsumerGroupRequest, CreateStaticConsumerGroupResponse,
            EventSubscriptionPolicy, JoinRequest, TimelineTranslationPolicy,
        },
    },
};

fn get_blockchain_event_types(
    event_sub_policy: EventSubscriptionPolicy,
) -> Vec<BlockchainEventType> {
    match event_sub_policy {
        EventSubscriptionPolicy::AccountUpdateOnly => vec![BlockchainEventType::AccountUpdate],
        EventSubscriptionPolicy::TransactionOnly => vec![BlockchainEventType::NewTransaction],
        EventSubscriptionPolicy::Both => vec![
            BlockchainEventType::AccountUpdate,
            BlockchainEventType::NewTransaction,
        ],
    }
}

pub struct ScyllaYsLog {
    coordinator: ConsumerGroupCoordinator,
}

impl ScyllaYsLog {
    pub async fn new(coordinator: ConsumerGroupCoordinator) -> anyhow::Result<Self> {
        Ok(ScyllaYsLog { coordinator })
    }
}

pub type LogStream = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl YellowstoneLog for ScyllaYsLog {
    #[doc = r" Server streaming response type for the consume method."]
    type ConsumeStream = LogStream;
    type JoinConsumerGroupStream = LogStream;

    async fn create_static_consumer_group(
        &self,
        request: tonic::Request<CreateStaticConsumerGroupRequest>,
    ) -> Result<tonic::Response<CreateStaticConsumerGroupResponse>, tonic::Status> {
        let remote_ip_addr = request.remote_addr().map(|addr| addr.ip());
        let request = request.into_inner();

        let instance_ids = request.instance_id_list.clone();

        let event_subscription_policy = request.event_subscription_policy();
        let seek_loc = match request.initial_offset_policy() {
            yellowstone_grpc_proto::yellowstone::log::InitialOffsetPolicy::Earliest => {
                SeekLocation::Earliest
            }
            yellowstone_grpc_proto::yellowstone::log::InitialOffsetPolicy::Latest => {
                SeekLocation::Latest
            }
            yellowstone_grpc_proto::yellowstone::log::InitialOffsetPolicy::Slot => {
                let slot = request.at_slot.ok_or(tonic::Status::invalid_argument(
                    "Expected at_lot when initital_offset_policy is to `Slot`",
                ))?;
                SeekLocation::SlotApprox(slot..=slot)
            }
        };

        let ttp = request.timeline_translation_policy();
        let translation_strategy = match ttp {
            TimelineTranslationPolicy::AllowLag => TranslationStrategy::AllowLag,
            TimelineTranslationPolicy::StrictSlot => TranslationStrategy::StrictSlot,
        };

        let commitment_level = match request.commitment_level() {
            yellowstone_grpc_proto::geyser::CommitmentLevel::Processed => {
                CommitmentLevel::Processed
            }
            yellowstone_grpc_proto::geyser::CommitmentLevel::Confirmed => {
                CommitmentLevel::Confirmed
            }
            yellowstone_grpc_proto::geyser::CommitmentLevel::Finalized => {
                CommitmentLevel::Finalized
            }
        };

        let consumer_group_id = self
            .coordinator
            .create_consumer_group(
                seek_loc,
                get_blockchain_event_types(event_subscription_policy),
                instance_ids,
                commitment_level,
                remote_ip_addr,
                Some(translation_strategy),
            )
            .await
            .map_err(map_lock_err_to_tonic_status)?;
        let group_id = Uuid::from_bytes(consumer_group_id).to_string();
        Ok(Response::new(CreateStaticConsumerGroupResponse { 
            group_id,
        }))
    }

    async fn consume(
        &self,
        _request: tonic::Request<ConsumeRequest>,
    ) -> Result<tonic::Response<Self::ConsumeStream>, tonic::Status> {
        unimplemented!()
    }

    async fn join_consumer_group(
        &self,
        request: tonic::Request<JoinRequest>,
    ) -> Result<tonic::Response<Self::ConsumeStream>, tonic::Status> {
        let join_request = request.into_inner();
        let cg_id = Uuid::from_str(join_request.group_id.as_str())
            .map_err(|_| tonic::Status::invalid_argument("invalid consumer group id value"))?
            .into_bytes();
        let instance_id = join_request
            .instance_id
            .ok_or(tonic::Status::invalid_argument(
                "missing instance id from join request",
            ))?;
        let (tx, rx) = mpsc::channel(10);

        self.coordinator
            .try_join_consumer_group::<GrpcEvent>(
                cg_id,
                instance_id,
                // TODO: add filtering
                None,
                tx,
            )
            .map_err(map_lock_err_to_tonic_status)
            .await?;

        let ret = ReceiverStream::new(rx);
        let res = Response::new(Box::pin(ret) as Self::JoinConsumerGroupStream);
        Ok(res)
    }
}

fn map_lock_err_to_tonic_status(e: anyhow::Error) -> tonic::Status {
    if let Some(e) = e.downcast_ref::<TryLockError>() {
        error!("error acquiring lock for {e:?}");
        match e {
            TryLockError::InvalidLockName => tonic::Status::internal("out of service"),
            TryLockError::AlreadyTaken => tonic::Status::already_exists(
                "Instance id is already consumed by another connection, try again later",
            ),
            TryLockError::LockingDeadlineExceeded => tonic::Status::deadline_exceeded(
                "failed to acquire exclusive access to instance id in time.",
            ),
        }
    } else {
        error!(
            error=%e
        );
        tonic::Status::internal("server failure")
    }
}

type GrpcEvent = Result<SubscribeUpdate, tonic::Status>;

impl FromBlockchainEvent for GrpcEvent {
    fn from(blockchain_event: crate::scylladb::types::BlockchainEvent) -> Self {
        let geyser_event = match blockchain_event.event_type {
            BlockchainEventType::AccountUpdate => {
                UpdateOneof::Account(blockchain_event.try_into().map_err(|e| {
                    error!(error=?e);
                    tonic::Status::internal("corrupted account update event in the stream")
                })?)
            }
            BlockchainEventType::NewTransaction => {
                UpdateOneof::Transaction(blockchain_event.try_into().map_err(|e| {
                    error!(error=?e);
                    tonic::Status::internal("corrupted new transaction event in the stream")
                })?)
            }
        };
        let subscribe_update = SubscribeUpdate {
            filters: Default::default(),
            update_oneof: Some(geyser_event),
        };

        Ok(subscribe_update)
    }
}
