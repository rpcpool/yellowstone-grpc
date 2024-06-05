use {
    super::{
        common::SeekLocation,
        consumer_group::{
            consumer_group_store::ConsumerGroupStore, consumer_source::FromBlockchainEvent,
            leader::create_leader_state_log, lock::InstanceLocker,
        },
    },
    crate::scylladb::{
        etcd_utils::lock::TryLockError,
        types::{BlockchainEventType, CommitmentLevel},
    },
    futures::Stream,
    scylla::Session,
    std::{pin::Pin, str::FromStr, sync::Arc, time::Duration},
    tokio::sync::mpsc,
    tonic::Response,
    tracing::{error, info, warn},
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

const ZERO_CONSUMER_GROUP_ID: Uuid =
    Uuid::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

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
    session: Arc<Session>,
    etcd: etcd_client::Client,
    consumer_group_repo: ConsumerGroupStore,
    instance_locker: InstanceLocker,
}

impl ScyllaYsLog {
    pub async fn new(
        session: Arc<Session>,
        etcd_client: etcd_client::Client,
    ) -> anyhow::Result<Self> {
        let consumer_group_repo =
            ConsumerGroupStore::new(Arc::clone(&session), etcd_client.clone()).await?;
        Ok(ScyllaYsLog {
            session,
            etcd: etcd_client.clone(),
            consumer_group_repo,
            instance_locker: InstanceLocker(etcd_client.clone()),
        })
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
        let initial_offset = match request.initial_offset_policy() {
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
                SeekLocation::SlotApprox {
                    desired_slot: slot,
                    min_slot: slot,
                }
            }
        };

        let consumer_group_info = self
            .consumer_group_repo
            .create_static_consumer_group(
                &instance_ids,
                match request.commitment_level() {
                    yellowstone_grpc_proto::geyser::CommitmentLevel::Processed => {
                        CommitmentLevel::Processed
                    }
                    yellowstone_grpc_proto::geyser::CommitmentLevel::Confirmed => {
                        CommitmentLevel::Confirmed
                    }
                    yellowstone_grpc_proto::geyser::CommitmentLevel::Finalized => {
                        CommitmentLevel::Finalized
                    }
                },
                &get_blockchain_event_types(event_subscription_policy),
                initial_offset,
                remote_ip_addr,
            )
            .await
            .map_err(|e| {
                error!("create_static_consumer_group: {e:?}");
                tonic::Status::internal("failed to create consumer group")
            })?;

        create_leader_state_log(&self.etcd, &consumer_group_info)
            .await
            .map_err(|e| {
                error!("create_static_consumer_group: {e:?}");
                tonic::Status::internal("failed to create consumer group")
            })?;
        Ok(Response::new(CreateStaticConsumerGroupResponse {
            group_id: String::from_utf8(consumer_group_info.consumer_group_id).map_err(|e| {
                error!("consumer group id is not utf8!");
                tonic::Status::internal("failed to create consumer group")
            })?,
        }))
    }

    async fn consume(
        &self,
        request: tonic::Request<ConsumeRequest>,
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
            .as_bytes()
            .to_vec();
        let instance_id = join_request
            .instance_id
            .ok_or(tonic::Status::invalid_argument(
                "missing instance id from join request",
            ))?;
        let maybe = self
            .consumer_group_repo
            .get_consumer_group_info(&cg_id)
            .await
            .map_err(|e| {
                error!("get_consumer_group_info raised an error: {e:?}");
                tonic::Status::internal("failed to validate consumer group id")
            })?;

        maybe.ok_or(tonic::Status::invalid_argument("Invalid consumer group id"))?;

        let lock = self
            .instance_locker
            .try_lock_instance_id(cg_id.clone(), instance_id.clone())
            .await
            .map_err(map_lock_err_to_tonic_status)?;
        join_request.group_id;
        todo!()
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
        tonic::Status::internal("server failure")
    }
}

type GrpcConsumerSender = mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>;
type GrpcConsumerReceiver = mpsc::Receiver<Result<SubscribeUpdate, tonic::Status>>;
type GrpcEvent = Result<SubscribeUpdate, tonic::Status>;

impl FromBlockchainEvent for GrpcEvent {
    type Output = Self;
    fn from(blockchain_event: crate::scylladb::types::BlockchainEvent) -> Self::Output {
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
