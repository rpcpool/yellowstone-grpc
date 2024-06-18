use {
    super::{
        etcd_path::{get_instance_lock_prefix_v1, parse_lock_key_v1},
        producer::ProducerMonitor,
        timeline::{self, ComputingNextProducerState, TimelineTranslator, TranslationState},
    },
    crate::scylladb::{
        self,
        etcd_utils::{
            self,
            barrier::{get_barrier, Barrier},
            lease::ManagedLease,
            Revision,
        },
        types::{
            BlockchainEventType, CommitmentLevel, ConsumerGroupId, ConsumerId, ExecutionId,
            ProducerId, ShardId, ShardOffset, ShardOffsetMap, Slot,
        },
        yellowstone_log::{
            common::SeekLocation,
            consumer_group::{
                self,
                error::{DeadConsumerGroup, LeaderStateLogNotFound},
                etcd_path::get_producer_lock_path_v1,
            },
        },
    },
    bincode::{deserialize, serialize},
    etcd_client::{
        Compare, EventType, GetOptions, LeaderKey, PutOptions, Txn, TxnOp, WatchOptions,
    },
    futures::Future,
    local_ip_address::list_afinet_netifas,
    serde::{Deserialize, Serialize},
    std::{collections::BTreeMap, fmt, net::IpAddr, sync::Arc, time::Duration},
    thiserror::Error,
    tokio::{
        sync::{
            oneshot::{self, error::RecvError},
            watch,
        },
        task::JoinHandle,
    },
    tokio_stream::StreamExt,
    tracing::{error, info, warn},
    uuid::Uuid,
};

const LEADER_LEASE_TTL: Duration = Duration::from_secs(60);

#[derive(Serialize, Deserialize)]
enum LeaderCommand {
    Join { lock_key: Vec<u8> },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ConsumerGroupState {
    Init(ConsumerGroupHeader),
    LostProducer(LostProducerState),
    WaitingBarrier(WaitingBarrierState),
    InTimelineTranslation(InTimelineTranslationState),
    Idle(IdleState),
    Dead(ConsumerGroupHeader),
}

impl ConsumerGroupState {
    pub fn header(&self) -> &ConsumerGroupHeader {
        match self {
            ConsumerGroupState::Init(header) => header,
            ConsumerGroupState::LostProducer(x) => &x.header,
            ConsumerGroupState::WaitingBarrier(x) => &x.header,
            ConsumerGroupState::InTimelineTranslation(x) => &x.header,
            ConsumerGroupState::Idle(x) => &x.header,
            ConsumerGroupState::Dead(header) => header,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LostProducerState {
    pub header: ConsumerGroupHeader,
    pub lost_producer_id: ProducerId,
    pub execution_id: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct WaitingBarrierState {
    pub header: ConsumerGroupHeader,
    pub lease_id: i64,
    pub barrier_key: Vec<u8>,
    pub wait_for: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct InTimelineTranslationState {
    pub header: ConsumerGroupHeader,
    pub substate: TranslationState,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IdleState {
    pub header: ConsumerGroupHeader,
    pub producer_id: ProducerId,
    pub execution_id: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ConsumerGroupHeader {
    pub consumer_group_id: ConsumerGroupId,
    pub commitment_level: CommitmentLevel,
    pub subscribed_blockchain_event_types: Vec<BlockchainEventType>,
    pub shard_assignments: BTreeMap<ConsumerId, Vec<ShardId>>,
}

pub struct ConsumerGroupLeaderNode {
    consumer_group_id: ConsumerGroupId,
    etcd: etcd_client::Client,
    leader_key: LeaderKey,
    leader_lease: ManagedLease,
    state_log_key: String,
    state: ConsumerGroupState,
    last_revision: Revision,
    barrier: Option<Barrier>,
    producer_monitor: Arc<dyn ProducerMonitor>,
    timeline_translator: Arc<dyn TimelineTranslator + Send + Sync>,
}

///
/// This error is raised when there is no active producer for the desired commitment level.
///
#[derive(Error, PartialEq, Eq, Debug, Clone)]
pub enum LeaderNodeError {
    FailedToUpdateStateLog(String),
}

impl fmt::Display for LeaderNodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaderNodeError::FailedToUpdateStateLog(e) => write!(f, "failed to update log, {}", e),
        }
    }
}

impl Drop for ConsumerGroupLeaderNode {
    fn drop(&mut self) {
        warn!("dropping consumer group leader node");
    }
}

impl ConsumerGroupLeaderNode {
    pub async fn new(
        mut etcd: etcd_client::Client,
        producer_monitor: Arc<dyn ProducerMonitor>,
        leader_key: LeaderKey,
        leader_lease: ManagedLease,
        timeline_translator: Arc<dyn TimelineTranslator + Send + Sync>,
    ) -> anyhow::Result<Self> {
        let state_log_key = leader_log_name_from_leader_key_v1(&leader_key);
        let get_resp = etcd.get(state_log_key.as_bytes(), None).await?;
        let (last_revision, state) = get_resp
            .kvs()
            .first()
            .map(|kv| {
                deserialize::<ConsumerGroupState>(kv.value()).map(|sm| (kv.mod_revision(), sm))
            })
            .transpose()?
            .ok_or(anyhow::anyhow!(
                "consumer group leader found no state in {state_log_key:?}"
            ))?;

        let consumer_group_id = state.header().consumer_group_id.clone();
        let ret = ConsumerGroupLeaderNode {
            consumer_group_id,
            etcd,
            leader_key,
            leader_lease,
            state,
            last_revision,
            barrier: None,
            state_log_key,
            producer_monitor,
            timeline_translator,
        };
        Ok(ret)
    }

    pub fn state(&self) -> &ConsumerGroupState {
        &self.state
    }

    pub async fn update_state_machine(
        &mut self,
        next_step: ConsumerGroupState,
    ) -> anyhow::Result<()> {
        let leader_log_key = &self.state_log_key;
        let txn = etcd_client::Txn::new()
            .when(vec![
                Compare::version(self.leader_key.key(), etcd_client::CompareOp::Greater, 0),
                Compare::mod_revision(
                    leader_log_key.as_str(),
                    etcd_client::CompareOp::Equal,
                    self.last_revision,
                ),
            ])
            .and_then(vec![TxnOp::put(
                leader_log_key.as_str(),
                serialize(&next_step)?,
                None,
            )]);

        let mut txn_client = self.etcd.kv_client();
        let txn_resp = txn_client.txn(txn).await?;

        anyhow::ensure!(
            txn_resp.succeeded(),
            LeaderNodeError::FailedToUpdateStateLog(format!("{txn_resp:?}"))
        );

        let revision = txn_resp
            .op_responses()
            .pop()
            .and_then(|op| match op {
                etcd_client::TxnOpResponse::Put(put_resp) => {
                    put_resp.header().map(|header| header.revision())
                }
                _ => panic!("unexpected txn op response"),
            })
            .expect("unexpected txn response");

        self.last_revision = revision;
        self.state = next_step;
        Ok(())
    }

    async fn handle_in_timeline_translation(
        &self,
        state: &InTimelineTranslationState,
    ) -> anyhow::Result<ConsumerGroupState> {
        let result = self
            .timeline_translator
            .next(state.substate.to_owned())
            .await;
        match result {
            Ok(substate2) => Ok(match substate2 {
                TranslationState::Done(inner) => ConsumerGroupState::Idle(IdleState {
                    header: state.header.to_owned(),
                    producer_id: inner.producer_id,
                    execution_id: inner.execution_id,
                }),
                anystate => ConsumerGroupState::InTimelineTranslation(InTimelineTranslationState {
                    header: state.header.to_owned(),
                    substate: anystate,
                }),
            }),
            Err(e) => match e {
                timeline::TranslationStepError::ConsumerGroupNotFound => {
                    Ok(ConsumerGroupState::Dead(state.header.clone()))
                }
                timeline::TranslationStepError::StaleProducerProposition(_) => Ok(
                    ConsumerGroupState::InTimelineTranslation(InTimelineTranslationState {
                        header: state.header.clone(),
                        substate: TranslationState::ComputingNextProducer(
                            ComputingNextProducerState {
                                consumer_group_id: state.header.consumer_group_id,
                                revision: self.last_revision,
                            },
                        ),
                    }),
                ),
                timeline::TranslationStepError::NoActiveProducer => {
                    anyhow::bail!(consumer_group::error::NoActiveProducer)
                }
                timeline::TranslationStepError::InternalError(e) => anyhow::bail!(e),
            },
        }
    }

    async fn handle_lost_producer(
        &self,
        state: &LostProducerState,
    ) -> anyhow::Result<ConsumerGroupState> {
        let barrier_key = generate_barrier_key_v1();
        let lease_id = self.etcd.lease_client().grant(10, None).await?.id();
        let lock_prefix = get_instance_lock_prefix_v1(self.consumer_group_id.clone());
        // TODO add healthcheck here
        let wait_for = self
            .etcd
            .kv_client()
            .get(lock_prefix, Some(GetOptions::new().with_prefix()))
            .await?
            .kvs()
            .iter()
            .map(|kv| kv.key())
            .map(|lock_key| {
                parse_lock_key_v1(lock_key)
                    .map(|(_, consumer_id)| consumer_id.as_bytes().to_vec())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let _barrier = etcd_utils::barrier::new_barrier(
            self.etcd.clone(),
            barrier_key.as_str(),
            &wait_for,
            lease_id,
        )
        .await?;
        
        let next_state = ConsumerGroupState::WaitingBarrier(WaitingBarrierState {
            header: state.header.clone(),
            lease_id,
            barrier_key: barrier_key.as_bytes().to_vec(),
            wait_for,
        });
        Ok(next_state)
    }

    async fn handle_wait_barrier(
        &self,
        state: &WaitingBarrierState,
    ) -> anyhow::Result<ConsumerGroupState> {
        let barrier = get_barrier(self.etcd.clone(), &state.barrier_key).await?;
        const BARRIER_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
        info!("waiting for barrier to be reached...");
        if let Err(e) = tokio::time::timeout(BARRIER_WAIT_TIMEOUT, barrier.wait()).await {
            warn!("wait barrier has reached timeout");
        }
        Ok(ConsumerGroupState::InTimelineTranslation(
            InTimelineTranslationState {
                header: state.header.to_owned(),
                substate: self
                    .timeline_translator
                    .begin_translation(state.header.consumer_group_id, self.last_revision),
            },
        ))
    }

    ///
    /// This function is cancel safe
    pub async fn step(&self) -> anyhow::Result<Option<ConsumerGroupState>> {
        let next_state = match &self.state {
            ConsumerGroupState::Init(header) => Some(ConsumerGroupState::InTimelineTranslation(
                InTimelineTranslationState {
                    header: header.to_owned(),
                    substate: self
                        .timeline_translator
                        .begin_translation(header.consumer_group_id, self.last_revision),
                },
            )),
            ConsumerGroupState::LostProducer(inner) => {
                info!("will handle lost producer");
                Some(self.handle_lost_producer(inner).await?)
            }
            ConsumerGroupState::WaitingBarrier(inner) => {
                info!("will handle wait barrier");
                Some(self.handle_wait_barrier(inner).await?)
            }
            ConsumerGroupState::InTimelineTranslation(inner) => {
                Some(self.handle_in_timeline_translation(inner).await?)
            }
            ConsumerGroupState::Idle(IdleState {
                header,
                producer_id,
                execution_id,
            }) => {
                let signal = self
                    .producer_monitor
                    .get_producer_dead_signal(*producer_id)
                    .await;
                let _ = signal.await?;
                warn!("received dead signal from producer {producer_id:?}");
                Some(ConsumerGroupState::LostProducer(LostProducerState {
                    header: header.clone(),
                    lost_producer_id: *producer_id,
                    execution_id: execution_id.clone(),
                }))
            }
            ConsumerGroupState::Dead(inner) => Some(ConsumerGroupState::Dead(inner.clone())),
        };
        return Ok(next_state);
    }

    /// Runs the leader loop for the consumer group.
    ///
    /// This function is responsible for managing the state machine of the consumer group leader. It
    /// handles various states such as computing the next producer, waiting for a barrier, updating the
    /// producer at the group level, and handling a lost producer. The function runs in a loop and
    /// updates the state machine as necessary, until an interrupt signal is received.
    ///
    /// # Arguments
    /// * `self` - A mutable reference to the `ConsumerGroupLeader` instance.
    /// * `interrupt_signal` - A oneshot receiver that can be used to interrupt the loop.
    ///
    /// # Returns
    /// A result indicating whether the leader loop completed successfully.
    pub async fn leader_loop(
        &mut self,
        mut interrupt_signal: oneshot::Receiver<()>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                res = self.step() => {
                    let maybe_next_state = res?;
                    if let Some(next_state) = maybe_next_state {
                        anyhow::ensure!(!matches!(&next_state, ConsumerGroupState::Dead(_)), DeadConsumerGroup(self.consumer_group_id.clone()));
                        self.update_state_machine(next_state).await?;
                    } else {
                        info!("skipping state transation no state change detected");
                    }
                }
                _ = &mut interrupt_signal => {
                    warn!("received interrupt signal in leader loop, exiting...");
                    return Ok(())
                }
            }
        }
    }
}

fn generate_barrier_key_v1() -> String {
    let uuid_str = Uuid::new_v4().to_string();
    format!("v1#barrier#{uuid_str}")
}

/// Creates a new leader state log for the given consumer group in etcd.
///
/// This function creates a new leader state log entry in etcd for the given consumer group. The
/// state log contains the current state of the consumer group, including the commitment level,
/// subscribed event types, shard assignments, and the current state of the consumer group state
/// machine.
///
/// The function uses a transaction to ensure that the state log is only created if it does not
/// already exist. If the state log already exists, the function will return an error.
///
/// # Arguments
/// * `etcd` - A reference to the etcd client to use for interacting with etcd.
/// * `consumer_group_info` - The consumer group information to use for creating the state log.
///
/// # Returns
/// A result indicating whether the state log was successfully created.
pub async fn create_leader_state_log(
    etcd: etcd_client::Client,
    scylla_consumer_group_info: &scylladb::types::ConsumerGroupInfo,
) -> anyhow::Result<()> {
    let header = ConsumerGroupHeader {
        consumer_group_id: scylla_consumer_group_info.consumer_group_id.clone(),
        commitment_level: scylla_consumer_group_info.commitment_level,
        subscribed_blockchain_event_types: scylla_consumer_group_info
            .subscribed_event_types
            .clone(),
        shard_assignments: scylla_consumer_group_info
            .consumer_id_shard_assignments
            .clone(),
    };
    let state = ConsumerGroupState::Idle(IdleState {
        header,
        producer_id: scylla_consumer_group_info
            .producer_id
            .expect("missing producer id"),

        execution_id: scylla_consumer_group_info
            .execution_id
            .clone()
            .expect("missing execution id"),
    });

    let state_log_key = leader_log_name_from_cg_id_v1(scylla_consumer_group_info.consumer_group_id);

    let txn = Txn::new()
        .when(vec![Compare::version(
            state_log_key.clone(),
            etcd_client::CompareOp::Equal,
            0,
        )])
        .and_then(vec![TxnOp::put(state_log_key, serialize(&state)?, None)]);

    let txn_resp = etcd.kv_client().txn(txn).await?;
    anyhow::ensure!(txn_resp.succeeded(), "failed to create state log");
    Ok(())
}

/// Observes changes to the consumer group state stored in the leader state log. Returns a watch
/// receiver that will receive updates whenever the state changes.
///
/// This function first retrieves the current consumer group state from etcd, and then starts a watch
/// on the leader state log key. Whenever the state log is updated or deleted, the watch receiver
/// will receive the new state or `None` if the state log has been deleted.
///
/// The function returns a `watch::Receiver` that can be used to observe the state changes. The
/// receiver will initially receive the current state, and then receive updates whenever the state
/// changes.
pub async fn observe_consumer_group_state(
    etcd: etcd_client::Client,
    consumer_group_id: ConsumerGroupId,
) -> anyhow::Result<watch::Receiver<(Revision, ConsumerGroupState)>> {
    let key = leader_log_name_from_cg_id_v1(consumer_group_id);
    let mut wc = etcd.watch_client();

    let mut kv_client = etcd.kv_client();

    let get_resp = kv_client.get(key.as_str(), None).await?;

    anyhow::ensure!(
        get_resp.count() == 1,
        LeaderStateLogNotFound(consumer_group_id.clone())
    );

    let initial_state = deserialize::<ConsumerGroupState>(get_resp.kvs()[0].value())?;
    let initial_revision = get_resp.kvs()[0].mod_revision();
    let (tx, rx) = watch::channel((initial_revision, initial_state));

    tokio::spawn(async move {
        let (mut watcher, mut stream) = wc
            .watch(key.as_str(), None)
            .await
            .unwrap_or_else(|_| panic!("failed to watch {key}"));

        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(15)) => {
                    if tx.is_closed() {
                        break;
                    }
                }
                // tokio's StreamExt::next is cancel safe
                Some(result) = stream.next() => {
                    let message = result.expect("watch stream was terminated early");
                    let events = message.events();
                    if events.iter().any(|ev| ev.event_type() == EventType::Delete) {
                        panic!("remote state log has been deleted")
                    }
                    let maybe = events
                        .iter()
                        .filter_map(|ev| {
                            ev.kv().map(|kv| {
                                (
                                    kv.mod_revision(),
                                    deserialize::<ConsumerGroupState>(kv.value())
                                        .expect("failed to deser consumer group state from leader log"),
                                )
                            })
                        })
                        .max_by_key(|(revision, _)| *revision);
                    if let Some(payload) = maybe {
                        if tx.send(payload).is_err() {
                            warn!("receiver half of LeaderStateLogObserver has been close");
                            break;
                        }
                    }
                }

            }
        }
        let _ = watcher.cancel().await;
    });

    Ok(rx)
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LeaderInfo {
    pub ipaddr: IpAddr,
    pub id: Vec<u8>,
}

fn leader_log_name_from_leader_key_v1(lk: &LeaderKey) -> String {
    let lk_name = lk.name_str().expect("invalid leader key name");
    let last_part = lk_name
        .split("#")
        .last()
        .expect("unexpected leader key name format");
    let cg_leader_uuid = last_part
        .split("/")
        .next()
        .expect("unexpected leader key name format");
    format!("v1#leader-state-log#{cg_leader_uuid}")
}

pub fn leader_log_name_from_cg_id_v1(consumer_group_id: ConsumerGroupId) -> String {
    let uuid = Uuid::from_bytes(consumer_group_id).to_string();
    format!("v1#leader-state-log#cg-leader-{uuid}")
}

fn leader_name_v1(consumer_group_id: ConsumerGroupId) -> String {
    let uuid = Uuid::from_bytes(consumer_group_id).to_string();
    format!("v1#leader#cg-leader-{uuid}")
}

/// Observes changes to the leader of the given consumer group. Returns a watch receiver that will
/// receive updates whenever the leader changes.
///
/// This function first retrieves the current leader information from etcd, and then starts a watch
/// on the leader key. Whenever the leader key is updated or deleted, the watch receiver will
/// receive the new leader information or `None` if the leader has been deleted.
///
/// The function returns a `watch::Receiver` that can be used to observe the leader changes. The
/// receiver will initially receive the current leader information, and then receive updates
/// whenever the leader changes.
pub async fn observe_leader_changes(
    etcd: etcd_client::Client,
    consumer_group_id: ConsumerGroupId,
) -> anyhow::Result<watch::Receiver<Option<LeaderInfo>>> {
    let mut kv_client = etcd.kv_client();
    let mut wc = etcd.watch_client();
    let leader = leader_name_v1(consumer_group_id);

    let mut get_resp = kv_client
        .get(leader.as_str(), Some(GetOptions::new().with_prefix()))
        .await?;

    let initital_value = get_resp
        .take_kvs()
        .into_iter()
        .max_by_key(|kv| kv.mod_revision())
        .map(|kv| serde_json::from_slice::<LeaderInfo>(kv.value()))
        .transpose()?;
    info!("initital value {initital_value:?}");
    let (tx, rx) = watch::channel(initital_value);
    tokio::spawn(async move {
        let watch_opts = WatchOptions::new().with_prefix();
        let (mut watcher, mut stream) = wc
            .watch(leader.as_str(), Some(watch_opts))
            .await
            .unwrap_or_else(|_| panic!("fail to watch {leader}"));

        'outer: loop {
            tokio::select! {

                _ = tokio::time::sleep(Duration::from_secs(15)) => {
                    if tx.is_closed() {
                        break;
                    }
                }

                Some(Ok(msg)) = stream.next() => {
                    for ev in msg.events() {
                        let payload = match ev.event_type() {
                            EventType::Put => ev.kv().map(|kv| {
                                // deserialize::<LeaderInfo>(kv.value())
                                //     .expect("invalid leader state log format")
                                serde_json::from_slice::<LeaderInfo>(kv.value())
                                    .expect("invalid leader state log format")
                            }),
                            EventType::Delete => None,
                        };
                        if tx.send(payload).is_err() {
                            break 'outer;
                        }
                    }
                }

            }
        }
        let _ = watcher.cancel().await;
    });

    Ok(rx)
}

pub async fn try_become_leader(
    etcd: etcd_client::Client,
    consumer_group_id: ConsumerGroupId,
    timeout: Duration,
    leader_ifname: String,
) -> anyhow::Result<Option<(LeaderKey, ManagedLease)>> {
    let network_interfaces = list_afinet_netifas()?;

    let ipaddr = network_interfaces
        .iter()
        .find_map(|(name, ipaddr)| {
            if name == &leader_ifname {
                Some(ipaddr)
            } else {
                None
            }
        })
        .ok_or(anyhow::anyhow!(
            "Found no interface named {}",
            leader_ifname
        ))?
        .to_owned();

    let mut ec = etcd.election_client();
    let leader_name = leader_name_v1(consumer_group_id);

    let id = Uuid::new_v4();
    let leader_info = LeaderInfo {
        ipaddr,
        id: id.as_bytes().to_vec(),
    };
    let lease = ManagedLease::new(etcd.clone(), LEADER_LEASE_TTL, None).await?;

    tokio::select! {
        _ = tokio::time::sleep(timeout) => {
            warn!("failed to become leader in time");
            return Ok(None)
        },
        Ok(mut campaign_resp) = ec.campaign(leader_name.as_str(), serde_json::to_string(&leader_info)?, lease.lease_id) => {
            info!("became leader for {leader_name}");
            let payload = campaign_resp
                .take_leader()
                .map(|lk| (lk, lease));
            return Ok(payload)
        },
    }
}
