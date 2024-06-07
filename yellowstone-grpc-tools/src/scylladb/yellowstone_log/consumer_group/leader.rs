use {
    super::{
        consumer_group_store::ConsumerGroupStore, etcd_path::get_instance_lock_prefix_v1,
        producer_queries::ProducerQueries,
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
            BlockchainEventType, CommitmentLevel, ConsumerGroupId, ExecutionId, InstanceId,
            ProducerId, ShardId, ShardOffset, ShardOffsetMap, Slot,
        },
        yellowstone_log::{
            common::SeekLocation,
            consumer_group::{
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
    std::{collections::BTreeMap, fmt, net::IpAddr, time::Duration},
    thiserror::Error,
    tokio::{
        sync::{
            oneshot::{self, error::RecvError},
            watch,
        },
        task::JoinHandle,
    },
    tracing::warn,
    uuid::Uuid,
};

const LEADER_LEASE_TTL: Duration = Duration::from_secs(60);

#[derive(Serialize, Deserialize)]
enum LeaderCommand {
    Join { lock_key: Vec<u8> },
}

///
/// Cancel safe producer dead signal
struct ProducerDeadSignal {
    // When this object is drop, the sender will drop too and cancel the watch automatically
    _cancel_watcher_tx: oneshot::Sender<()>,
    inner: oneshot::Receiver<()>,
}

impl Future for ProducerDeadSignal {
    type Output = Result<(), RecvError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = &mut self.inner;
        tokio::pin!(inner);
        inner.poll(cx)
    }
}

async fn get_producer_dead_signal(
    mut etcd: etcd_client::Client,
    producer_id: ProducerId,
) -> anyhow::Result<ProducerDeadSignal> {
    let producer_lock_path = get_producer_lock_path_v1(producer_id);
    let (mut watch_handle, mut stream) = etcd
        .watch(
            producer_lock_path.as_bytes(),
            Some(WatchOptions::new().with_prefix()),
        )
        .await?;

    let (tx, rx) = oneshot::channel();
    let get_resp = etcd.get(producer_lock_path.as_bytes(), None).await?;

    let (cancel_watch_tx, cancel_watch_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let _ = cancel_watch_rx.await;
        let _ = watch_handle.cancel().await;
    });

    // If the producer is already dead, we can quit early
    if get_resp.count() == 0 {
        tx.send(())
            .map_err(|_| anyhow::anyhow!("failed to early notify dead producer"))?;
        return Ok(ProducerDeadSignal {
            _cancel_watcher_tx: cancel_watch_tx,
            inner: rx,
        });
    }

    tokio::spawn(async move {
        while let Some(message) = stream
            .message()
            .await
            .expect("watch stream was terminated early")
        {
            let ev_type = message
                .events()
                .first()
                .map(|ev| ev.event_type())
                .expect("watch received a none event");
            match ev_type {
                etcd_client::EventType::Put => {
                    panic!("corrupted system state, producer was created after dead signal")
                }
                etcd_client::EventType::Delete => {
                    if tx.send(()).is_err() {
                        warn!("producer dead signal receiver half was terminated before signal was send");
                    }
                    break;
                }
            }
        }
    });
    Ok(ProducerDeadSignal {
        _cancel_watcher_tx: cancel_watch_tx,
        inner: rx,
    })
}

type EtcdKey = Vec<u8>;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ConsumerGroupState {
    Init(ConsumerGroupHeader),
    LostProducer(LostProducerState),
    WaitingBarrier(WaitingBarrierState),
    ComputingProducerSelection(ConsumerGroupHeader),
    ProducerProposition(ProducerPropositionState),
    ProducerUpdatedAtGroupLevel(ProducerUpdatedAtGroupLevelState),
    Idle(IdleState),
    Dead(ConsumerGroupHeader),
}

impl ConsumerGroupState {
    pub fn header(&self) -> &ConsumerGroupHeader {
        match self {
            ConsumerGroupState::Init(header) => header,
            ConsumerGroupState::LostProducer(x) => &x.header,
            ConsumerGroupState::WaitingBarrier(x) => &x.header,
            ConsumerGroupState::ComputingProducerSelection(header) => header,
            ConsumerGroupState::ProducerProposition(x) => &x.header,
            ConsumerGroupState::ProducerUpdatedAtGroupLevel(x) => &x.header,
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
pub struct ProducerPropositionState {
    pub header: ConsumerGroupHeader,
    pub producer_id: ProducerId,
    pub execution_id: Vec<u8>,
    pub new_shard_offsets: BTreeMap<ShardId, (ShardOffset, Slot)>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ProducerUpdatedAtGroupLevelState {
    pub header: ConsumerGroupHeader,
    pub producer_id: ProducerId,
    pub execution_id: Vec<u8>,
    pub new_shard_offsets: BTreeMap<ShardId, (ShardOffset, Slot)>,
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
    pub shard_assignments: BTreeMap<InstanceId, Vec<ShardId>>,
}

pub struct ConsumerGroupLeaderNode {
    consumer_group_id: ConsumerGroupId,
    etcd: etcd_client::Client,
    leader_key: LeaderKey,
    leader_lease: ManagedLease,
    state_log_key: String,
    state: ConsumerGroupState,
    last_revision: Revision,
    producer_dead_signal: Option<ProducerDeadSignal>,
    barrier: Option<Barrier>,
    consumer_group_store: ConsumerGroupStore,
    producer_queries: ProducerQueries,
}

///
/// This error is raised when there is no active producer for the desired commitment level.
///
#[derive(Copy, Error, PartialEq, Eq, Debug, Clone)]
pub enum LeaderInitError {
    FailedToUpdateStateLog,
}

impl fmt::Display for LeaderInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaderInitError::FailedToUpdateStateLog => f.write_str("FailedToUpdateStateLog"),
        }
    }
}

impl ConsumerGroupLeaderNode {
    pub async fn new(
        mut etcd: etcd_client::Client,
        leader_key: LeaderKey,
        leader_lease: ManagedLease,
        consumer_group_store: ConsumerGroupStore,
        producer_queries: ProducerQueries,
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
            producer_dead_signal: None,
            state,
            last_revision,
            barrier: None,
            consumer_group_store,
            state_log_key,
            producer_queries,
        };
        Ok(ret)
    }

    async fn compute_next_producer(
        &self,
    ) -> anyhow::Result<(ProducerId, ExecutionId, ShardOffsetMap)> {
        let (lcs, _max_revision) = self
            .consumer_group_store
            .get_lowest_common_slot_number(&self.consumer_group_id, Some(self.last_revision))
            .await?;
        let slot_ranges = Some(lcs - 10..=lcs);
        let (producer_id, execution_id) = self
            .producer_queries
            .get_producer_id_with_least_assigned_consumer(
                slot_ranges,
                self.state.header().commitment_level,
            )
            .await?;

        let seek_loc = SeekLocation::SlotApprox {
            desired_slot: lcs,
            min_slot: lcs - 10,
        };
        let shard_offset_map = self
            .producer_queries
            .compute_offset(producer_id, seek_loc, None)
            .await?;

        Ok((producer_id, execution_id, shard_offset_map))
    }

    async fn update_state_machine(&mut self, _next_step: ConsumerGroupState) -> anyhow::Result<()> {
        let leader_log_key = &self.state_log_key;
        let state2 = self.state.clone();
        let txn = etcd_client::Txn::new()
            .when(vec![
                Compare::version(self.leader_key.key(), etcd_client::CompareOp::Greater, 0),
                Compare::mod_revision(
                    leader_log_key.as_str(),
                    etcd_client::CompareOp::Less,
                    self.last_revision,
                ),
            ])
            .and_then(vec![TxnOp::put(
                leader_log_key.as_str(),
                serialize(&state2)?,
                None,
            )]);

        let txn_resp = self.etcd.txn(txn).await?;
        let revision = txn_resp
            .op_responses()
            .pop()
            .and_then(|op| match op {
                etcd_client::TxnOpResponse::Put(put_resp) => {
                    put_resp.header().map(|header| header.revision())
                }
                _ => panic!("unexpected op"),
            })
            .ok_or(LeaderInitError::FailedToUpdateStateLog)?;

        self.last_revision = revision;
        self.state = state2;
        Ok(())
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
            let next_state = match &self.state {
                ConsumerGroupState::Init(header) => {
                    ConsumerGroupState::ComputingProducerSelection(header.clone())
                }
                ConsumerGroupState::LostProducer(LostProducerState {
                    header,
                    lost_producer_id: _,
                    execution_id: _,
                }) => {
                    let barrier_key = Uuid::new_v4();
                    let lease_id = self.etcd.lease_grant(10, None).await?.id();
                    let lock_prefix = get_instance_lock_prefix_v1(self.consumer_group_id.clone());
                    // TODO add healthcheck here
                    let wait_for = self
                        .etcd
                        .get(lock_prefix, Some(GetOptions::new().with_prefix()))
                        .await?
                        .kvs()
                        .iter()
                        .map(|kv| kv.key().to_vec())
                        .collect::<Vec<_>>();

                    let barrier = etcd_utils::barrier::new_barrier(
                        self.etcd.clone(),
                        barrier_key.as_bytes(),
                        &wait_for,
                        lease_id,
                    )
                    .await?;
                    self.barrier = Some(barrier);

                    let next_state = ConsumerGroupState::WaitingBarrier(WaitingBarrierState {
                        header: header.clone(),
                        lease_id,
                        barrier_key: barrier_key.as_bytes().to_vec(),
                        wait_for,
                    });
                    next_state
                }
                ConsumerGroupState::WaitingBarrier(WaitingBarrierState {
                    header,
                    barrier_key,
                    wait_for: _,
                    lease_id: _,
                }) => {
                    let barrier = if let Some(barrier) = self.barrier.take() {
                        barrier
                    } else {
                        get_barrier(self.etcd.clone(), barrier_key).await?
                    };

                    tokio::select! {
                        _ = &mut interrupt_signal => return Ok(()),
                        _ = barrier.wait() => ()
                    }
                    ConsumerGroupState::ComputingProducerSelection(header.clone())
                }
                ConsumerGroupState::ComputingProducerSelection(header) => {
                    let (producer_id, execution_id, new_shard_offsets) =
                        self.compute_next_producer().await?;
                    ConsumerGroupState::ProducerProposition(ProducerPropositionState {
                        header: header.clone(),
                        producer_id,
                        execution_id,
                        new_shard_offsets,
                    })
                }
                ConsumerGroupState::ProducerProposition(ProducerPropositionState {
                    header,
                    producer_id,
                    execution_id,
                    new_shard_offsets: new_shard_offset,
                }) => {
                    let remote_state = self.producer_queries.get_execution_id(*producer_id).await?;
                    match remote_state {
                        Some((_remote_revision, remote_execution_id)) => {
                            if *execution_id != remote_execution_id {
                                ConsumerGroupState::ComputingProducerSelection(header.clone())
                            } else {
                                self.consumer_group_store
                                    .update_consumer_group_producer(
                                        &self.consumer_group_id,
                                        producer_id,
                                        execution_id,
                                        self.last_revision,
                                    )
                                    .await?;

                                ConsumerGroupState::ProducerUpdatedAtGroupLevel(
                                    ProducerUpdatedAtGroupLevelState {
                                        header: header.clone(),
                                        producer_id: *producer_id,
                                        execution_id: execution_id.clone(),
                                        new_shard_offsets: new_shard_offset.clone(),
                                    },
                                )
                            }
                        }
                        None => ConsumerGroupState::ComputingProducerSelection(header.clone()),
                    }
                }
                ConsumerGroupState::ProducerUpdatedAtGroupLevel(
                    ProducerUpdatedAtGroupLevelState {
                        header,
                        producer_id,
                        execution_id,
                        new_shard_offsets: new_shard_offset,
                    },
                ) => {
                    self.consumer_group_store
                        .set_static_group_members_shard_offset(
                            &self.consumer_group_id,
                            producer_id,
                            execution_id,
                            new_shard_offset,
                            self.last_revision,
                        )
                        .await?;
                    ConsumerGroupState::Idle(IdleState {
                        header: header.clone(),
                        producer_id: *producer_id,
                        execution_id: execution_id.clone(),
                    })
                }
                ConsumerGroupState::Idle(IdleState {
                    header,
                    producer_id,
                    execution_id,
                }) => {
                    let signal = self.producer_dead_signal.get_or_insert(
                        get_producer_dead_signal(self.etcd.clone(), *producer_id).await?,
                    );
                    tokio::select! {
                        _ = &mut interrupt_signal => return Ok(()),
                        _ = signal => {
                            warn!("received dead signal from producer {producer_id:?}");
                            let barrier_key = Uuid::new_v4();
                            let lease_id = self.etcd.lease_grant(10, None).await?.id();
                            self.etcd.put(barrier_key.as_bytes(), [], Some(PutOptions::new().with_lease(lease_id))).await?;

                            ConsumerGroupState::LostProducer (LostProducerState {
                                header: header.clone(),
                                lost_producer_id: *producer_id,
                                execution_id: execution_id.clone()
                            })
                        }
                    }
                }
                ConsumerGroupState::Dead(_) => {
                    anyhow::bail!(DeadConsumerGroup(self.consumer_group_id.clone()))
                }
            };

            self.update_state_machine(next_state).await?;

            match interrupt_signal.try_recv() {
                Ok(_) => return Ok(()),
                Err(oneshot::error::TryRecvError::Empty) => continue,
                Err(oneshot::error::TryRecvError::Closed) => return Ok(()),
            }
        }
    }
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
    etcd: &etcd_client::Client,
    scylla_consumer_group_info: &scylladb::types::ConsumerGroupInfo,
) -> anyhow::Result<()> {
    let header = ConsumerGroupHeader {
        consumer_group_id: scylla_consumer_group_info.consumer_group_id.clone(),
        commitment_level: scylla_consumer_group_info.commitment_level,
        subscribed_blockchain_event_types: scylla_consumer_group_info
            .subscribed_event_types
            .clone(),
        shard_assignments: scylla_consumer_group_info
            .instance_id_shard_assignments
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

    let state_log_key =
        leader_log_name_from_cg_id_v1(&scylla_consumer_group_info.consumer_group_id);

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
    etcd: &etcd_client::Client,
    consumer_group_id: ConsumerGroupId,
) -> anyhow::Result<watch::Receiver<(Revision, ConsumerGroupState)>> {
    let key = leader_log_name_from_cg_id_v1(consumer_group_id);
    let mut wc = etcd.watch_client();

    let mut kv_client = etcd.kv_client();
    let get_resp = kv_client.get(consumer_group_id.as_slice(), None).await?;

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
        while let Some(message) = stream
            .message()
            .await
            .expect("leader state log watch error")
        {
            let events = message.events();
            if events.iter().any(|ev| ev.event_type() == EventType::Delete) {
                panic!("remote state log has been deleted")
            }
            events
                .iter()
                .map(|ev| {
                    ev.kv().map(|kv| {
                        (
                            kv.mod_revision(),
                            deserialize::<ConsumerGroupState>(kv.value())
                                .expect("failed to deser consumer group state from leader log"),
                        )
                    })
                })
                .map(|opt| opt.expect("kv from state log is None"))
                .for_each(|(revision, state)| {
                    if tx.send((revision, state)).is_err() {
                        warn!("receiver half of LeaderStateLogObserver has been close");
                    }
                })
        }
        let _ = watcher.cancel().await;
    });

    Ok(rx)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LeaderInfo {
    ipaddr: IpAddr,
    id: Vec<u8>,
}

fn leader_log_name_from_leader_key_v1(lk: &LeaderKey) -> String {
    let lk_name = lk.name_str().expect("invalid leader key name");
    format!("{lk_name}/log",)
}

fn leader_log_name_from_cg_id_v1(consumer_group_id: impl AsRef<[u8]>) -> String {
    format!("{}/log", leader_name_v1(consumer_group_id))
}

fn leader_name_v1(consumer_group_id: impl AsRef<[u8]>) -> String {
    let cg = String::from_utf8_lossy(consumer_group_id.as_ref());
    format!("cg-leader-{cg}")
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
    etcd: &etcd_client::Client,
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
        .map(|kv| deserialize::<LeaderInfo>(kv.value()))
        .transpose()?;

    let (tx, rx) = watch::channel(initital_value);
    tokio::spawn(async move {
        let watch_opts = WatchOptions::new().with_prefix();
        let (mut watcher, mut stream) = wc
            .watch(leader.as_str(), Some(watch_opts))
            .await
            .unwrap_or_else(|_| panic!("fail to watch {leader}"));

        'outer: while let Some(msg) = stream.message().await.expect("") {
            for ev in msg.events() {
                let payload = match ev.event_type() {
                    EventType::Put => ev.kv().map(|kv| {
                        deserialize::<LeaderInfo>(kv.value())
                            .expect("invalid leader state log format")
                    }),
                    EventType::Delete => None,
                };
                if tx.send(payload).is_err() {
                    break 'outer;
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
    wait_for: Duration,
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
    const LEASE_TTL: Duration = Duration::from_secs(60);
    let etcd2 = etcd.clone();
    let lease = ManagedLease::new(etcd2, LEADER_LEASE_TTL, None).await?;
    tokio::select! {
        _ = tokio::time::sleep(wait_for) => {
            return Ok(None)
        },
        Ok(mut campaign_resp) = ec.campaign(leader_name.as_str(), serialize(&leader_info)?, lease.lease_id) => {
            let payload = campaign_resp
                .take_leader()
                .map(|lk| (lk, lease));
            return Ok(payload)
        },
    }
}
