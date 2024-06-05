use {
    super::{
        consumer_group_store::ConsumerGroupStore,
        etcd_path::{get_instance_lock_prefix_v1, get_leader_state_log_key_v1},
        producer_queries::ProducerQueries,
    },
    crate::scylladb::{
        etcd_utils::{
            self,
            barrier::{get_barrier, Barrier},
            lease::ManagedLease,
            Revision,
        },
        types::{
            BlockchainEventType, CommitmentLevel, ConsumerGroupId, ConsumerGroupInfo, ExecutionId,
            InstanceId, ProducerId, ShardId, ShardOffset, ShardOffsetMap, Slot,
        },
        yellowstone_log::{
            common::SeekLocation,
            consumer_group::{
                error::{LeaderStateLogNotFound, StaleConsumerGroup},
                etcd_path::get_producer_lock_path_v1,
            },
        },
    },
    bincode::{deserialize, serialize},
    etcd_client::{
        Client, Compare, EventType, GetOptions, PutOptions, Txn, TxnOp, WatchOptions, Watcher,
    },
    futures::{future, Future, FutureExt},
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

// enum ConsumerGroupLeaderLocation(
//     Local,
//     Remote()
// )

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

#[derive(Serialize, Deserialize, Clone)]
pub enum ConsumerGroupStateMachine {
    Init,
    LostProducer {
        lost_producer_id: ProducerId,
        execution_id: Vec<u8>,
    },
    WaitingBarrier {
        lease_id: i64,
        barrier_key: Vec<u8>,
        wait_for: Vec<Vec<u8>>,
    },
    ComputingProducerSelection,
    ProducerProposition {
        producer_id: ProducerId,
        execution_id: Vec<u8>,
        new_shard_offsets: BTreeMap<ShardId, (ShardOffset, Slot)>,
    },
    ProducerUpdatedAtGroupLevel {
        producer_id: ProducerId,
        execution_id: Vec<u8>,
        new_shard_offsets: BTreeMap<ShardId, (ShardOffset, Slot)>,
    },
    Idle {
        producer_id: ProducerId,
        execution_id: Vec<u8>,
        //initial_shard_offset: BTreeMap<ShardId, (ShardOffset, Slot)>
    },
    Dead,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ConsumerGroupState {
    pub consumer_group_id: ConsumerGroupId,
    pub commitment_level: CommitmentLevel,
    pub subscribed_blockchain_event_types: Vec<BlockchainEventType>,
    pub shard_assignments: BTreeMap<InstanceId, Vec<ShardId>>,
    pub state_machine: ConsumerGroupStateMachine,
}

pub struct ConsumerGroupLeaderNode {
    consumer_group_id: ConsumerGroupId,
    etcd: etcd_client::Client,
    leader_key: EtcdKey,
    leader_lease: ManagedLease,
    state_log_key: EtcdKey,
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

pub async fn setup_new_consumer_group(
    etcd: etcd_client::Client,
    consumer_group_id: ConsumerGroupId,
) {
}

impl ConsumerGroupLeaderNode {
    pub async fn new(
        consumer_group_id: ConsumerGroupId,
        leader_key: EtcdKey,
        leader_lease: ManagedLease,
        state_log_key: Vec<u8>,
        mut etcd: etcd_client::Client,
        consumer_group_store: ConsumerGroupStore,
        producer_queries: ProducerQueries,
    ) -> anyhow::Result<Self> {
        let get_resp = etcd.get(state_log_key.as_slice(), None).await?;
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
            .get_producer_id_with_least_assigned_consumer(slot_ranges, self.state.commitment_level)
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

    async fn update_state_machine(
        &mut self,
        next_step: ConsumerGroupStateMachine,
    ) -> anyhow::Result<()> {
        let leader_log_key = get_leader_state_log_key_v1(&self.consumer_group_id);
        let mut state2 = self.state.clone();
        state2.state_machine = next_step;
        let txn = etcd_client::Txn::new()
            .when(vec![
                Compare::version(
                    self.leader_key.as_slice(),
                    etcd_client::CompareOp::Greater,
                    0,
                ),
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

    pub async fn leader_loop(
        &mut self,
        mut interrupt_signal: oneshot::Receiver<()>,
    ) -> anyhow::Result<()> {
        loop {
            let next_state = match &self.state.state_machine {
                ConsumerGroupStateMachine::Init => {
                    ConsumerGroupStateMachine::ComputingProducerSelection
                }
                ConsumerGroupStateMachine::LostProducer {
                    lost_producer_id,
                    execution_id,
                } => {
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

                    let next_state = ConsumerGroupStateMachine::WaitingBarrier {
                        lease_id,
                        barrier_key: barrier_key.as_bytes().to_vec(),
                        wait_for,
                    };
                    next_state
                }
                ConsumerGroupStateMachine::WaitingBarrier {
                    barrier_key,
                    wait_for,
                    lease_id,
                } => {
                    let barrier = if let Some(barrier) = self.barrier.take() {
                        barrier
                    } else {
                        get_barrier(self.etcd.clone(), &barrier_key).await?
                    };

                    tokio::select! {
                        _ = &mut interrupt_signal => return Ok(()),
                        _ = barrier.wait() => ()
                    }
                    ConsumerGroupStateMachine::ComputingProducerSelection
                }
                ConsumerGroupStateMachine::ComputingProducerSelection => {
                    let (producer_id, execution_id, new_shard_offsets) =
                        self.compute_next_producer().await?;
                    ConsumerGroupStateMachine::ProducerProposition {
                        producer_id,
                        execution_id,
                        new_shard_offsets,
                    }
                }
                ConsumerGroupStateMachine::ProducerProposition {
                    producer_id,
                    execution_id,
                    new_shard_offsets: new_shard_offset,
                } => {
                    let remote_state = self.producer_queries.get_execution_id(*producer_id).await?;
                    match remote_state {
                        Some((_remote_revision, remote_execution_id)) => {
                            if *execution_id != remote_execution_id {
                                ConsumerGroupStateMachine::ComputingProducerSelection
                            } else {
                                self.consumer_group_store
                                    .update_consumer_group_producer(
                                        &self.consumer_group_id,
                                        producer_id,
                                        execution_id,
                                        self.last_revision,
                                    )
                                    .await?;

                                ConsumerGroupStateMachine::ProducerUpdatedAtGroupLevel {
                                    producer_id: *producer_id,
                                    execution_id: execution_id.clone(),
                                    new_shard_offsets: new_shard_offset.clone(),
                                }
                            }
                        }
                        None => ConsumerGroupStateMachine::ComputingProducerSelection,
                    }
                }
                ConsumerGroupStateMachine::ProducerUpdatedAtGroupLevel {
                    producer_id,
                    execution_id,
                    new_shard_offsets: new_shard_offset,
                } => {
                    self.consumer_group_store
                        .set_static_group_members_shard_offset(
                            &self.consumer_group_id,
                            producer_id,
                            execution_id,
                            new_shard_offset,
                            self.last_revision,
                        )
                        .await?;
                    ConsumerGroupStateMachine::Idle {
                        producer_id: *producer_id,
                        execution_id: execution_id.clone(),
                    }
                }
                ConsumerGroupStateMachine::Idle {
                    producer_id,
                    execution_id,
                } => {
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

                            ConsumerGroupStateMachine::LostProducer {
                                lost_producer_id: *producer_id,
                                execution_id: execution_id.clone()
                            }
                        }
                    }
                }
                ConsumerGroupStateMachine::Dead => {
                    anyhow::bail!(StaleConsumerGroup(self.consumer_group_id.clone()))
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

pub async fn create_leader_state_log(
    etcd: &etcd_client::Client,
    consumer_group_info: &ConsumerGroupInfo,
) -> anyhow::Result<()> {
    let state = ConsumerGroupState {
        consumer_group_id: consumer_group_info.consumer_group_id.clone(),
        commitment_level: consumer_group_info.commitment_level.clone(),
        subscribed_blockchain_event_types: consumer_group_info.subscribed_event_types.clone(),
        shard_assignments: consumer_group_info.instance_id_shard_assignments.clone(),
        state_machine: ConsumerGroupStateMachine::Idle {
            producer_id: consumer_group_info
                .producer_id
                .clone()
                .expect("missing producer id"),
            execution_id: consumer_group_info
                .execution_id
                .clone()
                .expect("missing execution id"),
        },
    };

    let state_log_key = get_leader_state_log_key_v1(&consumer_group_info.consumer_group_id);

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

pub struct LeaderStateLogWatch {
    etcd_watcher: Watcher,
    rx: watch::Receiver<(Revision, ConsumerGroupState)>,
}

impl LeaderStateLogWatch {
    pub async fn new(
        etcd: etcd_client::Client,
        consumer_group_id: ConsumerGroupId,
    ) -> anyhow::Result<Self> {
        let key = get_leader_state_log_key_v1(&consumer_group_id);
        let mut wc = etcd.watch_client();
        let (watcher, mut stream) = wc.watch(key, None).await?;

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
            while let Some(message) = stream
                .message()
                .await
                .expect("leader state log watch error")
            {
                let events = message.events();
                if events
                    .iter()
                    .find(|ev| ev.event_type() == EventType::Delete)
                    .is_some()
                {
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
                            return;
                        }
                    })
            }
        });

        let ret = LeaderStateLogWatch {
            etcd_watcher: watcher,
            rx,
        };

        Ok(ret)
    }

    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.rx.changed().await
    }

    pub async fn own_and_update(&mut self) -> (Revision, ConsumerGroupState) {
        self.rx.borrow_and_update().to_owned()
    }
}

#[derive(Serialize, Deserialize)]
pub struct LeaderInfo {
    ipaddr: IpAddr,
    id: Vec<u8>,
}

fn leader_name_v1(consumer_group_id: &ConsumerGroupId) -> String {
    let cg = String::from_utf8_lossy(&consumer_group_id.as_slice());
    format!("cg-leader-{cg}")
}

pub async fn observe_leader_changes(
    etcd: &etcd_client::Client,
    consumer_group_id: &ConsumerGroupId,
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
            .expect(format!("fail to watch {leader}").as_str());

        'outer: while let Some(mut msg) = stream.message().await.expect("") {
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
