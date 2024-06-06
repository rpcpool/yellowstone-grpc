use {
    super::{
        consumer_group_store::ConsumerGroupStore,
        consumer_source::{ConsumerSource, FromBlockchainEvent},
        leader::{ConsumerGroupState, LeaderInfo, WaitingBarrierState},
        lock::InstanceLock,
        shard_iterator::{ShardFilter, ShardIterator},
    },
    crate::scylladb::{
        etcd_utils::{barrier::release_child, Revision},
        types::InstanceId,
        yellowstone_log::consumer_group::leader::{self, ConsumerGroupStateMachine},
    },
    futures::{future::try_join_all, FutureExt},
    rdkafka::consumer::{self, Consumer},
    scylla::Session,
    std::{f64::consts::E, future, sync::Arc},
    tokio::{
        sync::{mpsc, oneshot, watch},
        task::JoinHandle,
    },
    tracing::{error, info, warn},
};

struct ConsumerSourceSupervisor<T: FromBlockchainEvent> {
    etcd: etcd_client::Client,
    session: Arc<Session>,
    consumer_group_store: ConsumerGroupStore,
    instance_id: InstanceId,
    leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
    leader_info_watch: watch::Receiver<Option<LeaderInfo>>,
    new_tx_filter: Option<ShardFilter>,
    acc_update_filter: Option<ShardFilter>,
    rx_terminate: oneshot::Receiver<()>,
    reusable_sink: mpsc::Sender<T>,
}

struct ConsumerSourceSupervisorHandle {
    tx_terminate: oneshot::Sender<()>,
}

impl<T: FromBlockchainEvent + Send + 'static> ConsumerSourceSupervisor<T> {
    pub async fn spawn(
        &self,
        etcd: etcd_client::Client,
        session: Arc<Session>,
        consumer_group_store: ConsumerGroupStore,
        leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
        leader_info_watch: watch::Receiver<Option<LeaderInfo>>,
        instance_lock: InstanceLock,
        new_tx_filter: Option<ShardFilter>,
        acc_update_filter: Option<ShardFilter>,
        reusable_sink: mpsc::Sender<T>,
    ) -> anyhow::Result<ConsumerSourceSupervisorHandle> {
        let (tx_terminate, rx_terminate) = oneshot::channel();
        let mut supervisor = ConsumerSourceSupervisor {
            etcd,
            session,
            consumer_group_store,
            leader_state_watch,
            leader_info_watch,
            new_tx_filter,
            acc_update_filter,
            rx_terminate,
            reusable_sink,
            instance_id: instance_lock.instance_id.clone(),
        };

        tokio::spawn(async move {
            supervisor.run(instance_lock).await;
        });

        Ok(ConsumerSourceSupervisorHandle { tx_terminate })
    }

    async fn build_consumer_source(
        &mut self,
        state: ConsumerGroupState,
        instance_lock: InstanceLock,
    ) -> anyhow::Result<ConsumerSource<T>> {
        let producer_id = state
            .decided_producer_id()
            .expect("decided producer id must be set");
        let execution_id = state
            .decided_execution_id()
            .expect("decided execution id must be set");

        let mut shard_iterators = Vec::with_capacity(64);
        for ev_type in state.subscribed_blockchain_event_types.iter().cloned() {
            let (_revision, shard_offsets) = self
                .consumer_group_store
                .get_shard_offset(
                    &state.consumer_group_id,
                    &instance_lock.instance_id,
                    &execution_id,
                    ev_type,
                )
                .await?;

            let shard_iterator_subset = try_join_all(shard_offsets.into_iter().map(
                |(shard_id, (offset, _slot))| {
                    ShardIterator::new(
                        Arc::clone(&self.session),
                        producer_id,
                        shard_id,
                        offset,
                        ev_type,
                        self.acc_update_filter.clone(),
                    )
                },
            ))
            .await?;
            shard_iterators.extend(shard_iterator_subset);
        }

        let consumer_source = ConsumerSource::new(
            Arc::clone(&self.session),
            state.consumer_group_id,
            producer_id,
            state.subscribed_blockchain_event_types,
            self.reusable_sink.clone(),
            shard_iterators,
            instance_lock,
            None,
        )
        .await?;
        Ok(consumer_source)
    }

    fn try_become_leader_bg(&self) {
        todo!()
    }

    fn wait_for_state_change(
        &self,
        curr_state: ConsumerGroupState,
    ) -> oneshot::Receiver<ConsumerGroupState> {
        let (tx, rx) = oneshot::channel();
        let mut state_watch = self.leader_state_watch.clone();
        let _h: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let (_revision, new_state) = state_watch
                .wait_for(|(_, new_state)| &curr_state != new_state)
                .await?
                .to_owned();
            tx.send(new_state);
            Ok(())
        });
        rx
    }

    async fn run(&mut self, instance_lock: InstanceLock) -> anyhow::Result<()> {
        let mut instance_lock_holder = Some(instance_lock);
        loop {
            self.leader_state_watch.mark_changed();
            self.leader_info_watch.mark_changed();
            self.leader_info_watch.changed().await?;
            let maybe = self.leader_info_watch.borrow_and_update().to_owned();
            let leader_info = match maybe {
                Some(leader_info) => leader_info,
                None => {
                    self.try_become_leader_bg();
                    self.leader_info_watch
                        .wait_for(Option::is_some)
                        .await?
                        .to_owned()
                        .expect("unexpected None when wait_for LeaderInfo to be defined")
                }
            };

            let (_revision, state) = self
                .leader_state_watch
                .wait_for(|(_revison, state)| state.is_idle())
                .await?
                .to_owned();

            let mut new_state_signal = self.wait_for_state_change(state.clone());
            let (tx_interrupt, rx_interrupt) = oneshot::channel();

            let instance_lock = instance_lock_holder
                .take()
                .expect("instance lock no longer exists");
            let mut consumer_source = self
                .build_consumer_source(state, instance_lock)
                .await
                .expect("failed to build consumer source");

            let mut consumer_fut: JoinHandle<anyhow::Result<InstanceLock>> =
                tokio::spawn(async move {
                    consumer_source.run(rx_interrupt).await?;
                    Ok(consumer_source.take_instance_lock())
                });

            tokio::select! {
                Ok(Err(e)) = &mut consumer_fut => {
                    error!("ConsumerSourceSupervisor failed to run consumer source: {}", e);
                    return Err(e);
                },
                _ = &mut self.rx_terminate => {
                    info!("ConsumerSourceSupervisor received terminate signal");
                    let _ = tx_interrupt.send(());
                    consumer_fut.await??;
                    return Ok(())
                },
                Ok(new_state) = &mut new_state_signal => {
                    info!("ConsumerSourceSupervisor received stop barrier signal");
                    let _ = tx_interrupt.send(());
                    let instance_lock = consumer_fut.await??;
                    instance_lock_holder.replace(instance_lock);

                    match new_state.state_machine {
                        ConsumerGroupStateMachine::WaitingBarrier(WaitingBarrierState { lease_id, barrier_key, wait_for }) => {
                            let is_in_wait_for = wait_for
                                .iter()
                                .any(|instance_id_blob| instance_id_blob.as_slice() == self.instance_id.as_bytes());
                            if is_in_wait_for {
                                release_child(self.etcd.clone(), &barrier_key, &self.instance_id).await?;
                            }
                        },
                        ConsumerGroupStateMachine::Dead => anyhow::bail!("ConsumerSourceSupervisor detected remote consumer group state machine is dead"),
                        _ => (),
                    }
                }
            }
        }
    }
}
