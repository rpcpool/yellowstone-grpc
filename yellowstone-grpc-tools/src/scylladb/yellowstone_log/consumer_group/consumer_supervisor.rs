use {
    super::{
        consumer_group_store::ConsumerGroupStore,
        consumer_source::{ConsumerSource, FromBlockchainEvent},
        leader::{ConsumerGroupState, IdleState, WaitingBarrierState},
        lock::InstanceLock,
        shard_iterator::{ShardFilter, ShardIterator},
    },
    crate::scylladb::{
        etcd_utils::{barrier::release_child, Revision},
        types::{ConsumerGroupId, InstanceId},
        yellowstone_log::consumer_group::error::DeadConsumerGroup,
    },
    futures::{future::try_join_all, Future},
    scylla::Session,
    std::sync::Arc,
    tokio::{
        sync::{mpsc, oneshot, watch},
        task::{JoinError, JoinHandle},
    },
    tracing::{error, info},
};

pub struct ConsumerSourceSupervisor<T: FromBlockchainEvent> {
    etcd: etcd_client::Client,
    session: Arc<Session>,
    consumer_groupd_id: ConsumerGroupId,
    consumer_group_store: ConsumerGroupStore,
    instance_id: InstanceId,
    leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
    new_tx_filter: Option<ShardFilter>,
    acc_update_filter: Option<ShardFilter>,
    rx_terminate: oneshot::Receiver<()>,
    reusable_sink: mpsc::Sender<T>,
}

pub struct ConsumerSourceSupervisorHandle {
    pub consumer_group_id: ConsumerGroupId,
    pub instance_id: InstanceId,
    tx_terminate: oneshot::Sender<()>,
    handle: JoinHandle<anyhow::Result<()>>,
}

impl Future for ConsumerSourceSupervisorHandle {
    type Output = Result<anyhow::Result<()>, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let handle = &mut self.handle;
        tokio::pin!(handle);
        handle.poll(cx)
    }
}

impl<T: FromBlockchainEvent + Send + 'static> ConsumerSourceSupervisor<T> {
    pub async fn spawn(
        etcd: etcd_client::Client,
        session: Arc<Session>,
        consumer_group_store: ConsumerGroupStore,
        leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
        instance_lock: InstanceLock,
        new_tx_filter: Option<ShardFilter>,
        acc_update_filter: Option<ShardFilter>,
        reusable_sink: mpsc::Sender<T>,
    ) -> anyhow::Result<ConsumerSourceSupervisorHandle> {
        let (tx_terminate, rx_terminate) = oneshot::channel();

        let instance_id = instance_lock.instance_id.clone();
        let consumer_group_id = instance_lock.consumer_group_id.clone();
        let mut supervisor = ConsumerSourceSupervisor {
            etcd,
            session,
            consumer_group_store,
            leader_state_watch,
            new_tx_filter,
            acc_update_filter,
            rx_terminate,
            reusable_sink,
            instance_id: instance_id.clone(),
            consumer_groupd_id: consumer_group_id.clone(),
        };

        let handle = tokio::spawn(async move { supervisor.run(instance_lock).await });

        Ok(ConsumerSourceSupervisorHandle {
            instance_id,
            consumer_group_id,
            tx_terminate,
            handle,
        })
    }

    async fn build_consumer_source(
        &mut self,
        state: IdleState,
        instance_lock: InstanceLock,
    ) -> anyhow::Result<ConsumerSource<T>> {
        let mut shard_iterators = Vec::with_capacity(64);
        for ev_type in state
            .header
            .subscribed_blockchain_event_types
            .iter()
            .cloned()
        {
            let (_revision, shard_offsets) = self
                .consumer_group_store
                .get_shard_offset(
                    &self.consumer_groupd_id,
                    &self.instance_id,
                    &state.execution_id,
                    ev_type,
                )
                .await?;

            let shard_iterator_subset = try_join_all(shard_offsets.into_iter().map(
                |(shard_id, (offset, _slot))| {
                    ShardIterator::new(
                        Arc::clone(&self.session),
                        state.producer_id,
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
            self.consumer_groupd_id.clone(),
            state.producer_id,
            state.execution_id,
            state.header.subscribed_blockchain_event_types.clone(),
            self.reusable_sink.clone(),
            shard_iterators,
            instance_lock,
            None,
        )
        .await?;
        Ok(consumer_source)
    }

    fn wait_for_state_change(
        &self,
        current_revision: Revision,
    ) -> oneshot::Receiver<(Revision, ConsumerGroupState)> {
        let (tx, rx) = oneshot::channel();
        let mut state_watch = self.leader_state_watch.clone();
        let _h: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let (revision, new_state) = state_watch
                .wait_for(|(revision, _)| *revision > current_revision)
                .await?
                .to_owned();
            let _ = tx.send((revision, new_state));
            Ok(())
        });
        rx
    }

    async fn handle_wait_barrier(&self, state: &WaitingBarrierState) -> anyhow::Result<()> {
        let is_in_wait_for = state
            .wait_for
            .iter()
            .any(|instance_id_blob| instance_id_blob.as_slice() == self.instance_id.as_bytes());
        if is_in_wait_for {
            release_child(self.etcd.clone(), &state.barrier_key, &self.instance_id).await
        } else {
            Ok(())
        }
    }

    async fn wait_for_idle_state(&self) -> anyhow::Result<(Revision, IdleState)> {
        let mut state_watch = self.leader_state_watch.clone();

        state_watch.mark_changed();
        loop {
            state_watch.changed().await?;

            let (revision, state) = state_watch.borrow_and_update().to_owned();

            match state {
                ConsumerGroupState::WaitingBarrier(waiting_barrier_state) => {
                    self.handle_wait_barrier(&waiting_barrier_state).await?
                }
                ConsumerGroupState::Idle(idle_state) => return Ok((revision, idle_state)),
                ConsumerGroupState::Dead(_) => {
                    anyhow::bail!(DeadConsumerGroup(self.consumer_groupd_id.clone()))
                }
                _ => continue,
            }
        }
    }

    async fn run(&mut self, instance_lock: InstanceLock) -> anyhow::Result<()> {
        let mut instance_lock_holder = Some(instance_lock);
        loop {
            self.leader_state_watch.mark_changed();

            let (revision, idle_state) = self.wait_for_idle_state().await?;

            let mut new_state_signal = self.wait_for_state_change(revision);
            let (tx_interrupt, rx_interrupt) = oneshot::channel();

            let instance_lock = instance_lock_holder
                .take()
                .expect("instance lock no longer exists");

            let mut consumer_source = self
                .build_consumer_source(idle_state, instance_lock)
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
                Ok((revision, new_state)) = &mut new_state_signal => {
                    info!("ConsumerSourceSupervisor received new state with revision {revision}");
                    let _ = tx_interrupt.send(());
                    let instance_lock = consumer_fut.await??;
                    instance_lock_holder.replace(instance_lock);
                    match new_state {
                        ConsumerGroupState::WaitingBarrier(wait_barrier_state) => {
                            self.handle_wait_barrier(&wait_barrier_state).await?;
                        },
                        ConsumerGroupState::Dead(_) => anyhow::bail!(DeadConsumerGroup(self.consumer_groupd_id.clone())),
                        _ => (),
                    };
                },
            }
        }
    }
}
