use {
    super::{
        consumer_group_store::ScyllaConsumerGroupStore,
        consumer_source::{ConsumerSource, ConsumerSourceHandle, FromBlockchainEvent},
        context::ConsumerContext,
        leader::{ConsumerGroupState, IdleState, WaitingBarrierState},
        lock::ConsumerLock,
        shard_iterator::{ShardFilter, ShardIterator},
    },
    crate::scylladb::{
        etcd_utils::{barrier::release_child, Revision},
        types::{ConsumerGroupId, ConsumerId},
        yellowstone_log::consumer_group::{
            consumer_source::ConsumerSourceCommand, error::DeadConsumerGroup,
        },
    },
    futures::{
        future::{try_join_all, BoxFuture},
        Future,
    },
    rdkafka::consumer::Consumer,
    scylla::Session,
    std::{
        collections::{BTreeMap, BTreeSet},
        process::Output,
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::{mpsc, oneshot, watch},
        task::{JoinError, JoinHandle},
        time::Instant,
    },
    tonic::async_trait,
    tracing::{error, info},
    uuid::Uuid,
};

pub struct ConsumerSourceSupervisor {
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
    consumer_lock: ConsumerLock,
    etcd: etcd_client::Client,
    session: Arc<Session>,
    consumer_group_store: ScyllaConsumerGroupStore,
    leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
}

struct InnerSupervisor<F: ConsumerSourceSpawner> {
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
    consumer_lock: ConsumerLock,
    etcd: etcd_client::Client,
    session: Arc<Session>,
    consumer_group_store: ScyllaConsumerGroupStore,
    leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
    factory: F,
}

impl<F> InnerSupervisor<F>
where
    F: ConsumerSourceSpawner,
{
    fn wait_for_state_change(
        &mut self,
        current_revision: Revision,
    ) -> JoinHandle<anyhow::Result<(Revision, ConsumerGroupState)>> {
        let mut state_watch = self.leader_state_watch.clone();
        tokio::spawn(async move {
            let (revision, new_state) = state_watch
                .wait_for(|(revision, _)| *revision > current_revision)
                .await?
                .to_owned();
            Ok((revision, new_state))
        })
    }

    async fn handle_wait_barrier(&mut self, state: &WaitingBarrierState) -> anyhow::Result<()> {
        let is_in_wait_for = state
            .wait_for
            .iter()
            .any(|instance_id_blob| instance_id_blob.as_slice() == self.consumer_id.as_bytes());
        if is_in_wait_for {
            release_child(self.etcd.clone(), &state.barrier_key, &self.consumer_id).await
        } else {
            Ok(())
        }
    }

    async fn wait_for_idle_state(&mut self) -> anyhow::Result<(Revision, IdleState)> {
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
                    anyhow::bail!(DeadConsumerGroup(self.consumer_group_id.clone()))
                }
                _ => continue,
            }
        }
    }

    fn build_consumer_context(&self, state: IdleState) -> ConsumerContext {
        ConsumerContext {
            consumer_group_id: state.header.consumer_group_id,
            consumer_id: self.consumer_id.clone(),
            producer_id: state.producer_id,
            execution_id: state.execution_id,
            subscribed_event_types: state.header.subscribed_blockchain_event_types,
            session: Arc::clone(&self.session),
            etcd: self.etcd.clone(),
            consumer_group_store: self.consumer_group_store.clone(),
            fencing_token_generator: self.consumer_lock.get_fencing_token_gen(),
        }
    }

    async fn run(&mut self, mut stop_signal: oneshot::Receiver<()>) -> anyhow::Result<()> {
        loop {
            info!("in supervisor");
            self.leader_state_watch.mark_changed();
            let result =
                tokio::time::timeout(Duration::from_secs(60), self.wait_for_idle_state()).await?;
            if let Err(result) = result {
                error!(
                    "ConsumerSourceSupervisor timeout while waiting for idle state: {}",
                    result
                );
                return Err(result);
            }

            let (revision, idle_state) = result?;
            info!("ConsumerSourceSupervisor received idle state, revision: {revision}");

            let ctx = self.build_consumer_context(idle_state);
            let mut new_state_signal = self.wait_for_state_change(revision);

            let spawned_time = Instant::now();
            info!("ConsumerSourceSupervisor spawning consumer source...");
            // TODO : add timeout for source spawn
            let mut handle = self.factory.spawn(ctx).await?;

            let cg_id_text = Uuid::from_bytes(self.consumer_group_id).to_string();
            info!(
                "ConsumerSourceSupervisor spawned a consumer source in {:?}, group_id={} consumer_id={}", 
                spawned_time.elapsed(), 
                cg_id_text, 
                self.consumer_id
            );
            tokio::select! {
                Ok(Err(e)) = &mut handle => {
                    error!("ConsumerSourceSupervisor failed to run consumer source: {}", e);
                    return Err(e);
                },
                _ = &mut stop_signal => {
                    info!("ConsumerSourceSupervisor received terminate signal");
                    handle.send(ConsumerSourceCommand::Stop).await?;
                    handle.await??;
                    return Ok(())
                },
                Ok(Ok((revision, new_state))) = &mut new_state_signal => {
                    info!("ConsumerSourceSupervisor received new state with revision {revision}");
                    handle.send(ConsumerSourceCommand::Stop).await?;
                    handle.await??;
                    match new_state {
                        ConsumerGroupState::WaitingBarrier(wait_barrier_state) => {
                            self.handle_wait_barrier(&wait_barrier_state).await?;
                        },
                        ConsumerGroupState::Dead(_) => anyhow::bail!(DeadConsumerGroup(self.consumer_group_id.clone())),
                        _ => (),
                    };
                },
            }
        }
    }
}

pub struct ConsumerSourceSupervisorHandle {
    pub consumer_group_id: ConsumerGroupId,
    pub consumer_id: ConsumerId,
    tx_terminate: oneshot::Sender<()>,
    handle: JoinHandle<anyhow::Result<()>>,
}

impl Drop for ConsumerSourceSupervisorHandle {
    fn drop(&mut self) {
        info!("dropped handle");
    }
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

#[async_trait]
pub trait ConsumerSourceSpawner {
    async fn spawn(&self, ctx: ConsumerContext) -> anyhow::Result<ConsumerSourceHandle>;
}

struct ConsumerSourceSpawnerFn<F> {
    inner: F,
}

impl<F> ConsumerSourceSpawner for ConsumerSourceSpawnerFn<F>
where
    F: Fn(ConsumerContext) -> BoxFuture<'static, anyhow::Result<ConsumerSourceHandle>>
        + Send
        + 'static,
{
    fn spawn<'a, 'b>(
        &'a self,
        ctx: ConsumerContext,
    ) -> BoxFuture<'b, anyhow::Result<ConsumerSourceHandle>>
    where
        'a: 'b,
        Self: 'b,
    {
        (self.inner)(ctx)
    }
}

impl ConsumerSourceSupervisor {
    pub async fn spawn<F: ConsumerSourceSpawner + Send + 'static>(
        mut self,
        factory: F,
    ) -> anyhow::Result<ConsumerSourceSupervisorHandle> {
        let (tx_terminate, rx_terminate) = oneshot::channel();
        let consumer_group_id = self.consumer_group_id.clone();
        let consumer_id = self.consumer_id.clone();

        let mut inner = self.into_inner(factory);
        let handle: JoinHandle<Result<(), anyhow::Error>> =
            tokio::spawn(async move { inner.run(rx_terminate).await });
        Ok(ConsumerSourceSupervisorHandle {
            consumer_id,
            consumer_group_id,
            tx_terminate,
            handle,
        })
    }

    pub async fn spawn_with<F>(self, callable: F) -> anyhow::Result<ConsumerSourceSupervisorHandle>
    where
        F: Fn(ConsumerContext) -> BoxFuture<'static, anyhow::Result<ConsumerSourceHandle>>
            + Send
            + 'static,
    {
        let factory = ConsumerSourceSpawnerFn { inner: callable };
        ConsumerSourceSupervisor::spawn(self, factory).await
    }

    fn into_inner<F: ConsumerSourceSpawner>(self, factory: F) -> InnerSupervisor<F> {
        InnerSupervisor {
            consumer_group_id: self.consumer_group_id,
            consumer_id: self.consumer_id,
            consumer_lock: self.consumer_lock,
            etcd: self.etcd,
            session: self.session,
            consumer_group_store: self.consumer_group_store,
            leader_state_watch: self.leader_state_watch,
            factory: factory,
        }
    }

    pub fn new(
        consumer_lock: ConsumerLock,
        etcd: etcd_client::Client,
        session: Arc<Session>,
        consumer_group_store: ScyllaConsumerGroupStore,
        leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
    ) -> Self {
        let consumer_id = consumer_lock.consumer_id.clone();
        let consumer_group_id = consumer_lock.consumer_group_id.clone();
        ConsumerSourceSupervisor {
            consumer_group_id,
            consumer_id: consumer_id.clone(),
            consumer_lock,
            session,
            etcd,
            consumer_group_store,
            leader_state_watch,
        }
    }
}
