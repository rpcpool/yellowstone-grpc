use {
    super::{
        consumer_group_store::ConsumerGroupStore,
        consumer_source::{ConsumerSource, FromBlockchainEvent, InterruptSignal},
        context::ConsumerContext,
        leader::{ConsumerGroupState, IdleState, WaitingBarrierState},
        lock::ConsumerLock,
        shard_iterator::{ShardFilter, ShardIterator},
    },
    crate::scylladb::{
        etcd_utils::{barrier::release_child, Revision},
        types::{ConsumerGroupId, ConsumerId},
        yellowstone_log::consumer_group::error::DeadConsumerGroup,
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
    },
    tokio::{
        sync::{mpsc, oneshot, watch},
        task::{JoinError, JoinHandle},
    },
    tonic::async_trait,
    tracing::{error, info},
};

pub struct ConsumerSourceSupervisor {
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
    consumer_lock: ConsumerLock,
    etcd: etcd_client::Client,
    session: Arc<Session>,
    consumer_group_store: ConsumerGroupStore,
    leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
}

struct InnerSupervisor<F: ConsumerSourceFactory> {
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
    consumer_lock: ConsumerLock,
    etcd: etcd_client::Client,
    session: Arc<Session>,
    consumer_group_store: ConsumerGroupStore,
    leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
    factory: F,
}

impl<F> InnerSupervisor<F>
where
    F: ConsumerSourceFactory,
{
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
            .any(|instance_id_blob| instance_id_blob.as_slice() == self.consumer_id.as_bytes());
        if is_in_wait_for {
            release_child(self.etcd.clone(), &state.barrier_key, &self.consumer_id).await
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

    async fn run(&mut self, mut stop_signal: InterruptSignal) -> anyhow::Result<()> {
        loop {
            self.leader_state_watch.mark_changed();

            let (revision, idle_state) = self.wait_for_idle_state().await?;

            let ctx = self.build_consumer_context(idle_state);
            let mut new_state_signal = self.wait_for_state_change(revision);
            let (tx_interrupt, rx_interrupt) = oneshot::channel();

            let mut consumer_source = self.factory.build(ctx).await?;

            let mut consumer_fut =
                tokio::spawn(async move { consumer_source.run(rx_interrupt).await });

            tokio::select! {
                Ok(Err(e)) = &mut consumer_fut => {
                    error!("ConsumerSourceSupervisor failed to run consumer source: {}", e);
                    return Err(e);
                },
                _ = &mut stop_signal => {
                    info!("ConsumerSourceSupervisor received terminate signal");
                    let _ = tx_interrupt.send(());
                    consumer_fut.await??;
                    return Ok(())
                },
                Ok((revision, new_state)) = &mut new_state_signal => {
                    info!("ConsumerSourceSupervisor received new state with revision {revision}");
                    let _ = tx_interrupt.send(());
                    consumer_fut.await??;
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
trait ConsumerSourceFactory: Send + Sync + 'static {
    type ConsumerOutput: FromBlockchainEvent + Send + 'static;

    async fn build(
        &self,
        ctx: ConsumerContext,
    ) -> anyhow::Result<ConsumerSource<Self::ConsumerOutput>>;
}

struct ConsumerSourceFactoryFn<F, O>
where
    O: FromBlockchainEvent + Send + 'static,
    F: Fn(ConsumerContext) -> BoxFuture<'static, anyhow::Result<ConsumerSource<O>>>
        + Send
        + 'static,
{
    inner: F,
}

impl<F, O> ConsumerSourceFactory for ConsumerSourceFactoryFn<F, O>
where
    O: FromBlockchainEvent + Send + 'static,
    F: Fn(ConsumerContext) -> BoxFuture<'static, anyhow::Result<ConsumerSource<O>>>
        + Send
        + Sync
        + 'static,
{
    type ConsumerOutput = O;

    fn build<'a, 'b>(
        &'a self,
        ctx: ConsumerContext,
    ) -> BoxFuture<'b, anyhow::Result<ConsumerSource<Self::ConsumerOutput>>>
    where
        'a: 'b,
        Self: 'b,
    {
        (self.inner)(ctx)
    }
}

impl ConsumerSourceSupervisor {
    pub async fn spawn<F: ConsumerSourceFactory + Send + 'static>(
        mut self,
        factory: F,
    ) -> anyhow::Result<ConsumerSourceSupervisorHandle> {
        let (tx_terminate, rx_terminate) = oneshot::channel();
        let consumer_group_id = self.consumer_group_id.clone();
        let consumer_id = self.consumer_id.clone();

        let mut inner = self.into_inner(factory);
        let handle = tokio::spawn(async move { inner.run(rx_terminate).await });
        Ok(ConsumerSourceSupervisorHandle {
            consumer_id,
            consumer_group_id,
            tx_terminate,
            handle,
        })
    }

    pub async fn spawn_with<O, F>(
        mut self,
        callable: F,
    ) -> anyhow::Result<ConsumerSourceSupervisorHandle>
    where
        O: FromBlockchainEvent + Send + 'static,
        F: Fn(ConsumerContext) -> BoxFuture<'static, anyhow::Result<ConsumerSource<O>>>
            + Send
            + Sync
            + 'static,
    {
        let factory = ConsumerSourceFactoryFn { inner: callable };
        ConsumerSourceSupervisor::spawn(self, factory).await
    }

    fn into_inner<F: ConsumerSourceFactory>(self, factory: F) -> InnerSupervisor<F> {
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
        consumer_group_store: ConsumerGroupStore,
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
