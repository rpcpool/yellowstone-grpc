use {
    super::{
        consumer_group_store::ScyllaConsumerGroupStore,
        consumer_source::ConsumerSourceHandle,
        context::ConsumerContext,
        leader::{ConsumerGroupState, IdleState, LeaderInfo, WaitingBarrierState},
        lock::ConsumerLock,
    },
    crate::scylladb::{
        etcd_utils::{barrier::release_child, Revision},
        types::{ConsumerGroupId, ConsumerId},
        yellowstone_log::consumer_group::error::DeadConsumerGroup,
    },
    futures::{future::BoxFuture, Future, FutureExt},
    scylla::Session,
    std::{convert::identity, sync::Arc, time::Duration},
    tokio::{
        sync::{oneshot, watch},
        task::JoinHandle,
        time::Instant,
    },
    tonic::async_trait,
    tracing::{error, info, warn},
    uuid::Uuid,
};

const CONSUMER_SOURCE_SPAWN_TIMEOUT: Duration = Duration::from_secs(5);
const LEADER_IDLE_STATE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ConsumerSourceSupervisor {
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
    consumer_lock: ConsumerLock,
    etcd: etcd_client::Client,
    session: Arc<Session>,
    consumer_group_store: ScyllaConsumerGroupStore,
    leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
    leader_info_watch: watch::Receiver<Option<LeaderInfo>>,
}

struct InnerSupervisor<F: ConsumerSourceSpawner> {
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
    consumer_lock: ConsumerLock,
    etcd: etcd_client::Client,
    session: Arc<Session>,
    consumer_group_store: ScyllaConsumerGroupStore,
    leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
    leader_info_watch: watch::Receiver<Option<LeaderInfo>>,
    consumer_factory: F,
}

impl<F> InnerSupervisor<F>
where
    F: ConsumerSourceSpawner,
{
    fn wait_for_state_change(
        &mut self,
        current_revision: Revision,
    ) -> impl Future<Output = anyhow::Result<(Revision, ConsumerGroupState)>> {
        let mut state_watch = self.leader_state_watch.clone();
        let h = tokio::spawn(async move {
            let (revision, new_state) = state_watch
                .wait_for(|(revision, _)| *revision > current_revision)
                .await?
                .to_owned();
            Ok((revision, new_state))
        });
        h.map(|result| result.map_err(anyhow::Error::new).and_then(identity))
    }

    async fn handle_wait_barrier(&mut self, state: &WaitingBarrierState) -> anyhow::Result<()> {
        let is_in_wait_for = state
            .wait_for
            .iter()
            .any(|instance_id_blob| instance_id_blob.as_slice() == self.consumer_id.as_bytes());
        if is_in_wait_for {
            info!("is in wait barrier");
            release_child(self.etcd.clone(), &state.barrier_key, &self.consumer_id).await
        } else {
            info!("is not in wait barrier");
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
            self.leader_info_watch.mark_changed();
            self.leader_state_watch.mark_changed();

            let leader_info = tokio::time::timeout(
                LEADER_IDLE_STATE_TIMEOUT,
                self.leader_info_watch.wait_for(Option::is_some)
            ).await??.to_owned().expect("leader info is none");

            info!("supervisor detected current leader info to be: {leader_info:?}");

            let (revision, idle_state) =
                tokio::time::timeout(LEADER_IDLE_STATE_TIMEOUT, self.wait_for_idle_state()).await??;

            info!("ConsumerSourceSupervisor received idle state, revision: {revision}");

            let ctx = self.build_consumer_context(idle_state);
            let mut new_state_signal = self.wait_for_state_change(revision);

            let spawned_time = Instant::now();
            info!("ConsumerSourceSupervisor spawning consumer source...");
            // TODO : add timeout for source spawn
            let spawn_result = tokio::time::timeout(
                CONSUMER_SOURCE_SPAWN_TIMEOUT,
                self.consumer_factory.spawn(ctx),
            )
            .await;
            let spawn_result =
                spawn_result.expect("failed to spawn consumer source before timeout");

            let mut consumer_source_handle = spawn_result?;

            let cg_id_text = Uuid::from_bytes(self.consumer_group_id).to_string();
            info!(
                "ConsumerSourceSupervisor spawned a consumer source in {:?}, group_id={} consumer_id={}", 
                spawned_time.elapsed(), 
                cg_id_text, 
                self.consumer_id
            );
            tokio::select! {
                result = &mut consumer_source_handle => {
                    warn!("consumer source handle terminated");
                    match result {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            error!("ConsumerSourceSupervisor failed to run consumer source: {}", e);
                            anyhow::bail!(e);
                        },
                    }
                },
                _ = &mut stop_signal => {
                    info!("ConsumerSourceSupervisor received terminate signal");
                    //consumer_source_handle.send(ConsumerSourceCommand::Stop).await?;
                    consumer_source_handle.gracefully_shutdown().await?;
                    //consumer_source_handle.await??;
                    return Ok(())
                },
                result = &mut new_state_signal => {
                    let (revision, new_state) = result?;
                    info!("ConsumerSourceSupervisor received new state with revision {revision}");
                    //let result = consumer_source_handle.send(ConsumerSourceCommand::Stop).await?;
                    let result = tokio::time::timeout(Duration::from_secs(2), consumer_source_handle.gracefully_shutdown()).await??;
                    info!("ConsumerSourceSupervisor result {result:?}");
                    //consumer_source_handle.await??;
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
    type Output = anyhow::Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let handle = &mut self.handle;
        tokio::pin!(handle);
        handle.poll(cx).map(|result| {
            result
                .map_err(anyhow::Error::new)
                .and_then(|subresult| subresult)
        })
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
            consumer_factory: factory,
            leader_info_watch: self.leader_info_watch,
        }
    }

    pub fn new(
        consumer_lock: ConsumerLock,
        etcd: etcd_client::Client,
        session: Arc<Session>,
        consumer_group_store: ScyllaConsumerGroupStore,
        leader_state_watch: watch::Receiver<(Revision, ConsumerGroupState)>,
        leader_info_watch: watch::Receiver<Option<LeaderInfo>>,
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
            leader_info_watch,
        }
    }
}
