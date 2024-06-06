use {
    super::{
        consumer_group_store::ConsumerGroupStore,
        consumer_source::FromBlockchainEvent,
        consumer_supervisor::{ConsumerSourceSupervisor, ConsumerSourceSupervisorHandle},
        leader::{
            observe_consumer_group_state, observe_leader_changes, try_become_leader,
            ConsumerGroupState, LeaderInfo,
        },
        lock::InstanceLocker,
        shard_iterator::ShardFilter,
    },
    crate::scylladb::{
        etcd_utils::Revision,
        types::{ConsumerGroupId, InstanceId},
    },
    scylla::Session,
    std::{borrow::BorrowMut, collections::HashMap, sync::Arc, time::Duration},
    tokio::sync::{mpsc, watch, Mutex},
    tracing::info,
};

struct Inner {
    leader_election_watch_map: HashMap<ConsumerGroupId, watch::Receiver<Option<LeaderInfo>>>,
    leader_state_watch_map:
        HashMap<ConsumerGroupId, watch::Receiver<(Revision, ConsumerGroupState)>>,
}

pub struct LocalConsumerGroupCoordinator {
    etcd: etcd_client::Client,
    session: Arc<Session>,
    instance_lock: InstanceLocker,
    consumer_group_store: ConsumerGroupStore,
    inner: Arc<Mutex<Inner>>,
    leader_ifname: String,
}

impl LocalConsumerGroupCoordinator {
    async fn try_become_leader(&self, consumer_group_id: &ConsumerGroupId) -> anyhow::Result<()> {
        let mut rx = try_become_leader(
            self.etcd.clone(),
            consumer_group_id,
            Duration::from_secs(5),
            &self.leader_ifname,
        )?;
        //rx.await
        Ok(())
    }

    async fn get_leader_state_watch(
        &self,
        consumer_group_id: &ConsumerGroupId,
    ) -> anyhow::Result<watch::Receiver<(i64, ConsumerGroupState)>> {
        let mut inner = self.inner.lock().await;
        match inner
            .borrow_mut()
            .leader_state_watch_map
            .entry(consumer_group_id.clone())
        {
            std::collections::hash_map::Entry::Occupied(o) => Ok(o.get().clone()),
            std::collections::hash_map::Entry::Vacant(v) => {
                let watch = observe_consumer_group_state(&self.etcd, consumer_group_id).await?;
                Ok(v.insert(watch).clone())
            }
        }
    }

    async fn get_leader_election_watch(
        &self,
        consumer_group_id: &ConsumerGroupId,
    ) -> anyhow::Result<watch::Receiver<Option<LeaderInfo>>> {
        let mut inner = self.inner.lock().await;
        match inner
            .borrow_mut()
            .leader_election_watch_map
            .entry(consumer_group_id.clone())
        {
            std::collections::hash_map::Entry::Occupied(o) => Ok(o.get().clone()),
            std::collections::hash_map::Entry::Vacant(v) => {
                let watch = observe_leader_changes(&self.etcd, consumer_group_id).await?;
                Ok(v.insert(watch).clone())
            }
        }
    }

    async fn register_consumer_handle(&self, consumer_handle: ConsumerSourceSupervisorHandle) {}

    pub async fn try_spawn_consumer_member<T: FromBlockchainEvent + Send + 'static>(
        &self,
        consumer_group_id: &ConsumerGroupId,
        instance_id: &InstanceId,
        sender: mpsc::Sender<T>,
        acc_update_filter: Option<ShardFilter>,
        new_tx_filter: Option<ShardFilter>,
    ) -> anyhow::Result<()> {
        let instance_lock = self
            .instance_lock
            .try_lock_instance_id(consumer_group_id.as_slice(), instance_id)
            .await?;

        let mut leader_election_watch = self.get_leader_election_watch(&consumer_group_id).await?;

        let mut leader_state_watch = self.get_leader_state_watch(&consumer_group_id).await?;

        leader_election_watch.mark_changed();
        leader_state_watch.mark_changed();

        let maybe_leader_info = { leader_election_watch.borrow_and_update().to_owned() };

        if maybe_leader_info.is_none() {
            info!("will attempt to be come leader for consumer group {consumer_group_id:?}");
            self.try_become_leader(consumer_group_id).await;
        }

        let consumer_handle = ConsumerSourceSupervisor::spawn(
            self.etcd.clone(),
            Arc::clone(&self.session),
            self.consumer_group_store.clone(),
            leader_state_watch,
            instance_lock,
            new_tx_filter,
            acc_update_filter,
            sender,
        )
        .await?;

        Ok(())
    }
}
