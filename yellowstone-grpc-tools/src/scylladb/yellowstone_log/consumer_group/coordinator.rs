use {
    super::{consumer_group_store::ConsumerGroupStore, consumer_source::ConsumerSource, leader::{self, observe_consumer_group_state, observe_leader_changes, try_become_leader, ConsumerGroupState, LeaderInfo}, lock::{InstanceLock, InstanceLocker}},
    crate::scylladb::{etcd_utils::Revision, types::{ConsumerGroupId, InstanceId}},
    chrono::Local,
    futures::channel::oneshot,
    std::{borrow::{Borrow, BorrowMut}, collections::HashMap, sync::Arc, time::Duration},
    tokio::sync::{mpsc, watch, Mutex}, tracing::info,
};

pub enum LocalCommand {
    StopInstanceOfConsumerGroup(ConsumerGroupId, oneshot::Sender<Vec<InstanceId>>),
}

struct ConsumerHandle {}



struct Inner {
    leader_election_watch_map: HashMap<ConsumerGroupId, watch::Receiver<Option<LeaderInfo>>>,
    leader_state_watch_map: HashMap<ConsumerGroupId, watch::Receiver<(Revision, ConsumerGroupState)>>,
}


pub struct LocalConsumerGroupCoordinator {
    etcd: etcd_client::Client,
    instance_lock: InstanceLocker,
    consumer_group_store: ConsumerGroupStore,
    inner: Arc<Mutex<Inner>>,
    leader_ifname: String,
}

impl LocalConsumerGroupCoordinator {
   

    async fn try_become_leader(&self, consumer_group_id: &ConsumerGroupId) -> anyhow::Result<()> {
        let mut rx = try_become_leader(&self.etcd, consumer_group_id, Duration::from_secs(5), &self.leader_ifname)?;
        //rx.await
        Ok(())
    }

    pub async fn join_consumer_group(
        &self, 
        consumer_group_id: &ConsumerGroupId,
        instance_id: &InstanceId,
    ) -> anyhow::Result<()> {
        let etcd = &self.etcd;
        
        let instance_lock = self.instance_lock
            .try_lock_instance_id(consumer_group_id.as_slice(), instance_id)
            .await?;
        
        let mut leader_election_watch = {
            let mut inner = self.inner.lock().await;
            match inner.borrow_mut()
                .leader_election_watch_map
                .entry(consumer_group_id.clone()) {
                    std::collections::hash_map::Entry::Occupied(o) => o.get().clone(),
                    std::collections::hash_map::Entry::Vacant(v) => {
                        let watch = observe_leader_changes(etcd, consumer_group_id).await?;
                        v.insert(watch).clone()
                    },
                }
        };

        let mut leader_state_watch = {
            let mut inner = self.inner.lock().await;
            match inner.borrow_mut()
                .leader_state_watch_map
                .entry(consumer_group_id.clone()) {
                    std::collections::hash_map::Entry::Occupied(o) => o.get().clone(),
                    std::collections::hash_map::Entry::Vacant(v) => {
                        let watch = observe_consumer_group_state(etcd, consumer_group_id).await?;
                        v.insert(watch).clone()
                    },
                }
        };

        let maybe_leader_info = {
            leader_election_watch.borrow_and_update().to_owned()
        };

        if maybe_leader_info.is_none() {
            info!("will attempt to be come leader for consumer group {consumer_group_id:?}");
            self.try_become_leader(consumer_group_id).await;
        }

        Ok(())
   }
}


struct ConsumerSourceSupervisor {
    sender: 
}




