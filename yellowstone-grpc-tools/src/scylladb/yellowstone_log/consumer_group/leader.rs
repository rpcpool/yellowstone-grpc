use std::{borrow::{Borrow, BorrowMut}, collections::HashMap, sync::{Arc, RwLock}};

use tokio::sync::Mutex;

use crate::scylladb::types::ProducerId;

use super::types::ConsumerGroupId;


#[derive(Clone, PartialEq, Eq)]
enum ConsumerGroupLeaderInner {
    Follower,
    Leader {
        consumer_group_producer_map: HashMap<ConsumerGroupId, ProducerId>
    }
}


#[derive(Clone)]
struct ConsumerGroupLeader {
    etcd_client: Arc<Mutex<etcd_client::Client>>,
    inner: Arc<RwLock<ConsumerGroupLeaderInner>>
}


impl ConsumerGroupLeader {

    pub async fn get_producer_id(&self, consumer_group_id: ConsumerGroupId) -> anyhow::Result<ProducerId> {
        // let x = self.inner.lock().await.borrow_mut();
        // match x {
        //     ConsumerGroupLeaderInner::Follower( => todo!(),
        //     ConsumerGroupLeaderInner::Leader(leader) => todo!(),
        // }        

        todo!()
    }
}

