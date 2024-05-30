use {
    crate::scylladb::{
        types::{BlockchainEventType, ProducerId},
        yellowstone_log::common::InitialOffset,
    },
    std::{
        borrow::{Borrow, BorrowMut},
        collections::HashMap,
        sync::{Arc, RwLock},
    },
    tokio::sync::Mutex,
    yellowstone_grpc_proto::{
        geyser::CommitmentLevel,
        yellowstone::log::{AccountUpdateEventFilter, TransactionEventFilter},
    },
};

// enum ConsumerGroupLeaderLocation(
//     Local,
//     Remote()
// )

#[derive(Clone)]
struct ConsumerGroupLeaderEdge {
    etcd_client: etcd_client::Client,
}

impl ConsumerGroupLeaderEdge {
    pub async fn create_consumer_group(
        initial_offset_policy: InitialOffset,
        blockchain_event_type_to_subscribe: Vec<BlockchainEventType>,
        commitment_level: CommitmentLevel,
        account_update_event_filter: Option<AccountUpdateEventFilter>,
        tx_event_filter: Option<TransactionEventFilter>,
    ) {
        // TODO
    }

    pub async fn get_producer_id(
        &self,
        consumer_group_id: impl Into<Vec<u8>>,
    ) -> anyhow::Result<ProducerId> {
        // let x = self.inner.lock().await.borrow_mut();
        // match x {
        //     ConsumerGroupLeaderInner::Follower( => todo!(),
        //     ConsumerGroupLeaderInner::Leader(leader) => todo!(),
        // }

        todo!()
    }
}
