use std::cell::{Ref, RefCell};
use std::sync::Arc;
use std::time::Duration;

use bincode::serialize;
use common::TestContext;
use local_ip_address::linux::local_ip;
use local_ip_address::list_afinet_netifas;
use rdkafka::producer;
use tokio::sync::RwLock;
use tonic::async_trait;
use tracing::info;
use uuid::Uuid;
use yellowstone_grpc_tools::scylladb::etcd_utils::lock::try_lock;
use yellowstone_grpc_tools::scylladb::types::{BlockchainEventType, CommitmentLevel, ConsumerGroupInfo};
use yellowstone_grpc_tools::scylladb::yellowstone_log::consumer_group::etcd_path::get_producer_lock_path_v1;
use yellowstone_grpc_tools::scylladb::yellowstone_log::consumer_group::lock::ConsumerLocker;
use yellowstone_grpc_tools::scylladb::yellowstone_log::consumer_group::leader::{create_leader_state_log, leader_log_name_from_cg_id_v1, observe_consumer_group_state, observe_leader_changes, try_become_leader, ConsumerGroupHeader, ConsumerGroupLeaderNode, ConsumerGroupState, IdleState, LeaderInfo, LostProducerState};
use yellowstone_grpc_tools::scylladb::yellowstone_log::consumer_group::timeline::{ComputingNextProducerState, ProducerProposalState, TimelineTranslator, TranslationState};
use yellowstone_grpc_tools::setup_tracing;
mod common;



#[tokio::test]
async fn test_create_leader_state_log() {
    let ctx = TestContext::new().await.unwrap();
    let mut etcd = ctx.etcd.clone();

    let revision0 = ctx.last_etcd_revision().await;
    
    let consumer_group_id = Uuid::new_v4().into_bytes();
    let producer_id = [0x00];
    let execution_id = Uuid::new_v4().into_bytes();
    let consumer_group_info = ConsumerGroupInfo { 
        consumer_group_id, 
        group_type: yellowstone_grpc_tools::scylladb::types::ConsumerGroupType::Static, 
        producer_id: Some(producer_id), 
        execution_id: Some(execution_id.to_vec()),
        revision: 1, 
        commitment_level: Default::default(), 
        subscribed_event_types: vec![BlockchainEventType::AccountUpdate], 
        consumer_id_shard_assignments: Default::default(),
        last_access_ip_address: None 
    };

    let res = create_leader_state_log(ctx.etcd.clone(), &consumer_group_info).await;
    assert!(res.is_ok());

    let mut state_watch = observe_consumer_group_state(ctx.etcd.clone(), consumer_group_id).await.unwrap();

    let (revision1, state) = state_watch.borrow_and_update().to_owned();
    let expected_state = ConsumerGroupState::Idle(IdleState { 
        header: ConsumerGroupHeader { 
            consumer_group_id, 
            commitment_level: Default::default(), 
            subscribed_blockchain_event_types: vec![BlockchainEventType::AccountUpdate], 
            shard_assignments: Default::default()
        } ,
        producer_id, 
        execution_id: execution_id.to_vec(),
    });

    assert_eq!(state, expected_state);
    assert!(revision1 > revision0);

    let new_state = ConsumerGroupState::Idle(IdleState { 
        header: ConsumerGroupHeader { 
            consumer_group_id, 
            commitment_level: CommitmentLevel::Confirmed, 
            subscribed_blockchain_event_types: vec![BlockchainEventType::AccountUpdate], 
            shard_assignments: Default::default()
        } ,
        producer_id, 
        execution_id: execution_id.to_vec(),
    });
    etcd.put(leader_log_name_from_cg_id_v1(consumer_group_id), serialize(&new_state).unwrap(), None)
        .await
        .unwrap();

    state_watch.changed().await.unwrap();

    let (revision2, actual_state) = state_watch.borrow_and_update().to_owned();

    assert!(revision2 > revision1);
    assert!(actual_state == new_state);
}



#[tokio::test]
async fn test_become_leader_and_resign() {
    let ctx = TestContext::new().await.unwrap();

    let consumer_group_id = Uuid::new_v4().into_bytes();
    let (leader_key, lease) = try_become_leader(
        ctx.etcd.clone(), 
        consumer_group_id, 
        Duration::from_secs(10), 
        ctx.default_ifname()
    ).await.unwrap().unwrap();


    let mut leader_observer = observe_leader_changes(ctx.etcd.clone(), consumer_group_id).await.unwrap();
    let mut leader_resp = ctx.etcd.election_client().leader(leader_key.name()).await.unwrap();
    let actual_leader_key = leader_resp.kv().unwrap().key().to_vec();
    let actual_leader_info = serde_json::from_slice::<LeaderInfo>(leader_resp.kv().unwrap().value()).unwrap();


    let leader_info = leader_observer.borrow().to_owned().unwrap();

    assert_eq!(leader_info, actual_leader_info);
    assert_eq!(leader_key.key(), actual_leader_key.as_slice());

    // resign leader by dropping the lease
    drop(lease);

    let leader_info = leader_observer.wait_for(Option::is_none).await.unwrap().to_owned();
    assert_eq!(leader_info, None);
}



#[tokio::test]
async fn test_leader_mutual_exclusion() {
    let ctx = TestContext::new().await.unwrap();
    let consumer_group_id = Uuid::new_v4().into_bytes();
    let (leader_key, _lease) = try_become_leader(
        ctx.etcd.clone(), 
        consumer_group_id, 
        Duration::from_secs(10), 
        ctx.default_ifname()
    ).await.unwrap().unwrap();

    let mut leader_resp = ctx.etcd.election_client().leader(leader_key.name()).await.unwrap();


    let actual_leader_key = leader_resp.take_kv().unwrap().key().to_vec();

    assert_eq!(leader_key.key(), actual_leader_key.as_slice());

    // The second attempt should timeout
    let maybe = try_become_leader(
        ctx.etcd.clone(), 
        consumer_group_id, 
        Duration::from_secs(1), 
        ctx.default_ifname()
    ).await.unwrap();

    assert!(maybe.is_none());


    // Assert the first campaignee is still the leader
    let mut leader_resp = ctx.etcd.election_client().leader(leader_key.name()).await.unwrap();
    let actual_leader_key = leader_resp.take_kv().unwrap().key().to_vec();
    assert_eq!(leader_key.key(), actual_leader_key.as_slice());
}



struct MockTimelineTranslator {
    next_state: Arc<RwLock<TranslationState>>,
}

#[async_trait]
impl TimelineTranslator for MockTimelineTranslator {
    async fn compute_next_producer(&self, state: ComputingNextProducerState) -> anyhow::Result<TranslationState> {
        Ok(self.next_state.read().await.to_owned())
    }

    async fn accept_proposal(&self, state: ProducerProposalState) -> anyhow::Result<TranslationState> {
        Ok(self.next_state.read().await.to_owned())
    }

    async fn next(&self, state: TranslationState) -> anyhow::Result<TranslationState> {
        Ok(self.next_state.read().await.to_owned())
    }

}

#[tokio::test]
async fn test_leader_state_transation_during_timeline_translation() {

    let _ = setup_tracing();
    let ctx = TestContext::new().await.unwrap();
    let consumer_group_id = Uuid::new_v4().into_bytes();
    let (leader_key, lease) = try_become_leader(
        ctx.etcd.clone(), 
        consumer_group_id, 
        Duration::from_secs(10), 
        ctx.default_ifname()
    ).await.unwrap().unwrap();
    let producer_id = [0x00];

    let producer_lock_keyname = get_producer_lock_path_v1(producer_id);
    let producer_lock = try_lock(ctx.etcd.clone(), &producer_lock_keyname).await.unwrap();

    let execution_id = Uuid::new_v4().into_bytes();
    let consumer_group_info = ConsumerGroupInfo { 
        consumer_group_id, 
        group_type: yellowstone_grpc_tools::scylladb::types::ConsumerGroupType::Static, 
        producer_id: Some(producer_id), 
        execution_id: Some(execution_id.to_vec()),
        revision: 1, 
        commitment_level: Default::default(), 
        subscribed_event_types: vec![BlockchainEventType::AccountUpdate], 
        consumer_id_shard_assignments: Default::default(),
        last_access_ip_address: None 
    };

    create_leader_state_log(ctx.etcd.clone(), &consumer_group_info).await.unwrap();
    let mut leader_state_log = observe_consumer_group_state(ctx.etcd.clone(), consumer_group_id).await.unwrap();

    let lock = RwLock::new(
        TranslationState::ComputingNextProducer(
            ComputingNextProducerState {
                consumer_group_id,
                revision: 1,
            }
        )
    );
    let lock = Arc::new(lock);

    let translator = MockTimelineTranslator {
        next_state: Arc::clone(&lock),
    };
    let translator: Arc<dyn TimelineTranslator + Send + Sync> = Arc::new(translator);
    let mut leader_node = ConsumerGroupLeaderNode::new(
        ctx.etcd.clone(), 
        leader_key, 
        lease, 
        translator
    )
        .await
        .unwrap();


    leader_node.step().await.unwrap();

    let (revision, state) = leader_state_log.borrow_and_update().to_owned();

    assert!(matches!(state, ConsumerGroupState::Idle(_)));

    producer_lock.revoke().await.unwrap();
    
    let fut = leader_state_log
        .wait_for(|(_, state)| matches!(state, ConsumerGroupState::LostProducer(_)));

    leader_node.step().await.unwrap();
    info!("current leader node state: {:?}", leader_node.state());



    let (_revision, state) = fut.await.unwrap().to_owned();
    

    assert!(matches!(state, ConsumerGroupState::LostProducer(
        LostProducerState { 
            header: _,
            lost_producer_id: producer_id,
            execution_id: _,
        }
    )));

    println!("state {state:?}");
}