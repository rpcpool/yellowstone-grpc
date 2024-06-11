use bincode::serialize;
use common::TestContext;
use uuid::Uuid;
use yellowstone_grpc_tools::scylladb::types::{BlockchainEventType, CommitmentLevel, ConsumerGroupInfo};
use yellowstone_grpc_tools::scylladb::yellowstone_log::consumer_group::lock::ConsumerLocker;
use yellowstone_grpc_tools::scylladb::yellowstone_log::consumer_group::leader::{create_leader_state_log, leader_log_name_from_cg_id_v1, observe_consumer_group_state, ConsumerGroupHeader, ConsumerGroupState, IdleState};
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