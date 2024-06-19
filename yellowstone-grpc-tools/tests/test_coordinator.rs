use {
    common::{MockProducerMonitor, ProducerKiller, TestContext, TestContextBuilder},
    core::time,
    rdkafka::consumer,
    sha2::digest::typenum::Prod,
    std::{collections::BTreeMap, sync::Arc, time::Duration},
    tokio::sync::{broadcast, mpsc, RwLock},
    yellowstone_grpc_tools::{
        scylladb::{
            types::{BlockchainEvent, BlockchainEventType, CommitmentLevel, ProducerId},
            yellowstone_log::{
                common::SeekLocation,
                consumer_group::{
                    consumer_group_store::ScyllaConsumerGroupStore, coordinator::ConsumerGroupCoordinatorBackend, leader::{observe_consumer_group_state, ConsumerGroupState}, producer::{ProducerMonitor, ScyllaProducerStore}
                },
            },
        },
        setup_tracing,
    },
};
mod common;

#[tokio::test]
async fn test_coordinator_backend_successful_run() {
    let (kill_producer_tx, kill_producer_rx) = broadcast::channel::<()>(1);

    let producer_id = ProducerId::try_from("00000000-0000-0000-0000-000000000000").unwrap();
    let ctx = TestContextBuilder::new()
        .with_producer_monitor_provider(common::ProducerMonitorProvider::Mock {
            producer_ids: vec![producer_id],
        })
        .build()
        .await
        .unwrap();
    let etcd = ctx.etcd.clone();
    let consumer_id1 = String::from("test1");
    let consumer_id2 = String::from("test2");
    let consumer_ids = vec![consumer_id1.clone(), consumer_id2.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];

    let (coordinator, backend_handle) = ConsumerGroupCoordinatorBackend::spawn(
        etcd.clone(),
        Arc::clone(&ctx.session),
        ctx.consumer_group_store.clone(),
        ctx.producer_store.clone(),
        Arc::clone(&ctx.producer_monitor),
        ctx.default_ifname(),
    );

    let consumer_group_id = coordinator
        .create_consumer_group(
            SeekLocation::Earliest,
            subscribed_events.clone(),
            consumer_ids.clone(),
            CommitmentLevel::Processed,
            None,
        )
        .await
        .unwrap();

    let (sink, mut source) = mpsc::channel::<BlockchainEvent>(1);
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink)
        .await
        .unwrap();
    println!("joined consumer group!!!");
    let event = source.recv().await.unwrap();
    println!("event slot : {}", event.slot);
    assert!(event.slot > 0);

    // Dropping the source shoud quit the consumer group
    drop(source);

    let (sink, mut source) = mpsc::channel::<BlockchainEvent>(1);
    // We should be albe to rejoin the group after quitting.
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink)
        .await
        .unwrap();
    println!("joined consumer group!!!");
    let event = source.recv().await;
    assert!(event.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_coordinator_producer_kill_signal_then_revive_producer() {
    let _ = setup_tracing();
    let producer_id = ProducerId::ZERO;
    let ctx = TestContextBuilder::new()
        .with_producer_monitor_provider(common::ProducerMonitorProvider::Mock {
            producer_ids: vec![producer_id],
        })
        .build()
        .await
        .unwrap();
    let etcd = ctx.etcd.clone();
    let consumer_id1 = String::from("test1");
    let consumer_id2 = String::from("test2");
    let consumer_ids = vec![consumer_id1.clone(), consumer_id2.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];

    let (coordinator, backend_handle) = ConsumerGroupCoordinatorBackend::spawn(
        etcd.clone(),
        Arc::clone(&ctx.session),
        ctx.consumer_group_store.clone(),
        ctx.producer_store.clone(),
        Arc::clone(&ctx.producer_monitor),
        ctx.default_ifname(),
    );

    let consumer_group_id = coordinator
        .create_consumer_group(
            SeekLocation::Earliest,
            subscribed_events.clone(),
            consumer_ids.clone(),
            CommitmentLevel::Processed,
            None,
        )
        .await
        .unwrap();

    let (sink, mut source) = mpsc::channel::<BlockchainEvent>(1);
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink)
        .await
        .unwrap();
    println!("joined consumer group!!!");
    let event = source.recv().await.unwrap();
    println!("event slot : {}", event.slot);
    assert!(event.slot > 0);

    // Dropping the source should quit the consumer group
    ctx.producer_killer
        .kill_producer(producer_id)
        .await
        .unwrap();

    while let Some(_) = source.recv().await {}
}



#[tokio::test(flavor = "multi_thread")]
async fn test_timeline_translation() {
    let _ = setup_tracing();
    let (kill_producer_tx, kill_producer_rx) = broadcast::channel::<()>(1);

    let producer_id1 = ProducerId::ZERO;
    let producer_id2 = ProducerId::try_from("11111111-1111-1111-1111-111111111111").unwrap();

    let mock = MockProducerMonitor::from(vec![producer_id1]);

    let ctx = TestContextBuilder::new()
        .with_producer_monitor_provider(common::ProducerMonitorProvider::Custom { 
            producer_monitor: Arc::new(mock.clone()), 
            producer_kill: Arc::new(mock.clone()) 
        })
        .build()
        .await
        .unwrap();
    let etcd = ctx.etcd.clone();
    let consumer_id1 = String::from("test1");
    let consumer_id2 = String::from("test2");
    let consumer_ids = vec![consumer_id1.clone(), consumer_id2.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];

    let (coordinator, backend_handle) = ConsumerGroupCoordinatorBackend::spawn(
        etcd.clone(),
        Arc::clone(&ctx.session),
        ctx.consumer_group_store.clone(),
        ctx.producer_store.clone(),
        Arc::clone(&ctx.producer_monitor),
        ctx.default_ifname(),
    );

    let consumer_group_id = coordinator
        .create_consumer_group(
            SeekLocation::Earliest,
            subscribed_events.clone(),
            consumer_ids.clone(),
            CommitmentLevel::Processed,
            None,
        )
        .await
        .unwrap();


    let mut state_watch = observe_consumer_group_state(ctx.etcd.clone(), consumer_group_id).await.unwrap();
    state_watch.mark_changed();

    let (sink, mut source) = mpsc::channel::<BlockchainEvent>(1);
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink)
        .await
        .unwrap();
    println!("joined consumer group!!!");
    let event = source.recv().await.unwrap();
    println!("event slot : {}", event.slot);
    assert!(event.slot > 0);
    

    // Add a new producer id
    mock.add_producer_id(producer_id2).await;

    let mut state_watch2 = state_watch.clone();
    // And kill the last producer so group leader triggers state transition
    let handle = tokio::spawn(async move {
        state_watch2.mark_changed();
        state_watch2
            .wait_for(|(_, state)| !matches!(state, ConsumerGroupState::Idle(_)))
            .await
            .map(|state| state.to_owned())
    });
    mock.kill_producer(producer_id1).await.unwrap();

    let (_revision, new_state) = handle.await.unwrap().unwrap();

    assert!(matches!(new_state, ConsumerGroupState::LostProducer(_)));

    state_watch.mark_changed();

    let (_revision, actual_state) = state_watch
        .wait_for(|(_, state)| matches!(state, ConsumerGroupState::Idle(_)))
        .await
        .unwrap()
        .to_owned();

    let actual_idle_state = match actual_state {
        ConsumerGroupState::Idle(state) => state,
        _ => panic!("Expected idle state"),
    };

    assert_eq!(actual_idle_state.producer_id, producer_id2);
}