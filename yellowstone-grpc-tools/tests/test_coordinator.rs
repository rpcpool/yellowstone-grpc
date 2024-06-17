use {
    common::{MockProducerMonitor, TestContext, TestContextBuilder},
    rdkafka::consumer,
    std::{collections::BTreeMap, sync::Arc, time::Duration},
    tokio::sync::{broadcast, mpsc, RwLock},
    yellowstone_grpc_tools::{
        scylladb::{
            types::{BlockchainEvent, BlockchainEventType, CommitmentLevel},
            yellowstone_log::{
                common::SeekLocation,
                consumer_group::{
                    consumer_group_store::ScyllaConsumerGroupStore,
                    coordinator::ConsumerGroupCoordinatorBackend,
                    producer::{ProducerMonitor, ScyllaProducerStore},
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

    let producer_id = [0x00];
    let ctx = TestContextBuilder::new()
        .with_producer_monitor_provider(common::ProducerMonitorProvider::Mock {
            producer_ids: vec![producer_id],
        })
        .build()
        .await
        .unwrap();
    let etcd = ctx.etcd.clone();
    let (revision, execution_id) = ctx
        .producer_store
        .get_execution_id(producer_id)
        .await
        .unwrap()
        .unwrap();
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

#[tokio::test]
async fn test_coordinator_producer_kill_signal_then_revive_producer() {
    // let _ = setup_tracing();
    let (kill_producer_tx, kill_producer_rx) = broadcast::channel::<()>(1);

    let producer_id = [0x00];
    let ctx = TestContextBuilder::new()
        .with_producer_monitor_provider(common::ProducerMonitorProvider::Mock {
            producer_ids: vec![producer_id],
        })
        .build()
        .await
        .unwrap();
    let etcd = ctx.etcd.clone();
    let (revision, execution_id) = ctx
        .producer_store
        .get_execution_id(producer_id)
        .await
        .unwrap()
        .unwrap();
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
    ctx.producer_killer
        .kill_producer(producer_id)
        .await
        .unwrap();

    loop {
        let event = source.try_recv();
        match event {
            Ok(_) => {
                println!("event slot : {}", event.unwrap().slot);
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            _ => panic!("unexpected error"),
        }
    }

    let res = source.recv().await;
    println!("source.recv() is finished {}", res.is_some());
    println!(
        "backend handle is finished: {}",
        backend_handle.is_finished()
    );
    // let (sink, mut source) = mpsc::channel::<BlockchainEvent>(1);
    // // We should be albe to rejoin the group after quitting.
    // coordinator
    //     .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink)
    //     .await
    //     .unwrap();
    // println!("joined consumer group!!!");
    // let event = source.recv().await;
    // assert!(event.is_some());
}
