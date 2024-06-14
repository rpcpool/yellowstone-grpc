use {
    common::{MockProducerMonitor, TestContext},
    rdkafka::consumer,
    std::{collections::BTreeMap, sync::Arc},
    tokio::sync::{mpsc, RwLock},
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
    let ctx = TestContext::new().await.unwrap();
    let etcd = ctx.etcd.clone();
    let producer_id = [0x00];
    let (revision, execution_id) = ctx
        .producer_store
        .get_execution_id(producer_id)
        .await
        .unwrap()
        .unwrap();
    let mut living_producer_list = BTreeMap::new();
    living_producer_list.insert(producer_id, 1);
    let living_producer_list = Arc::new(RwLock::new(living_producer_list));
    let mock = MockProducerMonitor {
        inner: Arc::clone(&living_producer_list),
    };
    let producer_monitor: Arc<dyn ProducerMonitor> = Arc::new(mock);
    let consumer_id1 = String::from("test1");
    let consumer_id2 = String::from("test2");
    let consumer_ids = vec![consumer_id1.clone(), consumer_id2.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];
    let producer_store =
        ScyllaProducerStore::new(Arc::clone(&ctx.session), Arc::clone(&producer_monitor))
            .await
            .unwrap();
    let cg_store = ScyllaConsumerGroupStore::new(Arc::clone(&ctx.session), producer_store.clone())
        .await
        .unwrap();

    let (coordinator, backend_handle) = ConsumerGroupCoordinatorBackend::spawn(
        etcd.clone(),
        Arc::clone(&ctx.session),
        cg_store,
        producer_store,
        Arc::clone(&producer_monitor),
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
}
