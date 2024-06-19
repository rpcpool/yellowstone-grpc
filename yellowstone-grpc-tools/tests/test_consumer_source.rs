use {
    common::{TestContext, TestContextBuilder},
    std::{collections::BTreeMap, sync::Arc, time::Duration},
    tokio::sync::{mpsc, oneshot},
    yellowstone_grpc_tools::{
        scylladb::{
            etcd_utils::lock::{try_lock, ManagedLock},
            types::{BlockchainEvent, BlockchainEventType, CommitmentLevel, Slot},
            yellowstone_log::{
                common::SeekLocation,
                consumer_group::{
                    consumer_source::{ConsumerSource, FromBlockchainEvent},
                    context::ConsumerContext,
                    etcd_path::get_producer_lock_path_v1,
                    lock::ConsumerLocker,
                },
            },
        },
        setup_tracing,
    },
};

mod common;

#[derive(Debug)]
struct MockEvent {
    slot: Slot,
}

impl FromBlockchainEvent for MockEvent {
    fn from(event: BlockchainEvent) -> Self {
        MockEvent { slot: event.slot }
    }
}

#[tokio::test]
async fn test_consumer_source_run() {
    let ctx = TestContextBuilder::new().build().await.unwrap();
    let etcd = ctx.etcd.clone();
    let producer_id = ctx.producer_id;
    let producer_info = ctx
        .producer_store
        .get_producer_info(producer_id)
        .await
        .unwrap()
        .unwrap();

    let lock_key = get_producer_lock_path_v1(producer_id);
    let producer_lock = try_lock(etcd.clone(), lock_key.as_str()).await.unwrap();
    let consumer_id = String::from("test");
    let consumer_ids = vec![consumer_id.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];
    let cg_info = ctx
        .consumer_group_store
        .create_static_consumer_group(
            &consumer_ids,
            CommitmentLevel::Processed,
            &subscribed_events,
            SeekLocation::Earliest,
            None,
            None,
        )
        .await
        .unwrap();

    let consumer_group_id = cg_info.consumer_group_id;

    let consumer_locker = ConsumerLocker(etcd.clone());

    let consumer_lock = consumer_locker
        .try_lock_instance_id(consumer_group_id, &consumer_id)
        .await
        .unwrap();

    let (initial_revision, shard_offset_map1) = ctx
        .consumer_group_store
        .get_shard_offset_map(
            &consumer_group_id,
            &consumer_id,
            &producer_id,
            BlockchainEventType::AccountUpdate,
        )
        .await
        .unwrap();

    let fencing_token_generator = consumer_lock.get_fencing_token_gen();
    let consumer_ctx = ConsumerContext {
        consumer_group_id: consumer_group_id,
        consumer_id: consumer_id.clone(),
        producer_id,
        subscribed_event_types: subscribed_events.clone(),
        session: Arc::clone(&ctx.session),
        etcd: etcd.clone(),
        consumer_group_store: ctx.consumer_group_store.clone(),
        fencing_token_generator: fencing_token_generator,
    };

    let mut shard_offset_map_per_event_type = BTreeMap::new();
    shard_offset_map_per_event_type.insert(
        BlockchainEventType::AccountUpdate,
        shard_offset_map1.clone(),
    );

    let (sink, mut source) = tokio::sync::mpsc::channel::<MockEvent>(1);
    let mut cs = ConsumerSource::new(
        consumer_ctx,
        shard_offset_map_per_event_type,
        sink,
        Some(Duration::from_secs(3600)),
        None,
    )
    .await
    .unwrap();

    let (tx_cmd, rx_cmd) = oneshot::channel();

    let h = tokio::spawn(async move { cs.run(rx_cmd).await });
    let _event = source.recv().await.unwrap();
    tx_cmd.send(()).unwrap();

    h.await.unwrap().unwrap();
    // It should have persisted the offset before shutting down
    let (new_revision, shard_offset_map2) = ctx
        .consumer_group_store
        .get_shard_offset_map(
            &consumer_group_id,
            &consumer_id,
            &producer_id,
            BlockchainEventType::AccountUpdate,
        )
        .await
        .unwrap();
    println!(
        "new_revision: {:?}, initial_revision: {:?}",
        new_revision, initial_revision
    );
    assert!(new_revision > initial_revision);
    assert_ne!(shard_offset_map1, shard_offset_map2);
}
