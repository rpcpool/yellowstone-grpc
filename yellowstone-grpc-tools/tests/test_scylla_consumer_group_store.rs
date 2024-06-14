use {
    common::{MockProducerMonitor, TestContext},
    std::{collections::BTreeMap, sync::Arc, time::Duration},
    tokio::sync::{mpsc, oneshot, RwLock},
    yellowstone_grpc_tools::{
        scylladb::{
            etcd_utils::lock::{try_lock, ManagedLock},
            types::{BlockchainEvent, BlockchainEventType, CommitmentLevel, Slot},
            yellowstone_log::{
                common::SeekLocation,
                consumer_group::{
                    consumer_group_store::ScyllaConsumerGroupStore,
                    consumer_source::{ConsumerSource, ConsumerSourceCommand, FromBlockchainEvent},
                    context::ConsumerContext,
                    etcd_path::get_producer_lock_path_v1,
                    lock::ConsumerLocker,
                    producer::{ProducerMonitor, ScyllaProducerStore},
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
async fn test_get_lowest_common_slot_number() {
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
    let consumer_ids = vec![consumer_id1, consumer_id2];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];
    let producer_store = ScyllaProducerStore::new(Arc::clone(&ctx.session), producer_monitor)
        .await
        .unwrap();
    let cg_store = ScyllaConsumerGroupStore::new(Arc::clone(&ctx.session), producer_store)
        .await
        .unwrap();
    let cg_info = cg_store
        .create_static_consumer_group(
            &consumer_ids,
            CommitmentLevel::Processed,
            &subscribed_events,
            SeekLocation::Earliest,
            None,
        )
        .await
        .unwrap();

    let consumer_group_id = cg_info.consumer_group_id;

    let result = cg_store
        .get_lowest_common_slot_number(&consumer_group_id, None)
        .await;

    assert!(result.is_ok());
}
