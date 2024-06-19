use {
    common::{MockProducerMonitor, TestContext, TestContextBuilder},
    std::{collections::BTreeMap, sync::Arc, time::Duration},
    tokio::sync::{mpsc, oneshot, RwLock},
    yellowstone_grpc_tools::{
        scylladb::{
            etcd_utils::lock::{try_lock, ManagedLock},
            types::{BlockchainEvent, BlockchainEventType, CommitmentLevel, ProducerId, Slot},
            yellowstone_log::{
                common::SeekLocation,
                consumer_group::{
                    consumer_group_store::ScyllaConsumerGroupStore,
                    consumer_source::{ConsumerSource, FromBlockchainEvent},
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

#[tokio::test]
async fn test_get_lowest_common_slot_number() {
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
    let consumer_ids = vec![consumer_id1, consumer_id2];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];
    let cg_info = ctx
        .consumer_group_store
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

    let result = ctx
        .consumer_group_store
        .get_lowest_common_slot_number(&consumer_group_id, None)
        .await;

    assert!(result.is_ok());
}
