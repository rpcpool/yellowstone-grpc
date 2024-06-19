use {
    common::{MockProducerMonitor, ProducerKiller, TestContext, TestContextBuilder}, core::time, etcd_client::{GetOptions, WatchFilterType, WatchOptions}, futures::TryFutureExt, rdkafka::consumer, sha2::digest::typenum::Prod, std::{collections::BTreeMap, sync::Arc, time::Duration}, tokio::sync::{broadcast, mpsc, RwLock}, tokio_stream::StreamExt, yellowstone_grpc_tools::{
        scylladb::{
            types::{BlockchainEvent, BlockchainEventType, CommitmentLevel, ProducerId, TranslationStrategy},
            yellowstone_log::{
                common::SeekLocation,
                consumer_group::{
                    consumer_group_store::ScyllaConsumerGroupStore, coordinator::ConsumerGroupCoordinatorBackend, etcd_path, leader::{observe_consumer_group_state, observe_leader_changes, ConsumerGroupState, InTimelineTranslationState}, producer::{ProducerMonitor, ScyllaProducerStore}
                },
            },
        },
        setup_tracing,
    }
};
mod common;

#[tokio::test]
async fn test_coordinator_backend_successful_run() {
    let producer_id = ProducerId::try_from("00000000-0000-0000-0000-000000000000").unwrap();
    let ctx = TestContextBuilder::new()
        .with_producer_monitor_provider(common::ProducerMonitorProvider::Mock {
            producer_ids: vec![producer_id],
        })
        .build()
        .await
        .unwrap();
    let beginning_revision = ctx.last_etcd_revision().await;
    let etcd = ctx.etcd.clone();
    let consumer_id1 = String::from("test1");
    let consumer_id2 = String::from("test2");
    let consumer_ids = vec![consumer_id1.clone(), consumer_id2.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];

    let (coordinator, backend_handle) = ctx.spawn_coordinator();

    let consumer_group_id = coordinator
        .create_consumer_group(
            SeekLocation::Earliest,
            subscribed_events.clone(),
            consumer_ids.clone(),
            CommitmentLevel::Processed,
            None,
            Some(TranslationStrategy::AllowLag)
        )
        .await
        .unwrap();

    let (sink, mut source) = mpsc::channel::<BlockchainEvent>(1);
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink)
        .await
        .unwrap();
    let event = source.recv().await.unwrap();
    assert!(event.slot > 0);

    // Dropping the source shoud quit the consumer group
    drop(source);

    let (sink, mut source) = mpsc::channel::<BlockchainEvent>(1);

    // wait for lock to be released
    let prefix = etcd_path::get_instance_lock_prefix_v1(consumer_group_id);
    let watch_option = WatchOptions::new()
        .with_prefix()
        .with_start_revision(beginning_revision)
        .with_filters(vec![WatchFilterType::NoPut]);
    let (_, mut stream) = ctx.etcd.watch_client().watch(prefix, Some(watch_option)).await.unwrap();

    let _lock_released = stream.next().await.unwrap().unwrap();

    // We should be albe to rejoin the group after quitting.
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink)
        .await
        .unwrap();
    let event = source.recv().await;
    assert!(event.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_coordinator_producer_kill_signal_then_revive_producer() {
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

    let (coordinator, backend_handle) = ctx.spawn_coordinator();

    let consumer_group_id = coordinator
        .create_consumer_group(
            SeekLocation::Earliest,
            subscribed_events.clone(),
            consumer_ids.clone(),
            CommitmentLevel::Processed,
            None,
            Some(TranslationStrategy::AllowLag)
        )
        .await
        .unwrap();

    let (sink, mut source) = mpsc::channel::<BlockchainEvent>(1);
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink)
        .await
        .unwrap();
    let event = source.recv().await.unwrap();
    assert!(event.slot > 0);

    // Dropping the source should quit the consumer group
    ctx.producer_killer
        .kill_producer(producer_id)
        .await
        .unwrap();

    while let Some(_) = source.recv().await {}
}



#[tokio::test(flavor = "multi_thread")]
async fn test_timeline_translation_when_there_is_no_other_producer() {
    let producer_id1 = ProducerId::ZERO;
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
    let consumer_ids = vec![consumer_id1.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];

    let (coordinator, backend_handle) = ctx.spawn_coordinator();

    let consumer_group_id = coordinator
        .create_consumer_group(
            SeekLocation::Earliest,
            subscribed_events.clone(),
            consumer_ids.clone(),
            CommitmentLevel::Processed,
            None,
            Some(TranslationStrategy::AllowLag)
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
    let event = source.recv().await.unwrap();
    assert!(event.slot > 0);

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

    let mut leader_changes = observe_leader_changes(etcd, consumer_group_id).await.unwrap();
    leader_changes.mark_changed();
    let state = leader_changes.wait_for(Option::is_none).await.unwrap().to_owned();
    assert!(matches!(state, None));

    state_watch.mark_changed();

    let (_revision, state) = state_watch.borrow_and_update().to_owned();
    assert!(matches!(state, ConsumerGroupState::InTimelineTranslation(_)));
}



#[tokio::test(flavor = "multi_thread")]
async fn test_timeline_translation() {
    ///
    /// In order to be able to test without too much hassle, we will kill the producer 1, then revive it asap, 
    /// so the newly revived is part of the elligibilty list computed by the ScyllaTimelineTranslator.
    /// 
    /// This should allow us to test the timeline translation logic while having a single producer timeline.
    let producer_id1 = ProducerId::ZERO;
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
    let consumer_ids = vec![consumer_id1.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];

    let (coordinator, backend_handle) = ctx.spawn_coordinator();

    let consumer_group_id = coordinator
        .create_consumer_group(
            SeekLocation::Earliest,
            subscribed_events.clone(),
            consumer_ids.clone(),
            CommitmentLevel::Processed,
            None,
            Some(TranslationStrategy::AllowLag)
        )
        .await
        .unwrap();

    let mut election_watch = observe_leader_changes(etcd, consumer_group_id).await.unwrap();
    let mut state_watch = observe_consumer_group_state(ctx.etcd.clone(), consumer_group_id).await.unwrap();
    state_watch.mark_changed();

    let (sink, mut source) = mpsc::channel::<BlockchainEvent>(1);
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink)
        .await
        .unwrap();
    let event = source.recv().await.unwrap();
    assert!(event.slot > 0);

    let mut state_watch2 = state_watch.clone();
    // And kill the last producer so group leader triggers state transition
    let handle = tokio::spawn(async move {
        state_watch2.mark_changed();
        state_watch2
            .wait_for(|(_, state)| !matches!(state, ConsumerGroupState::Idle(_)))
            .await
            .map(|state| state.to_owned())
    });

    // Send kill signal, but don't remove producer from the alive list.
    mock.send_kill_signal(producer_id1, false).await;
    let is_alive = mock.is_producer_alive(producer_id1).await;
    assert!(is_alive);

    let (revision1, new_state) = handle.await.unwrap().unwrap();

    assert!(matches!(new_state, ConsumerGroupState::LostProducer(_)));

    election_watch.mark_changed();
    state_watch.mark_changed();

    let leader_info = election_watch.borrow_and_update().to_owned();

    assert!(leader_info.is_some());

    let (revision2, state) = tokio::time::timeout(
        Duration::from_secs(10),
        state_watch.wait_for(|(_, state)| matches!(state, ConsumerGroupState::Idle(_)))
    )
        .await
        .unwrap()
        .unwrap()
        .to_owned();
    assert!(revision2 > revision1);
}


#[tokio::test]
async fn test_multiple_consumer_joining_group() {
    let producer_id = ProducerId::try_from("00000000-0000-0000-0000-000000000000").unwrap();
    let ctx = TestContextBuilder::new()
        .with_producer_monitor_provider(common::ProducerMonitorProvider::Mock {
            producer_ids: vec![producer_id],
        })
        .build()
        .await
        .unwrap();
    let beginning_revision = ctx.last_etcd_revision().await;
    let etcd = ctx.etcd.clone();
    let consumer_id1 = String::from("test1");
    let consumer_id2 = String::from("test2");
    let consumer_ids = vec![consumer_id1.clone(), consumer_id2.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];

    let (coordinator, backend_handle) = ctx.spawn_coordinator();

    let consumer_group_id = coordinator
        .create_consumer_group(
            SeekLocation::Earliest,
            subscribed_events.clone(),
            consumer_ids.clone(),
            CommitmentLevel::Processed,
            None,
            Some(TranslationStrategy::AllowLag)
        )
        .await
        .unwrap();

    let (sink1, mut source1) = mpsc::channel::<BlockchainEvent>(1);
    let (sink2, mut source2) = mpsc::channel::<BlockchainEvent>(1);
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink1)
        .await
        .unwrap();
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id2.clone(), None, sink2)
        .await
        .unwrap();
    let event1 = source1.recv().await.unwrap();
    let event2 = source2.recv().await.unwrap();
    assert!(event1.slot > 0);
    assert!(event2.slot > 0);
    // Dropping the source shoud quit the consumer group, but don't exit other consumer member
    drop(source1);
    let event2 = source2.recv().await;
    assert!(event2.is_some());
}


#[tokio::test]
async fn test_consumer_member_mutual_exclusion() {
    let producer_id = ProducerId::try_from("00000000-0000-0000-0000-000000000000").unwrap();
    let ctx = TestContextBuilder::new()
        .with_producer_monitor_provider(common::ProducerMonitorProvider::Mock {
            producer_ids: vec![producer_id],
        })
        .build()
        .await
        .unwrap();
    let beginning_revision = ctx.last_etcd_revision().await;
    let etcd = ctx.etcd.clone();
    let consumer_id1 = String::from("test1");
    let consumer_ids = vec![consumer_id1.clone()];
    let subscribed_events = vec![BlockchainEventType::AccountUpdate];

    let (coordinator, backend_handle) = ctx.spawn_coordinator();

    let consumer_group_id = coordinator
        .create_consumer_group(
            SeekLocation::Earliest,
            subscribed_events.clone(),
            consumer_ids.clone(),
            CommitmentLevel::Processed,
            None,
            Some(TranslationStrategy::AllowLag)
        )
        .await
        .unwrap();

    let (sink1, mut source1) = mpsc::channel::<BlockchainEvent>(1);
    let (sink2, mut source2) = mpsc::channel::<BlockchainEvent>(1);
    coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink1)
        .await
        .unwrap();
    let result = coordinator
        .try_join_consumer_group(consumer_group_id, consumer_id1.clone(), None, sink2)
        .await;
    assert!(result.is_err());
}