use {
    crate::common::TestContext, common::TestContextBuilder, futures::{future, FutureExt}, local_ip_address::linux::local_ip, std::sync::Arc, tokio::{
        sync::{mpsc, oneshot, watch},
        task::JoinHandle,
    }, uuid::Uuid, yellowstone_grpc_tools::scylladb::{
        types::BlockchainEventType,
        yellowstone_log::consumer_group::{
            consumer_source::ConsumerSourceHandle,
            consumer_supervisor::ConsumerSourceSupervisor,
            leader::{ConsumerGroupHeader, ConsumerGroupState, IdleState, LeaderInfo, LostProducerState},
            lock::ConsumerLocker,
        },
    }
};
mod common;

///
/// This test make sure of the following properties:
///
/// - the supervisor start a consumer when the consumer group state goes from init to Idle.
/// - the supervisor stop a consumer when it detected the producer is gone
/// - the supervisor restart a consumer when it receive a new producer
/// - the underlying consumer stop if the supervisor is dropped
/// -
#[tokio::test]
async fn test_supervisor() {
    let ctx = TestContextBuilder::new().build().await.unwrap();
    let locker = ConsumerLocker(ctx.etcd.clone());
    let consumer_group_id = Uuid::new_v4().into_bytes();
    let consumer_id = Uuid::new_v4().to_string();

    let lock = locker
        .try_lock_instance_id(consumer_group_id, &consumer_id)
        .await
        .unwrap();
    let cg_header = ConsumerGroupHeader {
        consumer_group_id: consumer_group_id,
        commitment_level: Default::default(),
        subscribed_blockchain_event_types: vec![BlockchainEventType::AccountUpdate],
        shard_assignments: Default::default(),
    };

    let id = Uuid::new_v4();
    let (tx_leader_info, rx_leader_info) = watch::channel(Some(
        LeaderInfo {
            ipaddr: local_ip().unwrap(),
            id: id.as_bytes().to_vec(),
        }
    ));

    let (tx_state, rx_state) = watch::channel((1, ConsumerGroupState::Init(cg_header.clone())));

    let supervisor = ConsumerSourceSupervisor::new(
        lock,
        ctx.etcd.clone(),
        Arc::clone(&ctx.session),
        ctx.consumer_group_store.clone(),
        rx_state,
        rx_leader_info,
    );

    let (tx_eavesdrop, mut rx_eavesdrop) = mpsc::channel(1);

    let handle = supervisor
        .spawn_with(move |ctx| {
            let (tx, mut rx) = oneshot::channel();
            let tx_passthrough = tx_eavesdrop.clone();
            let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                tx_passthrough.send(0).await?;
                rx.await;
                tx_passthrough.send(1).await?;
                Ok(())
            });
            future::ready(Ok(ConsumerSourceHandle { tx, handle })).boxed()
        })
        .await
        .unwrap();

    let state2 = ConsumerGroupState::Idle(IdleState {
        header: cg_header.clone(),
        producer_id: [0x00],
        execution_id: vec![0x00],
    });
    tx_state.send((2, state2)).unwrap();

    let msg = rx_eavesdrop.recv().await.unwrap();
    // 0 = consumer source started
    assert_eq!(msg, 0);

    let state3 = ConsumerGroupState::LostProducer(LostProducerState {
        header: cg_header.clone(),
        lost_producer_id: [0x00],
        execution_id: vec![0x00],
    });
    tx_state.send((3, state3)).unwrap();

    let msg = rx_eavesdrop.recv().await.unwrap();
    // 1 = consumer stop
    assert_eq!(msg, 1);

    let state4 = ConsumerGroupState::Idle(IdleState {
        header: cg_header.clone(),
        producer_id: [0x01],
        execution_id: vec![0x01],
    });

    tx_state.send((4, state4)).unwrap();

    let msg = rx_eavesdrop.recv().await.unwrap();
    // 0 = a new consumer has been spawn
    assert_eq!(msg, 0);

    drop(handle);

    let msg = rx_eavesdrop.recv().await.unwrap();
    // 1 = the consumer has been dropped
    assert_eq!(msg, 1);
}
