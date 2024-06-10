use std::sync::Arc;

use scylla::{Session, SessionBuilder};
use tokio::sync::watch;
use uuid::Uuid;
use yellowstone_grpc_tools::scylladb::{types::BlockchainEventType, yellowstone_log::consumer_group::{consumer_group_store::ConsumerGroupStore, consumer_supervisor::ConsumerSourceSupervisor, leader::{ConsumerGroupHeader, ConsumerGroupState}, lock::{ConsumerLock, ConsumerLocker}, producer_queries::ProducerQueries}};

#[]
use yellowstone_grpc_tools::scylladb::yellowstone_log::consumer_group::consumer_source::ConsumerSource;



struct TestContext {
    session: Arc<Session>,
    etcd: etcd_client::Client,
    consumer_group_store: ConsumerGroupStore,
    producer_queries: ProducerQueries,
}


impl TestContext {

    pub async fn new() -> anyhow::Result<Self> {
        let scylladb_endpoint = std::env::var("TEST_SCYLLADB_HOSTNAME")?;
        let scylladb_user = std::env::var("TEST_SCYLLADB_USER")?;
        let scylladb_passwd = std::env::var("TEST_SCYLLADB_PASSWD")?;
        let keyspace = std::env::var("TEST_SCYLLADB_KEYSPACE")?;
        let session: Session = SessionBuilder::new()
            .known_node(scylladb_endpoint)
            .user(scylladb_user, scylladb_passwd)
            .use_keyspace(keyspace, false)
            .build()
            .await?;

        let etcd = etcd_client::Client::connect(["localhost:2379"], None).await?;
        let session = Arc::new(session);
        let consumer_group_store =
            ConsumerGroupStore::new(Arc::clone(&session), etcd.clone()).await?;
        let producer_queries =
            ProducerQueries::new(Arc::clone(&session), etcd.clone()).await?;
        let ctx = TestContext {
            session: session,
            etcd,
            consumer_group_store,
            producer_queries,
        };
        Ok(ctx)
    }
}


#[tokio::test]
async fn test_supervisor() {
    let ctx = TestContext::new().await.unwrap();
    let locker = ConsumerLocker(ctx.etcd.clone());
    let consumer_group_id = Uuid::new_v4().into_bytes();
    let consumer_id = Uuid::new_v4().to_string();
    let lock = locker.try_lock_instance_id(consumer_group_id, &consumer_id).await.unwrap();
    let cg_header = ConsumerGroupHeader { 
        consumer_group_id: consumer_group_id, 
        commitment_level: Default::default(), 
        subscribed_blockchain_event_types: vec![BlockchainEventType::AccountUpdate], 
        shard_assignments: Default::default()
    };
    let (tx, rx) = watch::channel((1, ConsumerGroupState::Init(cg_header)));

    let supervisor = ConsumerSourceSupervisor::new(
        lock,
        ctx.etcd.clone(),
        Arc::clone(&ctx.session),
        ctx.consumer_group_store.clone(),
        rx,
    );


    // supervisor.spawn_with(|ctx| {

    //     async move {
    //         todo!()
    //     }
    // });
}