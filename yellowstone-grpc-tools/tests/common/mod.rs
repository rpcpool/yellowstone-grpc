use {
    futures::{future, FutureExt}, local_ip_address::{linux::local_ip, list_afinet_netifas}, scylla::{Session, SessionBuilder}, std::sync::Arc, tokio::sync::{broadcast, mpsc, watch}, uuid::Uuid, yellowstone_grpc_tools::{scylladb::{
        etcd_utils::Revision, types::BlockchainEventType, yellowstone_log::consumer_group::{
            consumer_group_store::ScyllaConsumerGroupStore, consumer_source::{ConsumerSourceCommand, ConsumerSourceHandle}, consumer_supervisor::ConsumerSourceSupervisor, leader::{ConsumerGroupHeader, ConsumerGroupState, IdleState, LostProducerState, WaitingBarrierState}, lock::{ConsumerLock, ConsumerLocker}, producer_queries::ProducerQueries
        }
    }, setup_tracing}
};

pub struct TestContext {
    pub session: Arc<Session>,
    pub etcd: etcd_client::Client,
    pub consumer_group_store: ScyllaConsumerGroupStore,
    pub producer_queries: ProducerQueries,
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
            ScyllaConsumerGroupStore::new(Arc::clone(&session), etcd.clone()).await?;
        let producer_queries = ProducerQueries::new(Arc::clone(&session), etcd.clone()).await?;
        let ctx = TestContext {
            session: session,
            etcd,
            consumer_group_store,
            producer_queries,
        };
        Ok(ctx)
    }


    pub async fn last_etcd_revision(&self) -> Revision {
        let mut kv = self.etcd.kv_client();
        let uuid = Uuid::new_v4().to_string();
        let key = format!("test-{uuid}");
        let resp = kv.put(key, "", None).await.unwrap();

        resp.header().unwrap().revision()
    }

    pub fn default_ifname(&self) -> String {
        let ipaddr = local_ip().unwrap();
        list_afinet_netifas()
            .unwrap()
            .iter()
            .find(|(_, ipaddr2)| ipaddr == *ipaddr2)
            .map(|(ifname, _)| ifname)
            .unwrap()
            .to_owned()
    }
}



