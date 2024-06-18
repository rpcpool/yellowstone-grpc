use {
    futures::{channel::oneshot, future, FutureExt},
    local_ip_address::{linux::local_ip, list_afinet_netifas},
    scylla::{Session, SessionBuilder},
    std::{collections::BTreeMap, sync::Arc},
    tokio::sync::{broadcast, mpsc, watch, RwLock},
    tonic::async_trait,
    tracing::info,
    uuid::Uuid,
    yellowstone_grpc_tools::{
        scylladb::{
            etcd_utils::Revision,
            types::{BlockchainEventType, ConsumerId, ProducerId},
            yellowstone_log::consumer_group::{
                consumer_group_store::ScyllaConsumerGroupStore,
                leader::{
                    ConsumerGroupHeader, ConsumerGroupState, IdleState, LostProducerState,
                    WaitingBarrierState,
                },
                lock::{ConsumerLock, ConsumerLocker},
                producer::{
                    EtcdProducerMonitor, ProducerDeadSignal, ProducerMonitor, ScyllaProducerStore,
                },
            },
        },
        setup_tracing,
    },
};

pub struct TestContext {
    pub session: Arc<Session>,
    pub etcd: etcd_client::Client,
    pub consumer_group_store: ScyllaConsumerGroupStore,
    pub producer_store: ScyllaProducerStore,
    pub producer_monitor: Arc<dyn ProducerMonitor>,
    pub producer_killer: Arc<dyn ProducerKiller>,
}

#[derive(Default)]
pub enum ProducerMonitorProvider {
    Mock {
        producer_ids: Vec<ProducerId>,
    },
    #[default]
    Etcd,
}

#[async_trait]
pub trait ProducerKiller {
    async fn kill_producer(&self, producer_id: ProducerId) -> anyhow::Result<()>;
}

pub struct TestContextBuilder {
    producer_monitor_provider: ProducerMonitorProvider,
}

impl TestContextBuilder {
    pub fn new() -> Self {
        TestContextBuilder {
            producer_monitor_provider: Default::default(),
        }
    }

    pub fn with_producer_monitor_provider(
        mut self,
        producer_monitor_provider: ProducerMonitorProvider,
    ) -> Self {
        self.producer_monitor_provider = producer_monitor_provider;
        self
    }

    pub async fn build(self) -> anyhow::Result<TestContext> {
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

        let (producer_monitor, producer_killer): (
            Arc<dyn ProducerMonitor>,
            Arc<dyn ProducerKiller>,
        ) = match self.producer_monitor_provider {
            ProducerMonitorProvider::Mock { producer_ids } => {
                let mock = MockProducerMonitor::from(producer_ids);
                let producer_monitor = Arc::new(mock.clone());

                (Arc::new(mock.clone()), Arc::new(mock.clone()))
            }
            ProducerMonitorProvider::Etcd => (
                Arc::new(EtcdProducerMonitor::new(etcd.clone())),
                Arc::new(NullProducerKiller {}),
            ),
        };

        let session = Arc::new(session);

        let producer_store =
            ScyllaProducerStore::new(Arc::clone(&session), Arc::clone(&producer_monitor)).await?;

        let consumer_group_store =
            ScyllaConsumerGroupStore::new(Arc::clone(&session), producer_store.clone()).await?;

        let ctx = TestContext {
            session: session,
            etcd,
            consumer_group_store,
            producer_store,
            producer_monitor,
            producer_killer,
        };
        Ok(ctx)
    }
}

impl TestContext {
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

struct MockProducerMonitorInner {
    living_producer: BTreeMap<ProducerId, i64>,
    dead_signals: BTreeMap<ProducerId, broadcast::Sender<()>>,
}
#[derive(Clone)]
pub struct MockProducerMonitor {
    pub inner: Arc<RwLock<MockProducerMonitorInner>>,
}

impl MockProducerMonitor {
    pub fn from<I>(producer_iter: I) -> Self
    where
        I: IntoIterator<Item = ProducerId>,
    {
        let mut living_producer = BTreeMap::new();
        producer_iter.into_iter().for_each(|producer_id| {
            living_producer.insert(producer_id, 1);
        });

        let inner = MockProducerMonitorInner {
            living_producer,
            dead_signals: Default::default(),
        };

        MockProducerMonitor {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    async fn send_kill_signal(&self, producer_id: ProducerId) {
        let maybe = {
            let mut lock = self.inner.write().await;
            lock.dead_signals.remove(&producer_id)
        };
        if let Some(tx) = maybe {
            let _ = tx.send(());
        }
    }
}

#[async_trait]
impl ProducerKiller for MockProducerMonitor {
    async fn kill_producer(&self, producer_id: ProducerId) -> anyhow::Result<()> {
        self.send_kill_signal(producer_id).await;
        Ok(())
    }
}

#[derive(Clone)]
struct NullProducerKiller {}

#[async_trait]
impl ProducerKiller for NullProducerKiller {
    async fn kill_producer(&self, producer_id: ProducerId) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ProducerMonitor for MockProducerMonitor {
    async fn list_living_producers(&self) -> BTreeMap<ProducerId, i64> {
        let ret = self.inner.read().await.living_producer.clone();
        ret
    }

    async fn is_producer_alive(&self, producer_id: ProducerId) -> bool {
        let ret = self
            .inner
            .read()
            .await
            .living_producer
            .contains_key(&producer_id);
        ret
    }

    async fn get_producer_dead_signal(&self, producer_id: ProducerId) -> ProducerDeadSignal {
        let (signal, tx, mut rx_terminate) = ProducerDeadSignal::new();
        let tx_broadcast = {
            let mut lock = self.inner.write().await;

            lock.dead_signals
                .entry(producer_id)
                .or_insert_with(|| {
                    let (tx, _) = broadcast::channel(1);
                    tx
                })
                .clone()
        };

        let mut rx_broadcast = tx_broadcast.subscribe();
        tokio::spawn(async move {
            tokio::select! {
                _ = &mut rx_terminate => (),
                _ = rx_broadcast.recv() => (),
            }
            let _ = tx.send(());
        });
        signal
    }
}
