use {
    super::{
        agent::{AgentHandler, Nothing, Ticker},
        prom::{
            scylladb_batch_request_lag_inc, scylladb_batch_request_lag_sub,
            scylladb_batch_sent_inc, scylladb_batch_size_observe, scylladb_batchitem_sent_inc_by,
        },
        types::Reward,
    },
    crate::scylladb::{agent::AgentSystem, types::AccountUpdate},
    futures::{Future, FutureExt},
    scylla::{
        batch::{Batch, BatchStatement},
        frame::{response::result::ColumnType, Compression},
        prepared_statement::PreparedStatement,
        routing::Token,
        serialize::{
            batch::{BatchValues, BatchValuesIterator},
            row::{RowSerializationContext, SerializedValues},
        },
        transport::errors::QueryError,
        QueryResult, Session, SessionBuilder,
    },
    std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration},
    tokio::{
        task::JoinSet,
        time::{self, Instant, Sleep},
    },
    tonic::async_trait,
    tracing::{info, warn, debug},
};

const SCYLLADB_ACCOUNT_UPDATE_LOG_TABLE_NAME: &str = "account_update_log";

const SCYLLADB_INSERT_ACCOUNT_UPDATE: &str = r###"
    INSERT INTO account_update_log (slot, pubkey, lamports, owner, executable, rent_epoch, write_version, data, txn_signature)
    VALUES (?,?,?,?,?,?,?,?,?)
"###;

#[derive(Clone, PartialEq, Debug)]
pub struct ScyllaSinkConfig {
    pub batch_size_limit: usize,
    pub linger: Duration,
    pub keyspace: String,
    pub max_inflight_batch_delivery: usize,
}

impl Default for ScyllaSinkConfig {
    fn default() -> Self {
        Self {
            batch_size_limit: 3000,
            linger: Duration::from_millis(10),
            keyspace: String::from("solana"),
            max_inflight_batch_delivery: 100,
        }
    }
}

enum BatchItem {
    // Add other action if necessary...
    Account(AccountUpdate),
}

pub struct BatchRequest {
    stmt: BatchStatement,
    item: BatchItem,
}

impl From<AccountUpdate> for SerializedValues {
    fn from(account_update: AccountUpdate) -> Self {
        let mut row = SerializedValues::new();
        row.add_value(&account_update.slot, &ColumnType::BigInt)
            .unwrap();
        row.add_value(&account_update.pubkey, &ColumnType::Blob)
            .unwrap();
        row.add_value(&account_update.lamports, &ColumnType::BigInt)
            .unwrap();
        row.add_value(&account_update.owner, &ColumnType::Blob)
            .unwrap();
        row.add_value(&account_update.executable, &ColumnType::Boolean)
            .unwrap();
        row.add_value(&account_update.rent_epoch, &ColumnType::BigInt)
            .unwrap();
        row.add_value(&account_update.write_version, &ColumnType::BigInt)
            .unwrap();
        row.add_value(&account_update.data, &ColumnType::Blob)
            .unwrap();
        row.add_value(&account_update.txn_signature, &ColumnType::Blob)
            .unwrap();
        row
    }
}

impl BatchItem {
    fn get_partition_key(&self) -> SerializedValues {
        match self {
            BatchItem::Account(acc_update) => {
                let slot = acc_update.slot;
                let pubkey = acc_update.pubkey;
                let mut pk_ser = SerializedValues::new();
                pk_ser.add_value(&slot, &ColumnType::BigInt).unwrap();
                pk_ser.add_value(&pubkey, &ColumnType::Blob).unwrap();
                pk_ser
            }
        }
    }

    fn resolve_token(&self, tt: &impl TokenTopology) -> Token {
        let pk = self.get_partition_key();

        let table = match self {
            BatchItem::Account(_) => SCYLLADB_ACCOUNT_UPDATE_LOG_TABLE_NAME,
        };
        tt.compute_token(table, &pk)
    }

    fn serialize(self) -> SerializedValues {
        match self {
            BatchItem::Account(acc_update) => acc_update.into(),
        }
    }
}

#[async_trait]
pub trait ScyllaBatcher {
    async fn batch(&self, br: BatchRequest);
}

type NodeUuid = u128;

struct PreSerializedBatchValuesIterator<'a> {
    inner: &'a [SerializedValues],
    i: usize,
}

struct PreSerializedBatchValues(Vec<SerializedValues>);

impl BatchValues for PreSerializedBatchValues {
    type BatchValuesIter<'r> = PreSerializedBatchValuesIterator<'r>;

    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        PreSerializedBatchValuesIterator {
            inner: &self.0,
            i: 0,
        }
    }
}

impl<'bv> BatchValuesIterator<'bv> for PreSerializedBatchValuesIterator<'bv> {
    fn serialize_next(
        &mut self,
        _ctx: &RowSerializationContext<'_>,
        writer: &mut scylla::serialize::RowWriter,
    ) -> Option<Result<(), scylla::serialize::SerializationError>> {
        writer.append_serialize_row(self.inner.get(self.i).unwrap());
        self.i += 1;
        Some(Ok(()))
    }

    fn is_empty_next(&mut self) -> Option<bool> {
        self.inner.get(self.i).map(|_| true)
    }

    fn skip_next(&mut self) -> Option<()> {
        self.inner.get(self.i).map(|_| {
            self.i += 1;
        })
    }
}

pub trait TokenTopology {
    fn get_node_uuid_for_token(&self, token: Token) -> NodeUuid;

    fn is_node_uuid_exists(&self, node_uuid: NodeUuid) -> bool;

    fn get_node_uuids(&self) -> Vec<NodeUuid>;

    fn compute_token(&self, table: &str, serialized_values: &SerializedValues) -> Token;
}

#[derive(Clone)]
struct LiveTokenTopology(Arc<Session>);

impl TokenTopology for LiveTokenTopology {
    fn get_node_uuid_for_token(&self, token: Token) -> NodeUuid {
        self.0
            .get_cluster_data()
            .replica_locator()
            .ring()
            .get_elem_for_token(token)
            .map(|node| node.host_id.as_u128())
            .unwrap() // If it is None it means we have no more -> we need to crash asap!
    }

    fn is_node_uuid_exists(&self, node_uuid: NodeUuid) -> bool {
        self.0
            .get_cluster_data()
            .get_nodes_info()
            .iter()
            .any(|node| node.host_id.as_u128() == node_uuid)
    }

    fn get_node_uuids(&self) -> Vec<NodeUuid> {
        self.0
            .get_cluster_data()
            .get_nodes_info()
            .iter()
            .map(|node| node.host_id.as_u128())
            .collect()
    }

    fn compute_token(&self, table: &str, partition_key: &SerializedValues) -> Token {
        let current_keysapce = self.0.get_keyspace().unwrap();
        self.0
            .get_cluster_data()
            .compute_token(&current_keysapce, table, partition_key)
            .unwrap()
    }
}

#[async_trait]
pub trait BatchSender: Send + Sync + Clone {
    async fn send_batch(
        self,
        batch: Batch,
        serialized_rows: Vec<SerializedValues>,
    ) -> Result<(), QueryError>;
}
struct LiveBatchSender {
    session: Arc<Session>,
    js: JoinSet<Result<(), BatchSenderError>>,
    max_inflight: usize,
}

impl LiveBatchSender {
    fn new(session: Arc<Session>, max_inflight: usize) -> Self {
        LiveBatchSender {
            session,
            js: JoinSet::new(),
            max_inflight,
        }
    }
}

#[derive(Debug)]
enum BatchSenderError {
    ScyllaError(QueryError),
    ChannelClosed,
}

#[async_trait]
impl Ticker for LiveBatchSender {
    type Input = (Batch, Vec<SerializedValues>);
    type Error = BatchSenderError;

    async fn tick(
        &mut self,
        now: Instant,
        msg: (Batch, Vec<SerializedValues>),
    ) -> Result<Nothing, BatchSenderError> {
        let (batch, ser_values) = msg;
        scylladb_batch_size_observe(batch.statements.len());
        let session = Arc::clone(&self.session);

        while self.js.len() >= self.max_inflight {
            let result = self.js.join_next().await.unwrap();
            if result.is_err() {
                return Err(BatchSenderError::ChannelClosed);
            }
        }

        self.js.spawn(async move {
            let result = session
                .batch(&batch, PreSerializedBatchValues(ser_values))
                .await
                .map(|_| ());

            let after = Instant::now();
            debug!(
                "Batch sent: size={:?}, latency={:?}",
                batch.statements.len() as i64,
                after - now
            );

            scylladb_batch_sent_inc();
            scylladb_batchitem_sent_inc_by(batch.statements.len() as u64);
            scylladb_batch_request_lag_sub(batch.statements.len() as i64);
            result.map_err(BatchSenderError::ScyllaError)
        });
        Ok(())
    }
}

struct Timer {
    deadline: Instant,
    linger: Duration,
}

impl Timer {
    fn new(linger: Duration) -> Self {
        Timer {
            deadline: Instant::now(),
            linger,
        }
    }

    fn restart(&mut self) {
        self.deadline = Instant::now() + self.linger;
    }

    fn sleep(&self) -> Sleep {
        time::sleep_until(self.deadline)
    }
}

struct TimedBatcher {
    batch_size_limit: usize,
    batch: Vec<(Batch, Vec<SerializedValues>)>,
    curr_batch_size: usize,
    batch_sender_handle: AgentHandler<(Batch, Vec<SerializedValues>)>,
    timer: Timer,
}

impl TimedBatcher {
    fn new(
        batch_size_limit: usize,
        batch_sender_handle: AgentHandler<(Batch, Vec<SerializedValues>)>,
        batch_linger: Duration,
    ) -> Self {
        TimedBatcher {
            batch_size_limit,
            batch: Vec::new(),
            curr_batch_size: 0,
            batch_sender_handle,
            timer: Timer::new(batch_linger),
        }
    }

    fn get_batch_mut(&mut self) -> &mut (Batch, Vec<SerializedValues>) {
        if self.batch.is_empty() {
            self.batch
                .push((Batch::default(), Vec::with_capacity(self.batch_size_limit)));
        }
        self.batch.get_mut(0).unwrap()
    }

    async fn flush(&mut self, _now: Instant) -> Result<(), ()> {
        self.curr_batch_size = 0;
        if let Some(batch) = self.batch.pop() {
            if !batch.1.is_empty() {
                return self.batch_sender_handle.send(batch).await;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Ticker for TimedBatcher {
    type Input = BatchRequest;
    type Error = Nothing;

    fn timeout(&self) -> Pin<Box<dyn Future<Output = Nothing> + Send + 'static>> {
        self.timer.sleep().map(|_| ()).boxed()
    }

    async fn on_timeout(&mut self, now: Instant) -> Result<Nothing, Nothing> {
        self.timer.restart();
        self.flush(now).await
    }

    async fn terminate(&mut self, now: Instant) -> Result<Nothing, Nothing> {
        self.flush(now).await
    }

    async fn tick(&mut self, now: Instant, msg: BatchRequest) -> Result<Nothing, Nothing> {
        {
            let ser_row = msg.item.serialize();
            let mybatch = &mut self.get_batch_mut();
            mybatch.0.append_statement(msg.stmt);
            mybatch.1.push(ser_row);
            self.curr_batch_size += 1;
        }
        if self.curr_batch_size >= self.batch_size_limit {
            self.flush(now).await
        } else {
            Ok(())
        }
    }
}

struct TokenAwareBatchRouter {
    token_topology: LiveTokenTopology,
    batchers: Vec<AgentHandler<BatchRequest>>,
    node2batcher: HashMap<NodeUuid, usize>,
}

impl TokenAwareBatchRouter {
    pub fn new(
        token_topology: LiveTokenTopology,
        batchers: Vec<AgentHandler<BatchRequest>>,
    ) -> Self {
        let mut res = TokenAwareBatchRouter {
            token_topology,
            batchers,
            node2batcher: Default::default(),
        };
        res.compute_batcher_assignments();
        res
    }

    fn compute_batcher_assignments(&mut self) {
        let node_uuids = self.token_topology.get_node_uuids();
        let cycle_iter = self.batchers.iter().enumerate().map(|(i, _)| i).cycle();
        self.node2batcher = node_uuids.into_iter().zip(cycle_iter).collect();
    }
}

#[async_trait]
impl Ticker for TokenAwareBatchRouter {
    type Input = BatchRequest;
    type Error = Nothing;

    async fn tick(&mut self, _now: Instant, msg: BatchRequest) -> Result<Nothing, Nothing> {
        let token = msg.item.resolve_token(&self.token_topology);
        let node_uuid = self.token_topology.get_node_uuid_for_token(token);

        if !self.node2batcher.contains_key(&node_uuid) {
            self.compute_batcher_assignments();
            warn!("Recomputing node to batcher assignments -- Token Topology got changed");
        }

        // It should be safe to unwrap otherwise we need to crash fast and investigate.
        // If it panic, this would mean we got a node uuid that does not exists anymore.
        let batcher_idx = *self.node2batcher.get(&node_uuid).unwrap();

        let batcher = self.batchers.get(batcher_idx).unwrap();
        let result = batcher.send(msg).await;
        if result.is_ok() {
            scylladb_batch_request_lag_inc();
        }
        result
    }
}

pub struct ScyllaSink {
    insert_account_update_ps: PreparedStatement,
    batch_router_handle: AgentHandler<BatchRequest>,
}

#[derive(Debug)]
pub enum ScyllaSinkError {
    SinkClose,
}

pub struct Test {
    session: Arc<Session>,
}

impl Test {
    pub async fn new(
        hostname: impl AsRef<str>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Test {
        let session: Session = SessionBuilder::new()
            .known_node(hostname)
            .user(username, password)
            .compression(Some(Compression::Lz4))
            .use_keyspace("solana", false)
            .build()
            .await
            .unwrap();
        Test {
            session: Arc::new(session),
        }
    }

    pub async fn test(&self, id: i64, x: Reward) -> QueryResult {
        let ps = self
            .session
            .prepare("insert into test (id, x) values (?, ?)")
            .await
            .unwrap();
        self.session.execute(&ps, (id, x)).await.unwrap()
    }
}

impl ScyllaSink {
    pub async fn new(
        config: ScyllaSinkConfig,
        hostname: impl AsRef<str>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let session: Session = SessionBuilder::new()
            .known_node(hostname)
            .user(username, password)
            .compression(Some(Compression::Lz4))
            .use_keyspace(config.keyspace.clone(), false)
            .build()
            .await
            .unwrap();

        let session = Arc::new(session);
        let insert_account_update_ps = session
            .prepare(SCYLLADB_INSERT_ACCOUNT_UPDATE)
            .await
            .unwrap();

        let token_topology = LiveTokenTopology(Arc::clone(&session));

        let system = AgentSystem::new(10000);

        let lbs = LiveBatchSender::new(Arc::clone(&session), 160);
        let lbs_handler = system.spawn(lbs);

        let mut batchers = vec![];
        for _ in 1..4 {
            let tb = TimedBatcher::new(config.batch_size_limit, lbs_handler.clone(), config.linger);
            let ah = system.spawn(tb);
            batchers.push(ah)
        }

        let router = TokenAwareBatchRouter::new(token_topology, batchers);

        let router_handle = system.spawn(router);

        //let batch_sender_handle = spawn_live_batch_sender(Arc::clone(&session), config.batch_size_limit, 2300);
        ScyllaSink {
            insert_account_update_ps,
            batch_router_handle: router_handle,
        }
    }

    pub async fn log_account_update(
        &mut self,
        update: AccountUpdate,
    ) -> Result<(), ScyllaSinkError> {
        let br = BatchRequest {
            stmt: BatchStatement::PreparedStatement(self.insert_account_update_ps.clone()),
            item: BatchItem::Account(update),
        };
        self.batch_router_handle
            .send(br)
            .await
            .map_err(|_e| ScyllaSinkError::SinkClose)
    }
}
