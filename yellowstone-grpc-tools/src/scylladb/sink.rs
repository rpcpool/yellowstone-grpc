use {
    super::{
        agent::{AgentHandler, Nothing, Ticker},
        prom::{
            scylladb_batch_request_lag_inc, scylladb_batch_request_lag_sub,
            scylladb_batch_sent_inc, scylladb_batch_size_observe, scylladb_batchitem_sent_inc_by,
        },
        types::{Reward, Transaction},
    }, crate::scylladb::{agent::AgentSystem, types::AccountUpdate}, deepsize::DeepSizeOf, futures::{Future, FutureExt, SinkExt}, scylla::{
        batch::{Batch, BatchStatement},
        frame::{response::result::ColumnType, Compression},
        prepared_statement::PreparedStatement,
        routing::Token,
        serialize::{
            batch::{BatchValues, BatchValuesIterator},
            row::{RowSerializationContext, SerializeRow, SerializedValues},
        },
        transport::errors::QueryError,
        QueryResult, Session, SessionBuilder,
    }, std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration}, tokio::{
        task::JoinSet,
        time::{self, Instant, Sleep},
    }, tonic::async_trait, tracing::{debug, error, info, warn}
};

const SCYLLADB_ACCOUNT_UPDATE_LOG_TABLE_NAME: &str = "account_update_log";
const SCYLLADB_TRANSACTION_LOG_TABLE_NAME: &str = "transaction_log";

const SCYLLADB_INSERT_ACCOUNT_UPDATE: &str = r###"
    INSERT INTO solana.account_update_log (slot, pubkey, lamports, owner, executable, rent_epoch, write_version, data, txn_signature)
    VALUES (?,?,?,?,?,?,?,?,?)
"###;

const SCYLLADB_INSERT_TRANSACTION: &str = r###"
    INSERT INTO solana.transaction_log (
        slot, 
        signature, 
        recent_blockhash, 
        account_keys, 
        address_table_lookups, 
        instructions, 
        meta, 
        num_readonly_signed_accounts, 
        num_readonly_unsigned_accounts,
        num_required_signatures,
        signatures,
        versioned
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
"###;

#[derive(Clone, PartialEq, Debug)]
pub struct ScyllaSinkConfig {
    pub batch_len_limit: usize,
    pub batch_size_kb_limit: usize,
    pub linger: Duration,
    pub keyspace: String,
    pub max_inflight_batch_delivery: usize,
}

impl Default for ScyllaSinkConfig {
    fn default() -> Self {
        Self {
            batch_len_limit: 300,
            batch_size_kb_limit: 1024 * 128,
            linger: Duration::from_millis(10),
            keyspace: String::from("solana"),
            max_inflight_batch_delivery: 100,
        }
    }
}

#[derive(Default, Clone)]
struct LogBuffer(Batch, Vec<BatchItem>);

impl LogBuffer {

    fn with_capacity(capacity: usize) -> LogBuffer {
        LogBuffer(Batch::default(), Vec::with_capacity(capacity))
    }

    fn split(self) -> (Batch, Vec<BatchItem>) {
        (self.0, self.1)
    }

    fn push(&mut self, br: BatchRequest) {
        self.0.append_statement(br.stmt);
        self.1.push(br.item);
    }

    fn len(&self) -> usize {
        self.1.len()
    }

    fn is_empty(&self) -> bool {
        self.1.is_empty()
    }
}



#[derive(Debug, Clone, DeepSizeOf)]
enum BatchItem {
    // Add other action if necessary...
    Account(AccountUpdate),
    Transaction(Transaction),
}

#[derive(Clone)]
struct BatchRequest {
    stmt: BatchStatement,
    item: BatchItem,
}

impl SerializeRow for BatchItem {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut scylla::serialize::RowWriter,
    ) -> Result<(), scylla::serialize::SerializationError> {
        match self {
            BatchItem::Account(acc) => acc.serialize(ctx, writer),
            BatchItem::Transaction(tx ) => tx.serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            BatchItem::Account(acc) => acc.is_empty(),
            BatchItem::Transaction(tx ) => tx.is_empty(),
        }
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
            BatchItem::Transaction(tx) => {
                let slot = tx.slot;
                let sig = &tx.signature;
                let mut pk_ser = SerializedValues::new();
                pk_ser.add_value(&slot, &ColumnType::BigInt).unwrap();
                pk_ser.add_value(sig, &ColumnType::Blob).unwrap();
                pk_ser
            }
        }
    }

    fn resolve_token(&self, tt: &impl TokenTopology) -> Token {
        let pk = self.get_partition_key();

        let table = match self {
            BatchItem::Account(_) => SCYLLADB_ACCOUNT_UPDATE_LOG_TABLE_NAME,
            BatchItem::Transaction(_) => SCYLLADB_TRANSACTION_LOG_TABLE_NAME,
        };
        tt.compute_token(table, &pk)
    }

}

type NodeUuid = u128;

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
    type Input = LogBuffer;
    type Error = BatchSenderError;

    async fn tick(
        &mut self,
        now: Instant,
        msg: LogBuffer,
    ) -> Result<Nothing, BatchSenderError> {
        scylladb_batch_size_observe(msg.len());
        let session = Arc::clone(&self.session);
        let batch_len = msg.len();
        while self.js.len() >= self.max_inflight {
            let _result = self.js
                .join_next()
                .await
                .unwrap()
                .map_err(|_join_error| BatchSenderError::ChannelClosed)??;
        }
        
        let (stmts, rows) = msg.split();

        let prepared_batch = self.session
            .prepare_batch(&stmts)
            .await
            .map_err(|e| BatchSenderError::ScyllaError(e))?;

        self.js.spawn(async move {
            let result = session
                .batch(&prepared_batch, &rows)
                .await
                .map(|_| ())
                .map_err(BatchSenderError::ScyllaError);

            scylladb_batch_sent_inc();
            scylladb_batchitem_sent_inc_by(batch_len as u64);
            scylladb_batch_request_lag_sub(batch_len as i64);
            result
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
    batch_len_limit: usize,
    batch_size_limit_kb: usize,
    batch: Option<LogBuffer>,
    curr_batch_len: usize,
    curr_batch_size_in_bytes: usize,
    batch_sender_handle: AgentHandler<LogBuffer>,
    timer: Timer,
}

impl TimedBatcher {
    fn new(
        batch_len_limit: usize,
        batch_size_limit_kb: usize,
        batch_sender_handle: AgentHandler<LogBuffer>,
        batch_linger: Duration,
    ) -> Self {
        TimedBatcher {
            batch_len_limit,
            batch_size_limit_kb,
            batch: None,
            curr_batch_len: 0,
            curr_batch_size_in_bytes: 0,
            batch_sender_handle,
            timer: Timer::new(batch_linger),
        }
    }

    fn get_batch_mut(&mut self) -> &mut LogBuffer {
        if self.batch.is_none() {
            self.batch.replace(LogBuffer::with_capacity(self.batch_len_limit));
        }
        self.batch.as_mut().unwrap()
    }

    async fn flush(&mut self, _now: Instant) -> Result<(), ()> {
        self.curr_batch_len = 0;
        self.curr_batch_size_in_bytes = 0;
        if let Some(batch) = self.batch.take() {
            if !batch.is_empty() {
                return self.batch_sender_handle.send(batch).await;
            }
        }
        self.timer.restart();
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
            let msg_bytes = msg.item.deep_size_of();
            if (self.curr_batch_size_in_bytes + msg_bytes) / 1024 >= self.batch_size_limit_kb {
                self.flush(now).await?;
            }

            let mybatch = self.get_batch_mut();

            mybatch.push(msg);
            self.curr_batch_len += 1;
            self.curr_batch_size_in_bytes += msg_bytes;
        }
        if self.curr_batch_len >= self.batch_len_limit {
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
    insert_tx_ps: PreparedStatement,
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

        let insert_tx_ps = session
            .prepare(SCYLLADB_INSERT_TRANSACTION)
            .await
            .unwrap();

        let token_topology = LiveTokenTopology(Arc::clone(&session));

        let system = AgentSystem::new(10000);

        let lbs = LiveBatchSender::new(Arc::clone(&session), 160);
        let lbs_handler = system.spawn(lbs);

        let mut batchers = vec![];
        for _ in 1..4 {
            let tb = TimedBatcher::new(config.batch_len_limit, config.batch_size_kb_limit, lbs_handler.clone(), config.linger);
            let ah = system.spawn(tb);
            batchers.push(ah)
        }

        let router = TokenAwareBatchRouter::new(token_topology, batchers);

        let router_handle = system.spawn(router);

        //let batch_sender_handle = spawn_live_batch_sender(Arc::clone(&session), config.batch_size_limit, 2300);
        ScyllaSink {
            insert_account_update_ps,
            insert_tx_ps,
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

    pub async fn log_transaction(&mut self, tx: Transaction) -> Result<(), ScyllaSinkError> {
        let br = BatchRequest {
            stmt: BatchStatement::PreparedStatement(self.insert_tx_ps.clone()),
            item: BatchItem::Transaction(tx),
        };
        self.batch_router_handle.send(br)
            .await
            .map_err(|_e| ScyllaSinkError::SinkClose)
    }
}
