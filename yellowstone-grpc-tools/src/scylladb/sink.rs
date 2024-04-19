use {
    super::{
        agent::{AgentHandler, Nothing, Ticker, WatchSignal},
        prom::{
            scylladb_batch_request_lag_inc, scylladb_batch_request_lag_sub,
            scylladb_batch_sent_inc, scylladb_batch_size_observe, scylladb_batchitem_sent_inc_by,
        },
        types::{BlockchainEvent, Reward, ShardId, ShardOffset, ShardStatistics, Transaction, SHARD_OFFSET_MODULO},
    },
    crate::scylladb::{agent::AgentSystem, types::AccountUpdate},
    deepsize::DeepSizeOf,
    futures::{future::Join, Future, FutureExt},
    scylla::{
        batch::{self, Batch, BatchStatement}, frame::{response::result::ColumnType, Compression}, prepared_statement::PreparedStatement, routing::Token, serialize::row::{RowSerializationContext, SerializeRow, SerializedValues}, transport::{errors::QueryError, Node}, QueryResult, Session, SessionBuilder
    },
    std::{any, collections::{hash_map::DefaultHasher, HashMap}, hash::{self, Hasher}, ops::Index, pin::Pin, sync::Arc, time::Duration},
    tokio::{
        sync::{mpsc::error::TrySendError, oneshot}, task::JoinSet, time::{self, Instant, Sleep}
    },
    tonic::async_trait,
    tracing::{error, info, instrument::WithSubscriber, warn},
};

const SCYLLADB_SOLANA_LOG_TABLE_NAME: &str = "log";


// The following query always return the latest period because
// of how the table was created, see DESC solana.shard_statistics
// Looking at the table's DDL, you'll see that period is the clustering key sorted in descending.
// Scylla uses cluster key ordering as the select default ordering.
const SCYLLADB_GET_SHARD_STATISTICS: &str = r###"
    SELECT
        shard_id,
        period,
        min_slot,
        max_slot,
        total_events,
        slot_event_counter
    FROM shard_statistics
    WHERE shard_id = ?
    PER PARTITION LIMIT 1
"###;

const SCYLLADB_GET_MAX_OFFSET_FOR_SHARD: &str = r###"
    SELECT
        offset
    FROM log
    WHERE shard_id = ? AND period = ?
    PER PARTITION LIMIT 1
"###;


const SCYLLADB_INSERT_ACCOUNT_UPDATE: &str = r###"
    INSERT INTO log (
        shard_id, 
        period,
        offset,
        slot,
        entry_type,


        pubkey, 
        lamports, 
        owner, 
        executable, 
        rent_epoch, 
        write_version, 
        data, 
        txn_signature,
        created_at
    )
    VALUES (?,?,?,?,0, ?,?,?,?,?,?,?,?, currentTimestamp())
"###;

// const SCYLLADB_INSERT_TRANSACTION: &str = r###"
//     INSERT INTO solana.transaction_log (
//         slot, 
//         signature, 
//         recent_blockhash, 
//         account_keys, 
//         address_table_lookups, 
//         instructions, 
//         meta, 
//         num_readonly_signed_accounts, 
//         num_readonly_unsigned_accounts,
//         num_required_signatures,
//         signatures,
//         versioned
//     )
//     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
// "###;

const SCYLLADB_INSERT_TRANSACTION: &str = r###"
    INSERT INTO log (
        shard_id, 
        period,
        offset,
        slot,
        entry_type,

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
        versioned,

        created_at
    )
    VALUES (?,?,?,?,1, ?,?,?,?,?,?,?,?,?,?,?, currentTimestamp())
"###;



const DEFAULT_SHARD_COUNT: i16 = 256;

#[derive(Clone, PartialEq, Debug)]
pub struct ScyllaSinkConfig {
    pub batch_len_limit: usize,
    pub batch_size_kb_limit: usize,
    pub linger: Duration,
    pub keyspace: String,
    pub max_inflight_batch_delivery: usize,
    pub shard_count: ShardId,
}

impl Default for ScyllaSinkConfig {
    fn default() -> Self {
        Self {
            batch_len_limit: 300,
            batch_size_kb_limit: 1024 * 128,
            linger: Duration::from_millis(10),
            keyspace: String::from("solana"),
            max_inflight_batch_delivery: 100,
            shard_count: DEFAULT_SHARD_COUNT,
        }
    }
}

#[derive(Default, Clone)]
struct LogBuffer {
    scylla_batch: Batch, 
    rows: Vec<BlockchainEvent>, 
    curr_byte_size: usize,
    capacity: usize,
    byte_size_capacity: usize,
}


impl LogBuffer {
    fn with_capacity(capacity: usize, byte_size_capacity: usize) -> LogBuffer {
        LogBuffer { 
            scylla_batch: Batch::default(), 
            rows: Vec::with_capacity(capacity), 
            curr_byte_size: 0,
            capacity,
            byte_size_capacity
        }
    }

    fn push(&mut self, br: ShardedBatchRequest) -> Result<(), ShardedBatchRequest> {
        let data_size = br.item.deep_size_of();
        if self.rows.len() == self.capacity {
            Err(br)
        } else if (self.curr_byte_size + data_size) > self.byte_size_capacity {
            Err(br)
        } else {
            self.scylla_batch.append_statement(br.stmt);
            self.curr_byte_size += data_size;
            self.rows.push(br.item);
            Ok(())
        }
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.scylla_batch.statements.clear();
    }
}

#[allow(clippy::large_enum_variant)]
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

impl BatchRequest {

    fn with_shard(self, shard_id: ShardId, offset: ShardOffset) -> ShardedBatchRequest {

        let event = match self.item {
            BatchItem::Account(acc) => acc.as_blockchain_event(shard_id, offset),
            BatchItem::Transaction(tx) => tx.as_blockchain_event(shard_id, offset),
        };

        ShardedBatchRequest {
            stmt: self.stmt,
            item: event
        }
    }
}

#[derive(Clone)]
struct ShardedBatchRequest {
    stmt: BatchStatement,
    item: BlockchainEvent
}


type NodeUuid = u128;

pub trait TokenTopology {
    fn get_node_uuid_for_token(&self, token: Token) -> NodeUuid;

    fn is_node_uuid_exists(&self, node_uuid: NodeUuid) -> bool;

    fn get_node_uuids(&self) -> Vec<NodeUuid>;

    fn get_number_of_nodes(&self) -> usize;

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

    fn get_number_of_nodes(&self) -> usize {
        self.0
            .get_cluster_data()
            .get_nodes_info()
            .len()
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
    timer: Timer,
    buffer: LogBuffer,
    watcher_signals: Vec<WatchSignal>,
}

impl LiveBatchSender {
    fn new(
        session: Arc<Session>, 
        linger: Duration,
    ) -> Self {
        LiveBatchSender {
            session,
            timer: Timer::new(linger),
            buffer: LogBuffer::with_capacity(300, 16000000),
            watcher_signals: Vec::with_capacity(10),
        }
    }
    
    async fn flush(&mut self) -> anyhow::Result<Nothing> {
        self.timer.restart();
        let batch = &self.buffer;

        scylladb_batch_size_observe(batch.len());
        let session: Arc<Session> = Arc::clone(&self.session);
        let batch_len = batch.len();

        info!("Sending batch of length: {:?}", batch_len);
        let prepared_batch = self
            .session
            .prepare_batch(&batch.scylla_batch)
            .await
            .map_err(anyhow::Error::new)?;

        let result = {
            let rows = &self.buffer.rows;
            session
                    .batch(&prepared_batch, rows)
                    .await
                    .map(|_| ())
                    .map_err(anyhow::Error::new)
        };

        self.watcher_signals.drain(..).for_each(|ws| {
            if let Err(e) = ws.send(()) {
                warn!("Notification failed")
            }
        });

        // TODO : don't drop the element here
        self.buffer.clear();
        scylladb_batch_sent_inc();
        scylladb_batchitem_sent_inc_by(batch_len as u64);
        scylladb_batch_request_lag_sub(batch_len as i64);
        result
    }

    

}

#[async_trait]
impl Ticker for LiveBatchSender {
    type Input = ShardedBatchRequest;

    fn timeout(&self) -> Pin<Box<dyn Future<Output = Nothing> + Send + 'static>> {
        self.timer.sleep().boxed()
    }

    async fn on_timeout(&mut self, _now: Instant) -> anyhow::Result<Nothing> {
        if self.buffer.len() > 0 {
            self.flush().await
        } else {
            Ok(())
        }
    }

    async fn tick(&mut self, _now: Instant, msg: ShardedBatchRequest) -> Result<Nothing, anyhow::Error> {
        let result = self.buffer.push(msg);
        if let Err(msg) = result { 
            let _ = self.flush().await?;
            self.buffer.push(msg).err().unwrap();
        } 
        Ok(())
    }

    async fn tick_with_watch(&mut self, now: Instant, msg: Self::Input, ws: WatchSignal) -> anyhow::Result<Nothing> {
        self.tick(now, msg).await?;
        self.watcher_signals.push(ws);
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

struct Shard {
    shard_id: ShardId,
    next_offset: ShardOffset,
    token_topology: Arc<dyn TokenTopology + Send + Sync>,
    batchers: Arc<[AgentHandler<ShardedBatchRequest>]>,
    current_batcher: Option<usize>,
}

impl Shard {
    fn new(
        shard_id: ShardId,
        next_offset: ShardOffset,
        token_topology: Arc<dyn TokenTopology + Send + Sync>,
        batchers: Arc<[AgentHandler<ShardedBatchRequest>]>,
    ) -> Self {
        Shard {
            shard_id,
            next_offset,
            token_topology,
            batchers,
            current_batcher: None
        }
    }

    fn get_batcher_idx_for_token(&self, token: Token) -> usize {
        let node_uuid = self.token_topology.get_node_uuid_for_token(token);
        let mut node_uuids = self.token_topology.get_node_uuids();
        node_uuids.sort();
        if let Ok(i) = node_uuids.binary_search(&node_uuid) {
            i % self.batchers.len()
        } else {
            warn!("Token topology didn't know about {:?} at the time of batcher assignment.", node_uuid);
            0
        }
    }

}

#[async_trait]
impl Ticker for Shard {
    type Input = BatchRequest;

    async fn tick(&mut self, now: Instant, msg: BatchRequest) -> anyhow::Result<Nothing> {
        let shard_id = self.shard_id;
        let offset = self.next_offset;
        self.next_offset += 1;

        // Resolve the partition key
        let next_period = (offset / SHARD_OFFSET_MODULO) * SHARD_OFFSET_MODULO;
        let mut partition_key = SerializedValues::new();
        partition_key.add_value(&shard_id, &ColumnType::SmallInt).unwrap();
        partition_key.add_value(&next_period, &ColumnType::BigInt).unwrap();
        let token = self.token_topology.compute_token(SCYLLADB_SOLANA_LOG_TABLE_NAME, &partition_key);

        let batcher_idx = if offset % SHARD_OFFSET_MODULO == 0 {
            // If we enter a new time period we need to decide on another batcher so we don't stick to the same
            // batcher
            let idx = self.get_batcher_idx_for_token(token);
            if self.current_batcher.is_some() {
                // It is never suppose to happen
                panic!("Sharder is trying to get a new batcher while he's holding one already");
            }
            idx
        } else {
            match self.current_batcher {
                Some(idx) => idx,
                None => self.get_batcher_idx_for_token(token),
            }
        };
        self.current_batcher.replace(batcher_idx);
        let batcher = &self.batchers[batcher_idx];
        let sharded = msg.with_shard(shard_id, offset);

        // Handle the end of a period
        if (offset + 1) % SHARD_OFFSET_MODULO == 0 {
            // Remove the current_batcher so next round we decide on a new batcher
            let _ = self.current_batcher.take();

            // With watch allows us to block until the period is either completly committed or abandonned
            // before sharding again.
            let watch = batcher.send_with_watch(sharded).await?;
            
            watch.await.map_err(anyhow::Error::new)
        } else {
            batcher.send(sharded).await
        }

    }
}


struct RoundRobinShardRouter {
    sharders: Vec<AgentHandler<BatchRequest>>,
    sharder_idx: usize,
}



impl RoundRobinShardRouter {
    pub fn new(
        batchers: Vec<AgentHandler<BatchRequest>>,
    ) -> Self {
        let mut res = RoundRobinShardRouter {
            sharders: batchers,
            sharder_idx: 0,
        };
        res
    }
}

#[async_trait]
impl Ticker for RoundRobinShardRouter {
    type Input = BatchRequest;

    async fn tick(&mut self, _now: Instant, msg: BatchRequest) -> Result<Nothing, anyhow::Error> {
        let begin = self.sharder_idx;

        let maybe_permit = self.sharders
            .iter()
            .enumerate()
            .cycle()
            .skip(begin)
            .take(self.sharders.len())
            .find_map(|(i, sharder)| {
                match sharder.try_reserve() {
                    Ok(permit) => {
                        Some((i, permit))
                    },
                    Err(TrySendError::Closed(_)) => {
                        None
                    },
                    _ => None
                }
            });

        if let Some((i, permit)) = maybe_permit {

            self.sharder_idx = std::cmp::max(1, i);
            scylladb_batch_request_lag_inc();
            permit.send(msg);
            return Ok(())
        } else {
            
            // Pick the first living sharder and wait until it becomes available;

            for sharder in self.sharders.iter() {
                let result = sharder.reserve().await;
                if let Ok(permit) = result {
                    permit.send(msg);
                    scylladb_batch_request_lag_inc();
                    return Ok(());
                }
            }
            return Err(anyhow::anyhow!("failed to find a sharder, message is dropped"));
        }
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


async fn get_shard_stat(session: Arc<Session>, shard_id: i16) -> anyhow::Result<Option<ShardStatistics>> {
    session
        .query(SCYLLADB_GET_SHARD_STATISTICS, (shard_id,))
        .await?
        .maybe_first_row()?
        .map(|row| row.into_typed())
        .transpose()
        .map_err(anyhow::Error::new)
}

async fn get_next_offset_for_shard(session: Arc<Session>, shard_id: i16) -> anyhow::Result<i64> {
    let last_period = get_shard_stat(Arc::clone(&session), shard_id)
        .await?
        .map(|stats| stats.period)
        .unwrap_or(0);

    session.query(SCYLLADB_GET_MAX_OFFSET_FOR_SHARD, (shard_id, last_period))
        .await?
        .maybe_first_row_typed::<(ShardOffset,)>()
        .map(|maybe| maybe.unwrap_or((-1,)))
        .map(|tuple| tuple.0 + 1)
        .map_err(anyhow::Error::new)
}   

impl ScyllaSink {
    pub async fn new(
        config: ScyllaSinkConfig,
        hostname: impl AsRef<str>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> anyhow::Result<Self> {
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

        let insert_tx_ps = session.prepare(SCYLLADB_INSERT_TRANSACTION).await.unwrap();

        let token_topology: Arc<dyn TokenTopology + Send + Sync> = Arc::new(LiveTokenTopology(Arc::clone(&session)));

        let system = AgentSystem::new(16);

        let num_batcher = (config.shard_count / 2) as usize;
        let mut batchers = Vec::with_capacity(num_batcher);
        for _ in 0..num_batcher {
            let lbs = LiveBatchSender::new(Arc::clone(&session), config.linger);
            let lbs_handler = system.spawn(lbs);
            batchers.push(lbs_handler);
        }
        
        let batchers: Arc<[AgentHandler<ShardedBatchRequest>]> = Arc::from(batchers.into_boxed_slice());

        let mut sharders = vec![];
        let mut js: JoinSet<anyhow::Result<Shard>> = JoinSet::new();
        for shard_id in 0..config.shard_count {
            let session = Arc::clone(&session);
            let tt = Arc::clone(&token_topology);
            let batchers = Arc::clone(&batchers);
            js.spawn(async move {
                let before = Instant::now();
                let next_offset = get_next_offset_for_shard(session, shard_id).await?;
                let tb = Shard::new(
                    shard_id, 
                    next_offset, 
                    tt,
                    batchers,
                );
                info!("sharder {:?} next_offset: {:?}, stats collected in: {:?}", shard_id, next_offset, before.elapsed());
                Ok(tb)
            });
        }

        while let Some(result) = js.join_next().await {
            let shard = result.unwrap().unwrap();
            sharders.push(system.spawn(shard));
        }

        let router = RoundRobinShardRouter::new(sharders);

        let router_handle = system.spawn(router);
        info!("Shard router has started.");

        Ok(ScyllaSink {
            insert_account_update_ps,
            insert_tx_ps,
            batch_router_handle: router_handle,
        })
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
        self.batch_router_handle
            .send(br)
            .await
            .map_err(|_e| ScyllaSinkError::SinkClose)
    }
}
