use {
    super::{
        agent::{AgentHandler, Nothing, Ticker, WatchSignal},
        prom::{
            scylladb_batch_request_lag_inc, scylladb_batch_request_lag_sub,
            scylladb_batch_sent_inc, scylladb_batch_size_observe, scylladb_batchitem_sent_inc_by,
        },
        types::{
            BlockchainEvent, ProducerId, Reward, ShardId, ShardOffset, ShardPeriod, ShardStatistics, ShardedAccountUpdate, Transaction, SHARD_OFFSET_MODULO
        },
    }, crate::scylladb::{
        agent::AgentSystem,
        types::{AccountUpdate, BlockchainEventType, ShardedTransaction},
    }, anyhow::anyhow, deepsize::DeepSizeOf, futures::{future::ready, Future, FutureExt}, google_cloud_googleapis::r#type, google_cloud_pubsub::client::google_cloud_auth::token, lazy_static::lazy_static, rdkafka::producer::Producer, scylla::{
        batch::{Batch, BatchStatement},
        frame::{request::query, response::result::ColumnType, Compression},
        prepared_statement::PreparedStatement,
        routing::Token,
        serialize::{
            row::{RowSerializationContext, SerializeRow, SerializedValues}, value::SerializeCql, RowWriter
        },
        transport::{errors::QueryError, Node},
        QueryResult, Session, SessionBuilder,
    }, sha2::digest::typenum::Prod, std::{
        borrow::BorrowMut, collections::{HashMap, HashSet}, fmt::format, hash::{self, Hasher}, iter::repeat, path::Display, pin::Pin, sync::Arc, time::Duration
    }, tokio::{
        sync::{mpsc::error::TrySendError, oneshot},
        task::JoinSet,
        time::{self, Instant, Sleep},
    }, tonic::async_trait, tracing::{error, info, instrument::WithSubscriber, warn}
};

const SHARD_COUNT: i16 = 256;


const SCYLLADB_SOLANA_LOG_TABLE_NAME: &str = "log";


const SCYLLADB_GET_MAX_OFFSET_FOR_SHARD_MV: &str = r###"
SELECT
    offset
FROM shard_max_offset_mv 
WHERE shard_id = ?
ORDER BY offset DESC 
PER PARTITION LIMIT 1
"###;


lazy_static! {

    static ref SCYLLADB_GET_MAX_OFFSET_FOR_ALL_SHARD_MV: String = {
        
        let joined = (0..SHARD_COUNT).map(|x| format!("{:?}", x)).collect::<Vec<_>>().join(",");

        format!(r###"
            SELECT
                offset
            FROM shard_max_offset_mv 
            WHERE shard_id IN ({:?})
            ORDER BY offset DESC 
            PER PARTITION LIMIT 1
            "###, 
            joined
        )
    };
}


const SCYLLADB_INSERT_SHARD_STATISTICS: &str = r###"
    INSERT INTO shard_statistics (
        shard_id,
        period,
        producer_id,
        offset,
        min_slot,
        max_slot,
        total_events,
        slot_event_counter
    )
    VALUES (?,?,?,?,?,?,?,?)
"###;

const SCYLLADB_INSERT_ACCOUNT_UPDATE: &str = r###"
    INSERT INTO log (
        shard_id, 
        period,
        producer_id,
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
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,currentTimestamp())
"###;

const SCYLLADB_INSERT_TRANSACTION: &str = r###"
    INSERT INTO log (
        shard_id, 
        period,
        producer_id,
        offset,
        slot,
        entry_type,

        signature,
        signatures,
        num_readonly_signed_accounts, 
        num_readonly_unsigned_accounts,
        num_required_signatures,
        account_keys, 
        recent_blockhash, 
        instructions, 
        versioned,
        address_table_lookups, 
        meta, 

        created_at
    )
    VALUES (?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?,?, currentTimestamp())
"###;


#[derive(Clone, PartialEq, Debug)]
pub struct ScyllaSinkConfig {
    pub producer_id: u8,
    pub batch_len_limit: usize,
    pub batch_size_kb_limit: usize,
    pub linger: Duration,
    pub keyspace: String,
    pub shard_count: ShardId,
}


#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, DeepSizeOf)]
enum ClientCommand {
    // Add other action if necessary...
    InsertAccountUpdate(AccountUpdate),
    InsertTransaction(Transaction),
}

impl ClientCommand {
    pub fn slot(&self) -> i64 {
        match self {
            Self::InsertAccountUpdate(x) => x.slot,
            Self::InsertTransaction(x) => x.slot,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, DeepSizeOf)]
struct ShardedClientCommand {
    shard_id: ShardId,
    offset: ShardOffset,
    producer_id: ProducerId,
    //stmt: BatchStatement,
    client_command: ClientCommand,
}

impl SerializeRow for ShardedClientCommand {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), scylla::serialize::SerializationError> {
        //let period = (self.offset / SHARD_OFFSET_MODULO) * SHARD_OFFSET_MODULO;
        match &self.client_command {
            ClientCommand::InsertAccountUpdate(val) => {
                let val: ShardedAccountUpdate = val.clone().as_blockchain_event(self.shard_id, self.producer_id, self.offset).into();
                //let serval = SerializedValues::from_serializable(&ctx, &val);
                val.serialize(ctx, writer)
            }   
            ClientCommand::InsertTransaction(val) => {
                let val: ShardedTransaction = val.clone().as_blockchain_event(self.shard_id, self.producer_id, self.offset).into();
                //let serval = SerializedValues::from_serializable(&ctx, &val);
                val.serialize(ctx, writer)
            }
        }
    }

    fn is_empty(&self) -> bool {
        todo!()
    }
}


impl ClientCommand {
    fn with_shard_info(self, shard_id: ShardId, producer_id: ProducerId, offset: ShardOffset) -> ShardedClientCommand {
        ShardedClientCommand {
            shard_id,
            producer_id,
            offset,
            client_command: self,
        }
    }
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
        self.0.get_cluster_data().get_nodes_info().len()
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
    buffer: Vec<ShardedClientCommand>,
    scylla_batch: Batch,
    watcher_signals: Vec<WatchSignal>,
    insert_tx_query: BatchStatement,
    insert_acccount_update_query: BatchStatement,
    curr_batch_byte_size: usize,
    max_batch_capacity: usize,
    max_batch_byte_size: usize,
}

impl LiveBatchSender {
    fn new(
        session: Arc<Session>, 
        linger: Duration,
        max_batch_len: usize,
        max_batch_size_kb: usize,
    ) -> Self {
        LiveBatchSender {
            session,
            timer: Timer::new(linger),
            buffer: Vec::with_capacity(max_batch_len),
            scylla_batch: Batch::default(),
            watcher_signals: Vec::with_capacity(10),
            insert_tx_query: SCYLLADB_INSERT_TRANSACTION.into(),
            insert_acccount_update_query: SCYLLADB_INSERT_ACCOUNT_UPDATE.into(),
            curr_batch_byte_size: 0,
            max_batch_capacity: max_batch_len,
            max_batch_byte_size: max_batch_size_kb * 1000,
        }
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.scylla_batch.statements.clear();
        self.curr_batch_byte_size = 0;
    }

    async fn flush(&mut self) -> anyhow::Result<Nothing> {
        self.timer.restart();
        let batch_len = self.buffer.len();

        scylladb_batch_size_observe(batch_len);

        if batch_len == 0 {
            return Ok(());
        }

        let session: Arc<Session> = Arc::clone(&self.session);

        //info!("Sending batch of length: {:?}, curr_batch_size: {:?}", batch_len, self.curr_batch_byte_size);
        let prepared_batch = self
            .session
            .prepare_batch(&self.scylla_batch)
            .await
            .map_err(anyhow::Error::new)?;

        let result = {
            let rows = &self.buffer;

            session
                .batch(&prepared_batch, rows)
                .await
                .map(|_| ())
                .map_err(anyhow::Error::new)
        };

        self.watcher_signals.drain(..).for_each(|ws| {
            if let Err(_e) = ws.send(()) {
                warn!("Notification failed")
            }
        });

        // TODO : don't drop the element here
        self.clear();
        scylladb_batch_sent_inc();
        scylladb_batchitem_sent_inc_by(batch_len as u64);
        scylladb_batch_request_lag_sub(batch_len as i64);
        result
    }
}

#[async_trait]
impl Ticker for LiveBatchSender {
    type Input = ShardedClientCommand;

    fn timeout(&self) -> Pin<Box<dyn Future<Output = Nothing> + Send + 'static>> {
        if self.timer.deadline.elapsed() > Duration::from_millis(0) {
            // If deadline has already pass..
            ready(()).boxed()
        } else {
            self.timer.sleep().boxed()
        }
    }

    async fn init(&mut self) -> anyhow::Result<Nothing> {
        let mut batch_stmts = [
            self.insert_acccount_update_query.borrow_mut(),
            self.insert_tx_query.borrow_mut(),
        ];

        for stmt in batch_stmts {
            if let BatchStatement::Query(query) = stmt {
                let ps = self.session.prepare(query.clone()).await?;
                *stmt = BatchStatement::PreparedStatement(ps);
            };
        }

        Ok(())
    }

    async fn on_timeout(&mut self, _now: Instant) -> anyhow::Result<Nothing> {
        self.flush().await
    }

    async fn tick(
        &mut self,
        now: Instant,
        msg: ShardedClientCommand,
    ) -> Result<Nothing, anyhow::Error> {

        let msg_size = msg.deep_size_of();

        // TODO: make the capacity parameterized
        let need_flush = self.buffer.len() >= self.max_batch_capacity
            || (self.curr_batch_byte_size + msg_size) >= self.max_batch_byte_size;

        if need_flush {
            self.flush().await?;
        }

        let batch_stmt = match msg.client_command {
            ClientCommand::InsertAccountUpdate(_) => self.insert_acccount_update_query.clone(),
            ClientCommand::InsertTransaction(_) => self.insert_tx_query.clone(),
        };

        self.scylla_batch.append_statement(batch_stmt);
        self.buffer.push(msg);
        self.curr_batch_byte_size += msg_size;
        Ok(())
    }

    async fn tick_with_watch(
        &mut self,
        now: Instant,
        msg: Self::Input,
        ws: WatchSignal,
    ) -> anyhow::Result<Nothing> {
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
    session: Arc<Session>,
    shard_id: ShardId,
    producer_id: ProducerId,
    next_offset: ShardOffset,
    token_topology: Arc<dyn TokenTopology + Send + Sync>,
    batchers: Arc<[AgentHandler<ShardedClientCommand>]>,
    current_batcher: Option<usize>,
    slot_event_counter : HashMap<i64, i32>,
    shard_stats_checkpoint_timer: Timer,
}

impl Shard {
    fn new(
        session: Arc<Session>,
        shard_id: ShardId,
        producer_id: ProducerId,
        next_offset: ShardOffset,
        token_topology: Arc<dyn TokenTopology + Send + Sync>,
        batchers: Arc<[AgentHandler<ShardedClientCommand>]>,
    ) -> Self {
        Shard {
            session,
            shard_id,
            producer_id,
            next_offset,
            token_topology,
            batchers,
            current_batcher: None,
            slot_event_counter: HashMap::new(),
            shard_stats_checkpoint_timer: Timer::new(Duration::from_secs(60))
        }
    }

    fn get_batcher_idx_for_token(&self, token: Token) -> usize {
        let node_uuid = self.token_topology.get_node_uuid_for_token(token);
        let mut node_uuids = self.token_topology.get_node_uuids();
        node_uuids.sort();
        if let Ok(i) = node_uuids.binary_search(&node_uuid) {
            i % self.batchers.len()
        } else {
            warn!(
                "Token topology didn't know about {:?} at the time of batcher assignment.",
                node_uuid
            );
            0
        }
    }

    async fn do_shard_stats_checkpoint(&mut self) -> anyhow::Result<Nothing> {
        self.session.query(
            SCYLLADB_INSERT_SHARD_STATISTICS,
            ShardStatistics::from_slot_event_counter(self.shard_id, self.period(), self.producer_id, self.next_offset, &self.slot_event_counter)
        ).await.map_err(|e|
            anyhow!("ICIIII {:?}", e)
        )?;

        self.slot_event_counter.clear();
        self.shard_stats_checkpoint_timer.restart();
        Ok(())
    }

    fn period(&self) -> i64 {
        self.next_offset / SHARD_OFFSET_MODULO
    }
}

#[async_trait]
impl Ticker for Shard {
    type Input = ClientCommand;

    fn timeout(&self) -> Pin<Box<dyn Future<Output = Nothing> + Send + 'static>> {
        if self.shard_stats_checkpoint_timer.deadline.elapsed() > Duration::from_millis(0) {
            // If deadline has already pass..
            ready(()).boxed()
        } else {
            self.shard_stats_checkpoint_timer.sleep().boxed()
        }
    }
    
    async fn on_timeout(&mut self, now: Instant) -> anyhow::Result<Nothing> {
        self.do_shard_stats_checkpoint().await?;
        info!("shard({:?}) checkpoint at {:?}", self.shard_id, now);
        Ok(())
    }

    async fn tick(&mut self, now: Instant, msg: Self::Input) -> anyhow::Result<Nothing> {
        let shard_id = self.shard_id;
        let producer_id = self.producer_id;
        let offset = self.next_offset;
        let is_end_of_period = (offset + 1) % SHARD_OFFSET_MODULO == 0;
        self.next_offset += 1;

        // Resolve the partition key
        let mut partition_key = SerializedValues::new();
        let period = self.period();
        partition_key
            .add_value(&shard_id, &ColumnType::SmallInt)
            .unwrap();
        partition_key
            .add_value(&period, &ColumnType::BigInt)
            .unwrap();
        let token = self
            .token_topology
            .compute_token(SCYLLADB_SOLANA_LOG_TABLE_NAME, &partition_key);

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
        let slot = msg.slot();
        let sharded = msg.with_shard_info(shard_id, producer_id, offset);

        // Handle the end of a period
        let result = if is_end_of_period {
            // Remove the current_batcher so next round we decide on a new batcher
            let _ = self.current_batcher.take();

            // With watch allows us to block until the period is either completly committed or abandonned
            // before sharding again.
            let watch = batcher.send_with_watch(sharded).await?;

            let subres = watch.await.map_err(anyhow::Error::new);
            subres
        } else {
            let subres = batcher.send(sharded).await;
            subres
        };

        if result.is_ok() {
            *self.slot_event_counter.entry(slot).or_default() += 1;

            if is_end_of_period {
                // Flush important statistics
                self.do_shard_stats_checkpoint().await?;
            }
            
        }

        if now.elapsed() > Duration::from_millis(100) {
            warn!("shard tick elapsed {:?}", now.elapsed());
        }
        return result;
    }
}

struct RoundRobinShardRouter {
    sharders: Vec<AgentHandler<ClientCommand>>,
    sharder_idx: usize,
}

impl RoundRobinShardRouter {
    pub fn new(batchers: Vec<AgentHandler<ClientCommand>>) -> Self {
        let res = RoundRobinShardRouter {
            sharders: batchers,
            sharder_idx: 0,
        };
        res
    }
}

#[async_trait]
impl Ticker for RoundRobinShardRouter {
    type Input = ClientCommand;

    async fn tick(&mut self, _now: Instant, msg: Self::Input) -> Result<Nothing, anyhow::Error> {
        let begin = self.sharder_idx;

        let maybe_permit = self
            .sharders
            .iter()
            .enumerate()
            .cycle()
            .skip(begin)
            .take(self.sharders.len())
            .find_map(|(i, sharder)| sharder.try_reserve().ok().map(|slot| (i, slot)));

        if let Some((i, permit)) = maybe_permit {
            self.sharder_idx = i + 1;
            scylladb_batch_request_lag_inc();
            permit.send(msg);
            return Ok(());
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
            return Err(anyhow::anyhow!(
                "failed to find a sharder, message is dropped"
            ));
        }
    }
}

pub struct ScyllaSink {
    batch_router_handle: AgentHandler<ClientCommand>,
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


async fn get_max_offset_for_shard(session: Arc<Session>, shard_id: i16) -> anyhow::Result<Option<i64>> {
    let query_result = session
        .query(SCYLLADB_GET_MAX_OFFSET_FOR_SHARD_MV, (shard_id,))
        .await?;

    query_result
        .single_row()
        .ok()
        .map(|row| row.into_typed::<(ShardOffset,)>())
        .transpose()
        .map(|maybe| maybe.map(|typed_row| typed_row.0))
        .map_err(anyhow::Error::new)
}


type BatcherArray = Arc<[AgentHandler<ShardedClientCommand>]>;

async fn shard_factory(
    session: Arc<Session>, 
    shard_id: ShardId,
    producer_id: ProducerId,
    token_toplogy: Arc<dyn TokenTopology + Send + Sync>, 
    batchers: BatcherArray
) -> anyhow::Result<Shard> {
    let before: Instant = Instant::now();
    let max_offset = get_max_offset_for_shard(Arc::clone(&session), shard_id).await?;
    let next_offset = max_offset.unwrap_or(0) + 1;
    let shard = Shard::new(session, shard_id, producer_id, next_offset, token_toplogy, batchers);
    info!(
        "sharder {:?} next_offset: {:?}, stats collected in: {:?}",
        shard_id,
        next_offset,
        before.elapsed()
    );
    Ok(shard)
}


async fn setup_producer_info(session: Arc<Session>) {
    let result = session.query(SCYLLADB_GET_MAX_OFFSET_FOR_ALL_SHARD_MV.as_str(), &[]).await;

    println!("{:?}", result);
}



impl ScyllaSink {
    pub async fn new(
        config: ScyllaSinkConfig,
        hostname: impl AsRef<str>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let producer_id = [config.producer_id];

        let session: Session = SessionBuilder::new()
            .known_node(hostname)
            .user(username, password)
            .compression(Some(Compression::Lz4))
            .use_keyspace(config.keyspace.clone(), false)
            .build()
            .await
            .unwrap();

        let session = Arc::new(session);
        setup_producer_info(Arc::clone(&session));
        let token_topology: Arc<dyn TokenTopology + Send + Sync> =
            Arc::new(LiveTokenTopology(Arc::clone(&session)));

        let system = AgentSystem::new(16);

        let shard_count =  1; // config.shard_count;
        let num_batcher = 1; // (shard_count / 8) as usize;

        let mut batchers = Vec::with_capacity(num_batcher);
        for i in 0..num_batcher {
            let lbs = LiveBatchSender::new(
                Arc::clone(&session),
                config.linger,
                config.batch_len_limit,
                config.batch_size_kb_limit
            );
            let lbs_handler = system.spawn(format!("batcher({:?})", i), lbs);
            batchers.push(lbs_handler);
        }

        let batchers: Arc<[AgentHandler<ShardedClientCommand>]> =
            Arc::from(batchers.into_boxed_slice());

        let mut sharders = vec![];
        let mut js: JoinSet<anyhow::Result<Shard>> = JoinSet::new();

        info!("Will create {:?} shards", shard_count);
        for shard_id in 0..shard_count {
            let session = Arc::clone(&session);
            let tt = Arc::clone(&token_topology);
            let batchers = Arc::clone(&batchers);
            js.spawn(async move {
                shard_factory(session, shard_id, producer_id, tt, batchers).await
            });
        }

        while let Some(result) = js.join_next().await {
            let shard = result.unwrap().unwrap();
            sharders.push(system.spawn(format!("shard({:?})", shard.shard_id), shard));
        }

        let router = RoundRobinShardRouter::new(sharders);

        let router_handle = system.spawn("router", router);
        info!("Shard router has started.");

        Ok(ScyllaSink {
            batch_router_handle: router_handle,
        })
    }

    pub async fn log_account_update(
        &mut self,
        update: AccountUpdate,
    ) -> Result<(), ScyllaSinkError> {
        let cc = ClientCommand::InsertAccountUpdate(update);
        self.batch_router_handle
            .send(cc)
            .await
            .map_err(|_e| ScyllaSinkError::SinkClose)
    }

    pub async fn log_transaction(&mut self, tx: Transaction) -> Result<(), ScyllaSinkError> {
        let cc = ClientCommand::InsertTransaction(tx);
        self.batch_router_handle
            .send(cc)
            .await
            .map_err(|_e| ScyllaSinkError::SinkClose)
    }
}
