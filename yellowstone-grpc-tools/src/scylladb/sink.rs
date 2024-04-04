use {
    super::prom::{
        scylladb_batch_queue_dec, scylladb_batch_queue_inc, scylladb_batch_request_lag_inc,
        scylladb_batch_request_lag_sub, scylladb_batch_sent_inc, scylladb_batch_size_observe,
        scylladb_batchitem_sent_inc_by, scylladb_peak_batch_linger_observe,
    },
    crate::scylladb::types::AccountUpdate,
    futures::future::pending,
    scylla::{
        batch::{Batch, BatchStatement},
        frame::response::result::ColumnType,
        prepared_statement::PreparedStatement,
        routing::Token,
        serialize::{
            batch::{BatchValues, BatchValuesIterator},
            row::{RowSerializationContext, SerializedValues},
        },
        transport::errors::QueryError,
        Session, SessionBuilder,
    },
    std::{
        cmp::Reverse,
        collections::{BinaryHeap, HashMap},
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::mpsc::{UnboundedReceiver, UnboundedSender},
        task::{JoinError, JoinHandle, JoinSet},
        time::{self, Instant},
    },
    tonic::async_trait,
    tracing::{debug, info, trace, warn},
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

impl Into<SerializedValues> for AccountUpdate {
    fn into(self) -> SerializedValues {
        let mut row = SerializedValues::new();
        row.add_value(&self.slot, &ColumnType::BigInt).unwrap();
        row.add_value(&self.pubkey, &ColumnType::Blob).unwrap();
        row.add_value(&self.lamports, &ColumnType::BigInt).unwrap();
        row.add_value(&self.owner, &ColumnType::Blob).unwrap();
        row.add_value(&self.executable, &ColumnType::Boolean)
            .unwrap();
        row.add_value(&self.rent_epoch, &ColumnType::BigInt)
            .unwrap();
        row.add_value(&self.write_version, &ColumnType::BigInt)
            .unwrap();
        row.add_value(&self.data, &ColumnType::Blob).unwrap();
        row.add_value(&self.txn_signature, &ColumnType::Blob)
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

/// This batcher is aware of the current partitioning scheme of scylla and build different
/// batch for different endpoint
pub struct TokenAwareBatcher<B: BatchSender + Send + Sync, TT: TokenTopology> {
    config: ScyllaSinkConfig,
    batch_sender: B,
    token_topology: TT,
    inner: UnboundedReceiver<BatchRequest>,
    batch_map: HashMap<NodeUuid, (Batch, Vec<SerializedValues>)>,
    batch_schedule: BinaryHeap<Reverse<(Instant, NodeUuid)>>,
    inflight_deliveries: JoinSet<Result<(), QueryError>>,
    batch_last_consumed_map: HashMap<NodeUuid, Instant>,
}

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

#[derive(Clone)]
struct LiveBatchSender(Arc<Session>);

// Session is Send so is LiveBatchSender
unsafe impl Send for LiveBatchSender {}

unsafe impl Sync for LiveBatchSender {}

#[async_trait]
impl BatchSender for LiveBatchSender {
    async fn send_batch(
        self,
        batch: Batch,
        serialized_rows: Vec<SerializedValues>,
    ) -> Result<(), QueryError> {
        let before = Instant::now();
        scylladb_batch_size_observe(batch.statements.len());

        let result = self
            .0
            .batch(&batch, PreSerializedBatchValues(serialized_rows))
            .await
            .map(|_| ());

        let after = Instant::now();
        info!(
            "Batch sent: size={:?}, latency={:?}",
            batch.statements.len(),
            after - before
        );
        if result.is_ok() {
            scylladb_batch_sent_inc();
            scylladb_batchitem_sent_inc_by(batch.statements.len() as u64);
            scylladb_batch_request_lag_sub(batch.statements.len() as i64)
        }
        result
    }
}

#[derive(Debug, PartialEq)]
enum TickError {
    DeliveryError,
    Timeout,
}

struct BatcherHandle {
    inner: JoinHandle<()>,
    sender: UnboundedSender<BatchRequest>,
}

impl BatcherHandle {
    fn send(
        &self,
        br: BatchRequest,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<BatchRequest>> {
        let res = self.sender.send(br);
        if res.is_ok() {
            scylladb_batch_request_lag_inc();
        }
        res
    }

    fn abort(&self) {
        self.inner.abort()
    }

    async fn join(self) -> Result<(), JoinError> {
        self.inner.await
    }
}

impl<B: BatchSender + 'static, TT: TokenTopology> TokenAwareBatcher<B, TT> {
    /// Tick allow the Batcher to "step foward" into its state.
    /// At each tick, it concurrently handles one of: batch request, batch delivery or throttling.
    async fn tick(&mut self, now: Instant) -> Result<(), TickError> {
        let tick_timeout = now + self.config.linger;
        let deadline: Instant = self
            .batch_schedule
            .peek()
            .map(|rev| rev.0 .0)
            .unwrap_or(tick_timeout);
        tokio::select! {
            _ = time::sleep_until(deadline), if !self.need_throttling() => {
                let old_value = self.consume_next_in_schedule(now);
                if let Some((batch, ser_values)) = old_value {
                    if !batch.statements.is_empty() {
                        let bs = self.batch_sender.clone();
                        let fut = bs.send_batch(batch, ser_values);
                        self.inflight_deliveries.spawn(fut);
                    }
                }
            },
            // recv is cancel safe, so no data will be lost if the other branch finish first or if branch pre-condition is false
            Some(BatchRequest { stmt, item }) = self.inner.recv(), if !self.need_throttling() && deadline > now => {
                let token = item.resolve_token(&self.token_topology);
                let (node_uuid, current_batch_size) = {
                    let (node_uuid, (batch, ser_values)) = self.get_batch_for_token(token);
                    let serialized_row = item.serialize();
                    batch.append_statement(stmt);
                    ser_values.push(serialized_row);
                    (node_uuid, ser_values.len())
                };

                if current_batch_size >= self.config.batch_size_limit {
                    self.batch_schedule.push(Reverse((now, node_uuid)));
                }
            },
            Some(Err(_join_err)) = self.inflight_deliveries.join_next() => return Err(TickError::DeliveryError),

            _ = time::sleep_until(tick_timeout) => return Err(TickError::Timeout)
        }
        Ok(())
    }

    fn need_throttling(&self) -> bool {
        let res = self.inflight_deliveries.len() >= self.config.max_inflight_batch_delivery;
        res
    }

    fn get_batch_for_token(
        &mut self,
        token: Token,
    ) -> (NodeUuid, &mut (Batch, Vec<SerializedValues>)) {
        let node_uuid = self.token_topology.get_node_uuid_for_token(token);
        if !self.batch_map.contains_key(&node_uuid) {
            self.refresh_schedule_for_node(Instant::now(), node_uuid);
            scylladb_batch_queue_inc();
        }
        (node_uuid, self.batch_map.entry(node_uuid).or_default())
    }

    fn refresh_schedule_for_node(&mut self, from: Instant, node_uuid: NodeUuid) {
        let next_instant = from.checked_add(self.config.linger).unwrap();
        self.batch_schedule.push(Reverse((next_instant, node_uuid)));
    }

    fn consume_next_in_schedule(
        &mut self,
        consumed_at: Instant,
    ) -> Option<(Batch, Vec<SerializedValues>)> {
        if let Some(Reverse((instant, node_uuid))) = self.batch_schedule.pop() {
            // This block of code is here to avoid double sending the same batch within the same linger period
            if let Some(last_consumed_instant) = self.batch_last_consumed_map.get(&node_uuid) {
                let time_delta = consumed_at.duration_since(*last_consumed_instant);
                if time_delta < self.config.linger {
                    let additional_wait_time = self.config.linger - time_delta;
                    // Reschedule
                    self.batch_schedule
                        .push(Reverse((instant + additional_wait_time, node_uuid)));
                    return None;
                }
            }

            scylladb_peak_batch_linger_observe(instant.elapsed());
            self.batch_last_consumed_map.insert(node_uuid, consumed_at);
            let ret = self.batch_map.remove(&node_uuid);
            if ret.is_some() {
                scylladb_batch_queue_dec();
            }
            ret
        } else {
            None
        }
    }

    fn new<B2: BatchSender + Send + 'static, TT2: TokenTopology + Send + 'static>(
        config: ScyllaSinkConfig,
        batch_sender: B2,
        token_topology: TT2,
    ) -> (TokenAwareBatcher<B2, TT2>, UnboundedSender<BatchRequest>) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let batcher = TokenAwareBatcher {
            config,
            batch_sender,
            token_topology,
            inner: receiver,
            batch_map: HashMap::default(),
            batch_schedule: BinaryHeap::default(),
            inflight_deliveries: JoinSet::new(),
            batch_last_consumed_map: HashMap::default(),
        };
        (batcher, sender)
    }

    ///
    /// Runs a TokenAwareBatcher in background task and returns a channel to send batch request and the underlying
    /// background task handle.
    fn spawn<B2: BatchSender + Send + 'static, TT2: TokenTopology + Send + 'static>(
        config: ScyllaSinkConfig,
        batch_sender: B2,
        token_topology: TT2,
    ) -> BatcherHandle {
        let (batcher, sender) =
            TokenAwareBatcher::<B2, TT2>::new(config, batch_sender, token_topology);

        // Background process handling the batcher
        let h = tokio::spawn(async move {
            let mut my_batcher = batcher;
            loop {
                let now = Instant::now();
                if let Err(TickError::DeliveryError) = my_batcher.tick(now).await {
                    break;
                }
            }
        });
        BatcherHandle { inner: h, sender }
    }
}

pub struct ScyllaSink {
    insert_account_update_ps: PreparedStatement,
    batcher_handle: BatcherHandle,
}

#[derive(Debug)]
pub enum ScyllaSinkError {
    SinkClose,
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
            .use_keyspace(config.keyspace.clone(), false)
            .build()
            .await
            .unwrap();

        let session = Arc::new(session);
        let insert_account_update_ps = session
            .prepare(SCYLLADB_INSERT_ACCOUNT_UPDATE)
            .await
            .unwrap();

        let batch_sender = LiveBatchSender(Arc::clone(&session));
        let token_topology = LiveTokenTopology(Arc::clone(&session));
        let batcher_handle = TokenAwareBatcher::<LiveBatchSender, LiveTokenTopology>::spawn(
            config.clone(),
            batch_sender,
            token_topology,
        );

        ScyllaSink {
            insert_account_update_ps,
            batcher_handle,
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
        self.batcher_handle
            .send(br)
            .map_err(|_e| ScyllaSinkError::SinkClose)
    }
}

mod tests {
    use {
        super::*,
        scylla::query::Query,
        std::{cell::RefCell, iter::repeat},
        tokio::sync::mpsc::error::TryRecvError,
    };

    type BatchSenderCallback = UnboundedReceiver<(Batch, Vec<SerializedValues>)>;

    #[derive(Clone)]
    struct NullBatchSender {
        callback: UnboundedSender<(Batch, Vec<SerializedValues>)>,
    }

    #[derive(Clone)]
    struct StuckedBatchSender;
    unsafe impl Sync for StuckedBatchSender {}
    unsafe impl Send for StuckedBatchSender {}

    #[async_trait]
    impl BatchSender for StuckedBatchSender {
        async fn send_batch(
            self,
            batch: Batch,
            serialized_rows: Vec<SerializedValues>,
        ) -> Result<(), QueryError> {
            pending().await
        }
    }

    #[derive(Clone)]
    struct ConstTokenTopology {
        node_uuid: RefCell<NodeUuid>,
        token: RefCell<Token>,
        is_node_exists: RefCell<bool>,
    }

    impl Default for ConstTokenTopology {
        fn default() -> Self {
            Self {
                node_uuid: Default::default(),
                token: RefCell::new(Token {
                    value: Default::default(),
                }),
                is_node_exists: RefCell::new(true),
            }
        }
    }
    unsafe impl Sync for NullBatchSender {}
    unsafe impl Send for NullBatchSender {}

    #[async_trait]
    impl BatchSender for NullBatchSender {
        async fn send_batch(
            self,
            batch: Batch,
            serialized_rows: Vec<SerializedValues>,
        ) -> Result<(), QueryError> {
            self.callback.send((batch, serialized_rows)).unwrap();
            Ok(())
        }
    }

    impl TokenTopology for ConstTokenTopology {
        fn get_node_uuid_for_token(&self, token: Token) -> NodeUuid {
            self.node_uuid.borrow().clone()
        }

        fn is_node_uuid_exists(&self, node_uuid: NodeUuid) -> bool {
            self.is_node_exists.borrow().clone()
        }

        fn compute_token(&self, table: &str, serialized_values: &SerializedValues) -> Token {
            self.token.borrow().clone()
        }
    }

    fn stucked_batcher(
        config: ScyllaSinkConfig,
    ) -> (
        TokenAwareBatcher<StuckedBatchSender, ConstTokenTopology>,
        UnboundedSender<BatchRequest>,
    ) {
        let (batcher, sender) = TokenAwareBatcher::<StuckedBatchSender, ConstTokenTopology>::new(
            config,
            StuckedBatchSender {},
            ConstTokenTopology::default(),
        );
        (batcher, sender)
    }

    fn manual_test_batcher(
        config: ScyllaSinkConfig,
    ) -> (
        TokenAwareBatcher<NullBatchSender, ConstTokenTopology>,
        UnboundedSender<BatchRequest>,
        BatchSenderCallback,
    ) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let (batcher, sender2) = TokenAwareBatcher::<NullBatchSender, ConstTokenTopology>::new(
            config,
            NullBatchSender { callback: sender },
            ConstTokenTopology::default(),
        );
        (batcher, sender2, receiver)
    }

    fn spawn_test_batcher(config: ScyllaSinkConfig) -> (BatcherHandle, BatchSenderCallback) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (
            TokenAwareBatcher::<NullBatchSender, ConstTokenTopology>::spawn(
                config,
                NullBatchSender { callback: sender },
                ConstTokenTopology::default(),
            ),
            receiver,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn it_should_send_one_batch() {
        let (bh, mut cb) = spawn_test_batcher(Default::default());
        let _ = bh
            .send(BatchRequest {
                stmt: BatchStatement::Query(Query::new(SCYLLADB_INSERT_ACCOUNT_UPDATE)),
                item: BatchItem::Account(AccountUpdate::zero_account()),
            })
            .unwrap();

        let res = cb.recv().await;
        assert!(res.is_some());
    }

    #[tokio::test]
    async fn it_should_batch_related_item_within_the_lingering_limit() {
        let (mut batcher, sender, mut cb) = manual_test_batcher(Default::default());

        let now = Instant::now();

        // 1st item to batch
        let _ = sender
            .send(BatchRequest {
                stmt: BatchStatement::Query(Query::new(SCYLLADB_INSERT_ACCOUNT_UPDATE)),
                item: BatchItem::Account(AccountUpdate::zero_account()),
            })
            .unwrap();

        batcher.tick(now).await.unwrap();

        // 2nd item to batch
        let _ = sender
            .send(BatchRequest {
                stmt: BatchStatement::Query(Query::new(SCYLLADB_INSERT_ACCOUNT_UPDATE)),
                item: BatchItem::Account(AccountUpdate::zero_account()),
            })
            .unwrap();

        batcher.tick(now).await.unwrap();

        // It should send the batch since we reach the lingering limit
        let now = now + batcher.config.linger;
        batcher.tick(now).await.unwrap();

        let res = cb.recv().await.unwrap();

        assert_eq!(res.0.statements.len(), 2);
        assert_eq!(res.1.len(), 2);
    }

    #[tokio::test]
    async fn it_should_throttle_if_we_reach_the_maximum_inflight() {
        let config = ScyllaSinkConfig {
            batch_size_limit: 1,
            linger: Duration::from_millis(10),
            keyspace: String::from("solana"),
            // We put a very limited amount of inflight batch delivery
            max_inflight_batch_delivery: 1,
        };
        let (mut batcher, sender) = stucked_batcher(config.clone());

        let now = Instant::now();

        let _ = sender
            .send(BatchRequest {
                stmt: BatchStatement::Query(Query::new(SCYLLADB_INSERT_ACCOUNT_UPDATE)),
                item: BatchItem::Account(AccountUpdate::zero_account()),
            })
            .unwrap();

        // 1st tick: start the batch
        batcher.tick(now).await.unwrap();

        let now = now + config.linger;

        // 2nd tick: send it to the BatchSender
        batcher.tick(now).await.unwrap();

        // 3rd tick: should cause tick timeout

        let now = now + config.linger;
        let tick_result = batcher.tick(now).await;

        assert_eq!(tick_result, Err(TickError::Timeout));

        // Any subsequent send, should still not be batched, since we are stucked and we have a maximum of a one inflight delivery
        let _ = sender
            .send(BatchRequest {
                stmt: BatchStatement::Query(Query::new(SCYLLADB_INSERT_ACCOUNT_UPDATE)),
                item: BatchItem::Account(AccountUpdate::zero_account()),
            })
            .unwrap();
        let now = now + config.linger;
        let tick_result2 = batcher.tick(now).await;
        assert_eq!(tick_result2, Err(TickError::Timeout));
    }

    #[tokio::test]
    async fn it_should_handle_orphan_batch_when_token_topology_changes() {
        let (mut batcher, sender, mut cb) = manual_test_batcher(Default::default());

        let now = Instant::now();

        let _ = sender
            .send(BatchRequest {
                stmt: BatchStatement::Query(Query::new(SCYLLADB_INSERT_ACCOUNT_UPDATE)),
                item: BatchItem::Account(AccountUpdate::zero_account()),
            })
            .unwrap();

        // 1st tick = create a new batch for a a given node_uuid
        batcher.tick(now).await.unwrap();

        // This will create a topology change where a specific token range is now assigned to another node.
        // This should create an orphan batch in the tokenbatcher
        batcher.token_topology.node_uuid.replace(100);

        // 2nd item to batch
        let _ = sender
            .send(BatchRequest {
                stmt: BatchStatement::Query(Query::new(SCYLLADB_INSERT_ACCOUNT_UPDATE)),
                item: BatchItem::Account(AccountUpdate::zero_account()),
            })
            .unwrap();

        // 2nd tick = create a new batch since we changed the topology
        batcher.tick(now).await.unwrap();

        let now = now + batcher.config.linger;
        // consume 1st orphan batch
        batcher.tick(now).await.unwrap();
        let now = now + batcher.config.linger;
        // consume 2nd batch
        batcher.tick(now).await.unwrap();

        let res1 = cb.recv().await.unwrap();
        let res2 = cb.recv().await.unwrap();
        assert_eq!(res1.0.statements.len(), 1);
        assert_eq!(res1.1.len(), 1);
        assert_eq!(res2.0.statements.len(), 1);
        assert_eq!(res2.1.len(), 1);
    }

    #[tokio::test]
    async fn it_should_send_batch_when_batch_limit_size_is_reached() {
        let sink_config = ScyllaSinkConfig {
            batch_size_limit: 10,
            linger: Duration::from_millis(10),
            keyspace: String::from("default"),
            max_inflight_batch_delivery: 10,
        };

        let (mut batcher, sender, mut cb) = manual_test_batcher(sink_config.clone());

        let mut now = Instant::now();

        for _ in repeat(()).take(19) {
            let _ = sender
                .send(BatchRequest {
                    stmt: BatchStatement::Query(Query::new(SCYLLADB_INSERT_ACCOUNT_UPDATE)),
                    item: BatchItem::Account(AccountUpdate::zero_account()),
                })
                .unwrap();
        }

        // Batch 10 requests (size limit)
        for _ in repeat(()).take(10) {
            batcher.tick(now).await.unwrap();
        }
        now = now + Duration::from_millis(1);
        // Should trigger batch send
        batcher.tick(now).await.unwrap();

        let res1 = cb.recv().await.unwrap();
        assert_eq!(res1.0.statements.len(), 10);
        assert_eq!(res1.1.len(), 10);

        for _ in repeat(()).take(9) {
            batcher.tick(now).await.unwrap();
        }
        // Should trigger batch send
        now = now + sink_config.linger;
        batcher.tick(now).await.unwrap();
        let res2 = cb.recv().await.unwrap();
        assert_eq!(res2.0.statements.len(), 9);
        assert_eq!(res2.1.len(), 9);

        let try_recv_result = cb.try_recv();
        assert_eq!(try_recv_result.err().unwrap(), TryRecvError::Empty);
    }
}
