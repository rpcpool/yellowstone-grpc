use {
    crate::scylladb::agents::{AgentSystem, Timer, Ticker},
    super::prom::{
        get, scylladb_batch_queue_dec, scylladb_batch_queue_inc, scylladb_batch_request_lag_inc, scylladb_batch_request_lag_sub, scylladb_batch_sent_inc, scylladb_batch_size_observe, scylladb_batchitem_sent_inc_by, scylladb_peak_batch_linger_observe
    }, crate::scylladb::types::AccountUpdate, futures::{channel::mpsc, future::pending, SinkExt, TryFutureExt}, google_cloud_pubsub::client::google_cloud_auth::token_source::service_account_token_source::ServiceAccountTokenSource, scylla::{
        batch::{self, Batch, BatchStatement},
        frame::{request::{query, Query}, response::result::ColumnType, Compression},
        prepared_statement::{PreparedStatement, TokenCalculationError},
        routing::Token,
        serialize::{
            batch::{BatchValues, BatchValuesIterator},
            row::{RowSerializationContext, SerializedValues},
        },
        transport::{errors::QueryError, Node},
        Session, SessionBuilder,
    }, sha2::digest::HashMarker, std::{
        cmp::Reverse, collections::{BinaryHeap, HashMap}, hash::Hash, num, sync::Arc, time::Duration
    }, tokio::{
        sync::mpsc::{channel, error::{SendError, TryRecvError, TrySendError}, Receiver, Sender, UnboundedReceiver, UnboundedSender},
        task::{JoinError, JoinHandle, JoinSet},
        time::{self, Instant},
    }, tonic::async_trait, tracing::{debug, info, trace, warn}
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
struct LiveBatchSender2 {
    receiver: Receiver<(Batch, Vec<SerializedValues>)>,
    session: Arc<Session>,
    js: JoinSet<Result<(), BatchSenderError>>,
    max_inflight: usize,
}

#[derive(Debug)]
enum BatchSenderError {
    ScyllaError(QueryError),
    ChannelClosed,
}

impl LiveBatchSender2 {

    async fn forever(&mut self) -> Result<(), BatchSenderError> {
        loop {
            let now = Instant::now();
            let result = self.tick2(now).await;
            if result.is_err() {
                return result;
            }
        }
    }

    async fn tick2(&mut self, now: Instant) -> Result<(), BatchSenderError> {
        if let Some((batch, serialized_rows)) = self.receiver.recv().await {
            scylladb_batch_size_observe(batch.statements.len());
            let session = Arc::clone(&self.session);


            while self.js.len() >= self.max_inflight {
                let result = self.js.join_next().await.unwrap();
                if result.is_err() {
                    return Err(BatchSenderError::ChannelClosed);
                }
            }

            self.js.spawn(async move {
                 let result =session
                    .batch(&batch, PreSerializedBatchValues(serialized_rows))
                    .await
                    .map(|_| ());

                let after = Instant::now();
                info!(
                    "Batch sent: size={:?}, latency={:?}",
                    batch.statements.len() as i64,
                    after - now
                );
                
                scylladb_batch_sent_inc();
                scylladb_batchitem_sent_inc_by(batch.statements.len() as u64);
                scylladb_batch_request_lag_sub(batch.statements.len() as i64);
                result.map_err(|query_error| BatchSenderError::ScyllaError(query_error))
            });
            Ok(())
        } else {
            Err(BatchSenderError::ChannelClosed)
        }        
    }

    async fn tick(&mut self, now: Instant) -> Result<(), BatchSenderError> {
        if let Some((batch, serialized_rows)) = self.receiver.recv().await {

            scylladb_batch_size_observe(batch.statements.len());

            let result = self
                .session
                .batch(&batch, PreSerializedBatchValues(serialized_rows))
                .await
                .map(|_| ());

            let after = Instant::now();
            info!(
                "Batch sent: size={:?}, latency={:?}",
                batch.statements.len() as i64,
                after - now
            );
            
            scylladb_batch_sent_inc();
            scylladb_batchitem_sent_inc_by(batch.statements.len() as u64);
            scylladb_batch_request_lag_sub(batch.statements.len() as i64);
            result.map_err(|query_error| BatchSenderError::ScyllaError(query_error))
        } else {
            Err(BatchSenderError::ChannelClosed)
        }        
    }
}

#[derive(Clone)]
struct BatchSenderHandle {
    sender: Sender<(Batch, Vec<SerializedValues>)>,
    batch_sender_ref: Arc<JoinHandle<Result<(), BatchSenderError>>>
}

impl BatchSenderHandle {
    async fn send(&self, msg: (Batch, Vec<SerializedValues>)) -> Result<(), ()> {
        self.sender.send(msg).await.map_err(|_err| ())
    }
}


fn spawn_live_batch_sender2(session: Arc<Session>, max_inflight: usize, num_worker: usize) -> BatchSenderHandle {

    let (sender, mut receiver) = channel(max_inflight);
    let mut batch_sender = LiveBatchSender2 {
        receiver: receiver,
        session: Arc::clone(&session),
        js: JoinSet::new(),
        max_inflight,
    };
    
    let h = tokio::spawn(async move {
        batch_sender.forever().await
    });
    BatchSenderHandle {
        sender,
        batch_sender_ref: Arc::new(h),
    }
    
}

// fn spawn_live_batch_sender(session: Arc<Session>, max_inflight: usize, num_worker: usize) -> BatchSenderHandle {
//     let (sender, mut receiver) = channel(max_inflight);

//     let mut batch_sender_mailboxes = Vec::with_capacity(4);
//     for _ in 0..num_worker {
//         let (sender2, receiver2) = channel(1);
//         let mut lbs = LiveBatchSender2 {
//             receiver: receiver2,
//             session: Arc::clone(&session),
//         };

//         let h = tokio::spawn(async move {
//             loop {
//                 match lbs.tick(Instant::now()).await {
//                     Err(e) => break,
//                     _ => continue
//                 }   
//             }
//         });
//         batch_sender_mailboxes.push((h, sender2));
//     }
    
//     let h = tokio::spawn(async move {
//         'outer: loop {
            
//             match receiver.recv().await {
//                 Some(msg) => {
//                     let mut msg = msg;

//                     'inner: for (_h, sender) in batch_sender_mailboxes.iter().cycle() {
                        
//                         match sender.try_send(msg) {
//                             Ok(_) => break 'inner,
//                             Err(TrySendError::Full(msg2)) => msg = msg2,
//                             Err(TrySendError::Closed(msg2)) => msg = msg2,
//                         }
//                     }   
//                 },
//                 //Err(TryRecvError::Empty) => continue,
//                 //Err(TryRecvError::Disconnected) => break 'outer,
//                 None => break 'outer
//             }
//         }
//     });
//     BatchSenderHandle {
//         sender,
//         batch_sender_ref: Arc::new(h),
//     }
// }

#[derive(Debug, PartialEq)]
enum TickError {
    DeliveryError,
    Timeout,
    IsClosed,
}

struct TimedBatcher {
    deadline: Instant,
    linger_duration: Duration,
    mailbox: Receiver<BatchRequest>,
    batch_size_limit: usize,
    batch: Vec<(Batch, Vec<SerializedValues>)>,
    curr_batch_size: usize,
    batch_sender_handle: BatchSenderHandle,
}

impl TimedBatcher {
    fn new(
        linger_duration: Duration, 
        mailbox: Receiver<BatchRequest>, 
        batch_size_limit: usize,
        batch_sender_handle: BatchSenderHandle,
    ) -> Self {
        TimedBatcher {
            deadline: Instant::now() + linger_duration,
            mailbox,
            linger_duration,
            batch_size_limit,
            batch: Vec::new(),
            curr_batch_size: 0,
            batch_sender_handle,
        }
    }

    async fn forever(&mut self) -> Result<(), TickError> {
        loop {
            let now = Instant::now();
            let res = self.tick(now).await;
            if res.is_err() {
                return res
            }
        }
    }

    fn get_batch_mut(&mut self) -> &mut (Batch, Vec<SerializedValues>) {
        if self.batch.is_empty() {
            self.batch.push((Batch::default(), Vec::with_capacity(self.batch_size_limit)));
        }
        self.batch.get_mut(0).unwrap()
    }

    async fn tick(&mut self, now: Instant) -> Result<(), TickError> {
        tokio::select! {
            _ = time::sleep_until(self.deadline) => self.flush(now).await.map_err(|_e| TickError::DeliveryError),
            msg = self.mailbox.recv() => {
                match msg {
                    Some(br) => {
                        {
                            let ser_row = br.item.serialize();
                            let ref mut mybatch = self.get_batch_mut();
                            mybatch.0.append_statement(br.stmt);
                            mybatch.1.push(ser_row);
                            self.curr_batch_size += 1;
                        }
                        if self.curr_batch_size >= self.batch_size_limit {
                            self.flush(now).map_err(|_e| TickError::DeliveryError).await
                        } else {
                            Ok(())
                        }
                    }
                    None => { 
                        Err(
                            self.flush(now)
                                .await
                                .map_err(|_e| TickError::DeliveryError)
                                .err()
                                .unwrap_or(TickError::IsClosed)
                        )
                    }
                }
            }
        }
    }

    async fn flush(&mut self, now: Instant) -> Result<(), ()> {
        self.curr_batch_size = 0;
        if let Some(batch) = self.batch.pop() {
            if !batch.1.is_empty() {
                return self.batch_sender_handle.send(batch).await
            }
        }
        self.deadline = now + self.linger_duration;
        Ok(())
    }
}

struct BatcherHandle {
    sender: Sender<BatchRequest>,
    handle: JoinHandle<Result<(), TickError>>
}

impl BatcherHandle {
    async fn send(&self, msg: BatchRequest) -> Result<(), SendError<BatchRequest>> {
        self.sender.send(msg).await
    }
}


struct BatcherFactory {
    spawner: Box<dyn Fn() -> BatcherHandle>,
}

impl BatcherFactory {
    fn spawn(&self) -> BatcherHandle {
        (self.spawner)()
    }
}

struct TokenAwareBatchRouter {
    mailbox: Receiver<BatchRequest>,
    token_topology: LiveTokenTopology,
    batchers: Vec<BatcherHandle>,
}

impl TokenAwareBatchRouter {

    async fn tick(&mut self, now: Instant) -> Result<(), TickError> {
        match self.mailbox.recv().await {
            Some(br) => {
                let token = br.item.resolve_token(&self.token_topology);
                let node_uuid = self.token_topology.get_node_uuid_for_token(token);
                let i = (node_uuid % u128::from(self.batchers.len() as u64)) as u64;
                let batcher = self.batchers.get(i as usize).unwrap();
                batcher.send(br).await.map_err(|_e| TickError::DeliveryError)
            }
            None => Err(TickError::DeliveryError)
        }
    }
}


fn nbuild_timed_batcher(
    n: usize,
    buffer: usize, 
    linger_duration: Duration, 
    batch_size_limit: usize, 
    batch_sender_handle: BatchSenderHandle
) -> Vec<BatcherHandle> {
    let mut res = Vec::with_capacity(n);
    for i in 1..(n + 1) {
        let (sender, receiver) = channel(buffer);

        let mut batcher = TimedBatcher::new(
            linger_duration,
            receiver,
            batch_size_limit,
            batch_sender_handle.clone(),
        );

        let h = tokio::spawn(async move {
            batcher.forever().await
        });
        let bh = BatcherHandle {
            sender,
            handle: h,
        };
        res.push(bh);
    }
    res
}

fn spawn_token_aware_router(token_topology: LiveTokenTopology, buffer: usize, batchers: Vec<BatcherHandle>) -> BatchRouterHandle {
    let (sender, receiver) = channel(buffer);
    let mut router = TokenAwareBatchRouter {
        mailbox: receiver,
        token_topology,
        batchers,
    };

    let h = tokio::spawn(async move {
        loop {
            let now = Instant::now();
            let result = router.tick(now).await;
            if result.is_err() {
               break
            }
        }
    });

    BatchRouterHandle {
        sender,
        inner: h,
    }
}

struct BatchRouterHandle {
    inner: JoinHandle<()>,
    sender: Sender<BatchRequest>,
}

impl BatchRouterHandle {
    async fn send(
        &self,
        br: BatchRequest,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<BatchRequest>> {
        let res = self.sender.send(br).await;
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

pub struct ScyllaSink {
    insert_account_update_ps: PreparedStatement,
    batch_router_handle: BatchRouterHandle,
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
        let batch_sender_handle = spawn_live_batch_sender2(session, config.max_inflight_batch_delivery, 160);
        let batchers = nbuild_timed_batcher(
            3,
            10000, 
            config.linger, 
            config.batch_size_limit, 
            batch_sender_handle
        );
        let router = spawn_token_aware_router(token_topology, 30000, batchers);

        //let batch_sender_handle = spawn_live_batch_sender(Arc::clone(&session), config.batch_size_limit, 2300);
        ScyllaSink {
            insert_account_update_ps,
            batch_router_handle: router,
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