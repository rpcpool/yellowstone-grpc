use {
    super::types::{
        BlockchainEvent, BlockchainEventType, ProducerId, ProducerInfo, ShardId,
        ShardOffset, SHARD_OFFSET_MODULO,
    },
    core::{panic},
    futures::{stream::BoxStream, StreamExt},
    scylla::{
        batch::{Batch, BatchType},
        frame::{
            response::result::{ColumnType},
        },
        prepared_statement::PreparedStatement,
        serialize::{
            row::{SerializeRow},
            value::SerializeCql,
        },
        transport::{query_result::SingleRowTypedError}, Session,
    },
    std::{
        fmt::{Debug},
        pin::Pin,
        sync::{Arc},
        time::Duration,
    },
    tokio::{sync::mpsc, time::Instant},
    tokio_stream::{wrappers::ReceiverStream, Stream},
    tonic::Response,
    tracing::{error},
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate,
        },
        yellowstone::log::{yellowstone_log_server::YellowstoneLog, ConsumeRequest},
    },
};

pub type OldShardOffset = ShardOffset;

type ConsumerId = String;

/**
 * This is a constant where we won't ever support more than 1024 shards.
 * This constant is helpful to create Stack
 */
const MAX_NUM_SHARD: usize = 1024;

const DEFAULT_OFFSET_COMMIT_INTERVAL: Duration = Duration::from_secs(10);

const DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY: usize = 100;

const SCYLLADB_UPDATE_CONSUMER_SHARD_OFFSET: &str = r###"
    UPDATE consumer_info
    SET offset = ?, updated_at = currentTimestamp() 
    WHERE 
        consumer_id = ?
        AND producer_id = ?
        AND shard_id = ?
    IF offset = ?
"###;

const SCYLLADB_PRODUCER_SHARD_PERIOD_COMMIT_EXISTS: &str = r###"
    SELECT
        1
    FROM producer_period_commit_log
    WHERE 
        producer_id = ?
        AND shard_id = ?
        AND period = ?
"###;

const SCYLLADB_QUERY_LOG: &str = r###"
    SELECT
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

        signature,
        signatures,
        num_required_signatures,
        num_readonly_signed_accounts,
        num_readonly_unsigned_accounts,
        account_keys,
        recent_blockhash,
        instructions,
        versioned,
        address_table_lookups,
        meta solana.transaction_meta
    FROM log
    WHERE producer_id = ? and shard_id = ? and offset > ? and period = ?
    ORDER BY offset ASC
"###;

const SCYLALDB_INSERT_CONSUMER_OFFSET: &str = r###"
    INSERT INTO (
        consumer_id,
        producer_id,
        shard_id,
        offset,
        created_at,
        updated_at
    )
    VALUES
    (?,?,?,?,currentTimestamp(), currentTimestamp())
"###;

const SCYLLADB_GET_CONSUMER_PRODUCER_MAPPING: &str = r###"
    SELECT
        producer_id
    FROM consumer_producer_mapping
    where consumer_id = ?
"###;

const SCYLLADB_GET_SHARD_OFFSETS_INFO_FOR_CONSUMER_ID: &str = r###"
    SELECT
        shard_id,
        offset
    FROM consumer_info
    WHERE 
        consumer_id = ?
        AND producer_id = ?
    ORDER BY shard_id ASC
"###;

const SCYLLADB_PRODUCER_CONSUMER_COUNT: &str = r###"
    SELECT
        producer_id,
        count(1)
    FROM consumer_producer_mapping_mv
    GROUP BY producer_id
"###;

const SCYLLADB_INSERT_CONSUMER_PRODUCER_MAP: &str = r###"
    INSERT INTO consumer_producer_mapping (
        consumer_id,
        producer_id,
        created_at,
        update_at
    )
    VALUES (?, ?, currentTimestamp(), currentTimestamp())
"###;

const SCYLLADB_LIST_PRODUCER_INFO: &str = r###"
    SELECT
        producer_id,
        num_shards,
        is_active
    FROM producer_info;
"###;

const SCYLLADB_GET_RANDOM_PRODUCER_INFO: &str = r###"
    SELECT
        producer_id,
        num_shards,
        is_active
    FROM producer_info
    WHERE ( producer_id = ? or ? )
    LIMIT 1;
"###;

struct QueryMaxOffsetPerShardArgs {
    producer_id: ProducerId,
    num_shards: i16,
}

impl SerializeRow for QueryMaxOffsetPerShardArgs {
    fn serialize(
        &self,
        _ctx: &scylla::serialize::row::RowSerializationContext<'_>,
        writer: &mut scylla::serialize::RowWriter,
    ) -> Result<(), scylla::serialize::SerializationError> {
        let cw = writer.make_cell_writer();
        let _pid = self.producer_id.as_ref();
        SerializeCql::serialize(&self.producer_id, &ColumnType::Blob, cw).map(|_proof| ())?;
        for shard_id in 0..self.num_shards {
            let cw = writer.make_cell_writer();
            SerializeCql::serialize(&shard_id, &ColumnType::SmallInt, cw).map(|_proof| ())?;
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.num_shards == 0
    }
}

async fn get_shard_offsets_info_for_consumer_id(
    session: Arc<Session>,
    consumer_id: impl AsRef<str>,
    producer_id: ProducerId,
) -> anyhow::Result<Vec<ShardOffset>> {
    let qr = session
        .query(
            SCYLLADB_GET_SHARD_OFFSETS_INFO_FOR_CONSUMER_ID,
            (consumer_id.as_ref(), producer_id),
        )
        .await?;

    let mut last_shard_id = -1;
    let mut ret = Vec::new();
    for result in qr.rows_typed_or_empty::<(ShardId, ShardOffset)>() {
        let typed_row = result?;
        if typed_row.0 != last_shard_id + 1 {
            return Err(anyhow::anyhow!("Shard id where not contiguous"));
        }
        ret.push(typed_row.1);
        last_shard_id = typed_row.0;
    }

    Ok(ret)
}

async fn get_producer_id_for_consumer(
    session: Arc<Session>,
    consumer_id: impl AsRef<str>,
) -> anyhow::Result<Option<ProducerId>> {
    session
        .query(
            SCYLLADB_GET_CONSUMER_PRODUCER_MAPPING,
            (consumer_id.as_ref(),),
        )
        .await?
        .maybe_first_row_typed::<(ProducerId,)>()
        .map(|opt| opt.map(|row| row.0))
        .map_err(anyhow::Error::new)
}

async fn get_producer_id_with_least_assigned_consumer(
    session: Arc<Session>,
) -> anyhow::Result<Option<ProducerId>> {
    let res = session
        .query(SCYLLADB_PRODUCER_CONSUMER_COUNT, &[])
        .await?
        .rows_typed_or_empty::<(ProducerId, i32)>()
        .map(|result| result.unwrap())
        .min_by_key(|r| r.1)
        .map(|r| r.0);

    Ok(res)
}

async fn get_any_producer_info(
    session: Arc<Session>,
    producer_id: Option<ProducerId>,
) -> anyhow::Result<Option<ProducerInfo>> {
    let qr = session
        .query(
            SCYLLADB_GET_RANDOM_PRODUCER_INFO,
            (
                producer_id.unwrap_or([0x00]),
                producer_id.map(|_| false).unwrap_or(true),
            ),
        )
        .await?;

    match qr.single_row_typed::<ProducerInfo>() {
        Ok(row) => Ok(Some(row)),
        Err(SingleRowTypedError::BadNumberOfRows(_)) => Ok(None),
        Err(e) => Err(anyhow::Error::new(e)),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
enum InitialLogPosition {
    Earliest,
    Latest,
}

/// Sets the consumer shard offsets at either extreme of log tip : beginning or the end.
async fn set_consumer_shard_offsets_at_earliest_or_latest(
    session: Arc<Session>,
    new_consumer_id: impl AsRef<str>,
    producer_id: ProducerId,
    initial_log_pos: InitialLogPosition,
) -> anyhow::Result<Vec<ShardOffset>> {
    // Create all the shards counter
    let producer_info = get_any_producer_info(Arc::clone(&session), Some(producer_id))
        .await?
        .unwrap_or_else(|| panic!("Producer Info `{:?}` must exists", producer_id));

    if producer_info.num_shards as usize > MAX_NUM_SHARD {
        panic!(
            "Producer {:?} num_shards={:?} exceed hard limit of {:?}",
            producer_id, producer_info.num_shards, MAX_NUM_SHARD
        );
    }

    let joined = (0..producer_info.num_shards)
        .map(|x| format!("{:?}", x))
        .collect::<Vec<_>>()
        .join(",");

    let query = format!(
        r###"
        SELECT
            shard_id,
            offset
        FROM shard_max_offset_mv 
        WHERE 
            producer_id = ?
            AND shard_id IN ({:?})
        ORDER BY shard_id, offset {:?}
        PER PARTITION LIMIT 1
        "###,
        joined,
        if initial_log_pos == InitialLogPosition::Earliest {
            "ASC"
        } else {
            "DESC"
        }
    );

    let qargs = QueryMaxOffsetPerShardArgs {
        producer_id,
        num_shards: producer_info.num_shards,
    };
    let shard_ordered_qr = session.query(query, &qargs).await?;

    let ps = session.prepare(SCYLALDB_INSERT_CONSUMER_OFFSET).await?;

    let mut batch = Batch::default();
    // Stack buffer will be for Scylladb.
    let mut stack_buffer = [(new_consumer_id.as_ref(), producer_id, 0, 0); MAX_NUM_SHARD];
    let mut heap_buffer = Vec::with_capacity(producer_info.num_shards as usize);
    let mut expected_shard = 0_i16;
    for r in shard_ordered_qr.rows_typed_or_empty::<(ShardId, ShardOffset)>() {
        let (shard_id, offset) = r?;
        if shard_id != expected_shard {
            panic!(
                "missing {:?} shard number for producer {:?}",
                expected_shard, producer_id
            );
        }
        batch.append_statement(ps.clone());
        heap_buffer.push(offset);
        stack_buffer[expected_shard as usize] =
            (new_consumer_id.as_ref(), producer_id, shard_id, offset);
        expected_shard += 1;
    }
    let scylla_values = &stack_buffer[0..(expected_shard as usize)];
    if heap_buffer.len() != producer_info.num_shards as usize {
        panic!(
            "System is corrupted, producer {:?} num shards is missing counters",
            producer_id
        );
    }
    session.batch(&batch, scylla_values).await?;
    Ok(heap_buffer)
}

///
/// Initial position in the log when creating a new consumer.
///  
#[derive(Default)]
pub enum InitialOffsetPolicy {
    Earliest,
    #[default]
    Latest,
    SlotApprox(i64),
}

///
/// Gets an existing consumer with id = `consumer_id` if exists, otherwise creates a new consumer and return.
///
pub async fn get_or_register_consumer(
    session: Arc<Session>,
    consumer_id: impl AsRef<str>,
    initial_offset_policy: InitialOffsetPolicy,
) -> anyhow::Result<ConsumerInfo> {
    let maybe_producer_id =
        get_producer_id_for_consumer(Arc::clone(&session), consumer_id.as_ref()).await?;
    let producer_id = if let Some(producer_id) = maybe_producer_id {
        producer_id
    } else {
        let maybe = get_producer_id_with_least_assigned_consumer(Arc::clone(&session)).await?;

        let producer_id = if let Some(producer_id) = maybe {
            producer_id
        } else {
            let producer = get_any_producer_info(Arc::clone(&session), None).await?;
            producer.expect("No producer registered").producer_id
        };

        session
            .query(
                SCYLLADB_INSERT_CONSUMER_PRODUCER_MAP,
                (consumer_id.as_ref(), producer_id),
            )
            .await?;
        producer_id
    };

    let shard_offsets = get_shard_offsets_info_for_consumer_id(
        Arc::clone(&session),
        consumer_id.as_ref(),
        producer_id,
    )
    .await?;
    let shard_offsets = if !shard_offsets.is_empty() {
        shard_offsets
    } else {
        match initial_offset_policy {
            InitialOffsetPolicy::Earliest => {
                set_consumer_shard_offsets_at_earliest_or_latest(
                    Arc::clone(&session),
                    consumer_id.as_ref(),
                    producer_id,
                    InitialLogPosition::Earliest,
                )
                .await?
            }
            InitialOffsetPolicy::Latest => {
                set_consumer_shard_offsets_at_earliest_or_latest(
                    Arc::clone(&session),
                    consumer_id.as_ref(),
                    producer_id,
                    InitialLogPosition::Latest,
                )
                .await?
            }
            InitialOffsetPolicy::SlotApprox(_) => unimplemented!(),
        }
    };
    let cs = ConsumerInfo {
        consumer_id: String::from(consumer_id.as_ref()),
        producer_id,
        shard_offsets,
    };
    Ok(cs)
}

pub struct ConsumerInfo {
    pub consumer_id: ConsumerId,
    pub producer_id: ProducerId,
    pub shard_offsets: Vec<ShardOffset>,
}

struct Consumer {
    session: Arc<Session>,
    state: ConsumerInfo,
    consumer_queries: ConsumerQueries,
    sender: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
    // idx = shard id
    shard_iterators: Vec<ShardIterator>,
    // The interval at which we want to commit our Offset progression to Scylla
    offset_commit_interval: Duration,
}

///
/// Consumer queries hold pre-compiled queries (PreparedStatement) to reuse throughout the code.
#[derive(Clone)]
struct ConsumerQueries {
    session: Arc<Session>,
    log_query: PreparedStatement,
    producer_commit_log_query: PreparedStatement,
    update_shard_offset_query: PreparedStatement,
}

impl ConsumerQueries {
    async fn new(session: Arc<Session>, page_size: i32) -> anyhow::Result<Self> {
        let mut log_query = session.prepare(SCYLLADB_QUERY_LOG).await?;
        let producer_commit_log_query = session
            .prepare(SCYLLADB_PRODUCER_SHARD_PERIOD_COMMIT_EXISTS)
            .await?;
        log_query.set_page_size(page_size);
        let update_shard_offset_query = session
            .prepare(SCYLLADB_UPDATE_CONSUMER_SHARD_OFFSET)
            .await?;

        Ok(ConsumerQueries {
            session,
            log_query,
            producer_commit_log_query,
            update_shard_offset_query,
        })
    }

    ///
    /// Tries to update atomically a shard offset for a specific consumer-producer pair.
    ///
    /// It uses Light Weight Transaction (LWT) capability of ScyllaDB to make sure
    /// no two connections trying to update the same consumer offset.
    ///
    /// This is done by requesting the expected `old_offset` that is the current value in ScyllaDB
    /// before applying the update.
    async fn update_shard_offsets_for_consumer(
        &self,
        consumer_id: impl AsRef<str>,
        producer_id: ProducerId,
        shards_old_and_new_offsets: &[(OldShardOffset, ShardOffset)],
    ) -> anyhow::Result<Result<(), ShardOffset>> {
        // Since the commit offset is partitionned by consumer_id/producer_id
        // and that we using LWT, the entire batch will be atomic.
        //
        // LOGGING Batch mode is when you have a batch that span multiple partition and need some atomicity.
        // In our case, we can disable batch logging since we are batching since-partition data.
        // Apparently, this is done by default by Scylla, but we make it explicit here since the driver is not quite mature.
        let mut atomic_batch = Batch::new(BatchType::Unlogged);

        atomic_batch.append_statement(self.update_shard_offset_query.clone());

        let buffer = shards_old_and_new_offsets
            .iter()
            .enumerate()
            .filter(|(_shard_idx, (old, new))| old < new)
            .map(|(shard_idx, (old, new))| {
                (
                    new,
                    consumer_id.as_ref(),
                    producer_id,
                    shard_idx as ShardId,
                    old,
                )
            })
            .collect::<Vec<_>>();

        let query_result = self.session.batch(&atomic_batch, &buffer).await?;

        let (success, actual_offset) = query_result
            .first_row_typed::<(bool, ShardOffset)>()
            .map_err(anyhow::Error::new)?;

        if success {
            Ok(Ok(()))
        } else {
            Ok(Err(actual_offset))
        }
    }

    async fn get_log_iterator_after_offset(
        &self,
        producer_id: ProducerId,
        shard_id: ShardId,
        offset: ShardOffset,
    ) -> anyhow::Result<BoxStream<'static, BlockchainEvent>> {
        let period = offset + 1 / SHARD_OFFSET_MODULO;
        let row_it = self
            .session
            .execute_iter(
                self.log_query.clone(),
                (producer_id, shard_id, offset, period),
            )
            .await
            .map_err(anyhow::Error::new)?;

        let boxed = row_it
            .map(|result| {
                let row = result.expect("faled to execute_iter on solana log");
                row.into_typed().expect("row has unexpected format")
            })
            .boxed();
        Ok(boxed)
    }

    async fn is_period_committed(
        &self,
        producer_id: ProducerId,
        shard_id: ShardId,
        offset: ShardOffset,
    ) -> anyhow::Result<bool> {
        let period = offset / SHARD_OFFSET_MODULO;
        self.session
            .execute(
                &self.producer_commit_log_query,
                (producer_id, shard_id, period),
            )
            .await
            .map(|qr| qr.maybe_first_row().map(|_| true).unwrap_or(false))
            .map_err(anyhow::Error::new)
    }
}

pub async fn spawn_consumer(
    session: Arc<Session>,
    consumer_info: ConsumerInfo,
    buffer_capacity: Option<usize>,
    offset_commit_interval: Option<Duration>,
) -> anyhow::Result<mpsc::Receiver<Result<SubscribeUpdate, tonic::Status>>> {
    let buffer_capacity = buffer_capacity.unwrap_or(DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY);
    let (sender, receiver) = mpsc::channel(buffer_capacity);
    //let last_committed_offsets = state.shard_offsets.clone();
    let consumer_session = Arc::clone(&session);

    let cq = ConsumerQueries::new(Arc::clone(&session), buffer_capacity as i32).await?;
    let num_shards = consumer_info.shard_offsets.len();

    //let offset_commit_interval = self.offset_commit_interval.unwrap_or(DEFAULT_OFFSET_COMMIT_INTERVAL);

    let mut shard_iterators = Vec::with_capacity(num_shards);
    for i in 0..num_shards {
        let shard_iterator = ShardIterator::new(
            Arc::clone(&session),
            cq.clone(),
            consumer_info.producer_id,
            i as i16,
            consumer_info.shard_offsets[i],
        );
        shard_iterators.push(shard_iterator);
    }

    let consumer = Consumer {
        session: consumer_session,
        state: consumer_info,
        consumer_queries: cq.clone(),
        sender,
        shard_iterators,
        offset_commit_interval: offset_commit_interval.unwrap_or(DEFAULT_OFFSET_COMMIT_INTERVAL),
    };

    tokio::spawn(async move {
        consumer
            .serve()
            .await
            .expect("consumer terminated abruptly");
    });
    Ok(receiver)
}

/// Here's the flow of the state machine a shard iterator go through it lifetime.
///                                  _____
///                                 |     |
///                                 |     |
///                                 v     |
/// Empty ----> Available ----> EndOfPeriod --+
///   ^                                       |
///   |                                       |
///   +---------------------------------------+
enum ShardIteratorState {
    Empty(ShardOffset),
    Available(ShardOffset, BoxStream<'static, BlockchainEvent>),
    EndOfPeriod(ShardOffset),
}

impl ShardIteratorState {
    fn last_offset(&self) -> ShardOffset {
        match self {
            Self::Empty(offset) => *offset,
            Self::Available(offset, _) => *offset,
            Self::EndOfPeriod(offset) => *offset,
        }
    }
}

struct ShardIterator {
    session: Arc<Session>,
    consumer_queries: ConsumerQueries,
    producer_id: ProducerId,
    shard_id: ShardId,
    inner: ShardIteratorState,
}

impl ShardIterator {
    fn new(
        session: Arc<Session>,
        consumer_queries: ConsumerQueries,
        producer_id: ProducerId,
        shard_id: ShardId,
        offset: ShardOffset,
    ) -> Self {
        ShardIterator {
            session,
            consumer_queries,
            producer_id,
            shard_id,
            inner: ShardIteratorState::Empty(offset),
        }
    }
    async fn try_next(&mut self) -> anyhow::Result<Option<BlockchainEvent>> {
        let last_offset = self.inner.last_offset();
        let current_state =
            std::mem::replace(&mut self.inner, ShardIteratorState::Empty(last_offset));

        //let inner_ptr = self.inner.borrow_mut();
        let (next_state, maybe_to_return) = match current_state {
            ShardIteratorState::Empty(last_offset) => {
                let row_stream = self
                    .consumer_queries
                    .get_log_iterator_after_offset(self.producer_id, self.shard_id, last_offset)
                    .await?;
                (ShardIteratorState::Available(last_offset, row_stream), None)
            }
            ShardIteratorState::Available(last_offset, mut row_stream) => {
                let row = row_stream.next().await;
                let next_state = if row.is_none() {
                    if (last_offset + 1) % SHARD_OFFSET_MODULO == 0 {
                        ShardIteratorState::EndOfPeriod(last_offset)
                    } else {
                        ShardIteratorState::Empty(last_offset)
                    }
                } else {
                    ShardIteratorState::Available(last_offset + 1, row_stream)
                };
                (next_state, row)
            }
            ShardIteratorState::EndOfPeriod(last_offset) => {
                let is_period_committed = self
                    .consumer_queries
                    .is_period_committed(self.producer_id, self.shard_id, last_offset)
                    .await?;
                let next_state = if is_period_committed {
                    ShardIteratorState::Empty(last_offset)
                } else {
                    ShardIteratorState::EndOfPeriod(last_offset)
                };
                (next_state, None)
            }
        };
        let _ = std::mem::replace(&mut self.inner, next_state);
        Ok(maybe_to_return)
    }
}

impl Consumer {
    async fn serve(mut self) -> anyhow::Result<()> {
        let consumer_id = self.state.consumer_id;
        let producer_id = self.state.producer_id;
        let mut commit_offset_deadline = Instant::now() + self.offset_commit_interval;

        // Allocate all of our Vec once here.
        let mut curr_shard_offsets = self.state.shard_offsets.clone();
        let mut shard_offset_last_committed = self.state.shard_offsets.clone();

        let mut shards_old_and_new_offsets = shard_offset_last_committed
            .iter()
            .cloned()
            .zip(curr_shard_offsets.iter().cloned())
            .collect::<Vec<_>>();

        loop {
            for (i, shard_it) in self.shard_iterators.iter_mut().enumerate() {
                let maybe = shard_it.try_next().await?;
                if let Some(block_chain_event) = maybe {
                    let event_offset = block_chain_event.offset;
                    let geyser_event = match block_chain_event.entry_type {
                        BlockchainEventType::AccountUpdate => {
                            UpdateOneof::Account(block_chain_event.try_into()?)
                        }
                        BlockchainEventType::NewTransaction => {
                            UpdateOneof::Transaction(block_chain_event.try_into()?)
                        }
                    };
                    let subscribe_update = SubscribeUpdate {
                        filters: Default::default(),
                        update_oneof: Some(geyser_event),
                    };
                    let permit = self.sender.reserve().await?;
                    permit.send(Ok(subscribe_update));
                    curr_shard_offsets[i] = event_offset;
                }
            }

            // Every now and then, we commit where the consumer is loc
            if commit_offset_deadline.elapsed() > Duration::ZERO {
                shards_old_and_new_offsets.splice(
                    ..,
                    shard_offset_last_committed
                        .iter()
                        .cloned()
                        .zip(curr_shard_offsets.iter().cloned()),
                );

                self.consumer_queries
                    .update_shard_offsets_for_consumer(
                        consumer_id.as_str(),
                        producer_id,
                        &shards_old_and_new_offsets,
                    )
                    .await?;

                // Swap old committed offset with new committed offset.
                shard_offset_last_committed.copy_from_slice(&curr_shard_offsets[..]);

                commit_offset_deadline = Instant::now() + self.offset_commit_interval;
            }
        }
    }
}

pub struct ScyllaYsLog {
    session: Arc<Session>,
}

pub type LogStream = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl YellowstoneLog for ScyllaYsLog {
    #[doc = r" Server streaming response type for the consume method."]
    type consumeStream = LogStream;

    async fn consume(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<tonic::Response<Self::consumeStream>, tonic::Status> {
        let cr = request.into_inner();

        let session = Arc::clone(&self.session);
        let consumer_state =
            get_or_register_consumer(session, cr.consumer_id.as_str(), Default::default())
                .await
                .map_err(|e| {
                    error!("{:?}", e);
                    tonic::Status::new(
                        tonic::Code::Internal,
                        format!("failed to get or create consumer {:?}", cr.consumer_id),
                    )
                })?;

        let rx = spawn_consumer(Arc::clone(&self.session), consumer_state, None, None)
            .await
            .map_err(|_e| tonic::Status::internal("fail to spawn consumer"))?;

        let ret = ReceiverStream::new(rx);

        let res = Response::new(Box::pin(ret) as Self::consumeStream);
        Ok(res)
    }
}
