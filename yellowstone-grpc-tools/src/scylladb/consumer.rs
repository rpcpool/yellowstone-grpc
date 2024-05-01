use core::{fmt, panic};
use std::any;
use std::char::MAX;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::ops::Sub;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::BoxStream;
use futures::StreamExt;
use hyper::header::LAST_MODIFIED;
use rdkafka::consumer::Consumer;
use rdkafka::Offset;
use scylla::batch::Batch;
use scylla::frame::response::result::{ColumnType, Row};
use scylla::frame::value::Value;
use scylla::prepared_statement::PreparedStatement;
use scylla::routing::Shard;
use scylla::serialize::batch::BatchValuesFromIterator;
use scylla::serialize::value::SerializeCql;
use scylla::transport::errors::QueryError;
use scylla::transport::iterator::RowIterator;
use scylla::transport::query_result::SingleRowTypedError;
use scylla::{QueryResult, Session};
use scylla::serialize::row::{SerializeRow, SerializedValues};
use sha2::digest::block_buffer::Block;
use sha2::digest::typenum::Prod;
use tokio::time::Instant;
use tonic::Response;
use yellowstone_grpc_proto::prost::bytes::buf;
use yellowstone_grpc_proto::yellowstone::log::{yellowstone_log_server::YellowstoneLog, ConsumeRequest};
use yellowstone_grpc_proto::geyser::{SubscribeRequestAccountsDataSlice, SubscribeUpdate};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tokio::sync::mpsc;
use crate::scylladb::types::SHARD_COUNT;
use tracing::{error, warn};
use super::types::{BlockchainEvent, ProducerId, ProducerInfo, ShardId, ShardOffset, ShardPeriod, SHARD_OFFSET_MODULO};

/**
 * This is a constant where we won't ever support more than 1024 shards.
 * This constant is helpful to create Stack
 */
const MAX_NUM_SHARD: usize = 1024;

const DEFAULT_OFFSET_COMMIT_INTERVAL: Duration = Duration::from_secs(10);

const DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY: usize = 100;

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
    ORDER BY offset
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
    ORDER BY shard_id
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
        ctx: &scylla::serialize::row::RowSerializationContext<'_>,
        writer: &mut scylla::serialize::RowWriter,
    ) -> Result<(), scylla::serialize::SerializationError> {

        let mut cw = writer.make_cell_writer();
        let pid = self.producer_id.as_ref();
        SerializeCql::serialize(&self.producer_id, &ColumnType::Blob, cw).map(|_proof| ())?;
        for shard_id in 0..self.num_shards {
            let mut cw = writer.make_cell_writer();
            SerializeCql::serialize(&shard_id, &ColumnType::SmallInt, cw)
                .map(|_proof| ())?;
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.num_shards == 0
    }
}


async fn get_shard_offsets_info_for_consumer_id(session: Arc<Session>, consumer_id: impl AsRef<str>, producer_id: ProducerId) -> anyhow::Result<Vec<ShardOffset>> {
    let qr = session
        .query(SCYLLADB_GET_SHARD_OFFSETS_INFO_FOR_CONSUMER_ID, (consumer_id.as_ref(), producer_id))
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

async fn get_producer_id_for_consumer(session: Arc<Session>, consumer_id: impl AsRef<str>) -> anyhow::Result<Option<ProducerId>> {
    session
        .query(SCYLLADB_GET_CONSUMER_PRODUCER_MAPPING, (consumer_id.as_ref(),))
        .await?
        .maybe_first_row_typed::<(ProducerId,)>()
        .map(|opt| opt.map(|row| row.0))
        .map_err(anyhow::Error::new)
}

async fn get_producer_id_with_least_assigned_consumer(session: Arc<Session>) -> anyhow::Result<Option<ProducerId>> {
    let res = session
        .query(SCYLLADB_PRODUCER_CONSUMER_COUNT, &[])
        .await?
        .rows_typed_or_empty::<(ProducerId, i32)>()
        .map(|result| result.unwrap())
        .min_by_key(|r| r.1)
        .map(|r| r.0);

    Ok(res)
}


async fn list_all_producers(session: Arc<Session>) -> anyhow::Result<Vec<ProducerInfo>> {
    let qr = session.query(SCYLLADB_LIST_PRODUCER_INFO, &[]).await?;

    let ret = qr
        .rows_typed_or_empty::<ProducerInfo>()
        .map(|result| result.unwrap())
        .collect();

    Ok(ret)
}


async fn get_any_producer_info(session: Arc<Session>, producer_id: Option<ProducerId>) -> anyhow::Result<Option<ProducerInfo>> {
    let qr = session
        .query(
            SCYLLADB_GET_RANDOM_PRODUCER_INFO, 
            (producer_id.unwrap_or([0x00]), producer_id.map(|_| false).unwrap_or(true))
        )
        .await?;

    match qr.single_row_typed::<ProducerInfo>() {
        Ok(row) => Ok(Some(row)),
        Err(SingleRowTypedError::BadNumberOfRows(_)) => Ok(None),
        Err(e) => Err(anyhow::Error::new(e))
    }
}


async fn set_consumer_shard_offsets(session: Arc<Session>, new_consumer_id: impl AsRef<str>, producer_id: ProducerId) -> anyhow::Result<Vec<ShardOffset>> {

    // Create all the shards counter
    let producer_info= get_any_producer_info(Arc::clone(&session), Some(producer_id))
        .await?
        .expect(format!("Producer Info `{:?}` must exists", producer_id).as_str());

    if producer_info.num_shards as usize > MAX_NUM_SHARD { 
        panic!("Producer {:?} num_shards={:?} exceed hard limit of {:?}", producer_id, producer_info.num_shards, MAX_NUM_SHARD);
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
        ORDER BY shard_id, offset DESC 
        PER PARTITION LIMIT 1
        "###,
        joined
    );

    let qargs = QueryMaxOffsetPerShardArgs {
        producer_id: producer_id,
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
            panic!("missing {:?} shard number for producer {:?}", expected_shard, producer_id);
        }
        batch.append_statement(ps.clone());
        heap_buffer.push(offset);
        stack_buffer[expected_shard as usize] = (new_consumer_id.as_ref(), producer_id, shard_id, offset);
        expected_shard += 1;
    }
    let scylla_values = &stack_buffer[0..(expected_shard as usize)];
    if heap_buffer.len() != producer_info.num_shards as usize {
        panic!("System is corrupted, producer {:?} num shards is missing counters", producer_id);
    }
    session.batch(&batch, scylla_values).await?;
    Ok(heap_buffer)

}

async fn get_or_create_consumer(session: Arc<Session>, consumer_id: impl AsRef<str>) -> anyhow::Result<ConsumerState> {
    let maybe_producer_id = get_producer_id_for_consumer(Arc::clone(&session), consumer_id.as_ref()).await?;
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

        session.query(
            SCYLLADB_INSERT_CONSUMER_PRODUCER_MAP,
            (consumer_id.as_ref(), producer_id)
        ).await?;
        producer_id
    };

    let shard_offsets = get_shard_offsets_info_for_consumer_id(
        Arc::clone(&session), 
        consumer_id.as_ref(), 
        producer_id
    ).await?;
    let shard_offsets = if !shard_offsets.is_empty() {
        shard_offsets
    } else {
        set_consumer_shard_offsets(
            Arc::clone(&session),
            consumer_id.as_ref(), 
            producer_id
        ).await?
    };
    let cs = ConsumerState {
        consumer_id: String::from(consumer_id.as_ref()),
        producer_id,
        shard_offsets,
    };
    Ok(cs)
}

struct ConsumerState {
    consumer_id: String,
    producer_id: ProducerId,
    shard_offsets: Vec<ShardOffset>,
}

struct ScyllaConsumer {
    session: Arc<Session>,
    state: ConsumerState,
    log_query: LogQuery,
    commit_exists_query: ProducerCommitExistsQuery,
    sender: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
    last_committed_offsets: Vec<ShardOffset>,
    offset_commit_interval: Duration,
    next_offset_commit: Instant,
    loaded_row_iterators: Vec<ShardIterator>,
    //loaded_row_iterators: Vec<Option<BoxStream<'static, anyhow::Result<BlockchainEvent>>>>,
}

struct ScyllaConsumerStreamBuilder {
    session: Arc<Session>,
    state: Option<ConsumerState>,
    buffer_capacity: Option<usize>,
    offset_commit_interval: Option<Duration>,
}

///
/// We wrapped a prepared stmt into a specific type so we force API user to specify query args in the proper types and ordering.
/// 
#[derive(Clone)]
struct LogQuery(PreparedStatement);

impl LogQuery {

    async fn execute(&self, session: Arc<Session>, producer_id: ProducerId, shard_id: ShardId, offset: ShardOffset) -> anyhow::Result<BoxStream<'static, anyhow::Result<BlockchainEvent>>> {
        let period = (offset / SHARD_OFFSET_MODULO);
        let row_it = session.execute_iter(
            self.0.clone(), 
            (producer_id, shard_id, offset, period), 
        ).await.map_err(anyhow::Error::new)?;

        let boxed = row_it.map(|result| 
            result
                .map_err(anyhow::Error::new)
                .and_then(|row| 
                    row
                        .into_typed::<BlockchainEvent>()
                        .map_err(anyhow::Error::new)
                )
        ).boxed();
        Ok(boxed)
    }
}

#[derive(Clone)]
struct ConsumerQueryService {
    session: Arc<Session>,
    log_query: PreparedStatement,
    producer_commit_log_query: PreparedStatement,
}

impl ConsumerQueryService {

    async fn new(session: Arc<Session>, page_size: i32) -> anyhow::Result<Self> {
        let mut log_query = session.prepare(SCYLLADB_QUERY_LOG).await?;
        let producer_commit_log_query = session.prepare(SCYLLADB_PRODUCER_SHARD_PERIOD_COMMIT_EXISTS).await?;
        log_query.set_page_size(page_size);

        Ok(ConsumerQueryService{
            session,
            log_query,
            producer_commit_log_query,
        })
    }

    async fn get_log_iterator_at_offset(&self, producer_id: ProducerId, shard_id: ShardId, offset: ShardOffset) -> anyhow::Result<RowIterator> {
        let period = (offset / SHARD_OFFSET_MODULO);
        let row_it = session.execute_iter(
            self.0.clone(), 
            (producer_id, shard_id, offset, period), 
        ).await.map_err(anyhow::Error::new)?;

        let boxed = row_it.map(|result| 
            result
                .map_err(anyhow::Error::new)
                .and_then(|row| 
                    row
                        .into_typed::<BlockchainEvent>()
                        .map_err(anyhow::Error::new)
                )
        ).boxed();
        Ok(boxed)
    }

    async fn is_period_committed(&self, producer_id: ProducerId, shard_id: ShardId, period: ShardPeriod) -> anyhow::Result<bool> {
        self.session.execute(
            &self.producer_commit_log_query, 
            (producer_id, shard_id, period), 
        ).await
        .map(|qr| qr.maybe_first_row().map(|_| true).unwrap_or(false))
        .map_err(anyhow::Error::new)
    }
}


impl ScyllaConsumerStreamBuilder {

    fn new(session: Arc<Session>) -> Self {
        ScyllaConsumerStreamBuilder {
            session,
            state: Default::default(),
            buffer_capacity: Default::default(),
            offset_commit_interval: Default::default(),
        }
    }

    pub fn with_state(mut self, state: ConsumerState) -> Self {
        self.state.replace(state);
        self
    }

    pub fn with_buffer_capacity(mut self, buffer_capacity: usize) -> Self {
        self.buffer_capacity.replace(buffer_capacity);
        self
    }

    pub fn with_offset_commit_interval(mut self, interval: Duration) -> Self {
        self.offset_commit_interval.replace(interval);
        self
    }

    async fn serve(self) -> anyhow::Result<mpsc::Receiver<Result<SubscribeUpdate, tonic::Status>>> {
        let buffer_capacity = self.buffer_capacity.unwrap_or(DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY);
        let (sender, receiver) = mpsc::channel(buffer_capacity);
        let state = self.state.ok_or(anyhow::anyhow!("consumer state is not set"))?;
        let last_committed_offsets = state.shard_offsets.clone();
        let consumer_session = Arc::clone(&self.session);

        let num_shards = state.shard_offsets.len();
        let lq = LogQuery(query_log_ps);
        let commit_exists_query = ProducerCommitExistsQuery(commit_exists_ps);

        let offset_commit_interval = self.offset_commit_interval.unwrap_or(DEFAULT_OFFSET_COMMIT_INTERVAL);
        
        let mut loaded_row_iterators = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            loaded_row_iterators.push(ShardIterator::new(Arc::clone(&self.session), i));
        }

        let consumer = ScyllaConsumer {
            session: consumer_session,
            state,
            log_query: lq,
            commit_exists_query,
            sender,
            last_committed_offsets,
            offset_commit_interval,
            next_offset_commit: Instant::now() + offset_commit_interval,
            loaded_row_iterators,
        };

        tokio::spawn(async move {
            consumer.serve().await;
        });
        Ok(receiver)
    }
}

#[derive(Debug)]
enum TryNextError {
    NoNewEvent,
}


impl fmt::Display for TryNextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self, f)
    }
}


impl Error for TryNextError {}



enum ShardIteratorState {
    Available(RowIterator),
    Empty,
    EndOfPeriod,
}

struct ShardIterator {
    session: Arc<Session>,
    shard_idx: usize,
    inner: ShardIteratorState,
}

impl ShardIterator {
    fn new(session: Arc<Session>, shard_idx: usize) -> Self {
        ShardIterator {
            session,
            shard_idx,
            inner: ShardIteratorState::Empty,
        }
    }
}

impl ScyllaConsumer {

    async fn create_new_shard_iter(&self, shard_idx: usize) -> anyhow::Result<BoxStream<'static, anyhow::Result<BlockchainEvent>>> {
         // Create row iterator if the shard was never loaded
        let offset = self.state.shard_offsets[shard_idx];
        self.log_query
            .execute(Arc::clone(&self.session), self.state.producer_id, shard_idx as i16, offset)
            .await
    }

    async fn check_if_period_is_committed(&self, shard_idx: usize) -> anyhow::Result<bool> {
        let curr_period = self.state.shard_offsets[shard_idx] / SHARD_OFFSET_MODULO;
        self.commit_exists_query
            .execute(Arc::clone(&self.session), self.state.producer_id, shard_idx as i16, curr_period)
            .await
    }

    async fn try_next_for_shard(&mut self, shard_idx: usize) -> anyhow::Result<BlockchainEvent> {


        let maybe_stream = self.loaded_row_iterators[]
            .get_mut(shard_idx)
            .expect("shard idx is out of bound");
        // if not scylla page iterator has been open yet
        //let maybe_stream = &mut self.loaded_row_iterators[shard_idx];
        // maybe_stream.
        if maybe_stream.is_none() {
            let row_it = self.create_new_shard_iter(shard_idx).await?;
            *maybe_stream = Some(row_it);
        };

        // let row_it = maybe_stream
        //     .as_mut()
        //     .expect("failed to create row iterator over scylla");

        // let maybe = row_it.next().await.transpose()?;

        // if let Some(row) = maybe {
        //     return Ok(row)
        // } else {
        //     let curr_offset = self.state.shard_offsets[shard_idx];
        //     if curr_offset % SHARD_OFFSET_MODULO == 0 {
        //         // // We are at the end of a period
        //         // //let period_committed = self.check_if_period_is_committed(shard_idx).await?;
        //         // if period_committed {
        //         //     // Drop the current iter and return EndOfPeriod
        //         //     // a new 
        //         //     *maybe_stream = None;
        //         //     return Err(anyhow::Error::new(TryNextError::EndOfPeriod))
        //         // } else {
        //         //     // We set the stream to null so next call reevaluate up to check_if_period_is_committed again.
        //         //     return Ok(None)
        //         // }
                
        //     } 
        //     // Check if our period is done and if we can change period.
        // }

        //*maybe_stream = None;
        return anyhow::bail!("test");
    }

    async fn commit_offsets(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn serve(mut self) -> anyhow::Result<()> {
        // let buffer = Vec::with_capacity(DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY);
        // loop {
        //     for shard in self.state.shard_offsets {
        //         let permit = self.sender.reserve().await?;
        //         self.try_next_for_shard(shard).await
        //     }
        //     buffer.push(value);
        // }
        Ok(())
    }

}


struct ScyllaYsLog {
    session: Arc<Session>,
    conn_buffer_capacity: usize,
}


type LogStream = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl YellowstoneLog for ScyllaYsLog {
    #[doc = r" Server streaming response type for the consume method."]
    type consumeStream = LogStream;

    async fn consume(&self, request: tonic::Request<ConsumeRequest>,) -> Result<tonic::Response<Self::consumeStream>, tonic::Status> {

        let cr = request.into_inner();

        let session = Arc::clone(&self.session);
        let consumer_state = get_or_create_consumer(session, cr.consumer_id.as_ref())
            .await
            .map_err(|e| {
                error!("{:?}", e);
                tonic::Status::new(tonic::Code::Internal, format!("failed to get or create consumer {:?}", cr.consumer_id))
            })?;
        
        let rx = ScyllaConsumerStreamBuilder::new(Arc::clone(&session))
            .with_state(consumer_state)
            .serve()
            .await?;

        let ret = ReceiverStream::new(rx);

        let res = Response::new(Box::pin(ret) as Self::consumeStream);
        Ok(res)
    }
}