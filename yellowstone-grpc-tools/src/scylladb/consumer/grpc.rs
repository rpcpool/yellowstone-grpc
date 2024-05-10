use {
    super::{
        common::{self, ConsumerId, ConsumerInfo, InitialOffsetPolicy},
        shard_iterator::{ShardFilter, ShardIterator},
    },
    crate::scylladb::types::{
        BlockchainEventType, ProducerId, ProducerInfo, ShardId, ShardOffset, MAX_PRODUCER,
        MIN_PROCUDER,
    },
    futures::Stream,
    scylla::{
        batch::{Batch, BatchType},
        cql_to_rust::FromCqlVal,
        prepared_statement::PreparedStatement,
        query::Query,
        transport::query_result::SingleRowTypedError,
        Session,
    },
    std::{iter::repeat, pin::Pin, sync::Arc, time::Duration},
    tokio::{sync::mpsc, task::JoinSet, time::Instant},
    tokio_stream::wrappers::ReceiverStream,
    tonic::Response,
    tracing::{error, info},
    uuid::Uuid,
    yellowstone_grpc_proto::{
        geyser::{subscribe_update::UpdateOneof, SubscribeUpdate},
        yellowstone::log::{
            yellowstone_log_server::YellowstoneLog, ConsumeRequest, EventSubscriptionPolicy,
        },
    },
};

const DEFAULT_OFFSET_COMMIT_INTERVAL: Duration = Duration::from_secs(10);

const DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY: usize = 100;

const UPDATE_CONSUMER_SHARD_OFFSET: &str = r###"
    UPDATE consumer_info
    SET offset = ?, updated_at = currentTimestamp() 
    WHERE 
        consumer_id = ?
        AND producer_id = ?
        AND shard_id = ?
        AND event_type = ?
    IF offset = ?
"###;

pub const GET_MIN_OFFSET_FOR_SLOT: &str = r###"
    SELECT
        shard_id,
        min(offset)
    FROM solana.slot_map_mv
    WHERE slot = ? and producer_id = ?
    ORDER BY shard_id
    GROUP BY shard_id;
"###;

pub const INSERT_CONSUMER_OFFSET: &str = r###"
    INSERT INTO consumer_info (
        consumer_id,
        producer_id,
        shard_id,
        event_type,
        offset,
        created_at,
        updated_at
    )
    VALUES
    (?,?,?,?,?,currentTimestamp(), currentTimestamp())
"###;

pub const GET_CONSUMER_PRODUCER_MAPPING: &str = r###"
    SELECT
        producer_id
    FROM consumer_producer_mapping
    where consumer_id = ?
"###;

pub const GET_SHARD_OFFSETS_FOR_CONSUMER_ID: &str = r###"
    SELECT
        shard_id,
        event_type,
        offset
    FROM consumer_info
    WHERE 
        consumer_id = ?
        AND producer_id = ?
    ORDER BY shard_id ASC
"###;

pub const GET_PRODUCERS_CONSUMER_COUNT: &str = r###"
    SELECT
        producer_id,
        count(1)
    FROM producer_consumer_mapping_mv
    GROUP BY producer_id
"###;

pub const INSERT_CONSUMER_PRODUCER_MAPPING: &str = r###"
    INSERT INTO consumer_producer_mapping (
        consumer_id,
        producer_id,
        created_at,
        updated_at
    )
    VALUES (?, ?, currentTimestamp(), currentTimestamp())
"###;

///
/// CQL does not support OR conditions,
/// this is why use >=/<= to emulate the following condition: (producer_id = ? or ?)
/// produ
pub const GET_PRODUCER_INFO_BY_ID_OR_ANY: &str = r###"
    SELECT
        producer_id,
        num_shards,
        is_active
    FROM producer_info
    WHERE producer_id >= ? and producer_id <= ?
    LIMIT 1
    ALLOW FILTERING
"###;

///
/// Returns the latest offset per shard for a consumer id
///
pub async fn get_shard_offsets_info_for_consumer_id(
    session: Arc<Session>,
    consumer_id: impl AsRef<str>,
    producer_id: ProducerId,
) -> anyhow::Result<Vec<(ShardId, BlockchainEventType, ShardOffset)>> {
    let qr = session
        .query(
            GET_SHARD_OFFSETS_FOR_CONSUMER_ID,
            (consumer_id.as_ref(), producer_id),
        )
        .await?;

    let mut ret = Vec::new();
    for result in qr.rows_typed_or_empty::<(ShardId, BlockchainEventType, ShardOffset)>() {
        let typed_row = result?;
        ret.push(typed_row);
    }

    Ok(ret)
}

///
/// Returns the assigned producer id to specific consumer if any.
///
pub async fn get_producer_id_for_consumer(
    session: Arc<Session>,
    consumer_id: impl AsRef<str>,
) -> anyhow::Result<Option<ProducerId>> {
    session
        .query(GET_CONSUMER_PRODUCER_MAPPING, (consumer_id.as_ref(),))
        .await?
        .maybe_first_row_typed::<(ProducerId,)>()
        .map(|opt| opt.map(|row| row.0))
        .map_err(anyhow::Error::new)
}

///
/// Returns the producer id with least consumer assignment.
///
pub async fn get_producer_id_with_least_assigned_consumer(
    session: Arc<Session>,
) -> anyhow::Result<Option<ProducerId>> {
    let res = session
        .query(GET_PRODUCERS_CONSUMER_COUNT, &[])
        .await?
        .rows_typed_or_empty::<(ProducerId, i32)>()
        .map(|result| result.unwrap())
        .min_by_key(|r| r.1)
        .map(|r| r.0);

    Ok(res)
}

///
/// Returns a specific producer information by id or return a random producer_info if `producer_id` is None.
pub async fn get_producer_info_by_id_or_any(
    session: Arc<Session>,
    producer_id: Option<ProducerId>,
) -> anyhow::Result<Option<ProducerInfo>> {
    let qr = session
        .query(
            GET_PRODUCER_INFO_BY_ID_OR_ANY,
            (
                producer_id.unwrap_or(MIN_PROCUDER),
                producer_id.unwrap_or(MAX_PRODUCER),
            ),
        )
        .await?;

    match qr.single_row_typed::<ProducerInfo>() {
        Ok(row) => Ok(Some(row)),
        Err(SingleRowTypedError::BadNumberOfRows(_)) => Ok(None),
        Err(e) => Err(anyhow::Error::new(e)),
    }
}

fn get_blockchain_event_types(
    event_sub_policy: EventSubscriptionPolicy,
) -> Vec<BlockchainEventType> {
    match event_sub_policy {
        EventSubscriptionPolicy::AccountUpdateOnly => vec![BlockchainEventType::AccountUpdate],
        EventSubscriptionPolicy::TransactionOnly => vec![BlockchainEventType::NewTransaction],
        EventSubscriptionPolicy::Both => vec![
            BlockchainEventType::AccountUpdate,
            BlockchainEventType::NewTransaction,
        ],
    }
}

///
/// Gets an existing consumer with id = `consumer_id` if exists, otherwise creates a new consumer.
///
pub async fn get_or_register_consumer(
    session: Arc<Session>,
    consumer_id: impl AsRef<str>,
    initial_offset_policy: InitialOffsetPolicy,
    event_sub_policy: EventSubscriptionPolicy,
) -> anyhow::Result<ConsumerInfo> {
    let maybe_producer_id =
        get_producer_id_for_consumer(Arc::clone(&session), consumer_id.as_ref()).await?;
    let producer_id = if let Some(producer_id) = maybe_producer_id {
        info!(
            "consumer {:?} exists with producer {:?} assigned to it",
            consumer_id.as_ref(),
            producer_id
        );
        producer_id
    } else {
        let maybe = get_producer_id_with_least_assigned_consumer(Arc::clone(&session)).await?;

        let producer_id = if let Some(producer_id) = maybe {
            producer_id
        } else {
            let producer = get_producer_info_by_id_or_any(Arc::clone(&session), None).await?;
            producer.expect("No producer registered").producer_id
        };
        info!(
            "consumer {:?} does not exists, will try to assign producer {:?}",
            consumer_id.as_ref(),
            producer_id
        );

        session
            .query(
                INSERT_CONSUMER_PRODUCER_MAPPING,
                (consumer_id.as_ref(), producer_id),
            )
            .await?;
        info!(
            "consumer {:?} successfully assigned producer {:?}",
            consumer_id.as_ref(),
            producer_id
        );
        producer_id
    };

    let ev_types = get_blockchain_event_types(event_sub_policy);

    let shard_offsets = get_shard_offsets_info_for_consumer_id(
        Arc::clone(&session),
        consumer_id.as_ref(),
        producer_id,
    )
    .await?;
    let shard_offsets = if !shard_offsets.is_empty() {
        shard_offsets
    } else {
        info!(
            "new consumer {:?} initial offset policy {:?}",
            consumer_id.as_ref(),
            initial_offset_policy
        );
        set_initial_consumer_shard_offsets(
            Arc::clone(&session),
            consumer_id.as_ref(),
            producer_id,
            initial_offset_policy,
            event_sub_policy,
        )
        .await?
    };
    let cs = ConsumerInfo {
        consumer_id: String::from(consumer_id.as_ref()),
        producer_id,
        shard_offsets,
        subscribed_blockchain_event_types: ev_types,
    };
    Ok(cs)
}

fn build_offset_per_shard_query(num_shards: ShardId, ordering: &str) -> impl Into<Query> {
    let shard_bind_markers = (0..num_shards)
        .map(|x| format!("{}", x))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r###"
        SELECT
            shard_id,
            offset
        FROM shard_max_offset_mv
        WHERE
            producer_id = ?
            AND shard_id IN ({shard_bind_markers})
        ORDER BY offset {ordering}, period {ordering}
        PER PARTITION LIMIT 1
        "###,
        shard_bind_markers = shard_bind_markers,
        ordering = ordering
    )
}

fn get_max_offset_per_shard_query(num_shards: ShardId) -> impl Into<Query> {
    build_offset_per_shard_query(num_shards, "DESC")
}

fn get_min_offset_per_shard_query(num_shards: ShardId) -> impl Into<Query> {
    build_offset_per_shard_query(num_shards, "ASC")
}

/// Sets the initial shard offsets for a newly created consumer based on [[`InitialOffsetPolicy`]].
///
/// Similar to seeking in a file, we can seek right at the beginning of the log, completly at the end or at first
/// log event containg a specific slot number.
async fn set_initial_consumer_shard_offsets(
    session: Arc<Session>,
    new_consumer_id: impl AsRef<str>,
    producer_id: ProducerId,
    initial_offset_policy: InitialOffsetPolicy,
    event_sub_policy: EventSubscriptionPolicy,
) -> anyhow::Result<Vec<(ShardId, BlockchainEventType, ShardOffset)>> {
    // Create all the shards counter
    let producer_info = get_producer_info_by_id_or_any(Arc::clone(&session), Some(producer_id))
        .await?
        .unwrap_or_else(|| panic!("Producer Info `{:?}` must exists", producer_id));

    let num_shards = producer_info.num_shards;

    let shard_offsets_query_result = match initial_offset_policy {
        InitialOffsetPolicy::Latest => {
            session
                .query(get_max_offset_per_shard_query(num_shards), (producer_id,))
                .await?
        }
        InitialOffsetPolicy::Earliest => {
            session
                .query(get_min_offset_per_shard_query(num_shards), (producer_id,))
                .await?
        }
        InitialOffsetPolicy::SlotApprox(slot) => {
            session
                .query(GET_MIN_OFFSET_FOR_SLOT, (slot, producer_id))
                .await?
        }
    };

    let adjustment = match initial_offset_policy {
        InitialOffsetPolicy::Earliest | InitialOffsetPolicy::SlotApprox(_) => -1,
        InitialOffsetPolicy::Latest => 0,
    };

    let insert_consumer_offset_ps: PreparedStatement =
        session.prepare(INSERT_CONSUMER_OFFSET).await?;

    let rows = shard_offsets_query_result
        .rows_typed_or_empty::<(ShardId, ShardOffset)>()
        .collect::<Result<Vec<_>, _>>()?;

    let mut batch = Batch::new(BatchType::Unlogged);
    let mut buffer = Vec::with_capacity(rows.len());

    let ev_types = get_blockchain_event_types(event_sub_policy);

    ev_types
        .into_iter()
        .flat_map(|ev_type| {
            rows.iter()
                .cloned()
                .map(move |(shard_id, offset)| (ev_type, shard_id, offset))
        })
        .for_each(|(ev_type, shard_id, offset)| {
            let offset = offset + adjustment;
            batch.append_statement(insert_consumer_offset_ps.clone());
            buffer.push((
                new_consumer_id.as_ref(),
                producer_id,
                shard_id,
                ev_type,
                offset,
            ));
        });

    session.batch(&batch, &buffer).await?;

    let shard_offsets = buffer
        .drain(..)
        .map(|(_, _, shard_id, ev_type, offset)| (shard_id, ev_type, offset))
        .collect::<Vec<_>>();

    Ok(shard_offsets)
}

pub struct ScyllaYsLog {
    session: Arc<Session>,
}

pub type LogStream = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl YellowstoneLog for ScyllaYsLog {
    #[doc = r" Server streaming response type for the consume method."]
    type ConsumeStream = LogStream;

    async fn consume(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<tonic::Response<Self::ConsumeStream>, tonic::Status> {
        let cr = request.into_inner();

        let consumer_id = cr.consumer_id.clone().unwrap_or(Uuid::new_v4().to_string());
        let initial_offset_policy = match cr.initial_offset_policy() {
            yellowstone_grpc_proto::yellowstone::log::InitialOffsetPolicy::Earliest => {
                InitialOffsetPolicy::Earliest
            }
            yellowstone_grpc_proto::yellowstone::log::InitialOffsetPolicy::Latest => {
                InitialOffsetPolicy::Latest
            }
            yellowstone_grpc_proto::yellowstone::log::InitialOffsetPolicy::Slot => {
                let slot = cr.at_slot.ok_or(tonic::Status::invalid_argument(
                    "Expected at_lot when initital_offset_policy is to `Slot`",
                ))?;
                InitialOffsetPolicy::SlotApprox(slot)
            }
        };

        let event_subscription_policy = cr.event_subscription_policy();
        let account_update_event_filter = cr.account_update_event_filter;
        let tx_event_filter = cr.tx_event_filter;

        let session = Arc::clone(&self.session);
        let consumer_info = get_or_register_consumer(
            session,
            consumer_id.as_str(),
            initial_offset_policy,
            event_subscription_policy,
        )
        .await
        .map_err(|e| {
            error!("{:?}", e);
            tonic::Status::new(
                tonic::Code::Internal,
                format!("failed to get or create consumer {:?}", consumer_id),
            )
        })?;

        let req = SpawnGrpcConsumerReq {
            session: Arc::clone(&self.session),
            consumer_info,
            account_update_event_filter,
            tx_event_filter,
            buffer_capacity: None,
            offset_commit_interval: None,
        };

        let rx = spawn_grpc_consumer(req)
            .await
            .map_err(|_e| tonic::Status::internal("fail to spawn consumer"))?;

        let ret = ReceiverStream::new(rx);

        let res = Response::new(Box::pin(ret) as Self::ConsumeStream);
        Ok(res)
    }
}

struct GrpcConsumerSession {
    session: Arc<Session>,
    consumer_info: ConsumerInfo,
    sender: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
    // The interval at which we want to commit our Offset progression to Scylla
    offset_commit_interval: Duration,
    shard_iterators: Vec<ShardIterator>,
}

pub struct SpawnGrpcConsumerReq {
    pub session: Arc<Session>,
    pub consumer_info: common::ConsumerInfo,
    pub account_update_event_filter:
        Option<yellowstone_grpc_proto::yellowstone::log::AccountUpdateEventFilter>,
    pub tx_event_filter: Option<yellowstone_grpc_proto::yellowstone::log::TransactionEventFilter>,
    pub buffer_capacity: Option<usize>,
    pub offset_commit_interval: Option<Duration>,
}

pub async fn spawn_grpc_consumer(
    spawnreq: SpawnGrpcConsumerReq,
) -> anyhow::Result<mpsc::Receiver<Result<SubscribeUpdate, tonic::Status>>> {
    let session = spawnreq.session;
    let buffer_capacity = spawnreq
        .buffer_capacity
        .unwrap_or(DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY);
    let (sender, receiver) = mpsc::channel(buffer_capacity);
    //let last_committed_offsets = state.shard_offsets.clone();
    let consumer_session = Arc::clone(&session);

    let shard_filter = ShardFilter {
        tx_account_keys: spawnreq
            .tx_event_filter
            .map(|f| f.account_keys)
            .unwrap_or_default(),
        account_pubkyes: spawnreq
            .account_update_event_filter
            .as_ref()
            .map(|f| f.pubkeys.to_owned())
            .unwrap_or_default(),
        account_owners: spawnreq
            .account_update_event_filter
            .as_ref()
            .map(|f| f.owners.to_owned())
            .unwrap_or_default(),
    };

    // We pre-warm all the shard iterator before streaming any event
    let mut prewarm_set: JoinSet<anyhow::Result<ShardIterator>> = JoinSet::new();

    // if the subscription policy requires to listen to both transaction + account update we have to double the amount of shard iterator.
    let mut account_update_shard_iterators = Vec::new();
    let mut new_tx_shard_iterators = Vec::new();

    for (shard_id, ev_type, shard_offset) in spawnreq.consumer_info.shard_offsets.iter().cloned() {
        if !spawnreq
            .consumer_info
            .subscribed_blockchain_event_types
            .contains(&ev_type)
        {
            continue;
        }
        let session = Arc::clone(&session);
        let producer_id = spawnreq.consumer_info.producer_id;
        let shard_filter = shard_filter.clone();
        prewarm_set.spawn(async move {
            let mut shard_iterator = ShardIterator::new(
                session,
                producer_id,
                shard_id,
                shard_offset,
                // The ev_type will dictate if shard iterator streams account update or transaction.
                ev_type,
                Some(shard_filter),
            )
            .await?;
            shard_iterator.warm().await?;
            Ok(shard_iterator)
        });
    }

    info!(
        "Prewarming shard iterators for consumer {}...",
        spawnreq.consumer_info.consumer_id
    );
    // Wait each shard iterator to finish warming up
    while let Some(result) = prewarm_set.join_next().await {
        let shard_iterator = result??;
        match shard_iterator.event_type {
            BlockchainEventType::AccountUpdate => {
                account_update_shard_iterators.push(shard_iterator)
            }
            BlockchainEventType::NewTransaction => new_tx_shard_iterators.push(shard_iterator),
        }
    }

    let shard_iterators = interleave(account_update_shard_iterators, new_tx_shard_iterators);

    let consumer = GrpcConsumerSession::new(
        consumer_session,
        spawnreq.consumer_info,
        sender,
        spawnreq
            .offset_commit_interval
            .unwrap_or(DEFAULT_OFFSET_COMMIT_INTERVAL),
        shard_iterators,
    )
    .await?;

    tokio::spawn(async move {
        consumer
            .into_daemon()
            .await
            .expect("consumer terminated abruptly");
    });
    Ok(receiver)
}

struct UpdateShardOffsetClosure {
    session: Arc<Session>,
    consumer_id: ConsumerId,
    producer_id: ProducerId,
    update_prepared_stmt: PreparedStatement,
}

impl UpdateShardOffsetClosure {
    async fn new(
        session: Arc<Session>,
        consumer_id: ConsumerId,
        producer_id: ProducerId,
    ) -> anyhow::Result<Self> {
        let ps = session.prepare(UPDATE_CONSUMER_SHARD_OFFSET).await?;
        Ok(UpdateShardOffsetClosure {
            session,
            consumer_id,
            producer_id,
            update_prepared_stmt: ps,
        })
    }

    async fn execute(
        &self,
        old_offsets: &[(ShardId, BlockchainEventType, ShardOffset)],
        new_offsets: &[(ShardId, BlockchainEventType, ShardOffset)],
    ) -> anyhow::Result<Result<(), ShardOffset>> {
        // Since the commit offset is partitionned by consumer_id/producer_id
        // and that we using LWT, the entire batch will be atomic.
        //
        // LOGGING Batch mode is when you have a batch that span multiple partition and need some atomicity.
        // In our case, we can disable batch logging since we are batching since-partition data.
        // Apparently, this is done by default by Scylla, but we make it explicit here since the driver is not quite mature.
        let mut atomic_batch = Batch::new(BatchType::Unlogged);

        let buffer = old_offsets
            .iter()
            .zip(new_offsets.iter())
            .filter(|((_, _, old_offset), (_, _, new_offset))| old_offset < new_offset)
            .map(
                |((shard_id, event_type, old_offset), (shard_id2, event_type2, new_offset))| {
                    if shard_id != shard_id2 {
                        panic!("Misaligned consumer offset update");
                    }
                    if event_type != event_type2 {
                        panic!("Misaligned event type during offset update");
                    }
                    (
                        new_offset,
                        self.consumer_id.clone(),
                        self.producer_id,
                        shard_id,
                        event_type,
                        old_offset,
                    )
                },
            )
            .collect::<Vec<_>>();

        if buffer.is_empty() {
            return Ok(Ok(()));
        }

        repeat(())
            .take(buffer.len())
            .for_each(|_| atomic_batch.append_statement(self.update_prepared_stmt.clone()));

        let query_result = self.session.batch(&atomic_batch, &buffer).await?;

        let row = query_result.first_row().map_err(anyhow::Error::new)?;

        let success = row
            .columns
            .first() // first column of LWT is always "success" field
            .and_then(|opt| opt.to_owned())
            .map(bool::from_cql)
            .transpose()?
            .unwrap_or(false);

        let actual_offset = row
            .columns
            .get(5) // offset column
            .and_then(|opt| opt.to_owned())
            .map(ShardOffset::from_cql)
            .transpose()?;

        if success {
            Ok(Ok(()))
        } else {
            Ok(Err(actual_offset.expect("missing actual offset from LWT")))
        }
    }
}

fn interleave<IT>(it1: IT, it2: IT) -> Vec<<IT as IntoIterator>::Item>
where
    IT: IntoIterator,
{
    let mut ret = vec![];
    let mut iter1 = it1.into_iter();
    let mut iter2 = it2.into_iter();
    loop {
        match (iter1.next(), iter2.next()) {
            (Some(x), Some(y)) => {
                ret.push(x);
                ret.push(y);
            }
            (Some(x), None) => ret.push(x),
            (None, Some(y)) => ret.push(y),
            (None, None) => break,
        }
    }

    ret
}

impl GrpcConsumerSession {
    async fn new(
        session: Arc<Session>,
        consumer_info: ConsumerInfo,
        sender: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
        offset_commit_interval: Duration,
        shard_iterators: Vec<ShardIterator>,
    ) -> anyhow::Result<Self> {
        Ok(GrpcConsumerSession {
            session,
            consumer_info,
            sender,
            offset_commit_interval,
            shard_iterators,
        })
    }

    async fn into_daemon(mut self) -> anyhow::Result<()> {
        let consumer_id = self.consumer_info.consumer_id;
        let producer_id = self.consumer_info.producer_id;
        let mut commit_offset_deadline = Instant::now() + self.offset_commit_interval;
        let update_shard_offset_fn = UpdateShardOffsetClosure::new(
            Arc::clone(&self.session),
            consumer_id.clone(),
            producer_id,
        )
        .await?;

        info!("Serving consumer: {:?}", consumer_id);

        let mut last_committed_offsets = self.consumer_info.shard_offsets.clone();
        last_committed_offsets.sort_by_key(|tuple| (tuple.0, tuple.1));
        self.shard_iterators
            .sort_by_key(|it| (it.shard_id, it.event_type));

        loop {
            for shard_it in self.shard_iterators.iter_mut() {
                let maybe = shard_it.try_next().await?;
                if let Some(block_chain_event) = maybe {
                    let geyser_event = match block_chain_event.event_type {
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
                    self.sender.send(Ok(subscribe_update)).await.map_err(|_| {
                        anyhow::anyhow!("Failed to deliver message to consumer {}", consumer_id)
                    })?;
                }
            }

            // Every now and then, we commit where the consumer is loc
            if commit_offset_deadline.elapsed() > Duration::ZERO {
                let mut new_offsets_to_commit = self
                    .shard_iterators
                    .iter()
                    .map(|shard_it| {
                        (
                            shard_it.shard_id,
                            shard_it.event_type,
                            shard_it.last_offset(),
                        )
                    })
                    .collect::<Vec<_>>();

                let result = update_shard_offset_fn
                    .execute(&last_committed_offsets, &new_offsets_to_commit)
                    .await?;

                if let Err(_actual_offset_in_scylla) = result {
                    anyhow::bail!("two concurrent connections are using the same consumer instance")
                }
                info!(
                    "Successfully committed offsets for consumer {:?}",
                    consumer_id
                );
                std::mem::swap(&mut new_offsets_to_commit, &mut last_committed_offsets);
                commit_offset_deadline = Instant::now() + self.offset_commit_interval;
            }
        }
    }
}
