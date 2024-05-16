use {
    super::{
        common::{ConsumerId, ConsumerInfo, InitialOffsetPolicy},
        shard_iterator::{ShardFilter, ShardIterator},
    },
    crate::scylladb::{
        sink,
        types::{
            BlockchainEventType, ProducerId, ProducerInfo, ShardId, ShardOffset, MAX_PRODUCER,
            MIN_PROCUDER,
        },
    },
    chrono::{DateTime, TimeDelta, Utc},
    futures::{future::try_join_all, Stream},
    scylla::{
        batch::{Batch, BatchType},
        cql_to_rust::FromCqlVal,
        prepared_statement::PreparedStatement,
        transport::query_result::SingleRowTypedError,
        Session,
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        iter::repeat,
        pin::Pin,
        sync::Arc,
        time::Duration,
    },
    tokio::{sync::mpsc, time::Instant},
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

const DEFAULT_LAST_HEARTBEAT_TIME_DELTA: Duration = Duration::from_secs(10);

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

///
/// This query leverage the fact that partition data are always sorted by the clustering key and that scylla
/// always iterator or scan data in cluster order. In leyman terms that mean per partition limit will always return
/// the most recent entry for each producer_id.
pub const LIST_PRODUCER_LAST_HEARBEAT: &str = r###"
    SELECT
        producer_id,
        created_at
    FROM producer_slot_seen
    PER PARTITION LIMIT 1
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

pub const LIST_PRODUCERS_WITH_LOCK: &str = r###"
    SELECT
        producer_id
    FROM producer_lock
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
        num_shards
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
    ev_types_to_include: &[BlockchainEventType],
) -> anyhow::Result<Vec<(ShardId, BlockchainEventType, ShardOffset)>> {
    session
        .query(
            GET_SHARD_OFFSETS_FOR_CONSUMER_ID,
            (consumer_id.as_ref(), producer_id),
        )
        .await?
        .rows_typed_or_empty::<(ShardId, BlockchainEventType, ShardOffset)>()
        .filter(|result| {
            if let Ok(triplet) = result {
                ev_types_to_include.contains(&triplet.1)
            } else {
                false
            }
        })
        .collect::<Result<Vec<(ShardId, BlockchainEventType, ShardOffset)>, _>>()
        .map_err(anyhow::Error::new)
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
/// Returns a list of producer that has a lock
///
async fn list_producers_with_lock_held(session: Arc<Session>) -> anyhow::Result<Vec<ProducerId>> {
    session
        .query(LIST_PRODUCERS_WITH_LOCK, &[])
        .await?
        .rows_typed::<(ProducerId,)>()?
        .map(|result| result.map(|row| row.0))
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::new)
}

async fn list_producers_heartbeat(
    session: Arc<Session>,
    heartbeat_time_dt: Duration,
) -> anyhow::Result<Vec<ProducerId>> {
    let utc_now = Utc::now();
    let min_last_heartbeat = utc_now
        .checked_sub_signed(TimeDelta::seconds(heartbeat_time_dt.as_secs().try_into()?))
        .ok_or(anyhow::anyhow!("Invalid heartbeat time delta"))?;

    let producer_id_with_last_hb_datetime_pairs = session
        .query(LIST_PRODUCER_LAST_HEARBEAT, &[])
        .await?
        .rows_typed::<(ProducerId, DateTime<Utc>)>()?
        //.map(|result| result.map(|row| row.0))
        .collect::<Result<Vec<_>, _>>()?;
    //.map_err(anyhow::Error::new)

    Ok(producer_id_with_last_hb_datetime_pairs
        .into_iter()
        .filter(|(_, last_hb)| last_hb >= &min_last_heartbeat)
        .map(|(pid, _)| pid)
        .collect::<Vec<_>>())
}

///
/// Returns the producer id with least consumer assignment.
///
async fn get_producer_id_with_least_assigned_consumer(
    session: Arc<Session>,
) -> anyhow::Result<ProducerId> {
    let locked_producers = list_producers_with_lock_held(Arc::clone(&session)).await?;
    let recently_active_producers = BTreeSet::from_iter(
        list_producers_heartbeat(Arc::clone(&session), DEFAULT_LAST_HEARTBEAT_TIME_DELTA).await?,
    );

    let elligible_producers = locked_producers
        .into_iter()
        .filter(|producer_id| recently_active_producers.contains(producer_id))
        .collect::<BTreeSet<_>>();

    let mut producer_count_pairs = session
        .query(GET_PRODUCERS_CONSUMER_COUNT, &[])
        .await?
        .rows_typed::<(ProducerId, i32)>()?
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    elligible_producers.iter().for_each(|producer_id| {
        producer_count_pairs
            .entry(producer_id.to_owned())
            .or_insert(0);
    });

    producer_count_pairs
        .into_iter()
        .filter(|(producer_id, _)| elligible_producers.contains(producer_id))
        .min_by_key(|(_, count)| *count)
        .map(|(producer_id, _)| producer_id)
        .ok_or(anyhow::anyhow!("No producer is available right now"))
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

async fn register_new_consumer(
    session: Arc<Session>,
    consumer_id: impl AsRef<str>,
    initial_offset_policy: InitialOffsetPolicy,
    event_sub_policy: EventSubscriptionPolicy,
) -> anyhow::Result<ConsumerInfo> {
    let producer_id = get_producer_id_with_least_assigned_consumer(Arc::clone(&session)).await?;

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
    let initital_shard_offsets = set_initial_consumer_shard_offsets(
        Arc::clone(&session),
        consumer_id.as_ref(),
        producer_id,
        initial_offset_policy,
        event_sub_policy,
    )
    .await?;
    let cs = ConsumerInfo {
        consumer_id: String::from(consumer_id.as_ref()),
        producer_id,
        initital_shard_offsets,
        subscribed_blockchain_event_types: get_blockchain_event_types(event_sub_policy),
    };

    Ok(cs)
}

///
/// Gets an existing consumer with id = `consumer_id` if exists, otherwise creates a new consumer.
///
async fn get_or_register_consumer(
    session: Arc<Session>,
    consumer_id: impl AsRef<str>,
    initial_offset_policy: InitialOffsetPolicy,
    event_sub_policy: EventSubscriptionPolicy,
) -> anyhow::Result<ConsumerInfo> {
    let maybe_producer_id =
        get_producer_id_for_consumer(Arc::clone(&session), consumer_id.as_ref()).await?;

    if let Some(producer_id) = maybe_producer_id {
        info!(
            "consumer {:?} exists with producer {:?} assigned to it",
            consumer_id.as_ref(),
            producer_id
        );

        let ev_types = get_blockchain_event_types(event_sub_policy);
        let shard_offsets = get_shard_offsets_info_for_consumer_id(
            Arc::clone(&session),
            consumer_id.as_ref(),
            producer_id,
            &ev_types,
        )
        .await?;
        if shard_offsets.is_empty() {
            anyhow::bail!("Consumer state is corrupted, existing consumer should have offset already available.");
        }
        let cs = ConsumerInfo {
            consumer_id: String::from(consumer_id.as_ref()),
            producer_id,
            initital_shard_offsets: shard_offsets,
            subscribed_blockchain_event_types: ev_types,
        };
        Ok(cs)
    } else {
        register_new_consumer(
            session,
            consumer_id,
            initial_offset_policy,
            event_sub_policy,
        )
        .await
    }
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

    let shard_offset_pairs = match initial_offset_policy {
        InitialOffsetPolicy::Latest => {
            sink::get_max_shard_offsets_for_producer(
                Arc::clone(&session),
                producer_id,
                num_shards as usize,
            )
            .await?
        }
        InitialOffsetPolicy::Earliest => repeat(0)
            .take(num_shards as usize)
            .enumerate()
            .map(|(i, x)| (i as ShardId, x))
            .collect::<Vec<_>>(),
        InitialOffsetPolicy::SlotApprox(slot) => session
            .query(GET_MIN_OFFSET_FOR_SLOT, (slot, producer_id))
            .await?
            .rows_typed_or_empty::<(ShardId, ShardOffset)>()
            .collect::<Result<Vec<_>, _>>()?,
    };

    let adjustment = match initial_offset_policy {
        InitialOffsetPolicy::Earliest | InitialOffsetPolicy::SlotApprox(_) => -1,
        InitialOffsetPolicy::Latest => 0,
    };

    let insert_consumer_offset_ps: PreparedStatement =
        session.prepare(INSERT_CONSUMER_OFFSET).await?;

    let mut batch = Batch::new(BatchType::Unlogged);
    let mut buffer = Vec::with_capacity(shard_offset_pairs.len());

    let ev_types = get_blockchain_event_types(event_sub_policy);

    ev_types
        .into_iter()
        .flat_map(|ev_type| {
            shard_offset_pairs
                .iter()
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

        let req = SpawnGrpcConsumerReq {
            consumer_id,
            account_update_event_filter,
            tx_event_filter,
            buffer_capacity: None,
            offset_commit_interval: None,
        };

        let rx = spawn_grpc_consumer(
            Arc::clone(&self.session),
            req,
            initial_offset_policy,
            event_subscription_policy,
        )
        .await
        .map_err(|_e| tonic::Status::internal("fail to spawn consumer"))?;

        let ret = ReceiverStream::new(rx);

        let res = Response::new(Box::pin(ret) as Self::ConsumeStream);
        Ok(res)
    }
}

struct GrpcConsumerSource {
    session: Arc<Session>,
    consumer_info: ConsumerInfo,
    sender: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
    // The interval at which we want to commit our Offset progression to Scylla
    offset_commit_interval: Duration,
    shard_iterators: Vec<ShardIterator>,
}

pub struct SpawnGrpcConsumerReq {
    pub consumer_id: ConsumerId,
    pub account_update_event_filter:
        Option<yellowstone_grpc_proto::yellowstone::log::AccountUpdateEventFilter>,
    pub tx_event_filter: Option<yellowstone_grpc_proto::yellowstone::log::TransactionEventFilter>,
    pub buffer_capacity: Option<usize>,
    pub offset_commit_interval: Option<Duration>,
}

type GrpcConsumerReceiver = mpsc::Receiver<Result<SubscribeUpdate, tonic::Status>>;

pub async fn spawn_grpc_consumer(
    session: Arc<Session>,
    req: SpawnGrpcConsumerReq,
    initial_offset_policy: InitialOffsetPolicy,
    event_subscription_policy: EventSubscriptionPolicy,
) -> anyhow::Result<GrpcConsumerReceiver> {
    let consumer_info = get_or_register_consumer(
        Arc::clone(&session),
        req.consumer_id.as_str(),
        initial_offset_policy,
        event_subscription_policy,
    )
    .await
    .map_err(|e| {
        error!("{:?}", e);
        tonic::Status::new(
            tonic::Code::Internal,
            format!("failed to get or create consumer {:?}", req.consumer_id),
        )
    })?;
    let buffer_capacity = req
        .buffer_capacity
        .unwrap_or(DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY);
    let (sender, receiver) = mpsc::channel(buffer_capacity);
    //let last_committed_offsets = state.shard_offsets.clone();
    let consumer_session = Arc::clone(&session);

    let shard_filter = ShardFilter {
        tx_account_keys: req
            .tx_event_filter
            .map(|f| f.account_keys)
            .unwrap_or_default(),
        account_pubkyes: req
            .account_update_event_filter
            .as_ref()
            .map(|f| f.pubkeys.to_owned())
            .unwrap_or_default(),
        account_owners: req
            .account_update_event_filter
            .as_ref()
            .map(|f| f.owners.to_owned())
            .unwrap_or_default(),
    };

    let shard_iterators = try_join_all(consumer_info.initital_shard_offsets.iter().cloned().map(
        |(shard_id, ev_type, shard_offset)| {
            let session = Arc::clone(&session);
            let producer_id = consumer_info.producer_id;
            let shard_filter = shard_filter.clone();
            ShardIterator::new(
                session,
                producer_id,
                shard_id,
                shard_offset,
                // The ev_type will dictate if shard iterator streams account update or transaction.
                ev_type,
                Some(shard_filter),
            )
        },
    ))
    .await?;

    let consumer = GrpcConsumerSource::new(
        consumer_session,
        consumer_info,
        sender,
        req.offset_commit_interval
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

impl GrpcConsumerSource {
    async fn new(
        session: Arc<Session>,
        consumer_info: ConsumerInfo,
        sender: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
        offset_commit_interval: Duration,
        mut shard_iterators: Vec<ShardIterator>,
    ) -> anyhow::Result<Self> {
        // Prewarm every shard iterator
        try_join_all(shard_iterators.iter_mut().map(|shard_it| shard_it.warm())).await?;

        Ok(GrpcConsumerSource {
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

        let mut last_committed_offsets = self.consumer_info.initital_shard_offsets.clone();
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
