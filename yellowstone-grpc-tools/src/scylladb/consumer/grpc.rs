use {
    super::{
        common::InitialOffsetPolicy,
        shard_iterator::{ShardFilter, ShardIterator},
    }, crate::{kafka::grpc, scylladb::{
        sink, types::{
            BlockchainEventType, ConsumerId, ConsumerInfo, ConsumerShardOffset, ProducerId, ProducerInfo, ShardId, ShardOffset, ShardPeriod, Slot, MAX_PRODUCER, MIN_PROCUDER, SHARD_OFFSET_MODULO, UNDEFINED_SLOT
        }
    }}, chrono::{offset, DateTime, TimeDelta, Utc}, core::fmt, futures::{future::{self, try_join, try_join_all}, Future, FutureExt, Stream}, google_cloud_googleapis::r#type::Date, rdkafka::consumer, scylla::{
        batch::{Batch, BatchType},
        cql_to_rust::FromCqlVal,
        prepared_statement::PreparedStatement,
        transport::query_result::SingleRowTypedError,
        Session,
    }, sha2::digest::block_buffer::Block, std::{
        collections::{BTreeMap, BTreeSet}, error::Error, iter::repeat, pin::Pin, sync::Arc, time::Duration
    }, tokio::{sync::mpsc, time::Instant}, tokio_stream::wrappers::ReceiverStream, tonic::Response, tracing::{error, info, warn}, uuid::Uuid, yellowstone_grpc_proto::{
        geyser::{subscribe_update::UpdateOneof, SubscribeUpdate},
        yellowstone::log::{
            yellowstone_log_server::YellowstoneLog, ConsumeRequest, EventSubscriptionPolicy,
        },
    }
};

const CHECK_PRODUCER_LIVENESS_DELAY: Duration = Duration::from_millis(600);

const CLIENT_LAG_WARN_THRESHOLD: Duration = Duration::from_millis(250);

const FETCH_MICRO_BATCH_LATENCY_WARN_THRESHOLD: Duration = Duration::from_millis(500);

const DEFAULT_LAST_HEARTBEAT_TIME_DELTA: Duration = Duration::from_secs(10);

const DEFAULT_OFFSET_COMMIT_INTERVAL: Duration = Duration::from_secs(10);

const DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY: usize = 100;

const UPDATE_CONSUMER_SHARD_OFFSET: &str = r###"
    UPDATE consumer_shard_offset
    SET offset = ?, slot = ?, updated_at = currentTimestamp() 
    WHERE 
        consumer_id = ?
        AND producer_id = ?
        AND shard_id = ?
        AND event_type = ?
"###;

const LIST_PRODUCER_WITH_SLOT: &str = r###"
    SELECT 
        producer_id, 
        min(slot) 
    FROM slot_map_mv  
    WHERE slot = ? 
    GROUP BY producer_id
"###;

///
/// This query leverage the fact that partition data are always sorted by the clustering key and that scylla
/// always iterator or scan data in cluster order. In leyman terms that mean per partition limit will always return
/// the most recent entry for each producer_id.
const LIST_PRODUCER_LAST_HEARBEAT: &str = r###"
    SELECT
        producer_id,
        created_at
    FROM producer_slot_seen
    PER PARTITION LIMIT 1
"###;

const GET_MIN_OFFSET_FOR_SLOT: &str = r###"
    SELECT
        shard_id,
        min(offset)
    FROM slot_map_mv
    WHERE slot = ? and producer_id = ?
    GROUP BY shard_id
"###;

const INSERT_CONSUMER_OFFSET: &str = r###"
    INSERT INTO consumer_shard_offset (
        consumer_id,
        producer_id,
        shard_id,
        event_type,
        offset,
        slot,
        created_at,
        updated_at
    )
    VALUES
    (?,?,?,?,?,?, currentTimestamp(), currentTimestamp())
"###;

const GET_CONSUMER_INFO_BY_ID: &str = r###"
    SELECT
        consumer_id,
        producer_id,
        subscribed_event_types
    FROM consumer_info 
    where consumer_id = ?
"###;


const GET_SHARD_OFFSETS_FOR_CONSUMER_ID: &str = r###"
    SELECT
        consumer_id,
        producer_id,
        shard_id,
        event_type,
        offset,
        slot
    FROM consumer_shard_offset
    WHERE 
        consumer_id = ?
        AND producer_id = ?
    ORDER BY shard_id ASC
"###;

const LIST_PRODUCERS_WITH_LOCK: &str = r###"
    SELECT
        producer_id
    FROM producer_lock
"###;

const GET_PRODUCERS_CONSUMER_COUNT: &str = r###"
    SELECT
        producer_id,
        count(1)
    FROM producer_consumer_mapping_mv
    GROUP BY producer_id
"###;


const INSERT_CONSUMER_INFO: &str = r###"
    INSERT INTO consumer_info (consumer_id, producer_id, subscribed_event_types, created_at, updated)
    VALUES (?,?,?, currentTimestamp(), currentTimestamp())
"###;

const UPSERT_CONSUMER_INFO: &str = r###"
    UPDATE consumer_info
    SET producer_id = ?, 
        subscribed_event_types = ?,
        updated_at = currentTimestamp()
    WHERE consumer_id = ?
"###;

const GET_PRODUCER_INFO_BY_ID: &str = r###"
    SELECT
        producer_id,
        num_shards
    FROM producer_info
    WHERE producer_id = ?
"###;

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
struct ImpossibleSlotOffset(Slot);

impl fmt::Display for ImpossibleSlotOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let slot = self.0;
        f.write_str(format!("ImpossbielInititalOffset({slot})").as_str())
    }
}

impl Error for ImpossibleSlotOffset {}

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
struct DeadProducerErr(ProducerId);

impl Error for DeadProducerErr {}

impl fmt::Display for DeadProducerErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let producer_id = self.0[0];
        f.write_str(format!("ProducerStale({producer_id})").as_str())
    }
}

///
/// Returns the latest offset per shard for a consumer id
///
async fn get_shard_offsets_info_for_consumer(
    session: Arc<Session>,
    consumer_info: &ConsumerInfo,
) -> anyhow::Result<Vec<ConsumerShardOffset>> {
    session
        .query(
            GET_SHARD_OFFSETS_FOR_CONSUMER_ID,
            (consumer_info.consumer_id.as_str(), consumer_info.producer_id),
        )
        .await?
        .rows_typed_or_empty::<ConsumerShardOffset>()
        .filter(|result| {
            if let Ok(shard_offset) = result {
                consumer_info.subscribed_blockchain_event_types.contains(&shard_offset.event_type)
            } else {
                false
            }
        })
        .collect::<Result<Vec<ConsumerShardOffset>, _>>()
        .map_err(anyhow::Error::new)
}


///
/// Returns the assigned producer id to specific consumer if any.
///
pub async fn get_consumer_info_by_id(
    session: Arc<Session>,
    consumer_id: ConsumerId,
) -> anyhow::Result<Option<ConsumerInfo>> {
    session
        .query(GET_CONSUMER_INFO_BY_ID, (consumer_id,))
        .await?
        .maybe_first_row_typed::<ConsumerInfo>()
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

async fn list_producer_with_slot(session: Arc<Session>, slot: Slot) -> anyhow::Result<Vec<ProducerId>> {
    session
        .query(LIST_PRODUCER_WITH_SLOT, (slot,))
        .await?
        .rows_typed_or_empty::<(ProducerId, Slot)>()
        .map(|result| result.map(|(producer_id, _slot)| producer_id ))
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::new)
}

async fn list_producers_heartbeat(
    session: Arc<Session>,
    heartbeat_time_dt: Duration,
) -> anyhow::Result<Vec<ProducerId>> {
    let utc_now = Utc::now();
    let heartbeat_lower_bound = utc_now
        .checked_sub_signed(TimeDelta::seconds(heartbeat_time_dt.as_secs().try_into()?))
        .ok_or(anyhow::anyhow!("Invalid heartbeat time delta"))?;
    println!("heartbeat lower bound: {heartbeat_lower_bound}");
    let producer_id_with_last_hb_datetime_pairs = session
        .query(LIST_PRODUCER_LAST_HEARBEAT, &[])
        .await?
        .rows_typed::<(ProducerId, DateTime<Utc>)>()?
        //.map(|result| result.map(|row| row.0))
        .collect::<Result<Vec<_>, _>>()?;

    println!("{producer_id_with_last_hb_datetime_pairs:?}");
    //.map_err(anyhow::Error::new)

    Ok(producer_id_with_last_hb_datetime_pairs
        .into_iter()
        .filter(|(_, last_hb)| last_hb >= &heartbeat_lower_bound)
        .map(|(pid, _)| pid)
        .collect::<Vec<_>>())
}


async fn is_producer_still_alive(session: Arc<Session>, producer_id: ProducerId) -> anyhow::Result<bool> {
    let check_last_slot_seen = r###"
        SELECT 
            slot, 
            created_at 
        FROM producer_slot_seen 
        WHERE 
            producer_id = ? 
        ORDER BY slot DESC 
        PER PARTITION LIMIT 1
    "###;
    let heartbeat_lower_bound = Utc::now() - TimeDelta::seconds(DEFAULT_LAST_HEARTBEAT_TIME_DELTA.as_secs() as i64);
    let check_if_lock_held  = "SELECT producer_id FROM producer_lock WHERE producer_id = ?";
    let fut1 = session.query(check_last_slot_seen, (producer_id, ));
    let fut2 = session.query(check_if_lock_held, (producer_id, ));
    let (qr1, qr2) = try_join(fut1, fut2)
        .await?;
    if let Some((_slot, created_at)) = qr1.maybe_first_row_typed::<(Slot, DateTime<Utc>)>()? {
        if created_at < heartbeat_lower_bound {
            return Ok(false)
        }
    }

    Ok(qr2.rows.is_some())
}


///
/// Returns the producer id with least consumer assignment.
///
async fn get_producer_id_with_least_assigned_consumer(
    session: Arc<Session>,
    slot_requirement: Option<Slot>,
) -> anyhow::Result<ProducerId> {

    let locked_producers = list_producers_with_lock_held(Arc::clone(&session)).await?;

    info!("{} producer lock(s) detected", locked_producers.len());
    let recently_active_producers = BTreeSet::from_iter(
        list_producers_heartbeat(Arc::clone(&session), DEFAULT_LAST_HEARTBEAT_TIME_DELTA).await?,
    );

    info!(
        "{} living producer(s) detected",
        recently_active_producers.len()
    );

    let mut elligible_producers = locked_producers
        .into_iter()
        .filter(|producer_id| recently_active_producers.contains(producer_id))
        .collect::<BTreeSet<_>>();

    if elligible_producers.is_empty() {
        anyhow::bail!("No producer available at the moment");
    }

    if let Some(slot) = slot_requirement {
        let ret = BTreeSet::from_iter(
            list_producer_with_slot(Arc::clone(&session), slot).await?
        );
        info!("{} producer(s) with required slot {slot}", ret.len());
        let to_remove = elligible_producers
            .iter()
            .cloned()
            .filter(|k| !ret.contains(k))
            .collect::<Vec<_>>();
        
        for k in to_remove {
            elligible_producers.remove(&k);
        }

        if elligible_producers.is_empty() {
            return Err(anyhow::Error::new(ImpossibleSlotOffset(slot)));
        }
    };
    
    info!("{} elligible producer(s)", recently_active_producers.len());
    let mut producer_count_pairs = session
        .query(GET_PRODUCERS_CONSUMER_COUNT, &[])
        .await?
        .rows_typed::<(ProducerId, i64)>()?
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
pub async fn get_producer_info_by_id(
    session: Arc<Session>,
    producer_id: ProducerId,
) -> anyhow::Result<Option<ProducerInfo>> {
    let qr = session
        .query(
            GET_PRODUCER_INFO_BY_ID,
            (
                producer_id,
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

async fn assign_producer_to_consumer(
    session: Arc<Session>,
    consumer_id: ConsumerId,
    initial_offset_policy: InitialOffsetPolicy,
    event_sub_policy: EventSubscriptionPolicy,
) -> anyhow::Result<(ConsumerInfo, Vec<ConsumerShardOffset>)> {

    let maybe_slot_hint = if let InitialOffsetPolicy::SlotApprox(slot) = initial_offset_policy {
        Some(slot)
    } else {
        None
    };

    let producer_id = get_producer_id_with_least_assigned_consumer(Arc::clone(&session), maybe_slot_hint).await?;
    let insert_consumer_info_ps = session.prepare(UPSERT_CONSUMER_INFO).await?;
    session
        .execute(
            &insert_consumer_info_ps,
            (producer_id, get_blockchain_event_types(event_sub_policy), consumer_id.as_str()),
        )
        .await?;

    info!(
        "consumer {:?} successfully assigned producer {:?}",
        consumer_id.as_str(),
        producer_id
    );
    let initital_shard_offsets = set_initial_consumer_shard_offsets(
        Arc::clone(&session),
        consumer_id.as_str(),
        producer_id,
        initial_offset_policy,
        event_sub_policy,
    )
    .await?;
    info!("Successfully set consumer shard offsets following {initial_offset_policy:?} policy");
    let cs = ConsumerInfo {
        consumer_id: consumer_id.clone(),
        producer_id,
        subscribed_blockchain_event_types: get_blockchain_event_types(event_sub_policy),
    };

    Ok((cs, initital_shard_offsets))
}

async fn get_consumer_info_with_last_shard_offsets(session: Arc<Session>, consumer_id: ConsumerId) -> anyhow::Result<Option<(ConsumerInfo, Vec<ConsumerShardOffset>)>> {
    let maybe_consumer_info =
        get_consumer_info_by_id(Arc::clone(&session), consumer_id.clone()).await?;

    if let Some(consumer_info) = maybe_consumer_info {
        info!(
            "consumer {:?} exists with producer {:?} assigned to it",
            consumer_id.as_str(),
            consumer_info.producer_id
        );

        let shard_offsets = get_shard_offsets_info_for_consumer(
            Arc::clone(&session),
            &consumer_info,
        )
        .await?;
        if shard_offsets.is_empty() {
            anyhow::bail!("Consumer state is corrupted, existing consumer should have offset already available.");
        }
        Ok(Some((consumer_info, shard_offsets)))
    } else {
        Ok(None)
    }
}

async fn get_min_offset_for_producer(
    session: Arc<Session>, 
    producer_id: ProducerId,
    num_shards: usize,
) -> anyhow::Result<Vec<(ShardId, ShardOffset)>> {

    let shard_id_list = (0..num_shards)
        .map(|x| format!("{x}"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(r###"
    SELECT
        shard_id,
        period
    FROM producer_period_commit_log
    WHERE 
        producer_id = ? 
        AND shard_id in ({shard_id_list})
    ORDER BY period ASC
    PER PARTITION LIMIT 1
    "###);

    session
        .query(query, (producer_id,))
        .await?
        .rows_typed::<(ShardId, ShardPeriod)>()?
        .map(|result| result.map(|(shard_id, period)|
            (shard_id, (period * SHARD_OFFSET_MODULO) as ShardOffset)
        ))
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::new)
}

async fn get_slot_shard_offsets(session: Arc<Session>, slot: Slot, producer_id: ProducerId, num_shards: ShardId) -> anyhow::Result<Vec<(ShardId, ShardOffset, Slot)>> {
    let mut offsets = session
        .query(GET_MIN_OFFSET_FOR_SLOT, (slot, producer_id))
        .await?
        .rows_typed::<(ShardId, ShardOffset)>()?
        .map(|result| result.map(|(shard_id, shard_offset)| (shard_id, (shard_offset, slot))))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    
    let missing_shard_id = (0..num_shards)
        .filter(|shard_id| !offsets.contains_key(shard_id))
        .collect::<Vec<_>>();

    // TODO change the sink so each producer period commit stored the last offset of each shard aswell so we never have have missing shard offset
    let min_shard_offset = offsets
        .iter()
        .min_by_key(|(_, (shard_offset, _))| shard_offset)
        .ok_or(anyhow::anyhow!("Producer never saw slot {slot}"))
        .map(|(_, (min_shard_offset, _))| *min_shard_offset)?;

    
    // Since scylla sink works in round robin fashion, if slot first appear at shard "i" at offset "j", it is guarantee
    // that missing shard_id will see it a offset "k" >= "j".
    missing_shard_id
        .into_iter()
        .for_each(|missing_shard_id| {
            offsets
                .entry(missing_shard_id)
                .or_insert((min_shard_offset, slot));
        });

    Ok(offsets
        .into_iter()
        .map(|(shard_id, (shard_offset, slot))| (shard_id, shard_offset, slot))
        .collect::<Vec<_>>())
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
) -> anyhow::Result<Vec<ConsumerShardOffset>> {
    // Create all the shards counter
    let producer_info = get_producer_info_by_id(Arc::clone(&session), producer_id)
        .await?
        .unwrap_or_else(|| panic!("Producer Info `{:?}` must exists", producer_id));

    let new_consumer_id = new_consumer_id.as_ref();
    info!("consumer {new_consumer_id} will be assigned to producer {producer_id:?}");
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
        InitialOffsetPolicy::Earliest => {
            get_min_offset_for_producer(
                Arc::clone(&session), 
                producer_id, 
                num_shards as usize
            ).await?
            .into_iter()
            .map(|(shard_id, shard_offset)| (shard_id, shard_offset, UNDEFINED_SLOT))
            .collect::<Vec<_>>()
        }
        InitialOffsetPolicy::SlotApprox(slot) => 
            get_slot_shard_offsets(Arc::clone(&session), slot, producer_id, num_shards)
            .await?,
    };

    if shard_offset_pairs.is_empty() {
        anyhow::bail!("Producer {producer_id:?} shard offsets is incomplete {new_consumer_id}");
    }


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
                .map(move |(shard_id, offset, slot)| (ev_type, shard_id, offset, slot))
        })
        .for_each(|(ev_type, shard_id, offset, slot)| {
            let offset = offset + adjustment;
            batch.append_statement(insert_consumer_offset_ps.clone());
            buffer.push((
                new_consumer_id.to_owned(),
                producer_id,
                shard_id,
                ev_type,
                offset,
                slot,
            ));
        });

    session.batch(&batch, &buffer).await?;

    let shard_offsets = buffer
        .drain(..)
        .map(|(consumer_id, producer_id, shard_id, event_type, offset, slot)| ConsumerShardOffset {
            consumer_id,
            producer_id,
            shard_id,
            event_type,
            offset,
            slot,
        })
        .collect::<Vec<_>>();

    Ok(shard_offsets)
}

pub struct ScyllaYsLog {
    session: Arc<Session>,
}

impl ScyllaYsLog {
    pub fn new(session: Arc<Session>) -> Self {
        ScyllaYsLog { session }
    }
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

        info!(
            consumer_id = consumer_id,
            initital_offset_policy = ?initial_offset_policy,
            event_subscription_policy = ?event_subscription_policy,
        );

        let req = SpawnGrpcConsumerReq {
            consumer_id: consumer_id.clone(),
            account_update_event_filter,
            tx_event_filter,
            buffer_capacity: None,
            offset_commit_interval: None,
        };

        let result = spawn_grpc_consumer(
            Arc::clone(&self.session),
            req,
            initial_offset_policy,
            event_subscription_policy,
        )
        .await;
        
        match result {
            Ok(rx) => {
                let ret = ReceiverStream::new(rx);
                let res = Response::new(Box::pin(ret) as Self::ConsumeStream);
                Ok(res)
            },
            Err(e) => {
                error!(consumer_id=consumer_id, error = %e);
                Err(tonic::Status::internal(format!("({consumer_id})fail to spawn consumer")))
            }
        }
    }
}


#[derive(Clone)]
pub struct SpawnGrpcConsumerReq {
    pub consumer_id: ConsumerId,
    pub account_update_event_filter:
        Option<yellowstone_grpc_proto::yellowstone::log::AccountUpdateEventFilter>,
    pub tx_event_filter: Option<yellowstone_grpc_proto::yellowstone::log::TransactionEventFilter>,
    pub buffer_capacity: Option<usize>,
    pub offset_commit_interval: Option<Duration>,
}

type GrpcConsumerSender = mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>;
type GrpcConsumerReceiver = mpsc::Receiver<Result<SubscribeUpdate, tonic::Status>>;



async fn build_grpc_consumer_source(
    sender: GrpcConsumerSender,
    session: Arc<Session>,
    req: SpawnGrpcConsumerReq,
    initial_offset_policy: InitialOffsetPolicy,
    event_subscription_policy: EventSubscriptionPolicy,
) -> anyhow::Result<GrpcConsumerSource> {
    let (consumer_info, initial_shard_offsets) = assign_producer_to_consumer(
        Arc::clone(&session),
        req.consumer_id.clone(),
        initial_offset_policy,
        event_subscription_policy,
    )
    .await?;

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

    let shard_iterators = try_join_all(initial_shard_offsets.iter().cloned().map(
        |consumer_shard_offset| {
            let session = Arc::clone(&session);
            let producer_id = consumer_info.producer_id;
            let shard_filter = shard_filter.clone();
            ShardIterator::new(
                session,
                producer_id,
                consumer_shard_offset.shard_id,
                consumer_shard_offset.offset,
                // The ev_type will dictate if shard iterator streams account update or transaction.
                consumer_shard_offset.event_type,
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
    Ok(consumer)
}

pub async fn spawn_grpc_consumer(
    session: Arc<Session>,
    req: SpawnGrpcConsumerReq,
    initial_offset_policy: InitialOffsetPolicy,
    event_subscription_policy: EventSubscriptionPolicy,
) -> anyhow::Result<GrpcConsumerReceiver> {
    let original_req = req.clone();
    let buffer_capacity = req
        .buffer_capacity
        .unwrap_or(DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY);
    let (sender, receiver) = mpsc::channel(buffer_capacity);

    let mut grpc_consumer_source = build_grpc_consumer_source(
        sender.clone(), 
        Arc::clone(&session), 
        req, 
        initial_offset_policy, 
        event_subscription_policy
    ).await?;
    let consumer_id = original_req.consumer_id.to_owned();
    
    info!("Spawning consumer {consumer_id} thread");
    tokio::spawn(async move{
        let consumer_id = original_req.consumer_id.to_owned();
        let sender = sender;
        let session = session;
        while !sender.is_closed() {

            match grpc_consumer_source.run_forever().await {
                Ok(_) => break,
                Err(e) => {
                    warn!("Consumer {consumer_id} source has stop with {e:?}");
                    if let Some(DeadProducerErr(_producer_id)) = e.downcast_ref::<DeadProducerErr>() {
                        
                        let forged_offset_policy = grpc_consumer_source
                            .shard_iterators_slot
                            .into_iter()
                            .min()
                            .map(InitialOffsetPolicy::SlotApprox)
                            .unwrap_or(initial_offset_policy); 

                        grpc_consumer_source = build_grpc_consumer_source(
                            sender.clone(), 
                            Arc::clone(&session), 
                            original_req.clone(), 
                            forged_offset_policy,
                            event_subscription_policy
                        ).await
                        .expect(format!("cannot translate consumer {consumer_id}").as_str());

                    } else {
                        panic!("{e:?}")
                    }
                }
            }
        }
    });
    Ok(receiver)
}

struct GrpcConsumerSource {
    session: Arc<Session>,
    consumer_info: ConsumerInfo,
    sender: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
    // The interval at which we want to commit our Offset progression to Scylla
    offset_commit_interval: Duration,
    shard_iterators: Vec<ShardIterator>,
    shard_iterators_slot: Vec<Slot>,
    update_consumer_shard_offset_prepared_stmt: PreparedStatement,
}

impl GrpcConsumerSource {
    async fn new(
        session: Arc<Session>,
        consumer_info: ConsumerInfo,
        sender: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
        offset_commit_interval: Duration,
        mut shard_iterators: Vec<ShardIterator>,
    ) -> anyhow::Result<Self> {

        let update_consumer_shard_offset_prepared_stmt = session.prepare(UPDATE_CONSUMER_SHARD_OFFSET).await?;
        // Prewarm every shard iterator
        try_join_all(shard_iterators.iter_mut().map(|shard_it| shard_it.warm())).await?;
        let num_shard_iterators = shard_iterators.len();
        let shard_iterators_slot = vec![UNDEFINED_SLOT; num_shard_iterators];
        Ok(GrpcConsumerSource {
            session,
            consumer_info,
            sender,
            offset_commit_interval,
            shard_iterators,
            shard_iterators_slot,
            update_consumer_shard_offset_prepared_stmt,
        })
    }

    async fn update_consumer_shard_offsets(&self) -> anyhow::Result<()> {
        let mut batch = Batch::new(BatchType::Unlogged);
        let mut values = Vec::with_capacity(self.shard_iterators_slot.len());
        for (i, shard_it) in self.shard_iterators.iter().enumerate() {
            values.push((
                shard_it.last_offset(), 
                self.shard_iterators_slot[i], 
                self.consumer_info.consumer_id.to_owned(), 
                self.consumer_info.producer_id,
                shard_it.shard_id,
                shard_it.event_type
            ));
            batch.append_statement(self.update_consumer_shard_offset_prepared_stmt.clone());
        }

        self.session.batch(&batch, values).await?;
        Ok(())
    }

    async fn run_forever(&mut self) -> anyhow::Result<()> {
        let producer_id = self.consumer_info.producer_id;
        let consumer_id = self.consumer_info.consumer_id.to_owned();
        let mut commit_offset_deadline = Instant::now() + self.offset_commit_interval;

        info!("Serving consumer: {:?}", consumer_id);

        self.shard_iterators
            .sort_by_key(|it| (it.shard_id, it.event_type));

        let mut max_seen_slot = UNDEFINED_SLOT;
        let mut num_event_between_two_slots = 0;

        let mut t = Instant::now();
        let mut next_producer_live_probing = Instant::now() + CHECK_PRODUCER_LIVENESS_DELAY;
        let mut producer_is_dead = false;
        loop {
            for (i, shard_it) in self.shard_iterators.iter_mut().enumerate() {
                let maybe = shard_it.try_next().await?;
                if let Some(block_chain_event) = maybe {
                    self.shard_iterators_slot[i] = block_chain_event.slot;
                    if t.elapsed() >= FETCH_MICRO_BATCH_LATENCY_WARN_THRESHOLD {
                        warn!(
                            "consumer {consumer_id} micro batch took {:?} to fetch.",
                            t.elapsed()
                        );
                    }
                    if max_seen_slot < block_chain_event.slot {
                        info!("Consumer {consumer_id} reach slot {max_seen_slot} after {num_event_between_two_slots} blockchain event(s)");
                        max_seen_slot = block_chain_event.slot;
                        num_event_between_two_slots = 0;
                    }
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
                    let t_send = Instant::now();

                    if self.sender.send(Ok(subscribe_update)).await.is_err() {
                        warn!("Consumer {consumer_id} closed its streaming half");
                        return Ok(());
                    }
                    let send_latency = t_send.elapsed();
                    if send_latency >= CLIENT_LAG_WARN_THRESHOLD {
                        warn!("Slow read from consumer {consumer_id}, recorded latency: {send_latency:?}")
                    }
                    num_event_between_two_slots += 1;
                    t = Instant::now();
                }
            }

            if next_producer_live_probing.elapsed() > Duration::ZERO {
                producer_is_dead = !is_producer_still_alive(Arc::clone(&self.session), self.consumer_info.producer_id).await?;
                if !producer_is_dead {
                    info!("producer {producer_id:?} is alive");
                }
                next_producer_live_probing = Instant::now() + CHECK_PRODUCER_LIVENESS_DELAY;
            }
            
            // Every now and then, we commit where the consumer is loc
            if commit_offset_deadline.elapsed() > Duration::ZERO || producer_is_dead {
                let t = Instant::now();
                self.update_consumer_shard_offsets().await?;
                info!("updated consumer shard offset in {:?}", t.elapsed());
                commit_offset_deadline = Instant::now() + self.offset_commit_interval;
            }

            if producer_is_dead {
                warn!("Producer {producer_id:?} is considered dead");
                return Err(anyhow::Error::new(DeadProducerErr(producer_id)));
            }
        }
    }
}
