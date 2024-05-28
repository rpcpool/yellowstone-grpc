use {
    super::{
        common::InitialOffset,
        consumer_group::repo::ConsumerGroupRepo,
        consumer_source::{ConsumerSource, FromBlockchainEvent},
        shard_iterator::{ShardFilter, ShardIterator},
    },
    crate::scylladb::{
        sink,
        types::{
            BlockchainEventType, CommitmentLevel, ConsumerId, ConsumerInfo, ConsumerShardOffset,
            ProducerId, ProducerInfo, ShardId, ShardOffset, Slot,
        },
        yellowstone_log::consumer_source::Interrupted,
    },
    chrono::{DateTime, TimeDelta, Utc},
    core::fmt,
    futures::{
        future::{try_join, try_join_all},
        Stream,
    },
    scylla::{
        batch::{Batch, BatchType},
        prepared_statement::PreparedStatement,
        transport::query_result::SingleRowTypedError,
        Session,
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        net::IpAddr,
        ops::RangeInclusive,
        pin::Pin,
        sync::Arc,
        time::Duration,
    },
    thiserror::Error,
    tokio::sync::{mpsc, oneshot},
    tokio_stream::wrappers::ReceiverStream,
    tonic::Response,
    tracing::{error, info, warn},
    uuid::Uuid,
    yellowstone_grpc_proto::{
        geyser::{subscribe_update::UpdateOneof, SubscribeUpdate},
        yellowstone::log::{
            yellowstone_log_server::YellowstoneLog, ConsumeRequest,
            CreateStaticConsumerGroupRequest, CreateStaticConsumerGroupResponse,
            EventSubscriptionPolicy, TimelineTranslationPolicy,
        },
    },
};

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

const LIST_PRODUCER_WITH_COMMITMENT_LEVEL: &str = r###"
    SELECT 
        producer_id
    FROM producer_info
    WHERE commitment_level = ?
    ALLOW FILTERING
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

const GET_SHARD_OFFSET_AT_SLOT_APPROX: &str = r###"
    SELECT
        shard_offset_map,
        slot
    FROM producer_slot_seen
    where 
        producer_id = ?
        AND slot <= ? 
        AND slot >= ?
    ORDER BY slot desc
    LIMIT 1;
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

const LIST_PRODUCERS_WITH_LOCK: &str = r###"
    SELECT
        producer_id
    FROM producer_lock
    WHERE is_ready = true
    ALLOW FILTERING
"###;

const GET_PRODUCERS_CONSUMER_COUNT: &str = r###"
    SELECT
        producer_id,
        count(1)
    FROM producer_consumer_mapping_mv
    GROUP BY producer_id
"###;

const INSERT_CONSUMER_INFO: &str = r###"
    INSERT INTO consumer_info (consumer_id, producer_id, consumer_ip, subscribed_event_types, created_at, updated_at)
    VALUES (?, ?, ?, ?, currentTimestamp(), currentTimestamp())
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
        num_shards,
        commitment_level
    FROM producer_info
    WHERE producer_id = ?
"###;

///
/// This error is raised when no lock is held by any producer.
///
#[derive(Error, PartialEq, Eq, Debug)]
struct NoActiveProducer;

impl fmt::Display for NoActiveProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoActiveProducer")
    }
}

///
/// This error is raised when there is no active producer for the desired commitment level.
///
#[derive(Copy, Error, PartialEq, Eq, Debug, Clone)]
struct ImpossibleCommitmentLevel(CommitmentLevel);

impl fmt::Display for ImpossibleCommitmentLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cl = self.0;
        f.write_fmt(format_args!("ImpossibleCommitmentLevel({})", cl))
    }
}

///
/// This error is raised when the combination of consumer critera result in an empty set of elligible producer timeline.
///
#[derive(Error, PartialEq, Eq, Debug)]
struct ImpossibleTimelineSelection;

impl fmt::Display for ImpossibleTimelineSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImpossibleTimelineSelection")
    }
}
///
/// This error is raised when no producer as seen the desired `slot`.
///
#[derive(Clone, Debug, Error, PartialEq, Eq, Copy)]
struct ImpossibleSlotOffset(Slot);

impl fmt::Display for ImpossibleSlotOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let slot = self.0;
        f.write_fmt(format_args!("ImpossbielInititalOffset({})", slot))
    }
}

#[derive(Clone, Debug, PartialEq, Error, Eq, Copy)]
struct DeadProducerErr(ProducerId);

impl fmt::Display for DeadProducerErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let producer_id = self.0[0];
        f.write_fmt(format_args!("ProducerStale({})", producer_id))
    }
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
        .rows_typed_or_empty::<(ProducerId,)>()
        .map(|result| result.map(|row| row.0))
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::new)
}

async fn list_producer_with_slot(
    session: Arc<Session>,
    slot_range: RangeInclusive<Slot>,
) -> anyhow::Result<Vec<ProducerId>> {
    let slot_values = slot_range
        .map(|slot| format!("{slot}"))
        .collect::<Vec<_>>()
        .join(", ");

    let query_template = format!(
        r###"
        SELECT 
            producer_id,
            slot
        FROM slot_producer_seen_mv  
        WHERE slot IN ({slot_values})
    "###
    );
    info!("query {query_template}");

    session
        .query(query_template, &[])
        .await?
        .rows_typed_or_empty::<(ProducerId, Slot)>()
        .map(|result| result.map(|(producer_id, _slot)| producer_id))
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(anyhow::Error::new)
        .map(|btree_set| btree_set.into_iter().collect())
}

async fn list_producer_with_commitment_level(
    session: Arc<Session>,
    commitment_level: CommitmentLevel,
) -> anyhow::Result<Vec<ProducerId>> {
    session
        .query(LIST_PRODUCER_WITH_COMMITMENT_LEVEL, (commitment_level,))
        .await?
        .rows_typed_or_empty::<(ProducerId,)>()
        .map(|result| result.map(|row| row.0))
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

async fn is_producer_still_alive(
    session: Arc<Session>,
    producer_id: ProducerId,
) -> anyhow::Result<bool> {
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
    let heartbeat_lower_bound =
        Utc::now() - TimeDelta::seconds(DEFAULT_LAST_HEARTBEAT_TIME_DELTA.as_secs() as i64);
    let check_if_lock_held = "SELECT producer_id FROM producer_lock WHERE producer_id = ?";
    let fut1 = session.query(check_last_slot_seen, (producer_id,));
    let fut2 = session.query(check_if_lock_held, (producer_id,));
    let (qr1, qr2) = try_join(fut1, fut2).await?;
    if let Some((_slot, created_at)) = qr1.maybe_first_row_typed::<(Slot, DateTime<Utc>)>()? {
        if created_at < heartbeat_lower_bound {
            return Ok(false);
        }
    }

    Ok(qr2.rows.is_some())
}

fn wait_for_producer_is_dead(
    session: Arc<Session>,
    producer_id: ProducerId,
) -> oneshot::Receiver<()> {
    let (sender, receiver) = oneshot::channel();

    tokio::spawn(async move {
        let session = session;
        loop {
            let is_alive = is_producer_still_alive(Arc::clone(&session), producer_id)
                .await
                .expect("checking producer is alive failed");
            if !is_alive {
                info!("producer {producer_id:?} is dead");
                sender
                    .send(())
                    .expect(format!("the receiveing half closed while waiting for producer({producer_id:?}) liveness status").as_str());
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    receiver
}

///
/// Returns the producer id with least consumer assignment.
///
async fn get_producer_id_with_least_assigned_consumer(
    session: Arc<Session>,
    opt_slot_range: Option<RangeInclusive<Slot>>,
    commitment_level: CommitmentLevel,
) -> anyhow::Result<ProducerId> {
    let locked_producers = list_producers_with_lock_held(Arc::clone(&session)).await?;
    info!("{} producer lock(s) detected", locked_producers.len());

    anyhow::ensure!(!locked_producers.is_empty(), NoActiveProducer);

    let recently_active_producers = BTreeSet::from_iter(
        list_producers_heartbeat(Arc::clone(&session), DEFAULT_LAST_HEARTBEAT_TIME_DELTA).await?,
    );
    info!(
        "{} living producer(s) detected",
        recently_active_producers.len()
    );

    anyhow::ensure!(!recently_active_producers.is_empty(), NoActiveProducer);

    let producers_with_commitment_level =
        list_producer_with_commitment_level(Arc::clone(&session), commitment_level).await?;
    info!(
        "{} producer(s) with {commitment_level:?} commitment level",
        producers_with_commitment_level.len()
    );

    if producers_with_commitment_level.is_empty() {
        anyhow::bail!(ImpossibleCommitmentLevel(commitment_level))
    }

    let mut elligible_producers = locked_producers
        .into_iter()
        .filter(|producer_id| recently_active_producers.contains(producer_id))
        .collect::<BTreeSet<_>>();

    anyhow::ensure!(!elligible_producers.is_empty(), ImpossibleTimelineSelection);

    if let Some(slot_range) = opt_slot_range {
        info!("Producer needs slot in {slot_range:?}");
        let producers_with_slot = BTreeSet::from_iter(
            list_producer_with_slot(
                Arc::clone(&session),
                *slot_range.start()..=*slot_range.end(),
            )
            .await?,
        );
        info!(
            "{} producer(s) with required slot range: {slot_range:?}",
            producers_with_slot.len()
        );

        elligible_producers.retain(|k| producers_with_slot.contains(k));

        anyhow::ensure!(
            !elligible_producers.is_empty(),
            ImpossibleSlotOffset(*slot_range.end())
        );
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
        .query(GET_PRODUCER_INFO_BY_ID, (producer_id,))
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
    consumer_ip: Option<IpAddr>,
    initial_offset: InitialOffset,
    event_sub_policy: EventSubscriptionPolicy,
    commitment_level: CommitmentLevel,
    is_new: bool,
) -> anyhow::Result<(ConsumerInfo, Vec<ConsumerShardOffset>)> {
    let maybe_slot_range = if let InitialOffset::SlotApprox {
        desired_slot,
        min_slot,
    } = initial_offset
    {
        Some(min_slot..=desired_slot)
    } else {
        None
    };

    let producer_id = get_producer_id_with_least_assigned_consumer(
        Arc::clone(&session),
        maybe_slot_range,
        commitment_level,
    )
    .await?;
    if is_new {
        session
            .query(
                INSERT_CONSUMER_INFO,
                (
                    consumer_id.as_str(),
                    producer_id,
                    consumer_ip.map(|ipaddr| ipaddr.to_string()),
                    get_blockchain_event_types(event_sub_policy),
                ),
            )
            .await?;
    } else {
        session
            .query(
                UPSERT_CONSUMER_INFO,
                (
                    producer_id,
                    get_blockchain_event_types(event_sub_policy),
                    consumer_id.as_str(),
                ),
            )
            .await?;
    }

    info!(
        "consumer {:?} successfully assigned producer {:?}",
        consumer_id.as_str(),
        producer_id
    );
    let initital_shard_offsets = set_initial_consumer_shard_offsets(
        Arc::clone(&session),
        consumer_id.as_str(),
        producer_id,
        initial_offset,
        event_sub_policy,
    )
    .await?;
    info!("Successfully set consumer shard offsets following {initial_offset:?} policy");
    let cs = ConsumerInfo {
        consumer_id: consumer_id.clone(),
        producer_id,
        subscribed_blockchain_event_types: get_blockchain_event_types(event_sub_policy),
    };

    Ok((cs, initital_shard_offsets))
}

async fn get_min_offset_for_producer(
    session: Arc<Session>,
    producer_id: ProducerId,
) -> anyhow::Result<Vec<(ShardId, ShardOffset, Slot)>> {
    session
        .query(
            "SELECT minimum_shard_offset FROM producer_lock WHERE producer_id = ?",
            (producer_id,),
        )
        .await?
        .first_row_typed::<(Option<Vec<(ShardId, ShardOffset, Slot)>>,)>()?
        .0
        .ok_or(anyhow::anyhow!(
            "Producer lock exists, but its minimum shard offset is not set."
        ))
}

async fn get_slot_shard_offsets(
    session: Arc<Session>,
    slot: Slot,
    min_slot: Slot,
    producer_id: ProducerId,
    _num_shards: ShardId,
) -> anyhow::Result<Option<Vec<(ShardId, ShardOffset, Slot)>>> {
    let maybe = session
        .query(
            GET_SHARD_OFFSET_AT_SLOT_APPROX,
            (producer_id, slot, min_slot),
        )
        .await?
        .maybe_first_row_typed::<(Vec<(ShardId, ShardOffset)>, Slot)>()?;

    if let Some((offsets, slot_approx)) = maybe {
        info!(
            "found producer({producer_id:?}) shard offsets within slot range: {min_slot}..={slot}"
        );
        Ok(Some(
            offsets
                .into_iter()
                .map(|(shard_id, shard_offset)| (shard_id, shard_offset, slot_approx))
                .collect(),
        ))
    } else {
        Ok(None)
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
    initial_offset_policy: InitialOffset,
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
        InitialOffset::Latest => {
            sink::get_max_shard_offsets_for_producer(
                Arc::clone(&session),
                producer_id,
                num_shards as usize,
            )
            .await?
        }
        InitialOffset::Earliest => {
            get_min_offset_for_producer(Arc::clone(&session), producer_id).await?
        }
        InitialOffset::SlotApprox {
            desired_slot,
            min_slot,
        } => {
            let minium_producer_offsets =
                get_min_offset_for_producer(Arc::clone(&session), producer_id)
                    .await?
                    .into_iter()
                    .map(|(shard_id, shard_offset, slot)| (shard_id, (shard_offset, slot)))
                    .collect::<BTreeMap<_, _>>();
            info!("(consumer-id={new_consumer_id}) SlotApprox step 1: retrieved minimum producer({producer_id:?}) offset.");

            let shard_offsets_contain_slot = get_slot_shard_offsets(
                Arc::clone(&session),
                desired_slot,
                min_slot,
                producer_id,
                num_shards,
            )
            .await?
            .ok_or(ImpossibleSlotOffset(desired_slot))?;

            info!("(consumer-id={new_consumer_id}) SlotApprox step 2: producer({producer_id:?}) shard offsets containing slot range.");

            let are_shard_offset_reachable =
                shard_offsets_contain_slot
                    .iter()
                    .all(|(shard_id, offset1, _)| {
                        minium_producer_offsets
                            .get(shard_id)
                            .filter(|(offset2, _)| offset1 > offset2)
                            .is_some()
                    });

            info!("(consumer-id={new_consumer_id}) SlotApprox step 3: producer({producer_id:?}) shard offset reachability: {are_shard_offset_reachable}");
            if !are_shard_offset_reachable {
                anyhow::bail!(ImpossibleSlotOffset(desired_slot))
            }

            shard_offsets_contain_slot
        }
    };

    if shard_offset_pairs.len() != (num_shards as usize) {
        anyhow::bail!("Producer {producer_id:?} shard offsets is incomplete {new_consumer_id}");
    }
    info!("Shard offset has been computed successfully");
    let adjustment = match initial_offset_policy {
        InitialOffset::Earliest
        | InitialOffset::SlotApprox {
            desired_slot: _,
            min_slot: _,
        } => -1,
        InitialOffset::Latest => 0,
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
        .map(
            |(consumer_id, producer_id, shard_id, event_type, offset, slot)| ConsumerShardOffset {
                consumer_id,
                producer_id,
                shard_id,
                event_type,
                offset,
                slot,
            },
        )
        .collect::<Vec<_>>();

    Ok(shard_offsets)
}

pub struct ScyllaYsLog {
    session: Arc<Session>,
    consumer_group_repo: ConsumerGroupRepo,
}

impl ScyllaYsLog {
    pub async fn new(session: Arc<Session>) -> anyhow::Result<Self> {
        let consumer_group_repo = ConsumerGroupRepo::new(Arc::clone(&session)).await?;
        Ok(ScyllaYsLog {
            session,
            consumer_group_repo,
        })
    }
}

pub type LogStream = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl YellowstoneLog for ScyllaYsLog {
    #[doc = r" Server streaming response type for the consume method."]
    type ConsumeStream = LogStream;

    async fn create_static_consumer_group(
        &self,
        request: tonic::Request<CreateStaticConsumerGroupRequest>,
    ) -> Result<tonic::Response<CreateStaticConsumerGroupResponse>, tonic::Status> {
        let remote_ip_addr = request.remote_addr().map(|addr| addr.ip());
        let request = request.into_inner();

        let instance_ids = request.instance_id_list;
        let redundant_instance_ids = request.redundancy_instance_id_list;

        let consumer_group_info = self
            .consumer_group_repo
            .create_static_consumer_group(&instance_ids, &redundant_instance_ids, remote_ip_addr)
            .await
            .map_err(|e| {
                error!("create_static_consumer_group: {e:?}");
                tonic::Status::internal("failed to create consumer group")
            })?;
        Ok(Response::new(CreateStaticConsumerGroupResponse {
            group_id: consumer_group_info.consumer_group_id.to_string(),
        }))
    }

    async fn consume(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<tonic::Response<Self::ConsumeStream>, tonic::Status> {
        let consumer_ip = request.remote_addr().map(|addr| addr.ip());
        let cr = request.into_inner();

        let consumer_id = cr.consumer_id.clone().unwrap_or(Uuid::new_v4().to_string());
        let initial_offset_policy = match cr.initial_offset_policy() {
            yellowstone_grpc_proto::yellowstone::log::InitialOffsetPolicy::Earliest => {
                InitialOffset::Earliest
            }
            yellowstone_grpc_proto::yellowstone::log::InitialOffsetPolicy::Latest => {
                InitialOffset::Latest
            }
            yellowstone_grpc_proto::yellowstone::log::InitialOffsetPolicy::Slot => {
                let slot = cr.at_slot.ok_or(tonic::Status::invalid_argument(
                    "Expected at_lot when initital_offset_policy is to `Slot`",
                ))?;
                InitialOffset::SlotApprox {
                    desired_slot: slot,
                    min_slot: slot,
                }
            }
        };

        let timeline_translation_policy = cr.timeline_translation_policy();

        let event_subscription_policy = cr.event_subscription_policy();
        let account_update_event_filter = cr.account_update_event_filter;
        let tx_event_filter = cr.tx_event_filter;
        let commitment_level: CommitmentLevel = (cr.commitment_level as i16)
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument("commitment level is invalid"))?;

        info!(
            consumer_id = consumer_id,
            initital_offset_policy = ?initial_offset_policy,
            event_subscription_policy = ?event_subscription_policy,
            commitment_level = ?commitment_level,
        );

        let req: SpawnGrpcConsumerReq = SpawnGrpcConsumerReq {
            consumer_id: consumer_id.clone(),
            consumer_ip,
            account_update_event_filter,
            tx_event_filter,
            buffer_capacity: None,
            offset_commit_interval: None,
            timeline_translation_policy,
            timeline_translation_allowed_lag: cr.ttp_maximum_slot_lag,
            event_subscription_policy,
            commitment_level,
        };

        let result =
            spawn_grpc_consumer(Arc::clone(&self.session), req, initial_offset_policy).await;

        match result {
            Ok(rx) => {
                let ret = ReceiverStream::new(rx);
                let res = Response::new(Box::pin(ret) as Self::ConsumeStream);
                Ok(res)
            }
            Err(e) => {
                error!(consumer_id=consumer_id, error = %e);
                Err(tonic::Status::internal(format!(
                    "({consumer_id}) fail to spawn consumer"
                )))
            }
        }
    }
}

#[derive(Clone)]
pub struct SpawnGrpcConsumerReq {
    pub consumer_id: ConsumerId,
    pub consumer_ip: Option<IpAddr>,
    pub account_update_event_filter:
        Option<yellowstone_grpc_proto::yellowstone::log::AccountUpdateEventFilter>,
    pub tx_event_filter: Option<yellowstone_grpc_proto::yellowstone::log::TransactionEventFilter>,
    pub buffer_capacity: Option<usize>,
    pub offset_commit_interval: Option<Duration>,
    pub timeline_translation_policy: TimelineTranslationPolicy,
    pub timeline_translation_allowed_lag: Option<u32>,
    pub event_subscription_policy: EventSubscriptionPolicy,
    pub commitment_level: CommitmentLevel,
}

type GrpcConsumerSender = mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>;
type GrpcConsumerReceiver = mpsc::Receiver<Result<SubscribeUpdate, tonic::Status>>;
type GrpcEvent = Result<SubscribeUpdate, tonic::Status>;

impl FromBlockchainEvent for GrpcEvent {
    type Output = Self;
    fn from(blockchain_event: crate::scylladb::types::BlockchainEvent) -> Self::Output {
        let geyser_event = match blockchain_event.event_type {
            BlockchainEventType::AccountUpdate => {
                UpdateOneof::Account(blockchain_event.try_into().map_err(|e| {
                    error!(error=?e);
                    tonic::Status::internal("corrupted account update event in the stream")
                })?)
            }
            BlockchainEventType::NewTransaction => {
                UpdateOneof::Transaction(blockchain_event.try_into().map_err(|e| {
                    error!(error=?e);
                    tonic::Status::internal("corrupted new transaction event in the stream")
                })?)
            }
        };
        let subscribe_update = SubscribeUpdate {
            filters: Default::default(),
            update_oneof: Some(geyser_event),
        };

        Ok(subscribe_update)
    }
}

async fn build_grpc_consumer_source(
    sender: GrpcConsumerSender,
    session: Arc<Session>,
    req: SpawnGrpcConsumerReq,
    initial_offset_policy: InitialOffset,
    is_new: bool,
) -> anyhow::Result<ConsumerSource<GrpcEvent>> {
    let (consumer_info, initial_shard_offsets) = assign_producer_to_consumer(
        Arc::clone(&session),
        req.consumer_id.clone(),
        req.consumer_ip,
        initial_offset_policy,
        req.event_subscription_policy,
        req.commitment_level,
        is_new,
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

    let consumer = ConsumerSource::new(
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
    initial_offset_policy: InitialOffset,
) -> anyhow::Result<GrpcConsumerReceiver> {
    let original_req = req.clone();
    let buffer_capacity = req
        .buffer_capacity
        .unwrap_or(DEFAULT_CONSUMER_STREAM_BUFFER_CAPACITY);
    let (sender, receiver) = mpsc::channel(buffer_capacity);
    const DEFAULT_ALLOWED_LAG: u32 = 10;
    let mut grpc_consumer_source = build_grpc_consumer_source(
        sender.clone(),
        Arc::clone(&session),
        req,
        initial_offset_policy,
        true,
    )
    .await?;
    let consumer_id = original_req.consumer_id.to_owned();

    info!("Spawning consumer {consumer_id} thread");
    tokio::spawn(async move {
        let consumer_id = original_req.consumer_id.to_owned();
        let sender = sender;
        let session = session;
        while !sender.is_closed() {
            let current_producer_id = grpc_consumer_source.producer_id();
            let interrupt_signal =
                wait_for_producer_is_dead(Arc::clone(&session), current_producer_id);

            match grpc_consumer_source.run(interrupt_signal).await {
                Ok(_) => break,
                Err(e) => {
                    warn!("Consumer {consumer_id} source has stop with {e:?}");
                    if let Some(Interrupted) = e.downcast_ref::<Interrupted>() {
                        let forged_offset_policy = grpc_consumer_source
                            .shard_iterators_slot
                            .into_iter()
                            .min()
                            .map(|(_shard_id, slot)| {
                                let min_slot = match &original_req.timeline_translation_policy {
                                    TimelineTranslationPolicy::AllowLag => {
                                        let lag = original_req
                                            .timeline_translation_allowed_lag
                                            .unwrap_or(DEFAULT_ALLOWED_LAG);
                                        slot - (lag as Slot)
                                    }
                                    TimelineTranslationPolicy::StrictSlot => slot,
                                };

                                InitialOffset::SlotApprox {
                                    desired_slot: slot,
                                    min_slot,
                                }
                            })
                            .unwrap_or(initial_offset_policy);

                        grpc_consumer_source = build_grpc_consumer_source(
                            sender.clone(),
                            Arc::clone(&session),
                            original_req.clone(),
                            forged_offset_policy,
                            false,
                        )
                        .await
                        .unwrap_or_else(|_| panic!("cannot translate consumer {consumer_id}"));
                    } else {
                        panic!("{e:?}")
                    }
                }
            }
        }
    });
    Ok(receiver)
}
