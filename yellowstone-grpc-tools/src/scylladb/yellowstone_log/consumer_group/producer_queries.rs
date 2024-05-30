use {
    crate::scylladb::types::{CommitmentLevel, ProducerId, ShardId, ShardOffset, Slot},
    chrono::{DateTime, TimeDelta, Utc},
    scylla::{prepared_statement::PreparedStatement, Session},
    std::{
        collections::{BTreeMap, BTreeSet},
        fmt,
        ops::RangeInclusive,
        sync::Arc,
        time::Duration,
    },
    thiserror::Error,
    tracing::info,
};

const DEFAULT_LAST_HEARTBEAT_TIME_DELTA: Duration = Duration::from_secs(10);

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

const GET_PRODUCERS_CONSUMER_COUNT: &str = r###"
    SELECT
        producer_id,
        count(1)
    FROM producer_consumer_mapping_mv
    GROUP BY producer_id
"###;

const LIST_PRODUCERS_WITH_LOCK: &str = r###"
    SELECT
        producer_id
    FROM producer_lock
    WHERE is_ready = true
    ALLOW FILTERING
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

///
/// This error is raised when no lock is held by any producer.
///
#[derive(Error, PartialEq, Eq, Debug)]
pub struct NoActiveProducer;

impl fmt::Display for NoActiveProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoActiveProducer")
    }
}

///
/// This error is raised when there is no active producer for the desired commitment level.
///
#[derive(Copy, Error, PartialEq, Eq, Debug, Clone)]
pub struct ImpossibleCommitmentLevel(CommitmentLevel);

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
pub struct ImpossibleTimelineSelection;

impl fmt::Display for ImpossibleTimelineSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ImpossibleTimelineSelection")
    }
}
///
/// This error is raised when no producer as seen the desired `slot`.
///
#[derive(Clone, Debug, Error, PartialEq, Eq, Copy)]
pub struct ImpossibleSlotOffset(pub Slot);

impl fmt::Display for ImpossibleSlotOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let slot = self.0;
        f.write_fmt(format_args!("ImpossbielInititalOffset({})", slot))
    }
}

pub struct ProducerQueries {
    session: Arc<Session>,
}

impl ProducerQueries {
    pub async fn new(session: Arc<Session>) -> anyhow::Result<Self> {
        Ok(ProducerQueries { session })
    }

    ///
    /// Returns a list of producer that has a lock
    ///
    pub async fn list_producers_with_lock_held(&self) -> anyhow::Result<Vec<ProducerId>> {
        self.session
            .query(LIST_PRODUCERS_WITH_LOCK, &[])
            .await?
            .rows_typed_or_empty::<(ProducerId,)>()
            .map(|result| result.map(|row| row.0))
            .collect::<Result<Vec<_>, _>>()
            .map_err(anyhow::Error::new)
    }

    pub async fn list_producer_with_slot(
        &self,
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

        self.session
            .query(query_template, &[])
            .await?
            .rows_typed_or_empty::<(ProducerId, Slot)>()
            .map(|result| result.map(|(producer_id, _slot)| producer_id))
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(anyhow::Error::new)
            .map(|btree_set| btree_set.into_iter().collect())
    }

    pub async fn list_producer_with_commitment_level(
        &self,
        commitment_level: CommitmentLevel,
    ) -> anyhow::Result<Vec<ProducerId>> {
        self.session
            .query(LIST_PRODUCER_WITH_COMMITMENT_LEVEL, (commitment_level,))
            .await?
            .rows_typed_or_empty::<(ProducerId,)>()
            .map(|result| result.map(|row| row.0))
            .collect::<Result<Vec<_>, _>>()
            .map_err(anyhow::Error::new)
    }

    pub async fn list_producers_heartbeat(
        &self,
        heartbeat_time_dt: Duration,
    ) -> anyhow::Result<Vec<ProducerId>> {
        let utc_now = Utc::now();
        let heartbeat_lower_bound = utc_now
            .checked_sub_signed(TimeDelta::seconds(heartbeat_time_dt.as_secs().try_into()?))
            .ok_or(anyhow::anyhow!("Invalid heartbeat time delta"))?;
        println!("heartbeat lower bound: {heartbeat_lower_bound}");
        let producer_id_with_last_hb_datetime_pairs = self
            .session
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

    ///
    /// Returns the producer id with least consumer assignment.
    ///
    pub async fn get_producer_id_with_least_assigned_consumer(
        &self,
        opt_slot_range: Option<RangeInclusive<Slot>>,
        commitment_level: CommitmentLevel,
    ) -> anyhow::Result<ProducerId> {
        let locked_producers = self.list_producers_with_lock_held().await?;
        info!("{} producer lock(s) detected", locked_producers.len());

        anyhow::ensure!(!locked_producers.is_empty(), NoActiveProducer);

        let recently_active_producers = BTreeSet::from_iter(
            self.list_producers_heartbeat(DEFAULT_LAST_HEARTBEAT_TIME_DELTA)
                .await?,
        );
        info!(
            "{} living producer(s) detected",
            recently_active_producers.len()
        );

        anyhow::ensure!(!recently_active_producers.is_empty(), NoActiveProducer);

        let producers_with_commitment_level = self
            .list_producer_with_commitment_level(commitment_level)
            .await?;
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
                self.list_producer_with_slot(*slot_range.start()..=*slot_range.end())
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

        let mut producer_count_pairs = self
            .session
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

    pub async fn get_min_offset_for_producer(
        &self,
        producer_id: ProducerId,
    ) -> anyhow::Result<Vec<(ShardId, ShardOffset, Slot)>> {
        self.session
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

    pub async fn get_slot_shard_offsets(
        &self,
        slot: Slot,
        min_slot: Slot,
        producer_id: ProducerId,
        _num_shards: ShardId,
    ) -> anyhow::Result<Option<Vec<(ShardId, ShardOffset, Slot)>>> {
        let maybe = self
            .session
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
}
