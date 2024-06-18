use {
    super::{
        error::ImpossibleSlotOffset,
        etcd_path::{
            get_producer_id_from_lock_key_v1, get_producer_lock_path_v1,
            get_producer_lock_prefix_v1,
        },
    },
    crate::scylladb::{
        sink,
        types::{
            CommitmentLevel, ExecutionId, ProducerExecutionInfo, ProducerId, ProducerInfo, ShardId,
            ShardOffset, Slot,
        },
        yellowstone_log::{
            common::SeekLocation,
            consumer_group::error::{
                ImpossibleCommitmentLevel, ImpossibleTimelineSelection, NoActiveProducer,
                StaleRevision,
            },
        },
    },
    chrono::{DateTime, TimeDelta, Utc},
    etcd_client::{GetOptions, WatchOptions},
    futures::{future::BoxFuture, Future, FutureExt},
    rdkafka::producer::Producer,
    scylla::{prepared_statement::PreparedStatement, statement::Consistency, Session},
    std::{
        collections::{BTreeMap, BTreeSet},
        ops::RangeInclusive,
        sync::Arc,
        time::Duration,
    },
    tokio::sync::oneshot,
    tokio_stream::StreamExt,
    tonic::async_trait,
    tracing::{info, trace, warn},
};

const DEFAULT_LAST_HEARTBEAT_TIME_DELTA: Duration = Duration::from_secs(10);

const GET_SHARD_OFFSET_AT_SLOT_APPROX: &str = r###"
    SELECT
        revision,
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

const LIST_PRODUCER_LOCKS: &str = r###"
    SELECT
        producer_id,
        execution_id,
        revision,
        ipv4,
        minimum_shard_offset
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

const GET_PRODUCER_INFO_BY_ID: &str = r###"
    SELECT 
        producer_id,
        commitment_level,
        num_shards
    FROM producer_info
    WHERE producer_id = ?
"###;

const GET_MIN_PRODUCER_OFFSET: &str = r###"
    SELECT
        revision,
        minimum_shard_offset 
    FROM producer_lock 
    WHERE producer_id = ?
"###;

const GET_PRODUCER_EXECUTION_ID: &str = r###"
    SELECT
        revision,
        execution_id
    FROM producer_lock
    WHERE producer_id = ?
    PER PARTITION LIMIT 1
"###;

const LIST_PRODUCER_WITH_SLOT: &str = r###"
    SELECT 
        producer_id,
        slot
    FROM slot_producer_seen_mv  
    WHERE slot IN ?
"###;

#[derive(Clone)]
pub struct ScyllaProducerStore {
    session: Arc<Session>,
    get_producer_by_id_ps: PreparedStatement,
    list_producer_locks_ps: PreparedStatement,
    get_shard_offset_in_slot_range_ps: PreparedStatement,
    get_min_producer_offset_ps: PreparedStatement,
    get_producer_execution_id_ps: PreparedStatement,
    list_producer_with_slot_ps: PreparedStatement,
    producer_monitor: Arc<dyn ProducerMonitor>,
}

impl ScyllaProducerStore {
    pub async fn new(
        session: Arc<Session>,
        producer_monitor: Arc<dyn ProducerMonitor>,
    ) -> anyhow::Result<Self> {
        let mut get_producer_by_id_ps = session.prepare(GET_PRODUCER_INFO_BY_ID).await?;
        get_producer_by_id_ps.set_consistency(Consistency::Serial);

        let list_producer_locks_ps = session.prepare(LIST_PRODUCER_LOCKS).await?;

        let mut get_shard_offset_in_slot_range_ps =
            session.prepare(GET_SHARD_OFFSET_AT_SLOT_APPROX).await?;
        get_shard_offset_in_slot_range_ps.set_consistency(Consistency::Serial);

        let mut get_min_producer_offset_ps = session.prepare(GET_MIN_PRODUCER_OFFSET).await?;
        get_min_producer_offset_ps.set_consistency(Consistency::Serial);

        let mut get_producer_execution_id_ps = session.prepare(GET_PRODUCER_EXECUTION_ID).await?;
        get_producer_execution_id_ps.set_consistency(Consistency::Serial);

        let list_producer_with_slot_ps = session.prepare(LIST_PRODUCER_WITH_SLOT).await?;

        Ok(ScyllaProducerStore {
            session,
            producer_monitor,
            get_producer_by_id_ps,
            list_producer_locks_ps,
            get_shard_offset_in_slot_range_ps,
            get_min_producer_offset_ps,
            get_producer_execution_id_ps,
            list_producer_with_slot_ps,
        })
    }

    pub async fn get_producer_info(
        &self,
        producer_id: ProducerId,
    ) -> anyhow::Result<Option<ProducerInfo>> {
        self.session
            .execute(&self.get_producer_by_id_ps, (producer_id,))
            .await?
            .maybe_first_row_typed::<ProducerInfo>()
            .map_err(anyhow::Error::new)
    }

    pub async fn list_producer_locks(
        &self,
    ) -> anyhow::Result<BTreeMap<ProducerId, ProducerExecutionInfo>> {
        trace!("list_producer_locks");
        self.session
            .execute(&self.list_producer_locks_ps, &[])
            .await?
            .rows_typed_or_empty::<ProducerExecutionInfo>()
            .map(|result| result.map(|pl| ([pl.producer_id[0]], pl)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map_err(anyhow::Error::new)
    }

    pub async fn list_producer_with_slot(
        &self,
        slot_range: RangeInclusive<Slot>,
    ) -> anyhow::Result<Vec<ProducerId>> {
        let slot_values = slot_range
            .collect::<Vec<_>>();

        self.session
            .execute(&self.list_producer_with_slot_ps, (slot_values,))
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
    ) -> anyhow::Result<(ProducerId, ExecutionId)> {
        let mut living_producers = self.producer_monitor.list_living_producers().await;
        info!("{} producer lock(s) detected", living_producers.len());
        let producer_exec_info_map = self.list_producer_locks().await?;
        anyhow::ensure!(!living_producers.is_empty(), NoActiveProducer);

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

        let mut elligible_producers = producers_with_commitment_level
            .into_iter()
            .filter_map(|producer_id| {
                living_producers.remove(&producer_id).and_then(|_revision| {
                    producer_exec_info_map
                        .get(&producer_id)
                        .map(|producer_exec_info| {
                            (producer_id, producer_exec_info.execution_id.clone())
                        })
                })
            })
            .collect::<BTreeMap<_, _>>();

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

            elligible_producers.retain(|k, _| producers_with_slot.contains(k));

            anyhow::ensure!(
                !elligible_producers.is_empty(),
                ImpossibleSlotOffset(*slot_range.end())
            );
        };

        info!("{} elligible producer(s)", elligible_producers.len());

        let producer_count_pairs = self
            .session
            .query(GET_PRODUCERS_CONSUMER_COUNT, &[])
            .await?
            .rows_typed::<(ProducerId, i64)>()?
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        elligible_producers
            .into_iter()
            .min_by_key(|(k, _)| producer_count_pairs.get(k).cloned().unwrap_or(0))
            .ok_or(anyhow::anyhow!("No producer is available right now"))
    }

    pub async fn get_min_offset_for_producer(
        &self,
        producer_id: ProducerId,
        max_revision_opt: Option<i64>,
    ) -> anyhow::Result<BTreeMap<ShardId, (ShardOffset, Slot)>> {
        let (remote_revision, offsets) = self
            .session
            .execute(&self.get_min_producer_offset_ps, (producer_id,))
            .await?
            .first_row_typed::<(i64, Option<BTreeMap<ShardId, (ShardOffset, Slot)>>)>()?;

        if let Some(max_revision) = max_revision_opt {
            anyhow::ensure!(max_revision >= remote_revision, StaleRevision(max_revision));
        }

        offsets.ok_or(anyhow::anyhow!(
            "Producer lock exists, but its minimum shard offset is not set."
        ))
    }

    pub async fn get_execution_id(
        &self,
        producer_id: ProducerId,
    ) -> anyhow::Result<Option<(i64, ExecutionId)>> {
        self.session
            .execute(&self.get_producer_execution_id_ps, (producer_id,))
            .await?
            .maybe_first_row_typed::<(i64, ExecutionId)>()
            .map_err(anyhow::Error::new)
    }

    pub async fn get_slot_shard_offsets(
        &self,
        slot: Slot,
        min_slot: Slot,
        producer_id: ProducerId,
        max_revision_opt: Option<i64>,
    ) -> anyhow::Result<Option<BTreeMap<ShardId, (ShardOffset, Slot)>>> {
        let maybe = self
            .session
            .execute(
                &self.get_shard_offset_in_slot_range_ps,
                (producer_id, slot, min_slot),
            )
            .await?
            .maybe_first_row_typed::<(i64, Vec<(ShardId, ShardOffset)>, Slot)>()?;

        if let Some((remote_revision, offsets, slot_approx)) = maybe {
            info!(
                "found producer({producer_id:?}) shard offsets within slot range: {min_slot}..={slot}"
            );

            if let Some(max_revision) = max_revision_opt {
                anyhow::ensure!(max_revision >= remote_revision, StaleRevision(max_revision));
            }

            Ok(Some(
                offsets
                    .into_iter()
                    .map(|(shard_id, shard_offset)| (shard_id, (shard_offset, slot_approx)))
                    .collect(),
            ))
        } else {
            Ok(None)
        }
    }

    pub async fn compute_offset(
        &self,
        producer_id: ProducerId,
        seek_loc: SeekLocation,
        max_revision_opt: Option<i64>,
    ) -> anyhow::Result<BTreeMap<ShardId, (ShardOffset, Slot)>> {
        let producer_info = self
            .get_producer_info(producer_id)
            .await?
            .ok_or(anyhow::anyhow!("producer does not exists"))?;
        let mut shard_offset_pairs: BTreeMap<ShardId, (ShardOffset, Slot)> = match seek_loc {
            SeekLocation::Latest => {
                info!("computing offset to latest seek location");
                sink::get_max_shard_offsets_for_producer(
                    Arc::clone(&self.session),
                    producer_id,
                    producer_info.num_shards as usize,
                )
                .await?
            }
            SeekLocation::Earliest => {
                info!("computing offset to earliest seek location");
                self.get_min_offset_for_producer(producer_id, max_revision_opt)
                    .await?
            }
            SeekLocation::SlotApprox {
                desired_slot,
                min_slot,
            } => {
                info!("computing offset to approx slot seek location (0)");
                let minium_producer_offsets = self
                    .get_min_offset_for_producer(producer_id, max_revision_opt)
                    .await?;

                info!("computing offset to approx slot seek location (1)");
                let shard_offsets_contain_slot = self
                    .get_slot_shard_offsets(desired_slot, min_slot, producer_id, max_revision_opt)
                    .await?
                    .ok_or(ImpossibleSlotOffset(desired_slot))?;

                info!("computing offset to approx slot seek location (2)");
                let are_shard_offset_reachable =
                    shard_offsets_contain_slot
                        .iter()
                        .all(|(shard_id, (offset1, _))| {
                            minium_producer_offsets
                                .get(shard_id)
                                .filter(|(offset2, _)| offset1 > offset2)
                                .is_some()
                        });

                info!("computing offset to approx slot seek location (3)");
                if !are_shard_offset_reachable {
                    anyhow::bail!(ImpossibleSlotOffset(desired_slot))
                }
                shard_offsets_contain_slot
            }
        };
        info!("compute offset has been done");
        let adjustment: i64 = match seek_loc {
            SeekLocation::Earliest
            | SeekLocation::SlotApprox {
                desired_slot: _,
                min_slot: _,
            } => -1,
            SeekLocation::Latest => 0,
        };

        shard_offset_pairs
            .iter_mut()
            .for_each(|(_k, v)| v.0 += adjustment);

        if shard_offset_pairs.len() != (producer_info.num_shards as usize) {
            anyhow::bail!("mismatch producer num shards and computed shard offset");
        }

        Ok(shard_offset_pairs)
    }
}

/// The `ProducerMonitor` trait defines methods for monitoring producers.
#[async_trait]
pub trait ProducerMonitor: Send + Sync + 'static {
    /// Lists all living producers.
    ///
    /// # Returns
    ///
    /// A vector of `ProducerId` representing the living producers.
    async fn list_living_producers(&self) -> BTreeMap<ProducerId, i64>;

    async fn is_producer_alive(&self, producer_id: ProducerId) -> bool {
        self.list_living_producers()
            .await
            .contains_key(&producer_id)
    }

    /// Gets the dead signal for a specific producer.
    ///
    /// # Parameters
    ///
    /// - `producer_id`: The ID of the producer to get the dead signal for.
    ///
    /// # Returns
    ///
    /// A `BoxFuture` that resolves to `()` when the producer is dead.
    async fn get_producer_dead_signal(&self, producer_id: ProducerId) -> ProducerDeadSignal;
}

pub struct EtcdProducerMonitor {
    etcd: etcd_client::Client,
}

impl EtcdProducerMonitor {
    pub fn new(etcd: etcd_client::Client) -> Self {
        Self { etcd }
    }
}

pub struct ProducerDeadSignal {
    // The actual signal that will be send when the producer is dead.
    rx: oneshot::Receiver<()>,

    // When dropped, this will signal any underlying thread implementation to terminate aswell.
    #[allow(dead_code)]
    tx_terminate: oneshot::Sender<()>,
}

impl Future for ProducerDeadSignal {
    type Output = Result<(), oneshot::error::RecvError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.rx.poll_unpin(cx)
    }
}

impl ProducerDeadSignal {
    pub fn new() -> (Self, oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        let (tx_terminate, rx_terminate) = oneshot::channel();
        (Self { rx, tx_terminate }, tx, rx_terminate)
    }

    pub fn from(rx: oneshot::Receiver<()>, tx_terminate: oneshot::Sender<()>) -> Self {
        Self { rx, tx_terminate }
    }
}

#[async_trait]
impl ProducerMonitor for EtcdProducerMonitor {
    /// Lists all living producers.
    ///
    /// # Returns
    ///
    /// A vector of `ProducerId` representing the living producers.
    async fn list_living_producers(&self) -> BTreeMap<ProducerId, i64> {
        let producer_lock_prefix = get_producer_lock_prefix_v1();
        let get_resp = self
            .etcd
            .kv_client()
            .get(producer_lock_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .expect("got an error while trying to get producer lock keys");
        get_resp
            .kvs()
            .iter()
            .map(|kv| {
                get_producer_id_from_lock_key_v1(kv.key()).map(|pid| (pid, kv.mod_revision()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
            .expect("failed to parse producer lock keys")
    }

    async fn is_producer_alive(&self, producer_id: ProducerId) -> bool {
        let producer_lock_path = get_producer_lock_path_v1(producer_id);
        let get_resp = self
            .etcd
            .kv_client()
            .get(producer_lock_path, Some(GetOptions::new().with_prefix()))
            .await
            .expect("failed to get producer lock key info");
        get_resp.count() > 0
    }

    /// Gets the dead signal for a specific producer.
    ///
    /// # Parameters
    ///
    /// - `producer_id`: The ID of the producer to get the dead signal for.
    ///
    /// # Returns
    ///
    /// A `BoxFuture` that resolves to `()` when the producer is dead.
    async fn get_producer_dead_signal(&self, producer_id: ProducerId) -> ProducerDeadSignal {
        let producer_lock_path = get_producer_lock_path_v1(producer_id);
        let (mut watcher, mut stream) = self
            .etcd
            .watch_client()
            .watch(
                producer_lock_path.as_bytes(),
                Some(WatchOptions::new().with_prefix()),
            )
            .await
            .expect("failed to acquire watch stream over producer lock");

        let get_resp = self
            .etcd
            .kv_client()
            .get(
                producer_lock_path.as_str(),
                Some(GetOptions::new().with_prefix()),
            )
            .await
            .expect("failed to get producer lock key info");

        let (signal, tx, mut rx_terminate) = ProducerDeadSignal::new();
        // If the producer is already dead, we can quit early
        if get_resp.count() == 0 {
            warn!("producer lock was not found, producer is dead already");
            tx.send(())
                .expect("failed to early send producer dead signal");
            return signal;
        }

        let anchor_revision = get_resp.kvs()[0].mod_revision();

        tokio::spawn(async move {
            'outer: loop {
                tokio::select! {
                    _ = &mut rx_terminate => {
                        if tx.is_closed() {
                            break 'outer;
                        }
                    }
                    Some(Ok(msg)) = stream.next() => {
                        for ev in msg.events() {
                            match ev.event_type() {
                                etcd_client::EventType::Put => {
                                    warn!("producer lock was updated");
                                    let kv = ev.kv().expect("empty put response");
                                    if kv.mod_revision() > anchor_revision {
                                        break 'outer;
                                    }
                                }
                                etcd_client::EventType::Delete => {
                                    warn!("producer lock was deleted");
                                    break 'outer;
                                }
                            }
                        }
                    }
                }
            }
            if tx.send(()).is_err() {
                warn!("producer dead signal receiver half was terminated before signal was send");
            }
            let _ = watcher.cancel().await;
        });
        signal
    }
}
