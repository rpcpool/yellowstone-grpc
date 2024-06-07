use {
    super::{lock::InstanceLock, shard_iterator::ShardIterator},
    crate::scylladb::{scylladb_utils::LwtResult, types::{
        BlockchainEvent, BlockchainEventType, ConsumerGroupId, ConsumerId, ExecutionId, ProducerId, ShardId, ShardOffsetMap, Slot, UNDEFINED_SLOT
    }},
    core::fmt,
    futures::future::try_join_all,
    scylla::{
        batch::{Batch, BatchType},
        prepared_statement::PreparedStatement,
        Session,
    },
    std::{collections::BTreeMap, sync::Arc, time::Duration},
    thiserror::Error,
    tokio::{
        sync::{
            mpsc,
            oneshot::{self, error::TryRecvError},
        },
        time::Instant,
    },
    tracing::{info, warn},
};

const CLIENT_LAG_WARN_THRESHOLD: Duration = Duration::from_millis(250);

const DEFAULT_OFFSET_COMMIT_INTERVAL: Duration = Duration::from_millis(500);

const FETCH_MICRO_BATCH_LATENCY_WARN_THRESHOLD: Duration = Duration::from_millis(500);

const UPDATE_CONSUMER_SHARD_OFFSET: &str = r###"
    UPDATE consumer_shard_offset
    SET offset = ?, slot = ?, revision = ?, updated_at = currentTimestamp() 
    WHERE 
        consumer_id = ?
        AND producer_id = ?
        AND shard_id = ?
        AND event_type = ?
    IF revision < ?
"###;

const UPDATE_CONSUMER_SHARD_OFFSET_V2: &str = r###"
    UPDATE consumer_shard_offset_v2
    SET 
        acc_shard_offset_map = ?, 
        tx_shard_offset_map = ?, 
        revision = ?
    WHERE
        consumer_group_id = ?
        AND consumer_id = ?
        AND execution_id = ?
    IF revision < ?
"###;

pub(crate) struct ConsumerSource<T: FromBlockchainEvent> {
    session: Arc<Session>,
    pub(crate) consumer_group_id: ConsumerGroupId,
    pub(crate) consumer_id: ConsumerId,
    pub(crate) producer_id: ProducerId,
    pub(crate) execution_id: ExecutionId,
    pub(crate) subscribed_event_types: Vec<BlockchainEventType>,
    sender: mpsc::Sender<T>,
    // The interval at which we want to commit our Offset progression to Scylla
    offset_commit_interval: Duration,
    shard_iterators: BTreeMap<ShardId, ShardIterator>,
    pub(crate) acc_update_shard_it_slot_map: BTreeMap<ShardId, Slot>,
    pub(crate) new_tx_shard_it_slot_map: BTreeMap<ShardId, Slot>,
    pub(crate) shard_iterators_slot: BTreeMap<ShardId, Slot>,
    update_consumer_shard_offset_prepared_stmt: PreparedStatement,
    update_consumer_shard_offset_v2_ps: PreparedStatement,
    instance_lock: InstanceLock,
}

pub type InterruptSignal = oneshot::Receiver<()>;

#[derive(Clone, Debug, PartialEq, Error, Eq, Copy)]
pub(crate) struct Interrupted;

impl fmt::Display for Interrupted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Interrupted")
    }
}

pub(crate) trait FromBlockchainEvent: Send + 'static {
    fn from(blockchain_event: BlockchainEvent) -> Self;
}

impl<T: FromBlockchainEvent> ConsumerSource<T> {
    pub(crate) async fn new(
        session: Arc<Session>,
        consumer_group_id: ConsumerGroupId,
        producer_id: ProducerId,
        execution_id: ExecutionId,
        subscribed_event_types: Vec<BlockchainEventType>,
        sender: mpsc::Sender<T>,
        mut shard_iterators: Vec<ShardIterator>,
        instance_lock: InstanceLock,
        offset_commit_interval: Option<Duration>,
    ) -> anyhow::Result<Self> {
        let update_consumer_shard_offset_prepared_stmt =
            session.prepare(UPDATE_CONSUMER_SHARD_OFFSET).await?;
        let update_consumer_shard_offset_v2_ps = session.prepare(UPDATE_CONSUMER_SHARD_OFFSET_V2).await?;
        // Prewarm every shard iterator
        try_join_all(shard_iterators.iter_mut().map(|shard_it| shard_it.warm())).await?;
        let shard_iterators_slot: BTreeMap<ShardId, Slot> = shard_iterators
            .iter()
            .map(|shard_it| (shard_it.shard_id, UNDEFINED_SLOT))
            .collect();
        Ok(ConsumerSource {
            session,
            consumer_group_id,
            consumer_id: instance_lock.instance_id.clone(),
            producer_id,
            execution_id,
            subscribed_event_types,
            sender,
            offset_commit_interval: offset_commit_interval
                .unwrap_or(DEFAULT_OFFSET_COMMIT_INTERVAL),
            shard_iterators: shard_iterators
                .into_iter()
                .map(|shard_it| (shard_it.shard_id, shard_it))
                .collect(),
            new_tx_shard_it_slot_map: shard_iterators_slot.clone(),
            acc_update_shard_it_slot_map: shard_iterators_slot.clone(),
            shard_iterators_slot,
            update_consumer_shard_offset_prepared_stmt,
            instance_lock,
            update_consumer_shard_offset_v2_ps,
        })
    }

    pub fn take_instance_lock(self) -> InstanceLock {
        self.instance_lock
    }

    async fn update_consumer_shard_offsets(&self) -> anyhow::Result<()> {
        let mut batch = Batch::new(BatchType::Unlogged);
        let mut values = Vec::with_capacity(self.shard_iterators_slot.len());
        for (shard_id, shard_it) in self.shard_iterators.iter() {
            values.push((
                shard_it.last_offset(),
                self.shard_iterators_slot
                    .get(shard_id)
                    .expect("missing shard slot info"),
                self.consumer_id.to_owned(),
                self.producer_id,
                shard_it.shard_id,
                shard_it.event_type,
            ));
            batch.append_statement(self.update_consumer_shard_offset_prepared_stmt.clone());
        }
        self.session.batch(&batch, values).await?;
        Ok(())
    }

    fn get_shard_offset_map(&self, ev_type: BlockchainEventType) -> ShardOffsetMap {
        self.shard_iterators
            .iter()
            .filter(|(_, v)| v.event_type == ev_type)
            .map(|(k, v)| {
                let slot = self.shard_iterators_slot.get(k).expect("missing shard slot info");
                (*k, (v.last_offset(), *slot))
            })
            .collect()
    }

    async fn update_consumer_shard_offsets_v2(&self) -> anyhow::Result<()> {
        
        let b1 = self.subscribed_event_types.contains(&BlockchainEventType::AccountUpdate);
        let b2 = self.subscribed_event_types.contains(&BlockchainEventType::NewTransaction);

        let (acc_shard_offsets, tx_shard_offsets) = match (b1, b2) {
            (true, false) => {
                let map = self.get_shard_offset_map(BlockchainEventType::AccountUpdate);
                (map.clone(), map) 
            },
            (false, true) => {
                let map = self.get_shard_offset_map(BlockchainEventType::NewTransaction);
                (map.clone(), map)
            },
            (true, true) => {
                let map1 = self.get_shard_offset_map(BlockchainEventType::AccountUpdate);
                let map2 = self.get_shard_offset_map(BlockchainEventType::NewTransaction);
                (map1, map2)
            }
            (false, false) => panic!("no blockchain event subscribed to")
        };
        let revision = self.instance_lock.get_fencing_token().await?;
        let values = (
            acc_shard_offsets,
            tx_shard_offsets,
            revision,
            &self.consumer_group_id,
            &self.consumer_id,
            &self.execution_id,
            revision,
        );

        let lwt_result = self.session
            .execute(&self.update_consumer_shard_offset_v2_ps, values)
            .await?
            .first_row_typed::<LwtResult>()?;
        if let LwtResult(false) = lwt_result {
            anyhow::bail!("Failed to update shard offset, lock is compromised");
        }

        Ok(())        
    }

    pub async fn run(&mut self, mut interrupt: InterruptSignal) -> anyhow::Result<()> {
        let consumer_id = self.consumer_id.to_owned();
        let mut commit_offset_deadline = Instant::now() + self.offset_commit_interval;
        const PRINT_CONSUMER_SLOT_REACH_DELAY: Duration = Duration::from_secs(5);
        info!("Serving consumer: {:?}", consumer_id);

        let mut max_seen_slot = UNDEFINED_SLOT;
        let mut num_event_between_two_slots = 0;

        let mut next_trace_schedule = Instant::now() + PRINT_CONSUMER_SLOT_REACH_DELAY;
        let mut t = Instant::now();
        loop {
            for (shard_id, shard_it) in self.shard_iterators.iter_mut() {
                match interrupt.try_recv() {
                    Ok(_) => {
                        warn!("consumer {consumer_id} received an interrupted signal");
                        //self.update_consumer_shard_offsets().await?;
                        self.update_consumer_shard_offsets_v2().await?;
                        return Ok(());
                    }
                    Err(TryRecvError::Closed) => anyhow::bail!("detected orphan consumer source"),
                    Err(TryRecvError::Empty) => (),
                }

                let maybe = shard_it.try_next().await?;

                if let Some(block_chain_event) = maybe {
                    self.shard_iterators_slot
                        .insert(*shard_id, block_chain_event.slot);
                    match block_chain_event.event_type {
                        BlockchainEventType::AccountUpdate => self.acc_update_shard_it_slot_map.insert(*shard_id, block_chain_event.slot),
                        BlockchainEventType::NewTransaction => self.new_tx_shard_it_slot_map.insert(*shard_id, block_chain_event.slot),
                    };
                    if t.elapsed() >= FETCH_MICRO_BATCH_LATENCY_WARN_THRESHOLD {
                        warn!(
                            "consumer {consumer_id} micro batch took {:?} to fetch.",
                            t.elapsed()
                        );
                    }
                    if max_seen_slot < block_chain_event.slot {
                        if next_trace_schedule.elapsed() > Duration::ZERO {
                            info!("Consumer {consumer_id} reach slot {max_seen_slot} after {num_event_between_two_slots} blockchain event(s)");
                            next_trace_schedule = Instant::now() + PRINT_CONSUMER_SLOT_REACH_DELAY;
                        }
                        max_seen_slot = block_chain_event.slot;
                        num_event_between_two_slots = 0;
                    }
                    let t_send = Instant::now();
                    if self.sender.send(T::from(block_chain_event)).await.is_err() {
                        anyhow::bail!("consumer {consumer_id} closed its streaming half");
                    }
                    let send_latency = t_send.elapsed();
                    if send_latency >= CLIENT_LAG_WARN_THRESHOLD {
                        warn!("Slow read from consumer {consumer_id}, recorded latency: {send_latency:?}")
                    }
                    num_event_between_two_slots += 1;
                    t = Instant::now();
                }
            }
            // Every now and then, we commit where the consumer is loc
            if commit_offset_deadline.elapsed() > Duration::ZERO {
                let t = Instant::now();
                // self.update_consumer_shard_offsets().await?;
                self.update_consumer_shard_offsets_v2().await?;
                info!("updated consumer shard offset in {:?}", t.elapsed());
                commit_offset_deadline = Instant::now() + self.offset_commit_interval;
            }
        }
    }
}
