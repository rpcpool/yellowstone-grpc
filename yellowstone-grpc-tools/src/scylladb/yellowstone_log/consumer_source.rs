use {
    crate::scylladb::{
        types::{BlockchainEvent, ConsumerInfo, ProducerId, ShardId, Slot, UNDEFINED_SLOT},
        yellowstone_log::shard_iterator::ShardIterator,
    },
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

const FETCH_MICRO_BATCH_LATENCY_WARN_THRESHOLD: Duration = Duration::from_millis(500);

const UPDATE_CONSUMER_SHARD_OFFSET: &str = r###"
    UPDATE consumer_shard_offset
    SET offset = ?, slot = ?, updated_at = currentTimestamp() 
    WHERE 
        consumer_id = ?
        AND producer_id = ?
        AND shard_id = ?
        AND event_type = ?
"###;

pub(crate) struct ConsumerSource<T: FromBlockchainEvent> {
    session: Arc<Session>,
    consumer_info: ConsumerInfo,
    sender: mpsc::Sender<T>,
    // The interval at which we want to commit our Offset progression to Scylla
    offset_commit_interval: Duration,
    shard_iterators: BTreeMap<ShardId, ShardIterator>,
    pub(crate) shard_iterators_slot: BTreeMap<ShardId, Slot>,
    update_consumer_shard_offset_prepared_stmt: PreparedStatement,
}

pub type InterruptSignal = oneshot::Receiver<()>;

#[derive(Clone, Debug, PartialEq, Error, Eq, Copy)]
pub(crate) struct Interrupted;

impl fmt::Display for Interrupted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Interrupted")
    }
}

pub(crate) trait FromBlockchainEvent {
    type Output;

    fn from(blockchain_event: BlockchainEvent) -> Self::Output;
}

impl<T: FromBlockchainEvent<Output = T>> ConsumerSource<T> {
    pub(crate) async fn new(
        session: Arc<Session>,
        consumer_info: ConsumerInfo,
        sender: mpsc::Sender<T>,
        offset_commit_interval: Duration,
        mut shard_iterators: Vec<ShardIterator>,
    ) -> anyhow::Result<Self> {
        let update_consumer_shard_offset_prepared_stmt =
            session.prepare(UPDATE_CONSUMER_SHARD_OFFSET).await?;
        // Prewarm every shard iterator
        try_join_all(shard_iterators.iter_mut().map(|shard_it| shard_it.warm())).await?;
        let shard_iterators_slot = shard_iterators
            .iter()
            .map(|shard_it| (shard_it.shard_id, UNDEFINED_SLOT))
            .collect();
        Ok(ConsumerSource {
            session,
            consumer_info,
            sender,
            offset_commit_interval,
            shard_iterators: shard_iterators
                .into_iter()
                .map(|shard_it| (shard_it.shard_id, shard_it))
                .collect(),
            shard_iterators_slot,
            update_consumer_shard_offset_prepared_stmt,
        })
    }

    pub(crate) fn producer_id(&self) -> ProducerId {
        self.consumer_info.producer_id
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
                self.consumer_info.consumer_id.to_owned(),
                self.consumer_info.producer_id,
                shard_it.shard_id,
                shard_it.event_type,
            ));
            batch.append_statement(self.update_consumer_shard_offset_prepared_stmt.clone());
        }

        self.session.batch(&batch, values).await?;
        Ok(())
    }

    pub async fn run(&mut self, mut interrupt: InterruptSignal) -> anyhow::Result<()> {
        let consumer_id = self.consumer_info.consumer_id.to_owned();
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
                        self.update_consumer_shard_offsets().await?;
                        anyhow::bail!(Interrupted)
                    }
                    Err(TryRecvError::Closed) => anyhow::bail!("detected orphan consumer source"),
                    Err(TryRecvError::Empty) => (),
                }

                let maybe = shard_it.try_next().await?;

                if let Some(block_chain_event) = maybe {
                    self.shard_iterators_slot
                        .insert(*shard_id, block_chain_event.slot);
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
            // Every now and then, we commit where the consumer is loc
            if commit_offset_deadline.elapsed() > Duration::ZERO {
                let t = Instant::now();
                self.update_consumer_shard_offsets().await?;
                info!("updated consumer shard offset in {:?}", t.elapsed());
                commit_offset_deadline = Instant::now() + self.offset_commit_interval;
            }
        }
    }
}
